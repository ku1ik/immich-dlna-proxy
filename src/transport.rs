use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::server::conn::http1;
use hyper_util::rt::TokioTimer;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

use crate::limits;

pub fn http1(header_timeout: Duration) -> http1::Builder {
    let mut builder = http1::Builder::new();

    builder
        .keep_alive(false)
        .half_close(false)
        .max_buf_size(limits::HEADER_BYTES)
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);

    builder
}

/// Only a pending write/flush/shutdown starts an idle clock, not upstream waiting.
pub struct WriteDeadline<T> {
    io: T,
    timeout: Duration,
    blocked: Option<Pin<Box<Sleep>>>,
}

impl<T> WriteDeadline<T> {
    pub fn new(io: T, timeout: Duration) -> Self {
        Self {
            io,
            timeout,
            blocked: None,
        }
    }

    fn check_deadline(&self) -> io::Result<()> {
        // Retaining an expired timer makes timeout terminal without another flag.
        if self
            .blocked
            .as_ref()
            .is_some_and(|timer| Instant::now() >= timer.deadline())
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP write stalled",
            ));
        }

        Ok(())
    }

    fn progress<R>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<R>>,
        progressed: bool,
    ) -> Poll<io::Result<R>> {
        self.check_deadline()?;

        if progressed {
            self.blocked = None;
        } else if result.is_pending() {
            let timer = self
                .blocked
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(self.timeout)));

            let _ = timer.as_mut().poll(cx);
            self.check_deadline()?;
        }

        result
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for WriteDeadline<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for WriteDeadline<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check_deadline()?;
        let result = Pin::new(&mut self.io).poll_write(cx, buf);
        let progressed = matches!(result, Poll::Ready(Ok(n)) if n > 0);

        self.progress(cx, result, progressed)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_deadline()?;
        let result = Pin::new(&mut self.io).poll_flush(cx);
        let progressed = matches!(result, Poll::Ready(Ok(())));

        self.progress(cx, result, progressed)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_deadline()?;
        let result = Pin::new(&mut self.io).poll_shutdown(cx);
        let progressed = matches!(result, Poll::Ready(Ok(())));

        self.progress(cx, result, progressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const TIMEOUT: Duration = Duration::from_secs(2);

    #[derive(Default)]
    struct Writer {
        ready: bool,
        zero: bool,
        polls: usize,
        elapsed_in_poll: Duration,
    }

    impl Writer {
        fn poll(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.polls += 1;

            if !self.elapsed_in_poll.is_zero() {
                // Advance updates the paused clock on its first poll, then yields.
                let mut advance = Box::pin(tokio::time::advance(self.elapsed_in_poll));
                let _ = advance.as_mut().poll(cx);
            }

            if self.ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
    }

    impl AsyncWrite for Writer {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.poll(cx)
                .map_ok(|()| if self.zero { 0 } else { buf.len() })
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll(cx)
        }
    }

    fn poll_operation(
        transport: &mut WriteDeadline<Writer>,
        operation: &str,
    ) -> Poll<io::Result<()>> {
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

        match operation {
            "write" => Pin::new(transport).poll_write(&mut cx, b"a").map_ok(|_| ()),
            "flush" => Pin::new(transport).poll_flush(&mut cx),
            "shutdown" => Pin::new(transport).poll_shutdown(&mut cx),
            _ => unreachable!(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_then_ready_before_at_and_after_deadline() {
        for operation in ["write", "flush", "shutdown"] {
            for elapsed in [TIMEOUT - Duration::from_nanos(1), TIMEOUT, TIMEOUT * 2] {
                let mut transport = WriteDeadline::new(Writer::default(), TIMEOUT);
                assert!(poll_operation(&mut transport, operation).is_pending());
                tokio::time::advance(elapsed).await;
                transport.io.ready = true;
                let result = poll_operation(&mut transport, operation);

                if elapsed < TIMEOUT {
                    assert!(matches!(result, Poll::Ready(Ok(()))), "{operation}");
                    assert!(transport.blocked.is_none());
                    assert_eq!(transport.io.polls, 2);
                } else {
                    assert!(
                        matches!(result, Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::TimedOut)
                    );

                    assert_eq!(transport.io.polls, 1);

                    for retry in ["write", "flush", "shutdown"] {
                        assert!(
                            matches!(poll_operation(&mut transport, retry), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::TimedOut)
                        );
                    }

                    assert_eq!(transport.io.polls, 1);
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_checked_after_underlying_poll() {
        for operation in ["write", "flush", "shutdown"] {
            for elapsed in [TIMEOUT - Duration::from_nanos(1), TIMEOUT, TIMEOUT * 2] {
                let mut transport = WriteDeadline::new(Writer::default(), TIMEOUT);
                assert!(poll_operation(&mut transport, operation).is_pending());
                transport.io.ready = true;
                transport.io.elapsed_in_poll = elapsed;
                let result = poll_operation(&mut transport, operation);
                assert_eq!(transport.io.polls, 2);

                if elapsed < TIMEOUT {
                    assert!(matches!(result, Poll::Ready(Ok(()))), "{operation}");
                    assert!(transport.blocked.is_none());
                } else {
                    assert!(
                        matches!(result, Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::TimedOut)
                    );

                    assert!(
                        matches!(poll_operation(&mut transport, operation), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::TimedOut)
                    );

                    assert_eq!(transport.io.polls, 2);
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn zero_byte_write_neither_starts_nor_clears_blocked_episode() {
        let mut transport = WriteDeadline::new(Writer::default(), TIMEOUT);
        transport.io.ready = true;
        transport.io.zero = true;
        assert_eq!(transport.write(b"a").await.unwrap(), 0);
        assert!(transport.blocked.is_none());
        transport.io.ready = false;
        assert!(poll_operation(&mut transport, "write").is_pending());
        let deadline = transport.blocked.as_ref().unwrap().deadline();
        tokio::time::advance(TIMEOUT / 2).await;
        transport.io.ready = true;
        assert_eq!(transport.write(b"a").await.unwrap(), 0);
        assert_eq!(transport.blocked.as_ref().unwrap().deadline(), deadline);
        tokio::time::advance(TIMEOUT / 2).await;

        assert_eq!(
            transport.write(b"a").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[tokio::test(start_paused = true)]
    async fn upstream_waiting_does_not_start_timer_and_reads_pass_through() {
        let (io, mut peer) = tokio::io::duplex(1);
        let mut transport = WriteDeadline::new(io, TIMEOUT);
        tokio::time::advance(TIMEOUT * 2).await;
        assert!(transport.blocked.is_none());
        transport.write_all(b"a").await.unwrap();
        tokio::time::advance(TIMEOUT * 2).await;
        assert!(transport.blocked.is_none());
        assert_eq!(peer.read_u8().await.unwrap(), b'a');
        transport.write_all(b"b").await.unwrap();

        assert_eq!(
            transport.write_all(b"c").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );

        peer.write_all(b"r").await.unwrap();
        assert_eq!(transport.read_u8().await.unwrap(), b'r');
        assert_eq!(peer.read_u8().await.unwrap(), b'b');

        assert_eq!(
            transport.write_all(b"d").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_progress_resets_a_shared_blocked_episode() {
        for operation in ["write", "flush", "shutdown"] {
            let mut transport = WriteDeadline::new(Writer::default(), TIMEOUT);
            assert!(poll_operation(&mut transport, "write").is_pending());
            let deadline = transport.blocked.as_ref().unwrap().deadline();
            tokio::time::advance(TIMEOUT / 2).await;
            assert!(poll_operation(&mut transport, operation).is_pending());
            assert_eq!(transport.blocked.as_ref().unwrap().deadline(), deadline);
            transport.io.ready = true;

            assert!(matches!(
                poll_operation(&mut transport, operation),
                Poll::Ready(Ok(()))
            ));

            assert!(transport.blocked.is_none());
            tokio::time::advance(TIMEOUT * 2).await;
            transport.io.ready = false;
            assert!(poll_operation(&mut transport, operation).is_pending());

            assert_eq!(
                transport.blocked.as_ref().unwrap().deadline(),
                Instant::now() + TIMEOUT
            );
        }

        let mut transport = WriteDeadline::new(Writer::default(), Duration::ZERO);

        assert!(
            matches!(poll_operation(&mut transport, "write"), Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::TimedOut)
        );

        assert_eq!(transport.io.polls, 1);
    }
}
