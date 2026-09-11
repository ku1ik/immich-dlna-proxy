//! Absolute deadlines for caller-owned asynchronous work.

use std::future::Future;

use tokio::time::Instant;

#[derive(Debug, Eq, PartialEq)]
pub struct Expired;

/// Accept an output only strictly before `deadline`.
///
/// Future construction happens before this helper runs. Put side effects in an
/// `async` block to defer them until polling. Expiry or cancellation drops the
/// owned future, but cannot undo side effects or stop independently owned work:
/// dropping a task's join handle, for example, does not stop that task.
/// Synchronous work cannot be interrupted; its late output is rejected instead.
/// A ready timer takes priority, but expiry is not a side-effect barrier.
pub async fn timeout_at<F: Future>(deadline: Instant, future: F) -> Result<F::Output, Expired> {
    if Instant::now() >= deadline {
        return Err(Expired);
    }

    let output = tokio::select! {
        biased;

        _ = tokio::time::sleep_until(deadline) => return Err(Expired),

        output = future => output,
    };

    if Instant::now() >= deadline {
        return Err(Expired);
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        future::{pending, poll_fn},
        pin::pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    #[tokio::test(start_paused = true)]
    async fn ready_outputs_before_exactly_at_and_after_deadline() {
        for elapsed in [9, 10, 11] {
            for output in [Ok(7), Err(8)] {
                let deadline = Instant::now() + Duration::from_secs(10);
                tokio::time::advance(Duration::from_secs(elapsed)).await;
                let result = timeout_at(deadline, async { output }).await;

                assert_eq!(
                    result,
                    if elapsed < 10 {
                        Ok(output)
                    } else {
                        Err(Expired)
                    }
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn synchronous_work_finishing_at_or_after_deadline_is_rejected() {
        for elapsed in [9, 10, 11] {
            for output in [Ok(7), Err(8)] {
                let deadline = Instant::now() + Duration::from_secs(10);

                let future = poll_fn(|cx| {
                    // Advance the paused clock synchronously inside this inner poll.
                    let mut advance = pin!(tokio::time::advance(Duration::from_secs(elapsed)));
                    assert!(advance.as_mut().poll(cx).is_pending());

                    Poll::Ready(output)
                });

                assert_eq!(
                    timeout_at(deadline, future).await,
                    if elapsed < 10 {
                        Ok(output)
                    } else {
                        Err(Expired)
                    }
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_work_ready_before_at_or_after_deadline() {
        for elapsed in [9, 10, 11] {
            for output in [Ok(7), Err(8)] {
                let deadline = Instant::now() + Duration::from_secs(10);
                let resumed = Cell::new(false);
                let (send, receive) = tokio::sync::oneshot::channel();

                let mut bounded = pin!(timeout_at(deadline, async {
                    let output = receive.await.unwrap();
                    resumed.set(true);

                    output
                }));

                assert!(futures_util::poll!(&mut bounded).is_pending());
                tokio::time::advance(Duration::from_secs(elapsed)).await;
                send.send(output).unwrap();

                assert_eq!(
                    bounded.await,
                    if elapsed < 10 {
                        Ok(output)
                    } else {
                        Err(Expired)
                    }
                );

                // Once the timer is ready, it wins without resuming the operation.
                // At the exact boundary, only result rejection is guaranteed.
                if elapsed != 10 {
                    assert_eq!(resumed.get(), elapsed < 10);
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timer_wakes_without_an_inner_wake() {
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(timeout_at(deadline, pending::<()>()).await, Err(Expired));
        assert_eq!(Instant::now(), deadline);
    }

    struct Dropped<'a>(&'a Cell<bool>);

    impl Drop for Dropped<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_and_cancellation_drop_owned_future() {
        for mode in 0..3 {
            let dropped = Cell::new(false);
            let guard = Dropped(&dropped);

            let future = async move {
                let _guard = guard;
                pending::<()>().await;
            };

            let mut bounded =
                Box::pin(timeout_at(Instant::now() + Duration::from_secs(10), future));

            if mode != 0 {
                assert!(
                    bounded
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            }

            if mode == 2 {
                assert_eq!(bounded.as_mut().await, Err(Expired));
                assert!(dropped.get());
            }

            drop(bounded);
            assert!(dropped.get());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn async_block_defers_side_effects_but_join_handle_does_not_own_task() {
        let started = Cell::new(false);

        assert_eq!(
            timeout_at(Instant::now(), async {
                started.set(true);
            })
            .await,
            Err(Expired)
        );

        assert!(!started.get());
        let (release, wait) = tokio::sync::oneshot::channel();
        let (finished, result) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            wait.await.unwrap();
            finished.send(7).unwrap();
        });

        assert!(matches!(
            timeout_at(Instant::now(), task).await,
            Err(Expired)
        ));

        release.send(()).unwrap();
        assert_eq!(result.await.unwrap(), 7);
    }
}
