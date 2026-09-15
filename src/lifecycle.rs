use std::time::Duration;

use anyhow::Context;
use tokio::signal::unix::{SignalKind, signal};
use tokio::{net::TcpListener, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    catalog::{Library, Store},
    config::Config,
    eventing::Subscriptions,
    media::MediaProxy,
    server::Server,
    ssdp::Discovery,
};

pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

pub async fn run(config: Config) -> anyhow::Result<()> {
    let signal = shutdown_signal()?;
    let (store, mut ledger) = Store::open(&config.state_directory, config.server_uuid)?;
    ledger.restart();
    store.persist(ledger.clone()).await;

    let media = MediaProxy::new(config.api_base.clone(), config.api_key.clone())?;
    let events = Subscriptions::new()?;
    let stop = CancellationToken::new();
    let events_stop = CancellationToken::new();
    let address = config.listen_address;
    let uuid = config.server_uuid;
    let name = config.friendly_name.clone();

    let http = TcpListener::bind(address)
        .await
        .context("cannot bind configured HTTP listener")?;
    let discovery = Discovery::bind(*address.ip(), config.interface.index, uuid, address)?;
    let library = Library::new(config, store, ledger, events.clone(), stop.clone())?;
    let server = Server::new(name, uuid, library.clone(), media, events.clone());
    let mut tasks = JoinSet::new();

    tasks.spawn(async move { ("catalog", library.run().await) });
    let token = events_stop.clone();
    tasks.spawn(async move { ("eventing", events.run(token).await) });
    let token = stop.clone();
    tasks.spawn(async move { ("HTTP", server.run(http, token, SHUTDOWN_GRACE).await) });
    let token = stop.clone();
    tasks.spawn(async move { ("SSDP", discovery.run(token).await) });
    tracing::info!(%address, %uuid, "service started");
    let result = supervise(tasks, stop, events_stop, signal).await;

    if result.is_ok() {
        tracing::info!("service stopped");
    }

    result
}

async fn supervise(
    mut tasks: JoinSet<(&'static str, anyhow::Result<()>)>,
    stop: CancellationToken,
    events_stop: CancellationToken,
    signal: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    tokio::pin!(signal);
    let mut deadline = None;
    let mut failure = None;

    while !tasks.is_empty() {
        let expired = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;
            joined = tasks.join_next() => {
                let error = match joined.expect("nonempty service tasks") {
                    Ok((name, Err(error))) => Some(error.context(format!("{name} service failed"))),
                    Ok((name, Ok(()))) if deadline.is_none() => Some(anyhow::anyhow!("{name} service exited unexpectedly")),
                    Ok(_) => None,
                    Err(_) => Some(anyhow::anyhow!("essential service task panicked or was cancelled")),
                };

                if let Some(error) = error {
                    tracing::error!(%error, "essential service failure");
                    failure.get_or_insert(error);
                    stop.cancel();
                    deadline.get_or_insert_with(|| Instant::now() + SHUTDOWN_GRACE);
                }
            }

            _ = &mut signal, if deadline.is_none() => {
                tracing::info!("shutdown requested; withdrawing discovery and draining admitted work");
                stop.cancel();
                deadline = Some(Instant::now() + SHUTDOWN_GRACE);
            }

            _ = expired => {
                // Eventing remains alive while admitted SUBSCRIBE responses resolve.
                // The same grace bounds pending callbacks, media and preparation.
                events_stop.cancel();
                tasks.abort_all();
                break;
            }
        }
    }

    stop.cancel();
    events_stop.cancel();

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((name, Err(error))) => {
                failure
                    .get_or_insert(error.context(format!("{name} service failed during shutdown")));
            }

            Err(error) if error.is_panic() => {
                failure.get_or_insert_with(|| {
                    anyhow::anyhow!("essential service task panicked during shutdown")
                });
            }

            _ => {}
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Install both signal listeners before starting essential tasks or announcing.
pub fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    let mut terminate =
        signal(SignalKind::terminate()).context("cannot install SIGTERM handler")?;
    let mut interrupt = signal(SignalKind::interrupt()).context("cannot install SIGINT handler")?;

    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => {}

            _ = interrupt.recv() => {}
        }
    })
}

pub fn logging(level: tracing::Level) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()
        .map_err(|_| anyhow::anyhow!("cannot initialize logging"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn pending_subscription_becomes_owed_during_shutdown_and_callback_obeys_grace() {
        use crate::protocol::Service;
        use http::{HeaderMap, HeaderValue, Method, StatusCode};
        use std::{net::Ipv4Addr, time::Duration};
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

        tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(3), async {
            let callback = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let subscriptions = Subscriptions::new().unwrap();
            subscriptions.publish(42);
            let mut headers = HeaderMap::new();
            headers.insert("nt", HeaderValue::from_static("upnp:event"));

            headers.insert(
                "callback",
                format!("<http://{}/events>", callback.local_addr().unwrap())
                    .parse()
                    .unwrap(),
            );

            let (response, pending) = subscriptions.request(
                Service::ContentDirectory,
                Ipv4Addr::LOCALHOST,
                &Method::from_bytes(b"SUBSCRIBE").unwrap(),
                &headers,
            );

            assert_eq!(response.status(), StatusCode::OK);
            let pending = pending.unwrap();
            let sid = response.headers()["sid"].to_str().unwrap().to_owned();
            let stop = CancellationToken::new();
            let events_stop = CancellationToken::new();
            let mut tasks = JoinSet::new();
            let events = subscriptions.clone();
            let token = events_stop.clone();

            tasks.spawn(async move { ("eventing", events.run(token).await) });

            let admission = stop.clone();

            tasks.spawn(async move {
                // Model completion of an admitted response after shutdown begins.
                // Transport/FIN ordering is exercised by the server's wire tests.
                admission.cancelled().await;
                subscriptions.response_complete(pending, true);

                ("HTTP", Ok(()))
            });

            let started = Instant::now();
            let supervisor = tokio::spawn(supervise(tasks, stop, events_stop.clone(), async {}));

            let (socket, _) = tokio::time::timeout(Duration::from_secs(2), callback.accept())
                .await
                .expect("accepted subscription must retain its initial notification")
                .unwrap();

            let mut callback = BufReader::new(socket);
            let mut notification = String::new();
            let mut length = None;

            loop {
                let mut line = String::new();
                assert_ne!(callback.read_line(&mut line).await.unwrap(), 0);
                notification.push_str(&line);

                if line == "\r\n" {
                    break;
                }

                if let Some(value) = line.strip_prefix("content-length:") {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }

            let mut body = vec![0; length.unwrap()];
            callback.read_exact(&mut body).await.unwrap();
            assert!(notification.starts_with("NOTIFY /events HTTP/1.1\r\n"));
            assert!(notification.contains(&format!("sid: {sid}\r\n")));
            assert!(notification.contains("seq: 0\r\n"));

            assert!(
                String::from_utf8(body)
                    .unwrap()
                    .contains("<SystemUpdateID>42</SystemUpdateID>")
            );

            assert!(!supervisor.is_finished());
            assert!(!events_stop.is_cancelled());

            // An unanswered callback has a 30-second timeout; shutdown must cut it short.
            supervisor.await.unwrap().unwrap();
            assert!(events_stop.is_cancelled());
            assert!(started.elapsed() < SHUTDOWN_GRACE + Duration::from_secs(1));
            let mut remaining = Vec::new();
            callback.read_to_end(&mut remaining).await.unwrap();
            assert!(remaining.is_empty());
        })
        .await
        .expect("subscription shutdown exceeded the common grace");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_retains_events_for_admitted_work_within_one_common_budget() {
        let stop = CancellationToken::new();
        let events_stop = CancellationToken::new();
        let signal = CancellationToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();
        let done = finished.clone();
        let token = stop.clone();
        let events = events_stop.clone();

        tasks.spawn(async move {
            token.cancelled().await;
            tokio::time::sleep(std::time::Duration::from_secs(4)).await;
            assert!(!events.is_cancelled());
            done.store(true, Ordering::SeqCst);

            ("HTTP", Ok(()))
        });

        let events = events_stop.clone();

        tasks.spawn(async move {
            events.cancelled().await;

            ("eventing", Ok(()))
        });

        let before = Instant::now();
        signal.cancel();
        supervise(
            tasks,
            stop.clone(),
            events_stop.clone(),
            signal.cancelled_owned(),
        )
        .await
        .unwrap();
        assert!(finished.load(Ordering::SeqCst));
        assert!(stop.is_cancelled());
        assert!(events_stop.is_cancelled());

        assert!(
            (std::time::Duration::from_secs(4)
                ..=SHUTDOWN_GRACE + std::time::Duration::from_millis(10))
                .contains(&before.elapsed())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn essential_failure_is_fatal_but_does_not_abort_an_admitted_commit() {
        for panic in [false, true] {
            let stop = CancellationToken::new();
            let events_stop = CancellationToken::new();
            let mut tasks = JoinSet::new();
            let finished = Arc::new(AtomicBool::new(false));
            let done = finished.clone();
            let token = stop.clone();

            tasks.spawn(async move {
                token.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                done.store(true, Ordering::SeqCst);

                ("catalog", Ok(()))
            });

            tasks.spawn(async move {
                assert!(!panic, "injected essential-task panic");

                ("SSDP", Err(anyhow::anyhow!("injected failure")))
            });

            assert!(
                supervise(tasks, stop, events_stop, std::future::pending())
                    .await
                    .is_err()
            );
            assert!(finished.load(Ordering::SeqCst));
        }
    }
}
