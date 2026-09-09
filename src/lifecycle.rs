use anyhow::Context;
use tokio::signal::unix::{SignalKind, signal};
use tokio::{net::TcpListener, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    catalog::Library, config::Config, eventing::Subscriptions, limits, media::MediaProxy,
    revisions::Store, server::Server, ssdp::Discovery,
};

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
    tasks.spawn(async move { ("HTTP", server.run(http, token).await) });
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
                    deadline.get_or_insert_with(|| Instant::now() + limits::SHUTDOWN_GRACE);
                }
            }

            _ = &mut signal, if deadline.is_none() => {
                tracing::info!("shutdown requested; withdrawing discovery and draining admitted work");
                stop.cancel();
                deadline = Some(Instant::now() + limits::SHUTDOWN_GRACE);
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

    #[tokio::test(start_paused = true)]
    async fn shutdown_retains_events_and_admitted_work_until_one_common_deadline() {
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
        assert_eq!(before.elapsed(), limits::SHUTDOWN_GRACE);
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
