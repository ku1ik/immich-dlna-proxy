use anyhow::Context;
use tokio::net::TcpListener;

use crate::{
    catalog::{Library, Store},
    config::Config,
    eventing::Subscriptions,
    media::MediaProxy,
    server::Server,
    ssdp::Discovery,
};

pub async fn run(config: Config) -> anyhow::Result<()> {
    let (store, mut ledger) = Store::open(&config.state_directory, config.server_uuid)?;
    ledger.restart();
    store.persist(ledger.clone()).await;

    let media = MediaProxy::new(config.api_base.clone(), config.api_key.clone())?;
    let events = Subscriptions::new()?;
    let address = config.listen_address;
    let uuid = config.server_uuid;
    let name = config.friendly_name.clone();

    let http = TcpListener::bind(address)
        .await
        .context("cannot bind configured HTTP listener")?;
    let discovery = Discovery::bind(config.interface_index, uuid, address)?;
    let library = Library::new(config, store, ledger, events.clone())?;
    let server = Server::new(name, uuid, library.clone(), media, events.clone());
    tracing::info!(%address, %uuid, "service started");

    let (name, result) = tokio::select! {
        result = library.run() => ("catalog", result),

        result = events.run() => ("eventing", result),

        result = server.run(http) => ("HTTP", result),

        result = discovery.run() => ("SSDP", result),
    };

    match result {
        Ok(()) => Err(anyhow::anyhow!("{name} service exited unexpectedly")),
        Err(error) => Err(error.context(format!("{name} service failed"))),
    }
}

pub fn logging(level: tracing::Level) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()
        .map_err(|_| anyhow::anyhow!("cannot initialize logging"))
}
