pub mod catalog;
pub mod config;
pub mod eventing;
mod immich;
pub mod media;
mod mime;
pub mod protocol;
pub mod server;
pub mod ssdp;

use anyhow::Context;
use tokio::net::TcpListener;

use crate::{
    catalog::ImmichCatalog, config::Config, eventing::Subscriptions, media::MediaProxy,
    server::Server, ssdp::Discovery,
};

pub async fn run(config: Config) -> anyhow::Result<()> {
    let media = MediaProxy::new(config.api_base.clone(), config.api_key.clone())?;
    let events = Subscriptions::new()?;
    let address = config.listen_address;
    let uuid = config.server_uuid;
    let name = config.friendly_name.clone();
    let interface_index = config.interface_index;
    let catalog = ImmichCatalog::open(config, events.clone()).await?;

    let http = TcpListener::bind(address)
        .await
        .context("cannot bind configured HTTP listener")?;

    let discovery = Discovery::bind(interface_index, uuid, address)?;
    let server = Server::new(name, uuid, catalog.clone(), media, events.clone());
    tracing::info!(%address, %uuid, "service started");

    let (name, result) = tokio::select! {
        result = catalog.run() => ("catalog", result),

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

pub(crate) fn outbound_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(std::time::Duration::from_secs(5))
}

pub(crate) fn server_header() -> &'static str {
    static HEADER: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let release = std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_else(|_| "unknown".into());

        let release: String = release
            .trim()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
            .collect();

        format!(
            "Linux/{release} UPnP/1.0 immich-dlna-proxy/{}",
            env!("CARGO_PKG_VERSION")
        )
    });

    &HEADER
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::PermissionsExt, path::PathBuf};

    use http::HeaderValue;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn revision_startup_precedes_listener_binding() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let state = directory.path().join("revisions.json");
        std::fs::write(&state, b"invalid").unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o600)).unwrap();

        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let address = match occupied.local_addr().unwrap() {
            std::net::SocketAddr::V4(address) => address,
            std::net::SocketAddr::V6(_) => unreachable!(),
        };

        let config = Config {
            api_base: "http://127.0.0.1:9/api/".parse().unwrap(),
            api_key: HeaderValue::from_static("test-key"),
            listen_address: address,
            friendly_name: "Test".into(),
            collator: crate::config::collator("en").unwrap(),
            server_uuid: Uuid::from_u128(1),
            state_directory: PathBuf::from(directory.path()),
            log_level: tracing::Level::INFO,
            interface_index: 1,
        };

        let error = run(config).await.unwrap_err().to_string();

        assert!(error.contains("invalid revision JSON"), "{error}");
    }
}
