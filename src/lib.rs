pub mod catalog;
pub mod config;
pub mod eventing;
mod immich;
pub mod lifecycle;
pub mod media;
mod mime;
pub mod protocol;
pub mod server;
pub mod ssdp;

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
