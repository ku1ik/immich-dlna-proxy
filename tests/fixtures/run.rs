#[path = "../fixture_server.rs"]
mod fixture_server;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = fixture_server::Arguments::parse();
    immich_dlna_proxy::lifecycle::logging(tracing::Level::DEBUG)?;

    fixture_server::run(arguments).await
}
