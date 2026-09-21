mod server;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = server::Arguments::parse();
    immich_dlna_proxy::lifecycle::logging(tracing::Level::DEBUG)?;

    server::run(arguments).await
}
