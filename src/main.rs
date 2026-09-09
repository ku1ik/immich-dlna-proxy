use clap::Parser;
use immich_dlna_proxy::{
    config::{Arguments, Config},
    lifecycle,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let config = Config::load(&arguments.config)?;
    lifecycle::logging(config.log_level)?;

    lifecycle::run(config).await
}
