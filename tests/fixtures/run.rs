mod server;

use std::{net::SocketAddrV4, path::PathBuf};

use anyhow::{Context, ensure};
use clap::Parser;
use immich_dlna_proxy::{
    config::{is_non_loopback_unicast, resolve_interface},
    lifecycle,
    ssdp::Discovery,
};

use server::{Bound, SERVER_UUID};

#[derive(Parser)]
#[command(about = "Serve a DLNA verification catalog from local fixture files")]
struct Arguments {
    #[arg(long, value_name = "IP:PORT", value_parser = listen_address)]
    listen: SocketAddrV4,
    #[arg(long, value_name = "DIRECTORY")]
    fixtures: PathBuf,
}

fn listen_address(value: &str) -> anyhow::Result<SocketAddrV4> {
    let address: SocketAddrV4 = value.parse().context("expected an IPv4 socket address")?;

    ensure!(
        is_non_loopback_unicast(*address.ip()) && address.port() >= 1024,
        "listen must be concrete non-loopback unicast IPv4 with port 1024-65535"
    );

    Ok(address)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    lifecycle::logging(tracing::Level::DEBUG)?;

    let interface_index = resolve_interface(*arguments.listen.ip())?;
    let bound = Bound::bind(arguments.listen, arguments.fixtures).await?;
    let discovery = Discovery::bind(interface_index, SERVER_UUID, arguments.listen)?;

    bound.run(Some(discovery)).await
}
