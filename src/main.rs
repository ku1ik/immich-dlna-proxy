use std::path::PathBuf;

use clap::Parser;
use immich_dlna_proxy::config::Config;

#[derive(Parser)]
#[command(version, about)]
struct Arguments {
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let config = Config::load(&arguments.config)?;
    immich_dlna_proxy::logging(config.log_level)?;

    immich_dlna_proxy::run(config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn cli_requires_config_and_exposes_only_config_help_and_version() {
        assert!(Arguments::try_parse_from(["immich-dlna-proxy"]).is_err());

        let arguments =
            Arguments::try_parse_from(["immich-dlna-proxy", "--config", "/run/proxy.toml"])
                .unwrap();

        assert_eq!(arguments.config, Path::new("/run/proxy.toml"));

        for argument in ["--help", "--version"] {
            let error = Arguments::try_parse_from(["immich-dlna-proxy", argument])
                .err()
                .unwrap();

            assert!(!error.use_stderr());
        }

        assert!(
            Arguments::try_parse_from([
                "immich-dlna-proxy",
                "--config",
                "/run/proxy.toml",
                "--listen",
                "192.168.1.10:8200",
            ])
            .is_err()
        );
    }
}
