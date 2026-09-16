use std::{
    fs::{self, OpenOptions},
    io::Read,
    net::{Ipv4Addr, SocketAddrV4},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail, ensure};
use http::HeaderValue;
use icu_collator::{
    Collator, CollatorBorrowed,
    options::{CaseLevel, CollatorOptions, Strength},
};
use icu_locale::Locale;
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

const KEY_BYTES: u64 = 8 * 1024;

#[derive(clap::Parser)]
#[command(version, about)]
pub struct Arguments {
    #[arg(long, value_name = "PATH")]
    pub config: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    immich_url: String,
    immich_api_key_file: PathBuf,
    listen_address: SocketAddrV4,
    #[serde(default = "default_name")]
    friendly_name: String,
    sort_locale: String,
    server_uuid: Uuid,
    state_directory: PathBuf,
    #[serde(default = "default_log_level")]
    log_level: String,
}

fn default_name() -> String {
    "Immich".into()
}

fn default_log_level() -> String {
    "info".into()
}

// Intentionally no Debug: configuration owns the upstream credential.
pub struct Config {
    pub(crate) api_base: Url,
    pub(crate) api_key: HeaderValue,
    pub(crate) listen_address: SocketAddrV4,
    pub(crate) friendly_name: String,
    pub(crate) collator: CollatorBorrowed<'static>,
    pub(crate) server_uuid: Uuid,
    pub(crate) state_directory: PathBuf,
    pub log_level: tracing::Level,
    pub(crate) interface_index: u32,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = fs::read_to_string(path).context("cannot read configuration file")?;
        let settings = parse_settings(&text)?;
        let api_base = normalize_api_base(&settings.immich_url)?;
        let collator = collator(&settings.sort_locale)?;
        let api_key = read_key(&settings.immich_api_key_file)?;
        let interface_index = resolve_interface(*settings.listen_address.ip())?;

        Ok(Self {
            api_base,
            api_key,
            listen_address: settings.listen_address,
            friendly_name: settings.friendly_name,
            collator,
            server_uuid: settings.server_uuid,
            state_directory: settings.state_directory,
            log_level: settings.log_level.parse().expect("validated log level"),
            interface_index,
        })
    }
}

fn parse_settings(text: &str) -> anyhow::Result<Settings> {
    // TOML diagnostics can include source lines: do not propagate the raw document.
    let settings: Settings = toml::from_str(text)
        .map_err(|_| anyhow::anyhow!("invalid configuration fields or TOML"))?;

    ensure!(
        settings.immich_api_key_file.is_absolute() && settings.state_directory.is_absolute(),
        "credential and state paths must be absolute"
    );

    ensure!(
        is_non_loopback_unicast(*settings.listen_address.ip())
            && settings.listen_address.port() >= 1024,
        "listen_address must be a concrete non-loopback unicast IPv4 address with port 1024-65535"
    );

    ensure!(
        !settings.server_uuid.is_nil(),
        "server_uuid must be a non-nil UUID"
    );

    ensure!(
        !settings.friendly_name.is_empty()
            && settings.friendly_name.len() <= 128
            && settings
                .friendly_name
                .chars()
                .all(crate::protocol::xml_char),
        "friendly_name must contain 1-128 UTF-8 bytes of XML-safe text"
    );

    ensure!(
        matches!(
            settings.log_level.as_str(),
            "error" | "warn" | "info" | "debug" | "trace"
        ),
        "log_level must be error, warn, info, debug or trace"
    );

    Ok(settings)
}

fn normalize_api_base(value: &str) -> anyhow::Result<Url> {
    let mut url = Url::parse(value).map_err(|_| anyhow::anyhow!("invalid immich_url"))?;

    ensure!(
        is_safe_api_base(&url),
        "immich_url must be an absolute HTTP(S) URL without userinfo, query or fragment"
    );

    let path = url.path().trim_end_matches('/');
    let path = if path.rsplit('/').next() == Some("api") {
        format!("{path}/")
    } else {
        format!("{path}/api/")
    };

    url.set_path(&path);

    Ok(url)
}

pub(crate) fn is_normalized_api_base(url: &Url) -> bool {
    is_safe_api_base(url) && url.path().ends_with("/api/")
}

fn is_safe_api_base(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

pub(crate) fn collator(locale: &str) -> anyhow::Result<CollatorBorrowed<'static>> {
    let locale: Locale = locale.parse().context("invalid sort_locale identifier")?;
    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Secondary);
    options.case_level = Some(CaseLevel::Off);

    Collator::try_new(locale.into(), options).context("cannot initialize sort_locale collation")
}

pub fn is_non_loopback_unicast(address: Ipv4Addr) -> bool {
    !matches!(address.octets()[0], 0 | 127 | 224..=255)
}

fn read_key(path: &Path) -> anyhow::Result<HeaderValue> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .context("cannot open immich_api_key_file")?;

    let metadata = file.metadata().context("cannot inspect API key file")?;

    ensure!(
        metadata.is_file() && metadata.len() <= KEY_BYTES,
        "API key must be a regular file of at most 8 KiB"
    );

    let mut bytes = Vec::new();

    (&file)
        .take(KEY_BYTES)
        .read_to_end(&mut bytes)
        .context("cannot read API key file")?;

    ensure!(
        file.metadata()?.len() <= KEY_BYTES,
        "API key file exceeds 8 KiB"
    );

    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes.pop();
    }

    ensure!(
        !bytes.is_empty()
            && !bytes.iter().any(|b| b.is_ascii_control())
            && !std::str::from_utf8(&bytes).is_ok_and(|text| text.chars().any(char::is_control)),
        "API key must be nonempty without embedded control characters"
    );

    let mut value =
        HeaderValue::from_bytes(&bytes).context("API key is not a valid HTTP header value")?;

    value.set_sensitive(true);

    Ok(value)
}

pub fn resolve_interface(address: Ipv4Addr) -> anyhow::Result<u32> {
    ensure!(
        is_non_loopback_unicast(address),
        "LAN address must be non-loopback unicast"
    );
    let mut list = std::ptr::null_mut();

    // SAFETY: getifaddrs initializes list on success; it is freed exactly once below.
    if unsafe { libc::getifaddrs(&mut list) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot enumerate network interfaces");
    }

    let mut found = std::collections::BTreeMap::new();
    let mut current = list;

    // SAFETY: the linked list, socket addresses and C strings are owned by getifaddrs
    // until freeifaddrs. IPv4 casts are guarded by family and all pointers by null checks.
    unsafe {
        while !current.is_null() {
            let entry = &*current;

            if !entry.ifa_addr.is_null()
                && !entry.ifa_name.is_null()
                && (*entry.ifa_addr).sa_family as i32 == libc::AF_INET
            {
                let socket = &*(entry.ifa_addr.cast::<libc::sockaddr_in>());
                let local = Ipv4Addr::from(socket.sin_addr.s_addr.to_ne_bytes());

                if local == address {
                    let index = libc::if_nametoindex(entry.ifa_name);
                    found.insert(index, entry.ifa_flags);
                }
            }

            current = entry.ifa_next;
        }

        libc::freeifaddrs(list);
    }

    if found.len() != 1 {
        bail!("listen_address must identify exactly one local network interface");
    }

    let (index, flags) = found.pop_first().expect("one interface");

    ensure!(
        index != 0
            && flags & libc::IFF_UP as u32 != 0
            && flags & libc::IFF_MULTICAST as u32 != 0
            && flags & libc::IFF_LOOPBACK as u32 == 0,
        "configured interface must be up, non-loopback and multicast-capable"
    );

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    const CONFIG: &str = r#"
immich_url = "https://immich.example.com"
immich_api_key_file = "/run/secrets/key"
listen_address = "192.168.1.10:8200"
sort_locale = "pl"
server_uuid = "7B37DF49-B75D-4BCB-89A6-0C917A934643"
state_directory = "/var/lib/immich-dlna-proxy"
"#;

    #[test]
    fn cli_requires_config_and_exposes_only_config_help_and_version() {
        use clap::Parser;

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

    #[test]
    fn defaults_and_canonical_identity() {
        let settings = parse_settings(CONFIG).unwrap();
        assert_eq!(settings.friendly_name, "Immich");
        assert_eq!(settings.log_level, "info");

        assert_eq!(
            settings.server_uuid.to_string(),
            "7b37df49-b75d-4bcb-89a6-0c917a934643"
        );
    }

    #[test]
    fn invalid_settings_fail_without_echoing_source() {
        for (old, new) in [
            ("sort_locale = \"pl\"", ""),
            ("192.168.1.10:8200", "0.0.0.0:8200"),
            ("192.168.1.10:8200", "127.0.0.1:8200"),
            ("192.168.1.10:8200", "239.255.255.250:8200"),
            ("192.168.1.10:8200", "192.168.1.10:80"),
            ("192.168.1.10:8200", "[::1]:8200"),
            (
                "7B37DF49-B75D-4BCB-89A6-0C917A934643",
                "00000000-0000-0000-0000-000000000000",
            ),
            ("/run/secrets/key", "relative-key"),
            ("/var/lib/immich-dlna-proxy", "relative-state"),
        ] {
            assert!(parse_settings(&CONFIG.replace(old, new)).is_err(), "{new}");
        }

        for extra in [
            "unknown = \"private-value\"",
            "friendly_name = \"\"",
            "friendly_name = \"\\u0001\"",
            "log_level = \"INFO\"",
        ] {
            let error = parse_settings(&format!("{CONFIG}\n{extra}")).err().unwrap();
            assert!(!error.to_string().contains("private-value"));
        }

        assert!(
            parse_settings(&format!(
                "{CONFIG}\nfriendly_name = \"{}\"",
                "x".repeat(129)
            ))
            .is_err()
        );
    }

    #[test]
    fn load_rejects_invalid_url_and_locale_before_runtime_lookups() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");

        for (old, new, diagnostic) in [
            (
                "https://immich.example.com",
                "https://secret@example.com",
                "immich_url must be an absolute HTTP(S) URL without userinfo, query or fragment",
            ),
            (
                "https://immich.example.com",
                "relative",
                "invalid immich_url",
            ),
            (
                "sort_locale = \"pl\"",
                "sort_locale = \"not_a_locale!\"",
                "invalid sort_locale identifier",
            ),
        ] {
            fs::write(&path, CONFIG.replace(old, new)).unwrap();
            let error = Config::load(&path).err().unwrap();
            assert_eq!(error.to_string(), diagnostic);
        }
    }

    #[test]
    fn api_base_preserves_prefix_and_endpoint_resolution() {
        for (input, expected) in [
            ("https://example.com", "https://example.com/api/"),
            ("https://example.com/api///", "https://example.com/api/"),
            (
                "https://example.com/prefix/",
                "https://example.com/prefix/api/",
            ),
            (
                "http://example.com:8080/prefix/api",
                "http://example.com:8080/prefix/api/",
            ),
            (
                "https://example.com/a%20b/",
                "https://example.com/a%20b/api/",
            ),
        ] {
            let base = normalize_api_base(input).unwrap();
            assert!(is_normalized_api_base(&base));
            assert_eq!(base.as_str(), expected);
            assert_eq!(
                base.join("albums").unwrap().as_str(),
                format!("{expected}albums")
            );
        }

        for value in [
            "relative",
            "ftp://example.com",
            "https://u:p@example.com",
            "https://example.com?x",
            "https://example.com#x",
        ] {
            assert!(normalize_api_base(value).is_err());
        }

        for value in [
            "ftp://example.com/api/",
            "https://user@example.com/api/",
            "https://example.com/api/?query",
            "https://example.com/api/#fragment",
            "https://example.com/api",
            "https://example.com/not-api/",
        ] {
            assert!(!is_normalized_api_base(&value.parse().unwrap()));
        }
    }

    #[test]
    fn collation_is_polish_case_insensitive_and_canonically_equivalent() {
        let collator = collator("pl").unwrap();
        assert_eq!(collator.compare("C", "\u{106}"), Ordering::Less);
        assert_eq!(collator.compare("\u{106}", "D"), Ordering::Less);
        assert_eq!(collator.compare("Album", "album"), Ordering::Equal);
        assert_eq!(collator.compare("\u{106}", "C\u{301}"), Ordering::Equal);
        assert!(super::collator("").is_err());
    }

    #[test]
    fn key_is_bounded_sensitive_and_only_crlf_trimmed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("key");
        fs::write(&path, b" secret \r\n").unwrap();
        let value = read_key(&path).unwrap();
        assert_eq!(value.as_bytes(), b" secret ");
        assert!(value.is_sensitive());
        assert!(!format!("{value:?}").contains("secret"));

        for value in [
            b"".as_slice(),
            b"\r\n",
            b"bad\nkey",
            b"bad\tkey",
            b"bad\x7fkey",
            b"bad\xc2\x85key",
        ] {
            fs::write(&path, value).unwrap();
            assert!(read_key(&path).is_err());
        }

        fs::write(&path, vec![b'x'; KEY_BYTES as usize]).unwrap();
        assert!(read_key(&path).is_ok());
        fs::write(&path, vec![b'x'; KEY_BYTES as usize + 1]).unwrap();
        assert!(read_key(&path).is_err());
        assert!(read_key(&directory.path().join("absent")).is_err());
        assert!(read_key(directory.path()).is_err());
    }

    #[test]
    fn interface_does_not_fall_back_to_another_address() {
        assert!(resolve_interface(Ipv4Addr::LOCALHOST).is_err());
        assert!(resolve_interface(Ipv4Addr::UNSPECIFIED).is_err());
        assert!(resolve_interface(Ipv4Addr::new(192, 0, 2, 249)).is_err());
    }
}
