use std::{
    io::{self, SeekFrom},
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use axum::{Router, body::Body, extract::Request, response::Response};
use clap::Parser;
use http::{HeaderValue, Method, StatusCode, header};
use immich_dlna_proxy::{
    catalog::{BrowseResult, Catalog, ObjectId, parse_id},
    config::{is_non_loopback_unicast, resolve_interface},
    eventing::Subscriptions,
    media::MediaProxy,
    protocol::{BrowseArguments, Fault, Object},
    server::Server,
    ssdp::Discovery,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
};
use uuid::Uuid;

#[path = "catalog.rs"]
pub(super) mod catalog;

pub(super) const SERVER_UUID: Uuid = Uuid::from_u128(0x30000000_0000_4000_8000_000000000001);
const API_KEY: &str = "synthetic-fixture-only";
pub(super) const FILES: [&str; 6] = [
    "original.jpg",
    "original-preview.jpg",
    "generated-display.jpg",
    "generated-preview.jpg",
    "video-original.mp4",
    "video-playback.mp4",
];

#[derive(Parser)]
#[command(about = "Serve a DLNA verification catalog from local fixture files")]
pub(super) struct Arguments {
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

#[cfg_attr(test, allow(dead_code))]
pub(super) async fn run(arguments: Arguments) -> anyhow::Result<()> {
    let interface_index = resolve_interface(*arguments.listen.ip())?;
    let bound = Bound::bind(arguments.listen, arguments.fixtures).await?;
    let discovery = Discovery::bind(interface_index, SERVER_UUID, arguments.listen)?;

    tracing::info!(listen = %arguments.listen, upstream = %bound.upstream.local_addr()?, uuid = %SERVER_UUID, "fixture server ready");

    bound.run(Some(discovery)).await
}

async fn validate_fixtures(directory: &Path) -> anyhow::Result<()> {
    ensure!(
        tokio::fs::metadata(directory).await?.is_dir(),
        "fixtures must be an existing directory"
    );

    for filename in FILES {
        let metadata = tokio::fs::metadata(directory.join(filename))
            .await
            .with_context(|| format!("cannot inspect fixture {filename}"))?;

        ensure!(
            metadata.is_file() && metadata.len() > 0,
            "fixture {filename} must be a nonempty regular file"
        );
    }

    Ok(())
}

struct FixtureCatalog(Vec<Object>);

impl Catalog for FixtureCatalog {
    fn system_update_id(&self) -> u32 {
        0
    }

    async fn browse(&self, args: BrowseArguments) -> Result<BrowseResult, Fault> {
        let id = match parse_id(&args.object_id)? {
            ObjectId::Root => "0".to_owned(),
            ObjectId::Album(album) => format!("album:{album}"),
            ObjectId::Item { album, asset } => format!("album:{album}:asset:{asset}"),
        };

        let object = self
            .0
            .iter()
            .find(|object| object.id == id)
            .ok_or(Fault { code: 701 })?;

        tracing::info!(object = %id, metadata = args.metadata, resources_selected = args.filter.res(), "fixture Browse selection");

        if args.metadata {
            return Ok(BrowseResult {
                objects: vec![object.clone()],
                total_matches: 1,
                update_id: 0,
            });
        }

        if object.class.starts_with("object.item") {
            return Err(Fault { code: 710 });
        }

        let mut children: Vec<_> = self
            .0
            .iter()
            .filter(|object| object.parent_id == id)
            .collect();

        children.sort_by(|a, b| {
            let dates = a.date.cmp(&b.date);
            let dates = if args.sort == Some(true) {
                dates.reverse()
            } else {
                dates
            };

            dates.then_with(|| a.id.cmp(&b.id))
        });

        let total_matches = children.len() as u32;
        let count = if args.requested_count == 0 {
            usize::MAX
        } else {
            args.requested_count as usize
        };

        let objects = children
            .into_iter()
            .skip(args.starting_index as usize)
            .take(count)
            .cloned()
            .collect();

        Ok(BrowseResult {
            objects,
            total_matches,
            update_id: 0,
        })
    }
}

fn file_range(value: &str, length: u64) -> Result<(u64, u64), StatusCode> {
    let invalid = StatusCode::BAD_REQUEST;
    let unsatisfiable = StatusCode::RANGE_NOT_SATISFIABLE;
    let (unit, offsets) = value.split_once('=').ok_or(invalid)?;

    if !unit.eq_ignore_ascii_case("bytes") {
        return Err(invalid);
    }

    let (start, end) = offsets.split_once('-').ok_or(invalid)?;

    let decimal = |value: &str| {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid);
        }

        value.parse::<u64>().map_err(|_| invalid)
    };

    if start.is_empty() {
        let count = decimal(end)?;

        if count == 0 || length == 0 {
            return Err(unsatisfiable);
        }

        return Ok((length.saturating_sub(count), length - 1));
    }

    let start = decimal(start)?;

    let end = if end.is_empty() {
        u64::MAX
    } else {
        decimal(end)?
    };

    if start > end {
        return Err(invalid);
    }

    if start >= length {
        return Err(unsatisfiable);
    }

    Ok((start, end.min(length - 1)))
}

async fn upstream_request(directory: PathBuf, request: Request) -> Response {
    let (parts, _) = request.into_parts();

    let empty = |status| {
        Response::builder()
            .status(status)
            .body(Body::empty())
            .unwrap()
    };

    if parts.headers.get_all("x-api-key").iter().count() != 1
        || parts.headers.get("x-api-key") != Some(&HeaderValue::from_static(API_KEY))
    {
        return empty(StatusCode::UNAUTHORIZED);
    }

    if parts.method != Method::GET && parts.method != Method::HEAD {
        return empty(StatusCode::METHOD_NOT_ALLOWED);
    }

    let Some((asset, endpoint)) = parts
        .uri
        .path()
        .strip_prefix("/api/assets/")
        .and_then(|path| path.split_once('/'))
    else {
        return empty(StatusCode::NOT_FOUND);
    };

    let Some((filename, mime)) = catalog::media_file(asset, endpoint, parts.uri.query()) else {
        return empty(StatusCode::NOT_FOUND);
    };

    tracing::info!(asset, filename, method = %parts.method, range = ?parts.headers.get(header::RANGE), "fixture media request");

    let result = async {
        let mut file = File::open(directory.join(filename)).await?;
        let length = file.metadata().await?.len();

        let range = if parts.method == Method::GET {
            let mut values = parts.headers.get_all(header::RANGE).iter();

            match (values.next(), values.next()) {
                (None, None) => None,
                (Some(value), None) => Some(file_range(value.to_str().unwrap_or(""), length)),
                _ => Some(Err(StatusCode::BAD_REQUEST)),
            }
        } else {
            None
        };

        let mut builder = Response::builder()
            .header(header::CONTENT_TYPE, mime)
            .header(header::ACCEPT_RANGES, "bytes");

        let (start, count) = match range {
            Some(Ok((start, end))) => {
                builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{length}"),
                );

                (start, end - start + 1)
            }

            Some(Err(status)) => {
                if status == StatusCode::RANGE_NOT_SATISFIABLE {
                    builder = builder.header(header::CONTENT_RANGE, format!("bytes */{length}"));
                }

                return Ok::<_, io::Error>(
                    builder
                        .status(status)
                        .header(header::CONTENT_LENGTH, 0)
                        .body(Body::empty())
                        .unwrap(),
                );
            }

            None => (0, length),
        };

        builder = builder.header(header::CONTENT_LENGTH, count);

        if parts.method == Method::HEAD {
            return Ok(builder.body(Body::empty()).unwrap());
        }

        file.seek(SeekFrom::Start(start)).await?;

        let stream =
            futures_util::stream::try_unfold((file, count), |(mut file, remaining)| async move {
                if remaining == 0 {
                    return Ok::<_, io::Error>(None);
                }

                let mut bytes = vec![0; remaining.min(64 * 1024) as usize];
                let read = file.read(&mut bytes).await?;

                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "fixture truncated during transfer",
                    ));
                }

                bytes.truncate(read);

                Ok(Some((bytes, (file, remaining - read as u64))))
            });

        Ok(builder.body(Body::from_stream(stream)).unwrap())
    }
    .await;

    match result {
        Ok(response) => response,

        Err(_) => {
            tracing::error!(filename, "fixture file I/O failed");

            empty(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub(super) struct Bound {
    pub(super) http: TcpListener,
    upstream: TcpListener,
    directory: PathBuf,
    server: Server<FixtureCatalog>,
    subscriptions: Subscriptions,
}

impl Bound {
    pub(super) async fn bind(address: SocketAddrV4, directory: PathBuf) -> anyhow::Result<Self> {
        validate_fixtures(&directory).await?;

        let http = TcpListener::bind(address)
            .await
            .context("bind fixture HTTP")?;

        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind fixture upstream")?;

        let subscriptions = Subscriptions::new()?;
        let address = SocketAddrV4::new(*address.ip(), http.local_addr()?.port());

        let media = MediaProxy::new(
            format!("http://{}/api/", upstream.local_addr()?).parse()?,
            HeaderValue::from_static(API_KEY),
        )?;

        let server = Server::new(
            "DLNA Fixture Baseline".into(),
            SERVER_UUID,
            FixtureCatalog(catalog::objects(address)),
            media,
            subscriptions.clone(),
        );

        Ok(Self {
            http,
            upstream,
            directory,
            server,
            subscriptions,
        })
    }

    pub(super) async fn run(self, discovery: Option<Discovery>) -> anyhow::Result<()> {
        let directory = self.directory;

        let router = Router::new().fallback(move |request| {
            let directory = directory.clone();

            upstream_request(directory, request)
        });

        let upstream = async move {
            axum::serve(self.upstream, router)
                .await
                .context("fixture upstream failed")
        };

        let http = self.server.run(self.http);
        let subscriptions = self.subscriptions.run();

        let discovery = async move {
            match discovery {
                Some(discovery) => discovery.run().await,
                None => std::future::pending().await,
            }
        };

        let (name, result) = tokio::select! {
            result = upstream => ("upstream", result),
            result = http => ("HTTP", result),
            result = subscriptions => ("subscriptions", result),
            result = discovery => ("SSDP", result),
        };

        match result {
            Ok(()) => anyhow::bail!("{name} fixture service exited unexpectedly"),
            Err(error) => Err(error.context(format!("{name} fixture service failed"))),
        }
    }
}
