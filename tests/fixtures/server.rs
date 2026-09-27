use std::{
    io::{self, SeekFrom},
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use axum::{Router, body::Body, extract::Request, response::Response};
use http::{HeaderValue, Method, StatusCode, header};
use immich_dlna_proxy::{
    eventing::Subscriptions,
    media::{
        MediaProxy,
        Representation::{Display, Original, Playback, Preview},
    },
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

fn media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    use catalog::{GENERATED_JPEG_ID, ORIGINAL_JPEG_ID, VIDEO_ID};

    let representation = match (endpoint, query) {
        ("original", None | Some("")) => Original,
        ("video/playback", None | Some("")) => Playback,
        ("thumbnail", Some("size=fullsize&edited=true" | "edited=true&size=fullsize")) => Display,
        ("thumbnail", Some("size=preview&edited=true" | "edited=true&size=preview")) => Preview,
        _ => return None,
    };

    match (asset, representation) {
        (ORIGINAL_JPEG_ID, Original) => Some(("original.jpg", "image/jpeg")),
        (ORIGINAL_JPEG_ID, Preview) => Some(("original-preview.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, Display) => Some(("generated-display.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, Preview) => Some(("generated-preview.jpg", "image/jpeg")),
        (VIDEO_ID, Original) => Some(("video-original.mp4", "video/mp4")),
        (VIDEO_ID, Playback) => Some(("video-playback.mp4", "video/mp4")),
        _ => None,
    }
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

    let Some((filename, mime)) = media_file(asset, endpoint, parts.uri.query()) else {
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
    server: Server<catalog::FixtureCatalog>,
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
        let activity = immich_dlna_proxy::Activity::default();

        let media = MediaProxy::new(
            format!("http://{}/api/", upstream.local_addr()?).parse()?,
            HeaderValue::from_static(API_KEY),
            activity.clone(),
        )?;

        let server = Server::new(
            "DLNA Fixture Baseline".into(),
            SERVER_UUID,
            catalog::FixtureCatalog(catalog::objects(address)),
            media,
            subscriptions.clone(),
            activity,
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
        tracing::info!(listen = %self.http.local_addr()?, upstream = %self.upstream.local_addr()?, uuid = %SERVER_UUID, "fixture server ready");

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
