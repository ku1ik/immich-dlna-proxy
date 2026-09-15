//! Runnable fixture server and loopback-only integration tests for the production transport.

mod fixture_catalog;

#[path = "fixtures/seek.rs"]
mod seek;

use std::{
    future::Future,
    io::{self, SeekFrom},
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, ensure};
use axum::{Router, body::Body, extract::Request, response::Response};
use clap::Parser;
use futures_util::StreamExt;
use http::{HeaderValue, Method, StatusCode, header};
use immich_dlna_proxy::{
    catalog::{BrowseResult, Catalog, ObjectId, parse_id},
    config::{is_unicast, resolve_interface},
    eventing::Subscriptions,
    lifecycle::{self, SHUTDOWN_GRACE},
    media::MediaProxy,
    protocol::{BrowseArguments, Fault, Object},
    server::Server,
    ssdp::Discovery,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const SERVER_UUID: Uuid = Uuid::from_u128(0x30000000_0000_4000_8000_000000000001);
const SEEK_UUID: Uuid = Uuid::from_u128(0x30000000_0000_4000_8000_000000000003);
const JPEG_LABEL_UUID: Uuid = Uuid::from_u128(0x30000000_0000_4000_8000_000000000005);
const ALBUM_ART_UUID: Uuid = Uuid::from_u128(0x30000000_0000_4000_8000_000000000006);
const API_KEY: &str = "synthetic-fixture-only";
const FILES: [&str; 6] = [
    "original.jpg",
    "original-preview.jpg",
    "generated-display.jpg",
    "generated-preview.jpg",
    "video-original.mp4",
    "video-playback.mp4",
];

#[derive(Parser)]
#[command(about = "Serve a DLNA verification catalog from local fixture files")]
pub struct Arguments {
    #[arg(long, value_name = "IP:PORT", value_parser = listen_address)]
    pub listen: SocketAddrV4,
    #[arg(long, value_name = "DIRECTORY")]
    pub fixtures: PathBuf,
    /// Add a controlled HTTP byte-seek comparison on the next TCP port.
    #[arg(long)]
    pub seek_test: bool,
    /// Compare identical JPEG bytes labeled PNG versus JPEG in DIDL and HTTP.
    #[arg(long, conflicts_with = "seek_test")]
    pub jpeg_label_test: bool,
    /// Compare album artwork alone versus artwork plus a generic JPEG resource.
    #[arg(long, conflicts_with_all = ["seek_test", "jpeg_label_test"])]
    pub album_art_test: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixtureMode {
    Baseline,
    Seek,
    JpegLabels,
    AlbumArt,
}

fn listen_address(value: &str) -> anyhow::Result<SocketAddrV4> {
    let address: SocketAddrV4 = value.parse().context("expected an IPv4 socket address")?;

    ensure!(
        is_unicast(*address.ip()) && address.port() >= 1024,
        "listen must be concrete non-loopback unicast IPv4 with port 1024-65535"
    );

    Ok(address)
}

/// Validate local inputs and bind every service before starting discovery.
pub async fn run(arguments: Arguments) -> anyhow::Result<()> {
    listen_address(&arguments.listen.to_string())?;
    let interface = resolve_interface(*arguments.listen.ip())?;
    let shutdown = lifecycle::shutdown_signal()?;

    let mode = match (
        arguments.seek_test,
        arguments.jpeg_label_test,
        arguments.album_art_test,
    ) {
        (false, false, false) => FixtureMode::Baseline,
        (true, false, false) => FixtureMode::Seek,
        (false, true, false) => FixtureMode::JpegLabels,
        (false, false, true) => FixtureMode::AlbumArt,
        _ => anyhow::bail!("select only one fixture experiment"),
    };

    let bound = Bound::bind(arguments.listen, arguments.fixtures, mode).await?;

    let discovery = Discovery::bind(
        *arguments.listen.ip(),
        interface.index,
        bound.uuid,
        arguments.listen,
    )?;

    tracing::info!(listen = %arguments.listen, upstream = %bound.upstream.local_addr()?, uuid = %bound.uuid, ?mode, "fixture server ready");

    bound.run(Some(discovery), shutdown).await
}

async fn validate_fixtures(directory: &Path, mode: FixtureMode) -> anyhow::Result<()> {
    ensure!(
        tokio::fs::metadata(directory).await?.is_dir(),
        "fixtures must be an existing directory"
    );

    let files: &[&str] = if mode == FixtureMode::JpegLabels {
        &fixture_catalog::JPEG_LABEL_FILES
    } else {
        &FILES
    };

    for filename in files {
        let metadata = tokio::fs::metadata(directory.join(filename))
            .await
            .with_context(|| format!("cannot inspect fixture {filename}"))?;

        ensure!(
            metadata.is_file() && metadata.len() > 0,
            "fixture {filename} must be a nonempty regular file"
        );

        let mut file = File::open(directory.join(filename))
            .await
            .with_context(|| format!("cannot read fixture {filename}"))?;

        if mode == FixtureMode::JpegLabels {
            let mut signature = [0; 3];
            file.read_exact(&mut signature)
                .await
                .context("read JPEG fixture signature")?;
            ensure!(
                signature == [0xff, 0xd8, 0xff],
                "JPEG label fixtures must contain JPEG data"
            );
        }
    }

    Ok(())
}

struct FixtureCatalog(Vec<Object>);

impl Catalog for FixtureCatalog {
    fn system_update_id(&self) -> u32 {
        0
    }

    async fn browse(&self, args: BrowseArguments) -> Result<BrowseResult, Fault> {
        let missing = Fault { code: 701 };

        let id = match parse_id(&args.object_id)? {
            ObjectId::Root => "0".to_owned(),
            ObjectId::Album(album) => format!("album:{album}"),
            ObjectId::Item { album, asset } => format!("album:{album}:asset:{asset}"),
        };

        // Full appearance lookup checks album membership as well as both UUIDs.
        let object = self
            .0
            .iter()
            .find(|object| object.id == id)
            .ok_or(missing)?;

        tracing::info!(
            object = %id,
            metadata = args.metadata,
            resources_selected = args.filter.res(),
            artwork_selected = args.filter.art(),
            duration_selected = args.filter.duration(),
            "fixture Browse selection"
        );

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

        // All fixture child dates are known; default item order is oldest first.
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

/// Inclusive file interval. Malformed ranges are rejected; valid non-overlapping ranges get 416.
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

async fn upstream_request(
    directory: PathBuf,
    request: Request,
    stop: CancellationToken,
    mode: FixtureMode,
) -> Response {
    let (parts, _) = request.into_parts();

    let empty = |status| {
        Response::builder()
            .status(status)
            .body(Body::empty())
            .unwrap()
    };

    let headers = &parts.headers;

    if headers.get_all("x-api-key").iter().count() != 1
        || headers.get("x-api-key") != Some(&HeaderValue::from_static(API_KEY))
    {
        return empty(StatusCode::UNAUTHORIZED);
    }

    if parts.method != Method::GET && parts.method != Method::HEAD {
        let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);

        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));

        return response;
    }

    let Some((asset, endpoint)) = parts
        .uri
        .path()
        .strip_prefix("/api/assets/")
        .and_then(|path| path.split_once('/'))
    else {
        return empty(StatusCode::NOT_FOUND);
    };

    let mapping = if mode == FixtureMode::JpegLabels {
        fixture_catalog::jpeg_label_media_file(asset, endpoint, parts.uri.query())
    } else if mode == FixtureMode::AlbumArt {
        fixture_catalog::album_art_media_file(asset, endpoint, parts.uri.query())
    } else {
        fixture_catalog::media_file(asset, endpoint, parts.uri.query())
    };

    let Some((filename, mime)) = mapping else {
        return empty(StatusCode::NOT_FOUND);
    };

    let representation = match endpoint {
        "original" => "original",
        "video/playback" => "playback",
        _ if filename == "generated-display.jpg" => "display",
        _ => "preview",
    };

    tracing::info!(asset, representation, filename, method = %parts.method, range = ?headers.get(header::RANGE), "fixture media request");

    let result = async {
        // Only the finite mapping above supplies filenames; no URL component is joined to disk.
        let mut file = File::open(directory.join(filename)).await?;
        let length = file.metadata().await?.len();

        let range = if parts.method == Method::GET && headers.contains_key(header::RANGE) {
            let values = headers.get_all(header::RANGE);
            let mut values = values.iter();

            match (
                values.next().and_then(|value| value.to_str().ok()),
                values.next(),
            ) {
                (Some(value), None) => Some(file_range(value, length)),
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

        Ok(builder
            .body(Body::from_stream(stream.take_until(stop.cancelled_owned())))
            .unwrap())
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

struct Bound {
    http: TcpListener,
    upstream: TcpListener,
    directory: PathBuf,
    server: Server<FixtureCatalog>,
    subscriptions: Subscriptions,
    uuid: Uuid,
    seek: Option<(TcpListener, MediaProxy)>,
    mode: FixtureMode,
}

impl Bound {
    async fn bind(
        address: SocketAddrV4,
        directory: PathBuf,
        mode: FixtureMode,
    ) -> anyhow::Result<Self> {
        validate_fixtures(&directory, mode).await?;

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

        let (name, uuid, objects, seek) = if mode == FixtureMode::Seek {
            let port = address
                .port()
                .checked_add(1)
                .context("seek test requires an available TCP port after listen port")?;

            let seek_address = SocketAddrV4::new(*address.ip(), port);
            let listener = TcpListener::bind(seek_address)
                .await
                .context("bind fixture seek HTTP")?;

            (
                "DLNA Seek Matrix",
                SEEK_UUID,
                seek_objects(address, seek_address),
                Some((listener, media.clone())),
            )
        } else if mode == FixtureMode::JpegLabels {
            (
                "DLNA JPEG Label Test",
                JPEG_LABEL_UUID,
                fixture_catalog::jpeg_label_objects(address),
                None,
            )
        } else if mode == FixtureMode::AlbumArt {
            (
                "DLNA Album Art A-B",
                ALBUM_ART_UUID,
                fixture_catalog::album_art_objects(address),
                None,
            )
        } else {
            (
                "DLNA Fixture Baseline",
                SERVER_UUID,
                fixture_catalog::objects(address),
                None,
            )
        };

        let server = Server::new(
            name.into(),
            uuid,
            FixtureCatalog(objects),
            media,
            subscriptions.clone(),
        );

        Ok(Self {
            http,
            upstream,
            directory,
            server,
            subscriptions,
            uuid,
            seek,
            mode,
        })
    }

    async fn run(
        self,
        discovery: Option<Discovery>,
        shutdown: impl Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let stop = CancellationToken::new();
        let upstream_stop = CancellationToken::new();
        let subscriptions_stop = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let directory = self.directory;
        let mode = self.mode;
        let token = upstream_stop.clone();

        let router = Router::new().fallback(move |request| {
            let directory = directory.clone();
            let token = token.clone();

            async move {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Body::empty())
                        .unwrap(),

                    response = upstream_request(directory, request, token.clone(), mode) => response,
                }
            }
        });

        let token = upstream_stop.clone();

        let upstream = async move {
            axum::serve(self.upstream, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .context("fixture upstream failed")
        };

        spawn(&mut tasks, "upstream", upstream_stop.clone(), upstream);

        let http_stop = stop.clone();

        let http = async move {
            let primary = self
                .server
                .run(self.http, http_stop.clone(), SHUTDOWN_GRACE);

            if let Some((listener, media)) = self.seek {
                // The shared upstream must outlive admitted streams on both listeners.
                tokio::try_join!(primary, seek::run(listener, media, http_stop))?;
            } else {
                primary.await?;
            }

            Ok(())
        };

        spawn(&mut tasks, "HTTP", stop.clone(), http);

        spawn(
            &mut tasks,
            "subscriptions",
            subscriptions_stop.clone(),
            self.subscriptions.run(subscriptions_stop.clone()),
        );

        if let Some(discovery) = discovery {
            spawn(
                &mut tasks,
                "SSDP",
                stop.clone(),
                discovery.run(stop.clone()),
            );
        }

        supervise(tasks, stop, upstream_stop, subscriptions_stop, shutdown).await
    }
}

fn seek_objects(address: SocketAddrV4, seek_address: SocketAddrV4) -> Vec<Object> {
    let mut objects = fixture_catalog::objects(address);
    objects[0].title = "DLNA Seek Matrix".into();
    objects[1].title = "Seek Signaling A-D".into();

    let original = objects
        .iter()
        .find(|object| {
            object.id
                == format!(
                    "{}:asset:{}",
                    fixture_catalog::ALBUM_ID,
                    fixture_catalog::VIDEO_ID
                )
        })
        .expect("fixture original video")
        .clone();

    for (suffix, title, path, byte_seek) in [
        (4, "A - Control.mp4", "control", false),
        (5, "B - HTTP byte seek.mp4", "byte-seek", false),
        (6, "C - DIDL and HTTP byte seek.mp4", "both", true),
        (7, "D - DIDL byte seek.mp4", "didl-only", true),
    ] {
        let mut item = original.clone();
        item.id = format!(
            "{}:asset:20000000-0000-4000-8000-{suffix:012}",
            fixture_catalog::ALBUM_ID
        );
        item.title = title.into();
        item.resources.truncate(1);
        item.resources[0].uri = format!("http://{seek_address}/{path}");
        item.resources[0].byte_seek = byte_seek;
        objects.push(item);
    }

    objects
}

type TaskResult = (&'static str, bool, anyhow::Result<()>);

fn spawn(
    tasks: &mut JoinSet<TaskResult>,
    name: &'static str,
    stop: CancellationToken,
    future: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    tasks.spawn(async move {
        let result = future.await;

        (name, stop.is_cancelled(), result)
    });
}

async fn supervise(
    mut tasks: JoinSet<TaskResult>,
    stop: CancellationToken,
    upstream_stop: CancellationToken,
    subscriptions_stop: CancellationToken,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    tokio::pin!(shutdown);
    let mut stopping = false;
    let mut failure = None;
    let mut deadline = None;

    while !tasks.is_empty() {
        let expired = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;
            joined = tasks.join_next() => {
                let error = match joined.expect("nonempty essential tasks") {
                    Ok((name, expected, result)) => {
                        if name == "HTTP" {
                            upstream_stop.cancel();
                        }

                        match result {
                            Err(error) => Some(error.context(format!("{name} task failed"))),
                            Ok(()) if !expected => Some(anyhow::anyhow!("{name} task exited prematurely")),
                            Ok(()) => None,
                        }
                    }

                    Err(error) => Some(anyhow::anyhow!("essential task panicked or was cancelled: {error}")),
                };

                if let Some(error) = error {
                    failure.get_or_insert(error);
                    stop.cancel();
                    upstream_stop.cancel();
                    subscriptions_stop.cancel();
                    stopping = true;
                    let cleanup = Instant::now() + Duration::from_secs(1);
                    deadline = Some(deadline.map_or(cleanup, |deadline| deadline.min(cleanup)));
                }
            }

            _ = &mut shutdown, if !stopping => {
                stopping = true;
                stop.cancel();
                // Reservations can become owed after admission stops. Keep their scheduler
                // alive for the remainder of this one grace period, even if HTTP finishes early.
                deadline = Some(Instant::now() + SHUTDOWN_GRACE);
            }

            _ = expired => {
                stop.cancel();
                upstream_stop.cancel();
                subscriptions_stop.cancel();
                tasks.abort_all();

                while let Some(joined) = tasks.join_next().await {
                    match joined {
                        Err(error) if error.is_panic() => {
                            failure.get_or_insert_with(|| anyhow::anyhow!("essential task panicked: {error}"));
                        }

                        Ok((name, _, Err(error))) => {
                            failure.get_or_insert_with(|| error.context(format!("{name} task failed")));
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use fixture_catalog::{ALBUM_ID, GENERATED_JPEG_ID, ORIGINAL_JPEG_ID, VIDEO_ID};
    use immich_dlna_proxy::protocol::{self, Filter, Service};

    struct Harness {
        directory: tempfile::TempDir,
        address: SocketAddrV4,
        upstream: String,
        client: reqwest::Client,
        stop: CancellationToken,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl Harness {
        async fn start() -> Self {
            Self::start_mode(FixtureMode::Baseline).await
        }

        async fn start_mode(mode: FixtureMode) -> Self {
            Self::start_with_catalog(mode, None).await
        }

        async fn start_with_catalog(mode: FixtureMode, catalog: Option<FixtureCatalog>) -> Self {
            let directory = tempfile::tempdir().unwrap();

            let files: &[&str] = if mode == FixtureMode::JpegLabels {
                &fixture_catalog::JPEG_LABEL_FILES
            } else {
                &FILES
            };

            for (index, filename) in files.iter().enumerate() {
                let mut payload = bytes(index);

                if mode == FixtureMode::JpegLabels {
                    payload[..3].copy_from_slice(&[0xff, 0xd8, 0xff]);
                }

                tokio::fs::write(directory.path().join(filename), payload)
                    .await
                    .unwrap();
            }

            let mut bound = Bound::bind(
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
                directory.path().into(),
                mode,
            )
            .await
            .unwrap();

            let address =
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, bound.http.local_addr().unwrap().port());

            let upstream = format!("http://{}", bound.upstream.local_addr().unwrap());

            if let Some(catalog) = catalog {
                bound.server = Server::new(
                    "Wire\rfixture\r\nname".into(),
                    bound.uuid,
                    catalog,
                    MediaProxy::new(
                        format!("{upstream}/api/").parse().unwrap(),
                        HeaderValue::from_static(API_KEY),
                    )
                    .unwrap(),
                    bound.subscriptions.clone(),
                );
            }

            let stop = CancellationToken::new();
            let task = tokio::spawn(bound.run(None, stop.clone().cancelled_owned()));

            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap();

            Self {
                directory,
                address,
                upstream,
                client,
                stop,
                task,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.address)
        }

        async fn action(
            &self,
            service: Service,
            name: &str,
            arguments: &str,
        ) -> (StatusCode, String) {
            let (route, urn) = match service {
                Service::ContentDirectory => (
                    "content-directory",
                    "urn:schemas-upnp-org:service:ContentDirectory:1",
                ),

                Service::ConnectionManager => (
                    "connection-manager",
                    "urn:schemas-upnp-org:service:ConnectionManager:1",
                ),
            };

            let body = format!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:{name} xmlns:u=\"{urn}\">{arguments}</u:{name}></s:Body></s:Envelope>"
            );

            let response = self
                .client
                .post(self.url(&format!("/upnp/{route}/control")))
                .header(header::CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                .header("soapaction", format!("\"{urn}#{name}\""))
                .body(body)
                .send()
                .await
                .unwrap();

            let status = response.status();

            (status, response.text().await.unwrap())
        }

        async fn browse(
            &self,
            id: &str,
            flag: &str,
            filter: &str,
            start: u32,
            count: u32,
            sort: &str,
        ) -> (StatusCode, String) {
            let arguments = format!(
                "<ObjectID>{}</ObjectID><BrowseFlag>{flag}</BrowseFlag><Filter>{}</Filter><StartingIndex>{start}</StartingIndex><RequestedCount>{count}</RequestedCount><SortCriteria>{sort}</SortCriteria>",
                protocol::escape_text(id),
                protocol::escape_text(filter)
            );

            self.action(Service::ContentDirectory, "Browse", &arguments)
                .await
        }

        async fn finish(mut self) {
            self.stop.cancel();

            tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(2), &mut self.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.stop.cancel();
            self.task.abort();
        }
    }

    fn bytes(index: usize) -> Vec<u8> {
        (0..150_000 + index)
            .map(|offset| ((offset + index * 37) % 251) as u8)
            .collect()
    }

    #[tokio::test]
    async fn album_art_experiment_serves_identical_covers_before_entering_albums() {
        let harness = Harness::start_mode(FixtureMode::AlbumArt).await;
        let objects = fixture_catalog::album_art_objects(harness.address);
        let albums = [objects[1].clone(), objects[6].clone()];

        let description = harness
            .client
            .get(harness.url("/device.xml"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(description.contains("DLNA Album Art A-B"));
        assert!(description.contains(&ALBUM_ART_UUID.to_string()));
        assert_ne!(ALBUM_ART_UUID, SERVER_UUID);
        assert_ne!(ALBUM_ART_UUID, SEEK_UUID);
        assert_ne!(ALBUM_ART_UUID, JPEG_LABEL_UUID);

        for filter in [
            "",
            "upnp:albumArtURI",
            "res",
            "*",
            "res@resolution,res@nrAudioChannels,res@sampleFrequency,res@bitrate,dc:creator,res@dlna:cleartextSize,dc:date,upnp:genre,res,res@duration,res@size,upnp:albumArtURI,upnp:originalTrackNumber,upnp:album,upnp:artist,upnp:author",
        ] {
            assert_browse(
                harness
                    .browse("0", "BrowseDirectChildren", filter, 0, 100, "")
                    .await,
                &albums,
                2,
                filter,
            );
        }

        // Neither cover request depends on first browsing the album's children.
        for album in &albums {
            let uri = album.art.as_ref().unwrap();
            let response = harness.client.get(uri).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "image/jpeg");
            assert!(!response.headers().contains_key("contentfeatures.dlna.org"));
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes(3));

            let response = harness.client.head(uri).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "image/jpeg");
            assert_eq!(response.headers()[header::CONTENT_LENGTH], "150003");
            assert!(response.bytes().await.unwrap().is_empty());

            let response = harness
                .client
                .get(uri)
                .header(header::RANGE, "bytes=17-48")
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                "bytes 17-48/150003"
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), &bytes(3)[17..49]);
        }

        for group in objects[1..].chunks_exact(5) {
            assert_browse(
                harness
                    .browse(&group[0].id, "BrowseMetadata", "*", 0, 0, "")
                    .await,
                &group[..1],
                1,
                "*",
            );

            assert_browse(
                harness
                    .browse(&group[0].id, "BrowseDirectChildren", "*", 0, 0, "")
                    .await,
                &group[1..],
                4,
                "*",
            );

            let response = harness
                .client
                .get(&group[2].resources[0].uri)
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes(2));
        }

        harness.finish().await;
    }

    #[test]
    fn album_art_experiment_excludes_other_modes() {
        let args = [
            "fixture-server",
            "--listen",
            "192.0.2.10:8400",
            "--fixtures",
            "/tmp/images",
            "--album-art-test",
        ];

        let parsed = Arguments::try_parse_from(args).unwrap();
        assert!(parsed.album_art_test && !parsed.seek_test && !parsed.jpeg_label_test);

        for flag in ["--seek-test", "--jpeg-label-test"] {
            assert!(Arguments::try_parse_from(args.into_iter().chain([flag])).is_err());
        }
    }

    #[tokio::test]
    async fn jpeg_label_experiment_preserves_bytes_and_corrects_both_mime_locations() {
        let harness = Harness::start_mode(FixtureMode::JpegLabels).await;
        let objects = fixture_catalog::jpeg_label_objects(harness.address);

        let description = harness
            .client
            .get(harness.url("/device.xml"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(description.contains("DLNA JPEG Label Test"));
        assert!(description.contains(&JPEG_LABEL_UUID.to_string()));

        assert_browse(
            harness
                .browse(ALBUM_ID, "BrowseDirectChildren", "*", 0, 0, "")
                .await,
            &objects[2..],
            4,
            "*",
        );

        for (index, pair) in objects[2..].chunks_exact(2).enumerate() {
            let mut expected = bytes(index);
            expected[..3].copy_from_slice(&[0xff, 0xd8, 0xff]);

            for item in pair {
                let resource = &item.resources[0];
                let response = harness.client.get(&resource.uri).send().await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.headers()[header::CONTENT_TYPE], resource.mime);
                assert_eq!(response.content_length(), Some(expected.len() as u64));
                assert!(!response.headers().contains_key("contentfeatures.dlna.org"));
                assert_eq!(response.bytes().await.unwrap().as_ref(), expected);

                let response = harness.client.head(&resource.uri).send().await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(response.headers()[header::CONTENT_TYPE], resource.mime);
                assert_eq!(
                    response.headers()[header::CONTENT_LENGTH],
                    expected.len().to_string()
                );
                assert!(response.bytes().await.unwrap().is_empty());

                let response = harness
                    .client
                    .get(&resource.uri)
                    .header(header::RANGE, "bytes=17-48")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
                assert_eq!(response.headers()[header::CONTENT_TYPE], resource.mime);
                assert_eq!(
                    response.headers()[header::CONTENT_RANGE],
                    format!("bytes 17-48/{}", expected.len())
                );
                assert_eq!(response.bytes().await.unwrap().as_ref(), &expected[17..49]);
            }
        }

        let response = harness
            .client
            .get(harness.url(&format!("/media/assets/{ORIGINAL_JPEG_ID}/original")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        harness.finish().await;
    }

    #[tokio::test]
    async fn jpeg_label_experiment_validates_its_own_files_and_excludes_other_modes() {
        let directory = tempfile::tempdir().unwrap();
        assert!(
            validate_fixtures(directory.path(), FixtureMode::JpegLabels)
                .await
                .is_err()
        );

        for file in fixture_catalog::JPEG_LABEL_FILES {
            tokio::fs::write(directory.path().join(file), b"\xff\xd8\xfffixture")
                .await
                .unwrap();
        }

        validate_fixtures(directory.path(), FixtureMode::JpegLabels)
            .await
            .unwrap();
        assert!(
            validate_fixtures(directory.path(), FixtureMode::Baseline)
                .await
                .is_err()
        );
        tokio::fs::write(directory.path().join("image-2.jpg"), b"not JPEG")
            .await
            .unwrap();
        assert!(
            validate_fixtures(directory.path(), FixtureMode::JpegLabels)
                .await
                .is_err()
        );

        let args = [
            "fixture-server",
            "--listen",
            "192.0.2.10:8400",
            "--fixtures",
            "/tmp/images",
            "--jpeg-label-test",
        ];
        let parsed = Arguments::try_parse_from(args).unwrap();
        assert!(parsed.jpeg_label_test && !parsed.seek_test);
        assert!(Arguments::try_parse_from(args.into_iter().chain(["--seek-test"])).is_err());
    }

    #[test]
    fn seek_matrix_changes_only_the_requested_capabilities_and_resource_identity() {
        let objects = seek_objects(
            "192.0.2.10:8200".parse().unwrap(),
            "192.0.2.10:8201".parse().unwrap(),
        );
        assert_eq!(objects.len(), 10);
        assert_eq!(objects[0].title, "DLNA Seek Matrix");
        assert_eq!(objects[1].title, "Seek Signaling A-D");
        let ids: std::collections::HashSet<_> = objects.iter().map(|object| &object.id).collect();
        assert_eq!(ids.len(), objects.len());
        let mut a = objects[6].clone();
        let b = &objects[7];
        assert_eq!(a.title, "A - Control.mp4");
        assert_eq!(b.title, "B - HTTP byte seek.mp4");
        assert_ne!(a.id, b.id);
        assert_eq!(a.resources.len(), 1);
        assert_eq!(a.resources[0].uri, "http://192.0.2.10:8201/control");
        assert_eq!(b.resources[0].uri, "http://192.0.2.10:8201/byte-seek");
        a.id.clone_from(&b.id);
        a.title.clone_from(&b.title);
        a.resources[0].uri.clone_from(&b.resources[0].uri);
        assert_eq!(&a, b);

        for (index, path) in [(8, "both"), (9, "didl-only")] {
            let mut item = objects[index].clone();
            assert_ne!(item.id, a.id);
            assert!(item.resources[0].byte_seek);
            assert_eq!(
                item.resources[0].uri,
                format!("http://192.0.2.10:8201/{path}")
            );
            item.id.clone_from(&a.id);
            item.title.clone_from(&a.title);
            item.resources[0].uri.clone_from(&a.resources[0].uri);
            item.resources[0].byte_seek = false;
            assert_eq!(item, a);
        }

        let didl = protocol::didl(&objects[6..], &protocol::Filter::parse("*").unwrap()).unwrap();
        assert_eq!(didl.matches("http-get:*:video/mp4:*").count(), 2);
        assert_eq!(
            didl.matches("http-get:*:video/mp4:DLNA.ORG_OP=01").count(),
            2
        );
        assert_eq!(didl.matches("duration=\"0:00:30.000\"").count(), 4);
        assert_eq!(didl.matches("DLNA.ORG_").count(), 2);
        assert!(!didl.contains("size="));

        let cli = Arguments::try_parse_from([
            "fixture-server",
            "--listen",
            "192.0.2.10:8200",
            "--fixtures",
            "/tmp/fixtures",
            "--seek-test",
        ])
        .unwrap();

        assert!(cli.seek_test);
    }

    fn assert_browse(response: (StatusCode, String), objects: &[Object], total: u32, filter: &str) {
        let (status, xml) = response;
        assert_eq!(status, StatusCode::OK, "{xml}");

        assert!(
            xml.contains(&format!(
                "<NumberReturned>{}</NumberReturned>",
                objects.len()
            )),
            "{xml}"
        );

        assert!(
            xml.contains(&format!("<TotalMatches>{total}</TotalMatches>")),
            "{xml}"
        );

        assert!(xml.contains("<UpdateID>0</UpdateID>"), "{xml}");
        let didl = protocol::didl(objects, &Filter::parse(filter).unwrap()).unwrap();

        assert!(
            xml.contains(&format!(
                "<Result>{}</Result>",
                protocol::escape_text(&didl)
            )),
            "{xml}"
        );
    }

    #[tokio::test]
    async fn fixture_object_ids_follow_production_grammar_and_normalize_aliases() {
        let album = Uuid::parse_str("abcdef01-2345-4678-9abc-def012345678").unwrap();
        let asset = Uuid::parse_str("fedcba98-7654-4321-8abc-def012345678").unwrap();
        let album_id = format!("album:{album}");
        let item_id = format!("{album_id}:asset:{asset}");
        let mut objects = fixture_catalog::objects("127.0.0.1:8200".parse().unwrap());
        objects.truncate(3);
        objects[1].id = album_id.clone();
        objects[2].id = item_id.clone();
        objects[2].parent_id = album_id.clone();
        let catalog = FixtureCatalog(objects);

        let browse = |object_id| BrowseArguments {
            object_id,
            metadata: true,
            filter: Filter::parse("*").unwrap(),
            starting_index: 0,
            requested_count: 0,
            sort: None,
        };

        for id in [
            format!("album:{}", album.urn()),
            format!("{album_id}:asset:{}", asset.urn()),
            format!("album:{}:asset:{asset}", album.urn()),
            format!("ALBUM:{album}"),
            format!("asset:{asset}"),
            format!("{album_id}:ASSET:{asset}"),
            format!("{album_id}:extra"),
            format!("{item_id}:extra"),
            format!("{album_id}:asset:"),
            format!("album::asset:{asset}"),
            format!(" {album_id}"),
            format!("{item_id} "),
            "album:broken".into(),
            "0:extra".into(),
            "".into(),
        ] {
            assert_eq!(parse_id(&id), Err(Fault { code: 701 }), "{id}");

            assert_eq!(
                catalog.browse(browse(id.clone())).await.err(),
                Some(Fault { code: 701 }),
                "{id}"
            );
        }

        for (id, parsed, canonical) in [
            ("0".into(), ObjectId::Root, "0"),
            (album_id.clone(), ObjectId::Album(album), album_id.as_str()),
            (
                format!("album:{}", album.simple()),
                ObjectId::Album(album),
                album_id.as_str(),
            ),
            (
                format!("album:{{{}}}", album.to_string().to_uppercase()),
                ObjectId::Album(album),
                album_id.as_str(),
            ),
            (
                item_id.clone(),
                ObjectId::Item { album, asset },
                item_id.as_str(),
            ),
            (
                format!("album:{}:asset:{}", album.simple(), asset.simple()),
                ObjectId::Item { album, asset },
                item_id.as_str(),
            ),
            (
                format!(
                    "album:{}:asset:{{{}}}",
                    album.to_string().to_uppercase(),
                    asset.to_string().to_uppercase()
                ),
                ObjectId::Item { album, asset },
                item_id.as_str(),
            ),
        ] {
            assert_eq!(parse_id(&id), Ok(parsed), "{id}");
            let result = catalog.browse(browse(id.clone())).await.unwrap();
            assert_eq!(result.objects.len(), 1, "{id}");
            assert_eq!(result.objects[0].id, canonical, "{id}");
            assert_eq!(result.total_matches, 1);
        }
    }

    #[tokio::test]
    async fn browse_wire_decodes_soap_then_namespaced_didl_and_escape_layers() {
        use quick_xml::{NsReader, events::BytesStart, events::Event, name::ResolveResult};

        fn start(reader: &mut NsReader<&[u8]>, namespace: &str, name: &str) -> BytesStart<'static> {
            loop {
                match reader.read_resolved_event().unwrap() {
                    (resolved, Event::Start(element)) => {
                        let actual = match resolved {
                            ResolveResult::Bound(namespace) => namespace.as_ref().to_vec(),
                            ResolveResult::Unbound => Vec::new(),
                            ResolveResult::Unknown(prefix) => panic!("unbound prefix: {prefix:?}"),
                        };

                        assert_eq!(actual, namespace.as_bytes());
                        assert_eq!(element.local_name().as_ref(), name.as_bytes());

                        return element.into_owned();
                    }

                    (_, Event::Decl(_) | Event::End(_)) => {}

                    event => panic!("expected {name}, got {event:?}"),
                }
            }
        }

        fn text(reader: &mut NsReader<&[u8]>, namespace: &str, name: &str) -> String {
            let element = start(reader, namespace, name);

            content(reader, &element)
        }

        fn content(reader: &mut NsReader<&[u8]>, element: &BytesStart<'_>) -> String {
            let mut value = String::new();

            loop {
                match reader.read_event().unwrap() {
                    Event::Text(text) => value.push_str(&text.xml10_content().unwrap()),

                    Event::GeneralRef(reference) => {
                        let c = if let Some(c) = reference.resolve_char_ref().unwrap() {
                            c
                        } else {
                            match reference.decode().unwrap().as_ref() {
                                "amp" => '&',
                                "lt" => '<',
                                "gt" => '>',
                                "quot" => '"',
                                "apos" => '\'',
                                name => panic!("unexpected reference: {name}"),
                            }
                        };

                        value.push(c);
                    }

                    Event::End(end) => {
                        assert_eq!(end.name(), element.name());

                        return value;
                    }

                    event => panic!("unexpected text content: {event:?}"),
                }
            }
        }

        const SOAP: &str = "http://schemas.xmlsoap.org/soap/envelope/";
        const CD: &str = "urn:schemas-upnp-org:service:ContentDirectory:1";
        const DIDL: &str = "urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/";
        const DC: &str = "http://purl.org/dc/elements/1.1/";
        const UPNP: &str = "urn:schemas-upnp-org:metadata-1-0/upnp/";
        const DEVICE: &str = "urn:schemas-upnp-org:device-1-0";
        const TITLE: &str = "Rock & Roll <live>\r\"encore\"\r\n 'take 2'\t&amp;.jpg\n";
        const ART: &str = "http://127.0.0.1:8200/cover?size=preview&label=rock%26roll";
        const RESOURCE: &str = "http://127.0.0.1:8200/photo?edited=true&label=live%3C2%3E";

        let mut normalization =
            NsReader::from_str("<title>raw\rline\r\nnext&#13;&#xD;&amp;#13;</title>");

        assert_eq!(
            text(&mut normalization, "", "title"),
            "raw\nline\nnext\r\r&#13;"
        );

        let id = format!("{ALBUM_ID}:asset:{ORIGINAL_JPEG_ID}");
        let mut object = fixture_catalog::objects("127.0.0.1:8200".parse().unwrap()).remove(2);
        object.title = TITLE.into();
        object.art = Some(ART.into());
        object.resources[0].uri = RESOURCE.into();
        object.resources.truncate(1);

        let harness =
            Harness::start_with_catalog(FixtureMode::Baseline, Some(FixtureCatalog(vec![object])))
                .await;

        let (status, xml) = harness.browse(&id, "BrowseMetadata", "*", 0, 0, "").await;

        assert_eq!(status, StatusCode::OK, "{xml}");
        let mut soap = NsReader::from_str(&xml);
        start(&mut soap, SOAP, "Envelope");
        start(&mut soap, SOAP, "Body");
        start(&mut soap, CD, "BrowseResponse");
        let result = text(&mut soap, "", "Result");
        assert_eq!(text(&mut soap, "", "NumberReturned"), "1");
        assert_eq!(text(&mut soap, "", "TotalMatches"), "1");
        assert_eq!(text(&mut soap, "", "UpdateID"), "0");

        let mut didl = NsReader::from_str(&result);
        start(&mut didl, DIDL, "DIDL-Lite");
        let item = start(&mut didl, DIDL, "item");

        for (name, expected) in [
            ("id", id.as_str()),
            ("parentID", ALBUM_ID),
            ("restricted", "1"),
        ] {
            let attribute = item.try_get_attribute(name).unwrap().unwrap();
            assert_eq!(attribute.unescape_value().unwrap(), expected);
        }

        assert_eq!(item.attributes().count(), 3);
        assert_eq!(text(&mut didl, DC, "title"), TITLE);

        assert_eq!(
            text(&mut didl, UPNP, "class"),
            "object.item.imageItem.photo"
        );

        assert_eq!(text(&mut didl, DC, "date"), "2024-01-01");
        assert_eq!(text(&mut didl, UPNP, "albumArtURI"), ART);
        let resource = start(&mut didl, DIDL, "res");
        let info = resource.try_get_attribute("protocolInfo").unwrap().unwrap();
        assert_eq!(info.unescape_value().unwrap(), "http-get:*:image/jpeg:*");
        assert_eq!(resource.attributes().count(), 1);
        assert_eq!(content(&mut didl, &resource), RESOURCE);

        for reader in [&mut didl, &mut soap] {
            loop {
                match reader.read_event().unwrap() {
                    Event::End(_) => {}

                    Event::Eof => break,

                    event => panic!("unexpected trailing XML: {event:?}"),
                }
            }
        }

        let description = harness
            .client
            .get(harness.url("/device.xml"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let mut device = NsReader::from_str(&description);
        start(&mut device, DEVICE, "root");
        start(&mut device, DEVICE, "specVersion");
        assert_eq!(text(&mut device, DEVICE, "major"), "1");
        assert_eq!(text(&mut device, DEVICE, "minor"), "0");
        start(&mut device, DEVICE, "device");

        assert_eq!(
            text(&mut device, DEVICE, "deviceType"),
            "urn:schemas-upnp-org:device:MediaServer:1"
        );

        assert_eq!(
            text(&mut device, DEVICE, "friendlyName"),
            "Wire\rfixture\r\nname"
        );

        harness.finish().await;
    }

    #[tokio::test]
    async fn descriptions_browse_hierarchy_sort_paging_and_real_filters() {
        let harness = Harness::start().await;
        let objects = fixture_catalog::objects(harness.address);

        for (path, document) in [
            (
                "/device.xml",
                protocol::device_description("DLNA Fixture Baseline", SERVER_UUID),
            ),
            (
                "/upnp/content-directory/scpd.xml",
                protocol::scpd(Service::ContentDirectory).into(),
            ),
            (
                "/upnp/connection-manager/scpd.xml",
                protocol::scpd(Service::ConnectionManager).into(),
            ),
        ] {
            let response = harness.client.get(harness.url(path)).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.text().await.unwrap(), document);
            let response = harness.client.head(harness.url(path)).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                document.len().to_string()
            );

            assert!(response.bytes().await.unwrap().is_empty());
        }

        for object in &objects {
            assert_browse(
                harness
                    .browse(&object.id, "BrowseMetadata", "*", 0, 99, "-dc:date")
                    .await,
                std::slice::from_ref(object),
                1,
                "*",
            );
        }

        for sort in ["", "+dc:date", "-dc:date"] {
            assert_browse(
                harness
                    .browse("0", "BrowseDirectChildren", "*", 0, 0, sort)
                    .await,
                &objects[1..2],
                1,
                "*",
            );

            let mut children = objects[2..].to_vec();

            if sort == "-dc:date" {
                // The two video appearances share a date; their ID tie-break stays ascending.
                children = [4, 5, 3, 2].map(|index| objects[index].clone()).to_vec();
            }

            for (start, count) in [
                (0, 0),
                (1, 0),
                (1, 1),
                (2, 99),
                (3, 0),
                (4, 0),
                (u32::MAX, u32::MAX),
            ] {
                let expected: Vec<_> = children
                    .iter()
                    .skip(start as usize)
                    .take(if count == 0 {
                        usize::MAX
                    } else {
                        count as usize
                    })
                    .cloned()
                    .collect();

                for filter in [
                    "*",
                    "",
                    "dc:date",
                    "res",
                    "res@duration",
                    "upnp:albumArtURI",
                    "unknown:property",
                ] {
                    assert_browse(
                        harness
                            .browse(ALBUM_ID, "BrowseDirectChildren", filter, start, count, sort)
                            .await,
                        &expected,
                        4,
                        filter,
                    );
                }
            }
        }

        let compact_id = format!(
            "album:{}:asset:{}",
            Uuid::parse_str(ALBUM_ID.strip_prefix("album:").unwrap())
                .unwrap()
                .simple(),
            Uuid::parse_str(ORIGINAL_JPEG_ID).unwrap().simple()
        );

        assert_browse(
            harness
                .browse(&compact_id, "BrowseMetadata", "*", 0, 0, "")
                .await,
            &objects[2..3],
            1,
            "*",
        );

        for (id, flag, start, sort, filter, code) in [
            ("nonsense", "BrowseMetadata", 0, "", "*", 701),
            ("album:broken", "BrowseMetadata", 0, "", "*", 701),
            (
                "album:urn:uuid:10000000-0000-4000-8000-000000000001",
                "BrowseMetadata",
                0,
                "",
                "*",
                701,
            ),
            (
                "album:10000000-0000-4000-8000-000000000001:asset:urn:uuid:20000000-0000-4000-8000-000000000001",
                "BrowseMetadata",
                0,
                "",
                "*",
                701,
            ),
            (
                "album:10000000-0000-4000-8000-000000000099:asset:20000000-0000-4000-8000-000000000001",
                "BrowseMetadata",
                0,
                "",
                "*",
                701,
            ),
            (
                "album:10000000-0000-4000-8000-000000000001:asset:20000000-0000-4000-8000-000000000099",
                "BrowseMetadata",
                0,
                "",
                "*",
                701,
            ),
            (&objects[2].id, "BrowseDirectChildren", 0, "", "*", 710),
            ("0", "BrowseMetadata", 1, "", "*", 402),
            ("0", "BrowseMetadata", 0, "+dc:title", "*", 709),
            ("0", "BrowseDirectChildren", 0, "", "res@@size", 402),
        ] {
            let (status, xml) = harness.browse(id, flag, filter, start, 0, sort).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{xml}");

            assert!(
                xml.contains(&format!("<errorCode>{code}</errorCode>")),
                "{xml}"
            );
        }

        for (service, action, args, expected) in [
            (
                Service::ContentDirectory,
                "GetSystemUpdateID",
                "",
                "<Id>0</Id>",
            ),
            (
                Service::ContentDirectory,
                "GetSortCapabilities",
                "",
                "<SortCaps>dc:date</SortCaps>",
            ),
            (
                Service::ContentDirectory,
                "GetSearchCapabilities",
                "",
                "<SearchCaps></SearchCaps>",
            ),
            (
                Service::ConnectionManager,
                "GetProtocolInfo",
                "",
                "<Source>http-get:*:*:*</Source>",
            ),
            (
                Service::ConnectionManager,
                "GetCurrentConnectionIDs",
                "",
                "<ConnectionIDs>0</ConnectionIDs>",
            ),
            (
                Service::ConnectionManager,
                "GetCurrentConnectionInfo",
                "<ConnectionID>0</ConnectionID>",
                "<Direction>Output</Direction>",
            ),
        ] {
            let (status, xml) = harness.action(service, action, args).await;
            assert_eq!(status, StatusCode::OK, "{xml}");
            assert!(xml.contains(expected), "{xml}");
        }

        harness.finish().await;
    }

    #[tokio::test]
    async fn all_six_media_files_get_head_and_seek_through_production_proxy() {
        let harness = Harness::start().await;

        for (index, (asset, representation, endpoint, mime)) in [
            (ORIGINAL_JPEG_ID, "original", "original", "image/jpeg"),
            (
                ORIGINAL_JPEG_ID,
                "preview",
                "thumbnail?size=preview&edited=true",
                "image/jpeg",
            ),
            (
                GENERATED_JPEG_ID,
                "display",
                "thumbnail?size=fullsize&edited=true",
                "image/jpeg",
            ),
            (
                GENERATED_JPEG_ID,
                "preview",
                "thumbnail?size=preview&edited=true",
                "image/jpeg",
            ),
            (VIDEO_ID, "original", "original", "video/mp4"),
            (VIDEO_ID, "playback", "video/playback", "video/mp4"),
        ]
        .into_iter()
        .enumerate()
        {
            let bytes = bytes(index);
            let length = bytes.len();
            let local = harness.url(&format!("/media/assets/{asset}/{representation}"));
            let upstream = format!("{}/api/assets/{asset}/{endpoint}", harness.upstream);

            for (url, direct) in [(&local, false), (&upstream, true)] {
                for method in [Method::GET, Method::HEAD] {
                    let mut request = harness.client.request(method.clone(), url);

                    if direct {
                        request = request.header("x-api-key", API_KEY);
                    }

                    let response = request.send().await.unwrap();
                    assert_eq!(response.status(), StatusCode::OK, "{url}");
                    assert_eq!(response.headers()[header::CONTENT_TYPE], mime);

                    assert_eq!(
                        response.headers()[header::CONTENT_LENGTH],
                        length.to_string()
                    );

                    assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
                    assert!(!response.headers().contains_key("x-api-key"));
                    let body = response.bytes().await.unwrap();

                    assert_eq!(
                        &body[..],
                        if method == Method::HEAD {
                            &[]
                        } else {
                            &bytes[..]
                        }
                    );
                }

                for (range, start, end) in [
                    ("bytes=0-1023", 0, 1023),
                    ("bytes=65530-65550", 65530, 65550),
                    ("bytes=1024-", 1024, length - 1),
                    ("bytes=-1024", length - 1024, length - 1),
                    ("bytes=0-18446744073709551615", 0, length - 1),
                    ("bytes=-18446744073709551615", 0, length - 1),
                ] {
                    let mut request = harness.client.get(url).header(header::RANGE, range);

                    if direct {
                        request = request.header("x-api-key", API_KEY);
                    }

                    let response = request.send().await.unwrap();

                    assert_eq!(
                        response.status(),
                        StatusCode::PARTIAL_CONTENT,
                        "{url} {range}"
                    );

                    assert_eq!(response.headers()[header::CONTENT_TYPE], mime);

                    assert_eq!(
                        response.headers()[header::CONTENT_RANGE],
                        format!("bytes {start}-{end}/{length}")
                    );

                    assert_eq!(
                        response.headers()[header::CONTENT_LENGTH],
                        (end - start + 1).to_string()
                    );

                    assert_eq!(&response.bytes().await.unwrap()[..], &bytes[start..=end]);
                }

                for range in [
                    format!("bytes={length}-"),
                    "bytes=18446744073709551615-".into(),
                    "bytes=-0".into(),
                ] {
                    let mut request = harness.client.get(url).header(header::RANGE, &range);

                    if direct {
                        request = request.header("x-api-key", API_KEY);
                    }

                    let response = request.send().await.unwrap();

                    assert_eq!(
                        response.status(),
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "{url} {range}"
                    );

                    assert_eq!(
                        response.headers()[header::CONTENT_RANGE],
                        format!("bytes */{length}")
                    );

                    assert!(response.bytes().await.unwrap().is_empty());
                }
            }

            let response = harness
                .client
                .get(&local)
                .body("unexpected body")
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(response.bytes().await.unwrap().is_empty());
        }

        harness.finish().await;
    }

    #[tokio::test]
    async fn upstream_is_finite_authenticated_and_validates_files_before_binding() {
        let harness = Harness::start().await;

        let url = format!(
            "{}/api/assets/{ORIGINAL_JPEG_ID}/original",
            harness.upstream
        );

        for key in [None, Some("wrong-key")] {
            let mut request = harness.client.get(&url);

            if let Some(key) = key {
                request = request.header("x-api-key", key);
            }

            assert_eq!(
                request.send().await.unwrap().status(),
                StatusCode::UNAUTHORIZED
            );
        }

        for path in [
            format!("/api/assets/{GENERATED_JPEG_ID}/original"),
            format!("/api/assets/{ORIGINAL_JPEG_ID}/thumbnail?size=preview"),
            "/api/assets/../original.jpg".into(),
            "/api/server/version".into(),
        ] {
            let response = harness
                .client
                .get(format!("{}{path}", harness.upstream))
                .header("x-api-key", API_KEY)
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        let response = harness
            .client
            .post(&url)
            .header("x-api-key", API_KEY)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        for range in [
            "bytes=4-2",
            "bytes=0-1,3-4",
            "bytes=+1-2",
            "bytes=-",
            "items=0-1",
        ] {
            let response = harness
                .client
                .get(&url)
                .header("x-api-key", API_KEY)
                .header(header::RANGE, range)
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        // Even an occupied listen address must not hide invalid fixture input.
        for filename in FILES {
            let path = harness.directory.path().join(filename);
            tokio::fs::remove_file(&path).await.unwrap();
            let result = Bound::bind(
                harness.address,
                harness.directory.path().into(),
                FixtureMode::Baseline,
            )
            .await;
            assert!(result.err().unwrap().to_string().contains(filename));
            tokio::fs::write(&path, []).await.unwrap();

            assert!(
                validate_fixtures(harness.directory.path(), FixtureMode::Baseline)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(filename)
            );

            tokio::fs::write(&path, b"restored").await.unwrap();
        }

        assert!(
            validate_fixtures(
                &harness.directory.path().join("missing"),
                FixtureMode::Baseline
            )
            .await
            .is_err()
        );

        assert!(
            validate_fixtures(
                &harness.directory.path().join(FILES[0]),
                FixtureMode::Baseline
            )
            .await
            .is_err()
        );

        harness.finish().await;
    }

    #[test]
    fn cli_requires_a_concrete_lan_listener_and_fixture_directory() {
        let parse = |address: &str| {
            Arguments::try_parse_from(["fixture-server", "--listen", address, "--fixtures", "/tmp"])
        };

        assert!(parse("192.0.2.10:8200").is_ok());

        for address in [
            "0.0.0.0:8200",
            "127.0.0.1:8200",
            "239.255.255.250:8200",
            "255.255.255.255:8200",
            "192.0.2.10:0",
            "192.0.2.10:1023",
            "[::1]:8200",
        ] {
            assert!(parse(address).is_err(), "{address}");
        }

        assert!(Arguments::try_parse_from(["fixture-server", "--fixtures", "/tmp"]).is_err());

        assert!(
            Arguments::try_parse_from(["fixture-server", "--listen", "192.0.2.10:8200"]).is_err()
        );

        assert!(
            Arguments::try_parse_from([
                "fixture-server",
                "--listen",
                "192.0.2.10:8200",
                "--fixtures",
                "/tmp",
                "--uuid",
                "anything"
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn essential_success_error_and_panic_fail_and_cancel_siblings() {
        for outcome in ["success", "error", "panic"] {
            let stop = CancellationToken::new();
            let upstream_stop = CancellationToken::new();
            let subscriptions_stop = CancellationToken::new();
            let mut tasks = JoinSet::new();

            spawn(&mut tasks, "broken", stop.clone(), async move {
                match outcome {
                    "success" => Ok(()),
                    "error" => bail!("deliberate error"),
                    _ => panic!("deliberate panic"),
                }
            });

            for (name, token) in [
                ("HTTP", stop.clone()),
                ("upstream", upstream_stop.clone()),
                ("subscriptions", subscriptions_stop.clone()),
            ] {
                spawn(&mut tasks, name, token.clone(), async move {
                    token.cancelled().await;

                    Ok(())
                });
            }

            let result = tokio::time::timeout(
                Duration::from_secs(1),
                supervise(
                    tasks,
                    stop.clone(),
                    upstream_stop.clone(),
                    subscriptions_stop.clone(),
                    std::future::pending(),
                ),
            )
            .await
            .unwrap();

            assert!(result.is_err(), "{outcome}");
            assert!(stop.is_cancelled());
            assert!(upstream_stop.is_cancelled());
            assert!(subscriptions_stop.is_cancelled());
        }
    }

    #[tokio::test]
    async fn admitted_media_finishes_over_http_during_shutdown() {
        let harness = Harness::start().await;

        // Larger than socket buffers, but sparse on disk and never buffered by the server.
        let length = 32 * 1024 * 1024;

        let file = File::create(harness.directory.path().join("video-original.mp4"))
            .await
            .unwrap();

        file.set_len(length).await.unwrap();

        let mut response = harness
            .client
            .get(harness.url(&format!("/media/assets/{VIDEO_ID}/original")))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let mut received = response.chunk().await.unwrap().unwrap().len() as u64;
        assert!(received < length);
        harness.stop.cancel();

        while let Some(chunk) = response.chunk().await.unwrap() {
            assert!(chunk.iter().all(|byte| *byte == 0));
            received += chunk.len() as u64;
        }

        assert_eq!(received, length);
        harness.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_deadlines_abort_uncooperative_tasks_without_an_extra_grace() {
        struct Dropped(CancellationToken);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }

        for fatal in [false, true] {
            let stop = CancellationToken::new();
            let upstream_stop = CancellationToken::new();
            let subscriptions_stop = CancellationToken::new();
            let mut tasks = JoinSet::new();
            let mut dropped = Vec::new();

            for (name, token) in [
                ("HTTP", stop.clone()),
                ("upstream", upstream_stop.clone()),
                ("subscriptions", subscriptions_stop.clone()),
            ] {
                let finished = CancellationToken::new();
                let guard = Dropped(finished.clone());
                dropped.push(finished);

                spawn(&mut tasks, name, token, async move {
                    let _guard = guard;

                    std::future::pending().await
                });
            }

            if fatal {
                spawn(&mut tasks, "broken", stop.clone(), async {
                    bail!("deliberate failure")
                });
            }

            let started = Instant::now();

            let result = supervise(
                tasks,
                stop.clone(),
                upstream_stop.clone(),
                subscriptions_stop.clone(),
                async {},
            )
            .await;

            assert_eq!(result.is_err(), fatal);
            assert!(stop.is_cancelled());
            assert!(upstream_stop.is_cancelled());
            assert!(subscriptions_stop.is_cancelled());
            assert!(dropped.iter().all(CancellationToken::is_cancelled));

            let budget = if fatal {
                Duration::from_secs(1)
            } else {
                SHUTDOWN_GRACE
            };

            assert!(started.elapsed() <= budget);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stages_share_one_grace_deadline() {
        let stop = CancellationToken::new();
        let upstream_stop = CancellationToken::new();
        let subscriptions_stop = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let http_stop = stop.clone();
        let upstream_check = upstream_stop.clone();
        let (finish_http, http_finished) = tokio::sync::oneshot::channel();
        let (drained, check_drained) = tokio::sync::oneshot::channel();

        spawn(&mut tasks, "HTTP", stop.clone(), async move {
            http_stop.cancelled().await;
            http_finished.await.unwrap();
            assert!(!upstream_check.is_cancelled());
            drained.send(()).unwrap();

            Ok(())
        });

        let upstream_wait = upstream_stop.clone();

        spawn(&mut tasks, "upstream", upstream_stop.clone(), async move {
            upstream_wait.cancelled().await;
            check_drained.await.unwrap();

            Ok(())
        });

        let scheduler = subscriptions_stop.clone();

        spawn(
            &mut tasks,
            "subscriptions",
            subscriptions_stop.clone(),
            async move {
                scheduler.cancelled().await;

                Ok(())
            },
        );

        let started = Instant::now();

        let supervisor = tokio::spawn(supervise(
            tasks,
            stop.clone(),
            upstream_stop.clone(),
            subscriptions_stop.clone(),
            async {},
        ));

        stop.cancelled().await;
        assert!(!upstream_stop.is_cancelled());
        assert!(!subscriptions_stop.is_cancelled());
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!supervisor.is_finished());
        assert!(!upstream_stop.is_cancelled());
        finish_http.send(()).unwrap();
        upstream_stop.cancelled().await;
        supervisor.await.unwrap().unwrap();
        assert!(subscriptions_stop.is_cancelled());
        assert!(started.elapsed() <= SHUTDOWN_GRACE);
    }
}
