use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    response::Response,
};
use http::{HeaderMap, HeaderValue, Method};
use serde_json::{Value, json};
use std::{
    fs,
    net::Ipv4Addr,
    os::unix::fs::{MetadataExt, PermissionsExt},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

use crate::protocol::{self, Filter, Service};

#[test]
fn ids_and_date_ordering() {
    let album = Uuid::from_u128(100_000);
    let asset = Uuid::from_u128(0xabcdef);
    let id = format!("album:{album}:asset:{asset}");

    assert_eq!(
        parse_id(
            &id.to_uppercase()
                .replacen("ALBUM", "album", 1)
                .replacen("ASSET", "asset", 1)
        ),
        Ok(ObjectId::Item { album, asset })
    );

    assert_eq!(parse_id("0"), Ok(ObjectId::Root));

    assert_eq!(
        parse_id(&format!("album:{album}")),
        Ok(ObjectId::Album(album))
    );

    for id in [
        "",
        "00",
        "0:",
        "asset:bad",
        "album:bad",
        "album:0:asset:0",
        &format!("album:{album}:"),
        &format!("{id}:extra"),
        &format!("album:{album}:asset:"),
    ] {
        assert_eq!(parse_id(id), Err(Fault { code: 701 }));
    }

    let early = "2023-12-31T22:30:00Z".parse::<DateTime<Utc>>().unwrap();
    let late = "2024-01-01T00:30:00Z".parse::<DateTime<Utc>>().unwrap();
    let low = Uuid::from_u128(1);
    let high = Uuid::from_u128(2);

    for descending in [false, true] {
        assert_eq!(
            compare_dates(
                None,
                Some(&early),
                low,
                Some("2024-01-01"),
                None,
                high,
                descending
            ),
            Ordering::Greater
        );

        assert_eq!(
            compare_dates(
                Some("2024-01-01"),
                None,
                low,
                Some("2024-01-01"),
                Some(&late),
                high,
                descending
            ),
            Ordering::Greater
        );

        assert_eq!(
            compare_dates(None, None, low, None, None, high, descending),
            Ordering::Less
        );

        let expected = if descending {
            Ordering::Greater
        } else {
            Ordering::Less
        };

        assert_eq!(
            compare_dates(
                Some("2023-12-31"),
                Some(&late),
                high,
                Some("2024-01-01"),
                Some(&early),
                low,
                descending
            ),
            expected
        );

        assert_eq!(
            compare_dates(None, Some(&early), high, None, Some(&late), low, descending),
            expected
        );
    }
}

pub(super) struct Barrier {
    pub(super) entered: Notify,
    pub(super) release: Semaphore,
    pub(super) panic: bool,
}

impl Barrier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
            panic: false,
        })
    }

    async fn entered(&self) {
        timeout_at(
            Instant::now() + Duration::from_secs(3),
            self.entered.notified(),
        )
        .await
        .unwrap();
    }
}

#[derive(Default)]
struct Upstream {
    albums: Vec<Value>,
    contents: BTreeMap<Uuid, Vec<Value>>,
    requests: Vec<(String, Value)>,
    http_requests: Vec<(Method, String, HeaderMap)>,
    media: BTreeMap<String, (&'static str, &'static [u8])>,
    notification_gate: Option<Arc<Barrier>>,
    gates: BTreeMap<Scope, Arc<Barrier>>,
    outage: bool,
}

struct Fake {
    upstream: Arc<Mutex<Upstream>>,
    address: std::net::SocketAddr,
    notifications: mpsc::Receiver<(HeaderMap, String)>,
    task: JoinHandle<()>,
}

impl Fake {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = Arc::new(Mutex::new(Upstream::default()));
        let captured = upstream.clone();
        let (notify, notifications) = mpsc::channel(64);

        let router = Router::new().fallback(move |request: Request| {
            let upstream = captured.clone();
            let notify = notify.clone();

            async move {
                let (parts, body) = request.into_parts();
                let body = to_bytes(body, 8192).await.unwrap();

                if parts.method.as_str() == "NOTIFY" {
                    let gate = upstream.lock().unwrap().notification_gate.clone();

                    let _ = notify
                        .send((parts.headers, String::from_utf8(body.to_vec()).unwrap()))
                        .await;

                    if let Some(gate) = gate {
                        gate.entered.notify_one();
                        gate.release.acquire().await.unwrap().forget();
                    }

                    return Response::new(Body::empty());
                }

                let media = {
                    let mut upstream = upstream.lock().unwrap();

                    upstream.http_requests.push((
                        parts.method.clone(),
                        parts.uri.to_string(),
                        parts.headers.clone(),
                    ));

                    upstream.media.get(&parts.uri.to_string()).copied()
                };

                if let Some((mime, bytes)) = media {
                    let partial = parts.method == Method::GET
                        && parts
                            .headers
                            .get("range")
                            .is_some_and(|value| value == "bytes=2-5");

                    let mut response = Response::builder()
                        .header("content-type", mime)
                        .header("accept-ranges", "bytes")
                        .header("etag", "\"fixture\"");

                    let payload = if partial {
                        response = response
                            .status(206)
                            .header("content-range", format!("bytes 2-5/{}", bytes.len()));

                        &bytes[2..6]
                    } else {
                        bytes
                    };

                    return response
                        .header("content-length", payload.len())
                        .body(if parts.method == Method::HEAD {
                            Body::empty()
                        } else {
                            Body::from(payload)
                        })
                        .unwrap();
                }

                let body = if body.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body).unwrap()
                };

                let (value, barrier, outage) = {
                    let mut upstream = upstream.lock().unwrap();

                    upstream
                        .requests
                        .push((parts.uri.path().to_string(), body.clone()));

                    let scope = match parts.uri.path() {
                        "/api/server/version" => None,
                        "/api/albums" => Some(Scope::Root),

                        "/api/search/metadata" => Some(Scope::Album(
                            serde_json::from_value(body["albumIds"][0].clone()).unwrap(),
                        )),

                        path => panic!("unexpected upstream path {path}"),
                    };

                    let value = match scope {
                        None => json!({"major": 3, "minor": 1, "patch": 0, "prerelease": null}),
                        Some(Scope::Root) => json!(upstream.albums),

                        Some(Scope::Album(id)) => {
                            let mut items = upstream.contents.get(&id).cloned().unwrap_or_default();

                            if body["isEncoded"] == true {
                                items.retain(|item| item["type"] == "VIDEO");
                            }

                            json!({"assets": {"items": items, "nextPage": null}})
                        }
                    };

                    (
                        value,
                        scope.and_then(|scope| upstream.gates.get(&scope).cloned()),
                        upstream.outage,
                    )
                };

                if let Some(barrier) = barrier {
                    barrier.entered.notify_one();
                    barrier.release.acquire().await.unwrap().forget();
                }

                Response::builder()
                    .status(if outage { 503 } else { 200 })
                    .header("content-type", "application/json")
                    .body(Body::from(value.to_string()))
                    .unwrap()
            }
        });

        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            upstream,
            address,
            notifications,
            task,
        }
    }

    fn config(&self, directory: &std::path::Path) -> Config {
        Config {
            api_base: format!("http://{}/api/", self.address).parse().unwrap(),
            api_key: HeaderValue::from_static("fake-key"),
            listen_address: "192.0.2.1:8200".parse().unwrap(),
            friendly_name: "Photos & videos".into(),
            collator: crate::config::collator("pl").unwrap(),
            server_uuid: Uuid::from_u128(999),
            state_directory: directory.to_owned(),
            log_level: tracing::Level::INFO,
            interface_index: 1,
        }
    }

    fn calls(&self, path: &str) -> usize {
        self.upstream
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(actual, _)| actual == path)
            .count()
    }

    fn subscribe(&self, library: &Library) {
        let mut headers = HeaderMap::new();
        headers.insert("nt", HeaderValue::from_static("upnp:event"));

        headers.insert(
            "callback",
            format!("<http://{}/events>", self.address).parse().unwrap(),
        );

        let response = library.inner.events.request(
            Service::ContentDirectory,
            Ipv4Addr::LOCALHOST,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers,
        );

        assert_eq!(response.status(), 200);
    }

    async fn event(&mut self, id: u32) {
        let (_, body) = timeout_at(
            Instant::now() + Duration::from_secs(3),
            self.notifications.recv(),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            body.contains(&format!("<SystemUpdateID>{id}</SystemUpdateID>")),
            "{body}"
        );
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    library: Library,
    fake: Fake,
    directory: TempDir,
}

impl Fixture {
    async fn new(albums: usize) -> Self {
        let fake = Fake::new().await;

        fake.upstream.lock().unwrap().albums =
            (1..=albums).map(|id| album(id as u128, "Album")).collect();

        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let config = fake.config(directory.path());
        let (store, mut ledger) = Store::open(directory.path(), config.server_uuid).unwrap();
        ledger.restart();
        store.persist(ledger.clone()).await;

        let library = Library::new(config, store, ledger, Subscriptions::new().unwrap()).unwrap();

        Self {
            library,
            fake,
            directory,
        }
    }

    fn run(&self) -> JoinHandle<Result<()>> {
        let library = self.library.clone();

        tokio::spawn(async move { library.run().await })
    }

    fn expire(&self, scope: Scope) {
        let mut state = self.library.inner.state.lock().unwrap();

        match scope {
            Scope::Root => state.cache.root.as_mut().unwrap().completed -= FRESHNESS,

            Scope::Album(id) => state.cache.albums.get_mut(&id).unwrap().completed -= FRESHNESS,
        }
    }

    fn disk(&self) -> (u64, Ledger) {
        let path = self.directory.path().join("revisions.json");

        (
            fs::metadata(&path).unwrap().ino(),
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap(),
        )
    }

    async fn abort(&self, task: JoinHandle<Result<()>>) {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert!(self.library.inner.state.lock().unwrap().flights.is_empty());

        assert_eq!(self.library.inner.permits.available_permits(), REFRESHES);
    }
}

fn album(id: u128, title: &str) -> Value {
    json!({"id": Uuid::from_u128(id), "albumName": title, "createdAt": "2024-01-01T00:00:00Z"})
}

fn item(id: u128, capture: Option<&str>, date: Option<&str>) -> Value {
    json!({
        "id": Uuid::from_u128(id), "type": "IMAGE", "visibility": "timeline",
        "isTrashed": false, "isEdited": false, "originalFileName": format!("Photo {id}"),
        "originalMimeType": "image/jpeg", "fileCreatedAt": capture, "localDateTime": date
    })
}

fn action(id: &str, metadata: bool, start: u32, count: u32, sort: Option<bool>) -> BrowseArguments {
    BrowseArguments {
        object_id: id.into(),
        metadata,
        starting_index: start,
        requested_count: count,
        sort,
        filter: Filter::parse("*").unwrap(),
    }
}

fn children(id: u128) -> BrowseArguments {
    action(&format!("album:{}", Uuid::from_u128(id)), false, 0, 0, None)
}

fn browse(library: &Library, query: BrowseArguments) -> JoinHandle<Result<BrowseResult, Fault>> {
    let library = library.clone();

    tokio::spawn(async move { library.browse(query).await })
}

async fn fault(task: JoinHandle<Result<BrowseResult, Fault>>, code: u16) {
    let result = timeout_at(Instant::now() + Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.err(), Some(Fault { code }));
}

#[tokio::test]
async fn real_http_catalog_events_durability_and_media_share_one_server() {
    use crate::{media::MediaProxy, server::Server};
    use quick_xml::{Reader, events::Event};

    async fn soap(client: &reqwest::Client, base: &str, name: &str, args: &str) -> (u16, String) {
        let body = format!(
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:{name} xmlns:u=\"{}\">{args}</u:{name}></s:Body></s:Envelope>",
            protocol::CONTENT_DIRECTORY,
        );

        let response = client
            .post(format!("{base}/upnp/content-directory/control"))
            .header("content-type", "text/xml; charset=\"utf-8\"")
            .header(
                "soapaction",
                format!("\"{}#{name}\"", protocol::CONTENT_DIRECTORY),
            )
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.headers()["server"], crate::server_header());
        assert!(response.headers().contains_key("ext"));

        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/xml")
        );

        let status = response.status().as_u16();
        let body = response.text().await.unwrap();
        assert!(!body.contains("fake-key"));

        (status, body)
    }

    fn text(xml: &str, name: &str) -> String {
        let mut reader = Reader::from_str(xml);

        loop {
            match reader.read_event().unwrap() {
                Event::Start(element) if element.local_name().as_ref() == name.as_bytes() => {
                    let raw = reader.read_text(element.name()).unwrap();

                    return quick_xml::escape::unescape(&raw).unwrap().into_owned();
                }

                Event::Eof => panic!("missing XML element {name}"),
                _ => {}
            }
        }
    }

    fn objects(xml: &str) -> Vec<(String, String)> {
        let mut reader = Reader::from_str(xml);
        let mut objects = Vec::new();

        loop {
            match reader.read_event().unwrap() {
                Event::Start(element)
                    if matches!(element.local_name().as_ref(), b"item" | b"container") =>
                {
                    let id = element
                        .try_get_attribute("id")
                        .unwrap()
                        .unwrap()
                        .unescape_value()
                        .unwrap()
                        .into_owned();

                    let body = reader.read_text(element.name()).unwrap().into_owned();
                    objects.push((id, body));
                }

                Event::Eof => return objects,
                _ => {}
            }
        }
    }

    fn resources(xml: &str) -> Vec<(String, String, Option<String>)> {
        let mut reader = Reader::from_str(xml);
        let mut resources = Vec::new();

        loop {
            match reader.read_event().unwrap() {
                Event::Start(element) if element.local_name().as_ref() == b"res" => {
                    let info = element
                        .try_get_attribute("protocolInfo")
                        .unwrap()
                        .unwrap()
                        .unescape_value()
                        .unwrap()
                        .into_owned();

                    let duration = element
                        .try_get_attribute("duration")
                        .unwrap()
                        .map(|value| value.unescape_value().unwrap().into_owned());

                    let uri = reader.read_text(element.name()).unwrap();

                    resources.push((
                        quick_xml::escape::unescape(&uri).unwrap().into_owned(),
                        info,
                        duration,
                    ));
                }

                Event::Eof => return resources,
                _ => {}
            }
        }
    }

    let fake = Fake::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let album_id = Uuid::from_u128(1);
    let image_id = Uuid::from_u128(10);
    let video_id = Uuid::from_u128(20);
    let callback_gate = Barrier::new();

    {
        let mut upstream = fake.upstream.lock().unwrap();
        upstream.albums = vec![album(1, "Family & friends")];

        let mut edited = item(
            10,
            Some("2024-01-01T01:00:00Z"),
            Some("2024-01-01T01:00:00Z"),
        );

        edited["isEdited"] = json!(true);
        edited["originalFileName"] = json!("Edited <sun> & snow.jpg");
        edited["originalMimeType"] = json!("image/png");

        let mut video = item(
            20,
            Some("2024-01-02T01:00:00Z"),
            Some("2024-01-02T01:00:00Z"),
        );

        video["type"] = json!("VIDEO");
        video["originalMimeType"] = json!("video/quicktime");
        video["duration"] = json!(1234);
        upstream.contents.insert(album_id, vec![edited, video]);
        upstream.notification_gate = Some(callback_gate.clone());

        upstream.media = BTreeMap::from([
            (
                format!("/api/assets/{image_id}/thumbnail?size=fullsize&edited=true"),
                ("image/jpeg", b"edited-jpeg".as_slice()),
            ),
            (
                format!("/api/assets/{video_id}/original"),
                ("video/quicktime", b"original-video".as_slice()),
            ),
            (
                format!("/api/assets/{video_id}/video/playback"),
                ("video/mp4", b"0123456789".as_slice()),
            ),
        ]);
    }

    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let mut config = fake.config(directory.path());

    config.listen_address = match address {
        std::net::SocketAddr::V4(address) => address,
        _ => unreachable!("literal IPv4 bind"),
    };

    let (store, mut ledger) = Store::open(directory.path(), config.server_uuid).unwrap();
    ledger.restart();
    store.persist(ledger.clone()).await;
    let media = MediaProxy::new(config.api_base.clone(), config.api_key.clone()).unwrap();
    let events = Subscriptions::new().unwrap();
    let name = config.friendly_name.clone();
    let uuid = config.server_uuid;

    let library = Library::new(config, store, ledger, events.clone()).unwrap();

    let mut fixture = Fixture {
        library,
        fake,
        directory,
    };

    let catalog_task = fixture.run();
    let events_task = tokio::spawn(events.clone().run());
    let server = Server::new(name, uuid, fixture.library.clone(), media, events);
    let server_task = tokio::spawn(server.run(listener));

    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let (status, local) = soap(&client, &base, "GetSystemUpdateID", "").await;
    assert_eq!(status, 200);
    assert_eq!(text(&local, "Id"), "1");
    assert_eq!(fixture.disk().1.system_update_id, 1);

    assert!(
        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .http_requests
            .is_empty()
    );

    let subscription = client
        .request(
            Method::from_bytes(b"SUBSCRIBE").unwrap(),
            format!("{base}/upnp/content-directory/events"),
        )
        .header("nt", "upnp:event")
        .header(
            "callback",
            format!("<http://{}/events>", fixture.fake.address),
        )
        .header("timeout", "Second-60")
        .send()
        .await
        .unwrap();

    assert_eq!(subscription.status(), 200);
    assert_eq!(subscription.headers()["timeout"], "Second-60");
    assert!(!subscription.headers().contains_key("connection"));
    let sid = subscription.headers()["sid"].clone();
    assert!(subscription.bytes().await.unwrap().is_empty());

    let (initial, body) =
        tokio::time::timeout(Duration::from_secs(3), fixture.fake.notifications.recv())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(initial["seq"], "0");
    assert_eq!(initial["sid"], sid);
    assert_eq!(initial["nt"], "upnp:event");
    assert_eq!(initial["nts"], "upnp:propchange");
    assert_eq!(text(&body, "SystemUpdateID"), "1");
    assert!(!initial.contains_key("x-api-key"));

    assert!(
        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .http_requests
            .is_empty()
    );

    let (status, root) = soap(
        &client,
        &base,
        "Browse",
        "<ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/>",
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(text(&root, "NumberReturned"), "1");
    assert_eq!(text(&root, "TotalMatches"), "1");
    assert_eq!(text(&root, "UpdateID"), "2");
    assert!(root.contains("Family &amp;amp; friends"));
    let albums = objects(&text(&root, "Result"));
    assert_eq!(albums[0].0, format!("album:{album_id}"));
    assert_eq!(text(&albums[0].1, "title"), "Family & friends");
    assert_eq!(text(&albums[0].1, "class"), "object.container.album");
    assert_eq!(fixture.disk().1.system_update_id, 2);
    assert_eq!(fixture.disk().1.albums[&album_id].update_id, 0);

    let (status, metadata) = soap(
        &client,
        &base,
        "Browse",
        &format!("<ObjectID>album:{album_id}</ObjectID><BrowseFlag>BrowseMetadata</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/>"),
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(text(&metadata, "UpdateID"), "0");
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);

    let children = format!(
        "<ObjectID>album:{album_id}</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter> dc:date, res@duration </Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/>"
    );

    let (status, listing) = soap(&client, &base, "Browse", &children).await;
    assert_eq!(status, 200);
    assert_eq!(text(&listing, "NumberReturned"), "2");
    assert_eq!(text(&listing, "TotalMatches"), "2");
    assert_eq!(text(&listing, "UpdateID"), "1");
    assert!(listing.contains("Edited &amp;lt;sun&amp;gt; &amp;amp; snow.jpg"));
    let didl = text(&listing, "Result");
    assert!(!didl.contains("albumArtURI"));
    let items = objects(&didl);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].0, format!("album:{album_id}:asset:{image_id}"));
    assert_eq!(text(&items[0].1, "title"), "Edited <sun> & snow.jpg");
    assert_eq!(text(&items[0].1, "date"), "2024-01-01");
    assert_eq!(text(&items[0].1, "class"), "object.item.imageItem.photo");
    let images = resources(&items[0].1);

    assert_eq!(
        images,
        vec![
            (
                format!("{base}/media/assets/{image_id}/display"),
                "http-get:*:image/jpeg:*".into(),
                None
            ),
            (
                format!("{base}/media/assets/{image_id}/preview"),
                "http-get:*:image/jpeg:*".into(),
                None
            ),
        ]
    );

    assert_eq!(text(&items[1].1, "class"), "object.item.videoItem");
    let videos = resources(&items[1].1);

    assert_eq!(
        videos,
        vec![
            (
                format!("{base}/media/assets/{video_id}/original"),
                "http-get:*:video/quicktime:DLNA.ORG_OP=01".into(),
                Some("0:00:01.234".into())
            ),
            (
                format!("{base}/media/assets/{video_id}/playback"),
                "http-get:*:video/mp4:DLNA.ORG_OP=01".into(),
                None
            ),
        ]
    );

    let published = fixture.disk();
    assert_eq!(published.1.system_update_id, 3);
    assert_eq!(published.1.albums[&album_id].update_id, 1);
    assert!(published.1.albums[&album_id].contents_digest.is_some());

    {
        let upstream = fixture.fake.upstream.lock().unwrap();
        assert_eq!(upstream.requests.len(), 4);
        assert_eq!(upstream.requests[0].0, "/api/server/version");
        assert_eq!(upstream.requests[1].0, "/api/albums");

        for (_, body) in &upstream.requests[2..] {
            assert_eq!(body["albumIds"], json!([album_id]));
            assert_eq!(body["page"], 1);
            assert_eq!(body["size"], 1000);
            assert_eq!(body["withDeleted"], false);
            assert_eq!(body["withExif"], false);
        }

        assert!(upstream.requests[2].1.get("isEncoded").is_none());
        assert_eq!(upstream.requests[3].1["type"], "VIDEO");
        assert_eq!(upstream.requests[3].1["isEncoded"], true);
    }

    let response = client.get(&images[0].0).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "image/jpeg");
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"edited-jpeg");
    let response = client.head(&videos[0].0).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-length"], "14");
    assert_eq!(response.headers()["content-type"], "video/quicktime");
    assert!(response.bytes().await.unwrap().is_empty());

    let response = client
        .get(&videos[1].0)
        .header("range", "bytes=2-5")
        .header("if-range", "\"fixture\"")
        .header("authorization", "caller-secret")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["content-range"], "bytes 2-5/10");
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    assert_eq!(response.headers()["content-length"], "4");
    assert!(!response.headers().contains_key("contentfeatures.dlna.org"));
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"2345");
    assert_eq!(fixture.disk(), published);

    {
        let upstream = fixture.fake.upstream.lock().unwrap();

        assert_eq!(
            upstream.requests.len(),
            4,
            "media must not refresh the catalog"
        );

        assert_eq!(upstream.http_requests.len(), 7);
        assert_eq!(upstream.http_requests[4].0, Method::GET);

        assert!(
            upstream.http_requests[4]
                .1
                .ends_with("thumbnail?size=fullsize&edited=true")
        );

        assert_eq!(upstream.http_requests[5].0, Method::HEAD);
        assert!(upstream.http_requests[5].1.ends_with("/original"));
        assert_eq!(upstream.http_requests[6].2["range"], "bytes=2-5");
        assert_eq!(upstream.http_requests[6].2["if-range"], "\"fixture\"");

        for (_, _, headers) in &upstream.http_requests {
            assert_eq!(headers["x-api-key"], "fake-key");
            assert_eq!(headers["accept-encoding"], "identity");
            assert!(!headers.contains_key("authorization"));
        }
    }

    let (status, fault) = soap(
        &client,
        &base,
        "Browse",
        "<ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria>+dc:title</SortCriteria>",
    )
    .await;

    assert_eq!(status, 500);
    assert_eq!(text(&fault, "errorCode"), "709");
    assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), 4);
    fixture.expire(Scope::Album(album_id));
    fixture.fake.upstream.lock().unwrap().outage = true;
    let (status, fault) = soap(&client, &base, "Browse", &children).await;
    assert_eq!(status, 500);
    assert_eq!(text(&fault, "errorCode"), "501");

    assert!(
        !fault.contains("127.0.0.1") && !fault.contains("Edited") && !fault.contains("checksum")
    );

    let before_local = fixture.fake.upstream.lock().unwrap().requests.len();
    let (status, local) = soap(&client, &base, "GetSystemUpdateID", "").await;
    assert_eq!(status, 200);
    assert_eq!(text(&local, "Id"), "3");

    assert_eq!(
        fixture.fake.upstream.lock().unwrap().requests.len(),
        before_local
    );

    assert_eq!(fixture.disk(), published);

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();
        upstream.outage = false;
        upstream.contents.get_mut(&album_id).unwrap()[0]["checksum"] = json!("changed");
    }

    let (status, changed) = soap(&client, &base, "Browse", &children).await;
    assert_eq!(status, 200);
    assert_eq!(text(&changed, "UpdateID"), "2");
    let committed = fixture.disk();
    assert_eq!(committed.1.system_update_id, 4);
    assert_eq!(committed.1.albums[&album_id].update_id, 2);

    assert_ne!(
        committed.1.albums[&album_id].contents_digest,
        published.1.albums[&album_id].contents_digest
    );

    assert!(fixture.fake.notifications.try_recv().is_err());

    // Root, initial contents, and the changed refresh coalesce while SEQ=0 is in flight.
    tokio::time::pause();
    tokio::time::advance(crate::eventing::MODERATION).await;
    tokio::time::resume();
    fixture.fake.upstream.lock().unwrap().notification_gate = None;
    callback_gate.release.add_permits(1);

    let (notification, body) =
        tokio::time::timeout(Duration::from_secs(3), fixture.fake.notifications.recv())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(notification["seq"], "1");
    assert_eq!(notification["sid"], sid);
    let (status, local) = soap(&client, &base, "GetSystemUpdateID", "").await;
    assert_eq!(status, 200);
    assert_eq!(text(&body, "SystemUpdateID"), text(&local, "Id"));

    assert_eq!(
        text(&local, "Id"),
        fixture.disk().1.system_update_id.to_string()
    );

    assert_eq!(fixture.disk(), committed);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), fixture.fake.notifications.recv())
            .await
            .is_err()
    );

    server_task.abort();
    assert!(server_task.await.unwrap_err().is_cancelled());
    fixture.abort(catalog_task).await;
    events_task.abort();
    assert!(events_task.await.unwrap_err().is_cancelled());
    fixture.fake.task.abort();
    assert!((&mut fixture.fake.task).await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn local_id_startup_event_outage_and_metadata_scopes() {
    let mut fixture = Fixture::new(2).await;
    let task = fixture.run();

    let events = tokio::spawn(fixture.library.inner.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(1).await;

    for _ in 0..5 {
        assert_eq!(fixture.library.system_update_id(), 1);
    }

    assert!(fixture.fake.upstream.lock().unwrap().requests.is_empty());
    fixture.fake.upstream.lock().unwrap().outage = true;
    fault(browse(&fixture.library, action("0", true, 0, 0, None)), 501).await;
    assert_eq!(fixture.library.system_update_id(), 1);
    fixture.fake.upstream.lock().unwrap().outage = false;

    let root = fixture
        .library
        .browse(action("0", true, 0, 99, None))
        .await
        .unwrap();

    assert_eq!(root.total_matches, 1);
    assert_eq!(root.objects.len(), 1);
    assert_eq!(root.objects[0].title, "Photos & videos");
    assert_eq!(root.objects[0].parent_id, "-1");
    assert_eq!(root.objects[0].class, "object.container");
    assert_eq!(root.objects[0].child_count, Some(2));
    assert!(root.objects[0].resources.is_empty());
    assert_eq!(root.update_id, 2);

    let metadata = fixture
        .library
        .browse(action(
            &format!("album:{}", Uuid::from_u128(1)),
            true,
            0,
            0,
            Some(true),
        ))
        .await
        .unwrap();

    assert_eq!(metadata.update_id, 0);
    assert_eq!(metadata.total_matches, 1);
    assert!(metadata.objects[0].child_count.is_none());
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    fixture.expire(Scope::Root);
    fixture.fake.upstream.lock().unwrap().outage = true;
    fault(browse(&fixture.library, children(1)), 501).await;
    assert_eq!(fixture.library.system_update_id(), 2);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}

#[tokio::test]
async fn invalid_object_id_fails_before_fetch() {
    let fixture = Fixture::new(1).await;

    assert_eq!(
        fixture
            .library
            .browse(action("asset:bad", true, 0, 0, None))
            .await
            .err(),
        Some(MISSING)
    );

    assert!(fixture.fake.upstream.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn albums_default_to_latest_date_with_title_ties_and_missing_dates_last() {
    let fixture = Fixture::new(9).await;

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();

        upstream.albums = vec![
            album(1, "Z"),
            album(2, "A"),
            album(3, "C"),
            album(4, "c"),
            album(5, "\u{106}"),
            album(6, "A missing"),
            album(7, "B invalid"),
            album(8, "C null"),
            album(9, "D nonstring"),
        ];

        for album in &mut upstream.albums[..5] {
            album["endDate"] = json!("2025-01-01T00:00:00.000Z");
        }

        upstream.albums[0]["createdAt"] = json!("2020-01-01T00:00:00Z");
        upstream.albums[1]["createdAt"] = json!("2030-01-01T00:00:00Z");
        upstream.albums[1]["endDate"] = json!("2024-12-31T00:00:00.000Z");
        upstream.albums[6]["endDate"] = json!("2025-99-01T00:00:00Z");
        upstream.albums[7]["endDate"] = Value::Null;
        upstream.albums[8]["endDate"] = json!(123);
        upstream.albums.reverse();
    }

    let task = fixture.run();

    for (sort, expected) in [
        (None, [3, 4, 5, 1, 2, 6, 7, 8, 9]),
        (Some(false), [1, 3, 4, 5, 6, 7, 8, 9, 2]),
        (Some(true), [2, 3, 4, 5, 6, 7, 8, 9, 1]),
    ] {
        let full = fixture
            .library
            .browse(action("0", false, 0, 0, sort))
            .await
            .unwrap();

        let expected: Vec<_> = expected
            .into_iter()
            .map(|id| format!("album:{}", Uuid::from_u128(id)))
            .collect();

        assert_eq!(full.total_matches, 9);

        assert_eq!(
            full.objects.iter().map(|o| &o.id).collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );

        for start in [0, 3, 6, 9] {
            let page = fixture
                .library
                .browse(action("0", false, start, 3, sort))
                .await
                .unwrap();

            assert_eq!(page.total_matches, 9);
            assert_eq!(page.update_id, full.update_id);

            assert_eq!(
                page.objects,
                full.objects[start as usize..(start as usize + 3).min(9)]
            );
        }

        let oldest_created = full.objects.iter().find(|o| o.title == "Z").unwrap();
        assert_eq!(oldest_created.date.as_deref(), Some("2020-01-01"));
    }

    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    fixture.abort(task).await;
}

#[tokio::test]
async fn album_latest_date_refresh_reorders_and_publishes_without_loading_contents() {
    let mut fixture = Fixture::new(2).await;

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();
        upstream.albums = vec![album(1, "A"), album(2, "Z")];
        upstream.albums[0]["endDate"] = json!("2020-01-01T00:00:00Z");
        upstream.albums[1]["endDate"] = json!("2024-01-01T00:00:00Z");
    }

    let task = fixture.run();
    let request = || action("0", false, 0, 0, None);
    let before = fixture.library.browse(request()).await.unwrap();
    assert_eq!(before.objects[0].title, "Z");
    let disk = fixture.disk().1;

    let events = tokio::spawn(fixture.library.inner.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(before.update_id).await;

    fixture.fake.upstream.lock().unwrap().albums[0]["endDate"] = json!("2025-01-01T00:00:00Z");

    let cached = fixture.library.browse(request()).await.unwrap();
    assert_eq!(cached.objects, before.objects);
    assert_eq!(cached.update_id, before.update_id);
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    fixture.expire(Scope::Root);
    let after = fixture.library.browse(request()).await.unwrap();
    assert_eq!(after.objects[0].title, "A");
    assert_eq!(after.objects[1], before.objects[0]);
    assert_eq!(after.update_id, before.update_id + 1);
    fixture.fake.event(after.update_id).await;
    let changed = fixture.disk();
    assert_eq!(changed.1.system_update_id, after.update_id);
    let id = Uuid::from_u128(1);

    assert_eq!(
        changed.1.albums[&id].update_id,
        disk.albums[&id].update_id + 1
    );

    assert_eq!(
        changed.1.albums[&Uuid::from_u128(2)],
        disk.albums[&Uuid::from_u128(2)]
    );

    assert!(
        changed
            .1
            .albums
            .values()
            .all(|a| a.contents_digest.is_none())
    );

    fixture.fake.upstream.lock().unwrap().albums.reverse();
    fixture.expire(Scope::Root);
    let same = fixture.library.browse(request()).await.unwrap();
    assert_eq!(same.objects, after.objects);
    assert_eq!(same.update_id, after.update_id);
    assert_eq!(fixture.disk(), changed);
    assert_eq!(fixture.fake.calls("/api/albums"), 3);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}

#[tokio::test]
async fn relationship_count_sort_filter_and_snapshot_counter_capture() {
    let fixture = Fixture::new(4).await;

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();

        upstream.albums = vec![
            album(4, "D"),
            album(3, "\u{106}"),
            album(2, "c"),
            album(1, "C"),
        ];

        upstream.albums[0]["createdAt"] = json!("2020-01-01T00:00:00Z");

        let mut hidden = item(9, None, None);
        hidden["visibility"] = json!("hidden");
        let mut video = item(3, None, None);
        video["type"] = json!("VIDEO");
        video["originalMimeType"] = json!("video/mp4");

        upstream.contents.insert(
            Uuid::from_u128(1),
            vec![
                item(
                    1,
                    Some("2024-01-02T00:00:00Z"),
                    Some("2024-01-01T00:00:00Z"),
                ),
                item(
                    2,
                    Some("2024-01-01T00:00:00Z"),
                    Some("2024-01-02T00:00:00Z"),
                ),
                video,
                hidden,
            ],
        );
    }

    let task = fixture.run();

    let root = fixture
        .library
        .browse(action("0", false, 1, 2, None))
        .await
        .unwrap();

    assert_eq!(root.total_matches, 4);

    assert_eq!(
        root.objects
            .iter()
            .map(|o| o.title.as_str())
            .collect::<Vec<_>>(),
        ["c", "\u{106}"]
    );

    let root_date = fixture
        .library
        .browse(action("0", false, 0, 1, Some(false)))
        .await
        .unwrap();

    assert_eq!(root_date.objects[0].title, "D");
    let baseline = fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(baseline.total_matches, 3);
    assert_eq!(baseline.update_id, 1);

    assert_eq!(
        baseline
            .objects
            .iter()
            .map(|o| o.title.as_str())
            .collect::<Vec<_>>(),
        ["Photo 2", "Photo 1", "Photo 3"]
    );

    for (sort, titles) in [
        (false, ["Photo 1", "Photo 2", "Photo 3"]),
        (true, ["Photo 2", "Photo 1", "Photo 3"]),
    ] {
        let mut request = children(1);
        request.sort = Some(sort);
        request.filter = Filter::parse("").unwrap();
        let rows = fixture.library.browse(request).await.unwrap();
        assert_eq!(rows.total_matches, 3);

        assert_eq!(
            rows.objects
                .iter()
                .map(|o| o.title.as_str())
                .collect::<Vec<_>>(),
            titles
        );

        let required = protocol::didl(&rows.objects, &Filter::parse("").unwrap()).unwrap();
        assert!(!required.contains("<res ") && !required.contains("dc:date"));
        let full = protocol::didl(&rows.objects, &Filter::parse("*").unwrap()).unwrap();
        assert!(full.contains("DLNA.ORG_OP=01"));
        assert!(full.contains("http://192.0.2.1:8200/media/assets/"));
    }

    for (start, count, returned) in [(1, 1, 1), (1, 0, 2), (3, 0, 0), (u32::MAX, 10, 0)] {
        let result = fixture
            .library
            .browse(action(
                &format!("album:{}", Uuid::from_u128(1)),
                false,
                start,
                count,
                Some(false),
            ))
            .await
            .unwrap();

        assert_eq!(result.total_matches, 3);
        assert_eq!(result.objects.len(), returned);

        if start == 1 && count == 1 {
            assert_eq!(result.objects[0].title, "Photo 2");
        }
    }

    let appearance = format!("album:{}:asset:{}", Uuid::from_u128(1), Uuid::from_u128(1));

    let metadata = fixture
        .library
        .browse(action(&appearance, true, 0, 0, None))
        .await
        .unwrap();

    assert_eq!(metadata.update_id, fixture.library.system_update_id());
    assert_eq!(metadata.total_matches, 1);

    fault(
        browse(&fixture.library, action(&appearance, false, 0, 0, None)),
        710,
    )
    .await;

    let wrong_album = format!("album:{}:asset:{}", Uuid::from_u128(2), Uuid::from_u128(1));

    fault(
        browse(&fixture.library, action(&wrong_album, true, 0, 0, None)),
        701,
    )
    .await;

    fault(
        browse(&fixture.library, action(&wrong_album, false, 0, 0, None)),
        701,
    )
    .await;

    fault(browse(&fixture.library, children(999)), 701).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 3);

    // A response owns its old rows/counter even after later publication.
    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .insert(Uuid::from_u128(1), vec![]);

    fixture.expire(Scope::Album(Uuid::from_u128(1)));
    let newer = fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(newer.total_matches, 0);
    assert_eq!(newer.update_id, baseline.update_id + 1);
    assert_eq!(baseline.objects.len(), 3);
    fixture.abort(task).await;
}

#[tokio::test]
async fn expiry_and_lru_refill_are_fresh_without_writes_or_events() {
    let mut fixture = Fixture::new(2).await;

    fixture
        .library
        .inner
        .state
        .lock()
        .unwrap()
        .cache
        .album_limit = 1;

    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();

    assert!(
        !fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .cache
            .albums
            .contains_key(&Uuid::from_u128(1))
    );

    let before = fixture.disk();

    let events = tokio::spawn(fixture.library.inner.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(before.1.system_update_id).await;
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.disk(), before);
    fixture.expire(Scope::Root);
    fixture.expire(Scope::Album(Uuid::from_u128(1)));

    let expired =
        fixture.library.inner.state.lock().unwrap().cache.albums[&Uuid::from_u128(1)].completed;

    fixture.library.browse(children(1)).await.unwrap();

    assert!(
        fixture.library.inner.state.lock().unwrap().cache.albums[&Uuid::from_u128(1)].completed
            > expired
    );

    assert_eq!(fixture.disk(), before);
    let calls = fixture.fake.upstream.lock().unwrap().requests.len();
    fixture.fake.upstream.lock().unwrap().outage = true;
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), calls);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), fixture.fake.notifications.recv())
            .await
            .is_err()
    );

    assert_eq!(fixture.fake.calls("/api/server/version"), 1);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}

#[tokio::test]
async fn root_counts_toward_byte_budget_and_payload_eviction_retains_history() {
    let fixture = Fixture::new(2).await;

    for id in 1..=2 {
        let mut asset = item(id, None, None);
        asset["checksum"] = json!("x".repeat(4096));

        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .contents
            .insert(Uuid::from_u128(id), vec![asset]);
    }

    let root = fixture.library.inner.source.root().await.unwrap();

    let contents = fixture
        .library
        .inner
        .source
        .contents(Uuid::from_u128(1))
        .await
        .unwrap();

    fixture.library.inner.state.lock().unwrap().cache.byte_limit = root.bytes + 2 * contents.bytes;

    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();

    {
        let state = fixture.library.inner.state.lock().unwrap();
        assert!(state.cache.root.is_some());
        assert_eq!(state.cache.albums.len(), 2);
        assert_eq!(state.ledger.albums.len(), 2);

        assert!(
            state
                .cache
                .albums
                .values()
                .all(|a| a.snapshot.bytes == contents.bytes)
        );

        assert!(
            state
                .ledger
                .albums
                .values()
                .all(|album| album.contents_digest.is_some())
        );
    }

    let before = fixture.disk();

    fixture.fake.upstream.lock().unwrap().albums[0]["albumName"] =
        json!("A".repeat(contents.bytes / 2));

    fixture.expire(Scope::Root);

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    {
        let state = fixture.library.inner.state.lock().unwrap();
        let root_bytes = state.cache.root.as_ref().unwrap().snapshot.bytes;
        assert!(root_bytes > root.bytes);
        assert!(root_bytes + contents.bytes <= state.cache.byte_limit);
        assert!(root_bytes + 2 * contents.bytes > state.cache.byte_limit);
        assert_eq!(state.cache.albums.len(), 1);
        assert!(!state.cache.albums.contains_key(&Uuid::from_u128(1)));
        assert_eq!(state.ledger.albums.len(), 2);

        for (id, album) in &state.ledger.albums {
            assert_eq!(album.contents_digest, before.1.albums[id].contents_digest);
        }
    }

    let grown = fixture.disk();
    assert_eq!(grown.1.system_update_id, before.1.system_update_id + 1);

    assert_eq!(
        grown.1.albums[&Uuid::from_u128(1)].update_id,
        before.1.albums[&Uuid::from_u128(1)].update_id + 1
    );

    assert_eq!(
        grown.1.albums[&Uuid::from_u128(2)],
        before.1.albums[&Uuid::from_u128(2)]
    );

    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.disk(), grown);
    fixture.abort(task).await;
}

#[tokio::test]
async fn metadata_contents_and_restart_preserve_independent_durable_counters() {
    let mut fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    let before = fixture.disk().1;
    fixture.fake.upstream.lock().unwrap().albums[0]["albumName"] = json!("Renamed");
    fixture.expire(Scope::Root);

    let result = fixture
        .library
        .browse(action(
            &format!("album:{}", Uuid::from_u128(1)),
            true,
            0,
            1,
            None,
        ))
        .await
        .unwrap();

    let renamed = fixture.disk().1;
    assert_eq!(result.objects[0].title, "Renamed");
    assert_eq!(renamed.system_update_id, before.system_update_id + 1);

    assert_eq!(
        renamed.albums[&Uuid::from_u128(1)].update_id,
        before.albums[&Uuid::from_u128(1)].update_id + 1
    );

    assert_eq!(
        renamed.albums[&Uuid::from_u128(2)],
        before.albums[&Uuid::from_u128(2)]
    );

    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .insert(Uuid::from_u128(1), vec![item(1, None, None)]);

    fixture.expire(Scope::Album(Uuid::from_u128(1)));
    fixture.library.browse(children(1)).await.unwrap();
    fixture.abort(task).await;
    let mut restarted = fixture.disk().1;
    restarted.restart();
    let store = fixture.library.inner.store.clone();
    store.persist(restarted.clone()).await;

    fixture.library = Library::new(
        fixture.fake.config(fixture.directory.path()),
        store,
        restarted.clone(),
        Subscriptions::new().unwrap(),
    )
    .unwrap();

    let disk = fixture.disk();
    let task = fixture.run();

    assert_eq!(
        fixture.library.system_update_id(),
        restarted.system_update_id
    );

    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    assert_eq!(fixture.disk(), disk);
    assert_eq!(fixture.fake.calls("/api/server/version"), 2);
    fixture.abort(task).await;
}

#[tokio::test]
async fn shared_flights_survive_callers_and_four_scopes_reject_without_backlog() {
    let fixture = Fixture::new(5).await;
    let task = fixture.run();

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    let mut callers = Vec::new();
    let mut gates = Vec::new();

    for id in 1..=4 {
        let barrier = Barrier::new();

        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .gates
            .insert(Scope::Album(Uuid::from_u128(id)), barrier.clone());

        callers.push(browse(&fixture.library, children(id)));
        barrier.entered().await;
        gates.push(barrier);
    }

    let mut short_waiter = Box::pin(fixture.library.browse(children(1)));
    assert!(futures_util::poll!(&mut short_waiter).is_pending());
    let mut independent = Box::pin(fixture.library.browse(children(1)));
    assert!(futures_util::poll!(&mut independent).is_pending());
    drop(short_waiter);
    callers.remove(0).abort();
    assert_eq!(fixture.library.inner.permits.available_permits(), 0);
    fault(browse(&fixture.library, children(5)), 501).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);

    for gate in gates {
        gate.release.add_permits(1);
    }

    independent.await.unwrap();

    for caller in callers {
        caller.await.unwrap().unwrap();
    }

    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);
    fixture.library.browse(children(5)).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 5);
    assert_eq!(fixture.library.system_update_id(), 7);

    assert_eq!(
        fixture.disk().1,
        *fixture.library.inner.state.lock().unwrap().ledger
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn preparation_timeout_drops_stalled_requests_across_pages() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    async fn request(listener: &TcpListener, path: &str) -> (TcpStream, Value) {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        let mut line = String::new();
        socket.read_line(&mut line).await.unwrap();
        assert!(line.contains(&format!(" {path} HTTP/1.1\r\n")));
        let mut length = 0;

        loop {
            line.clear();
            assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);

            if line == "\r\n" {
                break;
            }

            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
        }

        let mut body = vec![0; length];
        socket.read_exact(&mut body).await.unwrap();

        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };

        (socket.into_inner(), body)
    }

    async fn respond(mut socket: TcpStream, body: Value) {
        let body = body.to_string();

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );

        socket.write_all(response.as_bytes()).await.unwrap();
        socket.shutdown().await.unwrap();
    }

    // Stall version checking, album listing, or a later search page.
    for (stage, body_pending) in [(0, true), (1, false), (1, true), (2, false), (2, true)] {
        let mut fixture = Fixture::new(1).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let inner = Arc::get_mut(&mut fixture.library.inner).unwrap();

        inner.source.client = Client::new(
            format!("http://{}/api/", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
            HeaderValue::from_static("fake-key"),
        )
        .unwrap();

        tokio::time::pause();

        // Advance only explicitly while exchanging loopback responses.
        let clock_guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });

        let task = fixture.run();
        let caller = browse(&fixture.library, children(1));
        let (mut socket, _) = request(&listener, "/api/server/version").await;

        if stage > 0 {
            if stage == 1 {
                tokio::time::advance(Duration::from_secs(6)).await;
            }

            respond(
                socket,
                json!({"major": 3, "minor": 1, "patch": 0, "prerelease": null}),
            )
            .await;

            (socket, _) = request(&listener, "/api/albums").await;
        }

        if stage == 2 {
            respond(socket, json!([album(1, "Album")])).await;
            let (first, query) = request(&listener, "/api/search/metadata").await;
            assert_eq!(query["page"], 1);
            tokio::time::advance(Duration::from_secs(6)).await;

            respond(first, json!({"assets": {"items": [crate::immich::tests::asset(1, "IMAGE")], "nextPage": "2"}})).await;

            let query;
            (socket, query) = request(&listener, "/api/search/metadata").await;
            assert_eq!(query["page"], 2);
        }

        if body_pending {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n[")
                .await
                .unwrap();
        }

        assert!(!caller.is_finished());
        let before = fixture.disk();

        // Earlier requests consumed six seconds; a new page gets no new window.
        tokio::time::advance(Duration::from_secs(if stage == 0 { 21 } else { 15 })).await;

        clock_guard.abort();
        assert!(clock_guard.await.unwrap_err().is_cancelled());
        fault(caller, 501).await;
        assert_eq!(fixture.disk(), before);
        assert_eq!(fixture.library.inner.permits.available_permits(), REFRESHES);

        assert!(
            fixture
                .library
                .inner
                .state
                .lock()
                .unwrap()
                .flights
                .is_empty()
        );

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut [0; 1]))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        fixture.abort(task).await;
        tokio::time::resume();
    }
}

#[tokio::test]
async fn whole_permit_covers_commit_wait_and_preparation_timeout_cleans_flight() {
    let fixture = Fixture::new(1).await;
    let task = fixture.run();

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    let gate = fixture.library.inner.commit.lock().await;
    let barrier = Barrier::new();

    *fixture.library.inner.prepared.lock().unwrap() =
        Some((Scope::Album(Uuid::from_u128(1)), barrier.clone()));

    let caller = browse(&fixture.library, children(1));
    barrier.entered().await;
    tokio::time::pause();
    barrier.release.add_permits(1);
    tokio::task::yield_now().await;
    assert_eq!(fixture.library.inner.permits.available_permits(), 3);
    let before = fixture.disk();
    tokio::time::advance(PREPARATION_TIMEOUT + Duration::from_millis(1)).await;
    tokio::time::resume();
    fault(caller, 501).await;
    assert_eq!(fixture.disk(), before);
    assert_eq!(fixture.library.inner.permits.available_permits(), 4);

    assert!(
        fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .flights
            .is_empty()
    );

    drop(gate);
    *fixture.library.inner.prepared.lock().unwrap() = None;
    fixture.library.browse(children(1)).await.unwrap();
    fixture.abort(task).await;
}

#[tokio::test]
async fn stale_unchanged_album_cannot_survive_root_removal_and_reappearance() {
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.expire(Scope::Album(Uuid::from_u128(1)));
    let barrier = Barrier::new();

    *fixture.library.inner.prepared.lock().unwrap() =
        Some((Scope::Album(Uuid::from_u128(1)), barrier.clone()));

    let caller = browse(&fixture.library, children(1));
    barrier.entered().await;
    fixture.fake.upstream.lock().unwrap().albums.remove(0);
    fixture.expire(Scope::Root);

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    assert!(
        !fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .cache
            .albums
            .contains_key(&Uuid::from_u128(1))
    );

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .albums
        .push(album(1, "Album"));

    fixture.expire(Scope::Root);

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    let before = fixture.disk();
    barrier.release.add_permits(1);
    fault(caller, 501).await;
    assert_eq!(fixture.disk(), before);

    assert!(
        !fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .cache
            .albums
            .contains_key(&Uuid::from_u128(1))
    );

    *fixture.library.inner.prepared.lock().unwrap() = None;
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.disk(), before);
    fixture.abort(task).await;
}

#[tokio::test]
async fn unrelated_root_and_album_changes_do_not_invalidate_prepared_contents() {
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.expire(Scope::Album(Uuid::from_u128(1)));
    let barrier = Barrier::new();

    *fixture.library.inner.prepared.lock().unwrap() =
        Some((Scope::Album(Uuid::from_u128(1)), barrier.clone()));

    let caller = browse(&fixture.library, children(1));
    barrier.entered().await;

    let flight = fixture.library.inner.state.lock().unwrap().flights
        [&Scope::Album(Uuid::from_u128(1))]
        .clone();

    fixture.fake.upstream.lock().unwrap().albums[1]["albumName"] = json!("Other rename");
    fixture.expire(Scope::Root);
    fixture.library.browse(children(2)).await.unwrap();
    let before = fixture.disk();
    barrier.release.add_permits(1);
    caller.await.unwrap().unwrap();
    assert_eq!(fixture.disk(), before);
    let view = flight.borrow().as_ref().unwrap().as_ref().unwrap().clone();
    assert_eq!(*view.ledger, before.1);

    assert_eq!(
        view.root.albums[&Uuid::from_u128(2)].object.title,
        "Other rename"
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn publication_failure_worker() {
    let Some(directory) = std::env::var_os("CATALOG_TEST_DIRECTORY") else {
        return;
    };

    let mode = std::env::var("CATALOG_TEST_MODE").unwrap();
    let mut fake = Fake::new().await;
    fake.upstream.lock().unwrap().albums = vec![album(1, "Album")];
    let config = fake.config(std::path::Path::new(&directory));
    let (store, mut ledger) = Store::open(&config.state_directory, config.server_uuid).unwrap();
    ledger.restart();
    store.persist(ledger.clone()).await;

    let library = Library::new(config, store, ledger, Subscriptions::new().unwrap()).unwrap();

    let supervised = library.clone();
    let mut supervisor = tokio::spawn(async move { supervised.run().await });

    library
        .browse(action("0", false, 0, 0, None))
        .await
        .unwrap();

    let before = library.inner.state.lock().unwrap().ledger.clone();

    let mut events = tokio::spawn(library.inner.events.clone().run());

    fake.subscribe(&library);
    fake.event(before.system_update_id).await;
    fake.upstream.lock().unwrap().albums = vec![album(1, "Changed")];

    library
        .inner
        .state
        .lock()
        .unwrap()
        .cache
        .root
        .as_mut()
        .unwrap()
        .completed -= FRESHNESS;

    let mut barrier = Barrier::new();
    Arc::get_mut(&mut barrier).unwrap().panic = mode == "panic";
    *library.inner.publication.lock().unwrap() = Some(barrier.clone());
    let mut caller = browse(&library, action("0", false, 0, 0, None));
    barrier.entered().await;
    let directory = Path::new(&directory);

    let committed: Ledger =
        serde_json::from_slice(&fs::read(directory.join("revisions.json")).unwrap()).unwrap();

    assert_eq!(committed.system_update_id, before.system_update_id + 1);
    assert_ne!(committed.root_digest, before.root_digest);

    {
        let state = library.inner.state.lock().unwrap();
        assert_eq!(state.ledger, before);

        assert_eq!(
            state.cache.root.as_ref().unwrap().snapshot.albums[&Uuid::from_u128(1)]
                .object
                .title,
            "Album"
        );
    }

    // A subscription registered after durable completion still sees published state.
    fake.subscribe(&library);
    fake.event(before.system_update_id).await;
    assert!(fake.notifications.try_recv().is_err());
    assert!(!caller.is_finished());

    fs::write(
        directory.join("observed-candidate.json"),
        serde_json::to_vec(&committed).unwrap(),
    )
    .unwrap();

    if mode == "panic" {
        barrier.release.add_permits(1);
    }

    // Only Publication::drop may end this worker successfully for the parent.
    tokio::select! {
        _ = &mut caller => std::process::exit(90),

        _ = &mut supervisor => std::process::exit(91),

        _ = &mut events => std::process::exit(92),

        _ = fake.notifications.recv() => std::process::exit(93),

        _ = tokio::time::sleep(COMMIT_TIMEOUT + Duration::from_secs(1)) => {
            std::process::exit(94);
        }
    }
}

async fn publication_failure(mode: &str) {
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();

    let test_name = format!(
        "{}::publication_failure_worker",
        module_path!().split_once("::").unwrap().1
    );

    let mut child = {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();

        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--nocapture"])
            .env("CATALOG_TEST_DIRECTORY", directory.path())
            .env("CATALOG_TEST_MODE", mode)
            .stdin(std::process::Stdio::null())
            .spawn()
            .unwrap()
    };

    let started = std::time::Instant::now();

    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }

        if started.elapsed() >= Duration::from_secs(12) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("catalog publication worker exceeded watchdog deadline");
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    assert_eq!(status.code(), Some(1), "{mode}: {status}");

    let candidate: Ledger = serde_json::from_slice(
        &fs::read(directory.path().join("observed-candidate.json")).unwrap(),
    )
    .unwrap();

    assert_eq!(candidate.system_update_id, 3);
    let (store, mut restored) = Store::open(directory.path(), Uuid::from_u128(999)).unwrap();
    assert_eq!(restored, candidate);
    restored.restart();
    store.persist(restored.clone()).await;
    let mut fake = Fake::new().await;
    fake.upstream.lock().unwrap().albums = vec![album(1, "Changed")];

    let library = Library::new(
        fake.config(directory.path()),
        store,
        restored.clone(),
        Subscriptions::new().unwrap(),
    )
    .unwrap();

    let result = library
        .browse(action("0", false, 0, 0, None))
        .await
        .unwrap();

    assert_eq!(result.objects[0].title, "Changed");
    assert_eq!(result.update_id, 4);
    assert_eq!(*library.inner.state.lock().unwrap().ledger, restored);
    let events = tokio::spawn(library.inner.events.clone().run());
    fake.subscribe(&library);
    fake.event(4).await;
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn publication_timeout_after_persist_fails_stop_and_restores_candidate() {
    publication_failure("timeout").await;
}

#[tokio::test]
async fn publication_panic_after_persist_fails_stop_and_restores_candidate() {
    publication_failure("panic").await;
}

#[tokio::test]
async fn preparation_panic_is_fatal_and_supervised_even_without_waiters() {
    let fixture = Fixture::new(1).await;
    let task = fixture.run();
    let mut barrier = Barrier::new();
    Arc::get_mut(&mut barrier).unwrap().panic = true;
    *fixture.library.inner.prepared.lock().unwrap() = Some((Scope::Root, barrier.clone()));
    let caller = browse(&fixture.library, action("0", true, 0, 0, None));
    barrier.entered().await;
    caller.abort();
    barrier.release.add_permits(1);

    assert!(
        timeout_at(Instant::now() + Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );

    assert!(fixture.library.inner.failure.is_cancelled());
    assert_eq!(fixture.library.inner.permits.available_permits(), 4);

    assert!(
        fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .flights
            .is_empty()
    );

    assert_eq!(fixture.library.system_update_id(), 1);
}

#[tokio::test]
async fn abandoned_refresh_completes_and_expiry_alone_never_does_work() {
    let fixture = Fixture::new(1).await;
    let task = fixture.run();
    let barrier = Barrier::new();

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .gates
        .insert(Scope::Root, barrier.clone());

    let caller = browse(&fixture.library, action("0", true, 0, 0, None));
    barrier.entered().await;
    caller.abort();
    let _ = caller.await;

    let mut flight = fixture.library.inner.state.lock().unwrap().flights[&Scope::Root].clone();
    barrier.release.add_permits(1);
    flight.changed().await.unwrap();
    assert!(flight.borrow().as_ref().is_some_and(Result::is_ok));
    assert_eq!(fixture.library.system_update_id(), 2);
    let before = fixture.disk();
    let calls = fixture.fake.upstream.lock().unwrap().requests.len();
    tokio::time::pause();
    tokio::time::advance(FRESHNESS).await;
    tokio::time::resume();
    assert_eq!(fixture.library.system_update_id(), 2);
    assert_eq!(fixture.disk(), before);
    assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), calls);

    assert!(
        fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .cache
            .root
            .is_some()
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn completed_browse_pins_rows_and_revision_across_payload_eviction() {
    let fixture = Fixture::new(2).await;

    fixture.library.inner.state.lock().unwrap().cache.byte_limit = 2 * SNAPSHOT_BYTES;

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();
        upstream.albums[0]["albumName"] = json!("A".repeat(16 * 1024));

        for id in 1..=2 {
            let mut asset = item(id, None, None);
            asset["checksum"] = json!("x".repeat(SNAPSHOT_BYTES - 4096));
            upstream.contents.insert(Uuid::from_u128(id), vec![asset]);
        }
    }

    let task = fixture.run();

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    let mut children_waiter = Box::pin(fixture.library.browse(children(1)));
    assert!(futures_util::poll!(&mut children_waiter).is_pending());
    let appearance = format!("album:{}:asset:{}", Uuid::from_u128(1), Uuid::from_u128(1));

    let mut metadata_waiter =
        Box::pin(
            fixture
                .library
                .browse(action(&appearance, true, 0, 0, None)),
        );

    assert!(futures_util::poll!(&mut metadata_waiter).is_pending());

    // Observe completion without polling either Browse waiter again.
    let mut flight = fixture.library.inner.state.lock().unwrap().flights
        [&Scope::Album(Uuid::from_u128(1))]
        .clone();

    flight.changed().await.unwrap();

    let (album_id, system_id, a_bytes) = {
        let state = fixture.library.inner.state.lock().unwrap();
        let bytes = state.cache.albums[&Uuid::from_u128(1)].snapshot.bytes;
        assert!(bytes > SNAPSHOT_BYTES - 8192 && bytes <= SNAPSHOT_BYTES);

        (
            state.ledger.albums[&Uuid::from_u128(1)].update_id,
            state.ledger.system_update_id,
            bytes,
        )
    };

    fixture.library.browse(children(2)).await.unwrap();

    {
        let state = fixture.library.inner.state.lock().unwrap();
        let b_bytes = state.cache.albums[&Uuid::from_u128(2)].snapshot.bytes;
        assert!(b_bytes > SNAPSHOT_BYTES - 8192 && b_bytes <= SNAPSHOT_BYTES);

        assert!(
            a_bytes + b_bytes + state.cache.root.as_ref().unwrap().snapshot.bytes
                > 2 * SNAPSHOT_BYTES
        );

        assert!(!state.cache.albums.contains_key(&Uuid::from_u128(1)));
    }

    let result = children_waiter.await.unwrap();
    assert_eq!(result.total_matches, 1);
    assert_eq!(result.objects[0].id, appearance);
    assert_eq!(result.objects[0].title, "Photo 1");
    assert_eq!(result.update_id, album_id);

    // Later root removal cannot change the already completed item view.
    fixture.fake.upstream.lock().unwrap().albums.remove(0);
    fixture.expire(Scope::Root);

    fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    let metadata = metadata_waiter.await.unwrap();
    assert_eq!(metadata.objects, result.objects);
    assert_eq!(metadata.update_id, system_id);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    assert!(
        !fixture.library.inner.state.lock().unwrap().ledger.albums[&Uuid::from_u128(1)].present
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn fresh_hits_and_unchanged_root_flights_pin_coherent_views() {
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    let root_hit = fixture.library.fresh(Scope::Root).await.unwrap();

    let album_hit = fixture
        .library
        .fresh(Scope::Album(Uuid::from_u128(1)))
        .await
        .unwrap();

    assert!(Arc::ptr_eq(&root_hit.ledger, &album_hit.ledger));
    assert!(Arc::ptr_eq(&root_hit.root, &album_hit.root));
    assert!(root_hit.contents.is_none());

    assert_eq!(
        album_hit.ledger.albums[&Uuid::from_u128(1)]
            .contents_digest
            .as_deref(),
        Some(album_hit.contents.as_ref().unwrap().digest.as_str())
    );

    fixture.expire(Scope::Root);
    let mut root_waiter = Box::pin(fixture.library.browse(action("0", true, 0, 0, None)));
    assert!(futures_util::poll!(&mut root_waiter).is_pending());

    let mut album_waiter = Box::pin(fixture.library.browse(action(
        &format!("album:{}", Uuid::from_u128(1)),
        true,
        0,
        0,
        None,
    )));

    assert!(futures_util::poll!(&mut album_waiter).is_pending());
    let mut flight = fixture.library.inner.state.lock().unwrap().flights[&Scope::Root].clone();
    let before = fixture.disk();
    flight.changed().await.unwrap();
    assert_eq!(fixture.disk(), before);
    fixture.fake.upstream.lock().unwrap().albums.remove(0);
    fixture.expire(Scope::Root);

    let removed = fixture
        .library
        .browse(action("0", true, 0, 0, None))
        .await
        .unwrap();

    assert_eq!(removed.objects[0].child_count, Some(1));
    let old_root = root_waiter.await.unwrap();
    assert_eq!(old_root.objects[0].child_count, Some(2));
    assert_eq!(old_root.update_id, before.1.system_update_id);
    let old_album = album_waiter.await.unwrap();

    assert_eq!(
        old_album.objects[0],
        album_hit.root.albums[&Uuid::from_u128(1)].object
    );

    assert_eq!(
        old_album.update_id,
        before.1.albums[&Uuid::from_u128(1)].update_id
    );

    assert_eq!(
        album_hit.ledger.root_digest.as_deref(),
        Some(album_hit.root.digest.as_str())
    );

    assert!(album_hit.ledger.albums[&Uuid::from_u128(1)].present);
    assert_eq!(fixture.fake.calls("/api/albums"), 3);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 1);

    assert!(
        !fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .cache
            .albums
            .contains_key(&Uuid::from_u128(1))
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn failed_album_keeps_successful_root_and_later_requests_recover() {
    let mut fixture = Fixture::new(1).await;
    let task = fixture.run();

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .insert(Uuid::from_u128(1), vec![json!({"id": Uuid::from_u128(1)})]);

    fault(browse(&fixture.library, children(1)), 501).await;
    let root = fixture.disk();
    assert_eq!(root.1.system_update_id, 2);
    assert!(root.1.albums[&Uuid::from_u128(1)].contents_digest.is_none());
    assert_eq!(fixture.library.system_update_id(), 2);

    assert!(
        fixture
            .library
            .inner
            .state
            .lock()
            .unwrap()
            .flights
            .is_empty()
    );

    assert_eq!(fixture.library.inner.permits.available_permits(), 4);

    let events = tokio::spawn(fixture.library.inner.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(2).await;
    fixture.fake.upstream.lock().unwrap().contents.clear();
    let result = fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(result.update_id, 1);
    assert_eq!(result.total_matches, 0);
    fixture.fake.event(3).await;
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}
