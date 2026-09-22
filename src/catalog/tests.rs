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
    net::{Ipv4Addr, SocketAddr},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

use crate::protocol::Service;

#[path = "http_tests.rs"]
mod http_tests;

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
    requests: Vec<String>,
    media: BTreeMap<String, (&'static str, &'static [u8])>,
    gates: BTreeMap<Scope, Arc<Barrier>>,
    outage: bool,
}

struct Fake {
    upstream: Arc<Mutex<Upstream>>,
    address: std::net::SocketAddr,
    notifications: mpsc::Receiver<String>,
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
                    let _ = notify.send(String::from_utf8(body.to_vec()).unwrap()).await;

                    return Response::new(Body::empty());
                }

                let media = upstream
                    .lock()
                    .unwrap()
                    .media
                    .get(&parts.uri.to_string())
                    .copied();

                if let Some((mime, bytes)) = media {
                    return Response::builder()
                        .header("content-type", mime)
                        .body(Body::from(bytes))
                        .unwrap();
                }

                let body = if body.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body).unwrap()
                };

                let (value, barrier, outage) = {
                    let mut upstream = upstream.lock().unwrap();

                    upstream.requests.push(parts.uri.path().to_string());

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

    fn calls(&self, path: &str) -> usize {
        self.upstream
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|actual| actual.as_str() == path)
            .count()
    }

    fn subscribe(&self, library: &ImmichCatalog) {
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
        let body = timeout_at(
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

fn config(address: SocketAddr, directory: &Path) -> Config {
    Config {
        api_base: format!("http://{address}/api/").parse().unwrap(),
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

fn disk(directory: &Path) -> (u64, Ledger) {
    let path = directory.join("revisions.json");

    (
        fs::metadata(&path).unwrap().ino(),
        serde_json::from_slice(&fs::read(path).unwrap()).unwrap(),
    )
}

struct Fixture {
    library: ImmichCatalog,
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
        let config = config(fake.address, directory.path());
        let library = ImmichCatalog::open(config, Subscriptions::new().unwrap())
            .await
            .unwrap();

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
        disk(self.directory.path())
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

fn action(id: &str, metadata: bool, start: u32, count: u32, sort: Option<bool>) -> BrowseQuery {
    BrowseQuery {
        object_id: id.into(),
        metadata,
        starting_index: start,
        requested_count: count,
        sort,
    }
}

fn children(id: u128) -> BrowseQuery {
    action(&format!("album:{}", Uuid::from_u128(id)), false, 0, 0, None)
}

fn spawn_browse(
    library: &ImmichCatalog,
    query: BrowseQuery,
) -> JoinHandle<Result<BrowseResult, Fault>> {
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

    fault(
        spawn_browse(&fixture.library, action("0", true, 0, 0, None)),
        501,
    )
    .await;

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
    fault(spawn_browse(&fixture.library, children(1)), 501).await;
    assert_eq!(fixture.library.system_update_id(), 2);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}

#[tokio::test]
async fn invalid_object_id_fails_before_fetch() {
    let fixture = Fixture::new(0).await;

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
async fn album_latest_date_refresh_reorders_and_publishes_without_loading_contents() {
    let mut fixture = Fixture::new(0).await;

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
async fn root_browse_sorts_before_pagination() {
    let fixture = Fixture::new(0).await;

    fixture.fake.upstream.lock().unwrap().albums = vec![
        album(4, "D"),
        album(3, "\u{106}"),
        album(2, "c"),
        album(1, "C"),
    ];

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

    fixture.abort(task).await;
}

#[tokio::test]
async fn album_browse_counts_eligible_items_and_sorts_before_pagination() {
    let fixture = Fixture::new(1).await;
    let mut hidden = item(9, None, None);
    hidden["visibility"] = json!("hidden");
    let mut video = item(3, None, None);
    video["type"] = json!("VIDEO");
    video["originalMimeType"] = json!("video/mp4");

    fixture.fake.upstream.lock().unwrap().contents.insert(
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

    let task = fixture.run();
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
        let rows = fixture.library.browse(request).await.unwrap();
        assert_eq!(rows.total_matches, 3);

        assert_eq!(
            rows.objects
                .iter()
                .map(|o| o.title.as_str())
                .collect::<Vec<_>>(),
            titles
        );
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

    fixture.abort(task).await;
}

#[tokio::test]
async fn item_browse_validates_album_membership_and_returns_the_system_revision() {
    let fixture = Fixture::new(2).await;

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .insert(Uuid::from_u128(1), vec![item(1, None, None)]);

    let task = fixture.run();
    let appearance = format!("album:{}:asset:{}", Uuid::from_u128(1), Uuid::from_u128(1));

    let metadata = fixture
        .library
        .browse(action(&appearance, true, 0, 0, None))
        .await
        .unwrap();

    assert_eq!(metadata.update_id, fixture.library.system_update_id());
    assert_eq!(metadata.total_matches, 1);

    fault(
        spawn_browse(&fixture.library, action(&appearance, false, 0, 0, None)),
        710,
    )
    .await;

    let wrong_album = format!("album:{}:asset:{}", Uuid::from_u128(2), Uuid::from_u128(1));

    fault(
        spawn_browse(&fixture.library, action(&wrong_album, true, 0, 0, None)),
        701,
    )
    .await;

    fault(
        spawn_browse(&fixture.library, action(&wrong_album, false, 0, 0, None)),
        701,
    )
    .await;

    fault(spawn_browse(&fixture.library, children(999)), 701).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

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

    fixture.library = ImmichCatalog::from_parts(
        config(fixture.fake.address, fixture.directory.path()),
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

        callers.push(spawn_browse(&fixture.library, children(id)));
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
    fault(spawn_browse(&fixture.library, children(5)), 501).await;
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let config = config(listener.local_addr().unwrap(), directory.path());

        let library = ImmichCatalog::open(config, Subscriptions::new().unwrap())
            .await
            .unwrap();

        tokio::time::pause();

        // Advance only explicitly while exchanging loopback responses.
        let clock_guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });

        let supervised = library.clone();
        let task = tokio::spawn(async move { supervised.run().await });
        let caller = spawn_browse(&library, children(1));
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

            respond(first, json!({"assets": {"items": [crate::immich::test_support::asset(1, "IMAGE")], "nextPage": "2"}})).await;

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
        let before = disk(directory.path());

        // Earlier requests consumed six seconds; a new page gets no new window.
        tokio::time::advance(Duration::from_secs(if stage == 0 { 21 } else { 15 })).await;

        clock_guard.abort();
        assert!(clock_guard.await.unwrap_err().is_cancelled());
        fault(caller, 501).await;
        assert_eq!(disk(directory.path()), before);
        assert_eq!(library.inner.permits.available_permits(), REFRESHES);

        assert!(library.inner.state.lock().unwrap().flights.is_empty());

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut [0; 1]))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(library.inner.state.lock().unwrap().flights.is_empty());
        assert_eq!(library.inner.permits.available_permits(), REFRESHES);
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

    let caller = spawn_browse(&fixture.library, children(1));
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

    let caller = spawn_browse(&fixture.library, children(1));
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

    let caller = spawn_browse(&fixture.library, children(1));
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
    let config = config(fake.address, Path::new(&directory));
    let library = ImmichCatalog::open(config, Subscriptions::new().unwrap())
        .await
        .unwrap();

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
    let mut caller = spawn_browse(&library, action("0", false, 0, 0, None));
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

    let library = ImmichCatalog::from_parts(
        config(fake.address, directory.path()),
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
    let caller = spawn_browse(&fixture.library, action("0", true, 0, 0, None));
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

    let caller = spawn_browse(&fixture.library, action("0", true, 0, 0, None));
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

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();

        for id in 1..=2 {
            upstream
                .contents
                .insert(Uuid::from_u128(id), vec![item(id, None, None)]);
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
        let mut state = fixture.library.inner.state.lock().unwrap();
        let bytes = state.cache.albums[&Uuid::from_u128(1)].snapshot.bytes;
        state.cache.byte_limit = state.cache.root.as_ref().unwrap().snapshot.bytes + bytes;

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
        assert_eq!(b_bytes, a_bytes);

        assert_eq!(
            state.cache.root.as_ref().unwrap().snapshot.bytes + b_bytes,
            state.cache.byte_limit
        );

        assert!(
            a_bytes + b_bytes + state.cache.root.as_ref().unwrap().snapshot.bytes
                > state.cache.byte_limit
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

    fault(spawn_browse(&fixture.library, children(1)), 501).await;
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
