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
    net::{Ipv4Addr, SocketAddr},
    sync::Mutex,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore, mpsc},
    task::JoinHandle,
};

use crate::protocol::Service;

#[path = "http_tests.rs"]
mod http_tests;

#[path = "background_tests.rs"]
mod background_tests;

pub(super) struct Barrier {
    pub(super) entered: Notify,
    pub(super) release: Semaphore,
}

impl Barrier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
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

    fn subscribe(&self, library: &Library) {
        let mut headers = HeaderMap::new();
        headers.insert("nt", HeaderValue::from_static("upnp:event"));

        headers.insert(
            "callback",
            format!("<http://{}/events>", self.address).parse().unwrap(),
        );

        let response = library.events.request(
            Service::ContentDirectory,
            Ipv4Addr::LOCALHOST,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers,
        );

        assert_eq!(response.status(), 200);
        library.activity.touch();
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

fn config(address: SocketAddr) -> Config {
    Config {
        api_base: format!("http://{address}/api/").parse().unwrap(),
        api_key: HeaderValue::from_static("fake-key"),
        listen_address: "192.0.2.1:8200".parse().unwrap(),
        friendly_name: "Photos & videos".into(),
        collator: crate::config::collator("pl").unwrap(),
        server_uuid: Uuid::from_u128(999),
        log_level: tracing::Level::INFO,
        interface_index: 1,
    }
}

#[derive(Clone)]
struct Library {
    catalog: ImmichCatalog,
    events: Subscriptions,
    activity: activity::Activity,
}

// Explicit fixture operations keep test assertions read-only. No callback can
// rewrite catalog state or scheduling while the task is running.
pub(super) enum Control {
    Inspect(Box<dyn FnOnce(&CatalogTask) + Send>),
    Expire(Scope),
    Limits { albums: usize, bytes: usize },
}

impl Control {
    pub(super) fn apply(self, task: &mut CatalogTask) {
        match self {
            Self::Inspect(inspect) => inspect(task),

            Self::Expire(scope) => match scope {
                Scope::Root => task.state.cache.root.as_mut().unwrap().completed -= FRESHNESS,

                Scope::Album(id) => {
                    task.state.cache.albums.get_mut(&id).unwrap().completed -= FRESHNESS
                }
            },

            Self::Limits { albums, bytes } => {
                task.state.cache.album_limit = albums;
                task.state.cache.byte_limit = bytes;
            }
        }
    }
}

impl Library {
    fn new(config: Config, seed: u32) -> (Self, CatalogTask) {
        let events = Subscriptions::new().unwrap();
        let activity = activity::Activity::default();

        let (catalog, task) = ImmichCatalog::from_parts(
            config,
            Ledger::new(seed),
            events.clone(),
            activity.subscribe(),
        )
        .unwrap();

        (
            Self {
                catalog,
                events,
                activity,
            },
            task,
        )
    }

    async fn browse(&self, query: BrowseQuery) -> Result<BrowseResult, Fault> {
        self.activity.touch();

        self.catalog
            .browse(query, Instant::now() + REFRESH_TIMEOUT)
            .await
    }

    async fn system_update_id(&self) -> u32 {
        self.catalog.system_update_id().await.unwrap()
    }

    async fn inspect<T: Send + 'static>(
        &self,
        inspect: impl FnOnce(&CatalogTask) -> T + Send + 'static,
    ) -> T {
        let (reply, receiver) = oneshot::channel();

        self.catalog
            .commands
            .try_send(Command::Test(Control::Inspect(Box::new(move |task| {
                let _ = reply.send(inspect(task));
            }))))
            .unwrap_or_else(|_| panic!("test catalog unavailable"));

        receiver.await.unwrap()
    }

    async fn control(&self, control: Control) {
        self.catalog
            .commands
            .try_send(Command::Test(control))
            .unwrap_or_else(|_| panic!("test catalog unavailable"));

        self.inspect(|_| ()).await;
    }

    fn enqueue(&self, query: BrowseQuery, deadline: Instant) -> oneshot::Receiver<RefreshResult> {
        let (reply, receiver) = oneshot::channel();

        self.catalog
            .commands
            .try_send(Command::Browse(Pending {
                query,
                deadline,
                reply,
                waiting: false,
            }))
            .unwrap_or_else(|_| panic!("catalog unavailable"));

        receiver
    }
}

struct Fixture {
    library: Library,
    fake: Fake,
    task: Mutex<Option<CatalogTask>>,
}

impl Fixture {
    async fn new(albums: usize) -> Self {
        let fake = Fake::new().await;

        fake.upstream.lock().unwrap().albums =
            (1..=albums).map(|id| album(id as u128, "Album")).collect();

        let (library, task) = Library::new(config(fake.address), 1);

        Self {
            library,
            fake,
            task: Mutex::new(Some(task)),
        }
    }

    fn run(&self) -> JoinHandle<Result<()>> {
        let task = self.task.lock().unwrap().take().unwrap();

        tokio::spawn(task.run())
    }

    fn gate(&self, scope: Scope) -> Arc<Barrier> {
        let barrier = Barrier::new();

        self.fake
            .upstream
            .lock()
            .unwrap()
            .gates
            .insert(scope, barrier.clone());

        barrier
    }

    async fn expire(&self, scope: Scope) {
        self.library.control(Control::Expire(scope)).await;
    }

    async fn revisions(&self) -> Ledger {
        self.library.inspect(|task| task.state.ledger.clone()).await
    }

    async fn abort(&self, task: JoinHandle<Result<()>>) {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert_eq!(self.library.catalog.system_update_id().await, Err(FAILED));
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

fn metadata_query(id: &str) -> BrowseQuery {
    BrowseQuery {
        object_id: parse_id(id).unwrap(),
        mode: BrowseMode::Metadata,
        sort: SortOrder::Catalog,
    }
}

fn page_query(id: &str, start: u32, count: u32, sort: SortOrder) -> BrowseQuery {
    BrowseQuery {
        object_id: parse_id(id).unwrap(),
        mode: BrowseMode::DirectChildren {
            starting_index: start,
            requested_count: count,
        },
        sort,
    }
}

fn children(id: u128) -> BrowseQuery {
    page_query(
        &format!("album:{}", Uuid::from_u128(id)),
        0,
        0,
        SortOrder::Catalog,
    )
}

fn spawn_browse(library: &Library, query: BrowseQuery) -> JoinHandle<Result<BrowseResult, Fault>> {
    let library = library.clone();

    tokio::spawn(async move { library.browse(query).await })
}

async fn fault(task: JoinHandle<Result<BrowseResult, Fault>>, code: u16) {
    let result = timeout_at(Instant::now() + Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.err().map(Fault::code), Some(code));
}

#[tokio::test]
async fn shared_asset_keeps_album_identity_and_asset_only_resources() {
    let fixture = Fixture::new(2).await;
    let asset = Uuid::from_u128(42);

    for id in [1, 2] {
        fixture.fake.upstream.lock().unwrap().contents.insert(
            Uuid::from_u128(id),
            vec![item(42, Some("2024-01-01T00:00:00Z"), None)],
        );
    }

    let task = fixture.run();
    let first = fixture.library.browse(children(1)).await.unwrap();
    let second = fixture.library.browse(children(2)).await.unwrap();

    for (id, result) in [(1, &first), (2, &second)] {
        let album = Uuid::from_u128(id);
        assert_eq!(result.total_matches, 1);
        assert_eq!(result.objects.len(), 1);
        let object = &result.objects[0];
        assert_eq!(object.id(), ObjectId::Item { album, asset });
        assert_eq!(object.parent_id(), Some(ObjectId::Album(album)));

        let metadata = fixture
            .library
            .browse(metadata_query(&format!("album:{album}:asset:{asset}")))
            .await
            .unwrap();

        assert_eq!(metadata.objects, result.objects);
    }

    assert_eq!(first.objects[0].resources(), second.objects[0].resources());

    assert_eq!(
        first.objects[0]
            .resources()
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        [
            format!("http://192.0.2.1:8200/media/assets/{asset}/original"),
            format!("http://192.0.2.1:8200/media/assets/{asset}/preview"),
        ]
    );

    let ledger = fixture.revisions().await;
    let first_digest = ledger.albums[&Uuid::from_u128(1)].contents_digest.unwrap();
    let second_digest = ledger.albums[&Uuid::from_u128(2)].contents_digest.unwrap();
    assert_ne!(first_digest, second_digest);
    fixture.abort(task).await;
}

#[tokio::test]
async fn local_id_startup_event_outage_and_metadata_scopes() {
    let mut fixture = Fixture::new(2).await;
    let task = fixture.run();

    let events = tokio::spawn(fixture.library.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(1).await;

    for _ in 0..5 {
        assert_eq!(fixture.library.system_update_id().await, 1);
    }

    assert!(fixture.fake.upstream.lock().unwrap().requests.is_empty());
    fixture.fake.upstream.lock().unwrap().outage = true;

    fault(spawn_browse(&fixture.library, metadata_query("0")), 501).await;

    assert_eq!(fixture.library.system_update_id().await, 1);
    fixture.fake.upstream.lock().unwrap().outage = false;

    let root = fixture.library.browse(metadata_query("0")).await.unwrap();

    assert_eq!(root.total_matches, 1);
    assert_eq!(root.objects.len(), 1);
    assert_eq!(root.objects[0].title, "Photos & videos");
    assert_eq!(root.objects[0].parent_id(), None);
    assert_eq!(root.objects[0].class(), "object.container");
    assert_eq!(root.objects[0].child_count(), Some(2));
    assert!(root.objects[0].resources().is_empty());
    assert_eq!(root.update_id, 2);

    let metadata = fixture
        .library
        .browse(BrowseQuery {
            sort: SortOrder::DateDescending,
            ..metadata_query(&format!("album:{}", Uuid::from_u128(1)))
        })
        .await
        .unwrap();

    assert_eq!(metadata.update_id, 2);
    assert_eq!(metadata.total_matches, 1);
    assert!(metadata.objects[0].child_count().is_none());
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    fixture.expire(Scope::Root).await;
    fixture.fake.upstream.lock().unwrap().outage = true;
    fault(spawn_browse(&fixture.library, children(1)), 501).await;
    assert_eq!(fixture.library.system_update_id().await, 2);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
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
    let request = || page_query("0", 0, 0, SortOrder::Catalog);
    let before = fixture.library.browse(request()).await.unwrap();
    assert_eq!(before.objects[0].title, "Z");
    let revisions = fixture.revisions().await;

    let events = tokio::spawn(fixture.library.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(before.update_id).await;

    fixture.fake.upstream.lock().unwrap().albums[0]["endDate"] = json!("2025-01-01T00:00:00Z");

    let cached = fixture.library.browse(request()).await.unwrap();
    assert_eq!(cached.objects, before.objects);
    assert_eq!(cached.update_id, before.update_id);
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    fixture.expire(Scope::Root).await;
    let after = fixture.library.browse(request()).await.unwrap();
    assert_eq!(after.objects[0].title, "A");
    assert_eq!(after.objects[1], before.objects[0]);
    assert_eq!(after.update_id, before.update_id + 1);
    fixture.fake.event(after.update_id).await;
    let changed = fixture.revisions().await;
    assert_eq!(changed.system_update_id, after.update_id);
    let id = Uuid::from_u128(1);

    assert_eq!(
        changed.albums[&id].update_id,
        revisions.albums[&id].update_id + 1
    );

    assert_eq!(
        changed.albums[&Uuid::from_u128(2)],
        revisions.albums[&Uuid::from_u128(2)]
    );

    assert!(changed.albums.values().all(|a| a.contents_digest.is_none()));

    fixture.fake.upstream.lock().unwrap().albums.reverse();
    fixture.expire(Scope::Root).await;
    let same = fixture.library.browse(request()).await.unwrap();
    assert_eq!(same.objects, after.objects);
    assert_eq!(same.update_id, after.update_id);
    assert_eq!(fixture.revisions().await, changed);
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
        .browse(page_query("0", 1, 2, SortOrder::Catalog))
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
    assert_eq!(baseline.update_id, 3);

    assert_eq!(
        baseline
            .objects
            .iter()
            .map(|o| o.title.as_str())
            .collect::<Vec<_>>(),
        ["Photo 2", "Photo 1", "Photo 3"]
    );

    for (sort, titles) in [
        (SortOrder::DateAscending, ["Photo 1", "Photo 2", "Photo 3"]),
        (SortOrder::DateDescending, ["Photo 2", "Photo 1", "Photo 3"]),
    ] {
        let request = BrowseQuery {
            sort,
            ..children(1)
        };

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

    for (start, count, titles) in [
        (1, 1, &["Photo 2"][..]),
        (1, 0, &["Photo 2", "Photo 3"]),
        (3, 0, &[]),
        (u32::MAX, 10, &[]),
    ] {
        let result = fixture
            .library
            .browse(page_query(
                &format!("album:{}", Uuid::from_u128(1)),
                start,
                count,
                SortOrder::DateAscending,
            ))
            .await
            .unwrap();

        assert_eq!(result.total_matches, 3);
        assert_eq!(result.update_id, baseline.update_id);

        assert_eq!(
            result
                .objects
                .iter()
                .map(|o| o.title.as_str())
                .collect::<Vec<_>>(),
            titles,
            "start={start}, count={count}"
        );
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
        .browse(metadata_query(&appearance))
        .await
        .unwrap();

    assert_eq!(metadata.update_id, fixture.library.system_update_id().await);
    assert_eq!(metadata.total_matches, 1);

    fault(
        spawn_browse(
            &fixture.library,
            page_query(&appearance, 0, 0, SortOrder::Catalog),
        ),
        710,
    )
    .await;

    let wrong_album = format!("album:{}:asset:{}", Uuid::from_u128(2), Uuid::from_u128(1));

    fault(
        spawn_browse(&fixture.library, metadata_query(&wrong_album)),
        701,
    )
    .await;

    fault(
        spawn_browse(
            &fixture.library,
            page_query(&wrong_album, 0, 0, SortOrder::Catalog),
        ),
        701,
    )
    .await;

    fault(spawn_browse(&fixture.library, children(999)), 701).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    fixture.abort(task).await;
}

#[tokio::test]
async fn expiry_and_lru_refill_preserve_revisions_without_events() {
    let mut fixture = Fixture::new(2).await;

    let task = fixture.run();
    fixture
        .library
        .control(Control::Limits {
            albums: 1,
            bytes: CACHE_BYTES,
        })
        .await;
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();

    fixture
        .library
        .inspect(|task| assert!(!task.state.cache.albums.contains_key(&Uuid::from_u128(1))))
        .await;

    let before = fixture.revisions().await;

    let events = tokio::spawn(fixture.library.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(before.system_update_id).await;
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.revisions().await, before);
    fixture.expire(Scope::Root).await;
    fixture.expire(Scope::Album(Uuid::from_u128(1))).await;

    let expired = fixture
        .library
        .inspect(|task| task.state.cache.albums[&Uuid::from_u128(1)].completed)
        .await;

    fixture.library.browse(children(1)).await.unwrap();

    fixture
        .library
        .inspect(move |task| {
            assert!(task.state.cache.albums[&Uuid::from_u128(1)].completed > expired)
        })
        .await;

    assert_eq!(fixture.revisions().await, before);
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
        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .contents
            .insert(Uuid::from_u128(id), vec![item(id, None, None)]);
    }

    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();

    let (root_bytes, contents_bytes) = fixture
        .library
        .inspect(|task| {
            let state = &task.state;
            let root_bytes = state.cache.root.as_ref().unwrap().snapshot.bytes;
            let contents_bytes = state.cache.albums[&Uuid::from_u128(1)].snapshot.bytes;
            assert_eq!(state.cache.albums.len(), 2);
            assert_eq!(state.ledger.albums.len(), 2);

            assert!(
                state
                    .cache
                    .albums
                    .values()
                    .all(|a| a.snapshot.bytes == contents_bytes)
            );

            assert!(
                state
                    .ledger
                    .albums
                    .values()
                    .all(|album| album.contents_digest.is_some())
            );

            (root_bytes, contents_bytes)
        })
        .await;

    fixture
        .library
        .control(Control::Limits {
            albums: RESIDENT_ALBUMS,
            bytes: root_bytes + 2 * contents_bytes,
        })
        .await;

    let before = fixture.revisions().await;
    fixture.fake.upstream.lock().unwrap().albums[0]["albumName"] = json!("Albums");
    fixture.expire(Scope::Root).await;

    fixture.library.browse(metadata_query("0")).await.unwrap();

    let retained = before.clone();

    fixture
        .library
        .inspect(move |task| {
            let state = &task.state;
            let grown_bytes = state.cache.root.as_ref().unwrap().snapshot.bytes;
            assert_eq!(grown_bytes, root_bytes + 1);
            assert!(grown_bytes + contents_bytes <= state.cache.byte_limit);
            assert!(grown_bytes + 2 * contents_bytes > state.cache.byte_limit);
            assert_eq!(state.cache.albums.len(), 1);
            assert!(!state.cache.albums.contains_key(&Uuid::from_u128(1)));
            assert_eq!(state.ledger.albums.len(), 2);

            for (id, album) in &state.ledger.albums {
                assert_eq!(album.contents_digest, retained.albums[id].contents_digest);
            }
        })
        .await;

    let grown = fixture.revisions().await;
    assert_eq!(grown.system_update_id, before.system_update_id + 1);

    assert_eq!(
        grown.albums[&Uuid::from_u128(1)].update_id,
        before.albums[&Uuid::from_u128(1)].update_id + 1
    );

    assert_eq!(
        grown.albums[&Uuid::from_u128(2)],
        before.albums[&Uuid::from_u128(2)]
    );

    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.revisions().await, grown);
    fixture.abort(task).await;
}

#[tokio::test]
async fn metadata_contents_have_independent_counters_and_restart_forgets_history() {
    let mut fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    let before = fixture.revisions().await;
    fixture.fake.upstream.lock().unwrap().albums[0]["albumName"] = json!("Renamed");
    fixture.expire(Scope::Root).await;

    let result = fixture
        .library
        .browse(metadata_query(&format!("album:{}", Uuid::from_u128(1))))
        .await
        .unwrap();

    let renamed = fixture.revisions().await;
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

    fixture.expire(Scope::Album(Uuid::from_u128(1))).await;
    fixture.library.browse(children(1)).await.unwrap();
    fixture.abort(task).await;
    let (library, task) = Library::new(config(fixture.fake.address), 0);
    fixture.library = library;
    *fixture.task.lock().unwrap() = Some(task);

    let task = fixture.run();
    assert_eq!(fixture.library.system_update_id().await, 0);
    assert!(fixture.revisions().await.albums.is_empty());

    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    assert_eq!(fixture.library.system_update_id().await, 3);
    assert_eq!(
        fixture.revisions().await.albums[&Uuid::from_u128(1)].update_id,
        2
    );
    assert_eq!(fixture.fake.calls("/api/server/version"), 2);
    fixture.abort(task).await;
}

#[tokio::test]
async fn serial_shared_refreshes_survive_callers_and_keep_local_reads_live() {
    let fixture = Fixture::new(5).await;
    let task = fixture.run();
    fixture.library.browse(metadata_query("0")).await.unwrap();
    let barrier = fixture.gate(Scope::Album(Uuid::from_u128(1)));
    let first = spawn_browse(&fixture.library, children(1));
    barrier.entered().await;
    let mut independent = Box::pin(fixture.library.browse(children(1)));
    assert!(futures_util::poll!(&mut independent).is_pending());
    first.abort();
    assert!(first.await.err().unwrap().is_cancelled());
    let second = spawn_browse(&fixture.library, children(2));
    assert_eq!(fixture.library.system_update_id().await, 2);
    fixture.library.browse(metadata_query("0")).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 1);
    assert!(!second.is_finished());
    barrier.release.add_permits(1);
    independent.await.unwrap();
    second.await.unwrap().unwrap();
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);
    assert_eq!(fixture.library.system_update_id().await, 4);
    fixture.abort(task).await;
}

#[tokio::test]
async fn pending_bounds_prune_cancelled_and_expired_demand_without_fetching_it() {
    let fixture = Fixture::new(3).await;
    let task = fixture.run();
    fixture.library.browse(metadata_query("0")).await.unwrap();
    let gate = fixture.gate(Scope::Album(Uuid::from_u128(1)));
    let first = spawn_browse(&fixture.library, children(1));
    gate.entered().await;
    let mut replies = Vec::new();

    for _ in 1..REQUESTS {
        replies.push(
            fixture
                .library
                .enqueue(children(2), Instant::now() + REFRESH_TIMEOUT),
        );
    }

    assert_eq!(
        fixture.library.inspect(|task| task.pending.len()).await,
        REQUESTS
    );
    fixture.library.browse(metadata_query("0")).await.unwrap();
    fault(spawn_browse(&fixture.library, children(3)), 501).await;
    replies.clear();
    let expired = fixture.library.enqueue(children(2), Instant::now());
    assert_eq!(expired.await.unwrap().err(), Some(FAILED));
    let later = fixture
        .library
        .enqueue(children(3), Instant::now() + REFRESH_TIMEOUT);
    assert_eq!(fixture.library.inspect(|task| task.pending.len()).await, 2);
    gate.release.add_permits(1);
    first.await.unwrap().unwrap();
    assert!(later.await.unwrap().is_ok());
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);
    assert!(
        fixture.revisions().await.albums[&Uuid::from_u128(2)]
            .contents_digest
            .is_none()
    );
    fixture.abort(task).await;
}

#[tokio::test]
async fn one_failed_attempt_replies_to_all_waiters_and_next_request_can_recover() {
    let fixture = Fixture::new(1).await;
    let task = fixture.run();
    fixture.library.browse(metadata_query("0")).await.unwrap();
    fixture.fake.upstream.lock().unwrap().outage = true;
    let gate = fixture.gate(Scope::Album(Uuid::from_u128(1)));
    let first = spawn_browse(&fixture.library, children(1));
    gate.entered().await;
    let joined = fixture
        .library
        .enqueue(children(1), Instant::now() + REFRESH_TIMEOUT);
    assert_eq!(fixture.library.inspect(|task| task.pending.len()).await, 2);
    gate.release.add_permits(1);
    fault(first, 501).await;
    assert_eq!(joined.await.unwrap().err(), Some(FAILED));
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 1);
    fixture.fake.upstream.lock().unwrap().outage = false;
    gate.release.add_permits(1);
    fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);
    fixture.abort(task).await;
}

#[tokio::test]
async fn caller_deadline_does_not_cancel_shared_work_or_start_expired_queued_work() {
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(metadata_query("0")).await.unwrap();
    let gate = fixture.gate(Scope::Album(Uuid::from_u128(1)));
    let short = fixture
        .library
        .enqueue(children(1), Instant::now() + Duration::from_secs(1));
    gate.entered().await;
    let queued = fixture
        .library
        .enqueue(children(2), Instant::now() + Duration::from_secs(1));
    let joined = fixture
        .library
        .enqueue(children(1), Instant::now() + REFRESH_TIMEOUT);
    assert_eq!(fixture.library.inspect(|task| task.pending.len()).await, 3);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(short.await.unwrap().err(), Some(FAILED));
    assert_eq!(queued.await.unwrap().err(), Some(FAILED));
    tokio::time::resume();
    gate.release.add_permits(1);
    assert!(joined.await.unwrap().is_ok());
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 1);
    fixture.abort(task).await;
}

#[tokio::test]
async fn full_or_closed_mailbox_fails_without_waiting_to_enqueue() {
    let fake = Fake::new().await;
    let (library, task) = Library::new(config(fake.address), 1);
    let mut replies = Vec::new();

    for _ in 0..REQUESTS {
        replies.push(library.enqueue(metadata_query("0"), Instant::now() + REFRESH_TIMEOUT));
    }

    assert_eq!(library.catalog.system_update_id().await, Err(FAILED));
    assert_eq!(
        library.browse(metadata_query("0")).await.err(),
        Some(FAILED)
    );
    drop(task);

    for reply in replies {
        assert!(reply.await.is_err());
    }

    assert_eq!(library.catalog.system_update_id().await, Err(FAILED));
    assert!(fake.upstream.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn expired_publication_leaves_snapshot_and_revisions_untouched() {
    let fake = Fake::new().await;
    let (_, mut task) = Library::new(config(fake.address), 1);
    let candidate = Candidate::Root(task.source.root().await.unwrap());
    assert!(task.publish(candidate, Instant::now()).is_err());
    assert!(task.state.cache.root.is_none());
    assert_eq!(task.state.ledger.system_update_id, 1);

    fake.upstream.lock().unwrap().albums.push(album(1, "Album"));
    let candidate = Candidate::Root(task.source.root().await.unwrap());

    task.publish(candidate, Instant::now() + REFRESH_TIMEOUT)
        .unwrap();

    let before = task.state.ledger.clone();
    let id = Uuid::from_u128(1);
    let candidate = Candidate::Album(id, task.source.contents(id).await.unwrap());
    assert!(task.publish(candidate, Instant::now()).is_err());
    assert!(task.state.cache.albums.is_empty());
    assert_eq!(task.state.ledger, before);
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
        let (library, task) = Library::new(config(listener.local_addr().unwrap()), 1);

        tokio::time::pause();

        // Advance only explicitly while exchanging loopback responses.
        let clock_guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });

        let task = tokio::spawn(task.run());
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
        let before = library.inspect(|task| task.state.ledger.clone()).await;

        // Earlier requests consumed six seconds; a new page gets no new window.
        tokio::time::advance(Duration::from_secs(if stage == 0 { 26 } else { 20 })).await;

        clock_guard.abort();
        assert!(clock_guard.await.unwrap_err().is_cancelled());
        fault(caller, 501).await;
        assert_eq!(
            library.inspect(|task| task.state.ledger.clone()).await,
            before
        );

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut [0; 1]))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(library.catalog.system_update_id().await, Err(FAILED));
        tokio::time::resume();
    }
}

#[tokio::test]
async fn queued_album_rechecks_root_membership_after_serial_publication() {
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.expire(Scope::Album(Uuid::from_u128(1))).await;
    let barrier = fixture.gate(Scope::Album(Uuid::from_u128(1)));
    let caller = spawn_browse(&fixture.library, children(1));
    barrier.entered().await;
    fixture.fake.upstream.lock().unwrap().albums.remove(1);
    fixture.expire(Scope::Root).await;
    let removed = spawn_browse(&fixture.library, children(2));
    assert_eq!(fixture.library.system_update_id().await, 3);
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    barrier.release.add_permits(1);
    caller.await.unwrap().unwrap();
    fault(removed, 701).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);
    assert_eq!(fixture.fake.calls("/api/albums"), 2);

    assert!(
        !fixture
            .revisions()
            .await
            .albums
            .contains_key(&Uuid::from_u128(2))
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn task_exit_closes_handles_and_pending_replies() {
    let fixture = Fixture::new(0).await;
    let task = fixture.run();
    let barrier = fixture.gate(Scope::Root);
    let caller = spawn_browse(&fixture.library, metadata_query("0"));
    barrier.entered().await;

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fault(caller, 501).await;
    assert_eq!(
        fixture.library.catalog.system_update_id().await,
        Err(FAILED)
    );
}

#[tokio::test]
async fn abandoned_refresh_completes_and_expiry_alone_never_does_work() {
    let fixture = Fixture::new(0).await;
    let task = fixture.run();
    let barrier = fixture.gate(Scope::Root);

    let caller = spawn_browse(&fixture.library, metadata_query("0"));
    barrier.entered().await;
    caller.abort();
    let _ = caller.await;

    barrier.release.add_permits(1);
    fixture.library.browse(metadata_query("0")).await.unwrap();
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    assert_eq!(fixture.library.system_update_id().await, 2);
    let before = fixture.revisions().await;
    let calls = fixture.fake.upstream.lock().unwrap().requests.len();
    tokio::time::pause();
    tokio::time::advance(FRESHNESS).await;
    tokio::time::resume();
    assert_eq!(fixture.library.system_update_id().await, 2);
    assert_eq!(fixture.revisions().await, before);
    assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), calls);

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

    fixture.library.browse(metadata_query("0")).await.unwrap();

    let mut children_waiter = Box::pin(fixture.library.browse(children(1)));
    assert!(futures_util::poll!(&mut children_waiter).is_pending());
    let appearance = format!("album:{}:asset:{}", Uuid::from_u128(1), Uuid::from_u128(1));

    let mut metadata_waiter = Box::pin(fixture.library.browse(metadata_query(&appearance)));

    assert!(futures_util::poll!(&mut metadata_waiter).is_pending());

    // A third waiter observes publication without polling the first two again.
    fixture.library.browse(children(1)).await.unwrap();

    let (album_id, system_id) = fixture
        .library
        .inspect(|task| {
            let state = &task.state;

            (
                state.ledger.albums[&Uuid::from_u128(1)].update_id,
                state.ledger.system_update_id,
            )
        })
        .await;

    fixture
        .library
        .control(Control::Limits {
            albums: 1,
            bytes: CACHE_BYTES,
        })
        .await;
    fixture.library.browse(children(2)).await.unwrap();

    fixture
        .library
        .inspect(|task| {
            let state = &task.state;
            assert!(state.cache.albums.contains_key(&Uuid::from_u128(2)));
            assert!(!state.cache.albums.contains_key(&Uuid::from_u128(1)));
        })
        .await;

    let result = children_waiter.await.unwrap();
    assert_eq!(result.total_matches, 1);
    assert_eq!(result.objects[0].id().to_string(), appearance);
    assert_eq!(result.objects[0].title, "Photo 1");
    assert_eq!(result.update_id, album_id);

    // Later root removal cannot change the already completed item view.
    fixture.fake.upstream.lock().unwrap().albums.remove(0);
    fixture.expire(Scope::Root).await;

    fixture.library.browse(metadata_query("0")).await.unwrap();

    let metadata = metadata_waiter.await.unwrap();
    assert_eq!(metadata.objects, result.objects);
    assert_eq!(metadata.update_id, system_id);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    assert!(
        !fixture
            .revisions()
            .await
            .albums
            .contains_key(&Uuid::from_u128(1))
    );

    fixture.abort(task).await;
}

#[tokio::test]
async fn fresh_hits_and_unchanged_refreshes_pin_coherent_views() {
    let fixture = Fixture::new(2).await;

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .insert(Uuid::from_u128(1), vec![item(1, None, None)]);

    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();

    let root_hit = fixture
        .library
        .catalog
        .view(metadata_query("0"), Instant::now() + REFRESH_TIMEOUT)
        .await
        .unwrap();

    let album_hit = fixture
        .library
        .catalog
        .view(children(1), Instant::now() + REFRESH_TIMEOUT)
        .await
        .unwrap();

    let album_metadata_hit = fixture
        .library
        .catalog
        .view(
            metadata_query(&format!("album:{}", Uuid::from_u128(1))),
            Instant::now() + REFRESH_TIMEOUT,
        )
        .await
        .unwrap();

    let item_hit = fixture
        .library
        .catalog
        .view(
            metadata_query(&format!(
                "album:{}:asset:{}",
                Uuid::from_u128(1),
                Uuid::from_u128(1)
            )),
            Instant::now() + REFRESH_TIMEOUT,
        )
        .await
        .unwrap();

    let before = fixture.revisions().await;

    assert_ne!(
        before.system_update_id,
        before.albums[&Uuid::from_u128(1)].update_id
    );

    assert_eq!(root_hit.update_id, before.system_update_id);

    assert_eq!(
        album_hit.update_id,
        before.albums[&Uuid::from_u128(1)].update_id
    );

    assert!(Arc::ptr_eq(&root_hit.root, &album_hit.root));
    assert!(root_hit.contents.is_none());
    let contents = album_hit.contents.as_ref().unwrap();

    assert_eq!(
        before.albums[&Uuid::from_u128(1)].contents_digest.as_ref(),
        Some(&contents.digest)
    );

    fixture.expire(Scope::Root).await;
    let mut root_waiter = Box::pin(fixture.library.browse(metadata_query("0")));
    assert!(futures_util::poll!(&mut root_waiter).is_pending());

    let mut album_waiter = Box::pin(
        fixture
            .library
            .browse(metadata_query(&format!("album:{}", Uuid::from_u128(1)))),
    );

    assert!(futures_util::poll!(&mut album_waiter).is_pending());
    let before = fixture.revisions().await;
    fixture.library.browse(metadata_query("0")).await.unwrap();
    assert_eq!(fixture.revisions().await, before);
    fixture.fake.upstream.lock().unwrap().albums.remove(0);
    fixture.expire(Scope::Root).await;

    let removed = fixture.library.browse(metadata_query("0")).await.unwrap();

    assert_eq!(removed.objects[0].child_count(), Some(1));
    let old_root = root_waiter.await.unwrap();
    assert_eq!(old_root.objects[0].child_count(), Some(2));
    assert_eq!(old_root.update_id, before.system_update_id);
    let old_album = album_waiter.await.unwrap();

    assert_eq!(
        old_album.objects[0],
        album_hit.root.albums[&Uuid::from_u128(1)].metadata.object
    );

    assert_eq!(
        old_album.update_id,
        before.albums[&Uuid::from_u128(1)].update_id
    );

    let pinned_root = root_hit.browse(&fixture.library.catalog.collator).unwrap();
    assert_eq!(pinned_root.objects[0].child_count(), Some(2));
    assert_eq!(pinned_root.update_id, before.system_update_id);

    let pinned_album = album_hit.browse(&fixture.library.catalog.collator).unwrap();

    assert_eq!(
        pinned_album.update_id,
        before.albums[&Uuid::from_u128(1)].update_id
    );

    assert_eq!(pinned_album.objects[0].title, "Photo 1");

    let pinned_metadata = album_metadata_hit
        .browse(&fixture.library.catalog.collator)
        .unwrap();

    assert_eq!(pinned_metadata.update_id, pinned_album.update_id);
    assert_eq!(pinned_metadata.objects[0], old_album.objects[0]);

    let pinned_item = item_hit.browse(&fixture.library.catalog.collator).unwrap();
    assert_eq!(pinned_item.update_id, before.system_update_id);
    assert_eq!(pinned_item.objects, pinned_album.objects);
    assert_eq!(fixture.fake.calls("/api/albums"), 3);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    fixture
        .library
        .inspect(|task| assert!(!task.state.cache.albums.contains_key(&Uuid::from_u128(1))))
        .await;

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
    let root = fixture.revisions().await;
    assert_eq!(root.system_update_id, 2);
    assert!(root.albums[&Uuid::from_u128(1)].contents_digest.is_none());
    assert_eq!(fixture.library.system_update_id().await, 2);

    let events = tokio::spawn(fixture.library.events.clone().run());

    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(2).await;
    fixture.fake.upstream.lock().unwrap().contents.clear();
    let result = fixture.library.browse(children(1)).await.unwrap();
    assert_eq!(result.update_id, 3);
    assert_eq!(result.total_matches, 0);
    fixture.fake.event(3).await;
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    events.abort();
    assert!(events.await.unwrap_err().is_cancelled());
    fixture.abort(task).await;
}
