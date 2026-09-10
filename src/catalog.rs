//! Browse-driven snapshots and service-owned, durably published refreshes.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, watch},
    task::JoinSet,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    config::Config,
    eventing::Subscriptions,
    immich::{self, Contents, FetchBudget, Immich, ObjectId, Root},
    limits,
    protocol::{Action, Fault, Object},
    revisions::{AlbumRevision, Ledger, Store},
    server::{BrowseResult, Catalog},
};

const FAILED: Fault = Fault { code: 501 };
const MISSING: Fault = Fault { code: 701 };

#[derive(Clone)]
pub struct Library {
    inner: Arc<Inner>,
}

struct Inner {
    source: Immich,
    config: Config,
    store: Store,
    events: Subscriptions,
    stop: CancellationToken,
    state: Mutex<State>,
    commit: AsyncMutex<()>,
    permits: Arc<Semaphore>,
    supervisor: Mutex<Supervisor>,
    wake: Notify,
    bounds: Bounds,
    #[cfg(test)]
    publication: Mutex<Option<Arc<tests::Barrier>>>,
    #[cfg(test)]
    prepared: Mutex<Option<(Scope, Arc<tests::Barrier>)>>,
}

struct Bounds {
    freshness: Duration,
    albums: usize,
    bytes: usize,
    preparation: Duration,
}

struct Supervisor {
    tasks: JoinSet<()>,
    running: bool,
    failed: bool,
}

struct State {
    ledger: Arc<Ledger>,
    root: Option<Cached<Root>>,
    albums: BTreeMap<Uuid, Cached<Contents>>,
    flights: BTreeMap<Scope, watch::Receiver<Option<RefreshResult>>>,
}

// A response pins one publication independently of subsequent cache eviction.
struct View {
    root: Arc<Root>,
    contents: Option<Arc<Contents>>,
    ledger: Arc<Ledger>,
}

type RefreshResult = Result<Arc<View>, Fault>;

impl State {
    fn view(&self, scope: Scope) -> RefreshResult {
        let contents = match scope {
            Scope::Root => None,
            Scope::Album(id) => Some(self.albums.get(&id).ok_or(FAILED)?.snapshot.clone()),
        };

        Ok(Arc::new(View {
            root: self.root.as_ref().ok_or(FAILED)?.snapshot.clone(),
            contents,
            ledger: self.ledger.clone(),
        }))
    }
}

struct Cached<T> {
    snapshot: Arc<T>,
    completed: Instant,
    used: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Scope {
    Root,
    Album(Uuid),
}

enum Token {
    Root(Option<String>),
    Album(Uuid, AlbumRevision),
}

impl Token {
    fn applicable(&self, ledger: &Ledger) -> bool {
        match self {
            Self::Root(digest) => &ledger.root_digest == digest,
            Self::Album(id, revision) => {
                ledger.albums.get(id) == Some(revision) && revision.present
            }
        }
    }
}

enum Candidate {
    Root(Arc<Root>),
    Album(Uuid, Arc<Contents>),
}

// Created before spawning, so even cancellation before the first poll cleans up.
struct Flight {
    library: Library,
    scope: Scope,
    sender: watch::Sender<Option<RefreshResult>>,
    permit: Option<OwnedSemaphorePermit>,
    result: RefreshResult,
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut state = self
            .library
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        state.flights.remove(&self.scope);
        drop(self.permit.take());
        self.sender.send_replace(Some(self.result.clone()));
        drop(state);
        self.library.inner.wake.notify_one();
    }
}

// Persistence's own guard ends at disk completion; this one extends ownership
// through catalog assignment AND subscriber publication, including unwinding.
struct Publication;

impl Drop for Publication {
    fn drop(&mut self) {
        tracing::error!("catalog commit did not complete publication; terminating");
        std::process::exit(1);
    }
}

struct Running(Library);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.inner.stop.cancel();
        self.0.inner.supervisor.lock().unwrap().tasks.abort_all();
        self.0.inner.wake.notify_one();
    }
}

impl Library {
    /// The caller must restart and persist the ledger before construction/admission.
    pub fn new(
        config: Config,
        store: Store,
        ledger: Ledger,
        events: Subscriptions,
        stop: CancellationToken,
    ) -> Result<Self> {
        let source = Immich::new(
            config.api_base.clone(),
            config.api_key.clone(),
            config.listen_address,
            config.friendly_name.clone(),
        )?;

        events.publish(ledger.system_update_id);

        Ok(Self {
            inner: Arc::new(Inner {
                source,
                config,
                store,
                events,
                stop,
                state: Mutex::new(State {
                    ledger: Arc::new(ledger),
                    root: None,
                    albums: BTreeMap::new(),
                    flights: BTreeMap::new(),
                }),
                commit: AsyncMutex::new(()),
                permits: Arc::new(Semaphore::new(limits::REFRESHES)),
                supervisor: Mutex::new(Supervisor {
                    tasks: JoinSet::new(),
                    running: false,
                    failed: false,
                }),
                wake: Notify::new(),
                bounds: Bounds {
                    freshness: limits::CATALOG_FRESHNESS,
                    albums: limits::RESIDENT_ALBUMS,
                    bytes: limits::CACHE_BYTES,
                    preparation: limits::REFRESH_PREPARATION_TIMEOUT,
                },
                #[cfg(test)]
                publication: Mutex::new(None),
                #[cfg(test)]
                prepared: Mutex::new(None),
            }),
        })
    }

    /// Supervise until stopped, then drain preparation cancellation and any commit.
    pub async fn run(&self) -> Result<()> {
        {
            let mut supervisor = self.inner.supervisor.lock().unwrap();
            ensure!(!supervisor.running, "catalog supervisor already started");
            supervisor.running = true;
        }

        let _running = Running(self.clone());

        loop {
            let notified = self.inner.wake.notified();

            let finished = std::future::poll_fn(|cx| {
                let mut supervisor = self.inner.supervisor.lock().unwrap();

                // poll_join_next registers a completion waker, including the narrow
                // interval between Flight::drop and Tokio marking the task finished.
                match supervisor.tasks.poll_join_next(cx) {
                    Poll::Ready(Some(result)) => {
                        if result.is_err() {
                            supervisor.failed = true;
                            self.inner.stop.cancel();
                        }

                        Poll::Ready(false)
                    }

                    Poll::Ready(None) if self.inner.stop.is_cancelled() => Poll::Ready(true),
                    _ => Poll::Pending,
                }
            });

            tokio::select! {
                done = finished => {
                    if done {
                        ensure!(!self.inner.supervisor.lock().unwrap().failed, "catalog refresh task failed");

                        return Ok(());
                    }
                }

                _ = notified => {}

                _ = self.inner.stop.cancelled(), if !self.inner.stop.is_cancelled() => {}
            }
        }
    }

    async fn fresh(&self, scope: Scope) -> RefreshResult {
        let mut receiver = {
            let mut state = self.inner.state.lock().unwrap();

            if self.inner.stop.is_cancelled() {
                return Err(FAILED);
            }

            let now = Instant::now();

            let fresh = match scope {
                Scope::Root => state.root.as_mut().map(|cached| {
                    cached.used = now;

                    now.duration_since(cached.completed) < self.inner.bounds.freshness
                }),

                Scope::Album(id) => {
                    if !state
                        .ledger
                        .albums
                        .get(&id)
                        .is_some_and(|album| album.present)
                    {
                        return Err(MISSING);
                    }

                    state.albums.get_mut(&id).map(|cached| {
                        cached.used = now;

                        now.duration_since(cached.completed) < self.inner.bounds.freshness
                    })
                }
            };

            if fresh == Some(true) {
                return state.view(scope);
            }

            if let Some(receiver) = state.flights.get(&scope) {
                receiver.clone()
            } else {
                let mut supervisor = self.inner.supervisor.lock().unwrap();

                // Reap on admission too: finished handles cannot accumulate when
                // Browse traffic runs ahead of the supervisor.
                while let Some(result) = supervisor.tasks.try_join_next() {
                    supervisor.failed |= result.is_err();
                }

                if supervisor.failed {
                    self.inner.stop.cancel();
                    self.inner.wake.notify_one();

                    return Err(FAILED);
                }

                let permit = self
                    .inner
                    .permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| FAILED)?;

                let token = match scope {
                    Scope::Root => Token::Root(state.ledger.root_digest.clone()),
                    Scope::Album(id) => Token::Album(id, state.ledger.albums[&id].clone()),
                };

                let (sender, receiver) = watch::channel(None);
                state.flights.insert(scope, receiver.clone());

                let mut flight = Flight {
                    library: self.clone(),
                    scope,
                    sender,
                    permit: Some(permit),
                    result: Err(FAILED),
                };

                let library = self.clone();
                let deadline = now + self.inner.bounds.preparation;

                supervisor.tasks.spawn(async move {
                    flight.result =
                        library
                             .refresh(scope, token, deadline)
                             .await
                             .inspect(|view| {
                                 tracing::debug!(?scope, update_id = view.ledger.system_update_id, elapsed_ms = now.elapsed().as_millis(), "catalog refresh completed");
                             })
                             .map_err(|error| {
                                // Immich and ledger errors contain only sanitized diagnostics.
                                 tracing::warn!(?scope, %error, elapsed_ms = now.elapsed().as_millis(), "catalog refresh failed");

                                FAILED
                            });

                    drop(flight);
                });

                self.inner.wake.notify_one();

                receiver
            }
        };

        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }

            receiver.changed().await.map_err(|_| FAILED)?;
        }
    }

    async fn refresh(&self, scope: Scope, token: Token, preparation: Instant) -> Result<Arc<View>> {
        ensure!(
            !self.inner.stop.is_cancelled() && Instant::now() < preparation,
            "catalog preparation stopped or expired"
        );

        let prepare = async {
            let budget = FetchBudget {
                deadline: preparation,
                stop: self.inner.stop.clone(),
            };

            let candidate = match scope {
                Scope::Root => Candidate::Root(Arc::new(self.inner.source.root(&budget).await?)),
                Scope::Album(id) => {
                    Candidate::Album(id, Arc::new(self.inner.source.contents(id, &budget).await?))
                }
            };

            #[cfg(test)]
            {
                let barrier = self.inner.prepared.lock().unwrap().clone();

                if let Some((at, barrier)) = barrier
                    && at == scope
                {
                    barrier.entered.notify_one();
                    barrier.release.acquire().await.unwrap().forget();
                    assert!(!barrier.panic, "injected preparation panic");
                }
            }

            let gate = self.inner.commit.lock().await;

            Ok::<_, anyhow::Error>((candidate, gate))
        };

        let (candidate, _gate) = tokio::select! {
            biased;
            _ = self.inner.stop.cancelled() => return Err(anyhow!("catalog preparation stopped")),

            result = timeout_at(preparation, prepare) => {
                result.map_err(|_| anyhow!("catalog preparation deadline exceeded"))??
            }
        };

        ensure!(
            !self.inner.stop.is_cancelled() && Instant::now() < preparation,
            "catalog preparation stopped or expired"
        );

        let deadline = Instant::now() + limits::COMMIT_TIMEOUT;

        let (ledger, root_bytes) = {
            let state = self.inner.state.lock().unwrap();
            ensure!(token.applicable(&state.ledger), "stale catalog candidate");

            (
                state.ledger.clone(),
                state.root.as_ref().map_or(0, |root| root.snapshot.bytes),
            )
        };

        let next = match &candidate {
            Candidate::Root(root) => {
                ensure!(
                    root.bytes <= self.inner.bounds.bytes,
                    "root exceeds cache budget"
                );

                let albums = root
                    .albums
                    .iter()
                    .map(|(id, album)| (*id, album.digest.clone()))
                    .collect();

                ledger.root_transition(&root.digest, &albums)?
            }

            Candidate::Album(id, contents) => {
                ensure!(
                    self.inner.bounds.albums > 0
                        && contents.bytes <= self.inner.bounds.bytes.saturating_sub(root_bytes),
                    "album exceeds cache budget"
                );

                ledger.contents_transition(*id, &contents.digest)?
            }
        };

        ensure!(
            !self.inner.stop.is_cancelled()
                && Instant::now() < preparation
                && Instant::now() < deadline,
            "catalog preparation stopped or expired"
        );

        let changed = next.is_some();
        let guard = if changed { Some(Publication) } else { None };

        let publish = async {
            if let Some(next) = &next {
                self.inner.store.persist(next.clone()).await;
            }

            #[cfg(test)]
            {
                let barrier = self.inner.publication.lock().unwrap().clone();

                if let Some(barrier) = barrier {
                    barrier.entered.notify_one();
                    barrier.release.acquire().await.unwrap().forget();
                }
            }

            let mut state = self.inner.state.lock().unwrap();
            ensure!(
                token.applicable(&state.ledger),
                "stale catalog candidate at publication"
            );
            ensure!(
                Instant::now() < deadline,
                "catalog publication deadline exceeded"
            );

            if let Some(next) = next {
                let State { ledger, albums, .. } = &mut *state;

                albums.retain(|id, _| {
                    let before = ledger.albums.get(id);
                    let after = next.albums.get(id);

                    after.is_some_and(|album| album.present)
                        && before.map(|album| album.present) == after.map(|album| album.present)
                });

                state.ledger = Arc::new(next);
            }

            let now = Instant::now();

            match candidate {
                Candidate::Root(snapshot) => {
                    state.root = Some(Cached {
                        snapshot,
                        completed: now,
                        used: now,
                    });
                }

                Candidate::Album(id, snapshot) => {
                    state.albums.insert(
                        id,
                        Cached {
                            snapshot,
                            completed: now,
                            used: now,
                        },
                    );
                }
            }

            while state.albums.len() > self.inner.bounds.albums
                || state.root.as_ref().map_or(0, |root| root.snapshot.bytes)
                    + state
                        .albums
                        .values()
                        .map(|album| album.snapshot.bytes)
                        .sum::<usize>()
                    > self.inner.bounds.bytes
            {
                let oldest = state
                    .albums
                    .iter()
                    .filter(|(id, _)| scope != Scope::Album(**id))
                    .min_by_key(|(id, cached)| (cached.used, **id))
                    .map(|(id, _)| *id);

                if let Some(oldest) = oldest {
                    state.albums.remove(&oldest);
                } else {
                    return Err(anyhow!("root exceeds cache budget"));
                }
            }

            if changed {
                self.inner.events.publish(state.ledger.system_update_id);
            }

            let view = state
                .view(scope)
                .map_err(|_| anyhow!("catalog publication missing snapshot"))?;

            // Freshness starts after the entire publication boundary, not fetch.
            let completed = Instant::now();

            match scope {
                Scope::Root => state.root.as_mut().unwrap().completed = completed,
                Scope::Album(id) => {
                    if let Some(cached) = state.albums.get_mut(&id) {
                        cached.completed = completed;
                    }
                }
            }

            ensure!(
                completed < deadline,
                "catalog publication deadline exceeded"
            );

            Ok(view)
        };

        let result = timeout_at(deadline, publish)
            .await
            .map_err(|_| anyhow!("catalog commit deadline exceeded"))?;

        let view = result?;
        std::mem::forget(guard);

        Ok(view)
    }
}

impl Catalog for Library {
    fn system_update_id(&self) -> u32 {
        self.inner.state.lock().unwrap().ledger.system_update_id
    }

    async fn browse(&self, action: Action) -> Result<BrowseResult, Fault> {
        let invalid = Fault { code: 402 };

        if action.name != "Browse" {
            return Err(Fault { code: 401 });
        }

        let argument = |name: &str| {
            action
                .arguments
                .get(name)
                .map(String::as_str)
                .ok_or(invalid)
        };

        let id = immich::parse_id(argument("ObjectID")?)?;

        let metadata = match argument("BrowseFlag")? {
            "BrowseMetadata" => true,
            "BrowseDirectChildren" => false,
            _ => return Err(invalid),
        };

        let number = |name| {
            let value = argument(name)?;

            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid);
            }

            value.parse::<u32>().map_err(|_| invalid)
        };

        let start = number("StartingIndex")? as usize;
        let count = number("RequestedCount")? as usize;
        argument("Filter")?;

        let sort = match argument("SortCriteria")? {
            "" => None,
            "+dc:date" => Some(false),
            "-dc:date" => Some(true),
            _ => return Err(Fault { code: 709 }),
        };

        if metadata && start != 0 {
            return Err(invalid);
        }

        let mut view = self.fresh(Scope::Root).await?;

        match id {
            ObjectId::Album(album) if !metadata => view = self.fresh(Scope::Album(album)).await?,
            ObjectId::Item { album, .. } => view = self.fresh(Scope::Album(album)).await?,
            _ => {}
        }

        let View {
            root,
            contents,
            ledger,
        } = &*view;

        let update_id = match id {
            ObjectId::Root => ledger.system_update_id,

            ObjectId::Album(album) | ObjectId::Item { album, .. } => {
                if !root.albums.contains_key(&album) {
                    return Err(MISSING);
                }

                if matches!(id, ObjectId::Item { .. }) {
                    ledger.system_update_id
                } else {
                    ledger.albums.get(&album).ok_or(MISSING)?.update_id
                }
            }
        };

        if metadata {
            let object = match id {
                ObjectId::Root => Object {
                    id: "0".into(),
                    parent_id: "-1".into(),
                    title: self.inner.config.friendly_name.clone(),
                    class: "object.container".into(),
                    date: None,
                    art: None,
                    child_count: Some(root.albums.len()),
                    resources: Vec::new(),
                },

                ObjectId::Album(id) => root.albums.get(&id).ok_or(MISSING)?.object.clone(),

                ObjectId::Item { asset, .. } => contents
                    .as_ref()
                    .ok_or(FAILED)?
                    .items
                    .get(&asset)
                    .ok_or(MISSING)?
                    .object
                    .clone(),
            };

            return Ok(BrowseResult {
                objects: vec![object],
                total_matches: 1,
                update_id,
            });
        }

        let rows: Vec<&Object> = match id {
            ObjectId::Root => {
                let mut rows: Vec<_> = root.albums.values().collect();

                rows.sort_unstable_by(|a, b| match sort {
                    None => b
                        .end_date
                        .cmp(&a.end_date)
                        .then_with(|| {
                            self.inner
                                .config
                                .collator
                                .compare(&a.object.title, &b.object.title)
                        })
                        .then_with(|| a.id.cmp(&b.id)),

                    Some(descending) => immich::compare_dates(
                        a.object.date.as_deref(),
                        a.created_at.as_ref(),
                        a.id,
                        b.object.date.as_deref(),
                        b.created_at.as_ref(),
                        b.id,
                        descending,
                    ),
                });

                rows.into_iter().map(|album| &album.object).collect()
            }

            ObjectId::Album(_) => {
                let mut rows: Vec<_> = contents.as_ref().ok_or(FAILED)?.items.values().collect();

                rows.sort_unstable_by(|a, b| {
                    immich::compare_dates(
                        sort.and(a.object.date.as_deref()),
                        a.capture.as_ref(),
                        a.id,
                        sort.and(b.object.date.as_deref()),
                        b.capture.as_ref(),
                        b.id,
                        sort.unwrap_or(false),
                    )
                });

                rows.into_iter().map(|item| &item.object).collect()
            }

            ObjectId::Item { asset, .. } => {
                if !contents.as_ref().ok_or(FAILED)?.items.contains_key(&asset) {
                    return Err(MISSING);
                }

                return Err(Fault { code: 710 });
            }
        };

        let total_matches = rows.len() as u32;

        let objects = rows
            .into_iter()
            .skip(start)
            .take(if count == 0 { usize::MAX } else { count })
            .cloned()
            .collect();

        Ok(BrowseResult {
            objects,
            total_matches,
            update_id,
        })
    }
}

#[cfg(test)]
mod tests {
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
        stop: CancellationToken,
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
                                let mut items =
                                    upstream.contents.get(&id).cloned().unwrap_or_default();

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

            let stop = CancellationToken::new();
            let shutdown = stop.clone();

            let task = tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
                    .unwrap();
            });

            Self {
                upstream,
                address,
                notifications,
                stop,
                task,
            }
        }

        fn config(&self, directory: &TempDir) -> Config {
            Config {
                api_base: format!("http://{}/api/", self.address).parse().unwrap(),
                api_key: HeaderValue::from_static("fake-key"),
                listen_address: "192.0.2.1:8200".parse().unwrap(),
                friendly_name: "Photos & videos".into(),
                collator: crate::config::collator("pl").unwrap(),
                server_uuid: Uuid::from_u128(999),
                state_directory: directory.path().to_owned(),
                log_level: tracing::Level::INFO,
                interface: crate::config::Interface {
                    name: "test".into(),
                    index: 1,
                },
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

            let (response, token) = library.inner.events.request(
                Service::ContentDirectory,
                Ipv4Addr::LOCALHOST,
                &Method::from_bytes(b"SUBSCRIBE").unwrap(),
                &headers,
            );

            assert_eq!(response.status(), 200);
            library.inner.events.response_complete(token.unwrap(), true);
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
            self.stop.cancel();
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
            let config = fake.config(&directory);
            let (store, mut ledger) = Store::open(directory.path(), config.server_uuid).unwrap();
            ledger.restart();
            store.persist(ledger.clone()).await;

            let library = Library::new(
                config,
                store,
                ledger,
                Subscriptions::new().unwrap(),
                CancellationToken::new(),
            )
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
                Scope::Root => state.root.as_mut().unwrap().completed -= limits::CATALOG_FRESHNESS,

                Scope::Album(id) => {
                    state.albums.get_mut(&id).unwrap().completed -= limits::CATALOG_FRESHNESS
                }
            }
        }

        fn disk(&self) -> (u64, Ledger) {
            let path = self.directory.path().join("revisions.json");

            (
                fs::metadata(&path).unwrap().ino(),
                serde_json::from_slice(&fs::read(path).unwrap()).unwrap(),
            )
        }

        async fn stop(&self, task: JoinHandle<Result<()>>) {
            self.library.inner.stop.cancel();

            timeout_at(Instant::now() + Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            assert!(self.library.inner.state.lock().unwrap().flights.is_empty());

            assert_eq!(
                self.library.inner.permits.available_permits(),
                limits::REFRESHES
            );
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

    fn action(id: &str, metadata: bool, start: u32, count: u32, sort: &str) -> Action {
        Action {
            name: "Browse".into(),
            arguments: BTreeMap::from([
                ("ObjectID".into(), id.into()),
                (
                    "BrowseFlag".into(),
                    if metadata {
                        "BrowseMetadata"
                    } else {
                        "BrowseDirectChildren"
                    }
                    .into(),
                ),
                ("Filter".into(), "*".into()),
                ("StartingIndex".into(), start.to_string()),
                ("RequestedCount".into(), count.to_string()),
                ("SortCriteria".into(), sort.into()),
            ]),
        }
    }

    fn children(id: u128) -> Action {
        action(&format!("album:{}", Uuid::from_u128(id)), false, 0, 0, "")
    }

    fn browse(library: &Library, action: Action) -> JoinHandle<Result<BrowseResult, Fault>> {
        let library = library.clone();

        tokio::spawn(async move { library.browse(action).await })
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

        async fn soap(client: &reqwest::Client, base: &str, action: Action) -> (u16, String) {
            let args: String = action
                .arguments
                .iter()
                .map(|(name, value)| {
                    format!("<{name}>{}</{name}>", quick_xml::escape::escape(value))
                })
                .collect();

            let body = format!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:{} xmlns:u=\"{}\">{args}</u:{}></s:Body></s:Envelope>",
                action.name,
                protocol::CONTENT_DIRECTORY,
                action.name,
            );

            let response = client
                .post(format!("{base}/upnp/content-directory/control"))
                .header("content-type", "text/xml; charset=\"utf-8\"")
                .header(
                    "soapaction",
                    format!("\"{}#{}\"", protocol::CONTENT_DIRECTORY, action.name),
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
        let mut config = fake.config(&directory);

        config.listen_address = match address {
            std::net::SocketAddr::V4(address) => address,
            _ => unreachable!("literal IPv4 bind"),
        };

        let (store, mut ledger) = Store::open(directory.path(), config.server_uuid).unwrap();
        ledger.restart();
        store.persist(ledger.clone()).await;
        let media = MediaProxy::new(config.api_base.clone(), config.api_key.clone()).unwrap();
        let events = Subscriptions::new().unwrap();
        let source_stop = CancellationToken::new();
        let server_stop = CancellationToken::new();
        let events_stop = CancellationToken::new();
        let name = config.friendly_name.clone();
        let uuid = config.server_uuid;

        let library =
            Library::new(config, store, ledger, events.clone(), source_stop.clone()).unwrap();

        let mut fixture = Fixture {
            library,
            fake,
            directory,
        };

        let catalog_task = fixture.run();
        let events_task = tokio::spawn(events.clone().run(events_stop.clone()));
        let server = Server::new(name, uuid, fixture.library.clone(), media, events);
        let server_task = tokio::spawn(server.run(listener, server_stop.clone()));

        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        let system = || Action {
            name: "GetSystemUpdateID".into(),
            arguments: BTreeMap::new(),
        };

        let (status, local) = soap(&client, &base, system()).await;
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
        assert_eq!(subscription.headers()["connection"], "close");
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

        let (status, root) = soap(&client, &base, action("0", false, 0, 0, "")).await;
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
            action(&format!("album:{album_id}"), true, 0, 0, ""),
        )
        .await;

        assert_eq!(status, 200);
        assert_eq!(text(&metadata, "UpdateID"), "0");
        assert_eq!(fixture.fake.calls("/api/search/metadata"), 0);
        let mut request = children(1);

        request
            .arguments
            .insert("Filter".into(), " dc:date, res@duration ".into());

        let (status, listing) = soap(&client, &base, request).await;
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

        let (status, fault) = soap(&client, &base, action("0", false, 0, 0, "+dc:title")).await;
        assert_eq!(status, 500);
        assert_eq!(text(&fault, "errorCode"), "709");
        assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), 4);
        fixture.expire(Scope::Album(album_id));
        fixture.fake.upstream.lock().unwrap().outage = true;
        let (status, fault) = soap(&client, &base, children(1)).await;
        assert_eq!(status, 500);
        assert_eq!(text(&fault, "errorCode"), "501");

        assert!(
            !fault.contains("127.0.0.1")
                && !fault.contains("Edited")
                && !fault.contains("checksum")
        );

        let before_local = fixture.fake.upstream.lock().unwrap().requests.len();
        let (status, local) = soap(&client, &base, system()).await;
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

        let (status, changed) = soap(&client, &base, children(1)).await;
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
        tokio::time::advance(limits::EVENT_MODERATION).await;
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
        let (status, local) = soap(&client, &base, system()).await;
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

        server_stop.cancel();
        source_stop.cancel();

        tokio::time::timeout(Duration::from_secs(3), server_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fixture.stop(catalog_task).await;
        events_stop.cancel();

        tokio::time::timeout(Duration::from_secs(3), events_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        fixture.fake.stop.cancel();

        tokio::time::timeout(Duration::from_secs(3), &mut fixture.fake.task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_id_startup_event_outage_and_metadata_scopes() {
        let mut fixture = Fixture::new(2).await;
        let task = fixture.run();
        let events_stop = CancellationToken::new();

        let events = tokio::spawn(
            fixture
                .library
                .inner
                .events
                .clone()
                .run(events_stop.clone()),
        );

        fixture.fake.subscribe(&fixture.library);
        fixture.fake.event(1).await;

        for _ in 0..5 {
            assert_eq!(fixture.library.system_update_id(), 1);
        }

        assert!(fixture.fake.upstream.lock().unwrap().requests.is_empty());
        fixture.fake.upstream.lock().unwrap().outage = true;
        fault(browse(&fixture.library, action("0", true, 0, 0, "")), 501).await;
        assert_eq!(fixture.library.system_update_id(), 1);
        fixture.fake.upstream.lock().unwrap().outage = false;

        let root = fixture
            .library
            .browse(action("0", true, 0, 99, ""))
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
                "-dc:date",
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
        events_stop.cancel();
        events.await.unwrap().unwrap();
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn invalid_direct_arguments_do_not_panic_or_fetch() {
        let fixture = Fixture::new(1).await;

        for (key, value, code) in [
            ("ObjectID", "asset:bad", 701),
            ("SortCriteria", "+dc:title", 709),
            ("SortCriteria", "+dc:date,-dc:date", 709),
            ("BrowseFlag", "bad", 402),
            ("StartingIndex", "1", 402),
            ("StartingIndex", "+0", 402),
            ("RequestedCount", "4294967296", 402),
            ("RequestedCount", "", 402),
        ] {
            let mut request = action("0", true, 0, 0, "");
            request.arguments.insert(key.into(), value.into());

            assert_eq!(
                fixture.library.browse(request).await.err(),
                Some(Fault { code })
            );
        }

        for key in [
            "ObjectID",
            "BrowseFlag",
            "Filter",
            "StartingIndex",
            "RequestedCount",
            "SortCriteria",
        ] {
            let mut request = action("0", true, 0, 0, "");
            request.arguments.remove(key);

            assert_eq!(
                fixture.library.browse(request).await.err(),
                Some(Fault { code: 402 })
            );
        }

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
            ("", [3, 4, 5, 1, 2, 6, 7, 8, 9]),
            ("+dc:date", [1, 3, 4, 5, 6, 7, 8, 9, 2]),
            ("-dc:date", [2, 3, 4, 5, 6, 7, 8, 9, 1]),
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
        fixture.stop(task).await;
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
        let request = || action("0", false, 0, 0, "");
        let before = fixture.library.browse(request()).await.unwrap();
        assert_eq!(before.objects[0].title, "Z");
        let disk = fixture.disk().1;
        let events_stop = CancellationToken::new();

        let events = tokio::spawn(
            fixture
                .library
                .inner
                .events
                .clone()
                .run(events_stop.clone()),
        );

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
        events_stop.cancel();
        events.await.unwrap().unwrap();
        fixture.stop(task).await;
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
            .browse(action("0", false, 1, 2, ""))
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
            .browse(action("0", false, 0, 1, "+dc:date"))
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
            ("+dc:date", ["Photo 1", "Photo 2", "Photo 3"]),
            ("-dc:date", ["Photo 2", "Photo 1", "Photo 3"]),
        ] {
            let mut request = children(1);
            request.arguments.insert("SortCriteria".into(), sort.into());
            request.arguments.insert("Filter".into(), "".into());
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
                    "+dc:date",
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
            .browse(action(&appearance, true, 0, 0, ""))
            .await
            .unwrap();

        assert_eq!(metadata.update_id, fixture.library.system_update_id());
        assert_eq!(metadata.total_matches, 1);

        fault(
            browse(&fixture.library, action(&appearance, false, 0, 0, "")),
            710,
        )
        .await;

        let wrong_album = format!("album:{}:asset:{}", Uuid::from_u128(2), Uuid::from_u128(1));

        fault(
            browse(&fixture.library, action(&wrong_album, true, 0, 0, "")),
            701,
        )
        .await;

        fault(
            browse(&fixture.library, action(&wrong_album, false, 0, 0, "")),
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
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn expiry_and_lru_refill_are_fresh_without_writes_or_events() {
        let mut fixture = Fixture::new(2).await;

        Arc::get_mut(&mut fixture.library.inner)
            .unwrap()
            .bounds
            .albums = 1;

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
                .albums
                .contains_key(&Uuid::from_u128(1))
        );

        let before = fixture.disk();
        let events_stop = CancellationToken::new();

        let events = tokio::spawn(
            fixture
                .library
                .inner
                .events
                .clone()
                .run(events_stop.clone()),
        );

        fixture.fake.subscribe(&fixture.library);
        fixture.fake.event(before.1.system_update_id).await;
        fixture.library.browse(children(1)).await.unwrap();
        assert_eq!(fixture.disk(), before);
        fixture.expire(Scope::Root);
        fixture.expire(Scope::Album(Uuid::from_u128(1)));

        let expired =
            fixture.library.inner.state.lock().unwrap().albums[&Uuid::from_u128(1)].completed;

        fixture.library.browse(children(1)).await.unwrap();

        assert!(
            fixture.library.inner.state.lock().unwrap().albums[&Uuid::from_u128(1)].completed
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
        events_stop.cancel();
        events.await.unwrap().unwrap();
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn root_counts_toward_byte_budget_and_payload_eviction_retains_history() {
        let mut fixture = Fixture::new(2).await;

        let budget = FetchBudget {
            deadline: Instant::now() + limits::REFRESH_PREPARATION_TIMEOUT,
            stop: fixture.library.inner.stop.clone(),
        };

        let root = fixture.library.inner.source.root(&budget).await.unwrap();

        Arc::get_mut(&mut fixture.library.inner)
            .unwrap()
            .bounds
            .bytes = root.bytes + 2;

        let task = fixture.run();
        fixture.library.browse(children(1)).await.unwrap();
        fixture.library.browse(children(2)).await.unwrap();

        {
            let state = fixture.library.inner.state.lock().unwrap();
            assert!(state.root.is_some());
            assert_eq!(state.albums.len(), 1);
            assert_eq!(state.ledger.albums.len(), 2);

            assert!(
                state
                    .ledger
                    .albums
                    .values()
                    .all(|album| album.contents_digest.is_some())
            );
        }

        let before = fixture.disk();

        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .contents
            .insert(Uuid::from_u128(1), vec![item(1, None, None)]);

        fault(browse(&fixture.library, children(1)), 501).await;
        assert_eq!(fixture.disk(), before);
        fixture.stop(task).await;
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
                "",
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
        fixture.stop(task).await;
        let mut restarted = fixture.disk().1;
        restarted.restart();
        let store = fixture.library.inner.store.clone();
        store.persist(restarted.clone()).await;

        fixture.library = Library::new(
            fixture.fake.config(&fixture.directory),
            store,
            restarted.clone(),
            Subscriptions::new().unwrap(),
            CancellationToken::new(),
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
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn shared_flights_survive_callers_and_four_scopes_reject_without_backlog() {
        let fixture = Fixture::new(5).await;
        let task = fixture.run();

        fixture
            .library
            .browse(action("0", true, 0, 0, ""))
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

        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn whole_permit_covers_commit_wait_and_preparation_timeout_cleans_flight() {
        let mut fixture = Fixture::new(1).await;

        Arc::get_mut(&mut fixture.library.inner)
            .unwrap()
            .bounds
            .preparation = Duration::from_millis(150);

        let task = fixture.run();

        fixture
            .library
            .browse(action("0", true, 0, 0, ""))
            .await
            .unwrap();

        let gate = fixture.library.inner.commit.lock().await;
        let barrier = Barrier::new();

        *fixture.library.inner.prepared.lock().unwrap() =
            Some((Scope::Album(Uuid::from_u128(1)), barrier.clone()));

        let caller = browse(&fixture.library, children(1));
        barrier.entered().await;
        barrier.release.add_permits(1);
        assert_eq!(fixture.library.inner.permits.available_permits(), 3);
        let before = fixture.disk();
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
        fixture.stop(task).await;
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
            .browse(action("0", true, 0, 0, ""))
            .await
            .unwrap();

        assert!(
            !fixture
                .library
                .inner
                .state
                .lock()
                .unwrap()
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
            .browse(action("0", true, 0, 0, ""))
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
                .albums
                .contains_key(&Uuid::from_u128(1))
        );

        *fixture.library.inner.prepared.lock().unwrap() = None;
        fixture.library.browse(children(1)).await.unwrap();
        assert_eq!(fixture.disk(), before);
        fixture.stop(task).await;
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
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn shutdown_cancels_fetch_and_gate_wait_but_drains_durable_publication() {
        for waiting_gate in [false, true] {
            let fixture = Fixture::new(1).await;
            let task = fixture.run();
            let barrier = Barrier::new();
            let gate = fixture.library.inner.commit.lock().await;

            if waiting_gate {
                *fixture.library.inner.prepared.lock().unwrap() =
                    Some((Scope::Root, barrier.clone()));
            } else {
                fixture
                    .fake
                    .upstream
                    .lock()
                    .unwrap()
                    .gates
                    .insert(Scope::Root, barrier.clone());
            }

            let caller = browse(&fixture.library, action("0", true, 0, 0, ""));
            barrier.entered().await;

            if waiting_gate {
                barrier.release.add_permits(1);
            }

            let before = fixture.disk();
            fixture.stop(task).await;
            fault(caller, 501).await;
            assert_eq!(fixture.disk(), before);
            fault(browse(&fixture.library, children(1)), 501).await;
            drop(gate);
        }

        let mut fixture = Fixture::new(1).await;
        let mut task = fixture.run();
        let barrier = Barrier::new();
        *fixture.library.inner.publication.lock().unwrap() = Some(barrier.clone());
        let caller = browse(&fixture.library, action("0", true, 0, 0, ""));
        barrier.entered().await;
        assert_eq!(fixture.disk().1.system_update_id, 2);
        assert_eq!(fixture.library.system_update_id(), 1);
        assert_eq!(fixture.library.inner.permits.available_permits(), 3);
        caller.abort();
        fixture.library.inner.stop.cancel();

        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut task)
                .await
                .is_err()
        );

        barrier.release.add_permits(1);
        task.await.unwrap().unwrap();
        assert_eq!(fixture.library.system_update_id(), 2);
        assert_eq!(fixture.library.inner.permits.available_permits(), 4);
        assert!(fixture.library.inner.state.lock().unwrap().root.is_some());
        let events_stop = CancellationToken::new();

        let events = tokio::spawn(
            fixture
                .library
                .inner
                .events
                .clone()
                .run(events_stop.clone()),
        );

        fixture.fake.subscribe(&fixture.library);
        fixture.fake.event(2).await;
        events_stop.cancel();
        events.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn preparation_panic_is_fatal_and_supervised_even_without_waiters() {
        let fixture = Fixture::new(1).await;
        let task = fixture.run();
        let mut barrier = Barrier::new();
        Arc::get_mut(&mut barrier).unwrap().panic = true;
        *fixture.library.inner.prepared.lock().unwrap() = Some((Scope::Root, barrier.clone()));
        let caller = browse(&fixture.library, action("0", true, 0, 0, ""));
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

        assert!(fixture.library.inner.stop.is_cancelled());
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

        let caller = browse(&fixture.library, action("0", true, 0, 0, ""));
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
        tokio::time::advance(limits::CATALOG_FRESHNESS).await;
        tokio::time::resume();
        assert_eq!(fixture.library.system_update_id(), 2);
        assert_eq!(fixture.disk(), before);
        assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), calls);
        assert!(fixture.library.inner.state.lock().unwrap().root.is_some());
        fixture.stop(task).await;
    }

    #[tokio::test]
    async fn completed_browse_pins_rows_and_revision_across_payload_eviction() {
        let mut fixture = Fixture::new(2).await;

        Arc::get_mut(&mut fixture.library.inner)
            .unwrap()
            .bounds
            .bytes = 2 * limits::SNAPSHOT_BYTES;

        {
            let mut upstream = fixture.fake.upstream.lock().unwrap();
            upstream.albums[0]["albumName"] = json!("A".repeat(16 * 1024));

            for id in 1..=2 {
                let mut asset = item(id, None, None);
                asset["checksum"] = json!("x".repeat(limits::SNAPSHOT_BYTES - 4096));
                upstream.contents.insert(Uuid::from_u128(id), vec![asset]);
            }
        }

        let task = fixture.run();

        fixture
            .library
            .browse(action("0", true, 0, 0, ""))
            .await
            .unwrap();

        let mut children_waiter = Box::pin(fixture.library.browse(children(1)));
        assert!(futures_util::poll!(&mut children_waiter).is_pending());
        let appearance = format!("album:{}:asset:{}", Uuid::from_u128(1), Uuid::from_u128(1));

        let mut metadata_waiter =
            Box::pin(fixture.library.browse(action(&appearance, true, 0, 0, "")));

        assert!(futures_util::poll!(&mut metadata_waiter).is_pending());

        // Observe completion without polling either Browse waiter again.
        let mut flight = fixture.library.inner.state.lock().unwrap().flights
            [&Scope::Album(Uuid::from_u128(1))]
            .clone();

        flight.changed().await.unwrap();

        let (album_id, system_id, a_bytes) = {
            let state = fixture.library.inner.state.lock().unwrap();
            let bytes = state.albums[&Uuid::from_u128(1)].snapshot.bytes;
            assert!(bytes > limits::SNAPSHOT_BYTES - 8192 && bytes <= limits::SNAPSHOT_BYTES);

            (
                state.ledger.albums[&Uuid::from_u128(1)].update_id,
                state.ledger.system_update_id,
                bytes,
            )
        };

        fixture.library.browse(children(2)).await.unwrap();

        {
            let state = fixture.library.inner.state.lock().unwrap();
            let b_bytes = state.albums[&Uuid::from_u128(2)].snapshot.bytes;
            assert!(b_bytes > limits::SNAPSHOT_BYTES - 8192 && b_bytes <= limits::SNAPSHOT_BYTES);

            assert!(
                a_bytes + b_bytes + state.root.as_ref().unwrap().snapshot.bytes
                    > 2 * limits::SNAPSHOT_BYTES
            );

            assert!(!state.albums.contains_key(&Uuid::from_u128(1)));
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
            .browse(action("0", true, 0, 0, ""))
            .await
            .unwrap();

        let metadata = metadata_waiter.await.unwrap();
        assert_eq!(metadata.objects, result.objects);
        assert_eq!(metadata.update_id, system_id);
        assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

        assert!(
            !fixture.library.inner.state.lock().unwrap().ledger.albums[&Uuid::from_u128(1)].present
        );

        fixture.stop(task).await;
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
        let mut root_waiter = Box::pin(fixture.library.browse(action("0", true, 0, 0, "")));
        assert!(futures_util::poll!(&mut root_waiter).is_pending());

        let mut album_waiter = Box::pin(fixture.library.browse(action(
            &format!("album:{}", Uuid::from_u128(1)),
            true,
            0,
            0,
            "",
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
            .browse(action("0", true, 0, 0, ""))
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
                .albums
                .contains_key(&Uuid::from_u128(1))
        );

        fixture.stop(task).await;
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
        let events_stop = CancellationToken::new();

        let events = tokio::spawn(
            fixture
                .library
                .inner
                .events
                .clone()
                .run(events_stop.clone()),
        );

        fixture.fake.subscribe(&fixture.library);
        fixture.fake.event(2).await;
        fixture.fake.upstream.lock().unwrap().contents.clear();
        let result = fixture.library.browse(children(1)).await.unwrap();
        assert_eq!(result.update_id, 1);
        assert_eq!(result.total_matches, 0);
        fixture.fake.event(3).await;
        assert_eq!(fixture.fake.calls("/api/albums"), 1);
        events_stop.cancel();
        events.await.unwrap().unwrap();
        fixture.stop(task).await;
    }
}
