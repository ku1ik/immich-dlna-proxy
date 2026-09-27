//! Browse-driven snapshots and process-local revision history.
//!
//! Shared refreshes prepare snapshots concurrently, then publish snapshots,
//! counters, and pending event state under one short lock. Client deadlines only
//! stop waiting. Restarting forgets history and randomly seeds new counters.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use icu_collator::CollatorBorrowed;
use serde::Serialize;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, watch},
    task::JoinSet,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{config::Config, eventing::Subscriptions, immich, protocol::Fault};

mod browse;
mod digest;
mod revisions;
mod snapshots;

pub use browse::{ObjectId, parse_id};
use browse::{Payload, View};
use revisions::{AlbumRevision, Ledger};
use snapshots::{Contents, Root, Source};

const FRESHNESS: Duration = Duration::from_secs(60);
const RESIDENT_ALBUMS: usize = 32;
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ALBUMS: usize = 4_096;
const REFRESHES: usize = 4;
const REFRESH_TIMEOUT: Duration = Duration::from_secs(25);

/// Select, sort and paginate objects, capturing their revision together.
pub trait Catalog: Send + Sync + 'static {
    fn system_update_id(&self) -> u32;
    fn browse(
        &self,
        query: BrowseQuery,
    ) -> impl Future<Output = Result<BrowseResult, Fault>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrowseQuery {
    pub object_id: ObjectId,
    pub mode: BrowseMode,
    pub sort: SortOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrowseMode {
    Metadata,
    DirectChildren {
        starting_index: u32,
        requested_count: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortOrder {
    Catalog,
    DateAscending,
    DateDescending,
}

pub struct BrowseResult {
    pub objects: Vec<Object>,
    pub total_matches: u32,
    pub update_id: u32,
}

/// Complete projected metadata; callers own filtering and wire serialization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Object {
    pub kind: ObjectKind,
    pub title: String,
    pub date: Option<String>,
    pub art: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ObjectKind {
    Root {
        child_count: Option<usize>,
    },
    Album {
        id: Uuid,
        child_count: Option<usize>,
    },
    Photo {
        album: Uuid,
        asset: Uuid,
        resources: Vec<Resource>,
    },
    Video {
        album: Uuid,
        asset: Uuid,
        resources: Vec<Resource>,
    },
}

impl Object {
    pub fn id(&self) -> ObjectId {
        match self.kind {
            ObjectKind::Root { .. } => ObjectId::Root,
            ObjectKind::Album { id, .. } => ObjectId::Album(id),

            ObjectKind::Photo { album, asset, .. } | ObjectKind::Video { album, asset, .. } => {
                ObjectId::Item { album, asset }
            }
        }
    }

    pub fn parent_id(&self) -> Option<ObjectId> {
        match self.id() {
            ObjectId::Root => None,
            ObjectId::Album(_) => Some(ObjectId::Root),
            ObjectId::Item { album, .. } => Some(ObjectId::Album(album)),
        }
    }

    pub fn class(&self) -> &'static str {
        match self.kind {
            ObjectKind::Root { .. } => "object.container",
            ObjectKind::Album { .. } => "object.container.album",
            ObjectKind::Photo { .. } => "object.item.imageItem.photo",
            ObjectKind::Video { .. } => "object.item.videoItem",
        }
    }

    pub fn child_count(&self) -> Option<usize> {
        match self.kind {
            ObjectKind::Root { child_count } | ObjectKind::Album { child_count, .. } => child_count,
            ObjectKind::Photo { .. } | ObjectKind::Video { .. } => None,
        }
    }

    pub fn resources(&self) -> &[Resource] {
        match &self.kind {
            ObjectKind::Root { .. } | ObjectKind::Album { .. } => &[],
            ObjectKind::Photo { resources, .. } | ObjectKind::Video { resources, .. } => resources,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Resource {
    pub uri: String,
    pub mime: String,
    pub duration: Option<String>,
    /// Explicitly established byte-seek support, not inferred from the MIME.
    pub byte_seek: bool,
}

const FAILED: Fault = Fault::ActionFailed;
const MISSING: Fault = Fault::NoSuchObject;

// Eviction always has room for the retained root and the album being published.
const _: () = {
    assert!(2 * SNAPSHOT_BYTES <= CACHE_BYTES);
    assert!(RESIDENT_ALBUMS > 0);
};

#[derive(Clone)]
pub(crate) struct ImmichCatalog {
    inner: Arc<Inner>,
}

struct Inner {
    source: Source,
    collator: CollatorBorrowed<'static>,
    events: Subscriptions,
    failure: CancellationToken,
    state: Mutex<State>,
    permits: Arc<Semaphore>,
    supervisor: Mutex<Supervisor>,
    wake: Notify,
    #[cfg(test)]
    prepared: Mutex<Option<(Scope, Arc<tests::Barrier>)>>,
}

struct Supervisor {
    tasks: JoinSet<()>,
    running: bool,
}

struct State {
    ledger: Arc<Ledger>,
    cache: Cache,
    flights: BTreeMap<Scope, watch::Receiver<Option<RefreshResult>>>,
}

type RefreshResult = Result<View, Fault>;

impl State {
    fn view(&self, scope: Scope) -> RefreshResult {
        let payload = match scope {
            Scope::Root => Payload::Root,

            Scope::Album(id) => Payload::Album {
                id,
                contents: self.cache.albums.get(&id).ok_or(FAILED)?.snapshot.clone(),
            },
        };

        Ok(View {
            root: self.cache.root.as_ref().ok_or(FAILED)?.snapshot.clone(),
            payload,
            ledger: self.ledger.clone(),
        })
    }
}

// Snapshot residency and freshness; synchronized with the ledger by State's lock.
struct Cache {
    root: Option<Cached<Root>>,
    albums: BTreeMap<Uuid, Cached<Contents>>,
    album_limit: usize,
    byte_limit: usize,
}

impl Cache {
    fn new() -> Self {
        Self {
            root: None,
            albums: BTreeMap::new(),
            album_limit: RESIDENT_ALBUMS,
            byte_limit: CACHE_BYTES,
        }
    }

    fn is_fresh(&mut self, scope: Scope, now: Instant) -> bool {
        match scope {
            Scope::Root => self
                .root
                .as_mut()
                .is_some_and(|cached| cached.is_fresh(now)),

            Scope::Album(id) => self
                .albums
                .get_mut(&id)
                .is_some_and(|cached| cached.is_fresh(now)),
        }
    }

    fn insert(&mut self, candidate: Candidate, now: Instant) {
        let scope = match candidate {
            Candidate::Root(snapshot) => {
                self.albums.retain(|id, _| snapshot.albums.contains_key(id));

                self.root = Some(Cached {
                    snapshot: Arc::new(snapshot),
                    completed: now,
                    used: now,
                });

                Scope::Root
            }

            Candidate::Album(id, snapshot) => {
                self.albums.insert(
                    id,
                    Cached {
                        snapshot: Arc::new(snapshot),
                        completed: now,
                        used: now,
                    },
                );

                Scope::Album(id)
            }
        };

        while self.albums.len() > self.album_limit
            || self.root.as_ref().map_or(0, |root| root.snapshot.bytes)
                + self
                    .albums
                    .values()
                    .map(|album| album.snapshot.bytes)
                    .sum::<usize>()
                > self.byte_limit
        {
            let oldest = self
                .albums
                .iter()
                .filter(|(id, _)| scope != Scope::Album(**id))
                .min_by_key(|(id, cached)| (cached.used, **id))
                .map(|(id, _)| *id)
                .expect("cache budget fits root and the published album");

            self.albums.remove(&oldest);
        }
    }

    fn mark_completed(&mut self, scope: Scope, completed: Instant) {
        match scope {
            Scope::Root => self.root.as_mut().unwrap().completed = completed,

            Scope::Album(id) => {
                self.albums
                    .get_mut(&id)
                    .expect("published album remains cached")
                    .completed = completed;
            }
        }
    }
}

struct Cached<T> {
    snapshot: Arc<T>,
    completed: Instant,
    used: Instant,
}

impl<T> Cached<T> {
    fn is_fresh(&mut self, now: Instant) -> bool {
        self.used = now;

        now.duration_since(self.completed) < FRESHNESS
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Scope {
    Root,
    Album(Uuid),
}

enum Token {
    Root,
    Album(Uuid, AlbumRevision),
}

impl Token {
    fn scope(&self) -> Scope {
        match self {
            Self::Root => Scope::Root,
            Self::Album(id, _) => Scope::Album(*id),
        }
    }

    fn applicable(&self, ledger: &Ledger) -> bool {
        match self {
            // One root flight exists at a time; only it can change the root digest.
            Self::Root => true,

            Self::Album(id, revision) => {
                ledger.albums.get(id) == Some(revision) && revision.present
            }
        }
    }
}

enum Candidate {
    Root(Root),
    Album(Uuid, Contents),
}

// Created before spawning, so even cancellation before the first poll cleans up.
struct Flight {
    catalog: ImmichCatalog,
    scope: Scope,
    sender: watch::Sender<Option<RefreshResult>>,
    permit: Option<OwnedSemaphorePermit>,
    result: RefreshResult,
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut state = self
            .catalog
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        state.flights.remove(&self.scope);
        drop(self.permit.take());
        let result = std::mem::replace(&mut self.result, Err(FAILED));
        self.sender.send_replace(Some(result));
        drop(state);
        self.catalog.inner.wake.notify_one();
    }
}

struct Running(ImmichCatalog);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.inner.supervisor.lock().unwrap().tasks.abort_all();
    }
}

impl ImmichCatalog {
    pub(crate) fn new(config: Config, events: Subscriptions) -> Result<Self> {
        Self::from_parts(config, Ledger::new(rand::random()), events)
    }

    fn from_parts(config: Config, ledger: Ledger, events: Subscriptions) -> Result<Self> {
        let source = Source::new(
            immich::Client::new(config.api_base, config.api_key)?,
            config.listen_address,
            config.friendly_name,
        );

        events.publish(ledger.system_update_id);

        Ok(Self {
            inner: Arc::new(Inner {
                source,
                collator: config.collator,
                events,
                failure: CancellationToken::new(),
                state: Mutex::new(State {
                    ledger: Arc::new(ledger),
                    cache: Cache::new(),
                    flights: BTreeMap::new(),
                }),
                permits: Arc::new(Semaphore::new(REFRESHES)),
                supervisor: Mutex::new(Supervisor {
                    tasks: JoinSet::new(),
                    running: false,
                }),
                wake: Notify::new(),
                #[cfg(test)]
                prepared: Mutex::new(None),
            }),
        })
    }

    /// Supervise refresh tasks and fail after cancelling preparation on task failure.
    pub(crate) async fn run(&self) -> Result<()> {
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
                            self.inner.failure.cancel();
                        }

                        Poll::Ready(false)
                    }

                    Poll::Ready(None) if self.inner.failure.is_cancelled() => Poll::Ready(true),
                    _ => Poll::Pending,
                }
            });

            tokio::select! {
                done = finished => {
                    if done {
                        return Err(anyhow!("catalog refresh task failed"));
                    }
                }

                _ = notified => {}
            }
        }
    }

    async fn fresh(&self, scope: Scope) -> RefreshResult {
        let mut receiver = {
            let mut state = self.inner.state.lock().unwrap();

            if self.inner.failure.is_cancelled() {
                return Err(FAILED);
            }

            let now = Instant::now();

            let token = match scope {
                Scope::Root => Token::Root,

                Scope::Album(id) => {
                    let revision = state
                        .ledger
                        .albums
                        .get(&id)
                        .filter(|revision| revision.present)
                        .copied()
                        .ok_or(MISSING)?;

                    Token::Album(id, revision)
                }
            };

            if state.cache.is_fresh(scope, now) {
                return state.view(scope);
            }

            if let Some(receiver) = state.flights.get(&scope) {
                receiver.clone()
            } else {
                let mut supervisor = self.inner.supervisor.lock().unwrap();

                // Reap on admission too: finished handles cannot accumulate when
                // Browse traffic runs ahead of the supervisor.
                while let Some(result) = supervisor.tasks.try_join_next() {
                    if result.is_err() {
                        self.inner.failure.cancel();
                    }
                }

                if self.inner.failure.is_cancelled() {
                    self.inner.wake.notify_one();

                    return Err(FAILED);
                }

                let permit = self
                    .inner
                    .permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| FAILED)?;

                let (sender, receiver) = watch::channel(None);
                state.flights.insert(scope, receiver.clone());

                let mut flight = Flight {
                    catalog: self.clone(),
                    scope,
                    sender,
                    permit: Some(permit),
                    result: Err(FAILED),
                };

                let deadline = now + REFRESH_TIMEOUT;

                supervisor.tasks.spawn(async move {
                    flight.result = flight
                        .catalog
                        .refresh(token, deadline)
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

    async fn refresh(&self, token: Token, deadline: Instant) -> Result<View> {
        let scope = token.scope();

        ensure!(
            !self.inner.failure.is_cancelled() && Instant::now() < deadline,
            "catalog preparation failed or expired"
        );

        let prepare = async {
            let candidate = match scope {
                Scope::Root => Candidate::Root(self.inner.source.root().await?),
                Scope::Album(id) => Candidate::Album(id, self.inner.source.contents(id).await?),
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

            Ok::<_, anyhow::Error>(candidate)
        };

        let candidate = tokio::select! {
            biased;
            _ = self.inner.failure.cancelled() => return Err(anyhow!("catalog preparation cancelled after task failure")),

            result = timeout_at(deadline, prepare) => {
                result.map_err(|_| anyhow!("catalog refresh deadline exceeded"))??
            }
        };

        let mut state = self.inner.state.lock().unwrap();
        ensure!(token.applicable(&state.ledger), "stale catalog candidate");

        let next = match &candidate {
            Candidate::Root(root) => {
                let albums = root
                    .albums
                    .iter()
                    .map(|(id, album)| (*id, album.digest))
                    .collect();

                state.ledger.root_transition(&root.digest, &albums)?
            }

            Candidate::Album(id, contents) => {
                state.ledger.contents_transition(*id, &contents.digest)?
            }
        };

        ensure!(
            !self.inner.failure.is_cancelled() && Instant::now() < deadline,
            "catalog refresh failed or expired"
        );

        let changed = next.is_some();

        if let Some(next) = next {
            state.ledger = Arc::new(next);
        }

        state.cache.insert(candidate, Instant::now());

        if changed {
            self.inner.events.publish(state.ledger.system_update_id);
        }

        let view = state.view(scope).expect("published snapshot is resident");
        state.cache.mark_completed(scope, Instant::now());

        Ok(view)
    }
}

impl Catalog for ImmichCatalog {
    fn system_update_id(&self) -> u32 {
        self.inner.state.lock().unwrap().ledger.system_update_id
    }

    async fn browse(&self, query: BrowseQuery) -> Result<BrowseResult, Fault> {
        let id = query.object_id;
        let mut view = self.fresh(Scope::Root).await?;

        match id {
            ObjectId::Album(album) if matches!(query.mode, BrowseMode::DirectChildren { .. }) => {
                view = self.fresh(Scope::Album(album)).await?
            }

            ObjectId::Item { album, .. } => view = self.fresh(Scope::Album(album)).await?,

            _ => {}
        }

        view.browse(query, &self.inner.collator)
    }
}

#[cfg(test)]
mod tests;
