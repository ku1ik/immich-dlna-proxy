//! One catalog owner, serial shared refreshes, and process-local revision history.

use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, ensure};
use icu_collator::CollatorBorrowed;
use serde::Serialize;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use uuid::Uuid;

use crate::{activity, config::Config, eventing::Subscriptions, immich, protocol::Fault};

mod background;
mod browse;
mod revisions;
mod snapshots;

use background::Background;
pub use browse::{ObjectId, parse_id};
use browse::{Rows, Selection, View};
use revisions::Ledger;
use snapshots::{Contents, Root, Source};

const FRESHNESS: Duration = Duration::from_secs(60);
const RESIDENT_ALBUMS: usize = 32;
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ALBUMS: usize = 4_096;
const REQUESTS: usize = 8;
const REFRESH_TIMEOUT: Duration = Duration::from_secs(25);

type Digest = [u8; 32];

/// Select, sort and paginate objects, capturing their revision together.
pub trait Catalog: Send + Sync + 'static {
    fn system_update_id(&self) -> impl Future<Output = Result<u32, Fault>> + Send;
    fn browse(
        &self,
        query: BrowseQuery,
        deadline: Instant,
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
    pub date: Option<chrono::NaiveDate>,
    pub art: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ObjectKind {
    Root {
        child_count: usize,
    },
    Album {
        id: Uuid,
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
            ObjectKind::Album { id } => ObjectId::Album(id),

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
            ObjectKind::Root { child_count } => Some(child_count),
            ObjectKind::Album { .. } | ObjectKind::Photo { .. } | ObjectKind::Video { .. } => None,
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
    commands: mpsc::Sender<Command>,
    collator: Arc<CollatorBorrowed<'static>>,
}

pub(crate) struct CatalogTask {
    commands: mpsc::Receiver<Command>,
    source: Arc<Source>,
    events: Subscriptions,
    state: State,
    pending: Vec<Pending>,
    activity: watch::Receiver<activity::Snapshot>,
    background: Background,
}

enum Command {
    Browse(Pending),
    SystemUpdateId(oneshot::Sender<u32>),
    #[cfg(test)]
    Test(tests::Control),
}

struct Pending {
    query: BrowseQuery,
    deadline: Instant,
    reply: oneshot::Sender<Result<View, Fault>>,
    // Latched until this attempt completes, even if root freshness changes meanwhile.
    depends_on_active_refresh: bool,
}

struct Refresh {
    scope: Scope,
    background: bool,
    deadline: Instant,
    future: Pin<Box<dyn Future<Output = Result<Candidate>> + Send>>,
}

struct State {
    ledger: Ledger,
    cache: Cache,
}

impl State {
    fn needed(&self, query: BrowseQuery, now: Instant) -> Result<Option<Scope>, Fault> {
        if !self.cache.is_fresh(Scope::Root, now) {
            return Ok(Some(Scope::Root));
        }

        if let Scope::Album(id) = query.scope() {
            if !self.ledger.albums.contains_key(&id) {
                return Err(MISSING);
            }

            if !self.cache.is_fresh(Scope::Album(id), now) {
                return Ok(Some(Scope::Album(id)));
            }
        }

        Ok(None)
    }

    fn view(&self, query: BrowseQuery) -> Result<View, Fault> {
        let update_id = match query.object_id {
            ObjectId::Album(id) => self.ledger.albums.get(&id).ok_or(MISSING)?.update_id,
            ObjectId::Root | ObjectId::Item { .. } => self.ledger.system_update_id,
        };

        let root = &self.cache.root.as_ref().ok_or(FAILED)?.snapshot;

        let contents = |id| {
            self.cache
                .albums
                .get(&id)
                .map(|cached| &cached.snapshot)
                .ok_or(FAILED)
        };

        let selection = match (query.object_id, query.mode) {
            (ObjectId::Root, BrowseMode::Metadata) => Selection::Metadata(root.object()),

            (ObjectId::Album(id), BrowseMode::Metadata) => {
                let album = root.albums.get(&id).ok_or(MISSING)?;

                Selection::Metadata(album.metadata.object.clone())
            }

            (ObjectId::Item { album, asset }, mode) => {
                if !root.albums.contains_key(&album) {
                    return Err(MISSING);
                }

                let item = contents(album)?.items.get(&asset).ok_or(MISSING)?;

                if matches!(mode, BrowseMode::DirectChildren { .. }) {
                    return Err(Fault::NoSuchContainer);
                }

                Selection::Metadata(item.object.clone())
            }

            (
                ObjectId::Root,
                BrowseMode::DirectChildren {
                    starting_index,
                    requested_count,
                },
            ) => Selection::Children {
                rows: Rows::Albums(root.clone()),
                starting_index,
                requested_count,
                sort: query.sort,
            },

            (
                ObjectId::Album(id),
                BrowseMode::DirectChildren {
                    starting_index,
                    requested_count,
                },
            ) => {
                if !root.albums.contains_key(&id) {
                    return Err(MISSING);
                }

                Selection::Children {
                    rows: Rows::Items(contents(id)?.clone()),
                    starting_index,
                    requested_count,
                    sort: query.sort,
                }
            }
        };

        Ok(View {
            selection,
            update_id,
        })
    }
}

impl BrowseQuery {
    fn scope(self) -> Scope {
        match (self.object_id, self.mode) {
            (ObjectId::Album(id), BrowseMode::DirectChildren { .. })
            | (ObjectId::Item { album: id, .. }, _) => Scope::Album(id),
            _ => Scope::Root,
        }
    }
}

// Snapshot residency and freshness, owned alongside the revision ledger.
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

    fn is_fresh(&self, scope: Scope, now: Instant) -> bool {
        match scope {
            Scope::Root => self
                .root
                .as_ref()
                .is_some_and(|cached| cached.is_fresh(now)),

            Scope::Album(id) => self
                .albums
                .get(&id)
                .is_some_and(|cached| cached.is_fresh(now)),
        }
    }

    fn insert(&mut self, candidate: Candidate, now: Instant) {
        match candidate {
            Candidate::Root(snapshot) => {
                self.albums.retain(|id, _| snapshot.albums.contains_key(id));

                self.root = Some(Cached {
                    snapshot: Arc::new(snapshot),
                    completed: now,
                    used: now,
                });
            }

            Candidate::Album(id, snapshot) => {
                let used = self.albums.get(&id).map_or(now, |cached| cached.used);

                self.albums.insert(
                    id,
                    Cached {
                        snapshot: Arc::new(snapshot),
                        completed: now,
                        used,
                    },
                );
            }
        }
    }

    fn evict(&mut self) {
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
                .min_by_key(|(id, cached)| (cached.used, **id))
                .map(|(id, _)| *id)
                .expect("cache budget fits the root");

            self.albums.remove(&oldest);
        }
    }

    fn touch(&mut self, scope: Scope, now: Instant) {
        if let Scope::Album(id) = scope
            && let Some(cached) = self.albums.get_mut(&id)
        {
            cached.used = now;
        }
    }
}

struct Cached<T> {
    snapshot: Arc<T>,
    completed: Instant,
    used: Instant,
}

impl<T> Cached<T> {
    fn is_fresh(&self, now: Instant) -> bool {
        now.duration_since(self.completed) < FRESHNESS
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Scope {
    Root,
    Album(Uuid),
}

enum Candidate {
    Root(Root),
    Album(Uuid, Contents),
}

impl ImmichCatalog {
    pub(crate) fn new(
        config: Config,
        seed: u32,
        events: Subscriptions,
        activity: watch::Receiver<activity::Snapshot>,
    ) -> Result<(Self, CatalogTask)> {
        let source = Source::new(
            immich::Client::new(config.api_base, config.api_key)?,
            config.listen_address,
            config.friendly_name,
        );

        let (commands, receiver) = mpsc::channel(REQUESTS);

        Ok((
            Self {
                commands,
                collator: Arc::new(config.collator),
            },
            CatalogTask {
                commands: receiver,
                source: Arc::new(source),
                events,
                state: State {
                    ledger: Ledger::new(seed),
                    cache: Cache::new(),
                },
                pending: Vec::new(),
                activity,
                background: Background::default(),
            },
        ))
    }

    async fn view(&self, query: BrowseQuery, deadline: Instant) -> Result<View, Fault> {
        if Instant::now() >= deadline {
            return Err(FAILED);
        }

        let (reply, receiver) = oneshot::channel();

        self.commands
            .try_send(Command::Browse(Pending {
                query,
                deadline,
                reply,
                depends_on_active_refresh: false,
            }))
            .map_err(|_| FAILED)?;

        timeout_at(deadline, receiver)
            .await
            .map_err(|_| FAILED)?
            .map_err(|_| FAILED)?
    }
}

impl CatalogTask {
    pub(crate) async fn run(mut self) -> Result<()> {
        let mut active: Option<Refresh> = None;

        loop {
            let now = Instant::now();
            let activity = *self.activity.borrow_and_update();
            self.background.update(activity, &self.state.cache, now);
            self.settle_pending(active.as_ref().map(|refresh| refresh.scope));

            if active.is_none() {
                let next = self
                    .next_refresh_scope()
                    .map(|scope| (scope, false))
                    .or_else(|| {
                        self.background
                            .next(&self.state.cache, now)
                            .map(|scope| (scope, true))
                    });

                if let Some((scope, background)) = next {
                    active = Some(self.start(scope, background));

                    self.settle_pending(Some(scope));
                }
            }

            let deadline = self
                .pending
                .iter()
                .map(|request| request.deadline)
                .chain(self.background.deadline())
                .chain(activity.idle_deadline().filter(|deadline| *deadline > now))
                .min();

            let finished = async {
                match active.as_mut() {
                    Some(refresh) => refresh.future.as_mut().await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                command = self.commands.recv() => {
                    match command.ok_or_else(|| anyhow!("catalog command channel closed"))? {
                        Command::Browse(request) => {
                            // Ready views need no waiting slot. Prune cancellations too.
                            self.pending.push(request);
                            self.settle_pending(active.as_ref().map(|refresh| refresh.scope));

                            if self.pending.len() > REQUESTS {
                                let request = self.pending.pop().unwrap();
                                let _ = request.reply.send(Err(FAILED));
                            }
                        }

                        Command::SystemUpdateId(reply) => {
                            let _ = reply.send(self.state.ledger.system_update_id);
                        }

                        #[cfg(test)]
                        Command::Test(control) => control.apply(&mut self),
                    }
                }

                result = finished => {
                    let refresh = active.take().unwrap();
                    self.complete_refresh(refresh, result);
                }

                _ = sleep_until(deadline) => {}

                changed = self.activity.changed() => {
                    changed.map_err(|_| anyhow!("catalog activity channel closed"))?;
                }
            }
        }
    }

    fn complete_refresh(&mut self, refresh: Refresh, result: Result<Candidate>) {
        let result = result.and_then(|candidate| self.publish(candidate, refresh.deadline));

        if result.is_ok() {
            // Pin ready views before eviction can remove the newly published payload.
            self.settle_pending(None);
            self.state.cache.evict();
        }

        self.background
            .completed(refresh.scope, result.is_ok(), Instant::now());

        if let Err(error) = &result {
            tracing::warn!(scope = ?refresh.scope, background = refresh.background, %error, "catalog refresh failed");
        } else {
            tracing::debug!(scope = ?refresh.scope, background = refresh.background, update_id = self.state.ledger.system_update_id, "catalog refresh completed");
        }

        let mut index = 0;

        while index < self.pending.len() {
            if result.is_err() && self.pending[index].depends_on_active_refresh {
                let request = self.pending.remove(index);
                let _ = request.reply.send(Err(FAILED));
            } else {
                self.pending[index].depends_on_active_refresh = false;
                index += 1;
            }
        }
    }

    fn next_refresh_scope(&self) -> Option<Scope> {
        let now = Instant::now();

        self.pending.iter().find_map(|request| {
            if request.reply.is_closed() || now >= request.deadline {
                return None;
            }

            self.state.needed(request.query, now).ok().flatten()
        })
    }

    fn settle_pending(&mut self, active: Option<Scope>) {
        let now = Instant::now();
        let mut index = 0;

        while index < self.pending.len() {
            let request = &mut self.pending[index];

            let result = if request.reply.is_closed() || now >= request.deadline {
                Err(FAILED)
            } else {
                match self.state.needed(request.query, now) {
                    Ok(Some(scope)) => {
                        request.depends_on_active_refresh |= active == Some(scope);
                        index += 1;
                        continue;
                    }

                    Ok(None) => {
                        self.state.cache.touch(request.query.scope(), now);

                        self.state.view(request.query)
                    }

                    Err(error) => Err(error),
                }
            };

            let request = self.pending.remove(index);
            let _ = request.reply.send(result);
        }
    }

    fn start(&self, scope: Scope, background: bool) -> Refresh {
        let source = self.source.clone();
        let deadline = Instant::now() + REFRESH_TIMEOUT;

        let future = Box::pin(async move {
            ensure!(
                Instant::now() < deadline,
                "catalog refresh deadline exceeded"
            );

            let prepare = async {
                match scope {
                    Scope::Root => Ok(Candidate::Root(source.root().await?)),
                    Scope::Album(id) => Ok(Candidate::Album(id, source.contents(id).await?)),
                }
            };

            timeout_at(deadline, prepare)
                .await
                .map_err(|_| anyhow!("catalog refresh deadline exceeded"))?
        });

        Refresh {
            scope,
            background,
            deadline,
            future,
        }
    }

    fn publish(&mut self, candidate: Candidate, deadline: Instant) -> Result<()> {
        let state = &mut self.state;

        // Stage root reconciliation before the deadline check. A contents update
        // only mutates one existing entry after all fallible checks have passed.
        let next_root = match &candidate {
            Candidate::Root(root) => state.ledger.root_transition(root),
            Candidate::Album(..) => None,
        };

        ensure!(
            Instant::now() < deadline,
            "catalog refresh deadline exceeded"
        );

        let changed = match &candidate {
            Candidate::Root(_) => next_root.is_some(),

            Candidate::Album(id, contents) => {
                state.ledger.update_contents(*id, &contents.digest)?
            }
        };

        if let Some(next) = next_root {
            state.ledger = next;
        }

        state.cache.insert(candidate, Instant::now());

        if changed {
            self.events.publish(state.ledger.system_update_id);
        }

        Ok(())
    }
}

impl Catalog for ImmichCatalog {
    async fn system_update_id(&self) -> Result<u32, Fault> {
        let (reply, receiver) = oneshot::channel();

        self.commands
            .try_send(Command::SystemUpdateId(reply))
            .map_err(|_| FAILED)?;

        receiver.await.map_err(|_| FAILED)
    }

    async fn browse(&self, query: BrowseQuery, deadline: Instant) -> Result<BrowseResult, Fault> {
        let view = self.view(query, deadline).await?;

        Ok(view.browse(&self.collator))
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests;
