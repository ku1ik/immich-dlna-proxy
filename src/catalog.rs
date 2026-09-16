//! Browse-driven snapshots, durable revision history, and service-owned publication.
//!
//! Candidate construction, persistence, publication and notification-state updates
//! form one serialized, non-cancelable commit. Client deadlines only stop waiting.
//! History is never automatically pruned; exhausted or lost state requires explicit
//! recovery with a new server UUID and an empty state directory.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    net::SocketAddrV4,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use icu_collator::CollatorBorrowed;
use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};
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
    immich::{self, Asset, AssetFilter, Client},
    protocol::{BrowseArguments, Fault, Object, Resource, xml_char},
};

const FRESHNESS: Duration = Duration::from_secs(60);
const RESIDENT_ALBUMS: usize = 32;
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const ALBUM_ITEMS: usize = 20_000;
const MAX_ALBUMS: usize = 4_096;
const RETAINED_ALBUMS: usize = 16_384;
const REVISION_BYTES: usize = 8 * 1024 * 1024;
const SEARCH_PAGES: usize = 50;
const SEARCH_RECORDS: usize = 50_000;
const REFRESHES: usize = 4;
const PREPARATION_TIMEOUT: Duration = Duration::from_secs(20);
// The outer commit includes publication; persistence also runs independently at startup.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
const PERSIST_TIMEOUT: Duration = Duration::from_secs(5);

/// Select, sort and paginate objects, capturing their revision together.
pub trait Catalog: Send + Sync + 'static {
    fn system_update_id(&self) -> u32;
    fn browse(
        &self,
        arguments: BrowseArguments,
    ) -> impl Future<Output = Result<BrowseResult, Fault>> + Send;
}

pub struct BrowseResult {
    pub objects: Vec<Object>,
    pub total_matches: u32,
    pub update_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectId {
    Root,
    Album(Uuid),
    Item { album: Uuid, asset: Uuid },
}

pub fn parse_id(value: &str) -> Result<ObjectId, Fault> {
    let invalid = Fault { code: 701 };
    let mut parts = value.split(':');

    match (parts.next(), parts.next(), parts.next()) {
        (Some("0"), None, None) => Ok(ObjectId::Root),

        (Some("album"), Some(album), None) => Ok(ObjectId::Album(
            Uuid::parse_str(album).map_err(|_| invalid)?,
        )),

        (Some("album"), Some(album), Some("asset")) => {
            let asset = parts.next().ok_or(invalid)?;

            if parts.next().is_some() {
                return Err(invalid);
            }

            Ok(ObjectId::Item {
                album: Uuid::parse_str(album).map_err(|_| invalid)?,
                asset: Uuid::parse_str(asset).map_err(|_| invalid)?,
            })
        }

        _ => Err(invalid),
    }
}

fn compare_dates(
    a_date: Option<&str>,
    a_capture: Option<&DateTime<Utc>>,
    a_id: Uuid,
    b_date: Option<&str>,
    b_capture: Option<&DateTime<Utc>>,
    b_id: Uuid,
    descending: bool,
) -> Ordering {
    fn optional<T: Ord>(a: Option<T>, b: Option<T>, descending: bool) -> Ordering {
        match (a, b) {
            (Some(a), Some(b)) if descending => b.cmp(&a),
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    optional(a_date, b_date, descending)
        .then_with(|| optional(a_capture, b_capture, descending))
        .then_with(|| a_id.cmp(&b_id))
}

// Snapshot construction.

#[derive(Clone)]
struct Source {
    client: Client,
    http_address: SocketAddrV4,
    friendly_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Album {
    id: Uuid,
    object: Object,
    created_at: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    #[serde(skip)]
    digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Item {
    id: Uuid,
    object: Object,
    capture: Option<DateTime<Utc>>,
    is_edited: bool,
    checksum: Option<String>,
    updated_at: Option<String>,
    thumbhash: Option<String>,
}

#[derive(Clone, Debug)]
struct Root {
    object: Object,
    albums: BTreeMap<Uuid, Album>,
    digest: String,
    bytes: usize,
}

#[derive(Clone, Debug)]
struct Contents {
    items: BTreeMap<Uuid, Item>,
    digest: String,
    bytes: usize,
}

impl Source {
    fn new(client: Client, http_address: SocketAddrV4, friendly_name: String) -> Self {
        Self {
            client,
            http_address,
            friendly_name,
        }
    }

    /// The caller admits and bounds the whole refresh, including version checking.
    async fn root(&self) -> Result<Root> {
        self.client.ensure_supported_version().await?;
        let records = self.client.albums().await?;

        let root_object = Object {
            id: "0".into(),
            parent_id: "-1".into(),
            title: self.friendly_name.clone(),
            class: "object.container".into(),
            date: None,
            art: None,
            child_count: None,
            resources: Vec::new(),
        };

        let mut albums = BTreeMap::new();
        let mut bytes = 2 + encoded_size(&root_object, SNAPSHOT_BYTES - 2)?;
        let mut bad_dates = 0;

        for dto in records {
            let created_at = parse_date(dto.created_at.as_deref(), &mut bad_dates);

            let mut album = Album {
                id: dto.id,
                object: Object {
                    id: format!("album:{}", dto.id),
                    parent_id: "0".into(),
                    title: title(&dto.album_name, dto.id),
                    class: "object.container.album".into(),
                    date: created_at.map(|date| date.format("%Y-%m-%d").to_string()),
                    art: dto
                        .album_thumbnail_asset_id
                        .map(|id| self.media_url(id, "preview")),
                    child_count: None,
                    resources: Vec::new(),
                },
                created_at,
                end_date: parse_date(dto.end_date.as_deref(), &mut bad_dates),
                digest: String::new(),
            };

            let mut projection = Projection::new(SNAPSHOT_BYTES);
            projection.json(&album)?;
            let (digest, size) = projection.finish();
            album.digest = digest;

            if let Some(previous) = albums.get(&album.id) {
                ensure!(previous == &album, "conflicting duplicate Immich album");
                continue;
            }

            ensure!(albums.len() < MAX_ALBUMS, "Immich root exceeds album limit");

            bytes += 1 + size;

            ensure!(
                bytes <= SNAPSHOT_BYTES,
                "Immich root exceeds projected byte limit"
            );

            albums.insert(album.id, album);
        }

        log_dates(bad_dates);
        let mut projection = Projection::new(SNAPSHOT_BYTES);
        projection.write_all(b"[")?;
        projection.json(&root_object)?;

        for album in albums.values() {
            projection.write_all(b",")?;
            projection.json(album)?;
        }

        projection.write_all(b"]")?;
        let (digest, bytes) = projection.finish();

        Ok(Root {
            object: root_object,
            albums,
            digest,
            bytes,
        })
    }

    /// Requires the caller to establish readable root membership first. No version
    /// request, admission queue, timeout extension, or publication happens here.
    async fn contents(&self, album: Uuid) -> Result<Contents> {
        let mut items = BTreeMap::<Uuid, Item>::new();
        let mut excluded = BTreeSet::new();
        let mut encoded_ids = BTreeSet::new();
        let mut pages = 0;
        let mut records = 0;
        let mut bytes = 2;
        let mut bad_dates = 0;

        for encoded in [false, true] {
            if encoded
                && !items
                    .values()
                    .any(|item| item.object.class == "object.item.videoItem")
            {
                break;
            }

            let mut page = 1;

            loop {
                ensure!(
                    pages < SEARCH_PAGES && records < SEARCH_RECORDS,
                    "Immich album traversal limit exceeded"
                );

                pages += 1;

                let filter = if encoded {
                    AssetFilter::EncodedVideos
                } else {
                    AssetFilter::All
                };

                let result = self.client.search_album(album, page, filter).await?;

                ensure!(
                    result.items.len() <= SEARCH_RECORDS - records,
                    "Immich album record limit exceeded"
                );

                records += result.items.len();
                let mut advanced = false;

                for dto in result.items {
                    if encoded {
                        advanced |= encoded_ids.insert(dto.id);
                        continue;
                    }

                    let id = dto.id;
                    let item = self.project(album, dto, &mut bad_dates)?;

                    match item {
                        Some(item) => {
                            ensure!(
                                !excluded.contains(&id),
                                "conflicting duplicate Immich asset eligibility"
                            );

                            if let Some(previous) = items.get(&id) {
                                ensure!(previous == &item, "conflicting duplicate Immich asset");
                                continue;
                            }

                            ensure!(items.len() < ALBUM_ITEMS, "Immich album exceeds item limit");

                            bytes += usize::from(!items.is_empty())
                                + encoded_size(&item, SNAPSHOT_BYTES - bytes)?;

                            ensure!(
                                bytes <= SNAPSHOT_BYTES,
                                "Immich album exceeds projected byte limit"
                            );

                            items.insert(id, item);
                            advanced = true;
                        }

                        None => {
                            ensure!(
                                !items.contains_key(&id),
                                "conflicting duplicate Immich asset eligibility"
                            );

                            advanced |= excluded.insert(id);
                        }
                    }
                }

                let Some(next) = result.next_page else {
                    break;
                };

                ensure!(advanced, "Immich search continuation made no progress");
                page = next;
            }
        }

        for id in encoded_ids {
            if let Some(item) = items.get_mut(&id)
                && item.object.class == "object.item.videoItem"
            {
                let old_size = encoded_size(item, SNAPSHOT_BYTES)?;

                item.object.resources.push(Resource {
                    uri: self.media_url(id, "playback"),
                    mime: "video/mp4".into(),
                    duration: None,
                    byte_seek: true,
                });

                bytes -= old_size;
                bytes += encoded_size(item, SNAPSHOT_BYTES - bytes)?;
            }
        }

        log_dates(bad_dates);
        let mut projection = Projection::new(SNAPSHOT_BYTES);
        projection.write_all(b"[")?;

        for (index, item) in items.values().enumerate() {
            if index != 0 {
                projection.write_all(b",")?;
            }

            projection.json(item)?;
        }

        projection.write_all(b"]")?;
        let (digest, bytes) = projection.finish();

        Ok(Contents {
            items,
            digest,
            bytes,
        })
    }

    fn media_url(&self, id: Uuid, representation: &str) -> String {
        format!(
            "http://{}/media/assets/{id}/{representation}",
            self.http_address
        )
    }

    fn project(&self, album: Uuid, dto: Asset, bad_dates: &mut usize) -> Result<Option<Item>> {
        if !matches!(dto.kind.as_str(), "IMAGE" | "VIDEO")
            || !matches!(dto.visibility.as_str(), "timeline" | "archive")
            || dto.is_trashed
        {
            return Ok(None);
        }

        let name = dto
            .original_file_name
            .ok_or_else(|| anyhow!("eligible Immich asset is missing originalFileName"))?;

        let video = dto.kind == "VIDEO";
        let mime = dto.original_mime_type.as_deref().and_then(media_type);

        let (representation, mime) = if video {
            (
                "original",
                mime.unwrap_or_else(|| "application/octet-stream".into()),
            )
        } else if !dto.is_edited
            && mime
                .as_deref()
                .is_some_and(|mime| matches!(mime, "image/jpeg" | "image/png" | "image/gif"))
        {
            ("original", mime.expect("checked original image MIME"))
        } else {
            ("display", "image/jpeg".into())
        };

        let duration = dto
            .duration
            .filter(|ms| (0..=i32::MAX as i64).contains(ms))
            .filter(|_| video)
            .map(|ms| {
                format!(
                    "{}:{:02}:{:02}.{:03}",
                    ms / 3_600_000,
                    ms / 60_000 % 60,
                    ms / 1000 % 60,
                    ms % 1000
                )
            });

        let mut resources = vec![Resource {
            uri: self.media_url(dto.id, representation),
            mime,
            duration,
            byte_seek: video,
        }];

        if !video {
            resources.push(Resource {
                uri: self.media_url(dto.id, "preview"),
                mime: "image/jpeg".into(),
                duration: None,
                byte_seek: false,
            });
        }

        let capture = parse_date(dto.file_created_at.as_deref(), bad_dates);

        let date = dto
            .local_date_time
            .as_deref()
            .and_then(|date| DateTime::parse_from_rfc3339(date).ok())
            .map(|date| date.format("%Y-%m-%d").to_string());

        *bad_dates += usize::from(date.is_none());

        Ok(Some(Item {
            id: dto.id,
            object: Object {
                id: format!("album:{album}:asset:{}", dto.id),
                parent_id: format!("album:{album}"),
                title: title(&name, dto.id),
                class: if video {
                    "object.item.videoItem"
                } else {
                    "object.item.imageItem.photo"
                }
                .into(),
                date,
                art: Some(self.media_url(dto.id, "preview")),
                child_count: None,
                resources,
            },
            capture,
            is_edited: dto.is_edited,
            checksum: dto.checksum,
            updated_at: dto.updated_at,
            thumbhash: dto.thumbhash,
        }))
    }
}

fn parse_date(value: Option<&str>, bad_dates: &mut usize) -> Option<DateTime<Utc>> {
    let parsed = value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|date| date.with_timezone(&Utc));

    *bad_dates += usize::from(parsed.is_none());

    parsed
}

fn log_dates(count: usize) {
    if count != 0 {
        tracing::debug!(
            count,
            "Immich catalog omitted missing or invalid optional dates"
        );
    }
}

fn xml_text(value: &str) -> String {
    value
        .chars()
        .map(|c| if xml_char(c) { c } else { '\u{fffd}' })
        .collect()
}

fn title(value: &str, id: Uuid) -> String {
    if value.is_empty() {
        id.to_string()
    } else {
        xml_text(value)
    }
}

struct ByteCount {
    bytes: usize,
    limit: usize,
}

impl Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes {
            return Err(std::io::Error::other(
                "catalog projected byte limit exceeded",
            ));
        }

        self.bytes += bytes.len();

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// Hash and count the same canonical JSON without allocating a serialized snapshot.
struct Projection {
    hash: Sha256,
    count: ByteCount,
}

impl Projection {
    fn new(limit: usize) -> Self {
        Self {
            hash: Sha256::new(),
            count: ByteCount { bytes: 0, limit },
        }
    }

    fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(self, value)
            .map_err(|_| anyhow!("catalog projected byte limit exceeded"))
    }

    fn finish(self) -> (String, usize) {
        (format!("{:x}", self.hash.finalize()), self.count.bytes)
    }
}

impl Write for Projection {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.count.write_all(bytes)?;
        self.hash.update(bytes);

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encoded_size(value: &impl Serialize, limit: usize) -> Result<usize> {
    let mut count = ByteCount { bytes: 0, limit };

    serde_json::to_writer(&mut count, value)
        .map_err(|_| anyhow!("catalog projected byte limit exceeded"))?;

    Ok(count.bytes)
}

fn media_type(value: &str) -> Option<String> {
    crate::mime::parse(value).map(str::to_ascii_lowercase)
}

// Revision history and durable storage.

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    pub server_uuid: Uuid,
    pub system_update_id: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    pub root_digest: Option<String>,
    #[serde(deserialize_with = "deserialize_albums")]
    pub albums: BTreeMap<Uuid, AlbumRevision>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AlbumRevision {
    pub update_id: u32,
    pub present: bool,
    pub metadata_digest: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub contents_digest: Option<String>,
}

fn deserialize_albums<'de, D>(deserializer: D) -> Result<BTreeMap<Uuid, AlbumRevision>, D::Error>
where
    D: Deserializer<'de>,
{
    struct Albums;

    impl<'de> de::Visitor<'de> for Albums {
        type Value = BTreeMap<Uuid, AlbumRevision>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded map of distinct album UUIDs")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut albums = BTreeMap::new();

            while let Some(id) = map.next_key::<Uuid>()? {
                if albums.contains_key(&id) {
                    return Err(de::Error::custom("duplicate album UUID"));
                }

                if albums.len() == RETAINED_ALBUMS {
                    return Err(de::Error::custom(
                        "revision history exceeds 16384 identities",
                    ));
                }

                albums.insert(id, map.next_value()?);
            }

            Ok(albums)
        }
    }

    deserializer.deserialize_map(Albums)
}

fn validate_digest(digest: &str) -> anyhow::Result<()> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "revision digest must be 64 lowercase hexadecimal characters"
    );

    Ok(())
}

impl Ledger {
    fn validate(&self, uuid: Uuid) -> anyhow::Result<()> {
        ensure!(!uuid.is_nil(), "server UUID must not be nil");

        ensure!(
            self.server_uuid == uuid,
            "revision state has the wrong server UUID"
        );

        ensure!(
            self.albums.len() <= RETAINED_ALBUMS,
            "revision history exceeds 16384 identities; use a new server UUID and empty state directory"
        );

        ensure!(
            self.albums.values().filter(|album| album.present).count() <= MAX_ALBUMS,
            "revision state exceeds 4096 present albums"
        );

        ensure!(
            self.root_digest.is_some() || self.albums.is_empty(),
            "revision state with retained albums requires a root digest"
        );

        if let Some(digest) = &self.root_digest {
            validate_digest(digest)?;
        }

        for album in self.albums.values() {
            validate_digest(&album.metadata_digest)?;

            if let Some(digest) = &album.contents_digest {
                validate_digest(digest)?;
            }
        }

        Ok(())
    }

    /// Invalidate every retained counter once, without forgetting any digest.
    /// Persist this transition before binding listeners or announcing the device.
    pub fn restart(&mut self) {
        self.system_update_id = self.system_update_id.wrapping_add(1);

        for album in self.albums.values_mut() {
            album.update_id = album.update_id.wrapping_add(1);
        }
    }

    /// `albums` is the complete present map with internally generated metadata digests.
    /// The ledger is validated on load and again before persistence.
    /// `None` means no revision write, even when a snapshot needs a cache refill.
    fn root_transition(
        &self,
        root_digest: &str,
        albums: &BTreeMap<Uuid, String>,
    ) -> anyhow::Result<Option<Self>> {
        ensure!(
            albums.len() <= MAX_ALBUMS,
            "root exceeds 4096 present albums"
        );

        let new_identities = albums
            .keys()
            .filter(|id| !self.albums.contains_key(id))
            .count();

        ensure!(
            self.albums.len() + new_identities <= RETAINED_ALBUMS,
            "revision history exhausted; stop the service, archive state, and use a new server UUID and empty private directory"
        );

        let mut next = self.clone();
        let mut changed = self.root_digest.as_deref() != Some(root_digest);

        for (id, album) in &mut next.albums {
            let metadata = albums.get(id);
            let present = metadata.is_some();
            let metadata_changed = metadata.is_some_and(|digest| digest != &album.metadata_digest);

            if album.present != present || metadata_changed {
                album.update_id = album.update_id.wrapping_add(1);
                album.present = present;
                changed = true;

                if let Some(digest) = metadata {
                    album.metadata_digest.clone_from(digest);
                }
            }
        }

        for (id, metadata_digest) in albums {
            if !next.albums.contains_key(id) {
                next.albums.insert(
                    *id,
                    AlbumRevision {
                        update_id: 0,
                        present: true,
                        metadata_digest: metadata_digest.clone(),
                        contents_digest: None,
                    },
                );

                changed = true;
            }
        }

        if !changed {
            return Ok(None);
        }

        next.root_digest = Some(root_digest.to_owned());
        next.system_update_id = next.system_update_id.wrapping_add(1);

        Ok(Some(next))
    }

    fn contents_transition(&self, id: Uuid, digest: &str) -> anyhow::Result<Option<Self>> {
        let album = self.albums.get(&id).filter(|album| album.present);
        let album = album.context("contents transition requires a present album")?;

        if album.contents_digest.as_deref() == Some(digest) {
            return Ok(None);
        }

        let mut next = self.clone();

        let album = next
            .albums
            .get_mut(&id)
            .expect("validated album exists in clone");

        album.update_id = album.update_id.wrapping_add(1);
        album.contents_digest = Some(digest.to_owned());
        next.system_update_id = next.system_update_id.wrapping_add(1);

        Ok(Some(next))
    }
}

/// Owns the exclusive directory lock, also retained by every disk worker.
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    directory: File,
    path: PathBuf,
    uuid: Uuid,
    #[cfg(test)]
    fault: Option<revision_tests::Fault>,
}

fn check_private(metadata: &fs::Metadata, directory: bool) -> anyhow::Result<()> {
    // SAFETY: geteuid has no preconditions and does not modify process state.
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "state is not owned by this user"
    );

    if directory {
        ensure!(metadata.is_dir(), "state directory is not a directory");

        ensure!(
            metadata.mode() & 0o777 == 0o700,
            "state directory must be mode 0700"
        );
    } else {
        ensure!(metadata.is_file(), "revision files must be regular files");

        ensure!(
            metadata.mode() & 0o777 == 0o600,
            "revision files must be mode 0600"
        );

        ensure!(
            metadata.nlink() == 1,
            "revision files must not be hard links"
        );
    }

    Ok(())
}

impl Store {
    /// Lock and load synchronously; never perform the startup revision write here.
    /// The existing directory must be private, writable, trusted local storage.
    /// Resolve a configured symlink once, as used by systemd DynamicUser. The
    /// directory and its parent paths must not be moved or replaced while running.
    pub fn open(directory: &Path, uuid: Uuid) -> anyhow::Result<(Self, Ledger)> {
        ensure!(!uuid.is_nil(), "server UUID must not be nil");

        let path = directory
            .canonicalize()
            .context("resolve revision directory")?;

        let directory_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NONBLOCK)
            .open(&path)
            .context("open revision directory")?;

        check_private(&directory_file.metadata()?, true)?;

        // Lock before loading state or removing an uncommitted temporary file.
        directory_file
            .try_lock()
            .context("revision directory is already locked or cannot be locked")?;

        // O_NONBLOCK lets file-type validation reject a FIFO without hanging.
        let committed = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.join("revisions.json"));

        let ledger = match committed {
            Ok(file) => {
                check_private(&file.metadata()?, false)?;

                ensure!(
                    file.metadata()?.len() <= REVISION_BYTES as u64,
                    "revision file exceeds 8 MiB"
                );

                let mut bytes = Vec::new();

                file.take(REVISION_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;

                ensure!(bytes.len() <= REVISION_BYTES, "revision file exceeds 8 MiB");

                let ledger: Ledger = serde_json::from_slice(&bytes).map_err(|error| {
                    anyhow::anyhow!(
                        "invalid revision JSON at line {}, column {}",
                        error.line(),
                        error.column()
                    )
                })?;

                ledger.validate(uuid)?;

                match fs::remove_file(path.join("revisions.tmp")) {
                    Ok(()) => {}

                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}

                    Err(error) => {
                        return Err(error).context("remove uncommitted revision temporary file");
                    }
                }

                ledger
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut entries = fs::read_dir(&path).context("inspect new revision directory")?;

                ensure!(
                    entries.next().transpose()?.is_none(),
                    "revision state is missing in a nonempty directory; restore state or use a new server UUID"
                );

                Ledger {
                    server_uuid: uuid,
                    system_update_id: 0,
                    root_digest: None,
                    albums: BTreeMap::new(),
                }
            }

            Err(error) => return Err(error).context("open committed revision state"),
        };

        let store = Self {
            inner: Arc::new(StoreInner {
                directory: directory_file,
                path,
                uuid,
                #[cfg(test)]
                fault: None,
            }),
        };

        Ok((store, ledger))
    }

    /// Durably replace state or exit(1), including on cancellation after polling.
    ///
    /// PRECONDITION: a service-owned, non-cancelable task holds commit serialization
    /// from candidate construction through this call AND subsequent publication and
    /// subscriber updates. This method does not serialize callers or publish state.
    /// Never wrap it in a client/request timeout. The blocking worker retains the
    /// process lock; failure/timeout exits without waiting for worker/runtime drop.
    pub async fn persist(&self, ledger: Ledger) {
        struct Commit;

        impl Drop for Commit {
            fn drop(&mut self) {
                fail_stop("revision commit cancelled before durable completion");
            }
        }

        let guard = Commit;
        let deadline = tokio::time::Instant::now() + PERSIST_TIMEOUT;
        let inner = Arc::clone(&self.inner);

        let worker = tokio::task::spawn_blocking(move || {
            let result = inner.write(ledger);
            drop(inner);

            result
        });

        #[cfg(test)]
        if matches!(
            self.inner.fault,
            Some(revision_tests::Fault::LateObservation)
        ) {
            // Observe a ready worker only once the commit deadline has elapsed.
            while !worker.is_finished() {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            tokio::time::advance(PERSIST_TIMEOUT + std::time::Duration::from_millis(1)).await;
        }

        match tokio::time::timeout_at(deadline, worker).await {
            Ok(Ok(Ok(()))) if tokio::time::Instant::now() < deadline => std::mem::forget(guard),

            Ok(Ok(Err(error))) => {
                tracing::error!(
                    operation = error.operation,
                    kind = ?error.kind,
                    os_code = ?error.os_code,
                    "revision persistence failed; terminating without publishing candidate state"
                );

                std::process::exit(1);
            }

            Ok(Err(_)) => fail_stop("revision persistence worker failed"),
            Ok(Ok(Ok(()))) | Err(_) => fail_stop("revision commit exceeded five seconds"),
        }
    }
}

fn fail_stop(message: &'static str) -> ! {
    // Log only a fixed diagnostic, never JSON, paths or source data.
    tracing::error!("{message}; terminating without publishing candidate state");

    std::process::exit(1);
}

#[derive(Debug)]
struct PersistenceError {
    operation: &'static str,
    kind: Option<io::ErrorKind>,
    os_code: Option<i32>,
}

struct BoundedBytes(Vec<u8>);

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > REVISION_BYTES - self.0.len() {
            return Err(io::Error::other("serialized revision file exceeds 8 MiB"));
        }

        self.0.extend_from_slice(bytes);

        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl StoreInner {
    fn write(&self, ledger: Ledger) -> Result<(), PersistenceError> {
        let mut operation = "validate revision state";

        let result = (|| {
            ledger.validate(self.uuid)?;
            let mut bytes = BoundedBytes(Vec::new());
            operation = "serialize bounded revision state";
            serde_json::to_writer(&mut bytes, &ledger)?;
            operation = "create private revision temporary file";

            let mut temporary = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(self.path.join("revisions.tmp"))?;

            operation = "inspect revision temporary file";
            check_private(&temporary.metadata()?, false)?;
            let (first, second) = bytes.0.split_at(bytes.0.len() / 2);
            operation = "write revision state";
            temporary.write_all(first)?;
            #[cfg(test)]
            self.inject(revision_tests::Stage::Write)?;
            temporary.write_all(second)?;
            operation = "flush revision state";
            temporary.flush()?;
            operation = "sync revision state";
            #[cfg(test)]
            self.inject(revision_tests::Stage::FileSync)?;
            temporary.sync_all()?;
            operation = "replace revision state";
            #[cfg(test)]
            self.inject(revision_tests::Stage::Rename)?;

            fs::rename(
                self.path.join("revisions.tmp"),
                self.path.join("revisions.json"),
            )?;

            operation = "sync revision directory";
            #[cfg(test)]
            self.inject(revision_tests::Stage::DirectorySync)?;
            self.directory.sync_all()?;

            Ok(())
        })();

        // Discard all source text before returning to the logging task.
        result.map_err(|error: anyhow::Error| {
            let io = error
                .chain()
                .find_map(|cause| cause.downcast_ref::<io::Error>());

            PersistenceError {
                operation,
                kind: io.map(io::Error::kind),
                os_code: io.and_then(io::Error::raw_os_error),
            }
        })
    }
}

// Fork temporarily inherits flock descriptors even with CLOEXEC. All subprocess
// tests share this guard with close/reopen tests until exec closes those copies.
#[cfg(test)]
static SPAWN_OR_REOPEN: std::sync::Mutex<()> = std::sync::Mutex::new(());

// Refresh coordination, publication, and browsing.

const FAILED: Fault = Fault { code: 501 };
const MISSING: Fault = Fault { code: 701 };

// Eviction always has room for the retained root and the album being published.
const _: () = {
    assert!(2 * SNAPSHOT_BYTES <= CACHE_BYTES);
    assert!(RESIDENT_ALBUMS > 0);
};

#[derive(Clone)]
pub struct Library {
    inner: Arc<Inner>,
}

struct Inner {
    source: Source,
    collator: CollatorBorrowed<'static>,
    store: Store,
    events: Subscriptions,
    failure: CancellationToken,
    state: Mutex<State>,
    commit: AsyncMutex<()>,
    permits: Arc<Semaphore>,
    supervisor: Mutex<Supervisor>,
    wake: Notify,
    preparation_timeout: Duration,
    #[cfg(test)]
    publication: Mutex<Option<Arc<tests::Barrier>>>,
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

// A response pins one publication independently of subsequent cache eviction.
#[derive(Clone)]
struct View {
    root: Arc<Root>,
    contents: Option<Arc<Contents>>,
    ledger: Arc<Ledger>,
}

type RefreshResult = Result<View, Fault>;

impl State {
    fn view(&self, scope: Scope) -> RefreshResult {
        let contents = match scope {
            Scope::Root => None,
            Scope::Album(id) => Some(self.cache.albums.get(&id).ok_or(FAILED)?.snapshot.clone()),
        };

        Ok(View {
            root: self.cache.root.as_ref().ok_or(FAILED)?.snapshot.clone(),
            contents,
            ledger: self.ledger.clone(),
        })
    }
}

// Snapshot residency and freshness; synchronized with the ledger by State's lock.
struct Cache {
    root: Option<Cached<Root>>,
    albums: BTreeMap<Uuid, Cached<Contents>>,
    freshness: Duration,
    album_limit: usize,
    byte_limit: usize,
}

impl Cache {
    fn new() -> Self {
        Self {
            root: None,
            albums: BTreeMap::new(),
            freshness: FRESHNESS,
            album_limit: RESIDENT_ALBUMS,
            byte_limit: CACHE_BYTES,
        }
    }

    fn is_fresh(&mut self, scope: Scope, now: Instant) -> bool {
        match scope {
            Scope::Root => self
                .root
                .as_mut()
                .is_some_and(|cached| cached.is_fresh(now, self.freshness)),

            Scope::Album(id) => self
                .albums
                .get_mut(&id)
                .is_some_and(|cached| cached.is_fresh(now, self.freshness)),
        }
    }

    fn invalidate(&mut self, before: &Ledger, after: &Ledger) {
        self.albums.retain(|id, _| {
            let before = before.albums.get(id);
            let after = after.albums.get(id);

            after.is_some_and(|album| album.present)
                && before.map(|album| album.present) == after.map(|album| album.present)
        });
    }

    fn insert(&mut self, candidate: Candidate, now: Instant) {
        let scope = match candidate {
            Candidate::Root(snapshot) => {
                self.root = Some(Cached {
                    snapshot,
                    completed: now,
                    used: now,
                });

                Scope::Root
            }

            Candidate::Album(id, snapshot) => {
                self.albums.insert(
                    id,
                    Cached {
                        snapshot,
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
                if let Some(cached) = self.albums.get_mut(&id) {
                    cached.completed = completed;
                }
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
    fn is_fresh(&mut self, now: Instant, freshness: Duration) -> bool {
        self.used = now;

        now.duration_since(self.completed) < freshness
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
        self.0.inner.supervisor.lock().unwrap().tasks.abort_all();
    }
}

impl Library {
    /// The caller must restart and persist the ledger before construction/admission.
    pub fn new(
        config: Config,
        store: Store,
        ledger: Ledger,
        events: Subscriptions,
    ) -> Result<Self> {
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
                store,
                events,
                failure: CancellationToken::new(),
                state: Mutex::new(State {
                    ledger: Arc::new(ledger),
                    cache: Cache::new(),
                    flights: BTreeMap::new(),
                }),
                commit: AsyncMutex::new(()),
                permits: Arc::new(Semaphore::new(REFRESHES)),
                supervisor: Mutex::new(Supervisor {
                    tasks: JoinSet::new(),
                    running: false,
                }),
                wake: Notify::new(),
                preparation_timeout: PREPARATION_TIMEOUT,
                #[cfg(test)]
                publication: Mutex::new(None),
                #[cfg(test)]
                prepared: Mutex::new(None),
            }),
        })
    }

    /// Supervise refresh tasks and fail after cancelling preparation on task failure.
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

            if let Scope::Album(id) = scope
                && !state
                    .ledger
                    .albums
                    .get(&id)
                    .is_some_and(|album| album.present)
            {
                return Err(MISSING);
            }

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

                let token = match scope {
                    Scope::Root => Token::Root,
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
                let deadline = now + self.inner.preparation_timeout;

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

    async fn refresh(&self, scope: Scope, token: Token, preparation: Instant) -> Result<View> {
        ensure!(
            !self.inner.failure.is_cancelled() && Instant::now() < preparation,
            "catalog preparation failed or expired"
        );

        let prepare = async {
            let candidate = match scope {
                Scope::Root => Candidate::Root(Arc::new(self.inner.source.root().await?)),
                Scope::Album(id) => {
                    Candidate::Album(id, Arc::new(self.inner.source.contents(id).await?))
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
            _ = self.inner.failure.cancelled() => return Err(anyhow!("catalog preparation cancelled after task failure")),

            result = timeout_at(preparation, prepare) => {
                result.map_err(|_| anyhow!("catalog preparation deadline exceeded"))??
            }
        };

        ensure!(
            !self.inner.failure.is_cancelled() && Instant::now() < preparation,
            "catalog preparation failed or expired"
        );

        let deadline = Instant::now() + COMMIT_TIMEOUT;

        let ledger = {
            let state = self.inner.state.lock().unwrap();
            ensure!(token.applicable(&state.ledger), "stale catalog candidate");

            state.ledger.clone()
        };

        let next = match &candidate {
            Candidate::Root(root) => {
                let albums = root
                    .albums
                    .iter()
                    .map(|(id, album)| (*id, album.digest.clone()))
                    .collect();

                ledger.root_transition(&root.digest, &albums)?
            }

            Candidate::Album(id, contents) => ledger.contents_transition(*id, &contents.digest)?,
        };

        ensure!(
            !self.inner.failure.is_cancelled()
                && Instant::now() < preparation
                && Instant::now() < deadline,
            "catalog preparation failed or expired"
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
                    assert!(!barrier.panic, "injected publication panic");
                }
            }

            // The commit gate remains owned from applicability checking through
            // publication, so no other refresh can interleave a ledger write.
            let mut state = self.inner.state.lock().unwrap();

            ensure!(
                Instant::now() < deadline,
                "catalog publication deadline exceeded"
            );

            if let Some(next) = next {
                let State { ledger, cache, .. } = &mut *state;
                cache.invalidate(ledger, &next);
                *ledger = Arc::new(next);
            }

            state.cache.insert(candidate, Instant::now());

            if changed {
                self.inner.events.publish(state.ledger.system_update_id);
            }

            let view = state
                .view(scope)
                .map_err(|_| anyhow!("catalog publication missing snapshot"))?;

            // Freshness starts after the entire publication boundary, not fetch.
            let completed = Instant::now();
            state.cache.mark_completed(scope, completed);

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

    async fn browse(&self, query: BrowseArguments) -> Result<BrowseResult, Fault> {
        let id = parse_id(&query.object_id)?;
        let mut view = self.fresh(Scope::Root).await?;

        match id {
            ObjectId::Album(album) if !query.metadata => {
                view = self.fresh(Scope::Album(album)).await?
            }

            ObjectId::Item { album, .. } => view = self.fresh(Scope::Album(album)).await?,

            _ => {}
        }

        view.browse(id, query, &self.inner.collator)
    }
}

impl View {
    fn browse(
        &self,
        id: ObjectId,
        query: BrowseArguments,
        collator: &CollatorBorrowed<'_>,
    ) -> Result<BrowseResult, Fault> {
        let Self {
            root,
            contents,
            ledger,
        } = self;

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

        if query.metadata {
            let object = match id {
                ObjectId::Root => {
                    let mut object = root.object.clone();
                    object.child_count = Some(root.albums.len());

                    object
                }

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

                rows.sort_unstable_by(|a, b| match query.sort {
                    None => b
                        .end_date
                        .cmp(&a.end_date)
                        .then_with(|| collator.compare(&a.object.title, &b.object.title))
                        .then_with(|| a.id.cmp(&b.id)),

                    Some(descending) => compare_dates(
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
                    compare_dates(
                        query.sort.and(a.object.date.as_deref()),
                        a.capture.as_ref(),
                        a.id,
                        query.sort.and(b.object.date.as_deref()),
                        b.capture.as_ref(),
                        b.id,
                        query.sort.unwrap_or(false),
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
            .skip(query.starting_index as usize)
            .take(if query.requested_count == 0 {
                usize::MAX
            } else {
                query.requested_count as usize
            })
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
mod snapshot_tests {
    use super::*;
    use axum::response::Response;
    use http::Method;
    use serde_json::{Value, json};

    use crate::immich::tests::{Fake as Api, Received, album, asset, page, reply, version};

    const ALBUM: Uuid = Uuid::from_u128(100_000);

    struct SnapshotFixture {
        source: Source,
        requests: Arc<Mutex<Vec<Received>>>,
        _api: Api,
    }

    impl SnapshotFixture {
        async fn new(replies: Vec<Response>) -> Self {
            let api = Api::new(replies).await;

            let source = Source::new(
                api.client.clone(),
                "192.0.2.1:8200".parse().unwrap(),
                "Photos".into(),
            );

            Self {
                source,
                requests: api.requests.clone(),
                _api: api,
            }
        }
    }

    fn project(source: &Source, value: Value) -> Item {
        source
            .project(ALBUM, serde_json::from_value(value).unwrap(), &mut 0)
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn resources_dates_and_optional_hints() {
        let fake = SnapshotFixture::new(vec![]).await;

        for (mime, edited, representation, expected) in [
            (
                Some("IMAGE/JPEG; quality=90"),
                false,
                "original",
                "image/jpeg",
            ),
            (Some("image/png"), false, "original", "image/png"),
            (Some("image/jpeg; q =90"), false, "original", "image/jpeg"),
            (Some("image/jpeg; q= 90"), false, "original", "image/jpeg"),
            (
                Some("IMAGE/JPEG;; q \t=\t \"90\"; ; x = y;"),
                false,
                "original",
                "image/jpeg",
            ),
            (Some("image/gif"), false, "original", "image/gif"),
            (Some("image/jpeg"), true, "display", "image/jpeg"),
            (Some("image/heic"), false, "display", "image/jpeg"),
            (Some("image/webp"), false, "display", "image/jpeg"),
            (Some("image/jpeg;broken"), false, "display", "image/jpeg"),
            (Some("image/jpeg; q = "), false, "display", "image/jpeg"),
            (
                Some("image/jpeg; q = \"90\"oops"),
                false,
                "display",
                "image/jpeg",
            ),
            (
                Some("image/jpeg; q = \"bad\u{7f}\""),
                false,
                "display",
                "image/jpeg",
            ),
            (None, false, "display", "image/jpeg"),
        ] {
            let mut dto = asset(1, "IMAGE");
            dto["originalMimeType"] = json!(mime);
            dto["isEdited"] = json!(edited);
            dto["originalFileName"] = json!("A\u{0001}&B");
            let item = project(&fake.source, dto);
            assert_eq!(item.object.title, "A\u{fffd}&B");
            assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));

            assert_eq!(
                item.capture.unwrap().to_rfc3339(),
                "2023-12-31T22:30:00+00:00"
            );

            assert_eq!(item.object.resources.len(), 2);
            assert!(item.object.resources[0].uri.ends_with(representation));
            assert_eq!(item.object.resources[0].mime, expected);
            assert!(item.object.resources[1].uri.ends_with("preview"));

            assert!(
                item.object
                    .resources
                    .iter()
                    .all(|r| !r.byte_seek && r.duration.is_none())
            );
        }

        for (duration, expected) in [
            (0, Some("0:00:00.000")),
            (3_661_007, Some("1:01:01.007")),
            (i32::MAX as i64, Some("596:31:23.647")),
            (-1, None),
            (i32::MAX as i64 + 1, None),
        ] {
            let mut dto = asset(1, "VIDEO");
            dto["originalMimeType"] = Value::Null;
            dto["duration"] = json!(duration);
            dto["localDateTime"] = json!("2024-01-01T23:00:00-12:00");
            let item = project(&fake.source, dto);
            assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));
            assert_eq!(item.object.resources[0].duration.as_deref(), expected);
            assert_eq!(item.object.resources[0].mime, "application/octet-stream");
            assert!(item.object.resources[0].byte_seek);
            assert!(item.object.art.unwrap().ends_with("preview"));
        }

        let mut dto = asset(1, "IMAGE");
        dto["originalFileName"] = json!("");
        dto["fileCreatedAt"] = json!("2024-01-01T12:00:00");
        dto["localDateTime"] = json!("not a date");
        dto["checksum"] = json!("one");
        dto["updatedAt"] = json!("opaque hint");
        dto["thumbhash"] = json!("two");
        dto["exifInfo"] = json!(["ignored unsupported structure"]);
        let item = project(&fake.source, dto);
        assert_eq!(item.object.title, Uuid::from_u128(1).to_string());
        assert!(item.capture.is_none() && item.object.date.is_none());
        assert_eq!(item.checksum.as_deref(), Some("one"));
        assert_eq!(item.updated_at.as_deref(), Some("opaque hint"));
        assert_eq!(item.thumbhash.as_deref(), Some("two"));

        for invalid in [Value::Null, json!(123), json!(false), json!({"date": []})] {
            let mut dto = asset(1, "IMAGE");
            dto["fileCreatedAt"] = invalid.clone();
            dto["localDateTime"] = invalid.clone();
            let item = project(&fake.source, dto);
            assert!(item.capture.is_none() && item.object.date.is_none());
            let mut dto = album(ALBUM);
            dto["createdAt"] = invalid;
            let album: crate::immich::Album = serde_json::from_value(dto).unwrap();
            assert!(parse_date(album.created_at.as_deref(), &mut 0).is_none());
        }

        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn complete_pagination_and_scoped_encoded_intersection_without_probes() {
        let mut archived = asset(1001, "VIDEO");
        archived["visibility"] = json!("archive");
        archived["originalMimeType"] = json!("Video/QuickTime");
        let mut changed_encoded = archived.clone();
        changed_encoded["originalFileName"] = json!("changed between searches");

        let fake = SnapshotFixture::new(vec![
            version(),
            reply(json!([album(ALBUM)])),
            page((1..=1000).map(|id| asset(id, "IMAGE")).collect(), Some("2")),
            page(vec![archived], None),
            page(
                vec![
                    changed_encoded.clone(),
                    changed_encoded,
                    asset(9999, "VIDEO"),
                ],
                None,
            ),
            reply(json!([])),
        ])
        .await;

        let root = fake.source.root().await.unwrap();

        assert_eq!(
            root.albums[&ALBUM].object.date.as_deref(),
            Some("2023-12-31")
        );

        assert!(
            root.albums[&ALBUM]
                .object
                .art
                .as_ref()
                .unwrap()
                .contains(&Uuid::from_u128(777).to_string())
        );

        assert!(root.albums[&ALBUM].object.child_count.is_none());
        let contents = fake.source.contents(ALBUM).await.unwrap();
        assert_eq!(contents.items.len(), 1001);
        let video = &contents.items[&Uuid::from_u128(1001)].object;
        assert_eq!(video.resources.len(), 2);
        assert_eq!(video.resources[0].mime, "video/quicktime");
        assert_eq!(video.resources[1].mime, "video/mp4");
        assert!(video.resources.iter().all(|r| r.byte_seek));
        assert!(video.resources[1].duration.is_none());

        assert!(fake.source.clone().root().await.unwrap().albums.is_empty());

        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests.len(), 6);
        assert_eq!(requests[0].uri, "/prefix/api/server/version");
        assert_eq!(requests[1].uri, "/prefix/api/albums");
        assert_eq!(requests[5].uri, "/prefix/api/albums");

        for (index, expected_page) in [(2, 1), (3, 2), (4, 1)] {
            let request = &requests[index];
            assert_eq!(request.method, Method::POST);
            assert_eq!(request.uri, "/prefix/api/search/metadata");
            assert_eq!(request.body["albumIds"], json!([ALBUM]));
            assert_eq!(request.body["page"], json!(expected_page));
            assert_eq!(request.body["size"], json!(immich::SEARCH_PAGE_SIZE));
            assert_eq!(request.body["withDeleted"], false);
            assert_eq!(request.body["withExif"], false);
            assert_eq!(request.body["withPeople"], false);
            assert!(request.body.get("withStacked").is_none());

            assert_eq!(
                request.body.get("isEncoded"),
                (index == 4).then_some(&Value::Bool(true))
            );

            assert_eq!(
                request.body.get("type"),
                (index == 4).then_some(&json!("VIDEO"))
            );
        }

        assert!(
            requests
                .iter()
                .all(|request| request.headers["x-api-key"] == "private-test-key")
        );
    }

    #[tokio::test]
    async fn eligible_assets_require_original_file_name() {
        for null in [false, true] {
            let mut dto = asset(1, "IMAGE");

            if null {
                dto["originalFileName"] = Value::Null;
            } else {
                dto.as_object_mut().unwrap().remove("originalFileName");
            }

            let fake = SnapshotFixture::new(vec![page(vec![dto], None)]).await;

            assert_eq!(
                fake.source.contents(ALBUM).await.unwrap_err().to_string(),
                "eligible Immich asset is missing originalFileName"
            );
        }
    }

    #[tokio::test]
    async fn duplicates_compare_only_normalized_member_data_and_eligibility() {
        let first = asset(1, "IMAGE");

        for field in [
            "originalFileName",
            "checksum",
            "updatedAt",
            "thumbhash",
            "localDateTime",
            "fileCreatedAt",
            "isEdited",
            "visibility",
            "isTrashed",
            "type",
        ] {
            let mut second = first.clone();

            second[field] = match field {
                "isEdited" | "isTrashed" => json!(true),
                "visibility" => json!("hidden"),
                "type" => json!("VIDEO"),
                "localDateTime" | "fileCreatedAt" => json!("2025-01-01T00:00:00Z"),
                _ => json!("changed"),
            };

            for reversed in [false, true] {
                let records = if reversed {
                    vec![second.clone(), first.clone()]
                } else {
                    vec![first.clone(), second.clone()]
                };

                let fake = SnapshotFixture::new(vec![page(records, None)]).await;

                assert!(fake.source.contents(ALBUM).await.is_err(), "{field}");
            }
        }

        let mut irrelevant = first.clone();
        irrelevant["originalPath"] = json!("private ignored path");
        irrelevant["visibility"] = json!("archive");
        let mut excluded = asset(2, "VIDEO");
        excluded["visibility"] = json!("future-visibility");
        excluded.as_object_mut().unwrap().remove("originalFileName");
        let mut excluded_changed = excluded.clone();
        excluded_changed["originalFileName"] = json!("irrelevant name".repeat(1000));
        excluded_changed["checksum"] = json!("irrelevant hint".repeat(1000));
        excluded_changed["isEdited"] = json!(true);

        let fake = SnapshotFixture::new(vec![page(
            vec![first, irrelevant, excluded, excluded_changed],
            None,
        )])
        .await;

        let result = fake.source.contents(ALBUM).await.unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn root_duplicates_titles_covers_and_canonical_digests() {
        let mut empty = album(ALBUM);
        empty["albumName"] = json!("");
        empty["albumThumbnailAssetId"] = Value::Null;
        let other = album(Uuid::from_u128(2));

        let fake = SnapshotFixture::new(vec![
            version(),
            reply(json!([empty.clone(), other.clone(), empty.clone()])),
            reply(json!([other, empty.clone()])),
            reply(json!([empty.clone(), {"id": ALBUM, "albumName": "changed"}])),
            reply(json!([{"id": ALBUM}])),
            reply(json!([{"id": ALBUM, "albumName": "valid", "albumThumbnailAssetId": "invalid"}])),
        ])
        .await;

        let first = fake.source.root().await.unwrap();
        let second = fake.source.root().await.unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.albums[&ALBUM].object.title, ALBUM.to_string());
        assert!(first.albums[&ALBUM].object.art.is_none());

        for _ in 0..3 {
            assert!(fake.source.root().await.is_err());
        }

        let renamed = SnapshotFixture::new(vec![version(), reply(json!([empty.clone()]))]).await;
        let baseline = renamed.source.root().await.unwrap();
        let mut changed = SnapshotFixture::new(vec![version(), reply(json!([empty]))]).await;
        changed.source.friendly_name = "Another title".into();
        let changed = changed.source.root().await.unwrap();
        assert_ne!(baseline.digest, changed.digest);
        assert_eq!(
            baseline.albums[&ALBUM].digest,
            changed.albums[&ALBUM].digest
        );
    }

    #[tokio::test]
    async fn projection_hashes_hints_capture_resource_order_and_exact_bytes() {
        let mut first = asset(2, "IMAGE");
        first["originalFileName"] = json!("Zażółć \"photo\"\\name\n.jpg");
        let second = asset(1, "IMAGE");

        let fake = SnapshotFixture::new(vec![
            page(vec![first.clone(), second.clone()], None),
            page(vec![second.clone(), first.clone(), second], None),
        ])
        .await;

        let baseline = fake.source.contents(ALBUM).await.unwrap();
        let reordered = fake.source.contents(ALBUM).await.unwrap();
        assert_eq!(baseline.digest, reordered.digest);
        assert_eq!(baseline.bytes, reordered.bytes);
        let serialized = serde_json::to_vec(&baseline.items.values().collect::<Vec<_>>()).unwrap();
        assert_eq!(baseline.bytes, serialized.len());
        assert_eq!(
            baseline.digest,
            format!("{:x}", Sha256::digest(&serialized))
        );
        assert_eq!(baseline.digest.len(), 64);
        let item = baseline.items.values().next().unwrap();
        let mut original = Projection::new(SNAPSHOT_BYTES);
        original.json(item).unwrap();
        let original = original.finish().0;

        for field in [
            "checksum",
            "updatedAt",
            "thumbhash",
            "fileCreatedAt",
            "isEdited",
        ] {
            let mut changed = asset(1, "IMAGE");

            changed[field] = match field {
                "fileCreatedAt" => json!("2024-01-01T00:30:01+02:00"),
                "isEdited" => json!(true),
                _ => json!("changed"),
            };

            let changed = project(&fake.source, changed);
            let mut digest = Projection::new(SNAPSHOT_BYTES);
            digest.json(&changed).unwrap();
            assert_ne!(original, digest.finish().0, "{field}");
        }

        let mut reversed = item.clone();
        reversed.object.resources.reverse();
        let mut digest = Projection::new(SNAPSHOT_BYTES);
        digest.json(&reversed).unwrap();
        assert_ne!(original, digest.finish().0);

        for item in baseline.items.values() {
            let size = serde_json::to_vec(item).unwrap().len();
            assert_eq!(encoded_size(item, SNAPSHOT_BYTES).unwrap(), size);
            assert_eq!(encoded_size(item, size).unwrap(), size);
            assert!(encoded_size(item, size - 1).is_err());
        }
    }

    #[tokio::test]
    async fn traversal_progress_and_combined_page_budget() {
        for repeated in [false, true] {
            let mut replies = vec![page(vec![asset(1, "IMAGE")], Some("2"))];

            replies.push(page(
                if repeated {
                    vec![asset(1, "IMAGE")]
                } else {
                    vec![]
                },
                Some("3"),
            ));

            let fake = SnapshotFixture::new(replies).await;
            assert!(fake.source.contents(ALBUM).await.is_err());
            assert_eq!(fake.requests.lock().unwrap().len(), 2);
        }

        for finish in [false, true] {
            let mut replies = vec![page(vec![asset(1, "VIDEO")], None)];

            for number in 1..SEARCH_PAGES {
                let next = (number + 1).to_string();

                replies.push(page(
                    vec![asset(number as u128, "VIDEO")],
                    if finish && number == SEARCH_PAGES - 1 {
                        None
                    } else {
                        Some(&next)
                    },
                ));
            }

            let fake = SnapshotFixture::new(replies).await;
            assert_eq!(fake.source.contents(ALBUM).await.is_ok(), finish);
            assert_eq!(fake.requests.lock().unwrap().len(), SEARCH_PAGES);
        }
    }

    #[tokio::test]
    async fn raw_record_item_album_and_projected_payload_limits() {
        let excluded = json!({"id": Uuid::from_u128(1), "type": "AUDIO", "visibility": "hidden", "isTrashed": false, "isEdited": false});

        // Raw records count even when identical and excluded; pages need not be full.
        let fake = SnapshotFixture::new(vec![page(vec![excluded; SEARCH_RECORDS + 1], None)]).await;

        assert!(
            fake.source
                .contents(ALBUM)
                .await
                .unwrap_err()
                .to_string()
                .contains("record limit")
        );

        for extra in [0, 1] {
            let encoded = json!({"id": Uuid::from_u128(1), "type": "VIDEO", "visibility": "timeline", "isTrashed": false, "isEdited": false});

            let fake = SnapshotFixture::new(vec![
                page(vec![asset(1, "VIDEO")], None),
                page(vec![encoded; SEARCH_RECORDS - 1 + extra], None),
            ])
            .await;

            assert_eq!(fake.source.contents(ALBUM).await.is_ok(), extra == 0);

            assert_eq!(fake.requests.lock().unwrap().len(), 2);
        }

        let mut replies = Vec::new();

        for page_number in 0..=ALBUM_ITEMS / immich::SEARCH_PAGE_SIZE {
            let start = page_number * immich::SEARCH_PAGE_SIZE + 1;

            let count = if start > ALBUM_ITEMS {
                1
            } else {
                immich::SEARCH_PAGE_SIZE
            };

            let next = (page_number + 2).to_string();

            replies.push(page(
                (start..start + count)
                    .map(|id| asset(id as u128, "IMAGE"))
                    .collect(),
                (count != 1).then_some(next.as_str()),
            ));
        }

        let fake = SnapshotFixture::new(replies).await;

        let error = fake.source.contents(ALBUM).await.unwrap_err().to_string();

        assert!(error.contains("item limit"), "{error}");

        let fake = SnapshotFixture::new(vec![
            version(),
            reply(json!(
                (0..=MAX_ALBUMS)
                    .map(|id| album(Uuid::from_u128(id as u128)))
                    .collect::<Vec<_>>()
            )),
        ])
        .await;

        assert!(
            fake.source
                .root()
                .await
                .unwrap_err()
                .to_string()
                .contains("album limit")
        );

        let mut big = asset(1, "IMAGE");
        big["checksum"] = json!("x".repeat(SNAPSHOT_BYTES / 2));
        let mut bigger = big.clone();
        bigger["id"] = json!(Uuid::from_u128(2));

        let fake =
            SnapshotFixture::new(vec![page(vec![big], Some("2")), page(vec![bigger], None)]).await;

        assert!(
            fake.source
                .contents(ALBUM)
                .await
                .unwrap_err()
                .to_string()
                .contains("byte limit")
        );
    }

    #[test]
    fn mime_normalization_accepts_parameter_whitespace_but_rejects_garbage() {
        for value in [
            " IMAGE/JPEG ; q=90",
            "image/jpeg; q =90",
            "image/jpeg; q= 90",
            "IMAGE/JPEG;; q \t=\t \"90\"; ; x = y;",
        ] {
            assert_eq!(
                media_type(value).as_deref(),
                Some("image/jpeg"),
                "{value:?}"
            );
        }

        for value in [
            "image/jpeg; q = ",
            "image/jpeg; q = \"90\"oops",
            "image/jpeg; q\n=90",
            "image/jpeg; q=\n90",
            "image/jpeg; q = \"bad\u{7f}\"",
        ] {
            assert_eq!(media_type(value), None, "{value:?}");
        }
    }
}

#[cfg(test)]
mod revision_tests {
    use super::*;
    use std::{
        os::fd::AsRawFd,
        os::unix::{
            fs::{PermissionsExt, symlink},
            net::UnixStream,
            process::CommandExt,
        },
        process::{Child, Command, ExitStatus, Stdio},
        thread,
        time::{Duration, Instant},
    };
    use tempfile::TempDir;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Stage {
        Write,
        FileSync,
        Rename,
        DirectorySync,
    }

    #[derive(Clone, Copy)]
    pub(super) enum Fault {
        Error(Stage),
        SensitiveError(Stage),
        Crash(Stage),
        Hang,
        Panic,
        LateObservation,
    }

    impl StoreInner {
        pub(super) fn inject(&self, stage: Stage) -> anyhow::Result<()> {
            match self.fault {
                Some(Fault::Error(at)) if at == stage => {
                    return Err(io::Error::from_raw_os_error(libc::EIO))
                        .context("/private/state/api-key=secret ledger=private");
                }

                Some(Fault::SensitiveError(at)) if at == stage => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "/private/state/api-key=secret ledger=private",
                    ))
                    .context("private error context");
                }

                Some(Fault::Crash(at)) if at == stage => std::process::exit(42),

                Some(Fault::Hang) if stage == Stage::Write => loop {
                    thread::park();
                },

                Some(Fault::Panic) if stage == Stage::Write => panic!("injected worker failure"),

                _ => {}
            }

            Ok(())
        }
    }

    fn id(number: u128) -> Uuid {
        Uuid::from_u128(number)
    }

    fn digest(number: u32) -> String {
        format!("{number:064x}")
    }

    fn empty() -> Ledger {
        Ledger {
            server_uuid: id(1),
            system_update_id: 0,
            root_digest: None,
            albums: BTreeMap::new(),
        }
    }

    fn populated() -> Ledger {
        empty()
            .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
            .unwrap()
            .unwrap()
    }

    fn private_directory() -> TempDir {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();

        directory
    }

    fn put(directory: &Path, name: &str, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(directory.join(name))
            .unwrap();

        file.write_all(bytes).unwrap();
    }

    fn install(directory: &Path, ledger: &Ledger) {
        put(
            directory,
            "revisions.json",
            &serde_json::to_vec(ledger).unwrap(),
        );
    }

    #[test]
    fn first_installation_creates_no_files_and_restart_wraps_history() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(ledger, empty());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        ledger.restart();
        assert_eq!(ledger.system_update_id, 1);
        drop(store);
        assert_eq!(Store::open(directory.path(), id(1)).unwrap().1, empty());
        let mut ledger = populated();
        ledger.system_update_id = u32::MAX;
        ledger.albums.get_mut(&id(2)).unwrap().update_id = u32::MAX;
        ledger.albums.get_mut(&id(2)).unwrap().present = false;
        ledger.albums.get_mut(&id(2)).unwrap().contents_digest = Some(digest(3));
        let before = ledger.clone();
        ledger.restart();
        assert_eq!(ledger.system_update_id, 0);
        assert_eq!(ledger.albums[&id(2)].update_id, 0);
        assert_eq!(ledger.root_digest, before.root_digest);
        assert_eq!(ledger.albums[&id(2)].contents_digest, Some(digest(3)));
        assert_eq!(ledger.albums[&id(2)].metadata_digest, digest(2));
        assert!(!ledger.albums[&id(2)].present);
    }

    #[test]
    fn concurrent_first_installation_has_one_owner_without_creating_files() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let start = std::sync::Barrier::new(2);

        let (first, second) = thread::scope(|scope| {
            let first = scope.spawn(|| {
                start.wait();

                Store::open(directory.path(), id(1))
            });

            let second = scope.spawn(|| {
                start.wait();

                Store::open(directory.path(), id(1))
            });

            (first.join().unwrap(), second.join().unwrap())
        });

        let (store, mut ledger) = match (first, second) {
            (Ok(owner), Err(error)) | (Err(error), Ok(owner)) => {
                assert!(matches!(
                    error.downcast_ref::<std::fs::TryLockError>(),
                    Some(std::fs::TryLockError::WouldBlock)
                ));

                owner
            }

            _ => panic!("exactly one first start must acquire the directory"),
        };

        assert_eq!(ledger, empty());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        ledger.restart();
        store.inner.write(ledger.clone()).unwrap();
        drop(store);
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, ledger);
    }

    #[test]
    fn root_transitions_preserve_history_and_increment_each_affected_album_once() {
        let root = digest(1);
        let albums = BTreeMap::from([(id(2), digest(2))]);
        let ledger = populated();
        assert_eq!(ledger.system_update_id, 1);
        assert_eq!(ledger.albums[&id(2)].update_id, 0);
        assert_eq!(ledger.root_transition(&root, &albums).unwrap(), None);

        let projection = ledger
            .root_transition(&digest(9), &albums)
            .unwrap()
            .unwrap();

        assert_eq!(projection.system_update_id, 2);
        assert_eq!(projection.albums, ledger.albums);

        let ledger = ledger
            .contents_transition(id(2), &digest(3))
            .unwrap()
            .unwrap();

        let removed = ledger
            .root_transition(&root, &BTreeMap::new())
            .unwrap()
            .unwrap();

        assert_eq!(removed.system_update_id, ledger.system_update_id + 1);
        assert_eq!(removed.albums[&id(2)].update_id, 2);
        assert!(!removed.albums[&id(2)].present);
        assert_eq!(removed.albums[&id(2)].metadata_digest, digest(2));
        assert_eq!(removed.albums[&id(2)].contents_digest, Some(digest(3)));

        assert_eq!(
            removed.root_transition(&root, &BTreeMap::new()).unwrap(),
            None
        );

        assert!(removed.contents_transition(id(2), &digest(4)).is_err());
        assert!(removed.contents_transition(id(100), &digest(4)).is_err());

        let reappeared = removed
            .root_transition(&root, &BTreeMap::from([(id(2), digest(4))]))
            .unwrap()
            .unwrap();

        assert_eq!(reappeared.albums[&id(2)].update_id, 3);
        assert_eq!(reappeared.system_update_id, removed.system_update_id + 1);
        assert!(reappeared.albums[&id(2)].present);
        assert_eq!(reappeared.albums[&id(2)].metadata_digest, digest(4));
        assert_eq!(reappeared.albums[&id(2)].contents_digest, Some(digest(3)));

        assert_eq!(
            reappeared.contents_transition(id(2), &digest(3)).unwrap(),
            None
        );

        let renamed = reappeared
            .root_transition(&root, &BTreeMap::from([(id(2), digest(5))]))
            .unwrap()
            .unwrap();

        assert_eq!(renamed.albums[&id(2)].update_id, 4);
        assert_eq!(renamed.system_update_id, reappeared.system_update_id + 1);
    }

    #[test]
    fn root_and_contents_counters_wrap_and_unknown_empty_root_advances() {
        let initial = empty()
            .root_transition(&digest(0), &BTreeMap::new())
            .unwrap()
            .unwrap();

        assert_eq!(initial.system_update_id, 1);
        assert!(initial.albums.is_empty());
        let mut ledger = populated();
        ledger.system_update_id = u32::MAX;
        ledger.albums.get_mut(&id(2)).unwrap().update_id = u32::MAX;

        let contents = ledger
            .contents_transition(id(2), &digest(1))
            .unwrap()
            .unwrap();

        assert_eq!(contents.system_update_id, 0);
        assert_eq!(contents.albums[&id(2)].update_id, 0);

        let removed = ledger
            .root_transition(&digest(2), &BTreeMap::new())
            .unwrap()
            .unwrap();

        assert_eq!(removed.system_update_id, 0);
        assert_eq!(removed.albums[&id(2)].update_id, 0);
    }

    #[test]
    fn replacement_at_current_capacity_retains_history_and_exhaustion_is_recoverable() {
        let mut albums: BTreeMap<_, _> = (1..=MAX_ALBUMS)
            .map(|number| (id(number as u128), digest(1)))
            .collect();

        let ledger = empty()
            .root_transition(&digest(1), &albums)
            .unwrap()
            .unwrap();

        albums.remove(&id(1));
        albums.insert(id(MAX_ALBUMS as u128 + 1), digest(1));

        let replacement = ledger
            .root_transition(&digest(2), &albums)
            .unwrap()
            .unwrap();

        assert_eq!(replacement.albums.len(), MAX_ALBUMS + 1);
        assert!(!replacement.albums[&id(1)].present);
        assert_eq!(replacement.albums[&id(1)].update_id, 1);

        assert_eq!(replacement.albums[&id(MAX_ALBUMS as u128 + 1)].update_id, 0);

        albums.insert(id(MAX_ALBUMS as u128 + 2), digest(1));
        assert!(replacement.root_transition(&digest(3), &albums).is_err());
        assert_eq!(replacement.albums.len(), MAX_ALBUMS + 1);
        let mut full = populated();

        for number in 1..=RETAINED_ALBUMS {
            full.albums
                .entry(id(number as u128))
                .or_insert(AlbumRevision {
                    update_id: 42,
                    present: false,
                    metadata_digest: digest(1),
                    contents_digest: Some(digest(2)),
                });
        }

        full.validate(id(1)).unwrap();
        let before = full.clone();
        let new_album = BTreeMap::from([(id(RETAINED_ALBUMS as u128 + 1), digest(1))]);
        let error = full.root_transition(&digest(2), &new_album).unwrap_err();
        assert!(error.to_string().contains("new server UUID"));
        assert_eq!(full, before);
        let directory = private_directory();
        install(directory.path(), &full);
        let (store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert!(loaded.root_transition(&digest(2), &new_album).is_err());

        assert!(
            loaded
                .contents_transition(id(2), &digest(8))
                .unwrap()
                .is_some()
        );

        assert!(
            loaded
                .root_transition(&digest(3), &BTreeMap::new())
                .unwrap()
                .is_some()
        );

        drop(store);
        let recovered_directory = private_directory();
        let (_store, recovered) = Store::open(recovered_directory.path(), id(99)).unwrap();

        let recovered = recovered
            .root_transition(&digest(1), &new_album)
            .unwrap()
            .unwrap();

        assert_eq!(recovered.server_uuid, id(99));
        assert_eq!(recovered.albums.len(), 1);
    }

    #[tokio::test]
    async fn first_contents_survive_reload_and_unchanged_transitions_do_not_write() {
        let directory = private_directory();
        let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
        ledger.restart();
        store.persist(ledger.clone()).await;

        let ledger = ledger
            .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
            .unwrap()
            .unwrap();

        store.persist(ledger.clone()).await;

        let ledger = ledger
            .contents_transition(id(2), &digest(3))
            .unwrap()
            .unwrap();

        assert_eq!(ledger.system_update_id, 3);
        assert_eq!(ledger.albums[&id(2)].update_id, 1);
        store.persist(ledger.clone()).await;
        let metadata = fs::metadata(directory.path().join("revisions.json")).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert!(!directory.path().join("revisions.tmp").exists());
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        assert_eq!(Arc::strong_count(&store.inner), 1);
        let ownership = Arc::downgrade(&store.inner);
        drop(store);
        assert!(ownership.upgrade().is_none());
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, ledger);
        assert_eq!(loaded.contents_transition(id(2), &digest(3)).unwrap(), None);

        assert_eq!(
            loaded
                .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
                .unwrap(),
            None
        );

        let unchanged_metadata = fs::metadata(directory.path().join("revisions.json")).unwrap();
        assert_eq!(unchanged_metadata.ino(), metadata.ino());

        assert_eq!(
            unchanged_metadata.modified().unwrap(),
            metadata.modified().unwrap()
        );

        let changed = loaded
            .contents_transition(id(2), &digest(4))
            .unwrap()
            .unwrap();

        assert_eq!(changed.system_update_id, 4);
        assert_eq!(changed.albums[&id(2)].update_id, 2);
    }

    #[test]
    fn fork_retains_lock_after_worker_and_store_drop_until_exec() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
        ledger.restart();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(store.persist(ledger.clone()));
        assert_eq!(Arc::strong_count(&store.inner), 1);
        let ownership = Arc::downgrade(&store.inner);

        // SAFETY: the store owns a live descriptor; F_GETFD only reads its flags.
        let flags = unsafe { libc::fcntl(store.inner.directory.as_raw_fd(), libc::F_GETFD) };

        assert_ne!(flags, -1);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
        let (mut parent_signal, child_signal) = UnixStream::pair().unwrap();

        parent_signal
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        parent_signal
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let signal_fd = child_signal.as_raw_fd();
        let child_directory = directory.path().to_owned();

        let test_name = format!(
            "{}::subprocess_worker",
            module_path!().split_once("::").unwrap().1
        );

        let spawn = thread::spawn(move || {
            let mut command = Command::new(std::env::current_exe().unwrap());

            command
                .args(["--exact", &test_name])
                .env("REVISION_TEST_DIRECTORY", child_directory)
                .env("REVISION_TEST_MODE", "noop")
                .stdout(Stdio::null())
                .stderr(Stdio::null());

            // SAFETY: pre_exec uses only async-signal-safe syscalls and stack
            // storage. The socket stays live until spawn returns. The handshake
            // holds the forked child before exec without timing-based sleeps.
            unsafe {
                command.pre_exec(move || {
                    if libc::write(signal_fd, b"x".as_ptr().cast(), 1) != 1 {
                        return Err(io::Error::last_os_error());
                    }

                    let mut poll = libc::pollfd {
                        fd: signal_fd,
                        events: libc::POLLIN,
                        revents: 0,
                    };

                    if libc::poll(&mut poll, 1, 5_000) != 1 {
                        return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
                    }

                    let mut release = 0_u8;

                    if libc::read(signal_fd, (&mut release as *mut u8).cast(), 1) != 1 {
                        return Err(io::Error::last_os_error());
                    }

                    Ok(())
                });
            }

            let child = command.spawn();
            drop(child_signal);

            child.unwrap()
        });

        parent_signal.read_exact(&mut [0]).unwrap();
        drop(store);
        let ownership_released = ownership.upgrade().is_none();
        let before_exec = Store::open(directory.path(), id(1));
        parent_signal.write_all(b"x").unwrap();
        let mut child = spawn.join().unwrap();
        let after_exec = Store::open(directory.path(), id(1));
        let status = wait(&mut child, Duration::from_secs(5));
        assert!(ownership_released);

        assert!(matches!(
            before_exec
                .err()
                .unwrap()
                .downcast_ref::<std::fs::TryLockError>(),
            Some(std::fs::TryLockError::WouldBlock)
        ));

        assert_eq!(after_exec.unwrap().1, ledger);
        assert!(status.success());
    }

    #[test]
    fn process_lock_is_exclusive_and_retained_by_clones() {
        let directory = private_directory();
        install(directory.path(), &empty());
        let (store, _) = Store::open(directory.path(), id(1)).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        let clone = store.clone();
        drop(store);
        assert!(Store::open(directory.path(), id(1)).is_err());
        let mut child = child(directory.path(), "lock");
        assert!(wait(&mut child, Duration::from_secs(5)).success());
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        drop(clone);
        assert!(Store::open(directory.path(), id(1)).is_ok());
    }

    #[test]
    fn directory_lock_precedes_loading_and_temporary_cleanup() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let pinned = File::open(directory.path()).unwrap();
        pinned.try_lock().unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        install(directory.path(), &populated());
        put(directory.path(), "revisions.lock", b"ancillary");
        put(directory.path(), "revisions.tmp", b"uncommitted");

        let state_inode = fs::metadata(directory.path().join("revisions.json"))
            .unwrap()
            .ino();

        let error = Store::open(directory.path(), id(1)).err().unwrap();

        assert!(matches!(
            error.downcast_ref::<std::fs::TryLockError>(),
            Some(std::fs::TryLockError::WouldBlock)
        ));

        assert_eq!(
            fs::read(directory.path().join("revisions.tmp")).unwrap(),
            b"uncommitted"
        );

        assert_eq!(
            fs::metadata(directory.path().join("revisions.json"))
                .unwrap()
                .ino(),
            state_inode
        );

        drop(pinned);
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, populated());
        assert!(!directory.path().join("revisions.tmp").exists());

        assert!(matches!(
            File::open(directory.path()).unwrap().try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));

        assert_eq!(
            fs::read(directory.path().join("revisions.lock")).unwrap(),
            b"ancillary"
        );
    }

    #[test]
    fn missing_state_with_any_evidence_fails_and_temporary_is_never_promoted() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();

        for evidence in ["revisions.lock", "revisions.tmp", "unrelated"] {
            let directory = private_directory();

            put(
                directory.path(),
                evidence,
                &serde_json::to_vec(&populated()).unwrap(),
            );

            assert!(Store::open(directory.path(), id(1)).is_err(), "{evidence}");
            assert!(!directory.path().join("revisions.json").exists());
            assert!(directory.path().join(evidence).exists());
        }

        let directory = private_directory();
        install(directory.path(), &populated());
        put(directory.path(), "revisions.tmp", b"partial");
        assert!(Store::open(directory.path(), id(9)).is_err());
        assert!(directory.path().join("revisions.tmp").exists());
        put(directory.path(), "revisions.json", b"{");
        assert!(Store::open(directory.path(), id(1)).is_err());
        assert!(directory.path().join("revisions.tmp").exists());
        install(directory.path(), &populated());
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, populated());
        assert!(!directory.path().join("revisions.tmp").exists());
    }

    #[test]
    fn null_root_with_retained_albums_rejects_load_without_removing_tmp() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();

        for present in [true, false] {
            let directory = private_directory();
            let mut ledger = populated();
            ledger.system_update_id = 0;
            ledger.albums.get_mut(&id(2)).unwrap().present = present;
            ledger.root_digest = None;
            install(directory.path(), &ledger);
            put(directory.path(), "revisions.tmp", b"uncommitted");
            assert!(Store::open(directory.path(), id(1)).is_err());

            assert_eq!(
                fs::read(directory.path().join("revisions.tmp")).unwrap(),
                b"uncommitted"
            );

            ledger.root_digest = Some(digest(1));
            install(directory.path(), &ledger);
            let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
            assert_eq!(loaded, ledger);
            assert_eq!(loaded.system_update_id, 0);
            assert_eq!(loaded.albums[&id(2)].update_id, 0);
            assert!(!directory.path().join("revisions.tmp").exists());
        }
    }

    #[test]
    fn load_rejects_corruption_unknown_missing_fields_and_duplicate_uuid_aliases() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let valid = serde_json::to_string(&populated()).unwrap();
        let album = serde_json::to_string(&populated().albums[&id(2)]).unwrap();
        let uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

        let duplicates = format!(
            "{{\"server_uuid\":\"{}\",\"system_update_id\":0,\"root_digest\":\"{}\",\"albums\":{{\"{uuid}\":{album},\"{}\":{album}}}}}",
            id(1),
            digest(1),
            uuid.to_uppercase()
        );

        for key in [uuid.to_owned(), uuid.to_uppercase()] {
            let single = duplicates.replace(&format!(",\"{}\":{album}", uuid.to_uppercase()), "");
            let single = single.replace(uuid, &key);
            let directory = private_directory();
            put(directory.path(), "revisions.json", single.as_bytes());
            let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
            assert_eq!(loaded.root_digest, Some(digest(1)));
            assert_eq!(loaded.albums.len(), 1);

            assert_eq!(
                loaded.albums[&Uuid::parse_str(uuid).unwrap()],
                populated().albums[&id(2)]
            );
        }

        let invalid = [
            "{".to_owned(),
            format!("{valid} trailing"),
            valid.replacen('{', "{\"extra\":0,", 1),
            valid.replace("\"present\":true", "\"present\":true,\"title\":\"x\""),
            valid.replace(
                "\"contents_digest\":null",
                "\"contents_digest\":null,\"present\":false",
            ),
            valid.replace("\"contents_digest\":null", "\"contents_digest\":\"BAD\""),
            valid.replace(&digest(2), &"A".repeat(64)),
            valid.replace(&digest(2), &"g".repeat(64)),
            valid.replace(&digest(2), &"a".repeat(63)),
            valid.replace("\"system_update_id\":1", "\"system_update_id\":4294967296"),
            valid.replace("\"system_update_id\":1", "\"system_update_id\":-1"),
            valid.replace(",\"contents_digest\":null", ""),
            valid.replace(&format!("\"root_digest\":\"{}\",", digest(1)), ""),
            valid.replace(&id(2).to_string(), "not-a-uuid"),
            duplicates.clone(),
            duplicates.replace(&uuid.to_uppercase(), uuid),
        ];

        for json in invalid {
            let directory = private_directory();
            put(directory.path(), "revisions.json", json.as_bytes());

            assert!(
                Store::open(directory.path(), id(1)).is_err(),
                "accepted {json}"
            );
        }

        let directory = private_directory();
        install(directory.path(), &populated());
        assert!(Store::open(directory.path(), id(99)).is_err());
        assert!(Store::open(directory.path(), Uuid::nil()).is_err());
    }

    #[test]
    fn explicit_load_serialization_and_identity_bounds() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        install(directory.path(), &empty());

        let file = OpenOptions::new()
            .write(true)
            .open(directory.path().join("revisions.json"))
            .unwrap();

        file.set_len(REVISION_BYTES as u64 + 1).unwrap();

        assert!(
            Store::open(directory.path(), id(1))
                .err()
                .unwrap()
                .to_string()
                .contains("8 MiB")
        );

        let mut bytes = serde_json::to_vec(&empty()).unwrap();
        bytes.resize(REVISION_BYTES, b' ');
        put(directory.path(), "revisions.json", &bytes);
        assert!(Store::open(directory.path(), id(1)).is_ok());
        let mut bounded = BoundedBytes(Vec::new());
        bounded.write_all(&bytes).unwrap();
        assert!(bounded.write_all(b" ").is_err());
        assert_eq!(bounded.0.len(), REVISION_BYTES);
        let mut ledger = empty();
        ledger.root_digest = Some(digest(1));

        for number in 1..=RETAINED_ALBUMS + 1 {
            ledger.albums.insert(
                id(number as u128),
                AlbumRevision {
                    update_id: 0,
                    present: number <= MAX_ALBUMS,
                    metadata_digest: digest(1),
                    contents_digest: Some(digest(2)),
                },
            );
        }

        install(directory.path(), &ledger);
        assert!(Store::open(directory.path(), id(1)).is_err());
        ledger.albums.remove(&id(RETAINED_ALBUMS as u128 + 1));

        ledger
            .albums
            .get_mut(&id(MAX_ALBUMS as u128 + 1))
            .unwrap()
            .present = true;

        install(directory.path(), &ledger);
        assert!(Store::open(directory.path(), id(1)).is_err());

        ledger
            .albums
            .get_mut(&id(MAX_ALBUMS as u128 + 1))
            .unwrap()
            .present = false;

        install(directory.path(), &ledger);
        let (store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, ledger);
        store.inner.write(ledger.clone()).unwrap();
        assert!(!directory.path().join("revisions.tmp").exists());
        assert!(serde_json::to_vec(&ledger).unwrap().len() < REVISION_BYTES);
    }

    #[test]
    fn dynamic_user_directory_symlink_locks_persists_and_reloads_private_target() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let layout = private_directory();
        fs::set_permissions(layout.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let private = layout.path().join("private");
        let actual = private.join("unit");
        let configured = layout.path().join("unit");
        fs::create_dir(&private).unwrap();
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&actual).unwrap();
        fs::set_permissions(&actual, fs::Permissions::from_mode(0o700)).unwrap();
        symlink("private/unit", &configured).unwrap();
        let symlink_inode = fs::symlink_metadata(&configured).unwrap().ino();
        let (store, mut ledger) = Store::open(&configured, id(1)).unwrap();
        assert_eq!(ledger, empty());
        assert_eq!(fs::read_dir(&actual).unwrap().count(), 0);
        assert!(Store::open(&actual, id(1)).is_err());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        ledger.restart();
        runtime.block_on(store.persist(ledger.clone()));
        let first_inode = fs::metadata(actual.join("revisions.json")).unwrap().ino();

        let ledger = ledger
            .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
            .unwrap()
            .unwrap();

        runtime.block_on(store.persist(ledger.clone()));
        let committed = fs::metadata(actual.join("revisions.json")).unwrap();
        assert_ne!(committed.ino(), first_inode);
        assert_eq!(committed.mode() & 0o777, 0o600);
        assert!(!actual.join("revisions.tmp").exists());

        assert_eq!(
            fs::read(actual.join("revisions.json")).unwrap(),
            serde_json::to_vec(&ledger).unwrap()
        );

        assert_eq!(
            fs::read(configured.join("revisions.json")).unwrap(),
            fs::read(actual.join("revisions.json")).unwrap()
        );

        drop(store);
        let (store, loaded) = Store::open(&configured, id(1)).unwrap();
        assert_eq!(loaded, ledger);
        assert!(Store::open(&actual, id(1)).is_err());
        drop(store);
        let (_store, loaded) = Store::open(&actual, id(1)).unwrap();
        assert_eq!(loaded, ledger);
        let link = fs::symlink_metadata(&configured).unwrap();
        assert!(link.is_symlink());
        assert_eq!(link.ino(), symlink_inode);
        assert_eq!(
            fs::read_link(&configured).unwrap(),
            Path::new("private/unit")
        );
    }

    #[test]
    fn loss_of_only_revision_file_is_indistinguishable_from_first_installation() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
        ledger.restart();
        store.inner.write(ledger).unwrap();
        drop(store);
        fs::remove_file(directory.path().join("revisions.json")).unwrap();
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, empty());
    }

    #[test]
    fn rejects_nonprivate_symlink_nonregular_and_hardlinked_files_without_blocking() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        assert!(Store::open(&directory.path().join("absent"), id(1)).is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        let parent = private_directory();
        symlink(directory.path(), parent.path().join("link")).unwrap();
        assert!(Store::open(&parent.path().join("link"), id(1)).is_err());
        let directory = private_directory();
        let path = directory.path().join("revisions.json");
        let valid = serde_json::to_vec(&empty()).unwrap();
        put(directory.path(), "target", &valid);
        symlink(directory.path().join("target"), &path).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        assert_eq!(fs::read(directory.path().join("target")).unwrap(), valid);
        fs::remove_file(&path).unwrap();
        fs::hard_link(directory.path().join("target"), &path).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        fs::remove_dir(&path).unwrap();
        let name_c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();

        // SAFETY: the path is NUL-terminated and refers to this test's directory.
        assert_eq!(unsafe { libc::mkfifo(name_c.as_ptr(), 0o600) }, 0);
        let start = Instant::now();
        assert!(Store::open(directory.path(), id(1)).is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        fs::remove_file(&path).unwrap();
        install(directory.path(), &empty());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
    }

    fn child(directory: &Path, mode: &str) -> Child {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();

        let test_name = format!(
            "{}::subprocess_worker",
            module_path!().split_once("::").unwrap().1
        );

        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--nocapture"])
            .env("REVISION_TEST_DIRECTORY", directory)
            .env("REVISION_TEST_MODE", mode)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn wait(child: &mut Child, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;

        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }

            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("revision subprocess failed to terminate before watchdog deadline");
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn subprocess_worker() {
        let Some(directory) = std::env::var_os("REVISION_TEST_DIRECTORY") else {
            return;
        };

        let directory = Path::new(&directory);
        let mode = std::env::var("REVISION_TEST_MODE").unwrap();

        tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(io::stderr)
            .init();

        if mode == "noop" {
            return;
        }

        if mode == "lock" {
            assert!(Store::open(directory, id(1)).is_err());

            return;
        }

        let (mut store, mut ledger) = Store::open(directory, id(1)).unwrap();
        ledger.restart();

        let stage = match mode.rsplit('-').next().unwrap() {
            "write" => Stage::Write,
            "filesync" => Stage::FileSync,
            "rename" => Stage::Rename,
            "dirsync" => Stage::DirectorySync,
            _ => Stage::Write,
        };

        Arc::get_mut(&mut store.inner).unwrap().fault = match mode.as_str() {
            "hang" | "cancel" => Some(Fault::Hang),
            "panic" => Some(Fault::Panic),
            "late-ready" => Some(Fault::LateObservation),
            mode if mode.starts_with("error-") => Some(Fault::Error(stage)),
            mode if mode.starts_with("sensitive-") => Some(Fault::SensitiveError(stage)),
            mode if mode.starts_with("crash-") => Some(Fault::Crash(stage)),
            _ => None,
        };

        if mode == "invalid" {
            ledger = ledger
                .root_transition(
                    &"x".repeat(REVISION_BYTES + 1),
                    &BTreeMap::from([(id(2), digest(2))]),
                )
                .unwrap()
                .unwrap();
        }

        if mode == "temp-collision" {
            put(directory, "revisions.tmp", b"do not truncate");
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            if mode == "late-ready" {
                tokio::time::pause();
            }

            if mode == "cancel" {
                let task = tokio::spawn(async move { store.persist(ledger).await });
                tokio::time::sleep(Duration::from_millis(100)).await;
                task.abort();
                let _ = task.await;
            } else {
                store.persist(ledger).await;
            }
        });
    }

    #[test]
    fn io_failures_and_crashes_leave_only_old_or_new_committed_state_and_bounded_temp() {
        for action in ["error", "crash"] {
            for stage in ["write", "filesync", "rename", "dirsync"] {
                let directory = private_directory();
                let old = populated();
                install(directory.path(), &old);
                let mut child = child(directory.path(), &format!("{action}-{stage}"));
                let status = wait(&mut child, Duration::from_secs(5));
                assert_eq!(status.code(), Some(if action == "error" { 1 } else { 42 }));
                let mut expected = old.clone();

                if stage == "dirsync" {
                    expected.restart();
                }

                let committed: Ledger = serde_json::from_slice(
                    &fs::read(directory.path().join("revisions.json")).unwrap(),
                )
                .unwrap();

                assert_eq!(committed, expected);
                let temporary = directory.path().join("revisions.tmp");

                if stage != "dirsync" {
                    let metadata = fs::metadata(&temporary).unwrap();
                    assert!(metadata.len() <= REVISION_BYTES as u64);
                    assert_eq!(metadata.mode() & 0o777, 0o600);
                }

                let (store, recovered) = Store::open(directory.path(), id(1)).unwrap();
                assert_eq!(recovered, expected);
                assert!(!temporary.exists());
                drop(store);
            }
        }
    }

    #[test]
    fn persistence_diagnostics_exclude_error_payloads() {
        for (stage, operation) in [
            ("write", "write revision state"),
            ("filesync", "sync revision state"),
            ("rename", "replace revision state"),
            ("dirsync", "sync revision directory"),
        ] {
            for action in ["error", "sensitive"] {
                let directory = private_directory();
                let old = populated();
                install(directory.path(), &old);
                let mut child = child(directory.path(), &format!("{action}-{stage}"));
                assert_eq!(wait(&mut child, Duration::from_secs(5)).code(), Some(1));
                let mut diagnostic = String::new();

                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut diagnostic)
                    .unwrap();

                assert!(diagnostic.contains(operation), "{diagnostic}");

                let (kind, code) = if action == "error" {
                    (
                        io::Error::from_raw_os_error(libc::EIO).kind(),
                        Some(libc::EIO),
                    )
                } else {
                    (io::ErrorKind::PermissionDenied, None)
                };

                assert!(
                    diagnostic.contains(&format!("kind=Some({kind:?})")),
                    "{diagnostic}"
                );

                assert!(
                    diagnostic.contains(&format!("os_code={code:?}")),
                    "{diagnostic}"
                );

                for private in [
                    "private",
                    "api-key",
                    "secret",
                    "ledger=",
                    directory.path().to_str().unwrap(),
                    &old.server_uuid.to_string(),
                    &digest(1),
                ] {
                    assert!(!diagnostic.contains(private), "{diagnostic}");
                }
            }
        }
    }

    #[test]
    fn startup_crash_never_promotes_temp_or_resets_identity() {
        for stage in ["write", "filesync", "rename", "dirsync"] {
            let directory = private_directory();
            let mut child = child(directory.path(), &format!("crash-{stage}"));
            assert_eq!(wait(&mut child, Duration::from_secs(5)).code(), Some(42));

            if stage == "dirsync" {
                let (_store, ledger) = Store::open(directory.path(), id(1)).unwrap();
                assert_eq!(ledger.system_update_id, 1);
            } else {
                assert!(Store::open(directory.path(), id(1)).is_err());
                assert!(directory.path().join("revisions.tmp").exists());
                assert!(!directory.path().join("revisions.json").exists());
            }
        }
    }

    #[test]
    fn ready_worker_first_observed_after_deadline_fail_stops() {
        let directory = private_directory();
        let mut expected = populated();
        install(directory.path(), &expected);
        expected.restart();
        let mut child = child(directory.path(), "late-ready");
        let status = wait(&mut child, Duration::from_secs(5));

        assert_eq!(
            fs::read(directory.path().join("revisions.json")).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );

        assert!(!directory.path().join("revisions.tmp").exists());
        assert_eq!(status.code(), Some(1));
    }

    #[test]
    fn hung_worker_cancellation_panic_and_invalid_replacement_fail_stop() {
        for mode in ["hang", "cancel", "panic", "invalid", "temp-collision"] {
            let directory = private_directory();
            let old = populated();
            install(directory.path(), &old);
            let started = Instant::now();
            let mut child = child(directory.path(), mode);
            let status = wait(&mut child, Duration::from_secs(8));
            assert_eq!(status.code(), Some(1), "{mode}");

            if mode == "hang" {
                assert!(started.elapsed() >= PERSIST_TIMEOUT);
            }

            assert!(started.elapsed() < Duration::from_secs(8));

            assert_eq!(
                fs::read(directory.path().join("revisions.json")).unwrap(),
                serde_json::to_vec(&old).unwrap()
            );

            if mode == "invalid" {
                assert!(!directory.path().join("revisions.tmp").exists());
            }

            if mode == "temp-collision" {
                assert_eq!(
                    fs::read(directory.path().join("revisions.tmp")).unwrap(),
                    b"do not truncate"
                );
            }

            let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
            assert_eq!(loaded, old);
        }
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

            let library =
                Library::new(config, store, ledger, Subscriptions::new().unwrap()).unwrap();

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

    fn action(
        id: &str,
        metadata: bool,
        start: u32,
        count: u32,
        sort: Option<bool>,
    ) -> BrowseArguments {
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

    fn browse(
        library: &Library,
        query: BrowseArguments,
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
    async fn real_http_catalog_events_durability_and_media_share_one_server() {
        use crate::{media::MediaProxy, server::Server};
        use quick_xml::{Reader, events::Event};

        async fn soap(
            client: &reqwest::Client,
            base: &str,
            name: &str,
            args: &str,
        ) -> (u16, String) {
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
            !fault.contains("127.0.0.1")
                && !fault.contains("Edited")
                && !fault.contains("checksum")
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

        fixture.library.inner.state.lock().unwrap().cache.byte_limit =
            root.bytes + 2 * contents.bytes;

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
        for stage in 0..3 {
            for body_pending in [false, true] {
                let mut fixture = Fixture::new(1).await;
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let inner = Arc::get_mut(&mut fixture.library.inner).unwrap();
                inner.preparation_timeout = Duration::from_secs(10);

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
                tokio::time::advance(Duration::from_secs(if stage == 0 { 11 } else { 5 })).await;

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
    }

    #[tokio::test]
    async fn whole_permit_covers_commit_wait_and_preparation_timeout_cleans_flight() {
        let mut fixture = Fixture::new(1).await;

        Arc::get_mut(&mut fixture.library.inner)
            .unwrap()
            .preparation_timeout = Duration::from_millis(150);

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
}
