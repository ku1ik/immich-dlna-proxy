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
    immich::{self, Asset, Client},
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

struct Source {
    client: Client,
    http_address: SocketAddrV4,
    friendly_name: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
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

#[derive(Debug)]
struct Root {
    object: Object,
    albums: BTreeMap<Uuid, Album>,
    digest: String,
    bytes: usize,
}

#[derive(Debug)]
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

                let result = self.client.search_album(album, page, encoded).await?;

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
            .filter(|ms| *ms >= 0)
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
pub(crate) struct Ledger {
    server_uuid: Uuid,
    system_update_id: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    root_digest: Option<String>,
    #[serde(deserialize_with = "deserialize_albums")]
    albums: BTreeMap<Uuid, AlbumRevision>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AlbumRevision {
    update_id: u32,
    present: bool,
    metadata_digest: String,
    #[serde(deserialize_with = "Option::deserialize")]
    contents_digest: Option<String>,
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
    pub(crate) fn restart(&mut self) {
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
pub(crate) struct Store {
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
    pub(crate) fn open(directory: &Path, uuid: Uuid) -> anyhow::Result<(Self, Ledger)> {
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
                let metadata = file.metadata()?;
                check_private(&metadata, false)?;

                ensure!(
                    metadata.len() <= REVISION_BYTES as u64,
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
    pub(crate) async fn persist(&self, ledger: Ledger) -> Ledger {
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
            let result = inner.write(&ledger);
            drop(inner);

            result.map(|()| ledger)
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
            Ok(Ok(Ok(ledger))) if tokio::time::Instant::now() < deadline => {
                std::mem::forget(guard);

                ledger
            }

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
            Ok(Ok(Ok(_))) | Err(_) => fail_stop("revision commit exceeded five seconds"),
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
    fn write(&self, ledger: &Ledger) -> Result<(), PersistenceError> {
        let mut operation = "validate revision state";

        let result = (|| {
            ledger.validate(self.uuid)?;
            let mut bytes = BoundedBytes(Vec::new());
            operation = "serialize bounded revision state";
            serde_json::to_writer(&mut bytes, ledger)?;
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
pub(crate) struct Library {
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
        let result = std::mem::replace(&mut self.result, Err(FAILED));
        self.sender.send_replace(Some(result));
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
    pub(crate) fn new(
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
                #[cfg(test)]
                publication: Mutex::new(None),
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

                let deadline = now + PREPARATION_TIMEOUT;

                supervisor.tasks.spawn(async move {
                    flight.result =
                        flight
                            .library
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
            let next = match next {
                Some(next) => Some(self.inner.store.persist(next).await),
                None => None,
            };

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
                state.ledger = Arc::new(next);
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
mod snapshot_tests;

#[cfg(test)]
mod revision_tests;

#[cfg(test)]
mod tests;
