use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, ensure};
use serde::{Deserialize, Deserializer, Serialize, de};
use uuid::Uuid;

use super::MAX_ALBUMS;
#[cfg(test)]
use super::SPAWN_OR_REOPEN;
use super::digest::Digest;

const RETAINED_ALBUMS: usize = 16_384;
const REVISION_BYTES: usize = 8 * 1024 * 1024;
const PERSIST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Ledger {
    pub(super) server_uuid: Uuid,
    pub(super) system_update_id: u32,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(super) root_digest: Option<Digest>,
    #[serde(deserialize_with = "deserialize_albums")]
    pub(super) albums: BTreeMap<Uuid, AlbumRevision>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct AlbumRevision {
    pub(super) update_id: u32,
    pub(super) present: bool,
    pub(super) metadata_digest: Digest,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(super) contents_digest: Option<Digest>,
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

        Ok(())
    }

    /// Invalidate every retained counter once, without forgetting any digest.
    /// Persist this transition before binding listeners or announcing the device.
    pub(super) fn restart(&mut self) {
        self.system_update_id = self.system_update_id.wrapping_add(1);

        for album in self.albums.values_mut() {
            album.update_id = album.update_id.wrapping_add(1);
        }
    }

    /// `albums` is the complete present map with internally generated metadata digests.
    /// The ledger is validated on load and again before persistence.
    /// `None` means no revision write, even when a snapshot needs a cache refill.
    pub(super) fn root_transition(
        &self,
        root_digest: &Digest,
        albums: &BTreeMap<Uuid, Digest>,
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
        let mut changed = self.root_digest.as_ref() != Some(root_digest);

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

    pub(super) fn contents_transition(
        &self,
        id: Uuid,
        digest: &Digest,
    ) -> anyhow::Result<Option<Self>> {
        let album = self.albums.get(&id).filter(|album| album.present);
        let album = album.context("contents transition requires a present album")?;

        if album.contents_digest.as_ref() == Some(digest) {
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
pub(super) struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    directory: File,
    path: PathBuf,
    uuid: Uuid,
    #[cfg(test)]
    fault: Option<tests::Fault>,
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
    pub(super) fn open(directory: &Path, uuid: Uuid) -> anyhow::Result<(Self, Ledger)> {
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
    pub(super) async fn persist(&self, ledger: Ledger) -> Ledger {
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
        if matches!(self.inner.fault, Some(tests::Fault::LateObservation)) {
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
            self.inject(tests::Stage::Write)?;
            temporary.write_all(second)?;
            operation = "flush revision state";
            temporary.flush()?;
            operation = "sync revision state";
            #[cfg(test)]
            self.inject(tests::Stage::FileSync)?;
            temporary.sync_all()?;
            operation = "replace revision state";
            #[cfg(test)]
            self.inject(tests::Stage::Rename)?;

            fs::rename(
                self.path.join("revisions.tmp"),
                self.path.join("revisions.json"),
            )?;

            operation = "sync revision directory";
            #[cfg(test)]
            self.inject(tests::Stage::DirectorySync)?;
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

#[cfg(test)]
#[path = "revision_tests.rs"]
mod tests;
