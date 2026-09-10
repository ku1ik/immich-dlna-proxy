//! Durable revision history, independent of catalog payloads and freshness.
//!
//! The service must serialize candidate construction, persistence, publication and
//! notification-state updates as one non-cancelable commit. Construct transitions
//! from the latest published ledger after rechecking the prepared scope. A client
//! deadline must only stop waiting for that service-owned task, never cancel it.
//!
//! Complete directory loss is indistinguishable from a first installation. Restore
//! lost state or choose a new server UUID. To recover from exhausted history, stop
//! the service, archive its state directory, and use a new UUID and empty private
//! directory; history is never automatically pruned.

use std::{
    collections::BTreeMap,
    ffi::CStr,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    sync::Arc,
};

use anyhow::{Context, ensure};
use serde::{Deserialize, Deserializer, Serialize, de};
use uuid::Uuid;

use crate::limits::{COMMIT_TIMEOUT, CURRENT_ALBUMS, RETAINED_ALBUMS, REVISION_BYTES};

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
            self.albums.values().filter(|album| album.present).count() <= CURRENT_ALBUMS,
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

    /// `albums` is the complete present map with deterministic metadata digests.
    /// `None` means no revision write, even when a snapshot needs a cache refill.
    pub fn root_transition(
        &self,
        root_digest: &str,
        albums: &BTreeMap<Uuid, String>,
    ) -> anyhow::Result<Option<Self>> {
        self.validate(self.server_uuid)?;
        validate_digest(root_digest)?;

        ensure!(
            albums.len() <= CURRENT_ALBUMS,
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

        for digest in albums.values() {
            validate_digest(digest)?;
        }

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

    pub fn contents_transition(&self, id: Uuid, digest: &str) -> anyhow::Result<Option<Self>> {
        self.validate(self.server_uuid)?;
        validate_digest(digest)?;

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

/// Owns exclusive directory and file locks, also retained by every disk worker.
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    directory: File,
    _lock: File,
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
            "state directory must have mode 0700"
        );
    } else {
        ensure!(metadata.is_file(), "revision state must be a regular file");

        ensure!(
            metadata.mode() & 0o777 == 0o600,
            "revision files must have mode 0600"
        );

        ensure!(
            metadata.nlink() == 1,
            "revision files must not have hard links"
        );
    }

    Ok(())
}

fn open_file(directory: &File, name: &CStr, flags: i32) -> io::Result<File> {
    // SAFETY: directory is live and name is NUL-terminated; the returned fd is
    // uniquely owned. O_NONBLOCK prevents a FIFO from hanging before fstat.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
            0o600,
        )
    };

    if fd == -1 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: openat succeeded and transferred ownership of this fd.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn unlink_tmp(directory: &File) -> io::Result<()> {
    // SAFETY: directory is live and the filename is a static C string. This
    // unlinks a leftover symlink itself, never its target.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), c"revisions.tmp".as_ptr(), 0) };

    if result == -1 {
        let error = io::Error::last_os_error();

        if error.kind() != io::ErrorKind::NotFound {
            return Err(error);
        }
    }

    Ok(())
}

impl Store {
    /// Lock and load synchronously; never perform the startup revision write here.
    /// The existing directory must be private, writable, trusted local storage.
    /// The configured path may be a symlink, as with systemd DynamicUser; validate
    /// and pin its target. First-install inspection requires Linux procfs.
    pub fn open(directory: &Path, uuid: Uuid) -> anyhow::Result<(Self, Ledger)> {
        Self::open_inner(
            directory,
            uuid,
            #[cfg(test)]
            || {},
        )
    }

    fn open_inner(
        directory: &Path,
        uuid: Uuid,
        #[cfg(test)] after_lock_open: impl FnOnce(),
    ) -> anyhow::Result<(Self, Ledger)> {
        ensure!(!uuid.is_nil(), "server UUID must not be nil");

        let directory_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NONBLOCK)
            .open(directory)
            .context("open revision directory")?;

        check_private(&directory_file.metadata()?, true)?;

        // Serialize first-install ownership before creating any evidence of state.
        // Retain this flock on the pinned target for the store/worker lifetime.
        directory_file
            .try_lock()
            .context("revision directory is already locked or cannot be locked")?;

        let (lock, created) = match open_file(
            &directory_file,
            c"revisions.lock",
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
        ) {
            Ok(lock) => (lock, true),

            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                open_file(&directory_file, c"revisions.lock", libc::O_RDWR)
                    .context("open revision lock")?,
                false,
            ),

            Err(error) => return Err(error).context("create revision lock"),
        };

        check_private(&lock.metadata()?, false)?;

        #[cfg(test)]
        after_lock_open();

        // Keep the existing lock protocol for processes using only revisions.lock.
        lock.try_lock()
            .context("revision directory is already locked or cannot be locked")?;

        let ledger = match open_file(&directory_file, c"revisions.json", libc::O_RDONLY) {
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

                unlink_tmp(&directory_file)
                    .context("remove uncommitted revision temporary file")?;

                ledger
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                ensure!(
                    created,
                    "revision state is missing after prior initialization; restore state or use a new server UUID"
                );

                // Enumerate the validated target, not a fresh resolution of the
                // configured symlink. The descriptor stays owned throughout.
                let entries = fs::read_dir(format!("/proc/self/fd/{}", directory_file.as_raw_fd()))
                    .context("inspect new revision directory")?;

                for entry in entries {
                    ensure!(
                        entry?.file_name() == "revisions.lock",
                        "revision state is missing in a nonempty directory; restore state or use a new server UUID"
                    );
                }

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
                _lock: lock,
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
        let deadline = tokio::time::Instant::now() + COMMIT_TIMEOUT;
        let inner = Arc::clone(&self.inner);

        let worker = tokio::task::spawn_blocking(move || {
            let result = inner.write(ledger);
            drop(inner);

            result
        });

        #[cfg(test)]
        if matches!(self.inner.fault, Some(tests::Fault::LateObservation)) {
            // Observe a ready worker only once the commit deadline has elapsed.
            while !worker.is_finished() {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            tokio::time::advance(COMMIT_TIMEOUT + std::time::Duration::from_millis(1)).await;
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

            let mut temporary = open_file(
                &self.directory,
                c"revisions.tmp",
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            )?;

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

            // SAFETY: both directory descriptors are live; both names are static C
            // strings. Renaming within this directory provides atomic replacement.
            let result = unsafe {
                libc::renameat(
                    self.directory.as_raw_fd(),
                    c"revisions.tmp".as_ptr(),
                    self.directory.as_raw_fd(),
                    c"revisions.json".as_ptr(),
                )
            };

            if result == -1 {
                return Err(io::Error::last_os_error().into());
            }

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
mod tests {
    use super::*;
    use std::{
        os::unix::{
            fs::{PermissionsExt, symlink},
            net::UnixStream,
            process::CommandExt,
        },
        process::{Child, Command, ExitStatus, Stdio},
        sync::Mutex,
        thread,
        time::{Duration, Instant},
    };
    use tempfile::TempDir;

    // A fork temporarily inherits every test thread's flock descriptors, even
    // with CLOEXEC. Exclude spawning across fixture close/reopen sequences until
    // exec has closed those copies; never retry or weaken the production lock.
    static SPAWN_OR_REOPEN: Mutex<()> = Mutex::new(());

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
    fn first_installation_is_read_only_except_for_lock_and_restart_wraps_history() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(ledger, empty());
        assert!(!directory.path().join("revisions.json").exists());
        assert!(!directory.path().join("revisions.tmp").exists());
        ledger.restart();
        assert_eq!(ledger.system_update_id, 1);
        drop(store);
        assert!(Store::open(directory.path(), id(1)).is_err());

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
    fn concurrent_first_installation_preserves_creator_ownership() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();

        let (store, mut ledger) = Store::open_inner(directory.path(), id(1), || {
            assert!(directory.path().join("revisions.lock").exists());

            // Force the contender into the create/lock gap, without sleeps.
            let error = thread::scope(|scope| {
                scope
                    .spawn(|| Store::open(directory.path(), id(1)).err().unwrap())
                    .join()
                    .unwrap()
            });

            assert!(matches!(
                error.downcast_ref::<std::fs::TryLockError>(),
                Some(std::fs::TryLockError::WouldBlock)
            ));
        })
        .unwrap();

        assert_eq!(ledger, empty());
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
        let mut albums: BTreeMap<_, _> = (1..=CURRENT_ALBUMS)
            .map(|number| (id(number as u128), digest(1)))
            .collect();

        let ledger = empty()
            .root_transition(&digest(1), &albums)
            .unwrap()
            .unwrap();

        albums.remove(&id(1));
        albums.insert(id(CURRENT_ALBUMS as u128 + 1), digest(1));

        let replacement = ledger
            .root_transition(&digest(2), &albums)
            .unwrap()
            .unwrap();

        assert_eq!(replacement.albums.len(), CURRENT_ALBUMS + 1);
        assert!(!replacement.albums[&id(1)].present);
        assert_eq!(replacement.albums[&id(1)].update_id, 1);

        assert_eq!(
            replacement.albums[&id(CURRENT_ALBUMS as u128 + 1)].update_id,
            0
        );

        albums.insert(id(CURRENT_ALBUMS as u128 + 2), digest(1));
        assert!(replacement.root_transition(&digest(3), &albums).is_err());
        assert_eq!(replacement.albums.len(), CURRENT_ALBUMS + 1);

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
        let flags = unsafe { libc::fcntl(store.inner._lock.as_raw_fd(), libc::F_GETFD) };

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
    fn directory_lock_precedes_creation_and_existing_file_lock_remains_authoritative() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        let pinned = File::open(directory.path()).unwrap();
        pinned.try_lock().unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
        drop(pinned);

        install(directory.path(), &populated());
        put(directory.path(), "revisions.lock", b"");
        put(directory.path(), "revisions.tmp", b"uncommitted");

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join("revisions.lock"))
            .unwrap();

        lock.try_lock().unwrap();
        let lock_inode = lock.metadata().unwrap().ino();

        let state_inode = fs::metadata(directory.path().join("revisions.json"))
            .unwrap()
            .ino();

        // Simulate an existing process that holds only the original file lock.
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

        lock.unlock().unwrap();
        let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
        assert_eq!(loaded, populated());
        assert!(!directory.path().join("revisions.tmp").exists());

        assert!(matches!(
            lock.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));

        assert_eq!(
            fs::metadata(directory.path().join("revisions.lock"))
                .unwrap()
                .ino(),
            lock_inode
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
            "{{\"server_uuid\":\"{}\",\"system_update_id\":0,\"root_digest\":null,\"albums\":{{\"{uuid}\":{album},\"{}\":{album}}}}}",
            id(1),
            uuid.to_uppercase()
        );

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
                    present: number <= CURRENT_ALBUMS,
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
            .get_mut(&id(CURRENT_ALBUMS as u128 + 1))
            .unwrap()
            .present = true;

        install(directory.path(), &ledger);
        assert!(Store::open(directory.path(), id(1)).is_err());

        ledger
            .albums
            .get_mut(&id(CURRENT_ALBUMS as u128 + 1))
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
        assert!(actual.join("revisions.lock").is_file());
        assert!(!actual.join("revisions.json").exists());
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
    fn directory_lock_and_writes_remain_on_pinned_symlink_target() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let layout = private_directory();
        let original = private_directory();
        let replacement = private_directory();
        let configured = layout.path().join("state");
        symlink(original.path(), &configured).unwrap();
        let (store, mut ledger) = Store::open(&configured, id(1)).unwrap();
        fs::remove_file(&configured).unwrap();
        symlink(replacement.path(), &configured).unwrap();
        assert!(Store::open(original.path(), id(1)).is_err());
        assert_eq!(fs::read_dir(replacement.path()).unwrap().count(), 0);
        let (other, _) = Store::open(&configured, id(2)).unwrap();
        ledger.restart();
        store.inner.write(ledger.clone()).unwrap();
        assert!(!replacement.path().join("revisions.json").exists());
        drop(store);
        let (_store, loaded) = Store::open(original.path(), id(1)).unwrap();
        assert_eq!(loaded, ledger);
        assert!(Store::open(replacement.path(), id(2)).is_err());
        drop(other);
    }

    #[test]
    fn rejects_nonprivate_symlink_nonregular_and_hardlinked_files_without_blocking() {
        let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
        let directory = private_directory();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(Store::open(directory.path(), id(1)).is_err());
        let parent = private_directory();
        symlink(directory.path(), parent.path().join("link")).unwrap();
        assert!(Store::open(&parent.path().join("link"), id(1)).is_err());

        for name in ["revisions.json", "revisions.lock"] {
            let directory = private_directory();
            put(directory.path(), "target", b"unchanged");
            symlink(directory.path().join("target"), directory.path().join(name)).unwrap();
            assert!(Store::open(directory.path(), id(1)).is_err());

            assert_eq!(
                fs::read(directory.path().join("target")).unwrap(),
                b"unchanged"
            );

            fs::remove_file(directory.path().join(name)).unwrap();
            fs::hard_link(directory.path().join("target"), directory.path().join(name)).unwrap();
            assert!(Store::open(directory.path(), id(1)).is_err());
            fs::remove_file(directory.path().join(name)).unwrap();
            fs::create_dir(directory.path().join(name)).unwrap();
            assert!(Store::open(directory.path(), id(1)).is_err());
            fs::remove_dir(directory.path().join(name)).unwrap();

            let name_c =
                std::ffi::CString::new(directory.path().join(name).as_os_str().as_encoded_bytes())
                    .unwrap();

            // SAFETY: the path is NUL-terminated and refers to this test's directory.
            assert_eq!(unsafe { libc::mkfifo(name_c.as_ptr(), 0o600) }, 0);
            let start = Instant::now();
            assert!(Store::open(directory.path(), id(1)).is_err());
            assert!(start.elapsed() < Duration::from_secs(1));
            fs::remove_file(directory.path().join(name)).unwrap();
            install(directory.path(), &empty());

            if name == "revisions.lock" {
                put(directory.path(), name, b"");
            }

            fs::set_permissions(
                directory.path().join(name),
                fs::Permissions::from_mode(0o644),
            )
            .unwrap();

            assert!(Store::open(directory.path(), id(1)).is_err());
        }
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
            ledger.root_digest = Some("x".repeat(REVISION_BYTES + 1));
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
                assert!(started.elapsed() >= COMMIT_TIMEOUT);
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
