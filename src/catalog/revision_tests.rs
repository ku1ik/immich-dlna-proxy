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
fn first_installation_creates_no_files() {
    let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
    let directory = private_directory();
    let (store, mut ledger) = Store::open(directory.path(), id(1)).unwrap();
    assert_eq!(ledger, empty());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    ledger.restart();
    assert_eq!(ledger.system_update_id, 1);
    drop(store);
    assert_eq!(Store::open(directory.path(), id(1)).unwrap().1, empty());
}

#[test]
fn restart_wraps_counters_and_retains_history() {
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
    store.inner.write(&ledger).unwrap();
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
async fn first_contents_survive_reload_and_unchanged_transitions_are_detected() {
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
    drop(store);
    let (_store, loaded) = Store::open(directory.path(), id(1)).unwrap();
    assert_eq!(loaded, ledger);
    assert_eq!(loaded.contents_transition(id(2), &digest(3)).unwrap(), None);

    assert_eq!(
        loaded
            .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
            .unwrap(),
        None
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
fn decoding_rejects_corruption_unknown_missing_fields_and_duplicate_uuid_aliases() {
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
        let single = format!(
            "{{\"server_uuid\":\"{}\",\"system_update_id\":0,\"root_digest\":\"{}\",\"albums\":{{\"{key}\":{album}}}}}",
            id(1),
            digest(1)
        );

        let loaded: Ledger = serde_json::from_str(&single).unwrap();
        loaded.validate(id(1)).unwrap();
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
        valid.replace("\"system_update_id\":1", "\"system_update_id\":4294967296"),
        valid.replace("\"system_update_id\":1", "\"system_update_id\":-1"),
        valid.replace(",\"contents_digest\":null", ""),
        valid.replace(&format!("\"root_digest\":\"{}\",", digest(1)), ""),
        valid.replace(&id(2).to_string(), "not-a-uuid"),
        duplicates.clone(),
        duplicates.replace(&uuid.to_uppercase(), uuid),
    ];

    for json in invalid {
        assert!(
            serde_json::from_str::<Ledger>(&json).is_err(),
            "accepted {json}"
        );
    }
}

#[test]
fn validation_rejects_invalid_digests_and_identity() {
    let mut ledger = populated();
    ledger.albums.get_mut(&id(2)).unwrap().contents_digest = Some("BAD".into());
    assert!(ledger.validate(id(1)).is_err());

    for digest in ["A".repeat(64), "g".repeat(64), "a".repeat(63)] {
        let mut ledger = populated();
        ledger.albums.get_mut(&id(2)).unwrap().metadata_digest = digest.clone();
        assert!(ledger.validate(id(1)).is_err(), "accepted {digest}");
    }

    let ledger = populated();
    ledger.validate(id(1)).unwrap();
    assert!(ledger.validate(id(99)).is_err());
    assert!(ledger.validate(Uuid::nil()).is_err());
}

#[test]
fn load_rejects_invalid_state_without_disclosing_json_or_removing_tmp() {
    let _spawn = SPAWN_OR_REOPEN.lock().unwrap();
    let valid = serde_json::to_string(&populated()).unwrap();
    let directory = private_directory();
    put(directory.path(), "revisions.tmp", b"uncommitted");

    for (json, uuid, diagnostic) in [
        (
            "{\"private-state-content\":0}".to_owned(),
            id(1),
            "invalid revision JSON at line ",
        ),
        (
            valid.replace(&digest(2), "private-state-content"),
            id(1),
            "revision digest must be 64 lowercase hexadecimal characters",
        ),
        (
            valid.clone(),
            id(99),
            "revision state has the wrong server UUID",
        ),
        (valid, Uuid::nil(), "server UUID must not be nil"),
    ] {
        put(directory.path(), "revisions.json", json.as_bytes());
        let error = Store::open(directory.path(), uuid).err().unwrap();
        let error = format!("{error:#}");
        assert!(error.starts_with(diagnostic), "{error}");
        assert!(!error.contains("private-state-content"), "{error}");

        assert_eq!(
            fs::read(directory.path().join("revisions.tmp")).unwrap(),
            b"uncommitted"
        );
    }
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
    store.inner.write(&ledger).unwrap();
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
    store.inner.write(&ledger).unwrap();
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
        ledger.root_digest = Some("invalid".into());
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

            let committed: Ledger =
                serde_json::from_slice(&fs::read(directory.path().join("revisions.json")).unwrap())
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
