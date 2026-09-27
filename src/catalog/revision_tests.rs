use super::*;

fn id(number: u128) -> Uuid {
    Uuid::from_u128(number)
}

fn digest(number: u8) -> Digest {
    Digest::from_bytes([number; 32])
}

fn populated(seed: u32) -> Ledger {
    Ledger::new(seed)
        .root_transition(&digest(1), &BTreeMap::from([(id(2), digest(2))]))
        .unwrap()
        .unwrap()
}

#[test]
fn seeded_first_observations_and_restarts_are_process_local() {
    for seed in [0, 1, 42, u32::MAX] {
        let initial = Ledger::new(seed);
        assert_eq!(initial.system_update_id, seed);
        assert!(initial.albums.is_empty());
        assert!(initial.root_digest.is_none());
        let root = populated(seed);
        assert_eq!(root.system_update_id, seed.wrapping_add(1));
        assert_eq!(root.albums[&id(2)].update_id, seed);

        let contents = root
            .contents_transition(id(2), &digest(3))
            .unwrap()
            .unwrap();

        assert_eq!(contents.system_update_id, seed.wrapping_add(2));
        assert_eq!(contents.albums[&id(2)].update_id, seed.wrapping_add(1));

        assert_eq!(
            contents.contents_transition(id(2), &digest(3)).unwrap(),
            None
        );

        // Repeated or lower seeds are valid; restarting cannot promise uniqueness.
        assert_eq!(Ledger::new(seed), initial);
        assert_eq!(Ledger::new(0).system_update_id, 0);
    }
}

#[test]
fn root_transitions_preserve_history_and_increment_each_affected_album_once() {
    let root = digest(1);
    let albums = BTreeMap::from([(id(2), digest(2))]);
    let ledger = populated(0);
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
    assert!(reappeared.albums[&id(2)].present);

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
    let initial = Ledger::new(0)
        .root_transition(&digest(0), &BTreeMap::new())
        .unwrap()
        .unwrap();

    assert_eq!(initial.system_update_id, 1);
    assert!(initial.albums.is_empty());
    let mut ledger = populated(0);
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
fn replacement_at_capacity_preserves_history_and_restart_recovers_exhaustion() {
    let mut albums: BTreeMap<_, _> = (1..=MAX_ALBUMS)
        .map(|number| (id(number as u128), digest(1)))
        .collect();

    let ledger = Ledger::new(0)
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
    let mut full = populated(0);

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

    let before = full.clone();
    let new_album = BTreeMap::from([(id(RETAINED_ALBUMS as u128 + 1), digest(1))]);
    let error = full.root_transition(&digest(2), &new_album).unwrap_err();
    assert!(error.to_string().contains("restart the service"));
    assert_eq!(full, before);

    assert!(
        full.contents_transition(id(2), &digest(8))
            .unwrap()
            .is_some()
    );

    assert!(
        full.root_transition(&digest(3), &BTreeMap::new())
            .unwrap()
            .is_some()
    );

    let recovered = Ledger::new(17)
        .root_transition(&digest(1), &new_album)
        .unwrap()
        .unwrap();

    assert_eq!(recovered.albums.len(), 1);
    assert_eq!(recovered.albums.values().next().unwrap().update_id, 17);
}
