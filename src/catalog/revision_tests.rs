use super::*;

fn id(number: u128) -> Uuid {
    Uuid::from_u128(number)
}

fn digest(number: u8) -> Digest {
    Digest::from_bytes([number; 32])
}

fn populated(seed: u32) -> Ledger {
    Ledger::new(seed)
        .root_transition(&digest(1), [(id(2), digest(2))].into_iter())
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
        assert_eq!(root.albums[&id(2)].update_id, seed.wrapping_add(1));

        let mut contents = root;
        assert!(contents.update_contents(id(2), &digest(3)).unwrap());

        assert_eq!(contents.system_update_id, seed.wrapping_add(2));
        assert_eq!(contents.albums[&id(2)].update_id, seed.wrapping_add(2));

        let unchanged = contents.clone();
        assert!(!contents.update_contents(id(2), &digest(3)).unwrap());
        assert_eq!(contents, unchanged);

        // Repeated or lower seeds are valid; restarting cannot promise uniqueness.
        assert_eq!(Ledger::new(seed), initial);
        assert_eq!(Ledger::new(0).system_update_id, 0);
    }
}

#[test]
fn root_transitions_forget_removals_and_reintroduce_at_the_new_global_revision() {
    let root = digest(1);
    let albums = BTreeMap::from([(id(2), digest(2))]);
    let ledger = populated(0);
    assert_eq!(
        ledger
            .root_transition(&root, albums.clone().into_iter())
            .unwrap(),
        None
    );

    let projection = ledger
        .root_transition(&digest(9), albums.clone().into_iter())
        .unwrap()
        .unwrap();

    assert_eq!(projection.system_update_id, 2);
    assert_eq!(projection.albums, ledger.albums);

    let mut ledger = ledger;
    assert!(ledger.update_contents(id(2), &digest(3)).unwrap());

    let mut removed = ledger
        .root_transition(&root, std::iter::empty())
        .unwrap()
        .unwrap();

    assert_eq!(removed.system_update_id, ledger.system_update_id + 1);
    assert!(removed.albums.is_empty());

    assert_eq!(
        removed.root_transition(&root, std::iter::empty()).unwrap(),
        None
    );

    let unchanged = removed.clone();
    assert!(removed.update_contents(id(2), &digest(4)).is_err());
    assert!(removed.update_contents(id(100), &digest(4)).is_err());
    assert_eq!(removed, unchanged);

    let reappeared = removed
        .root_transition(&root, [(id(2), digest(4))].into_iter())
        .unwrap()
        .unwrap();

    assert_eq!(reappeared.albums[&id(2)].update_id, 4);
    assert_eq!(reappeared.albums[&id(2)].contents_digest, None);

    let mut contents = reappeared.clone();
    assert!(contents.update_contents(id(2), &digest(3)).unwrap());
    assert_eq!(contents.albums[&id(2)].update_id, 5);

    let renamed = reappeared
        .root_transition(&root, [(id(2), digest(5))].into_iter())
        .unwrap()
        .unwrap();

    assert_eq!(renamed.albums[&id(2)].update_id, 5);
    assert_eq!(renamed.system_update_id, reappeared.system_update_id + 1);
}

#[test]
fn root_and_contents_counters_wrap_and_unknown_empty_root_advances() {
    let initial = Ledger::new(0)
        .root_transition(&digest(0), std::iter::empty())
        .unwrap()
        .unwrap();

    assert_eq!(initial.system_update_id, 1);
    assert!(initial.albums.is_empty());
    let mut ledger = populated(0);
    ledger.system_update_id = u32::MAX;
    ledger.albums.get_mut(&id(2)).unwrap().update_id = u32::MAX;

    let mut contents = ledger.clone();
    assert!(contents.update_contents(id(2), &digest(1)).unwrap());

    assert_eq!(contents.system_update_id, 0);
    assert_eq!(contents.albums[&id(2)].update_id, 0);

    let removed = ledger
        .root_transition(&digest(2), std::iter::empty())
        .unwrap()
        .unwrap();

    assert_eq!(removed.system_update_id, 0);
    assert!(removed.albums.is_empty());
}

#[test]
fn replacement_at_capacity_discards_removed_identities_without_exhaustion() {
    let mut albums: BTreeMap<_, _> = (1..=MAX_ALBUMS)
        .map(|number| (id(number as u128), digest(1)))
        .collect();

    let ledger = Ledger::new(0)
        .root_transition(&digest(1), albums.clone().into_iter())
        .unwrap()
        .unwrap();

    albums.remove(&id(1));
    albums.insert(id(MAX_ALBUMS as u128 + 1), digest(1));

    let replacement = ledger
        .root_transition(&digest(2), albums.clone().into_iter())
        .unwrap()
        .unwrap();

    assert_eq!(replacement.albums.len(), MAX_ALBUMS);
    assert!(!replacement.albums.contains_key(&id(1)));
    assert_eq!(replacement.albums[&id(2)].update_id, 1);
    assert_eq!(replacement.albums[&id(MAX_ALBUMS as u128 + 1)].update_id, 2);
    albums.insert(id(MAX_ALBUMS as u128 + 2), digest(1));
    assert!(
        replacement
            .root_transition(&digest(3), albums.into_iter())
            .is_err()
    );
    let mut ledger = replacement;

    // More unique identities than the former process-lifetime allowance.
    for batch in 2..=6 {
        let albums = (1..MAX_ALBUMS + 1)
            .map(|number| (id((batch * MAX_ALBUMS + number) as u128), digest(1)));

        ledger = ledger.root_transition(&digest(2), albums).unwrap().unwrap();

        assert_eq!(ledger.albums.len(), MAX_ALBUMS);

        assert!(
            ledger
                .albums
                .values()
                .all(|album| album.update_id == ledger.system_update_id)
        );
    }
}
