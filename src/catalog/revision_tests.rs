use super::*;
use crate::catalog::{
    MAX_ALBUMS,
    snapshots::{Root, project_root},
};

fn id(number: u128) -> Uuid {
    Uuid::from_u128(number)
}

fn digest(number: u8) -> Digest {
    [number; 32]
}

fn snapshot(
    title: &str,
    albums: impl IntoIterator<Item = (Uuid, &'static str)>,
) -> anyhow::Result<Root> {
    let records = albums
        .into_iter()
        .map(|(id, title)| crate::immich::Album {
            id,
            album_name: title.into(),
            created_at: None,
            end_date: None,
            album_thumbnail_asset_id: None,
        })
        .collect();

    project_root("192.0.2.1:8200".parse().unwrap(), title, records, &mut 0)
}

fn populated(seed: u32) -> Ledger {
    Ledger::new(seed)
        .root_transition(&snapshot("Photos", [(id(2), "Original")]).unwrap())
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
    let root = snapshot("Photos", [(id(2), "Original")]).unwrap();
    let ledger = populated(0);
    assert_eq!(ledger.root_transition(&root), None);

    let projection = ledger
        .root_transition(&snapshot("Renamed root", [(id(2), "Original")]).unwrap())
        .unwrap();

    assert_eq!(projection.system_update_id, 2);
    assert_eq!(projection.albums, ledger.albums);

    let mut ledger = ledger;
    assert!(ledger.update_contents(id(2), &digest(3)).unwrap());
    assert_eq!(ledger.root_transition(&root), None);

    let empty = snapshot("Photos", []).unwrap();
    let mut removed = ledger.root_transition(&empty).unwrap();
    assert_eq!(removed.system_update_id, ledger.system_update_id + 1);
    assert!(removed.albums.is_empty());
    assert_eq!(removed.root_transition(&empty), None);

    let unchanged = removed.clone();
    assert!(removed.update_contents(id(2), &digest(4)).is_err());
    assert!(removed.update_contents(id(100), &digest(4)).is_err());
    assert_eq!(removed, unchanged);

    let reappeared = removed.root_transition(&root).unwrap();
    assert_eq!(reappeared.albums[&id(2)].update_id, 4);
    assert_eq!(reappeared.albums[&id(2)].contents_digest, None);

    let mut contents = reappeared.clone();
    assert!(contents.update_contents(id(2), &digest(3)).unwrap());
    assert_eq!(contents.albums[&id(2)].update_id, 5);

    let renamed = contents
        .root_transition(&snapshot("Photos", [(id(2), "Renamed")]).unwrap())
        .unwrap();

    assert_eq!(renamed.albums[&id(2)].update_id, 6);
    assert_eq!(renamed.system_update_id, contents.system_update_id + 1);

    assert_eq!(
        renamed.albums[&id(2)].contents_digest,
        contents.albums[&id(2)].contents_digest
    );
}

#[test]
fn root_and_contents_counters_wrap_and_unknown_empty_root_advances() {
    let empty = snapshot("Photos", []).unwrap();
    let initial = Ledger::new(0).root_transition(&empty).unwrap();
    assert_eq!(initial.system_update_id, 1);
    assert!(initial.albums.is_empty());
    let mut ledger = populated(0);
    ledger.system_update_id = u32::MAX;
    ledger.albums.get_mut(&id(2)).unwrap().update_id = u32::MAX;

    let mut contents = ledger.clone();
    assert!(contents.update_contents(id(2), &digest(1)).unwrap());

    assert_eq!(contents.system_update_id, 0);
    assert_eq!(contents.albums[&id(2)].update_id, 0);

    let renamed = ledger
        .root_transition(&snapshot("Photos", [(id(2), "Renamed")]).unwrap())
        .unwrap();

    assert_eq!(renamed.system_update_id, 0);
    assert_eq!(renamed.albums[&id(2)].update_id, 0);

    let removed = ledger.root_transition(&empty).unwrap();
    assert_eq!(removed.system_update_id, 0);
    assert!(removed.albums.is_empty());
}

#[test]
fn replacement_at_capacity_discards_removed_identities_without_exhaustion() {
    let mut albums: BTreeMap<_, _> = (1..=MAX_ALBUMS)
        .map(|number| (id(number as u128), "Original"))
        .collect();

    let ledger = Ledger::new(0)
        .root_transition(&snapshot("Photos", albums.clone()).unwrap())
        .unwrap();

    albums.remove(&id(1));
    albums.insert(id(MAX_ALBUMS as u128 + 1), "Original");

    let replacement = ledger
        .root_transition(&snapshot("Photos", albums.clone()).unwrap())
        .unwrap();

    assert_eq!(replacement.albums.len(), MAX_ALBUMS);
    assert!(!replacement.albums.contains_key(&id(1)));
    assert_eq!(replacement.albums[&id(2)].update_id, 1);
    assert_eq!(replacement.albums[&id(MAX_ALBUMS as u128 + 1)].update_id, 2);
    albums.insert(id(MAX_ALBUMS as u128 + 2), "Original");
    assert!(snapshot("Photos", albums).is_err());
    let mut ledger = replacement;

    // More unique identities than the former process-lifetime allowance.
    for batch in 2..=6 {
        let albums = (1..MAX_ALBUMS + 1)
            .map(|number| (id((batch * MAX_ALBUMS + number) as u128), "Original"));

        ledger = ledger
            .root_transition(&snapshot("Photos", albums).unwrap())
            .unwrap();

        assert_eq!(ledger.albums.len(), MAX_ALBUMS);

        assert!(
            ledger
                .albums
                .values()
                .all(|album| album.update_id == ledger.system_update_id)
        );
    }
}
