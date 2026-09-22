use std::{cmp::Ordering, sync::Arc};

use chrono::{DateTime, Utc};
use icu_collator::CollatorBorrowed;
use uuid::Uuid;

use super::{
    BrowseQuery, BrowseResult, FAILED, MISSING, Object,
    revisions::Ledger,
    snapshots::{Contents, Root},
};
use crate::protocol::Fault;

// A response pins one publication independently of subsequent cache eviction.
#[derive(Clone)]
pub(super) struct View {
    pub(super) root: Arc<Root>,
    pub(super) contents: Option<Arc<Contents>>,
    pub(super) ledger: Arc<Ledger>,
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

impl View {
    pub(super) fn browse(
        &self,
        id: ObjectId,
        query: BrowseQuery,
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
                    .map(|item| &item.object)
                    .ok_or(MISSING)?
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
mod tests {
    use super::*;
    use crate::catalog::{revisions::AlbumRevision, snapshots::Album};

    #[test]
    fn albums_default_to_latest_date_with_title_ties_and_missing_dates_last() {
        let root_object = Object {
            id: "0".into(),
            parent_id: "-1".into(),
            title: "Photos & videos".into(),
            class: "object.container".into(),
            date: None,
            art: None,
            child_count: None,
            resources: Vec::new(),
        };

        let albums = [
            (1, "Z", "2020-01-01", Some("2025-01-01")),
            (2, "A", "2030-01-01", Some("2024-12-31")),
            (3, "C", "2024-01-01", Some("2025-01-01")),
            (4, "c", "2024-01-01", Some("2025-01-01")),
            (5, "\u{106}", "2024-01-01", Some("2025-01-01")),
            (6, "A missing", "2024-01-01", None),
            (7, "B missing", "2024-01-01", None),
            (8, "C missing", "2024-01-01", None),
            (9, "D missing", "2024-01-01", None),
        ]
        .into_iter()
        .rev()
        .map(|(number, title, created, end)| {
            let id = Uuid::from_u128(number);

            let album = Album {
                id,
                object: Object {
                    id: format!("album:{id}"),
                    parent_id: "0".into(),
                    title: title.into(),
                    class: "object.container.album".into(),
                    date: Some(created.into()),
                    ..root_object.clone()
                },
                created_at: Some(format!("{created}T00:00:00Z").parse().unwrap()),
                end_date: end.map(|date| format!("{date}T00:00:00Z").parse().unwrap()),
                digest: format!("{number:064x}"),
            };

            (id, album)
        })
        .collect();

        let root = Root {
            object: root_object,
            albums,
            digest: "a".repeat(64),
            bytes: 0,
        };

        let ledger = Ledger {
            server_uuid: Uuid::from_u128(999),
            system_update_id: 42,
            root_digest: Some(root.digest.clone()),
            albums: root
                .albums
                .iter()
                .map(|(id, album)| {
                    (
                        *id,
                        AlbumRevision {
                            update_id: 0,
                            present: true,
                            metadata_digest: album.digest.clone(),
                            contents_digest: None,
                        },
                    )
                })
                .collect(),
        };

        let view = View {
            root: Arc::new(root),
            contents: None,
            ledger: Arc::new(ledger),
        };

        let collator = crate::config::collator("pl").unwrap();

        for (sort, expected) in [
            (None, [3, 4, 5, 1, 2, 6, 7, 8, 9]),
            (Some(false), [1, 3, 4, 5, 6, 7, 8, 9, 2]),
            (Some(true), [2, 3, 4, 5, 6, 7, 8, 9, 1]),
        ] {
            let query = BrowseQuery {
                object_id: "0".into(),
                metadata: false,
                starting_index: 0,
                requested_count: 0,
                sort,
            };

            let full = view
                .browse(ObjectId::Root, query.clone(), &collator)
                .unwrap();

            let expected: Vec<_> = expected
                .into_iter()
                .map(|id| format!("album:{}", Uuid::from_u128(id)))
                .collect();

            assert_eq!(full.total_matches, 9);
            assert_eq!(full.update_id, 42);

            assert_eq!(
                full.objects.iter().map(|o| &o.id).collect::<Vec<_>>(),
                expected.iter().collect::<Vec<_>>()
            );

            for start in [0, 3, 6, 9] {
                let page = view
                    .browse(
                        ObjectId::Root,
                        BrowseQuery {
                            starting_index: start,
                            requested_count: 3,
                            ..query.clone()
                        },
                        &collator,
                    )
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
    }

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
}
