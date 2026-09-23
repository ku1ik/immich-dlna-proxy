use std::{cmp::Ordering, sync::Arc};

use chrono::{DateTime, Utc};
use icu_collator::CollatorBorrowed;
use uuid::Uuid;

use super::{
    BrowseMode, BrowseQuery, BrowseResult, FAILED, MISSING, Object, SortOrder,
    revisions::Ledger,
    snapshots::{Contents, Root},
};
use crate::protocol::Fault;

// A response pins one publication independently of subsequent cache eviction.
#[derive(Clone)]
pub(super) struct View {
    pub(super) root: Arc<Root>,
    pub(super) payload: Payload,
    pub(super) ledger: Arc<Ledger>,
}

#[derive(Clone)]
pub(super) enum Payload {
    Root,
    Album { id: Uuid, contents: Arc<Contents> },
}

enum Rows<'a> {
    Albums(&'a Root),
    Items(&'a Contents),
}

enum Selection<'a> {
    Metadata(Object),
    Children {
        rows: Rows<'a>,
        starting_index: u32,
        requested_count: u32,
        sort: SortOrder,
    },
}

struct ResolvedBrowse<'a> {
    selection: Selection<'a>,
    update_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ObjectId {
    Root,
    Album(Uuid),
    Item { album: Uuid, asset: Uuid },
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root => f.write_str("0"),
            Self::Album(album) => write!(f, "album:{album}"),
            Self::Item { album, asset } => write!(f, "album:{album}:asset:{asset}"),
        }
    }
}

pub fn parse_id(value: &str) -> Result<ObjectId, Fault> {
    let invalid = Fault::NoSuchObject;
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
        query: BrowseQuery,
        collator: &CollatorBorrowed<'_>,
    ) -> Result<BrowseResult, Fault> {
        Ok(self.resolve(query)?.browse(collator))
    }

    fn contents(&self, album: Uuid) -> Result<&Contents, Fault> {
        match &self.payload {
            Payload::Album { id, contents } if *id == album => Ok(contents),
            _ => Err(FAILED),
        }
    }

    fn resolve(&self, query: BrowseQuery) -> Result<ResolvedBrowse<'_>, Fault> {
        let root = &self.root;
        let ledger = &self.ledger;

        let (selection, update_id) = match query.object_id {
            ObjectId::Root => {
                let selection = match query.mode {
                    BrowseMode::Metadata => {
                        let mut object = root.object.clone();

                        object.kind = super::ObjectKind::Root {
                            child_count: Some(root.albums.len()),
                        };

                        Selection::Metadata(object)
                    }

                    BrowseMode::DirectChildren {
                        starting_index,
                        requested_count,
                    } => Selection::Children {
                        rows: Rows::Albums(root),
                        starting_index,
                        requested_count,
                        sort: query.sort,
                    },
                };

                (selection, ledger.system_update_id)
            }

            ObjectId::Album(id) => {
                let album = root.albums.get(&id).ok_or(MISSING)?;
                let revision = ledger.albums.get(&id).ok_or(MISSING)?;

                let selection = match query.mode {
                    BrowseMode::Metadata => Selection::Metadata(album.metadata.object.clone()),

                    BrowseMode::DirectChildren {
                        starting_index,
                        requested_count,
                    } => Selection::Children {
                        rows: Rows::Items(self.contents(id)?),
                        starting_index,
                        requested_count,
                        sort: query.sort,
                    },
                };

                (selection, revision.update_id)
            }

            ObjectId::Item { album, asset } => {
                if !root.albums.contains_key(&album) {
                    return Err(MISSING);
                }

                let item = self.contents(album)?.items.get(&asset).ok_or(MISSING)?;

                if matches!(query.mode, BrowseMode::DirectChildren { .. }) {
                    return Err(Fault::NoSuchContainer);
                }

                (
                    Selection::Metadata(item.object.clone()),
                    ledger.system_update_id,
                )
            }
        };

        Ok(ResolvedBrowse {
            selection,
            update_id,
        })
    }
}

impl ResolvedBrowse<'_> {
    fn browse(self, collator: &CollatorBorrowed<'_>) -> BrowseResult {
        let Self {
            selection,
            update_id,
        } = self;

        let (rows, starting_index, requested_count, sort) = match selection {
            Selection::Metadata(object) => {
                return BrowseResult {
                    objects: vec![object],
                    total_matches: 1,
                    update_id,
                };
            }

            Selection::Children {
                rows,
                starting_index,
                requested_count,
                sort,
            } => (rows, starting_index, requested_count, sort),
        };

        let total_matches = match &rows {
            Rows::Albums(root) => root.albums.len(),
            Rows::Items(contents) => contents.items.len(),
        } as u32;

        if starting_index >= total_matches {
            return BrowseResult {
                objects: Vec::new(),
                total_matches,
                update_id,
            };
        }

        let objects = match rows {
            Rows::Albums(root) => {
                let mut rows: Vec<_> = root.albums.values().map(|album| &album.metadata).collect();

                rows.sort_unstable_by(|a, b| match sort {
                    SortOrder::Catalog => b
                        .end_date
                        .cmp(&a.end_date)
                        .then_with(|| collator.compare(&a.object.title, &b.object.title))
                        .then_with(|| a.id.cmp(&b.id)),

                    order => compare_dates(
                        a.object.date.as_deref(),
                        a.created_at.as_ref(),
                        a.id,
                        b.object.date.as_deref(),
                        b.created_at.as_ref(),
                        b.id,
                        order == SortOrder::DateDescending,
                    ),
                });

                paginate(
                    rows.into_iter().map(|album| &album.object),
                    starting_index,
                    requested_count,
                )
            }

            Rows::Items(contents) => {
                let mut rows: Vec<_> = contents.items.values().collect();

                rows.sort_unstable_by(|a, b| {
                    compare_dates(
                        a.object
                            .date
                            .as_deref()
                            .filter(|_| sort != SortOrder::Catalog),
                        a.capture.as_ref(),
                        a.id,
                        b.object
                            .date
                            .as_deref()
                            .filter(|_| sort != SortOrder::Catalog),
                        b.capture.as_ref(),
                        b.id,
                        sort == SortOrder::DateDescending,
                    )
                });

                paginate(
                    rows.into_iter().map(|item| &item.object),
                    starting_index,
                    requested_count,
                )
            }
        };

        BrowseResult {
            objects,
            total_matches,
            update_id,
        }
    }
}

fn paginate<'a>(
    rows: impl Iterator<Item = &'a Object>,
    starting_index: u32,
    requested_count: u32,
) -> Vec<Object> {
    rows.skip(starting_index as usize)
        .take(if requested_count == 0 {
            usize::MAX
        } else {
            requested_count as usize
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::snapshots::{Album, AlbumMetadata};

    #[test]
    fn albums_default_to_latest_date_with_title_ties_and_missing_dates_last() {
        let root_object = Object {
            kind: super::super::ObjectKind::Root { child_count: None },
            title: "Photos & videos".into(),
            date: None,
            art: None,
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
        .map(|(number, title, created, end)| {
            let id = Uuid::from_u128(number);

            let album = Album {
                metadata: AlbumMetadata {
                    id,
                    object: Object {
                        kind: super::super::ObjectKind::Album {
                            id,
                            child_count: None,
                        },
                        title: title.into(),
                        date: Some(created.into()),
                        ..root_object.clone()
                    },
                    created_at: Some(format!("{created}T00:00:00Z").parse().unwrap()),
                    end_date: end.map(|date| format!("{date}T00:00:00Z").parse().unwrap()),
                },
                digest: format!("{number:064x}").parse().unwrap(),
            };

            (id, album)
        })
        .collect();

        let root = Root {
            object: root_object,
            albums,
            digest: "a".repeat(64).parse().unwrap(),
            bytes: 0,
        };

        let ledger = Ledger {
            server_uuid: Uuid::from_u128(999),
            system_update_id: 42,
            root_digest: Some(root.digest),
            albums: Default::default(),
        };

        let view = View {
            root: Arc::new(root),
            payload: Payload::Root,
            ledger: Arc::new(ledger),
        };

        let collator = crate::config::collator("pl").unwrap();

        for (sort, expected) in [
            (SortOrder::Catalog, [3, 4, 5, 1, 2, 6, 7, 8, 9]),
            (SortOrder::DateAscending, [1, 3, 4, 5, 6, 7, 8, 9, 2]),
            (SortOrder::DateDescending, [2, 3, 4, 5, 6, 7, 8, 9, 1]),
        ] {
            let query = BrowseQuery {
                object_id: ObjectId::Root,
                mode: BrowseMode::DirectChildren {
                    starting_index: 0,
                    requested_count: 0,
                },
                sort,
            };

            let full = view.browse(query, &collator).unwrap();

            let expected: Vec<_> = expected
                .into_iter()
                .map(|id| format!("album:{}", Uuid::from_u128(id)))
                .collect();

            assert_eq!(full.total_matches, 9);
            assert_eq!(full.update_id, 42);

            assert_eq!(
                full.objects
                    .iter()
                    .map(|o| o.id().to_string())
                    .collect::<Vec<_>>(),
                expected
            );

            for start in [0, 3, 6, 9] {
                let page = view
                    .browse(
                        BrowseQuery {
                            mode: BrowseMode::DirectChildren {
                                starting_index: start,
                                requested_count: 3,
                            },
                            ..query
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
        }
    }

    #[test]
    fn object_ids_accept_canonical_namespaces_and_normalize_uuids() {
        let album = Uuid::from_u128(100_000);
        let asset = Uuid::from_u128(0xabcdef);
        let id = format!("album:{album}:asset:{asset}");

        assert_eq!(
            parse_id(&format!(
                "album:{}:asset:{}",
                album.to_string().to_uppercase(),
                asset.to_string().to_uppercase()
            )),
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
            assert_eq!(parse_id(id), Err(Fault::NoSuchObject));
        }
    }

    #[test]
    fn date_ordering_uses_capture_then_uuid_ties_and_keeps_missing_dates_last() {
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
