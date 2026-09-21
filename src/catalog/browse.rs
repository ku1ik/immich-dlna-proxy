use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use icu_collator::CollatorBorrowed;
use uuid::Uuid;

use super::{BrowseQuery, BrowseResult, FAILED, MISSING, Object, View};
use crate::protocol::Fault;

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

pub(super) fn compare_dates(
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
