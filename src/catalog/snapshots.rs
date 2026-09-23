use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::SocketAddrV4,
};

use anyhow::{Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{MAX_ALBUMS, Object, Resource, SNAPSHOT_BYTES};
use crate::{
    immich::{Asset, Client},
    media::{Representation, asset_url},
    protocol::xml_char,
};

const ALBUM_ITEMS: usize = 20_000;
const SEARCH_PAGES: usize = 50;
const SEARCH_RECORDS: usize = 50_000;

pub(super) struct Source {
    client: Client,
    http_address: SocketAddrV4,
    friendly_name: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(super) struct Album {
    pub(super) id: Uuid,
    pub(super) object: Object,
    pub(super) created_at: Option<DateTime<Utc>>,
    pub(super) end_date: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub(super) digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct Item {
    pub(super) id: Uuid,
    pub(super) object: Object,
    pub(super) capture: Option<DateTime<Utc>>,
    is_edited: bool,
    checksum: Option<String>,
    updated_at: Option<String>,
    thumbhash: Option<String>,
}

#[derive(Debug)]
pub(super) struct Root {
    pub(super) object: Object,
    pub(super) albums: BTreeMap<Uuid, Album>,
    pub(super) digest: String,
    pub(super) bytes: usize,
}

#[derive(Debug)]
pub(super) struct Contents {
    pub(super) items: BTreeMap<Uuid, Item>,
    pub(super) digest: String,
    pub(super) bytes: usize,
}

impl Source {
    pub(super) fn new(client: Client, http_address: SocketAddrV4, friendly_name: String) -> Self {
        Self {
            client,
            http_address,
            friendly_name,
        }
    }

    /// The caller admits and bounds the whole refresh, including version checking.
    pub(super) async fn root(&self) -> Result<Root> {
        self.client.ensure_supported_version().await?;
        let records = self.client.albums().await?;
        let mut bad_dates = 0;

        let root = project_root(
            self.http_address,
            &self.friendly_name,
            records,
            &mut bad_dates,
        )?;

        log_dates(bad_dates);

        Ok(root)
    }

    /// Requires the caller to establish readable root membership first. No version
    /// request, admission queue, timeout extension, or publication happens here.
    pub(super) async fn contents(&self, album: Uuid) -> Result<Contents> {
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
                    let item = project_item(self.http_address, album, dto, &mut bad_dates)?;

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
                    uri: asset_url(self.http_address, id, Representation::Playback),
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
}

fn project_root(
    http_address: SocketAddrV4,
    friendly_name: &str,
    records: Vec<crate::immich::Album>,
    bad_dates: &mut usize,
) -> Result<Root> {
    let root_object = Object {
        id: "0".into(),
        parent_id: "-1".into(),
        title: friendly_name.to_owned(),
        class: "object.container".into(),
        date: None,
        art: None,
        child_count: None,
        resources: Vec::new(),
    };

    let mut albums = BTreeMap::new();
    let mut bytes = 2 + encoded_size(&root_object, SNAPSHOT_BYTES - 2)?;

    for dto in records {
        let created_at = parse_date(dto.created_at.as_deref(), bad_dates);

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
                    .map(|id| asset_url(http_address, id, Representation::Preview)),
                child_count: None,
                resources: Vec::new(),
            },
            created_at,
            end_date: parse_date(dto.end_date.as_deref(), bad_dates),
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

fn project_item(
    http_address: SocketAddrV4,
    album: Uuid,
    dto: Asset,
    bad_dates: &mut usize,
) -> Result<Option<Item>> {
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

    let mime = dto
        .original_mime_type
        .as_deref()
        .and_then(crate::mime::parse)
        .map(str::to_ascii_lowercase);

    let (representation, mime) = if video {
        (
            Representation::Original,
            mime.unwrap_or_else(|| "application/octet-stream".into()),
        )
    } else if !dto.is_edited
        && mime
            .as_deref()
            .is_some_and(|mime| matches!(mime, "image/jpeg" | "image/png" | "image/gif"))
    {
        (
            Representation::Original,
            mime.expect("checked original image MIME"),
        )
    } else {
        (Representation::Display, "image/jpeg".into())
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
        uri: asset_url(http_address, dto.id, representation),
        mime,
        duration,
        byte_seek: video,
    }];

    if !video {
        resources.push(Resource {
            uri: asset_url(http_address, dto.id, Representation::Preview),
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
            art: Some(asset_url(http_address, dto.id, Representation::Preview)),
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
        serde_json::to_writer(&mut *self, value)
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

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
