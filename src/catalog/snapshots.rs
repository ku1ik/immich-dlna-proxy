use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::SocketAddrV4,
    num::NonZeroUsize,
};

use anyhow::{Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use sha2::{Digest as ShaDigest, Sha256};
use uuid::Uuid;

use super::{Digest, MAX_ALBUMS, Object, ObjectKind, Resource, SNAPSHOT_BYTES};
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

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Album {
    pub(super) metadata: AlbumMetadata,
    pub(super) digest: Digest,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub(super) struct AlbumMetadata {
    pub(super) object: Object,
    pub(super) created_at: Option<DateTime<Utc>>,
    pub(super) end_date: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct Item {
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
    pub(super) digest: Digest,
    pub(super) bytes: usize,
}

#[derive(Debug)]
pub(super) struct Contents {
    pub(super) items: BTreeMap<Uuid, Item>,
    pub(super) digest: Digest,
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
        let mut pages = 0;
        let mut records = 0;
        let mut count = ByteCount::new(SNAPSHOT_BYTES);
        count.write_all(b"[]")?;
        let mut bad_dates = 0;

        for encoded in [false, true] {
            if encoded
                && !items
                    .values()
                    .any(|item| matches!(item.object.kind, ObjectKind::Video { .. }))
            {
                break;
            }

            let mut seen = BTreeSet::new();
            let mut page = NonZeroUsize::MIN;

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
                    let id = dto.id;

                    if !seen.insert(id) {
                        continue;
                    }

                    advanced = true;

                    if encoded {
                        let Some(item) = items.get_mut(&id) else {
                            continue;
                        };

                        let ObjectKind::Video { resources, .. } = &mut item.object.kind else {
                            continue;
                        };

                        let resource = Resource {
                            uri: asset_url(self.http_address, id, Representation::Playback),
                            mime: "video/mp4".into(),
                            duration: None,
                            byte_seek: true,
                        };

                        count.write_all(b",")?;
                        count.json(&resource)?;
                        resources.push(resource);
                        continue;
                    }

                    let Some(item) = project_item(self.http_address, album, dto, &mut bad_dates)?
                    else {
                        continue;
                    };

                    ensure!(items.len() < ALBUM_ITEMS, "Immich album exceeds item limit");

                    if !items.is_empty() {
                        count.write_all(b",")?;
                    }

                    count.json(&item)?;

                    items.insert(id, item);
                }

                let Some(next) = result.next_page else {
                    break;
                };

                ensure!(advanced, "Immich search continuation made no progress");
                page = next;
            }
        }

        log_dates(bad_dates);
        let mut projection = Projection::new(SNAPSHOT_BYTES);

        serde_json::Serializer::new(&mut projection)
            .collect_seq(items.values())
            .map_err(|_| anyhow!("catalog projected byte limit exceeded"))?;

        let (digest, bytes) = projection.finish();

        Ok(Contents {
            items,
            digest,
            bytes,
        })
    }
}

pub(super) fn project_root(
    http_address: SocketAddrV4,
    friendly_name: &str,
    records: Vec<crate::immich::Album>,
    bad_dates: &mut usize,
) -> Result<Root> {
    let mut albums = BTreeMap::new();
    let mut bytes = 2;

    for dto in records {
        if albums.contains_key(&dto.id) {
            continue;
        }

        let created_at = parse_date(dto.created_at.as_deref(), bad_dates);

        let metadata = AlbumMetadata {
            object: Object {
                kind: ObjectKind::Album { id: dto.id },
                title: title(&dto.album_name, dto.id),
                date: created_at.map(|date| date.format("%Y-%m-%d").to_string()),
                art: dto
                    .album_thumbnail_asset_id
                    .map(|id| asset_url(http_address, id, Representation::Preview)),
            },
            created_at,
            end_date: parse_date(dto.end_date.as_deref(), bad_dates),
        };

        let mut projection = Projection::new(SNAPSHOT_BYTES);
        projection.json(&metadata)?;
        let (digest, size) = projection.finish();
        let album = Album { metadata, digest };

        ensure!(albums.len() < MAX_ALBUMS, "Immich root exceeds album limit");

        bytes += 1 + size;

        ensure!(
            bytes <= SNAPSHOT_BYTES,
            "Immich root exceeds projected byte limit"
        );

        albums.insert(dto.id, album);
    }

    let root_object = Object {
        kind: ObjectKind::Root {
            child_count: albums.len(),
        },
        title: friendly_name.to_owned(),
        date: None,
        art: None,
    };

    let mut projection = Projection::new(SNAPSHOT_BYTES);
    projection.write_all(b"[")?;
    projection.json(&root_object)?;

    for album in albums.values() {
        projection.write_all(b",")?;
        projection.json(&album.metadata)?;
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
    if !matches!(dto.visibility.as_str(), "timeline" | "archive") || dto.is_trashed {
        return Ok(None);
    }

    let mime = dto
        .original_mime_type
        .as_deref()
        .and_then(crate::mime::parse)
        .map(str::to_ascii_lowercase);

    let kind = match dto.kind.as_str() {
        "IMAGE" => {
            let (representation, mime) = match mime {
                Some(mime)
                    if !dto.is_edited
                        && matches!(mime.as_str(), "image/jpeg" | "image/png" | "image/gif") =>
                {
                    (Representation::Original, mime)
                }

                _ => (Representation::Display, "image/jpeg".into()),
            };

            ObjectKind::Photo {
                album,
                asset: dto.id,
                resources: vec![
                    Resource {
                        uri: asset_url(http_address, dto.id, representation),
                        mime,
                        duration: None,
                        byte_seek: false,
                    },
                    Resource {
                        uri: asset_url(http_address, dto.id, Representation::Preview),
                        mime: "image/jpeg".into(),
                        duration: None,
                        byte_seek: false,
                    },
                ],
            }
        }

        "VIDEO" => {
            let duration = dto.duration.filter(|ms| *ms >= 0).map(|ms| {
                format!(
                    "{}:{:02}:{:02}.{:03}",
                    ms / 3_600_000,
                    ms / 60_000 % 60,
                    ms / 1000 % 60,
                    ms % 1000
                )
            });

            ObjectKind::Video {
                album,
                asset: dto.id,
                resources: vec![Resource {
                    uri: asset_url(http_address, dto.id, Representation::Original),
                    mime: mime.unwrap_or_else(|| "application/octet-stream".into()),
                    duration,
                    byte_seek: true,
                }],
            }
        }

        _ => return Ok(None),
    };

    let name = dto
        .original_file_name
        .ok_or_else(|| anyhow!("eligible Immich asset is missing originalFileName"))?;

    let capture = parse_date(dto.file_created_at.as_deref(), bad_dates);

    let date = dto
        .local_date_time
        .as_deref()
        .and_then(|date| DateTime::parse_from_rfc3339(date).ok())
        .map(|date| date.format("%Y-%m-%d").to_string());

    *bad_dates += usize::from(date.is_none());

    Ok(Some(Item {
        object: Object {
            kind,
            title: title(&name, dto.id),
            date,
            art: Some(asset_url(http_address, dto.id, Representation::Preview)),
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

impl ByteCount {
    fn new(limit: usize) -> Self {
        Self { bytes: 0, limit }
    }

    fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(self, value)
            .map_err(|_| anyhow!("catalog projected byte limit exceeded"))
    }
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
            count: ByteCount::new(limit),
        }
    }

    fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(&mut *self, value)
            .map_err(|_| anyhow!("catalog projected byte limit exceeded"))
    }

    fn finish(self) -> (Digest, usize) {
        (self.hash.finalize().into(), self.count.bytes)
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

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
