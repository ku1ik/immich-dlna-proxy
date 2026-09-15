//! Construct bounded catalog snapshots from Immich metadata.

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

use crate::{
    deadline::Budget,
    immich::{Asset, AssetFilter, Client},
    limits,
    protocol::{Object, Resource},
};

#[derive(Clone)]
pub(super) struct Source {
    client: Client,
    http_address: SocketAddrV4,
    friendly_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Album {
    pub id: Uuid,
    pub object: Object,
    pub created_at: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Item {
    pub id: Uuid,
    pub object: Object,
    pub capture: Option<DateTime<Utc>>,
    is_edited: bool,
    checksum: Option<String>,
    updated_at: Option<String>,
    thumbhash: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Root {
    pub albums: BTreeMap<Uuid, Album>,
    pub digest: String,
    pub bytes: usize,
}

#[derive(Clone, Debug)]
pub struct Contents {
    pub items: BTreeMap<Uuid, Item>,
    pub digest: String,
    pub bytes: usize,
}

impl Source {
    pub fn new(client: Client, http_address: SocketAddrV4, friendly_name: String) -> Self {
        Self {
            client,
            http_address,
            friendly_name: xml_text(&friendly_name),
        }
    }

    /// The caller admits and bounds the whole refresh, including version checking.
    pub async fn root(&self, budget: &Budget) -> Result<Root> {
        budget.check()?;
        self.client.ensure_supported_version(budget).await?;
        let records = self.client.albums(budget).await?;

        let root_object = Object {
            id: "0".into(),
            parent_id: "-1".into(),
            title: self.friendly_name.clone(),
            class: "object.container".into(),
            date: None,
            art: None,
            child_count: None,
            resources: Vec::new(),
        };

        let mut albums = BTreeMap::new();
        let mut bytes = 2 + encoded_size(&root_object, limits::SNAPSHOT_BYTES - 2)?;
        let mut bad_dates = 0;

        for dto in records {
            budget.check()?;
            let created_at = parse_date(dto.created_at.as_deref(), &mut bad_dates);

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
                        .map(|id| self.media_url(id, "preview")),
                    child_count: None,
                    resources: Vec::new(),
                },
                created_at,
                end_date: parse_date(dto.end_date.as_deref(), &mut bad_dates),
                digest: String::new(),
            };

            let mut projection = Projection::new(limits::SNAPSHOT_BYTES);
            projection.json(&album)?;
            let (digest, size) = projection.finish();
            album.digest = digest;

            if let Some(previous) = albums.get(&album.id) {
                ensure!(previous == &album, "conflicting duplicate Immich album");
                continue;
            }

            ensure!(
                albums.len() < limits::CURRENT_ALBUMS,
                "Immich root exceeds album limit"
            );

            bytes += 1 + size;

            ensure!(
                bytes <= limits::SNAPSHOT_BYTES,
                "Immich root exceeds projected byte limit"
            );

            albums.insert(album.id, album);
        }

        budget.check()?;
        log_dates(bad_dates);
        let mut projection = Projection::new(limits::SNAPSHOT_BYTES);
        projection.write_all(b"[")?;
        projection.json(&root_object)?;

        for album in albums.values() {
            budget.check()?;
            projection.write_all(b",")?;
            projection.json(album)?;
        }

        projection.write_all(b"]")?;
        let (digest, bytes) = projection.finish();
        budget.check()?;

        Ok(Root {
            albums,
            digest,
            bytes,
        })
    }

    /// Requires the caller to establish readable root membership first. No version
    /// request, admission queue, timeout extension, or publication happens here.
    pub async fn contents(&self, album: Uuid, budget: &Budget) -> Result<Contents> {
        budget.check()?;
        let mut items = BTreeMap::<Uuid, Item>::new();
        let mut excluded = BTreeSet::new();
        let mut encoded_ids = BTreeSet::new();
        let mut pages = 0;
        let mut records = 0;
        let mut bytes = 2;
        let mut bad_dates = 0;

        for encoded in [false, true] {
            budget.check()?;

            if encoded
                && !items
                    .values()
                    .any(|item| item.object.class == "object.item.videoItem")
            {
                break;
            }

            let mut page = 1;

            loop {
                budget.check()?;

                ensure!(
                    pages < limits::SEARCH_PAGES && records < limits::SEARCH_RECORDS,
                    "Immich album traversal limit exceeded"
                );

                pages += 1;

                let filter = if encoded {
                    AssetFilter::EncodedVideos
                } else {
                    AssetFilter::All
                };

                let result = self
                    .client
                    .search_album(album, page, filter, budget)
                    .await?;

                ensure!(
                    result.items.len() <= limits::SEARCH_RECORDS - records,
                    "Immich album record limit exceeded"
                );

                records += result.items.len();
                let mut advanced = false;

                for dto in result.items {
                    budget.check()?;

                    if encoded {
                        advanced |= encoded_ids.insert(dto.id);
                        continue;
                    }

                    let id = dto.id;
                    let item = self.project(album, dto, &mut bad_dates)?;
                    budget.check()?;

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

                            ensure!(
                                items.len() < limits::ALBUM_ITEMS,
                                "Immich album exceeds item limit"
                            );

                            bytes += usize::from(!items.is_empty())
                                + encoded_size(&item, limits::SNAPSHOT_BYTES - bytes)?;

                            ensure!(
                                bytes <= limits::SNAPSHOT_BYTES,
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

                budget.check()?;

                let Some(next) = result.next_page else {
                    break;
                };

                ensure!(advanced, "Immich search continuation made no progress");
                page = next;
            }
        }

        for id in encoded_ids {
            budget.check()?;

            if let Some(item) = items.get_mut(&id)
                && item.object.class == "object.item.videoItem"
            {
                let old_size = encoded_size(item, limits::SNAPSHOT_BYTES)?;

                item.object.resources.push(Resource {
                    uri: self.media_url(id, "playback"),
                    mime: "video/mp4".into(),
                    duration: None,
                    byte_seek: true,
                });

                bytes -= old_size;
                bytes += encoded_size(item, limits::SNAPSHOT_BYTES - bytes)?;
            }
        }

        budget.check()?;
        log_dates(bad_dates);
        let mut projection = Projection::new(limits::SNAPSHOT_BYTES);
        projection.write_all(b"[")?;

        for (index, item) in items.values().enumerate() {
            budget.check()?;

            if index != 0 {
                projection.write_all(b",")?;
            }

            projection.json(item)?;
        }

        projection.write_all(b"]")?;
        let (digest, bytes) = projection.finish();
        budget.check()?;

        Ok(Contents {
            items,
            digest,
            bytes,
        })
    }

    fn media_url(&self, id: Uuid, representation: &str) -> String {
        format!(
            "http://{}/media/assets/{id}/{representation}",
            self.http_address
        )
    }

    fn project(&self, album: Uuid, dto: Asset, bad_dates: &mut usize) -> Result<Option<Item>> {
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
        let mime = dto.original_mime_type.as_deref().and_then(media_type);

        let (representation, mime) = if video {
            (
                "original",
                mime.unwrap_or_else(|| "application/octet-stream".into()),
            )
        } else if !dto.is_edited
            && mime
                .as_deref()
                .is_some_and(|mime| matches!(mime, "image/jpeg" | "image/png" | "image/gif"))
        {
            ("original", mime.expect("checked original image MIME"))
        } else {
            ("display", "image/jpeg".into())
        };

        let duration = dto
            .duration
            .filter(|ms| (0..=i32::MAX as i64).contains(ms))
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
            uri: self.media_url(dto.id, representation),
            mime,
            duration,
            byte_seek: video,
        }];

        if !video {
            resources.push(Resource {
                uri: self.media_url(dto.id, "preview"),
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
                art: Some(self.media_url(dto.id, "preview")),
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
    value.chars().map(|c| {
        if matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}') {
            c
        } else {
            '\u{fffd}'
        }
    }).collect()
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
        serde_json::to_writer(self, value)
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

fn media_type(value: &str) -> Option<String> {
    crate::mime::parse(value).map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::Response;
    use http::Method;
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use crate::immich::tests::{Fake as Api, Received, album, asset, page, reply, version};

    const ALBUM: Uuid = Uuid::from_u128(100_000);

    fn budget() -> Budget {
        Budget {
            deadline: Instant::now() + limits::REFRESH_PREPARATION_TIMEOUT,
            stop: CancellationToken::new(),
        }
    }

    struct Fake {
        source: Source,
        requests: Arc<Mutex<Vec<Received>>>,
        _api: Api,
    }

    impl Fake {
        async fn new(replies: Vec<Response>) -> Self {
            let api = Api::new(replies).await;

            let source = Source::new(
                api.client.clone(),
                "192.0.2.1:8200".parse().unwrap(),
                "Photos".into(),
            );

            Self {
                source,
                requests: api.requests.clone(),
                _api: api,
            }
        }
    }

    fn project(source: &Source, value: Value) -> Item {
        source
            .project(ALBUM, serde_json::from_value(value).unwrap(), &mut 0)
            .unwrap()
            .unwrap()
    }

    async fn processing_boundary(cancel: bool) {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        for stage in 0..6 {
            let replies = match stage {
                0 => vec![version(), reply(json!([]))],
                1 => vec![page(vec![asset(1, "IMAGE")], Some("2")), page(vec![], None)],
                2 => vec![page(vec![asset(1, "VIDEO")], None), page(vec![], None)],

                3 => vec![
                    page(vec![asset(1, "VIDEO")], None),
                    page(vec![asset(1, "VIDEO")], Some("2")),
                    page(vec![], None),
                ],

                4 => vec![version(), reply(json!([album(ALBUM)]))],
                _ => vec![page(vec![asset(1, "IMAGE")], None)],
            };

            let mut fake = Fake::new(replies).await;

            let budget = Budget {
                deadline: Instant::now() + std::time::Duration::from_secs(1),
                ..budget()
            };

            let deadline = budget.deadline;
            let cancelled = budget.stop.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let allowed = if matches!(stage, 3 | 4) { 2 } else { 1 };

            fake.source.client.after_json = Some(Arc::new(move || {
                if observed.fetch_add(1, AtomicOrdering::SeqCst) + 1 == allowed {
                    if cancel {
                        let cancelled = cancelled.clone();

                        std::thread::spawn(move || cancelled.cancel())
                            .join()
                            .unwrap();
                    } else {
                        // Block synchronously, as parsing can, without yielding to a timer.
                        std::thread::sleep(
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        );
                    }
                }
            }));

            let failed = if matches!(stage, 0 | 4) {
                fake.source.root(&budget).await.is_err()
            } else {
                fake.source.contents(ALBUM, &budget).await.is_err()
            };

            assert_eq!(
                fake.requests.lock().unwrap().len(),
                allowed,
                "stage {stage} started a request after processing exhausted the budget"
            );

            assert!(failed);
            assert_eq!(calls.load(AtomicOrdering::SeqCst), allowed);
        }
    }

    #[tokio::test]
    async fn expired_during_processing_does_not_start_next_request() {
        processing_boundary(false).await;
    }

    #[tokio::test]
    async fn cancelled_during_processing_does_not_start_next_request() {
        processing_boundary(true).await;
    }

    #[tokio::test]
    async fn invalid_budget_rejects_snapshot_preparation() {
        for checked in [false, true] {
            for cancel in [false, true] {
                let fake = Fake::new(vec![version(), reply(json!([]))]).await;

                if checked {
                    fake.source.root(&budget()).await.unwrap();
                }

                let mut budget = budget();

                if cancel {
                    budget.stop.cancel();
                } else {
                    budget.deadline = Instant::now();
                }

                let expected = if cancel {
                    "operation cancelled"
                } else {
                    "operation deadline exceeded"
                };

                assert_eq!(
                    fake.source.root(&budget).await.unwrap_err().to_string(),
                    expected
                );

                assert_eq!(
                    fake.source
                        .contents(ALBUM, &budget)
                        .await
                        .unwrap_err()
                        .to_string(),
                    expected
                );

                assert_eq!(
                    fake.requests.lock().unwrap().len(),
                    if checked { 2 } else { 0 }
                );
            }
        }
    }

    #[tokio::test]
    async fn resources_dates_and_optional_hints() {
        let fake = Fake::new(vec![]).await;

        for (mime, edited, representation, expected) in [
            (
                Some("IMAGE/JPEG; quality=90"),
                false,
                "original",
                "image/jpeg",
            ),
            (Some("image/png"), false, "original", "image/png"),
            (Some("image/jpeg; q =90"), false, "original", "image/jpeg"),
            (Some("image/jpeg; q= 90"), false, "original", "image/jpeg"),
            (
                Some("IMAGE/JPEG;; q \t=\t \"90\"; ; x = y;"),
                false,
                "original",
                "image/jpeg",
            ),
            (Some("image/gif"), false, "original", "image/gif"),
            (Some("image/jpeg"), true, "display", "image/jpeg"),
            (Some("image/heic"), false, "display", "image/jpeg"),
            (Some("image/webp"), false, "display", "image/jpeg"),
            (Some("image/jpeg;broken"), false, "display", "image/jpeg"),
            (Some("image/jpeg; q = "), false, "display", "image/jpeg"),
            (
                Some("image/jpeg; q = \"90\"oops"),
                false,
                "display",
                "image/jpeg",
            ),
            (
                Some("image/jpeg; q = \"bad\u{7f}\""),
                false,
                "display",
                "image/jpeg",
            ),
            (None, false, "display", "image/jpeg"),
        ] {
            let mut dto = asset(1, "IMAGE");
            dto["originalMimeType"] = json!(mime);
            dto["isEdited"] = json!(edited);
            dto["originalFileName"] = json!("A\u{0001}&B");
            let item = project(&fake.source, dto);
            assert_eq!(item.object.title, "A\u{fffd}&B");
            assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));
            assert_eq!(
                item.capture.unwrap().to_rfc3339(),
                "2023-12-31T22:30:00+00:00"
            );
            assert_eq!(item.object.resources.len(), 2);
            assert!(item.object.resources[0].uri.ends_with(representation));
            assert_eq!(item.object.resources[0].mime, expected);
            assert!(item.object.resources[1].uri.ends_with("preview"));
            assert!(
                item.object
                    .resources
                    .iter()
                    .all(|r| !r.byte_seek && r.duration.is_none())
            );
        }

        for (duration, expected) in [
            (0, Some("0:00:00.000")),
            (3_661_007, Some("1:01:01.007")),
            (i32::MAX as i64, Some("596:31:23.647")),
            (-1, None),
            (i32::MAX as i64 + 1, None),
        ] {
            let mut dto = asset(1, "VIDEO");
            dto["originalMimeType"] = Value::Null;
            dto["duration"] = json!(duration);
            dto["localDateTime"] = json!("2024-01-01T23:00:00-12:00");
            let item = project(&fake.source, dto);
            assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));
            assert_eq!(item.object.resources[0].duration.as_deref(), expected);
            assert_eq!(item.object.resources[0].mime, "application/octet-stream");
            assert!(item.object.resources[0].byte_seek);
            assert!(item.object.art.unwrap().ends_with("preview"));
        }

        let mut dto = asset(1, "IMAGE");
        dto["originalFileName"] = json!("");
        dto["fileCreatedAt"] = json!("2024-01-01T12:00:00");
        dto["localDateTime"] = json!("not a date");
        dto["checksum"] = json!("one");
        dto["updatedAt"] = json!("opaque hint");
        dto["thumbhash"] = json!("two");
        dto["exifInfo"] = json!(["ignored unsupported structure"]);
        let item = project(&fake.source, dto);
        assert_eq!(item.object.title, Uuid::from_u128(1).to_string());
        assert!(item.capture.is_none() && item.object.date.is_none());
        assert_eq!(item.checksum.as_deref(), Some("one"));
        assert_eq!(item.updated_at.as_deref(), Some("opaque hint"));
        assert_eq!(item.thumbhash.as_deref(), Some("two"));

        for invalid in [Value::Null, json!(123), json!(false), json!({"date": []})] {
            let mut dto = asset(1, "IMAGE");
            dto["fileCreatedAt"] = invalid.clone();
            dto["localDateTime"] = invalid.clone();
            let item = project(&fake.source, dto);
            assert!(item.capture.is_none() && item.object.date.is_none());
            let mut dto = album(ALBUM);
            dto["createdAt"] = invalid;
            let album: crate::immich::Album = serde_json::from_value(dto).unwrap();
            assert!(parse_date(album.created_at.as_deref(), &mut 0).is_none());
        }

        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn complete_pagination_and_scoped_encoded_intersection_without_probes() {
        let mut archived = asset(1001, "VIDEO");
        archived["visibility"] = json!("archive");
        archived["originalMimeType"] = json!("Video/QuickTime");
        let mut changed_encoded = archived.clone();
        changed_encoded["originalFileName"] = json!("changed between searches");

        let fake = Fake::new(vec![
            version(),
            reply(json!([album(ALBUM)])),
            page((1..=1000).map(|id| asset(id, "IMAGE")).collect(), Some("2")),
            page(vec![archived], None),
            page(
                vec![
                    changed_encoded.clone(),
                    changed_encoded,
                    asset(9999, "VIDEO"),
                ],
                None,
            ),
            reply(json!([])),
        ])
        .await;

        let root = fake.source.root(&budget()).await.unwrap();
        assert_eq!(
            root.albums[&ALBUM].object.date.as_deref(),
            Some("2023-12-31")
        );
        assert!(
            root.albums[&ALBUM]
                .object
                .art
                .as_ref()
                .unwrap()
                .contains(&Uuid::from_u128(777).to_string())
        );
        assert!(root.albums[&ALBUM].object.child_count.is_none());
        let contents = fake.source.contents(ALBUM, &budget()).await.unwrap();
        assert_eq!(contents.items.len(), 1001);
        let video = &contents.items[&Uuid::from_u128(1001)].object;
        assert_eq!(video.resources.len(), 2);
        assert_eq!(video.resources[0].mime, "video/quicktime");
        assert_eq!(video.resources[1].mime, "video/mp4");
        assert!(video.resources.iter().all(|r| r.byte_seek));
        assert!(video.resources[1].duration.is_none());
        assert!(
            fake.source
                .clone()
                .root(&budget())
                .await
                .unwrap()
                .albums
                .is_empty()
        );
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests.len(), 6);
        assert_eq!(requests[0].uri, "/prefix/api/server/version");
        assert_eq!(requests[1].uri, "/prefix/api/albums");
        assert_eq!(requests[5].uri, "/prefix/api/albums");

        for (index, expected_page) in [(2, 1), (3, 2), (4, 1)] {
            let request = &requests[index];
            assert_eq!(request.method, Method::POST);
            assert_eq!(request.uri, "/prefix/api/search/metadata");
            assert_eq!(request.body["albumIds"], json!([ALBUM]));
            assert_eq!(request.body["page"], json!(expected_page));
            assert_eq!(request.body["size"], json!(limits::SEARCH_PAGE_SIZE));
            assert_eq!(request.body["withDeleted"], false);
            assert_eq!(request.body["withExif"], false);
            assert_eq!(request.body["withPeople"], false);
            assert!(request.body.get("withStacked").is_none());
            assert_eq!(
                request.body.get("isEncoded"),
                (index == 4).then_some(&Value::Bool(true))
            );
            assert_eq!(
                request.body.get("type"),
                (index == 4).then_some(&json!("VIDEO"))
            );
        }

        assert!(
            requests
                .iter()
                .all(|request| request.headers["x-api-key"] == "private-test-key")
        );
    }

    #[tokio::test]
    async fn eligible_assets_require_original_file_name() {
        for null in [false, true] {
            let mut dto = asset(1, "IMAGE");

            if null {
                dto["originalFileName"] = Value::Null;
            } else {
                dto.as_object_mut().unwrap().remove("originalFileName");
            }

            let fake = Fake::new(vec![page(vec![dto], None)]).await;

            assert_eq!(
                fake.source
                    .contents(ALBUM, &budget())
                    .await
                    .unwrap_err()
                    .to_string(),
                "eligible Immich asset is missing originalFileName"
            );
        }
    }

    #[tokio::test]
    async fn duplicates_compare_only_normalized_member_data_and_eligibility() {
        let first = asset(1, "IMAGE");

        for field in [
            "originalFileName",
            "checksum",
            "updatedAt",
            "thumbhash",
            "localDateTime",
            "fileCreatedAt",
            "isEdited",
            "visibility",
            "isTrashed",
            "type",
        ] {
            let mut second = first.clone();

            second[field] = match field {
                "isEdited" | "isTrashed" => json!(true),
                "visibility" => json!("hidden"),
                "type" => json!("VIDEO"),
                "localDateTime" | "fileCreatedAt" => json!("2025-01-01T00:00:00Z"),
                _ => json!("changed"),
            };

            for reversed in [false, true] {
                let records = if reversed {
                    vec![second.clone(), first.clone()]
                } else {
                    vec![first.clone(), second.clone()]
                };
                let fake = Fake::new(vec![page(records, None)]).await;
                assert!(
                    fake.source.contents(ALBUM, &budget()).await.is_err(),
                    "{field}"
                );
            }
        }

        let mut irrelevant = first.clone();
        irrelevant["originalPath"] = json!("private ignored path");
        irrelevant["visibility"] = json!("archive");
        let mut excluded = asset(2, "VIDEO");
        excluded["visibility"] = json!("future-visibility");
        excluded.as_object_mut().unwrap().remove("originalFileName");
        let mut excluded_changed = excluded.clone();
        excluded_changed["originalFileName"] = json!("irrelevant name".repeat(1000));
        excluded_changed["checksum"] = json!("irrelevant hint".repeat(1000));
        excluded_changed["isEdited"] = json!(true);
        let fake = Fake::new(vec![page(
            vec![first, irrelevant, excluded, excluded_changed],
            None,
        )])
        .await;
        let result = fake.source.contents(ALBUM, &budget()).await.unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn root_duplicates_titles_covers_and_canonical_digests() {
        let mut empty = album(ALBUM);
        empty["albumName"] = json!("");
        empty["albumThumbnailAssetId"] = Value::Null;
        let other = album(Uuid::from_u128(2));

        let fake = Fake::new(vec![
            version(),
            reply(json!([empty.clone(), other.clone(), empty.clone()])),
            reply(json!([other, empty.clone()])),
            reply(json!([empty.clone(), {"id": ALBUM, "albumName": "changed"}])),
            reply(json!([{"id": ALBUM}])),
            reply(json!([{"id": ALBUM, "albumName": "valid", "albumThumbnailAssetId": "invalid"}])),
        ])
        .await;

        let first = fake.source.root(&budget()).await.unwrap();
        let second = fake.source.root(&budget()).await.unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.albums[&ALBUM].object.title, ALBUM.to_string());
        assert!(first.albums[&ALBUM].object.art.is_none());

        for _ in 0..3 {
            assert!(fake.source.root(&budget()).await.is_err());
        }

        let renamed = Fake::new(vec![version(), reply(json!([empty.clone()]))]).await;
        let baseline = renamed.source.root(&budget()).await.unwrap();
        let mut changed = Fake::new(vec![version(), reply(json!([empty]))]).await;
        changed.source.friendly_name = "Another title".into();
        let changed = changed.source.root(&budget()).await.unwrap();
        assert_ne!(baseline.digest, changed.digest);
        assert_eq!(
            baseline.albums[&ALBUM].digest,
            changed.albums[&ALBUM].digest
        );
    }

    #[tokio::test]
    async fn projection_hashes_hints_capture_resource_order_and_exact_bytes() {
        let mut first = asset(2, "IMAGE");
        first["originalFileName"] = json!("Zażółć \"photo\"\\name\n.jpg");
        let second = asset(1, "IMAGE");

        let fake = Fake::new(vec![
            page(vec![first.clone(), second.clone()], None),
            page(vec![second.clone(), first.clone(), second], None),
        ])
        .await;

        let baseline = fake.source.contents(ALBUM, &budget()).await.unwrap();
        let reordered = fake.source.contents(ALBUM, &budget()).await.unwrap();
        assert_eq!(baseline.digest, reordered.digest);
        assert_eq!(baseline.bytes, reordered.bytes);
        let serialized = serde_json::to_vec(&baseline.items.values().collect::<Vec<_>>()).unwrap();
        assert_eq!(baseline.bytes, serialized.len());
        assert_eq!(
            baseline.digest,
            format!("{:x}", Sha256::digest(&serialized))
        );
        assert_eq!(baseline.digest.len(), 64);

        let item = baseline.items.values().next().unwrap();
        let mut original = Projection::new(limits::SNAPSHOT_BYTES);
        original.json(item).unwrap();
        let original = original.finish().0;

        for field in [
            "checksum",
            "updatedAt",
            "thumbhash",
            "fileCreatedAt",
            "isEdited",
        ] {
            let mut changed = asset(1, "IMAGE");

            changed[field] = match field {
                "fileCreatedAt" => json!("2024-01-01T00:30:01+02:00"),
                "isEdited" => json!(true),
                _ => json!("changed"),
            };

            let changed = project(&fake.source, changed);
            let mut digest = Projection::new(limits::SNAPSHOT_BYTES);
            digest.json(&changed).unwrap();
            assert_ne!(original, digest.finish().0, "{field}");
        }

        let mut reversed = item.clone();
        reversed.object.resources.reverse();
        let mut digest = Projection::new(limits::SNAPSHOT_BYTES);
        digest.json(&reversed).unwrap();
        assert_ne!(original, digest.finish().0);

        for item in baseline.items.values() {
            let size = serde_json::to_vec(item).unwrap().len();
            assert_eq!(encoded_size(item, limits::SNAPSHOT_BYTES).unwrap(), size);
            assert_eq!(encoded_size(item, size).unwrap(), size);
            assert!(encoded_size(item, size - 1).is_err());
        }
    }

    #[tokio::test]
    async fn traversal_progress_and_combined_page_budget() {
        for repeated in [false, true] {
            let mut replies = vec![page(vec![asset(1, "IMAGE")], Some("2"))];
            replies.push(page(
                if repeated {
                    vec![asset(1, "IMAGE")]
                } else {
                    vec![]
                },
                Some("3"),
            ));
            let fake = Fake::new(replies).await;
            assert!(fake.source.contents(ALBUM, &budget()).await.is_err());
            assert_eq!(fake.requests.lock().unwrap().len(), 2);
        }

        for finish in [false, true] {
            let mut replies = vec![page(vec![asset(1, "VIDEO")], None)];

            for number in 1..limits::SEARCH_PAGES {
                let next = (number + 1).to_string();
                replies.push(page(
                    vec![asset(number as u128, "VIDEO")],
                    if finish && number == limits::SEARCH_PAGES - 1 {
                        None
                    } else {
                        Some(&next)
                    },
                ));
            }

            let fake = Fake::new(replies).await;
            assert_eq!(fake.source.contents(ALBUM, &budget()).await.is_ok(), finish);
            assert_eq!(fake.requests.lock().unwrap().len(), limits::SEARCH_PAGES);
        }
    }

    #[tokio::test]
    async fn raw_record_item_album_and_projected_payload_limits() {
        let excluded = json!({"id": Uuid::from_u128(1), "type": "AUDIO", "visibility": "hidden", "isTrashed": false, "isEdited": false});

        // Raw records count even when identical and excluded; pages need not be full.
        let fake = Fake::new(vec![page(vec![excluded; limits::SEARCH_RECORDS + 1], None)]).await;
        assert!(
            fake.source
                .contents(ALBUM, &budget())
                .await
                .unwrap_err()
                .to_string()
                .contains("record limit")
        );

        for extra in [0, 1] {
            let encoded = json!({"id": Uuid::from_u128(1), "type": "VIDEO", "visibility": "timeline", "isTrashed": false, "isEdited": false});

            let fake = Fake::new(vec![
                page(vec![asset(1, "VIDEO")], None),
                page(vec![encoded; limits::SEARCH_RECORDS - 1 + extra], None),
            ])
            .await;

            assert_eq!(
                fake.source.contents(ALBUM, &budget()).await.is_ok(),
                extra == 0
            );
            assert_eq!(fake.requests.lock().unwrap().len(), 2);
        }

        let mut replies = Vec::new();

        for page_number in 0..=limits::ALBUM_ITEMS / limits::SEARCH_PAGE_SIZE {
            let start = page_number * limits::SEARCH_PAGE_SIZE + 1;
            let count = if start > limits::ALBUM_ITEMS {
                1
            } else {
                limits::SEARCH_PAGE_SIZE
            };
            let next = (page_number + 2).to_string();
            replies.push(page(
                (start..start + count)
                    .map(|id| asset(id as u128, "IMAGE"))
                    .collect(),
                (count != 1).then_some(next.as_str()),
            ));
        }

        let fake = Fake::new(replies).await;
        let error = fake
            .source
            .contents(ALBUM, &budget())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("item limit"), "{error}");

        let fake = Fake::new(vec![
            version(),
            reply(json!(
                (0..=limits::CURRENT_ALBUMS)
                    .map(|id| album(Uuid::from_u128(id as u128)))
                    .collect::<Vec<_>>()
            )),
        ])
        .await;
        assert!(
            fake.source
                .root(&budget())
                .await
                .unwrap_err()
                .to_string()
                .contains("album limit")
        );

        let mut big = asset(1, "IMAGE");
        big["checksum"] = json!("x".repeat(limits::SNAPSHOT_BYTES / 2));
        let mut bigger = big.clone();
        bigger["id"] = json!(Uuid::from_u128(2));
        let fake = Fake::new(vec![page(vec![big], Some("2")), page(vec![bigger], None)]).await;
        assert!(
            fake.source
                .contents(ALBUM, &budget())
                .await
                .unwrap_err()
                .to_string()
                .contains("byte limit")
        );
    }

    #[test]
    fn mime_normalization_accepts_parameter_whitespace_but_rejects_garbage() {
        for value in [
            " IMAGE/JPEG ; q=90",
            "image/jpeg; q =90",
            "image/jpeg; q= 90",
            "IMAGE/JPEG;; q \t=\t \"90\"; ; x = y;",
        ] {
            assert_eq!(
                media_type(value).as_deref(),
                Some("image/jpeg"),
                "{value:?}"
            );
        }

        for value in [
            "image/jpeg; q = ",
            "image/jpeg; q = \"90\"oops",
            "image/jpeg; q\n=90",
            "image/jpeg; q=\n90",
            "image/jpeg; q = \"bad\u{7f}\"",
        ] {
            assert_eq!(media_type(value), None, "{value:?}");
        }
    }
}
