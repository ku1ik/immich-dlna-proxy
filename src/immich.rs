use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::SocketAddrV4,
    sync::Arc,
};

use anyhow::{Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use http::{HeaderValue, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::{sync::OnceCell, time::Instant};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    deadline, limits,
    protocol::{Fault, Object, Resource},
};

/// One caller-owned preparation budget, shared by every request in a refresh.
#[derive(Clone)]
pub struct FetchBudget {
    pub deadline: Instant,
    pub stop: CancellationToken,
}

impl FetchBudget {
    fn check(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled(),
            "Immich catalog refresh cancelled"
        );

        ensure!(
            Instant::now() < self.deadline,
            "Immich catalog refresh preparation deadline exceeded"
        );

        Ok(())
    }
}

#[derive(Clone)]
pub struct Immich {
    client: reqwest::Client,
    api_base: Url,
    api_key: HeaderValue,
    http_address: SocketAddrV4,
    friendly_name: String,
    version_checked: Arc<OnceCell<()>>,
    #[cfg(test)]
    after_json: Option<Arc<dyn Fn() + Send + Sync>>,
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

pub fn compare_dates(
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

#[derive(Deserialize)]
struct Version {
    major: u64,
    minor: u64,
    patch: u64,
    #[serde(deserialize_with = "required_nullable")]
    prerelease: Option<u64>,
}

// Unlike Option's default field handling, these API fields must be present.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

fn optional_date<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Date {
        Text(String),
        Invalid(serde::de::IgnoredAny),
    }

    match Date::deserialize(deserializer)? {
        Date::Text(value) => Ok(Some(value)),
        Date::Invalid(_) => Ok(None),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlbumDto {
    id: Uuid,
    album_name: String,
    #[serde(default, deserialize_with = "optional_date")]
    created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    end_date: Option<String>,
    album_thumbnail_asset_id: Option<Uuid>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetDto {
    id: Uuid,
    #[serde(rename = "type")]
    kind: String,
    visibility: String,
    is_trashed: bool,
    is_edited: bool,
    original_file_name: Option<String>,
    original_mime_type: Option<String>,
    duration: Option<i64>,
    #[serde(default, deserialize_with = "optional_date")]
    file_created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    local_date_time: Option<String>,
    checksum: Option<String>,
    updated_at: Option<String>,
    thumbhash: Option<String>,
}

#[derive(Deserialize)]
struct SearchResponse {
    assets: SearchAssets,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchAssets {
    items: Vec<AssetDto>,
    #[serde(deserialize_with = "required_nullable")]
    next_page: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Search {
    album_ids: [Uuid; 1],
    page: usize,
    size: usize,
    with_deleted: bool,
    with_exif: bool,
    with_people: bool,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_encoded: Option<bool>,
}

impl Immich {
    pub fn new(
        api_base: Url,
        mut api_key: HeaderValue,
        http_address: SocketAddrV4,
        friendly_name: String,
    ) -> Result<Self> {
        ensure!(
            matches!(api_base.scheme(), "http" | "https")
                && api_base.host_str().is_some()
                && api_base.username().is_empty()
                && api_base.password().is_none()
                && api_base.query().is_none()
                && api_base.fragment().is_none()
                && api_base.path().ends_with("/api/"),
            "catalog requires a normalized HTTP(S) API directory"
        );

        api_key.set_sensitive(true);

        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(limits::CONNECT_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("cannot initialize catalog HTTP client"))?;

        Ok(Self {
            client,
            api_base,
            api_key,
            http_address,
            friendly_name: xml_text(&friendly_name),
            version_checked: Arc::new(OnceCell::new()),
            #[cfg(test)]
            after_json: None,
        })
    }

    async fn json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        budget: &FetchBudget,
    ) -> Result<T> {
        let request = request
            .header("x-api-key", self.api_key.clone())
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity");

        // A cooperative outer timeout cannot interrupt synchronous preparation.
        // Recheck immediately before allowing another HTTP request to start.
        budget.check()?;

        let header_deadline = budget
            .deadline
            .min(Instant::now() + limits::UPSTREAM_HEADER_TIMEOUT);

        let response = deadline::timeout_at(header_deadline, async { request.send().await }).await;

        budget.check()?;

        let mut response = response
            .map_err(|_| anyhow!("Immich catalog response-header deadline exceeded"))?
            .map_err(|_| anyhow!("Immich catalog connection failed"))?;

        ensure!(
            response.status().is_success(),
            "Immich catalog returned HTTP {}; check availability and API-key permissions",
            response.status().as_u16()
        );

        ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= limits::JSON_BYTES as u64),
            "Immich JSON response exceeds byte limit"
        );

        let mut body = Vec::new();

        loop {
            budget.check()?;

            let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| anyhow!("Immich catalog body read failed"))?
            else {
                break;
            };

            budget.check()?;

            ensure!(
                chunk.len() <= limits::JSON_BYTES - body.len(),
                "Immich JSON response exceeds byte limit"
            );

            body.extend_from_slice(&chunk);
        }

        budget.check()?;

        let parsed = serde_json::from_slice(&body).map_err(|_| {
            anyhow!("invalid Immich catalog JSON structure; requires Immich 3.1.0 or newer")
        })?;

        #[cfg(test)]
        if let Some(after_json) = &self.after_json {
            after_json();
        }

        budget.check()?;

        Ok(parsed)
    }

    /// The caller admits and bounds the whole refresh, including version checking.
    pub async fn root(&self, budget: &FetchBudget) -> Result<Root> {
        budget.check()?;

        self.version_checked
            .get_or_try_init(|| async {
                let version: Version = self
                    .json(
                        self.client.get(self.api_base.join("server/version")?),
                        budget,
                    )
                    .await?;

                ensure!(
                    (version.major, version.minor, version.patch) > (3, 1, 0)
                        || ((version.major, version.minor, version.patch) == (3, 1, 0)
                            && version.prerelease.is_none()),
                    "Immich 3.1.0 or newer is required; upgrade the upstream server"
                );

                budget.check()?;

                Ok::<_, anyhow::Error>(())
            })
            .await?;

        let records: Vec<AlbumDto> = self
            .json(self.client.get(self.api_base.join("albums")?), budget)
            .await?;

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
            album.digest = projection.finish().0;

            if let Some(previous) = albums.get(&album.id) {
                ensure!(previous == &album, "conflicting duplicate Immich album");
                continue;
            }

            ensure!(
                albums.len() < limits::CURRENT_ALBUMS,
                "Immich root exceeds album limit"
            );

            bytes += 1 + encoded_size(&album, limits::SNAPSHOT_BYTES - bytes)?;

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
    pub async fn contents(&self, album: Uuid, budget: &FetchBudget) -> Result<Contents> {
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

                let query = Search {
                    album_ids: [album],
                    page,
                    size: limits::SEARCH_PAGE_SIZE,
                    with_deleted: false,
                    with_exif: false,
                    with_people: false,
                    kind: encoded.then_some("VIDEO"),
                    is_encoded: encoded.then_some(true),
                };

                let result: SearchResponse = self
                    .json(
                        self.client
                            .post(self.api_base.join("search/metadata")?)
                            .json(&query),
                        budget,
                    )
                    .await?;

                ensure!(
                    result.assets.items.len() <= limits::SEARCH_RECORDS - records,
                    "Immich album record limit exceeded"
                );

                records += result.assets.items.len();
                let mut advanced = false;

                for dto in result.assets.items {
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

                let Some(next) = result.assets.next_page else {
                    break;
                };

                ensure!(
                    next == (page + 1).to_string(),
                    "invalid Immich search nextPage; expected the next numeric page string"
                );

                ensure!(advanced, "Immich search continuation made no progress");
                page += 1;
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

    fn project(&self, album: Uuid, dto: AssetDto, bad_dates: &mut usize) -> Result<Option<Item>> {
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

// Hash and count the same canonical JSON without allocating a serialized snapshot.
struct Projection {
    hash: Sha256,
    bytes: usize,
    limit: usize,
}

impl Projection {
    fn new(limit: usize) -> Self {
        Self {
            hash: Sha256::new(),
            bytes: 0,
            limit,
        }
    }

    fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(self, value)
            .map_err(|_| anyhow!("catalog projected byte limit exceeded"))
    }

    fn finish(self) -> (String, usize) {
        (format!("{:x}", self.hash.finalize()), self.bytes)
    }
}

impl Write for Projection {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes {
            return Err(std::io::Error::other(
                "catalog projected byte limit exceeded",
            ));
        }

        self.hash.update(bytes);
        self.bytes += bytes.len();

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encoded_size(value: &impl Serialize, limit: usize) -> Result<usize> {
    let mut projection = Projection::new(limit);
    projection.json(value)?;

    Ok(projection.bytes)
}

fn media_type(value: &str) -> Option<String> {
    crate::mime::parse(value).map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{HeaderMap, Method, Request, StatusCode},
        response::Response,
    };
    use serde_json::{Value, json};
    use std::{collections::VecDeque, sync::Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    const ALBUM: Uuid = Uuid::from_u128(100_000);

    fn budget() -> FetchBudget {
        FetchBudget {
            deadline: Instant::now() + limits::REFRESH_PREPARATION_TIMEOUT,
            stop: CancellationToken::new(),
        }
    }

    struct Received {
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: Value,
    }

    struct Fake {
        immich: Immich,
        requests: Arc<Mutex<Vec<Received>>>,
        task: JoinHandle<()>,
    }

    impl Fake {
        async fn new(replies: Vec<Response>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured = requests.clone();
            let replies = Arc::new(Mutex::new(VecDeque::from(replies)));

            let router = Router::new().fallback(move |request: Request<Body>| {
                let captured = captured.clone();
                let replies = replies.clone();

                async move {
                    let (parts, body) = request.into_parts();
                    let body = to_bytes(body, 8192).await.unwrap();

                    captured.lock().unwrap().push(Received {
                        method: parts.method,
                        uri: parts.uri.to_string(),
                        headers: parts.headers,
                        body: if body.is_empty() {
                            Value::Null
                        } else {
                            serde_json::from_slice(&body).unwrap()
                        },
                    });

                    replies.lock().unwrap().pop_front().unwrap_or_else(|| {
                        Response::builder().status(500).body(Body::empty()).unwrap()
                    })
                }
            });

            let task = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });

            let immich = Immich::new(
                Url::parse(&format!("http://{address}/prefix/api/")).unwrap(),
                HeaderValue::from_static("private-test-key"),
                "192.0.2.1:8200".parse().unwrap(),
                "Photos".into(),
            )
            .unwrap();

            Self {
                immich,
                requests,
                task,
            }
        }
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn reply(value: Value) -> Response {
        Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .unwrap()
    }

    fn version() -> Response {
        reply(json!({"major": 3, "minor": 1, "patch": 0, "prerelease": null}))
    }

    fn album(id: Uuid) -> Value {
        json!({"id": id, "albumName": "Album", "createdAt": "2024-01-01T00:30:00+02:00", "albumThumbnailAssetId": Uuid::from_u128(777)})
    }

    fn asset(id: u128, kind: &str) -> Value {
        json!({
            "id": Uuid::from_u128(id), "type": kind, "visibility": "timeline",
            "isTrashed": false, "isEdited": false, "originalFileName": "photo.jpg",
            "originalMimeType": "image/jpeg", "duration": null,
            "fileCreatedAt": "2024-01-01T00:30:00+02:00",
            "localDateTime": "2024-01-01T00:30:00Z"
        })
    }

    fn page(items: Vec<Value>, next: Option<&str>) -> Response {
        reply(json!({"assets": {"items": items, "nextPage": next, "total": 0, "count": 0}}))
    }

    fn project(immich: &Immich, value: Value) -> Item {
        immich
            .project(ALBUM, serde_json::from_value(value).unwrap(), &mut 0)
            .unwrap()
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn ready_headers_before_at_and_after_header_or_preparation_deadline() {
        use std::{future::Future, task::Wake, time::Duration};

        use futures_util::FutureExt;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            sync::Notify,
        };

        struct HeaderWake(Notify);

        impl Wake for HeaderWake {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
        }

        // Let loopback I/O progress without auto-advancing the paused clock.
        let clock_guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });

        for preparation in [Duration::from_secs(10), limits::REFRESH_PREPARATION_TIMEOUT] {
            let limit = preparation.min(limits::UPSTREAM_HEADER_TIMEOUT);

            for elapsed in [
                limit - Duration::from_secs(1),
                limit,
                limit + Duration::from_secs(1),
            ] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

                let immich = Immich::new(
                    format!("http://{}/api/", listener.local_addr().unwrap())
                        .parse()
                        .unwrap(),
                    HeaderValue::from_static("private-test-key"),
                    "192.0.2.1:8200".parse().unwrap(),
                    "Photos".into(),
                )
                .unwrap();

                let started = Instant::now();

                let budget = FetchBudget {
                    deadline: started + preparation,
                    ..budget()
                };

                let request = immich.json::<Value>(
                    immich.client.get(immich.api_base.join("albums").unwrap()),
                    &budget,
                );

                tokio::pin!(request);

                let mut socket = tokio::select! {
                    _ = &mut request => panic!("request ended before headers"),
                    accepted = listener.accept() => accepted.unwrap().0,
                };

                let read_request = async {
                    let mut bytes = Vec::new();

                    while !bytes.ends_with(b"\r\n\r\n") {
                        bytes.push(socket.read_u8().await.unwrap());
                    }
                };

                tokio::select! {
                    _ = &mut request => panic!("request ended before headers"),
                    _ = read_request => {}
                }

                let wake = Arc::new(HeaderWake(Notify::new()));
                let waker = std::task::Waker::from(wake.clone());
                let mut context = std::task::Context::from_waker(&waker);
                assert!(request.as_mut().poll(&mut context).is_pending());
                let _ = wake.0.notified().now_or_never();

                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n[]")
                    .await
                    .unwrap();

                wake.0.notified().await;
                assert_eq!(Instant::now(), started);
                tokio::time::advance(elapsed).await;
                let result = request.await;

                if elapsed < limit {
                    assert_eq!(result.unwrap(), json!([]));
                } else {
                    let expected = if preparation <= limits::UPSTREAM_HEADER_TIMEOUT {
                        "Immich catalog refresh preparation deadline exceeded"
                    } else {
                        "Immich catalog response-header deadline exceeded"
                    };

                    assert_eq!(result.unwrap_err().to_string(), expected);
                }
            }
        }

        clock_guard.abort();
        let _ = clock_guard.await;
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

            let budget = FetchBudget {
                deadline: Instant::now() + std::time::Duration::from_secs(1),
                ..budget()
            };

            let deadline = budget.deadline;
            let cancelled = budget.stop.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let allowed = if matches!(stage, 3 | 4) { 2 } else { 1 };

            fake.immich.after_json = Some(Arc::new(move || {
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
                fake.immich.root(&budget).await.is_err()
            } else {
                fake.immich.contents(ALBUM, &budget).await.is_err()
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
    async fn invalid_budget_rejects_entry_cache_lookup_and_http_send() {
        for checked in [false, true] {
            for cancel in [false, true] {
                let fake = Fake::new(vec![version(), reply(json!([]))]).await;

                if checked {
                    fake.immich.root(&budget()).await.unwrap();
                }

                let mut budget = budget();

                if cancel {
                    budget.stop.cancel();
                } else {
                    budget.deadline = Instant::now();
                }

                let expected = if cancel {
                    "Immich catalog refresh cancelled"
                } else {
                    "Immich catalog refresh preparation deadline exceeded"
                };

                assert_eq!(
                    fake.immich.root(&budget).await.unwrap_err().to_string(),
                    expected
                );

                assert_eq!(
                    fake.immich
                        .contents(ALBUM, &budget)
                        .await
                        .unwrap_err()
                        .to_string(),
                    expected
                );

                let response: Result<Version> = fake
                    .immich
                    .json(
                        fake.immich
                            .client
                            .get(fake.immich.api_base.join("server/version").unwrap()),
                        &budget,
                    )
                    .await;

                assert_eq!(response.err().unwrap().to_string(), expected);

                assert_eq!(
                    fake.requests.lock().unwrap().len(),
                    if checked { 2 } else { 0 }
                );

                assert_eq!(fake.immich.version_checked.get().is_some(), checked);
            }
        }
    }

    #[test]
    fn ids_and_date_ordering() {
        let asset = Uuid::from_u128(0xabcdef);
        let id = format!("album:{ALBUM}:asset:{asset}");
        assert_eq!(
            parse_id(
                &id.to_uppercase()
                    .replacen("ALBUM", "album", 1)
                    .replacen("ASSET", "asset", 1)
            ),
            Ok(ObjectId::Item {
                album: ALBUM,
                asset
            })
        );
        assert_eq!(parse_id("0"), Ok(ObjectId::Root));
        assert_eq!(
            parse_id(&format!("album:{ALBUM}")),
            Ok(ObjectId::Album(ALBUM))
        );

        for id in [
            "",
            "00",
            "0:",
            "asset:bad",
            "album:bad",
            "album:0:asset:0",
            &format!("album:{ALBUM}:"),
            &format!("{id}:extra"),
            &format!("album:{ALBUM}:asset:"),
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
            let item = project(&fake.immich, dto);
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
            let item = project(&fake.immich, dto);
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
        let item = project(&fake.immich, dto);
        assert_eq!(item.object.title, Uuid::from_u128(1).to_string());
        assert!(item.capture.is_none() && item.object.date.is_none());
        assert_eq!(item.checksum.as_deref(), Some("one"));
        assert_eq!(item.updated_at.as_deref(), Some("opaque hint"));
        assert_eq!(item.thumbhash.as_deref(), Some("two"));

        for invalid in [Value::Null, json!(123), json!(false), json!({"date": []})] {
            let mut dto = asset(1, "IMAGE");
            dto["fileCreatedAt"] = invalid.clone();
            dto["localDateTime"] = invalid.clone();
            let item = project(&fake.immich, dto);
            assert!(item.capture.is_none() && item.object.date.is_none());
            let mut dto = album(ALBUM);
            dto["createdAt"] = invalid;
            let album: AlbumDto = serde_json::from_value(dto).unwrap();
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

        let root = fake.immich.root(&budget()).await.unwrap();
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
        let contents = fake.immich.contents(ALBUM, &budget()).await.unwrap();
        assert_eq!(contents.items.len(), 1001);
        let video = &contents.items[&Uuid::from_u128(1001)].object;
        assert_eq!(video.resources.len(), 2);
        assert_eq!(video.resources[0].mime, "video/quicktime");
        assert_eq!(video.resources[1].mime, "video/mp4");
        assert!(video.resources.iter().all(|r| r.byte_seek));
        assert!(video.resources[1].duration.is_none());
        assert!(
            fake.immich
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
        assert!(fake.immich.api_key.is_sensitive());
    }

    #[tokio::test]
    async fn versions_retry_only_failed_checks_and_never_retry_requests() {
        for value in [
            json!({"major": 3, "minor": 0, "patch": 99, "prerelease": null}),
            json!({"major": 3, "minor": 1, "patch": 0, "prerelease": 1}),
            json!({"major": -1, "minor": 1, "patch": 0, "prerelease": null}),
            json!({"major": 3, "minor": 1, "patch": 0.5, "prerelease": null}),
            json!({"major": 3, "minor": 1, "patch": 0, "prerelease": "rc"}),
            json!({"major": 3, "minor": 1}),
        ] {
            let fake = Fake::new(vec![
                reply(value),
                version(),
                reply(json!([])),
                reply(json!([])),
            ])
            .await;
            assert!(fake.immich.root(&budget()).await.is_err());
            assert_eq!(fake.requests.lock().unwrap().len(), 1);
            fake.immich.root(&budget()).await.unwrap();
            fake.immich.clone().root(&budget()).await.unwrap();
            assert_eq!(fake.requests.lock().unwrap().len(), 4);
        }

        let fake = Fake::new(vec![
            reply(json!({"major": 4, "minor": 0, "patch": 0, "prerelease": null, "extra": []})),
            Response::builder()
                .status(403)
                .body(Body::from("private upstream body"))
                .unwrap(),
            reply(json!([])),
        ])
        .await;

        let error = format!("{:#}", fake.immich.root(&budget()).await.unwrap_err());
        assert!(error.contains("403"));
        assert!(!error.contains("private") && !error.contains("http://"));
        fake.immich.root(&budget()).await.unwrap();
        assert_eq!(fake.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn redirects_are_rejected_at_every_catalog_endpoint() {
        let target = Fake::new(vec![reply(json!([]))]).await;

        for stage in 0..3 {
            for status in [301, 302, 303, 307, 308] {
                let mut replies = Vec::new();

                if stage > 0 {
                    replies.push(version());
                }

                if stage > 1 {
                    replies.push(reply(json!([album(ALBUM)])));
                }

                replies.push(
                    Response::builder()
                        .status(status)
                        .header(header::LOCATION, target.immich.api_base.as_str())
                        .body(Body::empty())
                        .unwrap(),
                );
                let fake = Fake::new(replies).await;

                if stage == 2 {
                    fake.immich.root(&budget()).await.unwrap();
                    assert!(fake.immich.contents(ALBUM, &budget()).await.is_err());
                } else {
                    assert!(fake.immich.root(&budget()).await.is_err());
                }

                assert_eq!(fake.requests.lock().unwrap().len(), stage + 1);
            }
        }

        assert!(target.requests.lock().unwrap().is_empty());

        let fake = Fake::new(vec![
            Response::builder()
                .status(307)
                .header(header::LOCATION, "/prefix/api/albums")
                .body(Body::empty())
                .unwrap(),
        ])
        .await;
        assert!(fake.immich.root(&budget()).await.is_err());
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn critical_structure_is_required_but_additive_fields_are_ignored() {
        for field in [
            "id",
            "type",
            "visibility",
            "isTrashed",
            "isEdited",
            "originalFileName",
        ] {
            for null in [false, true] {
                let mut dto = asset(1, "IMAGE");

                if null {
                    dto[field] = Value::Null;
                } else {
                    dto.as_object_mut().unwrap().remove(field);
                }

                let fake = Fake::new(vec![page(vec![dto], None)]).await;
                assert!(
                    fake.immich.contents(ALBUM, &budget()).await.is_err(),
                    "{field}"
                );
            }
        }

        for field in [
            "checksum",
            "updatedAt",
            "thumbhash",
            "originalMimeType",
            "duration",
        ] {
            let mut dto = asset(1, "IMAGE");
            dto[field] = json!({"wrong": "structure"});
            let fake = Fake::new(vec![page(vec![dto], None)]).await;
            assert!(
                fake.immich.contents(ALBUM, &budget()).await.is_err(),
                "{field}"
            );
        }

        for response in [
            json!({"assets": {"items": []}}),
            json!({"assets": {"items": [], "nextPage": 2}}),
            json!({"assets": {"nextPage": null}}),
            json!({"assets": []}),
            json!({"albums": []}),
        ] {
            let fake = Fake::new(vec![reply(response)]).await;
            assert!(fake.immich.contents(ALBUM, &budget()).await.is_err());
        }

        let mut dto = asset(1, "IMAGE");

        for field in [
            "checksum",
            "updatedAt",
            "thumbhash",
            "originalMimeType",
            "fileCreatedAt",
            "localDateTime",
            "duration",
        ] {
            dto.as_object_mut().unwrap().remove(field);
        }

        dto["stack"] = json!({"unexpected": [1, 2, 3]});
        let fake = Fake::new(vec![page(vec![dto], None)]).await;
        let result = fake.immich.contents(ALBUM, &budget()).await.unwrap();
        assert_eq!(result.items.len(), 1);
        assert!(result.items.values().next().unwrap().capture.is_none());
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
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
                    fake.immich.contents(ALBUM, &budget()).await.is_err(),
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
        let result = fake.immich.contents(ALBUM, &budget()).await.unwrap();
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

        let first = fake.immich.root(&budget()).await.unwrap();
        let second = fake.immich.root(&budget()).await.unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.albums[&ALBUM].object.title, ALBUM.to_string());
        assert!(first.albums[&ALBUM].object.art.is_none());

        for _ in 0..3 {
            assert!(fake.immich.root(&budget()).await.is_err());
        }

        let renamed = Fake::new(vec![version(), reply(json!([empty.clone()]))]).await;
        let baseline = renamed.immich.root(&budget()).await.unwrap();
        let mut changed = Fake::new(vec![version(), reply(json!([empty]))]).await;
        changed.immich.friendly_name = "Another title".into();
        let changed = changed.immich.root(&budget()).await.unwrap();
        assert_ne!(baseline.digest, changed.digest);
        assert_eq!(
            baseline.albums[&ALBUM].digest,
            changed.albums[&ALBUM].digest
        );
    }

    #[tokio::test]
    async fn projection_hashes_hints_capture_resource_order_and_exact_bytes() {
        let first = asset(2, "IMAGE");
        let second = asset(1, "IMAGE");

        let fake = Fake::new(vec![
            page(vec![first.clone(), second.clone()], None),
            page(vec![second.clone(), first.clone(), second], None),
        ])
        .await;

        let baseline = fake.immich.contents(ALBUM, &budget()).await.unwrap();
        let reordered = fake.immich.contents(ALBUM, &budget()).await.unwrap();
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

            let changed = project(&fake.immich, changed);
            let mut digest = Projection::new(limits::SNAPSHOT_BYTES);
            digest.json(&changed).unwrap();
            assert_ne!(original, digest.finish().0, "{field}");
        }

        let mut reversed = item.clone();
        reversed.object.resources.reverse();
        let mut digest = Projection::new(limits::SNAPSHOT_BYTES);
        digest.json(&reversed).unwrap();
        assert_ne!(original, digest.finish().0);
        let size = encoded_size(item, limits::SNAPSHOT_BYTES).unwrap();
        assert_eq!(encoded_size(item, size).unwrap(), size);
        assert!(encoded_size(item, size - 1).is_err());
    }

    #[tokio::test]
    async fn pagination_contract_and_combined_page_budget() {
        for next in ["", "1", "3", "02", "+2", "2 ", "18446744073709551616"] {
            let fake = Fake::new(vec![page(vec![asset(1, "IMAGE")], Some(next))]).await;
            assert!(
                fake.immich.contents(ALBUM, &budget()).await.is_err(),
                "{next}"
            );
            assert_eq!(fake.requests.lock().unwrap().len(), 1);
        }

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
            assert!(fake.immich.contents(ALBUM, &budget()).await.is_err());
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
            assert_eq!(fake.immich.contents(ALBUM, &budget()).await.is_ok(), finish);
            assert_eq!(fake.requests.lock().unwrap().len(), limits::SEARCH_PAGES);
        }
    }

    #[tokio::test]
    async fn raw_record_item_album_and_projected_payload_limits() {
        let excluded = json!({"id": Uuid::from_u128(1), "type": "AUDIO", "visibility": "hidden", "isTrashed": false, "isEdited": false});

        // Raw records count even when identical and excluded; pages need not be full.
        let fake = Fake::new(vec![page(vec![excluded; limits::SEARCH_RECORDS + 1], None)]).await;
        assert!(
            fake.immich
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
                fake.immich.contents(ALBUM, &budget()).await.is_ok(),
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
            .immich
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
            fake.immich
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
            fake.immich
                .contents(ALBUM, &budget())
                .await
                .unwrap_err()
                .to_string()
                .contains("byte limit")
        );
    }

    #[tokio::test]
    async fn json_bound_is_enforced_before_parsing_with_and_without_content_length() {
        for chunked in [false, true] {
            let body = if chunked {
                Body::from_stream(futures_util::stream::iter(
                    (0..=limits::JSON_BYTES / 1024)
                        .map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from_static(&[b' '; 1024]))),
                ))
            } else {
                Body::from(vec![b' '; limits::JSON_BYTES + 1])
            };

            let fake = Fake::new(vec![
                Response::builder()
                    .status(StatusCode::OK)
                    .body(body)
                    .unwrap(),
            ])
            .await;
            let error = fake.immich.root(&budget()).await.unwrap_err().to_string();
            assert!(
                error.contains("JSON response exceeds byte limit"),
                "{error}"
            );
            assert_eq!(fake.requests.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn transport_failures_are_sanitized_and_do_not_retry() {
        for status in [401, 404, 429, 500, 503] {
            let fake = Fake::new(vec![
                Response::builder()
                    .status(status)
                    .body(Body::from(
                        "private-test-key http://secret.example/private upstream body",
                    ))
                    .unwrap(),
            ])
            .await;

            let error = format!("{:#}", fake.immich.root(&budget()).await.unwrap_err());
            assert!(error.contains(&status.to_string()));
            assert!(!error.contains("private") && !error.contains("http://"));
            assert_eq!(fake.requests.lock().unwrap().len(), 1);
        }

        let fake = Fake::new(vec![
            Response::new(Body::from(
                "{\"private-test-key\":\"http://secret.example/\",",
            )),
            version(),
            reply(json!([])),
        ])
        .await;

        let error = format!("{:#}", fake.immich.root(&budget()).await.unwrap_err());
        assert!(!error.contains("private") && !error.contains("http://"));
        fake.immich.root(&budget()).await.unwrap();
        assert_eq!(fake.requests.lock().unwrap().len(), 3);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut immich = fake.immich.clone();
        immich.api_base =
            Url::parse(&format!("http://{}/api/", listener.local_addr().unwrap())).unwrap();
        drop(listener);
        let error = format!("{:#}", immich.root(&budget()).await.unwrap_err());
        assert_eq!(error, "Immich catalog connection failed");
    }

    #[tokio::test]
    async fn stalled_headers_are_capped_by_remaining_preparation_budget() {
        use std::time::Duration;

        use tokio::io::AsyncReadExt;

        let fake = Fake::new(vec![]).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut immich = fake.immich.clone();

        immich.api_base =
            Url::parse(&format!("http://{}/api/", listener.local_addr().unwrap())).unwrap();

        let budget = FetchBudget {
            deadline: Instant::now() + Duration::from_secs(10),
            ..budget()
        };

        // No outer timeout: the header wait must enforce the remaining budget itself.
        let fetch = immich.root(&budget);
        tokio::pin!(fetch);

        let (mut socket, _) = tokio::select! {
            _ = &mut fetch => panic!("fetch completed before connection"),

            accepted = listener.accept() => accepted.unwrap(),
        };

        tokio::select! {
            _ = &mut fetch => panic!("fetch completed before headers"),

            _ = async {
                let mut request = Vec::new();

                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                }
            } => {}
        }

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(futures_util::poll!(&mut fetch).is_pending());

        // Observe the timer after its resolution boundary, still before the
        // uncapped fifteen-second header deadline would expire.
        tokio::time::advance(Duration::from_secs(2)).await;
        let now = Instant::now();
        let error = fetch.await.unwrap_err();
        assert_eq!(Instant::now(), now);

        assert_eq!(
            error.to_string(),
            "Immich catalog refresh preparation deadline exceeded"
        );

        assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn header_deadline_and_caller_owned_body_deadline_cancel_transport() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for send_headers in [false, true] {
            let fake = Fake::new(vec![]).await;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut immich = fake.immich.clone();
            immich.api_base =
                Url::parse(&format!("http://{}/api/", listener.local_addr().unwrap())).unwrap();

            let fetch = tokio::spawn(async move {
                let budget = budget();

                tokio::time::timeout_at(budget.deadline, immich.root(&budget)).await
            });

            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();

            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }

            if send_headers {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                    .await
                    .unwrap();
                // Let the real socket deliver headers before advancing the virtual clock.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            tokio::time::pause();
            tokio::time::advance(limits::UPSTREAM_HEADER_TIMEOUT).await;

            if send_headers {
                assert!(!fetch.is_finished());
                tokio::time::advance(limits::REFRESH_PREPARATION_TIMEOUT).await;
                assert!(fetch.await.unwrap().is_err());
            } else {
                let error = fetch.await.unwrap().unwrap().unwrap_err();
                assert!(error.to_string().contains("response-header deadline"));
            }

            assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
            tokio::time::resume();
        }
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
