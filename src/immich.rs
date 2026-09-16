//! Typed access to the Immich album and asset metadata API.

#[cfg(test)]
pub(crate) mod tests;

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow, ensure};
use http::{HeaderValue, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{sync::OnceCell, time::timeout};
use url::Url;
use uuid::Uuid;

pub(crate) const SEARCH_PAGE_SIZE: usize = 1_000;
const JSON_BYTES: usize = 16 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    api_base: Url,
    api_key: HeaderValue,
    version_checked: Arc<OnceCell<()>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Album {
    pub id: Uuid,
    pub album_name: String,
    #[serde(default, deserialize_with = "optional_date")]
    pub created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    pub end_date: Option<String>,
    pub album_thumbnail_asset_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub kind: String,
    pub visibility: String,
    pub is_trashed: bool,
    pub is_edited: bool,
    pub original_file_name: Option<String>,
    pub original_mime_type: Option<String>,
    /// Duration in milliseconds, as supplied by Immich.
    pub duration: Option<i64>,
    #[serde(default, deserialize_with = "optional_date")]
    pub file_created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    pub local_date_time: Option<String>,
    pub checksum: Option<String>,
    pub updated_at: Option<String>,
    pub thumbhash: Option<String>,
}

#[derive(Debug)]
pub struct AssetPage {
    pub items: Vec<Asset>,
    pub next_page: Option<usize>,
}

/// Both searches exclude deleted assets and omit EXIF and people data.
#[derive(Clone, Copy)]
pub enum AssetFilter {
    All,
    EncodedVideos,
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
struct SearchResponse {
    assets: SearchAssets,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchAssets {
    items: Vec<Asset>,
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

impl Client {
    pub fn new(api_base: Url, mut api_key: HeaderValue) -> Result<Self> {
        ensure!(
            matches!(api_base.scheme(), "http" | "https")
                && api_base.host_str().is_some()
                && api_base.username().is_empty()
                && api_base.password().is_none()
                && api_base.query().is_none()
                && api_base.fragment().is_none()
                && api_base.path().ends_with("/api/"),
            "Immich requires a normalized HTTP(S) API directory"
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
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("cannot initialize Immich HTTP client"))?;

        Ok(Self {
            client,
            api_base,
            api_key,
            version_checked: Arc::new(OnceCell::new()),
        })
    }

    async fn json<T: DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let request = request
            .header("x-api-key", self.api_key.clone())
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity");

        let mut response = timeout(RESPONSE_HEADER_TIMEOUT, request.send())
            .await
            .map_err(|_| anyhow!("Immich response-header deadline exceeded"))?
            .map_err(|_| anyhow!("Immich connection failed"))?;

        ensure!(
            response.status().is_success(),
            "Immich returned HTTP {}; check availability and API-key permissions",
            response.status().as_u16()
        );

        ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= JSON_BYTES as u64),
            "Immich JSON response exceeds byte limit"
        );

        let mut body = Vec::new();

        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow!("Immich body read failed"))?
        {
            ensure!(
                chunk.len() <= JSON_BYTES - body.len(),
                "Immich JSON response exceeds byte limit"
            );

            body.extend_from_slice(&chunk);
        }

        serde_json::from_slice(&body)
            .map_err(|_| anyhow!("invalid Immich JSON structure; requires Immich 3.1.0 or newer"))
    }

    /// Cache a successful minimum-version check across client clones.
    pub async fn ensure_supported_version(&self) -> Result<()> {
        self.version_checked
            .get_or_try_init(|| async {
                let version: Version = self
                    .json(self.client.get(self.api_base.join("server/version")?))
                    .await?;

                ensure!(
                    (version.major, version.minor, version.patch) > (3, 1, 0)
                        || ((version.major, version.minor, version.patch) == (3, 1, 0)
                            && version.prerelease.is_none()),
                    "Immich 3.1.0 or newer is required; upgrade the upstream server"
                );

                Ok::<_, anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    pub async fn albums(&self) -> Result<Vec<Album>> {
        self.json(self.client.get(self.api_base.join("albums")?))
            .await
    }

    /// Fetch one page; traversal and filtering of returned assets belong to the caller.
    pub async fn search_album(
        &self,
        album: Uuid,
        page: usize,
        filter: AssetFilter,
    ) -> Result<AssetPage> {
        ensure!(page > 0, "Immich search pages start at one");
        let encoded = matches!(filter, AssetFilter::EncodedVideos);

        let query = Search {
            album_ids: [album],
            page,
            size: SEARCH_PAGE_SIZE,
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
            )
            .await?;

        let next_page = if let Some(next) = result.assets.next_page {
            let expected = page
                .checked_add(1)
                .ok_or_else(|| anyhow!("Immich search page overflow"))?;

            ensure!(
                next == expected.to_string(),
                "invalid Immich search nextPage; expected the next numeric page string"
            );

            Some(expected)
        } else {
            None
        };

        Ok(AssetPage {
            items: result.assets.items,
            next_page,
        })
    }
}
