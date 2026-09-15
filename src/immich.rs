//! Typed access to the Immich album and asset metadata API.

#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;

use anyhow::{Result, anyhow, ensure};
use http::{HeaderValue, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{sync::OnceCell, time::Instant};
use url::Url;
use uuid::Uuid;

use crate::{
    deadline::{self, Budget},
    limits,
};

#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    api_base: Url,
    api_key: HeaderValue,
    version_checked: Arc<OnceCell<()>>,
    #[cfg(test)]
    pub(crate) after_json: Option<Arc<dyn Fn() + Send + Sync>>,
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
            .connect_timeout(limits::CONNECT_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("cannot initialize Immich HTTP client"))?;

        Ok(Self {
            client,
            api_base,
            api_key,
            version_checked: Arc::new(OnceCell::new()),
            #[cfg(test)]
            after_json: None,
        })
    }

    async fn json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        budget: &Budget,
    ) -> Result<T> {
        budget.run(self.read_json(request, budget)).await?
    }

    async fn read_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
        budget: &Budget,
    ) -> Result<T> {
        let request = request
            .header("x-api-key", self.api_key.clone())
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity");

        // A cooperative timeout cannot interrupt synchronous JSON processing.
        budget.check()?;

        let header_deadline = budget
            .deadline
            .min(Instant::now() + limits::UPSTREAM_HEADER_TIMEOUT);

        let response = deadline::timeout_at(header_deadline, async { request.send().await }).await;

        budget.check()?;

        let mut response = response
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
                .is_none_or(|length| length <= limits::JSON_BYTES as u64),
            "Immich JSON response exceeds byte limit"
        );

        let mut body = Vec::new();

        loop {
            budget.check()?;

            let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| anyhow!("Immich body read failed"))?
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
            anyhow!("invalid Immich JSON structure; requires Immich 3.1.0 or newer")
        })?;

        #[cfg(test)]
        if let Some(after_json) = &self.after_json {
            after_json();
        }

        budget.check()?;

        Ok(parsed)
    }

    /// Cache a successful minimum-version check across client clones.
    pub async fn ensure_supported_version(&self, budget: &Budget) -> Result<()> {
        budget
            .run(self.version_checked.get_or_try_init(|| async {
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
            }))
            .await??;

        Ok(())
    }

    pub async fn albums(&self, budget: &Budget) -> Result<Vec<Album>> {
        self.json(self.client.get(self.api_base.join("albums")?), budget)
            .await
    }

    /// Fetch one page; traversal and filtering of returned assets belong to the caller.
    pub async fn search_album(
        &self,
        album: Uuid,
        page: usize,
        filter: AssetFilter,
        budget: &Budget,
    ) -> Result<AssetPage> {
        budget.check()?;
        ensure!(page > 0, "Immich search pages start at one");
        let encoded = matches!(filter, AssetFilter::EncodedVideos);

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

        budget.check()?;

        Ok(AssetPage {
            items: result.assets.items,
            next_page,
        })
    }
}
