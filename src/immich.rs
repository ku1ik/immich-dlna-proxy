//! Typed access to the Immich album and asset metadata API.

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Result, anyhow, ensure};
use http::{HeaderValue, header};
use serde::{Deserialize, Serialize, Serializer, de::DeserializeOwned, ser::SerializeMap};
use tokio::{sync::OnceCell, time::timeout};
use uuid::Uuid;

use crate::config::ApiBase;

pub(crate) const SEARCH_PAGE_SIZE: usize = 1_000;
const JSON_BYTES: usize = 16 * 1024 * 1024;
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct Client {
    client: reqwest::Client,
    api_base: ApiBase,
    api_key: HeaderValue,
    version_checked: OnceCell<()>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Album {
    pub(crate) id: Uuid,
    pub(crate) album_name: String,
    #[serde(default, deserialize_with = "optional_date")]
    pub(crate) created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    pub(crate) end_date: Option<String>,
    pub(crate) album_thumbnail_asset_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Asset {
    pub(crate) id: Uuid,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) visibility: String,
    pub(crate) is_trashed: bool,
    pub(crate) is_edited: bool,
    pub(crate) original_file_name: Option<String>,
    pub(crate) original_mime_type: Option<String>,
    /// Duration in milliseconds, as supplied by Immich.
    pub(crate) duration: Option<i64>,
    #[serde(default, deserialize_with = "optional_date")]
    pub(crate) file_created_at: Option<String>,
    #[serde(default, deserialize_with = "optional_date")]
    pub(crate) local_date_time: Option<String>,
    pub(crate) checksum: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) thumbhash: Option<String>,
}

#[derive(Debug)]
pub(crate) struct AssetPage {
    pub(crate) items: Vec<Asset>,
    pub(crate) next_page: Option<NonZeroUsize>,
}

#[derive(Deserialize)]
struct Version {
    major: u64,
    minor: u64,
    patch: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    prerelease: Option<u64>,
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
    #[serde(deserialize_with = "Option::deserialize")]
    next_page: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Search {
    album_ids: [Uuid; 1],
    page: NonZeroUsize,
    size: usize,
    with_deleted: bool,
    with_exif: bool,
    with_people: bool,
    #[serde(flatten)]
    mode: SearchMode,
}

#[derive(Clone, Copy)]
pub(crate) enum SearchMode {
    Members,
    EncodedVideos,
}

impl Serialize for SearchMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        if matches!(self, Self::EncodedVideos) {
            map.serialize_entry("type", "VIDEO")?;
            map.serialize_entry("isEncoded", &true)?;
        }

        map.end()
    }
}

impl Client {
    pub(crate) fn new(api_base: ApiBase, mut api_key: HeaderValue) -> Result<Self> {
        api_key.set_sensitive(true);

        let client = crate::outbound_client_builder()
            .build()
            .map_err(|_| anyhow!("cannot initialize Immich HTTP client"))?;

        Ok(Self {
            client,
            api_base,
            api_key,
            version_checked: OnceCell::new(),
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

    /// Cache a successful minimum-version check for this client instance.
    pub(crate) async fn ensure_supported_version(&self) -> Result<()> {
        self.version_checked
            .get_or_try_init(|| async {
                let version: Version = self
                    .json(
                        self.client
                            .get(self.api_base.as_url().join("server/version")?),
                    )
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

    pub(crate) async fn albums(&self) -> Result<Vec<Album>> {
        self.json(self.client.get(self.api_base.as_url().join("albums")?))
            .await
    }

    /// Fetch one page; traversal and filtering of returned assets belong to the caller.
    pub(crate) async fn search_album(
        &self,
        album: Uuid,
        page: NonZeroUsize,
        mode: SearchMode,
    ) -> Result<AssetPage> {
        let query = Search {
            album_ids: [album],
            page,
            size: SEARCH_PAGE_SIZE,
            with_deleted: false,
            with_exif: false,
            with_people: false,
            mode,
        };

        let result: SearchResponse = self
            .json(
                self.client
                    .post(self.api_base.as_url().join("search/metadata")?)
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
