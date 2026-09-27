use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use axum::{body::Body, response::Response};
use futures_util::TryStreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};
use url::Url;
use uuid::Uuid;

use crate::{
    activity::{Activity, MediaActivity},
    config::ApiBase,
};

const OPERATIONS: usize = 16;
const REDIRECTS: usize = 3;
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) const ASSET_ROUTE_PREFIX: &str = "/media/assets/";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Representation {
    Original,
    Display,
    Preview,
    Playback,
}

impl Representation {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "original" => Some(Self::Original),
            "display" => Some(Self::Display),
            "preview" => Some(Self::Preview),
            "playback" => Some(Self::Playback),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Display => "display",
            Self::Preview => "preview",
            Self::Playback => "playback",
        }
    }

    fn endpoint(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Display => "thumbnail?size=fullsize&edited=true",
            Self::Preview => "thumbnail?size=preview&edited=true",
            Self::Playback => "video/playback",
        }
    }

    fn expected_mime(self) -> Option<&'static str> {
        match self {
            Self::Original => None,
            Self::Display | Self::Preview => Some("image/jpeg"),
            Self::Playback => Some("video/mp4"),
        }
    }

    fn edited(self) -> bool {
        matches!(self, Self::Display | Self::Preview)
    }
}

pub fn asset_url(address: SocketAddrV4, asset: Uuid, representation: Representation) -> String {
    let representation = representation.as_str();

    format!("http://{address}{ASSET_ROUTE_PREFIX}{asset}/{representation}")
}

#[derive(Clone)]
pub struct MediaProxy {
    client: reqwest::Client,
    api_base: ApiBase,
    api_key: HeaderValue,
    operations: Arc<Semaphore>,
    activity: Activity,
}

// Admission and activity follow the same request/response-body lifetime.
struct Operation {
    _permit: OwnedSemaphorePermit,
    _activity: Option<MediaActivity>,
}

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("{0}")]
    Upstream(&'static str),
    #[error("upstream deadline exceeded")]
    Timeout,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ByteRange {
    From(u64, Option<u64>),
    Suffix(u64),
}

impl ByteRange {
    fn parse(value: &str) -> Option<Self> {
        let (unit, offsets) = value.split_once('=')?;

        if !unit.eq_ignore_ascii_case("bytes") {
            return None;
        }

        let (start, end) = offsets.split_once('-')?;

        if start.is_empty() {
            return Some(Self::Suffix(decimal(end)?));
        }

        let start = decimal(start)?;

        if end.is_empty() {
            return Some(Self::From(start, None));
        }

        let end = decimal(end)?;

        (start <= end).then_some(Self::From(start, Some(end)))
    }

    fn unsatisfiable(self, length: u64) -> bool {
        match self {
            Self::From(start, _) => start >= length,
            Self::Suffix(size) => size == 0 || length == 0,
        }
    }
}

impl MediaProxy {
    pub fn new(
        api_base: ApiBase,
        mut api_key: HeaderValue,
        activity: Activity,
    ) -> anyhow::Result<Self> {
        api_key.set_sensitive(true);

        let client = crate::outbound_client_builder().build()?;

        Ok(Self {
            client,
            api_base,
            api_key,
            operations: Arc::new(Semaphore::new(OPERATIONS)),
            activity,
        })
    }

    pub async fn serve(
        &self,
        asset: &str,
        representation: &str,
        method: Method,
        headers: HeaderMap,
    ) -> Response {
        let Ok(asset) = Uuid::parse_str(asset) else {
            return response(StatusCode::NOT_FOUND);
        };

        let Some(route) = Representation::parse(representation) else {
            return response(StatusCode::NOT_FOUND);
        };

        if method != Method::GET && method != Method::HEAD {
            let mut response = response(StatusCode::METHOD_NOT_ALLOWED);

            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));

            return response;
        }

        // Log parsed capabilities only: never dump client headers or arbitrary values.
        tracing::debug!(
            %asset,
            representation,
            %method,
            range_count = headers.get_all(header::RANGE).iter().count(),
            range = ?single(&headers, header::RANGE).and_then(ByteRange::parse),
            range_end_to_end = end_to_end(&headers, &header::RANGE),
            if_range_present = headers.contains_key(header::IF_RANGE),
            "media proxy request"
        );

        let Ok(permit) = self.operations.clone().try_acquire_owned() else {
            return response(StatusCode::SERVICE_UNAVAILABLE);
        };

        let activity = if method == Method::GET {
            Some(self.activity.media())
        } else {
            self.activity.touch();

            None
        };

        let operation = Operation {
            _permit: permit,
            _activity: activity,
        };

        let started = std::time::Instant::now();

        let result = self.request(asset, route, method, headers, operation).await;

        match result {
            Ok(response) => response,

            Err(failure) => {
                tracing::warn!(%asset, representation, elapsed_ms = started.elapsed().as_millis(), %failure, "media request failed");

                response(match failure {
                    Failure::Timeout => StatusCode::GATEWAY_TIMEOUT,
                    Failure::Upstream(_) => StatusCode::BAD_GATEWAY,
                })
            }
        }
    }

    async fn request(
        &self,
        asset: Uuid,
        route: Representation,
        method: Method,
        headers: HeaderMap,
        operation: Operation,
    ) -> Result<Response, Failure> {
        let mut forwarded = HeaderMap::new();

        for name in [
            header::IF_MATCH,
            header::IF_NONE_MATCH,
            header::IF_MODIFIED_SINCE,
            header::IF_UNMODIFIED_SINCE,
        ] {
            copy_header(&headers, &mut forwarded, name);
        }

        let range = if method == Method::GET && end_to_end(&headers, &header::RANGE) {
            single(&headers, header::RANGE).and_then(ByteRange::parse)
        } else {
            None
        };

        if range.is_some() {
            copy_header(&headers, &mut forwarded, header::RANGE);
            copy_header(&headers, &mut forwarded, header::IF_RANGE);
        }

        let (upstream, length) = self.fetch(asset, route, &method, forwarded).await?;

        let mut status = upstream.status();
        let mut safe = HeaderMap::new();

        tracing::debug!(%asset, endpoint = route.endpoint(), status = status.as_u16(), "upstream media response");

        for name in [
            header::CONTENT_TYPE,
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
            header::ETAG,
            header::LAST_MODIFIED,
        ] {
            copy_header(upstream.headers(), &mut safe, name);
        }

        let unsatisfied_length = single(&safe, header::CONTENT_RANGE)
            .and_then(|value| {
                let (unit, value) = value.split_once(' ')?;

                unit.eq_ignore_ascii_case("bytes").then_some(value)
            })
            .and_then(|value| value.strip_prefix("*/"))
            .and_then(decimal);

        let unsatisfiable =
            unsatisfied_length.is_some_and(|length| range.is_some_and(|r| r.unsatisfiable(length)));

        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            if range.is_none() {
                return Err(Failure::Upstream("unsolicited unsatisfied range response"));
            }

            // Native 416 may omit Content-Range, but supplied framing must agree.
            if upstream.headers().contains_key(header::CONTENT_RANGE) && !unsatisfiable {
                return Err(Failure::Upstream("invalid unsatisfied range framing"));
            }
        }

        if status == StatusCode::NOT_FOUND && unsatisfiable {
            status = StatusCode::RANGE_NOT_SATISFIABLE;
        }

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {}

            StatusCode::NOT_MODIFIED
            | StatusCode::PRECONDITION_FAILED
            | StatusCode::NOT_FOUND
            | StatusCode::RANGE_NOT_SATISFIABLE => {
                if status != StatusCode::NOT_MODIFIED {
                    safe.remove(header::CONTENT_TYPE);
                    safe.remove(header::CONTENT_LENGTH);
                }

                if status != StatusCode::RANGE_NOT_SATISFIABLE {
                    safe.remove(header::CONTENT_RANGE);
                }

                let mut result = response(status);
                result.headers_mut().extend(safe);

                return Ok(result);
            }

            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(Failure::Upstream(
                    "Immich denied media access; check API key asset.view and asset.download permissions",
                ));
            }

            _ => return Err(Failure::Upstream("unexpected upstream media status")),
        }

        if upstream.headers().contains_key(header::CONTENT_ENCODING)
            && !single(upstream.headers(), header::CONTENT_ENCODING)
                .is_some_and(|value| value.eq_ignore_ascii_case("identity"))
        {
            return Err(Failure::Upstream("unexpected media content encoding"));
        }

        if upstream.headers().contains_key(header::TRANSFER_ENCODING)
            && !single(upstream.headers(), header::TRANSFER_ENCODING)
                .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
        {
            return Err(Failure::Upstream("unexpected media transfer encoding"));
        }

        let mime = single(&safe, header::CONTENT_TYPE)
            .and_then(crate::mime::parse)
            .ok_or(Failure::Upstream("missing or invalid media MIME"))?;

        if mime.eq_ignore_ascii_case("multipart/byteranges")
            || route
                .expected_mime()
                .is_some_and(|expected| !mime.eq_ignore_ascii_case(expected))
        {
            return Err(Failure::Upstream("incompatible media MIME"));
        }

        let body_length = if status == StatusCode::PARTIAL_CONTENT {
            let span = range
                .and_then(|range| partial_span(single(&safe, header::CONTENT_RANGE)?, range))
                .ok_or(Failure::Upstream("invalid partial media framing"))?;

            if length.is_some_and(|length| length != span) {
                return Err(Failure::Upstream("inconsistent partial media framing"));
            }

            Some(span)
        } else if safe.contains_key(header::CONTENT_RANGE) {
            return Err(Failure::Upstream("unexpected media content range"));
        } else {
            length
        };

        let mut result = response(status);
        result.headers_mut().extend(safe);

        tracing::debug!(
            %asset,
            %status,
            content_length = ?length,
            accepts_bytes = single(result.headers(), header::ACCEPT_RANGES)
                .is_some_and(|value| value.eq_ignore_ascii_case("bytes")),
            body_length = ?body_length,
            "validated media response"
        );

        if method == Method::HEAD {
            return Ok(result);
        }

        *result.body_mut() = media_body(upstream, operation, body_length, asset);

        Ok(result)
    }

    async fn fetch(
        &self,
        asset: Uuid,
        route: Representation,
        method: &Method,
        mut forwarded: HeaderMap,
    ) -> Result<(reqwest::Response, Option<u64>), Failure> {
        forwarded.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );

        forwarded.insert("x-api-key", self.api_key.clone());

        let mut url = self
            .api_base
            .as_url()
            .join(&format!("assets/{asset}/{}", route.endpoint()))
            .map_err(|_| Failure::Upstream("invalid media endpoint"))?;

        let mut visited = vec![url.clone()];

        // One header budget covers the whole redirect chain, not each hop separately.
        let deadline = Instant::now() + RESPONSE_HEADER_TIMEOUT;

        let (upstream, length) = loop {
            if Instant::now() >= deadline {
                return Err(Failure::Timeout);
            }

            let upstream = timeout_at(deadline, async {
                self.client
                    .request(method.clone(), url.clone())
                    .headers(forwarded.clone())
                    .send()
                    .await
            })
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(transport_failure)?;

            // Validate raw framing even on redirects and bodyless/error responses.
            if upstream.headers().contains_key(header::TRANSFER_ENCODING)
                && upstream.headers().contains_key(header::CONTENT_LENGTH)
            {
                return Err(Failure::Upstream("conflicting media response framing"));
            }

            let length = if upstream.headers().contains_key(header::CONTENT_LENGTH) {
                Some(
                    single(upstream.headers(), header::CONTENT_LENGTH)
                        .and_then(decimal)
                        .ok_or(Failure::Upstream("invalid media content length"))?,
                )
            } else {
                None
            };

            if !matches!(upstream.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
                break (upstream, length);
            }

            let location = single(upstream.headers(), header::LOCATION)
                .filter(|_| end_to_end(upstream.headers(), &header::LOCATION))
                .ok_or(Failure::Upstream("missing or ambiguous media redirect"))?;

            let target = url
                .join(location)
                .map_err(|_| Failure::Upstream("invalid media redirect"))?;

            if visited.len() > REDIRECTS
                || visited.contains(&target)
                || !allowed_redirect(self.api_base.as_url(), route.edited(), &target, asset)
            {
                return Err(Failure::Upstream("unsafe or excessive media redirect"));
            }

            visited.push(target.clone());
            url = target;
        };

        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }

        Ok((upstream, length))
    }
}

fn allowed_redirect(api_base: &Url, initial_edited: bool, target: &Url, asset: Uuid) -> bool {
    if target.origin() != api_base.origin()
        || !target.username().is_empty()
        || target.password().is_some()
        || target.fragment().is_some()
    {
        return false;
    }

    let prefix = format!("{}assets/{asset}/", api_base.path());

    let Some(endpoint) = target.path().strip_prefix(&prefix) else {
        return false;
    };

    let mut edited = None;
    let mut size_seen = false;

    for (key, value) in target.query_pairs() {
        match key.as_ref() {
            "edited" if edited.is_none() && matches!(value.as_ref(), "true" | "false") => {
                edited = Some(value == "true");
            }

            "size" if !size_seen && matches!(value.as_ref(), "fullsize" | "preview") => {
                size_seen = true;
            }

            _ => return false,
        }
    }

    if edited.unwrap_or(false) != initial_edited {
        return false;
    }

    match endpoint {
        "original" => !size_seen,
        "thumbnail" => size_seen,
        "video/playback" => !size_seen && edited.is_none(),
        _ => false,
    }
}

fn media_body(
    upstream: reqwest::Response,
    operation: Operation,
    body_length: Option<u64>,
    asset: Uuid,
) -> Body {
    let stream = futures_util::stream::try_unfold(
        (upstream, operation, body_length),
        move |(mut upstream, operation, mut remaining)| async move {
            let deadline = Instant::now() + READ_IDLE_TIMEOUT;

            let chunk = timeout_at(deadline, async {
                loop {
                    if Instant::now() >= deadline {
                        return Err(Failure::Timeout);
                    }

                    match upstream.chunk().await.map_err(transport_failure)? {
                        Some(chunk) if chunk.is_empty() => continue,

                        chunk => break Ok(chunk),
                    }
                }
            })
            .await
            .map_err(|_| Failure::Timeout)??;

            match chunk {
                Some(chunk) => {
                    if let Some(left) = remaining.as_mut() {
                        *left = left
                            .checked_sub(chunk.len() as u64)
                            .ok_or(Failure::Upstream(
                                "media body exceeds declared content length",
                            ))?;
                    }

                    Ok(Some((chunk, (upstream, operation, remaining))))
                }

                None if remaining.is_none_or(|left| left == 0) => Ok(None),
                None => Err(Failure::Upstream("truncated media body")),
            }
        },
    );

    Body::from_stream(stream.inspect_err(move |failure| {
        tracing::warn!(%asset, %failure, "media stream terminated");
    }))
}

fn response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;

    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );

    response
}

fn transport_failure(error: reqwest::Error) -> Failure {
    if error.is_timeout() {
        return Failure::Timeout;
    }

    Failure::Upstream("upstream transport failed")
}

fn single(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;

    values.next().is_none().then_some(value)
}

fn end_to_end(headers: &HeaderMap, name: &header::HeaderName) -> bool {
    headers.get_all(header::CONNECTION).iter().all(|value| {
        value.to_str().is_ok_and(|value| {
            !value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(name.as_str()))
        })
    })
}

fn copy_header(source: &HeaderMap, target: &mut HeaderMap, name: header::HeaderName) {
    if end_to_end(source, &name) {
        for value in source.get_all(&name) {
            target.append(name.clone(), value.clone());
        }
    }
}

fn decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    value.parse().ok()
}

fn partial_span(value: &str, requested: ByteRange) -> Option<u64> {
    let (unit, value) = value.split_once(' ')?;

    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }

    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let (start, end) = (decimal(start)?, decimal(end)?);

    let total = if total == "*" {
        None
    } else {
        Some(decimal(total)?)
    };

    if total.is_some_and(|total| total <= end) {
        return None;
    }

    let span = end.checked_sub(start)?.checked_add(1)?;

    // A server may fulfill only part of the requested interval, but not a different one.
    let compatible = match requested {
        ByteRange::From(first, last) => start >= first && last.is_none_or(|last| end <= last),

        ByteRange::Suffix(size) => {
            size != 0
                && span <= size
                && total.is_none_or(|total| start >= total.saturating_sub(size))
        }
    };

    compatible.then_some(span)
}

#[cfg(test)]
mod tests;
