use std::{sync::Arc, time::Duration};

use axum::{body::Body, response::Response};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};
use url::Url;
use uuid::Uuid;

pub const OPERATIONS: usize = 16;
const REDIRECTS: usize = 3;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(15);
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct MediaProxy {
    client: reqwest::Client,
    api_base: Url,
    api_key: HeaderValue,
    operations: Arc<Semaphore>,
    header_timeout: Duration,
    read_timeout: Duration,
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
    pub fn new(api_base: Url, mut api_key: HeaderValue) -> anyhow::Result<Self> {
        anyhow::ensure!(
            matches!(api_base.scheme(), "http" | "https")
                && api_base.host_str().is_some()
                && api_base.username().is_empty()
                && api_base.password().is_none()
                && api_base.query().is_none()
                && api_base.fragment().is_none()
                && api_base.path().ends_with("/api/"),
            "media requires a normalized HTTP(S) API directory"
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
            .build()?;

        Ok(Self {
            client,
            api_base,
            api_key,
            operations: Arc::new(Semaphore::new(OPERATIONS)),
            header_timeout: RESPONSE_HEADER_TIMEOUT,
            read_timeout: READ_IDLE_TIMEOUT,
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

        let (endpoint, expected_mime) = match representation {
            "original" => ("original", None),
            "display" => ("thumbnail?size=fullsize&edited=true", Some("image/jpeg")),
            "preview" => ("thumbnail?size=preview&edited=true", Some("image/jpeg")),
            "playback" => ("video/playback", Some("video/mp4")),
            _ => return response(StatusCode::NOT_FOUND),
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
            content_features_present = headers.contains_key("getcontentfeatures.dlna.org"),
            content_features_requested = single(
                &headers,
                header::HeaderName::from_static("getcontentfeatures.dlna.org"),
            ) == Some("1"),
            time_seek_present = headers.contains_key("timeseekrange.dlna.org"),
            available_seek_range_present = headers.contains_key("getavailableseekrange.dlna.org"),
            play_speed_present = headers.contains_key("playspeed.dlna.org"),
            transfer_mode_present = headers.contains_key("transfermode.dlna.org"),
            "media request capabilities"
        );

        let Ok(permit) = self.operations.clone().try_acquire_owned() else {
            return response(StatusCode::SERVICE_UNAVAILABLE);
        };

        let started = std::time::Instant::now();

        let result = self
            .request(asset, endpoint, expected_mime, method, headers, permit)
            .await;

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
        endpoint: &str,
        expected_mime: Option<&str>,
        method: Method,
        headers: HeaderMap,
        permit: OwnedSemaphorePermit,
    ) -> Result<Response, Failure> {
        let mut url = self
            .api_base
            .join(&format!("assets/{asset}/{endpoint}"))
            .map_err(|_| Failure::Upstream("invalid media endpoint"))?;

        let initial_url = url.clone();
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

        forwarded.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );

        forwarded.insert("x-api-key", self.api_key.clone());
        let mut visited = vec![url.clone()];

        // One header budget covers the whole redirect chain, not each hop separately.
        let deadline = Instant::now() + self.header_timeout;

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
                || !self.allowed_redirect(&initial_url, &target, asset)
            {
                return Err(Failure::Upstream("unsafe or excessive media redirect"));
            }

            visited.push(target.clone());
            url = target;
        };

        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }

        let mut status = upstream.status();
        let mut safe = HeaderMap::new();

        tracing::debug!(%asset, endpoint, status = status.as_u16(), "upstream media response");

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

            StatusCode::NOT_MODIFIED => {
                safe.remove(header::CONTENT_RANGE);
                let mut result = response(status);
                result.headers_mut().extend(safe);

                return Ok(result);
            }

            StatusCode::PRECONDITION_FAILED | StatusCode::NOT_FOUND => {
                safe.remove(header::CONTENT_TYPE);
                safe.remove(header::CONTENT_LENGTH);
                safe.remove(header::CONTENT_RANGE);
                let mut result = response(status);
                result.headers_mut().extend(safe);

                return Ok(result);
            }

            StatusCode::RANGE_NOT_SATISFIABLE => {
                safe.remove(header::CONTENT_TYPE);
                safe.remove(header::CONTENT_LENGTH);
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
            || expected_mime.is_some_and(|expected| !mime.eq_ignore_ascii_case(expected))
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

        let idle = self.read_timeout;

        let stream = futures_util::stream::try_unfold(
            (upstream, permit, body_length),
            move |(mut upstream, permit, mut remaining)| async move {
                let deadline = Instant::now() + idle;

                let next = timeout_at(deadline, async {
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
                .map_err(|_| Failure::Timeout)
                .and_then(|result| result);

                let result =
                    next.and_then(|chunk| match chunk {
                        Some(chunk) => {
                            if let Some(left) = remaining.as_mut() {
                                *left = left.checked_sub(chunk.len() as u64).ok_or(
                                    Failure::Upstream("media body exceeds declared content length"),
                                )?;
                            }

                            Ok(Some((chunk, (upstream, permit, remaining))))
                        }

                        None if remaining.is_none_or(|left| left == 0) => Ok(None),
                        None => Err(Failure::Upstream("truncated media body")),
                    });

                if let Err(failure) = &result {
                    tracing::warn!(%asset, %failure, "media stream terminated");
                }

                result
            },
        );

        *result.body_mut() = Body::from_stream(stream);

        Ok(result)
    }

    fn allowed_redirect(&self, initial: &Url, target: &Url, asset: Uuid) -> bool {
        if target.origin() != self.api_base.origin()
            || !target.username().is_empty()
            || target.password().is_some()
            || target.fragment().is_some()
        {
            return false;
        }

        let prefix = format!("{}assets/{asset}/", self.api_base.path());
        let Some(endpoint) = target.path().strip_prefix(&prefix) else {
            return false;
        };

        let initial_edited = initial
            .query_pairs()
            .any(|(k, v)| k == "edited" && v == "true");
        let mut edited = None;
        let mut size = None;

        for (key, value) in target.query_pairs() {
            match key.as_ref() {
                "edited" if edited.is_none() && matches!(value.as_ref(), "true" | "false") => {
                    edited = Some(value == "true");
                }

                "size" if size.is_none() && matches!(value.as_ref(), "fullsize" | "preview") => {
                    size = Some(value);
                }

                _ => return false,
            }
        }

        if edited.unwrap_or(false) != initial_edited {
            return false;
        }

        match endpoint {
            "original" => size.is_none(),
            "thumbnail" => size.is_some(),
            "video/playback" => size.is_none() && edited.is_none() && !initial_edited,
            _ => false,
        }
    }
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
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use futures_util::{FutureExt, StreamExt};
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::mpsc,
        task::{JoinHandle, JoinSet},
    };

    const ASSET: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    const JPEG: &str = "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc";

    struct WakeSignal(tokio::sync::Notify);

    impl Wake for WakeSignal {
        fn wake(self: Arc<Self>) {
            self.0.notify_one();
        }
    }

    #[derive(Clone)]
    struct Reply {
        wire: String,
        header_delay: Duration,
        chunks: Vec<(Duration, String)>,
    }

    impl Reply {
        fn new(wire: &str) -> Self {
            Self {
                wire: wire.into(),
                header_delay: Duration::ZERO,
                chunks: Vec::new(),
            }
        }

        fn stalled(wire: &str) -> Self {
            Self {
                chunks: vec![(Duration::from_secs(60), String::new())],
                ..Self::new(wire)
            }
        }
    }

    struct FakeServer {
        base: Url,
        requests: mpsc::UnboundedReceiver<String>,
        task: JoinHandle<()>,
    }

    impl FakeServer {
        async fn new(replies: Vec<Reply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

            let base = Url::parse(&format!(
                "http://{}/prefix/api/",
                listener.local_addr().unwrap()
            ))
            .unwrap();

            let (sender, requests) = mpsc::unbounded_channel();

            let task = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                let mut index = 0;

                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let sender = sender.clone();
                    let reply = replies[index.min(replies.len() - 1)].clone();
                    index += 1;

                    connections.spawn(async move {
                        let mut request = Vec::new();
                        let mut buffer = [0; 1024];

                        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                            let count = socket.read(&mut buffer).await.unwrap();

                            if count == 0 {
                                return;
                            }

                            request.extend_from_slice(&buffer[..count]);
                            assert!(request.len() <= crate::server::HEADER_BYTES);
                        }

                        let _ = sender.send(String::from_utf8(request).unwrap());
                        tokio::time::sleep(reply.header_delay).await;

                        if socket.write_all(reply.wire.as_bytes()).await.is_err() {
                            return;
                        }

                        for (delay, chunk) in reply.chunks {
                            tokio::time::sleep(delay).await;

                            if socket.write_all(chunk.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });

            Self {
                base,
                requests,
                task,
            }
        }

        fn proxy(&self) -> MediaProxy {
            MediaProxy::new(self.base.clone(), HeaderValue::from_static("test-secret")).unwrap()
        }

        async fn request(&mut self) -> String {
            tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
                .await
                .unwrap()
                .unwrap()
        }
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();

        for (name, value) in pairs {
            headers.append(
                header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }

        headers
    }

    fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.trim()))
    }

    #[tokio::test]
    async fn fixed_routes_normalize_uuids_and_use_only_media_endpoints() {
        let mut server = FakeServer::new(vec![Reply::new(JPEG)]).await;
        let proxy = server.proxy();

        for (representation, endpoint, status) in [
            ("original", "original", StatusCode::OK),
            (
                "display",
                "thumbnail?size=fullsize&edited=true",
                StatusCode::OK,
            ),
            (
                "preview",
                "thumbnail?size=preview&edited=true",
                StatusCode::OK,
            ),
            ("playback", "video/playback", StatusCode::BAD_GATEWAY),
        ] {
            let result = proxy
                .serve(
                    &ASSET.to_uppercase(),
                    representation,
                    Method::GET,
                    HeaderMap::new(),
                )
                .await;

            assert_eq!(result.status(), status);

            assert!(server.request().await.starts_with(&format!(
                "GET /prefix/api/assets/{ASSET}/{endpoint} HTTP/1.1\r\n"
            )));
        }

        for (asset, representation, method, status) in [
            (
                "../server/version",
                "original",
                Method::GET,
                StatusCode::NOT_FOUND,
            ),
            (
                ASSET,
                "original?edited=true",
                Method::GET,
                StatusCode::NOT_FOUND,
            ),
            (
                ASSET,
                "original",
                Method::POST,
                StatusCode::METHOD_NOT_ALLOWED,
            ),
        ] {
            let result = proxy
                .serve(asset, representation, method, HeaderMap::new())
                .await;

            assert_eq!(result.status(), status);
        }

        assert!(server.requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn forwards_only_conditionals_and_single_range_with_sensitive_key() {
        let mut server = FakeServer::new(vec![Reply::new(JPEG)]).await;
        let proxy = server.proxy();
        assert!(proxy.api_key.is_sensitive());

        let input = headers(&[
            ("range", "bytes=2147483648-4294967296"),
            ("if-range", "\"version\""),
            ("if-match", "\"match\""),
            ("if-none-match", "\"none\""),
            ("if-modified-since", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ("if-unmodified-since", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ("authorization", "Bearer inbound-secret"),
            ("proxy-authorization", "Basic inbound-secret"),
            ("cookie", "private=inbound-secret"),
            ("host", "attacker.invalid"),
            ("accept-encoding", "gzip"),
            ("x-api-key", "inbound-secret"),
            ("x-arbitrary", "inbound-secret"),
            ("connection", "If-Match, X-Arbitrary"),
        ]);

        let result = proxy.serve(ASSET, "original", Method::GET, input).await;
        assert_eq!(result.status(), StatusCode::OK);
        let request = server.request().await;
        assert_eq!(request_header(&request, "x-api-key"), Some("test-secret"));

        assert_eq!(
            request_header(&request, "accept-encoding"),
            Some("identity")
        );

        assert_eq!(
            request_header(&request, "range"),
            Some("bytes=2147483648-4294967296")
        );

        assert_eq!(request_header(&request, "if-range"), Some("\"version\""));
        assert_eq!(request_header(&request, "if-none-match"), Some("\"none\""));
        assert!(request_header(&request, "if-modified-since").is_some());
        assert!(request_header(&request, "if-unmodified-since").is_some());
        assert!(request_header(&request, "if-match").is_none());
        assert!(!request.contains("inbound-secret"));
        assert!(!request.contains("attacker.invalid"));
    }

    #[tokio::test]
    async fn unsupported_ranges_and_head_remove_if_range_but_keep_conditionals() {
        let mut server = FakeServer::new(vec![Reply::new(JPEG)]).await;
        let proxy = server.proxy();

        for range in [
            "bytes=0-1,3-4",
            "items=0-1",
            "bytes=3-1",
            "bytes=18446744073709551616-",
            "bytes=-",
            "bytes=+1-2",
        ] {
            let result = proxy
                .serve(
                    ASSET,
                    "original",
                    Method::GET,
                    headers(&[
                        ("range", range),
                        ("if-range", "\"old\""),
                        ("if-none-match", "\"v1\""),
                    ]),
                )
                .await;

            assert_eq!(result.status(), StatusCode::OK);
            let request = server.request().await;
            assert!(request_header(&request, "range").is_none());
            assert!(request_header(&request, "if-range").is_none());
            assert_eq!(request_header(&request, "if-none-match"), Some("\"v1\""));
        }

        for input in [
            headers(&[
                ("range", "bytes=0-1"),
                ("range", "bytes=2-3"),
                ("if-range", "\"v1\""),
            ]),
            headers(&[
                ("range", "bytes=0-1"),
                ("connection", "range"),
                ("if-range", "\"v1\""),
            ]),
        ] {
            proxy.serve(ASSET, "original", Method::GET, input).await;
            let request = server.request().await;
            assert!(request_header(&request, "range").is_none());
            assert!(request_header(&request, "if-range").is_none());
        }

        let result = proxy
            .serve(
                ASSET,
                "original",
                Method::HEAD,
                headers(&[("range", "bytes=0-1"), ("if-range", "\"v1\"")]),
            )
            .await;

        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.headers()[header::CONTENT_LENGTH], "3");
        assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
        let request = server.request().await;
        assert!(request.starts_with("HEAD "));
        assert!(request_header(&request, "range").is_none());
        assert!(request_header(&request, "if-range").is_none());

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    }

    #[tokio::test]
    async fn preserves_single_partial_responses_and_wide_offsets() {
        for (range, content_range) in [
            ("bytes=0-2", "bytes 0-2/10"),
            ("bytes=7-", "bytes 7-9/10"),
            ("bytes=-3", "bytes 7-9/10"),
            (
                "bytes=4294967296-4294967298",
                "bytes 4294967296-4294967298/5000000000",
            ),
            ("bytes=0-2", "bytes 0-2/*"),
        ] {
            let wire = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: IMAGE/JPEG; note=\"a;b\"\r\nContent-Length: 3\r\nContent-Range: {content_range}\r\nAccept-Ranges: bytes\r\nETag: \"v1\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\nabc"
            );

            let mut server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, "preview", Method::GET, headers(&[("range", range)]))
                .await;

            assert_eq!(result.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(result.headers()[header::CONTENT_RANGE], content_range);
            assert_eq!(result.headers()[header::CONTENT_LENGTH], "3");
            assert_eq!(result.headers()[header::ACCEPT_RANGES], "bytes");
            assert_eq!(result.headers()[header::ETAG], "\"v1\"");
            assert!(result.headers().contains_key(header::LAST_MODIFIED));
            assert_eq!(to_bytes(result.into_body(), 1024).await.unwrap(), "abc");

            assert_eq!(
                request_header(&server.request().await, "range"),
                Some(range)
            );
        }
    }

    #[tokio::test]
    async fn rejects_invalid_partial_framing_before_streaming_on_every_route() {
        for representation in ["original", "display", "preview", "playback"] {
            let valid_mime = if representation == "playback" {
                "video/mp4"
            } else {
                "image/jpeg"
            };

            for (framing, mime, range) in [
                (
                    "Content-Range: bytes 0-2/10\r\n",
                    "multipart/byteranges; boundary=x",
                    Some("bytes=0-2"),
                ),
                ("Content-Range: bytes 0-2/10\r\n", valid_mime, None),
                ("", valid_mime, Some("bytes=0-2")),
                (
                    "Content-Range: bytes 3-2/10\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
                (
                    "Content-Range: bytes 0-2/2\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
                (
                    "Content-Range: bytes 0-3/10\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
                (
                    "Content-Range: bytes 0-18446744073709551615/*\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
                (
                    "Content-Range: bytes 0-2/10\r\nContent-Range: bytes 0-2/10\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
                (
                    "Content-Range: bytes 0-2/10\r\nConnection: Content-Range\r\n",
                    valid_mime,
                    Some("bytes=0-2"),
                ),
            ] {
                let wire = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: {mime}\r\nContent-Length: 3\r\n{framing}Connection: close\r\n\r\nabc"
                );

                let server = FakeServer::new(vec![Reply::new(&wire)]).await;
                let input = range.map(|r| headers(&[("range", r)])).unwrap_or_default();

                let result = server
                    .proxy()
                    .serve(ASSET, representation, Method::GET, input)
                    .await;

                assert_eq!(
                    result.status(),
                    StatusCode::BAD_GATEWAY,
                    "{representation}: {framing}"
                );

                assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
            }

            for (length, expected) in [
                (3, StatusCode::PARTIAL_CONTENT),
                (4, StatusCode::BAD_GATEWAY),
            ] {
                let payload = if length == 3 { "abc" } else { "abcd" };

                let wire = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: {valid_mime}\r\nContent-Length: {length}\r\nContent-Range: bytes 0-2/10\r\nConnection: close\r\n\r\n{payload}"
                );

                let server = FakeServer::new(vec![Reply::new(&wire)]).await;

                let result = server
                    .proxy()
                    .serve(
                        ASSET,
                        representation,
                        Method::GET,
                        headers(&[("range", "bytes=0-2")]),
                    )
                    .await;

                assert_eq!(
                    result.status(),
                    expected,
                    "{representation}: length={length}"
                );

                let body = to_bytes(result.into_body(), 1024).await.unwrap();

                assert_eq!(
                    body.as_ref(),
                    if length == 3 { b"abc".as_slice() } else { b"" }
                );
            }
        }
    }

    #[tokio::test]
    async fn unsatisfied_ranges_normalize_only_proven_immich_404s() {
        for (upstream_status, range, content_range, expected) in [
            (416, Some("bytes=10-"), "", 416),
            (416, Some("bytes=0-2"), "", 416),
            (416, None, "", 502),
            (416, Some("bytes=0-1,4-5"), "", 502),
            (404, Some("bytes=10-"), "", 404),
            (404, None, "", 404),
            (404, Some("bytes=10-"), "bytes */10", 416),
            (404, Some("bytes=10-20"), "bytes */10", 416),
            (404, Some("bytes=-0"), "bytes */10", 416),
            (404, Some("bytes=-3"), "bytes */0", 416),
            (404, Some("bytes=9-"), "bytes */10", 404),
            (404, Some("bytes=-3"), "bytes */10", 404),
            (404, Some("bytes=0-1,4-5"), "bytes */0", 404),
            (404, None, "bytes */0", 404),
            (404, Some("bytes=10-"), "bytes */18446744073709551616", 404),
            (416, Some("bytes=10-"), "bytes */10", 416),
            (416, Some("bytes=9-"), "bytes */10", 502),
            (416, Some("bytes=10-"), "bytes 0-2/10", 502),
            (416, Some("bytes=10-"), "garbage", 502),
            (416, Some("bytes=10-"), "bytes */18446744073709551616", 502),
            (
                416,
                Some("bytes=10-"),
                "bytes */10\r\nContent-Range: bytes */10",
                502,
            ),
            (
                416,
                Some("bytes=10-"),
                "bytes */10\r\nConnection: Content-Range",
                502,
            ),
            (416, Some("bytes=10-"), "bytes */*", 502),
            (416, None, "bytes */10", 502),
        ] {
            let framing = if content_range.is_empty() {
                String::new()
            } else {
                format!("Content-Range: {content_range}\r\n")
            };

            let wire = format!(
                "HTTP/1.1 {upstream_status} Error\r\nContent-Type: application/json\r\nContent-Length: 6\r\n{framing}ETag: \"v1\"\r\nAccept-Ranges: bytes\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nSet-Cookie: secret=value\r\nX-Private: secret\r\nConnection: close, Last-Modified\r\n\r\nsecret"
            );

            let mut server = FakeServer::new(vec![Reply::new(&wire)]).await;
            let input = range.map(|r| headers(&[("range", r)])).unwrap_or_default();

            let result = server
                .proxy()
                .serve(ASSET, "preview", Method::GET, input)
                .await;

            assert_eq!(
                result.status().as_u16(),
                expected,
                "{upstream_status}, {range:?}, {content_range}"
            );

            if expected == 416 && !content_range.is_empty() {
                assert_eq!(result.headers()[header::CONTENT_RANGE], content_range);
            } else {
                assert!(!result.headers().contains_key(header::CONTENT_RANGE));
            }

            if expected != 502 {
                assert_eq!(result.headers()[header::ETAG], "\"v1\"");
                assert_eq!(result.headers()[header::ACCEPT_RANGES], "bytes");
            }

            for name in ["last-modified", "set-cookie", "x-private", "connection"] {
                assert!(!result.headers().contains_key(name), "{name}");
            }

            assert!(!result.headers().contains_key(header::CONTENT_TYPE));
            assert!(!result.headers().contains_key(header::CONTENT_LENGTH));
            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());

            assert_eq!(
                request_header(&server.request().await, "range"),
                range.filter(|value| ByteRange::parse(value).is_some())
            );
        }
    }

    #[tokio::test]
    async fn bodyless_conditionals_do_not_require_media_mime_or_forward_error_bodies() {
        for (status, extra, expected) in [
            (304, "", 304),
            (304, "Content-Length: 123\r\n", 304),
            (
                412,
                "Content-Type: application/json\r\nContent-Length: 6\r\n",
                412,
            ),
            (
                404,
                "Content-Type: application/json\r\nContent-Length: 6\r\n",
                404,
            ),
            (
                401,
                "Content-Type: application/json\r\nContent-Length: 6\r\n",
                502,
            ),
            (
                403,
                "Content-Type: application/json\r\nContent-Length: 6\r\n",
                502,
            ),
            (
                500,
                "Content-Type: application/json\r\nContent-Length: 6\r\n",
                502,
            ),
            (204, "", 502),
            (
                202,
                "Content-Type: image/jpeg\r\nContent-Length: 6\r\n",
                502,
            ),
        ] {
            let wire = format!(
                "HTTP/1.1 {status} Response\r\n{extra}ETag: \"v1\"\r\nConnection: close\r\n\r\nsecret"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;
            let proxy = server.proxy();

            let result = proxy
                .serve(ASSET, "preview", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status().as_u16(), expected);

            if expected != 502 {
                assert_eq!(result.headers()[header::ETAG], "\"v1\"");
            }

            if status == 304 && !extra.is_empty() {
                assert_eq!(result.headers()[header::CONTENT_LENGTH], "123");
            }

            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());

            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }
    }

    #[tokio::test]
    async fn mime_and_encoding_validation_precedes_body_commitment() {
        for (representation, extra, expected) in [
            ("original", "Content-Type: image/heic\r\n", 200),
            ("original", "Content-Type: video/x-matroska\r\n", 200),
            ("preview", "Content-Type: IMAGE/JPEG; quality=high\r\n", 200),
            (
                "preview",
                "Content-Type: image/jpeg; quality =high\r\n",
                200,
            ),
            (
                "preview",
                "Content-Type: image/jpeg; quality= high\r\n",
                200,
            ),
            (
                "original",
                "Content-Type: IMAGE/JPEG;; quality \t=\t \"high\"; ; x = y;\r\n",
                200,
            ),
            (
                "display",
                "Content-Type: image/jpeg; quality\t=\thigh\r\n",
                200,
            ),
            ("preview", "Content-Type: image/jpeg; x = \r\n", 502),
            (
                "preview",
                "Content-Type: image/jpeg; x = \"v\"oops\r\n",
                502,
            ),
            (
                "preview",
                "Content-Type: image/jpeg; x = \"bad\u{7f}\"\r\n",
                502,
            ),
            ("playback", "Content-Type: Video/MP4\r\n", 200),
            ("playback", "Content-Type: video/quicktime\r\n", 502),
            ("display", "Content-Type: image/webp\r\n", 502),
            ("preview", "Content-Type: application/json\r\n", 502),
            ("original", "", 502),
            ("original", "Content-Type: invalid\r\n", 502),
            ("original", "Content-Type: image/jpeg; broken\r\n", 502),
            ("original", "Content-Type: */*\r\n", 502),
            (
                "original",
                "Content-Type: image/jpeg\r\nContent-Type: image/png\r\n",
                502,
            ),
            (
                "original",
                "Content-Type: image/jpeg\r\nConnection: Content-Type\r\n",
                502,
            ),
            (
                "original",
                "Content-Type: image/jpeg\r\nContent-Encoding: identity\r\n",
                200,
            ),
            (
                "original",
                "Content-Type: image/jpeg\r\nContent-Encoding: gzip\r\n",
                502,
            ),
            (
                "original",
                "Content-Type: image/jpeg\r\nContent-Encoding: br\r\nConnection: Content-Encoding\r\n",
                502,
            ),
        ] {
            let wire = format!(
                "HTTP/1.1 200 OK\r\n{extra}Content-Length: 3\r\nConnection: close\r\n\r\nabc"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, representation, Method::GET, HeaderMap::new())
                .await;

            assert_eq!(
                result.status().as_u16(),
                expected,
                "{representation}: {extra}"
            );

            let body = to_bytes(result.into_body(), 1024).await.unwrap();

            assert_eq!(
                body.as_ref(),
                if expected == 200 {
                    b"abc".as_slice()
                } else {
                    b""
                }
            );
        }
    }

    #[tokio::test]
    async fn downstream_header_allowlist_strips_connection_nominated_values() {
        let wire = "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nETag: \"private\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nSet-Cookie: secret=value\r\nContent-Disposition: attachment; filename=secret.jpg\r\nCache-Control: public, max-age=3600\r\nServer: secret-server\r\nX-Private: secret\r\nConnection: close, ETag\r\nConnection: LAST-MODIFIED\r\n\r\nabc";

        let server = FakeServer::new(vec![Reply::new(wire)]).await;

        let result = server
            .proxy()
            .serve(ASSET, "original", Method::GET, HeaderMap::new())
            .await;

        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.headers()[header::CACHE_CONTROL], "private, no-cache");
        assert!(!result.headers().contains_key(header::SERVER));

        for name in [
            "etag",
            "last-modified",
            "set-cookie",
            "content-disposition",
            "connection",
            "x-private",
        ] {
            assert!(!result.headers().contains_key(name), "{name}");
        }
    }

    #[tokio::test]
    async fn trusted_redirects_preserve_head_range_conditionals_and_edit_selection() {
        for method in [Method::GET, Method::HEAD] {
            let redirect = format!(
                "HTTP/1.1 302 Found\r\nLocation: /prefix/api/assets/{ASSET}/thumbnail?edited=true&size=preview\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );

            let mut server = FakeServer::new(vec![Reply::new(&redirect), Reply::new(JPEG)]).await;
            let proxy = server.proxy();

            let result = proxy
                .serve(
                    ASSET,
                    "display",
                    method.clone(),
                    headers(&[
                        ("range", "bytes=0-2"),
                        ("if-range", "\"v1\""),
                        ("if-none-match", "\"v0\""),
                    ]),
                )
                .await;

            assert_eq!(result.status(), StatusCode::OK);
            let first = server.request().await;
            let second = server.request().await;

            assert!(second.starts_with(&format!(
                "{method} /prefix/api/assets/{ASSET}/thumbnail?edited=true&size=preview "
            )));

            for request in [&first, &second] {
                assert_eq!(request_header(request, "x-api-key"), Some("test-secret"));
                assert_eq!(request_header(request, "if-none-match"), Some("\"v0\""));
                assert_eq!(request_header(request, "accept-encoding"), Some("identity"));

                assert_eq!(
                    request_header(request, "range"),
                    (method == Method::GET).then_some("bytes=0-2")
                );

                assert_eq!(
                    request_header(request, "if-range"),
                    (method == Method::GET).then_some("\"v1\"")
                );
            }
        }
    }

    #[tokio::test]
    async fn unsafe_redirects_never_make_a_second_request() {
        let mut other = FakeServer::new(vec![Reply::new(JPEG)]).await;

        for target in [
            other
                .base
                .join(&format!("assets/{ASSET}/original"))
                .unwrap()
                .to_string(),
            format!("/api/assets/{ASSET}/original"),
            "/prefix/api/server/version".into(),
            "/prefix/api/assets/00000000-0000-0000-0000-000000000000/original".into(),
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview&edited=false"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview&edited=true&edited=false"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview&edited=true&secret=value"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview&edited=true#fragment"),
            format!("/prefix/api/assets/{ASSET}/video/playback"),
        ] {
            let wire = format!(
                "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );

            let mut server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, "display", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status(), StatusCode::BAD_GATEWAY, "{target}");
            server.request().await;
            assert!(server.requests.try_recv().is_err());
            assert!(other.requests.try_recv().is_err());
        }

        let proxy = MediaProxy::new(
            Url::parse("https://example.invalid/prefix/api/").unwrap(),
            HeaderValue::from_static("test-secret"),
        )
        .unwrap();

        let initial = proxy
            .api_base
            .join(&format!("assets/{ASSET}/original"))
            .unwrap();

        for target in [
            format!("http://example.invalid/prefix/api/assets/{ASSET}/original"),
            format!("https://user:pass@example.invalid/prefix/api/assets/{ASSET}/original"),
            format!("https://example.invalid:444/prefix/api/assets/{ASSET}/original"),
        ] {
            assert!(!proxy.allowed_redirect(
                &initial,
                &Url::parse(&target).unwrap(),
                Uuid::parse_str(ASSET).unwrap()
            ));
        }
    }

    #[tokio::test]
    async fn redirect_hop_limit_and_loops_are_enforced() {
        let targets = [
            format!("/prefix/api/assets/{ASSET}/thumbnail?size=preview&edited=true"),
            format!("/prefix/api/assets/{ASSET}/original?edited=true"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?edited=true&size=preview"),
            format!("/prefix/api/assets/{ASSET}/thumbnail?edited=true&size=fullsize"),
        ];

        for count in [3, 4] {
            let mut replies: Vec<_> = targets[..count].iter().map(|target| {
                Reply::new(&format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
            }).collect();

            replies.push(Reply::new(JPEG));
            let mut server = FakeServer::new(replies).await;

            let result = server
                .proxy()
                .serve(ASSET, "display", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(
                result.status(),
                if count == 3 {
                    StatusCode::OK
                } else {
                    StatusCode::BAD_GATEWAY
                }
            );

            for _ in 0..4 {
                server.request().await;
            }

            assert!(server.requests.try_recv().is_err());
        }

        let loop_reply = format!(
            "HTTP/1.1 308 Permanent Redirect\r\nLocation: /prefix/api/assets/{ASSET}/original\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );

        let mut server = FakeServer::new(vec![Reply::new(&loop_reply)]).await;

        assert_eq!(
            server
                .proxy()
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await
                .status(),
            StatusCode::BAD_GATEWAY
        );

        server.request().await;
        assert!(server.requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn immediate_admission_is_shared_by_clones_and_body_drop_releases_permits() {
        let mut server = FakeServer::new(vec![Reply::stalled(
            "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 100\r\n\r\nabc",
        )])
        .await;

        let proxy = server.proxy();
        let mut responses = Vec::new();

        for _ in 0..OPERATIONS {
            let result = proxy
                .clone()
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status(), StatusCode::OK);
            responses.push(result);
            server.request().await;
        }

        assert_eq!(proxy.operations.available_permits(), 0);

        let overloaded = tokio::time::timeout(
            Duration::from_millis(100),
            proxy.serve(ASSET, "original", Method::HEAD, HeaderMap::new()),
        )
        .await
        .unwrap();

        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(server.requests.try_recv().is_err());
        drop(responses.pop());
        assert_eq!(proxy.operations.available_permits(), 1);

        let result = proxy
            .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
            .await;

        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(proxy.operations.available_permits(), 1);
        drop(responses);

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    }

    #[tokio::test]
    async fn head_conditional_validation_and_upstream_errors_release_permits_immediately() {
        for (method, wire, expected) in [
            (Method::HEAD, JPEG, StatusCode::OK),
            (
                Method::GET,
                "HTTP/1.1 304 Not Modified\r\nConnection: close\r\n\r\n",
                StatusCode::NOT_MODIFIED,
            ),
            (
                Method::GET,
                "HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                StatusCode::PRECONDITION_FAILED,
            ),
            (
                Method::GET,
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                StatusCode::NOT_FOUND,
            ),
            (
                Method::GET,
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                StatusCode::RANGE_NOT_SATISFIABLE,
            ),
            (
                Method::GET,
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                StatusCode::BAD_GATEWAY,
            ),
            (
                Method::GET,
                "HTTP/1.1 200 OK\r\nContent-Type: invalid\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                StatusCode::BAD_GATEWAY,
            ),
            (Method::GET, "", StatusCode::BAD_GATEWAY),
        ] {
            let server = FakeServer::new(vec![Reply::new(wire)]).await;
            let proxy = server.proxy();

            let result = proxy
                .serve(
                    ASSET,
                    "original",
                    method.clone(),
                    headers(&[("range", "bytes=0-2")]),
                )
                .await;

            assert_eq!(result.status(), expected, "{method}: {wire}");
            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }
    }

    #[tokio::test]
    async fn completion_truncation_and_read_timeout_release_body_permits() {
        for reply in [
            Reply::new(JPEG),
            Reply::new(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabc",
            ),
            Reply::stalled(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 5\r\n\r\nabc",
            ),
        ] {
            let succeeds = reply.wire == JPEG;
            let server = FakeServer::new(vec![reply]).await;
            let mut proxy = server.proxy();
            proxy.read_timeout = Duration::from_millis(100);

            let result = proxy
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status(), StatusCode::OK);

            assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);

            let body = to_bytes(result.into_body(), 1024).await;
            assert_eq!(body.is_ok(), succeeds);

            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }
    }

    #[tokio::test]
    async fn chunked_partials_enforce_range_span_even_without_content_length() {
        for payload in [
            "2\r\nab\r\n0\r\n\r\n",
            "4\r\nabcd\r\n0\r\n\r\n",
            "3\r\nabc\r\n0\r\n\r\n",
        ] {
            let wire = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Range: bytes 0-2/10\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{payload}"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;
            let proxy = server.proxy();

            let result = proxy
                .serve(
                    ASSET,
                    "preview",
                    Method::GET,
                    headers(&[("range", "bytes=0-2")]),
                )
                .await;

            assert_eq!(result.status(), StatusCode::PARTIAL_CONTENT);
            assert!(!result.headers().contains_key(header::TRANSFER_ENCODING));

            assert_eq!(
                to_bytes(result.into_body(), 1024).await.is_ok(),
                payload.starts_with('3')
            );

            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }
    }

    #[tokio::test]
    async fn redirects_share_one_absolute_header_budget() {
        let redirect = format!(
            "HTTP/1.1 302 Found\r\nLocation: /prefix/api/assets/{ASSET}/video/playback\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );

        let mut server = FakeServer::new(vec![
            Reply {
                header_delay: Duration::from_secs(7),
                ..Reply::new(&redirect)
            },
            Reply {
                header_delay: Duration::from_secs(8),
                ..Reply::new(JPEG)
            },
        ])
        .await;

        let proxy = server.proxy();
        tokio::time::pause();

        let clock_guard = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });

        let mut serve = Box::pin(proxy.serve(ASSET, "original", Method::GET, HeaderMap::new()));

        // Sixteen seconds exceeds the chain budget even allowing timer resolution,
        // but remains below a wrongly restarted second-hop deadline.
        for elapsed in [8, 8] {
            tokio::select! {
                _ = &mut serve => panic!("response before upstream headers"),

                _ = server.request() => {}
            }

            tokio::time::advance(Duration::from_secs(elapsed)).await;
        }

        let result = serve
            .now_or_never()
            .expect("redirect must not reset the header budget");

        assert_eq!(result.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(server.requests.try_recv().is_err());
        clock_guard.abort();
        assert!(clock_guard.await.unwrap_err().is_cancelled());
        tokio::time::resume();
    }

    #[tokio::test]
    async fn ready_headers_at_or_after_deadline_are_rejected_without_an_extra_hop() {
        for (redirect, elapsed) in [
            (false, 14),
            (false, 15),
            (false, 16),
            (true, 15),
            (true, 16),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

            let proxy = MediaProxy::new(
                format!("http://{}/api/", listener.local_addr().unwrap())
                    .parse()
                    .unwrap(),
                HeaderValue::from_static("test-secret"),
            )
            .unwrap();

            tokio::time::pause();

            // Keep network waits from automatically advancing the paused clock.
            let clock_guard = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });

            let mut serve = Box::pin(proxy.serve(ASSET, "original", Method::GET, HeaderMap::new()));

            let mut socket = tokio::select! {
                _ = &mut serve => panic!("response before upstream headers"),

                accepted = listener.accept() => accepted.unwrap().0,
            };

            tokio::select! {
                _ = &mut serve => panic!("response before upstream headers"),

                _ = async {
                    let mut request = Vec::new();

                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(socket.read_u8().await.unwrap());
                    }
                } => {}
            }

            let wake = Arc::new(WakeSignal(tokio::sync::Notify::new()));
            let waker = Waker::from(wake.clone());

            assert!(
                serve
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );

            let wire = if redirect {
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: /api/assets/{ASSET}/video/playback\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            } else {
                JPEG.into()
            };

            socket.write_all(wire.as_bytes()).await.unwrap();
            wake.0.notified().await;
            tokio::time::advance(Duration::from_secs(elapsed)).await;
            let result = serve.await;

            assert_eq!(
                result.status(),
                if elapsed < 15 {
                    StatusCode::OK
                } else {
                    StatusCode::GATEWAY_TIMEOUT
                }
            );

            assert!(listener.accept().now_or_never().is_none());
            clock_guard.abort();
            assert!(clock_guard.await.unwrap_err().is_cancelled());
            tokio::time::resume();
        }
    }

    #[tokio::test]
    async fn chunk_reads_allow_backpressure_but_time_out_when_upstream_stalls() {
        for eof in [false, true] {
            for elapsed in [59, 60, 61] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

                let proxy = MediaProxy::new(
                    format!("http://{}/api/", listener.local_addr().unwrap())
                        .parse()
                        .unwrap(),
                    HeaderValue::from_static("test-secret"),
                )
                .unwrap();

                tokio::time::pause();

                let clock_guard = tokio::spawn(async {
                    loop {
                        tokio::task::yield_now().await;
                    }
                });

                let mut serve =
                    Box::pin(proxy.serve(ASSET, "original", Method::GET, HeaderMap::new()));

                let mut socket = tokio::select! {
                    _ = &mut serve => panic!("response before upstream headers"),

                    accepted = listener.accept() => accepted.unwrap().0,
                };

                tokio::select! {
                    _ = &mut serve => panic!("response before upstream headers"),

                    _ = async {
                        let mut request = Vec::new();

                        while !request.ends_with(b"\r\n\r\n") {
                            request.push(socket.read_u8().await.unwrap());
                        }
                    } => {}
                }

                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n").await.unwrap();
                let result = serve.await;
                assert_eq!(result.status(), StatusCode::OK);
                let mut stream = result.into_body().into_data_stream();
                assert_eq!(stream.next().await.unwrap().unwrap(), "a");

                // Backpressure is not upstream idleness: no next-chunk demand yet.
                tokio::time::advance(Duration::from_secs(120)).await;
                let mut next = Box::pin(stream.next());
                let wake = Arc::new(WakeSignal(tokio::sync::Notify::new()));
                let waker = Waker::from(wake.clone());

                assert!(
                    next.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );

                tokio::time::advance(Duration::from_secs(elapsed) + Duration::from_millis(1)).await;
                let _ = wake.0.notified().now_or_never();

                if elapsed < 60 {
                    socket
                        .write_all(if eof { b"0\r\n\r\n" } else { b"1\r\nb\r\n" })
                        .await
                        .unwrap();

                    wake.0.notified().await;
                }

                let result = next.await;

                if elapsed >= 60 {
                    let error = result.expect("stalled read must fail").unwrap_err();
                    assert!(error.to_string().contains("upstream deadline exceeded"));
                } else if eof {
                    assert!(result.is_none());
                } else {
                    assert_eq!(result.unwrap().unwrap(), "b");
                }

                drop(stream);

                assert_eq!(proxy.operations.available_permits(), OPERATIONS);

                clock_guard.abort();
                assert!(clock_guard.await.unwrap_err().is_cancelled());
                tokio::time::resume();
            }
        }
    }

    #[tokio::test]
    async fn header_deadline_and_request_cancellation_release_admission() {
        let reply = Reply {
            header_delay: Duration::from_secs(60),
            ..Reply::new(JPEG)
        };

        let mut server = FakeServer::new(vec![reply]).await;
        let mut proxy = server.proxy();
        proxy.header_timeout = Duration::from_millis(100);

        let result = proxy
            .serve(ASSET, "original", Method::GET, HeaderMap::new())
            .await;

        assert_eq!(result.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(proxy.operations.available_permits(), OPERATIONS);

        server.request().await;

        let cloned = proxy.clone();

        let task = tokio::spawn(async move {
            cloned
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await
        });

        server.request().await;

        assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    }

    #[tokio::test]
    async fn malformed_headers_and_early_disconnect_fail_without_disclosing_upstream_content() {
        for wire in [
            "",
            "not HTTP\r\nsecret\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: not-a-number\r\n\r\nsecret",
            "HTTP/1.1 302 Found\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 302 Found\r\nLocation: /one\r\nLocation: /two\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 302 Found\r\nLocation: /one\r\nConnection: Location\r\nContent-Length: 0\r\n\r\n",
        ] {
            let server = FakeServer::new(vec![Reply::new(wire)]).await;
            let proxy = server.proxy();

            let result = proxy
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status(), StatusCode::BAD_GATEWAY);
            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());

            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }
    }

    #[tokio::test]
    async fn head_rejects_unsolicited_partial_response_without_get_fallback() {
        let wire = "HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/10\r\nConnection: close\r\n\r\n";

        let mut server = FakeServer::new(vec![Reply::new(wire)]).await;

        let result = server
            .proxy()
            .serve(
                ASSET,
                "original",
                Method::HEAD,
                headers(&[("range", "bytes=0-2")]),
            )
            .await;

        assert_eq!(result.status(), StatusCode::BAD_GATEWAY);
        assert!(server.request().await.starts_with("HEAD "));
        assert!(server.requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn healthy_streams_have_progress_timeout_not_total_deadline() {
        let reply = Reply {
            chunks: vec![
                (Duration::from_millis(60), "1\r\nb\r\n".into()),
                (Duration::from_millis(60), "1\r\nc\r\n".into()),
                (Duration::from_millis(60), "0\r\n\r\n".into()),
            ],
            ..Reply::new(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\na\r\n",
            )
        };

        let server = FakeServer::new(vec![reply]).await;
        let mut proxy = server.proxy();
        proxy.header_timeout = Duration::from_millis(100);
        proxy.read_timeout = Duration::from_millis(150);

        let result = proxy
            .serve(ASSET, "preview", Method::GET, HeaderMap::new())
            .await;

        let mut stream = result.into_body().into_data_stream();
        assert_eq!(stream.next().await.unwrap().unwrap(), "a");

        assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);

        assert_eq!(stream.next().await.unwrap().unwrap(), "b");
        assert_eq!(stream.next().await.unwrap().unwrap(), "c");
        assert!(stream.next().await.is_none());

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    }

    #[tokio::test]
    async fn rejects_simultaneous_transfer_encoding_and_content_length_before_commitment() {
        for status in [200, 206, 304, 404, 412, 416, 302] {
            for nominated in ["", ", Content-Length", ", Transfer-Encoding"] {
                let content_range = match status {
                    206 => "Content-Range: bytes 0-2/10\r\n",
                    416 => "Content-Range: bytes */0\r\n",
                    _ => "",
                };

                let wire = format!(
                    "HTTP/1.1 {status} Response\r\nContent-Type: image/jpeg\r\nTransfer-Encoding: chunked\r\nContent-Length: 100\r\n{content_range}Location: /prefix/api/assets/{ASSET}/original\r\nConnection: close{nominated}\r\n\r\n3\r\nabc\r\n0\r\n\r\n"
                );

                let mut server = FakeServer::new(vec![Reply::new(&wire), Reply::new(JPEG)]).await;
                let proxy = server.proxy();

                let result = proxy
                    .serve(
                        ASSET,
                        "preview",
                        Method::GET,
                        headers(&[("range", "bytes=0-2")]),
                    )
                    .await;

                assert_eq!(
                    result.status(),
                    StatusCode::BAD_GATEWAY,
                    "{status}{nominated}"
                );
                assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
                server.request().await;
                assert!(server.requests.try_recv().is_err());

                assert_eq!(proxy.operations.available_permits(), OPERATIONS);
            }
        }
    }

    #[tokio::test]
    async fn validates_bodyless_304_content_length_before_preserving_headers() {
        for method in [Method::GET, Method::HEAD] {
            for (framing, expected) in [
                ("Content-Length: invalid\r\n", StatusCode::BAD_GATEWAY),
                (
                    "Content-Length: 3\r\nContent-Length: 4\r\n",
                    StatusCode::BAD_GATEWAY,
                ),
                (
                    "Content-Length: 3\r\nContent-Length: 3\r\n",
                    StatusCode::BAD_GATEWAY,
                ),
                ("Content-Length: 3, 3\r\n", StatusCode::BAD_GATEWAY),
                (
                    "Content-Length: 18446744073709551616\r\n",
                    StatusCode::BAD_GATEWAY,
                ),
                (
                    "Content-Length: invalid\r\nConnection: Content-Length\r\n",
                    StatusCode::BAD_GATEWAY,
                ),
                ("Content-Length: 123\r\n", StatusCode::NOT_MODIFIED),
                ("", StatusCode::NOT_MODIFIED),
            ] {
                let wire = format!(
                    "HTTP/1.1 304 Not Modified\r\n{framing}ETag: \"v1\"\r\nConnection: close\r\n\r\n"
                );

                let server = FakeServer::new(vec![Reply::new(&wire)]).await;

                let result = server
                    .proxy()
                    .serve(
                        ASSET,
                        "preview",
                        method.clone(),
                        headers(&[("if-none-match", "\"v1\"")]),
                    )
                    .await;

                assert_eq!(result.status(), expected, "{method}: {framing}");

                if expected == StatusCode::NOT_MODIFIED && !framing.is_empty() {
                    assert_eq!(result.headers()[header::CONTENT_LENGTH], "123");
                } else {
                    assert!(!result.headers().contains_key(header::CONTENT_LENGTH));
                }

                assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn rejects_partial_responses_contradicting_the_forwarded_range() {
        for (range, content_range) in [
            ("bytes=0-2", "bytes 7-9/10"),
            ("bytes=0-2", "bytes 1-3/10"),
            ("bytes=5-", "bytes 4-6/10"),
            ("bytes=10-", "bytes 7-9/10"),
            ("bytes=-0", "bytes 7-9/10"),
            ("bytes=-0", "bytes 7-9/*"),
            ("bytes=-3", "bytes 6-8/10"),
            ("bytes=-2", "bytes 7-9/*"),
            ("bytes=0-2", "bytes 7-9/*"),
            (
                "bytes=4294967296-",
                "bytes 4294967295-4294967297/5000000000",
            ),
        ] {
            let wire = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nContent-Range: {content_range}\r\nConnection: close\r\n\r\nabc"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, "preview", Method::GET, headers(&[("range", range)]))
                .await;

            assert_eq!(
                result.status(),
                StatusCode::BAD_GATEWAY,
                "{range}: {content_range}"
            );
            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn preserves_compatible_partial_fulfillment_of_single_ranges() {
        for (range, content_range) in [
            ("bytes=0-9", "bytes 0-2/10"),
            ("bytes=0-9", "bytes 4-6/10"),
            ("bytes=0-99", "bytes 7-9/10"),
            ("bytes=5-", "bytes 5-7/10"),
            ("bytes=5-", "bytes 7-9/10"),
            ("bytes=-5", "bytes 5-7/10"),
            ("bytes=-5", "bytes 7-9/10"),
            ("bytes=-99", "bytes 0-2/10"),
            ("bytes=-3", "bytes 7-9/*"),
            ("bytes=-5", "bytes 7-9/*"),
            ("bytes=0-99", "bytes 7-9/*"),
            (
                "bytes=4294967296-",
                "bytes 4294967297-4294967299/5000000000",
            ),
        ] {
            let wire = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nContent-Range: {content_range}\r\nConnection: close\r\n\r\nabc"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, "preview", Method::GET, headers(&[("range", range)]))
                .await;

            assert_eq!(
                result.status(),
                StatusCode::PARTIAL_CONTENT,
                "{range}: {content_range}"
            );
            assert_eq!(result.headers()[header::CONTENT_RANGE], content_range);
            assert_eq!(to_bytes(result.into_body(), 1024).await.unwrap(), "abc");
        }
    }

    #[tokio::test]
    async fn native_416_with_content_range_requires_consistent_unsatisfied_length() {
        for (range, length, expected) in [
            ("bytes=0-2", 10, StatusCode::BAD_GATEWAY),
            ("bytes=9-", 10, StatusCode::BAD_GATEWAY),
            ("bytes=-3", 10, StatusCode::BAD_GATEWAY),
            ("bytes=-99", 10, StatusCode::BAD_GATEWAY),
            ("bytes=10-", 10, StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=10-20", 10, StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=-0", 10, StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=-3", 0, StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=0-2", 0, StatusCode::RANGE_NOT_SATISFIABLE),
        ] {
            let wire = format!(
                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{length}\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecret"
            );

            let server = FakeServer::new(vec![Reply::new(&wire)]).await;

            let result = server
                .proxy()
                .serve(ASSET, "preview", Method::GET, headers(&[("range", range)]))
                .await;

            assert_eq!(result.status(), expected, "{range}: {length}");
            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
        }
    }

    #[test]
    fn strict_numeric_parsing() {
        assert_eq!(
            ByteRange::parse("bytes=0-18446744073709551615"),
            Some(ByteRange::From(0, Some(u64::MAX)))
        );

        assert_eq!(ByteRange::parse("bytes=-0"), Some(ByteRange::Suffix(0)));
        assert_eq!(ByteRange::parse("bytes=18446744073709551616-"), None);
        let requested = ByteRange::From(0, None);
        assert_eq!(
            partial_span("bytes 0-18446744073709551615/*", requested),
            None
        );
        assert_eq!(partial_span("bytes 0-2/3", requested), Some(3));
        assert_eq!(partial_span("bytes 0-2/2", requested), None);
    }
}
