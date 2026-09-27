use super::*;
use axum::body::to_bytes;
use futures_util::{FutureExt, StreamExt};
use std::{
    future::Future,
    task::{Context, Wake, Waker},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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
        MediaProxy::new(
            self.base.as_str().parse().unwrap(),
            HeaderValue::from_static("test-secret"),
            Activity::default(),
        )
        .unwrap()
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

fn proxy_for(listener: &TcpListener) -> MediaProxy {
    MediaProxy::new(
        format!("http://{}/api/", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        HeaderValue::from_static("test-secret"),
        Activity::default(),
    )
    .unwrap()
}

async fn read_request_headers(socket: &mut TcpStream) {
    let mut request = Vec::new();

    while !request.ends_with(b"\r\n\r\n") {
        request.push(socket.read_u8().await.unwrap());
    }
}

#[tokio::test]
async fn fixed_routes_normalize_uuids_and_use_only_media_endpoints() {
    let mut server = FakeServer::new(vec![
        Reply::new(JPEG),
        Reply::new(JPEG),
        Reply::new(JPEG),
        Reply::new("HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"),
    ])
    .await;

    let proxy = server.proxy();

    for (representation, endpoint) in [
        ("original", "original"),
        ("display", "thumbnail?size=fullsize&edited=true"),
        ("preview", "thumbnail?size=preview&edited=true"),
        ("playback", "video/playback"),
    ] {
        let result = proxy
            .serve(
                &ASSET.to_uppercase(),
                representation,
                Method::GET,
                HeaderMap::new(),
            )
            .await;

        assert_eq!(result.status(), StatusCode::OK);

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

    let result = proxy
        .serve(
            ASSET,
            "original",
            Method::GET,
            headers(&[
                ("range", "bytes=0-1,3-4"),
                ("if-range", "\"old\""),
                ("if-none-match", "\"v1\""),
            ]),
        )
        .await;

    assert_eq!(result.status(), StatusCode::OK);
    drop(result);
    let request = server.request().await;
    assert!(request_header(&request, "range").is_none());
    assert!(request_header(&request, "if-range").is_none());
    assert_eq!(request_header(&request, "if-none-match"), Some("\"v1\""));

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
async fn preserves_partial_response_headers_body_and_wide_offsets() {
    let range = "bytes=4294967296-4294967298";
    let content_range = "bytes 4294967296-4294967298/5000000000";

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

#[tokio::test]
async fn rejects_invalid_partial_framing_before_streaming() {
    let valid_mime = "image/jpeg";

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
            .serve(ASSET, "original", Method::GET, input)
            .await;

        assert_eq!(result.status(), StatusCode::BAD_GATEWAY, "{framing}");

        assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn all_routes_require_content_length_to_match_the_partial_span() {
    for (representation, valid_mime) in [
        ("original", "image/jpeg"),
        ("display", "image/jpeg"),
        ("preview", "image/jpeg"),
        ("playback", "video/mp4"),
    ] {
        for (payload, expected, expected_body) in [
            ("abc", StatusCode::PARTIAL_CONTENT, "abc"),
            ("abcd", StatusCode::BAD_GATEWAY, ""),
        ] {
            let length = payload.len();

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

            assert_eq!(body.as_ref(), expected_body.as_bytes());
        }
    }
}

#[tokio::test]
async fn unsatisfied_ranges_normalize_only_proven_immich_404s() {
    for (range, content_range, expected) in [
        (Some("bytes=10-"), "", 404),
        (None, "", 404),
        (Some("bytes=10-"), "bytes */10", 416),
        (Some("bytes=9-"), "bytes */10", 404),
        (Some("bytes=0-1,4-5"), "bytes */0", 404),
        (None, "bytes */0", 404),
        (Some("bytes=10-"), "bytes */18446744073709551616", 404),
    ] {
        assert_unsatisfied_response(404, range, content_range, expected).await;
    }
}

#[tokio::test]
async fn native_416_requires_consistent_unsatisfied_framing() {
    for (range, content_range, expected) in [
        (Some("bytes=0-2"), "", 416),
        (None, "", 502),
        (Some("bytes=0-1,4-5"), "", 502),
        (Some("bytes=0-2"), "bytes */10", 502),
        (Some("bytes=10-"), "bytes */10", 416),
        (Some("bytes=10-"), "bytes 0-2/10", 502),
        (Some("bytes=10-"), "garbage", 502),
        (Some("bytes=10-"), "bytes */18446744073709551616", 502),
        (
            Some("bytes=10-"),
            "bytes */10\r\nContent-Range: bytes */10",
            502,
        ),
        (
            Some("bytes=10-"),
            "bytes */10\r\nConnection: Content-Range",
            502,
        ),
        (Some("bytes=10-"), "bytes */*", 502),
        (None, "bytes */10", 502),
    ] {
        assert_unsatisfied_response(416, range, content_range, expected).await;
    }
}

async fn assert_unsatisfied_response(
    upstream_status: u16,
    range: Option<&str>,
    content_range: &str,
    expected: u16,
) {
    let framing = if content_range.is_empty() {
        String::new()
    } else {
        format!("Content-Range: {content_range}\r\n")
    };

    let wire = format!(
        "HTTP/1.1 {upstream_status} Error\r\nContent-Type: application/json\r\nContent-Length: 6\r\n{framing}ETag: \"v1\"\r\nAccept-Ranges: bytes\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nSet-Cookie: secret=value\r\nX-Private: secret\r\nConnection: close, Last-Modified\r\n\r\nsecret"
    );

    let server = FakeServer::new(vec![Reply::new(&wire)]).await;
    let input = range.map(|r| headers(&[("range", r)])).unwrap_or_default();
    let proxy = server.proxy();

    let result = proxy.serve(ASSET, "preview", Method::GET, input).await;

    assert_eq!(proxy.operations.available_permits(), OPERATIONS);

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
}

#[tokio::test]
async fn upstream_errors_map_status_without_forwarding_error_bodies() {
    for (status, extra, expected) in [
        (
            412,
            "Content-Type: application/json\r\nContent-Length: 6\r\n",
            412,
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

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
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
        ("preview", "Content-Type: image/jpeg; x = \r\n", 502),
        ("playback", "Content-Type: Video/MP4\r\n", 200),
        ("playback", "Content-Type: video/quicktime\r\n", 502),
        ("display", "Content-Type: image/webp\r\n", 502),
        ("preview", "Content-Type: application/json\r\n", 502),
        ("original", "", 502),
        ("original", "Content-Type: invalid\r\n", 502),
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
        let wire =
            format!("HTTP/1.1 200 OK\r\n{extra}Content-Length: 3\r\nConnection: close\r\n\r\nabc");

        let server = FakeServer::new(vec![Reply::new(&wire)]).await;
        let proxy = server.proxy();

        let result = proxy
            .serve(ASSET, representation, Method::GET, HeaderMap::new())
            .await;

        assert_eq!(
            result.status().as_u16(),
            expected,
            "{representation}: {extra}"
        );

        if expected != 200 {
            assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        }

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
}

#[test]
fn redirect_policy_rejects_https_downgrades_userinfo_and_port_changes() {
    let api_base = Url::parse("https://example.invalid/prefix/api/").unwrap();

    for target in [
        format!("http://example.invalid/prefix/api/assets/{ASSET}/original"),
        format!("https://user:pass@example.invalid/prefix/api/assets/{ASSET}/original"),
        format!("https://example.invalid:444/prefix/api/assets/{ASSET}/original"),
    ] {
        assert!(!allowed_redirect(
            &api_base,
            false,
            &Url::parse(&target).unwrap(),
            Uuid::parse_str(ASSET).unwrap()
        ));
    }
}

#[test]
fn redirect_policy_requires_one_valid_size_only_for_thumbnails() {
    let api_base = Url::parse("https://example.invalid/prefix/api/").unwrap();
    let asset = Uuid::parse_str(ASSET).unwrap();

    for (endpoint, allowed) in [
        ("original", true),
        ("original?size=preview", false),
        ("video/playback", true),
        ("video/playback?size=fullsize", false),
        ("thumbnail?size=preview", true),
        ("thumbnail?size=fullsize", true),
        ("thumbnail?size=%70review", true),
        ("thumbnail", false),
        ("thumbnail?size=", false),
        ("thumbnail?size=thumbnail", false),
        ("thumbnail?size=PREVIEW", false),
        ("thumbnail?size=preview&size=preview", false),
        ("thumbnail?size=fullsize&size=preview", false),
        ("thumbnail?size=preview&size=fullsize", false),
        ("thumbnail?size=preview&%73ize=fullsize", false),
    ] {
        let target = api_base
            .join(&format!("assets/{asset}/{endpoint}"))
            .unwrap();

        assert_eq!(
            allowed_redirect(&api_base, false, &target, asset),
            allowed,
            "{endpoint}"
        );
    }
}

#[test]
fn redirect_policy_preserves_edit_selection() {
    let api_base = Url::parse("https://example.invalid/prefix/api/").unwrap();
    let asset = Uuid::parse_str(ASSET).unwrap();

    for (endpoint, unedited, edited) in [
        ("original", true, false),
        ("original?edited=false", true, false),
        ("original?edited=true", false, true),
        ("thumbnail?size=preview", true, false),
        ("thumbnail?size=preview&edited=false", true, false),
        ("thumbnail?size=preview&edited=true", false, true),
        ("video/playback", true, false),
        ("video/playback?edited=false", false, false),
        ("video/playback?edited=true", false, false),
        ("original?edited=", false, false),
        ("original?edited=TRUE", false, false),
        ("original?edited=false&edited=false", false, false),
        ("original?edited=true&edited=true", false, false),
        ("original?edited=true&edited=false", false, false),
    ] {
        let target = api_base
            .join(&format!("assets/{asset}/{endpoint}"))
            .unwrap();

        for (initial_edited, allowed) in [(false, unedited), (true, edited)] {
            assert_eq!(
                allowed_redirect(&api_base, initial_edited, &target, asset),
                allowed,
                "{endpoint}: initial_edited={initial_edited}"
            );
        }
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

    for (count, expected) in [(3, StatusCode::OK), (4, StatusCode::BAD_GATEWAY)] {
        let mut replies: Vec<_> = targets[..count].iter().map(|target| {
            Reply::new(&format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
        }).collect();

        replies.push(Reply::new(JPEG));
        let mut server = FakeServer::new(replies).await;

        let result = server
            .proxy()
            .serve(ASSET, "display", Method::GET, HeaderMap::new())
            .await;

        assert_eq!(result.status(), expected, "{count} redirects");

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
    let activity = proxy.activity.subscribe();
    assert_eq!(activity.borrow().media, OPERATIONS);
    let before_overload = activity.borrow().last;

    let overloaded = tokio::time::timeout(
        Duration::from_millis(100),
        proxy.serve(ASSET, "original", Method::HEAD, HeaderMap::new()),
    )
    .await
    .unwrap();

    assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(activity.borrow().last, before_overload);
    assert!(server.requests.try_recv().is_err());
    drop(responses.pop());
    assert_eq!(proxy.operations.available_permits(), 1);
    assert_eq!(activity.borrow().media, OPERATIONS - 1);

    let result = proxy
        .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
        .await;

    assert_eq!(result.status(), StatusCode::OK);
    assert_eq!(proxy.operations.available_permits(), 1);
    assert_eq!(activity.borrow().media, OPERATIONS - 1);
    drop(responses);

    assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    assert_eq!(activity.borrow().media, 0);
    assert_eq!(
        activity.borrow().idle_deadline(),
        activity
            .borrow()
            .last
            .map(|last| last + crate::activity::IDLE_TIMEOUT)
    );
}

#[tokio::test(start_paused = true)]
async fn completion_truncation_and_read_timeout_release_body_permits() {
    for (reply, succeeds) in [
        (Reply::new(JPEG), true),
        (
            Reply::new(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabc",
            ),
            false,
        ),
        (
            Reply::stalled(
                "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 5\r\n\r\nabc",
            ),
            false,
        ),
    ] {
        let server = FakeServer::new(vec![reply]).await;
        let proxy = server.proxy();

        let result = proxy
            .serve(ASSET, "original", Method::GET, HeaderMap::new())
            .await;

        assert_eq!(result.status(), StatusCode::OK);

        assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);
        let activity = proxy.activity.subscribe();
        assert_eq!(activity.borrow().media, 1);

        let body = to_bytes(result.into_body(), 1024).await;
        assert_eq!(body.is_ok(), succeeds);

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        assert_eq!(activity.borrow().media, 0);
        assert_eq!(activity.borrow().last, Some(Instant::now()));
    }
}

#[tokio::test]
async fn chunked_partials_enforce_range_span_even_without_content_length() {
    for (payload, succeeds) in [
        ("2\r\nab\r\n0\r\n\r\n", false),
        ("4\r\nabcd\r\n0\r\n\r\n", false),
        ("3\r\nabc\r\n0\r\n\r\n", true),
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

        assert_eq!(to_bytes(result.into_body(), 1024).await.is_ok(), succeeds);

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
    for (redirect, elapsed, expected) in [
        (false, 14, StatusCode::OK),
        (false, 15, StatusCode::GATEWAY_TIMEOUT),
        (false, 16, StatusCode::GATEWAY_TIMEOUT),
        (true, 15, StatusCode::GATEWAY_TIMEOUT),
        (true, 16, StatusCode::GATEWAY_TIMEOUT),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy_for(&listener);
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

            _ = read_request_headers(&mut socket) => {}
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
            expected,
            "redirect={redirect}, elapsed={elapsed}"
        );

        assert!(listener.accept().now_or_never().is_none());
        clock_guard.abort();
        assert!(clock_guard.await.unwrap_err().is_cancelled());
        tokio::time::resume();
    }
}

#[tokio::test]
async fn chunk_reads_allow_backpressure_but_time_out_when_upstream_stalls() {
    for (eof, elapsed) in [(false, 59), (true, 59), (false, 60), (false, 61)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy_for(&listener);
        tokio::time::pause();

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

            _ = read_request_headers(&mut socket) => {}
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

#[tokio::test(start_paused = true)]
async fn header_deadline_and_request_cancellation_release_admission() {
    let reply = Reply {
        header_delay: Duration::from_secs(60),
        ..Reply::new(JPEG)
    };

    let mut server = FakeServer::new(vec![reply]).await;
    let proxy = server.proxy();

    let result = proxy
        .serve(ASSET, "original", Method::GET, HeaderMap::new())
        .await;

    assert_eq!(result.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    let activity = proxy.activity.subscribe();
    assert_eq!(activity.borrow().media, 0);

    server.request().await;

    let cloned = proxy.clone();

    let task = tokio::spawn(async move {
        cloned
            .serve(ASSET, "original", Method::GET, HeaderMap::new())
            .await
    });

    server.request().await;

    assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);
    assert_eq!(activity.borrow().media, 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    assert_eq!(activity.borrow().media, 0);
    assert_eq!(activity.borrow().last, Some(Instant::now()));
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
        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
        assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
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

#[tokio::test(start_paused = true)]
async fn healthy_streams_have_progress_timeout_not_total_deadline() {
    let reply = Reply {
        chunks: std::iter::repeat_n((Duration::from_secs(59), "1\r\nb\r\n".into()), 11)
            .chain(std::iter::once((
                Duration::from_secs(59),
                "0\r\n\r\n".into(),
            )))
            .collect(),
        ..Reply::new(
            "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\na\r\n",
        )
    };

    let server = FakeServer::new(vec![reply]).await;
    let proxy = server.proxy();
    let activity = proxy.activity.subscribe();

    let result = proxy
        .serve(ASSET, "preview", Method::GET, HeaderMap::new())
        .await;

    let mut stream = result.into_body().into_data_stream();
    assert_eq!(stream.next().await.unwrap().unwrap(), "a");

    assert_eq!(proxy.operations.available_permits(), OPERATIONS - 1);

    for _ in 0..11 {
        assert_eq!(stream.next().await.unwrap().unwrap(), "b");
        assert_eq!(activity.borrow().media, 1);
        assert!(activity.borrow().active(Instant::now()));
    }

    assert!(stream.next().await.is_none());

    assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    assert_eq!(activity.borrow().media, 0);
    assert_eq!(activity.borrow().last, Some(Instant::now()));
}

#[tokio::test]
async fn rejected_media_does_not_report_activity_and_bodyless_outcomes_release_it() {
    let server = FakeServer::new(vec![Reply::new(
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )])
    .await;

    let proxy = server.proxy();
    let activity = proxy.activity.subscribe();

    for (id, route, method) in [
        ("invalid", "original", Method::GET),
        (ASSET, "invalid", Method::GET),
        (ASSET, "original", Method::POST),
    ] {
        assert!(
            !proxy
                .serve(id, route, method, HeaderMap::new())
                .await
                .status()
                .is_success()
        );
        assert_eq!(activity.borrow().last, None);
        assert_eq!(activity.borrow().media, 0);
    }

    for method in [Method::HEAD, Method::GET] {
        assert_eq!(
            proxy
                .serve(ASSET, "original", method, HeaderMap::new())
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        assert!(activity.borrow().last.is_some());
        assert_eq!(activity.borrow().media, 0);
    }
}

#[tokio::test]
async fn rejects_simultaneous_transfer_encoding_and_content_length_before_commitment() {
    for (status, nominated) in [
        (200, ""),
        (206, ""),
        (304, ""),
        (404, ""),
        (412, ""),
        (416, ""),
        (302, ""),
        (200, ", Content-Length"),
        (200, ", Transfer-Encoding"),
    ] {
        let content_range = match status {
            206 => "Content-Range: bytes 0-2/10\r\n",
            416 => "Content-Range: bytes */0\r\n",
            _ => "",
        };

        let wire = format!(
            "HTTP/1.1 {status} Response\r\nContent-Type: image/jpeg\r\nTransfer-Encoding: chunked\r\nContent-Length: 100\r\n{content_range}Location: /prefix/api/assets/{ASSET}/original?edited=true\r\nConnection: close{nominated}\r\n\r\n3\r\nabc\r\n0\r\n\r\n"
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

#[tokio::test]
async fn validates_bodyless_304_content_length_before_preserving_headers() {
    for (method, framing, expected) in [
        (
            Method::GET,
            "Content-Length: invalid\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: 3\r\nContent-Length: 4\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: 3\r\nContent-Length: 3\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: 3, 3\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: 18446744073709551616\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: invalid\r\nConnection: Content-Length\r\n",
            StatusCode::BAD_GATEWAY,
        ),
        (
            Method::GET,
            "Content-Length: 123\r\n",
            StatusCode::NOT_MODIFIED,
        ),
        (Method::GET, "", StatusCode::NOT_MODIFIED),
        (
            Method::HEAD,
            "Content-Length: 123\r\n",
            StatusCode::NOT_MODIFIED,
        ),
        (
            Method::HEAD,
            "Content-Length: invalid\r\n",
            StatusCode::BAD_GATEWAY,
        ),
    ] {
        let wire = format!(
            "HTTP/1.1 304 Not Modified\r\n{framing}ETag: \"v1\"\r\nConnection: close\r\n\r\n"
        );

        let server = FakeServer::new(vec![Reply::new(&wire)]).await;
        let proxy = server.proxy();

        let result = proxy
            .serve(
                ASSET,
                "preview",
                method.clone(),
                headers(&[("if-none-match", "\"v1\"")]),
            )
            .await;

        assert_eq!(result.status(), expected, "{method}: {framing}");
        assert_eq!(proxy.operations.available_permits(), OPERATIONS);

        if expected == StatusCode::NOT_MODIFIED {
            assert_eq!(result.headers()[header::ETAG], "\"v1\"");
        }

        if expected == StatusCode::NOT_MODIFIED && !framing.is_empty() {
            assert_eq!(result.headers()[header::CONTENT_LENGTH], "123");
        } else {
            assert!(!result.headers().contains_key(header::CONTENT_LENGTH));
        }

        assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
    }
}

#[test]
fn rejects_partial_spans_contradicting_the_requested_range() {
    for (range, content_range) in [
        ("bytes=0-2", "bytes 0-3/10"),
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
        assert_eq!(
            partial_span(content_range, ByteRange::parse(range).unwrap()),
            None,
            "{range}: {content_range}"
        );
    }
}

#[test]
fn accepts_compatible_partial_spans_of_single_ranges() {
    for (range, content_range) in [
        ("bytes=0-2", "bytes 0-2/10"),
        ("bytes=7-", "bytes 7-9/10"),
        ("bytes=-3", "bytes 7-9/10"),
        ("bytes=0-2", "bytes 0-2/*"),
        (
            "bytes=4294967296-4294967298",
            "bytes 4294967296-4294967298/5000000000",
        ),
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
        assert_eq!(
            partial_span(content_range, ByteRange::parse(range).unwrap()),
            Some(3),
            "{range}: {content_range}"
        );
    }
}

#[tokio::test]
async fn partial_response_compatibility_is_checked_before_streaming() {
    for (range, expected) in [
        ("bytes=0-9", StatusCode::PARTIAL_CONTENT),
        ("bytes=0-2", StatusCode::BAD_GATEWAY),
    ] {
        let wire = "HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nContent-Range: bytes 4-6/10\r\nConnection: close\r\n\r\nabc";
        let mut server = FakeServer::new(vec![Reply::new(wire)]).await;
        let proxy = server.proxy();

        let result = proxy
            .serve(ASSET, "preview", Method::GET, headers(&[("range", range)]))
            .await;

        assert_eq!(result.status(), expected, "{range}");

        assert_eq!(
            request_header(&server.request().await, "range"),
            Some(range)
        );

        if expected == StatusCode::PARTIAL_CONTENT {
            assert_eq!(result.headers()[header::CONTENT_RANGE], "bytes 4-6/10");
            assert_eq!(result.headers()[header::CONTENT_LENGTH], "3");
            assert_eq!(to_bytes(result.into_body(), 1024).await.unwrap(), "abc");
        } else {
            assert!(!result.headers().contains_key(header::CONTENT_RANGE));
            assert!(to_bytes(result.into_body(), 1024).await.unwrap().is_empty());
        }

        assert_eq!(proxy.operations.available_permits(), OPERATIONS);
    }
}

#[test]
fn single_range_parsing_requires_supported_units_and_valid_offsets() {
    for (value, expected) in [
        (
            "bytes=0-18446744073709551615",
            Some(ByteRange::From(0, Some(u64::MAX))),
        ),
        ("bytes=7-", Some(ByteRange::From(7, None))),
        ("bytes=-3", Some(ByteRange::Suffix(3))),
        ("bytes=-0", Some(ByteRange::Suffix(0))),
        ("bytes=0-1,3-4", None),
        ("items=0-1", None),
        ("bytes=3-1", None),
        ("bytes=18446744073709551616-", None),
        ("bytes=-", None),
        ("bytes=+1-2", None),
    ] {
        assert_eq!(ByteRange::parse(value), expected, "{value}");
    }
}

#[test]
fn partial_span_requires_ordered_offsets_valid_totals_and_representable_length() {
    for (value, expected) in [
        ("bytes 3-2/10", None),
        ("bytes 0-2/2", None),
        ("bytes 0-18446744073709551615/*", None),
        ("bytes 0-2/3", Some(3)),
    ] {
        assert_eq!(
            partial_span(value, ByteRange::From(0, None)),
            expected,
            "{value}"
        );
    }
}

#[test]
fn unsatisfiable_ranges_compare_start_or_suffix_with_resource_length() {
    for (range, length, expected) in [
        (ByteRange::From(10, None), 10, true),
        (ByteRange::From(10, Some(20)), 10, true),
        (ByteRange::From(9, None), 10, false),
        (ByteRange::From(0, Some(2)), 10, false),
        (ByteRange::From(0, Some(2)), 0, true),
        (ByteRange::Suffix(0), 10, true),
        (ByteRange::Suffix(3), 0, true),
        (ByteRange::Suffix(3), 10, false),
        (ByteRange::Suffix(99), 10, false),
    ] {
        assert_eq!(range.unsatisfiable(length), expected, "{range:?}: {length}");
    }
}
