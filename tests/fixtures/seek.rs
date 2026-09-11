//! Fixture-only comparison of otherwise identical original-video responses.

use std::sync::{Arc, Mutex};

use axum::{Router, body::Body, extract::Request, response::Response};
use http::{HeaderValue, Method, StatusCode, header};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use immich_dlna_proxy::{
    deadline::timeout_at,
    limits,
    media::MediaProxy,
    transport::{self, WriteDeadline},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::OwnedSemaphorePermit,
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const ASSET: &str = "20000000-0000-4000-8000-000000000003";
const CAPABILITY: &str = "contentfeatures.dlna.org";

/// Both cases use the caller's proxy, including its credentials and operation permits.
pub async fn response(media: &MediaProxy, request: Request) -> Response {
    let (parts, body) = request.into_parts();

    let case = match (parts.uri.path(), parts.uri.query()) {
        ("/control", None) => "control",
        ("/byte-seek", None) => "byte-seek",
        ("/both", None) => "both",
        ("/didl-only", None) => "didl-only",
        _ => "rejected",
    };

    let mut result = async {
        let header_bytes = parts.method.as_str().len()
            + parts.uri.to_string().len()
            + 14
            + parts
                .headers
                .iter()
                .map(|(name, value)| name.as_str().len() + value.len() + 4)
                .sum::<usize>();

        if header_bytes > limits::HEADER_BYTES {
            return empty(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
        }

        if case == "rejected" {
            return empty(StatusCode::NOT_FOUND);
        }

        if parts.method != Method::GET && parts.method != Method::HEAD {
            let mut result = empty(StatusCode::METHOD_NOT_ALLOWED);

            result
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));

            return result;
        }

        let mut lengths = parts.headers.get_all(header::CONTENT_LENGTH).iter();

        let empty_length = match lengths.next() {
            None => true,
            Some(value) => value.to_str().is_ok_and(|value| {
                let value = value.trim();

                !value.is_empty() && value.bytes().all(|byte| byte == b'0')
            }),
        };

        if !empty_length
            || lengths.next().is_some()
            || parts.headers.contains_key(header::TRANSFER_ENCODING)
            || parts.headers.contains_key(header::UPGRADE)
        {
            return empty(StatusCode::BAD_REQUEST);
        }

        // Also bound bodies supplied directly by callers, not just Hyper's framing.
        if !matches!(
            timeout_at(
                Instant::now() + limits::BODY_TIMEOUT,
                axum::body::to_bytes(body, 0),
            )
            .await,
            Ok(Ok(_))
        ) {
            return empty(StatusCode::BAD_REQUEST);
        }

        let mut result = media
            .serve(ASSET, "original", parts.method, parts.headers)
            .await;

        if matches!(case, "byte-seek" | "both")
            && matches!(
                result.status(),
                StatusCode::OK | StatusCode::PARTIAL_CONTENT
            )
        {
            result
                .headers_mut()
                .insert(CAPABILITY, HeaderValue::from_static("DLNA.ORG_OP=01"));
        }

        result
    }
    .await;

    result
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));

    tracing::info!(
        case,
        status = result.status().as_u16(),
        capability_present = result.headers().contains_key(CAPABILITY),
        "fixture seek response"
    );

    result
}

fn empty(status: StatusCode) -> Response {
    let mut result = Response::new(Body::empty());
    *result.status_mut() = status;

    result
}

/// Stop accepting on cancellation, then drain admitted connections for one grace period.
/// The caller must keep the upstream alive until this and the primary listener finish.
pub async fn run(
    listener: TcpListener,
    media: MediaProxy,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    let mut failure = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,

            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    failure = Some(anyhow::anyhow!("fixture seek connection task failed: {error}"));
                    break;
                }
            }

            accepted = listener.accept() => {
                let (socket, _) = match accepted {
                    Ok(accepted) => accepted,

                    Err(error) => {
                        failure = Some(anyhow::anyhow!("fixture seek listener accept failed: {error}"));
                        break;
                    }
                };

                if shutdown.is_cancelled() {
                    break;
                }

                // Completed but unreaped tasks count too, bounding the results backlog.
                if tasks.len() >= limits::CONNECTIONS {
                    let _ = socket.try_write(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
                    continue;
                }

                tasks.spawn(connection(socket, media.clone()));
            }
        }
    }

    drop(listener);

    if failure.is_some() {
        tasks.abort_all();
    }

    let deadline = Instant::now() + limits::SHUTDOWN_GRACE;

    while !tasks.is_empty() {
        match timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(Err(error))) if error.is_panic() => {
                failure = Some(anyhow::anyhow!(
                    "fixture seek connection task panicked: {error}"
                ));

                tasks.abort_all();
            }

            Ok(_) => {}

            Err(_) => {
                tasks.abort_all();
                break;
            }
        }
    }

    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined
            && error.is_panic()
        {
            failure = Some(anyhow::anyhow!(
                "fixture seek connection task panicked: {error}"
            ));
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn connection<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(io: T, media: MediaProxy) {
    let permit = Arc::new(Mutex::new(None));
    let admitted = permit.clone();

    let router = Router::new().fallback(move |request| {
        let media = media.clone();
        let admitted = admitted.clone();

        async move {
            let mut response = response(&media, request).await;

            *admitted.lock().unwrap() = response
                .extensions_mut()
                .remove::<Arc<OwnedSemaphorePermit>>();

            response
        }
    });

    let transport = WriteDeadline::new(io, limits::STREAM_IDLE_TIMEOUT);

    if transport::http1(limits::HEADER_TIMEOUT)
        .serve_connection(TokioIo::new(transport), TowerToHyperService::new(router))
        .await
        .is_err()
    {
        // Client framing, disconnects and timeouts are not listener failures.
        tracing::debug!("fixture seek connection transport or framing failure");
    }

    // Hyper can consume the body before its buffered bytes reach the transport.
    drop(permit);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use http::HeaderMap;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpStream,
        task::JoinHandle,
        time::timeout,
    };

    const KEY: &str = "seek-test-upstream-secret";
    const BYTES: &[u8] = b"unchanged original video bytes";

    struct Upstream {
        media: MediaProxy,
        requests: Arc<Mutex<Vec<(Method, HeaderMap)>>>,
        release: CancellationToken,
        task: JoinHandle<()>,
    }

    impl Upstream {
        async fn start() -> Self {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

            let media = MediaProxy::new(
                format!("http://{}/api/", listener.local_addr().unwrap())
                    .parse()
                    .unwrap(),
                HeaderValue::from_static(KEY),
            )
            .unwrap();

            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let release = CancellationToken::new();
            let released = release.clone();

            let router = Router::new().fallback(move |request: Request| {
                let recorded = recorded.clone();
                let released = released.clone();

                async move {
                    assert_eq!(
                        request.uri().path(),
                        format!("/api/assets/{ASSET}/original")
                    );

                    assert!(request.uri().query().is_none());
                    let method = request.method().clone();
                    let headers = request.headers();
                    assert_eq!(headers["x-api-key"], KEY);
                    assert_eq!(headers[header::ACCEPT_ENCODING], "identity");
                    assert!(!headers.contains_key(header::AUTHORIZATION));
                    assert!(!headers.contains_key(header::COOKIE));
                    assert!(!headers.contains_key("x-client-secret"));
                    assert!(!headers.contains_key("getcontentfeatures.dlna.org"));

                    assert!(
                        !headers[header::HOST]
                            .to_str()
                            .unwrap()
                            .contains("untrusted")
                    );

                    recorded
                        .lock()
                        .unwrap()
                        .push((method.clone(), headers.clone()));

                    let condition = headers
                        .get(header::IF_NONE_MATCH)
                        .and_then(|value| value.to_str().ok());

                    if condition == Some("\"headers\"") {
                        released.cancelled().await;
                    }

                    let range = headers
                        .get(header::RANGE)
                        .and_then(|value| value.to_str().ok());

                    let status = match condition {
                        Some("\"cached\"") => StatusCode::NOT_MODIFIED,
                        Some("\"missing\"") => StatusCode::NOT_FOUND,
                        Some("\"denied\"") => StatusCode::FORBIDDEN,
                        Some("\"broken\"") => StatusCode::INTERNAL_SERVER_ERROR,
                        _ if range == Some("bytes=999-") => StatusCode::RANGE_NOT_SATISFIABLE,
                        _ if range.is_some() => StatusCode::PARTIAL_CONTENT,
                        _ => StatusCode::OK,
                    };

                    let bytes = if status == StatusCode::PARTIAL_CONTENT {
                        &BYTES[2..6]
                    } else {
                        BYTES
                    };

                    let mut result = Response::builder()
                        .status(status)
                        .header(header::CONTENT_TYPE, "video/mp4")
                        .header(header::ACCEPT_RANGES, "bytes")
                        .header(header::ETAG, "\"original\"")
                        .header(header::LAST_MODIFIED, "Wed, 01 Jan 2025 00:00:00 GMT")
                        .header("x-api-key", KEY)
                        .header(header::SET_COOKIE, "private=secret")
                        .header(CAPABILITY, "untrusted-upstream-value");

                    if status == StatusCode::PARTIAL_CONTENT {
                        result = result
                            .header(header::CONTENT_RANGE, format!("bytes 2-5/{}", BYTES.len()));
                    }

                    if status == StatusCode::RANGE_NOT_SATISFIABLE {
                        result = result
                            .header(header::CONTENT_RANGE, format!("bytes */{}", BYTES.len()));
                    }

                    if status.is_success() {
                        result = result.header(header::CONTENT_LENGTH, bytes.len());
                    }

                    let body = if method == Method::HEAD || status == StatusCode::NOT_MODIFIED {
                        Body::empty()
                    } else if condition == Some("\"stream\"") {
                        Body::from_stream(futures_util::stream::once(async move {
                            released.cancelled().await;

                            Ok::<_, io::Error>(bytes)
                        }))
                    } else if status.is_success() {
                        Body::from(bytes)
                    } else {
                        Body::from(KEY)
                    };

                    result.body(body).unwrap()
                }
            });

            let task = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });

            Self {
                media,
                requests,
                release,
                task,
            }
        }
    }

    impl Drop for Upstream {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn ab_only_changes_capability_and_preserves_real_proxy_policy() {
        let upstream = Upstream::start().await;

        for (method, range, condition, status) in [
            (Method::GET, None, None, StatusCode::OK),
            (Method::HEAD, None, None, StatusCode::OK),
            (
                Method::GET,
                Some("bytes=2-5"),
                None,
                StatusCode::PARTIAL_CONTENT,
            ),
            (
                Method::GET,
                Some("bytes=2-"),
                None,
                StatusCode::PARTIAL_CONTENT,
            ),
            (Method::GET, Some("bytes=0-1,4-5"), None, StatusCode::OK),
            (Method::HEAD, Some("bytes=2-5"), None, StatusCode::OK),
            (
                Method::GET,
                Some("bytes=999-"),
                None,
                StatusCode::RANGE_NOT_SATISFIABLE,
            ),
            (
                Method::GET,
                None,
                Some("\"cached\""),
                StatusCode::NOT_MODIFIED,
            ),
            (
                Method::HEAD,
                None,
                Some("\"cached\""),
                StatusCode::NOT_MODIFIED,
            ),
            (
                Method::GET,
                None,
                Some("\"missing\""),
                StatusCode::NOT_FOUND,
            ),
            (
                Method::GET,
                None,
                Some("\"denied\""),
                StatusCode::BAD_GATEWAY,
            ),
            (
                Method::GET,
                None,
                Some("\"broken\""),
                StatusCode::BAD_GATEWAY,
            ),
        ] {
            let mut results = Vec::new();

            for path in ["/control", "/byte-seek", "/both", "/didl-only"] {
                let mut request = Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .header(header::HOST, "untrusted.invalid")
                    .header(header::AUTHORIZATION, "client-secret")
                    .header(header::COOKIE, "client-secret")
                    .header("x-api-key", "client-secret")
                    .header("x-client-secret", "client-secret")
                    .header("getcontentfeatures.dlna.org", "1");

                if let Some(range) = range {
                    request = request
                        .header(header::RANGE, range)
                        .header(header::IF_RANGE, "\"original\"");
                }

                if let Some(condition) = condition {
                    request = request.header(header::IF_NONE_MATCH, condition);
                }

                let result = response(&upstream.media, request.body(Body::empty()).unwrap()).await;

                assert_eq!(
                    result.status(),
                    status,
                    "{method} {path} {range:?} {condition:?}"
                );

                let (mut parts, body) = result.into_parts();

                assert_eq!(
                    parts.headers.remove(CAPABILITY),
                    (matches!(path, "/byte-seek" | "/both") && status.is_success())
                        .then(|| HeaderValue::from_static("DLNA.ORG_OP=01"))
                );

                assert_eq!(parts.headers[header::CONNECTION], "close");
                assert!(!parts.headers.contains_key("x-api-key"));
                assert!(!parts.headers.contains_key(header::SET_COOKIE));
                let body = axum::body::to_bytes(body, 1024).await.unwrap();

                let expected = if method == Method::HEAD || !status.is_success() {
                    &[][..]
                } else if status == StatusCode::PARTIAL_CONTENT {
                    &BYTES[2..6]
                } else {
                    BYTES
                };

                assert_eq!(&body[..], expected);
                results.push((parts.headers, body));
            }

            assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
            let requests = upstream.requests.lock().unwrap();
            let batch = &requests[requests.len() - 4..];
            assert!(batch.windows(2).all(|pair| pair[0] == pair[1]));
            assert_eq!(batch[0].0, method);

            let forwarded_range =
                range.filter(|range| method == Method::GET && !range.contains(','));

            assert_eq!(
                batch[0].1.get(header::RANGE).map(|v| v.to_str().unwrap()),
                forwarded_range
            );

            assert_eq!(
                batch[0].1.contains_key(header::IF_RANGE),
                forwarded_range.is_some()
            );

            assert_eq!(
                batch[0]
                    .1
                    .get(header::IF_NONE_MATCH)
                    .map(|v| v.to_str().unwrap()),
                condition
            );
        }
    }

    #[tokio::test]
    async fn invalid_routes_methods_and_bodies_do_not_start_upstream_work() {
        let upstream = Upstream::start().await;

        for path in [
            "/",
            "/control?",
            "/byte-seek?x=1",
            "/control/",
            "/byte-seek/extra",
            "/both?x=1",
            "/didl-only/extra",
            "/api/server/version",
            "/%63ontrol",
        ] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            let result = response(&upstream.media, request).await;
            assert_eq!(result.status(), StatusCode::NOT_FOUND, "{path}");
            assert_eq!(result.headers()[header::CONNECTION], "close");
            assert!(!result.headers().contains_key(CAPABILITY));
        }

        for path in ["/control", "/byte-seek", "/both", "/didl-only"] {
            for method in [Method::POST, Method::PUT, Method::OPTIONS, Method::DELETE] {
                let request = Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap();

                let result = response(&upstream.media, request).await;
                assert_eq!(result.status(), StatusCode::METHOD_NOT_ALLOWED);
                assert_eq!(result.headers()[header::ALLOW], "GET, HEAD");
                assert!(!result.headers().contains_key(CAPABILITY));
            }

            for (name, value) in [
                (header::CONTENT_LENGTH, "1"),
                (header::CONTENT_LENGTH, "invalid"),
                (header::TRANSFER_ENCODING, "chunked"),
                (header::UPGRADE, "websocket"),
            ] {
                // A declared body is rejected without waiting for any bytes.
                let body = Body::from_stream(futures_util::stream::pending::<
                    Result<bytes::Bytes, io::Error>,
                >());

                let request = Request::builder()
                    .uri(path)
                    .header(name, value)
                    .body(body)
                    .unwrap();

                let result = timeout(Duration::from_secs(1), response(&upstream.media, request))
                    .await
                    .unwrap();

                assert_eq!(result.status(), StatusCode::BAD_REQUEST);
                assert!(!result.headers().contains_key(CAPABILITY));
            }

            let request = Request::builder()
                .uri(path)
                .body(Body::from("undeclared"))
                .unwrap();

            assert_eq!(
                response(&upstream.media, request).await.status(),
                StatusCode::BAD_REQUEST
            );

            let request = Request::builder()
                .uri(path)
                .header(header::CONTENT_LENGTH, "0")
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap();

            assert_eq!(
                response(&upstream.media, request).await.status(),
                StatusCode::BAD_REQUEST
            );

            let request = Request::builder()
                .uri(path)
                .header("x-large", "a".repeat(limits::HEADER_BYTES))
                .body(Body::empty())
                .unwrap();

            assert_eq!(
                response(&upstream.media, request).await.status(),
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            );
        }

        assert!(upstream.requests.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn undeclared_pending_body_is_bounded_without_upstream_work() {
        let upstream = Upstream::start().await;

        let body = Body::from_stream(futures_util::stream::pending::<
            Result<bytes::Bytes, io::Error>,
        >());

        let request = Request::builder().uri("/byte-seek").body(body).unwrap();
        let started = Instant::now();

        assert_eq!(
            response(&upstream.media, request).await.status(),
            StatusCode::BAD_REQUEST
        );

        assert_eq!(started.elapsed(), limits::BODY_TIMEOUT);
        assert!(upstream.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn shared_proxy_permits_cover_both_cases_and_release_on_response_drop() {
        let upstream = Upstream::start().await;
        let mut held = Vec::new();

        for _ in 0..limits::MEDIA_OPERATIONS {
            let result = upstream
                .media
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(result.status(), StatusCode::OK);
            held.push(result);
        }

        for path in ["/control", "/byte-seek", "/both", "/didl-only"] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            let result = response(&upstream.media.clone(), request).await;
            assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(!result.headers().contains_key(CAPABILITY));
        }

        assert_eq!(
            upstream.requests.lock().unwrap().len(),
            limits::MEDIA_OPERATIONS
        );

        held.pop();

        let request = Request::builder()
            .uri("/byte-seek")
            .body(Body::empty())
            .unwrap();

        let result = response(&upstream.media, request).await;
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.headers()[CAPABILITY], "DLNA.ORG_OP=01");
    }

    #[tokio::test]
    async fn direct_head_retains_admission_until_response_disposal() {
        let upstream = Upstream::start().await;
        let mut held = Vec::new();

        for _ in 0..limits::MEDIA_OPERATIONS {
            let request = Request::builder()
                .method(Method::HEAD)
                .uri("/byte-seek")
                .body(Body::empty())
                .unwrap();

            let result = response(&upstream.media, request).await;
            assert_eq!(result.status(), StatusCode::OK);

            assert!(
                result
                    .extensions()
                    .get::<Arc<OwnedSemaphorePermit>>()
                    .is_some()
            );

            held.push(result);
        }

        let rejected = upstream
            .media
            .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
            .await;

        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        held.pop();

        let admitted = upstream
            .media
            .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
            .await;

        assert_eq!(admitted.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn buffered_responses_hold_per_connection_admission_until_teardown() {
        for method in ["GET", "HEAD"] {
            for finish in ["read", "disconnect", "cancel"] {
                let upstream = Upstream::start().await;
                let mut connections = Vec::new();

                for _ in 0..limits::MEDIA_OPERATIONS {
                    let (mut client, io) = tokio::io::duplex(1);
                    let task = tokio::spawn(connection(io, upstream.media.clone()));

                    client
                        .write_all(
                            format!("{method} /byte-seek HTTP/1.1\r\nHost: fixture\r\n\r\n")
                                .as_bytes(),
                        )
                        .await
                        .unwrap();

                    timeout(Duration::from_secs(2), async {
                        if method == "GET" {
                            let mut headers = Vec::new();

                            while !headers.ends_with(b"\r\n\r\n") {
                                headers.push(client.read_u8().await.unwrap());
                                assert!(headers.len() < 2048);
                            }

                            assert!(headers.starts_with(b"HTTP/1.1 200 OK\r\n"));
                            assert_eq!(client.read_u8().await.unwrap(), BYTES[0]);
                        } else {
                            assert_eq!(client.read_u8().await.unwrap(), b'H');
                        }
                    })
                    .await
                    .unwrap();

                    assert!(!task.is_finished());
                    connections.push((client, task));
                }

                let rejected = upstream
                    .media
                    .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
                    .await;

                assert_eq!(
                    rejected.status(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{method} {finish}"
                );

                for (mut client, task) in connections {
                    match finish {
                        "read" => {
                            let mut rest = Vec::new();

                            timeout(Duration::from_secs(2), client.read_to_end(&mut rest))
                                .await
                                .unwrap()
                                .unwrap();

                            if method == "GET" {
                                assert_eq!(rest, &BYTES[1..]);
                            }
                        }

                        "disconnect" => drop(client),

                        "cancel" => task.abort(),

                        _ => unreachable!(),
                    }

                    assert_eq!(
                        timeout(Duration::from_secs(2), task)
                            .await
                            .unwrap()
                            .is_err(),
                        finish == "cancel"
                    );
                }

                // All slots, not only the most recent connection's slot, must return.
                let mut held = Vec::new();

                for _ in 0..limits::MEDIA_OPERATIONS {
                    let result = upstream
                        .media
                        .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
                        .await;

                    assert_eq!(result.status(), StatusCode::OK, "{method} {finish}");
                    held.push(result);
                }
            }
        }
    }

    async fn wire(address: SocketAddr, request: &str) -> String {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut result = String::new();

        timeout(Duration::from_secs(2), socket.read_to_string(&mut result))
            .await
            .expect("one response must close the connection")
            .unwrap();

        result
    }

    #[tokio::test]
    async fn tcp_reset_cancels_upstream_waiting_and_returns_all_admission() {
        for condition in ["headers", "stream"] {
            let upstream = Upstream::start().await;
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let stop = CancellationToken::new();
            let task = tokio::spawn(run(listener, upstream.media.clone(), stop.clone()));
            let mut clients = Vec::new();

            for count in 1..=limits::MEDIA_OPERATIONS {
                let mut client = TcpStream::connect(address).await.unwrap();

                client
                    .write_all(format!("GET /byte-seek HTTP/1.1\r\nHost: fixture\r\nIf-None-Match: \"{condition}\"\r\n\r\n").as_bytes())
                    .await
                    .unwrap();

                timeout(Duration::from_secs(2), async {
                    while upstream.requests.lock().unwrap().len() < count {
                        tokio::task::yield_now().await;
                    }

                    if condition == "stream" {
                        let mut headers = Vec::new();

                        while !headers.ends_with(b"\r\n\r\n") {
                            headers.push(client.read_u8().await.unwrap());
                            assert!(headers.len() < 2048);
                        }

                        assert!(headers.starts_with(b"HTTP/1.1 200 OK\r\n"));
                    }
                })
                .await
                .unwrap();

                clients.push(client);
            }

            let rejected = upstream
                .media
                .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
                .await;

            assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);

            for client in clients {
                socket2::SockRef::from(&client)
                    .set_linger(Some(Duration::ZERO))
                    .unwrap();

                drop(client);
            }

            let mut held = Vec::new();

            timeout(Duration::from_secs(2), async {
                while held.len() < limits::MEDIA_OPERATIONS {
                    let result = upstream
                        .media
                        .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
                        .await;

                    if result.status() == StatusCode::SERVICE_UNAVAILABLE {
                        tokio::task::yield_now().await;
                        continue;
                    }

                    assert_eq!(result.status(), StatusCode::OK);
                    held.push(result);
                }
            })
            .await
            .expect("disconnect must release admission before upstream deadlines");

            stop.cancel();

            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn listener_closes_after_one_response_and_leaves_upstream_alive() {
        let upstream = Upstream::start().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(run(listener, upstream.media.clone(), stop.clone()));

        for (path, capability) in [
            ("/control", false),
            ("/byte-seek", true),
            ("/both", true),
            ("/didl-only", false),
        ] {
            for method in ["GET", "HEAD"] {
                let result = wire(address, &format!("{method} {path} HTTP/1.1\r\nHost: fixture\r\nConnection: keep-alive\r\n\r\nGET /byte-seek HTTP/1.1\r\nHost: fixture\r\n\r\n")).await;
                let (headers, body) = result.split_once("\r\n\r\n").unwrap();
                assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
                assert!(headers.contains("connection: close"));

                assert_eq!(
                    headers.contains("contentfeatures.dlna.org: DLNA.ORG_OP=01"),
                    capability
                );

                assert_eq!(
                    body.as_bytes(),
                    if method == "HEAD" { &[][..] } else { BYTES }
                );
            }
        }

        assert_eq!(upstream.requests.lock().unwrap().len(), 8);
        stop.cancel();

        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(TcpStream::connect(address).await.is_err());

        let result = upstream
            .media
            .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
            .await;

        assert_eq!(result.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admitted_stream_finishes_or_is_aborted_at_the_single_shutdown_deadline() {
        for finish in [true, false] {
            let upstream = Upstream::start().await;
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let stop = CancellationToken::new();
            let task = tokio::spawn(run(listener, upstream.media.clone(), stop.clone()));
            let mut socket = TcpStream::connect(address).await.unwrap();

            socket
                .write_all(b"GET /byte-seek HTTP/1.1\r\nHost: fixture\r\nIf-None-Match: \"stream\"\r\n\r\n")
                .await
                .unwrap();

            let mut socket = BufReader::new(socket);
            let mut headers = String::new();

            timeout(Duration::from_secs(2), async {
                while !headers.ends_with("\r\n\r\n") {
                    assert!(headers.len() < 2048);
                    assert_ne!(socket.read_line(&mut headers).await.unwrap(), 0);
                }
            })
            .await
            .unwrap();

            assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(!task.is_finished());

            if !finish {
                tokio::time::pause();
            }

            let started = Instant::now();
            stop.cancel();
            tokio::task::yield_now().await;
            assert!(!task.is_finished());

            if finish {
                upstream.release.cancel();
            }

            timeout(limits::SHUTDOWN_GRACE + Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            if !finish {
                // Tokio rounds absolute deadlines up to its millisecond timer tick.
                assert!(
                    (limits::SHUTDOWN_GRACE..=limits::SHUTDOWN_GRACE + Duration::from_millis(1))
                        .contains(&started.elapsed())
                );

                tokio::time::resume();
            }

            let mut body = Vec::new();

            timeout(Duration::from_secs(2), socket.read_to_end(&mut body))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(&body[..], if finish { BYTES } else { &[][..] });
            assert!(TcpStream::connect(address).await.is_err());
        }
    }
}
