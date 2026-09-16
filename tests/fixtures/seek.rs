//! Fixture-only comparison of otherwise identical original-video responses.

use axum::{Router, body::Body, extract::Request, response::Response};
use http::{HeaderValue, Method, StatusCode, header};
use immich_dlna_proxy::{media::MediaProxy, server::HEADER_BYTES};
use tokio::net::TcpListener;

const ASSET: &str = "20000000-0000-4000-8000-000000000003";
const CAPABILITY: &str = "contentfeatures.dlna.org";

/// All four cases use the caller's proxy, including its credentials and operation permits.
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

        if header_bytes > HEADER_BYTES {
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

        drop(body);

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

    result.headers_mut().insert(
        header::SERVER,
        HeaderValue::from_static(immich_dlna_proxy::server_header()),
    );

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

pub async fn run(listener: TcpListener, media: MediaProxy) -> anyhow::Result<()> {
    let router = Router::new().fallback(move |request| {
        let media = media.clone();

        async move { response(&media, request).await }
    });

    axum::serve(listener, router).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        net::Ipv4Addr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use http::HeaderMap;
    use tokio::{task::JoinHandle, time::timeout};

    const KEY: &str = "seek-test-upstream-secret";
    const BYTES: &[u8] = b"unchanged original video bytes";

    struct Upstream {
        media: MediaProxy,
        requests: Arc<Mutex<Vec<(Method, HeaderMap)>>>,
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

            let router = Router::new().fallback(move |request: Request| {
                let recorded = recorded.clone();

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

                    let range = headers
                        .get(header::RANGE)
                        .and_then(|value| value.to_str().ok());

                    let status = match condition {
                        Some("\"cached\"") => StatusCode::NOT_MODIFIED,
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

                    if status.is_success() {
                        result = result.header(header::CONTENT_LENGTH, bytes.len());
                    }

                    let body = if method == Method::HEAD || status == StatusCode::NOT_MODIFIED {
                        Body::empty()
                    } else {
                        Body::from(bytes)
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
                None,
                Some("\"cached\""),
                StatusCode::NOT_MODIFIED,
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

            let forwarded_range = range.filter(|_| method == Method::GET);

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
                .header("x-large", "a".repeat(HEADER_BYTES))
                .body(Body::empty())
                .unwrap();

            assert_eq!(
                response(&upstream.media, request).await.status(),
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            );
        }

        assert!(upstream.requests.lock().unwrap().is_empty());
    }
}
