use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request},
    response::Response,
};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle, time::Instant};

const ALBUM: Uuid = Uuid::from_u128(100_000);

pub(crate) struct Received {
    pub method: Method,
    pub uri: String,
    pub headers: HeaderMap,
    pub body: Value,
}

pub(crate) struct Fake {
    pub client: Client,
    pub requests: Arc<Mutex<Vec<Received>>>,
    task: JoinHandle<()>,
}

impl Fake {
    pub async fn new(replies: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));

        let router =
            Router::new().fallback(move |request: Request<Body>| {
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

        let client = Client::new(
            Url::parse(&format!("http://{address}/prefix/api/")).unwrap(),
            HeaderValue::from_static("private-test-key"),
        )
        .unwrap();

        Self {
            client,
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

pub(crate) fn reply(value: Value) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}

pub(crate) fn version() -> Response {
    reply(json!({"major": 3, "minor": 1, "patch": 0, "prerelease": null}))
}

pub(crate) fn album(id: Uuid) -> Value {
    json!({"id": id, "albumName": "Album", "createdAt": "2024-01-01T00:30:00+02:00", "albumThumbnailAssetId": Uuid::from_u128(777)})
}

pub(crate) fn asset(id: u128, kind: &str) -> Value {
    json!({
        "id": Uuid::from_u128(id), "type": kind, "visibility": "timeline",
        "isTrashed": false, "isEdited": false, "originalFileName": "photo.jpg",
        "originalMimeType": "image/jpeg", "duration": null,
        "fileCreatedAt": "2024-01-01T00:30:00+02:00",
        "localDateTime": "2024-01-01T00:30:00Z"
    })
}

pub(crate) fn page(items: Vec<Value>, next: Option<&str>) -> Response {
    reply(json!({"assets": {"items": items, "nextPage": next, "total": 0, "count": 0}}))
}

#[tokio::test]
async fn albums_and_search_return_upstream_metadata_one_page_at_a_time() {
    let mut raw_album = album(ALBUM);
    raw_album["albumName"] = json!("A\u{0001}&B");
    let mut raw_asset = asset(1, "FUTURE_TYPE");
    raw_asset["visibility"] = json!("future-visibility");
    raw_asset["isTrashed"] = json!(true);
    raw_asset["originalMimeType"] = json!("IMAGE/JPEG; quality=90");
    raw_asset["originalFileName"] = Value::Null;

    let fake = Fake::new(vec![
        reply(json!([raw_album])),
        page(vec![raw_asset.clone(), raw_asset], Some("2")),
        page(vec![asset(2, "VIDEO")], None),
    ])
    .await;

    let albums = fake.client.albums().await.unwrap();
    assert_eq!(albums[0].album_name, "A\u{0001}&B");

    assert_eq!(
        albums[0].created_at.as_deref(),
        Some("2024-01-01T00:30:00+02:00")
    );

    let result = fake
        .client
        .search_album(ALBUM, 1, AssetFilter::All)
        .await
        .unwrap();

    assert_eq!(result.next_page, Some(2));
    assert_eq!(result.items.len(), 2);
    assert_eq!(result.items[0].kind, "FUTURE_TYPE");
    assert_eq!(result.items[0].visibility, "future-visibility");
    assert!(result.items[0].is_trashed);
    assert!(result.items[0].original_file_name.is_none());

    assert_eq!(
        result.items[0].original_mime_type.as_deref(),
        Some("IMAGE/JPEG; quality=90")
    );

    assert_eq!(fake.requests.lock().unwrap().len(), 2);

    let result = fake
        .client
        .search_album(ALBUM, 2, AssetFilter::EncodedVideos)
        .await
        .unwrap();

    assert_eq!(result.next_page, None);
    assert_eq!(result.items[0].id, Uuid::from_u128(2));
    let requests = fake.requests.lock().unwrap();
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(requests[0].uri, "/prefix/api/albums");

    for (index, encoded) in [(1, false), (2, true)] {
        let request = &requests[index];
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri, "/prefix/api/search/metadata");

        let mut expected = json!({
            "albumIds": [ALBUM], "page": index, "size": SEARCH_PAGE_SIZE,
            "withDeleted": false, "withExif": false, "withPeople": false,
        });

        if encoded {
            expected["type"] = json!("VIDEO");
            expected["isEncoded"] = json!(true);
        }

        assert_eq!(request.body, expected);
    }

    for request in requests.iter() {
        assert_eq!(request.headers["x-api-key"], "private-test-key");
        assert_eq!(request.headers[header::ACCEPT], "application/json");
        assert_eq!(request.headers[header::ACCEPT_ENCODING], "identity");
    }

    assert!(fake.client.api_key.is_sensitive());
}

#[tokio::test]
async fn critical_structure_is_required_but_additive_fields_are_ignored() {
    for field in ["id", "type", "visibility", "isTrashed", "isEdited"] {
        for null in [false, true] {
            let mut dto = asset(1, "IMAGE");

            if null {
                dto[field] = Value::Null;
            } else {
                dto.as_object_mut().unwrap().remove(field);
            }

            let fake = Fake::new(vec![page(vec![dto], None)]).await;

            assert!(
                fake.client
                    .search_album(ALBUM, 1, AssetFilter::All)
                    .await
                    .is_err(),
                "{field}"
            );
        }
    }

    for field in [
        "checksum",
        "updatedAt",
        "thumbhash",
        "originalFileName",
        "originalMimeType",
        "duration",
    ] {
        let mut dto = asset(1, "IMAGE");
        dto[field] = json!({"wrong": "structure"});
        let fake = Fake::new(vec![page(vec![dto], None)]).await;

        assert!(
            fake.client
                .search_album(ALBUM, 1, AssetFilter::All)
                .await
                .is_err(),
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

        assert!(
            fake.client
                .search_album(ALBUM, 1, AssetFilter::All)
                .await
                .is_err()
        );
    }

    let mut dto = asset(1, "IMAGE");

    for field in [
        "checksum",
        "updatedAt",
        "thumbhash",
        "originalFileName",
        "originalMimeType",
        "fileCreatedAt",
        "localDateTime",
        "duration",
    ] {
        dto.as_object_mut().unwrap().remove(field);
    }

    dto["stack"] = json!({"unexpected": [1, 2, 3]});
    let fake = Fake::new(vec![page(vec![dto], None)]).await;

    let result = fake
        .client
        .search_album(ALBUM, 1, AssetFilter::All)
        .await
        .unwrap();

    assert_eq!(result.items.len(), 1);
    assert!(result.items[0].file_created_at.is_none());
    assert!(result.items[0].original_file_name.is_none());
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn search_validates_page_numbers_and_continuation_tokens() {
    for next in ["", "1", "3", "02", "+2", "2 ", "18446744073709551616"] {
        let fake = Fake::new(vec![page(vec![], Some(next))]).await;

        assert!(
            fake.client
                .search_album(ALBUM, 1, AssetFilter::All)
                .await
                .is_err(),
            "{next}"
        );

        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    let fake = Fake::new(vec![page(vec![], Some("0"))]).await;

    assert!(
        fake.client
            .search_album(ALBUM, 0, AssetFilter::All)
            .await
            .is_err()
    );

    assert!(fake.requests.lock().unwrap().is_empty());

    assert!(
        fake.client
            .search_album(ALBUM, usize::MAX, AssetFilter::All)
            .await
            .is_err()
    );

    assert_eq!(fake.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn versions_retry_only_failed_checks_and_share_success_across_clones() {
    for value in [
        json!({"major": 3, "minor": 0, "patch": 99, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0, "prerelease": 1}),
        json!({"major": -1, "minor": 1, "patch": 0, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0.5, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0, "prerelease": "rc"}),
        json!({"major": 3, "minor": 1}),
    ] {
        let fake = Fake::new(vec![reply(value), version()]).await;

        assert!(fake.client.ensure_supported_version().await.is_err());

        assert_eq!(fake.requests.lock().unwrap().len(), 1);

        fake.client.ensure_supported_version().await.unwrap();

        fake.client
            .clone()
            .ensure_supported_version()
            .await
            .unwrap();

        assert_eq!(fake.requests.lock().unwrap().len(), 2);

        assert_eq!(
            fake.requests.lock().unwrap()[0].uri,
            "/prefix/api/server/version"
        );
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

    fake.client.ensure_supported_version().await.unwrap();

    let error = format!("{:#}", fake.client.albums().await.unwrap_err());
    assert!(error.contains("403"));
    assert!(!error.contains("private") && !error.contains("http://"));

    fake.client.ensure_supported_version().await.unwrap();

    fake.client.albums().await.unwrap();
    assert_eq!(fake.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn redirects_are_rejected_at_every_endpoint() {
    let target = Fake::new(vec![reply(json!([]))]).await;

    for endpoint in 0..3 {
        for status in [301, 302, 303, 307, 308] {
            for location in [target.client.api_base.as_str(), "/prefix/api/albums"] {
                let fake = Fake::new(vec![
                    Response::builder()
                        .status(status)
                        .header(header::LOCATION, location)
                        .body(Body::empty())
                        .unwrap(),
                ])
                .await;

                let result = match endpoint {
                    0 => fake.client.ensure_supported_version().await,

                    1 => fake.client.albums().await.map(|_| ()),

                    _ => fake
                        .client
                        .search_album(ALBUM, 1, AssetFilter::All)
                        .await
                        .map(|_| ()),
                };

                assert!(result.is_err());
                assert_eq!(fake.requests.lock().unwrap().len(), 1);
            }
        }
    }

    assert!(target.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn json_bound_is_enforced_before_parsing_with_and_without_content_length() {
    for chunked in [false, true] {
        let body = if chunked {
            Body::from_stream(futures_util::stream::iter((0..=JSON_BYTES / 1024).map(
                |_| Ok::<_, std::io::Error>(bytes::Bytes::from_static(&[b' '; 1024])),
            )))
        } else {
            Body::from(vec![b' '; JSON_BYTES + 1])
        };

        let fake = Fake::new(vec![Response::new(body)]).await;
        let error = fake.client.albums().await.unwrap_err().to_string();

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

        let error = format!("{:#}", fake.client.albums().await.unwrap_err());
        assert!(error.contains(&status.to_string()));
        assert!(!error.contains("private") && !error.contains("http://"));
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    let fake = Fake::new(vec![
        Response::new(Body::from(
            "{\"private-test-key\":\"http://secret.example/\",",
        )),
        reply(json!([])),
    ])
    .await;

    let error = format!("{:#}", fake.client.albums().await.unwrap_err());
    assert!(!error.contains("private") && !error.contains("http://"));
    fake.client.albums().await.unwrap();
    assert_eq!(fake.requests.lock().unwrap().len(), 2);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = client_for(&listener);
    drop(listener);
    let error = format!("{:#}", client.albums().await.unwrap_err());
    assert_eq!(error, "Immich connection failed");
}

fn client_for(listener: &TcpListener) -> Client {
    Client::new(
        format!("http://{}/api/", listener.local_addr().unwrap())
            .parse()
            .unwrap(),
        HeaderValue::from_static("private-test-key"),
    )
    .unwrap()
}

#[tokio::test]
async fn stalled_headers_time_out_and_close_the_connection() {
    use tokio::io::AsyncReadExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = client_for(&listener);

    let fetch = client.albums();
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
    tokio::time::advance(RESPONSE_HEADER_TIMEOUT - Duration::from_secs(1)).await;
    assert!(futures_util::poll!(&mut fetch).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    let now = Instant::now();
    let error = fetch.await.unwrap_err();
    assert_eq!(Instant::now(), now);
    assert_eq!(
        error.to_string(),
        "Immich response-header deadline exceeded"
    );
    assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
}
