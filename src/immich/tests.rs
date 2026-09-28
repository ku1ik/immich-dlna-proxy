use super::test_support::*;
use super::*;
use axum::{body::Body, http::Method, http::StatusCode, response::Response};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{net::TcpListener, time::Instant};

const ALBUM: Uuid = Uuid::from_u128(100_000);

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

    let client = fake.new_client();
    let albums = client.albums().await.unwrap();
    assert_eq!(albums[0].album_name, "A\u{0001}&B");

    assert_eq!(
        albums[0].created_at.as_deref(),
        Some("2024-01-01T00:30:00+02:00")
    );

    let result = client
        .search_album(ALBUM, NonZeroUsize::MIN, SearchMode::Members)
        .await
        .unwrap();

    assert_eq!(result.next_page, NonZeroUsize::new(2));
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

    let result = client
        .search_album(
            ALBUM,
            NonZeroUsize::new(2).unwrap(),
            SearchMode::EncodedVideos,
        )
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

    assert!(client.api_key.is_sensitive());
}

#[test]
fn critical_structure_is_required_but_additive_fields_are_ignored() {
    for dto in [
        json!({"major": -1, "minor": 1, "patch": 0, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0.5, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0, "prerelease": "rc"}),
        json!({"major": 3, "minor": 1}),
    ] {
        assert!(serde_json::from_value::<Version>(dto).is_err());
    }

    for dto in [
        json!({"id": ALBUM}),
        json!({"id": ALBUM, "albumName": "valid", "albumThumbnailAssetId": "invalid"}),
    ] {
        assert!(serde_json::from_value::<Album>(dto).is_err());
    }

    for field in ["id", "type", "visibility", "isTrashed", "isEdited"] {
        for null in [false, true] {
            let mut dto = asset(1, "IMAGE");

            if null {
                dto[field] = Value::Null;
            } else {
                dto.as_object_mut().unwrap().remove(field);
            }

            assert!(serde_json::from_value::<Asset>(dto).is_err(), "{field}");
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
        assert!(serde_json::from_value::<Asset>(dto).is_err(), "{field}");
    }

    for response in [
        json!({"assets": {"items": []}}),
        json!({"assets": {"items": [], "nextPage": 2}}),
        json!({"assets": {"nextPage": null}}),
        json!({"assets": []}),
        json!({"albums": []}),
    ] {
        assert!(serde_json::from_value::<SearchResponse>(response).is_err());
    }

    let dto = json!({
        "id": Uuid::from_u128(1),
        "type": "IMAGE",
        "visibility": "timeline",
        "isTrashed": false,
        "isEdited": false,
        "stack": {"unexpected": [1, 2, 3]},
    });

    let asset: Asset = serde_json::from_value(dto).unwrap();
    assert!(asset.file_created_at.is_none());
    assert!(asset.original_file_name.is_none());
}

#[tokio::test]
async fn structurally_invalid_responses_return_sanitized_errors() {
    let invalid = json!({"id": "private-test-key http://secret.example/private"});

    let fake = Fake::new(vec![
        reply(json!([invalid.clone()])),
        page(vec![invalid], None),
    ])
    .await;

    let client = fake.new_client();

    for error in [
        client.albums().await.unwrap_err(),
        client
            .search_album(ALBUM, NonZeroUsize::MIN, SearchMode::Members)
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            format!("{error:#}"),
            "invalid Immich JSON structure; requires Immich 3.1.0 or newer"
        );
    }

    assert_eq!(fake.requests.lock().unwrap().len(), 2);
}

#[test]
fn optional_dates_preserve_strings_and_ignore_other_json_values() {
    for (value, expected) in [
        (None, None),
        (Some(Value::Null), None),
        (Some(json!(123)), None),
        (Some(json!(false)), None),
        (Some(json!({"date": []})), None),
        (Some(json!([])), None),
        (
            Some(json!("2024-01-01T00:30:00+02:00")),
            Some("2024-01-01T00:30:00+02:00"),
        ),
        (Some(json!("not a date")), Some("not a date")),
    ] {
        let mut raw_album = album(ALBUM);
        let mut raw_asset = asset(1, "IMAGE");

        for (dto, fields) in [
            (&mut raw_album, ["createdAt", "endDate"]),
            (&mut raw_asset, ["fileCreatedAt", "localDateTime"]),
        ] {
            for field in fields {
                if let Some(value) = &value {
                    dto[field] = value.clone();
                } else {
                    dto.as_object_mut().unwrap().remove(field);
                }
            }
        }

        let album: Album = serde_json::from_value(raw_album).unwrap();
        let asset: Asset = serde_json::from_value(raw_asset).unwrap();

        for (field, date) in [
            ("createdAt", album.created_at),
            ("endDate", album.end_date),
            ("fileCreatedAt", asset.file_created_at),
            ("localDateTime", asset.local_date_time),
        ] {
            assert_eq!(date.as_deref(), expected, "{field}: {value:?}");
        }
    }
}

#[tokio::test]
async fn search_validates_page_numbers_and_continuation_tokens() {
    for next in ["", "1", "3", "02", "+2", "2 ", "18446744073709551616"] {
        let fake = Fake::new(vec![page(vec![], Some(next))]).await;
        let client = fake.new_client();

        assert!(
            client
                .search_album(ALBUM, NonZeroUsize::MIN, SearchMode::Members)
                .await
                .is_err(),
            "{next}"
        );

        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    let fake = Fake::new(vec![page(vec![], Some("0"))]).await;
    let client = fake.new_client();

    assert!(
        client
            .search_album(ALBUM, NonZeroUsize::MAX, SearchMode::Members)
            .await
            .is_err()
    );

    assert_eq!(fake.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn versions_retry_only_failed_checks_and_cache_success() {
    for value in [
        json!({"major": 3, "minor": 0, "patch": 99, "prerelease": null}),
        json!({"major": 3, "minor": 1, "patch": 0, "prerelease": 1}),
        json!({"major": 3, "minor": 1}),
    ] {
        let fake = Fake::new(vec![reply(value), version()]).await;
        let client = fake.new_client();

        assert!(client.ensure_supported_version().await.is_err());

        assert_eq!(fake.requests.lock().unwrap().len(), 1);

        client.ensure_supported_version().await.unwrap();

        client.ensure_supported_version().await.unwrap();

        assert_eq!(fake.requests.lock().unwrap().len(), 2);

        assert_eq!(
            fake.requests.lock().unwrap()[0].uri,
            "/prefix/api/server/version"
        );
    }

    let fake = Fake::new(vec![
        reply(json!({"major": 4, "minor": 0, "patch": 0, "prerelease": null, "extra": []})),
        Response::builder().status(403).body(Body::empty()).unwrap(),
        reply(json!([])),
    ])
    .await;

    let client = fake.new_client();
    client.ensure_supported_version().await.unwrap();

    assert!(client.albums().await.is_err());

    client.ensure_supported_version().await.unwrap();

    client.albums().await.unwrap();
    assert_eq!(fake.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn redirects_are_rejected_at_every_endpoint() {
    let target = Fake::new(vec![reply(json!([]))]).await;

    for endpoint in 0..3 {
        let fake = Fake::new(vec![
            Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, target.api_base.as_url().as_str())
                .body(Body::empty())
                .unwrap(),
        ])
        .await;

        let client = fake.new_client();

        let result = match endpoint {
            0 => client.ensure_supported_version().await,

            1 => client.albums().await.map(|_| ()),

            _ => client
                .search_album(ALBUM, NonZeroUsize::MIN, SearchMode::Members)
                .await
                .map(|_| ()),
        };

        assert!(result.is_err());
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
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
        let client = fake.new_client();
        let error = client.albums().await.unwrap_err().to_string();

        assert!(
            error.contains("JSON response exceeds byte limit"),
            "{error}"
        );

        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn transport_failures_are_sanitized_and_do_not_retry() {
    for status in [401, 403, 404, 429, 500, 503] {
        let fake = Fake::new(vec![
            Response::builder()
                .status(status)
                .body(Body::from(
                    "private-test-key http://secret.example/private upstream body",
                ))
                .unwrap(),
        ])
        .await;

        let client = fake.new_client();
        let error = format!("{:#}", client.albums().await.unwrap_err());
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

    let client = fake.new_client();
    let error = format!("{:#}", client.albums().await.unwrap_err());
    assert!(!error.contains("private") && !error.contains("http://"));
    client.albums().await.unwrap();
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
