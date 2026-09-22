use super::*;
use axum::response::Response;
use http::Method;
use serde_json::{Value, json};

use crate::immich::{
    self,
    test_support::{Fake as Api, album, asset, page, reply, version},
};

const ALBUM: Uuid = Uuid::from_u128(100_000);

struct SnapshotFixture {
    source: Source,
    api: Api,
}

impl SnapshotFixture {
    async fn new(replies: Vec<Response>) -> Self {
        let api = Api::new(replies).await;

        let source = Source::new(
            api.new_client(),
            "192.0.2.1:8200".parse().unwrap(),
            "Photos".into(),
        );

        Self { source, api }
    }
}

fn project(value: Value) -> Item {
    project_item(
        "192.0.2.1:8200".parse().unwrap(),
        ALBUM,
        serde_json::from_value(value).unwrap(),
        &mut 0,
    )
    .unwrap()
    .unwrap()
}

fn root(name: &str, albums: Vec<Value>) -> Result<Root> {
    project_root(
        "192.0.2.1:8200".parse().unwrap(),
        name,
        albums
            .into_iter()
            .map(|album| serde_json::from_value(album).unwrap())
            .collect(),
        &mut 0,
    )
}

#[test]
fn resources_dates_and_optional_hints() {
    for (mime, edited, representation, expected) in [
        (
            Some("IMAGE/JPEG; quality=90"),
            false,
            "original",
            "image/jpeg",
        ),
        (Some("image/png"), false, "original", "image/png"),
        (Some("image/jpeg; q =90"), false, "original", "image/jpeg"),
        (Some("image/gif"), false, "original", "image/gif"),
        (Some("image/jpeg"), true, "display", "image/jpeg"),
        (Some("image/heic"), false, "display", "image/jpeg"),
        (Some("image/webp"), false, "display", "image/jpeg"),
        (Some("image/jpeg;broken"), false, "display", "image/jpeg"),
        (None, false, "display", "image/jpeg"),
    ] {
        let mut dto = asset(1, "IMAGE");
        dto["originalMimeType"] = json!(mime);
        dto["isEdited"] = json!(edited);
        dto["originalFileName"] = json!("A\u{0001}&B");
        let item = project(dto);
        assert_eq!(item.object.title, "A\u{fffd}&B");
        assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));

        assert_eq!(
            item.capture.unwrap().to_rfc3339(),
            "2023-12-31T22:30:00+00:00"
        );

        assert_eq!(item.object.resources.len(), 2);
        assert!(item.object.resources[0].uri.ends_with(representation));
        assert_eq!(item.object.resources[0].mime, expected);
        assert!(item.object.resources[1].uri.ends_with("preview"));

        assert!(
            item.object
                .resources
                .iter()
                .all(|r| !r.byte_seek && r.duration.is_none())
        );
    }

    for (duration, expected) in [
        (0, Some("0:00:00.000")),
        (3_661_007, Some("1:01:01.007")),
        (i32::MAX as i64, Some("596:31:23.647")),
        (i32::MAX as i64 + 1, Some("596:31:23.648")),
        (-1, None),
    ] {
        let mut dto = asset(1, "VIDEO");
        dto["originalMimeType"] = Value::Null;
        dto["duration"] = json!(duration);
        dto["localDateTime"] = json!("2024-01-01T23:00:00-12:00");
        let item = project(dto);
        assert_eq!(item.object.date.as_deref(), Some("2024-01-01"));
        assert_eq!(item.object.resources[0].duration.as_deref(), expected);
        assert_eq!(item.object.resources[0].mime, "application/octet-stream");
        assert!(item.object.resources[0].byte_seek);
        assert!(item.object.art.unwrap().ends_with("preview"));
    }

    let mut dto = asset(1, "IMAGE");
    dto["originalFileName"] = json!("");
    dto["fileCreatedAt"] = json!("2024-01-01T12:00:00");
    dto["localDateTime"] = json!("not a date");
    dto["checksum"] = json!("one");
    dto["updatedAt"] = json!("opaque hint");
    dto["thumbhash"] = json!("two");
    dto["exifInfo"] = json!(["ignored unsupported structure"]);
    let item = project(dto);
    assert_eq!(item.object.title, Uuid::from_u128(1).to_string());
    assert!(item.capture.is_none() && item.object.date.is_none());
    assert_eq!(item.checksum.as_deref(), Some("one"));
    assert_eq!(item.updated_at.as_deref(), Some("opaque hint"));
    assert_eq!(item.thumbhash.as_deref(), Some("two"));
}

#[tokio::test]
async fn complete_pagination_and_scoped_encoded_intersection_without_probes() {
    let mut archived = asset(1001, "VIDEO");
    archived["visibility"] = json!("archive");
    archived["originalMimeType"] = json!("Video/QuickTime");
    let mut changed_encoded = archived.clone();
    changed_encoded["originalFileName"] = json!("changed between searches");

    let fake = SnapshotFixture::new(vec![
        version(),
        reply(json!([album(ALBUM)])),
        page((1..=1000).map(|id| asset(id, "IMAGE")).collect(), Some("2")),
        page(vec![archived], None),
        page(
            vec![
                changed_encoded.clone(),
                changed_encoded,
                asset(9999, "VIDEO"),
            ],
            None,
        ),
        reply(json!([])),
    ])
    .await;

    let root = fake.source.root().await.unwrap();

    assert_eq!(
        root.albums[&ALBUM].object.date.as_deref(),
        Some("2023-12-31")
    );

    assert!(
        root.albums[&ALBUM]
            .object
            .art
            .as_ref()
            .unwrap()
            .contains(&Uuid::from_u128(777).to_string())
    );

    assert!(root.albums[&ALBUM].object.child_count.is_none());
    let contents = fake.source.contents(ALBUM).await.unwrap();
    assert_eq!(contents.items.len(), 1001);
    let video = &contents.items[&Uuid::from_u128(1001)].object;
    assert_eq!(video.resources.len(), 2);
    assert_eq!(video.resources[0].mime, "video/quicktime");
    assert_eq!(video.resources[1].mime, "video/mp4");
    assert!(video.resources.iter().all(|r| r.byte_seek));
    assert!(video.resources[1].duration.is_none());

    assert!(fake.source.root().await.unwrap().albums.is_empty());

    let requests = fake.api.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[0].uri, "/prefix/api/server/version");
    assert_eq!(requests[1].uri, "/prefix/api/albums");
    assert_eq!(requests[5].uri, "/prefix/api/albums");

    for (index, expected_page) in [(2, 1), (3, 2), (4, 1)] {
        let request = &requests[index];
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri, "/prefix/api/search/metadata");
        assert_eq!(request.body["albumIds"], json!([ALBUM]));
        assert_eq!(request.body["page"], json!(expected_page));
        assert_eq!(request.body["size"], json!(immich::SEARCH_PAGE_SIZE));
        assert_eq!(request.body["withDeleted"], false);
        assert_eq!(request.body["withExif"], false);
        assert_eq!(request.body["withPeople"], false);
        assert!(request.body.get("withStacked").is_none());

        assert_eq!(
            request.body.get("isEncoded"),
            (index == 4).then_some(&Value::Bool(true))
        );

        assert_eq!(
            request.body.get("type"),
            (index == 4).then_some(&json!("VIDEO"))
        );
    }

    assert!(
        requests
            .iter()
            .all(|request| request.headers["x-api-key"] == "private-test-key")
    );
}

#[test]
fn eligible_assets_require_original_file_name() {
    for null in [false, true] {
        let mut dto = asset(1, "IMAGE");

        if null {
            dto["originalFileName"] = Value::Null;
        } else {
            dto.as_object_mut().unwrap().remove("originalFileName");
        }

        let result = project_item(
            "192.0.2.1:8200".parse().unwrap(),
            ALBUM,
            serde_json::from_value(dto).unwrap(),
            &mut 0,
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "eligible Immich asset is missing originalFileName"
        );
    }
}

#[tokio::test]
async fn duplicates_compare_only_normalized_member_data_and_eligibility() {
    let first = asset(1, "IMAGE");

    for field in [
        "originalFileName",
        "checksum",
        "updatedAt",
        "thumbhash",
        "localDateTime",
        "fileCreatedAt",
        "isEdited",
        "visibility",
        "isTrashed",
        "type",
    ] {
        let mut second = first.clone();

        second[field] = match field {
            "isEdited" | "isTrashed" => json!(true),
            "visibility" => json!("hidden"),
            "type" => json!("VIDEO"),
            "localDateTime" | "fileCreatedAt" => json!("2025-01-01T00:00:00Z"),
            _ => json!("changed"),
        };

        for reversed in [false, true] {
            let records = if reversed {
                vec![second.clone(), first.clone()]
            } else {
                vec![first.clone(), second.clone()]
            };

            let fake = SnapshotFixture::new(vec![page(records, None)]).await;

            assert!(fake.source.contents(ALBUM).await.is_err(), "{field}");
        }
    }

    let mut irrelevant = first.clone();
    irrelevant["originalPath"] = json!("private ignored path");
    irrelevant["visibility"] = json!("archive");
    let mut excluded = asset(2, "VIDEO");
    excluded["visibility"] = json!("future-visibility");
    excluded.as_object_mut().unwrap().remove("originalFileName");
    let mut excluded_changed = excluded.clone();
    excluded_changed["originalFileName"] = json!("irrelevant name".repeat(1000));
    excluded_changed["checksum"] = json!("irrelevant hint".repeat(1000));
    excluded_changed["isEdited"] = json!(true);

    let fake = SnapshotFixture::new(vec![page(
        vec![first, irrelevant, excluded, excluded_changed],
        None,
    )])
    .await;

    let result = fake.source.contents(ALBUM).await.unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(fake.api.requests.lock().unwrap().len(), 1);
}

#[test]
fn root_duplicates_titles_covers_and_canonical_digests() {
    let mut empty = album(ALBUM);
    empty["albumName"] = json!("");
    empty["albumThumbnailAssetId"] = Value::Null;
    let other = album(Uuid::from_u128(2));

    let first = root("Photos", vec![empty.clone(), other.clone(), empty.clone()]).unwrap();
    let second = root("Photos", vec![other, empty.clone()]).unwrap();
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.albums[&ALBUM].object.title, ALBUM.to_string());
    assert!(first.albums[&ALBUM].object.art.is_none());

    assert!(
        root(
            "Photos",
            vec![empty.clone(), json!({"id": ALBUM, "albumName": "changed"})]
        )
        .is_err()
    );

    let baseline = root("Photos", vec![empty.clone()]).unwrap();
    let changed = root("Another title", vec![empty]).unwrap();
    assert_ne!(baseline.digest, changed.digest);

    assert_eq!(
        baseline.albums[&ALBUM].digest,
        changed.albums[&ALBUM].digest
    );
}

#[test]
fn album_latest_dates_are_optional_and_independent_of_advertised_dates() {
    for (value, expected) in [
        (None, None),
        (Some(Value::Null), None),
        (Some(json!("2025-99-01T00:00:00Z")), None),
        (Some(json!(123)), None),
        (
            Some(json!("2025-01-01T01:30:00+02:00")),
            Some("2024-12-31T23:30:00Z"),
        ),
    ] {
        let mut dto = album(ALBUM);

        if let Some(value) = value {
            dto["endDate"] = value;
        }

        let root = root("Photos", vec![dto]).unwrap();
        let album = &root.albums[&ALBUM];
        assert_eq!(album.end_date, expected.map(|date| date.parse().unwrap()));
        assert_eq!(album.object.date.as_deref(), Some("2023-12-31"));

        assert_eq!(
            album.created_at,
            Some("2023-12-31T22:30:00Z".parse().unwrap())
        );
    }
}

#[tokio::test]
async fn contents_hashes_canonical_order_and_exact_bytes() {
    let mut first = asset(2, "IMAGE");
    first["originalFileName"] = json!("Zażółć \"photo\"\\name\n.jpg");
    let second = asset(1, "IMAGE");

    let fake = SnapshotFixture::new(vec![
        page(vec![first.clone(), second.clone()], None),
        page(vec![second.clone(), first.clone(), second], None),
    ])
    .await;

    let baseline = fake.source.contents(ALBUM).await.unwrap();
    let reordered = fake.source.contents(ALBUM).await.unwrap();
    assert_eq!(baseline.digest, reordered.digest);
    assert_eq!(baseline.bytes, reordered.bytes);
    let serialized = serde_json::to_vec(&baseline.items.values().collect::<Vec<_>>()).unwrap();
    assert_eq!(baseline.bytes, serialized.len());

    assert_eq!(
        baseline.digest,
        format!("{:x}", Sha256::digest(&serialized))
    );
    assert_eq!(baseline.digest.len(), 64);
}

#[test]
fn projection_hashes_hints_capture_resource_order_and_exact_bytes() {
    let mut dto = asset(1, "IMAGE");
    dto["originalFileName"] = json!("Zażółć \"photo\"\\name\n.jpg");
    let item = project(dto.clone());
    let mut original = Projection::new(SNAPSHOT_BYTES);
    original.json(&item).unwrap();
    let original = original.finish().0;

    for field in [
        "checksum",
        "updatedAt",
        "thumbhash",
        "fileCreatedAt",
        "isEdited",
    ] {
        let mut changed = dto.clone();

        changed[field] = match field {
            "fileCreatedAt" => json!("2024-01-01T00:30:01+02:00"),
            "isEdited" => json!(true),
            _ => json!("changed"),
        };

        let changed = project(changed);
        let mut digest = Projection::new(SNAPSHOT_BYTES);
        digest.json(&changed).unwrap();
        assert_ne!(original, digest.finish().0, "{field}");
    }

    let mut reversed = item.clone();
    reversed.object.resources.reverse();
    let mut digest = Projection::new(SNAPSHOT_BYTES);
    digest.json(&reversed).unwrap();
    assert_ne!(original, digest.finish().0);

    for item in [&item, &reversed] {
        let size = serde_json::to_vec(item).unwrap().len();
        assert_eq!(encoded_size(item, SNAPSHOT_BYTES).unwrap(), size);
        assert_eq!(encoded_size(item, size).unwrap(), size);
        assert!(encoded_size(item, size - 1).is_err());
    }
}

#[test]
fn projected_item_encoding_is_stable_across_refactors() {
    let asset = Uuid::from_u128(1);
    let album = Uuid::from_u128(2);

    let item = Item {
        id: asset,
        object: Object {
            id: format!("album:{album}:asset:{asset}"),
            parent_id: format!("album:{album}"),
            title: "Photo".into(),
            class: "object.item.imageItem.photo".into(),
            date: Some("2024-01-02".into()),
            art: Some(format!(
                "http://192.0.2.1:8200/media/assets/{asset}/preview"
            )),
            child_count: None,
            resources: vec![Resource {
                uri: format!("http://192.0.2.1:8200/media/assets/{asset}/original"),
                mime: "image/jpeg".into(),
                duration: None,
                byte_seek: false,
            }],
        },
        capture: Some("2024-01-02T03:04:05Z".parse().unwrap()),
        is_edited: false,
        checksum: Some("checksum".into()),
        updated_at: Some("updated".into()),
        thumbhash: Some("thumbhash".into()),
    };

    let json = serde_json::to_string(&item).unwrap();

    assert_eq!(
        json,
        r#"{"id":"00000000-0000-0000-0000-000000000001","object":{"id":"album:00000000-0000-0000-0000-000000000002:asset:00000000-0000-0000-0000-000000000001","parent_id":"album:00000000-0000-0000-0000-000000000002","title":"Photo","class":"object.item.imageItem.photo","date":"2024-01-02","art":"http://192.0.2.1:8200/media/assets/00000000-0000-0000-0000-000000000001/preview","child_count":null,"resources":[{"uri":"http://192.0.2.1:8200/media/assets/00000000-0000-0000-0000-000000000001/original","mime":"image/jpeg","duration":null,"byte_seek":false}]},"capture":"2024-01-02T03:04:05Z","is_edited":false,"checksum":"checksum","updated_at":"updated","thumbhash":"thumbhash"}"#
    );

    assert_eq!(
        format!("{:x}", Sha256::digest(json.as_bytes())),
        "d27096d54b96b8520a8ddbbededf29439eb59cb9e1020dbccc02d3c72b0422e0"
    );
}

#[test]
fn projected_root_encoding_is_stable_across_refactors() {
    let id = Uuid::from_u128(1);
    let mut dto = album(id);
    dto["albumName"] = json!("A&B");
    dto["endDate"] = json!("2024-02-03T04:05:06Z");
    let root = root("Photos", vec![dto]).unwrap();

    let expected_album = r#"{"id":"00000000-0000-0000-0000-000000000001","object":{"id":"album:00000000-0000-0000-0000-000000000001","parent_id":"0","title":"A&B","class":"object.container.album","date":"2023-12-31","art":"http://192.0.2.1:8200/media/assets/00000000-0000-0000-0000-000000000309/preview","child_count":null,"resources":[]},"created_at":"2023-12-31T22:30:00Z","end_date":"2024-02-03T04:05:06Z"}"#;

    let expected = [
        r#"[{"id":"0","parent_id":"-1","title":"Photos","class":"object.container","date":null,"art":null,"child_count":null,"resources":[]},"#,
        expected_album,
        "]",
    ].concat();

    assert_eq!(root.bytes, expected.len());

    assert_eq!(
        root.digest,
        format!("{:x}", Sha256::digest(expected.as_bytes()))
    );

    assert_eq!(
        serde_json::to_string(&root.albums[&id]).unwrap(),
        expected_album
    );

    assert_eq!(
        root.albums[&id].digest,
        format!("{:x}", Sha256::digest(expected_album.as_bytes()))
    );
}

#[tokio::test]
async fn traversal_progress_and_combined_page_budget() {
    for repeated in [false, true] {
        let mut replies = vec![page(vec![asset(1, "IMAGE")], Some("2"))];

        replies.push(page(
            if repeated {
                vec![asset(1, "IMAGE")]
            } else {
                vec![]
            },
            Some("3"),
        ));

        let fake = SnapshotFixture::new(replies).await;
        assert!(fake.source.contents(ALBUM).await.is_err());
        assert_eq!(fake.api.requests.lock().unwrap().len(), 2);
    }

    for finish in [false, true] {
        let mut replies = vec![page(vec![asset(1, "VIDEO")], None)];

        for number in 1..SEARCH_PAGES {
            let next = (number + 1).to_string();

            replies.push(page(
                vec![asset(number as u128, "VIDEO")],
                if finish && number == SEARCH_PAGES - 1 {
                    None
                } else {
                    Some(&next)
                },
            ));
        }

        let fake = SnapshotFixture::new(replies).await;
        assert_eq!(fake.source.contents(ALBUM).await.is_ok(), finish);
        assert_eq!(fake.api.requests.lock().unwrap().len(), SEARCH_PAGES);
    }
}

#[tokio::test]
async fn raw_record_item_and_projected_payload_limits() {
    let excluded = json!({"id": Uuid::from_u128(1), "type": "AUDIO", "visibility": "hidden", "isTrashed": false, "isEdited": false});

    // Raw records count even when identical and excluded; pages need not be full.
    let fake = SnapshotFixture::new(vec![page(vec![excluded; SEARCH_RECORDS + 1], None)]).await;

    assert!(
        fake.source
            .contents(ALBUM)
            .await
            .unwrap_err()
            .to_string()
            .contains("record limit")
    );

    for extra in [0, 1] {
        let encoded = json!({"id": Uuid::from_u128(1), "type": "VIDEO", "visibility": "timeline", "isTrashed": false, "isEdited": false});

        let fake = SnapshotFixture::new(vec![
            page(vec![asset(1, "VIDEO")], None),
            page(vec![encoded; SEARCH_RECORDS - 1 + extra], None),
        ])
        .await;

        assert_eq!(fake.source.contents(ALBUM).await.is_ok(), extra == 0);

        assert_eq!(fake.api.requests.lock().unwrap().len(), 2);
    }

    let mut replies = Vec::new();

    for page_number in 0..=ALBUM_ITEMS / immich::SEARCH_PAGE_SIZE {
        let start = page_number * immich::SEARCH_PAGE_SIZE + 1;

        let count = if start > ALBUM_ITEMS {
            1
        } else {
            immich::SEARCH_PAGE_SIZE
        };

        let next = (page_number + 2).to_string();

        replies.push(page(
            (start..start + count)
                .map(|id| asset(id as u128, "IMAGE"))
                .collect(),
            (count != 1).then_some(next.as_str()),
        ));
    }

    let fake = SnapshotFixture::new(replies).await;

    let error = fake.source.contents(ALBUM).await.unwrap_err().to_string();

    assert!(error.contains("item limit"), "{error}");

    let mut big = asset(1, "IMAGE");
    big["checksum"] = json!("x".repeat(SNAPSHOT_BYTES / 2));
    let mut bigger = big.clone();
    bigger["id"] = json!(Uuid::from_u128(2));

    let fake =
        SnapshotFixture::new(vec![page(vec![big], Some("2")), page(vec![bigger], None)]).await;

    assert!(
        fake.source
            .contents(ALBUM)
            .await
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
}

#[test]
fn root_album_limit_is_enforced() {
    let albums = (0..=MAX_ALBUMS)
        .map(|id| album(Uuid::from_u128(id as u128)))
        .collect();

    assert!(
        root("Photos", albums)
            .unwrap_err()
            .to_string()
            .contains("album limit")
    );
}
