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
fn image_resources_follow_mime_and_edit_selection() {
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
        let item = project(dto);
        assert_eq!(item.object.resources().len(), 2);
        assert!(item.object.resources()[0].uri.ends_with(representation));
        assert_eq!(item.object.resources()[0].mime, expected);
        assert!(item.object.resources()[1].uri.ends_with("preview"));

        assert!(
            item.object
                .resources()
                .iter()
                .all(|r| !r.byte_seek && r.duration.is_none())
        );
    }
}

#[test]
fn original_video_duration_is_formatted_from_nonnegative_milliseconds() {
    for (duration, expected) in [
        (0, Some("0:00:00.000")),
        (3_661_007, Some("1:01:01.007")),
        (i32::MAX as i64, Some("596:31:23.647")),
        (i32::MAX as i64 + 1, Some("596:31:23.648")),
        (-1, None),
    ] {
        let mut dto = asset(1, "VIDEO");
        dto["duration"] = json!(duration);
        let item = project(dto);
        assert_eq!(item.object.resources()[0].duration.as_deref(), expected);
    }
}

#[test]
fn original_video_without_mime_remains_seekable_with_preview_artwork() {
    let mut dto = asset(1, "VIDEO");
    dto["originalMimeType"] = Value::Null;
    let item = project(dto);
    assert_eq!(item.object.resources()[0].mime, "application/octet-stream");
    assert!(item.object.resources()[0].byte_seek);
    assert!(item.object.art.unwrap().ends_with("preview"));
}

#[test]
fn capture_instants_and_local_dates_are_independent() {
    let item = project(asset(1, "IMAGE"));
    assert_eq!(item.object.date, Some("2024-01-01".parse().unwrap()));

    assert_eq!(
        item.capture.unwrap().to_rfc3339(),
        "2023-12-31T22:30:00+00:00"
    );

    let mut dto = asset(1, "VIDEO");
    dto["localDateTime"] = json!("2024-01-01T23:00:00-12:00");

    assert_eq!(
        project(dto).object.date,
        Some("2024-01-01".parse().unwrap())
    );

    let mut dto = asset(1, "IMAGE");
    dto["fileCreatedAt"] = json!("2024-01-01T12:00:00");
    dto["localDateTime"] = json!("not a date");
    let item = project(dto);
    assert!(item.capture.is_none());
    assert!(item.object.date.is_none());
}

#[test]
fn item_titles_are_xml_safe_and_fall_back_to_asset_identity() {
    let mut dto = asset(1, "IMAGE");
    dto["originalFileName"] = json!("A\u{0001}&B");
    assert_eq!(project(dto.clone()).object.title, "A\u{fffd}&B");
    dto["originalFileName"] = json!("");
    assert_eq!(project(dto).object.title, Uuid::from_u128(1).to_string());
}

#[test]
fn projection_retains_revision_hints_without_exif_enrichment() {
    let mut dto = asset(1, "IMAGE");
    dto["checksum"] = json!("one");
    dto["updatedAt"] = json!("opaque hint");
    dto["thumbhash"] = json!("two");
    dto["exifInfo"] = json!(["ignored unsupported structure"]);
    let item = project(dto);
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
    ])
    .await;

    let root = fake.source.root().await.unwrap();

    assert!(root.albums.contains_key(&ALBUM));
    let contents = fake.source.contents(ALBUM).await.unwrap();
    assert_eq!(contents.items.len(), 1001);
    let video = &contents.items[&Uuid::from_u128(1001)].object;
    assert_eq!(video.resources().len(), 2);
    assert_eq!(video.resources()[0].mime, "video/quicktime");
    assert_eq!(video.resources()[1].mime, "video/mp4");
    assert!(video.resources().iter().all(|r| r.byte_seek));
    assert!(video.resources()[1].duration.is_none());

    let requests = fake.api.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].uri, "/prefix/api/server/version");
    assert_eq!(requests[1].uri, "/prefix/api/albums");

    for (index, expected_page) in [(2, 1), (3, 2), (4, 1)] {
        let request = &requests[index];
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri, "/prefix/api/search/metadata");
        assert_eq!(request.body["albumIds"], json!([ALBUM]));
        assert_eq!(request.body["page"], json!(expected_page));

        assert_eq!(
            request.body.get("isEncoded"),
            (index == 4).then_some(&Value::Bool(true))
        );

        assert_eq!(
            request.body.get("type"),
            (index == 4).then_some(&json!("VIDEO"))
        );
    }
}

#[tokio::test]
async fn encoded_pages_deduplicate_matches_and_track_nonmember_progress() {
    let fake = SnapshotFixture::new(vec![
        page(vec![asset(1, "VIDEO"), asset(2, "VIDEO")], None),
        page(vec![asset(99, "VIDEO")], Some("2")),
        page(vec![asset(2, "VIDEO"), asset(1, "VIDEO")], Some("3")),
        page(vec![asset(1, "VIDEO"), asset(2, "VIDEO")], None),
        page(vec![asset(2, "VIDEO"), asset(1, "VIDEO")], None),
        page(vec![asset(1, "VIDEO"), asset(2, "VIDEO")], None),
    ])
    .await;

    let paginated = fake.source.contents(ALBUM).await.unwrap();
    let reordered = fake.source.contents(ALBUM).await.unwrap();
    assert_eq!(paginated.items.len(), 2);
    assert_eq!(paginated.items, reordered.items);
    assert_eq!(paginated.digest, reordered.digest);
    assert_eq!(paginated.bytes, reordered.bytes);

    for item in paginated.items.values() {
        assert_eq!(item.object.resources().len(), 2);
    }

    let fake = SnapshotFixture::new(vec![
        page(vec![asset(1, "VIDEO")], None),
        page(vec![asset(99, "VIDEO")], Some("2")),
        page(vec![asset(99, "VIDEO")], Some("3")),
    ])
    .await;

    assert!(fake.source.contents(ALBUM).await.is_err());
    assert_eq!(fake.api.requests.lock().unwrap().len(), 3);
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
async fn duplicate_members_keep_first_metadata_and_eligibility() {
    let first = asset(1, "IMAGE");

    for (field, replacement) in [
        ("originalFileName", json!("changed")),
        ("checksum", json!("changed")),
        ("updatedAt", json!("changed")),
        ("thumbhash", json!("changed")),
        ("localDateTime", json!("2025-01-01T00:00:00Z")),
        ("fileCreatedAt", json!("2025-01-01T00:00:00Z")),
        ("isEdited", json!(true)),
        ("visibility", json!("hidden")),
        ("isTrashed", json!(true)),
        ("type", json!("VIDEO")),
    ] {
        let mut second = first.clone();
        second[field] = replacement;

        for reversed in [false, true] {
            let records = if reversed {
                vec![second.clone(), first.clone()]
            } else {
                vec![first.clone(), second.clone()]
            };

            let expected = project_item(
                "192.0.2.1:8200".parse().unwrap(),
                ALBUM,
                serde_json::from_value(records[0].clone()).unwrap(),
                &mut 0,
            )
            .unwrap();

            for paginated in [false, true] {
                let mut replies = if paginated {
                    vec![
                        page(vec![records[0].clone()], Some("2")),
                        page(vec![records[1].clone()], None),
                    ]
                } else {
                    vec![page(records.clone(), None)]
                };

                replies.push(page(vec![], None));
                let fake = SnapshotFixture::new(replies).await;
                let result = fake.source.contents(ALBUM).await.unwrap();

                assert_eq!(
                    result.items.values().collect::<Vec<_>>(),
                    expected.iter().collect::<Vec<_>>(),
                    "{field}, reversed={reversed}, paginated={paginated}"
                );

                let bytes = serde_json::to_vec(&result.items.values().collect::<Vec<_>>()).unwrap();
                assert_eq!(result.bytes, bytes.len());
                let digest: Digest = Sha256::digest(bytes).into();
                assert_eq!(result.digest, digest);
            }
        }
    }

    let mut irrelevant = first.clone();
    irrelevant["originalPath"] = json!("private ignored path");
    irrelevant["visibility"] = json!("archive");
    irrelevant
        .as_object_mut()
        .unwrap()
        .remove("originalFileName");
    let mut excluded = asset(2, "VIDEO");
    excluded["visibility"] = json!("future-visibility");
    excluded.as_object_mut().unwrap().remove("originalFileName");
    let mut excluded_changed = excluded.clone();
    excluded_changed["originalFileName"] = json!("irrelevant name");
    excluded_changed["checksum"] = json!("irrelevant hint");
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
    assert_eq!(first.object().child_count(), Some(2));

    let expected_object = Object {
        kind: ObjectKind::Root { child_count: 2 },
        title: "Photos".into(),
        date: None,
        art: None,
    };

    let expected = serde_json::to_vec(&(
        &expected_object,
        &first.albums[&Uuid::from_u128(2)].metadata,
        &first.albums[&ALBUM].metadata,
    ))
    .unwrap();

    assert_eq!(first.object(), expected_object);
    assert_eq!(first.bytes, expected.len());
    assert_eq!(first.digest, <Digest>::from(Sha256::digest(&expected)));

    assert_eq!(
        root("Photos", vec![]).unwrap().object().child_count(),
        Some(0)
    );

    assert_eq!(
        first.albums[&ALBUM].metadata.object.title,
        ALBUM.to_string()
    );

    assert!(first.albums[&ALBUM].metadata.object.art.is_none());

    let duplicate = json!({"id": ALBUM, "albumName": "changed"});

    for records in [
        vec![empty.clone(), duplicate.clone()],
        vec![duplicate, empty.clone()],
    ] {
        let expected = root("Photos", vec![records[0].clone()]).unwrap();
        let actual = root("Photos", records).unwrap();
        assert_eq!(actual.albums, expected.albums);
        assert_eq!(actual.digest, expected.digest);
        assert_eq!(actual.bytes, expected.bytes);
    }

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
        let album = &root.albums[&ALBUM].metadata;
        assert_eq!(album.end_date, expected.map(|date| date.parse().unwrap()));
        assert_eq!(album.object.date, Some("2023-12-31".parse().unwrap()));

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

    let digest: Digest = Sha256::digest(&serialized).into();
    assert_eq!(baseline.digest, digest);
}

#[test]
fn projection_hashes_hints_capture_resource_order_and_exact_bytes() {
    let mut dto = asset(1, "IMAGE");
    dto["originalFileName"] = json!("Zażółć \"photo\"\\name\n.jpg");
    let item = project(dto.clone());
    let mut original = Projection::new(SNAPSHOT_BYTES);
    original.json(&item).unwrap();
    let original = original.finish().0;

    for (field, replacement) in [
        ("checksum", json!("changed")),
        ("updatedAt", json!("changed")),
        ("thumbhash", json!("changed")),
        ("fileCreatedAt", json!("2024-01-01T00:30:01+02:00")),
        ("isEdited", json!(true)),
    ] {
        let mut changed = dto.clone();
        changed[field] = replacement;
        let changed = project(changed);
        let mut digest = Projection::new(SNAPSHOT_BYTES);
        digest.json(&changed).unwrap();
        assert_ne!(original, digest.finish().0, "{field}");
    }

    let mut reversed = item.clone();

    let ObjectKind::Photo { resources, .. } = &mut reversed.object.kind else {
        panic!("expected photo");
    };

    resources.reverse();
    let mut digest = Projection::new(SNAPSHOT_BYTES);
    digest.json(&reversed).unwrap();
    assert_ne!(original, digest.finish().0);

    for item in [&item, &reversed] {
        let size = serde_json::to_vec(item).unwrap().len();

        for limit in [SNAPSHOT_BYTES, size] {
            let mut count = ByteCount::new(limit);
            count.json(item).unwrap();
            assert_eq!(count.bytes, size);
        }

        assert!(ByteCount::new(size - 1).json(item).is_err());
    }
}

#[test]
fn root_fingerprints_cover_membership_and_album_metadata() {
    let dto = album(ALBUM);
    let baseline = root("Photos", vec![dto.clone()]).unwrap();
    assert_ne!(baseline.digest, root("Photos", vec![]).unwrap().digest);

    for (field, value) in [
        ("albumName", json!("Changed")),
        ("albumThumbnailAssetId", json!(Uuid::from_u128(987))),
        ("createdAt", json!("2025-01-01T00:00:00Z")),
        ("endDate", json!("2025-01-01T00:00:00Z")),
    ] {
        let mut changed = dto.clone();
        changed[field] = value;
        let changed = root("Photos", vec![changed]).unwrap();
        assert_ne!(baseline.digest, changed.digest, "{field}");

        assert_ne!(
            baseline.albums[&ALBUM].digest, changed.albums[&ALBUM].digest,
            "{field}"
        );
    }
}

#[tokio::test]
async fn traversal_progress_and_combined_page_budget() {
    for items in [vec![], vec![asset(1, "IMAGE")]] {
        let fake = SnapshotFixture::new(vec![
            page(vec![asset(1, "IMAGE")], Some("2")),
            page(items, Some("3")),
        ])
        .await;

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

#[test]
fn root_respects_exact_snapshot_byte_budget() {
    let mut first = album(ALBUM);
    let mut second = album(Uuid::from_u128(2));
    first["albumName"] = json!("x");
    second["albumName"] = json!("y");
    let baseline = root("Photos", vec![first.clone(), second.clone()]).unwrap();
    let padding = SNAPSHOT_BYTES - baseline.bytes;

    for extra in [0, 1] {
        first["albumName"] = json!("x".repeat(1 + padding / 2));
        second["albumName"] = json!("y".repeat(1 + padding - padding / 2 + extra));
        let result = root("Photos", vec![first.clone(), second.clone()]);

        if extra == 0 {
            let root = result.unwrap();

            let expected = serde_json::to_vec(&(
                &root.object(),
                &root.albums[&Uuid::from_u128(2)].metadata,
                &root.albums[&ALBUM].metadata,
            ))
            .unwrap();

            assert_eq!(root.bytes, SNAPSHOT_BYTES);
            assert_eq!(root.bytes, expected.len());
        } else {
            assert!(result.unwrap_err().to_string().contains("byte limit"));
        }
    }
}

#[tokio::test]
async fn playback_append_respects_exact_snapshot_byte_budget() {
    let mut video = asset(1, "VIDEO");
    let mut photo = asset(2, "IMAGE");
    video["checksum"] = json!("");
    photo["checksum"] = json!("");
    let mut expected = [project(video.clone()), project(photo.clone())];

    let ObjectKind::Video { resources, .. } = &mut expected[0].object.kind else {
        panic!("expected video");
    };

    resources.push(Resource {
        uri: "http://192.0.2.1:8200/media/assets/00000000-0000-0000-0000-000000000001/playback"
            .into(),
        mime: "video/mp4".into(),
        duration: None,
        byte_seek: true,
    });

    let padding = SNAPSHOT_BYTES - serde_json::to_vec(&expected).unwrap().len();

    for extra in [0, 1] {
        video["checksum"] = json!("x".repeat(padding / 2));
        photo["checksum"] = json!("y".repeat(padding - padding / 2 + extra));

        let fake = SnapshotFixture::new(vec![
            page(vec![video.clone()], Some("2")),
            page(vec![photo.clone()], None),
            // Non-video IDs in the encoded response must not acquire a resource.
            page(vec![asset(1, "VIDEO"), asset(2, "IMAGE")], None),
        ])
        .await;

        let result = fake.source.contents(ALBUM).await;

        if extra == 0 {
            let contents = result.unwrap();
            assert_eq!(contents.bytes, SNAPSHOT_BYTES);
            let resources = contents.items[&Uuid::from_u128(1)].object.resources();
            assert_eq!(resources, expected[0].object.resources());

            assert_eq!(
                contents.items[&Uuid::from_u128(2)].object.resources(),
                expected[1].object.resources()
            );
        } else {
            assert!(result.unwrap_err().to_string().contains("byte limit"));
        }

        assert_eq!(fake.api.requests.lock().unwrap().len(), 3);
    }
}
