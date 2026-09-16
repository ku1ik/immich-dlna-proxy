use std::net::SocketAddrV4;

use immich_dlna_proxy::protocol::{Object, Resource};

pub const ALBUM_ID: &str = "album:10000000-0000-4000-8000-000000000001";
pub const ORIGINAL_JPEG_ID: &str = "20000000-0000-4000-8000-000000000001";
pub const GENERATED_JPEG_ID: &str = "20000000-0000-4000-8000-000000000002";
pub const VIDEO_ID: &str = "20000000-0000-4000-8000-000000000003";

const ALBUM_ART_CASES: [(&str, &str, &str); 2] = [
    (
        "album:10000000-0000-4000-8000-000000000201",
        "20000000-0000-4000-8000-000000000201",
        "A - Art URI Only",
    ),
    (
        "album:10000000-0000-4000-8000-000000000202",
        "20000000-0000-4000-8000-000000000202",
        "B - Art URI Plus Resource",
    ),
];

pub fn album_art_objects(address: SocketAddrV4) -> Vec<Object> {
    let baseline = objects(address);
    let mut objects = vec![baseline[0].clone()];
    objects[0].title = "DLNA Album Art A-B".into();
    objects[0].child_count = Some(2);

    for (index, (album_id, alias, title)) in ALBUM_ART_CASES.into_iter().enumerate() {
        let cover = format!("http://{address}/media/assets/{alias}/preview");
        let mut album = baseline[1].clone();
        album.id = album_id.into();
        album.title = title.into();
        album.art = Some(cover.clone());

        if index == 1 {
            album.resources.push(Resource {
                uri: cover,
                mime: "image/jpeg".into(),
                duration: None,
                byte_seek: false,
            });
        }

        objects.push(album);

        for mut child in baseline[2..].iter().cloned() {
            child.id = child
                .id
                .replace(ALBUM_ID, album_id)
                .replace(GENERATED_JPEG_ID, alias);

            child.parent_id = album_id.into();
            child.art = child.art.map(|uri| uri.replace(GENERATED_JPEG_ID, alias));

            for resource in &mut child.resources {
                resource.uri = resource.uri.replace(GENERATED_JPEG_ID, alias);
            }

            objects.push(child);
        }
    }

    objects
}

pub fn album_art_media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    let asset = if ALBUM_ART_CASES.iter().any(|(_, alias, _)| *alias == asset) {
        GENERATED_JPEG_ID
    } else {
        asset
    };

    media_file(asset, endpoint, query)
}

pub const JPEG_LABEL_FILES: [&str; 2] = ["image-1.jpg", "image-2.jpg"];
const JPEG_LABEL_CASES: [(&str, &str, &str, &str); 4] = [
    (
        "20000000-0000-4000-8000-000000000101",
        "1A - PNG label",
        "image/png",
        "image-1.jpg",
    ),
    (
        "20000000-0000-4000-8000-000000000102",
        "1B - JPEG label",
        "image/jpeg",
        "image-1.jpg",
    ),
    (
        "20000000-0000-4000-8000-000000000103",
        "2A - PNG label",
        "image/png",
        "image-2.jpg",
    ),
    (
        "20000000-0000-4000-8000-000000000104",
        "2B - JPEG label",
        "image/jpeg",
        "image-2.jpg",
    ),
];

pub fn jpeg_label_objects(address: SocketAddrV4) -> Vec<Object> {
    let mut objects = objects(address);
    objects.truncate(2);
    objects[0].title = "DLNA JPEG Label Test".into();
    objects[1].title = "Same Bytes - PNG vs JPEG".into();
    objects[1].date = None;
    objects[1].art = None;

    for (asset, title, mime, _) in JPEG_LABEL_CASES {
        objects.push(Object {
            id: format!("{ALBUM_ID}:asset:{asset}"),
            parent_id: ALBUM_ID.into(),
            title: title.into(),
            class: "object.item.imageItem.photo".into(),
            date: None,
            art: None,
            child_count: None,
            resources: vec![Resource {
                uri: format!("http://{address}/media/assets/{asset}/original"),
                mime: mime.into(),
                byte_seek: false,
                duration: None,
            }],
        });
    }

    objects
}

pub fn jpeg_label_media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    if endpoint != "original" || query.is_some() {
        return None;
    }

    JPEG_LABEL_CASES
        .iter()
        .find(|(id, _, _, _)| *id == asset)
        .map(|(_, _, mime, file)| (*file, *mime))
}

/// Immutable root, mixed album, and four items, in that order. No upstream I/O.
pub fn objects(http_address: SocketAddrV4) -> Vec<Object> {
    let media_uri = |asset: &str, representation: &str| {
        format!("http://{http_address}/media/assets/{asset}/{representation}")
    };

    let mut objects = vec![
        Object {
            id: "0".into(),
            parent_id: "-1".into(),
            title: "DLNA Fixture Baseline".into(),
            class: "object.container".into(),
            date: None,
            art: None,
            child_count: Some(1),
            resources: vec![],
        },
        Object {
            id: ALBUM_ID.into(),
            parent_id: "0".into(),
            title: "Mixed JPEG and H264 AAC".into(),
            class: "object.container.album".into(),
            date: Some("2024-01-01".into()),
            art: Some(media_uri(ORIGINAL_JPEG_ID, "preview")),
            child_count: None,
            resources: vec![],
        },
    ];

    for (asset, title, date, class, mime, representations) in [
        (
            ORIGINAL_JPEG_ID,
            "original.jpg",
            "2024-01-01",
            "object.item.imageItem.photo",
            "image/jpeg",
            ["original", "preview"],
        ),
        (
            GENERATED_JPEG_ID,
            "generated-display.jpg",
            "2024-01-02",
            "object.item.imageItem.photo",
            "image/jpeg",
            ["display", "preview"],
        ),
        (
            VIDEO_ID,
            "video-original.mp4",
            "2024-01-03",
            "object.item.videoItem",
            "video/mp4",
            ["original", "playback"],
        ),
    ] {
        let resources = representations
            .into_iter()
            .map(|representation| Resource {
                uri: media_uri(asset, representation),
                mime: mime.into(),
                byte_seek: asset == VIDEO_ID,
                duration: (asset == VIDEO_ID && representation == "original")
                    .then(|| "0:00:30.000".into()),
            })
            .collect();

        objects.push(Object {
            id: format!("{ALBUM_ID}:asset:{asset}"),
            parent_id: ALBUM_ID.into(),
            title: title.into(),
            class: class.into(),
            date: Some(date.into()),
            art: (mime == "image/jpeg").then(|| media_uri(asset, "preview")),
            child_count: None,
            resources,
        });
    }

    // A separate test appearance makes the existing alternative selectable on first-resource clients.
    objects.push(Object {
        id: format!("{ALBUM_ID}:asset:20000000-0000-4000-8000-000000000008"),
        parent_id: ALBUM_ID.into(),
        title: "Playback Only - H264 AAC.mp4".into(),
        class: "object.item.videoItem".into(),
        date: Some("2024-01-03".into()),
        art: None,
        child_count: None,
        resources: vec![Resource {
            uri: media_uri(VIDEO_ID, "playback"),
            mime: "video/mp4".into(),
            byte_seek: true,
            duration: None,
        }],
    });

    objects
}

/// Map the suffix after `/api/assets/<asset>/` and raw query (without `?`).
/// Only fixture renditions are recognized; callers serve the file from their fixture directory.
pub fn media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    let representation = match (endpoint, query) {
        ("original", None | Some("")) => "original",
        ("video/playback", None | Some("")) => "playback",
        ("thumbnail", Some("size=fullsize&edited=true" | "edited=true&size=fullsize")) => "display",
        ("thumbnail", Some("size=preview&edited=true" | "edited=true&size=preview")) => "preview",
        _ => return None,
    };

    match (asset, representation) {
        (ORIGINAL_JPEG_ID, "original") => Some(("original.jpg", "image/jpeg")),
        (ORIGINAL_JPEG_ID, "preview") => Some(("original-preview.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, "display") => Some(("generated-display.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, "preview") => Some(("generated-preview.jpg", "image/jpeg")),
        (VIDEO_ID, "original") => Some(("video-original.mp4", "video/mp4")),
        (VIDEO_ID, "playback") => Some(("video-playback.mp4", "video/mp4")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn album_art_cases_preserve_baseline_with_only_the_controlled_differences() {
        let address = "192.0.2.10:8500".parse().unwrap();
        let baseline = objects(address);
        let catalog = album_art_objects(address);
        assert_eq!(catalog.len(), 11);
        let mut root = baseline[0].clone();
        root.title = "DLNA Album Art A-B".into();
        root.child_count = Some(2);
        assert_eq!(catalog[0], root);
        let ids: std::collections::HashSet<_> = catalog.iter().map(|object| &object.id).collect();
        assert_eq!(ids.len(), catalog.len());
        assert_ne!(catalog[1].art, catalog[6].art);

        for (case, (album_id, alias, title, resource_count)) in catalog[1..].chunks_exact(5).zip([
            (
                "album:10000000-0000-4000-8000-000000000201",
                "20000000-0000-4000-8000-000000000201",
                "A - Art URI Only",
                0,
            ),
            (
                "album:10000000-0000-4000-8000-000000000202",
                "20000000-0000-4000-8000-000000000202",
                "B - Art URI Plus Resource",
                1,
            ),
        ]) {
            let cover = format!("http://{address}/media/assets/{alias}/preview");
            let mut album = baseline[1].clone();
            album.id = album_id.into();
            album.title = title.into();
            album.art = Some(cover.clone());

            if resource_count == 1 {
                album.resources.push(Resource {
                    uri: cover.clone(),
                    mime: "image/jpeg".into(),
                    duration: None,
                    byte_seek: false,
                });
            }

            assert_eq!(case[0], album);

            assert_eq!(
                catalog
                    .iter()
                    .filter(|item| item.parent_id == album_id)
                    .count(),
                4
            );

            assert_eq!(case[2].id, format!("{album_id}:asset:{alias}"));
            assert_eq!(case[2].art.as_deref(), Some(cover.as_str()));
            assert_eq!(case[2].resources[1].uri, cover);
            assert_ne!(case[1].art, case[0].art);

            assert!(
                case[1]
                    .resources
                    .iter()
                    .all(|resource| resource.uri != cover)
            );

            for (index, (item, original)) in case[1..].iter().zip(&baseline[2..]).enumerate() {
                let asset = if index == 1 {
                    alias
                } else {
                    original.id.rsplit(':').next().unwrap()
                };

                assert_eq!(item.id, format!("{album_id}:asset:{asset}"));
                assert_eq!(item.parent_id, album_id);
                let mut normalized = item.clone();
                normalized.id.clone_from(&original.id);
                normalized.parent_id.clone_from(&original.parent_id);

                if index == 1 {
                    assert_eq!(
                        item.resources[0].uri,
                        format!("http://{address}/media/assets/{alias}/display")
                    );

                    normalized.art = normalized
                        .art
                        .map(|uri| uri.replace(alias, GENERATED_JPEG_ID));

                    for resource in &mut normalized.resources {
                        resource.uri = resource.uri.replace(alias, GENERATED_JPEG_ID);
                    }
                }

                assert_eq!(&normalized, original);
            }
        }
    }

    #[test]
    fn album_art_aliases_map_to_existing_generated_files_with_strict_queries() {
        for alias in [
            "20000000-0000-4000-8000-000000000201",
            "20000000-0000-4000-8000-000000000202",
        ] {
            for (query, file) in [
                ("size=fullsize&edited=true", "generated-display.jpg"),
                ("edited=true&size=fullsize", "generated-display.jpg"),
                ("size=preview&edited=true", "generated-preview.jpg"),
                ("edited=true&size=preview", "generated-preview.jpg"),
            ] {
                assert_eq!(
                    album_art_media_file(alias, "thumbnail", Some(query)),
                    Some((file, "image/jpeg"))
                );

                assert_eq!(media_file(alias, "thumbnail", Some(query)), None);
            }
        }

        for asset in [
            ORIGINAL_JPEG_ID,
            GENERATED_JPEG_ID,
            VIDEO_ID,
            "20000000-0000-4000-8000-000000000201",
            "20000000-0000-4000-8000-000000000202",
            "20000000-0000-4000-8000-000000000203",
            "20000000-0000-4000-8000-000000000008",
            "../generated-preview.jpg",
        ] {
            let mapped = match asset {
                "20000000-0000-4000-8000-000000000201" | "20000000-0000-4000-8000-000000000202" => {
                    GENERATED_JPEG_ID
                }

                _ => asset,
            };

            for endpoint in [
                "original",
                "thumbnail",
                "video/playback",
                "display",
                "preview",
                "playback",
                "../original",
            ] {
                for query in [
                    None,
                    Some(""),
                    Some("size=fullsize&edited=true"),
                    Some("size=preview&edited=true"),
                    Some("edited=true"),
                    Some("size=preview"),
                    Some("size=fullsize&edited=false"),
                    Some("size=preview&edited=false"),
                    Some("size=preview&edited=true&edited=false"),
                    Some("size=preview&edited=true&extra=1"),
                    Some("size=preview&edited=true&edited=true"),
                ] {
                    assert_eq!(
                        album_art_media_file(asset, endpoint, query),
                        media_file(mapped, endpoint, query),
                        "{asset}/{endpoint}?{query:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn album_art_didl_resource_is_generic_and_obeys_filters() {
        let catalog = album_art_objects("192.0.2.10:8500".parse().unwrap());
        assert_eq!(catalog.len(), 11);

        for (filter, art_count, resource_count) in [
            ("*", 1, 1),
            ("", 0, 0),
            ("upnp:albumArtURI", 1, 0),
            ("res", 0, 1),
            ("res@duration,res@size,res@resolution", 0, 1),
        ] {
            for (album, has_resource) in [(&catalog[1], false), (&catalog[6], true)] {
                let xml = immich_dlna_proxy::protocol::didl(
                    std::slice::from_ref(album),
                    &immich_dlna_proxy::protocol::Filter::parse(filter).unwrap(),
                )
                .unwrap();

                assert_eq!(xml.matches("<container ").count(), 1);
                assert!(xml.contains("<upnp:class>object.container.album</upnp:class>"));
                assert_eq!(xml.matches("<upnp:albumArtURI>").count(), art_count);

                assert_eq!(
                    xml.matches("<res ").count(),
                    if has_resource { resource_count } else { 0 }
                );

                if art_count == 1 {
                    assert!(xml.contains(&format!(
                        "<upnp:albumArtURI>{}</upnp:albumArtURI>",
                        album.art.as_deref().unwrap()
                    )));
                }

                if has_resource && resource_count == 1 {
                    assert!(xml.contains(&format!(
                        "<res protocolInfo=\"http-get:*:image/jpeg:*\">{}</res>",
                        album.art.as_deref().unwrap()
                    )));
                }

                for absent in [
                    "DLNA.ORG_",
                    "duration=",
                    "size=",
                    "resolution=",
                    "childCount=",
                ] {
                    assert!(!xml.contains(absent), "{filter}: {xml}");
                }
            }
        }
    }

    #[test]
    fn jpeg_label_pairs_share_bytes_without_artwork_or_alternative_resources() {
        let objects = jpeg_label_objects("192.0.2.10:8400".parse().unwrap());
        assert_eq!(objects.len(), 6);
        assert_eq!(objects[0].title, "DLNA JPEG Label Test");
        assert_eq!(objects[1].title, "Same Bytes - PNG vs JPEG");
        assert!(objects.iter().all(|object| object.art.is_none()));

        for (pair, expected_file) in objects[2..].chunks_exact(2).zip(JPEG_LABEL_FILES) {
            assert_eq!(pair[0].resources.len(), 1);
            assert_eq!(pair[1].resources.len(), 1);
            assert_eq!(pair[0].resources[0].mime, "image/png");
            assert_eq!(pair[1].resources[0].mime, "image/jpeg");
            let mut first = pair[0].clone();
            first.id.clone_from(&pair[1].id);
            first.title.clone_from(&pair[1].title);
            first.resources.clone_from(&pair[1].resources);
            assert_eq!(first, pair[1]);

            for item in pair {
                let asset = item.id.rsplit(':').next().unwrap();

                assert_eq!(
                    jpeg_label_media_file(asset, "original", None),
                    Some((expected_file, item.resources[0].mime.as_str()))
                );
                assert_eq!(jpeg_label_media_file(asset, "preview", None), None);
                assert_eq!(
                    jpeg_label_media_file(asset, "original", Some("edited=true")),
                    None
                );
                assert_eq!(media_file(asset, "original", None), None);
            }
        }

        assert_eq!(
            jpeg_label_media_file(ORIGINAL_JPEG_ID, "original", None),
            None
        );

        let xml = immich_dlna_proxy::protocol::didl(
            &objects,
            &immich_dlna_proxy::protocol::Filter::parse("*").unwrap(),
        )
        .unwrap();
        assert_eq!(xml.matches("http-get:*:image/png:*").count(), 2);
        assert_eq!(xml.matches("http-get:*:image/jpeg:*").count(), 2);
        assert!(!xml.contains("DLNA.ORG_"));
    }

    #[test]
    fn fixture_identity_hierarchy_and_resource_order_are_fixed() {
        let address = "192.0.2.10:8200".parse().unwrap();
        let catalog = objects(address);
        assert_eq!(catalog.len(), 6);
        assert_eq!(catalog[0].id, "0");
        assert_eq!(catalog[0].parent_id, "-1");
        assert_eq!(catalog[0].class, "object.container");
        assert_eq!(catalog[0].child_count, Some(1));
        assert_eq!(catalog[1].id, ALBUM_ID);
        assert_eq!(catalog[1].parent_id, "0");
        assert_eq!(catalog[1].class, "object.container.album");
        assert_eq!(catalog[1].child_count, None);

        for (item, (asset, representations, mime, class)) in catalog[2..5].iter().zip([
            (
                ORIGINAL_JPEG_ID,
                ["original", "preview"],
                "image/jpeg",
                "object.item.imageItem.photo",
            ),
            (
                GENERATED_JPEG_ID,
                ["display", "preview"],
                "image/jpeg",
                "object.item.imageItem.photo",
            ),
            (
                VIDEO_ID,
                ["original", "playback"],
                "video/mp4",
                "object.item.videoItem",
            ),
        ]) {
            assert_eq!(item.id, format!("{ALBUM_ID}:asset:{asset}"));
            assert_eq!(item.parent_id, ALBUM_ID);
            assert_eq!(item.class, class);
            assert_eq!(item.resources.len(), 2);

            for (resource, representation) in item.resources.iter().zip(representations) {
                assert_eq!(
                    resource.uri,
                    format!("http://{address}/media/assets/{asset}/{representation}")
                );

                assert_eq!(resource.mime, mime);
                assert_eq!(resource.byte_seek, asset == VIDEO_ID);

                assert_eq!(
                    resource.duration.as_deref(),
                    (asset == VIDEO_ID && representation == "original").then_some("0:00:30.000")
                );
            }
        }

        let other_address = objects("192.0.2.20:8300".parse().unwrap());

        for (before, after) in catalog.iter().zip(other_address) {
            assert_eq!(before.id, after.id);
            assert_eq!(before.parent_id, after.parent_id);
        }
    }

    #[test]
    fn playback_only_item_reuses_existing_video_resource_without_invented_metadata() {
        let address = "192.0.2.10:8200".parse().unwrap();
        let catalog = objects(address);
        let playback = catalog.last().unwrap();
        let appearance = "20000000-0000-4000-8000-000000000008";
        assert_eq!(playback.id, format!("{ALBUM_ID}:asset:{appearance}"));
        assert_eq!(playback.title, "Playback Only - H264 AAC.mp4");
        assert_eq!(playback.parent_id, ALBUM_ID);
        assert_eq!(playback.class, "object.item.videoItem");
        assert_eq!(playback.date, catalog[4].date);
        assert!(playback.art.is_none());
        assert_eq!(playback.child_count, None);
        assert_eq!(playback.resources, catalog[4].resources[1..]);
        assert_eq!(media_file(appearance, "video/playback", None), None);
        assert_eq!(media_file(appearance, "original", None), None);

        let ids: std::collections::HashSet<_> = catalog.iter().map(|object| &object.id).collect();
        assert_eq!(ids.len(), catalog.len());

        let xml = immich_dlna_proxy::protocol::didl(
            std::slice::from_ref(playback),
            &immich_dlna_proxy::protocol::Filter::parse("*").unwrap(),
        )
        .unwrap();

        assert_eq!(xml.matches("<res ").count(), 1);

        assert!(xml.contains(&format!(
            "<res protocolInfo=\"http-get:*:video/mp4:DLNA.ORG_OP=01\">http://{address}/media/assets/{VIDEO_ID}/playback</res>"
        )));

        assert_eq!(xml.matches("DLNA.ORG_").count(), 1);
        assert!(!xml.contains("duration="));
        assert!(!xml.contains("size="));
        assert!(!xml.contains("resolution="));
    }

    #[test]
    fn fixture_media_routes_map_to_the_exact_manifest() {
        for (asset, endpoint, query, filename, mime) in [
            (
                ORIGINAL_JPEG_ID,
                "original",
                None,
                "original.jpg",
                "image/jpeg",
            ),
            (
                ORIGINAL_JPEG_ID,
                "thumbnail",
                Some("size=preview&edited=true"),
                "original-preview.jpg",
                "image/jpeg",
            ),
            (
                GENERATED_JPEG_ID,
                "thumbnail",
                Some("size=fullsize&edited=true"),
                "generated-display.jpg",
                "image/jpeg",
            ),
            (
                GENERATED_JPEG_ID,
                "thumbnail",
                Some("size=preview&edited=true"),
                "generated-preview.jpg",
                "image/jpeg",
            ),
            (
                VIDEO_ID,
                "original",
                None,
                "video-original.mp4",
                "video/mp4",
            ),
            (
                VIDEO_ID,
                "video/playback",
                None,
                "video-playback.mp4",
                "video/mp4",
            ),
        ] {
            assert_eq!(media_file(asset, endpoint, query), Some((filename, mime)));

            if let Some(query) = query {
                let reversed = query.split('&').rev().collect::<Vec<_>>().join("&");

                assert_eq!(
                    media_file(asset, endpoint, Some(&reversed)),
                    Some((filename, mime))
                );
            } else {
                assert_eq!(
                    media_file(asset, endpoint, Some("")),
                    Some((filename, mime))
                );
            }
        }
    }

    #[test]
    fn fixture_media_mapping_rejects_unknown_or_changed_requests() {
        for (asset, endpoint, query) in [
            ("00000000-0000-0000-0000-000000000000", "original", None),
            ("../original.jpg", "original", None),
            (ORIGINAL_JPEG_ID, "../original", None),
            (GENERATED_JPEG_ID, "original", None),
            (ORIGINAL_JPEG_ID, "original", Some("edited=true")),
            (VIDEO_ID, "video/playback", Some("edited=true")),
            (VIDEO_ID, "playback", None),
            (GENERATED_JPEG_ID, "thumbnail", None),
            (GENERATED_JPEG_ID, "thumbnail", Some("size=fullsize")),
            (
                GENERATED_JPEG_ID,
                "thumbnail",
                Some("size=preview&edited=false"),
            ),
            (
                GENERATED_JPEG_ID,
                "thumbnail",
                Some("size=preview&edited=true&edited=false"),
            ),
            (
                GENERATED_JPEG_ID,
                "thumbnail",
                Some("size=preview&edited=true&extra=1"),
            ),
        ] {
            assert_eq!(media_file(asset, endpoint, query), None);
        }
    }
}
