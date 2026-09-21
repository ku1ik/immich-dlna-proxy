use std::net::SocketAddrV4;

use immich_dlna_proxy::{
    catalog::{Object, Resource},
    media::{DISPLAY, ORIGINAL, PLAYBACK, PREVIEW, asset_url},
};

pub const ALBUM_ID: &str = "album:10000000-0000-4000-8000-000000000001";
pub const ORIGINAL_JPEG_ID: &str = "20000000-0000-4000-8000-000000000001";
pub const GENERATED_JPEG_ID: &str = "20000000-0000-4000-8000-000000000002";
pub const VIDEO_ID: &str = "20000000-0000-4000-8000-000000000003";

pub fn objects(address: SocketAddrV4) -> Vec<Object> {
    let media = |asset: &str, representation: &str| {
        asset_url(address, asset.parse().unwrap(), representation)
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
            resources: Vec::new(),
        },
        Object {
            id: ALBUM_ID.into(),
            parent_id: "0".into(),
            title: "Mixed JPEG and H264 AAC".into(),
            class: "object.container.album".into(),
            date: Some("2024-01-01".into()),
            art: Some(media(ORIGINAL_JPEG_ID, "preview")),
            child_count: None,
            resources: Vec::new(),
        },
    ];

    for (asset, title, date, class, mime, representations) in [
        (
            ORIGINAL_JPEG_ID,
            "original.jpg",
            "2024-01-01",
            "object.item.imageItem.photo",
            "image/jpeg",
            [ORIGINAL, PREVIEW],
        ),
        (
            GENERATED_JPEG_ID,
            "generated-display.jpg",
            "2024-01-02",
            "object.item.imageItem.photo",
            "image/jpeg",
            [DISPLAY, PREVIEW],
        ),
        (
            VIDEO_ID,
            "video-original.mp4",
            "2024-01-03",
            "object.item.videoItem",
            "video/mp4",
            [ORIGINAL, PLAYBACK],
        ),
    ] {
        let resources = representations
            .into_iter()
            .map(|representation| Resource {
                uri: media(asset, representation),
                mime: mime.into(),
                duration: (asset == VIDEO_ID && representation == ORIGINAL)
                    .then(|| "0:00:30.000".into()),
                byte_seek: asset == VIDEO_ID,
            })
            .collect();

        objects.push(Object {
            id: format!("{ALBUM_ID}:asset:{asset}"),
            parent_id: ALBUM_ID.into(),
            title: title.into(),
            class: class.into(),
            date: Some(date.into()),
            art: (mime == "image/jpeg").then(|| media(asset, "preview")),
            child_count: None,
            resources,
        });
    }

    objects.push(Object {
        id: format!("{ALBUM_ID}:asset:20000000-0000-4000-8000-000000000008"),
        parent_id: ALBUM_ID.into(),
        title: "Playback Only - H264 AAC.mp4".into(),
        class: "object.item.videoItem".into(),
        date: Some("2024-01-03".into()),
        art: None,
        child_count: None,
        resources: vec![Resource {
            uri: media(VIDEO_ID, PLAYBACK),
            mime: "video/mp4".into(),
            duration: None,
            byte_seek: true,
        }],
    });

    objects
}

pub fn media_file(
    asset: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Option<(&'static str, &'static str)> {
    let representation = match (endpoint, query) {
        ("original", None | Some("")) => ORIGINAL,
        ("video/playback", None | Some("")) => PLAYBACK,
        ("thumbnail", Some("size=fullsize&edited=true" | "edited=true&size=fullsize")) => DISPLAY,
        ("thumbnail", Some("size=preview&edited=true" | "edited=true&size=preview")) => PREVIEW,
        _ => return None,
    };

    match (asset, representation) {
        (ORIGINAL_JPEG_ID, ORIGINAL) => Some(("original.jpg", "image/jpeg")),
        (ORIGINAL_JPEG_ID, PREVIEW) => Some(("original-preview.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, DISPLAY) => Some(("generated-display.jpg", "image/jpeg")),
        (GENERATED_JPEG_ID, PREVIEW) => Some(("generated-preview.jpg", "image/jpeg")),
        (VIDEO_ID, ORIGINAL) => Some(("video-original.mp4", "video/mp4")),
        (VIDEO_ID, PLAYBACK) => Some(("video-playback.mp4", "video/mp4")),
        _ => None,
    }
}
