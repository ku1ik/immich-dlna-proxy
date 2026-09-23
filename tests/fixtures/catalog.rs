use std::net::SocketAddrV4;

use immich_dlna_proxy::{
    catalog::{BrowseQuery, BrowseResult, Catalog, Object, ObjectId, Resource, parse_id},
    media::{
        Representation,
        Representation::{Display, Original, Playback, Preview},
        asset_url,
    },
    protocol::Fault,
};

pub const ALBUM_ID: &str = "album:10000000-0000-4000-8000-000000000001";
pub const ORIGINAL_JPEG_ID: &str = "20000000-0000-4000-8000-000000000001";
pub const GENERATED_JPEG_ID: &str = "20000000-0000-4000-8000-000000000002";
pub const VIDEO_ID: &str = "20000000-0000-4000-8000-000000000003";

pub(super) struct FixtureCatalog(pub(super) Vec<Object>);

impl Catalog for FixtureCatalog {
    fn system_update_id(&self) -> u32 {
        0
    }

    async fn browse(&self, args: BrowseQuery) -> Result<BrowseResult, Fault> {
        let id = match parse_id(&args.object_id)? {
            ObjectId::Root => "0".to_owned(),
            ObjectId::Album(album) => format!("album:{album}"),
            ObjectId::Item { album, asset } => format!("album:{album}:asset:{asset}"),
        };

        let object = self
            .0
            .iter()
            .find(|object| object.id == id)
            .ok_or(Fault::NoSuchObject)?;

        tracing::info!(object = %id, metadata = args.metadata, "fixture Browse selection");

        if args.metadata {
            return Ok(BrowseResult {
                objects: vec![object.clone()],
                total_matches: 1,
                update_id: 0,
            });
        }

        if object.class.starts_with("object.item") {
            return Err(Fault::NoSuchContainer);
        }

        let mut children: Vec<_> = self
            .0
            .iter()
            .filter(|object| object.parent_id == id)
            .collect();

        children.sort_by(|a, b| {
            let dates = a.date.cmp(&b.date);
            let dates = if args.sort == Some(true) {
                dates.reverse()
            } else {
                dates
            };

            dates.then_with(|| a.id.cmp(&b.id))
        });

        let total_matches = children.len() as u32;
        let count = if args.requested_count == 0 {
            usize::MAX
        } else {
            args.requested_count as usize
        };

        let objects = children
            .into_iter()
            .skip(args.starting_index as usize)
            .take(count)
            .cloned()
            .collect();

        Ok(BrowseResult {
            objects,
            total_matches,
            update_id: 0,
        })
    }
}

pub fn objects(address: SocketAddrV4) -> Vec<Object> {
    let media = |asset: &str, representation: Representation| {
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
            art: Some(media(ORIGINAL_JPEG_ID, Preview)),
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
            [Original, Preview],
        ),
        (
            GENERATED_JPEG_ID,
            "generated-display.jpg",
            "2024-01-02",
            "object.item.imageItem.photo",
            "image/jpeg",
            [Display, Preview],
        ),
        (
            VIDEO_ID,
            "video-original.mp4",
            "2024-01-03",
            "object.item.videoItem",
            "video/mp4",
            [Original, Playback],
        ),
    ] {
        let resources = representations
            .into_iter()
            .map(|representation| Resource {
                uri: media(asset, representation),
                mime: mime.into(),
                duration: (asset == VIDEO_ID && representation == Original)
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
            art: (mime == "image/jpeg").then(|| media(asset, Preview)),
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
            uri: media(VIDEO_ID, Playback),
            mime: "video/mp4".into(),
            duration: None,
            byte_seek: true,
        }],
    });

    objects
}
