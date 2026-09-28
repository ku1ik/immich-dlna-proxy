use std::net::SocketAddrV4;

use immich_dlna_proxy::{
    catalog::{
        BrowseMode, BrowseQuery, BrowseResult, Catalog, Object, ObjectId, ObjectKind, Resource,
        SortOrder, parse_id,
    },
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
    async fn system_update_id(&self) -> Result<u32, Fault> {
        Ok(0)
    }

    async fn browse(
        &self,
        args: BrowseQuery,
        _: tokio::time::Instant,
    ) -> Result<BrowseResult, Fault> {
        let id = args.object_id;

        let object = self
            .0
            .iter()
            .find(|object| object.id() == id)
            .ok_or(Fault::NoSuchObject)?;

        tracing::info!(object = %id, metadata = (args.mode == BrowseMode::Metadata), "fixture Browse selection");

        let BrowseMode::DirectChildren {
            starting_index,
            requested_count,
        } = args.mode
        else {
            return Ok(BrowseResult {
                objects: vec![object.clone()],
                total_matches: 1,
                update_id: 0,
            });
        };

        if matches!(
            object.kind,
            ObjectKind::Photo { .. } | ObjectKind::Video { .. }
        ) {
            return Err(Fault::NoSuchContainer);
        }

        let mut children: Vec<_> = self
            .0
            .iter()
            .filter(|object| object.parent_id().is_some_and(|parent| parent == id))
            .collect();

        children.sort_by(|a, b| {
            let dates = a.date.cmp(&b.date);

            let dates = if args.sort == SortOrder::DateDescending {
                dates.reverse()
            } else {
                dates
            };

            dates.then_with(|| a.id().cmp(&b.id()))
        });

        let total_matches = children.len() as u32;

        let count = if requested_count == 0 {
            usize::MAX
        } else {
            requested_count as usize
        };

        let objects = children
            .into_iter()
            .skip(starting_index as usize)
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
    let ObjectId::Album(album) = parse_id(ALBUM_ID).unwrap() else {
        unreachable!();
    };

    let media = |asset: &str, representation: Representation| {
        asset_url(address, asset.parse().unwrap(), representation)
    };

    let mut objects = vec![
        Object {
            kind: ObjectKind::Root { child_count: 1 },
            title: "DLNA Fixture Baseline".into(),
            date: None,
            art: None,
        },
        Object {
            kind: ObjectKind::Album { id: album },
            title: "Mixed JPEG and H264 AAC".into(),
            date: Some("2024-01-01".parse().unwrap()),
            art: Some(media(ORIGINAL_JPEG_ID, Preview)),
        },
    ];

    for (asset, title, date, mime, representations) in [
        (
            ORIGINAL_JPEG_ID,
            "original.jpg",
            "2024-01-01",
            "image/jpeg",
            [Original, Preview],
        ),
        (
            GENERATED_JPEG_ID,
            "generated-display.jpg",
            "2024-01-02",
            "image/jpeg",
            [Display, Preview],
        ),
        (
            VIDEO_ID,
            "video-original.mp4",
            "2024-01-03",
            "video/mp4",
            [Original, Playback],
        ),
    ] {
        let resources = representations
            .into_iter()
            .map(|representation| Resource {
                uri: media(asset, representation),
                mime: mime.into(),
                duration_ms: (asset == VIDEO_ID && representation == Original).then_some(30_000),
                byte_seek: asset == VIDEO_ID,
            })
            .collect();

        objects.push(Object {
            kind: if asset == VIDEO_ID {
                ObjectKind::Video {
                    album,
                    asset: asset.parse().unwrap(),
                    resources,
                }
            } else {
                ObjectKind::Photo {
                    album,
                    asset: asset.parse().unwrap(),
                    resources,
                }
            },
            title: title.into(),
            date: Some(date.parse().unwrap()),
            art: (mime == "image/jpeg").then(|| media(asset, Preview)),
        });
    }

    objects.push(Object {
        title: "Playback Only - H264 AAC.mp4".into(),
        date: Some("2024-01-03".parse().unwrap()),
        art: None,
        kind: ObjectKind::Video {
            album,
            asset: "20000000-0000-4000-8000-000000000008".parse().unwrap(),
            resources: vec![Resource {
                uri: media(VIDEO_ID, Playback),
                mime: "video/mp4".into(),
                duration_ms: None,
                byte_seek: true,
            }],
        },
    });

    objects
}
