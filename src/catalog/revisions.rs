use std::collections::BTreeMap;

use anyhow::Context;
use uuid::Uuid;

use super::{Digest, snapshots::Root};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Ledger {
    pub(super) system_update_id: u32,
    pub(super) root_digest: Option<Digest>,
    pub(super) albums: BTreeMap<Uuid, AlbumRevision>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct AlbumRevision {
    pub(super) update_id: u32,
    pub(super) metadata_digest: Digest,
    pub(super) contents_digest: Option<Digest>,
}

impl Ledger {
    pub(super) fn new(seed: u32) -> Self {
        Self {
            system_update_id: seed,
            root_digest: None,
            albums: BTreeMap::new(),
        }
    }

    /// Stage reconciliation from a complete, bounded root before publication.
    pub(super) fn root_transition(&self, root: &Root) -> Option<Self> {
        // The canonical digest covers root properties, membership, and album metadata.
        if self.root_digest == Some(root.digest) {
            return None;
        }

        let update_id = self.system_update_id.wrapping_add(1);
        let mut next = BTreeMap::new();

        for (&id, metadata) in &root.albums {
            let metadata_digest = metadata.digest;

            let revision = match self.albums.get(&id).copied() {
                Some(mut album) => {
                    if metadata_digest != album.metadata_digest {
                        album.update_id = album.update_id.wrapping_add(1);
                        album.metadata_digest = metadata_digest;
                    }

                    album
                }

                None => AlbumRevision {
                    update_id,
                    metadata_digest,
                    contents_digest: None,
                },
            };

            next.insert(id, revision);
        }

        Some(Self {
            system_update_id: update_id,
            root_digest: Some(root.digest),
            albums: next,
        })
    }

    pub(super) fn update_contents(&mut self, id: Uuid, digest: &Digest) -> anyhow::Result<bool> {
        let album = self
            .albums
            .get_mut(&id)
            .context("contents transition requires a present album")?;

        if album.contents_digest.as_ref() == Some(digest) {
            return Ok(false);
        }

        album.update_id = album.update_id.wrapping_add(1);
        album.contents_digest = Some(*digest);
        self.system_update_id = self.system_update_id.wrapping_add(1);

        Ok(true)
    }
}

#[cfg(test)]
#[path = "revision_tests.rs"]
mod tests;
