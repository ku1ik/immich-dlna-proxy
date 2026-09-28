use std::collections::BTreeMap;

use anyhow::{Context, ensure};
use uuid::Uuid;

use super::{MAX_ALBUMS, digest::Digest};

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

    /// Build a transition from current state without changing it on failure.
    pub(super) fn root_transition(
        &self,
        root_digest: &Digest,
        albums: &BTreeMap<Uuid, Digest>,
    ) -> anyhow::Result<Option<Self>> {
        ensure!(
            albums.len() <= MAX_ALBUMS,
            "root exceeds 4096 present albums"
        );

        let mut next = self.clone();
        next.albums.retain(|id, _| albums.contains_key(id));

        let mut changed = self.root_digest.as_ref() != Some(root_digest)
            || next.albums.len() != self.albums.len();

        let update_id = self.system_update_id.wrapping_add(1);

        for (id, album) in &mut next.albums {
            let metadata = albums[id];

            if metadata != album.metadata_digest {
                album.update_id = album.update_id.wrapping_add(1);
                album.metadata_digest = metadata;
                changed = true;
            }
        }

        for (id, metadata_digest) in albums {
            if !next.albums.contains_key(id) {
                next.albums.insert(
                    *id,
                    AlbumRevision {
                        update_id,
                        metadata_digest: *metadata_digest,
                        contents_digest: None,
                    },
                );

                changed = true;
            }
        }

        if !changed {
            return Ok(None);
        }

        next.root_digest = Some(*root_digest);
        next.system_update_id = update_id;

        Ok(Some(next))
    }

    pub(super) fn contents_transition(
        &self,
        id: Uuid,
        digest: &Digest,
    ) -> anyhow::Result<Option<Self>> {
        let album = self.albums.get(&id);
        let mut album = *album.context("contents transition requires a present album")?;

        if album.contents_digest.as_ref() == Some(digest) {
            return Ok(None);
        }

        album.update_id = album.update_id.wrapping_add(1);
        album.contents_digest = Some(*digest);
        let mut next = self.clone();
        next.albums.insert(id, album);
        next.system_update_id = next.system_update_id.wrapping_add(1);

        Ok(Some(next))
    }
}

#[cfg(test)]
#[path = "revision_tests.rs"]
mod tests;
