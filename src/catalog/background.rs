use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

use super::{Cache, Scope};
use crate::activity::Snapshot;

pub(super) const INTERVAL: Duration = Duration::from_secs(2 * 60);

#[derive(Default)]
pub(super) enum Background {
    #[default]
    Inactive,
    Waiting(Instant),
    Pass(VecDeque<Scope>),
}

impl Background {
    pub(super) fn update(&mut self, activity: Snapshot, cache: &Cache, now: Instant) {
        if !activity.active(now) {
            *self = Self::Inactive;

            return;
        }

        match self {
            Self::Inactive => *self = Self::Waiting(now + INTERVAL),

            Self::Waiting(due) if *due <= now => {
                let scopes = std::iter::once(Scope::Root)
                    .chain(cache.albums.keys().copied().map(Scope::Album))
                    .collect();

                *self = Self::Pass(scopes);
            }

            _ => {}
        }
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        match self {
            Self::Waiting(due) => Some(*due),
            _ => None,
        }
    }

    pub(super) fn next(&mut self, cache: &Cache, now: Instant) -> Option<Scope> {
        let Self::Pass(scopes) = self else {
            return None;
        };

        while let Some(&scope) = scopes.front() {
            let resident = match scope {
                Scope::Root => true,
                Scope::Album(id) => cache.albums.contains_key(&id),
            };

            if resident && !cache.is_fresh(scope, now) {
                return Some(scope);
            }

            scopes.pop_front();
        }

        *self = Self::Waiting(now + INTERVAL);

        None
    }

    pub(super) fn completed(&mut self, scope: Scope, success: bool, now: Instant) {
        let Self::Pass(scopes) = self else {
            return;
        };

        // Foreground and background attempts consume the same pass target once.
        if !scopes.contains(&scope) {
            return;
        }

        scopes.retain(|target| *target != scope);

        if scope == Scope::Root && !success {
            scopes.clear();
        }

        if scopes.is_empty() {
            *self = Self::Waiting(now + INTERVAL);
        }
    }
}
