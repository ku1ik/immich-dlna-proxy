//! Coalesced client activity; media lifetime is reported synchronously, even on drop.

use std::time::Duration;

use tokio::{sync::watch, time::Instant};

pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Shared activity reporter supplied to the HTTP server and media proxy.
#[derive(Clone)]
pub struct Activity {
    state: watch::Sender<Snapshot>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Snapshot {
    pub(crate) last: Option<Instant>,
    pub(crate) media: usize,
}

impl Snapshot {
    pub(crate) fn active(self, now: Instant) -> bool {
        self.media > 0 || self.last.is_some_and(|last| now < last + IDLE_TIMEOUT)
    }

    pub(crate) fn idle_deadline(self) -> Option<Instant> {
        self.last
            .filter(|_| self.media == 0)
            .map(|last| last + IDLE_TIMEOUT)
    }
}

impl Default for Activity {
    fn default() -> Self {
        Self {
            state: watch::channel(Snapshot::default()).0,
        }
    }
}

impl Activity {
    pub(crate) fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.state.subscribe()
    }

    pub(crate) fn touch(&self) {
        self.state
            .send_modify(|state| state.last = Some(Instant::now()));
    }

    pub(crate) fn media(&self) -> MediaActivity {
        self.state.send_modify(|state| {
            state.last = Some(Instant::now());
            state.media += 1;
        });

        MediaActivity(self.clone())
    }
}

pub(crate) struct MediaActivity(Activity);

impl Drop for MediaActivity {
    fn drop(&mut self) {
        self.0.state.send_modify(|state| {
            state.media -= 1;
            state.last = Some(Instant::now());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn coalesced_media_lifetimes_keep_activity_until_final_release_and_grace() {
        let activity = Activity::default();
        let mut state = activity.subscribe();
        assert!(!state.borrow().active(Instant::now()));
        let first = activity.media();
        let second = activity.media();
        activity.touch();
        tokio::time::advance(IDLE_TIMEOUT * 2).await;
        assert!(state.borrow().active(Instant::now()));
        assert_eq!(state.borrow().idle_deadline(), None);
        drop(first);
        assert_eq!(state.borrow().media, 1);
        drop(second);
        state.changed().await.unwrap();
        assert_eq!(state.borrow().media, 0);

        assert_eq!(
            state.borrow().idle_deadline(),
            Some(Instant::now() + IDLE_TIMEOUT)
        );

        tokio::time::advance(IDLE_TIMEOUT).await;
        assert!(!state.borrow().active(Instant::now()));
    }
}
