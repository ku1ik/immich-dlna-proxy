use super::*;
use crate::{activity::IDLE_TIMEOUT, catalog::background::INTERVAL};

// Keep paused time explicit while loopback I/O is pending.
pub(super) struct Clock(JoinHandle<()>);

impl Clock {
    pub(super) fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}

impl Drop for Clock {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn wait(fixture: &Fixture, ready: fn(&CatalogTask) -> bool) {
    let started = std::time::Instant::now();

    while !fixture.library.inspect(ready).await {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "catalog did not settle"
        );

        tokio::task::yield_now().await;
    }
}

pub(super) async fn finished(fixture: &Fixture) {
    wait(fixture, |task| {
        matches!(task.background, Background::Waiting(_))
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn activity_arms_without_postponing_and_passes_use_completion_based_delay() {
    let _clock = Clock::new();
    let mut fixture = Fixture::new(0).await;
    let task = fixture.run();
    tokio::time::advance(INTERVAL * 5).await;
    assert_eq!(fixture.library.system_update_id().await, 1);
    assert!(fixture.fake.upstream.lock().unwrap().requests.is_empty());
    let events = fixture.run_events();
    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(1).await;

    let due = fixture
        .library
        .inspect(|task| task.background.deadline().unwrap())
        .await;

    tokio::time::advance(INTERVAL / 2).await;
    fixture.library.activity.touch();

    assert_eq!(
        fixture
            .library
            .inspect(|task| task.background.deadline())
            .await,
        Some(due)
    );

    let gate = fixture.gate(Scope::Root);
    tokio::time::advance(INTERVAL / 2).await;
    gate.entered().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    gate.release.add_permits(1);
    finished(&fixture).await;
    fixture.fake.event(2).await;

    assert_eq!(
        fixture
            .library
            .inspect(|task| task.background.deadline())
            .await,
        Some(Instant::now() + INTERVAL)
    );

    tokio::time::advance(INTERVAL - Duration::from_secs(1)).await;
    assert_eq!(fixture.library.system_update_id().await, 2);
    assert_eq!(fixture.fake.calls("/api/albums"), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    gate.entered().await;
    gate.release.add_permits(1);
    finished(&fixture).await;
    assert_eq!(fixture.library.system_update_id().await, 2);
    assert!(fixture.fake.notifications.try_recv().is_err());
    events.abort();
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn passes_observe_only_resident_contents_and_preserve_user_lru() {
    let _clock = Clock::new();
    let fixture = Fixture::new(4).await;
    let task = fixture.run();

    fixture
        .library
        .control(Control::Limits {
            albums: 2,
            bytes: CACHE_BYTES,
        })
        .await;

    for id in 1..=3 {
        fixture.library.browse(children(id)).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
    }

    let before = fixture.revisions().await;

    let used = fixture
        .library
        .inspect(|task| {
            task.state
                .cache
                .albums
                .iter()
                .map(|(id, cached)| (*id, cached.used))
                .collect::<BTreeMap<_, _>>()
        })
        .await;

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();
        upstream.albums.push(album(5, "New"));

        upstream
            .contents
            .insert(Uuid::from_u128(2), vec![item(20, None, None)]);
    }

    let gate = fixture.gate(Scope::Root);
    tokio::time::advance(INTERVAL).await;
    gate.entered().await;
    gate.release.add_permits(1);
    finished(&fixture).await;
    let after = fixture.revisions().await;
    assert_eq!(after.system_update_id, before.system_update_id + 2);

    assert_eq!(
        after.albums[&Uuid::from_u128(1)],
        before.albums[&Uuid::from_u128(1)]
    );

    assert!(after.albums[&Uuid::from_u128(4)].contents_digest.is_none());
    assert!(after.albums[&Uuid::from_u128(5)].contents_digest.is_none());
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 5);

    fixture
        .library
        .inspect(move |task| {
            assert_eq!(task.state.cache.albums.len(), used.len());

            for (id, cached) in &task.state.cache.albums {
                assert_eq!(cached.used, used[id]);
            }
        })
        .await;

    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn foreground_wins_between_scopes_and_evicted_and_shared_targets_are_not_repeated() {
    let _clock = Clock::new();
    let fixture = Fixture::new(4).await;
    let task = fixture.run();

    fixture
        .library
        .control(Control::Limits {
            albums: 2,
            bytes: CACHE_BYTES,
        })
        .await;

    fixture.library.browse(children(2)).await.unwrap();
    fixture.library.browse(children(3)).await.unwrap();
    let root = fixture.gate(Scope::Root);
    let fourth = fixture.gate(Scope::Album(Uuid::from_u128(4)));
    let third = fixture.gate(Scope::Album(Uuid::from_u128(3)));
    tokio::time::advance(INTERVAL).await;
    root.entered().await;

    let request = fixture
        .library
        .enqueue(children(4), Instant::now() + REFRESH_TIMEOUT);

    fixture
        .library
        .inspect(|task| assert_eq!(task.pending.len(), 1))
        .await;

    root.release.add_permits(1);
    fourth.entered().await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 3);
    fourth.release.add_permits(1);
    assert!(request.await.unwrap().is_ok());
    third.entered().await;

    let joined = fixture
        .library
        .enqueue(children(3), Instant::now() + REFRESH_TIMEOUT);

    let first = fixture
        .library
        .enqueue(children(1), Instant::now() + REFRESH_TIMEOUT);

    fixture
        .library
        .inspect(|task| assert_eq!(task.pending.len(), 2))
        .await;

    third.release.add_permits(1);
    assert!(first.await.unwrap().is_ok());
    assert!(joined.await.unwrap().is_ok());
    finished(&fixture).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 5);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn root_failure_ends_pass_album_failure_is_isolated_and_shared_failure_is_not_retried() {
    let _clock = Clock::new();
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    let before = fixture.revisions().await;
    fixture.fake.upstream.lock().unwrap().outage = true;
    let root = fixture.gate(Scope::Root);
    tokio::time::advance(INTERVAL).await;
    root.entered().await;

    let joined = fixture
        .library
        .enqueue(children(1), Instant::now() + REFRESH_TIMEOUT);

    fixture
        .library
        .inspect(|task| assert_eq!(task.pending.len(), 1))
        .await;

    root.release.add_permits(1);
    assert_eq!(joined.await.unwrap().err(), Some(FAILED));
    finished(&fixture).await;
    assert_eq!(fixture.revisions().await, before);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);

    {
        let mut upstream = fixture.fake.upstream.lock().unwrap();
        upstream.outage = false;

        upstream
            .contents
            .insert(Uuid::from_u128(1), vec![json!({"id": Uuid::from_u128(1)})]);

        upstream
            .contents
            .insert(Uuid::from_u128(2), vec![item(2, None, None)]);
    }

    tokio::time::advance(INTERVAL).await;
    root.entered().await;
    root.release.add_permits(1);
    finished(&fixture).await;
    let after = fixture.revisions().await;
    assert_eq!(after.system_update_id, before.system_update_id + 1);

    assert_eq!(
        after.albums[&Uuid::from_u128(1)],
        before.albums[&Uuid::from_u128(1)]
    );

    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);
    assert_eq!(fixture.fake.calls("/api/albums"), 3);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn inactivity_discards_unstarted_targets_but_finishes_the_active_scope() {
    let _clock = Clock::new();
    let fixture = Fixture::new(1).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    let root = fixture.gate(Scope::Root);
    tokio::time::advance(IDLE_TIMEOUT - Duration::from_secs(10)).await;
    root.entered().await;
    tokio::time::advance(Duration::from_secs(10)).await;

    wait(&fixture, |task| {
        matches!(task.background, Background::Inactive)
    })
    .await;

    root.release.add_permits(1);

    wait(&fixture, |task| {
        task.state.cache.is_fresh(Scope::Root, Instant::now())
    })
    .await;

    assert_eq!(fixture.fake.calls("/api/search/metadata"), 1);
    tokio::time::advance(INTERVAL * 3).await;
    assert_eq!(fixture.library.system_update_id().await, 3);
    assert_eq!(fixture.fake.calls("/api/albums"), 2);
    fixture.library.activity.touch();
    finished(&fixture).await;
    tokio::time::advance(INTERVAL - Duration::from_secs(1)).await;
    assert_eq!(fixture.library.system_update_id().await, 3);
    assert_eq!(fixture.fake.calls("/api/albums"), 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    root.entered().await;
    root.release.add_permits(1);
    finished(&fixture).await;
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 2);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn regular_renewal_activity_and_open_media_keep_polling_long_renewal_gaps_do_not() {
    let _clock = Clock::new();
    let fixture = Fixture::new(0).await;
    let task = fixture.run();
    let root = fixture.gate(Scope::Root);
    fixture.library.activity.touch();
    finished(&fixture).await;

    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(210)).await;
        fixture.library.activity.touch();
        root.entered().await;
        root.release.add_permits(1);
        finished(&fixture).await;
    }

    assert_eq!(fixture.fake.calls("/api/albums"), 5);
    tokio::time::advance(IDLE_TIMEOUT).await;

    wait(&fixture, |task| {
        matches!(task.background, Background::Inactive)
    })
    .await;

    assert_eq!(fixture.fake.calls("/api/albums"), 5);
    let media = fixture.library.activity.media();
    finished(&fixture).await;
    tokio::time::advance(IDLE_TIMEOUT * 2).await;
    root.entered().await;
    root.release.add_permits(1);
    finished(&fixture).await;
    drop(media);
    fixture.library.inspect(|_| ()).await;
    tokio::time::advance(IDLE_TIMEOUT).await;

    wait(&fixture, |task| {
        matches!(task.background, Background::Inactive)
    })
    .await;

    assert_eq!(fixture.fake.calls("/api/albums"), 6);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn a_pass_skips_root_and_contents_recently_refreshed_by_browse() {
    let _clock = Clock::new();
    let fixture = Fixture::new(2).await;
    let task = fixture.run();
    fixture.library.browse(children(1)).await.unwrap();
    fixture.library.browse(children(2)).await.unwrap();
    tokio::time::advance(Duration::from_secs(100)).await;
    fixture.library.browse(children(1)).await.unwrap();
    let before = fixture.revisions().await;
    let second = fixture.gate(Scope::Album(Uuid::from_u128(2)));
    tokio::time::advance(Duration::from_secs(20)).await;
    second.entered().await;
    assert_eq!(fixture.fake.calls("/api/albums"), 2);
    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);
    second.release.add_permits(1);
    finished(&fixture).await;
    assert_eq!(fixture.revisions().await, before);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn growing_background_payload_can_evict_itself_without_promoting_its_recency() {
    let _clock = Clock::new();
    let fixture = Fixture::new(2).await;
    let task = fixture.run();

    for id in 1..=2 {
        fixture
            .fake
            .upstream
            .lock()
            .unwrap()
            .contents
            .insert(Uuid::from_u128(id), vec![item(id, None, None)]);

        fixture.library.browse(children(id)).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
    }

    let before = fixture.revisions().await;

    let bytes = fixture
        .library
        .inspect(|task| {
            task.state.cache.root.as_ref().unwrap().snapshot.bytes
                + task
                    .state
                    .cache
                    .albums
                    .values()
                    .map(|cached| cached.snapshot.bytes)
                    .sum::<usize>()
        })
        .await;

    fixture
        .library
        .control(Control::Limits {
            albums: RESIDENT_ALBUMS,
            bytes,
        })
        .await;

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .get_mut(&Uuid::from_u128(1))
        .unwrap()[0]["originalFileName"] = json!("x".repeat(100));

    let root = fixture.gate(Scope::Root);
    tokio::time::advance(INTERVAL).await;
    root.entered().await;
    root.release.add_permits(1);
    finished(&fixture).await;

    assert_eq!(
        fixture.revisions().await.system_update_id,
        before.system_update_id + 1
    );

    fixture
        .library
        .inspect(|task| {
            assert!(!task.state.cache.albums.contains_key(&Uuid::from_u128(1)));
            assert!(task.state.cache.albums.contains_key(&Uuid::from_u128(2)));

            assert!(
                task.state.cache.root.as_ref().unwrap().snapshot.bytes
                    + task
                        .state
                        .cache
                        .albums
                        .values()
                        .map(|cached| cached.snapshot.bytes)
                        .sum::<usize>()
                    <= task.state.cache.byte_limit
            );
        })
        .await;

    assert_eq!(fixture.fake.calls("/api/search/metadata"), 4);
    fixture.abort(task).await;
}

#[tokio::test(start_paused = true)]
async fn ready_reply_is_pinned_before_ordinary_lru_can_evict_its_payload() {
    let _clock = Clock::new();
    let fixture = Fixture::new(2).await;
    let task = fixture.run();

    fixture
        .library
        .control(Control::Limits {
            albums: 1,
            bytes: CACHE_BYTES,
        })
        .await;
    fixture.library.browse(children(2)).await.unwrap();

    // Equal access times make UUID the LRU tie-breaker. Album 1 is evicted
    // immediately, even though its publication satisfies a waiting Browse.
    let view = fixture
        .library
        .enqueue(children(1), Instant::now() + REFRESH_TIMEOUT)
        .await
        .unwrap()
        .unwrap();

    fixture
        .library
        .inspect(|task| {
            assert!(!task.state.cache.albums.contains_key(&Uuid::from_u128(1)));
            assert!(task.state.cache.albums.contains_key(&Uuid::from_u128(2)));
        })
        .await;

    assert_eq!(
        view.browse(&fixture.library.catalog.collator)
            .unwrap()
            .total_matches,
        0
    );
    fixture.abort(task).await;
}
