use super::*;
use axum::{Router, body::to_bytes};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};

const PEER: Ipv4Addr = Ipv4Addr::new(192, 168, 1, 20);
const SERVICE: Service = Service::ContentDirectory;

fn headers(values: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();

    for (name, value) in values {
        headers.append(
            header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }

    headers
}

fn response_sid(response: &Response) -> Uuid {
    response.headers()["sid"]
        .to_str()
        .unwrap()
        .strip_prefix("uuid:")
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap()
}

fn subscribe(subscriptions: &Subscriptions, lease: Option<&str>) -> (Response, Uuid) {
    let mut headers = headers(&[
        ("nt", "upnp:event"),
        ("callback", "<http://192.168.1.20/events>"),
        ("content-length", "0"),
    ]);

    if let Some(lease) = lease {
        headers.insert("timeout", lease.parse().unwrap());
    }

    let response = subscriptions.request(
        SERVICE,
        PEER,
        &Method::from_bytes(b"SUBSCRIBE").unwrap(),
        &headers,
    );

    assert_eq!(response.status(), StatusCode::OK);
    let sid = response_sid(&response);

    (response, sid)
}

fn sid_request(
    subscriptions: &Subscriptions,
    sid: Uuid,
    method: &str,
    service: Service,
    peer: Ipv4Addr,
    timeout: Option<&str>,
) -> Response {
    let mut headers = headers(&[("sid", &format!("uuid:{sid}"))]);

    if let Some(timeout) = timeout {
        headers.insert("timeout", timeout.parse().unwrap());
    }

    subscriptions.request(
        service,
        peer,
        &Method::from_bytes(method.as_bytes()).unwrap(),
        &headers,
    )
}

#[tokio::test(start_paused = true)]
async fn leases_are_bounded_and_responses_are_framed() {
    for (requested, expected) in [
        (None, 1800),
        (Some("Second-infinite"), 1800),
        (Some("Second-9999"), 1800),
        (Some("Second-1"), 1),
        (Some("Second-0012"), 12),
    ] {
        let subscriptions = Subscriptions::new().unwrap();
        subscriptions.publish(42);
        let (response, sid) = subscribe(&subscriptions, requested);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "0");
        assert!(!response.headers().contains_key(header::SERVER));
        assert!(!response.headers().contains_key(header::CONNECTION));
        assert_eq!(response.headers()["sid"], format!("uuid:{sid}"));
        assert_eq!(response.headers()["timeout"], format!("Second-{expected}"));
        assert!(to_bytes(response.into_body(), 0).await.unwrap().is_empty());

        let state = subscriptions.state.lock().unwrap();
        assert_eq!(state.entries.len(), 1);

        assert_eq!(
            state.entries[0].expires,
            Some(Instant::now() + Duration::from_secs(expected))
        );

        assert_eq!(state.entries[0].initial_update_id, Some(42));
    }
}

#[test]
fn publication_and_registration_share_capture_without_old_replay() {
    let subscriptions = Subscriptions::new().unwrap();
    let (_, first) = subscribe(&subscriptions, None);
    subscriptions.publish(u32::MAX);
    let (_, second) = subscribe(&subscriptions, None);
    subscriptions.publish(u32::MAX);

    {
        let state = subscriptions.state.lock().unwrap();

        let first = state
            .entries
            .iter()
            .find(|entry| entry.sid == first)
            .unwrap();

        let second = state
            .entries
            .iter()
            .find(|entry| entry.sid == second)
            .unwrap();
        assert_eq!(first.initial_update_id, Some(0));
        assert_eq!(first.pending, Some(u32::MAX));
        assert_eq!(second.initial_update_id, Some(u32::MAX));
        assert_eq!(second.pending, None);
    }

    subscriptions.publish(0);
    let (_, third) = subscribe(&subscriptions, None);
    let state = subscriptions.state.lock().unwrap();
    assert_eq!(state.system_update_id, 0);

    for entry in &state.entries {
        assert_eq!(entry.pending, (entry.sid != third).then_some(0));
    }
}

#[test]
fn concurrent_registration_captures_either_side_of_publication_atomically() {
    for _ in 0..64 {
        let subscriptions = Subscriptions::new().unwrap();
        subscriptions.publish(7);
        let barrier = std::sync::Barrier::new(2);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                subscriptions.publish(8);
            });

            barrier.wait();

            subscribe(&subscriptions, None);
        });

        {
            let state = subscriptions.state.lock().unwrap();
            let entry = &state.entries[0];

            assert!(matches!(
                (entry.initial_update_id, entry.pending),
                (Some(7), Some(8)) | (Some(8), None)
            ));
        }
    }
}

#[test]
fn malformed_headers_never_reserve_capacity() {
    let subscriptions = Subscriptions::new().unwrap();

    let cases = [
        ("SUBSCRIBE", vec![], StatusCode::PRECONDITION_FAILED),
        (
            "SUBSCRIBE",
            vec![("nt", "wrong"), ("callback", "<http://192.168.1.20/>")],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "SUBSCRIBE",
            vec![("nt", "upnp:event")],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "SUBSCRIBE",
            vec![("sid", "bad")],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "SUBSCRIBE",
            vec![("sid", "bad"), ("nt", "upnp:event")],
            StatusCode::BAD_REQUEST,
        ),
        ("UNSUBSCRIBE", vec![], StatusCode::BAD_REQUEST),
        (
            "UNSUBSCRIBE",
            vec![("sid", "bad"), ("timeout", "Second-5")],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            "UNSUBSCRIBE",
            vec![("sid", "bad"), ("callback", "<http://192.168.1.20/>")],
            StatusCode::BAD_REQUEST,
        ),
        ("GET", vec![], StatusCode::METHOD_NOT_ALLOWED),
    ];

    for (method, values, expected) in cases {
        let response = subscriptions.request(
            SERVICE,
            PEER,
            &Method::from_bytes(method.as_bytes()).unwrap(),
            &headers(&values),
        );

        assert_eq!(response.status(), expected, "{method} {values:?}");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "0");
        assert!(!response.headers().contains_key(header::CONNECTION));
        assert!(!response.headers().contains_key(header::SERVER));
    }

    for value in [
        "",
        "Second-0",
        "Second--1",
        "Second-+1",
        "Second-1.5",
        "second-1",
        "infinite",
        "Second-18446744073709551616",
    ] {
        let values = headers(&[
            ("nt", "upnp:event"),
            ("callback", "<http://192.168.1.20/>"),
            ("timeout", value),
        ]);

        let response = subscriptions.request(
            SERVICE,
            PEER,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &values,
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{value}");
    }

    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
}

#[test]
fn subscription_capacity_is_bounded() {
    let subscriptions = Subscriptions::new().unwrap();

    for _ in 0..SUBSCRIPTIONS {
        subscribe(&subscriptions, None);
    }

    let response = subscriptions.request(
        SERVICE,
        PEER,
        &Method::from_bytes(b"SUBSCRIBE").unwrap(),
        &headers(&[("nt", "upnp:event"), ("callback", "<http://192.168.1.20/>")]),
    );

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    assert_eq!(
        subscriptions.state.lock().unwrap().entries.len(),
        SUBSCRIPTIONS
    );
}

#[test]
fn duplicate_event_headers_are_rejected_before_admission() {
    let subscriptions = Subscriptions::new().unwrap();
    let sid = "uuid:00000000-0000-0000-0000-000000000001";

    for values in [
        &[("nt", "upnp:event"), ("nt", "upnp:event")][..],
        &[
            ("nt", "upnp:event"),
            ("callback", "<http://192.168.1.20/>"),
            ("timeout", "Second-1"),
            ("timeout", "Second-1"),
        ],
        &[
            ("nt", "upnp:event"),
            ("callback", "<http://192.168.1.20/>"),
            ("callback", "<http://192.168.1.20/>"),
        ],
        &[("sid", sid), ("sid", sid)],
    ] {
        let response = subscriptions.request(
            SERVICE,
            PEER,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers(values),
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{values:?}");
    }

    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
}

#[test]
fn callback_authorities_are_literal_same_peer_and_entire_list_is_validated() {
    let urls = callbacks(
        " <http://192.168.1.20/path?q=a%26b><HTTP://192.168.1.20:1234/other> ",
        PEER,
    )
    .unwrap();

    assert_eq!(urls.len(), 2);
    assert_eq!(urls[0].path(), "/path");
    assert_eq!(urls[0].query(), Some("q=a%26b"));
    assert_eq!(urls[1].port(), Some(1234));

    assert_eq!(
        callbacks(&"<http://192.168.1.20/>".repeat(4), PEER)
            .unwrap()
            .len(),
        4
    );

    assert!(callbacks(&"<http://192.168.1.20/>".repeat(5), PEER).is_none());

    for bad in [
        "",
        "http://192.168.1.20/",
        "<http://192.168.1.20/",
        "<http://192.168.1.20/>garbage",
        "<https://192.168.1.20/>",
        "<http://192.168.1.21/>",
        "<http://localhost/>",
        "<http://3232235796/>",
        "<http://0xc0a80114/>",
        "<http://192.168.276/>",
        "<http://192.168.001.20/>",
        "<http://192%2e168.1.20/>",
        "<http://[::ffff:c0a8:114]/>",
        "<http://user@192.168.1.20/>",
        "<http://@192.168.1.20/>",
        "<http://192.168.1.20/#frag>",
        "<http://192.168.1.20:0/>",
        "<http://192.168.1.20:65536/>",
        "<http://192.168.1.20:/>",
        "<http://192.168.1.20:+80/>",
        "<http://192.168.1.20/a b>",
        "<http://192.168.1.20\\@evil/>",
        "<http://192.168.1.20/> <http://elsewhere/>",
        "<http://192.168.1.20/a\nb>",
    ] {
        assert!(callbacks(bad, PEER).is_none(), "{bad}");
    }

    for peer in [
        Ipv4Addr::UNSPECIFIED,
        Ipv4Addr::BROADCAST,
        Ipv4Addr::new(0, 1, 2, 3),
        Ipv4Addr::new(224, 1, 2, 3),
        Ipv4Addr::new(240, 1, 2, 3),
    ] {
        assert!(callbacks(&format!("<http://{peer}/>"), peer).is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn renewal_and_unsubscribe_require_live_same_peer_same_service_sid() {
    let subscriptions = Subscriptions::new().unwrap();
    subscriptions.publish(42);
    let (_, sid) = subscribe(&subscriptions, Some("Second-10"));

    for method in ["SUBSCRIBE", "UNSUBSCRIBE"] {
        for (service, peer, requested_sid) in [
            (Service::ConnectionManager, PEER, sid),
            (SERVICE, Ipv4Addr::new(192, 168, 1, 21), sid),
            (SERVICE, PEER, Uuid::new_v4()),
        ] {
            assert_eq!(
                sid_request(&subscriptions, requested_sid, method, service, peer, None).status(),
                StatusCode::PRECONDITION_FAILED
            );
        }
    }

    tokio::time::advance(Duration::from_secs(9)).await;

    let response = sid_request(
        &subscriptions,
        sid,
        "SUBSCRIBE",
        SERVICE,
        PEER,
        Some("Second-20"),
    );

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["sid"], format!("uuid:{sid}"));
    assert_eq!(response.headers()["timeout"], "Second-20");

    {
        let state = subscriptions.state.lock().unwrap();
        assert_eq!(state.entries[0].initial_update_id, Some(42));

        assert_eq!(
            state.entries[0].expires,
            Some(Instant::now() + Duration::from_secs(20))
        );
    }

    tokio::time::advance(Duration::from_secs(20)).await;

    assert_eq!(
        sid_request(&subscriptions, sid, "SUBSCRIBE", SERVICE, PEER, None).status(),
        StatusCode::PRECONDITION_FAILED
    );

    assert_eq!(
        sid_request(&subscriptions, sid, "UNSUBSCRIBE", SERVICE, PEER, None).status(),
        StatusCode::PRECONDITION_FAILED
    );

    assert_eq!(subscriptions.state.lock().unwrap().entries.len(), 1);
}

struct Callback {
    port: u16,
    received: mpsc::Receiver<(http::request::Parts, String)>,
    release: Arc<tokio::sync::Semaphore>,
    task: JoinHandle<()>,
}

impl Drop for Callback {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Callback {
    async fn new() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sender, received) = mpsc::channel(64);
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let gate = release.clone();

        let app = Router::new().fallback(move |request: axum::extract::Request| {
            let sender = sender.clone();
            let gate = gate.clone();

            async move {
                let (parts, body) = request.into_parts();
                let path = parts.uri.path().to_owned();
                let body = to_bytes(body, CALLBACK_BODY_BYTES).await.unwrap();

                sender
                    .send((parts, String::from_utf8(body.to_vec()).unwrap()))
                    .await
                    .unwrap();

                match path.as_str() {
                    "/gated" | "/gated-fail" => {
                        gate.acquire().await.unwrap().forget();

                        Response::builder()
                            .status(if path == "/gated-fail" { 500 } else { 200 })
                            .body(Body::empty())
                            .unwrap()
                    }

                    "/reject" => Response::builder().status(412).body(Body::empty()).unwrap(),
                    "/fail" => Response::builder().status(500).body(Body::empty()).unwrap(),

                    "/redirect" => Response::builder()
                        .status(302)
                        .header("location", "/forbidden")
                        .body(Body::empty())
                        .unwrap(),

                    "/oversized" => Response::new(Body::from(vec![b'x'; CALLBACK_BODY_BYTES + 1])),

                    "/chunked" => Response::new(Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::io::Error>(vec![b'x'; CALLBACK_BODY_BYTES]),
                        Ok(vec![b'x']),
                    ]))),

                    "/slow-headers" => std::future::pending::<Response>().await,

                    _ => Response::new(Body::empty()),
                }
            }
        });

        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            port,
            received,
            release,
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("<http://127.0.0.1:{}{path}>", self.port)
    }

    async fn next(&mut self) -> (http::request::Parts, String) {
        tokio::time::timeout(Duration::from_secs(3), self.received.recv())
            .await
            .unwrap()
            .unwrap()
    }

    fn register(&self, subscriptions: &Subscriptions, service: Service, paths: &[&str]) -> Uuid {
        let callbacks: String = paths.iter().map(|path| self.url(path)).collect();

        let response = subscriptions.request(
            service,
            Ipv4Addr::LOCALHOST,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers(&[
                ("nt", "upnp:event"),
                ("callback", &callbacks),
                ("cookie", "private"),
                ("authorization", "private"),
            ]),
        );

        assert_eq!(response.status(), StatusCode::OK);

        response_sid(&response)
    }
}

async fn abort_scheduler(task: JoinHandle<anyhow::Result<()>>) {
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

async fn finished(subscriptions: &Subscriptions, sid: Uuid) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if subscriptions
                .state
                .lock()
                .unwrap()
                .entries
                .iter()
                .all(|entry| {
                    entry.sid != sid || (entry.initial_update_id.is_none() && !entry.delivering)
                })
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn initial_event_is_immediately_eligible_and_renewal_does_not_replay() {
    let subscriptions = Subscriptions::new().unwrap();
    subscriptions.publish(17);
    let mut callback = Callback::new().await;
    let sid = callback.register(&subscriptions, SERVICE, &["/event?q=a%26b"]);
    let task = tokio::spawn(subscriptions.clone().run());
    let (request, body) = callback.next().await;
    assert_eq!(request.method.as_str(), "NOTIFY");
    assert_eq!(request.uri.to_string(), "/event?q=a%26b");
    assert_eq!(request.headers["nt"], "upnp:event");
    assert_eq!(request.headers["nts"], "upnp:propchange");
    assert_eq!(request.headers["sid"], format!("uuid:{sid}"));
    assert_eq!(request.headers["seq"], "0");

    assert_eq!(
        request.headers[header::CONTENT_LENGTH],
        body.len().to_string()
    );

    assert_eq!(
        request.headers[header::CONTENT_TYPE],
        "text/xml; charset=\"utf-8\""
    );

    assert_eq!(request.headers[header::ACCEPT_ENCODING], "identity");

    for name in [
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-key",
    ] {
        assert!(!request.headers.contains_key(name));
    }

    assert_eq!(body, protocol::event_body(SERVICE, 17));
    finished(&subscriptions, sid).await;

    assert_eq!(
        sid_request(
            &subscriptions,
            sid,
            "SUBSCRIBE",
            SERVICE,
            Ipv4Addr::LOCALHOST,
            None
        )
        .status(),
        StatusCode::OK
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    abort_scheduler(task).await;
}

#[tokio::test]
async fn pending_during_initial_coalesces_and_final_event_flushes_on_timer() {
    let subscriptions = Subscriptions::new().unwrap();
    subscriptions.publish(10);
    let mut callback = Callback::new().await;
    let sid = callback.register(&subscriptions, SERVICE, &["/gated"]);
    subscriptions.publish(11);
    subscriptions.publish(12);
    let cm = callback.register(&subscriptions, Service::ConnectionManager, &["/cm"]);
    let task = tokio::spawn(subscriptions.clone().run());
    let mut initial = std::collections::BTreeSet::new();

    for _ in 0..2 {
        let (request, body) = callback.next().await;
        let sid_header = request.headers["sid"].to_str().unwrap();
        assert_eq!(request.headers["seq"], "0");

        if sid_header == format!("uuid:{sid}") {
            assert_eq!(body, protocol::event_body(SERVICE, 10));
            assert!(initial.insert(sid));
        } else {
            assert_eq!(sid_header, format!("uuid:{cm}"));
            assert_eq!(body, protocol::event_body(Service::ConnectionManager, 12));
            assert!(initial.insert(cm));
        }
    }

    finished(&subscriptions, cm).await;

    assert_eq!(
        subscriptions
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .find(|entry| entry.sid == sid)
            .unwrap()
            .pending,
        Some(12)
    );

    subscriptions.publish(13);
    subscriptions.publish(14);

    {
        let state = subscriptions.state.lock().unwrap();
        let entry = state.entries.iter().find(|entry| entry.sid == sid).unwrap();
        let cm = state.entries.iter().find(|entry| entry.sid == cm).unwrap();
        assert_eq!(entry.pending, Some(14));
        assert!(entry.initial_update_id.is_none());
        assert!(entry.delivering);
        assert_eq!(cm.pending, None);
    }

    tokio::time::pause();
    tokio::time::advance(MODERATION).await;
    tokio::time::resume();

    // Even an overdue change cannot overlap the still-running initial attempt.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    callback.release.add_permits(1);
    let (request, body) = callback.next().await;
    assert_eq!(request.headers["seq"], "1");
    assert_eq!(body, protocol::event_body(SERVICE, 14));
    subscriptions.publish(15);
    subscriptions.publish(16);
    callback.release.add_permits(1);
    finished(&subscriptions, sid).await;

    let deadline = subscriptions
        .state
        .lock()
        .unwrap()
        .entries
        .iter()
        .find(|entry| entry.sid == sid)
        .unwrap()
        .next_attempt;

    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    // No publication or request wakes the scheduler after this point.
    let (request, body) = callback.next().await;
    assert!(Instant::now() >= deadline);
    assert_eq!(request.headers["seq"], "2");
    assert_eq!(body, protocol::event_body(SERVICE, 16));
    callback.release.add_permits(1);
    finished(&subscriptions, sid).await;
    subscriptions.publish(16);

    {
        let state = subscriptions.state.lock().unwrap();
        let entry = state.entries.iter().find(|entry| entry.sid == sid).unwrap();
        let cm = state.entries.iter().find(|entry| entry.sid == cm).unwrap();
        assert_eq!(entry.pending, None);
        assert_eq!(entry.next_seq, 3);
        assert_eq!(cm.pending, None);
        assert_eq!(cm.next_seq, 1);
    }

    assert!(callback.received.try_recv().is_err());
    abort_scheduler(task).await;
}

#[tokio::test]
async fn failed_attempts_allocate_sequence_once_wrap_and_preserve_future_pending() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;
    let sid = callback.register(&subscriptions, SERVICE, &["/gated-fail", "/fail"]);
    let task = tokio::spawn(subscriptions.clone().run());

    for (id, seq, next_seq, future) in [(0, 0, 1, None), (8, u32::MAX, 1, Some(9)), (9, 1, 2, None)]
    {
        let (request, body) = callback.next().await;
        assert_eq!(request.uri.path(), "/gated-fail");
        assert_eq!(request.headers["seq"], seq.to_string());
        assert_eq!(body, protocol::event_body(SERVICE, id));

        if let Some(future) = future {
            subscriptions.publish(future);
        }

        callback.release.add_permits(1);
        let (alternative, alternative_body) = callback.next().await;
        assert_eq!(alternative.uri.path(), "/fail");
        assert_eq!(alternative.headers["seq"], request.headers["seq"]);
        assert_eq!(alternative.headers["sid"], request.headers["sid"]);
        assert_eq!(alternative_body, body);
        finished(&subscriptions, sid).await;

        {
            let mut state = subscriptions.state.lock().unwrap();
            let entry = &mut state.entries[0];
            assert!(entry.expires.is_some());
            assert_eq!(entry.pending, future);
            assert_eq!(entry.next_seq, next_seq);

            if seq == 0 {
                entry.next_seq = u32::MAX;
            }
        }

        if seq == 0 {
            subscriptions.publish(8);

            assert!(
                tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
                    .await
                    .is_err()
            );
        }

        tokio::time::pause();
        tokio::time::advance(MODERATION).await;
        tokio::time::resume();
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    abort_scheduler(task).await;
}

#[tokio::test]
async fn ordinary_deliveries_share_global_bound_and_coalesce_while_saturated() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;
    callback.release.add_permits(SUBSCRIPTIONS);
    let mut tokens = Vec::new();

    for _ in 0..SUBSCRIPTIONS {
        let sid = callback.register(&subscriptions, SERVICE, &["/gated"]);
        tokens.push(sid);
    }

    let task = tokio::spawn(subscriptions.clone().run());

    for _ in &tokens {
        assert_eq!(callback.next().await.0.headers["seq"], "0");
    }

    for sid in &tokens {
        finished(&subscriptions, *sid).await;
    }

    subscriptions.publish(1);
    tokio::time::pause();
    tokio::time::advance(MODERATION).await;
    tokio::time::resume();

    let mut sequences: std::collections::BTreeMap<_, u32> = tokens
        .iter()
        .map(|sid| (format!("uuid:{sid}"), 0))
        .collect();

    for _ in 0..DELIVERIES {
        let (request, body) = callback.next().await;
        assert_eq!(request.headers["seq"], "1");
        assert_eq!(body, protocol::event_body(SERVICE, 1));

        let seq = sequences
            .get_mut(request.headers["sid"].to_str().unwrap())
            .unwrap();

        assert_eq!(*seq, 0);
        *seq = 1;
    }

    subscriptions.publish(2);

    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    {
        let state = subscriptions.state.lock().unwrap();

        assert_eq!(
            state
                .entries
                .iter()
                .filter(|entry| entry.delivering)
                .count(),
            DELIVERIES
        );

        assert!(state.entries.iter().all(|entry| entry.pending == Some(2)));
    }

    // Every subscriber gets the coalesced change, without a global FIFO contract.
    tokio::time::pause();
    tokio::time::advance(MODERATION).await;
    tokio::time::resume();
    let mut seen = std::collections::BTreeSet::new();

    for _ in &tokens {
        callback.release.add_permits(1);
        let (request, body) = callback.next().await;
        let sid = request.headers["sid"].to_str().unwrap();
        assert!(seen.insert(sid.to_owned()));
        let seq = sequences.get_mut(sid).unwrap();
        *seq += 1;
        assert_eq!(request.headers["seq"], seq.to_string());
        assert_eq!(body, protocol::event_body(SERVICE, 2));

        assert_eq!(
            subscriptions
                .state
                .lock()
                .unwrap()
                .entries
                .iter()
                .filter(|entry| entry.delivering)
                .count(),
            DELIVERIES
        );
    }

    abort_scheduler(task).await;
}

#[tokio::test]
async fn inactive_initial_obligations_remain_eligible_behind_bounded_deliveries() {
    for unsubscribe in [false, true] {
        let subscriptions = Subscriptions::new().unwrap();
        subscriptions.publish(17);
        let mut callback = Callback::new().await;
        let mut inflight = Vec::new();

        for _ in 0..DELIVERIES {
            inflight.push(callback.register(&subscriptions, SERVICE, &["/gated"]));
        }

        let task = tokio::spawn(subscriptions.clone().run());

        let mut seen = std::collections::BTreeSet::new();

        for _ in &inflight {
            seen.insert(
                callback.next().await.0.headers["sid"]
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
        }

        assert_eq!(
            seen,
            inflight.iter().map(|sid| format!("uuid:{sid}")).collect()
        );

        let target = callback.register(&subscriptions, SERVICE, &["/gated"]);

        if unsubscribe {
            assert_eq!(
                sid_request(
                    &subscriptions,
                    target,
                    "UNSUBSCRIBE",
                    SERVICE,
                    Ipv4Addr::LOCALHOST,
                    None
                )
                .status(),
                StatusCode::OK
            );
        } else {
            let mut state = subscriptions.state.lock().unwrap();

            let entry = state
                .entries
                .iter_mut()
                .find(|entry| entry.sid == target)
                .unwrap();

            entry.expires = Some(Instant::now());
            state.expire(Instant::now());
        }

        callback.register(&subscriptions, SERVICE, &["/gated"]);
        callback.release.add_permits(1);
        let (request, body) = callback.next().await;
        assert_eq!(request.headers["sid"], format!("uuid:{target}"));
        assert_eq!(request.headers["seq"], "0");
        assert_eq!(body, protocol::event_body(SERVICE, 17));
        abort_scheduler(task).await;
    }
}

#[tokio::test]
async fn inactive_inflight_attempts_finish_but_drop_all_future_changes() {
    for initial in [false, true] {
        for unsubscribe in [false, true] {
            let subscriptions = Subscriptions::new().unwrap();
            let mut callback = Callback::new().await;
            let sid = callback.register(&subscriptions, SERVICE, &["/gated"]);
            let task = tokio::spawn(subscriptions.clone().run());
            assert_eq!(callback.next().await.0.headers["seq"], "0");

            if !initial {
                callback.release.add_permits(1);
                finished(&subscriptions, sid).await;
                subscriptions.publish(1);
                tokio::time::pause();
                tokio::time::advance(MODERATION).await;
                tokio::time::resume();
                assert_eq!(callback.next().await.0.headers["seq"], "1");
            }

            subscriptions.publish(2);

            if unsubscribe {
                assert_eq!(
                    sid_request(
                        &subscriptions,
                        sid,
                        "UNSUBSCRIBE",
                        SERVICE,
                        Ipv4Addr::LOCALHOST,
                        None
                    )
                    .status(),
                    StatusCode::OK
                );
            } else {
                subscriptions.state.lock().unwrap().entries[0].expires = Some(Instant::now());
            }

            subscriptions.publish(3);

            {
                let state = subscriptions.state.lock().unwrap();
                let entry = &state.entries[0];
                assert!(entry.expires.is_none());
                assert!(entry.delivering);
                assert_eq!(entry.pending, None);
            }

            callback.release.add_permits(1);
            finished(&subscriptions, sid).await;
            assert!(subscriptions.state.lock().unwrap().entries.is_empty());
            assert!(callback.received.try_recv().is_err());
            abort_scheduler(task).await;
        }
    }
}

#[tokio::test]
async fn alternatives_keep_identical_seq_and_body_and_do_not_follow_redirects() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;

    let sid = callback.register(
        &subscriptions,
        Service::ConnectionManager,
        &["/redirect", "/fail", "/ok", "/unused"],
    );

    let task = tokio::spawn(subscriptions.clone().run());

    for path in ["/redirect", "/fail", "/ok"] {
        let (request, body) = callback.next().await;
        assert_eq!(request.uri.path(), path);
        assert_eq!(request.headers["seq"], "0");
        assert_eq!(request.headers["sid"], format!("uuid:{sid}"));
        assert_eq!(body, protocol::event_body(Service::ConnectionManager, 0));
    }

    finished(&subscriptions, sid).await;
    assert!(callback.received.try_recv().is_err());

    assert!(
        subscriptions.state.lock().unwrap().entries[0]
            .expires
            .is_some()
    );

    abort_scheduler(task).await;
}

#[tokio::test]
async fn oversized_bodies_fall_back_and_412_removes_without_trying_alternatives() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;

    let sid = callback.register(
        &subscriptions,
        SERVICE,
        &["/oversized", "/chunked", "/reject", "/unused"],
    );

    let task = tokio::spawn(subscriptions.clone().run());

    for path in ["/oversized", "/chunked", "/reject"] {
        assert_eq!(callback.next().await.0.uri.path(), path);
    }

    finished(&subscriptions, sid).await;
    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
    assert!(callback.received.try_recv().is_err());
    abort_scheduler(task).await;
}

#[tokio::test]
async fn inactive_initials_finish_and_active_failures_retain_the_lease() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;
    let inactive = callback.register(&subscriptions, SERVICE, &["/ok"]);
    let active = callback.register(&subscriptions, SERVICE, &["/fail"]);

    assert_eq!(
        sid_request(
            &subscriptions,
            inactive,
            "UNSUBSCRIBE",
            SERVICE,
            Ipv4Addr::LOCALHOST,
            None
        )
        .status(),
        StatusCode::OK
    );

    let task = tokio::spawn(subscriptions.clone().run());
    callback.next().await;
    callback.next().await;
    finished(&subscriptions, inactive).await;
    finished(&subscriptions, active).await;

    {
        let state = subscriptions.state.lock().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.entries[0].sid, active);
        assert!(state.entries[0].expires.is_some());
    }

    assert_eq!(
        sid_request(
            &subscriptions,
            active,
            "UNSUBSCRIBE",
            SERVICE,
            Ipv4Addr::LOCALHOST,
            None
        )
        .status(),
        StatusCode::OK
    );

    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
    abort_scheduler(task).await;
}

#[tokio::test]
async fn delivery_concurrency_is_bounded_and_scheduler_is_single_run() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;

    for _ in 0..SUBSCRIPTIONS {
        let sid = callback.register(&subscriptions, SERVICE, &["/slow-headers"]);

        assert_eq!(
            sid_request(
                &subscriptions,
                sid,
                "UNSUBSCRIBE",
                SERVICE,
                Ipv4Addr::LOCALHOST,
                None
            )
            .status(),
            StatusCode::OK
        );
    }

    let task = tokio::spawn(subscriptions.clone().run());

    for _ in 0..DELIVERIES {
        callback.next().await;
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(30), callback.received.recv())
            .await
            .is_err()
    );

    {
        let state = subscriptions.state.lock().unwrap();
        assert_eq!(state.entries.len(), SUBSCRIPTIONS);

        assert_eq!(
            state
                .entries
                .iter()
                .filter(|entry| entry.delivering)
                .count(),
            DELIVERIES
        );
    }

    assert_eq!(
        subscriptions.clone().run().await.unwrap_err().to_string(),
        "eventing scheduler already started"
    );

    abort_scheduler(task).await;
    assert!(callback.received.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn stalled_callbacks_fall_back_with_same_sequence_and_keep_the_lease() {
    use std::{future::Future, task::Wake};

    use futures_util::FutureExt;

    struct CallbackWake(Notify);

    impl Wake for CallbackWake {
        fn wake(self: Arc<Self>) {
            self.0.notify_one();
        }
    }

    // Keep loopback I/O from auto-advancing the paused clock to a timeout.
    let clock_guard = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });

    let limit = CALLBACK_TIMEOUT;

    for (body_pending, status, elapsed) in [
        (false, 412, limit - Duration::from_secs(1)),
        (false, 200, limit - Duration::from_secs(1)),
        (true, 200, limit - Duration::from_secs(1)),
        (false, 200, limit),
        (true, 200, limit),
        (false, 200, limit + Duration::from_secs(1)),
        (true, 200, limit + Duration::from_secs(1)),
    ] {
        let subscriptions = Subscriptions::new().unwrap();

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut callback = Callback::new().await;

        let urls = format!(
            "<http://{}/first>{}{}",
            listener.local_addr().unwrap(),
            callback.url("/ok"),
            callback.url("/unused")
        );

        let response = subscriptions.request(
            SERVICE,
            Ipv4Addr::LOCALHOST,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers(&[("nt", "upnp:event"), ("callback", &urls)]),
        );

        assert_eq!(response.status(), StatusCode::OK);
        let sid = response_sid(&response);
        let scheduler = subscriptions.clone().run();
        tokio::pin!(scheduler);
        let wake = Arc::new(CallbackWake(Notify::new()));
        let waker = std::task::Waker::from(wake.clone());
        let mut context = std::task::Context::from_waker(&waker);

        for (seq, id) in [(0, 0), (1, 7)] {
            if seq != 0 {
                subscriptions.publish(id);
            }

            let started = Instant::now();

            let socket = tokio::select! {
                _ = &mut scheduler => panic!("scheduler ended"),
                accepted = listener.accept() => accepted.unwrap().0,
            };

            let mut socket = BufReader::new(socket);
            let expected_body = protocol::event_body(SERVICE, id);

            let read_request = async {
                let mut request = String::new();

                loop {
                    assert_ne!(socket.read_line(&mut request).await.unwrap(), 0);

                    if request.ends_with("\r\n\r\n") {
                        break;
                    }
                }

                assert!(request.starts_with("NOTIFY /first HTTP/1.1\r\n"));
                assert!(request.contains(&format!("\r\nseq: {seq}\r\n")));
                assert!(request.contains(&format!("\r\nsid: uuid:{sid}\r\n")));
                let mut body = vec![0; expected_body.len()];
                socket.read_exact(&mut body).await.unwrap();
                assert_eq!(body, expected_body.as_bytes());
            };

            tokio::select! {
                _ = &mut scheduler => panic!("scheduler ended"),
                _ = read_request => {}
            }

            assert!(scheduler.as_mut().poll(&mut context).is_pending());
            let _ = wake.0.notified().now_or_never();

            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\n\r\n",
                usize::from(body_pending)
            );

            if body_pending || elapsed < limit {
                socket
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();

                wake.0.notified().await;
            }

            if body_pending {
                // Observe headers on time; only timely callbacks supply a body.
                assert!(scheduler.as_mut().poll(&mut context).is_pending());
                let _ = wake.0.notified().now_or_never();

                if elapsed < limit {
                    socket.get_mut().write_all(b"x").await.unwrap();
                    wake.0.notified().await;
                }
            }

            // Leave slow callbacks pending through expiry, allowing for timer granularity.
            assert_eq!(Instant::now(), started);
            tokio::time::advance(elapsed + Duration::from_millis(1)).await;
            assert!(scheduler.as_mut().poll(&mut context).is_pending());
            let expired = elapsed >= limit;
            let rejected = !expired && status == 412;

            let entry = subscriptions
                .state
                .lock()
                .unwrap()
                .entries
                .first()
                .map(|entry| (entry.expires.is_some(), entry.delivering, entry.next_seq));

            assert_eq!(
                entry,
                (!rejected).then_some((true, expired, seq + 1)),
                "body_pending={body_pending}, status={status}, elapsed={elapsed:?}, seq={seq}"
            );

            if rejected {
                assert!(callback.received.try_recv().is_err());
                break;
            }

            if expired {
                let (alternative, body) = tokio::select! {
                    _ = &mut scheduler => panic!("scheduler ended"),
                    request = callback.next() => request,
                };

                assert_eq!(alternative.uri.path(), "/ok");
                assert_eq!(alternative.headers["sid"], format!("uuid:{sid}"));
                assert_eq!(alternative.headers["seq"], seq.to_string());
                assert_eq!(body, expected_body);

                while subscriptions.state.lock().unwrap().entries[0].delivering {
                    assert!(scheduler.as_mut().poll(&mut context).is_pending());
                    tokio::task::yield_now().await;
                }
            }

            assert!(callback.received.try_recv().is_err());

            assert_eq!(
                sid_request(
                    &subscriptions,
                    sid,
                    "SUBSCRIBE",
                    SERVICE,
                    Ipv4Addr::LOCALHOST,
                    None
                )
                .status(),
                StatusCode::OK
            );

            let state = subscriptions.state.lock().unwrap();
            let entry = &state.entries[0];
            assert!(entry.expires.is_some());
            assert!(entry.initial_update_id.is_none());
            assert_eq!(entry.next_seq, seq + 1);
            assert_eq!(entry.pending, None);
        }
    }

    clock_guard.abort();
    clock_guard.await.unwrap_err();
}

#[tokio::test]
async fn callback_port_churn_closes_keep_alive_sockets_after_delivery() {
    let subscriptions = Subscriptions::new().unwrap();
    let task = tokio::spawn(subscriptions.clone().run());
    let mut listeners = Vec::new();
    let mut sockets = Vec::new();

    // Keep listeners bound so every subscription uses a distinct callback origin.
    for _ in 0..SUBSCRIPTIONS + DELIVERIES {
        listeners.push(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
    }

    for listener in &listeners {
        let callback = format!("<http://{}/events>", listener.local_addr().unwrap());

        let response = subscriptions.request(
            SERVICE,
            Ipv4Addr::LOCALHOST,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers(&[("nt", "upnp:event"), ("callback", &callback)]),
        );

        assert_eq!(response.status(), StatusCode::OK);
        let sid = response_sid(&response);

        let socket = tokio::time::timeout(Duration::from_secs(3), async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            assert_eq!(line, "NOTIFY /events HTTP/1.1\r\n");

            loop {
                line.clear();
                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);

                if line == "\r\n" {
                    break;
                }
            }

            let expected = protocol::event_body(SERVICE, 0);
            let mut body = vec![0; expected.len()];
            socket.read_exact(&mut body).await.unwrap();
            assert_eq!(body, expected.as_bytes());

            socket
                .get_mut()
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();

            socket
        })
        .await
        .unwrap();

        finished(&subscriptions, sid).await;

        let response = sid_request(
            &subscriptions,
            sid,
            "UNSUBSCRIBE",
            SERVICE,
            Ipv4Addr::LOCALHOST,
            None,
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert!(subscriptions.state.lock().unwrap().entries.is_empty());
        sockets.push(socket);
    }

    // Callback servers remain alive while the client closes idle sockets.
    for socket in &mut sockets {
        let mut byte = [0];

        let read = tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .expect("callback client retained an idle keep-alive socket")
            .unwrap();

        assert_eq!(read, 0);
    }

    abort_scheduler(task).await;
}

#[tokio::test]
async fn expired_initial_obligations_drain_and_release_all_capacity() {
    let subscriptions = Subscriptions::new().unwrap();
    let mut callback = Callback::new().await;
    let mut tokens = Vec::new();

    for _ in 0..SUBSCRIPTIONS {
        let sid = callback.register(&subscriptions, SERVICE, &["/ok"]);
        tokens.push(sid);
    }

    tokio::time::pause();
    tokio::time::advance(SUBSCRIPTION_LEASE).await;
    tokio::time::resume();

    let task = tokio::spawn(subscriptions.clone().run());
    let mut seen = std::collections::BTreeSet::new();

    for _ in 0..SUBSCRIPTIONS {
        let (request, _) = callback.next().await;
        assert!(seen.insert(request.headers["sid"].to_str().unwrap().to_owned()));
        assert_eq!(request.headers["seq"], "0");
    }

    for sid in tokens {
        finished(&subscriptions, sid).await;
    }

    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
    abort_scheduler(task).await;
}

#[tokio::test(start_paused = true)]
async fn scheduler_expires_idle_leases_without_requests() {
    let subscriptions = Subscriptions::new().unwrap();
    let (_, sid) = subscribe(&subscriptions, None);
    subscriptions.state.lock().unwrap().entries[0].initial_update_id = None;
    let task = tokio::spawn(subscriptions.clone().run());
    tokio::task::yield_now().await;

    let response = sid_request(
        &subscriptions,
        sid,
        "SUBSCRIBE",
        SERVICE,
        PEER,
        Some("Second-2"),
    );

    assert_eq!(response.status(), StatusCode::OK);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(subscriptions.state.lock().unwrap().entries.is_empty());
    abort_scheduler(task).await;
}
