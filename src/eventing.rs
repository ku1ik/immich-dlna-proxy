//! Bounded in-memory leases and moderated notifications of published state.

use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{body::Body, response::Response};
use futures_util::{StreamExt, stream::FuturesUnordered};
use http::{HeaderMap, Method, StatusCode, header};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};
use url::Url;
use uuid::Uuid;

use crate::protocol::{HEADER_BYTES, Service};

pub(crate) const SUBSCRIPTIONS: usize = 32;
const CALLBACK_URLS: usize = 4;
const SUBSCRIPTION_LEASE: Duration = Duration::from_secs(1800);
const DELIVERIES: usize = 4;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const CALLBACK_BODY_BYTES: usize = 8 * 1024;
pub(crate) const MODERATION: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct Subscriptions {
    state: Arc<Mutex<State>>,
    wake: Arc<Notify>,
    client: reqwest::Client,
}

#[derive(Default)]
struct State {
    entries: Vec<Subscription>,
    system_update_id: u32,
    running: bool,
    stopped: bool,
}

struct Subscription {
    sid: Uuid,
    service: Service,
    peer: Ipv4Addr,
    callbacks: Vec<Url>,
    lease: Duration,
    expires: Instant,
    active: bool,
    initial: Initial,
    system_update_id: u32,
    pending: Option<u32>,
    delivering: bool,
    next_seq: u32,
    next_attempt: Instant,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Initial {
    Owed,
    Delivering,
    Finished,
}

impl State {
    fn expire(&mut self, now: Instant) {
        self.entries.retain_mut(|entry| {
            if entry.expires <= now {
                entry.active = false;
            }

            if !entry.active {
                entry.pending = None;
            }

            entry.active || entry.initial != Initial::Finished || entry.delivering
        });
    }
}

// Dropping the scheduler also drops its delivery futures and closes admission.
struct Running(Subscriptions);

impl Drop for Running {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.stopped = true;
        state.entries.clear();
    }
}

impl Subscriptions {
    pub fn new() -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            // Callback-port churn must not accumulate idle sockets across origins.
            .pool_max_idle_per_host(0)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(CALLBACK_TIMEOUT)
            .build()?;

        Ok(Self {
            state: Arc::new(Mutex::new(State::default())),
            wake: Arc::new(Notify::new()),
            client,
        })
    }

    /// Publish only durably committed changes, while holding the short catalog
    /// publication lock after assigning its ledger/snapshot. Initialize with the
    /// startup ID before discovery or subscription admission. No I/O occurs here,
    /// and subscription operations never acquire a catalog lock.
    pub fn publish(&self, id: u32) {
        let mut state = self.state.lock().unwrap();

        if state.system_update_id == id {
            return;
        }

        state.system_update_id = id;
        state.expire(Instant::now());

        for entry in &mut state.entries {
            if entry.active && entry.service == Service::ContentDirectory {
                entry.pending = Some(id);
            }
        }

        drop(state);
        self.wake.notify_one();
    }

    pub fn request(
        &self,
        service: Service,
        peer: Ipv4Addr,
        method: &Method,
        headers: &HeaderMap,
    ) -> Response {
        let result = (|| {
            if headers
                .iter()
                .map(|(name, value)| name.as_str().len() + value.len() + 4)
                .sum::<usize>()
                > HEADER_BYTES
            {
                return Err(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
            }

            if headers.contains_key(header::TRANSFER_ENCODING)
                || single_header(headers, "content-length")?
                    .is_some_and(|value| decimal(value) != Some(0))
            {
                return Err(StatusCode::BAD_REQUEST);
            }

            if !matches!(method.as_str(), "SUBSCRIBE" | "UNSUBSCRIBE") {
                return Err(StatusCode::METHOD_NOT_ALLOWED);
            }

            let sid = single_header(headers, "sid")?;
            let nt = single_header(headers, "nt")?;
            let callback = single_header(headers, "callback")?;
            let timeout = single_header(headers, "timeout")?;

            if (sid.is_some() && (nt.is_some() || callback.is_some()))
                || (method.as_str() == "UNSUBSCRIBE"
                    && (sid.is_none() || nt.is_some() || callback.is_some()))
            {
                return Err(StatusCode::BAD_REQUEST);
            }

            let lease = lease(timeout)?;

            if let Some(sid) = sid {
                let sid = sid
                    .strip_prefix("uuid:")
                    .filter(|value| value.len() == 36)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or(StatusCode::PRECONDITION_FAILED)?;

                let mut state = self.state.lock().unwrap();
                let now = Instant::now();
                state.expire(now);

                if state.stopped {
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }

                let entry = state
                    .entries
                    .iter_mut()
                    .find(|entry| {
                        entry.sid == sid
                            && entry.service == service
                            && entry.peer == peer
                            && entry.active
                    })
                    .ok_or(StatusCode::PRECONDITION_FAILED)?;

                if method.as_str() == "UNSUBSCRIBE" {
                    entry.active = false;
                } else {
                    entry.lease = lease;
                    entry.expires = now + lease;
                }

                let granted = entry.lease;
                state.expire(now);

                return Ok((sid, granted));
            }

            if nt != Some("upnp:event") {
                return Err(StatusCode::PRECONDITION_FAILED);
            }

            let callbacks = callbacks(callback.ok_or(StatusCode::PRECONDITION_FAILED)?, peer)
                .ok_or(StatusCode::PRECONDITION_FAILED)?;

            let mut state = self.state.lock().unwrap();
            let now = Instant::now();
            state.expire(now);

            if state.stopped || state.entries.len() >= SUBSCRIPTIONS {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }

            let sid = Uuid::new_v4();
            let system_update_id = state.system_update_id;

            // Registration and publication share this lock; older changes are not replayed.
            state.entries.push(Subscription {
                sid,
                service,
                peer,
                callbacks,
                lease,
                expires: now + lease,
                active: true,
                initial: Initial::Owed,
                system_update_id,
                pending: None,
                delivering: false,
                next_seq: 0,
                next_attempt: now,
            });

            Ok((sid, lease))
        })();

        let mut response = Response::builder().header(header::CONTENT_LENGTH, "0");

        match result {
            Ok((sid, lease)) => {
                response = response
                    .status(StatusCode::OK)
                    .header("sid", format!("uuid:{sid}"))
                    .header("timeout", format!("Second-{}", lease.as_secs()));

                self.wake.notify_one();
            }

            Err(status) => {
                response = response.status(status);

                if status == StatusCode::METHOD_NOT_ALLOWED {
                    response = response.header(header::ALLOW, "SUBSCRIBE, UNSUBSCRIBE");
                }
            }
        }

        let response = response.body(Body::empty()).unwrap();

        let method = match method.as_str() {
            "SUBSCRIBE" => "SUBSCRIBE",
            "UNSUBSCRIBE" => "UNSUBSCRIBE",
            _ => "unsupported",
        };

        tracing::debug!(
            %peer, ?service, method,
            sid_supplied = headers.contains_key("sid"),
            status = response.status().as_u16(),
            sid = ?response.headers().get("sid"),
            lease = ?response.headers().get("timeout"),
            "subscription response"
        );

        response
    }

    /// Runs the single bounded delivery scheduler. No catalog polling occurs.
    pub async fn run(self) -> anyhow::Result<()> {
        {
            let mut state = self.state.lock().unwrap();

            anyhow::ensure!(
                !state.running && !state.stopped,
                "eventing scheduler already started or stopped"
            );

            state.running = true;
        }

        let _running = Running(self.clone());
        let mut deliveries = FuturesUnordered::new();

        loop {
            // Notify retains a permit if a request/publication races inspection and select.
            let notified = self.wake.notified();

            let deadline = {
                let mut state = self.state.lock().unwrap();
                let now = Instant::now();
                state.expire(now);

                while deliveries.len() < DELIVERIES {
                    let Some(index) = state.entries.iter().position(|entry| {
                        !entry.delivering
                            && (entry.initial == Initial::Owed
                                || (entry.initial == Initial::Finished
                                    && entry.active
                                    && entry.next_attempt <= now
                                    && entry.pending.is_some()))
                    }) else {
                        break;
                    };

                    let mut entry = state.entries.remove(index);

                    let id = if entry.initial == Initial::Owed {
                        entry.initial = Initial::Delivering;

                        entry.system_update_id
                    } else {
                        entry.pending.take().expect("eligible pending event")
                    };

                    // Allocate once at preparation, including failures and the initial attempt.
                    let seq = entry.next_seq;
                    entry.next_seq = seq.wrapping_add(1).max(1);
                    entry.next_attempt = now + MODERATION;
                    entry.delivering = true;

                    deliveries.push(deliver(
                        self.client.clone(),
                        entry.sid,
                        entry.callbacks.clone(),
                        seq,
                        event_body(entry.service, id),
                    ));

                    // Preserve waiting order across removals and new registrations.
                    state.entries.push(entry);
                }

                state
                    .entries
                    .iter()
                    .filter(|entry| entry.active)
                    .flat_map(|entry| {
                        // A ready event waits on completion when all delivery slots are full.
                        let moderation = (deliveries.len() < DELIVERIES
                            && entry.initial == Initial::Finished
                            && !entry.delivering
                            && entry.pending.is_some())
                        .then_some(entry.next_attempt);

                        Some(entry.expires).into_iter().chain(moderation)
                    })
                    .min()
            };

            let expiry = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };

            tokio::select! {
                biased;
                finished = deliveries.next(), if !deliveries.is_empty() => {
                    let (sid, deactivate) = finished.expect("nonempty delivery set");
                    let mut state = self.state.lock().unwrap();

                    if let Some(entry) = state.entries.iter_mut().find(|entry| entry.sid == sid) {
                        entry.initial = Initial::Finished;
                        entry.delivering = false;
                        entry.active &= !deactivate;
                    }

                    state.expire(Instant::now());
                }

                _ = notified => {}

                _ = expiry => {}
            }
        }
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, StatusCode> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();

    if values.next().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }

    value
        .map(|value| {
            value
                .to_str()
                .map(str::trim)
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()
}

fn decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    value.parse().ok()
}

fn lease(value: Option<&str>) -> Result<Duration, StatusCode> {
    let Some(value) = value else {
        return Ok(SUBSCRIPTION_LEASE);
    };

    if value == "Second-infinite" {
        return Ok(SUBSCRIPTION_LEASE);
    }

    let seconds = value
        .strip_prefix("Second-")
        .and_then(decimal)
        .filter(|seconds| *seconds > 0)
        .ok_or(StatusCode::BAD_REQUEST)?;

    Ok(Duration::from_secs(seconds).min(SUBSCRIPTION_LEASE))
}

fn callbacks(value: &str, peer: Ipv4Addr) -> Option<Vec<Url>> {
    if matches!(peer.octets()[0], 0 | 224..=255) || (peer.is_loopback() && !cfg!(test)) {
        return None;
    }

    let mut remaining = value.trim();
    let mut urls = Vec::new();

    while !remaining.is_empty() {
        if urls.len() == CALLBACK_URLS {
            return None;
        }

        let (raw, rest) = remaining.strip_prefix('<')?.split_once('>')?;

        if raw
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control() || byte == b'\\')
        {
            return None;
        }

        // Validate the original authority before URL parsing can normalize numeric hosts.
        let (scheme, location) = raw.split_once("://")?;
        let authority = location.split(['/', '?', '#']).next()?;

        let host = match authority.split_once(':') {
            Some((host, port)) => {
                let port = decimal(port)?;

                if port == 0 || port > u16::MAX.into() {
                    return None;
                }

                host
            }

            None => authority,
        };

        if !scheme.eq_ignore_ascii_case("http") || host.parse::<Ipv4Addr>().ok()? != peer {
            return None;
        }

        let url = Url::parse(raw).ok()?;

        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return None;
        }

        urls.push(url);
        remaining = rest.trim_start();
    }

    (!urls.is_empty()).then_some(urls)
}

fn event_body(service: Service, system_update_id: u32) -> String {
    let properties = match service {
        Service::ContentDirectory => {
            format!("<e:property><SystemUpdateID>{system_update_id}</SystemUpdateID></e:property>")
        }

        Service::ConnectionManager => "<e:property><SourceProtocolInfo>http-get:*:*:*</SourceProtocolInfo></e:property><e:property><SinkProtocolInfo></SinkProtocolInfo></e:property><e:property><CurrentConnectionIDs>0</CurrentConnectionIDs></e:property>".into(),
    };

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><e:propertyset xmlns:e=\"urn:schemas-upnp-org:event-1-0\">{properties}</e:propertyset>"
    )
}

async fn deliver(
    client: reqwest::Client,
    sid: Uuid,
    callbacks: Vec<Url>,
    seq: u32,
    body: String,
) -> (Uuid, bool) {
    for url in callbacks {
        let started = Instant::now();
        let header_deadline = started + RESPONSE_HEADER_TIMEOUT;
        let deadline = started + CALLBACK_TIMEOUT;

        let attempt = async {
            let request = client
                .request(Method::from_bytes(b"NOTIFY").unwrap(), url)
                .header("nt", "upnp:event")
                .header("nts", "upnp:propchange")
                .header("sid", format!("uuid:{sid}"))
                .header("seq", seq.to_string())
                .header(header::CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                .header(header::CONTENT_LENGTH, body.len())
                .header(header::ACCEPT_ENCODING, "identity")
                .body(body.clone());

            let mut response = timeout_at(header_deadline, async { request.send().await })
                .await
                .ok()?
                .ok()?;

            let status = response.status();

            if status == StatusCode::PRECONDITION_FAILED {
                return Some(status);
            }

            if response
                .content_length()
                .is_some_and(|length| length > CALLBACK_BODY_BYTES as u64)
            {
                return None;
            }

            let mut bytes = 0;

            while let Some(chunk) = response.chunk().await.ok()? {
                if chunk.len() > CALLBACK_BODY_BYTES - bytes {
                    return None;
                }

                bytes += chunk.len();
            }

            Some(status)
        };

        let result = timeout_at(deadline, attempt).await;

        match result {
            Ok(Some(StatusCode::OK)) => {
                tracing::debug!(%sid, seq, "event delivered");

                return (sid, false);
            }

            Ok(Some(StatusCode::PRECONDITION_FAILED)) => {
                tracing::debug!(%sid, seq, "event rejected with invalid SID; subscription removed");

                return (sid, true);
            }
            _ => {}
        }
    }

    tracing::debug!(%sid, seq, "event delivery failed; lease retained if active");

    (sid, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::to_bytes, routing::any};
    use quick_xml::{NsReader, events::Event, name::ResolveResult};
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
                Instant::now() + Duration::from_secs(expected)
            );

            assert_eq!(state.entries[0].system_update_id, 42);
            assert!(state.entries[0].initial == Initial::Owed);
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
            assert_eq!(first.system_update_id, 0);
            assert_eq!(first.pending, Some(u32::MAX));
            assert_eq!(second.system_update_id, u32::MAX);
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
                    (entry.system_update_id, entry.pending),
                    (7, Some(8)) | (8, None)
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
            (
                "SUBSCRIBE",
                vec![("nt", "upnp:event"), ("nt", "upnp:event")],
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
    fn body_and_duplicate_headers_are_rejected_before_admission() {
        let subscriptions = Subscriptions::new().unwrap();

        for extra in [
            vec![("content-length", "1")],
            vec![("content-length", "bad")],
            vec![("content-length", "0"), ("content-length", "0")],
            vec![("transfer-encoding", "chunked")],
            vec![("timeout", "Second-1"), ("timeout", "Second-1")],
            vec![("callback", "<http://192.168.1.20/>")],
            vec![("sid", "bad"), ("sid", "bad")],
        ] {
            let mut values = vec![("nt", "upnp:event"), ("callback", "<http://192.168.1.20/>")];
            values.extend(extra);

            let response = subscriptions.request(
                SERVICE,
                PEER,
                &Method::from_bytes(b"SUBSCRIBE").unwrap(),
                &headers(&values),
            );

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        let response = subscriptions.request(
            SERVICE,
            PEER,
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers(&[("x-large", &"a".repeat(HEADER_BYTES))]),
        );

        assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );

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
                    sid_request(&subscriptions, requested_sid, method, service, peer, None)
                        .status(),
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
            assert!(state.entries[0].initial == Initial::Owed);
            assert_eq!(state.entries[0].system_update_id, 42);

            assert_eq!(
                state.entries[0].expires,
                Instant::now() + Duration::from_secs(20)
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

            let app = Router::new().fallback(any(move |request: axum::extract::Request| {
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

                        "/oversized" => {
                            Response::new(Body::from(vec![b'x'; CALLBACK_BODY_BYTES + 1]))
                        }

                        "/chunked" => {
                            Response::new(Body::from_stream(futures_util::stream::iter([
                                Ok::<_, std::io::Error>(vec![b'x'; CALLBACK_BODY_BYTES]),
                                Ok(vec![b'x']),
                            ])))
                        }

                        "/slow-headers" => std::future::pending::<Response>().await,

                        "/slow-body" => {
                            Response::new(Body::from_stream(futures_util::stream::pending::<
                                Result<Vec<u8>, std::io::Error>,
                            >()))
                        }

                        _ => Response::new(Body::empty()),
                    }
                }
            }));

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

        fn register(
            &self,
            subscriptions: &Subscriptions,
            service: Service,
            paths: &[&str],
        ) -> Uuid {
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
                        entry.sid != sid
                            || (entry.initial == Initial::Finished && !entry.delivering)
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

        assert_eq!(body, event_body(SERVICE, 17));
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
                assert_eq!(body, event_body(SERVICE, 10));
                assert!(initial.insert(sid));
            } else {
                assert_eq!(sid_header, format!("uuid:{cm}"));
                assert_eq!(body, event_body(Service::ConnectionManager, 12));
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
            assert!(entry.initial == Initial::Delivering);
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
        assert_eq!(body, event_body(SERVICE, 14));
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
        assert_eq!(body, event_body(SERVICE, 16));
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

        for (id, seq, future) in [(0, 0, None), (8, u32::MAX, Some(9)), (9, 1, None)] {
            let (request, body) = callback.next().await;
            assert_eq!(request.uri.path(), "/gated-fail");
            assert_eq!(request.headers["seq"], seq.to_string());
            assert_eq!(body, event_body(SERVICE, id));

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
                assert!(entry.active);
                assert_eq!(entry.pending, future);
                assert_eq!(entry.next_seq, seq.wrapping_add(1).max(1));

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
            assert_eq!(body, event_body(SERVICE, 1));

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
            assert_eq!(body, event_body(SERVICE, 2));

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
        assert!(subscriptions.state.lock().unwrap().entries.is_empty());
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

                entry.expires = Instant::now();
                state.expire(Instant::now());
            }

            for _ in 0..8 {
                callback.register(&subscriptions, SERVICE, &["/gated"]);
            }

            callback.release.add_permits(1);
            let (request, body) = callback.next().await;
            assert_eq!(request.headers["sid"], format!("uuid:{target}"));
            assert_eq!(request.headers["seq"], "0");
            assert_eq!(body, event_body(SERVICE, 17));
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
                    subscriptions.state.lock().unwrap().entries[0].expires = Instant::now();
                }

                subscriptions.publish(3);

                {
                    let state = subscriptions.state.lock().unwrap();
                    let entry = &state.entries[0];
                    assert!(!entry.active);
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

    #[test]
    fn propertyset_namespaces_and_static_values_are_correct() {
        for (service, expected) in [
            (SERVICE, vec![("SystemUpdateID", "4294967295")]),
            (
                Service::ConnectionManager,
                vec![
                    ("SourceProtocolInfo", "http-get:*:*:*"),
                    ("SinkProtocolInfo", ""),
                    ("CurrentConnectionIDs", "0"),
                ],
            ),
        ] {
            let body = event_body(service, u32::MAX);
            let mut reader = NsReader::from_str(&body);
            let mut variables = Vec::new();

            loop {
                match reader.read_resolved_event().unwrap() {
                    (namespace, Event::Start(start)) => {
                        let name = String::from_utf8(start.local_name().as_ref().to_vec()).unwrap();

                        if matches!(name.as_str(), "propertyset" | "property") {
                            assert_eq!(
                                namespace,
                                ResolveResult::Bound(quick_xml::name::Namespace(
                                    b"urn:schemas-upnp-org:event-1-0"
                                ))
                            );
                        } else {
                            assert_eq!(namespace, ResolveResult::Unbound);
                            variables.push((name, String::new()));
                        }
                    }

                    (_, Event::Text(text)) => variables
                        .last_mut()
                        .unwrap()
                        .1
                        .push_str(&text.decode().unwrap()),

                    (_, Event::Eof) => break,

                    _ => {}
                }
            }

            assert_eq!(
                variables,
                expected
                    .into_iter()
                    .map(|(name, value)| (name.to_owned(), value.to_owned()))
                    .collect::<Vec<_>>()
            );
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
            assert_eq!(body, event_body(Service::ConnectionManager, 0));
        }

        finished(&subscriptions, sid).await;
        assert!(callback.received.try_recv().is_err());
        assert!(subscriptions.state.lock().unwrap().entries[0].active);
        abort_scheduler(task).await;
    }

    #[tokio::test]
    async fn oversized_bodies_fall_back_and_412_removes_without_trying_alternatives() {
        let subscriptions = Subscriptions::new().unwrap();
        subscriptions.publish(3);
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
            assert!(state.entries[0].active);
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
                    .filter(|entry| entry.initial == Initial::Delivering)
                    .count(),
                DELIVERIES
            );
        }

        assert!(subscriptions.clone().run().await.is_err());

        abort_scheduler(task).await;
        assert!(callback.received.try_recv().is_err());
    }

    #[tokio::test]
    async fn callback_header_and_total_deadlines_are_per_url_and_keep_active_leases() {
        for (path, deadline) in [
            ("/slow-headers", RESPONSE_HEADER_TIMEOUT),
            ("/slow-body", CALLBACK_TIMEOUT),
        ] {
            let subscriptions = Subscriptions::new().unwrap();
            let mut callback = Callback::new().await;
            let sid = callback.register(&subscriptions, SERVICE, &[path, "/fail"]);
            let task = tokio::spawn(subscriptions.clone().run());
            assert_eq!(callback.next().await.0.uri.path(), path);
            tokio::time::pause();
            tokio::time::advance(deadline).await;
            tokio::time::resume();
            assert_eq!(callback.next().await.0.uri.path(), "/fail");
            finished(&subscriptions, sid).await;
            assert!(subscriptions.state.lock().unwrap().entries[0].active);
            abort_scheduler(task).await;
        }
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

        for (body_pending, status, limit) in [
            (false, 412, RESPONSE_HEADER_TIMEOUT),
            (false, 200, RESPONSE_HEADER_TIMEOUT),
            (true, 200, CALLBACK_TIMEOUT),
        ] {
            for elapsed in [
                limit - Duration::from_secs(1),
                limit,
                limit + Duration::from_secs(1),
            ] {
                let mut subscriptions = Subscriptions::new().unwrap();

                if body_pending {
                    // Exercise our total deadline without reqwest's own timer masking it.
                    subscriptions.client = reqwest::Client::builder()
                        .no_proxy()
                        .pool_max_idle_per_host(0)
                        .build()
                        .unwrap();
                }

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
                    let expected_body = event_body(SERVICE, id);

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
                        .map(|entry| (entry.active, entry.delivering, entry.next_seq));

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
                    assert!(entry.active);
                    assert!(entry.initial == Initial::Finished);
                    assert_eq!(entry.next_seq, seq + 1);
                    assert_eq!(entry.pending, None);
                }
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

                let expected = event_body(SERVICE, 0);
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

        for id in 0..SUBSCRIPTIONS as u32 {
            subscriptions.publish(id);
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
        subscriptions.state.lock().unwrap().entries[0].initial = Initial::Finished;
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
}
