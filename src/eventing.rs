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

use crate::protocol::{self, Service};

pub(crate) const SUBSCRIPTIONS: usize = 32;
const CALLBACK_URLS: usize = 4;
const SUBSCRIPTION_LEASE: Duration = Duration::from_secs(1800);
const DELIVERIES: usize = 4;
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const CALLBACK_BODY_BYTES: usize = 8 * 1024;
pub(crate) const MODERATION: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Subscriptions {
    state: Arc<Mutex<State>>,
    wake: Arc<Notify>,
}

/// The uniquely owned notification scheduler, consumed when started.
pub struct EventTask {
    subscriptions: Subscriptions,
    client: reqwest::Client,
}

#[derive(Default)]
struct State {
    entries: Vec<Subscription>,
    system_update_id: u32,
}

struct Subscription {
    sid: Uuid,
    service: Service,
    peer: Ipv4Addr,
    callbacks: Vec<Url>,
    lease: Duration,
    expires: Option<Instant>,
    delivery: Delivery,
    pending: Option<u32>,
    next_seq: u32,
    next_attempt: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    Initial(u32),
    Idle,
    InFlight,
}

impl Subscription {
    fn next_delivery(&self) -> Option<(Instant, u32)> {
        let id = match self.delivery {
            Delivery::Initial(id) => id,
            Delivery::Idle if self.expires.is_some() => self.pending?,
            Delivery::Idle | Delivery::InFlight => return None,
        };

        // The initial obligation survives lease expiry. Its next_attempt is the
        // registration time; moderation starts only when an attempt is prepared.
        Some((self.next_attempt, id))
    }
}

impl State {
    fn expire(&mut self, now: Instant) {
        self.entries.retain_mut(|entry| {
            if entry.expires.is_some_and(|expires| expires <= now) {
                entry.expires = None;
            }

            if entry.expires.is_none() {
                entry.pending = None;
            }

            entry.expires.is_some() || entry.delivery != Delivery::Idle
        });
    }
}

impl Subscriptions {
    /// Create a shared subscription handle and its single delivery task.
    pub fn new() -> anyhow::Result<(Self, EventTask)> {
        let client = crate::outbound_client_builder()
            // Callback-port churn must not accumulate idle sockets across origins.
            .pool_max_idle_per_host(0)
            .build()?;

        let subscriptions = Self {
            state: Arc::new(Mutex::new(State::default())),
            wake: Arc::new(Notify::new()),
        };

        Ok((
            subscriptions.clone(),
            EventTask {
                subscriptions,
                client,
            },
        ))
    }

    /// Update pending events synchronously with catalog publication. Initialize
    /// with the startup ID before discovery or subscription admission. No I/O
    /// occurs here; registration and publication share only the event-state lock.
    pub(crate) fn publish(&self, id: u32) {
        let mut state = self.state.lock().unwrap();

        if state.system_update_id == id {
            return;
        }

        state.system_update_id = id;
        state.expire(Instant::now());

        for entry in &mut state.entries {
            if entry.expires.is_some() && entry.service == Service::ContentDirectory {
                entry.pending = Some(id);
            }
        }

        drop(state);
        self.wake.notify_one();
    }

    pub(crate) fn request(
        &self,
        service: Service,
        peer: Ipv4Addr,
        method: &Method,
        headers: &HeaderMap,
    ) -> Response {
        let result = self.apply_request(service, peer, method, headers);
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

    fn apply_request(
        &self,
        service: Service,
        peer: Ipv4Addr,
        method: &Method,
        headers: &HeaderMap,
    ) -> Result<(Uuid, Duration), StatusCode> {
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

            let entry = state
                .entries
                .iter_mut()
                .find(|entry| {
                    entry.sid == sid
                        && entry.service == service
                        && entry.peer == peer
                        && entry.expires.is_some()
                })
                .ok_or(StatusCode::PRECONDITION_FAILED)?;

            if method.as_str() == "UNSUBSCRIBE" {
                entry.expires = None;
            } else {
                entry.lease = lease;
                entry.expires = Some(now + lease);
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

        if state.entries.len() >= SUBSCRIPTIONS {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

        let sid = Uuid::new_v4();
        let delivery = Delivery::Initial(state.system_update_id);

        // Registration and publication share this lock; older changes are not replayed.
        state.entries.push(Subscription {
            sid,
            service,
            peer,
            callbacks,
            lease,
            expires: Some(now + lease),
            delivery,
            pending: None,
            next_seq: 0,
            next_attempt: now,
        });

        Ok((sid, lease))
    }
}

impl EventTask {
    /// Runs the single bounded delivery scheduler. No catalog polling occurs.
    pub async fn run(self) -> anyhow::Result<()> {
        let subscriptions = self.subscriptions;
        let mut deliveries = FuturesUnordered::new();

        loop {
            // Notify retains a permit if a request/publication races inspection and select.
            let notified = subscriptions.wake.notified();

            let deadline = {
                let mut state = subscriptions.state.lock().unwrap();
                let now = Instant::now();
                state.expire(now);

                while deliveries.len() < DELIVERIES {
                    let Some((index, id)) =
                        state.entries.iter().enumerate().find_map(|(index, entry)| {
                            let (at, id) = entry.next_delivery()?;

                            (at <= now).then_some((index, id))
                        })
                    else {
                        break;
                    };

                    let mut entry = state.entries.remove(index);

                    if entry.delivery == Delivery::Idle {
                        entry.pending = None;
                    }

                    // Allocate once at preparation, including failures and the initial attempt.
                    let seq = entry.next_seq;
                    entry.next_seq = seq.wrapping_add(1).max(1);
                    entry.next_attempt = now + MODERATION;
                    entry.delivery = Delivery::InFlight;

                    deliveries.push(deliver(
                        self.client.clone(),
                        entry.sid,
                        entry.callbacks.clone(),
                        seq,
                        protocol::event_body(entry.service, id),
                    ));

                    // Preserve waiting order across removals and new registrations.
                    state.entries.push(entry);
                }

                state
                    .entries
                    .iter()
                    .flat_map(|entry| {
                        // A ready event waits on completion when all delivery slots are full.
                        let delivery = if deliveries.len() < DELIVERIES {
                            entry.next_delivery().map(|(at, _)| at)
                        } else {
                            None
                        };

                        entry.expires.into_iter().chain(delivery)
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
                    let mut state = subscriptions.state.lock().unwrap();

                    let entry = state
                        .entries
                        .iter_mut()
                        .find(|entry| entry.sid == sid)
                        .expect("in-flight subscription is retained");

                    entry.delivery = Delivery::Idle;

                    if deactivate {
                        entry.expires = None;
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

async fn deliver(
    client: reqwest::Client,
    sid: Uuid,
    callbacks: Vec<Url>,
    seq: u32,
    body: String,
) -> (Uuid, bool) {
    for url in callbacks {
        let deadline = Instant::now() + CALLBACK_TIMEOUT;

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

            let mut response = request.send().await.ok()?;

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
mod tests;
