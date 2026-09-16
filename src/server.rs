use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Router, body::Body, extract::Request, response::Response};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinSet,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    catalog::Catalog,
    eventing::Subscriptions,
    media::MediaProxy,
    protocol::{self, Action, Fault, SOAP_BODY_BYTES, Service},
    transport::{self, HEADER_BYTES, WriteDeadline},
};

const BROWSES: usize = 8;
pub const CONNECTIONS: usize = 64;
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_PROCESSING_TIMEOUT: Duration = Duration::from_secs(25);
const CONTROL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
pub const WRITE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Server<C> {
    catalog: C,
    media: MediaProxy,
    subscriptions: Subscriptions,
    device: String,
    browses: Arc<Semaphore>,
    timing: Timing,
}

#[derive(Clone, Copy)]
struct Timing {
    header: Duration,
    body: Duration,
    processing: Duration,
    response: Duration,
    write: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            header: HEADER_TIMEOUT,
            body: BODY_TIMEOUT,
            processing: CONTROL_PROCESSING_TIMEOUT,
            response: CONTROL_RESPONSE_TIMEOUT,
            write: WRITE_IDLE_TIMEOUT,
        }
    }
}

impl<C: Catalog> Server<C> {
    pub fn new(
        friendly_name: String,
        uuid: Uuid,
        catalog: C,
        media: MediaProxy,
        subscriptions: Subscriptions,
    ) -> Self {
        Self {
            catalog,
            media,
            subscriptions,
            device: protocol::device_description(&friendly_name, uuid),
            browses: Arc::new(Semaphore::new(BROWSES)),
            timing: Timing::default(),
        }
    }

    /// Discovery and the subscription scheduler are supervised by the caller.
    pub async fn run(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
        shutdown_grace: Duration,
    ) -> anyhow::Result<()> {
        let server = Arc::new(self);
        let mut tasks = JoinSet::new();
        let mut failure = None;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,

                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if joined.unwrap().is_err() {
                        failure = Some(anyhow::anyhow!("HTTP connection task failed"));
                        break;
                    }
                }

                accepted = listener.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(accepted) => accepted,

                        Err(_) => {
                            failure = Some(anyhow::anyhow!("HTTP listener accept failed"));
                            break;
                        }
                    };

                    if shutdown.is_cancelled() {
                        break;
                    }

                    // Finished tasks count until reaped: neither live tasks nor results backlog.
                    if tasks.len() >= CONNECTIONS {
                        let _ = socket.try_write(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
                        continue;
                    }

                    let std::net::IpAddr::V4(peer) = peer.ip() else {
                        continue;
                    };

                    tasks.spawn(connection(socket, peer, server.clone(), shutdown.clone()));
                }
            }
        }

        drop(listener);

        if failure.is_some() {
            tasks.abort_all();
        }

        let deadline = Instant::now() + shutdown_grace;

        while !tasks.is_empty() {
            match timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Err(error))) if error.is_panic() => {
                    failure = Some(anyhow::anyhow!("HTTP connection task panicked"));
                    tasks.abort_all();
                }

                Ok(_) => {}

                Err(_) => {
                    tasks.abort_all();
                    break;
                }
            }
        }

        // Aborted tasks must actually drop their bodies, upstream work and pending tokens.
        while let Some(joined) = tasks.join_next().await {
            if joined.is_err_and(|error| error.is_panic()) {
                failure = Some(anyhow::anyhow!("HTTP connection task panicked"));
            }
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn handle(
        &self,
        request: Request,
        peer: Ipv4Addr,
        token: &Mutex<Option<Uuid>>,
        shutdown: &CancellationToken,
        started: Instant,
    ) -> Response {
        if shutdown.is_cancelled() {
            return empty(StatusCode::SERVICE_UNAVAILABLE);
        }

        let (parts, body) = request.into_parts();
        let path = parts.uri.path();

        let header_bytes = parts.method.as_str().len()
            + parts.uri.to_string().len()
            + 14
            + parts
                .headers
                .iter()
                .map(|(name, value)| name.as_str().len() + value.len() + 4)
                .sum::<usize>();

        if header_bytes > HEADER_BYTES {
            return empty(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
        }

        // Hyper records upgrades before dispatch and may finish without poll_shutdown.
        // Reject them before accepting work, especially a subscription obligation.
        if parts.headers.contains_key(header::UPGRADE) {
            return empty(StatusCode::BAD_REQUEST);
        }

        if parts.uri.query().is_some() {
            return empty(StatusCode::NOT_FOUND);
        }

        let service_route = match path {
            "/upnp/content-directory/scpd.xml" => Some((Service::ContentDirectory, "scpd")),
            "/upnp/connection-manager/scpd.xml" => Some((Service::ConnectionManager, "scpd")),
            "/upnp/content-directory/control" => Some((Service::ContentDirectory, "control")),
            "/upnp/connection-manager/control" => Some((Service::ConnectionManager, "control")),
            "/upnp/content-directory/events" => Some((Service::ContentDirectory, "events")),
            "/upnp/connection-manager/events" => Some((Service::ConnectionManager, "events")),
            _ => None,
        };

        if let Some((service, "control")) = service_route {
            if parts.method != Method::POST {
                return method_not_allowed("POST");
            }

            let response = self
                .control(service, parts.headers, body, started, peer)
                .await;

            tracing::debug!(%peer, ?service, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis(), "control response");

            return response;
        }

        let media_route = path
            .strip_prefix("/media/assets/")
            .and_then(|path| path.split_once('/'));

        if path != "/device.xml" && service_route.is_none() && media_route.is_none() {
            return empty(StatusCode::NOT_FOUND);
        }

        if declared_body(&parts.headers) {
            return empty(StatusCode::BAD_REQUEST);
        }

        if let Some((service, "events")) = service_route {
            let (response, pending) =
                self.subscriptions
                    .request(service, peer, &parts.method, &parts.headers);

            *token.lock().unwrap() = pending;

            return response;
        }

        if parts.method != Method::GET && parts.method != Method::HEAD {
            return method_not_allowed("GET, HEAD");
        }

        if let Some((asset, representation)) = media_route {
            if let Ok(asset) = Uuid::parse_str(asset)
                && matches!(
                    representation,
                    "original" | "display" | "preview" | "playback"
                )
            {
                tracing::debug!(%peer, %asset, representation, "media request peer");
            }

            return self
                .media
                .serve(asset, representation, parts.method, parts.headers)
                .await;
        }

        let document = match service_route {
            Some((service, "scpd")) => protocol::scpd(service),
            _ => &self.device,
        };

        let mut response = xml(StatusCode::OK, document.to_owned());

        if parts.method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }

        response
    }

    async fn control(
        &self,
        service: Service,
        headers: HeaderMap,
        body: Body,
        started: Instant,
        peer: Ipv4Addr,
    ) -> Response {
        if !single(&headers, "content-type").is_some_and(soap_content_type) {
            return empty(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }

        let Some(soap_action) = single(&headers, "soapaction") else {
            return soap(Err(Fault { code: 402 }));
        };

        if single(&headers, "content-length")
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > SOAP_BODY_BYTES as u64)
        {
            return soap(Err(Fault { code: 501 }));
        }

        let body_deadline = started + self.timing.body;

        let body =
            match timeout_at(body_deadline, axum::body::to_bytes(body, SOAP_BODY_BYTES)).await {
                Ok(Ok(body)) => body,

                Err(_) => return empty(StatusCode::REQUEST_TIMEOUT),

                Ok(Err(error)) => {
                    use std::error::Error;

                    if error
                        .source()
                        .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                    {
                        return soap(Err(Fault { code: 501 }));
                    }

                    return empty(StatusCode::BAD_REQUEST);
                }
            };

        // Acquisition has its own HTTP errors; only an acquired body enters SOAP processing.
        let deadline = started + self.timing.processing;

        if Instant::now() >= deadline {
            return soap(Err(Fault { code: 501 }));
        }

        let parsed = protocol::parse_action(&body, soap_action, service);

        if Instant::now() >= deadline {
            return soap(Err(Fault { code: 501 }));
        }

        let action = match parsed {
            Ok(action) => action,

            Err(fault) => {
                tracing::debug!(%peer, ?service, code = fault.code, "SOAP request rejected");

                return soap(Err(fault));
            }
        };

        let name = action.name();
        tracing::debug!(%peer, ?service, action = name, "SOAP action");

        if Instant::now() >= deadline {
            return soap(Err(Fault { code: 501 }));
        }

        // Keep admission outside timed execution, including fault construction and handoff.
        let permit = if matches!(action, Action::Browse(_)) {
            let Ok(permit) = self.browses.clone().try_acquire_owned() else {
                return empty(StatusCode::SERVICE_UNAVAILABLE);
            };

            Some(permit)
        } else {
            None
        };

        let result = timeout_at(deadline, async {
            let args = match action {
                Action::Browse(args) => {
                    let filter = args.filter.clone();
                    let object = crate::catalog::parse_id(&args.object_id).ok();

                    tracing::debug!(
                        %peer, ?object,
                        metadata = args.metadata,
                        starting_index = ?Some(args.starting_index),
                        requested_count = ?Some(args.requested_count),
                        sort = ?args.sort,
                        resources_selected = filter.res(),
                        "Browse request"
                    );

                    let result = self.catalog.browse(args).await?;

                    if Instant::now() >= deadline {
                        return Err(Fault { code: 501 });
                    }

                    let didl = protocol::didl(&result.objects, &filter)?;

                    if Instant::now() >= deadline {
                        return Err(Fault { code: 501 });
                    }

                    let envelope = protocol::action_response(
                        service,
                        "Browse",
                        &[
                            ("Result", &didl),
                            ("NumberReturned", &result.objects.len().to_string()),
                            ("TotalMatches", &result.total_matches.to_string()),
                            ("UpdateID", &result.update_id.to_string()),
                        ],
                    )?;

                    tracing::debug!(%peer, ?object, returned = result.objects.len(), total = result.total_matches, update_id = result.update_id, "Browse response");

                    return Ok(soap(Ok(envelope)));
                }

                Action::GetSearchCapabilities => vec![("SearchCaps", String::new())],
                Action::GetSortCapabilities => vec![("SortCaps", "dc:date".into())],

                Action::GetSystemUpdateId => {
                    let id = self.catalog.system_update_id();
                    tracing::debug!(%peer, id, "published update ID");

                    vec![("Id", id.to_string())]
                }

                Action::GetProtocolInfo => {
                    vec![("Source", "http-get:*:*:*".into()), ("Sink", String::new())]
                }

                Action::GetCurrentConnectionIds => vec![("ConnectionIDs", "0".into())],

                Action::GetCurrentConnectionInfo(id) => {
                    if id != 0 {
                        return Err(Fault { code: 706 });
                    }

                    vec![
                        ("RcsID", "-1".into()),
                        ("AVTransportID", "-1".into()),
                        ("ProtocolInfo", String::new()),
                        ("PeerConnectionManager", String::new()),
                        ("PeerConnectionID", "-1".into()),
                        ("Direction", "Output".into()),
                        ("Status", "Unknown".into()),
                    ]
                }
            };

            let args: Vec<_> = args
                .iter()
                .map(|(name, value)| (*name, value.as_str()))
                .collect();

            Ok(soap(protocol::action_response(
                service,
                name,
                &args,
            )))
        }).await.unwrap_or(Err(Fault { code: 501 }));

        let mut response = result.unwrap_or_else(|fault| {
            tracing::debug!(%peer, ?service, code = fault.code, "SOAP action failed");

            soap(Err(fault))
        });

        if let Some(permit) = permit {
            response.extensions_mut().insert(Arc::new(permit));
        }

        response
    }
}

struct Completion {
    subscriptions: Subscriptions,
    token: Arc<Mutex<Option<Uuid>>>,
}

impl Completion {
    fn finish(&self, success: bool) {
        if let Some(token) = self.token.lock().unwrap().take() {
            self.subscriptions.response_complete(token, success);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(false);
    }
}

async fn connection<C: Catalog, T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    io: T,
    peer: Ipv4Addr,
    server: Arc<Server<C>>,
    shutdown: CancellationToken,
) {
    let completion = Completion {
        subscriptions: server.subscriptions.clone(),
        token: Arc::new(Mutex::new(None)),
    };

    let timing = server.timing;
    let token = completion.token.clone();
    let (start_tx, mut start_rx) = oneshot::channel();
    let start_tx = Arc::new(Mutex::new(Some(start_tx)));
    let admission = Arc::new(Mutex::new(None));
    let admitted = admission.clone();

    let router = Router::new().fallback(move |request: Request| {
        let server = server.clone();
        let token = token.clone();
        let shutdown = shutdown.clone();
        let admitted = admitted.clone();
        let started = Instant::now();
        let media = request.uri().path().starts_with("/media/assets/");

        if let Some(tx) = start_tx.lock().unwrap().take() {
            let _ = tx.send((started, media));
        }

        async move {
            let mut response = server
                .handle(request, peer, &token, &shutdown, started)
                .await;

            // Hyper may consume a body frame while its bytes still await transmission.
            // One request per connection lets admission cover the entire transport.
            *admitted.lock().unwrap() = response
                .extensions_mut()
                .remove::<Arc<OwnedSemaphorePermit>>();

            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));

            response.headers_mut().insert(
                header::SERVER,
                HeaderValue::from_static(crate::server_header()),
            );

            response
        }
    });

    let builder = transport::http1(timing.header);
    let transport = WriteDeadline::new(io, timing.write);

    let connection =
        builder.serve_connection(TokioIo::new(transport), TowerToHyperService::new(router));

    tokio::pin!(connection);
    let mut deadline = Some(Instant::now() + timing.header);
    let mut started = false;

    let success = loop {
        let expired = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;
            start = &mut start_rx, if !started => {
                started = true;

                let Ok((start, media)) = start else {
                    break false;
                };

                deadline = (!media).then_some(start + timing.response);
            }

            _ = expired => {
                tracing::debug!("HTTP connection deadline exceeded");
                break false;
            }

            result = &mut connection => {
                // The handler and entire response can complete in this single poll.
                if !started && let Ok((start, media)) = start_rx.try_recv() {
                    deadline = (!media).then_some(start + timing.response);
                }

                if result.is_err() {
                    tracing::debug!("HTTP connection transport or framing failure");
                }

                // Normal Hyper completion includes flush and poll_shutdown, not just body EOF.
                break result.is_ok() && deadline.is_none_or(|deadline| Instant::now() < deadline);
            }
        }
    };

    completion.finish(success);
}

fn single<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;

    values.next().is_none().then_some(value.trim())
}

fn declared_body(headers: &HeaderMap) -> bool {
    headers.contains_key(header::TRANSFER_ENCODING)
        || (headers.contains_key(header::CONTENT_LENGTH)
            && !single(headers, "content-length")
                .is_some_and(|value| !value.is_empty() && value.bytes().all(|byte| byte == b'0')))
}

fn soap_content_type(value: &str) -> bool {
    crate::mime::parse(value).is_some_and(|mime| mime.eq_ignore_ascii_case("text/xml"))
}

fn empty(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .unwrap()
}

fn method_not_allowed(allow: &'static str) -> Response {
    let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);

    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static(allow));

    response
}

fn xml(status: StatusCode, document: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header(header::CONTENT_LENGTH, document.len())
        .body(Body::from(document))
        .unwrap()
}

fn soap(result: Result<String, Fault>) -> Response {
    let mut response = match result {
        Ok(document) => xml(StatusCode::OK, document),

        Err(fault) => xml(
            StatusCode::INTERNAL_SERVER_ERROR,
            protocol::fault_xml(fault),
        ),
    };

    response
        .headers_mut()
        .insert("ext", HeaderValue::from_static(""));

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::BrowseResult,
        eventing::SUBSCRIPTIONS,
        lifecycle::SHUTDOWN_GRACE,
        media,
        protocol::{BrowseArguments, SOAP_RESPONSE_BYTES},
    };
    use std::{
        io,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadBuf},
        net::TcpStream,
        sync::Notify,
        task::JoinHandle,
        time::timeout,
    };

    const CDS: &str = "/upnp/content-directory/control";
    const CM: &str = "/upnp/connection-manager/control";
    const EVENTS: &str = "/upnp/content-directory/events";
    const ASSET: &str = "10000000-0000-4000-8000-000000000001";

    #[derive(Default)]
    struct TestCatalog {
        calls: AtomicUsize,
        actions: Mutex<Vec<BrowseArguments>>,
        entered: Notify,
        release: Option<Semaphore>,
        panic: bool,
        fail: bool,
    }

    impl Catalog for TestCatalog {
        fn system_update_id(&self) -> u32 {
            42
        }

        async fn browse(&self, arguments: BrowseArguments) -> Result<BrowseResult, Fault> {
            assert!(!self.panic, "test catalog panic");
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.actions.lock().unwrap().push(arguments);
            self.entered.notify_one();

            if let Some(release) = &self.release {
                release.acquire().await.unwrap().forget();
            }

            if self.fail {
                return Err(Fault { code: 501 });
            }

            Ok(BrowseResult {
                objects: vec![protocol::Object {
                    id: "0".into(),
                    parent_id: "-1".into(),
                    title: "A&B".into(),
                    class: "object.container".into(),
                    date: Some("2024-01-01".into()),
                    art: None,
                    child_count: Some(9),
                    resources: vec![protocol::Resource {
                        byte_seek: false,
                        uri: format!("http://192.0.2.1/media/assets/{ASSET}/original"),
                        mime: "image/jpeg".into(),
                        duration: None,
                    }],
                }],
                total_matches: 9,
                update_id: 7,
            })
        }
    }

    fn server(catalog: TestCatalog) -> Server<TestCatalog> {
        Server::new(
            "Test & Media".into(),
            Uuid::nil(),
            catalog,
            MediaProxy::new(
                "http://127.0.0.1:9/api/".parse().unwrap(),
                HeaderValue::from_static("test-key"),
            )
            .unwrap(),
            Subscriptions::new().unwrap(),
        )
    }

    fn request(method: &str, path: &str, headers: &str, body: &str) -> String {
        format!("{method} {path} HTTP/1.1\r\nHost: untrusted.example\r\n{headers}\r\n{body}")
    }

    fn action(path: &str, name: &str, args: &str) -> String {
        let namespace = if path == CDS {
            protocol::CONTENT_DIRECTORY
        } else {
            protocol::CONNECTION_MANAGER
        };

        let body = format!(
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:{name} xmlns:u=\"{namespace}\">{args}</u:{name}></s:Body></s:Envelope>"
        );

        request(
            "POST",
            path,
            &format!(
                "Content-Type: text/xml; charset=\"utf-8\"\r\nSOAPAction: \"{namespace}#{name}\"\r\nContent-Length: {}\r\n",
                body.len()
            ),
            &body,
        )
    }

    fn browse(filter: &str) -> String {
        action(
            CDS,
            "Browse",
            &format!(
                "<ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>{filter}</Filter><StartingIndex>3</StartingIndex><RequestedCount>2</RequestedCount><SortCriteria>-dc:date</SortCriteria>"
            ),
        )
    }

    fn connect(
        server: Arc<Server<TestCatalog>>,
        capacity: usize,
    ) -> (DuplexStream, JoinHandle<()>) {
        let (client, io) = tokio::io::duplex(capacity);

        let task = tokio::spawn(connection(
            io,
            Ipv4Addr::LOCALHOST,
            server,
            CancellationToken::new(),
        ));

        (client, task)
    }

    async fn exchange(server: Arc<Server<TestCatalog>>, request: &str) -> String {
        let (mut client, task) = connect(server, 128 * 1024);
        client.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();

        timeout(Duration::from_secs(3), client.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();

        task.await.unwrap();

        String::from_utf8(bytes).unwrap()
    }

    fn status(response: &str, code: u16) {
        assert!(
            response.starts_with(&format!("HTTP/1.1 {code} ")),
            "{response}"
        );

        assert!(response.contains("connection: close\r\n"), "{response}");
    }

    #[tokio::test]
    async fn descriptions_head_and_exactly_one_request_per_connection() {
        let server = Arc::new(server(TestCatalog::default()));

        for path in [
            "/device.xml",
            "/upnp/content-directory/scpd.xml",
            "/upnp/connection-manager/scpd.xml",
        ] {
            let get = exchange(server.clone(), &request("GET", path, "", "")).await;
            let head = exchange(server.clone(), &request("HEAD", path, "", "")).await;
            status(&get, 200);
            status(&head, 200);
            let (get_headers, body) = get.split_once("\r\n\r\n").unwrap();
            let (head_headers, head_body) = head.split_once("\r\n\r\n").unwrap();
            assert!(head_body.is_empty());
            let length = format!("content-length: {}\r\n", body.len());
            assert!(get_headers.contains(length.trim_end()));
            assert!(head_headers.contains(length.trim_end()));
            assert!(get_headers.contains(crate::server_header()));
            assert!(!body.contains("untrusted.example"));
        }

        let pipelined = request("GET", "/device.xml", "", "").repeat(2);
        let response = exchange(server.clone(), &pipelined).await;
        assert_eq!(response.matches("HTTP/1.1").count(), 1);
        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn local_actions_and_faults_do_not_browse() {
        let server = Arc::new(server(TestCatalog::default()));

        for (path, name, args, code, fragment) in [
            (
                CDS,
                "GetSearchCapabilities",
                "",
                200,
                "<SearchCaps></SearchCaps>",
            ),
            (
                CDS,
                "GetSortCapabilities",
                "",
                200,
                "<SortCaps>dc:date</SortCaps>",
            ),
            (CDS, "GetSystemUpdateID", "", 200, "<Id>42</Id>"),
            (
                CM,
                "GetProtocolInfo",
                "",
                200,
                "<Source>http-get:*:*:*</Source><Sink></Sink>",
            ),
            (
                CM,
                "GetCurrentConnectionIDs",
                "",
                200,
                "<ConnectionIDs>0</ConnectionIDs>",
            ),
            (
                CM,
                "GetCurrentConnectionInfo",
                "<ConnectionID>0</ConnectionID>",
                200,
                "<Direction>Output</Direction><Status>Unknown</Status>",
            ),
            (
                CM,
                "GetCurrentConnectionInfo",
                "<ConnectionID>1</ConnectionID>",
                500,
                "<errorCode>706</errorCode>",
            ),
            (CDS, "Search", "", 500, "<errorCode>401</errorCode>"),
            (
                CDS,
                "GetSystemUpdateID",
                "<Extra/>",
                500,
                "<errorCode>402</errorCode>",
            ),
        ] {
            let response = exchange(server.clone(), &action(path, name, args)).await;
            status(&response, code);
            assert!(response.contains(fragment), "{response}");
            assert!(response.contains("ext: \r\n"));
            assert!(response.contains("content-type: text/xml; charset=\"utf-8\"\r\n"));
        }

        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn browse_uses_selected_count_snapshot_revision_and_protocol_filter() {
        let server = Arc::new(server(TestCatalog::default()));

        for (filter, resources) in [("", false), ("*", true), ("res@protocolInfo", true)] {
            let response = exchange(server.clone(), &browse(filter)).await;
            status(&response, 200);
            assert!(response.contains("<NumberReturned>1</NumberReturned><TotalMatches>9</TotalMatches><UpdateID>7</UpdateID>"));
            assert!(response.contains("A&amp;amp;B"));
            assert_eq!(response.contains("&lt;res "), resources);
        }

        let actions = server.catalog.actions.lock().unwrap();
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].starting_index, 3);
        assert_eq!(actions[0].requested_count, 2);
        assert_eq!(actions[0].sort, Some(true));
    }

    #[test]
    fn soap_mime_parameters_are_parsed_not_prefix_matched() {
        for value in [
            "text/xml",
            "TEXT/XML; Charset=utf-8",
            "text/xml;",
            "text/xml; ",
            "text/xml;; a=x;",
            "text/xml; a=\"with;semicolon\"; b=token",
            "text/xml; a=\"escaped\\\"quote\"",
        ] {
            assert!(soap_content_type(value), "{value}");
        }

        for value in [
            "text/xmljunk",
            "application/soap+xml",
            "text/xml; charset",
            "text/xml; charset=",
            "text/xml; a=\"unterminated",
            "text/xml; a=\"ok\"garbage",
        ] {
            assert!(!soap_content_type(value), "{value}");
        }
    }

    #[tokio::test]
    async fn unsupported_mime_duplicate_action_and_oversized_soap_fail_before_browse() {
        let server = Arc::new(server(TestCatalog::default()));
        let valid = browse("*");

        for (request, expected) in [
            (
                valid.replace("text/xml; charset=\"utf-8\"", "application/xml"),
                415,
            ),
            (
                valid.replace("SOAPAction:", "SOAPAction: duplicate\r\nSOAPAction:"),
                500,
            ),
            (
                request(
                    "POST",
                    CDS,
                    &format!(
                        "Content-Type: text/xml\r\nSOAPAction: \"{}#Browse\"\r\nContent-Length: {}\r\n",
                        protocol::CONTENT_DIRECTORY,
                        SOAP_BODY_BYTES + 1
                    ),
                    "",
                ),
                500,
            ),
            (browse("res@"), 500),
        ] {
            let response = exchange(server.clone(), &request).await;
            status(&response, expected);
        }

        let chunk = " ".repeat(SOAP_BODY_BYTES + 1);

        let chunked = request(
            "POST",
            CDS,
            &format!(
                "Content-Type: text/xml\r\nSOAPAction: \"{}#Browse\"\r\nTransfer-Encoding: chunked\r\n",
                protocol::CONTENT_DIRECTORY
            ),
            &format!("{:x}\r\n{chunk}\r\n0\r\n\r\n", chunk.len()),
        );

        let response = exchange(server.clone(), &chunked).await;
        status(&response, 500);
        assert!(response.contains("<errorCode>501</errorCode>"));
        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_browse_faults_precede_catalog_and_full_admission() {
        let server = Arc::new(server(TestCatalog::default()));

        let _permits = server
            .browses
            .clone()
            .try_acquire_many_owned(BROWSES as u32)
            .unwrap();

        let valid = "<ObjectID>0</ObjectID><BrowseFlag>BrowseMetadata</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/>";

        for (original, replacement, code) in [
            ("<ObjectID>0</ObjectID>", "", 402),
            ("<Filter>*</Filter>", "", 402),
            ("BrowseMetadata", "Unknown", 402),
            ("<StartingIndex>0", "<StartingIndex>1", 402),
            ("<RequestedCount>0", "<RequestedCount>+1", 402),
            (
                "<SortCriteria/>",
                "<SortCriteria>+dc:title</SortCriteria>",
                709,
            ),
            ("<Filter>*", "<Filter>res@@size", 402),
        ] {
            let response = exchange(
                server.clone(),
                &action(CDS, "Browse", &valid.replace(original, replacement)),
            )
            .await;

            status(&response, 500);
            assert!(response.contains(&format!("<errorCode>{code}</errorCode>")));
        }

        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.browses.available_permits(), 0);
        status(&exchange(server.clone(), &browse("*")).await, 503);
    }

    #[tokio::test]
    async fn bodyless_rejection_does_not_contact_upstream_or_reserve_subscription() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut server = server(TestCatalog::default());

        server.media = MediaProxy::new(
            format!("http://{}/api/", upstream.local_addr().unwrap())
                .parse()
                .unwrap(),
            HeaderValue::from_static("test-key"),
        )
        .unwrap();

        let server = Arc::new(server);
        let media = format!("/media/assets/{ASSET}/original");

        for (method, path) in [
            ("GET", "/device.xml"),
            ("HEAD", "/upnp/content-directory/scpd.xml"),
            ("GET", media.as_str()),
            ("HEAD", media.as_str()),
            ("SUBSCRIBE", EVENTS),
        ] {
            for body_header in ["Content-Length: 1\r\n", "Transfer-Encoding: chunked\r\n"] {
                let headers = format!(
                    "{body_header}NT: upnp:event\r\nCALLBACK: <http://127.0.0.1:12345/notify>\r\n"
                );

                let response = exchange(server.clone(), &request(method, path, &headers, "")).await;
                status(&response, 400);
                assert!(!response.contains("sid:"));
            }
        }

        assert!(futures_util::poll!(Box::pin(upstream.accept())).is_pending());
        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);

        for (method, path, code) in [
            ("POST", "/device.xml", 405),
            ("GET", CDS, 405),
            ("GET", EVENTS, 405),
            ("POST", "/unknown", 404),
            ("GET", "/device.xml?other", 404),
        ] {
            let response = exchange(server.clone(), &request(method, path, "", "")).await;
            status(&response, code);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn header_and_body_deadlines_close_without_waiting_for_client_eof() {
        let mut server = server(TestCatalog::default());
        server.timing.header = Duration::from_secs(2);
        server.timing.body = Duration::from_secs(3);
        let server = Arc::new(server);

        for partial in ["", "GET /device.xml HTTP/1.1\r\nHost: incomplete"] {
            let (mut client, task) = connect(server.clone(), 1024);
            client.write_all(partial.as_bytes()).await.unwrap();
            let mut bytes = Vec::new();
            let started = Instant::now();
            client.read_to_end(&mut bytes).await.unwrap();
            assert!(Instant::now() - started <= Duration::from_secs(2));
            assert!(!String::from_utf8_lossy(&bytes).contains("200 OK"));
            task.await.unwrap();
        }

        let request = request(
            "POST",
            CDS,
            &format!(
                "Content-Type: text/xml\r\nSOAPAction: \"{}#Browse\"\r\nContent-Length: 5\r\n",
                protocol::CONTENT_DIRECTORY
            ),
            "<",
        );

        let (mut client, task) = connect(server, 4096);
        client.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        let started = Instant::now();
        client.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(Instant::now() - started, Duration::from_secs(3));
        status(&String::from_utf8(bytes).unwrap(), 408);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn ready_body_can_complete_after_body_deadline_but_processing_is_still_bounded() {
        for (poll_pending_first, late) in [
            (false, Duration::ZERO),
            (true, Duration::from_secs(1)),
            (true, Duration::from_secs(16)),
        ] {
            let server = server(TestCatalog::default());
            let request = browse("*");
            let (_, body) = request.split_once("\r\n\r\n").unwrap();
            let payload = bytes::Bytes::copy_from_slice(body.as_bytes());
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/xml"));

            headers.insert(
                "soapaction",
                format!("\"{}#Browse\"", protocol::CONTENT_DIRECTORY)
                    .parse()
                    .unwrap(),
            );

            let (send, receive) = oneshot::channel();

            let body = Body::from_stream(futures_util::stream::once(async move {
                Ok::<_, io::Error>(receive.await.unwrap())
            }));

            let mut request = Request::builder()
                .method("POST")
                .uri(CDS)
                .body(body)
                .unwrap();
            *request.headers_mut() = headers;
            let token = Mutex::new(None);
            let stop = CancellationToken::new();

            let response =
                server.handle(request, Ipv4Addr::LOCALHOST, &token, &stop, Instant::now());
            tokio::pin!(response);

            if poll_pending_first {
                assert!(futures_util::poll!(&mut response).is_pending());
            }

            // The control future is not spawned: neither body readiness nor timer
            // expiry can poll it before both conditions are ready together.
            tokio::time::advance(server.timing.body + late).await;
            send.send(payload).unwrap();
            let response = response.await;
            let processing_expired = server.timing.body + late >= server.timing.processing;

            assert_eq!(
                response.status(),
                if processing_expired {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::OK
                }
            );

            assert_eq!(
                server.catalog.calls.load(Ordering::SeqCst),
                usize::from(!processing_expired)
            );

            drop(response);
            assert_eq!(server.browses.available_permits(), BROWSES);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn acquired_body_processing_deadline_precedes_parse_result_or_fault() {
        for elapsed in [1, 2, 3] {
            for valid in [false, true] {
                let mut server = server(TestCatalog::default());
                server.timing.processing = Duration::from_secs(2);
                let wire = browse("*");
                let (_, body) = wire.split_once("\r\n\r\n").unwrap();
                let body = if valid { body } else { "<malformed" };
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/xml"));

                headers.insert(
                    "soapaction",
                    format!("{}#Browse", protocol::CONTENT_DIRECTORY)
                        .parse()
                        .unwrap(),
                );

                let response = server
                    .control(
                        Service::ContentDirectory,
                        headers,
                        Body::from(body.to_owned()),
                        Instant::now() - Duration::from_secs(elapsed),
                        Ipv4Addr::LOCALHOST,
                    )
                    .await;

                let accepted = elapsed < 2 && valid;

                assert_eq!(
                    response.status(),
                    if accepted {
                        StatusCode::OK
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                );

                let body = axum::body::to_bytes(response.into_body(), SOAP_RESPONSE_BYTES)
                    .await
                    .unwrap();

                if !accepted {
                    let code = if elapsed < 2 { 402 } else { 501 };

                    assert!(
                        std::str::from_utf8(&body)
                            .unwrap()
                            .contains(&format!("<errorCode>{code}</errorCode>"))
                    );
                }

                assert_eq!(
                    server.catalog.calls.load(Ordering::SeqCst),
                    usize::from(accepted)
                );

                assert_eq!(server.browses.available_permits(), BROWSES);
            }
        }
    }

    #[tokio::test]
    async fn huge_and_aggregate_headers_are_rejected() {
        let server = Arc::new(server(TestCatalog::default()));

        for headers in [
            format!("X-Huge: {}\r\n", "a".repeat(HEADER_BYTES)),
            (0..90)
                .map(|i| format!("X-{i}: {}\r\n", "b".repeat(190)))
                .collect(),
        ] {
            let response =
                exchange(server.clone(), &request("GET", "/device.xml", &headers, "")).await;

            assert!(
                response.is_empty() || response.starts_with("HTTP/1.1 431 "),
                "{response}"
            );
        }

        // Hyper's parser can receive a complete head in one read; also enforce the
        // aggregate limit at the service boundary, independently of buffer growth.
        let mut request = Request::builder()
            .uri("/device.xml")
            .body(Body::empty())
            .unwrap();

        for _ in 0..90 {
            request
                .headers_mut()
                .append("x-large", HeaderValue::from_str(&"a".repeat(190)).unwrap());
        }

        let response = server
            .handle(
                request,
                Ipv4Addr::LOCALHOST,
                &Mutex::new(None),
                &CancellationToken::new(),
                Instant::now(),
            )
            .await;

        assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_body_repolled_after_processing_deadline_still_sends_408() {
        let server = Arc::new(server(TestCatalog::default()));
        let (mut client, io) = tokio::io::duplex(4096);

        client.write_all(request(
            "POST", CDS,
            &format!("Content-Type: text/xml\r\nSOAPAction: \"{}#Browse\"\r\nContent-Length: 100\r\n", protocol::CONTENT_DIRECTORY),
            "<",
        ).as_bytes()).await.unwrap();

        let mut task = Box::pin(connection(
            io,
            Ipv4Addr::LOCALHOST,
            server.clone(),
            CancellationToken::new(),
        ));
        assert!(futures_util::poll!(&mut task).is_pending());
        assert!(futures_util::poll!(&mut task).is_pending());
        tokio::time::advance(Duration::from_secs(26)).await;

        let (_, response) = tokio::join!(task, async {
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();

            response
        });

        status(&response, 408);
        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.browses.available_permits(), BROWSES);
    }

    #[tokio::test]
    async fn browse_admission_is_immediate_and_small_actions_bypass_it() {
        let server = Arc::new(server(TestCatalog {
            release: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        }));

        let mut clients = Vec::new();

        for _ in 0..BROWSES {
            let (mut client, task) = connect(server.clone(), 4096);
            client.write_all(browse("*").as_bytes()).await.unwrap();
            server.catalog.entered.notified().await;
            clients.push((client, task));
        }

        status(&exchange(server.clone(), &browse("*")).await, 503);

        status(
            &exchange(server.clone(), &action(CDS, "GetSystemUpdateID", "")).await,
            200,
        );

        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), BROWSES);

        server
            .catalog
            .release
            .as_ref()
            .unwrap()
            .add_permits(BROWSES);

        for (mut client, task) in clients {
            let mut bytes = Vec::new();
            client.read_to_end(&mut bytes).await.unwrap();
            status(&String::from_utf8(bytes).unwrap(), 200);
            task.await.unwrap();
        }

        assert_eq!(server.browses.available_permits(), BROWSES);
    }

    #[tokio::test(start_paused = true)]
    async fn browse_admission_covers_buffered_transmission_and_cleanup() {
        for finish in ["read", "disconnect", "deadline", "cancel"] {
            let mut server = server(TestCatalog::default());
            server.timing.response = Duration::from_secs(2);
            let server = Arc::new(server);
            let mut clients = Vec::new();

            for _ in 0..BROWSES {
                let (mut client, task) = connect(server.clone(), 64);
                client.write_all(browse("*").as_bytes()).await.unwrap();
                status(&read_headers(&mut client).await, 200);
                assert!(!task.is_finished());
                clients.push((client, task));
            }

            assert_eq!(server.browses.available_permits(), 0, "{finish}");
            status(&exchange(server.clone(), &browse("*")).await, 503);

            status(
                &exchange(server.clone(), &action(CDS, "GetSystemUpdateID", "")).await,
                200,
            );

            if finish == "deadline" {
                tokio::time::advance(Duration::from_secs(2)).await;
            }

            for (mut client, task) in clients {
                match finish {
                    "read" => {
                        let mut body = String::new();
                        client.read_to_string(&mut body).await.unwrap();
                        assert!(body.ends_with("</s:Envelope>"));
                    }

                    "disconnect" => drop(client),

                    "cancel" => task.abort(),

                    "deadline" => {}

                    _ => unreachable!(),
                }

                assert_eq!(task.await.is_err(), finish == "cancel");
            }

            assert_eq!(server.browses.available_permits(), BROWSES);
            status(&exchange(server.clone(), &browse("*")).await, 200);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn browse_permit_survives_body_eof_until_transport_shutdown() {
        for outcome in ["success", "failure", "timeout"] {
            let mut server = server(TestCatalog {
                release: (outcome == "timeout").then(|| Semaphore::new(0)),
                fail: outcome == "failure",
                ..TestCatalog::default()
            });

            server.timing.processing = Duration::from_millis(1);
            let server = Arc::new(server);
            let (mut client, io) = tokio::io::duplex(4096);
            let entered = Arc::new(Notify::new());
            let (release, gate) = oneshot::channel();

            let task = tokio::spawn(connection(
                GatedIo {
                    io,
                    shutdown_entered: entered.clone(),
                    allow_shutdown: gate,
                    shutdown_completed: Arc::new(AtomicBool::new(false)),
                },
                Ipv4Addr::LOCALHOST,
                server.clone(),
                CancellationToken::new(),
            ));

            client.write_all(browse("*").as_bytes()).await.unwrap();
            entered.notified().await;
            status(
                &read_headers(&mut client).await,
                if outcome == "success" { 200 } else { 500 },
            );
            assert_eq!(server.browses.available_permits(), BROWSES - 1);
            release.send(true).unwrap();
            task.await.unwrap();
            assert_eq!(server.browses.available_permits(), BROWSES);
        }
    }

    #[tokio::test]
    async fn media_admission_covers_head_conditional_and_error_transport_shutdown() {
        for (method, upstream_status, local_status) in
            [("HEAD", 200, 200), ("GET", 304, 304), ("GET", 403, 502)]
        {
            let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut server = server(TestCatalog::default());

            server.media = MediaProxy::new(
                format!("http://{}/api/", upstream.local_addr().unwrap())
                    .parse()
                    .unwrap(),
                HeaderValue::from_static("test-key"),
            )
            .unwrap();

            let server = Arc::new(server);
            let body = if method == "HEAD" || upstream_status == 304 {
                ""
            } else {
                "abc"
            };

            let wire = format!(
                "HTTP/1.1 {upstream_status} Response\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\nConnection: close\r\n\r\n{body}"
            );

            let remote = tokio::spawn(async move {
                for _ in 0..media::OPERATIONS {
                    let (mut io, _) = upstream.accept().await.unwrap();
                    read_headers(&mut io).await;
                    io.write_all(wire.as_bytes()).await.unwrap();
                    io.shutdown().await.unwrap();
                }
            });

            let mut connections = Vec::new();
            let path = format!("/media/assets/{ASSET}/original");

            for _ in 0..media::OPERATIONS {
                let (mut client, io) = tokio::io::duplex(4096);
                let entered = Arc::new(Notify::new());
                let (release, gate) = oneshot::channel();

                let task = tokio::spawn(connection(
                    GatedIo {
                        io,
                        shutdown_entered: entered.clone(),
                        allow_shutdown: gate,
                        shutdown_completed: Arc::new(AtomicBool::new(false)),
                    },
                    Ipv4Addr::LOCALHOST,
                    server.clone(),
                    CancellationToken::new(),
                ));

                client
                    .write_all(request(method, &path, "", "").as_bytes())
                    .await
                    .unwrap();
                entered.notified().await;
                status(&read_headers(&mut client).await, local_status);
                connections.push((client, task, release));
            }

            remote.await.unwrap();
            status(
                &exchange(server.clone(), &request("GET", &path, "", "")).await,
                503,
            );

            for (_client, task, release) in connections {
                release.send(true).unwrap();
                task.await.unwrap();
            }

            assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn tcp_disconnect_cancels_stalled_media_and_releases_admission() {
        for body_started in [false, true] {
            let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut server = server(TestCatalog::default());

            server.media = MediaProxy::new(
                format!("http://{}/api/", upstream.local_addr().unwrap())
                    .parse()
                    .unwrap(),
                HeaderValue::from_static("test-key"),
            )
            .unwrap();

            let server = Arc::new(server);
            let mut connections = Vec::new();

            for _ in 0..media::OPERATIONS {
                let mut client = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();

                let (socket, _) = listener.accept().await.unwrap();

                let task = tokio::spawn(connection(
                    socket,
                    Ipv4Addr::LOCALHOST,
                    server.clone(),
                    CancellationToken::new(),
                ));

                client
                    .write_all(
                        request("GET", &format!("/media/assets/{ASSET}/original"), "", "")
                            .as_bytes(),
                    )
                    .await
                    .unwrap();

                let (mut remote, _) = upstream.accept().await.unwrap();

                assert!(
                    read_headers(&mut remote)
                        .await
                        .starts_with("GET /api/assets/")
                );

                if body_started {
                    remote.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 2\r\nConnection: close\r\n\r\na").await.unwrap();
                    status(&read_headers(&mut client).await, 200);
                    assert_eq!(client.read_u8().await.unwrap(), b'a');
                }

                connections.push((client, task, remote));
            }

            let response = server
                .media
                .serve(ASSET, "original", Method::GET, HeaderMap::new())
                .await;

            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

            for (client, task, mut remote) in connections {
                socket2::SockRef::from(&client)
                    .set_linger(Some(Duration::ZERO))
                    .unwrap();

                drop(client);

                timeout(Duration::from_secs(2), task)
                    .await
                    .expect("client reset must cancel without waiting for upstream progress")
                    .unwrap();

                let closed = timeout(Duration::from_secs(2), remote.read(&mut [0]))
                    .await
                    .unwrap();

                assert!(
                    matches!(closed, Ok(0))
                        || closed.is_err_and(|e| e.kind() == io::ErrorKind::ConnectionReset)
                );
            }

            // A new admitted request proves that disconnected operations released capacity.
            let media = server.media.clone();

            let task = tokio::spawn(async move {
                media
                    .serve(ASSET, "original", Method::HEAD, HeaderMap::new())
                    .await
            });

            let (mut remote, _) = timeout(Duration::from_secs(2), upstream.accept())
                .await
                .unwrap()
                .unwrap();

            assert!(
                read_headers(&mut remote)
                    .await
                    .starts_with("HEAD /api/assets/")
            );

            remote.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").await.unwrap();
            assert_eq!(task.await.unwrap().status(), StatusCode::OK);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn processing_deadline_returns_fault_and_releases_browse_permit() {
        let mut server = server(TestCatalog {
            release: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        });

        server.timing.processing = Duration::from_secs(2);
        let server = Arc::new(server);
        let response = exchange(server.clone(), &browse("*")).await;
        status(&response, 500);
        assert!(response.contains("<errorCode>501</errorCode>"));
        assert_eq!(server.browses.available_permits(), BROWSES);
    }

    #[tokio::test(start_paused = true)]
    async fn total_deadline_covers_response_transmission_after_headers() {
        let mut server = server(TestCatalog::default());
        server.timing.response = Duration::from_secs(2);
        let (mut client, task) = connect(Arc::new(server), 64);

        client
            .write_all(request("GET", "/device.xml", "", "").as_bytes())
            .await
            .unwrap();

        let started = Instant::now();

        // Do not read: the descriptor cannot fit in the bounded transport buffer.
        task.await.unwrap();
        assert_eq!(Instant::now() - started, Duration::from_secs(2));
        let mut partial = String::new();
        client.read_to_string(&mut partial).await.unwrap();
        assert!(partial.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(!partial.contains("</root>"));
    }

    #[tokio::test]
    async fn media_routes_stream_past_control_deadline_and_head_remains_bodyless() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut server = server(TestCatalog::default());
        server.timing.response = Duration::from_secs(2);

        server.media = MediaProxy::new(
            format!("http://{}/api/", upstream.local_addr().unwrap())
                .parse()
                .unwrap(),
            HeaderValue::from_static("test-key"),
        )
        .unwrap();

        let server = Arc::new(server);
        let (release, wait) = oneshot::channel();

        let upstream_task = tokio::spawn(async move {
            let (mut stream, _) = upstream.accept().await.unwrap();
            let headers = read_headers(&mut stream).await;

            assert!(headers.starts_with(&format!(
                "GET /api/assets/{ASSET}/thumbnail?size=preview&edited=true HTTP/1.1\r\n"
            )));

            assert!(headers.contains("x-api-key: test-key\r\n"));

            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 2\r\nConnection: close\r\n\r\na").await.unwrap();
            wait.await.unwrap();
            stream.write_all(b"b").await.unwrap();
            stream.shutdown().await.unwrap();

            let (mut stream, _) = upstream.accept().await.unwrap();
            let headers = read_headers(&mut stream).await;

            assert!(
                headers.starts_with(&format!("HEAD /api/assets/{ASSET}/original HTTP/1.1\r\n"))
            );

            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 123\r\nConnection: close\r\n\r\n").await.unwrap();
        });

        let (mut client, task) = connect(server.clone(), 4096);

        client
            .write_all(request("GET", &format!("/media/assets/{ASSET}/preview"), "", "").as_bytes())
            .await
            .unwrap();

        let headers = read_headers(&mut client).await;
        status(&headers, 200);
        assert!(!headers.contains("test-key"));
        assert_eq!(client.read_u8().await.unwrap(), b'a');

        // Pause only after real sockets are connected. Advance below the upstream
        // idle limit, but far past the control-response clock.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::resume();
        release.send(()).unwrap();
        let mut body = Vec::new();
        client.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, b"b");
        task.await.unwrap();

        let head = exchange(
            server.clone(),
            &request("HEAD", &format!("/media/assets/{ASSET}/original"), "", ""),
        )
        .await;

        status(&head, 200);
        assert!(head.contains("content-length: 123\r\n"));
        assert!(head.ends_with("\r\n\r\n"));
        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
        upstream_task.await.unwrap();
    }

    struct GatedIo {
        io: DuplexStream,
        shutdown_entered: Arc<Notify>,
        allow_shutdown: oneshot::Receiver<bool>,
        shutdown_completed: Arc<AtomicBool>,
    }

    impl AsyncRead for GatedIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for GatedIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.io).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdown_entered.notify_one();

            match Pin::new(&mut self.allow_shutdown).poll(cx) {
                Poll::Ready(Ok(true)) => {
                    let result = Pin::new(&mut self.io).poll_shutdown(cx);

                    if matches!(result, Poll::Ready(Ok(()))) {
                        self.shutdown_completed.store(true, Ordering::SeqCst);
                    }

                    result
                }

                Poll::Ready(_) => Poll::Ready(Err(io::Error::other("test shutdown failure"))),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn subscribe(callback: std::net::SocketAddr) -> String {
        request(
            "SUBSCRIBE",
            EVENTS,
            &format!(
                "NT: upnp:event\r\nCALLBACK: <http://{callback}/notify>\r\nContent-Length: 0\r\n"
            ),
            "",
        )
    }

    fn sid(response: &str) -> &str {
        response
            .lines()
            .find_map(|line| line.strip_prefix("sid: "))
            .unwrap()
    }

    async fn read_headers(client: &mut (impl AsyncRead + Unpin)) -> String {
        let mut bytes = Vec::new();

        while !bytes.ends_with(b"\r\n\r\n") {
            bytes.push(client.read_u8().await.unwrap());
        }

        String::from_utf8(bytes).unwrap()
    }

    #[tokio::test]
    async fn regression_upgrade_subscribe_is_rejected_before_reserving() {
        let server = Arc::new(server(TestCatalog::default()));
        let callbacks = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, io) = tokio::io::duplex(4096);
        let (_allow_shutdown, gate) = oneshot::channel();
        let shutdown_entered = Arc::new(Notify::new());
        let shutdown_completed = Arc::new(AtomicBool::new(false));

        let task = tokio::spawn(connection(
            GatedIo {
                io,
                shutdown_entered: shutdown_entered.clone(),
                allow_shutdown: gate,
                shutdown_completed: shutdown_completed.clone(),
            },
            Ipv4Addr::LOCALHOST,
            server.clone(),
            CancellationToken::new(),
        ));

        let request = subscribe(callbacks.local_addr().unwrap()).replace(
            "Content-Length: 0\r\n",
            "Content-Length: 0\r\nConnection: upgrade\r\nUpgrade: websocket\r\n",
        );

        client.write_all(request.as_bytes()).await.unwrap();
        let response = read_headers(&mut client).await;
        status(&response, 400);
        assert!(!response.contains("sid:"));
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();

        // Hyper has already recorded the upgrade and skips poll_shutdown even
        // for this non-101 response. Rejection must leave no GENA obligation.
        assert!(!shutdown_completed.load(Ordering::SeqCst));
        assert!(futures_util::poll!(Box::pin(shutdown_entered.notified())).is_pending());
        let mut headers = HeaderMap::new();
        headers.insert("nt", HeaderValue::from_static("upnp:event"));

        headers.insert(
            "callback",
            format!("<http://{}/notify>", callbacks.local_addr().unwrap())
                .parse()
                .unwrap(),
        );

        for _ in 0..SUBSCRIPTIONS {
            let (response, token) = server.subscriptions.request(
                Service::ContentDirectory,
                Ipv4Addr::LOCALHOST,
                &Method::from_bytes(b"SUBSCRIBE").unwrap(),
                &headers,
            );

            assert_eq!(response.status(), StatusCode::OK);
            assert!(token.is_some());
        }

        assert_eq!(server.catalog.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn initial_notify_requires_successful_transport_shutdown_not_body_exhaustion() {
        let server = Arc::new(server(TestCatalog::default()));
        let scheduler_stop = CancellationToken::new();
        let scheduler = tokio::spawn(server.subscriptions.clone().run(scheduler_stop.clone()));
        let callbacks = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (mut client, io) = tokio::io::duplex(4096);
        let shutdown_entered = Arc::new(Notify::new());
        let shutdown_completed = Arc::new(AtomicBool::new(false));
        let (allow_shutdown, gate) = oneshot::channel();

        let task = tokio::spawn(connection(
            GatedIo {
                io,
                shutdown_entered: shutdown_entered.clone(),
                allow_shutdown: gate,
                shutdown_completed: shutdown_completed.clone(),
            },
            Ipv4Addr::LOCALHOST,
            server.clone(),
            CancellationToken::new(),
        ));

        client
            .write_all(subscribe(callbacks.local_addr().unwrap()).as_bytes())
            .await
            .unwrap();

        let response = read_headers(&mut client).await;
        status(&response, 200);
        assert!(response.contains("content-length: 0\r\n"));
        shutdown_entered.notified().await;
        assert!(!shutdown_completed.load(Ordering::SeqCst));

        // The complete bodyless response is already readable, but EOF is impossible
        // until this gate opens. Poll the scheduler while that condition holds.
        tokio::task::yield_now().await;
        assert!(futures_util::poll!(Box::pin(callbacks.accept())).is_pending());
        assert!(futures_util::poll!(Box::pin(client.read_u8())).is_pending());
        allow_shutdown.send(true).unwrap();

        let (mut callback, _) = timeout(Duration::from_secs(3), callbacks.accept())
            .await
            .unwrap()
            .unwrap();

        assert!(shutdown_completed.load(Ordering::SeqCst));

        // Before reading any NOTIFY bytes, assert the response transport is *already*
        // at EOF without yielding. This tests causal shutdown, not packet arrival timing.
        let mut byte = [0];

        assert!(matches!(
            futures_util::poll!(Box::pin(client.read(&mut byte))),
            Poll::Ready(Ok(0))
        ));

        let notify = read_headers(&mut callback).await;
        assert!(notify.starts_with("NOTIFY /notify HTTP/1.1\r\n"));
        assert!(notify.contains("seq: 0\r\n"));
        assert!(notify.contains(&format!("sid: {}\r\n", sid(&response))));

        callback
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        task.await.unwrap();
        scheduler_stop.cancel();
        scheduler.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_timed_out_and_cancelled_gena_responses_release_reservations() {
        for failure in ["io", "deadline", "cancel"] {
            let mut server = server(TestCatalog::default());
            server.timing.response = Duration::from_secs(2);
            let server = Arc::new(server);
            let callbacks = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (mut client, io) = tokio::io::duplex(4096);
            let shutdown_entered = Arc::new(Notify::new());
            let (allow_shutdown, gate) = oneshot::channel();

            let task = tokio::spawn(connection(
                GatedIo {
                    io,
                    shutdown_entered: shutdown_entered.clone(),
                    allow_shutdown: gate,
                    shutdown_completed: Arc::new(AtomicBool::new(false)),
                },
                Ipv4Addr::LOCALHOST,
                server.clone(),
                CancellationToken::new(),
            ));

            client
                .write_all(subscribe(callbacks.local_addr().unwrap()).as_bytes())
                .await
                .unwrap();

            let response = read_headers(&mut client).await;
            status(&response, 200);
            shutdown_entered.notified().await;

            match failure {
                "io" => allow_shutdown.send(false).unwrap(),
                "deadline" => tokio::time::advance(Duration::from_secs(2)).await,
                "cancel" => task.abort(),
                _ => unreachable!(),
            }

            let joined = task.await;
            assert_eq!(joined.is_err(), failure == "cancel");

            let renewal = request(
                "SUBSCRIBE",
                EVENTS,
                &format!("SID: {}\r\n", sid(&response)),
                "",
            );

            status(&exchange(server.clone(), &renewal).await, 412);
            assert!(futures_util::poll!(Box::pin(callbacks.accept())).is_pending());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn write_deadline_measures_blocked_writes_not_stream_duration() {
        let (mut reader, writer) = tokio::io::duplex(1);

        let mut writer = WriteDeadline::new(writer, Duration::from_secs(2));

        writer.write_all(b"a").await.unwrap();
        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(reader.read_u8().await.unwrap(), b'a');
        writer.write_all(b"b").await.unwrap();
        let mut blocked = Box::pin(writer.write_all(b"c"));
        assert!(futures_util::poll!(&mut blocked).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(reader.read_u8().await.unwrap(), b'b');
        blocked.await.unwrap();
        tokio::time::advance(Duration::from_secs(100)).await;
        let started = Instant::now();
        let error = writer.write_all(b"d").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(Instant::now() - started, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn production_listener_overload_and_shutdown_are_bounded() {
        let server = server(TestCatalog {
            release: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.run(listener, shutdown.clone(), Duration::from_millis(30)));
        let mut clients = Vec::new();

        // An interim response proves each connection has been accepted and is
        // retained in body acquisition, rather than merely in the listen backlog.
        for _ in 0..CONNECTIONS {
            let mut client = TcpStream::connect(address).await.unwrap();

            let headers = format!(
                "Content-Type: text/xml\r\nSOAPAction: \"{}#Browse\"\r\nContent-Length: 1\r\nExpect: 100-continue\r\n",
                protocol::CONTENT_DIRECTORY
            );

            client
                .write_all(request("POST", CDS, &headers, "").as_bytes())
                .await
                .unwrap();

            assert!(
                read_headers(&mut client)
                    .await
                    .starts_with("HTTP/1.1 100 Continue")
            );

            clients.push(client);
        }

        let mut rejected = TcpStream::connect(address).await.unwrap();
        let mut bytes = Vec::new();

        timeout(Duration::from_secs(3), rejected.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();

        // try_write may legitimately fail before readiness: immediate close is safe.
        assert!(bytes.is_empty() || String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 503 "));
        shutdown.cancel();

        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        for mut client in clients {
            let mut bytes = Vec::new();

            let _ = timeout(Duration::from_secs(1), client.read_to_end(&mut bytes))
                .await
                .unwrap();
        }

        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn connection_task_panic_is_fatal_and_admitted_response_can_finish_during_grace() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let panicking_server = server(TestCatalog {
            panic: true,
            ..TestCatalog::default()
        });

        let task =
            tokio::spawn(panicking_server.run(listener, CancellationToken::new(), SHUTDOWN_GRACE));
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(browse("*").as_bytes()).await.unwrap();

        assert!(
            timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server(TestCatalog::default()).run(
            listener,
            shutdown.clone(),
            SHUTDOWN_GRACE,
        ));
        let mut client = TcpStream::connect(address).await.unwrap();
        let whole = action(CDS, "GetSystemUpdateID", "");
        let (headers, body) = whole.split_once("\r\n\r\n").unwrap();

        client
            .write_all(format!("{headers}\r\nExpect: 100-continue\r\n\r\n").as_bytes())
            .await
            .unwrap();

        assert!(
            read_headers(&mut client)
                .await
                .starts_with("HTTP/1.1 100 Continue")
        );

        shutdown.cancel();
        client.write_all(body.as_bytes()).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        status(&response, 200);
        task.await.unwrap().unwrap();
    }
}
