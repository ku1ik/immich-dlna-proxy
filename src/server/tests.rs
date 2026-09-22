use std::{io, sync::Mutex};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Notify, oneshot},
};

use super::*;
use crate::{
    catalog::{BrowseQuery, BrowseResult, Object},
    eventing::SUBSCRIPTIONS,
};

const CDS: &str = "/upnp/content-directory/control";
const EVENTS: &str = "/upnp/content-directory/events";

#[derive(Default)]
struct TestCatalog {
    actions: Mutex<Vec<BrowseQuery>>,
    entered: Notify,
    release: Option<Semaphore>,
    block: Option<Duration>,
}

impl Catalog for TestCatalog {
    fn system_update_id(&self) -> u32 {
        42
    }

    async fn browse(&self, arguments: BrowseQuery) -> Result<BrowseResult, Fault> {
        self.actions.lock().unwrap().push(arguments);
        self.entered.notify_one();

        if let Some(duration) = self.block {
            std::thread::sleep(duration);
        }

        if let Some(release) = &self.release {
            release.acquire().await.unwrap().forget();
        }

        Ok(BrowseResult {
            objects: vec![Object {
                id: "0".into(),
                parent_id: "-1".into(),
                title: "A&B".into(),
                class: "object.container".into(),
                date: Some("2024-01-01".into()),
                art: None,
                child_count: Some(9),
                resources: Vec::new(),
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

fn request(method: Method, path: &str, body: Body) -> Request {
    Request::builder()
        .method(method)
        .uri(path)
        .body(body)
        .unwrap()
}

fn action(path: &str, name: &str, args: &str) -> Request {
    let namespace = if path == CDS {
        protocol::CONTENT_DIRECTORY
    } else {
        protocol::CONNECTION_MANAGER
    };

    let body = format!(
        "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:{name} xmlns:u=\"{namespace}\">{args}</u:{name}></s:Body></s:Envelope>"
    );

    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(header::CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header("soapaction", format!("\"{namespace}#{name}\""))
        .header(header::CONTENT_LENGTH, body.len())
        .body(Body::from(body))
        .unwrap()
}

fn browse() -> Request {
    action(
        CDS,
        "Browse",
        "<ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>3</StartingIndex><RequestedCount>2</RequestedCount><SortCriteria>-dc:date</SortCriteria>",
    )
}

async fn body(response: Response) -> String {
    String::from_utf8(
        axum::body::to_bytes(response.into_body(), protocol::SOAP_RESPONSE_BYTES)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn descriptions_head_and_common_header() {
    let server = server(TestCatalog::default());
    let peer = ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 12345)));

    for path in [
        "/device.xml",
        "/upnp/content-directory/scpd.xml",
        "/upnp/connection-manager/scpd.xml",
    ] {
        let get = server
            .handle(peer, request(Method::GET, path, Body::empty()))
            .await;

        let length = get.headers()[header::CONTENT_LENGTH].clone();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers()[header::SERVER], crate::server_header());
        assert!(!body(get).await.contains("untrusted.example"));

        let head = server
            .handle(peer, request(Method::HEAD, path, Body::empty()))
            .await;
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(head.headers()[header::CONTENT_LENGTH], length);
        assert!(body(head).await.is_empty());
    }
}

#[tokio::test]
async fn local_actions_and_faults_do_not_browse() {
    let server = server(TestCatalog::default());

    for (path, name, args, status, fragment) in [
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
            "/upnp/connection-manager/control",
            "GetProtocolInfo",
            "",
            200,
            "<Source>http-get:*:*:*</Source><Sink></Sink>",
        ),
        (
            "/upnp/connection-manager/control",
            "GetCurrentConnectionInfo",
            "<ConnectionID>1</ConnectionID>",
            500,
            "<errorCode>706</errorCode>",
        ),
        (CDS, "Search", "", 500, "<errorCode>401</errorCode>"),
    ] {
        let response = server
            .route(action(path, name, args), Ipv4Addr::LOCALHOST)
            .await;

        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["ext"], "");
        assert!(body(response).await.contains(fragment));
    }

    assert!(server.catalog.actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn browse_serializes_snapshot_and_releases_permit_with_response() {
    let server = server(TestCatalog::default());
    let response = server.route(browse(), Ipv4Addr::LOCALHOST).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(server.browses.available_permits(), BROWSES);

    let document = body(response).await;
    assert!(document.contains("<NumberReturned>1</NumberReturned>"));
    assert!(document.contains("<TotalMatches>9</TotalMatches>"));
    assert!(document.contains("<UpdateID>7</UpdateID>"));
    assert!(document.contains("A&amp;amp;B"));

    let actions = server.catalog.actions.lock().unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].starting_index, 3);
    assert_eq!(actions[0].requested_count, 2);
    assert_eq!(actions[0].sort, Some(true));
}

#[test]
fn soap_mime_parameters_are_parsed_not_prefix_matched() {
    for value in ["text/xml", "TEXT/XML; Charset=utf-8"] {
        assert!(soap_content_type(value), "{value}");
    }

    for value in ["text/xmljunk", "application/soap+xml"] {
        assert!(!soap_content_type(value), "{value}");
    }
}

#[tokio::test]
async fn routing_rejects_invalid_headers_bodies_methods_and_peers() {
    let server = server(TestCatalog::default());
    let peer = ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 12345)));

    let mut oversized = request(Method::GET, "/device.xml", Body::empty());

    for _ in 0..90 {
        oversized
            .headers_mut()
            .append("x-large", HeaderValue::from_str(&"a".repeat(190)).unwrap());
    }

    assert_eq!(
        server.handle(peer, oversized).await.status(),
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
    );

    let mut upgrade = request(Method::GET, "/device.xml", Body::empty());
    upgrade
        .headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    assert_eq!(
        server.handle(peer, upgrade).await.status(),
        StatusCode::BAD_REQUEST
    );

    let mut declared = request(Method::GET, "/device.xml", Body::empty());
    declared
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("1"));
    assert_eq!(
        server.handle(peer, declared).await.status(),
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        server
            .handle(peer, request(Method::POST, "/device.xml", Body::empty()))
            .await
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );

    let ipv6 = ConnectInfo("[::1]:12345".parse().unwrap());

    let response = server
        .handle(ipv6, request(Method::GET, "/device.xml", Body::empty()))
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()[header::SERVER], crate::server_header());
    assert!(server.catalog.actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routing_preserves_path_body_and_method_error_precedence() {
    let server = server(TestCatalog::default());

    for (path, method, declared, status, allow) in [
        ("/unknown", Method::POST, true, 404, None),
        ("/device.xml?query", Method::POST, true, 404, None),
        ("/device.xml", Method::POST, true, 400, None),
        ("/device.xml", Method::POST, false, 405, Some("GET, HEAD")),
        (
            "/upnp/content-directory/scpd.xml",
            Method::POST,
            true,
            400,
            None,
        ),
        (
            "/upnp/connection-manager/scpd.xml",
            Method::POST,
            false,
            405,
            Some("GET, HEAD"),
        ),
        (CDS, Method::GET, true, 405, Some("POST")),
        (CDS, Method::POST, true, 415, None),
        (EVENTS, Method::POST, true, 400, None),
        (
            EVENTS,
            Method::POST,
            false,
            405,
            Some("SUBSCRIBE, UNSUBSCRIBE"),
        ),
        ("/media/assets/bad", Method::POST, true, 404, None),
        ("/media/assets/bad/unknown", Method::POST, true, 400, None),
        (
            "/media/assets/bad/unknown",
            Method::POST,
            false,
            405,
            Some("GET, HEAD"),
        ),
        ("/media/assets/bad/unknown", Method::GET, false, 404, None),
    ] {
        let mut request = request(method, path, Body::empty());

        if declared {
            request
                .headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from_static("1"));
        }

        let response = server.route(request, Ipv4Addr::LOCALHOST).await;
        assert_eq!(response.status().as_u16(), status, "{path}");

        assert_eq!(
            response
                .headers()
                .get(header::ALLOW)
                .map(|value| value.to_str().unwrap()),
            allow,
            "{path}"
        );
    }

    assert!(server.catalog.actions.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn soap_body_bounds_and_processing_deadlines_are_retained() {
    let server = server(TestCatalog::default());

    let (send, receive) = oneshot::channel::<bytes::Bytes>();
    let stalled_body = Body::from_stream(futures_util::stream::once(async move {
        Ok::<_, io::Error>(receive.await.unwrap())
    }));

    let mut stalled = action(CDS, "Browse", "");
    *stalled.body_mut() = stalled_body;
    let response = server.route(stalled, Ipv4Addr::LOCALHOST);
    tokio::pin!(response);
    assert!(futures_util::poll!(&mut response).is_pending());
    tokio::time::advance(BODY_TIMEOUT).await;
    assert_eq!(response.await.status(), StatusCode::REQUEST_TIMEOUT);
    drop(send);

    let (parts, exact_body) = action(CDS, "GetSystemUpdateID", "").into_parts();

    let mut exact_body = axum::body::to_bytes(exact_body, SOAP_BODY_BYTES)
        .await
        .unwrap()
        .to_vec();

    exact_body.resize(SOAP_BODY_BYTES, b' ');
    let mut exact = Request::from_parts(parts, Body::from(exact_body));

    exact.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&SOAP_BODY_BYTES.to_string()).unwrap(),
    );

    let response = server.route(exact, Ipv4Addr::LOCALHOST).await;
    assert_eq!(response.status(), StatusCode::OK);

    let mut declared = action(CDS, "GetSystemUpdateID", "");

    declared.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&(SOAP_BODY_BYTES + 1).to_string()).unwrap(),
    );

    let response = server.route(declared, Ipv4Addr::LOCALHOST).await;
    assert!(body(response).await.contains("<errorCode>501</errorCode>"));

    let mut streamed = action(CDS, "GetSystemUpdateID", "");
    streamed.headers_mut().remove(header::CONTENT_LENGTH);
    *streamed.body_mut() = Body::from(vec![b' '; SOAP_BODY_BYTES + 1]);
    let response = server.route(streamed, Ipv4Addr::LOCALHOST).await;
    assert!(body(response).await.contains("<errorCode>501</errorCode>"));

    let request = browse();
    let (parts, request_body) = request.into_parts();

    let response = server
        .control(
            Service::ContentDirectory,
            parts.headers,
            request_body,
            Instant::now() - CONTROL_PROCESSING_TIMEOUT,
            Ipv4Addr::LOCALHOST,
        )
        .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body(response).await.contains("<errorCode>501</errorCode>"));
    assert_eq!(server.browses.available_permits(), BROWSES);
    assert!(server.catalog.actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn synchronous_completion_after_control_deadline_is_rejected() {
    let server = server(TestCatalog {
        block: Some(Duration::from_millis(200)),
        ..TestCatalog::default()
    });

    let (parts, request_body) = browse().into_parts();
    let started = Instant::now() - CONTROL_PROCESSING_TIMEOUT + Duration::from_millis(100);

    let response = server
        .control(
            Service::ContentDirectory,
            parts.headers,
            request_body,
            started,
            Ipv4Addr::LOCALHOST,
        )
        .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body(response).await.contains("<errorCode>501</errorCode>"));
    assert_eq!(server.browses.available_permits(), BROWSES);
    assert_eq!(server.catalog.actions.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn browse_admission_is_immediate_and_local_actions_bypass_it() {
    let server = Arc::new(server(TestCatalog {
        release: Some(Semaphore::new(0)),
        ..TestCatalog::default()
    }));

    let mut active = Vec::new();

    for _ in 0..BROWSES {
        let worker = server.clone();

        active.push(tokio::spawn(async move {
            worker.route(browse(), Ipv4Addr::LOCALHOST).await
        }));

        server.catalog.entered.notified().await;
    }

    assert_eq!(
        server.route(browse(), Ipv4Addr::LOCALHOST).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    assert_eq!(
        server
            .route(action(CDS, "GetSystemUpdateID", ""), Ipv4Addr::LOCALHOST)
            .await
            .status(),
        StatusCode::OK
    );

    server
        .catalog
        .release
        .as_ref()
        .unwrap()
        .add_permits(BROWSES);

    for task in active {
        assert_eq!(task.await.unwrap().status(), StatusCode::OK);
    }

    assert_eq!(server.browses.available_permits(), BROWSES);
}

#[tokio::test]
async fn upgrade_subscribe_is_rejected_before_reserving() {
    let server = server(TestCatalog::default());
    let peer = ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 12345)));
    let callback = "<http://127.0.0.1:12345/notify>";

    let subscribe = || {
        Request::builder()
            .method("SUBSCRIBE")
            .uri(EVENTS)
            .header("nt", "upnp:event")
            .header("callback", callback)
            .body(Body::empty())
            .unwrap()
    };

    let mut upgrade = subscribe();

    upgrade
        .headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));

    let response = server.handle(peer, upgrade).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key("sid"));

    let mut declared = subscribe();

    declared
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("1"));

    let response = server.handle(peer, declared).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key("sid"));

    for _ in 0..SUBSCRIPTIONS {
        assert_eq!(
            server.handle(peer, subscribe()).await.status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn run_serves_with_connect_info() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(server(TestCatalog::default()).run(listener));
    let mut client = TcpStream::connect(address).await.unwrap();

    client
        .write_all(b"GET /device.xml HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut response = String::new();
    client.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));

    assert!(response.to_ascii_lowercase().contains(&format!(
        "server: {}\r\n",
        crate::server_header().to_ascii_lowercase()
    )));

    task.abort();
}
