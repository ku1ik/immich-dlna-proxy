use std::{io, sync::Mutex};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
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
    async fn system_update_id(&self) -> Result<u32, Fault> {
        Ok(42)
    }

    async fn browse(&self, arguments: BrowseQuery, _: Instant) -> Result<BrowseResult, Fault> {
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
                kind: crate::catalog::ObjectKind::Root { child_count: 9 },
                title: "A&B".into(),
                date: Some("2024-01-01".parse().unwrap()),
                art: None,
            }],
            total_matches: 9,
            update_id: 7,
        })
    }
}

fn server(catalog: TestCatalog) -> Server<TestCatalog> {
    let activity = Activity::default();

    Server::new(
        "Test & Media".into(),
        Uuid::nil(),
        catalog,
        MediaProxy::new(
            "http://127.0.0.1:9/api/".parse().unwrap(),
            HeaderValue::from_static("test-key"),
            activity.clone(),
        )
        .unwrap(),
        Subscriptions::new(0).unwrap().0,
        activity,
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
        let mut get = request(Method::GET, path, Body::empty());

        get.headers_mut()
            .insert(header::HOST, HeaderValue::from_static("untrusted.example"));

        let get = server.handle(peer, get).await;

        let length = get.headers()[header::CONTENT_LENGTH].clone();
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers()[header::SERVER], crate::server_header());
        assert!(!body(get).await.contains("untrusted.example"));

        let mut head = request(Method::HEAD, path, Body::empty());

        head.headers_mut()
            .insert(header::HOST, HeaderValue::from_static("untrusted.example"));

        let head = server.handle(peer, head).await;
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
            "GetCurrentConnectionIDs",
            "",
            200,
            "<ConnectionIDs>0</ConnectionIDs>",
        ),
        (
            "/upnp/connection-manager/control",
            "GetCurrentConnectionInfo",
            "<ConnectionID>0</ConnectionID>",
            200,
            "<Status>Unknown</Status>",
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
        let document = body(response).await;
        assert!(document.contains(fragment));

        if status == 200 {
            let namespace = if path == CDS {
                protocol::CONTENT_DIRECTORY
            } else {
                protocol::CONNECTION_MANAGER
            };

            assert!(document.contains(&format!("<u:{name}Response xmlns:u=\"{namespace}\">")));
        }
    }

    assert!(server.catalog.actions.lock().unwrap().is_empty());
    assert_eq!(server.activity.subscribe().borrow().last, None);
}

#[tokio::test]
async fn browse_serializes_snapshot_and_releases_permit_with_response() {
    let server = server(TestCatalog::default());
    let response = server.route(browse(), Ipv4Addr::LOCALHOST).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(server.browses.available_permits(), BROWSES);
    assert!(server.activity.subscribe().borrow().last.is_some());

    let document = body(response).await;

    assert!(document.contains(&format!(
        "<u:BrowseResponse xmlns:u=\"{}\">",
        protocol::CONTENT_DIRECTORY
    )));

    assert!(document.contains("<NumberReturned>1</NumberReturned>"));
    assert!(document.contains("<TotalMatches>9</TotalMatches>"));
    assert!(document.contains("<UpdateID>7</UpdateID>"));
    assert!(document.contains("A&amp;amp;B"));

    let actions = server.catalog.actions.lock().unwrap();
    assert_eq!(actions.len(), 1);

    assert_eq!(
        actions[0].mode,
        crate::catalog::BrowseMode::DirectChildren {
            starting_index: 3,
            requested_count: 2
        }
    );

    assert_eq!(actions[0].sort, crate::catalog::SortOrder::DateDescending);
}

#[tokio::test]
async fn invalid_object_id_fails_before_catalog_access_and_after_argument_validation() {
    let server = server(TestCatalog::default());

    for (sort, expected) in [("", 701), ("bad", 709)] {
        let request = action(
            CDS,
            "Browse",
            &format!(
                "<ObjectID>asset:bad</ObjectID><BrowseFlag>BrowseMetadata</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria>{sort}</SortCriteria>"
            ),
        );

        let response = server.route(request, Ipv4Addr::LOCALHOST).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        assert!(
            body(response)
                .await
                .contains(&format!("<errorCode>{expected}</errorCode>"))
        );
    }

    assert!(server.catalog.actions.lock().unwrap().is_empty());
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

    for _ in 0..2 {
        oversized.headers_mut().append(
            "x-large",
            HeaderValue::from_str(&"a".repeat(HEADER_BYTES / 2)).unwrap(),
        );
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

    let stalled_body = Body::from_stream(futures_util::stream::pending::<
        Result<bytes::Bytes, io::Error>,
    >());

    let mut stalled = action(CDS, "Browse", "");
    *stalled.body_mut() = stalled_body;
    let response = server.route(stalled, Ipv4Addr::LOCALHOST);
    tokio::pin!(response);
    assert!(futures_util::poll!(&mut response).is_pending());
    tokio::time::advance(BODY_TIMEOUT).await;
    assert_eq!(response.await.status(), StatusCode::REQUEST_TIMEOUT);

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
    let peer = ConnectInfo(SocketAddr::from((Ipv4Addr::new(192, 168, 1, 20), 12345)));
    let callback = "<http://192.168.1.20:12345/notify>";

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

#[tokio::test(start_paused = true)]
async fn accepted_content_directory_subscriptions_and_renewals_extend_activity() {
    let server = server(TestCatalog::default());
    let activity = server.activity.subscribe();
    let peer = Ipv4Addr::new(192, 168, 1, 20);

    let subscribe = |path: &str| {
        Request::builder()
            .method("SUBSCRIBE")
            .uri(path)
            .header("nt", "upnp:event")
            .header("callback", "<http://192.168.1.20:12345/events>")
            .header("timeout", "Second-300")
            .body(Body::empty())
            .unwrap()
    };

    let renew = |path: &str, sid: &HeaderValue, method: &str, lease: &str| {
        Request::builder()
            .method(method)
            .uri(path)
            .header("sid", sid)
            .header("timeout", lease)
            .body(Body::empty())
            .unwrap()
    };

    let connection_events = "/upnp/connection-manager/events";

    let connection = server.route(subscribe(connection_events), peer).await;

    assert_eq!(connection.status(), StatusCode::OK);
    let sid = connection.headers()["sid"].clone();

    let renewed = server
        .route(
            renew(connection_events, &sid, "SUBSCRIBE", "Second-300"),
            peer,
        )
        .await;

    assert_eq!(renewed.status(), StatusCode::OK);
    assert_eq!(activity.borrow().last, None);
    let response = server.route(subscribe(EVENTS), peer).await;
    assert_eq!(response.status(), StatusCode::OK);
    let sid = response.headers()["sid"].clone();
    assert_eq!(activity.borrow().last, Some(Instant::now()));

    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(210)).await;

        let response = server
            .route(renew(EVENTS, &sid, "SUBSCRIBE", "Second-300"), peer)
            .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(activity.borrow().last, Some(Instant::now()));
    }

    let response = server
        .route(renew(EVENTS, &sid, "SUBSCRIBE", "Second-1800"), peer)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let before = activity.borrow().last;
    tokio::time::advance(crate::activity::IDLE_TIMEOUT).await;
    assert!(!activity.borrow().active(Instant::now()));

    // The lease is still live, but publications/local reads/unsubscribe do not touch activity.
    server.subscriptions.publish(43);

    server
        .route(action(CDS, "GetSystemUpdateID", ""), peer)
        .await;

    let response = server
        .route(renew(EVENTS, &sid, "UNSUBSCRIBE", "Second-1800"), peer)
        .await;

    assert_eq!(response.status(), StatusCode::OK);

    let response = server
        .route(renew(EVENTS, &sid, "SUBSCRIBE", "Second-300"), peer)
        .await;

    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(activity.borrow().last, before);
}
