use std::{
    io::{self, Write},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Router, body::Body, extract::Request, response::Response};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use immich_dlna_proxy::{
    catalog::{BrowseResult, Catalog},
    eventing::Subscriptions,
    media::MediaProxy,
    protocol::{self, BrowseArguments, Fault, Object, Service},
    server::Server,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use uuid::Uuid;

const ASSET: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
const EVENTS: &str = "/upnp/content-directory/events";

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);

        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct TestCatalog;

impl Catalog for TestCatalog {
    fn system_update_id(&self) -> u32 {
        42
    }

    async fn browse(&self, arguments: BrowseArguments) -> Result<BrowseResult, Fault> {
        assert_eq!(arguments.object_id, "0");
        assert!(!arguments.metadata);
        assert_eq!(arguments.filter, protocol::Filter::parse("res").unwrap());
        assert_eq!(arguments.starting_index, 3);
        assert_eq!(arguments.requested_count, 2);
        assert_eq!(arguments.sort, Some(true));

        Ok(BrowseResult {
            objects: vec![Object {
                id: "0".into(),
                parent_id: "-1".into(),
                title: "A&B".into(),
                class: "object.container".into(),
                date: None,
                art: None,
                child_count: Some(9),
                resources: Vec::new(),
            }],
            total_matches: 9,
            update_id: 7,
        })
    }
}

async fn exchange(address: SocketAddr, request: &str, status: u16) -> String {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let request = request.replacen("\r\n\r\n", "\r\nConnection: close\r\n\r\n", 1);
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();

    assert!(
        response.starts_with(&format!("HTTP/1.1 {status} ")),
        "{response}"
    );

    response
}

// Keep one test in this executable: the global subscriber must precede every callsite.
#[tokio::test]
async fn control_and_media_diagnostics_are_bounded_and_exclude_secrets() {
    let capture = Capture(Arc::new(Mutex::new(Vec::new())));
    let writer = capture.clone();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .without_time()
        .with_writer(move || writer.clone())
        .try_init()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tasks = JoinSet::new();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();

        let router = Router::new().fallback(|request: Request| async move {
            assert_eq!(request.method(), Method::GET);
            assert_eq!(request.uri().path(), format!("/api/assets/{ASSET}/original"));
            assert!(request.uri().query().is_none());
            assert_eq!(request.headers()["x-api-key"], "api-key-secret");

            for name in [
                "range",
                "if-range",
                "timeseekrange.dlna.org",
                "transfermode.dlna.org",
                "getcontentfeatures.dlna.org",
                "getavailableseekrange.dlna.org",
                "playspeed.dlna.org",
                "authorization",
                "cookie",
                "user-agent",
            ] {
                assert!(!request.headers().contains_key(name), "forwarded {name}");
            }

            Response::builder()
                .header(header::CONTENT_TYPE, "IMAGE/JPEG; note=\"a;b\"")
                .header(header::CONTENT_LENGTH, "3")
                .header(header::ACCEPT_RANGES, "bytes")
                .body(Body::from("abc"))
                .unwrap()
        });

        tasks.spawn(async move {
            axum::serve(upstream, router).await.unwrap();
        });

        let subscriptions = Subscriptions::new().unwrap();

        // Production rejects loopback callbacks. Check successful lease state via the
        // public API with a documentation-only peer; no notification scheduler runs.
        let mut headers = HeaderMap::new();
        headers.insert("nt", HeaderValue::from_static("upnp:event"));

        headers.insert(
            "callback",
            HeaderValue::from_static("<http://192.0.2.1/event?sensitiveCallbackQuery>"),
        );

        headers.insert("timeout", HeaderValue::from_static("Second-60"));

        let response = subscriptions.request(
            Service::ContentDirectory,
            "192.0.2.1".parse().unwrap(),
            &Method::from_bytes(b"SUBSCRIBE").unwrap(),
            &headers,
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["timeout"], "Second-60");
        assert!(response.headers().contains_key("sid"));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let media = MediaProxy::new(
            format!("http://{upstream_address}/api/").parse().unwrap(),
            HeaderValue::from_static("api-key-secret"),
        )
        .unwrap();

        let server = Server::new(
            "Test & Media".into(),
            Uuid::nil(),
            TestCatalog,
            media,
            subscriptions,
        );

        tasks.spawn(async move {
            server.run(listener).await.unwrap();
        });

        let namespace = protocol::CONTENT_DIRECTORY;

        let body = format!(
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:Browse xmlns:u=\"{namespace}\"><ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>res,sensitiveFilterToken</Filter><StartingIndex>3</StartingIndex><RequestedCount>2</RequestedCount><SortCriteria>-dc:date</SortCriteria></u:Browse></s:Body></s:Envelope>"
        );

        let request = format!(
            "POST /upnp/content-directory/control HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/xml; charset = \"utf-8\"\r\nSOAPAction: \"{namespace}#Browse\"\r\nAuthorization: Bearer sensitiveAuthorization\r\nCookie: sensitiveCookie\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );

        let response = exchange(address, &request, 200).await;
        assert!(response.contains("A&amp;amp;B"));
        assert!(response.contains("<NumberReturned>1</NumberReturned><TotalMatches>9</TotalMatches><UpdateID>7</UpdateID>"));

        for headers in [
            "NT: upnp:event\r\nCallback: <http://127.0.0.1:12345/event?sensitiveCallbackQuery>\r\nTimeout: Second-60\r\n",
            "SID: uuid:sensitiveInvalidSid\r\n",
        ] {
            let request = format!(
                "SUBSCRIBE {EVENTS} HTTP/1.1\r\nHost: localhost\r\n{headers}\r\n"
            );

            exchange(address, &request, 412).await;
        }

        let request = format!(
            "GET /media/assets/{ASSET}/original HTTP/1.1\r\nHost: localhost\r\nRange: bytes=1-2\r\nRange: invalid-range-secret\r\nIf-Range: validator-secret\r\ngetcontentfeatures.dlna.org: 1\r\ntimeseekrange.dlna.org: untrusted-time-seek-secret\r\ngetavailableseekrange.dlna.org: available-seek-secret\r\nplayspeed.dlna.org: play-speed-secret\r\ntransfermode.dlna.org: untrusted-transfer-mode-secret\r\nAuthorization: Bearer authorization-secret\r\nCookie: cookie-secret\r\nUser-Agent: user-agent-secret\r\n\r\n"
        );

        let response = exchange(address, &request, 200).await;
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(!headers.contains("contentfeatures.dlna.org"));
        assert!(headers.contains("content-length: 3"));
        assert!(headers.contains("content-type: IMAGE/JPEG; note=\"a;b\""));
        assert_eq!(body, "abc");

        let overrun = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let overrun_address = overrun.local_addr().unwrap();

        tasks.spawn(async move {
            let (mut socket, _) = overrun.accept().await.unwrap();
            let mut request = Vec::new();

            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }

            socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Type: image/jpeg\r\nContent-Range: bytes 0-2/10\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n0\r\n\r\n").await.unwrap();
        });

        let media = MediaProxy::new(
            format!("http://{overrun_address}/api/").parse().unwrap(),
            HeaderValue::from_static("api-key-secret"),
        )
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-2"));
        let response = media.serve(ASSET, "original", Method::GET, headers).await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(axum::body::to_bytes(response.into_body(), 1024).await.is_err());
        tasks.abort_all();

        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled());
            }
        }
    })
    .await
    .unwrap();

    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();

    for expected in [
        "SOAP action",
        "Browse request",
        "object=Some(Root)",
        "starting_index=Some(3)",
        "requested_count=Some(2)",
        "resources_selected=true",
        "Browse response",
        "returned=1",
        "total=9",
        "update_id=7",
        "control response",
        "subscription response",
        "Second-60",
        "sid_supplied=true",
        "status=412",
        "media request peer",
        "media request capabilities",
        "range_count=2",
        "range=None",
        "if_range_present=true",
        "content_features_requested=true",
        "time_seek_present=true",
        "available_seek_range_present=true",
        "play_speed_present=true",
        "transfer_mode_present=true",
        "validated media response",
        "content_length=Some(3)",
        "accepts_bytes=true",
        "body_length=Some(3)",
        "media stream terminated",
        "media body exceeds declared content length",
    ] {
        assert!(log.contains(expected), "missing {expected}: {log}");
    }

    for excluded in [
        "secret",
        "sensitive",
        "<s:Envelope",
        "A&amp;B",
        "A&amp;amp;B",
    ] {
        assert!(
            !log.contains(excluded),
            "unexpected disclosure of {excluded}: {log}"
        );
    }
}
