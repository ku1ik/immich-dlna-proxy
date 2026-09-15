use std::{net::SocketAddr, time::Duration};

use http::HeaderValue;
use immich_dlna_proxy::{
    catalog::{BrowseResult, Catalog},
    eventing::Subscriptions,
    media::MediaProxy,
    protocol::{BrowseArguments, Fault},
    server::Server,
};
use socket2::SockRef;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::mpsc,
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const ASSET: &str = "10000000-0000-4000-8000-000000000001";
const BODY: [u8; 8192] = [b'x'; 8192];

struct NoCatalog;

impl Catalog for NoCatalog {
    fn system_update_id(&self) -> u32 {
        panic!("media requests must not access the catalog");
    }

    async fn browse(&self, _: BrowseArguments) -> Result<BrowseResult, Fault> {
        panic!("media requests must not browse");
    }
}

async fn headers(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();

    // Do not read ahead into the body: its TCP receive window must stay full.
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(bytes.len() < 8192, "oversized HTTP headers");
        bytes.push(socket.read_u8().await.unwrap());
    }

    String::from_utf8(bytes).unwrap()
}

async fn request(address: SocketAddr, method: &str) -> TcpStream {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    let mut client = socket.connect(address).await.unwrap();

    client
        .write_all(
            format!(
                "{method} /media/assets/{ASSET}/original HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    client
}

#[tokio::test]
async fn media_admission_survives_body_eof_until_tcp_completion() {
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let result = timeout(Duration::from_secs(20), async {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let (sent, mut received) = mpsc::unbounded_channel();

        tasks.spawn(async move {
            // Sixteen GETs and the recovery HEAD. Also answer a wrongly admitted
            // seventeenth GET so the regression fails with 200, not a timeout.
            for _ in 0..17 {
                let (mut socket, _) = upstream.accept().await.unwrap();
                let request = headers(&mut socket).await;
                let method = request.split_whitespace().next().unwrap().to_owned();

                assert!(
                    request.starts_with(&format!("{method} /api/assets/{ASSET}/original HTTP/1.1\r\n")),
                    "unexpected upstream request: {request}"
                );
                assert!(matches!(method.as_str(), "GET" | "HEAD"));

                let mut response = b"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 8192\r\nConnection: close\r\n\r\n".to_vec();

                if method == "GET" {
                    response.extend_from_slice(&BODY);
                }

                // A complete, small body fits Hyper's buffer, unlike a stalled
                // upstream stream whose body itself still owns the permit.
                socket.write_all(&response).await.unwrap();
                socket.shutdown().await.unwrap();
                sent.send(method).unwrap();
            }
        });

        let server = Server::new(
            "Admission test".into(),
            Uuid::nil(),
            NoCatalog,
            MediaProxy::new(
                format!("http://{upstream_address}/api/").parse().unwrap(),
                HeaderValue::from_static("synthetic-admission-test"),
            )
            .unwrap(),
            Subscriptions::new().unwrap(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Accepted sockets inherit this before Server::run takes the listener.
        SockRef::from(&listener).set_send_buffer_size(1024).unwrap();
        let server_shutdown = shutdown.clone();

        tasks.spawn(async move {
            server.run(listener, server_shutdown).await.unwrap();
        });

        let mut clients = Vec::new();

        for index in 0..16 {
            let mut client = request(address, "GET").await;
            assert_eq!(received.recv().await.as_deref(), Some("GET"));
            let response = headers(&mut client).await;

            assert!(
                response.starts_with("HTTP/1.1 200 OK\r\n"),
                "admitted GET {index}: {response}"
            );

            clients.push(client);
        }

        let mut rejected = request(address, "GET").await;
        let response = headers(&mut rejected).await;

        assert!(
            response.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
            "seventeenth GET must be rejected while TCP bodies are unread: {response}"
        );
        assert!(
            matches!(received.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "rejected GET must not reach upstream"
        );

        // Drain concurrently so the deliberately tiny windows do not serialize
        // TCP window updates across all sixteen clients.
        futures_util::future::join_all(clients.into_iter().map(|mut client| async move {
            let mut body = Vec::new();
            client.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, BODY);
        }))
        .await;

        let mut recovered = request(address, "HEAD").await;
        let response = headers(&mut recovered).await;

        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "HEAD must be admitted after draining: {response}"
        );
        assert_eq!(received.recv().await.as_deref(), Some("HEAD"));

        let mut body = Vec::new();
        recovered.read_to_end(&mut body).await.unwrap();
        assert!(body.is_empty());
        shutdown.cancel();

        while let Some(joined) = tasks.join_next().await {
            joined.unwrap();
        }
    })
    .await;

    shutdown.cancel();
    tasks.shutdown().await;
    result.expect("TCP media admission regression exceeded 20 seconds");
}
