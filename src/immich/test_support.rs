use std::{collections::VecDeque, sync::Arc, sync::Mutex};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request},
    response::Response,
};
use http::HeaderValue;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use url::Url;
use uuid::Uuid;

use super::Client;

pub(crate) struct Received {
    pub(crate) method: Method,
    pub(crate) uri: String,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Value,
}

pub(crate) struct Fake {
    pub(crate) requests: Arc<Mutex<Vec<Received>>>,
    pub(super) api_base: Url,
    task: JoinHandle<()>,
}

impl Fake {
    pub(crate) async fn new(replies: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let api_base = Url::parse(&format!("http://{address}/prefix/api/")).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));

        let router =
            Router::new().fallback(move |request: Request<Body>| {
                let captured = captured.clone();
                let replies = replies.clone();

                async move {
                    let (parts, body) = request.into_parts();
                    let body = to_bytes(body, 8192).await.unwrap();

                    captured.lock().unwrap().push(Received {
                        method: parts.method,
                        uri: parts.uri.to_string(),
                        headers: parts.headers,
                        body: if body.is_empty() {
                            Value::Null
                        } else {
                            serde_json::from_slice(&body).unwrap()
                        },
                    });

                    replies.lock().unwrap().pop_front().unwrap_or_else(|| {
                        Response::builder().status(500).body(Body::empty()).unwrap()
                    })
                }
            });

        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            requests,
            api_base,
            task,
        }
    }

    pub(crate) fn new_client(&self) -> Client {
        Client::new(
            self.api_base.clone(),
            HeaderValue::from_static("private-test-key"),
        )
        .unwrap()
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn reply(value: Value) -> Response {
    Response::builder()
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}

pub(crate) fn version() -> Response {
    reply(json!({"major": 3, "minor": 1, "patch": 0, "prerelease": null}))
}

pub(crate) fn album(id: Uuid) -> Value {
    json!({"id": id, "albumName": "Album", "createdAt": "2024-01-01T00:30:00+02:00", "albumThumbnailAssetId": Uuid::from_u128(777)})
}

pub(crate) fn asset(id: u128, kind: &str) -> Value {
    json!({
        "id": Uuid::from_u128(id), "type": kind, "visibility": "timeline",
        "isTrashed": false, "isEdited": false, "originalFileName": "photo.jpg",
        "originalMimeType": "image/jpeg", "duration": null,
        "fileCreatedAt": "2024-01-01T00:30:00+02:00",
        "localDateTime": "2024-01-01T00:30:00Z"
    })
}

pub(crate) fn page(items: Vec<Value>, next: Option<&str>) -> Response {
    reply(json!({"assets": {"items": items, "nextPage": next, "total": 0, "count": 0}}))
}
