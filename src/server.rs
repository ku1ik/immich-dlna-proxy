use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request},
    response::Response,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    time::{Instant, timeout_at},
};
use uuid::Uuid;

use crate::{
    catalog::Catalog,
    eventing::Subscriptions,
    media::MediaProxy,
    protocol::{self, Action, Fault, Service},
};

pub(crate) const HEADER_BYTES: usize = 16 * 1024;
const SOAP_BODY_BYTES: usize = 64 * 1024;
const BROWSES: usize = 8;
const BODY_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_PROCESSING_TIMEOUT: Duration = Duration::from_secs(25);

pub struct Server<C> {
    catalog: C,
    media: MediaProxy,
    subscriptions: Subscriptions,
    device: String,
    browses: Semaphore,
}

enum Route<'a> {
    Device,
    Scpd(Service),
    Control(Service),
    Events(Service),
    Media {
        asset: &'a str,
        representation: &'a str,
    },
}

impl<'a> Route<'a> {
    fn parse(path: &'a str) -> Option<Self> {
        match path {
            "/device.xml" => Some(Self::Device),
            "/upnp/content-directory/scpd.xml" => Some(Self::Scpd(Service::ContentDirectory)),
            "/upnp/connection-manager/scpd.xml" => Some(Self::Scpd(Service::ConnectionManager)),
            "/upnp/content-directory/control" => Some(Self::Control(Service::ContentDirectory)),
            "/upnp/connection-manager/control" => Some(Self::Control(Service::ConnectionManager)),
            "/upnp/content-directory/events" => Some(Self::Events(Service::ContentDirectory)),
            "/upnp/connection-manager/events" => Some(Self::Events(Service::ConnectionManager)),

            _ => {
                let (asset, representation) = path
                    .strip_prefix(crate::media::ASSET_ROUTE_PREFIX)?
                    .split_once('/')?;

                Some(Self::Media {
                    asset,
                    representation,
                })
            }
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
            browses: Semaphore::new(BROWSES),
        }
    }

    pub async fn run(self, listener: TcpListener) -> anyhow::Result<()> {
        let server = Arc::new(self);
        let app = Router::new().fallback(move |peer: ConnectInfo<SocketAddr>, request: Request| {
            let server = server.clone();

            async move { server.handle(peer, request).await }
        });

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;

        Ok(())
    }

    async fn handle(
        &self,
        ConnectInfo(peer): ConnectInfo<SocketAddr>,
        request: Request,
    ) -> Response {
        let mut response = match peer.ip() {
            IpAddr::V4(peer) => self.route(request, peer).await,
            IpAddr::V6(_) => empty(StatusCode::BAD_REQUEST),
        };

        response.headers_mut().insert(
            header::SERVER,
            HeaderValue::from_static(crate::server_header()),
        );

        response
    }

    async fn route(&self, request: Request, peer: Ipv4Addr) -> Response {
        let started = Instant::now();
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

        // Reject upgrades before accepting application work, especially subscriptions.
        if parts.headers.contains_key(header::UPGRADE) {
            return empty(StatusCode::BAD_REQUEST);
        }

        if parts.uri.query().is_some() {
            return empty(StatusCode::NOT_FOUND);
        }

        let Some(route) = Route::parse(path) else {
            return empty(StatusCode::NOT_FOUND);
        };

        if !matches!(route, Route::Control(_)) && declared_body(&parts.headers) {
            return empty(StatusCode::BAD_REQUEST);
        }

        let document = match route {
            Route::Control(service) => {
                if parts.method != Method::POST {
                    return method_not_allowed("POST");
                }

                let response = self
                    .control(service, parts.headers, body, started, peer)
                    .await;

                tracing::debug!(%peer, ?service, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis(), "control response");

                return response;
            }

            Route::Events(service) => {
                return self
                    .subscriptions
                    .request(service, peer, &parts.method, &parts.headers);
            }

            _ if parts.method != Method::GET && parts.method != Method::HEAD => {
                return method_not_allowed("GET, HEAD");
            }

            Route::Media {
                asset,
                representation,
            } => {
                tracing::debug!(%peer, "media request peer");

                return self
                    .media
                    .serve(asset, representation, parts.method, parts.headers)
                    .await;
            }

            Route::Device => &self.device,
            Route::Scpd(service) => protocol::scpd(service),
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

        let body_deadline = started + BODY_TIMEOUT;

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
        let deadline = started + CONTROL_PROCESSING_TIMEOUT;

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
        let _permit = if matches!(action, Action::Browse { .. }) {
            let Ok(permit) = self.browses.try_acquire() else {
                return empty(StatusCode::SERVICE_UNAVAILABLE);
            };

            Some(permit)
        } else {
            None
        };

        let result = timeout_at(deadline, execute(&self.catalog, service, action, peer))
            .await
            .unwrap_or(Err(Fault { code: 501 }));

        let result = if Instant::now() < deadline {
            result
        } else {
            Err(Fault { code: 501 })
        };

        if let Err(fault) = &result {
            tracing::debug!(%peer, ?service, code = fault.code, "SOAP action failed");
        }

        soap(result)
    }
}

async fn execute<C: Catalog>(
    catalog: &C,
    service: Service,
    action: Action,
    peer: Ipv4Addr,
) -> Result<String, Fault> {
    let name = action.name();
    let response = |args: &[(&'static str, &str)]| protocol::action_response(service, name, args);

    match action {
        Action::Browse { query, filter } => {
            let object = crate::catalog::parse_id(&query.object_id).ok();

            tracing::debug!(
                %peer, ?object,
                metadata = query.metadata,
                starting_index = query.starting_index,
                requested_count = query.requested_count,
                sort = ?query.sort,
                resources_selected = filter.res(),
                "Browse request"
            );

            let result = catalog.browse(query).await?;
            let didl = protocol::didl(&result.objects, &filter)?;
            let envelope = response(&[
                ("Result", &didl),
                ("NumberReturned", &result.objects.len().to_string()),
                ("TotalMatches", &result.total_matches.to_string()),
                ("UpdateID", &result.update_id.to_string()),
            ])?;

            tracing::debug!(%peer, ?object, returned = result.objects.len(), total = result.total_matches, update_id = result.update_id, "Browse response");

            Ok(envelope)
        }

        Action::GetSearchCapabilities => response(&[("SearchCaps", "")]),
        Action::GetSortCapabilities => response(&[("SortCaps", "dc:date")]),

        Action::GetSystemUpdateId => {
            let id = catalog.system_update_id();
            tracing::debug!(%peer, id, "published update ID");
            let id = id.to_string();

            response(&[("Id", &id)])
        }

        Action::GetProtocolInfo => response(&[("Source", "http-get:*:*:*"), ("Sink", "")]),

        Action::GetCurrentConnectionIds => response(&[("ConnectionIDs", "0")]),

        Action::GetCurrentConnectionInfo(id) => {
            if id != 0 {
                return Err(Fault { code: 706 });
            }

            response(&[
                ("RcsID", "-1"),
                ("AVTransportID", "-1"),
                ("ProtocolInfo", ""),
                ("PeerConnectionManager", ""),
                ("PeerConnectionID", "-1"),
                ("Direction", "Output"),
                ("Status", "Unknown"),
            ])
        }
    }
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
mod tests;
