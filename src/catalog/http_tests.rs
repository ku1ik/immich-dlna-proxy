use super::*;
use crate::{media::MediaProxy, protocol, server::Server};
use quick_xml::{Reader, events::Event};

fn text(xml: &str, name: &str) -> String {
    let mut reader = Reader::from_str(xml);

    loop {
        match reader.read_event().unwrap() {
            Event::Start(element) if element.local_name().as_ref() == name.as_bytes() => {
                let raw = reader.read_text(element.name()).unwrap();

                return quick_xml::escape::unescape(&raw).unwrap().into_owned();
            }

            Event::Eof => panic!("missing XML element {name}"),
            _ => {}
        }
    }
}

async fn browse_http(client: &reqwest::Client, base: &str, album: Uuid) -> String {
    let namespace = protocol::CONTENT_DIRECTORY;

    let body = format!(
        "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:Browse xmlns:u=\"{namespace}\"><ObjectID>album:{album}</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/></u:Browse></s:Body></s:Envelope>"
    );

    let response = client
        .post(format!("{base}/upnp/content-directory/control"))
        .header("content-type", "text/xml")
        .header("soapaction", format!("\"{namespace}#Browse\""))
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    response.text().await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn browse_resources_and_published_revisions_connect_over_http() {
    let _clock = background_tests::Clock::new();
    let fake = Fake::new().await;
    let album = Uuid::from_u128(1);
    let asset = Uuid::from_u128(10);

    {
        let mut upstream = fake.upstream.lock().unwrap();
        upstream.albums = vec![super::album(1, "Family & friends")];
        upstream.contents.insert(album, vec![item(10, None, None)]);

        upstream.media.insert(
            format!("/api/assets/{asset}/original"),
            ("image/jpeg", b"original-jpeg"),
        );
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        unreachable!("literal IPv4 bind");
    };

    let base = format!("http://{address}");
    let mut config = config(fake.address);
    config.listen_address = address;
    let activity = activity::Activity::default();
    let media = MediaProxy::new(
        config.api_base.clone(),
        config.api_key.clone(),
        activity.clone(),
    )
    .unwrap();
    let (events, event_task) = Subscriptions::new(1).unwrap();
    let name = config.friendly_name.clone();
    let uuid = config.server_uuid;
    let (catalog, task) =
        ImmichCatalog::new(config, 1, events.clone(), activity.subscribe()).unwrap();
    let library = Library {
        catalog,
        events: events.clone(),
        activity: activity.clone(),
    };

    let mut fixture = Fixture {
        library,
        fake,
        task: Mutex::new(Some(task)),
        event_task: Mutex::new(Some(event_task)),
    };

    let catalog_task = fixture.run();
    let events_task = fixture.run_events();
    let server = Server::new(
        name,
        uuid,
        fixture.library.catalog.clone(),
        media,
        events,
        activity,
    );
    let server_task = tokio::spawn(server.run(listener));

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let listing = browse_http(&client, &base, album).await;
    assert_eq!(text(&listing, "NumberReturned"), "1");
    let didl = text(&listing, "Result");
    let resource = text(&didl, "res");
    assert_eq!(resource, format!("{base}/media/assets/{asset}/original"));
    let published = fixture.revisions().await;

    assert_eq!(
        text(&listing, "UpdateID"),
        published.albums[&album].update_id.to_string()
    );

    let calls = fixture.fake.upstream.lock().unwrap().requests.len();
    let response = client.get(resource).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"original-jpeg");
    assert_eq!(fixture.fake.upstream.lock().unwrap().requests.len(), calls);
    assert_eq!(fixture.revisions().await, published);

    let subscription = client
        .request(
            Method::from_bytes(b"SUBSCRIBE").unwrap(),
            format!("{base}/upnp/content-directory/events"),
        )
        .header("nt", "upnp:event")
        .header(
            "callback",
            format!("<http://{}/events>", fixture.fake.address),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(subscription.status(), 412);

    // Loopback callbacks are rejected over HTTP; set up notification delivery explicitly.
    fixture.fake.subscribe(&fixture.library);
    fixture.fake.event(published.system_update_id).await;

    fixture
        .fake
        .upstream
        .lock()
        .unwrap()
        .contents
        .get_mut(&album)
        .unwrap()[0]["checksum"] = json!("changed");

    let gate = fixture.gate(Scope::Root);
    tokio::time::advance(crate::catalog::background::INTERVAL).await;
    gate.entered().await;
    gate.release.add_permits(1);
    background_tests::finished(&fixture).await;
    let committed = fixture.revisions().await;
    assert_eq!(committed.system_update_id, published.system_update_id + 1);
    fixture.fake.event(committed.system_update_id).await;
    let changed = browse_http(&client, &base, album).await;

    assert_eq!(
        committed.albums[&album].update_id,
        published.albums[&album].update_id + 1
    );

    assert_eq!(
        text(&changed, "UpdateID"),
        committed.albums[&album].update_id.to_string()
    );

    assert_eq!(
        fixture.library.system_update_id().await,
        committed.system_update_id
    );

    server_task.abort();
    assert!(server_task.await.unwrap_err().is_cancelled());
    fixture.abort(catalog_task).await;
    events_task.abort();
    assert!(events_task.await.unwrap_err().is_cancelled());
}
