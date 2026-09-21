//! Two end-to-end HTTP smoke tests for the runnable fixture server.

#[path = "fixtures/server.rs"]
mod fixture_server;

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    };

    use http::{StatusCode, header};
    use immich_dlna_proxy::{
        catalog::Object,
        protocol::{self, Filter, Service},
    };

    use super::fixture_server::{
        Bound, FILES, SERVER_UUID,
        catalog::{ALBUM_ID, GENERATED_JPEG_ID, ORIGINAL_JPEG_ID, VIDEO_ID},
    };

    struct Harness {
        _directory: tempfile::TempDir,
        address: SocketAddrV4,
        client: reqwest::Client,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl Harness {
        async fn start() -> Self {
            let directory = tempfile::tempdir().unwrap();

            for (index, filename) in FILES.into_iter().enumerate() {
                tokio::fs::write(directory.path().join(filename), bytes(index))
                    .await
                    .unwrap();
            }

            let bound = Bound::bind(
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
                directory.path().into(),
            )
            .await
            .unwrap();

            let address =
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, bound.http.local_addr().unwrap().port());

            let task = tokio::spawn(bound.run(None));
            let client = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap();

            Self {
                _directory: directory,
                address,
                client,
                task,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.address)
        }

        async fn browse(&self, id: &str) -> (StatusCode, String) {
            let urn = protocol::CONTENT_DIRECTORY;

            let arguments = format!(
                "<ObjectID>{}</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria></SortCriteria>",
                protocol::escape_text(id)
            );

            let body = format!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><u:Browse xmlns:u=\"{urn}\">{arguments}</u:Browse></s:Body></s:Envelope>"
            );

            let response = self
                .client
                .post(self.url("/upnp/content-directory/control"))
                .header(header::CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                .header("soapaction", format!("\"{urn}#Browse\""))
                .body(body)
                .send()
                .await
                .unwrap();

            let status = response.status();

            (status, response.text().await.unwrap())
        }

        async fn finish(mut self) {
            self.task.abort();
            assert!((&mut self.task).await.unwrap_err().is_cancelled());
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn bytes(index: usize) -> Vec<u8> {
        (0..150_000 + index)
            .map(|offset| ((offset + index * 37) % 251) as u8)
            .collect()
    }

    fn assert_browse(response: (StatusCode, String), objects: &[Object]) {
        let (status, xml) = response;
        assert_eq!(status, StatusCode::OK, "{xml}");

        assert!(xml.contains(&format!(
            "<NumberReturned>{}</NumberReturned>",
            objects.len()
        )));

        assert!(xml.contains(&format!("<TotalMatches>{}</TotalMatches>", objects.len())));
        assert!(xml.contains("<UpdateID>0</UpdateID>"));

        let didl = protocol::didl(objects, &Filter::parse("*").unwrap()).unwrap();

        assert!(xml.contains(&format!(
            "<Result>{}</Result>",
            protocol::escape_text(&didl)
        )));
    }

    #[tokio::test]
    async fn descriptions_and_browse_work_over_the_production_http_stack() {
        let harness = Harness::start().await;
        let objects = super::fixture_server::catalog::objects(harness.address);

        for (path, expected) in [
            (
                "/device.xml",
                protocol::device_description("DLNA Fixture Baseline", SERVER_UUID),
            ),
            (
                "/upnp/content-directory/scpd.xml",
                protocol::scpd(Service::ContentDirectory).into(),
            ),
            (
                "/upnp/connection-manager/scpd.xml",
                protocol::scpd(Service::ConnectionManager).into(),
            ),
        ] {
            let response = harness.client.get(harness.url(path)).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.text().await.unwrap(), expected);
        }

        assert_browse(harness.browse("0").await, &objects[1..2]);
        assert_browse(harness.browse(ALBUM_ID).await, &objects[2..]);
        harness.finish().await;
    }

    #[tokio::test]
    async fn all_six_media_representations_support_get_head_and_ranges() {
        let harness = Harness::start().await;

        for (index, (asset, representation, mime)) in [
            (ORIGINAL_JPEG_ID, "original", "image/jpeg"),
            (ORIGINAL_JPEG_ID, "preview", "image/jpeg"),
            (GENERATED_JPEG_ID, "display", "image/jpeg"),
            (GENERATED_JPEG_ID, "preview", "image/jpeg"),
            (VIDEO_ID, "original", "video/mp4"),
            (VIDEO_ID, "playback", "video/mp4"),
        ]
        .into_iter()
        .enumerate()
        {
            let expected = bytes(index);
            let url = harness.url(&format!("/media/assets/{asset}/{representation}"));

            let response = harness.client.get(&url).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{url}");
            assert_eq!(response.headers()[header::CONTENT_TYPE], mime);
            assert_eq!(response.bytes().await.unwrap().as_ref(), expected);

            let response = harness.client.head(&url).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{url}");

            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                expected.len().to_string()
            );

            assert!(response.bytes().await.unwrap().is_empty());

            let response = harness
                .client
                .get(&url)
                .header(header::RANGE, "bytes=17-48")
                .send()
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT, "{url}");

            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                format!("bytes 17-48/{}", expected.len())
            );

            assert_eq!(response.bytes().await.unwrap().as_ref(), &expected[17..49]);
        }

        harness.finish().await;
    }
}
