use super::*;

const UUID: Uuid = Uuid::from_u128(0x7b37df49775d4bcb89a60c917a934643);
const LOCAL: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8200);

fn search(target: &str, mx: &str) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: {MULTICAST}\r\nMAN: \"ssdp:discover\"\r\nMX: {mx}\r\nST: {target}\r\n\r\n"
    )
}

fn loopback() -> Discovery {
    // SAFETY: the C string is terminated and valid for this read-only call.
    let interface_index = unsafe { libc::if_nametoindex(c"lo".as_ptr()) };
    assert_ne!(interface_index, 0);

    let socket = make_socket(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();

    Discovery {
        socket: UdpSocket::from_std(socket.into()).unwrap(),
        address: Ipv4Addr::LOCALHOST,
        interface_index,
        targets: targets(UUID),
        location: format!("http://{LOCAL}/device.xml"),
    }
}

#[test]
fn parses_all_and_each_exact_target_and_clamps_mx() {
    let targets = targets(UUID);

    for (index, target) in targets.iter().enumerate() {
        assert_eq!(
            parse_search(search(target, "2").as_bytes(), &targets),
            Some(Search {
                target: Some(index),
                mx: Duration::from_secs(2),
            })
        );
    }

    assert_eq!(
        parse_search(search("ssdp:all", "4294967295").as_bytes(), &targets),
        Some(Search {
            target: None,
            mx: Duration::from_secs(5),
        })
    );

    let mixed_case = search("ssdp:all", "1")
        .replace("HOST:", "hOsT:")
        .replace("MAN:", "man:\t")
        .replace("MX: 1", "mx: 01\t");

    assert!(parse_search(mixed_case.as_bytes(), &targets).is_some());
}

#[test]
fn rejects_malformed_searches_and_unsupported_targets() {
    let targets = targets(UUID);
    let valid = search("ssdp:all", "1");

    for invalid in [
        valid.replace("M-SEARCH", "NOTIFY"),
        valid.replace(" * ", " / "),
        valid.replace("HTTP/1.1", "HTTP/1.0"),
        valid.replace("\r\n", "\n"),
        valid.replace("\"ssdp:discover\"", "ssdp:discover"),
        valid.replace("\"ssdp:discover\"", "\"other\""),
        valid.replace("HOST: 239.255.255.250:1900\r\n", ""),
        valid.replace("MAN: \"ssdp:discover\"\r\n", ""),
        valid.replace("MX: 1\r\n", ""),
        valid.replace("ST: ssdp:all\r\n", ""),
        valid.replace("MX: 1", "MX: 1\r\nmx: 2"),
        valid.replace("ST: ssdp:all", "ST: ssdp:all\r\nST: upnp:rootdevice"),
        valid.replace("HOST:", " HOST:"),
        valid.replace("MX: 1", "MX : 1"),
        valid.replace("MX: 1", "MX: 1\0"),
        valid.replace("MX: 1", "MX: 1\rX: bad"),
        format!("{valid}body"),
        format!("{valid}\r\n"),
        search("urn:schemas-upnp-org:device:MediaServer:2", "1"),
        search("urn:schemas-upnp-org:service:ContentDirectory:2", "1"),
        search("urn:schemas-upnp-org:service:ConnectionManager:2", "1"),
        search("uuid:00000000-0000-0000-0000-000000000000", "1"),
    ] {
        assert!(
            parse_search(invalid.as_bytes(), &targets).is_none(),
            "{invalid:?}"
        );
    }

    for mx in [
        "0",
        "-1",
        "+1",
        "1.5",
        "",
        "4294967296",
        "999999999999999999999",
    ] {
        assert!(parse_search(search("ssdp:all", mx).as_bytes(), &targets).is_none());
    }
}

#[test]
fn parser_enforces_exact_datagram_bound() {
    let valid = search("ssdp:all", "1");
    let prefix = valid.strip_suffix("\r\n").unwrap();
    let padding = "a".repeat(DATAGRAM_BYTES - prefix.len() - 7);
    let packet = format!("{prefix}X: {padding}\r\n\r\n");
    assert_eq!(packet.len(), DATAGRAM_BYTES);
    assert!(parse_search(packet.as_bytes(), &targets(UUID)).is_some());
    assert!(parse_search(packet.replace("X: ", "X: aa").as_bytes(), &targets(UUID)).is_none());
}

#[test]
fn queue_admits_all_targets_atomically_and_never_exceeds_bound() {
    let now = Instant::now();
    let mut responses = Responses::new(now);

    let all = Search {
        target: None,
        mx: Duration::from_secs(5),
    };

    for _ in 0..1000 {
        responses.enqueue(all, LOCAL, now, || 0);
    }

    assert_eq!(responses.pending.len(), 255);

    assert_eq!(
        responses
            .pending
            .iter()
            .filter(|entry| entry.target == 4)
            .count(),
        51
    );

    responses.enqueue(
        Search {
            target: Some(1),
            ..all
        },
        LOCAL,
        now,
        || 0,
    );

    assert_eq!(responses.pending.len(), PENDING_RESPONSES);

    responses.enqueue(
        Search {
            target: Some(1),
            ..all
        },
        LOCAL,
        now,
        || 0,
    );

    assert_eq!(responses.pending.len(), PENDING_RESPONSES);
    assert_eq!(responses.next_deadline(now + all.mx), None);
    responses.enqueue(all, LOCAL, now + all.mx, || 0);
    assert_eq!(responses.pending.len(), 5);
}

#[test]
fn response_delays_rate_burst_and_expiration_are_bounded() {
    let now = Instant::now();
    let mut responses = Responses::new(now);

    let request = Search {
        target: Some(0),
        mx: Duration::from_secs(5),
    };

    responses.enqueue(request, LOCAL, now, || 123_000_000);

    assert!(
        responses
            .pop_due(now + Duration::from_millis(122))
            .is_none()
    );

    assert!(
        responses
            .pop_due(now + Duration::from_millis(123))
            .is_some()
    );

    let now = now + Duration::from_secs(10);

    for _ in 0..PENDING_RESPONSES {
        responses.enqueue(request, LOCAL, now, || 0);
    }

    for _ in 0..RESPONSE_BURST {
        assert!(responses.pop_due(now).is_some());
    }

    assert!(responses.pop_due(now).is_none());
    assert_eq!(responses.next_deadline(now), Some(now + RESPONSE_SPACING));

    assert!(
        responses
            .pop_due(now + RESPONSE_SPACING - Duration::from_nanos(1))
            .is_none()
    );

    assert!(responses.pop_due(now + RESPONSE_SPACING).is_some());
    assert!(responses.pop_due(now + RESPONSE_SPACING).is_none());

    for tick in 2..=RESPONSES_PER_SECOND {
        assert!(responses.pop_due(now + RESPONSE_SPACING * tick).is_some());
        assert!(responses.pop_due(now + RESPONSE_SPACING * tick).is_none());
    }

    assert!(responses.pop_due(now + request.mx).is_none());
    assert!(responses.pending.is_empty());
    responses.enqueue(request, LOCAL, now + request.mx, || u64::MAX);
    assert_eq!(responses.pending[0].due, now + request.mx * 2);
    assert!(responses.pop_due(now + request.mx * 2).is_none());
}

#[test]
fn ingress_rejects_wrong_interface_destination_and_non_unicast_source() {
    let address = Ipv4Addr::new(192, 168, 1, 10);

    let mut packet = Packet {
        length: 1,
        source: SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 20), 12345),
        destination: *MULTICAST.ip(),
        interface_index: 2,
    };

    assert!(packet.accepted(address, 2));
    assert!(!packet.accepted(address, 3));
    packet.destination = Ipv4Addr::BROADCAST;
    assert!(!packet.accepted(address, 2));
    packet.destination = address;
    assert!(packet.accepted(address, 2));

    for source in [
        Ipv4Addr::UNSPECIFIED,
        Ipv4Addr::new(0, 1, 2, 3),
        Ipv4Addr::BROADCAST,
        *MULTICAST.ip(),
        Ipv4Addr::new(240, 1, 2, 3),
        Ipv4Addr::LOCALHOST,
    ] {
        packet.source.set_ip(source);
        assert!(!packet.accepted(address, 2));
    }

    packet.source = SocketAddrV4::new(address, 0);
    assert!(!packet.accepted(address, 2));
}

#[test]
fn all_wire_messages_have_exact_target_usn_and_framing() {
    let targets = targets(UUID);
    let location = format!("http://{LOCAL}/device.xml");

    for target in 0..5 {
        for kind in [Message::Alive, Message::Response] {
            let message = message(&targets, &location, target, kind);

            let usn = if target == 1 {
                targets[1].clone()
            } else {
                format!("{}::{}", targets[1], targets[target])
            };

            assert!(message.contains(&format!("USN: {usn}\r\n")));
            assert!(message.ends_with("\r\n\r\n"));
            assert!(!message.replace("\r\n", "").contains(['\r', '\n']));
            assert!(message.len() <= DATAGRAM_BYTES);
            assert!(!message.contains("BOOTID"));
            assert!(!message.contains("CONFIGID"));

            if matches!(kind, Message::Response) {
                assert!(message.starts_with("HTTP/1.1 200 OK\r\nEXT:\r\n"));
                assert!(message.contains(&format!("ST: {}\r\n", targets[target])));
            } else {
                assert!(message.starts_with("NOTIFY * HTTP/1.1\r\n"));
                assert!(message.contains(&format!("HOST: {MULTICAST}\r\n")));
                assert!(message.contains(&format!("NT: {}\r\n", targets[target])));
                assert!(message.contains("NTS: ssdp:alive\r\n"));
            }

            assert!(message.contains("CACHE-CONTROL: max-age=1800\r\n"));
            assert!(message.contains(&format!("LOCATION: http://{LOCAL}/device.xml\r\n")));
            assert!(message.contains(&format!("SERVER: {}\r\n", crate::server_header())));
        }
    }
}

#[tokio::test]
async fn unprivileged_loopback_pktinfo_source_and_datagram_truncation() {
    let discovery = loopback();
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let destination = discovery.socket.local_addr().unwrap();
    let mut buffer = [0; DATAGRAM_BYTES];
    peer.send_to(b"probe", destination).await.unwrap();

    let packet = tokio::time::timeout(
        Duration::from_secs(2),
        discovery.socket.async_io(Interest::READABLE, || {
            receive(&discovery.socket, &mut buffer)
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_eq!(packet.interface_index, discovery.interface_index);
    assert_eq!(packet.destination, Ipv4Addr::LOCALHOST);
    assert!(packet.accepted(discovery.address, discovery.interface_index));
    assert_eq!(&buffer[..packet.length], b"probe");
    discovery.send(b"reply", packet.source).unwrap();

    let (length, source) =
        tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();

    assert_eq!(source, destination);
    assert_eq!(&buffer[..length], b"reply");

    let selected_source = Ipv4Addr::new(127, 0, 0, 2);

    send(
        &discovery.socket,
        b"selected-source",
        packet.source,
        selected_source,
        discovery.interface_index,
    )
    .unwrap();

    let (_, source) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(source.ip(), selected_source);
    assert_eq!(source.port(), destination.port());

    peer.send_to(&vec![b'x'; DATAGRAM_BYTES + 1], destination)
        .await
        .unwrap();

    let packet = tokio::time::timeout(
        Duration::from_secs(2),
        discovery.socket.async_io(Interest::READABLE, || {
            receive(&discovery.socket, &mut buffer)
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(packet.is_none());

    assert!(discovery.send(&vec![0; DATAGRAM_BYTES + 1], LOCAL).is_err());
}

#[tokio::test]
async fn shared_port_bind_does_not_send_or_require_capabilities() {
    let first = make_socket(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = first.local_addr().unwrap();
    let second = make_socket(address.as_socket_ipv4().unwrap()).unwrap();
    assert_eq!(second.local_addr().unwrap(), address);
    assert_eq!(first.multicast_ttl_v4().unwrap(), TTL);
    assert!(!first.multicast_all_v4().unwrap());
    let socket = UdpSocket::from_std(first.into()).unwrap();

    assert_eq!(
        receive(&socket, &mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn missing_pktinfo_is_rejected_even_for_loopback_unicast() {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut buffer = [0; 32];

    peer.send_to(b"probe", socket.local_addr().unwrap())
        .await
        .unwrap();

    let packet = tokio::time::timeout(
        Duration::from_secs(2),
        socket.async_io(Interest::READABLE, || receive(&socket, &mut buffer)),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(packet.is_none());
}

#[tokio::test]
async fn isolated_search_round_trip_returns_all_targets_from_selected_source() {
    let discovery = loopback();
    let address = discovery.socket.local_addr().unwrap();
    let notifications = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

    let std::net::SocketAddr::V4(destination) = notifications.local_addr().unwrap() else {
        panic!("IPv4 socket");
    };

    let task = tokio::spawn(discovery.run_to(destination));
    let mut buffer = [0; DATAGRAM_BYTES];

    peer.send_to(search("ssdp:all", "1").as_bytes(), address)
        .await
        .unwrap();

    let mut received = Vec::new();

    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..5 {
            let (length, source) = peer.recv_from(&mut buffer).await.unwrap();
            assert_eq!(source, address);
            let message = std::str::from_utf8(&buffer[..length]).unwrap();
            assert!(message.starts_with("HTTP/1.1 200 OK\r\n"));

            received.push(
                message
                    .split("\r\n")
                    .find_map(|line| line.strip_prefix("ST: "))
                    .unwrap()
                    .to_owned(),
            );
        }
    })
    .await
    .unwrap();

    received.sort();
    let mut expected = targets(UUID);
    expected.sort();
    assert_eq!(received, expected);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn sends_startup_repeat_and_periodic_alive_announcements() {
    let discovery = loopback();
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

    let std::net::SocketAddr::V4(destination) = peer.local_addr().unwrap() else {
        panic!("IPv4 socket");
    };

    let task = tokio::spawn(discovery.run_to(destination));
    let mut buffer = [0; DATAGRAM_BYTES];

    for advance in [
        Duration::ZERO,
        STARTUP_REPEAT,
        ANNOUNCEMENT_INTERVAL - STARTUP_REPEAT,
    ] {
        tokio::time::advance(advance).await;
        tokio::task::yield_now().await;

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut received = 0;

        // Keep the paused clock from auto-advancing while waiting for OS I/O.
        while received < 5 {
            assert!(
                std::time::Instant::now() < deadline,
                "missing ssdp:alive datagrams"
            );

            match peer.try_recv_from(&mut buffer) {
                Ok((length, _)) => {
                    assert!(
                        std::str::from_utf8(&buffer[..length])
                            .unwrap()
                            .contains("ssdp:alive")
                    );

                    received += 1;
                }

                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }

                Err(error) => panic!("receive announcement: {error}"),
            }
        }

        assert_eq!(
            peer.try_recv_from(&mut buffer).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
