use std::{
    io,
    mem::{size_of, size_of_val},
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
    time::Duration,
};

use anyhow::{Context, ensure};
use socket2::{Domain, InterfaceIndexOrAddress, Protocol, Socket, Type};
use tokio::{io::Interest, net::UdpSocket, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const SSDP_MAX_AGE: u64 = 1800;
const SSDP_ANNOUNCEMENT_INTERVAL: Duration = Duration::from_secs(900);
const SSDP_TTL: u32 = 2;
const SSDP_DATAGRAM_BYTES: usize = 8 * 1024;
const SSDP_PENDING_RESPONSES: usize = 256;
const SSDP_RESPONSES_PER_SECOND: u32 = 50;
const SSDP_RESPONSE_BURST: u32 = 100;

const MULTICAST: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900);
const STARTUP_REPEAT: Duration = Duration::from_secs(1);
const RESPONSE_SPACING: Duration =
    Duration::from_nanos(1_000_000_000 / SSDP_RESPONSES_PER_SECOND as u64);

pub struct Discovery {
    socket: UdpSocket,
    address: Ipv4Addr,
    interface_index: u32,
    targets: [String; 5],
    location: String,
}

impl Discovery {
    /// Bind without announcing; call `run` only after HTTP startup succeeds.
    pub fn bind(
        address: Ipv4Addr,
        interface_index: u32,
        uuid: Uuid,
        http_address: SocketAddrV4,
    ) -> anyhow::Result<Self> {
        ensure!(is_unicast(address), "SSDP requires a unicast IPv4 address");

        ensure!(
            interface_index > 0 && interface_index <= i32::MAX as u32,
            "invalid SSDP interface index"
        );

        ensure!(
            *http_address.ip() == address && http_address.port() != 0 && !uuid.is_nil(),
            "SSDP identity and HTTP address must match the configured server"
        );

        let socket = make_socket(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MULTICAST.port()))
            .context("bind shared SSDP UDP port")?;

        socket
            .set_multicast_if_v4(&address)
            .context("select SSDP multicast source")?;

        socket
            .join_multicast_v4_n(
                MULTICAST.ip(),
                &InterfaceIndexOrAddress::Index(interface_index),
            )
            .context("join SSDP multicast group on configured interface")?;

        let socket =
            UdpSocket::from_std(socket.into()).context("register SSDP socket with Tokio")?;

        Ok(Self {
            socket,
            address,
            interface_index,
            targets: targets(uuid),
            location: format!("http://{http_address}/device.xml"),
        })
    }

    pub async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.run_to(shutdown, MULTICAST).await
    }

    async fn run_to(
        self,
        shutdown: CancellationToken,
        announcement_destination: SocketAddrV4,
    ) -> anyhow::Result<()> {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let result = self.serve(&shutdown, announcement_destination).await;
        let withdrawal = self.announce(Message::Byebye, announcement_destination);

        result.and(withdrawal)
    }

    async fn serve(
        &self,
        shutdown: &CancellationToken,
        announcement_destination: SocketAddrV4,
    ) -> anyhow::Result<()> {
        self.announce(Message::Alive, announcement_destination)?;

        let start = Instant::now();
        let mut announcement = start + STARTUP_REPEAT;
        let mut startup_repeat = true;
        let mut responses = Responses::new(start);
        let mut buffer = [0; SSDP_DATAGRAM_BYTES];

        loop {
            let now = Instant::now();
            let response_deadline = responses.next_deadline(now);

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => return Ok(()),

                _ = tokio::time::sleep_until(announcement) => {
                    self.announce(Message::Alive, announcement_destination)?;

                    announcement = if startup_repeat {
                        startup_repeat = false;

                        start + SSDP_ANNOUNCEMENT_INTERVAL
                    } else {
                        Instant::now() + SSDP_ANNOUNCEMENT_INTERVAL
                    };
                }

                _ = tokio::time::sleep_until(response_deadline.unwrap_or(announcement)),
                    if response_deadline.is_some() =>
                {
                    if let Some(response) = responses.pop_due(Instant::now()) {
                        let message = self.message(response.target, Message::Response);

                        if let Err(error) = self.send(message.as_bytes(), response.destination) {
                            tracing::debug!(%error, "SSDP search response dropped");
                        }
                    }
                }

                received = self.socket.async_io(Interest::READABLE, || {
                    receive(&self.socket, &mut buffer)
                }) => {
                    let packet = match received {
                        Ok(Some(packet)) => packet,

                        Ok(None) => continue,

                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,

                        Err(error) => return Err(error).context("receive SSDP datagram"),
                    };

                    if !packet.accepted(self.address, self.interface_index) {
                        continue;
                    }

                    if let Some(search) = parse_search(&buffer[..packet.length], &self.targets) {
                        responses.enqueue(search, packet.source, Instant::now(), || {
                            rand::random_range(0..search.mx.as_nanos() as u64)
                        });
                    }
                }
            }
        }
    }

    fn send(&self, bytes: &[u8], destination: SocketAddrV4) -> io::Result<()> {
        send(
            &self.socket,
            bytes,
            destination,
            self.address,
            self.interface_index,
        )
    }

    fn announce(&self, kind: Message, destination: SocketAddrV4) -> anyhow::Result<()> {
        let mut result = Ok(());

        for target in 0..self.targets.len() {
            if let Err(error) = self.send(self.message(target, kind).as_bytes(), destination) {
                if error.kind() == io::ErrorKind::WouldBlock {
                    tracing::warn!("SSDP announcement dropped: UDP send buffer full");
                } else if result.is_ok() {
                    result = Err(error).context("send SSDP announcement");
                }
            }
        }

        result
    }

    fn message(&self, target: usize, kind: Message) -> String {
        let name = &self.targets[target];

        let usn = if target == 1 {
            self.targets[1].clone()
        } else {
            format!("{}::{name}", self.targets[1])
        };

        let mut message = match kind {
            Message::Response => format!("HTTP/1.1 200 OK\r\nEXT:\r\nST: {name}\r\n"),

            Message::Alive | Message::Byebye => {
                let nts = match kind {
                    Message::Alive => "ssdp:alive",
                    _ => "ssdp:byebye",
                };

                format!("NOTIFY * HTTP/1.1\r\nHOST: {MULTICAST}\r\nNT: {name}\r\nNTS: {nts}\r\n")
            }
        };

        if !matches!(kind, Message::Byebye) {
            message.push_str(&format!(
                "CACHE-CONTROL: max-age={}\r\nLOCATION: {}\r\nSERVER: {}\r\n",
                SSDP_MAX_AGE,
                self.location,
                crate::server_header(),
            ));
        }

        message.push_str(&format!("USN: {usn}\r\n\r\n"));

        message
    }
}

#[derive(Clone, Copy)]
enum Message {
    Alive,
    Byebye,
    Response,
}

fn targets(uuid: Uuid) -> [String; 5] {
    [
        "upnp:rootdevice".into(),
        format!("uuid:{uuid}"),
        "urn:schemas-upnp-org:device:MediaServer:1".into(),
        "urn:schemas-upnp-org:service:ContentDirectory:1".into(),
        "urn:schemas-upnp-org:service:ConnectionManager:1".into(),
    ]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Search {
    target: Option<usize>,
    mx: Duration,
}

fn parse_search(bytes: &[u8], targets: &[String; 5]) -> Option<Search> {
    if bytes.len() > SSDP_DATAGRAM_BYTES || !bytes.is_ascii() {
        return None;
    }

    let text = std::str::from_utf8(bytes).ok()?.strip_suffix("\r\n\r\n")?;
    let mut lines = text.split("\r\n");

    if lines.next()? != "M-SEARCH * HTTP/1.1" {
        return None;
    }

    let (mut host, mut man, mut mx, mut st) = (None, None, None, None);

    for line in lines {
        let (name, value) = line.split_once(':')?;

        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return None;
        }

        let slot = if name.eq_ignore_ascii_case("HOST") {
            &mut host
        } else if name.eq_ignore_ascii_case("MAN") {
            &mut man
        } else if name.eq_ignore_ascii_case("MX") {
            &mut mx
        } else if name.eq_ignore_ascii_case("ST") {
            &mut st
        } else {
            continue;
        };

        if slot.replace(value.trim_matches([' ', '\t'])).is_some() {
            return None;
        }
    }

    if host? != "239.255.255.250:1900" || !man?.eq_ignore_ascii_case("\"ssdp:discover\"") {
        return None;
    }

    let mx = mx?;

    if mx.is_empty() || !mx.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let mx: u32 = mx.parse().ok()?;

    if mx == 0 {
        return None;
    }

    let st = st?;

    let target = if st == "ssdp:all" {
        None
    } else {
        Some(targets.iter().position(|target| target == st)?)
    };

    Some(Search {
        target,
        mx: Duration::from_secs(u64::from(mx.min(5))),
    })
}

struct Pending {
    target: usize,
    destination: SocketAddrV4,
    due: Instant,
    expires: Instant,
}

struct Responses {
    pending: Vec<Pending>,
    // One response consumes RESPONSE_SPACING of accumulated refill time.
    credit: Duration,
    updated: Instant,
}

impl Responses {
    fn new(now: Instant) -> Self {
        Self {
            pending: Vec::with_capacity(SSDP_PENDING_RESPONSES),
            credit: RESPONSE_SPACING * SSDP_RESPONSE_BURST,
            updated: now,
        }
    }

    fn enqueue(
        &mut self,
        search: Search,
        destination: SocketAddrV4,
        now: Instant,
        mut random_delay_nanos: impl FnMut() -> u64,
    ) {
        self.pending.retain(|response| response.expires > now);

        let count = if search.target.is_some() { 1 } else { 5 };

        if self.pending.len() + count > SSDP_PENDING_RESPONSES {
            return;
        }

        for target in 0..5 {
            if search.target.is_some_and(|selected| selected != target) {
                continue;
            }

            self.pending.push(Pending {
                target,
                destination,
                due: now + Duration::from_nanos(random_delay_nanos()).min(search.mx),
                expires: now + search.mx,
            });
        }
    }

    fn next_deadline(&mut self, now: Instant) -> Option<Instant> {
        self.pending.retain(|response| response.expires > now);

        self.credit = (self.credit + now.duration_since(self.updated))
            .min(RESPONSE_SPACING * SSDP_RESPONSE_BURST);

        self.updated = now;

        let due = self.pending.iter().map(|response| response.due).min()?;
        let expires = self.pending.iter().map(|response| response.expires).min()?;
        let available = now + RESPONSE_SPACING.saturating_sub(self.credit);

        Some(due.max(available).min(expires))
    }

    fn pop_due(&mut self, now: Instant) -> Option<Pending> {
        self.next_deadline(now)?;

        if self.credit < RESPONSE_SPACING {
            return None;
        }

        let index = self
            .pending
            .iter()
            .enumerate()
            .filter(|(_, response)| response.due <= now)
            .min_by_key(|(_, response)| response.due)?
            .0;

        self.credit -= RESPONSE_SPACING;

        Some(self.pending.swap_remove(index))
    }
}

fn is_unicast(address: Ipv4Addr) -> bool {
    address.octets()[0] != 0 && address.octets()[0] < 224
}

#[derive(Debug)]
struct Packet {
    length: usize,
    source: SocketAddrV4,
    destination: Ipv4Addr,
    interface_index: u32,
}

impl Packet {
    fn accepted(&self, address: Ipv4Addr, interface_index: u32) -> bool {
        self.interface_index == interface_index
            && is_unicast(*self.source.ip())
            && (!self.source.ip().is_loopback() || address.is_loopback())
            && self.source.port() != 0
            && (self.destination == *MULTICAST.ip() || self.destination == address)
    }
}

fn make_socket(address: SocketAddrV4) -> io::Result<Socket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_multicast_all_v4(false)?;
    socket.set_multicast_ttl_v4(SSDP_TTL)?;
    socket.set_nonblocking(true)?;
    let enabled: libc::c_int = 1;

    // SAFETY: the live socket and correctly sized integer remain valid for setsockopt.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_PKTINFO,
            (&enabled as *const libc::c_int).cast(),
            size_of_val(&enabled) as libc::socklen_t,
        )
    };

    if result == -1 {
        return Err(io::Error::last_os_error());
    }

    socket.bind(&address.into())?;

    Ok(socket)
}

fn receive(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<Option<Packet>> {
    let mut control = [0usize; 16];

    // SAFETY: these C structs admit all-zero initialization. All recvmsg pointers
    // refer to live, writable stack storage; usize aligns ancillary headers.
    let (mut source, mut message): (libc::sockaddr_in, libc::msghdr) =
        unsafe { (std::mem::zeroed(), std::mem::zeroed()) };

    let mut vector = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };

    message.msg_name = (&mut source as *mut libc::sockaddr_in).cast();
    message.msg_namelen = size_of_val(&source) as libc::socklen_t;
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = size_of_val(&control);

    // SAFETY: message's buffers and socket are valid for the duration of recvmsg.
    let length = unsafe {
        libc::recvmsg(
            socket.as_raw_fd(),
            &mut message,
            libc::MSG_DONTWAIT | libc::MSG_TRUNC,
        )
    };

    if length < 0 {
        return Err(io::Error::last_os_error());
    }

    if length as usize > buffer.len()
        || message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0
        || message.msg_namelen as usize != size_of_val(&source)
        || source.sin_family != libc::AF_INET as libc::sa_family_t
    {
        return Ok(None);
    }

    let mut info = None;

    // SAFETY: only kernel-produced, untruncated ancillary data is traversed using
    // libc's bounds-aware macros. Check payload length before an unaligned copy.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);

        while !header.is_null() {
            if (*header).cmsg_level == libc::IPPROTO_IP && (*header).cmsg_type == libc::IP_PKTINFO {
                if (*header).cmsg_len
                    != libc::CMSG_LEN(size_of::<libc::in_pktinfo>() as u32) as usize
                    || info.is_some()
                {
                    return Ok(None);
                }

                info = Some(std::ptr::read_unaligned(
                    libc::CMSG_DATA(header).cast::<libc::in_pktinfo>(),
                ));
            }

            header = libc::CMSG_NXTHDR(&message, header);
        }
    }

    let Some(info) = info.filter(|info| info.ipi_ifindex > 0) else {
        return Ok(None);
    };

    Ok(Some(Packet {
        length: length as usize,
        source: SocketAddrV4::new(
            Ipv4Addr::from(source.sin_addr.s_addr.to_ne_bytes()),
            u16::from_be(source.sin_port),
        ),
        destination: Ipv4Addr::from(info.ipi_addr.s_addr.to_ne_bytes()),
        interface_index: info.ipi_ifindex as u32,
    }))
}

fn send(
    socket: &UdpSocket,
    bytes: &[u8],
    destination: SocketAddrV4,
    address: Ipv4Addr,
    interface_index: u32,
) -> io::Result<()> {
    if bytes.len() > SSDP_DATAGRAM_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "SSDP datagram too large",
        ));
    }

    let destination = socket2::SockAddr::from(destination);
    let mut control = [0usize; 16];

    let info = libc::in_pktinfo {
        ipi_ifindex: interface_index as i32,
        ipi_spec_dst: libc::in_addr {
            s_addr: u32::from_ne_bytes(address.octets()),
        },
        ipi_addr: libc::in_addr { s_addr: 0 },
    };

    let mut vector = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };

    // SAFETY: msghdr admits zero initialization. sendmsg only reads its payload,
    // destination and ancillary storage, all of which remain live through the call.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_name = destination.as_ptr().cast_mut().cast();
    message.msg_namelen = destination.len();
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();

    // Nonblocking, best-effort sends cannot hold up reception or shutdown.
    // SAFETY: the aligned control buffer exceeds CMSG_SPACE(in_pktinfo), and
    // CMSG_DATA has room for the copied value. No pointers escape this syscall.
    let sent = unsafe {
        message.msg_controllen = libc::CMSG_SPACE(size_of_val(&info) as u32) as usize;
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::IPPROTO_IP;
        (*header).cmsg_type = libc::IP_PKTINFO;
        (*header).cmsg_len = libc::CMSG_LEN(size_of_val(&info) as u32) as usize;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<libc::in_pktinfo>(), info);

        libc::sendmsg(socket.as_raw_fd(), &message, libc::MSG_DONTWAIT)
    };

    if sent < 0 {
        return Err(io::Error::last_os_error());
    }

    if sent as usize != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "incomplete SSDP datagram",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
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
        let padding = "a".repeat(SSDP_DATAGRAM_BYTES - prefix.len() - 7);
        let packet = format!("{prefix}X: {padding}\r\n\r\n");
        assert_eq!(packet.len(), SSDP_DATAGRAM_BYTES);
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

        assert_eq!(responses.pending.len(), SSDP_PENDING_RESPONSES);

        responses.enqueue(
            Search {
                target: Some(1),
                ..all
            },
            LOCAL,
            now,
            || 0,
        );

        assert_eq!(responses.pending.len(), SSDP_PENDING_RESPONSES);
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

        for _ in 0..SSDP_PENDING_RESPONSES {
            responses.enqueue(request, LOCAL, now, || 0);
        }

        for _ in 0..SSDP_RESPONSE_BURST {
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

        for tick in 2..=SSDP_RESPONSES_PER_SECOND {
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

    #[tokio::test]
    async fn all_wire_messages_have_exact_target_usn_and_framing() {
        let discovery = loopback();

        for target in 0..5 {
            for kind in [Message::Alive, Message::Byebye, Message::Response] {
                let message = discovery.message(target, kind);

                let usn = if target == 1 {
                    discovery.targets[1].clone()
                } else {
                    format!("{}::{}", discovery.targets[1], discovery.targets[target])
                };

                assert!(message.contains(&format!("USN: {usn}\r\n")));
                assert!(message.ends_with("\r\n\r\n"));
                assert!(!message.replace("\r\n", "").contains(['\r', '\n']));
                assert!(message.len() <= SSDP_DATAGRAM_BYTES);
                assert!(!message.contains("BOOTID"));
                assert!(!message.contains("CONFIGID"));

                if matches!(kind, Message::Response) {
                    assert!(message.starts_with("HTTP/1.1 200 OK\r\nEXT:\r\n"));
                    assert!(message.contains(&format!("ST: {}\r\n", discovery.targets[target])));
                } else {
                    assert!(message.starts_with("NOTIFY * HTTP/1.1\r\n"));
                    assert!(message.contains(&format!("HOST: {MULTICAST}\r\n")));
                    assert!(message.contains(&format!("NT: {}\r\n", discovery.targets[target])));
                }

                if matches!(kind, Message::Byebye) {
                    assert!(message.contains("NTS: ssdp:byebye\r\n"));
                    assert!(!message.contains("LOCATION:"));
                } else {
                    assert!(message.contains("CACHE-CONTROL: max-age=1800\r\n"));
                    assert!(message.contains(&format!("LOCATION: http://{LOCAL}/device.xml\r\n")));
                    assert!(message.contains(&format!("SERVER: {}\r\n", crate::server_header())));
                }
            }
        }
    }

    #[tokio::test]
    async fn unprivileged_loopback_pktinfo_source_and_datagram_truncation() {
        let discovery = loopback();
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let destination = discovery.socket.local_addr().unwrap();
        let mut buffer = [0; SSDP_DATAGRAM_BYTES];
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

        peer.send_to(&vec![b'x'; SSDP_DATAGRAM_BYTES + 1], destination)
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

        assert!(
            discovery
                .send(&vec![0; SSDP_DATAGRAM_BYTES + 1], LOCAL)
                .is_err()
        );
    }

    #[tokio::test]
    async fn shared_port_bind_does_not_send_or_require_capabilities() {
        let first = make_socket(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = first.local_addr().unwrap();
        let second = make_socket(address.as_socket_ipv4().unwrap()).unwrap();
        assert_eq!(second.local_addr().unwrap(), address);
        assert_eq!(first.multicast_ttl_v4().unwrap(), SSDP_TTL);
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

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(discovery.run_to(shutdown.clone(), destination));
        let mut buffer = [0; SSDP_DATAGRAM_BYTES];

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
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_repeats_reannounces_and_withdraws_on_isolated_unicast_socket() {
        let discovery = loopback();
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

        let std::net::SocketAddr::V4(destination) = peer.local_addr().unwrap() else {
            panic!("IPv4 socket");
        };

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(discovery.run_to(shutdown.clone(), destination));
        let mut buffer = [0; SSDP_DATAGRAM_BYTES];

        for (advance, nts) in [
            (Duration::ZERO, "ssdp:alive"),
            (STARTUP_REPEAT, "ssdp:alive"),
            (SSDP_ANNOUNCEMENT_INTERVAL - STARTUP_REPEAT, "ssdp:alive"),
            (Duration::ZERO, "ssdp:byebye"),
        ] {
            if nts == "ssdp:byebye" {
                shutdown.cancel();
            }

            tokio::time::advance(advance).await;
            tokio::task::yield_now().await;

            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            let mut received = 0;

            // Keep the paused clock from auto-advancing while waiting for OS I/O.
            while received < 5 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "missing {nts} datagrams"
                );

                match peer.try_recv_from(&mut buffer) {
                    Ok((length, _)) => {
                        assert!(
                            std::str::from_utf8(&buffer[..length])
                                .unwrap()
                                .contains(nts)
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

        task.await.unwrap().unwrap();
    }
}
