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
use uuid::Uuid;

const MAX_AGE: u64 = 1800;
const ANNOUNCEMENT_INTERVAL: Duration = Duration::from_secs(900);
const TTL: u32 = 2;
const DATAGRAM_BYTES: usize = 8 * 1024;
const PENDING_RESPONSES: usize = 256;
const RESPONSES_PER_SECOND: u32 = 50;
const RESPONSE_BURST: u32 = 100;

const MULTICAST: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900);
const STARTUP_REPEAT: Duration = Duration::from_secs(1);
const RESPONSE_SPACING: Duration =
    Duration::from_nanos(1_000_000_000 / RESPONSES_PER_SECOND as u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    RootDevice,
    Uuid,
    MediaServer,
    ContentDirectory,
    ConnectionManager,
}

impl Target {
    const ALL: [Self; 5] = [
        Self::RootDevice,
        Self::Uuid,
        Self::MediaServer,
        Self::ContentDirectory,
        Self::ConnectionManager,
    ];

    fn name(self, udn: &str) -> &str {
        match self {
            Self::RootDevice => "upnp:rootdevice",
            Self::Uuid => udn,
            Self::MediaServer => crate::protocol::MEDIA_SERVER,
            Self::ContentDirectory => crate::protocol::CONTENT_DIRECTORY,
            Self::ConnectionManager => crate::protocol::CONNECTION_MANAGER,
        }
    }
}

pub struct Discovery {
    socket: UdpSocket,
    address: Ipv4Addr,
    interface_index: u32,
    udn: String,
    location: String,
}

impl Discovery {
    /// Bind without announcing; call `run` only after HTTP startup succeeds.
    pub fn bind(
        interface_index: u32,
        uuid: Uuid,
        http_address: SocketAddrV4,
    ) -> anyhow::Result<Self> {
        let address = *http_address.ip();

        ensure!(is_unicast(address), "SSDP requires a unicast IPv4 address");

        ensure!(
            interface_index > 0 && interface_index <= i32::MAX as u32,
            "invalid SSDP interface index"
        );

        ensure!(
            http_address.port() != 0 && !uuid.is_nil(),
            "SSDP requires a nonzero HTTP port and server UUID"
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
            udn: format!("uuid:{uuid}"),
            location: format!("http://{http_address}/device.xml"),
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.run_to(MULTICAST).await
    }

    async fn run_to(self, announcement_destination: SocketAddrV4) -> anyhow::Result<()> {
        self.announce(announcement_destination)?;

        let start = Instant::now();
        let mut announcement = start + STARTUP_REPEAT;
        let mut startup_repeat = true;
        let mut responses = Responses::new(start);
        let mut buffer = [0; DATAGRAM_BYTES];

        loop {
            let now = Instant::now();
            let response_deadline = responses.next_deadline(now);

            tokio::select! {
                biased;

                _ = tokio::time::sleep_until(announcement) => {
                    self.announce(announcement_destination)?;

                    announcement = if startup_repeat {
                        startup_repeat = false;

                        start + ANNOUNCEMENT_INTERVAL
                    } else {
                        Instant::now() + ANNOUNCEMENT_INTERVAL
                    };
                }

                _ = tokio::time::sleep_until(response_deadline.unwrap_or(announcement)),
                    if response_deadline.is_some() =>
                {
                    if let Some(response) = responses.pop_due(Instant::now()) {
                        let message = message(
                            &self.udn,
                            &self.location,
                            response.target,
                            Message::Response,
                        );

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

                    if let Some(search) = parse_search(&buffer[..packet.length], &self.udn) {
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

    fn announce(&self, destination: SocketAddrV4) -> anyhow::Result<()> {
        let mut result = Ok(());

        for target in Target::ALL {
            let message = message(&self.udn, &self.location, target, Message::Alive);

            if let Err(error) = self.send(message.as_bytes(), destination) {
                if error.kind() == io::ErrorKind::WouldBlock {
                    tracing::warn!("SSDP announcement dropped: UDP send buffer full");
                } else if result.is_ok() {
                    result = Err(error).context("send SSDP announcement");
                }
            }
        }

        result
    }
}

#[derive(Clone, Copy)]
enum Message {
    Alive,
    Response,
}

fn message(udn: &str, location: &str, target: Target, kind: Message) -> String {
    let name = target.name(udn);

    let usn = if target == Target::Uuid {
        udn.to_owned()
    } else {
        format!("{udn}::{name}")
    };

    let mut message = match kind {
        Message::Response => format!("HTTP/1.1 200 OK\r\nEXT:\r\nST: {name}\r\n"),

        Message::Alive => {
            format!("NOTIFY * HTTP/1.1\r\nHOST: {MULTICAST}\r\nNT: {name}\r\nNTS: ssdp:alive\r\n")
        }
    };

    message.push_str(&format!(
        "CACHE-CONTROL: max-age={}\r\nLOCATION: {}\r\nSERVER: {}\r\n",
        MAX_AGE,
        location,
        crate::server_header(),
    ));

    message.push_str(&format!("USN: {usn}\r\n\r\n"));

    message
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Search {
    target: Option<Target>,
    mx: Duration,
}

fn parse_search(bytes: &[u8], udn: &str) -> Option<Search> {
    if bytes.len() > DATAGRAM_BYTES || !bytes.is_ascii() {
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
        Some(
            Target::ALL
                .into_iter()
                .find(|&target| target.name(udn) == st)?,
        )
    };

    Some(Search {
        target,
        mx: Duration::from_secs(u64::from(mx.min(5))),
    })
}

struct Pending {
    target: Target,
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
            pending: Vec::with_capacity(PENDING_RESPONSES),
            credit: RESPONSE_SPACING * RESPONSE_BURST,
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

        let count = if search.target.is_some() {
            1
        } else {
            Target::ALL.len()
        };

        if self.pending.len() + count > PENDING_RESPONSES {
            return;
        }

        for target in Target::ALL {
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

        self.credit =
            (self.credit + now.duration_since(self.updated)).min(RESPONSE_SPACING * RESPONSE_BURST);

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
    socket.set_multicast_ttl_v4(TTL)?;
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
    if bytes.len() > DATAGRAM_BYTES {
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

    // Nonblocking, best-effort sends cannot hold up reception.
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
mod tests;
