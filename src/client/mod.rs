use std::{
    net::{IpAddr, SocketAddr},
    ops::Deref,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use atomic_enum::atomic_enum;
use hickory_resolver::{Resolver, config::*, name_server::TokioConnectionProvider};
use nohash_hasher::IntMap;
use socket2::Domain;
use thiserror::Error;
use tokio::{net::UdpSocket, task::JoinSet};
use tracing::{debug, warn};

use crate::{
    buffers::{byte_writer::ByteWriter, multi_buffer_pool::MultiBufferPool},
    client::builder::ClientBuilder,
    message::{Protocol, QunetMessage},
    protocol::DEFAULT_PORT,
    transport::tcp::ClientTcpTransport,
};

mod builder;
mod event_handler;

pub use event_handler::*;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    #[default]
    Qunet,
    Udp,
    Tcp,
    Quic,
}

impl ConnectionType {
    pub fn from_str(proto: &str) -> Result<ConnectionType, ConnectionError> {
        match proto {
            "qunet" => Ok(ConnectionType::Qunet),
            "udp" => Ok(ConnectionType::Udp),
            "tcp" => Ok(ConnectionType::Tcp),
            "quic" => Ok(ConnectionType::Quic),
            _ => Err(ConnectionError::InvalidProtocol),
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientOutcome {
    #[error("Client shut down gracefully")]
    GracefulShutdown,
    #[error("Application error: {0}")]
    CustomError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Connection already in progress")]
    InProgress,
    #[error("Already connected")]
    AlreadyConnected,
    #[error("Invalid protocol in URL")]
    InvalidProtocol,
    #[error("DNS resolution failed: {0}")]
    DnsError(#[from] hickory_resolver::ResolveError),
    #[error("DNS resolution failed: no results")]
    DnsNoResults,
    #[error("All connection attempts failed")]
    AllAttemptsFailed,
}

#[atomic_enum]
#[derive(PartialEq, Eq)]
enum ConnectionState {
    Disconnected,
    DnsResolving,
    Pinging,
    Connecting,
    Connected,
}

pub struct Client<H: EventHandler> {
    pub(crate) _builder: ClientBuilder<H>,
    pub(crate) event_handler: H,
    pub(crate) buffer_pool: Arc<MultiBufferPool>,
    conn_state: AtomicConnectionState,
    resolver: Option<Resolver<TokioConnectionProvider>>,
}

impl Client<DefaultEventHandler> {
    pub fn builder() -> ClientBuilder<DefaultEventHandler> {
        ClientBuilder::<DefaultEventHandler>::default().with_event_handler(DefaultEventHandler)
    }
}

impl<H: EventHandler> Client<H> {
    type Handle = ClientHandle<H>;

    pub fn from_builder(mut builder: ClientBuilder<H>) -> Self {
        let event_handler =
            builder.event_handler.take().expect("Event handler must be set in the builder");

        Self {
            _builder: builder,
            event_handler,
            buffer_pool: Arc::new(MultiBufferPool::new()),
            conn_state: ConnectionState::Disconnected.into(),
            resolver: None,
        }
    }

    pub(crate) async fn setup(&mut self) -> Result<(), ClientOutcome> {
        self.event_handler.pre_setup(self).await.map_err(ClientOutcome::CustomError)?;

        self.resolver = Some(
            Resolver::builder_with_config(
                ResolverConfig::cloudflare(),
                TokioConnectionProvider::default(),
            )
            .build(),
        );

        self.event_handler.post_setup(self).await.map_err(ClientOutcome::CustomError)?;

        Ok(())
    }

    // various connection state methods

    pub fn connecting(&self) -> bool {
        let s = self.conn_state.load(Ordering::Relaxed);

        !matches!(s, ConnectionState::Connected | ConnectionState::Disconnected)
    }

    pub fn connected(&self) -> bool {
        let s = self.conn_state.load(Ordering::Relaxed);

        matches!(s, ConnectionState::Connected)
    }

    /// Attempt to asynchronously connect to the server at the given URL.
    /// See qunet-cpp Connection class for the URL format.
    pub fn connect(self: ClientHandle<H>, url: &str) -> Result<(), ConnectionError> {
        let proto = url.split("://").next().unwrap_or("qunet");
        let proto = ConnectionType::from_str(proto)?;

        // remove trailing slashes
        let url = url.trim_end_matches('/');

        // check if it's an IP+port / IP / domain+port / domain
        if let Ok(addr) = url.parse::<SocketAddr>() {
            self.connect_ip(addr, proto)
        } else if let Ok(ip) = url.parse::<IpAddr>() {
            self.connect_ip(SocketAddr::new(ip, DEFAULT_PORT), proto)
        } else if let Some((domain, port)) = url.split_once(':') {
            let port = port.parse::<u16>().map_err(|_| ConnectionError::InvalidProtocol)?;
            self.connect_domain(domain, Some(port), proto)
        } else {
            self.connect_domain(url, None, proto)
        }
    }

    fn _swap_state(&self, from: ConnectionState, to: ConnectionState) -> bool {
        self.conn_state.compare_exchange(from, to, Ordering::SeqCst, Ordering::SeqCst).is_ok()
    }

    fn _set_state(&self, state: ConnectionState) {
        self.conn_state.store(state, Ordering::SeqCst);
    }

    pub async fn run_main_loop(self: ClientHandle<H>) -> ClientOutcome {
        ClientOutcome::GracefulShutdown
    }

    pub fn connect_ip(
        self: ClientHandle<H>,
        addr: SocketAddr,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        if !self._swap_state(ConnectionState::Disconnected, ConnectionState::DnsResolving) {
            return Err(ConnectionError::InProgress);
        }

        Ok(())
    }

    pub fn connect_domain(
        self: ClientHandle<H>,
        hostname: &str,
        port: Option<u16>,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        if !self._swap_state(ConnectionState::Disconnected, ConnectionState::DnsResolving) {
            return Err(ConnectionError::InProgress);
        }

        // ensure the hostname ends with a dot
        let hostname = if hostname.ends_with('.') {
            hostname.to_owned()
        } else {
            format!("{hostname}.")
        };

        tokio::spawn(async move {
            // if a port number is provided, skip SRV query and resolve A/AAAA records
            let res = if let Some(port) = port {
                self._dns_fetch_ip_and_connect(&hostname, port, ty).await
            } else {
                match self._dns_fetch_srv_and_connect(&hostname, ty).await {
                    Ok(()) => Ok(()),
                    Err(ConnectionError::DnsNoResults) => {
                        debug!("no results for {hostname} (SRV), trying A/AAAA records");
                        // fallback to A/AAAA records if SRV query fails
                        self._dns_fetch_ip_and_connect(&hostname, DEFAULT_PORT, ty).await
                    }
                    Err(e) => Err(e),
                }
            };

            if let Err(e) = res {
                warn!("Failed to connect to {hostname}: {e}");
                // TODO: post event handler stuff idk
                self._set_state(ConnectionState::Disconnected);
            }
        });

        Ok(())
    }

    async fn _dns_fetch_ip_and_connect(
        &self,
        hostname: &str,
        port: u16,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        let resolver = self.resolver.as_ref().unwrap();

        let res = resolver.lookup_ip(hostname).await?;
        let addrs: Vec<_> = res.iter().map(|ip| SocketAddr::new(ip, port)).collect();

        if addrs.is_empty() {
            return Err(ConnectionError::DnsNoResults);
        }

        self._dns_post_query(addrs, ty).await
    }

    async fn _dns_fetch_srv_and_connect(
        &self,
        hostname: &str,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        let resolver = self.resolver.as_ref().unwrap();

        let res = resolver.srv_lookup(hostname).await?;
        let record = match res.iter().next() {
            Some(record) => record,
            None => return Err(ConnectionError::DnsNoResults),
        };

        let sockaddrs: Vec<_> = res
            .ip_iter()
            .map(|x| {
                let port = record.port();
                SocketAddr::new(x, if port != 0 { port } else { DEFAULT_PORT })
            })
            .collect();

        if sockaddrs.is_empty() {
            return Err(ConnectionError::DnsNoResults);
        }

        self._dns_post_query(sockaddrs, ty).await
    }

    async fn _dns_post_query(
        &self,
        addrs: Vec<SocketAddr>,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        // if connection type is Qunet, we need to try and ping the addresses, otherwise connect directly
        if ty == ConnectionType::Qunet {
            self._ping_addrs(addrs).await
        } else {
            self._try_connect_all(&addrs.iter().map(|&addr| (addr, ty)).collect::<Vec<_>>()).await
        }
    }

    async fn _ping_addrs(&self, mut addrs: Vec<SocketAddr>) -> Result<(), ConnectionError> {
        // sort addresses by IP version, prefer IPv6 over IPv4
        addrs.sort_unstable_by_key(|addr| match addr.ip() {
            IpAddr::V6(_) => 0,
            IpAddr::V4(_) => 1,
        });

        let bind_addr = "[::]:0".parse::<SocketAddr>().unwrap();

        let socket = socket2::Socket::new(Domain::IPV6, socket2::Type::DGRAM, None)
            .expect("Failed to create UDP socket");

        socket.set_only_v6(false).expect("Failed to set IPV6_V6ONLY to false");
        socket.set_nonblocking(true).expect("Failed to set socket to non-blocking");
        socket.bind(&bind_addr.into()).expect("Failed to bind UDP socket");
        let socket =
            UdpSocket::from_std(socket.into()).expect("Failed to convert socket to UdpSocket");

        let mut addrmap = IntMap::default();

        let mut hdrbuf = [0u8; 8];
        let mut bodybuf = [0u8; 32];

        let mut ping_id: u32 = rand::random();

        let mut failed = 0usize;

        for addr in &addrs {
            ping_id = ping_id.wrapping_add(1);

            debug!("Pinging address: {addr}");

            addrmap.insert(ping_id, *addr);

            let msg = QunetMessage::Ping { ping_id, omit_protocols: false };

            let mut hdrwriter = ByteWriter::new(&mut hdrbuf);
            let mut bodywriter = ByteWriter::new(&mut bodybuf);

            msg.encode_control_msg(&mut hdrwriter, &mut bodywriter)
                .expect("Failed to encode ping message");

            // join into a single buffer
            let mut total_buf = [0u8; 40];
            let mut writer = ByteWriter::new(&mut total_buf);
            writer.write_bytes(hdrwriter.written());
            writer.write_bytes(bodywriter.written());

            // send the ping message, ignore failures (may happen if network does not support ipv6)
            if socket.send_to(writer.written(), addr).await.is_err() {
                failed += 1;
            }
        }

        let mut arrived = Vec::new();
        let started_at = Instant::now();

        let mut buf = [0u8; 1024];

        loop {
            if arrived.len() == (addrmap.len() - failed) {
                break;
            }

            let mut timeout = Duration::from_millis(2500).saturating_sub(started_at.elapsed());
            if !arrived.is_empty() {
                timeout = timeout.min(Duration::from_millis(50));
            }

            match tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await {
                Ok(Ok((len, addr))) => {
                    let data = &buf[..len];
                    let message = match QunetMessage::parse_header(data, true)
                        .and_then(|x| QunetMessage::decode(x, &self.buffer_pool))
                    {
                        Ok(msg) => msg,
                        Err(e) => {
                            warn!("Failed to parse message from {addr}: {e}");
                            continue;
                        }
                    };

                    match message {
                        QunetMessage::Pong { ping_id, protocols, .. } => {
                            if let Some(addr) = addrmap.get(&ping_id) {
                                arrived.push((*addr, started_at.elapsed(), protocols));
                            }
                        }

                        _ => {
                            warn!(
                                "Received unexpected message from {addr}: {}",
                                message.type_str()
                            );
                            continue;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("Failed to receive pong data: {e}");
                }
                Err(_) => break,
            }
        }

        let mut to_try = Vec::new();

        if arrived.is_empty() {
            warn!("No pings arrived, will try all possible addresses and connection types");

            for addr in &addrs {
                to_try.push((*addr, ConnectionType::Tcp));
                to_try.push((*addr, ConnectionType::Quic));
                to_try.push((*addr, ConnectionType::Udp));
            }
        } else {
            // sort by latency, slightly prefer ipv6
            arrived.sort_unstable_by(|(a_addr, a_latency, _), (b_addr, b_latency, _)| {
                use std::cmp::Ordering;

                let a_v6 = a_addr.ip().is_ipv6();
                let b_v6 = b_addr.ip().is_ipv6();

                let lat_diff = a_latency.abs_diff(*b_latency);

                if lat_diff.as_millis() < 30 {
                    // if latencies are close, prefer ipv6
                    if a_v6 && !b_v6 {
                        Ordering::Less
                    } else if !a_v6 && b_v6 {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                } else {
                    a_latency.cmp(b_latency)
                }
            });

            for (addr, _, protos) in &arrived {
                // try all protocols that were advertised in the pong message
                for proto in protos {
                    if let Some(port) = proto.as_tcp() {
                        to_try.push((SocketAddr::new(addr.ip(), port), ConnectionType::Tcp));
                    } else if let Some(port) = proto.as_quic() {
                        to_try.push((SocketAddr::new(addr.ip(), port), ConnectionType::Quic));
                    } else if let Some(port) = proto.as_udp() {
                        to_try.push((SocketAddr::new(addr.ip(), port), ConnectionType::Udp));
                    } else {
                        warn!("Received unsupported protocol in pong: {:?}", proto.protocol);
                        continue;
                    }
                }
            }
        }

        self._try_connect_all(&to_try).await
    }

    async fn _try_connect_all(
        &self,
        conns: &[(SocketAddr, ConnectionType)],
    ) -> Result<(), ConnectionError> {
        self._set_state(ConnectionState::Connecting);

        for (addr, ty) in conns {
            match self._try_connect(*addr, *ty).await {
                Ok(()) => {}

                Err(e) => {
                    warn!("Failed to connect to {addr} with protocol {ty:?}: {e}");
                }
            }
        }

        Err(ConnectionError::AllAttemptsFailed)
    }

    async fn _try_connect(
        &self,
        addr: SocketAddr,
        ty: ConnectionType,
    ) -> Result<(), ConnectionError> {
        let idle_timeout = Duration::from_secs(60);

        let fut = async move {
            // match ty {
            //     ConnectionType::Tcp => {
            //         ClientTcpTransport::connect(addr, idle_timeout).await;
            //     }
            // }
        };

        Ok(())
    }
}

pub struct ClientHandle<H: EventHandler> {
    inner: Arc<Client<H>>,
}

impl<H: EventHandler> Clone for ClientHandle<H> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<H: EventHandler> ClientHandle<H> {
    pub async fn run(self) -> ClientOutcome {
        Client::run_main_loop(self.clone()).await
    }
}

impl<H: EventHandler> Deref for ClientHandle<H> {
    type Target = Client<H>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
