use std::{
    net::SocketAddr,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(feature = "client")]
use crate::client::{Client, EventHandler};
use crate::{
    buffers::HybridBufferPool,
    message::{CompressionHeader, CompressionType, DataMessageKind, QunetMessage, channel},
    protocol::QunetHandshakeError,
    server::{
        Server, ServerHandle, app_handler::AppHandler, builder::ShouldCompressFn,
        client::ClientNotification,
    },
    transport::compression::CompressionHandler,
};

use tracing::trace;

use self::{tcp::ClientTcpTransport, udp::ClientUdpTransport};

#[cfg(target_os = "linux")]
use self::lowlevel::{SocketAddrCRepr, socket_addr_to_c};

#[cfg(feature = "quic")]
use self::quic::ClientQuicTransport;

use tracing::debug;
pub(crate) use udp_misc::*;

pub mod compression;
mod error;
pub mod lowlevel;
#[cfg(feature = "quic")]
pub mod quic;
mod rate_limiter;
mod stream;
pub mod tcp;
pub mod udp;
mod udp_misc;

pub use error::*;
pub use rate_limiter::RateLimiter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    Udp,
    Tcp,
    Quic,
}

pub(crate) enum QunetTransportKind {
    Udp(ClientUdpTransport),
    Tcp(ClientTcpTransport),
    #[cfg(feature = "quic")]
    Quic(ClientQuicTransport),
}

pub(crate) struct QunetTransportData {
    pub connection_id: u64,
    pub closed: bool,
    pub address: SocketAddr,
    pub qunet_major_version: u16,
    pub initial_qdb_hash: [u8; 16],
    pub message_size_limit: usize,
    pub buffer_pool: Arc<HybridBufferPool>,
    pub idle_timeout: Duration,
    pub last_data_exchange: Instant,
    pub keepalive_interval: Duration,
    pub compression_func: Arc<dyn ShouldCompressFn>,
    pub is_client: bool,
    pub rate_limiter: RateLimiter,

    #[cfg(target_os = "linux")]
    pub c_sockaddr_data: SocketAddrCRepr,
    #[cfg(target_os = "linux")]
    pub c_sockaddr_len: libc::socklen_t,
}

pub(crate) struct QunetTransport {
    pub(crate) kind: QunetTransportKind,
    pub data: QunetTransportData,
    pub notif_chan: (channel::Sender<ClientNotification>, channel::Receiver<ClientNotification>),
}

impl QunetTransportData {
    #[cfg(target_os = "linux")]
    pub fn c_sockaddr(&self) -> (&SocketAddrCRepr, libc::socklen_t) {
        (&self.c_sockaddr_data, self.c_sockaddr_len)
    }

    #[inline]
    fn update_exchange_time(&mut self) {
        self.last_data_exchange = Instant::now();
    }
}

impl QunetTransport {
    pub fn new_server<H: AppHandler>(
        kind: QunetTransportKind,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        server: ServerHandle<H>,
    ) -> Self {
        Self::new(
            false,
            kind,
            address,
            qunet_major_version,
            initial_qdb_hash,
            server.message_size_limit(),
            server.buffer_pool.clone(),
            server.listener_opts.idle_timeout,
            Duration::from_secs(2u64.pow(30)), // server never sends keepalives
            server.compression_func.clone(),
            server.create_rate_limiter(),
        )
    }

    #[cfg(feature = "client")]
    pub fn new_client<H: EventHandler>(
        kind: QunetTransportKind,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        client: &Client<H>,
    ) -> Self {
        use crate::{
            protocol::DEFAULT_MESSAGE_SIZE_LIMIT, server::builder::should_compress_adaptive,
        };

        Self::new(
            true,
            kind,
            address,
            qunet_major_version,
            initial_qdb_hash,
            DEFAULT_MESSAGE_SIZE_LIMIT,
            client.buffer_pool.clone(),
            Duration::from_secs(60),
            Duration::from_secs(30),
            Arc::new(should_compress_adaptive),
            RateLimiter::new_unlimited(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        is_client: bool,
        kind: QunetTransportKind,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        message_size_limit: usize,
        buffer_pool: Arc<HybridBufferPool>,
        idle_timeout: Duration,
        keepalive_interval: Duration,
        compression_func: Arc<dyn ShouldCompressFn>,
        rate_limiter: RateLimiter,
    ) -> Self {
        #[cfg(target_os = "linux")]
        let (c_sockaddr_data, c_sockaddr_len) = socket_addr_to_c(&address);

        Self {
            kind,
            data: QunetTransportData {
                is_client,
                address,
                connection_id: 0,
                closed: false,
                qunet_major_version,
                initial_qdb_hash,
                message_size_limit,
                buffer_pool,
                last_data_exchange: Instant::now(),
                idle_timeout,
                keepalive_interval,
                compression_func,
                rate_limiter,
                #[cfg(target_os = "linux")]
                c_sockaddr_data,
                #[cfg(target_os = "linux")]
                c_sockaddr_len,
            },
            notif_chan: channel::new_channel(16),
        }
    }

    #[inline]
    pub fn kind_str(&self) -> &'static str {
        match &self.kind {
            QunetTransportKind::Udp(_) => "UDP",
            QunetTransportKind::Tcp(_) => "TCP",
            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(_) => "QUIC",
        }
    }

    #[inline]
    pub fn transport_type(&self) -> TransportType {
        match &self.kind {
            QunetTransportKind::Udp(_) => TransportType::Udp,
            QunetTransportKind::Tcp(_) => TransportType::Tcp,
            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(_) => TransportType::Quic,
        }
    }

    #[inline]
    pub fn address(&self) -> SocketAddr {
        self.data.address
    }

    #[inline]
    pub fn set_connection_id(&mut self, connection_id: u64) {
        self.data.connection_id = connection_id;
    }

    #[inline]
    pub fn connection_id(&self) -> u64 {
        self.data.connection_id
    }

    #[inline]
    pub async fn send_handshake_response(
        &mut self,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
    ) -> Result<(), TransportError> {
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => {
                udp.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size).await
            }

            QunetTransportKind::Tcp(tcp) => {
                tcp.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size).await
            }

            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(quic) => {
                quic.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size).await
            }
        }
    }

    #[inline]
    pub async fn run_server_setup<H: AppHandler>(
        &mut self,
        server: &Server<H>,
    ) -> Result<(), TransportError> {
        self.data.update_exchange_time();

        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.run_server_setup(&self.data, server).await,
            QunetTransportKind::Tcp(tcp) => tcp.run_setup().await,
            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(quic) => quic.run_setup().await,
        }
    }

    #[inline]
    pub async fn run_server_cleanup<H: AppHandler>(
        &mut self,
        server: &Server<H>,
    ) -> Result<(), TransportError> {
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.run_server_cleanup(&self.data, server).await,
            QunetTransportKind::Tcp(tcp) => tcp.run_cleanup().await,
            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(quic) => quic.run_cleanup().await,
        }
    }

    #[inline]
    pub async fn receive_message(&mut self) -> Result<QunetMessage, TransportError> {
        let msg = self.receive_message_inner().await?;

        // if the client exceeded the rate limit, forcibly disconnect them
        if !self.data.rate_limiter.consume() {
            return Err(TransportError::RateLimitExceeded);
        }

        Ok(msg)
    }

    #[inline]
    async fn receive_message_inner(&mut self) -> Result<QunetMessage, TransportError> {
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.receive_message(&mut self.data).await,
            QunetTransportKind::Tcp(tcp) => tcp.receive_message(&mut self.data).await,
            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(quic) => quic.receive_message(&self.data).await,
        }
    }

    #[inline]
    pub fn until_timer_expiry(&self) -> Option<Duration> {
        let timeout = if self.data.is_client {
            // for clients, the timer expires when the keepalive interval is reached
            self.data.keepalive_interval.saturating_sub(self.data.last_data_exchange.elapsed())
        } else {
            // for servers, the timer expires when the idle timeout is reached
            self.data.idle_timeout.saturating_sub(self.data.last_data_exchange.elapsed())
        };

        match &self.kind {
            QunetTransportKind::Udp(udp) => Some(udp.until_timer_expiry().min(timeout)),
            QunetTransportKind::Tcp(_tcp) => Some(timeout),

            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(_) => None, // quic does keepalives internally
        }
    }

    #[inline]
    pub async fn handle_timer_expiry(&mut self) -> Result<(), TransportError> {
        // if there's been no activity for a while, close the connection
        let now = Instant::now();
        let since_last_exchange = now.duration_since(self.data.last_data_exchange);

        if since_last_exchange >= self.data.keepalive_interval {
            self.send_message(QunetMessage::Keepalive { timestamp: 0 }, None, &()).await?;
        }

        if since_last_exchange >= self.data.idle_timeout {
            debug!("[{}] idle timeout reached, closing connection", self.data.address);
            return Err(TransportError::IdleTimeout);
        }

        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.handle_timer_expiry(&mut self.data).await,
            _ => Ok(()),
        }
    }

    pub async fn decompress_message<C: CompressionHandler>(
        &mut self,
        msg: QunetMessage,
        ch: &C,
    ) -> Result<QunetMessage, TransportError> {
        if !msg.is_data_compressed() {
            return Ok(msg);
        }

        self.do_decompress_data_message(msg, ch).await
    }

    #[inline]
    pub async fn send_message<C: CompressionHandler>(
        &mut self,
        mut message: QunetMessage,
        opts: Option<QunetMessageOpts>,
        ch: &C,
    ) -> Result<(), TransportError> {
        let opts = opts.unwrap_or_default();

        // Compress this message?
        if message.is_data()
            && !opts.uncompressed
            && let Some(comp_type) = (self.data.compression_func)(message.data_bytes().unwrap())
        {
            message = self.do_compress_data_message(message, comp_type, ch).await?;
        }

        match &mut self.kind {
            QunetTransportKind::Udp(udp) => {
                udp.send_message(&mut self.data, message, opts.reliable).await
            }

            QunetTransportKind::Tcp(tcp) => tcp.send_message(&mut self.data, message).await,

            #[cfg(feature = "quic")]
            QunetTransportKind::Quic(quic) => quic.send_message(&self.data, message).await,
        }
    }

    /// Shorthand for sending a handshake error message.
    #[inline]
    pub async fn send_handshake_error<C: CompressionHandler>(
        &mut self,
        error_code: QunetHandshakeError,
        reason: Option<String>,
        ch: &C,
    ) -> Result<(), TransportError> {
        let message = QunetMessage::HandshakeFailure { error_code, reason };

        self.send_message(message, None, ch).await
    }

    async fn do_compress_data_message<C: CompressionHandler>(
        &mut self,
        message: QunetMessage,
        comp_type: CompressionType,
        ch: &C,
    ) -> Result<QunetMessage, TransportError> {
        let data_buf = message.data_bytes().expect("Non data message passed to compression check");

        if data_buf.is_empty() {
            return Ok(message);
        }

        let compressed_buf = match comp_type {
            CompressionType::Lz4 => ch.compress_lz4(data_buf)?,
            CompressionType::Zstd => ch.compress_zstd(data_buf, true)?,
            CompressionType::ZstdNoDict => ch.compress_zstd(data_buf, false)?,
        };

        trace!(
            "[{}] compressed msg ({}): {} -> {} bytes",
            self.data.address,
            comp_type,
            data_buf.len(),
            compressed_buf.len()
        );

        // discard compression if it doesn't help
        if compressed_buf.len() >= data_buf.len() {
            return Ok(message);
        }

        let compression_header = CompressionHeader {
            compression_type: comp_type,
            uncompressed_size: NonZeroU32::new(data_buf.len() as u32).unwrap(),
        };

        let reliability = match message {
            QunetMessage::Data { reliability, .. } => reliability,
            _ => unreachable!(),
        };

        Ok(QunetMessage::Data {
            kind: DataMessageKind::Regular { data: compressed_buf },
            reliability,
            compression: Some(compression_header),
        })
    }

    async fn do_decompress_data_message<C: CompressionHandler>(
        &mut self,
        message: QunetMessage,
        ch: &C,
    ) -> Result<QunetMessage, TransportError> {
        let compression_header = match &message {
            QunetMessage::Data { compression, .. } => {
                compression.as_ref().expect("Data message without compression header")
            }
            _ => unreachable!("Non data message passed to decompression check"),
        };

        let data = message.data_bytes().unwrap();
        let unc_size = compression_header.uncompressed_size.get() as usize;

        let buf = match compression_header.compression_type {
            CompressionType::Zstd => ch.decompress_zstd(data, unc_size, true)?,
            CompressionType::ZstdNoDict => ch.decompress_zstd(data, unc_size, false)?,
            CompressionType::Lz4 => ch.decompress_lz4(data, unc_size)?,
        };

        Ok(QunetMessage::Data {
            kind: DataMessageKind::Regular { data: buf },
            reliability: None,
            compression: None,
        })
    }
}

// just a helper function

#[inline]
pub fn exponential_moving_average<T: Into<f64>>(current: T, previous: T, alpha: f64) -> f64 {
    let current = current.into();
    let previous = previous.into();

    alpha * current + (1.0 - alpha) * previous
}

#[derive(Clone, Copy)]
pub struct QunetMessageOpts {
    pub reliable: bool,
    pub uncompressed: bool,
}

impl QunetMessageOpts {
    pub fn unreliable() -> Self {
        Self {
            reliable: false,
            ..Default::default()
        }
    }
}

impl Default for QunetMessageOpts {
    fn default() -> Self {
        Self {
            reliable: true,
            uncompressed: false,
        }
    }
}
