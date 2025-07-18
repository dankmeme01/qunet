use std::{net::SocketAddr, num::NonZeroU32, sync::Arc, time::Duration};

#[cfg(feature = "client")]
use crate::client::{Client, EventHandler};
use crate::{
    buffers::MultiBufferPool,
    message::{CompressionHeader, CompressionType, DataMessageKind, QunetMessage, channel},
    protocol::QunetHandshakeError,
    server::{Server, ServerHandle, app_handler::AppHandler, client::ClientNotification},
    transport::compression::CompressionHandler,
};

use self::{
    lowlevel::{SocketAddrCRepr, socket_addr_to_c},
    quic::ClientQuicTransport,
    tcp::ClientTcpTransport,
    udp::ClientUdpTransport,
};

pub(crate) use udp_misc::*;

pub mod compression;
mod error;
pub mod lowlevel;
pub mod quic;
mod stream;
pub mod tcp;
pub mod udp;
mod udp_misc;

pub use error::{QuicError, TransportError, TransportErrorOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    Udp,
    Tcp,
    Quic,
}

pub(crate) enum QunetTransportKind {
    Udp(ClientUdpTransport),
    Tcp(ClientTcpTransport),
    Quic(ClientQuicTransport),
}

pub(crate) struct QunetTransportData {
    pub connection_id: u64,
    pub closed: bool,
    pub address: SocketAddr,
    pub qunet_major_version: u16,
    pub initial_qdb_hash: [u8; 16],
    pub message_size_limit: usize,
    pub buffer_pool: Arc<MultiBufferPool>,

    c_sockaddr_data: SocketAddrCRepr,
    c_sockaddr_len: libc::socklen_t,
}

pub(crate) struct QunetTransport {
    pub(crate) kind: QunetTransportKind,
    pub data: QunetTransportData,
    pub notif_chan: (channel::Sender<ClientNotification>, channel::Receiver<ClientNotification>),
}

impl QunetTransportData {
    pub fn c_sockaddr(&self) -> (&SocketAddrCRepr, libc::socklen_t) {
        (&self.c_sockaddr_data, self.c_sockaddr_len)
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
            kind,
            address,
            qunet_major_version,
            initial_qdb_hash,
            server.message_size_limit(),
            server.buffer_pool.clone(),
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
        use crate::protocol::DEFAULT_MESSAGE_SIZE_LIMIT;

        Self::new(
            kind,
            address,
            qunet_major_version,
            initial_qdb_hash,
            DEFAULT_MESSAGE_SIZE_LIMIT,
            client.buffer_pool.clone(),
        )
    }

    pub fn new(
        kind: QunetTransportKind,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        message_size_limit: usize,
        buffer_pool: Arc<MultiBufferPool>,
    ) -> Self {
        let (c_sockaddr_data, c_sockaddr_len) = socket_addr_to_c(&address);

        Self {
            kind,
            data: QunetTransportData {
                address,
                connection_id: 0,
                closed: false,
                qunet_major_version,
                initial_qdb_hash,
                message_size_limit,
                buffer_pool,
                c_sockaddr_data,
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
            QunetTransportKind::Quic(_) => "QUIC",
        }
    }

    #[inline]
    pub fn transport_type(&self) -> TransportType {
        match &self.kind {
            QunetTransportKind::Udp(_) => TransportType::Udp,
            QunetTransportKind::Tcp(_) => TransportType::Tcp,
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
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.run_server_setup(&self.data, server).await,
            QunetTransportKind::Tcp(tcp) => tcp.run_setup().await,
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
            QunetTransportKind::Quic(quic) => quic.run_cleanup().await,
        }
    }

    #[inline]
    pub async fn receive_message(&mut self) -> Result<QunetMessage, TransportError> {
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.receive_message(&self.data).await,
            QunetTransportKind::Tcp(tcp) => tcp.receive_message(&self.data).await,
            QunetTransportKind::Quic(quic) => quic.receive_message(&self.data).await,
        }
    }

    #[inline]
    pub fn until_timer_expiry(&self) -> Option<Duration> {
        match &self.kind {
            QunetTransportKind::Udp(udp) => Some(udp.until_timer_expiry()),
            QunetTransportKind::Tcp(tcp) => Some(tcp.until_timer_expiry()),
            _ => None,
        }
    }

    #[inline]
    pub async fn handle_timer_expiry(&mut self) -> Result<(), TransportError> {
        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.handle_timer_expiry(&mut self.data).await,
            QunetTransportKind::Tcp(tcp) => tcp.handle_timer_expiry(&mut self.data),
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
        reliable: bool,
        ch: &C,
    ) -> Result<(), TransportError> {
        // Compress this message?
        if let QunetMessage::Data { .. } = &message
            && let Some(comp_type) = self.should_compress_data_message(&message)
        {
            message = self.do_compress_data_message(message, comp_type, ch).await?;
        }

        match &mut self.kind {
            QunetTransportKind::Udp(udp) => udp.send_message(&self.data, message, reliable).await,
            QunetTransportKind::Tcp(tcp) => tcp.send_message(&self.data, message).await,
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

        self.send_message(message, false, ch).await
    }

    #[inline]
    fn should_compress_data_message(&self, message: &QunetMessage) -> Option<CompressionType> {
        let data_buf = message.data_bytes().expect("Non data message passed to compression check");

        // determine if it's worth compressing

        // TODO: properly benchmark these values,
        return None;

        // we want to be careful especially with udp game packets
        if data_buf.len() < 512 {
            None
        } else if data_buf.len() < 2048 {
            Some(CompressionType::Lz4)
        } else {
            // Some(CompressionType::Zstd)
            Some(CompressionType::Zstd)
            // None
        }
    }

    async fn do_compress_data_message<C: CompressionHandler>(
        &mut self,
        message: QunetMessage,
        comp_type: CompressionType,
        ch: &C,
    ) -> Result<QunetMessage, TransportError> {
        let data_buf = message.data_bytes().expect("Non data message passed to compression check");

        assert!(!data_buf.is_empty(), "Data buffer must not be empty");

        let compression_header = CompressionHeader {
            compression_type: comp_type,
            uncompressed_size: NonZeroU32::new(data_buf.len() as u32).unwrap(),
        };

        let compressed_buf = match comp_type {
            CompressionType::Lz4 => ch.compress_lz4(data_buf).await?,
            CompressionType::Zstd => ch.compress_zstd(data_buf).await?,
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
            CompressionType::Zstd => ch.decompress_zstd(data, unc_size).await?,
            CompressionType::Lz4 => ch.decompress_lz4(data, unc_size).await?,
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
