use std::{net::SocketAddr, num::NonZeroU32, time::Duration};

use thiserror::Error;

use crate::server::{
    ServerHandle,
    app_handler::AppHandler,
    client::ClientNotification,
    message::{
        CompressionHeader, CompressionType, DataMessageKind, QunetMessage, QunetMessageDecodeError,
        channel,
    },
    protocol::QunetHandshakeError,
    transport::{
        lowlevel::{SocketAddrCRepr, socket_addr_to_c},
        quic::ClientQuicTransport,
        tcp::ClientTcpTransport,
        udp::ClientUdpTransport,
    },
};

pub mod lowlevel;
pub mod quic;
mod stream;
pub mod tcp;
pub mod udp;
pub mod udp_misc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    Udp,
    Tcp,
    Quic,
}

pub(crate) enum ClientTransportKind<H: AppHandler> {
    Udp(ClientUdpTransport<H>),
    Tcp(ClientTcpTransport<H>),
    Quic(ClientQuicTransport<H>),
}

pub(crate) struct ClientTransportData<H: AppHandler> {
    pub connection_id: u64,
    pub closed: bool,
    pub address: SocketAddr,
    pub qunet_major_version: u16,
    pub initial_qdb_hash: [u8; 16],
    pub message_size_limit: usize,
    pub server: ServerHandle<H>,

    c_sockaddr_data: SocketAddrCRepr,
    c_sockaddr_len: libc::socklen_t,
}

pub(crate) struct ClientTransport<H: AppHandler> {
    pub(crate) kind: ClientTransportKind<H>,
    pub data: ClientTransportData<H>,
    pub notif_chan: (
        channel::Sender<ClientNotification>,
        channel::Receiver<ClientNotification>,
    ),
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Connection closed by peer")]
    ConnectionClosed,
    #[error("Operation timed out")]
    Timeout,
    #[error("Client sent an invalid zero-length message")]
    ZeroLengthMessage,
    #[error("Client sent a message that exceeds the size limit")]
    MessageTooLong,
    #[error("Failed to decode message: {0}")]
    DecodeError(#[from] QunetMessageDecodeError),
    #[error("Message channel was closed")]
    MessageChannelClosed,
    #[error("Failed to compress data with lz4: {0}")]
    CompressionLz4Error(#[from] lz4_flex::block::CompressError),
    #[error("Failed to compress data with zstd: {0}")]
    CompressionZstdError(&'static str),
    #[error("Failed to decompress data")]
    DecompressionError,
    #[error("Remote is too unreliable, way too many lost messages")]
    TooUnreliable,
    #[error("Too many pending fragmented messages")]
    TooManyPendingFragments,
    #[error("Error defragmenting message")]
    DefragmentationError,
}

impl<H: AppHandler> ClientTransportData<H> {
    pub fn c_sockaddr(&self) -> (&SocketAddrCRepr, libc::socklen_t) {
        (&self.c_sockaddr_data, self.c_sockaddr_len)
    }
}

impl<H: AppHandler> ClientTransport<H> {
    pub fn new(
        kind: ClientTransportKind<H>,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        server: ServerHandle<H>,
    ) -> Self {
        let (c_sockaddr_data, c_sockaddr_len) = socket_addr_to_c(&address);

        Self {
            kind,
            data: ClientTransportData {
                address,
                connection_id: 0,
                closed: false,
                qunet_major_version,
                initial_qdb_hash,
                message_size_limit: server.message_size_limit(),
                server,
                c_sockaddr_data,
                c_sockaddr_len,
            },
            notif_chan: channel::new_channel(),
        }
    }

    #[inline]
    pub fn kind_str(&self) -> &'static str {
        match &self.kind {
            ClientTransportKind::Udp(_) => "UDP",
            ClientTransportKind::Tcp(_) => "TCP",
            ClientTransportKind::Quic(_) => "QUIC",
        }
    }

    #[inline]
    pub fn transport_type(&self) -> TransportType {
        match &self.kind {
            ClientTransportKind::Udp(_) => TransportType::Udp,
            ClientTransportKind::Tcp(_) => TransportType::Tcp,
            ClientTransportKind::Quic(_) => TransportType::Quic,
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
            ClientTransportKind::Udp(udp) => {
                udp.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size)
                    .await
            }

            ClientTransportKind::Tcp(tcp) => {
                tcp.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size)
                    .await
            }

            ClientTransportKind::Quic(quic) => {
                quic.send_handshake_response(&self.data, qdb_data, qdb_uncompressed_size)
                    .await
            }
        }
    }

    #[inline]
    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.run_setup(&self.data, &self.data.server).await,
            ClientTransportKind::Tcp(tcp) => tcp.run_setup().await,
            ClientTransportKind::Quic(quic) => quic.run_setup().await,
        }
    }

    #[inline]
    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.run_cleanup(&self.data, &self.data.server).await,
            ClientTransportKind::Tcp(tcp) => tcp.run_cleanup().await,
            ClientTransportKind::Quic(quic) => quic.run_cleanup().await,
        }
    }

    #[inline]
    pub async fn receive_message(&mut self) -> Result<QunetMessage, TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.receive_message(&self.data).await,
            ClientTransportKind::Tcp(tcp) => tcp.receive_message(&self.data).await,
            ClientTransportKind::Quic(quic) => quic.receive_message(&self.data).await,
        }
    }

    #[inline]
    pub fn until_timer_expiry(&self) -> Option<Duration> {
        match &self.kind {
            ClientTransportKind::Udp(udp) => Some(udp.until_timer_expiry()),
            _ => None,
        }
    }

    #[inline]
    pub async fn handle_timer_expiry(&mut self) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.handle_timer_expiry(&self.data).await,
            _ => Ok(()),
        }
    }

    pub async fn decompress_message(
        &mut self,
        msg: QunetMessage,
    ) -> Result<QunetMessage, TransportError> {
        self.do_decompress_data_message(msg).await
    }

    #[inline]
    pub async fn send_message(
        &mut self,
        mut message: QunetMessage,
        reliable: bool,
    ) -> Result<(), TransportError> {
        // Compress this message?
        if let QunetMessage::Data { .. } = &message
            && let Some(comp_type) = self.should_compress_data_message(&message)
        {
            message = self.do_compress_data_message(message, comp_type).await?;
        }

        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.send_message(&self.data, message, reliable).await,
            ClientTransportKind::Tcp(tcp) => tcp.send_message(&self.data, message).await,
            ClientTransportKind::Quic(quic) => quic.send_message(&self.data, message).await,
        }
    }

    /// Shorthand for sending a handshake error message.
    #[inline]
    pub async fn send_handshake_error(
        &mut self,
        error_code: QunetHandshakeError,
        reason: Option<String>,
    ) -> Result<(), TransportError> {
        let message = QunetMessage::HandshakeFailure { error_code, reason };

        self.send_message(message, false).await
    }

    #[inline]
    fn should_compress_data_message(&self, message: &QunetMessage) -> Option<CompressionType> {
        let data_buf = message
            .data_bytes()
            .expect("Non data message passed to compression check");

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

    async fn do_compress_data_message(
        &mut self,
        message: QunetMessage,
        comp_type: CompressionType,
    ) -> Result<QunetMessage, TransportError> {
        let data_buf = message
            .data_bytes()
            .expect("Non data message passed to compression check");

        assert!(!data_buf.is_empty(), "Data buffer must not be empty");

        let compression_header = CompressionHeader {
            compression_type: comp_type,
            uncompressed_size: NonZeroU32::new(data_buf.len() as u32).unwrap(),
        };

        let compressed_buf = match comp_type {
            CompressionType::Lz4 => self.data.server.compress_lz4_data(data_buf).await?,
            CompressionType::Zstd => self.data.server.compress_zstd_data(data_buf).await?,
        };

        let reliability = match message {
            QunetMessage::Data { reliability, .. } => reliability,
            _ => unreachable!(),
        };

        Ok(QunetMessage::Data {
            kind: DataMessageKind::Regular {
                data: compressed_buf,
            },
            reliability,
            compression: Some(compression_header),
        })
    }

    async fn do_decompress_data_message(
        &mut self,
        message: QunetMessage,
    ) -> Result<QunetMessage, TransportError> {
        let compression_header = match &message {
            QunetMessage::Data { compression, .. } => compression
                .as_ref()
                .expect("Data message without compression header"),
            _ => unreachable!("Non data message passed to decompression check"),
        };

        let data = message.data_bytes().unwrap();

        let buf = match compression_header.compression_type {
            CompressionType::Zstd => {
                self.data
                    .server
                    .decompress_zstd_data(data, compression_header.uncompressed_size.get() as usize)
                    .await?
            }

            _ => todo!("lz4"),
        };

        Ok(QunetMessage::Data {
            kind: DataMessageKind::Regular { data: buf },
            reliability: None,
            compression: None,
        })
    }
}

impl<H: AppHandler> ClientTransportKind<H> {}

// just a helper function

#[inline]
pub fn exponential_moving_average<T: Into<f64>>(current: T, previous: T, alpha: f64) -> f64 {
    let current = current.into();
    let previous = previous.into();

    alpha * current + (1.0 - alpha) * previous
}
