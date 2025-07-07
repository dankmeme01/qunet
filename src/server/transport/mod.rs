use std::{net::SocketAddr, num::NonZeroU32, ops::DerefMut as _, sync::Arc};

use thiserror::Error;

use crate::{
    buffers::buffer_pool::BufferPool,
    server::{
        Server,
        app_handler::AppHandler,
        client::ClientNotification,
        message::{
            BufferKind, CompressionHeader, CompressionType, DataMessageKind, QunetMessage,
            QunetMessageDecodeError, channel,
        },
        transport::{
            lowlevel::{SocketAddrCRepr, socket_addr_to_c},
            quic::ClientQuicTransport,
            tcp::ClientTcpTransport,
            udp::ClientUdpTransport,
        },
    },
};

pub mod lowlevel;
pub mod quic;
mod stream;
pub mod tcp;
pub mod udp;

pub(crate) enum ClientTransportKind<H: AppHandler> {
    Udp(ClientUdpTransport<H>),
    Tcp(ClientTcpTransport),
    Quic(ClientQuicTransport),
}

pub(crate) struct ClientTransportData {
    pub connection_id: u64,
    pub closed: bool,
    pub address: SocketAddr,
    pub qunet_major_version: u16,
    pub initial_qdb_hash: [u8; 16],
    pub message_size_limit: usize,
    pub buffer_pool: Arc<BufferPool>,
    pub large_buffer_pool: Arc<BufferPool>,

    c_sockaddr_data: SocketAddrCRepr,
    c_sockaddr_len: libc::socklen_t,
}

pub(crate) struct ClientTransport<H: AppHandler> {
    pub(crate) kind: ClientTransportKind<H>,
    pub data: ClientTransportData,
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
    #[error("Client sent an invalid zero-length message")]
    ZeroLengthMessage,
    #[error("Client sent a message that exceeds the size limit")]
    MessageTooLong,
    #[error("Failed to decode message: {0}")]
    DecodeError(#[from] QunetMessageDecodeError),
    #[error("Message channel was closed")]
    MessageChannelClosed,
    #[error("Failed to compress data with lz4: {0}")]
    CompressionError(#[from] lz4_flex::block::CompressError),
}

impl ClientTransportData {
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
        server: &Server<H>,
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
                buffer_pool: server.buffer_pool.clone(),
                large_buffer_pool: server.large_buffer_pool.clone(),
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
    pub async fn run_setup(&mut self, server: &Server<H>) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.run_setup(&self.data, server).await,
            ClientTransportKind::Tcp(tcp) => tcp.run_setup().await,
            ClientTransportKind::Quic(quic) => quic.run_setup().await,
        }
    }

    #[inline]
    pub async fn run_cleanup(&mut self, server: &Server<H>) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.run_cleanup(&self.data, server).await,
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

    #[inline]
    fn should_compress_data_message(&self, message: &QunetMessage) -> Option<CompressionType> {
        let data_buf = message
            .data_bytes()
            .expect("Non data message passed to compression check");

        // determine if it's worth compressing

        // TODO: properly benchmark these values,
        // we want to be careful especially with udp game packets
        if data_buf.len() < 512 {
            None
        } else if data_buf.len() < 2048 {
            Some(CompressionType::Lz4)
        } else {
            // Some(CompressionType::Zstd)
            Some(CompressionType::Lz4)
            // None
        }
    }

    #[inline]
    async fn get_new_buffer(&self, size: usize) -> BufferKind {
        if size < self.data.buffer_pool.buf_size() {
            BufferKind::Pooled {
                buf: self.data.buffer_pool.get().await,
                pos: 0,
                size: 0,
            }
        } else if size < self.data.large_buffer_pool.buf_size() {
            BufferKind::Pooled {
                buf: self.data.large_buffer_pool.get().await,
                pos: 0,
                size: 0,
            }
        } else {
            // fallback for very large needs
            BufferKind::Heap(Vec::with_capacity(size))
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
            CompressionType::Lz4 => self.do_compress_lz4(data_buf).await?,
            CompressionType::Zstd => Self::do_compress_zstd(data_buf).await?,
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

    async fn do_compress_lz4(&self, data: &[u8]) -> Result<BufferKind, TransportError> {
        let needed_len = lz4_flex::block::get_maximum_output_size(data.len());

        let mut buf = self.get_new_buffer(needed_len).await;

        let output = match &mut buf {
            // safety: the buffer is only used for writing
            BufferKind::Heap(vec) => unsafe {
                std::slice::from_raw_parts_mut(vec.as_mut_ptr(), vec.capacity())
            },

            BufferKind::Pooled { buf, .. } => {
                let cap = buf.len();
                &mut buf.deref_mut()[..cap]
            }

            BufferKind::Small { buf, size } => &mut buf[..*size],

            BufferKind::Reference(_) => unreachable!(),
        };

        debug_assert!(
            output.len() >= needed_len,
            "Output buffer is too small ({} < {})",
            output.len(),
            needed_len
        );

        let written = lz4_flex::compress_into(data, output)?;

        match &mut buf {
            // safety: exactly `written` bytes are guaranteed to be initialized
            BufferKind::Heap(vec) => unsafe {
                vec.set_len(written);
            },

            BufferKind::Pooled { size, .. } => {
                *size = written;
            }

            BufferKind::Small { size, .. } => {
                *size = written;
            }

            BufferKind::Reference(_) => unreachable!(),
        }

        Ok(buf)
    }

    async fn do_compress_zstd(data: &[u8]) -> Result<BufferKind, TransportError> {
        // TODO: actually use the zstd dictionary stuff
        todo!()
    }
}

impl<H: AppHandler> ClientTransportKind<H> {}
