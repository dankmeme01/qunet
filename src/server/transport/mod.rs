use std::{net::SocketAddr, sync::Arc};

use thiserror::Error;

use crate::{
    buffers::buffer_pool::BufferPool,
    server::{
        Server,
        message::{QunetMessage, QunetMessageDecodeError},
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

pub(crate) enum ClientTransportKind {
    Udp(ClientUdpTransport),
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

    c_sockaddr_data: SocketAddrCRepr,
    c_sockaddr_len: libc::socklen_t,
}

pub(crate) struct ClientTransport {
    kind: ClientTransportKind,
    pub data: ClientTransportData,
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
}

impl ClientTransportData {
    pub fn c_sockaddr(&self) -> (&SocketAddrCRepr, libc::socklen_t) {
        (&self.c_sockaddr_data, self.c_sockaddr_len)
    }
}

impl ClientTransport {
    pub fn new(
        kind: ClientTransportKind,
        address: SocketAddr,
        qunet_major_version: u16,
        initial_qdb_hash: [u8; 16],
        server: &Server,
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
                c_sockaddr_data,
                c_sockaddr_len,
            },
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
    pub async fn run_setup(&mut self, server: &Server) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.run_setup(&self.data, server).await,
            ClientTransportKind::Tcp(tcp) => tcp.run_setup().await,
            ClientTransportKind::Quic(quic) => quic.run_setup().await,
        }
    }

    #[inline]
    pub async fn run_cleanup(&mut self, server: &Server) -> Result<(), TransportError> {
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
    pub async fn send_message(&mut self, message: &QunetMessage) -> Result<(), TransportError> {
        match &mut self.kind {
            ClientTransportKind::Udp(udp) => udp.send_message(&self.data, message).await,
            ClientTransportKind::Tcp(tcp) => tcp.send_message(&self.data, message).await,
            ClientTransportKind::Quic(quic) => quic.send_message(&self.data, message).await,
        }
    }
}
