use std::sync::Arc;

use thiserror::Error;

use crate::{
    buffers::{byte_reader::ByteReaderError, byte_writer::ByteWriterError},
    server::{AcceptError, ServerHandle},
};

#[derive(Debug, Error)]
pub enum ListenerError {
    #[error("{0}")]
    IoError(#[from] std::io::Error),
    #[error("Subtask panicked: {0}")]
    Panic(#[from] tokio::task::JoinError),
    #[error("Failed to decode user packet: {0}")]
    DecodeError(#[from] ByteReaderError),
    #[error("Failed to encode a packet: {0}")]
    EncodeError(#[from] ByteWriterError),
    #[error("Failed to accept a connection: {0}")]
    AcceptError(#[from] AcceptError),
    #[error("Handshake packet is invalid")]
    MalformedHandshake,
    #[error("QUIC connection error: {0}")]
    QuicConnectionError(#[from] s2n_quic::connection::Error),
    #[error("Connection closed by peer")]
    ConnectionClosed,
}

pub(crate) trait ServerListener {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), ListenerError>;
    fn identifier(&self) -> String;
    fn port(&self) -> u16;
}

#[derive(Debug, Error)]
pub enum BindError {
    #[error("{0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to start QUIC server: {0}")]
    QuicStartError(#[from] s2n_quic::provider::StartError),
    #[error("Failed to load TLS certificate: {0}")]
    TlsError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[inline(always)]
pub fn uninit_bytes<const N: usize>() -> [u8; N] {
    // // safety: eh
    // unsafe { std::mem::MaybeUninit::<[u8; N]>::uninit().assume_init() }
    [0; N]
}
