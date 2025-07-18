use std::sync::Arc;

use thiserror::Error;

use crate::{
    buffers::{ByteReaderError, ByteWriterError},
    server::{AcceptError, ServerHandle, app_handler::AppHandler},
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

pub(crate) trait ServerListener<H: AppHandler> {
    async fn run(self: Arc<Self>, server: ServerHandle<H>) -> Result<(), ListenerError>;
    fn identifier(&self) -> String;
    fn port(&self) -> u16;
}

#[derive(Debug, Error)]
pub enum BindError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to start QUIC server: {0}")]
    QuicStart(#[from] s2n_quic::provider::StartError),
    #[error("Failed to load TLS certificate: {0}")]
    Tls(Box<dyn std::error::Error + Send + Sync + 'static>),
}
