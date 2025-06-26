use std::sync::Arc;

use thiserror::Error;
use tokio::task::JoinHandle;

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
}

pub type ListenerHandle = JoinHandle<Result<(), ListenerError>>;

pub trait ServerListener {
    fn run(self: Arc<Self>, server: ServerHandle) -> ListenerHandle;
    fn identifier(&self) -> String;
    fn port(&self) -> u16;
    fn shutdown(&self);
}

#[derive(Debug, Error)]
pub enum BindError {
    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

#[inline(always)]
pub fn uninit_bytes<const N: usize>() -> [u8; N] {
    // // safety: eh
    // unsafe { std::mem::MaybeUninit::<[u8; N]>::uninit().assume_init() }
    [0; N]
}
