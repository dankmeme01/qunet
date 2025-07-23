use std::io::Cursor;

use thiserror::Error;

use crate::{
    message::QunetMessageDecodeError,
    transport::compression::{CompressError, DecompressError},
};

#[derive(Debug, Error)]
pub enum QuicError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to start client: {0}")]
    ClientStartError(#[from] s2n_quic::provider::StartError),
    #[error("Connection error: {0}")]
    ConnectionError(#[from] s2n_quic::connection::Error),
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
    #[error("Failed to compress data: {0}")]
    CompressionError(#[from] CompressError),
    #[error("Failed to decompress data: {0}")]
    DecompressionError(#[from] DecompressError),
    #[error("Remote is too unreliable, way too many lost messages")]
    TooUnreliable,
    #[error("Too many pending fragmented messages")]
    TooManyPendingFragments,
    #[error("Error defragmenting message")]
    DefragmentationError,
    #[error("QUIC error: {0}")]
    QuicError(#[from] QuicError),
    #[error("Not implemented: {0}")]
    NotImplemented(&'static str),
    #[error("Client requested the connection to be suspended")]
    SuspendRequested,
    #[error("Connection timed out, no data received for a long time")]
    IdleTimeout,
    #[error("{0}")]
    Other(String),
}

pub enum TransportErrorOutcome {
    /// A critical error that requires the connection to be terminated immediately.
    Terminate,
    /// Graceful closure of the connection, requires the connection to be closed immediately.
    GracefulClosure,
    /// A non-critical error, but one that may indicate a bug in the server, and should be logged.
    Log,
    /// A non-critical error that can be ignored, the connection can continue to operate.
    Ignore,
}

impl TransportError {
    pub fn analyze(&self) -> TransportErrorOutcome {
        match self {
            TransportError::ConnectionClosed => TransportErrorOutcome::GracefulClosure,
            TransportError::IoError(e) => {
                use std::io::Write;
                // unfortunately we have to write the error to a buffer, i tried `downcast` but it just refused to work with s2n_quic errors

                let mut buf = [0u8; 256];
                let mut cursor = Cursor::new(&mut buf[..]);

                let write_res = write!(cursor, "{e}");
                let cpos = cursor.position();

                if write_res.is_ok() {
                    // check for the error type
                    if let Ok(str) = std::str::from_utf8(&buf[..cpos as usize])
                        && (str.contains("closed on the application level")
                            || str.contains("connection was closed without an error"))
                    {
                        // s2n-quic no-error message, treat it the same as ConnectionClosed
                        return TransportErrorOutcome::GracefulClosure;
                    }
                }

                // TODO: handle other IO errors

                TransportErrorOutcome::Terminate
            }

            // Critical errors
            TransportError::MessageChannelClosed
            | TransportError::ZeroLengthMessage
            | TransportError::MessageTooLong
            | TransportError::Timeout
            | TransportError::TooUnreliable
            | TransportError::TooManyPendingFragments
            | TransportError::QuicError(_)
            | TransportError::SuspendRequested
            | TransportError::IdleTimeout => TransportErrorOutcome::Terminate,

            // Errors that indicate a bug in qunet
            TransportError::CompressionError(_)
            | TransportError::NotImplemented(_)
            | TransportError::Other(_) => TransportErrorOutcome::Log,

            // Non-critical errors, just log and continue
            _ => TransportErrorOutcome::Ignore,
        }
    }
}
