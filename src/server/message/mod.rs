pub mod channel;
mod meta;
mod raw;

pub use meta::QunetMessageMeta;
use num_traits::FromPrimitive;
pub use raw::{QUNET_SMALL_MESSAGE_SIZE, QunetRawMessage};

use std::{ops::Deref, sync::Arc};

use thiserror::Error;

use crate::{
    buffers::{
        bits::Bits,
        buffer_pool::{BorrowedMutBuffer, BufferPool},
        byte_reader::{ByteReader, ByteReaderError},
    },
    server::{
        message::meta::{
            CompressionHeader, FragmentationHeader, QunetMessageBareMeta, ReliabilityHeader,
        },
        protocol::*,
    },
};

#[derive(Debug, Error)]
pub enum QunetMessageDecodeError {
    #[error("Invalid message header: {0}")]
    InvalidStructure(#[from] ByteReaderError),
    #[error("Invalid compression type in message header")]
    InvalidCompressionType,
    #[error("Invalid header, a value was zero when it is not allowed to be")]
    UnexpectedZero,
    #[error("Missing connection ID in message header")]
    MissingConnectionId,
    #[error("Invalid message type in message header")]
    InvalidMessageType,
    #[error("Message had additional data in it that is too long ({0} bytes)")]
    AdditionalDataTooLong(usize),
    #[error("Message header is malformed")]
    InvalidHeader,
    #[error("Invalid error code in an error message: {0}")]
    InvalidErrorCode(u32),
}

pub enum BufferKind {
    Heap(Vec<u8>),
    Pooled {
        buf: BorrowedMutBuffer,
        pos: usize,
        size: usize,
    },

    Small {
        buf: [u8; QUNET_SMALL_MESSAGE_SIZE],
        size: usize,
    },
}

impl Deref for BufferKind {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            BufferKind::Heap(buf) => buf,
            BufferKind::Pooled { buf, pos, size } => &buf[*pos..*pos + *size],
            BufferKind::Small { buf, size } => &buf[..*size],
        }
    }
}

pub enum DataMessageKind {
    Fragment {
        header: FragmentationHeader,
        data: BufferKind,
    },

    Regular {
        data: BufferKind,
    },
}

pub enum QunetMessage {
    Keepalive {
        timestamp: u64,
    },

    KeepaliveResponse {
        timestamp: u64,
        data: Option<BorrowedMutBuffer>,
    },

    HandshakeStart {
        qunet_major: u16,
        frag_limit: u16,
        qdb_hash: [u8; 16],
    },

    HandshakeFailure {
        error_code: QunetHandshakeError,
        reason: Option<String>,
    },

    ClientClose {
        dont_terminate: bool,
    },

    ServerClose {
        error_code: QunetConnectionError,
        error_message: Option<String>,
    },

    ClientReconnect {
        connection_id: u64,
    },

    ConnectionError {
        error_code: QunetConnectionError,
    },

    QdbChunkRequest {
        offset: u32,
        size: u32,
    },

    // Invariant: when this messgae is instantiated, `offset` and `size` must be valid.
    QdbChunkResponse {
        offset: u32,
        size: u32,
        qdb_data: Arc<[u8]>,
    },

    Data {
        kind: DataMessageKind,
        reliability: Option<ReliabilityHeader>,
        compression: Option<CompressionHeader>,
    },
}

enum RawOrSlice<'a> {
    Raw(QunetRawMessage),
    Slice(&'a [u8]),
}

impl QunetMessage {
    /// Parses the header of a Qunet message into a `QunetMessageMeta` structure,
    /// which cen then be used to fully decode the message.
    #[inline]
    pub fn parse_header<'a>(
        data: &'a [u8],
        udp: bool,
    ) -> Result<QunetMessageMeta<'a>, QunetMessageDecodeError> {
        QunetMessageMeta::parse(data, udp)
    }

    /// Decodes a Qunet message from a `QunetMessageMeta` structure earlier obtained from `parse_header`.
    pub async fn decode(
        meta: QunetMessageMeta<'_>,
        buffer_pool: &BufferPool,
    ) -> Result<QunetMessage, QunetMessageDecodeError> {
        let mut reader = ByteReader::new(meta.data);
        if !meta.bare.is_data {
            match meta.bare.header_byte {
                MSG_KEEPALIVE => {
                    let timestamp = reader.read_u64()?;
                    let _flags = reader.read_bits::<u8>()?;

                    Ok(QunetMessage::Keepalive { timestamp })
                }

                MSG_KEEPALIVE_RESPONSE => {
                    let timestamp = reader.read_u64()?;
                    let data_len = reader.read_u16()? as usize;
                    if data_len > buffer_pool.buf_size() {
                        return Err(QunetMessageDecodeError::AdditionalDataTooLong(data_len));
                    }

                    let buf = if data_len > 0 {
                        let mut buf = buffer_pool.get().await;
                        let rem = reader.remaining_bytes();
                        if rem.len() != data_len {
                            return Err(QunetMessageDecodeError::InvalidHeader);
                        }

                        buf[..data_len].copy_from_slice(rem);
                        Some(buf)
                    } else {
                        None
                    };

                    Ok(QunetMessage::KeepaliveResponse {
                        timestamp,
                        data: buf,
                    })
                }

                MSG_HANDSHAKE_START => {
                    let qunet_major = reader.read_u16()?;
                    let frag_limit = reader.read_u16()?;
                    let mut qdb_hash = [0u8; 16];
                    reader.read_bytes(&mut qdb_hash)?;

                    Ok(QunetMessage::HandshakeStart {
                        qunet_major,
                        frag_limit,
                        qdb_hash,
                    })
                }

                MSG_CLIENT_CLOSE => {
                    let flags = reader.read_bits::<u8>()?;
                    let dont_terminate = flags.get_bit(0);

                    Ok(QunetMessage::ClientClose { dont_terminate })
                }

                MSG_CLIENT_RECONNECT => {
                    let connection_id = reader.read_u64()?;

                    Ok(QunetMessage::ClientReconnect { connection_id })
                }

                MSG_CONNECTION_ERROR => {
                    let code = reader.read_u32()?;

                    Ok(QunetMessage::ConnectionError {
                        error_code: QunetConnectionError::from_u32(code)
                            .ok_or(QunetMessageDecodeError::InvalidErrorCode(code))?,
                    })
                }

                MSG_QDB_CHUNK_REQUEST => {
                    let offset = reader.read_u32()?;
                    let size = reader.read_u32()?;

                    Ok(QunetMessage::QdbChunkRequest { offset, size })
                }

                _ => Err(QunetMessageDecodeError::InvalidMessageType),
            }
        } else {
            // data message
            Self::decode_data_message(meta.bare, buffer_pool, RawOrSlice::Slice(meta.data)).await
        }
    }

    async fn decode_data_message(
        meta: QunetMessageBareMeta,
        buffer_pool: &BufferPool,
        raw_msg: RawOrSlice<'_>,
    ) -> Result<QunetMessage, QunetMessageDecodeError> {
        // we want to try and not copy data/request buffers if possible

        let data_len = match raw_msg {
            RawOrSlice::Raw(ref raw) => raw.len() - meta.data_offset,
            RawOrSlice::Slice(data) => data.len(), // already offset
        };

        // try to pick the most efficient buffer kind

        let buf_kind = if let RawOrSlice::Raw(QunetRawMessage::Large { buffer, .. }) = raw_msg {
            BufferKind::Pooled {
                buf: buffer,
                pos: meta.data_offset,
                size: data_len,
            }
        } else {
            match raw_msg {
                RawOrSlice::Raw(QunetRawMessage::Small { data, len }) => {
                    debug_assert!(len <= QUNET_SMALL_MESSAGE_SIZE);
                    let mut small_buf = [0u8; QUNET_SMALL_MESSAGE_SIZE];
                    small_buf[..data_len]
                        .copy_from_slice(&data[meta.data_offset..meta.data_offset + data_len]);

                    BufferKind::Small {
                        buf: small_buf,
                        size: data_len,
                    }
                }

                RawOrSlice::Slice(data) => {
                    if data_len <= QUNET_SMALL_MESSAGE_SIZE {
                        // note that when a Slice is passed, it is already offset
                        let mut small_buf = [0u8; QUNET_SMALL_MESSAGE_SIZE];
                        small_buf[..data_len].copy_from_slice(data);

                        BufferKind::Small {
                            buf: small_buf,
                            size: data_len,
                        }
                    } else if data_len <= buffer_pool.buf_size() {
                        // request a buffer from the pool
                        let mut buf = buffer_pool.get().await;
                        buf[..data_len].copy_from_slice(data);

                        BufferKind::Pooled {
                            buf,
                            pos: 0,
                            size: data_len,
                        }
                    } else {
                        // heap allocate
                        let mut heap_buf = Vec::with_capacity(data_len);
                        heap_buf.extend_from_slice(data);

                        BufferKind::Heap(heap_buf)
                    }
                }

                RawOrSlice::Raw(_) => unreachable!(),
            }
        };

        let data_kind = if let Some(header) = meta.fragmentation_header {
            DataMessageKind::Fragment {
                header,
                data: buf_kind,
            }
        } else {
            DataMessageKind::Regular { data: buf_kind }
        };

        Ok(QunetMessage::Data {
            kind: data_kind,
            reliability: meta.reliability_header,
            compression: meta.compression_header,
        })
    }

    /// Decodes a `QunetRawMessage` into a `QunetMessage`. UDP is assumed.
    #[inline]
    pub async fn from_raw_udp_message(
        raw_msg: QunetRawMessage,
        buffer_pool: &BufferPool,
    ) -> Result<QunetMessage, QunetMessageDecodeError> {
        let meta = QunetMessageMeta::parse(&raw_msg, true)?;

        if !meta.bare.is_data {
            // control message, no need to read the data
            return Self::decode(meta, buffer_pool).await;
        }

        Self::decode_data_message(meta.bare, buffer_pool, RawOrSlice::Raw(raw_msg)).await
    }

    /// This function must only be used for UDP messages.
    /// Extracts the connection ID from the message header. Returns None if the header is invalid.
    #[inline]
    pub fn connection_id_from_header(data: &[u8]) -> Option<u64> {
        assert!(!data.is_empty(), "data must not be empty");

        if data[0] & MSG_DATA_MASK != 0 {
            // data message, take care of the optional compression header
            let bits = Bits::new(data[0]);

            // TODO: benchmark if bits is fast enough, maybe just use manual bit manip
            let compression_num =
                bits.get_multiple_bits(MSG_DATA_BIT_COMPRESSION_1, MSG_DATA_BIT_COMPRESSION_2);

            if compression_num == 0 {
                // no compression, connection ID is right after the header byte
                ByteReader::new(&data[1..]).read_u64().ok()
            } else {
                // skip compression header (4 bytes)
                if data.len() < 5 {
                    return None; // invalid header
                }

                ByteReader::new(&data[5..]).read_u64().ok()
            }
        } else {
            // control message, connection ID is right after the header byte
            ByteReader::new(&data[1..]).read_u64().ok()
        }
    }

    #[inline]
    pub fn type_str(&self) -> &'static str {
        match self {
            QunetMessage::Keepalive { .. } => "Keepalive",
            QunetMessage::KeepaliveResponse { .. } => "KeepaliveResponse",
            QunetMessage::HandshakeStart { .. } => "HandshakeStart",
            QunetMessage::HandshakeFailure { .. } => "HandshakeFailure",
            QunetMessage::ClientClose { .. } => "ClientClose",
            QunetMessage::ServerClose { .. } => "ServerClose",
            QunetMessage::ClientReconnect { .. } => "ClientReconnect",
            QunetMessage::ConnectionError { .. } => "ConnectionError",
            QunetMessage::QdbChunkRequest { .. } => "QdbChunkRequest",
            QunetMessage::QdbChunkResponse { .. } => "QdbChunkResponse",
            QunetMessage::Data { .. } => "Data",
        }
    }
}
