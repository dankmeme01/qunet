mod buffer_kind;
pub mod channel;
mod meta;
mod msg_data;
mod raw;

pub use buffer_kind::BufferKind;
pub(crate) use meta::{
    CompressionHeader, CompressionType, FragmentationHeader, QunetMessageBareMeta,
    QunetMessageMeta, ReliabilityHeader,
};
pub use msg_data::MsgData;
use num_derive::{FromPrimitive, ToPrimitive};
pub use raw::{QUNET_SMALL_MESSAGE_SIZE, QunetRawMessage};

use num_traits::FromPrimitive;
use std::{borrow::Cow, ops::Deref, sync::Arc};
use thiserror::Error;

use crate::{
    buffers::{
        BinaryWriter, Bits, BufPool, ByteReader, ByteReaderError, ByteWriter, ByteWriterError,
    },
    protocol::*,
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
    #[error("Message had invalid additional data")]
    InvalidAdditionalData,
    #[error("Message header is malformed")]
    InvalidHeader,
    #[error("Too many protocols in a Pong message")]
    TooManyProtocols,
    #[error("Invalid protocol in a Pong message")]
    InvalidProtocol,
    #[error("Invalid error code in an error message: {0}")]
    InvalidErrorCode(u32),
    #[error("QDB chunk is zero bytes")]
    QdbChunkZeroBytes,
}

pub(crate) enum DataMessageKind {
    Fragment {
        header: FragmentationHeader,
        data: BufferKind,
    },

    Regular {
        data: BufferKind,
    },
}

impl DataMessageKind {
    #[inline]
    pub fn is_fragment(&self) -> bool {
        matches!(self, DataMessageKind::Fragment { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive, ToPrimitive)]
#[repr(u8)]
pub(crate) enum Protocol {
    Tcp = 1,
    Udp = 2,
    Quic = 3,
}

pub(crate) struct PongProtocol {
    pub protocol: Protocol,
    pub port: u16,
}

impl PongProtocol {
    pub fn as_tcp(&self) -> Option<u16> {
        if self.protocol == Protocol::Tcp { Some(self.port) } else { None }
    }

    pub fn as_udp(&self) -> Option<u16> {
        if self.protocol == Protocol::Udp { Some(self.port) } else { None }
    }

    pub fn as_quic(&self) -> Option<u16> {
        if self.protocol == Protocol::Quic { Some(self.port) } else { None }
    }
}

#[allow(unused)]
pub(crate) struct HandshakeQdbData {
    pub uncompressed_size: u32,
    pub full_size: u32,
    pub chunk_offset: u32,
    pub chunk_size: u32,
    pub data: BufferKind,
}

pub(crate) enum QunetMessage {
    Ping {
        ping_id: u32,
        omit_protocols: bool,
    },

    Pong {
        ping_id: u32,
        protocols: heapless::Vec<PongProtocol, 4>,
        #[allow(unused)]
        data: Option<BufferKind>,
    },

    Keepalive {
        timestamp: u64,
    },

    KeepaliveResponse {
        timestamp: u64,
        data: Option<BufferKind>,
    },

    HandshakeStart {
        qunet_major: u16,
        frag_limit: u16,
        qdb_hash: [u8; 16],
    },

    HandshakeFinishPartial {
        connection_id: u64,
        qdb: Option<HandshakeQdbData>,
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
        error_message: Option<Cow<'static, str>>,
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

    ReconnectSuccess,
    ReconnectFailure,

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
    pub fn new_data(buf: BufferKind) -> Self {
        QunetMessage::Data {
            kind: DataMessageKind::Regular { data: buf },
            reliability: None,
            compression: None,
        }
    }

    /// Parses the header of a Qunet message into a `QunetMessageMeta` structure,
    /// which can then be used to fully decode the message.
    #[inline]
    pub fn parse_header<'a>(
        data: &'a [u8],
        udp: bool,
    ) -> Result<QunetMessageMeta<'a>, QunetMessageDecodeError> {
        QunetMessageMeta::parse(data, udp)
    }

    /// Get a buffer for additional data in a message.
    fn _fill_additional_data<P: BufPool>(
        size: usize,
        pool: &P,
        reader: &mut ByteReader<'_>,
    ) -> Result<Option<BufferKind>, QunetMessageDecodeError> {
        if size > pool.max_buf_size() {
            return Err(QunetMessageDecodeError::AdditionalDataTooLong(size));
        }

        if size > 0 {
            let rem = reader.remaining_bytes();
            if rem.len() != size {
                return Err(QunetMessageDecodeError::InvalidAdditionalData);
            }

            let mut buf = if size < QUNET_SMALL_MESSAGE_SIZE {
                BufferKind::new_small()
            } else {
                BufferKind::new_pooled(pool.get_busy_loop(size).unwrap())
            };

            buf.append_bytes(rem);
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    /// Decodes a Qunet message from a `QunetMessageMeta` structure earlier obtained from `parse_header`.
    pub fn decode<P: BufPool>(
        meta: QunetMessageMeta<'_>,
        buffer_pool: &P,
    ) -> Result<QunetMessage, QunetMessageDecodeError> {
        let mut reader = ByteReader::new(meta.data);
        if !meta.bare.is_data {
            match meta.bare.header_byte {
                MSG_PING => {
                    let ping_id = reader.read_u32()?;
                    let flags = reader.read_bits::<u8>()?;

                    let omit_protocols = flags.get_bit(0);

                    Ok(QunetMessage::Ping { ping_id, omit_protocols })
                }

                MSG_PONG => {
                    let ping_id = reader.read_u32()?;
                    let protocol_count = reader.read_u8()?;

                    let mut protocols = heapless::Vec::new();
                    for _ in 0..protocol_count {
                        let protocol_num = reader.read_u8()?;
                        let port = reader.read_u16()?;

                        let protocol = Protocol::from_u8(protocol_num)
                            .ok_or(QunetMessageDecodeError::InvalidProtocol)?;

                        protocols
                            .push(PongProtocol { protocol, port })
                            .map_err(|_| QunetMessageDecodeError::TooManyProtocols)?;
                    }

                    let data_len = reader.read_u16()? as usize;
                    let data = Self::_fill_additional_data(data_len, buffer_pool, &mut reader)?;

                    Ok(QunetMessage::Pong { ping_id, protocols, data })
                }

                MSG_KEEPALIVE => {
                    let timestamp = reader.read_u64()?;
                    let _flags = reader.read_bits::<u8>()?;

                    Ok(QunetMessage::Keepalive { timestamp })
                }

                MSG_KEEPALIVE_RESPONSE => {
                    let timestamp = reader.read_u64()?;
                    let data_len = reader.read_u16()? as usize;

                    let data = Self::_fill_additional_data(data_len, buffer_pool, &mut reader)?;

                    Ok(QunetMessage::KeepaliveResponse { timestamp, data })
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

                MSG_HANDSHAKE_FINISH => {
                    let connection_id = reader.read_u64()?;

                    let qdb = if reader.read_bool()? {
                        let uncompressed_size = reader.read_u32()?;
                        let full_size = reader.read_u32()?;
                        let chunk_offset = reader.read_u32()?;
                        let chunk_size = reader.read_u32()? as usize;
                        let data =
                            Self::_fill_additional_data(chunk_size, buffer_pool, &mut reader)?;

                        if let Some(data) = data {
                            Some(HandshakeQdbData {
                                uncompressed_size,
                                full_size,
                                chunk_offset,
                                chunk_size: chunk_size as u32,
                                data,
                            })
                        } else {
                            return Err(QunetMessageDecodeError::QdbChunkZeroBytes);
                        }
                    } else {
                        None
                    };

                    Ok(QunetMessage::HandshakeFinishPartial { connection_id, qdb })
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

                MSG_RECONNECT_SUCCESS => Ok(QunetMessage::ReconnectSuccess),

                MSG_RECONNECT_FAILURE => Ok(QunetMessage::ReconnectFailure),

                _ => Err(QunetMessageDecodeError::InvalidMessageType),
            }
        } else {
            // data message
            Self::decode_data_message(meta.bare, buffer_pool, RawOrSlice::Slice(meta.data))
        }
    }

    fn decode_data_message<P: BufPool>(
        meta: QunetMessageBareMeta,
        buffer_pool: &P,
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

                    BufferKind::Small { buf: small_buf, size: data_len }
                }

                RawOrSlice::Slice(data) => {
                    if data_len <= QUNET_SMALL_MESSAGE_SIZE {
                        // note that when a Slice is passed, it is already offset
                        let mut small_buf = [0u8; QUNET_SMALL_MESSAGE_SIZE];
                        small_buf[..data_len].copy_from_slice(data);

                        BufferKind::Small { buf: small_buf, size: data_len }
                    } else if data_len <= buffer_pool.max_buf_size() {
                        // request a buffer from the pool
                        let mut buf = buffer_pool.get_busy_loop(data_len).unwrap();
                        buf[..data_len].copy_from_slice(data);

                        BufferKind::Pooled { buf, pos: 0, size: data_len }
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
            DataMessageKind::Fragment { header, data: buf_kind }
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
    pub fn from_raw_udp_message<P: BufPool>(
        raw_msg: QunetRawMessage,
        buffer_pool: &P,
    ) -> Result<QunetMessage, QunetMessageDecodeError> {
        let meta = QunetMessageMeta::parse(&raw_msg, true)?;

        if !meta.bare.is_data {
            // control message, no need to read the data
            return Self::decode(meta, buffer_pool);
        }

        Self::decode_data_message(meta.bare, buffer_pool, RawOrSlice::Raw(raw_msg))
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
    pub fn is_data(&self) -> bool {
        matches!(self, QunetMessage::Data { .. })
    }

    /// Convenience method that returns bytes of a `Data` message. Returns None if the message is not a `Data` message.
    pub fn data_bytes(&self) -> Option<&[u8]> {
        self.data_bufkind().map(|buf| buf.deref())
    }

    /// Convenience method that returns a `BufferKind` of a `Data` message. Returns None if the message is not a `Data` message.
    pub fn data_bufkind(&self) -> Option<&BufferKind> {
        if let QunetMessage::Data { kind, .. } = self {
            match kind {
                DataMessageKind::Fragment { data, .. } => Some(data),
                DataMessageKind::Regular { data } => Some(data),
            }
        } else {
            None
        }
    }

    /// Convenience method that returns a mutable `BufferKind` of a `Data` message. Returns None if the message is not a `Data` message.
    pub fn data_bufkind_mut(&mut self) -> Option<&mut BufferKind> {
        if let QunetMessage::Data { kind, .. } = self {
            match kind {
                DataMessageKind::Fragment { data, .. } => Some(data),
                DataMessageKind::Regular { data } => Some(data),
            }
        } else {
            None
        }
    }

    /// Convenience method that returns whether a `Data` message is compressed.
    pub fn is_data_compressed(&self) -> bool {
        if let QunetMessage::Data { compression, .. } = self {
            compression.is_some()
        } else {
            false
        }
    }

    /// Calculates the header size of the message.
    pub fn calc_header_size(&self) -> usize {
        match self {
            Self::Data { kind, reliability, compression } => {
                let mut size = 1; // header byte

                if let Some(CompressionHeader { .. }) = compression {
                    size += 4; // compression header size
                }

                if let Some(ReliabilityHeader { acks, .. }) = reliability {
                    size += 4 + acks.len() * 2; // reliability header size
                }

                if kind.is_fragment() {
                    size += 4; // fragmentation header size
                }

                size
            }

            _ => 1,
        }
    }

    /// Convenience method that checks if this is a reliable message with a nonzero message ID.
    pub fn is_reliable_message(&self) -> bool {
        if let QunetMessage::Data {
            reliability: Some(reliability), ..
        } = self
        {
            reliability.message_id != 0
        } else {
            false
        }
    }

    #[inline]
    pub fn is_handshake_start(&self) -> bool {
        matches!(self, QunetMessage::HandshakeStart { .. })
    }

    // Encodes a control message into the given writers. Panics if this is a `QunetMessage::Data` message.
    // The header writer must fit at least 8 bytes, and the body writer does not have a strict size limit.
    pub fn encode_control_msg<W: std::io::Write, W2: std::io::Write>(
        &self,
        header_writer: &mut W,
        body_writer: &mut W2,
    ) -> std::io::Result<()> {
        let mut header_writer = BinaryWriter::new(header_writer);
        let mut body_writer = BinaryWriter::new(body_writer);

        match self {
            Self::Ping { ping_id, omit_protocols } => {
                header_writer.write_u8(MSG_PING)?;
                body_writer.write_u32(*ping_id)?;

                let mut flags = Bits::new(0u8);
                if *omit_protocols {
                    flags.set_bit(0);
                }

                body_writer.write_u8(flags.to_bits())?;
            }

            Self::Keepalive { timestamp } => {
                header_writer.write_u8(MSG_KEEPALIVE)?;
                body_writer.write_u64(*timestamp)?;
                body_writer.write_u8(0)?; // flags, currently unused
            }

            Self::KeepaliveResponse { timestamp, data } => {
                header_writer.write_u8(MSG_KEEPALIVE_RESPONSE)?;
                body_writer.write_u64(*timestamp)?;
                if let Some(data) = data {
                    body_writer.write_u16(data.len() as u16)?;
                    body_writer.write_bytes(data)?;
                } else {
                    body_writer.write_u16(0)?;
                }
            }

            Self::HandshakeStart {
                qunet_major,
                frag_limit,
                qdb_hash,
            } => {
                header_writer.write_u8(MSG_HANDSHAKE_START)?;
                body_writer.write_u16(*qunet_major)?;
                body_writer.write_u16(*frag_limit)?;
                body_writer.write_bytes(qdb_hash)?;
            }

            Self::HandshakeFailure { error_code, reason } => {
                header_writer.write_u8(MSG_HANDSHAKE_FAILURE)?;
                body_writer.write_u32(*error_code as u32)?;

                if *error_code == QunetHandshakeError::Custom {
                    if let Some(reason) = reason {
                        body_writer.write_string(reason)?;
                    } else {
                        panic!(
                            "QunetMessage::encode_control_msg: HandshakeFailure with 0 error code and no reason"
                        );
                    }
                }
            }

            Self::ServerClose { error_code, error_message } => {
                header_writer.write_u8(MSG_SERVER_CLOSE)?;
                body_writer.write_u32(*error_code as u32)?;

                if *error_code == QunetConnectionError::Custom {
                    if let Some(reason) = error_message {
                        body_writer.write_string(reason)?;
                    } else {
                        panic!(
                            "QunetMessage::encode_control_msg: ServerClose with 0 error code and no reason"
                        );
                    }
                }
            }

            Self::ConnectionError { error_code } => {
                header_writer.write_u8(MSG_CONNECTION_ERROR)?;
                body_writer.write_u32(*error_code as u32)?;
            }

            Self::QdbChunkResponse { offset, size, qdb_data } => {
                header_writer.write_u8(MSG_QDB_CHUNK_RESPONSE)?;
                body_writer.write_u32(*offset)?;
                body_writer.write_u32(*size)?;

                let offset = *offset as usize;
                let size = *size as usize;

                body_writer.write_bytes(&qdb_data[offset..offset + size])?;
            }

            Self::ReconnectSuccess => {
                header_writer.write_u8(MSG_RECONNECT_SUCCESS)?;
            }

            Self::ReconnectFailure => {
                header_writer.write_u8(MSG_RECONNECT_FAILURE)?;
            }

            _ => panic!(
                "QunetMessage::encode_control_msg: called with unexpected message: {}",
                self.type_str()
            ),
        };

        Ok(())
    }

    /// Writes the header of a `Data` message to the given writer.
    pub fn encode_data_header(
        &self,
        writer: &mut ByteWriter,
        omit_headers: bool,
    ) -> Result<(), ByteWriterError> {
        let Self::Data { reliability, compression, .. } = self else {
            unreachable!("encode_data_header called on non-data message: {}", self.type_str());
        };

        let mut hb = Bits::new(0u8);

        // top bit is always set for data messages
        hb.set_bit(7);

        if !omit_headers {
            // set compression bits
            hb.set_multiple_bits(
                MSG_DATA_BIT_COMPRESSION_1,
                MSG_DATA_BIT_COMPRESSION_2,
                match compression {
                    Some(CompressionHeader { compression_type, .. }) => match compression_type {
                        CompressionType::Zstd => 0b01,
                        CompressionType::Lz4 => 0b10,
                    },

                    None => 0b00,
                },
            );

            // set reliability bits
            if reliability.is_some() {
                hb.set_bit(MSG_DATA_BIT_RELIABILITY);
            }
        }

        // write header byte
        writer.write_u8(hb.to_bits());

        if !omit_headers {
            // write compression header
            if let Some(CompressionHeader { uncompressed_size, .. }) = compression {
                writer.write_u32(uncompressed_size.get());
            }

            // write reliability header
            if let Some(ReliabilityHeader { message_id, acks }) = reliability {
                writer.write_u16(*message_id);
                writer.write_u16(acks.len() as u16);
                for ack in acks {
                    writer.write_u16(*ack);
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn type_str(&self) -> &'static str {
        match self {
            QunetMessage::Ping { .. } => "Ping",
            QunetMessage::Pong { .. } => "Pong",
            QunetMessage::Keepalive { .. } => "Keepalive",
            QunetMessage::KeepaliveResponse { .. } => "KeepaliveResponse",
            QunetMessage::HandshakeStart { .. } => "HandshakeStart",
            QunetMessage::HandshakeFinishPartial { .. } => "HandshakeFinishPartial",
            QunetMessage::HandshakeFailure { .. } => "HandshakeFailure",
            QunetMessage::ClientClose { .. } => "ClientClose",
            QunetMessage::ServerClose { .. } => "ServerClose",
            QunetMessage::ClientReconnect { .. } => "ClientReconnect",
            QunetMessage::ConnectionError { .. } => "ConnectionError",
            QunetMessage::QdbChunkRequest { .. } => "QdbChunkRequest",
            QunetMessage::QdbChunkResponse { .. } => "QdbChunkResponse",
            QunetMessage::ReconnectSuccess => "ReconnectSuccess",
            QunetMessage::ReconnectFailure => "ReconnectFailure",
            QunetMessage::Data { .. } => "Data",
        }
    }
}
