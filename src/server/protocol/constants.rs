//! This file defines many constants of the protocol, and it must be updated in case of protocol changes.

pub const MAJOR_VERSION: u16 = 1;

pub const MSG_PING: u8 = 0x01;
pub const MSG_PONG: u8 = 0x02;
pub const MSG_KEEPALIVE: u8 = 0x03;
pub const MSG_KEEPALIVE_RESPONSE: u8 = 0x04;
pub const MSG_HANDSHAKE_START: u8 = 0x05;
pub const MSG_HANDSHAKE_FINISH: u8 = 0x06;
pub const MSG_HANDSHAKE_FAILURE: u8 = 0x07;
pub const MSG_CLIENT_CLOSE: u8 = 0x08;
pub const MSG_SERVER_CLOSE: u8 = 0x09;
pub const MSG_CLIENT_RECONNECT: u8 = 10;
pub const MSG_CONNECTION_ERROR: u8 = 11;
pub const MSG_QDB_CHUNK_REQUEST: u8 = 12;
pub const MSG_QDB_CHUNK_RESPONSE: u8 = 13;

pub const MSG_QDBG_TOGGLE: u8 = 64;
pub const MSG_QDBG_REPORT: u8 = 65;

pub const MSG_DATA: u8 = 0x80;
pub const MSG_DATA_START: u8 = 0x80;
pub const MSG_DATA_END: u8 = 0xff;
pub const MSG_DATA_MASK: u8 = MSG_DATA;

pub const MSG_DATA_BIT_COMPRESSION_1: usize = 0; // least significant bit
pub const MSG_DATA_BIT_COMPRESSION_2: usize = 1; // second least significant bit
pub const MSG_DATA_BIT_RELIABILITY: usize = 4; // second least significant bit
pub const MSG_DATA_BIT_FRAGMENTATION: usize = 5; // second least significant bit

pub const MSG_DATA_LAST_FRAGMENT_MASK: u16 = 0x8000; // most significant bit of fragment index

pub const PROTO_TCP: u8 = 0x01;
pub const PROTO_UDP: u8 = 0x02;
pub const PROTO_QUIC: u8 = 0x03;
pub const PROTO_WEBSOCKET: u8 = 0x04;

pub const UDP_PACKET_LIMIT: usize = 1400;
pub const DEFAULT_MESSAGE_SIZE_LIMIT: usize = 1024 * 1024; // 1 MiB limit for incoming messages

pub const HANDSHAKE_START_SIZE: usize = 1 + 2 + 2 + 16; // header, qunet major, frag limit, qdb hash
pub const HANDSHAKE_HEADER_SIZE: usize = 1 + 9; // qunet header, connection ID (u64) + qdb presence (bool)
pub const HANDSHAKE_HEADER_SIZE_WITH_QDB: usize = HANDSHAKE_HEADER_SIZE + 16; // four u32s: uncompressed size, qdb size, chunk offset, chunk size
