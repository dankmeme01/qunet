// bug in the crate i think
#![allow(clippy::unused_unit)]

use std::fmt::Display;

use bitpiece::*;

#[bitpiece(2)]
#[derive(Debug, PartialEq, Eq)]
pub enum CompressionType {
    None = 0,
    Zstd = 1,
    ZstdNoDict = 2,
    Lz4 = 3,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "None"),
            CompressionType::Zstd => write!(f, "Zstd"),
            CompressionType::ZstdNoDict => write!(f, "Zstd (no dict)"),
            CompressionType::Lz4 => write!(f, "Lz4"),
        }
    }
}

#[bitpiece(8)]
#[derive(Default)]
pub struct PingFlags {
    pub padding: B7,
    pub no_protocols: bool,
}

#[bitpiece(8)]
#[derive(Default)]
pub struct ClientCloseFlags {
    pub padding: B7,
    pub dont_terminate: bool,
}

#[bitpiece(8)]
pub struct DataHeader {
    // msb - always 1
    pub is_data: bool,
    // reserved bit, for future header expansions
    pub reserved_ext: bool,

    // fragmentation and reliability (UDP only)
    pub is_fragmented: bool,
    // fragmentation and reliability (UDP only)
    pub is_reliable: bool,

    // currently unused bits
    pub padding: B2,

    // compression
    pub compression: CompressionType,
}

impl Default for DataHeader {
    fn default() -> Self {
        Self::from_bits(0).with_is_data(true)
    }
}
