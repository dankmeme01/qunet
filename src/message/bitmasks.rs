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
    // bit 0 - omit protocols
    pub no_protocols: bool,
    pub padding: B7,
}

#[bitpiece(8)]
#[derive(Default)]
pub struct ClientCloseFlags {
    // bit 0 - don't terminate connection
    pub dont_terminate: bool,
    pub padding: B7,
}

#[bitpiece(8)]
pub struct DataHeader {
    // bits 0 and 1 - compression
    pub compression: CompressionType,

    // bits 2 and 3 - currently unused
    pub padding: B2,

    // bit 4 - reliability (UDP only)
    pub is_reliable: bool,

    // bit 5 - fragmentation (UDP only)
    pub is_fragmented: bool,

    // bit 6 - reserved for future header expansions
    pub reserved_ext: bool,

    // bit 7 (msb) - always 1
    pub is_data: bool,
}

impl Default for DataHeader {
    fn default() -> Self {
        Self::from_bits(0).with_is_data(true)
    }
}
