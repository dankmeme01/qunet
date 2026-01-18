use std::{fmt::Display, num::NonZeroU32};

use crate::{
    buffers::{Bits, ByteReader},
    message::QunetMessageDecodeError,
    protocol::*,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Zstd,
    ZstdNoDict,
    Lz4,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::Zstd => write!(f, "Zstd"),
            CompressionType::ZstdNoDict => write!(f, "Zstd (no dict)"),
            CompressionType::Lz4 => write!(f, "Lz4"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompressionHeader {
    pub compression_type: CompressionType,
    pub uncompressed_size: NonZeroU32,
}

#[derive(Debug, Clone)]
pub(crate) struct FragmentationHeader {
    pub message_id: u16,
    pub fragment_index: u16,
    pub last_fragment: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ReliabilityHeader {
    pub message_id: u16,
    pub acks: heapless::Vec<u16, 8>,
}

pub struct QunetMessageBareMeta {
    pub(crate) header_byte: u8,
    pub(crate) is_data: bool,
    pub(crate) compression_header: Option<CompressionHeader>,
    pub(crate) fragmentation_header: Option<FragmentationHeader>,
    pub(crate) reliability_header: Option<ReliabilityHeader>,
    pub(crate) data_offset: usize,
}

pub struct QunetMessageMeta<'a> {
    pub(crate) bare: QunetMessageBareMeta,
    pub(crate) data: &'a [u8],
}

impl<'a> QunetMessageMeta<'a> {
    pub fn parse(data: &'a [u8], udp: bool) -> Result<Self, QunetMessageDecodeError> {
        assert!(!data.is_empty(), "data must not be empty");

        let header_byte = data[0];
        let is_data = header_byte & MSG_DATA_MASK != 0;

        if !is_data {
            // control messages never have additional headers.. except UDP connection ID
            // we don't need to parse the connection ID, it's been done before us, but we need to align the data properly

            let data_start = if udp { 9 } else { 1 };

            if data.len() < data_start {
                // Technically this code shouldn't ever be reachable, but let's be safe
                return Err(QunetMessageDecodeError::MissingConnectionId);
            }

            return Ok(QunetMessageMeta {
                bare: QunetMessageBareMeta {
                    header_byte,
                    is_data,
                    compression_header: None,
                    fragmentation_header: None,
                    reliability_header: None,
                    data_offset: data_start,
                },
                data: &data[data_start..],
            });
        }

        let bits = Bits::new(header_byte);
        let mut reader = ByteReader::new(&data[1..]);

        let compression_header = match bits.get_multiple_bits(0, 1) {
            0b00 => None,
            0b01 => Some(CompressionHeader {
                compression_type: CompressionType::Zstd,
                uncompressed_size: NonZeroU32::new(reader.read_u32()?)
                    .ok_or(QunetMessageDecodeError::UnexpectedZero)?,
            }),
            0b10 => Some(CompressionHeader {
                compression_type: CompressionType::ZstdNoDict,
                uncompressed_size: NonZeroU32::new(reader.read_u32()?)
                    .ok_or(QunetMessageDecodeError::UnexpectedZero)?,
            }),
            0b11 => Some(CompressionHeader {
                compression_type: CompressionType::Lz4,
                uncompressed_size: NonZeroU32::new(reader.read_u32()?)
                    .ok_or(QunetMessageDecodeError::UnexpectedZero)?,
            }),

            _ => unreachable!(),
        };

        // Connection ID, fragmentation and reliability headers are only present in UDP

        if udp {
            // skip connection ID, it's already been parsed before
            reader.skip_bytes(8)?;
        }

        let reliability_header = if udp && bits.get_bit(MSG_DATA_BIT_RELIABILITY) {
            let message_id = reader.read_u16()?;
            let ack_count = reader.read_u16()?.min(8);

            let mut acks = heapless::Vec::new();

            for _ in 0..ack_count {
                let ack_id = reader.read_u16()?;
                let _ = acks.push(ack_id);
            }

            Some(ReliabilityHeader { message_id, acks })
        } else {
            None
        };

        let fragmentation_header = if udp && bits.get_bit(MSG_DATA_BIT_FRAGMENTATION) {
            let message_id = reader.read_u16()?;
            let mut fragment_index = reader.read_u16()?;

            // Last fragment bit is the most significant bit inside fragment index
            let last_fragment = fragment_index & MSG_DATA_LAST_FRAGMENT_MASK != 0;
            fragment_index &= !MSG_DATA_LAST_FRAGMENT_MASK; // clear that bit

            Some(FragmentationHeader {
                message_id,
                fragment_index,
                last_fragment,
            })
        } else {
            None
        };

        let bare = QunetMessageBareMeta {
            header_byte,
            is_data,
            compression_header,
            fragmentation_header,
            reliability_header,
            data_offset: reader.pos() + 1, // +1 for the header byte, it was not included in the reader
        };

        Ok(QunetMessageMeta {
            bare,
            data: reader.remaining_bytes(),
        })
    }
}

impl<'a> From<QunetMessageMeta<'a>> for QunetMessageBareMeta {
    fn from(meta: QunetMessageMeta<'a>) -> Self {
        meta.bare
    }
}
