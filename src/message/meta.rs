use std::num::NonZeroU32;

use crate::{
    buffers::ByteReader,
    message::{CompressionType, DataHeader, QunetMessageDecodeError},
    protocol::*,
};

#[derive(Debug, Clone)]
pub(crate) struct CompressionHeader {
    // Invariant: this is never None
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

        let header = DataHeader::from_bits(data[0]);

        if !header.is_data() {
            // control messages never have additional headers.. except UDP connection ID
            // we don't need to parse the connection ID, it's been done before us, but we need to align the data properly

            let data_start = if udp { 9 } else { 1 };

            if data.len() < data_start {
                // Technically this code shouldn't ever be reachable, but let's be safe
                return Err(QunetMessageDecodeError::MissingConnectionId);
            }

            return Ok(QunetMessageMeta {
                bare: QunetMessageBareMeta {
                    header_byte: data[0],
                    is_data: false,
                    compression_header: None,
                    fragmentation_header: None,
                    reliability_header: None,
                    data_offset: data_start,
                },
                data: &data[data_start..],
            });
        }

        let mut reader = ByteReader::new(&data[1..]);

        let compression_header = match header.compression() {
            CompressionType::None => None,
            ty => Some(CompressionHeader {
                compression_type: ty,
                uncompressed_size: NonZeroU32::new(reader.read_u32()?)
                    .ok_or(QunetMessageDecodeError::UnexpectedZero)?,
            }),
        };

        // Connection ID, fragmentation and reliability headers are only present in UDP

        if udp {
            // skip connection ID, it's already been parsed before
            reader.skip_bytes(8)?;
        }

        let reliability_header = if udp && header.is_reliable() {
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

        let fragmentation_header = if udp && header.is_fragmented() {
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
            header_byte: data[0],
            is_data: true,
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
