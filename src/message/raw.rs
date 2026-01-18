use std::ops::Deref;

use crate::buffers::PooledBuffer;

pub const QUNET_SMALL_MESSAGE_SIZE: usize = 71; // 119

pub enum QunetRawMessage {
    Small {
        data: [u8; QUNET_SMALL_MESSAGE_SIZE],
        len: usize,
    },

    Large {
        buffer: PooledBuffer,
        len: usize,
    },
}

impl QunetRawMessage {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            QunetRawMessage::Small { data, len } => &data[..*len],
            QunetRawMessage::Large { buffer, len } => &buffer[..*len],
        }
    }
}

impl Deref for QunetRawMessage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}
