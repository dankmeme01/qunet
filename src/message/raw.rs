use std::ops::Deref;

use crate::message::BufferKind;

pub const QUNET_SMALL_MESSAGE_SIZE: usize = 71; // 119

pub struct QunetRawMessage(pub BufferKind);

impl QunetRawMessage {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.deref()
    }
}

impl Deref for QunetRawMessage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}
