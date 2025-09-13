use std::ops::{Deref, DerefMut};

use crate::message::BufferKind;

/// MsgData is a simple wrapper around a mutable reference to `BufferKind`.
pub struct MsgData<'a> {
    pub(crate) data: &'a mut BufferKind,
}

impl<'a> MsgData<'a> {
    pub fn new(data: &'a mut BufferKind) -> Self {
        MsgData { data }
    }

    pub fn discard(self) {
        self.data.reset();
    }

    pub fn take_buf(self) -> BufferKind {
        self.data.take()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.deref()
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.data.deref_mut()
    }
}

impl<'a> Deref for MsgData<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data.deref()
    }
}

impl<'a> DerefMut for MsgData<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.deref_mut()
    }
}
