use std::{
    io::Write,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{buffers::buffer_pool::BorrowedMutBuffer, message::QUNET_SMALL_MESSAGE_SIZE};

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
    Reference(Arc<BufferKind>),
}

impl BufferKind {
    pub fn append_bytes(&mut self, data: &[u8]) -> bool {
        match self {
            BufferKind::Heap(buf) => {
                buf.extend_from_slice(data);
                true
            }

            BufferKind::Pooled { buf, pos, size } => {
                let pushed_start = *pos + *size;
                let new_end = pushed_start + data.len();

                if new_end > buf.len() {
                    return false; // not enough space
                }

                buf[pushed_start..new_end].copy_from_slice(data);
                *size += data.len();

                true
            }

            BufferKind::Small { buf, size } => {
                let pushed_start = *size;
                let new_end = pushed_start + data.len();

                if new_end > buf.len() {
                    return false; // not enough space
                }

                buf[pushed_start..new_end].copy_from_slice(data);
                *size += data.len();

                true
            }

            BufferKind::Reference(arc) => {
                if let Some(inner) = Arc::get_mut(arc) {
                    inner.append_bytes(data)
                } else {
                    panic!(
                        "tried calling deref_mut on BufferKind::Reference with more than 1 reference"
                    );
                }
            }
        }
    }
}

impl Write for BufferKind {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.append_bytes(buf) {
            Ok(buf.len())
        } else {
            Err(std::io::Error::other("buffer overflow"))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Deref for BufferKind {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            BufferKind::Heap(buf) => buf,
            BufferKind::Pooled { buf, pos, size } => &buf[*pos..*pos + *size],
            BufferKind::Small { buf, size } => &buf[..*size],
            BufferKind::Reference(arc) => arc.deref(),
        }
    }
}

impl DerefMut for BufferKind {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            BufferKind::Heap(buf) => buf,
            BufferKind::Pooled { buf, pos, size } => &mut buf[*pos..*pos + *size],
            BufferKind::Small { buf, size } => &mut buf[..*size],
            BufferKind::Reference(arc) => {
                if let Some(inner) = Arc::get_mut(arc) {
                    inner.deref_mut()
                } else {
                    panic!(
                        "tried calling deref_mut on BufferKind::Reference with more than 1 reference"
                    );
                }
            }
        }
    }
}
