use std::{
    io::Write,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{buffers::PooledBuffer, message::QUNET_SMALL_MESSAGE_SIZE};

pub enum BufferKind {
    Heap(Vec<u8>),
    Pooled {
        buf: PooledBuffer,
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
    pub fn new_small() -> Self {
        BufferKind::Small {
            buf: [0; QUNET_SMALL_MESSAGE_SIZE],
            size: 0,
        }
    }

    pub fn new_pooled(buf: PooledBuffer) -> Self {
        BufferKind::Pooled { buf, pos: 0, size: 0 }
    }

    pub fn new_heap(cap: usize) -> Self {
        BufferKind::Heap(Vec::with_capacity(cap))
    }

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
                        "tried calling append_bytes on BufferKind::Reference with more than 1 reference"
                    );
                }
            }
        }
    }

    /// Returns a mutable slice of the buffer that can be used for writing.
    /// For `BufferKind::Heap`, it will return unallocated memory, which must not be read from, only written to.
    /// Returns `None` if the buffer is not large enough to accommodate the requested size.
    pub unsafe fn write_window(&mut self, size: usize) -> Option<&mut [u8]> {
        match self {
            BufferKind::Heap(buf) => {
                if size > buf.capacity() {
                    return None;
                }

                unsafe {
                    buf.set_len(size);
                }

                Some(&mut buf[..size])
            }
            BufferKind::Pooled { buf, pos, .. } => {
                if *pos + size > buf.len() {
                    return None;
                }

                Some(&mut buf[*pos..*pos + size])
            }

            BufferKind::Small { buf, .. } => {
                if buf.len() < size {
                    return None;
                }

                Some(&mut buf[..size])
            }

            BufferKind::Reference(arc) => {
                if let Some(inner) = Arc::get_mut(arc) {
                    unsafe { inner.write_window(size) }
                } else {
                    panic!(
                        "tried calling write_window on BufferKind::Reference with more than 1 reference"
                    );
                }
            }
        }
    }

    /// Resets the buffer. Any held memory will be completely released, and the buffer
    /// will be reset to a `Heap` buffer with zero capacity.
    /// This is intended to be used when the buffer is no longer needed.
    pub fn reset(&mut self) {
        *self = BufferKind::Heap(Vec::new());
    }

    /// Clones the buffer into a `BufferKind::Small` buffer.
    /// If the inner data is larger than `QUNET_SMALL_MESSAGE_SIZE`, it will panic.
    pub fn clone_into_small(&self) -> BufferKind {
        let mut out = BufferKind::new_small();
        if !out.append_bytes(self) {
            panic!(
                "tried to clone a buffer into a small buffer, but the data is too large ({} bytes)",
                self.len()
            );
        }

        out
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
