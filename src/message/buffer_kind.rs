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
    /// Returns `None` if the buffer is not large enough to accommodate the requested size.
    ///
    /// # Safety
    /// If the buffer is a `BufferKind::Heap`, the returned slice is uninitialized memory.
    /// Reading from this memory is undefined behavior unless it's explicitly initialized.
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

    /// Sets the length of the buffer. This should be called after you wrote into the buffer returned by write_window.
    pub unsafe fn set_len(&mut self, len: usize) {
        match self {
            BufferKind::Heap(buf) => unsafe {
                buf.set_len(len);
            },
            BufferKind::Pooled { size, .. } => {
                *size = len;
            }
            BufferKind::Small { size, .. } => {
                *size = len;
            }

            BufferKind::Reference(arc) => {
                if let Some(inner) = Arc::get_mut(arc) {
                    unsafe { inner.set_len(len) }
                } else {
                    panic!(
                        "tried calling set_len on BufferKind::Reference with more than 1 reference"
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

    /// Returns a buffer with the same contents as this one, and resets this buffer.
    pub fn take(&mut self) -> BufferKind {
        let mut out = BufferKind::Heap(Vec::new());
        std::mem::swap(self, &mut out);
        out
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

    pub fn into_subslice(self, r_offset: usize, r_size: usize) -> BufferKind {
        match self {
            BufferKind::Pooled { buf, pos, size } => {
                debug_assert!(r_offset + r_size <= size);
                BufferKind::Pooled {
                    buf,
                    pos: pos + r_offset,
                    size: r_size,
                }
            }

            BufferKind::Small { buf, size } => {
                debug_assert!(size <= QUNET_SMALL_MESSAGE_SIZE);
                debug_assert!(r_offset + r_size <= size);

                let mut small_buf = [0u8; QUNET_SMALL_MESSAGE_SIZE];
                small_buf[..r_size].copy_from_slice(&buf[r_offset..r_offset + r_size]);

                BufferKind::Small { buf: small_buf, size: r_size }
            }

            BufferKind::Heap(mut heap) => {
                debug_assert!(r_offset + r_size <= heap.len());

                heap.copy_within(r_offset..r_offset + r_size, 0);
                heap.truncate(r_size);

                BufferKind::Heap(heap)
            }

            BufferKind::Reference(arc) => (*arc).clone().into_subslice(r_offset, r_size),
        }
    }
}

impl Clone for BufferKind {
    fn clone(&self) -> Self {
        let size = self.len();

        if size <= QUNET_SMALL_MESSAGE_SIZE {
            self.clone_into_small()
        } else {
            let mut new_buf = Self::new_heap(size);
            new_buf.append_bytes(self);
            new_buf
        }
    }
}

impl Default for BufferKind {
    fn default() -> Self {
        BufferKind::new_small()
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
