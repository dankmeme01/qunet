use crate::transport::lowlevel::uninit_box_bytes;

/// Circular byte buffer implementation
/// This buffer allows efficient amortized read/write operations, which are a simple memcpy,
/// while also efficiently storing the data and only reallocating when the reader is significantly behind the writer.
/// Due to this, random access is not easy, `peek` may return 2 spans if the requested data wraps around the end of the buffer.
#[derive(Debug, Clone, Default)]
pub struct CircularByteBuffer {
    buf: Option<Box<[u8]>>,
    start_idx: usize,
    end_idx: usize,
    size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotEnoughData;

#[derive(Debug, Clone)]
pub struct WrappedRead<'a> {
    pub first: &'a [u8],
    pub second: Option<&'a [u8]>,
}

impl CircularByteBuffer {
    #[inline]
    pub fn new() -> Self {
        CircularByteBuffer {
            buf: None,
            start_idx: 0,
            end_idx: 0,
            size: 0,
        }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        CircularByteBuffer {
            buf: Some(uninit_box_bytes(cap)),
            start_idx: 0,
            end_idx: 0,
            size: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.start_idx = 0;
        self.end_idx = 0;
        self.size = 0;
    }

    #[inline]
    pub fn reserve(&mut self, extra: usize) {
        self.grow_until_at_least(self.capacity() + extra);
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.as_ref().map_or(0, |buf| buf.len())
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn write(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        let rem_space = self.capacity() - self.size;
        if data.len() > rem_space {
            self.grow_until_at_least(self.size + data.len());
        }

        let capacity = self.capacity();

        debug_assert!(capacity >= self.size + data.len());

        // now, there are two cases:

        // 1. end < start, we have a guarantee that we can always write everything without wrapping
        if self.end_idx < self.start_idx {
            debug_assert!(self.end_idx + data.len() <= self.capacity());
            debug_assert!(self.end_idx + data.len() <= self.start_idx);

            let end_idx = self.end_idx;
            let bytes = self.buf_bytes_mut();

            bytes[end_idx..end_idx + data.len()].copy_from_slice(data);
            self.end_idx += data.len();
        } else {
            // 2. end >= start, depending on the length of the input data, we may need to wrap around
            let tail_space = capacity - self.end_idx;
            let written = data.len().min(tail_space);

            let end_idx = self.end_idx;
            let bytes = self.buf_bytes_mut();

            bytes[end_idx..end_idx + written].copy_from_slice(&data[..written]);

            debug_assert!(end_idx + written <= capacity);

            // if we wrote everything, we are done here!
            if written == data.len() {
                self.end_idx += written;
            } else {
                // otherwise, we need to wrap around and write the rest of the data
                let remaining = data.len() - written;
                bytes[..rem_space].copy_from_slice(&data[written..]);
                self.end_idx = remaining;

                debug_assert!(self.end_idx <= self.start_idx);
                debug_assert!(self.end_idx <= capacity);
            }
        }

        self.size += data.len();
    }

    #[inline]
    fn buf_bytes_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut().expect("Buffer is not initialized")
    }

    #[inline]
    fn buf_bytes(&self) -> &[u8] {
        self.buf.as_ref().expect("Buffer is not initialized")
    }

    pub fn read_into(&mut self, data: &mut [u8]) -> Result<(), NotEnoughData> {
        self.peek_into(data)?;
        self.skip(data.len())?;
        Ok(())
    }

    pub fn peek_into(&self, out: &mut [u8]) -> Result<(), NotEnoughData> {
        let wrapped = self.peek(out.len())?;

        out[..wrapped.first.len()].copy_from_slice(wrapped.first);

        if let Some(second) = wrapped.second {
            out[wrapped.first.len()..].copy_from_slice(second);
        }

        Ok(())
    }

    pub fn peek(&self, len: usize) -> Result<WrappedRead<'_>, NotEnoughData> {
        if len > self.size {
            return Err(NotEnoughData);
        }

        let bytes = self.buf_bytes();

        let first =
            &bytes[self.start_idx..self.start_idx + len.min(self.capacity() - self.start_idx)];

        let remaining = len - first.len();

        if remaining == 0 {
            Ok(WrappedRead { first, second: None })
        } else {
            let second = &bytes[0..remaining];
            Ok(WrappedRead { first, second: Some(second) })
        }
    }

    pub fn skip(&mut self, len: usize) -> Result<(), NotEnoughData> {
        if len > self.size {
            return Err(NotEnoughData);
        }

        let capacity = self.capacity();
        let new_start_idx = (self.start_idx + len) % capacity;

        self.start_idx = new_start_idx;
        self.size -= len;

        Ok(())
    }

    fn grow_until_at_least(&mut self, min_size: usize) {
        let mut cur_cap = self.capacity();

        if cur_cap == 0 {
            cur_cap = 64;
        }

        while cur_cap < min_size {
            cur_cap *= 2;
        }

        self.grow_to(cur_cap);
    }

    fn grow_to(&mut self, new_cap: usize) {
        let mut new_buf = uninit_box_bytes(new_cap);

        // See comments in qunet-cpp/src/buffers/CircularByteBuffer.cpp for more info

        if self.start_idx < self.end_idx || (self.start_idx == self.end_idx && self.size == 0) {
            // case 1 and 3, no wrapping
            new_buf[..self.size].copy_from_slice(&self.buf_bytes()[self.start_idx..self.end_idx]);
        } else {
            // case 2 and 4, wrapping
            let start_part_size = self.end_idx;
            let end_part_size = self.capacity() - self.start_idx;

            let bytes = self.buf_bytes();

            new_buf[..end_part_size].copy_from_slice(&bytes[self.start_idx..]);
            new_buf[end_part_size..end_part_size + start_part_size]
                .copy_from_slice(&bytes[..self.end_idx]);
        }

        self.start_idx = 0;
        self.end_idx = self.size;
        self.buf = Some(new_buf);
    }
}
