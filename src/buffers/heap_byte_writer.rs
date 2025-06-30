use std::io::Write;

use thiserror::Error;

#[derive(Debug, Default, Clone)]
pub struct HeapByteWriter {
    buffer: Vec<u8>,
    pos: usize,
}

#[derive(Debug, Clone, Error)]
pub enum HeapByteWriterError {
    #[error("Varint overflow")]
    VarintOverflow,
    #[error("String is longer than the maximum allowed length")]
    StringTooLong,
}

type Result<T> = std::result::Result<T, HeapByteWriterError>;

impl HeapByteWriter {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn set_pos(&mut self, pos: usize) {
        if pos > self.buffer.len() {
            self.buffer.resize(pos, 0);
        }

        self.pos = pos;
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn write_bytes(&mut self, data: &[u8]) {
        let len = data.len();

        if self.pos + len > self.buffer.len() {
            self.buffer.resize(self.pos + len, 0);
        }

        self.buffer[self.pos..self.pos + len].copy_from_slice(data);
        self.pos += len;
    }

    #[inline]
    pub fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value])
    }

    #[inline]
    pub fn write_u16(&mut self, value: u16) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_i8(&mut self, value: i8) {
        self.write_u8(value as u8)
    }

    #[inline]
    pub fn write_i16(&mut self, value: i16) {
        self.write_u16(value as u16)
    }

    #[inline]
    pub fn write_i32(&mut self, value: i32) {
        self.write_u32(value as u32)
    }

    #[inline]
    pub fn write_i64(&mut self, value: i64) {
        self.write_u64(value as u64)
    }

    #[inline]
    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(if value { 1 } else { 0 })
    }

    #[inline]
    pub fn write_f32(&mut self, value: f32) {
        self.write_u32(value.to_bits())
    }

    #[inline]
    pub fn write_f64(&mut self, value: f64) {
        self.write_u64(value.to_bits())
    }

    // https://github.com/gimli-rs/leb128/blob/master/src/lib.rs
    pub fn write_varint(&mut self, mut value: i64) -> Result<usize> {
        let mut written = 0;

        loop {
            let mut byte = value as u8;
            value >>= 6;

            let done = value == 0 || value == -1;

            if done {
                byte &= 0x7f;
            } else {
                value >>= 1;
                byte |= 0x80;
            }

            self.write_u8(byte);
            written += 1;

            if done {
                break Ok(written);
            }
        }
    }

    pub fn write_varuint(&mut self, mut value: u64) -> Result<usize> {
        let mut written = 0;

        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;

            if value != 0 {
                // set continuation bit
                byte |= 0x80;
            }

            self.write_u8(byte);
            written += 1;

            if value == 0 {
                break Ok(written);
            }
        }
    }

    pub fn write_string_var(&mut self, val: &str) -> Result<usize> {
        if val.len() > 1024 * 1024 {
            return Err(HeapByteWriterError::StringTooLong);
        }

        let mut written = 0;

        written += self.write_varuint(val.len() as u64)?;

        if !val.is_empty() {
            self.write_bytes(val.as_bytes());
            written += val.len();
        }

        Ok(written)
    }

    pub fn write_string_u8(&mut self, val: &str) -> Result<()> {
        if val.len() > u8::MAX as usize {
            return Err(HeapByteWriterError::StringTooLong);
        }

        self.write_u8(val.len() as u8);
        self.write_bytes(val.as_bytes());

        Ok(())
    }

    pub fn write_string_u16(&mut self, val: &str) -> Result<()> {
        if val.len() > u16::MAX as usize {
            return Err(HeapByteWriterError::StringTooLong);
        }

        self.write_u16(val.len() as u16);
        self.write_bytes(val.as_bytes());

        Ok(())
    }
}

impl Write for HeapByteWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_bytes(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
