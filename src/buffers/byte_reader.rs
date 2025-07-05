use thiserror::Error;

use crate::buffers::bits::{Bits, Integer};

pub struct ByteReader<'a> {
    buffer: &'a [u8],
    pos: usize,
}

#[derive(Debug, Clone, Error)]
pub enum ByteReaderError {
    #[error("Out of bounds read ({pos} >= {len})")]
    OutOfBounds { pos: usize, len: usize },
    #[error("Varint overflow")]
    VarintOverflow,
    #[error("String is longer than the maximum allowed length")]
    StringTooLong,
    #[error("String is not valid UTF-8")]
    InvalidUtf8,
}

type Result<T> = std::result::Result<T, ByteReaderError>;

impl<'a> ByteReader<'a> {
    #[inline]
    pub fn new(buffer: &'a [u8]) -> Self {
        ByteReader { buffer, pos: 0 }
    }

    #[inline]
    pub fn set_pos(&mut self, pos: usize) -> Result<()> {
        if pos > self.buffer.len() {
            return Err(ByteReaderError::OutOfBounds {
                pos,
                len: self.buffer.len(),
            });
        }
        self.pos = pos;
        Ok(())
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buffer.len() - self.pos
    }

    #[inline]
    pub fn remaining_bytes(&self) -> &'a [u8] {
        &self.buffer[self.pos..]
    }

    #[inline]
    pub fn read_bytes(&mut self, out: &mut [u8]) -> Result<()> {
        let len = out.len();

        if self.pos + len > self.buffer.len() {
            return Err(ByteReaderError::OutOfBounds {
                pos: self.pos + len,
                len: self.buffer.len(),
            });
        }

        out.copy_from_slice(&self.buffer[self.pos..self.pos + len]);
        self.pos += len;
        Ok(())
    }

    #[inline]
    pub fn skip_bytes(&mut self, len: usize) -> Result<()> {
        if self.pos + len > self.buffer.len() {
            return Err(ByteReaderError::OutOfBounds {
                pos: self.pos + len,
                len: self.buffer.len(),
            });
        }

        self.pos += len;
        Ok(())
    }

    #[inline]
    pub fn read_u8(&mut self) -> Result<u8> {
        let mut out = [0u8; 1];
        self.read_bytes(&mut out)?;
        Ok(out[0])
    }

    #[inline]
    pub fn read_bool(&mut self) -> Result<bool> {
        let value = self.read_u8()?;
        Ok(value != 0)
    }

    #[inline]
    pub fn read_u16(&mut self) -> Result<u16> {
        let mut out = [0u8; 2];
        self.read_bytes(&mut out)?;
        Ok(u16::from_le_bytes(out))
    }

    #[inline]
    pub fn read_u32(&mut self) -> Result<u32> {
        let mut out = [0u8; 4];
        self.read_bytes(&mut out)?;
        Ok(u32::from_le_bytes(out))
    }

    #[inline]
    pub fn read_u64(&mut self) -> Result<u64> {
        let mut out = [0u8; 8];
        self.read_bytes(&mut out)?;
        Ok(u64::from_le_bytes(out))
    }

    #[inline]
    pub fn read_i8(&mut self) -> Result<i8> {
        let mut out = [0u8; 1];
        self.read_bytes(&mut out)?;
        Ok(out[0] as i8)
    }

    #[inline]
    pub fn read_i16(&mut self) -> Result<i16> {
        let mut out = [0u8; 2];
        self.read_bytes(&mut out)?;
        Ok(i16::from_le_bytes(out))
    }

    #[inline]
    pub fn read_i32(&mut self) -> Result<i32> {
        let mut out = [0u8; 4];
        self.read_bytes(&mut out)?;
        Ok(i32::from_le_bytes(out))
    }

    #[inline]
    pub fn read_i64(&mut self) -> Result<i64> {
        let mut out = [0u8; 8];
        self.read_bytes(&mut out)?;
        Ok(i64::from_le_bytes(out))
    }

    #[inline]
    pub fn read_f32(&mut self) -> Result<f32> {
        let mut out = [0u8; 4];
        self.read_bytes(&mut out)?;
        Ok(f32::from_le_bytes(out))
    }

    #[inline]
    pub fn read_f64(&mut self) -> Result<f64> {
        let mut out = [0u8; 8];
        self.read_bytes(&mut out)?;
        Ok(f64::from_le_bytes(out))
    }

    #[inline]
    pub fn read_bits<T: Integer>(&mut self) -> Result<Bits<T>>
    where
        [(); T::SIZE]:,
    {
        let mut out = [0u8; T::SIZE];
        self.read_bytes(&mut out)?;

        Ok(Bits::new(T::decode(out)))
    }

    pub fn read_varint(&mut self) -> Result<i64> {
        let mut value = 0i64;
        let mut shift = 0;
        let size = 64;
        let mut byte;

        loop {
            byte = self.read_u8()?;

            if shift == 63 && byte != 0 && byte != 1 {
                return Err(ByteReaderError::VarintOverflow);
            }

            value |= i64::from(byte & 0x7f) << shift;
            shift += 7;

            if byte & 0x80 == 0 {
                break;
            }
        }

        if shift < size && (byte & 0x40) != 0 {
            value |= -1 << shift;
        }

        Ok(value)
    }

    pub fn read_varuint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            let byte = self.read_u8()?;

            if shift == 63 && byte > 1 {
                return Err(ByteReaderError::VarintOverflow);
            }

            value |= u64::from(byte & 0x7f) << shift;

            if byte & 0x80 == 0 {
                break Ok(value);
            }

            shift += 7;
        }
    }

    pub fn read_string_fixed(&mut self, length: usize) -> Result<&str> {
        if length > self.remaining() {
            return Err(ByteReaderError::OutOfBounds {
                pos: self.pos + length,
                len: self.buffer.len(),
            });
        }

        let str_bytes = &self.buffer[self.pos..self.pos + length];
        self.pos += length;

        std::str::from_utf8(str_bytes).map_err(|_| ByteReaderError::InvalidUtf8)
    }

    pub fn read_string_var(&mut self) -> Result<&str> {
        let length = self.read_varuint()? as usize;

        // arbitrary limit tbh
        if length > 1024 * 1024 {
            return Err(ByteReaderError::StringTooLong);
        }

        self.read_string_fixed(length)
    }

    pub fn read_string_u8(&mut self) -> Result<&str> {
        let length = self.read_u8()? as usize;

        self.read_string_fixed(length)
    }

    pub fn read_string_u16(&mut self) -> Result<&str> {
        let length = self.read_u16()? as usize;

        self.read_string_fixed(length)
    }
}
