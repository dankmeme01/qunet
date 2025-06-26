use thiserror::Error;

pub struct ByteWriter<'a> {
    buffer: &'a mut [u8],
    pos: usize,
}

#[derive(Debug, Clone, Error)]
pub enum ByteWriterError {
    #[error("Out of bounds write ({pos} >= {len})")]
    OutOfBounds { pos: usize, len: usize },
    #[error("Varint overflow")]
    VarintOverflow,
    #[error("String is longer than the maximum allowed length")]
    StringTooLong,
}

type Result<T> = std::result::Result<T, ByteWriterError>;

macro_rules! bw_int_method {
    ($name:ident, $try_name: ident, $value:ty) => {
        #[inline]
        pub fn $name(&mut self, value: $value) {
            self.write_bytes(&value.to_le_bytes())
        }

        #[inline]
        pub fn $try_name(&mut self, value: $value) -> Result<()> {
            self.try_write_bytes(&value.to_le_bytes())
        }
    };
}

impl<'a> ByteWriter<'a> {
    #[inline]
    pub fn new(buffer: &'a mut [u8]) -> Self {
        ByteWriter { buffer, pos: 0 }
    }

    #[inline]
    pub fn try_set_pos(&mut self, pos: usize) -> Result<()> {
        if pos > self.buffer.len() {
            return Err(ByteWriterError::OutOfBounds {
                pos,
                len: self.buffer.len(),
            });
        }
        self.pos = pos;
        Ok(())
    }

    #[inline]
    pub fn set_pos(&mut self, pos: usize) {
        if pos > self.buffer.len() {
            panic!(
                "Out of bounds set position (pos: {}, buffer len: {})",
                pos,
                self.buffer.len()
            );
        }

        self.pos = pos;
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn written(&self) -> &[u8] {
        &self.buffer[..self.pos]
    }

    #[inline]
    pub fn write_bytes(&mut self, data: &[u8]) {
        let len = data.len();

        if self.pos + len > self.buffer.len() {
            panic!(
                "Out of bounds write (pos: {}, len: {}, buffer len: {})",
                self.pos + len,
                len,
                self.buffer.len()
            );
        }

        self.buffer[self.pos..self.pos + len].copy_from_slice(data);
        self.pos += len;
    }

    #[inline]
    pub fn try_write_bytes(&mut self, data: &[u8]) -> Result<()> {
        let len = data.len();

        if self.pos + len > self.buffer.len() {
            return Err(ByteWriterError::OutOfBounds {
                pos: self.pos + len,
                len: self.buffer.len(),
            });
        }

        self.buffer[self.pos..self.pos + len].copy_from_slice(data);
        self.pos += len;
        Ok(())
    }

    bw_int_method!(write_u8, try_write_u8, u8);
    bw_int_method!(write_u16, try_write_u16, u16);
    bw_int_method!(write_u32, try_write_u32, u32);
    bw_int_method!(write_u64, try_write_u64, u64);
    bw_int_method!(write_i8, try_write_i8, i8);
    bw_int_method!(write_i16, try_write_i16, i16);
    bw_int_method!(write_i32, try_write_i32, i32);
    bw_int_method!(write_i64, try_write_i64, i64);
    bw_int_method!(write_f32, try_write_f32, f32);
    bw_int_method!(write_f64, try_write_f64, f64);

    #[inline]
    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(if value { 1 } else { 0 });
    }

    #[inline]
    pub fn try_write_bool(&mut self, value: bool) -> Result<()> {
        self.try_write_u8(if value { 1 } else { 0 })
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

            self.try_write_u8(byte)?;
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

            self.try_write_u8(byte)?;
            written += 1;

            if value == 0 {
                break Ok(written);
            }
        }
    }

    pub fn write_string_var(&mut self, val: &str) -> Result<usize> {
        if val.len() > 1024 * 1024 {
            return Err(ByteWriterError::StringTooLong);
        }

        let mut written = 0;

        written += self.write_varuint(val.len() as u64)?;

        if !val.is_empty() {
            self.try_write_bytes(val.as_bytes())?;
            written += val.len();
        }

        Ok(written)
    }

    pub fn write_string_u8(&mut self, val: &str) {
        if val.len() > u8::MAX as usize {
            panic!(
                "String is longer than the maximum allowed length ({} > {})",
                val.len(),
                u8::MAX
            );
        }

        self.write_u8(val.len() as u8);
        self.write_bytes(val.as_bytes());
    }

    pub fn try_write_string_u8(&mut self, val: &str) -> Result<()> {
        if val.len() > u8::MAX as usize {
            return Err(ByteWriterError::StringTooLong);
        }

        self.try_write_u8(val.len() as u8)?;
        self.try_write_bytes(val.as_bytes())
    }

    pub fn write_string_u16(&mut self, val: &str) {
        if val.len() > u16::MAX as usize {
            panic!(
                "String is longer than the maximum allowed length ({} > {})",
                val.len(),
                u16::MAX
            );
        }

        self.write_u16(val.len() as u16);
        self.write_bytes(val.as_bytes());
    }

    pub fn try_write_string_u16(&mut self, val: &str) -> Result<()> {
        if val.len() > u16::MAX as usize {
            return Err(ByteWriterError::StringTooLong);
        }

        self.try_write_u16(val.len() as u16)?;
        self.try_write_bytes(val.as_bytes())
    }
}
