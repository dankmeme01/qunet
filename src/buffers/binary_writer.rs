use std::io;

pub struct BinaryWriter<'a, W: io::Write> {
    writer: &'a mut W,
}

impl<'a, W: io::Write> BinaryWriter<'a, W> {
    #[inline]
    pub fn new(writer: &'a mut W) -> Self {
        BinaryWriter { writer }
    }

    #[inline]
    pub fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)
    }

    #[inline]
    pub fn write_u8(&mut self, value: u8) -> io::Result<()> {
        self.writer.write_all(&[value])
    }

    #[inline]
    pub fn write_bool(&mut self, value: bool) -> io::Result<()> {
        self.write_u8(if value { 1 } else { 0 })
    }

    #[inline]
    pub fn write_u16(&mut self, value: u16) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_u32(&mut self, value: u32) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_u64(&mut self, value: u64) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_i8(&mut self, value: i8) -> io::Result<()> {
        self.writer.write_all(&[value as u8])
    }

    #[inline]
    pub fn write_i16(&mut self, value: i16) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_i32(&mut self, value: i32) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_i64(&mut self, value: i64) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_f32(&mut self, value: f32) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    #[inline]
    pub fn write_f64(&mut self, value: f64) -> io::Result<()> {
        self.writer.write_all(&value.to_le_bytes())
    }

    // https://github.com/gimli-rs/leb128/blob/master/src/lib.rs
    pub fn write_varint(&mut self, mut value: i64) -> io::Result<usize> {
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

            self.write_u8(byte)?;
            written += 1;

            if done {
                break Ok(written);
            }
        }
    }

    pub fn write_varuint(&mut self, mut value: u64) -> io::Result<usize> {
        let mut written = 0;

        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;

            if value != 0 {
                // set continuation bit
                byte |= 0x80;
            }

            self.write_u8(byte)?;
            written += 1;

            if value == 0 {
                break Ok(written);
            }
        }
    }

    pub fn write_string_var(&mut self, val: &str) -> io::Result<usize> {
        let mut written = 0;

        written += self.write_varuint(val.len() as u64)?;

        if !val.is_empty() {
            self.write_bytes(val.as_bytes())?;
            written += val.len();
        }

        Ok(written)
    }
}
