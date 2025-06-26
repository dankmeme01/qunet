use std::io;

pub struct BinaryReader<'a, R: io::Read> {
    reader: &'a mut R,
}

impl<'a, R: io::Read> BinaryReader<'a, R> {
    #[inline]
    pub fn new(reader: &'a mut R) -> Self {
        BinaryReader { reader }
    }

    #[inline]
    pub fn read_bytes(&mut self, output: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(output)
    }

    #[inline]
    pub fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    #[inline]
    pub fn read_bool(&mut self) -> io::Result<bool> {
        let value = self.read_u8()?;
        Ok(value != 0)
    }

    #[inline]
    pub fn read_u16(&mut self) -> io::Result<u16> {
        let mut buf = [0; 2];
        self.reader.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_i8(&mut self) -> io::Result<i8> {
        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0] as i8)
    }

    #[inline]
    pub fn read_i16(&mut self) -> io::Result<i16> {
        let mut buf = [0; 2];
        self.reader.read_exact(&mut buf)?;
        Ok(i16::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_i32(&mut self) -> io::Result<i32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_i64(&mut self) -> io::Result<i64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_f32(&mut self) -> io::Result<f32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    #[inline]
    pub fn read_f64(&mut self) -> io::Result<f64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    pub fn read_varint(&mut self) -> io::Result<i64> {
        let mut value = 0i64;
        let mut shift = 0;
        let size = 64;
        let mut byte;

        loop {
            byte = self.read_u8()?;

            if shift == 63 && byte != 0 && byte != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Varint overflow",
                ));
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

    pub fn read_varuint(&mut self) -> io::Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            let byte = self.read_u8()?;

            if shift == 63 && byte > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Varuint overflow",
                ));
            }

            value |= u64::from(byte & 0x7f) << shift;

            if byte & 0x80 == 0 {
                break Ok(value);
            }

            shift += 7;
        }
    }

    pub fn read_string_var(&mut self) -> io::Result<String> {
        let length = self.read_varuint()? as usize;

        // arbitrary limit tbh
        if length > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "String too long",
            ));
        }

        let mut buffer = vec![0; length];
        self.read_bytes(&mut buffer)?;
        String::from_utf8(buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
