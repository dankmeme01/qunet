use std::{fs::File, io, path::Path};

use thiserror::Error;

use crate::buffers::{BinaryReader, BinaryWriter};

pub const CURRENT_VERSION: u16 = 1;

#[derive(Default)]
pub struct QunetDatabase {
    pub tag_count: u32,
    pub type_count: u32,
    pub zstd_dict: Option<Vec<u8>>,
    pub zstd_level: i32,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("Unsupported database version")]
    UnsupportedVersion,
    #[error("Invalid database header")]
    InvalidHeader,
    #[error("Section size or offset in the header are invalid")]
    SectionSizeInvalid,
    #[error("No data section found in the database")]
    NoDataSection,
    #[error("Error decoding type section")]
    InvalidDataSection,
    #[error("Invalid tag or type ID used in the type section")]
    InvalidTagOrTypeUsed,
    #[error("Error decoding options section: {0}")]
    InvalidOptionsSection(String),
    #[error("Zstd dictionary section is too large ({0} bytes)")]
    ZstdDictTooLarge(usize),
    #[error("Unknown type kind: {0}")]
    UnknownTypeKind(u8),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
}

struct SectionHeader {
    r#type: u16,
    _options: u16,
    offset: u32,
    size: u32,
}

impl QunetDatabase {
    pub fn decode<R: io::Read>(reader: &mut R) -> Result<QunetDatabase, DecodeError> {
        let mut reader = BinaryReader::new(reader);

        // Read the header
        let mut magic = [0; 4];
        reader.read_bytes(&mut magic)?;

        if &magic != b"\xa3\xdb\xdb\x11" {
            return Err(DecodeError::InvalidHeader);
        }

        let version = reader.read_u16()?;
        if version != CURRENT_VERSION {
            return Err(DecodeError::UnsupportedVersion);
        }

        let section_count = reader.read_u16()?;
        let mut section_headers = Vec::new();

        for _ in 0..section_count {
            let section_type = reader.read_u16()?;
            let options = reader.read_u16()?;
            let offset = reader.read_u32()?;
            let size = reader.read_u32()?;

            if size == 0 || offset == 0 {
                return Err(DecodeError::SectionSizeInvalid);
            }

            section_headers.push(SectionHeader {
                r#type: section_type,
                _options: options,
                offset,
                size,
            });
        }

        let header_size = 4 + 2 + 2 + (section_count as usize * 12);

        // advance buffer until the next multiple of 16
        let padding = round_up_to_16(header_size) - header_size;
        let mut padding_buffer = [0u8; 16];
        reader.read_bytes(&mut padding_buffer[..padding])?;

        let sections_begin = header_size + padding;

        // reserve data in the buffer to fit all sections
        let mut data_buffer = Vec::new();

        for section in &section_headers {
            let section_end = section.offset as usize - sections_begin + section.size as usize;

            if section_end > data_buffer.len() {
                data_buffer.resize(section_end, 0);
            }
        }

        // read all sections into the data buffer
        reader.read_bytes(&mut data_buffer)?;

        let mut db = QunetDatabase::default();

        for section in &section_headers {
            let begin = section.offset as usize - sections_begin;
            let end = begin + section.size as usize;

            let mut section_data = &data_buffer[begin..end];

            let mut reader = BinaryReader::new(&mut section_data);

            db._decode_section(section.r#type, section.size as usize, &mut reader)?;
        }

        Ok(db)
    }

    pub fn from_file(path: &Path) -> Result<QunetDatabase, DecodeError> {
        let file = File::open(path)?;
        let mut reader = io::BufReader::new(file);
        QunetDatabase::decode(&mut reader)
    }

    fn _decode_section<R: io::Read>(
        &mut self,
        section_type: u16,
        section_size: usize,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<(), DecodeError> {
        match section_type {
            3 => self._decode_zstd_dict_section(reader, section_size),
            _ => Ok(()), // skip unknown sections
        }
    }

    fn _decode_zstd_dict_section<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
        section_size: usize,
    ) -> Result<(), DecodeError> {
        if section_size > 1024 * 1024 {
            return Err(DecodeError::ZstdDictTooLarge(section_size));
        }

        self.zstd_level = reader.read_i32()?;

        let mut buf = vec![0; section_size - 4];
        reader.read_bytes(&mut buf)?;

        self.zstd_dict = Some(buf);

        Ok(())
    }

    pub fn encode<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        const MAGIC: [u8; 4] = [0xa3, 0xdb, 0xdb, 0x11];
        const SECTIONS: u16 = 1; // count of sections that we know about

        // upper limit for the highest possible header size
        const FULL_HEADER_SIZE: usize = round_up_to_16(4 + 2 + 2 + SECTIONS as usize * 12);

        let encode_zstd = self.zstd_dict.is_some();
        let section_count: u16 = encode_zstd as u16;

        let mut header = [0u8; FULL_HEADER_SIZE];
        let mut header_wr = &mut header[..];

        let mut wr = BinaryWriter::new(&mut header_wr);
        wr.write_bytes(&MAGIC)?;
        wr.write_u16(CURRENT_VERSION)?;
        wr.write_u16(section_count)?;

        // header size without padding
        let header_size: usize = 4 + 2 + 2 + (section_count as usize * 12);

        let data_start = round_up_to_16(header_size);
        let padding = data_start - header_size;

        // println!("Data starts at: {data_start}, padding: {padding}");

        let mut section_offset = data_start as u32;

        let mut data_vec = Vec::new();
        let mut data_wr = BinaryWriter::new(&mut data_vec);

        if encode_zstd {
            self._write_section(3, &mut section_offset, &mut wr, &mut data_wr)?;
        }

        // write padding to the header
        let pad_bytes = [0u8; 16];
        wr.write_bytes(&pad_bytes[..padding])?;

        // write the data to the output
        writer.write_all(&header[..data_start])?;
        writer.write_all(&data_vec)?;

        // done!

        Ok(())
    }

    pub fn _write_section<WH: io::Write, WD: io::Write>(
        &self,
        section_type: u16,
        section_offset: &mut u32,
        header_writer: &mut BinaryWriter<'_, WH>,
        data_writer: &mut BinaryWriter<'_, WD>,
    ) -> io::Result<()> {
        let written = match section_type {
            3 => self._encode_zstd_dict_section(data_writer),
            _ => panic!("Unknown section type: {section_type}"),
        }?;

        // write the header
        header_writer.write_u16(section_type)?;
        header_writer.write_u16(0)?; // options, not used
        header_writer.write_u32(*section_offset)?;
        header_writer.write_u32(written as u32)?;

        // update the section offset
        *section_offset += written as u32;
        let new_offset = round_up_to_16(*section_offset as usize) as u32;
        let padding = new_offset - *section_offset;
        *section_offset = new_offset;

        let padding_bytes = [0u8; 16];
        data_writer.write_bytes(&padding_bytes[..padding as usize])?;

        println!("Wrote {padding} bytes of padding for section {section_type}");

        Ok(())
    }

    fn _encode_zstd_dict_section<W: io::Write>(
        &self,
        wr: &mut BinaryWriter<W>,
    ) -> io::Result<usize> {
        wr.write_i32(self.zstd_level)?;
        let data = self.zstd_dict.as_deref().unwrap_or(&[]);
        wr.write_bytes(data)?;
        Ok(data.len())
    }
}

const fn round_up_to_16(s: usize) -> usize {
    (s + 15) & !15
}
