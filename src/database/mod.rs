use std::io;

use thiserror::Error;

use crate::buffers::{binary_reader::BinaryReader, binary_writer::BinaryWriter, bits::Bits};

pub const CURRENT_VERSION: u16 = 1;

pub struct QunetStructField {
    pub name: String,
    pub r#type: u32,
}

#[derive(Debug, Clone)]
pub struct QunetEnumVariant<T: Clone> {
    pub name: String,
    pub value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QunetEventPriority {
    None,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QunetReliability {
    Unreliable,
    Reliable,
    ReliableOrdered,
}

pub enum QunetTypeKind {
    Bool,
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    VarString,
    U8String,
    U16String,
    Alias(u32),
    Struct {
        priority: QunetEventPriority,
        reliability: QunetReliability,
        fields: Vec<QunetStructField>,
    },
    EnumU8(Vec<QunetEnumVariant<u8>>),
    EnumU16(Vec<QunetEnumVariant<u16>>),
    EnumU32(Vec<QunetEnumVariant<u32>>),
    EnumI8(Vec<QunetEnumVariant<i8>>),
    EnumI16(Vec<QunetEnumVariant<i16>>),
    EnumI32(Vec<QunetEnumVariant<i32>>),
}

impl QunetTypeKind {
    pub fn name(&self) -> &str {
        match self {
            QunetTypeKind::Bool => "Bool",
            QunetTypeKind::U8 => "U8",
            QunetTypeKind::U16 => "U16",
            QunetTypeKind::U32 => "U32",
            QunetTypeKind::U64 => "U64",
            QunetTypeKind::I8 => "I8",
            QunetTypeKind::I16 => "I16",
            QunetTypeKind::I32 => "I32",
            QunetTypeKind::I64 => "I64",
            QunetTypeKind::F32 => "F32",
            QunetTypeKind::F64 => "F64",
            QunetTypeKind::VarString => "VarString",
            QunetTypeKind::U8String => "U8String",
            QunetTypeKind::U16String => "U16String",
            QunetTypeKind::Alias(_) => "Alias",
            QunetTypeKind::Struct { .. } => "Struct",
            QunetTypeKind::EnumU8(_) => "EnumU8",
            QunetTypeKind::EnumU16(_) => "EnumU16",
            QunetTypeKind::EnumU32(_) => "EnumU32",
            QunetTypeKind::EnumI8(_) => "EnumI8",
            QunetTypeKind::EnumI16(_) => "EnumI16",
            QunetTypeKind::EnumI32(_) => "EnumI32",
        }
    }
}

pub struct QunetType {
    pub id: u32,
    pub tag: u32,
    pub name: String,
    pub kind: QunetTypeKind,
}

#[derive(Default)]
pub struct QunetDatabase {
    pub tag_count: u32,
    pub type_count: u32,
    pub types: Vec<QunetType>,
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
    pub fn get_type_by_id(&self, id: u32) -> Option<&QunetType> {
        let idx = id - 1;
        if idx < self.types.len() as u32 {
            let ty = &self.types[idx as usize];

            debug_assert_eq!(
                ty.id, id,
                "Type ID mismatch (expected {} at idx {}, got {})",
                id, idx, ty.id
            );

            Some(ty)
        } else {
            None
        }
    }

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

    fn _decode_section<R: io::Read>(
        &mut self,
        section_type: u16,
        section_size: usize,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<(), DecodeError> {
        match section_type {
            1 => self._decode_data_section(reader),
            2 => self._decode_options_section(reader),
            3 => self._decode_zstd_dict_section(reader, section_size),
            _ => Ok(()), // skip unknown sections
        }
    }

    fn _decode_data_section<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<(), DecodeError> {
        self.type_count = reader.read_u32()?;
        self.tag_count = reader.read_u32()?;

        for id in 1..=self.type_count {
            let tag = self._read_tag(reader)?;
            let name = reader.read_string_var()?;
            let kind = self._decode_type(reader)?;

            self.types.push(QunetType { id, tag, name, kind });
        }

        Ok(())
    }

    fn _decode_type<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<QunetTypeKind, DecodeError> {
        let kind = reader.read_u8()?;

        Ok(match kind {
            1 => QunetTypeKind::Bool,
            2 => QunetTypeKind::U8,
            3 => QunetTypeKind::U16,
            4 => QunetTypeKind::U32,
            5 => QunetTypeKind::U64,
            6 => QunetTypeKind::I8,
            7 => QunetTypeKind::I16,
            8 => QunetTypeKind::I32,
            9 => QunetTypeKind::I64,
            10 => QunetTypeKind::F32,
            11 => QunetTypeKind::F64,
            12 => QunetTypeKind::VarString,
            13 => QunetTypeKind::U8String,
            14 => QunetTypeKind::U16String,

            // alias
            128 => {
                let type_id = self._read_type_id(reader)?;
                QunetTypeKind::Alias(type_id)
            }

            // struct
            129 => {
                let field_count = reader.read_varuint()?;
                let mut fields = Vec::new();

                for _ in 0..field_count {
                    let r#type = self._read_type_id(reader)?;
                    let name = reader.read_string_var()?;
                    fields.push(QunetStructField { name, r#type });
                }

                // note: priority and reliability are set in the options section, not here
                QunetTypeKind::Struct {
                    priority: QunetEventPriority::None,
                    reliability: QunetReliability::Unreliable,
                    fields,
                }
            }

            // enums
            160 => {
                let variant_count = reader.read_u8()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_u8()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumU8(variants)
            }

            161 => {
                let variant_count = reader.read_u16()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_u16()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumU16(variants)
            }

            162 => {
                let variant_count = reader.read_u32()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_u32()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumU32(variants)
            }

            163 => {
                let variant_count = reader.read_u8()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_i8()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumI8(variants)
            }

            164 => {
                let variant_count = reader.read_u16()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_i16()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumI16(variants)
            }

            165 => {
                let variant_count = reader.read_u32()? as usize;
                let mut variants = Vec::new();

                for _ in 0..variant_count {
                    let value = reader.read_i32()?;
                    let name = reader.read_string_var()?;
                    variants.push(QunetEnumVariant { name, value });
                }

                QunetTypeKind::EnumI32(variants)
            }

            _ => return Err(DecodeError::UnknownTypeKind(kind)),
        })
    }

    fn _read_tag<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<u32, DecodeError> {
        self._read_variable_size(reader, self.tag_count)
    }

    fn _read_type_id<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<u32, DecodeError> {
        self._read_variable_size(reader, self.type_count)
    }

    fn _read_variable_size<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
        max: u32,
    ) -> Result<u32, DecodeError> {
        let tc = max + 1;

        if tc <= u8::MAX as u32 {
            let value = reader.read_u8()?;
            if value as u32 >= tc {
                return Err(DecodeError::InvalidTagOrTypeUsed);
            }
            Ok(value as u32)
        } else if tc <= u16::MAX as u32 {
            let value = reader.read_u16()?;
            if value as u32 >= tc {
                return Err(DecodeError::InvalidTagOrTypeUsed);
            }
            Ok(value as u32)
        } else {
            let value = reader.read_u32()?;
            if value >= tc {
                return Err(DecodeError::InvalidTagOrTypeUsed);
            }
            Ok(value)
        }
    }

    fn _decode_options_section<R: io::Read>(
        &mut self,
        reader: &mut BinaryReader<'_, R>,
    ) -> Result<(), DecodeError> {
        let type_count = reader.read_u32()?;

        for _ in 0..type_count {
            let type_id = self._read_type_id(reader)?;
            let options = Bits::new(reader.read_u16()?);

            let priority = match options.get_multiple_bits(0, 1) {
                0b00 => QunetEventPriority::None,
                0b01 => QunetEventPriority::Low,
                0b10 => QunetEventPriority::Medium,
                0b11 => QunetEventPriority::High,
                _ => unreachable!(),
            };

            let reliability = match options.get_multiple_bits(2, 3) {
                0b00 => QunetReliability::Unreliable,
                0b01 => QunetReliability::Reliable,
                0b10 => {
                    return Err(DecodeError::InvalidOptionsSection(format!(
                        "Invalid reliability bits for type ID {type_id}"
                    )));
                }
                0b11 => QunetReliability::ReliableOrdered,
                _ => unreachable!(),
            };

            let type_idx = (type_id - 1) as usize;
            let ty = &mut self.types[type_idx];

            assert!(
                ty.id == type_id,
                "type ID mismatch when decoding options section, expected {} at index {}, got {}",
                type_id,
                type_idx,
                ty.id,
            );

            match ty.kind {
                QunetTypeKind::Struct {
                    reliability: ref mut r,
                    priority: ref mut p,
                    ..
                } => {
                    *r = reliability;
                    *p = priority;
                }

                _ => Err(DecodeError::InvalidOptionsSection(format!(
                    "Options are only valid for struct types, got type ID {type_id} with kind {:?}",
                    ty.kind.name()
                )))?,
            }
        }

        Ok(())
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
        const SECTIONS: u16 = 3; // count of sections that we know about

        // upper limit for the highest possible header size
        const FULL_HEADER_SIZE: usize = round_up_to_16(4 + 2 + 2 + SECTIONS as usize * 12);

        let encode_options = self._options_present();
        let encode_zstd = self.zstd_dict.is_some();
        let section_count: u16 = 1 + encode_options as u16 + encode_zstd as u16;

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

        println!("Data starts at: {data_start}, padding: {padding}");

        let mut section_offset = data_start as u32;

        let mut data_vec = Vec::new();
        let mut data_wr = BinaryWriter::new(&mut data_vec);

        self._write_section(1, &mut section_offset, &mut wr, &mut data_wr)?;

        if encode_options {
            self._write_section(2, &mut section_offset, &mut wr, &mut data_wr)?;
        }

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
            1 => self._encode_data_section(data_writer),
            2 => self._encode_options_section(data_writer),
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

    fn _options_present(&self) -> bool {
        self._count_types_with_options() > 0
    }

    fn _encode_data_section<W: io::Write>(&self, wr: &mut BinaryWriter<W>) -> io::Result<usize> {
        // ensure types are sorted by id
        if !self.types.is_sorted_by_key(|ty| ty.id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Types must be sorted by ID before encoding",
            ));
        }

        wr.write_u32(self.type_count)?;
        wr.write_u32(self.tag_count)?;

        let mut written = 8;

        let mut last_id = 0;

        for ty in &self.types {
            let id = ty.id;

            // ensure ids are sequential and only differ by 1
            if id <= last_id || id - last_id > 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Type IDs must be sequential and differ by 1",
                ));
            }

            last_id = id;

            written += self._write_tag(wr, ty.tag)?;
            written += wr.write_string_var(&ty.name)?;
            written += self._encode_type(wr, &ty.kind)?;
        }

        Ok(written)
    }

    fn _encode_type<W: io::Write>(
        &self,
        writer: &mut BinaryWriter<'_, W>,
        kind: &QunetTypeKind,
    ) -> io::Result<usize> {
        match kind {
            QunetTypeKind::Bool => writer.write_u8(1).map(|_| 1),
            QunetTypeKind::U8 => writer.write_u8(2).map(|_| 1),
            QunetTypeKind::U16 => writer.write_u8(3).map(|_| 1),
            QunetTypeKind::U32 => writer.write_u8(4).map(|_| 1),
            QunetTypeKind::U64 => writer.write_u8(5).map(|_| 1),
            QunetTypeKind::I8 => writer.write_u8(6).map(|_| 1),
            QunetTypeKind::I16 => writer.write_u8(7).map(|_| 1),
            QunetTypeKind::I32 => writer.write_u8(8).map(|_| 1),
            QunetTypeKind::I64 => writer.write_u8(9).map(|_| 1),
            QunetTypeKind::F32 => writer.write_u8(10).map(|_| 1),
            QunetTypeKind::F64 => writer.write_u8(11).map(|_| 1),
            QunetTypeKind::VarString => writer.write_u8(12).map(|_| 1),
            QunetTypeKind::U8String => writer.write_u8(13).map(|_| 1),
            QunetTypeKind::U16String => writer.write_u8(14).map(|_| 1),
            QunetTypeKind::Alias(type_id) => {
                writer.write_u8(128)?;
                Ok(self._write_type_id(writer, *type_id)? + 1)
            }
            QunetTypeKind::Struct { fields, .. } => {
                let mut written = 1;

                writer.write_u8(129)?;
                written += writer.write_varuint(fields.len() as u64)?;

                for field in fields {
                    written += self._write_type_id(writer, field.r#type)?;
                    written += writer.write_string_var(&field.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumU8(variants) => {
                let mut written = 2;

                writer.write_u8(160)?;
                writer.write_u8(variants.len() as u8)?;

                for variant in variants {
                    writer.write_u8(variant.value)?;
                    written += 1 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumU16(variants) => {
                let mut written = 3;

                writer.write_u8(161)?;
                writer.write_u16(variants.len() as u16)?;

                for variant in variants {
                    writer.write_u16(variant.value)?;
                    written += 2 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumU32(variants) => {
                let mut written = 5;

                writer.write_u8(162)?;
                writer.write_u32(variants.len() as u32)?;

                for variant in variants {
                    writer.write_u32(variant.value)?;
                    written += 4 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumI8(variants) => {
                let mut written = 2;

                writer.write_u8(163)?;
                writer.write_u8(variants.len() as u8)?;

                for variant in variants {
                    writer.write_i8(variant.value)?;
                    written += 1 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumI16(variants) => {
                let mut written = 3;

                writer.write_u8(164)?;
                writer.write_u16(variants.len() as u16)?;

                for variant in variants {
                    writer.write_i16(variant.value)?;
                    written += 2 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }

            QunetTypeKind::EnumI32(variants) => {
                let mut written = 5;

                writer.write_u8(165)?;
                writer.write_u32(variants.len() as u32)?;

                for variant in variants {
                    writer.write_i32(variant.value)?;
                    written += 4 + writer.write_string_var(&variant.name)?;
                }

                Ok(written)
            }
        }
    }

    fn _encode_options_section<W: io::Write>(&self, wr: &mut BinaryWriter<W>) -> io::Result<usize> {
        // first pass, count how many types there are options for
        let count = self._count_types_with_options();

        // write the count
        wr.write_u32(count as u32)?;

        let mut written = 4;

        for ty in &self.types {
            if let QunetTypeKind::Struct { priority, reliability, .. } = ty.kind
                && (priority != QunetEventPriority::None
                    || reliability != QunetReliability::Unreliable)
            {
                // write the type ID
                written += self._write_type_id(wr, ty.id)?;

                // write the options
                let mut options = Bits::<u16>::default();

                if priority != QunetEventPriority::None {
                    let bits: u16 = match priority {
                        QunetEventPriority::None => 0b00,
                        QunetEventPriority::Low => 0b01,
                        QunetEventPriority::Medium => 0b10,
                        QunetEventPriority::High => 0b11,
                    };

                    options.set_multiple_bits(0, 1, bits);
                }

                if reliability != QunetReliability::Unreliable {
                    let bits: u16 = match reliability {
                        QunetReliability::Reliable => 0b01,
                        QunetReliability::ReliableOrdered => 0b11,
                        QunetReliability::Unreliable => unreachable!(),
                    };

                    options.set_multiple_bits(2, 3, bits);
                }

                wr.write_u16(options.to_bits())?;
                written += 2;
            }
        }

        Ok(written)
    }

    fn _count_types_with_options(&self) -> usize {
        self.types
            .iter()
            .filter(|ty| {
                if let QunetTypeKind::Struct { priority, reliability, .. } = ty.kind {
                    priority != QunetEventPriority::None
                        || reliability != QunetReliability::Unreliable
                } else {
                    false
                }
            })
            .count()
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

    fn _write_tag<W: io::Write>(
        &self,
        writer: &mut BinaryWriter<'_, W>,
        tag: u32,
    ) -> io::Result<usize> {
        self._write_variable_size(writer, tag, self.tag_count)
    }

    fn _write_type_id<W: io::Write>(
        &self,
        writer: &mut BinaryWriter<'_, W>,
        ty: u32,
    ) -> io::Result<usize> {
        self._write_variable_size(writer, ty, self.type_count)
    }

    fn _write_variable_size<W: io::Write>(
        &self,
        writer: &mut BinaryWriter<'_, W>,
        val: u32,
        max: u32,
    ) -> io::Result<usize> {
        let tc = max + 1;

        if tc <= u8::MAX as u32 {
            writer.write_u8(val as u8)?;
            Ok(1)
        } else if tc <= u16::MAX as u32 {
            writer.write_u16(val as u16)?;
            Ok(2)
        } else {
            writer.write_u32(val)?;
            Ok(4)
        }
    }
}

const fn round_up_to_16(s: usize) -> usize {
    (s + 15) & !15
}
