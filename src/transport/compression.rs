use std::{cell::RefCell, ops::DerefMut, sync::Arc};

use thiserror::Error;
use zstd_safe::{CCtx, CDict, DCtx, DDict};

use crate::{buffers::BufPool, message::BufferKind, protocol::MSG_ZSTD_COMPRESSION_LEVEL};

thread_local! {
    static ZSTD_CCTX: RefCell<CCtx<'static>> = RefCell::new(CCtx::create());
    static ZSTD_DCTX: RefCell<DCtx<'static>> = RefCell::new(DCtx::create());
}

#[derive(Debug, Error)]
pub enum CompressError {
    #[error("zstd compression failed: {0}")]
    Zstd(&'static str),
    #[error("lz4 compression failed: {0}")]
    Lz4(#[from] lz4_flex::block::CompressError),
}

#[derive(Debug, Error)]
pub enum DecompressError {
    #[error("zstd decompression failed: {0}")]
    Zstd(&'static str),
}

#[inline]
fn map_zstd_compress<T>(result: Result<T, usize>) -> Result<T, CompressError> {
    result.map_err(|e| CompressError::Zstd(zstd_safe::get_error_name(e)))
}

#[inline]
fn map_zstd_decompress<T>(result: Result<T, usize>) -> Result<T, DecompressError> {
    result.map_err(|e| DecompressError::Zstd(zstd_safe::get_error_name(e)))
}

#[inline]
pub fn zstd_compress_bound(size: usize) -> usize {
    zstd_safe::compress_bound(size)
}

/// Compresses data using Zstandard, optionally using a provided compression dictionary.
/// Returns amount of bytes written to the output buffer on success.
#[inline]
pub fn zstd_compress(
    data: &[u8],
    output: &mut [u8],
    dict: Option<&CDict<'_>>,
) -> Result<usize, CompressError> {
    zstd_compress_level(data, output, dict, MSG_ZSTD_COMPRESSION_LEVEL)
}

/// Like `zstd_compress`, but allows specifying a custom compression level.
/// Note that if a dictionary is provided, the level is ignored.
pub fn zstd_compress_level(
    data: &[u8],
    output: &mut [u8],
    dict: Option<&CDict<'_>>,
    level: i32,
) -> Result<usize, CompressError> {
    let result = ZSTD_CCTX.with_borrow_mut(|cctx| {
        if let Some(dict) = dict {
            cctx.compress_using_cdict(output, data, dict)
        } else {
            cctx.compress(output, data, level)
        }
    });

    map_zstd_compress(result)
}

/// Decompresses data using Zstandard, optionally using a provided decompression dictionary.
/// Returns amount of bytes written to the output buffer on success.
pub fn zstd_decompress(
    data: &[u8],
    output: &mut [u8],
    dict: Option<&DDict<'_>>,
) -> Result<usize, DecompressError> {
    let result = ZSTD_DCTX.with_borrow_mut(|ctx| {
        if let Some(dict) = dict {
            ctx.decompress_using_ddict(output, data, dict)
        } else {
            ctx.decompress(output, data)
        }
    });

    map_zstd_decompress(result)
}

pub fn lz4_compress(data: &[u8], output: &mut [u8]) -> Result<usize, CompressError> {
    Ok(lz4_flex::compress_into(data, output)?)
}

// Trait for servers / clients that hold a compression dictionary/context

pub trait CompressionHandler {
    fn compress_zstd(&self, data: &[u8], use_dict: bool) -> Result<BufferKind, CompressError>;

    fn decompress_zstd(
        &self,
        data: &[u8],
        uncompressed_size: usize,
        use_dict: bool,
    ) -> Result<BufferKind, DecompressError>;

    fn compress_lz4(&self, data: &[u8]) -> Result<BufferKind, CompressError>;

    fn decompress_lz4(
        &self,
        data: &[u8],
        uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError>;
}

// A compression handler that works with a buffer pool and optional dictionaries

pub struct CompressionHandlerImpl<P: BufPool> {
    zstd_cdict: Option<CDict<'static>>,
    zstd_ddict: Option<DDict<'static>>,
    buffer_pool: Arc<P>,
}

impl<P: BufPool> CompressionHandlerImpl<P> {
    pub fn new(buffer_pool: Arc<P>) -> Self {
        Self {
            zstd_cdict: None,
            zstd_ddict: None,
            buffer_pool,
        }
    }

    pub fn init_zstd_cdict(&mut self, data: &[u8], level: i32) {
        self.zstd_cdict = Some(CDict::create(data, level));
    }

    pub fn init_zstd_ddict(&mut self, data: &[u8]) {
        self.zstd_ddict = Some(DDict::create(data));
    }
}

impl<P: BufPool> CompressionHandlerImpl<P> {
    #[inline]
    fn get_new_buffer(&self, size: usize) -> BufferKind {
        self.buffer_pool.get_or_heap(size)
    }

    /// Safety: this function assumes the buffer will only be used for writing.
    unsafe fn write_buf_from_buffer_kind(buf: &mut BufferKind) -> &mut [u8] {
        match buf {
            BufferKind::Heap(vec) => unsafe {
                std::slice::from_raw_parts_mut(vec.as_mut_ptr(), vec.capacity())
            },

            BufferKind::Pooled { buf, .. } => {
                let cap = buf.len();
                &mut buf.deref_mut()[..cap]
            }

            BufferKind::Small { buf, size } => &mut buf[..*size],

            BufferKind::Reference(_) => unreachable!(),
        }
    }

    unsafe fn set_buffer_kind_len(buf: &mut BufferKind, len: usize) {
        match buf {
            BufferKind::Heap(vec) => unsafe { vec.set_len(len) },
            BufferKind::Pooled { size, .. } => *size = len,
            BufferKind::Small { size, .. } => *size = len,
            BufferKind::Reference(_) => unreachable!(),
        }
    }
}

impl<P: BufPool> CompressionHandler for CompressionHandlerImpl<P> {
    fn compress_zstd(&self, data: &[u8], use_dict: bool) -> Result<BufferKind, CompressError> {
        let needed_len = zstd_compress_bound(data.len());

        let mut buf = self.get_new_buffer(needed_len);

        // safety: the buffer is only used for writing
        let output = unsafe { Self::write_buf_from_buffer_kind(&mut buf) };

        debug_assert!(
            output.len() >= needed_len,
            "Output buffer is too small ({} < {})",
            output.len(),
            needed_len
        );

        let written = zstd_compress(data, output, self.zstd_cdict.as_ref().take_if(|_| use_dict))?;

        // safety: zstd guarantees that exactly `size` bytes are written to the output buffer
        unsafe { Self::set_buffer_kind_len(&mut buf, written) };
        Ok(buf)
    }

    fn decompress_zstd(
        &self,
        data: &[u8],
        uncompressed_size: usize,
        use_dict: bool,
    ) -> Result<BufferKind, DecompressError> {
        let mut buf = self.get_new_buffer(uncompressed_size);

        // safety: the buffer is only used for writing
        let output = unsafe { Self::write_buf_from_buffer_kind(&mut buf) };

        debug_assert!(
            output.len() >= uncompressed_size,
            "Output buffer is too small ({} < {})",
            output.len(),
            uncompressed_size
        );

        let written =
            zstd_decompress(data, output, self.zstd_ddict.as_ref().take_if(|_| use_dict))?;

        // safety: zstd guarantees that exactly `size` bytes are written to the output buffer
        unsafe { Self::set_buffer_kind_len(&mut buf, written) };
        Ok(buf)
    }

    fn compress_lz4(&self, data: &[u8]) -> Result<BufferKind, CompressError> {
        let needed_len = lz4_flex::block::get_maximum_output_size(data.len());

        let mut buf = self.get_new_buffer(needed_len);

        // safety: the buffer is only used for writing
        let output = unsafe { Self::write_buf_from_buffer_kind(&mut buf) };

        debug_assert!(
            output.len() >= needed_len,
            "Output buffer is too small ({} < {})",
            output.len(),
            needed_len
        );

        let written = lz4_compress(data, output)?;

        // safety: lz4 guarantees that exactly `size` bytes are written to the output buffer
        unsafe { Self::set_buffer_kind_len(&mut buf, written) };

        Ok(buf)
    }

    fn decompress_lz4(
        &self,
        _data: &[u8],
        _uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError> {
        todo!();
    }
}

impl CompressionHandler for () {
    fn compress_zstd(&self, _data: &[u8], _use_dict: bool) -> Result<BufferKind, CompressError> {
        panic!("null CompressionHandler cannot compress data");
    }

    fn decompress_zstd(
        &self,
        _data: &[u8],
        _uncompressed_size: usize,
        _use_dict: bool,
    ) -> Result<BufferKind, DecompressError> {
        panic!("null CompressionHandler cannot decompress data");
    }

    fn compress_lz4(&self, _data: &[u8]) -> Result<BufferKind, CompressError> {
        panic!("null CompressionHandler cannot compress data");
    }

    fn decompress_lz4(
        &self,
        _data: &[u8],
        _uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError> {
        panic!("null CompressionHandler cannot decompress data");
    }
}
