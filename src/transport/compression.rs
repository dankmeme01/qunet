use std::cell::RefCell;

use thiserror::Error;
use zstd_safe::{CCtx, CDict, DCtx, DDict};

use crate::protocol::MSG_ZSTD_COMPRESSION_LEVEL;

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
