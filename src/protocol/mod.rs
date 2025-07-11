mod constants;
mod error;

pub use constants::*;
pub use error::{QunetConnectionError, QunetHandshakeError};

pub fn blake3_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}
