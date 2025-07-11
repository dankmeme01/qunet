// Helper functions shared between stream-like listeners (TCP, QUIC, etc.)

use tokio::io::AsyncReadExt;

use crate::{
    buffers::byte_reader::ByteReader,
    protocol::{HANDSHAKE_START_SIZE, MSG_HANDSHAKE_START},
    server::listeners::listener::ListenerError,
};

pub async fn wait_for_handshake<S: AsyncReadExt + Unpin>(
    stream: &mut S,
) -> Result<(u16, [u8; 16]), ListenerError> {
    let mut buf = [0u8; HANDSHAKE_START_SIZE];
    let mut read_bytes = 0;

    while read_bytes < HANDSHAKE_START_SIZE {
        read_bytes += stream.read(&mut buf[read_bytes..]).await?;
    }

    let mut reader = ByteReader::new(&buf[..read_bytes]);

    let msg_type = reader.read_u8()?;
    let qunet_major = reader.read_u16()?;
    let _ = reader.read_u16()?; // we dont use udp frag limit in tcp

    let mut qdb_hash = [0u8; 16];
    reader.read_bytes(&mut qdb_hash)?;

    if msg_type != MSG_HANDSHAKE_START {
        return Err(ListenerError::MalformedHandshake);
    }

    Ok((qunet_major, qdb_hash))
}
