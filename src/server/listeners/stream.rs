// Helper functions shared between stream-like listeners (TCP, QUIC, etc.)

use tokio::io::AsyncReadExt;

use crate::{
    buffers::ByteReader,
    protocol::{HANDSHAKE_START_SIZE, MSG_CLIENT_RECONNECT, MSG_HANDSHAKE_START},
    server::listeners::listener::ListenerError,
};

pub enum StreamFirstPacket {
    HandshakeStart(u16, [u8; 16]),
    ClientReconnect(u64),
}

pub async fn wait_for_handshake<S: AsyncReadExt + Unpin>(
    stream: &mut S,
) -> Result<StreamFirstPacket, ListenerError> {
    let mut buf = [0u8; HANDSHAKE_START_SIZE];
    let mut read_bytes = 0;

    let read = stream.read(&mut buf[read_bytes..]).await?;
    if read == 0 {
        return Err(ListenerError::ConnectionClosed);
    }
    read_bytes += read;

    let is_reconnect = buf[0] == MSG_CLIENT_RECONNECT;
    let message_len = if is_reconnect { 9 } else { HANDSHAKE_START_SIZE };

    while read_bytes < message_len {
        let read = stream.read(&mut buf[read_bytes..]).await?;
        if read == 0 {
            return Err(ListenerError::ConnectionClosed);
        }
        read_bytes += read;
    }

    // .. scuffed part over

    let mut reader = ByteReader::new(&buf[..read_bytes]);

    let msg_type = reader.read_u8()?;
    match msg_type {
        MSG_CLIENT_RECONNECT => {
            let connection_id = reader.read_u64()?;
            Ok(StreamFirstPacket::ClientReconnect(connection_id))
        }

        MSG_HANDSHAKE_START => {
            let qunet_major = reader.read_u16()?;
            let _ = reader.read_u16()?; // we dont use udp frag limit in tcp

            let mut qdb_hash = [0u8; 16];
            reader.read_bytes(&mut qdb_hash)?;
            Ok(StreamFirstPacket::HandshakeStart(qunet_major, qdb_hash))
        }

        _ => Err(ListenerError::MalformedHandshake),
    }
}
