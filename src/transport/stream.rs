// Helper functions shared between stream-like transports (TCP, QUIC, etc.)

use std::io::IoSlice;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;

use crate::{
    buffers::{
        byte_reader::ByteReader, byte_writer::ByteWriter, multi_buffer_pool::MultiBufferPool,
    },
    message::QunetMessage,
    protocol::{HANDSHAKE_HEADER_SIZE_WITH_QDB, MSG_HANDSHAKE_FINISH},
    transport::{QunetTransportData, TransportError},
};

/// Blocks until a full message can be read from the stream, or an error occurs.
/// This function is cancel-safe.
pub async fn receive_message<S: AsyncReadExt + Unpin>(
    buffer: &mut Vec<u8>,
    buffer_pos: &mut usize,
    transport_data: &QunetTransportData,
    stream: &mut S,
) -> Result<QunetMessage, TransportError> {
    // TODO: maybe refactor this to add another position variable,
    // then reset buffer positions to 0 if there is no leftover data
    // this will probably improve performance by avoiding unnecessary memmoves,
    // but will make things more complex if partial messages are received (though this is rare)

    loop {
        // first, try to parse a message from the buffer
        if *buffer_pos >= 4 {
            let length = ByteReader::new(&buffer[..4]).read_u32().unwrap() as usize;

            if length == 0 {
                return Err(TransportError::ZeroLengthMessage);
            } else if length > transport_data.message_size_limit {
                return Err(TransportError::MessageTooLong);
            }

            let total_len = 4 + length;
            if *buffer_pos >= total_len {
                // we have a full message in the buffer
                let data = &buffer[4..total_len];
                // TODO dont early return if parsing failed, still shift the buffer
                let meta = QunetMessage::parse_header(data, false)?;
                let msg = QunetMessage::decode(meta, &transport_data.buffer_pool)?;

                // shift leftover bytes in the buffer
                // TODO: we could elide the memmove by adding another pos field
                let leftover_bytes = *buffer_pos - total_len;
                if leftover_bytes > 0 {
                    buffer.copy_within(total_len..*buffer_pos, 0);
                }

                *buffer_pos = leftover_bytes;

                return Ok(msg);
            }

            // if there's not enough data but we know the length, check if additional space needs to be allocated
            if total_len > buffer.len() {
                buffer.resize(total_len, 0);
            }
        }

        // there's not enough data in the buffer, read from the socket
        let len = stream.read(&mut buffer[*buffer_pos..]).await?;

        if len == 0 {
            return Err(TransportError::ConnectionClosed);
        }

        *buffer_pos += len;
    }
}

/// Sends the handshake response message to the stream.
pub async fn send_handshake_response<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    transport_data: &QunetTransportData,
    qdb_data: Option<&[u8]>,
    qdb_uncompressed_size: usize,
    conn_type: &str,
) -> Result<(), TransportError> {
    let mut header_buf = [0u8; HANDSHAKE_HEADER_SIZE_WITH_QDB + 4];
    let mut header_writer = ByteWriter::new(&mut header_buf);

    // reserve space for the message length
    header_writer.write_u32(0);

    let msg_start = header_writer.pos();

    header_writer.write_u8(MSG_HANDSHAKE_FINISH);
    header_writer.write_u64(transport_data.connection_id);
    header_writer.write_bool(qdb_data.is_some());

    if let Some(qdb_data) = qdb_data {
        debug!("Sending {conn_type} handshake response (QDB {} bytes)", qdb_data.len());

        header_writer.write_u32(qdb_uncompressed_size as u32);
        header_writer.write_u32(qdb_data.len() as u32);

        // unlike udp, bytestream-based protocols handle fragmentation internally, so send it as 1 chunk
        // set offset to 0 and size to the full size
        header_writer.write_u32(0);
        header_writer.write_u32(qdb_data.len() as u32);

        // write the full message length at the start
        let header_len = header_writer.pos() - msg_start;
        let full_len = header_len + qdb_data.len();
        header_writer.perform_at(msg_start - 4, |w| w.write_u32(full_len as u32));

        let mut iovecs = [IoSlice::new(header_writer.written()), IoSlice::new(qdb_data)];

        send_raw_bytes_vectored(stream, &mut iovecs, &transport_data.buffer_pool).await?;
    } else {
        debug!("Sending {conn_type} handshake response (no QDB)");

        // write the full message length at the start
        let header_len = header_writer.pos() - msg_start;
        header_writer.perform_at(msg_start - 4, |w| w.write_u32(header_len as u32));

        send_raw_bytes(stream, header_writer.written()).await?;
    }

    Ok(())
}

/// Sends the given message to the stream.
pub async fn send_message<S: AsyncWriteExt + Unpin>(
    _stream: &mut S,
    transport_data: &QunetTransportData,
    msg: &QunetMessage,
) -> Result<(), TransportError> {
    let mut header_buf = [0u8; 12];
    let mut header_writer = ByteWriter::new(&mut header_buf);

    let write_len = !msg.is_handshake_start();

    // reserve space for the message length
    if write_len {
        header_writer.write_u32(0);
    }

    if !msg.is_data() {
        let mut body_buf = [0u8; 256];
        let mut body_writer = ByteWriter::new(&mut body_buf);

        msg.encode_control_msg(&mut header_writer, &mut body_writer)?;

        if write_len {
            let msg_len = header_writer.pos() + body_writer.pos() - 4; // -4 for the reserved length field
            header_writer.perform_at(0, |wr| wr.write_u32(msg_len as u32));
        }

        let mut vecs = [IoSlice::new(header_writer.written()), IoSlice::new(body_writer.written())];
        send_raw_bytes_vectored(_stream, &mut vecs, &transport_data.buffer_pool).await?;

        return Ok(());
    }

    // handle data messages
    let Some(data) = msg.data_bytes() else { unreachable!() };

    msg.encode_data_header(&mut header_writer, false).unwrap();

    if write_len {
        let msg_len = header_writer.pos() + data.len() - 4; // -4 for the reserved length field
        header_writer.perform_at(0, |wr| wr.write_u32(msg_len as u32));
    }

    let mut vecs = [IoSlice::new(header_writer.written()), IoSlice::new(data)];
    send_raw_bytes_vectored(_stream, &mut vecs, &transport_data.buffer_pool).await?;

    Ok(())
}

pub async fn send_raw_bytes<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    data: &[u8],
) -> Result<(), TransportError> {
    Ok(stream.write_all(data).await?)
}

/// Sends a vector of IoSlices to the stream, returning an error if the write fails.
/// If the underlying stream does not support vectored writes, it will allocate a buffer from the pool
/// if possible, otherwise allocates a temporary buffer.
pub async fn send_raw_bytes_vectored<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    bufs: &mut [IoSlice<'_>],
    buffer_pool: &MultiBufferPool,
) -> Result<(), TransportError> {
    if stream.is_write_vectored() {
        do_vectored_write(stream, bufs).await
    } else {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();

        if total_len < buffer_pool.max_buf_size() {
            let mut buffer = buffer_pool.get(total_len).await.unwrap();
            let mut offset: usize = 0;

            for buf in bufs.iter() {
                buffer[offset..offset + buf.len()].copy_from_slice(buf);
                offset += buf.len();
            }

            debug_assert!(offset == total_len); // sanity check

            send_raw_bytes(stream, &buffer[..offset]).await
        } else {
            let mut buffer = Vec::with_capacity(total_len);

            for buf in bufs.iter() {
                buffer.extend_from_slice(buf);
            }

            debug_assert!(buffer.len() == total_len); // sanity check

            send_raw_bytes(stream, &buffer).await
        }
    }
}

async fn do_vectored_write<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    mut bufs: &mut [IoSlice<'_>],
) -> Result<(), TransportError> {
    // Unfortunately there's no method like write_all_vectored, so we have to make sure all data is written ourselves

    while !bufs.is_empty() {
        let mut written = stream.write_vectored(bufs).await?;

        if written == 0 {
            return Err(TransportError::ConnectionClosed);
        }

        // advance buffers
        let mut i = 0;
        while i < bufs.len() {
            let len = bufs[i].len();

            if written < len {
                // partially written
                bufs[i].advance(written);
                break;
            }

            // fully written
            written -= len;
            i += 1;
        }

        // remove fully written buffers
        bufs = &mut bufs[i..];
    }

    Ok(())
}
