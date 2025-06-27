use std::io::IoSlice;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tracing::{debug, warn};

use crate::{
    buffers::{byte_reader::ByteReader, byte_writer::ByteWriter},
    server::{
        message::QunetMessage,
        protocol::{HANDSHAKE_HEADER_SIZE_WITH_QDB, MSG_HANDSHAKE_FINISH},
        transport::{ClientTransportData, TransportError},
    },
};

pub(crate) struct ClientTcpTransport {
    sock_write: OwnedWriteHalf,
    sock_read: OwnedReadHalf,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl ClientTcpTransport {
    pub fn new(socket: TcpStream) -> Self {
        let (sock_read, sock_write) = socket.into_split();

        Self {
            sock_read,
            sock_write,
            buffer: vec![0u8; 512], // TODO: if it's a significant performance issue, don't zero the bytes, also maybe make size configurable
            buffer_pos: 0,
        }
    }

    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        // TCP transport does not require any setup, just return Ok
        Ok(())
    }

    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    /// Note: this function MUST be cancel safe.
    pub async fn receive_message(
        &mut self,
        transport_data: &ClientTransportData,
    ) -> Result<QunetMessage, TransportError> {
        loop {
            // first, try to parse a message from the buffer
            if self.buffer_pos >= 4 {
                let length = ByteReader::new(&self.buffer[..4]).read_u32().unwrap() as usize;

                if length == 0 {
                    return Err(TransportError::ZeroLengthMessage);
                } else if length > transport_data.message_size_limit {
                    return Err(TransportError::MessageTooLong);
                }

                let total_len = 4 + length;
                if self.buffer_pos >= total_len {
                    // we have a full message in the buffer
                    let data = &self.buffer[4..total_len];
                    // TODO dont early return if parsing failed, still shift the buffer
                    let meta = QunetMessage::parse_header(data, false)?;
                    let msg = QunetMessage::decode(meta, &transport_data.buffer_pool).await?;

                    // shift leftover bytes in the buffer
                    // TODO: we could elide the memmove by adding another pos field
                    let leftover_bytes = self.buffer_pos - total_len;
                    if leftover_bytes > 0 {
                        self.buffer.copy_within(total_len..self.buffer_pos, 0);
                    }
                    self.buffer_pos = leftover_bytes;

                    return Ok(msg);
                }

                // if there's not enough data but we know the length, check if additional space needs to be allocated
                if total_len > self.buffer.len() {
                    self.buffer.resize(total_len, 0);
                }
            }

            // there's not enough data in the buffer, read from the socket
            let len = self
                .sock_read
                .read(&mut self.buffer[self.buffer_pos..])
                .await?;

            if len == 0 {
                return Err(TransportError::ConnectionClosed);
            }

            self.buffer_pos += len;
        }
    }

    pub async fn send_message(
        &mut self,
        transport_data: &ClientTransportData,
        msg: &QunetMessage,
    ) -> Result<(), TransportError> {
        warn!(
            "[{}] Unimplemented, sending message: {}",
            transport_data.address,
            msg.type_str()
        );

        Ok(())
    }

    pub async fn send_handshake_response(
        &mut self,
        transport_data: &ClientTransportData,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
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
            debug!(
                "Sending TCP handshake response (QDB {} bytes)",
                qdb_data.len()
            );

            header_writer.write_u32(qdb_uncompressed_size as u32);
            header_writer.write_u32(qdb_data.len() as u32);

            // unlike udp, tcp handles fragmentation internally, so send it as 1 chunk
            // set offset to 0 and size to the full size
            header_writer.write_u32(0);
            header_writer.write_u32(qdb_data.len() as u32);

            // write the full message length at the start
            let header_len = header_writer.pos() - msg_start;
            let full_len = header_len + qdb_data.len();
            header_writer.perform_at(msg_start - 4, |w| w.write_u32(full_len as u32));

            let mut iovecs = [
                IoSlice::new(header_writer.written()),
                IoSlice::new(qdb_data),
            ];

            self.send_packet_vectored(&mut iovecs).await?;
        } else {
            debug!("Sending TCP handshake response (no QDB)");

            // write the full message length at the start
            let header_len = header_writer.pos() - msg_start;
            header_writer.perform_at(msg_start - 4, |w| w.write_u32(header_len as u32));

            self.send_packet(header_writer.written()).await?;
        }

        Ok(())
    }

    async fn send_packet(&mut self, data: &[u8]) -> Result<(), TransportError> {
        Ok(self.sock_write.write_all(data).await?)
    }

    async fn send_packet_vectored(
        &mut self,
        mut bufs: &mut [IoSlice<'_>],
    ) -> Result<(), TransportError> {
        // Unfortunately there's no method like write_all_vectored, so we have to make sure all data is written ourselves
        while !bufs.is_empty() {
            let mut written = self.sock_write.write_vectored(bufs).await?;

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
}
