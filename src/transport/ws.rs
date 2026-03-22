use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::bytes::BytesMut;
use tracing::{debug, info};

use crate::{
    buffers::ByteWriter,
    message::QunetMessage,
    protocol::{HANDSHAKE_HEADER_SIZE_WITH_QDB, MSG_HANDSHAKE_FINISH},
    transport::{QunetTransportData, TransportError},
};

pub(crate) struct ClientWsTransport {
    stream: WebSocketStream<TcpStream>,
}

impl ClientWsTransport {
    pub fn new(stream: WebSocketStream<TcpStream>) -> Self {
        Self { stream }
    }

    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        transport_data: &QunetTransportData,
    ) -> Result<QunetMessage, TransportError> {
        let addr = self.stream.get_ref().peer_addr()?;

        loop {
            let msg = self.stream.next().await.ok_or(TransportError::ConnectionClosed)??;
            if msg.is_close() {
                info!("WebSocket connection closed by client @ {addr}");
                return Err(TransportError::ConnectionClosed);
            }

            if !msg.is_binary() {
                debug!("ignoring non-binary message from {addr}: {msg:?}",);
                continue; // ignore non-binary messages
            }

            let data = msg.into_data();
            let meta = QunetMessage::parse_header(&data, false)?;
            break Ok(QunetMessage::decode(meta, &*transport_data.buffer_pool)?);
        }
    }

    pub async fn send_message(
        &mut self,
        _transport_data: &QunetTransportData,
        msg: QunetMessage,
    ) -> Result<(), TransportError> {
        let mut header_buf = [0u8; 16];
        let mut header_writer = ByteWriter::new(&mut header_buf);

        if !msg.is_data() {
            let mut body_buf = [0u8; 256];
            let mut body_writer = ByteWriter::new(&mut body_buf);

            msg.encode_control_msg(&mut header_writer, &mut body_writer)?;
            self.send_message_v(header_writer.written(), body_writer.written()).await?;

            return Ok(());
        }

        // handle data messages
        let Some(data) = msg.data_bytes() else { unreachable!() };
        msg.encode_data_header(&mut header_writer, false).unwrap();

        self.send_message_v(header_writer.written(), data).await
    }

    pub async fn send_handshake_response(
        &mut self,
        transport_data: &QunetTransportData,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
    ) -> Result<(), TransportError> {
        let mut header_buf = [0u8; HANDSHAKE_HEADER_SIZE_WITH_QDB + 4];
        let mut header_writer = ByteWriter::new(&mut header_buf);

        header_writer.write_u8(MSG_HANDSHAKE_FINISH);
        header_writer.write_u64(transport_data.connection_id);
        header_writer.write_bool(qdb_data.is_some());

        if let Some(qdb_data) = qdb_data {
            debug!("Sending WS handshake response (QDB {} bytes)", qdb_data.len());

            header_writer.write_u32(qdb_uncompressed_size as u32);
            header_writer.write_u32(qdb_data.len() as u32);

            // send as one chunk, set offset to 0 and size to the full size
            header_writer.write_u32(0);
            header_writer.write_u32(qdb_data.len() as u32);

            self.send_message_v(header_writer.written(), qdb_data).await
        } else {
            debug!("Sending WS handshake response (no QDB)");

            self.send_message_v(header_writer.written(), &[]).await
        }
    }

    async fn send_message_v(&mut self, header: &[u8], body: &[u8]) -> Result<(), TransportError> {
        let mut msg_buf = BytesMut::new();
        msg_buf.reserve(header.len() + body.len());
        msg_buf.extend_from_slice(header);
        msg_buf.extend_from_slice(body);

        match tokio::time::timeout(
            Duration::from_secs(30),
            self.stream.send(Message::Binary(msg_buf.freeze())),
        )
        .await
        {
            Ok(res) => res?,
            Err(_) => return Err(TransportError::Timeout),
        }

        Ok(())
    }
}
