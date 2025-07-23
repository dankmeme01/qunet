use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tracing::debug;

use crate::{
    message::QunetMessage,
    transport::{QunetTransportData, TransportError},
};

use super::stream;

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

    pub async fn connect(addr: SocketAddr) -> Result<Self, io::Error> {
        let socket = TcpStream::connect(addr).await?;

        Ok(Self::new(socket))
    }

    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        // TCP transport does not require any setup, just return Ok
        Ok(())
    }

    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        let _ = self.sock_write.shutdown().await;
        // TODO: idk how to shutdown read half

        Ok(())
    }
    pub async fn receive_message(
        &mut self,
        transport_data: &mut QunetTransportData,
    ) -> Result<QunetMessage, TransportError> {
        let msg = stream::receive_message(
            &mut self.buffer,
            &mut self.buffer_pos,
            transport_data,
            &mut self.sock_read,
        )
        .await?;

        transport_data.update_exchange_time();

        Ok(msg)
    }

    pub async fn send_message(
        &mut self,
        transport_data: &mut QunetTransportData,
        msg: QunetMessage,
    ) -> Result<(), TransportError> {
        transport_data.update_exchange_time();

        match tokio::time::timeout(
            Duration::from_secs(30),
            stream::send_message(&mut self.sock_write, transport_data, &msg),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => Err(TransportError::Timeout),
        }
    }

    pub async fn send_handshake_response(
        &mut self,
        transport_data: &QunetTransportData,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
    ) -> Result<(), TransportError> {
        stream::send_handshake_response(
            &mut self.sock_write,
            transport_data,
            qdb_data,
            qdb_uncompressed_size,
            "TCP",
        )
        .await
    }
}
