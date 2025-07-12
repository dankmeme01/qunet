use std::{marker::PhantomData, net::SocketAddr, time::Duration};

use crate::{
    message::QunetMessage,
    server::app_handler::AppHandler,
    transport::{QuicError, QunetTransportData, TransportError},
};
use s2n_quic::{Client, client::Connect, stream::BidirectionalStream};

use super::stream;

pub(crate) struct ClientQuicTransport<H: AppHandler> {
    stream: BidirectionalStream,
    buffer: Vec<u8>,
    buffer_pos: usize,
    _phantom: PhantomData<H>,
}

impl<H: AppHandler> ClientQuicTransport<H> {
    pub fn new(_conn: s2n_quic::Connection, stream: BidirectionalStream) -> Self {
        Self {
            stream,
            buffer: vec![0u8; 512], // TODO: see comment in ClientTcpTransport
            buffer_pos: 0,
            _phantom: PhantomData,
        }
    }

    pub async fn connect(
        addr: SocketAddr,
        hostname: &str,
        timeout: Duration,
    ) -> Result<Self, QuicError> {
        let limits = s2n_quic::provider::limits::Limits::new()
            .with_max_idle_timeout(Duration::from_secs(60))
            .unwrap()
            .with_max_handshake_duration(timeout)
            .unwrap();

        let client = Client::builder()
            .with_limits(limits)
            .unwrap()
            .with_io(if addr.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" })?
            .start()?;

        let connect = Connect::new(addr).with_server_name(hostname);
        let mut connection = client.connect(connect).await?;

        connection.keep_alive(true)?;

        let stream = connection.open_bidirectional_stream().await?;

        Ok(Self::new(connection, stream))
    }

    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        // QUIC transport does not require any setup, just return Ok
        Ok(())
    }

    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        transport_data: &QunetTransportData<H>,
    ) -> Result<QunetMessage, TransportError> {
        stream::receive_message(
            &mut self.buffer,
            &mut self.buffer_pos,
            transport_data,
            &mut self.stream,
        )
        .await
    }

    pub async fn send_message(
        &mut self,
        transport_data: &QunetTransportData<H>,
        msg: QunetMessage,
    ) -> Result<(), TransportError> {
        match tokio::time::timeout(
            Duration::from_secs(30),
            stream::send_message(&mut self.stream, transport_data, &msg),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => Err(TransportError::Timeout),
        }
    }

    pub async fn send_handshake_response(
        &mut self,
        transport_data: &QunetTransportData<H>,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
    ) -> Result<(), TransportError> {
        stream::send_handshake_response(
            &mut self.stream,
            transport_data,
            qdb_data,
            qdb_uncompressed_size,
            "QUIC",
        )
        .await
    }
}
