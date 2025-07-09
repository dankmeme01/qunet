use std::{marker::PhantomData, time::Duration};

use crate::server::{
    app_handler::AppHandler,
    message::QunetMessage,
    transport::{ClientTransportData, TransportError},
};
use s2n_quic::stream::BidirectionalStream;

use super::stream;

pub(crate) struct ClientQuicTransport<H: AppHandler> {
    conn: s2n_quic::Connection,
    stream: BidirectionalStream,
    buffer: Vec<u8>,
    buffer_pos: usize,
    _phantom: PhantomData<H>,
}

impl<H: AppHandler> ClientQuicTransport<H> {
    pub fn new(conn: s2n_quic::Connection, stream: BidirectionalStream) -> Self {
        Self {
            conn,
            stream,
            buffer: vec![0u8; 512], // TODO: see comment in ClientTcpTransport
            buffer_pos: 0,
            _phantom: PhantomData,
        }
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
        transport_data: &ClientTransportData<H>,
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
        transport_data: &ClientTransportData<H>,
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
        transport_data: &ClientTransportData<H>,
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
