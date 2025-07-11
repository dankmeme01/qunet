use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::server::{
    Server,
    app_handler::AppHandler,
    message::QunetMessage,
    transport::{ClientTransportData, TransportError},
};

use super::stream;

pub(crate) struct ClientTcpTransport<H: AppHandler> {
    sock_write: OwnedWriteHalf,
    sock_read: OwnedReadHalf,
    buffer: Vec<u8>,
    buffer_pos: usize,
    idle_timeout: Duration,
    last_data_exchange: Instant,
    _phantom: PhantomData<H>,
}

impl<H: AppHandler> ClientTcpTransport<H> {
    pub fn new(socket: TcpStream, server: &Server<H>) -> Self {
        let (sock_read, sock_write) = socket.into_split();

        Self {
            sock_read,
            sock_write,
            buffer: vec![0u8; 512], // TODO: if it's a significant performance issue, don't zero the bytes, also maybe make size configurable
            buffer_pos: 0,
            idle_timeout: server._builder.listener_opts.idle_timeout,
            last_data_exchange: Instant::now(),
            _phantom: PhantomData,
        }
    }

    pub async fn run_setup(&mut self) -> Result<(), TransportError> {
        // TCP transport does not require any setup, just return Ok
        Ok(())
    }

    pub async fn run_cleanup(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    #[inline]
    fn update_exchange_time(&mut self) {
        self.last_data_exchange = Instant::now();
    }

    #[inline]
    pub fn until_timer_expiry(&self) -> Duration {
        self.idle_timeout.saturating_sub(self.last_data_exchange.elapsed())
    }

    #[inline]
    pub fn handle_timer_expiry(
        &self,
        transport_data: &mut ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        if self.last_data_exchange.elapsed() >= self.idle_timeout {
            transport_data.closed = true;
        }

        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
    ) -> Result<QunetMessage, TransportError> {
        let msg = stream::receive_message(
            &mut self.buffer,
            &mut self.buffer_pos,
            transport_data,
            &mut self.sock_read,
        )
        .await?;

        self.update_exchange_time();

        Ok(msg)
    }

    pub async fn send_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
        msg: QunetMessage,
    ) -> Result<(), TransportError> {
        self.update_exchange_time();

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
        transport_data: &ClientTransportData<H>,
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
