use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use futures_util::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream, accept_async_with_config, tungstenite::protocol::WebSocketConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{
    message::QunetMessage,
    protocol::ProtocolVersion,
    server::{
        ServerHandle,
        app_handler::AppHandler,
        builder::{ListenerOptions, WsOptions},
        listeners::{BindError, ListenerError, ServerListener},
    },
    transport::{QunetTransport, QunetTransportKind, ws::ClientWsTransport},
};

pub(crate) struct WsServerListener<H: AppHandler> {
    opts: WsOptions,
    handshake_timeout: Duration,
    shutdown_token: CancellationToken,
    socket: TcpListener,
    _phantom: PhantomData<H>,
}

impl<H: AppHandler> WsServerListener<H> {
    pub async fn new(
        opts: WsOptions,
        shutdown_token: CancellationToken,
        opts2: &ListenerOptions,
    ) -> Result<Self, BindError> {
        let socket = TcpListener::bind(opts.address).await?;

        Ok(Self {
            opts,
            handshake_timeout: opts2.handshake_timeout,
            shutdown_token,
            socket,
            _phantom: PhantomData,
        })
    }

    pub fn accept_connection(&self, server: ServerHandle<H>, stream: TcpStream, addr: SocketAddr) {
        let timeout = self.handshake_timeout;
        tokio::spawn(async move {
            debug!("Accepted TCP (WS) connection from {addr}, waiting for WS handshake");

            let config = make_config(server.message_size_limit);
            match tokio::time::timeout(timeout, Self::do_accept(server, stream, addr, config)).await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    debug!("Client {addr} (WS) failed to complete handshake: {err}");
                }

                Err(_) => {
                    warn!(
                        "Client {addr} (TCP) did not complete the handshake in time, closing connection"
                    );
                }
            }
        });
    }

    async fn do_accept(
        server: ServerHandle<H>,
        stream: TcpStream,
        addr: SocketAddr,
        config: WebSocketConfig,
    ) -> Result<(), ListenerError> {
        let mut stream = accept_async_with_config(stream, Some(config)).await?;
        let msg = stream.next().await.ok_or(ListenerError::ConnectionClosed)??;

        if !msg.is_binary() {
            warn!("Expected binary message during WS handshake, got: {msg}");
            return Err(ListenerError::MalformedHandshake);
        }

        let data = msg.into_data();
        let meta = QunetMessage::parse_header(&data, false)?;
        let msg = QunetMessage::decode(meta, &*server.buffer_pool)?;

        match msg {
            QunetMessage::HandshakeStart {
                protocol_version,
                frag_limit: _,
                qdb_hash,
            } => Self::do_handle_handshake(server, stream, addr, protocol_version, qdb_hash).await,

            QunetMessage::ClientReconnect { connection_id } => {
                Self::do_handle_reconnect(server, stream, addr, connection_id).await
            }

            _ => {
                warn!("Received unknown message type during WS handshake: {}", msg.type_str());
                Err(ListenerError::MalformedHandshake)
            }
        }
    }

    async fn do_handle_handshake(
        server: ServerHandle<H>,
        stream: WebSocketStream<TcpStream>,
        addr: SocketAddr,
        protocol_version: ProtocolVersion,
        qdb_hash: [u8; 16],
    ) -> Result<(), ListenerError> {
        let transport = QunetTransport::new_server(
            QunetTransportKind::Ws(ClientWsTransport::new(stream)),
            addr,
            protocol_version,
            qdb_hash,
            server.clone(),
        );

        server.accept_connection(transport);

        Ok(())
    }

    async fn do_handle_reconnect(
        server: ServerHandle<H>,
        stream: WebSocketStream<TcpStream>,
        addr: SocketAddr,
        connection_id: u64,
    ) -> Result<(), ListenerError> {
        let transport: QunetTransport = QunetTransport::new_server(
            QunetTransportKind::Ws(ClientWsTransport::new(stream)),
            addr,
            ProtocolVersion::default(),
            [0; 16],
            server.clone(),
        );

        server.recover_connection(connection_id, transport).await;

        Ok(())
    }
}

impl<H: AppHandler> ServerListener<H> for WsServerListener<H> {
    async fn run(
        self: Arc<WsServerListener<H>>,
        server: ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        debug!("Starting WebSocket listener on {}", self.opts.address);

        loop {
            tokio::select! {
                res = self.socket.accept() => match res {
                    Ok((stream, addr)) => self.accept_connection(server.clone(), stream, addr),

                    Err(err) => {
                        // unfortunately, EMFILE and ENFILE errors don't have a specific error kind,
                        // we have to convert to string and check the contents
                        let err_string = err.to_string();

                        if err_string == "Too many open files" {
                            error!("Failed to accept WebSocket connection: Too many open files. Server is unable to accept new connections until the limit is raised. Sleeping for 1 second to prevent log spam.");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        } else {
                            error!("Failed to accept WebSocket connection: {err_string}");
                        }
                    }
                },

                _ = self.shutdown_token.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }

    fn identifier(&self) -> String {
        format!("{} (WS)", self.opts.address)
    }

    fn port(&self) -> u16 {
        self.opts.address.port()
    }
}

fn make_config(message_limit: usize) -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(8 * 1024)
        .max_write_buffer_size(32 * 1024)
        .max_message_size(Some(message_limit))
        .max_frame_size(Some(message_limit))
}
