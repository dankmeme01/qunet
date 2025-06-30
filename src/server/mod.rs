use std::{ops::Deref, sync::Arc, time::Duration};

use nohash_hasher::IntMap;
use parking_lot::Mutex;
use thiserror::Error;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, warn};

use crate::{
    buffers::{
        buffer_pool::BufferPool,
        byte_writer::{ByteWriter, ByteWriterError},
    },
    database::{self, QunetDatabase},
    server::{
        builder::ServerBuilder,
        listeners::{
            listener::{BindError, ListenerError, ServerListener},
            quic::QuicServerListener,
            tcp::TcpServerListener,
            udp::UdpServerListener,
        },
        message::{
            QunetMessage, QunetRawMessage,
            channel::{RawMessageReceiver, RawMessageSender},
        },
        protocol::{
            DEFAULT_MESSAGE_SIZE_LIMIT, QunetConnectionError, QunetHandshakeError, UDP_PACKET_LIMIT,
        },
        transport::{ClientTransport, TransportError},
    },
};

pub mod builder;
pub mod listeners;
pub mod message;
pub mod protocol;
pub mod transport;

#[derive(Error, Debug)]
pub enum ServerOutcome {
    #[error("Server shut down gracefully")]
    GracefulShutdown,
    #[error("Server failed to cleanup during shutdown: {0}")]
    CleanupFailure(Box<ServerOutcome>),
    #[error("Server took too long to shut down gracefully")]
    ShutdownTimeout,
    #[error("Failed to bind endpoint: {0}")]
    BindFailure(#[from] BindError),
    #[error("Failed to read QDB file: {0}")]
    QdbReadError(std::io::Error),
    #[error("Failed to initialize QDB: {0}")]
    QdbInitError(#[from] database::DecodeError),
    #[error("Failed to compress QDB data: {0}")]
    QdbCompressError(std::io::Error),
    #[error("Listener unexpectedly shutdown: {0}")]
    ListenerShutdown(ListenerError),
    #[error("All listeners terminated unsuccessfully")]
    AllListenersTerminated,
    #[error("Listener panicked: {0}")]
    ListenerPanic(#[from] tokio::task::JoinError),
}

#[derive(Debug, Error)]
pub enum AcceptError {
    #[error("Major protocol version mismatch: client using {client}, server using {server}")]
    MajorVersionMismatch { client: u16, server: u16 },
}

pub struct Server {
    _builder: ServerBuilder,
    udp_listener: Option<Arc<UdpServerListener>>,
    tcp_listener: Option<Arc<TcpServerListener>>,
    quic_listener: Option<Arc<QuicServerListener>>,
    buffer_pool: Arc<BufferPool>,
    large_buffer_pool: Arc<BufferPool>,

    shutdown_token: CancellationToken,
    listener_tracker: TaskTracker,

    // TODO: rwlock? dashmap? arcswap?
    udp_router: Mutex<IntMap<u64, RawMessageSender>>,

    // misc settings
    _message_size_limit: usize,

    // Qdb stuff
    qdb: QunetDatabase,
    qdb_data: Arc<[u8]>,
    qdb_hash: [u8; 32],
    qdb_uncompressed_size: usize,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    pub(crate) fn from_builder(builder: ServerBuilder) -> Self {
        Server {
            _builder: builder,
            udp_listener: None,
            tcp_listener: None,
            quic_listener: None,
            buffer_pool: Arc::new(BufferPool::new(4096, 128, 1024)), // TODO: allow configuring this too
            large_buffer_pool: Arc::new(BufferPool::new(65536, 16, 256)), // TODO: allow configuring this too

            shutdown_token: CancellationToken::new(),
            listener_tracker: TaskTracker::new(),

            udp_router: Mutex::new(IntMap::default()),
            _message_size_limit: 0,

            qdb: QunetDatabase::default(),
            qdb_data: Arc::new([]),
            qdb_hash: [0; 32],
            qdb_uncompressed_size: 0,
        }
    }

    pub async fn setup(&mut self) -> Result<(), ServerOutcome> {
        // Setup connection listeners
        if let Some(opts) = self._builder.udp_opts.take() {
            let listener = UdpServerListener::new(opts, self.shutdown_token.clone()).await?;
            self.udp_listener = Some(Arc::new(listener));
        }

        if let Some(opts) = self._builder.tcp_opts.take() {
            let listener = TcpServerListener::new(opts, self.shutdown_token.clone()).await?;
            self.tcp_listener = Some(Arc::new(listener));
        }

        if let Some(opts) = self._builder.quic_opts.take() {
            let listener = QuicServerListener::new(
                opts,
                self._builder.listener_opts.clone(),
                self.shutdown_token.clone(),
            )
            .await?;
            self.quic_listener = Some(Arc::new(listener));
        }

        // Setup misc settings
        self._message_size_limit = self
            ._builder
            .message_size_limit
            .unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT);

        // Setup qdb stuff
        let qdb_data = if let Some(data) = self._builder.qdb_data.take() {
            data.into_boxed_slice()
        } else if let Some(path) = self._builder.qdb_path.take() {
            std::fs::read(path)
                .map_err(ServerOutcome::QdbReadError)?
                .into_boxed_slice()
        } else {
            Box::new([])
        };

        if qdb_data.is_empty() {
            warn!("No QDB file provided, data packets will not be functional");
        } else {
            self.qdb_hash = protocol::blake3_hash(&qdb_data);
            self.qdb = QunetDatabase::decode(&mut &*qdb_data)?;
            self.qdb_uncompressed_size = qdb_data.len();

            // the actual qdb_data will be stored with zstd compression
            let mut destination = Vec::new();

            // TODO: tweak this level in the future
            zstd::stream::copy_encode(&mut &*qdb_data, &mut destination, 12)
                .map_err(ServerOutcome::QdbCompressError)?;

            debug!(
                "Loaded QDB ({} bytes raw, {} compressed), hash: {}",
                self.qdb_uncompressed_size,
                destination.len(),
                hex::encode(self.qdb_hash.as_ref())
            );
            self.qdb_data = destination.into();
        }

        Ok(())
    }

    async fn run_listener<L: ServerListener>(self: ServerHandle, listener: Arc<L>) {
        match listener.clone().run(self).await {
            Ok(()) => info!(
                "Listener {} terminated with no errors",
                listener.identifier()
            ),

            Err(e) => {
                error!(
                    "Listener {} terminated with error: {}",
                    listener.identifier(),
                    e
                );
            }
        }
    }

    pub async fn run_server(self: ServerHandle) -> ServerOutcome {
        self.print_config();

        if let Some(udp) = self.udp_listener.clone() {
            let srv = self.clone();
            self.listener_tracker
                .spawn(async move { srv.run_listener(udp).await });
        }

        if let Some(tcp) = self.tcp_listener.clone() {
            let srv = self.clone();
            self.listener_tracker
                .spawn(async move { srv.run_listener(tcp).await });
        }

        if let Some(quic) = self.quic_listener.clone() {
            let srv = self.clone();
            self.listener_tracker
                .spawn(async move { srv.run_listener(quic).await });
        }

        self.listener_tracker.close();

        tokio::select! {
            _ = self.listener_tracker.wait() => {
                error!("All listeners have unexpectedly terminated, shutting down!");
                return ServerOutcome::AllListenersTerminated;
            },

            _ = tokio::signal::ctrl_c() => {
                info!("Interrupt received, trying to shut down gracefully");
            }
        };

        let timeout = self
            ._builder
            .graceful_shutdown_timeout
            .unwrap_or(Duration::from_secs(5));

        match tokio::time::timeout(timeout, self.graceful_shutdown()).await {
            Ok(Ok(())) => info!("Shutdown successful"),
            Ok(Err(e)) => {
                error!("Failed to shut down gracefully: {e}");
                return ServerOutcome::CleanupFailure(e.into());
            }

            Err(_) => {
                warn!("Failed to shut down gracefully in the given time period, aborting");

                return ServerOutcome::ShutdownTimeout;
            }
        }

        ServerOutcome::GracefulShutdown
    }

    async fn graceful_shutdown(&self) -> Result<(), ServerOutcome> {
        self.shutdown_token.cancel();

        self.listener_tracker.wait().await;

        Ok(())
    }

    pub(crate) async fn accept_connection(
        self: ServerHandle,
        mut transport: ClientTransport,
    ) -> Result<(), AcceptError> {
        let client_ver = transport.data.qunet_major_version;

        if client_ver != protocol::MAJOR_VERSION {
            // also send an error to the client
            tokio::spawn(async move {
                let res = transport
                    .send_message(QunetMessage::HandshakeFailure {
                        error_code: if client_ver < protocol::MAJOR_VERSION {
                            QunetHandshakeError::VersionTooOld
                        } else {
                            QunetHandshakeError::VersionTooNew
                        },
                        reason: None,
                    })
                    .await;

                if let Err(e) = res {
                    warn!(
                        "[{}] Failed to send handshake failure: {}",
                        transport.address(),
                        e
                    );
                }
            });

            return Err(AcceptError::MajorVersionMismatch {
                client: client_ver,
                server: protocol::MAJOR_VERSION,
            });
        }

        let client_qdb_hash = &transport.data.initial_qdb_hash[..];
        let server_qdb_hash = &self.qdb_hash[..16];

        let send_qdb = client_qdb_hash != server_qdb_hash && !self.qdb_data.is_empty();
        let connection_id = self.generate_connection_id();

        transport.set_connection_id(connection_id);

        tokio::spawn(async move {
            let addr = transport.address();

            if let Err(e) = Self::client_handler(&self, &mut transport, send_qdb).await {
                warn!(
                    "[{}] Client connection terminated due to error: {}",
                    addr, e
                );

                // depending on the error, we might want to send a message to the client notifying about it
                let (error_code, error_message) = match e {
                    TransportError::MessageTooLong => {
                        (QunetConnectionError::StreamMessageTooLong, None)
                    }

                    TransportError::ZeroLengthMessage => {
                        (QunetConnectionError::ZeroLengthStreamMessage, None)
                    }

                    TransportError::MessageChannelClosed => {
                        (QunetConnectionError::InternalServerError, None)
                    }

                    _ => (QunetConnectionError::Custom, Some(e.to_string())),
                };

                // we don't care if it fails here
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    transport.send_message(QunetMessage::ServerClose {
                        error_code,
                        error_message,
                    }),
                )
                .await;
            }

            // always run cleanup!
            match Self::cleanup_connection(&self, &mut transport).await {
                Ok(()) => {}
                Err(err) => warn!("[{}] Failed to clean up connection: {}", addr, err),
            }
        });

        Ok(())
    }

    pub(crate) async fn dispatch_udp_message(
        &self,
        connection_id: u64,
        msg: QunetRawMessage,
    ) -> bool {
        if let Some(route) = self.udp_router.lock().get(&connection_id) {
            route.send(msg)
        } else {
            false
        }
    }

    pub(crate) fn create_udp_route(&self, connection_id: u64) -> RawMessageReceiver {
        let (tx, rx) = message::channel::new_channel();
        self.udp_router.lock().insert(connection_id, tx);

        rx
    }

    pub(crate) fn remove_udp_route(&self, connection_id: u64) {
        self.udp_router.lock().remove(&connection_id);
    }

    fn generate_connection_id(&self) -> u64 {
        // TODO: check if there already is a connection with this ID
        rand::random()
    }

    fn message_size_limit(&self) -> usize {
        self._message_size_limit
    }

    async fn client_handler(
        &self,
        transport: &mut ClientTransport,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
        info!(
            "[{}] New connection ({}{})",
            transport.address(),
            transport.kind_str(),
            if send_qdb { ", outdated QDB" } else { "" }
        );

        // run setup (udp needs this)
        transport.run_setup(self).await?;

        // send the handshake response
        transport
            .send_handshake_response(
                if send_qdb {
                    Some(self.qdb_data.as_ref())
                } else {
                    None
                },
                self.qdb_uncompressed_size,
            )
            .await?;

        while !transport.data.closed {
            let msg = match transport.receive_message().await {
                Ok(msg) => msg,

                Err(TransportError::ConnectionClosed) => break,

                // Critical errors
                Err(e @ TransportError::IoError(_))
                | Err(e @ TransportError::MessageChannelClosed)
                | Err(e @ TransportError::ZeroLengthMessage)
                | Err(e @ TransportError::MessageTooLong) => return Err(e),

                // Non-critical errors, just log and continue
                Err(e) => {
                    debug!("[{}] Error receiving message: {}", transport.address(), e);

                    continue;
                }
            };

            self.handle_client_message(transport, &msg).await?;
        }

        debug!("[{}] Connection terminated", transport.address());

        Ok(())
    }

    async fn handle_client_message(
        &self,
        transport: &mut ClientTransport,
        msg: &QunetMessage,
    ) -> Result<(), TransportError> {
        #[cfg(debug_assertions)]
        debug!(
            "[{}] Received message: {:?}",
            transport.address(),
            msg.type_str()
        );

        match msg {
            QunetMessage::Keepalive { timestamp } => {
                // TODO: custom data
                transport
                    .send_message(QunetMessage::KeepaliveResponse {
                        timestamp: *timestamp,
                        data: None,
                    })
                    .await?;
            }

            QunetMessage::ClientClose { dont_terminate } => {
                // TODO: handle dont_terminate flag
                transport.data.closed = true;
            }

            QunetMessage::ConnectionError { error_code } => {
                warn!(
                    "[{}] Client reported connection error: {:?}",
                    transport.address(),
                    error_code
                );
            }

            QunetMessage::QdbChunkRequest { offset, size } => {
                let offset = *offset as usize;
                let size = *size as usize;

                if self.qdb_data.is_empty() {
                    transport
                        .send_message(QunetMessage::ConnectionError {
                            error_code: QunetConnectionError::QdbUnavailable,
                        })
                        .await?;

                    return Ok(());
                } else if size > UDP_PACKET_LIMIT {
                    transport
                        .send_message(QunetMessage::ConnectionError {
                            error_code: QunetConnectionError::QdbChunkTooLong,
                        })
                        .await?;

                    return Ok(());
                }

                // check if offset and size are valid
                if offset + size > self.qdb_data.len() || offset >= self.qdb_data.len() {
                    transport
                        .send_message(QunetMessage::ConnectionError {
                            error_code: QunetConnectionError::QdbInvalidChunk,
                        })
                        .await?;

                    return Ok(());
                }

                // send the requested chunk

                transport
                    .send_message(QunetMessage::QdbChunkResponse {
                        offset: offset as u32,
                        size: size as u32,
                        qdb_data: self.qdb_data.clone(),
                    })
                    .await?;
            }

            msg @ QunetMessage::Data { .. } => {
                self.handle_data_message(transport, msg).await?;
            }

            _ => {
                debug!(
                    "[{}] unable to handle message! is this a server-only message?",
                    transport.address()
                );
            }
        }

        Ok(())
    }

    async fn handle_data_message(
        &self,
        _transport: &mut ClientTransport,
        _msg: &QunetMessage,
    ) -> Result<(), TransportError> {
        // TODO!
        Ok(())
    }

    #[inline]
    async fn cleanup_connection(
        &self,
        transport: &mut ClientTransport,
    ) -> Result<(), TransportError> {
        transport.run_cleanup(self).await
    }

    fn print_config(&self) {
        info!("Server configuration:");

        if let Some(udp) = &self._builder.udp_opts {
            info!(
                "- {} (UDP, {} binds, discovery mode: {:?})",
                udp.address, udp.binds, udp.discovery_mode
            );
        }

        if let Some(tcp) = &self._builder.tcp_opts {
            info!("- {} (TCP)", tcp.address);
        }

        if let Some(quic) = &self._builder.quic_opts {
            info!("- {} (QUIC)", quic.address);
        }

        if let Some(ws) = &self._builder.ws_opts {
            info!("- {} (WebSocket)", ws.address);
        }
    }

    pub fn write_ping_appdata(&self, writer: &mut ByteWriter) -> Result<(), ByteWriterError> {
        // right now placeholder, later let the application write custom data
        writer.try_write_bytes(b"hi")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerHandle {
    server: Arc<Server>,
}

impl ServerHandle {
    pub async fn run(self) -> ServerOutcome {
        Server::run_server(self.clone()).await
    }
}

impl Deref for ServerHandle {
    type Target = Server;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}
