use std::{io::Cursor, net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use dashmap::{DashMap, DashSet};
use nohash_hasher::BuildNoHashHasher;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, warn};

use crate::{
    buffers::{
        buffer_pool::BufferPool,
        byte_writer::{ByteWriter, ByteWriterError},
    },
    database::{self, QunetDatabase},
    server::{
        app_handler::{AppHandler, DefaultAppHandler},
        builder::ServerBuilder,
        client::{ClientNotification, ClientState},
        listeners::{
            listener::{BindError, ListenerError, ServerListener},
            quic::QuicServerListener,
            tcp::TcpServerListener,
            udp::UdpServerListener,
        },
        message::{
            BufferKind, DataMessageKind, QUNET_SMALL_MESSAGE_SIZE, QunetMessage, QunetRawMessage,
            channel::{RawMessageReceiver, RawMessageSender},
        },
        protocol::{
            DEFAULT_MESSAGE_SIZE_LIMIT, QunetConnectionError, QunetHandshakeError, UDP_PACKET_LIMIT,
        },
        transport::{ClientTransport, TransportError},
    },
};

pub mod app_handler;
pub mod builder;
pub mod client;
pub(crate) mod listeners;
pub mod message;
pub mod protocol;
pub(crate) mod transport;

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
    #[error("Application error: {0}")]
    CustomError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[derive(Debug, Error)]
pub enum AcceptError {
    #[error("Major protocol version mismatch: client using {client}, server using {server}")]
    MajorVersionMismatch { client: u16, server: u16 },
}

pub struct Server<H: AppHandler> {
    _builder: ServerBuilder<H>,
    udp_listener: Option<Arc<UdpServerListener<H>>>,
    tcp_listener: Option<Arc<TcpServerListener<H>>>,
    quic_listener: Option<Arc<QuicServerListener<H>>>,
    buffer_pool: Arc<BufferPool>,
    large_buffer_pool: Arc<BufferPool>,
    app_handler: H,

    shutdown_token: CancellationToken,
    listener_tracker: TaskTracker,

    // TODO: rwlock? dashmap? arcswap?
    udp_router: DashMap<u64, RawMessageSender, BuildNoHashHasher<u64>>,
    clients: DashMap<u64, Arc<ClientState<H>>, BuildNoHashHasher<u64>>,
    connected_addrs: DashSet<SocketAddr>, // this serves to disallow duplicate connections from the same ip:port tuple
    schedules: parking_lot::Mutex<JoinSet<!>>,

    // misc settings
    _message_size_limit: usize,

    // Qdb stuff
    qdb: QunetDatabase,
    qdb_data: Arc<[u8]>,
    qdb_hash: [u8; 32],
    qdb_uncompressed_size: usize,
}

impl Server<DefaultAppHandler> {
    pub fn builder() -> ServerBuilder<DefaultAppHandler> {
        ServerBuilder::<DefaultAppHandler>::default().with_app_handler(DefaultAppHandler)
    }
}

enum ErrorOutcome {
    Terminate,
    GracefulClosure,
    Ignore,
}

impl<H: AppHandler> Server<H> {
    pub(crate) fn from_builder(mut builder: ServerBuilder<H>) -> Self {
        let app_handler = builder
            .app_handler
            .take()
            .expect("App handler must be set in the builder");

        Server {
            _builder: builder,
            udp_listener: None,
            tcp_listener: None,
            quic_listener: None,
            buffer_pool: Arc::new(BufferPool::new(4096, 128, 1024)), // TODO: allow configuring this too
            large_buffer_pool: Arc::new(BufferPool::new(65536, 16, 256)), // TODO: allow configuring this too
            app_handler,

            shutdown_token: CancellationToken::new(),
            listener_tracker: TaskTracker::new(),

            udp_router: DashMap::default(),
            clients: DashMap::default(),
            connected_addrs: DashSet::default(),
            schedules: parking_lot::Mutex::new(JoinSet::new()),
            _message_size_limit: 0,

            qdb: QunetDatabase::default(),
            qdb_data: Arc::new([]),
            qdb_hash: [0; 32],
            qdb_uncompressed_size: 0,
        }
    }

    pub(crate) async fn setup(&mut self) -> Result<(), ServerOutcome> {
        self.print_config();

        self.app_handler
            .pre_setup(self)
            .await
            .map_err(ServerOutcome::CustomError)?;

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

        self.app_handler
            .post_setup(self)
            .await
            .map_err(ServerOutcome::CustomError)?;

        Ok(())
    }

    async fn run_listener<L: ServerListener<H>>(self: ServerHandle<H>, listener: Arc<L>) {
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

    pub async fn run_server(self: ServerHandle<H>) -> ServerOutcome {
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

        // invoke launch hook
        if let Err(o) = self.app_handler.on_launch(self.clone()).await {
            return ServerOutcome::CustomError(o);
        }

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
        // Run pre-shutdown hook
        self.app_handler
            .pre_shutdown(self)
            .await
            .map_err(ServerOutcome::CustomError)?;

        // Cancel all listeners
        self.shutdown_token.cancel();

        // Cancel all schedules
        self.schedules.lock().abort_all();

        // Wait for all listeners to finish
        self.listener_tracker.wait().await;

        // Run post-shutdown hook
        self.app_handler
            .post_shutdown(self)
            .await
            .map_err(ServerOutcome::CustomError)?;

        // Done!

        Ok(())
    }

    pub(crate) async fn accept_connection(
        self: ServerHandle<H>,
        mut transport: ClientTransport<H>,
    ) {
        // spawn a new task for the connection
        tokio::spawn(async move {
            let client_ver = transport.data.qunet_major_version;
            let address = transport.address();

            // send an error to the client if the version is not compatible
            if client_ver != protocol::MAJOR_VERSION {
                self.send_handshake_error(
                    &mut transport,
                    if client_ver < protocol::MAJOR_VERSION {
                        QunetHandshakeError::VersionTooOld
                    } else {
                        QunetHandshakeError::VersionTooNew
                    },
                )
                .await;

                return;
            }

            // early reject duplicate connections from the same IP and port
            if !self.connected_addrs.insert(address) {
                warn!("[{address}] Duplicate connection attempt from same address, rejecting");

                self.send_handshake_error(&mut transport, QunetHandshakeError::DuplicateConnection)
                    .await;

                return;
            }

            let client_qdb_hash = &transport.data.initial_qdb_hash[..];
            let server_qdb_hash = &self.qdb_hash[..16];

            let send_qdb = client_qdb_hash != server_qdb_hash && !self.qdb_data.is_empty();
            let connection_id = self.generate_connection_id();

            transport.set_connection_id(connection_id);

            let client_data = match self
                .app_handler
                .on_client_connect(
                    &self,
                    transport.connection_id(),
                    transport.address(),
                    transport.kind_str(),
                )
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    // The application rejected this connection, so we silently drop it.
                    // cleanup does not need to be run as the transport setup has never been called.
                    debug!("[{address}] Connection rejected due to application error: {e}");

                    // though we still should remove the address from the set
                    self.connected_addrs.remove(&address);
                    return;
                }
            };

            let client = Arc::new(ClientState::<H> {
                app_data: client_data,
                connection_id: transport.connection_id(),
                address,
                notif_tx: transport.notif_chan.0.clone(),
            });

            self.clients.insert(client.connection_id, client.clone());

            if let Err(e) =
                Self::client_handler(&self, &mut transport, client.clone(), send_qdb).await
            {
                warn!(
                    "[{}] Client connection terminated due to error: {}",
                    address, e
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
                let _ = transport
                    .send_message(
                        QunetMessage::ServerClose {
                            error_code,
                            error_message,
                        },
                        false,
                    )
                    .await;
            }

            // always run cleanup!
            match Self::cleanup_connection(&self, &mut transport, client).await {
                Ok(()) => {}
                Err(err) => warn!("[{}] Failed to clean up connection: {}", address, err),
            }
        });
    }

    async fn send_handshake_error(
        &self,
        transport: &mut ClientTransport<H>,
        error: QunetHandshakeError,
    ) {
        assert!(error != QunetHandshakeError::Custom);

        if let Err(e) = transport.send_handshake_error(error, None).await {
            debug!(
                "[{}] Failed to send handshake error: {}",
                transport.address(),
                e
            );
        }
    }

    #[inline]
    pub(crate) async fn dispatch_udp_message(
        &self,
        connection_id: u64,
        msg: QunetRawMessage,
    ) -> bool {
        if let Some(route) = self.udp_router.get(&connection_id) {
            route.send(msg)
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn create_udp_route(&self, connection_id: u64) -> RawMessageReceiver {
        let (tx, rx) = message::channel::new_channel();
        self.udp_router.insert(connection_id, tx);

        rx
    }

    #[inline]
    pub(crate) fn remove_udp_route(&self, connection_id: u64) {
        self.udp_router.remove(&connection_id);
    }

    #[inline]
    fn generate_connection_id(&self) -> u64 {
        // TODO: check if there already is a connection with this ID
        rand::random()
    }

    #[inline]
    fn message_size_limit(&self) -> usize {
        self._message_size_limit
    }

    async fn client_handler(
        &self,
        transport: &mut ClientTransport<H>,
        client: Arc<ClientState<H>>,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
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

        let notif_chan = transport.notif_chan.1.clone();

        while !transport.data.closed {
            tokio::select! {
                msg = transport.receive_message() => match msg {
                    Ok(msg) => {
                        match self.handle_client_message(transport, &client, &msg).await {
                            Ok(()) => {},
                            Err(e) => match self.on_client_error(transport, &e) {
                                ErrorOutcome::Terminate => return Err(e),
                                ErrorOutcome::GracefulClosure => break,
                                ErrorOutcome::Ignore => {}
                            }
                        }
                    },

                    Err(e) => match self.on_client_error(transport, &e) {
                        ErrorOutcome::Terminate => return Err(e),
                        ErrorOutcome::GracefulClosure => break,
                        ErrorOutcome::Ignore => {}
                    }
                },

                notif = notif_chan.recv() => match notif {
                    Some(notif) => match notif {
                        ClientNotification::DataMessage{ buf, reliable } => {
                            match transport.send_message(QunetMessage::Data { kind: DataMessageKind::Regular { data: buf }, reliability: None, compression: None }, reliable).await {
                                Ok(()) => {},
                                Err(e) => match self.on_client_error(transport, &e) {
                                    ErrorOutcome::Terminate => return Err(e),
                                    ErrorOutcome::GracefulClosure => break,
                                    ErrorOutcome::Ignore => {}
                                }
                            }
                        }
                    }

                    None => return Err(TransportError::MessageChannelClosed),
                }
            }
        }

        debug!("[{}] Connection terminated", transport.address());

        Ok(())
    }

    fn on_client_error(
        &self,
        transport: &ClientTransport<H>,
        err: &TransportError,
    ) -> ErrorOutcome {
        match err {
            TransportError::ConnectionClosed => ErrorOutcome::GracefulClosure,
            TransportError::IoError(e) => {
                use std::io::Write;
                // unfortunately we have to write the error to a buffer, i tried `downcast` but it just refused to work with s2n_quic errors

                let mut buf = [0u8; 256];
                let mut cursor = Cursor::new(&mut buf[..]);

                let write_res = write!(cursor, "{e}");
                let cpos = cursor.position();

                if write_res.is_ok() {
                    // check for the error type
                    if let Ok(str) = std::str::from_utf8(&buf[..cpos as usize])
                        && (str.contains("closed on the application level")
                            || str.contains("connection was closed without an error"))
                    {
                        // s2n-quic no-error message, treat it the same as ConnectionClosed
                        return ErrorOutcome::GracefulClosure;
                    }
                }

                // TODO: handle other IO errors

                ErrorOutcome::Terminate
            }

            // Critical errors
            TransportError::MessageChannelClosed
            | TransportError::ZeroLengthMessage
            | TransportError::MessageTooLong => ErrorOutcome::Terminate,

            // Errors that indicate a bug in the server
            TransportError::CompressionError(e) => {
                warn!("[{}] Error compressing message: {}", transport.address(), e);
                ErrorOutcome::Ignore
            }

            // Non-critical errors, just log and continue
            e => {
                debug!("[{}] Error handling message: {}", transport.address(), e);
                ErrorOutcome::Ignore
            }
        }
    }

    #[inline]
    async fn handle_client_message(
        &self,
        transport: &mut ClientTransport<H>,
        client: &ClientState<H>,
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
                    .send_message(
                        QunetMessage::KeepaliveResponse {
                            timestamp: *timestamp,
                            data: None,
                        },
                        false,
                    )
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
                        .send_message(
                            QunetMessage::ConnectionError {
                                error_code: QunetConnectionError::QdbUnavailable,
                            },
                            false,
                        )
                        .await?;

                    return Ok(());
                } else if size > UDP_PACKET_LIMIT {
                    transport
                        .send_message(
                            QunetMessage::ConnectionError {
                                error_code: QunetConnectionError::QdbChunkTooLong,
                            },
                            false,
                        )
                        .await?;

                    return Ok(());
                }

                // check if offset and size are valid
                if offset + size > self.qdb_data.len() || offset >= self.qdb_data.len() {
                    transport
                        .send_message(
                            QunetMessage::ConnectionError {
                                error_code: QunetConnectionError::QdbInvalidChunk,
                            },
                            false,
                        )
                        .await?;

                    return Ok(());
                }

                // send the requested chunk

                transport
                    .send_message(
                        QunetMessage::QdbChunkResponse {
                            offset: offset as u32,
                            size: size as u32,
                            qdb_data: self.qdb_data.clone(),
                        },
                        false,
                    )
                    .await?;
            }

            msg @ QunetMessage::Data { .. } => {
                self.handle_data_message(client, msg).await?;
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

    #[inline]
    async fn handle_data_message(
        &self,
        client: &ClientState<H>,
        msg: &QunetMessage,
    ) -> Result<(), TransportError> {
        let bytes = match msg.data_bytes() {
            Some(x) => x,
            None => unreachable!(),
        };

        self.app_handler.on_client_data(self, client, bytes).await;

        Ok(())
    }

    #[inline]
    async fn cleanup_connection(
        &self,
        transport: &mut ClientTransport<H>,
        client: Arc<ClientState<H>>,
    ) -> Result<(), TransportError> {
        self.app_handler.on_client_disconnect(self, &client).await;

        self.clients.remove(&client.connection_id);
        self.connected_addrs.remove(&client.address);

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

    #[inline]
    pub(crate) fn write_ping_appdata(
        &self,
        writer: &mut ByteWriter,
    ) -> Result<(), ByteWriterError> {
        self.app_handler.on_ping(self, writer)
    }

    // Public API for the application (sending packets, etc.)

    pub async fn schedule<F, Fut>(self: ServerHandle<H>, interval: Duration, mut f: F)
    where
        F: FnMut(&Server<H>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let this = self.clone();

        self.schedules.lock().spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.tick().await; // avoid instant tick

            loop {
                interval.tick().await;
                f(&this).await;
            }
        });
    }

    pub async fn request_buffer(&self, size: usize) -> BufferKind {
        if size <= QUNET_SMALL_MESSAGE_SIZE {
            BufferKind::Small {
                buf: [0; QUNET_SMALL_MESSAGE_SIZE],
                size: 0,
            }
        } else if size <= self.buffer_pool.buf_size() {
            BufferKind::Pooled {
                buf: self.buffer_pool.get().await,
                pos: 0,
                size: 0,
            }
        } else if size <= self.large_buffer_pool.buf_size() {
            BufferKind::Pooled {
                buf: self.large_buffer_pool.get().await,
                pos: 0,
                size: 0,
            }
        } else {
            BufferKind::Heap(Vec::with_capacity(size))
        }
    }
}

pub struct ServerHandle<H: AppHandler> {
    server: Arc<Server<H>>,
}

impl<H: AppHandler> Clone for ServerHandle<H> {
    fn clone(&self) -> Self {
        ServerHandle {
            server: Arc::clone(&self.server),
        }
    }
}

impl<H: AppHandler> ServerHandle<H> {
    pub async fn run(self) -> ServerOutcome {
        Server::run_server(self.clone()).await
    }
}

impl<H: AppHandler> Deref for ServerHandle<H> {
    type Target = Server<H>;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}
