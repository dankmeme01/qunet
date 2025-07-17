use std::{
    io::Cursor,
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, Weak},
    time::Duration,
};

use dashmap::DashMap;
use nohash_hasher::BuildNoHashHasher;
use thiserror::Error;
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, trace, warn};

use crate::{
    buffers::{
        buffer_pool::BufferPool,
        byte_writer::{ByteWriter, ByteWriterError},
        multi_buffer_pool::MultiBufferPool,
    },
    database::{self, QunetDatabase},
    message::{
        self, BufferKind, MsgData, QUNET_SMALL_MESSAGE_SIZE, QunetMessage, QunetRawMessage,
        channel::{RawMessageReceiver, RawMessageSender},
    },
    protocol::{self, *},
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
    },
    transport::{
        QunetTransport, TransportError, TransportErrorOutcome, TransportType,
        compression::{CompressError, CompressionHandler, CompressionHandlerImpl, DecompressError},
    },
};

pub mod app_handler;
pub mod builder;
pub mod client;
pub(crate) mod listeners;

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
    QdbCompressError(&'static str),
    #[error("Listener unexpectedly shutdown: {0}")]
    ListenerShutdown(ListenerError),
    #[error("All listeners terminated unsuccessfully")]
    AllListenersTerminated,
    #[error("No listeners were enabled, server cannot run")]
    NoListenersEnabled,
    #[error("Application error: {0}")]
    CustomError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

#[derive(Debug, Error)]
pub enum AcceptError {
    #[error("Major protocol version mismatch: client using {client}, server using {server}")]
    MajorVersionMismatch {
        client: u16,
        server: u16,
    },
}

pub struct Server<H: AppHandler> {
    pub(crate) _builder: ServerBuilder<H>,
    udp_listener: Option<Arc<UdpServerListener<H>>>,
    tcp_listener: Option<Arc<TcpServerListener<H>>>,
    quic_listener: Option<Arc<QuicServerListener<H>>>,
    pub(crate) buffer_pool: Arc<MultiBufferPool>,
    pub(crate) app_handler: H,

    shutdown_token: CancellationToken,
    listener_tracker: TaskTracker,
    conn_tracker: TaskTracker,
    terminate_notify: Notify,

    udp_router: DashMap<u64, RawMessageSender, BuildNoHashHasher<u64>>,
    clients: DashMap<u64, Arc<ClientState<H>>, BuildNoHashHasher<u64>>,
    connected_addrs: DashMap<SocketAddr, u64>, // this serves to detect duplicate connections from the same ip:port tuple
    schedules: AsyncMutex<JoinSet<!>>,
    compressor: CompressionHandlerImpl,

    // misc settings
    message_size_limit: usize,

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

impl<H: AppHandler> Server<H> {
    pub(crate) fn from_builder(mut builder: ServerBuilder<H>) -> Self {
        let app_handler =
            builder.app_handler.take().expect("App handler must be set in the builder");

        // setup buffer pool, while it's unique
        let mut buffer_pool = MultiBufferPool::new();

        for opts in &builder.mem_options.buffer_pools {
            buffer_pool.add_pool(BufferPool::new(
                opts.buf_size,
                opts.initial_buffers,
                opts.max_buffers,
            ));
        }

        let buffer_pool = Arc::new(buffer_pool);
        let compressor = CompressionHandlerImpl::new(buffer_pool.clone());

        Server {
            _builder: builder,
            udp_listener: None,
            tcp_listener: None,
            quic_listener: None,
            buffer_pool,
            app_handler,

            shutdown_token: CancellationToken::new(),
            listener_tracker: TaskTracker::new(),
            conn_tracker: TaskTracker::new(),
            terminate_notify: Notify::new(),

            udp_router: DashMap::default(),
            clients: DashMap::default(),
            connected_addrs: DashMap::default(),
            schedules: AsyncMutex::new(JoinSet::new()),
            compressor,
            message_size_limit: 0,

            qdb: QunetDatabase::default(),
            qdb_data: Arc::new([]),
            qdb_hash: [0; 32],
            qdb_uncompressed_size: 0,
        }
    }

    pub(crate) async fn setup(&mut self) -> Result<(), ServerOutcome> {
        self.app_handler.pre_setup(self).await.map_err(ServerOutcome::CustomError)?;

        self.print_config();

        // Setup connection listeners
        if let Some(opts) = self._builder.udp_opts.take() {
            let listener = UdpServerListener::new(
                opts,
                self.shutdown_token.clone(),
                &self._builder.listener_opts,
                &self._builder.mem_options,
            )
            .await?;

            self.udp_listener = Some(Arc::new(listener));
        }

        if let Some(opts) = self._builder.tcp_opts.take() {
            let listener = TcpServerListener::new(
                opts,
                self.shutdown_token.clone(),
                &self._builder.listener_opts,
            )
            .await?;
            self.tcp_listener = Some(Arc::new(listener));
        }

        if let Some(opts) = self._builder.quic_opts.take() {
            let listener = QuicServerListener::new(
                opts,
                self.shutdown_token.clone(),
                &self._builder.listener_opts,
            )
            .await?;

            self.quic_listener = Some(Arc::new(listener));
        }

        // Setup misc settings
        self.message_size_limit =
            self._builder.message_size_limit.unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT);

        // Setup qdb stuff
        let qdb_data = if let Some(data) = self._builder.qdb_data.take() {
            data.into_boxed_slice()
        } else if let Some(path) = self._builder.qdb_path.take() {
            std::fs::read(path).map_err(ServerOutcome::QdbReadError)?.into_boxed_slice()
        } else {
            Box::new([])
        };

        if !qdb_data.is_empty() {
            self.qdb_hash = protocol::blake3_hash(&qdb_data);
            self.qdb = QunetDatabase::decode(&mut &*qdb_data)?;
            self.qdb_uncompressed_size = qdb_data.len();

            // the actual qdb_data will be stored with zstd compression
            let mut destination = Vec::with_capacity(zstd_safe::compress_bound(qdb_data.len()));

            zstd_safe::compress(&mut destination, &qdb_data, QDB_ZSTD_COMPRESSION_LEVEL)
                .map_err(|code| ServerOutcome::QdbCompressError(zstd_safe::get_error_name(code)))?;

            // create compression and decompression dictionaries
            if let Some(dict) = self.qdb.zstd_dict.as_ref() {
                self.compressor.init_zstd_cdict(dict, self.qdb.zstd_level);
                self.compressor.init_zstd_ddict(dict);
            }

            debug!(
                "Loaded QDB ({} bytes raw, {} compressed), hash: {}",
                self.qdb_uncompressed_size,
                destination.len(),
                hex::encode(self.qdb_hash.as_ref())
            );

            self.qdb_data = destination.into();
        }

        self.app_handler.post_setup(self).await.map_err(ServerOutcome::CustomError)?;

        Ok(())
    }

    async fn run_listener<L: ServerListener<H>>(self: ServerHandle<H>, listener: Arc<L>) {
        match listener.clone().run(self).await {
            Ok(()) => info!("Listener {} terminated with no errors", listener.identifier()),

            Err(e) => {
                error!("Listener {} terminated with error: {}", listener.identifier(), e);
            }
        }
    }

    pub async fn run_server(self: ServerHandle<H>) -> ServerOutcome {
        if let Some(udp) = self.udp_listener.clone() {
            let srv = self.clone();
            self.listener_tracker.spawn(async move { srv.run_listener(udp).await });
        }

        if let Some(tcp) = self.tcp_listener.clone() {
            let srv = self.clone();
            self.listener_tracker.spawn(async move { srv.run_listener(tcp).await });
        }

        if let Some(quic) = self.quic_listener.clone() {
            let srv = self.clone();
            self.listener_tracker.spawn(async move { srv.run_listener(quic).await });
        }

        self.listener_tracker.close();

        if self.listener_tracker.is_empty() {
            error!(
                "No listeners were started, shutting down! Please turn on at least one of the listeners (UDP, TCP, QUIC)"
            );
            return ServerOutcome::NoListenersEnabled;
        }

        // invoke launch hook
        if let Err(o) = self.app_handler.on_launch(self.clone()).await {
            return ServerOutcome::CustomError(o);
        }

        tokio::select! {
            _ = self.listener_tracker.wait() => {
                error!("All listeners have unexpectedly terminated, shutting down!");
                return ServerOutcome::AllListenersTerminated;
            },

            _ = self.terminate_notify.notified() => {
                info!("Server termination requested, trying to shut down gracefully");
            },

            _ = tokio::signal::ctrl_c() => {
                info!("Interrupt received, trying to shut down gracefully");
            }
        };

        let timeout = self._builder.graceful_shutdown_timeout.unwrap_or(Duration::from_secs(10));

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
        trace!("running pre-shutdown hook");
        self.app_handler.pre_shutdown(self).await.map_err(ServerOutcome::CustomError)?;

        trace!("cancelling all listeners and active connections");
        self.shutdown_token.cancel();
        self.conn_tracker.close();

        trace!("aborting all schedules");
        let mut schedules = self.schedules.lock().await;
        schedules.abort_all();

        while !schedules.is_empty() {
            schedules.join_next().await;
        }

        trace!("waiting for all listeners to terminate");
        self.listener_tracker.wait().await;
        trace!("waiting for all connections to terminate");
        self.conn_tracker.wait().await;

        trace!("running post-shutdown hook");
        self.app_handler.post_shutdown(self).await.map_err(ServerOutcome::CustomError)?;

        trace!("graceful shutdown complete!");

        Ok(())
    }

    pub(crate) async fn accept_connection(self: ServerHandle<H>, mut transport: QunetTransport) {
        // spawn a new task for the connection
        self.clone().conn_tracker.spawn(async move {
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

            // early reject duplicate connections from the same IP and port,
            // unless it might be a wrongfully retransmitted UDP handshake
            let existing_conn_id = self.connected_addrs.get(&address).map(|x| *x);

            if let Some(id) = existing_conn_id {
                // if this client sent a duplicate handshake packet, it might be because our initial handshake response was lost,
                // so retransmit it to the client.

                if let Some(client) = self.clients.get(&id)
                    && client.transport_type() == TransportType::Udp
                {
                    debug!("[{address}] Received duplicate handshake, retransmitting response");
                    client.retransmit_handshake();
                } else {
                    warn!("[{address}] Duplicate connection attempt from same address, rejecting");

                    self.send_handshake_error(
                        &mut transport,
                        QunetHandshakeError::DuplicateConnection,
                    )
                    .await;
                }

                return;
            }

            let connection_id = self.generate_connection_id();
            transport.set_connection_id(connection_id);

            self.connected_addrs.insert(address, connection_id);

            let client_qdb_hash = &transport.data.initial_qdb_hash[..];
            let server_qdb_hash = &self.qdb_hash[..16];

            let send_qdb = client_qdb_hash != server_qdb_hash && !self.qdb_data.is_empty();

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

            let client = Arc::new(ClientState::new(client_data, &transport));

            self.clients.insert(client.connection_id, Arc::clone(&client));

            if let Err(e) =
                Self::client_handler(&self, &mut transport, Arc::clone(&client), send_qdb).await
            {
                warn!("[{}] Client connection terminated due to error: {}", address, e);

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
                        QunetMessage::ServerClose { error_code, error_message },
                        false,
                        &*self,
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
        transport: &mut QunetTransport,
        error: QunetHandshakeError,
    ) {
        assert!(error != QunetHandshakeError::Custom);

        if let Err(e) = transport.send_handshake_error(error, None, self).await {
            debug!("[{}] Failed to send handshake error: {}", transport.address(), e);
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
        let (tx, rx) = message::channel::new_channel(16);
        self.udp_router.insert(connection_id, tx);

        rx
    }

    #[inline]
    pub(crate) fn remove_udp_route(&self, connection_id: u64) {
        self.udp_router.remove(&connection_id);
    }

    #[inline]
    fn generate_connection_id(&self) -> u64 {
        loop {
            let id = rand::random();

            if !self.clients.contains_key(&id) {
                break id;
            }
        }
    }

    #[inline]
    pub fn message_size_limit(&self) -> usize {
        self.message_size_limit
    }

    async fn client_handler(
        &self,
        transport: &mut QunetTransport,
        client: Arc<ClientState<H>>,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
        // run setup (udp needs this)
        transport.run_server_setup(self).await?;

        // send the handshake response
        transport
            .send_handshake_response(
                if send_qdb { Some(self.qdb_data.as_ref()) } else { None },
                self.qdb_uncompressed_size,
            )
            .await?;

        let notif_chan = transport.notif_chan.1.clone();

        let mut handshake_retx_count: u8 = 0;

        while !transport.data.closed {
            let timer_expiry = transport.until_timer_expiry();

            let res = tokio::select! {
                msg = transport.receive_message() => match msg {
                    Ok(msg) => {
                        match transport.decompress_message(msg, self).await {
                            Ok(mut msg) => self.handle_client_message(transport, &client, &mut msg).await,
                            Err(e) => Err(e),
                        }
                    },

                    Err(e) => Err(e),
                },

                _ = tokio::time::sleep(timer_expiry.unwrap()), if timer_expiry.is_some() => {
                    transport.handle_timer_expiry().await
                },

                _ = self.shutdown_token.cancelled() => {
                    // server has been shut down, notify and close the connection
                    let _ = tokio::time::timeout(Duration::from_secs(1),
                        transport.send_message(
                            QunetMessage::ServerClose { error_code: QunetConnectionError::ServerShutdown, error_message: None },
                        false, self)
                    ).await;

                    break;
                },

                notif = notif_chan.recv() => match notif {
                    Some(notif) => match notif {
                        ClientNotification::DataMessage{ buf, reliable } => {
                            transport.send_message(QunetMessage::new_data(buf), reliable, self).await
                        }

                        ClientNotification::RetransmitHandshake => {
                            handshake_retx_count += 1;

                            // prevent malicious clients from spamming handshake retransmits, silently drop connection
                            if handshake_retx_count > 5 {
                                return Ok(());
                            }

                            transport
                                .send_handshake_response(
                                    if send_qdb {
                                        Some(self.qdb_data.as_ref())
                                    } else {
                                        None
                                    },
                                    self.qdb_uncompressed_size,
                                )
                                .await
                        }
                    }

                    None => return Err(TransportError::MessageChannelClosed),
                }
            };

            match res {
                Ok(()) => continue,
                Err(e) => match e.analyze() {
                    TransportErrorOutcome::Terminate => return Err(e),
                    TransportErrorOutcome::GracefulClosure => break,
                    TransportErrorOutcome::Log => {
                        warn!("[{}] error handling client message: {}", transport.address(), e);
                    }
                    TransportErrorOutcome::Ignore => {}
                },
            }
        }

        debug!("[{}] Connection terminated", transport.address());

        Ok(())
    }

    #[inline]
    async fn handle_client_message(
        &self,
        transport: &mut QunetTransport,
        client: &Arc<ClientState<H>>,
        msg: &mut QunetMessage,
    ) -> Result<(), TransportError> {
        #[cfg(debug_assertions)]
        debug!("[{}] Received message: {:?}", transport.address(), msg.type_str());

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
                        self,
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
                            self,
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
                            self,
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
                            self,
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
                        self,
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
        client: &Arc<ClientState<H>>,
        msg: &mut QunetMessage,
    ) -> Result<(), TransportError> {
        let data = match msg.data_bufkind_mut() {
            Some(x) => x,
            None => unreachable!(),
        };

        self.app_handler.on_client_data(self, client, MsgData { data }).await;

        Ok(())
    }

    #[inline]
    async fn cleanup_connection(
        &self,
        transport: &mut QunetTransport,
        client: Arc<ClientState<H>>,
    ) -> Result<(), TransportError> {
        self.app_handler.on_client_disconnect(self, &client).await;

        self.clients.remove(&client.connection_id);
        self.connected_addrs.remove(&client.address);

        transport.run_server_cleanup(self).await
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

        debug!("- Estimate buffer pool memory usage: {} bytes", self.buffer_pool.heap_usage());
    }

    #[inline]
    pub(crate) fn write_ping_appdata(
        &self,
        writer: &mut ByteWriter,
    ) -> Result<(), ByteWriterError> {
        self.app_handler.on_ping(self, writer)
    }

    // Compression apis

    pub(crate) async fn get_new_buffer(&self, size: usize) -> BufferKind {
        match self.buffer_pool.get(size).await {
            Some(buf) => BufferKind::new_pooled(buf),

            // fallback for very large needs
            None => BufferKind::new_heap(size),
        }
    }

    // Public API for the application (sending packets, etc.)

    pub fn shutdown(&self) {
        self.terminate_notify.notify_one();
    }

    pub fn handler(&self) -> &H {
        &self.app_handler
    }

    pub async fn schedule<F, Fut>(self: ServerHandle<H>, interval: Duration, mut f: F)
    where
        F: FnMut(ServerHandle<H>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let this = self.clone();

        self.schedules.lock().await.spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.tick().await; // avoid instant tick

            loop {
                interval.tick().await;
                f(this.clone()).await;
            }
        });
    }

    pub async fn request_buffer(&self, size: usize) -> BufferKind {
        if size <= QUNET_SMALL_MESSAGE_SIZE {
            BufferKind::new_small()
        } else {
            self.get_new_buffer(size).await
        }
    }

    pub fn print_server_status(&self) {
        info!("Server status:");
        info!(
            " - {}/{} clients connected, {} udp routes",
            self.clients.len(),
            self.connected_addrs.len(),
            self.udp_router.len()
        );

        let pool_stats = self.buffer_pool.stats();
        info!(" - Buffer pool heap usage: {} bytes", pool_stats.total_heap_usage);

        for (i, pool) in pool_stats.pool_stats.iter().enumerate() {
            info!(
                "   - Subpool {} (bufsize {}): {}/{} allocated, {} free, heap usage: {} bytes",
                i + 1,
                pool.buf_size,
                pool.allocated_buffers,
                pool.max_buffers,
                pool.avail_buffers,
                pool.heap_usage,
            );
        }
    }
}

impl<H: AppHandler> CompressionHandler for Server<H> {
    async fn compress_zstd(&self, data: &[u8]) -> Result<BufferKind, CompressError> {
        self.compressor.compress_zstd(data).await
    }

    async fn decompress_zstd(
        &self,
        data: &[u8],
        uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError> {
        self.compressor.decompress_zstd(data, uncompressed_size).await
    }

    async fn compress_lz4(&self, data: &[u8]) -> Result<BufferKind, CompressError> {
        self.compressor.compress_lz4(data).await
    }

    async fn decompress_lz4(
        &self,
        data: &[u8],
        uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError> {
        self.compressor.decompress_lz4(data, uncompressed_size).await
    }
}

pub struct ServerHandle<H: AppHandler> {
    inner: Arc<Server<H>>,
}

impl<H: AppHandler> Clone for ServerHandle<H> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<H: AppHandler> ServerHandle<H> {
    pub async fn run(self) -> ServerOutcome {
        Server::run_server(self.clone()).await
    }

    pub fn make_weak(&self) -> WeakServerHandle<H> {
        WeakServerHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl<H: AppHandler> Deref for ServerHandle<H> {
    type Target = Server<H>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct WeakServerHandle<H: AppHandler> {
    inner: Weak<Server<H>>,
}

impl<H: AppHandler> WeakServerHandle<H> {
    pub fn upgrade(&self) -> Option<ServerHandle<H>> {
        self.inner.upgrade().map(ServerHandle::from)
    }
}

impl<H: AppHandler> From<Arc<Server<H>>> for ServerHandle<H> {
    fn from(server: Arc<Server<H>>) -> Self {
        Self { inner: server }
    }
}
