use std::{
    borrow::Cow,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Weak},
    time::Duration,
};

use dashmap::DashMap;
use nohash_hasher::BuildNoHashHasher;
use thiserror::Error;
use tokio::{sync::Notify, task::JoinSet};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info, trace, warn};

use crate::{
    buffers::{BufPool, ByteWriter, ByteWriterError, HybridBufferPool},
    database::{self, QunetDatabase},
    message::{
        self, BufferKind, MsgData, QUNET_SMALL_MESSAGE_SIZE, QunetMessage, QunetRawMessage,
        channel::{RawMessageReceiver, RawMessageSender},
    },
    protocol::{self, *},
    server::{
        app_handler::{AppHandler, DefaultAppHandler},
        builder::{CompressionMode, ListenerOptions, ServerBuilder},
        client::{ClientNotification, ClientState},
        listeners::*,
        stat_tracker::StatTracker,
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
pub mod stat_tracker;

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

#[derive(Default)]
struct SuspendedClientState {
    new_transport: parking_lot::Mutex<Option<QunetTransport>>,
    reclaim_notify: Notify,
}

impl SuspendedClientState {
    pub async fn wait(&self, timeout: Duration) -> Option<QunetTransport> {
        tokio::time::timeout(timeout, self.reclaim_notify.notified()).await.ok()?;
        self.new_transport.lock().take()
    }

    pub fn resume(&self, transport: QunetTransport) -> bool {
        *self.new_transport.lock() = Some(transport);
        self.reclaim_notify.notify_one();
        true
    }

    // pub fn cancel_resume(&self) {
    //     self.reclaim_notify.notify_one();
    // }
}

pub struct Server<H: AppHandler> {
    udp_listener: Option<Arc<UdpServerListener<H>>>,
    tcp_listener: Option<Arc<TcpServerListener<H>>>,
    #[cfg(feature = "quic")]
    quic_listener: Option<Arc<QuicServerListener<H>>>,
    pub(crate) buffer_pool: Arc<HybridBufferPool>,
    pub(crate) app_handler: H,

    shutdown_token: CancellationToken,
    listener_tracker: TaskTracker,
    conn_tracker: TaskTracker,
    terminate_notify: Notify,
    schedules: parking_lot::Mutex<JoinSet<!>>,
    compressor: CompressionHandlerImpl<HybridBufferPool>,
    stat_tracker: Option<StatTracker>,

    udp_router: DashMap<u64, RawMessageSender, BuildNoHashHasher<u64>>,
    clients: DashMap<u64, Arc<ClientState<H>>, BuildNoHashHasher<u64>>,
    connected_addrs: DashMap<SocketAddr, u64>, // this serves to detect duplicate connections from the same ip:port tuple
    suspended_clients: DashMap<u64, Arc<SuspendedClientState>, BuildNoHashHasher<u64>>,

    // misc settings
    message_size_limit: usize,
    suspend_timeout: Duration,
    graceful_shutdown_timeout: Duration,
    pub(crate) listener_opts: ListenerOptions,
    pub(crate) compression_mode: CompressionMode,

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
    pub(crate) async fn from_builder(mut builder: ServerBuilder<H>) -> Result<Self, ServerOutcome> {
        let app_handler =
            builder.app_handler.take().expect("App handler must be set in the builder");

        // setup buffer pool, while it's unique
        let buffer_pool = Arc::new(HybridBufferPool::new(
            builder.mem_options.initial_mem,
            builder.mem_options.max_mem,
        ));

        // setup compressor
        let compressor = CompressionHandlerImpl::new(buffer_pool.clone());

        // setup stat tracker
        let stat_tracker = if builder.stat_tracker { Some(StatTracker::new()) } else { None };

        let mut server = Server {
            udp_listener: None,
            tcp_listener: None,
            #[cfg(feature = "quic")]
            quic_listener: None,
            buffer_pool,
            app_handler,

            shutdown_token: CancellationToken::new(),
            listener_tracker: TaskTracker::new(),
            conn_tracker: TaskTracker::new(),
            terminate_notify: Notify::new(),
            schedules: parking_lot::Mutex::new(JoinSet::new()),
            compressor,
            stat_tracker,

            udp_router: DashMap::default(),
            clients: DashMap::default(),
            connected_addrs: DashMap::default(),
            suspended_clients: DashMap::default(),

            message_size_limit: builder.message_size_limit.unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT),
            suspend_timeout: builder.max_suspend_time.unwrap_or(Duration::from_secs(60)),
            graceful_shutdown_timeout: builder
                .graceful_shutdown_timeout
                .unwrap_or(Duration::from_secs(10)),
            listener_opts: builder.listener_opts,
            compression_mode: builder.compression_mode,

            qdb: QunetDatabase::default(),
            qdb_data: Arc::new([]),
            qdb_hash: [0; 32],
            qdb_uncompressed_size: 0,
        };

        server.setup(builder).await?;

        Ok(server)
    }

    async fn setup(&mut self, mut builder: ServerBuilder<H>) -> Result<(), ServerOutcome> {
        self.app_handler.pre_setup(self).await.map_err(ServerOutcome::CustomError)?;

        self.print_config(&builder);

        // Setup connection listeners
        if let Some(opts) = builder.udp_opts.take() {
            let listener = UdpServerListener::new(
                opts,
                self.shutdown_token.clone(),
                &builder.listener_opts,
                &builder.mem_options,
            )
            .await?;

            self.udp_listener = Some(Arc::new(listener));
        }

        if let Some(opts) = builder.tcp_opts.take() {
            let listener =
                TcpServerListener::new(opts, self.shutdown_token.clone(), &builder.listener_opts)
                    .await?;
            self.tcp_listener = Some(Arc::new(listener));
        }

        #[cfg(feature = "quic")]
        if let Some(opts) = builder.quic_opts.take() {
            let listener =
                QuicServerListener::new(opts, self.shutdown_token.clone(), &builder.listener_opts)
                    .await?;

            self.quic_listener = Some(Arc::new(listener));
        }

        // Setup qdb stuff
        let qdb_data = if let Some(data) = builder.qdb_data.take() {
            data.into_boxed_slice()
        } else if let Some(path) = builder.qdb_path.take() {
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

        #[cfg(feature = "quic")]
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

        loop {
            let wait_for_usr1: Pin<Box<dyn Future<Output = bool> + Send + 'static>> = if cfg!(unix)
            {
                use tokio::signal::unix;
                Box::pin(async move {
                    unix::signal(unix::SignalKind::user_defined1()).unwrap().recv().await.is_some()
                })
            } else {    
                // this future will never complete
                Box::pin(async move { std::future::pending().await })
            };

            tokio::select! {
                _ = self.listener_tracker.wait() => {
                    error!("All listeners have unexpectedly terminated, shutting down!");
                    return ServerOutcome::AllListenersTerminated;
                },

                _ = self.terminate_notify.notified() => {
                    info!("Server termination requested, trying to shut down gracefully");
                    break;
                },

                _ = tokio::signal::ctrl_c() => {
                    info!("Interrupt received, trying to shut down gracefully");
                    break;
                }

                _ = wait_for_usr1 => {
                    self.app_handler.on_sigusr1(&self).await;
                }
            };
        }

        match tokio::time::timeout(self.graceful_shutdown_timeout, self.graceful_shutdown()).await {
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

    #[allow(clippy::await_holding_lock)]
    async fn graceful_shutdown(&self) -> Result<(), ServerOutcome> {
        trace!("running pre-shutdown hook");
        self.app_handler.pre_shutdown(self).await.map_err(ServerOutcome::CustomError)?;

        trace!("cancelling all listeners and active connections");
        self.shutdown_token.cancel();
        self.conn_tracker.close();

        trace!("aborting all schedules");
        {
            let mut schedules = self.schedules.lock();
            schedules.abort_all();

            while !schedules.is_empty() {
                schedules.join_next().await;
            }
        }

        trace!("waiting for all listeners to terminate");
        self.listener_tracker.wait().await;
        trace!("waiting 1s to let connections terminate");
        let _ = tokio::time::timeout(Duration::from_secs(1), self.conn_tracker.wait()).await;

        trace!("cleaning up and running post-shutdown hook");
        self.clients.clear();
        self.udp_router.clear();
        self.app_handler.post_shutdown(self).await.map_err(ServerOutcome::CustomError)?;

        trace!("graceful shutdown complete!");

        Ok(())
    }

    pub(crate) fn accept_connection(self: ServerHandle<H>, mut transport: QunetTransport) {
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
            self.with_stat_tracker(|s| s.new_connection(client.connection_id, client.address));

            // main entry point
            if let Err(e) =
                Self::client_handler(&self, transport, Arc::clone(&client), send_qdb).await
            {
                warn!("[{}] Failed to setup client connection: {}", address, e);
            }

            self.with_stat_tracker(|s| s.end_connection(client.connection_id));

            // always run cleanup!
            match Self::cleanup_connection(&self, client).await {
                Ok(()) => {}
                Err(err) => warn!("[{}] Failed to clean up connection: {}", address, err),
            }
        });
    }

    pub(crate) async fn recover_connection(
        &self,
        connection_id: u64,
        mut transport: QunetTransport,
    ) {
        if let Some(client) = self.suspended_clients.get(&connection_id) {
            client.resume(transport);
            return;
        }

        let addr = transport.address();

        // does this connection exist at all?
        let Some(client) = self.clients.get(&connection_id) else {
            debug!(
                "[{addr}] Attempted to recover a connection that is not suspended ({connection_id})",
            );

            let _ = transport.send_message(QunetMessage::ReconnectFailure, false, &()).await;

            return;
        };

        // forcefully migrate the connection to the new transport
        client.notif_tx.send(ClientNotification::TerminateSuspend);

        if tokio::time::timeout(Duration::from_secs(5), client.suspended_notify.notified())
            .await
            .is_ok()
        {
            if let Some(sclient) = self.suspended_clients.get(&connection_id) {
                sclient.resume(transport);
                return;
            }

            // if the client is not in the map, it might be that someone else just reclaimed it
        }

        warn!(
            "[{addr}] Timed out waiting for a connection to suspend to handle a reconnect, will kill it ({connection_id})",
        );

        let _ = transport.send_message(QunetMessage::ReconnectFailure, false, &()).await;
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

    /// This function only returns an error if the initial transport setup fails, otherwise it always returns `Ok(())`
    async fn client_handler(
        &self,
        mut transport: QunetTransport,
        client: Arc<ClientState<H>>,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
        let conn_id = transport.connection_id();

        // run setup (udp needs this)
        transport.run_server_setup(self).await?;

        // send the handshake response
        if let Err(e) = transport
            .send_handshake_response(
                if send_qdb { Some(self.qdb_data.as_ref()) } else { None },
                self.qdb_uncompressed_size,
            )
            .await
        {
            warn!("failed to send handshake for {}, cleaning up", transport.connection_id());
            transport.run_server_cleanup(self).await?;
            return Err(e);
        }

        loop {
            let res = self.client_main_loop(&mut transport, client.clone(), send_qdb).await;

            let should_suspend = match res {
                Ok(()) => {
                    // graceful closure
                    debug!(
                        "[{} ({})] Connection terminated gracefully",
                        transport.address(),
                        transport.connection_id()
                    );

                    false
                }

                Err(err) => {
                    debug!(
                        "[{} ({})] Connection terminated with error: {}",
                        transport.address(),
                        transport.connection_id(),
                        err
                    );

                    self.maybe_send_error_to_client(&mut transport, &err).await;

                    true
                }
            };

            // run cleanup
            trace!(
                "connection {} terminated, suspending: {}",
                transport.connection_id(),
                should_suspend
            );
            let _ = transport.run_server_cleanup(self).await;

            if should_suspend {
                let sstate = Arc::new(SuspendedClientState::default());
                self.suspended_clients.insert(conn_id, sstate.clone());
                client.set_suspended(true);

                self.app_handler.on_client_suspend(self, &client).await;
                self.with_stat_tracker(|s| s.suspend_connection(conn_id));

                // wait to be recovered
                let new_transport = tokio::select! {
                    res = sstate.wait(self.suspend_timeout) => res,

                    _ = client.terminate_notify.notified() => None
                };

                // remove from suspended map
                self.suspended_clients.remove(&client.connection_id);

                let Some(new_transport) = new_transport else {
                    debug!(
                        "timed out waiting for connection {} to unsuspend",
                        transport.connection_id()
                    );

                    break;
                };

                // we got reclaimed!
                debug!("connection {} reclaimed from suspension", transport.connection_id());

                client.set_suspended(false);
                self.app_handler.on_client_resume(self, &client).await;
                self.with_stat_tracker(|s| s.resume_connection(conn_id));

                transport.kind = new_transport.kind;
                transport.data.closed = false;
                transport.data.address = new_transport.data.address;
                #[cfg(target_os = "linux")]
                {
                    transport.data.c_sockaddr_data = new_transport.data.c_sockaddr_data;
                    transport.data.c_sockaddr_len = new_transport.data.c_sockaddr_len;
                }
                transport.run_server_setup(self).await?;

                if let Err(e) =
                    transport.send_message(QunetMessage::ReconnectSuccess, false, &()).await
                {
                    // something really weird happened
                    warn!(
                        "failed to send reconnect success for {}, cleaning up",
                        transport.connection_id()
                    );
                    transport.run_server_cleanup(self).await?;
                    return Err(e);
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn maybe_send_error_to_client(
        &self,
        transport: &mut QunetTransport,
        error: &TransportError,
    ) {
        // depending on the error, we might want to send a message to the client notifying about it
        let (error_code, error_message) = match error {
            TransportError::MessageTooLong => (QunetConnectionError::StreamMessageTooLong, None),

            TransportError::ZeroLengthMessage => {
                (QunetConnectionError::ZeroLengthStreamMessage, None)
            }

            TransportError::MessageChannelClosed => {
                (QunetConnectionError::InternalServerError, None)
            }

            TransportError::SuspendRequested => {
                return;
            }

            _ => (QunetConnectionError::Custom, Some(Cow::Owned(error.to_string()))),
        };

        // we don't care if it fails here
        let _ = transport
            .send_message(QunetMessage::ServerClose { error_code, error_message }, false, &())
            .await;
    }

    async fn client_main_loop(
        &self,
        transport: &mut QunetTransport,
        client: Arc<ClientState<H>>,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
        const NEVER: Duration = Duration::from_hours(24 * 365 * 100);
        let notif_chan = transport.notif_chan.1.clone();

        let mut handshake_retx_count: u8 = 0;

        while !transport.data.closed {
            let timer_expiry = transport.until_timer_expiry().unwrap_or(NEVER);

            let res = tokio::select! {
                msg = transport.receive_message() => match msg {
                    Ok(msg) => match transport.decompress_message(msg, self).await {
                        Ok(mut msg) => self.handle_client_message(transport, &client, &mut msg).await,
                        Err(e) => Err(e),
                    },

                    Err(e) => Err(e),
                },

                _ = tokio::time::sleep(timer_expiry), if timer_expiry != NEVER => {
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
                    Some(notif) => {
                        self.handle_client_notification(
                            transport,
                            notif,
                            &mut handshake_retx_count,
                            send_qdb,
                        ).await
                    },
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

        Ok(())
    }

    #[inline]
    async fn handle_client_notification(
        &self,
        transport: &mut QunetTransport,
        notif: ClientNotification,
        handshake_retx_count: &mut u8,
        send_qdb: bool,
    ) -> Result<(), TransportError> {
        match notif {
            ClientNotification::DataMessage { buf, reliable } => {
                self.with_stat_tracker(|s| s.data_downstream(transport.connection_id(), &buf));

                transport.send_message(QunetMessage::new_data(buf), reliable, self).await
            }

            ClientNotification::RetransmitHandshake => {
                *handshake_retx_count += 1;

                // prevent malicious clients from spamming handshake retransmits, silently drop connection
                if *handshake_retx_count > 5 {
                    return Ok(());
                }

                transport
                    .send_handshake_response(
                        if send_qdb { Some(self.qdb_data.as_ref()) } else { None },
                        self.qdb_uncompressed_size,
                    )
                    .await
            }

            ClientNotification::Terminate => {
                transport.data.closed = true;
                Ok(())
            }

            ClientNotification::TerminateSuspend => {
                transport.data.closed = true;
                Err(TransportError::SuspendRequested)
            }

            ClientNotification::Disconnect(reason) => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    transport.send_message(
                        QunetMessage::ServerClose {
                            error_code: QunetConnectionError::Custom,
                            error_message: Some(reason),
                        },
                        false,
                        &(),
                    ),
                )
                .await;

                transport.data.closed = true;
                Ok(())
            }
        }
    }

    #[inline]
    async fn handle_client_message(
        &self,
        transport: &mut QunetTransport,
        client: &Arc<ClientState<H>>,
        msg: &mut QunetMessage,
    ) -> Result<(), TransportError> {
        // #[cfg(debug_assertions)]
        // trace!(cid = transport.connection_id(), "received message: {:?}", msg.type_str());

        match msg {
            QunetMessage::Keepalive { timestamp } => {
                self.with_stat_tracker(|s| s.keepalive(transport.connection_id()));

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
                if *dont_terminate {
                    // return an error, this will force the client to terminate with an error and suspend the connection
                    return Err(TransportError::SuspendRequested);
                } else {
                    // simply set the closed flag, which causes a graceful closure
                    transport.data.closed = true;
                }
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
        let data = msg.data_bufkind_mut().unwrap();

        self.with_stat_tracker(|s| s.data_upstream(client.connection_id, data));
        self.app_handler.on_client_data(self, client, MsgData { data }).await;

        Ok(())
    }

    #[inline]
    async fn cleanup_connection(&self, client: Arc<ClientState<H>>) -> Result<(), TransportError> {
        self.app_handler.on_client_disconnect(self, &client).await;

        self.clients.remove(&client.connection_id);
        self.connected_addrs.remove(&client.address);

        Ok(())
    }

    fn print_config(&self, builder: &ServerBuilder<H>) {
        debug!("Server configuration:");

        if let Some(udp) = &builder.udp_opts {
            debug!(
                "- {} (UDP, {} binds, discovery mode: {:?})",
                udp.address, udp.binds, udp.discovery_mode
            );
        }

        if let Some(tcp) = &builder.tcp_opts {
            debug!("- {} (TCP)", tcp.address);
        }

        #[cfg(feature = "quic")]
        if let Some(quic) = &builder.quic_opts {
            debug!("- {} (QUIC)", quic.address);
        }

        if let Some(ws) = &builder.ws_opts {
            debug!("- {} (WebSocket)", ws.address);
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

    fn with_stat_tracker(&self, f: impl FnOnce(&StatTracker)) {
        if let Some(tracker) = &self.stat_tracker {
            f(tracker);
        }
    }

    // Compression apis

    pub(crate) fn get_new_buffer(&self, size: usize) -> BufferKind {
        self.buffer_pool.get_or_heap(size)
    }

    // Public API for the application (sending packets, etc.)

    pub fn shutdown(&self) {
        self.terminate_notify.notify_one();
    }

    pub fn handler(&self) -> &H {
        &self.app_handler
    }

    pub fn stat_tracker(&self) -> Option<&StatTracker> {
        self.stat_tracker.as_ref()
    }

    pub fn schedule<F, Fut>(self: &ServerHandle<H>, interval: Duration, mut f: F)
    where
        F: FnMut(ServerHandle<H>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let this = self.clone();

        self.schedules.lock().spawn(async move {
            let mut interval = tokio::time::interval(interval);
            interval.tick().await; // avoid instant tick

            loop {
                interval.tick().await;
                f(this.clone()).await;
            }
        });
    }

    pub fn request_buffer(&self, size: usize) -> BufferKind {
        if size <= QUNET_SMALL_MESSAGE_SIZE {
            BufferKind::new_small()
        } else {
            self.get_new_buffer(size)
        }
    }

    pub fn get_buffer_pool(&self) -> Arc<HybridBufferPool> {
        Arc::clone(&self.buffer_pool)
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn udp_route_count(&self) -> usize {
        self.udp_router.len()
    }

    pub fn suspended_client_count(&self) -> usize {
        self.suspended_clients.len()
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
    fn compress_zstd(&self, data: &[u8], use_dict: bool) -> Result<BufferKind, CompressError> {
        self.compressor.compress_zstd(data, use_dict)
    }

    fn decompress_zstd(
        &self,
        data: &[u8],
        uncompressed_size: usize,
        use_dict: bool,
    ) -> Result<BufferKind, DecompressError> {
        self.compressor.decompress_zstd(data, uncompressed_size, use_dict)
    }

    fn compress_lz4(&self, data: &[u8]) -> Result<BufferKind, CompressError> {
        self.compressor.compress_lz4(data)
    }

    fn decompress_lz4(
        &self,
        data: &[u8],
        uncompressed_size: usize,
    ) -> Result<BufferKind, DecompressError> {
        self.compressor.decompress_lz4(data, uncompressed_size)
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
