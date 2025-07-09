use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::server::{
    Server, ServerHandle, ServerOutcome,
    app_handler::{AppHandler, DefaultAppHandler},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpDiscoveryMode {
    Discovery,
    Connections,
    Both,
}

#[derive(Debug, Clone)]
pub(crate) struct UdpOptions {
    pub address: SocketAddr,
    pub binds: NonZeroUsize,
    pub discovery_mode: UdpDiscoveryMode,
}

#[derive(Debug)]
pub(crate) struct TcpOptions {
    pub address: SocketAddr,
}

#[derive(Debug)]
pub(crate) struct QuicOptions {
    pub address: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct WsOptions {
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub(crate) struct ListenerOptions {
    pub handshake_timeout: Duration,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolOpts {
    pub buf_size: usize,
    pub initial_buffers: usize,
    pub max_buffers: usize,
}

impl BufferPoolOpts {
    pub fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        Self {
            buf_size,
            initial_buffers,
            max_buffers,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryUsageOptions {
    pub buffer_pools: Vec<BufferPoolOpts>,
}

impl Default for MemoryUsageOptions {
    fn default() -> Self {
        Self {
            buffer_pools: vec![
                BufferPoolOpts::new(1500, 256, 4096), // buffers around mtu size for udp
                BufferPoolOpts::new(4096, 128, 1024), // small buffers
                BufferPoolOpts::new(65536, 16, 256),  // large buffers
            ],
        }
    }
}

#[derive(Default, Debug)]
pub struct ServerBuilder<H: AppHandler = DefaultAppHandler> {
    pub(crate) udp_opts: Option<UdpOptions>,
    pub(crate) tcp_opts: Option<TcpOptions>,
    pub(crate) quic_opts: Option<QuicOptions>,
    pub(crate) ws_opts: Option<WsOptions>,
    pub(crate) listener_opts: ListenerOptions,
    pub(crate) mem_options: MemoryUsageOptions,
    pub(crate) app_handler: Option<H>,

    pub(crate) message_size_limit: Option<usize>,

    pub(crate) qdb_path: Option<PathBuf>,
    pub(crate) qdb_data: Option<Vec<u8>>,

    pub(crate) graceful_shutdown_timeout: Option<Duration>,
    pub(crate) max_suspend_time: Option<Duration>,
}

impl<H: AppHandler> ServerBuilder<H> {
    pub fn with_udp(mut self, address: SocketAddr, discovery_mode: UdpDiscoveryMode) -> Self {
        self.udp_opts = Some(UdpOptions {
            address,
            binds: NonZeroUsize::new(1).unwrap(),
            discovery_mode,
        });
        self
    }

    pub fn with_udp_multiple(
        mut self,
        address: SocketAddr,
        discovery_mode: UdpDiscoveryMode,
        binds: usize,
    ) -> Self {
        self.udp_opts = Some(UdpOptions {
            address,
            binds: NonZeroUsize::new(binds).expect("Binds value must be non-zero"),
            discovery_mode,
        });
        self
    }

    pub fn with_tcp(mut self, address: SocketAddr) -> Self {
        self.tcp_opts = Some(TcpOptions { address });
        self
    }

    pub fn with_quic<P: AsRef<Path>, P2: AsRef<Path>>(
        mut self,
        address: SocketAddr,
        tls_cert_path: P,
        tls_key_path: P2,
    ) -> Self {
        self.quic_opts = Some(QuicOptions {
            address,
            tls_cert_path: tls_cert_path.as_ref().to_path_buf(),
            tls_key_path: tls_key_path.as_ref().to_path_buf(),
        });
        self
    }

    pub fn with_ws(mut self, address: SocketAddr) -> Self {
        self.ws_opts = Some(WsOptions { address });
        self
    }

    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.listener_opts.handshake_timeout = timeout;
        self
    }

    pub fn with_qdb_file<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.qdb_path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn with_qdb_data(mut self, data: Vec<u8>) -> Self {
        self.qdb_data = Some(data);
        self
    }

    pub fn with_graceful_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = Some(timeout);
        self
    }

    pub fn with_max_suspend_time(mut self, timeout: Duration) -> Self {
        self.max_suspend_time = Some(timeout);
        self
    }

    pub fn with_memory_options(mut self, options: MemoryUsageOptions) -> Self {
        self.mem_options = options;
        self
    }

    pub fn with_app_handler<NewH: AppHandler>(self, app_handler: NewH) -> ServerBuilder<NewH> {
        ServerBuilder {
            udp_opts: self.udp_opts,
            tcp_opts: self.tcp_opts,
            quic_opts: self.quic_opts,
            ws_opts: self.ws_opts,
            listener_opts: self.listener_opts,
            app_handler: Some(app_handler),
            message_size_limit: self.message_size_limit,
            qdb_path: self.qdb_path,
            qdb_data: self.qdb_data,
            graceful_shutdown_timeout: self.graceful_shutdown_timeout,
            max_suspend_time: self.max_suspend_time,
            mem_options: self.mem_options,
        }
    }

    pub fn build(self) -> Server<H> {
        Server::<H>::from_builder(self)
    }

    pub async fn run(self) -> ServerOutcome {
        let mut server = self.build();

        if let Err(o) = server.setup().await {
            return o;
        }

        let handle = ServerHandle {
            server: Arc::new(server),
        };

        handle.run().await
    }
}
