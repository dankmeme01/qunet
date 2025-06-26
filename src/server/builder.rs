use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::server::{Server, ServerHandle, ServerOutcome};

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
}

#[derive(Debug)]
pub(crate) struct WsOptions {
    pub address: SocketAddr,
}

#[derive(Default, Debug)]
pub struct ServerBuilder {
    pub(crate) udp_opts: Option<UdpOptions>,
    pub(crate) tcp_opts: Option<TcpOptions>,
    pub(crate) quic_opts: Option<QuicOptions>,
    pub(crate) ws_opts: Option<WsOptions>,

    pub(crate) message_size_limit: Option<usize>,

    pub(crate) qdb_path: Option<PathBuf>,
    pub(crate) qdb_data: Option<Vec<u8>>,

    pub(crate) graceful_shutdown_timeout: Option<Duration>,
}

impl ServerBuilder {
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

    pub fn with_quic(mut self, address: SocketAddr) -> Self {
        self.quic_opts = Some(QuicOptions { address });
        self
    }

    pub fn with_ws(mut self, address: SocketAddr) -> Self {
        self.ws_opts = Some(WsOptions { address });
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

    pub fn build(self) -> Server {
        Server::from_builder(self)
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
