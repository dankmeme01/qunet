use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use s2n_quic::stream::BidirectionalStream;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::server::{
    ServerHandle,
    app_handler::AppHandler,
    builder::{ListenerOptions, QuicOptions},
    listeners::listener::{BindError, ListenerError, ServerListener},
    transport::{ClientTransport, ClientTransportKind, quic::ClientQuicTransport},
};

use super::stream;

pub(crate) struct QuicServerListener<H: AppHandler> {
    opts: QuicOptions,
    opts2: ListenerOptions,
    shutdown_token: CancellationToken,
    quic_server: Mutex<s2n_quic::Server>,
    _phantom: PhantomData<H>,
}

struct PendingQuicConnection {
    conn: s2n_quic::Connection,
}

struct HandshakeOutcome {
    stream: BidirectionalStream,
    qunet_major: u16,
    qdb_hash: [u8; 16],
}

impl PendingQuicConnection {
    pub async fn wait_for_handshake(&mut self) -> Result<HandshakeOutcome, ListenerError> {
        let stream = self.conn.accept_bidirectional_stream().await?;
        let Some(mut stream) = stream else {
            return Err(ListenerError::ConnectionClosed);
        };

        let (qunet_major, qdb_hash) = stream::wait_for_handshake(&mut stream).await?;

        Ok(HandshakeOutcome {
            stream,
            qunet_major,
            qdb_hash,
        })
    }
}

impl<H: AppHandler> QuicServerListener<H> {
    pub async fn new(
        opts: QuicOptions,
        opts2: ListenerOptions,
        shutdown_token: CancellationToken,
    ) -> Result<Self, BindError> {
        let tls = s2n_quic::provider::tls::rustls::Server::builder()
            .with_application_protocols([&b"qunet1"[..], &b"h3"[..]].iter())
            .map_err(BindError::Tls)?
            .with_certificate(opts.tls_cert_path.as_path(), opts.tls_key_path.as_path())
            .map_err(BindError::Tls)?;

        let tls = tls.build().map_err(BindError::Tls)?;

        // We only use one bidi quic stream
        let conn_limits = s2n_quic::provider::limits::Limits::new()
            .with_max_open_local_bidirectional_streams(2)
            .unwrap()
            .with_max_open_remote_bidirectional_streams(2)
            .unwrap()
            .with_max_open_local_unidirectional_streams(0)
            .unwrap()
            .with_max_open_remote_unidirectional_streams(0)
            .unwrap()
            .with_max_idle_timeout(Duration::from_secs(60))
            .unwrap();

        let quic_server = s2n_quic::Server::builder()
            .with_tls(tls)
            .unwrap()
            .with_limits(conn_limits)
            .unwrap()
            .with_io(opts.address)?
            .start()?;

        Ok(Self {
            opts,
            opts2,
            shutdown_token,
            quic_server: Mutex::new(quic_server),
            _phantom: PhantomData,
        })
    }

    pub fn accept_connection(
        server: ServerHandle<H>,
        conn: s2n_quic::Connection,
        timeout: Duration,
    ) {
        tokio::spawn(async move {
            let remote_addr = conn
                .remote_addr()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));

            debug!(
                "Accepted QUIC connection from {}, waiting for the handshake",
                remote_addr
            );

            let mut conn = PendingQuicConnection { conn };

            match tokio::time::timeout(timeout, conn.wait_for_handshake()).await {
                Ok(Ok(outcome)) => {
                    let transport = ClientTransport::new(
                        ClientTransportKind::Quic(ClientQuicTransport::new(
                            conn.conn,
                            outcome.stream,
                        )),
                        remote_addr,
                        outcome.qunet_major,
                        outcome.qdb_hash,
                        &server,
                    );

                    server.accept_connection(transport).await;
                }

                Ok(Err(err)) => {
                    warn!(
                        "Client {} (QUIC) failed to complete handshake: {err}",
                        remote_addr
                    );
                }

                Err(_) => {
                    warn!(
                        "Client {} (QUIC) did not complete the handshake in time, closing connection",
                        remote_addr
                    );
                }
            }
        });
    }
}

impl<H: AppHandler> ServerListener<H> for QuicServerListener<H> {
    async fn run(self: Arc<Self>, server: ServerHandle<H>) -> Result<(), ListenerError> {
        debug!("Starting QUIC listener on {}", self.opts.address);

        let mut qsrv = self.quic_server.lock().await;

        loop {
            tokio::select! {
                conn = qsrv.accept() => match conn {
                    Some(conn) => Self::accept_connection(server.clone(), conn, self.opts2.handshake_timeout),

                    None => {
                        warn!("QUIC listener suddenly closed");
                        break;
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
        format!("{} (QUIC)", self.opts.address)
    }

    fn port(&self) -> u16 {
        self.opts.address.port()
    }
}
