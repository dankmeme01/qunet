use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::server::{
    ServerHandle,
    app_handler::AppHandler,
    builder::TcpOptions,
    listeners::listener::{BindError, ListenerError, ServerListener},
    transport::{ClientTransport, ClientTransportKind, tcp::ClientTcpTransport},
};

use super::stream;

pub(crate) struct TcpServerListener<H: AppHandler> {
    opts: TcpOptions,
    socket: TcpListener,
    shutdown_token: CancellationToken,
    _phantom: PhantomData<H>,
}

struct PendingTcpConnection {
    stream: TcpStream,
    addr: SocketAddr,
}

impl PendingTcpConnection {
    pub async fn wait_for_handshake(&mut self) -> Result<(u16, [u8; 16]), ListenerError> {
        stream::wait_for_handshake(&mut self.stream).await
    }
}

impl<H: AppHandler> TcpServerListener<H> {
    pub async fn new(
        opts: TcpOptions,
        shutdown_token: CancellationToken,
    ) -> Result<Self, BindError> {
        let socket = TcpListener::bind(opts.address).await?;

        Ok(Self {
            opts,
            socket,
            shutdown_token,
            _phantom: PhantomData,
        })
    }

    pub fn accept_connection(server: ServerHandle<H>, stream: TcpStream, addr: SocketAddr) {
        tokio::spawn(async move {
            debug!("Accepted TCP connection from {addr}, waiting for the handshake");

            let mut conn = PendingTcpConnection { stream, addr };

            match tokio::time::timeout(Duration::from_secs(30), conn.wait_for_handshake()).await {
                Ok(Ok((qunet_major, qdb_hash))) => {
                    let transport = ClientTransport::new(
                        ClientTransportKind::Tcp(ClientTcpTransport::new(conn.stream)),
                        conn.addr,
                        qunet_major,
                        qdb_hash,
                        server.clone(),
                    );

                    server.accept_connection(transport).await;
                }
                Ok(Err(err)) => {
                    warn!("Client {addr} (TCP) failed to complete handshake: {err}");
                }

                Err(_) => {
                    warn!(
                        "Client {addr} (TCP) did not complete the handshake in time, closing connection"
                    );
                }
            }
        });
    }
}

impl<H: AppHandler> ServerListener<H> for TcpServerListener<H> {
    async fn run(
        self: Arc<TcpServerListener<H>>,
        server: ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        debug!("Starting TCP listener on {}", self.opts.address);

        loop {
            tokio::select! {
                res = self.socket.accept() => match res {
                    Ok((stream, addr)) => Self::accept_connection(server.clone(), stream, addr),

                    Err(err) => {
                        // unfortunately, EMFILE and ENFILE errors don't have a specific error kind,
                        // we have to convert to string and check the contents
                        let err_string = err.to_string();

                        if err_string == "Too many open files" {
                            error!("Failed to accept TCP connection: Too many open files. Server is unable to accept new connections until the limit is raised. Sleeping for 1 second to prevent log spam.");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        } else {
                            error!("Failed to accept TCP connection: {err_string}");
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
        format!("{} (TCP)", self.opts.address)
    }

    fn port(&self) -> u16 {
        self.opts.address.port()
    }
}
