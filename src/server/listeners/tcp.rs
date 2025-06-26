use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tracing::{debug, error, warn};

use crate::{
    buffers::byte_reader::ByteReader,
    server::{
        ServerHandle,
        builder::TcpOptions,
        listeners::listener::{BindError, ListenerError, ListenerHandle, ServerListener},
        protocol::{HANDSHAKE_START_SIZE, MSG_HANDSHAKE_START},
        transport::{ClientTransport, ClientTransportKind, tcp::ClientTcpTransport},
    },
};

pub(crate) struct TcpServerListener {
    opts: TcpOptions,
    socket: TcpListener,
    shutdown_notify: Notify,
}

struct PendingTcpConnection {
    stream: TcpStream,
    addr: SocketAddr,
}

impl PendingTcpConnection {
    pub async fn wait_for_handshake(&mut self) -> Result<(u16, [u8; 16]), ListenerError> {
        let mut buf = [0u8; HANDSHAKE_START_SIZE];
        let mut read_bytes = 0;

        while read_bytes < HANDSHAKE_START_SIZE {
            read_bytes += self.stream.read(&mut buf[read_bytes..]).await?;
        }

        let mut reader = ByteReader::new(&buf[..read_bytes]);

        let msg_type = reader.read_u8()?;
        let qunet_major = reader.read_u16()?;
        let _ = reader.read_u16()?; // we dont use udp frag limit in tcp

        let mut qdb_hash = [0u8; 16];
        reader.read_bytes(&mut qdb_hash)?;

        if msg_type != MSG_HANDSHAKE_START {
            return Err(ListenerError::MalformedHandshake);
        }

        Ok((qunet_major, qdb_hash))
    }
}

impl TcpServerListener {
    pub async fn new(opts: TcpOptions) -> Result<Self, BindError> {
        let socket = TcpListener::bind(opts.address).await?;

        Ok(Self {
            opts,
            socket,
            shutdown_notify: Notify::new(),
        })
    }

    pub fn accept_connection(server: ServerHandle, stream: TcpStream, addr: SocketAddr) {
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
                        &server,
                    );

                    if let Err(err) = server.accept_connection(transport).await {
                        warn!("Failed to accept handshake from client {addr} (TCP): {err}");
                    }
                }
                Ok(Err(err)) => {
                    warn!("Client {addr} (TCP) failed to complete handshake: {err}");
                }

                Err(_) => {
                    warn!("Client {addr} (TCP) did not complete the handshake in time");
                }
            }
        });
    }
}

impl ServerListener for TcpServerListener {
    fn run(self: Arc<TcpServerListener>, server: ServerHandle) -> ListenerHandle {
        tokio::spawn(async move {
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

                    _ = self.shutdown_notify.notified() => {
                        break;
                    }
                }
            }

            Ok(())
        })
    }

    fn shutdown(&self) {
        self.shutdown_notify.notify_one();
    }

    fn identifier(&self) -> String {
        format!("{} (TCP)", self.opts.address)
    }

    fn port(&self) -> u16 {
        self.opts.address.port()
    }
}
