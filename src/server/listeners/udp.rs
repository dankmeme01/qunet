use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use socket2::{Domain, Socket, Type};
use tokio::{net::UdpSocket, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{
    buffers::{buffer_pool::BufferPool, byte_reader::ByteReader, byte_writer::ByteWriter},
    server::{
        Server, ServerHandle,
        app_handler::AppHandler,
        builder::{ListenerOptions, MemoryUsageOptions, UdpDiscoveryMode, UdpOptions},
        listeners::listener::{BindError, ListenerError, ServerListener},
        message::{QUNET_SMALL_MESSAGE_SIZE, QunetMessage, QunetRawMessage},
        protocol::*,
        transport::{ClientTransport, ClientTransportKind, udp::ClientUdpTransport},
    },
};

struct OneListener {
    socket: Arc<UdpSocket>,
}

pub(crate) struct UdpServerListener<H: AppHandler> {
    opts: UdpOptions,
    sockets: Vec<OneListener>,
    shutdown_token: CancellationToken,
    buffer_pool: BufferPool,
    _phantom: PhantomData<H>,
}

pub(crate) async fn make_socket(
    address: SocketAddr,
    multi: bool,
    recv_buffer_size: Option<usize>,
) -> std::io::Result<UdpSocket> {
    let domain = if address.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };

    debug!("Binding UDP socket to {address} (multi: {multi})");

    let socket = Socket::new(domain, Type::DGRAM, None)?;

    if multi {
        socket.set_reuse_port(true)?;
    }

    if let Some(size) = recv_buffer_size {
        socket.set_recv_buffer_size(size)?;
    }

    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;

    let udp_socket = UdpSocket::from_std(socket.into())?;

    Ok(udp_socket)
}

impl<H: AppHandler> UdpServerListener<H> {
    pub async fn new(
        opts: UdpOptions,
        shutdown_token: CancellationToken,
        _listener_opts: &ListenerOptions,
        mem_opts: &MemoryUsageOptions,
    ) -> Result<Self, BindError> {
        let binds = opts.binds.get();

        let mut sockets = Vec::with_capacity(binds);

        for _ in 0..binds {
            let socket =
                make_socket(opts.address, binds > 1, mem_opts.udp_recv_buffer_size).await?;

            sockets.push(OneListener {
                socket: Arc::new(socket),
            });
        }

        Ok(UdpServerListener {
            opts,
            sockets,
            shutdown_token,
            buffer_pool: BufferPool::new(
                mem_opts.udp_listener_buffer_pool.buf_size,
                mem_opts.udp_listener_buffer_pool.initial_buffers,
                mem_opts.udp_listener_buffer_pool.max_buffers,
            ),
            _phantom: PhantomData,
        })
    }

    #[inline]
    fn accepts_pings(&self) -> bool {
        self.opts.discovery_mode != UdpDiscoveryMode::Connections
    }

    fn wrap_error<T>(peer: SocketAddr, res: Result<T, ListenerError>) -> Option<T> {
        match res {
            Ok(v) => Some(v),

            Err(ListenerError::DecodeError(e)) => {
                debug!("[UDP {peer}] error handling message: {e}");
                None
            }

            Err(e) => {
                warn!("[UDP {peer}] error handling message: {e}");
                None
            }
        }
    }

    pub async fn run_listener(
        self: Arc<Self>,
        index: usize,
        server: ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        let socket_arc = &self.sockets[index].socket;
        let socket = &*self.sockets[index].socket;

        // we try to reuse the same buffer when accepting packets,
        // only getting a new one when we give away this buffer to another task
        let mut buf = self.buffer_pool.get().await;

        loop {
            // TODO: dabble into recvmmsg for better performance, linux only
            let (len, peer) = socket
                .recv_from(&mut buf)
                .await
                .map_err(ListenerError::IoError)?;

            if len == 0 {
                continue;
            }

            let data = &buf[..len];

            // handle pings
            let mut reader = ByteReader::new(data);
            if buf[0] == MSG_PING {
                if !self.accepts_pings() {
                    debug!("[UDP {peer}] received ping when disallowed, ignoring");
                    continue;
                }

                Self::wrap_error(
                    peer,
                    self.handle_ping_message(reader, socket, peer, &server)
                        .await,
                );

                continue;
            }

            let msg_type = reader.read_u8()?;

            // handle handshake start (no conn id)
            if msg_type == MSG_HANDSHAKE_START {
                Self::wrap_error(
                    peer,
                    Self::handle_handshake(socket_arc.clone(), reader, peer, &server).await,
                );

                continue;
            }

            // handle other messages
            if let Some(conn_id) = QunetMessage::connection_id_from_header(data) {
                // don't decode the message, route it to the connection
                let msg = if data.len() <= QUNET_SMALL_MESSAGE_SIZE {
                    let mut msg_data = [0u8; QUNET_SMALL_MESSAGE_SIZE];
                    msg_data[..len].copy_from_slice(data);

                    QunetRawMessage::Small {
                        data: msg_data,
                        len,
                    }
                } else {
                    let msg = QunetRawMessage::Large { buffer: buf, len };

                    // get a new buffer for the next message
                    buf = match self.buffer_pool.try_get() {
                        Some(x) => x,
                        None => self.buffer_pool.get().await,
                    };

                    msg
                };

                if !server.dispatch_udp_message(conn_id, msg).await {
                    debug!("[UDP {peer}] failed to dispatch message to connection {conn_id}");
                }
            } else {
                debug!("[UDP {peer}] received message without connection ID, ignoring");
            }
        }
    }

    pub async fn run_ping_listener(
        self: Arc<Self>,
        index: usize,
        server: ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        let socket = &self.sockets[index].socket;
        let mut buf = [0u8; 1500];

        loop {
            let (len, peer) = socket
                .recv_from(&mut buf)
                .await
                .map_err(ListenerError::IoError)?;

            if len == 0 || buf[0] != MSG_PING {
                // ignore empty packets or packets that are not ping messages
                continue;
            }

            let reader = ByteReader::new(&buf[..len]);

            if let Err(err) = self
                .handle_ping_message(reader, socket, peer, &server)
                .await
            {
                match err {
                    ListenerError::DecodeError(_) => {
                        debug!("[UDP {peer}] {err}");
                    }

                    e => {
                        warn!("[UDP {peer}] error handling ping: {e}");
                    }
                }
            }
        }
    }

    async fn handle_ping_message(
        &self,
        mut reader: ByteReader<'_>,
        socket: &UdpSocket,
        peer: SocketAddr,
        server: &ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        assert_eq!(reader.read_u8()?, MSG_PING);
        let ping_id = reader.read_u32()?;
        let flags = reader.read_bits::<u8>()?;
        let omit_protocols = flags.get_bit(0);

        if omit_protocols {
            let mut out_buf = [0u8; 10];
            let mut writer = ByteWriter::new(&mut out_buf);

            writer.write_u8(MSG_PONG);
            writer.write_u32(ping_id);
            writer.write_u8(0); // no protocols

            socket
                .send_to(writer.written(), peer)
                .await
                .map_err(ListenerError::IoError)?;

            return Ok(());
        }

        // sure hope it's enough
        // let mut out_buf = uninit_bytes::<256>();
        let mut out_buf = [0u8; 256];
        let mut writer = ByteWriter::new(&mut out_buf);
        writer.write_u8(MSG_PONG);
        writer.write_u32(ping_id);

        // skip 1 byte for the protocol count
        let protocol_start = writer.pos();
        let mut protocol_count = 0;
        writer.write_u8(0);

        // write the protocols

        if let Some(listener) = &server.udp_listener {
            writer.write_u8(PROTO_UDP);
            writer.write_u16(listener.port());
            protocol_count += 1;
        }

        if let Some(listener) = &server.tcp_listener {
            writer.write_u8(PROTO_TCP);
            writer.write_u16(listener.port());
            protocol_count += 1;
        }

        // write application specific data
        let appdata_start = writer.pos();
        writer.write_u16(0); // length, set to 0 for now

        server.write_ping_appdata(&mut writer)?;

        // preserve end position
        let end_pos = writer.pos();
        let appdata_size = end_pos - appdata_start - 2;

        // write protocol count and appdata size
        writer.set_pos(protocol_start);
        writer.write_u8(protocol_count);
        writer.set_pos(appdata_start);
        writer.write_u16(appdata_size as u16);

        // restore end position
        writer.set_pos(end_pos);

        // send the response
        socket
            .send_to(writer.written(), peer)
            .await
            .map_err(ListenerError::IoError)?;

        // done!

        Ok(())
    }

    async fn handle_handshake(
        socket: Arc<UdpSocket>,
        mut reader: ByteReader<'_>,
        peer: SocketAddr,
        server: &ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        let major_version = reader.read_u16()?;
        let mut frag_limit = reader.read_u16()?;
        let mut qdb_hash = [0u8; 16];
        reader.read_bytes(&mut qdb_hash)?;

        // adjust fragmentation limit, must be between 1000 and 1400
        if frag_limit == 0 {
            frag_limit = UDP_PACKET_LIMIT as u16;
        }

        frag_limit = frag_limit.clamp(1000, UDP_PACKET_LIMIT as u16);

        let transport = ClientTransport::new(
            ClientTransportKind::Udp(ClientUdpTransport::new(socket, frag_limit as usize, server)),
            peer,
            major_version,
            qdb_hash,
            server.clone(),
        );

        Server::accept_connection(server.clone(), transport).await;

        Ok(())
    }
}

impl<H: AppHandler> ServerListener<H> for UdpServerListener<H> {
    async fn run(
        self: Arc<UdpServerListener<H>>,
        server: ServerHandle<H>,
    ) -> Result<(), ListenerError> {
        debug!(
            "Starting UDP listener on {} (sockets: {})",
            self.opts.address,
            self.sockets.len(),
        );

        let mut set = JoinSet::new();

        for i in 0..self.sockets.len() {
            let self_clone = self.clone();
            let server = server.clone();

            set.spawn(async move {
                if self_clone.opts.discovery_mode == UdpDiscoveryMode::Discovery {
                    self_clone.run_ping_listener(i, server).await
                } else {
                    self_clone.run_listener(i, server).await
                }
            });
        }

        tokio::select! {
            res = set.join_all() => {
                warn!("All UDP sub-listeners have terminated");

                for handle in res {
                    match handle {
                        // listeners should never terminate with no error
                        Ok(()) => unreachable!(),

                        Err(e) => {
                            error!("UDP sub-listener exited with an error: {e}");
                        }
                    }
                }
            },

            _ = self.shutdown_token.cancelled() => {}
        }

        Ok(())
    }

    fn identifier(&self) -> String {
        format!("{} (UDP)", self.opts.address)
    }

    fn port(&self) -> u16 {
        self.opts.address.port()
    }
}
