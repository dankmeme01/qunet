use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{io::IoSlice, marker::PhantomData};

use tokio::net::UdpSocket;
use tracing::debug;

use crate::server::message::{BufferKind, DataMessageKind};
use crate::server::transport::udp_misc::FragmentStore;
use crate::{
    buffers::byte_writer::ByteWriter,
    server::{
        Server,
        app_handler::AppHandler,
        message::{QunetMessage, ReliabilityHeader, channel::RawMessageReceiver},
        protocol::*,
        transport::{ClientTransportData, TransportError},
    },
};

use super::udp_misc::ReliableStore;

const MAX_HEADER_SIZE: usize = 1 + 4 + 20 + 8; // qunet, compression, reliability, fragmentation headers

pub(crate) struct ClientUdpTransport<H> {
    socket: Arc<UdpSocket>,
    mtu: usize,
    receiver: Option<RawMessageReceiver>,
    rel_store: ReliableStore,
    frag_store: FragmentStore,
    idle_timeout: Duration,
    last_data_exchange: Instant,

    _phantom: PhantomData<H>,
}

impl<H: AppHandler> ClientUdpTransport<H> {
    pub fn new(socket: Arc<UdpSocket>, mtu: usize, server: &Server<H>) -> Self {
        Self {
            socket,
            mtu,
            receiver: None,
            rel_store: ReliableStore::new(),
            frag_store: FragmentStore::new(),
            idle_timeout: server._builder.listener_opts.idle_timeout,
            last_data_exchange: Instant::now(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub async fn run_setup(
        &mut self,
        transport_data: &ClientTransportData<H>,
        server: &Server<H>,
    ) -> Result<(), TransportError> {
        self.receiver = Some(server.create_udp_route(transport_data.connection_id));
        Ok(())
    }

    #[inline]
    pub async fn run_cleanup(
        &mut self,
        transport_data: &ClientTransportData<H>,
        server: &Server<H>,
    ) -> Result<(), TransportError> {
        server.remove_udp_route(transport_data.connection_id);
        Ok(())
    }

    #[inline]
    pub fn until_timer_expiry(&self) -> Duration {
        let idle_timeout = self.idle_timeout.saturating_sub(self.last_data_exchange.elapsed());

        self.rel_store.until_timer_expiry().min(idle_timeout)
    }

    #[inline]
    fn update_exchange_time(&mut self) {
        self.last_data_exchange = Instant::now();
    }

    #[inline]
    pub async fn receive_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
    ) -> Result<QunetMessage, TransportError> {
        if let Some(msg) = self.rel_store.pop_delayed_message() {
            // we have a delayed message, return it
            return Ok(msg);
        }

        loop {
            if let Some(msg) = self.process_incoming(transport_data).await? {
                self.update_exchange_time();
                break Ok(msg);
            }
        }
    }

    async fn process_incoming(
        &mut self,
        transport_data: &ClientTransportData<H>,
    ) -> Result<Option<QunetMessage>, TransportError> {
        let raw_msg = match &self.receiver {
            Some(r) => match r.recv().await {
                Some(msg) => Ok(msg),
                None => Err(TransportError::MessageChannelClosed),
            },

            None => unreachable!("run_setup was not called before receiving messages"),
        }?;

        let mut message =
            QunetMessage::from_raw_udp_message(raw_msg, &transport_data.server.buffer_pool)?;

        if !message.is_data() {
            // control messages don't need special handling
            return Ok(Some(message));
        }

        // handle fragmented / reliable messages
        let QunetMessage::Data { kind, .. } = &message else {
            unreachable!();
        };

        if kind.is_fragment() {
            match self.frag_store.process_fragment(message, &transport_data.server.buffer_pool)? {
                None => {
                    // not enough fragments yet, return None
                    return Ok(None);
                }

                // otherwise, we have a complete message and we can keep processing it
                Some(msg) => message = msg,
            }
        }

        let QunetMessage::Data { reliability, .. } = &message else {
            unreachable!();
        };

        if reliability.is_some() {
            match self.rel_store.handle_incoming(message)? {
                None => {
                    // duplicate or otherwise invalid message, return None
                    return Ok(None);
                }

                Some(msg) => message = msg,
            }
        }

        // if this is a data message with no data, don't return it
        if message.data_bytes().expect("non-data message").is_empty() {
            return Ok(None);
        }

        Ok(Some(message))
    }

    pub async fn send_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
        mut msg: QunetMessage,
        reliable: bool,
    ) -> Result<(), TransportError> {
        self.update_exchange_time();

        if !msg.is_data() {
            let mut header_buf = [0u8; MAX_HEADER_SIZE];
            let mut header_writer = ByteWriter::new(&mut header_buf);

            let mut body_buf = [0u8; 256];
            let mut body_writer = ByteWriter::new(&mut body_buf);

            msg.encode_control_msg(&mut header_writer, &mut body_writer)?;

            let mut vecs =
                [IoSlice::new(header_writer.written()), IoSlice::new(body_writer.written())];

            self.send_packet_vectored(&mut vecs, transport_data).await?;

            return Ok(());
        }

        // handle data messages

        let mut rel_header = ReliabilityHeader::default();
        self.rel_store.set_outgoing_acks(&mut rel_header);

        if reliable {
            rel_header.message_id = self.rel_store.next_message_id();
        }

        // only assign the reliability header if the message is reliable or if there's any acks to send
        if reliable || !rel_header.acks.is_empty() {
            let QunetMessage::Data { reliability, .. } = &mut msg else {
                unreachable!();
            };

            *reliability = Some(rel_header);
        }

        self.do_send_prefrag_data(msg, transport_data).await?;

        Ok(())
    }

    async fn do_send_prefrag_data(
        &mut self,
        message: QunetMessage,
        transport_data: &ClientTransportData<H>,
    ) -> Result<Option<QunetMessage>, TransportError> {
        let is_reliable = message.is_reliable_message();
        let data = message.data_bytes().expect("non-data message");

        // decide if the message needs to be fragmented.

        let unfrag_total_size = message.calc_header_size() + data.len();

        if unfrag_total_size <= self.mtu {
            // no fragmentation :)
            self.do_send_unfrag_data(&message, transport_data).await?;

            if is_reliable {
                self.rel_store.push_local_unacked(message)?;
                return Ok(None);
            } else {
                return Ok(Some(message));
            }
        }

        // fragmentation is needed :(
        self.do_fragment_and_send(&message, transport_data).await?;

        if is_reliable {
            self.rel_store.push_local_unacked(message)?;
            Ok(None)
        } else {
            Ok(Some(message))
        }
    }

    async fn do_send_prefrag_retrans_data(
        &self,
        message: &QunetMessage,
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        let data = message.data_bytes().expect("non-data message");

        // decide if the message needs to be fragmented.

        let unfrag_total_size = message.calc_header_size() + data.len();

        if unfrag_total_size <= self.mtu {
            // no fragmentation :)
            self.do_send_unfrag_data(message, transport_data).await?;

            return Ok(());
        }

        // fragmentation is needed :(
        self.do_fragment_and_send(message, transport_data).await?;

        Ok(())
    }

    async fn do_send_unfrag_data(
        &self,
        message: &QunetMessage,
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        let mut header_buf = [0u8; MAX_HEADER_SIZE];
        let mut header_writer = ByteWriter::new(&mut header_buf);

        message.encode_data_header(&mut header_writer, false).unwrap();

        let mut vecs = [
            IoSlice::new(header_writer.written()),
            IoSlice::new(message.data_bytes().expect("non-data message")),
        ];

        self.send_packet_vectored(&mut vecs, transport_data).await?;

        Ok(())
    }

    async fn do_fragment_and_send(
        &self,
        message: &QunetMessage,
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        // TODO: rewrite a linux-only version of this function that uses sendmmsg, potentially use some static vec for that in Server?

        let data = message.data_bytes().expect("non-data message");

        // determine the maximum size of the payload for each fragment
        // first fragment must include reliability and compression headers if they are present, rest don't have to

        let frag_hdr_size = 4;
        let first_payload_size = self.mtu - message.calc_header_size() - frag_hdr_size;
        let rest_payload_size = self.mtu - 1 - frag_hdr_size;

        let frag_message_id = self.frag_store.next_message_id();

        let mut offset = 0usize;
        let mut fragment_index = 0u16;

        while offset < data.len() {
            let is_first = fragment_index == 0;
            let payload_size = if is_first { first_payload_size } else { rest_payload_size };

            let is_last = offset + payload_size >= data.len();

            let chunk = &data[offset..(offset + payload_size).min(data.len())];

            // write the header, omit headers for all but the first fragment
            let mut header_buf = [0u8; MAX_HEADER_SIZE];
            let mut header_writer = ByteWriter::new(&mut header_buf);
            message.encode_data_header(&mut header_writer, !is_first).unwrap();

            let prev_pos = header_writer.pos();

            // ... but add the fragmentation header!
            header_buf[0] |= MSG_DATA_FRAGMENTATION_MASK;

            // this reinit is scuffed but needed
            let mut header_writer = ByteWriter::new(&mut header_buf);
            header_writer.set_pos(prev_pos);
            header_writer.write_u16(frag_message_id);
            header_writer
                .write_u16(fragment_index | if is_last { MSG_DATA_LAST_FRAGMENT_MASK } else { 0 });

            offset += chunk.len();
            fragment_index += 1;

            let mut vecs = [IoSlice::new(header_writer.written()), IoSlice::new(chunk)];

            self.send_packet_vectored(&mut vecs, transport_data).await?;
        }

        debug!("Sent fragmented message (ID: {}, fragments: {})", frag_message_id, fragment_index);

        Ok(())
    }

    pub async fn handle_timer_expiry(
        &mut self,
        transport_data: &mut ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        // if there's been no activity for a while, close the connection
        if self.last_data_exchange.elapsed() >= self.idle_timeout {
            debug!("Idle timeout reached, closing connection");
            transport_data.closed = true;
            return Ok(());
        }

        while self.rel_store.maybe_retransmit()? {
            match self.rel_store.get_retransmit_message() {
                Some(msg) => self.do_send_prefrag_retrans_data(msg, transport_data).await?,
                None => break,
            };
        }

        // if we have not sent any data messages recently that we could tag acks onto,
        // we might need to send an explicit ack message with no data

        if self.rel_store.has_urgent_outgoing_acks() {
            let mut header = ReliabilityHeader::default();
            self.rel_store.set_outgoing_acks(&mut header);

            debug_assert!(!header.acks.is_empty(), "Urgent outgoing acks should not be empty");

            let msg = QunetMessage::Data {
                kind: DataMessageKind::Regular {
                    data: BufferKind::Heap(Vec::new()),
                },
                reliability: Some(header),
                compression: None,
            };

            self.do_send_prefrag_data(msg, transport_data).await?;
        }

        Ok(())
    }

    pub async fn send_handshake_response(
        &self,
        transport_data: &ClientTransportData<H>,
        qdb_data: Option<&[u8]>,
        qdb_uncompressed_size: usize,
    ) -> Result<(), TransportError> {
        let max_chunk_size = self.mtu - HANDSHAKE_HEADER_SIZE_WITH_QDB;

        let mut header_buf = [0u8; HANDSHAKE_HEADER_SIZE_WITH_QDB];
        let mut header_writer = ByteWriter::new(&mut header_buf);

        header_writer.write_u8(MSG_HANDSHAKE_FINISH);
        header_writer.write_u64(transport_data.connection_id);
        header_writer.write_bool(qdb_data.is_some());

        if let Some(qdb_data) = qdb_data {
            header_writer.write_u32(qdb_uncompressed_size as u32);
            header_writer.write_u32(qdb_data.len() as u32);

            // if it fits into a single packet, send directly
            if qdb_data.len() <= max_chunk_size {
                debug!(
                    "Sending UDP handshake response (single chunk QDB, {} bytes)",
                    qdb_data.len()
                );

                // offset and size set to 0 and size
                header_writer.write_u32(0);
                header_writer.write_u32(qdb_data.len() as u32);

                let mut iovecs = [IoSlice::new(header_writer.written()), IoSlice::new(qdb_data)];

                self.send_packet_vectored(&mut iovecs, transport_data).await?;
            } else {
                debug!(
                    "Sending UDP handshake response (fragmented QDB, {} bytes, chunk size {})",
                    qdb_data.len(),
                    max_chunk_size
                );

                self.send_fragmented_qdb(
                    &mut header_writer,
                    qdb_data,
                    max_chunk_size,
                    transport_data,
                )
                .await?;
            }
        } else {
            debug!("Sending UDP handshake response (no QDB)");

            // no qdb
            self.send_packet(header_writer.written(), transport_data).await?;
        }

        Ok(())
    }

    /// Sends the QDB to the client in fragments, assumes that the header has been written up to the
    /// offset and size fields
    async fn send_fragmented_qdb(
        &self,
        header_writer: &mut ByteWriter<'_>,
        data: &[u8],
        chunk_size: usize,
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        // TODO: rewrite a linux-only version of this function that uses sendmmsg, potentially use some static vec for that in Server?

        let mut offset = 0;
        let mut remaining = data.len();

        let writer_start_pos = header_writer.pos();

        while remaining > 0 {
            let chunk_len = remaining.min(chunk_size);

            // write offset and size
            header_writer.write_u32(offset as u32);
            header_writer.write_u32(chunk_len as u32);

            // create and send ioslices
            let mut iovecs = [
                IoSlice::new(header_writer.written()),
                IoSlice::new(&data[offset..offset + chunk_len]),
            ];

            self.send_packet_vectored(&mut iovecs, transport_data).await?;

            // update offset and remaining
            offset += chunk_len;
            remaining -= chunk_len;

            // reset header writer position
            header_writer.set_pos(writer_start_pos);
        }

        Ok(())
    }

    async fn send_packet(
        &self,
        data: &[u8],
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        let _ = self.socket.send_to(data, transport_data.address).await?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn send_packet_vectored(
        &self,
        data: &mut [IoSlice<'_>],
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        use std::os::fd::AsRawFd;
        use tokio::io::Interest;

        #[cfg(debug_assertions)]
        {
            let total_len = data.iter().map(|x| x.len()).sum::<usize>();
            assert!(total_len <= self.mtu, "Data exceeds MTU size: {} > {}", total_len, self.mtu);
        }

        let (sockaddr, socklen) = transport_data.c_sockaddr();

        self.socket
            .async_io(Interest::WRITABLE, || unsafe {
                let header = libc::msghdr {
                    msg_name: sockaddr as *const _ as *mut libc::c_void,
                    msg_namelen: socklen as libc::socklen_t,
                    msg_iov: data.as_ptr() as *mut libc::iovec,
                    msg_iovlen: data.len() as libc::size_t,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                };

                let status =
                    libc::sendmsg(self.socket.as_raw_fd(), &header as *const libc::msghdr, 0);

                match status {
                    -1 => Err(std::io::Error::last_os_error()),
                    _ => Ok(()),
                }
            })
            .await?;

        Ok(())
    }

    // TODO: we have an upper limit on packet size, we don't have to allocate here
    #[cfg(not(target_os = "linux"))]
    async fn send_packet_vectored(
        &self,
        data: &mut [IoSlice<'_>],
        transport_data: &ClientTransportData<H>,
    ) -> Result<(), TransportError> {
        let total_len: usize = data.iter().map(|slice| slice.len()).sum();
        debug_assert!(total_len <= self.mtu, "Data exceeds MTU size");

        let mut out_buf = Vec::with_capacity(total_len);

        for slice in data {
            super::lowlevel::append_to_vec(&mut out_buf, slice.as_slice());
        }

        self.send_packet(&out_buf, transport_data).await?;
    }
}
