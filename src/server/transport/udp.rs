use std::collections::VecDeque;
use std::sync::Arc;
use std::{io::IoSlice, marker::PhantomData};

use heapless::Deque;
use tokio::net::UdpSocket;
use tracing::debug;

use crate::server::message::ReliabilityHeader;
use crate::server::protocol::{MSG_DATA_BIT_FRAGMENTATION, MSG_DATA_LAST_FRAGMENT_MASK};
use crate::{
    buffers::byte_writer::ByteWriter,
    server::{
        Server,
        app_handler::AppHandler,
        message::{QunetMessage, channel::RawMessageReceiver},
        protocol::{HANDSHAKE_HEADER_SIZE_WITH_QDB, MSG_HANDSHAKE_FINISH},
        transport::{ClientTransportData, TransportError},
    },
};

struct UnackedMessage {
    message_id: u16,
    msg: QunetMessage,
}

pub(crate) struct ClientUdpTransport<H> {
    socket: Arc<UdpSocket>,
    mtu: usize,
    receiver: Option<RawMessageReceiver>,
    next_tx_rid: u16,
    next_tx_fragid: u16,

    ack_queue: Deque<u16, 64>,
    unacked_messages: VecDeque<UnackedMessage>,

    _phantom: PhantomData<H>,
}

impl<H: AppHandler> ClientUdpTransport<H> {
    pub fn new(socket: Arc<UdpSocket>, mtu: usize) -> Self {
        Self {
            socket,
            mtu,
            receiver: None,
            next_tx_rid: 1,
            next_tx_fragid: 1,
            ack_queue: Deque::new(),
            unacked_messages: VecDeque::with_capacity(32),
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn next_reliable_id(&mut self) -> u16 {
        let id = self.next_tx_rid;

        self.next_tx_rid = self.next_tx_rid.wrapping_add(1);
        if self.next_tx_rid == 0 {
            self.next_tx_rid = 1; // avoid zero
        }

        id
    }

    #[inline]
    fn next_fragment_id(&mut self) -> u16 {
        let id = self.next_tx_fragid;

        self.next_tx_fragid = self.next_tx_fragid.wrapping_add(1);

        id
    }

    #[inline]
    fn get_acks<const LIM: usize>(&mut self) -> heapless::Vec<u16, LIM> {
        let mut acks = heapless::Vec::new();

        // drain the queue into the acks vector
        for _ in 0..LIM {
            if let Some(ack) = self.ack_queue.pop_front() {
                let _ = acks.push(ack);
            } else {
                break; // no more acks to process
            }
        }

        acks
    }

    fn add_unacked(&mut self, transport_data: &ClientTransportData<H>, msg: QunetMessage) -> bool {
        if self.unacked_messages.len() >= 64 {
            debug!(
                "[{}] Unacked messages queue is full, dropping oldest message",
                transport_data.address
            );

            self.unacked_messages.pop_front();
        }

        let QunetMessage::Data { reliability, .. } = &msg else {
            unreachable!("add_unacked called with a non-data message");
        };

        let Some(ReliabilityHeader { message_id, .. }) = reliability else {
            panic!("add_unacked called with an unreliable message");
        };

        #[cfg(debug_assertions)]
        debug!(
            "[{}] Adding unacked message with ID {}",
            transport_data.address, message_id
        );

        self.unacked_messages.push_back(UnackedMessage {
            message_id: *message_id,
            msg,
        });

        true
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

    pub async fn receive_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
    ) -> Result<QunetMessage, TransportError> {
        let raw_msg = match &self.receiver {
            Some(r) => match r.recv().await {
                Some(msg) => Ok(msg),
                None => Err(TransportError::MessageChannelClosed),
            },

            None => unreachable!("run_setup was not called before receiving messages"),
        }?;

        Ok(QunetMessage::from_raw_udp_message(raw_msg, &transport_data.server.buffer_pool).await?)
    }

    pub async fn send_message(
        &mut self,
        transport_data: &ClientTransportData<H>,
        mut msg: QunetMessage,
        reliable: bool,
    ) -> Result<(), TransportError> {
        const MAX_HEADER_SIZE: usize = 1 + 4 + 20 + 8; // qunet, compression, reliability, fragmentation headers

        let mut header_buf = [0u8; MAX_HEADER_SIZE];
        let mut header_writer = ByteWriter::new(&mut header_buf);

        if !msg.is_data() {
            let mut body_buf = [0u8; 256];
            let mut body_writer = ByteWriter::new(&mut body_buf);

            msg.encode_control_msg(&mut header_writer, &mut body_writer)?;

            let mut vecs = [
                IoSlice::new(header_writer.written()),
                IoSlice::new(body_writer.written()),
            ];

            self.send_packet_vectored(&mut vecs, transport_data).await?;

            return Ok(());
        }

        // handle data messages

        let mut rel_hdr_size = 0;
        let comp_hdr_size = if msg.is_data_compressed() { 4usize } else { 0 };

        if reliable {
            let QunetMessage::Data { reliability, .. } = &mut msg else {
                unreachable!();
            };

            let message_id = self.next_reliable_id();
            let acks = self.get_acks();

            rel_hdr_size = 4 + acks.len() * 2;

            *reliability = Some(ReliabilityHeader { message_id, acks });
        }

        let Some(data) = msg.data_bytes() else {
            unreachable!()
        };

        // decide if the message needs to be fragmented.

        let unfrag_total_size = header_writer.pos() + rel_hdr_size + comp_hdr_size + data.len();

        if unfrag_total_size <= self.mtu {
            // no fragmentation :)

            msg.encode_data_header(&mut header_writer, false).unwrap();

            let mut vecs = [IoSlice::new(header_writer.written()), IoSlice::new(data)];
            self.send_packet_vectored(&mut vecs, transport_data).await?;

            if reliable {
                self.add_unacked(transport_data, msg);
            }

            return Ok(());
        }

        // fragmentation is needed :(

        // determine the maximum size of the payload for each fragment
        // first fragment must include reliability and compression headers if they are present, rest don't have to

        let frag_hdr_size = 4;

        let first_payload_size =
            self.mtu - header_writer.pos() - rel_hdr_size - comp_hdr_size - frag_hdr_size;
        let rest_payload_size = self.mtu - header_writer.pos() - frag_hdr_size;

        let frag_message_id = self.next_fragment_id();

        let mut offset = 0usize;
        let mut fragment_index = 0u16;

        while offset < data.len() {
            let is_first = fragment_index == 0;
            let payload_size = if is_first {
                first_payload_size
            } else {
                rest_payload_size
            };

            let chunk = &data[offset..(offset + payload_size).min(data.len())];

            // write the header
            let mut header_buf = [0u8; MAX_HEADER_SIZE];
            let mut header_writer = ByteWriter::new(&mut header_buf);

            // omit headers for all but the first fragment
            msg.encode_data_header(&mut header_writer, !is_first)
                .unwrap();

            // ... but add the fragmentation header!
            header_buf[0] &= 1u8 << MSG_DATA_BIT_FRAGMENTATION;

            // this reinit is scuffed but needed
            let mut header_writer = ByteWriter::new(&mut header_buf);
            header_writer.write_u16(frag_message_id);
            header_writer.write_u16(
                fragment_index
                    | if is_first {
                        0
                    } else {
                        MSG_DATA_LAST_FRAGMENT_MASK
                    },
            );

            offset += chunk.len();
            fragment_index += 1;

            let mut vecs = [IoSlice::new(header_writer.written()), IoSlice::new(chunk)];

            self.send_packet_vectored(&mut vecs, transport_data).await?;
        }

        if reliable {
            self.add_unacked(transport_data, msg);
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

                let mut iovecs = [
                    IoSlice::new(header_writer.written()),
                    IoSlice::new(qdb_data),
                ];

                self.send_packet_vectored(&mut iovecs, transport_data)
                    .await?;
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
            self.send_packet(header_writer.written(), transport_data)
                .await?;
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

            self.send_packet_vectored(&mut iovecs, transport_data)
                .await?;

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
            assert!(
                total_len <= self.mtu,
                "Data exceeds MTU size: {} > {}",
                total_len,
                self.mtu
            );
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
