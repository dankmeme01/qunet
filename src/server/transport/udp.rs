use std::io::IoSlice;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tracing::{debug, warn};

use crate::{
    buffers::byte_writer::ByteWriter,
    server::{
        Server,
        message::{QunetMessage, channel::RawMessageReceiver},
        protocol::{HANDSHAKE_HEADER_SIZE_WITH_QDB, MSG_HANDSHAKE_FINISH},
        transport::{ClientTransportData, TransportError},
    },
};

pub(crate) struct ClientUdpTransport {
    socket: Arc<UdpSocket>,
    mtu: usize,
    receiver: Option<RawMessageReceiver>,
}

impl ClientUdpTransport {
    pub fn new(socket: Arc<UdpSocket>, mtu: usize) -> Self {
        Self {
            socket,
            mtu,
            receiver: None,
        }
    }

    #[inline]
    pub async fn run_setup(
        &mut self,
        transport_data: &ClientTransportData,
        server: &Server,
    ) -> Result<(), TransportError> {
        self.receiver = Some(server.create_udp_route(transport_data.connection_id));
        Ok(())
    }

    #[inline]
    pub async fn run_cleanup(
        &mut self,
        transport_data: &ClientTransportData,
        server: &Server,
    ) -> Result<(), TransportError> {
        server.remove_udp_route(transport_data.connection_id);
        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        transport_data: &ClientTransportData,
    ) -> Result<QunetMessage, TransportError> {
        let raw_msg = match &self.receiver {
            Some(r) => match r.recv().await {
                Some(msg) => Ok(msg),
                None => Err(TransportError::MessageChannelClosed),
            },

            None => unreachable!("run_setup was not called before receiving messages"),
        }?;

        Ok(QunetMessage::from_raw_udp_message(raw_msg, &transport_data.buffer_pool).await?)
    }

    pub async fn send_message(
        &mut self,
        transport_data: &ClientTransportData,
        msg: &QunetMessage,
    ) -> Result<(), TransportError> {
        warn!(
            "[{}] Unimplemented, sending message: {}",
            transport_data.address,
            msg.type_str()
        );

        Ok(())
    }

    pub async fn send_handshake_response(
        &self,
        transport_data: &ClientTransportData,
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
        transport_data: &ClientTransportData,
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
        transport_data: &ClientTransportData,
    ) -> Result<(), TransportError> {
        let _ = self.socket.send_to(data, transport_data.address).await?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn send_packet_vectored(
        &self,
        data: &mut [IoSlice<'_>],
        transport_data: &ClientTransportData,
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
        transport_data: &ClientTransportData,
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
