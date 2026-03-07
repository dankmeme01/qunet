use std::{
    io,
    net::SocketAddr,
    os::fd::AsRawFd,
    sync::{Arc, OnceLock},
};

use cfg_if::cfg_if;
use tokio::{net::UdpSocket, task::JoinHandle};

use crate::{
    buffers::{BufPool, BufferPool},
    message::BufferKind,
};

#[cfg(target_os = "linux")]
use crate::transport::lowlevel::{SocketAddrCRepr, socket_addr_to_c};

#[cfg(target_os = "linux")]
const CAN_BATCH_SEND: bool = true;
#[cfg(not(target_os = "linux"))]
const CAN_BATCH_SEND: bool = false;

// Kind of arbitrary, taken by measuring traffic of the globed server
// and seeing that the avg packet is ~145 bytes and stddev is 127
const MAX_SIZE_TO_BATCH: usize = 320;

#[cfg(target_os = "linux")]
type BatchedMsg = (BufferKind, SocketAddrCRepr, u32);

/// A wrapper over a `UdpSocket` that provides extra functionality, for high performance servers and for convenience.
/// More specifically:
/// - Automatic batching of smaller messages (Linux only) via the `sendmmsg` syscall, using a worker task
/// - Easy way of doing scatter-gather IO with `sendmsg` and `recvmsg` syscalls
/// - Cross-platform and has fallbacks for platforms that don't support the above features
pub struct UdpSocketExt {
    socket: UdpSocket,
    worker_task: OnceLock<JoinHandle<()>>,
    pool: BufferPool,
    #[cfg(target_os = "linux")]
    tx: flume::Sender<BatchedMsg>,
}

impl UdpSocketExt {
    pub fn create(socket: UdpSocket, batching: bool) -> Arc<Self> {
        #[cfg(target_os = "linux")]
        let (tx, rx) = flume::bounded(4096);

        let this = Arc::new(Self {
            socket,
            worker_task: OnceLock::new(),
            pool: BufferPool::new(MAX_SIZE_TO_BATCH, 64, 2048),
            #[cfg(target_os = "linux")]
            tx,
        });

        #[cfg(target_os = "linux")]
        if batching {
            let _ = this.worker_task.set(tokio::spawn(this.clone().run_loop(rx)));
        }

        this
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(buf).await
    }

    pub async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            if self.should_batch(data.len()) {
                let mut buf = self.pool.get_or_heap(data.len());
                let success = buf.append_bytes(data);
                debug_assert!(success);

                let (sa, sl) = socket_addr_to_c(&target);
                let _ = self.tx.send_async((buf, sa, sl)).await;
                return Ok(());
            }
        }

        self.socket.send_to(data, target).await?;
        Ok(())
    }

    pub async fn send_to_vectored(
        &self,
        data: &mut [io::IoSlice<'_>],
        target: SocketAddr,
    ) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let total_len: usize = data.iter().map(|slice| slice.len()).sum();

            if self.should_batch(total_len) {
                let buf = self.gather_into_buf(data, total_len);

                let (sa, sl) = socket_addr_to_c(&target);
                let _ = self.tx.send_async((buf, sa, sl)).await;
                return Ok(());
            }
        }

        vectored_send(&self.socket, data, target).await?;
        Ok(())
    }

    fn gather_into_buf(&self, data: &mut [io::IoSlice<'_>], len: usize) -> BufferKind {
        let mut buf = self.pool.get_or_heap(len);
        for slice in data {
            let success = buf.append_bytes(slice);
            debug_assert!(success);
        }
        buf
    }

    fn should_batch(&self, size: usize) -> bool {
        CAN_BATCH_SEND && self.worker_task.get().is_some() && size <= MAX_SIZE_TO_BATCH
    }

    #[cfg(target_os = "linux")]
    async fn run_loop(self: Arc<Self>, rx: flume::Receiver<BatchedMsg>) {
        use std::time::Duration;

        const BATCH_SIZE: usize = 64;
        const MAX_DELAY: Duration = Duration::from_millis(2);

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut mmsg_batch: Vec<MMsgHdr> = Vec::with_capacity(BATCH_SIZE);
        let mut tmp_iovs: Vec<IoVec> = Vec::with_capacity(BATCH_SIZE);

        loop {
            let timeout = tokio::time::sleep(MAX_DELAY);

            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    biased;

                    Ok(pkt) = rx.recv_async() => {
                        batch.push(pkt);
                        if batch.len() >= BATCH_SIZE {
                            break;
                        }
                    }

                    _ = &mut timeout => break,
                }
            }

            while batch.len() < BATCH_SIZE
                && let Ok(pkt) = rx.try_recv()
            {
                batch.push(pkt);
            }

            self.flush(&mut batch, &mut mmsg_batch, &mut tmp_iovs).await;
            batch.clear();
        }
    }

    #[cfg(target_os = "linux")]
    async fn flush(
        &self,
        batch: &mut [BatchedMsg],
        mmsg_batch: &mut Vec<MMsgHdr>,
        tmp_iovs: &mut Vec<IoVec>,
    ) {
        if batch.is_empty() {
            return;
        }

        tracing::trace!("flushing batch of {} udp packets", batch.len());

        for (buf, sockaddr, socklen) in batch.iter() {
            debug_assert!(tmp_iovs.len() < tmp_iovs.capacity());
            tmp_iovs.push(IoVec {
                iov_base: buf.as_ptr() as *mut _,
                iov_len: buf.len(),
            });
            let iov = tmp_iovs.last().unwrap();

            let header = libc::msghdr {
                msg_name: sockaddr as *const SocketAddrCRepr as *mut libc::c_void,
                msg_namelen: *socklen as libc::socklen_t,
                msg_iov: iov as *const _ as *mut libc::iovec,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            };

            mmsg_batch.push(MMsgHdr { msg_hdr: header, msg_len: 0 });
        }

        // sendmmsg may not send everything at once, so loop until the request is satisfied
        let mut start_idx = 0;
        while start_idx < mmsg_batch.len() {
            match vectored_msend(&self.socket, &mut mmsg_batch[start_idx..]).await {
                Ok(n) => {
                    start_idx += n;
                }

                Err(e) => {
                    tracing::error!("Error in vectored send: {e}");
                    break;
                }
            }
        }

        tmp_iovs.clear();
        mmsg_batch.clear();
    }

    #[cfg(not(target_os = "linux"))]
    async fn run_loop(self: Arc<Self>, _rx: flume::Receiver<BatchedMsg>) {
        panic!("run_loop should never be called on non-linux platforms");
    }
}

#[cfg(unix)]
async fn vectored_send(
    socket: &UdpSocket,
    data: &mut [io::IoSlice<'_>],
    target: SocketAddr,
) -> io::Result<()> {
    use tokio::io::Interest;

    let (sockaddr, socklen) = socket_addr_to_c(&target);

    socket
        .async_io(Interest::WRITABLE, || unsafe {
            let mut header: libc::msghdr = std::mem::zeroed();

            header.msg_name = &sockaddr as *const _ as *mut libc::c_void;
            header.msg_namelen = socklen as libc::socklen_t;
            header.msg_iov = data.as_ptr() as *mut libc::iovec;
            header.msg_iovlen = data.len() as _;
            header.msg_control = std::ptr::null_mut();
            header.msg_controllen = 0;
            header.msg_flags = 0;

            let status = libc::sendmsg(socket.as_raw_fd(), &header as *const libc::msghdr, 0);

            match status {
                -1 => Err(io::Error::last_os_error()),
                _ => Ok(()),
            }
        })
        .await
}

#[cfg(not(unix))]
async fn vectored_send(
    socket: &UdpSocket,
    data: &mut [io::IoSlice<'_>],
    target: SocketAddr,
) -> io::Result<()> {
    // assert that we have an upper limit on the packet size
    const PACKET_LIMIT: usize = 1500;

    let mut buf = [0u8; 1500];
    let mut pos = 0;

    for slice in data {
        buf[pos..pos + slice.len()].copy_from_slice(slice);
        pos += slice.len();
    }

    socket.send_to(&buf[..pos], target).await?;

    Ok(())
}

#[cfg(target_os = "linux")]
/// Attempts a vectored send of multiple messages, returns how many messages were actually sent.
async fn vectored_msend(socket: &UdpSocket, data: &mut [MMsgHdr]) -> io::Result<usize> {
    use tokio::io::Interest;

    socket
        .async_io(Interest::WRITABLE, || unsafe {
            let status = libc::sendmmsg(
                socket.as_raw_fd(),
                data.as_mut_ptr() as *mut libc::mmsghdr,
                data.len() as u32,
                0,
            );

            match status {
                -1 => Err(io::Error::last_os_error()),
                n => Ok(n as usize),
            }
        })
        .await
}

cfg_if! {
    if #[cfg(target_os = "linux")] {
        #[repr(C)]
        struct MMsgHdr {
            msg_hdr: libc::msghdr,
            msg_len: libc::c_uint,
        }

        unsafe impl Send for MMsgHdr {}
        unsafe impl Sync for MMsgHdr {}

        #[cfg(target_os = "linux")]
        #[repr(C)]
        struct IoVec {
            iov_base: *mut libc::c_void,
            iov_len: libc::size_t,
        }

        unsafe impl Send for IoVec {}
        unsafe impl Sync for IoVec {}
    }
}
