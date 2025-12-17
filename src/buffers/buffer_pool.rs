use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::message::BufferKind;

struct BufferPoolInner {
    rx: flume::Receiver<Box<[u8]>>,
    tx: flume::Sender<Box<[u8]>>,
    allocated_buffers: AtomicUsize,
}

pub struct BufferPool {
    buf_size: usize,
    max_buffers: usize,
    inner: Arc<BufferPoolInner>,
}

pub struct BufferPoolStats {
    pub buf_size: usize,
    pub heap_usage: usize,
    pub allocated_buffers: usize,
    pub max_buffers: usize,
    pub avail_buffers: usize,
}

impl BufferPoolInner {
    /// Atomically returns a buffer to the pool
    #[inline]
    fn return_buffer(&self, buffer: Box<[u8]>) {
        // this unwrap must never fail, as BufferPool ensures we cannot create more buffers than the max
        self.tx.try_send(buffer).unwrap();

        // // if less than half the buffers are actually used, automatically shrink the pool
        // let allocated = self.allocated_buffers.load(Ordering::Relaxed);
        // if storage.len() <= allocated / 2 {
        //     self.shrink();
        // }
    }

    fn shrink(&self) {
        let released_bufs = self.rx.drain().count();

        // decrease the allocated buffers count
        let old_bufs = self.allocated_buffers.fetch_sub(released_bufs, Ordering::AcqRel);

        // check for potential overflow, should not be possible
        debug_assert!(old_bufs.checked_sub(released_bufs).is_some());
    }

    fn try_get(&self) -> Option<Box<[u8]>> {
        self.rx.try_recv().ok()
    }

    async fn get(&self) -> Box<[u8]> {
        self.rx.recv_async().await.unwrap()
    }
}

impl BufferPool {
    /// Creates a new `BufferPool` with the specified buffer size, initial number of buffers, and maximum number of buffers.
    /// The minimum memory usage of the pool will be `buf_size * initial_buffers`.
    /// The maximum memory usage of the pool will be `buf_size * max_buffers`.
    pub fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        assert!(max_buffers > 0, "max_buffers in a buffer pool cannot be 0");
        assert!(initial_buffers <= max_buffers, "initial_buffers must be <= max_buffers");

        let (tx, rx) = flume::bounded(max_buffers);

        for _ in 0..initial_buffers {
            let buffer = vec![0u8; buf_size].into_boxed_slice();
            tx.try_send(buffer).unwrap();
        }

        let inner = BufferPoolInner {
            rx,
            tx,
            allocated_buffers: AtomicUsize::new(initial_buffers),
        };

        Self {
            buf_size,
            max_buffers,
            inner: Arc::new(inner),
        }
    }

    /// Allocates and returns a new buffer.
    /// It is the caller's responsibility to not call this function when the limit of buffers is reached,
    /// and to increment the `allocated_buffers` count accordingly.
    #[inline]
    fn alloc_new_buffer(&self) -> Box<[u8]> {
        vec![0u8; self.buf_size].into_boxed_slice()
    }

    /// Like `get`, but without needing to pass the size.
    pub async fn get_unchecked(&self) -> PooledBuffer {
        if let Some(buffer) = self.try_get() {
            return buffer;
        }

        // if we reached here, this means the pool is at max capacity and no buffers are available right now,
        // we must wait

        let buffer = self.inner.get().await;

        PooledBuffer::new(buffer, self.inner.clone())
    }

    #[inline]
    fn try_get_no_grow(&self) -> Option<PooledBuffer> {
        self.inner.try_get().map(|buf| PooledBuffer::new(buf, self.inner.clone()))
    }

    pub fn try_get(&self) -> Option<PooledBuffer> {
        // first, try acquire a buffer from the pool if there are any available
        if let Some(buf) = self.try_get_no_grow() {
            return Some(buf);
        }

        // if we failed to acquire, see if we can grow the pool
        let mut num_buffers = self.inner.allocated_buffers.load(Ordering::Acquire);

        loop {
            if num_buffers >= self.max_buffers {
                break None;
            }

            // try to grow the pool
            match self.inner.allocated_buffers.compare_exchange_weak(
                num_buffers,
                num_buffers + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // successfully incremented the count, allocate a new buffer
                    let buffer = self.alloc_new_buffer();

                    break Some(PooledBuffer::new(buffer, self.inner.clone()));
                }

                Err(num) => {
                    // another thread has beaten us, let's try again if we can
                    num_buffers = num;
                    continue;
                }
            }
        }
    }

    /// Returns the size of a single buffer in this pool.
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Returns the total heap usage of this pool. (only including buffers, not the pool itself)
    #[inline]
    pub fn heap_usage(&self) -> usize {
        self.buf_size * self.inner.allocated_buffers.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> BufferPoolStats {
        let allocated = self.inner.allocated_buffers.load(Ordering::Relaxed);
        let max = self.max_buffers;
        let available = self.inner.rx.len();

        BufferPoolStats {
            buf_size: self.buf_size,
            heap_usage: self.heap_usage(),
            allocated_buffers: allocated,
            max_buffers: max,
            avail_buffers: available,
        }
    }
}

pub struct PooledBuffer {
    // ManuallyDrop used because we cant move out the buffer in Drop, to avoid using an Option
    buffer: ManuallyDrop<Box<[u8]>>,
    pool: Arc<BufferPoolInner>,
    // #[cfg(debug_assertions)]
    // created_at: std::time::Instant,
}

impl PooledBuffer {
    fn new(buffer: Box<[u8]>, pool: Arc<BufferPoolInner>) -> Self {
        Self {
            buffer: ManuallyDrop::new(buffer),
            pool,
            // #[cfg(debug_assertions)]
            // created_at: std::time::Instant::now(),
        }
    }
}

impl Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        // return the buffer to the pool

        // #[cfg(debug_assertions)]
        // {
        //     let elapsed = self.created_at.elapsed();
        //     tracing::trace!(
        //         "PooledBuffer (size {}, free pool bufs: {}) returned after {:?}",
        //         self.buffer.len(),
        //         self.pool.semaphore.available_permits(),
        //         elapsed
        //     );
        // }

        // safety: `self.buffer` is never used after this point
        let buf = unsafe { ManuallyDrop::take(&mut self.buffer) };
        self.pool.return_buffer(buf);
    }
}

pub trait BufPool: Send + Sync + 'static {
    fn get(&self, size: usize) -> impl Future<Output = Option<PooledBuffer>> + Send;
    /// Similar to `get`, but is synchronous, instead of waiting it will heap allocate a new buffer if the pool is at max capacity.
    fn get_or_heap(&self, size: usize) -> BufferKind;

    /// Attempts to returns a new buffer of at least `size` bytes without blocking.
    /// If no pool is large enough or no buffers are available in the chosen pool, this will return `None`.
    fn try_get(&self, size: usize) -> Option<PooledBuffer>;

    /// Returns the size of the smallest buffer in the pool.
    fn min_buf_size(&self) -> usize;
    /// Returns the size of the largest buffer in the pool.
    fn max_buf_size(&self) -> usize;

    /// Shrinks the pool to the smallest possible size, releasing the excess buffers.
    /// This will free up memory, but subsequent calls to `get` or `try_get` may have to allocate new buffers again.
    fn shrink(&self);

    /// Returns the total heap usage of all the buffer pools. (only including buffers, not the pool itself or any inner pools)
    fn heap_usage(&self) -> usize;

    /// Returns whether it is possible to allocate a buffer of at least `size` bytes in the pool.
    fn can_allocate(&self, size: usize) -> bool {
        size <= self.max_buf_size()
    }
}

impl BufPool for BufferPool {
    /// Returns a future which will return a new buffer of at least `size` bytes, as soon as one is available.
    /// If no pool is large enough, this will return `None`.
    async fn get(&self, size: usize) -> Option<PooledBuffer> {
        if size > self.buf_size {
            return None;
        }

        Some(self.get_unchecked().await)
    }

    fn try_get(&self, size: usize) -> Option<PooledBuffer> {
        if size > self.buf_size {
            return None;
        }

        self.try_get()
    }

    fn get_or_heap(&self, size: usize) -> BufferKind {
        if let Some(buffer) = self.try_get() {
            return BufferKind::new_pooled(buffer);
        }

        BufferKind::new_heap(size)
    }

    fn min_buf_size(&self) -> usize {
        self.buf_size
    }

    fn max_buf_size(&self) -> usize {
        self.buf_size
    }

    fn shrink(&self) {
        self.inner.shrink();
    }

    fn heap_usage(&self) -> usize {
        self.heap_usage()
    }
}
