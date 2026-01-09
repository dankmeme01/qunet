use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::message::BufferKind;

type Buf = Box<[u8]>;

/// MPSC-based pool of buffers
pub struct MpscInnerPool {
    rx: flume::Receiver<Buf>,
    tx: flume::Sender<Buf>,
    allocated_buffers: AtomicUsize,
    buf_size: usize,
}

/// Notify-based pool, more cache friendly because buffers are returned to the front of the queue (stack structure)
pub struct NotifyInnerPool {
    notify: Notify,
    storage: Mutex<Vec<Buf>>,
    allocated_buffers: AtomicUsize,
    buf_size: usize,
}

pub trait InnerPool: Send + Sync + 'static {
    fn try_get(&self) -> Option<Buf>;
    fn get(&self) -> impl Future<Output = Buf> + Send
    where
        Self: Sized;

    /// Atomically returns a buffer to the pool
    fn return_buffer(&self, buffer: Buf);

    fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self
    where
        Self: Sized;
    fn shrink(&self);

    fn allocated(&self) -> usize;
    fn available(&self) -> usize;
    fn max_buffers(&self) -> usize;
    fn buf_size(&self) -> usize;

    fn _allocated_ref(&self) -> &AtomicUsize;

    /// Tries to allocate a new buffer and increases the allocated buffers count, returns None if at max capacity.
    fn try_grow(&self) -> Option<Buf> {
        let allocated = self._allocated_ref();

        let mut num_buffers = allocated.load(Ordering::Acquire);

        loop {
            if num_buffers >= self.max_buffers() {
                break None;
            }

            // try to grow the pool
            match allocated.compare_exchange_weak(
                num_buffers,
                num_buffers + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // successfully incremented the count, allocate a new buffer
                    let buffer = make_buffer(self.buf_size());

                    break Some(buffer);
                }

                Err(num) => {
                    // another thread has beaten us, let's try again if we can
                    num_buffers = num;
                    continue;
                }
            }
        }
    }
}

pub struct BufferPool<I: InnerPool = NotifyInnerPool> {
    inner: Arc<I>,
}

pub struct BufferPoolStats {
    pub buf_size: usize,
    pub heap_usage: usize,
    pub allocated_buffers: usize,
    pub max_buffers: usize,
    pub avail_buffers: usize,
}

impl<I: InnerPool> BufferPool<I> {
    /// Creates a new `BufferPool` with the specified buffer size, initial number of buffers, and maximum number of buffers.
    /// The minimum memory usage of the pool will be `buf_size * initial_buffers`.
    /// The maximum memory usage of the pool will be `buf_size * max_buffers`.
    pub fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        assert!(max_buffers > 0, "max_buffers in a buffer pool cannot be 0");
        assert!(initial_buffers <= max_buffers, "initial_buffers must be <= max_buffers");

        Self {
            inner: Arc::new(I::new(buf_size, initial_buffers, max_buffers)),
        }
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

        // try to grow the pool
        self.inner.try_grow().map(|buf| PooledBuffer::new(buf, self.inner.clone()))
    }

    /// Returns the size of a single buffer in this pool.
    #[inline]
    pub fn buf_size(&self) -> usize {
        self.inner.buf_size()
    }

    /// Returns the total heap usage of this pool. (only including buffers, not the pool itself)
    #[inline]
    pub fn heap_usage(&self) -> usize {
        self.buf_size() * self.inner.allocated()
    }

    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            buf_size: self.inner.buf_size(),
            heap_usage: self.heap_usage(),
            allocated_buffers: self.inner.allocated(),
            max_buffers: self.inner.max_buffers(),
            avail_buffers: self.inner.available(),
        }
    }
}

pub struct PooledBuffer {
    // ManuallyDrop used because we cant move out the buffer in Drop, to avoid using an Option
    buffer: ManuallyDrop<Buf>,
    pool: Arc<dyn InnerPool>,
    // #[cfg(debug_assertions)]
    // created_at: std::time::Instant,
}

impl PooledBuffer {
    fn new(buffer: Buf, pool: Arc<dyn InnerPool>) -> Self {
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

impl<I: InnerPool> BufPool for BufferPool<I> {
    /// Returns a future which will return a new buffer of at least `size` bytes, as soon as one is available.
    /// If no pool is large enough, this will return `None`.
    async fn get(&self, size: usize) -> Option<PooledBuffer> {
        if size > self.buf_size() {
            return None;
        }

        Some(self.get_unchecked().await)
    }

    fn try_get(&self, size: usize) -> Option<PooledBuffer> {
        if size > self.buf_size() {
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
        self.buf_size()
    }

    fn max_buf_size(&self) -> usize {
        self.buf_size()
    }

    fn shrink(&self) {
        self.inner.shrink();
    }

    fn heap_usage(&self) -> usize {
        self.heap_usage()
    }
}

impl InnerPool for MpscInnerPool {
    fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        let (tx, rx) = flume::bounded(max_buffers);

        for _ in 0..initial_buffers {
            tx.try_send(make_buffer(buf_size)).unwrap();
        }

        Self {
            buf_size,
            rx,
            tx,
            allocated_buffers: AtomicUsize::new(initial_buffers),
        }
    }

    #[inline]
    fn return_buffer(&self, buffer: Buf) {
        // this unwrap must never fail, as BufferPool ensures we cannot create more buffers than the max
        self.tx.try_send(buffer).unwrap();
    }

    fn shrink(&self) {
        let released_bufs = self.rx.drain().count();

        // decrease the allocated buffers count
        let old_bufs = self.allocated_buffers.fetch_sub(released_bufs, Ordering::AcqRel);

        // check for potential overflow, should not be possible
        debug_assert!(old_bufs.checked_sub(released_bufs).is_some());
    }

    fn try_get(&self) -> Option<Buf> {
        self.rx.try_recv().ok()
    }

    async fn get(&self) -> Buf {
        self.rx.recv_async().await.unwrap()
    }

    fn allocated(&self) -> usize {
        self.allocated_buffers.load(Ordering::Relaxed)
    }

    fn _allocated_ref(&self) -> &AtomicUsize {
        &self.allocated_buffers
    }

    fn available(&self) -> usize {
        self.rx.len()
    }

    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn max_buffers(&self) -> usize {
        self.tx.capacity().unwrap()
    }
}

impl InnerPool for NotifyInnerPool {
    fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        let mut storage = Vec::with_capacity(max_buffers);

        for _ in 0..initial_buffers {
            storage.push(make_buffer(buf_size));
        }

        Self {
            storage: Mutex::new(storage),
            notify: Notify::new(),
            buf_size,
            allocated_buffers: AtomicUsize::new(initial_buffers),
        }
    }

    #[inline]
    fn return_buffer(&self, buffer: Buf) {
        self.storage.lock().push(buffer);
        self.notify.notify_one();
    }

    fn shrink(&self) {
        let released_bufs = {
            let mut vec = self.storage.lock();
            let count = vec.len();
            vec.clear();
            count
        };

        // decrease the allocated buffers count
        let old_bufs = self.allocated_buffers.fetch_sub(released_bufs, Ordering::AcqRel);

        // check for potential overflow, should not be possible
        debug_assert!(old_bufs.checked_sub(released_bufs).is_some());
    }

    fn try_get(&self) -> Option<Buf> {
        self.storage.lock().pop()
    }

    async fn get(&self) -> Buf {
        loop {
            if let Some(buf) = self.try_get() {
                return buf;
            }
            self.notify.notified().await;
        }
    }

    fn allocated(&self) -> usize {
        self.allocated_buffers.load(Ordering::Relaxed)
    }

    fn _allocated_ref(&self) -> &AtomicUsize {
        &self.allocated_buffers
    }

    fn available(&self) -> usize {
        self.storage.lock().len()
    }

    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn max_buffers(&self) -> usize {
        self.storage.lock().capacity()
    }
}

fn make_buffer(size: usize) -> Buf {
    vec![0u8; size].into_boxed_slice()
}
