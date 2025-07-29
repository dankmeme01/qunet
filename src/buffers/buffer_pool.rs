use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

struct BufferPoolInner {
    storage: Mutex<Vec<Box<[u8]>>>,
    semaphore: Arc<Semaphore>,
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
    /// Atomically returns a buffer to the pool and returns the semaphore permit if it was acquired.
    /// If not acquired, increments the semaphore permit count by 1.
    #[inline]
    fn return_buffer(&self, buffer: Box<[u8]>, permit: Option<OwnedSemaphorePermit>) {
        let mut storage = self.storage.lock();
        storage.push(buffer);

        // if we have a permit, it will release on its own when dropped,
        // if we don't have a permit, we need to manually increment the semaphore permit count
        match permit {
            Some(_) => {}
            None => self.semaphore.add_permits(1),
        }

        // // if less than half the buffers are actually used, automatically shrink the pool
        // let allocated = self.allocated_buffers.load(Ordering::Relaxed);
        // if storage.len() <= allocated / 2 {
        //     self.shrink();
        // }
    }

    fn shrink(&self) {
        // `forget_permits` returns how many permits actually were released, and we can free that many buffers
        let released_bufs =
            self.semaphore.forget_permits(self.allocated_buffers.load(Ordering::Relaxed));

        // decrease the allocated buffers count
        self.allocated_buffers.fetch_sub(released_bufs, Ordering::SeqCst);

        let mut storage = self.storage.lock();

        assert!(released_bufs <= storage.len());

        let to_keep = storage.len() - released_bufs;
        storage.truncate(to_keep);
    }
}

impl BufferPool {
    /// Creates a new `BufferPool` with the specified buffer size, initial number of buffers, and maximum number of buffers.
    /// The minimum memory usage of the pool will be `buf_size * initial_buffers`.
    /// The maximum memory usage of the pool will be `buf_size * max_buffers`.
    pub fn new(buf_size: usize, initial_buffers: usize, max_buffers: usize) -> Self {
        let mut storage = Vec::with_capacity(initial_buffers);

        for _ in 0..initial_buffers {
            let buffer = vec![0u8; buf_size].into_boxed_slice();
            storage.push(buffer);
        }

        let inner = BufferPoolInner {
            storage: Mutex::new(storage),
            semaphore: Arc::new(Semaphore::new(initial_buffers)),
            allocated_buffers: AtomicUsize::new(initial_buffers),
        };

        Self {
            buf_size,
            max_buffers,
            inner: Arc::new(inner),
        }
    }

    /// This function should only be called when a permit for the buffer is already acquired.
    /// Using this otherwise will lead to a panic.
    #[inline]
    fn get_buffer(&self) -> Box<[u8]> {
        // it is the caller's responsibility to ensure that a permit is acquired before calling this
        // number of available permits is guaranteed to be equal to the number of buffers in storage,
        // so we can safely pop a buffer from the storage
        let mut storage = self.inner.storage.lock();

        match storage.pop() {
            Some(x) => x,
            None => panic!("BufferPool::get_buffer misuse: no available buffers"),
        }
    }

    /// Allocates and returns a new buffer.
    /// It is the caller's responsibility to not call this function when the limit of buffers is reached,
    /// and to increment the `allocated_buffers` count accordingly.
    #[inline]
    fn alloc_new_buffer(&self) -> Box<[u8]> {
        // we intentionally don't increment semaphore permits here, as it could lead to a race condition
        vec![0u8; self.buf_size].into_boxed_slice()
    }

    /// Like `get`, but without needing to pass the size.
    pub async fn get_unchecked(&self) -> PooledBuffer {
        if let Some(buffer) = self.try_get() {
            return buffer;
        }

        // if we reached here, this means the pool is at max capacity and no permits are available right now,
        // we must wait

        let permit = self.inner.semaphore.clone().acquire_owned().await.unwrap();
        let buffer = self.get_buffer();

        PooledBuffer::new(buffer, self.inner.clone(), Some(permit))
    }

    #[inline]
    fn try_get_no_grow(&self) -> Option<PooledBuffer> {
        if let Ok(permit) = self.inner.semaphore.clone().try_acquire_owned() {
            return Some(PooledBuffer::new(self.get_buffer(), self.inner.clone(), Some(permit)));
        }

        None
    }

    pub fn try_get(&self) -> Option<PooledBuffer> {
        // first, try acquire a permit if there are any available
        if let Some(buf) = self.try_get_no_grow() {
            return Some(buf);
        }

        // if we failed to acquire a permit, see if we can grow the pool
        let mut num_buffers = self.inner.allocated_buffers.load(Ordering::Relaxed);

        loop {
            if num_buffers >= self.max_buffers {
                break None;
            }

            // try to grow the pool
            match self.inner.allocated_buffers.compare_exchange(
                num_buffers,
                num_buffers + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // successfully incremented the count, allocate a new buffer
                    let buffer = self.alloc_new_buffer();

                    break Some(PooledBuffer::new(buffer, self.inner.clone(), None));
                }

                Err(num) => {
                    // another thread has beaten us, let's try again if we can
                    num_buffers = num;
                    continue;
                }
            }
        }
    }

    pub fn get_busy_loop_unchecked(&self) -> PooledBuffer {
        loop {
            let bufs = self.inner.allocated_buffers.load(Ordering::Relaxed);
            let buf = if bufs >= self.max_buffers {
                self.try_get_no_grow()
            } else {
                self.try_get()
            };

            match buf {
                Some(buf) => break buf,
                None => {
                    #[cfg(debug_assertions)]
                    warn!(
                        "BufferPool::get_busy_loop: no buffers available (size {}, alloocated {}/{})",
                        self.buf_size, bufs, self.max_buffers
                    );

                    std::thread::yield_now()
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
        let available = self.inner.semaphore.available_permits();

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
    buffer: ManuallyDrop<Box<[u8]>>,
    pool: Arc<BufferPoolInner>,
    permit: Option<OwnedSemaphorePermit>,

    #[cfg(debug_assertions)]
    created_at: std::time::Instant,
}

impl PooledBuffer {
    fn new(
        buffer: Box<[u8]>,
        pool: Arc<BufferPoolInner>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            buffer: ManuallyDrop::new(buffer),
            pool,
            permit,
            #[cfg(debug_assertions)]
            created_at: std::time::Instant::now(),
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
        self.pool.return_buffer(buf, self.permit.take());
    }
}

pub trait BufPool: Send + Sync + 'static {
    fn get(&self, size: usize) -> impl Future<Output = Option<PooledBuffer>> + Send;
    /// Same as `get`, but is synchronous. Not recommended to use in async code.
    fn get_busy_loop(&self, size: usize) -> Option<PooledBuffer>;

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

    fn get_busy_loop(&self, size: usize) -> Option<PooledBuffer> {
        if size > self.buf_size {
            return None;
        }

        Some(self.get_busy_loop_unchecked())
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
