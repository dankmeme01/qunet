use crate::buffers::buffer_pool::{BorrowedMutBuffer, BufferPool};

/// A pool of `BufferPool`s. Wow.
#[derive(Default)]
pub struct MultiBufferPool {
    pools: Vec<BufferPool>,
}

impl MultiBufferPool {
    /// Creates a new `MultiBufferPool` with no pools.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new `BufferPool` to the `MultiBufferPool`.
    pub fn add_pool(&mut self, pool: BufferPool) {
        self.pools.push(pool);

        // sort the pools by buffer size in ascending order
        // yes this is not very efficient
        self.pools.sort_by_key(|pool| pool.buf_size());
    }

    /// Shrinks all the pools to their minimum size.
    /// This will free up memory, but subsequent calls to `get` or `try_get` may have to allocate new buffers again.
    pub fn shrink(&self) {
        for pool in &self.pools {
            pool.shrink();
        }
    }

    /// Returns the total heap usage of all the buffer pools. (only including buffers, not the pool itself or any inner pools)
    #[inline]
    pub fn heap_usage(&self) -> usize {
        self.pools.iter().map(|pool| pool.heap_usage()).sum()
    }

    /// Returns the size of the smallest buffer in the pool.
    #[inline]
    pub fn min_buf_size(&self) -> usize {
        // assume that pools are sorted by buffer size
        self.pools.first().map_or(0, |pool| pool.buf_size())
    }

    /// Returns the size of the largest buffer in the pool.
    #[inline]
    pub fn max_buf_size(&self) -> usize {
        // assume that pools are sorted by buffer size
        self.pools.last().map_or(0, |pool| pool.buf_size())
    }

    /// Returns a reference to a `BufferPool` that can allocate buffers of at least `size` bytes.
    fn get_pool_for_size(&self, size: usize) -> Option<&BufferPool> {
        // we know pools are sorted so we can use binary search

        match self
            .pools
            .binary_search_by_key(&size, |pool| pool.buf_size())
        {
            Ok(index) => Some(&self.pools[index]),
            // the bin search function will return the index of the next pool that is larger than the requested size,
            // or it will return `pools.len()`, which means no pool is large enough
            Err(insp) => self.pools.get(insp),
        }
    }

    /// Returns whether it is possible to allocate a buffer of at least `size` bytes in the pool.
    #[inline]
    pub fn can_allocate(&self, size: usize) -> bool {
        self.get_pool_for_size(size).is_some()
    }

    /// Attempts to returns a new buffer of at least `size` bytes without blocking.
    /// If no pool is large enough or no buffers are available in the chosen pool, this will return `None`.
    #[inline]
    pub fn try_get(&self, size: usize) -> Option<BorrowedMutBuffer> {
        self.get_pool_for_size(size)?.try_get()
    }

    /// Returns a future which will return a new buffer of at least `size` bytes, as soon as one is available.
    /// If no pool is large enough, this will return `None`.
    #[inline]
    pub async fn get(&self, size: usize) -> Option<BorrowedMutBuffer> {
        Some(self.get_pool_for_size(size)?.get().await)
    }
}
