use crate::buffers::buffer_pool::{BufPool, BufferPool, BufferPoolStats, PooledBuffer};

/// A pool of `BufferPool`s. Wow.
#[derive(Default)]
pub struct MultiBufferPool {
    pools: Vec<BufferPool>,
}

pub struct MultiBufferPoolStats {
    pub total_heap_usage: usize,
    pub pool_stats: Vec<BufferPoolStats>,
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

    /// Returns a reference to a `BufferPool` that can allocate buffers of at least `size` bytes.
    fn get_pool_for_size(&self, size: usize) -> Option<&BufferPool> {
        // we know pools are sorted so we can use binary search

        match self.pools.binary_search_by_key(&size, |pool| pool.buf_size()) {
            Ok(index) => Some(&self.pools[index]),
            // the bin search function will return the index of the next pool that is larger than the requested size,
            // or it will return `pools.len()`, which means no pool is large enough
            Err(insp) => self.pools.get(insp),
        }
    }

    pub fn stats(&self) -> MultiBufferPoolStats {
        let total_heap_usage = self.heap_usage();
        let pool_stats = self.pools.iter().map(|pool| pool.stats()).collect();

        MultiBufferPoolStats { total_heap_usage, pool_stats }
    }
}

impl BufPool for MultiBufferPool {
    #[inline]
    async fn get(&self, size: usize) -> Option<PooledBuffer> {
        Some(self.get_pool_for_size(size)?.get_unchecked().await)
    }

    #[inline]
    fn try_get(&self, size: usize) -> Option<PooledBuffer> {
        self.get_pool_for_size(size)?.try_get()
    }

    #[inline]
    fn get_busy_loop(&self, size: usize) -> Option<PooledBuffer> {
        Some(self.get_pool_for_size(size)?.get_busy_loop_unchecked())
    }

    #[inline]
    fn min_buf_size(&self) -> usize {
        // assume that pools are sorted by buffer size
        self.pools.first().map_or(0, |pool| pool.buf_size())
    }

    #[inline]
    fn max_buf_size(&self) -> usize {
        // assume that pools are sorted by buffer size
        self.pools.last().map_or(0, |pool| pool.buf_size())
    }

    #[inline]
    fn shrink(&self) {
        for pool in &self.pools {
            pool.shrink();
        }
    }

    #[inline]
    fn heap_usage(&self) -> usize {
        self.pools.iter().map(|pool| pool.heap_usage()).sum()
    }

    #[inline]
    fn can_allocate(&self, size: usize) -> bool {
        self.get_pool_for_size(size).is_some()
    }
}
