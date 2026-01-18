use crate::{
    buffers::buffer_pool::{BufPool, BufferPool, BufferPoolStats, PooledBuffer},
    message::BufferKind,
};

/// A more adaptable version of `MultiBufferPool`
#[derive(Default)]
pub struct HybridBufferPool {
    pools: Vec<BufferPool>,
}

pub struct HybridBufferPoolStats {
    pub total_heap_usage: usize,
    pub pool_stats: Vec<BufferPoolStats>,
}

impl HybridBufferPool {
    /// Creates a new `HybridBufferPool`. Pass in the initial and maximum memory size (in bytes) for the pools.
    /// These are very soft limits, and should only be considered as hints.
    pub fn new(initial_mem: usize, max_mem: usize) -> Self {
        // kind of arbitrary
        let pool_attrs = [
            (256usize, 6usize, 24usize),
            (512, 6, 24),
            (1024, 4, 16),
            (2048, 2, 8),
            (4096, 0, 4),
            (8192, 0, 2),
            (16384, 0, 1),
            (32768, 0, 1),
        ];

        Self::new_with_weights(initial_mem, max_mem, &pool_attrs)
    }

    pub fn new_with_weights(
        initial_mem: usize,
        max_mem: usize,
        weights: &[(usize, usize, usize)],
    ) -> Self {
        let mut pools = Vec::new();

        let total_initial_mult =
            weights.iter().map(|(_, initial_mult, _)| *initial_mult).sum::<usize>();
        let total_max_mult = weights.iter().map(|(_, _, max_mult)| *max_mult).sum::<usize>();

        for (buf_size, initial_mult, max_mult) in weights {
            let initial_bufs = initial_mem * initial_mult / total_initial_mult / buf_size;
            let max_bufs = max_mem * max_mult / total_max_mult / buf_size;

            assert!(max_bufs >= initial_bufs);

            // don't create empty pools
            if max_bufs > 0 {
                pools.push(BufferPool::new(*buf_size, initial_bufs, max_bufs));
            }
        }

        Self { pools }
    }

    /// Returns a slice of `BufferPool` structures that can allocate buffers of at least `size` bytes.
    fn get_pools_for_size(&self, size: usize) -> Option<&[BufferPool]> {
        // we know pools are sorted so we can use binary search

        match self.pools.binary_search_by_key(&size, |pool| pool.buf_size()) {
            Ok(index) => Some(&self.pools[index..]),
            // the bin search function will return the index of the next pool that is larger than the requested size,
            // or it will return `pools.len()`, which means no pool is large enough
            Err(insp) => {
                if insp < self.pools.len() {
                    Some(&self.pools[insp..])
                } else {
                    None
                }
            }
        }
    }

    pub fn stats(&self) -> HybridBufferPoolStats {
        let total_heap_usage = self.heap_usage();
        let pool_stats = self.pools.iter().map(|pool| pool.stats()).collect();

        HybridBufferPoolStats { total_heap_usage, pool_stats }
    }
}

impl BufPool for HybridBufferPool {
    #[inline]
    async fn get(&self, size: usize) -> Option<PooledBuffer> {
        let pools = self.get_pools_for_size(size)?;
        debug_assert!(!pools.is_empty(), "get_pools_for_size must not return an empty slice");

        let first = &pools[0];
        let second = pools.get(1);

        // if we have only one pool, just call the blocking 'get'
        if second.is_none() {
            return Some(first.get_unchecked().await);
        }

        // otherwise, try a fast get from the first pool, then a fast get from the second pool,
        // and finally fall back to the first pool's get if both fail
        if let Some(buf) = first.try_get() {
            Some(buf)
        } else if let Some(buf) = second.unwrap().try_get() {
            Some(buf)
        } else {
            Some(first.get_unchecked().await)
        }
    }

    #[inline]
    fn try_get(&self, size: usize) -> Option<PooledBuffer> {
        let pools = self.get_pools_for_size(size)?;
        debug_assert!(!pools.is_empty(), "get_pools_for_size must not return an empty slice");

        let first = &pools[0];
        let second = pools.get(1);

        match first.try_get() {
            Some(buf) => Some(buf),
            None => match second {
                Some(second_pool) => second_pool.try_get(),
                None => None,
            },
        }
    }

    fn get_or_heap(&self, size: usize) -> BufferKind {
        let Some(pools) = self.get_pools_for_size(size) else {
            return BufferKind::new_heap(size);
        };

        debug_assert!(!pools.is_empty(), "get_pools_for_size must not return an empty slice");

        let first = &pools[0];
        let second = pools.get(1);

        // if there is only 1 pool, call `get_or_heap` directly
        if second.is_none() {
            return first.get_or_heap(size);
        }

        // otherwise, try a fast get from the first pool, then a fast get from the second pool
        if let Some(buf) = first.try_get() {
            return BufferKind::new_pooled(buf);
        }

        if let Some(second_pool) = second
            && let Some(buf) = second_pool.try_get()
        {
            return BufferKind::new_pooled(buf);
        }

        // could not get one quickly, just heap allocate
        BufferKind::new_heap(size)
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
        self.get_pools_for_size(size).is_some()
    }
}
