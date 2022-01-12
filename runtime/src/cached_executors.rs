#[cfg(RUSTC_WITH_SPECIALIZATION)]
use solana_frozen_abi::abi_example::AbiExample;
use {
    lru::LruCache,
    solana_program_runtime::invoke_context::Executor,
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::Pubkey,
    },
    std::sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
};

// 10 MB assuming programs are around 100k
pub(crate) const MAX_CACHED_EXECUTORS: usize = 100;

/// LFU Cache of executors with single-epoch memory of usage counts
#[derive(Debug)]
pub(crate) struct CachedExecutors {
    executors: LruCache<Pubkey, Arc<dyn Executor>>,
    stats: CachedExecutorsStats,
}

#[derive(Debug, Default)]
struct CachedExecutorsStats {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Default for CachedExecutors {
    fn default() -> Self {
        Self {
            executors: LruCache::new(MAX_CACHED_EXECUTORS),
            stats: CachedExecutorsStats::default(),
        }
    }
}

#[cfg(RUSTC_WITH_SPECIALIZATION)]
impl AbiExample for CachedExecutors {
    fn example() -> Self {
        // Delegate AbiExample impl to Default before going deep and stuck with
        // not easily impl-able Arc<dyn Executor> due to rust's coherence issue
        // This is safe because CachedExecutors isn't serializable by definition.
        Self::default()
    }
}

impl Clone for CachedExecutors {
    fn clone(&self) -> Self {
        let mut executors = LruCache::new(self.executors.cap());
        for (key, entry) in self.executors.iter().rev() {
            executors.put(*key, entry.clone());
        }
        Self {
            executors,
            stats: CachedExecutorsStats::default(),
        }
    }
}

impl CachedExecutors {
    pub(crate) fn clone_with_epoch(self: &Arc<Self>, _epoch: Epoch) -> Arc<Self> {
        self.clone()
    }

    pub(crate) fn new(max: usize, _current_epoch: Epoch) -> Self {
        Self {
            executors: LruCache::new(max),
            stats: CachedExecutorsStats::default(),
        }
    }

    pub(crate) fn get(&mut self, pubkey: &Pubkey) -> Option<Arc<dyn Executor>> {
        if let Some(entry) = self.executors.get(pubkey) {
            self.stats.hits.fetch_add(1, Relaxed);
            Some(entry.clone())
        } else {
            self.stats.misses.fetch_add(1, Relaxed);
            None
        }
    }

    pub(crate) fn put(&mut self, executors: &[(&Pubkey, Arc<dyn Executor>)]) {
        for (pubkey, executor) in executors {
            self.executors.put(**pubkey, executor.clone());
        }
    }

    pub(crate) fn remove(&mut self, pubkey: &Pubkey) {
        self.executors.pop(pubkey);
    }

    pub(crate) fn submit_stats(&self, slot: Slot) {
        self.stats.submit(slot);
    }
}

impl CachedExecutorsStats {
    fn submit(&self, slot: Slot) {
        let hits = self.hits.load(Relaxed);
        let misses = self.misses.load(Relaxed);
        datapoint_info!(
            "bank-executor-cache-stats",
            ("slot", slot, i64),
            ("hits", hits, i64),
            ("misses", misses, i64),
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_program_runtime::invoke_context::InvokeContext,
        solana_sdk::instruction::InstructionError,
    };

    #[derive(Debug)]
    struct TestExecutor {}
    impl Executor for TestExecutor {
        fn execute<'a, 'b>(
            &self,
            _first_instruction_account: usize,
            _instruction_data: &[u8],
            _invoke_context: &'a mut InvokeContext<'b>,
            _use_jit: bool,
        ) -> std::result::Result<(), InstructionError> {
            Ok(())
        }
    }

    #[test]
    fn test_cached_executors() {
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let key3 = solana_sdk::pubkey::new_rand();
        let key4 = solana_sdk::pubkey::new_rand();
        let executor: Arc<dyn Executor> = Arc::new(TestExecutor {});
        let mut cache = CachedExecutors::new(3, 0);

        cache.put(&[(&key1, executor.clone())]);
        cache.put(&[(&key2, executor.clone())]);
        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_some());
        assert!(cache.get(&key3).is_some());

        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_some());
        cache.put(&[(&key4, executor.clone())]);
        assert!(cache.get(&key4).is_some());
        let num_retained = [&key1, &key2, &key3]
            .iter()
            .map(|key| cache.get(key))
            .flatten()
            .count();
        assert_eq!(num_retained, 2);

        assert!(cache.get(&key4).is_some());
        assert!(cache.get(&key4).is_some());
        assert!(cache.get(&key4).is_some());
        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key3).is_some());
        let num_retained = [&key1, &key2, &key4]
            .iter()
            .map(|key| cache.get(key))
            .flatten()
            .count();
        assert_eq!(num_retained, 2);
    }

    #[test]
    fn test_cached_executor_eviction() {
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let key3 = solana_sdk::pubkey::new_rand();
        let key4 = solana_sdk::pubkey::new_rand();
        let executor: Arc<dyn Executor> = Arc::new(TestExecutor {});
        let mut cache = CachedExecutors::new(3, 0);
        assert!(cache.current_epoch == 0);

        cache.put(&[(&key1, executor.clone())]);
        cache.put(&[(&key2, executor.clone())]);
        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key1).is_some());

        let mut cache = Arc::new(cache).clone_with_epoch(1);
        assert!(cache.current_epoch == 1);

        assert!(cache.get(&key2).is_some());
        assert!(cache.get(&key2).is_some());
        assert!(cache.get(&key3).is_some());
        Arc::make_mut(&mut cache).put(&[(&key4, executor.clone())]);

        assert!(cache.get(&key4).is_some());
        let num_retained = [&key1, &key2, &key3]
            .iter()
            .map(|key| cache.get(key))
            .flatten()
            .count();
        assert_eq!(num_retained, 2);

        Arc::make_mut(&mut cache).put(&[(&key1, executor.clone())]);
        Arc::make_mut(&mut cache).put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key3).is_some());
        let num_retained = [&key2, &key4]
            .iter()
            .map(|key| cache.get(key))
            .flatten()
            .count();
        assert_eq!(num_retained, 1);

        cache = cache.clone_with_epoch(2);
        assert!(cache.current_epoch == 2);

        Arc::make_mut(&mut cache).put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key3).is_some());
    }

    #[test]
    fn test_cached_executors_evicts_smallest() {
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let key3 = solana_sdk::pubkey::new_rand();
        let executor: Arc<dyn Executor> = Arc::new(TestExecutor {});
        let mut cache = CachedExecutors::new(2, 0);

        cache.put(&[(&key1, executor.clone())]);
        for _ in 0..5 {
            let _ = cache.get(&key1);
        }
        cache.put(&[(&key2, executor.clone())]);
        // make key1's use-count for sure greater than key2's
        let _ = cache.get(&key1);

        let mut entries = cache
            .executors
            .iter()
            .map(|(k, v)| (*k, v.epoch_count.load(Relaxed)))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, v)| *v);
        assert!(entries[0].1 < entries[1].1);

        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&entries[0].0).is_none());
        assert!(cache.get(&entries[1].0).is_some());
    }

    #[test]
    fn test_executor_cache_get_primer_count_upper_bound_inclusive() {
        let pubkey = Pubkey::default();
        let v = [];
        assert_eq!(
            CachedExecutors::get_primer_count_upper_bound_inclusive(&v),
            0
        );
        let v = [(&pubkey, 1)];
        assert_eq!(
            CachedExecutors::get_primer_count_upper_bound_inclusive(&v),
            1
        );
        let v = (0u64..10).map(|i| (&pubkey, i)).collect::<Vec<_>>();
        assert_eq!(
            CachedExecutors::get_primer_count_upper_bound_inclusive(v.as_slice()),
            7
        );
    }
}
