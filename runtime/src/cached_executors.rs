#[cfg(RUSTC_WITH_SPECIALIZATION)]
use solana_frozen_abi::abi_example::AbiExample;
use {
    indexmap::IndexMap,
    log::*,
    rand::Rng,
    solana_program_runtime::invoke_context::Executor,
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::Pubkey,
    },
    std::{
        collections::HashMap,
        iter::repeat_with,
        sync::{
            atomic::{AtomicU64, Ordering::Relaxed},
            Arc,
        },
    },
};

// 10 MB assuming programs are around 100k
pub(crate) const MAX_CACHED_EXECUTORS: usize = 100;
const NUM_RANDOM_SAMPLES: usize = 2;
const COUNTS_CACHE_RATIO: usize = 20;

/// LFU Cache of executors with single-epoch memory of usage counts
#[derive(Debug)]
pub(crate) struct CachedExecutors {
    max: usize,
    executors: IndexMap<Pubkey, Arc<dyn Executor>>,
    counts: IndexMap<Pubkey, AtomicU64>,
    stats: CachedExecutorsStats,
}

#[derive(Debug, Default)]
struct CachedExecutorsStats {
    hits: AtomicU64,
    num_gets: AtomicU64,
    evictions: HashMap<Pubkey, u64>,
    insertions: AtomicU64,
    replacements: AtomicU64,
}

impl Default for CachedExecutors {
    fn default() -> Self {
        Self {
            max: MAX_CACHED_EXECUTORS,
            executors: IndexMap::default(),
            counts: IndexMap::default(),
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
        let counts = self
            .counts
            .iter()
            .map(|(&key, entry)| (key, AtomicU64::new(entry.load(Relaxed))));
        Self {
            max: self.max,
            executors: self.executors.clone(),
            counts: counts.collect(),
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
            max,
            executors: IndexMap::default(),
            counts: IndexMap::default(),
            stats: CachedExecutorsStats::default(),
        }
    }

    pub(crate) fn get(&self, pubkey: &Pubkey) -> Option<Arc<dyn Executor>> {
        self.stats.num_gets.fetch_add(1, Relaxed);
        self.counts.get(pubkey)?.fetch_add(1, Relaxed);
        if let Some(executor) = self.executors.get(pubkey) {
            self.stats.hits.fetch_add(1, Relaxed);
            Some(executor.clone())
        } else {
            None
        }
    }

    pub(crate) fn put(&mut self, executors: &[(&Pubkey, Arc<dyn Executor>)]) {
        for (key, executor) in executors {
            self.executors.insert(**key, executor.clone());
            self.counts.entry(**key).or_default();
        }
        let mut rng = rand::thread_rng();
        while self.counts.len() > self.max.saturating_mul(COUNTS_CACHE_RATIO) {
            let size = self.counts.len();
            let (index, _) = repeat_with(|| rng.gen_range(0, size))
                .take(NUM_RANDOM_SAMPLES)
                .map(|index| (index, self.counts[index].load(Relaxed)))
                .min_by_key(|(_, count)| *count)
                .unwrap();
            if let Some((key, _)) = self.counts.swap_remove_index(index) {
                self.executors.swap_remove(&key);
            }
        }
        while self.executors.len() > self.max {
            let size = self.executors.len();
            let (index, _) = repeat_with(|| rng.gen_range(0, size))
                .take(NUM_RANDOM_SAMPLES)
                .map(|index| {
                    let (key, _) = self.executors.get_index(index).unwrap();
                    (index, self.counts[key].load(Relaxed))
                })
                .min_by_key(|(_, count)| *count)
                .unwrap();
            self.executors.swap_remove_index(index);
        }
    }

    pub(crate) fn remove(&mut self, pubkey: &Pubkey) {
        self.executors.remove(pubkey);
    }

    pub(crate) fn submit_stats(&self, slot: Slot) {
        self.stats.submit(slot);
    }
}

impl CachedExecutorsStats {
    fn submit(&self, slot: Slot) {
        let hits = self.hits.load(Relaxed);
        let misses = self.num_gets.load(Relaxed).saturating_sub(hits);
        let insertions = self.insertions.load(Relaxed);
        let replacements = self.replacements.load(Relaxed);
        let evictions: u64 = self.evictions.values().sum();
        datapoint_info!(
            "bank-executor-cache-stats",
            ("slot", slot, i64),
            ("hits", hits, i64),
            ("misses", misses, i64),
            ("evictions", evictions, i64),
            ("insertions", insertions, i64),
            ("replacements", replacements, i64),
        );
        debug!(
                "Executor Cache Stats -- Hits: {}, Misses: {}, Evictions: {}, Insertions: {}, Replacements: {}",
                hits, misses, evictions, insertions, replacements,
            );
        if log_enabled!(log::Level::Trace) && !self.evictions.is_empty() {
            let mut evictions = self.evictions.iter().collect::<Vec<_>>();
            evictions.sort_by_key(|e| e.1);
            let evictions = evictions
                .into_iter()
                .rev()
                .map(|(program_id, evictions)| {
                    format!("  {:<44}  {}", program_id.to_string(), evictions)
                })
                .collect::<Vec<_>>();
            let evictions = evictions.join("\n");
            trace!(
                "Eviction Details:\n  {:<44}  {}\n{}",
                "Program",
                "Count",
                evictions
            );
        }
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

        cache.put(&[(&key1, executor.clone())]);
        cache.put(&[(&key2, executor.clone())]);
        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key1).is_some());

        let mut cache = Arc::new(cache).clone_with_epoch(1);

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
            .counts
            .iter()
            .map(|(k, v)| (*k, v.load(Relaxed)))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, v)| *v);
        assert!(entries[0].1 < entries[1].1);

        cache.put(&[(&key3, executor.clone())]);
        assert!(cache.get(&entries[0].0).is_none());
        assert!(cache.get(&entries[1].0).is_some());
    }
}
