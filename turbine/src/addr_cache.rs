use {
    crate::retransmit_stage::RetransmitSlotStats,
    solana_ledger::{
        blockstore::MAX_DATA_SHREDS_PER_SLOT,
        shred::{shred_code::MAX_CODE_SHREDS_PER_SLOT, ShredId, ShredType},
    },
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque},
        net::SocketAddr,
    },
};

// Number of most recent shreds to track slots counts based off.
const ROLLING_WINDOW_NUM_SHREDS: usize = 512;
// Capacity to initially allocate for CacheEntry.{code,data}.
const ADDR_CAPACITY: usize = 2_560;
// How far the cached addresses are speculatively extended beyond max-index
// observed.
const EXTEND_BUFFER: usize = ADDR_CAPACITY / 5;

// Cache of Turbine tree retransmit addresses for the most frequent slots
// within the rolling window of shreds arriving at retransmit-stage.
pub(crate) struct AddrCache {
    // Number of slots to cache addresses for.
    capacity: usize,
    // Number of shreds observed within the rolling window.
    // Equivalent to:
    //   self.window.iter().map(|&(_, count)| count).sum::<usize>()
    num_shreds: usize,
    // Rolling window of slots and number of shreds observed.
    // Worst case, all entries have count == 1, in which case the size of this
    // ring buffer is bounded by ROLLING_WINDOW_NUM_SHREDS.
    window: VecDeque<(Slot, /*count:*/ usize)>,
    // Number of shreds observed in each slot within the rolling window.
    // Equivalent to:
    //   self.window.iter().fold(HashMap::new(), |mut acc, &(slot, count)| {
    //     *acc.entry(slot).or_default() += count;
    //     acc
    //   })
    // Worst case, all shreds within the rolling window are from a unique slot,
    // in which case the size is bounded by ROLLING_WINDOW_NUM_SHREDS.
    counts: HashMap<Slot, /*count:*/ usize>,
    // Cache of addresses for the most frequent slots.
    // Lazily trimmed to self.capacity size to achieve amortized O(1)
    // complexity. The size is bounded by 1 + self.capacity * 2.
    cache: HashMap<Slot, CacheEntry>,
}

struct CacheEntry {
    // Root distance and socket addresses cached either speculatively or when
    // retransmitting incoming shreds.
    code: Vec<Option<(/*root_distance:*/ u8, Vec<SocketAddr>)>>,
    data: Vec<Option<(/*root_distance:*/ u8, Vec<SocketAddr>)>>,
    // Code and data indices where [..index] are fully populated.
    index_code: usize,
    index_data: usize,
    // Maximum code and data indices observed in retransmit-stage.
    max_index_code: u32,
    max_index_data: u32,
    // If the last data shred in the slot is already observed, the cache is no
    // longer extended beyond max_index_{code,data}.
    last_shred_in_slot: bool,
}

impl AddrCache {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            num_shreds: 0,
            window: VecDeque::new(),
            counts: HashMap::new(),
            cache: {
                // 2x capacity in order to implement lazy eviction.
                let capacity = capacity.saturating_mul(2).saturating_add(1);
                HashMap::with_capacity(capacity)
            },
        }
    }

    // Returns (root-distance, socket-addresses) cached for the given shred-id.
    #[inline]
    pub(crate) fn get(&self, shred: &ShredId) -> Option<(/*root_distance:*/ u8, &Vec<SocketAddr>)> {
        self.cache
            .get(&shred.slot())?
            .get(shred.shred_type(), shred.index())
    }

    // Stores (root-distance, socket-addresses) precomputed speculatively for
    // the given shred-id.
    pub(crate) fn put(&mut self, shred: &ShredId, entry: (/*root_distance:*/ u8, Vec<SocketAddr>)) {
        self.get_cache_entry_mut(shred.slot())
            .put(shred.shred_type(), shred.index(), entry);
        self.maybe_trim_cache();
    }

    // Records data observed from incoming shreds at retransmit stage.
    pub(crate) fn record(&mut self, slot: Slot, stats: &mut RetransmitSlotStats) {
        // All addresses should be for the same slot.
        debug_assert!(stats.addrs.iter().all(|(shred, _, _)| shred.slot() == slot));
        // Update rolling window count of shreds per slot.
        let num_shreds: usize = stats.num_shreds_received.iter().sum();
        if num_shreds > 0 {
            self.num_shreds += num_shreds;
            self.window.push_back((slot, num_shreds));
            *self.counts.entry(slot).or_default() += num_shreds;
            self.maybe_trim_slot_counts();
        }
        debug_assert!(self.verify());
        // If there are no addresses to cache and the cache entry is not
        // allocated for the slot yet, then ignore.
        if stats.addrs.is_empty() && !self.cache.contains_key(&slot) {
            return;
        }
        // Update the cached entry for the slot.
        let entry = self.get_cache_entry_mut(slot);
        entry.max_index_code = entry.max_index_code.max(stats.max_index_code);
        entry.max_index_data = entry.max_index_data.max(stats.max_index_data);
        entry.last_shred_in_slot |= stats.last_shred_in_slot;
        for (shred, root_distance, addrs) in std::mem::take(&mut stats.addrs) {
            debug_assert_eq!(shred.slot(), slot);
            entry.put(shred.shred_type(), shred.index(), (root_distance, addrs));
        }
        self.maybe_trim_cache();
        debug_assert!(self.verify());
    }

    // Returns num_shreds shred-ids to speculatively pre-compute turbine tree.
    // ShredIds are chosen based on which slots have received most number of
    // shreds within the rolling window.
    pub(crate) fn get_shreds(&mut self, num_shreds: usize) -> Vec<ShredId> {
        // Find first two most frequent slots.
        let mut slots = [None, None];
        for (&slot, &count) in self.counts.iter() {
            let entry = Some((count, slot));
            if entry > slots[0] {
                slots[1] = slots[0];
                slots[0] = entry;
            } else if entry > slots[1] {
                slots[1] = entry;
            }
        }
        slots[1] = slots[1].or(slots[0]);
        let mut out = Vec::with_capacity(num_shreds);
        for ((_, slot), num_shreds) in slots
            .into_iter()
            .flatten()
            .zip([num_shreds * 3 / 4, num_shreds / 4])
        {
            out.extend(
                self.get_cache_entry_mut(slot)
                    .get_shreds(/*extend_buffer:*/ 256)
                    .take(num_shreds)
                    .map(move |(shred_type, index)| ShredId::new(slot, index as u32, shred_type)),
            );
        }
        out
    }

    // Returns a mutable reference to the cached entry for the given slot.
    // Initializes the entry if not allocated yet.
    #[inline]
    fn get_cache_entry_mut(&mut self, slot: Slot) -> &mut CacheEntry {
        self.cache
            .entry(slot)
            .or_insert_with(|| CacheEntry::new(ADDR_CAPACITY))
    }

    // If there are more than ROLLING_WINDOW_NUM_SHREDS shreds in the rolling
    // window, drops the oldest entries and updates self.counts.
    fn maybe_trim_slot_counts(&mut self) {
        while let Some(count) = self
            .num_shreds
            .checked_sub(ROLLING_WINDOW_NUM_SHREDS)
            .filter(|&k| k > 0)
        {
            let (slot, num_shreds) = self.window.front_mut().unwrap();
            let count = count.min(*num_shreds);
            self.num_shreds -= count;
            *num_shreds -= count;
            let Entry::Occupied(mut entry) = self.counts.entry(*slot) else {
                panic!("Entry must exist if it has non-zero count.");
            };
            *entry.get_mut() -= count;
            if *entry.get() == 0 {
                entry.remove_entry();
            }
            if *num_shreds == 0 {
                self.window.pop_front();
            }
        }
    }

    // If there are more than 2 * self.capacity entries in the cache, drops
    // the slots with the fewest counts in the rolling window.
    fn maybe_trim_cache(&mut self) {
        if self.cache.len() <= self.capacity * 2 {
            return;
        }
        let mut entries: Vec<((Slot, CacheEntry), /*count:*/ usize)> = self
            .cache
            .drain()
            .map(|entry @ (slot, _)| {
                let count = self.counts.get(&slot).copied().unwrap_or_default();
                (entry, count)
            })
            .collect();
        let index = self.capacity.saturating_sub(1);
        entries.select_nth_unstable_by_key(index, |&((slot, _), count)| Reverse((count, slot)));
        self.cache.extend(
            entries
                .into_iter()
                .take(self.capacity)
                .map(|(entry, _)| entry),
        );
    }

    // Verifies internal consistency for tests and debug assertions.
    #[must_use]
    fn verify(&self) -> bool {
        let num_shreds: usize = self.window.iter().map(|&(_, count)| count).sum();
        let counts = self
            .window
            .iter()
            .fold(HashMap::new(), |mut acc, &(slot, count)| {
                *acc.entry(slot).or_default() += count;
                acc
            });
        num_shreds <= ROLLING_WINDOW_NUM_SHREDS
            && self.num_shreds == num_shreds
            && self.counts == counts
            && self.window.iter().all(|&(_, count)| count > 0)
            && self.counts.values().all(|&count| count > 0)
    }
}

impl CacheEntry {
    fn new(capacity: usize) -> Self {
        Self {
            code: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
            index_code: 0,
            index_data: 0,
            max_index_code: 0,
            max_index_data: 0,
            last_shred_in_slot: false,
        }
    }

    // Returns (root-distance, socket-addresses) cached for the given shred
    // type and index.
    #[inline]
    fn get(
        &self,
        shred_type: ShredType,
        shred_index: u32,
    ) -> Option<(/*root_distance:*/ u8, &Vec<SocketAddr>)> {
        match shred_type {
            ShredType::Code => &self.code,
            ShredType::Data => &self.data,
        }
        .get(shred_index as usize)?
        .as_ref()
        .map(|(root_distance, addrs)| (*root_distance, addrs))
    }

    // Stores (root-distance, socket-addresses) for the given shred type and
    // index.
    #[inline]
    fn put(
        &mut self,
        shred_type: ShredType,
        shred_index: u32,
        entry: (/*root_distance:*/ u8, Vec<SocketAddr>),
    ) {
        let cache = match shred_type {
            ShredType::Code => &mut self.code,
            ShredType::Data => &mut self.data,
        };
        let k = shred_index as usize;
        if cache.len() <= k {
            cache.resize(k + 1, None);
        }
        cache[k] = Some(entry)
    }

    // Returns an iterator of (shred-type, shred-index) to speculatively
    // pre-compute turbine tree for.
    fn get_shreds(
        &mut self,
        // How far the cached addresses are speculatively extended beyond
        // max-index observed.
        extend_buffer: usize,
    ) -> impl Iterator<Item = (ShredType, /*index:*/ usize)> + '_ {
        while matches!(self.code.get(self.index_code), Some(Some(_))) {
            self.index_code += 1;
        }
        while matches!(self.data.get(self.index_data), Some(Some(_))) {
            self.index_data += 1;
        }
        let extend_buffer = if self.last_shred_in_slot {
            0
        } else {
            extend_buffer
        };
        let mut index_code = {
            let index_code_max = self.code.len().max(self.data.len() * 11 / 10) + extend_buffer;
            self.index_code..index_code_max.min(MAX_CODE_SHREDS_PER_SLOT)
        };
        let mut index_data = {
            let index_data_max = self.data.len() + extend_buffer;
            self.index_data..index_data_max.min(MAX_DATA_SHREDS_PER_SLOT)
        };
        std::iter::from_fn(move || {
            let index_code = index_code.find(|&k| matches!(self.code.get(k), None | Some(None)));
            let index_data = index_data.find(|&k| matches!(self.data.get(k), None | Some(None)));
            let out = [
                index_code.map(|k| (ShredType::Code, k)),
                index_data.map(|k| (ShredType::Data, k)),
            ];
            (!matches!(out, [None, None])).then_some(out)
        })
        .flatten()
        .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_entry_get_shreds() {
        let mut entry = CacheEntry::new(/*capacity:*/ 100);
        assert!(entry.get_shreds(3).eq([
            (ShredType::Code, 0),
            (ShredType::Data, 0),
            (ShredType::Code, 1),
            (ShredType::Data, 1),
            (ShredType::Code, 2),
            (ShredType::Data, 2)
        ]));
        assert_eq!(entry.index_code, 0);
        assert_eq!(entry.index_data, 0);

        entry.put(ShredType::Code, 0, (0, vec![]));
        entry.put(ShredType::Code, 2, (0, vec![]));
        entry.put(ShredType::Data, 1, (0, vec![]));
        assert!(entry.get_shreds(5).eq([
            (ShredType::Code, 1),
            (ShredType::Data, 0),
            (ShredType::Code, 3),
            (ShredType::Data, 2),
            (ShredType::Code, 4),
            (ShredType::Data, 3),
            (ShredType::Data, 4),
        ]));
        assert_eq!(entry.index_code, 1);
        assert_eq!(entry.index_data, 0);

        entry.put(ShredType::Code, 1, (0, vec![]));
        entry.put(ShredType::Code, 4, (0, vec![]));
        entry.put(ShredType::Data, 0, (0, vec![]));
        entry.put(ShredType::Data, 3, (0, vec![]));
        assert!(entry.get_shreds(5).eq([
            (ShredType::Code, 3),
            (ShredType::Data, 2),
            (ShredType::Data, 4),
        ]));
        assert_eq!(entry.index_code, 3);
        assert_eq!(entry.index_data, 2);

        entry.max_index_code = 4;
        entry.max_index_data = 3;
        assert!(entry.get_shreds(4).eq([
            (ShredType::Code, 3),
            (ShredType::Data, 2),
            (ShredType::Code, 5),
            (ShredType::Data, 4),
            (ShredType::Code, 6),
            (ShredType::Data, 5),
            (ShredType::Code, 7),
            (ShredType::Data, 6),
        ]));
        assert_eq!(entry.index_code, 3);
        assert_eq!(entry.index_data, 2);

        entry.last_shred_in_slot = true;
        assert!(entry
            .get_shreds(7)
            .eq([(ShredType::Code, 3), (ShredType::Data, 2)]));
        assert_eq!(entry.index_code, 3);
        assert_eq!(entry.index_data, 2);
    }
}
