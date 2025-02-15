use {
    solana_ledger::{
        blockstore::MAX_DATA_SHREDS_PER_SLOT,
        shred::{shred_code::MAX_CODE_SHREDS_PER_SLOT, ShredId, ShredType},
    },
    solana_sdk::clock::Slot,
    std::{
        cmp::Reverse,
        collections::{hash_map::Entry, HashMap, VecDeque},
        net::SocketAddr,
    },
};

// Maximum number of most recent shreds to track.
const MAX_NUM_SHREDS: usize = 512;
// Capacity to initialize CacheEntry.{code,data}.
const ADDR_CAPACITY: usize = 2_048;

pub(crate) struct AddrCache {
    // Number of slots to cache addresses for.
    capacity: usize,
    // Number of shreds observed within the rolling window.
    num_shreds: usize,
    // Rolling window of number of shreds per slot observed.
    window: VecDeque<(Slot, /*count:*/ usize)>,
    // Number of shreds seen in each slot within the rolling window.
    counts: HashMap<Slot, /*count:*/ usize>,
    // Cache of addresses for the most frequent slots.
    cache: HashMap<Slot, CacheEntry>,
}

struct CacheEntry {
    code: Vec<Option<(/*root_distance:*/ u8, Vec<SocketAddr>)>>,
    data: Vec<Option<(/*root_distance:*/ u8, Vec<SocketAddr>)>>,
    // If the last data shred in slot has already been observed.
    last_shred_in_slot: bool,
    // Last code and data indices populated.
    index_code: usize,
    index_data: usize,
}

impl AddrCache {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            num_shreds: 0,
            window: VecDeque::with_capacity(MAX_NUM_SHREDS + 1),
            counts: HashMap::default(),
            cache: HashMap::with_capacity(capacity * 2 + 1),
        }
    }

    #[inline]
    pub(crate) fn get(&self, shred: &ShredId) -> Option<(/*root_distance:*/ u8, &Vec<SocketAddr>)> {
        self.cache
            .get(&shred.slot())?
            .get(shred.shred_type(), shred.index())
    }

    pub(crate) fn put(&mut self, shred: &ShredId, entry: (/*root_distance:*/ u8, Vec<SocketAddr>)) {
        self.get_cache_entry_mut(shred.slot())
            .put(shred.shred_type(), shred.index(), entry);
    }

    pub(crate) fn record(
        &mut self,
        slot: Slot,
        num_shreds: usize,
        last_shred_in_slot: bool,
        addrs: Vec<(ShredId, /*root_distance:*/ u8, Vec<SocketAddr>)>,
    ) {
        if num_shreds > 0 {
            self.num_shreds += num_shreds;
            self.window.push_back((slot, num_shreds));
            *self.counts.entry(slot).or_default() += num_shreds;
            self.maybe_trim_counts();
        }
        if !last_shred_in_slot && addrs.is_empty() {
            return;
        }
        let num_addrs = addrs.len();
        let entry = self.get_cache_entry_mut(slot);
        entry.last_shred_in_slot |= last_shred_in_slot;
        for (shred, root_distance, addrs) in addrs {
            assert_eq!(shred.slot(), slot);
            entry.put(shred.shred_type(), shred.index(), (root_distance, addrs));
        }
        self.maybe_trim_cache();
        if false {
            // && (solana_sdk::timing::timestamp() / 1000) % 120 < 2 {
            let mut window = HashMap::<Slot, usize>::new();
            for &(slot, count) in self.window.iter() {
                *window.entry(slot).or_default() += count;
            }
            let window = window == self.counts;
            error!(
                "addr_cache: slot: {slot}, num_shreds: {num_shreds}, \
                num_addrs: {num_addrs}, self.num_shreds: {}, counts: {:?}, \
                window: {window:?}",
                self.num_shreds, self.counts
            );
            error!("addr_cache: {:?}", self.get_shreds(10));
        }
    }

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

    #[inline]
    fn get_cache_entry_mut(&mut self, slot: Slot) -> &mut CacheEntry {
        self.cache
            .entry(slot)
            .or_insert_with(|| CacheEntry::new(ADDR_CAPACITY))
    }

    // If there are more than MAX_NUM_SHREDS shreds in the rolling window,
    // drops the oldest entries and updates self.counts.
    fn maybe_trim_counts(&mut self) {
        while let Some(cnt) = self
            .num_shreds
            .checked_sub(MAX_NUM_SHREDS)
            .filter(|&k| k > 0)
        {
            let (slot, num_shreds) = self.window.front_mut().unwrap();
            let cnt = cnt.min(*num_shreds);
            self.num_shreds -= cnt;
            *num_shreds -= cnt;
            let Entry::Occupied(mut entry) = self.counts.entry(*slot) else {
                unreachable!()
            };
            *entry.get_mut() -= cnt;
            if *entry.get() == 0 {
                entry.remove_entry();
            }
            if *num_shreds == 0 {
                self.window.pop_front();
            }
        }
    }

    // If there are more thant 2 * self.capacity entries in the cache, drops
    // the slots with the fewest counts in the rolling window.
    fn maybe_trim_cache(&mut self) {
        if self.cache.len() <= self.capacity * 2 {
            return;
        }
        let mut entries: Vec<_> = self
            .cache
            .drain()
            .map(|(slot, entry)| {
                let count = self.counts.get(&slot).copied().unwrap_or_default();
                (count, slot, entry)
            })
            .collect();
        entries.select_nth_unstable_by_key(self.capacity.saturating_sub(1), |&(count, _, _)| {
            Reverse(count)
        });
        self.cache.extend(
            entries
                .into_iter()
                .take(self.capacity)
                .map(|(_, slot, entry)| (slot, entry)),
        );
    }
}

impl CacheEntry {
    fn new(capacity: usize) -> Self {
        Self {
            code: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
            last_shred_in_slot: false,
            index_code: 0,
            index_data: 0,
        }
    }

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

    // Returns next (SherdType, shred-index) to compute.
    fn get_shreds(
        &mut self,
        // Maximum number of shreds to extend beyond current max indices.
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
        entry.put(ShredType::Code, 0, (0, vec![]));
        entry.put(ShredType::Code, 2, (0, vec![]));
        entry.put(ShredType::Data, 1, (0, vec![]));
        assert!(entry.get_shreds(3).eq([
            (ShredType::Code, 1),
            (ShredType::Data, 0),
            (ShredType::Code, 3),
            (ShredType::Data, 2),
            (ShredType::Code, 4),
            (ShredType::Data, 3),
            (ShredType::Code, 5),
            (ShredType::Data, 4),
        ]));
        entry.put(ShredType::Code, 1, (0, vec![]));
        entry.put(ShredType::Code, 4, (0, vec![]));
        entry.put(ShredType::Data, 0, (0, vec![]));
        entry.put(ShredType::Data, 3, (0, vec![]));
        assert!(entry.get_shreds(3).eq([
            (ShredType::Code, 3),
            (ShredType::Data, 2),
            (ShredType::Code, 5),
            (ShredType::Data, 4),
            (ShredType::Code, 6),
            (ShredType::Data, 5),
            (ShredType::Code, 7),
            (ShredType::Data, 6),
        ]));
    }
}
