use {
    crate::{
        contact_info::ContactInfo,
        crds::{Crds, CrdsError, CrdsStats, Cursor as CrdsCursor, GossipRoute, VersionedCrdsValue},
        crds_entry::CrdsEntry,
        crds_value::{CrdsValue, CrdsValueLabel},
    },
    rand::Rng,
    rayon::{prelude::*, ThreadPool},
    solana_sdk::{
        hash::Hash,
        pubkey::{Pubkey, PUBKEY_BYTES},
    },
    std::{
        collections::HashMap,
        iter::repeat_with,
        ops::Deref,
        sync::{RwLock, RwLockReadGuard},
    },
};

const NUM_SHARDS: usize = 256;
const SHARD_INDEX_MASK: u8 = (NUM_SHARDS - 1) as u8;

macro_rules! flat_map_shards (
    ($self:ident, $method:ident) => {
        $self.shards.iter().flat_map(|shard| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.$method();
            GuardIter { _guard: guard, inner }
        })
    };
    ($self:ident, $cursor: ident, $method:ident) => {{
        $self.shards.iter().zip(&mut $cursor.0).flat_map(|(shard, cursor)| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.$method(cursor);
            GuardIter { _guard: guard, inner }
        })
    }};
);

macro_rules! sum_shards (
    ($self:ident, $method:ident) => {
        $self.shards.iter().map(|shard| shard.read().unwrap().$method()).sum()
    };
);

pub struct CrdsPool {
    shards: Vec<RwLock<Crds>>,
    offset: usize,
}

#[derive(Clone)]
pub struct Cursor([CrdsCursor; NUM_SHARDS]);

pub struct GuardRef<'a, T: 'a> {
    _guard: RwLockReadGuard<'a, Crds>,
    inner: T,
}

// TODO: Can we just use GuardRef instead?!
struct GuardIter<'a, I: 'a> {
    _guard: RwLockReadGuard<'a, Crds>,
    inner: I,
}

unsafe fn change_lifetime_const<'a, 'b, T>(x: &'a T) -> &'b T {
    &*(x as *const T)
}

impl CrdsPool {
    fn shard_index(&self, pubkey: &Pubkey) -> usize {
        usize::from(pubkey.as_ref()[self.offset] & SHARD_INDEX_MASK)
    }

    /// Returns true if the given value updates an existing one in the table.
    /// The value is outdated and fails to insert, if it already exists in the
    /// table with a more recent wallclock.
    /// TODO: Probably want to get rid of this!
    pub(crate) fn upserts(&self, value: &CrdsValue) -> bool {
        let index = self.shard_index(&value.pubkey());
        let crds = self.shards[index].read().unwrap();
        crds.upserts(value)
    }

    pub fn insert(&self, value: CrdsValue, now: u64, route: GossipRoute) -> Result<(), CrdsError> {
        let index = self.shard_index(&value.pubkey());
        let mut crds = self.shards[index].write().unwrap();
        crds.insert(value, now, route)
    }

    pub fn get<'a, 'b, V>(&'a self, key: V::Key) -> Option<GuardRef<'a, V>>
    where
        V: CrdsEntry<'a, 'b>,
        V::Key: Copy,
    {
        let index = self.shard_index(&V::pubkey(key));
        let guard = self.shards[index].read().unwrap();
        let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
        let inner = crds.get(key)?;
        Some(GuardRef {
            _guard: guard,
            inner,
        })
    }

    pub(crate) fn get_shred_version(&self, pubkey: &Pubkey) -> Option<u16> {
        let index = self.shard_index(pubkey);
        let crds = self.shards[index].read().unwrap();
        crds.get_shred_version(pubkey)
    }

    /// Returns all entries which are ContactInfo.
    pub(crate) fn get_nodes(&self) -> impl Iterator<Item = &VersionedCrdsValue> {
        flat_map_shards!(self, get_nodes)
    }

    /// Returns ContactInfo of all known nodes.
    pub(crate) fn get_nodes_contact_info(&self) -> impl Iterator<Item = &ContactInfo> {
        flat_map_shards!(self, get_nodes_contact_info)
    }

    /// Returns all vote entries inserted since the given cursor.
    /// Updates the cursor as the votes are consumed.
    pub(crate) fn get_votes<'a>(
        &'a self,
        cursor: &'a mut Cursor,
    ) -> impl Iterator<Item = &'a VersionedCrdsValue> {
        flat_map_shards!(self, cursor, get_votes)
    }

    /// Returns epoch-slots inserted since the given cursor.
    /// Updates the cursor as the values are consumed.
    pub(crate) fn get_epoch_slots<'a>(
        &'a self,
        cursor: &'a mut Cursor,
    ) -> impl Iterator<Item = &'a VersionedCrdsValue> {
        flat_map_shards!(self, cursor, get_epoch_slots)
    }

    /// Returns all entries inserted since the given cursor.
    pub(crate) fn get_entries<'a>(
        &'a self,
        cursor: &'a mut Cursor,
    ) -> impl Iterator<Item = &'a VersionedCrdsValue> {
        flat_map_shards!(self, cursor, get_entries)
    }

    /// Returns all records associated with a pubkey.
    pub(crate) fn get_records<'a>(
        &'a self,
        pubkey: &'a Pubkey,
    ) -> impl Iterator<Item = &'a VersionedCrdsValue> {
        let index = self.shard_index(pubkey);
        let guard = self.shards[index].read().unwrap();
        let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
        let inner = crds.get_records(pubkey);
        GuardIter {
            _guard: guard,
            inner,
        }
    }

    /// Returns number of known contact-infos (network size).
    pub(crate) fn num_nodes(&self) -> usize {
        sum_shards!(self, num_nodes)
    }

    /// Returns number of unique pubkeys.
    pub(crate) fn num_pubkeys(&self) -> usize {
        sum_shards!(self, num_pubkeys)
    }

    pub fn len(&self) -> usize {
        sum_shards!(self, len)
    }

    pub fn is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.read().unwrap().is_empty())
    }

    #[cfg(test)]
    pub(crate) fn values(&self) -> impl Iterator<Item = &VersionedCrdsValue> {
        self.shards.iter().flat_map(|shard| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.values();
            GuardIter {
                _guard: guard,
                inner,
            }
        })
    }

    pub(crate) fn par_values(&self) -> impl ParallelIterator<Item = &VersionedCrdsValue> {
        // TODO How to change this to ParallelIterator/flat_map?
        self.shards.par_iter().flat_map_iter(|shard| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.values();
            GuardIter {
                _guard: guard,
                inner,
            }
        })
    }

    pub(crate) fn num_purged(&self) -> usize {
        sum_shards!(self, num_purged)
    }

    pub(crate) fn purged(&self) -> impl ParallelIterator<Item = Hash> + '_ {
        // TODO: How to change this to ParallelIterator/flat_map?
        self.shards.par_iter().flat_map_iter(|shard| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.purged();
            GuardIter {
                _guard: guard,
                inner,
            }
        })
    }

    /// Drops purged value hashes with timestamp less than the given one.
    pub(crate) fn trim_purged(&self, timestamp: u64) {
        for shard in &self.shards {
            let mut shard = shard.write().unwrap();
            shard.trim_purged(timestamp);
        }
    }

    /// Returns all crds values which the first 'mask_bits'
    /// of their hash value is equal to 'mask'.
    pub(crate) fn filter_bitmask(
        &self,
        mask: u64,
        mask_bits: u32,
    ) -> impl Iterator<Item = &VersionedCrdsValue> {
        self.shards.iter().flat_map(move |shard| {
            let guard = shard.read().unwrap();
            let crds: &Crds = unsafe { change_lifetime_const(&*guard) };
            let inner = crds.filter_bitmask(mask, mask_bits);
            GuardIter {
                _guard: guard,
                inner,
            }
        })
    }

    /// Update the timestamp's of all the labels that are associated with Pubkey
    pub(crate) fn update_record_timestamp(&self, pubkey: &Pubkey, now: u64) {
        let index = self.shard_index(pubkey);
        let mut crds = self.shards[index].write().unwrap();
        crds.update_record_timestamp(pubkey, now);
    }

    /// Find all the keys that are older or equal to the timeout.
    /// * timeouts - Pubkey specific timeouts with Pubkey::default() as the default timeout.
    pub fn find_old_labels(
        &self,
        thread_pool: &ThreadPool,
        now: u64,
        timeouts: &HashMap<Pubkey, u64>,
    ) -> Vec<CrdsValueLabel> {
        assert!(timeouts.contains_key(&Pubkey::default()));
        thread_pool.install(|| {
            self.shards
                .par_iter()
                .flat_map(|shard| {
                    let crds = shard.read().unwrap();
                    crds.find_old_labels(thread_pool, now, timeouts)
                })
                .collect()
        })
    }

    pub fn remove(&self, key: &CrdsValueLabel, now: u64) {
        let index = self.shard_index(&key.pubkey());
        let mut crds = self.shards[index].write().unwrap();
        crds.remove(key, now)
    }

    /// Returns true if the number of unique pubkeys in the table exceeds the
    /// given capacity (plus some margin).
    /// Allows skipping unnecessary calls to trim without obtaining a write
    /// lock on gossip.
    pub(crate) fn should_trim(&self, cap: usize) -> bool {
        // Allow 10% overshoot so that the computation cost is amortized down.
        10 * self.num_pubkeys() > 11 * cap
    }

    /// Trims the table by dropping all values associated with the pubkeys with
    /// the lowest stake, so that the number of unique pubkeys are bounded.
    pub(crate) fn trim(
        &self,
        cap: usize, // Capacity hint for number of unique pubkeys.
        // Set of pubkeys to never drop.
        // e.g. known validators, self pubkey, ...
        keep: &[Pubkey],
        stakes: &HashMap<Pubkey, u64>,
        now: u64,
    ) -> Result</*num purged:*/ usize, CrdsError> {
        // TODO: This should apply cap globally!
        let cap = cap / self.shards.len();
        self.shards.iter().try_fold(0, |num_purged, shard| {
            if !shard.read().unwrap().should_trim(cap) {
                return Ok(num_purged);
            }
            let mut crds = shard.write().unwrap();
            Ok(num_purged + crds.trim(cap, keep, stakes, now)?)
        })
    }

    pub(crate) fn take_stats(&self) -> CrdsStats {
        sum_shards!(self, take_stats)
    }

    // Only for tests and simulations.
    pub(crate) fn mock_clone(&self) -> Self {
        let shards = self
            .shards
            .iter()
            .map(|shard| shard.read().unwrap().mock_clone())
            .map(RwLock::new)
            .collect();
        Self { shards, ..*self }
    }
}

impl Default for CrdsPool {
    fn default() -> Self {
        Self {
            shards: repeat_with(RwLock::default).take(NUM_SHARDS).collect(),
            offset: rand::thread_rng().gen_range(0, PUBKEY_BYTES),
        }
    }
}

impl Default for Cursor {
    fn default() -> Self {
        Self(
            <[CrdsCursor; NUM_SHARDS]>::try_from(
                repeat_with(CrdsCursor::default)
                    .take(NUM_SHARDS)
                    .collect::<Vec<CrdsCursor>>(),
            )
            .unwrap(),
        )
    }
}

impl<'a, I: 'a, T> Iterator for GuardIter<'a, I>
where
    I: Iterator<Item = T>,
{
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, T: 'a> Deref for GuardRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
