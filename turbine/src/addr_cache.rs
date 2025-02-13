use {
    solana_ledger::shred::{ShredId, ShredType},
    solana_sdk::clock::Slot,
    std::collections::{BTreeSet, VecDeque},
};

#[derive(Default)]
pub(crate) struct AddrCache<const N: usize> {
    code: RollingMedian<N, (Slot, /*index:*/ u32)>,
    data: RollingMedian<N, (Slot, /*index:*/ u32)>,
}

struct RollingMedian<const N: usize, T> {
    // Rolling window of values observed.
    window: VecDeque<T>,
    // tress[0] (trees[1]) maintains lower (upper) half values in sorted order.
    trees: [BTreeSet<(T, /*ordinal:*/ u64)>; 2],
    // Ordinal index for distingushing equal values.
    ordinal: u64,
}

impl<const N: usize> AddrCache<N> {
    pub(crate) fn push(&mut self, shred: ShredId) {
        let (slot, index, shred_type) = shred.unpack();
        match shred_type {
            ShredType::Code => &mut self.code,
            ShredType::Data => &mut self.data,
        }
        .push((slot, index));
        if ((solana_sdk::timing::timestamp() / 1000) % 120) < 10 {
            error!(
                "addr_cache_median: {:?}, {:?}",
                shred,
                match shred_type {
                    ShredType::Code => &mut self.code,
                    ShredType::Data => &mut self.data,
                }
                .median()
            );
        }
    }
}

impl<const N: usize, T: Ord + Copy + std::fmt::Debug> RollingMedian<N, T> {
    fn push(&mut self, value: T) {
        self.window.push_back(value);
        let entry = (value, self.ordinal);
        // Insert the entry into the smaller tree first.
        // Then ensure the lower (upper) half are in the 1st (2nd) tree.
        if Some(value) < self.trees[1].first().map(|&(v, _)| v) {
            self.trees[0].insert(entry);
        } else {
            self.trees[1].insert(entry);
        }
        self.ordinal += 1;
        // Drop the values outside of the rolling window.
        while self.window.len() > N {
            let value = self.window.pop_front().unwrap();
            let entry = (value, self.ordinal - self.window.len() as u64 - 1);
            assert!(self.trees[0].remove(&entry) || self.trees[1].remove(&entry));
        }
        // Rebalance trees to the ~same size.
        while self.trees[0].len() + 1 < self.trees[1].len() {
            let entry = self.trees[1].pop_first().unwrap();
            self.trees[0].insert(entry);
        }
        while self.trees[1].len() + 1 < self.trees[0].len() {
            let entry = self.trees[0].pop_last().unwrap();
            self.trees[1].insert(entry);
        }
    }

    fn median(&self) -> Option<T> {
        let &(out, _) = if self.trees[0].len() < self.trees[1].len() {
            self.trees[1].first()
        } else {
            self.trees[0].last()
        }?;
        Some(out)
    }
}

impl<const N: usize, T> Default for RollingMedian<N, T> {
    fn default() -> Self {
        Self {
            window: VecDeque::with_capacity(N + 1),
            trees: <[_; 2]>::default(),
            ordinal: 0u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert_matches::assert_matches};

    #[test]
    fn test_rolling_median() {
        let mut rm = RollingMedian::<5, u64>::default();
        assert_matches!(rm.median(), None);
        rm.push(200);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(200));
        rm.push(203);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(200));
        rm.push(197);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(200));
        rm.push(205);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(200));
        rm.push(195);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(200));
        rm.push(207);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(203));
        rm.push(193);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(197));
        rm.push(209);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(205));
        rm.push(191);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(195));
        rm.push(208);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(207));
        rm.push(192);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(193));
        rm.push(206);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(206));
        rm.push(194);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(194));
        rm.push(204);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(204));
        rm.push(196);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(196));
        rm.push(202);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(202));
        rm.push(198);
        eprintln!("{:?}, {:?}", &rm.window, &rm.trees);
        assert_matches!(rm.median(), Some(198));
    }
}
