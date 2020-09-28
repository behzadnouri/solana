#![feature(test)]

extern crate test;

use rand::{thread_rng, Rng};
use rayon::ThreadPoolBuilder;
use solana_core::cluster_info::MAX_BLOOM_SIZE;
use solana_core::crds::Crds;
use solana_core::crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS;
use solana_core::crds_gossip_pull::{CrdsFilter, CrdsGossipPull};
use solana_core::crds_value::CrdsValue;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use test::Bencher;

#[bench]
fn bench_hash_as_u64(bencher: &mut Bencher) {
    let mut rng = thread_rng();
    let hashes: Vec<_> = std::iter::repeat_with(|| Hash::new_rand(&mut rng))
        .take(1000)
        .collect();
    bencher.iter(|| {
        hashes
            .iter()
            .map(CrdsFilter::hash_as_u64)
            .collect::<Vec<_>>()
    });
}

#[bench]
fn bench_build_crds_filters(bencher: &mut Bencher) {
    let thread_pool = ThreadPoolBuilder::new().build().unwrap();
    let mut rng = thread_rng();
    let mut crds_gossip_pull = CrdsGossipPull::default();
    let mut crds = Crds::default();
    for _ in 0..50_000 {
        crds_gossip_pull
            .purged_values
            .push_back((Hash::new_rand(&mut rng), rng.gen()));
    }
    let mut num_inserts = 0;
    for _ in 0..90_000 {
        if crds
            .insert(CrdsValue::new_rand(&mut rng), rng.gen())
            .is_ok()
        {
            num_inserts += 1;
        }
    }
    assert_eq!(num_inserts, 90_000);
    bencher.iter(|| {
        let filters = crds_gossip_pull.build_crds_filters(&thread_pool, &crds, MAX_BLOOM_SIZE);
        assert_eq!(filters.len(), 128);
    });
}

#[bench]
fn bench_purge_active(bencher: &mut Bencher) {
    let mut rng = thread_rng();
    let mut crds = Crds::default();
    let mut num_inserts = 0;
    let now = CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS + 200;
    for _ in 0..50_000 {
        if crds
            .insert(CrdsValue::new_rand(&mut rng), rng.gen_range(0, now))
            .is_ok()
        {
            num_inserts += 1;
        }
    }
    assert_eq!(num_inserts, 50_000);
    let mut timeouts = HashMap::new();
    timeouts.insert(Pubkey::default(), CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS);
    let mut fail = 0;
    let mut elapsed = Duration::new(0, 0);
    let mut count = 0;
    bencher.iter(|| {
        let mut crds = crds.clone();
        let mut crds_gossip_pull = CrdsGossipPull::default();
        let start = Instant::now();
        let out = crds_gossip_pull.purge_active(&mut crds, now, &timeouts);
        elapsed += Instant::now().duration_since(start);
        count += 1;
        if out < 300 {
            fail += 1;
        }
    });
    assert_eq!(fail, 0);
    println!("bench_purge_active: {} ns/iter", elapsed.as_nanos() / count);
}
