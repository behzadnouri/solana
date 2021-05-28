use {
    crate::{crds::Cursor, crds_gossip::CrdsGossip, crds_value::CrdsData},
    itertools::Itertools,
    solana_measure::measure::Measure,
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey},
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            RwLock,
        },
        time::Instant,
    },
};

#[derive(Default)]
pub(crate) struct Counter(AtomicU64);

impl Counter {
    pub(crate) fn add_measure(&self, x: &mut Measure) {
        x.stop();
        self.0.fetch_add(x.as_us(), Ordering::Relaxed);
    }
    pub(crate) fn add_relaxed(&self, x: u64) {
        self.0.fetch_add(x, Ordering::Relaxed);
    }
    fn clear(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

pub(crate) struct ScopedTimer<'a> {
    clock: Instant,
    metric: &'a AtomicU64,
}

impl<'a> From<&'a Counter> for ScopedTimer<'a> {
    // Output should be assigned to a *named* variable, otherwise it is
    // immediately dropped.
    #[must_use]
    fn from(counter: &'a Counter) -> Self {
        Self {
            clock: Instant::now(),
            metric: &counter.0,
        }
    }
}

impl Drop for ScopedTimer<'_> {
    fn drop(&mut self) {
        let micros = self.clock.elapsed().as_micros();
        self.metric.fetch_add(micros as u64, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct GossipStats {
    pub(crate) all_tvu_peers: Counter,
    pub(crate) entrypoint2: Counter,
    pub(crate) entrypoint: Counter,
    pub(crate) epoch_slots_lookup: Counter,
    pub(crate) filter_pull_response: Counter,
    pub(crate) generate_pull_responses: Counter,
    pub(crate) get_accounts_hash: Counter,
    pub(crate) get_votes: Counter,
    pub(crate) gossip_packets_dropped_count: Counter,
    pub(crate) handle_batch_ping_messages_time: Counter,
    pub(crate) handle_batch_pong_messages_time: Counter,
    pub(crate) handle_batch_prune_messages_time: Counter,
    pub(crate) handle_batch_pull_requests_time: Counter,
    pub(crate) handle_batch_pull_responses_time: Counter,
    pub(crate) handle_batch_push_messages_time: Counter,
    pub(crate) mark_pull_request: Counter,
    pub(crate) new_pull_requests: Counter,
    pub(crate) new_pull_requests_count: Counter,
    pub(crate) new_pull_requests_pings_count: Counter,
    pub(crate) new_push_requests2: Counter,
    pub(crate) new_push_requests: Counter,
    pub(crate) new_push_requests_num: Counter,
    pub(crate) packets_received_count: Counter,
    pub(crate) packets_received_prune_messages_count: Counter,
    pub(crate) packets_received_pull_requests_count: Counter,
    pub(crate) packets_received_pull_responses_count: Counter,
    pub(crate) packets_received_push_messages_count: Counter,
    pub(crate) packets_received_verified_count: Counter,
    pub(crate) packets_sent_gossip_requests_count: Counter,
    pub(crate) packets_sent_prune_messages_count: Counter,
    pub(crate) packets_sent_pull_requests_count: Counter,
    pub(crate) packets_sent_pull_responses_count: Counter,
    pub(crate) packets_sent_push_messages_count: Counter,
    pub(crate) process_gossip_packets_time: Counter,
    pub(crate) process_prune: Counter,
    pub(crate) process_pull_requests: Counter,
    pub(crate) process_pull_response: Counter,
    pub(crate) process_pull_response_count: Counter,
    pub(crate) process_pull_response_fail_insert: Counter,
    pub(crate) process_pull_response_fail_timeout: Counter,
    pub(crate) process_pull_response_len: Counter,
    pub(crate) process_pull_response_success: Counter,
    pub(crate) process_pull_response_timeout: Counter,
    pub(crate) process_push_message: Counter,
    pub(crate) prune_message_count: Counter,
    pub(crate) prune_message_len: Counter,
    pub(crate) prune_received_cache: Counter,
    pub(crate) pull_from_entrypoint_count: Counter,
    pub(crate) pull_request_ping_pong_check_failed_count: Counter,
    pub(crate) pull_requests_count: Counter,
    pub(crate) purge: Counter,
    pub(crate) push_message_count: Counter,
    pub(crate) push_message_value_count: Counter,
    pub(crate) push_response_count: Counter,
    pub(crate) push_vote_read: Counter,
    pub(crate) repair_peers: Counter,
    pub(crate) require_stake_for_gossip_unknown_feature_set: Counter,
    pub(crate) require_stake_for_gossip_unknown_stakes: Counter,
    pub(crate) skip_pull_response_shred_version: Counter,
    pub(crate) skip_pull_shred_version: Counter,
    pub(crate) skip_push_message_shred_version: Counter,
    pub(crate) trim_crds_table_failed: Counter,
    pub(crate) trim_crds_table_purged_values_count: Counter,
    pub(crate) tvu_peers: Counter,
}

pub(crate) fn submit_gossip_stats(
    stats: &GossipStats,
    gossip: &RwLock<CrdsGossip>,
    stakes: &HashMap<Pubkey, u64>,
) {
    struct Vote {
        pubkey: Pubkey,
        wallclock: u64,
        slot: Slot,
        hash: Hash,
    }
    let (table_size, num_nodes, purged_values_size, failed_inserts_size, votes) = {
        let gossip = gossip.read().unwrap();
        let votes: Vec<Vote> = gossip
            .crds
            .get_votes(&mut Cursor::default())
            .filter_map(|entry| {
                let vote = match &entry.value.data {
                    CrdsData::Vote(_, vote) => vote,
                    _ => panic!("this should not happen!"),
                };
                Some(Vote {
                    pubkey: entry.value.pubkey(),
                    wallclock: entry.value.wallclock(),
                    slot: vote.slot()?,
                    hash: vote.hash()?,
                })
            })
            .collect();
        (
            gossip.crds.len(),
            gossip.crds.num_nodes(),
            gossip.crds.num_purged(),
            gossip.pull.failed_inserts.len(),
            votes,
        )
    };
    let num_nodes_staked = stakes.values().filter(|stake| **stake > 0).count();
    let total_stake: u64 = stakes.values().sum();
    if total_stake > 0 {
        let votes = votes
            .into_iter()
            .filter_map(|vote| {
                let key = (vote.slot, vote.hash);
                let val = (vote.wallclock, *stakes.get(&vote.pubkey)?);
                Some((key, val))
            })
            .into_grouping_map()
            .aggregate(|acc, (_slot, _hash), (wallclock, stake)| match acc {
                None => Some((1, wallclock, stake)),
                Some((k, w, s)) => Some((k + 1, w.max(wallclock), s + stake)),
            });
        // Latest slot with 1/10 of stake voted.
        let latest_slot = votes
            .iter()
            .filter(|(_, (_, _, stake))| stake * 10 > total_stake)
            .map(|((slot, _), (_, wallclock, _))| (*slot, *wallclock))
            .max();
        // Latest slot with 1/3 of stake voted.
        let voting_slot = votes
            .iter()
            .filter(|(_, (_, _, stake))| stake * 3 > total_stake)
            .map(|((slot, _), (_, wallclock, _))| (*slot, *wallclock))
            .max();
        // Latest slot with 2/3 of stake voted.
        if let Some(((slot, _hash), (num_nodes, wallclock, stake))) = votes
            .into_iter()
            .filter(|(_, (_, _, stake))| stake * 3 > 2 * total_stake)
            .max()
        {
            let stake = stake * 100 / total_stake;
            datapoint_info!(
                "cluster_info_stats_votes",
                ("majority_slot", slot as i64, i64),
                ("majority_wallclock", wallclock as i64, i64),
                ("voting_slot", voting_slot.unwrap().0 as i64, i64),
                ("voting_wallclock", voting_slot.unwrap().1 as i64, i64),
                ("latest_slot", latest_slot.unwrap().0 as i64, i64),
                ("latest_wallclock", latest_slot.unwrap().1 as i64, i64),
                ("num_nodes", num_nodes as i64, i64),
                ("stake", stake as i64, i64),
            );
        }
    }
    datapoint_info!(
        "cluster_info_stats",
        ("entrypoint", stats.entrypoint.clear(), i64),
        ("entrypoint2", stats.entrypoint2.clear(), i64),
        ("push_vote_read", stats.push_vote_read.clear(), i64),
        ("get_votes", stats.get_votes.clear(), i64),
        ("get_accounts_hash", stats.get_accounts_hash.clear(), i64),
        ("all_tvu_peers", stats.all_tvu_peers.clear(), i64),
        ("tvu_peers", stats.tvu_peers.clear(), i64),
        (
            "new_push_requests_num",
            stats.new_push_requests_num.clear(),
            i64
        ),
        ("table_size", table_size as i64, i64),
        ("purged_values_size", purged_values_size as i64, i64),
        ("failed_inserts_size", failed_inserts_size as i64, i64),
        ("num_nodes", num_nodes as i64, i64),
        ("num_nodes_staked", num_nodes_staked as i64, i64),
        ("total_stake", total_stake as i64, i64),
    );
    datapoint_info!(
        "cluster_info_stats2",
        (
            "gossip_packets_dropped_count",
            stats.gossip_packets_dropped_count.clear(),
            i64
        ),
        ("repair_peers", stats.repair_peers.clear(), i64),
        ("new_push_requests", stats.new_push_requests.clear(), i64),
        ("new_push_requests2", stats.new_push_requests2.clear(), i64),
        ("purge", stats.purge.clear(), i64),
        (
            "process_gossip_packets_time",
            stats.process_gossip_packets_time.clear(),
            i64
        ),
        (
            "handle_batch_ping_messages_time",
            stats.handle_batch_ping_messages_time.clear(),
            i64
        ),
        (
            "handle_batch_pong_messages_time",
            stats.handle_batch_pong_messages_time.clear(),
            i64
        ),
        (
            "handle_batch_prune_messages_time",
            stats.handle_batch_prune_messages_time.clear(),
            i64
        ),
        (
            "handle_batch_pull_requests_time",
            stats.handle_batch_pull_requests_time.clear(),
            i64
        ),
        (
            "handle_batch_pull_responses_time",
            stats.handle_batch_pull_responses_time.clear(),
            i64
        ),
        (
            "handle_batch_push_messages_time",
            stats.handle_batch_push_messages_time.clear(),
            i64
        ),
        (
            "process_pull_resp",
            stats.process_pull_response.clear(),
            i64
        ),
        ("filter_pull_resp", stats.filter_pull_response.clear(), i64),
        (
            "process_pull_resp_count",
            stats.process_pull_response_count.clear(),
            i64
        ),
        (
            "pull_response_fail_insert",
            stats.process_pull_response_fail_insert.clear(),
            i64
        ),
        (
            "pull_response_fail_timeout",
            stats.process_pull_response_fail_timeout.clear(),
            i64
        ),
        (
            "pull_response_success",
            stats.process_pull_response_success.clear(),
            i64
        ),
        (
            "process_pull_resp_timeout",
            stats.process_pull_response_timeout.clear(),
            i64
        ),
        (
            "push_response_count",
            stats.push_response_count.clear(),
            i64
        ),
    );
    datapoint_info!(
        "cluster_info_stats3",
        (
            "process_pull_resp_len",
            stats.process_pull_response_len.clear(),
            i64
        ),
        (
            "process_pull_requests",
            stats.process_pull_requests.clear(),
            i64
        ),
        (
            "pull_request_ping_pong_check_failed_count",
            stats.pull_request_ping_pong_check_failed_count.clear(),
            i64
        ),
        (
            "new_pull_requests_pings_count",
            stats.new_pull_requests_pings_count.clear(),
            i64
        ),
        (
            "generate_pull_responses",
            stats.generate_pull_responses.clear(),
            i64
        ),
        ("process_prune", stats.process_prune.clear(), i64),
        (
            "process_push_message",
            stats.process_push_message.clear(),
            i64
        ),
        (
            "prune_received_cache",
            stats.prune_received_cache.clear(),
            i64
        ),
        ("epoch_slots_lookup", stats.epoch_slots_lookup.clear(), i64),
        ("new_pull_requests", stats.new_pull_requests.clear(), i64),
        ("mark_pull_request", stats.mark_pull_request.clear(), i64),
    );
    datapoint_info!(
        "cluster_info_stats4",
        (
            "skip_push_message_shred_version",
            stats.skip_push_message_shred_version.clear(),
            i64
        ),
        (
            "skip_pull_response_shred_version",
            stats.skip_pull_response_shred_version.clear(),
            i64
        ),
        (
            "skip_pull_shred_version",
            stats.skip_pull_shred_version.clear(),
            i64
        ),
        ("push_message_count", stats.push_message_count.clear(), i64),
        (
            "push_message_value_count",
            stats.push_message_value_count.clear(),
            i64
        ),
        (
            "new_pull_requests_count",
            stats.new_pull_requests_count.clear(),
            i64
        ),
        (
            "pull_from_entrypoint_count",
            stats.pull_from_entrypoint_count.clear(),
            i64
        ),
        (
            "prune_message_count",
            stats.prune_message_count.clear(),
            i64
        ),
        ("prune_message_len", stats.prune_message_len.clear(), i64),
    );
    datapoint_info!(
        "cluster_info_stats5",
        (
            "pull_requests_count",
            stats.pull_requests_count.clear(),
            i64
        ),
        (
            "packets_received_count",
            stats.packets_received_count.clear(),
            i64
        ),
        (
            "packets_received_prune_messages_count",
            stats.packets_received_prune_messages_count.clear(),
            i64
        ),
        (
            "packets_received_pull_requests_count",
            stats.packets_received_pull_requests_count.clear(),
            i64
        ),
        (
            "packets_received_pull_responses_count",
            stats.packets_received_pull_responses_count.clear(),
            i64
        ),
        (
            "packets_received_push_messages_count",
            stats.packets_received_push_messages_count.clear(),
            i64
        ),
        (
            "packets_received_verified_count",
            stats.packets_received_verified_count.clear(),
            i64
        ),
        (
            "packets_sent_gossip_requests_count",
            stats.packets_sent_gossip_requests_count.clear(),
            i64
        ),
        (
            "packets_sent_prune_messages_count",
            stats.packets_sent_prune_messages_count.clear(),
            i64
        ),
        (
            "packets_sent_pull_requests_count",
            stats.packets_sent_pull_requests_count.clear(),
            i64
        ),
        (
            "packets_sent_pull_responses_count",
            stats.packets_sent_pull_responses_count.clear(),
            i64
        ),
        (
            "packets_sent_push_messages_count",
            stats.packets_sent_push_messages_count.clear(),
            i64
        ),
        (
            "require_stake_for_gossip_unknown_feature_set",
            stats.require_stake_for_gossip_unknown_feature_set.clear(),
            i64
        ),
        (
            "require_stake_for_gossip_unknown_stakes",
            stats.require_stake_for_gossip_unknown_stakes.clear(),
            i64
        ),
        (
            "trim_crds_table_failed",
            stats.trim_crds_table_failed.clear(),
            i64
        ),
        (
            "trim_crds_table_purged_values_count",
            stats.trim_crds_table_purged_values_count.clear(),
            i64
        ),
    );
}
