//! The `shred_fetch_stage` pulls shreds from UDP sockets and sends it to a channel.

use {
    crate::packet_hasher::PacketHasher,
    crossbeam_channel::{unbounded, Sender},
    lru::LruCache,
    solana_ledger::shred::{get_shred_slot_index_type, ShredFetchStats},
    solana_perf::packet::{Packet, PacketBatch, PacketBatchRecycler, PacketFlags},
    solana_runtime::bank_forks::BankForks,
    solana_sdk::clock::{Slot, DEFAULT_SLOT_DURATION},
    solana_streamer::streamer::{self, PacketBatchReceiver, StreamerReceiveStats},
    std::{
        net::UdpSocket,
        sync::{atomic::AtomicBool, Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const DEFAULT_LRU_SIZE: usize = 10_000;
type ShredsReceived = LruCache<u64, ()>;

pub(crate) struct ShredFetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

impl ShredFetchStage {
    // updates packets received on a channel and sends them on another channel
    fn modify_packets<F>(
        recvr: PacketBatchReceiver,
        sendr: Sender<Vec<PacketBatch>>,
        bank_forks: &RwLock<BankForks>,
        name: &'static str,
        modify: F,
    ) where
        F: Fn(&mut Packet),
    {
        const STATS_SUBMIT_CADENCE: Duration = Duration::from_secs(1);
        let mut shreds_received = LruCache::new(DEFAULT_LRU_SIZE);
        let mut last_updated = Instant::now();

        let (mut root, mut max_slot) = get_slots_range(bank_forks);

        let mut stats = ShredFetchStats::default();
        let mut packet_hasher = PacketHasher::default();

        for mut packet_batch in recvr {
            if last_updated.elapsed() > DEFAULT_SLOT_DURATION {
                last_updated = Instant::now();
                packet_hasher.reset();
                shreds_received.clear();
                (root, max_slot) = get_slots_range(bank_forks);
            }
            stats.shred_count += packet_batch.len();
            for packet in packet_batch.iter_mut() {
                if should_discard_packet(
                    packet,
                    (root, max_slot),
                    &packet_hasher,
                    &mut shreds_received,
                    &mut stats,
                ) {
                    packet.meta.set_discard(true);
                } else {
                    modify(packet)
                }
            }
            stats.maybe_submit(name, STATS_SUBMIT_CADENCE);
            if sendr.send(vec![packet_batch]).is_err() {
                break;
            }
        }
    }

    fn packet_modifier<F>(
        sockets: Vec<Arc<UdpSocket>>,
        exit: &Arc<AtomicBool>,
        sender: Sender<Vec<PacketBatch>>,
        recycler: PacketBatchRecycler,
        bank_forks: Arc<RwLock<BankForks>>,
        name: &'static str,
        modify: F,
    ) -> (Vec<JoinHandle<()>>, JoinHandle<()>)
    where
        F: Fn(&mut Packet) + Send + 'static,
    {
        let (packet_sender, packet_receiver) = unbounded();
        let streamers = sockets
            .into_iter()
            .map(|s| {
                streamer::receiver(
                    s,
                    exit.clone(),
                    packet_sender.clone(),
                    recycler.clone(),
                    Arc::new(StreamerReceiveStats::new("packet_modifier")),
                    1,
                    true,
                    None,
                )
            })
            .collect();

        let modifier_hdl = Builder::new()
            .name("solana-tvu-fetch-stage-packet-modifier".to_string())
            .spawn(move || Self::modify_packets(packet_receiver, sender, &bank_forks, name, modify))
            .unwrap();
        (streamers, modifier_hdl)
    }

    pub(crate) fn new(
        sockets: Vec<Arc<UdpSocket>>,
        forward_sockets: Vec<Arc<UdpSocket>>,
        repair_socket: Arc<UdpSocket>,
        sender: Sender<Vec<PacketBatch>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let recycler = PacketBatchRecycler::warmed(100, 1024);

        let (mut tvu_threads, tvu_filter) = Self::packet_modifier(
            sockets,
            exit,
            sender.clone(),
            recycler.clone(),
            bank_forks.clone(),
            "shred_fetch",
            |_| {},
        );

        let (tvu_forwards_threads, fwd_thread_hdl) = Self::packet_modifier(
            forward_sockets,
            exit,
            sender.clone(),
            recycler.clone(),
            bank_forks.clone(),
            "shred_fetch_tvu_forwards",
            |p| p.meta.flags.insert(PacketFlags::FORWARDED),
        );

        let (repair_receiver, repair_handler) = Self::packet_modifier(
            vec![repair_socket],
            exit,
            sender,
            recycler,
            bank_forks,
            "shred_fetch_repair",
            |p| p.meta.flags.insert(PacketFlags::REPAIR),
        );

        tvu_threads.extend(tvu_forwards_threads.into_iter());
        tvu_threads.extend(repair_receiver.into_iter());
        tvu_threads.push(tvu_filter);
        tvu_threads.push(fwd_thread_hdl);
        tvu_threads.push(repair_handler);

        Self {
            thread_hdls: tvu_threads,
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

// Returns slots range for which the node will ingest shreds.
fn get_slots_range(bank_forks: &RwLock<BankForks>) -> (/*root:*/ Slot, /*max_slot:*/ Slot) {
    let (root_bank, working_bank) = {
        let bank_forks = bank_forks.read().unwrap();
        (bank_forks.root_bank(), bank_forks.working_bank())
    };
    let slots_per_epoch = root_bank.get_slots_in_epoch(root_bank.epoch());
    // Limit shreds to 2 epochs away.
    let max_slot = working_bank.slot().saturating_add(2 * slots_per_epoch);
    (root_bank.slot(), max_slot)
}

// Returns true if packet does not pass shred
// sanity checks and should be discarded.
#[must_use]
fn should_discard_packet(
    packet: &Packet,
    (root, max_slot): (Slot, Slot),
    packet_hasher: &PacketHasher,
    shreds_received: &mut ShredsReceived,
    stats: &mut ShredFetchStats,
) -> bool {
    let (slot, _index, _shred_type) = match get_shred_slot_index_type(packet, stats) {
        Some(slot_index_type) => (slot_index_type),
        None => return true,
    };
    if slot < root || slot > max_slot {
        stats.slot_out_of_range += 1;
        return true;
    }
    // match shred_type {
    //     // Only data shreds have parent information
    //     ShredType::Data => match shred.parent() {
    //         Ok(parent) => blockstore::verify_shred_slots(shred.slot(), parent, root),
    //         Err(_) => false,
    //     },
    //     // Filter out outdated coding shreds
    //     ShredType::Code => shred.slot() >= root,
    // }
    let hash = packet_hasher.hash_packet(packet);
    if shreds_received.put(hash, ()) == Some(()) {
        stats.duplicate_shred += 1;
        return true;
    }
    false
}

// fn verify_shred_slot(shred: &Shred, root: u64) -> bool {
//     match shred.shred_type() {
//         // Only data shreds have parent information
//         ShredType::Data => match shred.parent() {
//             Ok(parent) => blockstore::verify_shred_slots(shred.slot(), parent, root),
//             Err(_) => false,
//         },
//         // Filter out outdated coding shreds
//         ShredType::Code => shred.slot() >= root,
//     }
// }

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_ledger::{
            blockstore::MAX_DATA_SHREDS_PER_SLOT,
            shred::{Shred, ShredFlags},
        },
    };

    #[test]
    fn test_data_code_same_index() {
        solana_logger::setup();
        let mut shreds_received = LruCache::new(DEFAULT_LRU_SIZE);
        let mut packet = Packet::default();
        let mut stats = ShredFetchStats::default();

        let slot = 1;
        let shred = Shred::new_from_data(
            slot,
            3,   // shred index
            0,   // parent offset
            &[], // data
            ShredFlags::LAST_SHRED_IN_SLOT,
            0, // reference_tick
            0, // version
            3, // fec_set_index
        );
        shred.copy_to_packet(&mut packet);

        let hasher = PacketHasher::default();

        let last_root = 0;
        let last_slot = 100;
        let slots_per_epoch = 10;
        let max_slot = last_slot + 2 * slots_per_epoch;
        assert!(!should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
        let coding = solana_ledger::shred::Shredder::generate_coding_shreds(
            &[shred],
            false, // is_last_in_slot
            3,     // next_code_index
        );
        coding[0].copy_to_packet(&mut packet);
        assert!(!should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
    }

    #[test]
    fn test_shred_filter() {
        solana_logger::setup();
        let mut shreds_received = LruCache::new(DEFAULT_LRU_SIZE);
        let mut packet = Packet::default();
        let mut stats = ShredFetchStats::default();
        let last_root = 0;
        let last_slot = 100;
        let slots_per_epoch = 10;
        let max_slot = last_slot + 2 * slots_per_epoch;

        let hasher = PacketHasher::default();

        // packet size is 0, so cannot get index
        assert!(should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
        assert_eq!(stats.index_overrun, 1);
        let shred = Shred::new_from_data(1, 3, 0, &[], ShredFlags::LAST_SHRED_IN_SLOT, 0, 0, 0);
        shred.copy_to_packet(&mut packet);

        // rejected slot is 1, root is 3
        assert!(should_discard_packet(
            &mut packet,
            (3, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
        assert_eq!(stats.slot_out_of_range, 1);

        // Accepted for 1,3
        assert!(!should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));

        // shreds_received should filter duplicate
        assert!(should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
        assert_eq!(stats.duplicate_shred, 1);

        let shred = Shred::new_from_data(
            1_000_000,
            3,
            0,
            &[],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0,
        );
        shred.copy_to_packet(&mut packet);

        // Slot 1 million is too high
        assert!(should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
        assert_eq!(stats.slot_out_of_range, 2);

        let index = MAX_DATA_SHREDS_PER_SLOT as u32;
        let shred = Shred::new_from_data(5, index, 0, &[], ShredFlags::LAST_SHRED_IN_SLOT, 0, 0, 0);
        shred.copy_to_packet(&mut packet);
        assert!(should_discard_packet(
            &mut packet,
            (last_root, max_slot),
            &hasher,
            &mut shreds_received,
            &mut stats,
        ));
    }
}
