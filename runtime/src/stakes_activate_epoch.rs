use {rayon::ThreadPoolBuilder, solana_runtime::stakes::Stakes, std::time::Instant};

fn main() {
    let now = Instant::now();
    let files = [
        (289, "/tmp/stakes-epoch-289.bin"),
        (291, "/tmp/stakes-epoch-291.bin"),
        (292, "/tmp/stakes-epoch-292.bin"),
        (293, "/tmp/stakes-epoch-293.bin"),
    ];
    let thread_pool = ThreadPoolBuilder::new().build().unwrap();
    let epoch_stakes: Vec<_> = files
        .iter()
        .map(|(epoch, file)| {
            let stakes: Stakes = {
                let file = std::fs::File::open(file).unwrap();
                bincode::deserialize_from(file).unwrap()
            };
            eprintln!(
                "num vote accounts: {}",
                stakes.vote_accounts().as_ref().len()
            );
            eprintln!(
                "num stake delegations: {}",
                stakes.stake_delegations().len()
            );
            eprintln!("num stake history entries: {}", stakes.history().len());
            (*epoch, stakes)
        })
        .collect();
    eprintln!("elapsed: {}ms", now.elapsed().as_millis());

    for (epoch, mut stakes) in epoch_stakes {
        let now = Instant::now();
        stakes.activate_epoch(epoch, &thread_pool);
        eprintln!("elapsed: {}us", now.elapsed().as_micros());
    }
}
