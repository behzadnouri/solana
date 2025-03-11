#[allow(unused)]
use {
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, App, AppSettings, Arg, ArgMatches,
        SubCommand,
    },
    log::{error, info, trace},
    rand::{seq::SliceRandom, Rng},
    rayon::{prelude::*, ThreadPoolBuilder},
    solana_clap_utils::{
        hidden_unless_forced,
        input_parsers::{keypair_of, pubkeys_of},
        input_validators::{is_keypair_or_ask_keyword, is_port, is_pubkey},
    },
    solana_gossip::{
        contact_info::{ContactInfo, Protocol},
        crds_value::CrdsValue,
        gossip_service::discover,
    },
    solana_native_token::LAMPORTS_PER_SOL,
    solana_net_utils::{bind_more_with_config, bind_to, bind_to_localhost, SocketConfig},
    solana_packet::PACKET_DATA_SIZE,
    solana_pubkey::Pubkey,
    solana_streamer::sendmmsg::{multi_target_send, SendPktsError},
    solana_streamer::socket::SocketAddrSpace,
    std::{
        cmp::Reverse,
        collections::HashMap,
        error,
        fs::File,
        io::{self, BufRead, BufReader},
        net::UdpSocket,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::Path,
        process::exit,
        str::FromStr,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
        time::Instant,
    },
};

const TESTNET_SHRED_VERSION: u16 = 64475;

fn get_arg_matches() -> ArgMatches<'static> {
    App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("run")
                .about("run!")
                .arg(
                    Arg::with_name("contact_info_path")
                        .long("contact-info-path")
                        .value_name("PATH")
                        .takes_value(true)
                        .required(true)
                        .help("path to serialized contact-infos"),
                )
                .arg(
                    Arg::with_name("stakes_path")
                        .long("stakes-path")
                        .value_name("PATH")
                        .takes_value(true)
                        .required(true)
                        .help("path to stakes"),
                )
                .arg(
                    Arg::with_name("num_worker_threads")
                        .long("num-worker-threads")
                        .value_name("NUM")
                        .takes_value(true)
                        .default_value("8")
                        .required(true)
                        .help("number of worker threads"),
                )
                .arg(
                    Arg::with_name("num_packets")
                        .long("num-packets")
                        .value_name("NUM")
                        .takes_value(true)
                        .default_value("1")
                        .required(true)
                        .help("number of packets to send out"),
                )
                .arg(
                    Arg::with_name("num_nodes")
                        .long("num-nodes")
                        .value_name("NUM")
                        .takes_value(true)
                        .default_value("112")
                        .required(true)
                        .help("number of nodes to send packets to"),
                )
                .arg(
                    Arg::with_name("num_reps")
                        .long("num-reps")
                        .value_name("REPS")
                        .takes_value(true)
                        .default_value("3")
                        .required(true)
                        .help("number of nodes to send packets to"),
                )
                .arg(
                    Arg::with_name("sleep_secs")
                        .long("sleep-secs")
                        .value_name("SECS")
                        .takes_value(true)
                        .default_value("5")
                        .required(true)
                        .help("number of seconds to sleep between reps"),
                )
                .setting(AppSettings::DisableVersion),
        )
        .get_matches()
}

fn get_nodes(contact_info_path: &str) -> Vec<ContactInfo> {
    let contact_info_path = Path::new(contact_info_path);
    info!("contact_info_path: {contact_info_path:?}");
    let nodes: Vec<ContactInfo> = File::open(&contact_info_path)
        .map(bincode::deserialize_from::<_, Vec<CrdsValue>>)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|node| node.contact_info().cloned().unwrap())
        .filter(|node| node.shred_version() == TESTNET_SHRED_VERSION)
        .filter(|node| {
            node.gossip().is_some()
                && node.tpu(Protocol::QUIC).is_some()
                && node.tvu(Protocol::UDP).is_some()
        })
        .collect();
    info!("number of nodes: {}", nodes.len());
    nodes
}

fn get_stakes(path: &str) -> HashMap<Pubkey, u64> {
    let file = File::open(path).unwrap();
    let reader = io::BufReader::new(file);
    let mut stakes = HashMap::<Pubkey, u64>::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let tokens: Vec<_> = line.split_whitespace().collect();
        if tokens.len() != 2 {
            error!("{tokens:?}");
            continue;
        }
        let pubkey = Pubkey::from_str(tokens[0]).unwrap();
        let stake = tokens[1].parse::<u64>().unwrap();
        stakes.insert(pubkey, stake);
    }
    stakes
}

fn make_packets<R: Rng>(rng: &mut R, num: usize) -> Vec<[u8; PACKET_DATA_SIZE]> {
    std::iter::repeat_with(|| {
        let mut buffer = [0u8; PACKET_DATA_SIZE];
        rng.fill(&mut buffer[4..]);
        buffer
    })
    .take(num)
    .collect()
}

fn verify_addr(addr: SocketAddr, socket: &UdpSocket) -> bool {
    const BYTES: [u8; PACKET_DATA_SIZE] = [0x55; PACKET_DATA_SIZE];
    match multi_target_send(socket, BYTES, &[addr]) {
        Ok(()) => true,
        Err(err) => {
            error!("{addr}, {err:?}");
            false
        }
    }
}

fn run(matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    let mut rng = rand::thread_rng();
    let mut nodes = get_nodes(matches.value_of("contact_info_path").unwrap());
    let stakes = get_stakes(matches.value_of("stakes_path").unwrap());
    // Remove unstaked nodes.
    nodes.retain(|node| stakes.get(node.pubkey()).copied() > Some(0));
    info!("number of staked nodes: {}", nodes.len());
    // Sort nodes by stake in descending order.
    nodes.sort_by_key(|node| Reverse(stakes.get(node.pubkey()).copied()));
    for node in &nodes[..10] {
        let stake = stakes.get(node.pubkey()).unwrap() / LAMPORTS_PER_SOL;
        info!(
            "{}, {:?}, {} SOL",
            node.pubkey(),
            node.gossip().unwrap(),
            stake
        );
    }
    let num_reps = value_t_or_exit!(matches, "num_reps", usize);
    let sleep_secs = value_t_or_exit!(matches, "sleep_secs", u64);
    let num_nodes = value_t_or_exit!(matches, "num_nodes", usize);
    let num_packets = value_t_or_exit!(matches, "num_packets", usize);
    let num_worker_threads = value_t_or_exit!(matches, "num_worker_threads", usize);
    let socket = bind_to(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        0,    // port
        true, // reuse_port
    )
    .unwrap();
    info!("socket: {socket:?}");
    let addrs: Vec<Box<[SocketAddr]>> = {
        let mut addrs: Vec<SocketAddr> = nodes
            .iter()
            .filter_map(|node| node.gossip())
            .filter(|addr| verify_addr(*addr, &socket))
            .take(num_nodes)
            .collect();
        addrs.shuffle(&mut rng);
        let chunk_size = addrs.len().div_ceil(num_worker_threads);
        addrs
            .chunks(chunk_size)
            .map(|chunk| Vec::from(chunk).into_boxed_slice())
            .collect()
    };
    info!(
        "num addrs: {}, {} chunks",
        addrs.iter().map(|chunk| chunk.len()).sum::<usize>(),
        addrs.len()
    );
    info!(
        "addr chunks: {:?}",
        addrs.iter().map(|chunk| chunk.len()).collect::<Vec<_>>()
    );
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(num_worker_threads)
        .build()
        .unwrap();
    let sockets = {
        let config = SocketConfig::default()
            .reuseport(true)
            .recv_buffer_size(128 * 1024 * 1024)
            .send_buffer_size(128 * 1024 * 1024);
        bind_more_with_config(socket, num_worker_threads, config).unwrap()
    };
    info!("sockets: {sockets:?}");
    for k in 0..num_reps {
        info!("rep: {k}");
        let packets = make_packets(&mut rng, num_packets);
        let now = Instant::now();
        let num_errs = AtomicUsize::default();
        thread_pool.install(|| {
            packets.into_par_iter().for_each(|packet| {
                let index = thread_pool.current_thread_index().unwrap();
                let socket = &sockets[0]; // index % sockets.len()];
                // for addr in &addrs[index] {
                //     if socket.send_to(&packet, addr).is_err() {
                //         num_errs.fetch_add(1, Ordering::Relaxed);
                //     }
                // }
                match multi_target_send(socket, packet, &addrs[index]) {
                    Ok(()) => (),
                    Err(SendPktsError::IoError(err, num)) => {
                        num_errs.fetch_add(num, Ordering::Relaxed);
                        trace!("{err:?}, {:?}", &addrs[index]);
                    }
                };
            })
        });
        let elapsed = now.elapsed();
        let num_errs = num_errs.into_inner();
        info!(
            "elapsed: {}ms, {}us/pkt",
            elapsed.as_millis(),
            elapsed.as_micros() / num_packets as u128
        );
        info!(
            "num_errs: {} / ({} x {}) == {}%",
            num_errs,
            num_packets,
            num_nodes,
            num_errs * 100 / (num_packets * num_nodes)
        );
        std::thread::sleep(Duration::from_secs(sleep_secs));
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn error::Error>> {
    solana_logger::setup_with("solana=info,agave=info,socket_main=info");
    let matches = get_arg_matches();
    match matches.subcommand() {
        ("run", Some(matches)) => {
            run(matches)?;
        }
        _ => unreachable!(),
    };
    Ok(())
}
