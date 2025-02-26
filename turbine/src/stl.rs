#![allow(unused)]
use {
    crossbeam_channel::{Receiver, RecvError, Sender},
    rand::{CryptoRng, Rng},
    solana_sdk::{packet::PACKET_DATA_SIZE, pubkey::Pubkey, signature::Keypair},
    std::{
        borrow::{Borrow, Cow},
        collections::HashMap,
        io::ErrorKind,
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    },
};

// 1  byte : version+type == 0x01
// 7  bytes: session-id   == random bytes, 0xadbeefadbeef
// ?  bytes: payload      == shred
// 16 bytes: MAC          == random bytes, 0xff * 16
// TODO: does 1256 bytes even work?
const STL_MTU: usize = 1 + 7 + PACKET_DATA_SIZE + 16;

type SessionId = [u8; 7];

struct ClientConnection {
    sid: SessionId,
    // server_pubkey, symmetric key, ...
}

struct ServerConnection {
    sid: SessionId,
    client_pubkey: Pubkey,
}

pub struct Client {
    // TODO: probably need Arc<...> to spawn handshake thread.
    socket: UdpSocket,
    connections: RwLock<HashMap<SocketAddr, ClientConnection>>,
}

pub struct Server {
    socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    join_handle: JoinHandle<()>,
    // connections: RwLock<HashMap<SocketAddr, ClientConnection>>,
}

impl Client {
    pub fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            connections: RwLock::<HashMap<SocketAddr, ClientConnection>>::default(),
        }
    }

    pub(crate) fn send_many(
        &self,
        bytes: impl AsRef<[u8]>,
        addrs: impl IntoIterator<Item: Borrow<SocketAddr>>,
    ) {
        let bytes = bytes.as_ref();
        if bytes.len() > PACKET_DATA_SIZE {
            error!(
                "stl::Client::send_many: bytes.len() > PACKET_DATA_SIZE: {}",
                bytes.len()
            );
            return;
        }
        // TODO: should probably use sendmmsg instead.
        let mut buffer = [0u8; STL_MTU];
        buffer[0] = 0x01;
        buffer[8..8 + bytes.len()].copy_from_slice(bytes);
        let mut rng = rand::thread_rng();
        let mut connections = self.connections.write().unwrap();
        // version+type (1 byte), session-id (7 bytes), payload, MAC (16 bytes)
        let offset = 1 + 7 + bytes.len() + 16;
        for addr in addrs {
            let addr = addr.borrow();
            let connection = connections
                .entry(*addr)
                .or_insert_with(|| ClientConnection::new_dummy(&mut rng));
            buffer[1..8].copy_from_slice(&connection.sid);
            match self.socket.send_to(&buffer[..offset], addr) {
                Ok(size) => {
                    if size != offset {
                        error!("stl::Client::send_many: size: size != offset: offset");
                    }
                }
                Err(err) => error!("stl::Client::send_many: {err:?}"),
            }
        }
    }
}

impl ClientConnection {
    fn new_dummy<R: Rng + CryptoRng>(rng: &mut R) -> Self {
        Self {
            sid: rng.gen(), // Dummy session with no handshake.
        }
    }
}

impl Server {
    pub fn new(
        socket: Arc<UdpSocket>,
        exit: Arc<AtomicBool>,
        sender: Sender<(Pubkey, SocketAddr, Vec<u8>)>,
    ) -> Self {
        let join_handle = {
            let socket = socket.clone();
            let exit = exit.clone();
            Builder::new()
                .spawn(move || run_server_task(socket, exit, sender))
                .unwrap()
        };
        Self {
            socket,
            exit,
            join_handle,
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.exit.store(true, Ordering::Relaxed);
        self.join_handle.join()
    }
}

fn run_server_task(
    socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    sender: Sender<(Pubkey, SocketAddr, Vec<u8>)>,
) -> () {
    socket.set_nonblocking(true);
    let mut buffer = [0u8; STL_MTU];
    while !exit.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buffer[..]) {
            Ok((size, addr)) => {
                if size < 1 + 7 + 16 {
                    error!("stl::Server: invalid size: {size}");
                } else {
                    let bytes = buffer[1 + 7..size - 16].to_vec();
                    // TODO: should look up pubkey from session-id.
                    let pubkey = Pubkey::default();
                    if let Err(_) = sender.send((pubkey, addr, bytes)) {
                        error!("stl::Server: SendErr");
                        break;
                    }
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    // TODO: Only sleep if the socket is empty.
                    std::thread::sleep(Duration::from_millis(2));
                } else {
                    error!("stl::Server: {err}");
                }
            }
        };
    }
}
