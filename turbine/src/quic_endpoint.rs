use {
    bytes::Bytes,
    crossbeam_channel::Sender,
    futures::future::TryJoin,
    log::error,
    quinn::{
        Chunk, ClientConfig, ConnectError, Connecting, Connection, ConnectionError, Endpoint,
        EndpointConfig, ReadError, RecvStream, SendDatagramError, ServerConfig, TokioRuntime,
        TransportConfig, VarInt, WriteError,
    },
    rcgen::RcgenError,
    rustls::{Certificate, PrivateKey},
    solana_quic_client::nonblocking::quic_client::SkipServerVerification,
    solana_sdk::{packet::PACKET_DATA_SIZE, pubkey::Pubkey, signature::Keypair},
    solana_streamer::{
        quic::SkipClientVerification, tls_certificates::new_self_signed_tls_certificate,
    },
    std::{
        collections::{hash_map::Entry, HashMap},
        io::Error as IoError,
        net::{IpAddr, SocketAddr, UdpSocket},
        ops::Deref,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
    thiserror::Error,
    tokio::{
        sync::{
            mpsc::{Receiver as AsyncReceiver, Sender as AsyncSender},
            RwLock,
        },
        task::JoinHandle,
    },
};

static HANDLE_CONNECTION_COUNT: AtomicUsize = AtomicUsize::new(0usize);

const CLIENT_CHANNEL_CAPACITY: usize = 1 << 10;
// switch to 1500u32 - 28 = 1472?!
// max_datagram_size: this number - 38
// 1500 > 1434
// 1472 > 1434
// 1280 > 1242 == 38
// quinn_udp] sendmsg error: Os { code: 90, kind: Uncategorized, message:
// "Message too long" }, Transmit: { len: 1452, segment_size: None }
const INITIAL_MAXIMUM_TRANSMISSION_UNIT: u16 = 1280;
const ALPN_TURBINE_PROTOCOL_ID: &[u8] = b"solana-turbine";
const CONNECT_SERVER_NAME: &str = "solana-turbine";

const CONNECTION_CLOSE_ERROR_CODE_SHUTDOWN: VarInt = VarInt::from_u32(1);
const CONNECTION_CLOSE_ERROR_CODE_DROPPED: VarInt = VarInt::from_u32(2);
const CONNECTION_CLOSE_ERROR_CODE_INVALID_IDENTITY: VarInt = VarInt::from_u32(3);
const CONNECTION_CLOSE_ERROR_CODE_REPLACED: VarInt = VarInt::from_u32(4);

const CONNECTION_CLOSE_REASON_SHUTDOWN: &[u8] = b"SHUTDOWN";
const CONNECTION_CLOSE_REASON_DROPPED: &[u8] = b"DROPPED";
const CONNECTION_CLOSE_REASON_INVALID_IDENTITY: &[u8] = b"INVALID_IDENTITY";
const CONNECTION_CLOSE_REASON_REPLACED: &[u8] = b"REPLACED";

pub type AsyncTryJoinHandle = TryJoin<JoinHandle<()>, JoinHandle<()>>;
type ConnectionCache = HashMap<(SocketAddr, Option<Pubkey>), Arc<RwLock<Option<Connection>>>>;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    CertificateError(#[from] RcgenError),
    #[error(transparent)]
    ConnectError(#[from] ConnectError),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error("Channel Send Error")]
    ChannelSendError,
    #[error("Invalid Chunk: {0:?}")]
    InvalidChunk(Chunk),
    #[error("Invalid Identity: {0:?}")]
    InvalidIdentity(SocketAddr),
    #[error(transparent)]
    IoError(#[from] IoError),
    #[error(transparent)]
    ReadError(#[from] ReadError),
    #[error(transparent)]
    SendDatagramError(#[from] SendDatagramError),
    #[error(transparent)]
    TlsError(#[from] rustls::Error),
    #[error(transparent)]
    WriteError(#[from] WriteError),
}

#[allow(clippy::type_complexity)]
pub fn new_quic_endpoint(
    runtime: &tokio::runtime::Handle,
    keypair: &Keypair,
    socket: UdpSocket,
    address: IpAddr,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
) -> Result<
    (
        Endpoint,
        AsyncSender<(SocketAddr, Bytes)>,
        AsyncTryJoinHandle,
    ),
    Error,
> {
    let (cert, key) = new_self_signed_tls_certificate(keypair, address)?;
    let server_config = new_server_config(cert.clone(), key.clone())?;
    let client_config = new_client_config(cert, key)?;
    let mut endpoint = {
        // Endpoint::new requires entering the runtime context,
        // otherwise the code below will panic.
        let _guard = runtime.enter();
        Endpoint::new(
            EndpointConfig::default(),
            Some(server_config),
            socket,
            Arc::new(TokioRuntime),
        )?
    };
    endpoint.set_default_client_config(client_config);
    let cache = Arc::<RwLock<ConnectionCache>>::default();
    let (client_sender, client_receiver) = tokio::sync::mpsc::channel(CLIENT_CHANNEL_CAPACITY);
    let server_task = runtime.spawn(run_server(endpoint.clone(), sender.clone(), cache.clone()));
    let client_task = runtime.spawn(run_client(endpoint.clone(), client_receiver, sender, cache));
    let task = futures::future::try_join(server_task, client_task);
    Ok((endpoint, client_sender, task))
}

pub fn close_quic_endpoint(endpoint: &Endpoint) {
    error!("close_quic_endpoint");
    error!("{:?}", std::backtrace::Backtrace::force_capture());
    endpoint.close(
        CONNECTION_CLOSE_ERROR_CODE_SHUTDOWN,
        CONNECTION_CLOSE_REASON_SHUTDOWN,
    );
}

fn new_server_config(cert: Certificate, key: PrivateKey) -> Result<ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(Arc::new(SkipClientVerification {}))
        .with_single_cert(vec![cert], key)?;
    config.alpn_protocols = vec![ALPN_TURBINE_PROTOCOL_ID.to_vec()];
    let mut config = ServerConfig::with_crypto(Arc::new(config));
    config
        .transport_config(Arc::new(new_transport_config()))
        .use_retry(true)
        .migration(false);
    Ok(config)
}

fn new_client_config(cert: Certificate, key: PrivateKey) -> Result<ClientConfig, rustls::Error> {
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification {}))
        .with_client_auth_cert(vec![cert], key)?;
    config.enable_early_data = true;
    config.alpn_protocols = vec![ALPN_TURBINE_PROTOCOL_ID.to_vec()];
    let mut config = ClientConfig::new(Arc::new(config));
    config.transport_config(Arc::new(new_transport_config()));
    Ok(config)
}

fn new_transport_config() -> TransportConfig {
    let mut config = TransportConfig::default();
    let mut mtu_discovery_config = quinn::MtuDiscoveryConfig::default();
    mtu_discovery_config.upper_bound(INITIAL_MAXIMUM_TRANSMISSION_UNIT);
    config
        .max_concurrent_bidi_streams(VarInt::from(0u8))
        .max_concurrent_uni_streams(VarInt::from(10_000u32))
        .initial_mtu(INITIAL_MAXIMUM_TRANSMISSION_UNIT)
        // .datagram_receive_buffer_size(Some(usize::MAX / 3))
        // .datagram_send_buffer_size(usize::MAX / 3)
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0)
        .mtu_discovery_config(Some(quinn::MtuDiscoveryConfig::default()));
    config
}

async fn run_server(
    endpoint: Endpoint,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    while let Some(connecting) = endpoint.accept().await {
        tokio::task::spawn(handle_connecting_error(
            endpoint.clone(),
            connecting,
            sender.clone(),
            cache.clone(),
        ));
    }
}

async fn run_client(
    endpoint: Endpoint,
    mut receiver: AsyncReceiver<(SocketAddr, Bytes)>,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    // 4 just misses with --tx_count 200!
    let semaphore = Arc::new(tokio::sync::Semaphore::new(512));
    let mut count: usize = 0;
    while let Some((remote_address, bytes)) = receiver.recv().await {
        count += 1;
        if count % 10_000 == 0 {
            error!("run_client: {}", count / 10_000);
        }
        // use try_acquire_owned instead?!
        tokio::task::spawn(send_stream_task(
            endpoint.clone(),
            remote_address,
            bytes,
            semaphore.clone().acquire_owned().await.unwrap(),
            sender.clone(),
            cache.clone(),
        ));
    }
    close_quic_endpoint(&endpoint);
}

async fn handle_connecting_error(
    endpoint: Endpoint,
    connecting: Connecting,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    if let Err(err) = handle_connecting(endpoint, connecting, sender, cache).await {
        error!("handle_connecting: {err:?}");
    }
}

async fn handle_connecting(
    endpoint: Endpoint,
    connecting: Connecting,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) -> Result<(), Error> {
    let connection = connecting.await?;
    let remote_address = connection.remote_address();
    let remote_pubkey = get_remote_pubkey(&connection)?;
    handle_connection_error(
        endpoint,
        remote_address,
        remote_pubkey,
        connection,
        sender,
        cache,
    )
    .await;
    Ok(())
}

async fn handle_connection_error(
    endpoint: Endpoint,
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    connection: Connection,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    error!("max datagram size: {:?}", connection.max_datagram_size());
    cache_connection(remote_address, remote_pubkey, connection.clone(), &cache).await;
    if let Err(err) = handle_connection(
        &endpoint,
        remote_address,
        remote_pubkey,
        &connection,
        &sender,
    )
    .await
    {
        drop_connection(remote_address, remote_pubkey, &connection, &cache).await;
        error!("handle_connection: {remote_pubkey}, {remote_address}, {err:?}");
    }
}

#[allow(unreachable_code)]
async fn handle_connection(
    endpoint: &Endpoint,
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    connection: &Connection,
    sender: &Sender<(Pubkey, SocketAddr, Bytes)>,
) -> Result<(), Error> {
    // Assert that send won't block.
    debug_assert_eq!(sender.capacity(), None);
    loop {
        match connection.accept_uni().await {
            Ok(stream) => {
                // TODO: Probably need semaphore here too!
                tokio::task::spawn(handle_recv_stream_error(
                    endpoint.clone(),
                    stream,
                    remote_address,
                    remote_pubkey,
                    sender.clone(),
                ));
            }
            Err(err) => {
                if let Some(err) = connection.close_reason() {
                    return Err(Error::from(err));
                }
                error!("connection.read_datagram: {remote_pubkey}, {remote_address}, {err:?}");
            }
        }

        continue;
        match connection.read_datagram().await {
            Ok(bytes) => {
                let count = HANDLE_CONNECTION_COUNT.fetch_add(1, Ordering::Relaxed);
                if count % 10_000 == 0 {
                    error!("handle_connection: {}", count / 10_000);
                }
                if let Err(err) = sender.send((remote_pubkey, remote_address, bytes)) {
                    close_quic_endpoint(endpoint);
                    return Err(Error::from(err));
                }
            }
            Err(err) => {
                if let Some(err) = connection.close_reason() {
                    return Err(Error::from(err));
                }
                error!("connection.read_datagram: {remote_pubkey}, {remote_address}, {err:?}");
            }
        };
    }
}

async fn handle_recv_stream_error(
    endpoint: Endpoint,
    stream: RecvStream,
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
) {
    if let Err(err) =
        handle_recv_stream(&endpoint, stream, remote_address, remote_pubkey, &sender).await
    {
        error!("handle_recv_stream: {remote_pubkey}, {remote_address}, {err:?}");
    }
}

async fn handle_recv_stream(
    endpoint: &Endpoint,
    mut stream: RecvStream,
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    sender: &Sender<(Pubkey, SocketAddr, Bytes)>,
) -> Result<(), Error> {
    let mut size = 0;
    let mut buffer = [0u8; PACKET_DATA_SIZE];
    loop {
        let Some(chunk) = stream
            .read_chunk(PACKET_DATA_SIZE, /*ordered:*/ false)
            .await?
        else {
            let Err(err) = sender.send((remote_pubkey, remote_address, Bytes::copy_from_slice(&buffer[..size]))) else {
                return Ok(());
            };
            close_quic_endpoint(endpoint);
            return Err(Error::from(err));
        };
        let Ok(offset) = usize::try_from(chunk.offset) else {
            return Err(Error::InvalidChunk(chunk));
        };
        let Some(end) = offset.checked_add(chunk.bytes.len()) else {
            return Err(Error::InvalidChunk(chunk));
        };
        let Some(buffer) = buffer.get_mut(offset..end) else {
            return Err(Error::InvalidChunk(chunk));
        };
        buffer.copy_from_slice(&chunk.bytes);
        size = size.max(end);
    }
}

#[allow(dead_code)]
async fn send_datagram_task(
    endpoint: Endpoint,
    remote_address: SocketAddr,
    bytes: Bytes,
    permit: tokio::sync::OwnedSemaphorePermit,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    if let Err(err) = send_datagram(&endpoint, remote_address, bytes, sender, cache).await {
        error!("send_datagram: {remote_address}, {err:?}");
    }
    drop(permit)
}

async fn send_datagram(
    endpoint: &Endpoint,
    remote_address: SocketAddr,
    bytes: Bytes,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) -> Result<(), Error> {
    let connection = get_connection(endpoint, remote_address, sender, cache).await?;
    connection.send_datagram(bytes)?;
    Ok(())
}

async fn send_stream_task(
    endpoint: Endpoint,
    remote_address: SocketAddr,
    bytes: Bytes,
    permit: tokio::sync::OwnedSemaphorePermit,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) {
    if let Err(err) = send_stream(&endpoint, remote_address, bytes, sender, cache).await {
        error!("send_stream: {remote_address}, {err:?}");
    }
    drop(permit)
}

async fn send_stream(
    endpoint: &Endpoint,
    remote_address: SocketAddr,
    bytes: Bytes,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) -> Result<(), Error> {
    let connection = get_connection(endpoint, remote_address, sender, cache).await?;
    let mut stream = connection.open_uni().await?;
    stream.write_all(&bytes).await?;
    stream.finish().await?;
    Ok(())
}

async fn get_connection(
    endpoint: &Endpoint,
    remote_address: SocketAddr,
    sender: Sender<(Pubkey, SocketAddr, Bytes)>,
    cache: Arc<RwLock<ConnectionCache>>,
) -> Result<Connection, Error> {
    let key = (remote_address, /*remote_pubkey:*/ None);
    let entry = cache.write().await.entry(key).or_default().clone();
    {
        let connection: Option<Connection> = entry.read().await.clone();
        if let Some(connection) = connection {
            if connection.close_reason().is_none() {
                return Ok(connection);
            }
        }
    }
    let connection = {
        // Need to write lock here so that only one task initiates
        // a new connection to the same remote_address.
        let mut entry = entry.write().await;
        if let Some(connection) = entry.deref() {
            if connection.close_reason().is_none() {
                return Ok(connection.clone());
            }
        }
        let connection = endpoint
            .connect(remote_address, CONNECT_SERVER_NAME)?
            .await?;
        entry.insert(connection).clone()
    };
    tokio::task::spawn(handle_connection_error(
        endpoint.clone(),
        connection.remote_address(),
        get_remote_pubkey(&connection)?,
        connection.clone(),
        sender,
        cache,
    ));
    Ok(connection)
}

fn get_remote_pubkey(connection: &Connection) -> Result<Pubkey, Error> {
    match solana_streamer::nonblocking::quic::get_remote_pubkey(connection) {
        Some(remote_pubkey) => Ok(remote_pubkey),
        None => {
            connection.close(
                CONNECTION_CLOSE_ERROR_CODE_INVALID_IDENTITY,
                CONNECTION_CLOSE_REASON_INVALID_IDENTITY,
            );
            Err(Error::InvalidIdentity(connection.remote_address()))
        }
    }
}

async fn cache_connection(
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    connection: Connection,
    cache: &RwLock<ConnectionCache>,
) {
    let entries: [Arc<RwLock<Option<Connection>>>; 2] = {
        let mut cache = cache.write().await;
        error!("cache size: {}", cache.len());
        [Some(remote_pubkey), None].map(|remote_pubkey| {
            let key = (remote_address, remote_pubkey);
            cache.entry(key).or_default().clone()
        })
    };
    let mut entry = entries[0].write().await;
    *entries[1].write().await = Some(connection.clone());
    if let Some(old) = entry.replace(connection) {
        drop(entry);
        old.close(
            CONNECTION_CLOSE_ERROR_CODE_REPLACED,
            CONNECTION_CLOSE_REASON_REPLACED,
        );
    }
}

async fn drop_connection(
    remote_address: SocketAddr,
    remote_pubkey: Pubkey,
    connection: &Connection,
    cache: &RwLock<ConnectionCache>,
) {
    if connection.close_reason().is_none() {
        connection.close(
            CONNECTION_CLOSE_ERROR_CODE_DROPPED,
            CONNECTION_CLOSE_REASON_DROPPED,
        );
    }
    let key = (remote_address, Some(remote_pubkey));
    if let Entry::Occupied(entry) = cache.write().await.entry(key) {
        if matches!(entry.get().read().await.deref(),
                    Some(entry) if entry.stable_id() == connection.stable_id())
        {
            entry.remove();
        }
    }
    // Cache entry for (remote_address, None) will be lazily evicted.
}

impl<T> From<crossbeam_channel::SendError<T>> for Error {
    fn from(_: crossbeam_channel::SendError<T>) -> Self {
        Error::ChannelSendError
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        itertools::{izip, multiunzip},
        solana_sdk::signature::Signer,
        std::{iter::repeat_with, net::Ipv4Addr, time::Duration},
    };

    #[test]
    fn test_quic_endpoint() {
        // solana_logger::setup();
        const NUM_ENDPOINTS: usize = 3;
        const RECV_TIMEOUT: Duration = Duration::from_secs(60);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        let keypairs: Vec<Keypair> = repeat_with(Keypair::new).take(NUM_ENDPOINTS).collect();
        let sockets: Vec<UdpSocket> = repeat_with(|| UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)))
            .take(NUM_ENDPOINTS)
            .collect::<Result<_, _>>()
            .unwrap();
        let addresses: Vec<SocketAddr> = sockets
            .iter()
            .map(UdpSocket::local_addr)
            .collect::<Result<_, _>>()
            .unwrap();
        let (senders, receivers): (Vec<_>, Vec<_>) =
            repeat_with(crossbeam_channel::unbounded::<(Pubkey, SocketAddr, Bytes)>)
                .take(NUM_ENDPOINTS)
                .unzip();
        let (endpoints, senders, tasks): (Vec<_>, Vec<_>, Vec<_>) =
            multiunzip(keypairs.iter().zip(sockets).zip(senders).map(
                |((keypair, socket), sender)| {
                    new_quic_endpoint(
                        runtime.handle(),
                        keypair,
                        socket,
                        IpAddr::V4(Ipv4Addr::LOCALHOST),
                        sender,
                    )
                    .unwrap()
                },
            ));
        // Send a unique message from each endpoint to every other endpoint.
        for (i, (keypair, &address, sender)) in izip!(&keypairs, &addresses, &senders).enumerate() {
            for (j, &address) in addresses.iter().enumerate() {
                if i != j {
                    let bytes = Bytes::from(format!("{i}=>{j}"));
                    sender.blocking_send((address, bytes)).unwrap();
                }
            }
            // Verify all messages are received.
            for (j, receiver) in receivers.iter().enumerate() {
                if i != j {
                    let bytes = Bytes::from(format!("{i}=>{j}"));
                    let entry = (keypair.pubkey(), address, bytes);
                    assert_eq!(receiver.recv_timeout(RECV_TIMEOUT).unwrap(), entry);
                }
            }
        }
        drop(senders);
        for endpoint in endpoints {
            close_quic_endpoint(&endpoint);
        }
        for task in tasks {
            runtime.block_on(task).unwrap();
        }
    }
}
