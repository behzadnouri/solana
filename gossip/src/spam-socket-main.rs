use {
    rand::Rng,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        time::Duration,
    },
};

const SOCKET_ADDRS: [SocketAddr; 11] = [
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(107, 182, 162, 250)), 8008), // LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(109, 194, 51, 78)), 8007), // dkisqBjujxMvG9HkmWK4aooockCCRcnYZEdR2fmN8TP
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(129, 213, 40, 161)), 8007), // 87T8k8EXRPxsTpF8TXGSWVSf4Bheaz4GEy73BPwm2CwH
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(185, 6, 13, 180)), 8007), // HTTod6mktmDvM5B1fHdyf8uUTaAL9HLTY9MaLRgCDrPd
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(185, 6, 13, 189)), 8007), // 9XV9hf2oBdJdrcPaLWLALpnCRu9t5Cozd4hJWMZnF4iu
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(217, 110, 201, 116)), 8007), // Bxps8LSWxZVx618iK8rHvnCD5f9u7cTYuXz5sLsAGuSn
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(35, 75, 180, 30)), 8007), // Fc6NNdS2j3EmrWbU6Uqt6wsKB5ef72NjaWfNxKYbULGD
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 199, 170, 85)), 8007), // CmGiehaqWfEZwyhmN9rckNtnVMeZtsjz1obusNWGyj4p
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(66, 45, 238, 30)), 8007), // 2NCzepxVangimxxDGLBiMYFfPS5ioRKr4aij6Rf668rg
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(69, 166, 3, 194)), 8008), // HUjU6MUEtpzj8PpLEqFPLGC5VynkWfHkXZpskEfM6GdY
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(69, 166, 3, 195)), 8008), // A85mL9EUKnq8r52WpyHMLuZw77bv4buZrbAu8cx8BHjg
];

fn main() {
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; 1024];
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    dbg!(&socket);
    dbg!(SOCKET_ADDRS);
    for _ in 0..5_000 {
        rng.fill(&mut buf[..]);
        for addr in SOCKET_ADDRS {
            socket.send_to(&buf[..], &addr).unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
