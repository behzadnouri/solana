//! The `recvmmsg` module provides recvmmsg() API implementation

use crate::packet::Packet;
pub use solana_perf::packet::NUM_RCVMMSGS;
use std::cmp;
use std::io;

#[cfg(not(target_os = "linux"))]
pub fn recv_mmsg(socket: &mut UdpSocket, packets: &mut [Packet]) -> io::Result<(usize, usize)> {
    let mut i = 0;
    let count = cmp::min(NUM_RCVMMSGS, packets.len());
    let mut total_size = 0;
    for p in packets.iter_mut().take(count) {
        p.meta.size = 0;
        match socket.recv_from(&mut p.data) {
            Err(_) if i > 0 => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
            Ok((nrecv, from)) => {
                total_size += nrecv;
                p.meta.size = nrecv;
                p.meta.set_addr(&from);
                if i == 0 {
                    socket.set_nonblocking(true)?;
                }
            }
        }
        i += 1;
    }
    Ok((total_size, i))
}

#[cfg(target_os = "linux")]
pub fn recv_mmsg(socket: &mut UdpSocket, packets: &mut [Packet]) -> io::Result<(usize, usize)> {
    socket.recv_mmsg(packets)
}

#[cfg(not(target_os = "linux"))]
pub struct UdpSocket<'a> {
    socket: &'a std::net::UdpSocket,
}

#[cfg(target_os = "linux")]
pub struct UdpSocket<'a> {
    hdrs: [libc::mmsghdr; NUM_RCVMMSGS],
    iovs: [libc::iovec; NUM_RCVMMSGS],
    addr: [libc::sockaddr_in; NUM_RCVMMSGS],
    socket: &'a std::net::UdpSocket,
}

impl std::ops::Deref for UdpSocket<'_> {
    type Target = std::net::UdpSocket;
    fn deref(&self) -> &Self::Target {
        self.socket
    }
}

impl<'a> From<&'a std::net::UdpSocket> for UdpSocket<'a> {
    #[cfg(not(target_os = "linux"))]
    fn from(socket: &'a std::net::UdpSocket) -> Self {
        Self { socket }
    }

    #[cfg(target_os = "linux")]
    fn from(socket: &'a std::net::UdpSocket) -> Self {
        use itertools::izip;
        let mut out = Self {
            hdrs: unsafe { std::mem::zeroed() },
            iovs: unsafe { std::mem::zeroed() },
            addr: unsafe { std::mem::zeroed() },
            socket,
        };
        let addrlen = std::mem::size_of_val(&out.addr) as libc::socklen_t;
        for (hdr, iov, addr) in izip!(
            out.hdrs.iter_mut(),
            out.iovs.iter_mut(),
            out.addr.iter_mut()
        ) {
            hdr.msg_hdr.msg_name = addr as *mut _ as *mut _;
            hdr.msg_hdr.msg_namelen = addrlen;
            hdr.msg_hdr.msg_iov = iov;
            hdr.msg_hdr.msg_iovlen = 1;
        }
        out
    }
}

#[cfg(target_os = "linux")]
impl UdpSocket<'_> {
    fn recv_mmsg(&mut self, packets: &mut [Packet]) -> io::Result<(usize, usize)> {
        use itertools::izip;
        use libc::{c_uint, c_void, recvmmsg, time_t, timespec, MSG_WAITFORONE};
        use nix::sys::socket::InetAddr;
        use std::os::unix::io::AsRawFd;
        for (iov, packet) in self.iovs.iter_mut().zip(packets.iter_mut()) {
            iov.iov_base = packet.data.as_mut_ptr() as *mut c_void;
            iov.iov_len = packet.data.len();
        }
        let vlen = cmp::min(NUM_RCVMMSGS, packets.len()) as c_uint;
        let mut timeout = timespec {
            tv_sec: 1 as time_t,
            tv_nsec: 0,
        };
        let npkts = match unsafe {
            recvmmsg(
                self.socket.as_raw_fd(),
                &mut self.hdrs[0],
                vlen,
                MSG_WAITFORONE,
                &mut timeout,
            )
        } {
            -1 => return Err(io::Error::last_os_error()),
            npkts => npkts as usize,
        };
        let size = izip!(packets, self.hdrs.iter(), self.addr.iter())
            .take(npkts)
            .map(|(packet, hdr, addr)| {
                let addr = InetAddr::V4(*addr).to_std();
                packet.meta.set_addr(&addr);
                packet.meta.size = hdr.msg_len as usize;
                packet.meta.size
            })
            .sum();
        Ok((size, npkts))
    }
}

#[cfg(test)]
mod tests {
    use crate::packet::PACKET_DATA_SIZE;
    use crate::recvmmsg::*;
    use std::net::UdpSocket;
    use std::time::{Duration, Instant};

    const TEST_NUM_MSGS: usize = 32;
    #[test]
    pub fn test_recv_mmsg_one_iter() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr = sender.local_addr().unwrap();
        let sent = TEST_NUM_MSGS - 1;
        for _ in 0..sent {
            let data = [0; PACKET_DATA_SIZE];
            sender.send_to(&data[..], &addr).unwrap();
        }

        let mut packets = vec![Packet::default(); TEST_NUM_MSGS];
        let recv = recv_mmsg(&mut (&reader).into(), &mut packets[..])
            .unwrap()
            .1;
        assert_eq!(sent, recv);
        for packet in packets.iter().take(recv) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr);
        }
    }

    #[test]
    pub fn test_recv_mmsg_multi_iter() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr = sender.local_addr().unwrap();
        let sent = TEST_NUM_MSGS + 10;
        for _ in 0..sent {
            let data = [0; PACKET_DATA_SIZE];
            sender.send_to(&data[..], &addr).unwrap();
        }

        let mut packets = vec![Packet::default(); TEST_NUM_MSGS];
        let mut reader = (&reader).into();
        let recv = recv_mmsg(&mut reader, &mut packets[..]).unwrap().1;
        assert_eq!(TEST_NUM_MSGS, recv);
        for packet in packets.iter().take(recv) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr);
        }

        let recv = recv_mmsg(&mut reader, &mut packets[..]).unwrap().1;
        assert_eq!(sent - TEST_NUM_MSGS, recv);
        for packet in packets.iter().take(recv) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr);
        }
    }

    #[test]
    pub fn test_recv_mmsg_multi_iter_timeout() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        reader.set_read_timeout(Some(Duration::new(5, 0))).unwrap();
        reader.set_nonblocking(false).unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr = sender.local_addr().unwrap();
        let sent = TEST_NUM_MSGS;
        for _ in 0..sent {
            let data = [0; PACKET_DATA_SIZE];
            sender.send_to(&data[..], &addr).unwrap();
        }

        let start = Instant::now();
        let mut packets = vec![Packet::default(); TEST_NUM_MSGS];
        let mut reader = (&reader).into();
        let recv = recv_mmsg(&mut reader, &mut packets[..]).unwrap().1;
        assert_eq!(TEST_NUM_MSGS, recv);
        for packet in packets.iter().take(recv) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr);
        }
        reader.set_nonblocking(true).unwrap();

        let _recv = recv_mmsg(&mut reader, &mut packets[..]);
        assert!(start.elapsed().as_secs() < 5);
    }

    #[test]
    pub fn test_recv_mmsg_multi_addrs() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();

        let sender1 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr1 = sender1.local_addr().unwrap();
        let sent1 = TEST_NUM_MSGS - 1;

        let sender2 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr2 = sender2.local_addr().unwrap();
        let sent2 = TEST_NUM_MSGS + 1;

        for _ in 0..sent1 {
            let data = [0; PACKET_DATA_SIZE];
            sender1.send_to(&data[..], &addr).unwrap();
        }

        for _ in 0..sent2 {
            let data = [0; PACKET_DATA_SIZE];
            sender2.send_to(&data[..], &addr).unwrap();
        }

        let mut packets = vec![Packet::default(); TEST_NUM_MSGS];

        let mut reader = (&reader).into();
        let recv = recv_mmsg(&mut reader, &mut packets[..]).unwrap().1;
        assert_eq!(TEST_NUM_MSGS, recv);
        for packet in packets.iter().take(sent1) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr1);
        }
        for packet in packets.iter().skip(sent1).take(recv - sent1) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr2);
        }

        let recv = recv_mmsg(&mut reader, &mut packets[..]).unwrap().1;
        assert_eq!(sent1 + sent2 - TEST_NUM_MSGS, recv);
        for packet in packets.iter().take(recv) {
            assert_eq!(packet.meta.size, PACKET_DATA_SIZE);
            assert_eq!(packet.meta.addr(), saddr2);
        }
    }
}
