use crate::REPLICATION;
use clap::ValueEnum;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub is_self: bool,
}

pub static PEERS: OnceLock<Vec<Peer>> = OnceLock::new();

#[derive(ValueEnum, Clone)]
pub enum PeerDiscoveryStrategy {
    Dns,
    Csv,
}

fn is_local_ip(ip: IpAddr) -> bool {
    let interfaces = local_ip_address::list_afinet_netifas().unwrap();
    for (_, iface_ip) in interfaces {
        if iface_ip == ip {
            return true;
        }
    }

    false
}

fn is_self(self_addr: SocketAddr, addr: SocketAddr) -> bool {
    addr.port() == self_addr.port() && is_local_ip(addr.ip())
}

impl PeerDiscoveryStrategy {
    pub fn initialize<A: ToSocketAddrs>(&self, self_addr: A, value: &str) -> anyhow::Result<()> {
        let self_addr = self_addr.to_socket_addrs()?.next().unwrap();

        PEERS
            .set(match self {
                PeerDiscoveryStrategy::Dns => {
                    let mut all: Vec<SocketAddr> =
                        value.to_socket_addrs().expect("No peers in DNS").collect();
                    all.sort();
                    all.into_iter()
                        .map(|addr| Peer {
                            addr,
                            is_self: is_self(self_addr, addr),
                        })
                        .collect()
                }
                PeerDiscoveryStrategy::Csv => value
                    .split(",")
                    .map(|addr| {
                        let addr = addr
                            .to_socket_addrs()
                            .expect("Not an address")
                            .next()
                            .unwrap();
                        Peer {
                            addr,
                            is_self: is_self(self_addr, addr),
                        }
                    })
                    .collect(),
            })
            .expect("Peers already initialized");
        Ok(())
    }
}

pub struct PeersForKey {
    idx: usize,
    len: usize,
}

impl PeersForKey {
    pub fn from_key(key: &[u8]) -> Self {
        let peers = PEERS.get().expect("Peers uninitialized");

        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let idx = ((hash as u128 * peers.len() as u128) >> 64) as usize;
        let len = REPLICATION as usize;
        Self { idx, len }
    }
}

impl Iterator for PeersForKey {
    type Item = &'static Peer;

    fn next(&mut self) -> Option<Self::Item> {
        let peers = PEERS.get().expect("Peers uninitialized");

        if self.len == 0 {
            None
        } else {
            let idx = self.idx;
            self.idx = (self.idx + 1) % peers.len();
            self.len -= 1;
            Some(&peers[idx])
        }
    }
}
