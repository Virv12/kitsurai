use crate::REPLICATION;
use clap::Parser;
use std::{
    convert::Infallible,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::OnceLock,
};

#[derive(Clone, Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub is_self: bool,
}

pub static PEERS: OnceLock<Vec<Peer>> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) enum Peers {
    Dns(String),
    List(String),
}

#[derive(Debug, Parser)]
pub(crate) struct PeerCli {
    #[arg(long, value_parser = parse_peers)]
    pub(crate) peers: Peers,
}

fn parse_peers(value: &str) -> Result<Peers, Infallible> {
    if let Some(stripped) = value.strip_prefix("dns:") {
        Ok(Peers::Dns(stripped.to_string()))
    } else {
        Ok(Peers::List(value.to_string()))
    }
}

pub(crate) fn init(cli: PeerCli, self_addr: &str) -> anyhow::Result<()> {
    let self_addr = self_addr.to_socket_addrs()?.next().unwrap();
    let mut peers: Vec<SocketAddr> = match cli.peers {
        Peers::Dns(v) => v.to_socket_addrs()?.collect(),
        Peers::List(l) => l
            .split(',')
            .map(|s| s.to_socket_addrs().unwrap().next().unwrap())
            .collect(),
    };
    peers.sort();
    let peers = peers
        .into_iter()
        .map(|addr| Peer {
            addr,
            is_self: is_self(self_addr, addr),
        })
        .collect();
    PEERS.set(peers).expect("Peers already initialized");
    Ok(())
}

// TODO: this could break if there are two peers with addresses 127.0.0.1:3000 and 127.0.0.2:3000
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
