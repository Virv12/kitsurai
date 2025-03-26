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
pub static SELF_INDEX: OnceLock<usize> = OnceLock::new();

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

    let peers: Vec<_> = peers
        .into_iter()
        .map(|addr| Peer {
            addr,
            is_self: is_self(self_addr, addr),
        })
        .collect();

    let self_index = peers
        .iter()
        .position(|p| p.is_self)
        .expect("self not in peers");

    PEERS.set(peers).expect("Peers already initialized");
    SELF_INDEX
        .set(self_index)
        .expect("self already initialized");
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
