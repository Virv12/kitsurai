//! Handles Peer discovery (see [Discovery]) and self identification.
//! Also handles this peer availability zone.

use clap::Parser;
use std::{
    convert::Infallible,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::OnceLock,
};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

static PEERS: OnceLock<Vec<Peer>> = OnceLock::new();
static LOCAL_INDEX: OnceLock<usize> = OnceLock::new();
static AVAILABILITY_ZONE: OnceLock<String> = OnceLock::new();

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Peer {
    pub addr: String,
    _marker: (),
}

impl Peer {
    pub fn is_local(&self) -> bool {
        std::ptr::eq(self, local())
    }
}

pub fn peers() -> &'static [Peer] {
    PEERS.get().expect("peers list is uninitialized")
}

pub fn local_index() -> usize {
    *LOCAL_INDEX.get().expect("local index is uninitialized")
}

pub fn local() -> &'static Peer {
    &peers()[local_index()]
}

pub fn availability_zone() -> &'static str {
    AVAILABILITY_ZONE
        .get()
        .expect("availability zone is uninitialized")
}

#[derive(Debug, Clone)]
enum Discovery {
    Dns(String),
    List(String),
}

impl Discovery {
    fn value_parser(value: &str) -> Result<Discovery, Infallible> {
        if let Some(stripped) = value.strip_prefix("dns:") {
            Ok(Discovery::Dns(stripped.to_string()))
        } else {
            Ok(Discovery::List(value.to_string()))
        }
    }
}

#[derive(Debug, Parser)]
pub struct PeerCli {
    #[arg(long = "peers", value_parser = Discovery::value_parser)]
    discovery: Discovery,

    #[arg(long, default_value = None)]
    availability_zone: Option<String>,
}

pub fn init(cli: PeerCli, local_addr: SocketAddr) {
    let mut peers: Vec<_> = match &cli.discovery {
        Discovery::Dns(v) => v
            .to_socket_addrs()
            .expect("could not resolve address")
            .map(|a| Peer {
                addr: a.to_string(),
                _marker: (),
            })
            .collect(),
        Discovery::List(l) => l
            .split(',')
            .map(|a| Peer {
                addr: a.to_owned(),
                _marker: (),
            })
            .collect(),
    };

    peers.sort();

    let local_index = peers
        .iter()
        .position(|p| is_self(local_addr, &p.addr))
        .expect("self is not in peers");

    PEERS
        .set(peers)
        .expect("peers should not be initialized!!!");

    LOCAL_INDEX
        .set(local_index)
        .expect("local index already initialized");

    let zone = cli.availability_zone.unwrap_or_else(|| {
        Uuid::new_v8(xxh3_128(local_addr.to_string().as_bytes()).to_ne_bytes()).to_string()
    });

    AVAILABILITY_ZONE
        .set(zone)
        .expect("availability zone already initialized");
}

fn is_self(local_addr: SocketAddr, addr: &str) -> bool {
    fn is_local_ip(ip: IpAddr) -> bool {
        let interfaces = local_ip_address::list_afinet_netifas().unwrap();
        for (_, iface_ip) in interfaces {
            if iface_ip == ip {
                return true;
            }
        }

        false
    }

    for addr in addr.to_socket_addrs().expect("could not resolve address") {
        let ip_ok = if local_addr.ip().is_unspecified() {
            is_local_ip(addr.ip())
        } else {
            local_addr.ip() == addr.ip()
        };

        let port_ok = local_addr.port() == addr.port();

        if ip_ok && port_ok {
            return true;
        }
    }

    false
}
