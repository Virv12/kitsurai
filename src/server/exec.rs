use crate::rpc::{Rpc, RpcRequest};
use crate::store;
use anyhow::bail;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::LazyLock,
    time::Duration,
};
use tokio::{net::TcpStream, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

static REPLICATION: u64 = 3;
static NECESSARY_READ: u64 = 2;
static NECESSARY_WRITE: u64 = 2;
static TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub(crate) struct Peer {
    pub(crate) addr: SocketAddr,
    pub(crate) is_self: bool,
}

static PEERS: LazyLock<Vec<Peer>> = LazyLock::new(|| {
    fn is_local_ip(ip: IpAddr) -> bool {
        let interfaces = local_ip_address::list_afinet_netifas().unwrap();
        for (_, iface_ip) in interfaces {
            if iface_ip == ip {
                return true;
            }
        }

        false
    }

    let mut all: Vec<SocketAddr> = "kitsurai:3000"
        .to_socket_addrs()
        .expect("Empty peers.")
        .collect();
    all.sort();
    let all: Vec<Peer> = all
        .into_iter()
        .map(|addr| Peer {
            addr,
            is_self: is_local_ip(addr.ip()),
        })
        .collect();
    all
});

struct PeersForKey {
    idx: usize,
    len: usize,
}

impl PeersForKey {
    fn from_key(key: &[u8]) -> Self {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let idx = ((hash as u128 * PEERS.len() as u128) >> 64) as usize;
        let len = REPLICATION as usize;
        Self { idx, len }
    }
}

impl Iterator for PeersForKey {
    type Item = &'static Peer;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            let idx = self.idx;
            self.idx = (self.idx + 1) % PEERS.len();
            self.len -= 1;
            Some(&PEERS[idx])
        }
    }
}

async fn keep_peer<O>(peer: &Peer, task: impl Future<Output = O>) -> (&Peer, O) {
    (peer, task.await)
}

pub async fn item_get(key: Bytes) -> anyhow::Result<Vec<Option<Bytes>>> {
    let mut set = Box::new(JoinSet::new());
    for peer in PeersForKey::from_key(&key) {
        let rpc = ItemGet { key: key.clone() };
        set.spawn(keep_peer(peer, timeout(TIMEOUT, rpc.exec(peer))));
    }

    let mut successes = 0;
    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        match res.expect("GET: join error") {
            (_, Ok(Ok(Ok(res)))) => {
                successes += 1;
                results.push(res);
            }
            (Peer { addr, is_self }, Ok(Ok(Err(store_error)))) => {
                eprintln!("GET: store error on {addr} ({is_self}), {store_error}")
            }
            (Peer { addr, is_self }, Ok(Err(rpc_error))) => {
                eprintln!("GET: rpc error on {addr} ({is_self}), {rpc_error}")
            }
            (Peer { addr, is_self }, Err(timeout_error)) => {
                eprintln!("GET: timeout error on {addr} ({is_self}), {timeout_error}")
            }
        }

        if successes >= NECESSARY_READ {
            set.abort_all();
            break;
        }
    }

    Ok(results)
}

pub async fn item_set(key: Bytes, value: Bytes) -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    for peer in PeersForKey::from_key(&key) {
        let rpc = ItemSet {
            key: key.clone(),
            value: value.clone(),
        };
        set.spawn(keep_peer(peer, timeout(TIMEOUT, rpc.exec(peer))));
    }

    let mut successes = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("Join error.") {
            (_, Ok(Ok(Ok(())))) => successes += 1,
            (Peer { addr, is_self }, Ok(Ok(Err(store_error)))) => {
                eprintln!("GET: store error on {addr} ({is_self}), {store_error}")
            }
            (Peer { addr, is_self }, Ok(Err(rpc_error))) => {
                eprintln!("GET: rpc error on {addr} ({is_self}), {rpc_error}")
            }
            (Peer { addr, is_self }, Err(timeout_error)) => {
                eprintln!("GET: timeout error on {addr} ({is_self}), {timeout_error}")
            }
        }

        if successes >= NECESSARY_WRITE {
            set.detach_all();
            return Ok(());
        }
    }

    bail!("Failed to write to {NECESSARY_WRITE} nodes, only {successes} succeeded.")
}

pub async fn visualizer_data() -> anyhow::Result<Vec<(SocketAddr, Vec<(Bytes, Bytes)>)>> {
    let mut data = Vec::new();
    let mut set = JoinSet::new();
    for peer in &*PEERS {
        set.spawn(keep_peer(peer, timeout(TIMEOUT, ItemList {}.exec(peer))));
    }
    while let Some(res) = set.join_next().await {
        let (peer, res) = res.expect("Join error.");
        let res = match res {
            Ok(Ok(Ok(res))) => res,
            _ => Vec::new(),
        };
        data.push((peer.addr, res));
    }
    Ok(data)
}

#[derive(Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
enum Operations {
    ItemGet(ItemGet),
    ItemSet(ItemSet),
    ItemList(ItemList),
}

impl RpcRequest for Operations {
    async fn remote(self, stream: TcpStream) -> anyhow::Result<()> {
        let name = match self {
            Operations::ItemGet(_) => "get",
            Operations::ItemSet(_) => "set",
            Operations::ItemList(_) => "list",
        };
        let peer = stream.peer_addr()?.ip();
        eprintln!("RPC: handling {name} for {peer}");

        match self {
            Self::ItemGet(get) => get.remote(stream).await,
            Self::ItemSet(set) => set.remote(stream).await,
            Self::ItemList(list) => list.remote(stream).await,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ItemGet {
    key: Bytes,
}

impl Rpc for ItemGet {
    type Request = Operations;
    type Response = Result<Option<Bytes>, store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemGet(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_get(self.key))
    }
}

#[derive(Serialize, Deserialize)]
struct ItemSet {
    key: Bytes,
    value: Bytes,
}

impl Rpc for ItemSet {
    type Request = Operations;
    type Response = Result<(), store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemSet(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_set(self.key, self.value))
    }
}

#[derive(Serialize, Deserialize)]
struct ItemList {}

impl Rpc for ItemList {
    type Request = Operations;
    type Response = Result<Vec<(Bytes, Bytes)>, store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemList(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_list())
    }
}

pub async fn listener(token: CancellationToken) -> anyhow::Result<()> {
    Operations::listener("0.0.0.0:3000", token).await
}
