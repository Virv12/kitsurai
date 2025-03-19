use crate::rpc::{Rpc, RpcExec, RpcRequest, RpcRequestRecv};
use anyhow::bail;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::LazyLock,
    time::Duration,
};
use tokio::{net::TcpStream, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

mod rpc;
mod store;

static REPLICATION: u64 = 3;
static NECESSARY_READ: u64 = 2;
static NECESSARY_WRITE: u64 = 2;
static TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct Peer {
    addr: SocketAddr,
    is_self: bool,
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

    let mut circular = Vec::new();
    circular.extend_from_slice(&all);
    circular.extend_from_slice(&all[..REPLICATION as usize - 1]);
    circular
});

// TODO: non so se serve veramente il più 1
static BUCKET_SIZE: LazyLock<u64> =
    LazyLock::new(|| u64::MAX / (PEERS.len() as u64 - (REPLICATION - 1)) + 1);

fn peers_for_key(key: &str) -> &'static [Peer] {
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_ref());
    let index = hash / *BUCKET_SIZE;
    eprintln!("DHT: {hash} -> {index}");
    let index = index as usize;
    &PEERS[index..index + REPLICATION as usize]
}

pub async fn item_get(key: &str) -> anyhow::Result<Vec<Option<Bytes>>> {
    let key_bytes = Bytes::from(key.to_string());
    let mut set = JoinSet::new();
    for peer in peers_for_key(key) {
        set.spawn(timeout(
            TIMEOUT,
            ItemGet {
                key: key_bytes.clone(),
            }
            .exec(peer),
        ));
    }

    let mut successes = 0;
    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        match res.expect("Join error.") {
            Ok(Ok(Ok(res))) => {
                successes += 1;
                results.push(res);
            }
            Ok(Ok(Err(store_error))) => eprintln!("Store error: {store_error}"),
            Ok(Err(rpc_error)) => eprintln!("Rpc error: {rpc_error}"),
            Err(timeout_error) => eprintln!("Timeout error: {timeout_error}"),
        }

        if successes >= NECESSARY_READ {
            set.detach_all();
            return Ok(results);
        }
    }

    bail!("Failed to read from {NECESSARY_READ} nodes.")
}

pub async fn item_set(key: &str, value: Bytes) -> anyhow::Result<()> {
    let key_bytes = Bytes::from(key.to_string());
    let mut set = JoinSet::new();
    for peer in peers_for_key(key) {
        set.spawn(timeout(
            TIMEOUT,
            ItemSet {
                key: key_bytes.clone(),
                value: value.clone(),
            }
            .exec(peer),
        ));
    }

    let mut successes = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("Join error.") {
            Ok(Ok(Ok(()))) => successes += 1,
            Ok(Ok(Err(store_error))) => eprintln!("Store error: {store_error}"),
            Ok(Err(rpc_error)) => eprintln!("Rpc error: {rpc_error}"),
            Err(timeout_error) => eprintln!("Timeout error: {timeout_error}"),
        }

        if successes >= NECESSARY_WRITE {
            set.detach_all();
            return Ok(());
        }
    }

    bail!("Failed to write to {NECESSARY_WRITE} nodes.")
}

pub async fn visualizer_data() -> anyhow::Result<Vec<(SocketAddr, Vec<(Bytes, Bytes)>)>> {
    let mut data = Vec::new();
    let mut set = JoinSet::new();
    for peer in &PEERS[..PEERS.len() - (REPLICATION as usize - 1)] {
        set.spawn(async move { (peer.addr, ItemList {}.exec(peer).await) });
    }
    while let Some(res) = set.join_next().await {
        let (addr, res) = res.expect("Join error.");
        data.push((addr, res??));
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
