use crate::meta::Table;
use crate::{
    meta,
    peer::{Peer, PEERS},
    rpc::{Rpc, RpcRequest},
    store, NECESSARY_READ, NECESSARY_WRITE, TIMEOUT,
};
use anyhow::{bail, Context};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{future::Future, net::SocketAddr};
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn keep_peer<O>(peer: &Peer, task: impl Future<Output = O>) -> (&Peer, O) {
    (peer, task.await)
}

pub async fn item_get(table: Uuid, key: Bytes) -> anyhow::Result<Vec<Option<Bytes>>> {
    if (table.get_version().context("invalid table id")? != uuid::Version::SortRand) {
        bail!("table id has invalid version")
    }

    let table = meta::get_table(table)?.context("table not found")?;

    let mut set = Box::new(JoinSet::new());
    for peer in table.peers_for_key(&key) {
        let rpc = ItemGet {
            table: table.id,
            key: key.clone(),
        };
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

pub async fn item_set(table: Uuid, key: Bytes, value: Bytes) -> anyhow::Result<()> {
    assert_eq!(table.get_version().unwrap(), uuid::Version::SortRand);
    let table = meta::get_table(table)?.context("table not found")?;

    let mut set = JoinSet::new();
    for peer in table.peers_for_key(&key) {
        let rpc = ItemSet {
            table: table.id,
            key: key.clone(),
            value: value.clone(),
        };
        set.spawn(keep_peer(peer, timeout(TIMEOUT, rpc.exec(peer))));
    }

    let mut successes = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("Join error") {
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

pub async fn item_list(table: Uuid) -> anyhow::Result<Vec<(SocketAddr, Vec<(Vec<u8>, Vec<u8>)>)>> {
    let mut data = Vec::new();
    let mut set = JoinSet::new();
    for peer in PEERS.get().expect("Peers uninitialized") {
        set.spawn(keep_peer(
            peer,
            timeout(TIMEOUT, ItemList { table }.exec(peer)),
        ));
    }
    while let Some(res) = set.join_next().await {
        let (peer, res) = res.expect("Join error");
        let res = match res {
            Ok(Ok(Ok(res))) => res,
            _ => Vec::new(),
        };
        data.push((peer.addr, res));
    }
    Ok(data)
}

#[derive(Serialize, Deserialize)]
enum Operations {
    ItemGet(ItemGet),
    ItemSet(ItemSet),
    ItemList(ItemList),
    TablePrepare(TablePrepare),
    TableCommit(TableCommit),
}

impl Operations {
    fn name(&self) -> &'static str {
        match self {
            Operations::ItemGet(_) => "item-get",
            Operations::ItemSet(_) => "item-set",
            Operations::ItemList(_) => "item-list",
            Operations::TablePrepare(_) => "table-prepare",
            Operations::TableCommit(_) => "table-commit",
        }
    }
}

impl RpcRequest for Operations {
    async fn remote(self, stream: TcpStream) -> anyhow::Result<()> {
        let name = self.name();
        let peer = stream.peer_addr()?.ip();
        eprintln!("RPC: handling {name} for {peer}");

        match self {
            Self::ItemGet(get) => get.remote(stream).await,
            Self::ItemSet(set) => set.remote(stream).await,
            Self::ItemList(list) => list.remote(stream).await,
            Self::TablePrepare(prepare) => prepare.remote(stream).await,
            Self::TableCommit(commit) => commit.remote(stream).await,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ItemGet {
    table: Uuid,
    key: Bytes,
}

impl Rpc for ItemGet {
    type Request = Operations;
    type Response = Result<Option<Bytes>, store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemGet(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_get(self.table, &self.key).map(|opt| opt.map(Bytes::from)))
    }
}

#[derive(Serialize, Deserialize)]
struct ItemSet {
    table: Uuid,
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
        Ok(store::item_set(self.table, &self.key, &self.value))
    }
}

#[derive(Serialize, Deserialize)]
struct ItemList {
    table: Uuid,
}

impl Rpc for ItemList {
    type Request = Operations;
    type Response = Result<Vec<(Vec<u8>, Vec<u8>)>, store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemList(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_list(self.table))
    }
}

pub async fn listener<A: ToSocketAddrs>(addr: A, token: CancellationToken) -> anyhow::Result<()> {
    Operations::listener(addr, token).await
}

pub async fn table_create(mut b: u64, n: u64, r: u64, w: u64) -> anyhow::Result<Uuid> {
    let peers = PEERS.get().expect("Peers uninitialized");

    let mut table = Table {
        id: Uuid::now_v7(),
        peers: vec![],
        n,
        r,
        w,
    };

    for (index, peer) in peers.iter().enumerate() {
        let rpc = TablePrepare { bandwidth: b };
        let available = rpc.exec(peer).await?;
        table.peers.push((index as u64, available));
        b = b.saturating_sub(available);

        if b == 0 {
            break;
        }
    }

    let mut set = JoinSet::new();
    for (index, _) in &table.peers {
        let rpc = TableCommit {
            table: table.clone(),
        };
        let peer = &peers[*index as usize];
        set.spawn(keep_peer(peer, rpc.exec(peer)));
    }
    set.join_all().await;

    meta::create_table(&table)?;
    Ok(table.id)
}

#[derive(Serialize, Deserialize)]
struct TablePrepare {
    bandwidth: u64,
}

impl Rpc for TablePrepare {
    type Request = Operations;
    type Response = u64;

    fn into_variant(self) -> Self::Request {
        Operations::TablePrepare(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        // TODO: prepare la banda
        Ok(self.bandwidth.min(10))
    }
}

#[derive(Serialize, Deserialize)]
struct TableCommit {
    table: Table,
}

impl Rpc for TableCommit {
    type Request = Operations;
    type Response = ();

    fn into_variant(self) -> Self::Request {
        Operations::TableCommit(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        // TODO: commit la banda
        meta::create_table(&self.table)?;
        Ok(())
    }
}
