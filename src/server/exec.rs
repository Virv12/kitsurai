use crate::exec::TableError::{Bandwidth, Expired, Store, Unexisting, WrongStatus};
use crate::meta::{Table, TableStatus};
use crate::peer::SELF_INDEX;
use crate::{
    meta,
    peer::{Peer, PEERS},
    rpc::{Rpc, RpcRequest},
    store, BANDWIDTH, NECESSARY_READ, NECESSARY_WRITE, TIMEOUT,
};
use anyhow::{bail, Context};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::{future::Future, net::SocketAddr};
use thiserror::Error;
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
    if table.get_version().context("invalid table id")? != uuid::Version::SortRand {
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Error, Debug, Serialize, Deserialize)]
enum TableError {
    #[error("table does not have expected status {0:?}")]
    WrongStatus(TableStatus),
    #[error("could not store table")]
    Store,
    #[error("could not store table")]
    Unexisting,
    #[error("expired table")]
    Expired,
    #[error("bandwidth error")]
    Bandwidth,
}

pub async fn table_create(mut b: u64, n: u64, r: u64, w: u64) -> anyhow::Result<Uuid> {
    log::info!("Creating table with b={b}, n={n}, r={r}, w={w}.");

    let peers = PEERS.get().expect("Peers uninitialized");

    let mut table = Table {
        id: Uuid::now_v7(),
        status: TableStatus::Prepared,
        peers: vec![],
        n,
        r,
        w,
    };

    let mut allocated = 0;
    b *= n;
    for (index, peer) in peers.iter().enumerate() {
        let rpc = TablePrepare {
            table: table.clone(),
            request: b,
        };
        let available = rpc.exec(peer).await??;
        if available == 0 {
            continue;
        }
        let available = available.min(b / n);
        table.peers.push((index as u64, available));
        allocated += available;
        if allocated >= b {
            break;
        }
    }

    log::info!(
        "Found {} peers with a total of {} bandwidth.",
        table.peers.len(),
        allocated
    );

    if allocated < b {
        bail!("Could not allocate enough bandwidth.");
    }

    for (_, x) in &mut table.peers {
        let y = *x * b / allocated;
        b -= y;
        allocated -= *x;
        *x = y;
    }

    table.status = TableStatus::Created;

    let mut set = JoinSet::new();
    let mut own_table = false;
    for (index, _) in &table.peers {
        let rpc = TableCommit {
            table: table.clone(),
        };
        let peer = &peers[*index as usize];
        own_table |= peer.is_self;
        set.spawn(keep_peer(peer, rpc.exec(peer)));
    }
    set.join_all().await;

    if !own_table {
        meta::set_table(&table)?;
    }
    Ok(table.id)
}

#[derive(Debug, Serialize, Deserialize)]
struct TablePrepare {
    table: Table,
    request: u64,
}

impl Rpc for TablePrepare {
    type Request = Operations;
    type Response = Result<u64, TableError>;

    fn into_variant(self) -> Self::Request {
        Operations::TablePrepare(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(mut rpc: TablePrepare) -> Result<u64, TableError> {
            if rpc.table.status != TableStatus::Prepared {
                return Err(WrongStatus(TableStatus::Prepared));
            }

            let mut available = BANDWIDTH.load(Ordering::Relaxed);
            let proposed = loop {
                let proposed = rpc.request.min(available);
                match BANDWIDTH.compare_exchange(
                    available,
                    available - proposed,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break proposed,
                    Err(new) => available = new,
                }
            };

            let self_index = *SELF_INDEX.get().expect("self_index uninitialized") as u64;
            rpc.table.peers.push((self_index, proposed));
            meta::set_table(&rpc.table).map_err(|_| Store)?;

            Ok(proposed)
        }

        Ok(inner(self).await)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableCommit {
    table: Table,
}

impl Rpc for TableCommit {
    type Request = Operations;
    type Response = Result<(), TableError>;

    fn into_variant(self) -> Self::Request {
        Operations::TableCommit(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TableCommit) -> Result<(), TableError> {
            if rpc.table.status != TableStatus::Created {
                return Err(WrongStatus(TableStatus::Created));
            }

            let table = meta::get_table(rpc.table.id)
                .map_err(|_| Store)?
                .ok_or(Unexisting)?;

            if table.status != TableStatus::Prepared {
                return Err(Expired);
            }

            let self_index = *SELF_INDEX.get().expect("self uninitialized") as u64;
            let pre_bandwidth = table
                .peers
                .iter()
                .filter_map(|(i, b)| if *i == self_index { Some(*b) } else { None })
                .next()
                .unwrap_or(0);
            let post_bandwidth = rpc
                .table
                .peers
                .iter()
                .filter_map(|(i, b)| if *i == self_index { Some(*b) } else { None })
                .next()
                .unwrap_or(0);

            if post_bandwidth > pre_bandwidth {
                return Err(Bandwidth);
            }

            BANDWIDTH.fetch_add(pre_bandwidth - post_bandwidth, Ordering::Relaxed);

            meta::set_table(&rpc.table).map_err(|_| Store)?;

            Ok(())
        }

        Ok(inner(self).await)
    }
}
