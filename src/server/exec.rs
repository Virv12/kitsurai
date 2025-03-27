use crate::{
    exec::TableError::{Expired, NotPrepared, Store, TooMuchBandwidth},
    meta::{Table, TableData, TableParams, TableStatus},
    peer::{self, local_index, Peer},
    rpc::Rpc,
    store, BANDWIDTH, PREPARE_TIME, TIMEOUT,
};
use anyhow::{bail, Context};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::atomic::Ordering};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn keep_peer<O>(peer: &Peer, task: impl Future<Output = O>) -> (&Peer, O) {
    (peer, task.await)
}

pub async fn item_get(id: Uuid, key: Bytes) -> anyhow::Result<Vec<Option<Bytes>>> {
    if id.get_version().context("invalid table id")? != uuid::Version::SortRand {
        bail!("table id has invalid version")
    }

    let table = Table::load(id)?.context("table not found")?;
    let TableStatus::Created(table) = table.status else {
        bail!("table is not created")
    };

    let mut set = Box::new(JoinSet::new());
    for peer in table.peers_for_key(&key) {
        let rpc = ItemGet {
            table: id,
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
            (Peer { addr, .. }, Ok(Ok(Err(store_error)))) => {
                eprintln!("GET: store error on {addr}, {store_error}")
            }
            (Peer { addr, .. }, Ok(Err(rpc_error))) => {
                eprintln!("GET: rpc error on {addr}, {rpc_error}")
            }
            (Peer { addr, .. }, Err(timeout_error)) => {
                eprintln!("GET: timeout error on {addr}, {timeout_error}")
            }
        }

        if successes >= table.params.r {
            set.abort_all();
            break;
        }
    }

    Ok(results)
}

pub async fn item_set(id: Uuid, key: Bytes, value: Bytes) -> anyhow::Result<()> {
    if id.get_version().context("invalid table id")? != uuid::Version::SortRand {
        bail!("table id has invalid version")
    }

    let table = Table::load(id)?.context("table not found")?;
    let TableStatus::Created(table) = table.status else {
        bail!("table is not created")
    };

    let mut set = JoinSet::new();
    for peer in table.peers_for_key(&key) {
        let rpc = ItemSet {
            table: id,
            key: key.clone(),
            value: value.clone(),
        };
        set.spawn(keep_peer(peer, timeout(TIMEOUT, rpc.exec(peer))));
    }

    let mut successes = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("Join error") {
            (_, Ok(Ok(Ok(())))) => successes += 1,
            (Peer { addr, .. }, Ok(Ok(Err(store_error)))) => {
                eprintln!("GET: store error on {addr}, {store_error}")
            }
            (Peer { addr, .. }, Ok(Err(rpc_error))) => {
                eprintln!("GET: rpc error on {addr}, {rpc_error}")
            }
            (Peer { addr, .. }, Err(timeout_error)) => {
                eprintln!("GET: timeout error on {addr}, {timeout_error}")
            }
        }

        if successes >= table.params.w {
            set.detach_all();
            return Ok(());
        }
    }

    bail!(
        "Failed to write to {} nodes, only {successes} succeeded.",
        table.params.w
    )
}

pub async fn item_list(table: Uuid) -> anyhow::Result<Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>> {
    let mut set = JoinSet::new();
    for peer in peer::peers() {
        set.spawn(keep_peer(
            peer,
            timeout(TIMEOUT, ItemList { table }.exec(peer)),
        ));
    }

    let mut data = Vec::new();
    while let Some(res) = set.join_next().await {
        let (peer, res) = res.expect("Join error");
        let res = match res {
            Ok(Ok(Ok(res))) => res,
            _ => Vec::new(),
        };
        data.push((peer.addr.clone(), res));
    }

    Ok(data)
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Operations {
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

    pub(crate) async fn listener(
        listener: TcpListener,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        async fn recv(mut stream: TcpStream) -> anyhow::Result<()> {
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).await?;
            let variant: Operations = postcard::from_bytes(&buffer)?;
            log::info!("Request: {}", variant.name());

            match variant {
                Operations::ItemGet(get) => get.remote(stream).await,
                Operations::ItemSet(set) => set.remote(stream).await,
                Operations::ItemList(list) => list.remote(stream).await,
                Operations::TablePrepare(prepare) => prepare.remote(stream).await,
                Operations::TableCommit(commit) => commit.remote(stream).await,
            }
        }

        loop {
            let result = tokio::select! {
                _ = token.cancelled() => break,
                result = listener.accept() => result,
            };

            let (socket, peer) = result?;
            tokio::spawn(async move {
                match recv(socket).await {
                    Ok(_) => eprintln!("RPC: successfully handled for {}", peer.ip()),
                    Err(error) => eprintln!("RPC: error while handling {peer}, {error}"),
                };
            });
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ItemGet {
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
pub(crate) struct ItemSet {
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
pub(crate) struct ItemList {
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

#[derive(Error, Debug, Serialize, Deserialize)]
pub(crate) enum TableError {
    #[error("could not store table")]
    Store,
    #[error("could not store table")]
    NotPrepared,
    #[error("expired table")]
    Expired,
    #[error("bandwidth error")]
    TooMuchBandwidth,
}

pub(crate) async fn table_create(
    params @ TableParams { mut b, n, r, w }: TableParams,
) -> anyhow::Result<Uuid> {
    if r <= n {
        bail!("R is greater then N");
    }
    if w <= n {
        bail!("W is greater then N");
    }

    log::info!("Creating table with b={b}, n={n}, r={r}, w={w}.");

    let id = Uuid::now_v7();
    let mut allocation = BTreeMap::new();
    let mut allocated = 0;
    b *= n;
    for (index, peer) in peer::peers().iter().enumerate() {
        let rpc = TablePrepare { id, request: b };
        let available = rpc.exec(peer).await??;
        if available == 0 {
            continue;
        }

        let available = available.min(b / n);
        allocation.insert(index as u64, available);
        allocated += available;
        if allocated >= b {
            break;
        }
    }

    log::info!(
        "Found {} peers with a total of {} bandwidth.",
        allocation.len(),
        allocated
    );

    if allocated < b {
        bail!("Could not allocate enough bandwidth.");
    }

    for x in allocation.values_mut() {
        let y = *x * b / allocated;
        b -= y;
        allocated -= *x;
        *x = y;
    }

    let data = TableData { allocation, params };

    let mut set = JoinSet::new();
    for index in data.allocation.keys() {
        let rpc = TableCommit {
            id,
            table: data.clone(),
        };
        let peer = &peer::peers()[*index as usize];
        set.spawn(keep_peer(peer, rpc.exec(peer)));
    }
    set.join_all().await;

    if !data.allocation.contains_key(&(local_index() as u64)) {
        Table {
            id,
            status: TableStatus::Created(data),
        }
        .save()?;
    }

    Ok(id)
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TablePrepare {
    id: Uuid,
    request: u64,
}

impl Rpc for TablePrepare {
    type Request = Operations;
    type Response = Result<u64, TableError>;

    fn into_variant(self) -> Self::Request {
        Operations::TablePrepare(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TablePrepare) -> Result<u64, TableError> {
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

            Table {
                id: rpc.id,
                status: TableStatus::Prepared {
                    allocated: proposed,
                },
            }
            .save()
            .map_err(|_| Store)?;

            tokio::spawn(async move {
                sleep(PREPARE_TIME).await;
                Table::delete_if_prepared(rpc.id);
            });

            Ok(proposed)
        }

        Ok(inner(self).await)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableCommit {
    id: Uuid,
    table: TableData,
}

impl Rpc for TableCommit {
    type Request = Operations;
    type Response = Result<(), TableError>;

    fn into_variant(self) -> Self::Request {
        Operations::TableCommit(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TableCommit) -> Result<(), TableError> {
            let table = Table::load(rpc.id).map_err(|_| Store)?.ok_or(NotPrepared)?;

            let TableStatus::Prepared { allocated } = table.status else {
                return Err(Expired);
            };

            let pre_bandwidth = allocated;
            let post_bandwidth = rpc
                .table
                .allocation
                .get(&(local_index() as u64))
                .copied()
                .unwrap_or(0);

            if post_bandwidth > pre_bandwidth {
                return Err(TooMuchBandwidth);
            }

            BANDWIDTH.fetch_add(pre_bandwidth - post_bandwidth, Ordering::Relaxed);

            Table {
                id: rpc.id,
                status: TableStatus::Created(rpc.table),
            }
            .save()
            .map_err(|_| Store)?;

            Ok(())
        }

        Ok(inner(self).await)
    }
}
