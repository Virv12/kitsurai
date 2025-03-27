use crate::{
    exec::{keep_peer, Operations, Peer, Rpc},
    meta::{Table, TableStatus},
    peer, store, TIMEOUT,
};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::{task::JoinSet, time::timeout};
use uuid::Uuid;

pub async fn item_get(id: Uuid, key: Bytes) -> Result<Vec<Option<Bytes>>> {
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
        match res.expect("join error") {
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

pub async fn item_set(id: Uuid, key: Bytes, value: Bytes) -> Result<()> {
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

pub async fn item_list(table: Uuid) -> Result<Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>> {
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

    async fn handle(self) -> Result<Self::Response> {
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

    async fn handle(self) -> Result<Self::Response> {
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

    async fn handle(self) -> Result<Self::Response> {
        Ok(store::item_list(self.table))
    }
}
