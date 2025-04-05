use crate::{
    exec::{table::TableError::*, Operations, Rpc, Table, TableData, TableParams, TableStatus},
    peer::{self, availability_zone, local_index},
    state, PREPARE_TIME,
};
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::LazyLock};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    task::{JoinHandle, JoinSet},
    time::sleep,
};
use uuid::Uuid;

static PENDING: LazyLock<Mutex<BTreeMap<Uuid, JoinHandle<()>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum TableError {
    #[error("could not store table: {0}")]
    Store(String),
    #[error("table was not prepared")]
    NotPrepared,
    #[error("expired table")]
    Expired,
    #[error("bandwidth error")]
    TooMuchBandwidth,
}

/// Allocates a table with the given parameters and returns its ID.
/// When this function returns the table will be ready to use from this node,
/// but not necessarily from all nodes.
pub async fn table_create(
    params @ TableParams { mut b, n, r, w }: TableParams,
) -> anyhow::Result<Uuid> {
    let id = Uuid::now_v7();
    log::info!("{id} creating with b={b}, n={n}, r={r}, w={w}");

    let mut allocation_zone = BTreeMap::new();
    let mut allocation_peer = BTreeMap::new();
    let mut allocated = 0;
    b *= n;
    for (index, peer) in peer::peers().iter().enumerate() {
        let rpc = TablePrepare { id, request: b / n };
        let (zone, available) = rpc.exec(peer).await??;
        log::debug!("{id} {zone} @ {} proposed {available}", peer.addr);

        let zone_remaining = (b / n).saturating_sub(*allocation_zone.get(&zone).unwrap_or(&0));
        let available = available.min(zone_remaining);
        log::debug!(
            "{id} {zone} @ {} will take {available}/{zone_remaining}",
            peer.addr
        );
        if available == 0 {
            continue;
        }

        allocation_zone
            .entry(zone.clone())
            .and_modify(|v| *v += available)
            .or_insert(available);
        allocation_peer.insert((zone, index as u64), available);
        allocated += available;
        log::debug!("{id} allocated {allocated}/{b}");

        if allocated >= b {
            break;
        }
    }

    log::info!(
        "{id}: found {} peers with a total of {allocated} bandwidth",
        allocation_peer.len()
    );

    if allocated < b {
        for (_, index) in allocation_peer.keys() {
            let rpc = TableDelete { id };
            let peer = &peer::peers()[*index as usize];
            tokio::spawn(rpc.exec(peer));
        }
        bail!("Could not allocate enough bandwidth.");
    }

    for x in allocation_peer.values_mut() {
        let y = *x * b / allocated;
        b -= y;
        allocated -= *x;
        *x = y;
    }

    let data = TableData {
        allocation: allocation_peer,
        params,
    };

    let mut set = JoinSet::new();
    for (_, index) in data.allocation.keys() {
        let rpc = TableCommit {
            id,
            table: data.clone(),
        };
        let peer = &peer::peers()[*index as usize];
        set.spawn(rpc.exec(peer));
    }
    let results = set.join_all().await;
    log::debug!("commit results {:?}", results);
    if results.iter().flatten().any(Result::is_err) {
        for (_, index) in data.allocation.keys() {
            let rpc = TableDelete { id };
            let peer = &peer::peers()[*index as usize];
            tokio::spawn(rpc.exec(peer));
        }
        bail!("could not commit table");
    }

    if !data
        .allocation
        .contains_key(&(availability_zone().to_owned(), local_index() as u64))
    {
        Table {
            id,
            status: TableStatus::Created(data),
        }
        .save()
        .await?;
    }

    Ok(id)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TablePrepare {
    id: Uuid,
    request: u64,
}

impl Rpc for TablePrepare {
    type Request = Operations;
    type Response = Result<(String, u64), TableError>;

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TablePrepare) -> Result<(String, u64), TableError> {
            log::info!("{} prepare", rpc.id);
            let proposed = state::bandwidth_alloc(rpc.request);

            Table {
                id: rpc.id,
                status: TableStatus::Prepared {
                    allocated: proposed,
                },
            }
            .save()
            .await
            .map_err(|e| Store(e.to_string()))?;

            PENDING.lock().await.insert(
                rpc.id,
                tokio::spawn(async move {
                    sleep(PREPARE_TIME).await;

                    Table::delete_if_prepared(rpc.id)
                        .await
                        .expect("could not delete table");

                    PENDING.lock().await.remove(&rpc.id);
                }),
            );

            Ok((availability_zone().to_owned(), proposed))
        }

        Ok(inner(self).await)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableCommit {
    id: Uuid,
    table: TableData,
}

impl Rpc for TableCommit {
    type Request = Operations;
    type Response = Result<(), TableError>;

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TableCommit) -> Result<(), TableError> {
            log::info!("{} commit", rpc.id);
            let table = Table::load(rpc.id)
                .map_err(|e| Store(e.to_string()))?
                .ok_or(NotPrepared)?;

            let TableStatus::Prepared { allocated } = table.status else {
                return Err(Expired);
            };

            let pre_bandwidth = allocated;
            let post_bandwidth = rpc
                .table
                .allocation
                .get(&(availability_zone().to_owned(), local_index() as u64))
                .copied()
                .unwrap_or(0);

            if post_bandwidth > pre_bandwidth {
                return Err(TooMuchBandwidth);
            }

            state::bandwidth_free(pre_bandwidth - post_bandwidth);

            Table {
                id: rpc.id,
                status: TableStatus::Created(rpc.table),
            }
            .save()
            .await
            .map_err(|e| Store(e.to_string()))?;

            if let Some(task) = PENDING.lock().await.remove(&rpc.id) {
                task.abort();
            }
            Ok(())
        }

        Ok(inner(self).await)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableDelete {
    id: Uuid,
}

impl Rpc for TableDelete {
    type Request = Operations;
    type Response = Result<(), TableError>;

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(Table::delete(self.id)
            .await
            .map_err(|e| Store(e.to_string())))
    }
}

pub async fn table_delete(id: Uuid) -> anyhow::Result<()> {
    log::info!("{} delete", id);

    let table = Table::load(id)?;
    let Some(table) = table else {
        bail!("Table not found");
    };
    let TableStatus::Created(data) = table.status else {
        bail!("Invalid table");
    };

    let mut set = JoinSet::new();
    for &(_, peer_id) in data.allocation.keys() {
        let rpc = TableDelete { id };
        set.spawn(rpc.exec(&peer::peers()[peer_id as usize]));
    }
    set.join_all().await;

    Table::delete(id).await?;
    Ok(())
}
