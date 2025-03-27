use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Mutex},
};

use anyhow::bail;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    exec::{keep_peer, table::TableError::*, Table, TableData, TableStatus},
    peer::{self, local_index},
    BANDWIDTH, PREPARE_TIME,
};

use super::{Operations, Rpc, TableParams};

static PENDING: Mutex<BTreeMap<Uuid, JoinHandle<()>>> = Mutex::new(BTreeMap::new());

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
            log::info!("{} prepare", rpc.id);
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

            PENDING.lock().expect("poisoned lock").insert(
                rpc.id,
                tokio::spawn(async move {
                    sleep(PREPARE_TIME).await;

                    Table::delete_if_prepared(rpc.id);

                    PENDING.lock().expect("poisoned lock").remove(&rpc.id);
                }),
            );

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
            log::info!("{} commit", rpc.id);
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

            if let Some(task) = PENDING.lock().expect("poisoned lock").remove(&rpc.id) {
                task.abort();
            }
            Ok(())
        }

        Ok(inner(self).await)
    }
}
