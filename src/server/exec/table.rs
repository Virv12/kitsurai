use crate::{
    exec::{
        keep_peer, table::TableError::*, Operations, Rpc, Table, TableData, TableParams,
        TableStatus,
    },
    peer::{self, availability_zone, local_index},
    BANDWIDTH, PREPARE_TIME,
};
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Mutex},
};
use thiserror::Error;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::sleep,
};
use uuid::Uuid;

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
        set.spawn(keep_peer(peer, rpc.exec(peer)));
    }
    set.join_all().await;

    if !data
        .allocation
        .contains_key(&(availability_zone().to_owned(), local_index() as u64))
    {
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
    type Response = Result<(String, u64), TableError>;

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TablePrepare) -> Result<(String, u64), TableError> {
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

            Ok((availability_zone().to_owned(), proposed))
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
                .get(&(availability_zone().to_owned(), local_index() as u64))
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableDelete {
    id: Uuid,
}

impl Rpc for TableDelete {
    type Request = Operations;
    type Response = Result<(), TableError>;

    async fn handle(self) -> anyhow::Result<Self::Response> {
        async fn inner(rpc: TableDelete) -> Result<(), TableError> {
            let Some(old) = Table {
                id: rpc.id,
                status: TableStatus::Deleted,
            }
            .save()
            .map_err(|_| Store)?
            else {
                return Ok(());
            };

            match old.status {
                TableStatus::Prepared { allocated } => {
                    BANDWIDTH.fetch_add(allocated, Ordering::Relaxed);
                }
                TableStatus::Created(data) => {
                    if let Some(&allocated) = data
                        .allocation
                        .get(&(availability_zone().to_owned(), local_index() as u64))
                    {
                        BANDWIDTH.fetch_add(allocated, Ordering::Relaxed);
                    }
                }
                TableStatus::Deleted => {}
            }

            Ok(())
        }

        Ok(inner(self).await)
    }
}
