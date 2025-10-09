//! Handles table metadata, exposing methods to load, save and list metadata.

mod bandwidth;
mod store;

use crate::exec::table::TableError::{Expired, NotPrepared, State, TooMuchBandwidth};
use crate::{
    merkle::{self, Merkle},
    peer::{self, local_index, Peer},
};
use anyhow::{bail, Result};
use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::sync::RwLock as StdRwLock;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};
use std::{collections::HashMap, sync::LazyLock};
use store::KeyValue;
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableStatus {
    Prepared { allocated: NonZeroU64 },
    Created(TableData),
    Deleted,
}

impl TableStatus {
    fn age(&self) -> usize {
        match self {
            TableStatus::Prepared { .. } => 1,
            TableStatus::Created(_) => 2,
            TableStatus::Deleted => 3,
        }
    }

    fn local_bandwidth(&self) -> u64 {
        match self {
            &TableStatus::Prepared { allocated } => allocated.get(),
            TableStatus::Created(TableData { allocation, .. }) => allocation
                .get(&(local_index() as u64))
                .copied()
                .map_or(0, NonZeroU64::get),
            TableStatus::Deleted => 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct TableParams {
    pub b: u64,
    pub n: u64,
    pub r: u64,
    pub w: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableData {
    pub allocation: BTreeMap<u64, NonZeroU64>,
    pub params: TableParams,
}

impl TableData {
    /// Return the peer on position `virt` on the hash ring.
    fn virt_to_peer(&self, virt: u64) -> &'static Peer {
        let TableParams { b, n, .. } = self.params;
        let ord = virt / n + virt % n * b;
        let index = self
            .allocation
            .iter()
            .scan(0, |acc, (&k, &v)| {
                *acc += v.get();
                Some((k, *acc))
            })
            .find(|&(_, acc)| acc > ord)
            .expect("index should be less than sigma")
            .0;

        &peer::peers()[index as usize]
    }

    /// Return an iterator over the peers that are responsible for the given key.
    pub fn peers_for_key(&self, key: &[u8]) -> impl Iterator<Item = &'static Peer> + use<'_> {
        let hash = xxh3_64(key);
        let TableParams { b, n, .. } = self.params;
        let virt = ((hash as u128 * (b * n) as u128) >> 64) as u64;
        (0..n).map(move |i| self.virt_to_peer(virt + i))
    }
}

static LOCK: LazyLock<RwLock<()>> = LazyLock::new(|| RwLock::new(()));
static MERKLE: LazyLock<Mutex<Merkle>> = LazyLock::new(|| Mutex::new(Merkle::new()));
static SCHED_BASE_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);
static SCHED_AVAIL_AT: LazyLock<StdRwLock<HashMap<Uuid, AtomicU64>>> =
    LazyLock::new(|| StdRwLock::new(HashMap::new()));
static TABLE_CACHE: LazyLock<StdRwLock<Table>> = LazyLock::new(|| {
    StdRwLock::new(Table {
        id: Uuid::nil(),
        status: TableStatus::Deleted,
    })
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: Uuid,
    pub status: TableStatus,
}

impl Table {
    pub async fn list() -> Result<Vec<Self>> {
        let _guard = LOCK.read().await;

        log::debug!("list");
        let tables = store::item_list(META)
            .await?
            .into_iter()
            .map(|(key, value)| Self {
                id: Uuid::from_slice(&key).expect("id deserialization failed"),
                status: postcard::from_bytes(value.as_ref()).expect("table deserialization failed"),
            })
            .collect();

        Ok(tables)
    }

    async fn locked_load(id: Uuid) -> Result<Option<Self>> {
        log::debug!("{id} load");
        assert!(LOCK.try_write().is_err());

        {
            let cache = TABLE_CACHE.read().expect("poisoned");
            if cache.id == id {
                return Ok(Some(cache.clone()));
            }
        }
        let blob = store::item_get(META, id.as_bytes()).await?;
        let table = blob.map(|blob| Self {
            id,
            status: postcard::from_bytes(blob.as_ref()).expect("table deserialization failed"),
        });
        {
            if let Some(table) = &table {
                *TABLE_CACHE.write().expect("poisoned") = table.clone();
            }
        }
        Ok(table)
    }

    pub async fn load(id: Uuid) -> Result<Option<Self>> {
        let _guard = LOCK.read().await;
        Self::locked_load(id).await
    }

    async fn locked_save(&self) -> Result<()> {
        log::debug!("{} save", self.id);
        assert!(LOCK.try_read().is_err());

        let blob = postcard::to_allocvec(&self.status)?;
        store::item_set(META, self.id.as_bytes(), &blob).await?;

        match &self.status {
            TableStatus::Prepared { .. } => {}
            TableStatus::Created(_) => {
                MERKLE.lock().await.insert(self.id.to_u128_le(), &blob);
                SCHED_AVAIL_AT
                    .write()
                    .expect("poisoned")
                    .insert(self.id, AtomicU64::new(0));
            }
            TableStatus::Deleted => {
                MERKLE.lock().await.insert(self.id.to_u128_le(), &blob);
                SCHED_AVAIL_AT.write().expect("poisoned").remove(&self.id);
            }
        }

        // Update cache.
        {
            *TABLE_CACHE.write().expect("poisoned") = self.clone();
        }

        Ok(())
    }

    pub async fn prepare(id: Uuid, requested: u64) -> Result<Option<NonZeroU64>> {
        log::debug!("{id} prepare for {requested}");
        let _guard = LOCK.write().await;

        let table = Self::locked_load(id)
            .await
            .map_err(|e| State(e.to_string()))?;
        if table.is_some() {
            bail!("table should not exist")
        }

        let Some(proposed) = NonZeroU64::new(bandwidth::alloc(requested)) else {
            return Ok(None);
        };

        Table {
            id,
            status: TableStatus::Prepared {
                allocated: proposed,
            },
        }
        .locked_save()
        .await?;

        Ok(Some(proposed))
    }

    pub async fn commit(id: Uuid, data: TableData) -> Result<()> {
        log::debug!("{id} commit");
        let _guard = LOCK.write().await;

        let table = Self::locked_load(id)
            .await
            .map_err(|e| State(e.to_string()))?
            .ok_or(NotPrepared)?;

        let TableStatus::Prepared {
            allocated: pre_bandwidth,
        } = table.status
        else {
            return Err(Expired.into());
        };

        let post_bandwidth = data
            .allocation
            .get(&(local_index() as u64))
            .copied()
            .map_or(0, NonZeroU64::get);

        if post_bandwidth > pre_bandwidth.get() {
            return Err(TooMuchBandwidth.into());
        }
        bandwidth::free(pre_bandwidth.get() - post_bandwidth);

        Table {
            id,
            status: TableStatus::Created(data),
        }
        .locked_save()
        .await?;

        Ok(())
    }

    pub async fn delete_if_prepared(id: Uuid) -> Result<Table> {
        log::debug!("{id} delete_if_prepared");
        let _guard = LOCK.write().await;

        let mut table = Self::locked_load(id)
            .await
            .ok()
            .flatten()
            .expect("table should exists");

        if let TableStatus::Prepared { allocated } = table.status {
            table.status = TableStatus::Deleted;
            table.locked_save().await?;
            bandwidth::free(allocated.get());
        }
        Ok(table)
    }

    pub async fn delete(id: Uuid) -> Result<()> {
        log::debug!("{id} delete");
        let _guard = LOCK.write().await;

        let mut table = Self::locked_load(id)
            .await
            .ok()
            .flatten()
            .expect("table should exists");

        let allocated = table.status.local_bandwidth();
        table.status = TableStatus::Deleted;
        table.locked_save().await?;
        bandwidth::free(allocated);
        store::table_delete(id).await?;
        Ok(())
    }

    pub async fn save(&self) -> Result<()> {
        log::debug!("{} save", self.id);
        let _guard = LOCK.write().await;

        let current = Self::locked_load(self.id).await.ok().flatten();
        if current.as_ref().map_or(0, |t| t.status.age()) >= self.status.age() {
            bail!("not a growing save");
        }

        let pre_bandwidth = current.map_or(0, |t| t.status.local_bandwidth());
        let post_bandwidth = self.status.local_bandwidth();
        if pre_bandwidth > post_bandwidth {
            bandwidth::free(pre_bandwidth - post_bandwidth);
        } else {
            let diff = post_bandwidth - pre_bandwidth;
            let allocated = bandwidth::alloc(diff);
            if diff != allocated {
                bandwidth::free(allocated);
                bail!("insufficient bandwidth");
            };
        }

        self.locked_save().await?;

        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct StateCli {
    /// Configuration for the storage backend component.
    #[clap(flatten)]
    store_cli: store::StoreCli,

    /// Available _"bandwidth"_ for this peer.
    /// See [BANDWIDTH] for more details.
    #[arg(short, long, default_value = "100")]
    bandwidth: u64,
}

pub async fn init(cli: StateCli) {
    log::info!("Initialize state");
    store::init(cli.store_cli);
    bandwidth::init(cli.bandwidth);

    LazyLock::force(&SCHED_BASE_TIME);

    let mut merkle = MERKLE.lock().await;

    for table in Table::list().await.expect("could not get table list") {
        let table = Table::delete_if_prepared(table.id).await.unwrap();

        let requested = table.status.local_bandwidth();
        if bandwidth::alloc(requested) != requested {
            panic!("Overflowed bandwidth on initialization. Database may be corrupted?");
        }

        let data = postcard::to_allocvec(&table.status).unwrap();
        merkle.insert(table.id.to_u128_le(), &data);

        if matches!(table.status, TableStatus::Created(_)) {
            let mut available = SCHED_AVAIL_AT.write().expect("poisoned");
            available.insert(table.id, AtomicU64::new(0));
        }
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
pub enum Error {
    #[error("table not found")]
    TableNotFound,
    #[error("store error")]
    Store(#[from] store::Error),
}

async fn sched_wait(table: &Table) {
    let bandwidth = table.status.local_bandwidth() as u32;

    let instant = {
        let available = SCHED_AVAIL_AT.read().expect("poisoned");
        let available = available
            .get(&table.id)
            .expect("table should be registered");

        let filter = SCHED_BASE_TIME.elapsed() - tokio::time::Duration::from_secs(1) / 10;
        let filter = (filter * bandwidth).as_secs();
        available.fetch_max(filter, Ordering::Relaxed);

        let at = available.fetch_add(1, Ordering::Relaxed);
        *SCHED_BASE_TIME + tokio::time::Duration::from_secs(at) / bandwidth
    };
    tokio::time::sleep_until(instant).await;
}

pub async fn item_get(table_id: Uuid, key: &[u8]) -> Result<Option<Bytes>, Error> {
    // let _guard = LOCK.read().await;
    let table = Table::load(table_id).await.unwrap();
    if !table
        .as_ref()
        .is_some_and(|t| matches!(t.status, TableStatus::Created(_)))
    {
        return Err(Error::TableNotFound);
    }
    sched_wait(table.as_ref().unwrap()).await;
    Ok(store::item_get(table_id, key).await?)
}

pub async fn item_set(table_id: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    // let _guard = LOCK.read().await;
    let table = Table::load(table_id).await.unwrap();
    if !table
        .as_ref()
        .is_some_and(|t| matches!(t.status, TableStatus::Created(_)))
    {
        return Err(Error::TableNotFound);
    }
    sched_wait(table.as_ref().unwrap()).await;
    Ok(store::item_set(table_id, key, value).await?)
}

pub async fn item_list(table_id: Uuid) -> Result<Vec<KeyValue>, Error> {
    // let _guard = LOCK.read().await;
    let table = Table::load(table_id).await.unwrap();
    if !table.is_some_and(|t| matches!(t.status, TableStatus::Created(_))) {
        return Err(Error::TableNotFound);
    }
    Ok(store::item_list(table_id).await?)
}

pub async fn merkle_find(path: merkle::Path) -> Option<(merkle::Path, u128)> {
    MERKLE.lock().await.find(path)
}
