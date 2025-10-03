//! Handles table metadata, exposing methods to load, save and list metadata.

mod store;

use crate::{
    merkle::{self, Merkle},
    peer::{self, availability_zone, local_index, Peer},
};
use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
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
    Prepared { allocated: u64 },
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
    pub allocation: BTreeMap<(String, u64), u64>,
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
            .scan(0, |acc, (k, &v)| {
                *acc += v;
                Some((k, *acc))
            })
            .find(|&(_, acc)| acc > ord)
            .expect("index should be less than sigma")
            .0
             .1;

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
static SCHED_AVAIL_AT: LazyLock<RwLock<HashMap<Uuid, AtomicU64>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static TABLE_CACHE: LazyLock<Mutex<Table>> = LazyLock::new(|| {
    Mutex::new(Table {
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
    pub async fn load(id: Uuid) -> Result<Option<Self>> {
        log::debug!("{id} load");
        {
            let cache = TABLE_CACHE.lock().await;
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
                *TABLE_CACHE.lock().await = table.clone();
            }
        }
        Ok(table)
    }

    async fn save_inner(&self) -> Result<Option<Table>> {
        let old = Table::load(self.id).await?;
        {
            *TABLE_CACHE.lock().await = self.clone();
        }
        let blob = postcard::to_allocvec(&self.status)?;
        store::item_set(META, self.id.as_bytes(), &blob).await?;
        match &self.status {
            TableStatus::Prepared { .. } => {}
            TableStatus::Created(_) => {
                MERKLE.lock().await.insert(self.id.to_u128_le(), &blob);
                SCHED_AVAIL_AT
                    .write()
                    .await
                    .insert(self.id, AtomicU64::new(0));
            }
            TableStatus::Deleted => {
                MERKLE.lock().await.insert(self.id.to_u128_le(), &blob);
                SCHED_AVAIL_AT.write().await.remove(&self.id);
            }
        }
        Ok(old)
    }

    pub async fn list() -> Result<Vec<Self>> {
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

    pub async fn save(&self) -> Result<Option<Table>> {
        log::debug!("{} save", self.id);
        let _guard = LOCK.write().await;
        self.save_inner().await
    }

    pub async fn growing_save(&self) -> Result<Option<Table>> {
        log::debug!("{} checked_save", self.id);
        let _guard = LOCK.write().await;
        let old = Table::load(self.id).await?;
        if old.as_ref().map_or(0, |o| o.status.age()) >= self.status.age() {
            return Ok(old);
        }
        self.save_inner().await
    }

    pub async fn delete_if_prepared(id: Uuid) -> Result<()> {
        log::debug!("{id} delete_if_prepared");
        let _guard = LOCK.write().await;
        let mut table = Table::load(id)
            .await
            .ok()
            .flatten()
            .expect("table should exists");
        if let TableStatus::Prepared { allocated } = table.status {
            table.status = TableStatus::Deleted;
            table.save_inner().await?;
            bandwidth_free(allocated);
        }
        Ok(())
    }

    pub async fn delete(id: Uuid) -> Result<()> {
        log::debug!("{id} delete");
        let _guard = LOCK.write().await;
        let mut table = Table::load(id)
            .await
            .ok()
            .flatten()
            .expect("table should exists");
        let allocated = match table.status {
            TableStatus::Prepared { allocated } => allocated,
            TableStatus::Created(ref data) => data
                .allocation
                .get(&(availability_zone().to_owned(), local_index() as u64))
                .copied()
                .unwrap_or(0),
            _ => 0,
        };
        table.status = TableStatus::Deleted;
        table.save_inner().await?;
        bandwidth_free(allocated);
        store::table_delete(id).await?;
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
    // Initialize the store backend.
    store::init(cli.store_cli);

    log::info!("Initialize state");

    // Set the initial bandwidth as given by configuration.
    BANDWIDTH.store(cli.bandwidth as i64, Ordering::Relaxed);

    LazyLock::force(&SCHED_BASE_TIME);

    let mut merkle = MERKLE.lock().await;
    let mut available = SCHED_AVAIL_AT.write().await;

    for mut table in Table::list().await.expect("could not get table list") {
        match table.status {
            TableStatus::Prepared { .. } => {
                table.status = TableStatus::Deleted;
                table.save().await.expect("failed to prepared table");
            }
            TableStatus::Created(ref data) => {
                if let Some(&allocated) = data
                    .allocation
                    .get(&(availability_zone().to_owned(), (local_index() as u64)))
                {
                    let prev = BANDWIDTH.fetch_sub(allocated as i64, Ordering::Relaxed);
                    if prev - (allocated as i64) < 0 {
                        panic!(
                            "Overflowed bandwidth on initialization. Database may be corrupted?"
                        );
                    }
                }

                let data = postcard::to_allocvec(&table.status).unwrap();
                merkle.insert(table.id.to_u128_le(), &data);

                available.insert(table.id, AtomicU64::new(0));
            }
            TableStatus::Deleted => {
                let data = postcard::to_allocvec(&table.status).unwrap();
                merkle.insert(table.id.to_u128_le(), &data);
            }
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
    let bandwidth = match &table.status {
        TableStatus::Created(data) => data
            .allocation
            .get(&(availability_zone().to_owned(), local_index() as u64))
            .copied()
            .expect("table should be allocated") as u32,
        _ => panic!("table must be created to wait for scheduler"),
    };

    let available = SCHED_AVAIL_AT.read().await;
    let available = available
        .get(&table.id)
        .expect("table should be registered");

    let filter = SCHED_BASE_TIME.elapsed() - tokio::time::Duration::from_secs(1) / 10;
    let filter = (filter * bandwidth).as_secs();
    available.fetch_max(filter, Ordering::Relaxed);

    let at = available.fetch_add(1, Ordering::Relaxed);
    let instant = *SCHED_BASE_TIME + tokio::time::Duration::from_secs(at) / bandwidth;
    tokio::time::sleep_until(instant).await;
}

pub async fn item_get(table_id: Uuid, key: &[u8]) -> Result<Option<Bytes>, Error> {
    let _guard = LOCK.read().await;
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
    let _guard = LOCK.read().await;
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
    let _guard = LOCK.read().await;
    let table = Table::load(table_id).await.unwrap();
    if !table.is_some_and(|t| matches!(t.status, TableStatus::Created(_))) {
        return Err(Error::TableNotFound);
    }
    Ok(store::item_list(table_id).await?)
}

pub async fn merkle_find(path: merkle::Path) -> Option<(merkle::Path, u128)> {
    MERKLE.lock().await.find(path)
}

/// Holds the current available _"bandwidth"_.
///
/// Strictly speaking it has no unit and its value can be set semi-arbitrarily.<br>
/// In practice the unit chosen is the smallest amount that can be allocated by a table.
static BANDWIDTH: AtomicI64 = AtomicI64::new(0);

/// Allocates _"bandwidth"_ for a table.
///
/// May return less than requested if there is not enough available.
pub fn bandwidth_alloc(request: u64) -> u64 {
    let request = request as i64;
    let available = BANDWIDTH.fetch_sub(request, Ordering::Relaxed);
    let proposed = available.min(request).max(0);
    BANDWIDTH.fetch_add(request - proposed, Ordering::Relaxed);
    proposed as u64
}

/// Frees _"bandwidth"_ previously allocated by a table.
pub fn bandwidth_free(allocated: u64) {
    BANDWIDTH.fetch_add(allocated as i64, Ordering::Relaxed);
}
