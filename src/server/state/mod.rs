//! Handles table metadata, exposing methods to load, save and list metadata.

mod store;

use crate::{
    merkle::{self, Merkle},
    peer::{self, availability_zone, local_index, Peer},
};
use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex, RwLock,
    },
};
use store::KeyValue;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Serialize, Deserialize)]
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

    pub fn peers_for_key(&self, key: &[u8]) -> impl Iterator<Item = &'static Peer> + use<'_> {
        let hash = xxh3_64(key);
        let TableParams { b, n, .. } = self.params;
        let virt = ((hash as u128 * (b * n) as u128) >> 64) as u64;
        (0..n).map(move |i| self.virt_to_peer(virt + i))
    }
}

static LOCK: RwLock<()> = RwLock::new(());
static MERKLE: Mutex<Merkle> = Mutex::new(Merkle::new());

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub id: Uuid,
    pub status: TableStatus,
}

impl Table {
    pub fn load(id: Uuid) -> Result<Option<Self>> {
        log::debug!("{id} load");
        let blob = store::item_get(META, id.as_bytes())?;
        let table = blob.map(|blob| Self {
            id,
            status: postcard::from_bytes(blob.as_ref()).expect("table deserialization failed"),
        });
        Ok(table)
    }

    fn save_inner(&self) -> Result<Option<Table>> {
        let old = Table::load(self.id)?;
        let blob = postcard::to_allocvec(&self.status)?;
        store::item_set(META, self.id.as_bytes(), &blob)?;
        if !matches!(self.status, TableStatus::Prepared { .. }) {
            MERKLE
                .lock()
                .expect("poisoned merkle lock")
                .insert(self.id.to_u128_le(), &blob);
        }
        Ok(old)
    }

    pub fn list() -> Result<Vec<Self>> {
        log::debug!("list");
        let tables = store::item_list(META)?
            .into_iter()
            .map(|(key, value)| Self {
                id: Uuid::from_slice(&key).expect("id deserialization failed"),
                status: postcard::from_bytes(value.as_ref()).expect("table deserialization failed"),
            })
            .collect();

        Ok(tables)
    }

    pub fn save(&self) -> Result<Option<Table>> {
        log::debug!("{} save", self.id);
        let _guard = LOCK.write().expect("poisoned lock");
        self.save_inner()
    }

    pub fn growing_save(&self) -> Result<Option<Table>> {
        log::debug!("{} checked_save", self.id);
        let _guard = LOCK.write().expect("poisoned lock");
        let old = Table::load(self.id)?;
        if old.as_ref().map_or(0, |o| o.status.age()) >= self.status.age() {
            return Ok(old);
        }
        self.save_inner()
    }

    pub fn delete_if_prepared(id: Uuid) -> Result<()> {
        log::debug!("{id} delete_if_prepared");
        let _guard = LOCK.write().expect("poisoned lock");
        let mut table = Table::load(id).ok().flatten().expect("table should exists");
        if let TableStatus::Prepared { allocated } = table.status {
            table.status = TableStatus::Deleted;
            table.save_inner()?;
            bandwidth_free(allocated);
        }
        Ok(())
    }

    pub fn delete(id: Uuid) -> Result<()> {
        log::debug!("{id} delete");
        let _guard = LOCK.write().expect("poisoned lock");
        let mut table = Table::load(id).ok().flatten().expect("table should exists");
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
        table.save_inner()?;
        bandwidth_free(allocated);
        store::table_delete(id)?;
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

pub fn init(cli: StateCli) {
    // Initialize the store backend.
    store::init(cli.store_cli);

    log::info!("Initialize state");

    // Set the initial bandwidth as given by configuration.
    BANDWIDTH.store(cli.bandwidth as i64, Ordering::Relaxed);

    let mut merkle = MERKLE.lock().expect("poisoned merkle lock");
    for mut table in Table::list().expect("could not get table list") {
        match table.status {
            TableStatus::Prepared { .. } => {
                table.status = TableStatus::Deleted;
                table.save().expect("failed to prepared table");
            }
            TableStatus::Created(ref data) => {
                if let Some(&allocated) = data
                    .allocation
                    .get(&(availability_zone().to_owned(), (local_index() as u64)))
                {
                    BANDWIDTH.fetch_sub(allocated as i64, Ordering::Relaxed);
                }

                let data = postcard::to_allocvec(&table.status).unwrap();
                merkle.insert(table.id.to_u128_le(), &data);
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

pub fn item_get(table_id: Uuid, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    let _guard = LOCK.read().expect("poisoned lock");
    let table = Table::load(table_id).unwrap();
    if !table.is_some_and(|t| matches!(t.status, TableStatus::Created(_))) {
        return Err(Error::TableNotFound);
    }
    Ok(store::item_get(table_id, key)?)
}

pub fn item_set(table_id: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    let _guard = LOCK.read().expect("poisoned lock");
    let table = Table::load(table_id).unwrap();
    if !table.is_some_and(|t| matches!(t.status, TableStatus::Created(_))) {
        return Err(Error::TableNotFound);
    }
    Ok(store::item_set(table_id, key, value)?)
}

pub fn item_list(table_id: Uuid) -> Result<Vec<KeyValue>, Error> {
    let _guard = LOCK.read().expect("poisoned lock");
    let table = Table::load(table_id).unwrap();
    if !table.is_some_and(|t| matches!(t.status, TableStatus::Created(_))) {
        return Err(Error::TableNotFound);
    }
    Ok(store::item_list(table_id)?)
}

pub fn merkle_find(path: merkle::Path) -> Option<(merkle::Path, u128)> {
    MERKLE.lock().expect("poisoned merkle lock").find(path)
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
