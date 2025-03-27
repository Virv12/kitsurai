use crate::{merkle::Merkle, peer, peer::local_index, peer::Peer, store, BANDWIDTH};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    process::exit,
    sync::{atomic::Ordering, Mutex},
};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TableStatus {
    Prepared { allocated: u64 },
    Created(TableData),
    Deleted,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub(crate) struct TableParams {
    pub(crate) b: u64,
    pub(crate) n: u64,
    pub(crate) r: u64,
    pub(crate) w: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct TableData {
    pub(crate) allocation: BTreeMap<u64, u64>,
    pub(crate) params: TableParams,
}

impl TableData {
    fn virt_to_peer(&self, virt: u64) -> &'static Peer {
        let TableParams { b, n, .. } = self.params;
        let ord = virt / n + virt % n * b;
        let index = self
            .allocation
            .iter()
            .scan(0, |acc, (&k, &v)| {
                *acc += v;
                Some((k, *acc))
            })
            .find(|&(_, acc)| acc > ord)
            .expect("index should be less than sigma")
            .0;

        &peer::peers()[index as usize]
    }
    pub(crate) fn peers_for_key(
        &self,
        key: &[u8],
    ) -> impl Iterator<Item = &'static Peer> + use<'_> {
        let hash = xxh3_64(key);
        let TableParams { b, n, .. } = self.params;
        let virt = ((hash as u128 * (b * n) as u128) >> 64) as u64;
        (0..n).map(move |i| self.virt_to_peer(virt + i))
    }
}

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) id: Uuid,
    pub(crate) status: TableStatus,
}

static LOCK: Mutex<()> = Mutex::new(());
static MERKLE: Mutex<Merkle> = Mutex::new(Merkle::new());

impl Table {
    pub(crate) fn load(id: Uuid) -> Result<Option<Self>> {
        let blob = store::item_get(META, id.as_bytes())?;
        let table = blob.map(|blob| Self {
            id,
            status: postcard::from_bytes(blob.as_ref()).expect("table deserialization failed"),
        });
        Ok(table)
    }

    pub(crate) fn list() -> Result<Vec<Self>> {
        let tables = store::item_list(META)?
            .into_iter()
            .map(|(key, value)| Self {
                id: Uuid::from_slice(&key).expect("id deserialization failed"),
                status: postcard::from_bytes(value.as_ref()).expect("table deserialization failed"),
            })
            .collect();

        Ok(tables)
    }

    pub(crate) fn save(&self) -> Result<()> {
        let _guard = LOCK.lock().expect("poisoned lock");
        let blob = postcard::to_allocvec(&self.status)?;
        store::item_set(META, self.id.as_bytes(), &blob)?;
        if !matches!(self.status, TableStatus::Prepared { .. }) {
            MERKLE
                .lock()
                .expect("poisoned merkle lock")
                .insert(*self.id.as_bytes(), &blob);
        }
        Ok(())
    }

    pub(crate) fn delete_if_prepared(id: Uuid) {
        let _guard = LOCK.lock().expect("poisoned lock");
        let mut table = Table::load(id).ok().flatten().expect("table should exists");

        if let TableStatus::Prepared { allocated } = table.status {
            BANDWIDTH.fetch_add(allocated, Ordering::Relaxed);
            table.status = TableStatus::Deleted;
            if table.save().is_err() {
                exit(1);
            }
        }
    }
}
pub(crate) fn init() {
    for mut table in Table::list().expect("could not get table list") {
        match table.status {
            TableStatus::Prepared { .. } => {
                table.status = TableStatus::Deleted;
                table.save().expect("failed to prepared table");
            }
            TableStatus::Created(ref data) => {
                if let Some(&allocated) = data.allocation.get(&(local_index() as u64)) {
                    BANDWIDTH.fetch_sub(allocated, Ordering::Relaxed);
                }

                let data = postcard::to_allocvec(&table.status).unwrap();
                MERKLE.lock().unwrap().insert(*table.id.as_bytes(), &data);
            }
            TableStatus::Deleted => {
                let data = postcard::to_allocvec(&table.status).unwrap();
                MERKLE.lock().unwrap().insert(*table.id.as_bytes(), &data);
            }
        }
    }
}
