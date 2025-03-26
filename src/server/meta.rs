use crate::meta::TableStatus::{Deleted, Prepared};
use crate::peer::{Peer, PEERS, SELF_INDEX};
use crate::{store, BANDWIDTH, PREPARE_TIME, REPLICATION};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use tokio::time::sleep;
use uuid::Uuid;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TableStatus {
    Prepared,
    Created,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) id: Uuid,
    pub(crate) status: TableStatus,

    /// peer's idx and peer's bandwidth
    pub(crate) peers: Vec<(u64, u64)>,
    pub(crate) n: u64,
    pub(crate) r: u64,
    pub(crate) w: u64,
}

impl Table {
    pub fn peers_for_key(&self, key: &[u8]) -> impl Iterator<Item = &'static Peer> + use<'_> {
        let peers = PEERS.get().expect("peers uninitialized");
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let idx = ((hash as u128 * self.peers.len() as u128) >> 64) as usize;
        self.peers
            .iter()
            .cycle()
            .skip(idx)
            .take(REPLICATION)
            .map(|(i, _)| &peers[*i as usize])
    }
}

pub fn cleanup_tables() -> Result<()> {
    let self_index = *SELF_INDEX.get().expect("self uninitialized") as u64;

    for (key, value) in store::item_list(META)? {
        let table = postcard::from_bytes::<Table>(&value);
        match table {
            Ok(mut table) if table.status == TableStatus::Prepared => {
                table.status = TableStatus::Deleted;
                store::item_set(META, &key, &postcard::to_allocvec(&table)?)?;
            }
            Ok(table) => {
                let allocated = table
                    .peers
                    .iter()
                    .filter_map(|(i, b)| if *i == self_index { Some(*b) } else { None })
                    .next();
                if let Some(allocated) = allocated {
                    BANDWIDTH.fetch_sub(allocated, Ordering::AcqRel);
                }
            }
            Err(_err) => {
                // TODO: set table as deleted? but we do not have its id...
                todo!()
            }
        };
    }
    Ok(())
}

static LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn get_table(id: Uuid) -> Result<Option<Table>> {
    Ok(match store::item_get(META, id.as_bytes())? {
        Some(blob) => Some(postcard::from_bytes(blob.as_ref())?),
        None => None,
    })
}

async fn allocation_timeout(id: Uuid) {
    sleep(PREPARE_TIME).await;

    let _guard = LOCK.lock().expect("poisoned lock");

    if let Ok(Some(mut table)) = get_table(id) {
        if table.status == Prepared {
            let self_index = *SELF_INDEX.get().expect("self uninitialized") as u64;
            let allocated = table
                .peers
                .iter()
                .find_map(|(i, b)| if *i == self_index { Some(*b) } else { None })
                // TODO: how can we get here?
                .expect("self not in table peers");

            BANDWIDTH.fetch_add(allocated, Ordering::Relaxed);

            table.status = Deleted;
            set_table(&table)
                // TODO: how could we get here?
                .expect("could not set table");
        }
    } else {
        panic!("table should exists");
    }
}

pub(crate) fn set_table(table: &Table) -> Result<()> {
    let _guard = LOCK.lock().expect("poisoned lock");

    use TableStatus::*;
    let old_status = get_table(table.id)?.map(|table: Table| table.status);

    match (old_status, &table.status) {
        // Prepared
        (None, Prepared) => {
            store::item_set(META, table.id.as_bytes(), &postcard::to_allocvec(table)?)?;
            tokio::spawn(allocation_timeout(table.id));
        }
        (Some(Prepared), Prepared) => todo!(),
        (Some(Created), Prepared) => todo!(),
        (Some(Deleted), Prepared) => todo!(),

        // Created
        (None, Created) => {
            store::item_set(META, table.id.as_bytes(), &postcard::to_allocvec(table)?)?;
        }
        (Some(Prepared), Created) => {
            store::item_set(META, table.id.as_bytes(), &postcard::to_allocvec(table)?)?;
        }
        (Some(Created), Created) => todo!(),
        (Some(Deleted), Created) => todo!(),

        // Deleted
        (None, Deleted) | (Some(Prepared), Deleted) | (Some(Created), Deleted) => {
            store::item_set(META, table.id.as_bytes(), &postcard::to_allocvec(table)?)?;
        }
        (Some(Deleted), Deleted) => todo!(),
    };

    Ok(())
}

pub(crate) fn list_tables() -> Result<Vec<Table>> {
    // ignore decode errors.
    Ok(store::item_list(META)?
        .into_iter()
        .flat_map(|(_, value)| postcard::from_bytes(value.as_ref()))
        .collect())
}

#[allow(dead_code)]
pub(crate) fn destroy_table() {
    todo!()
}
