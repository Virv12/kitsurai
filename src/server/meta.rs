use crate::peer::{Peer, PEERS};
use crate::{store, BANDWIDTH, REPLICATION};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use uuid::Uuid;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TableStatus {
    Unknown,
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
    let self_index = PEERS
        .get()
        .context("peers uninitialized")?
        .iter()
        .enumerate()
        .filter_map(|(i, p)| if p.is_self { Some(i) } else { None })
        .next()
        .context("self if not in peers list")? as u64;

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

pub(crate) fn set_table(table: &Table) -> Result<()> {
    store::item_set(META, table.id.as_bytes(), &postcard::to_allocvec(table)?)?;
    Ok(())
}

pub(crate) fn get_table(id: Uuid) -> Result<Option<Table>> {
    Ok(match store::item_get(META, id.as_bytes())? {
        Some(blob) => Some(postcard::from_bytes(blob.as_ref())?),
        None => None,
    })
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
