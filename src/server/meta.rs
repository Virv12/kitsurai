use crate::peer::{Peer, PEERS};
use crate::{store, REPLICATION};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const META: Uuid = Uuid::new_v8(*b"kitsuraimetadata");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Table {
    pub(crate) id: Uuid,

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

pub(crate) fn create_table(table: &Table) -> Result<()> {
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
