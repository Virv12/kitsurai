use crate::store;
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

pub(crate) fn create_table(table: Table) -> Result<()> {
    store::item_set(
        META,
        table.id.as_bytes(),
        &postcard::to_allocvec(&table)?,
    )?;
    Ok(())
}

pub(crate) fn get_table(id: Uuid) -> Result<Option<Table>> {
    let tbl = store::item_get(META, id.as_bytes())?;
    let tbl = tbl.map(|bytes| postcard::from_bytes(bytes.as_ref()).unwrap());
    Ok(tbl)
}

pub(crate) fn destroy_table() {
    todo!()
}
