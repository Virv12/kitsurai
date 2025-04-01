//! Exposes methods to set and get for a given table and key,
//!  also supports listing keys in a table.

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, sync::LazyLock};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Storage configuration.
#[derive(Debug, Parser)]
pub struct StoreCli {
    #[arg(long, default_value = "store.db")]
    store_path: PathBuf,
}

static DATA: LazyLock<RwLock<BTreeMap<(Uuid, Vec<u8>), Vec<u8>>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// Initializes the storage global state as specified in the configuration.
///
/// Creates the sqlite table if it does not exist.
pub fn init(cli: StoreCli) {
    log::info!("Initialize store at {}", cli.store_path.display());
}

/// Errors that can be returned by this module's methods.
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub enum Error {}

pub async fn item_get(table: Uuid, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    log::debug!("{table} get");
    Ok(DATA.read().await.get(&(table, key.to_owned())).cloned())
}

pub async fn item_set(table: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    log::debug!("{table} set");
    DATA.write()
        .await
        .insert((table, key.to_owned()), value.to_owned());
    Ok(())
}

pub type KeyValue = (Vec<u8>, Vec<u8>);

pub async fn item_list(table: Uuid) -> Result<Vec<KeyValue>, Error> {
    log::debug!("{table} list");
    Ok(DATA
        .read()
        .await
        .iter()
        .filter_map(|((tbl, key), value)| {
            if *tbl == table {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect())
}

pub async fn table_delete(table: Uuid) -> Result<(), Error> {
    log::debug!("{table} destroy");
    DATA.write().await.retain(|(tbl, _), _| *tbl != table);
    Ok(())
}
