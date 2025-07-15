//! Exposes methods to set and get for a given table and key,
//!  also supports listing keys in a table.

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{os::unix::ffi::OsStringExt, path::PathBuf, sync::OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

/// Where the `sqlite` database should be stored.
/// Initialized by [init].
static STORE_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Storage configuration.
#[derive(Debug, Parser)]
pub struct StoreCli {
    #[arg(long, default_value = "store.db")]
    store_path: PathBuf,
}

/// Initializes the storage global state as specified in the configuration.
///
/// Creates the sqlite table if it does not exist.
pub fn init(cli: StoreCli) {
    log::info!("Initialize store at {}", cli.store_path.display());
    std::fs::create_dir_all(cli.store_path.join("tmp")).expect("Failed to create store directory");
    STORE_PATH
        .set(cli.store_path)
        .expect("Store path already initialized");
}

/// Errors that can be returned by this module's methods.
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub enum Error {}

pub async fn item_get(table: Uuid, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    log::debug!("{table} get");
    let path = STORE_PATH
        .get()
        .expect("Store path not initialized")
        .join(table.to_string())
        .join(hex::encode(key));
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(e) => {
            panic!("Error opening file: {e}");
        }
    };
    let mut content = Vec::new();
    file.read_to_end(&mut content)
        .await
        .expect("Failed to read file");
    Ok(Some(content))
}

pub async fn item_set(table: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    log::debug!("{table} set");
    let tmp_path = STORE_PATH
        .get()
        .expect("Store path not initialized")
        .join("tmp")
        .join(Uuid::now_v7().to_string());
    let mut file = tokio::fs::File::create_new(&tmp_path)
        .await
        .expect("Failed to create file");
    file.write_all(value)
        .await
        .expect("Failed to write to file");
    file.sync_all().await.expect("Failed to sync file");
    let path = STORE_PATH
        .get()
        .expect("Store path not initialized")
        .join(table.to_string());
    std::fs::create_dir_all(&path).expect("Failed to create table directory");
    let path = path.join(hex::encode(key));
    tokio::fs::rename(tmp_path, path)
        .await
        .expect("Failed to rename temporary file");
    Ok(())
}

pub type KeyValue = (Vec<u8>, Vec<u8>);

pub async fn item_list(table: Uuid) -> Result<Vec<KeyValue>, Error> {
    log::debug!("{table} list");
    let path = STORE_PATH
        .get()
        .expect("Store path not initialized")
        .join(table.to_string());
    let mut items = Vec::new();
    let mut iter = match tokio::fs::read_dir(path).await {
        Ok(iter) => iter,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(items);
        }
        Err(e) => {
            panic!("Error reading directory: {e}");
        }
    };
    while let Some(entry) = iter
        .next_entry()
        .await
        .expect("Failed to read directory entry")
    {
        let key = hex::decode(entry.file_name().into_vec()).unwrap();
        let value = item_get(table, &key).await?.expect("Item should exist");
        items.push((key, value));
    }
    Ok(items)
}

pub async fn table_delete(table: Uuid) -> Result<(), Error> {
    log::debug!("{table} destroy");
    let path = STORE_PATH
        .get()
        .expect("Store path not initialized")
        .join(table.to_string());
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            panic!("Error deleting table directory: {e}");
        }
    }

    Ok(())
}
