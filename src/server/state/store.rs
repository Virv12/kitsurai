//! Exposes methods to set and get for a given table and key,
//!  also supports listing keys in a table.

use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{os::unix::ffi::OsStringExt, path::PathBuf, sync::OnceLock};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Where the `sqlite` database should be stored.
/// Initialized by [init].
static STORE_PATH: OnceLock<PathBuf> = OnceLock::new();

fn store_path() -> &'static PathBuf {
    STORE_PATH.get().expect("Store path not initialized")
}

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
pub enum Error {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("Hex decoding error: {0}")]
    Hex(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl From<hex::FromHexError> for Error {
    fn from(e: hex::FromHexError) -> Self {
        Error::Hex(e.to_string())
    }
}

pub async fn item_get(table: Uuid, key: &[u8]) -> Result<Option<Bytes>, Error> {
    log::debug!("{table} get");
    let path = store_path().join(table.to_string()).join(hex::encode(key));

    match std::fs::read(&path) {
        Ok(content) => Ok(Some(Bytes::from(content))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_vendor = "apple")]
fn fsync(f: &File) -> std::io::Result<()> {
    use std::os::fd::{AsFd, AsRawFd};

    if unsafe { libc::fsync(f.as_fd().as_raw_fd()) } == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

pub async fn item_set(table: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    log::debug!("{table} set");
    let tmp_path = store_path().join("tmp").join(Uuid::now_v7().to_string());
    let mut file = File::create_new(&tmp_path).await?;
    file.write_all(value).await?;

    #[cfg(not(target_vendor = "apple"))]
    file.sync_all().await?;
    #[cfg(target_vendor = "apple")]
    fsync(&file)?;

    let path = store_path().join(table.to_string());
    std::fs::create_dir_all(&path)?;
    let path = path.join(hex::encode(key));
    tokio::fs::rename(&tmp_path, &path).await?;
    Ok(())
}

pub type KeyValue = (Vec<u8>, Vec<u8>);

pub async fn item_list(table: Uuid) -> Result<Vec<KeyValue>, Error> {
    log::debug!("{table} list");
    let path = store_path().join(table.to_string());
    let mut items = Vec::new();
    let mut iter = match tokio::fs::read_dir(path).await {
        Ok(iter) => iter,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(items);
        }
        Err(e) => return Err(e.into()),
    };
    while let Some(entry) = iter.next_entry().await? {
        let key = hex::decode(entry.file_name().into_vec())?;
        let value = item_get(table, &key).await?.expect("Item should exist");
        items.push((key, value.to_vec()));
    }
    Ok(items)
}

pub async fn table_delete(table: Uuid) -> Result<(), Error> {
    log::debug!("{table} destroy");
    let path = store_path().join(table.to_string());
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
