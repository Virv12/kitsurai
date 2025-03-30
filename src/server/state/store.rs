//! Exposes methods to set and get for a given table and key,
//!  also supports listing keys in a table.

use clap::Parser;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::OnceLock};
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

thread_local! {
    /// This threads sqlite connection.
    ///
    /// TODO: Every thread has its own connection to allow request parallelization?
    static SQLITE: rusqlite::Connection = {
        let path = STORE_PATH.get().expect("Store path uninitialized");
        rusqlite::Connection::open(path).expect("Failed to open SQLite database")
    };
}

/// Initializes the storage global state as specified in the configuration.
///
/// Creates the sqlite table if it does not exist.
pub fn init(cli: StoreCli) {
    log::info!("Initialize store at {}", cli.store_path.display());
    STORE_PATH
        .set(cli.store_path)
        .expect("Store path already initialized");
    SQLITE.with(|conn| {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS store (
                tbl BLOB,
                key BLOB,
                value BLOB,
                PRIMARY KEY (tbl, key)
            )",
            [],
        )
        .expect("Failed to create table");
    });
}

/// Errors that can be returned by this module's methods.
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub enum Error {}

pub fn item_get(table: Uuid, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    log::debug!("{table} get");
    SQLITE.with(|conn| {
        Ok(conn
            .query_row(
                "SELECT value FROM store WHERE (tbl, key) = (?, ?)",
                (table.as_bytes(), key),
                |row| row.get(0),
            )
            .optional()
            .unwrap())
    })
}

pub fn item_set(table: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
    log::debug!("{table} set");
    SQLITE.with(|conn| {
        conn.execute(
            "INSERT OR REPLACE INTO store (tbl, key, value) VALUES (?, ?, ?)",
            (table.as_bytes(), key, value),
        )
        .unwrap();
    });
    Ok(())
}

pub type KeyValue = (Vec<u8>, Vec<u8>);

pub fn item_list(table: Uuid) -> Result<Vec<KeyValue>, Error> {
    log::debug!("{table} list");
    SQLITE.with(|conn| {
        let mut stmt = conn
            .prepare("SELECT key, value FROM store where tbl = ?")
            .expect("Failed to prepare statement");

        let rows = stmt
            .query_map([table.as_bytes()], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0).unwrap(),
                    row.get::<_, Vec<u8>>(1).unwrap(),
                ))
            })
            .unwrap();

        Ok(rows.map(Result::unwrap).collect())
    })
}

pub fn table_delete(table: Uuid) -> Result<(), Error> {
    log::debug!("{table} destroy");
    SQLITE.with(|conn| {
        conn.execute("DELETE FROM store WHERE tbl = ?", [table.as_bytes()])
            .unwrap();
    });
    Ok(())
}
