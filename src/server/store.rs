use clap::Parser;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::OnceLock};
use uuid::Uuid;

static STORE_PATH: OnceLock<PathBuf> = OnceLock::new();

#[derive(Debug, Parser)]
pub(crate) struct StoreCli {
    #[arg(long, default_value = "store.db")]
    store_path: PathBuf,
}

thread_local! {
    static SQLITE: rusqlite::Connection = {
        let path = STORE_PATH.get().expect("Store path uninitialized");
        rusqlite::Connection::open(path).expect("Failed to open SQLite database")
    };
}

pub(crate) fn init(cli: StoreCli) -> anyhow::Result<()> {
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
    })?;
    Ok(())
}

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Error {}

pub(crate) fn item_get(table: Uuid, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
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

pub(crate) fn item_set(table: Uuid, key: &[u8], value: &[u8]) -> Result<(), Error> {
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

pub(crate) type KeyValue = (Vec<u8>, Vec<u8>);

pub(crate) fn item_list(table: Uuid) -> Result<Vec<KeyValue>, Error> {
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
