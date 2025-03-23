use bytes::Bytes;
use clap::Parser;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::OnceLock};

static STORE_PATH: OnceLock<PathBuf> = OnceLock::new();

thread_local! {
    static SQLITE: rusqlite::Connection = {
        let path = STORE_PATH.get().expect("Store path uninitialized");
        rusqlite::Connection::open(path).expect("Failed to open SQLite database")
    };
}

#[derive(Debug, Parser)]
pub(crate) struct StoreCli {
    #[arg(long, default_value = "store.db")]
    store_path: PathBuf,
}

pub(crate) fn init(cli: StoreCli) -> anyhow::Result<()> {
    STORE_PATH
        .set(cli.store_path)
        .expect("Store path already initialized");

    SQLITE.with(|conn| {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS store (
                key TEXT PRIMARY KEY,
                value BLOB
            )",
            [],
        )
    })?;

    Ok(())
}

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Error {}

pub(crate) fn item_get(key: Bytes) -> Result<Option<Bytes>, Error> {
    eprintln!("STORE: get {key:?}");
    SQLITE.with(|conn| {
        Ok(conn
            .query_row(
                "SELECT value FROM store WHERE key = ?",
                (&key[..],),
                |row| row.get(0).map(|v: Vec<u8>| Bytes::from(v)),
            )
            .optional()
            .unwrap())
    })
}

pub(crate) fn item_set(key: Bytes, value: Bytes) -> Result<(), Error> {
    eprintln!("STORE: set {key:?}");
    SQLITE.with(|conn| {
        conn.execute(
            "INSERT OR REPLACE INTO store (key, value) VALUES (?, ?)",
            (&key[..], &value[..]),
        )
        .unwrap();
    });
    Ok(())
}

pub(crate) fn item_list() -> Result<Vec<(Bytes, Bytes)>, Error> {
    SQLITE.with(|conn| {
        let mut stmt = conn
            .prepare("SELECT key, value FROM store")
            .expect("Failed to prepare statement");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    Bytes::from(row.get::<_, Vec<u8>>(0).unwrap()),
                    Bytes::from(row.get::<_, Vec<u8>>(1).unwrap()),
                ))
            })
            .unwrap();
        Ok(rows.map(Result::unwrap).collect())
    })
}
