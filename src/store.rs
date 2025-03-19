use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

fn open_connection() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open("store.db").expect("Failed to open SQLite database.");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS store (
            key TEXT PRIMARY KEY,
            value BLOB
        )",
        [],
    )
    .unwrap();
    conn
}

thread_local! {
    static SQLITE: rusqlite::Connection = open_connection();
}

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone)]
pub(crate) enum Error {}

pub(crate) fn item_get(key: &str) -> Result<Option<Vec<u8>>, Error> {
    SQLITE.with(|conn| {
        Ok(conn
            .query_row("SELECT value FROM store WHERE key = ?", (key,), |row| {
                row.get(0)
            })
            .optional()
            .unwrap())
    })
}

pub(crate) fn item_set(key: &str, value: Vec<u8>) -> Result<(), Error> {
    SQLITE.with(|conn| {
        conn.execute(
            "INSERT OR REPLACE INTO store (key, value) VALUES (?, ?)",
            (key, value),
        )
        .unwrap();
    });
    Ok(())
}
