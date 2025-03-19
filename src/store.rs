use bytes::Bytes;
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

pub(crate) fn item_get(key: Bytes) -> Result<Option<Bytes>, Error> {
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
            .expect("Failed to prepare statement.");
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
