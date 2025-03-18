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
        let mut stmt = conn
            .prepare("SELECT value FROM store WHERE key = ?")
            .unwrap();
        let mut rows = stmt.query((key,)).unwrap();
        if let Some(row) = rows.next().unwrap() {
            let value: Vec<u8> = row.get(0).unwrap();
            Ok(Some(value))
        } else {
            Ok(None)
        }
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
