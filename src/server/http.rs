//! Implements the client facing HTTP server.

use crate::{
    exec, peer,
    state::{Table, TableParams, TableStatus},
};
use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path},
    http::StatusCode,
    routing::{delete, get, post},
    Form, Router,
};
use kitsurai::codec::Header;
use std::{collections::BTreeMap, io::Write};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Sets up axum's router and starts listening for requests.
pub async fn main<A: ToSocketAddrs>(addr: A, token: CancellationToken) -> Result<()> {
    let app = Router::new()
        .route("/", get(table_list))
        .route("/", post(table_create))
        .route("/{table}", delete(table_delete))
        .route("/{table}", get(item_list))
        .route("/{table}/{*key}", get(item_get))
        .route(
            "/{table}/{*key}",
            post(item_set).layer(DefaultBodyLimit::disable()),
        );

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;
    Ok(())
}

/// Handler for `GET /`<br>
/// Returns all table metadata.
///
/// Internal use only.
async fn table_list() -> (StatusCode, String) {
    match Table::list() {
        Ok(tables) => (
            StatusCode::OK,
            tables
                .into_iter()
                .filter_map(|t| {
                    let id = t.id;
                    let TableStatus::Created(t) = t.status else {
                        return None;
                    };

                    Some(format!(
                        "{} ({}, {}, {}) -> {}",
                        id,
                        t.params.n,
                        t.params.r,
                        t.params.w,
                        t.allocation
                            .into_iter()
                            .map(|((_, i), b)| format!("{b}@{}", peer::peers()[i as usize].addr))
                            .collect::<Vec<String>>()
                            .join(", ")
                    ))
                })
                .collect::<Vec<String>>()
                .join("\n")
                + "\n",
        ),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string() + "\n"),
    }
}

/// Handler for `POST /`<br>
/// Creates a table and returns its id if successful.
/// Expects table parameters in a form string.
async fn table_create(Form(params): Form<TableParams>) -> (StatusCode, String) {
    match exec::table::table_create(params).await {
        Ok(uuid) => (StatusCode::OK, uuid.to_string() + "\n"),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string() + "\n"),
    }
}

/// Handler for `DELETE /{table}`<br>
/// Deletes the table.
async fn table_delete(Path(table): Path<Uuid>) -> (StatusCode, String) {
    match exec::table::table_delete(table).await {
        Ok(()) => (StatusCode::OK, "Table deleted!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

/// Handler for `GET /{table}/{*key}`<br>
/// Read the key's value.
///
/// Returns the first `R` values read if successful, or any read value otherwise.
/// `R` is a parameter that can be configured per table. See [TableParams].
async fn item_get(Path((table, key)): Path<(Uuid, Bytes)>) -> (StatusCode, Vec<u8>) {
    match exec::item::item_get(table, key).await {
        Ok(values) => {
            let lengths: Vec<Option<u64>> = values
                .iter()
                .map(|r| r.as_ref().map(|bytes| bytes.len() as u64))
                .collect();
            match postcard::to_allocvec(&Header { lengths }) {
                Ok(header) => {
                    let mut serialized = header;
                    for value in values.into_iter().flatten() {
                        serialized.extend(value);
                    }

                    (StatusCode::OK, serialized)
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{e}\n").into_bytes(),
                ),
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            format!("{e}\n{}\n", e.backtrace()).into_bytes(),
        ),
    }
}

/// Handler for `POST /{table}/{*key}`<br>
/// Sets the key's value equal to this request's body.
///
/// Returns after `W` successful writes, or reports the failure otherwise.
/// `R` is a parameter that can be configured per table. See [TableParams].
async fn item_set(Path((table, key)): Path<(Uuid, Bytes)>, body: Bytes) -> (StatusCode, String) {
    match exec::item::item_set(table, key, body).await {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

/// Handler for `GET /{table}`<br>
/// Lists this table keys.
async fn item_list(Path(table): Path<Uuid>) -> (StatusCode, Vec<u8>) {
    async fn inner(table: Uuid) -> Result<Vec<u8>> {
        let mut data = exec::item::item_list(table).await?;
        data.sort_by_key(|(peer, _)| peer.clone());

        let mut keys = data
            .iter()
            .flat_map(|(_, keyvalue)| keyvalue.iter().map(|(key, _)| key.clone()))
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();

        let data: Vec<_> = data
            .into_iter()
            .map(|(peer, keyvalue)| {
                let keyvalue: BTreeMap<_, _> = keyvalue.into_iter().collect();
                (peer, keyvalue)
            })
            .collect();

        let mut out = Vec::new();
        for (peer, _) in &data {
            write!(out, "${peer}")?;
        }
        writeln!(out)?;

        for key in keys {
            write!(out, "{key:?}")?;
            for (_, keyvalue) in &data {
                write!(out, "$")?;
                if let Some(value) = keyvalue.get(&key) {
                    write!(out, "{value:?}")?;
                }
            }
            writeln!(out)?;
        }

        for (_, keyvalue) in &data {
            write!(out, "${}", keyvalue.len())?;
        }
        writeln!(out)?;

        Ok(out)
    }

    match inner(table).await {
        Ok(out) => (StatusCode::OK, out),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{e}\n").into_bytes(),
        ),
    }
}
