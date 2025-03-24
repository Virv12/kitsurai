use crate::exec;
use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::{
    body::Bytes,
    extract::Path,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use kitsurai::codec::Header;
use std::str::FromStr;
use std::{collections::BTreeMap, io::Write};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;

pub async fn main<A: ToSocketAddrs>(addr: A, token: CancellationToken) -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/visualizer", get(visualizer))
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

async fn root() -> &'static str {
    "Hello from Kitsurai!\n"
}

async fn item_get(Path(table): Path<String>, Path(key): Path<String>) -> (StatusCode, Vec<u8>) {
    let table = uuid::Uuid::from_str(&table).expect("");
    match exec::item_get(table, Bytes::from(key)).await {
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
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    }
}

async fn item_set(Path(key): Path<String>, body: Bytes) -> (StatusCode, String) {
    match exec::item_set(Bytes::from(key), body).await {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

async fn visualizer() -> (StatusCode, Vec<u8>) {
    async fn inner() -> Result<Vec<u8>> {
        let mut data = exec::visualizer_data().await?;
        data.sort_by_key(|(peer, _)| *peer);

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
        for &(peer, _) in &data {
            write!(out, "${}", peer.ip())?;
        }
        writeln!(out)?;

        for key in keys {
            write!(out, "{:?}", key)?;
            for (_, keyvalue) in &data {
                write!(out, "$")?;
                if let Some(value) = keyvalue.get(&key) {
                    write!(out, "{:?}", value)?;
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

    match inner().await {
        Ok(out) => (StatusCode::OK, out),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{e}\n").into_bytes(),
        ),
    }
}
