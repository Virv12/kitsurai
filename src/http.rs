use anyhow::Result;
use axum::extract::Query;
use axum::{
    body::Bytes,
    extract::Path,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use tokio::net::TcpListener;

pub async fn main() -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/{*key}", get(item_get))
        .route("/{*key}", post(item_set));

    let listener = TcpListener::bind("0.0.0.0:8000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root() -> &'static str {
    "Hello from Kitsurai!\n"
}

use serde::Deserialize;

#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum Decode {
    #[default]
    Raw,
    Utf8,
}

#[derive(Deserialize, Default)]
struct GetConfig {
    #[serde(default)]
    decode: Decode,
}

async fn item_get(
    Path(key): Path<String>,
    Query(config): Query<GetConfig>,
) -> (StatusCode, Vec<u8>) {
    match kitsurai::item_get(&key).await {
        Ok(value) => {
            let output = match config.decode {
                Decode::Raw => Vec::from(json::stringify(value) + "\n"),
                Decode::Utf8 => {
                    let decoded: Vec<Option<String>> = value
                        .into_iter()
                        .map(|read| read.map(|bytes| String::from(String::from_utf8_lossy(&bytes))))
                        .collect();

                    Vec::from(json::stringify(decoded) + "\n")
                }
            };

            (StatusCode::OK, output)
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    }
}

async fn item_set(Path(key): Path<String>, body: Bytes) -> (StatusCode, String) {
    match kitsurai::item_set(&key, body.to_vec()).await {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}
