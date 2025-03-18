use anyhow::Result;
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
        .route("/{key}", get(item_get))
        .route("/{key}", post(item_set));

    let listener = TcpListener::bind("0.0.0.0:8000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root() -> &'static str {
    "Hello from Kitsurai!\n"
}

async fn item_get(Path(key): Path<String>) -> (StatusCode, Vec<u8>) {
    match kitsurai::item_get(&key).await {
        Ok(value) if value.len() == 0 => (StatusCode::NOT_FOUND, Vec::new()),
        Ok(value) => (StatusCode::OK, Vec::from(json::stringify(value) + "\n")),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    }
}

async fn item_set(Path(key): Path<String>, body: Bytes) -> (StatusCode, String) {
    match kitsurai::item_set(&key, body.to_vec()).await {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}
