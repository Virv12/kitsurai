use anyhow::Result;
use axum::{
    body::Bytes,
    extract::Path,
    http::StatusCode,
    routing::{delete, get, post},
    Router,
};
use tokio::net::TcpListener;

pub async fn main() -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/table/{table}", post(table_create))
        .route("/table/{table}", delete(table_delete))
        .route("/table/{table}/{key}", get(item_get))
        .route("/table/{table}/{key}", post(item_set))
        .route("/table/{table}/{key}", delete(item_delete));

    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root() -> &'static str {
    "Hello from Kitsurai!\n"
}

async fn table_create(Path(table): Path<String>) -> (StatusCode, String) {
    match kitsurai::table_create(&table) {
        Ok(()) => (StatusCode::CREATED, "Table created!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

async fn table_delete(Path(table): Path<String>) -> (StatusCode, String) {
    match kitsurai::table_delete(&table) {
        Ok(()) => (StatusCode::OK, "Table deleted!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

async fn item_get(Path((table, key)): Path<(String, String)>) -> (StatusCode, Vec<u8>) {
    match kitsurai::item_get(&table, &key) {
        Ok(Some(value)) => (StatusCode::OK, value),
        Ok(None) => (StatusCode::NOT_FOUND, Vec::new()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    }
}

async fn item_set(Path((table, key)): Path<(String, String)>, body: Bytes) -> (StatusCode, String) {
    match kitsurai::item_set(&table, &key, body.to_vec()) {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}

async fn item_delete(Path((table, key)): Path<(String, String)>) -> (StatusCode, String) {
    match kitsurai::item_delete(&table, &key) {
        Ok(()) => (StatusCode::OK, "Item deleted!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}
