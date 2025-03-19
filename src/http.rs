use anyhow::Result;
use axum::{
    body::Bytes,
    extract::Path,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub async fn main(token: CancellationToken) -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/{*key}", get(item_get))
        .route("/{*key}", post(item_set));

    let listener = TcpListener::bind("0.0.0.0:8000").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;
    Ok(())
}

async fn root() -> &'static str {
    "Hello from Kitsurai!\n"
}

struct BytesFormatter;

impl serde_json::ser::Formatter for BytesFormatter {
    fn write_byte_array<W>(&mut self, writer: &mut W, mut value: &[u8]) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b"\"")?;
        while !value.is_empty() {
            let valid_up_to = match std::str::from_utf8(value) {
                Ok(valid) => valid.len(),
                Err(utf8_error) => utf8_error.valid_up_to(),
            };
            let (valid, rest) = value.split_at(valid_up_to);
            for byte in valid {
                match byte {
                    0x08 => writer.write_all(b"\\b"),
                    b'\t' => writer.write_all(b"\\t"),
                    b'\n' => writer.write_all(b"\\n"),
                    0x0C => writer.write_all(b"\\f"),
                    b'\r' => writer.write_all(b"\\r"),
                    b'"' => writer.write_all(b"\\\""),
                    b'\\' => writer.write_all(b"\\\\"),
                    ctrl @ ..=0x1F => write!(writer, "\\u{ctrl:04x}"),
                    _ => writer.write_all(std::slice::from_ref(byte)),
                }?;
            }
            if let Some((invalid, rest)) = rest.split_first() {
                write!(writer, "\\u{invalid:04x}")?;
                value = rest;
            } else {
                break;
            }
        }
        writer.write_all(b"\"")
    }
}

async fn item_get(Path(key): Path<String>) -> (StatusCode, Vec<u8>) {
    match kitsurai::item_get(&key).await {
        Ok(value) => {
            let value: Vec<_> = value
                .into_iter()
                .map(|read| read.map(serde_bytes::ByteBuf::from))
                .collect();
            let mut body = Vec::new();
            let mut ser = serde_json::Serializer::with_formatter(&mut body, BytesFormatter);
            value.serialize(&mut ser).unwrap();
            body.push(b'\n');
            // TODO: missing content-type
            (StatusCode::OK, body)
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n").into_bytes()),
    }
}

async fn item_set(Path(key): Path<String>, body: Bytes) -> (StatusCode, String) {
    match kitsurai::item_set(&key, body).await {
        Ok(()) => (StatusCode::CREATED, "Item set!\n".to_string()),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")),
    }
}
