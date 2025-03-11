use anyhow::Result;

mod http;

#[tokio::main]
async fn main() -> Result<()> {
    http::main().await?;
    Ok(())
}
