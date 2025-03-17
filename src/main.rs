use anyhow::Result;
use tokio::try_join;

mod http;

#[tokio::main]
async fn main() -> Result<()> {
    try_join!(http::main(), kitsurai::listener("0.0.0.0:3000"))?;
    Ok(())
}
