use anyhow::Result;
use tokio::{signal::unix::SignalKind, try_join};
use tokio_util::sync::CancellationToken;

mod http;

async fn killer(token: CancellationToken) -> Result<()> {
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv() => println!("Received SIGINT."),
        _ = sigterm.recv() => println!("Received SIGTERM."),
    }
    token.cancel();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = CancellationToken::new();

    let http = http::main(token.clone());
    let rpc = kitsurai::listener(token.clone());
    let killer = killer(token.clone());

    try_join!(http, rpc, killer)?;
    Ok(())
}
