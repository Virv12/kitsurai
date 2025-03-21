mod exec;
mod http;
mod peer;
mod rpc;
mod store;

use anyhow::Result;
use clap::{arg, Parser};
use peer::PeerDiscoveryStrategy;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::signal::unix::SignalKind;
use tokio::try_join;
use tokio_util::sync::CancellationToken;

// Compile-time configuration.
static REPLICATION: u64 = 3;
static NECESSARY_READ: u64 = 2;
static NECESSARY_WRITE: u64 = 2;
static TIMEOUT: Duration = Duration::from_secs(1);
static STORE_PATH: OnceLock<PathBuf> = OnceLock::new();

// Run-time configuration.
#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:8000")]
    http_addr: String,

    #[arg(long, default_value = "0.0.0.0:3000")]
    peer_addr: String,

    #[arg(long, default_value = "store.db")]
    store_path: PathBuf,

    #[arg(long, requires = "discovery_value", default_value = "dns")]
    discovery_strategy: PeerDiscoveryStrategy,

    #[arg(long, requires = "discovery_strategy", default_value = "kitsurai:3000")]
    discovery_value: String,
}

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
    let Cli {
        http_addr,
        peer_addr,
        store_path,
        discovery_strategy,
        discovery_value,
    } = Cli::parse();

    // Initialize storage.
    STORE_PATH
        .set(store_path)
        .expect("Store path already initialized");

    // Initialize peers.
    discovery_strategy.initialize(&peer_addr, &discovery_value)?;

    let token = CancellationToken::new();
    let http = http::main(&http_addr, token.clone());
    let rpc = exec::listener(&peer_addr, token.clone());
    let killer = killer(token.clone());
    try_join!(http, rpc, killer)?;
    Ok(())
}
