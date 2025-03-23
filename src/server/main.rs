mod exec;
mod http;
mod peer;
mod rpc;
mod store;

use anyhow::Result;
use clap::{arg, Parser};
use std::time::Duration;
use tokio::{signal::unix::SignalKind, try_join};
use tokio_util::sync::CancellationToken;

// Compile-time configuration.
const REPLICATION: u64 = 3;
const NECESSARY_READ: u64 = 2;
const NECESSARY_WRITE: u64 = 2;
const TIMEOUT: Duration = Duration::from_secs(1);

// Run-time configuration.
#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:8000")]
    http_addr: String,

    #[arg(long, default_value = "0.0.0.0:3000")]
    rpc_addr: String,

    #[clap(flatten)]
    store_cli: store::StoreCli,

    #[clap(flatten)]
    peer_cli: peer::PeerCli,
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
        rpc_addr,
        store_cli,
        peer_cli,
    } = Cli::parse();

    store::init(store_cli)?;
    peer::init(peer_cli, &rpc_addr)?;

    let token = CancellationToken::new();
    let http = http::main(&http_addr, token.clone());
    let rpc = exec::listener(rpc_addr, token.clone());
    let killer = killer(token);
    try_join!(http, rpc, killer)?;
    Ok(())
}
