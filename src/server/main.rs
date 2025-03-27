mod exec;
mod http;
mod merkle;
mod meta;
mod peer;
mod rpc;
mod store;

use crate::exec::Operations;
use anyhow::Result;
use clap::{arg, Parser};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::{signal::unix::SignalKind, try_join};
use tokio_util::sync::CancellationToken;

// Compile-time configuration.
const TIMEOUT: Duration = Duration::from_secs(1);
const PREPARE_TIME: Duration = Duration::from_secs(60);

// Run-time configuration.
pub static BANDWIDTH: AtomicU64 = AtomicU64::new(0);

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

    #[arg(short, long, default_value = "100")]
    bandwidth: u64,
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
    pretty_env_logger::init();

    let Cli {
        http_addr,
        rpc_addr,
        store_cli,
        peer_cli,
        bandwidth,
    } = Cli::parse();

    let listener = TcpListener::bind(rpc_addr).await?;
    peer::init(peer_cli, listener.local_addr()?);
    store::init(store_cli)?;
    BANDWIDTH.store(bandwidth, Ordering::Relaxed);
    meta::init();

    let token = CancellationToken::new();
    let http = http::main(&http_addr, token.clone());
    let rpc = Operations::listener(listener, token.clone());
    let killer = killer(token);
    try_join!(http, rpc, killer)?;
    Ok(())
}
