mod exec;
mod http;
mod merkle;
mod meta;
mod peer;
mod rpc;
mod store;

use crate::exec::{gossip, Operations};
use anyhow::Result;
use clap::{arg, Parser};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io::Write, time::Duration};
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
        _ = sigint.recv() => log::info!("Received SIGINT."),
        _ = sigterm.recv() => log::info!("Received SIGTERM."),
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
        bandwidth,
    } = Cli::parse();

    let rpc_addr2 = rpc_addr.clone();
    env_logger::Builder::from_default_env()
        .format(move |buf, record| {
            let s_addr = anstyle::Style::new().dimmed();

            let s_lvl = buf.default_level_style(record.level());
            let lvl = record.level();

            let s_tgt = anstyle::Style::new().bold();
            let tgt = record.target();

            let args = record.args();

            writeln!(
                buf,
                "{s_addr}{rpc_addr2}{s_addr:#} {s_lvl}{lvl:5}{s_lvl:#} {s_tgt}{tgt:17}{s_tgt:#} | {args}"
            )
        })
        .init();

    let listener = TcpListener::bind(rpc_addr).await?;
    peer::init(peer_cli, listener.local_addr()?);
    store::init(store_cli)?;
    BANDWIDTH.store(bandwidth, Ordering::Relaxed);
    meta::init();

    let token = CancellationToken::new();
    let http = http::main(&http_addr, token.clone());
    let rpc = Operations::listener(listener, token.clone());
    let gossip = gossip::gossip(token.clone());
    let killer = killer(token);
    try_join!(http, rpc, gossip, killer)?;
    Ok(())
}
