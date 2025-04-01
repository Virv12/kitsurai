mod exec;
mod http;
mod merkle;
mod peer;
mod rpc;
mod state;

use crate::exec::{gossip, Operations};
use anyhow::Result;
use clap::{arg, Parser};
use std::{io::Write, time::Duration};
use tokio::{net::TcpListener, signal::unix::SignalKind, try_join};
use tokio_util::sync::CancellationToken;

/// Time that an RPC may take to complete.
///
/// Any RPC with that does not respond completely within this specified time
///  will be counted as a failure.
///
/// Defaults to 1 second.
const TIMEOUT: Duration = Duration::from_secs(1);

/// Time that a stored table can remain in the [Prepared](meta::TableStatus::Prepared) state.
///
/// If it is not commited or deleted explicitly before this timer runs out
///  the table will be automatically deleted.
///
/// Defaults to 60 seconds.
const PREPARE_TIME: Duration = Duration::from_secs(60);

/// `ktd`'s configuration and CLI interface.
#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The address where to bind the HTTP server to.
    #[arg(long, default_value = "0.0.0.0:8000")]
    http_addr: String,

    /// The address where to bind the RPC server to.
    #[arg(long, default_value = "0.0.0.0:3000")]
    rpc_addr: String,

    #[clap(flatten)]
    state_cli: state::StateCli,

    /// Configuration for peer discovery.
    #[clap(flatten)]
    peer_cli: peer::PeerCli,
}

/// Handles signals received by `ktd`, terminating cleanly.
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

/// Initializes the logger.
///
/// The following format is used: `RPC_ADDR LOG_LEVEL CALLEE | MESSAGE`<br>
/// Usually the message will start with the table uuid.
fn logger_init(rpc_addr: String) {
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
                "{s_addr}{rpc_addr}{s_addr:#} {s_lvl}{lvl:5}{s_lvl:#} {s_tgt}{tgt:17}{s_tgt:#} | {args}"
            )
        })
        .init();
}

async fn metrics(token: CancellationToken) -> Result<()> {
    //loop {
    //    tokio::select! {
    //        _ = token.cancelled() => break,
    //        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
    //    }
    //    let handle = tokio::runtime::Handle::current();
    //    let metrics = handle.metrics();
    //    log::info!(
    //        "alive tasks: {}, queued tasks: {}, asdf: {:?}, park count: {:?}, mean poll time: {:?}",
    //        metrics.num_alive_tasks(),
    //        metrics.global_queue_depth(),
    //        (0..12)
    //            .map(|i| metrics.worker_local_queue_depth(i))
    //            .collect::<Vec<_>>(),
    //        (0..12)
    //            .map(|i| metrics.worker_park_count(i))
    //            .collect::<Vec<_>>(),
    //        (0..12)
    //            .map(|i| metrics.worker_mean_poll_time(i))
    //            .collect::<Vec<_>>(),
    //    );
    //}

    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);

    // print runtime metrics every 500ms
    let frequency = std::time::Duration::from_millis(500);
    for metrics in runtime_monitor.intervals() {
        println!("Metrics = {:#?}", metrics);
        tokio::time::sleep(frequency).await;
    }

    Ok(())
}

/// Initializes all modules and starts all servers.
#[tokio::main]
async fn main() -> Result<()> {
    // Parse configuration from the cli.
    let Cli {
        http_addr,
        rpc_addr,
        state_cli,
        peer_cli,
    } = Cli::parse();

    logger_init(rpc_addr.clone());

    // Bind the rpc address early.
    let listener = TcpListener::bind(rpc_addr).await?;
    // Initialize the static peer list, given our rpc address.
    peer::init(peer_cli, listener.local_addr()?);
    // Initialize the table metadata and gossip structures.
    state::init(state_cli).await;

    // Start the HTTP server and RPC server.
    let token = CancellationToken::new();
    let http = http::main(&http_addr, token.clone());
    let rpc = Operations::listener(listener, token.clone());
    let gossip = gossip::gossip(token.clone());
    let metrics = metrics(token.clone());
    let killer = killer(token);
    try_join!(http, rpc, gossip, metrics, killer)?;
    Ok(())
}
