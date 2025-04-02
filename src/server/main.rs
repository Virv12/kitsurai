mod exec;
mod http;
mod merkle;
mod peer;
mod rpc;
mod state;

use crate::exec::{gossip, Operations};
use anyhow::Result;
use clap::{arg, Parser};
use serde::Serialize;
use std::fs::File;
use std::{io::Write, time::Duration};
use tokio::{net::TcpListener, signal::unix::SignalKind, try_join};
use tokio_metrics::RuntimeMetrics;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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

    #[derive(Serialize)]
    struct NewRuntimeMetrics {
        workers_count: usize,
        total_park_count: u64,
        max_park_count: u64,
        min_park_count: u64,
        mean_poll_duration: u128,
        mean_poll_duration_worker_min: u128,
        mean_poll_duration_worker_max: u128,
        poll_time_histogram: String,
        total_noop_count: u64,
        max_noop_count: u64,
        min_noop_count: u64,
        total_steal_count: u64,
        max_steal_count: u64,
        min_steal_count: u64,
        total_steal_operations: u64,
        max_steal_operations: u64,
        min_steal_operations: u64,
        num_remote_schedules: u64,
        total_local_schedule_count: u64,
        max_local_schedule_count: u64,
        min_local_schedule_count: u64,
        total_overflow_count: u64,
        max_overflow_count: u64,
        min_overflow_count: u64,
        total_polls_count: u64,
        max_polls_count: u64,
        min_polls_count: u64,
        total_busy_duration: u128,
        max_busy_duration: u128,
        min_busy_duration: u128,
        global_queue_depth: usize,
        total_local_queue_depth: usize,
        max_local_queue_depth: usize,
        min_local_queue_depth: usize,
        elapsed: u128,
        budget_forced_yield_count: u64,
        io_driver_ready_count: u64,
    }

    impl From<RuntimeMetrics> for NewRuntimeMetrics {
        fn from(metrics: RuntimeMetrics) -> Self {
            Self {
                workers_count: metrics.workers_count,
                total_park_count: metrics.total_park_count,
                max_park_count: metrics.max_park_count,
                min_park_count: metrics.min_park_count,
                mean_poll_duration: metrics.mean_poll_duration.as_micros(),
                mean_poll_duration_worker_min: metrics.mean_poll_duration_worker_min.as_micros(),
                mean_poll_duration_worker_max: metrics.mean_poll_duration_worker_max.as_micros(),
                poll_time_histogram: "N/A".to_string(),
                total_noop_count: metrics.total_noop_count,
                max_noop_count: metrics.max_noop_count,
                min_noop_count: metrics.min_noop_count,
                total_steal_count: metrics.total_steal_count,
                max_steal_count: metrics.max_steal_count,
                min_steal_count: metrics.min_steal_count,
                total_steal_operations: metrics.total_steal_operations,
                max_steal_operations: metrics.max_steal_operations,
                min_steal_operations: metrics.min_steal_operations,
                num_remote_schedules: metrics.num_remote_schedules,
                total_local_schedule_count: metrics.total_local_schedule_count,
                max_local_schedule_count: metrics.max_local_schedule_count,
                min_local_schedule_count: metrics.min_local_schedule_count,
                total_overflow_count: metrics.total_overflow_count,
                max_overflow_count: metrics.max_overflow_count,
                min_overflow_count: metrics.min_overflow_count,
                total_polls_count: metrics.total_polls_count,
                max_polls_count: metrics.max_polls_count,
                min_polls_count: metrics.min_polls_count,
                total_busy_duration: metrics.total_busy_duration.as_micros(),
                max_busy_duration: metrics.max_busy_duration.as_micros(),
                min_busy_duration: metrics.min_busy_duration.as_micros(),
                global_queue_depth: metrics.global_queue_depth,
                total_local_queue_depth: metrics.total_local_queue_depth,
                max_local_queue_depth: metrics.max_local_queue_depth,
                min_local_queue_depth: metrics.min_local_queue_depth,
                elapsed: metrics.elapsed.as_micros(),
                budget_forced_yield_count: metrics.budget_forced_yield_count,
                io_driver_ready_count: metrics.io_driver_ready_count,
            }
        }
    }

    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);

    // print runtime metrics every 500ms
    let frequency = Duration::from_millis(500);
    let path = format!("shared/{}.csv", Uuid::new_v4());
    log::info!("This instance will log metrics to {path}");
    let mut writer = csv::Writer::from_writer(File::create_new(path)?);
    for metrics in runtime_monitor.intervals() {
        writer.serialize(NewRuntimeMetrics::from(metrics))?;
        writer.flush()?;
        tokio::time::sleep(frequency).await;
        if token.is_cancelled() {
            break;
        }
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
