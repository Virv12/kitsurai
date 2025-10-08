use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::sync::Arc;
use std::{fs::File, io::Write, time::Duration};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::Instant;

type Client = SendRequest<Full<Bytes>>;
type Request<T = Full<Bytes>> = http::Request<T>;
type RequestFn = Arc<dyn Fn(usize) -> Request + Send + Sync>;

type Res = Result<f64, ()>;

async fn exec_req(mut client: Client, req: Request) -> Res {
    let start_time = Instant::now();
    let res = client.send_request(req).await.map_err(|e| {
        eprintln!("Request failed: {e:?}");
    })?;
    let status = res.status();
    let mut body = res.into_body().map_err(|e| {
        eprintln!("Failed to read response body: {e}");
    });
    while let Some(chunk) = body.frame().await {
        chunk.map_err(|e| {
            eprintln!("Failed to read response body chunk: {e:?}");
        })?;
    }
    let elapsed = start_time.elapsed();
    if !status.is_success() {
        return Err(());
    }
    if elapsed > Duration::from_secs(5) {
        eprintln!("Request took too long: {:?}", elapsed);
        return Err(());
    }
    Ok(elapsed.as_secs_f64() * 1000.)
}

async fn run_fstress(server: &str, reqs: RequestFn, period_us: u64) {
    let mut set = JoinSet::new();
    let period = Duration::from_micros(period_us);
    let mut interval = tokio::time::interval(period);

    let mut clients = Vec::new();
    for _ in 0..20 {
        clients.push(get_conn(server).await.unwrap());
    }

    for i in 0.. {
        while set.try_join_next().is_some() {}

        let client = clients[i % clients.len()].clone();
        let req = reqs(i);
        interval.tick().await;
        set.spawn(exec_req(client, req));
    }

    set.join_all().await;
}

async fn run_lstress(server: &str, reqs: RequestFn, tasks: u64) {
    let mut set = JoinSet::new();
    let mut clients = Vec::new();
    for _ in 0..20 {
        clients.push(get_conn(server).await.unwrap());
    }

    for idx in 0..tasks as usize {
        let reqs = reqs.clone();
        let client = clients[idx % clients.len()].clone();
        set.spawn(async move {
            loop {
                let req = reqs(idx);
                let _ = exec_req(client.clone(), req).await;
            }
        });
    }

    set.join_all().await;
}

async fn bench_max(name: &str, server: &str, reqs: RequestFn, tasks: u64) -> Result<()> {
    let mut clients = Vec::new();
    for _ in 0..20 {
        clients.push(get_conn(server).await.unwrap());
    }

    let start_at = Instant::now() + Duration::from_millis(10);
    let end_at = start_at + Duration::from_secs(20);

    let mut set = JoinSet::new();
    for task_idx in 0..tasks as usize {
        let reqs = reqs.clone();
        let client = clients[task_idx % clients.len()].clone();
        set.spawn(async move {
            tokio::time::sleep_until(start_at).await;
            let mut idx = 0;
            let mut res = Vec::new();
            let mut bad_count = 0;
            while Instant::now() < end_at {
                let req = reqs(idx * tasks as usize + task_idx);
                let r = exec_req(client.clone(), req).await;
                match r {
                    Ok(r) => res.push(r),
                    Err(()) => {
                        bad_count += 1;
                        if bad_count >= 10 {
                            break;
                        }
                    }
                }
                idx += 1;
            }
            (res, bad_count)
        });
    }

    let mut res = Vec::new();
    let mut bad_count = 0;
    while let Some(r) = set.join_next().await {
        let (mut r, bc) = r.unwrap();
        bad_count += bc;
        res.append(&mut r);
    }

    let good_count = res.len();
    let count_req = good_count + bad_count;
    let sum: f64 = res.iter().sum();
    let avg = sum / good_count as f64;
    let mdev = res.iter().map(|x| (x - avg).abs()).sum::<f64>() / good_count as f64;
    let p0 = res.first().unwrap();
    let p50 = res[good_count / 2];
    let p90 = res[good_count * 9 / 10];
    let p99 = res[good_count * 99 / 100];
    let p999 = res[good_count * 999 / 1000];
    let p100 = res.last().unwrap();

    println!(
        "[{name}] req/s: {} count: {count_req}, bad: {bad_count}, avg: {avg:.3}, mdev: {mdev:.3}, p0: {p0:.3}, p50: {p50:.3}, p90: {p90:.3}, p99: {p99:.3}, p99.9: {p999:.3}, p100: {p100:.3}",
        count_req as f64 / 20.,
    );

    Ok(())
}

async fn bench(
    name: &str,
    server: &str,
    reqs: RequestFn,
    period: Duration,
    time: Duration,
    out: &mut dyn Write,
) -> bool {
    let count_req = (time.as_secs_f64() / period.as_secs_f64()) as usize;

    let mut clients = Vec::new();
    for _ in 0..20 {
        clients.push(get_conn(server).await.unwrap());
    }

    let mut set = JoinSet::new();
    let mut res = Vec::new();
    let mut bad_count = 0;
    let mut interval = tokio::time::interval(period);
    for idx in 0..count_req {
        while let Some(r) = set.try_join_next() {
            match r.unwrap() {
                Ok(r) => res.push(r),
                Err(()) => {
                    bad_count += 1;
                    if bad_count >= 10 {
                        return false;
                    }
                }
            }
        }

        let client = clients[idx % clients.len()].clone();
        let req = reqs(idx);

        interval.tick().await;
        set.spawn(exec_req(client, req));

        if (idx + 1).count_ones() == 1 {
            eprintln!("[{}] {} / {} / {}", name, res.len(), idx + 1, count_req);
        }
    }

    while let Some(r) = set.join_next().await {
        match r.unwrap() {
            Ok(r) => res.push(r),
            Err(()) => {
                bad_count += 1;
                if bad_count >= 10 {
                    return false;
                }
            }
        }
    }

    res.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let good_count = res.len();
    let sum: f64 = res.iter().sum();
    let avg = sum / good_count as f64;
    let mdev = res.iter().map(|x| (x - avg).abs()).sum::<f64>() / good_count as f64;
    let p0 = res.first().unwrap();
    let p50 = res[good_count / 2];
    let p90 = res[good_count * 9 / 10];
    let p99 = res[good_count * 99 / 100];
    let p999 = res[good_count * 999 / 1000];
    let p100 = res.last().unwrap();

    println!(
        "[{name}] count: {count_req}, bad: {bad_count}, avg: {avg:.3}, mdev: {mdev:.3}, p0: {p0:.3}, p50: {p50:.3}, p90: {p90:.3}, p99: {p99:.3}, p99.9: {p999:.3}, p100: {p100:.3}",
    );
    writeln!(
        out,
        "{per},{count_req},{bad_count},{avg},{mdev},{p0},{p50},{p90},{p99},{p999},{p100}",
        per = period.as_secs_f64() * 1000.
    )
    .unwrap();

    true
}

async fn test(name: &str, server: &str, reqs: RequestFn, graph: u64) {
    let mut out = File::create(format!("bench-{name}.csv")).unwrap();
    writeln!(
        out,
        "period,count,bad_count,avg,mdev,p0,p50,p90,p99,p999,p100"
    )
    .unwrap();

    //const SYNC_S: u64 = 30;

    let mut period_us = graph;
    loop {
        //let unix = UNIX_EPOCH.elapsed().unwrap().as_secs();
        //let unix_start = unix.next_multiple_of(SYNC_S);
        //let unix_diff = UNIX_EPOCH
        //    .add(Duration::from_secs(unix_start))
        //    .duration_since(SystemTime::now())
        //    .unwrap();
        //println!("Starting in {:?}", unix_diff);
        //tokio::time::sleep(unix_diff).await;

        let period = Duration::from_micros(period_us);
        let good = bench(
            &format!("{name}-{:.3}ms", period_us as f64 / 1000.),
            server,
            reqs.clone(),
            period,
            Duration::from_secs(20),
            &mut out,
        )
        .await;

        if !good {
            break;
        }
        period_us = (period_us as f64 / 1.1) as u64;
    }
}

#[derive(Parser)]
struct Args {
    #[clap(short, long, default_value = "localhost:8000")]
    server: String,

    table: String,
    key: String,

    action: Action,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    FreqStress { freq: u64 },
    LatStress { tasks: u64 },
    Graph { start_period: u64 },
    Max { tasks: u64 },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Action {
    Get,
    Set,
}

async fn get_conn(peer: &str) -> Result<Client> {
    let stream = TcpStream::connect(peer).await?;
    let io = TokioIo::new(stream);

    // Perform an HTTP/2 handshake over the TCP connection
    let (sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;

    // Spawn the connection driver (handles incoming frames)
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("Connection error: {:?}", err);
        }
    });

    Ok(sender)
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        server,
        table,
        key,
        action,
        command,
    } = Args::parse();

    let reqs: RequestFn = match action {
        Action::Get => Arc::new(move |i| {
            let i = i % 1000;
            Request::get(format!("/{table}/{key}-{i}"))
                .body(Full::new(Bytes::new()))
                .unwrap()
        }),
        Action::Set => Arc::new(move |i| {
            let i = i % 1000;
            Request::post(format!("/{table}/{key}-{i}"))
                .body(Full::new(Bytes::from(vec![0u8; 4096])))
                .unwrap()
        }),
    };

    match command {
        Command::FreqStress { freq } => {
            run_fstress(&server, reqs, freq).await;
        }
        Command::LatStress { tasks } => {
            run_lstress(&server, reqs, tasks).await;
        }
        Command::Graph { start_period } => {
            let name = match action {
                Action::Get => "get",
                Action::Set => "set",
            };
            test(name, &server, reqs, start_period).await;
        }
        Command::Max { tasks } => {
            let name = match action {
                Action::Get => "get",
                Action::Set => "set",
            };
            bench_max(name, &server, reqs, tasks).await?;
        }
    }

    Ok(())
}
