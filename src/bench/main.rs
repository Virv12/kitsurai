use clap::{Parser, Subcommand};
use reqwest::{Client, Request};
use std::ops::Add;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

type Res = Result<f64, ()>;

async fn exec_req(client: Client, req: Request) -> Res {
    let start_time = Instant::now();
    let res = client.execute(req).await.map_err(|e| {
        eprintln!("Request failed: {e:?}");
    })?;
    let status = res.status();
    let _body = res.bytes().await.map_err(|e| {
        eprintln!("Failed to read response body: {e}");
    })?;
    let end_time = Instant::now();
    if !status.is_success() {
        return Err(());
    }
    Ok(end_time.duration_since(start_time).as_secs_f64() * 1000.)
}

async fn run_fstress(client: Client, reqs: &[Request], period_us: u64) {
    let mut set = JoinSet::new();
    let period = Duration::from_micros(period_us);
    let mut interval = tokio::time::interval(period);

    for i in 0.. {
        while set.try_join_next().is_some() {}

        let req = reqs[i % reqs.len()].try_clone().unwrap();
        interval.tick().await;
        set.spawn(exec_req(client.clone(), req));
    }

    set.join_all().await;
}

async fn run_lstress(client: Client, reqs: &[Request], tasks: u64) {
    let mut set = JoinSet::new();

    for req in reqs.iter().cycle().take(tasks as usize) {
        let req = req.try_clone().unwrap();
        let client = client.clone();
        set.spawn(async move {
            loop {
                let req = req.try_clone().unwrap();
                let _ = exec_req(client.clone(), req).await;
            }
        });
    }

    set.join_all().await;
}

async fn bench(
    name: &str,
    client: Client,
    reqs: &[Request],
    period: Duration,
    time: Duration,
    out: &mut dyn Write,
) -> bool {
    let mut interval = tokio::time::interval(period);
    let mut set = JoinSet::new();
    let mut res = Vec::new();
    let mut bad_count = 0;

    let count_req = (time.as_secs_f64() / period.as_secs_f64()) as usize;
    for idx in 0..count_req {
        while let Some(r) = set.try_join_next() {
            match r.unwrap() {
                Ok(r) => res.push(r),
                Err(()) => {
                    bad_count += 1;
                    if bad_count == 10 {
                        return false;
                    }
                }
            }
        }

        interval.tick().await;
        set.spawn(exec_req(
            client.clone(),
            reqs[idx % reqs.len()].try_clone().unwrap(),
        ));

        if (idx + 1).count_ones() == 1 {
            eprintln!("[{}] {} / {} / {}", name, res.len(), idx + 1, count_req);
        }
    }

    while let Some(r) = set.join_next().await {
        match r.unwrap() {
            Ok(r) => res.push(r),
            Err(()) => {
                bad_count += 1;
                if bad_count == 10 {
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

async fn test(name: &str, client: Client, reqs: &[Request]) {
    let mut out = File::create(format!("bench-{name}.csv")).unwrap();
    writeln!(
        out,
        "period,count,bad_count,avg,mdev,p0,p50,p90,p99,p999,p100"
    )
    .unwrap();

    for req in reqs {
        exec_req(client.clone(), req.try_clone().unwrap())
            .await
            .unwrap();
    }

    const SYNC_S: u64 = 25;

    let mut period_us = 200;
    loop {
        let unix = UNIX_EPOCH.elapsed().unwrap().as_secs();
        let unix_start = unix.next_multiple_of(SYNC_S);
        let unix_diff = UNIX_EPOCH
            .add(Duration::from_secs(unix_start))
            .duration_since(SystemTime::now())
            .unwrap();
        println!("Starting in {:?}", unix_diff);
        tokio::time::sleep(unix_diff).await;

        let period = Duration::from_micros(period_us);
        let good = bench(
            &format!("{name}-{:.3}ms", period_us as f64 / 1000.),
            client.clone(),
            reqs,
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

    #[clap(long)]
    freq_stress: Option<u64>,

    #[clap(long)]
    lat_stress: Option<u64>,

    table: String,
    key: String,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    Get,
    Set,
}

#[tokio::main]
async fn main() {
    let Args {
        server,
        freq_stress,
        lat_stress,
        table,
        key,
        action,
    } = Args::parse();

    let client = Client::builder()
        // Already uses HTTP2 if detected and has nodelay set to true.
        .http2_prior_knowledge()
        .build()
        .unwrap();

    let reqs: Vec<_> = match action {
        Action::Get => (0..100)
            .map(|i| {
                client
                    .get(format!("http://{server}/{table}/{key}-{i}"))
                    .timeout(Duration::from_secs(5))
                    .build()
                    .unwrap()
            })
            .collect(),
        Action::Set => (0..100)
            .map(|i| {
                client
                    .post(format!("http://{server}/{table}/{key}-{i}"))
                    .timeout(Duration::from_secs(5))
                    .body("value")
                    .build()
                    .unwrap()
            })
            .collect(),
    };

    match (freq_stress, lat_stress) {
        (Some(freq), None) => {
            run_fstress(client.clone(), &reqs, freq).await;
        }
        (None, Some(lat)) => {
            run_lstress(client.clone(), &reqs, lat).await;
        }
        (None, None) => {
            let name = match action {
                Action::Get => "get",
                Action::Set => "set",
            };
            test(name, client.clone(), &reqs).await;
        }
        _ => {
            eprintln!("Please specify either --freq-stress or --lat-stress, not both.");
            return;
        }
    }
}
