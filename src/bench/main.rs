use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};

use reqwest::{Client, Request};
use tokio::task::JoinSet;

type Res = Result<f64, ()>;

async fn exec_req(client: Client, req: Request) -> Res {
    let start_time = Instant::now();
    let res = client.execute(req).await.map_err(|_| ())?;
    let status = res.status();
    let _body = res.bytes().await.map_err(|_| ())?;
    let end_time = Instant::now();
    if !status.is_success() {
        return Err(());
    }
    Ok(end_time.duration_since(start_time).as_secs_f64() * 1000.)
}

async fn bench(
    name: &str,
    client: Client,
    req: Request,
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

        if (idx + 1).count_ones() == 1 {
            eprintln!("[{}] {} / {} / {}", name, res.len(), idx + 1, count_req);
        }

        interval.tick().await;
        set.spawn(exec_req(client.clone(), req.try_clone().unwrap()));
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

async fn test(name: &str, client: Client, req: Request) {
    let mut out = File::create(format!("bench-{name}.csv")).unwrap();
    writeln!(
        out,
        "period,count,bad_count,avg,mdev,p0,p50,p90,p99,p999,p100"
    )
    .unwrap();

    for _ in 0..30 {
        exec_req(client.clone(), req.try_clone().unwrap())
            .await
            .unwrap();
    }

    let mut period_us = 1000;
    loop {
        let period = Duration::from_micros(period_us);
        let good = bench(
            &format!("{name}-{:.3}ms", period_us as f64 / 1000.),
            client.clone(),
            req.try_clone().unwrap(),
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

#[tokio::main]
async fn main() {
    let table = std::env::args().nth(1).unwrap();
    let key = std::env::args().nth(2).unwrap();

    let client = Client::new();

    test(
        "get",
        client.clone(),
        client
            .get(format!("http://localhost:8000/{table}/{key}"))
            .build()
            .unwrap(),
    )
    .await;

    test(
        "set",
        client.clone(),
        client
            .post(format!("http://localhost:8000/{table}/{key}"))
            .body("value-1")
            .build()
            .unwrap(),
    )
    .await;
}
