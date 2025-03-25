use clap::{Parser, Subcommand};
use kitsurai::codec::Header;
use reqwest::{Body, IntoUrl};
use serde::Serialize;
use std::{ffi::OsString, os::unix::ffi::OsStringExt};
use tokio::fs::File;
use uuid::Uuid;

#[derive(Subcommand, Debug, Clone)]
enum TableActions {
    #[command(alias = "c")]
    Create { b: u64, n: u64, r: u64, w: u64 },
    #[command(alias = "ls")]
    List,
}

#[derive(Subcommand, Debug, Clone)]
enum Actions {
    #[command(alias = "t")]
    Table {
        #[command(subcommand)]
        action: TableActions,
    },
    #[command(alias = "i")]
    Item {
        #[arg(short, long, default_value = "false", requires = "value")]
        /// Parses VALUE as a file and sends its contents.
        file: bool,

        #[arg(short, long, default_value = "4096")]
        /// Maximum length that will be printed.
        limit: usize,

        table: Uuid,
        key: String,
        value: Option<OsString>,
    },
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "localhost:8000")]
    server: String,

    #[command(subcommand)]
    action: Actions,
}

async fn post(url: impl IntoUrl, value: impl Into<Body>) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client.post(url).body(value).send().await?;
    println!("{}: {}", res.status(), res.text().await?.trim_end());
    Ok(())
}

#[derive(Serialize)]
struct TableParams {
    b: u64,
    n: u64,
    w: u64,
    r: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { server, action } = Args::parse();

    match action {
        Actions::Table {
            action: TableActions::Create { b, n, r, w },
        } => {
            let client = reqwest::Client::new();
            let res = client
                .post(format!("http://{server}"))
                .form(&TableParams { b, n, w, r })
                .send()
                .await?;
            println!("{}: {}", res.status(), res.text().await?.trim_end());
        }
        Actions::Table {
            action: TableActions::List,
        } => {
            let res = reqwest::get(format!("http://{server}")).await?;
            println!("{}\n{}", res.status(), res.text().await?.trim_end());
        }
        Actions::Item {
            file,
            limit,
            table,
            key,
            value,
        } => {
            let url = format!("http://{server}/{table}/{key}");
            match (value, file) {
                (Some(input), false) => post(url, input.into_vec()).await?,
                (Some(file), true) => post(url, File::open(file).await?).await?,
                (None, _) => {
                    let res = reqwest::get(&url).await?;

                    if res.status().is_success() {
                        let bytes = res.bytes().await?;
                        let (header, rest) = postcard::take_from_bytes::<Header>(&bytes)?;
                        println!("{}", header);
                        for value in header.extract(bytes.slice(bytes.len() - rest.len()..)) {
                            if value.len() <= limit {
                                println!("{:?}", value);
                            } else {
                                println!("{:?}...", value.slice(..limit));
                            }
                        }
                    } else {
                        println!("{}: {}", res.status(), res.text().await?);
                    }
                }
            }
        }
    }

    Ok(())
}
