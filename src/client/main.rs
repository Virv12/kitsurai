use clap::{Parser, Subcommand};
use kitsurai::codec::Header;
use reqwest::Body;
use serde::Serialize;
use std::{ffi::OsString, os::unix::ffi::OsStringExt};
use tokio::fs::File;
use uuid::Uuid;

#[derive(Parser, Serialize, Debug, Clone, Copy)]
struct TableParams {
    b: u64,
    n: u64,
    w: u64,
    r: u64,
}

#[derive(Subcommand, Debug, Clone)]
enum TableActions {
    #[command(alias = "c")]
    Create(TableParams),
    #[command(alias = "ls")]
    List,
    #[command(alias = "d")]
    Delete { table: Uuid },
}

#[derive(Subcommand, Debug, Clone)]
enum ItemActions {
    #[command(alias = "g")]
    Get {
        #[arg(short, long, default_value = "4096")]
        /// Maximum length that will be printed.
        limit: usize,

        key: String,
    },
    #[command(alias = "s")]
    Set {
        #[arg(short, long, default_value = "false", requires = "value")]
        /// Parses VALUE as a file and sends its contents.
        file: bool,

        key: String,
        value: OsString,
    },
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
        table: Uuid,

        #[command(subcommand)]
        action: ItemActions,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { server, action } = Args::parse();

    match action {
        Actions::Table {
            action: TableActions::Create(params),
        } => {
            let client = reqwest::Client::new();
            let res = client
                .post(format!("http://{server}"))
                .form(&params)
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
        Actions::Table {
            action: TableActions::Delete { table },
        } => {
            let client = reqwest::Client::new();
            let res = client
                .delete(format!("http://{server}/{table}"))
                .send()
                .await?;
            println!("{}: {}", res.status(), res.text().await?.trim_end());
        }
        Actions::Item {
            table,
            action: ItemActions::Get { limit, key },
        } => {
            let url = format!("http://{server}/{table}/{key}");
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
        Actions::Item {
            table,
            action: ItemActions::Set { file, key, value },
        } => {
            let url = format!("http://{server}/{table}/{key}");
            let body: Body = if !file {
                value.into_vec().into()
            } else {
                File::open(value).await?.into()
            };
            let client = reqwest::Client::new();
            let res = client.post(url).body(body).send().await?;
            println!("{}: {}", res.status(), res.text().await?.trim_end());
        }
        Actions::Item {
            table,
            action: ItemActions::List,
        } => {
            let res = reqwest::get(format!("http://{server}/{table}")).await?;
            if res.status().is_success() {
                println!("{}", res.text().await?.trim_end());
            } else {
                println!("{}: {}", res.status(), res.text().await?);
            }
        }
    }

    Ok(())
}
