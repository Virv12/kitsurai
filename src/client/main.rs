use clap::Parser;
use kitsurai::codec::Header;
use reqwest::IntoUrl;
use std::{ffi::OsString, fs::read, os::unix::ffi::OsStringExt, path::PathBuf};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "localhost:8000")]
    server: String,

    #[arg(short, long, default_value = "false", requires = "value")]
    /// Parses VALUE as a file and sends its contents.
    file: bool,

    #[arg(short, long, default_value = "4096")]
    /// Maximum length that will be printed.
    limit: usize,

    key: String,

    #[arg(raw = true)]
    value: Option<OsString>,
}

async fn post(url: impl IntoUrl, value: Vec<u8>) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let res = client.post(url).body(value).send().await?;
    println!("{}: {}", res.status(), res.text().await?.trim_end());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        server,
        key,
        limit,
        value,
        file,
    } = Args::parse();

    let url = format!("http://{server}/{key}");

    match (value, file) {
        (Some(input), false) => post(url, input.into_vec()).await?,
        (Some(file), true) => post(url, read(PathBuf::from(file).canonicalize()?)?).await?,
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
                        println!("Too large, skipped.");
                    }
                }
            }
        }
    }

    Ok(())
}
