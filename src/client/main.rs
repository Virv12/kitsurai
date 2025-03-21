use clap::Parser;
use kitsurai::codec::Header;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "localhost:8000")]
    server: String,

    key: String,

    #[arg(raw = true)]
    value: Option<OsString>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { server, key, value } = Args::parse();

    let url = format!("http://{server}/{key}");
    let client = reqwest::Client::new();

    if let Some(value) = value {
        let res = client.post(&url).body(value.into_vec()).send().await?;
        println!("{}: {}", res.status(), res.text().await?.trim_end());
    } else {
        let res = client.get(&url).send().await?;
        if res.status().is_success() {
            let bytes = res.bytes().await?;
            let (header, rest) = postcard::take_from_bytes::<Header>(&bytes)?;
            println!("{}", header);
            for value in header.extract(bytes.slice(bytes.len() - rest.len()..)) {
                println!("{:?}", value);
            }
        }
    }

    Ok(())
}
