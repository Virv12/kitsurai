use crate::rpc::Rpc;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::net::ToSocketAddrs;
use std::{
    net::{IpAddr, SocketAddr},
    sync::LazyLock,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

mod rpc;
mod store;

static REPLICATION: u64 = 3;
static NECESSARY_READ: u64 = 2;
static NECESSARY_WRITE: u64 = 2;

#[derive(Clone)]
struct Peer {
    addr: SocketAddr,
    is_self: bool,
}

static PEERS: LazyLock<Vec<Peer>> = LazyLock::new(|| {
    fn is_local_ip(ip: IpAddr) -> bool {
        let interfaces = local_ip_address::list_afinet_netifas().unwrap();
        for (_, iface_ip) in interfaces {
            if iface_ip == ip {
                return true;
            }
        }

        false
    }

    let mut all: Vec<SocketAddr> = "kitsurai:3000"
        .to_socket_addrs()
        .expect("Empty peers.")
        .collect();
    all.sort();
    let all: Vec<Peer> = all
        .into_iter()
        .map(|addr| Peer {
            addr,
            is_self: is_local_ip(addr.ip()),
        })
        .collect();

    let mut circular = Vec::new();
    circular.extend_from_slice(&all);
    circular.extend_from_slice(&all[..REPLICATION as usize]);
    circular
});

// TODO: non so se serve veramente il più 1
static BUCKET_SIZE: LazyLock<u64> = LazyLock::new(|| u64::MAX / PEERS.len() as u64 + 1);

async fn item_get_handle(
    item_get: ItemGet,
    peer: &Peer,
) -> anyhow::Result<<ItemGet as Rpc<'_>>::Response> {
    if peer.is_self {
        item_get.handle().await
    } else {
        let mut stream = TcpStream::connect(peer.addr).await?;
        let bytes = postcard::to_allocvec(&item_get.into_variant())?;
        stream.write_all(&bytes).await?;

        // Read remote response.
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await?;
        let parsed: <ItemGet as Rpc<'_>>::Response = postcard::from_bytes(&buffer)?;
        Ok(parsed.clone())
    }
}

pub async fn item_get(key: &str) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    let index = xxhash_rust::xxh3::xxh3_64(key.as_ref()) / *BUCKET_SIZE;
    let index = index as usize;
    let chosen = &PEERS[index..index + REPLICATION as usize];

    let mut set = JoinSet::new();
    for peer in chosen {
        set.spawn(item_get_handle(
            ItemGet {
                key: key.to_string(),
            },
            peer,
        ));
    }

    let mut successes = 0;
    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        match res.expect("Join error.")? {
            Ok(res) => {
                successes += 1;
                results.push(res);
            }
            Err(error) => eprintln!("One day maybe handle this: {error}"),
        }

        if successes >= NECESSARY_READ {
            // TODO: gestisci i task che rimangono? o almeno fargli esplodere?
            return Ok(results);
        }
    }

    bail!("Failed to read from {NECESSARY_READ} nodes.")
}

async fn item_set_handle(
    item_set: ItemSet,
    peer: &Peer,
) -> anyhow::Result<<ItemSet as Rpc<'_>>::Response> {
    if peer.is_self {
        item_set.handle().await
    } else {
        let mut stream = TcpStream::connect(peer.addr).await?;
        let bytes = postcard::to_allocvec(&item_set.into_variant())?;
        stream.write_all(&bytes).await?;

        // Read remote response.
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await?;
        let parsed: <ItemSet as Rpc<'_>>::Response = postcard::from_bytes(&buffer)?;
        Ok(parsed.clone())
    }
}

pub async fn item_set(key: &str, value: Vec<u8>) -> anyhow::Result<()> {
    let index = (xxhash_rust::xxh3::xxh3_64(key.as_ref()) / *BUCKET_SIZE) as usize;
    let chosen: &[Peer] = &PEERS[index..index + REPLICATION as usize];

    let mut set = JoinSet::new();
    for peer in chosen {
        set.spawn(item_set_handle(
            ItemSet {
                key: key.to_string(),
                value: value.clone(),
            },
            peer,
        ));
    }

    let mut successes = 0;
    while let Some(res) = set.join_next().await {
        match res.expect("Join error.")? {
            Ok(()) => successes += 1,
            Err(error) => eprintln!("One day maybe handle this: {error}"),
        }

        if successes >= NECESSARY_WRITE {
            // TODO: gestisci i task che rimangono? o almeno fargli esplodere?
            return Ok(());
        }
    }

    bail!("Failed to write to {NECESSARY_WRITE} nodes.")
}

#[derive(Serialize, Deserialize)]
enum Operations {
    ItemGet(ItemGet),
    ItemSet(ItemSet),
}

// impl RpcRequest<'_> for Operations {
//     async fn remote(self, stream: TcpStream) -> anyhow::Result<()> {
//         match self {
//             Self::ItemGet(get) => get.remote(stream).await,
//             Self::ItemSet(set) => set.remote(stream).await,
//         }
//     }
// }

#[derive(Serialize, Deserialize)]
struct ItemGet {
    key: String,
}

impl Rpc<'_> for ItemGet {
    type Request = Operations;
    type Response = Result<Option<Vec<u8>>, store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemGet(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_get(&self.key))
    }
}

#[derive(Serialize, Deserialize)]
struct ItemSet {
    key: String,
    value: Vec<u8>,
}

impl Rpc<'_> for ItemSet {
    type Request = Operations;
    type Response = Result<(), store::Error>;

    fn into_variant(self) -> Self::Request {
        Operations::ItemSet(self)
    }

    async fn handle(self) -> anyhow::Result<Self::Response> {
        Ok(store::item_set(&self.key, self.value))
    }
}

pub async fn listener<A: tokio::net::ToSocketAddrs>(addr: A) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(recv(socket));
    }
}
async fn recv(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await?;
    let variant: Operations = postcard::from_bytes(&buffer)?;
    match variant {
        Operations::ItemGet(get) => {
            let result = get.handle().await?;

            // Send result.
            let bytes = postcard::to_allocvec(&result)?;
            stream.write_all(&bytes).await?;
            Ok(())
        }
        Operations::ItemSet(set) => {
            let result = set.handle().await?;

            // Send result.
            let bytes = postcard::to_allocvec(&result)?;
            stream.write_all(&bytes).await?;
            Ok(())
        }
    }
}
