//! Implements RPCs and uses them to execute actions on the cluster.<br>
//! Exposes methods for client's usage.

pub mod gossip;
pub mod item;
pub mod table;

use crate::{
    exec::{
        item::{ItemGet, ItemList, ItemSet},
        table::{TableCommit, TableDelete, TablePrepare},
    },
    peer::Peer,
    state::{Table, TableData, TableParams, TableStatus},
};
use anyhow::Result;
use derive_more::From;
use gossip::{GossipFind, GossipSync};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, future::Future, sync::LazyLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

/// Await the task (usually some RPC) and keep its peer to aid debugging.
async fn keep_peer<O>(peer: &Peer, task: impl Future<Output = O>) -> (&Peer, O) {
    (peer, task.await)
}

/// All known RPCs.
#[derive(Debug, Serialize, Deserialize, From)]
pub enum Operations {
    ItemGet(ItemGet),
    ItemSet(ItemSet),
    ItemList(ItemList),
    TablePrepare(TablePrepare),
    TableCommit(TableCommit),
    TableDelete(TableDelete),
    GossipSync(GossipSync),
    GossipFind(GossipFind),
}

static CONN_CACHE: LazyLock<Mutex<HashMap<String, Vec<TcpStream>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl Operations {
    /// Returns the RPC name for debugging purposes.
    fn name(&self) -> &'static str {
        match self {
            Operations::ItemGet(_) => "item-get",
            Operations::ItemSet(_) => "item-set",
            Operations::ItemList(_) => "item-list",
            Operations::TablePrepare(_) => "table-prepare",
            Operations::TableCommit(_) => "table-commit",
            Operations::TableDelete(_) => "table-delete",
            Operations::GossipSync(_) => "gossip-sync",
            Operations::GossipFind(_) => "gossip-find",
        }
    }

    /// Runs the RPC listener.
    ///
    /// Waits for incoming requests, deserializes them and executes the appropriate action.
    pub async fn listener(listener: TcpListener, token: CancellationToken) -> anyhow::Result<()> {
        async fn recv(mut stream: TcpStream) -> anyhow::Result<()> {
            loop {
                let mut len_buffer = [0u8; 8];
                stream.read_exact(&mut len_buffer).await?;
                let len = u64::from_le_bytes(len_buffer) as usize;
                let mut buffer = vec![0u8; len];
                stream.read_exact(&mut buffer).await?;
                let variant: Operations = postcard::from_bytes(&buffer)?;
                log::debug!("Request from {}: {}", stream.peer_addr()?, variant.name());

                match variant {
                    Operations::ItemGet(get) => get.remote(&mut stream).await?,
                    Operations::ItemSet(set) => set.remote(&mut stream).await?,
                    Operations::ItemList(list) => list.remote(&mut stream).await?,
                    Operations::TablePrepare(prepare) => prepare.remote(&mut stream).await?,
                    Operations::TableCommit(commit) => commit.remote(&mut stream).await?,
                    Operations::TableDelete(delete) => delete.remote(&mut stream).await?,
                    Operations::GossipSync(sync) => sync.remote(&mut stream).await?,
                    Operations::GossipFind(find) => find.remote(&mut stream).await?,
                }
            }
        }

        loop {
            let result = tokio::select! {
                _ = token.cancelled() => break,
                result = listener.accept() => result,
            };

            let (socket, peer) = result?;
            tokio::spawn(async move {
                match recv(socket).await {
                    Ok(_) => {}
                    Err(error) => log::error!("failed to handle request from {peer}: {error}"),
                };
            });
        }

        Ok(())
    }
}

async fn get_conn(peer: &str) -> Result<TcpStream> {
    let cached = CONN_CACHE
        .lock()
        .await
        .entry(peer.to_string())
        .or_insert_with(Vec::new)
        .pop();
    match cached {
        Some(stream) => Ok(stream),
        None => Ok(TcpStream::connect(peer).await?),
    }
}

async fn put_conn(peer: &str, stream: TcpStream) {
    CONN_CACHE
        .lock()
        .await
        .entry(peer.to_string())
        .or_insert_with(Vec::new)
        .push(stream);
}

pub trait Rpc: Serialize + DeserializeOwned {
    type Request: Serialize + DeserializeOwned + From<Self>;
    type Response: Serialize + DeserializeOwned;

    async fn handle(self) -> Result<Self::Response>;

    async fn exec(self, peer: &Peer) -> Result<Self::Response> {
        if peer.is_local() {
            self.handle().await
        } else {
            let mut stream = get_conn(&peer.addr).await?;
            let req: Self::Request = self.into();
            let bytes = postcard::to_allocvec(&req)?;
            stream
                .write_all(&(bytes.len() as u64).to_le_bytes())
                .await?;
            stream.write_all(&bytes).await?;

            // Read remote response.
            let mut len_buffer = [0u8; 8];
            stream.read_exact(&mut len_buffer).await?;
            let len = u64::from_le_bytes(len_buffer) as usize;
            let mut buffer = vec![0u8; len];
            stream.read_exact(&mut buffer).await?;
            put_conn(&peer.addr, stream).await;
            let parsed = postcard::from_bytes(&buffer)?;
            Ok(parsed)
        }
    }

    async fn remote(self, stream: &mut TcpStream) -> Result<()> {
        let result = self.handle().await?;

        // Send result.
        let bytes = postcard::to_allocvec(&result)?;
        stream
            .write_all(&(bytes.len() as u64).to_le_bytes())
            .await?;
        stream.write_all(&bytes).await?;
        Ok(())
    }
}
