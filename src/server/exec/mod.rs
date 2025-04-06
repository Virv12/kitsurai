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
    rpc::Rpc,
    state::{Table, TableData, TableParams, TableStatus},
};
use derive_more::From;
use gossip::{GossipFind, GossipSync};
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
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
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).await?;
            let variant: Operations = postcard::from_bytes(&buffer)?;
            log::debug!("Request from {}: {}", stream.peer_addr()?, variant.name());

            match variant {
                Operations::ItemGet(get) => get.remote(stream).await,
                Operations::ItemSet(set) => set.remote(stream).await,
                Operations::ItemList(list) => list.remote(stream).await,
                Operations::TablePrepare(prepare) => prepare.remote(stream).await,
                Operations::TableCommit(commit) => commit.remote(stream).await,
                Operations::TableDelete(delete) => delete.remote(stream).await,
                Operations::GossipSync(sync) => sync.remote(stream).await,
                Operations::GossipFind(find) => find.remote(stream).await,
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
