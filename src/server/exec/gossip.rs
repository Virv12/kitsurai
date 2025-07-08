use crate::{
    exec::Operations,
    merkle,
    peer::{self, Peer},
    rpc::Rpc,
    state::{self, Table, TableStatus},
};
use anyhow::Result;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Runs the gossip tasks with a random peer
///  with a delay of 1 second between executions.
pub async fn gossip(token: CancellationToken) -> Result<()> {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = sleep(Duration::from_secs(1)) => {}
        }

        let peers = peer::peers();
        let mut rng = rand::rng();
        let peer = peers.choose(&mut rng).expect("no peers available");
        if !peer.is_local() {
            log::debug!("Gossiping with {}", peer.addr);
            if let Err(err) = check_sync(peer, merkle::Path::root()).await {
                log::error!("Gossiping with {} failed: {}", peer.addr, err);
            }
        }
    }

    Ok(())
}

/// Checks if this node is in sync for the given path with the given peer.
///
/// Syncs whenever a difference is detected.
async fn check_sync(peer: &'static Peer, path: merkle::Path) -> Result<()> {
    log::debug!("Gossiping with {} at {}", peer.addr, path);

    let local_node = state::merkle_find(path).await;
    let remote_node = GossipFind { path }.exec(peer).await?;

    match (local_node, remote_node) {
        (None, None) => Ok(()),
        (None, Some((p, _))) => sync(peer, p).await,
        (Some((p, _)), None) => sync(peer, p).await,
        (Some((p1, h1)), Some((p2, h2))) => {
            if h1 == h2 {
                return Ok(());
            }

            if !p1.contains(p2) || p1 == p2 {
                sync(peer, p2).await?;
            }
            if !p2.contains(p1) {
                sync(peer, p1).await?;
            }
            Ok(())
        }
    }
}

/// Recursively finds the different table
///  or, if found, sends the table metadata to the other peer.
async fn sync(peer: &'static Peer, path: merkle::Path) -> Result<()> {
    fn spawn(peer: &'static Peer, path: merkle::Path) -> JoinHandle<Result<()>> {
        tokio::spawn(check_sync(peer, path))
    }

    if let Some((left, right)) = path.children() {
        let left = spawn(peer, left);
        let right = spawn(peer, right);
        left.await??;
        right.await??;
    } else {
        log::debug!("Syncing {path}");
        let id = Uuid::from_u128_le(path.id);
        let table = Table::load(id).await?;
        let table = table.map(|table| table.status);
        let res = GossipSync { id, table }.exec(peer).await?;
        if let Some(table) = res {
            Table { id, status: table }.growing_save().await?;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GossipSync {
    id: Uuid,
    table: Option<TableStatus>,
}

impl Rpc for GossipSync {
    type Request = Operations;
    type Response = Option<TableStatus>;

    async fn handle(self) -> Result<Self::Response> {
        if let Some(table) = self.table {
            Table {
                id: self.id,
                status: table,
            }
            .growing_save()
            .await
        } else {
            Table::load(self.id).await
        }
        .map(|opt| opt.map(|table| table.status))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GossipFind {
    path: merkle::Path,
}

impl Rpc for GossipFind {
    type Request = Operations;
    type Response = Option<(merkle::Path, u128)>;

    async fn handle(self) -> Result<Self::Response> {
        Ok(state::merkle_find(self.path).await)
    }
}
