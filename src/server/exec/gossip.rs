use crate::{
    exec::Operations,
    merkle,
    meta::{self, Table, TableStatus},
    peer::{self, Peer},
    rpc::Rpc,
};
use anyhow::Result;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) async fn gossip(token: CancellationToken) -> Result<()> {
    loop {
        let peers = peer::peers();
        let mut rng = rand::rng();
        let peer = peers.choose(&mut rng).expect("no peers available");
        if !peer.is_local() {
            check_sync(peer, merkle::Path::root()).await?;
        }

        tokio::select! {
            _ = token.cancelled() => break,
            _ = sleep(Duration::from_secs(10)) => {}
        };
    }

    Ok(())
}

async fn check_sync(peer: &Peer, path: merkle::Path) -> Result<()> {
    log::debug!("Gossiping with {} at {}", peer.addr, path);

    let local_node = meta::MERKLE
        .lock()
        .expect("poisoned merkle lock")
        .find(path);

    let remote_node = MerkleFind { path }.exec(peer).await?;

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

async fn sync(peer: &Peer, path: merkle::Path) -> Result<()> {
    if let Some((left, right)) = path.children() {
        Box::pin(check_sync(peer, left)).await?;
        Box::pin(check_sync(peer, right)).await?;
    } else {
        log::debug!("Syncing {}", path);
        let id = Uuid::from_u128_le(path.id);
        let table = Table::load(id)?;
        let table = table.map(|table| table.status);
        let res = Gossip { id, table }.exec(peer).await?;
        if let Some(table) = res {
            Table { id, status: table }.growing_save()?;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Gossip {
    id: Uuid,
    table: Option<TableStatus>,
}

impl Rpc for Gossip {
    type Request = Operations;
    type Response = Option<TableStatus>;

    async fn handle(self) -> Result<Self::Response> {
        if let Some(table) = self.table {
            Table {
                id: self.id,
                status: table,
            }
            .growing_save()
        } else {
            Table::load(self.id)
        }
        .map(|opt| opt.map(|table| table.status))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MerkleFind {
    path: merkle::Path,
}

impl Rpc for MerkleFind {
    type Request = Operations;
    type Response = Option<(merkle::Path, u128)>;

    async fn handle(self) -> Result<Self::Response> {
        let node = meta::MERKLE
            .lock()
            .expect("poisoned merkle lock")
            .find(self.path);
        Ok(node)
    }
}
