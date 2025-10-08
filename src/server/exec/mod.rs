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
use axum::body::Body;
use bytes::Bytes;
use derive_more::From;
use gossip::{GossipFind, GossipSync};
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::{client::conn::http2::SendRequest, server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock as StdRwLock;
use std::{collections::HashMap, future::Future, sync::LazyLock};
use tokio::net::{TcpListener, TcpStream};
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
    #[allow(dead_code)]
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
    pub async fn listener(listener: TcpListener, token: CancellationToken) -> Result<()> {
        async fn recv(req: Request<hyper::body::Incoming>) -> Result<Response<Body>> {
            let body = req.into_body();
            let data = body.collect().await?.to_bytes();
            let variant: Operations = postcard::from_bytes(&data)?;
            // log::debug!("Request from {}: {}", stream.peer_addr()?, variant.name());

            let body = match variant {
                Operations::ItemGet(get) => get.remote().await?,
                Operations::ItemSet(set) => set.remote().await?,
                Operations::ItemList(list) => list.remote().await?,
                Operations::TablePrepare(prepare) => prepare.remote().await?,
                Operations::TableCommit(commit) => commit.remote().await?,
                Operations::TableDelete(delete) => delete.remote().await?,
                Operations::GossipSync(sync) => sync.remote().await?,
                Operations::GossipFind(find) => find.remote().await?,
            };

            Ok(Response::builder().status(200).body(body)?)
        }

        loop {
            let result = tokio::select! {
                _ = token.cancelled() => break,
                result = listener.accept() => result,
            };
            let (stream, _peer) = result?;
            stream.set_nodelay(true)?;
            let io = TokioIo::new(stream);

            tokio::spawn(async move {
                if let Err(err) = http2::Builder::new(TokioExecutor::new())
                    .serve_connection(io, service_fn(recv))
                    .await
                {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }

        Ok(())
    }
}

type Connection = SendRequest<Full<Bytes>>;

struct ConnectionQueue {
    current: AtomicUsize,
    queue: Vec<Connection>,
}

impl ConnectionQueue {
    fn next(&self) -> Connection {
        self.queue[self.current.fetch_add(1, Ordering::Relaxed) % self.queue.len()].clone()
    }
}

static CONN_CACHE: LazyLock<StdRwLock<HashMap<String, ConnectionQueue>>> =
    LazyLock::new(|| StdRwLock::new(HashMap::new()));

async fn get_conn(peer: &str) -> Result<SendRequest<Full<Bytes>>> {
    {
        let cache = CONN_CACHE.read().expect("not poisoned");
        if let Some(conn_queue) = cache.get(peer) {
            let conn = conn_queue.next();
            if !conn.is_closed() {
                return Ok(conn);
            }
        }
    }

    let mut queue = vec![];
    for _ in 0..20 {
        let stream = TcpStream::connect(peer).await?;
        let io = TokioIo::new(stream);

        // Perform an HTTP/2 handshake over the TCP connection
        let (sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;

        // Spawn the connection driver (handles incoming frames)
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("Connection error: {:?}", err);
            }
        });

        queue.push(sender);
    }

    let mut cache = CONN_CACHE.write().expect("poisoned");
    let conn_queue = ConnectionQueue {
        queue,
        current: AtomicUsize::new(0),
    };
    let conn = conn_queue.next();
    cache.insert(peer.to_owned(), conn_queue);
    Ok(conn)
}

pub trait Rpc: Serialize + DeserializeOwned {
    type Request: Serialize + DeserializeOwned + From<Self>;
    type Response: Serialize + DeserializeOwned;

    async fn exec(self, peer: &Peer) -> Result<Self::Response> {
        if peer.is_local() {
            self.handle().await
        } else {
            let mut sender = get_conn(&peer.addr).await?;

            let req: Self::Request = self.into();
            let bytes = postcard::to_allocvec(&req)?;
            let req = Request::builder().body(Full::new(Bytes::from(bytes)))?;

            let resp = sender.send_request(req).await?;
            let data = resp.into_body().collect().await?.to_bytes();
            Ok(postcard::from_bytes(&data)?)
        }
    }

    /// Implements the RPC on this node, automatically wrapped by the trait on remotes.
    async fn handle(self) -> Result<Self::Response>;

    /// Wraps the locally produced value into an HTTP Body.
    /// Can be overridden to provide a faster path.
    async fn remote(self) -> Result<Body> {
        let result = self.handle().await?;
        let bytes = postcard::to_allocvec(&result)?;
        Ok(Body::new(Full::new(Bytes::from(bytes))))
    }
}
