use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::LazyLock,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod store;

pub async fn table_create(name: &str) -> Result<()> {
    store::table_create(name)?;
    rpc_broadcast(RpcRequest::TableCreate(name.to_string())).await?;
    Ok(())
}

pub async fn table_delete(name: &str) -> Result<()> {
    store::table_delete(name)?;
    rpc_broadcast(RpcRequest::TableDelete(name.to_string())).await?;
    Ok(())
}

pub async fn item_get(table: &str, key: &str) -> Result<Option<Vec<u8>>> {
    store::item_get(table, key)
}

pub async fn item_set(table: &str, key: &str, value: Vec<u8>) -> Result<()> {
    store::item_set(table, key, value.clone())?;
    rpc_broadcast(RpcRequest::ItemSet(
        table.to_string(),
        key.to_string(),
        value,
    ))
    .await?;
    Ok(())
}

pub async fn item_delete(table: &str, key: &str) -> Result<()> {
    store::item_delete(table, key)?;
    rpc_broadcast(RpcRequest::ItemDelete(table.to_string(), key.to_string())).await?;
    Ok(())
}

static PEERS: LazyLock<Vec<SocketAddr>> = LazyLock::new(|| {
    fn is_local_ip(ip: IpAddr) -> bool {
        // TODO: This is not portable to non-linux machines
        let interfaces = local_ip_address::list_afinet_netifas().unwrap();
        for (_, iface_ip) in interfaces {
            if iface_ip == ip {
                return true;
            }
        }

        false
    }

    "kitsurai:3000"
        .to_socket_addrs()
        .unwrap()
        .filter(|addr| !is_local_ip(addr.ip()))
        .collect()
});

#[derive(Serialize, Deserialize)]
enum RpcRequest {
    TableCreate(String),
    TableDelete(String),
    ItemSet(String, String, Vec<u8>),
    ItemDelete(String, String),
}

async fn rpc_broadcast(req: RpcRequest) -> Result<()> {
    // TODO: parallelize
    for &peer in &*PEERS {
        rpc_send(peer, &req).await?;
    }
    Ok(())
}

async fn rpc_send(peer: SocketAddr, req: &RpcRequest) -> Result<()> {
    let mut stream = TcpStream::connect(peer).await?;
    let bytes = postcard::to_allocvec(&req).unwrap();
    stream.write_all(&bytes).await?;
    Ok(())
}

async fn rpc_recv(mut stream: TcpStream) {
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await.unwrap();

    match postcard::from_bytes(&buffer).unwrap() {
        RpcRequest::TableCreate(table) => store::table_create(&table).unwrap(),
        RpcRequest::TableDelete(table) => store::table_delete(&table).unwrap(),
        RpcRequest::ItemSet(table, key, value) => store::item_set(&table, &key, value).unwrap(),
        RpcRequest::ItemDelete(table, key) => store::item_delete(&table, &key).unwrap(),
    }
}

pub async fn rpc_listener() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:3000").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(rpc_recv(socket));
    }
}
