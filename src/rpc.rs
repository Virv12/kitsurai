use crate::Peer;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

pub(crate) trait Rpc
where
    Self: Serialize + DeserializeOwned,
{
    type Request: Serialize + DeserializeOwned;
    type Response: Serialize + DeserializeOwned + Clone;

    fn into_variant(self) -> Self::Request;
    async fn handle(self) -> anyhow::Result<Self::Response>;
}

pub(crate) trait RpcExec
where
    Self: Rpc,
{
    async fn exec(self, peer: &Peer) -> anyhow::Result<Self::Response>;
    async fn remote(self, stream: TcpStream) -> anyhow::Result<()>;
}

impl<T: Rpc> RpcExec for T {
    async fn exec(self, peer: &Peer) -> anyhow::Result<Self::Response> {
        if peer.is_self {
            self.handle().await
        } else {
            let mut stream = TcpStream::connect(peer.addr).await?;
            let bytes = postcard::to_allocvec(&self.into_variant())?;
            stream.write_all(&bytes).await?;
            stream.shutdown().await?;

            // Read remote response.
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).await?;
            let parsed = postcard::from_bytes::<Self::Response>(&buffer)?;
            Ok(parsed.clone())
        }
    }

    async fn remote(self, mut stream: TcpStream) -> anyhow::Result<()> {
        let result = self.handle().await?;

        // Send result.
        let bytes = postcard::to_allocvec(&result)?;
        stream.write_all(&bytes).await?;
        Ok(())
    }
}

pub(crate) trait RpcRequest: Serialize + DeserializeOwned {
    fn remote(
        self,
        stream: TcpStream,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

pub(crate) trait RpcRequestRecv: RpcRequest {
    async fn listener<A: ToSocketAddrs>(addr: A) -> anyhow::Result<()>;
    async fn recv(stream: TcpStream) -> anyhow::Result<()>;
}

impl<T: RpcRequest + 'static> RpcRequestRecv for T {
    async fn listener<A: ToSocketAddrs>(addr: A) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            let (socket, _) = listener.accept().await?;
            tokio::spawn(Self::recv(socket));
        }
    }
    async fn recv(mut stream: TcpStream) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await?;
        let variant: Self = postcard::from_bytes(&buffer)?;
        variant.remote(stream).await
    }
}
