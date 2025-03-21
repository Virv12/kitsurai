use crate::peer::Peer;
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tokio_util::sync::CancellationToken;

pub(crate) trait Rpc: Serialize + DeserializeOwned {
    type Request: Serialize + DeserializeOwned;
    type Response: Serialize + DeserializeOwned;

    fn into_variant(self) -> Self::Request;

    async fn handle(self) -> Result<Self::Response>;

    async fn exec(self, peer: &Peer) -> Result<Self::Response> {
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
            let parsed = postcard::from_bytes(&buffer)?;
            Ok(parsed)
        }
    }

    async fn remote(self, mut stream: TcpStream) -> Result<()> {
        let result = self.handle().await?;

        // Send result.
        let bytes = postcard::to_allocvec(&result)?;
        stream.write_all(&bytes).await?;
        Ok(())
    }
}

pub(crate) trait RpcRequest: Serialize + DeserializeOwned {
    fn remote(self, stream: TcpStream) -> impl std::future::Future<Output = Result<()>> + Send;

    async fn listener<A: ToSocketAddrs>(addr: A, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            let result = tokio::select! {
                _ = token.cancelled() => break,
                result = listener.accept() => result,
            };

            let (socket, peer) = result?;
            tokio::spawn(async move {
                match recv::<Self>(socket).await {
                    Ok(_) => eprintln!("RPC: successfully handled for {}", peer.ip()),
                    Err(error) => eprintln!("RPC: error while handling {peer}, {error}"),
                };
            });
        }

        Ok(())
    }
}

async fn recv<T: RpcRequest>(mut stream: TcpStream) -> Result<()> {
    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await?;
    let variant: T = postcard::from_bytes(&buffer)?;
    variant.remote(stream).await
}
