use serde::{Deserialize, Serialize};

pub(crate) trait Rpc<'de>
where
    Self: Serialize + Deserialize<'de>,
{
    type Request: Serialize + Deserialize<'de>;
    type Response: Serialize + Deserialize<'de> + Clone;

    fn into_variant(self) -> Self::Request;
    async fn handle(self) -> anyhow::Result<Self::Response>;
}

// trait RpcExec<'de>
// where
//     Self: Rpc<'de>,
// {
//     async fn exec(self, peer: &Peer) -> anyhow::Result<Self::Response>;
//     async fn remote(self, stream: TcpStream) -> anyhow::Result<()>;
// }

// impl<'de, T: Rpc<'de>> RpcExec<'de> for T {
//     async fn exec(self, peer: &Peer) -> anyhow::Result<Self::Response> {
//         if peer.is_self {
//             self.handle().await
//         } else {
//             let mut stream = TcpStream::connect(peer.addr).await?;
//             let bytes = postcard::to_allocvec(&self.into_variant())?;
//             stream.write_all(&bytes).await?;
//     
//             // Read remote response.
//             let mut buffer = Vec::new();
//             stream.read_to_end(&mut buffer).await?;
//             let parsed = postcard::from_bytes::<Self::Response>(&buffer)?;
//             Ok(parsed.clone())
//         }
//     }
// 
//     async fn remote(self, mut stream: TcpStream) -> anyhow::Result<()> {
//         let result = self.handle().await?;
// 
//         // Send result.
//         let bytes = postcard::to_allocvec(&result)?;
//         stream.write_all(&bytes).await?;
//         Ok(())
//     }
// }

// pub(crate) trait RpcRequest<'de>: Serialize + Deserialize<'de> {
//     async fn remote(self, stream: TcpStream) -> anyhow::Result<()>;
// }

// pub(crate) trait RpcRequestRecv<'de>: RpcRequest<'de> {
//     async fn listener(addr: SocketAddr) -> anyhow::Result<()>;
//     async fn recv(stream: TcpStream) -> anyhow::Result<()>;
// }

// impl<'de, T: RpcRequest<'de>> RpcRequestRecv<'de> for T {
//     async fn listener(addr: SocketAddr) -> anyhow::Result<()> {
//         let listener = TcpListener::bind(addr).await?;
//
//         loop {
//             let (socket, _) = listener.accept().await?;
//             tokio::spawn(Self::recv(socket));
//         }
//     }
//     async fn recv(mut stream: TcpStream) -> anyhow::Result<()> {
//         let mut buffer = Vec::new();
//         stream.read_to_end(&mut buffer).await?;
//         let variant: Self = postcard::from_bytes(&buffer)?;
//         variant.remote(stream).await.await
//     }
// }
