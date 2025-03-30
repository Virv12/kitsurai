use crate::peer::Peer;
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub trait Rpc: Serialize + DeserializeOwned {
    type Request: Serialize + DeserializeOwned + From<Self>;
    type Response: Serialize + DeserializeOwned;

    async fn handle(self) -> Result<Self::Response>;

    async fn exec(self, peer: &Peer) -> Result<Self::Response> {
        if peer.is_local() {
            self.handle().await
        } else {
            let mut stream = TcpStream::connect(&peer.addr).await?;
            let req: Self::Request = self.into();
            let bytes = postcard::to_allocvec(&req)?;
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
