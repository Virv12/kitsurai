use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Serialize, Deserialize)]
pub struct Header {
    pub lengths: Vec<Option<u64>>,
}

impl Header {
    pub fn extract(&self, data: Bytes) -> Vec<Bytes> {
        let mut output = Vec::new();
        let mut offset = 0;
        for len in self.lengths.iter().copied().flatten() {
            output.push(data.slice(offset..offset + len as usize));
            offset += len as usize;
        }
        output
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "R={}, {:?}", self.lengths.len(), self.lengths)?;
        Ok(())
    }
}
