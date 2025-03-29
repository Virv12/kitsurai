//! Merkle tree implementation, used by the gossip protocol.

use serde::{Deserialize, Serialize};
use std::{fmt::Display, hash::Hasher, net::Ipv6Addr};

#[derive(Debug, Serialize, Deserialize, Default, Copy, Clone, Eq, PartialEq)]
pub struct Path {
    pub id: u128,
    pub prefix: u8,
}

impl Path {
    pub fn root() -> Self {
        Self { id: 0, prefix: 0 }
    }

    pub fn leaf(id: u128) -> Self {
        Self { id, prefix: 128 }
    }

    pub fn is_leaf(self) -> bool {
        self.prefix == 128
    }

    pub fn contains(self, other: Self) -> bool {
        self.prefix <= other.prefix && (self.id ^ other.id) & ((1 << self.prefix) - 1) == 0
    }

    pub fn children(self) -> Option<(Path, Path)> {
        if self.prefix == 128 {
            return None;
        }

        let left = Path {
            id: self.id,
            prefix: self.prefix + 1,
        };
        let right = Path {
            id: self.id | (1 << self.prefix),
            prefix: self.prefix + 1,
        };
        Some((left, right))
    }

    pub fn lca(self, other: Self) -> Self {
        let prefix = (self.id ^ other.id).trailing_zeros() as u8;
        let prefix = prefix.min(self.prefix).min(other.prefix);
        let id = self.id & ((1 << prefix) - 1);
        Self { id, prefix }
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ipv6 = Ipv6Addr::from(self.id.to_le_bytes());
        write!(f, "{}/{}", ipv6, self.prefix)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Node {
    path: Path,
    hash: u128,
    left: Tree,
    right: Tree,
}

type Tree = Option<Box<Node>>;

fn insert(tree: Tree, id: u128, hash: u128) -> Tree {
    let Some(mut node) = tree else {
        return Some(Box::new(Node {
            path: Path::leaf(id),
            hash,
            left: None,
            right: None,
        }));
    };

    let lca = node.path.lca(Path::leaf(id));
    if lca.is_leaf() {
        node.hash = hash;
        return Some(node);
    }

    if lca != node.path {
        let new_node = Box::new(Node {
            path: lca,
            hash: 0,
            left: None,
            right: None,
        });
        let old_node = std::mem::replace(&mut node, new_node);
        if old_node.path.id & (1 << lca.prefix) != 0 {
            node.right = Some(old_node);
        } else {
            node.left = Some(old_node);
        }
    }

    if id & (1 << lca.prefix) != 0 {
        node.right = insert(node.right, id, hash);
    } else {
        node.left = insert(node.left, id, hash);
    }

    let mut data = [0; 32];
    data[..16].copy_from_slice(&node.left.as_ref().unwrap().hash.to_le_bytes());
    data[16..].copy_from_slice(&node.right.as_ref().unwrap().hash.to_le_bytes());
    node.hash = xxhash_rust::xxh3::xxh3_128(&data);
    Some(node)
}

fn find(mut tree: &Tree, path: Path) -> Option<(Path, u128)> {
    loop {
        let Some(node) = tree else {
            return None;
        };

        if path.contains(node.path) {
            return Some((node.path, node.hash));
        }

        if !node.path.contains(path) {
            return None;
        }

        tree = if path.id & (1 << node.path.prefix) != 0 {
            &node.right
        } else {
            &node.left
        };
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Merkle {
    tree: Tree,
}

impl Merkle {
    pub const fn new() -> Self {
        Self { tree: None }
    }

    pub fn insert(&mut self, id: u128, data: &[u8]) {
        let mut xxh3 = xxhash_rust::xxh3::Xxh3::new();
        xxh3.write_u128(id);
        xxh3.write(data);
        let hash = xxh3.digest128();
        self.tree = insert(self.tree.take(), id, hash);
    }

    pub fn find(&self, path: Path) -> Option<(Path, u128)> {
        find(&self.tree, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_nodes(tree: &Merkle) -> usize {
        fn inner(tree: &Tree) -> usize {
            match tree {
                None => 0,
                Some(node) => 1 + inner(&node.left) + inner(&node.right),
            }
        }
        inner(&tree.tree)
    }

    fn tree_from_ints(ints: impl Iterator<Item = u128>) -> Merkle {
        let mut tree = Merkle::new();
        for i in ints {
            tree.insert(i, &i.to_le_bytes());
        }
        tree
    }

    #[test]
    fn tree_size() {
        for i in 1..100 {
            let t = tree_from_ints(0..i as _);
            assert_eq!(count_nodes(&t), 2 * i - 1);
        }
    }

    #[test]
    fn tree_size_rev() {
        for i in 1..100 {
            let t = tree_from_ints((0..i as _).rev());
            assert_eq!(count_nodes(&t), 2 * i - 1);
        }
    }

    #[test]
    fn test_shuffle_eq() {
        for i in 0..100 {
            let a = tree_from_ints(0..i);
            let b = tree_from_ints((0..i).rev());
            assert_eq!(a, b);
        }
    }
}
