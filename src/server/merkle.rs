#[derive(Debug, Eq, PartialEq)]
struct Node {
    id: [u8; 16],
    prefix: u8,
    left: Tree,
    right: Tree,
    hash: u128,
}

type Tree = Option<Box<Node>>;

fn common_prefix(a: [u8; 16], b: [u8; 16]) -> u8 {
    let a = u128::from_le_bytes(a);
    let b = u128::from_le_bytes(b);
    let diff = a ^ b;
    diff.trailing_zeros() as u8
}

fn get_bit(id: [u8; 16], prefix: u8) -> bool {
    let idx = prefix / 8;
    let bit = prefix % 8;
    id[idx as usize] & (1 << bit) != 0
}

fn truncate(id: [u8; 16], prefix: u8) -> [u8; 16] {
    let id = u128::from_le_bytes(id);
    let mask = (1 << prefix) - 1;
    let id = id & mask;
    id.to_le_bytes()
}

fn insert(tree: Tree, id: [u8; 16], hash: u128) -> Tree {
    let Some(mut node) = tree else {
        return Some(Box::new(Node {
            id,
            prefix: 128,
            left: None,
            right: None,
            hash,
        }));
    };

    let cp = common_prefix(node.id, id);
    if cp == 128 && node.prefix == 128 {
        node.hash = hash;
        return Some(node);
    }

    if cp < node.prefix {
        let mut new_node = Box::new(Node {
            id: truncate(id, cp),
            prefix: cp,
            left: None,
            right: None,
            hash,
        });
        std::mem::swap(&mut new_node, &mut node);
        if get_bit(new_node.id, cp) {
            node.right = Some(new_node);
        } else {
            node.left = Some(new_node);
        }
    }

    if get_bit(id, node.prefix) {
        node.right = insert(node.right, id, hash);
    } else {
        node.left = insert(node.left, id, hash);
    }

    let mut data = [0; 48];
    data[..16].copy_from_slice(&node.id);
    data[16..32].copy_from_slice(&node.left.as_ref().unwrap().hash.to_le_bytes());
    data[32..].copy_from_slice(&node.right.as_ref().unwrap().hash.to_le_bytes());
    node.hash = xxhash_rust::xxh3::xxh3_128(&data);
    Some(node)
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Merkle {
    tree: Tree,
}

impl Merkle {
    pub(crate) const fn new() -> Self {
        Self { tree: None }
    }

    pub(crate) fn insert(&mut self, id: [u8; 16], data: &[u8]) {
        let hash = xxhash_rust::xxh3::xxh3_128(data);
        self.tree = insert(self.tree.take(), id, hash);
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
            let id = i.to_le_bytes();
            tree.insert(id, &id);
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
