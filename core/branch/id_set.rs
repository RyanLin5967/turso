//! The set of unreleased branch ids as a persistent trie: F-cat-snap (lane r11-diff-list, turso
//! 8c0945ebb / 421e2e125), ported to the durable store for the githost-shape lane.
//!
//! `BranchStore::ids` used to walk every branch state and sort the result while holding the store's
//! one mutex: Theta(N log N) under the lock that every fork, commit and read takes. Here the store keeps
//! the unreleased ids in a 32-way radix trie whose nodes are shared between versions (path copying;
//! Driscoll, Sarnak, Sleator and Tarjan, JCSS 1989; the lazy clone google/btree gives etcd and
//! CockroachDB). A listing clones the trie under the mutex -- one reference-count increment -- and
//! walks its private version after releasing it, in ascending id order, so nothing is sorted.
//!
//! The code is r11-diff-list's generic `PageMap` (insert, remove with pruning of emptied nodes, walk),
//! specialised to a set: a leaf holds a presence bit per id. Every node a walk visits holds an entry
//! (remove prunes, and the first root is only as tall as its first key needs), so a walk costs
//! O(entries), never more than 1 + 1/32 + ... nodes per entry.

use std::sync::Arc;

const BITS: u32 = 5;
const WIDTH: usize = 1 << BITS;
const MASK: u32 = WIDTH as u32 - 1;

#[derive(Clone)]
enum Node {
    Inner([Option<Arc<Node>>; WIDTH]),
    /// Bit `i` set: id `base | i` is present.
    Leaf(u32),
}

impl Node {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Node::Leaf(0)
        } else {
            Node::Inner(std::array::from_fn(|_| None))
        }
    }
}

/// Bytes of one stored node: the `ArcInner` holding it, two reference counts and the node (githost-shape r3).
pub(crate) const NODE_BYTES: u64 = (std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>()) as u64;

/// `(len, nodes, bytes)` of the set F-W1's first listing would build for exactly these ids (a fresh set, the
/// ids inserted in order): the census of `IdSet::census` without a store (githost-shape r3 instrument,
/// observation only).
#[doc(hidden)]
pub fn id_set_census(ids: impl IntoIterator<Item = u32>) -> (u64, u64, u64) {
    let mut set = IdSet::default();
    for id in ids {
        set.insert(id);
    }
    let (nodes, bytes) = set.census();
    (set.len(), nodes, bytes)
}

#[derive(Clone, Default)]
pub(crate) struct IdSet {
    root: Option<Arc<Node>>,
    /// Inner levels above the leaves: keys below `WIDTH^(height + 1)` are addressable.
    height: u32,
    len: u64,
}

/// What a walk touched (observation only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IdSetWork {
    pub(crate) nodes: u64,
    pub(crate) reported: u64,
}

impl IdSet {
    fn covers(&self, key: u32) -> bool {
        u64::from(key) < 1u64 << (BITS * (self.height + 1))
    }

    fn index(key: u32, level: u32) -> usize {
        ((key >> (BITS * level)) & MASK) as usize
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn contains(&self, key: u32) -> bool {
        let Some(mut node) = self.root.as_deref() else {
            return false;
        };
        if !self.covers(key) {
            return false;
        }
        let mut level = self.height;
        loop {
            match node {
                Node::Inner(kids) => match kids[Self::index(key, level)].as_deref() {
                    Some(kid) => {
                        node = kid;
                        level -= 1;
                    }
                    None => return false,
                },
                Node::Leaf(bits) => return bits & (1 << Self::index(key, 0)) != 0,
            }
        }
    }

    /// Add `key`. Every clone taken before this call keeps the set it had.
    pub(crate) fn insert(&mut self, key: u32) {
        if self.contains(key) {
            return;
        }
        if self.root.is_none() {
            // As tall as the first key needs: no empty leaf is left at kid 0 (r11-diff-list 68cb8dd59).
            self.height = 0;
            while !self.covers(key) {
                self.height += 1;
            }
            self.root = Some(Arc::new(Node::empty(self.height)));
        }
        while !self.covers(key) {
            let mut kids: [Option<Arc<Node>>; WIDTH] = std::array::from_fn(|_| None);
            kids[0] = self.root.take();
            self.root = Some(Arc::new(Node::Inner(kids)));
            self.height += 1;
        }
        let mut level = self.height;
        let mut node = Arc::make_mut(self.root.as_mut().expect("created above"));
        loop {
            node = match node {
                Node::Inner(kids) => {
                    let kid = kids[Self::index(key, level)]
                        .get_or_insert_with(|| Arc::new(Node::empty(level - 1)));
                    level -= 1;
                    Arc::make_mut(kid)
                }
                Node::Leaf(bits) => {
                    *bits |= 1 << Self::index(key, 0);
                    self.len += 1;
                    return;
                }
            };
        }
    }

    /// Remove `key` if present. A node left empty is dropped from its parent, and an empty root from
    /// the set, so every node a walk visits holds an entry.
    pub(crate) fn remove(&mut self, key: u32) {
        if !self.contains(key) {
            return;
        }
        let height = self.height;
        let root = self.root.as_mut().expect("the key is present");
        if remove_in(root, height, key) {
            self.root = None;
            self.height = 0;
        }
        self.len -= 1;
    }

    /// githost-shape r3 instrument (observation only; lead 2026-09-27, the round-12 scout's scale lens): the
    /// set's stored nodes, and their bytes as node allocations. Every node is one `ArcInner<Node>`, sized to the
    /// larger variant (`Inner`'s 32 kid pointers), so bytes = nodes x `NODE_BYTES`; the allocator's rounding is
    /// not counted. A walk, O(nodes): call it outside measured operations.
    pub(crate) fn census(&self) -> (u64, u64) {
        let mut work = IdSetWork::default();
        self.for_each(&mut work, &mut |_| {});
        (work.nodes, work.nodes * NODE_BYTES)
    }

    /// Call `f` on every key, ascending.
    pub(crate) fn for_each(&self, work: &mut IdSetWork, f: &mut impl FnMut(u32)) {
        if let Some(root) = &self.root {
            walk(root, self.height, 0, work, f);
        }
    }
}

fn remove_in(node: &mut Arc<Node>, level: u32, key: u32) -> bool {
    match Arc::make_mut(node) {
        Node::Leaf(bits) => {
            *bits &= !(1 << (key & MASK));
            *bits == 0
        }
        Node::Inner(kids) => {
            let i = ((key >> (BITS * level)) & MASK) as usize;
            let kid = kids[i].as_mut().expect("the key is under this kid");
            if remove_in(kid, level - 1, key) {
                kids[i] = None;
            }
            kids.iter().all(Option::is_none)
        }
    }
}

fn walk(node: &Node, level: u32, base: u32, work: &mut IdSetWork, f: &mut impl FnMut(u32)) {
    work.nodes += 1;
    match node {
        Node::Inner(kids) => {
            for (i, kid) in kids.iter().enumerate() {
                if let Some(kid) = kid {
                    walk(kid, level - 1, base | ((i as u32) << (BITS * level)), work, f);
                }
            }
        }
        Node::Leaf(bits) => {
            for i in 0..WIDTH as u32 {
                if bits & (1 << i) != 0 {
                    work.reported += 1;
                    f(base | i);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The census counts what is stored. 1,000 consecutive ids from 0: the first key makes a leaf root; key 32
    /// grows it to one inner root (height 1, covering keys below 32^2 = 1,024); so 32 leaves (31 full, one with
    /// 8 ids) under 1 inner node, 33 nodes, whatever the order. Removing ids 0..32 prunes the first leaf: 32.
    #[test]
    fn a_census_counts_leaves_and_inner_nodes() {
        let mut set = IdSet::default();
        for k in 0..1_000u32 {
            set.insert(k);
        }
        assert_eq!(set.census(), (33, 33 * NODE_BYTES));
        assert_eq!(id_set_census((0..1_000u32).rev()), (1_000, 33, 33 * NODE_BYTES));
        for k in 0..32u32 {
            set.remove(k);
        }
        assert_eq!(set.census(), (32, 32 * NODE_BYTES));
    }

    /// Against a BTreeSet model under random inserts and removes, with clones taken along the way that
    /// must keep their own contents (path copying), and a walk whose node count stays within the
    /// pruning bound.
    #[test]
    fn an_id_set_matches_a_model_and_its_clones_keep_their_contents() {
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut set = IdSet::default();
        let mut model = BTreeSet::new();
        let mut frozen: Vec<(IdSet, BTreeSet<u32>)> = Vec::new();
        for step in 0..20_000u32 {
            let key = (next() % 5_000) as u32 + if step % 7 == 0 { 1 << 20 } else { 0 };
            if next() % 3 == 0 {
                set.remove(key);
                model.remove(&key);
            } else {
                set.insert(key);
                model.insert(key);
            }
            if step % 2_500 == 0 {
                frozen.push((set.clone(), model.clone()));
            }
        }
        for (s, m) in frozen.iter().chain(std::iter::once(&(set.clone(), model.clone()))) {
            let mut got = Vec::new();
            let mut work = IdSetWork::default();
            s.for_each(&mut work, &mut |k| got.push(k));
            let want: Vec<u32> = m.iter().copied().collect();
            assert_eq!(got, want, "walk order and contents");
            assert_eq!(s.len(), m.len() as u64);
            assert_eq!(work.reported, m.len() as u64);
            // Leaves hold >= 1 entry each and every inner node >= 1 kid: nodes <= (height + 1) x entries.
            assert!(work.nodes <= (s.height as u64 + 1) * m.len().max(1) as u64);
            for k in 0..5_100u32 {
                assert_eq!(s.contains(k), m.contains(&k));
            }
        }
    }
}
