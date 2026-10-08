//! A persistent map from page number to an arena slot and its checksum, for resolving a branch's
//! pages without walking its ancestor chain. (Round 10's F4 map, ported from turso `a31198dd8`; the
//! durable store checks a slot's crc32c on every read, so the map carries it beside the slot.)
//!
//! A 32-way radix trie over the page number whose nodes are shared between versions: a clone is one
//! reference-count increment, and an insert copies only the nodes on the path to its leaf (path
//! copying — Driscoll, Sarnak, Sleator and Tarjan, JCSS 1989; the shape of Bagwell's hash tries and
//! of copy-on-write B-trees). A lookup visits `height + 1` nodes: 2 for a database of up to 1,024
//! pages, and at most 7 for any `u32`. Nothing here depends on how many versions exist or on how
//! they descend from one another.
//!
//! The map only NAMES slots; it owns none. The store's retained-version bookkeeping decides how long
//! a slot lives, and it keeps every slot any live branch can see (see `store`), which is every slot a
//! live branch's map can hold.
//!
//! `std::sync::Arc` rather than `crate::sync::Arc`: the nodes are plain data that is only ever
//! touched under the branch store's mutex, so there is no interleaving for a model checker to
//! explore, and `Arc::make_mut` — which copies a node only while another version shares it — is
//! what makes an insert into an unshared map free of copies.

use std::sync::Arc;

use super::arena::Slot;

const BITS: u32 = 5;
const WIDTH: usize = 1 << BITS;
const MASK: u32 = WIDTH as u32 - 1;
/// No arena slot has this index ([`PageMap::insert`] refuses it); it marks an empty leaf entry.
const EMPTY: Slot = Slot::MAX;

/// A mapped page: its arena slot and the crc32c of the bytes committed there.
pub(crate) type Mapped = (Slot, u32);

#[derive(Clone)]
enum Node {
    Inner([Option<Arc<Node>>; WIDTH]),
    Leaf([Mapped; WIDTH]),
}

impl Node {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Node::Leaf([(EMPTY, 0); WIDTH])
        } else {
            Node::Inner(std::array::from_fn(|_| None))
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct PageMap {
    root: Option<Arc<Node>>,
    /// Inner levels above the leaves: pages below `WIDTH^(height + 1)` are addressable.
    height: u32,
}

impl PageMap {
    fn covers(&self, page: u32) -> bool {
        u64::from(page) < 1u64 << (BITS * (self.height + 1))
    }

    fn index(page: u32, level: u32) -> usize {
        ((page >> (BITS * level)) & MASK) as usize
    }

    pub(crate) fn get(&self, page: u32) -> Option<Mapped> {
        let mut node = self.root.as_deref()?;
        if !self.covers(page) {
            return None;
        }
        let mut level = self.height;
        loop {
            match node {
                Node::Inner(kids) => {
                    node = kids[Self::index(page, level)].as_deref()?;
                    level -= 1;
                }
                Node::Leaf(slots) => {
                    let mapped = slots[Self::index(page, 0)];
                    return (mapped.0 != EMPTY).then_some(mapped);
                }
            }
        }
    }

    /// Map `page` to `mapped`, replacing any previous mapping. Every other version of this map —
    /// every clone taken before this call — keeps the mapping it had.
    pub(crate) fn insert(&mut self, page: u32, mapped: Mapped) {
        crate::turso_assert!(
            mapped.0 != EMPTY,
            "arena slot u32::MAX is the page map's empty marker"
        );
        if self.root.is_none() {
            // The first root is as tall as its first page needs. Starting from a leaf and lifting it
            // would leave that leaf, empty, at kid 0: never read by a lookup, but a node no entry
            // needs (r11-diff-list's fix, turso 68cb8dd59, ported).
            self.height = 0;
            while !self.covers(page) {
                self.height += 1;
            }
            self.root = Some(Arc::new(Node::empty(self.height)));
        }
        while !self.covers(page) {
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
                    let kid = kids[Self::index(page, level)]
                        .get_or_insert_with(|| Arc::new(Node::empty(level - 1)));
                    level -= 1;
                    Arc::make_mut(kid)
                }
                Node::Leaf(slots) => {
                    slots[Self::index(page, 0)] = mapped;
                    return;
                }
            };
        }
    }

    /// Every mapped page number, ascending: a walk of the map, O(nodes). The Merger reads it to
    /// derive a stack top's write set (r13-compose A6.1: a top sees its levels' pages through here).
    pub(crate) fn pages(&self) -> Vec<u32> {
        fn walk(node: &Node, level: u32, prefix: u32, out: &mut Vec<u32>) {
            match node {
                Node::Inner(kids) => {
                    for (i, kid) in kids.iter().enumerate() {
                        if let Some(kid) = kid {
                            let p = prefix | ((i as u32) << (BITS * level));
                            walk(kid, level - 1, p, out);
                        }
                    }
                }
                Node::Leaf(slots) => {
                    for (i, m) in slots.iter().enumerate() {
                        if m.0 != EMPTY {
                            out.push(prefix | i as u32);
                        }
                    }
                }
            }
        }
        let mut out = Vec::new();
        if let Some(root) = self.root.as_deref() {
            walk(root, self.height, 0, &mut out);
        }
        out
    }
}

#[cfg(test)]
impl PageMap {
    /// (nodes reachable from the root, pages mapped).
    fn census(&self) -> (u64, u64) {
        fn walk(node: &Node) -> (u64, u64) {
            match node {
                Node::Inner(kids) => kids.iter().flatten().fold((1, 0), |(n, e), kid| {
                    let (kn, ke) = walk(kid);
                    (n + kn, e + ke)
                }),
                Node::Leaf(slots) => (1, slots.iter().filter(|m| m.0 != EMPTY).count() as u64),
            }
        }
        self.root.as_deref().map_or((0, 0), walk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// Many versions, each derived from a random earlier one by a clone and some inserts, against
    /// a plain `HashMap` copied at every clone. Keys span small pages and the whole `u32` range, so
    /// the root grows under shared versions.
    #[test]
    fn every_version_keeps_its_own_mappings_under_clones_and_inserts() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut versions: Vec<(PageMap, HashMap<u32, Mapped>)> = vec![Default::default()];
        for step in 0..3000u32 {
            let from = (rng.next() % versions.len() as u64) as usize;
            let (mut map, mut model) = versions[from].clone();
            for _ in 0..(rng.next() % 4) {
                let page = match rng.next() % 3 {
                    0 => (rng.next() % 64) as u32,
                    1 => (rng.next() % 5000) as u32,
                    _ => (rng.next() % u64::from(u32::MAX)) as u32,
                };
                map.insert(page, (step, step ^ 0xA5A5));
                model.insert(page, (step, step ^ 0xA5A5));
            }
            versions.push((map, model));
        }
        for (i, (map, model)) in versions.iter().enumerate() {
            // Every node holds at least one mapping, so no version keeps a node no entry needs.
            let (nodes, entries) = map.census();
            assert_eq!(entries, model.len() as u64, "version {i}: census disagrees with the model");
            assert!(
                nodes <= (u64::from(map.height) + 1) * entries,
                "version {i}: {nodes} nodes for {entries} pages at height {}",
                map.height
            );
            for (&page, &slot) in model {
                assert_eq!(map.get(page), Some(slot), "version {i} page {page}");
            }
            for page in [0, 1, 31, 32, 1023, 1024, 4999, u32::MAX - 1] {
                assert_eq!(map.get(page), model.get(&page).copied(), "version {i} page {page}");
            }
        }
    }

    #[test]
    fn an_empty_map_and_an_unaddressable_page_read_as_absent() {
        let mut map = PageMap::default();
        assert_eq!(map.get(0), None);
        map.insert(3, (7, 70));
        assert_eq!(map.get(3), Some((7, 70)));
        assert_eq!(map.get(4), None);
        assert_eq!(map.get(1 << 20), None, "a page beyond the root's reach");
        let mut tall = PageMap::default();
        tall.insert(1000, (9, 90));
        assert_eq!(tall.census(), (2, 1), "a first page >= 32 left a node no entry needs");
    }
}
