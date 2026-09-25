//! A persistent map from page number to arena slot, for resolving a branch's pages without walking
//! its ancestor chain.
//!
//! A 32-way radix trie over the page number whose nodes are shared between versions: a clone is one
//! reference-count increment, and an insert copies only the nodes on the path to its leaf (path
//! copying — Driscoll, Sarnak, Sleator and Tarjan, JCSS 1989; the shape of Bagwell's hash tries and
//! of copy-on-write B-trees). A lookup visits `height + 1` nodes: 2 for a database of up to 1,024
//! pages, and at most 7 for any `u32`. Nothing here depends on how many versions exist or on how
//! they descend from one another.
//!
//! The maps OWN the branch slots they name, by count: each slot counts the leaf nodes that name it
//! (see [`PageMap::set`] and [`PageMap::release`]), and it is freed when the last one goes. So a
//! branch slot lives exactly as long as some map can reach it. Trunk-retained slots never appear in
//! a map; the trunk's interval reclamation keeps those (see `store`).
//!
//! `std::sync::Arc` rather than `crate::sync::Arc`: the nodes are plain data that is only ever
//! touched under the branch store's mutex, so there is no interleaving for a model checker to
//! explore, and `Arc::make_mut` — which copies a node only while another version shares it — is
//! what makes an insert into an unshared map free of copies.

use std::sync::Arc;

use super::arena::{Arena, Slot};

/// Where a counted map reports the references its leaves take and drop (see [`PageMap::set`]).
pub(crate) trait SlotRefs {
    fn incref(&mut self, slot: Slot);
    /// Returns whether that was the slot's last reference (it is freed).
    fn decref(&mut self, slot: Slot) -> bool;
}

impl SlotRefs for Arena {
    fn incref(&mut self, slot: Slot) {
        Arena::incref(self, slot)
    }
    fn decref(&mut self, slot: Slot) -> bool {
        Arena::decref(self, slot)
    }
}

/// An uncounted map (the unit tests below).
#[cfg(test)]
struct NoRefs;

#[cfg(test)]
impl SlotRefs for NoRefs {
    fn incref(&mut self, _: Slot) {}
    fn decref(&mut self, _: Slot) -> bool {
        false
    }
}

/// Observation only: what a counted map operation did.
#[derive(Default)]
pub(crate) struct MapWork {
    /// Nodes cloned because another map shared them.
    pub(crate) nodes_copied: u64,
    /// References taken: one per slot of each cloned leaf, one per new entry.
    pub(crate) slot_increfs: u64,
    /// Slots whose last reference this operation dropped (freed).
    pub(crate) freed: u64,
}

/// `Arc::make_mut` that counts: a leaf cloned because another map shares it is one more node naming
/// each of its slots.
fn unique<'a>(node: &'a mut Arc<Node>, refs: &mut impl SlotRefs, w: &mut MapWork) -> &'a mut Node {
    let shared = Arc::get_mut(node).is_none();
    let node = Arc::make_mut(node);
    if shared {
        w.nodes_copied += 1;
        if let Node::Leaf(slots) = node {
            for &slot in slots.iter().filter(|&&s| s != EMPTY) {
                refs.incref(slot);
                w.slot_increfs += 1;
            }
        }
    }
    node
}

const BITS: u32 = 5;
const WIDTH: usize = 1 << BITS;
const MASK: u32 = WIDTH as u32 - 1;
/// No arena slot has this index ([`PageMap::set`] refuses it); it marks an empty leaf entry.
const EMPTY: Slot = Slot::MAX;

#[derive(Clone)]
enum Node {
    Inner([Option<Arc<Node>>; WIDTH]),
    Leaf([Slot; WIDTH]),
}

impl Node {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Node::Leaf([EMPTY; WIDTH])
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

    pub(crate) fn get(&self, page: u32) -> Option<Slot> {
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
                    let slot = slots[Self::index(page, 0)];
                    return (slot != EMPTY).then_some(slot);
                }
            }
        }
    }

    /// Every slot the map names, for accounting checks (a node shared with another map is
    /// visited once per map that holds it).
    #[cfg(test)]
    pub(crate) fn slots(&self) -> Vec<Slot> {
        let mut out = Vec::new();
        let mut stack: Vec<&Node> = self.root.as_deref().into_iter().collect();
        while let Some(node) = stack.pop() {
            match node {
                Node::Inner(kids) => stack.extend(kids.iter().flatten().map(|k| &**k)),
                Node::Leaf(slots) => out.extend(slots.iter().copied().filter(|&s| s != EMPTY)),
            }
        }
        out
    }

    /// Uncounted [`PageMap::set`], for the unit tests. Returns the nodes it cloned.
    #[cfg(test)]
    pub(crate) fn insert(&mut self, page: u32, slot: Slot) -> u64 {
        let mut w = MapWork::default();
        self.set(page, slot, &mut NoRefs, &mut w);
        w.nodes_copied
    }

    /// Map `page` to `slot`, replacing any previous mapping. Every other version of this map —
    /// every clone taken before this call — keeps the mapping it had.
    ///
    /// Reference counts (Rodeh, "B-trees, Shadowing, and Clones", ACM TOS 2008, as btrfs counts
    /// shadowed tree blocks): each slot is counted once per LEAF NODE that names it, not per map,
    /// so a clone of a whole map costs nothing. Copying a shared leaf takes a reference to each of
    /// its slots; replacing an entry moves one; [`PageMap::release`] drops the references of the
    /// nodes only this map held. Setting an entry to the slot it already holds changes no count and
    /// only makes the path to it this map's own.
    pub(crate) fn set(&mut self, page: u32, slot: Slot, refs: &mut impl SlotRefs, w: &mut MapWork) {
        crate::turso_assert!(slot != EMPTY, "arena slot u32::MAX is the page map's empty marker");
        if self.root.is_none() {
            self.root = Some(Arc::new(Node::empty(0)));
            self.height = 0;
        }
        while !self.covers(page) {
            let mut kids: [Option<Arc<Node>>; WIDTH] = std::array::from_fn(|_| None);
            kids[0] = self.root.take();
            self.root = Some(Arc::new(Node::Inner(kids)));
            self.height += 1;
        }
        let mut level = self.height;
        let mut node = unique(self.root.as_mut().expect("created above"), refs, w);
        loop {
            node = match node {
                Node::Inner(kids) => {
                    let kid = kids[Self::index(page, level)]
                        .get_or_insert_with(|| Arc::new(Node::empty(level - 1)));
                    level -= 1;
                    unique(kid, refs, w)
                }
                Node::Leaf(slots) => {
                    let old = std::mem::replace(&mut slots[Self::index(page, 0)], slot);
                    if old != slot {
                        refs.incref(slot);
                        w.slot_increfs += 1;
                        if old != EMPTY && refs.decref(old) {
                            w.freed += 1;
                        }
                    }
                    return;
                }
            };
        }
    }

    /// Drop this map, and the references of every node it alone held; a node another map still
    /// holds is left to that map. Visits only this map's own nodes, iteratively.
    pub(crate) fn release(&mut self, refs: &mut impl SlotRefs, w: &mut MapWork) {
        let mut stack: Vec<Arc<Node>> = self.root.take().into_iter().collect();
        while let Some(node) = stack.pop() {
            match Arc::try_unwrap(node) {
                Ok(Node::Inner(kids)) => stack.extend(kids.into_iter().flatten()),
                Ok(Node::Leaf(slots)) => {
                    for slot in slots.into_iter().filter(|&s| s != EMPTY) {
                        if refs.decref(slot) {
                            w.freed += 1;
                        }
                    }
                }
                Err(_shared) => {}
            }
        }
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
        let mut versions: Vec<(PageMap, HashMap<u32, Slot>)> = vec![Default::default()];
        for step in 0..3000u32 {
            let from = (rng.next() % versions.len() as u64) as usize;
            let (mut map, mut model) = versions[from].clone();
            for _ in 0..(rng.next() % 4) {
                let page = match rng.next() % 3 {
                    0 => (rng.next() % 64) as u32,
                    1 => (rng.next() % 5000) as u32,
                    _ => (rng.next() % u64::from(u32::MAX)) as u32,
                };
                map.insert(page, step);
                model.insert(page, step);
            }
            versions.push((map, model));
        }
        for (i, (map, model)) in versions.iter().enumerate() {
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
        map.insert(3, 7);
        assert_eq!(map.get(3), Some(7));
        assert_eq!(map.get(4), None);
        assert_eq!(map.get(1 << 20), None, "a page beyond the root's reach");
    }
}
