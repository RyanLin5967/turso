//! A persistent map from page number to arena slot: every arena page one branch reads.
//!
//! A 32-way radix trie over the page number whose nodes are shared between versions: a clone is one
//! reference-count increment, and an insert copies only the nodes on the path to its leaf that
//! another version shares (path copying — Driscoll, Sarnak, Sleator and Tarjan, JCSS 1989; the
//! shape of Bagwell's hash tries and of copy-on-write B-trees). A lookup visits `height + 1` nodes:
//! 2 for a database of up to 1,024 pages, and at most 7 for any `u32`. Nothing here depends on how
//! many versions exist or on how they descend from one another.
//!
//! # Who owns a slot: reference-counted shadowing
//!
//! The maps OWN the slots they name, collectively, the way btrfs's copy-on-write B-trees own their
//! extents (Rodeh, "B-trees, Shadowing, and Clones", ACM TOS 2008). The store keeps `refs`: for each
//! slot, how many leaf NODES name it. A node shared by many maps counts once, and the node's own
//! `Arc` count says how many maps (or parent nodes) share it. So:
//!
//! * a clone (a fork) touches no count but the root's `Arc`;
//! * copying a shared leaf on the way to an insert names each of its slots once more
//!   ([`PageMap::insert_counted`]), and the entry the insert replaces is named once less;
//! * releasing a map ([`PageMap::release`]) walks only the nodes it alone holds; each leaf among them
//!   names its slots once less, and a slot whose count reaches 0 is reachable from no map at all.
//!
//! A slot is therefore free exactly when no live map can reach it, and a write may overwrite a slot
//! in place exactly when [`PageMap::exclusive`] holds and its count is 1: no other map can reach it.
//!
//! `std::sync::Arc` rather than `crate::sync::Arc`: the nodes are plain data that is only ever
//! touched under the branch store's mutex, so there is no interleaving for a model checker to
//! explore, and `Arc::get_mut` — which succeeds only while no other version shares a node — is
//! what tells a copy from an in-place update.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::arena::Slot;

const BITS: u32 = 5;
const WIDTH: usize = 1 << BITS;
const MASK: u32 = WIDTH as u32 - 1;
/// No arena slot has this index ([`PageMap::insert_counted`] refuses it); it marks an empty leaf
/// entry.
const EMPTY: Slot = Slot::MAX;

/// Trie nodes alive in this process, across every map of every database. Observation only
/// ([`live_nodes`]): counted where a node is built or copied and where it is dropped, so a node
/// that many maps share counts once. Nothing in the mechanism reads it.
static LIVE_NODES: AtomicUsize = AtomicUsize::new(0);

/// The number of trie nodes alive in this process (see [`LIVE_NODES`]).
pub(crate) fn live_nodes() -> usize {
    LIVE_NODES.load(Ordering::Relaxed)
}

enum Node {
    Inner([Option<Arc<Node>>; WIDTH]),
    Leaf([Slot; WIDTH]),
}

impl Node {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Self::counted(Node::Leaf([EMPTY; WIDTH]))
        } else {
            Self::counted(Node::Inner(std::array::from_fn(|_| None)))
        }
    }

    /// Every node is built through here, so that [`LIVE_NODES`] sees it.
    fn counted(node: Node) -> Self {
        LIVE_NODES.fetch_add(1, Ordering::Relaxed);
        node
    }
}

/// A shared node is copied through this before an insert changes it.
impl Clone for Node {
    fn clone(&self) -> Self {
        Self::counted(match self {
            Node::Inner(kids) => Node::Inner(kids.clone()),
            Node::Leaf(slots) => Node::Leaf(*slots),
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        LIVE_NODES.fetch_sub(1, Ordering::Relaxed);
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

    /// True if no other map shares any node on the path to `page`'s leaf. With the slot's count
    /// at 1 it means no other map can reach the slot `page` names here.
    pub(crate) fn exclusive(&self, page: u32) -> bool {
        let Some(mut arc) = self.root.as_ref() else {
            return false;
        };
        if !self.covers(page) {
            return false;
        }
        let mut level = self.height;
        loop {
            if Arc::strong_count(arc) != 1 {
                return false;
            }
            match &**arc {
                Node::Inner(kids) => {
                    let Some(kid) = kids[Self::index(page, level)].as_ref() else {
                        return false;
                    };
                    arc = kid;
                    level -= 1;
                }
                Node::Leaf(_) => return true,
            }
        }
    }

    /// Make `arc` this map's own node, copying it if another version shares it. A copied leaf
    /// names each of its slots once more.
    fn unshare<'a>(arc: &'a mut Arc<Node>, refs: &mut [u32]) -> &'a mut Node {
        if Arc::get_mut(arc).is_none() {
            let copy = (**arc).clone();
            if let Node::Leaf(slots) = &copy {
                for &slot in slots.iter().filter(|&&s| s != EMPTY) {
                    refs[slot as usize] += 1;
                }
            }
            *arc = Arc::new(copy);
        }
        Arc::get_mut(arc).expect("just made this map's own")
    }

    /// Map `page` to `slot` in this map only, keeping `refs` exact (see the module doc). Every other
    /// version of this map — every clone taken before this call — keeps the mapping it had. Returns
    /// the slot the entry named before if no leaf names it any more, for the caller to free.
    pub(crate) fn insert_counted(&mut self, page: u32, slot: Slot, refs: &mut [u32]) -> Option<Slot> {
        crate::turso_assert!(slot != EMPTY, "arena slot u32::MAX is the page map's empty marker");
        if self.root.is_none() {
            self.root = Some(Arc::new(Node::empty(0)));
            self.height = 0;
        }
        while !self.covers(page) {
            let mut kids: [Option<Arc<Node>>; WIDTH] = std::array::from_fn(|_| None);
            kids[0] = self.root.take();
            self.root = Some(Arc::new(Node::counted(Node::Inner(kids))));
            self.height += 1;
        }
        let mut level = self.height;
        let mut node = Self::unshare(self.root.as_mut().expect("created above"), refs);
        loop {
            node = match node {
                Node::Inner(kids) => {
                    let kid = kids[Self::index(page, level)]
                        .get_or_insert_with(|| Arc::new(Node::empty(level - 1)));
                    level -= 1;
                    Self::unshare(kid, refs)
                }
                Node::Leaf(slots) => {
                    let old = std::mem::replace(&mut slots[Self::index(page, 0)], slot);
                    refs[slot as usize] += 1;
                    if old == EMPTY {
                        return None;
                    }
                    refs[old as usize] -= 1;
                    return (refs[old as usize] == 0).then_some(old);
                }
            };
        }
    }

    /// Give up this map. The nodes it alone holds are dropped; each leaf among them names its
    /// slots once less, and `free` gets every slot no leaf names any more. Nodes another map shares
    /// only lose this map's reference. The cost is the nodes this map alone holds, times 32.
    pub(crate) fn release(mut self, refs: &mut [u32], free: &mut impl FnMut(Slot)) {
        fn release_node(arc: Arc<Node>, refs: &mut [u32], free: &mut impl FnMut(Slot)) {
            let Ok(mut node) = Arc::try_unwrap(arc) else {
                return;
            };
            match &mut node {
                Node::Inner(kids) => {
                    for kid in kids.iter_mut() {
                        if let Some(kid) = kid.take() {
                            release_node(kid, refs, free);
                        }
                    }
                }
                Node::Leaf(slots) => {
                    for &slot in slots.iter().filter(|&&s| s != EMPTY) {
                        refs[slot as usize] -= 1;
                        if refs[slot as usize] == 0 {
                            free(slot);
                        }
                    }
                }
            }
        }
        if let Some(root) = self.root.take() {
            release_node(root, refs, free);
        }
    }

    /// Call `f(page, slot)` for every mapping.
    pub(crate) fn for_each(&self, mut f: impl FnMut(u32, Slot)) {
        fn walk(node: &Node, level: u32, base: u32, f: &mut impl FnMut(u32, Slot)) {
            match node {
                Node::Inner(kids) => {
                    for (i, kid) in kids.iter().enumerate() {
                        if let Some(kid) = kid {
                            walk(kid, level - 1, base | ((i as u32) << (BITS * level)), f);
                        }
                    }
                }
                Node::Leaf(slots) => {
                    for (i, &slot) in slots.iter().enumerate() {
                        if slot != EMPTY {
                            f(base | i as u32, slot);
                        }
                    }
                }
            }
        }
        if let Some(root) = self.root.as_deref() {
            walk(root, self.height, 0, &mut f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// Many versions, each derived from a random earlier one by a clone and some inserts, and
    /// released in random order, against a plain `HashMap` copied at every clone. Keys span small
    /// pages and the whole `u32` range, so the root grows under shared versions. After every step:
    /// every live version reads exactly its model, `for_each` lists exactly it, and a slot has been
    /// freed exactly when no live version's model names it any more (refcounts neither leak nor
    /// free early).
    #[test]
    fn every_version_keeps_its_own_mappings_and_a_slot_dies_with_its_last_reader() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut refs = vec![0u32; 5_000];
        let mut next_slot: Slot = 0;
        let mut freed: HashSet<Slot> = HashSet::new();
        let mut versions: Vec<Option<(PageMap, HashMap<u32, Slot>)>> = vec![Some(Default::default())];
        for step in 0..1500u32 {
            let live: Vec<usize> = (0..versions.len()).filter(|&i| versions[i].is_some()).collect();
            if rng.next() % 3 == 0 && live.len() > 1 {
                let i = live[(rng.next() % live.len() as u64) as usize];
                let (map, _) = versions[i].take().unwrap();
                map.release(&mut refs, &mut |slot| {
                    assert!(freed.insert(slot), "step {step}: slot {slot} freed twice");
                });
            } else {
                let from = live[(rng.next() % live.len() as u64) as usize];
                let (mut map, mut model) = versions[from].clone().unwrap();
                for _ in 0..(rng.next() % 4) {
                    let page = match rng.next() % 3 {
                        0 => (rng.next() % 64) as u32,
                        1 => (rng.next() % 5000) as u32,
                        _ => (rng.next() % u64::from(u32::MAX)) as u32,
                    };
                    let slot = next_slot;
                    next_slot += 1;
                    if let Some(dead) = map.insert_counted(page, slot, &mut refs) {
                        assert!(freed.insert(dead), "step {step}: slot {dead} freed twice");
                    }
                    model.insert(page, slot);
                }
                versions.push(Some((map, model)));
            }
            let mut reachable = HashSet::new();
            for (map, model) in versions.iter().flatten() {
                reachable.extend(model.values().copied());
                for (&page, &slot) in model {
                    assert_eq!(map.get(page), Some(slot), "step {step} page {page}");
                }
                let mut listed = HashMap::new();
                map.for_each(|page, slot| assert!(listed.insert(page, slot).is_none()));
                assert_eq!(&listed, model, "step {step}: for_each disagrees with the model");
            }
            for slot in 0..next_slot {
                assert_eq!(
                    freed.contains(&slot),
                    !reachable.contains(&slot),
                    "step {step}: slot {slot} freed {} but reachable {}",
                    freed.contains(&slot),
                    reachable.contains(&slot)
                );
            }
        }
        for (map, _) in versions.into_iter().flatten() {
            map.release(&mut refs, &mut |slot| assert!(freed.insert(slot)));
        }
        assert_eq!(freed.len(), next_slot as usize, "a slot outlived every map");
        assert!(refs.iter().all(|&r| r == 0), "a count is left over");
    }

    /// `exclusive` holds for a map's own path and fails for any path a clone shares, and an
    /// insert through a shared path makes it exclusive again for the writer only.
    #[test]
    fn a_path_is_exclusive_until_a_clone_shares_it() {
        let mut refs = vec![0u32; 16];
        let mut a = PageMap::default();
        assert!(!a.exclusive(3), "an unmapped page is not exclusively mapped");
        assert!(a.insert_counted(3, 0, &mut refs).is_none());
        assert!(a.insert_counted(700, 1, &mut refs).is_none());
        assert!(a.exclusive(3) && a.exclusive(700));
        let mut b = a.clone();
        assert!(!a.exclusive(3) && !b.exclusive(3), "a clone shares the whole map");
        assert_eq!(b.insert_counted(3, 2, &mut refs), None, "a still names slot 0");
        assert!(b.exclusive(3), "b's copied path is b's own");
        assert!(!b.exclusive(700), "b still shares page 700's leaf with a");
        assert_eq!(refs[0], 1, "only a's leaf names slot 0");
        assert_eq!(a.get(3), Some(0));
        assert_eq!(b.get(3), Some(2));
        let mut freed = Vec::new();
        a.release(&mut refs, &mut |s| freed.push(s));
        assert_eq!(freed, vec![0], "slot 1 is still b's");
        assert!(b.exclusive(700), "a's release left b the only holder");
        b.release(&mut refs, &mut |s| freed.push(s));
        freed.sort();
        assert_eq!(freed, vec![0, 1, 2]);
    }

    #[test]
    fn an_empty_map_and_an_unaddressable_page_read_as_absent() {
        let mut refs = vec![0u32; 8];
        let mut map = PageMap::default();
        assert_eq!(map.get(0), None);
        map.insert_counted(3, 7, &mut refs);
        assert_eq!(map.get(3), Some(7));
        assert_eq!(map.get(4), None);
        assert_eq!(map.get(1 << 20), None, "a page beyond the root's reach");
        map.release(&mut refs, &mut |_| {});
    }
}
