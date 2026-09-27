//! A persistent radix trie over `u64` keys whose every node carries a BIRTH: the greatest sequence
//! number of any change beneath it.
//!
//! This is the shape ZFS gives a dataset's block tree — every block pointer records the txg its
//! subtree last changed in — and it buys two things at once:
//!
//! * a snapshot is one reference-count increment on the root, because a write copies only the
//!   nodes on its path (path copying, as in [`super::page_map`]); and
//! * "everything that changed after sequence number `base`" is a walk that skips every subtree
//!   whose birth is `<= base`, so it visits the changed items and their ancestors, not the tree.
//!
//! A removed key can be left as a HOLE that keeps its birth (ZFS's hole birth), so that a walk
//! from an older base still sees that something was deleted there. A change a receiver does not need
//! to see (it can derive it) is written without raising births.
//!
//! Nodes are sparse — a node stores only its present children, in key order — so a tree of a few
//! keys costs a few small nodes, not 32-slot arrays.

use std::sync::Arc;

const BITS: u32 = 5;
const MASK: u64 = (1 << BITS) - 1;
/// Levels above the leaves needed for any `u64` key: 13 levels of 5 bits cover 65 bits.
const MAX_HEIGHT: u32 = 12;

pub(crate) enum Item<V> {
    Live { birth: u64, val: Arc<V> },
    /// A key that was removed at `birth`; `born` is what the caller recorded about it (for a
    /// branch state, the sequence number it was forked at).
    Hole { birth: u64, born: u64 },
}

impl<V> Clone for Item<V> {
    fn clone(&self) -> Self {
        match self {
            Item::Live { birth, val } => Item::Live {
                birth: *birth,
                val: val.clone(),
            },
            Item::Hole { birth, born } => Item::Hole {
                birth: *birth,
                born: *born,
            },
        }
    }
}

impl<V> Item<V> {
    pub(crate) fn birth(&self) -> u64 {
        match self {
            Item::Live { birth, .. } | Item::Hole { birth, .. } => *birth,
        }
    }

    pub(crate) fn live(&self) -> Option<&Arc<V>> {
        match self {
            Item::Live { val, .. } => Some(val),
            Item::Hole { .. } => None,
        }
    }
}

enum Node<V> {
    Inner {
        birth: u64,
        kids: Vec<(u8, Arc<Node<V>>)>,
    },
    Leaf {
        birth: u64,
        items: Vec<(u8, Item<V>)>,
    },
}

impl<V> Clone for Node<V> {
    fn clone(&self) -> Self {
        match self {
            Node::Inner { birth, kids } => Node::Inner {
                birth: *birth,
                kids: kids.clone(),
            },
            Node::Leaf { birth, items } => Node::Leaf {
                birth: *birth,
                items: items.clone(),
            },
        }
    }
}

impl<V> Node<V> {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Node::Leaf {
                birth: 0,
                items: Vec::new(),
            }
        } else {
            Node::Inner {
                birth: 0,
                kids: Vec::new(),
            }
        }
    }

    fn birth(&self) -> u64 {
        match self {
            Node::Inner { birth, .. } | Node::Leaf { birth, .. } => *birth,
        }
    }

    fn raise(&mut self, seq: u64) {
        let (Node::Inner { birth, .. } | Node::Leaf { birth, .. }) = self;
        if seq > *birth {
            *birth = seq;
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Node::Inner { kids, .. } => kids.is_empty(),
            Node::Leaf { items, .. } => items.is_empty(),
        }
    }
}

/// Work a tree operation did: nodes entered by a walk, items looked at, nodes a write went
/// through, and nodes a write had to COPY because a snapshot still shared them.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TreeWork {
    pub(crate) nodes_visited: u64,
    pub(crate) items_checked: u64,
    pub(crate) nodes_touched: u64,
    pub(crate) nodes_copied: u64,
}

impl TreeWork {
    pub(crate) fn add(&mut self, o: TreeWork) {
        self.nodes_visited += o.nodes_visited;
        self.items_checked += o.items_checked;
        self.nodes_touched += o.nodes_touched;
        self.nodes_copied += o.nodes_copied;
    }
}

pub(crate) struct BirthTree<V> {
    root: Option<Arc<Node<V>>>,
    /// Inner levels above the leaves: keys below `32^(height + 1)` are addressable.
    height: u32,
}

impl<V> Clone for BirthTree<V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            height: self.height,
        }
    }
}

impl<V> Default for BirthTree<V> {
    fn default() -> Self {
        Self {
            root: None,
            height: 0,
        }
    }
}

fn index(key: u64, level: u32) -> u8 {
    ((key >> (BITS * level)) & MASK) as u8
}

/// A node about to be written: copied first if anything else (a snapshot) still shares it.
fn cow<'a, V>(node: &'a mut Arc<Node<V>>, work: &mut TreeWork) -> &'a mut Node<V> {
    work.nodes_touched += 1;
    if Arc::strong_count(node) > 1 {
        work.nodes_copied += 1;
    }
    Arc::make_mut(node)
}

impl<V> BirthTree<V> {
    fn covers(&self, key: u64) -> bool {
        self.height >= MAX_HEIGHT || key < 1u64 << (BITS * (self.height + 1))
    }

    #[cfg(test)]
    pub(crate) fn height(&self) -> u32 {
        self.height
    }

    pub(crate) fn get(&self, key: u64) -> Option<&Item<V>> {
        if !self.covers(key) {
            return None;
        }
        let mut node = self.root.as_deref()?;
        let mut level = self.height;
        loop {
            match node {
                Node::Inner { kids, .. } => {
                    let i = index(key, level);
                    let pos = kids.binary_search_by_key(&i, |(k, _)| *k).ok()?;
                    node = &kids[pos].1;
                    level -= 1;
                }
                Node::Leaf { items, .. } => {
                    let i = index(key, 0);
                    let pos = items.binary_search_by_key(&i, |(k, _)| *k).ok()?;
                    return Some(&items[pos].1);
                }
            }
        }
    }

    pub(crate) fn get_live(&self, key: u64) -> Option<&Arc<V>> {
        self.get(key).and_then(Item::live)
    }

    /// Put `item` at `key`. With `bump`, every node on the path gets a birth of at least that
    /// sequence number (a change a walk from an older base must find); without it births stay as
    /// they were (a change a receiver derives for itself).
    pub(crate) fn put(&mut self, key: u64, item: Item<V>, bump: Option<u64>, work: &mut TreeWork) {
        if self.root.is_none() {
            self.root = Some(Arc::new(Node::empty(0)));
            self.height = 0;
        }
        while !self.covers(key) {
            let old = self.root.take().expect("created above");
            let birth = old.birth();
            self.root = Some(Arc::new(Node::Inner {
                birth,
                kids: vec![(0, old)],
            }));
            self.height += 1;
        }
        // Mutant S2 (PREREG A5): inner nodes keep their births, so a walk prunes changed subtrees.
        let keep_inner_births = super::ship_mutant() == "S2";
        let mut level = self.height;
        let mut node = cow(self.root.as_mut().expect("created above"), work);
        loop {
            if let Some(seq) = bump {
                if level == 0 || !keep_inner_births {
                    node.raise(seq);
                }
            }
            match node {
                Node::Inner { kids, .. } => {
                    let i = index(key, level);
                    let pos = match kids.binary_search_by_key(&i, |(k, _)| *k) {
                        Ok(pos) => pos,
                        Err(pos) => {
                            kids.insert(pos, (i, Arc::new(Node::empty(level - 1))));
                            pos
                        }
                    };
                    level -= 1;
                    node = cow(&mut kids[pos].1, work);
                }
                Node::Leaf { items, .. } => {
                    let i = index(key, 0);
                    match items.binary_search_by_key(&i, |(k, _)| *k) {
                        Ok(pos) => items[pos].1 = item,
                        Err(pos) => items.insert(pos, (i, item)),
                    }
                    return;
                }
            }
        }
    }

    /// Remove `key` outright (no hole, no birth change), pruning nodes it leaves empty. Returns
    /// the item that was there.
    pub(crate) fn remove(&mut self, key: u64, work: &mut TreeWork) -> Option<Item<V>> {
        if !self.covers(key) {
            return None;
        }
        self.get(key)?;
        let root = self.root.as_mut()?;
        let removed = Self::remove_in(root, key, self.height, work);
        if self.root.as_ref().is_some_and(|r| r.is_empty()) {
            self.root = None;
            self.height = 0;
        }
        removed
    }

    fn remove_in(node: &mut Arc<Node<V>>, key: u64, level: u32, work: &mut TreeWork) -> Option<Item<V>> {
        match cow(node, work) {
            Node::Inner { kids, .. } => {
                let i = index(key, level);
                let pos = kids.binary_search_by_key(&i, |(k, _)| *k).ok()?;
                let removed = Self::remove_in(&mut kids[pos].1, key, level - 1, work);
                if kids[pos].1.is_empty() {
                    kids.remove(pos);
                }
                removed
            }
            Node::Leaf { items, .. } => {
                let i = index(key, 0);
                let pos = items.binary_search_by_key(&i, |(k, _)| *k).ok()?;
                Some(items.remove(pos).1)
            }
        }
    }

    /// Every item whose birth is `> base`, in key order, entering only subtrees whose birth is
    /// `> base`. `work.nodes_visited` counts the nodes entered, `items_checked` the items looked at.
    pub(crate) fn walk_changed(&self, base: u64, work: &mut TreeWork, mut f: impl FnMut(u64, &Item<V>)) {
        if let Some(root) = self.root.as_deref() {
            Self::walk_in(root, self.height, 0, Some(base), work, &mut f);
        }
    }

    /// Every item, in key order.
    pub(crate) fn walk_all(&self, work: &mut TreeWork, mut f: impl FnMut(u64, &Item<V>)) {
        if let Some(root) = self.root.as_deref() {
            Self::walk_in(root, self.height, 0, None, work, &mut f);
        }
    }

    fn walk_in(
        node: &Node<V>,
        level: u32,
        prefix: u64,
        base: Option<u64>,
        work: &mut TreeWork,
        f: &mut impl FnMut(u64, &Item<V>),
    ) {
        if base.is_some_and(|b| node.birth() <= b) {
            return;
        }
        work.nodes_visited += 1;
        match node {
            Node::Inner { kids, .. } => {
                for (i, kid) in kids {
                    let prefix = prefix | (u64::from(*i) << (BITS * level));
                    Self::walk_in(kid, level - 1, prefix, base, work, f);
                }
            }
            Node::Leaf { items, .. } => {
                for (i, item) in items {
                    work.items_checked += 1;
                    if base.is_none_or(|b| item.birth() > b) {
                        f(prefix | u64::from(*i), item);
                    }
                }
            }
        }
    }

    /// The live item with the greatest key in `[lo, hi]`.
    pub(crate) fn pred(&self, lo: u64, hi: u64) -> Option<(u64, &Arc<V>)> {
        let root = self.root.as_deref()?;
        let hi = if self.covers(hi) {
            hi
        } else {
            (1u64 << (BITS * (self.height + 1))) - 1
        };
        if lo > hi {
            return None;
        }
        Self::pred_in(root, self.height, 0, lo, hi)
    }

    fn pred_in(node: &Node<V>, level: u32, prefix: u64, lo: u64, hi: u64) -> Option<(u64, &Arc<V>)> {
        match node {
            Node::Inner { kids, .. } => {
                let span = 1u64 << (BITS * level);
                for (i, kid) in kids.iter().rev() {
                    let start = prefix | (u64::from(*i) << (BITS * level));
                    let end = start + (span - 1);
                    if start > hi || end < lo {
                        continue;
                    }
                    if let Some(found) = Self::pred_in(kid, level - 1, start, lo, hi) {
                        return Some(found);
                    }
                }
                None
            }
            Node::Leaf { items, .. } => items.iter().rev().find_map(|(i, item)| {
                let key = prefix | u64::from(*i);
                (key >= lo && key <= hi).then(|| item.live().map(|v| (key, v))).flatten()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// Versions derived from random earlier ones by puts (bumped and quiet), holes and removals,
    /// against a model that records, per key, the value and the birth: every version reads back
    /// its own contents, `walk_changed(base)` returns exactly the model's items born after `base`
    /// whatever later versions did, and `pred` agrees with the model's range.
    #[test]
    fn every_version_keeps_its_items_and_walks_exactly_what_changed() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut work = TreeWork::default();
        // (tree, model: key -> (birth, Some(val) | None for a hole), seq at the version)
        let mut versions: Vec<(BirthTree<u64>, BTreeMap<u64, (u64, Option<u64>)>, u64)> =
            vec![(BirthTree::default(), BTreeMap::new(), 0)];
        let mut seq = 0u64;
        for _ in 0..2000 {
            let from = (rng.next() % versions.len() as u64) as usize;
            let (mut tree, mut model, _) = versions[from].clone();
            for _ in 0..(rng.next() % 5) {
                seq += 1;
                let key = match rng.next() % 3 {
                    0 => rng.next() % 64,
                    1 => rng.next() % 5000,
                    _ => rng.next(),
                };
                match rng.next() % 6 {
                    0..=2 => {
                        tree.put(key, Item::Live { birth: seq, val: Arc::new(seq) }, Some(seq), &mut work);
                        model.insert(key, (seq, Some(seq)));
                    }
                    3 => {
                        tree.put(key, Item::Hole { birth: seq, born: 7 }, Some(seq), &mut work);
                        model.insert(key, (seq, None));
                    }
                    4 => {
                        // A quiet replacement keeps the item's birth.
                        if let Some(&(birth, Some(_))) = model.get(&key) {
                            tree.put(key, Item::Live { birth, val: Arc::new(seq) }, None, &mut work);
                            model.insert(key, (birth, Some(seq)));
                        }
                    }
                    _ => {
                        tree.remove(key, &mut work);
                        model.remove(&key);
                    }
                }
            }
            versions.push((tree, model, seq));
        }
        for (v, (tree, model, at)) in versions.iter().enumerate() {
            for (&key, &(birth, val)) in model {
                let item = tree.get(key).unwrap_or_else(|| panic!("version {v}: key {key} lost"));
                assert_eq!(item.birth(), birth, "version {v} key {key}");
                assert_eq!(item.live().map(|x| **x), val, "version {v} key {key}");
            }
            for base in [0, at / 3, at / 2, at.saturating_sub(3), *at] {
                let mut got = Vec::new();
                tree.walk_changed(base, &mut work, |k, item| got.push((k, item.birth())));
                let want: Vec<(u64, u64)> = model
                    .iter()
                    .filter(|(_, (b, _))| *b > base)
                    .map(|(&k, &(b, _))| (k, b))
                    .collect();
                assert_eq!(got, want, "version {v}: walk from {base}");
            }
            let mut all = Vec::new();
            tree.walk_all(&mut work, |k, _| all.push(k));
            assert_eq!(all, model.keys().copied().collect::<Vec<_>>(), "version {v}: walk_all");
            for _ in 0..8 {
                let a = rng.next() % 6000;
                let b = a + rng.next() % 3000;
                let want = model
                    .range(a..=b)
                    .rev()
                    .find(|(_, (_, val))| val.is_some())
                    .map(|(&k, _)| k);
                assert_eq!(tree.pred(a, b).map(|(k, _)| k), want, "version {v}: pred [{a}, {b}]");
            }
        }
    }

    /// A walk from a base enters only the changed items' paths: after one bumped put among many
    /// keys, it visits one node per level.
    #[test]
    fn a_walk_after_one_change_visits_one_path() {
        let mut work = TreeWork::default();
        let mut tree = BirthTree::default();
        for key in 0..100_000u64 {
            tree.put(key, Item::Live { birth: 1, val: Arc::new(key) }, Some(1), &mut work);
        }
        tree.put(54_321, Item::Live { birth: 2, val: Arc::new(0) }, Some(2), &mut work);
        let mut walk = TreeWork::default();
        let mut found = Vec::new();
        tree.walk_changed(1, &mut walk, |k, _| found.push(k));
        assert_eq!(found, vec![54_321]);
        assert_eq!(walk.nodes_visited, u64::from(tree.height()) + 1, "{walk:?}");
    }
}
