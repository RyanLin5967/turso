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
//! The map only NAMES slots; it owns none. The store's retained-version bookkeeping decides how long
//! a slot lives, and it keeps every slot any live branch can see (see `store`), which is every slot a
//! live branch's map can hold.
//!
//! `std::sync::Arc` rather than `crate::sync::Arc`: the nodes are plain data that is only ever
//! touched under the branch store's mutex, so there is no interleaving for a model checker to
//! explore, and `Arc::make_mut` — which copies a node only while another version shares it — is
//! what makes an insert into an unshared map free of copies.
//!
//! The value type is generic so that the trunk's persistent map of last-write epochs (`u64`) shares
//! this code with the branches' slot maps (`Slot`). Two versions of one map that descend from a
//! common clone share every node neither has written since, so [`PageMap::diff`] skips a shared
//! subtree by pointer equality and costs what differs, not what exists — the structural-sharing
//! diff of hash array mapped tries and of Merkle trees, and ZFS's birth-time pruning in another
//! form.

use std::sync::Arc;

use super::arena::Slot;

const BITS: u32 = 5;
const WIDTH: usize = 1 << BITS;
const MASK: u32 = WIDTH as u32 - 1;

/// A value a [`PageMap`] can hold. `EMPTY` marks an absent leaf entry, so no real value may equal it
/// ([`PageMap::insert`] refuses it).
pub(crate) trait TrieValue: Copy + Eq {
    const EMPTY: Self;
}

/// No arena slot has index `Slot::MAX`.
impl TrieValue for Slot {
    const EMPTY: Self = Slot::MAX;
}

/// No trunk epoch reaches `u64::MAX`: epochs advance by one per fork.
impl TrieValue for u64 {
    const EMPTY: Self = u64::MAX;
}

#[derive(Clone)]
enum Node<V: TrieValue> {
    Inner([Option<Arc<Node<V>>>; WIDTH]),
    Leaf([V; WIDTH]),
}

impl<V: TrieValue> Node<V> {
    fn empty(level: u32) -> Self {
        if level == 0 {
            Node::Leaf([V::EMPTY; WIDTH])
        } else {
            Node::Inner(std::array::from_fn(|_| None))
        }
    }
}

#[derive(Clone)]
pub(crate) struct PageMap<V: TrieValue = Slot> {
    root: Option<Arc<Node<V>>>,
    /// Inner levels above the leaves: pages below `WIDTH^(height + 1)` are addressable.
    height: u32,
}

impl<V: TrieValue> Default for PageMap<V> {
    fn default() -> Self {
        Self {
            root: None,
            height: 0,
        }
    }
}

/// What a walk or a diff of page maps touched. Observation only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrieWork {
    /// Nodes visited by a full walk, plus node PAIRS compared by a diff that were not the same
    /// node. A pair skipped by pointer equality is not counted here.
    pub nodes: u64,
    /// Leaf entries read: 32 per leaf a walk visits or a diff compares.
    pub leaf_entries: u64,
    /// Pages the walk or diff reported.
    pub reported: u64,
    /// Subtrees a diff skipped because both sides held the same node.
    pub pruned: u64,
}

impl<V: TrieValue> PageMap<V> {
    fn covers(&self, page: u32) -> bool {
        u64::from(page) < 1u64 << (BITS * (self.height + 1))
    }

    fn index(page: u32, level: u32) -> usize {
        ((page >> (BITS * level)) & MASK) as usize
    }

    pub(crate) fn height(&self) -> u32 {
        self.height
    }

    pub(crate) fn get(&self, page: u32) -> Option<V> {
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
                Node::Leaf(values) => {
                    let value = values[Self::index(page, 0)];
                    return (value != V::EMPTY).then_some(value);
                }
            }
        }
    }

    /// Map `page` to `slot`, replacing any previous mapping. Every other version of this map —
    /// every clone taken before this call — keeps the mapping it had.
    pub(crate) fn insert(&mut self, page: u32, value: V) {
        crate::turso_assert!(value != V::EMPTY, "a page map value equal to its empty marker");
        if self.root.is_none() {
            // The first root is as tall as its first page needs. Starting from a leaf and lifting it
            // would leave that leaf, empty, at kid 0: never read by a lookup, but visited by every
            // walk and diff, which count on every node holding at least one entry.
            self.height = 0;
            while !self.covers(page) {
                self.height += 1;
            }
            self.root = Some(Arc::new(Node::empty(self.height)));
        }
        while !self.covers(page) {
            let mut kids: [Option<Arc<Node<V>>>; WIDTH] = std::array::from_fn(|_| None);
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
                Node::Leaf(values) => {
                    values[Self::index(page, 0)] = value;
                    return;
                }
            };
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Remove `page`'s entry, if it has one. Every other version keeps its own, and an absent entry
    /// copies nothing. A node left without an entry is dropped from its parent, and a root left
    /// empty from the map, so every node a walk visits still holds an entry.
    pub(crate) fn remove(&mut self, page: u32) {
        if self.get(page).is_none() {
            return;
        }
        let height = self.height;
        let root = self.root.as_mut().expect("the entry is there");
        if remove_in(root, height, page) {
            self.root = None;
            self.height = 0;
        }
    }

    /// Call `f` on every entry. Visits every node, so it costs what the map holds.
    pub(crate) fn for_each(&self, work: &mut TrieWork, f: &mut impl FnMut(u32, V)) {
        if let Some(root) = &self.root {
            walk(root, self.height, 0, work, f);
        }
    }

    /// Report every page whose entry differs between `self` and `other`, including a page present
    /// in only one of them. A subtree the two maps share — the same node, by pointer — is skipped
    /// unread, so two versions of one map that descend from a common clone cost the nodes on the
    /// paths written since they diverged, not the size of either.
    ///
    /// When one map is taller, only kid 0 of each of its extra top levels overlaps the shorter
    /// map's pages (a root grows by becoming kid 0 of a new root, `insert`); the other kids hold
    /// pages the shorter map cannot, and are walked whole.
    pub(crate) fn diff(&self, other: &Self, work: &mut TrieWork, f: &mut impl FnMut(u32)) {
        let (mut a, mut b) = (self.root.as_ref(), other.root.as_ref());
        // A map with no root has no entries at any height.
        let (mut ha, mut hb) = (self.height, other.height);
        if a.is_none() {
            ha = hb;
        }
        if b.is_none() {
            hb = ha;
        }
        while ha > hb {
            a = descend_taller(a, ha, work, f);
            ha -= 1;
        }
        while hb > ha {
            b = descend_taller(b, hb, work, f);
            hb -= 1;
        }
        diff_nodes(a, b, ha, 0, work, f);
    }
}

/// Clear `page` under `node` (at `level`), which holds it; true when that leaves `node` empty.
fn remove_in<V: TrieValue>(node: &mut Arc<Node<V>>, level: u32, page: u32) -> bool {
    match Arc::make_mut(node) {
        Node::Leaf(values) => {
            values[(page & MASK) as usize] = V::EMPTY;
            values.iter().all(|&v| v == V::EMPTY)
        }
        Node::Inner(kids) => {
            let i = ((page >> (BITS * level)) & MASK) as usize;
            let kid = kids[i].as_mut().expect("the entry is under this kid");
            if remove_in(kid, level - 1, page) {
                kids[i] = None;
            }
            kids.iter().all(Option::is_none)
        }
    }
}

fn walk<V: TrieValue>(
    node: &Node<V>,
    level: u32,
    base: u32,
    work: &mut TrieWork,
    f: &mut impl FnMut(u32, V),
) {
    work.nodes += 1;
    match node {
        Node::Inner(kids) => {
            for (i, kid) in kids.iter().enumerate() {
                if let Some(kid) = kid {
                    walk(kid, level - 1, base | ((i as u32) << (BITS * level)), work, f);
                }
            }
        }
        Node::Leaf(values) => {
            work.leaf_entries += WIDTH as u64;
            for (i, &value) in values.iter().enumerate() {
                if value != V::EMPTY {
                    work.reported += 1;
                    f(base | i as u32, value);
                }
            }
        }
    }
}

/// One extra top level of the taller map in a diff: report every page under kids 1.. (the shorter
/// map cannot hold them) and return kid 0, the part that overlaps.
fn descend_taller<'a, V: TrieValue>(
    node: Option<&'a Arc<Node<V>>>,
    level: u32,
    work: &mut TrieWork,
    f: &mut impl FnMut(u32),
) -> Option<&'a Arc<Node<V>>> {
    let Node::Inner(kids) = &**node? else {
        unreachable!("a node above the leaves is an inner node");
    };
    work.nodes += 1;
    for (i, kid) in kids.iter().enumerate().skip(1) {
        if let Some(kid) = kid {
            walk(kid, level - 1, (i as u32) << (BITS * level), work, &mut |p, _| f(p));
        }
    }
    kids[0].as_ref()
}

fn diff_nodes<V: TrieValue>(
    a: Option<&Arc<Node<V>>>,
    b: Option<&Arc<Node<V>>>,
    level: u32,
    base: u32,
    work: &mut TrieWork,
    f: &mut impl FnMut(u32),
) {
    let (a, b) = match (a, b) {
        (None, None) => return,
        (Some(a), Some(b)) if Arc::ptr_eq(a, b) => {
            work.pruned += 1;
            return;
        }
        (Some(only), None) | (None, Some(only)) => {
            walk(only, level, base, work, &mut |p, _| f(p));
            return;
        }
        (Some(a), Some(b)) => (a, b),
    };
    work.nodes += 1;
    match (&**a, &**b) {
        (Node::Inner(ka), Node::Inner(kb)) => {
            for i in 0..WIDTH {
                let base = base | ((i as u32) << (BITS * level));
                diff_nodes(ka[i].as_ref(), kb[i].as_ref(), level - 1, base, work, f);
            }
        }
        (Node::Leaf(va), Node::Leaf(vb)) => {
            work.leaf_entries += WIDTH as u64;
            for i in 0..WIDTH {
                if va[i] != vb[i] {
                    work.reported += 1;
                    f(base | i as u32);
                }
            }
        }
        _ => unreachable!("two nodes at the same level are both inner or both leaves"),
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
        let mut versions: Vec<(PageMap<Slot>, HashMap<u32, Slot>)> = vec![Default::default()];
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

    /// `for_each` and `diff` against the same copied-`HashMap` model, over versions of all heights
    /// derived from one another by clones and inserts; and the diff's cost against its contract: a
    /// version diffed against one it was cloned from costs at most `height + 1` node pairs and one
    /// leaf of entries per page written since, however large the maps are.
    #[test]
    fn walk_and_diff_match_a_model_and_a_diff_costs_what_changed() {
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        let mut versions: Vec<(PageMap<u64>, HashMap<u32, u64>)> = vec![Default::default()];
        let mut pruned_cases = 0;
        for step in 0..600u64 {
            let from = (rng.next() % versions.len() as u64) as usize;
            let (mut map, mut model) = versions[from].clone();
            let inserts = rng.next() % 4;
            let mut written = std::collections::HashSet::new();
            for _ in 0..inserts {
                let page = match rng.next() % 4 {
                    0 => (rng.next() % 64) as u32,
                    1 => (rng.next() % 5000) as u32,
                    2 => (rng.next() % 70_000) as u32,
                    _ => (rng.next() % u64::from(u32::MAX)) as u32,
                };
                map.insert(page, step);
                model.insert(page, step);
                written.insert(page);
            }
            // The cost contract, against the version this one was cloned from.
            let mut work = TrieWork::default();
            let mut got = Vec::new();
            map.diff(&versions[from].0, &mut work, &mut |p| got.push(p));
            got.sort_unstable();
            let mut want: Vec<u32> = written
                .iter()
                .copied()
                .filter(|p| versions[from].1.get(p) != Some(&step))
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "step {step}: diff against its source");
            let h = u64::from(map.height().max(versions[from].0.height()));
            if versions[from].0.height() == map.height() {
                assert!(
                    work.nodes <= (h + 1) * want.len() as u64
                        && work.leaf_entries <= 32 * want.len() as u64,
                    "step {step}: {work:?} for {} written pages at height {h}",
                    want.len()
                );
                if !want.is_empty() && !versions[from].1.is_empty() {
                    pruned_cases += 1;
                }
            }
            versions.push((map, model));
        }
        assert!(pruned_cases > 100, "only {pruned_cases} diffs exercised the pruning contract");
        for _ in 0..3000 {
            let i = (rng.next() % versions.len() as u64) as usize;
            let j = (rng.next() % versions.len() as u64) as usize;
            let (a, ma) = &versions[i];
            let (b, mb) = &versions[j];
            let mut work = TrieWork::default();
            let mut got = Vec::new();
            a.diff(b, &mut work, &mut |p| got.push(p));
            got.sort_unstable();
            let mut want: Vec<u32> = ma
                .keys()
                .chain(mb.keys())
                .copied()
                .filter(|p| ma.get(p) != mb.get(p))
                .collect();
            want.sort_unstable();
            want.dedup();
            assert_eq!(got, want, "versions {i} and {j}");
            let mut all = Vec::new();
            let mut walked = TrieWork::default();
            a.for_each(&mut walked, &mut |p, v| all.push((p, v)));
            // Every node holds at least one entry, so a walk costs at most one path per entry.
            assert!(
                walked.nodes <= (u64::from(a.height()) + 1) * walked.reported,
                "version {i}: a walk visited {walked:?} at height {}",
                a.height()
            );
            all.sort_unstable();
            let mut want_all: Vec<(u32, u64)> = ma.iter().map(|(&p, &v)| (p, v)).collect();
            want_all.sort_unstable();
            assert_eq!(all, want_all, "version {i} walk");
        }
    }

    /// Inserts and removes against a copied `HashMap`, over versions derived from one another: every
    /// version keeps its own entries, and every node a walk visits holds an entry, so a map emptied by
    /// removals has no root and a walk costs at most one path per entry.
    #[test]
    fn removals_keep_every_version_and_leave_no_empty_node() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut versions: Vec<(PageMap<u64>, HashMap<u32, u64>)> = vec![Default::default()];
        for step in 0..3000u64 {
            let from = (rng.next() % versions.len() as u64) as usize;
            let (mut map, mut model) = versions[from].clone();
            for _ in 0..(1 + rng.next() % 4) {
                let page = match rng.next() % 3 {
                    0 => (rng.next() % 64) as u32,
                    1 => (rng.next() % 5000) as u32,
                    _ => (rng.next() % 70_000) as u32,
                };
                if rng.next() % 2 == 0 {
                    map.insert(page, step);
                    model.insert(page, step);
                } else {
                    // Half the removals hit an entry the model holds (sorted: a HashMap's order is
                    // random per process, and the test must replay).
                    let mut held: Vec<u32> = model.keys().copied().collect();
                    held.sort_unstable();
                    let page = if !held.is_empty() && rng.next() % 2 == 0 {
                        held[(rng.next() % held.len() as u64) as usize]
                    } else {
                        page
                    };
                    map.remove(page);
                    model.remove(&page);
                }
            }
            assert_eq!(map.is_empty(), model.is_empty(), "step {step}");
            versions.push((map, model));
        }
        for (i, (map, model)) in versions.iter().enumerate() {
            let mut all = Vec::new();
            let mut walked = TrieWork::default();
            map.for_each(&mut walked, &mut |p, v| all.push((p, v)));
            all.sort_unstable();
            let mut want: Vec<(u32, u64)> = model.iter().map(|(&p, &v)| (p, v)).collect();
            want.sort_unstable();
            assert_eq!(all, want, "version {i}");
            assert!(
                walked.nodes <= (u64::from(map.height()) + 1) * walked.reported,
                "version {i}: {walked:?} at height {}",
                map.height()
            );
        }
    }

    #[test]
    fn an_empty_map_and_an_unaddressable_page_read_as_absent() {
        let mut map = PageMap::<Slot>::default();
        assert_eq!(map.get(0), None);
        map.insert(3, 7);
        assert_eq!(map.get(3), Some(7));
        assert_eq!(map.get(4), None);
        assert_eq!(map.get(1 << 20), None, "a page beyond the root's reach");
    }
}
