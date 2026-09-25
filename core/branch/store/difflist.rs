//! DIFF and LIST over the branch store: which pages two views see differently, and which branches
//! exist, optionally filtered by their metadata. Every operation takes the store's mutex for its
//! whole run, like every other store operation.
//!
//! # DIFF by identity
//!
//! The version of a page a view sees is named by its IDENTITY: an arena slot, or "the trunk's
//! current version" (`StoreInner::resolve` returning `None`; the trunk itself always sees that).
//! Two live views that name the same slot see the same bytes: a slot is rewritten in place only
//! when no live child forked inside its current version's interval (`first_write_branch`), so no
//! second live view can hold it while it changes, and a retained trunk version is a fresh slot
//! holding the pre-image. So DIFF compares identities and reads no page bytes. It over-reports
//! only a page whose write was rolled back: its slot was allocated and holds the pre-image.
//!
//! Four arms return the same set, and differ only in how they find the candidates they resolve:
//!
//! * [`DiffArm::Scan`] resolves every page up to a bound: what the store could answer before this
//!   module, one `resolve` per page.
//! * [`DiffArm::Died`] and [`DiffArm::Written`] use only the indexes the store already had. Both
//!   enumerate each side's whole arena view (its `current` keys and a full walk of `inherited`).
//!   For the trunk side, the pages the trunk wrote between the two views' fork points `lo < hi`:
//!   `Died` walks the trunk's retained versions by death over `(lo, hi]` and keeps those born at or
//!   before `lo` (the first write after `lo` to a page kills the version the view at `lo` sees, and
//!   that view's trunk-child ancestor keeps it retained); `Written` scans the trunk's whole
//!   last-write map and keeps the pages written after `lo`.
//! * [`DiffArm::Fix`] diffs persistent maps and skips what they share. On the branch side the two
//!   views' page maps (`view` once a branch has forked, else `inherited` with `current` overlaid);
//!   on the trunk side the two versions of the trunk's last-write map the views were forked with
//!   (`trunk_snap`, or the live `written_map` for the trunk).
//!
//! # Why every `Fix` candidate is output
//!
//! So `Fix` resolves exactly the pages it reports:
//! * a page whose branch-side map entries differ: the two sides name different slots, or one names
//!   a slot and the other reads through to the trunk — unless a side's own `current` shadows its
//!   entry, and then that side names a private slot no other live view can hold;
//! * an overlaid `current` page of a branch that has never forked: a private slot;
//! * a page the trunk wrote between the two fork points: two views with different `trunk_at` sit
//!   under different trunk children, whose arena views share no slot, so either both name
//!   different slots, one names a slot and the other a trunk version, or both read trunk versions
//!   on either side of a write.
//!
//! # LIST
//!
//! Lists the branches that hold a handle. [`ListArm::Scan`] walks the whole branch table, which
//! also holds the handle-less states a live child keeps. [`ListArm::Index`] reads secondary
//! indexes kept at fork, `set_meta` and handle release (the [`Catalog`]), and the per-parent
//! `children` maps the store already had; [`ListArm::IndexSingle`] answers the owner-and-lease
//! filter from the owner index alone, filtering the lease.
//!
//! Every arm above walks its output under the store's mutex, so a listing of all N branches stalls
//! every other branch operation for Θ(N). [`ListArm::Snapshot`] answers `All` and `Parent` from
//! persistent id maps (the page-map trie, keyed by branch id): it clones one in O(1) under the
//! mutex and walks it after releasing it — a copy-on-write snapshot of an ordered index, as
//! `google/btree`'s lazy `Clone` gives etcd and CockroachDB, and read-copy-update in general. The
//! listing is the catalog as of the clone. Its other filters return small ranges and use `Index`.
//! Branch ids are the maps' keys, so they must fit in a `u32`; a fork past that is refused.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use super::{gone, BranchState, BranchStore, Owned, StoreInner, NO_PAGE};
use crate::branch::arena::Slot;
use crate::branch::page_map::{PageMap, TrieWork};
use crate::branch::BranchId;
use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffArm {
    /// Resolve pages `1..=pages` on both sides.
    Scan { pages: u32 },
    Died,
    Written,
    Fix,
}

/// What one DIFF did. Observation only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiffWork {
    /// Candidate pages resolved on both sides (after de-duplication).
    pub pages_resolved: u64,
    /// `current` keys enumerated: every one for `Died`/`Written`, the overlay for `Fix`.
    pub arena_entries: u64,
    /// Page-map nodes visited by full walks, plus node pairs compared by diffs that were not the
    /// same node.
    pub trie_nodes: u64,
    /// Page-map leaf entries read (32 per leaf visited or compared).
    pub trie_leaf_entries: u64,
    /// Pages the page-map walks and diffs reported, before de-duplication.
    pub trie_reported: u64,
    /// Shared subtrees the page-map diffs skipped by pointer equality.
    pub trie_pruned: u64,
    /// Trunk index entries visited: retained versions by death (`Died`), or last-write entries
    /// (`Written`).
    pub trunk_entries: u64,
    /// The tallest page map the diff read, for the cost contract.
    pub max_height: u32,
    /// Pages in the result.
    pub output: u64,
}

impl DiffWork {
    /// Every counted step.
    pub fn total(&self) -> u64 {
        self.pages_resolved
            + self.arena_entries
            + self.trie_nodes
            + self.trie_leaf_entries
            + self.trunk_entries
    }

    fn add_trie(&mut self, t: TrieWork) {
        self.trie_nodes += t.nodes;
        self.trie_leaf_entries += t.leaf_entries;
        self.trie_reported += t.reported;
        self.trie_pruned += t.pruned;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff {
    /// Page numbers, ascending.
    pub pages: Vec<u32>,
    pub work: DiffWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListFilter {
    All,
    /// Children of this branch, or of the trunk.
    Parent(BranchId),
    /// Ids in `lo..=hi`: ids are allocated in creation order.
    Created { lo: u64, hi: u64 },
    Owner(u64),
    /// Lease deadline strictly before this.
    LeaseBefore(u64),
    OwnerLease { owner: u64, before: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListArm {
    Scan,
    Index,
    IndexSingle,
    Snapshot,
}

/// What one LIST did. Observation only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListWork {
    /// Table entries or index entries visited.
    pub entries_visited: u64,
    /// Of those, the ones visited while holding the store's mutex: all of them, except for a
    /// snapshot listing, which visits none under it.
    pub under_lock: u64,
    pub output: u64,
    /// The branch table's entries and capacity at the call: a scan's time follows the capacity.
    pub table_len: u64,
    pub table_capacity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// In the order the arm met them: sorting is the caller's, so a list's cost is its own.
    pub ids: Vec<BranchId>,
    pub work: ListWork,
}

/// Secondary indexes over the branches that hold a handle: `(key…, id)` sets, so every filter is a
/// range. Kept at fork, `set_meta` and handle release.
#[derive(Default)]
pub(crate) struct Catalog {
    live: BTreeSet<u64>,
    by_owner: BTreeSet<(u64, u64)>,
    by_lease: BTreeSet<(u64, u64)>,
    by_owner_lease: BTreeSet<(u64, u64, u64)>,
    /// The same ids as persistent maps (id → parent id), all of them and per parent, for
    /// [`ListArm::Snapshot`]. An insert or removal copies a path only while a listing holds a clone.
    live_map: PageMap<u64>,
    by_parent: HashMap<BranchId, PageMap<u64>>,
}

/// The largest branch id a snapshot map can key.
pub(super) const MAX_LISTED_ID: u64 = u32::MAX as u64 - 1;

fn map_key(id: BranchId) -> u32 {
    u32::try_from(id.0)
        .ok()
        .filter(|&k| u64::from(k) <= MAX_LISTED_ID)
        .expect("forks refuse ids past MAX_LISTED_ID")
}

/// `BranchState`'s defaults: no owner, no lease.
const NO_OWNER: u64 = 0;
const NO_LEASE: u64 = u64::MAX;

impl Catalog {
    pub(super) fn add(&mut self, id: BranchId, parent: BranchId) {
        self.insert(id, NO_OWNER, NO_LEASE);
        self.live_map.insert(map_key(id), parent.0);
        self.by_parent
            .entry(parent)
            .or_default()
            .insert(map_key(id), parent.0);
    }

    pub(super) fn remove(&mut self, id: BranchId, parent: BranchId, owner: u64, lease: u64) {
        self.remove_meta(id, owner, lease);
        self.live_map.remove(map_key(id));
        let siblings = self
            .by_parent
            .get_mut(&parent)
            .expect("a listed branch is listed under its parent");
        siblings.remove(map_key(id));
        if siblings.is_empty() {
            self.by_parent.remove(&parent);
        }
    }

    fn insert(&mut self, id: BranchId, owner: u64, lease: u64) {
        let id = id.0;
        let fresh = self.live.insert(id)
            & self.by_owner.insert((owner, id))
            & self.by_lease.insert((lease, id))
            & self.by_owner_lease.insert((owner, lease, id));
        crate::turso_assert!(fresh, "a branch was listed twice in the catalog");
    }

    fn remove_meta(&mut self, id: BranchId, owner: u64, lease: u64) {
        let id = id.0;
        let listed = self.live.remove(&id)
            & self.by_owner.remove(&(owner, id))
            & self.by_lease.remove(&(lease, id))
            & self.by_owner_lease.remove(&(owner, lease, id));
        crate::turso_assert!(listed, "removed a branch the catalog did not list under its metadata");
    }
}

impl BranchStore {
    pub(crate) fn diff(&self, x: BranchId, y: BranchId, arm: DiffArm) -> Result<Diff> {
        self.inner.lock().diff(x, y, arm)
    }

    pub(crate) fn list(&self, filter: ListFilter, arm: ListArm) -> Result<Listing> {
        let inner = self.inner.lock();
        let snapshot = match (arm, filter) {
            (ListArm::Snapshot, ListFilter::All) => inner.catalog.live_map.clone(),
            (ListArm::Snapshot, ListFilter::Parent(p)) => {
                if !p.is_trunk() {
                    inner.state(p)?;
                }
                inner.catalog.by_parent.get(&p).cloned().unwrap_or_default()
            }
            _ => return inner.list(filter, arm),
        };
        let mut work = ListWork {
            table_len: inner.branches.len() as u64,
            table_capacity: inner.branches.capacity() as u64,
            ..Default::default()
        };
        drop(inner);
        let mut ids = Vec::new();
        let mut t = TrieWork::default();
        snapshot.for_each(&mut t, &mut |id, _| ids.push(BranchId(u64::from(id))));
        work.entries_visited = t.reported;
        work.output = ids.len() as u64;
        Ok(Listing { ids, work })
    }

    /// Set a branch's listing metadata. Refused for a branch without a handle: it is not listed.
    pub(crate) fn set_meta(&self, id: BranchId, owner: u64, lease: u64) -> Result<()> {
        let mut inner = self.inner.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if !st.handle {
            return Err(gone(id));
        }
        let old = (st.owner, st.lease);
        (st.owner, st.lease) = (owner, lease);
        inner.catalog.remove_meta(id, old.0, old.1);
        inner.catalog.insert(id, owner, lease);
        Ok(())
    }
}

impl StoreInner {
    fn state(&self, id: BranchId) -> Result<&BranchState> {
        self.branches.get(&id).ok_or_else(|| gone(id))
    }

    /// The identity of the version of `page` that `id` sees: an arena slot, or `None` for the
    /// trunk's current version.
    fn identity(&self, id: BranchId, page: u32) -> Result<Option<Slot>> {
        if id.is_trunk() {
            return Ok(None);
        }
        let (mut levels, mut examined) = (0, 0);
        self.resolve(id, page, &mut levels, &mut examined)
    }

    /// The fork epoch at which `id`'s ancestry leaves the trunk; `None` for the trunk itself,
    /// which sees every trunk write.
    fn trunk_at_of(&self, id: BranchId) -> Result<Option<u64>> {
        if id.is_trunk() {
            return Ok(None);
        }
        Ok(Some(self.state(id)?.trunk_at))
    }

    fn diff(&self, x: BranchId, y: BranchId, arm: DiffArm) -> Result<Diff> {
        let mut work = DiffWork::default();
        for id in [x, y] {
            self.trunk_at_of(id)?;
        }
        if x == y {
            return Ok(Diff {
                pages: Vec::new(),
                work,
            });
        }
        let mut candidates: Vec<u32> = Vec::new();
        match arm {
            DiffArm::Scan { pages } => candidates.extend(1..=pages),
            DiffArm::Died | DiffArm::Written => {
                for id in [x, y] {
                    self.whole_arena_view(id, &mut work, &mut candidates)?;
                }
                self.trunk_writes_between(x, y, arm, &mut work, &mut candidates)?;
            }
            DiffArm::Fix => self.fix_candidates(x, y, &mut work, &mut candidates)?,
        }
        candidates.sort_unstable();
        candidates.dedup();
        let mut pages = Vec::new();
        for page in candidates {
            work.pages_resolved += 1;
            if self.identity(x, page)? != self.identity(y, page)? {
                pages.push(page);
            }
        }
        work.output = pages.len() as u64;
        Ok(Diff { pages, work })
    }

    /// Every page `id` sees in the arena: its own pages and a full walk of what it inherited.
    fn whole_arena_view(
        &self,
        id: BranchId,
        work: &mut DiffWork,
        out: &mut Vec<u32>,
    ) -> Result<()> {
        if id.is_trunk() {
            return Ok(());
        }
        let st = self.state(id)?;
        for &page in st.current.keys() {
            work.arena_entries += 1;
            out.push(page);
        }
        let mut t = TrieWork::default();
        st.inherited.for_each(&mut t, &mut |page, _| out.push(page));
        work.add_trie(t);
        work.max_height = work.max_height.max(st.inherited.height());
        Ok(())
    }

    /// The pages the trunk wrote between the two views' fork points, by the trunk's existing
    /// indexes (`Died`: exactly those pages; `Written`: every page written after the earlier
    /// point, a superset the caller's resolution filters).
    fn trunk_writes_between(
        &self,
        x: BranchId,
        y: BranchId,
        arm: DiffArm,
        work: &mut DiffWork,
        out: &mut Vec<u32>,
    ) -> Result<()> {
        let (lo, hi) = match (self.trunk_at_of(x)?, self.trunk_at_of(y)?) {
            (None, None) => return Ok(()),
            (Some(a), None) | (None, Some(a)) => (a, None),
            (Some(a), Some(b)) if a == b => return Ok(()),
            (Some(a), Some(b)) => (a.min(b), Some(a.max(b))),
        };
        match arm {
            DiffArm::Died => {
                // `died` in (lo, hi], as bounds on the index's first field (see `Lineage::garbage`).
                let after = |e: u64| (e, NO_PAGE, u64::MAX);
                let from = Bound::Excluded(after(lo));
                let to = hi.map_or(Bound::Unbounded, |hi| Bound::Included(after(hi)));
                for &(_died, page, born) in self.trunk.lineage.by_died.range((from, to)) {
                    work.trunk_entries += 1;
                    if born <= lo {
                        out.push(page);
                    }
                }
            }
            DiffArm::Written => {
                for (&page, &epoch) in &self.trunk.written {
                    work.trunk_entries += 1;
                    if epoch > lo {
                        out.push(page);
                    }
                }
            }
            DiffArm::Scan { .. } | DiffArm::Fix => unreachable!("index arms only"),
        }
        Ok(())
    }

    /// A side's arena page map, and the `current` pages that map does not hold yet: a branch that
    /// has forked keeps `view` = `inherited` + `current`; one that has not has only `inherited`.
    #[allow(clippy::type_complexity)]
    fn arena_map(
        &self,
        id: BranchId,
    ) -> Result<(Option<&PageMap>, Option<&HashMap<u32, Owned>>)> {
        if id.is_trunk() {
            return Ok((None, None));
        }
        let st = self.state(id)?;
        Ok(match &st.view {
            Some(view) => (Some(view), None),
            None => (Some(&st.inherited), Some(&st.current)),
        })
    }

    /// The version of the trunk's last-write map a side was forked with; the live one for the trunk.
    fn trunk_snap_of(&self, id: BranchId) -> Result<&PageMap<u64>> {
        Ok(if id.is_trunk() {
            &self.trunk.written_map
        } else {
            &self.state(id)?.trunk_snap
        })
    }

    fn fix_candidates(
        &self,
        x: BranchId,
        y: BranchId,
        work: &mut DiffWork,
        out: &mut Vec<u32>,
    ) -> Result<()> {
        let ((map_x, own_x), (map_y, own_y)) = (self.arena_map(x)?, self.arena_map(y)?);
        let mut t = TrieWork::default();
        match (map_x, map_y) {
            (Some(a), Some(b)) => a.diff(b, &mut t, &mut |page| out.push(page)),
            (Some(only), None) | (None, Some(only)) => {
                only.for_each(&mut t, &mut |page, _| out.push(page))
            }
            (None, None) => {}
        }
        for map in [map_x, map_y].into_iter().flatten() {
            work.max_height = work.max_height.max(map.height());
        }
        for own in [own_x, own_y].into_iter().flatten() {
            for &page in own.keys() {
                work.arena_entries += 1;
                out.push(page);
            }
        }
        let (snap_x, snap_y) = (self.trunk_snap_of(x)?, self.trunk_snap_of(y)?);
        work.max_height = work
            .max_height
            .max(snap_x.height())
            .max(snap_y.height());
        snap_x.diff(snap_y, &mut t, &mut |page| out.push(page));
        work.add_trie(t);
        Ok(())
    }

    fn list(&self, filter: ListFilter, arm: ListArm) -> Result<Listing> {
        let mut work = ListWork {
            table_len: self.branches.len() as u64,
            table_capacity: self.branches.capacity() as u64,
            ..Default::default()
        };
        let mut ids = Vec::new();
        let take = |ids: &mut Vec<BranchId>, work: &mut ListWork, id: u64| {
            work.entries_visited += 1;
            ids.push(BranchId(id));
        };
        let c = &self.catalog;
        match (arm, filter) {
            (ListArm::Scan, _) => {
                for (&id, st) in &self.branches {
                    work.entries_visited += 1;
                    if st.handle && matches(filter, id, st) {
                        ids.push(id);
                    }
                }
            }
            (_, ListFilter::All) => c.live.iter().for_each(|&id| take(&mut ids, &mut work, id)),
            (_, ListFilter::Parent(p)) => {
                let children = if p.is_trunk() {
                    &self.trunk.lineage.children
                } else {
                    &self.state(p)?.lineage.children
                };
                for &id in children.values() {
                    work.entries_visited += 1;
                    if self.state(id)?.handle {
                        ids.push(id);
                    }
                }
            }
            (_, ListFilter::Created { lo, hi }) => {
                if lo <= hi {
                    c.live.range(lo..=hi).for_each(|&id| take(&mut ids, &mut work, id));
                }
            }
            (_, ListFilter::Owner(o)) => c
                .by_owner
                .range((o, 0)..=(o, u64::MAX))
                .for_each(|&(_, id)| take(&mut ids, &mut work, id)),
            (_, ListFilter::LeaseBefore(t)) => c
                .by_lease
                .range(..(t, 0))
                .for_each(|&(_, id)| take(&mut ids, &mut work, id)),
            (ListArm::Index | ListArm::Snapshot, ListFilter::OwnerLease { owner, before }) => c
                .by_owner_lease
                .range((owner, 0, 0)..(owner, before, 0))
                .for_each(|&(_, _, id)| take(&mut ids, &mut work, id)),
            (ListArm::IndexSingle, ListFilter::OwnerLease { owner, before }) => {
                for &(_, id) in c.by_owner.range((owner, 0)..=(owner, u64::MAX)) {
                    work.entries_visited += 1;
                    if self.state(BranchId(id))?.lease < before {
                        ids.push(BranchId(id));
                    }
                }
            }
        }
        work.output = ids.len() as u64;
        work.under_lock = work.entries_visited;
        Ok(Listing { ids, work })
    }
}

fn matches(filter: ListFilter, id: BranchId, st: &BranchState) -> bool {
    match filter {
        ListFilter::All => true,
        ListFilter::Parent(p) => st.parent == p,
        ListFilter::Created { lo, hi } => lo <= id.0 && id.0 <= hi,
        ListFilter::Owner(o) => st.owner == o,
        ListFilter::LeaseBefore(t) => st.lease < t,
        ListFilter::OwnerLease { owner, before } => st.owner == owner && st.lease < before,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::storage::pager::PageRef;
    use crate::sync::Arc;
    use std::collections::HashMap;

    const PAGE: usize = 64;
    /// The pages the tests write: several leaves of one page map, and numbers that need maps of
    /// height 0 to 3, so the diffs cross roots of different heights.
    const PAGES: [u32; 13] = [1, 2, 3, 31, 32, 33, 64, 100, 1023, 1024, 1500, 33_000, 40_000];

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    fn image(generation: u64) -> Vec<u8> {
        generation.to_le_bytes().repeat(PAGE / 8)
    }

    fn page_with(page: u32, generation: u64) -> PageRef {
        let p = Arc::new(crate::storage::pager::Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(PAGE));
        buffer.as_mut_slice().copy_from_slice(&image(generation));
        p.get().buffer = Some(buffer);
        p
    }

    struct Node {
        id: BranchId,
        sees: HashMap<u32, u64>,
        handle: bool,
        depth: usize,
    }

    #[derive(Default)]
    struct Coverage {
        nonempty: u64,
        empty_distinct: u64,
        different_trunk_at: u64,
        fix_pruned: u64,
        scans: u64,
        max_depth: usize,
        max_height: u32,
    }

    /// Every DIFF arm against a model in which each branch is a plain copy of its parent's pages at
    /// its fork: random trees (trunk forks, branch forks, chains), trunk writes, branch writes and
    /// reaps, and after every step three random pairs of live views, the trunk included. The
    /// expected set is where the model's page CONTENTS differ, so the identity argument in the
    /// module doc is tested, not assumed. `Fix` must also meet its cost contract: it resolves
    /// exactly the pages it returns, and its page-map work is bounded by the pages the maps report.
    #[test]
    fn every_diff_arm_equals_the_models_content_diff_and_fix_costs_what_differs() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_diff(seed);
        }
    }

    fn run_diff(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = PAGES.iter().map(|&p| (p, 0)).collect();
        let mut nodes: Vec<Node> = Vec::new();
        let mut generation = 0u64;
        let mut cov = Coverage::default();
        for step in 0..2000 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            match rng.below(12) {
                0..=1 if live.len() < 50 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    nodes.push(Node {
                        id,
                        sees: trunk.clone(),
                        handle: true,
                        depth: 1,
                    });
                }
                2..=3 if !live.is_empty() && live.len() < 50 => {
                    // Three times in four the newest live branch, so that chains grow deep.
                    let parent = if rng.below(4) != 0 {
                        *live.last().unwrap()
                    } else {
                        live[rng.below(live.len() as u64) as usize]
                    };
                    let id = store.fork_branch(nodes[parent].id).unwrap();
                    let (sees, depth) = (nodes[parent].sees.clone(), nodes[parent].depth + 1);
                    cov.max_depth = cov.max_depth.max(depth);
                    nodes.push(Node {
                        id,
                        sees,
                        handle: true,
                        depth,
                    });
                }
                4..=6 => {
                    let page = PAGES[rng.below(PAGES.len() as u64) as usize];
                    if store.trunk_has_children() {
                        store.first_write_trunk(page, &image(trunk[&page]));
                    }
                    generation += 1;
                    trunk.insert(page, generation);
                }
                7..=9 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    let id = nodes[v].id;
                    store.begin_write(id).unwrap();
                    let mut committed: Vec<PageRef> = Vec::new();
                    for _ in 0..=rng.below(2) {
                        let page = PAGES[rng.below(PAGES.len() as u64) as usize];
                        if committed.iter().any(|p| p.get().id == page as usize) {
                            continue;
                        }
                        store
                            .first_write_branch(id, page, &image(nodes[v].sees[&page]))
                            .unwrap();
                        generation += 1;
                        committed.push(page_with(page, generation));
                        nodes[v].sees.insert(page, generation);
                    }
                    store.commit_pages(id, &committed).unwrap();
                    store.end_write(id);
                }
                _ if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    nodes[v].handle = false;
                    store.release_handle(nodes[v].id);
                }
                _ => {}
            }
            let views: Vec<(BranchId, &HashMap<u32, u64>)> = std::iter::once((BranchId::TRUNK, &trunk))
                .chain(nodes.iter().filter(|n| n.handle).map(|n| (n.id, &n.sees)))
                .collect();
            for pair in 0..3 {
                let (x, sx) = views[rng.below(views.len() as u64) as usize];
                let (y, sy) = views[rng.below(views.len() as u64) as usize];
                let scan = pair == 0 && step % 50 == 0;
                check(&store, (x, sx), (y, sy), scan, &mut cov, seed, step);
            }
        }
        assert!(
            cov.nonempty > 100
                && cov.empty_distinct > 10
                && cov.different_trunk_at > 100
                && cov.fix_pruned > 100
                && cov.scans > 20
                && cov.max_depth >= 8
                && cov.max_height >= 2,
            "seed {seed:#x}: the shapes the arms differ on did not all occur: nonempty {}, empty \
             between distinct views {}, pairs with different trunk_at {}, Fix diffs that skipped a \
             shared subtree and still found a difference {}, \
             scans {}, depth {}, map height {}",
            cov.nonempty,
            cov.empty_distinct,
            cov.different_trunk_at,
            cov.fix_pruned,
            cov.scans,
            cov.max_depth,
            cov.max_height
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
    }

    fn check(
        store: &BranchStore,
        (x, sx): (BranchId, &HashMap<u32, u64>),
        (y, sy): (BranchId, &HashMap<u32, u64>),
        scan: bool,
        cov: &mut Coverage,
        seed: u64,
        step: u32,
    ) {
        let want: Vec<u32> = PAGES.iter().copied().filter(|p| sx[p] != sy[p]).collect();
        let mut arms = vec![DiffArm::Died, DiffArm::Written, DiffArm::Fix];
        if scan {
            arms.push(DiffArm::Scan { pages: 40_000 });
            cov.scans += 1;
        }
        for arm in arms {
            let d = store.diff(x, y, arm).unwrap();
            assert_eq!(
                d.pages, want,
                "seed {seed:#x} step {step}: {arm:?} diff of {} and {}",
                x.0, y.0
            );
            let w = d.work;
            match arm {
                DiffArm::Fix => {
                    let h = u64::from(w.max_height);
                    assert!(
                        w.pages_resolved == w.output
                            && w.trie_nodes <= (h + 1) * w.trie_reported + 2 * h
                            && w.trie_leaf_entries <= 32 * w.trie_reported,
                        "seed {seed:#x} step {step}: Fix broke its cost contract diffing {} and \
                         {}: {w:?}",
                        x.0,
                        y.0
                    );
                    if w.trie_pruned > 0 && w.output > 0 {
                        cov.fix_pruned += 1;
                    }
                    cov.max_height = cov.max_height.max(w.max_height);
                }
                DiffArm::Died | DiffArm::Written => {}
                // A view diffed against itself returns before resolving anything.
                DiffArm::Scan { pages } => assert_eq!(
                    w.pages_resolved,
                    if x == y { 0 } else { u64::from(pages) }
                ),
            }
        }
        if !want.is_empty() {
            cov.nonempty += 1;
        } else if x != y {
            cov.empty_distinct += 1;
        }
        let at = |id: BranchId| {
            let inner = store.inner.lock();
            inner.trunk_at_of(id).unwrap()
        };
        if at(x) != at(y) {
            cov.different_trunk_at += 1;
        }
    }

    #[derive(Clone)]
    struct Meta {
        id: BranchId,
        parent: BranchId,
        owner: u64,
        lease: u64,
        handle: bool,
    }

    /// Every LIST arm against the harness's own record of every branch's parent, metadata and
    /// handle, under forks from the trunk and from branches, metadata changes and reaps (some
    /// deferred, leaving handle-less states in the table). The index arm must visit exactly the
    /// entries it returns, except for `Parent`, which also visits the handle-less children.
    #[test]
    fn every_list_arm_equals_the_model_and_the_index_visits_only_its_output() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03] {
            run_list(seed);
        }
    }

    fn run_list(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut model: Vec<Meta> = Vec::new();
        let (mut deferred, mut nonempty_filters) = (0, 0);
        for step in 0..2000u32 {
            let live: Vec<usize> = (0..model.len()).filter(|&i| model[i].handle).collect();
            match rng.below(10) {
                0..=2 if live.len() < 80 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    model.push(Meta {
                        id,
                        parent: BranchId::TRUNK,
                        owner: 0,
                        lease: u64::MAX,
                        handle: true,
                    });
                }
                3..=4 if !live.is_empty() && live.len() < 80 => {
                    let parent = model[live[rng.below(live.len() as u64) as usize]].id;
                    let id = store.fork_branch(parent).unwrap();
                    model.push(Meta {
                        id,
                        parent,
                        owner: 0,
                        lease: u64::MAX,
                        handle: true,
                    });
                }
                5..=6 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    let owner = rng.below(5);
                    let lease = if rng.below(8) == 0 {
                        u64::MAX
                    } else {
                        rng.below(1000)
                    };
                    store.set_meta(model[v].id, owner, lease).unwrap();
                    (model[v].owner, model[v].lease) = (owner, lease);
                }
                7..=8 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    model[v].handle = false;
                    if store.release_handle(model[v].id).deferred {
                        deferred += 1;
                    }
                    assert!(
                        store.set_meta(model[v].id, 1, 1).is_err(),
                        "seed {seed:#x} step {step}: metadata set on a released branch"
                    );
                }
                _ => {}
            }
            let max_id = model.last().map_or(0, |m| m.id.0);
            let live: Vec<&Meta> = model.iter().filter(|m| m.handle).collect();
            let mut filters = vec![
                ListFilter::All,
                ListFilter::Parent(BranchId::TRUNK),
                ListFilter::Created {
                    lo: rng.below(max_id + 2),
                    hi: rng.below(max_id + 2),
                },
                ListFilter::Owner(rng.below(5)),
                ListFilter::LeaseBefore(rng.below(1100)),
                ListFilter::OwnerLease {
                    owner: rng.below(5),
                    before: rng.below(1100),
                },
            ];
            if !live.is_empty() {
                let p = live[rng.below(live.len() as u64) as usize].id;
                filters.push(ListFilter::Parent(p));
            }
            for filter in filters {
                let want: Vec<BranchId> = live
                    .iter()
                    .filter(|m| match filter {
                        ListFilter::All => true,
                        ListFilter::Parent(p) => m.parent == p,
                        ListFilter::Created { lo, hi } => lo <= m.id.0 && m.id.0 <= hi,
                        ListFilter::Owner(o) => m.owner == o,
                        ListFilter::LeaseBefore(t) => m.lease < t,
                        ListFilter::OwnerLease { owner, before } => {
                            m.owner == owner && m.lease < before
                        }
                    })
                    .map(|m| m.id)
                    .collect();
                if !want.is_empty() {
                    nonempty_filters += 1;
                }
                for arm in [ListArm::Scan, ListArm::Index, ListArm::IndexSingle, ListArm::Snapshot] {
                    let mut got = store.list(filter, arm).unwrap();
                    got.ids.sort_unstable();
                    assert_eq!(
                        got.ids, want,
                        "seed {seed:#x} step {step}: {arm:?} {filter:?}"
                    );
                    let w = got.work;
                    let exact = match (arm, filter) {
                        (ListArm::Snapshot, ListFilter::All | ListFilter::Parent(_)) => {
                            w.entries_visited == w.output && w.under_lock == 0
                        }
                        (ListArm::Scan, _) => w.entries_visited == w.table_len,
                        (_, ListFilter::Parent(_)) => w.entries_visited >= w.output,
                        (ListArm::IndexSingle, ListFilter::OwnerLease { .. }) => {
                            w.entries_visited >= w.output
                        }
                        _ => w.entries_visited == w.output,
                    };
                    assert!(exact, "seed {seed:#x} step {step}: {arm:?} {filter:?} {w:?}");
                }
            }
        }
        assert!(
            deferred > 10 && nonempty_filters > 5000,
            "seed {seed:#x}: deferred reaps {deferred}, non-empty filters {nonempty_filters}"
        );
    }
}
