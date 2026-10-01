//! Per-branch page spaces, and the rule that decides which version of a page a branch sees.
//!
//! # The model
//!
//! Every node in the branch tree — the trunk and each branch — carries an `epoch` that its own
//! forks advance: a child forked from a node records the node's epoch at that moment as its
//! `fork_epoch`, and the node's epoch then increments. A version of a page that a node wrote in
//! epoch `born` is visible to that node's children forked at any epoch `>= born`, until the node
//! overwrites it in epoch `died`; after that it is visible only to children forked in
//! `[born, died)`. A branch therefore sees, for each page:
//!
//! 1. its own current version, if it has written the page; else
//! 2. its parent's version at the branch's `fork_epoch` — the parent's current version if it was
//!    born at or before that epoch, else the parent's RETAINED version covering it; else
//! 3. the same question one level up, down to the trunk, whose current version lives in the WAL
//!    and the database file and is read by the ordinary pager path.
//!
//! # Resolution without the walk — the page map
//!
//! Steps 2 and 3 are answered for every level between a branch and the trunk at once, and frozen,
//! at the moment the branch forks: nothing a branch sees through its ancestors can change after its
//! fork (an ancestor's later write retains the version the branch saw, in the same slot). So each
//! branch carries `inherited`, a persistent [`PageMap`] of every arena page it sees through its
//! ancestors, which is its parent's `view` at the fork: the parent's own `inherited` plus the
//! parent's current pages, kept up to date by the parent's writes once it has forked a child. A
//! fork clones the parent's `view` in O(1) and a write path-copies O(log P) trie nodes, so a
//! lookup costs the same at depth 1000 as at depth 1. A page no branch in the chain wrote is the
//! trunk's, as of `trunk_at`, the fork epoch at which the branch's ancestry leaves the trunk.
//!
//! # Where the copies come from — the write ticket
//!
//! A version is retained, or a branch gets its own copy, at exactly one moment: the first
//! [`crate::storage::pager::Pager::add_dirty`] of a page in a transaction, which is also the only
//! place a [`crate::storage::pager::WriteTicket`] can be minted. So every page write is preceded by
//! the copy decision by construction:
//!
//! * a **branch** writing a page it does not yet own copies the page into a fresh slot of its own
//!   page space; writing a page it owns while a live child can still see the current version moves
//!   that version to the retained set and copies into a fresh slot;
//! * the **trunk** writing a page that a live child can still see copies the pre-image into a slot
//!   and retains it, before the write reaches the WAL — so neither the commit nor a later
//!   checkpoint that moves the new version into the database file can reach the child.
//!
//! # Reclamation
//!
//! A retained version is garbage once no live child of its node forked inside `[born, died)`.
//! Removing the child forked at `f` can only make versions containing `f` garbage, and a version
//! containing `f` becomes garbage exactly when it also lies strictly between `f`'s neighbouring
//! live siblings `lo` and `hi`: `born > lo` and `died <= hi`.
//!
//! r2d (r11-adversarial amendment 22) answers that with no keyed search. The live children a version holds are a
//! contiguous run of the fork-ordered child list, and the run only loses members, so each version is filed under
//! its FIRST live holder, in a leftist min-heap by `died` (Mendelson, Tarjan, Thorup and Zwick, SWAT 2004; Crane
//! 1972). The heaps live on gap nodes, one per fork epoch, in a union-find where a dead child links to the next
//! epoch, so `retain` finds the first live holder of `[born, now)` as find(born) (round 10's U7). When the child at
//! `f` goes, its neighbours come from the child list (`sib_prev` / `sib_next`); the heap filed under `f` is popped
//! while `died <= hi` (all of it, with no younger sibling), and the rest is melded into `hi`'s. Each popped version
//! is reached by its slab index, so it is freed as a tombstone with no search; a page's map drops its tombstones
//! when they exceed half of it.
//!
//! A branch whose handle has been dropped but that still has a live child or an open connection
//! is kept (its versions are still read through); it is freed the moment the last of those goes,
//! and freeing it may in turn free its parent.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmark this lane ships is single-threaded and says so.
//! * The persistent page maps are an index over slots the lineages own; they own nothing. A
//!   branch's `inherited` names only slots its ancestors keep for it (see "Resolution without the
//!   walk"), so dropping a map never frees a page and keeping one never pins a page.
//!
//! # Per-page version order (the fat node)
//!
//! Within one node, one page's retained versions have non-empty, pairwise disjoint `[born, died)`
//! ranges: the trunk retains `[written, epoch)` and then sets `written = epoch`, and a branch
//! retains `[owned.born, epoch)` and re-bears its current version at `epoch`. So `born` is unique
//! per (node, page), and the version a child forked at `f` sees is the one with the greatest
//! `born <= f`, provided `f < died`. The versions are kept in a map ordered by `born`, which makes
//! that lookup a predecessor search and a release a removal by key — Driscoll, Sarnak, Sleator and
//! Tarjan's fat node (JCSS 1989) with a search tree over its version stamps. [`Lineage::retain`]
//! refuses a version that would break the disjointness the search relies on.

use std::collections::{BTreeMap, HashMap, VecDeque};

use super::arena::{Arena, Slot};
use super::page_map::PageMap;
use super::{BranchId, BranchStats, BranchWork, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

pub(crate) struct BranchStore {
    inner: Mutex<StoreInner>,
    /// Live children of the trunk. Read without the lock on every trunk first-write so that a
    /// database with no branches pays one atomic load per written page and nothing else.
    ///
    /// The unlocked read is sound because the only transition that matters — 0 to 1 — happens in
    /// a trunk fork, which holds the trunk's WAL write lock; a trunk writer reading this holds the
    /// same lock. A 1-to-0 transition (a reap) racing the read only makes the writer take the lock
    /// and find nothing to do.
    trunk_children: AtomicUsize,
}

struct StoreInner {
    arena: Option<Arena>,
    next_id: u64,
    trunk: TrunkState,
    branches: HashMap<BranchId, BranchState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children in fork-epoch order, as an intrusive doubly-linked list (r11-adversarial amendment 21; the
    /// HyPer / Steam transaction-list shape, Böttcher et al., PVLDB 13(2) 2019, section 4.1). Fork epochs are
    /// handed out in increasing order, so appending keeps the list sorted, and a child's neighbours are its own
    /// `sib_prev` / `sib_next`, found with no search. `first` and `last` are the oldest and newest live child.
    first: Option<(u64, BranchId)>,
    last: Option<(u64, BranchId)>,
    nchildren: usize,
    /// Superseded versions kept because a live child forked while they were current, per page: `born` ->
    /// index into `recs` (see "Per-page version order" above). Freed versions stay as tombstones until the
    /// page's map is rebuilt.
    retained: HashMap<u32, PageVersions>,
    /// The retained versions' records (a slab), reached by index from the page maps and the heaps.
    recs: Vec<Rec>,
    free_recs: Vec<u32>,
    /// Gap nodes for the fork epochs `gap_base..=epoch` (the last is the open one, for the next fork): a
    /// union-find where a dead child's node links to the next epoch, each root holding the heap of the
    /// versions it is the first live holder of.
    gap: VecDeque<Gap>,
    gap_base: u64,
}

#[derive(Default)]
struct PageVersions {
    map: BTreeMap<u64, u32>,
    /// Tombstones in `map`.
    dead: u32,
}

struct Rec {
    died: u64,
    slot: Slot,
    page: u32,
    live: bool,
}

struct Gap {
    /// This node's own epoch while it is live or open; the next epoch once its child is gone.
    parent: u64,
    heap: Heap,
}

/// A leftist min-heap of retained versions ordered by `died` (Crane 1972): every node's right spine is no longer
/// than its left one, so a meld walks two right spines of O(log n) nodes each. Ported from turso e5230f575 (F-R5).
struct HeapNode {
    died: u64,
    rec: u32,
    /// Length of the right spine below and including this node.
    rank: u32,
    left: Heap,
    right: Heap,
}

type Heap = Option<Box<HeapNode>>;

fn rank(h: &Heap) -> u32 {
    h.as_ref().map_or(0, |n| n.rank)
}

/// Meld two heaps; `steps` gains the right-spine nodes visited (observation only).
fn meld(a: Heap, b: Heap, steps: &mut u64) -> Heap {
    match (a, b) {
        (None, h) | (h, None) => h,
        (Some(mut x), Some(mut y)) => {
            *steps += 1;
            if y.died < x.died {
                std::mem::swap(&mut x, &mut y);
            }
            let right = x.right.take();
            x.right = meld(right, Some(y), steps);
            if rank(&x.left) < rank(&x.right) {
                std::mem::swap(&mut x.left, &mut x.right);
            }
            x.rank = rank(&x.right) + 1;
            Some(x)
        }
    }
}

impl Drop for HeapNode {
    /// Iterative: a leftist heap's LEFT paths can be as long as the heap, and a recursive `Box` drop down one
    /// would overflow the stack.
    fn drop(&mut self) {
        let mut stack: Vec<Box<HeapNode>> = Vec::new();
        stack.extend(self.left.take());
        stack.extend(self.right.take());
        while let Some(mut n) = stack.pop() {
            stack.extend(n.left.take());
            stack.extend(n.right.take());
        }
    }
}

#[derive(Clone, Copy)]
struct Retained {
    born: u64,
    died: u64,
    slot: Slot,
}

struct TrunkState {
    lineage: Lineage,
    /// The trunk epoch of its last write to each page. Absent means "before the first fork that
    /// was live at the time", i.e. epoch 0, which is the conservative answer: it can only cause a
    /// retention that was not strictly needed, never skip one that was.
    written: HashMap<u32, u64>,
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has written.
    current: HashMap<u32, Owned>,
    /// The branch's committed schema. Shared with the parent at fork (an `Arc` clone), replaced by
    /// a committed DDL on the branch.
    schema: Arc<Schema>,
    /// The `Branch` handle is alive.
    handle: bool,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
    /// The fork epoch at which this branch's ancestry leaves the trunk: its own fork epoch if its
    /// parent is the trunk, else its parent's `trunk_at`.
    trunk_at: u64,
    /// Every arena page this branch sees through its ancestors: its parent's `view` at the fork.
    inherited: PageMap,
    /// `inherited` plus this branch's current pages, for its children to inherit. Built at the
    /// branch's first fork and kept current by its writes from then on; `None` until it forks.
    view: Option<PageMap>,
    /// This branch's neighbours in its parent's child list (see `Lineage::first`), with their fork epochs.
    sib_prev: Option<(u64, BranchId)>,
    sib_next: Option<(u64, BranchId)>,
}

#[derive(Clone, Copy)]
struct Owned {
    slot: Slot,
    born: u64,
}

impl Lineage {
    /// True if a live child forked in `[from, to)` can see a version current over that range.
    fn has_child_in(&self, from: u64, to: u64) -> bool {
        // Every caller passes the lineage's current epoch as `to`, and every live child forked below it, so a
        // child forked in [from, to) exists iff the newest live child forked at or after `from`.
        crate::turso_assert!(to == self.epoch, "has_child_in's O(1) form needs `to` = the lineage's epoch");
        from < to && self.last.is_some_and(|(e, _)| e >= from)
    }

    /// A new fork of this node: its epoch, and a new open gap node for the fork after it.
    fn next_fork(&mut self) -> u64 {
        if self.gap.is_empty() {
            self.gap_base = self.epoch;
            self.gap.push_back(Gap { parent: self.epoch, heap: None });
        }
        let f = self.epoch;
        self.epoch += 1;
        self.gap.push_back(Gap { parent: self.epoch, heap: None });
        f
    }

    /// The first gap node at or after epoch `e` that is a live child or the open node: the union-find root of
    /// `e`'s set, with path halving. Epochs below `gap_base` were all dead children, so they start at the base.
    fn find(&mut self, e: u64, steps: &mut u64) -> u64 {
        let base = self.gap_base;
        let mut x = e.max(base);
        loop {
            let p = self.gap[(x - base) as usize].parent;
            if p == x {
                return x;
            }
            let gp = self.gap[(p - base) as usize].parent;
            self.gap[(x - base) as usize].parent = gp;
            *steps += 1;
            x = gp;
        }
    }

    fn retain(&mut self, page: u32, v: Retained, work: &mut BranchWork) {
        let rec = Rec {
            died: v.died,
            slot: v.slot,
            page,
            live: true,
        };
        let idx = match self.free_recs.pop() {
            Some(i) => {
                self.recs[i as usize] = rec;
                i
            }
            None => {
                self.recs.push(rec);
                (self.recs.len() - 1) as u32
            }
        };
        let recs = &self.recs;
        let pv = self.retained.entry(page).or_default();
        crate::turso_assert!(
            pv.map
                .last_key_value()
                .is_none_or(|(_, &last)| recs[last as usize].died <= v.born),
            "a retained version overlaps an older one of the same page; the born-ordered lookup \
             would return the wrong one"
        );
        pv.map.insert(v.born, idx);
        let s = self.find(v.born, &mut work.uf_find_steps);
        crate::turso_assert!(
            s < self.epoch,
            "a version is retained only while a live child forked inside it"
        );
        let node = Some(Box::new(HeapNode {
            died: v.died,
            rec: idx,
            rank: 1,
            left: None,
            right: None,
        }));
        let g = &mut self.gap[(s - self.gap_base) as usize];
        g.heap = meld(g.heap.take(), node, &mut work.gc_meld_steps);
    }

    /// The retained version of `page` visible to a child forked at `f`: the born-predecessor of
    /// `f`, if it was still current at `f`. `examined` counts the versions compared against `f` —
    /// at most one; the O(log V) descent that finds it is not counted. A tombstone never answers: a
    /// freed version held no live child, so `f` (live) is past its `died`.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        let (_, &idx) = self.retained.get(&page)?.map.range(..=f).next_back()?;
        *examined += 1;
        let r = &self.recs[idx as usize];
        (r.live && f < r.died).then_some(r.slot)
    }

    /// Release every retained version that only the child forked at `f` could see. The caller has
    /// unlinked the child from the list; `hi` is its former younger neighbour's fork epoch. The heap
    /// filed under `f` is popped while `died <= hi` (all of it with no `hi`), the rest melded into
    /// `hi`'s, and `f`'s gap node linked to the next epoch. No keyed search (amendment 22).
    fn child_gone(
        &mut self,
        f: u64,
        _lo: Option<u64>,
        hi: Option<u64>,
        arena: &mut Arena,
        work: &mut BranchWork,
    ) -> usize {
        let t1 = std::time::Instant::now();
        crate::turso_assert!(f >= self.gap_base, "a live child's gap node was trimmed");
        let i = (f - self.gap_base) as usize;
        let mut heap = self.gap[i].heap.take();
        self.gap[i].parent = f + 1;
        let mut freed = 0;
        while let Some(root) = heap.as_ref() {
            work.gc_heap_examined += 1;
            if hi.is_some_and(|hi| root.died > hi) {
                break;
            }
            let mut root = heap.take().expect("checked above");
            heap = meld(root.left.take(), root.right.take(), &mut work.gc_meld_steps);
            let r = &mut self.recs[root.rec as usize];
            crate::turso_assert!(r.live, "a filed version was already freed");
            r.live = false;
            let (page, slot) = (r.page, r.slot);
            arena.release(slot);
            work.gc_examined += 1;
            freed += 1;
            let recs = &self.recs;
            let free = &mut self.free_recs;
            let pv = self.retained.get_mut(&page).expect("a filed version is listed");
            pv.dead += 1;
            let mut now_empty = false;
            if 2 * pv.dead as usize > pv.map.len() {
                work.page_rebuild_entries += pv.map.len() as u64;
                pv.map.retain(|_, idx| {
                    let live = recs[*idx as usize].live;
                    if !live {
                        free.push(*idx);
                    }
                    live
                });
                pv.dead = 0;
                now_empty = pv.map.is_empty();
            }
            if now_empty {
                self.retained.remove(&page);
            }
        }
        let t2 = std::time::Instant::now();
        match hi {
            Some(hi) => {
                let g = &mut self.gap[(hi - self.gap_base) as usize];
                g.heap = meld(g.heap.take(), heap, &mut work.gc_meld_steps);
            }
            None => crate::turso_assert!(
                heap.is_none(),
                "the youngest child left a version no live child holds"
            ),
        }
        // Every epoch below the oldest live child is a dead child with an empty heap.
        let keep = self.first.map_or(self.epoch, |(e, _)| e);
        while self.gap_base < keep {
            let n = self.gap.pop_front().expect("gap covers gap_base..=epoch");
            crate::turso_assert!(n.heap.is_none(), "a dead child's gap node still held versions");
            self.gap_base += 1;
        }
        work.cg_garbage_ns += (t2 - t1).as_nanos() as u64;
        work.cg_remove_ns += t2.elapsed().as_nanos() as u64;
        freed
    }

    fn release_all(self, arena: &mut Arena) -> Vec<Slot> {
        let slots: Vec<Slot> = self.recs.iter().filter(|r| r.live).map(|r| r.slot).collect();
        for &slot in &slots {
            arena.release(slot);
        }
        slots
    }
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

impl BranchStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(StoreInner {
                arena: None,
                next_id: 1,
                trunk: TrunkState {
                    lineage: Lineage::default(),
                    written: HashMap::new(),
                },
                branches: HashMap::new(),
                work: BranchWork::default(),
            }),
            trunk_children: AtomicUsize::new(0),
        }
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        !self.inner.lock().branches.is_empty()
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    pub(crate) fn fork_trunk(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        match &inner.arena {
            None => inner.arena = Some(Arena::new(page_size)),
            Some(arena) if arena.page_size() != page_size => {
                return Err(LimboError::InternalError(format!(
                    "branch arena holds {}-byte pages but the database now uses {page_size}",
                    arena.page_size()
                )));
            }
            Some(_) => {}
        }
        let id = BranchId(inner.next_id);
        inner.next_id += 1;
        let f = inner.trunk.lineage.next_fork();
        let prev = inner.trunk.lineage.last.replace((f, id));
        if inner.trunk.lineage.first.is_none() {
            inner.trunk.lineage.first = Some((f, id));
        }
        inner.trunk.lineage.nchildren += 1;
        if let Some((_, p)) = prev {
            inner.branches.get_mut(&p).expect("a listed child is kept").sib_next = Some((f, id));
        }
        let mut child = BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default());
        child.sib_prev = prev;
        inner.branches.insert(id, child);
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        let id = BranchId(inner.next_id);
        let st = inner.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        let f = st.lineage.next_fork();
        let prev = st.lineage.last.replace((f, id));
        if st.lineage.first.is_none() {
            st.lineage.first = Some((f, id));
        }
        st.lineage.nchildren += 1;
        let schema = st.schema.clone();
        let (current, inherited) = (&st.current, &st.inherited);
        let view = st
            .view
            .get_or_insert_with(|| {
                let mut view = inherited.clone();
                for (&page, owned) in current {
                    view.insert(page, owned.slot);
                }
                view
            })
            .clone();
        let trunk_at = st.trunk_at;
        inner.next_id += 1;
        if let Some((_, p)) = prev {
            inner.branches.get_mut(&p).expect("a listed child is kept").sib_next = Some((f, id));
        }
        let mut child = BranchState::new(parent, f, schema, trunk_at, view);
        child.sib_prev = prev;
        inner.branches.insert(id, child);
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.inner.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.open {
            return Err(LimboError::InvalidArgument(format!(
                "branch {} already has an open connection; a branch serves one connection at a \
                 time, because a second one's page cache would silently miss the first one's \
                 commits",
                id.0
            )));
        }
        st.open = true;
        Ok(st.schema.clone())
    }

    /// The connection on `id` has gone. Releases its write lock if a transaction was abandoned.
    pub(crate) fn close(&self, id: BranchId) {
        let mut inner = self.inner.lock();
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
        }
        self.collect(&mut inner, id);
    }

    /// The `Branch` handle has gone.
    /// Observation only (r11-adversarial PREREG amendment 19): read, and change nothing, what
    /// `release_handle(id)` of an unforked branch would touch, so a harness can take those cache misses outside
    /// its timed window. `trees` false: the branch's own state (its table entry, its `current` map, the arena
    /// words of its pages). `trees` true: that plus every B-tree path its parent's `child_gone` walks, with the
    /// same neighbours and the same garbage. Returns a value built from what was read, so no read is elided.
    #[doc(hidden)]
    pub(crate) fn bench_prewarm(&self, id: BranchId, trees: bool) -> u64 {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return 0;
        };
        let mut h = st.fork_epoch ^ st.trunk_at ^ u64::from(st.handle) ^ st.lineage.nchildren as u64;
        for owned in st.current.values() {
            h = h.wrapping_add(u64::from(owned.slot) ^ owned.born);
            if let Some(a) = inner.arena.as_ref() {
                h ^= u64::from(a.is_free(owned.slot));
            }
        }
        if !trees {
            return h;
        }
        let lineage = if st.parent.is_trunk() {
            &inner.trunk.lineage
        } else {
            match inner.branches.get(&st.parent) {
                Some(p) => &p.lineage,
                None => return h,
            }
        };
        let f = st.fork_epoch;
        // The unlink touches both neighbours' states (amendment 21).
        for (_, sib) in st.sib_prev.iter().chain(st.sib_next.iter()) {
            if let Some(s) = inner.branches.get(sib) {
                h ^= s.fork_epoch ^ u64::from(s.handle);
            }
        }
        h ^= lineage.nchildren as u64;
        let lo = st.sib_prev.map(|(e, _)| e);
        let hi = st.sib_next.map(|(e, _)| e);
        // r2d: f's gap node, the heap root filed under it and its record, and hi's gap node.
        if let Some(g) = f.checked_sub(lineage.gap_base).and_then(|i| lineage.gap.get(i as usize)) {
            if let Some(root) = g.heap.as_ref() {
                h ^= root.died ^ u64::from(lineage.recs[root.rec as usize].slot);
            }
        }
        if let Some(g) = hi
            .and_then(|hi| hi.checked_sub(lineage.gap_base))
            .and_then(|i| lineage.gap.get(i as usize))
        {
            h ^= u64::from(g.heap.is_some());
        }
        h ^ lo.unwrap_or(0) ^ hi.unwrap_or(0)
    }

    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut inner = self.inner.lock();
        let held = std::time::Instant::now();
        let Some(st) = inner.branches.get_mut(&id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        st.handle = false;
        let freed_pages = self.collect(&mut inner, id);
        let deferred = inner.branches.contains_key(&id);
        inner.work.reap_hold_ns += held.elapsed().as_nanos() as u64;
        Reaped {
            freed_pages,
            deferred,
        }
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut inner = self.inner.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        st.writer = true;
        Ok(())
    }

    pub(crate) fn end_write(&self, id: BranchId) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.writer = false;
        }
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.inner
            .lock()
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let inner = self.inner.lock();
        Ok(inner.branches.get(&id).ok_or_else(|| gone(id))?.schema.clone())
    }

    /// The copy decision for a branch's first write to `page` in a transaction. `pre_image` is the
    /// page as the branch sees it now — the version this write supersedes.
    pub(crate) fn first_write_branch(
        &self,
        id: BranchId,
        page: u32,
        pre_image: &[u8],
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena,
            branches,
            work,
            ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        crate::turso_assert!(st.writer, "branch page written outside a write transaction");
        let epoch = st.lineage.epoch;
        match st.current.get(&page).copied() {
            None => {
                let slot = arena.alloc();
                arena.page_mut(slot).copy_from_slice(pre_image);
                st.current.insert(page, Owned { slot, born: epoch });
                if let Some(view) = st.view.as_mut() {
                    view.insert(page, slot);
                }
            }
            Some(owned) if owned.born == epoch => {}
            Some(owned) => {
                if st.lineage.has_child_in(owned.born, epoch) {
                    let slot = arena.alloc();
                    arena.page_mut(slot).copy_from_slice(pre_image);
                    st.lineage.retain(
                        page,
                        Retained {
                            born: owned.born,
                            died: epoch,
                            slot: owned.slot,
                        },
                        work,
                    );
                    st.current.insert(page, Owned { slot, born: epoch });
                    if let Some(view) = st.view.as_mut() {
                        view.insert(page, slot);
                    }
                } else {
                    // No live child can see the current version: it is rewritten in its own slot,
                    // which is already the one `view` names.
                    st.current.insert(
                        page,
                        Owned {
                            slot: owned.slot,
                            born: epoch,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena, trunk, work, ..
        } = &mut *inner;
        let epoch = trunk.lineage.epoch;
        let born = trunk.written.get(&page).copied().unwrap_or(0);
        if born >= epoch {
            return;
        }
        if trunk.lineage.has_child_in(born, epoch) {
            let arena = arena.as_mut().expect("the trunk has a child, so the arena exists");
            let slot = arena.alloc();
            arena.page_mut(slot).copy_from_slice(pre_image);
            trunk.lineage.retain(
                page,
                Retained {
                    born,
                    died: epoch,
                    slot,
                },
                work,
            );
        }
        trunk.written.insert(page, epoch);
    }

    /// Commit a branch's dirty pages into the slots their copy decisions allocated.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena, branches, ..
        } = &mut *inner;
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if pages.is_empty() {
            return Ok(());
        }
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        for page in pages {
            let no = page.get().id as u32;
            let owned = st.current.get(&no).copied().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "branch {} committed page {no} with no copy decision behind it",
                    id.0
                ))
            })?;
            crate::turso_assert!(
                owned.born == st.lineage.epoch,
                "a committed branch page was decided in an earlier epoch"
            );
            arena
                .page_mut(owned.slot)
                .copy_from_slice(page.get_contents().as_slice());
        }
        Ok(())
    }

    pub(crate) fn set_schema(&self, id: BranchId, schema: Arc<Schema>) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.inner.lock();
        let (mut levels, mut examined) = (0, 0);
        let resolved = inner.resolve(id, page, &mut levels, &mut examined);
        inner.work.resolve_calls += 1;
        inner.work.resolve_levels += levels;
        inner.work.resolve_retained_examined += examined;
        let Some(slot) = resolved? else {
            return Ok(false);
        };
        out.copy_from_slice(
            inner
                .arena
                .as_ref()
                .expect("a slot resolved, so the arena exists")
                .page(slot),
        );
        Ok(true)
    }

    pub(crate) fn stats(&self) -> BranchStats {
        let inner = self.inner.lock();
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
            work: inner.work,
        }
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        slots.extend(st.lineage.recs.iter().filter(|r| r.live).map(|r| r.slot));
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<u32> {
        self.inner
            .lock()
            .arena
            .as_ref()
            .map_or_else(Vec::new, |a| a.slots_in_use())
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self.inner
            .lock()
            .arena
            .as_ref()
            .is_some_and(|a| a.is_free(slot))
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist. Returns the number of arena pages released.
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId) -> usize {
        let mut freed = 0;
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return freed;
            };
            if st.handle || st.open || st.lineage.nchildren != 0 {
                return freed;
            }
            let st = inner.branches.remove(&id).expect("just looked it up");
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                ..
            } = &mut *inner;
            let arena = arena.as_mut().expect("a branch existed, so the arena does");
            for owned in st.current.values() {
                arena.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(arena).len();
            // Unlink `st` from its parent's child list in O(1): its neighbours are in hand (amendment 21).
            let t = std::time::Instant::now();
            if let Some((_, p)) = st.sib_prev {
                branches.get_mut(&p).expect("a listed sibling is kept").sib_next = st.sib_next;
            }
            if let Some((_, n)) = st.sib_next {
                branches.get_mut(&n).expect("a listed sibling is kept").sib_prev = st.sib_prev;
            }
            let lineage = if st.parent.is_trunk() {
                &mut trunk.lineage
            } else {
                &mut branches
                    .get_mut(&st.parent)
                    .expect("a live branch's parent is kept while the branch lives")
                    .lineage
            };
            crate::turso_assert!(
                (st.sib_prev.is_some() || lineage.first == Some((st.fork_epoch, id)))
                    && (st.sib_next.is_some() || lineage.last == Some((st.fork_epoch, id))),
                "a child at an end of its parent's list is not that end"
            );
            if st.sib_prev.is_none() {
                lineage.first = st.sib_next;
            }
            if st.sib_next.is_none() {
                lineage.last = st.sib_prev;
            }
            lineage.nchildren -= 1;
            work.cg_children_ns += t.elapsed().as_nanos() as u64;
            let (lo, hi) = (st.sib_prev.map(|(e, _)| e), st.sib_next.map(|(e, _)| e));
            freed += lineage.child_gone(st.fork_epoch, lo, hi, arena, work);
            work.child_gone_ns += t.elapsed().as_nanos() as u64;
            work.child_gone_calls += 1;
            if st.parent.is_trunk() {
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return freed;
            }
            id = st.parent;
        }
    }
}

impl StoreInner {
    /// `levels` counts the nodes consulted — the branch (its own pages and its `inherited` map),
    /// then the trunk if neither holds the page — and `examined` the retained versions compared.
    fn resolve(
        &self,
        id: BranchId,
        page: u32,
        levels: &mut u64,
        examined: &mut u64,
    ) -> Result<Option<Slot>> {
        *levels += 1;
        let st = self.branches.get(&id).ok_or_else(|| gone(id))?;
        // A branch sees all of its own versions; its ancestors' as of its fork, which `inherited`
        // froze then.
        if let Some(owned) = st.current.get(&page) {
            return Ok(Some(owned.slot));
        }
        if let Some(slot) = st.inherited.get(page) {
            return Ok(Some(slot));
        }
        *levels += 1;
        let at = st.trunk_at;
        if let Some(slot) = self.trunk.lineage.retained_at(page, at, examined) {
            return Ok(Some(slot));
        }
        let born = self.trunk.written.get(&page).copied().unwrap_or(0);
        if born > at {
            // The trunk overwrote this page after the fork and nothing was retained: the ordinary
            // read path would return the NEW version. Refuse rather than serve it.
            return Err(LimboError::Corrupt(format!(
                "branch {} would read trunk page {page} written after its fork; the pre-image was \
                 not retained",
                id.0
            )));
        }
        Ok(None)
    }
}

impl BranchState {
    fn new(
        parent: BranchId,
        fork_epoch: u64,
        schema: Arc<Schema>,
        trunk_at: u64,
        inherited: PageMap,
    ) -> Self {
        Self {
            parent,
            fork_epoch,
            lineage: Lineage::default(),
            current: HashMap::new(),
            schema,
            handle: true,
            open: false,
            writer: false,
            trunk_at,
            inherited,
            view: None,
            sib_prev: None,
            sib_next: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const PAGE: usize = 64;
    const PAGES: u32 = 6;

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

    /// The trunk's retained-version index against a brute-force model, through the store's own
    /// entry points (`fork_trunk`, `first_write_trunk`, `release_handle`, `resolve_into`), and the
    /// garbage query's cost against its contract: a reap examines exactly the versions it frees,
    /// plus one heap root when some version filed under the reaped child survives it — a version
    /// with `born` in `(lo, f]` and `died > hi` — and never any other version; and no reap's melds
    /// walk more than 2·(log2(V + 1) + 1) right-spine nodes.
    ///
    /// The trunk rewrites a handful of pages, several per epoch, so versions share `born` and `died`
    /// across pages; children are reaped oldest-first, newest-first and at random, so the garbage
    /// query runs with no older sibling, with no younger one, and with both. After every step, every
    /// live child must read, for every page, the trunk's page as of its fork — from its retained
    /// version or, when there is none, from the trunk's current one — and the arena must hold
    /// exactly the versions whose `[born, died)` still contains a live child's fork epoch.
    #[test]
    fn retained_versions_match_a_model_under_forks_rewrites_and_reaps_in_every_order() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run(seed);
        }
    }

    fn run(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        // The trunk's current generation of each page, and each live child's view at its fork.
        let mut current: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, u64>)> = Vec::new();
        // Every version ever retained: (page, born, died), with `born`/`died` in fork epochs.
        let mut history: Vec<(u32, u64, u64)> = Vec::new();
        let mut written: HashMap<u32, u64> = HashMap::new();
        let mut epoch = 0u64;
        let mut generation = 0u64;
        let (mut freed_oldest, mut freed_newest, mut freed_middle) = (0, 0, 0);
        for step in 0..1500 {
            match rng.below(10) {
                0..=2 if live.len() < 40 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    live.push((id, epoch, current.clone()));
                    epoch += 1;
                }
                0..=5 => {
                    // One trunk transaction: first writes of up to three pages in this epoch.
                    for _ in 0..=rng.below(3) {
                        let page = rng.below(PAGES as u64) as u32;
                        if store.trunk_has_children() {
                            let born = written.get(&page).copied().unwrap_or(0);
                            if born < epoch {
                                if live.iter().any(|&(_, f, _)| born <= f && f < epoch) {
                                    history.push((page, born, epoch));
                                }
                                written.insert(page, epoch);
                            }
                            store.first_write_trunk(page, &image(current[&page]));
                        }
                        generation += 1;
                        current.insert(page, generation);
                    }
                }
                _ if !live.is_empty() => {
                    let at = match rng.below(3) {
                        0 => 0,
                        1 => live.len() - 1,
                        _ => rng.below(live.len() as u64) as usize,
                    };
                    let lo = at.checked_sub(1).map(|i| live[i].1);
                    let hi = live.get(at + 1).map(|c| c.1);
                    let (id, f, _) = live.remove(at);
                    let filed = |&&(_, born, died): &&(u32, u64, u64)| {
                        lo.is_none_or(|lo| born > lo) && born <= f && f < died
                    };
                    let survivors = history
                        .iter()
                        .filter(filed)
                        .filter(|&&(_, _, died)| hi.is_some_and(|hi| died > hi))
                        .count() as u64;
                    let versions = history.len() as f64;
                    let before = store.stats();
                    let reaped = store.release_handle(id);
                    let after = store.stats();
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}");
                    assert_eq!(
                        before.arena_slots_in_use - after.arena_slots_in_use,
                        reaped.freed_pages,
                        "seed {seed:#x} step {step}: the reap's report disagrees with the arena"
                    );
                    let examined = after.work.gc_heap_examined - before.work.gc_heap_examined;
                    let contract = reaped.freed_pages as u64 + u64::from(survivors > 0);
                    assert_eq!(
                        examined, contract,
                        "seed {seed:#x} step {step}: reaping the child forked at {f} (lo {lo:?}, \
                         hi {hi:?}, {survivors} filed survivors) examined {examined} heap roots"
                    );
                    let steps = after.work.gc_meld_steps - before.work.gc_meld_steps;
                    let bound = 2.0 * ((versions + 1.0).log2() + 1.0) * (1.0 + examined as f64);
                    assert!(
                        steps as f64 <= bound,
                        "seed {seed:#x} step {step}: the reap's melds walked {steps} nodes over \
                         {versions} versions"
                    );
                    assert_eq!(
                        after.work.gc_range_entries, before.work.gc_range_entries,
                        "the range walks are gone"
                    );
                    if reaped.freed_pages > 0 {
                        match at {
                            0 => freed_oldest += 1,
                            _ if at == live.len() => freed_newest += 1,
                            _ => freed_middle += 1,
                        }
                    }
                }
                _ => {}
            }
            let alive: HashSet<(u32, u64, u64)> = history
                .iter()
                .copied()
                .filter(|&(_, born, died)| live.iter().any(|&(_, f, _)| born <= f && f < died))
                .collect();
            assert_eq!(
                store.stats().arena_slots_in_use,
                alive.len(),
                "seed {seed:#x} step {step}: the arena holds a version no live child can see, or \
                 lost one a live child can"
            );
            // A version no live child can see never becomes visible again.
            history.retain(|v| alive.contains(v));
            let mut buf = vec![0u8; PAGE];
            for (id, f, view) in &live {
                for page in 0..PAGES {
                    let in_arena = store.resolve_into(*id, page, &mut buf).unwrap();
                    let got = if in_arena {
                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                    } else {
                        current[&page]
                    };
                    assert_eq!(
                        got, view[&page],
                        "seed {seed:#x} step {step}: child forked at {f} read the wrong page {page}"
                    );
                }
            }
        }
        // The walk above must have freed versions through all three shapes of the garbage query;
        // otherwise a green run says nothing about the shape it skipped.
        assert!(
            freed_oldest > 0 && freed_newest > 0 && freed_middle > 0,
            "seed {seed:#x}: reaps that freed versions: oldest {freed_oldest}, newest \
             {freed_newest}, middle {freed_middle}"
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: versions leaked");
    }

    /// A committed branch page holding `image(generation)`, as the pager hands it to
    /// `commit_pages`.
    fn page_with(page: u32, generation: u64) -> PageRef {
        let p = Arc::new(crate::storage::pager::Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(PAGE));
        buffer.as_mut_slice().copy_from_slice(&image(generation));
        p.get().buffer = Some(buffer);
        p
    }

    /// Branch TREES — forks from the trunk and from branches, deep chains, writes on the trunk and
    /// on branches before and after they fork, deferred reaps of branches with live children —
    /// against a model in which each branch is a plain copy of its parent's pages at its fork.
    /// Every live branch must read, for every page, what the model says, through
    /// `resolve_into`, the path the pager uses.
    #[test]
    fn every_branch_of_a_random_tree_reads_its_parent_as_of_its_fork() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_tree(seed);
        }
    }

    struct Node {
        id: BranchId,
        sees: HashMap<u32, u64>,
        handle: bool,
        depth: usize,
        /// This branch has forked a child, so its writes must reach the view its children inherit.
        forked: bool,
    }

    fn run_tree(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut nodes: Vec<Node> = Vec::new();
        let mut generation = 0u64;
        let (mut deferred, mut max_depth, mut wrote_after_fork) = (0, 0, 0);
        for step in 0..2500 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            match rng.below(12) {
                0 if live.len() < 60 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    nodes.push(Node {
                        id,
                        sees: trunk.clone(),
                        handle: true,
                        depth: 1,
                        forked: false,
                    });
                }
                1..=3 if !live.is_empty() && live.len() < 60 => {
                    // Half the time the newest live branch, so that chains grow deep.
                    let parent = if rng.below(2) == 0 {
                        *live.last().unwrap()
                    } else {
                        live[rng.below(live.len() as u64) as usize]
                    };
                    let id = store.fork_branch(nodes[parent].id).unwrap();
                    let (sees, depth) = (nodes[parent].sees.clone(), nodes[parent].depth + 1);
                    nodes[parent].forked = true;
                    max_depth = max_depth.max(depth);
                    nodes.push(Node {
                        id,
                        sees,
                        handle: true,
                        depth,
                        forked: false,
                    });
                }
                4..=5 => {
                    let page = rng.below(u64::from(PAGES)) as u32;
                    if store.trunk_has_children() {
                        store.first_write_trunk(page, &image(trunk[&page]));
                    }
                    generation += 1;
                    trunk.insert(page, generation);
                }
                6..=9 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    if nodes[v].forked {
                        wrote_after_fork += 1;
                    }
                    let id = nodes[v].id;
                    store.begin_write(id).unwrap();
                    let mut committed = Vec::new();
                    for _ in 0..=rng.below(2) {
                        let page = rng.below(u64::from(PAGES)) as u32;
                        if committed.iter().any(|p: &PageRef| p.get().id == page as usize) {
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
                    if store.release_handle(nodes[v].id).deferred {
                        deferred += 1;
                    }
                }
                _ => {}
            }
            let mut buf = vec![0u8; PAGE];
            for n in nodes.iter().filter(|n| n.handle) {
                for page in 0..PAGES {
                    let got = if store.resolve_into(n.id, page, &mut buf).unwrap() {
                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                    } else {
                        trunk[&page]
                    };
                    assert_eq!(
                        got,
                        n.sees[&page],
                        "seed {seed:#x} step {step}: branch {} at depth {} read the wrong page {page}",
                        n.id.0,
                        n.depth
                    );
                }
            }
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing.
        assert!(
            max_depth >= 10 && deferred > 0 && wrote_after_fork > 0,
            "seed {seed:#x}: max depth {max_depth}, deferred reaps {deferred}, writes by a branch \
             after its first fork {wrote_after_fork}"
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    }
}
