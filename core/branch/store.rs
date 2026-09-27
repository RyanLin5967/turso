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
//! fork (an ancestor's later write goes to a new slot and the version the branch saw keeps its
//! own). So each branch carries `view`, a persistent [`PageMap`] of every arena page it sees: its
//! parent's `view` at the fork, then updated by the branch's own writes. A fork clones the parent's
//! `view` in O(1) and a write path-copies O(log P) trie nodes, so a lookup costs the same at depth
//! 1000 as at depth 1. A page no branch in the chain wrote is the trunk's, as of `trunk_at`, the
//! fork epoch at which the branch's ancestry leaves the trunk.
//!
//! # Where the copies come from — the write ticket
//!
//! A version is retained, or a branch gets its own copy, at exactly one moment: the first
//! [`crate::storage::pager::Pager::add_dirty`] of a page in a transaction, which is also the only
//! place a [`crate::storage::pager::WriteTicket`] can be minted. So every page write is preceded by
//! the copy decision by construction:
//!
//! * a **branch** writes in place only into a slot no other map names (see "Reclamation");
//!   otherwise it copies the page into a fresh slot and its map points there, while every map that
//!   named the old slot — a child's, a sibling's, an ancestor's — keeps it;
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
//! The live children a version holds are a contiguous run of the node's live children (those
//! forked in `[born, died)`), and the run only ever loses members. So each version is filed under
//! its FIRST live holder, in a min-heap ordered by `died`: priority queues on the nodes of a
//! union–find over the live list (Mendelson, Tarjan, Thorup and Zwick, SWAT 2004). When the child
//! at `f` goes, the versions filed under it are exactly those with `born` in `(lo, f]`; of those,
//! the garbage is the prefix of its heap with `died <= hi` (all of it, if there is no younger live
//! sibling), and the rest now start at `hi`, so the heap is melded into `hi`'s. A reap therefore
//! visits what it frees plus one heap root, and melds two heaps — leftist heaps (Crane 1972), so
//! O(log V) worst case — and never walks a surviving version. (The two range walks this replaces,
//! ZFS-deadlist indexes by `born` and by `died`, cost twice the smaller range whatever it freed:
//! a reap could walk every version and free none.)
//!
//! That is the TRUNK's reclamation. A branch's versions are reclaimed by reference counts instead
//! (Rodeh, "B-trees, Shadowing, and Clones", ACM TOS 2008): every branch page lives in a slot that
//! counts the page-map leaf nodes naming it, a branch resolves and forks through one map (its
//! `view`), and a branch write copies the path to its leaf and takes a new slot only if another map
//! can still reach the old one — otherwise it writes in place. So a branch slot lives exactly as
//! long as some live branch's map can reach it: a version a descendant has overwritten, or one only
//! a dead ancestor named, is freed when the last map naming it goes, which interval retention
//! cannot see (the interval predicate ignores descendants' writes).
//!
//! A branch whose handle has been dropped and that has no open connection releases its map at once
//! (its children hold their own). If it still has two or more live children its state is kept as
//! their fork point; with one it is spliced out, the child taking its place in its parent; with
//! none it is freed, and freeing it may in turn free or splice its parent.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmark this lane ships is single-threaded and says so.
//! * Branch space held is branch space reachable, exactly — but the TRUNK's retained versions are
//!   kept by the interval predicate, so a trunk version a trunk child has itself overwritten is
//!   kept for that child although it cannot read it (at most one per page the child wrote).
//!
//! # Per-page version order (the fat node)
//!
//! Within one node, one page's retained versions have non-empty, pairwise disjoint `[born, died)`
//! ranges: the trunk retains `[written, epoch)` and then sets `written = epoch` (branches no longer
//! retain; see "Reclamation"). So `born` is unique
//! per (node, page), and the version a child forked at `f` sees is the one with the greatest
//! `born <= f`, provided `f < died`. The versions are kept in a map ordered by `born`, which makes
//! that lookup a predecessor search and a release a removal by key — Driscoll, Sarnak, Sleator and
//! Tarjan's fat node (JCSS 1989) with a search tree over its version stamps. [`Lineage::retain`]
//! refuses a version that would break the disjointness the search relies on.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use super::arena::{Arena, Slot};
use super::linear_map::LinearMap;
use super::page_map::{MapWork, PageMap};
use super::{BranchId, BranchStats, BranchWork, HoldMax, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard};
use crate::{LimboError, Result};

/// Time every store-mutex hold (observation only; off unless a harness turns it on).
static HOLD_TIMING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn set_hold_timing(on: bool) {
    HOLD_TIMING.store(on, std::sync::atomic::Ordering::Relaxed);
}

/// The counters a hold's maxima are taken over, in [`HoldMax`] order after `holds`: arena slots
/// released, heap roots examined, meld steps, map nodes released, slot references dropped, pages
/// copied by evacuation. Observation only.
fn hold_counters(inner: &StoreInner) -> [u64; 6] {
    let (released, copied) = inner
        .arena
        .as_ref()
        .map_or((0, 0), |a| (a.released(), a.compaction().0));
    [
        released,
        inner.work.gc_heap_examined,
        inner.work.gc_meld_steps,
        inner.work.reclaim_map_nodes,
        inner.work.reclaim_slot_decrefs,
        copied,
    ]
}

/// A hold of the store mutex that folds what it did into [`HoldMax`] when it is released
/// (r11-bigtxn's counting lock). Observation only: nothing in the mechanism reads it.
struct Hold<'a> {
    guard: Option<MutexGuard<'a, StoreInner>>,
    /// Started once the lock is ours, so a contended acquire's wait is not counted as hold.
    start: Option<std::time::Instant>,
    at: [u64; 6],
}

impl Hold<'_> {
    fn account(inner: &mut StoreInner, start: Option<std::time::Instant>, at: [u64; 6]) {
        let now = hold_counters(inner);
        let d: [u64; 6] = std::array::from_fn(|i| now[i] - at[i]);
        inner.work.lock_holds += 1;
        let max = &mut inner.hold_max;
        max.holds += 1;
        max.freed = max.freed.max(d[0]);
        max.heap_examined = max.heap_examined.max(d[1]);
        max.meld_steps = max.meld_steps.max(d[2]);
        max.map_nodes = max.map_nodes.max(d[3]);
        max.slot_decrefs = max.slot_decrefs.max(d[4]);
        max.frames_copied = max.frames_copied.max(d[5]);
        if let Some(start) = start {
            max.ns = max.ns.max(start.elapsed().as_nanos() as u64);
        }
    }
}

impl Deref for Hold<'_> {
    type Target = StoreInner;
    fn deref(&self) -> &StoreInner {
        self.guard.as_ref().expect("a hold owns its guard until it is dropped")
    }
}

impl DerefMut for Hold<'_> {
    fn deref_mut(&mut self) -> &mut StoreInner {
        self.guard.as_mut().expect("a hold owns its guard until it is dropped")
    }
}

impl Drop for Hold<'_> {
    fn drop(&mut self) {
        if let Some(mut guard) = self.guard.take() {
            Self::account(&mut guard, self.start, self.at);
        }
    }
}

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
    branches: LinearMap<BranchId, BranchState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
    /// Observation only: the largest `capacity()` the `branches` table has had.
    branch_table_peak: usize,
    /// Observation only; see [`HoldMax`].
    hold_max: HoldMax,
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch, each with the retained versions it is the first live holder
    /// of. Fork epochs are unique within a parent.
    children: BTreeMap<u64, Child>,
    /// Superseded versions kept because a live child forked while they were current, per page and
    /// ordered by `born` (see "Per-page version order" above).
    retained: LinearMap<u32, BTreeMap<u64, Retained>>,
}

struct Child {
    id: BranchId,
    /// The retained versions whose first live holder this child is, by `died`.
    first_of: Heap,
}

impl Child {
    fn new(id: BranchId) -> Self {
        Self { id, first_of: None }
    }
}

/// A leftist min-heap of retained versions ordered by `died` (Crane 1972): every node's right
/// spine is no longer than its left one, so a meld walks two right spines of O(log n) nodes each,
/// worst case.
struct HeapNode {
    died: u64,
    born: u64,
    page: u32,
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
    /// Iterative: a leftist heap's LEFT paths can be as long as the heap, and a recursive `Box`
    /// drop down one would overflow the stack.
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
    written: LinearMap<u32, u64>,
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has written.
    current: LinearMap<u32, Owned>,
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
    /// Every arena page this branch sees: its parent's `view` at the fork (a clone, one reference
    /// count), updated by each of the branch's writes, so a fork clones it in O(1) whatever the
    /// branch wrote before. Its leaves hold the counted references that keep branch slots alive
    /// (see "Reclamation"). `None` once the branch is dead: it released the map.
    view: Option<PageMap>,
}

#[derive(Clone, Copy)]
struct Owned {
    slot: Slot,
    born: u64,
}

impl Lineage {
    /// True if a live child forked in `[from, to)` can see a version current over that range.
    fn has_child_in(&self, from: u64, to: u64) -> bool {
        from < to && self.children.range(from..to).next().is_some()
    }

    fn retain(&mut self, page: u32, v: Retained, work: &mut BranchWork) {
        if !self.retained.contains_key(&page) {
            counted_insert(
                &mut self.retained,
                page,
                BTreeMap::new(),
                &mut work.page_table_moved,
            );
        }
        let versions = self.retained.get_mut(&page).expect("inserted above");
        crate::turso_assert!(
            versions
                .last_key_value()
                .is_none_or(|(_, last)| last.died <= v.born),
            "a retained version overlaps an older one of the same page; the born-ordered lookup \
             would return the wrong one"
        );
        versions.insert(v.born, v);
        let (_, first) = self
            .children
            .range_mut(v.born..v.died)
            .next()
            .expect("a version is retained only while a live child forked inside it");
        let node = Some(Box::new(HeapNode {
            died: v.died,
            born: v.born,
            page,
            rank: 1,
            left: None,
            right: None,
        }));
        first.first_of = meld(first.first_of.take(), node, &mut work.gc_meld_steps);
    }

    /// The retained version of `page` visible to a child forked at `f`: the born-predecessor of
    /// `f`, if it was still current at `f`. `examined` counts the versions compared against `f` —
    /// at most one; the O(log V) descent that finds it is not counted.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        let (_, v) = self.retained.get(&page)?.range(..=f).next_back()?;
        *examined += 1;
        (f < v.died).then_some(v.slot)
    }

    /// Detach the child forked at `f` and release every retained version that only it could see:
    /// the versions filed under `f` (it was their first live holder) whose `died` is at most the
    /// next live sibling's fork epoch. The rest move to that sibling's heap.
    fn child_gone(&mut self, f: u64, arena: &mut Arena, work: &mut BranchWork) -> usize {
        let child = self.children.remove(&f);
        crate::turso_assert!(child.is_some(), "detached a child the parent does not list");
        let mut heap = child.expect("checked above").first_of;
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        let mut freed = 0;
        while let Some(root) = heap.as_ref() {
            work.gc_heap_examined += 1;
            if hi.is_some_and(|hi| root.died > hi) {
                break;
            }
            let mut root = heap.take().expect("checked above");
            heap = meld(root.left.take(), root.right.take(), &mut work.gc_meld_steps);
            let versions = self
                .retained
                .get_mut(&root.page)
                .expect("a filed version is listed");
            let v = versions.remove(&root.born).expect("a filed version is listed");
            crate::turso_assert!(v.died == root.died, "a filed version disagrees with its listing");
            work.gc_examined += 1;
            if versions.is_empty() {
                self.retained.remove(&root.page, &mut work.page_table_moved);
            }
            arena.release(v.slot);
            freed += 1;
        }
        match hi {
            Some(hi) => {
                let next = self.children.get_mut(&hi).expect("hi is a live child");
                next.first_of = meld(next.first_of.take(), heap, &mut work.gc_meld_steps);
            }
            None => crate::turso_assert!(
                heap.is_none(),
                "the youngest child left a version no live child holds"
            ),
        }
        freed
    }
}

/// `map.insert`, adding to `moved` the entries a bucket split relinked (observation only; see
/// [`BranchWork::branch_table_moved`]). A linear-hashing map never rehashes the whole table, so
/// there is no capacity to report (see [`note_branch_table`]).
fn counted_insert<K: std::hash::Hash + Eq, V>(
    map: &mut LinearMap<K, V>,
    k: K,
    v: V,
    moved: &mut u64,
) -> Option<usize> {
    map.insert(k, v, moved);
    None
}

/// Attribute a `branches` relocation: a capacity above every earlier one is a doubling, anything
/// else an in-place rebuild.
fn note_branch_table(work: &mut BranchWork, peak: &mut usize, after: Option<usize>) {
    if let Some(after) = after {
        if after > *peak {
            *peak = after;
            work.branch_table_resizes += 1;
        } else {
            work.branch_table_rehashes += 1;
        }
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
                    written: LinearMap::default(),
                },
                branches: LinearMap::default(),
                work: BranchWork::default(),
                branch_table_peak: 0,
                hold_max: HoldMax::default(),
            }),
            trunk_children: AtomicUsize::new(0),
        }
    }

    /// Take the store mutex for the mechanism. Every hold it returns is counted in
    /// [`BranchWork::lock_holds`] and folded into [`HoldMax`]; the observation calls (`stats`,
    /// `take_hold_max`, the membership diagnostics) lock directly so they do not count themselves.
    fn lock(&self) -> Hold<'_> {
        let guard = self.inner.lock();
        let start = HOLD_TIMING
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(std::time::Instant::now);
        let at = hold_counters(&guard);
        Hold {
            guard: Some(guard),
            start,
            at,
        }
    }

    /// The per-hold maxima since the previous call, which this call resets.
    pub(crate) fn take_hold_max(&self) -> HoldMax {
        std::mem::take(&mut self.inner.lock().hold_max)
    }

    /// Bytes of one `branches` entry, for the adversarial driver's space estimate.
    pub(crate) fn branch_entry_bytes() -> usize {
        std::mem::size_of::<(BranchId, BranchState)>()
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        !self.lock().branches.is_empty()
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    pub(crate) fn fork_trunk(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        let mut inner = self.lock();
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
        let f = inner.trunk.lineage.epoch;
        inner.trunk.lineage.epoch += 1;
        inner.trunk.lineage.children.insert(f, Child::new(id));
        let StoreInner {
            branches,
            work,
            branch_table_peak,
            ..
        } = &mut *inner;
        let after = counted_insert(
            branches,
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
            &mut work.branch_table_moved,
        );
        note_branch_table(work, branch_table_peak, after);
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.lock();
        let id = BranchId(inner.next_id);
        let StoreInner {
            branches,
            work,
            branch_table_peak,
            ..
        } = &mut *inner;
        let st = branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
        // A dead branch (no handle, no connection) has released its map; nothing in the engine can
        // name it, and a store-level caller gets `gone` rather than a fork of nothing.
        let view = st.view.clone().ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        let f = st.lineage.epoch;
        st.lineage.epoch += 1;
        st.lineage.children.insert(f, Child::new(id));
        let schema = st.schema.clone();
        let trunk_at = st.trunk_at;
        let after = counted_insert(
            branches,
            id,
            BranchState::new(parent, f, schema, trunk_at, view),
            &mut work.branch_table_moved,
        );
        note_branch_table(work, branch_table_peak, after);
        inner.next_id += 1;
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.view.is_none() {
            // Dead (see `fork_branch`): only a store-level caller can name it.
            return Err(gone(id));
        }
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
        let mut inner = self.lock();
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
        }
        self.collect(&mut inner, id);
    }

    /// The `Branch` handle has gone.
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut inner = self.lock();
        let Some(st) = inner.branches.get_mut(&id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        st.handle = false;
        // Deferred means something still reads through the branch, so its pages outlive this call:
        // kept with the branch, or, when it is spliced out, with the child that took them over.
        let deferred = st.open || !st.lineage.children.is_empty();
        let freed_pages = self.collect(&mut inner, id);
        crate::turso_assert!(
            deferred || !inner.branches.contains_key(&id),
            "a branch nothing reads through was kept"
        );
        Reaped {
            freed_pages,
            deferred,
        }
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut inner = self.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.view.is_none() {
            return Err(gone(id));
        }
        if st.writer {
            return Err(LimboError::Busy);
        }
        st.writer = true;
        Ok(())
    }

    pub(crate) fn end_write(&self, id: BranchId) {
        if let Some(st) = self.lock().branches.get_mut(&id) {
            st.writer = false;
        }
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.lock()
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let inner = self.lock();
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
        let mut inner = self.lock();
        let StoreInner {
            arena,
            branches,
            work,
            ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.view.is_none() {
            return Err(gone(id));
        }
        crate::turso_assert!(st.writer, "branch page written outside a write transaction");
        let epoch = st.lineage.epoch;
        if st.current.get(&page).is_some_and(|o| o.born == epoch) {
            // Already decided since the branch's last fork: no child has seen this slot.
            return Ok(());
        }
        let view = st.view.as_mut().expect("checked above");
        let mut w = MapWork::default();
        // The slot this branch sees now, with the path to it made this map's own. If no other map
        // names it — no child forked since it was written, no sibling or ancestor still reads it —
        // the write goes in place; else it takes a new slot and the others keep the old one.
        let reuse = view.get(page).filter(|&seen| {
            view.set(page, seen, arena, &mut w);
            arena.refs(seen) == 1
        });
        let slot = match reuse {
            Some(slot) => {
                work.writes_in_place += 1;
                slot
            }
            None => {
                let slot = arena.alloc();
                arena.page_mut(slot).copy_from_slice(pre_image);
                view.set(page, slot, arena, &mut w);
                slot
            }
        };
        work.map_nodes_copied += w.nodes_copied;
        work.map_slot_increfs += w.slot_increfs;
        counted_insert(
            &mut st.current,
            page,
            Owned { slot, born: epoch },
            &mut work.page_table_moved,
        );
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut inner = self.lock();
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
        counted_insert(&mut trunk.written, page, epoch, &mut work.page_table_moved);
    }

    /// Commit a branch's dirty pages into the slots their copy decisions allocated.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut inner = self.lock();
        let StoreInner {
            arena, branches, ..
        } = &mut *inner;
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.view.is_none() {
            return Err(gone(id));
        }
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
        let mut inner = self.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.lock();
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
        let mut work = inner.work;
        [
            work.branch_table_seg_alloc_bytes,
            work.branch_table_seg_freed_bytes,
        ] = inner.branches.segment_bytes();
        if let Some(arena) = inner.arena.as_ref() {
            [
                work.arena_free_moved,
                work.arena_bits_moved,
                work.arena_chunks_moved,
            ] = arena.moved();
            (work.arena_frames_copied, work.arena_chunks_freed) = arena.compaction();
        }
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
            arena_handles: inner.arena.as_ref().map_or(0, |a| a.handles_held()),
            work,
        }
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.values().map(|v| v.slot));
        }
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
            if st.handle || st.open {
                return freed;
            }
            if st.view.is_some() {
                freed += self.retire(inner, id);
            }
            let st = inner.branches.get(&id).expect("looked up above");
            if st.lineage.children.len() == 1 {
                self.splice(inner, id);
                return freed;
            }
            if !st.lineage.children.is_empty() {
                return freed;
            }
            let st = inner
                .branches
                .remove(&id, &mut inner.work.branch_table_moved)
                .expect("just looked it up");
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                ..
            } = &mut *inner;
            let arena = arena.as_mut().expect("a branch existed, so the arena does");
            crate::turso_assert!(
                st.lineage.retained.is_empty(),
                "a branch lineage retained a version; branch versions live by reference count"
            );
            if st.parent.is_trunk() {
                freed += trunk.lineage.child_gone(st.fork_epoch, arena, work);
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return freed;
            }
            let parent = branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            freed += parent.lineage.child_gone(st.fork_epoch, arena, work);
            id = st.parent;
        }
    }
}

impl BranchStore {
    /// `id` has no handle and no connection, so it will never be written, read or forked again:
    /// release its map. A slot only this map named is freed; one a child's map (or anyone's) still
    /// names lives on there. Returns the pages freed.
    fn retire(&self, inner: &mut StoreInner, id: BranchId) -> usize {
        let StoreInner {
            arena,
            branches,
            work,
            ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).expect("the caller looked it up");
        let mut w = MapWork::default();
        if let Some(mut view) = st.view.take() {
            view.release(arena, &mut w);
        }
        work.reclaim_map_nodes += w.nodes_released;
        work.reclaim_slot_decrefs += w.decrefs;
        work.retired += st.current.len() as u64;
        st.current = LinearMap::default();
        w.freed as usize
    }

    /// Remove `id` — dead, map released, exactly one live child — from the tree and give the child
    /// its place: the child becomes its parent's child at `id`'s fork epoch, which is what it saw
    /// of that parent. Nothing moves: every slot the child reads is named, and counted, by its own
    /// map. So a chain of dead ancestors costs one state, not one each.
    fn splice(&self, inner: &mut StoreInner, id: BranchId) {
        let st = inner
            .branches
            .remove(&id, &mut inner.work.branch_table_moved)
            .expect("the caller looked it up");
        crate::turso_assert!(
            st.view.is_none() && st.lineage.retained.is_empty(),
            "a spliced branch still names or retains a slot"
        );
        let StoreInner {
            trunk,
            branches,
            work,
            ..
        } = &mut *inner;
        let child_id = st
            .lineage
            .children
            .values()
            .next()
            .expect("the caller saw exactly one child")
            .id;
        let c = branches
            .get_mut(&child_id)
            .expect("a live branch's child is live");
        c.parent = st.parent;
        c.fork_epoch = st.fork_epoch;
        work.splices += 1;
        let parent = if st.parent.is_trunk() {
            &mut trunk.lineage
        } else {
            &mut branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives")
                .lineage
        };
        parent
            .children
            .get_mut(&st.fork_epoch)
            .expect("the parent lists the spliced branch")
            .id = child_id;
    }
}

impl StoreInner {
    /// `levels` counts the nodes consulted — the branch (its map), then the trunk if the map does
    /// not hold the page — and `examined` the retained versions compared.
    fn resolve(
        &self,
        id: BranchId,
        page: u32,
        levels: &mut u64,
        examined: &mut u64,
    ) -> Result<Option<Slot>> {
        *levels += 1;
        let st = self.branches.get(&id).ok_or_else(|| gone(id))?;
        // A branch sees its own versions and its ancestors' as of its fork, all through its map. A
        // dead branch has released it and reads nothing (it would otherwise fall through to the
        // trunk and read the wrong page).
        let view = st.view.as_ref().ok_or_else(|| gone(id))?;
        if let Some(slot) = view.get(page) {
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
        view: PageMap,
    ) -> Self {
        Self {
            parent,
            fork_epoch,
            lineage: Lineage::default(),
            current: LinearMap::default(),
            schema,
            handle: true,
            open: false,
            writer: false,
            trunk_at,
            view: Some(view),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

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

    /// A reap of 2^20 pages holds the store mutex for at most a batch of 64 frees at a time,
    /// whichever structure held the pages (r12-async-destroy PREREG §4, T-red): 2^20 versions the
    /// trunk retained for one child alone (the heap side of a reap), and 2^20 pages one branch
    /// wrote itself (the page-map side). A store that frees inside the reap's one hold frees all
    /// 2^20 in it. Both sides run before anything is asserted, so a failure reports both, and the
    /// hold bound is asserted before the totals.
    #[test]
    fn a_reap_of_2_20_pages_holds_the_store_mutex_for_at_most_a_batch_of_frees() {
        const K: u32 = 1 << 20;
        const B: u64 = 64;
        // (own, held before, freed_pages, held after, branches after, the reap's per-hold maxima)
        let mut seen = Vec::new();
        for own in [false, true] {
            let store = BranchStore::new();
            let schema = Arc::new(Schema::default());
            let child = store.fork_trunk(schema.clone(), PAGE).unwrap();
            let pre = image(0);
            if own {
                store.begin_write(child).unwrap();
                for page in 1..=K {
                    store.first_write_branch(child, page, &pre).unwrap();
                }
                store.end_write(child);
            } else {
                for page in 1..=K {
                    store.first_write_trunk(page, &pre);
                }
            }
            // A younger live sibling, so the trunk side's garbage has a neighbour to be filed
            // under.
            let sibling = store.fork_trunk(schema, PAGE).unwrap();
            let before = store.stats().arena_slots_in_use;
            store.take_hold_max();
            let reaped = store.release_handle(child);
            let h = store.take_hold_max();
            let after = store.stats().arena_slots_in_use;
            store.release_handle(sibling);
            seen.push((own, before, reaped.freed_pages, after, store.stats().live_branches, h));
        }
        for &(own, _, _, _, _, h) in &seen {
            assert!(
                h.freed <= B && h.slot_decrefs <= B && h.map_nodes <= B && h.heap_examined <= B,
                "own {own}: one hold of the reap freed {} slots, dropped {} slot references, visited \
                 {} map nodes and examined {} heap roots, against at most {B} each ({} holds); \
                 both sides: {seen:?}",
                h.freed,
                h.slot_decrefs,
                h.map_nodes,
                h.heap_examined,
                h.holds
            );
        }
        for &(own, before, freed, after, branches, _) in &seen {
            assert_eq!(
                (before, freed, after, branches),
                (K as usize, K as usize, 0, 0),
                "own {own}: slots held before, freed, held after, branches left"
            );
        }
    }

    /// One committed write transaction on `id` of `pages`, each holding `image(generation)`.
fn commit(store: &BranchStore, id: BranchId, pages: &[u32], before: u64, generation: u64) {
    store.begin_write(id).unwrap();
    for &page in pages {
        store.first_write_branch(id, page, &image(before)).unwrap();
    }
    let committed: Vec<PageRef> = pages.iter().map(|&p| page_with(p, generation)).collect();
    store.commit_pages(id, &committed).unwrap();
    store.end_write(id);
}

/// A parent that forks two children, then writes, then dies frees at once the pages it wrote
/// after its last fork — neither child can see them — and keeps the pages it wrote before,
/// which both children read, until neither child can.
#[test]
fn a_dead_parent_frees_what_it_wrote_after_its_last_fork() {
    let store = BranchStore::new();
    let parent = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
    commit(&store, parent, &[0, 1, 2], 0, 1);
    let c1 = store.fork_branch(parent).unwrap();
    let c2 = store.fork_branch(parent).unwrap();
    commit(&store, parent, &[3, 4, 5], 0, 2);
    assert_eq!(store.stats().arena_slots_in_use, 6);

    let reaped = store.release_handle(parent);
    assert!(reaped.deferred, "its children still read through it");
    assert_eq!(reaped.freed_pages, 3, "the three pages written after the last fork");
    assert_eq!(store.stats().arena_slots_in_use, 3);
    let mut buf = vec![0u8; PAGE];
    for child in [c1, c2] {
        for page in 0..3 {
            assert!(store.resolve_into(child, page, &mut buf).unwrap());
            assert_eq!(buf, image(1), "child {} page {page}", child.0);
        }
        for page in 3..6 {
            assert!(!store.resolve_into(child, page, &mut buf).unwrap(), "page {page}");
        }
    }

    store.release_handle(c1);
    assert_eq!(store.stats().arena_slots_in_use, 3, "c2 still reads the pre-fork pages");
    for page in 0..3 {
        assert!(store.resolve_into(c2, page, &mut buf).unwrap());
        assert_eq!(buf, image(1));
    }
    store.release_handle(c2);
    assert_eq!(store.stats().arena_slots_in_use, 0);
    assert_eq!(store.stats().live_branches, 0);
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
            // Branch space held is branch space reachable: every arena slot is either a trunk
            // version kept for a trunk child, or named by the page map of a live branch. A slot
            // that is neither is held for nobody (PREREG amendment 4).
            let (held, reachable) = {
                let inner = store.inner.lock();
                let mut reach: HashSet<Slot> = HashSet::new();
                for n in nodes.iter().filter(|n| n.handle) {
                    let st = inner.branches.get(&n.id).expect("a live branch has state");
                    if let Some(view) = st.view.as_ref() {
                        reach.extend(view.slots());
                    }
                }
                let trunk_versions: usize = inner
                    .trunk
                    .lineage
                    .retained
                    .values()
                    .map(|versions| versions.len())
                    .sum();
                (
                    inner.arena.as_ref().map_or(0, |a| a.in_use()),
                    reach.len() + trunk_versions,
                )
            };
            assert_eq!(
                held, reachable,
                "seed {seed:#x} step {step}: the arena holds {held} slots, but only {reachable} \
                 are reachable from a live branch or kept for a trunk child"
            );
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing —
        // including a dead branch spliced out from above its one live child.
        let (splices, in_place) = {
            let w = store.stats().work;
            (w.splices, w.writes_in_place)
        };
        assert!(
            max_depth >= 10
                && deferred > 0
                && wrote_after_fork > 0
                && splices > 0
                && in_place > 0,
            "seed {seed:#x}: max depth {max_depth}, deferred reaps {deferred}, writes by a branch \
             after its first fork {wrote_after_fork}, splices {splices}, writes in place {in_place}"
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    }
}
