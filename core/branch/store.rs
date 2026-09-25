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
//! live siblings `lo` and `hi`: `born > lo` and `died <= hi`. The versions are indexed both by
//! `born` and by `died` — ZFS's deadlists, which key a dead block by the interval that killed it
//! and split it by birth — so that each side of that query is a range, not a scan:
//!
//! * with no older live sibling (the oldest child, which is whom uniform-TTL lease expiry reaps),
//!   the garbage is exactly the versions with `died` in `(f, hi]`;
//! * with no younger one (the newest child), exactly those with `born` in `(lo, f]`;
//! * with both, each range also holds survivors, and the two are walked in lockstep until the
//!   shorter one ends (see [`BranchStore::reclaim_both`]).
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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use super::arena::{Arena, Slot, SlotPtr};
use super::page_map::PageMap;
use super::{BranchId, BranchStats, BranchWork, HoldMax, Reaped};
use crate::schema::Schema;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard};
use crate::{LimboError, Result};

/// Time every store-mutex hold (observation only; off unless a harness turns it on).
static HOLD_TIMING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn set_hold_timing(on: bool) {
    HOLD_TIMING.store(on, std::sync::atomic::Ordering::Relaxed);
}

/// What one store-mutex hold did, folded into [`BranchWork`] and [`HoldMax`] when it ends.
/// Observation only.
#[derive(Default)]
struct HoldAcc {
    pages: u64,
    copy_bytes: u64,
    realloc_moved: u64,
}

/// A hold of the store mutex that accounts for itself when it is released.
struct Hold<'a> {
    guard: MutexGuard<'a, StoreInner>,
    start: Option<std::time::Instant>,
}

impl Deref for Hold<'_> {
    type Target = StoreInner;
    fn deref(&self) -> &StoreInner {
        &self.guard
    }
}

impl DerefMut for Hold<'_> {
    fn deref_mut(&mut self) -> &mut StoreInner {
        &mut self.guard
    }
}

impl Drop for Hold<'_> {
    fn drop(&mut self) {
        let inner = &mut *self.guard;
        let acc = std::mem::take(&mut inner.hold);
        inner.work.lock_holds += 1;
        inner.work.locked_copy_bytes += acc.copy_bytes;
        let max = &mut inner.hold_max;
        max.pages = max.pages.max(acc.pages);
        max.copy_bytes = max.copy_bytes.max(acc.copy_bytes);
        max.realloc_moved = max.realloc_moved.max(acc.realloc_moved);
        if let Some(start) = self.start {
            max.ns = max.ns.max(start.elapsed().as_nanos() as u64);
        }
    }
}

/// The most pages one hold of the store mutex maps, allocates or frees on behalf of one
/// transaction's commit or rollback, or of one reap: work proportional to a transaction's size is
/// split into holds of this many pages, so no other branch ever waits for more than this.
const HOLD_BATCH: usize = 64;

/// `map.insert`, charging the entries a growth moved to the current hold.
fn insert_counted<K: Hash + Eq, V>(
    map: &mut HashMap<K, V>,
    acc: &mut HoldAcc,
    k: K,
    v: V,
) -> Option<V> {
    let (len, cap) = (map.len(), map.capacity());
    let old = map.insert(k, v);
    if map.capacity() != cap {
        acc.realloc_moved += len as u64;
    }
    old
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
    /// Test-only: runs while a first fork builds its view outside the mutex, so a test can make
    /// another thread act inside that window deterministically.
    #[cfg(test)]
    fork_build_pause: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

struct StoreInner {
    arena: Option<Arena>,
    next_id: u64,
    trunk: TrunkState,
    branches: HashMap<BranchId, BranchState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
    /// Observation only: the hold in progress, and the per-hold maxima since the last
    /// [`BranchStore::take_hold_max`].
    hold: HoldAcc,
    hold_max: HoldMax,
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current, per page and
    /// ordered by `born` (see "Per-page version order" above). A B-tree, not a hash table: it grows
    /// with a transaction's size under the store mutex, and a hash table's growth moves every entry
    /// inside one hold.
    retained: BTreeMap<u32, BTreeMap<u64, Retained>>,
    /// The same versions as `(born, page, died)`, for the reclamation range query by birth.
    by_born: BTreeSet<(u64, u32, u64)>,
    /// The same versions as `(died, page, born)`, for the reclamation range query by death.
    by_died: BTreeSet<(u64, u32, u64)>,
}

/// No page has this number (SQLite's largest is `u32::MAX - 1`; [`Lineage::retain`] refuses it), so
/// `(e, NO_PAGE, u64::MAX)` sorts after every index entry whose first field is `e`.
const NO_PAGE: u32 = u32::MAX;

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
    /// retention that was not strictly needed, never skip one that was. A B-tree for the reason
    /// `Lineage::retained` is one.
    written: BTreeMap<u32, u64>,
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has written. A B-tree for the reason
    /// `Lineage::retained` is one; behind an `Arc` so that a first fork can build its view from a
    /// snapshot outside the store mutex (see `fork_branch`).
    /// `std::sync::Arc` for `make_mut`, as in `page_map`: only ever touched under the store mutex
    /// or by the one first fork that `forking` protects.
    current: std::sync::Arc<BTreeMap<u32, Owned>>,
    /// The branch's committed schema. Shared with the parent at fork (an `Arc` clone), replaced by
    /// a committed DDL on the branch.
    schema: Arc<Schema>,
    /// The `Branch` handle is alive.
    handle: bool,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
    /// A first fork is building `view` outside the store mutex; a writer waits for it.
    forking: bool,
    /// The fork epoch at which this branch's ancestry leaves the trunk: its own fork epoch if its
    /// parent is the trunk, else its parent's `trunk_at`.
    trunk_at: u64,
    /// Every arena page this branch sees through its ancestors: its parent's `view` at the fork.
    inherited: PageMap,
    /// `inherited` plus this branch's current pages, for its children to inherit. Built at the
    /// branch's first fork and kept current by its writes from then on; `None` until it forks.
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

    fn retain(&mut self, page: u32, v: Retained) {
        let versions = self.retained.entry(page).or_default();
        crate::turso_assert!(
            versions
                .last_key_value()
                .is_none_or(|(_, last)| last.died <= v.born),
            "a retained version overlaps an older one of the same page; the born-ordered lookup \
             would return the wrong one"
        );
        crate::turso_assert!(page != NO_PAGE, "page number u32::MAX is the index sentinel");
        versions.insert(v.born, v);
        self.by_born.insert((v.born, page, v.died));
        self.by_died.insert((v.died, page, v.born));
    }

    /// The retained version of `page` visible to a child forked at `f`: the born-predecessor of
    /// `f`, if it was still current at `f`. `examined` counts the versions compared against `f` —
    /// at most one; the O(log V) descent that finds it is not counted.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        let (_, v) = self.retained.get(&page)?.range(..=f).next_back()?;
        *examined += 1;
        (f < v.died).then_some(v.slot)
    }

    /// Detach the child forked at `f` and return the index range(s) holding every retained version
    /// only it could see (see [`BranchStore::reclaim_range`]).
    fn child_gone(&mut self, f: u64) -> GarbageRange {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        match (lo, hi) {
            (None, _) => GarbageRange::Died { after: f, to: hi },
            (Some(lo), None) => GarbageRange::Born { after: lo, to: f },
            (Some(lo), Some(hi)) => GarbageRange::Both { lo, f, hi },
        }
    }

    /// Remove one retained version from the per-page map and both indexes, and free its slot.
    fn release_version(
        &mut self,
        born: u64,
        page: u32,
        died: u64,
        arena: &mut Arena,
        work: &mut BranchWork,
    ) {
        let versions = self.retained.get_mut(&page).expect("indexed version is listed");
        let v = versions.remove(&born).expect("indexed version is listed");
        work.gc_examined += 1;
        if versions.is_empty() {
            self.retained.remove(&page);
        }
        let indexed =
            self.by_born.remove(&(born, page, died)) && self.by_died.remove(&(died, page, born));
        crate::turso_assert!(indexed, "a released version was missing from an index");
        arena.release(v.slot);
    }

    /// Up to `n` entries of `range` strictly after the index key `resume`, as `(born, page, died)`
    /// in the range's index order.
    fn range_batch(
        &self,
        range: GarbageRange,
        resume: Option<(u64, u32, u64)>,
        n: usize,
    ) -> Vec<(u64, u32, u64)> {
        let after = |e: u64| (e, NO_PAGE, u64::MAX);
        match range {
            GarbageRange::Died { after: f, to } => {
                let from = Bound::Excluded(resume.unwrap_or(after(f)));
                let to = to.map_or(Bound::Unbounded, |hi| Bound::Included(after(hi)));
                self.by_died
                    .range((from, to))
                    .take(n)
                    .map(|&(died, page, born)| (born, page, died))
                    .collect()
            }
            GarbageRange::Born { after: lo, to: f } => {
                let from = Bound::Excluded(resume.unwrap_or(after(lo)));
                self.by_born
                    .range((from, Bound::Included(after(f))))
                    .take(n)
                    .copied()
                    .collect()
            }
            GarbageRange::Both { .. } => unreachable!("a two-sided query is walked in lockstep"),
        }
    }

    /// The two-sided query's next entry on `side` (0: `born` in `(lo, f]`; 1: `died` in
    /// `(f, hi]`) strictly after the index key `resume`, as `(born, page, died)`.
    fn lockstep_next(
        &self,
        side: usize,
        (lo, f, hi): (u64, u64, u64),
        resume: Option<(u64, u32, u64)>,
    ) -> Option<(u64, u32, u64)> {
        let after = |e: u64| (e, NO_PAGE, u64::MAX);
        if side == 0 {
            let from = Bound::Excluded(resume.unwrap_or(after(lo)));
            self.by_born
                .range((from, Bound::Included(after(f))))
                .next()
                .copied()
        } else {
            let from = Bound::Excluded(resume.unwrap_or(after(f)));
            self.by_died
                .range((from, Bound::Included(after(hi))))
                .next()
                .map(|&(died, page, born)| (born, page, died))
        }
    }

    /// `(born, page, died)` is still retained.
    fn holds(&self, born: u64, page: u32, died: u64) -> bool {
        self.by_born.contains(&(born, page, died))
    }
}

/// Clears a branch's `forking` flag if its first fork's view build unwinds, so that a writer or a
/// second fork waiting on the flag is not left waiting for ever.
struct ForkingGuard<'a> {
    store: &'a BranchStore,
    id: BranchId,
    armed: bool,
}

impl Drop for ForkingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            if let Some(st) = self.store.inner.lock().branches.get_mut(&self.id) {
                st.forking = false;
            }
        }
    }
}

/// The versions a reaped child alone could see when it had no live neighbour on one side: one
/// range of one index, or two walked in lockstep (see [`BranchStore::reclaim_both`]).
#[derive(Clone, Copy)]
enum GarbageRange {
    /// `died` in `(after, to]`, `to` absent meaning unbounded: the child had no older live sibling.
    Died { after: u64, to: Option<u64> },
    /// `born` in `(after, to]`: it had no younger one.
    Born { after: u64, to: u64 },
    /// It had both neighbours, `lo` and `hi`: `born` in `(lo, f]` and `died` in `(f, hi]`, each also
    /// holding survivors, walked in lockstep.
    Both { lo: u64, f: u64, hi: u64 },
}

/// What a reap unlinked under one hold and left to be freed in holds of at most [`HOLD_BATCH`]
/// pages, so that reaping a large branch never stalls every other branch for its whole size.
#[derive(Default)]
struct Reclaim {
    /// Unlinked branch states: their slots are released, then they are dropped outside the mutex.
    states: Vec<BranchState>,
    /// One-sided garbage ranges of a node's lineage (the trunk's, or a live branch's).
    ranges: Vec<(BranchId, GarbageRange)>,
}

/// A branch write transaction's own arena slots: shadow paging (Lorie, 1977). Every page the
/// transaction dirties gets a fresh slot at its first write; the transaction fills that slot —
/// when the page is spilled out of its page cache, and at commit — without the store mutex, and
/// [`BranchStore::publish`] maps the slots at commit in holds of at most [`HOLD_BATCH`] pages. The
/// committed slots are never written while the transaction runs, so a rollback only returns the
/// transaction's own slots, and a dirty page may leave the page cache before commit (a STEAL
/// buffer policy) without making that rollback unrecoverable.
///
/// Owned by the branch pager's [`super::BranchBinding`]; only the branch's one connection uses it.
#[derive(Default)]
pub(crate) struct ShadowTxn {
    pages: HashMap<u32, Shadow>,
}

#[derive(Clone, Copy)]
struct Shadow {
    slot: Slot,
    ptr: SlotPtr,
    /// The slot holds the page's latest spilled or committed image.
    filled: bool,
}

impl ShadowTxn {
    /// Copy `image` into `page`'s slot.
    pub(crate) fn fill(&mut self, page: u32, image: &[u8]) -> Result<()> {
        let s = self.pages.get_mut(&page).ok_or_else(|| {
            LimboError::InternalError(format!(
                "branch page {page} was dirtied with no shadow slot behind it"
            ))
        })?;
        // SAFETY: this transaction allocated the slot and has not published it (`SlotPtr`).
        unsafe { s.ptr.write(image) };
        s.filled = true;
        Ok(())
    }

    /// Fill `out` from `page`'s slot if the transaction spilled the page. A page it dirtied but
    /// never spilled cannot have left the page cache; finding one is an error, never a silent read
    /// of the committed version.
    pub(crate) fn read(&self, page: u32, out: &mut [u8]) -> Result<bool> {
        match self.pages.get(&page) {
            None => Ok(false),
            Some(s) if s.filled => {
                // SAFETY: as in `fill`.
                unsafe { s.ptr.read(out) };
                Ok(true)
            }
            Some(_) => Err(LimboError::InternalError(format!(
                "dirty branch page {page} left the page cache unspilled"
            ))),
        }
    }

    pub(crate) fn is_filled(&self, page: u32) -> bool {
        self.pages.get(&page).is_some_and(|s| s.filled)
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
                    written: BTreeMap::new(),
                },
                branches: HashMap::new(),
                work: BranchWork::default(),
                hold: HoldAcc::default(),
                hold_max: HoldMax::default(),
            }),
            trunk_children: AtomicUsize::new(0),
            #[cfg(test)]
            fork_build_pause: Mutex::new(None),
        }
    }

    /// Take the store mutex for the mechanism. Every hold it returns is counted in
    /// [`BranchWork::lock_holds`] and folded into [`HoldMax`]; the observation calls (`stats`,
    /// `take_hold_max`, the membership diagnostics) lock directly so they do not count themselves.
    fn lock(&self) -> Hold<'_> {
        let guard = self.inner.lock();
        // Started once the lock is ours, so a contended acquire's wait is not counted as hold.
        let start = HOLD_TIMING
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(std::time::Instant::now);
        Hold { guard, start }
    }

    /// The per-hold maxima since the previous call, which this call resets.
    pub(crate) fn take_hold_max(&self) -> HoldMax {
        std::mem::take(&mut self.inner.lock().hold_max)
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
        inner.trunk.lineage.children.insert(f, id);
        let StoreInner { branches, hold, .. } = &mut *inner;
        insert_counted(
            branches,
            hold,
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
        );
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        loop {
            let mut inner = self.lock();
            let id = BranchId(inner.next_id);
            let StoreInner { branches, hold, .. } = &mut *inner;
            let st = branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
            if st.writer {
                return Err(LimboError::Busy);
            }
            if st.forking {
                // Another first fork is building this branch's view; it waits on nothing.
                drop(inner);
                std::thread::yield_now();
                continue;
            }
            let Some(view) = st.view.clone() else {
                // The branch's first fork: build its view — the inherited map plus one entry per
                // page it owns — OUTSIDE the store mutex, from two snapshots that cost a reference
                // count each. `forking` keeps a writer and any other first fork off this branch
                // until the view is installed, so `current` cannot change under the build. The
                // branch cannot be reaped meanwhile: whoever forks it holds its handle or its open
                // connection.
                st.forking = true;
                let (mut view, current) = (st.inherited.clone(), st.current.clone());
                drop(inner);
                let mut unwinding = ForkingGuard {
                    store: self,
                    id: parent,
                    armed: true,
                };
                #[cfg(test)]
                if let Some(pause) = self.fork_build_pause.lock().as_ref() {
                    pause();
                }
                for (&page, owned) in current.iter() {
                    view.insert(page, owned.slot);
                }
                let built = current.len() as u64;
                // Dropped before `forking` clears, so the next writer's `Arc::make_mut` on
                // `current` finds it unshared and copies nothing.
                drop(current);
                let mut inner = self.lock();
                inner.work.view_build_pages += built;
                let st = inner
                    .branches
                    .get_mut(&parent)
                    .expect("a branch being forked is kept by the handle or connection forking it");
                st.view = Some(view);
                st.forking = false;
                unwinding.armed = false;
                continue;
            };
            let f = st.lineage.epoch;
            st.lineage.epoch += 1;
            st.lineage.children.insert(f, id);
            let schema = st.schema.clone();
            let trunk_at = st.trunk_at;
            insert_counted(
                branches,
                hold,
                id,
                BranchState::new(parent, f, schema, trunk_at, view),
            );
            inner.next_id += 1;
            return Ok(id);
        }
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.lock();
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
        let mut reclaim = Reclaim::default();
        {
            let mut inner = self.lock();
            if let Some(st) = inner.branches.get_mut(&id) {
                st.open = false;
                st.writer = false;
            }
            self.collect(&mut inner, id, &mut reclaim);
        }
        self.reclaim(reclaim);
    }

    /// The `Branch` handle has gone.
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let Some((reclaim, deferred)) = self.unlink_handle(id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        Reaped {
            freed_pages: self.reclaim(reclaim),
            deferred,
        }
    }

    /// The first half of `release_handle`, in one hold: drop the handle and unlink whatever that
    /// made unreachable. Returns what is left to free, and whether the branch itself was kept.
    fn unlink_handle(&self, id: BranchId) -> Option<(Reclaim, bool)> {
        let mut reclaim = Reclaim::default();
        let mut inner = self.lock();
        let st = inner.branches.get_mut(&id)?;
        st.handle = false;
        self.collect(&mut inner, id, &mut reclaim);
        let deferred = inner.branches.contains_key(&id);
        Some((reclaim, deferred))
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        loop {
            let mut inner = self.lock();
            let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
            if st.writer {
                return Err(LimboError::Busy);
            }
            if st.forking {
                // A first fork is building this branch's view from `current` outside the mutex. It
                // waits on nothing, so wait for it rather than fail the writer.
                drop(inner);
                std::thread::yield_now();
                continue;
            }
            st.writer = true;
            return Ok(());
        }
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

    /// A branch's first write to `page` in a transaction: give the page a fresh slot of its own,
    /// which the transaction fills outside the store mutex (see [`ShadowTxn`]). The copy decision —
    /// retain the version this write supersedes for a child that can see it, or free it — is taken
    /// when the transaction publishes, in the same hold that maps the page, so it is serialised
    /// with the child reaps that can change it.
    pub(crate) fn shadow_slot(&self, id: BranchId, page: u32, txn: &mut ShadowTxn) -> Result<()> {
        if txn.pages.contains_key(&page) {
            return Ok(());
        }
        let (slot, ptr) = {
            let mut inner = self.lock();
            let StoreInner {
                arena,
                branches,
                hold,
                ..
            } = &mut *inner;
            let st = branches.get(&id).ok_or_else(|| gone(id))?;
            crate::turso_assert!(st.writer, "branch page written outside a write transaction");
            let arena = arena.as_mut().expect("a branch exists, so the arena does");
            let slot = arena.alloc();
            hold.pages += 1;
            (slot, arena.slot_ptr(slot))
        };
        txn.pages.insert(
            page,
            Shadow {
                slot,
                ptr,
                filled: false,
            },
        );
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut inner = self.lock();
        let StoreInner {
            arena, trunk, hold, ..
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
            hold.pages += 1;
            hold.copy_bytes += pre_image.len() as u64;
            trunk.lineage.retain(
                page,
                Retained {
                    born,
                    died: epoch,
                    slot,
                },
            );
        }
        trunk.written.insert(page, epoch);
    }

    /// Commit a branch transaction: map each page of `dirty` to the slot the transaction filled for
    /// it, in holds of at most [`HOLD_BATCH`] pages, and return the transaction's other slots (those
    /// of pages a statement rollback dropped). Nothing observes the branch between two holds: its
    /// one connection is the committer, `fork_branch` refuses while the writer flag is set, and a
    /// child reads only what its fork froze. Each page's copy decision is taken in the hold that
    /// maps it: the version it supersedes is retained if a live child forked while it was current,
    /// else freed.
    pub(crate) fn publish(&self, id: BranchId, txn: &mut ShadowTxn, dirty: &[u32]) -> Result<()> {
        for &page in dirty {
            if !txn.is_filled(page) {
                return Err(LimboError::InternalError(format!(
                    "branch {} committed page {page} whose image never reached its slot",
                    id.0
                )));
            }
        }
        let mapped: Vec<(u32, Slot)> = dirty
            .iter()
            .map(|page| (*page, txn.pages.remove(page).expect("checked above").slot))
            .collect();
        self.discard(txn);
        for batch in mapped.chunks(HOLD_BATCH) {
            let mut inner = self.lock();
            let StoreInner {
                arena,
                branches,
                hold,
                ..
            } = &mut *inner;
            let arena = arena.as_mut().expect("a branch exists, so the arena does");
            let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
            crate::turso_assert!(st.writer, "branch commit outside a write transaction");
            let epoch = st.lineage.epoch;
            let BranchState {
                current,
                lineage,
                view,
                ..
            } = st;
            // Unshared: no fork snapshot outlives `forking`, and no fork runs while `writer` is set.
            let current = std::sync::Arc::make_mut(current);
            for &(page, slot) in batch {
                match current.insert(page, Owned { slot, born: epoch }) {
                    Some(old) if lineage.has_child_in(old.born, epoch) => lineage.retain(
                        page,
                        Retained {
                            born: old.born,
                            died: epoch,
                            slot: old.slot,
                        },
                    ),
                    Some(old) => arena.release(old.slot),
                    None => {}
                }
                if let Some(view) = view.as_mut() {
                    view.insert(page, slot);
                }
            }
            hold.pages += batch.len() as u64;
        }
        Ok(())
    }

    /// Return every slot `txn` still holds (a rollback, an abandoned transaction, or pages its
    /// statements dropped), in holds of at most [`HOLD_BATCH`] pages.
    pub(crate) fn discard(&self, txn: &mut ShadowTxn) {
        let slots: Vec<Slot> = txn.pages.drain().map(|(_, s)| s.slot).collect();
        self.release_slots(&slots);
    }

    fn release_slots(&self, slots: &[Slot]) {
        for batch in slots.chunks(HOLD_BATCH) {
            let mut inner = self.lock();
            let StoreInner { arena, hold, .. } = &mut *inner;
            let arena = arena.as_mut().expect("slots exist, so the arena does");
            for &slot in batch {
                arena.release(slot);
            }
            hold.pages += batch.len() as u64;
        }
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
        inner.hold.pages += 1;
        inner.hold.copy_bytes += out.len() as u64;
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

    /// Unlink `id` if nothing can reach it any more, then its parent if that removed the parent's
    /// last reason to exist. Frees nothing: what an unlinked branch owned, and the garbage its
    /// parent kept only for it, go to `reclaim` for [`BranchStore::reclaim`].
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId, reclaim: &mut Reclaim) {
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return;
            };
            if st.handle || st.open || !st.lineage.children.is_empty() {
                return;
            }
            let st = inner.branches.remove(&id).expect("just looked it up");
            let StoreInner {
                trunk, branches, ..
            } = &mut *inner;
            let (parent, fork_epoch) = (st.parent, st.fork_epoch);
            reclaim.states.push(st);
            let lineage = if parent.is_trunk() {
                &mut trunk.lineage
            } else {
                &mut branches
                    .get_mut(&parent)
                    .expect("a live branch's parent is kept while the branch lives")
                    .lineage
            };
            reclaim.ranges.push((parent, lineage.child_gone(fork_epoch)));
            if parent.is_trunk() {
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return;
            }
            id = parent;
        }
    }

    /// Free what `collect` unlinked, in holds of at most [`HOLD_BATCH`] pages. Returns the pages
    /// freed. The unlinked states are unreachable, so their slots can be returned at leisure; a
    /// garbage range is re-read from the live index in each hold (see `reclaim_range`).
    fn reclaim(&self, reclaim: Reclaim) -> usize {
        let mut freed = 0;
        for (node, range) in reclaim.ranges {
            freed += self.reclaim_range(node, range);
        }
        for st in reclaim.states {
            let mut slots: Vec<Slot> = st.current.values().map(|o| o.slot).collect();
            for versions in st.lineage.retained.values() {
                slots.extend(versions.values().map(|v| v.slot));
            }
            self.release_slots(&slots);
            freed += slots.len();
            // Its maps are dropped here, outside the mutex.
            drop(st);
        }
        freed
    }

    /// Release the versions in `range` of `node`'s lineage that no live child can see, at most
    /// [`HOLD_BATCH`] index entries per hold. Each hold re-reads the live index after the last key
    /// it visited and frees a version only if no live child forked inside its `[born, died)`,
    /// which no later fork can change (a fork's epoch is past every `died`). So an interleaved reap
    /// of a sibling that frees some of the same versions, or a new fork, cannot make this free a
    /// version twice or free one a child can see. With no interleaving every entry of the range is
    /// garbage (see [`BranchStore::reclaim_both`]) and the entries visited equal the versions freed. If
    /// `node` is gone its whole lineage was freed with it and there is nothing left to do.
    fn reclaim_range(&self, node: BranchId, range: GarbageRange) -> usize {
        if let GarbageRange::Both { lo, f, hi } = range {
            return self.reclaim_both(node, (lo, f, hi));
        }
        let mut freed = 0;
        let mut resume = None;
        loop {
            let mut inner = self.lock();
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                hold,
                ..
            } = &mut *inner;
            let lineage = if node.is_trunk() {
                &mut trunk.lineage
            } else {
                match branches.get_mut(&node) {
                    Some(st) => &mut st.lineage,
                    None => return freed,
                }
            };
            let Some(arena) = arena.as_mut() else {
                return freed;
            };
            let batch = lineage.range_batch(range, resume, HOLD_BATCH);
            for &(born, page, died) in &batch {
                work.gc_range_entries += 1;
                if !lineage.has_child_in(born, died) {
                    lineage.release_version(born, page, died, arena, work);
                    freed += 1;
                }
            }
            hold.pages += batch.len() as u64;
            let Some(&(born, page, died)) = batch.last() else {
                return freed;
            };
            if batch.len() < HOLD_BATCH {
                return freed;
            }
            resume = Some(match range {
                GarbageRange::Died { .. } => (died, page, born),
                GarbageRange::Born { .. } => (born, page, died),
                GarbageRange::Both { .. } => unreachable!("walked by reclaim_both"),
            });
        }
    }

    /// The two-sided query, for a child reaped between two live siblings `lo` and `hi`.
    ///
    /// Every retained version holds at least one live child's fork epoch: it is retained only if
    /// one forked inside it, and it is handed back the moment the last one goes. So a version with
    /// `born > lo` and `died <= hi` held `f` and nothing else, and one holding `f` that reaches
    /// back to `lo` or on to `hi` is still needed. The garbage `{born > lo, died <= hi}` lies in
    /// both `born` in `(lo, f]` and `died` in `(f, hi]`, and each range also yields survivors, so
    /// the two are walked in lockstep and the first to end holds all the garbage: the walk costs
    /// twice the SMALLER range, never the larger (`gc_range_entries` counts every entry either
    /// range yields). The walk takes at most [`HOLD_BATCH`] steps per hold, resuming each side
    /// after the last key it visited; the finished side's entries are then freed, at most
    /// [`HOLD_BATCH`] per hold, each only if it is still retained and no live child lies in its
    /// `[born, died)` — with no interleaving that is exactly `born > lo && died <= hi`.
    fn reclaim_both(&self, node: BranchId, bounds: (u64, u64, u64)) -> usize {
        let mut seen: [Vec<(u64, u32, u64)>; 2] = Default::default();
        let mut resume: [Option<(u64, u32, u64)>; 2] = [None, None];
        let finished = 'walk: loop {
            let mut inner = self.lock();
            let StoreInner {
                trunk,
                branches,
                work,
                hold,
                ..
            } = &mut *inner;
            let lineage = if node.is_trunk() {
                &trunk.lineage
            } else {
                match branches.get(&node) {
                    Some(st) => &st.lineage,
                    None => return 0,
                }
            };
            for _ in 0..HOLD_BATCH {
                for side in 0..2 {
                    match lineage.lockstep_next(side, bounds, resume[side]) {
                        Some(v @ (born, page, died)) => {
                            work.gc_range_entries += 1;
                            hold.pages += 1;
                            seen[side].push(v);
                            resume[side] = Some(if side == 0 {
                                (born, page, died)
                            } else {
                                (died, page, born)
                            });
                        }
                        None => break 'walk side,
                    }
                }
            }
        };
        let mut freed = 0;
        for batch in seen[finished].chunks(HOLD_BATCH) {
            let mut inner = self.lock();
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                hold,
                ..
            } = &mut *inner;
            let lineage = if node.is_trunk() {
                &mut trunk.lineage
            } else {
                match branches.get_mut(&node) {
                    Some(st) => &mut st.lineage,
                    None => return freed,
                }
            };
            let arena = arena.as_mut().expect("retained versions exist, so the arena does");
            for &(born, page, died) in batch {
                if lineage.holds(born, page, died) && !lineage.has_child_in(born, died) {
                    lineage.release_version(born, page, died, arena, work);
                    freed += 1;
                }
            }
            hold.pages += batch.len() as u64;
        }
        freed
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
            current: std::sync::Arc::default(),
            schema,
            handle: true,
            open: false,
            writer: false,
            forking: false,
            trunk_at,
            inherited,
            view: None,
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
    /// garbage query's cost against its contract: a reap with no older live sibling, or no younger
    /// one, visits exactly the versions it frees, and one with both visits 2·|B| index entries if
    /// |B| <= |D| and 2·|D| + 1 otherwise, where B and D are the versions born in `(lo, f]` and
    /// dying in `(f, hi]`.
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
                    let b = history
                        .iter()
                        .filter(|&&(_, born, _)| lo.is_none_or(|lo| born > lo) && born <= f)
                        .count() as u64;
                    let d = history
                        .iter()
                        .filter(|&&(_, _, died)| f < died && hi.is_none_or(|hi| died <= hi))
                        .count() as u64;
                    let before = store.stats();
                    let reaped = store.release_handle(id);
                    let after = store.stats();
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}");
                    assert_eq!(
                        before.arena_slots_in_use - after.arena_slots_in_use,
                        reaped.freed_pages,
                        "seed {seed:#x} step {step}: the reap's report disagrees with the arena"
                    );
                    let visited = after.work.gc_range_entries - before.work.gc_range_entries;
                    let contract = match (lo, hi) {
                        (None, _) | (_, None) => reaped.freed_pages as u64,
                        _ if b <= d => 2 * b,
                        _ => 2 * d + 1,
                    };
                    assert_eq!(
                        visited, contract,
                        "seed {seed:#x} step {step}: reaping the child forked at {f} (lo {lo:?}, \
                         hi {hi:?}, |B| {b}, |D| {d}) visited {visited} index entries"
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
                    // One transaction through the pager's entry points: a shadow slot at each
                    // page's first write, filled with the committed image, then published.
                    store.begin_write(id).unwrap();
                    let mut txn = ShadowTxn::default();
                    let mut committed: Vec<u32> = Vec::new();
                    for _ in 0..=rng.below(2) {
                        let page = rng.below(u64::from(PAGES)) as u32;
                        if committed.contains(&page) {
                            continue;
                        }
                        store.shadow_slot(id, page, &mut txn).unwrap();
                        generation += 1;
                        txn.fill(page, &image(generation)).unwrap();
                        committed.push(page);
                        nodes[v].sees.insert(page, generation);
                    }
                    committed.sort_unstable();
                    store.publish(id, &mut txn, &committed).unwrap();
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

    /// One committed transaction writing generation `gen` into every page below `pages`, through
    /// the pager's entry points.
    fn commit_all(store: &BranchStore, id: BranchId, pages: u32, gen: u64) {
        let mut txn = ShadowTxn::default();
        for page in 0..pages {
            store.shadow_slot(id, page, &mut txn).unwrap();
            txn.fill(page, &image(gen)).unwrap();
        }
        store
            .publish(id, &mut txn, &(0..pages).collect::<Vec<_>>())
            .unwrap();
    }

    fn read_gen(store: &BranchStore, id: BranchId, page: u32) -> Option<u64> {
        let mut buf = vec![0u8; PAGE];
        store
            .resolve_into(id, page, &mut buf)
            .unwrap()
            .then(|| u64::from_le_bytes(buf[..8].try_into().unwrap()))
    }

    /// A branch's first fork builds its view outside the store mutex while another thread commits
    /// to the same branch. The writer waits for the build, or the fork waits (Busy) for the
    /// commit, so the child sees the branch wholly before or wholly after that commit.
    #[test]
    fn a_first_fork_racing_a_commit_sees_one_side_of_it() {
        const N: u32 = 2000;
        let store = Arc::new(BranchStore::new());
        let (mut before, mut after) = (0, 0);
        for round in 0..100u64 {
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
            store.begin_write(id).unwrap();
            commit_all(&store, id, N, 1);
            store.end_write(id);
            let s = store.clone();
            let writer = std::thread::spawn(move || {
                s.begin_write(id).unwrap();
                commit_all(&s, id, N, 2);
                s.end_write(id);
            });
            let child = loop {
                match store.fork_branch(id) {
                    Ok(child) => break child,
                    Err(LimboError::Busy) => std::thread::yield_now(),
                    Err(e) => panic!("round {round}: {e}"),
                }
            };
            writer.join().unwrap();
            let seen: HashSet<Option<u64>> = (0..N).map(|p| read_gen(&store, child, p)).collect();
            assert_eq!(seen.len(), 1, "round {round}: the child saw a mix {seen:?}");
            match seen.into_iter().next().unwrap() {
                Some(1) => before += 1,
                Some(2) => after += 1,
                other => panic!("round {round}: the child saw {other:?}"),
            }
            for p in 0..N {
                assert_eq!(read_gen(&store, id, p), Some(2), "round {round}: parent page {p}");
            }
            store.release_handle(child);
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
        assert!(before + after == 100, "before {before}, after {after}");
    }

    /// Reaps of trunk children on four threads, each child with more garbage than one hold frees,
    /// so the one-sided ranges are released across many holds while the other threads reap
    /// neighbours of the same versions. Every surviving child keeps reading the trunk as of its
    /// fork (a freed slot would fail the arena's free-slot assertion), no version is freed twice
    /// (the arena's double-release assertion), and none is leaked.
    #[test]
    fn concurrent_reaps_free_each_retained_version_once() {
        const W: u32 = 200;
        const CHILDREN: usize = 120;
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03] {
            let store = Arc::new(BranchStore::new());
            let mut gen: HashMap<u32, u64> = (0..W).map(|p| (p, 0)).collect();
            let mut next = 0u64;
            let mut children = Vec::new();
            for _ in 0..CHILDREN {
                let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                children.push((id, gen.clone()));
                for page in 0..W {
                    store.first_write_trunk(page, &image(gen[&page]));
                    next += 1;
                    gen.insert(page, next);
                }
            }
            let mut rng = Rng(seed);
            let mut lists: Vec<Vec<(BranchId, HashMap<u32, u64>)>> = vec![Vec::new(); 4];
            for child in children {
                lists[rng.below(4) as usize].push(child);
            }
            let trunk_now = Arc::new(gen);
            let threads: Vec<_> = lists
                .into_iter()
                .map(|mut mine| {
                    let s = store.clone();
                    let trunk_now = trunk_now.clone();
                    std::thread::spawn(move || {
                        while !mine.is_empty() {
                            let (id, _) = mine.remove(0);
                            s.release_handle(id);
                            for (id, sees) in &mine {
                                for page in 0..W {
                                    let got = read_gen(&s, *id, page).unwrap_or(trunk_now[&page]);
                                    assert_eq!(got, sees[&page], "child {} page {page}", id.0);
                                }
                            }
                        }
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
            assert_eq!(store.stats().live_branches, 0);
            assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: versions leaked");
        }
    }

    /// Three commits of D pages on a branch with a fork after each of the first two, then a reap of
    /// the middle child: the two-sided walk finds the second commit's D versions as garbage, and
    /// no hold of the store mutex visits more than 2·HOLD_BATCH index entries on the way.
    #[test]
    fn a_two_sided_reap_of_a_large_garbage_set_holds_the_mutex_briefly() {
        const D: u32 = 5000;
        let store = BranchStore::new();
        let p = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        let write = |gen: u64| {
            store.begin_write(p).unwrap();
            commit_all(&store, p, D, gen);
            store.end_write(p);
        };
        write(1);
        let c1 = store.fork_branch(p).unwrap();
        write(2);
        let c2 = store.fork_branch(p).unwrap();
        write(3);
        let c3 = store.fork_branch(p).unwrap();
        let _ = store.take_hold_max();
        let before = store.stats();
        let reaped = store.release_handle(c2);
        let after = store.stats();
        let max = store.take_hold_max();
        assert_eq!(reaped.freed_pages, D as usize, "the middle child's garbage");
        assert_eq!(
            after.work.gc_range_entries - before.work.gc_range_entries,
            2 * u64::from(D),
            "the lockstep walk visits twice the smaller range"
        );
        assert!(max.pages <= 2 * HOLD_BATCH as u64, "one hold visited {} entries", max.pages);
        for page in 0..D {
            assert_eq!(read_gen(&store, c1, page), Some(1), "c1 page {page}");
            assert_eq!(read_gen(&store, c3, page), Some(3), "c3 page {page}");
        }
        for id in [c1, c3, p] {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
    }

    /// A reap's deferred garbage range can gain a version a NEW child needs before the reap frees
    /// it: the newest child f is unlinked, a child g is forked, and the trunk rewrites a page whose
    /// current version was born inside f's range, retaining it for g. The reclamation must keep that
    /// version (no live child may lose a version it can see), and free the rest.
    #[test]
    fn a_deferred_reclamation_keeps_a_version_a_later_child_needs() {
        let store = BranchStore::new();
        let lo = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        // epoch 1: the trunk rewrites page 0 (generation 0 -> 1), retaining [0, 1) for lo.
        store.first_write_trunk(0, &image(0));
        let f = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        let (reclaim, deferred) = store.unlink_handle(f).expect("f is live");
        assert!(!deferred);
        // Between the unlink and the reclamation: fork g, and rewrite page 0 (generation 1 -> 2),
        // which retains [1, 3) — born inside f's range (lo, f] = (0, 1] — for g.
        let g = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        store.first_write_trunk(0, &image(1));
        let before = store.stats().arena_slots_in_use;
        store.reclaim(reclaim);
        assert_eq!(store.stats().arena_slots_in_use, before, "the reclamation freed a live version");
        assert_eq!(read_gen(&store, g, 0), Some(1), "g reads page 0 as of its fork");
        assert_eq!(read_gen(&store, lo, 0), Some(0), "lo reads page 0 as of its fork");
        for id in [lo, g] {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
    }

    /// A writer that commits while a first fork is building its view outside the mutex: the writer
    /// must wait for the view to be installed (the `forking` flag), so that the commit sees the new
    /// child and retains the versions the child's view names. The test pauses the build and lets a
    /// writer thread try to commit inside the pause.
    #[test]
    fn a_commit_cannot_land_inside_a_first_fork_view_build() {
        const N: u32 = 300;
        let store = Arc::new(BranchStore::new());
        let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
        store.begin_write(id).unwrap();
        commit_all(&store, id, N, 1);
        store.end_write(id);
        let writer: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
        {
            let (s, w) = (store.clone(), writer.clone());
            *store.fork_build_pause.lock() = Some(Box::new(move || {
                let s = s.clone();
                *w.lock() = Some(std::thread::spawn(move || {
                    s.begin_write(id).unwrap();
                    commit_all(&s, id, N, 2);
                    s.end_write(id);
                }));
                // Long enough for an unobstructed writer to finish inside the build window.
                std::thread::sleep(std::time::Duration::from_millis(200));
            }));
        }
        let child = store.fork_branch(id).unwrap();
        *store.fork_build_pause.lock() = None;
        writer.lock().take().unwrap().join().unwrap();
        for p in 0..N {
            assert_eq!(read_gen(&store, child, p), Some(1), "the child's page {p}");
            assert_eq!(read_gen(&store, id, p), Some(2), "the parent's page {p}");
        }
        store.release_handle(child);
        store.release_handle(id);
        assert_eq!(store.stats().arena_slots_in_use, 0, "slots leaked");
    }
}
