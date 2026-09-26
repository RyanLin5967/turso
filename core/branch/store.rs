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
//!   shorter one ends (see [`Lineage::garbage`]).
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Bound;

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
    /// What a merge validates against (see [`super::merge`]).
    merge: MergeState,
}

/// The trunk-side record a merge validates against. Every field is written only while the trunk has
/// a live child: a branch forked later has `trunk_at` at or above the epoch of anything written
/// before it, so nothing written while the trunk had no child can refuse it.
#[derive(Default)]
pub(super) struct MergeState {
    /// Trunk write transactions committed with at least one page while the trunk had a child (V0).
    pub(super) trunk_commits: u64,
    /// V1: committed trunk write sets as (epoch, pages sorted), oldest first, pruned below the
    /// oldest live child's fork epoch.
    pub(super) log: VecDeque<(u64, Vec<u32>)>,
    /// V3: (table root, rowid) -> the epoch of the trunk's last write of that row.
    pub(super) row_stamps: HashMap<(i64, i64), u64>,
    /// `row_stamps` in stamping order (epochs ascend), for pruning.
    stamp_order: VecDeque<(u64, i64, i64)>,
    /// Tables the trunk wrote without naming the rows: root -> epoch.
    pub(super) table_stamps: HashMap<i64, u64>,
    /// Record each branch's page reads; the physical install's structural guard needs them.
    pub(super) track_reads: bool,
    /// Times read tracking was turned off. A branch forked while tracking was on, at this count,
    /// has had every read tracked if the count has not moved since.
    track_off: u64,
}

/// What one trunk write transaction wrote while the trunk had a live child, kept by the writing
/// connection's pager and handed to the store only if the transaction commits: its rows and tables
/// are stamped with the epoch of the commit (Silo's TIDs are assigned at commit, SOSP 2013), and
/// one that rolls back stamps nothing. A merge in progress validates its later batch members
/// against it too, as the batch's own uncommitted writes (see `branch::merge`).
#[derive(Default)]
pub(crate) struct TrunkPending {
    /// Pages first-written in the transaction.
    pub(crate) pages: HashSet<u32>,
    /// Those of `pages` that were interior b-tree pages just before that first write: the version
    /// every child forked before the transaction reads.
    pub(crate) interior: HashSet<u32>,
    /// Rows written or deleted, as (table root, rowid).
    pub(crate) rows: HashSet<(i64, i64)>,
    /// Tables written without naming the rows.
    pub(crate) tables: HashSet<i64>,
}

impl MergeState {
    fn stamp_row(&mut self, root: i64, rowid: i64, epoch: u64) {
        if self.row_stamps.insert((root, rowid), epoch) != Some(epoch) {
            self.stamp_order.push_back((epoch, root, rowid));
        }
    }

    /// Drop every stamp and log entry at or below `oldest`, the oldest live trunk child's fork
    /// epoch: a record of epoch `e` refuses a branch only if `e > trunk_at`, and every live or
    /// future child has `trunk_at >= oldest`. `None` (no child) drops everything.
    pub(super) fn prune(&mut self, oldest: Option<u64>) {
        let slack = u64::from(mutant(8));
        let keep = |e: u64| oldest.is_some_and(|o| e > o + slack);
        while self.log.front().is_some_and(|&(e, _)| !keep(e)) {
            self.log.pop_front();
        }
        while let Some(&(e, root, rowid)) = self.stamp_order.front() {
            if keep(e) {
                break;
            }
            self.stamp_order.pop_front();
            if self.row_stamps.get(&(root, rowid)) == Some(&e) {
                self.row_stamps.remove(&(root, rowid));
            }
        }
        self.table_stamps.retain(|_, e| keep(*e));
    }
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current, per page and
    /// ordered by `born` (see "Per-page version order" above).
    retained: HashMap<u32, BTreeMap<u64, Retained>>,
    /// The same versions as `(born, page, died)`, for the reclamation range query by birth.
    by_born: BTreeSet<(u64, u32, u64)>,
    /// The same versions as `(died, page, born)`, for the reclamation range query by death.
    by_died: BTreeSet<(u64, u32, u64)>,
}

/// Fire-check only: `R11_MERGE_MUTANT=n` in a TEST build breaks merge mechanism n (validators,
/// pruning and the guard, 1-10; the write-set and stamp hooks, the replay, the statement cache, the
/// scope gate and the install's isolation, 11-13 and 15-31; 14 is not built, since no SQL path
/// without DDL clears a user table's b-tree), so each test can be shown to fail for it
/// (frontier/round11/r11-merge PREREG A6, A13, A14, A15). Always false otherwise.
#[cfg(test)]
pub(crate) fn mutant(n: u32) -> bool {
    static CHOSEN: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *CHOSEN.get_or_init(|| {
        std::env::var("R11_MERGE_MUTANT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }) == n
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn mutant(_n: u32) -> bool {
    false
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
    /// Trunk commits counted when this branch forked (V0). Meaningful for trunk children only.
    commits_at_fork: u64,
    /// Rows this branch's table cursors wrote, as (table root, rowid): its row write set, each with
    /// the sequence number of its LAST write, so a merge can replay the rows in the branch's order.
    /// Kept through a rollback, so it can only over-state what the branch changed.
    rows: HashMap<(i64, i64), u64>,
    /// The last sequence number handed to `rows`.
    row_seq: u64,
    /// Roots of b-trees this branch wrote through index cursors (indexes, WITHOUT ROWID tables).
    index_roots: HashSet<i64>,
    /// This branch wrote a table without naming the rows (clear, destroy, incremental blob I/O).
    bulk: bool,
    /// This branch committed a DDL.
    ddl: bool,
    /// Pages this branch read, while the store tracks reads.
    reads: HashSet<u32>,
    /// The store's `track_off` at this branch's fork if tracking was on then: `reads` holds every
    /// read if it still equals the count.
    reads_from: Option<u64>,
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

    /// Detach the child forked at `f` and release every retained version that only it could see.
    fn child_gone(&mut self, f: u64, arena: &mut Arena, work: &mut BranchWork) -> usize {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        let dead = self.garbage(f, lo, hi, work);
        for &(born, page, died) in &dead {
            let versions = self.retained.get_mut(&page).expect("indexed version is listed");
            let v = versions.remove(&born).expect("indexed version is listed");
            work.gc_examined += 1;
            if versions.is_empty() {
                self.retained.remove(&page);
            }
            let indexed = self.by_born.remove(&(born, page, died))
                && self.by_died.remove(&(died, page, born));
            crate::turso_assert!(indexed, "a released version was missing from an index");
            arena.release(v.slot);
        }
        dead.len()
    }

    /// The versions that held `f` and no other live child, as `(born, page, died)`, once `f` has
    /// left `children`; `lo` and `hi` are its former neighbours there.
    ///
    /// Every retained version holds at least one live child's fork epoch: it is retained only if
    /// one forked inside it, and this function hands it back the moment the last one goes. So a
    /// version with `born > lo` and `died <= hi` held `f` and nothing else, and one holding `f`
    /// that reaches back to `lo` or on to `hi` is still needed. That makes the garbage
    /// `{born > lo, died <= hi}`, and each index answers one side of it:
    ///
    /// * `lo` absent: `died` in `(f, hi]`. Every such version was born at or before `f` (it holds
    ///   a live child, and there is none below `f`), so every entry the range yields is garbage.
    ///   This is the oldest child — the victim of uniform-TTL expiry — and the cost is what it frees.
    /// * `hi` absent: `born` in `(lo, f]`, every entry garbage by the same argument.
    /// * both: garbage lies in both ranges, and each also yields survivors (versions reaching past
    ///   `hi`, or back past `lo`). The ranges are walked in lockstep and the first to end is
    ///   filtered, so the walk costs twice the SMALLER range, never the larger.
    ///
    /// `gc_range_entries` counts every entry either range yields.
    fn garbage(
        &self,
        f: u64,
        lo: Option<u64>,
        hi: Option<u64>,
        work: &mut BranchWork,
    ) -> Vec<(u64, u32, u64)> {
        // `born` in (lo, f] and `died` in (f, hi], as bounds on the two indexes' first field.
        let after = |e: u64| (e, NO_PAGE, u64::MAX);
        let born_from = lo.map_or(Bound::Unbounded, |lo| Bound::Excluded(after(lo)));
        let born_to = Bound::Included(after(f));
        let died_from = Bound::Excluded(after(f));
        let died_to = hi.map_or(Bound::Unbounded, |hi| Bound::Included(after(hi)));
        let only_f = |born: u64, died: u64| {
            lo.is_none_or(|lo| born > lo)
                && born <= f
                && f < died
                && hi.is_none_or(|hi| died <= hi)
        };
        let mut by_born = self.by_born.range((born_from, born_to));
        let mut by_died = self
            .by_died
            .range((died_from, died_to))
            .map(|&(died, page, born)| (born, page, died));
        let mut seen: [Vec<(u64, u32, u64)>; 2] = Default::default();
        // Which ranges to walk: the one side's own range when a neighbour is missing, else both.
        let walk = match (lo, hi) {
            (None, _) => [false, true],
            (Some(_), None) => [true, false],
            (Some(_), Some(_)) => [true, true],
        };
        let finished = 'walk: loop {
            for side in 0..2 {
                if !walk[side] {
                    continue;
                }
                let next = if side == 0 {
                    by_born.next().copied()
                } else {
                    by_died.next()
                };
                match next {
                    Some(v) => {
                        work.gc_range_entries += 1;
                        seen[side].push(v);
                    }
                    None => break 'walk side,
                }
            }
        };
        let mut dead = std::mem::take(&mut seen[finished]);
        dead.retain(|&(born, _, died)| only_f(born, died));
        dead
    }

    fn release_all(self, arena: &mut Arena) -> Vec<Slot> {
        let mut slots = Vec::new();
        for (_, versions) in self.retained {
            slots.extend(versions.into_values().map(|v| v.slot));
        }
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
                merge: MergeState::default(),
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
        let f = inner.trunk.lineage.epoch;
        inner.trunk.lineage.epoch += 1;
        inner.trunk.lineage.children.insert(f, id);
        let mut st = BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default());
        st.commits_at_fork = inner.merge.trunk_commits;
        st.reads_from = inner.merge.track_reads.then_some(inner.merge.track_off);
        inner.branches.insert(id, st);
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
        let f = st.lineage.epoch;
        st.lineage.epoch += 1;
        st.lineage.children.insert(f, id);
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
        inner
            .branches
            .insert(id, BranchState::new(parent, f, schema, trunk_at, view));
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
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut inner = self.inner.lock();
        let Some(st) = inner.branches.get_mut(&id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        st.handle = false;
        let freed_pages = self.collect(&mut inner, id);
        Reaped {
            freed_pages,
            deferred: inner.branches.contains_key(&id),
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
            arena, branches, ..
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
        let StoreInner { arena, trunk, .. } = &mut *inner;
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
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        st.schema = schema;
        st.ddl = true;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.inner.lock();
        if inner.merge.track_reads {
            if let Some(st) = inner.branches.get_mut(&id) {
                st.reads.insert(page);
            }
        }
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
            merge_log_entries: inner.merge.log.len(),
            merge_log_pages: inner.merge.log.iter().map(|(_, p)| p.len()).sum(),
            row_stamps: inner.merge.row_stamps.len(),
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

    /// A table cursor on branch `id` wrote or deleted `rowid` in the b-tree rooted at `root`.
    pub(crate) fn branch_row_written(&self, id: BranchId, root: i64, rowid: i64) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.row_seq += 1;
            st.rows.insert((root, rowid), st.row_seq);
        }
    }

    /// An index cursor on branch `id` wrote the b-tree rooted at `root`.
    pub(crate) fn branch_index_written(&self, id: BranchId, root: i64) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.index_roots.insert(root);
        }
    }

    /// Branch `id` wrote a table without naming the rows.
    pub(crate) fn branch_bulk_written(&self, id: BranchId) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.bulk = true;
        }
    }

    /// Mutant 30 only: stamp a trunk row at WRITE time, with the epoch current then, as this store
    /// did before stamps moved to the commit (frontier/round11/r11-merge PREREG A15).
    pub(crate) fn trunk_row_written(&self, root: i64, rowid: i64) {
        self.trunk_rows_written(&[(root, rowid)]);
    }

    /// Mutant 30 only: [`Self::trunk_row_written`] for several rows.
    pub(crate) fn trunk_rows_written(&self, rows: &[(i64, i64)]) {
        let mut inner = self.inner.lock();
        let epoch = inner.trunk.lineage.epoch;
        for &(root, rowid) in rows {
            inner.merge.stamp_row(root, rowid, epoch);
        }
    }

    /// Mutant 30 only: a table stamp at write time.
    pub(crate) fn trunk_bulk_written(&self, root: i64) {
        let mut inner = self.inner.lock();
        let epoch = inner.trunk.lineage.epoch;
        inner.merge.table_stamps.insert(root, epoch);
    }

    /// The trunk write transaction in progress committed (`Some`, with what it wrote while the
    /// trunk had a child) or rolled back (`None`). A committed one's rows and tables are stamped
    /// with the epoch of the commit, and one that wrote a page is one trunk commit and one log
    /// entry; a rolled-back one stamps nothing. Page stamps (`written`) are set at the page's first
    /// write instead, by the copy decision, and stay through a rollback: a stale stamp can only
    /// refuse a merge that did not conflict, never admit one that did. On this store a fork takes
    /// the trunk's WAL write lock, so the epoch cannot move inside a transaction, and the commit's
    /// epoch is the one its first write saw.
    pub(crate) fn trunk_tx_end(&self, committed: Option<TrunkPending>) {
        let mut inner = self.inner.lock();
        let oldest = inner.trunk.lineage.children.keys().next().copied();
        let epoch = inner.trunk.lineage.epoch;
        let StoreInner { merge, work, .. } = &mut *inner;
        if let Some(tx) = committed {
            for (root, rowid) in tx.rows {
                merge.stamp_row(root, rowid, epoch);
            }
            for root in tx.tables {
                merge.table_stamps.insert(root, epoch);
            }
            if !tx.pages.is_empty() {
                let mut pages: Vec<u32> = tx.pages.into_iter().collect();
                pages.sort_unstable();
                merge.trunk_commits += 1;
                work.trunk_commits += 1;
                merge.log.push_back((epoch, pages));
            }
        }
        merge.prune(oldest);
    }

    pub(crate) fn set_track_reads(&self, on: bool) {
        let mut inner = self.inner.lock();
        let merge = &mut inner.merge;
        if merge.track_reads && !on {
            merge.track_off += 1;
        }
        merge.track_reads = on;
    }

    pub(crate) fn tracks_reads(&self) -> bool {
        self.inner.lock().merge.track_reads
    }

    /// Everything a merge of `id` needs from the store, under one hold of the lock: the scope
    /// check, every validator's verdict (the active one's probes counted), and what to install.
    /// `batch` is what the batch's earlier members wrote in the transaction in progress, which no
    /// stamp holds until it commits.
    pub(super) fn merge_prepare(
        &self,
        id: BranchId,
        active: super::merge::Validation,
        physical: bool,
        batch: &TrunkPending,
    ) -> Result<super::merge::Prepared> {
        use super::merge::{Prepared, Validation};
        let mut inner = self.inner.lock();
        let oldest = inner.trunk.lineage.children.keys().next().copied();
        inner.merge.prune(oldest);
        inner.work.merge_attempts += 1;
        let StoreInner {
            arena,
            trunk,
            branches,
            work,
            merge,
            ..
        } = &mut *inner;
        let st = branches.get(&id).ok_or_else(|| gone(id))?;
        let scope = if !st.parent.is_trunk() {
            Some("the branch is not a child of the trunk")
        } else if st.open || st.writer {
            Some("the branch has an open connection")
        } else if !st.lineage.children.is_empty() {
            Some("the branch has a live child")
        } else if st.ddl {
            Some("the branch committed a DDL")
        } else if st.bulk {
            Some("the branch wrote a table without naming the rows")
        } else if physical && st.reads_from != Some(merge.track_off) {
            Some("the branch's reads were not all tracked, which the physical install needs")
        } else {
            None
        };
        let at = st.trunk_at;
        let written = |page: u32| trunk.written.get(&page).copied().unwrap_or(0);
        let (m2, m3, m4) = (
            u64::from(mutant(2)),
            u64::from(mutant(3)),
            u64::from(mutant(4)),
        );
        let mut probes = 0u64;
        let count = |v: Validation, probes: &mut u64| {
            if v == active {
                *probes += 1;
            }
        };
        // V0: any trunk commit since the fork, or any page the transaction in progress wrote (an
        // earlier member of the same batch).
        let commits_since_fork = merge.trunk_commits - st.commits_at_fork;
        count(Validation::Scalar, &mut probes);
        let scalar = commits_since_fork > u64::from(mutant(1))
            || (!batch.pages.is_empty() && !mutant(24));
        // V2: any page this branch wrote that the trunk wrote after the fork.
        let mut page = false;
        for &p in st.current.keys() {
            count(Validation::PageStamp, &mut probes);
            if written(p) + u64::from(mutant(9)) > at + m2 || batch.pages.contains(&p) {
                page = true;
                break;
            }
        }
        // V3: any row this branch wrote that the trunk wrote after the fork.
        let mut key = false;
        for &(root, rowid) in st.rows.keys() {
            count(Validation::KeyStamp, &mut probes);
            let row = merge.row_stamps.get(&(root, rowid)).copied().unwrap_or(0);
            let table = merge.table_stamps.get(&root).copied().unwrap_or(0);
            if row.max(table) + u64::from(mutant(10)) > at + m3
                || batch.rows.contains(&(root, rowid))
                || batch.tables.contains(&root)
            {
                key = true;
                break;
            }
        }
        // V1, computed only when active: every committed trunk write set since the fork, newest
        // first, checked page by page against this branch's write set.
        let log = (active == Validation::Log).then(|| {
            for (e, pages) in merge.log.iter().rev() {
                if *e <= at + m4 {
                    break;
                }
                work.merge_log_entries_scanned += 1;
                for p in pages {
                    probes += 1;
                    if st.current.contains_key(p) {
                        return true;
                    }
                }
            }
            false
        });
        work.merge_probes += probes;
        // The physical install's guard: an interior page this branch read that the trunk wrote
        // since the fork (its type read from the version the branch saw).
        let structural = physical.then(|| {
            if mutant(5) {
                return false;
            }
            let mut unused = 0;
            for &q in &st.reads {
                work.merge_structural_probes += 1;
                if written(q) <= at {
                    // Not committed since the fork. An earlier member of this batch may have
                    // written it; the branch read the version before this transaction's first
                    // write of it, whose kind the pager recorded.
                    if batch.interior.contains(&q) && !mutant(31) {
                        return true;
                    }
                    continue;
                }
                let Some(slot) = trunk.lineage.retained_at(q, at, &mut unused) else {
                    // Written after the fork while this branch lived, so a version was retained;
                    // not finding one means the bookkeeping is broken, and guessing is unsound.
                    return true;
                };
                let base = arena.as_ref().expect("a branch exists").page(slot);
                let kind = base[if q == 1 { 100 } else { 0 }];
                if kind == 0x02 || kind == 0x05 {
                    return true;
                }
            }
            false
        });
        let arena = arena.as_ref().expect("a branch exists, so the arena does");
        let pages = if physical {
            let mut pages: Vec<(u32, Vec<u8>)> = st
                .current
                .iter()
                .map(|(&p, owned)| (p, arena.page(owned.slot).to_vec()))
                .collect();
            pages.sort_unstable_by_key(|(p, _)| *p);
            pages
        } else {
            Vec::new()
        };
        // The replay applies rows in the branch's last-write order: a UNIQUE value the branch moved
        // from one row to another must leave the first before it reaches the second.
        let mut ordered: Vec<((i64, i64), u64)> = st.rows.iter().map(|(&k, &s)| (k, s)).collect();
        if mutant(15) {
            ordered.sort_unstable_by_key(|&(k, _)| k);
        } else {
            ordered.sort_unstable_by_key(|&(_, s)| s);
        }
        let rows: Vec<(i64, i64)> = ordered.into_iter().map(|(k, _)| k).collect();
        Ok(Prepared {
            scope,
            commits_since_fork,
            scalar,
            log,
            page,
            key,
            structural,
            pages_written: st.current.len(),
            pages,
            rows,
            index_roots: st.index_roots.iter().copied().collect(),
            schema: st.schema.clone(),
        })
    }

    /// Count a merge's outcome.
    pub(super) fn merge_counted(&self, f: impl FnOnce(&mut BranchWork)) {
        f(&mut self.inner.lock().work);
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist. Returns the number of arena pages released.
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId) -> usize {
        let mut freed = 0;
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return freed;
            };
            if st.handle || st.open || !st.lineage.children.is_empty() {
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
            commits_at_fork: 0,
            rows: HashMap::new(),
            row_seq: 0,
            index_roots: HashSet::new(),
            bulk: false,
            ddl: false,
            reads: HashSet::new(),
            reads_from: None,
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
