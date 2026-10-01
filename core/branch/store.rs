//! Per-branch page spaces, and the rule that decides which version of a page a branch sees.
//!
//! # The model
//!
//! The trunk carries an `epoch` that its forks advance: a branch forked from the trunk records the
//! trunk's epoch at that moment as its `trunk_at`, and the epoch then increments. A version of a
//! trunk page written in epoch `born` is visible to branches whose `trunk_at` is `>= born`, until the
//! trunk overwrites it in epoch `died`; after that it is visible only to `trunk_at` in `[born, died)`.
//! A branch forked from a branch inherits its parent's `trunk_at`. A branch therefore sees, for
//! each page:
//!
//! 1. the arena page its own [`PageMap`] names, if the page is there: its own write, or an
//!    ancestor's write as of the fork that made this branch (see below); else
//! 2. the trunk's version at `trunk_at` — its current version if it was born at or before that
//!    epoch, which lives in the WAL and the database file and is read by the ordinary pager path,
//!    else the trunk's RETAINED version covering it.
//!
//! # One page map per branch: reference-counted shadowing
//!
//! Each branch holds ONE persistent [`PageMap`] naming every arena page it reads. A fork clones the
//! parent's map in O(1), so the child sees the parent's pages as of the fork; a write replaces the
//! page's entry in the writer's map only, path-copying the nodes another map shares. The maps own
//! the slots they name collectively, through `StoreInner::refs` — Rodeh's reference-counted
//! shadowing ("B-trees, Shadowing, and Clones", ACM TOS 2008; btrfs's snapshots): a slot is free
//! exactly when no map can reach it (see `page_map`). So there is no per-branch retained-version
//! bookkeeping and no parent link: a branch whose handle and connection are gone gives up its map
//! at once, which frees exactly the slots only it could reach — including pages its children had
//! all overwritten — and keeps every slot a live descendant still reads, whether or not the branch
//! that wrote it still exists.
//!
//! # Where the copies come from — the write ticket
//!
//! A slot is allocated, or a version retained, at exactly one moment: the first
//! [`crate::storage::pager::Pager::add_dirty`] of a page in a transaction, which is also the only
//! place a [`crate::storage::pager::WriteTicket`] can be minted. So every page write is preceded by
//! the copy decision by construction:
//!
//! * a **branch** writing a page commits in place into the slot its map names if no other map can
//!   reach that slot (the path is its own and the slot's count is 1); otherwise it copies the page
//!   into a fresh slot and names that one instead, so every other map keeps the old one;
//! * the **trunk** writing a page that a live branch can still see copies the pre-image into a slot
//!   and retains it, before the write reaches the WAL — so neither the commit nor a later
//!   checkpoint that moves the new version into the database file can reach the branch.
//!
//! # Reclamation of the trunk's versions
//!
//! `trunk.lineage.children` counts, for each `trunk_at`, the live branches reading the trunk at it.
//! A retained trunk version is garbage once no `trunk_at` inside `[born, died)` is counted.
//! Removing the last branch at `f` can only make versions containing `f` garbage, and a version
//! containing `f` becomes garbage exactly when it also lies strictly between `f`'s neighbouring
//! counted epochs `lo` and `hi`: `born > lo` and `died <= hi`. The versions are indexed both by
//! `born` and by `died` — ZFS's deadlists, which key a dead block by the interval that killed it
//! and split it by birth — so that each side of that query is a range, not a scan:
//!
//! * with no lower neighbour, the garbage is exactly the versions with `died` in `(f, hi]`;
//! * with no higher one, exactly those with `born` in `(lo, f]`;
//! * with both, each range also holds survivors, and the two are walked in lockstep until the
//!   shorter one ends (see [`Lineage::garbage`]).
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmarks this lane ships are single-threaded and say so.
//! * The trunk's retention predicate is the interval one: a trunk pre-image is kept while any
//!   branch at a `trunk_at` inside it lives, even if every such branch has overwritten the page.
//!   Branch-written pages have no such slack (see "One page map per branch").
//!
//! # Per-page version order (the fat node)
//!
//! One trunk page's retained versions have non-empty, pairwise disjoint `[born, died)` ranges: the
//! trunk retains `[written, epoch)` and then sets `written = epoch`. So `born` is unique per page,
//! and the version a branch at `f` sees is the one with the greatest `born <= f`, provided
//! `f < died`. The versions are kept in a map ordered by `born`, which makes that lookup a
//! predecessor search and a release a removal by key — Driscoll, Sarnak, Sleator and Tarjan's fat
//! node (JCSS 1989) with a search tree over its version stamps. [`Lineage::retain`] refuses a
//! version that would break the disjointness the search relies on.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Bound, Deref, DerefMut};
use std::time::Instant;

use super::arena::{Arena, Slot};
use super::page_map::{self, PageMap};
use super::{BranchId, BranchStats, BranchWork, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard};
use crate::{LimboError, Result};

/// Where the store's lock is taken, for the per-site counts in [`super::LockCounts`] (the order of
/// [`super::LOCK_SITES`]).
#[derive(Clone, Copy)]
enum Site {
    Fork,
    ForkTrunk,
    Open,
    Close,
    Release,
    BeginWrite,
    EndWrite,
    HoldsWriter,
    Schema,
    SetSchema,
    FirstWriteBranch,
    FirstWriteTrunk,
    Commit,
    Resolve,
    /// Accounting and membership queries: stats, zombies, needed and owned slots, `has_branches`.
    Observe,
}

/// The store's lock, held. Observation only: dropping it adds the time it was held to
/// `lock.hold_ns` when lock timing was on at the acquisition, and does nothing else.
struct Held<'a> {
    guard: MutexGuard<'a, StoreInner>,
    since: Option<Instant>,
    site: usize,
}

impl Deref for Held<'_> {
    type Target = StoreInner;
    fn deref(&self) -> &StoreInner {
        &self.guard
    }
}

impl DerefMut for Held<'_> {
    fn deref_mut(&mut self) -> &mut StoreInner {
        &mut self.guard
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        if let Some(since) = self.since {
            let held = since.elapsed().as_nanos() as u64;
            self.guard.work.lock.hold_ns += held;
            self.guard.work.lock.hold_site_ns[self.site] += held;
        }
    }
}

pub(crate) struct BranchStore {
    inner: Mutex<StoreInner>,
    /// Whether [`BranchStore::take`] times how long each acquisition holds the lock. Off by
    /// default: it is the one part of the lock accounting that adds work inside the critical
    /// section.
    lock_timing: AtomicBool,
    /// Counted `trunk_at` epochs, i.e. whether any branch reads the trunk as of a past epoch. Read
    /// without the lock on every trunk first-write so that a database with no branches pays one
    /// atomic load per written page and nothing else.
    ///
    /// The unlocked read is sound because the only transition that matters — 0 to 1 — happens in
    /// a trunk fork, which holds the trunk's WAL write lock; a trunk writer reading this holds the
    /// same lock. A 1-to-0 transition (a reap) racing the read only makes the writer take the lock
    /// and find nothing to do.
    trunk_children: AtomicUsize,
}

struct StoreInner {
    arena: Option<Arena>,
    /// How many page-map leaf nodes name each arena slot a branch wrote, indexed by slot (see
    /// `page_map`). Slots the trunk retains are not counted here; the trunk's lineage owns them.
    refs: Vec<u32>,
    next_id: u64,
    trunk: TrunkState,
    branches: HashMap<BranchId, BranchState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each trunk fork; the pre-increment value is the new branch's `trunk_at`.
    epoch: u64,
    /// For each `trunk_at` some live branch reads the trunk at, how many do. A key is removed when
    /// its count reaches 0, so the keys are exactly the epochs a live branch can read.
    children: BTreeMap<u64, u64>,
    /// Superseded versions kept because a live branch reads the trunk inside them, per page and
    /// ordered by `born` (see "Per-page version order" above).
    retained: HashMap<u32, BTreeMap<u64, Retained>>,
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
    /// retention that was not strictly needed, never skip one that was.
    written: HashMap<u32, u64>,
}

struct BranchState {
    /// Every arena page this branch reads: a clone of its parent's map at the fork, with this
    /// branch's own writes replacing their entries since.
    map: PageMap,
    /// The slot each page written in the open write transaction commits into.
    dirty: HashMap<u32, Slot>,
    /// The branch's committed schema. Shared with the parent at fork (an `Arc` clone), replaced by
    /// a committed DDL on the branch.
    schema: Arc<Schema>,
    /// The `Branch` handle is alive.
    handle: bool,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
    /// The trunk epoch this branch reads the trunk at: its own fork epoch if its parent is the
    /// trunk, else its parent's `trunk_at`.
    trunk_at: u64,
}

impl Lineage {
    /// True if a live branch reads the trunk at an epoch in `[from, to)`.
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

    /// The retained version of `page` visible at trunk epoch `f`: the born-predecessor of `f`, if
    /// it was still current at `f`. `examined` counts the versions compared against `f` — at most
    /// one; the O(log V) descent that finds it is not counted.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        let (_, v) = self.retained.get(&page)?.range(..=f).next_back()?;
        *examined += 1;
        (f < v.died).then_some(v.slot)
    }

    /// Stop counting `f` (its count reached 0) and release every retained version that only a
    /// branch at `f` could see.
    fn child_gone(&mut self, f: u64, arena: &mut Arena, work: &mut BranchWork) -> usize {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed == Some(0), "uncounted an epoch still counted, or never counted");
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

    /// The versions that held `f` and no other counted epoch, as `(born, page, died)`, once `f` has
    /// left `children`; `lo` and `hi` are its former neighbours there.
    ///
    /// Every retained version holds at least one counted epoch: it is retained only if one lies
    /// inside it, and this function hands it back the moment the last one goes. So a version with
    /// `born > lo` and `died <= hi` held `f` and nothing else, and one holding `f` that reaches
    /// back to `lo` or on to `hi` is still needed. That makes the garbage `{born > lo, died <= hi}`,
    /// and each index answers one side of it:
    ///
    /// * `lo` absent: `died` in `(f, hi]`. Every such version was born at or before `f` (it holds
    ///   a counted epoch, and there is none below `f`), so every entry the range yields is garbage.
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
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

impl BranchStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(StoreInner {
                arena: None,
                refs: Vec::new(),
                next_id: 1,
                trunk: TrunkState {
                    lineage: Lineage::default(),
                    written: HashMap::new(),
                },
                branches: HashMap::new(),
                work: BranchWork::default(),
            }),
            lock_timing: AtomicBool::new(false),
            trunk_children: AtomicUsize::new(0),
        }
    }

    /// Take the store's lock at `site`, counting the acquisition: every one, the ones that found
    /// the lock held, and how long those waited (F5's `take`, turso a7adf8704). The counts are
    /// written under the lock itself, so counting adds no shared write the lock does not already
    /// make, and the clock is read only on the contended path, by the thread that is waiting
    /// anyway — except with lock timing on, which reads it once more at the acquisition and once
    /// at the release.
    fn take(&self, site: Site) -> Held<'_> {
        let (mut guard, waited) = match self.inner.try_lock() {
            Some(guard) => (guard, None),
            None => {
                let start = Instant::now();
                let guard = self.inner.lock();
                (guard, Some(start.elapsed()))
            }
        };
        let lock = &mut guard.work.lock;
        lock.acquisitions[site as usize] += 1;
        if let Some(waited) = waited {
            lock.contended[site as usize] += 1;
            lock.wait_ns += waited.as_nanos() as u64;
            lock.wait_site_ns[site as usize] += waited.as_nanos() as u64;
        }
        let since = self
            .lock_timing
            .load(Ordering::Relaxed)
            .then(Instant::now);
        Held {
            guard,
            since,
            site: site as usize,
        }
    }

    /// Turn the lock's hold timing on or off (see [`BranchStore::take`]).
    pub(crate) fn set_lock_timing(&self, on: bool) {
        self.lock_timing.store(on, Ordering::Relaxed);
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all. Paths that rewrite the trunk without passing through
    /// `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        !self.take(Site::Observe).branches.is_empty()
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    pub(crate) fn fork_trunk(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        let mut inner = self.take(Site::ForkTrunk);
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
        inner.trunk.lineage.children.insert(f, 1);
        inner
            .branches
            .insert(id, BranchState::new(schema, f, PageMap::default()));
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch: a clone of its page map, O(1). Refused while the parent has a
    /// write transaction in progress, for the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.take(Site::Fork);
        let id = BranchId(inner.next_id);
        let StoreInner {
            branches,
            trunk,
            next_id,
            ..
        } = &mut *inner;
        let st = branches.get(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        let child = BranchState::new(st.schema.clone(), st.trunk_at, st.map.clone());
        *trunk
            .lineage
            .children
            .get_mut(&child.trunk_at)
            .expect("a live branch's trunk_at is counted") += 1;
        *next_id += 1;
        branches.insert(id, child);
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.take(Site::Open);
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
        let mut inner = self.take(Site::Close);
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
            st.dirty.clear();
        }
        self.collect(&mut inner, id);
    }

    /// The `Branch` handle has gone.
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut inner = self.take(Site::Release);
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
        let mut inner = self.take(Site::BeginWrite);
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        st.writer = true;
        st.dirty.clear();
        Ok(())
    }

    pub(crate) fn end_write(&self, id: BranchId) {
        if let Some(st) = self.take(Site::EndWrite).branches.get_mut(&id) {
            st.writer = false;
            st.dirty.clear();
        }
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.take(Site::HoldsWriter)
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let inner = self.take(Site::Schema);
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
        let mut inner = self.take(Site::FirstWriteBranch);
        let StoreInner {
            arena,
            refs,
            branches,
            work,
            ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        crate::turso_assert!(st.writer, "branch page written outside a write transaction");
        if st.dirty.contains_key(&page) {
            return Ok(());
        }
        work.branch_first_writes += 1;
        let slot = match st.map.get(page) {
            // No other map can reach it: nobody else reads this version, so it is rewritten in
            // place.
            Some(slot) if refs[slot as usize] == 1 && st.map.exclusive(page) => {
                work.in_place_writes += 1;
                slot
            }
            // Another map can reach it (or it is the trunk's): a fresh slot for this branch only.
            _ => {
                let slot = arena.alloc();
                if refs.len() <= slot as usize {
                    refs.resize(slot as usize + 1, 0);
                }
                arena.page_mut(slot).copy_from_slice(pre_image);
                if let Some(dead) = st.map.insert_counted(page, slot, refs) {
                    arena.release(dead);
                }
                slot
            }
        };
        st.dirty.insert(page, slot);
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live branch
    /// can still see the version about to be overwritten, keep a copy of it for that branch.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut inner = self.take(Site::FirstWriteTrunk);
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

    /// Commit a branch's dirty pages into the slots their copy decisions chose.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut inner = self.take(Site::Commit);
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
            let slot = st.dirty.get(&no).copied().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "branch {} committed page {no} with no copy decision behind it in this \
                     transaction",
                    id.0
                ))
            })?;
            arena
                .page_mut(slot)
                .copy_from_slice(page.get_contents().as_slice());
        }
        Ok(())
    }

    pub(crate) fn set_schema(&self, id: BranchId, schema: Arc<Schema>) -> Result<()> {
        let mut inner = self.take(Site::SetSchema);
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.take(Site::Resolve);
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
        let inner = self.take(Site::Observe);
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
            page_map_nodes: page_map::live_nodes(),
            map_work: page_map::map_work(),
            work: inner.work,
        }
    }

    /// Branch states with no handle and no open connection. Observation only; O(branches). Such a
    /// state is freed the moment it arises, so this is 0 whenever the lock is free.
    pub(crate) fn zombies(&self) -> usize {
        self.take(Site::Observe)
            .branches
            .values()
            .filter(|st| !st.handle && !st.open)
            .count()
    }

    /// The arena slots some live branch (a handle or an open connection) can still read: every
    /// slot its map names, and the trunk's retained versions at its `trunk_at` for pages its map
    /// does not hold. Every other slot in use is one no read can reach again. Observation only;
    /// O(sum of mapped pages).
    pub(crate) fn needed_slots(&self) -> usize {
        let inner = self.take(Site::Observe);
        let mut needed = HashSet::new();
        let mut examined = 0;
        for st in inner.branches.values().filter(|st| st.handle || st.open) {
            st.map.for_each(|_, slot| {
                needed.insert(slot);
            });
            for &page in inner.trunk.lineage.retained.keys() {
                if st.map.get(page).is_some() {
                    continue;
                }
                if let Some(slot) =
                    inner
                        .trunk
                        .lineage
                        .retained_at(page, st.trunk_at, &mut examined)
                {
                    needed.insert(slot);
                }
            }
        }
        needed.len()
    }

    /// Every arena slot this branch's map names: its own writes and every ancestor page it reads.
    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.take(Site::Observe);
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots = Vec::new();
        st.map.for_each(|_, slot| slots.push(slot));
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<u32> {
        self.take(Site::Observe)
            .arena
            .as_ref()
            .map_or_else(Vec::new, |a| a.slots_in_use())
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self.take(Site::Observe)
            .arena
            .as_ref()
            .is_some_and(|a| a.is_free(slot))
    }

    /// Free `id` if it has neither a handle nor an open connection: give up its page map, which
    /// frees every slot no other map reaches, and uncount its `trunk_at`, which may free trunk
    /// versions only it read. Its children, if any, are untouched: their maps hold what they read.
    /// Returns the number of arena pages released.
    fn collect(&self, inner: &mut StoreInner, id: BranchId) -> usize {
        let Some(st) = inner.branches.get(&id) else {
            return 0;
        };
        if st.handle || st.open {
            return 0;
        }
        let st = inner.branches.remove(&id).expect("just looked it up");
        let StoreInner {
            arena,
            refs,
            trunk,
            work,
            ..
        } = &mut *inner;
        work.states_freed += 1;
        let arena = arena.as_mut().expect("a branch existed, so the arena does");
        let mut freed = 0;
        st.map.release(refs, &mut |slot| {
            arena.release(slot);
            freed += 1;
        });
        let count = trunk
            .lineage
            .children
            .get_mut(&st.trunk_at)
            .expect("a live branch's trunk_at is counted");
        *count -= 1;
        if *count == 0 {
            freed += trunk.lineage.child_gone(st.trunk_at, arena, work);
            self.trunk_children.fetch_sub(1, Ordering::AcqRel);
        }
        freed
    }
}

impl StoreInner {
    /// `levels` counts the nodes consulted — the branch's page map, then the trunk if the map does
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
        if let Some(slot) = st.map.get(page) {
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
    fn new(schema: Arc<Schema>, trunk_at: u64, map: PageMap) -> Self {
        Self {
            map,
            dirty: HashMap::new(),
            schema,
            handle: true,
            open: false,
            writer: false,
            trunk_at,
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
    /// on branches before and after they fork, reaps of branches with live descendants — against a
    /// model in which each branch is a plain copy of its parent's pages at its fork. Every live
    /// branch must read, for every page, what the model says, through `resolve_into`, the path the
    /// pager uses. And the arena must hold exactly what live branches can read: the trunk's
    /// retained versions plus one slot per distinct (page, generation) a branch wrote and a live
    /// branch sees — no page kept for a branch that is gone, or that every reader has overwritten.
    #[test]
    fn every_branch_of_a_random_tree_reads_its_parent_as_of_its_fork() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_tree(seed);
        }
    }

    struct Node {
        id: BranchId,
        parent: Option<usize>,
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
        let (mut released_over_live, mut max_depth, mut wrote_after_fork) = (0, 0, 0);
        // Generations a branch wrote; the others are the trunk's.
        let mut branch_generations: HashSet<u64> = HashSet::new();
        for step in 0..2500 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            match rng.below(12) {
                0 if live.len() < 60 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    nodes.push(Node {
                        id,
                        parent: None,
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
                        parent: Some(parent),
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
                        branch_generations.insert(generation);
                        committed.push(page_with(page, generation));
                        nodes[v].sees.insert(page, generation);
                    }
                    store.commit_pages(id, &committed).unwrap();
                    store.end_write(id);
                }
                _ if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    nodes[v].handle = false;
                    let descends_from_v = |mut a: Option<usize>| loop {
                        match a {
                            Some(p) if p == v => break true,
                            Some(p) => a = nodes[p].parent,
                            None => break false,
                        }
                    };
                    if nodes.iter().any(|n| n.handle && descends_from_v(n.parent)) {
                        released_over_live += 1;
                    }
                    let reaped = store.release_handle(nodes[v].id);
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}: no connection is open");
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
            let seen: HashSet<(u32, u64)> = nodes
                .iter()
                .filter(|n| n.handle)
                .flat_map(|n| n.sees.iter().map(|(&page, &g)| (page, g)))
                .filter(|(_, g)| branch_generations.contains(g))
                .collect();
            let trunk_retained = store.inner.lock().trunk.lineage.by_born.len();
            assert_eq!(
                store.stats().arena_slots_in_use,
                trunk_retained + seen.len(),
                "seed {seed:#x} step {step}: the arena holds a page no live branch can read, or \
                 lost one it can ({trunk_retained} trunk versions, {} branch versions seen)",
                seen.len()
            );
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing.
        assert!(
            max_depth >= 10 && released_over_live > 0 && wrote_after_fork > 0,
            "seed {seed:#x}: max depth {max_depth}, reaps of a branch with a live descendant \
             {released_over_live}, writes by a branch after its first fork {wrote_after_fork}"
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    }
}
