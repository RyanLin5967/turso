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
//! # Trunk pages without the file — a shared, versioned page cache
//!
//! A page no branch in a chain wrote is the trunk's, and the pager reads it through the WAL or the
//! database file. Every branch connection starts with an empty page cache, so under many short
//! connections every one of those reads is a read system call on the one file every thread shares,
//! and on this box the kernel serialises them (PREREG amendments 7 and 8). The store therefore keeps
//! the trunk's pages as branches have read them, shared by every branch, keyed by page and by the
//! trunk epoch of the page's last write — the shared buffer pool of every server database, with the
//! version in the key as in a buffer tag with its LSN.
//!
//! A cached version is immutable. A branch reads the trunk's current version of a page only when
//! the trunk's last write to it came in an epoch at or before the branch's `trunk_at`; that epoch
//! was closed by a fork, which holds the WAL write lock, so no trunk write can change the version
//! the key names. The one gap is a trunk with no live child: it writes without telling the store
//! (see `first_write_trunk`'s caller), so `written` can name an epoch whose page has since changed.
//! The trunk's last child going therefore bumps the cache's generation, and a version cached under
//! an older generation is never served. The cache is filled by the pager after it reads a page for
//! a branch ([`BranchStore::fill_trunk_page`]), under the key [`BranchStore::resolve_into`] gave.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch. Correct, and a known wall under concurrent writers on
//!   different branches; the benchmark this lane ships is single-threaded and says so. Every
//!   acquisition goes through [`BranchStore::lock`], which counts it (see [`BranchWork`]), so the
//!   wall can be read from integers rather than inferred from a latency curve.
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
use std::ops::{Bound, Deref, DerefMut};
use std::time::Instant;

use super::arena::{Arena, Slot};
use super::page_map::PageMap;
use super::{BranchId, BranchStats, BranchWork, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard, OnceLock};
use crate::{LimboError, Result};
use arc_swap::ArcSwapOption;

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
    /// Whether [`BranchStore::lock`] times how long each acquisition holds the lock. Off by default:
    /// it is the one part of the lock accounting that adds work inside the critical section.
    lock_timing: AtomicBool,
    /// The trunk's pages as branches have read them (see "Trunk pages without the file").
    trunk_pages: TrunkPages,
    /// The trunk's page size and reserved bytes per page, recorded at the first fork. Neither can
    /// change while a branch exists (both need VACUUM, which is refused), so a branch connection
    /// takes its page format from here instead of reading the trunk's file header.
    trunk_format: OnceLock<(usize, u8)>,
    /// r11-walpin FW3 for this database's branches: taken from the process switch when the store
    /// is created (`walpin::set_fixes` before open), or set per database by a test.
    fw3: std::sync::atomic::AtomicBool,
}

/// Where the page a branch asked for comes from.
pub(crate) enum Resolved {
    /// The page is in the caller's buffer: a branch version from the arena, or the trunk's version
    /// from the shared cache.
    Filled,
    /// The trunk's version, from the shared cache, not copied: the caller holds these bytes as its
    /// page's buffer and must copy them before its first write (FS5).
    Shared(Arc<crate::alloc::DynBoxedSlice<u8>>),
    /// The branch sees the trunk's current version and the cache does not hold it: the caller reads
    /// it through the WAL or the database file, and may hand it to
    /// [`BranchStore::fill_trunk_page`] under this key.
    Trunk(TrunkPageKey),
}

/// A version of a trunk page: the page, the trunk epoch of its last write, and the cache
/// generation it was resolved in.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TrunkPageKey {
    page: u32,
    epoch: u64,
    generation: u64,
}

/// A map from page number to `T` that any thread can read without a lock: three levels of 2^10,
/// 2^10 and 2^12 entries over the 32-bit page number, each installed once and never freed or moved
/// until the store drops. A lookup is three acquire loads and no write, so readers share the lines
/// they read instead of taking them from one another.
struct Radix<T> {
    top: OnceLock<Box<[OnceLock<Box<[OnceLock<Box<[T]>>]>>]>>,
}

impl<T: Default> Radix<T> {
    const TOP: usize = 1 << 10;
    const MID: usize = 1 << 10;
    const LEAF: usize = 1 << 12;

    fn new() -> Self {
        Self {
            top: OnceLock::new(),
        }
    }

    fn split(page: u32) -> (usize, usize, usize) {
        let page = page as usize;
        (page >> 22, (page >> 12) & (Self::MID - 1), page & (Self::LEAF - 1))
    }

    fn get(&self, page: u32) -> Option<&T> {
        let (t, m, l) = Self::split(page);
        let leaf = self.top.get()?[t].get()?[m].get()?;
        Some(&leaf[l])
    }

    fn get_or_insert(&self, page: u32) -> &T {
        let (t, m, l) = Self::split(page);
        let top = self
            .top
            .get_or_init(|| (0..Self::TOP).map(|_| OnceLock::new()).collect());
        let mid = top[t].get_or_init(|| (0..Self::MID).map(|_| OnceLock::new()).collect());
        let leaf = mid[m].get_or_init(|| (0..Self::LEAF).map(|_| T::default()).collect());
        &leaf[l]
    }
}

/// One cached trunk page. `std::sync::Arc`, as `arc_swap` requires. The bytes sit in their own
/// shared allocation so that a branch pager can hold them as its page's buffer without a copy
/// (r11-sessions FS5); they are never written after the fill.
struct CachedPage {
    generation: u64,
    epoch: u64,
    bytes: Arc<crate::alloc::DynBoxedSlice<u8>>,
}

struct TrunkPages {
    /// Bumped whenever the trunk's last child goes (see "Trunk pages without the file").
    generation: AtomicU64,
    pages: Radix<ArcSwapOption<CachedPage>>,
}

impl TrunkPages {
    /// The bytes of the version `key` names, if it is cached.
    fn get(&self, key: TrunkPageKey) -> Option<Arc<crate::alloc::DynBoxedSlice<u8>>> {
        let slot = self.pages.get(key.page)?;
        let cached = slot.load();
        match cached.as_deref() {
            Some(p) if p.generation == key.generation && p.epoch == key.epoch => {
                Some(p.bytes.clone())
            }
            _ => None,
        }
    }

    /// Copy the version `key` names into `out`, if it is cached.
    fn copy_into(&self, key: TrunkPageKey, out: &mut [u8]) -> bool {
        match self.get(key) {
            Some(bytes) => {
                out.copy_from_slice(&bytes);
                true
            }
            None => false,
        }
    }

    /// Cache `bytes` as the version `key` names, unless the generation has moved on since `key` was
    /// resolved or a version at least as new is already cached.
    fn fill(&self, key: TrunkPageKey, bytes: &[u8]) {
        if key.generation != self.generation.load(Ordering::Acquire) {
            return;
        }
        let new = std::sync::Arc::new(CachedPage {
            generation: key.generation,
            epoch: key.epoch,
            bytes: Arc::new(bytes.to_vec().into_boxed_slice()),
        });
        self.pages.get_or_insert(key.page).rcu(|old| match old {
            Some(p) if p.generation == key.generation && p.epoch >= key.epoch => Some(p.clone()),
            _ => Some(new.clone()),
        });
    }
}

/// The store's lock, held. Observation only: dropping it adds the time it was held to
/// `lock_hold_ns` when lock timing was on at the acquisition, and does nothing else.
struct Held<'a> {
    guard: MutexGuard<'a, StoreInner>,
    since: Option<Instant>,
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
            self.guard.work.lock_hold_ns += since.elapsed().as_nanos() as u64;
        }
    }
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
            }),
            trunk_children: AtomicUsize::new(0),
            lock_timing: AtomicBool::new(false),
            trunk_pages: TrunkPages {
                generation: AtomicU64::new(0),
                pages: Radix::new(),
            },
            trunk_format: OnceLock::new(),
            fw3: std::sync::atomic::AtomicBool::new(super::walpin::fw3()),
        }
    }

    /// The trunk's page size and reserved bytes per page, once a branch has been forked.
    pub(crate) fn trunk_page_format(&self) -> Option<(usize, u8)> {
        self.trunk_format.get().copied()
    }

    /// Cache a trunk page the pager has read for a branch, under the key [`Self::resolve_into`]
    /// gave for it. `bytes` must be the whole page as the WAL or the database file held it under
    /// the reading connection's snapshot.
    pub(crate) fn fill_trunk_page(&self, key: TrunkPageKey, bytes: &[u8]) {
        let page_size = self.trunk_format.get().map(|f| f.0);
        crate::turso_assert!(
            page_size == Some(bytes.len()),
            "a trunk page to cache is not one page long"
        );
        self.trunk_pages.fill(key, bytes);
    }

    pub(crate) fn fw3(&self) -> bool {
        self.fw3.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn set_fw3(&self, on: bool) {
        self.fw3.store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Take the store's lock, counting the acquisition into `work`: every one, the ones that found
    /// the lock held, and how long those waited. The counts are written under the lock itself, so
    /// counting adds no shared write the lock does not already make, and the clock is read only on
    /// the contended path, by the thread that is waiting anyway — except with lock timing on, which
    /// reads it once more at the acquisition and once at the release.
    fn lock(&self) -> Held<'_> {
        let (mut guard, waited) = match self.inner.try_lock() {
            Some(guard) => (guard, None),
            None => {
                let start = Instant::now();
                let guard = self.inner.lock();
                (guard, Some(start.elapsed()))
            }
        };
        let work = &mut guard.work;
        work.lock_acquisitions += 1;
        if let Some(waited) = waited {
            work.lock_contended += 1;
            work.lock_wait_ns += waited.as_nanos() as u64;
        }
        let since = self.lock_timing.load(Ordering::Relaxed).then(Instant::now);
        Held { guard, since }
    }

    /// Turn the lock-hold timing on or off (see [`BranchWork::lock_hold_ns`]).
    pub(crate) fn set_lock_timing(&self, on: bool) {
        self.lock_timing.store(on, Ordering::Relaxed);
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
    pub(crate) fn fork_trunk(
        &self,
        schema: Arc<Schema>,
        page_size: usize,
        reserved_space: u8,
    ) -> Result<BranchId> {
        let format = *self.trunk_format.get_or_init(|| (page_size, reserved_space));
        if format != (page_size, reserved_space) {
            return Err(LimboError::InternalError(format!(
                "branches were forked from {}-byte pages with {} reserved bytes, but the database \
                 now has {page_size} and {reserved_space}",
                format.0, format.1
            )));
        }
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
        inner.branches.insert(
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
        );
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.lock();
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
        let freed_pages = self.collect(&mut inner, id);
        Reaped {
            freed_pages,
            deferred: inner.branches.contains_key(&id),
        }
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut inner = self.lock();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
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
        self
            .lock()
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
        let mut inner = self.lock();
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
        let mut inner = self.lock();
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
        let mut inner = self.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    ///
    /// The trunk's version comes from the shared cache when it holds it; otherwise the answer is
    /// [`Resolved::Trunk`] with the key to cache the caller's read under.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<Resolved> {
        self.resolve_impl(id, page, out, false)
    }

    /// As [`Self::resolve_into`], except that a trunk version the shared cache holds is handed
    /// back as [`Resolved::Shared`] — the cached bytes themselves, for the caller to hold as its
    /// page's (immutable) buffer — instead of being copied into `out` (FS5).
    pub(crate) fn resolve_shared(
        &self,
        id: BranchId,
        page: u32,
        out: &mut [u8],
    ) -> Result<Resolved> {
        self.resolve_impl(id, page, out, true)
    }

    fn resolve_impl(&self, id: BranchId, page: u32, out: &mut [u8], share: bool) -> Result<Resolved> {
        let mut inner = self.lock();
        let (mut levels, mut examined) = (0, 0);
        let resolved = inner.resolve(id, page, &mut levels, &mut examined);
        inner.work.resolve_calls += 1;
        inner.work.resolve_levels += levels;
        inner.work.resolve_retained_examined += examined;
        let Some(slot) = resolved? else {
            // `resolve` answered "the trunk's current version", so the trunk's last write to this
            // page came at or before the branch's `trunk_at`, in a closed epoch.
            let key = TrunkPageKey {
                page,
                epoch: inner.trunk.written.get(&page).copied().unwrap_or(0),
                generation: self.trunk_pages.generation.load(Ordering::Acquire),
            };
            if share {
                if let Some(bytes) = self.trunk_pages.get(key) {
                    inner.work.trunk_page_hits += 1;
                    return Ok(Resolved::Shared(bytes));
                }
            } else if self.trunk_pages.copy_into(key, out) {
                inner.work.trunk_page_hits += 1;
                return Ok(Resolved::Filled);
            }
            inner.work.trunk_page_misses += 1;
            return Ok(Resolved::Trunk(key));
        };
        out.copy_from_slice(
            inner
                .arena
                .as_ref()
                .expect("a slot resolved, so the arena exists")
                .page(slot),
        );
        Ok(Resolved::Filled)
    }

    /// r11-walpin FW3: whether branch `id` still reads `page` from the trunk (the store holds no
    /// version of it for this branch). Not counted in the work counters.
    pub(crate) fn sees_trunk(&self, id: BranchId, page: u32) -> Result<bool> {
        let inner = self.lock();
        let (mut levels, mut examined) = (0, 0);
        Ok(inner.resolve(id, page, &mut levels, &mut examined)?.is_none())
    }

    pub(crate) fn stats(&self) -> BranchStats {
        let inner = self.lock();
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
            work: inner.work,
        }
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.lock();
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
        self
            .lock()
            .arena
            .as_ref()
            .map_or_else(Vec::new, |a| a.slots_in_use())
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self
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
                if self.trunk_children.fetch_sub(1, Ordering::AcqRel) == 1 {
                    // From here the trunk writes without telling the store, so no cached version
                    // can be trusted once a branch exists again. No branch can read in between:
                    // a fork needs this lock.
                    self.trunk_pages.generation.fetch_add(1, Ordering::AcqRel);
                }
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
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
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
                    let in_arena = matches!(
                        store.resolve_into(*id, page, &mut buf).unwrap(),
                        Resolved::Filled
                    );
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

    /// The lock accounting, forced to fire and shown not to fire spuriously. One thread alone makes
    /// only uncontended acquisitions, one per call, and with timing off no hold time. A holder that
    /// keeps the lock for 500 ms while a second thread asks for it makes that acquisition contended,
    /// with a non-zero wait, and with timing on its own hold is counted before the waiter can read
    /// the counters. The bounds are loose on purpose: the waiter would have to be descheduled for the
    /// whole 500 ms to miss the hold, and nothing here is timed tighter than "more than zero".
    #[test]
    fn lock_accounting_counts_a_forced_wait_and_nothing_else() {
        let store = Arc::new(BranchStore::new());
        let base = store.stats().work;
        for _ in 0..10 {
            store.stats();
        }
        let quiet = store.stats().work;
        assert_eq!(quiet.lock_acquisitions - base.lock_acquisitions, 11);
        assert_eq!(
            (quiet.lock_contended, quiet.lock_wait_ns, quiet.lock_hold_ns),
            (0, 0, 0),
            "one thread, timing off: nothing contended, nothing timed"
        );

        store.set_lock_timing(true);
        let hold = std::time::Duration::from_millis(500);
        let (tx, rx) = std::sync::mpsc::channel();
        let holder = {
            let store = store.clone();
            std::thread::spawn(move || {
                let held = store.lock();
                tx.send(()).unwrap();
                std::thread::sleep(hold);
                drop(held);
            })
        };
        rx.recv().unwrap();
        let forced = store.stats().work;
        holder.join().unwrap();
        assert_eq!(forced.lock_contended, 1, "the waiting acquisition was not counted");
        assert!(forced.lock_wait_ns > 0, "a contended acquisition waited 0 ns");
        assert!(
            forced.lock_hold_ns >= hold.as_nanos() as u64,
            "the holder's {hold:?} was counted as {} ns",
            forced.lock_hold_ns
        );
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
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
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
                    let got = if matches!(
                        store.resolve_into(n.id, page, &mut buf).unwrap(),
                        Resolved::Filled
                    ) {
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

    /// The shared trunk-page cache against a model. Children fork and are reaped, often down to
    /// none, so the trunk also writes pages while it has no child — as the pager then does, with no
    /// copy decision and so nothing the store can see. The trunk rewrites pages with and without
    /// children, and after every step every live child reads every page through `resolve_into`. A
    /// miss is answered from the model's trunk and cached under the key the store gave, as the
    /// pager does after its read. Every read must return the page as of the child's fork, and the
    /// cache must have served reads: a cache that never hits has not been tested.
    #[test]
    fn the_trunk_page_cache_serves_each_branch_the_version_it_forked_from() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_cache(seed);
        }
    }

    fn run_cache(seed: u64) {
        let store = BranchStore::new();
        let mut rng = Rng(seed);
        let mut current: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut live: Vec<(BranchId, HashMap<u32, u64>)> = Vec::new();
        let (mut generation, mut childless_writes, mut emptied) = (0u64, 0u64, 0u64);
        let mut buf = vec![0u8; PAGE];
        for step in 0..3000 {
            match rng.below(10) {
                0..=2 if live.len() < 3 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
                    live.push((id, current.clone()));
                }
                3..=5 => {
                    let page = rng.below(u64::from(PAGES)) as u32;
                    if store.trunk_has_children() {
                        store.first_write_trunk(page, &image(current[&page]));
                    } else {
                        childless_writes += 1;
                    }
                    generation += 1;
                    current.insert(page, generation);
                }
                _ if !live.is_empty() => {
                    let (id, _) = live.swap_remove(rng.below(live.len() as u64) as usize);
                    store.release_handle(id);
                    if live.is_empty() {
                        emptied += 1;
                    }
                }
                _ => {}
            }
            for (id, view) in &live {
                for page in 0..PAGES {
                    // Odd steps take the pager's by-reference path (FS5), even steps the copying one.
                    let resolved = if step % 2 == 1 {
                        store.resolve_shared(*id, page, &mut buf)
                    } else {
                        store.resolve_into(*id, page, &mut buf)
                    };
                    let got = match resolved.unwrap() {
                        Resolved::Filled => u64::from_le_bytes(buf[..8].try_into().unwrap()),
                        Resolved::Shared(bytes) => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                        Resolved::Trunk(key) => {
                            store.fill_trunk_page(key, &image(current[&page]));
                            current[&page]
                        }
                    };
                    assert_eq!(
                        got, view[&page],
                        "seed {seed:#x} step {step}: branch {} read the wrong page {page}",
                        id.0
                    );
                }
            }
        }
        let work = store.stats().work;
        assert!(
            work.trunk_page_hits > 0 && childless_writes > 0 && emptied > 0,
            "seed {seed:#x}: cache hits {}, trunk writes with no child {childless_writes}, times the \
             last child went {emptied}",
            work.trunk_page_hits
        );
        for (id, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x}: versions leaked");
    }
}
