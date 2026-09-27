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
//!   and keeps it, before the write reaches the WAL — so neither the commit nor a later
//!   checkpoint that moves the new version into the database file can reach the child.
//!
//! # The trunk keeps sub-page versions — a version store at chunk granularity
//!
//! A trunk write usually changes a few bytes of a page (an in-place row update changes the row),
//! yet a child forked before it must still read the rest of the page as it was. Keeping every
//! superseded page whole costs a page per retained version; at 10^6 live branches that is tens of
//! gigabytes for a trunk of megabytes (r11-space, PREREG §3). The published answer is to keep the
//! superseded RECORD, not its page: the SQL Server version store, ImmortalDB and Hekaton keep row
//! versions, the Bw-tree keeps delta records. The pager has no rows, so the unit here is a
//! fixed-size byte CHUNK of the page (`chunk_size`, 64 bytes by default), which a row update covers
//! with two or three chunks:
//!
//! * The trunk's first write of page `p` in epoch `e` keeps the whole pre-image as the page's
//!   PENDING image (`died = e`), for as long as a child forked before `e` lives. At most one per
//!   page, so the pending images cost at most the trunk's write set, whatever the number of
//!   branches.
//! * At the page's NEXT trunk write, its pre-image is the previous write's post-image, so the
//!   previous write's change is now known: the pending image is compared with it chunk by chunk,
//!   and each chunk that differs becomes a retained CHUNK version `[born_c, died)`, where `born_c`
//!   is the epoch of that chunk's previous change — kept only if a live child forked in that range
//!   (the same interval predicate as a whole page, applied per chunk). The pending image is then
//!   released and this write's pre-image becomes the new one.
//! * A child forked at `f` reads a page the trunk has written since `f` as the pending image with
//!   every chunk version containing `f` laid over it: a chunk changed between `f` and the pending
//!   write has a version holding its bytes at `f`, and every other chunk is the same in the pending
//!   image as at `f`. At most one version per chunk contains `f`, so a read consults at most
//!   `page_size / chunk_size` chunks, whatever the number of branches or versions.
//!
//! Every write the trunk makes while it has a child passes through here (the write ticket), so the
//! chain of pending images is unbroken for every live child: the trunk writes without consulting
//! the store only while it has no child, and a pending image is released as soon as no live child
//! is older than it.
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
//! The same query serves whole pages (a branch's own versions) and chunks (the trunk's). A pending
//! image is garbage once the oldest live child of the trunk is no older than its write.
//!
//! # Bounded reclamation at the trunk — the horizon sweep
//!
//! At chunk granularity almost every version's `[born, died)` holds some live child, so garbage
//! appears mostly when an OLD child goes, and it is everything that child alone could see: measured,
//! up to two million chunk versions in one reap at 10^6 live branches (r11-space, raw
//! `s2a_w32768_n1e6`). The trunk therefore reclaims incrementally, as MVCC engines do below their
//! oldest snapshot (EBR; PostgreSQL's `OldestXmin`; Hekaton's GC watermark):
//!
//! * every version and pending image whose `died` is no later than the oldest live child's fork is
//!   garbage (no live child can have forked inside it), and `by_died` lists them first; the SWEEP
//!   frees at most `gc_budget` of each per store call;
//! * the oldest child's reap therefore walks nothing — its garbage all has `died` at or before the
//!   new oldest fork;
//! * a middle or newest child's reap queues its garbage query as a TASK, which store calls advance
//!   `gc_budget` steps at a time (one index entry visited, or one candidate examined), resuming each
//!   side of the lockstep walk after the last key it visited. Until its task frees it, garbage sits
//!   where no read can reach it: a read's predecessor search finds the version that contains its
//!   fork, and a garbage version contains none. A candidate is freed only if, when examined, no live
//!   child forked inside it — the query's bounds are those of the moment the task was queued, and a
//!   version split off since can hold a child forked since; a version another task or the sweep has
//!   freed is skipped. (Abandoning long walks instead, and leaving their garbage to the horizon,
//!   kept up to a third more versions at 10^5 branches: r11-space PREREG A4.)
//!
//! So a store call visits at most `2 * gc_budget` index entries and releases at most `gc_budget`
//! pending images, and every task completes — except the reap of the trunk's LAST child, which
//! releases everything at once (nothing can be read any more) and forgets the per-page epochs
//! (`written`, `chunk_born`), whose absence is their conservative value. With no budget (the tests'
//! eager mode) every reap frees its garbage at once, as before.
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
//! * A BRANCH's own versions stay whole pages: a branch that rewrites a page its child can see
//!   retains the page. Only the trunk keeps chunks.
//!
//! # Per-page version order (the fat node)
//!
//! Within one node, one page's (or one chunk's) retained versions have non-empty, pairwise
//! disjoint `[born, died)` ranges: a branch retains `[owned.born, epoch)` and re-bears its current
//! version at `epoch`, and the trunk retains a chunk's `[born_c, died)` and sets `born_c = died`.
//! So `born` is unique per (node, page, chunk), and the version a child forked at `f` sees is the
//! one with the greatest `born <= f`, provided `f < died`. The versions are kept in a map ordered
//! by `(chunk, born)`, which makes that lookup a predecessor search and a release a removal by key
//! — Driscoll, Sarnak, Sleator and Tarjan's fat node (JCSS 1989) with a search tree over its
//! version stamps. [`Lineage::retain`] refuses a version that would break the disjointness the
//! search relies on.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
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

/// The trunk's chunk size when `TURSO_BRANCH_CHUNK_BYTES` does not set one.
const DEFAULT_CHUNK_BYTES: usize = 64;
/// The trunk's reclamation steps per store call when `TURSO_BRANCH_GC_BUDGET` does not set one.
const DEFAULT_GC_BUDGET: usize = 64;

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
    /// FS9 (r11-sessions): serve retained trunk versions by reference (see `retained_clones`).
    fs9: AtomicBool,
    /// FS10 (r11-sessions): branch pagers release their private cache entries for pages held by
    /// reference when their connection's last statement ends (see `Pager::release_shared_pages`).
    fs10: AtomicBool,
    /// FS9B (r11-sessions): serve inherited ancestor-branch versions by reference (`Arena::clones`).
    fs9b: AtomicBool,
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

    /// Entries of the installed leaves for which `present` holds.
    fn count(&self, present: impl Fn(&T) -> bool) -> usize {
        let Some(top) = self.top.get() else {
            return 0;
        };
        top.iter()
            .filter_map(|mid| mid.get())
            .flat_map(|mid| mid.iter().filter_map(|leaf| leaf.get()))
            .map(|leaf| leaf.iter().filter(|t| present(t)).count())
            .sum()
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
    /// FS9 (r11-sessions), keyed by U11's f-interval (r12-lakehouse): clones of the trunk's past
    /// page images — a pending image with the chunk versions containing a fork laid over it — keyed
    /// by `(page, lo, hi)` from [`TrunkState::past_interval`], which shows that two live children
    /// whose reads compute the same key read the same bytes. Each clone is built on its key's first
    /// read and handed out by reference, like FS5's trunk pages; it is dropped once the oldest live
    /// fork reaches `hi` (see `collect`), and every clone goes with the trunk's last child. Prior
    /// art: Oracle's consistent-read clones in the buffer cache [RECALLED].
    retained_clones: HashMap<(u32, u64, u64), Arc<crate::alloc::DynBoxedSlice<u8>>>,
    /// The keys of `retained_clones` as `(hi, page, lo)`, for the horizon eviction.
    clones_by_hi: BTreeSet<(u64, u32, u64)>,
}

/// Where `StoreInner::resolve_origin` found a page.
enum Origin {
    /// In the arena: the branch's own current version (mutable: the branch rewrites it in place
    /// when no child can see it).
    Arena(Slot),
    /// In the arena: a version the branch sees through an ANCESTOR branch (its inherited page map):
    /// never written again while the branch lives.
    Inherited(Slot),
    /// The trunk's page as of `at` (the branch's `trunk_at`), which the trunk has written since:
    /// the pending image in `base`, with the chunk versions that contain `at` laid over it.
    TrunkPast { page: u32, base: Slot, at: u64 },
    /// The trunk's current version.
    Trunk,
}

/// Where a retained version sits inside its page: nowhere (`()`, the version is the whole page, as
/// a branch keeps them) or a chunk index (`u16`, as the trunk keeps them).
trait Unit: Copy + Ord + Hash + Default + Debug {
    /// Sorts after every unit, for the index range bounds.
    const LAST: Self;
}

impl Unit for () {
    const LAST: Self = ();
}

impl Unit for u16 {
    const LAST: Self = u16::MAX;
}

#[derive(Default)]
struct Lineage<U: Unit> {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current, per page and
    /// ordered by `(unit, born)` (see "Per-page version order" above).
    retained: HashMap<u32, BTreeMap<(U, u64), Retained>>,
    /// The same versions as `(born, page, unit, died)`, for the reclamation range query by birth.
    by_born: BTreeSet<(u64, u32, U, u64)>,
    /// The same versions as `(died, page, unit, born)`, for the reclamation range query by death.
    by_died: BTreeSet<(u64, u32, U, u64)>,
    /// How many versions `retained` holds. Observation only.
    versions: usize,
}

/// No page has this number (SQLite's largest is `u32::MAX - 1`; [`Lineage::retain`] refuses it), so
/// `(e, NO_PAGE, U::LAST, u64::MAX)` sorts after every index entry whose first field is `e`.
const NO_PAGE: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Retained {
    born: u64,
    died: u64,
    slot: Slot,
}

struct TrunkState {
    lineage: Lineage<u16>,
    /// The trunk epoch of its last write to each page. Absent means "before the first fork that
    /// was live at the time", i.e. epoch 0, which is the conservative answer: it can only cause a
    /// retention that was not strictly needed, never skip one that was.
    written: HashMap<u32, u64>,
    /// Per page, the pre-image of its latest trunk write (`died` = `written`), kept while a child
    /// forked before that write lives.
    pending: HashMap<u32, Pending>,
    /// The pending images as `(died, page)`, released from the front as the oldest child ages.
    pending_by_died: BTreeSet<(u64, u32)>,
    /// Per page, the epoch of each chunk's last change the store has seen. Zero (absent) is the
    /// conservative answer, as for `written`.
    chunk_born: HashMap<u32, Box<[u64]>>,
    /// Slots of `chunk_size` bytes for the retained chunks; created with the page arena.
    chunks: Option<Arena>,
    /// Bytes per chunk: the configured size, or the page size if that is smaller.
    chunk_size: usize,
    /// Reclamation steps per store call (see "Bounded reclamation"); `None` reclaims eagerly.
    gc_budget: Option<usize>,
    /// Reaps' garbage queries still running, oldest first.
    gc_tasks: VecDeque<GcTask>,
}

/// A reap's garbage query (`Lineage::garbage`, with the same lockstep and the same answer), run a
/// few steps at a time. Side 0 walks `by_born` over `born` in `(lo, f]`; side 1, only when `hi`
/// exists, walks `by_died` over `died` in `(f, hi]`. Each side resumes after the last key it
/// visited, so versions inserted or removed between steps do not disturb it (module doc).
struct GcTask {
    f: u64,
    lo: u64,
    hi: Option<u64>,
    after: [Option<(u64, u32, u16, u64)>; 2],
    side: usize,
    /// Entries seen per side, as `(born, page, unit, died)`.
    seen: [Vec<(u64, u32, u16, u64)>; 2],
    /// The side that ended first; its entries are the candidates, examined from `next_free` on.
    finished: Option<usize>,
    next_free: usize,
}

impl GcTask {
    fn new(f: u64, lo: u64, hi: Option<u64>) -> Self {
        Self {
            f,
            lo,
            hi,
            after: [None, None],
            side: 0,
            seen: Default::default(),
            finished: None,
            next_free: 0,
        }
    }

    /// Advance by at most `*budget` steps (one index entry visited, or one candidate examined).
    /// Returns true once the task is complete.
    fn step(
        &mut self,
        lineage: &mut Lineage<u16>,
        chunks: &mut Arena,
        work: &mut BranchWork,
        budget: &mut usize,
        freed: &mut usize,
    ) -> bool {
        let bound = |e: u64| (e, NO_PAGE, u16::MAX, u64::MAX);
        while self.finished.is_none() {
            if *budget == 0 {
                return false;
            }
            *budget -= 1;
            let side = self.side;
            let next = if side == 0 {
                let from = Bound::Excluded(self.after[0].unwrap_or(bound(self.lo)));
                lineage
                    .by_born
                    .range((from, Bound::Included(bound(self.f))))
                    .next()
                    .copied()
            } else {
                let from = Bound::Excluded(self.after[1].unwrap_or(bound(self.f)));
                let to = self.hi.map_or(Bound::Unbounded, |hi| Bound::Included(bound(hi)));
                lineage.by_died.range((from, to)).next().copied()
            };
            match next {
                Some(key) => {
                    work.gc_range_entries += 1;
                    self.after[side] = Some(key);
                    let (a, page, unit, b) = key;
                    self.seen[side].push(if side == 0 { key } else { (b, page, unit, a) });
                    if self.hi.is_some() {
                        self.side = 1 - side;
                    }
                }
                None => self.finished = Some(side),
            }
        }
        let side = self.finished.expect("the walk has ended");
        while self.next_free < self.seen[side].len() {
            if *budget == 0 {
                return false;
            }
            *budget -= 1;
            let v = self.seen[side][self.next_free];
            self.next_free += 1;
            let (born, _, _, died) = v;
            // Garbage NOW: no live child forked inside it. The query's `{born > lo, died <= hi}` held
            // for the versions that existed when the task was queued, but a version split off since
            // can hold a child forked since (when the reaped child was the newest, `hi` is open), so
            // each candidate is checked against the children as they are.
            if lineage.by_born.contains(&v) && !lineage.has_child_in(born, died) {
                lineage.release(v, chunks, work);
                *freed += 1;
            }
        }
        true
    }
}

#[derive(Clone, Copy)]
struct Pending {
    slot: Slot,
    died: u64,
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage<()>,
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

impl<U: Unit> Lineage<U> {
    /// True if a live child forked in `[from, to)` can see a version current over that range.
    fn has_child_in(&self, from: u64, to: u64) -> bool {
        from < to && self.children.range(from..to).next().is_some()
    }

    fn retain(&mut self, page: u32, unit: U, v: Retained) {
        crate::turso_assert!(page != NO_PAGE, "page number u32::MAX is the index sentinel");
        let versions = self.retained.entry(page).or_default();
        crate::turso_assert!(
            versions
                .range((unit, 0)..=(unit, u64::MAX))
                .next_back()
                .is_none_or(|(_, last)| last.died <= v.born),
            "a retained version overlaps an older one of the same page; the born-ordered lookup \
             would return the wrong one"
        );
        versions.insert((unit, v.born), v);
        self.by_born.insert((v.born, page, unit, v.died));
        self.by_died.insert((v.died, page, unit, v.born));
        self.versions += 1;
    }

    /// Detach the child forked at `f` and release every retained version that only it could see.
    fn child_gone(&mut self, f: u64, arena: &mut Arena, work: &mut BranchWork) -> usize {
        let (lo, hi) = self.detach(f);
        let dead = self.garbage(f, lo, hi, work);
        for &v in &dead {
            self.release(v, arena, work);
        }
        dead.len()
    }

    /// Remove the child forked at `f` from `children`, returning its former neighbours.
    fn detach(&mut self, f: u64) -> (Option<u64>, Option<u64>) {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        (lo, hi)
    }

    /// Release one indexed version, `(born, page, unit, died)`: out of the fat node and both
    /// indexes, and its slot back to `arena`.
    fn release(&mut self, (born, page, unit, died): (u64, u32, U, u64), arena: &mut Arena, work: &mut BranchWork) {
        let versions = self.retained.get_mut(&page).expect("indexed version is listed");
        let v = versions
            .remove(&(unit, born))
            .expect("indexed version is listed");
        work.gc_examined += 1;
        if versions.is_empty() {
            self.retained.remove(&page);
        }
        let indexed = self.by_born.remove(&(born, page, unit, died))
            && self.by_died.remove(&(died, page, unit, born));
        crate::turso_assert!(indexed, "a released version was missing from an index");
        self.versions -= 1;
        arena.release(v.slot);
    }

    /// The versions that held `f` and no other live child, as `(born, page, unit, died)`, once `f`
    /// has left `children`; `lo` and `hi` are its former neighbours there.
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
    ) -> Vec<(u64, u32, U, u64)> {
        // `born` in (lo, f] and `died` in (f, hi], as bounds on the two indexes' first field.
        let after = |e: u64| (e, NO_PAGE, U::LAST, u64::MAX);
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
            .map(|&(died, page, unit, born)| (born, page, unit, died));
        let mut seen: [Vec<(u64, u32, U, u64)>; 2] = Default::default();
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
        dead.retain(|&(born, _, _, died)| only_f(born, died));
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

/// FS9's process default: `TURSO_R11S_FS9=1` turns it on for stores created afterwards.
fn fs9_from_env() -> bool {
    std::env::var("TURSO_R11S_FS9").is_ok_and(|v| v == "1" || v == "2")
}

/// FS9B's process default: `TURSO_R11S_FS9=2`.
fn fs9b_from_env() -> bool {
    std::env::var("TURSO_R11S_FS9").is_ok_and(|v| v == "2")
}

/// Mutant schemata for this lane's fire-checks, compiled into test builds only and chosen per test
/// run by `TURSO_R11S_MUTANT` (one name), so one compile serves every mutant.
#[cfg(test)]
pub(crate) mod mutants {
    pub(crate) fn on(name: &str) -> bool {
        std::env::var("TURSO_R11S_MUTANT").is_ok_and(|v| v == name)
    }
}

impl TrunkState {
    fn new() -> Self {
        Self {
            lineage: Lineage::default(),
            written: HashMap::new(),
            pending: HashMap::new(),
            pending_by_died: BTreeSet::new(),
            chunk_born: HashMap::new(),
            chunks: None,
            chunk_size: 0,
            gc_budget: None,
            gc_tasks: VecDeque::new(),
        }
    }

    /// Advance the queued garbage queries by at most `budget` steps in all. Returns the chunk
    /// versions released.
    fn run_tasks(&mut self, work: &mut BranchWork, mut budget: usize) -> usize {
        let TrunkState {
            lineage,
            chunks,
            gc_tasks,
            ..
        } = self;
        let Some(chunks) = chunks.as_mut() else {
            return 0;
        };
        let mut freed = 0;
        while budget > 0 {
            let Some(task) = gc_tasks.front_mut() else {
                break;
            };
            if task.step(lineage, chunks, work, &mut budget, &mut freed) {
                gc_tasks.pop_front();
            }
        }
        freed
    }

    /// `page`'s pending image `p` was the pre-image of the trunk write in epoch `p.died`, and
    /// `post` is the page after it: keep, as chunk versions, the chunks that write changed and a
    /// live child can still see.
    fn split_pending(
        &mut self,
        page: u32,
        p: Pending,
        post: &[u8],
        arena: &Arena,
        work: &mut BranchWork,
    ) {
        let TrunkState {
            lineage,
            chunk_born,
            chunks,
            chunk_size,
            ..
        } = self;
        let cs = *chunk_size;
        let chunks = chunks
            .as_mut()
            .expect("the page arena exists, so the chunk arena does");
        let borns = chunk_born
            .entry(page)
            .or_insert_with(|| vec![0; post.len() / cs].into_boxed_slice());
        let old = arena.page(p.slot);
        for (c, (was, now)) in old.chunks(cs).zip(post.chunks(cs)).enumerate() {
            if was == now {
                continue;
            }
            work.trunk_chunks_changed += 1;
            let born = borns[c];
            if lineage.has_child_in(born, p.died) {
                let slot = chunks.alloc();
                chunks.page_mut(slot).copy_from_slice(was);
                lineage.retain(
                    page,
                    c as u16,
                    Retained {
                        born,
                        died: p.died,
                        slot,
                    },
                );
                work.trunk_chunks_retained += 1;
            }
            borns[c] = p.died;
        }
    }

    /// The horizon sweep: release, in `died` order, up to `budget` chunk versions and up to `budget`
    /// pending images that no live child is older than (with no live child, all of them). Returns
    /// the pages and the chunks released.
    fn sweep(&mut self, arena: &mut Arena, work: &mut BranchWork, budget: usize) -> (usize, usize) {
        let oldest = self.lineage.children.keys().next().copied();
        // Nothing a live child can see died at or before the oldest live fork.
        let below = |died: u64| oldest.is_none_or(|f| died <= f);
        let TrunkState {
            lineage,
            pending,
            pending_by_died,
            chunks,
            ..
        } = self;
        let mut freed_chunks = 0;
        if let Some(chunks) = chunks.as_mut() {
            while freed_chunks < budget {
                let Some(&(died, page, unit, born)) = lineage.by_died.first() else {
                    break;
                };
                if !below(died) {
                    break;
                }
                work.gc_range_entries += 1;
                lineage.release((born, page, unit, died), chunks, work);
                freed_chunks += 1;
            }
        }
        let mut freed_pages = 0;
        while freed_pages < budget {
            let Some(&(died, page)) = pending_by_died.first() else {
                break;
            };
            if !below(died) {
                break;
            }
            pending_by_died.pop_first();
            let p = pending
                .remove(&page)
                .expect("an indexed pending image is listed");
            arena.release(p.slot);
            freed_pages += 1;
        }
        (freed_pages, freed_chunks)
    }
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

/// The chunk size `TURSO_BRANCH_CHUNK_BYTES` asks for, or the default. Refuses a size that is not
/// a power of two in 16..=65536: a chunk must tile every page size SQLite allows.
fn configured_chunk_size() -> usize {
    match std::env::var("TURSO_BRANCH_CHUNK_BYTES") {
        Err(_) => DEFAULT_CHUNK_BYTES,
        Ok(v) => {
            let n: usize = v
                .parse()
                .unwrap_or_else(|_| panic!("TURSO_BRANCH_CHUNK_BYTES={v} is not a number"));
            assert!(
                n.is_power_of_two() && (16..=65536).contains(&n),
                "TURSO_BRANCH_CHUNK_BYTES={n} must be a power of two in 16..=65536"
            );
            n
        }
    }
}

/// The trunk's reclamation steps per store call: `TURSO_BRANCH_GC_BUDGET` (a positive number, or
/// `eager` for none), else the default.
fn configured_gc_budget() -> Option<usize> {
    match std::env::var("TURSO_BRANCH_GC_BUDGET") {
        Err(_) => Some(DEFAULT_GC_BUDGET),
        Ok(v) if v == "eager" => None,
        Ok(v) => {
            let n: usize = v
                .parse()
                .unwrap_or_else(|_| panic!("TURSO_BRANCH_GC_BUDGET={v} is not a number or `eager`"));
            assert!(n > 0, "TURSO_BRANCH_GC_BUDGET must be positive");
            Some(n)
        }
    }
}

impl BranchStore {
    pub(crate) fn new() -> Self {
        Self::with_config(configured_chunk_size(), configured_gc_budget())
    }

    /// A store whose trunk keeps `chunk_size`-byte chunks (a power of two of at least 16 bytes;
    /// pages smaller than it are kept whole) and reclaims eagerly.
    #[cfg(test)]
    pub(crate) fn with_chunk_size(chunk_size: usize) -> Self {
        Self::with_config(chunk_size, None)
    }

    /// As [`Self::with_chunk_size`], reclaiming at most about `2 * gc_budget` steps per store call
    /// (`None`: eagerly).
    pub(crate) fn with_config(chunk_size: usize, gc_budget: Option<usize>) -> Self {
        assert!(
            chunk_size.is_power_of_two() && chunk_size >= 16,
            "chunk size {chunk_size} must be a power of two of at least 16"
        );
        assert!(gc_budget != Some(0), "a reclamation budget must be positive");
        let mut trunk = TrunkState::new();
        trunk.chunk_size = chunk_size;
        trunk.gc_budget = gc_budget;
        Self {
            inner: Mutex::new(StoreInner {
                arena: None,
                next_id: 1,
                trunk,
                branches: HashMap::new(),
                work: BranchWork::default(),
                retained_clones: HashMap::new(),
                clones_by_hi: BTreeSet::new(),
            }),
            trunk_children: AtomicUsize::new(0),
            lock_timing: AtomicBool::new(false),
            trunk_pages: TrunkPages {
                generation: AtomicU64::new(0),
                pages: Radix::new(),
            },
            trunk_format: OnceLock::new(),
            fw3: std::sync::atomic::AtomicBool::new(super::walpin::fw3()),
            fs9: AtomicBool::new(fs9_from_env()),
            fs10: AtomicBool::new(std::env::var("TURSO_R11S_FS10").is_ok_and(|v| v == "1")),
            fs9b: AtomicBool::new(fs9b_from_env()),
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

    pub(crate) fn set_fs9(&self, on: bool) {
        self.fs9.store(on, Ordering::Relaxed);
    }

    pub(crate) fn set_fs9b(&self, on: bool) {
        self.fs9b.store(on, Ordering::Relaxed);
    }

    pub(crate) fn slot_clone_count(&self) -> usize {
        self.lock().arena.as_ref().map_or(0, |a| a.clone_count())
    }

    pub(crate) fn set_fs10(&self, on: bool) {
        self.fs10.store(on, Ordering::Relaxed);
    }

    pub(crate) fn fs10(&self) -> bool {
        self.fs10.load(Ordering::Relaxed)
    }

    /// Trunk pages F6's shared cache holds a version of: the shared pool's size, in pages.
    /// Observation only (a walk of the installed radix leaves).
    pub(crate) fn trunk_cache_pages(&self) -> usize {
        self.trunk_pages.pages.count(|slot| slot.load().is_some())
    }

    pub(crate) fn retained_clone_count(&self) -> usize {
        self.lock().retained_clones.len()
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
            None => {
                inner.arena = Some(Arena::new(page_size));
                let trunk = &mut inner.trunk;
                trunk.chunk_size = trunk.chunk_size.min(page_size);
                trunk.chunks = Some(Arena::new(trunk.chunk_size));
            }
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
                freed_chunks: 0,
                deferred: false,
            };
        };
        st.handle = false;
        let (freed_pages, freed_chunks) = self.collect(&mut inner, id);
        Reaped {
            freed_pages,
            freed_chunks,
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
                        (),
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

    /// The copy decision for the trunk's first write to `page` in a transaction: split the page's
    /// previous pending image into the chunks its write changed (`pre_image` is that write's
    /// post-image), then keep `pre_image` as the new pending image while a live child is older
    /// than this write.
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
        let arena = arena.as_mut().expect("the trunk has a child, so the arena exists");
        if let Some(p) = trunk.pending.remove(&page) {
            let indexed = trunk.pending_by_died.remove(&(p.died, page));
            crate::turso_assert!(indexed, "a pending image was missing from its index");
            trunk.split_pending(page, p, pre_image, arena, work);
            arena.release(p.slot);
        }
        // Every live child was forked before `epoch`, so any child at all needs this pre-image.
        if !trunk.lineage.children.is_empty() {
            let slot = arena.alloc();
            arena.page_mut(slot).copy_from_slice(pre_image);
            trunk.pending.insert(page, Pending { slot, died: epoch });
            trunk.pending_by_died.insert((epoch, page));
        }
        trunk.written.insert(page, epoch);
        if let Some(budget) = trunk.gc_budget {
            trunk.run_tasks(work, budget);
            trunk.sweep(arena, work, budget);
        }
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
        let mut levels = 0;
        let resolved = inner.resolve_origin(id, page, &mut levels);
        inner.work.resolve_calls += 1;
        inner.work.resolve_levels += levels;
        let slot = match resolved? {
            // The branch's own current page stays a private copy: it is the page the branch writes.
            Origin::Arena(slot) => slot,
            Origin::Inherited(slot) => {
                if share && self.fs9b.load(Ordering::Relaxed) {
                    let StoreInner { arena, work, .. } = &mut *inner;
                    let arena = arena.as_mut().expect("a slot resolved, so the arena exists");
                    let (bytes, filled) = arena.shared_clone(slot);
                    if filled {
                        work.inherited_clone_fills += 1;
                    } else {
                        work.inherited_shared_hits += 1;
                    }
                    return Ok(Resolved::Shared(bytes));
                }
                inner.work.inherited_copies += 1;
                slot
            }
            Origin::TrunkPast { page, base, at } => {
                let StoreInner {
                    arena,
                    trunk,
                    work,
                    retained_clones,
                    clones_by_hi,
                    ..
                } = &mut *inner;
                let arena = arena
                    .as_ref()
                    .expect("a pending image resolved, so the arena exists");
                if share && self.fs9.load(Ordering::Relaxed) {
                    let mut examined = 0;
                    let (lo, hi) = trunk.past_interval(page, at, &mut examined);
                    work.resolve_retained_examined += examined;
                    // U11's red: a key naming the pending image alone, not the reader's interval.
                    #[cfg(test)]
                    let (lo, hi) = if mutants::on("FS9_KEY_NO_BORN") {
                        (0, trunk.pending[&page].died)
                    } else {
                        (lo, hi)
                    };
                    let bytes = match retained_clones.get(&(page, lo, hi)) {
                        Some(bytes) => {
                            work.retained_shared_hits += 1;
                            bytes.clone()
                        }
                        None => {
                            let mut image = arena.page(base).to_vec();
                            // The interval walk above already counted these versions.
                            let mut again = 0;
                            work.resolve_chunks_overlaid +=
                                trunk.overlay(page, at, &mut image, &mut again);
                            let bytes = Arc::new(image.into_boxed_slice());
                            retained_clones.insert((page, lo, hi), bytes.clone());
                            clones_by_hi.insert((hi, page, lo));
                            work.retained_clone_fills += 1;
                            bytes
                        }
                    };
                    return Ok(Resolved::Shared(bytes));
                }
                out.copy_from_slice(arena.page(base));
                let mut examined = 0;
                work.resolve_chunks_overlaid += trunk.overlay(page, at, out, &mut examined);
                work.resolve_retained_examined += examined;
                work.retained_copies += 1;
                return Ok(Resolved::Filled);
            }
            Origin::Trunk => {
            // `resolve_origin` answered "the trunk's current version", so the trunk's last write to
            // this page came at or before the branch's `trunk_at`, in a closed epoch.
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
            }
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
        let mut levels = 0;
        Ok(matches!(
            inner.resolve_origin(id, page, &mut levels)?,
            Origin::Trunk
        ))
    }

    pub(crate) fn stats(&self) -> BranchStats {
        let inner = self.lock();
        let trunk = &inner.trunk;
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
            trunk_versions: trunk.lineage.versions,
            trunk_version_bytes: trunk.lineage.versions * trunk.chunk_size,
            trunk_pending_pages: trunk.pending.len(),
            chunk_slots_in_use: trunk.chunks.as_ref().map_or(0, |a| a.in_use()),
            chunk_size: trunk.chunk_size,
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

    /// Run every queued garbage query and the sweep to the end, for exactness assertions.
    #[cfg(test)]
    fn drain_reclamation(&self) {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena, trunk, work, ..
        } = &mut *inner;
        let Some(arena) = arena.as_mut() else {
            return;
        };
        trunk.run_tasks(work, usize::MAX);
        trunk.sweep(arena, work, usize::MAX);
    }

    /// FS9's clone keys, and the size of their horizon index.
    #[cfg(test)]
    fn clone_keys(&self) -> (Vec<(u32, u64, u64)>, usize) {
        let inner = self.inner.lock();
        let mut keys: Vec<_> = inner.retained_clones.keys().copied().collect();
        keys.sort_unstable();
        (keys, inner.clones_by_hi.len())
    }

    /// The trunk's retained chunk versions as `(page, chunk, died)`, for membership assertions.
    #[cfg(test)]
    fn trunk_version_keys(&self) -> std::collections::BTreeSet<(u32, u16, u64)> {
        self.inner
            .lock()
            .trunk
            .lineage
            .by_died
            .iter()
            .map(|&(died, page, unit, _)| (page, unit, died))
            .collect()
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self
            .lock()
            .arena
            .as_ref()
            .is_some_and(|a| a.is_free(slot))
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist. Returns the arena pages released (the branch's own, its parent's versions
    /// only it could see, and trunk pending images no live child needs any more) and the trunk
    /// chunk versions released.
    fn collect(&self, inner: &mut StoreInner, mut id: BranchId) -> (usize, usize) {
        let mut freed = 0;
        loop {
            let Some(st) = inner.branches.get(&id) else {
                return (freed, 0);
            };
            if st.handle || st.open || !st.lineage.children.is_empty() {
                return (freed, 0);
            }
            let st = inner.branches.remove(&id).expect("just looked it up");
            let StoreInner {
                arena,
                trunk,
                branches,
                work,
                retained_clones,
                clones_by_hi,
                ..
            } = &mut *inner;
            let arena = arena.as_mut().expect("a branch existed, so the arena does");
            for owned in st.current.values() {
                arena.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(arena).len();
            if st.parent.is_trunk() {
                let chunks = trunk
                    .chunks
                    .as_mut()
                    .expect("the page arena exists, so the chunk arena does");
                let freed_chunks = match trunk.gc_budget {
                    None => {
                        let walked = trunk.lineage.child_gone(st.fork_epoch, chunks, work);
                        let (pages, swept) = trunk.sweep(arena, work, usize::MAX);
                        freed += pages;
                        walked + swept
                    }
                    Some(budget) => {
                        let f = st.fork_epoch;
                        let (lo, hi) = trunk.lineage.detach(f);
                        // The oldest child's garbage lies below the new horizon: the sweep's.
                        if let Some(lo) = lo {
                            trunk.gc_tasks.push_back(GcTask::new(f, lo, hi));
                        }
                        if trunk.lineage.children.is_empty() {
                            // Nothing can be read any more, so everything goes now.
                            trunk.gc_tasks.clear();
                            let (pages, swept) = trunk.sweep(arena, work, usize::MAX);
                            freed += pages;
                            swept
                        } else {
                            let walked = trunk.run_tasks(work, budget);
                            let (pages, swept) = trunk.sweep(arena, work, budget);
                            freed += pages;
                            walked + swept
                        }
                    }
                };
                // FS9's clones. A live child reads a clone only if its fork lies in the clone's
                // [lo, hi), and every later fork is at or past `hi`, so a clone whose `hi` is at or
                // below the oldest live fork can never be read again (U11's horizon eviction).
                let oldest = trunk.lineage.children.keys().next().copied();
                #[cfg(test)]
                let evict = !mutants::on("FS9_NO_EVICT");
                #[cfg(not(test))]
                let evict = true;
                while evict {
                    let Some(&(hi, page, lo)) = clones_by_hi.first() else {
                        break;
                    };
                    if oldest.is_some_and(|f| f < hi) {
                        break;
                    }
                    clones_by_hi.pop_first();
                    retained_clones.remove(&(page, lo, hi));
                    work.retained_clone_evictions += 1;
                }
                if trunk.lineage.children.is_empty() {
                    // No child left: every version is gone, and so is any reason to remember when
                    // a page or a chunk last changed (an absent epoch is the conservative 0).
                    trunk.written.clear();
                    trunk.chunk_born.clear();
                    retained_clones.clear();
                    clones_by_hi.clear();
                    // From here the trunk writes without telling the store, so no cached version
                    // can be trusted once a branch exists again. No branch can read in between:
                    // a fork needs this lock.
                    self.trunk_pages.generation.fetch_add(1, Ordering::AcqRel);
                }
                self.trunk_children.fetch_sub(1, Ordering::AcqRel);
                return (freed, freed_chunks);
            }
            let parent = branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            freed += parent.lineage.child_gone(st.fork_epoch, arena, work);
            id = st.parent;
        }
    }
}

impl TrunkState {
    /// Lay over `out` — `page`'s pending image — every chunk version of `page` that contains `at`.
    /// A chunk with versions is found by one range step and its version by one predecessor search;
    /// `examined` counts the versions compared against `at`, at most one per chunk. Returns the
    /// number of chunks overlaid.
    fn overlay(&self, page: u32, at: u64, out: &mut [u8], examined: &mut u64) -> u64 {
        let Some(versions) = self.lineage.retained.get(&page) else {
            return 0;
        };
        let chunks = self
            .chunks
            .as_ref()
            .expect("a chunk version exists, so the chunk arena does");
        let cs = self.chunk_size;
        let mut overlaid = 0;
        let mut next: Option<u16> = Some(0);
        while let Some(from) = next {
            let Some((&(c, _), _)) = versions.range((from, 0)..).next() else {
                break;
            };
            if let Some((_, v)) = versions.range((c, 0)..=(c, at)).next_back() {
                *examined += 1;
                if at < v.died {
                    let off = c as usize * cs;
                    out[off..off + cs].copy_from_slice(chunks.page(v.slot));
                    overlaid += 1;
                }
            }
            next = c.checked_add(1);
        }
        overlaid
    }

    /// U11's key for FS9's clone of `page` as a child forked at `at` reads it (the pending image with
    /// every chunk version containing `at` laid over it): the epochs `[lo, hi)`, where `hi` is the
    /// pending image's `died` lowered to the `died` of each containing version, and `lo` is the
    /// greatest `born` of a containing version or `died` of an older one. One predecessor search
    /// per chunk that has versions, as in `overlay`; `examined` counts the versions compared.
    ///
    /// Two LIVE children whose reads compute the same key read the same bytes. Say `f < f'` both
    /// lie in `[lo, hi)` and some chunk changed at an epoch `e` with `f < e <= f'`. When the older
    /// one, `f`, computed its key, either that change had been split into a version — then the
    /// chunk's version holding `f`, which ends at the first change after `f`, was retained because
    /// `f` was live then and is still retained because `f` still is, so `hi <= e <= f'` — or it had
    /// not, and then `e` is at or past the pending image's `died`, so again `hi <= e`. Either way
    /// `f'` is not in `[lo, hi)`, whichever computed its key first. `lo` adds nothing to that
    /// argument; it only keeps the clones of different intervals apart. A fork made after the key
    /// is at or past the page's latest write, so at or past `hi`: once the oldest live fork reaches
    /// `hi`, no child can compute the key again.
    fn past_interval(&self, page: u32, at: u64, examined: &mut u64) -> (u64, u64) {
        let died = self
            .pending
            .get(&page)
            .expect("the page resolved to its pending image")
            .died;
        let (mut lo, mut hi) = (0, died);
        let Some(versions) = self.lineage.retained.get(&page) else {
            return (lo, hi);
        };
        let mut next: Option<u16> = Some(0);
        while let Some(from) = next {
            let Some((&(c, _), _)) = versions.range((from, 0)..).next() else {
                break;
            };
            if let Some((_, v)) = versions.range((c, 0)..=(c, at)).next_back() {
                *examined += 1;
                if at < v.died {
                    lo = lo.max(v.born);
                    hi = hi.min(v.died);
                } else {
                    lo = lo.max(v.died);
                }
            }
            next = c.checked_add(1);
        }
        (lo, hi)
    }
}

impl StoreInner {
    /// `levels` counts the nodes consulted — the branch (its own pages and its `inherited` map),
    /// then the trunk if neither holds the page.
    fn resolve_origin(&self, id: BranchId, page: u32, levels: &mut u64) -> Result<Origin> {
        *levels += 1;
        let st = self.branches.get(&id).ok_or_else(|| gone(id))?;
        // A branch sees all of its own versions; its ancestors' as of its fork, which `inherited`
        // froze then.
        if let Some(owned) = st.current.get(&page) {
            return Ok(Origin::Arena(owned.slot));
        }
        if let Some(slot) = st.inherited.get(page) {
            return Ok(Origin::Inherited(slot));
        }
        *levels += 1;
        let at = st.trunk_at;
        let written = self.trunk.written.get(&page).copied().unwrap_or(0);
        if written <= at {
            return Ok(Origin::Trunk);
        }
        // The trunk has written this page since the fork, so the branch reads its pending image
        // with the chunks changed in between laid back over it.
        let Some(p) = self.trunk.pending.get(&page) else {
            // The ordinary read path would return the NEW version. Refuse rather than serve it.
            return Err(LimboError::Corrupt(format!(
                "branch {} would read trunk page {page} written after its fork; the pre-image was \
                 not retained",
                id.0
            )));
        };
        crate::turso_assert!(
            p.died == written,
            "a pending image is not the pre-image of its page's latest trunk write"
        );
        Ok(Origin::TrunkPast {
            page,
            base: p.slot,
            at,
        })
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
    /// The chunk size every test store uses: four chunks per test page.
    const CHUNK: usize = 16;
    const CPP: usize = PAGE / CHUNK;

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

    /// A page whose chunk `c` holds generation `gens[c]`.
    fn chunked(gens: &[u64; CPP]) -> Vec<u8> {
        gens.iter()
            .flat_map(|g| g.to_le_bytes().repeat(CHUNK / 8))
            .collect()
    }

    /// The trunk's sub-page versions against a model of the rule in the module doc, through the
    /// store's own entry points (`fork_trunk`, `first_write_trunk`, `release_handle`,
    /// `resolve_into`).
    ///
    /// Each trunk write rewrites a random subset of a page's chunks — sometimes none, sometimes
    /// all — so a split keeps some chunks and not others; children are reaped oldest-first,
    /// newest-first and at random, so the garbage query runs with no older sibling, with no
    /// younger one, and with both. After every step:
    ///
    /// * every live child reads, for every page, the trunk's page as of its fork, byte for byte —
    ///   from the pending image with its chunk versions laid over it or, when there is none, from
    ///   the trunk's current page;
    /// * the chunk arena holds exactly the chunk versions whose `[born, died)` still contains a live
    ///   child, and the page arena exactly the pending images a live child is older than;
    /// * a reap visits exactly the index entries the garbage query's contract allows (as in the
    ///   page-granular store: what it frees when one-sided, 2·|B| or 2·|D| + 1 when two-sided).
    #[test]
    fn chunk_versions_match_a_model_under_partial_rewrites_forks_and_reaps_in_every_order() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_chunks(seed);
        }
    }

    fn run_chunks(seed: u64) {
        let store = BranchStore::with_chunk_size(CHUNK);
        let mut rng = Rng(seed);
        // The trunk's current generation of each chunk, and each live child's view at its fork.
        let mut current: HashMap<u32, [u64; CPP]> = (0..PAGES).map(|p| (p, [0; CPP])).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, [u64; CPP]>)> = Vec::new();
        // The rule, kept by the model: each page's latest write epoch, its pending pre-image, the
        // epoch of each chunk's last change a split recorded, and every chunk version retained as
        // (page, chunk, born, died).
        let mut written: HashMap<u32, u64> = HashMap::new();
        let mut pending: HashMap<u32, ([u64; CPP], u64)> = HashMap::new();
        let mut chunk_born: HashMap<(u32, usize), u64> = HashMap::new();
        let mut history: Vec<(u32, usize, u64, u64)> = Vec::new();
        let mut epoch = 0u64;
        let mut generation = 0u64;
        let (mut freed_oldest, mut freed_newest, mut freed_middle) = (0, 0, 0);
        let (mut partial_splits, mut empty_splits) = (0, 0);
        for step in 0..2000 {
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
                                if let Some((pre, died)) = pending.remove(&page) {
                                    let now = current[&page];
                                    let changed = (0..CPP).filter(|&c| pre[c] != now[c]).count();
                                    match changed {
                                        0 => empty_splits += 1,
                                        CPP => {}
                                        _ => partial_splits += 1,
                                    }
                                    for c in (0..CPP).filter(|&c| pre[c] != now[c]) {
                                        let b = chunk_born.get(&(page, c)).copied().unwrap_or(0);
                                        if live.iter().any(|&(_, f, _)| b <= f && f < died) {
                                            history.push((page, c, b, died));
                                        }
                                        chunk_born.insert((page, c), died);
                                    }
                                }
                                if !live.is_empty() {
                                    pending.insert(page, (current[&page], epoch));
                                }
                                written.insert(page, epoch);
                            }
                            store.first_write_trunk(page, &chunked(&current[&page]));
                        }
                        // The write: each chunk rewritten with probability 1/2.
                        let mut gens = current[&page];
                        for g in gens.iter_mut() {
                            if rng.below(2) == 0 {
                                generation += 1;
                                *g = generation;
                            }
                        }
                        current.insert(page, gens);
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
                        .filter(|&&(_, _, born, _)| lo.is_none_or(|lo| born > lo) && born <= f)
                        .count() as u64;
                    let d = history
                        .iter()
                        .filter(|&&(_, _, _, died)| f < died && hi.is_none_or(|hi| died <= hi))
                        .count() as u64;
                    let oldest = live.first().map(|c| c.1);
                    let stale = pending
                        .values()
                        .filter(|&&(_, died)| oldest.is_none_or(|o| o >= died))
                        .count();
                    pending.retain(|_, &mut (_, died)| oldest.is_some_and(|o| o < died));
                    let before = store.stats();
                    let reaped = store.release_handle(id);
                    let after = store.stats();
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}");
                    assert_eq!(
                        reaped.freed_pages, stale,
                        "seed {seed:#x} step {step}: the reap released {} pending images, the \
                         model {stale}",
                        reaped.freed_pages
                    );
                    assert_eq!(
                        before.chunk_slots_in_use - after.chunk_slots_in_use,
                        reaped.freed_chunks,
                        "seed {seed:#x} step {step}: the reap's report disagrees with the chunk arena"
                    );
                    let visited = after.work.gc_range_entries - before.work.gc_range_entries;
                    let contract = match (lo, hi) {
                        (None, _) | (_, None) => reaped.freed_chunks as u64,
                        _ if b <= d => 2 * b,
                        _ => 2 * d + 1,
                    };
                    assert_eq!(
                        visited, contract,
                        "seed {seed:#x} step {step}: reaping the child forked at {f} (lo {lo:?}, \
                         hi {hi:?}, |B| {b}, |D| {d}) visited {visited} index entries"
                    );
                    if reaped.freed_chunks > 0 {
                        match at {
                            0 => freed_oldest += 1,
                            _ if at == live.len() => freed_newest += 1,
                            _ => freed_middle += 1,
                        }
                    }
                }
                _ => {}
            }
            let alive: HashSet<(u32, usize, u64, u64)> = history
                .iter()
                .copied()
                .filter(|&(_, _, born, died)| live.iter().any(|&(_, f, _)| born <= f && f < died))
                .collect();
            let s = store.stats();
            assert_eq!(
                (s.chunk_slots_in_use, s.trunk_versions),
                (alive.len(), alive.len()),
                "seed {seed:#x} step {step}: the store holds a chunk version no live child can see, \
                 or lost one a live child can"
            );
            assert_eq!(
                (s.arena_slots_in_use, s.trunk_pending_pages),
                (pending.len(), pending.len()),
                "seed {seed:#x} step {step}: pending images disagree with the model"
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
                    let want = chunked(&view[&page]);
                    let got = if in_arena {
                        buf.clone()
                    } else {
                        chunked(&current[&page])
                    };
                    assert_eq!(
                        got, want,
                        "seed {seed:#x} step {step}: child forked at {f} read the wrong page {page}"
                    );
                }
            }
        }
        // Every shape must have occurred, or a green run says nothing about the shape it skipped:
        // the three garbage-query shapes, splits that keep some chunks and not others, splits of a
        // write that changed nothing, and reads that both lay a chunk over the pending image and
        // pass over one that does not contain the reader.
        let w = store.stats().work;
        assert!(
            freed_oldest > 0 && freed_newest > 0 && freed_middle > 0,
            "seed {seed:#x}: reaps that freed versions: oldest {freed_oldest}, newest \
             {freed_newest}, middle {freed_middle}"
        );
        assert!(
            partial_splits > 0 && empty_splits > 0,
            "seed {seed:#x}: partial splits {partial_splits}, empty splits {empty_splits}"
        );
        assert!(
            w.resolve_chunks_overlaid > 0 && w.resolve_retained_examined > w.resolve_chunks_overlaid,
            "seed {seed:#x}: chunks overlaid {}, examined {}",
            w.resolve_chunks_overlaid,
            w.resolve_retained_examined
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        let s = store.stats();
        assert_eq!(
            (s.arena_slots_in_use, s.chunk_slots_in_use, s.trunk_versions),
            (0, 0, 0),
            "seed {seed:#x}: versions leaked"
        );
    }

    /// Bounded reclamation (module doc, "Bounded reclamation at the trunk") against the same model
    /// as the test above, with a budget of 2 steps. After every step:
    ///
    /// * every live child reads every page byte for byte (a version still waiting for its task or
    ///   the sweep must never be laid over a read, and a version a child needs must never be gone);
    /// * the store holds every chunk version a live child can see, and only versions that were once
    ///   retained;
    /// * a reap and a trunk write each visit at most `2 * BUDGET` index entries (tasks, then the
    ///   sweep) — except the reap of the last child, which frees everything, since nothing can be
    ///   read any more.
    ///
    /// Every 100 steps the queued work is drained, and the store must then hold EXACTLY the versions
    /// a live child can see: a task that never finishes, or garbage nothing will ever free, fails
    /// here. The run must have deferred garbage and capped work at least once, or it says nothing
    /// about the bounded path; and at the end the store must be empty.
    #[test]
    fn bounded_reclamation_never_frees_a_visible_version_and_catches_up() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            run_bounded(seed);
        }
    }

    fn run_bounded(seed: u64) {
        const BUDGET: usize = 2;
        let store = BranchStore::with_config(CHUNK, Some(BUDGET));
        let mut rng = Rng(seed);
        let mut current: HashMap<u32, [u64; CPP]> = (0..PAGES).map(|p| (p, [0; CPP])).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, [u64; CPP]>)> = Vec::new();
        // The eager rule, as in `run_chunks`; `ever` is every chunk version it ever retained.
        let mut written: HashMap<u32, u64> = HashMap::new();
        let mut pending: HashMap<u32, ([u64; CPP], u64)> = HashMap::new();
        let mut chunk_born: HashMap<(u32, usize), u64> = HashMap::new();
        let mut history: Vec<(u32, usize, u64, u64)> = Vec::new();
        let mut ever: HashSet<(u32, u16, u64)> = HashSet::new();
        let mut epoch = 0u64;
        let mut generation = 0u64;
        let (mut deferred, mut capped) = (0, 0);
        for step in 0..2000 {
            match rng.below(10) {
                0..=2 if live.len() < 40 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    live.push((id, epoch, current.clone()));
                    epoch += 1;
                }
                0..=5 => {
                    for _ in 0..=rng.below(3) {
                        let page = rng.below(PAGES as u64) as u32;
                        if store.trunk_has_children() {
                            let born = written.get(&page).copied().unwrap_or(0);
                            if born < epoch {
                                if let Some((pre, died)) = pending.remove(&page) {
                                    let now = current[&page];
                                    for c in (0..CPP).filter(|&c| pre[c] != now[c]) {
                                        let b = chunk_born.get(&(page, c)).copied().unwrap_or(0);
                                        if live.iter().any(|&(_, f, _)| b <= f && f < died) {
                                            history.push((page, c, b, died));
                                            ever.insert((page, c as u16, died));
                                        }
                                        chunk_born.insert((page, c), died);
                                    }
                                }
                                if !live.is_empty() {
                                    pending.insert(page, (current[&page], epoch));
                                }
                                written.insert(page, epoch);
                            }
                            let before = store.stats().work.gc_range_entries;
                            store.first_write_trunk(page, &chunked(&current[&page]));
                            let steps = store.stats().work.gc_range_entries - before;
                            assert!(
                                steps <= 2 * BUDGET as u64,
                                "seed {seed:#x} step {step}: a trunk write took {steps} reclamation steps"
                            );
                            capped += usize::from(steps >= BUDGET as u64);
                        }
                        let mut gens = current[&page];
                        for g in gens.iter_mut() {
                            if rng.below(2) == 0 {
                                generation += 1;
                                *g = generation;
                            }
                        }
                        current.insert(page, gens);
                    }
                }
                _ if !live.is_empty() => {
                    let at = match rng.below(3) {
                        0 => 0,
                        1 => live.len() - 1,
                        _ => rng.below(live.len() as u64) as usize,
                    };
                    let (id, _, _) = live.remove(at);
                    let oldest = live.first().map(|c| c.1);
                    pending.retain(|_, &mut (_, died)| oldest.is_some_and(|o| o < died));
                    let before = store.stats().work.gc_range_entries;
                    let reaped = store.release_handle(id);
                    let steps = store.stats().work.gc_range_entries - before;
                    assert!(!reaped.deferred, "seed {seed:#x} step {step}");
                    if !live.is_empty() {
                        assert!(
                            steps <= 2 * BUDGET as u64,
                            "seed {seed:#x} step {step}: a reap took {steps} reclamation steps"
                        );
                        capped += usize::from(steps >= BUDGET as u64);
                    }
                }
                _ => {}
            }
            let alive: HashSet<(u32, u16, u64)> = history
                .iter()
                .filter(|&&(_, _, born, died)| live.iter().any(|&(_, f, _)| born <= f && f < died))
                .map(|&(page, c, _, died)| (page, c as u16, died))
                .collect();
            let held = store.trunk_version_keys();
            for v in &alive {
                assert!(
                    held.contains(v),
                    "seed {seed:#x} step {step}: chunk version {v:?} freed while a live child can see it"
                );
            }
            for v in &held {
                assert!(
                    ever.contains(v),
                    "seed {seed:#x} step {step}: the store holds {v:?}, which was never retained"
                );
            }
            deferred += usize::from(held.len() > alive.len());
            if step % 100 == 99 {
                store.drain_reclamation();
                let held = store.trunk_version_keys();
                let alive: std::collections::BTreeSet<_> = alive.iter().copied().collect();
                assert_eq!(
                    held, alive,
                    "seed {seed:#x} step {step}: after draining, the store must hold exactly the \
                     versions a live child can see"
                );
            }
            let mut buf = vec![0u8; PAGE];
            for (id, f, view) in &live {
                for page in 0..PAGES {
                    let in_arena = matches!(
                        store.resolve_into(*id, page, &mut buf).unwrap(),
                        Resolved::Filled
                    );
                    let got = if in_arena {
                        buf.clone()
                    } else {
                        chunked(&current[&page])
                    };
                    assert_eq!(
                        got,
                        chunked(&view[&page]),
                        "seed {seed:#x} step {step}: child forked at {f} read the wrong page {page}"
                    );
                }
            }
        }
        assert!(
            deferred > 0 && capped > 0,
            "seed {seed:#x}: steps with garbage deferred {deferred}, calls capped {capped}"
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        let s = store.stats();
        assert_eq!(
            (s.arena_slots_in_use, s.chunk_slots_in_use, s.trunk_versions, s.trunk_pending_pages),
            (0, 0, 0, 0),
            "seed {seed:#x}: the sweep did not catch up once the last child went"
        );
    }

    /// The lock accounting, forced to fire and shown not to fire spuriously. One thread alone makes
    /// only uncontended acquisitions, one per call, and with timing off no hold time. A holder that
    /// keeps the lock for 500 ms while a second thread asks for it makes that acquisition contended,
    /// with a non-zero wait, and with timing on its own hold is counted before the waiter can read
    /// the counters. The bounds are loose on purpose: the waiter would have to be descheduled for the
    /// whole 500 ms to miss the hold, and nothing here is timed tighter than "more than zero".
    /// FS9 under the sub-page trunk, keyed by U11's f-interval (`TrunkState::past_interval`): with
    /// clones on, every live child reads every page through `resolve_shared`, the pager's path, and
    /// gets the trunk's page as of its fork, byte for byte, while children fork, the trunk rewrites
    /// random chunks and children are reaped oldest-first, newest-first and at random, eagerly and
    /// with a budget of 2. After every step no clone outlives the horizon: every key's `hi` is past
    /// the oldest live fork, and the key index agrees with the cache. Red mutants: `FS9_KEY_NO_BORN`
    /// keys a clone by the pending image alone (a wrong read), `FS9_NO_EVICT` keeps clones past the
    /// horizon.
    #[test]
    fn fs9_clones_keyed_by_the_fork_interval_serve_every_child_its_own_page() {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            for budget in [None, Some(2)] {
                run_clones(seed, budget);
            }
        }
    }

    fn run_clones(seed: u64, budget: Option<usize>) {
        let store = BranchStore::with_config(CHUNK, budget);
        store.set_fs9(true);
        let mut rng = Rng(seed);
        let mut current: HashMap<u32, [u64; CPP]> = (0..PAGES).map(|p| (p, [0; CPP])).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, [u64; CPP]>)> = Vec::new();
        let (mut epoch, mut generation) = (0u64, 0u64);
        for step in 0..2000 {
            match rng.below(10) {
                0..=2 if live.len() < 40 => {
                    let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
                    live.push((id, epoch, current.clone()));
                    epoch += 1;
                }
                0..=5 => {
                    for _ in 0..=rng.below(3) {
                        let page = rng.below(PAGES as u64) as u32;
                        if store.trunk_has_children() {
                            store.first_write_trunk(page, &chunked(&current[&page]));
                        }
                        let mut gens = current[&page];
                        for g in gens.iter_mut() {
                            if rng.below(2) == 0 {
                                generation += 1;
                                *g = generation;
                            }
                        }
                        current.insert(page, gens);
                    }
                }
                _ if !live.is_empty() => {
                    let at = match rng.below(3) {
                        0 => 0,
                        1 => live.len() - 1,
                        _ => rng.below(live.len() as u64) as usize,
                    };
                    let (id, _, _) = live.remove(at);
                    store.release_handle(id);
                }
                _ => {}
            }
            let (keys, indexed) = store.clone_keys();
            assert_eq!(keys.len(), indexed, "seed {seed:#x} step {step}: the key index disagrees");
            let oldest = live.first().map(|c| c.1);
            for &(page, lo, hi) in &keys {
                assert!(
                    lo < hi && oldest.is_some_and(|o| o < hi),
                    "seed {seed:#x} {budget:?} step {step}: clone ({page}, {lo}, {hi}) outlived \
                     the horizon (oldest live fork {oldest:?})"
                );
            }
            let mut buf = vec![0u8; PAGE];
            for (id, f, view) in &live {
                for page in 0..PAGES {
                    let got = match store.resolve_shared(*id, page, &mut buf).unwrap() {
                        Resolved::Shared(bytes) => bytes.to_vec(),
                        Resolved::Filled => buf.clone(),
                        Resolved::Trunk(_) => chunked(&current[&page]),
                    };
                    assert_eq!(
                        got,
                        chunked(&view[&page]),
                        "seed {seed:#x} {budget:?} step {step}: child forked at {f} read the wrong \
                         page {page}"
                    );
                }
            }
        }
        // Not blind: clones were built, shared, overlaid with chunk versions and evicted.
        let w = store.stats().work;
        assert!(
            w.retained_clone_fills > 0
                && w.retained_shared_hits > 0
                && w.resolve_chunks_overlaid > 0
                && w.retained_clone_evictions > 0
                && w.retained_copies == 0,
            "seed {seed:#x} {budget:?}: fills {}, hits {}, overlaid {}, evictions {}, copies {}",
            w.retained_clone_fills,
            w.retained_shared_hits,
            w.resolve_chunks_overlaid,
            w.retained_clone_evictions,
            w.retained_copies
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.clone_keys(), (Vec::new(), 0), "seed {seed:#x}: clones leaked");
    }

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
        let store = BranchStore::with_chunk_size(CHUNK);
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
        assert_eq!(store.stats().chunk_slots_in_use, 0, "seed {seed:#x}: chunks leaked");
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
