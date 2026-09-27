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
//! # Concurrency — a striped store
//!
//! The store was one `Mutex` over every branch. Under threads it was not the first wall — the
//! kernel's read path was, removed by the shared trunk-page cache above — but it was the next one:
//! every operation of every branch took it, held for 1.6–4.6 µs per agent cycle (PREREG amendments
//! 7, 8, 8a and 9). It is now striped, the standard answer for a map whose operations each touch one
//! key (lock striping: the segments of Java's `ConcurrentHashMap`, and every sharded lock table
//! since):
//!
//! * **Shards.** Branch `id` lives in shard `id % SHARDS`, together with the arena domain its own
//!   pages are allocated from, behind the shard's lock. An operation on one branch — open, close,
//!   begin and end a write, a copy decision, a commit, a resolution — takes that one lock.
//! * **The trunk** keeps its lineage (fork epochs of its children, retained versions and their
//!   indexes) and the arena of its retained versions behind its own lock. Trunk forks, trunk
//!   copy decisions and the reaps of trunk children take it.
//! * **Reads of the trunk without its lock.** A resolution that falls through to the trunk needs
//!   the trunk's lock only if the trunk has rewritten the page since the branch's ancestry left it.
//!   The trunk epoch of each page's last write is kept in a [`Radix`], which any thread reads
//!   with three acquire loads and no write (the read side of read-copy-update, with nothing to
//!   reclaim), so a branch reading a page the trunk has not rewritten takes no trunk lock.
//! * **Two branches, two locks, never at once.** A fork locks the parent's shard, then the child's;
//!   a reap locks the child's, then the parent's (or the trunk). Neither holds two, so there is no
//!   lock order to violate; the windows between them are harmless (see [`BranchStore::fork_trunk`]
//!   and [`BranchStore::collect`]).
//! * A slot's number carries its domain, so a slot of another shard — a page an ancestor in that
//!   shard wrote — is read under that shard's lock, and a release into the wrong domain is refused.
//!
//! Every acquisition of every lock goes through `take`, which counts it (see [`BranchWork`]), so
//! what still serialises can be read from integers rather than inferred from a latency curve.
//!
//! # What this does not do
//!
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

use crossbeam_utils::CachePadded;

use super::arena::{Arena, Slot as Local};
use super::page_map::PageMap;
use super::{BranchId, BranchStats, BranchWork, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard, OnceLock};
use crate::{LimboError, Result};
use arc_swap::ArcSwapOption;

/// Shards of the branch map: branch `id` lives, with every page it owns, in shard `id % SHARDS`.
const SHARDS: usize = 64;
/// r11-coherence FM (amendment 15): the stripe count with finer stripes. A store picks `SHARDS` or this once, at
/// its construction; both are powers of two, so a branch's shard is `id & (n - 1)`.
const SHARDS_FINE: usize = 1024;
/// The arena domain of the trunk's retained versions. Shard `i` allocates from domain `i`. The same number for
/// either stripe count, so the slot layout does not depend on the arm.
const TRUNK_DOMAIN: usize = SHARDS_FINE;
/// A slot names one page-sized arena slot store-wide: its domain in the high 32 bits and the domain arena's own
/// slot number ([`Local`], a `u32`) in the low 32. Every domain holds `u32::MAX` slots in every arm, and no slot
/// reaches `u64::MAX` (the page map's empty marker). r11-coherence amendment 29: FM had packed both into a `u32` as
/// 11 domain bits and 21 local bits (7 and 25 before it), so a store with 2^21 + 1 trunk versions retained for live
/// children, or one shard with 2^21 + 1 branch pages, fired the domain's out-of-slots assert.
pub(crate) type Slot = u64;
const LOCAL_BITS: u32 = 32;

fn domain_of(slot: Slot) -> usize {
    (slot >> LOCAL_BITS) as usize
}

fn shard_of(id: BranchId, shards: usize, owner: bool) -> usize {
    // FO (amendment 22): a thread's id block (FS hands out ID_BLOCK consecutive ids) maps to one stripe.
    let key = if owner {
        id.0 >> ID_BLOCK.trailing_zeros()
    } else {
        id.0
    };
    (key & (shards as u64 - 1)) as usize
}

pub(crate) struct BranchStore {
    /// The branch map, striped: each shard holds its branches' states and the arena their pages
    /// live in, behind its own lock.
    shards: Box<[CachePadded<Mutex<Shard>>]>,
    /// The trunk's lineage and the arena of its retained versions, behind their own lock.
    trunk: CachePadded<Mutex<TrunkInner>>,
    /// The trunk epoch of its last write to each page, readable without a lock (see [`Radix`]).
    /// Written only under `trunk`'s lock. Absent reads as 0: "before the first fork that was live at
    /// the time", the conservative answer (see `first_write_trunk`).
    written: Radix<AtomicU64>,
    next_id: AtomicU64,
    /// The trunk's page size and reserved bytes per page, recorded at the first fork. Every arena
    /// domain uses the page size; neither can change while a branch exists (both need VACUUM, which
    /// is refused), so a branch connection takes its page format from here instead of reading the
    /// trunk's file header.
    trunk_format: OnceLock<(usize, u8)>,
    /// The trunk's pages as branches have read them (see "Trunk pages without the file").
    trunk_pages: TrunkPages,
    /// Branch states that exist, including ones kept alive only by a live child.
    live: AtomicUsize,
    /// Live children of the trunk. Read without the lock on every trunk first-write so that a
    /// database with no branches pays one atomic load per written page and nothing else.
    ///
    /// The unlocked read is sound because the only transition that matters — 0 to 1 — happens in
    /// a trunk fork, which holds the trunk's WAL write lock; a trunk writer reading this holds the
    /// same lock. A 1-to-0 transition (a reap) racing the read only makes the writer take the lock
    /// and find nothing to do.
    trunk_children: AtomicUsize,
    /// Whether [`take`] times how long each acquisition holds its lock. Off by default: it is the
    /// one part of the lock accounting that adds work inside a critical section.
    lock_timing: AtomicBool,
    /// r11-coherence FS: the counters below replace `next_id`, `live` and `trunk_children`, each on a line of its
    /// own, so the read-mostly fields above (`lock_timing`, read by every lock, `trunk_pages.generation`, read by
    /// every trunk resolve, `trunk_format`) sit on lines nobody writes; ids come from per-thread blocks.
    fs: bool,
    uid: u64,
    fs_next_id: CachePadded<AtomicU64>,
    fs_live: CachePadded<AtomicUsize>,
    fs_trunk_children: CachePadded<AtomicUsize>,
    /// r11-coherence FK: the trunk's live children by fork epoch, a lock-free skiplist, and the epoch counter a
    /// fork takes its epoch from, replacing `trunk.lineage.{children, epoch}`. `k_retained` counts the trunk's
    /// retained versions (changed only under the trunk lock): a reap that finds it 0 needs no garbage pass and no
    /// lock. A trunk writer raises it before it scans the children and a reaper reads it after its removal, both
    /// SeqCst, so a version retained for a child being reaped is always collected by that reap.
    fk: bool,
    k_epoch: CachePadded<AtomicU64>,
    k_children: crate::skiplist::SkipMap<u64, BranchId>,
    k_retained: CachePadded<AtomicUsize>,
    /// FZ (amendment 21): the live counter is sloppy (Boyd-Wickizer et al., OSDI 2010): each thread slot holds spare
    /// credits, the global count moves [`LIVE_BATCH`] at a time and equals the live branches plus every slot's
    /// spares, so it never under-reports. [`BranchStore::has_branches`] reclaims the spares before it answers.
    fz: bool,
    live_spares: Box<[CachePadded<AtomicUsize>]>,
    /// FO (amendment 22): owner-affine stripes (see [`shard_of`]).
    fo: bool,
    /// r11-coherence FX: copy trunk-cache hits outside the shard lock.
    copy_out: bool,
}

#[cfg(test)]
thread_local! {
    static K_DEPART_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// Tests: run `f` once, on this thread, inside FK's reap of the trunk's last child, after its depart and before its
/// generation bump (no store lock is held there).
#[cfg(test)]
pub(crate) fn set_k_depart_hook_for_test(f: Box<dyn FnOnce()>) {
    K_DEPART_HOOK.with(|h| *h.borrow_mut() = Some(f));
}

#[cfg(test)]
fn run_k_depart_hook() {
    if let Some(f) = K_DEPART_HOOK.with(|h| h.borrow_mut().take()) {
        f();
    }
}

/// FZ: live-count credits a thread slot takes per trip to the store's counter.
const LIVE_BATCH: usize = 64;

/// FS: ids a thread takes per trip to the store's counter.
const ID_BLOCK: u64 = 64;
static STORE_UIDS: AtomicU64 = AtomicU64::new(1);

std::thread_local! {
    /// FS: this thread's id blocks, one per store, keyed by the store's `uid` (never reused, unlike its address).
    static ID_BLOCKS: std::cell::RefCell<Vec<(u64, u64, u64)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// One stripe of the branch map.
struct Shard {
    branches: HashMap<BranchId, BranchState>,
    domain: Domain,
    /// The thread slot that last took this shard's lock (the coherence instrument's shard_xfer; counting builds only).
    last_slot: usize,
    /// Observation only; see [`BranchWork`]. This shard's lock is counted into its `lock_*` fields.
    work: BranchWork,
}

struct TrunkInner {
    lineage: Lineage,
    domain: Domain,
    /// Observation only. The trunk's lock is counted into its `lock_*` fields.
    work: BranchWork,
}

/// A lock-protected structure that counts its own lock.
trait Counted {
    fn work(&mut self) -> &mut BranchWork;
}

impl Counted for Shard {
    fn work(&mut self) -> &mut BranchWork {
        &mut self.work
    }
}

impl Counted for TrunkInner {
    fn work(&mut self) -> &mut BranchWork {
        &mut self.work
    }
}

/// A lock of the store, held. Observation only: dropping it adds the time it was held to its
/// structure's `lock_hold_ns` when lock timing was on at the acquisition, and does nothing else.
struct Held<'a, T: Counted> {
    guard: MutexGuard<'a, T>,
    since: Option<Instant>,
    /// The trunk lock's site (see [`TrunkSite`]); `None` for a shard's lock.
    site: Option<usize>,
}

impl<T: Counted> Deref for Held<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T: Counted> DerefMut for Held<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T: Counted> Drop for Held<'_, T> {
    fn drop(&mut self) {
        if let Some(since) = self.since {
            let held = since.elapsed().as_nanos() as u64;
            let work = self.guard.work();
            work.lock_hold_ns += held;
            if let Some(site) = self.site {
                work.trunk_sites.hold_ns[site] += held;
            }
        }
    }
}

/// Where the store takes the trunk's lock, as an index into [`super::TrunkSites`]' arrays
/// ([`super::TRUNK_LOCK_SITES`]). Observation only. (r11-k3-trunklock a328c4d05's instrument.)
#[derive(Clone, Copy)]
enum TrunkSite {
    Fork = 0,
    Reap = 1,
    Resolve = 2,
    Write = 3,
    Observe = 4,
}

/// Take `lock`, counting the acquisition into the structure it guards: every one, the ones that
/// found the lock held, and how long those waited. The counts are written under the lock itself,
/// so counting adds no shared write the lock does not already make, and the clock is read only on
/// the contended path, by the thread that is waiting anyway — except with `timed`, which reads it
/// once more at the acquisition and once at the release.
fn take<T: Counted>(lock: &Mutex<T>, timed: bool, site: Option<usize>) -> Held<'_, T> {
    let (mut guard, waited) = match lock.try_lock() {
        Some(guard) => (guard, None),
        None => {
            let start = Instant::now();
            let guard = lock.lock();
            (guard, Some(start.elapsed()))
        }
    };
    let work = guard.work();
    work.lock_acquisitions += 1;
    if let Some(waited) = waited {
        work.lock_contended += 1;
        work.lock_wait_ns += waited.as_nanos() as u64;
    }
    if let Some(site) = site {
        work.trunk_sites.acquisitions[site] += 1;
        if let Some(waited) = waited {
            work.trunk_sites.contended[site] += 1;
            work.trunk_sites.wait_ns[site] += waited.as_nanos() as u64;
        }
    }
    let since = timed.then(Instant::now);
    Held { guard, since, site }
}

/// One arena domain: a shard's pages, or the trunk's retained versions. Slots it hands out carry
/// its number (see [`LOCAL_BITS`]); it refuses a slot of another domain, so a page released into
/// the wrong arena is caught at the release.
struct Domain {
    id: usize,
    arena: Option<Arena>,
}

impl Domain {
    fn new(id: usize) -> Self {
        Self { id, arena: None }
    }

    fn local(&self, slot: Slot) -> Local {
        crate::turso_assert!(domain_of(slot) == self.id, "an arena slot of another domain");
        (slot & ((1 << LOCAL_BITS) - 1)) as Local
    }

    fn arena(&self) -> &Arena {
        self.arena.as_ref().expect("a slot of this domain exists, so its arena does")
    }

    fn alloc(&mut self, page_size: usize) -> Slot {
        let local = self.arena.get_or_insert_with(|| Arena::new(page_size)).alloc();
        ((self.id as Slot) << LOCAL_BITS) | Slot::from(local)
    }

    fn release(&mut self, slot: Slot) {
        let local = self.local(slot);
        self.arena
            .as_mut()
            .expect("a slot of this domain exists, so its arena does")
            .release(local);
    }

    fn page(&self, slot: Slot) -> &[u8] {
        self.arena().page(self.local(slot))
    }

    fn page_mut(&mut self, slot: Slot) -> &mut [u8] {
        let local = self.local(slot);
        self.arena
            .as_mut()
            .expect("a slot of this domain exists, so its arena does")
            .page_mut(local)
    }

    fn in_use(&self) -> usize {
        self.arena.as_ref().map_or(0, |a| a.in_use())
    }

    fn free_count(&self) -> usize {
        self.arena.as_ref().map_or(0, |a| a.free_count())
    }

    fn slots_in_use(&self) -> impl Iterator<Item = Slot> + '_ {
        let id = (self.id as Slot) << LOCAL_BITS;
        self.arena
            .iter()
            .flat_map(|a| a.slots_in_use())
            .map(move |local| id | Slot::from(local))
    }

    fn is_free(&self, slot: Slot) -> bool {
        self.arena
            .as_ref()
            .is_some_and(|a| a.is_free(self.local(slot)))
    }
}

/// Where the page a branch asked for comes from.
pub(crate) enum Resolved {
    /// The page is in the caller's buffer: a branch version from the arena, or the trunk's version
    /// from the shared cache.
    Filled,
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

/// One cached trunk page. `std::sync::Arc`, as `arc_swap` requires.
struct CachedPage {
    generation: u64,
    epoch: u64,
    bytes: Box<[u8]>,
}

struct TrunkPages {
    /// Bumped whenever the trunk's last child goes (see "Trunk pages without the file").
    generation: AtomicU64,
    pages: Radix<ArcSwapOption<CachedPage>>,
}

impl TrunkPages {
    /// Copy the version `key` names into `out`, if it is cached.
    fn copy_into(&self, key: TrunkPageKey, out: &mut [u8]) -> bool {
        let Some(slot) = self.pages.get(key.page) else {
            return false;
        };
        let cached = slot.load();
        match cached.as_deref() {
            Some(p) if p.generation == key.generation && p.epoch == key.epoch => {
                out.copy_from_slice(&p.bytes);
                true
            }
            _ => false,
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
            bytes: bytes.into(),
        });
        self.pages.get_or_insert(key.page).rcu(|old| match old {
            Some(p) if p.generation == key.generation && p.epoch >= key.epoch => Some(p.clone()),
            _ => Some(new.clone()),
        });
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

/// No page has this number (SQLite's largest is `u32::MAX - 1`; [`Lineage::retain`] refuses it), so
/// `(e, NO_PAGE, u64::MAX)` sorts after every index entry whose first field is `e`.
const NO_PAGE: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Retained {
    born: u64,
    died: u64,
    slot: Slot,
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
    fn child_gone(&mut self, f: u64, arena: &mut Domain, work: &mut BranchWork) -> usize {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        self.release_garbage(f, lo, hi, arena, work)
    }

    /// Free the versions that held `f` and no other live child, given `f`'s former neighbours.
    fn release_garbage(
        &mut self,
        f: u64,
        lo: Option<u64>,
        hi: Option<u64>,
        arena: &mut Domain,
        work: &mut BranchWork,
    ) -> usize {
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

    fn release_all(self, arena: &mut Domain) -> Vec<Slot> {
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
        let n_shards = if crate::coherence::fix(crate::coherence::FIX_STRIPES) {
            SHARDS_FINE
        } else {
            SHARDS
        };
        Self {
            shards: (0..n_shards)
                .map(|i| {
                    CachePadded::new(Mutex::new(Shard {
                        branches: HashMap::new(),
                        last_slot: usize::MAX,
                        domain: Domain::new(i),
                        work: BranchWork::default(),
                    }))
                })
                .collect(),
            trunk: CachePadded::new(Mutex::new(TrunkInner {
                lineage: Lineage::default(),
                domain: Domain::new(TRUNK_DOMAIN),
                work: BranchWork::default(),
            })),
            written: Radix::new(),
            next_id: AtomicU64::new(1),
            trunk_format: OnceLock::new(),
            trunk_pages: TrunkPages {
                generation: AtomicU64::new(0),
                pages: Radix::new(),
            },
            live: AtomicUsize::new(0),
            trunk_children: AtomicUsize::new(0),
            lock_timing: AtomicBool::new(false),
            fs: crate::coherence::fix(crate::coherence::FIX_STORE),
            uid: STORE_UIDS.fetch_add(1, Ordering::Relaxed),
            fs_next_id: CachePadded::new(AtomicU64::new(1)),
            fs_live: CachePadded::new(AtomicUsize::new(0)),
            fs_trunk_children: CachePadded::new(AtomicUsize::new(0)),
            fk: crate::coherence::fix(crate::coherence::FIX_TRUNKIDX),
            k_epoch: CachePadded::new(AtomicU64::new(0)),
            k_children: crate::skiplist::SkipMap::new(),
            k_retained: CachePadded::new(AtomicUsize::new(0)),
            fz: crate::coherence::fix(crate::coherence::FIX_UARC),
            fo: crate::coherence::fix(crate::coherence::FIX_OWNER),
            live_spares: (0..crate::bravo::THREADS)
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect(),
            copy_out: crate::coherence::fix(crate::coherence::FIX_COPYOUT),
        }
    }

    /// The live-branch counter (FS: on its own line).
    #[inline(always)]
    fn live_ctr(&self) -> &AtomicUsize {
        if self.fs {
            &self.fs_live
        } else {
            &self.live
        }
    }

    /// The trunk-children counter (FS: on its own line).
    #[inline(always)]
    fn trunk_children_ctr(&self) -> &AtomicUsize {
        if self.fs {
            &self.fs_trunk_children
        } else {
            &self.trunk_children
        }
    }

    /// A fresh branch id. FS: from this thread's block of [`ID_BLOCK`], refilled from the store's counter.
    fn next_branch_id(&self) -> BranchId {
        if !self.fs {
            crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
            return BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        }
        ID_BLOCKS.with(|b| {
            let mut b = b.borrow_mut();
            let i = match b.iter().position(|e| e.0 == self.uid) {
                Some(i) => i,
                None => {
                    b.push((self.uid, 0, 0));
                    b.len() - 1
                }
            };
            if b[i].1 == b[i].2 {
                crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
                let start = self.fs_next_id.fetch_add(ID_BLOCK, Ordering::Relaxed);
                b[i].1 = start;
                b[i].2 = start + ID_BLOCK;
            }
            let id = b[i].1;
            b[i].1 += 1;
            BranchId(id)
        })
    }

    fn timed(&self) -> bool {
        self.lock_timing.load(Ordering::Relaxed)
    }

    /// The shard of branch `id`, locked.
    /// Coherence instrument (r11-coherence PREREG §0 (b)): the addresses of the store's shared words.
    pub(crate) fn coherence_addrs(&self, out: &mut Vec<(String, usize)>) {
        let a = |x: *const u8| x as usize;
        out.push(("store.shards[0]".into(), a(&*self.shards[0] as *const _ as *const u8)));
        out.push(("store.shards[1]".into(), a(&*self.shards[1] as *const _ as *const u8)));
        out.push(("store.trunk".into(), a(&*self.trunk as *const _ as *const u8)));
        out.push(("store.next_id".into(), a(&self.next_id as *const _ as *const u8)));
        out.push(("store.live".into(), a(&self.live as *const _ as *const u8)));
        out.push(("store.trunk_children".into(), a(&self.trunk_children as *const _ as *const u8)));
        out.push(("store.lock_timing".into(), a(&self.lock_timing as *const _ as *const u8)));
        out.push(("store.trunk_format".into(), a(&self.trunk_format as *const _ as *const u8)));
        out.push((
            "store.trunk_pages.generation".into(),
            a(&self.trunk_pages.generation as *const _ as *const u8),
        ));
        out.push(("store.written".into(), a(&self.written as *const _ as *const u8)));
        out.push(("store.self".into(), a(self as *const _ as *const u8)));
        out.push(("store.fs_next_id".into(), a(&*self.fs_next_id as *const _ as *const u8)));
        out.push(("store.fs_live".into(), a(&*self.fs_live as *const _ as *const u8)));
        out.push(("store.fs_trunk_children".into(), a(&*self.fs_trunk_children as *const _ as *const u8)));
    }

    fn shard(&self, id: BranchId) -> Held<'_, Shard> {
        let mut held = take(&self.shards[self.shard_ix(id)], self.timed(), None);
        if crate::coherence::ENABLED {
            let me = crate::bravo::thread_index();
            if held.last_slot != me {
                crate::coherence::bump(crate::coherence::Class::ShardXfer, 1);
                held.last_slot = me;
            }
        }
        held
    }

    /// The stripe of branch `id` (FO: owner-affine).
    fn shard_ix(&self, id: BranchId) -> usize {
        shard_of(id, self.shards.len(), self.fo)
    }

    fn trunk(&self, site: TrunkSite) -> Held<'_, TrunkInner> {
        take(&self.trunk, self.timed(), Some(site as usize))
    }

    /// Turn the lock-hold timing on or off (see [`BranchWork::lock_hold_ns`]).
    pub(crate) fn set_lock_timing(&self, on: bool) {
        self.lock_timing.store(on, Ordering::Relaxed);
    }

    fn page_size(&self) -> usize {
        self.trunk_format
            .get()
            .expect("a branch exists, so the first fork fixed the page size")
            .0
    }

    /// The trunk epoch of its last write to `page`, without a lock.
    fn written(&self, page: u32) -> u64 {
        self.written
            .get(page)
            .map_or(0, |epoch| epoch.load(Ordering::Acquire))
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

    /// FK: whether every trunk write takes its copy decision (and stamps `written`) even with no child alive.
    pub(crate) fn stamps_every_trunk_write(&self) -> bool {
        self.fk
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children_ctr().load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        if self.fz && self.live_ctr().load(Ordering::Acquire) > 0 {
            // Take every slot's spares back first, so the count is exact unless a fork or reap races this call
            // (then it can only over-report: spares are never negative).
            for slot in self.live_spares.iter() {
                let spare = slot.swap(0, Ordering::AcqRel);
                if spare > 0 {
                    self.live_ctr().fetch_sub(spare, Ordering::AcqRel);
                }
            }
        }
        self.live_ctr().load(Ordering::Acquire) > 0
    }

    /// One more live branch (FZ: from this thread slot's spares when it has one).
    fn live_inc(&self) {
        if self.fz {
            let slot = &self.live_spares[crate::bravo::thread_index() % self.live_spares.len()];
            if slot
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_sub(1))
                .is_ok()
            {
                return;
            }
            crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
            self.live_ctr().fetch_add(LIVE_BATCH, Ordering::AcqRel);
            slot.fetch_add(LIVE_BATCH - 1, Ordering::AcqRel);
            return;
        }
        crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
        self.live_ctr().fetch_add(1, Ordering::AcqRel);
    }

    /// One live branch fewer (FZ: into this thread slot's spares, returned to the count a batch at a time).
    fn live_dec(&self) {
        if self.fz {
            let slot = &self.live_spares[crate::bravo::thread_index() % self.live_spares.len()];
            slot.fetch_add(1, Ordering::AcqRel);
            if slot
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    (v > 2 * LIVE_BATCH).then(|| v - LIVE_BATCH)
                })
                .is_ok()
            {
                crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
                self.live_ctr().fetch_sub(LIVE_BATCH, Ordering::AcqRel);
            }
            return;
        }
        crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
        self.live_ctr().fetch_sub(1, Ordering::AcqRel);
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    ///
    /// The child is listed among the trunk's children before its state exists in its shard. Nothing
    /// can reach it in between: its id has not been returned, and a sibling's reap only reads its
    /// fork epoch, which is final.
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
        let (id, f) = if self.fk {
            // The caller excludes trunk writers (the WAL write lock, or FG's gate), so no copy decision reads the
            // epoch or the children while they change; concurrent forks and reaps are the skiplist's to order.
            let id = self.next_branch_id();
            let f = self.k_epoch.fetch_add(1, Ordering::AcqRel);
            self.k_children.insert(f, id);
            (id, f)
        } else {
            let mut trunk = self.trunk(TrunkSite::Fork);
            let id = self.next_branch_id();
            let f = trunk.lineage.epoch;
            trunk.lineage.epoch += 1;
            trunk.lineage.children.insert(f, id);
            (id, f)
        };
        self.live_inc();
        self.shard(id).branches.insert(
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
        );
        crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
        self.trunk_children_ctr().fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock. The parent's shard and the child's are
    /// locked one after the other, never together (see [`BranchStore::fork_trunk`] for why the
    /// window between them is harmless).
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        self.fork_branch_with(parent, |s| {
            // The child's schema clone and its drop at the reap.
            crate::coherence::bump(crate::coherence::Class::SchemaArc, 2);
            s.clone()
        })
    }

    /// [`BranchStore::fork_branch`], with `schema` choosing the child's schema handle from the parent's (FZ: this
    /// thread's copy of it).
    pub(crate) fn fork_branch_with(
        &self,
        parent: BranchId,
        schema: impl FnOnce(&Arc<Schema>) -> Arc<Schema>,
    ) -> Result<BranchId> {
        let pick = schema;
        let (id, child) = {
            let mut shard = self.shard(parent);
            let st = shard.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
            if st.writer {
                return Err(LimboError::Busy);
            }
            let id = self.next_branch_id();
            let f = st.lineage.epoch;
            st.lineage.epoch += 1;
            st.lineage.children.insert(f, id);
            let schema = pick(&st.schema);
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
            (id, BranchState::new(parent, f, schema, st.trunk_at, view))
        };
        self.live_inc();
        self.shard(id).branches.insert(id, child);
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
        self.open_with(id, |s| {
            // The clone and its drop at the connection's close.
            crate::coherence::bump(crate::coherence::Class::SchemaArc, 2);
            s.clone()
        })
    }

    /// [`BranchStore::open`], with `schema` choosing the connection's schema handle from the branch's (FZ: this
    /// thread's copy of it).
    pub(crate) fn open_with(
        &self,
        id: BranchId,
        schema: impl FnOnce(&Arc<Schema>) -> Arc<Schema>,
    ) -> Result<Arc<Schema>> {
        let mut shard = self.shard(id);
        let st = shard.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.open {
            return Err(LimboError::InvalidArgument(format!(
                "branch {} already has an open connection; a branch serves one connection at a \
                 time, because a second one's page cache would silently miss the first one's \
                 commits",
                id.0
            )));
        }
        st.open = true;
        Ok(schema(&st.schema))
    }

    /// The connection on `id` has gone. Releases its write lock if a transaction was abandoned.
    pub(crate) fn close(&self, id: BranchId) {
        let mut shard = self.shard(id);
        if let Some(st) = shard.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
        }
        self.collect(shard, id);
    }

    /// The `Branch` handle has gone.
    pub(crate) fn release_handle(&self, id: BranchId) -> Reaped {
        let mut shard = self.shard(id);
        let Some(st) = shard.branches.get_mut(&id) else {
            return Reaped {
                freed_pages: 0,
                deferred: false,
            };
        };
        st.handle = false;
        let (freed_pages, removed) = self.collect(shard, id);
        Reaped {
            freed_pages,
            deferred: !removed,
        }
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut shard = self.shard(id);
        let st = shard.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        st.writer = true;
        Ok(())
    }

    pub(crate) fn end_write(&self, id: BranchId) {
        if let Some(st) = self.shard(id).branches.get_mut(&id) {
            st.writer = false;
        }
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.shard(id)
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let shard = self.shard(id);
        Ok(shard.branches.get(&id).ok_or_else(|| gone(id))?.schema.clone())
    }

    /// The copy decision for a branch's first write to `page` in a transaction. `pre_image` is the
    /// page as the branch sees it now — the version this write supersedes.
    pub(crate) fn first_write_branch(
        &self,
        id: BranchId,
        page: u32,
        pre_image: &[u8],
    ) -> Result<()> {
        let page_size = self.page_size();
        let mut shard = self.shard(id);
        let Shard {
            branches, domain, ..
        } = &mut *shard;
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        crate::turso_assert!(st.writer, "branch page written outside a write transaction");
        let epoch = st.lineage.epoch;
        match st.current.get(&page).copied() {
            None => {
                let slot = domain.alloc(page_size);
                domain.page_mut(slot).copy_from_slice(pre_image);
                st.current.insert(page, Owned { slot, born: epoch });
                if let Some(view) = st.view.as_mut() {
                    view.insert(page, slot);
                }
            }
            Some(owned) if owned.born == epoch => {}
            Some(owned) => {
                if st.lineage.has_child_in(owned.born, epoch) {
                    let slot = domain.alloc(page_size);
                    domain.page_mut(slot).copy_from_slice(pre_image);
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
    ///
    /// `written` is updated after the retained version is in place and before this write can reach
    /// the WAL, so a reader whose WAL snapshot holds the new version also sees the new epoch, and
    /// looks the page up under the trunk's lock (see [`BranchStore::resolve_into`]).
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        let mut trunk = self.trunk(TrunkSite::Write);
        let TrunkInner {
            lineage, domain, ..
        } = &mut *trunk;
        let epoch = if self.fk {
            self.k_epoch.load(Ordering::Acquire)
        } else {
            lineage.epoch
        };
        let born = self.written(page);
        if born >= epoch {
            return;
        }
        let has_child = if self.fk {
            // Announce the version before looking for its readers (see `k_retained`); undone if none.
            self.k_retained.fetch_add(1, Ordering::SeqCst);
            let found = born < epoch && self.k_children.range(born..epoch).next().is_some();
            if !found {
                self.k_retained.fetch_sub(1, Ordering::SeqCst);
            }
            found
        } else {
            lineage.has_child_in(born, epoch)
        };
        if has_child {
            let slot = domain.alloc(self.page_size());
            domain.page_mut(slot).copy_from_slice(pre_image);
            lineage.retain(
                page,
                Retained {
                    born,
                    died: epoch,
                    slot,
                },
            );
        }
        self.written.get_or_insert(page).store(epoch, Ordering::Release);
    }

    /// Commit a branch's dirty pages into the slots their copy decisions allocated.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut shard = self.shard(id);
        let Shard {
            branches, domain, ..
        } = &mut *shard;
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if pages.is_empty() {
            return Ok(());
        }
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
            domain
                .page_mut(owned.slot)
                .copy_from_slice(page.get_contents().as_slice());
        }
        Ok(())
    }

    pub(crate) fn set_schema(&self, id: BranchId, schema: Arc<Schema>) -> Result<()> {
        self.shard(id)
            .branches
            .get_mut(&id)
            .ok_or_else(|| gone(id))?
            .schema = schema;
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena or the
    /// shared trunk-page cache holds it. [`Resolved::Trunk`] means the branch sees the trunk's
    /// current version, which the caller reads through the ordinary WAL / database-file path, and
    /// the key to cache that read under.
    ///
    /// The branch's own shard answers for its own pages and for everything its ancestors wrote
    /// (`inherited`). A slot of another shard is copied under that shard's lock, taken after this
    /// one is released: nothing frees a slot a live branch can see, and the caller's connection
    /// keeps this branch live. A page no branch in the chain wrote is the trunk's. The trunk's lock
    /// is taken only when `written` says the trunk has rewritten the page since this branch's
    /// ancestry left it; otherwise the trunk's current version is the answer — from the cache when
    /// it holds it — and no trunk lock is taken. The cache is consulted, and its hit or miss
    /// counted, under this branch's shard lock.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<Resolved> {
        // FX: a trunk-cache key decided under the shard lock, copied after it is released.
        let mut copy_after: Option<TrunkPageKey> = None;
        let (at, elsewhere) = {
            let mut shard = self.shard(id);
            shard.work.resolve_calls += 1;
            shard.work.resolve_levels += 1;
            let st = shard.branches.get(&id).ok_or_else(|| gone(id))?;
            let found = st
                .current
                .get(&page)
                .map(|owned| owned.slot)
                .or_else(|| st.inherited.get(page));
            let at = st.trunk_at;
            match found {
                Some(slot) if domain_of(slot) == self.shard_ix(id) => {
                    out.copy_from_slice(shard.domain.page(slot));
                    return Ok(Resolved::Filled);
                }
                Some(slot) => (at, Some(slot)),
                None => {
                    shard.work.resolve_levels += 1;
                    // Every retained version of `page` died at or before the trunk's last write to
                    // it, so none can cover `at` unless that write came after `at`. Otherwise the
                    // branch sees the trunk's current version, last written in a closed epoch.
                    let epoch = self.written(page);
                    if epoch <= at {
                        let key = TrunkPageKey {
                            page,
                            epoch,
                            generation: self.trunk_pages.generation.load(Ordering::Acquire),
                        };
                        if self.copy_out {
                            // Counted as a hit now; a miss (rare once the cache is warm) is corrected below.
                            shard.work.trunk_page_hits += 1;
                            copy_after = Some(key);
                            (at, None)
                        } else {
                            if self.trunk_pages.copy_into(key, out) {
                                shard.work.trunk_page_hits += 1;
                                return Ok(Resolved::Filled);
                            }
                            shard.work.trunk_page_misses += 1;
                            return Ok(Resolved::Trunk(key));
                        }
                    } else {
                        (at, None)
                    }
                }
            }
        };
        if let Some(key) = copy_after {
            // The cache holds immutable versions under keys that name them exactly, so this copy needs no lock.
            if self.trunk_pages.copy_into(key, out) {
                return Ok(Resolved::Filled);
            }
            let mut shard = self.shard(id);
            shard.work.trunk_page_hits -= 1;
            shard.work.trunk_page_misses += 1;
            return Ok(Resolved::Trunk(key));
        }
        if let Some(slot) = elsewhere {
            let d = domain_of(slot);
            crate::turso_assert!(d < self.shards.len(), "a branch's page map named a trunk slot");
            out.copy_from_slice(take(&self.shards[d], self.timed(), None).domain.page(slot));
            return Ok(Resolved::Filled);
        }
        let mut trunk = self.trunk(TrunkSite::Resolve);
        let mut examined = 0;
        let found = trunk.lineage.retained_at(page, at, &mut examined);
        trunk.work.resolve_retained_examined += examined;
        if let Some(slot) = found {
            out.copy_from_slice(trunk.domain.page(slot));
            return Ok(Resolved::Filled);
        }
        // The trunk overwrote this page after the fork and nothing was retained: the ordinary
        // read path would return the NEW version. Refuse rather than serve it.
        Err(LimboError::Corrupt(format!(
            "branch {} would read trunk page {page} written after its fork; the pre-image was \
             not retained",
            id.0
        )))
    }

    /// Every counter summed over the trunk and the shards; `trunk_lock_*` is the trunk's lock
    /// alone. Takes every lock once, one at a time, so under concurrent use the sums are not one
    /// instant's: each is exact for the calls that finished before this one began.
    pub(crate) fn stats(&self) -> BranchStats {
        fn add(stats: &mut BranchStats, work: &BranchWork, domain: &Domain) {
            stats.arena_slots_in_use += domain.in_use();
            stats.arena_slots_free += domain.free_count();
            stats.work.add(work);
        }
        let mut stats = BranchStats::default();
        {
            let trunk = self.trunk(TrunkSite::Observe);
            add(&mut stats, &trunk.work, &trunk.domain);
            stats.work.trunk_lock_acquisitions = trunk.work.lock_acquisitions;
            stats.work.trunk_lock_contended = trunk.work.lock_contended;
            stats.work.trunk_lock_wait_ns = trunk.work.lock_wait_ns;
            stats.work.trunk_lock_hold_ns = trunk.work.lock_hold_ns;
        }
        for lock in self.shards.iter() {
            let shard = take(lock, self.timed(), None);
            stats.live_branches += shard.branches.len();
            add(&mut stats, &shard.work, &shard.domain);
        }
        stats
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<Slot> {
        let shard = self.shard(id);
        let Some(st) = shard.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<Slot> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.values().map(|v| v.slot));
        }
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<Slot> {
        let mut slots: Vec<Slot> = self.trunk(TrunkSite::Observe).domain.slots_in_use().collect();
        for lock in self.shards.iter() {
            slots.extend(take(lock, self.timed(), None).domain.slots_in_use());
        }
        slots
    }

    pub(crate) fn slot_is_free(&self, slot: Slot) -> bool {
        match domain_of(slot) {
            TRUNK_DOMAIN => self.trunk(TrunkSite::Observe).domain.is_free(slot),
            d if d < self.shards.len() => take(&self.shards[d], self.timed(), None).domain.is_free(slot),
            _ => false,
        }
    }

    /// Free `id` — whose shard the caller holds — if nothing can reach it any more, then its parent
    /// if that freed the parent's last reason to exist. Returns the number of arena pages released
    /// and whether `id` itself was freed.
    ///
    /// A freed branch leaves its shard (with every page it owned) before its parent forgets it, so
    /// for a moment the parent still lists its fork epoch. A sibling reaped in that moment keeps
    /// what that epoch could see — the conservative side — and the parent's `child_gone` for this
    /// branch then frees it, because each `child_gone` computes its garbage from the children the
    /// parent lists at that instant, under the parent's lock. Locks are taken child first, one at a
    /// time, never two at once.
    fn collect<'a>(&'a self, mut shard: Held<'a, Shard>, id: BranchId) -> (usize, bool) {
        let mut freed = 0;
        let mut at = id;
        loop {
            let Some(st) = shard.branches.get(&at) else {
                return (freed, at != id);
            };
            if st.handle || st.open || !st.lineage.children.is_empty() {
                return (freed, at != id);
            }
            let st = shard.branches.remove(&at).expect("just looked it up");
            self.live_dec();
            let domain = &mut shard.domain;
            for owned in st.current.values() {
                domain.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(domain).len();
            drop(shard);
            if st.parent.is_trunk() && self.fk {
                let removed = self.k_children.remove(&st.fork_epoch);
                crate::turso_assert!(removed.is_some(), "detached a child the trunk does not list");
                drop(removed);
                if self.k_retained.load(Ordering::SeqCst) > 0 {
                    // Garbage passes run one at a time under the trunk lock, each with the neighbours the
                    // skiplist lists at that moment, so two reaps of adjacent children cannot both keep a version
                    // only they held (the second pass sees the first child gone).
                    let mut trunk = self.trunk(TrunkSite::Reap);
                    let TrunkInner {
                        lineage,
                        domain,
                        work,
                    } = &mut *trunk;
                    let f = st.fork_epoch;
                    let lo = self.k_children.range(..f).next_back().map(|e| *e.key());
                    let hi = self.k_children.range(f..).next().map(|e| *e.key());
                    let n = lineage.release_garbage(f, lo, hi, domain, work);
                    self.k_retained.fetch_sub(n, Ordering::SeqCst);
                    freed += n;
                }
                crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
                if self.trunk_children_ctr().fetch_sub(1, Ordering::AcqRel) == 1 {
                    // The window r11-coherence's model (model/k_cond_a.py, FIRE_K_without_stamping) finds a stale read
                    // in when writes are not all stamped; a test replays it here (amendment 16).
                    #[cfg(test)]
                    run_k_depart_hook();
                    // Kept, but not relied on: with FK every trunk write stamps `written` whether or not the trunk
                    // has children (`stamps_every_trunk_write`), so a cache key names its version exactly and the
                    // generation guards nothing. It must not: forks no longer take the trunk lock, so one can run
                    // between the decrement above and this bump.
                    self.trunk_pages.generation.fetch_add(1, Ordering::AcqRel);
                }
                return (freed, true);
            }
            if st.parent.is_trunk() {
                let mut trunk = self.trunk(TrunkSite::Reap);
                let TrunkInner {
                    lineage,
                    domain,
                    work,
                } = &mut *trunk;
                freed += lineage.child_gone(st.fork_epoch, domain, work);
                crate::coherence::bump(crate::coherence::Class::StoreGlobal, 1);
                if self.trunk_children_ctr().fetch_sub(1, Ordering::AcqRel) == 1 {
                    // From here the trunk writes without telling the store, so no cached version
                    // can be trusted once a branch exists again. No branch can read in between: a
                    // fork needs this lock.
                    self.trunk_pages.generation.fetch_add(1, Ordering::AcqRel);
                }
                return (freed, true);
            }
            shard = self.shard(st.parent);
            let Shard {
                branches,
                domain,
                work,
                ..
            } = &mut *shard;
            let parent = branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            freed += parent.lineage.child_gone(st.fork_epoch, domain, work);
            at = st.parent;
        }
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
    /// only uncontended acquisitions, one per lock per call, and with timing off no hold time. A
    /// holder that keeps the trunk's lock for 500 ms while a second thread asks for it makes that
    /// acquisition contended, counted against the trunk,
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
        // `stats` takes the trunk's lock and every shard's, once each.
        let per_call = store.shards.len() as u64 + 1;
        assert_eq!(quiet.lock_acquisitions - base.lock_acquisitions, 11 * per_call);
        assert_eq!(quiet.trunk_lock_acquisitions - base.trunk_lock_acquisitions, 11);
        // The per-site accounting (r11-k3-trunklock a328c4d05's assertions, ported with its instrument).
        let observe = TrunkSite::Observe as usize;
        assert_eq!(
            quiet.trunk_sites.acquisitions[observe] - base.trunk_sites.acquisitions[observe],
            11,
            "`stats` is an observe site"
        );
        assert_eq!(
            quiet.trunk_sites.acquisitions.iter().sum::<u64>(),
            quiet.trunk_lock_acquisitions,
            "every trunk acquisition is counted at exactly one site"
        );
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
                let held = store.trunk(TrunkSite::Observe);
                tx.send(()).unwrap();
                std::thread::sleep(hold);
                drop(held);
            })
        };
        rx.recv().unwrap();
        let forced = store.stats().work;
        holder.join().unwrap();
        assert_eq!(forced.lock_contended, 1, "the waiting acquisition was not counted");
        assert_eq!(forced.trunk_lock_contended, 1, "it was not counted against the trunk's lock");
        assert!(forced.lock_wait_ns > 0, "a contended acquisition waited 0 ns");
        assert!(
            forced.trunk_lock_hold_ns >= hold.as_nanos() as u64,
            "the holder's {hold:?} was counted as {} ns",
            forced.trunk_lock_hold_ns
        );
        let sites = forced.trunk_sites;
        assert_eq!(sites.contended[observe], 1, "the wait was not counted at its site");
        assert_eq!(sites.contended.iter().sum::<u64>(), 1, "a wait was counted at another site");
        assert!(sites.wait_ns[observe] > 0 && sites.hold_ns[observe] >= hold.as_nanos() as u64);
    }

    /// The trunk lock's per-site accounting, one site at a time (r11-k3-trunklock 5e9450cf0's test, ported to this
    /// store's modes): a trunk fork, a trunk copy decision, a resolution of the page it rewrote, and the reap each
    /// count one acquisition at their own site and none elsewhere, less the one `observe` acquisition each closing
    /// snapshot makes itself. With FK a trunk fork takes no trunk lock; its reap takes it for the garbage pass,
    /// since the write retained a version for the child.
    #[test]
    fn trunk_lock_sites_are_counted_where_they_are_taken() {
        for mask in [0, crate::coherence::FIX_TRUNKIDX] {
            crate::coherence::force_fixes_for_test(mask);
            let fk = mask != 0;
            let store = BranchStore::new();
            let snap = || store.stats().work.trunk_sites.acquisitions;
            let delta = |a: [u64; 5], b: [u64; 5]| {
                let mut d: [u64; 5] = std::array::from_fn(|i| b[i] - a[i]);
                d[TrunkSite::Observe as usize] -= 1;
                d
            };
            let one = |site: TrunkSite| {
                let mut d = [0u64; 5];
                d[site as usize] = 1;
                d
            };
            let before = snap();
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
            let fork = if fk { [0; 5] } else { one(TrunkSite::Fork) };
            assert_eq!(delta(before, snap()), fork, "fk {fk}: fork");
            let before = snap();
            // The trunk rewrites page 0, whose content until now was `image(0)`.
            store.first_write_trunk(0, &image(0));
            assert_eq!(delta(before, snap()), one(TrunkSite::Write), "fk {fk}: trunk write");
            let before = snap();
            let mut buf = vec![0u8; PAGE];
            assert!(matches!(store.resolve_into(id, 0, &mut buf).unwrap(), Resolved::Filled));
            assert_eq!(buf, image(0), "fk {fk}: the child reads the pre-image");
            assert_eq!(delta(before, snap()), one(TrunkSite::Resolve), "fk {fk}: resolve");
            let before = snap();
            assert_eq!(store.release_handle(id).freed_pages, 1);
            assert_eq!(delta(before, snap()), one(TrunkSite::Reap), "fk {fk}: reap");
        }
        crate::coherence::force_fixes_for_test(0);
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

    /// The striped store under threads, against a model. Four workers each grow their own trees —
    /// trunk forks, forks of their own branches, writes, reaps — and after every step resolve every
    /// page of every branch they hold, while a fifth thread rewrites trunk pages. The test imposes
    /// the discipline the pager does: trunk forks and trunk copy decisions hold the WAL write lock
    /// (`wal`), a trunk commit publishes under the snapshot lock's write side (`committed`), and a
    /// reader resolves a page and reads the trunk's committed page under one read-side snapshot.
    /// Every branch must read, for every page, what it saw at its fork or wrote itself, and at the
    /// end every branch and every slot must be gone.
    ///
    /// The shapes the stripes exist for must occur, or a green run says nothing: pages a branch
    /// sees through an ancestor in another shard, trunk rewrites a branch reads through the trunk's
    /// lock, and reaps deferred by a live child.
    #[test]
    fn striped_store_under_threads_reads_every_fork_as_it_was() {
        use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64};
        use std::sync::{Mutex as StdMutex, RwLock};
        const WORKERS: u64 = 4;
        const STEPS: usize = 600;
        let store = Arc::new(BranchStore::new());
        let wal = Arc::new(StdMutex::new(()));
        let committed = Arc::new(RwLock::new(
            (0..PAGES).map(|p| (p, 0u64)).collect::<HashMap<u32, u64>>(),
        ));
        let generation = Arc::new(StdU64::new(1));
        let done = Arc::new(StdBool::new(false));
        let writer = {
            let (store, wal, committed, generation, done) = (
                store.clone(),
                wal.clone(),
                committed.clone(),
                generation.clone(),
                done.clone(),
            );
            std::thread::spawn(move || {
                let mut rng = Rng(0x5851_F42D_4C95_7F2D);
                let mut writes = 0u64;
                while !done.load(std::sync::atomic::Ordering::Acquire) {
                    let _w = wal.lock().unwrap();
                    let page = rng.below(u64::from(PAGES)) as u32;
                    if store.trunk_has_children() {
                        let before = committed.read().unwrap()[&page];
                        store.first_write_trunk(page, &image(before));
                    }
                    let g = generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    committed.write().unwrap().insert(page, g);
                    writes += 1;
                    drop(_w);
                    std::thread::yield_now();
                }
                writes
            })
        };
        let workers: Vec<_> = (0..WORKERS)
            .map(|w| {
                let (store, wal, committed, generation) =
                    (store.clone(), wal.clone(), committed.clone(), generation.clone());
                std::thread::spawn(move || {
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (w + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
                    let mut nodes: Vec<Node> = Vec::new();
                    let (mut cross_shard, mut deferred) = (0u64, 0u64);
                    let mut buf = vec![0u8; PAGE];
                    for step in 0..STEPS {
                        let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
                        match rng.below(10) {
                            0..=1 if live.len() < 16 => {
                                let _w = wal.lock().unwrap();
                                let sees = committed.read().unwrap().clone();
                                let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
                                nodes.push(Node { id, sees, handle: true, depth: 1, forked: false });
                            }
                            2..=3 if !live.is_empty() && live.len() < 16 => {
                                let p = live[rng.below(live.len() as u64) as usize];
                                let id = store.fork_branch(nodes[p].id).unwrap();
                                if store.shard_ix(id) != store.shard_ix(nodes[p].id) {
                                    cross_shard += 1;
                                }
                                let (sees, depth) = (nodes[p].sees.clone(), nodes[p].depth + 1);
                                nodes[p].forked = true;
                                nodes.push(Node { id, sees, handle: true, depth, forked: false });
                            }
                            4..=6 if !live.is_empty() => {
                                let v = live[rng.below(live.len() as u64) as usize];
                                let id = nodes[v].id;
                                store.begin_write(id).unwrap();
                                let mut committed_pages: Vec<PageRef> = Vec::new();
                                for _ in 0..=rng.below(2) {
                                    let page = rng.below(u64::from(PAGES)) as u32;
                                    if committed_pages.iter().any(|p| p.get().id == page as usize) {
                                        continue;
                                    }
                                    store
                                        .first_write_branch(id, page, &image(nodes[v].sees[&page]))
                                        .unwrap();
                                    let g = generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    committed_pages.push(page_with(page, g));
                                    nodes[v].sees.insert(page, g);
                                }
                                store.commit_pages(id, &committed_pages).unwrap();
                                store.end_write(id);
                            }
                            7..=8 if !live.is_empty() => {
                                let v = live[rng.below(live.len() as u64) as usize];
                                nodes[v].handle = false;
                                if store.release_handle(nodes[v].id).deferred {
                                    deferred += 1;
                                }
                            }
                            _ => {}
                        }
                        for n in nodes.iter().filter(|n| n.handle) {
                            for page in 0..PAGES {
                                let snapshot = committed.read().unwrap();
                                let got = match store.resolve_into(n.id, page, &mut buf).unwrap() {
                                    Resolved::Filled => {
                                        u64::from_le_bytes(buf[..8].try_into().unwrap())
                                    }
                                    // As the pager: read the committed page under this snapshot,
                                    // then cache it under the key.
                                    Resolved::Trunk(key) => {
                                        store.fill_trunk_page(key, &image(snapshot[&page]));
                                        snapshot[&page]
                                    }
                                };
                                assert_eq!(
                                    got, n.sees[&page],
                                    "worker {w} step {step}: branch {} at depth {} read the wrong \
                                     page {page}",
                                    n.id.0, n.depth
                                );
                            }
                        }
                    }
                    for n in nodes.iter().filter(|n| n.handle) {
                        store.release_handle(n.id);
                    }
                    (cross_shard, deferred)
                })
            })
            .collect();
        let (mut cross_shard, mut deferred) = (0, 0);
        for w in workers {
            let (c, d) = w.join().unwrap();
            cross_shard += c;
            deferred += d;
        }
        done.store(true, std::sync::atomic::Ordering::Release);
        let writes = writer.join().unwrap();
        let stats = store.stats();
        assert!(
            cross_shard > 0 && deferred > 0 && writes > 0 && stats.work.resolve_retained_examined > 0,
            "cross-shard forks {cross_shard}, deferred reaps {deferred}, trunk writes {writes}, \
             trunk-locked resolutions that examined a version {}",
            stats.work.resolve_retained_examined
        );
        assert_eq!(stats.live_branches, 0, "branches leaked");
        assert_eq!(stats.arena_slots_in_use, 0, "slots leaked");
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
                    let got = match store.resolve_into(*id, page, &mut buf).unwrap() {
                        Resolved::Filled => u64::from_le_bytes(buf[..8].try_into().unwrap()),
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

    /// r11-coherence amendment 29 (round 12, from SCOUT.md @ 0c36deaf; fix-interactions K5's capacity half, premise
    /// A17): the trunk domain holds every trunk pre-image a live child can still see. More than 2^21 of them must fit
    /// (FM set 21 local bits in every arm; 25 before). One child forks at epoch 0, then the trunk writes 2^21 + 1
    /// distinct pages once each: every write retains the page's version for the child. 8-byte pages keep the arena at
    /// 16 MiB. Expected pre-images are written out (the page number), not read from the subject.
    #[test]
    fn the_trunk_domain_holds_more_than_2_pow_21_retained_versions() {
        const TINY: usize = 8;
        const N: u32 = 1 << 21;
        let store = BranchStore::new();
        let child = store.fork_trunk(Arc::new(Schema::default()), TINY, 0).unwrap();
        for page in 0..N {
            store.first_write_trunk(page, &u64::from(page).to_le_bytes());
        }
        assert_eq!(
            store.stats().arena_slots_in_use,
            N as usize,
            "premise: each of the 2^21 trunk writes retained a version for the live child"
        );
        store.first_write_trunk(N, &u64::from(N).to_le_bytes());
        assert_eq!(store.stats().arena_slots_in_use, N as usize + 1, "the 2^21+1-th version was not retained");
        let mut buf = [0u8; TINY];
        for page in [0, N - 1, N] {
            assert!(
                matches!(store.resolve_into(child, page, &mut buf).unwrap(), Resolved::Filled),
                "the child must read page {page} from its retained version"
            );
            assert_eq!(u64::from_le_bytes(buf), u64::from(page), "the child read the wrong bytes for page {page}");
        }
        store.release_handle(child);
        assert_eq!(store.stats().arena_slots_in_use, 0, "the child's reap must free every retained version");
    }

    /// r11-coherence amendment 29: a shard domain holds one shard's branch-owned pages, and one branch writing more
    /// than 2^21 distinct pages must fit. One branch writes 2^21 + 1 pages in one write transaction; each first write
    /// takes a slot of the branch's shard domain.
    #[test]
    fn a_shard_domain_holds_more_than_2_pow_21_branch_pages() {
        const TINY: usize = 8;
        const N: u32 = 1 << 21;
        let store = BranchStore::new();
        let id = store.fork_trunk(Arc::new(Schema::default()), TINY, 0).unwrap();
        store.begin_write(id).unwrap();
        for page in 0..N {
            store.first_write_branch(id, page, &u64::from(page).to_le_bytes()).unwrap();
        }
        assert_eq!(
            store.stats().arena_slots_in_use,
            N as usize,
            "premise: each of the branch's 2^21 first writes took a slot"
        );
        store.first_write_branch(id, N, &u64::from(N).to_le_bytes()).unwrap();
        assert_eq!(store.stats().arena_slots_in_use, N as usize + 1, "the 2^21+1-th page took no slot");
        store.end_write(id);
        let mut buf = [0u8; TINY];
        for page in [0, N - 1, N] {
            assert!(
                matches!(store.resolve_into(id, page, &mut buf).unwrap(), Resolved::Filled),
                "the branch must read page {page} from its own slot"
            );
            assert_eq!(u64::from_le_bytes(buf), u64::from(page), "the branch read the wrong bytes for page {page}");
        }
        store.release_handle(id);
        assert_eq!(store.stats().arena_slots_in_use, 0, "the reap must free every page the branch owned");
    }
}
