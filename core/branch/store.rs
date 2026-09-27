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
//! # Reads of rewritten trunk pages without the trunk's lock (F-K3, lane r11-k3-trunklock)
//!
//! Under a writing trunk nearly every trunk page a branch reads has been rewritten since the
//! branch's `trunk_at` (with W pages on a walk and N live children the fraction is
//! ((N+1)/W)(1-(N/(N+1))^W), 0.9997 at N = 10^6), so the lock-free read above rarely applies and
//! the trunk's lock carries nearly every such read. With F-K3 (`TURSO_K3=lockfree`, chosen at
//! construction) each page's retained versions are also a lock-free skip list keyed by `born` —
//! Fraser's skip list with epoch-based reclamation (Fraser 2004; `crate::skiplist` is crossbeam's)
//! — written only under the trunk's lock, as before, and searched without it:
//!
//! * the version a branch forked at `f` sees is retained while a child forked at `f` lives, which
//!   the reading branch guarantees (it is that child or keeps it alive), so it cannot be released
//!   during the read, and its arena slot cannot be reused;
//! * versions of one page have disjoint `[born, died)`, so no concurrent insert or removal can put
//!   a key between that version's `born` and `f`: the predecessor search finds it whatever else
//!   the list is doing;
//! * a retain inserts the version before it stores `written` (Release), and the reader loads
//!   `written` (Acquire) before it searches, so a reader that sees the rewrite sees the version;
//! * removed list nodes are reclaimed by epochs, and a page's bytes are read through the trunk
//!   arena's chunks, which never move ([`Chunks`]).
//!
//! Without F-K3 the store is F5 exactly: no list is kept, and the lookup runs under the lock.
//!
//! Epoch reclamation has one known wall: a reader stalled while pinned keeps every node removed
//! after it pinned. F-K3v (`TURSO_K3=olc`, lane r11-k3-trunklock amendments 3.2 and 3e) keeps the
//! same lists with one writer and optimistic readers over type-stable nodes that are reused at once,
//! so a stalled reader holds nothing (see `super::olc`). Its readers take no lock but are NOT
//! lock-free: a writer stopped mid-change blocks readers whose path crosses the node it is changing,
//! so a search that fails 64 attempts falls back to F5's lookup under the trunk's lock (counted).
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

use super::arena::{Arena, Chunks, Slot};
use super::page_map::PageMap;
use super::olc::{OlcCounts, OlcLists, OlcLookup, OlcVersion};
use super::radix::Radix;
use super::{BranchId, BranchStats, BranchWork, Reaped};
use crate::alloc::{AllocError, ApiAllocator, Global, Layout};
use crate::schema::Schema;
use crate::skiplist::comparator::BasicComparator;
use crate::skiplist::SkipList;
use crossbeam_epoch::{self as epoch, Guard};
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard, OnceLock};
use crate::{LimboError, Result};
use arc_swap::ArcSwapOption;

/// Shards of the branch map: branch `id` lives, with every page it owns, in shard `id % SHARDS`.
const SHARDS: usize = 64;
/// The arena domain of the trunk's retained versions. Shard `i` allocates from domain `i`.
const TRUNK_DOMAIN: usize = SHARDS;
/// A slot carries its domain in its top bits and the domain arena's own slot number below them.
/// 65 domains need 7 bits, which leaves 2^25 slots per domain, and no global slot reaches
/// `u32::MAX` (the page map's empty marker).
const LOCAL_BITS: u32 = 25;

fn domain_of(slot: Slot) -> usize {
    (slot >> LOCAL_BITS) as usize
}

fn shard_of(id: BranchId) -> usize {
    (id.0 % SHARDS as u64) as usize
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
    /// Spin-then-park for the trunk's lock (see [`take`]); from `TURSO_K3_TRUNKSPIN` (ns) at construction.
    trunk_spin_ns: AtomicU64,
    /// Observation only (amendment 8.2): branch pagers' page caches emptied, for any cause.
    branch_cache_clears: CachePadded<AtomicU64>,
    /// Observation only (lane r12-branch-noclear): WAL changes a branch pager saw at the start of a
    /// read transaction and kept its cache across.
    branch_cache_clears_skipped: CachePadded<AtomicU64>,
    /// `TURSO_BRANCH_CLEAR=always` at construction: branch pagers empty their cache on every WAL change,
    /// as before lane r12-branch-noclear, so one binary times both (PREREG amendment 10). Unset: they keep it.
    branch_clear_always: bool,
    /// F-K3: the trunk's retained versions, searchable without the trunk's lock (the same `Arc` as
    /// the trunk lineage's `shared`). `None` is F5's locked lookup.
    k3: Option<std::sync::Arc<SharedVersions>>,
    k3_mode: K3Mode,
    /// F-K3: the trunk arena's pages, for reading a retained version without the trunk's lock. Set
    /// under the trunk lock by the first retain, before that version is published.
    trunk_chunks: OnceLock<std::sync::Arc<Chunks>>,
}

/// One stripe of the branch map.
struct Shard {
    branches: HashMap<BranchId, BranchState>,
    domain: Domain,
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
/// ([`super::TRUNK_LOCK_SITES`]). Observation only.
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
///
/// With `spin_ns > 0` a waiter first re-tries the lock, spinning, for up to `spin_ns` before it blocks
/// in `lock()` (spin-then-park: Ousterhout 1982; Karlin et al. 1991), so a waiter whose holder is about
/// to release does not pay a block and a wake. Acquisitions that reached the blocking `lock()` are
/// counted (`lock_blocking`).
fn take<T: Counted>(
    lock: &Mutex<T>,
    timed: bool,
    site: Option<usize>,
    spin_ns: u64,
) -> Held<'_, T> {
    let (mut guard, waited, blocked) = match lock.try_lock() {
        Some(guard) => (guard, None, false),
        None => {
            let start = Instant::now();
            let mut spun = None;
            while spin_ns > 0 && (start.elapsed().as_nanos() as u64) < spin_ns {
                std::hint::spin_loop();
                if let Some(guard) = lock.try_lock() {
                    spun = Some(guard);
                    break;
                }
            }
            match spun {
                Some(guard) => (guard, Some(start.elapsed()), false),
                None => {
                    let guard = lock.lock();
                    (guard, Some(start.elapsed()), true)
                }
            }
        }
    };
    let work = guard.work();
    work.lock_acquisitions += 1;
    if let Some(waited) = waited {
        work.lock_contended += 1;
        work.lock_wait_ns += waited.as_nanos() as u64;
    }
    if blocked {
        work.lock_blocking += 1;
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

    fn local(&self, slot: Slot) -> Slot {
        crate::turso_assert!(domain_of(slot) == self.id, "an arena slot of another domain");
        slot & ((1 << LOCAL_BITS) - 1)
    }

    fn arena(&self) -> &Arena {
        self.arena.as_ref().expect("a slot of this domain exists, so its arena does")
    }

    fn alloc(&mut self, page_size: usize) -> Slot {
        let local = self.arena.get_or_insert_with(|| Arena::new(page_size)).alloc();
        crate::turso_assert!(local < 1 << LOCAL_BITS, "an arena domain is out of slots");
        ((self.id as u32) << LOCAL_BITS) | local
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
        let id = (self.id as u32) << LOCAL_BITS;
        self.arena
            .iter()
            .flat_map(|a| a.slots_in_use())
            .map(move |local| id | local)
    }

    fn is_free(&self, slot: Slot) -> bool {
        self.arena
            .as_ref()
            .is_some_and(|a| a.is_free(self.local(slot)))
    }
}

/// Why the pager asks for a page (observation only; lane r11-k3-trunklock amendment 8.2): this
/// connection's first read of it, a re-read after a WAL change emptied its page cache, another
/// re-read, or a caller that does not say (the store's own tests).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolveCause {
    First,
    AgainAfterClear,
    AgainOther,
    Untracked,
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
    /// F-K3, the trunk's lineage only: when set, the per-page versions live here instead of in
    /// `retained`, still written only under this lineage's lock.
    shared: Option<std::sync::Arc<SharedVersions>>,
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

/// Which read path the store takes for a page the trunk rewrote after the reader's fork.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum K3Mode {
    /// F5: a predecessor search under the trunk's lock.
    Off,
    /// F-K3: crossbeam's lock-free skip list, nodes reclaimed by epochs (`TURSO_K3=lockfree`).
    Ebr,
    /// F-K3v: single-writer skip lists read optimistically over type-stable nodes, with a locked
    /// fallback after 64 failed attempts (`TURSO_K3=olc`; see `super::olc`).
    Olc,
}

impl K3Mode {
    #[cfg(test)]
    const ALL: [K3Mode; 3] = [K3Mode::Off, K3Mode::Ebr, K3Mode::Olc];

    fn lockfree(self) -> bool {
        self != K3Mode::Off
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            K3Mode::Off => "off",
            K3Mode::Ebr => "lockfree",
            K3Mode::Olc => "olc",
        }
    }
}

/// F-K3 and F-K3v: the trunk's retained versions of each page, ordered by `born`, searchable without
/// the trunk's lock (see "Reads of rewritten trunk pages without the trunk's lock"). Every write —
/// `insert`, `remove` — happens under the trunk's lock, from the lineage; `covering` is the reader's.
enum SharedVersions {
    Ebr(EbrVersions),
    Olc(OlcLists),
}

impl SharedVersions {
    fn insert(&self, page: u32, v: Retained) {
        match self {
            SharedVersions::Ebr(e) => e.insert(page, v),
            SharedVersions::Olc(o) => o.insert(page, v.into()),
        }
    }

    fn remove(&self, page: u32, born: u64) -> Option<Retained> {
        match self {
            SharedVersions::Ebr(e) => e.remove(page, born),
            SharedVersions::Olc(o) => o.remove(page, born).map(Retained::from),
        }
    }

    fn last(&self, page: u32) -> Option<Retained> {
        match self {
            SharedVersions::Ebr(e) => e.last(page),
            SharedVersions::Olc(o) => o.last(page).map(Retained::from),
        }
    }

    /// The version of `page` a child forked at `f` sees, without the trunk's lock: `Ok(found)`, or
    /// `Err(())` when F-K3v's bounded optimistic search gave up and the caller must answer under the
    /// lock. Under F-K3 the epoch guard covers the search only: the version found cannot be released
    /// while the reader's branch lives, so copying its bytes needs no guard.
    fn covering(&self, page: u32, f: u64) -> std::result::Result<Option<Retained>, ()> {
        match self {
            SharedVersions::Ebr(e) => Ok(e.covering(page, f, &epoch::pin())),
            SharedVersions::Olc(o) => match o.covering(page, f) {
                OlcLookup::Found(v) => Ok(Some(v.into())),
                OlcLookup::Absent => Ok(None),
                OlcLookup::GaveUp => Err(()),
            },
        }
    }

    /// `covering` for a caller that holds the trunk's lock: no writer can run, so it always answers.
    fn covering_locked(&self, page: u32, f: u64) -> Option<Retained> {
        match self {
            SharedVersions::Ebr(e) => e.covering(page, f, &epoch::pin()),
            SharedVersions::Olc(o) => o.covering_locked(page, f).map(Retained::from),
        }
    }

    /// List nodes allocated and not free: live versions plus the garbage reclamation holds (F-K3),
    /// or exactly the live versions (F-K3v).
    fn nodes_live(&self) -> u64 {
        match self {
            SharedVersions::Ebr(e) => e.counts.nodes.load(std::sync::atomic::Ordering::Relaxed),
            SharedVersions::Olc(o) => o.nodes_in_use(),
        }
    }

    /// F-K3: bytes of those nodes; F-K3v: bytes of the whole node pool, in use or free.
    fn node_bytes(&self) -> u64 {
        match self {
            SharedVersions::Ebr(e) => e.counts.bytes.load(std::sync::atomic::Ordering::Relaxed),
            SharedVersions::Olc(o) => o.pool_bytes(),
        }
    }

    /// F-K3v's reader and pool accounting (see [`OlcCounts`]), all 0 under F-K3.
    fn olc_counts(&self) -> OlcCounts {
        match self {
            SharedVersions::Ebr(_) => OlcCounts::default(),
            SharedVersions::Olc(o) => o.counts(),
        }
    }
}

impl From<OlcVersion> for Retained {
    fn from(v: OlcVersion) -> Self {
        Retained {
            born: v.born,
            died: v.died,
            slot: v.slot,
        }
    }
}

impl From<Retained> for OlcVersion {
    fn from(v: Retained) -> Self {
        OlcVersion {
            born: v.born,
            died: v.died,
            slot: v.slot,
        }
    }
}

/// F-K3: the trunk's retained versions of each page as crossbeam's lock-free skip list; removed
/// nodes are freed by epoch reclamation. A page's list is created once and lives as long as the store.
struct EbrVersions {
    pages: Radix<OnceLock<VersionList>>,
    /// The lists' allocator's counts (see [`NodeAlloc`]).
    counts: &'static NodeCounts,
}

type VersionList = SkipList<u64, Retained, BasicComparator, NodeAlloc>;

/// F-K3's instrument: nodes and bytes of the trunk's version lists that are allocated and not yet
/// freed. A node removed from its list is freed only when epoch reclamation finds no reader can
/// still reach it, so these minus the live versions are the garbage reclamation holds back.
/// Observation only: one relaxed add per node allocation and per free.
#[derive(Default)]
struct NodeCounts {
    nodes: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
}

/// The version lists' allocator: the global one, counting into [`NodeCounts`]. A `&'static`, so the
/// copy each deferred free captures costs no reference count; one small `NodeCounts` is leaked per
/// F-K3 store.
#[derive(Clone, Copy)]
struct NodeAlloc(&'static NodeCounts);

// SAFETY: delegates every allocation and free to `Global` unchanged; it only counts them.
unsafe impl ApiAllocator for NodeAlloc {
    fn allocate(&self, layout: Layout) -> std::result::Result<std::ptr::NonNull<[u8]>, AllocError> {
        let block = <Global as ApiAllocator>::allocate(&Global, layout)?;
        self.0.nodes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0
            .bytes
            .fetch_add(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(block)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        self.0.nodes.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        self.0
            .bytes
            .fetch_sub(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { <Global as ApiAllocator>::deallocate(&Global, ptr, layout) }
    }
}

impl EbrVersions {
    fn new() -> Self {
        Self {
            pages: Radix::new(),
            counts: Box::leak(Box::default()),
        }
    }

    fn list(&self, page: u32) -> Option<&VersionList> {
        self.pages.get(page)?.get()
    }

    fn insert(&self, page: u32, v: Retained) {
        let list = self.pages.get_or_insert(page).get_or_init(|| {
            SkipList::new_in(epoch::default_collector().clone(), NodeAlloc(self.counts))
        });
        let guard = epoch::pin();
        list.insert(v.born, v, &guard).release(&guard);
    }

    fn remove(&self, page: u32, born: u64) -> Option<Retained> {
        let list = self.list(page)?;
        let guard = epoch::pin();
        let entry = list.remove(&born, &guard)?;
        let v = *entry.value();
        entry.release(&guard);
        Some(v)
    }

    /// The newest retained version of `page`.
    fn last(&self, page: u32) -> Option<Retained> {
        let guard = epoch::pin();
        self.list(page)?.back(&guard).map(|e| *e.value())
    }

    /// The version of `page` a child forked at `f` sees: the born-predecessor of `f`, if it was
    /// still current at `f`. Lock-free; `guard` keeps the nodes the search passes alive.
    fn covering(&self, page: u32, f: u64, guard: &Guard) -> Option<Retained> {
        let v = *self
            .list(page)?
            .upper_bound(Bound::Included(&f), guard)?
            .value();
        (f < v.died).then_some(v)
    }
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
        let last = match &self.shared {
            Some(shared) => shared.last(page),
            None => self
                .retained
                .get(&page)
                .and_then(|versions| versions.last_key_value().map(|(_, last)| *last)),
        };
        crate::turso_assert!(
            last.is_none_or(|last| last.died <= v.born),
            "a retained version overlaps an older one of the same page; the born-ordered lookup \
             would return the wrong one"
        );
        crate::turso_assert!(page != NO_PAGE, "page number u32::MAX is the index sentinel");
        match &self.shared {
            Some(shared) => shared.insert(page, v),
            None => {
                self.retained.entry(page).or_default().insert(v.born, v);
            }
        }
        self.by_born.insert((v.born, page, v.died));
        self.by_died.insert((v.died, page, v.born));
    }

    /// The retained version of `page` visible to a child forked at `f`: the born-predecessor of
    /// `f`, if it was still current at `f`. `examined` counts the versions compared against `f` —
    /// at most one; the O(log V) descent that finds it is not counted.
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<Slot> {
        if let Some(shared) = &self.shared {
            *examined += 1;
            return shared.covering_locked(page, f).map(|v| v.slot);
        }
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
        let dead = self.garbage(f, lo, hi, work);
        for &(born, page, died) in &dead {
            let v = match &self.shared {
                Some(shared) => shared.remove(page, born).expect("indexed version is listed"),
                None => {
                    let versions = self.retained.get_mut(&page).expect("indexed version is listed");
                    let v = versions.remove(&born).expect("indexed version is listed");
                    if versions.is_empty() {
                        self.retained.remove(&page);
                    }
                    v
                }
            };
            work.gc_examined += 1;
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
    /// A store whose trunk reads follow `TURSO_K3`: `lockfree` for F-K3, `olc` for F-K3v, unset (or
    /// `off`) for F5's locked lookup. Any other value is refused: a run must not measure a mode it
    /// did not name.
    pub(crate) fn new() -> Self {
        let mode = match std::env::var("TURSO_K3") {
            Err(std::env::VarError::NotPresent) => K3Mode::Off,
            Ok(v) if v.is_empty() || v == "off" => K3Mode::Off,
            Ok(v) if v == "lockfree" => K3Mode::Ebr,
            Ok(v) if v == "olc" => K3Mode::Olc,
            other => panic!("TURSO_K3={other:?}: expected `lockfree`, `olc` or `off`"),
        };
        Self::with_k3(mode)
    }

    /// A store whose reads of rewritten trunk pages take the path `mode` names.
    pub(crate) fn with_k3(mode: K3Mode) -> Self {
        let shared = match mode {
            K3Mode::Off => None,
            K3Mode::Ebr => Some(SharedVersions::Ebr(EbrVersions::new())),
            K3Mode::Olc => Some(SharedVersions::Olc(OlcLists::new())),
        };
        Self::with_shared(mode, shared)
    }

    /// F-K3v with unlocked searches that give up after `max_failed` failed attempts; 0 sends every
    /// read of a rewritten page to the locked fallback (PREREG amendment 3f, N4a).
    #[cfg(test)]
    fn with_olc_max_failed(max_failed: u32) -> Self {
        Self::with_shared(
            K3Mode::Olc,
            Some(SharedVersions::Olc(OlcLists::with_max_failed(max_failed))),
        )
    }

    fn with_shared(mode: K3Mode, shared: Option<SharedVersions>) -> Self {
        let k3 = shared.map(std::sync::Arc::new);
        Self {
            shards: (0..SHARDS)
                .map(|i| {
                    CachePadded::new(Mutex::new(Shard {
                        branches: HashMap::new(),
                        domain: Domain::new(i),
                        work: BranchWork::default(),
                    }))
                })
                .collect(),
            trunk: CachePadded::new(Mutex::new(TrunkInner {
                lineage: Lineage {
                    shared: k3.clone(),
                    ..Lineage::default()
                },
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
            branch_cache_clears: CachePadded::new(AtomicU64::new(0)),
            branch_cache_clears_skipped: CachePadded::new(AtomicU64::new(0)),
            branch_clear_always: match std::env::var("TURSO_BRANCH_CLEAR") {
                Err(std::env::VarError::NotPresent) => false,
                Ok(v) if v.is_empty() || v == "keep" => false,
                Ok(v) if v == "always" => true,
                other => panic!("TURSO_BRANCH_CLEAR={other:?}: expected `keep` or `always`"),
            },
            trunk_spin_ns: AtomicU64::new(match std::env::var("TURSO_K3_TRUNKSPIN") {
                Err(std::env::VarError::NotPresent) => 0,
                Ok(v) => v
                    .parse()
                    .unwrap_or_else(|_| panic!("TURSO_K3_TRUNKSPIN={v:?}: expected nanoseconds")),
                Err(e) => panic!("TURSO_K3_TRUNKSPIN: {e}"),
            }),
            k3,
            k3_mode: mode,
            trunk_chunks: OnceLock::new(),
        }
    }

    /// Whether this store reads rewritten trunk pages without the trunk's lock (F-K3 or F-K3v).
    pub(crate) fn lockfree_trunk_reads(&self) -> bool {
        self.k3_mode.lockfree()
    }

    /// The trunk-read mode's name: `off`, `lockfree` or `olc`.
    pub(crate) fn k3_mode(&self) -> &'static str {
        self.k3_mode.name()
    }

    fn timed(&self) -> bool {
        self.lock_timing.load(Ordering::Relaxed)
    }

    /// The shard of branch `id`, locked.
    fn shard(&self, id: BranchId) -> Held<'_, Shard> {
        take(&self.shards[shard_of(id)], self.timed(), None, 0)
    }

    fn trunk(&self, site: TrunkSite) -> Held<'_, TrunkInner> {
        take(
            &self.trunk,
            self.timed(),
            Some(site as usize),
            self.trunk_spin_ns.load(Ordering::Relaxed),
        )
    }

    /// A branch pager's page cache was emptied (observation only).
    pub(crate) fn note_branch_cache_clear(&self) {
        self.branch_cache_clears.fetch_add(1, Ordering::Relaxed);
    }

    /// A branch pager saw the WAL changed and kept its page cache (observation only).
    pub(crate) fn note_branch_cache_clear_skipped(&self) {
        self.branch_cache_clears_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether branch pagers empty their cache on every WAL change (`TURSO_BRANCH_CLEAR=always`).
    pub(crate) fn branch_clear_always(&self) -> bool {
        self.branch_clear_always
    }

    /// How long a waiter for the trunk's lock spins before it blocks (PREREG amendment 5); 0: it
    /// blocks at once, as every other lock of the store does.
    pub(crate) fn set_trunk_spin_ns(&self, ns: u64) {
        self.trunk_spin_ns.store(ns, Ordering::Relaxed);
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

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        self.live.load(Ordering::Acquire) > 0
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
        let (id, f) = {
            let mut trunk = self.trunk(TrunkSite::Fork);
            let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
            let f = trunk.lineage.epoch;
            trunk.lineage.epoch += 1;
            trunk.lineage.children.insert(f, id);
            (id, f)
        };
        self.live.fetch_add(1, Ordering::AcqRel);
        self.shard(id).branches.insert(
            id,
            BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default()),
        );
        self.trunk_children.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock. The parent's shard and the child's are
    /// locked one after the other, never together (see [`BranchStore::fork_trunk`] for why the
    /// window between them is harmless).
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let (id, child) = {
            let mut shard = self.shard(parent);
            let st = shard.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
            if st.writer {
                return Err(LimboError::Busy);
            }
            let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
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
            (id, BranchState::new(parent, f, schema, st.trunk_at, view))
        };
        self.live.fetch_add(1, Ordering::AcqRel);
        self.shard(id).branches.insert(id, child);
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema. One connection per
    /// branch: two would each hold a private page cache of the same page space, and nothing would
    /// tell one that the other had committed — a silently stale read, so it is refused.
    pub(crate) fn open(&self, id: BranchId) -> Result<Arc<Schema>> {
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
        Ok(st.schema.clone())
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
        let epoch = lineage.epoch;
        let born = self.written(page);
        if born >= epoch {
            return;
        }
        if lineage.has_child_in(born, epoch) {
            let slot = domain.alloc(self.page_size());
            domain.page_mut(slot).copy_from_slice(pre_image);
            if self.k3.is_some() {
                // Before the version is published: a reader that finds it reads through these.
                self.trunk_chunks.get_or_init(|| domain.arena().chunks());
            }
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
        self.resolve_into_as(id, page, out, ResolveCause::Untracked)
    }

    /// [`Self::resolve_into`], counting the resolve under `cause` (observation only).
    pub(crate) fn resolve_into_as(
        &self,
        id: BranchId,
        page: u32,
        out: &mut [u8],
        cause: ResolveCause,
    ) -> Result<Resolved> {
        let (at, elsewhere) = {
            let mut shard = self.shard(id);
            shard.work.resolve_calls += 1;
            match cause {
                ResolveCause::First => shard.work.resolve_first += 1,
                ResolveCause::AgainAfterClear => shard.work.resolve_again_clear += 1,
                ResolveCause::AgainOther => shard.work.resolve_again_other += 1,
                ResolveCause::Untracked => {}
            }
            shard.work.resolve_levels += 1;
            let st = shard.branches.get(&id).ok_or_else(|| gone(id))?;
            let found = st
                .current
                .get(&page)
                .map(|owned| owned.slot)
                .or_else(|| st.inherited.get(page));
            let at = st.trunk_at;
            match found {
                Some(slot) if domain_of(slot) == shard_of(id) => {
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
                        if self.trunk_pages.copy_into(key, out) {
                            shard.work.trunk_page_hits += 1;
                            return Ok(Resolved::Filled);
                        }
                        shard.work.trunk_page_misses += 1;
                        return Ok(Resolved::Trunk(key));
                    }
                    shard.work.resolve_trunk_rewritten += 1;
                    (at, None)
                }
            }
        };
        if let Some(slot) = elsewhere {
            let d = domain_of(slot);
            crate::turso_assert!(d < SHARDS, "a branch's page map named a trunk slot");
            out.copy_from_slice(take(&self.shards[d], self.timed(), None, 0).domain.page(slot));
            return Ok(Resolved::Filled);
        }
        // F-K3/F-K3v: no trunk lock (see "Reads of rewritten trunk pages without the trunk's
        // lock"). F-K3v's search may give up after too many failed attempts; it then takes F5's path.
        let lockfree = self.k3.as_ref().map(|shared| shared.covering(page, at));
        if let Some(Ok(found)) = lockfree {
            if let Some(v) = found {
                crate::turso_assert!(
                    domain_of(v.slot) == TRUNK_DOMAIN,
                    "a trunk version outside the trunk's arena"
                );
                let chunks = self
                    .trunk_chunks
                    .get()
                    .expect("a retained version was allocated, so the trunk arena exists");
                // SAFETY: the version stays retained, and its slot unwritten, while this branch
                // lives; its bytes were written before the retain published it, which the load of
                // `written` above ordered before this read.
                unsafe { chunks.read(v.slot & ((1 << LOCAL_BITS) - 1), out) };
                return Ok(Resolved::Filled);
            }
        } else {
            // F5, or F-K3v's fallback: under the trunk's lock no writer runs.
            let mut trunk = self.trunk(TrunkSite::Resolve);
            trunk.work.resolve_trunk_locked += 1;
            let mut examined = 0;
            let found = trunk.lineage.retained_at(page, at, &mut examined);
            trunk.work.resolve_retained_examined += examined;
            if let Some(slot) = found {
                out.copy_from_slice(trunk.domain.page(slot));
                return Ok(Resolved::Filled);
            }
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
            stats.trunk_slots_in_use = trunk.domain.in_use();
            if let Some(k3) = &self.k3 {
                stats.k3_nodes_live = k3.nodes_live();
                stats.k3_node_bytes_live = k3.node_bytes();
                let olc = k3.olc_counts();
                if matches!(**k3, SharedVersions::Olc(_)) {
                    // One snapshot for the list side and the pool side, so the harness can compare
                    // them exactly.
                    stats.k3_nodes_live = olc.in_use;
                    stats.k3_node_bytes_live = olc.pool_bytes;
                }
                stats.k3_olc_restarts = olc.restarts;
                stats.k3_olc_head_spins = olc.head_spins;
                stats.k3_olc_fallbacks = olc.fallbacks;
                stats.k3_olc_peak_in_use = olc.peak_in_use;
                stats.k3_olc_pool_bound_bytes = olc.pool_bound_bytes;
                stats.k3_olc_pool_in_use = olc.pool_in_use;
                stats.k3_olc_max_class_chunks = olc.max_class_chunks;
            }
            stats.work.trunk_lock_acquisitions = trunk.work.lock_acquisitions;
            stats.work.trunk_lock_contended = trunk.work.lock_contended;
            stats.work.trunk_lock_wait_ns = trunk.work.lock_wait_ns;
            stats.work.trunk_lock_hold_ns = trunk.work.lock_hold_ns;
            stats.work.trunk_lock_blocking = trunk.work.lock_blocking;
            stats.branch_cache_clears = self.branch_cache_clears.load(Ordering::Relaxed);
            stats.branch_cache_clears_skipped =
                self.branch_cache_clears_skipped.load(Ordering::Relaxed);
        }
        for lock in self.shards.iter() {
            let shard = take(lock, self.timed(), None, 0);
            stats.live_branches += shard.branches.len();
            add(&mut stats, &shard.work, &shard.domain);
        }
        stats
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let shard = self.shard(id);
        let Some(st) = shard.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.values().map(|v| v.slot));
        }
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<u32> {
        let mut slots: Vec<u32> = self.trunk(TrunkSite::Observe).domain.slots_in_use().collect();
        for lock in self.shards.iter() {
            slots.extend(take(lock, self.timed(), None, 0).domain.slots_in_use());
        }
        slots
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        match domain_of(slot) {
            TRUNK_DOMAIN => self.trunk(TrunkSite::Observe).domain.is_free(slot),
            d if d < SHARDS => take(&self.shards[d], self.timed(), None, 0).domain.is_free(slot),
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
            self.live.fetch_sub(1, Ordering::AcqRel);
            let domain = &mut shard.domain;
            for owned in st.current.values() {
                domain.release(owned.slot);
                freed += 1;
            }
            freed += st.lineage.release_all(domain).len();
            drop(shard);
            if st.parent.is_trunk() {
                let mut trunk = self.trunk(TrunkSite::Reap);
                let TrunkInner {
                    lineage,
                    domain,
                    work,
                } = &mut *trunk;
                freed += lineage.child_gone(st.fork_epoch, domain, work);
                if self.trunk_children.fetch_sub(1, Ordering::AcqRel) == 1 {
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
        for mode in K3Mode::ALL {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run(seed, mode);
            }
        }
    }

    fn run(seed: u64, mode: K3Mode) {
        let store = BranchStore::with_k3(mode);
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
                    assert!(!reaped.deferred, "seed {seed:#x} k3 {mode:?} step {step}");
                    assert_eq!(
                        before.arena_slots_in_use - after.arena_slots_in_use,
                        reaped.freed_pages,
                        "seed {seed:#x} k3 {mode:?} step {step}: the reap's report disagrees with the arena"
                    );
                    let visited = after.work.gc_range_entries - before.work.gc_range_entries;
                    let contract = match (lo, hi) {
                        (None, _) | (_, None) => reaped.freed_pages as u64,
                        _ if b <= d => 2 * b,
                        _ => 2 * d + 1,
                    };
                    assert_eq!(
                        visited, contract,
                        "seed {seed:#x} k3 {mode:?} step {step}: reaping the child forked at {f} (lo {lo:?}, \
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
                "seed {seed:#x} k3 {mode:?} step {step}: the arena holds a version no live child can see, or \
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
                        "seed {seed:#x} k3 {mode:?} step {step}: child forked at {f} read the wrong page {page}"
                    );
                }
            }
        }
        // The walk above must have freed versions through all three shapes of the garbage query;
        // otherwise a green run says nothing about the shape it skipped.
        assert!(
            freed_oldest > 0 && freed_newest > 0 && freed_middle > 0,
            "seed {seed:#x} k3 {mode:?}: reaps that freed versions: oldest {freed_oldest}, newest \
             {freed_newest}, middle {freed_middle}"
        );
        for (id, _, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x} k3 {mode:?}: versions leaked");
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
        let per_call = SHARDS as u64 + 1;
        assert_eq!(quiet.lock_acquisitions - base.lock_acquisitions, 11 * per_call);
        assert_eq!(quiet.trunk_lock_acquisitions - base.trunk_lock_acquisitions, 11);
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

    /// Spin-then-park on the trunk's lock (PREREG amendment 5), its counter forced to fire both ways:
    /// a holder keeps the lock 20 ms while another thread asks for it. With a 5 s spin the waiter gets
    /// the lock while spinning and never blocks; with no spin it blocks. Either way the acquisition is
    /// contended and waited.
    #[test]
    fn trunk_spin_waits_out_a_short_hold_without_blocking() {
        for (spin_ns, blocks) in [(5_000_000_000u64, 0u64), (0, 1)] {
            let store = Arc::new(BranchStore::with_k3(K3Mode::Off));
            store.set_trunk_spin_ns(spin_ns);
            let before = store.stats().work;
            let (tx, rx) = std::sync::mpsc::channel();
            let holder = {
                let store = store.clone();
                std::thread::spawn(move || {
                    let held = store.trunk(TrunkSite::Observe);
                    tx.send(()).unwrap();
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    drop(held);
                })
            };
            rx.recv().unwrap();
            let after = store.stats().work;
            holder.join().unwrap();
            assert_eq!(
                after.trunk_lock_contended - before.trunk_lock_contended,
                1,
                "spin {spin_ns}: the waiting acquisition was not counted"
            );
            assert_eq!(
                after.trunk_lock_blocking - before.trunk_lock_blocking,
                blocks,
                "spin {spin_ns}: blocking acquisitions"
            );
            assert!(after.trunk_lock_wait_ns > before.trunk_lock_wait_ns);
        }
    }

    /// The trunk lock's per-site accounting, one site at a time: a trunk fork, a trunk copy
    /// decision, a resolution of the page it rewrote, and the reap each count one acquisition at
    /// their own site and none elsewhere (F-K3's resolution counts none at all), less the one
    /// `observe` acquisition each closing snapshot makes itself.
    #[test]
    fn trunk_lock_sites_are_counted_where_they_are_taken() {
        for mode in K3Mode::ALL {
            let store = BranchStore::with_k3(mode);
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
            assert_eq!(delta(before, snap()), one(TrunkSite::Fork), "k3 {mode:?}: fork");
            let before = snap();
            // The trunk rewrites page 0, whose content until now was `image(0)`.
            store.first_write_trunk(0, &image(0));
            assert_eq!(delta(before, snap()), one(TrunkSite::Write), "k3 {mode:?}: trunk write");
            let before = snap();
            let mut buf = vec![0u8; PAGE];
            assert!(matches!(store.resolve_into(id, 0, &mut buf).unwrap(), Resolved::Filled));
            assert_eq!(buf, image(0), "k3 {mode:?}: the child reads the pre-image");
            let resolve = if mode.lockfree() { [0; 5] } else { one(TrunkSite::Resolve) };
            assert_eq!(delta(before, snap()), resolve, "k3 {mode:?}: resolve");
            let before = snap();
            assert_eq!(store.release_handle(id).freed_pages, 1);
            assert_eq!(delta(before, snap()), one(TrunkSite::Reap), "k3 {mode:?}: reap");
        }
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
        for mode in K3Mode::ALL {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run_tree(seed, mode);
            }
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

    fn run_tree(seed: u64, mode: K3Mode) {
        let store = BranchStore::with_k3(mode);
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
                        "seed {seed:#x} k3 {mode:?} step {step}: branch {} at depth {} read the wrong page {page}",
                        n.id.0,
                        n.depth
                    );
                }
            }
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing.
        assert!(
            max_depth >= 10 && deferred > 0 && wrote_after_fork > 0,
            "seed {seed:#x} k3 {mode:?}: max depth {max_depth}, deferred reaps {deferred}, writes by a branch \
             after its first fork {wrote_after_fork}"
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id);
        }
        assert_eq!(store.stats().live_branches, 0, "seed {seed:#x} k3 {mode:?}: branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x} k3 {mode:?}: slots leaked");
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
        for mode in K3Mode::ALL {
            striped_threads(mode);
        }
    }

    fn striped_threads(mode: K3Mode) {
        use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64};
        use std::sync::{Mutex as StdMutex, RwLock};
        const WORKERS: u64 = 4;
        const STEPS: usize = 600;
        let store = Arc::new(BranchStore::with_k3(mode));
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
                                if shard_of(id) != shard_of(nodes[p].id) {
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
            cross_shard > 0 && deferred > 0 && writes > 0 && stats.work.resolve_trunk_rewritten > 0,
            "k3 {mode:?}: cross-shard forks {cross_shard}, deferred reaps {deferred}, trunk writes \
             {writes}, resolutions of rewritten trunk pages {}",
            stats.work.resolve_trunk_rewritten
        );
        // F5 takes the trunk's lock for every rewritten-page read, F-K3 for none, F-K3v only for the
        // searches that gave up.
        let expect_locked = match mode {
            K3Mode::Off => stats.work.resolve_trunk_rewritten,
            K3Mode::Ebr => 0,
            K3Mode::Olc => stats.k3_olc_fallbacks,
        };
        assert_eq!(stats.work.resolve_trunk_locked, expect_locked, "k3 {mode:?}: trunk-locked reads");
        // N6: the margin, printed for the run's record (`--nocapture`).
        println!(
            "striped k3 {mode:?}: rewritten reads {}, locked {}, olc restarts {}, head spins {}, \
             fallbacks {}",
            stats.work.resolve_trunk_rewritten,
            stats.work.resolve_trunk_locked,
            stats.k3_olc_restarts,
            stats.k3_olc_head_spins,
            stats.k3_olc_fallbacks
        );
        // F-K3v's concurrent validation must have run: a run where no reader ever met a node that
        // changed under it is VOID (a starved box can produce one), not a pass.
        assert!(
            mode != K3Mode::Olc || stats.k3_olc_restarts > 0,
            "k3 {mode:?}: no reader restarted, so the optimistic race was not run (VOID)"
        );
        assert!(
            mode != K3Mode::Olc || stats.k3_olc_pool_in_use == stats.k3_nodes_live,
            "k3 {mode:?}: the pools lost track of removed nodes ({} handed out, {} in lists)",
            stats.k3_olc_pool_in_use,
            stats.k3_nodes_live
        );
        assert_eq!(stats.live_branches, 0, "branches leaked");
        assert_eq!(stats.arena_slots_in_use, 0, "slots leaked");
    }

    /// F-K3 under threads (lane r11-k3-trunklock): readers resolve trunk pages the trunk rewrote
    /// after their fork while a writer retains new versions of the same pages and a churner forks
    /// and reaps trunk children between theirs, so `child_gone` removes versions next to the readers'
    /// from the very lists they are searching; each reader also swaps its own children now and then.
    /// The discipline is the pager's, as in `striped_store_under_threads_reads_every_fork_as_it_was`.
    /// Every read must return the page as of the reader's fork; F-K3 must answer every rewritten
    /// read without the trunk's lock and F5 every one under it; the churn must have freed versions
    /// while the readers read (or the race was not run); and nothing may leak.
    #[test]
    fn rewritten_trunk_reads_race_retains_and_reaps() {
        for mode in K3Mode::ALL {
            race(BranchStore::with_k3(mode), mode, false);
        }
    }

    /// F-K3v's fallback, run on every read (PREREG amendment 3f, N4a): with unlocked searches that
    /// give up at once, every read of a rewritten page must be answered on F5's path under the
    /// trunk's lock, correctly (the race's own checks), and be counted as a fallback.
    #[test]
    fn every_rewritten_read_can_fall_back_to_the_trunk_lock() {
        race(BranchStore::with_olc_max_failed(0), K3Mode::Olc, true);
    }

    type Committed = std::sync::RwLock<HashMap<u32, u64>>;

    /// Fork a trunk child as the pager does: under the WAL write lock, seeing the committed pages.
    fn fork_seeing(
        store: &BranchStore,
        wal: &std::sync::Mutex<()>,
        committed: &Committed,
    ) -> (BranchId, HashMap<u32, u64>) {
        let _w = wal.lock().unwrap();
        let sees = committed.read().unwrap().clone();
        (store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap(), sees)
    }

    /// The race on `store`, which reads rewritten pages as `mode` says; with `always_fallback`, every
    /// F-K3v search gives up at once and falls back (see `every_rewritten_read_can_fall_back_...`).
    fn race(store: BranchStore, mode: K3Mode, always_fallback: bool) {
        use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64, Ordering as O};
        use std::sync::{Mutex as StdMutex, RwLock};
        const READERS: u64 = 4;
        const HELD: usize = 6;
        // Workload strengthened (PREREG amendment 3h, the lead's race-test decision): long enough that
        // the churn below completes many rounds while the readers still read.
        const READS: usize = 40_000;
        // Churn rounds the readers keep reading for, past READS if need be (up to 4 x READS).
        const MIN_CHURN_ROUNDS: u64 = 50;
        let store = Arc::new(store);
        let wal = Arc::new(StdMutex::new(()));
        let committed: Arc<Committed> =
            Arc::new(RwLock::new((0..PAGES).map(|p| (p, 0u64)).collect()));
        let generation = StdU64::new(1);
        let done = Arc::new(StdBool::new(false));
        // Readers still reading: the churn's frees count only while this is above 0, so the race
        // is shown to have run, not merely to have started before or after the reads.
        let active = Arc::new(StdU64::new(READERS));
        // Trunk write ROUNDS so far (each round rewrites every page), published by the writer so the
        // churner can fork right after one and keep its children alive across two more; and churn
        // rounds done, so the readers keep reading until enough of them ran (amendment 3h).
        let wcount = Arc::new(StdU64::new(0));
        let crounds = Arc::new(StdU64::new(0));
        let writer = {
            let (store, wal, committed, done, wcount) = (
                store.clone(),
                wal.clone(),
                committed.clone(),
                done.clone(),
                wcount.clone(),
            );
            std::thread::spawn(move || {
                let mut writes = 0u64;
                let mut rounds = 0u64;
                while !done.load(O::Acquire) {
                    // One trunk transaction rewriting every page (amendment 3h): each page's newest
                    // version is then born at the round, so a child forked after it and reaped after
                    // the next rounds is alone in the versions those rounds retain.
                    let _w = wal.lock().unwrap();
                    for page in 0..PAGES {
                        if store.trunk_has_children() {
                            let before = committed.read().unwrap()[&page];
                            store.first_write_trunk(page, &image(before));
                        }
                        let g = generation.fetch_add(1, O::Relaxed);
                        committed.write().unwrap().insert(page, g);
                        writes += 1;
                    }
                    rounds += 1;
                    wcount.store(rounds, O::Release);
                    drop(_w);
                    std::thread::yield_now();
                }
                writes
            })
        };
        let churner = {
            let (store, wal, committed, done, active, wcount, crounds) = (
                store.clone(),
                wal.clone(),
                committed.clone(),
                done.clone(),
                active.clone(),
                wcount,
                crounds.clone(),
            );
            std::thread::spawn(move || {
                let mut rng = Rng(0x2545_F491_4F6C_DD1D);
                let mut freed = 0u64;
                while !done.load(O::Acquire) {
                    // Fork right after a fresh write round, and keep the children alive across two
                    // more (amendment 3h): the versions retained for them alone are then there for
                    // their reaps to free.
                    let after = wcount.load(O::Acquire);
                    while wcount.load(O::Acquire) == after && !done.load(O::Acquire) {
                        std::thread::yield_now();
                    }
                    let mut kids: Vec<BranchId> = (0..=rng.below(3))
                        .map(|_| fork_seeing(&store, &wal, &committed).0)
                        .collect();
                    let target = wcount.load(O::Acquire) + 2;
                    while wcount.load(O::Acquire) < target && !done.load(O::Acquire) {
                        std::thread::yield_now();
                    }
                    while !kids.is_empty() {
                        let k = kids.swap_remove(rng.below(kids.len() as u64) as usize);
                        let f = store.release_handle(k).freed_pages as u64;
                        if active.load(O::Acquire) > 0 {
                            freed += f;
                        }
                    }
                    crounds.fetch_add(1, O::Release);
                }
                freed
            })
        };
        let readers: Vec<_> = (0..READERS)
            .map(|r| {
                let (store, wal, committed, active, crounds) = (
                    store.clone(),
                    wal.clone(),
                    committed.clone(),
                    active.clone(),
                    crounds.clone(),
                );
                std::thread::spawn(move || {
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (r + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
                    let mut held: Vec<(BranchId, HashMap<u32, u64>)> =
                        (0..HELD).map(|_| fork_seeing(&store, &wal, &committed)).collect();
                    let mut buf = vec![0u8; PAGE];
                    // At least READS reads, and on while the churn has not yet run MIN_CHURN_ROUNDS
                    // rounds during them, up to 4 x READS (amendment 3h).
                    let mut i = 0;
                    while i < READS || (crounds.load(O::Acquire) < MIN_CHURN_ROUNDS && i < 4 * READS) {
                        if i % 500 == 499 {
                            let (old, _) = held.swap_remove(rng.below(HELD as u64) as usize);
                            store.release_handle(old);
                            held.push(fork_seeing(&store, &wal, &committed));
                        }
                        let (id, sees) = &held[rng.below(HELD as u64) as usize];
                        let page = rng.below(u64::from(PAGES)) as u32;
                        let snapshot = committed.read().unwrap();
                        let got = match store.resolve_into(*id, page, &mut buf).unwrap() {
                            Resolved::Filled => u64::from_le_bytes(buf[..8].try_into().unwrap()),
                            Resolved::Trunk(key) => {
                                store.fill_trunk_page(key, &image(snapshot[&page]));
                                snapshot[&page]
                            }
                        };
                        drop(snapshot);
                        assert_eq!(
                            got, sees[&page],
                            "k3 {mode:?}: reader {r} read {i}: branch {} read the wrong page {page}",
                            id.0
                        );
                        i += 1;
                    }
                    active.fetch_sub(1, O::Release);
                    for (id, _) in held {
                        store.release_handle(id);
                    }
                })
            })
            .collect();
        for r in readers {
            r.join().unwrap();
        }
        done.store(true, O::Release);
        let writes = writer.join().unwrap();
        let churn_freed = churner.join().unwrap();
        let stats = store.stats();
        assert!(
            writes > 0 && churn_freed > 0 && stats.work.resolve_trunk_rewritten > 1_000,
            "k3 {mode:?}: trunk writes {writes}, versions the churn freed while readers read {churn_freed}, \
             resolutions of rewritten trunk pages {}",
            stats.work.resolve_trunk_rewritten
        );
        // F5 takes the trunk's lock for every rewritten-page read, F-K3 for none, F-K3v only for the
        // searches that gave up.
        let expect_locked = match mode {
            K3Mode::Off => stats.work.resolve_trunk_rewritten,
            K3Mode::Ebr => 0,
            K3Mode::Olc => stats.k3_olc_fallbacks,
        };
        assert_eq!(stats.work.resolve_trunk_locked, expect_locked, "k3 {mode:?}: trunk-locked reads");
        // N6: the margin, printed for the run's record (`--nocapture`).
        println!(
            "race k3 {mode:?} always_fallback={always_fallback}: rewritten reads {}, locked {}, olc \
             restarts {}, head spins {}, fallbacks {}",
            stats.work.resolve_trunk_rewritten,
            stats.work.resolve_trunk_locked,
            stats.k3_olc_restarts,
            stats.k3_olc_head_spins,
            stats.k3_olc_fallbacks
        );
        if always_fallback {
            assert_eq!(
                (stats.k3_olc_fallbacks, stats.work.resolve_trunk_locked),
                (stats.work.resolve_trunk_rewritten, stats.work.resolve_trunk_rewritten),
                "every rewritten read must have fallen back and been answered under the lock"
            );
        } else {
            // F-K3v's concurrent validation must have run: a run where no reader ever met a node
            // that changed under it is VOID (a starved box can produce one), not a pass.
            assert!(
                mode != K3Mode::Olc || stats.k3_olc_restarts > 0,
                "k3 {mode:?}: no reader restarted, so the optimistic race was not run (VOID)"
            );
        }
        assert!(
            mode != K3Mode::Olc || stats.k3_olc_pool_in_use == stats.k3_nodes_live,
            "k3 {mode:?}: the pools lost track of removed nodes ({} handed out, {} in lists)",
            stats.k3_olc_pool_in_use,
            stats.k3_nodes_live
        );
        assert_eq!(stats.live_branches, 0, "k3 {mode:?}: branches leaked");
        assert_eq!(stats.arena_slots_in_use, 0, "k3 {mode:?}: slots leaked");
    }

    /// F-K3's garbage instrument, forced to fire and then shown to drain. While this thread holds an
    /// epoch guard, no node a reap removes can be freed (a reader pinned since before the removal
    /// might still reach it), so the lists' live nodes exceed the live versions by exactly the
    /// removals. Once the guard is gone and the epoch advances, that garbage is freed.
    #[test]
    fn k3_garbage_is_held_by_a_pinned_guard_and_drains_after_it() {
        let store = BranchStore::with_k3(K3Mode::Ebr);
        let pinned = epoch::pin();
        let mut removed = 0u64;
        for round in 0..20u64 {
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
            for page in 0..PAGES {
                store.first_write_trunk(page, &image(round));
            }
            removed += store.release_handle(id).freed_pages as u64;
        }
        let held = store.stats();
        assert_eq!(removed, 20 * u64::from(PAGES), "each round retains and frees one version per page");
        assert_eq!(held.trunk_slots_in_use, 0, "every version was reaped");
        assert_eq!(held.k3_nodes_live, removed, "a removed node was freed under a pinned guard");
        assert!(held.k3_node_bytes_live > 0);
        drop(pinned);
        let mut flushes = 0;
        while store.stats().k3_nodes_live > 0 && flushes < 1_000_000 {
            epoch::pin().flush();
            std::thread::yield_now();
            flushes += 1;
        }
        let drained = store.stats();
        assert_eq!(
            (drained.k3_nodes_live, drained.k3_node_bytes_live),
            (0, 0),
            "garbage outlived the guard by {flushes} flushes"
        );
    }

    /// F-K3v reuses a removed node at once: 1,000 rounds of retain-and-reap (six versions each)
    /// never hold more than six nodes, so every height's pool stays at one chunk and the pools' own
    /// count matches the lists' (a pool that deferred or leaked would grow chunks within ~230 rounds,
    /// and its count would drift). A REUSE
    /// test only: F-K3v's readers never touch the epoch, so the pinned guard here stalls nothing
    /// of theirs (PREREG amendment 3e, D3); the stalled-reader claim rests on reading plus
    /// `olc::tests::a_stalled_reader_holds_nothing`.
    #[test]
    fn olc_holds_no_garbage_under_a_pinned_guard() {
        let store = BranchStore::with_k3(K3Mode::Olc);
        let _pinned = epoch::pin();
        for round in 0..1_000u64 {
            let id = store.fork_trunk(Arc::new(Schema::default()), PAGE, 0).unwrap();
            for page in 0..PAGES {
                store.first_write_trunk(page, &image(round));
            }
            assert_eq!(store.release_handle(id).freed_pages, PAGES as usize);
            let s = store.stats();
            assert_eq!((s.k3_nodes_live, s.trunk_slots_in_use), (0, 0), "round {round}");
            // At most six nodes are ever live, so a pool that reuses keeps one chunk per height; one
            // that did not get its nodes back would pass 1,024 nodes of height 1 by round ~230.
            assert!(s.k3_olc_max_class_chunks <= 1, "round {round}: a node was not reused");
            assert_eq!(s.k3_olc_pool_in_use, s.k3_nodes_live, "round {round}: the pools lost track of a node");
            assert!(
                s.k3_node_bytes_live > 0 && s.k3_node_bytes_live <= s.k3_olc_pool_bound_bytes,
                "round {round}: pools {} B above the {} B their peak allows",
                s.k3_node_bytes_live,
                s.k3_olc_pool_bound_bytes
            );
        }
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
        for mode in K3Mode::ALL {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run_cache(seed, mode);
            }
        }
    }

    fn run_cache(seed: u64, mode: K3Mode) {
        let store = BranchStore::with_k3(mode);
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
                        "seed {seed:#x} k3 {mode:?} step {step}: branch {} read the wrong page {page}",
                        id.0
                    );
                }
            }
        }
        let work = store.stats().work;
        assert!(
            work.trunk_page_hits > 0 && childless_writes > 0 && emptied > 0,
            "seed {seed:#x} k3 {mode:?}: cache hits {}, trunk writes with no child {childless_writes}, times the \
             last child went {emptied}",
            work.trunk_page_hits
        );
        for (id, _) in live {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "seed {seed:#x} k3 {mode:?}: versions leaked");
    }
}
