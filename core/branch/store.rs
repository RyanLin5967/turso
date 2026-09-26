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
//! the trunk's last COMMIT of it came in an epoch at or before the branch's `trunk_at`: `written`
//! holds the epoch of each page's last commit, stamped at the commit's serialization point (see
//! "Trunk commits and forks" below), so a commit after the fork at `trunk_at` stamps its pages with
//! a later epoch before it publishes them, and no trunk write can change the version the key names —
//! the buffer tag with its LSN, where the LSN is the commit's epoch. The one gap is a trunk with no
//! live child: it writes without telling the store (see
//! [`BranchStore::begin_trunk_commit_with`]), so `written` can name an epoch whose page has since
//! changed.
//! The trunk's last child going therefore bumps the cache's generation, and a version cached under
//! an older generation is never served. The cache is filled by the pager after it reads a page for
//! a branch ([`BranchStore::fill_trunk_page`]), under the key [`BranchStore::resolve_into`] gave.
//!
//! # Trunk commits and forks — decisions at the commit, forks without the writer's lock
//!
//! A trunk fork used to take the trunk's WAL write lock, because a trunk write transaction took its
//! copy decisions at each page's first write, for the epoch of that moment: had a fork come in
//! between, the commit would have reached the new child. So every fork waited out every trunk
//! transaction from BEGIN to COMMIT, and every fork serialised with every other (sweep-lock L4).
//! Now the decisions are taken where an optimistic transaction takes its own — at the commit's
//! serialization point, against the epoch there (Silo, SOSP 2013):
//!
//! * **The writer** only CAPTURES, at a page's first write in the transaction, the page as it was
//!   (the pager's write set), and only while the trunk has a live child. After the frames are
//!   written and synced and before they are published,
//!   [`BranchStore::begin_trunk_commit_with`] takes every decision in one section under the
//!   trunk's lock — retain `[born, epoch)` if a live child forked in it, stamp `written` with
//!   `epoch` — and opens the COMMIT GATE (`trunk_commits` odd); dropping the [`TrunkCommitGate`]
//!   after the publication closes it. A rolled-back transaction leaves nothing in the store.
//! * **A fork** reads `trunk_commits` (waiting out an open gate), begins a WAL read transaction, and
//!   registers under the trunk's lock only if `trunk_commits` has not moved since: no commit has
//!   taken its decisions since the fork's snapshot, so every commit decided before the registration
//!   is in the snapshot (and published), and every commit decided after it sees the child and
//!   retains for it. The registration is the fork's linearisation point; it takes no WAL write lock.
//! * **The first child** is forked the old way, under the WAL write lock: a writer that saw no live
//!   child captured nothing, and must not see one appear before it commits. `trunk_children` goes
//!   from 0 to 1 only there, and a writer holds that lock from BEGIN to COMMIT.
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Bound, Deref, DerefMut};
use std::time::Instant;

use crossbeam_utils::CachePadded;

use super::arena::{Arena, Slot};
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
    /// The trunk epoch of its last commit of each page, readable without a lock (see [`Radix`]).
    /// Written only under `trunk`'s lock, by [`BranchStore::begin_trunk_commit_with`]. Absent reads
    /// as 0: "before the first fork that was live at the time", the conservative answer.
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
    /// a trunk fork that holds the trunk's WAL write lock (a fork without it refuses to be the first
    /// child); a trunk writer reading this holds the same lock. A 1-to-0 transition (a reap) racing
    /// the read only makes the writer capture a pre-image nobody needs. Changed only under the
    /// trunk's lock.
    trunk_children: AtomicUsize,
    /// Record each branch's page reads; the physical merge install's structural guard needs them.
    /// Read on every branch resolution, so it is an atomic rather than a field behind the trunk's
    /// lock.
    track_reads: AtomicBool,
    /// The commit gate: trunk commits that have taken their copy decisions, counted twice — odd
    /// while a commit is between its decisions and its publication (see "Trunk commits and forks").
    /// Advanced only by the trunk's one writer; the opening advance is under the trunk's lock.
    trunk_commits: AtomicU64,
    /// Whether [`take`] times how long each acquisition holds its lock. Off by default: it is the
    /// one part of the lock accounting that adds work inside a critical section.
    lock_timing: AtomicBool,
    /// Nanoseconds trunk forks held the trunk's WAL write lock, summed; written only while lock
    /// timing is on (see [`BranchWork::trunk_fork_wal_hold_ns`]). Observation only.
    fork_wal_hold_ns: AtomicU64,
    /// Test-only: run by a trunk fork right after its trunk-lock hold, where a trunk writer could
    /// read `trunk_children` (K10-7).
    #[cfg(test)]
    after_fork_hold: std::sync::Mutex<Option<Box<dyn Fn(&BranchStore) + Send>>>,
}

/// What a trunk fork did before it registered, for [`BranchWork`]'s `trunk_fork_*` counters.
/// Observation only.
#[derive(Clone, Copy, Default)]
pub(crate) struct ForkAttempts {
    /// WAL read transactions begun.
    pub(crate) read_txs: u64,
    /// WAL write-lock acquisitions.
    pub(crate) wal_locks: u64,
    /// Lock-free attempts abandoned because a trunk commit took its decisions in between.
    pub(crate) gate_retries: u64,
}

/// What a trunk fork's registration did.
pub(crate) enum TrunkFork {
    Forked(BranchId),
    /// A trunk commit took its copy decisions after the fork's snapshot, or was taking them: begin
    /// a new snapshot and try again.
    Retry,
    /// The trunk has no live child, so this fork would be the first: fork under the WAL write lock.
    NeedsWriterLock,
}

/// A trunk commit between its copy decisions and its publication. Dropping it closes the commit
/// gate: drop it once the commit is published (or has failed), never before.
pub(crate) struct TrunkCommitGate<'a> {
    store: &'a BranchStore,
}

impl Drop for TrunkCommitGate<'_> {
    fn drop(&mut self) {
        self.store.trunk_commits.fetch_add(1, Ordering::AcqRel);
    }
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
    /// What a merge validates against (see [`super::merge`]).
    merge: MergeState,
}

/// The trunk-side record a merge validates against, behind the trunk's lock. Every field is written
/// only while the trunk has a live child: a branch forked later has `trunk_at` at or above the epoch
/// of anything written before it, so nothing written while the trunk had no child can refuse it.
/// Every stamp is taken at a trunk commit's decision, with the epoch its page decisions use
/// ([`BranchStore::begin_trunk_commit_with`]), so a row stamp is above a child's `trunk_at`
/// exactly when the commit is outside that child's snapshot, for a fork with the WAL write lock or
/// without it (frontier/round11/r11-merge PREREG A15).
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
    /// Times read tracking was turned off (see [`BranchStore::set_track_reads`]). A branch forked
    /// while tracking was on, at this count, has had every read tracked if the count has not moved.
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
            self.guard.work().lock_hold_ns += since.elapsed().as_nanos() as u64;
        }
    }
}

/// Take `lock`, counting the acquisition into the structure it guards: every one, the ones that
/// found the lock held, and how long those waited. The counts are written under the lock itself,
/// so counting adds no shared write the lock does not already make, and the clock is read only on
/// the contended path, by the thread that is waiting anyway — except with `timed`, which reads it
/// once more at the acquisition and once at the release.
fn take<T: Counted>(lock: &Mutex<T>, timed: bool) -> Held<'_, T> {
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
    let since = timed.then(Instant::now);
    Held { guard, since }
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
    fn child_gone(&mut self, f: u64, arena: &mut Domain, work: &mut BranchWork) -> usize {
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
                lineage: Lineage::default(),
                domain: Domain::new(TRUNK_DOMAIN),
                work: BranchWork::default(),
                merge: MergeState::default(),
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
            track_reads: AtomicBool::new(false),
            trunk_commits: AtomicU64::new(0),
            lock_timing: AtomicBool::new(false),
            fork_wal_hold_ns: AtomicU64::new(0),
            #[cfg(test)]
            after_fork_hold: std::sync::Mutex::new(None),
        }
    }

    /// Whether lock timing is on (see [`BranchWork::lock_hold_ns`]).
    pub(crate) fn timed(&self) -> bool {
        self.lock_timing.load(Ordering::Relaxed)
    }

    /// Add one trunk fork's WAL write-lock hold to [`BranchWork::trunk_fork_wal_hold_ns`]. Called
    /// only while lock timing is on.
    pub(crate) fn add_fork_wal_hold(&self, ns: u64) {
        self.fork_wal_hold_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// The shard of branch `id`, locked.
    fn shard(&self, id: BranchId) -> Held<'_, Shard> {
        take(&self.shards[shard_of(id)], self.timed())
    }

    fn trunk(&self) -> Held<'_, TrunkInner> {
        take(&self.trunk, self.timed())
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

    /// The commit gate's count, for a lock-free trunk fork to hand back to [`Self::fork_trunk`]. Odd
    /// while a trunk commit is between its copy decisions and its publication.
    pub(crate) fn trunk_commit_seq(&self) -> u64 {
        self.trunk_commits.load(Ordering::Acquire)
    }

    /// Fork a child of the trunk, the caller holding a WAL read snapshot taken after it read
    /// `seen` from [`Self::trunk_commit_seq`] (an even count). Registered only if no trunk commit
    /// has taken its copy decisions since `seen` — so every commit decided before this registration
    /// is in the caller's snapshot, and every one decided after it retains what the child sees —
    /// and only if the trunk already has a live child (see "Trunk commits and forks"). `seen: None`
    /// means the caller holds the trunk's WAL write lock instead, and the fork always registers.
    ///
    /// The child is listed among the trunk's children before its state exists in its shard. Nothing
    /// can reach it in between: its id has not been returned, and a sibling's reap only reads its
    /// fork epoch, which is final.
    pub(crate) fn fork_trunk(
        &self,
        schema: Arc<Schema>,
        page_size: usize,
        reserved_space: u8,
        seen: Option<u64>,
        attempts: ForkAttempts,
    ) -> Result<TrunkFork> {
        let format = *self.trunk_format.get_or_init(|| (page_size, reserved_space));
        if format != (page_size, reserved_space) {
            return Err(LimboError::InternalError(format!(
                "branches were forked from {}-byte pages with {} reserved bytes, but the database \
                 now has {page_size} and {reserved_space}",
                format.0, format.1
            )));
        }
        let (id, f, commits_at_fork, reads_from) = {
            let mut trunk = self.trunk();
            if let Some(seen) = seen {
                if self.trunk_children.load(Ordering::Acquire) == 0 {
                    return Ok(TrunkFork::NeedsWriterLock);
                }
                // An odd count was read inside a commit's gate: the snapshot may or may not hold
                // that commit, so it cannot be told which side of it the child is on.
                if seen % 2 == 1 || self.trunk_commits.load(Ordering::Acquire) != seen {
                    return Ok(TrunkFork::Retry);
                }
            }
            let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
            let f = trunk.lineage.epoch;
            trunk.lineage.epoch += 1;
            trunk.lineage.children.insert(f, id);
            // V0's count, read in the same hold as the registration: a commit decided before it is
            // in the child's snapshot, one decided after it is not.
            let commits_at_fork = trunk.merge.trunk_commits;
            // Whether every read of this child will be tracked: the flag and the count read in the
            // hold that `set_track_reads` changes them in.
            let reads_from = self
                .track_reads
                .load(Ordering::SeqCst)
                .then_some(trunk.merge.track_off);
            self.trunk_children.fetch_add(1, Ordering::AcqRel);
            let work = &mut trunk.work;
            work.trunk_forks += 1;
            if seen.is_some() {
                work.trunk_forks_fast += 1;
            } else {
                work.trunk_forks_locked += 1;
            }
            work.trunk_fork_read_txs += attempts.read_txs;
            work.trunk_fork_wal_locks += attempts.wal_locks;
            work.trunk_fork_gate_retries += attempts.gate_retries;
            (id, f, commits_at_fork, reads_from)
        };
        #[cfg(test)]
        if let Some(hook) = self.after_fork_hold.lock().unwrap().as_ref() {
            hook(self);
        }
        self.live.fetch_add(1, Ordering::AcqRel);
        let mut st = BranchState::new(BranchId::TRUNK, f, schema, f, PageMap::default());
        st.commits_at_fork = commits_at_fork;
        st.reads_from = reads_from;
        self.shard(id).branches.insert(id, st);
        Ok(TrunkFork::Forked(id))
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

    /// The copy decisions of one trunk commit, taken at its serialization point: after its frames
    /// are written and before they are published. `pages` is every page the commit writes, each with
    /// the pre-image the pager captured at the page's first write in the transaction, or `None` if
    /// the trunk had no live child then. For each page last committed before this epoch: if a live
    /// child forked since then can see the version being overwritten, keep a copy of it for that
    /// child, and stamp `written` with this epoch.
    ///
    /// The decisions are taken against the epoch NOW, so a fork registered at any moment of the
    /// transaction is seen. `written` is stamped before the commit is published, so a reader whose
    /// WAL snapshot holds the new version also sees the new epoch and looks the page up under the
    /// trunk's lock (see [`BranchStore::resolve_into`]). The returned gate keeps lock-free forks from
    /// registering until it is dropped; the caller drops it once the commit is published.
    ///
    /// A page without a pre-image was first written while the trunk had no live child; the trunk's
    /// WAL write lock, which the writer holds, keeps a first child from being forked before the
    /// commit, so no live child can see what it overwrites — asserted, as the writer tells the store
    /// nothing else about that page (the cache's generation covers it; see "Trunk pages without the
    /// file").
    #[cfg(test)]
    pub(crate) fn begin_trunk_commit<'a, 'p>(
        &'a self,
        pages: impl IntoIterator<Item = (u32, Option<&'p [u8]>)>,
    ) -> TrunkCommitGate<'a> {
        self.begin_trunk_commit_with(pages, TrunkPending::default())
    }

    /// The copy decisions of `begin_trunk_commit` (above), also taking the merge record's decisions
    /// for the commit in the same hold, at the same epoch: `tx`'s rows and tables are stamped with
    /// it, and a commit that wrote a page while the trunk had a child is one trunk commit (V0) and
    /// one log entry (V1).
    /// A child forked before this hold has `trunk_at` below the epoch, so it is refused on these
    /// rows, and its pages are retained for it here too; a child forked after it sees the commit.
    pub(crate) fn begin_trunk_commit_with<'a, 'p>(
        &'a self,
        pages: impl IntoIterator<Item = (u32, Option<&'p [u8]>)>,
        tx: TrunkPending,
    ) -> TrunkCommitGate<'a> {
        let mut trunk = self.trunk();
        let opened = self.trunk_commits.fetch_add(1, Ordering::AcqRel);
        crate::turso_assert!(opened % 2 == 0, "two trunk commits inside the commit gate at once");
        let gate = TrunkCommitGate { store: self };
        let TrunkInner {
            lineage,
            domain,
            work,
            merge,
        } = &mut *trunk;
        let epoch = lineage.epoch;
        work.trunk_commits_decided += 1;
        // U14's instrument (observation only, frontier/round11/r11-merge PREREG A17): this hold's
        // time split into the merge record's row stamps, the page decisions and the prune, read
        // only while lock timing is on, so the untimed path adds no clock read.
        let timed = self.timed();
        let t_rows = timed.then(Instant::now);
        work.trunk_commit_rows_stamped += tx.rows.len() as u64;
        for (root, rowid) in tx.rows {
            merge.stamp_row(root, rowid, epoch);
        }
        for root in tx.tables {
            merge.table_stamps.insert(root, epoch);
        }
        if !tx.pages.is_empty() {
            let mut logged: Vec<u32> = tx.pages.into_iter().collect();
            logged.sort_unstable();
            merge.trunk_commits += 1;
            work.trunk_commits += 1;
            merge.log.push_back((epoch, logged));
        }
        let t_pages = timed.then(Instant::now);
        for (page, pre_image) in pages {
            work.trunk_commit_pages_decided += 1;
            let born = self.written(page);
            let Some(pre_image) = pre_image else {
                crate::turso_assert!(
                    !lineage.has_child_in(born, epoch),
                    "a trunk commit overwrites a page a live child can see, and no pre-image was \
                     captured for it"
                );
                continue;
            };
            work.trunk_pre_images_captured += 1;
            if born >= epoch {
                continue;
            }
            if lineage.has_child_in(born, epoch) {
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
                work.trunk_pre_images_retained += 1;
            }
            self.written.get_or_insert(page).store(epoch, Ordering::Release);
        }
        let t_prune = timed.then(Instant::now);
        // Pruned here, in the hold the commit already takes, not in a second one after it.
        merge.prune(lineage.children.keys().next().copied());
        if let (Some(r), Some(p), Some(q)) = (t_rows, t_pages, t_prune) {
            work.trunk_commit_row_ns += p.duration_since(r).as_nanos() as u64;
            work.trunk_commit_page_ns += q.duration_since(p).as_nanos() as u64;
            work.trunk_commit_prune_ns += q.elapsed().as_nanos() as u64;
        }
        gate
    }

    /// The copy decision for one page, as a one-page trunk commit: for the store's model tests,
    /// which drive the store without a pager.
    #[cfg(test)]
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) {
        drop(self.begin_trunk_commit([(page, Some(pre_image))]));
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
        let mut shard = self.shard(id);
        let st = shard.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        st.schema = schema;
        st.ddl = true;
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
        let (at, elsewhere) = {
            let mut shard = self.shard(id);
            shard.work.resolve_calls += 1;
            shard.work.resolve_levels += 1;
            if self.track_reads.load(Ordering::Relaxed) {
                if let Some(st) = shard.branches.get_mut(&id) {
                    st.reads.insert(page);
                }
            }
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
                    (at, None)
                }
            }
        };
        if let Some(slot) = elsewhere {
            let d = domain_of(slot);
            crate::turso_assert!(d < SHARDS, "a branch's page map named a trunk slot");
            out.copy_from_slice(take(&self.shards[d], self.timed()).domain.page(slot));
            return Ok(Resolved::Filled);
        }
        let mut trunk = self.trunk();
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
            let trunk = self.trunk();
            add(&mut stats, &trunk.work, &trunk.domain);
            stats.work.trunk_lock_acquisitions = trunk.work.lock_acquisitions;
            stats.work.trunk_lock_contended = trunk.work.lock_contended;
            stats.work.trunk_lock_wait_ns = trunk.work.lock_wait_ns;
            stats.work.trunk_lock_hold_ns = trunk.work.lock_hold_ns;
            stats.merge_log_entries = trunk.merge.log.len();
            stats.merge_log_pages = trunk.merge.log.iter().map(|(_, p)| p.len()).sum();
            stats.row_stamps = trunk.merge.row_stamps.len();
        }
        stats.work.trunk_fork_wal_hold_ns = self.fork_wal_hold_ns.load(Ordering::Relaxed);
        for lock in self.shards.iter() {
            let shard = take(lock, self.timed());
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
        let mut slots: Vec<u32> = self.trunk().domain.slots_in_use().collect();
        for lock in self.shards.iter() {
            slots.extend(take(lock, self.timed()).domain.slots_in_use());
        }
        slots
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        match domain_of(slot) {
            TRUNK_DOMAIN => self.trunk().domain.is_free(slot),
            d if d < SHARDS => take(&self.shards[d], self.timed()).domain.is_free(slot),
            _ => false,
        }
    }

    /// A table cursor on branch `id` wrote or deleted `rowid` in the b-tree rooted at `root`.
    pub(crate) fn branch_row_written(&self, id: BranchId, root: i64, rowid: i64) {
        if let Some(st) = self.shard(id).branches.get_mut(&id) {
            st.row_seq += 1;
            st.rows.insert((root, rowid), st.row_seq);
        }
    }

    /// An index cursor on branch `id` wrote the b-tree rooted at `root`.
    pub(crate) fn branch_index_written(&self, id: BranchId, root: i64) {
        if let Some(st) = self.shard(id).branches.get_mut(&id) {
            st.index_roots.insert(root);
        }
    }

    /// Branch `id` wrote a table without naming the rows.
    pub(crate) fn branch_bulk_written(&self, id: BranchId) {
        if let Some(st) = self.shard(id).branches.get_mut(&id) {
            st.bulk = true;
        }
    }

    /// Mutant 30 only: stamp a trunk row at WRITE time, with the epoch current then, as the store
    /// did before stamps moved to the commit (frontier/round11/r11-merge PREREG A15). Under
    /// lock-free forks this is the lost update A15 fixes.
    pub(crate) fn trunk_row_written(&self, root: i64, rowid: i64) {
        self.trunk_rows_written(&[(root, rowid)]);
    }

    /// Mutant 30 only: [`Self::trunk_row_written`] for several rows.
    pub(crate) fn trunk_rows_written(&self, rows: &[(i64, i64)]) {
        let mut trunk = self.trunk();
        let epoch = trunk.lineage.epoch;
        for &(root, rowid) in rows {
            trunk.merge.stamp_row(root, rowid, epoch);
        }
    }

    /// Mutant 30 only: a table stamp at write time.
    pub(crate) fn trunk_bulk_written(&self, root: i64) {
        let mut trunk = self.trunk();
        let epoch = trunk.lineage.epoch;
        trunk.merge.table_stamps.insert(root, epoch);
    }

    /// The trunk write transaction in progress committed (`Some`) or rolled back (`None`). Its
    /// writes were normally handed to [`Self::begin_trunk_commit_with`] at the commit's decision,
    /// so `tx` is empty here; what is left (a commit that published without passing the decision)
    /// is stamped now, with an epoch at or above the decision's, which can only refuse more. A
    /// rolled-back transaction stamps nothing. With nothing to stamp the trunk's lock is not taken:
    /// the decision already pruned.
    pub(crate) fn trunk_tx_end(&self, committed: Option<TrunkPending>) {
        let Some(tx) = committed else {
            return;
        };
        if tx.pages.is_empty() && tx.rows.is_empty() && tx.tables.is_empty() {
            return;
        }
        let mut trunk = self.trunk();
        let oldest = trunk.lineage.children.keys().next().copied();
        let epoch = trunk.lineage.epoch;
        let TrunkInner { merge, work, .. } = &mut *trunk;
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
        merge.prune(oldest);
    }

    pub(crate) fn set_track_reads(&self, on: bool) {
        // Under the trunk's lock, where a fork reads the flag and the count together.
        let mut trunk = self.trunk();
        let was = self.track_reads.swap(on, Ordering::SeqCst);
        if was && !on {
            trunk.merge.track_off += 1;
        }
    }

    pub(crate) fn tracks_reads(&self) -> bool {
        self.track_reads.load(Ordering::Relaxed)
    }

    /// Everything a merge of `id` needs from the store: the scope check, every validator's verdict
    /// (the active one's probes counted), and what to install. `batch` is what the batch's earlier
    /// members wrote in the transaction in progress, which no stamp holds until it commits.
    ///
    /// Two holds, never at once (see "Two branches, two locks"): the branch's data is copied out
    /// under its shard's lock, then validated under the trunk's. Nothing moves in between: the
    /// branch has no open connection (or the merge is refused as out of scope) and the merger holds
    /// its handle, and the merger holds the trunk's WAL write lock, so no trunk commit can decide.
    /// A lock-free fork can register in between; it only advances the epoch, which no verdict
    /// reads for this branch.
    pub(super) fn merge_prepare(
        &self,
        id: BranchId,
        active: super::merge::Validation,
        physical: bool,
        batch: &TrunkPending,
    ) -> Result<super::merge::Prepared> {
        use super::merge::{Prepared, Validation};
        struct Copied {
            scope: Option<&'static str>,
            at: u64,
            commits_at_fork: u64,
            reads_from: Option<u64>,
            current: Vec<u32>,
            pages: Vec<(u32, Vec<u8>)>,
            rows: Vec<((i64, i64), u64)>,
            index_roots: Vec<i64>,
            reads: Vec<u32>,
            schema: Arc<Schema>,
        }
        let b = {
            let shard = self.shard(id);
            let st = shard.branches.get(&id).ok_or_else(|| gone(id))?;
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
            } else {
                None
            };
            let pages = if physical {
                let mut pages = Vec::with_capacity(st.current.len());
                for (&p, owned) in &st.current {
                    // A trunk child's own pages come from its own shard's domain.
                    crate::turso_assert!(
                        domain_of(owned.slot) == shard_of(id),
                        "a trunk child's page lives in another shard's domain"
                    );
                    pages.push((p, shard.domain.page(owned.slot).to_vec()));
                }
                pages.sort_unstable_by_key(|(p, _)| *p);
                pages
            } else {
                Vec::new()
            };
            Copied {
                scope,
                at: st.trunk_at,
                commits_at_fork: st.commits_at_fork,
                reads_from: st.reads_from,
                current: st.current.keys().copied().collect(),
                pages,
                rows: st.rows.iter().map(|(&k, &s)| (k, s)).collect(),
                index_roots: st.index_roots.iter().copied().collect(),
                reads: st.reads.iter().copied().collect(),
                schema: st.schema.clone(),
            }
        };
        let mut trunk = self.trunk();
        let oldest = trunk.lineage.children.keys().next().copied();
        trunk.merge.prune(oldest);
        let TrunkInner {
            lineage,
            domain,
            work,
            merge,
        } = &mut *trunk;
        work.merge_attempts += 1;
        // The last scope clause needs the trunk's count of tracking turned off.
        let scope = b.scope.or_else(|| {
            (physical && b.reads_from != Some(merge.track_off)).then_some(
                "the branch's reads were not all tracked, which the physical install needs",
            )
        });
        let at = b.at;
        let written = |page: u32| self.written(page);
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
        let commits_since_fork = merge.trunk_commits - b.commits_at_fork;
        count(Validation::Scalar, &mut probes);
        let scalar = commits_since_fork > u64::from(mutant(1))
            || (!batch.pages.is_empty() && !mutant(24));
        // V2: any page this branch wrote that the trunk wrote after the fork, or that an earlier
        // batch member wrote (a page stamp is taken only at the commit's decision).
        let mut page = false;
        for &p in &b.current {
            count(Validation::PageStamp, &mut probes);
            if written(p) + u64::from(mutant(9)) > at + m2 || batch.pages.contains(&p) {
                page = true;
                break;
            }
        }
        // V3: any row this branch wrote that the trunk wrote after the fork, or that an earlier
        // batch member wrote.
        let mut key = false;
        for &((root, rowid), _) in &b.rows {
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
        let current: HashSet<u32> = b.current.iter().copied().collect();
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
                    if current.contains(p) {
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
            for &q in &b.reads {
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
                let Some(slot) = lineage.retained_at(q, at, &mut unused) else {
                    // Written after the fork while this branch lived, so a version was retained;
                    // not finding one means the bookkeeping is broken, and guessing is unsound.
                    return true;
                };
                let base = domain.page(slot);
                let kind = base[if q == 1 { 100 } else { 0 }];
                if kind == 0x02 || kind == 0x05 {
                    return true;
                }
            }
            false
        });
        drop(trunk);
        // The replay applies rows in the branch's last-write order: a UNIQUE value the branch moved
        // from one row to another must leave the first before it reaches the second.
        let mut ordered = b.rows;
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
            pages_written: b.current.len(),
            pages: b.pages,
            rows,
            index_roots: b.index_roots,
            schema: b.schema,
        })
    }

    /// Count a merge's outcome.
    pub(super) fn merge_counted(&self, f: impl FnOnce(&mut BranchWork)) {
        f(&mut self.trunk().work);
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
                let mut trunk = self.trunk();
                let TrunkInner {
                    lineage,
                    domain,
                    work,
                    ..
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

    /// A trunk fork as the pager makes it under the trunk's WAL write lock: it always registers.
    fn fork_locked(store: &BranchStore) -> BranchId {
        match store
            .fork_trunk(Arc::new(Schema::default()), PAGE, 0, None, ForkAttempts::default())
            .unwrap()
        {
            TrunkFork::Forked(id) => id,
            _ => unreachable!("a fork under the WAL write lock always registers"),
        }
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
                    let id = fork_locked(&store);
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
        let per_call = SHARDS as u64 + 1;
        assert_eq!(quiet.lock_acquisitions - base.lock_acquisitions, 11 * per_call);
        assert_eq!(quiet.trunk_lock_acquisitions - base.trunk_lock_acquisitions, 11);
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
                let held = store.trunk();
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
                    let id = fork_locked(&store);
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
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((w + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)));
                    let mut nodes: Vec<Node> = Vec::new();
                    let (mut cross_shard, mut deferred) = (0u64, 0u64);
                    let mut buf = vec![0u8; PAGE];
                    for step in 0..STEPS {
                        let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
                        match rng.below(10) {
                            0..=1 if live.len() < 16 => {
                                let _w = wal.lock().unwrap();
                                let sees = committed.read().unwrap().clone();
                                let id = fork_locked(&store);
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
                    let id = fork_locked(&store);
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

    /// A fork without the WAL write lock (r11-forklock PREREG §0, K10-4): it registers only between
    /// two commits' gates, against the count it read before its snapshot, and never as the trunk's
    /// first child. Each refusal is forced to fire, and each admission shown after it.
    #[test]
    fn a_lock_free_fork_registers_only_between_commit_gates_and_never_as_the_first_child() {
        let store = BranchStore::new();
        let fast = |seen: u64| {
            store
                .fork_trunk(Arc::new(Schema::default()), PAGE, 0, Some(seen), ForkAttempts::default())
                .unwrap()
        };
        assert!(
            matches!(fast(store.trunk_commit_seq()), TrunkFork::NeedsWriterLock),
            "a lock-free fork registered as the trunk's first child"
        );
        let first = fork_locked(&store);
        let seen = store.trunk_commit_seq();
        assert_eq!(seen % 2, 0, "the gate is open with no commit in flight");
        let TrunkFork::Forked(a) = fast(seen) else {
            panic!("a lock-free fork between commits was refused");
        };

        let gate = store.begin_trunk_commit([(1, Some(&image(0)[..]))]);
        let inside = store.trunk_commit_seq();
        assert_eq!(inside, seen + 1, "a commit's decisions did not open the gate");
        assert!(
            matches!(fast(seen), TrunkFork::Retry),
            "a fork registered against a snapshot taken before a commit's decisions"
        );
        assert!(
            matches!(fast(inside), TrunkFork::Retry),
            "a fork registered inside a commit's gate"
        );
        drop(gate);
        let after = store.trunk_commit_seq();
        assert_eq!(after, seen + 2, "the gate did not close");
        assert!(
            matches!(fast(seen), TrunkFork::Retry),
            "a fork registered against a snapshot taken before a published commit"
        );
        let TrunkFork::Forked(b) = fast(after) else {
            panic!("a lock-free fork after the commit was refused");
        };
        let work = store.stats().work;
        assert_eq!(
            (work.trunk_forks, work.trunk_forks_fast, work.trunk_forks_locked),
            (3, 2, 1)
        );
        for id in [first, a, b] {
            store.release_handle(id);
        }
    }

    /// The copy decisions of a trunk transaction are taken at its commit, against the epoch there
    /// (K10-1 and K10-2 at the store's level). A child forked after the writer captured a page and
    /// before its commit reads the pre-image, both when the page's last commit was in an earlier
    /// epoch and when it was in the epoch just before this one; a child forked after the commit
    /// reads the trunk's new version.
    #[test]
    fn a_trunk_commit_decides_against_the_epoch_at_its_commit_not_at_its_first_write() {
        let store = BranchStore::new();
        let first = fork_locked(&store);
        let mut buf = vec![0u8; PAGE];
        let mut read = |id: BranchId| match store.resolve_into(id, 3, &mut buf).unwrap() {
            Resolved::Filled => Some(u64::from_le_bytes(buf[..8].try_into().unwrap())),
            Resolved::Trunk(_) => None,
        };
        let fast = || {
            let seen = store.trunk_commit_seq();
            match store
                .fork_trunk(Arc::new(Schema::default()), PAGE, 0, Some(seen), ForkAttempts::default())
                .unwrap()
            {
                TrunkFork::Forked(id) => id,
                _ => panic!("a lock-free fork between commits was refused"),
            }
        };
        // C0: page 3 goes from generation 0 to 1 in epoch 1; `first` keeps generation 0.
        drop(store.begin_trunk_commit([(3, Some(&image(0)[..]))]));
        // C1: the writer captures generation 1, then a child forks, then C1 commits generation 2.
        let captured = image(1);
        let x = fast();
        drop(store.begin_trunk_commit([(3, Some(&captured[..]))]));
        // C2: the page's last commit is two epochs back; again a child forks mid-transaction.
        let captured2 = image(2);
        let y = fast();
        let z = fast();
        drop(store.begin_trunk_commit([(3, Some(&captured2[..]))]));
        let after = fast();
        assert_eq!(read(first), Some(0), "the child forked before C0 lost its version");
        assert_eq!(read(x), Some(1), "a child forked inside C1 did not get C1's pre-image");
        assert_eq!(read(y), Some(2), "a child forked inside C2 did not get C2's pre-image");
        assert_eq!(read(z), Some(2), "a child forked inside C2 did not get C2's pre-image");
        assert_eq!(read(after), None, "a child forked after C2 does not read the trunk's page");
        let work = store.stats().work;
        assert_eq!(work.trunk_commits_decided, 3);
        assert_eq!(work.trunk_pre_images_retained, 3, "one retained version per commit");
        for id in [first, x, y, z, after] {
            store.release_handle(id);
        }
        assert_eq!(store.stats().arena_slots_in_use, 0, "versions leaked");
    }

    /// Lock-free forks racing trunk commits under threads (K10). A writer runs transactions of one to
    /// three pages under a stand-in for the WAL write lock (`wal`): it captures each page's pre-image
    /// at its first write while the trunk has a live child, yields (so forks land inside the
    /// transaction), takes the commit's decisions, publishes under the stand-in for the WAL snapshot
    /// (`committed`), yields inside the gate, and closes it. Four fork threads fork without `wal`
    /// (under it only when the store answers `NeedsWriterLock`), each taking its snapshot after
    /// reading the gate's count as the pager does, and check every page of every child they hold —
    /// resolving and reading under one snapshot, filling the trunk-page cache on a miss — against
    /// the snapshot the child was forked from, then reap some. The shapes the protocol exists for
    /// must occur: forks inside an open transaction, retained pre-images, and lock-free forks.
    #[test]
    fn lock_free_forks_racing_trunk_commits_read_every_fork_as_it_was() {
        use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64};
        use std::sync::{Mutex as StdMutex, RwLock};
        const FORKERS: u64 = 4;
        const FORKS: usize = 300;
        let store = Arc::new(BranchStore::new());
        let wal = Arc::new(StdMutex::new(()));
        let committed = Arc::new(RwLock::new(
            (0..PAGES).map(|p| (p, 0u64)).collect::<HashMap<u32, u64>>(),
        ));
        let done = Arc::new(StdBool::new(false));
        let mid_txn_forks = Arc::new(StdU64::new(0));
        let writer = {
            let (store, wal, committed, done, mid) = (
                store.clone(),
                wal.clone(),
                committed.clone(),
                done.clone(),
                mid_txn_forks.clone(),
            );
            std::thread::spawn(move || {
                let mut rng = Rng(0x5851_F42D_4C95_7F2D);
                let mut generation = 1u64;
                while !done.load(std::sync::atomic::Ordering::Acquire) {
                    let _w = wal.lock().unwrap();
                    let forks_before = store.stats().work.trunk_forks;
                    let mut captured: HashMap<u32, Vec<u8>> = HashMap::new();
                    let mut pages = Vec::new();
                    for _ in 0..=rng.below(3) {
                        let page = rng.below(u64::from(PAGES)) as u32;
                        if !pages.contains(&page) {
                            pages.push(page);
                            if store.trunk_has_children() {
                                captured.insert(page, image(committed.read().unwrap()[&page]));
                            }
                        }
                        std::thread::yield_now();
                    }
                    let gate = store.begin_trunk_commit(
                        pages
                            .iter()
                            .map(|&p| (p, captured.get(&p).map(|b| &b[..]))),
                    );
                    if store.stats().work.trunk_forks > forks_before {
                        mid.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    {
                        let mut c = committed.write().unwrap();
                        for &p in &pages {
                            c.insert(p, generation);
                            generation += 1;
                        }
                    }
                    std::thread::yield_now();
                    drop(gate);
                    drop(_w);
                    std::thread::yield_now();
                }
            })
        };
        let forkers: Vec<_> = (0..FORKERS)
            .map(|w| {
                let (store, wal, committed) = (store.clone(), wal.clone(), committed.clone());
                std::thread::spawn(move || {
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((w + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)));
                    let mut held: Vec<(BranchId, HashMap<u32, u64>)> = Vec::new();
                    let mut buf = vec![0u8; PAGE];
                    let check = |id: BranchId, sees: &HashMap<u32, u64>, buf: &mut Vec<u8>| {
                        for page in 0..PAGES {
                            let snap = committed.read().unwrap();
                            let got = match store.resolve_into(id, page, buf).unwrap() {
                                Resolved::Filled => u64::from_le_bytes(buf[..8].try_into().unwrap()),
                                Resolved::Trunk(key) => {
                                    store.fill_trunk_page(key, &image(snap[&page]));
                                    snap[&page]
                                }
                            };
                            drop(snap);
                            assert_eq!(got, sees[&page], "child {} page {page}", id.0);
                        }
                    };
                    for _ in 0..FORKS {
                        let (id, sees) = loop {
                            let seen = store.trunk_commit_seq();
                            if seen % 2 == 1 {
                                std::thread::yield_now();
                                continue;
                            }
                            let sees = committed.read().unwrap().clone();
                            match store
                                .fork_trunk(
                                    Arc::new(Schema::default()),
                                    PAGE,
                                    0,
                                    Some(seen),
                                    ForkAttempts::default(),
                                )
                                .unwrap()
                            {
                                TrunkFork::Forked(id) => break (id, sees),
                                TrunkFork::Retry => std::thread::yield_now(),
                                TrunkFork::NeedsWriterLock => {
                                    let _w = wal.lock().unwrap();
                                    let sees = committed.read().unwrap().clone();
                                    break (fork_locked(&store), sees);
                                }
                            }
                        };
                        check(id, &sees, &mut buf);
                        held.push((id, sees));
                        let i = rng.below(held.len() as u64) as usize;
                        check(held[i].0, &held[i].1, &mut buf);
                        if held.len() > 6 || rng.below(4) == 0 {
                            let (id, _) = held.swap_remove(rng.below(held.len() as u64) as usize);
                            store.release_handle(id);
                        }
                    }
                    for (id, sees) in &held {
                        check(*id, sees, &mut buf);
                    }
                    for (id, _) in held {
                        store.release_handle(id);
                    }
                })
            })
            .collect();
        for f in forkers {
            f.join().unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::Release);
        writer.join().unwrap();
        let work = store.stats().work;
        assert_eq!(work.trunk_forks, FORKERS * FORKS as u64);
        assert!(work.trunk_forks_fast > 0, "no fork took the lock-free path: {work:?}");
        assert!(work.trunk_pre_images_retained > 0, "no commit retained a pre-image: {work:?}");
        assert!(
            mid_txn_forks.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "no fork landed inside an open trunk transaction"
        );
        assert_eq!(store.stats().live_branches, 0, "branches leaked");
        assert_eq!(store.stats().arena_slots_in_use, 0, "versions leaked");
    }

    /// K10-7 (F6 condition (i), lead item 17:05Z): outside the trunk's lock a listed trunk child is
    /// always counted in `trunk_children`, which a trunk writer reads without that lock to decide
    /// whether to capture pre-images. A hook run right after each fork's trunk-lock hold asserts it,
    /// for the first child (forked under the WAL write lock) and for a lock-free one.
    #[test]
    fn a_listed_trunk_child_is_counted_the_moment_the_trunk_lock_is_released() {
        let store = BranchStore::new();
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let ran = ran.clone();
            *store.after_fork_hold.lock().unwrap() = Some(Box::new(move |s: &BranchStore| {
                assert!(
                    s.trunk_has_children(),
                    "a trunk child is listed and not counted once the trunk's lock is released"
                );
                ran.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }
        let first = fork_locked(&store);
        let seen = store.trunk_commit_seq();
        let TrunkFork::Forked(second) = store
            .fork_trunk(Arc::new(Schema::default()), PAGE, 0, Some(seen), ForkAttempts::default())
            .unwrap()
        else {
            panic!("a lock-free fork between commits was refused");
        };
        assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 2, "the hook did not run");
        *store.after_fork_hold.lock().unwrap() = None;
        store.release_handle(first);
        store.release_handle(second);
    }
}
