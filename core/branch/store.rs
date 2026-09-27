//! Per-branch page spaces, the rule that decides which version of a page a branch sees, and — for
//! durable branches — how that state is logged and recovered. ⚠ The durability half is UNBUILT.
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
//! # Resolution without the walk — the page map (round 10's F4, ported from turso `a31198dd8`)
//!
//! Steps 2 and 3 are answered for every level between a branch and the trunk at once, and frozen,
//! at the moment the branch forks: nothing a branch sees through its ancestors can change after its
//! fork (an ancestor's later commit retains the version the branch saw, in the same slot, and a
//! released interior's retirement keeps every version a live child forked inside). So each branch
//! carries `inherited`, a persistent [`PageMap`] of every arena page it sees through its ancestors,
//! which is its parent's `view` at the fork: the parent's own `inherited` plus the parent's current
//! pages, kept up to date by the parent's commits once it has forked a child. A fork clones the
//! parent's `view` in O(1) and a commit path-copies O(log P) trie nodes, so a lookup costs the same
//! at depth 1000 as at depth 1. A page no branch in the chain wrote is the trunk's, as of
//! `trunk_at`, the fork epoch at which the branch's ancestry leaves the trunk.
//!
//! The maps are derived state, like the retained indexes: replay applies `Fork` and `Commit`
//! records through the same code that builds them live, and after `load_snapshot` (which replays no
//! fork) `derive_page_maps` rebuilds every `inherited` from the recovered lineages, parents before
//! children. The log and snapshot formats do not change. In catalog mode `ensure` derives the maps
//! of a branch the first time it makes the branch resident, from its parent made resident first:
//! the same derivation, one branch at a time (a12-durable-open lane).
//!
//! # Where the copies come from — the write ticket
//!
//! Every copy decision is taken at the first [`crate::storage::pager::Pager::add_dirty`] of a
//! page in a transaction, the only place a [`crate::storage::pager::WriteTicket`] is minted:
//!
//! * on a **branch**, the decision reserves a FRESH slot in the branch's own page space. The commit
//!   writes the page into it and only then moves the branch's map to it (shadow paging, as LMDB and
//!   WAFL do): the version it replaces is retained if a live child forked while it was current,
//!   else freed. A rolled-back transaction returns its reservations.
//! * on the **trunk**, a page a live child can still see has its pre-image copied into a slot and
//!   retained before the write, so neither the trunk's commit nor a later checkpoint that moves
//!   the new version into the database file can reach the child.
//!
//! # Reclamation
//!
//! A retained version is garbage once no live child of its node forked inside `[born, died)`.
//! Removing the child forked at `f` can only make versions containing `f` garbage, and a version
//! containing `f` becomes garbage exactly when it also lies strictly between `f`'s neighbouring
//! live siblings `lo` and `hi`: `born > lo` and `died <= hi`. The versions are indexed both by
//! `born` and by `died` — ZFS's deadlists, which key a dead block by the interval that killed it
//! and split it by birth (round 10's F2, ported from turso `2d2653599`) — so that each side of that
//! query is a range, not a scan:
//!
//! * with no older live sibling (the oldest child, which is whom uniform-TTL lease expiry reaps),
//!   the garbage is exactly the versions with `died` in `(f, hi]`;
//! * with no younger one (the newest child), exactly those with `born` in `(lo, f]`;
//! * with both, each range also holds survivors, and the two are walked in lockstep until the
//!   shorter one ends (see [`Lineage::garbage`]).
//!
//! Both indexes are derived state, rebuilt through [`Lineage::retain`] at replay and at
//! `load_snapshot`; the log and snapshot formats do not change.
//!
//! A released branch with an open connection is kept whole until the connection goes. A released
//! branch with live children is RETIRED (F4, UNBUILT): it keeps exactly the versions some live
//! child can read — its current versions become retained ones that died at the release epoch, and
//! any with no live child inside `[born, release)` are freed at the release itself — and it is
//! freed whole when its last child goes, which may in turn free its parent.
//!
//! # Leases (F5, UNBUILT)
//!
//! A branch may carry a lease deadline on the store's LEASE CLOCK — time the database has been
//! open, summed across opens, never read from the wall (see `LeaseClock`). An expiry pass reaps
//! every branch past its deadline, non-cooperatively and deepest first, through the same release
//! path as a dropped handle, so an expired interior with a live child is retired, not kept whole.
//! The pass runs at every fork (so an expired parent is refused, not revived), at every lease
//! renewal and every connect (so an expired branch is neither renewed nor opened), at every
//! database open (so a crashed agent's branch goes at the next start), and on
//! `Database::expire_branches`.
//!
//! What survives a CRASH of the database process is the clock as last stamped. Stamps ride on
//! flushes that happen anyway while a lease is outstanding: every branch commit, every fork,
//! renewal, release and expiry pass that writes a record, and a clean close; a pass with nothing
//! due also queues a stamp (at most one per second) for the next flush to carry. A TRUNK commit
//! stamps too, at most once per second, flushing for the stamp alone when it has no pre-image to
//! make durable (review N2: otherwise a trunk-only workload never advanced the durable clock). So
//! a crash loses the open time since the last stamped flush — bounded by the gap between commits
//! of any kind, not by how long ago someone last called `expire_branches`. It extends leases,
//! never shortens one. The clock tracks what is QUEUED and what is DURABLE separately (review N3):
//! a queued stamp dies with the process, so the close and `expire_branches` compare with the
//! durable one.
//!
//! # Durability (see `journal.rs` for the files and their prior art)
//!
//! Durable branches log OPERATIONS — `Fork`, `Commit`, `TrunkRetain`, `Release` — and recover by
//! replaying them through the same `apply_*` functions the live store runs, so epochs, children and
//! every branch-side retain/free decision are re-derived rather than stored. Two rules:
//!
//! 1. A record is written only after every slot it names is durable (the journal's flush order),
//!    and an operation that returns to its caller has had its record flushed.
//! 2. A slot is freed only after the record that frees it is durable: an older durable state that
//!    still names it can never see it reused.
//!
//! The trunk's `written` epochs are NOT persisted. At recovery each is rebuilt as the largest
//! `died` among the trunk's retained versions of that page. That can only UNDER-state the true
//! last-write epoch, and only for writes that retained nothing — writes made when no live child
//! had forked inside the interval. Forks only take later epochs, so no child that could need the
//! understated interval can ever exist, and every retention the lower bound later triggers covers
//! only children that see the trunk's current version, which is exactly what it copies.
//!
//! # What this does not do
//!
//! * One `Mutex` guards every branch, held across the durable store's fsyncs except a group
//!   flight's: forks, branch commits and batch releases wait for durability with it released (see
//!   `Group`, r11-churn amendment 4). Correct, and a named wall under concurrent writers; the
//!   benchmark this lane ships is single-threaded and says so.
//! * The persistent page maps are an index over slots the lineages own; they own nothing. A
//!   branch's `inherited` names only slots its ancestors keep for it, so dropping a map never frees
//!   a page and keeping one never pins a page.
//! * Snapshot mode (`BranchDurability::Durable`) recovers eagerly: every branch map is materialised
//!   at open, O(live branch state). Catalog mode reads state on demand (see `catalog.rs`).
//!
//! # Per-page version order (the fat node; round 10's F1, ported from turso `0de3aa904`)
//!
//! Within one node, one page's retained versions have non-empty, pairwise disjoint `[born, died)`
//! ranges: the trunk retains `[written, epoch)` and then sets `written = epoch`; a branch commit
//! retains `[old.born, epoch)` and its new version is born at `epoch`; a released interior retires
//! `[owned.born, epoch)`; replay and `load_snapshot` insert them through [`Lineage::retain`] in
//! `born` order per page. So `born` is unique per (node, page), and the version a child forked at
//! `f` sees is the one with the greatest `born <= f`, provided `f < died`. The versions are kept in a
//! map ordered by `born`, which makes that lookup a predecessor search and a release a removal by
//! key — Driscoll, Sarnak, Sleator and Tarjan's fat node (JCSS 1989) with a search tree over its
//! version stamps. [`Lineage::retain`] refuses a version that would break the disjointness the
//! search relies on. The map is derived state: recovery rebuilds it from the records and the
//! snapshot, whose formats do not change.
//!
//! # The composition (a12-durable-open lane, round 11)
//!
//! This file composes the sota-durable port of F1, F2 and F4 (turso `716723965`) with r11-restart's
//! catalog mode (turso `b99c4106f`..`14b04b575`). The children of every node live in the store-wide
//! `ChildIndex` (catalog-backed), so F2's neighbours `lo`/`hi` and the "a live child forked in
//! `[from, to)`" test come from it, and a lineage keeps only its child count. In catalog mode a
//! branch's F1 maps and F2 indexes are rebuilt from its own `ret` rows when `ensure` first makes it
//! resident, and its F4 page map from its parent's state at its fork epoch. The trunk's retained
//! versions are NOT made resident a page at a time (C-L did that, as r11-restart built it, and a
//! recovery then read every version of every page its log tail touched: all of them, ∝ N, when the
//! trunk keeps writing). They are read in place (C-P; `catalog.rs`): the trunk's lineage holds only
//! the versions retained since the last checkpoint, `trunk_version_at` probes the catalog for the
//! version holding a fork epoch, `trunk_written_known` reads a page's last version once, and a trunk
//! child's reap adds the catalog's garbage (`trunk_catalog_garbage`) to what F2 finds in memory.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Bound, Deref, DerefMut};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::arena::{Arena, Slot, SlotPtr};
use super::catalog::{CatBranch, Catalog, Meta};
use super::journal::{BranchFiles, Flight, Journal, Record, SnapBranch, SnapshotState};
use super::page_map::PageMap;
use super::{
    BranchDurability, BranchFailpoint, BranchId, BranchOpenStats, BranchStats, BranchWork, Expired,
    HoldMax, Reaped,
};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, MutexGuard};
use crate::{LimboError, Result};

/// Time every store-mutex hold (observation only; off unless a harness turns it on). Ported from
/// r11-bigtxn (turso `7fcc8db5b`), as the rest of this block.
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
    /// Bytes those growths moved.
    realloc_bytes: u64,
    /// Page-map nodes path-copied (an `Arc::make_mut` on a shared node).
    node_copies: u64,
    /// Arena chunk bytes newly allocated, zero-filled, inside the hold.
    zeroed_bytes: u64,
}

/// A hold of the store mutex that accounts for itself when it is released.
struct Hold<'a> {
    guard: MutexGuard<'a, StoreInner>,
    start: Option<std::time::Instant>,
    /// The arena's vector capacities at acquisition: a growth inside the hold moved the old
    /// contents, which the realloc counter charges to it.
    arena_caps: [usize; 3],
    /// The arena's chunk bytes at acquisition, for `zeroed_bytes`.
    arena_chunk_bytes: usize,
    /// Bytes written into the arena under the mutex so far, at acquisition, for `copy_bytes`.
    arena_written: u64,
}

impl<'a> Hold<'a> {
    /// Account for the hold `guard` is (see `BranchStore::lock`). Also a fuzzy checkpoint's install,
    /// whose thread has the mutex but not the store (merge 1b(ii)): the frees it returns are hold
    /// pages, charged to it rather than leaked into the next counted hold.
    fn of(guard: MutexGuard<'a, StoreInner>) -> Self {
        // Started once the lock is ours, so a contended acquire's wait is not counted as hold.
        let start = HOLD_TIMING
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(std::time::Instant::now);
        let arena_caps = guard.arena.as_ref().map_or([0; 3], |a| a.capacities());
        let arena_chunk_bytes = guard.arena.as_ref().map_or(0, |a| a.chunk_bytes());
        let arena_written = guard.arena.as_ref().map_or(0, |a| a.written_bytes());
        Hold {
            guard,
            start,
            arena_caps,
            arena_chunk_bytes,
            arena_written,
        }
    }
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
        let mut acc = std::mem::take(&mut inner.hold);
        let caps = inner.arena.as_ref().map_or([0; 3], |a| a.capacities());
        for (old, new) in self.arena_caps.iter().zip(caps) {
            if new != *old {
                // Each of the arena's growable vectors holds one block pointer per entry.
                acc.realloc_moved += *old as u64;
                acc.realloc_bytes += (*old * std::mem::size_of::<usize>()) as u64;
            }
        }
        let chunk_bytes = inner.arena.as_ref().map_or(0, |a| a.chunk_bytes());
        acc.zeroed_bytes += chunk_bytes.saturating_sub(self.arena_chunk_bytes) as u64;
        let written = inner.arena.as_ref().map_or(0, |a| a.written_bytes());
        acc.copy_bytes += written.saturating_sub(self.arena_written);
        inner.work.lock_holds += 1;
        inner.work.locked_copy_bytes += acc.copy_bytes;
        let max = &mut inner.hold_max;
        max.pages = max.pages.max(acc.pages);
        max.copy_bytes = max.copy_bytes.max(acc.copy_bytes);
        max.realloc_moved = max.realloc_moved.max(acc.realloc_moved);
        max.realloc_bytes = max.realloc_bytes.max(acc.realloc_bytes);
        max.node_copies = max.node_copies.max(acc.node_copies);
        max.zeroed_bytes = max.zeroed_bytes.max(acc.zeroed_bytes);
        if let Some(start) = self.start {
            let ns = start.elapsed().as_nanos() as u64;
            max.ns = max.ns.max(ns);
            inner.hold_hist[hold_bucket(ns)] += 1;
        }
    }
}

/// The most pages one hold of the store mutex maps, allocates or frees on behalf of one
/// transaction's commit or rollback: work proportional to a transaction's size is split into holds
/// of this many pages, so no other branch ever waits for more than this.
const HOLD_BATCH: usize = 64;

/// A commit counted in `publishing` from its first hold to its last: released on every exit from
/// `publish`, a panic in a mapping hold included, so compaction is never disabled for good.
struct Publishing<'a>(&'a BranchStore);

impl Drop for Publishing<'_> {
    fn drop(&mut self) {
        let mut inner = self.0.lock();
        if std::thread::panicking() {
            // A mapping hold panicked: the commit is half-mapped and its record only buffered.
            // Letting a compaction snapshot that would make half a commit durable, so fail-stop
            // first (review 2 F3).
            if let Some(journal) = inner.journal.as_mut() {
                journal.poison();
            }
        }
        inner.publishing -= 1;
    }
}

/// Hold-duration histogram: 8 buckets per octave of nanoseconds.
const HOLD_HIST_BUCKETS: usize = 8 * 40;

fn hold_bucket(ns: u64) -> usize {
    (((ns.max(1) as f64).log2() * 8.0) as usize).min(HOLD_HIST_BUCKETS - 1)
}

/// A branch write transaction's own slots (r11-bigtxn F-shadow with STEAL, ported onto the durable
/// store): every page it first writes gets a slot reserved under the store mutex (the durable
/// store's copy decision, `pending`), which the transaction fills WITHOUT the mutex, at a spill
/// and at commit, through the slot's [`SlotPtr`]. The committed slots are never written, so a
/// rollback is returning these; and no durable record names them before the commit's own.
#[derive(Default)]
pub(crate) struct ShadowTxn {
    pages: HashMap<u32, Shadow>,
}

#[derive(Clone)]
struct Shadow {
    slot: Slot,
    ptr: SlotPtr,
    /// The slot holds the page's latest spilled or committed image.
    filled: bool,
    /// crc32c of that image, computed as it is filled (the `Commit` record carries it).
    crc: u32,
}

impl ShadowTxn {
    /// Record the slot `BranchStore::shadow_slot` reserved for `page`.
    pub(crate) fn insert(&mut self, page: u32, slot: Slot, ptr: SlotPtr) {
        self.pages.insert(
            page,
            Shadow {
                slot,
                ptr,
                filled: false,
                crc: 0,
            },
        );
    }

    /// Copy `image` into `page`'s slot.
    pub(crate) fn fill(&mut self, page: u32, image: &[u8]) -> Result<()> {
        let s = self.pages.get_mut(&page).ok_or_else(|| {
            LimboError::InternalError(format!(
                "branch page {page} was dirtied with no shadow slot behind it"
            ))
        })?;
        // SAFETY: this transaction reserved the slot and has not published it (`SlotPtr`).
        unsafe { s.ptr.write(image)? };
        s.crc = crc32c::crc32c(image);
        s.filled = true;
        Ok(())
    }

    /// The page's image as this transaction last filled it, if it did.
    pub(crate) fn read(&self, page: u32, out: &mut [u8]) -> Result<bool> {
        let Some(s) = self.pages.get(&page) else {
            return Ok(false);
        };
        if !s.filled {
            return Ok(false);
        }
        // SAFETY: as `fill`.
        unsafe { s.ptr.read(out)? };
        Ok(true)
    }

    pub(crate) fn is_filled(&self, page: u32) -> bool {
        self.pages.get(&page).is_some_and(|s| s.filled)
    }
}

pub(crate) struct BranchStore {
    /// Shared with a fuzzy checkpoint's thread, which takes it for its install (F-FZ).
    inner: Arc<Mutex<StoreInner>>,
    /// Threads of fuzzy checkpoints started (F-FZ): joined by `compact_now`, by `Drop`, and once
    /// more than `FLIGHTS_KEPT` accumulate (the oldest is long past its install).
    flights: Mutex<Vec<crate::thread::JoinHandle<()>>>,
    /// Test and harness hook (F-FZ): while it holds `HOLD_BEFORE_COMMIT` or `HOLD_AFTER_COMMIT`, a
    /// fuzzy checkpoint in flight waits at that point (its catalog rows written but not committed;
    /// or committed but not installed), so a caller can act on the store there, or image its files.
    flight_hold: Arc<AtomicU8>,
    /// F-FZ back-pressure: set while a fuzzy checkpoint is in flight and the log is past twice the
    /// threshold; an operation that sees it after releasing the store mutex waits for the install.
    over_hard: Arc<AtomicBool>,
    /// F-FZ: set from a fuzzy checkpoint's install until its WAL truncation ends; no checkpoint
    /// starts meanwhile, whose capture would pin a read mark and make the truncation busy.
    truncating: Arc<AtomicBool>,
    /// F-FZ: fuzzy-checkpoint installs so far, and a condvar the install signals; back-pressure
    /// waits on it, so every waiting operation (not only one that happens to join the thread)
    /// waits for the install, and none waits for the WAL truncation after it.
    installs: Arc<(std::sync::Mutex<u64>, std::sync::Condvar)>,
    /// Live children of the trunk. Read without the lock on every trunk first-write so that a
    /// database with no branches pays one atomic load per written page and nothing else.
    ///
    /// The unlocked read is sound because the only transition that matters — 0 to 1 — happens in
    /// a trunk fork, which holds the trunk's WAL write lock; a trunk writer reading this holds the
    /// same lock. A 1-to-0 transition (a reap) racing the read only makes the writer take the lock
    /// and find nothing to do. An EARLY release that drops it sets `unsynced` first (see there).
    trunk_children: AtomicUsize,
    /// Trunk pre-image records wait in the journal's buffer between the trunk's `add_dirty` and its
    /// commit. The commit's barrier reads this without the lock, so a trunk commit with nothing to
    /// make durable pays one atomic load.
    ///
    /// An early release (a batch release, or an expiry pass riding on a fork's flight) sets it too,
    /// until a flush covers it: that release may have removed the child a trunk write's copy
    /// decision would have kept a pre-image for, so the next trunk commit must wait for it — and be
    /// refused if its flight failed, when the child comes back at the next open (review r12-merge1
    /// N1).
    unsynced: AtomicBool,
    /// Whether a durable store has any lease outstanding. A trunk commit's barrier reads it without
    /// the lock, so a trunk with no leases still pays one load for the stamp (review N2). A stale
    /// `true` costs one lock; a stale `false` misses one stamp, which only lengthens leases.
    leases_outstanding: AtomicBool,
    /// A read-only open of a database WITH branch files: the branch store was not opened, and every
    /// branch operation is refused by name (review 4 C2; see `open_with_flags`).
    trunk_only: bool,
    /// What the open read and rebuilt (r11-restart lane instrument; observing only).
    open_stats: BranchOpenStats,
    /// `resolve_into` calls and the arena slot reads they made (r11-restart lane instrument).
    resolve_calls: AtomicU64,
    arena_reads: AtomicU64,
    /// Group commit with the flush outside the store mutex (r11-churn amendment 4); see `Group`.
    /// Shared with a fuzzy checkpoint's thread (merge 1b(ii)): its arena sync and its install take
    /// the group's flight slot, as a compaction does.
    group: Arc<Group>,
    /// Observation only (r11-bigtxn amendment 8): bytes of log and arena a flight wrote and synced
    /// with the store mutex held (`flush_locked`) and with no lock held (`wait_durable`'s leader).
    flight_locked_bytes: AtomicU64,
    flight_unlocked_bytes: AtomicU64,
    /// Observation only: times a holder of the store mutex waited out a flight in the air (a fuzzy
    /// checkpoint's install included, hence shared with its thread).
    locked_flight_waits: Arc<AtomicU64>,
}

/// Group commit with the flush OUTSIDE the store mutex (r11-churn PREREG amendment 4): early lock
/// release and flush pipelining, as Aether (Johnson et al., VLDB 2010) and ferrodb's D159 do.
///
/// A fork, a branch commit or a batch release decides and applies under the store mutex, buffers
/// its records, releases the mutex, and only then waits until its records are durable
/// (`wait_durable`). One waiter at a time LEADS a flush: it takes everything buffered so far, under
/// the mutex (`Journal::take_flight`), and writes and syncs it holding no lock at all, so whatever
/// arrives meanwhile buffers behind it and shares the next flush. Every other flush site still
/// flushes under the mutex (`flush_locked`), but waits out a flight in the air first: frames are
/// written in log order, and a later region must never be synced ahead of an earlier one.
///
/// What early release must not break, and why it does not:
/// * **Rule 1** (a record is durable only after the slots it names): a commit writes its slots
///   before it buffers its record, so they are written before the flight that carries the record
///   is taken, and the flight syncs the arena before the log.
/// * **Rule 2** (a slot is reused only once the record that freed it is durable): a slot freed by an
///   early-released operation waits in `pending_free` under that operation's log position, and
///   returns to the arena only once a flush has covered it (`mature_frees`).
/// * **Acknowledgement**: no caller learns of an operation before its records are durable. A later
///   operation that depends on an earlier one (a commit on a branch whose fork is still in flight)
///   is later in the log, so its own durability implies the earlier one's.
/// * **Failure**: a failed flight fail-stops the journal, and every waiter gets the error. The
///   in-memory state is then ahead of the disk — the fail-stop rule already governs that (nothing
///   more is written; the next open recovers from disk), and the slots such operations freed are
///   never returned, because no flush will ever cover them.
struct Group {
    state: std::sync::Mutex<GroupState>,
    cv: std::sync::Condvar,
}

#[derive(Default)]
struct GroupState {
    /// A flight is being written.
    flushing: bool,
    /// Every journal byte below this log sequence number is durable.
    durable: u64,
    /// A flight failed: nothing more becomes durable in this process.
    poisoned: bool,
}

impl Group {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(GroupState::default()),
            cv: std::sync::Condvar::new(),
        }
    }
}

struct StoreInner {
    arena: Option<Arena>,
    journal: Option<Journal>,
    /// `Some` for a durable store.
    files: Option<BranchFiles>,
    sync: bool,
    next_id: u64,
    trunk: TrunkState,
    branches: HashMap<BranchId, BranchState>,
    failpoint: Option<BranchFailpoint>,
    orphans: Vec<Slot>,
    lease: LeaseClock,
    /// Every leased, unreleased branch by deadline: the expiry pass is a range, never a scan.
    leases: BTreeSet<(u64, BranchId)>,
    /// The lease a fork is given when `DatabaseOpts::with_branch_lease` sets one.
    default_lease: Option<Duration>,
    /// Live children of every node (see `ChildIndex`).
    children: ChildIndex,
    /// Branch states that exist, loaded or not (`live_branches`).
    n_states: u64,
    /// `BranchDurability::Catalog`: checkpoint into the catalog, read state on demand.
    catalog_mode: bool,
    /// The catalog and what this process holds beside it; `Some` once a catalog store has files.
    cat: Option<CatState>,
    /// Observation only; see [`BranchWork`].
    work: BranchWork,
    /// Page-map inserts made deriving page maps since open: by `derive_page_maps` after a snapshot
    /// load, and by `ensure` for each branch it makes resident (observing only).
    derived_inserts: u64,
    /// C-R, redo on demand (catalog recovery): the tail's Commits to branches that were not
    /// resident, per branch, in log order with their log positions. Applied when something first
    /// makes the branch resident (`ensure`), and all of them before any checkpoint (`settle`).
    parked: HashMap<BranchId, Vec<(u64, Vec<(u32, Slot, u32)>)>>,
    /// The last log position of the tail that names each slot: a slot a parked Commit frees, which
    /// a LATER record names, was reused by the free list and is not freed again.
    named_at: HashMap<Slot, u64>,
    /// The log position of the record being replayed.
    replay_pos: u64,
    /// Slots a parked Commit freed while recovery was still replaying (the arena does not exist
    /// yet): recovery marks them free in record order.
    deferred_freed: Vec<Slot>,
    /// Commits parked by recovery, and parked Commits applied since (observing only).
    parked_records: u64,
    parked_applied: u64,
    /// Slots freed by an early-released operation whose records are not yet durable, under the log
    /// sequence number that makes them free (see `Group`, rule 2). A catalog checkpoint covers
    /// every operation applied before its capture, so it takes these out at the capture and lists
    /// them free in its own transaction (`checkpoint_capture`; they come back here if it does not
    /// commit).
    pending_free: VecDeque<(u64, Vec<Slot>)>,
    /// The hold in progress (see `Hold`); observation only.
    hold: HoldAcc,
    /// Per-hold maxima since the last `take_hold_max`; observation only.
    hold_max: HoldMax,
    /// Hold durations since the last `take_hold_hist` (only while hold timing is on).
    hold_hist: [u64; HOLD_HIST_BUCKETS],
    /// Commits between their first and last hold (r11-bigtxn ported): their record is buffered and
    /// only part of their pages is mapped, so no compaction or catalog checkpoint may snapshot the
    /// state until this is 0 (a snapshot drops the buffered records whose effects it carries).
    publishing: u32,
    /// `MaybeCompactBetweenMapHolds`: the automatic check runs as if the log wanted compacting.
    force_compaction: bool,
    /// The highest log sequence number of any early-released Release (a batch release, or an
    /// expiry pass riding on a fork's flight); 0 if none. A branch such a release kept alive — open
    /// at the time — is freed at its close, and those frees wait for this (review r12-merge1 R2).
    early_released: u64,
    /// F-EXP: the last expiry pass stopped at its bound with more due (in memory, or in the
    /// catalog sweep): `expire_now` runs another.
    expire_more: bool,
}

/// Catalog mode's bookkeeping beside the in-memory cache of branch states (see `catalog.rs`).
struct CatState {
    catalog: Catalog,
    /// Branches whose catalog rows are stale, and which of their rows (`DIRTY_*`): at the next
    /// checkpoint only those are rewritten (fix v3, PREREG A9: a commit rewrites the branch's `cur`
    /// rows and never its `branch` row, so its secondary indexes are not touched).
    dirty: HashMap<BranchId, u8>,
    /// Branches removed since the last checkpoint: deleted from the catalog at the next one, and
    /// never loaded from it again.
    removed: HashSet<BranchId>,
    /// Trunk pages whose `written` epoch this process has reconciled with the catalog: the `died`
    /// of the page's last catalog version (C-P).
    trunk_known: HashSet<u32>,
    /// Trunk versions this process has read from the catalog (clean copies), consulted only by
    /// containment: a known version holding the fork epoch asked about is the answer, since one
    /// page's versions are disjoint (C-P).
    trunk_cache: HashMap<u32, BTreeMap<u64, Retained>>,
    /// Catalog trunk versions reaped since the last checkpoint, as (page, born): skipped by every
    /// catalog read and deleted by the next checkpoint (C-P).
    trunk_gone: HashSet<(u32, u64)>,
    /// No catalog row that is not loaded has a lease deadline below this (`None`: no lease).
    lease_floor: Option<u64>,
    /// F-EXP: where the bounded expiry passes' sweep of the catalog's due rows has got to, as
    /// (deadline, id); `None` between sweeps.
    lease_cursor: Option<(u64, u64)>,
    /// The highest catalog free slot moved into the arena's in-memory free list.
    free_cursor: Option<Slot>,
    /// The catalog has no free slot above `free_cursor`.
    free_exhausted: bool,
    /// Slots the catalog lists free that this process owns otherwise now (in use, or already on the
    /// in-memory free list): never fetched, and deleted from the catalog at the next checkpoint.
    taken: HashSet<Slot>,
    /// Branch states and trunk pages read from the catalog since open (instrument).
    branch_loads: u64,
    trunk_page_loads: u64,
    /// Trunk-version probes and range reads, and the version rows they returned (C-P).
    trunk_probes: u64,
    trunk_rows: u64,
    /// The fuzzy checkpoint's writer: a second connection on the catalog (F-FZ).
    writer: Arc<Mutex<Catalog>>,
    /// The catalog generation the last checkpoint committed. The log's generation can lag it by
    /// one, until the log is rewritten to the checkpoint's suffix (F-FZ).
    generation: u64,
    /// The generation the next checkpoint attempt takes: consumed by every capture, committed or
    /// not, so no two `Record::Checkpoint` markers in a log share a generation, and recovery's cut at
    /// the committed one can never land on a stale one (second fresh-context review, finding 1).
    next_generation: u64,
    /// A checkpoint has captured and not yet installed: the catalog connection holds a pinned
    /// read snapshot, the catalog free table is not refilled from, and no checkpoint starts.
    flight: bool,
    /// Checkpoint and settle counters (observing only).
    ckpt: CkptCounters,
}

/// Catalog checkpoints and C-R settle batches, as counted (r11-restart-r2 instrument, observing
/// only). Times are wall-clock ns.
#[derive(Clone, Copy, Default)]
struct CkptCounters {
    /// Checkpoints installed (fuzzy and sharp).
    count: u64,
    /// Fuzzy checkpoints started (a thread spawned).
    flights: u64,
    /// Store-mutex hold inside checkpoints: capture + install (+ the write, on the sharp path).
    hold_ns: u64,
    hold_max_ns: u64,
    /// The writer's time without the store mutex (fuzzy path; phase 2 only).
    flight_ns: u64,
    /// Catalog statements run while the store mutex was held inside a checkpoint.
    stmts_locked: u64,
    /// C-R settle batches run from `maybe_compact`, the branches they loaded, and the most loads in
    /// one batch.
    settle_batches: u64,
    settle_loads: u64,
    settle_max_loads: u64,
}

impl CkptCounters {
    fn hold(&mut self, ns: u64) {
        self.hold_ns += ns;
        self.hold_max_ns = self.hold_max_ns.max(ns);
    }

    fn as_array(&self) -> [u64; 9] {
        [
            self.count,
            self.flights,
            self.hold_ns,
            self.hold_max_ns,
            self.flight_ns,
            self.stmts_locked,
            self.settle_batches,
            self.settle_loads,
            self.settle_max_loads,
        ]
    }
}

/// C-R's parked Commits settled per `maybe_compact` call, at most (in branches): Graefe's
/// background redo in bounded quanta, so no one mutex hold loads a whole checkpoint window's
/// branches (r11-restart-r2).
const SETTLE_BATCH: usize = 64;

/// F-EXP (r11-restart-r2): the most branches an expiry pass (at open, fork, open_conn, set_lease;
/// `expire_now` runs passes until none is due) reaps, and the most catalog rows it sweeps. Redis's
/// active expiry bounds each cycle the same way; a branch an operation names is reaped on access if
/// due. `R11_EXPIRE=unbounded` restores the base's single unbounded pass: the BEFORE arm.
fn expire_batch() -> usize {
    static BATCH: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *BATCH.get_or_init(|| match std::env::var("R11_EXPIRE") {
        Ok(v) if v == "unbounded" => usize::MAX >> 1,
        _ => 256,
    })
}

/// F-FZ: fuzzy-checkpoint threads kept unjoined before the oldest is joined.
const FLIGHTS_KEPT: usize = 64;

/// F-FZ hook stages (`BranchStore::checkpoint_hold`): the writer has written the captured rows and
/// not committed; or it has committed and the install has not run.
pub(crate) const HOLD_BEFORE_COMMIT: u8 = 2;
pub(crate) const HOLD_AFTER_COMMIT: u8 = 3;

/// Or-ed into the hook's stage once the flight has arrived there (tests wait for it).
pub(crate) const HOLD_ARRIVED: u8 = 0x80;

/// If the hook is at `stage`, mark the arrival and wait until it is moved (tests and the harness
/// release it by storing 0).
fn pause_at(hold: Option<&AtomicU8>, stage: u8) {
    if let Some(hold) = hold {
        let arrived = stage | HOLD_ARRIVED;
        if hold
            .compare_exchange(stage, arrived, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            while hold.load(Ordering::Acquire) == arrived {
                crate::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// F-FZ's arm switch (lead decision 27960c3d): `maybe_compact` checkpoints a catalog store the
/// base's way (settle everything, then capture, write and install under the store mutex) unless
/// `R11_CKPT=fuzzy`, which opts into the fuzzy checkpoint. The default suite therefore runs the base's
/// checkpoint, and every measurement arm names its mode. (`compact_now`, and the tests' and harness's
/// `checkpoint_fuzzy_now`, choose their own path whatever the switch says.)
fn fuzzy_checkpoints() -> bool {
    static FUZZY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FUZZY.get_or_init(|| std::env::var("R11_CKPT").is_ok_and(|v| v == "fuzzy"))
}

impl CatState {
    fn new(catalog: Catalog, sync: bool, generation: u64) -> Result<Self> {
        let writer = Arc::new(Mutex::new(catalog.writer(sync)?));
        Ok(Self {
            writer,
            generation,
            next_generation: generation + 1,
            flight: false,
            ckpt: CkptCounters::default(),
            catalog,
            dirty: HashMap::new(),
            removed: HashSet::new(),
            trunk_known: HashSet::new(),
            trunk_cache: HashMap::new(),
            trunk_gone: HashSet::new(),
            lease_floor: None,
            lease_cursor: None,
            free_cursor: None,
            free_exhausted: false,
            taken: HashSet::new(),
            branch_loads: 0,
            trunk_page_loads: 0,
            trunk_probes: 0,
            trunk_rows: 0,
        })
    }
}

/// What of a branch's catalog state a checkpoint must rewrite.
const DIRTY_ROW: u8 = 1;
const DIRTY_CUR: u8 = 2;
const DIRTY_RET: u8 = 4;
/// Forked since the last checkpoint: no catalog row yet, so everything is written.
const DIRTY_NEW: u8 = 8;

/// The lease clock: milliseconds of time the database has been OPEN, summed across every open.
///
/// Chubby's rule (§2.9): while the authority is down "the session lease timer is stopped; this is
/// legal because it is equivalent to extending the client's lease". So the clock is never read
/// from the wall: it is the value recovered from the journal plus a monotonic `Instant` since this
/// open. Downtime is not charged, and a wall-clock step (ferrodb's F2) cannot expire anything.
/// It is persisted by stamping it into `Lease` and `Clock` records riding on flushes (see the
/// module doc); a crash loses the time since the last flush that carried one, which extends
/// leases and never shortens one.
struct LeaseClock {
    /// The clock recovered at open.
    base_ms: u64,
    opened: Instant,
    /// Test-only forward motion (`Database::branch_lease_clock_advance`).
    advanced_ms: u64,
    /// Test-only: real time no longer moves the clock (`Database::branch_lease_clock_freeze`), so
    /// a test can hit "same millisecond" orderings deterministically.
    frozen: bool,
    /// The largest reading buffered in, or written to, the journal.
    queued_ms: u64,
    /// The largest reading a successful flush (or snapshot) has made durable. `queued_ms` is never
    /// behind it; a reading between the two dies with the process (review N3).
    durable_ms: u64,
}

impl LeaseClock {
    fn new() -> Self {
        Self {
            base_ms: 0,
            opened: Instant::now(),
            advanced_ms: 0,
            frozen: false,
            queued_ms: 0,
            durable_ms: 0,
        }
    }

    fn now_ms(&self) -> u64 {
        let real = if self.frozen {
            0
        } else {
            millis(self.opened.elapsed())
        };
        self.base_ms
            .saturating_add(real)
            .saturating_add(self.advanced_ms)
    }

    /// Stop real time moving the clock, keeping `now` where it is (it must never move back).
    fn freeze(&mut self) {
        if !self.frozen {
            self.advanced_ms = self
                .advanced_ms
                .saturating_add(millis(self.opened.elapsed()));
            self.frozen = true;
        }
    }

    /// Recovery saw the clock at `ms`. The clock only moves forward.
    fn recovered(&mut self, ms: u64) {
        self.base_ms = self.base_ms.max(ms);
        self.queued_ms = self.queued_ms.max(ms);
        self.durable_ms = self.durable_ms.max(ms);
    }

    /// A record carrying the reading `ms` is about to be buffered.
    fn queued(&mut self, ms: u64) {
        self.queued_ms = self.queued_ms.max(ms);
    }

    /// A flush succeeded: every buffered reading is durable.
    fn flushed(&mut self) {
        self.durable_ms = self.queued_ms;
    }

    /// A checkpoint made the reading `ms` durable in the catalog's meta row. Readings queued after
    /// its capture are still only buffered (F-FZ), so they stay undurable.
    fn durable_at_least(&mut self, ms: u64) {
        self.queued_ms = self.queued_ms.max(ms);
        self.durable_ms = self.durable_ms.max(ms);
    }
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// How many live children this node has. The children themselves are indexed store-wide by
    /// (parent, fork epoch) in `StoreInner::children`, which a catalog store reads on demand.
    n_children: u64,
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
    crc: u32,
}

struct TrunkState {
    lineage: Lineage,
    /// The trunk epoch of its last write to each page. Absent means "before the first fork that
    /// was live at the time", i.e. epoch 0, which is the conservative answer: it can only cause a
    /// retention that was not strictly needed, never skip one that was.
    written: HashMap<u32, u64>,
}

/// Who holds a branch. `Detached` is a branch with no handle that is NOT released: after
/// [`super::Branch::into_id`], and every unreleased branch after a reopen.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Handle {
    Attached,
    Detached,
    /// Released, and the `Release` record is durable: `collect` may free it.
    Released,
    /// Released by its handle, but the `Release` record could not be made durable (the journal
    /// failed). After a restart the branch comes back Detached and still names its slots, so
    /// nothing of it may EVER be freed in this process — `collect` requires `Released` exactly,
    /// which makes that a matter of the type, not of every caller remembering (review R1).
    ReleasePending,
}

impl Handle {
    /// Gone from the caller's point of view, durably or not: takes no connection, write, fork or
    /// lease.
    fn is_released(self) -> bool {
        matches!(self, Handle::Released | Handle::ReleasePending)
    }
}

/// Whether an expiry pass stamps the lease clock when nothing is due.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stamp {
    No,
    /// Queue a `Clock` record (at most one per `STAMP_EVERY_MS`) for the next flush to carry.
    Queue,
    /// Write and flush one now (`Database::expire_branches`).
    Flush,
}

/// Observation only (r11-churn instrument; nothing reads them): what expiry passes that found
/// something due did, and what compactions cost. Process-wide; updated under the store mutex.
pub(crate) mod churn_counters {
    use std::sync::atomic::AtomicU64;
    pub(crate) static EXPIRE_PASSES_WITH_DUE: AtomicU64 = AtomicU64::new(0);
    pub(crate) static EXPIRE_REAPED: AtomicU64 = AtomicU64::new(0);
    pub(crate) static EXPIRE_FREED_PAGES: AtomicU64 = AtomicU64::new(0);
    pub(crate) static EXPIRE_FSYNCS: AtomicU64 = AtomicU64::new(0);
    /// Passes whose records rode on a fork's own flush (r11-churn amendment 2's fix).
    pub(crate) static EXPIRE_PIGGYBACKED: AtomicU64 = AtomicU64::new(0);
    /// Group commit (amendment 4): flights led outside the mutex, flushes taken under it, the
    /// operations that waited for durability, and those whose records a flight had already made
    /// durable by the time they looked.
    pub(crate) static GC_FLIGHTS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static GC_LOCKED_FLUSHES: AtomicU64 = AtomicU64::new(0);
    pub(crate) static GC_WAITS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static GC_ALREADY_DURABLE: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMPACTIONS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMPACT_NS_TOTAL: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMPACT_NS_MAX: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMPACT_FSYNCS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static COMPACT_BYTES_LAST: AtomicU64 = AtomicU64::new(0);
}

/// An expiry pass planned but not yet durable (see `BranchStore::expire_plan`).
struct ExpirePlan {
    due: Vec<BranchId>,
    records: Vec<Record>,
    now: u64,
}

/// The finest grain at which expiry passes queue a clock stamp.
const STAMP_EVERY_MS: u64 = 1000;

/// A `Duration` in lease-clock milliseconds, saturating: `as_millis() as u64` WRAPS, which turns a
/// practically-infinite lease into a short one (review R6).
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Why a fail-stopped store refuses, naming the real cause (review 4 C6): a reopen cures an I/O
/// failure, but not a fork(2) child — its inherited descriptor holds the very lock a reopen needs.
fn fail_stop_cause(journal: Option<&Journal>) -> String {
    match journal.and_then(Journal::fork_parent) {
        Some(pid) => format!(
            "fail-stopped in this process, a fork(2) child of process {pid}, which opened it: a \
             branch store is not carried across fork()"
        ),
        None => "fail-stopped after an I/O failure, until the database is reopened".to_string(),
    }
}

fn fail_stopped(journal: Option<&Journal>, id: BranchId, what: &str) -> LimboError {
    LimboError::InternalError(format!(
        "branch store is {}; branch {} takes {what}",
        fail_stop_cause(journal),
        id.0
    ))
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has committed.
    current: BTreeMap<u32, Owned>,
    /// Slots reserved by the open write transaction's copy decisions, not yet published.
    pending: BTreeMap<u32, Slot>,
    /// The branch's committed schema: shared with the parent at fork, replaced by a committed
    /// DDL. `None` after a reopen until the first connection reparses it from the branch's pages.
    schema: Option<Arc<Schema>>,
    handle: Handle,
    /// A connection is open on this branch.
    open: bool,
    /// A write transaction on this branch is in progress.
    writer: bool,
    /// The lease deadline on the lease clock; `None` = the branch never expires.
    lease: Option<u64>,
    /// The fork epoch at which this branch's ancestry leaves the trunk: its own fork epoch if its
    /// parent is the trunk, else its parent's `trunk_at`.
    trunk_at: u64,
    /// Every arena page this branch sees through its ancestors: its parent's `view` at the fork.
    inherited: PageMap,
    /// `inherited` plus this branch's current pages, for its children to inherit. Built at the
    /// branch's first fork and kept current by its commits from then on; `None` until it forks, and
    /// again once it is retired (a released branch takes no child).
    view: Option<PageMap>,
}

impl BranchState {
    /// The map a child forked now inherits, built from `inherited` and `current` the first time.
    fn view_now(&mut self) -> &PageMap {
        let (inherited, current) = (&self.inherited, &self.current);
        self.view.get_or_insert_with(|| {
            let mut view = inherited.clone();
            for (&page, owned) in current {
                view.insert(page, (owned.slot, owned.crc));
            }
            view
        })
    }

    /// The version of `page` this branch held at its own epoch `f` — what a child forked at `f`
    /// reads from it — if it held one of its own then.
    fn version_at(&self, page: u32, f: u64) -> Option<(Slot, u32)> {
        if let Some(owned) = self.current.get(&page) {
            if owned.born <= f {
                return Some((owned.slot, owned.crc));
            }
        }
        self.lineage.retained_at(page, f, &mut 0)
    }

    /// F4. A released branch never reads its own `current` again, never writes, and takes no new
    /// child. So each current version becomes a retained one that died at the branch's epoch — a
    /// live child forked at `f` reads it exactly as before, because `born <= f < epoch` — and every
    /// one that no live child forked inside `[born, epoch)` goes to `freed` NOW rather than when the
    /// last child goes. That is the interval rule with the free epoch set to the release epoch
    /// (ferrodb's `retire_arenas_by_rule`); `Lineage::child_gone` then frees the rest incrementally.
    /// Only for a branch with no open connection: an open one still reads `current` at `u64::MAX`.
    fn retire_current(
        &mut self,
        id: BranchId,
        children: &ChildIndex,
        mut cat: Option<&mut Catalog>,
        freed: &mut Vec<Slot>,
    ) -> Result<()> {
        // Its children keep their own `inherited`; a released branch forks no new one.
        self.view = None;
        let epoch = self.lineage.epoch;
        let current: Vec<(u32, Owned)> = std::mem::take(&mut self.current).into_iter().collect();
        for (page, owned) in current {
            if children.any_in(cat.as_deref_mut(), id, owned.born, epoch)? {
                self.lineage.retain(
                    page,
                    Retained {
                        born: owned.born,
                        died: epoch,
                        slot: owned.slot,
                        crc: owned.crc,
                    },
                );
            } else {
                freed.push(owned.slot);
            }
        }
        Ok(())
    }
}

/// Live children of every node, by (parent, fork epoch); fork epochs are unique within a parent.
///
/// An eager store holds every child here. A catalog store holds here only the children forked
/// since its last checkpoint; the rest are read from the catalog's `branch_children` index on
/// demand. Every child removed since the last checkpoint is kept in `removed` with its nearest live
/// siblings at the moment it went (fix v4, PREREG A11): a catalog query that lands on a removed
/// row follows those links instead of reading the next row, so K removals cost O(K) lookups, not
/// the O(K^2) of skipping every removed row one by one. The links stay true because a fork epoch
/// is never reused and new children only ever take higher epochs.
#[derive(Default)]
struct ChildIndex {
    map: BTreeMap<(u64, u64), BranchId>,
    /// (parent, fork epoch) -> (nearest live sibling below, above) when it was removed.
    removed: HashMap<(u64, u64), (Option<u64>, Option<u64>)>,
}

impl ChildIndex {
    fn insert(&mut self, parent: BranchId, f: u64, child: BranchId) {
        self.map.insert((parent.0, f), child);
    }

    /// Remove the child `parent` forked at `f`, and return its nearest live siblings below and
    /// above. `catalog`: a catalog store, whose catalog may still hold the child's row. False in
    /// the first element when an eager store does not list the child.
    fn remove(
        &mut self,
        mut cat: Option<&mut Catalog>,
        parent: BranchId,
        f: u64,
        catalog: bool,
    ) -> Result<(bool, Option<u64>, Option<u64>)> {
        let listed = self.map.remove(&(parent.0, f)).is_some();
        if !catalog {
            // An eager store holds every child in `map`: no links are needed, or kept.
            if !listed {
                return Ok((false, None, None));
            }
            return Ok((true, self.below(None, parent, f)?, self.above(None, parent, f)?));
        }
        // Mark it removed before looking for its neighbours, so neither lookup returns it.
        let fresh = self.removed.insert((parent.0, f), (None, None)).is_none();
        let lo = self.below(cat.as_deref_mut(), parent, f)?;
        let hi = self.above(cat, parent, f)?;
        self.removed.insert((parent.0, f), (lo, hi));
        Ok((listed || fresh, lo, hi))
    }

    /// Follow the removal links from `e` downward (or upward) to a live child.
    fn resolve(&self, p: u64, mut e: Option<u64>, down: bool) -> Option<u64> {
        while let Some(x) = e {
            match self.removed.get(&(p, x)) {
                None => return Some(x),
                Some(&(lo, hi)) => e = if down { lo } else { hi },
            }
        }
        None
    }

    /// The nearest live child of `p` below `f`.
    fn below(&self, cat: Option<&mut Catalog>, parent: BranchId, f: u64) -> Result<Option<u64>> {
        let p = parent.0;
        let mem = self.map.range((p, 0)..(p, f)).next_back().map(|(&(_, e), _)| e);
        let Some(cat) = cat else {
            return Ok(mem);
        };
        let row = cat.children_below(p, f, 1)?.into_iter().next();
        Ok(mem.max(self.resolve(p, row, true)))
    }

    /// The nearest live child of `p` above `f`.
    fn above(&self, cat: Option<&mut Catalog>, parent: BranchId, f: u64) -> Result<Option<u64>> {
        let p = parent.0;
        let mem = self
            .map
            .range((p, f.saturating_add(1))..=(p, u64::MAX))
            .next()
            .map(|(&(_, e), _)| e);
        let Some(cat) = cat else {
            return Ok(mem);
        };
        let row = cat.children_above(p, f, 1)?.into_iter().next();
        Ok(match (mem, self.resolve(p, row, false)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        })
    }

    /// True if a live child of `parent` forked in `[from, to)`: one that can see a version current
    /// over that range.
    fn any_in(&self, cat: Option<&mut Catalog>, parent: BranchId, from: u64, to: u64) -> Result<bool> {
        if from >= to {
            return Ok(false);
        }
        let p = parent.0;
        if self.map.range((p, from)..(p, to)).next().is_some() {
            return Ok(true);
        }
        let Some(cat) = cat else {
            return Ok(false);
        };
        let row = cat.children_in(p, from, to, 1)?.into_iter().next();
        Ok(self.resolve(p, row, false).is_some_and(|e| e < to))
    }
}

#[derive(Clone, Copy)]
struct Owned {
    slot: Slot,
    born: u64,
    crc: u32,
}

impl Lineage {
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
    fn retained_at(&self, page: u32, f: u64, examined: &mut u64) -> Option<(Slot, u32)> {
        let (_, v) = self.retained.get(&page)?.range(..=f).next_back()?;
        *examined += 1;
        (f < v.died).then_some((v.slot, v.crc))
    }

    /// The child forked at `f` is gone: already removed from the child index, whose nearest live
    /// siblings at its removal were `lo` below and `hi` above. Every retained version only it could
    /// see goes to `freed`. Returns the pages whose retained versions changed (a catalog store
    /// rewrites them at its next checkpoint).
    fn child_gone(
        &mut self,
        f: u64,
        lo: Option<u64>,
        hi: Option<u64>,
        freed: &mut Vec<Slot>,
        work: &mut BranchWork,
    ) -> Vec<u32> {
        self.n_children -= 1;
        let dead = self.garbage(f, lo, hi, work);
        let mut pages = Vec::with_capacity(dead.len());
        for (born, page, died) in dead {
            let versions = self.retained.get_mut(&page).expect("indexed version is listed");
            let v = versions.remove(&born).expect("indexed version is listed");
            work.gc_examined += 1;
            if versions.is_empty() {
                self.retained.remove(&page);
            }
            let indexed = self.by_born.remove(&(born, page, died))
                && self.by_died.remove(&(died, page, born));
            crate::turso_assert!(indexed, "a released version was missing from an index");
            freed.push(v.slot);
            pages.push(page);
        }
        pages
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

    /// Remove one version, if this lineage holds it, from the per-page map and both indexes
    /// (F-FZ: a checkpoint moved it to the catalog).
    fn take_version(&mut self, page: u32, born: u64) -> Option<Retained> {
        let versions = self.retained.get_mut(&page)?;
        let v = versions.remove(&born)?;
        if versions.is_empty() {
            self.retained.remove(&page);
        }
        self.by_born.remove(&(v.born, page, v.died));
        self.by_died.remove(&(v.died, page, v.born));
        Some(v)
    }

    fn release_all(self, freed: &mut Vec<Slot>) {
        for (_, versions) in self.retained {
            freed.extend(versions.into_values().map(|v| v.slot));
        }
    }

    fn retained_list(&self) -> Vec<(u32, u64, u64, Slot, u32)> {
        let mut out: Vec<(u32, u64, u64, Slot, u32)> = self
            .retained
            .iter()
            .flat_map(|(&page, vs)| vs.values().map(move |v| (page, v.born, v.died, v.slot, v.crc)))
            .collect();
        out.sort_unstable();
        out
    }
}

/// F-FZ: what a catalog checkpoint captured under the store mutex (phase 1), for its writer (phase
/// 2, no store mutex) and its install (phase 3). See `catalog.rs`, "The fuzzy checkpoint".
struct Captured {
    /// The catalog generation this checkpoint commits.
    generation: u64,
    /// The logical log position the capture covers (`Journal::mark`).
    log_from: u64,
    /// The journal's log sequence number at the same point (`Journal::lsn`, just past the capture's
    /// `Record::Checkpoint`): what the checkpoint makes durable once it commits, for the group's
    /// landing (merge 1b(ii)).
    lsn: u64,
    rows: Vec<(CatBranch, u8)>,
    /// The dirty map the capture swapped out: merged back if the write fails.
    dirty: HashMap<BranchId, u8>,
    removed: Vec<BranchId>,
    trunk_new: Vec<(u32, u64, u64, Slot, u32)>,
    trunk_gone: Vec<(u32, u64)>,
    free_cursor: Option<Slot>,
    taken: Vec<Slot>,
    free_list: Vec<Slot>,
    reserved: Vec<Slot>,
    /// Deferred frees (`StoreInner::pending_free`) taken out at the capture (merge 1b(ii)): slots
    /// freed by operations applied before it, whose records are not yet durable. The checkpoint
    /// lists them free; they return to the arena only once it has committed, and go back to
    /// `pending_free` if it does not.
    covered: VecDeque<(u64, Vec<Slot>)>,
    meta: Meta,
    /// `ChildIndex` keys (children forked, and removal links) as of the capture.
    child_keys: Vec<(u64, u64)>,
    child_removed: Vec<(u64, u64)>,
    /// A handle on the arena file: synced before the catalog names the slots. `None` in the sharp
    /// form, which syncs the arena in its capture (merge 1b(ii)).
    arena: Option<std::fs::File>,
    lease_now: u64,
    fail_after_commit: bool,
}

/// F-FZ phase 2: write a capture into the catalog through `catalog` (the writer connection) in ONE
/// transaction. Holds no store lock: nothing here reads the store.
///
/// `in_doubt` (merge 1b(ii), review 2 F2) is set while a failure would leave the outcome unknown,
/// so that the install fail-stops the journal: the arena fsync (a failed one may have dropped the
/// pages it was writing back, and a later fsync of the same file can report success without them)
/// and the COMMIT (a failed one may be on disk). A failure anywhere else rolls back and changes
/// nothing. With `group` (the fuzzy form), the arena fsync takes the group's flight slot, so no
/// flight syncs the arena between a failure and the fail-stop (`sync_arena_in_group`).
fn checkpoint_write(
    catalog: &mut Catalog,
    cap: &Captured,
    hold: Option<&AtomicU8>,
    group: Option<&Group>,
    in_doubt: &mut bool,
) -> Result<()> {
    // Every slot the catalog is about to name must be durable first.
    if let Some(file) = cap.arena.as_ref() {
        *in_doubt = true;
        sync_arena_in_group(group, file)?;
        *in_doubt = false;
    }
    catalog.begin()?;
    let written = (|| -> Result<()> {
        for (b, what) in &cap.rows {
            if what & DIRTY_NEW != 0 {
                catalog.put_branch(b)?;
                continue;
            }
            if what & DIRTY_ROW != 0 {
                catalog.update_row(b)?;
            }
            if what & DIRTY_CUR != 0 {
                catalog.put_cur(b)?;
            }
            if what & DIRTY_RET != 0 {
                catalog.put_ret(b)?;
            }
        }
        for &id in &cap.removed {
            catalog.delete_branch(id.0)?;
        }
        for &(page, born) in &cap.trunk_gone {
            catalog.trunk_delete(page, born)?;
        }
        for &(page, born, died, slot, crc) in &cap.trunk_new {
            catalog.trunk_insert(page, born, died, slot, crc)?;
        }
        if let Some(cursor) = cap.free_cursor {
            catalog.free_delete_upto(cursor)?;
        }
        for &slot in &cap.taken {
            catalog.free_delete(slot)?;
        }
        // The covered deferred frees too (r11-churn amendment 4): this checkpoint stops naming
        // them, and left out of the free table they would stay in use in the catalog for good (a
        // recovery reads the free table, and no later record frees them again).
        for &slot in cap
            .free_list
            .iter()
            .chain(cap.reserved.iter())
            .chain(cap.covered.iter().flat_map(|(_, freed)| freed.iter()))
        {
            catalog.free_put(slot)?;
        }
        catalog.put_meta(&cap.meta)
    })()
    .and_then(|()| {
        pause_at(hold, HOLD_BEFORE_COMMIT);
        *in_doubt = true;
        catalog.commit()
    });
    if let Err(e) = written {
        catalog.rollback();
        return Err(e);
    }
    Ok(())
}

/// The fuzzy writer's arena fsync, as the holder of the group's flight slot (merge 1b(ii)). It waits
/// out a flight in the air and refuses if one failed; a failure of its own (or a panic) poisons the
/// group as the slot is released. So no flight fsyncs the arena file between this fsync's failure
/// and the install's fail-stop, and none reports records durable over pages this fsync found lost
/// (the error is reported once per open file, and every descriptor of the arena shares one). The
/// store mutex is not held here. Without a group, a plain fsync.
fn sync_arena_in_group(group: Option<&Group>, file: &std::fs::File) -> Result<()> {
    /// Releases the slot on every exit; `ok` still false then poisons the group.
    struct FlightSlot<'a> {
        group: &'a Group,
        ok: bool,
    }
    impl Drop for FlightSlot<'_> {
        fn drop(&mut self) {
            let mut g = self.group.state.lock().unwrap_or_else(|e| e.into_inner());
            g.flushing = false;
            if !self.ok {
                g.poisoned = true;
            }
            self.group.cv.notify_all();
        }
    }
    let Some(group) = group else {
        return super::journal::fsync_file(file);
    };
    {
        let mut g = group.state.lock().unwrap();
        while g.flushing {
            g = group.cv.wait(g).unwrap();
        }
        if g.poisoned {
            return Err(group_poisoned());
        }
        g.flushing = true;
    }
    let mut slot = FlightSlot { group, ok: false };
    let synced = super::journal::fsync_file(file);
    slot.ok = synced.is_ok();
    drop(slot);
    synced
}

/// F-FZ phase 4 (and the sharp path's last step): bound the catalog's WAL (fix v2, PREREG A7). A
/// PASSIVE backfill first, which waits for no reader, then a TRUNCATE attempt. The checkpoint is
/// already durable, so a failure costs only WAL length: it is logged, not returned.
fn truncate_catalog_wal(catalog: &mut Catalog) {
    if let Err(e) = catalog.wal_passive() {
        tracing::warn!("branch catalog WAL backfill failed: {e}");
    }
    match catalog.truncate_wal() {
        Ok(r) if r.first().copied().unwrap_or(0) != 0 => {
            tracing::warn!("branch catalog WAL truncation was busy: {r:?}")
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("branch catalog WAL truncation failed: {e}"),
    }
}

/// What a fuzzy checkpoint's thread shares with its store (F-FZ); the group and its wait counter
/// added by merge 1b(ii), so that its arena sync and its install take the group's flight slot.
struct FlightShared {
    hold: Arc<AtomicU8>,
    over_hard: Arc<AtomicBool>,
    truncating: Arc<AtomicBool>,
    installs: Arc<(std::sync::Mutex<u64>, std::sync::Condvar)>,
    group: Arc<Group>,
    waits: Arc<AtomicU64>,
}

/// F-FZ: the body of a fuzzy checkpoint's thread. Phase 2 holds only the writer; phase 3 only the
/// store mutex; phase 4 only the writer again. No path takes the writer while waiting for the store
/// mutex, and the sharp path (which takes the store mutex, then the writer) refuses while a capture
/// is in flight, so the two cannot deadlock.
///
/// Group commit (merge 1b(ii)). Phase 2's arena fsync holds the group's flight slot (see
/// `sync_arena_in_group`); no lock is held while it waits for the slot, and the slot's holders
/// never wait for the writer. Phase 3 takes the slot too, as a compaction does
/// (`BranchStore::compact`): the install reads the log back and renames a new file over it, so no
/// flight may be writing it; waiting under the store mutex cannot deadlock, since a flight's leader
/// needs only the group lock to land. The install then lands the group by the checkpoint's commit
/// point, re-derived for a capture the log has moved past since:
/// * the journal poisoned (a flight failed, the arena fsync or the COMMIT failed, the rewrite failed
///   after its rename) -> the group is poisoned;
/// * else the catalog committed -> everything up to the capture's marker is durable (the catalog
///   holds all of it), whatever failed after (a rewrite that failed before its rename leaves the old
///   log in use, which recovery cuts at the marker; the lease floor); what the log holds after the
///   marker is exactly as durable as it was;
/// * else (nothing committed, the journal live) -> nothing changes; the slot is released.
fn run_flight(
    inner: Arc<Mutex<StoreInner>>,
    writer: Arc<Mutex<Catalog>>,
    cap: Box<Captured>,
    shared: FlightShared,
) {
    let FlightShared {
        hold,
        over_hard,
        truncating,
        installs,
        group,
        waits,
    } = shared;
    let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let t = Instant::now();
    // A panic in the writer must still reach the install (with an error), or `flight` would stay
    // set: no checkpoint would start again and the read snapshot would stay pinned. One inside the
    // arena fsync or the COMMIT leaves `in_doubt` set, and the install fail-stops.
    let mut in_doubt = false;
    let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut w = writer.lock();
        let written = checkpoint_write(&mut w, &cap, Some(&*hold), Some(&*group), &mut in_doubt);
        if written.is_err() {
            w.rollback();
        }
        written
    }))
    .unwrap_or_else(|_| {
        writer.lock().rollback();
        Err(LimboError::InternalError(
            "the branch catalog checkpoint's writer panicked".to_string(),
        ))
    });
    let write_ns = ns(t);
    if written.is_ok() {
        pause_at(Some(&*hold), HOLD_AFTER_COMMIT);
    }
    let installed = {
        // A counted hold: the deferred frees the install returns are its pages.
        let mut guard = Hold::of(inner.lock());
        let t = Instant::now();
        {
            let mut g = group.state.lock().unwrap();
            if g.flushing {
                waits.fetch_add(1, Ordering::Relaxed);
            }
            while g.flushing {
                g = group.cv.wait(g).unwrap();
            }
            if g.poisoned {
                // A flight failed: nothing more becomes durable in this process. The rewrite below
                // refuses a poisoned journal; the catalog may have committed all the same, and
                // recovery reads what the files say.
                if let Some(journal) = guard.journal.as_mut() {
                    journal.poison();
                }
            }
            g.flushing = true;
        }
        let lsn = cap.lsn;
        let committed = written.is_ok();
        let installed = guard.checkpoint_install(cap, written, in_doubt);
        let poisoned = guard.poisoned();
        {
            let mut g = group.state.lock().unwrap();
            g.flushing = false;
            if poisoned {
                g.poisoned = true;
            } else if committed {
                g.durable = g.durable.max(lsn);
            }
            group.cv.notify_all();
        }
        if committed && !poisoned {
            guard.work.compactions += 1;
        }
        let hold_ns = ns(t);
        if let Some(cat) = guard.cat.as_mut() {
            cat.ckpt.hold(hold_ns);
            cat.ckpt.flight_ns += write_ns;
        }
        // Set under the store mutex, so no capture can slip in before the truncation.
        truncating.store(installed.is_ok(), Ordering::Release);
        over_hard.store(false, Ordering::Release);
        let (count, signal) = &*installs;
        *count.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        signal.notify_all();
        installed
    };
    // Phase 4 takes only the writer, never the store mutex again: `start_flight` may join this
    // thread while holding the store mutex. (Its time is not counted in `flight_ns`.)
    match installed {
        Ok(()) => {
            // A panic here must not leave `truncating` set: no checkpoint would start again.
            let truncated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                truncate_catalog_wal(&mut writer.lock())
            }));
            truncating.store(false, Ordering::Release);
            if truncated.is_err() {
                tracing::warn!("the branch catalog WAL truncation panicked");
            }
        }
        Err(e) => tracing::warn!("branch catalog fuzzy checkpoint failed: {e}"),
    }
}

/// F-FZ back-pressure: declared BEFORE the store mutex's guard in an operation that can grow the
/// log, so it drops after the guard: if a fuzzy checkpoint is in flight and the log is past twice
/// the threshold, the operation waits for the install without holding the store mutex. It waits
/// on the install counter, 60 s at most (a flight that never installs is a bug, logged, not a hang).
struct Backpressure<'a>(&'a BranchStore);

impl Drop for Backpressure<'_> {
    fn drop(&mut self) {
        if !self.0.over_hard.load(Ordering::Acquire) {
            return;
        }
        let (count, installed) = &*self.0.installs;
        let mut n = count.lock().unwrap_or_else(|e| e.into_inner());
        let seen = *n;
        let started = Instant::now();
        while *n == seen && self.0.over_hard.load(Ordering::Acquire) {
            if started.elapsed() > Duration::from_secs(60) {
                tracing::warn!("branch store back-pressure: no checkpoint install in 60 s");
                return;
            }
            n = installed
                .wait_timeout(n, Duration::from_millis(50))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

impl BranchStore {
    /// `open`, for a database opened read-only or not (review 4 C2, the lead's decision; it
    /// replaces review 3's outright refusal, which made such a database unreadable read-only for
    /// good, since branch files are never removed).
    ///
    /// A READ-ONLY open of a database WITH branch files gets a TRUNK-ONLY store: no recovery (which
    /// writes: it cuts a torn tail, discards a temp snapshot, and reaps expired leases durably), no
    /// lock, no branch file read or written, and every branch operation refused by name. Trunk
    /// reads are safe beside a live writer that holds the branch lock, because:
    /// * a branch never writes a trunk page. Branch commits go to arena slots, and the trunk's
    ///   pre-images are COPIES into the arena. The trunk is exactly what Turso's WAL serves, under
    ///   this reader's own WAL snapshot, as it is beside any writer;
    /// * this handle reads no branch file, so it cannot see the writer's log or arena mid-append,
    ///   and it takes no lock, so it blocks no writer;
    /// * this handle writes no trunk page, so it cannot skip a pre-image the writer's branches need:
    ///   the connection refuses writes on a read-only database, and a trunk write that got past it
    ///   is refused here (`first_write_trunk`).
    ///
    /// Whether a second PROCESS may read the trunk beside a writer is Turso's own rule, unchanged.
    ///
    /// Durable + read-only with NO branch files stays refused: a fork would create them, and
    /// nothing below the connection refuses a fork on a read-only database.
    pub(crate) fn open_with_flags(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        db_path: &str,
        read_only: bool,
    ) -> Result<Self> {
        if read_only {
            if !crate::is_memory_like(db_path) && BranchFiles::for_db(db_path).exist() {
                return Ok(Self::trunk_only());
            }
            if matches!(
                durability,
                BranchDurability::Durable { .. } | BranchDurability::Catalog { .. }
            ) {
                return Err(LimboError::InvalidArgument(format!(
                    "{db_path}: durable branches need a read-write open: a fork would create \
                     branch files, and nothing below the connection refuses one when read-only"
                )));
            }
        }
        Self::open(durability, default_lease, db_path)
    }

    fn trunk_only() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner::fresh(None, false, None))),
            flights: Mutex::new(Vec::new()),
            flight_hold: Arc::new(AtomicU8::new(0)),
            over_hard: Arc::new(AtomicBool::new(false)),
            truncating: Arc::new(AtomicBool::new(false)),
            installs: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
            trunk_children: AtomicUsize::new(0),
            unsynced: AtomicBool::new(false),
            leases_outstanding: AtomicBool::new(false),
            trunk_only: true,
            open_stats: BranchOpenStats::default(),
            resolve_calls: AtomicU64::new(0),
            arena_reads: AtomicU64::new(0),
            group: Arc::new(Group::new()),
            flight_locked_bytes: AtomicU64::new(0),
            flight_unlocked_bytes: AtomicU64::new(0),
            locked_flight_waits: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn is_trunk_only(&self) -> bool {
        self.trunk_only
    }

    /// Refuse `what` on a trunk-only store (see `open_with_flags`).
    pub(crate) fn refuse_if_trunk_only(&self, what: &str) -> Result<()> {
        if self.trunk_only {
            return Err(LimboError::InvalidArgument(format!(
                "{what} is refused: this database was opened read-only while it has durable \
                 branches, so its branch store was not opened (trunk reads only)"
            )));
        }
        Ok(())
    }

    /// The store for a database whose sidecar files are named from `db_path`. A durable store
    /// recovers whatever its files hold; a volatile one refuses a database whose files say it has
    /// durable branches, because opened volatile, the trunk's writes would skip the pre-image
    /// barrier and silently change what those branches read.
    pub(crate) fn open(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        db_path: &str,
    ) -> Result<Self> {
        let memory = crate::is_memory_like(db_path);
        let opened = Instant::now();
        let mut stats = BranchOpenStats::default();
        let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let inner = match durability {
            BranchDurability::Volatile => {
                if !memory && BranchFiles::for_db(db_path).exist() {
                    return Err(LimboError::InvalidArgument(format!(
                        "{db_path} has durable branches; open it with branch durability \
                         (DatabaseOpts::with_branch_durability), or its trunk writes would \
                         silently change what those branches read"
                    )));
                }
                StoreInner::fresh(None, false, default_lease)
            }
            BranchDurability::Durable { sync } => {
                if memory {
                    return Err(LimboError::InvalidArgument(
                        "durable branches need a file-backed database".to_string(),
                    ));
                }
                let files = BranchFiles::for_db(db_path);
                if files.cat.exists() {
                    return Err(LimboError::InvalidArgument(format!(
                        "{db_path} has a catalog-mode branch store ({}); open it with \
                         BranchDurability::Catalog",
                        files.cat.display()
                    )));
                }
                let mut inner = StoreInner::fresh(Some(files.clone()), sync, default_lease);
                if files.exist() {
                    let t = Instant::now();
                    let recovered = Journal::recover(&files, sync)?;
                    stats.recover_ns = ns(t);
                    if let Some(recovered) = recovered {
                        stats.snap_bytes = recovered.snap_bytes;
                        stats.log_bytes = recovered.log_bytes;
                        stats.records = recovered.records.len() as u64;
                        let t = Instant::now();
                        if let Some(snapshot) = recovered.snapshot {
                            stats.snap_branches = snapshot.branches.len() as u64;
                            inner.load_snapshot(snapshot)?;
                        }
                        stats.load_ns = ns(t);
                        // Frees during replay are not acted on: the free set is derived below
                        // from what the recovered state references.
                        let mut ignored = Vec::new();
                        let t = Instant::now();
                        for record in &recovered.records {
                            inner.replay(record, &mut ignored)?;
                        }
                        stats.replay_ns = ns(t);
                        // A snapshot can hold a released branch that was kept only by an open
                        // connection; after a restart nothing is open.
                        let t = Instant::now();
                        stats.released_scanned = inner.collect_released(&mut ignored)?;
                        stats.collect_ns = ns(t);
                        let t = Instant::now();
                        let referenced = inner.referenced_slots();
                        stats.referenced_ns = ns(t);
                        stats.referenced_slots = referenced.len() as u64;
                        let t = Instant::now();
                        let arena = Arena::open_file(
                            &files.arena,
                            recovered.page_size,
                            false,
                            &referenced,
                        )?;
                        stats.arena_ns = ns(t);
                        stats.arena_high_water = arena.in_use() as u64 + arena.free_count() as u64;
                        stats.arena_free = arena.free_count() as u64;
                        inner.arena = Some(arena);
                        inner.journal = Some(recovered.journal);
                    }
                }
                inner
            }
            BranchDurability::Catalog { sync } => {
                if memory {
                    return Err(LimboError::InvalidArgument(
                        "durable branches need a file-backed database".to_string(),
                    ));
                }
                let files = BranchFiles::for_db(db_path);
                if files.snap.exists() {
                    return Err(LimboError::InvalidArgument(format!(
                        "{db_path} has a snapshot-mode branch store ({}); open it with \
                         BranchDurability::Durable",
                        files.snap.display()
                    )));
                }
                let mut inner = StoreInner::fresh_mode(Some(files.clone()), sync, default_lease, true);
                if files.exist() {
                    Self::recover_catalog(&mut inner, &files, sync, &mut stats)?;
                }
                inner
            }
        };
        let mut store = Self {
            trunk_children: AtomicUsize::new(inner.trunk.lineage.n_children as usize),
            inner: Arc::new(Mutex::new(inner)),
            flights: Mutex::new(Vec::new()),
            flight_hold: Arc::new(AtomicU8::new(0)),
            over_hard: Arc::new(AtomicBool::new(false)),
            truncating: Arc::new(AtomicBool::new(false)),
            installs: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
            unsynced: AtomicBool::new(false),
            leases_outstanding: AtomicBool::new(false),
            trunk_only: false,
            open_stats: BranchOpenStats::default(),
            resolve_calls: AtomicU64::new(0),
            arena_reads: AtomicU64::new(0),
            group: Arc::new(Group::new()),
            flight_locked_bytes: AtomicU64::new(0),
            flight_unlocked_bytes: AtomicU64::new(0),
            locked_flight_waits: Arc::new(AtomicU64::new(0)),
        };
        // A branch whose lease ran out before the last close — or before the last flush that
        // carried a stamp, if the process crashed — goes now, with nobody having to ask: this is
        // what makes a crashed agent's branch temporary. The clock resumed where it was last
        // stamped, so nothing expires here that had time left then.
        {
            let t = Instant::now();
            let mut inner = store.inner.lock();
            store.expire(&mut inner, Stamp::No)?;
            store.sync_lease_flag(&inner);
            stats.expire_ns = ns(t);
        }
        stats.total_ns = ns(opened);
        // The instrument's own counting scan, after `total_ns` so the open time does not carry it.
        {
            let inner = store.inner.lock();
            stats.branches = inner.branches.len() as u64;
            stats.current_entries = inner.branches.values().map(|b| b.current.len() as u64).sum();
            stats.retained_entries = inner
                .branches
                .values()
                .map(|b| b.lineage.retained.values().map(|v| v.len() as u64).sum::<u64>())
                .sum();
            stats.trunk_retained =
                inner.trunk.lineage.retained.values().map(|v| v.len() as u64).sum();
            stats.trunk_children = inner.trunk.lineage.n_children;
            stats.states = inner.n_states;
            stats.derived_map_inserts = inner.derived_inserts;
            stats.parked_records = inner.parked_records;
            stats.parked_applied = inner.parked_applied;
            if let Some(cat) = inner.cat.as_ref() {
                stats.branch_loads = cat.branch_loads;
                stats.trunk_page_loads = cat.trunk_page_loads;
                stats.trunk_probes = cat.trunk_probes;
                stats.trunk_rows = cat.trunk_rows;
                stats.cat_queries = cat.catalog.counters.queries;
                stats.cat_rows_read = cat.catalog.counters.rows_read;
            }
        }
        store.open_stats = stats;
        Ok(store)
    }

    /// Catalog-mode recovery (on demand): open the catalog and read its meta row, replay the log's
    /// tail — which loads only the branches and trunk pages its records touch — collect released
    /// branches the catalog kept for a connection that no longer exists, and rebuild the arena's
    /// free space from the catalog's free table plus what the replay changed. Nothing here reads a
    /// branch that no record since the last checkpoint touches.
    fn recover_catalog(
        inner: &mut StoreInner,
        files: &BranchFiles,
        sync: bool,
        stats: &mut BranchOpenStats,
    ) -> Result<()> {
        let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let t = Instant::now();
        let mut catalog = Catalog::open(&files.cat, sync)?;
        let meta = catalog.meta()?;
        stats.catalog_ns = ns(t);
        let t = Instant::now();
        let recovered =
            Journal::recover_catalog(files, sync, meta.map(|m| (m.page_size, m.generation)))?;
        stats.recover_ns = ns(t);
        let Some(recovered) = recovered else {
            return Ok(());
        };
        stats.log_bytes = recovered.log_bytes;
        stats.records = recovered.records.len() as u64;
        let meta = meta.unwrap_or(Meta {
            page_size: recovered.page_size as u32,
            ..Meta::default()
        });
        inner.next_id = inner.next_id.max(meta.next_id);
        inner.trunk.lineage.epoch = meta.trunk_epoch;
        inner.trunk.lineage.n_children = meta.trunk_children;
        inner.n_states = meta.states;
        inner.lease.recovered(meta.lease_now_ms);
        let mut cat = CatState::new(catalog, sync, meta.generation)?;
        // Past every checkpoint marker the log still holds, committed or not (F-FZ).
        let marked = recovered
            .records
            .iter()
            .filter_map(|r| match r {
                Record::Checkpoint { generation } => Some(*generation),
                _ => None,
            })
            .max();
        if let Some(g) = marked {
            cat.next_generation = cat.next_generation.max(g + 1);
        }
        cat.lease_floor = cat.catalog.lease_min()?;
        inner.cat = Some(cat);
        // Replay, remembering every slot a record names (in use) and every slot its replay frees,
        // in order: the last word on each slot wins.
        let t = Instant::now();
        let mut touched: HashMap<Slot, bool> = HashMap::new();
        for (pos, record) in recovered.records.iter().enumerate() {
            let pos = pos as u64;
            match record {
                Record::Commit { pages, .. } => {
                    for &(_, slot, _) in pages {
                        touched.insert(slot, true);
                        inner.named_at.insert(slot, pos);
                    }
                }
                Record::TrunkRetain { slot, .. } => {
                    touched.insert(*slot, true);
                    inner.named_at.insert(*slot, pos);
                }
                _ => {}
            }
            inner.replay_pos = pos;
            let mut freed = Vec::new();
            inner.replay(record, &mut freed)?;
            // A parked Commit applied during this record (C-R) freed its slots here, in order.
            freed.append(&mut inner.deferred_freed);
            for slot in freed {
                touched.insert(slot, false);
            }
        }
        stats.replay_ns = ns(t);
        let t = Instant::now();
        let mut freed = Vec::new();
        stats.released_scanned = inner.collect_released(&mut freed)?;
        freed.append(&mut inner.deferred_freed);
        for slot in freed {
            touched.insert(slot, false);
        }
        if inner.parked.is_empty() {
            inner.named_at = HashMap::new();
        }
        stats.collect_ns = ns(t);
        // The arena: the catalog's free table as of the checkpoint, overridden by what the replay
        // touched, plus every untouched slot past the checkpoint's high-water mark (written by an
        // operation whose record never became durable, or never written at all).
        let t = Instant::now();
        let page_size = recovered.page_size;
        let file_len = std::fs::metadata(&files.arena).map_or(0, |m| m.len());
        let file_hw = u32::try_from(file_len / page_size as u64)
            .map_err(|_| LimboError::Corrupt("branch arena is larger than 2^32 slots".into()))?;
        let cat = inner.cat.as_mut().expect("set above");
        let mut in_use = meta.in_use as i64;
        let mut free_mem = Vec::new();
        let mut taken = HashSet::new();
        let mut high_water = file_hw.max(meta.arena_hw);
        for (&slot, &used) in &touched {
            let was_used = slot < meta.arena_hw && !cat.catalog.free_has(slot)?;
            in_use += used as i64 - was_used as i64;
            if slot < meta.arena_hw && !was_used {
                // The catalog lists it free; this process owns it now either way.
                taken.insert(slot);
            }
            if used {
                if slot >= file_hw {
                    return Err(LimboError::Corrupt(format!(
                        "branch store names arena slot {slot}, past the end of the arena file"
                    )));
                }
            } else {
                free_mem.push(slot);
            }
            high_water = high_water.max(slot + 1);
        }
        for slot in meta.arena_hw..file_hw {
            if !touched.contains_key(&slot) {
                free_mem.push(slot);
            }
        }
        stats.touched_slots = touched.len() as u64;
        if super::arena::trace_slots() {
            let mut t: Vec<(Slot, bool)> = touched.iter().map(|(&s, &u)| (s, u)).collect();
            t.sort_unstable();
            eprintln!(
                "R11SLOT recover meta_hw={} file_hw={file_hw} meta_in_use={} touched={t:?} free_mem={free_mem:?} taken={taken:?} in_use={in_use} records={}",
                meta.arena_hw, meta.in_use, recovered.records.len()
            );
        }
        stats.arena_free = free_mem.len() as u64;
        stats.arena_high_water = high_water as u64;
        cat.taken = taken;
        let in_use = u64::try_from(in_use)
            .map_err(|_| LimboError::Corrupt("branch catalog: negative arena use".into()))?;
        inner.arena = Some(Arena::open_file_catalog(
            &files.arena,
            page_size,
            high_water,
            in_use,
            free_mem,
        )?);
        inner.journal = Some(recovered.journal);
        stats.arena_ns = ns(t);
        Ok(())
    }

    /// A trunk-only store answers yes, so every trunk page write reaches `first_write_trunk` and
    /// its refusal.
    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_only || self.trunk_children.load(Ordering::Acquire) > 0
    }

    fn sync_trunk_children(&self, inner: &StoreInner) {
        self.trunk_children
            .store(inner.trunk.lineage.n_children as usize, Ordering::Release);
    }

    /// Call after anything that adds or removes a lease.
    fn sync_lease_flag(&self, inner: &StoreInner) {
        self.leases_outstanding.store(
            inner.journal.is_some() && inner.leases_exist(),
            Ordering::Release,
        );
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        self.trunk_only || self.inner.lock().n_states > 0
    }

    /// Append `records` and make them durable with ONE flush before the caller acts on any of
    /// them. A no-op when volatile.
    fn log_all(&self, inner: &mut StoreInner, records: Vec<Record>) -> Result<()> {
        {
            let StoreInner {
                journal,
                arena,
                failpoint,
                ..
            } = &mut *inner;
            let (Some(journal), Some(_)) = (journal.as_mut(), arena.as_ref()) else {
                return Ok(());
            };
            injected_flush_failure(failpoint, journal)?;
            for record in &records {
                journal.buffer(record)?;
            }
        }
        self.flush_locked(inner)?;
        inner.lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Buffer `records` for an early-released operation and return the log sequence number the
    /// caller must wait for (`wait_durable`) before acknowledging it. Nothing is flushed here; 0
    /// when volatile (always durable).
    fn buffer_all(&self, inner: &mut StoreInner, records: Vec<Record>) -> Result<u64> {
        let StoreInner {
            journal, failpoint, ..
        } = inner;
        let Some(journal) = journal.as_mut() else {
            return Ok(0);
        };
        injected_flush_failure(failpoint, journal)?;
        journal.check_live()?;
        if *failpoint == Some(BranchFailpoint::GroupFlightFails) {
            *failpoint = None;
            // Fails inside the flight's write, after this operation is applied (amendment 4).
            journal.fail_next_write();
        }
        for record in &records {
            journal.buffer(record)?;
        }
        Ok(journal.lsn())
    }

    /// Flush everything buffered, under the store mutex the caller holds. A flight in the air goes
    /// first (its frames precede these in the log); its leader needs only the group lock to finish,
    /// never the store mutex, so waiting for it here cannot deadlock.
    fn flush_locked(&self, inner: &mut StoreInner) -> Result<()> {
        let mut g = self.group.state.lock().unwrap();
        if g.flushing {
            self.locked_flight_waits.fetch_add(1, Ordering::Relaxed);
        }
        while g.flushing {
            g = self.group.cv.wait(g).unwrap();
        }
        if g.poisoned {
            return Err(group_poisoned());
        }
        let flight = match inner.take_flight() {
            Ok(Some(flight)) => flight,
            Ok(None) => return Ok(()),
            Err(e) => {
                g.poisoned = true;
                self.group.cv.notify_all();
                return Err(e);
            }
        };
        g.flushing = true;
        drop(g);
        churn_counters::GC_LOCKED_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let bytes = flight.sync_bytes();
        let end = flight.end_lsn;
        let written = flight.write();
        if written.is_ok() {
            self.flight_locked_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        if written.is_err() {
            if let Some(journal) = inner.journal.as_mut() {
                journal.poison();
            }
        }
        self.land(end, written.is_ok());
        written
    }

    /// Wait until no flight is in the air (called under the store mutex, so none can start).
    fn quiesce(&self) {
        let mut g = self.group.state.lock().unwrap();
        if g.flushing {
            self.locked_flight_waits.fetch_add(1, Ordering::Relaxed);
        }
        while g.flushing {
            g = self.group.cv.wait(g).unwrap();
        }
    }

    /// Return deferred frees a flush has covered, at most [`HOLD_BATCH`] of them (called under the
    /// store mutex, by the mechanism). An operation that deferred many drains the rest in bounded
    /// holds once it is durable (`wait_durable`), so no one hold frees a large operation's garbage.
    fn mature(&self, inner: &mut StoreInner) {
        if inner.pending_free.is_empty() {
            return;
        }
        let durable = self.group.state.lock().unwrap().durable;
        inner.mature_frees(durable, HOLD_BATCH - 1);
    }

    /// Return every deferred free a flush has covered, however many (the observation calls, whose
    /// counts must describe the log; not the mechanism, so not bounded).
    fn mature_all(&self, inner: &mut StoreInner) {
        if inner.pending_free.is_empty() {
            return;
        }
        let durable = self.group.state.lock().unwrap().durable;
        while inner
            .pending_free
            .front()
            .is_some_and(|&(lsn, _)| lsn <= durable)
        {
            inner.mature_frees(durable, HOLD_BATCH);
        }
        // An observation call is not a counted hold: what it freed must not be charged to the
        // next one.
        inner.hold = HoldAcc::default();
    }

    /// Return every deferred free a flush has covered, in holds of at most [`HOLD_BATCH`] (with no
    /// lock held on entry: each round takes its own hold).
    fn drain_matured(&self) {
        loop {
            let mut inner = self.lock();
            let durable = self.group.state.lock().unwrap().durable;
            if !inner
                .pending_free
                .front()
                .is_some_and(|&(lsn, _)| lsn <= durable)
            {
                return;
            }
            inner.mature_frees(durable, HOLD_BATCH);
        }
    }

    /// Record a flight's outcome and wake every waiter.
    fn land(&self, end: u64, ok: bool) {
        let mut g = self.group.state.lock().unwrap();
        g.flushing = false;
        if ok {
            g.durable = g.durable.max(end);
        } else {
            g.poisoned = true;
        }
        self.group.cv.notify_all();
    }

    /// Wait until the journal's first `lsn` bytes are durable, leading a flight when none is in the
    /// air, then return the deferred frees that flush covered, in bounded holds (amendment 8b: the
    /// operation that deferred them pays for them, and no one hold frees all of them). Called
    /// WITHOUT the store mutex.
    pub(crate) fn wait_durable(&self, lsn: u64) -> Result<()> {
        self.wait_durable_inner(lsn)?;
        self.drain_matured();
        Ok(())
    }

    fn wait_durable_inner(&self, lsn: u64) -> Result<()> {
        if lsn == 0 {
            return Ok(());
        }
        churn_counters::GC_WAITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut first = true;
        loop {
            {
                let mut g = self.group.state.lock().unwrap();
                loop {
                    if g.durable >= lsn {
                        if first {
                            churn_counters::GC_ALREADY_DURABLE
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        return Ok(());
                    }
                    if g.poisoned {
                        return Err(group_poisoned());
                    }
                    if !g.flushing {
                        break;
                    }
                    first = false;
                    g = self.group.cv.wait(g).unwrap();
                }
            }
            first = false;
            // Lead: take what is buffered under the store mutex, then write it holding nothing.
            let flight = {
                let mut inner = self.lock();
                let mut g = self.group.state.lock().unwrap();
                if g.durable >= lsn {
                    return Ok(());
                }
                if g.poisoned {
                    return Err(group_poisoned());
                }
                if g.flushing {
                    continue;
                }
                match inner.take_flight() {
                    Ok(Some(flight)) if flight.len() == 0 => {
                        // Our frames left the buffer, yet no flush covered them and none is in
                        // the air: only a failed flush does that, and it poisons the group.
                        return Err(group_poisoned());
                    }
                    Ok(Some(flight)) => {
                        g.flushing = true;
                        flight
                    }
                    // Nothing buffered, yet not durable: a flush under the mutex took our frames
                    // and has not landed. It cannot be in the air (flushing is false), so this is
                    // a flush that failed; the group says so.
                    Ok(None) => return Err(group_poisoned()),
                    Err(e) => {
                        g.poisoned = true;
                        self.group.cv.notify_all();
                        return Err(e);
                    }
                }
            };
            churn_counters::GC_FLIGHTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let bytes = flight.sync_bytes();
            let end = flight.end_lsn;
            let written = flight.write();
            if written.is_ok() {
                self.flight_unlocked_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            if written.is_err() {
                // Poison the group first (waking the waiters, some of whom hold the store mutex
                // while they wait), then the journal under the mutex.
                self.land(end, false);
                if let Some(journal) = self.lock().journal.as_mut() {
                    journal.poison();
                }
                return written;
            }
            self.land(end, true);
        }
    }

    /// Grant or extend `id`'s lease to `ttl` past the lease clock's now. A deadline only moves
    /// forward (Chubby §2.8: the master "is free to advance this timeout further into the future,
    /// but may not move it backwards in time").
    pub(crate) fn set_lease(&self, id: BranchId, ttl: Duration) -> Result<()> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        // A lease that has run out is not renewable: reap first, so a late renewal is refused
        // rather than reviving the branch.
        // One `now` for the whole operation (review R5): the renewal is decided at the instant
        // the pass judged the lease, not after the pass's own flush.
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        inner.ensure(id)?;
        self.reap_if_due(&mut inner, id, now)?;
        let st = inner.branches.get(&id).ok_or_else(|| gone(id))?;
        if st.handle.is_released() {
            return Err(reaped(id));
        }
        // `apply_lease` keeps the later of this and any earlier deadline: the ONE guard that a
        // deadline never moves back, and the one replay also runs (review R9).
        let deadline = now.saturating_add(millis(ttl));
        inner.lease.queued(now);
        self.log(
            &mut inner,
            Record::Lease {
                branch: id.0,
                deadline_ms: deadline,
                now_ms: now,
            },
        )?;
        inner.apply_lease(id, deadline);
        self.sync_lease_flag(&inner);
        Ok(())
    }

    /// The expiry pass, on demand. Also stamps the lease clock when a lease is outstanding, so time
    /// spent open survives a restart even if nothing expired.
    pub(crate) fn expire_now(&self) -> Result<Expired> {
        self.refuse_if_trunk_only("the expiry pass")?;
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        // A fail-stopped pass reaps nothing and stamps nothing: an empty `Expired` would say
        // "nothing was due" when the truth is "could not run" (review N4).
        if inner.poisoned() {
            return Err(LimboError::InternalError(format!(
                "branch store is {}; the expiry pass cannot make a release durable",
                fail_stop_cause(inner.journal.as_ref())
            )));
        }
        // Every branch due, in bounded passes (F-EXP), until a pass stops short of its bound.
        let mut all = Expired::default();
        loop {
            let (pass, _) = self.expire(&mut inner, Stamp::Flush)?;
            let more = inner.expire_more && !inner.poisoned();
            all.freed_pages += pass.freed_pages;
            all.reaped.extend(pass.reaped);
            if !more {
                return Ok(all);
            }
        }
    }

    /// F-EXP: reap `id` now if its lease has run out and it is still live, so an operation naming
    /// it is refused exactly as if the expiry pass had reaped it (a bounded pass may not have got
    /// to it). A fail-stopped store reaps nothing, as the pass does; its callers refuse instead.
    fn reap_if_due(&self, inner: &mut StoreInner, id: BranchId, now: u64) -> Result<()> {
        if inner.poisoned() {
            return Ok(());
        }
        let due = inner.branches.get(&id).is_some_and(|st| {
            !st.handle.is_released() && st.lease.is_some_and(|deadline| deadline <= now)
        });
        if !due {
            return Ok(());
        }
        inner.lease.queued(now);
        self.log_all(
            inner,
            vec![Record::Release { branch: id.0 }, Record::Clock { now_ms: now }],
        )?;
        let mut freed = Vec::new();
        if let Err(e) = inner.apply_release(id, &mut freed) {
            return Err(inner.fatal(e));
        }
        inner.release_slots(freed);
        self.sync_trunk_children(inner);
        self.sync_lease_flag(inner);
        Ok(())
    }

    /// Reap every branch whose lease has run out — non-cooperatively: attached, detached, or open
    /// (an open one takes no more writes and is freed when its connection closes). Deepest first,
    /// so a chain that expires together goes child before parent and each interior is freed whole
    /// rather than retired and then freed. Each release takes F4's path: an interior with a live
    /// child keeps exactly the versions that child can read. The Release records are made durable
    /// together, before anything is freed.
    fn expire(&self, inner: &mut StoreInner, stamp: Stamp) -> Result<(Expired, u64)> {
        let plan = self.expire_plan(inner, stamp)?;
        let now = plan.now;
        if plan.due.is_empty() {
            return Ok((Expired::default(), now));
        }
        let fsyncs0 = super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed);
        self.log_all(inner, plan.records.clone())?;
        let fsyncs = super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed) - fsyncs0;
        let expired = self.expire_apply(inner, plan, fsyncs)?;
        self.maybe_compact(inner);
        Ok((expired, now))
    }

    /// The first half of an expiry pass: what is due, and the records that release it (deepest
    /// first, then a clock stamp), WITHOUT flushing them. A caller that flushes anyway puts them in
    /// its own flush ahead of its own records (group commit, r11-churn amendment 2), and applies the
    /// plan only after that flush, so rule 2 holds: nothing is freed before its Release is durable.
    /// With nothing due, the stamp is handled here exactly as the pass always did.
    fn expire_plan(&self, inner: &mut StoreInner, stamp: Stamp) -> Result<ExpirePlan> {
        let now = inner.lease.now_ms();
        let empty = ExpirePlan {
            due: Vec::new(),
            records: Vec::new(),
            now,
        };
        inner.expire_more = false;
        // A fail-stopped store cannot make a Release durable, so it reaps nothing (and frees
        // nothing); reads stay available and the next open recovers from disk.
        if inner.poisoned() {
            return Ok(empty);
        }
        // Catalog stores: a lease a branch not yet resident carries is in the catalog's lease
        // index; the rows due are made resident, which puts their deadlines in `leases`. F-EXP: at
        // most `expire_batch()` rows per pass, in a keyset sweep that the next pass continues.
        if let Some(cat) = inner.cat.as_mut() {
            if cat.lease_floor.is_some_and(|floor| floor <= now) {
                let (due_rows, complete) =
                    cat.catalog
                        .lease_due_page(now, cat.lease_cursor, expire_batch())?;
                cat.lease_cursor = if complete { None } else { due_rows.last().map(|&(id, lease)| (lease, id)) };
                for &(id, _) in &due_rows {
                    inner.ensure(BranchId(id))?;
                }
                let cat = inner.cat.as_mut().expect("still a catalog store");
                if complete {
                    cat.lease_floor = cat.catalog.lease_min_after(now)?;
                } else {
                    inner.expire_more = true;
                }
            }
        }
        let mut due: Vec<BranchId> = inner
            .leases
            .range(..=(now, BranchId(u64::MAX)))
            .take(expire_batch().saturating_add(1))
            .map(|&(_, id)| id)
            .collect();
        if due.len() > expire_batch() {
            due.truncate(expire_batch());
            inner.expire_more = true;
        }
        if due.is_empty() {
            if inner.leases_exist() {
                match stamp {
                    Stamp::No => {}
                    Stamp::Queue => {
                        if now >= inner.lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                            if let Some(journal) = inner.journal.as_mut() {
                                journal.buffer(&Record::Clock { now_ms: now })?;
                                inner.lease.queued(now);
                            }
                        }
                    }
                    Stamp::Flush => {
                        // Against what is DURABLE: a stamp only queued at this same instant is
                        // still in the buffer, and this flush is what carries it (review N3).
                        if now > inner.lease.durable_ms {
                            inner.lease.queued(now);
                            self.log(inner, Record::Clock { now_ms: now })?;
                        }
                    }
                }
            }
            return Ok(empty);
        }
        let mut by_depth = Vec::with_capacity(due.len());
        for id in due {
            by_depth.push((std::cmp::Reverse(inner.depth(id)?), id));
        }
        by_depth.sort();
        let due: Vec<BranchId> = by_depth.into_iter().map(|(_, id)| id).collect();
        let mut records: Vec<Record> = due
            .iter()
            .map(|id| Record::Release { branch: id.0 })
            .collect();
        records.push(Record::Clock { now_ms: now });
        inner.lease.queued(now);
        Ok(ExpirePlan { due, records, now })
    }

    /// The second half of an expiry pass, once its records are durable: release what was due and
    /// free what that frees. `fsyncs` is what the flush that carried the records cost the pass (0
    /// when it rode on another operation's flush); observation only.
    fn expire_apply(
        &self,
        inner: &mut StoreInner,
        plan: ExpirePlan,
        fsyncs: u64,
    ) -> Result<Expired> {
        self.expire_apply_at(inner, plan, fsyncs, None)
    }

    /// `expire_apply`, with the frees held until `defer_to` is durable when the pass's records ride
    /// on an early-released operation's flush (amendment 4). A release that fails part-way (a
    /// catalog read, a12-durable-open) fail-stops the store, so none of the pass's records is ever
    /// made durable.
    fn expire_apply_at(
        &self,
        inner: &mut StoreInner,
        plan: ExpirePlan,
        fsyncs: u64,
        defer_to: Option<u64>,
    ) -> Result<Expired> {
        // Review r12-merge1 N1, as in `release_many`: a release riding on a fork's flight can drop
        // the trunk's child count before that flight lands.
        if defer_to.is_some_and(|lsn| lsn > 0) {
            self.unsynced.store(true, Ordering::Release);
        }
        if let Some(lsn) = defer_to {
            inner.early_released = inner.early_released.max(lsn);
        }
        let due = plan.due;
        let mut freed = Vec::new();
        for &id in &due {
            if let Err(e) = inner.apply_release(id, &mut freed) {
                return Err(inner.fatal(e));
            }
        }
        let freed_pages = freed.len();
        {
            use churn_counters::*;
            EXPIRE_PASSES_WITH_DUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            EXPIRE_REAPED.fetch_add(due.len() as u64, std::sync::atomic::Ordering::Relaxed);
            EXPIRE_FREED_PAGES.fetch_add(freed_pages as u64, std::sync::atomic::Ordering::Relaxed);
            EXPIRE_FSYNCS.fetch_add(fsyncs, std::sync::atomic::Ordering::Relaxed);
        }
        match defer_to {
            Some(lsn) => inner.defer_frees(lsn, freed),
            None => inner.release_slots(freed),
        }
        self.sync_trunk_children(inner);
        self.sync_lease_flag(inner);
        Ok(Expired {
            reaped: due,
            freed_pages,
        })
    }

    /// Leases outstanding, and how many of them have run out (observation only, r11-churn).
    pub(crate) fn lease_counts(&self) -> (usize, usize) {
        let inner = self.inner.lock();
        let now = inner.lease.now_ms();
        let due = inner.leases.range(..=(now, BranchId(u64::MAX))).count();
        (inner.leases.len(), due)
    }

    /// Move the lease clock forward, for tests. It never moves back.
    pub(crate) fn advance_lease_clock(&self, by: Duration) {
        let mut inner = self.inner.lock();
        inner.lease.advanced_ms = inner.lease.advanced_ms.saturating_add(millis(by));
    }

    pub(crate) fn lease_now(&self) -> Duration {
        Duration::from_millis(self.inner.lock().lease.now_ms())
    }

    /// Stop real time moving the lease clock, for tests.
    pub(crate) fn freeze_lease_clock(&self) {
        self.inner.lock().lease.freeze();
    }

    /// Append `record` and make it durable before the caller acts on it. A no-op when volatile.
    fn log(&self, inner: &mut StoreInner, record: Record) -> Result<()> {
        {
            let StoreInner {
                journal,
                arena,
                failpoint,
                ..
            } = &mut *inner;
            let (Some(journal), Some(_)) = (journal.as_mut(), arena.as_ref()) else {
                return Ok(());
            };
            injected_flush_failure(failpoint, journal)?;
            journal.buffer(&record)?;
        }
        self.flush_locked(inner)?;
        inner.lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Compact the log into a snapshot if it has outgrown the live state. Best effort: a failure
    /// before the rename leaves the log, its buffer and the store healthy, and the triggering
    /// operation is made durable by its own flush (`commit_pages` and `release_many` compact
    /// BEFORE they wait, so it need not be durable yet); a failure after the rename fail-stops the
    /// journal (see `Journal::compact`).
    ///
    /// With `R11_CKPT=fuzzy`, a catalog store checkpoints FUZZILY instead (F-FZ): C-R's parked Commits
    /// first, a bounded batch per call; then a capture under this mutex, and the write on a thread of
    /// its own.
    fn maybe_compact(&self, inner: &mut StoreInner) {
        let wants =
            inner.force_compaction || inner.journal.as_ref().is_some_and(|j| j.wants_compaction());
        if !wants {
            return;
        }
        // A commit between its holds has its record buffered and half its pages mapped: a
        // snapshot now would drop the record and keep half the commit. The last hold of that
        // commit tries again. The same holds for a checkpoint's capture, sharp or fuzzy (merge
        // 1b(ii); `start_flight` and `checkpoint_capture` refuse too).
        if inner.publishing > 0 {
            inner.work.compactions_refused += 1;
            return;
        }
        if inner.cat.is_none() || !fuzzy_checkpoints() {
            let t = Instant::now();
            let fsyncs0 = super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed);
            if let Err(e) = self.compact(inner, false) {
                tracing::warn!("branch store compaction failed: {e}");
            }
            use churn_counters::*;
            let ns = t.elapsed().as_nanos() as u64;
            COMPACTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            COMPACT_NS_TOTAL.fetch_add(ns, std::sync::atomic::Ordering::Relaxed);
            COMPACT_NS_MAX.fetch_max(ns, std::sync::atomic::Ordering::Relaxed);
            COMPACT_FSYNCS.fetch_add(
                super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed) - fsyncs0,
                std::sync::atomic::Ordering::Relaxed,
            );
            if let Some(j) = inner.journal.as_ref() {
                COMPACT_BYTES_LAST.store(j.snapshot_len(), std::sync::atomic::Ordering::Relaxed);
            }
            return;
        }
        // Past twice the threshold with a checkpoint in flight: this operation waits for its
        // install once the mutex is released. Set only under the mutex while `flight` holds, and
        // cleared by the install under the same mutex, so it is never left set with no flight.
        // (During a WAL truncation no checkpoint starts and none waits: the log can pass twice the
        // threshold by what that truncation's time appends.)
        if !self.start_flight(inner)
            && inner.cat.as_ref().is_some_and(|c| c.flight)
            && inner.journal.as_ref().is_some_and(|j| j.past_hard_limit())
        {
            self.over_hard.store(true, Ordering::Release);
        }
    }

    /// F-FZ: start a fuzzy checkpoint unless one is in flight. Parked Commits (C-R) are settled
    /// first, at most `SETTLE_BATCH` branches per call, and the checkpoint waits for the next call
    /// while any remain. Returns whether a checkpoint started.
    fn start_flight(&self, inner: &mut StoreInner) -> bool {
        // No capture while a commit is between its holds (r11-bigtxn port; merge 1b(ii)): its record
        // is buffered ahead of the marker and half its pages are mapped. Refused and counted, as a
        // compaction is (this is reached so from `checkpoint_fuzzy_now`; `maybe_compact` refuses
        // first).
        if inner.publishing > 0 {
            inner.work.compactions_refused += 1;
            return false;
        }
        if inner.cat.as_ref().is_none_or(|c| c.flight)
            || inner.poisoned()
            || self.truncating.load(Ordering::Acquire)
        {
            return false;
        }
        let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if !inner.parked.is_empty() {
            let t = Instant::now();
            if let Err(e) = inner.settle_batch(SETTLE_BATCH) {
                tracing::warn!("branch store: parked commits not applied: {e}");
                let _ = inner.fatal(e);
                return false;
            }
            if let Some(cat) = inner.cat.as_mut() {
                cat.ckpt.hold(ns(t));
            }
            if !inner.parked.is_empty() {
                return false;
            }
        }
        let t = Instant::now();
        let q0 = inner.cat.as_ref().map_or(0, |c| c.catalog.counters.queries);
        let cap = match inner.checkpoint_capture(false, false, false) {
            Ok(cap) => cap,
            Err(e) => {
                tracing::warn!("branch catalog checkpoint not started: {e}");
                return false;
            }
        };
        let cat = inner.cat.as_mut().expect("captured above");
        cat.ckpt.hold(ns(t));
        cat.ckpt.stmts_locked += cat.catalog.counters.queries - q0;
        cat.ckpt.flights += 1;
        let writer = cat.writer.clone();
        let shared = self.inner.clone();
        let flight = FlightShared {
            hold: self.flight_hold.clone(),
            over_hard: self.over_hard.clone(),
            truncating: self.truncating.clone(),
            installs: self.installs.clone(),
            group: self.group.clone(),
            waits: self.locked_flight_waits.clone(),
        };
        let mut flights = self.flights.lock();
        if flights.len() >= FLIGHTS_KEPT {
            // Past its install long ago (only one checkpoint is ever in flight), so this join
            // waits for nothing.
            let _ = flights.remove(0).join();
        }
        // The thread takes the capture from a shared slot, so a failed spawn (whose closure is
        // dropped) still finds it here to undo (merge 1b(ii): it now carries the deferred frees it
        // took, which must come back).
        let slot = Arc::new(Mutex::new(Some(cap)));
        let taken = slot.clone();
        let spawned = crate::thread::Builder::new()
            .name("branch-checkpoint".to_string())
            .spawn(move || {
                let cap = taken.lock().take();
                if let Some(cap) = cap {
                    run_flight(shared, writer, cap, flight);
                }
            });
        match spawned {
            Ok(handle) => {
                flights.push(handle);
                true
            }
            Err(e) => {
                tracing::warn!("branch catalog checkpoint thread not started: {e}");
                // Nothing was written: undo the capture exactly as a failed write is undone (the
                // snapshot ended, `flight` cleared, the dirty map and the deferred frees put back).
                let unsent = slot.lock().take();
                if let Some(cap) = unsent {
                    let _ = inner.checkpoint_install(
                        cap,
                        Err(LimboError::InternalError(
                            "the branch catalog checkpoint's thread did not start".to_string(),
                        )),
                        false,
                    );
                }
                false
            }
        }
    }

    /// Wait for every fuzzy checkpoint thread started so far (F-FZ). Called without the store
    /// mutex: a thread in flight takes it for its install.
    fn join_flights(&self) {
        let handles = std::mem::take(&mut *self.flights.lock());
        for handle in handles {
            if handle.join().is_err() {
                tracing::warn!("a branch catalog checkpoint thread panicked");
            }
        }
    }

    fn compact(&self, inner: &mut StoreInner, fail_after_rename: bool) -> Result<()> {
        // No flight may be writing the log this truncates. The snapshot (in catalog mode, the
        // checkpoint) carries every applied operation, including the early-released ones still
        // buffered, so once it is durable so are they.
        let mut g = self.group.state.lock().unwrap();
        if g.flushing {
            self.locked_flight_waits.fetch_add(1, Ordering::Relaxed);
        }
        while g.flushing {
            g = self.group.cv.wait(g).unwrap();
        }
        if g.poisoned {
            return Err(group_poisoned());
        }
        g.flushing = true;
        drop(g);
        let lsn = inner.journal.as_ref().map_or(0, Journal::lsn);
        let generation = inner.committed_generation();
        let compacted = self.compact_quiesced(inner, fail_after_rename);
        // The commit point is the move to a new committed generation: from there the snapshot (or
        // the catalog) is the truth and holds everything applied. In snapshot mode that is the
        // journal's generation (the rename, then the log reset); in catalog mode it is the
        // catalog's COMMIT, which the log follows only once it is rewritten (r11-restart-r2): a
        // rewrite that fails before its rename leaves the old log in use, cut by recovery at the
        // checkpoint's marker, and the checkpoint durable all the same (merge 1b(ii)).
        let advanced = inner.committed_generation() != generation;
        let poisoned = inner.journal.as_ref().is_some_and(Journal::is_poisoned);
        match &compacted {
            // Everything applied is durable: past the commit point a later failure (the catalog's
            // lease floor, say) loses nothing (review 2 F1).
            Ok(()) | Err(_) if advanced && !poisoned => {
                inner.work.compactions += 1;
                self.land(lsn, true);
            }
            // Nothing was compacted (no catalog or arena yet): nothing more is durable.
            Ok(()) => self.land_unchanged(),
            // Failed before its commit point with the journal live (no I/O whose outcome is in
            // doubt: those poison it): nothing was lost and nothing more is durable. Only the
            // flight slot is released; poisoning the group here would take commits in memory and
            // fail every one at its wait (review H4).
            Err(_) if !poisoned => self.land_unchanged(),
            Err(_) => self.land(lsn, false),
        }
        compacted
    }

    /// End a compaction that made nothing durable and lost nothing: no flight is in the air now.
    fn land_unchanged(&self) {
        let mut g = self.group.state.lock().unwrap();
        g.flushing = false;
        self.group.cv.notify_all();
    }

    fn compact_quiesced(&self, inner: &mut StoreInner, fail_after_rename: bool) -> Result<()> {
        if inner.cat.is_some() {
            // Catalog mode compacts by checkpointing into the catalog (a12-durable-open), the sharp
            // form (r11-restart-r2); the checkpoint lists free the deferred frees it covers (see
            // `checkpoint_capture`).
            inner.checkpoint_catalog(fail_after_rename, false)?;
            self.unsynced.store(false, Ordering::Release);
            return Ok(());
        }
        let snapshot = inner.snapshot();
        let StoreInner {
            journal,
            arena,
            lease,
            ..
        } = inner;
        let (Some(journal), Some(arena)) = (journal.as_mut(), arena.as_mut()) else {
            return Ok(());
        };
        journal.compact(&snapshot, arena, fail_after_rename)?;
        // The snapshot carries the clock, and it replaced every buffered stamp.
        lease.queued(snapshot.lease_now_ms);
        lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Fork a child of the trunk. The caller must hold the trunk's WAL write lock: a trunk write
    /// transaction in flight across the fork would commit pages whose copy decision was taken for
    /// the previous epoch, and the new child would see them.
    ///
    /// Early release (amendment 4): the fork is applied and its records buffered; the returned log
    /// sequence number must be passed to `wait_durable` — after the caller has released the WAL
    /// write lock — before the branch is handed to anyone.
    pub(crate) fn fork_trunk(
        &self,
        schema: Arc<Schema>,
        page_size: usize,
    ) -> Result<(BranchId, u64)> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        let restart = inner.arena.as_ref().is_some_and(|a| a.page_size() != page_size);
        if restart {
            // `ensure_backing` may start an empty store over (a compaction that truncates the
            // log, or a sharp checkpoint that rewrites it): no flight may be writing it. None can
            // start while this holds the mutex.
            self.quiesce();
        }
        inner.ensure_backing(page_size)?;
        if restart {
            // It did (it refuses otherwise): the empty snapshot supersedes everything buffered.
            let lsn = inner.journal.as_ref().map_or(0, Journal::lsn);
            self.land(lsn, true);
            // Deferred frees name slots of the arena the restart replaced: returned to the new one
            // they would free slots it never handed out.
            inner.pending_free.clear();
        }
        self.mature(&mut inner);
        // The expiry pass rides on the fork's own flush (group commit, r11-churn amendment 2): the
        // same records in the same order as the pass's own flush followed by the fork's, made
        // durable by one flush, then freed, then the fork applied.
        let plan = self.expire_plan(&mut inner, Stamp::Queue)?;
        let now = plan.now;
        let id = BranchId(inner.next_id);
        let (fork_records, lease) = inner.fork_records(id, BranchId::TRUNK, now);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        let mut records = plan.records.clone();
        records.extend(fork_records);
        let lsn = self.buffer_all(&mut inner, records)?;
        if !plan.due.is_empty() {
            self.expire_apply_at(&mut inner, plan, 0, Some(lsn))?;
            churn_counters::EXPIRE_PIGGYBACKED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // A catalog read failing part-way leaves the fork half-applied: fail-stop, so its buffered
        // records are never made durable (a12-durable-open).
        if let Err(e) = inner.apply_fork(BranchId::TRUNK, id, Some(schema), Handle::Attached) {
            return Err(inner.fatal(e));
        }
        inner.apply_fork_lease(id, lease);
        self.sync_trunk_children(&inner);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok((id, lsn))
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock, and refused on a released branch.
    ///
    /// Early release (amendment 4), as `fork_trunk`: the caller waits on the returned log sequence
    /// number before handing the branch out.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<(BranchId, u64)> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        self.mature(&mut inner);
        // Reap what has expired first, so a parent whose lease ran out is refused rather than
        // revived by a child that would pin it (Neon refuses to "create children from expiring
        // branches"). The pass rides on the fork's own flush (group commit, r11-churn amendment 2);
        // when the fork is refused, the pass is completed alone, as before.
        let plan = self.expire_plan(&mut inner, Stamp::Queue)?;
        let now = plan.now;
        // Catalog stores: the parent made resident (ancestors first) before its state is read, as
        // the pass-then-ensure order did (a12-durable-open).
        inner.ensure(parent)?;
        // F-EXP (r11-restart-r2): a bounded pass may not have reached a parent whose lease ran out.
        // Such a parent is not forkable either; it is reaped on access below, exactly as the pass
        // would have reaped it. (A fail-stopped store reaps nothing: the fork goes on to its own
        // buffering, which refuses it, as the base's order did.)
        let lapsed = !inner.poisoned()
            && inner.branches.get(&parent).is_some_and(|st| {
                !st.handle.is_released() && st.lease.is_some_and(|deadline| deadline <= now)
            });
        let forkable = !lapsed
            && !plan.due.contains(&parent)
            && matches!(inner.branches.get(&parent),
                        Some(st) if !st.writer && !st.handle.is_released());
        if !forkable {
            // Refused: complete the pass alone, reap the parent if the pass did not reach it, then
            // refuse exactly as the pass-then-check order always did (gone, Busy, or reaped, in
            // that order).
            if !plan.due.is_empty() {
                let fsyncs0 = super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed);
                self.log_all(&mut inner, plan.records.clone())?;
                let fsyncs =
                    super::journal::FSYNCS.load(std::sync::atomic::Ordering::Relaxed) - fsyncs0;
                self.expire_apply(&mut inner, plan, fsyncs)?;
                self.maybe_compact(&mut inner);
            }
            self.reap_if_due(&mut inner, parent, now)?;
            let st = inner.branches.get(&parent).ok_or_else(|| gone(parent))?;
            if st.writer {
                return Err(LimboError::Busy);
            }
            return Err(reaped(parent));
        }
        let schema = inner.branches.get(&parent).expect("checked above").schema.clone();
        let id = BranchId(inner.next_id);
        let (fork_records, lease) = inner.fork_records(id, parent, now);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        let mut records = plan.records.clone();
        records.extend(fork_records);
        let lsn = self.buffer_all(&mut inner, records)?;
        if !plan.due.is_empty() {
            self.expire_apply_at(&mut inner, plan, 0, Some(lsn))?;
            churn_counters::EXPIRE_PIGGYBACKED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if let Err(e) = inner.apply_fork(parent, id, schema, Handle::Attached) {
            return Err(inner.fatal(e));
        }
        inner.apply_fork_lease(id, lease);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok((id, lsn))
    }

    /// Mark the branch open for a connection and return its committed schema (`None` after a
    /// reopen: the caller reparses it). One connection per branch: two would each hold a private
    /// page cache of the same page space, and nothing would tell one that the other had committed
    /// — a silently stale read, so it is refused.
    pub(crate) fn open_conn(&self, id: BranchId) -> Result<Option<Arc<Schema>>> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        // Nor is an expired branch openable: the same pass, the same refusal.
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        let poisoned = inner.poisoned().then(|| fail_stop_cause(inner.journal.as_ref()));
        inner.ensure(id)?;
        self.reap_if_due(&mut inner, id, now)?;
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.handle.is_released() {
            return Err(reaped(id));
        }
        // A fail-stopped pass cannot reap (it cannot make a Release durable), so the refusal that
        // reaping gives an expired branch is given here instead (review N4). Unexpired branches
        // stay readable.
        if let Some(cause) = poisoned {
            if st.lease.is_some_and(|deadline| deadline <= now) {
                return Err(LimboError::InvalidArgument(format!(
                    "branch {}'s lease has run out, and the branch store is {cause}",
                    id.0
                )));
            }
        }
        if st.handle != Handle::Attached {
            return Err(LimboError::InvalidArgument(format!(
                "branch {} has no attached handle; attach it with Database::branch first",
                id.0
            )));
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

    /// The connection on `id` has gone. Releases its write lock and reservations if a transaction
    /// was abandoned, and frees the branch if it was released meanwhile.
    pub(crate) fn close(&self, id: BranchId) {
        let mut inner = self.lock();
        let mut freed = Vec::new();
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
            freed.extend(std::mem::take(&mut st.pending).into_values());
        }
        if let Err(e) = inner.collect(id, &mut freed) {
            tracing::warn!("branch {} not collected at close: {e}", id.0);
            let _ = inner.fatal(e);
            return;
        }
        // Rule 2 (review r12-merge1 R2): a branch an early release kept alive because it was open is
        // freed here, and its Release may still be only buffered. Hold the frees until every early
        // Release so far is durable; `mature` returns them at once when it already is. (r11-churn
        // gc3 b1f323eed; it supersedes r11-bigtxn's H3, which deferred to the journal's current
        // position: equally safe, looser.)
        let lsn = inner.early_released;
        inner.defer_frees(lsn, freed);
        self.mature(&mut inner);
        self.sync_trunk_children(&inner);
    }

    /// The `Branch` handle has gone: release the branch.
    ///
    /// An error when the release could not be made durable (review N4): the branch is then kept —
    /// nothing is freed in this process — and comes back, detached, at the next open.
    pub(crate) fn release_handle(&self, id: BranchId) -> Result<Reaped> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        inner.ensure(id)?;
        let Some(st) = inner.branches.get(&id) else {
            return Ok(Reaped {
                freed_pages: 0,
                deferred: false,
            });
        };
        match st.handle {
            Handle::Released => {
                return Ok(Reaped {
                    freed_pages: 0,
                    deferred: true,
                })
            }
            Handle::ReleasePending => {
                return Err(fail_stopped(inner.journal.as_ref(), id, "no release"))
            }
            Handle::Attached | Handle::Detached => {}
        }
        if let Err(e) = self.log(&mut inner, Record::Release { branch: id.0 }) {
            // The release is not durable, so nothing may be freed — now or ever in this process:
            // after a restart the branch comes back (detached), and its slots must still hold
            // what it names. ReleasePending is the state `collect` never frees.
            tracing::warn!("branch {} released in memory only: {e}", id.0);
            if let Some(st) = inner.branches.get_mut(&id) {
                st.handle = Handle::ReleasePending;
            }
            return Err(LimboError::InternalError(format!(
                "branch {} was not released durably ({e}); it is kept, and comes back at the next \
                 open",
                id.0
            )));
        }
        let mut freed = Vec::new();
        if let Err(e) = inner.apply_release(id, &mut freed) {
            return Err(inner.fatal(e));
        }
        let freed_pages = freed.len();
        inner.release_slots(freed);
        self.sync_trunk_children(&inner);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok(Reaped {
            freed_pages,
            deferred: inner.branches.contains_key(&id),
        })
    }

    /// Release several branches with ONE durable flush: group commit for reaps (r11-churn, PREREG
    /// amendment 2). Every Release record is buffered and one flush makes them all durable before
    /// anything is freed — rule 2, per batch instead of per branch, as `expire` already does for
    /// leases and as ZFS frees a transaction group's destroys at its one sync. The releases are then
    /// applied in the given order, so a chain released root first defers each interior until its
    /// tip frees it, exactly as one `release_handle` per branch would. If the flush fails, every
    /// branch of the batch is kept (`ReleasePending`), as `release_handle` keeps one.
    pub(crate) fn release_many(&self, ids: &[BranchId]) -> Result<Vec<Reaped>> {
        // F-FZ back-pressure, as every operation that grows the log (merge 1b(ii)): dropped after
        // the guard below, and after this batch's own wait for durability.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        let mut out = vec![
            Reaped {
                freed_pages: 0,
                deferred: false,
            };
            ids.len()
        ];
        let mut todo = Vec::with_capacity(ids.len());
        let mut seen = std::collections::HashSet::with_capacity(ids.len());
        for (i, &id) in ids.iter().enumerate() {
            // Catalog stores: the branch made resident first, as `release_handle` does; a branch
            // not resident would otherwise be skipped, unreleased.
            inner.ensure(id)?;
            let Some(st) = inner.branches.get(&id) else {
                continue;
            };
            match st.handle {
                Handle::Released => out[i].deferred = true,
                Handle::ReleasePending => {
                    return Err(fail_stopped(inner.journal.as_ref(), id, "no release"))
                }
                Handle::Attached | Handle::Detached => {
                    if seen.insert(id) {
                        todo.push(i);
                    } else {
                        out[i].deferred = true;
                    }
                }
            }
        }
        if todo.is_empty() {
            return Ok(out);
        }
        let records = todo
            .iter()
            .map(|&i| Record::Release { branch: ids[i].0 })
            .collect();
        // Early release (amendment 4): buffer the batch, apply it, and wait for the flight with the
        // mutex released; the slots it frees return to the arena only once it is durable.
        let lsn = match self.buffer_all(&mut inner, records) {
            Ok(lsn) => lsn,
            Err(e) => {
                tracing::warn!("{} branches released in memory only: {e}", todo.len());
                for &i in &todo {
                    if let Some(st) = inner.branches.get_mut(&ids[i]) {
                        st.handle = Handle::ReleasePending;
                    }
                }
                return Err(LimboError::InternalError(format!(
                    "{} branches were not released durably ({e}); they are kept, and come back at \
                     the next open",
                    todo.len()
                )));
            }
        };
        // Review r12-merge1 N1: the releases below can drop the trunk's child count before this
        // batch is durable, and a trunk commit that then retains no pre-image must not become
        // durable ahead of it. `unsynced` sends that commit's barrier down its slow path, which
        // waits out this batch's flight and is refused if it failed. Stored before
        // `sync_trunk_children` publishes the new count, so a writer that sees the count sees this.
        if lsn > 0 {
            self.unsynced.store(true, Ordering::Release);
        }
        inner.early_released = inner.early_released.max(lsn);
        let mut freed = Vec::new();
        for &i in &todo {
            let before = freed.len();
            // A catalog read failing part-way leaves the batch half-applied: fail-stop, so none of
            // its buffered records is ever made durable (a12-durable-open).
            if let Err(e) = inner.apply_release(ids[i], &mut freed) {
                return Err(inner.fatal(e));
            }
            out[i].freed_pages = freed.len() - before;
            out[i].deferred = inner.branches.contains_key(&ids[i]);
        }
        inner.defer_frees(lsn, freed);
        self.sync_trunk_children(&inner);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        drop(inner);
        self.wait_durable(lsn)?;
        Ok(out)
    }

    /// Detach a live branch from its handle without releasing it.
    pub(crate) fn detach(&self, id: BranchId) {
        let mut inner = self.lock();
        let _ = inner.ensure(id);
        if let Some(st) = inner.branches.get_mut(&id) {
            if st.handle == Handle::Attached {
                st.handle = Handle::Detached;
            }
        }
    }

    /// Give a detached branch a handle again. One handle per branch.
    pub(crate) fn attach(&self, id: BranchId) -> Result<()> {
        self.refuse_if_trunk_only("attaching a branch")?;
        let mut inner = self.lock();
        inner.ensure(id)?;
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        match st.handle {
            Handle::Detached => {
                st.handle = Handle::Attached;
                Ok(())
            }
            Handle::Attached => Err(LimboError::InvalidArgument(format!(
                "branch {} is already attached to a handle",
                id.0
            ))),
            Handle::Released | Handle::ReleasePending => Err(reaped(id)),
        }
    }

    /// Every unreleased branch. Refused on a trunk-only store, where "none" would be a lie.
    pub(crate) fn ids(&self) -> Result<Vec<BranchId>> {
        self.refuse_if_trunk_only("listing branches")?;
        let mut inner = self.inner.lock();
        let mut ids: Vec<BranchId> = inner
            .branches
            .iter()
            .filter(|(_, st)| !st.handle.is_released())
            .map(|(&id, _)| id)
            .collect();
        // Catalog stores: every unreleased row, less what memory knows better (resident states
        // are listed above; removed ones are gone).
        let StoreInner { cat, branches, .. } = &mut *inner;
        if let Some(cat) = cat.as_mut() {
            for id in cat.catalog.unreleased_ids()?.into_iter().map(BranchId) {
                if !branches.contains_key(&id) && !cat.removed.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// Take the store mutex for the mechanism. Every hold it returns is counted in
    /// [`BranchWork::lock_holds`] and folded into [`HoldMax`]; the observation calls (`stats`, the
    /// membership diagnostics, the counters) lock directly so they do not count themselves.
    fn lock(&self) -> Hold<'_> {
        Hold::of(self.inner.lock())
    }

    /// The per-hold maxima since the previous call, which this call resets.
    pub(crate) fn take_hold_max(&self) -> HoldMax {
        std::mem::take(&mut self.inner.lock().hold_max)
    }

    /// The hold-duration histogram since the previous call (bucket b holds durations in
    /// [2^(b/8), 2^((b+1)/8)) ns), which this call resets. Empty unless hold timing is on.
    pub(crate) fn take_hold_hist(&self) -> Vec<u64> {
        let mut inner = self.inner.lock();
        let hist = inner.hold_hist.to_vec();
        inner.hold_hist = [0; HOLD_HIST_BUCKETS];
        hist
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        // A fail-stopped store takes no write: a commit would write its pages into the arena
        // before its record failed, possibly into a slot durable state still names (review R1).
        if inner.poisoned() {
            return Err(fail_stopped(inner.journal.as_ref(), id, "no write transaction"));
        }
        inner.ensure(id)?;
        // F-EXP: a branch whose lease ran out takes no write, though no bounded pass reached it.
        let now = inner.lease.now_ms();
        self.reap_if_due(&mut inner, id, now)?;
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        // A released branch takes no writes: its Release record is already durable, and a commit
        // logged after it would name a branch that recovery has already freed.
        if st.handle.is_released() {
            return Err(reaped(id));
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

    /// A branch transaction rolled back: its reservations were never published, so they are free.
    pub(crate) fn abort_write(&self, id: BranchId) {
        self.discard(id, &mut ShadowTxn::default());
    }

    /// Roll back branch `id`'s transaction: return every slot it reserved, in holds of at most
    /// [`HOLD_BATCH`]. No durable record names them (only a commit's own does, and it is taken out
    /// of `pending` as it maps), so they are free at once.
    pub(crate) fn discard(&self, id: BranchId, txn: &mut ShadowTxn) {
        txn.pages.clear();
        loop {
            let mut inner = self.lock();
            let Some(st) = inner.branches.get_mut(&id) else {
                return;
            };
            let mut batch = Vec::with_capacity(HOLD_BATCH);
            while batch.len() < HOLD_BATCH {
                match st.pending.pop_first() {
                    Some((_, slot)) => batch.push(slot),
                    None => break,
                }
            }
            if batch.is_empty() {
                return;
            }
            inner.release_slots(batch);
        }
    }

    /// The first write of `page` in a transaction on branch `id`: reserve its slot (the copy
    /// decision `first_write_branch` makes) and return the transaction's writer for it, or `None`
    /// if the transaction has it already. The transaction records it after this returns, so the
    /// growth of its own map is never inside a hold.
    pub(crate) fn shadow_slot(
        &self,
        id: BranchId,
        page: u32,
        txn: &ShadowTxn,
    ) -> Result<Option<(Slot, SlotPtr)>> {
        if txn.pages.contains_key(&page) {
            return Ok(None);
        }
        self.reserve_slot(id, page).map(Some)
    }

    /// Commit branch `id`'s transaction (r11-bigtxn F-shadow, ported onto the durable store with
    /// r11-churn's group commit). Every page of `dirty` is in its slot already, filled WITHOUT the
    /// store mutex (at a spill, or by the caller just before this); the `Commit` record is encoded
    /// here, without it too. Then:
    /// 1. one hold checks fail-stop, notes the arena's unsynced writes (rule 1: a record is durable
    ///    only after the slots it names) and hands the journal the record by move;
    /// 2. holds of at most [`HOLD_BATCH`] pages map the pages, each page's copy decision taken in
    ///    the hold that maps it (the version it supersedes is retained for a live child that can
    ///    still see it, else freed), and every freed slot waits in `pending_free` under the
    ///    record's log position (rule 2);
    /// 3. holds of at most [`HOLD_BATCH`] return the reservations of pages that are not dirty (a
    ///    statement rollback dropped them; no record names them);
    /// 4. with no lock held, the caller waits for the record to be durable (the group's flight).
    ///
    /// Nothing observes the branch between the holds: its one connection is the committer,
    /// `fork_branch` refuses while the writer flag is set, and no compaction or catalog checkpoint
    /// runs while `publishing` counts this commit. A crash replays the record whole or not at all.
    pub(crate) fn publish(&self, id: BranchId, txn: &mut ShadowTxn, dirty: &[u32]) -> Result<()> {
        // F-FZ back-pressure (r11-restart-r2 put it on `commit_pages`, which now rides on this):
        // dropped last, with no lock held, after this commit's own wait for durability.
        let _backpressure = Backpressure(self);
        let mut shadows = std::mem::take(&mut txn.pages);
        let mut entries = Vec::with_capacity(dirty.len());
        for &page in dirty {
            let s = shadows.remove(&page).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "branch {} committed page {page} with no copy decision behind it",
                    id.0
                ))
            })?;
            if !s.filled {
                return Err(LimboError::InternalError(format!(
                    "branch {} committed page {page} whose slot was never filled",
                    id.0
                )));
            }
            entries.push((page, s.slot, s.crc));
        }
        let leftover: Vec<u32> = shadows.into_keys().collect();
        if entries.is_empty() {
            self.return_reserved(id, &leftover);
            return Ok(());
        }
        let record = Record::Commit {
            branch: id.0,
            pages: entries,
        };
        let frame = super::journal::encode_frame(&record);
        let Record::Commit { pages: entries, .. } = record else {
            unreachable!("built as a Commit just above")
        };
        let named: Vec<Slot> = entries.iter().map(|&(_, slot, _)| slot).collect();

        // 1. The record.
        let lsn = {
            let mut inner = self.lock();
            self.mature(&mut inner);
            // Refuse BEFORE the record: after the journal failed, it can never be durable. The
            // slots stay reserved; the rollback that follows returns them.
            if inner.poisoned() {
                return Err(fail_stopped(inner.journal.as_ref(), id, "no commit"));
            }
            let st = inner.branches.get(&id).ok_or_else(|| gone(id))?;
            if st.handle.is_released() {
                return Err(reaped(id));
            }
            // Bytes the transaction wrote into these slots through their `SlotPtr`s.
            let bytes = named.len() as u64
                * inner.arena.as_ref().map_or(0, |a| a.page_size()) as u64;
            if inner.failpoint == Some(BranchFailpoint::CommitAfterSlotsBeforeRecord) {
                inner.failpoint = None;
                let StoreInner {
                    arena,
                    branches,
                    journal,
                    orphans,
                    ..
                } = &mut *inner;
                let arena = arena.as_mut().expect("a branch exists, so the arena does");
                arena.note_unsynced_writes(bytes);
                arena.sync()?;
                *orphans = named.clone();
                let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
                for (_, slot) in std::mem::take(&mut st.pending) {
                    arena.release(slot);
                }
                if let Some(journal) = journal.as_mut() {
                    journal.poison();
                }
                return Err(LimboError::InternalError(
                    "failpoint: branch commit stopped after its slots, before its record"
                        .to_string(),
                ));
            }
            inner
                .arena
                .as_mut()
                .expect("a branch exists, so the arena does")
                .note_unsynced_writes(bytes);
            // The commit pays for a flush anyway: while a lease is outstanding, stamp the clock in
            // it, so a crash cannot lose the open time an agent spent committing (review R2). A
            // lease on a catalog row not loaded counts (r11-restart-r2, `leases_exist`).
            let mut after = Vec::new();
            if inner.leases_exist() {
                let now = inner.lease.now_ms();
                if now > inner.lease.queued_ms {
                    after.push(Record::Clock { now_ms: now });
                    inner.lease.queued(now);
                }
            }
            let lsn = self.buffer_frame_then(&mut inner, frame, named, after)?;
            inner.publishing += 1;
            lsn
        };
        // Held from here to the last hold, and released on every exit (a panic included).
        let publishing = Publishing(self);
        // A volatile store frees at once; its superseded versions are freed after the map, in
        // their own holds, so no hold maps a batch AND frees one.
        let mut to_free: Vec<Slot> = Vec::new();
        // 2. The map, in bounded holds.
        let mapped = (|| -> Result<()> {
            for (i, batch) in entries.chunks(HOLD_BATCH).enumerate() {
                let mut inner = self.lock();
                if i == 1 {
                    // A crash between two holds of mapping, for the tests (amendment 8b).
                    let stop = inner.failpoint;
                    if matches!(
                        stop,
                        Some(
                            BranchFailpoint::CommitBetweenMapHolds
                                | BranchFailpoint::CommitBetweenMapHoldsUndurable
                        )
                    ) {
                        inner.failpoint = None;
                        if stop == Some(BranchFailpoint::CommitBetweenMapHolds) {
                            drop(inner);
                            self.wait_durable(lsn)?;
                            inner = self.lock();
                        }
                        if let Some(journal) = inner.journal.as_mut() {
                            journal.poison();
                        }
                        return Err(LimboError::InternalError(
                            "failpoint: branch commit stopped between two holds of its map"
                                .to_string(),
                        ));
                    }
                    if stop == Some(BranchFailpoint::CompactBetweenMapHolds) {
                        inner.failpoint = None;
                        drop(inner);
                        // Refused (Busy) while this commit is between its holds; the commit goes
                        // on either way, and the test reads what a crash after it recovers.
                        let _ = self.compact_now();
                        inner = self.lock();
                    }
                    if stop == Some(BranchFailpoint::MaybeCompactBetweenMapHolds) {
                        inner.failpoint = None;
                        inner.force_compaction = true;
                        self.maybe_compact(&mut inner);
                        inner.force_compaction = false;
                    }
                }
                if let Some(st) = inner.branches.get_mut(&id) {
                    for &(page, _, _) in batch {
                        st.pending.remove(&page);
                    }
                }
                let mut freed = Vec::new();
                if let Err(e) = inner.apply_commit(id, batch, &mut freed) {
                    return Err(inner.fatal(e));
                }
                inner.hold.pages += batch.len() as u64;
                if lsn == 0 {
                    to_free.extend(freed);
                } else {
                    // This hold decided retain-or-free against the children as they are NOW, so
                    // its frees wait for everything buffered so far (a child's Release buffered
                    // after this commit's record included), not only for the record (review H2).
                    let now = inner.journal.as_ref().map_or(lsn, Journal::lsn);
                    inner.defer_frees(now, freed);
                }
            }
            Ok(())
        })();
        for batch in to_free.chunks(HOLD_BATCH) {
            self.lock().release_slots(batch.to_vec());
        }
        drop(publishing);
        if mapped.is_ok() {
            let mut inner = self.lock();
            self.maybe_compact(&mut inner);
        }
        mapped?;
        // 3. The reservations no page used.
        self.return_reserved(id, &leftover);
        // 4. Durability, with no lock held.
        self.wait_durable(lsn)
    }

    /// Pending slots whose free waits for durability (observation only).
    pub(crate) fn pending_free_slots(&self) -> usize {
        self.inner
            .lock()
            .pending_free
            .iter()
            .map(|(_, freed)| freed.len())
            .sum()
    }

    /// The failpoint armed and not yet spent (observation only).
    pub(crate) fn failpoint_pending(&self) -> Option<BranchFailpoint> {
        self.inner.lock().failpoint
    }

    /// Return the reservations of `pages` (not dirty at commit) to the arena, in bounded holds.
    fn return_reserved(&self, id: BranchId, pages: &[u32]) {
        for batch in pages.chunks(HOLD_BATCH) {
            let mut inner = self.lock();
            let Some(st) = inner.branches.get_mut(&id) else {
                return;
            };
            let slots: Vec<Slot> = batch.iter().filter_map(|p| st.pending.remove(p)).collect();
            inner.release_slots(slots);
        }
    }

    /// `buffer_all` for a commit whose record was encoded without the mutex: the frame goes in by
    /// move, then `after` (small records) behind it.
    fn buffer_frame_then(
        &self,
        inner: &mut StoreInner,
        frame: Vec<u8>,
        named: Vec<Slot>,
        after: Vec<Record>,
    ) -> Result<u64> {
        let StoreInner {
            journal, failpoint, ..
        } = inner;
        let Some(journal) = journal.as_mut() else {
            return Ok(0);
        };
        injected_flush_failure(failpoint, journal)?;
        journal.check_live()?;
        if *failpoint == Some(BranchFailpoint::GroupFlightFails) {
            *failpoint = None;
            // Fails inside the flight's write, after this operation is applied (amendment 4).
            journal.fail_next_write();
        }
        journal.buffer_frame(frame, named)?;
        for record in &after {
            journal.buffer(record)?;
        }
        Ok(journal.lsn())
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.inner
            .lock()
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.lock();
        inner.ensure(id)?;
        inner
            .branches
            .get(&id)
            .ok_or_else(|| gone(id))?
            .schema
            .clone()
            .ok_or_else(|| {
                LimboError::InternalError(format!("branch {} has no schema loaded yet", id.0))
            })
    }

    pub(crate) fn set_schema(&self, id: BranchId, schema: Arc<Schema>) -> Result<()> {
        let mut inner = self.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = Some(schema);
        Ok(())
    }

    /// The copy decision for a branch's first write to `page` in a transaction: reserve the fresh
    /// slot the commit will write the page into. Nothing is published until then.
    pub(crate) fn first_write_branch(&self, id: BranchId, page: u32) -> Result<()> {
        self.reserve_slot(id, page).map(|_| ())
    }

    /// `first_write_branch`, returning the page's reserved slot and its writer from the same hold.
    fn reserve_slot(&self, id: BranchId, page: u32) -> Result<(Slot, SlotPtr)> {
        let mut inner = self.lock();
        self.mature(&mut inner);
        // A transaction that began before the journal failed may write no further page.
        if inner.poisoned() {
            return Err(fail_stopped(inner.journal.as_ref(), id, "no page write"));
        }
        inner.refill_free()?;
        let StoreInner {
            arena,
            branches,
            hold,
            ..
        } = &mut *inner;
        let arena = arena.as_mut().expect("a branch exists, so the arena does");
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if !st.writer {
            return Err(LimboError::InternalError(format!(
                "branch {} page {page} written outside a write transaction",
                id.0
            )));
        }
        let slot = match st.pending.entry(page) {
            std::collections::btree_map::Entry::Vacant(e) => {
                hold.pages += 1;
                *e.insert(arena.alloc())
            }
            std::collections::btree_map::Entry::Occupied(e) => *e.get(),
        };
        Ok((slot, arena.slot_ptr(slot)))
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child. The
    /// record waits in the journal until the trunk's commit barrier makes it durable.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) -> Result<()> {
        // The second fence of a trunk-only store, behind the connection's read-only check: this
        // write would retain no pre-image for the branches on disk.
        self.refuse_if_trunk_only("a trunk page write")?;
        let mut inner = self.lock();
        // Deferred frees a flush has covered go back to the arena before it allocates (amendment 4).
        self.mature(&mut inner);
        // Catalog stores: the page's `written` epoch, from its last catalog version, first.
        inner.trunk_written_known(page)?;
        let epoch = inner.trunk.lineage.epoch;
        let born = inner.trunk.written.get(&page).copied().unwrap_or(0);
        if born >= epoch {
            return Ok(());
        }
        let keep = {
            let StoreInner { children, cat, .. } = &mut *inner;
            children.any_in(cat.as_mut().map(|c| &mut c.catalog), BranchId::TRUNK, born, epoch)?
        };
        if keep {
            inner.refill_free()?;
        }
        let StoreInner {
            arena,
            trunk,
            journal,
            hold,
            ..
        } = &mut *inner;
        if keep {
            if journal.as_ref().is_some_and(|j| j.is_poisoned()) {
                return Err(LimboError::InternalError(format!(
                    "the trunk would overwrite a page a durable branch reads, but the branch store \
                     is {}",
                    fail_stop_cause(journal.as_ref())
                )));
            }
            let arena = arena.as_mut().expect("the trunk has a child, so the arena exists");
            // r11-restart lane instrument: who asked for this pre-image (observing only).
            if std::env::var_os("R11_TRACE_TRUNK_RETAIN").is_some() {
                eprintln!(
                    "R11_TRACE_TRUNK_RETAIN page={page} born={born} epoch={epoch}\n{}",
                    std::backtrace::Backtrace::force_capture()
                );
            }
            let slot = arena.alloc();
            arena.write_slot(slot, pre_image)?;
            hold.pages += 1;
            let crc = crc32c::crc32c(pre_image);
            let retained = Retained {
                born,
                died: epoch,
                slot,
                crc,
            };
            trunk.lineage.retain(page, retained);
            if let Some(journal) = journal.as_mut() {
                journal.buffer(&Record::TrunkRetain {
                    page,
                    born,
                    died: epoch,
                    slot,
                    crc,
                })?;
                self.unsynced.store(true, Ordering::Release);
            }
        }
        trunk.written.insert(page, epoch);
        Ok(())
    }

    /// Make every buffered trunk pre-image durable. `Pager::commit_wal` calls this before it writes
    /// a single frame, so a trunk commit is never durable ahead of the pre-images it overwrote.
    ///
    /// While a lease is outstanding it also stamps the lease clock, at most once per
    /// `STAMP_EVERY_MS` (review N2), and flushes a stamp still only queued.
    pub(crate) fn durability_barrier(&self) -> Result<()> {
        if !self.unsynced.load(Ordering::Acquire)
            && !self.leases_outstanding.load(Ordering::Acquire)
        {
            return Ok(());
        }
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock();
        // Re-read under the lock: `first_write_trunk` sets it while holding it.
        let unsynced = self.unsynced.load(Ordering::Acquire);
        // A lease on a catalog row not loaded counts too (r11-restart-r2).
        let leases_exist = inner.leases_exist();
        if inner.journal.is_none() || inner.arena.is_none() {
            return Ok(());
        }
        if !unsynced {
            // Only stamps are at stake: the buffer holds nothing else without `unsynced`. A trunk
            // commit that needs no pre-image does not fail because its stamp could not be written;
            // a lost stamp lengthens leases and loses no data. But a failed flush POISONS the
            // journal, as every failed flush does: from then on every branch write and every trunk
            // commit that needs a pre-image is refused until the database is reopened.
            let flush = {
                let StoreInner {
                    journal,
                    failpoint,
                    lease,
                    ..
                } = &mut *inner;
                let journal = journal.as_mut().expect("checked above");
                if journal.is_poisoned() || !leases_exist {
                    return Ok(());
                }
                let now = lease.now_ms();
                if now >= lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                    journal.buffer(&Record::Clock { now_ms: now })?;
                    lease.queued(now);
                }
                if lease.queued_ms > lease.durable_ms {
                    if *failpoint == Some(BranchFailpoint::StampFlushFails) {
                        *failpoint = None;
                        // Fails inside the flush, so the poisoning is the flush's own (review 4 C7).
                        journal.fail_next_write();
                    }
                    true
                } else {
                    false
                }
            };
            if flush {
                match self.flush_locked(&mut inner) {
                    Ok(()) => inner.lease.flushed(),
                    Err(e) => {
                        tracing::warn!("branch lease clock not stamped at a trunk commit: {e}")
                    }
                }
            }
            return Ok(());
        }
        {
            let StoreInner {
                journal,
                failpoint,
                orphans,
                lease,
                ..
            } = &mut *inner;
            let journal = journal.as_mut().expect("checked above");
            if *failpoint == Some(BranchFailpoint::BarrierBeforeRecords) {
                *failpoint = None;
                *orphans = journal.pending_slot_list();
                journal.poison();
                return Err(LimboError::InternalError(
                    "failpoint: the trunk commit's branch barrier stopped before its records"
                        .to_string(),
                ));
            }
            if leases_exist {
                let now = lease.now_ms();
                if now >= lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                    journal.buffer(&Record::Clock { now_ms: now })?;
                    lease.queued(now);
                }
            }
        }
        self.flush_locked(&mut inner)?;
        inner.lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        self.maybe_compact(&mut inner);
        Ok(())
    }

    /// Commit a branch's dirty pages given as resident pages (the store's own tests drive a
    /// transaction this way; a pager uses `shadow_slot`/`publish` with its `ShadowTxn`): write each
    /// into the slot `first_write_branch` reserved, without the store mutex, then `publish`.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        // Back-pressure and the lease stamp (r11-restart-r2's additions to this function) are in
        // `publish`, which this rides on (merge 1b(ii)).
        let mut txn = ShadowTxn::default();
        {
            let inner = self.lock();
            // Refuse BEFORE any slot is written: after the journal failed, this commit's record
            // can never be durable, so its pages have no business in the arena.
            if inner.poisoned() {
                return Err(fail_stopped(inner.journal.as_ref(), id, "no commit"));
            }
            let st = inner.branches.get(&id).ok_or_else(|| gone(id))?;
            if st.handle.is_released() {
                return Err(reaped(id));
            }
            let arena = inner.arena.as_ref().expect("a branch exists, so the arena does");
            for (&page, &slot) in &st.pending {
                txn.insert(page, slot, arena.slot_ptr(slot));
            }
        }
        let mut dirty = Vec::with_capacity(pages.len());
        for page in pages {
            let no = page.get().id as u32;
            if !txn.pages.contains_key(&no) {
                return Err(LimboError::InternalError(format!(
                    "branch {} committed page {no} with no copy decision behind it",
                    id.0
                )));
            }
            txn.fill(no, page.get_contents().as_slice())?;
            dirty.push(no);
        }
        self.publish(id, &mut txn, &dirty)
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.lock();
        self.resolve_calls.fetch_add(1, Ordering::Relaxed);
        let (mut levels, mut examined) = (0, 0);
        let resolved = inner.resolve(id, page, &mut levels, &mut examined);
        inner.work.resolve_calls += 1;
        inner.work.resolve_levels += levels;
        inner.work.resolve_retained_examined += examined;
        let Some((slot, crc)) = resolved? else {
            return Ok(false);
        };
        self.arena_reads.fetch_add(1, Ordering::Relaxed);
        let arena = inner
            .arena
            .as_ref()
            .expect("a slot resolved, so the arena exists");
        arena.read_slot(slot, out)?;
        // A slot on disk is checked on every read: a torn or rotted page is an error, never a
        // silently wrong page.
        if arena.is_file_backed() && crc32c::crc32c(out) != crc {
            return Err(LimboError::Corrupt(format!(
                "branch {} page {page} (arena slot {slot}) failed its checksum",
                id.0
            )));
        }
        Ok(true)
    }

    pub(crate) fn open_stats(&self) -> BranchOpenStats {
        self.open_stats
    }

    pub(crate) fn trunk_retained_count(&self) -> u64 {
        let mut inner = self.inner.lock();
        // In memory: every version (eager) or the versions retained since the last checkpoint.
        let resident: u64 = inner.trunk.lineage.retained.values().map(|v| v.len() as u64).sum();
        // Catalog stores: plus the catalog's, less those reaped since (an instrument's count).
        let StoreInner { cat, .. } = &mut *inner;
        let others = match cat.as_mut() {
            Some(cat) => cat
                .catalog
                .trunk_count()
                .unwrap_or_default()
                .saturating_sub(cat.trunk_gone.len() as u64),
            None => 0,
        };
        resident + others
    }

    /// Catalog statements that wrote a row, since open (r11-restart lane instrument).
    pub(crate) fn catalog_rows_written(&self) -> u64 {
        self.inner
            .lock()
            .cat
            .as_ref()
            .map_or(0, |c| c.catalog.counters.rows_written)
    }

    /// `(branch states read from the catalog, trunk pages read, catalog queries, catalog rows
    /// read)` since open (r11-restart lane instrument; zeros for a snapshot store).
    pub(crate) fn catalog_counters(&self) -> (u64, u64, u64, u64) {
        let inner = self.inner.lock();
        inner.cat.as_ref().map_or((0, 0, 0, 0), |c| {
            (
                c.branch_loads,
                c.trunk_page_loads,
                c.catalog.counters.queries,
                c.catalog.counters.rows_read,
            )
        })
    }

    pub(crate) fn read_counters(&self) -> (u64, u64) {
        (
            self.resolve_calls.load(Ordering::Relaxed),
            self.arena_reads.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn stats(&self) -> Result<BranchStats> {
        self.refuse_if_trunk_only("branch statistics")?;
        let mut inner = self.inner.lock();
        // The slot counts the log describes: deferred frees a flush has covered returned
        // (amendment 4), and parked Commits applied (C-R), first.
        self.mature_all(&mut inner);
        inner.settle()?;
        Ok(BranchStats {
            live_branches: inner.n_states as usize,
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner
                .arena
                .as_ref()
                .map_or(0, |a| a.high_water() as usize - a.in_use()),
            work: {
                let mut w = inner.work;
                let j = inner.journal.as_ref();
                w.sync_locked_bytes = j.map_or(0, |j| j.synced_locked_bytes)
                    + self.flight_locked_bytes.load(Ordering::Relaxed);
                w.sync_unlocked_bytes = self.flight_unlocked_bytes.load(Ordering::Relaxed);
                w.journal_copied_bytes = j.map_or(0, |j| j.copied_bytes);
                w.journal_handed_bytes = j.map_or(0, |j| j.handed_bytes);
                w.compact_locked_bytes += j.map_or(0, |j| j.compact_synced_bytes);
                w.locked_flight_waits = self.locked_flight_waits.load(Ordering::Relaxed);
                w
            },
        })
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let mut inner = self.inner.lock();
        self.mature_all(&mut inner);
        if let Err(e) = inner.settle() {
            tracing::warn!("branch store: parked commits not applied: {e}");
        }
        let _ = inner.ensure(id);
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
        let mut inner = self.inner.lock();
        // Deferred frees a flush has covered are free (amendment 4): return them first.
        self.mature_all(&mut inner);
        if let Err(e) = inner.settle() {
            tracing::warn!("branch store: parked commits not applied: {e}");
        }
        let StoreInner { arena, cat, .. } = &mut *inner;
        let Some(arena) = arena.as_ref() else {
            return Vec::new();
        };
        let Some(cat) = cat.as_mut() else {
            return arena.slots_in_use();
        };
        // Catalog stores: a slot the catalog lists free and this process has not taken is free
        // too, though the arena's bitmap does not say so.
        // Only rows this process has not fetched: a fetched row stays in the table until the next
        // checkpoint, whether its slot is on the in-memory free list (the bitmap says so) or in use.
        let cursor = cat.free_cursor;
        let listed: HashSet<Slot> = cat
            .catalog
            .free_all()
            .unwrap_or_default()
            .into_iter()
            .filter(|s| !cat.taken.contains(s) && cursor.is_none_or(|c| *s > c))
            .collect();
        arena
            .slots_in_use()
            .into_iter()
            .filter(|s| !listed.contains(s))
            .collect()
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        let mut inner = self.inner.lock();
        self.mature_all(&mut inner);
        if let Err(e) = inner.settle() {
            tracing::warn!("branch store: parked commits not applied: {e}");
        }
        let StoreInner { arena, cat, .. } = &mut *inner;
        if arena.as_ref().is_some_and(|a| a.is_free(slot)) {
            return true;
        }
        cat.as_mut().is_some_and(|cat| {
            !cat.taken.contains(&slot)
                && cat.free_cursor.is_none_or(|c| slot > c)
                && cat.catalog.free_has(slot).unwrap_or(false)
        })
    }

    pub(crate) fn set_failpoint(&self, failpoint: Option<BranchFailpoint>) {
        let mut inner = self.inner.lock();
        inner.failpoint = failpoint;
        inner.orphans.clear();
    }

    pub(crate) fn failpoint_orphans(&self) -> Vec<u32> {
        self.inner.lock().orphans.clone()
    }

    pub(crate) fn compact_now(&self) -> Result<()> {
        // A fuzzy checkpoint in flight finishes first: the sharp one may not overlap it (F-FZ). One
        // another thread starts after the join is waited for once more; then this refuses.
        let mut attempts = 0;
        let mut inner = loop {
            self.join_flights();
            let inner = self.inner.lock();
            if !inner.cat.as_ref().is_some_and(|c| c.flight) {
                break inner;
            }
            attempts += 1;
            if attempts == 3 {
                return Err(LimboError::Busy);
            }
        };
        // Nor while a commit is between its holds (r11-bigtxn port): refused and counted.
        if inner.publishing > 0 {
            inner.work.compactions_refused += 1;
            return Err(LimboError::Busy);
        }
        let fail = inner.failpoint == Some(BranchFailpoint::CompactAfterRenameBeforeLogReset);
        if fail {
            inner.failpoint = None;
        }
        if inner.failpoint == Some(BranchFailpoint::CompactArenaSyncFails) {
            inner.failpoint = None;
            if let Some(journal) = inner.journal.as_mut() {
                journal.fail_next_compact_arena_sync();
            }
        }
        self.compact(&mut inner, fail)
    }

    pub(crate) fn log_path(&self) -> Option<PathBuf> {
        self.inner
            .lock()
            .journal
            .as_ref()
            .map(|j| j.log_path().to_path_buf())
    }

    /// Checkpoint and settle counters (r11-restart-r2 instrument): `[checkpoints installed, fuzzy
    /// flights started, store-mutex hold ns inside checkpoints (sum, max), writer ns without the
    /// mutex, catalog statements under the mutex, settle batches, settle loads, most loads in one
    /// batch]`. Zeros for a store that is not a catalog store.
    pub(crate) fn checkpoint_counters(&self) -> [u64; 9] {
        self.inner
            .lock()
            .cat
            .as_ref()
            .map_or([0; 9], |c| c.ckpt.as_array())
    }

    /// Start a fuzzy checkpoint now, whatever the log's length (F-FZ; tests and the harness), as
    /// `maybe_compact` would: while C-R has parked Commits, this settles one bounded batch and
    /// starts nothing. `Ok(false)`: nothing started (parked Commits remain, one is in flight, or
    /// this is not a catalog store).
    pub(crate) fn checkpoint_fuzzy_now(&self) -> Result<bool> {
        let mut inner = self.inner.lock();
        if inner.cat.is_none() || inner.journal.is_none() || inner.arena.is_none() {
            return Ok(false);
        }
        Ok(self.start_flight(&mut inner))
    }

    /// Make a fuzzy checkpoint in flight wait at `stage` (`HOLD_BEFORE_COMMIT`,
    /// `HOLD_AFTER_COMMIT`) until this is called with another value (0 releases it).
    pub(crate) fn checkpoint_hold(&self, stage: u8) {
        self.flight_hold.store(stage, Ordering::Release);
    }

    /// Wait for every fuzzy checkpoint started so far to install.
    pub(crate) fn checkpoint_wait(&self) {
        self.join_flights();
    }

    /// The hook's value: a stage, with `HOLD_ARRIVED` once a flight waits there.
    pub(crate) fn checkpoint_held(&self) -> u8 {
        self.flight_hold.load(Ordering::Acquire)
    }
}

impl Drop for BranchStore {
    /// A clean close stamps the lease clock, so the time spent open is not lost with the process.
    /// (A crash loses the time since the last stamp, which extends leases and never shortens one.)
    /// A fuzzy checkpoint in flight is finished first: its thread holds the store's files.
    fn drop(&mut self) {
        self.flight_hold.store(0, Ordering::Release);
        self.join_flights();
        let mut inner = self.inner.lock();
        let now = inner.lease.now_ms();
        // With no lease outstanding the clock's value constrains nothing, so a close writes nothing.
        // Compared with what is DURABLE: a stamp only queued dies here with the journal (N3).
        if inner.leases_exist() && now > inner.lease.durable_ms {
            inner.lease.queued(now);
            if let Err(e) = self.log(&mut inner, Record::Clock { now_ms: now }) {
                tracing::debug!("branch lease clock not stamped at close: {e}");
            }
        }
    }
}

/// The `LogFlushFails` failpoint: fail this record flush as an I/O error would, which poisons the
/// journal exactly as `Journal::flush` does on a real failure.
fn injected_flush_failure(failpoint: &mut Option<BranchFailpoint>, journal: &mut Journal) -> Result<()> {
    if *failpoint == Some(BranchFailpoint::LogFlushFails) {
        *failpoint = None;
        journal.poison();
        return Err(LimboError::InternalError(
            "failpoint: the branch log flush failed".to_string(),
        ));
    }
    Ok(())
}

/// A group flight failed (amendment 4): the journal is fail-stopped, and nothing an operation
/// buffered after the last durable flush will become durable in this process.
fn group_poisoned() -> LimboError {
    LimboError::InternalError(
        "branch store is fail-stopped after a failed group flush; reopen the database to recover \
         it from disk"
            .to_string(),
    )
}

fn reaped(id: BranchId) -> LimboError {
    LimboError::InvalidArgument(format!("branch {} has been reaped", id.0))
}

impl StoreInner {
    fn fresh(files: Option<BranchFiles>, sync: bool, default_lease: Option<Duration>) -> Self {
        Self::fresh_mode(files, sync, default_lease, false)
    }

    fn fresh_mode(
        files: Option<BranchFiles>,
        sync: bool,
        default_lease: Option<Duration>,
        catalog_mode: bool,
    ) -> Self {
        Self {
            arena: None,
            journal: None,
            files,
            sync,
            next_id: 1,
            trunk: TrunkState {
                lineage: Lineage::default(),
                written: HashMap::new(),
            },
            branches: HashMap::new(),
            failpoint: None,
            orphans: Vec::new(),
            lease: LeaseClock::new(),
            leases: BTreeSet::new(),
            default_lease,
            children: ChildIndex::default(),
            n_states: 0,
            catalog_mode,
            cat: None,
            work: BranchWork::default(),
            derived_inserts: 0,
            parked: HashMap::new(),
            named_at: HashMap::new(),
            replay_pos: 0,
            deferred_freed: Vec::new(),
            parked_records: 0,
            parked_applied: 0,
            pending_free: VecDeque::new(),
            hold: HoldAcc::default(),
            hold_max: HoldMax::default(),
            hold_hist: [0; HOLD_HIST_BUCKETS],
            publishing: 0,
            force_compaction: false,
            early_released: 0,
            expire_more: false,
        }
    }

    /// The records a fork writes — the fork, and the default lease if there is one, flushed
    /// together so a fork is never durable without the lease it was given — and that lease's
    /// `(deadline, now)`, which the caller applies after the flush.
    fn fork_records(
        &self,
        child: BranchId,
        parent: BranchId,
        now: u64,
    ) -> (Vec<Record>, Option<(u64, u64)>) {
        let mut records = vec![Record::Fork {
            child: child.0,
            parent: parent.0,
        }];
        let lease = self
            .default_lease
            .map(|ttl| (now.saturating_add(millis(ttl)), now));
        if let Some((deadline_ms, now_ms)) = lease {
            records.push(Record::Lease {
                branch: child.0,
                deadline_ms,
                now_ms,
            });
        }
        (records, lease)
    }

    /// Apply the lease `fork_records` logged for a new branch, if any. (Its clock reading was
    /// queued before the flush that carried it.)
    fn apply_fork_lease(&mut self, id: BranchId, lease: Option<(u64, u64)>) {
        if let Some((deadline, _)) = lease {
            self.apply_lease(id, deadline);
        }
    }

    fn apply_lease(&mut self, id: BranchId, deadline: u64) {
        let Some(st) = self.branches.get_mut(&id) else {
            return;
        };
        if let Some(old) = st.lease {
            self.leases.remove(&(old, id));
        }
        let deadline = st.lease.unwrap_or(0).max(deadline);
        st.lease = Some(deadline);
        self.leases.insert((deadline, id));
        self.mark_dirty(id, DIRTY_ROW);
    }

    fn poisoned(&self) -> bool {
        self.journal.as_ref().is_some_and(|j| j.is_poisoned())
    }

    /// A lease is outstanding: on a resident branch, or on a catalog row not loaded. (r11-restart-r2:
    /// a reopened catalog store holds no resident branch until one is touched, and with `leases`
    /// alone its trunk commits and its close never stamped the lease clock, so a crash lost all the
    /// time since open — the safe direction, leases only lengthen, but the clock stood still.)
    fn leases_exist(&self) -> bool {
        !self.leases.is_empty() || self.cat.as_ref().is_some_and(|c| c.lease_floor.is_some())
    }

    /// The generation the last compaction COMMITTED, the commit point `BranchStore::compact` lands
    /// the group by (merge 1b(ii)): the journal's in snapshot mode (the snapshot's rename, then the
    /// log reset); the catalog's in catalog mode, which the log's own generation follows only once
    /// the log is rewritten to the checkpoint's suffix (r11-restart-r2), and a rewrite that fails
    /// before its rename leaves it behind.
    fn committed_generation(&self) -> u64 {
        match self.cat.as_ref() {
            Some(cat) => cat.generation,
            None => self.journal.as_ref().map_or(0, Journal::generation),
        }
    }

    /// Fail-stop after a catalog read failed in the middle of an operation: the in-memory state
    /// may be half-changed, and only a reopen (which recovers from the files) is safe.
    fn fatal(&mut self, e: LimboError) -> LimboError {
        if let Some(journal) = self.journal.as_mut() {
            journal.poison();
        }
        e
    }

    /// The catalog, in a catalog store that has files.
    fn catalog(&mut self) -> Option<&mut Catalog> {
        self.cat.as_mut().map(|c| &mut c.catalog)
    }

    /// Make `id`'s state resident. An eager store holds every state; a catalog store reads one
    /// from the catalog the first time something touches it (on-demand recovery), together with
    /// every ancestor not yet resident, parents first, so that each one's page map can be derived
    /// from its parent's (F4). `Ok(false)`: no such branch. The trunk always exists.
    fn ensure(&mut self, id: BranchId) -> Result<bool> {
        if id.is_trunk() || self.branches.contains_key(&id) {
            return Ok(true);
        }
        if self.cat.is_none() {
            return Ok(false);
        }
        let mut chain: Vec<CatBranch> = Vec::new();
        let mut next = id;
        while !next.is_trunk() && !self.branches.contains_key(&next) {
            let cat = self.cat.as_mut().expect("checked above");
            let loaded = if cat.removed.contains(&next) {
                None
            } else {
                cat.catalog.load_branch(next.0)?
            };
            let Some(b) = loaded else {
                if chain.is_empty() {
                    return Ok(false);
                }
                return Err(LimboError::Corrupt(format!(
                    "branch catalog: branch {} names a missing parent {}",
                    chain.last().expect("not empty").id,
                    next.0
                )));
            };
            cat.branch_loads += 1;
            next = BranchId(b.parent);
            chain.push(b);
        }
        while let Some(b) = chain.pop() {
            let loaded = BranchId(b.id);
            self.insert_loaded(b);
            self.apply_parked(loaded)?;
        }
        Ok(true)
    }

    /// C-R: apply `id`'s parked Commits, in log order, now that it is resident. A slot one of them
    /// frees that a later record of the tail names was reused already, and stays in use.
    fn apply_parked(&mut self, id: BranchId) -> Result<()> {
        let Some(list) = self.parked.remove(&id) else {
            return Ok(());
        };
        for (pos, pages) in list {
            let mut freed = Vec::new();
            self.apply_commit(id, &pages, &mut freed)?;
            freed.retain(|s| self.named_at.get(s).is_none_or(|&p| p <= pos));
            self.parked_applied += 1;
            self.free_deferred(freed);
        }
        if self.parked.is_empty() {
            self.named_at = HashMap::new();
        }
        Ok(())
    }

    /// Free slots a parked Commit freed: to the arena once it exists; during recovery's replay,
    /// to the list recovery marks free in record order.
    fn free_deferred(&mut self, freed: Vec<Slot>) {
        if self.arena.is_some() {
            self.release_slots(freed);
        } else {
            self.deferred_freed.extend(freed);
        }
    }

    /// C-R: make every parked branch resident (applying its parked Commits). Before a checkpoint,
    /// whose catalog must hold the state the log describes, and before the slot instruments.
    fn settle(&mut self) -> Result<()> {
        let ids: Vec<BranchId> = self.parked.keys().copied().collect();
        for id in ids {
            if !self.ensure(id)? {
                return Err(LimboError::Corrupt(format!(
                    "branch log replay: branch {} named by a Commit is not in the catalog",
                    id.0
                )));
            }
        }
        Ok(())
    }

    /// Install a branch read from the catalog, whose parent is resident: its F1 per-page maps and F2
    /// indexes from its own retained versions, and its F4 page map from the parent's version of
    /// each of the parent's pages at the branch's fork epoch — exactly what `derive_page_maps` does
    /// for one child after a snapshot load.
    fn insert_loaded(&mut self, b: CatBranch) {
        let id = BranchId(b.id);
        let parent = BranchId(b.parent);
        let mut lineage = Lineage {
            epoch: b.epoch,
            n_children: b.n_children,
            ..Lineage::default()
        };
        let mut retained = b.retained;
        // F1's per-page map takes a page's versions in `born` order.
        retained.sort_unstable_by_key(|&(page, born, ..)| (page, born));
        for (page, born, died, slot, crc) in retained {
            lineage.retain(page, Retained { born, died, slot, crc });
        }
        let current = b
            .current
            .into_iter()
            .map(|(page, slot, born, crc)| (page, Owned { slot, born, crc }))
            .collect();
        let (inherited, trunk_at) = if parent.is_trunk() {
            (PageMap::default(), b.fork_epoch)
        } else {
            let p = &self.branches[&parent];
            let mut map = p.inherited.clone();
            let mut pages: BTreeSet<u32> = p.current.keys().copied().collect();
            pages.extend(p.lineage.retained.keys().copied());
            for page in pages {
                if let Some(found) = p.version_at(page, b.fork_epoch) {
                    map.insert(page, found);
                    self.derived_inserts += 1;
                }
            }
            (map, p.trunk_at)
        };
        let lease = if b.released { None } else { b.lease };
        if let Some(deadline) = lease {
            self.leases.insert((deadline, id));
        }
        self.branches.insert(
            id,
            BranchState {
                parent,
                fork_epoch: b.fork_epoch,
                lineage,
                current,
                pending: BTreeMap::new(),
                schema: None,
                handle: if b.released {
                    Handle::Released
                } else {
                    Handle::Detached
                },
                open: false,
                writer: false,
                lease,
                trunk_at,
                inherited,
                view: None,
            },
        );
    }

    /// Catalog stores: make the trunk's `written` epoch of `page` at least the `died` of the page's
    /// last catalog version, as an eager recovery rebuilds it (the largest `died`), by ONE probe per
    /// page per process. The page's other versions are not read (C-P).
    fn trunk_written_known(&mut self, page: u32) -> Result<()> {
        let Some(cat) = self.cat.as_mut() else {
            return Ok(());
        };
        if !cat.trunk_known.insert(page) {
            return Ok(());
        }
        cat.trunk_probes += 1;
        if let Some((born, died, slot, crc)) = cat.catalog.trunk_pred(page, u64::MAX)? {
            cat.trunk_rows += 1;
            // A version reaped since the checkpoint still dates the page's last write.
            if !cat.trunk_gone.contains(&(page, born)) {
                cat.trunk_cache
                    .entry(page)
                    .or_default()
                    .insert(born, Retained { born, died, slot, crc });
            }
            let written = self.trunk.written.entry(page).or_insert(0);
            *written = (*written).max(died);
        }
        Ok(())
    }

    /// The trunk's version of `page` a child forked at `at` sees, if one was retained. In memory:
    /// every version (eager), or those retained since the last checkpoint (catalog), which are
    /// newer than every catalog version of the page, so a predecessor there is the answer. Else a
    /// cached catalog version holding `at`, else one probe of the catalog (C-P).
    fn trunk_version_at(
        &mut self,
        page: u32,
        at: u64,
        examined: &mut u64,
    ) -> Result<Option<(Slot, u32)>> {
        if let Some((_, v)) = self
            .trunk
            .lineage
            .retained
            .get(&page)
            .and_then(|vs| vs.range(..=at).next_back())
        {
            *examined += 1;
            return Ok((at < v.died).then_some((v.slot, v.crc)));
        }
        let Some(cat) = self.cat.as_mut() else {
            return Ok(None);
        };
        if let Some((_, v)) = cat
            .trunk_cache
            .get(&page)
            .and_then(|vs| vs.range(..=at).next_back())
        {
            if at < v.died {
                *examined += 1;
                return Ok(Some((v.slot, v.crc)));
            }
        }
        cat.trunk_probes += 1;
        let Some((born, died, slot, crc)) = cat.catalog.trunk_pred(page, at)? else {
            return Ok(None);
        };
        cat.trunk_rows += 1;
        *examined += 1;
        // A reaped version held no live child, so it cannot be the one a live child reads.
        if at >= died || cat.trunk_gone.contains(&(page, born)) {
            return Ok(None);
        }
        cat.trunk_cache
            .entry(page)
            .or_default()
            .insert(born, Retained { born, died, slot, crc });
        Ok(Some((slot, crc)))
    }

    /// Catalog stores: F2's garbage query over the trunk's CATALOG versions, for the child forked
    /// at `f` whose nearest live siblings were `lo` and `hi`: the versions with `born` in `(lo, f]`
    /// and `died` in `(f, hi]`, read in place. With one neighbour missing every entry of the one
    /// range is garbage (or reaped already), so that range alone is read; with both, the two ranges
    /// are read in doubling batches until one ends, and that one is filtered — so the rows read are
    /// at most 4x the smaller range, plus 64 (C-P). Each garbage version is marked reaped (deleted
    /// by the next checkpoint) and its slot goes to `freed`.
    fn trunk_catalog_garbage(
        &mut self,
        f: u64,
        lo: Option<u64>,
        hi: Option<u64>,
        freed: &mut Vec<Slot>,
    ) -> Result<()> {
        let Some(cat) = self.cat.as_mut() else {
            return Ok(());
        };
        let only_f = |born: u64, died: u64| {
            lo.is_none_or(|lo| born > lo) && born <= f && f < died && hi.is_none_or(|hi| died <= hi)
        };
        let mut candidates = match (lo, hi) {
            (None, _) => {
                cat.trunk_probes += 1;
                cat.catalog.trunk_died_range(f, hi, u64::MAX >> 1)?
            }
            (Some(_), None) => {
                cat.trunk_probes += 1;
                cat.catalog.trunk_born_range(lo, f, u64::MAX >> 1)?
            }
            (Some(_), Some(_)) => {
                let mut batch = 16u64;
                loop {
                    cat.trunk_probes += 2;
                    let by_born = cat.catalog.trunk_born_range(lo, f, batch)?;
                    let by_died = cat.catalog.trunk_died_range(f, hi, batch)?;
                    let fetched = (by_born.len() + by_died.len()) as u64;
                    cat.trunk_rows += fetched;
                    self.work.gc_range_entries += fetched;
                    if (by_born.len() as u64) < batch {
                        break by_born;
                    }
                    if (by_died.len() as u64) < batch {
                        break by_died;
                    }
                    batch *= 2;
                }
            }
        };
        if lo.is_none() || hi.is_none() {
            cat.trunk_rows += candidates.len() as u64;
            self.work.gc_range_entries += candidates.len() as u64;
        }
        candidates.retain(|&(page, born, died, _, _)| {
            only_f(born, died) && !cat.trunk_gone.contains(&(page, born))
        });
        for (page, born, _, slot, _) in candidates {
            cat.trunk_gone.insert((page, born));
            if let Some(vs) = cat.trunk_cache.get_mut(&page) {
                vs.remove(&born);
            }
            self.work.gc_examined += 1;
            freed.push(slot);
        }
        Ok(())
    }

    /// Part of `id`'s catalog state (`DIRTY_*`) is stale: rewrite it at the next checkpoint.
    fn mark_dirty(&mut self, id: BranchId, what: u8) {
        if let Some(cat) = self.cat.as_mut() {
            if !id.is_trunk() {
                *cat.dirty.entry(id).or_insert(0) |= what;
            }
        }
    }

    /// Top up the arena's in-memory free list from the catalog's free table before an allocation.
    fn refill_free(&mut self) -> Result<()> {
        let (Some(cat), Some(arena)) = (self.cat.as_mut(), self.arena.as_mut()) else {
            return Ok(());
        };
        // Not while a checkpoint is in flight (F-FZ): the catalog connection reads the snapshot the
        // capture pinned, whose free table the checkpoint is rewriting; allocations take the high
        // water mark meanwhile, and the install reconciles the list with what it committed.
        while arena.free_count() == 0 && !cat.free_exhausted && !cat.flight {
            let batch = cat.catalog.free_batch(cat.free_cursor, 256)?;
            match batch.last() {
                None => cat.free_exhausted = true,
                Some(&last) => cat.free_cursor = Some(last),
            }
            for slot in batch {
                if !cat.taken.contains(&slot) {
                    arena.add_free(slot);
                }
            }
        }
        Ok(())
    }

    fn alloc_slot(&mut self) -> Result<Slot> {
        self.refill_free()?;
        Ok(self
            .arena
            .as_mut()
            .expect("an allocation happens after the first fork")
            .alloc())
    }

    /// Ancestors between `id` and the trunk.
    fn depth(&mut self, mut id: BranchId) -> Result<usize> {
        let mut depth = 0;
        while self.ensure(id)? && !id.is_trunk() {
            depth += 1;
            id = self.branches[&id].parent;
        }
        Ok(depth)
    }

    /// Create the arena (and, for a durable store, its files) at the first fork, when the page size
    /// is known.
    fn ensure_backing(&mut self, page_size: usize) -> Result<()> {
        if let Some(current) = self.arena.as_ref().map(Arena::page_size) {
            if current == page_size {
                return Ok(());
            }
            // The database's page size changed. A store that holds nothing follows it; one that
            // holds a branch or a retained trunk version has pages of the old size and cannot.
            // (VACUUM and journal-mode changes are refused while a branch exists, so only the
            // empty case is reachable.)
            let catalog_retains = match self.catalog() {
                Some(cat) => cat.any_retained()?,
                None => false,
            };
            if self.n_states > 0 || !self.trunk.lineage.retained.is_empty() || catalog_retains {
                return Err(LimboError::InternalError(format!(
                    "branch arena holds {current}-byte pages but the database now uses {page_size}"
                )));
            }
            return self.restart_empty(page_size);
        }
        match &self.files {
            None => self.arena = Some(Arena::new(page_size)),
            Some(files) => {
                // Files that exist here held no recoverable state when this store opened
                // (`Journal::recover` said so): start them over. The journal first: it takes the
                // log's lock and refuses files another store has written since (review N1), and
                // the arena must not be truncated before that refusal. Once its lock is taken it is
                // KEPT, whatever fails after — its own start (poisoned, review 3 F3) or the arena's
                // open — so a retry never takes this store's own header for another store's state.
                if self.journal.is_none() {
                    let fail = self.failpoint == Some(BranchFailpoint::CreateFailsAfterHeader);
                    if fail {
                        self.failpoint = None;
                    }
                    let fail_lock = self.failpoint == Some(BranchFailpoint::CreateLockFails);
                    if fail_lock {
                        self.failpoint = None;
                    }
                    let mut journal =
                        Journal::open_fresh_with(files, page_size, self.sync, fail_lock)?;
                    let started = journal.start(fail);
                    self.journal = Some(journal);
                    started?;
                }
                let journal = self.journal.as_mut().expect("kept above");
                // A failed start, or a fork(2) child: fail-stop, before the arena is touched.
                journal.check_live()?;
                // Kept from an attempt whose arena failed to open, the journal holds only a header
                // written for THAT attempt's page size (review 3 F4).
                if journal.page_size() != page_size {
                    journal.restart(page_size)?;
                }
                // Catalog mode: the catalog is created now, at generation 0, after the log's lock
                // refused any other store and before the arena is truncated.
                if self.catalog_mode && self.cat.is_none() {
                    let mut catalog = Catalog::open(&files.cat, self.sync)?;
                    if catalog.meta()?.is_some() {
                        return Err(LimboError::LockingError(format!(
                            "branch catalog {} gained state after this branch store opened: \
                             another store instance wrote it",
                            files.cat.display()
                        )));
                    }
                    catalog.begin()?;
                    let meta = Meta {
                        generation: 0,
                        page_size: page_size as u32,
                        next_id: self.next_id,
                        ..Meta::default()
                    };
                    if let Err(e) = catalog.put_meta(&meta).and_then(|()| catalog.commit()) {
                        catalog.rollback();
                        return Err(e);
                    }
                    self.cat = Some(CatState::new(catalog, self.sync, 0)?);
                }
                let arena = Arena::open_file(&files.arena, page_size, true, &[])?;
                // The journal's create synced the directory before the arena file existed.
                if self.sync {
                    super::journal::fsync_dir_of(&files.arena)?;
                }
                self.arena = Some(arena);
            }
        }
        Ok(())
    }

    /// Start an EMPTY store over at a new page size. Durable: an empty snapshot at the new page
    /// size replaces the log (the snapshot rename is the commit point), and only then is the arena
    /// truncated. Nothing references a slot before or after, so a crash anywhere in between
    /// recovers an empty store; an arena file left at the old size only yields free slots, since
    /// `Arena::open_file` counts whole slots of the recovered page size.
    fn restart_empty(&mut self, page_size: usize) -> Result<()> {
        let Some(files) = self.files.clone() else {
            self.arena = Some(Arena::new(page_size));
            return Ok(());
        };
        if self.cat.is_some() {
            // The catalog's meta row and the log header take the new page size in one checkpoint;
            // the arena is truncated only after it committed.
            let old = self.journal.as_ref().map(Journal::page_size);
            if let Some(journal) = self.journal.as_mut() {
                journal.set_page_size(page_size);
            }
            if let Err(e) = self.checkpoint_catalog(false, true) {
                if let (Some(journal), Some(old)) = (self.journal.as_mut(), old) {
                    journal.set_page_size(old);
                }
                return Err(e);
            }
            match Arena::open_file(&files.arena, page_size, true, &[]) {
                Ok(arena) => self.arena = Some(arena),
                Err(e) => {
                    if let Some(journal) = self.journal.as_mut() {
                        journal.poison();
                    }
                    return Err(e);
                }
            }
            return Ok(());
        }
        let snapshot = self.snapshot();
        let (Some(journal), Some(arena)) = (self.journal.as_mut(), self.arena.as_mut()) else {
            return Err(LimboError::InternalError(
                "a durable branch arena has no journal".to_string(),
            ));
        };
        journal.check_live()?;
        let old = journal.page_size();
        journal.set_page_size(page_size);
        if let Err(e) = journal.compact(&snapshot, arena, false) {
            journal.set_page_size(old);
            return Err(e);
        }
        self.lease.queued(snapshot.lease_now_ms);
        self.lease.flushed();
        match Arena::open_file(&files.arena, page_size, true, &[]) {
            Ok(arena) => self.arena = Some(arena),
            Err(e) => {
                // The snapshot and the log header already say the new page size; the arena in
                // memory still has the old one. Fail-stop rather than run on with the two
                // disagreeing (review 4 C5): recovery starts from the files, where they agree.
                if let Some(journal) = self.journal.as_mut() {
                    journal.poison();
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Catalog mode's compaction, an incremental checkpoint: every branch and trunk page changed
    /// since the last checkpoint, the free-space changes and the meta row go to the catalog in ONE
    /// transaction; then the log keeps only what follows the capture. The catalog commit is the
    /// commit point (a crash after it replays only what follows the capture's `Record::Checkpoint`), as
    /// the snapshot's rename is in snapshot mode. The work is proportional to what changed since
    /// the last checkpoint, which the log's size bounds, not to the live state.
    ///
    /// This is the SHARP form (`compact_now`, `restart_empty`, and every test that checkpoints on
    /// purpose): capture, write and install back to back under the store mutex. `maybe_compact`
    /// runs the same three steps as a fuzzy checkpoint instead, with the write on its own thread
    /// and no store mutex held across it (F-FZ).
    ///
    /// `restart` (R3, PREREG A19): `restart_empty` is about to truncate the arena to a new page size, so the
    /// checkpoint records the arena the store will have, not the one it has: no free row at all (every old one is
    /// deleted), high-water mark 0, nothing in use. The truncation still follows the commit.
    fn checkpoint_catalog(&mut self, fail_after_commit: bool, restart: bool) -> Result<()> {
        // The catalog must hold the state the log describes: parked Commits first (C-R).
        self.settle()?;
        if self.journal.is_none() || self.arena.is_none() {
            return Ok(());
        }
        let Some(cat) = self.cat.as_ref() else {
            return Ok(());
        };
        if cat.flight {
            // A fuzzy checkpoint is between its capture and its install; this one would overlap it.
            return Err(LimboError::Busy);
        }
        let t = Instant::now();
        let writer = cat.writer.clone();
        let q0 = cat.catalog.counters.queries;
        let cap = self.checkpoint_capture(fail_after_commit, restart, true)?;
        let mut w = writer.lock();
        let w0 = w.counters.queries;
        // The sharp form synced the arena in its capture, so its writer syncs nothing and takes no
        // flight slot (the compaction that runs this holds it; `restart_empty`'s caller quiesced).
        let mut in_doubt = false;
        let written = checkpoint_write(&mut w, &cap, None, None, &mut in_doubt);
        let wrote = w.counters.queries - w0;
        let installed = self.checkpoint_install(cap, written, in_doubt);
        if installed.is_ok() {
            truncate_catalog_wal(&mut w);
        }
        drop(w);
        if let Some(cat) = self.cat.as_mut() {
            let q1 = cat.catalog.counters.queries;
            cat.ckpt.stmts_locked += wrote + q1.saturating_sub(q0);
            cat.ckpt.hold(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
        installed
    }

    /// F-FZ phase 1, under the store mutex: capture what the checkpoint writes, as of the log's
    /// current end, and pin the catalog connection's read snapshot there. Nothing parked may be
    /// pending (the catalog must hold what the captured log describes) and no other checkpoint may
    /// be in flight. The dirty map is swapped out (a branch changed after this is dirty again);
    /// every other in-memory set stays as it is until the install, so between now and then the
    /// store reads exactly as it does between checkpoints.
    ///
    /// `sharp` (merge 1b(ii)): the arena is synced here, under the mutex, and counted in
    /// `compact_locked_bytes`, as the durable port's checkpoint did (7a4f2db88); the fuzzy form
    /// hands its writer a handle instead. The deferred frees are taken out here (see
    /// `Captured::covered`).
    fn checkpoint_capture(
        &mut self,
        fail_after_commit: bool,
        restart: bool,
        sharp: bool,
    ) -> Result<Box<Captured>> {
        // No capture while a commit is between its holds (r11-bigtxn port): its record is buffered
        // ahead of the marker and half its pages are mapped, so the catalog would hold half a
        // commit that the log cut then drops. Every caller refuses first (`maybe_compact`,
        // `compact_now`, `start_flight`; `restart_empty` runs with no branch); this is the guard
        // itself.
        if self.publishing > 0 {
            self.work.compactions_refused += 1;
            return Err(LimboError::Busy);
        }
        let now = self.lease.now_ms();
        let (Some(journal), Some(arena), Some(cat)) =
            (self.journal.as_mut(), self.arena.as_mut(), self.cat.as_mut())
        else {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint needs its log, arena and catalog".to_string(),
            ));
        };
        journal.check_live()?;
        if !self.parked.is_empty() || cat.flight {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint started with parked commits or another in flight"
                    .to_string(),
            ));
        }
        // Every slot the catalog is about to name must be durable first (the sharp form; the fuzzy
        // one syncs in its writer). A failed fsync may have dropped the pages (review 2 F2):
        // fail-stop.
        if sharp && self.sync {
            match arena.sync() {
                Ok(bytes) => self.work.compact_locked_bytes += bytes,
                Err(e) => {
                    journal.poison();
                    return Err(e);
                }
            }
        }
        let generation = cat.next_generation;
        cat.next_generation += 1;
        let dirty = std::mem::take(&mut cat.dirty);
        let rows: Vec<(CatBranch, u8)> = dirty
            .iter()
            .filter_map(|(id, &what)| self.branches.get(id).map(|st| (id, st, what)))
            .map(|(&id, st, what)| (CatBranch {
                id: id.0,
                parent: st.parent.0,
                fork_epoch: st.fork_epoch,
                epoch: st.lineage.epoch,
                released: st.handle == Handle::Released,
                lease: if st.handle == Handle::Released { None } else { st.lease },
                n_children: st.lineage.n_children,
                current: st
                    .current
                    .iter()
                    .map(|(&page, o)| (page, o.slot, o.born, o.crc))
                    .collect(),
                retained: st.lineage.retained_list(),
            }, what))
            .collect();
        // The trunk: the versions retained since the last checkpoint (all in memory, none in the
        // catalog) are inserted, and the catalog versions reaped since are deleted, one row each.
        let trunk_new = self.trunk.lineage.retained_list();
        let trunk_gone: Vec<(u32, u64)> = cat.trunk_gone.iter().copied().collect();
        // Slots reserved by open write transactions are named by no durable state: the catalog
        // lists them free (a crash frees them), and this process keeps them as taken.
        let reserved: Vec<Slot> = if restart {
            Vec::new()
        } else {
            self.branches
                .values()
                .flat_map(|st| st.pending.values().copied())
                .collect()
        };
        // Slots freed by early-released operations (r11-churn amendment 4) wait in `pending_free`
        // until a flush covers their records (rule 2). This checkpoint covers every operation
        // applied before its capture (no commit is between its holds, above), the ones still only
        // buffered included, so it lists their slots free in the same transaction that stops
        // naming them, and this process takes them back only once it has committed. A restart
        // records an arena with nothing free in it (R3): its deferred frees name slots of the arena
        // it replaces, and `fork_trunk` drops them.
        let covered_len: usize = if restart {
            0
        } else {
            self.pending_free.iter().map(|(_, freed)| freed.len()).sum()
        };
        // ARIES's begin-checkpoint record: the capture covers the log up to and including it.
        if let Err(e) = journal.buffer(&Record::Checkpoint { generation }) {
            cat.dirty = dirty;
            return Err(e);
        }
        let log_from = journal.mark();
        let lsn = journal.lsn();
        let meta = Meta {
            generation,
            page_size: journal.page_size() as u32,
            next_id: self.next_id,
            trunk_epoch: self.trunk.lineage.epoch,
            trunk_children: self.trunk.lineage.n_children,
            lease_now_ms: now,
            // R3: a restart records the truncated arena the store is about to have.
            arena_hw: if restart { 0 } else { arena.high_water() },
            in_use: if restart {
                0
            } else {
                (arena.in_use() - reserved.len() - covered_len) as u64
            },
            states: self.n_states,
        };
        if super::arena::trace_slots() {
            let named: Vec<(u64, Vec<Slot>)> = rows
                .iter()
                .map(|(b, _)| {
                    let mut v: Vec<Slot> = b.current.iter().map(|c| c.1).collect();
                    v.extend(b.retained.iter().map(|r| r.3));
                    (b.id, v)
                })
                .collect();
            eprintln!(
                "R11SLOT checkpoint gen={generation} log_from={log_from} rows={named:?} removed={:?} trunk_new={:?} trunk_gone={:?} cursor={:?} taken={:?} free_mem={:?} reserved={reserved:?} hw={} in_use={}",
                cat.removed,
                trunk_new,
                trunk_gone,
                cat.free_cursor,
                cat.taken,
                arena.free_list(),
                arena.high_water(),
                arena.in_use()
            );
        }
        let arena_file = if self.sync && !sharp {
            match arena.sync_handle() {
                Ok(f) => f,
                Err(e) => {
                    cat.dirty = dirty;
                    return Err(e);
                }
            }
        } else {
            None
        };
        if let Err(e) = cat.catalog.begin_read_snapshot() {
            cat.dirty = dirty;
            return Err(e);
        }
        cat.flight = true;
        // Taken last, once nothing here can fail, so no failure above has to put them back.
        let covered = if restart {
            VecDeque::new()
        } else {
            std::mem::take(&mut self.pending_free)
        };
        Ok(Box::new(Captured {
            generation,
            log_from,
            lsn,
            rows,
            dirty,
            removed: cat.removed.iter().copied().collect(),
            trunk_new,
            trunk_gone,
            // R3: a restart deletes every free row (a cursor past every slot) and writes none.
            free_cursor: if restart { Some(Slot::MAX) } else { cat.free_cursor },
            taken: if restart { Vec::new() } else { cat.taken.iter().copied().collect() },
            free_list: if restart { Vec::new() } else { arena.free_list() },
            reserved,
            covered,
            meta,
            child_keys: self.children.map.keys().copied().collect(),
            child_removed: self.children.removed.keys().copied().collect(),
            arena: arena_file,
            lease_now: now,
            fail_after_commit,
        }))
    }

    /// F-FZ phase 3, under the store mutex: end the pinned snapshot and, if the writer committed,
    /// cut the log to what follows the capture and take out of memory exactly what the catalog now
    /// holds — nothing that changed since the capture. If it did not commit, what the capture swapped
    /// out is dirty again and nothing else has changed. `in_doubt` (see `checkpoint_write`): the
    /// write failed where its outcome is unknown, and the journal is fail-stopped.
    fn checkpoint_install(
        &mut self,
        cap: Box<Captured>,
        written: Result<()>,
        in_doubt: bool,
    ) -> Result<()> {
        let Some(cat) = self.cat.as_mut() else {
            return written;
        };
        let q0 = cat.catalog.counters.queries;
        cat.catalog.end_read_snapshot();
        cat.flight = false;
        if let Err(e) = written {
            // A failed arena fsync may have dropped the pages, and a failed COMMIT may be on disk
            // (review 2 F2): fail-stop. Any other failure rolled back and changed nothing.
            if in_doubt {
                if let Some(journal) = self.journal.as_mut() {
                    journal.poison();
                }
            }
            for (id, what) in cap.dirty {
                *cat.dirty.entry(id).or_insert(0) |= what;
            }
            // The deferred frees the capture took wait again, ahead of any deferred since (they
            // are older), for a flush or the next checkpoint to cover them.
            for entry in cap.covered.into_iter().rev() {
                self.pending_free.push_front(entry);
            }
            return Err(e);
        }
        cat.generation = cap.generation;
        cat.ckpt.count += 1;
        // From here the catalog is the truth up to the capture's log position.
        let Some(journal) = self.journal.as_mut() else {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint lost its log".to_string(),
            ));
        };
        if cap.fail_after_commit {
            journal.poison();
            // Nothing is installed: the deferred frees stay deferred, as they did when the durable
            // port's checkpoint (7a4f2db88) stopped here, before it took them.
            for entry in cap.covered.into_iter().rev() {
                self.pending_free.push_front(entry);
            }
            return Err(LimboError::InternalError(
                "failpoint: branch checkpoint stopped after the catalog commit".to_string(),
            ));
        }
        // A failure before its rename leaves the old log in use, whose checkpoint marker says
        // where recovery cuts it: correct, only longer. The install below must happen either way.
        let rewritten = journal.rewrite_from(cap.log_from, cap.generation);
        for id in &cap.removed {
            cat.removed.remove(id);
        }
        for key in &cap.trunk_gone {
            cat.trunk_gone.remove(key);
        }
        // The trunk's captured versions are catalog versions now: they move to the read cache, so
        // that what stays in the lineage is again only what the catalog does not hold. One reaped
        // since the capture is in the catalog anyway, so the next checkpoint deletes it.
        for &(page, born, died, slot, crc) in &cap.trunk_new {
            if self.trunk.lineage.take_version(page, born).is_some() {
                cat.trunk_cache
                    .entry(page)
                    .or_default()
                    .insert(born, Retained { born, died, slot, crc });
            } else {
                cat.trunk_gone.insert((page, born));
            }
        }
        // Children forked before the capture have rows now; links of children removed before it
        // pointed at rows that are deleted now.
        for key in &cap.child_keys {
            self.children.map.remove(key);
        }
        for key in &cap.child_removed {
            self.children.removed.remove(key);
        }
        // The free list: every slot the checkpoint listed free in the catalog leaves the in-memory
        // list if it is still there; one that is not is in use (or reserved) now, and is taken.
        // Slots freed since the capture stay on the list: the catalog does not hold them. (The
        // free table is not read while a checkpoint is in flight, so nothing on the list came from
        // it after the capture.)
        if let Some(arena) = self.arena.as_mut() {
            // The covered deferred frees are catalog free rows now, like every other free slot: no
            // longer in use here (the meta row's count already left them out), and taken off the
            // in-memory list with the rest just below. They count as this hold's pages.
            let mut covered = 0u64;
            for (_, freed) in &cap.covered {
                for &slot in freed {
                    arena.release(slot);
                    covered += 1;
                }
            }
            self.hold.pages += covered;
            let listed: HashSet<Slot> = cap
                .free_list
                .iter()
                .chain(cap.reserved.iter())
                .chain(cap.covered.iter().flat_map(|(_, freed)| freed.iter()))
                .copied()
                .collect();
            cat.taken = arena.remove_free(&listed).into_iter().collect();
        }
        cat.free_cursor = None;
        cat.free_exhausted = false;
        cat.lease_floor = cat.catalog.lease_min()?;
        cat.ckpt.stmts_locked += cat.catalog.counters.queries - q0;
        self.lease.durable_at_least(cap.lease_now);
        rewritten
    }

    /// C-R's parked Commits, at most `max` branches per call with their unloaded ancestors:
    /// Graefe's background redo in bounded quanta, run where a checkpoint wants to start
    /// (r11-restart-r2). Returns the catalog loads it made.
    fn settle_batch(&mut self, max: usize) -> Result<u64> {
        let loads0 = self.cat.as_ref().map_or(0, |c| c.branch_loads);
        let ids: Vec<BranchId> = self.parked.keys().take(max).copied().collect();
        for id in ids {
            if !self.ensure(id)? {
                return Err(LimboError::Corrupt(format!(
                    "branch log replay: branch {} named by a Commit is not in the catalog",
                    id.0
                )));
            }
        }
        let loads = self.cat.as_ref().map_or(0, |c| c.branch_loads) - loads0;
        if let Some(cat) = self.cat.as_mut() {
            cat.ckpt.settle_batches += 1;
            cat.ckpt.settle_loads += loads;
            cat.ckpt.settle_max_loads = cat.ckpt.settle_max_loads.max(loads);
        }
        Ok(loads)
    }

    /// Everything buffered, as one flight (`Journal::take_flight`); `None` when volatile.
    fn take_flight(&mut self) -> Result<Option<Flight>> {
        let (Some(journal), Some(arena)) = (self.journal.as_mut(), self.arena.as_mut()) else {
            return Ok(None);
        };
        journal.take_flight(arena).map(Some)
    }

    /// Hold `freed` until the records that freed them — up to `lsn` — are durable (rule 2 under
    /// early release; see `Group`). A volatile store frees at once.
    fn defer_frees(&mut self, lsn: u64, freed: Vec<Slot>) {
        if self.journal.is_none() || lsn == 0 {
            self.release_slots(freed);
            return;
        }
        if !freed.is_empty() {
            self.pending_free.push_back((lsn, freed));
        }
    }

    /// Return to the arena deferred frees a flush has covered: at most `budget` slots, the front
    /// entry split if it is larger (r11-bigtxn port, amendment 8b).
    fn mature_frees(&mut self, durable: u64, budget: usize) {
        let mut budget = budget;
        while budget > 0 {
            let Some((lsn, front)) = self.pending_free.front_mut() else {
                break;
            };
            if *lsn > durable {
                break;
            }
            let take = budget.min(front.len());
            let batch: Vec<Slot> = front.drain(front.len() - take..).collect();
            if front.is_empty() {
                self.pending_free.pop_front();
            }
            budget -= take;
            self.release_slots(batch);
        }
    }

    fn release_slots(&mut self, freed: Vec<Slot>) {
        if freed.is_empty() {
            return;
        }
        self.hold.pages += freed.len() as u64;
        let arena = self.arena.as_mut().expect("slots were freed, so the arena exists");
        for slot in freed {
            arena.release(slot);
        }
    }

    fn apply_fork(
        &mut self,
        parent: BranchId,
        child: BranchId,
        schema: Option<Arc<Schema>>,
        handle: Handle,
    ) -> Result<()> {
        // An id at or past `next_id` was never allocated, so the catalog cannot hold it (C-R): only
        // an older id is looked up there.
        let exists = if child.0 >= self.next_id {
            self.branches.contains_key(&child)
        } else {
            self.ensure(child)?
        };
        if child.is_trunk() || exists {
            return Err(LimboError::Corrupt(format!(
                "branch {} forked twice",
                child.0
            )));
        }
        if !self.ensure(parent)? {
            return Err(gone(parent));
        }
        let (f, inherited, trunk_at) = if parent.is_trunk() {
            let lineage = &mut self.trunk.lineage;
            let f = lineage.epoch;
            lineage.epoch += 1;
            lineage.n_children += 1;
            (f, PageMap::default(), f)
        } else {
            let st = self.branches.get_mut(&parent).ok_or_else(|| gone(parent))?;
            let f = st.lineage.epoch;
            st.lineage.epoch += 1;
            st.lineage.n_children += 1;
            // A first fork builds the parent's view from all its pages in this hold (r11-bigtxn's
            // F-fork1 is not ported): counted as the hold's pages.
            let built = if st.view.is_none() {
                st.current.len() as u64
            } else {
                0
            };
            let inherited = st.view_now().clone();
            self.hold.pages += built;
            (f, inherited, st.trunk_at)
        };
        self.children.insert(parent, f, child);
        self.next_id = self.next_id.max(child.0 + 1);
        self.branches.insert(
            child,
            BranchState {
                parent,
                fork_epoch: f,
                lineage: Lineage::default(),
                current: BTreeMap::new(),
                pending: BTreeMap::new(),
                schema,
                handle,
                open: false,
                writer: false,
                lease: None,
                trunk_at,
                inherited,
                view: None,
            },
        );
        self.n_states += 1;
        self.mark_dirty(parent, DIRTY_ROW);
        self.mark_dirty(child, DIRTY_NEW);
        Ok(())
    }

    /// Move the branch's map to the committed slots. The version each replaces is retained if a
    /// live child forked while it was current, else freed.
    fn apply_commit(
        &mut self,
        id: BranchId,
        pages: &[(u32, Slot, u32)],
        freed: &mut Vec<Slot>,
    ) -> Result<()> {
        if !self.ensure(id)? {
            return Err(gone(id));
        }
        let StoreInner {
            branches,
            children,
            cat,
            ..
        } = self;
        let mut catalog = cat.as_mut().map(|c| &mut c.catalog);
        let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
        let epoch = st.lineage.epoch;
        let mut what = DIRTY_CUR;
        for &(page, slot, crc) in pages {
            let new = Owned {
                slot,
                born: epoch,
                crc,
            };
            if let Some(view) = st.view.as_mut() {
                view.insert(page, (slot, crc));
            }
            if let Some(old) = st.current.insert(page, new) {
                if children.any_in(catalog.as_deref_mut(), id, old.born, epoch)? {
                    what |= DIRTY_RET;
                    st.lineage.retain(
                        page,
                        Retained {
                            born: old.born,
                            died: epoch,
                            slot: old.slot,
                            crc: old.crc,
                        },
                    );
                } else {
                    freed.push(old.slot);
                }
            }
        }
        self.mark_dirty(id, what);
        Ok(())
    }

    fn apply_trunk_retain(&mut self, page: u32, v: Retained) -> Result<()> {
        // Replay only (and the snapshot load). Blind redo (C-R): the record's `died` is the page's
        // latest trunk write, and every catalog version of the page died at or before its `born`,
        // so `written` is known from the record, without reading the catalog.
        if let Some(cat) = self.cat.as_mut() {
            cat.trunk_known.insert(page);
        }
        self.trunk.lineage.retain(page, v);
        let written = self.trunk.written.entry(page).or_insert(0);
        *written = (*written).max(v.died);
        Ok(())
    }

    fn apply_release(&mut self, id: BranchId, freed: &mut Vec<Slot>) -> Result<()> {
        self.ensure(id)?;
        if let Some(st) = self.branches.get_mut(&id) {
            st.handle = Handle::Released;
            if let Some(deadline) = st.lease.take() {
                self.leases.remove(&(deadline, id));
            }
        }
        self.mark_dirty(id, DIRTY_ROW);
        self.collect(id, freed)
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist; a released `id` that still has live children is retired instead (see
    /// `BranchState::retire_current`). Every freed slot goes to `freed`.
    fn collect(&mut self, mut id: BranchId, freed: &mut Vec<Slot>) -> Result<()> {
        loop {
            if !self.ensure(id)? {
                return Ok(());
            }
            let Some(st) = self.branches.get_mut(&id) else {
                return Ok(());
            };
            // Exactly `Released`: a `ReleasePending` branch's release is not durable and nothing of
            // it is ever freed here (review R1).
            if st.handle != Handle::Released || st.open {
                return Ok(());
            }
            if st.lineage.n_children > 0 {
                // F4: a released interior keeps only what a live child can still read.
                let StoreInner {
                    branches,
                    children,
                    cat,
                    ..
                } = self;
                let st = branches.get_mut(&id).expect("just looked it up");
                st.retire_current(id, children, cat.as_mut().map(|c| &mut c.catalog), freed)?;
                self.mark_dirty(id, DIRTY_CUR | DIRTY_RET);
                return Ok(());
            }
            let st = self.branches.remove(&id).expect("just looked it up");
            self.n_states -= 1;
            if let Some(cat) = self.cat.as_mut() {
                cat.dirty.remove(&id);
                cat.removed.insert(id);
            }
            freed.extend(st.current.values().map(|o| o.slot));
            freed.extend(st.pending.values().copied());
            st.lineage.release_all(freed);
            let (parent, f) = (st.parent, st.fork_epoch);
            // The parent resident (its parked Commits applied, C-R) while the child is still listed:
            // each of those Commits decides retain-or-free as it did when the child was alive.
            if !parent.is_trunk() && !self.ensure(parent)? {
                return Err(LimboError::Corrupt(format!(
                    "branch {} names a missing parent {}",
                    id.0, parent.0
                )));
            }
            let catalog_mode = self.cat.is_some();
            let catalog = self.cat.as_mut().map(|c| &mut c.catalog);
            let (listed, lo, hi) = self.children.remove(catalog, parent, f, catalog_mode)?;
            crate::turso_assert!(listed, "detached a child the parent does not list");
            if parent.is_trunk() {
                // In memory: every version (eager), or those retained since the last checkpoint
                // (catalog), whose garbage F2's walk finds as before. Catalog stores add the
                // catalog's garbage, read in place (C-P).
                self.trunk.lineage.child_gone(f, lo, hi, freed, &mut self.work);
                self.trunk_catalog_garbage(f, lo, hi, freed)?;
                return Ok(());
            }
            if !self.ensure(parent)? {
                return Err(LimboError::Corrupt(format!(
                    "branch {} names a missing parent {}",
                    id.0, parent.0
                )));
            }
            let parent_st = self
                .branches
                .get_mut(&parent)
                .expect("a live branch's parent is kept while the branch lives");
            parent_st.lineage.child_gone(f, lo, hi, freed, &mut self.work);
            self.mark_dirty(parent, DIRTY_ROW | DIRTY_RET);
            id = parent;
        }
    }

    /// Collect every released branch that nothing reads through any more.
    fn collect_released(&mut self, freed: &mut Vec<Slot>) -> Result<u64> {
        let mut released: Vec<BranchId> = self
            .branches
            .iter()
            .filter(|(_, st)| st.handle == Handle::Released)
            .map(|(&id, _)| id)
            .collect();
        if let Some(cat) = self.catalog() {
            released.extend(cat.released_ids()?.into_iter().map(BranchId));
            released.sort_unstable();
            released.dedup();
        }
        let n = released.len() as u64;
        for id in released {
            self.collect(id, freed)?;
        }
        Ok(n)
    }

    /// `levels` counts the nodes consulted — the branch (its own pages and its `inherited` map),
    /// then the trunk if neither holds the page — and `examined` the retained versions compared.
    fn resolve(
        &mut self,
        id: BranchId,
        page: u32,
        levels: &mut u64,
        examined: &mut u64,
    ) -> Result<Option<(Slot, u32)>> {
        *levels += 1;
        if !self.ensure(id)? {
            return Err(gone(id));
        }
        let st = self.branches.get(&id).ok_or_else(|| gone(id))?;
        // A branch sees all of its own versions; its ancestors' as of its fork, which `inherited`
        // froze then.
        if let Some(owned) = st.current.get(&page) {
            return Ok(Some((owned.slot, owned.crc)));
        }
        if let Some(found) = st.inherited.get(page) {
            return Ok(Some(found));
        }
        *levels += 1;
        let at = st.trunk_at;
        self.trunk_written_known(page)?;
        if let Some(found) = self.trunk_version_at(page, at, examined)? {
            return Ok(Some(found));
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

    /// Rebuild every branch's `inherited` map and `trunk_at` from the recovered lineages, parents
    /// before children: `load_snapshot` restores state without replaying the forks that built them.
    /// A child forked from branch `p` at `f` inherits `p`'s own `inherited` overlaid with every page
    /// `p` held at `f` — its current version if born by then, else the retained version holding `f`.
    fn derive_page_maps(&mut self) -> Result<()> {
        // The children of `p`, from the store-wide index (an eager store holds every child there).
        let children_of = |children: &ChildIndex, p: BranchId| -> Vec<(u64, BranchId)> {
            children
                .map
                .range((p.0, 0)..=(p.0, u64::MAX))
                .map(|(&(_, f), &id)| (f, id))
                .collect()
        };
        let mut todo: Vec<(BranchId, PageMap, u64)> = children_of(&self.children, BranchId::TRUNK)
            .into_iter()
            .map(|(f, id)| (id, PageMap::default(), f))
            .collect();
        let (mut reached, mut inserts) = (0, 0u64);
        while let Some((id, inherited, trunk_at)) = todo.pop() {
            reached += 1;
            let st = self
                .branches
                .get_mut(&id)
                .expect("a lineage lists only branches that exist");
            st.inherited = inherited;
            st.trunk_at = trunk_at;
            st.view = None;
            let st = &self.branches[&id];
            let mut pages: BTreeSet<u32> = st.current.keys().copied().collect();
            pages.extend(st.lineage.retained.keys().copied());
            for (f, child) in children_of(&self.children, id) {
                let mut map = st.inherited.clone();
                for &page in &pages {
                    if let Some(found) = st.version_at(page, f) {
                        map.insert(page, found);
                        inserts += 1;
                    }
                }
                todo.push((child, map, st.trunk_at));
            }
        }
        self.derived_inserts += inserts;
        // A branch no lineage lists would keep an empty map and read its ancestors' pages from the
        // trunk: refuse the open rather than serve it.
        if reached != self.branches.len() {
            return Err(LimboError::Corrupt(format!(
                "branch snapshot: {} of {} branches are listed by no lineage; their page maps \
                 cannot be derived",
                self.branches.len() - reached,
                self.branches.len()
            )));
        }
        Ok(())
    }

    /// Re-execute one logged operation during recovery.
    fn replay(&mut self, record: &Record, freed: &mut Vec<Slot>) -> Result<()> {
        let corrupt = |e: LimboError| {
            LimboError::Corrupt(format!("branch log replay of {record:?}: {e}"))
        };
        match record {
            Record::Fork { child, parent } => self
                .apply_fork(BranchId(*parent), BranchId(*child), None, Handle::Detached)
                .map_err(corrupt),
            Record::Commit { branch, pages } => {
                let id = BranchId(*branch);
                // C-R: in catalog recovery (the arena is not open yet), a Commit to a branch that is
                // not resident is parked, not replayed: the branch is not read until it is touched.
                let recovering = self.cat.is_some() && self.arena.is_none();
                let removed = self.cat.as_ref().is_some_and(|c| c.removed.contains(&id));
                if recovering && !removed && (!self.branches.contains_key(&id) || self.parked.contains_key(&id)) {
                    self.parked
                        .entry(id)
                        .or_default()
                        .push((self.replay_pos, pages.clone()));
                    self.parked_records += 1;
                    return Ok(());
                }
                self.apply_commit(id, pages, freed).map_err(corrupt)
            }
            Record::TrunkRetain {
                page,
                born,
                died,
                slot,
                crc,
            } => self
                .apply_trunk_retain(
                    *page,
                    Retained {
                        born: *born,
                        died: *died,
                        slot: *slot,
                        crc: *crc,
                    },
                )
                .map_err(corrupt),
            Record::Release { branch } => {
                let id = BranchId(*branch);
                if !self.ensure(id)? {
                    return Err(corrupt(gone(id)));
                }
                self.apply_release(id, freed).map_err(corrupt)
            }
            Record::Lease {
                branch,
                deadline_ms,
                now_ms,
            } => {
                let id = BranchId(*branch);
                if !self.ensure(id)? {
                    return Err(corrupt(gone(id)));
                }
                self.apply_lease(id, *deadline_ms);
                self.lease.recovered(*now_ms);
                Ok(())
            }
            Record::Clock { now_ms } => {
                self.lease.recovered(*now_ms);
                Ok(())
            }
            // A checkpoint that did not commit (or did, and this is the log recovery cut after
            // it): nothing to redo (F-FZ).
            Record::Checkpoint { .. } => Ok(()),
        }
    }

    /// Every slot the state names: what the arena must NOT treat as free after a reopen.
    fn referenced_slots(&self) -> Vec<Slot> {
        let mut slots: Vec<Slot> = self
            .trunk
            .lineage
            .retained
            .values()
            .flat_map(|vs| vs.values().map(|v| v.slot))
            .collect();
        for st in self.branches.values() {
            slots.extend(st.current.values().map(|o| o.slot));
            slots.extend(
                st.lineage
                    .retained
                    .values()
                    .flat_map(|vs| vs.values().map(|v| v.slot)),
            );
        }
        slots
    }

    fn snapshot(&self) -> SnapshotState {
        let mut branches: Vec<SnapBranch> = self
            .branches
            .iter()
            .map(|(&id, st)| {
                let mut current: Vec<(u32, Slot, u64, u32)> = st
                    .current
                    .iter()
                    .map(|(&page, o)| (page, o.slot, o.born, o.crc))
                    .collect();
                current.sort_unstable();
                SnapBranch {
                    id: id.0,
                    parent: st.parent.0,
                    fork_epoch: st.fork_epoch,
                    epoch: st.lineage.epoch,
                    released: st.handle == Handle::Released,
                    // Deadline + 1, so that a real deadline of 0 is not read back as "no lease"
                    // (review R7). A saturated u64::MAX deadline comes back 1 ms shorter.
                    lease_deadline_ms: st.lease.map_or(0, |d| d.saturating_add(1)),
                    current,
                    retained: st.lineage.retained_list(),
                }
            })
            .collect();
        branches.sort_unstable_by_key(|b| b.id);
        SnapshotState {
            next_id: self.next_id,
            trunk_epoch: self.trunk.lineage.epoch,
            lease_now_ms: self.lease.now_ms(),
            trunk_retained: self.trunk.lineage.retained_list(),
            branches,
        }
    }

    fn load_snapshot(&mut self, snapshot: SnapshotState) -> Result<()> {
        self.next_id = snapshot.next_id;
        self.trunk.lineage.epoch = snapshot.trunk_epoch;
        self.lease.recovered(snapshot.lease_now_ms);
        for (page, born, died, slot, crc) in snapshot.trunk_retained {
            self.apply_trunk_retain(
                page,
                Retained {
                    born,
                    died,
                    slot,
                    crc,
                },
            )?;
        }
        let mut edges = Vec::with_capacity(snapshot.branches.len());
        for b in snapshot.branches {
            let mut lineage = Lineage {
                epoch: b.epoch,
                ..Lineage::default()
            };
            for (page, born, died, slot, crc) in b.retained {
                lineage.retain(
                    page,
                    Retained {
                        born,
                        died,
                        slot,
                        crc,
                    },
                );
            }
            let current = b
                .current
                .into_iter()
                .map(|(page, slot, born, crc)| (page, Owned { slot, born, crc }))
                .collect();
            edges.push((BranchId(b.parent), b.fork_epoch, BranchId(b.id)));
            let lease = (b.lease_deadline_ms != 0 && !b.released).then(|| b.lease_deadline_ms - 1);
            if let Some(deadline) = lease {
                self.leases.insert((deadline, BranchId(b.id)));
            }
            self.branches.insert(
                BranchId(b.id),
                BranchState {
                    parent: BranchId(b.parent),
                    fork_epoch: b.fork_epoch,
                    lineage,
                    current,
                    pending: BTreeMap::new(),
                    schema: None,
                    handle: if b.released {
                        Handle::Released
                    } else {
                        Handle::Detached
                    },
                    open: false,
                    writer: false,
                    lease,
                    // Placeholders: `derive_page_maps` sets both once every lineage is linked.
                    trunk_at: 0,
                    inherited: PageMap::default(),
                    view: None,
                },
            );
        }
        for (parent, f, child) in edges {
            let lineage = if parent.is_trunk() {
                &mut self.trunk.lineage
            } else {
                &mut self
                    .branches
                    .get_mut(&parent)
                    .ok_or_else(|| LimboError::Corrupt(format!(
                        "branch snapshot: branch {} names a missing parent {}",
                        child.0, parent.0
                    )))?
                    .lineage
            };
            if self.children.map.contains_key(&(parent.0, f)) {
                return Err(LimboError::Corrupt(format!(
                    "branch snapshot: two children at fork epoch {f} of branch {}",
                    parent.0
                )));
            }
            lineage.n_children += 1;
            self.children.insert(parent, f, child);
        }
        self.n_states = self.branches.len() as u64;
        self.derive_page_maps()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Review 4 C2 (the lead's decision): a read-only open of a database WITH branch files opens
    /// trunk-only — no recovery, no lock — and every branch operation on it is refused. This is the
    /// second fence behind the VDBE's read-only check: a trunk page write reaching this store is
    /// refused, because it would retain no pre-image for the branches on disk.
    #[test]
    fn a_read_only_store_over_branch_files_is_trunk_only() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let durable = BranchDurability::Durable { sync: false };
        {
            let first = BranchStore::open(durable, None, path).unwrap();
            first.inner.lock().ensure_backing(512).unwrap();
        }
        let ro = BranchStore::open_with_flags(BranchDurability::Volatile, None, path, true)
            .expect("a read-only open over branch files must open trunk-only");
        assert!(ro.has_branches(), "VACUUM and journal-mode changes must stay refused");
        assert!(ro.trunk_has_children(), "every trunk write must reach the refusal below");
        assert!(ro.first_write_trunk(1, &[0u8; 512]).is_err(), "a trunk-only store took a trunk write");
        assert!(ro.log_path().is_none(), "a trunk-only store opened the branch log");
        let _rw = BranchStore::open(durable, None, path).expect("a read-only store must hold no lock");
    }

    /// Review 4 C5. The empty store's restart moved the snapshot and the log header to the new
    /// page size; if the arena then cannot be reopened, the store must fail-stop, not keep taking
    /// records beside an arena of the old size.
    #[cfg(unix)]
    #[test]
    fn a_restart_whose_arena_reopen_fails_fail_stops_the_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: false }, None, path).unwrap();
        store.inner.lock().ensure_backing(512).unwrap();
        let files = BranchFiles::for_db(path);
        // The open arena's file is replaced by a directory: the restart's reopen fails.
        std::fs::remove_file(&files.arena).unwrap();
        std::fs::create_dir(&files.arena).unwrap();
        assert!(store.inner.lock().ensure_backing(1024).is_err(), "the arena reopened over a directory");
        std::fs::remove_dir(&files.arena).unwrap();
        let _ = store.inner.lock().ensure_backing(512);
        let mut inner = store.inner.lock();
        assert!(
            store.log(&mut inner, Record::Release { branch: 9 }).is_err(),
            "a store with a half-done restart took a record"
        );
    }

    /// Found while fixing review 3 F4: when every branch is gone, nothing refuses a page-size
    /// change (VACUUM and journal-mode changes are refused only while a branch exists). A store
    /// with nothing in it must then follow the database to the new page size. Before, its arena's
    /// old size refused every later fork, across reopens too: recovery opens the arena with the
    /// log's page size.
    #[test]
    fn an_empty_store_follows_the_database_to_a_new_page_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: false }, None, path).unwrap();
        store.inner.lock().ensure_backing(512).unwrap();
        store
            .inner
            .lock()
            .ensure_backing(1024)
            .expect("an empty store refused the database's new page size");
        {
            let mut inner = store.inner.lock();
            store.log(&mut inner, Record::Release { branch: 9 }).unwrap();
        }
        drop(store);
        let recovered = Journal::recover(&BranchFiles::for_db(path), false)
            .unwrap()
            .expect("state");
        assert_eq!(recovered.page_size, 1024, "the store kept the old page size");
        assert_eq!(recovered.records, vec![Record::Release { branch: 9 }]);
    }

    /// R3 (r11-bigtxn merge-1 review 4/6, artie-research f167c44b; PREREG A19): the catalog twin of the test above.
    /// An emptied catalog store that follows a new page size must leave its catalog agreeing with the truncated
    /// arena: no free row naming an old slot, and a high-water mark of 0. At d7a2b8f6e the next allocation panicked
    /// ("a free slot past the high-water mark"): the restart's checkpoint wrote the OLD arena's free rows and
    /// high-water mark; and a reopen restored the old high-water mark at the new page size.
    #[test]
    fn an_empty_catalog_store_follows_the_database_to_a_new_page_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        {
            let store =
                BranchStore::open(BranchDurability::Catalog { sync: false }, None, path).unwrap();
            let mut inner = store.inner.lock();
            inner.ensure_backing(512).unwrap();
            let s = inner.alloc_slot().unwrap();
            inner.release_slots(vec![s]);
            inner
                .ensure_backing(1024)
                .expect("an empty catalog store refused the database's new page size");
            let s = inner.alloc_slot().unwrap();
            inner.release_slots(vec![s]);
        }
        let store = BranchStore::open(BranchDurability::Catalog { sync: false }, None, path).unwrap();
        let inner = store.inner.lock();
        let arena = inner.arena.as_ref().expect("the reopened catalog store has its arena");
        assert_eq!(arena.page_size(), 1024, "the store kept the old page size");
        let file_len = std::fs::metadata(&BranchFiles::for_db(path).arena).map_or(0, |m| m.len());
        assert!(
            u64::from(arena.high_water()) * 1024 <= file_len,
            "the reopened arena's high-water mark {} (at 1024 B) exceeds its file ({file_len} B)",
            arena.high_water()
        );
    }

    /// The guard beside it: a store that still HOLDS something — here one branch — cannot follow
    /// a page-size change, and must keep refusing it.
    #[test]
    fn a_store_holding_a_branch_refuses_a_new_page_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: false }, None, path).unwrap();
        let mut inner = store.inner.lock();
        inner.ensure_backing(512).unwrap();
        inner
            .apply_fork(BranchId::TRUNK, BranchId(1), None, Handle::Detached)
            .unwrap();
        assert!(inner.ensure_backing(1024).is_err(), "a store holding a branch changed page size");
    }

    /// Review 3 F4. A journal kept from a first fork whose ARENA failed to open holds only its
    /// header, written for that attempt's page size. A retry at another page size must not append
    /// under the old one: recovery would then open the arena with the wrong page size.
    #[test]
    fn a_kept_journal_takes_the_retrys_page_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: false }, None, path).unwrap();
        let files = BranchFiles::for_db(path);
        // A directory where the arena file goes: the first attempt's arena open fails.
        std::fs::create_dir(&files.arena).unwrap();
        assert!(store.inner.lock().ensure_backing(512).is_err(), "the arena opened over a directory");
        std::fs::remove_dir(&files.arena).unwrap();
        store.inner.lock().ensure_backing(1024).unwrap();
        {
            let mut inner = store.inner.lock();
            store.log(&mut inner, Record::Release { branch: 9 }).unwrap();
        }
        drop(store);
        let recovered = Journal::recover(&files, false).unwrap().expect("state");
        assert_eq!(recovered.page_size, 1024, "the log kept the failed attempt's page size");
        assert_eq!(recovered.records, vec![Record::Release { branch: 9 }]);
    }

    /// N1, the lazy door. A store that opened when no branch files existed holds no lock until its
    /// first fork creates them. If another store created them in between, that first fork must
    /// refuse — while the other lives (its lock) and after it has gone (its files now hold state):
    /// "start the files over" is only right for files that held nothing recoverable.
    #[test]
    fn a_store_that_opened_before_the_files_existed_does_not_start_them_over() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let durable = BranchDurability::Durable { sync: false };
        let late = BranchStore::open(durable, None, path).unwrap();
        {
            let first = BranchStore::open(durable, None, path).unwrap();
            let mut inner = first.inner.lock();
            inner.ensure_backing(512).unwrap();
            first.log(&mut inner, Record::Release { branch: 7 }).unwrap();
            drop(inner);
            assert!(
                late.inner.lock().ensure_backing(512).is_err(),
                "a second store created its files over a live store's"
            );
        }
        assert!(
            late.inner.lock().ensure_backing(512).is_err(),
            "a store started over files another store had written since it opened"
        );
        let files = BranchFiles::for_db(path);
        let recovered = Journal::recover(&files, false).unwrap().expect("state");
        assert_eq!(recovered.records, vec![Record::Release { branch: 7 }]);
    }

    /// R7. A deadline of 0 is a real deadline (`lease(ZERO)` at the clock's first millisecond),
    /// and must not come back from a snapshot as "no lease" — which would make the branch
    /// permanent.
    #[test]
    fn a_zero_deadline_is_still_a_lease_after_a_snapshot() {
        let mut live = StoreInner::fresh(None, false, None);
        let id = BranchId(1);
        live.apply_fork(BranchId::TRUNK, id, None, Handle::Detached)
            .unwrap();
        live.apply_lease(id, 0);
        let snapshot = live.snapshot();
        let mut recovered = StoreInner::fresh(None, false, None);
        recovered.load_snapshot(snapshot).unwrap();
        assert_eq!(recovered.branches[&id].lease, Some(0), "a 0 deadline read back as no lease");
        assert!(recovered.leases.contains(&(0, id)), "the deadline index lost it");
    }
}

/// Shared by the sota-durable tests (round 11 PREREG D3): stores of every mode (catalog added by the
/// a12-durable-open lane), and crash images of a durable one.
#[cfg(test)]
mod sota_helpers {
    use super::*;

    pub(super) const PAGE: usize = 512;

    pub(super) struct Rng(pub(super) u64);
    impl Rng {
        pub(super) fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    pub(super) fn image(generation: u64) -> Vec<u8> {
        generation.to_le_bytes().repeat(PAGE / 8)
    }

    /// The store modes the lane tests run in (a12-durable-open lane: catalog mode added).
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub(super) enum Mode {
        Volatile,
        Durable,
        Catalog,
    }

    pub(super) const MODES: [Mode; 3] = [Mode::Volatile, Mode::Durable, Mode::Catalog];

    impl Mode {
        fn durability(self) -> BranchDurability {
            match self {
                Mode::Volatile => BranchDurability::Volatile,
                Mode::Durable => BranchDurability::Durable { sync: false },
                Mode::Catalog => BranchDurability::Catalog { sync: false },
            }
        }

        pub(super) fn durable(self) -> bool {
            self != Mode::Volatile
        }
    }

    /// A store of the given mode over `dir` (unused when volatile).
    pub(super) fn open_store(mode: Mode, dir: &std::path::Path, name: &str) -> BranchStore {
        let path = if mode.durable() {
            dir.join(name).to_str().unwrap().to_string()
        } else {
            ":memory:".to_string()
        };
        BranchStore::open(mode.durability(), None, &path).unwrap()
    }

    /// Copy every branch file of the store at `dir/name` to `dir/<image>` while it is open, and
    /// open the copy: what a kill -9 at this point would recover. A catalog store's files include
    /// the catalog and its WAL.
    pub(super) fn crash_image(mode: Mode, dir: &std::path::Path, name: &str, image: &str) -> BranchStore {
        let from = BranchFiles::for_db(dir.join(name).to_str().unwrap());
        let to = BranchFiles::for_db(dir.join(image).to_str().unwrap());
        let wal = |p: &std::path::Path| std::path::PathBuf::from(format!("{}-wal", p.display()));
        for (src, dst) in [
            (from.log.clone(), to.log.clone()),
            (from.arena.clone(), to.arena.clone()),
            (from.snap.clone(), to.snap.clone()),
            (from.cat.clone(), to.cat.clone()),
            (wal(&from.cat), wal(&to.cat)),
        ] {
            let _ = std::fs::remove_file(&dst);
            if src.exists() {
                std::fs::copy(&src, &dst).unwrap();
            }
        }
        BranchStore::open(mode.durability(), None, to_str(&dir.join(image))).unwrap()
    }

    fn to_str(p: &std::path::Path) -> &str {
        p.to_str().unwrap()
    }
}

#[cfg(test)]
impl Lineage {
    /// The per-page maps and both indexes hold exactly the same versions.
    fn check_indexes(&self, what: &str) {
        let mut from_maps = BTreeSet::new();
        for (&page, versions) in &self.retained {
            for (&born, v) in versions {
                assert_eq!(born, v.born, "{what}: page {page} keyed under the wrong born");
                from_maps.insert((v.born, page, v.died));
            }
        }
        assert_eq!(from_maps, self.by_born, "{what}: by_born disagrees with the per-page maps");
        let died: BTreeSet<(u64, u32, u64)> =
            self.by_died.iter().map(|&(died, page, born)| (born, page, died)).collect();
        assert_eq!(from_maps, died, "{what}: by_died disagrees with the per-page maps");
    }
}

#[cfg(test)]
impl BranchStore {
    /// Catalog trunk versions reaped since the last checkpoint (C-P; 0 for other modes).
    fn trunk_gone_len(&self) -> u64 {
        self.inner.lock().cat.as_ref().map_or(0, |c| c.trunk_gone.len() as u64)
    }

    /// Every lineage's retained versions agree across the per-page maps and both indexes.
    fn check_indexes(&self) {
        let inner = self.inner.lock();
        inner.trunk.lineage.check_indexes("trunk");
        for (id, st) in &inner.branches {
            st.lineage.check_indexes(&format!("branch {}", id.0));
        }
    }
}

/// Round 10's F1/F2 tests (turso `2d2653599`, `a3d79b98d`), ported to the durable store: the
/// store's own entry points against a brute-force model, and the garbage query's cost contract.
/// Each runs volatile and durable (no sync; the barrier flushes each trunk write's records as a
/// trunk commit would), and the durable run opens a CRASH IMAGE — every branch file copied while
/// the store is open, no clean close — at random points and checks that recovery rebuilt the same
/// retained versions and indexes.
#[cfg(test)]
mod sota_index_tests {
    use super::sota_helpers::{crash_image, image, open_store, Mode, Rng, MODES, PAGE};
    use super::*;
    use std::collections::HashSet;

    const PAGES: u32 = 6;

    /// The trunk's retained-version index against a brute-force model, through the store's own
    /// entry points, and the garbage query's cost against its contract: a reap with no older live
    /// sibling, or no younger one, visits exactly the versions it frees, and one with both visits
    /// 2·|B| index entries if |B| <= |D| and 2·|D| + 1 otherwise.
    #[test]
    fn retained_versions_match_a_model_under_forks_rewrites_and_reaps_in_every_order() {
        for mode in MODES {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run(seed, mode);
            }
        }
    }

    fn run(seed: u64, mode: Mode) {
        let durable = mode.durable();
        let dir = tempfile::TempDir::new().unwrap();
        let store = open_store(mode, dir.path(), "db");
        let mut rng = Rng(seed);
        let mut current: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut live: Vec<(BranchId, u64, HashMap<u32, u64>)> = Vec::new();
        let mut history: Vec<(u32, u64, u64)> = Vec::new();
        let mut written: HashMap<u32, u64> = HashMap::new();
        let mut epoch = 0u64;
        let mut generation = 0u64;
        let (mut freed_oldest, mut freed_newest, mut freed_middle, mut images) = (0, 0, 0, 0);
        for step in 0..1500 {
            match rng.below(10) {
                0..=2 if live.len() < 40 => {
                    // Early release (r11-churn amendment 4): a fork is handed out once durable.
                    let (id, lsn) = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    store.wait_durable(lsn).unwrap();
                    live.push((id, epoch, current.clone()));
                    epoch += 1;
                }
                0..=5 => {
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
                            store.first_write_trunk(page, &image(current[&page])).unwrap();
                        }
                        generation += 1;
                        current.insert(page, generation);
                    }
                    // The trunk commit: its barrier makes the buffered pre-image records durable.
                    store.durability_barrier().unwrap();
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
                    let before = store.stats().unwrap();
                    let reaped = store.release_handle(id).unwrap();
                    let after = store.stats().unwrap();
                    assert!(!reaped.deferred, "{mode:?} seed {seed:#x} step {step}");
                    assert_eq!(
                        before.arena_slots_in_use - after.arena_slots_in_use,
                        reaped.freed_pages,
                        "{mode:?} seed {seed:#x} step {step}: the reap's report disagrees with the arena"
                    );
                    let visited = after.work.gc_range_entries - before.work.gc_range_entries;
                    let contract = match (lo, hi) {
                        (None, _) | (_, None) => reaped.freed_pages as u64,
                        _ if b <= d => 2 * b,
                        _ => 2 * d + 1,
                    };
                    // Eager modes: F2's exact in-memory contract. Catalog mode (C-P) walks the
                    // in-memory versions under the same contract and reads the catalog's ranges in
                    // place, in doubling batches (at most 4x the smaller range plus 64) or one range
                    // whose entries are garbage or reaped since the checkpoint: so at most the
                    // contract, plus max(64, 8 (contract + 1)), plus the versions reaped since.
                    if mode == Mode::Catalog {
                        let bound = contract + (8 * (contract + 1)).max(64) + store.trunk_gone_len();
                        assert!(
                            visited <= bound,
                            "{mode:?} seed {seed:#x} step {step}: reaping the child forked at {f} (lo \
                             {lo:?}, hi {hi:?}, |B| {b}, |D| {d}) visited {visited} index entries, \
                             bound {bound}"
                        );
                    } else {
                        assert_eq!(
                            visited, contract,
                            "{mode:?} seed {seed:#x} step {step}: reaping the child forked at {f} (lo {lo:?}, \
                             hi {hi:?}, |B| {b}, |D| {d}) visited {visited} index entries"
                        );
                    }
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
                store.stats().unwrap().arena_slots_in_use,
                alive.len(),
                "{mode:?} seed {seed:#x} step {step}: the arena holds a version no live child can see, or \
                 lost one a live child can"
            );
            history.retain(|v| alive.contains(v));
            store.check_indexes();
            let check = |s: &BranchStore, what: &str| {
                let mut buf = vec![0u8; PAGE];
                for (id, f, view) in &live {
                    for page in 0..PAGES {
                        let in_arena = s.resolve_into(*id, page, &mut buf).unwrap();
                        let got = if in_arena {
                            u64::from_le_bytes(buf[..8].try_into().unwrap())
                        } else {
                            current[&page]
                        };
                        assert_eq!(
                            got, view[&page],
                            "{mode:?} seed {seed:#x} step {step} {what}: child forked at {f} read the \
                             wrong page {page}"
                        );
                    }
                }
            };
            check(&store, "live");
            if durable && rng.below(25) == 0 {
                if rng.below(3) == 0 {
                    store.compact_now().unwrap();
                }
                let recovered = crash_image(mode, dir.path(), "db", "image");
                images += 1;
                recovered.check_indexes();
                assert_eq!(
                    recovered.slots_in_use(),
                    store.slots_in_use(),
                    "{mode:?} seed {seed:#x} step {step}: recovery changed the live slot set"
                );
                check(&recovered, "after recovery");
            }
        }
        assert!(
            freed_oldest > 0 && freed_newest > 0 && freed_middle > 0 && (!durable || images > 10),
            "{mode:?} seed {seed:#x}: reaps that freed versions: oldest {freed_oldest}, newest \
             {freed_newest}, middle {freed_middle}; crash images {images}"
        );
        for (id, _, _) in live {
            store.release_handle(id).unwrap();
        }
        assert_eq!(store.stats().unwrap().arena_slots_in_use, 0, "{mode:?} seed {seed:#x}: versions leaked");
    }
}

/// Round 10's F4 tree test (turso `a31198dd8`), ported to the durable store and run volatile and
/// durable: branch TREES — forks from the trunk and from branches, deep chains, trunk writes and
/// branch commits before and after forking, deferred reaps — against a model in which each branch is
/// a plain copy of its parent's pages at its fork. The durable run compacts at random steps and opens
/// a CRASH IMAGE (every branch file copied while the store is open) at random points: after
/// recovery — replay alone, or a snapshot whose page maps `derive_page_maps` rebuilt — every live
/// branch must read every page as before, from the same slot set, with consistent indexes.
#[cfg(test)]
mod sota_tree_tests {
    use super::sota_helpers::{crash_image, image, open_store, Mode, Rng, MODES, PAGE};
    use super::*;

    const PAGES: u32 = 6;

    /// A committed branch page holding `image(generation)`, as the pager hands it to
    /// `commit_pages`.
    fn page_with(page: u32, generation: u64) -> PageRef {
        let p = Arc::new(crate::storage::pager::Page::new(i64::from(page)));
        let buffer = Arc::new(crate::Buffer::new_temporary(PAGE));
        buffer.as_mut_slice().copy_from_slice(&image(generation));
        p.get().buffer = Some(buffer);
        p
    }

    struct Node {
        id: BranchId,
        sees: HashMap<u32, u64>,
        handle: bool,
        depth: usize,
        forked: bool,
    }

    #[test]
    fn every_branch_of_a_random_tree_reads_its_parent_as_of_its_fork() {
        for mode in MODES {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run_tree(seed, mode);
            }
        }
    }

    fn run_tree(seed: u64, mode: Mode) {
        let durable = mode.durable();
        let dir = tempfile::TempDir::new().unwrap();
        let store = open_store(mode, dir.path(), "db");
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut nodes: Vec<Node> = Vec::new();
        let mut generation = 0u64;
        let (mut deferred, mut max_depth, mut wrote_after_fork, mut images) = (0, 0, 0, 0);
        for step in 0..2500 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            match rng.below(12) {
                0 if live.len() < 60 => {
                    // Early release (r11-churn amendment 4): a fork is handed out once durable.
                    let (id, lsn) = store.fork_trunk(Arc::new(Schema::default()), PAGE).unwrap();
                    store.wait_durable(lsn).unwrap();
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
                    let (id, lsn) = store.fork_branch(nodes[parent].id).unwrap();
                    store.wait_durable(lsn).unwrap();
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
                        store.first_write_trunk(page, &image(trunk[&page])).unwrap();
                    }
                    store.durability_barrier().unwrap();
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
                        store.first_write_branch(id, page).unwrap();
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
                    if store.release_handle(nodes[v].id).unwrap().deferred {
                        deferred += 1;
                    }
                }
                _ => {}
            }
            let check = |s: &BranchStore, what: &str| {
                let mut buf = vec![0u8; PAGE];
                for n in nodes.iter().filter(|n| n.handle) {
                    for page in 0..PAGES {
                        let got = if s.resolve_into(n.id, page, &mut buf).unwrap() {
                            u64::from_le_bytes(buf[..8].try_into().unwrap())
                        } else {
                            trunk[&page]
                        };
                        assert_eq!(
                            got,
                            n.sees[&page],
                            "{mode:?} seed {seed:#x} step {step} {what}: branch {} at depth {} read the \
                             wrong page {page}",
                            n.id.0,
                            n.depth
                        );
                    }
                }
            };
            check(&store, "live");
            if durable && rng.below(20) == 0 {
                if rng.below(2) == 0 {
                    store.compact_now().unwrap();
                }
                let recovered = crash_image(mode, dir.path(), "db", "image");
                images += 1;
                recovered.check_indexes();
                assert_eq!(
                    recovered.slots_in_use(),
                    store.slots_in_use(),
                    "{mode:?} seed {seed:#x} step {step}: recovery changed the live slot set"
                );
                check(&recovered, "after recovery");
            }
        }
        // The shapes the page maps exist for must have occurred, or a green run says nothing.
        assert!(
            max_depth >= 10 && deferred > 0 && wrote_after_fork > 0 && (!durable || images > 20),
            "{mode:?} seed {seed:#x}: max depth {max_depth}, deferred reaps {deferred}, writes by a branch \
             after its first fork {wrote_after_fork}, crash images {images}"
        );
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id).unwrap();
        }
        assert_eq!(store.stats().unwrap().live_branches, 0, "{mode:?} seed {seed:#x}: branches leaked");
        assert_eq!(store.stats().unwrap().arena_slots_in_use, 0, "{mode:?} seed {seed:#x}: slots leaked");
    }

    /// The failpoints this test kills the store at, each armed just before the operation that
    /// consumes it.
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    enum Kill {
        ForkFlush,
        TrunkBarrier,
        CommitAfterSlots,
        CommitFlush,
        ReleaseFlush,
        CompactAfterRename,
    }
    const KILLS: [Kill; 6] = [
        Kill::ForkFlush,
        Kill::TrunkBarrier,
        Kill::CommitAfterSlots,
        Kill::CommitFlush,
        Kill::ReleaseFlush,
        Kill::CompactAfterRename,
    ];

    /// kill -9 AT THE FAILPOINTS (round 11 PREREG D3): the same random tree, durable, but now and
    /// then the next operation is made to fail at one of the store's failpoints — a fork or commit
    /// whose record never becomes durable, a commit that wrote its slots and died before its
    /// record, a trunk commit that died at its barrier, a release whose record failed, a compaction
    /// that died after renaming its snapshot. The process "dies" there: every branch file is copied
    /// as it stands and the copy is opened, and the workload CONTINUES on the recovered store, so
    /// forks, commits, reaps and compactions run against state that recovery rebuilt (the page maps
    /// `derive_page_maps` made, the indexes `Lineage::retain` refilled). The model is the state the
    /// failed operation did not reach. After every recovery every live branch reads every page as
    /// the model says and the indexes agree; at the end every branch is released and the arena is
    /// empty, so no recovery leaked a slot or freed one twice.
    #[test]
    fn every_branch_reads_as_the_model_says_after_a_kill_at_any_failpoint() {
        for mode in [Mode::Durable, Mode::Catalog] {
            for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
                run_killed(seed, mode);
            }
        }
    }

    fn run_killed(seed: u64, mode: Mode) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut name = "db".to_string();
        let mut store = open_store(mode, dir.path(), &name);
        let mut rng = Rng(seed);
        let mut trunk: HashMap<u32, u64> = (0..PAGES).map(|p| (p, 0)).collect();
        let mut nodes: Vec<Node> = Vec::new();
        let mut generation = 0u64;
        let mut kills: HashMap<Kill, u32> = HashMap::new();
        let (mut max_depth, mut lives) = (0, 0);
        for step in 0..3000 {
            let live: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].handle).collect();
            // One operation in ten is killed at a failpoint that operation consumes.
            let kill = rng.below(10) == 0;
            let arm = |store: &BranchStore, fp: BranchFailpoint| {
                if kill {
                    store.set_failpoint(Some(fp));
                }
            };
            let mut killed: Option<Kill> = None;
            match rng.below(13) {
                0..=3 if live.len() < 50 => {
                    let from_trunk = live.is_empty() || rng.below(4) == 0;
                    arm(&store, BranchFailpoint::LogFlushFails);
                    let (forked, sees, depth) = if from_trunk {
                        // Early release (r11-churn amendment 4): a fork is handed out once
                        // durable, and a failed flight is a failed fork.
                        let r = store
                            .fork_trunk(Arc::new(Schema::default()), PAGE)
                            .and_then(|(id, lsn)| store.wait_durable(lsn).map(|()| id));
                        (r, trunk.clone(), 1)
                    } else {
                        let parent = if rng.below(2) == 0 {
                            *live.last().unwrap()
                        } else {
                            live[rng.below(live.len() as u64) as usize]
                        };
                        let r = store
                            .fork_branch(nodes[parent].id)
                            .and_then(|(id, lsn)| store.wait_durable(lsn).map(|()| id));
                        if r.is_ok() {
                            nodes[parent].forked = true;
                        }
                        (r, nodes[parent].sees.clone(), nodes[parent].depth + 1)
                    };
                    match forked {
                        Ok(id) => {
                            max_depth = max_depth.max(depth);
                            nodes.push(Node { id, sees, handle: true, depth, forked: false });
                        }
                        Err(e) => {
                            assert!(kill, "{mode:?} seed {seed:#x} step {step}: fork failed unarmed: {e}");
                            killed = Some(Kill::ForkFlush);
                        }
                    }
                }
                4..=5 => {
                    let page = rng.below(u64::from(PAGES)) as u32;
                    if store.trunk_has_children() {
                        store.first_write_trunk(page, &image(trunk[&page])).unwrap();
                    }
                    arm(&store, BranchFailpoint::BarrierBeforeRecords);
                    match store.durability_barrier() {
                        Ok(()) => {
                            generation += 1;
                            trunk.insert(page, generation);
                        }
                        // The trunk commit dies at its barrier: it never happened.
                        Err(e) => {
                            assert!(kill, "{mode:?} seed {seed:#x} step {step}: barrier failed unarmed: {e}");
                            killed = Some(Kill::TrunkBarrier);
                        }
                    }
                }
                6..=9 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    let id = nodes[v].id;
                    store.begin_write(id).unwrap();
                    let mut committed = Vec::new();
                    let mut sees = nodes[v].sees.clone();
                    for _ in 0..=rng.below(2) {
                        let page = rng.below(u64::from(PAGES)) as u32;
                        if committed.iter().any(|p: &PageRef| p.get().id == page as usize) {
                            continue;
                        }
                        store.first_write_branch(id, page).unwrap();
                        generation += 1;
                        committed.push(page_with(page, generation));
                        sees.insert(page, generation);
                    }
                    let fp = if rng.below(2) == 0 {
                        BranchFailpoint::CommitAfterSlotsBeforeRecord
                    } else {
                        BranchFailpoint::LogFlushFails
                    };
                    arm(&store, fp);
                    match store.commit_pages(id, &committed) {
                        Ok(()) => {
                            store.end_write(id);
                            nodes[v].sees = sees;
                        }
                        // The commit is not durable: the branch is as it was.
                        Err(e) => {
                            assert!(kill, "{mode:?} seed {seed:#x} step {step}: commit failed unarmed: {e}");
                            killed = Some(if fp == BranchFailpoint::LogFlushFails {
                                Kill::CommitFlush
                            } else {
                                Kill::CommitAfterSlots
                            });
                        }
                    }
                }
                10..=11 if !live.is_empty() => {
                    let v = live[rng.below(live.len() as u64) as usize];
                    arm(&store, BranchFailpoint::LogFlushFails);
                    match store.release_handle(nodes[v].id) {
                        Ok(_) => nodes[v].handle = false,
                        // The release is not durable: the branch comes back, detached.
                        Err(e) => {
                            assert!(kill, "{mode:?} seed {seed:#x} step {step}: release failed unarmed: {e}");
                            killed = Some(Kill::ReleaseFlush);
                        }
                    }
                }
                12 if !nodes.is_empty() => {
                    arm(&store, BranchFailpoint::CompactAfterRenameBeforeLogReset);
                    if let Err(e) = store.compact_now() {
                        assert!(kill, "{mode:?} seed {seed:#x} step {step}: compaction failed unarmed: {e}");
                        killed = Some(Kill::CompactAfterRename);
                    }
                }
                _ => {}
            }
            // An armed failpoint the operation did not consume (a barrier with nothing to flush)
            // is disarmed: the operation completed.
            store.set_failpoint(None);
            let check = |s: &BranchStore, what: &str| {
                let mut buf = vec![0u8; PAGE];
                for n in nodes.iter().filter(|n| n.handle) {
                    for page in 0..PAGES {
                        let got = if s.resolve_into(n.id, page, &mut buf).unwrap() {
                            u64::from_le_bytes(buf[..8].try_into().unwrap())
                        } else {
                            trunk[&page]
                        };
                        assert_eq!(
                            got, n.sees[&page],
                            "{mode:?} seed {seed:#x} step {step} {what}: branch {} at depth {} read the \
                             wrong page {page}",
                            n.id.0, n.depth
                        );
                    }
                }
            };
            if let Some(k) = killed {
                *kills.entry(k).or_default() += 1;
                let next = format!("life{lives}");
                lives += 1;
                let recovered = crash_image(mode, dir.path(), &name, &next);
                recovered.check_indexes();
                check(&recovered, &format!("after a kill at {k:?}"));
                // The dead process's store goes; the workload continues on what recovery built.
                store = recovered;
                name = next;
            } else {
                check(&store, "live");
            }
        }
        for n in nodes.iter().filter(|n| n.handle) {
            store.release_handle(n.id).unwrap();
        }
        assert_eq!(store.stats().unwrap().live_branches, 0, "{mode:?} seed {seed:#x}: branches leaked");
        assert_eq!(
            store.stats().unwrap().arena_slots_in_use,
            0,
            "{mode:?} seed {seed:#x}: slots leaked across {lives} recoveries"
        );
        // The shapes the test exists for must have occurred, or a green run says nothing.
        for k in KILLS {
            assert!(
                kills.get(&k).copied().unwrap_or(0) > 0,
                "{mode:?} seed {seed:#x}: no kill at {k:?} (kills {kills:?})"
            );
        }
        assert!(max_depth >= 10, "{mode:?} seed {seed:#x}: max depth {max_depth}");
    }
}
