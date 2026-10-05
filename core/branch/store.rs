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
//! `[born, died)`. A child's own epochs start one past its `fork_epoch` (epoch inheritance, the F7
//! durable port below), so along a line of descent the epochs nest. A branch therefore sees, for
//! each page:
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
//! at depth 1000 as at depth 1. The first fork builds the `view`, from the current versions born
//! after `inherited` was taken only (`inherited_at`): a splice's moved-in versions are already in
//! `inherited`, and re-inserting them made that build O(every page the dead levels above moved in). A page no branch in the chain wrote is the trunk's, as of
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
//! * on the **trunk**, the page as it was before the transaction is CAPTURED while the trunk has a
//!   live child, and the decision is taken at the commit against the epoch there
//!   (`BranchStore::begin_trunk_commit`, F-L on the durable store): a page a live child can still
//!   see has its pre-image copied into a slot and retained, durably, before the commit's first
//!   frame, so neither the commit nor a later checkpoint that moves the new version into the
//!   database file can reach the child, and a child forked while the transaction is open is seen.
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
//! child can read — a current version is read by the children forked at or after its birth, so
//! those born after its newest live child's fork are freed at the release itself, and again each
//! time a child of it goes — and it is freed whole when its last child goes, which may in turn free
//! its parent. (Before the F7 durable port the kept current versions became retained ones that
//! died at the release epoch; they stay `current`, indexed by `born`, so a splice can merge them by
//! size.)
//!
//! # Splicing a zombie out (F7 durable port, r11-ever; UNBUILT)
//!
//! An ARM, off by default (`DatabaseOpts::with_branch_splice`; the lane's suites and harnesses turn
//! it on with `R11_SPLICE=1` and name it). Off, a released branch with a live child is only
//! retired, as above, and kept until its last child goes: the base's rule. Its frees are the base's
//! at the same calls (argued from source, not run: a kept current version is read by the children
//! forked at or after its birth, so it is garbage exactly when the newest child's fork is below its
//! birth, which is the base's interval rule for a version retained until the release epoch, since
//! every child forked below that epoch), so the per-release attribution the base's tests assert
//! holds unedited. In the arm, a released branch no connection holds, left with exactly ONE live
//! child, is spliced out: the
//! child takes its place under its parent at the same fork epoch and inherits the versions it read
//! through it (`StoreInner::splice`; the volatile store's F7, turso r11-ever fad7db24d / a85f41ab2 /
//! d9be3f03a; prior art: ZFS `zfs promote`, QEMU `block-stream` / `block-commit`, Neon
//! `detach_ancestor`). Without it a workload that forks from its newest branch and releases its
//! oldest keeps every branch it ever created: as catalog rows, children-index entries and snapshot
//! entries. With it a kept released branch no connection holds has two or more live children, so
//! kept states number under twice the live ones. Composed with this file's other fixes, the splice
//! carries three obligations (r11-invariant-matrix U5-U7). U8 does not arise on this base: F-reclaim
//! (turso e7d0fd4a7) is not in it, and with it a splice would first have to drain or filter the
//! zombie's queued ranges, which name slots the splice moves to the child:
//!
//! * U5, F1's per-page order: a splice can retain a version OLDER than the child's own, so
//!   `Lineage::retain` checks both neighbours, not only the last (fad7db24d's check).
//! * U6, the children index: the child is re-keyed under its new parent (`ChildIndex::relink`: the
//!   zombie's entry is replaced, never removed, so no removal link is left under a key the child
//!   occupies), and a catalog store rewrites the child's (parent, fork epoch) at its next checkpoint
//!   (`DIRTY_KEY`, `Catalog::rekey`), which moves its `branch_children` entry with it.
//! * U7, redo on demand: the splice makes the child resident first (`ensure`, which applies its
//!   parked Commits), in the code path, before any map moves.
//!
//! A splice is a pure function of the state, and every collect happens at a logged point, so replay
//! repeats every splice where the live store made it and needs no record of its own: a release
//! (`Release`, or `ReleaseOpen` when a connection holds the branch), the close of a held branch
//! (`Close`), and the end of recovery, which logs a `Close` for every branch a crash left held. A
//! snapshot records the hold (`held_open`), a catalog row as `released = 2`. The log format is 3, and
//! 4 in the splice arm (both arms have epoch inheritance and the hold records): each arm reads only
//! its own, since replay without the splices that were made, or with ones that were not, would
//! rebuild another tree. A catalog store also carries the version in its meta row (`Meta::format`),
//! checked at open, so a torn log header does not let the other arm, or a pre-port catalog, in.
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
//! * One `Mutex` guards every branch, held across the durable store's fsyncs. Correct, and a named
//!   wall under concurrent writers; the benchmark this lane ships is single-threaded and says so.
//! * The persistent page maps are an index over slots the lineages own; they own nothing. A
//!   branch's `inherited` names only slots its ancestors keep for it, so dropping a map never frees
//!   a page and keeping one never pins a page.
//! * Snapshot mode (`BranchDurability::Durable`) recovers eagerly: every branch map is materialised
//!   at open, O(live branch state). Catalog mode reads state on demand (see `catalog.rs`).
//! * A catalog written before the F7 durable port (children's epochs from 0), or in the other splice
//!   arm, is refused by its meta row's format key (`Meta::format`, in the page-size key's high bits),
//!   as well as by its log header's version, so a torn header does not let it open.
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
use std::ops::Bound;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::arena::{Arena, Slot};
use super::catalog::{CatBranch, Catalog, Meta};
use super::prewarm::{self, Prewarm, PrewarmStats, Targets};
use super::id_set::{IdSet, IdSetWork};
use super::journal::{BranchFiles, Flight, Journal, Record, SnapBranch, SnapshotState};
use super::page_map::PageMap;
use super::table::BranchTable;
use super::{
    BranchDurability, BranchFailpoint, BranchId, BranchOpenStats, BranchStats, BranchWork, Expired,
    Reaped, SyncClass,
};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

pub(crate) struct BranchStore {
    /// Shared with a fuzzy checkpoint's thread, which takes it for its install (F-FZ).
    inner: Arc<StoreMutex>,
    /// What this open's prewarm did to files other than the catalog (r12-catload; the catalog's
    /// own part is on its handle).
    prewarm: PrewarmStats,
    /// Threads of fuzzy checkpoints started (F-FZ): joined by `compact_now`, by `Drop`, and once
    /// more than `FLIGHTS_KEPT` accumulate (the oldest is long past its install).
    flights: Mutex<Vec<crate::thread::JoinHandle<()>>>,
    /// The name filter's build after open (`start_name_filter`), and its stop flag (set at drop).
    name_filter_build: Mutex<Option<crate::thread::JoinHandle<()>>>,
    name_filter_stop: Arc<std::sync::atomic::AtomicBool>,
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
    /// a trunk fork that holds the trunk's WAL write lock (a lock-free fork refuses to be the first
    /// child: `TrunkFork::NeedsWriterLock`); a trunk writer reading this holds the same lock. A
    /// 1-to-0 transition (a reap) racing the read only makes the writer capture a pre-image nobody
    /// needs. Changed only under the store mutex, in the same hold as the change to the trunk's
    /// children (F-L's F6 condition (i)).
    trunk_children: AtomicUsize,
    /// F-L's commit gate (fastest-engine M1 item 1, r11-forklock 573642f19 on the durable store):
    /// trunk commits that have taken their copy decisions, counted twice — odd while a commit is
    /// between its decisions and its publication (see "Trunk commits and forks" in
    /// `begin_trunk_commit`). Opened only under the store mutex, by the trunk's one writer; closed
    /// by the same writer once its frames are published or its commit failed.
    trunk_commits: AtomicU64,
    /// Signalled at every close of the commit gate, so a lock-free fork that found it open waits
    /// for the close instead of spinning (or falling back to the WAL write lock).
    gate_closed: (std::sync::Mutex<()>, std::sync::Condvar),
    /// Trunk pre-image records wait in the journal's buffer between the trunk's `add_dirty` and its
    /// commit. The commit's barrier reads this without the lock, so a trunk commit with nothing to
    /// make durable pays one atomic load.
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
    /// Per-fork lock holds (fastest-engine M1 item 5; observing only).
    holds: ForkHoldCounters,
    /// Group commit with the flush outside the store mutex (fastest-engine M1 item 2); see
    /// [`Group`]. Shared with a fuzzy checkpoint's thread, whose install rewrites the log.
    group: Arc<Group>,
    /// The store's class, fixed at open (`Off` when volatile), readable without the mutex.
    class: SyncClass,
    /// Test hook: while it holds `HOLD_TRUNK_DECIDED`, a trunk commit waits inside its commit gate
    /// (see `pause_at`), so a test can act on the store with the gate open.
    #[cfg(test)]
    pub(crate) trunk_commit_hold: Arc<AtomicU8>,
    /// Test hook: the publication wait in milliseconds, in place of `PUBLISH_WAIT` (0: unset).
    #[cfg(test)]
    publish_wait_ms: AtomicU64,
    /// The trunk's WAL flush is an F_FULLFSYNC of the branch files' device (`note_trunk_wal`,
    /// `ordered_trunk`).
    trunk_same_device: AtomicBool,
    /// The device the branch log and arena were opened on (`StoreInner::files_device`), shared
    /// with the store's inner state; `NO_DEVICE` until they are.
    files_dev: Arc<AtomicU64>,
    /// Trunk commit barriers that took the store mutex (observation only; lead review 1 item 10).
    barrier_locks: AtomicU64,
    /// A catalog store's checkpoints are fuzzy (`BranchCheckpoint`; lead review 1 item 7).
    fuzzy: bool,
    /// The log sequence number that makes the newest Release durable (early-released or logged):
    /// every trunk commit's barrier covers it, since a commit retains nothing for a child released
    /// before its decisions (gc3's N1; lead review 1 item 10; skill review 2 #3).
    last_release_lsn: AtomicU64,
    /// The log sequence number that makes the newest TrunkRetain durable: every trunk commit's
    /// barrier covers it too, since a pre-image kept by a decision pass that was then refused (or
    /// rolled back) is not decided again by the retry — the page's written epoch already is the
    /// commit's own — yet the commit overwrites what it kept (review 3 #1).
    retain_floor: AtomicU64,
}

/// Group commit with the flush OUTSIDE the store mutex (fastest-engine M1 item 2: gc 389b474b4,
/// with a95fb53ff and r11-churn gc3's fixes 915f8040c..19b8f8a29 re-derived here, ported to the r13
/// composed store): early lock release and flush pipelining, as Aether (Johnson et al., VLDB 2010)
/// and ferrodb's D159 do.
///
/// A fork, a branch commit or a release decides and applies under the store mutex, buffers its
/// records, releases the mutex, and only then waits until its records are durable
/// (`wait_durable`). One waiter at a time LEADS a flight: it takes everything buffered so far under
/// the mutex (`Journal::take_flight`), and writes and syncs it holding no lock at all, so whatever
/// arrives meanwhile buffers behind it and shares the next flight. Every other flush site still
/// flushes under the mutex (`flush_locked`), but first waits out a flight in the air: frames are
/// written in log order, and a later region must never be synced ahead of an earlier one. Every
/// rewrite of the log (snapshot compaction, a catalog checkpoint's cut, a page-size restart) waits
/// the same way, holding the mutex so no new flight is taken.
///
/// What early release must not break, and why it does not:
/// * **Rule 1** (a record is durable only after the slots it names): a commit writes its slots
///   before it buffers its record, and each slot write marks the arena dirty again, so the flight
///   that carries the record syncs the arena before the log.
/// * **Rule 2** (a slot is reused only once the record that freed it is durable): a slot freed by an
///   early-released operation waits in `StoreInner::pending_free` under that operation's log
///   position, and returns to the arena only once a flight has covered it (`mature`). A catalog
///   checkpoint first flushes and matures everything, so its free table lists those slots.
/// * **Acknowledgement**: no caller learns of an operation before its records are durable. A later
///   operation that depends on an earlier one (a commit on a branch whose fork is still in flight,
///   or a trunk commit that retained nothing for a child whose Release is still in flight) is later
///   in the log, and the trunk's barrier waits for everything buffered before its decisions (gc3's
///   N1), so its own durability implies the earlier one's.
/// * **The class**: durability is tracked per class (`GroupState::durable`), so a flight synced in
///   D1 never satisfies a wait that needs D2; such a wait leads an UPGRADE flight, an F_FULLFSYNC of
///   the log, which drains the device's cache of everything synced before it.
/// * **Failure**: a failed flight — or one that could not even be taken — fail-stops the group and
///   the journal in one step, through the flag they share (`StoreInner::fail_stop`; review B-F1),
///   and every waiter gets the error. The in-memory state is then ahead of the disk; the fail-stop rule already governs that
///   (nothing more is written, the next open recovers from disk), and the slots such operations
///   freed are never returned, because no flight will ever cover them.
pub(crate) struct Group {
    state: std::sync::Mutex<GroupState>,
    cv: std::sync::Condvar,
    /// `GroupState::durable`, readable without the group's lock (lead review 1 item 10): written
    /// under it, after the state, by `set_durable`.
    durable_now: [AtomicU64; 3],
    /// The store's fail-stop flag, shared with its journal (`StoreInner::fail_stop`).
    failed: Arc<AtomicBool>,
}

#[derive(Default)]
struct GroupState {
    /// A flight is being written.
    flushing: bool,
    /// `durable[c]`: every journal byte below this log sequence number is durable in class `c` or
    /// a stronger one (indexed `Off`, `Fsync`, `FullFsync`; `Off` means written to the OS).
    durable: [u64; 3],
    /// Every journal byte below this was written by an ORDERED flight (`Flight::ordered`): on the
    /// device ahead of anything written after it, durable once a full flush of that device is.
    ordered: u64,
    /// A trunk commit's WAL F_FULLFSYNC will make the journal durable up to this (lead review 1
    /// item 6): a waiter it covers waits for it rather than lead an upgrade flight, a second
    /// flusher on the same device. Cleared when the WAL is synced, and when the commit's gate
    /// closes whether or not it was.
    pending_full: Option<u64>,
    /// A fuzzy checkpoint is copying the log for its cut, off the store mutex (`begin_cut`): no
    /// group flight starts until its install, so the bytes it copies stay the log's last ones.
    /// (A synchronous flush under the store mutex still may: the install copies what it wrote.)
    cutting: bool,
    /// Observation only (fastest-engine M2): flights led outside the mutex, flushes taken under
    /// it, operations that waited, those already durable when they looked, upgrade flights, and
    /// the waits each flight released.
    flights: u64,
    locked_flushes: u64,
    waits: u64,
    already_durable: u64,
    upgrades: u64,
    riders: u64,
}

fn class_index(class: SyncClass) -> usize {
    match class {
        SyncClass::Off => 0,
        SyncClass::Fsync => 1,
        SyncClass::FullFsync => 2,
    }
}

impl Group {
    fn new(durable: u64, failed: Arc<AtomicBool>) -> Self {
        Self {
            state: std::sync::Mutex::new(GroupState {
                durable: [durable; 3],
                ..GroupState::default()
            }),
            cv: std::sync::Condvar::new(),
            durable_now: [AtomicU64::new(durable), AtomicU64::new(durable), AtomicU64::new(durable)],
            failed,
        }
    }

    /// Every journal byte below `end` is durable in class index `c` (called holding the lock, `g`).
    fn set_durable(&self, g: &mut GroupState, c: usize, end: u64) {
        g.durable[c] = g.durable[c].max(end);
        self.durable_now[c].fetch_max(end, Ordering::AcqRel);
    }

    /// A flight failed, or could not be taken: nothing more becomes durable in this process, and
    /// the journal (which shares the flag) writes nothing more.
    fn poisoned(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Fail-stop the store and wake every waiter (called holding the group's lock, `_g`).
    fn fail(&self, _g: &mut GroupState) {
        self.failed.store(true, Ordering::Release);
        self.cv.notify_all();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GroupState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait<'a>(&self, g: std::sync::MutexGuard<'a, GroupState>) -> std::sync::MutexGuard<'a, GroupState> {
        self.cv.wait(g).unwrap_or_else(|e| e.into_inner())
    }

    /// Wait until no flight is in the air. Called under the store mutex (so none can start); a
    /// flight's leader needs only this group's lock to land, so this cannot deadlock.
    fn quiesce(&self) -> std::sync::MutexGuard<'_, GroupState> {
        let mut g = self.lock();
        while g.flushing {
            g = self.wait(g);
        }
        g
    }

    /// Record a flight's outcome and wake every waiter: on success every byte below `end` is
    /// durable in `class` and every weaker one.
    fn land(&self, end: u64, class: SyncClass, ok: bool) {
        let mut g = self.lock();
        g.flushing = false;
        if ok {
            for c in 0..=class_index(class) {
                self.set_durable(&mut g, c, end);
            }
        } else {
            self.failed.store(true, Ordering::Release);
        }
        self.cv.notify_all();
    }

    fn durable(&self, class: SyncClass) -> u64 {
        self.durable_now[class_index(class)].load(Ordering::Acquire)
    }

    /// An ordered flight's outcome (`BranchStore::order_for_trunk`): on success every byte below
    /// `end` is written and ordered; it is durable only once `trunk_wal_synced` says so.
    fn land_ordered(&self, end: u64, ok: bool) {
        let mut g = self.lock();
        g.flushing = false;
        if ok {
            self.set_durable(&mut g, 0, end);
            g.ordered = g.ordered.max(end);
        } else {
            self.failed.store(true, Ordering::Release);
            g.pending_full = None;
        }
        self.cv.notify_all();
    }

    /// Every byte below `end` is durable in `class` and every weaker one, by a rewrite of the log
    /// made under the store mutex with no flight in the air (a compaction's snapshot, a checkpoint's
    /// catalog commit and cut log), and waiters are woken.
    fn mark_durable(&self, end: u64, class: SyncClass) {
        let mut g = self.lock();
        for c in 0..=class_index(class) {
            self.set_durable(&mut g, c, end);
        }
        self.cv.notify_all();
    }
}

/// A group flight failed: the journal is fail-stopped, and nothing an operation buffered after the
/// last durable flight will become durable in this process.
fn group_poisoned() -> LimboError {
    LimboError::InternalError(
        "branch store is fail-stopped after a failed group flush; reopen the database to recover \
         it from disk"
            .to_string(),
    )
}

/// One histogram of [`super::HoldStats`], kept in atomics so a fork records into it after it has
/// released every lock (fastest-engine M1 item 5; observing only).
struct HoldHist {
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
    buckets: Box<[AtomicU64]>,
}

impl HoldHist {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: (0..super::HOLD_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
        self.buckets[super::hold_bucket(ns)].fetch_add(1, Ordering::Relaxed);
    }

    /// A snapshot. Each field is read on its own, so one taken while forks record can be off by the
    /// forks in flight; read it with the store quiet for an exact one.
    fn stats(&self) -> super::HoldStats {
        super::HoldStats {
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
            buckets: self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).collect(),
        }
    }
}

/// The per-fork hold histograms (see [`super::ForkHolds`]).
struct ForkHoldCounters {
    store: HoldHist,
    wal: HoldHist,
    locked_trunk_forks: AtomicU64,
}

impl ForkHoldCounters {
    fn new() -> Self {
        Self {
            store: HoldHist::new(),
            wal: HoldHist::new(),
            locked_trunk_forks: AtomicU64::new(0),
        }
    }
}

/// A trunk fork's hold of the trunk's WAL write lock, for [`BranchStore::record_fork`]: `ns` 0 and
/// `locked` false for a fork that never took it.
#[derive(Clone, Copy, Default)]
pub(crate) struct WalHold {
    pub(crate) ns: u64,
    pub(crate) locked: bool,
}

thread_local! {
    /// Nanoseconds this thread has held the store mutex through [`BranchStore::lock_counted`] since
    /// the last [`take_counted_hold`] (fastest-engine M1 item 5; observing only). A fork resets it
    /// when it begins and takes it when it ends, so it holds exactly that fork's holds: forks do
    /// not nest, and a thread runs one at a time.
    static COUNTED_HOLD_NS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The store mutex. A thin wrapper: in test builds its guard also marks this thread as holding it,
/// so every sync issued meanwhile is counted (`syncs_under_store_mutex`; lead review 1 item 7's
/// instrument — a flush inside the store mutex stalls every operation behind it).
struct StoreMutex(Mutex<StoreInner>);

impl StoreMutex {
    fn new(inner: StoreInner) -> Self {
        Self(Mutex::new(inner))
    }

    fn lock(&self) -> StoreGuard<'_> {
        let guard = self.0.lock();
        #[cfg(test)]
        STORE_HELD.with(|held| held.set(held.get() + 1));
        StoreGuard(guard)
    }
}

struct StoreGuard<'a>(crate::sync::MutexGuard<'a, StoreInner>);

impl std::ops::Deref for StoreGuard<'_> {
    type Target = StoreInner;
    fn deref(&self) -> &StoreInner {
        &self.0
    }
}

impl std::ops::DerefMut for StoreGuard<'_> {
    fn deref_mut(&mut self) -> &mut StoreInner {
        &mut self.0
    }
}

#[cfg(test)]
impl Drop for StoreGuard<'_> {
    fn drop(&mut self) {
        STORE_HELD.with(|held| held.set(held.get() - 1));
    }
}

#[cfg(test)]
thread_local! {
    /// How many store-mutex guards this thread holds (test builds).
    static STORE_HELD: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Syncs of any file (`io::count_sync`, `io::count_barrier`) issued while the issuing thread held a
/// store mutex (test builds; lead review 1 item 7).
#[cfg(test)]
static SYNCS_UNDER_STORE_MUTEX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A sync was issued: counted if this thread holds a store mutex (test builds).
#[cfg(test)]
pub(crate) fn note_sync() {
    if STORE_HELD.with(|held| held.get()) > 0 {
        SYNCS_UNDER_STORE_MUTEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// See `SYNCS_UNDER_STORE_MUTEX`.
#[cfg(test)]
pub(crate) fn syncs_under_store_mutex() -> u64 {
    SYNCS_UNDER_STORE_MUTEX.load(std::sync::atomic::Ordering::Relaxed)
}

/// The nanoseconds this thread held the store mutex through `lock_counted` since the last call, and
/// start counting again from zero.
pub(crate) fn take_counted_hold() -> u64 {
    COUNTED_HOLD_NS.with(|c| c.replace(0))
}

/// The store mutex's guard on a fork path: the hold, from the moment the lock is granted to the
/// moment the guard is dropped, is added to this thread's counted total. The clock is read once
/// inside the critical section (at the drop), so a counted hold includes one clock read.
struct Counted<G: std::ops::DerefMut<Target = StoreInner>> {
    guard: G,
    since: Instant,
}

impl<G: std::ops::DerefMut<Target = StoreInner>> std::ops::Deref for Counted<G> {
    type Target = StoreInner;
    fn deref(&self) -> &StoreInner {
        &self.guard
    }
}

impl<G: std::ops::DerefMut<Target = StoreInner>> std::ops::DerefMut for Counted<G> {
    fn deref_mut(&mut self) -> &mut StoreInner {
        &mut self.guard
    }
}

impl<G: std::ops::DerefMut<Target = StoreInner>> Drop for Counted<G> {
    fn drop(&mut self) {
        let ns = u64::try_from(self.since.elapsed().as_nanos()).unwrap_or(u64::MAX);
        COUNTED_HOLD_NS.with(|c| c.set(c.get().saturating_add(ns)));
    }
}

struct StoreInner {
    arena: Option<Arena>,
    journal: Option<Journal>,
    /// `Some` for a durable store.
    files: Option<BranchFiles>,
    sync: SyncClass,
    next_id: u64,
    trunk: TrunkState,
    /// F8' (r11-ever's F8 5d580203f as ported in e343173dc; r13-compose step 3): chunks that never
    /// move, so no fork moves the table.
    branches: BranchTable<BranchState>,
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
    /// The F7 SPLICE arm (`DatabaseOpts::with_branch_splice`, r11-ever amendment 15): a released
    /// branch left with one live child is spliced into it. Off, the store keeps the base's rule
    /// (a released branch with a live child is retired and kept), so the base's per-release
    /// attribution holds unedited. A durable store writes and reads only its arm's format version
    /// (`journal::format_version`), so its files cannot be reopened in the other arm.
    splice: bool,
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
    /// F-EXP: the last expiry pass stopped at its bound with more due (in memory, or in the
    /// catalog sweep): `expire_now` runs another.
    expire_more: bool,
    /// githost-shape lane instrument (observing only): see [`super::BranchCatShape`].
    shape: ShapeCounters,
    /// F-W2 (githost-shape lane): every branch that reserved a slot (`first_write_branch`, the one
    /// place a `pending` entry is made) since the last catalog checkpoint pruned this set; a superset
    /// of the branches that hold a reserved slot now. The checkpoint collects reserved slots from
    /// these alone instead of walking every resident state (a dirty list, as ARIES's dirty page
    /// table is for pages).
    pending_holders: HashSet<BranchId>,
    /// F-W3 (githost-shape lane): the most branch states a catalog store keeps resident after a
    /// checkpoint. `None` keeps every state it has touched since the open (the catalog as published).
    resident_cap: Option<usize>,
    /// F-W1 (githost-shape lane; F-cat-snap, r11-diff-list 8c0945ebb / 421e2e125, as r2 ported it in
    /// 8618d2e46): every unreleased branch id, as a persistent set a listing clones in O(1) under the
    /// mutex and walks after releasing it. Built by the first listing of a process (`None` until
    /// then, so an open reads nothing for it), and kept current from then on by every fork and every
    /// release.
    live_ids: Option<IdSet>,
    /// V4 base reads (r11-merge PREREG A20; observing only).
    v4: V4Counters,
    /// V3's row and table stamps (r13-compose, the Merger port), under this one mutex.
    stamps: RowStamps,
    /// The Merger's work since open (observing only).
    merge_work: super::BranchMergeWork,
    /// Slots freed by an early-released operation whose records are not yet durable, under the log
    /// sequence number that makes them free (see [`Group`], rule 2).
    pending_free: VecDeque<(u64, Vec<Slot>)>,
    /// Named server branches' names (fastest-engine M1 item 4).
    names: NameIndex,
    /// The trunk's epoch at the copy decisions of the trunk commit in flight (or the last one): the
    /// Merger stamps that commit's rows with it (`stamp_committed`).
    trunk_commit_epoch: u64,
    /// The store's fail-stop flag, shared by its journal and its group: set once, by whichever fails
    /// first, and read by every write path through `Journal::check_live` (review B-F1).
    fail_stop: Arc<AtomicBool>,
    /// The device the branch log and arena are on (`files_device`), `NO_DEVICE` until both are
    /// open; shared with `BranchStore::files_dev`.
    files_dev: Arc<AtomicU64>,
    /// The log sequence number that makes the newest fork durable: a listing waits for it, so no
    /// branch is listed before its fork is durable (review C-F4).
    last_fork_lsn: u64,
}

/// A branch id as the live set's key. Ids are minted from 1 upward; one past `u32` would need a wider
/// trie, and is refused rather than truncated (githost-shape r2, 8618d2e46).
fn id_key(id: BranchId) -> u32 {
    u32::try_from(id.0)
        .unwrap_or_else(|_| panic!("branch id {} is past the live-id set's u32 keys", id.0))
}

/// githost-shape lane instrument (observing only): the cumulative fields of
/// [`super::BranchCatShape`]. Each is bumped under the store mutex by the call that does the work,
/// from a length that call has in hand; nothing in the mechanism reads them.
#[derive(Default)]
struct ShapeCounters {
    checkpoints: u64,
    checkpoint_ns: u64,
    ckpt_trunk_inserted: u64,
    ckpt_trunk_deleted: u64,
    ckpt_branch_rows: u64,
    ckpt_rows_written: u64,
    ckpt_states_walked: u64,
    ids_calls: u64,
    ids_resident_visited: u64,
    ids_catalog_rows: u64,
    ids_build_rows: u64,
    table_grows: u64,
    table_moved: u64,
    evictions: u64,
    evicted_states: u64,
    /// r13-compose I4: `ensure` calls that loaded at least one state, the states they loaded in
    /// all, and the most loaded by one call.
    ensure_cold: u64,
    ensure_chain_sum: u64,
    ensure_chain_max: u64,
    /// r13-compose I5: evicted states that were the parent of a state still resident after that
    /// eviction (amendment 8).
    evicted_with_resident_descendant: u64,
    /// r13-compose I11: items yielded by walks of the branch table (evictions, live-id builds), and
    /// the slots those walks visited, empty ones included (A2.F5's `walk_slots_scanned`).
    walk_items_yielded: u64,
    walk_slots_scanned: u64,
    /// r13-compose I5's own walk of the survivors of an eviction (an instrument, kept out of I11).
    instrument_walk_items: u64,
    /// C-R's settle on the SHARP path (`settle`): calls that loaded parked branches, the branches
    /// they loaded, and the most in one call (review wf_5c230f31 M: the fuzzy path's settle_batch
    /// counters read 0 in a sharp-only census).
    settle_sharp_calls: u64,
    settle_sharp_loads: u64,
    settle_sharp_max_loads: u64,
}

/// What one trunk write transaction wrote while the trunk had a live child, kept by the writing
/// connection's pager and handed to the store only if the transaction commits: its rows and tables
/// are stamped with the trunk's epoch at the commit (Silo's TIDs are assigned at commit, SOSP 2013),
/// and one that rolls back stamps nothing (r13-compose, the Merger port from b161e861d; the page set
/// and its interior subset are not ported: no registered validator reads them).
#[derive(Default, Debug)]
pub(crate) struct TrunkPending {
    /// Table rows written or deleted, as (table root, rowid).
    pub(crate) rows: HashSet<(i64, i64)>,
    /// Tables written without naming the rows (clear, destroy, incremental blob I/O).
    pub(crate) tables: HashSet<i64>,
}

impl TrunkPending {
    fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.tables.is_empty()
    }
}

/// V3's row and table stamps (r13-compose, the Merger port of b161e861d store.rs:359-418), under
/// the store's one mutex: (table root, rowid) -> the trunk epoch of its last committed write while
/// the trunk had a child. A stamp `e` refuses a branch iff `e > trunk_at`.
///
/// D-M5, the RESTART HORIZON: stamps are in memory only. `horizon` is the trunk's lineage epoch when
/// this process opened the store; a branch forked at or after it (`trunk_at >= horizon`) has every
/// trunk write after its fork stamped, and KeyStamp decides for it. One forked before it may have
/// had a conflicting write before the restart that no stamp records, so KeyStamp gives NO verdict
/// (None) and the Merger decides by its stateless base read (MV4) instead. Without the horizon a
/// restart would let every pre-restart conflict through: a lost update.
#[derive(Default)]
struct RowStamps {
    row_stamps: HashMap<(i64, i64), u64>,
    /// `row_stamps` in stamping order (epochs ascend), for pruning.
    stamp_order: VecDeque<(u64, i64, i64)>,
    /// Tables the trunk wrote without naming the rows: root -> epoch.
    table_stamps: HashMap<i64, u64>,
    horizon: u64,
}

impl RowStamps {
    fn stamp_row(&mut self, root: i64, rowid: i64, epoch: u64) {
        if self.row_stamps.insert((root, rowid), epoch) != Some(epoch) {
            self.stamp_order.push_back((epoch, root, rowid));
        }
    }

    /// Drop every stamp at or below `oldest`, the oldest live trunk child's fork epoch: no live or
    /// future child is refused by it. `None` (no child) drops everything. A stale (smaller)
    /// `oldest` only keeps more.
    fn prune(&mut self, oldest: Option<u64>) {
        let keep = |e: u64| oldest.is_some_and(|o| e > o);
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

/// What the Merger reads of a branch before it derives the branch's writes (r13-compose A5/A6.1).
pub(crate) struct MergeView {
    pub(crate) parent_is_trunk: bool,
    /// The fork epoch at which the branch's ancestry leaves the trunk: the base is the trunk then.
    pub(crate) trunk_at: u64,
    pub(crate) live_children: u64,
    pub(crate) open: bool,
    pub(crate) writer: bool,
    /// O = keys(current) ∪ keys(inherited), ascending (A6.1).
    pub(crate) owned: Vec<u32>,
}

/// A V4 merge's base reads since open: `base_page_into` calls, those answered from the arena (a
/// retained trunk version) and those refused, and the retained versions compared (r11-merge A20).
#[derive(Debug, Default, Clone, Copy)]
struct V4Counters {
    base_reads: u64,
    base_arena: u64,
    base_refused: u64,
    base_examined: u64,
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
    /// The part of `trunk_probes`/`trunk_rows` made by `trunk_written_known`'s once-per-page probe
    /// (r12-composition K8-B instrument; observing only).
    twk_probes: u64,
    twk_rows: u64,
    /// Database page reads made inside those probes, and the same three for `trunk_version_at`'s
    /// probe (r12-composition K8-B amendment 13: T2's reads split by probe kind; observing only).
    twk_reads: u64,
    tva_probes: u64,
    tva_rows: u64,
    tva_reads: u64,
}

/// Catalog checkpoints and C-R settle batches, as counted (r11-restart-r2 instrument, observing
/// only). Times are wall-clock ns.
#[derive(Clone, Copy, Default)]
struct CkptCounters {
    /// Checkpoints installed (fuzzy and sharp).
    count: u64,
    /// r13-compose A4.G: fuzzy checkpoints refused because the store is in the splice arm (G-b).
    /// A FINDING canary in the census, which runs sharp only.
    fuzzy_refused_splice: u64,
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

/// How long a fork's creator, or an open of a new branch, waits for the trunk commit it forks after
/// to be published before it is `Busy` (`BranchStore::wait_trunk_commit_published`).
const PUBLISH_WAIT: Duration = Duration::from_secs(60);

/// What a trunk fork's registration did (F-L on the durable store; see `BranchStore::fork_trunk`).
pub(crate) enum TrunkFork {
    /// Registered and applied; durable once `lsn` is, and, when `after` is set, readable once the
    /// trunk commit holding the gate at that count is published. The creator waits for both with
    /// every lock released (`wait_trunk_commit_published`, then `wait_durable`), and so does any
    /// other opener of the branch (`BranchStore::settle`; review A-F3).
    Forked { id: BranchId, lsn: u64, after: Option<u64> },
    /// The trunk has no live child, so this fork would be the first: fork under the WAL write lock.
    NeedsWriterLock,
}

/// F-FZ hook stages (`BranchStore::checkpoint_hold`): the writer has written the captured rows and
/// not committed; or it has committed and the install has not run.
pub(crate) const HOLD_BEFORE_COMMIT: u8 = 2;
pub(crate) const HOLD_AFTER_COMMIT: u8 = 3;

/// Or-ed into the hook's stage once the flight has arrived there (tests wait for it).
pub(crate) const HOLD_ARRIVED: u8 = 0x80;

/// fastest-engine (test hook `BranchStore::trunk_commit_hold`): a trunk commit waits here, its copy
/// decisions taken and its commit gate OPEN, before its barrier and its first frame.
#[cfg(test)]
pub(crate) const HOLD_TRUNK_DECIDED: u8 = 4;

/// fastest-engine (test hook `BranchStore::trunk_commit_hold`, same atomic): a group flight's leader
/// waits here, its flight taken from the buffer and not yet written, so a test can act while an
/// operation's records are in the air.
#[cfg(test)]
pub(crate) const HOLD_FLIGHT_TAKEN: u8 = 5;

/// fastest-engine (test hook `BranchStore::trunk_commit_hold`, same atomic): a trunk commit waits
/// here, its pre-image barrier done and its commit gate open, before its first frame.
#[cfg(test)]
pub(crate) const HOLD_TRUNK_BARRIER_DONE: u8 = 6;

/// fastest-engine (test hook `BranchStore::trunk_commit_hold`, same atomic): a lock-free trunk
/// fork waits here, its read snapshot taken, before it registers.
#[cfg(test)]
pub(crate) const HOLD_FORK_REGISTERING: u8 = 7;

/// How long a waiter an ordered flight covers waits for the trunk commit's WAL flush before it
/// leads an upgrade flight of its own (liveness, should that commit stall: its caller may be the
/// thread that drives it).
const PENDING_FULL_WAIT: Duration = Duration::from_millis(100);

/// `files_dev` before the branch files are open, or when their device could not be read.
const NO_DEVICE: u64 = u64::MAX;

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

/// F-FZ's arm switch (lead decision 27960c3d) for the store's MODEL tests, which open without a
/// database (`BranchStore::open_mode`): sharp unless `R11_CKPT=fuzzy`. A database's store resolves
/// its own mode (`BranchCheckpoint::resolve`, fuzzy by default: lead review 1 item 7). (`compact_now`,
/// and the tests' and harness's `checkpoint_fuzzy_now`, choose their own path whatever it says.)
#[cfg(test)]
fn fuzzy_checkpoints() -> bool {
    static FUZZY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FUZZY.get_or_init(|| std::env::var("R11_CKPT").is_ok_and(|v| v == "fuzzy"))
}

impl CatState {
    fn new(catalog: Catalog, sync: SyncClass, generation: u64) -> Result<Self> {
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
            twk_probes: 0,
            twk_rows: 0,
            twk_reads: 0,
            tva_probes: 0,
            tva_rows: 0,
            tva_reads: 0,
        })
    }
}

/// What of a branch's catalog state a checkpoint must rewrite.
const DIRTY_ROW: u8 = 1;
const DIRTY_CUR: u8 = 2;
const DIRTY_RET: u8 = 4;
/// Forked since the last checkpoint: no catalog row yet, so everything is written.
const DIRTY_NEW: u8 = 8;
/// Spliced into its parent's place since the last checkpoint: its (parent, fork epoch) moved, so
/// the row is re-keyed and its `branch_children` entry with it (F7 durable port, U6).
const DIRTY_KEY: u8 = 16;

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
    /// Released, and the `Release` record is durable or in the air (early release): `collect` may
    /// free it, and what it frees waits for the record's flight (`pending_free`, rule 2).
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

/// A commit of branch `id` was applied in memory and its flight failed (`BranchState::in_doubt`).
fn in_doubt(id: BranchId) -> LimboError {
    LimboError::InternalError(format!(
        "branch {} is in doubt: a commit of it failed to become durable after it was applied, so \
         the store is fail-stopped and this branch is read again only after the database is \
         reopened, which recovers it from disk",
        id.0
    ))
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
    /// The branch's current version of every page it has committed, or reads through a spliced-out
    /// ancestor (F7 durable port).
    current: HashMap<u32, Owned>,
    /// `current` as `(born, page)`, so the versions born after a given epoch are a range (retire).
    current_by_born: BTreeSet<(u64, u32)>,
    /// Slots reserved by the open write transaction's copy decisions, not yet published.
    pending: HashMap<u32, Slot>,
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
    /// The fork epoch at which `inherited` was taken: the fork itself, or, after a snapshot load or a
    /// catalog load, the fork epoch `inherited` was derived at. A splice changes `fork_epoch` and
    /// leaves this and `inherited` alone. Every current version born at or below it is one a splice
    /// moved in from a zombie, which `inherited` already names with the same slot and crc (the
    /// zombie's view at this epoch held the version the child read through it); every other
    /// current version is the branch's own, born above it (epoch inheritance). So `view_now` needs
    /// only the versions born above it.
    inherited_at: u64,
    /// `inherited` plus this branch's current pages, for its children to inherit. Built at the
    /// branch's first fork and kept current by its commits from then on; `None` until it forks, and
    /// again once it is retired (a released branch takes no child).
    view: Option<PageMap>,
    /// A named server branch's name (fastest-engine M1 item 4); taken at its release, which frees it.
    name: Option<Arc<str>>,
    /// The log sequence number that makes this branch's fork durable; 0 for a branch loaded from
    /// disk, which is durable already (`BranchStore::settle`; review C-F4).
    fork_lsn: u64,
    /// A commit of this branch was applied and its flight failed, so the caller got an error for a
    /// write the store holds in memory: nothing reads the branch again in this process (lead
    /// review 1 item 11). Only ever set on a fail-stopped store.
    in_doubt: bool,
}

/// Named server branches' names (fastest-engine M1 item 4), under the store mutex. An eager store
/// holds every unreleased named branch in `map`. A catalog store holds there the names of resident
/// states and every name created since its last checkpoint (`fresh`, written at the next), and
/// masks with `gone` the catalog rows of names released since (deleted at the next); every other
/// name is read from the catalog's `branch_name` table.
///
/// A checkpoint's capture COPIES `fresh` and `gone`, and its install removes only the entries that
/// are unchanged since (review C-F1, C-F2): until the catalog holds what the capture wrote, the
/// index still masks and still names what it did, and a capture that is never written (a write
/// that failed, a thread that did not start) leaves nothing to restore.
#[derive(Default)]
struct NameIndex {
    map: HashMap<Arc<str>, BranchId>,
    filter: NameFilter,
    fresh: HashMap<Arc<str>, BranchId>,
    /// Each released name, with the release count at its release: an install drops it only if no
    /// later release of the same name moved it.
    gone: HashMap<Arc<str>, u64>,
    releases: u64,
}

/// A catalog store's name filter (lead review 1 item 2): a keyed hash of every name the store ever
/// held, so a create of a NEW name — every successful server create — is answered "free" without a
/// catalog query under the store mutex. Insert-only: a released name stays in it, and costs one
/// query if it is ever looked up again; a hash collision costs the same. Built off the mutex after
/// open from the catalog's names (`BranchStore::start_name_filter`) plus every name applied while
/// the build ran (`pending`); until then, lookups query the catalog as before. Empty and built at
/// once for a store with no catalog yet.
#[derive(Default)]
struct NameFilter {
    hasher: std::collections::hash_map::RandomState,
    built: Option<HashSet<u64, BuildIdHasher>>,
    pending: Option<HashSet<u64, BuildIdHasher>>,
}

impl NameFilter {
    fn hash(&self, name: &str) -> u64 {
        use std::hash::BuildHasher;
        self.hasher.hash_one(name)
    }

    /// `name` is held by a branch from now on.
    fn note(&mut self, name: &str) {
        if self.built.is_none() && self.pending.is_none() {
            return;
        }
        let h = self.hash(name);
        if let Some(built) = self.built.as_mut() {
            built.insert(h);
        }
        if let Some(pending) = self.pending.as_mut() {
            pending.insert(h);
        }
    }

    /// No branch ever held `name` (`false` while the filter is not built: ask the catalog).
    fn says_absent(&self, name: &str) -> bool {
        // fastest-engine mutant `filter_says_absent` (test builds only).
        if fe_mutant("filter_says_absent") {
            return self.built.is_some();
        }
        self.built.as_ref().is_some_and(|built| !built.contains(&self.hash(name)))
    }
}

/// The name filter's keys are keyed hashes already: hashed again as themselves.
#[derive(Default, Clone, Copy)]
struct IdHasher(u64);

impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}

type BuildIdHasher = std::hash::BuildHasherDefault<IdHasher>;

/// The names a branch may have: 1 to 255 bytes, no NUL (fastest-engine M1 item 4). A server's
/// connection syntax may narrow this; the store refuses nothing narrower.
fn check_branch_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 255 || name.contains('\0') {
        return Err(LimboError::InvalidArgument(format!(
            "branch name {name:?}: a name is 1 to 255 bytes with no NUL"
        )));
    }
    Ok(())
}

impl BranchState {
    /// The map a child forked now inherits, built from `inherited` and `current` the first time,
    /// from the current versions born after `inherited_at` only (see there). Iterating all of
    /// `current` made the first fork after a splice cost every page the dead levels above had moved
    /// into it: Theta(d^2) over a chain that writes a new page, forks and releases its parent at
    /// each of d levels (r11-adversarial's chainw, against the volatile store's F7). Returns the map
    /// and the entries this call inserted (0 when the map was already built; observation only).
    fn view_now(&mut self) -> (&PageMap, u64) {
        let mut built = 0u64;
        let (inherited, current, by_born, at) =
            (&self.inherited, &self.current, &self.current_by_born, self.inherited_at);
        let view = self.view.get_or_insert_with(|| {
            let mut view = inherited.clone();
            for &(_, page) in by_born.range((at.saturating_add(1), 0)..) {
                let owned = current[&page];
                view.insert(page, (owned.slot, owned.crc));
                built += 1;
            }
            view
        });
        (&*view, built)
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

    /// F4 (as the F7 durable port keeps it). A released branch never reads its own `current` again,
    /// never writes, and takes no new child, so a current version is read only by the live children
    /// forked at or after its birth. Every one born after the newest live child's fork is read by
    /// nobody and goes to `freed` NOW rather than when the last child goes — the interval rule with
    /// the free epoch set to the release epoch (ferrodb's `retire_arenas_by_rule`); the rest stay
    /// current. `collect` runs this again whenever a child of a released branch goes, which frees
    /// what the newest child alone read; `Lineage::child_gone` frees the retained versions. Cost:
    /// one child-index query and the versions freed. Only for a branch with no open connection: an
    /// open one still reads `current` at `u64::MAX`.
    fn retire_current(
        &mut self,
        id: BranchId,
        children: &ChildIndex,
        cat: Option<&mut Catalog>,
        freed: &mut Vec<Slot>,
    ) -> Result<()> {
        // Its children keep their own `inherited`; a released branch forks no new one.
        self.view = None;
        let unread = match children.below(cat, id, u64::MAX)? {
            Some(newest) => Bound::Excluded((newest, u32::MAX)),
            None => Bound::Unbounded,
        };
        let dead: Vec<(u64, u32)> = self
            .current_by_born
            .range((unread, Bound::Unbounded))
            .copied()
            .collect();
        for (born, page) in dead {
            self.current_by_born.remove(&(born, page));
            let owned = self.current.remove(&page).expect("indexed current version");
            freed.push(owned.slot);
        }
        Ok(())
    }

    /// Set `page`'s current version, keeping `current_by_born` in step; returns the one it replaces.
    fn set_current(&mut self, page: u32, owned: Owned) -> Option<Owned> {
        let old = self.current.insert(page, owned);
        if let Some(old) = old {
            self.current_by_born.remove(&(old.born, page));
        }
        self.current_by_born.insert((owned.born, page));
        old
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
    /// (parent, fork epoch) -> (nearest live sibling below, above) when it was removed. Ordered, so
    /// that a spliced-out parent's links are one range (`relink`).
    removed: BTreeMap<(u64, u64), (Option<u64>, Option<u64>)>,
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

    /// The fork epoch of `parent`'s oldest live child. (Also the Merger's stamp prune reads it:
    /// r13-compose A2.R1; the copy step 6 ported for the build before F7's merge is dropped here, so
    /// one copy remains.)
    fn lowest(&self, cat: Option<&mut Catalog>, parent: BranchId) -> Result<Option<u64>> {
        let p = parent.0;
        let mem = self.map.range((p, 0)..=(p, u64::MAX)).next().map(|(&(_, e), _)| e);
        let Some(cat) = cat else {
            return Ok(mem);
        };
        let row = cat.children_in(p, 0, u64::MAX, 1)?.into_iter().next();
        Ok(match (mem, self.resolve(p, row, false)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        })
    }

    /// The live child `parent` forked at `f`: the in-memory entry if there is one (a splice since
    /// the last checkpoint re-keyed it here, while the catalog still lists the spliced-out parent
    /// under this key until that checkpoint), else the catalog's.
    fn child_at(&self, cat: Option<&mut Catalog>, parent: BranchId, f: u64) -> Result<Option<BranchId>> {
        if let Some(&id) = self.map.get(&(parent.0, f)) {
            return Ok(Some(id));
        }
        match cat {
            Some(cat) if !self.removed.contains_key(&(parent.0, f)) => {
                Ok(cat.child_at(parent.0, f)?.map(BranchId))
            }
            _ => Ok(None),
        }
    }

    /// U6: `child`, forked by `zombie` at `f`, takes the zombie's place as `parent`'s child at `zf`.
    /// The zombie's key is REPLACED, never removed, so no removal link names a key a live child
    /// holds; the zombie's own links (children of it removed since the last checkpoint) go with it,
    /// since no query names it as a parent again.
    fn relink(&mut self, zombie: BranchId, f: u64, parent: BranchId, zf: u64, child: BranchId) {
        self.map.remove(&(zombie.0, f));
        self.map.insert((parent.0, zf), child);
        let links: Vec<(u64, u64)> = self
            .removed
            .range((zombie.0, 0)..=(zombie.0, u64::MAX))
            .map(|(&k, _)| k)
            .collect();
        for k in links {
            self.removed.remove(&k);
        }
        crate::turso_assert!(
            !self.removed.contains_key(&(parent.0, zf)),
            "a removal link names the key a spliced child now holds"
        );
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
        // U5 (r11-invariant-matrix): a splice can retain a version OLDER than the ones already
        // kept, so both neighbours are checked, not only the last: the predecessor must die by
        // `v.born`, the successor be born at `v.died` or later (the volatile store's F7 check).
        crate::turso_assert!(
            v.born < v.died
                && versions
                    .range(..=v.born)
                    .next_back()
                    .is_none_or(|(_, prev)| prev.died <= v.born && prev.born != v.born)
                && versions
                    .range(v.born..)
                    .next()
                    .is_none_or(|(_, next)| v.died <= next.born),
            "a retained version overlaps another of the same page; the born-ordered lookup would \
             return the wrong one"
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
    /// Slots freed by early-released operations whose records were not yet durable at the
    /// capture (`StoreInner::pending_free`, all at or below `deferred_lsn`): the capture's state
    /// does not name them, so the catalog lists them free, and its commit makes their releases
    /// durable. They stay out of the allocator until the install takes them out of memory; a
    /// failed write leaves them waiting for their flight (lead review 1 item 7).
    deferred: Vec<Slot>,
    /// The journal's sequence number at the capture, its `Record::Checkpoint` included: every
    /// record the capture covers lies below it. A fuzzy checkpoint commits only once all of it is
    /// durable in `log_class` (review 4 #1); the deferred frees mature at it.
    deferred_lsn: u64,
    /// The journal's own class: what an operation the capture covers was acknowledged in.
    log_class: SyncClass,
    /// The store's fail-stop flag (shared with its journal and group): read just before the catalog
    /// commit, and set when the checkpoint's arena sync or its commit fails (review 4 #1).
    fail_stop: Arc<AtomicBool>,
    meta: Meta,
    /// `ChildIndex` keys (children forked, and removal links) as of the capture.
    child_keys: Vec<(u64, u64)>,
    child_removed: Vec<(u64, u64)>,
    /// A handle on the arena file: synced, in `arena_sync`, before the catalog names the slots.
    arena: Option<std::fs::File>,
    arena_sync: SyncClass,
    /// fastest-engine M1 item 4: names released since the last checkpoint (rows deleted first),
    /// then names created since (rows written), as swapped out of the index at the capture.
    names_gone: HashMap<Arc<str>, u64>,
    names_fresh: HashMap<Arc<str>, BranchId>,
    lease_now: u64,
    fail_after_commit: bool,
    /// BranchFailpoint::CheckpointWriteFails, taken at the capture: the write fails before its
    /// catalog commit, on the sharp path and on a fuzzy flight alike (D-T2's failed-write order).
    fail_write: bool,
    /// BranchFailpoint::ArenaSyncFails, taken at a capture that syncs the arena: that sync fails.
    fail_arena_sync: bool,
    /// githost-shape instrument (observing only; r13-compose S-1): when the capture began, and the
    /// rows, trunk versions inserted and trunk versions deleted it captured. Counted at the install.
    shape_started: Instant,
    shape_rows: u64,
    shape_trunk_new: u64,
    shape_trunk_gone: u64,
    /// Catalog rows the writer connection wrote for this capture, set by the caller of
    /// `checkpoint_write` (the writer, not `cat.catalog`, writes every checkpoint: S-1).
    shape_rows_written: u64,
}

/// F-FZ phase 2: write a capture into the catalog through `catalog` (the writer connection) in ONE
/// transaction. Holds no store lock: nothing here reads the store.
fn checkpoint_write(catalog: &mut Catalog, cap: &Captured, hold: Option<&AtomicU8>) -> Result<()> {
    if cap.fail_write {
        return Err(LimboError::InternalError(
            "failpoint: the catalog checkpoint's write failed before its commit".to_string(),
        ));
    }
    // Every slot the catalog is about to name reaches the device before its commit, whose flush
    // makes them durable with it (ruling 85a032f01: a plain fsync). A failed sync fail-stops the
    // store (review 3 #5): a later sync of the same file may report success for pages it lost.
    if let Some(file) = cap.arena.as_ref() {
        if cap.arena_sync.syncs() {
            let synced = if cap.fail_arena_sync {
                Err(LimboError::InternalError(
                    "failpoint: the checkpoint's arena sync failed".to_string(),
                ))
            } else {
                super::journal::fsync_file(file, SyncClass::Fsync)
            };
            if let Err(e) = synced {
                // Mutant `checkpoint_sync_error_kept` (test builds only): as before, the store
                // goes on.
                if !fe_mutant("checkpoint_sync_error_kept") {
                    cap.fail_stop.store(true, Ordering::Release);
                }
                return Err(e);
            }
        }
    }
    // And the commit as durable as any record it replaces (review B-F3).
    catalog.raise_sync(cap.arena_sync)?;
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
            // F7's U6 (r13-compose S-5): a spliced child's row and children-index entry move with
            // its new key. F7 wrote this in the base's write loop, which F-FZ replaced by this one.
            if what & DIRTY_KEY != 0 && !mutant("r13_no_dirty_key") {
                catalog.rekey(b)?;
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
        // fastest-engine M1 item 4: freed names first, so a name released and created again since
        // the last checkpoint ends up naming its new branch.
        for name in cap.names_gone.keys() {
            catalog.name_del(name)?;
        }
        for (name, id) in &cap.names_fresh {
            catalog.name_put(name, id.0)?;
        }
        if let Some(cursor) = cap.free_cursor {
            catalog.free_delete_upto(cursor)?;
        }
        for &slot in &cap.taken {
            catalog.free_delete(slot)?;
        }
        for &slot in cap.free_list.iter().chain(cap.reserved.iter()).chain(cap.deferred.iter()) {
            catalog.free_put(slot)?;
        }
        catalog.put_meta(&cap.meta)
    })()
    .and_then(|()| {
        pause_at(hold, HOLD_BEFORE_COMMIT);
        // A store fail-stopped since the capture commits nothing more (review 4 #1); checked
        // last, so no failure before the commit is missed. Mutant `commit_poisoned` (test builds
        // only): it commits regardless.
        if cap.fail_stop.load(Ordering::Acquire) && !fe_mutant("commit_poisoned") {
            return Err(group_poisoned());
        }
        // A failed commit may or may not have reached the catalog: from here on, only a reopen
        // can tell, so the store fail-stops.
        catalog.commit().inspect_err(|_| cap.fail_stop.store(true, Ordering::Release))
    });
    if let Err(e) = written {
        catalog.rollback();
        return Err(e);
    }
    Ok(())
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

/// F-FZ: the body of a fuzzy checkpoint's thread. Phase 2 holds only the writer; phase 3 only the
/// store mutex; phase 4 only the writer again. No path takes the writer while waiting for the store
/// mutex, and the sharp path (which takes the store mutex, then the writer) refuses while a capture
/// is in flight, so the two cannot deadlock.
#[allow(clippy::too_many_arguments)]
fn run_flight(
    inner: Arc<StoreMutex>,
    group: Arc<Group>,
    writer: Arc<Mutex<Catalog>>,
    cap: Box<Captured>,
    hold: Arc<AtomicU8>,
    over_hard: Arc<AtomicBool>,
    truncating: Arc<AtomicBool>,
    installs: Arc<(std::sync::Mutex<u64>, std::sync::Condvar)>,
) {
    let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let t = Instant::now();
    // A panic in the writer must still reach the install (with an error), or `flight` would stay
    // set: no checkpoint would start again and the read snapshot would stay pinned.
    let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Everything the capture covers is durable before the catalog restates it (review 4 #1):
        // a record still buffered or in the air at the capture belongs to an operation whose
        // caller may yet be told it failed, and then the catalog must not hold it. Waited for
        // holding no lock, but the store mutex for the moment it takes to lead a flight if none
        // is up. Mutant `commit_unsettled` (test builds only): the catalog commits at once.
        if !fe_mutant("commit_unsettled") {
            let settled =
                BranchStore::wait_durable_on(&inner, &group, None, cap.deferred_lsn, cap.log_class);
            if let Err(e) = settled {
                return (Err(e), 0);
            }
        }
        let mut w = writer.lock();
        let r0 = w.counters.rows_written;
        let written = checkpoint_write(&mut w, &cap, Some(&*hold));
        if written.is_err() {
            w.rollback();
        }
        (written, w.counters.rows_written - r0)
    }))
    .unwrap_or_else(|_| {
        writer.lock().rollback();
        (
            Err(LimboError::InternalError(
                "the branch catalog checkpoint's writer panicked".to_string(),
            )),
            0,
        )
    });
    let (written, rows_written) = written;
    let mut cap = cap;
    cap.shape_rows_written = rows_written;
    let write_ns = ns(t);
    if written.is_ok() {
        pause_at(Some(&*hold), HOLD_AFTER_COMMIT);
    }
    // The cut's copy and sync, holding no lock (lead review 1 item 7): what it keeps of the log is
    // fixed while it runs (no group flight starts), and the install below only renames it in.
    let cut = written.as_ref().ok().and_then(|()| begin_cut(&inner, &group, &cap));
    let installed = {
        let mut guard = inner.lock();
        // The install cuts the log: no group flight may be writing it (fastest-engine M1 item 2),
        // and none starts while this holds the store mutex.
        drop(group.quiesce());
        let t = Instant::now();
        let installed = guard.checkpoint_install(cap, written, cut.as_ref().and_then(|c| c.take()));
        if installed.is_ok() {
            if let Some(journal) = guard.journal.as_ref() {
                group.mark_durable(journal.lsn() - journal.pending_len(), journal.sync_class());
            }
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
    // Flights go on, into the new log.
    drop(cut);
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

/// A cut being prepared (`begin_cut`): holds the group's `cutting` until dropped, so no group flight
/// writes the log while its kept part is copied, however the checkpoint ends.
struct CutGate<'a> {
    group: &'a Group,
    prep: std::cell::Cell<Option<super::journal::CutPrep>>,
}

impl CutGate<'_> {
    /// The prepared cut, for the install (once).
    fn take(&self) -> Option<super::journal::CutPrep> {
        self.prep.take()
    }
}

impl Drop for CutGate<'_> {
    fn drop(&mut self) {
        let mut g = self.group.lock();
        g.cutting = false;
        self.group.cv.notify_all();
    }
}

/// A fuzzy checkpoint's cut, after its catalog commit and before its install, holding no lock but
/// briefly the store mutex (lead review 1 item 7, review 2 #5): wait — not holding the mutex — for
/// no flight to be in the air; then stop group flights (`cutting`) and copy the kept part of the
/// log into the temp log and sync it (`Journal::prepare_cut`). Everything the capture covers was
/// flown before the commit (`run_flight`), so the copy starts at a written position. `None`
/// (nothing prepared, the install cuts under the mutex as before): a poisoned store, a failed copy,
/// or a log that changed under the capture.
fn begin_cut<'a>(inner: &StoreMutex, group: &'a Group, cap: &Captured) -> Option<CutGate<'a>> {
    loop {
        let src = {
            let guard = inner.lock();
            let g = group.lock();
            if group.poisoned() {
                return None;
            }
            if g.flushing || g.cutting {
                drop(guard);
                drop(group.wait(g));
                continue;
            }
            // Only under mutant `commit_unsettled` is any of it still buffered.
            if guard.journal.as_ref()?.log_len() < cap.log_from {
                return None;
            }
            let src = match guard.journal.as_ref()?.cut_source(cap.log_from) {
                Ok(Some(src)) => src,
                _ => return None,
            };
            let mut g = g;
            g.cutting = true;
            src
        };
        let gate = CutGate {
            group,
            prep: std::cell::Cell::new(None),
        };
        match Journal::prepare_cut(src, cap.log_from, cap.generation) {
            Ok(prep) => gate.prep.set(Some(prep)),
            Err(e) => {
                tracing::warn!("branch log cut not prepared off the store mutex: {e}");
                return None;
            }
        }
        return Some(gate);
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

/// r13-compose's runtime ablation knobs (PREREG A2.0 step 7: I1, I2, I10): `R13_FW1=off` lists by the
/// scan and catalog query under the mutex at every listing (the store before F-W1), `R13_FW2=off`
/// makes a checkpoint walk every resident state for reserved slots (before F-W2), and
/// `R13_MERGER=off` turns the Merger off (it refuses every merge) and with it the trunk's row
/// stamping and its prune (A3.F17). Each is read once per process; unset, every fix is on.
pub(crate) fn knob_off(name: &str) -> bool {
    static KNOBS: std::sync::OnceLock<[bool; 3]> = std::sync::OnceLock::new();
    let k = KNOBS.get_or_init(|| {
        let off = |v: &str| std::env::var(v).is_ok_and(|x| x == "off");
        [off("R13_FW1"), off("R13_FW2"), off("R13_MERGER")]
    });
    match name {
        "fw1" => k[0],
        "fw2" => k[1],
        "merger" => k[2],
        _ => panic!("unknown r13 knob {name}"),
    }
}

/// r13-compose I9: the bytes of one branch state, and of one branch-table slot.
pub(crate) fn state_sizes() -> (usize, usize) {
    (
        std::mem::size_of::<BranchState>(),
        std::mem::size_of::<Option<(BranchId, BranchState)>>(),
    )
}

/// r13-compose I12: with `R13_VICTIM_LOG=<path>`, each evicting checkpoint appends one line, the
/// checkpoint count then and its victims' ids in eviction order (observing only; A3.F14's
/// determinism check and A3.L1's attribution read it).
fn victim_log(checkpoint: u64, victims: &[BranchId]) {
    static PATH: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let Some(path) = PATH.get_or_init(|| std::env::var("R13_VICTIM_LOG").ok()).as_deref() else {
        return;
    };
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let ids: Vec<String> = victims.iter().map(|v| v.0.to_string()).collect();
        let _ = writeln!(f, "{checkpoint} {}", ids.join(","));
    }
}

/// A4.G's guard mutants: `R13_MUTANT=r13_guard_<a|b|c>_debug` compiles that guard behind
/// `cfg!(debug_assertions)`, as a `debug_assert!` would be: in a RELEASE test build the guard is gone
/// and its red must fail. In a debug build the mutant changes nothing.
fn guard_mutant(which: &str) -> bool {
    !cfg!(debug_assertions) && mutant(&format!("r13_guard_{which}_debug"))
}

#[cfg(test)]
thread_local! {
    /// A4.G's G-b red: captures entered on this thread (a guard that stopped before the capture
    /// leaves it unchanged).
    pub(crate) static CAPTURE_ENTERED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// r13-compose's registered mutants (PREREG §5, A6): `R13_MUTANT` names one deliberate defect, so
/// each red test can be shown to fail on its mutant from the same test binary. Unset, every mutant is
/// off. Read once per process.
pub(crate) fn mutant(name: &str) -> bool {
    static ON: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    ON.get_or_init(|| std::env::var("R13_MUTANT").ok()).as_deref() == Some(name)
}

/// fastest-engine's registered mutants (PREREG v1 §8, amendment 36): `FE_MUTANT` names one
/// deliberate defect, so each red test can be shown to fail on its mutant from the same test
/// binary. TEST BUILDS ONLY: a production binary has no mutant to switch on. Read once per process.
pub(crate) fn fe_mutant(name: &str) -> bool {
    #[cfg(test)]
    {
        static ON: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
        ON.get_or_init(|| std::env::var("FE_MUTANT").ok()).as_deref() == Some(name)
    }
    #[cfg(not(test))]
    {
        let _ = name;
        false
    }
}

/// fastest-engine C1 (the SIGKILL crash harness, PREREG v1 §8): a named code point a crash test
/// can aim a kill at. TEST BUILDS ONLY, and inert unless `FE_KILL_AT=<point>:<n>` is set: the n-th
/// time this process reaches `point`, it appends `KILL-AT <point> <n>` to the file `FE_CRASH_LOG`
/// names (the harness's own record, read after the kill) and SIGKILLs itself on the spot, which no
/// destructor, flush or unwinding outlives.
#[inline]
pub(crate) fn kill_point(name: &'static str) {
    #[cfg(all(test, unix))]
    kill_point_aimed(name);
    #[cfg(not(all(test, unix)))]
    let _ = name;
}

#[cfg(all(test, unix))]
fn kill_point_aimed(name: &str) {
    static AIM: std::sync::OnceLock<Option<(String, u64)>> = std::sync::OnceLock::new();
    static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let aim = AIM.get_or_init(|| {
        let v = std::env::var("FE_KILL_AT").ok()?;
        let (point, n) = v.rsplit_once(':')?;
        Some((point.to_string(), n.parse().ok()?))
    });
    let Some((point, n)) = aim.as_ref() else {
        return;
    };
    if point != name {
        return;
    }
    if HITS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == *n {
        // `FE_KILL_DELAY_MS`: the other threads run on this long first (an acknowledgement a
        // waiter was just given reaches the log before the kill).
        if let Some(ms) = std::env::var("FE_KILL_DELAY_MS").ok().and_then(|v| v.parse().ok()) {
            std::thread::sleep(Duration::from_millis(ms));
        }
        crash_log(&format!("KILL-AT {name} {n}"));
        // SAFETY: signals this process; nothing after it runs.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGKILL);
        }
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}

/// Append one line to the file `FE_CRASH_LOG` names, with one `write(2)` (O_APPEND), so it is in
/// the file before the caller's next step, whatever kill follows. Test builds only.
#[cfg(all(test, unix))]
pub(crate) fn crash_log(line: &str) {
    use std::io::Write as _;
    static LOG: std::sync::OnceLock<Option<std::sync::Mutex<std::fs::File>>> =
        std::sync::OnceLock::new();
    let log = LOG.get_or_init(|| {
        let path = std::env::var_os("FE_CRASH_LOG")?;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()
            .map(std::sync::Mutex::new)
    });
    if let Some(log) = log {
        let mut f = log.lock().unwrap_or_else(|e| e.into_inner());
        let _ = f.write_all(format!("{line}\n").as_bytes());
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
    ///   is refused here (`Pager::copy_on_write_decision`, and `begin_trunk_commit` again).
    ///
    /// Whether a second PROCESS may read the trunk beside a writer is Turso's own rule, unchanged.
    ///
    /// Durable + read-only with NO branch files stays refused: a fork would create them, and
    /// nothing below the connection refuses a fork on a read-only database.
    #[cfg(test)]
    pub(crate) fn open_with_flags(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        splice: bool,
        db_path: &str,
        read_only: bool,
    ) -> Result<Self> {
        Self::open_with_checkpoint(durability, default_lease, splice, None, db_path, read_only)
    }

    /// `open_with_flags`, with the checkpoint mode the database's options ask for (`None`: the
    /// default, see `BranchCheckpoint::resolve`).
    pub(crate) fn open_with_checkpoint(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        splice: bool,
        checkpoint: Option<super::BranchCheckpoint>,
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
        let fuzzy = super::BranchCheckpoint::resolve(checkpoint, splice) == super::BranchCheckpoint::Fuzzy;
        Self::open_resolved(durability, default_lease, splice, fuzzy, db_path)
    }

    fn trunk_only() -> Self {
        Self {
            inner: Arc::new(StoreMutex::new(StoreInner::fresh(None, SyncClass::Off, None))),
            prewarm: PrewarmStats::default(),
            flights: Mutex::new(Vec::new()),
            name_filter_build: Mutex::new(None),
            name_filter_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            flight_hold: Arc::new(AtomicU8::new(0)),
            over_hard: Arc::new(AtomicBool::new(false)),
            truncating: Arc::new(AtomicBool::new(false)),
            installs: Arc::new((std::sync::Mutex::new(0), std::sync::Condvar::new())),
            trunk_children: AtomicUsize::new(0),
            trunk_commits: AtomicU64::new(0),
            gate_closed: (std::sync::Mutex::new(()), std::sync::Condvar::new()),
            unsynced: AtomicBool::new(false),
            leases_outstanding: AtomicBool::new(false),
            trunk_only: true,
            open_stats: BranchOpenStats::default(),
            resolve_calls: AtomicU64::new(0),
            arena_reads: AtomicU64::new(0),
            holds: ForkHoldCounters::new(),
            group: Arc::new(Group::new(0, Arc::new(AtomicBool::new(false)))),
            class: SyncClass::Off,
            #[cfg(test)]
            trunk_commit_hold: Arc::new(AtomicU8::new(0)),
            #[cfg(test)]
            publish_wait_ms: AtomicU64::new(0),
            trunk_same_device: AtomicBool::new(false),
            files_dev: Arc::new(AtomicU64::new(NO_DEVICE)),
            barrier_locks: AtomicU64::new(0),
            last_release_lsn: AtomicU64::new(0),
            retain_floor: AtomicU64::new(0),
            fuzzy: false,
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

    /// `open_mode` in the default arm: the tests' shorthand (the database opens through
    /// `open_with_flags`, in the arm its options name).
    #[cfg(test)]
    pub(crate) fn open(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        db_path: &str,
    ) -> Result<Self> {
        Self::open_mode(durability, default_lease, false, db_path)
    }

    /// `open_resolved` as the store's model tests open it: checkpoints sharp, the base's way, unless
    /// `R11_CKPT=fuzzy` (their own arm switch, F-FZ's lead decision 27960c3d).
    #[cfg(test)]
    pub(crate) fn open_mode(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        splice: bool,
        db_path: &str,
    ) -> Result<Self> {
        Self::open_resolved(durability, default_lease, splice, fuzzy_checkpoints(), db_path)
    }

    /// The store for a database whose sidecar files are named from `db_path`, in the F7 splice arm
    /// (`splice`) or not. A durable store recovers whatever its files hold, if they were written in
    /// the same arm; a volatile one refuses a database whose files say it has durable branches,
    /// because opened volatile, the trunk's writes would skip the pre-image barrier and silently
    /// change what those branches read.
    pub(crate) fn open_resolved(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        splice: bool,
        fuzzy: bool,
        db_path: &str,
    ) -> Result<Self> {
        let format = super::journal::format_version(splice);
        // S-12 (r13-compose, A4.G: stands): the splice arm and fuzzy checkpoints are refused
        // together at construction, as fl_refusal does for its switch pairs (8976dc691): a splice's
        // relink during a flight is untested. G-a..G-c refuse the same state on every other path.
        // (The splice arm resolves to sharp unless fuzzy is asked for.)
        if splice && fuzzy && !matches!(durability, BranchDurability::Volatile) {
            return Err(LimboError::InvalidArgument(
                "the F7 splice arm and fuzzy checkpoints are refused together (r13-compose S-12)"
                    .to_string(),
            ));
        }
        let memory = crate::is_memory_like(db_path);
        let opened = Instant::now();
        let mut stats = BranchOpenStats::default();
        let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        // r12-catload: the prewarm (`R12_PREWARM`, off when unset). The arena is warmed here, before
        // recovery reads anything; the catalog by `recover_catalog`, on its own handle.
        let (warm, targets) = if matches!(durability, BranchDurability::Volatile) {
            (Prewarm::Off, Targets { catalog: false, arena: false })
        } else {
            prewarm::from_env()?
        };
        let mut warmed = PrewarmStats {
            mode: warm,
            ..PrewarmStats::default()
        };
        if targets.arena && !memory {
            prewarm::warm_files(&[BranchFiles::for_db(db_path).arena.as_path()], warm, &mut warmed)?;
        }
        let inner = match durability {
            BranchDurability::Volatile => {
                if !memory && BranchFiles::for_db(db_path).exist() {
                    return Err(LimboError::InvalidArgument(format!(
                        "{db_path} has durable branches; open it with branch durability \
                         (DatabaseOpts::with_branch_durability), or its trunk writes would \
                         silently change what those branches read"
                    )));
                }
                let mut inner = StoreInner::fresh(None, SyncClass::Off, default_lease);
                inner.splice = splice;
                inner
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
                inner.splice = splice;
                if files.exist() {
                    let t = Instant::now();
                    let recovered = Journal::recover_as(&files, sync, format)?;
                    stats.recover_ns = ns(t);
                    if let Some(mut recovered) = recovered {
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
                            kill_point("recover.replay");
                            inner.replay(record, &mut ignored)?;
                        }
                        stats.replay_ns = ns(t);
                        // A snapshot or a `ReleaseOpen` can hold a released branch that was kept
                        // only by an open connection; after a restart nothing is open. Each close is
                        // logged (flushed below, once the arena is open).
                        let t = Instant::now();
                        let (scanned, closed) =
                            inner.close_held(&mut recovered.journal, &mut ignored)?;
                        stats.released_scanned = scanned;
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
                        inner.files_device(recovered.journal.device(), arena.device())?;
                        let mut arena = arena;
                        if closed {
                            recovered.journal.flush(&mut arena)?;
                        }
                        inner.arena = Some(arena);
                        let mut journal = recovered.journal;
                        journal.share_fail_stop(&inner.fail_stop);
                        inner.journal = Some(journal);
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
                inner.splice = splice;
                if files.exist() {
                    Self::recover_catalog(&mut inner, &files, sync, &mut stats, (warm, targets))?;
                }
                inner
            }
        };
        // Every byte the open buffered was flushed by the recovery itself (single-threaded).
        let opened_lsn = inner.journal.as_ref().map_or(0, |j| j.lsn() - j.pending_len());
        let mut store = Self {
            fuzzy,
            group: Arc::new(Group::new(opened_lsn, inner.fail_stop.clone())),
            class: inner.sync,
            #[cfg(test)]
            trunk_commit_hold: Arc::new(AtomicU8::new(0)),
            #[cfg(test)]
            publish_wait_ms: AtomicU64::new(0),
            trunk_same_device: AtomicBool::new(false),
            files_dev: inner.files_dev.clone(),
            barrier_locks: AtomicU64::new(0),
            last_release_lsn: AtomicU64::new(0),
            retain_floor: AtomicU64::new(0),
            trunk_children: AtomicUsize::new(inner.trunk.lineage.n_children as usize),
            trunk_commits: AtomicU64::new(0),
            gate_closed: (std::sync::Mutex::new(()), std::sync::Condvar::new()),
            inner: Arc::new(StoreMutex::new(inner)),
            prewarm: warmed,
            flights: Mutex::new(Vec::new()),
            name_filter_build: Mutex::new(None),
            name_filter_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
            holds: ForkHoldCounters::new(),
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
        // D-M5: every trunk write from here on is stamped; a branch forked before this epoch is
        // validated by the base read, never by stamps a restart lost.
        {
            let mut inner = store.inner.lock();
            inner.stamps.horizon = inner.trunk.lineage.epoch;
        }
        store.start_name_filter();
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
        sync: SyncClass,
        stats: &mut BranchOpenStats,
        (warm, targets): (Prewarm, Targets),
    ) -> Result<()> {
        let ns = |t: Instant| u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let t = Instant::now();
        let mut catalog = Catalog::open(&files.cat, sync)?;
        let meta = catalog.meta()?;
        stats.catalog_ns = ns(t);
        // The catalog's own format key (in the meta row this open reads anyway), checked before
        // anything else is read: the log header's version is checked too, but a torn header has
        // none, and the catalog alone would then open in the other splice arm, or under another
        // build's format (r13-compose S-6: 5/6 composed, 7 before F7's merge, 3/4 F7-durable alone,
        // 2 the base, 0 before the key). A refused open pays no catalog prewarm.
        let format = super::journal::format_version(inner.splice);
        if let Some(m) = meta.filter(|m| m.format != format) {
            return Err(LimboError::Corrupt(format!(
                "branch catalog {}: format version {}; this store reads version {format}{}",
                files.cat.display(),
                m.format,
                if m.format == 0 {
                    " (0 was written before the catalog carried the key)".to_string()
                } else {
                    super::journal::version_hint(m.format, format)
                }
            )));
        }
        // A catalog that names branch state is never opened over a missing log or arena (review 2
        // #12): it would open with every branch's slots gone, and say nothing.
        if let Some(m) = meta.filter(|m| m.in_use > 0 || m.states > 0) {
            for (what, path) in [("log", &files.log), ("arena", &files.arena)] {
                if !path.exists() {
                    return Err(LimboError::Corrupt(format!(
                        "branch catalog {} names {} branch states, but the branch {what} {} is \
                         missing: the branch files were moved apart; put all three back together \
                         ({}, {}, {})",
                        files.cat.display(),
                        m.states,
                        path.display(),
                        files.log.display(),
                        files.arena.display(),
                        files.cat.display()
                    )));
                }
            }
        }
        // r12-catload: the catalog's prewarm, before the replay reads: its pages (`interior`,
        // `buffer`), or its files unless `R12_PREWARM_FILES` leaves the catalog out.
        if !warm.warms_files() || targets.catalog {
            catalog.prewarm(&files.cat, warm)?;
        }
        let t = Instant::now();
        let recovered = Journal::recover_catalog_as(
            files,
            sync,
            meta.map(|m| (m.page_size, m.generation)),
            format,
        )?;
        stats.recover_ns = ns(t);
        let Some(mut recovered) = recovered else {
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
            kill_point("recover.replay");
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
        // Branches a crash left held (catalog rows `released = 2`, or a replayed `ReleaseOpen`) are
        // closed and collected, each close logged (flushed below, once the arena is open).
        let (scanned, closed) = inner.close_held(&mut recovered.journal, &mut freed)?;
        stats.released_scanned = scanned;
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
        let mut arena = Arena::open_file_catalog(&files.arena, page_size, high_water, in_use, free_mem)?;
        inner.files_device(recovered.journal.device(), arena.device())?;
        if closed {
            recovered.journal.flush(&mut arena)?;
        }
        inner.arena = Some(arena);
        let mut journal = recovered.journal;
        journal.share_fail_stop(&inner.fail_stop);
        inner.journal = Some(journal);
        stats.arena_ns = ns(t);
        Ok(())
    }

    /// Stamp a committed trunk transaction's rows and tables with the trunk's epoch. Called by the
    /// committing pager while its commit gate is still open and it still holds the WAL write lock:
    /// no fork registers inside an open gate (F-L, `begin_trunk_commit`), so the epoch is the one
    /// the commit's copy decisions were taken at, and every merge validation takes the WAL write
    /// lock, so none lands between the commit and its stamps. Returns whether anything was stamped
    /// (then the caller prunes).
    pub(crate) fn stamp_committed(&self, tx: TrunkPending) -> bool {
        if tx.is_empty() || knob_off("merger") {
            return false;
        }
        let mut inner = self.inner.lock();
        // The epoch this commit's copy decisions were taken at (a fork registered inside its gate
        // since then forks AFTER it, and took a later epoch).
        let epoch = inner.trunk_commit_epoch;
        let st = &mut inner.stamps;
        for (root, rowid) in tx.rows {
            st.stamp_row(root, rowid, epoch);
        }
        for root in tx.tables {
            st.table_stamps.insert(root, epoch);
        }
        inner.merge_work.stamp_commits += 1;
        inner.merge_work.stamps_held = inner.stamps.row_stamps.len() as u64;
        inner.merge_work.stamp_entries = inner.stamps.stamp_order.len() as u64;
        true
    }

    /// Prune the stamps below the oldest live trunk child (A2.R1): `ChildIndex::lowest` merges the
    /// in-memory index with the catalog, so a child forked before the last checkpoint (and so gone
    /// from the in-memory map) still counts. Reading the map alone would find a child that is too
    /// young and prune stamps an older child still needs: a lost update. A catalog error keeps every
    /// stamp (keeping more is safe).
    pub(crate) fn prune_stamps(&self) {
        let mut inner = self.inner.lock();
        let StoreInner { children, cat, stamps, merge_work, .. } = &mut *inner;
        let catalog = cat.as_mut().map(|c| &mut c.catalog);
        let oldest = if mutant("r13_prune_map_only") {
            children.lowest(None, BranchId::TRUNK)
        } else {
            children.lowest(catalog, BranchId::TRUNK)
        };
        match oldest {
            Ok(oldest) => {
                stamps.prune(oldest);
                merge_work.stamp_prunes += 1;
            }
            Err(e) => tracing::warn!("branch stamps not pruned (kept): {e}"),
        }
        merge_work.stamps_held = stamps.row_stamps.len() as u64;
        merge_work.stamp_entries = stamps.stamp_order.len() as u64;
    }

    /// KeyStamp's verdict (V3, with D-M5's horizon): `Some(true)` if the trunk wrote any of `keys`
    /// (or wrote one of `roots` without naming rows) after `trunk_at`, or the transaction in
    /// progress (`pending`) did; `Some(false)` if not; `None` if `trunk_at` is before this process's
    /// horizon, so the stamps cannot answer.
    pub(crate) fn keystamp_verdict(
        &self,
        trunk_at: u64,
        keys: &[(i64, i64)],
        roots: &[i64],
        pending: &TrunkPending,
    ) -> Option<bool> {
        let mut inner = self.inner.lock();
        if trunk_at < inner.stamps.horizon && !mutant("r13_no_horizon") {
            inner.merge_work.v3_horizon_fallbacks += 1;
            return None;
        }
        let st = &inner.stamps;
        let stamped = |e: Option<&u64>| e.is_some_and(|&e| e > trunk_at);
        let conflict = keys.iter().any(|k| stamped(st.row_stamps.get(k)) || pending.rows.contains(k))
            || roots
                .iter()
                .any(|r| stamped(st.table_stamps.get(r)) || pending.tables.contains(r));
        Some(conflict)
    }

    /// The Merger's view of branch `id` (r13-compose A5/A6.1): made resident first (`ensure`
    /// applies its parked Commits, D-M9's rule), then its scope facts and its owned page set.
    pub(crate) fn merge_view(&self, id: BranchId) -> Result<MergeView> {
        let mut inner = self.inner.lock();
        if !inner.ensure(id)? {
            return Err(gone(id));
        }
        let st = inner.branches.get(&id).ok_or_else(|| gone(id))?;
        let mut owned: BTreeSet<u32> = st.current.keys().copied().collect();
        if !mutant("r13_owned_current_only") {
            owned.extend(st.inherited.pages());
        }
        Ok(MergeView {
            parent_is_trunk: st.parent.is_trunk(),
            trunk_at: st.trunk_at,
            live_children: st.lineage.n_children,
            open: st.open,
            writer: st.writer,
            owned: owned.into_iter().collect(),
        })
    }

    /// The Merger's counters since open, with the stamps held now.
    pub(crate) fn merge_work(&self) -> super::BranchMergeWork {
        let inner = self.inner.lock();
        let mut w = inner.merge_work;
        w.stamps_held = inner.stamps.row_stamps.len() as u64;
        w.stamp_entries = inner.stamps.stamp_order.len() as u64;
        w
    }

    /// r13-compose I13 (A4.X3): a RESIDENT state's distinct pages over `current` ∪ its retained
    /// versions (observing only). A child loaded under this state takes one inherited-map entry per
    /// such page that has a version at its fork, so for a stack level read when its child forked,
    /// this is the child's cold-load `derived_inserts`. `None` when the state is not resident.
    pub(crate) fn state_pages(&self, id: BranchId) -> Option<u64> {
        let inner = self.inner.lock();
        let st = inner.branches.get(&id)?;
        let mut pages: BTreeSet<u32> = st.current.keys().copied().collect();
        pages.extend(st.lineage.retained.keys().copied());
        Some(pages.len() as u64)
    }

    /// Add to the Merger's counters.
    pub(crate) fn merge_counted(&self, f: impl FnOnce(&mut super::BranchMergeWork)) {
        f(&mut self.inner.lock().merge_work);
    }

    /// A trunk-only store answers yes, so every trunk page write reaches the pager's capture and
    /// its refusal (`Pager::copy_on_write_decision`).
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
    /// them, holding the store mutex (`flush_locked`). A no-op when volatile.
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
            // A Release flushed here, in the store's class, must still be covered by the next trunk
            // commit's barrier in the trunk's (skill review 2 #3).
            // Mutant `log_all_no_floor` (test builds only): as at 5f7f68120, it does not.
            if records
                .iter()
                .any(|r| matches!(r, Record::Release { .. } | Record::ReleaseOpen { .. }))
                && !fe_mutant("log_all_no_floor")
            {
                self.last_release_lsn.fetch_max(journal.lsn(), Ordering::AcqRel);
            }
        }
        self.flush_locked(inner, SyncClass::Off)?;
        inner.lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Buffer `records` for an early-released operation and return the log sequence number the
    /// caller must wait for (`wait_durable`) once it has released every lock, before acknowledging
    /// the operation. Nothing is flushed here; 0 when volatile (always durable).
    fn buffer_records(&self, inner: &mut StoreInner, records: &[Record]) -> Result<u64> {
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
            // Fails inside the flight's write, after this operation is applied.
            journal.fail_next_write();
        }
        if *failpoint == Some(BranchFailpoint::GroupFlightTakeFails) {
            *failpoint = None;
            // Fails as the flight is taken, after this operation is applied.
            journal.fail_next_take();
        }
        for record in records {
            journal.buffer(record)?;
        }
        Ok(journal.lsn())
    }

    /// The flight of everything buffered (`Journal::take_flight`); `None` when volatile.
    fn take_flight(inner: &mut StoreInner, class: SyncClass, upgrade: bool) -> Result<Option<Flight>> {
        let (Some(journal), Some(arena)) = (inner.journal.as_mut(), inner.arena.as_mut()) else {
            return Ok(None);
        };
        journal.take_flight(arena, class, upgrade).map(Some)
    }

    /// Flush everything buffered, under the store mutex the caller holds, in at least `class`. A
    /// flight in the air goes first (its frames precede these in the log); its leader needs only
    /// the group's lock to land, never the store mutex, so waiting for it here cannot deadlock.
    fn flush_locked(&self, inner: &mut StoreInner, class: SyncClass) -> Result<()> {
        let mut g = self.group.quiesce();
        if self.group.poisoned() {
            return Err(group_poisoned());
        }
        let flight = match Self::take_flight(inner, class, false) {
            Ok(Some(flight)) if !flight.is_empty() => flight,
            Ok(_) => return Ok(()),
            Err(e) => {
                self.group.fail(&mut g);
                return Err(e);
            }
        };
        g.flushing = true;
        g.locked_flushes += 1;
        drop(g);
        let (end, class) = (flight.end_lsn, flight.class);
        let written = flight.write();
        if written.is_err() {
            if let Some(journal) = inner.journal.as_mut() {
                journal.poison();
            }
        }
        self.group.land(end, class, written.is_ok());
        written?;
        self.mature(inner);
        Ok(())
    }

    /// Wait until the journal's first `lsn` bytes are durable in `class`, leading a flight when none
    /// is in the air: an ordinary one if anything is buffered, an UPGRADE one if the bytes were made
    /// durable only in a weaker class (see [`Group`]). Called WITHOUT the store mutex.
    pub(crate) fn wait_durable(&self, lsn: u64, class: SyncClass) -> Result<()> {
        #[cfg(test)]
        let hold = Some(&*self.trunk_commit_hold);
        #[cfg(not(test))]
        let hold = None;
        Self::wait_durable_on(&self.inner, &self.group, hold, lsn, class)
    }

    /// `wait_durable` for a caller holding only the store's shared parts: a fuzzy checkpoint's
    /// thread (`run_flight`), which waits for what its capture covers before its catalog commit.
    /// `hold` is the test hook a leader pauses at once its flight is taken.
    fn wait_durable_on(
        store: &StoreMutex,
        group: &Group,
        hold: Option<&AtomicU8>,
        lsn: u64,
        class: SyncClass,
    ) -> Result<()> {
        #[cfg(not(test))]
        let _ = hold;
        // Mutant M-c (PREREG v1 amendment 36): the operation is acknowledged before its records
        // are even written.
        if lsn == 0 || fe_mutant("ack_before_pwrite") {
            return Ok(());
        }
        let need = class_index(class);
        let mut first = true;
        loop {
            {
                let mut g = group.lock();
                if first {
                    g.waits += 1;
                }
                loop {
                    if g.durable[need] >= lsn {
                        if first {
                            g.already_durable += 1;
                        }
                        return Ok(());
                    }
                    if group.poisoned() {
                        return Err(group_poisoned());
                    }
                    // An ordered flight carries these bytes and a trunk commit's WAL flush is
                    // about to make them durable (lead review 1 item 6): waited for, briefly,
                    // rather than led past with an upgrade flight, a second flusher.
                    if need > 0 && g.pending_full.is_some_and(|p| p >= lsn) {
                        first = false;
                        let (woken, timeout) = group
                            .cv
                            .wait_timeout(g, PENDING_FULL_WAIT)
                            .unwrap_or_else(|e| e.into_inner());
                        g = woken;
                        if timeout.timed_out()
                            && g.pending_full.is_some_and(|p| p >= lsn)
                            && !g.flushing
                            && !g.cutting
                        {
                            break;
                        }
                        continue;
                    }
                    if !g.flushing && !g.cutting {
                        break;
                    }
                    first = false;
                    g = group.wait(g);
                }
            }
            first = false;
            // Lead: take what is buffered under the store mutex, then write it holding nothing.
            let flight = {
                let mut inner = Counted {
                    guard: store.lock(),
                    since: Instant::now(),
                };
                let mut g = group.lock();
                if g.durable[need] >= lsn {
                    return Ok(());
                }
                if group.poisoned() {
                    return Err(group_poisoned());
                }
                if g.flushing || g.cutting {
                    continue;
                }
                // Written already in a weaker class: only an upgrade can make it durable in `class`.
                let upgrade = g.durable[0] >= lsn;
                match Self::take_flight(&mut inner, class, upgrade) {
                    Ok(Some(flight)) if !flight.is_empty() => {
                        g.flushing = true;
                        g.flights += 1;
                        if upgrade && flight.len() == 0 {
                            g.upgrades += 1;
                        }
                        flight
                    }
                    // Our frames left the buffer, yet no flight covered them and none is in the
                    // air: only a failed flush does that, and it poisons the group.
                    Ok(_) => return Err(group_poisoned()),
                    Err(e) => {
                        group.fail(&mut g);
                        return Err(e);
                    }
                }
            };
            kill_point("flight.taken");
            #[cfg(test)]
            pause_at(hold, HOLD_FLIGHT_TAKEN);
            let (end, flight_class) = (flight.end_lsn, flight.class);
            // Mutant M-b (PREREG v1 amendment 36): the waiters are acknowledged after the pwrite
            // and before the sync. Caught by C1b and V2, not by SIGKILL.
            let written = if fe_mutant("ack_before_sync") {
                flight.write_with(|| group.mark_durable(end, flight_class))
            } else {
                flight.write()
            };
            if written.is_err() {
                // Fail-stops the group and the journal in one step (they share the flag), and
                // wakes the waiters, some of whom hold the store mutex while they wait.
                group.land(end, flight_class, false);
                return written;
            }
            group.land(end, flight_class, true);
            kill_point("flight.landed");
        }
    }

    /// Return the deferred frees a flight has covered (called under the store mutex).
    fn mature(&self, inner: &mut StoreInner) {
        if inner.pending_free.is_empty() {
            return;
        }
        let durable = self.group.durable(SyncClass::Off);
        inner.mature_frees(durable);
    }

    /// Make everything buffered durable and every deferred free mature, under the store mutex:
    /// before a catalog checkpoint's capture, so the free table it writes lists every slot the
    /// state no longer names (catalog mode has no reachability sweep at open).
    fn settle_durable(&self, inner: &mut StoreInner) -> Result<()> {
        self.flush_locked(inner, SyncClass::Off)?;
        let durable = self.group.durable(SyncClass::Off);
        inner.mature_frees(durable);
        crate::turso_assert!(
            inner.pending_free.is_empty() || inner.journal.as_ref().is_some_and(Journal::is_poisoned),
            "deferred frees remain after everything buffered was made durable"
        );
        Ok(())
    }

    /// The group's counters (fastest-engine M2; observing only): `[flights led outside the mutex,
    /// locked flushes, waits, waits already durable, upgrade flights]`.
    pub(crate) fn group_counters(&self) -> [u64; 5] {
        let g = self.group.lock();
        [g.flights, g.locked_flushes, g.waits, g.already_durable, g.upgrades]
    }

    /// Grant or extend `id`'s lease to `ttl` past the lease clock's now. A deadline only moves
    /// forward (Chubby §2.8: the master "is free to advance this timeout further into the future,
    /// but may not move it backwards in time").
    pub(crate) fn set_lease(&self, id: BranchId, ttl: Duration) -> Result<()> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
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
        let mut inner = self.inner.lock();
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
        let now = inner.lease.now_ms();
        inner.expire_more = false;
        // A fail-stopped store cannot make a Release durable, so it reaps nothing (and frees
        // nothing); reads stay available and the next open recovers from disk.
        if inner.poisoned() {
            return Ok((Expired::default(), now));
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
            return Ok((Expired::default(), now));
        }
        let mut by_depth = Vec::with_capacity(due.len());
        for id in due {
            by_depth.push((std::cmp::Reverse(inner.depth(id)?), id));
        }
        by_depth.sort();
        let due: Vec<BranchId> = by_depth.into_iter().map(|(_, id)| id).collect();
        let mut records: Vec<Record> = due.iter().map(|&id| inner.release_record(id)).collect();
        records.push(Record::Clock { now_ms: now });
        inner.lease.queued(now);
        self.log_all(inner, records)?;
        let mut freed = Vec::new();
        for &id in &due {
            if let Err(e) = inner.apply_release(id, &mut freed) {
                return Err(inner.fatal(e));
            }
        }
        let freed_pages = freed.len();
        inner.release_slots(freed);
        self.sync_trunk_children(inner);
        self.sync_lease_flag(inner);
        self.maybe_compact(inner);
        Ok((
            Expired {
                reaped: due,
                freed_pages,
            },
            now,
        ))
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

    /// Append `record` and make it durable before the caller acts on it, holding the store mutex
    /// (`flush_locked`). A no-op when volatile.
    fn log(&self, inner: &mut StoreInner, record: Record) -> Result<()> {
        self.log_all(inner, vec![record])
    }

    /// Compact the log into a snapshot if it has outgrown the live state. Best effort: the
    /// operation that triggered it is already durable, and a failure before the rename leaves the
    /// log intact; a failure after it fail-stops the journal (see `Journal::compact`).
    ///
    /// With `R11_CKPT=fuzzy`, a catalog store checkpoints FUZZILY instead (F-FZ): C-R's parked Commits
    /// first, a bounded batch per call; then a capture under this mutex, and the write on a thread of
    /// its own.
    fn maybe_compact(&self, inner: &mut StoreInner) {
        if !inner.journal.as_ref().is_some_and(|j| j.wants_compaction()) {
            return;
        }
        if inner.cat.is_none() || !self.fuzzy {
            if let Err(e) = self.compact(inner, false) {
                tracing::warn!("branch store compaction failed: {e}");
                if let Some(journal) = inner.journal.as_mut() {
                    journal.defer_compaction();
                }
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
        if inner.cat.as_ref().is_none_or(|c| c.flight)
            || inner.poisoned()
            || self.truncating.load(Ordering::Acquire)
        {
            return false;
        }
        // G-b (r13-compose A4.G): every fuzzy path reaches here; a splice-arm store is refused and
        // counted (a nonzero count in a census run is a FINDING: the census runs sharp only).
        if inner.splice && !guard_mutant("b") {
            if let Some(cat) = inner.cat.as_mut() {
                cat.ckpt.fuzzy_refused_splice += 1;
            }
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
        // Nothing is flushed for the capture (lead review 1 item 7): what is buffered is in it, and
        // the catalog's commit makes it durable; the frees still waiting for a flight are listed
        // free in the catalog and taken out of memory only at the install (`Captured::deferred`).
        // Mutant `settle_at_capture` (test builds only): the flush under the mutex, as before.
        if fe_mutant("settle_at_capture") {
            if let Err(e) = self.settle_durable(inner) {
                tracing::warn!("branch catalog checkpoint not started: {e}");
                return false;
            }
        }
        self.mature(inner);
        let t = Instant::now();
        let q0 = inner.cat.as_ref().map_or(0, |c| c.catalog.counters.queries);
        let flight_in_air = self.group.lock().flushing;
        let cap = match inner.checkpoint_capture_mode(false, true, flight_in_air) {
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
        let dirty = cap.dirty.clone();
        let shared = self.inner.clone();
        let group = self.group.clone();
        let hold = self.flight_hold.clone();
        let over_hard = self.over_hard.clone();
        let truncating = self.truncating.clone();
        let installs = self.installs.clone();
        let mut flights = self.flights.lock();
        if flights.len() >= FLIGHTS_KEPT {
            // Past its install long ago (only one checkpoint is ever in flight), so this join
            // waits for nothing.
            let _ = flights.remove(0).join();
        }
        let spawned = crate::thread::Builder::new()
            .name("branch-checkpoint".to_string())
            .spawn(move || {
                run_flight(shared, group, writer, cap, hold, over_hard, truncating, installs)
            });
        match spawned {
            Ok(handle) => {
                flights.push(handle);
                true
            }
            Err(e) => {
                // Nothing was written: undo the capture (the thread's closure, and the capture
                // with it, are gone).
                tracing::warn!("branch catalog checkpoint thread not started: {e}");
                let cat = inner.cat.as_mut().expect("captured above");
                cat.catalog.end_read_snapshot();
                cat.flight = false;
                for (id, what) in dirty {
                    *cat.dirty.entry(id).or_insert(0) |= what;
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
        if inner.cat.is_some() {
            // No flight is in the air while it cuts the log, and none can start (this holds the
            // store mutex). What is buffered, and the frees waiting for it, the capture takes
            // (`Captured::deferred`).
            self.mature(inner);
            drop(self.group.quiesce());
            inner.checkpoint_catalog(fail_after_rename)?;
            if let Some(journal) = inner.journal.as_ref() {
                // The catalog holds what preceded the capture; the cut log holds, synced, what
                // followed it in the file.
                self.group
                    .mark_durable(journal.lsn() - journal.pending_len(), journal.sync_class());
            }
            self.unsynced.store(false, Ordering::Release);
            return Ok(());
        }
        // No flight may be writing the log this truncates. The snapshot carries every applied
        // operation, the early-released ones still buffered included, so once it is durable so are
        // they.
        drop(self.group.quiesce());
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
        self.group.mark_durable(journal.lsn(), journal.sync_class());
        // The snapshot carries the clock, and it replaced every buffered stamp.
        lease.queued(snapshot.lease_now_ms);
        lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// The commit gate's count, for a lock-free trunk fork to read before it takes its WAL read
    /// snapshot and hand back to [`Self::fork_trunk`]. Odd while a trunk commit is between its copy
    /// decisions and its publication.
    pub(crate) fn trunk_commit_seq(&self) -> u64 {
        self.trunk_commits.load(Ordering::Acquire)
    }

    /// Wait until the trunk commit that holds the gate at the odd count `c` has closed it (its
    /// frames are published, or it failed). Every commit closes its gate when its writer releases
    /// the WAL write lock, so this waits for one commit's write and sync. Past `PUBLISH_WAIT` it is
    /// `Busy`, as the WAL write lock's holder makes a fork of the base busy: a writer that never
    /// publishes is not waited on for ever.
    pub(crate) fn wait_trunk_commit_published(&self, c: u64) -> Result<()> {
        let (lock, closed) = &self.gate_closed;
        let started = Instant::now();
        #[cfg(test)]
        let limit = match self.publish_wait_ms.load(Ordering::Acquire) {
            0 => PUBLISH_WAIT,
            ms => Duration::from_millis(ms),
        };
        #[cfg(not(test))]
        let limit = PUBLISH_WAIT;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        while self.trunk_commits.load(Ordering::Acquire) <= c {
            if started.elapsed() > limit {
                return Err(LimboError::Busy);
            }
            guard = closed
                .wait_timeout(guard, Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        Ok(())
    }

    /// Shorten the publication wait (`wait_trunk_commit_published`), for a test of its timeout.
    #[cfg(test)]
    pub(crate) fn set_publish_wait(&self, wait: Duration) {
        self.publish_wait_ms
            .store(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX).max(1), Ordering::Release);
    }

    /// Fork a child of the trunk.
    ///
    /// `seen: None`: the caller holds the trunk's WAL write lock, so no trunk write transaction is
    /// in flight, and its snapshot (the latest commit) gives the child its schema.
    ///
    /// `seen: Some(c)`: the caller holds a WAL READ snapshot taken after it read `c` from
    /// [`Self::trunk_commit_seq`], and no WAL write lock. The fork registers at once (F-L
    /// 573642f19 on the durable store, with no retry): every trunk commit decided after the
    /// registration sees the child and retains what it reads (`begin_trunk_commit` takes the same
    /// mutex to decide). If no commit was decided since `c`, every commit decided before is in the
    /// caller's snapshot, and the child takes its schema. If one was — published since, or still
    /// in flight with the gate open — the child forks AFTER it: it reads the trunk's current pages,
    /// which that commit wrote, so its schema is read from its own pages at its first connection
    /// (`schema: None`; the snapshot's cookie predates the commit), and a commit still in flight is
    /// recorded as the one it forks after: nothing reads the child — not its creator, not a
    /// connection that finds it by name — until that commit is published (`settle`, at every open;
    /// review A-F3), so nobody reads it while the commit is half there. The fork itself waits for
    /// no trunk commit and never retries; it is never the trunk's first child (a writer that saw no
    /// child captured no pre-image, so the first child is forked under the writer's lock:
    /// `NeedsWriterLock`). The check, the registration and the children count move together in one
    /// store-mutex hold.
    pub(crate) fn fork_trunk(
        &self,
        schema: Arc<Schema>,
        page_size: usize,
        seen: Option<u64>,
        name: Option<&str>,
    ) -> Result<TrunkFork> {
        if let Some(name) = name {
            check_branch_name(name)?;
        }
        #[cfg(test)]
        if seen.is_some() {
            pause_at(Some(&*self.trunk_commit_hold), HOLD_FORK_REGISTERING);
        }
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock_counted();
        self.mature(&mut inner);
        let restart = inner.arena.as_ref().is_some_and(|a| a.page_size() != page_size);
        if restart {
            // `ensure_backing` may start an empty store over (a compaction or checkpoint that
            // rewrites the log): no flight may be writing it. None can start while this holds the
            // mutex.
            drop(self.group.quiesce());
        }
        inner.ensure_backing(page_size)?;
        if restart {
            // It did (it refuses otherwise): the empty state supersedes everything buffered.
            if let Some(journal) = inner.journal.as_ref() {
                self.group.mark_durable(journal.lsn(), journal.sync_class());
            }
        }
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        // After the expiry pass, which can reap the trunk's last child: a lock-free fork must never
        // be the first one.
        let mut schema = Some(schema);
        let mut after = None;
        // A fork under the WAL write lock moves the gate's count (lead review 1 item 10): a commit
        // made while the trunk had no child opened no gate, so a lock-free fork whose snapshot
        // predates it learns of it here — it registers only once this child exists. Mutant
        // `no_locked_fork_bump` (test builds only).
        if seen.is_none() && !fe_mutant("no_locked_fork_bump") {
            self.trunk_commits.fetch_add(2, Ordering::AcqRel);
        }
        if let Some(seen) = seen {
            if inner.trunk.lineage.n_children == 0 {
                return Ok(TrunkFork::NeedsWriterLock);
            }
            let now = self.trunk_commits.load(Ordering::Acquire);
            // Mutant M-f (PREREG v1 amendment 36): the gate admits a fork past a trunk commit it did
            // not see, with the snapshot's schema and no wait for the commit's publication.
            if (now != seen || now % 2 == 1) && !fe_mutant("gate_admits_inflight") {
                inner.work.trunk_forks_after_commit += 1;
                schema = None;
                if now % 2 == 1 {
                    after = Some(now);
                }
            }
        }
        // The name's uniqueness is decided in the same store-mutex hold that buffers its record.
        if let Some(name) = name {
            if inner.name_lookup(name)?.is_some() {
                return Err(name_taken(name));
            }
        }
        let id = BranchId(inner.next_id);
        let (records, lease) = inner.fork_records(id, BranchId::TRUNK, now, name);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        // Early release (fastest-engine M1 item 2): buffered, applied, and made durable by the
        // caller's `wait_durable` once it holds no lock.
        let lsn = self.buffer_records(&mut inner, &records)?;
        let handle = if name.is_some() { Handle::Detached } else { Handle::Attached };
        if let Err(e) = inner.apply_fork(BranchId::TRUNK, id, schema, handle, name.map(Arc::from)) {
            return Err(inner.fatal(e));
        }
        inner.apply_fork_lease(id, lease);
        inner.note_fork_lsn(id, lsn);
        self.sync_trunk_children(&inner);
        kill_point("fork.applied");
        let work = &mut inner.work;
        work.trunk_forks += 1;
        if seen.is_some() {
            work.trunk_forks_fast += 1;
        } else {
            work.trunk_forks_locked += 1;
        }
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok(TrunkFork::Forked { id, lsn, after })
    }

    /// A trunk fork as the pager makes it under the trunk's WAL write lock (`seen: None`), for the
    /// store's model tests, which drive the store without a pager: it always registers.
    #[cfg(test)]
    pub(crate) fn fork_trunk_locked(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        match self.fork_trunk(schema, page_size, None, None)? {
            TrunkFork::Forked { id, lsn, .. } => {
                self.wait_durable(lsn, self.sync_class())?;
                Ok(id)
            }
            _ => unreachable!("a fork under the WAL write lock always registers"),
        }
    }

    /// A branch fork as the public API makes it, waiting until it is durable, for the store's
    /// model tests.
    #[cfg(test)]
    pub(crate) fn fork_branch_durable(&self, parent: BranchId) -> Result<BranchId> {
        let (id, lsn) = self.fork_branch(parent, None)?;
        self.wait_durable(lsn, self.sync_class())?;
        Ok(id)
    }

    /// The store's own class (`Off` when volatile).
    pub(crate) fn sync_class(&self) -> SyncClass {
        self.class
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress — a
    /// branch's write transaction takes its copy decisions at each page's first write, so a child
    /// forked inside it would see its commit — and refused on a released branch.
    ///
    /// Early release (fastest-engine M1 item 2), as `fork_trunk`: the caller waits on the returned
    /// log sequence number (`wait_durable`) before handing the branch out.
    pub(crate) fn fork_branch(&self, parent: BranchId, name: Option<&str>) -> Result<(BranchId, u64)> {
        if let Some(name) = name {
            check_branch_name(name)?;
        }
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.lock_counted();
        self.mature(&mut inner);
        // Reap what has expired first, so a parent whose lease ran out is refused rather than
        // revived by a child that would pin it (Neon refuses to "create children from expiring
        // branches").
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        inner.ensure(parent)?;
        self.reap_if_due(&mut inner, parent, now)?;
        let st = inner.branches.get(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        if st.handle.is_released() {
            return Err(reaped(parent));
        }
        let schema = st.schema.clone();
        // The name's uniqueness is decided in the same store-mutex hold that buffers its record.
        if let Some(name) = name {
            if inner.name_lookup(name)?.is_some() {
                return Err(name_taken(name));
            }
        }
        let id = BranchId(inner.next_id);
        let (records, lease) = inner.fork_records(id, parent, now, name);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        let lsn = self.buffer_records(&mut inner, &records)?;
        let handle = if name.is_some() { Handle::Detached } else { Handle::Attached };
        if let Err(e) = inner.apply_fork(parent, id, schema, handle, name.map(Arc::from)) {
            return Err(inner.fatal(e));
        }
        inner.apply_fork_lease(id, lease);
        inner.note_fork_lsn(id, lsn);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok((id, lsn))
    }

    /// Mark the branch open for a connection and return its committed schema (`None` after a
    /// reopen: the caller reparses it). One connection per branch: two would each hold a private
    /// page cache of the same page space, and nothing would tell one that the other had committed
    /// — a silently stale read, so it is refused.
    pub(crate) fn open_conn(&self, id: BranchId) -> Result<Option<Arc<Schema>>> {
        self.settle(id, true)?;
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
        // Nor is an expired branch openable: the same pass, the same refusal.
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        let poisoned = inner.poisoned().then(|| fail_stop_cause(inner.journal.as_ref()));
        inner.ensure(id)?;
        self.reap_if_due(&mut inner, id, now)?;
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.handle.is_released() {
            return Err(reaped(id));
        }
        if st.in_doubt {
            return Err(in_doubt(id));
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
        // A named server branch is connected by name and has no handle (fastest-engine item 4).
        if st.handle != Handle::Attached && !(st.handle == Handle::Detached && st.name.is_some()) {
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
        let mut inner = self.inner.lock();
        let mut freed = Vec::new();
        // A released branch is collected (and possibly spliced) here, outside any other logged
        // operation, so the close is logged: replay collects it at this same point (F7 durable
        // port). A `Close` that cannot be made durable leaves the store fail-stopped (every failed
        // flush fail-stops it), so no record can follow it: recovery replays to the release, finds
        // nothing open, and collects the branch at its end. A fail-stopped store frees nothing in
        // this process (review B-F1): its Release may not be durable either.
        let held = inner
            .branches
            .get(&id)
            .is_some_and(|st| st.open && st.handle == Handle::Released);
        if held {
            if let Err(e) = self.log(&mut inner, Record::Close { branch: id.0 }) {
                tracing::warn!("released branch {} closed, the Close not durable: {e}", id.0);
            }
            // The catalog row must stop saying held (`released = 2`): a checkpoint after this close
            // drops the `Close` from the log, and a row still marked held would be held again at the
            // next recovery, which would then skip the collects made since (review of c47df7e64).
            inner.mark_dirty(id, DIRTY_ROW);
        }
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
            freed.extend(st.pending.drain().map(|(_, slot)| slot));
        }
        // fastest-engine mutant `close_frees_when_stopped` (test builds only).
        if inner.poisoned() && !fe_mutant("close_frees_when_stopped") {
            return;
        }
        if let Err(e) = inner.collect(id, &mut freed) {
            tracing::warn!("branch {} not collected at close: {e}", id.0);
            let _ = inner.fatal(e);
            return;
        }
        inner.release_slots(freed);
        self.sync_trunk_children(&inner);
    }

    /// The `Branch` handle has gone: release the branch.
    ///
    /// An error when the release could not be made durable (review N4): the branch is then kept —
    /// nothing is freed in this process — and comes back, detached, at the next open.
    pub(crate) fn release_handle(&self, id: BranchId) -> Result<Reaped> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
        inner.ensure(id)?;
        // Released already — by a racing drop of the same name, say — and possibly collected: the
        // Release may still be in the air, so this reports only once everything buffered so far is
        // durable (review B-F2).
        let released = match inner.branches.get(&id) {
            None => Some(false),
            Some(st) if st.handle == Handle::Released => Some(true),
            Some(_) => None,
        };
        if let Some(deferred) = released {
            // fastest-engine mutant `rerelease_no_wait` (test builds only): reported at once.
            let lsn = if fe_mutant("rerelease_no_wait") {
                0
            } else {
                inner.journal.as_ref().map_or(0, Journal::lsn)
            };
            let class = inner.sync;
            drop(inner);
            self.wait_durable(lsn, class)?;
            return Ok(Reaped {
                freed_pages: 0,
                deferred,
            });
        }
        let st = inner.branches.get(&id).ok_or_else(|| gone(id))?;
        match st.handle {
            Handle::Released => unreachable!("answered above"),
            Handle::ReleasePending => {
                return Err(fail_stopped(inner.journal.as_ref(), id, "no release"))
            }
            Handle::Attached | Handle::Detached => {}
        }
        let record = inner.release_record(id);
        // Early release (fastest-engine M1 item 2): buffered and applied under the mutex, durable
        // by `wait_durable` after it; the slots it frees wait for that (rule 2).
        let lsn = match self.buffer_records(&mut inner, &[record]) {
            Ok(lsn) => lsn,
            Err(e) => {
                // The release is not durable, so nothing may be freed — now or ever in this
                // process: after a restart the branch comes back (detached), and its slots must
                // still hold what it names. ReleasePending is the state `collect` never frees.
                tracing::warn!("branch {} released in memory only: {e}", id.0);
                if let Some(st) = inner.branches.get_mut(&id) {
                    st.handle = Handle::ReleasePending;
                }
                // F-W1: gone from the caller's point of view, so not listed (as `is_released` says).
                inner.live_ids_remove(id);
                return Err(LimboError::InternalError(format!(
                    "branch {} was not released durably ({e}); it is kept, and comes back at the \
                     next open",
                    id.0
                )));
            }
        };
        self.last_release_lsn.fetch_max(lsn, Ordering::AcqRel);
        let mut freed = Vec::new();
        let spliced = match inner.apply_release(id, &mut freed) {
            Ok(spliced) => spliced,
            Err(e) => return Err(inner.fatal(e)),
        };
        let freed_pages = freed.len();
        inner.defer_frees(lsn, freed);
        self.sync_trunk_children(&inner);
        self.sync_lease_flag(&inner);
        // Kept for a live child is decided now: the checkpoint `maybe_compact` may run can evict the
        // released state under a resident cap, which would report it as freed (second review).
        let deferred = spliced || inner.branches.contains_key(&id);
        self.maybe_compact(&mut inner);
        let class = inner.sync;
        drop(inner);
        kill_point("release.applied");
        if let Err(e) = self.wait_durable(lsn, class) {
            // Applied in memory and not durable: the store is fail-stopped, and the slots it freed
            // never return (no flight will cover them); the branch comes back at the next open.
            // `Released` means durable or in the air, so it becomes `ReleasePending`, which nothing
            // ever frees (lead review 1 item 3).
            if let Some(st) = self.inner.lock().branches.get_mut(&id) {
                if st.handle == Handle::Released {
                    st.handle = Handle::ReleasePending;
                }
            }
            return Err(LimboError::InternalError(format!(
                "branch {} was not released durably ({e}); it comes back at the next open",
                id.0
            )));
        }
        Ok(Reaped {
            freed_pages,
            deferred,
        })
    }

    /// Detach a live branch from its handle without releasing it.
    pub(crate) fn detach(&self, id: BranchId) {
        let mut inner = self.inner.lock();
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
        self.settle(id, false)?;
        let mut inner = self.inner.lock();
        inner.ensure(id)?;
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        // A named server branch takes no handle (review C-F3): a handle's drop would release it and
        // a handle could lease it, and only `Database::drop_branch` may end it.
        if let Some(name) = st.name.as_ref().filter(|_| !fe_mutant("named_attach")) {
            if !st.handle.is_released() {
                return Err(LimboError::InvalidArgument(format!(
                    "branch {} is the named server branch {name:?}: it takes no handle; connect to \
                     it with Database::connect_named and release it with Database::drop_branch",
                    id.0
                )));
            }
        }
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

    /// The unreleased branch named `name` (fastest-engine M1 item 4).
    pub(crate) fn branch_named(&self, name: &str) -> Result<Option<BranchId>> {
        self.refuse_if_trunk_only("looking a branch up by name")?;
        check_branch_name(name)?;
        let found = self.inner.lock().name_lookup(name)?;
        // Not before its fork is durable (review C-F4): a crash would lose a branch already found.
        if let Some(id) = found {
            self.settle(id, false)?;
        }
        Ok(found)
    }

    /// Wait, holding no lock, until branch `id` may be found, attached or (`readable`) opened
    /// (review A-F3, C-F4): its fork's records are durable, and, for an open, no trunk commit it
    /// forked after is still between its copy decisions and its publication. A branch whose
    /// ancestry left the trunk at or after the open gate's decision epoch (`trunk_at`, which its
    /// children inherit) forked after that commit, and nothing retained the pages it overwrites
    /// for it: read before the publication, it would see them one way, and after it the other.
    fn settle(&self, id: BranchId, readable: bool) -> Result<()> {
        let (fork_lsn, after) = {
            let mut inner = self.inner.lock();
            if !inner.ensure(id)? {
                return Ok(());
            }
            let Some(st) = inner.branches.get(&id) else {
                return Ok(());
            };
            let gate = self.trunk_commits.load(Ordering::Acquire);
            let after = (readable && gate % 2 == 1 && st.trunk_at >= inner.trunk_commit_epoch)
                .then_some(gate);
            (st.fork_lsn, after)
        };
        // fastest-engine mutants `open_no_publish_wait` and `find_no_durable_wait` (test builds
        // only): an opener does not wait out the commit; a finder does not wait for durability.
        if let Some(c) = after.filter(|_| !fe_mutant("open_no_publish_wait")) {
            self.wait_trunk_commit_published(c)?;
        }
        if fe_mutant("find_no_durable_wait") {
            return Ok(());
        }
        self.wait_durable(fork_lsn, self.class)
    }

    /// Every unreleased branch. Refused on a trunk-only store, where "none" would be a lie.
    ///
    /// F-W1 (githost-shape lane; F-cat-snap, r11-diff-list, as r2 ported it): the live-id set is
    /// cloned under the mutex in O(1) and walked after it is released, in id order, so a listing of
    /// every branch holds the lock for a reference-count increment, not for a scan of the resident
    /// states, a catalog query over every unreleased row, and a sort. The first listing of a process
    /// builds the set once (`build_live_ids`: the same scan and query, under the mutex, counted).
    pub(crate) fn ids(&self) -> Result<Vec<BranchId>> {
        self.refuse_if_trunk_only("listing branches")?;
        let (durable_at, snapshot) = {
            let mut inner = self.inner.lock();
            // No fork is listed before it is durable (review C-F4): the listing waits for the
            // newest one, after the lock is released. A fail-stopped store makes nothing durable
            // again, and every fork it did not make durable was released by its creator, so it
            // lists without waiting (N4: a stopped store stays readable).
            let durable_at = if inner.poisoned() { 0 } else { inner.last_fork_lsn };
            // githost-shape instrument (observing only).
            inner.shape.ids_calls += 1;
            if knob_off("fw1") {
                // I1: the store before F-W1: the scan and the catalog query, under the mutex, at
                // every listing; its catalog rows are a listing's (ids_catalog_rows).
                let before = inner.shape.ids_build_rows;
                let set = inner.build_live_ids()?;
                let rows = inner.shape.ids_build_rows - before;
                inner.shape.ids_build_rows = before;
                inner.shape.ids_catalog_rows += rows;
                (durable_at, set)
            } else {
                if inner.live_ids.is_none() {
                    let set = inner.build_live_ids()?;
                    inner.live_ids = Some(set);
                }
                (durable_at, inner.live_ids.clone().expect("built above"))
            }
        };
        self.wait_durable(durable_at, self.class)?;
        let mut ids = Vec::with_capacity(snapshot.len() as usize);
        let mut work = IdSetWork::default();
        snapshot.for_each(&mut work, &mut |key| ids.push(BranchId(u64::from(key))));
        Ok(ids)
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
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
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            st.writer = false;
        }
    }

    /// A branch transaction rolled back: its reservations were never published, so they are free.
    pub(crate) fn abort_write(&self, id: BranchId) {
        let mut inner = self.inner.lock();
        let mut freed = Vec::new();
        if let Some(st) = inner.branches.get_mut(&id) {
            freed.extend(st.pending.drain().map(|(_, slot)| slot));
        }
        inner.release_slots(freed);
    }

    pub(crate) fn holds_writer(&self, id: BranchId) -> bool {
        self.inner
            .lock()
            .branches
            .get(&id)
            .is_some_and(|st| st.writer)
    }

    pub(crate) fn schema(&self, id: BranchId) -> Result<Arc<Schema>> {
        let mut inner = self.inner.lock();
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
        let mut inner = self.inner.lock();
        inner.branches.get_mut(&id).ok_or_else(|| gone(id))?.schema = Some(schema);
        Ok(())
    }

    /// The copy decision for a branch's first write to `page` in a transaction: reserve the fresh
    /// slot the commit will write the page into. Nothing is published until then.
    pub(crate) fn first_write_branch(&self, id: BranchId, page: u32) -> Result<()> {
        let mut inner = self.inner.lock();
        // Covered deferred frees first, so this reservation can reuse them (group commit).
        self.mature(&mut inner);
        // A transaction that began before the journal failed may write no further page.
        if inner.poisoned() {
            return Err(fail_stopped(inner.journal.as_ref(), id, "no page write"));
        }
        inner.refill_free()?;
        let StoreInner {
            arena,
            branches,
            pending_holders,
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
        if let std::collections::hash_map::Entry::Vacant(e) = st.pending.entry(page) {
            e.insert(arena.alloc());
            // F-W2: this branch may hold a reserved slot at the next checkpoint.
            pending_holders.insert(id);
        }
        Ok(())
    }

    /// The copy decisions of one trunk commit, taken at its serialization point: after its frames
    /// are prepared and before any of them is written (`Pager::commit_wal`, which then makes the
    /// retained pre-images durable through `durability_barrier` before the first frame), against
    /// the trunk's epoch NOW, so a fork registered at any moment of the transaction is seen (F-L,
    /// r11-forklock 573642f19, ported to the durable store; Silo, SOSP 2013, takes a transaction's
    /// decisions at its commit the same way).
    ///
    /// # Trunk commits and forks — decisions at the commit, forks without the writer's lock
    ///
    /// A trunk fork used to take the trunk's WAL write lock, because a trunk write transaction took
    /// its copy decisions at each page's first write, for the epoch of that moment: had a fork come
    /// in between, the commit would have reached the new child. So every fork waited out every trunk
    /// transaction from BEGIN to COMMIT, and serialised with every other fork on that lock.
    ///
    /// * **The writer** only CAPTURES, at a page's first write in the transaction, the page as it
    ///   was (the pager's `trunk_pre_images`), and only while the trunk has a live child. Here, in one
    ///   store-mutex hold, it opens the COMMIT GATE (`trunk_commits` odd) and takes every decision:
    ///   for a page last committed before this epoch, retain `[born, epoch)` if a live child forked
    ///   in it (the pre-image goes to an arena slot and a `TrunkRetain` record), and stamp
    ///   `written` with `epoch`. The pager closes the gate ([`Self::end_trunk_commit`]) once the
    ///   frames are published (or the commit failed), after the Merger's stamps.
    /// * **A fork** reads `trunk_commits`, begins a WAL read snapshot, and registers under the store
    ///   mutex at once: every commit decided after the registration sees the child. If a commit was
    ///   decided since its snapshot (published, or in flight with the gate open), the child forks
    ///   AFTER it — its schema is reparsed from its own pages, and it is handed out only once that
    ///   commit is published (`fork_trunk`'s `after`) — so a fork waits for at most one commit and
    ///   never retries (a gate a fork had to wait CLOSED for starved forks under back-to-back trunk
    ///   commits, measured in C1 at 4 writers: the lane's first port did that).
    /// * **The first child** is forked the old way, under the WAL write lock: a writer that saw no
    ///   live child captured nothing, and must not see one appear before it commits.
    ///
    /// `pages` is every page the commit writes, each with its captured pre-image, or `None` if the
    /// trunk had no live child at the page's first write; then no live child can see the version it
    /// overwrites (the first child waits for this writer's lock), which is asserted.
    ///
    /// Durability is unchanged from the base's rule: a retained pre-image's record is buffered here
    /// and made durable by the barrier BEFORE the commit's first frame is written. A commit that
    /// fails after this leaves retained versions of pages it never changed, which only keeps a copy
    /// equal to the trunk's current page (the base's first-write decisions did the same).
    pub(crate) fn begin_trunk_commit<'p>(
        &self,
        pages: impl IntoIterator<Item = (u32, Option<&'p [u8]>)>,
    ) -> Result<u64> {
        // The second fence of a trunk-only store, behind the connection's read-only check: this
        // write would retain no pre-image for the branches on disk.
        self.refuse_if_trunk_only("a trunk page write")?;
        let mut inner = self.inner.lock();
        self.mature(&mut inner);
        let opened = self.trunk_commits.fetch_add(1, Ordering::AcqRel);
        crate::turso_assert!(opened % 2 == 0, "two trunk commits inside the commit gate at once");
        inner.work.trunk_commits_decided += 1;
        let epoch = inner.trunk.lineage.epoch;
        inner.trunk_commit_epoch = epoch;
        let retained_before = inner.work.trunk_pre_images_retained;
        let decided = self.decide_trunk_pages(&mut inner, pages, epoch);
        // What must be durable before the commit's first frame (lead review 1 item 10), fixed here
        // so a re-entry of the commit never waits for, or fails on, records buffered after it: its
        // own pre-images, and every early-released Release (it retained nothing for those
        // children). Mutant `barrier_own_only`: the Releases left out.
        let retained_end = if inner.work.trunk_pre_images_retained > retained_before {
            inner.journal.as_ref().map_or(0, Journal::lsn)
        } else {
            0
        };
        drop(inner);
        if let Err(e) = decided {
            // Refused part-way: the gate closes, and the pager takes the whole pass again
            // (`Pager::commit_wal`; review A-F2).
            self.end_trunk_commit();
            return Err(e);
        }
        kill_point("trunk.decided");
        #[cfg(test)]
        pause_at(Some(&*self.trunk_commit_hold), HOLD_TRUNK_DECIDED);
        if fe_mutant("barrier_own_only") {
            return Ok(retained_end);
        }
        Ok(retained_end.max(self.barrier_floor()))
    }

    /// What every trunk commit — one that took no copy decision too — must make durable before its
    /// first frame: every Release (gc3's N1) and every pre-image kept (review 3 #1) so far.
    pub(crate) fn barrier_floor(&self) -> u64 {
        if fe_mutant("barrier_own_only") {
            return 0;
        }
        self.last_release_lsn
            .load(Ordering::Acquire)
            .max(self.retain_floor.load(Ordering::Acquire))
    }

    /// `begin_trunk_commit`'s decisions, one per page the commit writes, at `epoch`.
    fn decide_trunk_pages<'p>(
        &self,
        inner: &mut StoreInner,
        pages: impl IntoIterator<Item = (u32, Option<&'p [u8]>)>,
        epoch: u64,
    ) -> Result<()> {
        for (page, pre_image) in pages {
            let Some(pre_image) = pre_image else {
                // Uncaptured: the trunk had no live child at this page's first write. With no live
                // child now either, nothing can see any version (and no catalog query is made).
                if inner.trunk.lineage.n_children > 0 {
                    inner.trunk_written_known(page)?;
                    let born = inner.trunk.written.get(&page).copied().unwrap_or(0);
                    let StoreInner { children, cat, .. } = &mut *inner;
                    let seen = born < epoch
                        && children.any_in(cat.as_mut().map(|c| &mut c.catalog), BranchId::TRUNK, born, epoch)?;
                    crate::turso_assert!(
                        !seen,
                        "a trunk commit overwrites a page a live child can see, and no pre-image was \
                         captured for it"
                    );
                }
                continue;
            };
            inner.work.trunk_pre_images_captured += 1;
            self.decide_trunk_page(inner, page, pre_image, epoch)?;
            if inner.failpoint == Some(BranchFailpoint::TrunkDecisionBusy) {
                inner.failpoint = None;
                return Err(LimboError::Busy);
            }
        }
        Ok(())
    }

    /// Close the commit gate opened by `begin_trunk_commit`: the commit's frames are published, or
    /// it failed. Only the trunk's one writer (the WAL write lock's holder) calls it, so an odd count
    /// is this writer's open gate; an even one means its `begin_trunk_commit` refused before
    /// opening it.
    pub(crate) fn end_trunk_commit(&self) {
        // The WAL flush that would have made an ordered flight durable did not come, or came and
        // said so already: either way nothing waits on it any more.
        {
            let mut g = self.group.lock();
            if g.pending_full.take().is_some() {
                self.group.cv.notify_all();
            }
        }
        if self.trunk_commits.load(Ordering::Acquire) % 2 == 1 {
            self.trunk_commits.fetch_add(1, Ordering::AcqRel);
            let (lock, closed) = &self.gate_closed;
            let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
            closed.notify_all();
        }
    }

    /// The copy decision for one model-test page write, as a one-page trunk commit decided at once:
    /// for the store's model tests, which drive the store without a pager.
    #[cfg(test)]
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) -> Result<()> {
        let decided = self.begin_trunk_commit([(page, Some(pre_image))]);
        self.end_trunk_commit();
        decided.map(|_| ())
    }

    /// One captured page's decision at `epoch` (see `begin_trunk_commit`): if a live child forked
    /// since the page's last commit can still see the version being overwritten, keep a copy of it
    /// for that child; then stamp `written`.
    fn decide_trunk_page(
        &self,
        inner: &mut StoreInner,
        page: u32,
        pre_image: &[u8],
        epoch: u64,
    ) -> Result<()> {
        // Catalog stores: the page's `written` epoch, from its last catalog version, first.
        inner.trunk_written_known(page)?;
        let born = inner.trunk.written.get(&page).copied().unwrap_or(0);
        if born >= epoch {
            return Ok(());
        }
        let keep = {
            let StoreInner { children, cat, .. } = &mut *inner;
            children.any_in(cat.as_mut().map(|c| &mut c.catalog), BranchId::TRUNK, born, epoch)?
        };
        // Mutant M-e (PREREG v1 amendment 36): a trunk commit skips the TrunkRetain of a page a
        // live branch reads.
        let keep = keep && !fe_mutant("skip_trunk_retain");
        if keep {
            inner.refill_free()?;
            inner.work.trunk_pre_images_retained += 1;
        }
        let StoreInner {
            arena,
            trunk,
            journal,
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
                // fastest-engine mutant `no_retain_floor` (test builds only).
                if !fe_mutant("no_retain_floor") {
                    self.retain_floor.fetch_max(journal.lsn(), Ordering::AcqRel);
                }
            }
        }
        trunk.written.insert(page, epoch);
        Ok(())
    }

    /// The barrier as the store's model tests drive it, with no pager: everything buffered so far.
    #[cfg(test)]
    pub(crate) fn durability_barrier(&self, trunk: SyncClass) -> Result<()> {
        let required = self.inner.lock().journal.as_ref().map_or(0, Journal::lsn);
        self.durability_barrier_to(trunk, required)
    }

    /// Make what a trunk commit relies on durable (`required`, from `begin_trunk_commit`).
    /// `Pager::commit_wal` calls this before it writes a single frame, so a trunk commit is never
    /// durable ahead of the pre-images it overwrote. A commit with nothing to make durable and no
    /// stamp due returns at once, taking no lock. `trunk` is the class the commit will sync its WAL in: when it is stronger than the store's,
    /// the pre-images are flushed in it, so the commit is never MORE durable than they are (a D1
    /// store under a `PRAGMA fullfsync` trunk connection; see [`SyncClass`]).
    ///
    /// While a lease is outstanding it also stamps the lease clock, at most once per
    /// `STAMP_EVERY_MS` (review N2), and flushes a stamp still only queued.
    pub(crate) fn durability_barrier_to(&self, trunk: SyncClass, required: u64) -> Result<()> {
        // Nothing to make durable, no stamp to write: no lock at all (lead review 1 item 10).
        let class = self.class.max(trunk);
        if !self.unsynced.load(Ordering::Acquire)
            && !self.leases_outstanding.load(Ordering::Acquire)
            && self.group.durable(class) >= required
        {
            return Ok(());
        }
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
        self.barrier_locks.fetch_add(1, Ordering::Relaxed);
        // Re-read under the lock: `begin_trunk_commit` sets it while holding it.
        let unsynced = self.unsynced.load(Ordering::Acquire);
        let leases_exist = inner.leases_exist();
        let StoreInner {
            journal,
            arena,
            failpoint,
            orphans,
            lease,
            ..
        } = &mut *inner;
        let (Some(journal), Some(_)) = (journal.as_mut(), arena.as_ref()) else {
            return Ok(());
        };
        if unsynced && *failpoint == Some(BranchFailpoint::BarrierBeforeRecords) {
            *failpoint = None;
            *orphans = journal.pending_slots.clone();
            journal.poison();
            return Err(LimboError::InternalError(
                "failpoint: the trunk commit's branch barrier stopped before its records"
                    .to_string(),
            ));
        }
        // What must be durable before the commit's first frame was fixed at its decisions
        // (`begin_trunk_commit`): its own pre-images, and every early-released Release.
        let mut target = required;
        let mut stamp_due = false;
        if leases_exist && !journal.is_poisoned() {
            let now = lease.now_ms();
            if now >= lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                if *failpoint == Some(BranchFailpoint::StampFlushFails) {
                    *failpoint = None;
                    // Fails inside the flight, so the poisoning is the flight's own (review 4 C7).
                    journal.fail_next_write();
                }
                journal.buffer(&Record::Clock { now_ms: now })?;
                lease.queued(now);
                target = journal.lsn();
                stamp_due = true;
            } else if lease.queued_ms > lease.durable_ms {
                // A stamp only queued: carried by this flush (review N3).
                stamp_due = true;
            }
        }
        if unsynced {
            journal.raise_pending_class(class);
        }
        drop(inner);
        if self.group.durable(class) >= required && !stamp_due {
            self.unsynced.store(false, Ordering::Release);
            return Ok(());
        }
        // A commit that F_FULLFSYNCs its WAL on the branch files' device needs its pre-images only
        // ORDERED before its frames: that flush makes both durable (lead review 1 item 6).
        let waited = if class == SyncClass::FullFsync && self.ordered_trunk() {
            self.order_for_trunk(target)
        } else {
            self.wait_durable(target, class)
        };
        let mut inner = self.inner.lock();
        match waited {
            Ok(()) => {
                if stamp_due {
                    inner.lease.flushed();
                }
                self.unsynced.store(false, Ordering::Release);
                self.maybe_compact(&mut inner);
                drop(inner);
                kill_point("trunk.barrier_done");
                #[cfg(test)]
                pause_at(Some(&*self.trunk_commit_hold), HOLD_TRUNK_BARRIER_DONE);
                Ok(())
            }
            // Only stamps were at stake: a trunk commit does not fail because its stamp could not
            // be written (a lost stamp lengthens leases and loses no data), though the failed
            // flight poisoned the store, as every failed flush does.
            Err(e) if self.group.durable(class) >= required => {
                tracing::warn!("branch lease clock not stamped at a trunk commit: {e}");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Whether a trunk commit that F_FULLFSYNCs its WAL makes the branch files durable with it: Apple
    /// (F_BARRIERFSYNC orders a device's writes, F_FULLFSYNC drains its whole cache), and the WAL on
    /// the branch files' device (`note_trunk_wal`). Mutant `trunk_unordered` (test builds only).
    fn ordered_trunk(&self) -> bool {
        cfg!(target_vendor = "apple")
            && self.trunk_same_device.load(Ordering::Acquire)
            && !fe_mutant("trunk_unordered")
    }

    /// Record whether this trunk commit's WAL flush drains the branch files' device (see
    /// `ordered_trunk`): `wal_device` is what the WAL file being synced reports
    /// (`Wal::full_fsync_device`: `Some` only for an F_FULLFSYNC on its own descriptor), and it must
    /// be the device the branch log and arena were opened on, read from their descriptors
    /// (`files_device`). Anything unknown reads as "not" (review 3 #3). Called by every trunk
    /// commit that syncs its WAL in FullFsync, before its barrier.
    pub(crate) fn note_trunk_wal(&self, wal_device: Option<u64>) {
        let files = self.files_dev.load(Ordering::Acquire);
        // Mutant `trust_any_wal` (test builds only): as before review 3 #3, any WAL flush counts
        // once the branch files' device is known.
        let same = files != NO_DEVICE && (wal_device == Some(files) || fe_mutant("trust_any_wal"));
        if self.trunk_same_device.load(Ordering::Relaxed) != same {
            self.trunk_same_device.store(same, Ordering::Release);
        }
    }

    /// Make the journal's first `lsn` bytes ORDERED ahead of a trunk commit's frames, leading an
    /// ordered flight when nothing covers them (lead review 1 item 6): the arena and the log are
    /// barriered, not flushed, and the commit's WAL F_FULLFSYNC makes them durable
    /// (`trunk_wal_synced`). Called by that commit, holding the WAL write lock, so it is the only
    /// trunk commit in flight; the flight it leads is marked pending until its WAL is synced.
    fn order_for_trunk(&self, lsn: u64) -> Result<()> {
        let full = class_index(SyncClass::FullFsync);
        loop {
            {
                let mut g = self.group.lock();
                loop {
                    if g.durable[full] >= lsn || (g.ordered >= lsn && g.pending_full.is_some()) {
                        return Ok(());
                    }
                    if self.group.poisoned() {
                        return Err(group_poisoned());
                    }
                    if !g.flushing && !g.cutting {
                        break;
                    }
                    g = self.group.wait(g);
                }
            }
            let flight = {
                let mut inner = self.lock_counted();
                let mut g = self.group.lock();
                if g.durable[full] >= lsn || (g.ordered >= lsn && g.pending_full.is_some()) {
                    return Ok(());
                }
                if self.group.poisoned() {
                    return Err(group_poisoned());
                }
                if g.flushing || g.cutting {
                    continue;
                }
                let upgrade = g.durable[0] >= lsn;
                match Self::take_flight(&mut inner, SyncClass::FullFsync, upgrade) {
                    Ok(Some(flight)) if !flight.is_empty() => {
                        g.flushing = true;
                        g.flights += 1;
                        g.pending_full = Some(g.pending_full.unwrap_or(0).max(flight.end_lsn));
                        flight.ordered()
                    }
                    Ok(_) => return Err(group_poisoned()),
                    Err(e) => {
                        self.group.fail(&mut g);
                        return Err(e);
                    }
                }
            };
            let end = flight.end_lsn;
            let written = flight.write();
            self.group.land_ordered(end, written.is_ok());
            written?;
        }
    }

    /// Just before a trunk commit F_FULLFSYNCs its WAL (`Pager::commit_wal_inner`): order what was
    /// buffered since its pre-image barrier — the forks that registered inside its gate — so that
    /// flush carries them too, and return how far the branch journal is on the device ahead of it
    /// (0: nothing rides). `trunk` is the class the WAL is synced in. A failed flight fail-stops
    /// the store as any does; the commit, whose pre-images were ordered before, goes on.
    pub(crate) fn order_riders(&self, trunk: SyncClass) -> u64 {
        if trunk != SyncClass::FullFsync || !self.ordered_trunk() {
            return 0;
        }
        let buffered = {
            let inner = self.inner.lock();
            inner.journal.as_ref().map_or(0, |j| if j.pending_len() > 0 { j.lsn() } else { 0 })
        };
        if buffered > 0 {
            if let Err(e) = self.order_for_trunk(buffered) {
                tracing::warn!("branch records not ordered ahead of a trunk WAL flush: {e}");
            }
        }
        let g = self.group.lock();
        g.ordered.max(g.durable[class_index(SyncClass::Fsync)])
    }

    /// The trunk commit's WAL F_FULLFSYNC returned: every journal byte below `frontier` (read by
    /// `order_riders` before it was issued) is durable in every class.
    pub(crate) fn trunk_wal_synced(&self, frontier: u64) {
        if frontier == 0 {
            return;
        }
        let mut g = self.group.lock();
        for c in 0..=class_index(SyncClass::FullFsync) {
            self.group.set_durable(&mut g, c, frontier);
        }
        if g.pending_full.is_some_and(|p| p <= frontier) {
            g.pending_full = None;
        }
        self.group.cv.notify_all();
    }

    /// Commit a branch's dirty pages: write each into the slot its copy decision reserved, buffer
    /// the `Commit` record and move the branch's map under the store mutex, then wait — holding no
    /// lock — until the record is durable before reporting the commit (early release, M1 item 2).
    /// A commit whose wait fails leaves the branch in doubt: refused from then on (lead review 1
    /// item 11).
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        // F-FZ back-pressure: dropped after the guard below.
        let _backpressure = Backpressure(self);
        let mut inner = self.inner.lock();
        // Refuse BEFORE any slot is written: after the journal failed, this commit's record can
        // never be durable, so its pages have no business in the arena.
        if inner.poisoned() {
            return Err(fail_stopped(inner.journal.as_ref(), id, "no commit"));
        }
        let entries = {
            let StoreInner {
                arena,
                branches,
                journal,
                failpoint,
                orphans,
                sync,
                ..
            } = &mut *inner;
            let st = branches.get_mut(&id).ok_or_else(|| gone(id))?;
            if st.handle.is_released() {
                return Err(reaped(id));
            }
            let arena = arena.as_mut().expect("a branch exists, so the arena does");
            let mut entries = Vec::with_capacity(pages.len());
            for page in pages {
                let no = page.get().id as u32;
                let slot = st.pending.remove(&no).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "branch {} committed page {no} with no copy decision behind it",
                        id.0
                    ))
                })?;
                let bytes = page.get_contents().as_slice();
                arena.write_slot(slot, bytes)?;
                entries.push((no, slot, crc32c::crc32c(bytes)));
            }
            // A reservation whose page is not dirty at commit was never written: free it.
            for (_, slot) in st.pending.drain() {
                arena.release(slot);
            }
            if *failpoint == Some(BranchFailpoint::CommitAfterSlotsBeforeRecord) {
                *failpoint = None;
                arena.sync(*sync)?;
                *orphans = entries.iter().map(|&(_, slot, _)| slot).collect();
                for &(_, slot, _) in &entries {
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
            entries
        };
        if entries.is_empty() {
            return Ok(());
        }
        // On failure the written slots are NOT freed: the record may have reached the disk, and
        // recovery, not this process, decides whether they are live.
        let mut records = vec![Record::Commit {
            branch: id.0,
            pages: entries.clone(),
        }];
        // The commit pays for a flush anyway: while a lease is outstanding, stamp the clock in it,
        // so a crash cannot lose the open time an agent spent committing (review R2).
        if inner.leases_exist() {
            let now = inner.lease.now_ms();
            if now > inner.lease.queued_ms {
                records.push(Record::Clock { now_ms: now });
                inner.lease.queued(now);
            }
        }
        kill_point("commit.slots_written");
        // Early release (fastest-engine M1 item 2): buffered, applied, and waited for with the
        // mutex released. The versions this commit supersedes are freed only once it is durable.
        let lsn = self.buffer_records(&mut inner, &records)?;
        let mut freed = Vec::new();
        if let Err(e) = inner.apply_commit(id, &entries, &mut freed) {
            return Err(inner.fatal(e));
        }
        inner.defer_frees(lsn, freed);
        self.maybe_compact(&mut inner);
        let class = inner.sync;
        drop(inner);
        kill_point("commit.applied");
        let durable = self.wait_durable(lsn, class);
        // fastest-engine mutant `commit_doubt_ignored` (test builds only).
        if durable.is_err() && !fe_mutant("commit_doubt_ignored") {
            if let Some(st) = self.inner.lock().branches.get_mut(&id) {
                st.in_doubt = true;
            }
        }
        durable
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.inner.lock();
        // One atomic load on the hot path: a branch can be in doubt only once the store stopped.
        if inner.fail_stop.load(Ordering::Acquire) {
            inner.refuse_in_doubt(id)?;
        }
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

    /// Fill `out` with `page` as the trunk held it when branch `id` forked, if that version lives
    /// in the arena (a retained trunk version). `false` means the trunk's current version is the
    /// base, which the caller reads through the ordinary path; an error means the base is gone
    /// (r11-merge A20: V4's base read). The durable Merger's derivation reads every base page
    /// through it (r13-compose A5), so V4's base_reads/base_arena counters include Merger work.
    pub(crate) fn base_page_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let mut inner = self.inner.lock();
        let mut examined = 0;
        let resolved = inner.base_resolve(id, page, &mut examined);
        inner.v4.base_reads += 1;
        inner.v4.base_examined += examined;
        if resolved.is_err() {
            inner.v4.base_refused += 1;
        }
        let Some((slot, crc)) = resolved? else {
            return Ok(false);
        };
        let arena = inner
            .arena
            .as_ref()
            .expect("a slot resolved, so the arena exists");
        // An arena read counts only once its page is read and checked; a failed read or checksum
        // is a refusal, as the caller sees it.
        let read = arena.read_slot(slot, out).and_then(|()| {
            if arena.is_file_backed() && crc32c::crc32c(out) != crc {
                return Err(LimboError::Corrupt(format!(
                    "branch {}'s base page {page} (arena slot {slot}) failed its checksum",
                    id.0
                )));
            }
            Ok(())
        });
        match &read {
            Ok(()) => inner.v4.base_arena += 1,
            Err(_) => inner.v4.base_refused += 1,
        }
        read.map(|()| true)
    }

    /// `(base reads, arena-resolved, refused, retained versions examined, C-P trunk probes, C-P
    /// trunk rows)` since open (r11-merge A20). Does not settle, so reading it moves nothing.
    pub(crate) fn v4_counters(&self) -> (u64, u64, u64, u64, u64, u64) {
        let inner = self.inner.lock();
        let v = inner.v4;
        let (probes, rows) = inner
            .cat
            .as_ref()
            .map_or((0, 0), |c| (c.trunk_probes, c.trunk_rows));
        (v.base_reads, v.base_arena, v.base_refused, v.base_examined, probes, rows)
    }

    /// `(probes, rows)` of `trunk_written_known`'s once-per-page catalog probe since open
    /// (r12-composition K8-B instrument). Does not settle.
    pub(crate) fn twk_counters(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        inner.cat.as_ref().map_or((0, 0), |c| (c.twk_probes, c.twk_rows))
    }

    /// `(twk reads, tva probes, tva rows, tva reads)`: database page reads inside
    /// `trunk_written_known`'s probes, and `trunk_version_at`'s probes, rows and reads, since open
    /// (r12-composition K8-B amendment 13). Does not settle.
    pub(crate) fn probe_split_counters(&self) -> (u64, u64, u64, u64) {
        let inner = self.inner.lock();
        inner
            .cat
            .as_ref()
            .map_or((0, 0, 0, 0), |c| (c.twk_reads, c.tva_probes, c.tva_rows, c.tva_reads))
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

    /// The catalog file's shape, read under the store's lock so no checkpoint writes it meanwhile
    /// (r11-ever amendment 19; observation only).
    pub(crate) fn catalog_shape(&self) -> Result<Option<super::CatalogShape>> {
        let inner = self.inner.lock();
        match inner.cat.as_ref() {
            Some(c) => c.catalog.shape().map(Some),
            None => Ok(None),
        }
    }

    /// Catalog statements that wrote a row, since open (r11-restart lane instrument).
    pub(crate) fn catalog_rows_written(&self) -> u64 {
        self.inner
            .lock()
            .cat
            .as_ref()
            .map_or(0, |c| c.catalog.counters.rows_written)
    }

    /// What this open's prewarm did, the arena's part and the catalog's merged (r12-catload
    /// instrument).
    pub(crate) fn prewarm_stats(&self) -> PrewarmStats {
        let inner = self.inner.lock();
        let catalog = inner.cat.as_ref().map(|c| c.catalog.prewarm).unwrap_or_default();
        self.prewarm.merged(catalog)
    }

    /// F-W3 (githost-shape lane): the most branch states a catalog store keeps resident after each
    /// checkpoint; `None` keeps every state touched since the open. It takes effect at the next
    /// checkpoint.
    pub(crate) fn set_resident_cap(&self, cap: Option<usize>) {
        self.inner.lock().resident_cap = cap;
    }

    /// githost-shape instrument (observing only): see [`super::BranchCatShape`]. Runs no catalog
    /// query, so it does not move the catalog counters it sits beside.
    pub(crate) fn cat_shape(&self) -> super::BranchCatShape {
        let inner = self.inner.lock();
        let s = &inner.shape;
        let mut shape = super::BranchCatShape {
            checkpoints: s.checkpoints,
            checkpoint_ns: s.checkpoint_ns,
            ckpt_trunk_inserted: s.ckpt_trunk_inserted,
            ckpt_trunk_deleted: s.ckpt_trunk_deleted,
            ckpt_branch_rows: s.ckpt_branch_rows,
            ckpt_rows_written: s.ckpt_rows_written,
            ckpt_states_walked: s.ckpt_states_walked,
            ids_calls: s.ids_calls,
            ids_resident_visited: s.ids_resident_visited,
            ids_catalog_rows: s.ids_catalog_rows,
            ids_build_rows: s.ids_build_rows,
            table_grows: s.table_grows,
            table_moved: s.table_moved,
            evictions: s.evictions,
            evicted_states: s.evicted_states,
            ensure_cold: s.ensure_cold,
            ensure_chain_sum: s.ensure_chain_sum,
            ensure_chain_max: s.ensure_chain_max,
            evicted_with_resident_descendant: s.evicted_with_resident_descendant,
            walk_items_yielded: s.walk_items_yielded,
            walk_slots_scanned: s.walk_slots_scanned,
            instrument_walk_items: s.instrument_walk_items,
            settle_sharp_calls: s.settle_sharp_calls,
            settle_sharp_loads: s.settle_sharp_loads,
            settle_sharp_max_loads: s.settle_sharp_max_loads,
            derived_inserts: inner.derived_inserts,
            table_chunks: inner.branches.chunk_stats().0,
            chunk_allocs: inner.branches.chunk_stats().1,
            table_slots_allocated: inner.branches.capacity() as u64,
            table_slot_bytes: (inner.branches.capacity()
                * std::mem::size_of::<Option<(BranchId, BranchState)>>()) as u64,
            resident_states: inner.branches.len() as u64,
            trunk_overlay_versions: inner
                .trunk
                .lineage
                .retained
                .values()
                .map(|v| v.len() as u64)
                .sum(),
            log_len: inner.journal.as_ref().map_or(0, |j| j.log_len()),
            ..Default::default()
        };
        if let Some(c) = inner.cat.as_ref() {
            shape.dirty_branches = c.dirty.len() as u64;
            shape.trunk_cache_versions = c.trunk_cache.values().map(|v| v.len() as u64).sum();
            shape.trunk_cache_pages = c.trunk_cache.len() as u64;
            shape.trunk_known_pages = c.trunk_known.len() as u64;
            shape.trunk_probes = c.trunk_probes;
            shape.trunk_rows = c.trunk_rows;
        }
        shape
    }

    /// `(branch states read from the catalog, trunk pages read, catalog queries, catalog rows
    /// read)` since open (r11-restart lane instrument; zeros for a snapshot store).
    /// Trunk commit barriers that took the store mutex (lead review 1 item 10's instrument).
    #[cfg(test)]
    pub(crate) fn barrier_locks(&self) -> u64 {
        self.barrier_locks.load(Ordering::Relaxed)
    }

    /// See `Database::branch_wait_name_filter`.
    pub(crate) fn wait_name_filter(&self) {
        if let Some(build) = self.name_filter_build.lock().take() {
            let _ = build.join();
        }
    }

    /// Build a catalog store's name filter (`NameFilter`) after open, off the store mutex: the
    /// names in the catalog, read on a connection of its own, plus every name applied while the
    /// read ran — those held now (the resident and since-checkpoint names, seeded here under the
    /// mutex) and those created from now on (`NameFilter::note`). A failed read leaves the filter
    /// unbuilt: lookups keep asking the catalog.
    fn start_name_filter(&self) {
        let reader = {
            let mut inner = self.inner.lock();
            let StoreInner { cat, names, sync, .. } = &mut *inner;
            let Some(cat) = cat.as_ref() else {
                return;
            };
            if names.filter.built.is_some() {
                return;
            }
            let reader = match cat.catalog.writer(*sync) {
                Ok(reader) => reader,
                Err(e) => {
                    tracing::warn!("branch name filter not built: {e}");
                    return;
                }
            };
            let mut pending: HashSet<u64, BuildIdHasher> = HashSet::default();
            for name in names.map.keys().chain(names.fresh.keys()) {
                pending.insert(names.filter.hash(name));
            }
            names.filter.pending = Some(pending);
            reader
        };
        let hasher = self.inner.lock().names.filter.hasher.clone();
        let shared = self.inner.clone();
        let stop = self.name_filter_stop.clone();
        let spawned = crate::thread::Builder::new()
            .name("branch-name-filter".to_string())
            .spawn(move || {
                use std::hash::BuildHasher;
                let mut reader = reader;
                let scanned = reader.name_hashes(|name| hasher.hash_one(name), &stop);
                let mut inner = shared.lock();
                let filter = &mut inner.names.filter;
                let pending = filter.pending.take().unwrap_or_default();
                match scanned {
                    Ok(hashes) => {
                        let mut built: HashSet<u64, BuildIdHasher> = hashes.into_iter().collect();
                        built.extend(pending);
                        filter.built = Some(built);
                    }
                    Err(e) => tracing::debug!("branch name filter not built: {e}"),
                }
            });
        match spawned {
            Ok(handle) => *self.name_filter_build.lock() = Some(handle),
            Err(e) => {
                tracing::warn!("branch name filter thread not started: {e}");
                self.inner.lock().names.filter.pending = None;
            }
        }
    }

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

    /// githost-shape r3 instrument (observation only): `(len, nodes, bytes)` of F-W1's live-id set as the store
    /// holds it, or `None` before this process's first listing builds it. A walk under the store mutex,
    /// O(nodes): called only outside measured operations.
    pub(crate) fn live_id_census(&self) -> Option<(u64, u64, u64)> {
        let inner = self.inner.lock();
        inner.live_ids.as_ref().map(|set| {
            let (nodes, bytes) = set.census();
            (set.len(), nodes, bytes)
        })
    }

    pub(crate) fn read_counters(&self) -> (u64, u64) {
        (
            self.resolve_calls.load(Ordering::Relaxed),
            self.arena_reads.load(Ordering::Relaxed),
        )
    }

    /// Per-fork lock holds since open (fastest-engine M1 item 5).
    pub(crate) fn fork_holds(&self) -> super::ForkHolds {
        super::ForkHolds {
            store: self.holds.store.stats(),
            wal: self.holds.wal.stats(),
            locked_trunk_forks: self.holds.locked_trunk_forks.load(Ordering::Relaxed),
        }
    }

    /// Record one fork's holds: `store_ns` from [`take_counted_hold`], and a trunk fork's WAL hold.
    pub(crate) fn record_fork(&self, store_ns: u64, wal: Option<WalHold>) {
        self.holds.store.record(store_ns);
        if let Some(wal) = wal {
            self.holds.wal.record(wal.ns);
            if wal.locked {
                self.holds.locked_trunk_forks.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Lock the store mutex on a fork path, timing the hold (see [`Counted`]).
    fn lock_counted(&self) -> Counted<impl std::ops::DerefMut<Target = StoreInner> + '_> {
        let guard = self.inner.lock();
        Counted {
            guard,
            since: Instant::now(),
        }
    }

    pub(crate) fn stats(&self) -> Result<BranchStats> {
        self.refuse_if_trunk_only("branch statistics")?;
        let mut inner = self.inner.lock();
        // Covered deferred frees count as free, as the store before group commit freed them.
        self.mature(&mut inner);
        // The slot counts the log describes: parked Commits applied first (C-R).
        inner.settle()?;
        Ok(BranchStats {
            live_branches: inner.n_states as usize,
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner
                .arena
                .as_ref()
                .map_or(0, |a| a.high_water() as usize - a.in_use()),
            work: inner.work,
        })
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let mut inner = self.inner.lock();
        self.mature(&mut inner);
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
        self.mature(&mut inner);
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
        self.mature(&mut inner);
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
        let fail = inner.failpoint == Some(BranchFailpoint::CompactAfterRenameBeforeLogReset);
        if fail {
            inner.failpoint = None;
        }
        if inner.cat.is_none() && inner.failpoint.take_if(|f| *f == BranchFailpoint::ArenaSyncFails).is_some() {
            if let Some(journal) = inner.journal.as_mut() {
                journal.fail_next_arena_sync();
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

    /// A4.G's G-b red: `start_flight` called directly, past G-a (tests only).
    #[cfg(test)]
    pub(crate) fn start_flight_for_test(&self) -> bool {
        let mut inner = self.inner.lock();
        self.start_flight(&mut inner)
    }

    /// A4.G's G-c red: a FUZZY capture called directly, past G-a and G-b (tests only). A capture
    /// that succeeds is undone at once (its snapshot ended, its dirty set restored).
    #[cfg(test)]
    pub(crate) fn capture_fuzzy_for_test(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        let cap = inner.checkpoint_capture_mode(false, true, true)?;
        let cat = inner.cat.as_mut().expect("captured");
        cat.catalog.end_read_snapshot();
        cat.flight = false;
        for (id, what) in cap.dirty {
            *cat.dirty.entry(id).or_insert(0) |= what;
        }
        Ok(())
    }

    /// Fuzzy checkpoints refused because the store is in the splice arm (G-b; a census FINDING).
    pub(crate) fn fuzzy_refused_splice(&self) -> u64 {
        self.inner.lock().cat.as_ref().map_or(0, |c| c.ckpt.fuzzy_refused_splice)
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
        // G-a (r13-compose A2.R4a/A4.G): a splice-arm store takes no fuzzy checkpoint (S-12: a
        // splice's relink during a flight is untested). A runtime refusal, never a debug_assert.
        if inner.splice && !guard_mutant("a") {
            return Err(LimboError::InvalidArgument(
                "fuzzy checkpoint refused: splice arm (r13-compose S-12)".to_string(),
            ));
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
        // The name filter's build holds the store's files too: stopped, then joined.
        self.name_filter_stop.store(true, Ordering::Release);
        self.wait_name_filter();
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

fn name_taken(name: &str) -> LimboError {
    LimboError::InvalidArgument(format!(
        "branch name {name:?} already names an unreleased branch"
    ))
}

fn reaped(id: BranchId) -> LimboError {
    LimboError::InvalidArgument(format!("branch {} has been reaped", id.0))
}

impl StoreInner {
    fn fresh(files: Option<BranchFiles>, sync: SyncClass, default_lease: Option<Duration>) -> Self {
        Self::fresh_mode(files, sync, default_lease, false)
    }

    fn fresh_mode(
        files: Option<BranchFiles>,
        sync: SyncClass,
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
            branches: BranchTable::new(),
            failpoint: None,
            orphans: Vec::new(),
            lease: LeaseClock::new(),
            leases: BTreeSet::new(),
            default_lease,
            children: ChildIndex::default(),
            n_states: 0,
            catalog_mode,
            splice: false,
            cat: None,
            work: BranchWork::default(),
            derived_inserts: 0,
            parked: HashMap::new(),
            named_at: HashMap::new(),
            replay_pos: 0,
            deferred_freed: Vec::new(),
            parked_records: 0,
            parked_applied: 0,
            expire_more: false,
            shape: ShapeCounters::default(),
            pending_holders: HashSet::new(),
            resident_cap: None,
            live_ids: None,
            v4: V4Counters::default(),
            stamps: RowStamps::default(),
            merge_work: super::BranchMergeWork::default(),
            pending_free: VecDeque::new(),
            names: NameIndex::default(),
            trunk_commit_epoch: 0,
            fail_stop: Arc::new(AtomicBool::new(false)),
            files_dev: Arc::new(AtomicU64::new(NO_DEVICE)),
            last_fork_lsn: 0,
        }
    }

    /// Ruling 85a032f01's guard over the branch log and arena as opened (`one_device`), recording
    /// their device for `BranchStore::note_trunk_wal` (review 3 #3).
    fn files_device(&self, log: Option<u64>, arena: Option<u64>) -> Result<()> {
        super::journal::one_device(log, arena)?;
        self.files_dev.store(log.unwrap_or(NO_DEVICE), Ordering::Release);
        Ok(())
    }

    /// F-W1: the live-id set as the listing before it computed the list: every resident state that
    /// is not released, and every unreleased catalog row that is neither resident (memory knows
    /// better) nor removed since the last checkpoint. Once per process, under the mutex; its catalog
    /// rows are counted as `ids_build_rows`, apart from a listing's.
    fn build_live_ids(&mut self) -> Result<IdSet> {
        let mut set = IdSet::default();
        let scanned = std::cell::Cell::new(0u64);
        for (&id, st) in self.branches.iter_scanned(&scanned) {
            if !st.handle.is_released() {
                set.insert(id_key(id));
            }
        }
        self.shape.ids_resident_visited += self.branches.len() as u64;
        self.shape.walk_items_yielded += self.branches.len() as u64;
        self.shape.walk_slots_scanned += scanned.get();
        if let Some(cat) = self.cat.as_mut() {
            let rows = cat.catalog.unreleased_ids()?;
            self.shape.ids_build_rows += rows.len() as u64;
            for id in rows.into_iter().map(BranchId) {
                if !self.branches.contains_key(&id) && !cat.removed.contains(&id) {
                    set.insert(id_key(id));
                }
            }
        }
        Ok(set)
    }

    /// F-W1: `id` is listed from now on (a fork), if the set exists.
    fn live_ids_insert(&mut self, id: BranchId) {
        if let Some(set) = self.live_ids.as_mut() {
            set.insert(id_key(id));
        }
    }

    /// F-W1: `id` is not listed from now on (a release, durable or not), if the set exists.
    fn live_ids_remove(&mut self, id: BranchId) {
        if let Some(set) = self.live_ids.as_mut() {
            set.remove(id_key(id));
        }
    }

    /// githost-shape instrument (observing only), F8' meaning (r13-compose R4.9, S-9): the table's
    /// directory reallocations at this insert, and the chunk POINTERS they moved. No branch state
    /// moves under F8'; `capacity()` jumps by a whole chunk at every new chunk, so the hashbrown test
    /// this replaced would count every chunk allocation as a growth that moved every entry.
    fn note_table_growth(&mut self, before: (u64, u64)) {
        let (grows, moved) = self.branches.growth();
        self.shape.table_grows += grows - before.0;
        self.shape.table_moved += moved - before.1;
    }

    /// F-W3 (githost-shape lane, PREREG G5.3): evict clean branch states while more than
    /// `resident_cap` are resident. Called when a catalog checkpoint has committed, so the catalog
    /// holds every state that is not dirty. A state is clean when the catalog holds all of it and
    /// this process holds nothing of its own in it: not dirty, no parked Commit, handle Detached or
    /// Released (an attached handle is this process's), no connection, no write transaction, no
    /// reserved slot. What else it carries is derived and rebuilt on its next touch (its F4 page map
    /// from its parent, its child view, its schema), and its lease deadline is in the catalog's lease
    /// index, which `lease_floor` (just recomputed) covers. So an evicted state is exactly a state no
    /// one has touched since the open, and `ensure` reads it back the same way (on-demand recovery,
    /// Graefe and Sauer). Prior art: a buffer pool's eviction of clean pages (ARC, Megiddo and Modha,
    /// FAST 2003); the victims here are in table order, random replacement, since the claim is a
    /// bound on what stays resident, not a hit rate. The trunk-version read cache (clean copies of
    /// catalog rows) is dropped when it holds more than the cap.
    fn evict_clean(&mut self) {
        let Some(cap) = self.resident_cap else {
            return;
        };
        let Some(cat) = self.cat.as_mut() else {
            return;
        };
        if self.branches.len() > cap {
            let excess = self.branches.len() - cap;
            let parked = &self.parked;
            let mut yielded = 0u64;
            let scanned = std::cell::Cell::new(0u64);
            let victims: Vec<BranchId> = self
                .branches
                .iter_scanned(&scanned)
                .inspect(|_| yielded += 1)
                .filter(|(id, st)| {
                    !cat.dirty.contains_key(*id)
                        && !parked.contains_key(*id)
                        && matches!(st.handle, Handle::Detached | Handle::Released)
                        && !st.open
                        && !st.writer
                        && st.pending.is_empty()
                })
                .map(|(&id, _)| id)
                .take(excess)
                .collect();
            self.shape.walk_items_yielded += yielded;
            self.shape.walk_slots_scanned += scanned.get();
            victim_log(self.shape.checkpoints, &victims);
            for id in &victims {
                if let Some(st) = self.branches.remove(id) {
                    if let Some(deadline) = st.lease {
                        self.leases.remove(&(deadline, *id));
                    }
                    // The catalog holds its name (it is clean); a lookup reads it from there.
                    if let Some(name) = st.name {
                        if self.names.map.get(&name) == Some(id) {
                            self.names.map.remove(&name);
                        }
                    }
                }
            }
            // I5 (amendment 8): a victim that is the parent of a state STILL resident after this
            // eviction (its child's next cold touch re-derives through it: N2's amplification,
            // S-11); a child evicted in the same batch does not count. The survivors' walk is the
            // instrument's own, counted apart from I11 (review wf_5c230f31 L).
            let victim_set: HashSet<BranchId> = victims.iter().copied().collect();
            let mut with_child: HashSet<BranchId> = HashSet::new();
            for st in self.branches.values() {
                self.shape.instrument_walk_items += 1;
                if victim_set.contains(&st.parent) {
                    with_child.insert(st.parent);
                }
            }
            self.shape.evicted_with_resident_descendant += with_child.len() as u64;
            self.shape.evictions += 1;
            self.shape.evicted_states += victims.len() as u64;
        }
        let cached: usize = cat.trunk_cache.values().map(BTreeMap::len).sum();
        if cached > cap {
            cat.trunk_cache.clear();
        }
    }

    /// The records a fork writes — the fork, and the default lease if there is one, flushed
    /// together so a fork is never durable without the lease it was given — and that lease's
    /// `(deadline, now)`, which the caller applies after the flush.
    /// A NAMED server branch (fastest-engine M1 item 4) is forked by one `ForkNamed` record and
    /// is never given the default lease.
    fn fork_records(
        &self,
        child: BranchId,
        parent: BranchId,
        now: u64,
        name: Option<&str>,
    ) -> (Vec<Record>, Option<(u64, u64)>) {
        // Mutant M-d (PREREG v1 amendment 36): the fork record written without its parent (as the
        // trunk's child, whatever the parent was).
        let logged_parent = if fe_mutant("fork_without_parent") { 0 } else { parent.0 };
        let mut records = vec![match name {
            None => Record::Fork {
                child: child.0,
                parent: logged_parent,
            },
            Some(name) => Record::ForkNamed {
                child: child.0,
                parent: logged_parent,
                name: name.to_string(),
            },
        }];
        let lease = self
            .default_lease
            .filter(|_| name.is_none())
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
        // fastest-engine mutant `split_fail_stop` (test builds only, with `Journal::share_fail_stop`'s
        // half): the store reads only the journal's flag, as before review B-F1.
        let shared = self.fail_stop.load(Ordering::Acquire) && !fe_mutant("split_fail_stop");
        shared || self.journal.as_ref().is_some_and(|j| j.is_poisoned())
    }

    /// Refuse a read of branch `id` if a failed commit left it in doubt (`BranchState::in_doubt`).
    fn refuse_in_doubt(&self, id: BranchId) -> Result<()> {
        match self.branches.get(&id) {
            Some(st) if st.in_doubt => Err(in_doubt(id)),
            _ => Ok(()),
        }
    }

    /// A fork just applied is durable once `lsn` is (see `BranchState::fork_lsn`).
    fn note_fork_lsn(&mut self, id: BranchId, lsn: u64) {
        if let Some(st) = self.branches.get_mut(&id) {
            st.fork_lsn = lsn;
        }
        self.last_fork_lsn = self.last_fork_lsn.max(lsn);
    }

    /// The unreleased branch named `name` (fastest-engine M1 item 4): the in-memory index, then —
    /// unless a release since the last checkpoint freed the name — the catalog.
    fn name_lookup(&mut self, name: &str) -> Result<Option<BranchId>> {
        // Mutant M-i (PREREG v1 amendment 36): uniqueness checked against the CHECKPOINTED names
        // only, outside what the log has made durable since.
        if !fe_mutant("name_check_outside") {
            if let Some(&id) = self.names.map.get(name) {
                return Ok(Some(id));
            }
            if self.names.gone.contains_key(name) {
                return Ok(None);
            }
            // Never held by any branch: no catalog query (lead review 1 item 2).
            if self.names.filter.says_absent(name) {
                return Ok(None);
            }
        }
        match self.cat.as_mut() {
            Some(cat) => Ok(cat.catalog.name_get(name)?.map(BranchId)),
            None => Ok(None),
        }
    }

    /// A lease is outstanding: on a resident branch, or on a catalog row not loaded. (r11-restart-r2:
    /// a reopened catalog store holds no resident branch until one is touched, and with `leases`
    /// alone its trunk commits and its close never stamped the lease clock, so a crash lost all the
    /// time since open — the safe direction, leases only lengthen, but the clock stood still.)
    fn leases_exist(&self) -> bool {
        !self.leases.is_empty() || self.cat.as_ref().is_some_and(|c| c.lease_floor.is_some())
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
        if !chain.is_empty() {
            let n = chain.len() as u64;
            self.shape.ensure_cold += 1;
            self.shape.ensure_chain_sum += n;
            self.shape.ensure_chain_max = self.shape.ensure_chain_max.max(n);
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
        if ids.is_empty() {
            return Ok(());
        }
        let loads0 = self.cat.as_ref().map_or(0, |c| c.branch_loads);
        for id in ids {
            if !self.ensure(id)? {
                return Err(LimboError::Corrupt(format!(
                    "branch log replay: branch {} named by a Commit is not in the catalog",
                    id.0
                )));
            }
        }
        let loads = self.cat.as_ref().map_or(0, |c| c.branch_loads) - loads0;
        self.shape.settle_sharp_calls += 1;
        self.shape.settle_sharp_loads += loads;
        self.shape.settle_sharp_max_loads = self.shape.settle_sharp_max_loads.max(loads);
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
        let current: HashMap<u32, Owned> = b
            .current
            .into_iter()
            .map(|(page, slot, born, crc)| (page, Owned { slot, born, crc }))
            .collect();
        let current_by_born = current.iter().map(|(&page, o)| (o.born, page)).collect();
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
        let grown_before = self.branches.growth();
        self.branches.insert(
            id,
            BranchState {
                parent,
                fork_epoch: b.fork_epoch,
                lineage,
                current,
                current_by_born,
                pending: HashMap::new(),
                schema: None,
                handle: if b.released {
                    Handle::Released
                } else {
                    Handle::Detached
                },
                // Held until its `Close` in the log's tail, or the end of recovery.
                open: b.held_open,
                writer: false,
                lease,
                trunk_at,
                inherited,
                inherited_at: b.fork_epoch,
                view: None,
                name: None,
                fork_lsn: 0,
                in_doubt: false,
            },
        );
        self.note_table_growth(grown_before);
        // A resident named branch's name is in the index, as every created one is.
        if let Some(name) = b.name.filter(|_| !b.released) {
            let name: Arc<str> = Arc::from(name);
            self.names.map.insert(name.clone(), id);
            if let Some(st) = self.branches.get_mut(&id) {
                st.name = Some(name);
            }
        }
    }

    /// Catalog stores: make the trunk's `written` epoch of `page` at least the `died` of the page's
    /// last catalog version, as an eager recovery rebuilds it (the largest `died`), by ONE probe per
    /// page per process. The page's other versions are not read (C-P). The page counts as known
    /// only once its probe has answered: a probe refused (`Busy`, an I/O error) is made again by the
    /// next caller, never taken as "no catalog version" (review 3 #2).
    fn trunk_written_known(&mut self, page: u32) -> Result<()> {
        let Some(cat) = self.cat.as_mut() else {
            return Ok(());
        };
        if cat.trunk_known.contains(&page) {
            return Ok(());
        }
        // Mutant `probe_marked_first` (test builds only): as before review 3 #2, the page is known
        // before its probe answers.
        if fe_mutant("probe_marked_first") {
            cat.trunk_known.insert(page);
        }
        if self.failpoint == Some(BranchFailpoint::TrunkProbeBusy) {
            self.failpoint = None;
            return Err(LimboError::Busy);
        }
        cat.trunk_probes += 1;
        cat.twk_probes += 1;
        let reads0 = super::page_io()[0];
        let pred = cat.catalog.trunk_pred(page, u64::MAX);
        cat.twk_reads += super::page_io()[0] - reads0;
        if let Some((born, died, slot, crc)) = pred? {
            cat.trunk_rows += 1;
            cat.twk_rows += 1;
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
        cat.trunk_known.insert(page);
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
        cat.tva_probes += 1;
        let reads0 = super::page_io()[0];
        let pred = cat.catalog.trunk_pred(page, at);
        cat.tva_reads += super::page_io()[0] - reads0;
        let Some((born, died, slot, crc)) = pred? else {
            return Ok(None);
        };
        cat.trunk_rows += 1;
        cat.tva_rows += 1;
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
                    // The arm's format version, in the header `start` writes (amendment 15).
                    journal.set_format(super::journal::format_version(self.splice));
                    journal.share_fail_stop(&self.fail_stop);
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
                        format: super::journal::format_version(self.splice),
                        ..Meta::default()
                    };
                    if let Err(e) = catalog.put_meta(&meta).and_then(|()| catalog.commit()) {
                        catalog.rollback();
                        return Err(e);
                    }
                    self.cat = Some(CatState::new(catalog, self.sync, 0)?);
                    // A new catalog holds no name: the filter is built, empty, at once.
                    if self.names.filter.built.is_none() && self.names.filter.pending.is_none() {
                        let mut built = HashSet::default();
                        for name in self.names.map.keys() {
                            built.insert(self.names.filter.hash(name));
                        }
                        self.names.filter.built = Some(built);
                    }
                }
                let arena = Arena::open_file(&files.arena, page_size, true, &[])?;
                self.files_device(self.journal.as_ref().and_then(Journal::device), arena.device())?;
                // The journal's create synced the directory before the arena file existed.
                if self.sync.syncs() {
                    super::journal::fsync_dir_of(&files.arena, self.sync)?;
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
            self.restarted_arena(Arena::new(page_size));
            return Ok(());
        };
        if self.cat.is_some() {
            // The catalog's meta row and the log header take the new page size in one checkpoint;
            // the arena is truncated only after it committed.
            let old = self.journal.as_ref().map(Journal::page_size);
            if let Some(journal) = self.journal.as_mut() {
                journal.set_page_size(page_size);
            }
            if let Err(e) = self.checkpoint_catalog(false) {
                if let (Some(journal), Some(old)) = (self.journal.as_mut(), old) {
                    journal.set_page_size(old);
                }
                return Err(e);
            }
            match Arena::open_file(&files.arena, page_size, true, &[]) {
                Ok(arena) => self.restarted_arena(arena),
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
            Ok(arena) => self.restarted_arena(arena),
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

    /// The restarted empty store's arena replaces the old one, and with it go the frees deferred
    /// for releases whose flights had not landed (skill review 1 (f)): their slots were the old
    /// arena's, and the empty state the restart wrote made those releases durable. Mutant
    /// `restart_keeps_deferred` (test builds only).
    fn restarted_arena(&mut self, arena: Arena) {
        self.arena = Some(arena);
        if !fe_mutant("restart_keeps_deferred") {
            self.pending_free.clear();
        }
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
    fn checkpoint_catalog(&mut self, fail_after_commit: bool) -> Result<()> {
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
        let mut cap = self.checkpoint_capture(fail_after_commit)?;
        kill_point("ckpt.captured");
        let mut w = writer.lock();
        let w0 = w.counters.queries;
        let r0 = w.counters.rows_written;
        let written = checkpoint_write(&mut w, &cap, None);
        kill_point("ckpt.written");
        let wrote = w.counters.queries - w0;
        cap.shape_rows_written = w.counters.rows_written - r0;
        let installed = self.checkpoint_install(cap, written, None);
        kill_point("ckpt.installed");
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
    fn checkpoint_capture(&mut self, fail_after_commit: bool) -> Result<Box<Captured>> {
        // The sharp path quiesced first: no flight is in the air.
        self.checkpoint_capture_mode(fail_after_commit, false, false)
    }

    /// G-c (r13-compose A2.R4a): the capture knows whether it serves a fuzzy checkpoint, and a fuzzy
    /// capture of a splice-arm store returns `Err` in every build (sharp checkpoints legitimately
    /// run capture, so the refusal is keyed on both).
    fn checkpoint_capture_mode(
        &mut self,
        fail_after_commit: bool,
        fuzzy: bool,
        flight_in_air: bool,
    ) -> Result<Box<Captured>> {
        #[cfg(test)]
        CAPTURE_ENTERED.with(|c| c.set(c.get() + 1));
        if fuzzy && self.splice && !guard_mutant("c") {
            return Err(LimboError::InvalidArgument(
                "fuzzy capture refused: splice arm (r13-compose S-12)".to_string(),
            ));
        }
        let now = self.lease.now_ms();
        let (Some(journal), Some(arena), Some(cat)) =
            (self.journal.as_mut(), self.arena.as_mut(), self.cat.as_mut())
        else {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint needs its log, arena and catalog".to_string(),
            ));
        };
        // githost-shape instrument (observing only).
        let started = Instant::now();
        journal.check_live()?;
        if !self.parked.is_empty() || cat.flight {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint started with parked commits or another in flight"
                    .to_string(),
            ));
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
                held_open: st.handle == Handle::Released && st.open,
                lease: if st.handle == Handle::Released { None } else { st.lease },
                n_children: st.lineage.n_children,
                current: st
                    .current
                    .iter()
                    .map(|(&page, o)| (page, o.slot, o.born, o.crc))
                    .collect(),
                retained: st.lineage.retained_list(),
                name: None,
            }, what))
            .collect();
        // The trunk: the versions retained since the last checkpoint (all in memory, none in the
        // catalog) are inserted, and the catalog versions reaped since are deleted, one row each.
        let trunk_new = self.trunk.lineage.retained_list();
        let trunk_gone: Vec<(u32, u64)> = cat.trunk_gone.iter().copied().collect();
        let (n_rows, n_trunk_new, n_trunk_gone) =
            (rows.len() as u64, trunk_new.len() as u64, trunk_gone.len() as u64);
        // Slots reserved by open write transactions are named by no durable state: the catalog
        // lists them free (a crash frees them), and this process keeps them as taken.
        // F-W2: only a branch that reserved a slot since the last prune can hold one, so the walk
        // visits those alone; the ones holding none any more (committed, rolled back, closed,
        // collected) leave the set here. githost-shape instrument (observing only): F-W2's walk
        // counts the holders it visits; under I2 the base's walk counts every resident state and
        // nothing else (the holder set is still kept, uncounted, so it stays exact; r13-compose
        // review wf_5c230f31 H1: counting both double-counted the holders written since the last
        // checkpoint).
        let fw2_off = knob_off("fw2");
        // r13_fw2_double_count (amendment 8.6): H1's double count, as I2's red evidence.
        if !fw2_off || mutant("r13_fw2_double_count") {
            self.shape.ckpt_states_walked += self.pending_holders.len() as u64;
        }
        let branches = &self.branches;
        self.pending_holders
            .retain(|id| branches.get(id).is_some_and(|st| !st.pending.is_empty()));
        let reserved: Vec<Slot> = if fw2_off {
            // I2: the store before F-W2: every resident state is walked for reserved slots.
            self.shape.ckpt_states_walked += branches.len() as u64;
            branches.values().flat_map(|st| st.pending.values().copied()).collect()
        } else {
            self.pending_holders
                .iter()
                .flat_map(|id| branches[id].pending.values().copied())
                .collect()
        };
        // ARIES's begin-checkpoint record: the capture covers the log up to and including it.
        if let Err(e) = journal.buffer(&Record::Checkpoint { generation }) {
            cat.dirty = dirty;
            return Err(e);
        }
        let log_from = journal.mark();
        let deferred: Vec<Slot> = self.pending_free.iter().flat_map(|(_, s)| s.iter().copied()).collect();
        let deferred_lsn = journal.lsn();
        // fastest-engine mutant `deferred_matured_at_capture` (test builds only): the capture hands
        // the deferred frees to the allocator at once, before their releases are durable (rule 2).
        if fe_mutant("deferred_matured_at_capture") {
            for (_, slots) in std::mem::take(&mut self.pending_free) {
                for slot in slots {
                    arena.release(slot);
                }
            }
        }
        let meta = Meta {
            generation,
            page_size: journal.page_size() as u32,
            next_id: self.next_id,
            trunk_epoch: self.trunk.lineage.epoch,
            trunk_children: self.trunk.lineage.n_children,
            lease_now_ms: now,
            arena_hw: arena.high_water(),
            in_use: (arena.in_use() - reserved.len() - deferred.len()) as u64,
            states: self.n_states,
            format: super::journal::format_version(self.splice),
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
        // The arena is synced before the catalog commit only if a slot was written since its last
        // sync, or a flight that took its last unsynced writes may not have synced them yet (lead
        // review 1 item 7(4)): a clean arena costs the checkpoint no sync.
        let rewrite_syncs = journal.rewrite_class().syncs();
        let arena_file = if rewrite_syncs && (arena.is_dirty() || flight_in_air) {
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
        Ok(Box::new(Captured {
            generation,
            log_from,
            rows,
            dirty,
            removed: cat.removed.iter().copied().collect(),
            trunk_new,
            trunk_gone,
            free_cursor: cat.free_cursor,
            taken: cat.taken.iter().copied().collect(),
            free_list: arena.free_list().to_vec(),
            reserved,
            deferred,
            deferred_lsn,
            meta,
            child_keys: self.children.map.keys().copied().collect(),
            child_removed: self.children.removed.keys().copied().collect(),
            arena: arena_file,
            // Every record the catalog takes over stays as durable as it was (review B-F3).
            arena_sync: self.journal.as_ref().map_or(self.sync, Journal::rewrite_class),
            log_class: self.journal.as_ref().map_or(self.sync, Journal::sync_class),
            fail_stop: self.fail_stop.clone(),
            // fastest-engine mutant `names_taken_at_capture` (test builds only): as before review
            // C-F1, the capture takes the names out of the index.
            names_gone: if fe_mutant("names_taken_at_capture") {
                std::mem::take(&mut self.names.gone)
            } else {
                self.names.gone.clone()
            },
            names_fresh: self.names.fresh.clone(),
            lease_now: now,
            fail_after_commit,
            fail_write: self
                .failpoint
                .take_if(|f| *f == BranchFailpoint::CheckpointWriteFails)
                .is_some(),
            fail_arena_sync: arena_file.is_some()
                && self.failpoint.take_if(|f| *f == BranchFailpoint::ArenaSyncFails).is_some(),
            shape_started: started,
            shape_rows: n_rows,
            shape_trunk_new: n_trunk_new,
            shape_trunk_gone: n_trunk_gone,
            shape_rows_written: 0,
        }))
    }

    /// F-FZ phase 3, under the store mutex: end the pinned snapshot and, if the writer committed,
    /// cut the log to what follows the capture and take out of memory exactly what the catalog now
    /// holds — nothing that changed since the capture. If it did not commit, what the capture swapped
    /// out is dirty again and nothing else has changed.
    fn checkpoint_install(
        &mut self,
        cap: Box<Captured>,
        written: Result<()>,
        prepared: Option<super::journal::CutPrep>,
    ) -> Result<()> {
        let Some(cat) = self.cat.as_mut() else {
            return written;
        };
        let q0 = cat.catalog.counters.queries;
        cat.catalog.end_read_snapshot();
        cat.flight = false;
        if let Err(e) = written {
            // D-T2's mutant r13_install_err_drops_dirty: a failed write forgets the captured dirt.
            if !mutant("r13_install_err_drops_dirty") {
                for (id, what) in cap.dirty {
                    *cat.dirty.entry(id).or_insert(0) |= what;
                }
            }
            // The capture copied the names; the index still holds them, for the next checkpoint.
            // Not retried before another threshold's worth of log (review 2 #5).
            if let Some(journal) = self.journal.as_mut() {
                journal.defer_compaction();
            }
            return Err(e);
        }
        cat.generation = cap.generation;
        cat.ckpt.count += 1;
        // The catalog holds the captured names now: drop each entry no release or create since
        // has moved (see `NameIndex`).
        for (name, release) in &cap.names_gone {
            if self.names.gone.get(name) == Some(release) {
                self.names.gone.remove(name);
            }
        }
        for (name, id) in &cap.names_fresh {
            if self.names.fresh.get(name) == Some(id) {
                self.names.fresh.remove(name);
            }
        }
        // From here the catalog is the truth up to the capture's log position.
        let Some(journal) = self.journal.as_mut() else {
            return Err(LimboError::InternalError(
                "a branch catalog checkpoint lost its log".to_string(),
            ));
        };
        if cap.fail_after_commit {
            journal.defer_compaction();
            journal.poison();
            return Err(LimboError::InternalError(
                "failpoint: branch checkpoint stopped after the catalog commit".to_string(),
            ));
        }
        // A failure before its rename leaves the old log in use, whose checkpoint marker says
        // where recovery cuts it: correct, only longer. The install below must happen either way.
        let rewritten = match prepared {
            Some(prep) => journal.finish_cut(prep, cap.log_from, cap.generation),
            None => journal.rewrite_from(cap.log_from, cap.generation),
        };
        if rewritten.is_err() {
            // The old log stays, correct and longer; not cut again before another threshold's
            // worth of it (review 2 #5).
            journal.defer_compaction();
        }
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
            // The deferred frees the catalog now lists, still waiting for their flight: free in the
            // catalog, so out of memory (their releases are durable with it). One that matured
            // since the capture is on the in-memory list, and leaves it below like any listed slot.
            let mut still_deferred: HashSet<Slot> = HashSet::new();
            while self
                .pending_free
                .front()
                .is_some_and(|&(lsn, _)| lsn <= cap.deferred_lsn)
            {
                let (_, slots) = self.pending_free.pop_front().expect("just looked");
                for slot in slots {
                    arena.forget_listed(slot);
                    still_deferred.insert(slot);
                }
            }
            let listed: HashSet<Slot> = cap
                .free_list
                .iter()
                .chain(cap.reserved.iter())
                .chain(cap.deferred.iter())
                .copied()
                .filter(|slot| !still_deferred.contains(slot))
                .collect();
            cat.taken = arena.remove_free(&listed).into_iter().collect();
        }
        cat.free_cursor = None;
        cat.free_exhausted = false;
        cat.lease_floor = cat.catalog.lease_min()?;
        cat.ckpt.stmts_locked += cat.catalog.counters.queries - q0;
        self.lease.durable_at_least(cap.lease_now);
        // githost-shape instrument (observing only; r13-compose S-1): counted from the capture,
        // with the rows the WRITER connection wrote (`cat.catalog`, the reader, writes none).
        // `checkpoint_ns` spans capture to install, the writer's thread included on the fuzzy path.
        self.shape.checkpoint_ns +=
            u64::try_from(cap.shape_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.shape.checkpoints += 1;
        self.shape.ckpt_trunk_inserted += cap.shape_trunk_new;
        self.shape.ckpt_trunk_deleted += cap.shape_trunk_gone;
        self.shape.ckpt_branch_rows += cap.shape_rows;
        self.shape.ckpt_rows_written += cap.shape_rows_written;
        // F-W3: the catalog now holds every state that is not dirty. A state changed since the
        // capture is dirty again, so `evict_clean` keeps it (r13-compose S-2b). Only after the log
        // was cut: a failed rewrite leaves the old log, whose replay is correct, and nothing evicted.
        if rewritten.is_ok() {
            self.evict_clean();
        }
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

    /// Hold `freed` until the records that freed them — up to `lsn` — are durable (rule 2 under
    /// early release; see [`Group`]). A volatile store frees at once.
    fn defer_frees(&mut self, lsn: u64, freed: Vec<Slot>) {
        // Mutant M-g (PREREG v1 amendment 36): slots freed before the record that frees them is
        // durable.
        if self.journal.is_none() || lsn == 0 || fe_mutant("free_before_durable") {
            self.release_slots(freed);
            return;
        }
        if !freed.is_empty() {
            self.pending_free.push_back((lsn, freed));
        }
    }

    /// Return to the arena every deferred free a flight has covered.
    fn mature_frees(&mut self, durable: u64) {
        while self
            .pending_free
            .front()
            .is_some_and(|&(lsn, _)| lsn <= durable)
        {
            let (_, freed) = self.pending_free.pop_front().expect("just looked");
            self.release_slots(freed);
        }
    }

    fn release_slots(&mut self, freed: Vec<Slot>) {
        if freed.is_empty() {
            return;
        }
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
        name: Option<Arc<str>>,
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
            let (view, built) = st.view_now();
            let inherited = view.clone();
            let trunk_at = st.trunk_at;
            self.work.view_build_entries += built;
            (f, inherited, trunk_at)
        };
        self.children.insert(parent, f, child);
        self.next_id = self.next_id.max(child.0 + 1);
        let grown_before = self.branches.growth();
        self.branches.insert(
            child,
            BranchState {
                parent,
                fork_epoch: f,
                // Epoch inheritance (F7 durable port): the child's epochs start above its fork
                // epoch, so every version it can inherit in a splice was born below all of them.
                lineage: Lineage {
                    epoch: f + 1,
                    ..Lineage::default()
                },
                current: HashMap::new(),
                current_by_born: BTreeSet::new(),
                pending: HashMap::new(),
                schema,
                handle,
                open: false,
                writer: false,
                lease: None,
                trunk_at,
                inherited,
                inherited_at: f,
                view: None,
                name: name.clone(),
                fork_lsn: 0,
                in_doubt: false,
            },
        );
        self.note_table_growth(grown_before);
        if let Some(name) = name {
            self.names.filter.note(&name);
            self.names.map.insert(name.clone(), child);
            if self.cat.is_some() || self.catalog_mode {
                self.names.fresh.insert(name, child);
            }
        }
        // F-W1: a new branch is listed (no fork is ever of a released branch).
        self.live_ids_insert(child);
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
            if let Some(old) = st.set_current(page, new) {
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

    /// Returns whether `id` was spliced out (see `collect`).
    fn apply_release(&mut self, id: BranchId, freed: &mut Vec<Slot>) -> Result<bool> {
        self.ensure(id)?;
        let mut name = None;
        if let Some(st) = self.branches.get_mut(&id) {
            st.handle = Handle::Released;
            if let Some(deadline) = st.lease.take() {
                self.leases.remove(&(deadline, id));
            }
            name = st.name.take();
        }
        // fastest-engine M1 item 4: a release frees its name at once.
        if let Some(name) = name {
            if self.names.map.get(&name) == Some(&id) {
                self.names.map.remove(&name);
            }
            if self.names.fresh.get(&name) == Some(&id) {
                self.names.fresh.remove(&name);
            }
            if self.cat.is_some() || self.catalog_mode {
                self.names.releases += 1;
                self.names.gone.insert(name, self.names.releases);
            }
        }
        // F-W1: a released branch is not listed.
        self.live_ids_remove(id);
        self.mark_dirty(id, DIRTY_ROW);
        self.collect(id, freed)
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist; a released `id` that still has live children is retired instead (see
    /// `BranchState::retire_current`), and spliced out if that leaves it exactly one (see `splice`).
    /// Every freed slot goes to `freed`. Returns whether `id` itself was spliced: gone from the store
    /// like a branch freed whole, but its versions live on in its child, so its release was deferred
    /// (the volatile store's `collect` reports the same).
    fn collect(&mut self, mut id: BranchId, freed: &mut Vec<Slot>) -> Result<bool> {
        let first = id;
        loop {
            if !self.ensure(id)? {
                return Ok(false);
            }
            let Some(st) = self.branches.get_mut(&id) else {
                return Ok(false);
            };
            // Exactly `Released`: a `ReleasePending` branch's release is not durable and nothing of
            // it is ever freed here (review R1).
            if st.handle != Handle::Released || st.open {
                return Ok(false);
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
                let one = st.lineage.n_children == 1;
                self.mark_dirty(id, DIRTY_CUR | DIRTY_RET);
                // F7 durable port, in the splice arm: with exactly one live child left, it is
                // spliced out. Off, it stays retired, as on the base.
                if one && self.splice {
                    self.splice(id, freed)?;
                    return Ok(id == first);
                }
                return Ok(false);
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
                return Ok(false);
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

    /// F7 durable port (UNBUILT) of the volatile store's splice (turso r11-ever fad7db24d, F7.1
    /// a85f41ab2, F7' d9be3f03a; the port onto the pre-F1 store is r11-ever-durable 0905219cd): take
    /// the released zombie `zid` — resident, no open connection, retired, exactly one live child
    /// `c` — out of the tree; `c` takes its place under its parent at the same fork epoch and
    /// inherits the versions it reads through it.
    ///
    /// 1. U7 first: `c` is made resident (`ensure`, which applies its parked Commits) before any
    ///    map moves; a catalog store may not have read it yet, and a Commit of it may be parked.
    /// 2. `retire_current` has freed every current version born after `c`'s fork `f`, so the
    ///    zombie's current versions are exactly the ones `c` reads of their pages; each retained
    ///    version holds `f` (`child_gone` frees what no live child's fork lies inside), at most one
    ///    per page, on a page whose current version was born after `f` and is gone: it becomes
    ///    current.
    /// 3. Those versions are merged into `c`'s own by iterating the SMALLER side and probing the
    ///    larger (union by size). A zombie version of a page `c` has any version of — current, or
    ///    retained — stays for `c`'s children forked before `c`'s first own version of that page,
    ///    as a retained version of `c` OLDER than `c`'s others (U5: `Lineage::retain` checks both
    ///    neighbours), or is freed when there are none. Epoch inheritance puts every zombie version
    ///    `c` reads below every epoch of `c`, so no key changes. `c`'s page maps are unchanged:
    ///    `inherited` (and `view`) already name these same slots for these pages, and the moved
    ///    versions are born at or below `inherited_at`, so `view_now` does not re-insert them.
    /// 4. U6: `c` is re-keyed under the zombie's parent at the zombie's fork epoch (`relink`), and a
    ///    catalog store removes the zombie and re-keys `c`'s row at its next checkpoint.
    ///
    /// Cost: step 2 is the zombie's retained versions, each made by one of its own commits; step 3
    /// the smaller side; plus the child-index lookups. `BranchWork::splices`, `splice_commits`,
    /// `splice_entries` count them (observation only).
    fn splice(&mut self, zid: BranchId, freed: &mut Vec<Slot>) -> Result<()> {
        let (zp, zf) = {
            let z = &self.branches[&zid];
            crate::turso_assert!(
                z.handle == Handle::Released
                    && !z.open
                    && z.pending.is_empty()
                    && z.lineage.n_children == 1,
                "spliced a branch that is not a released zombie with one live child"
            );
            (z.parent, z.fork_epoch)
        };
        let listed = |what: &str| {
            LimboError::Corrupt(format!(
                "branch {} counts one live child and the child index {what}",
                zid.0
            ))
        };
        let (cf, cid) = {
            let StoreInner { children, cat, .. } = &mut *self;
            let mut catalog = cat.as_mut().map(|c| &mut c.catalog);
            let cf = children
                .lowest(catalog.as_deref_mut(), zid)?
                .ok_or_else(|| listed("lists none"))?;
            let cid = children
                .child_at(catalog, zid, cf)?
                .ok_or_else(|| listed("names no branch at its epoch"))?;
            (cf, cid)
        };
        // 1. U7: the child resident, its parked Commits applied, before any map moves.
        if !self.ensure(cid)? {
            return Err(listed("names a missing branch"));
        }
        let mut z = self.branches.remove(&zid).expect("the zombie is resident");
        crate::turso_assert!(
            z.current_by_born.last().is_none_or(|&(born, _)| born <= cf),
            "spliced a zombie that was not retired: it holds a version its only child cannot read"
        );
        let mut visited = 0u64;
        // 2. The retained versions become current.
        let retained = std::mem::take(&mut z.lineage.retained);
        z.lineage.by_born.clear();
        z.lineage.by_died.clear();
        for (page, versions) in retained {
            crate::turso_assert!(
                versions.len() == 1,
                "two retained versions of one page hold the only child's fork epoch"
            );
            let (_, v) = versions.into_iter().next().expect("one version");
            crate::turso_assert!(
                v.born <= cf && cf < v.died && !z.current.contains_key(&page),
                "a zombie's retained version does not hold its only child's fork epoch"
            );
            z.set_current(
                page,
                Owned {
                    slot: v.slot,
                    born: v.born,
                    crc: v.crc,
                },
            );
            visited += 1;
        }
        // 3. Merge into the child, smaller side iterated.
        /// A zombie version of a page the child has a version of: kept for the child's children
        /// forked before the child's first own version, else freed.
        #[allow(clippy::too_many_arguments)]
        fn shadowed(
            children: &ChildIndex,
            cat: Option<&mut Catalog>,
            cid: BranchId,
            c: &mut BranchState,
            page: u32,
            zo: Owned,
            first: u64,
            freed: &mut Vec<Slot>,
        ) -> Result<()> {
            if children.any_in(cat, cid, zo.born, first)? {
                c.lineage.retain(
                    page,
                    Retained {
                        born: zo.born,
                        died: first,
                        slot: zo.slot,
                        crc: zo.crc,
                    },
                );
            } else {
                freed.push(zo.slot);
            }
            Ok(())
        }
        // The child's first own version of `page`, if it has any (a retired child can hold retained
        // versions of a page and no current one).
        let first_own = |c: &BranchState, page: u32| -> Option<u64> {
            let retained = c.lineage.retained.get(&page).and_then(|v| v.first_key_value());
            match (c.current.get(&page).map(|o| o.born), retained.map(|(&b, _)| b)) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            }
        };
        let commit = {
            let StoreInner {
                branches,
                children,
                cat,
                ..
            } = &mut *self;
            let mut catalog = cat.as_mut().map(|x| &mut x.catalog);
            let c = branches.get_mut(&cid).expect("made resident above");
            crate::turso_assert!(c.parent == zid && c.fork_epoch == cf, "child link mismatch");
            let c_pages = c.current.len() + c.lineage.retained.len();
            let commit = z.current.len() > c_pages;
            if !commit {
                // Stream: the zombie's versions into the child.
                z.current_by_born.clear();
                for (page, zo) in std::mem::take(&mut z.current) {
                    visited += 1;
                    match first_own(c, page) {
                        // r13-compose D-T3's mutant: the zombie's versions are not streamed into
                        // the child's current (the in-memory child still reads them through
                        // `inherited`; its merge loses them).
                        None if mutant("r13_splice_no_stream") => {}
                        None => {
                            c.set_current(page, zo);
                        }
                        Some(first) => shadowed(
                            children,
                            catalog.as_deref_mut(),
                            cid,
                            c,
                            page,
                            zo,
                            first,
                            freed,
                        )?,
                    }
                }
            } else {
                // Commit: the child's versions into the zombie's map, which then becomes the child's.
                let own: Vec<u32> = c
                    .current
                    .keys()
                    .chain(c.lineage.retained.keys())
                    .copied()
                    .collect();
                for page in own {
                    visited += 1;
                    if let Some(zo) = z.current.remove(&page) {
                        z.current_by_born.remove(&(zo.born, page));
                        let first = first_own(c, page).expect("the child has a version of its own page");
                        shadowed(
                            children,
                            catalog.as_deref_mut(),
                            cid,
                            c,
                            page,
                            zo,
                            first,
                            freed,
                        )?;
                    }
                }
                // One insert per child entry (the volatile store's F7.1: an append rebuilds both).
                for (page, co) in c.current.drain() {
                    z.current.insert(page, co);
                }
                for entry in std::mem::take(&mut c.current_by_born) {
                    z.current_by_born.insert(entry);
                }
                std::mem::swap(&mut c.current, &mut z.current);
                std::mem::swap(&mut c.current_by_born, &mut z.current_by_born);
            }
            // 4. U6: the child takes the zombie's place, in the index as in the lineage.
            c.parent = zp;
            c.fork_epoch = zf;
            children.relink(zid, cf, zp, zf, cid);
            if let Some(cat) = cat.as_mut() {
                cat.dirty.remove(&zid);
                cat.removed.insert(zid);
            }
            commit
        };
        self.mark_dirty(cid, DIRTY_KEY | DIRTY_CUR | DIRTY_RET);
        self.n_states -= 1;
        self.work.splices += 1;
        self.work.splice_commits += u64::from(commit);
        self.work.splice_entries += visited;
        Ok(())
    }

    /// The record that releases `id`: `ReleaseOpen` while a connection holds it (replay must hold
    /// it until that connection's `Close`, as the live store does), else `Release`.
    fn release_record(&self, id: BranchId) -> Record {
        if self.branches.get(&id).is_some_and(|st| st.open) {
            Record::ReleaseOpen { branch: id.0 }
        } else {
            Record::Release { branch: id.0 }
        }
    }

    /// The end of recovery (F7 durable port): nothing is open after a restart, so every branch a
    /// snapshot (`held_open`), a catalog row (`released = 2`) or a replayed `ReleaseOpen` left held is
    /// closed here, in id order, and collected; each close is buffered in `journal` as a `Close`
    /// (the caller flushes it before the open returns), so the next recovery collects it at this same
    /// point and replays this session's records on the tree they were made on. A released branch no
    /// connection held was collected at its release or its close, so nothing else is collected here.
    /// Returns the branches examined, and whether a `Close` was buffered.
    fn close_held(&mut self, journal: &mut Journal, freed: &mut Vec<Slot>) -> Result<(u64, bool)> {
        let mut ids: Vec<BranchId> = self
            .branches
            .iter()
            .filter(|(_, st)| st.handle == Handle::Released && st.open)
            .map(|(&id, _)| id)
            .collect();
        if let Some(cat) = self.cat.as_mut() {
            let removed = &cat.removed;
            let rows = cat.catalog.released_ids()?;
            ids.extend(rows.into_iter().map(BranchId).filter(|id| !removed.contains(id)));
        }
        ids.sort_unstable();
        ids.dedup();
        let mut logged = false;
        for &id in &ids {
            if !self.ensure(id)? {
                continue;
            }
            let Some(st) = self.branches.get_mut(&id) else {
                continue;
            };
            if st.handle != Handle::Released || !st.open {
                // A catalog row still marked held whose `Close` this recovery replayed.
                continue;
            }
            st.open = false;
            journal.buffer(&Record::Close { branch: id.0 })?;
            logged = true;
            // The catalog row stops saying held at the next checkpoint (see `close`).
            self.mark_dirty(id, DIRTY_ROW);
            self.collect(id, freed)?;
        }
        Ok((ids.len() as u64, logged))
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

    /// The version of `page` the trunk held when branch `id` forked: `resolve`'s trunk tail at the
    /// branch's `trunk_at`, ignoring the branch's own and inherited pages (the base a V4 merge
    /// diffs against, r11-merge A20). `None`: the trunk has not rewritten the page since, so its
    /// current version is the base.
    fn base_resolve(
        &mut self,
        id: BranchId,
        page: u32,
        examined: &mut u64,
    ) -> Result<Option<(Slot, u32)>> {
        if !self.ensure(id)? {
            return Err(gone(id));
        }
        let at = self.branches.get(&id).ok_or_else(|| gone(id))?.trunk_at;
        self.trunk_written_known(page)?;
        if let Some(found) = self.trunk_version_at(page, at, examined)? {
            return Ok(Some(found));
        }
        let born = self.trunk.written.get(&page).copied().unwrap_or(0);
        if born > at {
            return Err(LimboError::Corrupt(format!(
                "branch {}'s base page {page} was written after its fork and not retained",
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
            st.inherited_at = st.fork_epoch;
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
                .apply_fork(BranchId(*parent), BranchId(*child), None, Handle::Detached, None)
                .map_err(corrupt),
            Record::ForkNamed {
                child,
                parent,
                name,
            } => self
                .apply_fork(
                    BranchId(*parent),
                    BranchId(*child),
                    None,
                    Handle::Detached,
                    Some(Arc::from(name.as_str())),
                )
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
                self.apply_release(id, freed).map(|_| ()).map_err(corrupt)
            }
            Record::ReleaseOpen { branch } => {
                // Released while a connection held it: hold it, as the live store did, until the
                // matching `Close` (or the end of recovery, since nothing is open after a restart).
                let id = BranchId(*branch);
                if !self.ensure(id)? {
                    return Err(corrupt(gone(id)));
                }
                if let Some(st) = self.branches.get_mut(&id) {
                    st.open = true;
                }
                self.apply_release(id, freed).map(|_| ()).map_err(corrupt)
            }
            Record::Close { branch } => {
                let id = BranchId(*branch);
                if !self.ensure(id)? {
                    return Err(corrupt(gone(id)));
                }
                if let Some(st) = self.branches.get_mut(&id) {
                    st.open = false;
                }
                // The catalog row stops saying held at the next checkpoint (see `close`).
                self.mark_dirty(id, DIRTY_ROW);
                self.collect(id, freed).map(|_| ()).map_err(corrupt)
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
                    held_open: st.handle == Handle::Released && st.open,
                    // Deadline + 1, so that a real deadline of 0 is not read back as "no lease"
                    // (review R7). A saturated u64::MAX deadline comes back 1 ms shorter.
                    lease_deadline_ms: st.lease.map_or(0, |d| d.saturating_add(1)),
                    current,
                    retained: st.lineage.retained_list(),
                    name: st.name.as_deref().map(str::to_string),
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
            let current: HashMap<u32, Owned> = b
                .current
                .into_iter()
                .map(|(page, slot, born, crc)| (page, Owned { slot, born, crc }))
                .collect();
            let current_by_born = current.iter().map(|(&page, o)| (o.born, page)).collect();
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
                    current_by_born,
                    pending: HashMap::new(),
                    schema: None,
                    handle: if b.released {
                        Handle::Released
                    } else {
                        Handle::Detached
                    },
                    // Held until its `Close` in the log that follows, or the end of recovery.
                    open: b.held_open,
                    writer: false,
                    lease,
                    // Placeholders: `derive_page_maps` sets these once every lineage is linked.
                    trunk_at: 0,
                    inherited: PageMap::default(),
                    inherited_at: 0,
                    view: None,
                    name: b.name.as_deref().filter(|_| !b.released).map(Arc::from),
                    fork_lsn: 0,
                    in_doubt: false,
                },
            );
            if let Some(name) = b.name.filter(|_| !b.released) {
                self.names.map.insert(Arc::from(name), BranchId(b.id));
            }
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
        let durable = BranchDurability::Durable { sync: crate::branch::SyncClass::Off };
        {
            let first = BranchStore::open(durable, None, path).unwrap();
            first.inner.lock().ensure_backing(512).unwrap();
        }
        let ro = BranchStore::open_with_flags(BranchDurability::Volatile, None, false, path, true)
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
        let store = BranchStore::open(BranchDurability::Durable { sync: crate::branch::SyncClass::Off }, None, path).unwrap();
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
        let store = BranchStore::open(BranchDurability::Durable { sync: crate::branch::SyncClass::Off }, None, path).unwrap();
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
        let recovered = Journal::recover(&BranchFiles::for_db(path), SyncClass::Off)
            .unwrap()
            .expect("state");
        assert_eq!(recovered.page_size, 1024, "the store kept the old page size");
        assert_eq!(recovered.records, vec![Record::Release { branch: 9 }]);
    }

    /// Skill review 1 (f): an empty store's page-size restart drops the deferred frees of a release
    /// whose flight had not landed (here: one under a sequence number nothing has reached). Their
    /// slots are the truncated arena's: matured into the new arena's free list, each would be
    /// handed out twice, from the list and from the high-water mark.
    #[test]
    fn a_page_size_restart_drops_the_old_arenas_deferred_frees() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: crate::branch::SyncClass::Off }, None, path).unwrap();
        let mut inner = store.inner.lock();
        inner.ensure_backing(512).unwrap();
        let far = inner.journal.as_ref().unwrap().lsn() + 1;
        inner.pending_free.push_back((far, vec![1, 2]));
        inner.ensure_backing(1024).expect("an empty store refused the database's new page size");
        store.group.mark_durable(far, SyncClass::Off);
        store.mature(&mut inner);
        let arena = inner.arena.as_mut().unwrap();
        let mut seen = HashSet::new();
        for _ in 0..4 {
            let slot = arena.alloc();
            assert!(seen.insert(slot), "slot {slot} handed out twice after a page-size restart");
        }
    }

    /// The guard beside it: a store that still HOLDS something — here one branch — cannot follow
    /// a page-size change, and must keep refusing it.
    #[test]
    fn a_store_holding_a_branch_refuses_a_new_page_size() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        let path = path.to_str().unwrap();
        let store = BranchStore::open(BranchDurability::Durable { sync: crate::branch::SyncClass::Off }, None, path).unwrap();
        let mut inner = store.inner.lock();
        inner.ensure_backing(512).unwrap();
        inner
            .apply_fork(BranchId::TRUNK, BranchId(1), None, Handle::Detached, None)
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
        let store = BranchStore::open(BranchDurability::Durable { sync: crate::branch::SyncClass::Off }, None, path).unwrap();
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
        let recovered = Journal::recover(&files, SyncClass::Off).unwrap().expect("state");
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
        let durable = BranchDurability::Durable { sync: crate::branch::SyncClass::Off };
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
        let recovered = Journal::recover(&files, SyncClass::Off).unwrap().expect("state");
        assert_eq!(recovered.records, vec![Record::Release { branch: 7 }]);
    }

    /// R7. A deadline of 0 is a real deadline (`lease(ZERO)` at the clock's first millisecond),
    /// and must not come back from a snapshot as "no lease" — which would make the branch
    /// permanent.
    #[test]
    fn a_zero_deadline_is_still_a_lease_after_a_snapshot() {
        let mut live = StoreInner::fresh(None, SyncClass::Off, None);
        let id = BranchId(1);
        live.apply_fork(BranchId::TRUNK, id, None, Handle::Detached, None)
            .unwrap();
        live.apply_lease(id, 0);
        let snapshot = live.snapshot();
        let mut recovered = StoreInner::fresh(None, SyncClass::Off, None);
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

    /// The store modes the lane tests run in (a12-durable-open lane: catalog mode added;
    /// githost-shape lane: catalog mode with a resident cap of 0, so every checkpoint evicts every
    /// clean state (F-W3) and the tests' reads after it go through `ensure`'s reload).
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub(super) enum Mode {
        Volatile,
        Durable,
        Catalog,
        CatalogEvict,
    }

    pub(super) const MODES: [Mode; 4] = [Mode::Volatile, Mode::Durable, Mode::Catalog, Mode::CatalogEvict];

    impl Mode {
        fn durability(self) -> BranchDurability {
            match self {
                Mode::Volatile => BranchDurability::Volatile,
                Mode::Durable => BranchDurability::Durable { sync: crate::branch::SyncClass::Off },
                Mode::Catalog | Mode::CatalogEvict => BranchDurability::Catalog { sync: crate::branch::SyncClass::Off },
            }
        }

        pub(super) fn durable(self) -> bool {
            self != Mode::Volatile
        }

        /// A catalog store, evicting or not.
        pub(super) fn catalog(self) -> bool {
            matches!(self, Mode::Catalog | Mode::CatalogEvict)
        }

        /// Open a store of this mode at `path` (F-W3's cap applied for `CatalogEvict`).
        fn open_at(self, path: &str) -> BranchStore {
            // r13-compose R3.3: in either splice arm, so `R11_SPLICE=1` runs F-W3's eviction (cap
            // 0) under F7's splice through every model test (N11's test).
            let store = BranchStore::open_mode(self.durability(), None, splice_arm(), path).unwrap();
            if self == Mode::CatalogEvict {
                store.set_resident_cap(Some(0));
            }
            store
        }
    }

    /// The F7 splice arm, from `R11_SPLICE` (r11-ever amendment 15): these model tests check every
    /// read against a model, so the lane runs them in both arms.
    pub(super) fn splice_arm() -> bool {
        std::env::var_os("R11_SPLICE").is_some()
    }

    /// A store of the given mode over `dir` (unused when volatile).
    pub(super) fn open_store(mode: Mode, dir: &std::path::Path, name: &str) -> BranchStore {
        let path = if mode.durable() {
            dir.join(name).to_str().unwrap().to_string()
        } else {
            ":memory:".to_string()
        };
        mode.open_at(&path)
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
        mode.open_at(to_str(&dir.join(image)))
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
            let by_born: BTreeSet<(u64, u32)> =
                st.current.iter().map(|(&page, o)| (o.born, page)).collect();
            assert_eq!(st.current_by_born, by_born, "branch {}: current_by_born disagrees", id.0);
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
                    let id = store.fork_trunk_locked(Arc::new(Schema::default()), PAGE).unwrap();
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
                    store.durability_barrier(SyncClass::Off).unwrap();
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
                    if mode.catalog() {
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
                    let id = store.fork_trunk_locked(Arc::new(Schema::default()), PAGE).unwrap();
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
                    let id = store.fork_branch_durable(nodes[parent].id).unwrap();
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
                    store.durability_barrier(SyncClass::Off).unwrap();
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
        for mode in [Mode::Durable, Mode::Catalog, Mode::CatalogEvict] {
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
                        (store.fork_trunk_locked(Arc::new(Schema::default()), PAGE), trunk.clone(), 1)
                    } else {
                        let parent = if rng.below(2) == 0 {
                            *live.last().unwrap()
                        } else {
                            live[rng.below(live.len() as u64) as usize]
                        };
                        let r = store.fork_branch_durable(nodes[parent].id);
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
                    match store.durability_barrier(SyncClass::Off) {
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
