//! Branch-per-agent isolation for Turso.
//!
//! * Step 1 (below): the admissibility gate — refuse to branch an MVCC-mode database.
//! * Steps 2-3 ([`store`], [`arena`]): the copy-on-write decision in `Pager::add_dirty` and the
//!   per-branch page space it copies into. [`store`] carries the model; the public surface is
//!   [`Connection::fork_branch`], [`Branch`] and [`Database::branch_stats`].
//!
//! # What this is
//!
//! A fork of an established engine, to demonstrate that branch-per-agent copy-on-write isolation
//! survives an existing WAL, an existing recovery path and an existing page cache — the thing a
//! from-scratch proof of concept cannot show. The seam is [`Pager::add_dirty`], where
//! `subjournal_page_if_required` already performs a conditional pre-image copy: branch CoW is the
//! same operation with a different destination and a different predicate, so this widens a
//! mechanism the engine already has rather than adding one.
//!
//! # Why the FIRST thing here is a refusal and not a copy
//!
//! The fork decision carried a pre-registered blocker: *"If branching uncheckpointed MVCC state
//! turns out to require changes INSIDE the MVCC layer rather than at the pager, the seam advantage
//! is gone."* That question was answered before any of this was written, and **it fires — for
//! MVCC-mode databases only.** Verified against this tree, not assumed:
//!
//! * `grep -rn add_dirty core/mvcc/` -> **0 matches.** The MVCC layer never reaches the seam.
//!   It is a parallel storage engine above the pager, with its own version store
//!   (`MvStore.rows: SkipMap<RowID, RowVersions>`), its own durability file (`.db-log`), and its
//!   own physical-root mapping (`table_id_to_rootpage`).
//! * `MvStore` is per-`Database`, not per-connection (`database.rs:520`), so two branches sharing
//!   a `Database` see each other's rows through the MVCC half of the dual cursor.
//! * `table_id_to_rootpage` maps MVCC table ids to **physical root page numbers**. After a page-
//!   space fork those numbers mean different pages in different branches, so a branch reads a
//!   valid-looking wrong page. Not an error — bad rows.
//!
//! So a pager-level CoW under MVCC gives a branch that is **stale** (misses committed-but-
//! uncheckpointed rows), **leaky** (shared version store) and **silently wrong** (shared root
//! page numbers). Three failure modes, all quiet.
//!
//! # Why that is a scope line and not a dead end
//!
//! MVCC is not Turso's engine; it is one of seven `JournalMode`s
//! (`storage/journal_mode.rs:20-29`), spelled `experimental_mvcc`, selected per database through
//! the header via `PRAGMA journal_mode`. The default is `Wal`. In WAL mode `mv_store` is `None`
//! and the pager seam is the whole story.
//!
//! **So: branch a WAL-mode database; REFUSE an MVCC-mode one.** The rule this follows is that a
//! dangerous state should be made unrepresentable rather than documented — refuse rather than
//! warn, because the three failure modes above are all silent and a warning is a thing a caller
//! can be unaware of. A branch that is quietly stale is worse than no branch.
//!
//! ⚠ **This refusal is the SCOPE, stated honestly, not a claim that MVCC branching is impossible.**
//! One `Database` (hence one `MvStore`) per branch would avoid the MVCC layer entirely, trading a
//! `.db-log` and a version store per agent for zero MVCC edits. That is unmeasured and it abandons
//! the pager seam, so it is recorded as the open question it is rather than promised.

pub(crate) mod arena;

/// Process-wide page I/O counters (r11-restart lane instrument, observing only): pages read from
/// and written to database files (`DatabaseFile::read_page` / `write_page(s)`) and WAL frames read
/// and appended, across every database in the process. `[db reads, db writes, wal reads, wal writes]`.
#[doc(hidden)]
pub static PAGE_IO: [crate::sync::atomic::AtomicU64; 4] = [
    crate::sync::atomic::AtomicU64::new(0),
    crate::sync::atomic::AtomicU64::new(0),
    crate::sync::atomic::AtomicU64::new(0),
    crate::sync::atomic::AtomicU64::new(0),
];

/// A snapshot of [`PAGE_IO`].
#[doc(hidden)]
pub fn page_io() -> [u64; 4] {
    use crate::sync::atomic::Ordering::Relaxed;
    [
        PAGE_IO[0].load(Relaxed),
        PAGE_IO[1].load(Relaxed),
        PAGE_IO[2].load(Relaxed),
        PAGE_IO[3].load(Relaxed),
    ]
}

/// r12-catload: with `R12_TRACE_READS` set, every database page read as `(page number, bytes)`,
/// process-wide and in order (observing only; unset, one initialised-flag load per read).
static READ_TRACE_ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
static READ_TRACE: std::sync::Mutex<Vec<(u32, u32)>> = std::sync::Mutex::new(Vec::new());

pub(crate) fn trace_page_read(page: usize, bytes: usize) {
    if *READ_TRACE_ON.get_or_init(|| std::env::var_os("R12_TRACE_READS").is_some()) {
        if let Ok(mut t) = READ_TRACE.lock() {
            t.push((page as u32, bytes as u32));
        }
    }
}

/// Take the page reads [`trace_page_read`] recorded since the last take (r12-catload instrument).
#[doc(hidden)]
pub fn take_page_read_trace() -> Vec<(u32, u32)> {
    READ_TRACE.lock().map(|mut t| std::mem::take(&mut *t)).unwrap_or_default()
}

pub(crate) fn count_page_io(which: usize, n: u64) {
    PAGE_IO[which].fetch_add(n, crate::sync::atomic::Ordering::Relaxed);
}

/// The WAL checkpoint's backfill reads, process-wide (r11-restart-r2 item 4, observing only): pages
/// the backfill found in the page cache at the frame it needed, and WAL frame reads it issued
/// instead. R7 read "WAL frame reads 0" with no counter on this path (the r11-restart-refute report).
#[doc(hidden)]
pub static BACKFILL_IO: [crate::sync::atomic::AtomicU64; 2] = [
    crate::sync::atomic::AtomicU64::new(0),
    crate::sync::atomic::AtomicU64::new(0),
];

/// A snapshot of [`BACKFILL_IO`]: `[page-cache hits, WAL frame reads issued]`.
#[doc(hidden)]
pub fn backfill_io() -> [u64; 2] {
    use crate::sync::atomic::Ordering::Relaxed;
    [BACKFILL_IO[0].load(Relaxed), BACKFILL_IO[1].load(Relaxed)]
}

pub(crate) fn count_backfill_io(which: usize) {
    BACKFILL_IO[which].fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
}
pub(crate) mod catalog;
pub(crate) mod derive;
#[doc(hidden)]
pub use catalog::catalog_only_fixture;
pub use catalog::CatalogProbe;
pub(crate) mod id_set;

#[doc(hidden)]
pub use id_set::id_set_census;
pub(crate) mod journal;
pub mod merge;
pub(crate) mod page_map;
pub(crate) mod prewarm;
pub(crate) mod store;
pub(crate) mod table;

use crate::error::LimboError;
use crate::storage::pager::{AutoVacuumMode, Pager};
use crate::storage::wal::WalAutoActions;
use crate::sync::Arc;
use crate::util::IOExt as _;
use crate::{Connection, Database, Result, TransactionState};
use store::BranchStore;

/// The identity of a branch. Distinct from any page or transaction id on purpose: a branch
/// outlives the transactions that write into it, which is the whole point of the mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(pub u64);

impl BranchId {
    /// The database as it exists without branching. Every fork descends from it.
    pub const TRUNK: BranchId = BranchId(0);

    pub fn is_trunk(&self) -> bool {
        *self == Self::TRUNK
    }
}

/// Why a database cannot be branched. Each variant is a condition that produces a SILENTLY wrong
/// branch rather than a loud failure, which is why each is refused up front.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unbranchable {
    /// The database is in `experimental_mvcc` journal mode.
    MvccJournalMode,
    /// The database has no page 1 yet.
    Empty,
    /// The database is encrypted (built-in encryption or an external page codec).
    Encrypted,
    /// The database uses auto-vacuum.
    AutoVacuum,
    /// The database was opened with multi-process WAL coordination.
    MultiProcess,
}

impl Unbranchable {
    /// The message a caller gets. It names the mechanism, not just the verdict: a refusal a caller
    /// cannot act on gets worked around, and working around this one produces bad rows.
    pub fn explain(&self) -> &'static str {
        match self {
            Unbranchable::MvccJournalMode => {
                "cannot branch a database in experimental_mvcc journal mode: MVCC row versions \
                 live in MvStore and the logical log, never in pages, so a page-level branch \
                 would silently miss every committed-but-uncheckpointed row, share one version \
                 store between branches, and resolve shared root page numbers against a forked \
                 page space. Use journal_mode=wal to branch this database."
            }
            Unbranchable::Empty => {
                "cannot branch an empty database: it has no committed page 1 for a branch to \
                 start from. Create a table first."
            }
            // The two below are NOT shown to be silent failures. They are refused because this
            // fork has not made their paths branch-aware and has no test of what they would do.
            Unbranchable::Encrypted => {
                "cannot branch an encrypted database: branch page copies are held in memory in \
                 plaintext and a branch connection is opened without the key; neither path has \
                 been made branch-aware."
            }
            Unbranchable::AutoVacuum => {
                "cannot branch an auto-vacuum database: auto-vacuum relocates pages and \
                 truncates the database file at commit, and a truncation rewrites what a branch \
                 reads without passing through the copy-on-write decision."
            }
            // This one IS silent: the branch store and every pre-image it keeps live in this
            // process, so a trunk write committed by another process takes no copy decision and
            // a branch reading the trunk's current page would simply see it.
            Unbranchable::MultiProcess => {
                "cannot branch a database opened for multi-process WAL access: branches and the \
                 pre-images that isolate them live in this process only, so another process's \
                 trunk writes would silently appear in every branch."
            }
        }
    }
}

impl From<Unbranchable> for LimboError {
    fn from(u: Unbranchable) -> Self {
        LimboError::InvalidArgument(u.explain().to_string())
    }
}

/// The admissibility gate. **Every entry point that creates a branch must pass through here.**
///
/// It takes the answer as a bool rather than a `&Database` so that it is callable from a unit test
/// without standing up a database — a guard nobody can exercise cheaply is a guard nobody
/// exercises. The caller's job is to supply `Database::mvcc_enabled()`; this function's job is to
/// be the only place that decides what that implies.
pub fn check_branchable(mvcc_enabled: bool) -> Result<()> {
    if mvcc_enabled {
        return Err(Unbranchable::MvccJournalMode.into());
    }
    Ok(())
}

/// Whether branch state survives the process. `Durable` (UNBUILT when written) keeps arena pages,
/// page maps, lineage and releases in files next to the database; see `journal.rs` for the files,
/// the ordering rules and the prior art they copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BranchDurability {
    /// In memory only: branches die with the process.
    #[default]
    Volatile,
    /// Arena pages, page maps, lineage and releases are written to files next to the database and
    /// recovered at open. `sync` is the flush every acknowledged branch operation waits for, and
    /// the trunk's (see [`SyncClass`]).
    Durable { sync: SyncClass },
    /// Durable, with the published fixes for an open that grows with the number of branches
    /// (r11-restart lane prototype; see `catalog.rs`): the same operation log, checkpointed
    /// incrementally into a B-tree catalog `<db>-branch-cat` instead of a whole-state snapshot, and
    /// read back on demand. `sync` as for `Durable`.
    Catalog { sync: SyncClass },
}

impl BranchDurability {
    /// The flush class of a durable store; `None` for a volatile one.
    pub fn sync_class(&self) -> Option<SyncClass> {
        match *self {
            BranchDurability::Volatile => None,
            BranchDurability::Durable { sync } | BranchDurability::Catalog { sync } => Some(sync),
        }
    }
}

/// How a durable write reaches the disk (fastest-engine PREREG §4's modes). ONE class governs the
/// branch store's files AND the trunk's WAL and database file, because the two are ordered: a
/// trunk commit that overwrites a page a branch reads must never be more durable than the
/// pre-image kept for that branch. With the branch store weaker than the trunk, a power cut could
/// keep the trunk's new page and lose the pre-image, and the branch would then read the new page
/// with no error. So the class is set once, here, and the database applies it to every trunk
/// connection it opens (see `Database::_init`); a trunk connection that later asks for a stronger
/// flush (`PRAGMA fullfsync`) has the branch store's barrier raised to match
/// (`BranchStore::durability_barrier`), never the other way round. Ordered weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncClass {
    /// D0: nothing is synced, on the branch store or the trunk (`synchronous = OFF`). A
    /// measurement arm with no durability guarantee: an acknowledged operation can be lost by any
    /// crash that loses the OS page cache.
    Off,
    /// D1: `fsync(2)`. On Linux that reaches stable storage; on Apple platforms it hands the data
    /// to the drive, whose volatile cache may still lose it on power loss.
    Fsync,
    /// D2: `fcntl(F_FULLFSYNC)` on Apple platforms, which also flushes the drive's cache; elsewhere
    /// the same as `Fsync`. The trunk's connections get `PRAGMA fullfsync` (`FileSyncType::FullFsync`).
    FullFsync,
}

impl SyncClass {
    /// Whether anything is synced at all.
    pub fn syncs(self) -> bool {
        self != SyncClass::Off
    }

    /// The trunk's file sync type in this class (`Off` syncs nothing; its type is moot).
    pub(crate) fn file_sync_type(self) -> crate::io::FileSyncType {
        match self {
            SyncClass::FullFsync => crate::io::FileSyncType::FullFsync,
            SyncClass::Off | SyncClass::Fsync => crate::io::FileSyncType::Fsync,
        }
    }

    /// The trunk's sync mode in this class: `Off` opens trunk connections `synchronous = OFF`.
    pub(crate) fn sync_mode(self) -> crate::SyncMode {
        match self {
            SyncClass::Off => crate::SyncMode::Off,
            SyncClass::Fsync | SyncClass::FullFsync => crate::SyncMode::Full,
        }
    }

    /// The class a trunk commit actually syncs its WAL in, from its connection's settings: a commit
    /// syncs only under `synchronous = FULL` (`Pager::commit_wal_inner`'s `WaitSync`).
    pub(crate) fn of_trunk(mode: crate::SyncMode, sync_type: crate::io::FileSyncType) -> Self {
        match (mode, sync_type) {
            (crate::SyncMode::Full, crate::io::FileSyncType::FullFsync) => SyncClass::FullFsync,
            (crate::SyncMode::Full, crate::io::FileSyncType::Fsync) => SyncClass::Fsync,
            (crate::SyncMode::Off | crate::SyncMode::Normal, _) => SyncClass::Off,
        }
    }
}

/// Every sync this process issued through the branch store's files or the platform IO backend, by
/// primitive (fastest-engine instrument V1-in-process; observing only): `fsync(2)` calls and
/// `fcntl(F_FULLFSYNC)` calls. A cross-check of the DYLD syscall shim, never a substitute for it:
/// a sync issued by anything else (the catalog's own IO is the platform backend, so it IS counted)
/// does not appear.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SyncCounts {
    pub fsync: u64,
    pub full_fsync: u64,
}

#[doc(hidden)]
pub fn sync_counts() -> SyncCounts {
    use crate::sync::atomic::Ordering::Relaxed;
    SyncCounts {
        fsync: crate::io::SYNC_COUNTS[0].load(Relaxed),
        full_fsync: crate::io::SYNC_COUNTS[1].load(Relaxed),
    }
}

/// Failure injection for the durability tests.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchFailpoint {
    /// The next branch commit writes and syncs its slots, then fails before appending its record.
    CommitAfterSlotsBeforeRecord,
    /// The next durability barrier (a trunk commit's) fails before writing its records.
    BarrierBeforeRecords,
    /// The next compaction fails after renaming the new snapshot, before resetting the log.
    CompactAfterRenameBeforeLogReset,
    /// The next record flush fails as an I/O error would: the record is not durable and the
    /// journal fail-stops (poisoned) from then on.
    LogFlushFails,
    /// The next creation of the branch log fails after its header is written, before its
    /// directory is synced — as an fsync failure there would.
    CreateFailsAfterHeader,
    /// The next stamp-only flush at a trunk commit's barrier fails as an I/O error would.
    StampFlushFails,
    /// The next branch-log creation fails to take the log's lock just after creating the file —
    /// as a filesystem without `flock` would — leaving an empty log behind.
    CreateLockFails,
    /// The next catalog checkpoint's write (sharp, or a fuzzy flight's) fails before its catalog
    /// commit, as an I/O error would (r13-compose D-T2's failed-write order, A5.4; review
    /// wf_5c230f31).
    CheckpointWriteFails,
}

/// A live branch: an isolated, writable view of the database as it was when the branch was forked.
///
/// The handle owns the branch. Dropping it — or calling [`Branch::reap`], which is the same thing
/// with a report — releases the branch's pages at once, unless something still reads through them:
/// an open connection on the branch, or a live child forked from it. Then the branch is kept until
/// the last of those goes, and freed at that moment.
pub struct Branch {
    db: Arc<Database>,
    id: BranchId,
    released: bool,
}

/// What a lease-expiry pass reaped.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Expired {
    /// Branches reaped because their lease ran out, in the order they were reaped.
    pub reaped: Vec<BranchId>,
    /// Arena pages the pass returned to the free list.
    pub freed_pages: usize,
}

/// What reaping a branch released.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reaped {
    /// Arena pages returned to the free list by this call: the branch's own, plus any version an
    /// ancestor was retaining only for it.
    pub freed_pages: usize,
    /// True when the branch could not be freed yet (an open connection or a live child still reads
    /// through it); its pages are freed when the last of those goes away. Also true when it was
    /// spliced out (its only live child now holds what it read through it): it has left the store's
    /// branch states, but its pages live on in that child.
    pub deferred: bool,
}

/// What the last open of the branch store read and rebuilt (r11-restart lane). An observing
/// instrument only: nothing reads it back. Each phase time is paired with the integer that phase
/// is proportional to, so the phase table closes against `total_ns`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchOpenStats {
    /// Bytes of `<db>-branch-snap` read (0 when absent).
    pub snap_bytes: u64,
    /// Bytes of `<db>-branch-log` read.
    pub log_bytes: u64,
    /// Log records decoded and replayed.
    pub records: u64,
    /// Branches decoded from the snapshot.
    pub snap_branches: u64,
    /// Branch states in memory after replay and collection.
    pub branches: u64,
    /// `current` entries materialised, over every branch.
    pub current_entries: u64,
    /// Retained versions materialised in branch lineages.
    pub retained_entries: u64,
    /// Retained versions materialised in the trunk's lineage.
    pub trunk_retained: u64,
    /// Live children of the trunk.
    pub trunk_children: u64,
    /// Page-map inserts made by `derive_page_maps` after a snapshot load (sota-durable lane; the
    /// page maps are derived at open, and this is that derivation's work). Timed inside `load_ns`.
    pub derived_map_inserts: u64,
    /// Slots named by the recovered state (the reachability sweep's input).
    pub referenced_slots: u64,
    /// The arena file's high-water mark in slots (the sweep's range).
    pub arena_high_water: u64,
    /// Slots the sweep put on the free list.
    pub arena_free: u64,
    /// `Journal::recover`: read and decode both files (snap_bytes + log_bytes).
    pub recover_ns: u64,
    /// `load_snapshot` (snap_branches).
    pub load_ns: u64,
    /// Replay (records).
    pub replay_ns: u64,
    /// `close_held`: closing and collecting what a crash left held (`released_scanned`).
    pub collect_ns: u64,
    /// `referenced_slots` (referenced_slots).
    pub referenced_ns: u64,
    /// `Arena::open_file` (arena_high_water).
    pub arena_ns: u64,
    /// The expiry pass at open.
    pub expire_ns: u64,
    /// The whole `BranchStore::open`.
    pub total_ns: u64,
    /// Branch states that exist, resident or not (catalog stores keep most on disk).
    pub states: u64,
    /// Released branches the open's collection pass examined: since the F7 durable port, the ones a
    /// crash left held (a snapshot's `held_open`, a catalog row's `released = 2`, or a replayed
    /// `ReleaseOpen`), each closed and collected there. (Before: every released branch in memory,
    /// plus the catalog's released rows with no child.)
    pub released_scanned: u64,
    /// Catalog stores: opening the catalog and reading its meta row.
    pub catalog_ns: u64,
    /// Catalog stores: branch states and trunk pages read from the catalog during the open.
    pub branch_loads: u64,
    pub trunk_page_loads: u64,
    /// Catalog stores: catalog queries run and rows read during the open.
    pub cat_queries: u64,
    pub cat_rows_read: u64,
    /// Catalog stores: arena slots the log's replay named or freed.
    pub touched_slots: u64,
    /// Catalog stores (a12-durable-open C-P): trunk-version probes and range reads made during the
    /// open, and the trunk version rows they returned (`trunk_page_loads` stays 0: no trunk page is
    /// loaded whole).
    pub trunk_probes: u64,
    pub trunk_rows: u64,
    /// Catalog stores (a12-durable-open C-R, redo on demand): the log tail's Commits parked because
    /// their branch was not resident, and how many of them the open itself applied (a later record
    /// of the tail, a release or the expiry pass touched the branch).
    pub parked_records: u64,
    pub parked_applied: u64,
}

/// The Merger's work since open (r13-compose, the Merger port; observing only). Every field is an
/// integer count bumped by the call that does the work.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchMergeWork {
    /// Merges attempted, committed, and refused by kind.
    pub merge_attempts: u64,
    pub merge_commits: u64,
    pub merge_refused_scope: u64,
    pub merge_refused_key: u64,
    pub merge_refused_base: u64,
    pub merge_refused_install: u64,
    /// MV4 refusals where theirs == ours on every conflicting key (A6.4 (ii)).
    pub refusals_same_change: u64,
    /// KeyStamp verdicts handed to the base read because the branch forked before the horizon (D-M5).
    pub v3_horizon_fallbacks: u64,
    /// Trunk commits stamped, stamp prunes, and the stamps held now: `stamps_held` is the distinct
    /// stamped keys (bounded by the key space), `stamp_entries` the prune queue's (key, epoch)
    /// entries, one per key re-stamped at a new epoch, which is A31's term: it grows with the
    /// trunk's row writes since the oldest live child (review wf_5c230f31: stamps_held alone
    /// saturates at the key count; PREREG amendment 8 scores A31 on stamp_entries).
    pub stamp_commits: u64,
    pub stamp_prunes: u64,
    pub stamps_held: u64,
    pub stamp_entries: u64,
    /// The derived write set (r13-compose A5/A6): pages read (branch and base versions, descents,
    /// freelist, overflow), attribution descents, rows compared, subtrees enumerated and cancelled,
    /// freelist trunk pages read, refusals by reason, and keys derived.
    pub derive_pages_read: u64,
    pub derive_attribution_seeks: u64,
    pub derive_rows_compared: u64,
    pub derive_subtrees_enumerated: u64,
    pub derive_subtrees_cancelled: u64,
    pub derive_freelist_reads: u64,
    pub derive_refusals_ddl: u64,
    pub derive_refusals_unattributed: u64,
    pub derive_refusals_without_rowid: u64,
    pub derive_refusals_clear_or_delete_all: u64,
    pub derive_keys: u64,
    /// MV4's base rows compared with the trunk now.
    pub mv4_keys: u64,
    /// I8: pages MV4 read from the trunk NOW to compare each changed row with its base (A2.F9's
    /// "mv4_base_reads per key <= depth"). Kept out of `derive_pages_read`, whose KNOWN bound
    /// (A6.5) covers the derivation only.
    pub mv4_base_reads: u64,
}

/// githost-shape lane instrument (observing only; r3, on a12-durable-open's C-P + C-R): the catalog
/// store's checkpoint work, what it keeps resident, and what a listing does while it holds the store
/// mutex. Cumulative fields count since the open; the rest are the state now. Taking it runs no
/// catalog query, so it moves none of the catalog counters it sits beside.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchCatShape {
    /// Catalog checkpoints run, and the nanoseconds inside them (settling parked commits excluded).
    pub checkpoints: u64,
    pub checkpoint_ns: u64,
    /// Trunk version rows the checkpoints inserted (versions retained since the previous one) and
    /// deleted (catalog versions reaped since).
    pub ckpt_trunk_inserted: u64,
    pub ckpt_trunk_deleted: u64,
    /// Branch states the checkpoints wrote, and catalog rows written by them in all.
    pub ckpt_branch_rows: u64,
    pub ckpt_rows_written: u64,
    /// Branch states the checkpoints visited to collect the slots open write transactions reserve.
    pub ckpt_states_walked: u64,
    /// Listings (`ids`): calls, resident states visited and catalog rows read while holding the
    /// store mutex, and catalog rows read to build a live-id set (0 unless a fix builds one).
    pub ids_calls: u64,
    pub ids_resident_visited: u64,
    pub ids_catalog_rows: u64,
    pub ids_build_rows: u64,
    /// Resident-table growths and what they moved. Under F8' (r13-compose step 3) these are the
    /// table's DIRECTORY reallocations and the chunk pointers they moved: no branch state moves.
    pub table_grows: u64,
    pub table_moved: u64,
    /// Checkpoints that evicted clean resident states, and the states evicted (0 unless a fix
    /// evicts).
    pub evictions: u64,
    pub evicted_states: u64,
    /// r13-compose I4: `ensure` calls that loaded at least one state (cold), the states they loaded
    /// in all, and the most loaded by one call.
    pub ensure_cold: u64,
    pub ensure_chain_sum: u64,
    pub ensure_chain_max: u64,
    /// r13-compose I5: evicted states that were the parent of a state still resident after that
    /// eviction (amendment 8).
    pub evicted_with_resident_descendant: u64,
    /// r13-compose I11: items yielded by walks of the branch table, and the slots those walks
    /// visited, empty ones included (A2.F5's `walk_slots_scanned`: B_ALL's table; B_noF8 reads 0).
    pub walk_items_yielded: u64,
    pub walk_slots_scanned: u64,
    /// I5's own walk of an eviction's survivors (instrument cost, apart from I11).
    pub instrument_walk_items: u64,
    /// C-R's settle on the sharp path: calls that loaded parked branches, the branches they loaded,
    /// and the most in one call (amendment 8's C-R row; the fuzzy path's are in
    /// `branch_checkpoint_counters`).
    pub settle_sharp_calls: u64,
    pub settle_sharp_loads: u64,
    pub settle_sharp_max_loads: u64,
    /// r13-compose I3: F4 page-map inserts made deriving loaded states, since open (live).
    pub derived_inserts: u64,
    /// r13-compose I6/I11 (gauges): the table's chunks now, chunks allocated since open, slots in
    /// allocated chunks, and those slots' bytes (one slot = one `Option<(BranchId, BranchState)>`).
    pub table_chunks: u64,
    pub chunk_allocs: u64,
    pub table_slots_allocated: u64,
    pub table_slot_bytes: u64,
    /// Branch states resident now, and dirty now.
    pub resident_states: u64,
    pub dirty_branches: u64,
    /// Trunk versions in memory that the catalog does not hold yet (retained since the last
    /// checkpoint); catalog trunk versions cached in memory, and the pages they are on; trunk pages
    /// whose `written` epoch this process has read.
    pub trunk_overlay_versions: u64,
    pub trunk_cache_versions: u64,
    pub trunk_cache_pages: u64,
    pub trunk_known_pages: u64,
    /// The log's whole-record length.
    pub log_len: u64,
    /// C-P's trunk-version probes and range reads since open, and the version rows they returned
    /// (cumulative; the open's own share is also in `BranchOpenStats`).
    pub trunk_probes: u64,
    pub trunk_rows: u64,
}

/// The catalog file's shape (r11-ever amendment 19): what its pages hold, so a size can be
/// attributed to rows, tree levels or the free list. Observation only; it reads every page of the
/// catalog, so it is an instrument for a harness's checkpoints, never a store path.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogShape {
    /// Pages in the catalog file, and on its free list (SQLite's header count: trunks and leaves).
    pub page_count: u64,
    pub freelist_count: u64,
    /// Rows per table.
    pub rows: Vec<(String, u64)>,
    /// Pages per B-tree level, root first, per table and index (`sqlite_schema` first).
    pub trees: Vec<(String, Vec<u64>)>,
    /// `page_count` less the trees' pages and the free list: overflow pages, or a tree this walk
    /// did not reach. 0 when the ledger closes.
    pub unaccounted: i64,
    /// Per tree (the order of `trees`), what its leaf pages hold, field by field (r11-ever amendment
    /// 35's census): so leaf growth can be split into bytes (wider integers) and fill.
    pub census: Vec<(String, LeafCensus)>,
    /// The page size the census read (bytes per page).
    pub page_size: u64,
}

/// One B-tree's leaf pages, cell by cell (r11-ever amendment 35). SQLite's record format stores an
/// integer in the fewest of 1, 2, 3, 4, 6 or 8 bytes that hold it (0 and 1 in none), and a rowid
/// or a length as a varint, so a key or value that crosses 2^23 or a rowid that crosses 2^21 widens
/// its cell by a byte with no other change. Observation only.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeafCensus {
    pub leaf_pages: u64,
    pub cells: u64,
    /// Bytes of the cells: length and rowid varints plus the payload held on the page.
    pub cell_bytes: u64,
    /// `cell_bytes` plus each leaf page's 8-byte header and 2-byte cell pointers.
    pub used_bytes: u64,
    /// Integer fields stored in 1, 2, 3, 4, 6 and 8 bytes (serial types 1-6), then the 0/1
    /// constants (serial types 8 and 9).
    pub ints: [u64; 7],
    /// Bytes of the rowid varints (table leaves only).
    pub rowid_varint_bytes: u64,
    /// Cells whose payload spills to an overflow page: counted, not width-parsed.
    pub overflow_cells: u64,
}

/// A snapshot of the branch arena's accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchStats {
    /// Branch states that exist, including reaped branches kept alive by an open connection or by a
    /// live child. In the F7 splice arm (`DatabaseOpts::with_branch_splice`) only by two or more: one
    /// with exactly one is spliced into it and is not counted, while its pages are (they now belong
    /// to that child).
    pub live_branches: usize,
    /// Arena pages owned by some branch (or retained for one).
    pub arena_slots_in_use: usize,
    /// Arena pages on the free list.
    pub arena_slots_free: usize,
    /// Cumulative work counters, for attributing a latency curve to the loop that paid for it.
    pub work: BranchWork,
}

/// Cumulative counts of the store's per-call work since the database opened. Observation only:
/// nothing in the mechanism reads them. Each is updated once per call under the lock the call
/// already holds, from a loop index the call computes anyway, so counting adds no per-element step.
/// (Ported from the volatile store's counters, turso `c41a1909b`, so durable and volatile runs of
/// the same workload can be compared integer for integer. Recovery's replay counts too.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchWork {
    /// Page resolutions against the branch tree (one per branch-pager page read).
    pub resolve_calls: u64,
    /// Nodes consulted by those resolutions: the branch (its own pages and its inherited page map),
    /// then the trunk when neither holds the page — at most 2. (Before the persistent page map this
    /// counted the branch, each ancestor walked, and the trunk.)
    pub resolve_levels: u64,
    /// Retained versions compared against the fork epoch while resolving (`Lineage::retained_at`):
    /// at most one per lineage consulted, the page's born-predecessor. The O(log V) descent that
    /// finds it is not counted; time is the only instrument for it.
    pub resolve_retained_examined: u64,
    /// Retained versions released by `child_gone`: one per removal by key. (Before the born-ordered
    /// index this counted a position scan's comparisons.)
    pub gc_examined: u64,
    /// Index entries (`by_born` and `by_died`) visited by `child_gone`'s garbage query.
    pub gc_range_entries: u64,
    /// F7 durable port: released branches spliced into their only live child, splices that merged
    /// the child into the zombie's map (the child's side was the smaller), and versions the splices
    /// visited (the zombie's retained ones plus the smaller side of each merge).
    pub splices: u64,
    pub splice_commits: u64,
    pub splice_entries: u64,
    /// Entries a branch's lazily built page map (`view`) inserted at its first fork: its current
    /// versions born after its `inherited` map was taken (r11-ever amendment 17; r11-adversarial's
    /// counter of the same name counted all of `current`).
    pub view_build_entries: u64,
}

impl Branch {
    fn new(db: Arc<Database>, id: BranchId) -> Self {
        Self {
            db,
            id,
            released: false,
        }
    }

    pub fn id(&self) -> BranchId {
        self.id
    }

    /// Open a connection whose reads and writes see only this branch. One at a time: a second
    /// connection on the same branch is refused (see [`store::BranchStore::open`]).
    pub fn connect(&self) -> Result<Arc<Connection>> {
        self.db.connect_branch(self.id)
    }

    /// Fork a child of this branch. Refused with `Busy` while a write transaction is open on it.
    ///
    /// No admissibility check here: this branch passed [`check_forkable`] when its root was forked
    /// from the trunk, and journal-mode changes are refused while any branch exists.
    pub fn fork(&self) -> Result<Branch> {
        let id = self.db.branches.fork_branch(self.id)?;
        Ok(Branch::new(self.db.clone(), id))
    }

    /// Release this branch. Dropping the handle does the same; this form reports what was freed,
    /// and an error when the release could not be made durable (the branch is then kept, and comes
    /// back at the next open).
    pub fn reap(mut self) -> Result<Reaped> {
        self.released = true;
        self.db.branches.release_handle(self.id)
    }

    /// Grant or extend this branch's lease to `ttl` past now on the lease clock. A deadline only
    /// moves forward. When it passes, the next expiry pass — at a fork, at open, or on
    /// [`Database::expire_branches`] — reaps the branch whatever holds it; see `store.rs`.
    pub fn lease(&self, ttl: std::time::Duration) -> Result<()> {
        self.db.branches.set_lease(self.id, ttl)
    }

    /// Detach this branch from its handle WITHOUT releasing it — the branch outlives the handle
    /// (and, with durable branches, the process) until [`Database::branch`] re-attaches it and
    /// the new handle is reaped or dropped.
    pub fn into_id(mut self) -> BranchId {
        self.released = true;
        self.db.branches.detach(self.id);
        self.id
    }

    /// The arena slots this branch currently owns or retains, for membership assertions.
    #[doc(hidden)]
    pub fn owned_slots(&self) -> Vec<u32> {
        self.db.branches.owned_slots(self.id)
    }
}

impl Drop for Branch {
    fn drop(&mut self) {
        if !self.released {
            // A drop cannot report; the store has already logged a release that failed.
            let _ = self.db.branches.release_handle(self.id);
        }
    }
}

/// A pager's claim on the branch it serves. It lives exactly as long as the pager, so dropping the
/// connection — cleanly or not — is what closes the branch for connections and releases a write
/// lock an abandoned transaction still held. A `Drop`, not a call at `close()`: a connection can go
/// away without `close()`, and the next writer would then wait on a lock nobody holds.
pub(crate) struct BranchBinding {
    pub(crate) store: Arc<BranchStore>,
    pub(crate) id: BranchId,
}

impl Drop for BranchBinding {
    fn drop(&mut self) {
        self.store.close(self.id);
    }
}

/// Every condition under which a fork is refused, in one place. MVCC is the pre-registered one
/// (see the module doc); the rest are paths this fork has not made branch-aware.
fn check_forkable(db: &Database, pager: &Pager) -> Result<()> {
    check_branchable(db.mvcc_enabled())?;
    if !pager.db_initialized() {
        return Err(Unbranchable::Empty.into());
    }
    if pager.is_encryption_ctx_set() || pager.has_external_page_codec() {
        return Err(Unbranchable::Encrypted.into());
    }
    if pager.get_auto_vacuum_mode() != AutoVacuumMode::None {
        return Err(Unbranchable::AutoVacuum.into());
    }
    if db.opts.enable_multiprocess_wal {
        return Err(Unbranchable::MultiProcess.into());
    }
    Ok(())
}

impl Connection {
    /// Fork a branch from whatever this connection is on: the trunk, or the branch it was opened
    /// on. The branch sees the committed state at the moment of the fork.
    pub fn fork_branch(self: &Arc<Connection>) -> Result<Branch> {
        // First: on a read-only handle of a database with branches, nothing below would refuse.
        self.db.branches.refuse_if_trunk_only("fork")?;
        if self.get_tx_state() != TransactionState::None {
            return Err(LimboError::InvalidArgument(
                "cannot fork inside a transaction: a branch starts from committed state, so \
                 commit or roll back first"
                    .to_string(),
            ));
        }
        let pager = self.pager.load().clone();
        check_forkable(&self.db, &pager)?;
        let id = match pager.branch_id() {
            Some(parent) => self.db.branches.fork_branch(parent)?,
            None => self.fork_trunk(&pager)?,
        };
        Ok(Branch::new(self.db.clone(), id))
    }

    /// Fork the trunk under its WAL write lock. The lock is the point: a trunk write transaction
    /// in flight across the fork took its copy decisions for the previous epoch, so the pages it
    /// commits afterwards would be visible to the new branch. Holding the writer lock means there
    /// is no such transaction, and the read snapshot it forces is the latest commit.
    fn fork_trunk(self: &Arc<Connection>, pager: &Arc<Pager>) -> Result<BranchId> {
        const SNAPSHOT_RETRIES: usize = 8;
        let mut attempt = 0;
        loop {
            pager.begin_read_tx()?;
            let begun = pager
                .io
                .block(|| pager.begin_write_tx(WalAutoActions::empty()));
            match begun {
                Ok(()) => {}
                Err(LimboError::BusySnapshot) if attempt < SNAPSHOT_RETRIES => {
                    pager.end_read_tx();
                    attempt += 1;
                    continue;
                }
                Err(err) => {
                    pager.end_read_tx();
                    return Err(err);
                }
            }
            let forked = self.fork_trunk_locked(pager);
            pager.end_write_tx();
            pager.end_read_tx();
            return forked;
        }
    }

    fn fork_trunk_locked(self: &Arc<Connection>, pager: &Arc<Pager>) -> Result<BranchId> {
        let cookie = pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie.get()))?;
        // The branch starts with the schema that matches the committed pages it will read. The
        // connection's own snapshot or the shared one is that schema whenever the cookie agrees;
        // if neither does, a DDL commit is between publishing its pages and its schema, and the
        // caller retries rather than fork a branch whose schema disagrees with its pages.
        let schema = [self.schema.read().clone(), self.db.clone_schema()]
            .into_iter()
            .find(|schema| schema.schema_version == cookie)
            .ok_or(LimboError::SchemaUpdated)?;
        let page_size = pager.get_page_size_unchecked().get() as usize;
        self.db.branches.fork_trunk(schema, page_size)
    }

    /// The branch this connection is open on, if any.
    pub fn branch_id(&self) -> Option<BranchId> {
        self.pager.load().branch_id()
    }
}

impl Database {
    /// Refused on a read-only handle of a database with branches, whose branch store is not open.
    pub fn branch_stats(&self) -> Result<BranchStats> {
        self.branches.stats()
    }

    /// What the branch store's open read and rebuilt (r11-restart lane instrument).
    #[doc(hidden)]
    pub fn branch_open_stats(&self) -> BranchOpenStats {
        self.branches.open_stats()
    }

    /// `(branch states read from the catalog, trunk pages read, catalog queries, catalog rows
    /// read)` since open; zeros for a store that is not a catalog store (r11-restart instrument).
    #[doc(hidden)]
    pub fn branch_catalog_counters(&self) -> (u64, u64, u64, u64) {
        self.branches.catalog_counters()
    }

    /// What this open's prewarm did (r12-catload instrument, `R12_PREWARM`): `(mode, files warmed in
    /// the OS page cache, bytes read, bytes whose read-ahead was requested, catalog pages read
    /// through its page cache, interior pages among them, catalog page cache capacity after it,
    /// nanoseconds)`.
    #[doc(hidden)]
    pub fn branch_prewarm(&self) -> (&'static str, u64, u64, u64, u64, u64, u64, u64) {
        let p = self.branches.prewarm_stats();
        (
            p.mode.name(),
            p.files,
            p.bytes,
            p.advised,
            p.pages,
            p.interior,
            p.cache_pages,
            p.ns,
        )
    }

    /// r13-compose I9: `(size_of::<BranchState>(), size_of::<Option<(BranchId, BranchState)>>())`,
    /// the bytes behind one resident state and one table slot.
    #[doc(hidden)]
    pub fn branch_state_sizes() -> (usize, usize) {
        store::state_sizes()
    }

    /// The Merger's work since open (r13-compose, the Merger port; observing only).
    #[doc(hidden)]
    pub fn branch_merge_work(&self) -> BranchMergeWork {
        self.branches.merge_work()
    }

    /// r13-compose I13: a resident branch state's distinct pages over its current and retained
    /// versions; `None` when the state is not resident (observing only; S1 reads it at each push).
    #[doc(hidden)]
    pub fn branch_state_pages(&self, id: BranchId) -> Option<u64> {
        self.branches.state_pages(id)
    }

    /// The catalog store's checkpoint work, resident state and listing work (githost-shape
    /// instrument; runs no catalog query).
    #[doc(hidden)]
    pub fn branch_cat_shape(&self) -> BranchCatShape {
        self.branches.cat_shape()
    }

    /// Catalog stores: keep at most `cap` branch states resident after each checkpoint, evicting
    /// clean ones (githost-shape lane F-W3); `None` keeps every state touched since the open.
    #[doc(hidden)]
    pub fn branch_set_resident_cap(&self, cap: Option<usize>) {
        self.branches.set_resident_cap(cap)
    }

    /// Catalog statements that wrote a row since open; 0 for a store that is not a catalog store.
    #[doc(hidden)]
    pub fn branch_catalog_rows_written(&self) -> u64 {
        self.branches.catalog_rows_written()
    }

    /// The catalog file's shape (see [`CatalogShape`]); `None` for a store that is not a catalog
    /// store or has no catalog yet. Reads every catalog page.
    #[doc(hidden)]
    pub fn branch_catalog_shape(&self) -> Result<Option<CatalogShape>> {
        self.branches.catalog_shape()
    }

    /// `(resolve calls, arena slot reads)` since open (r11-restart lane instrument).
    #[doc(hidden)]
    pub fn branch_read_counters(&self) -> (u64, u64) {
        self.branches.read_counters()
    }

    /// `(len, nodes, bytes)` of F-W1's live-id set as the store holds it, `None` before the first listing of
    /// this process (githost-shape r3 instrument; a walk, O(nodes)).
    #[doc(hidden)]
    pub fn branch_live_id_census(&self) -> Option<(u64, u64, u64)> {
        self.branches.live_id_census()
    }

    /// V4's base read (r11-merge PREREG A20): fill `out` with `page` as the trunk held it when
    /// branch `id` forked. `Ok(true)`: read from the arena (a retained trunk version); `Ok(false)`:
    /// the trunk's current version is the base, and `out` is untouched.
    #[doc(hidden)]
    pub fn branch_base_page(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        self.branches.base_page_into(id, page, out)
    }

    /// `(base reads, arena-resolved, refused, retained versions examined, C-P trunk probes, C-P
    /// trunk rows)` since open (r11-merge A20 instrument).
    #[doc(hidden)]
    pub fn branch_v4_counters(&self) -> (u64, u64, u64, u64, u64, u64) {
        self.branches.v4_counters()
    }

    /// `(probes, rows)` of the once-per-page `written` probe since open (r12-composition K8-B).
    #[doc(hidden)]
    pub fn branch_twk_counters(&self) -> (u64, u64) {
        self.branches.twk_counters()
    }

    /// `(twk reads, tva probes, tva rows, tva reads)` since open (r12-composition K8-B amendment 13).
    #[doc(hidden)]
    pub fn branch_probe_split_counters(&self) -> (u64, u64, u64, u64) {
        self.branches.probe_split_counters()
    }

    /// Trunk pre-images the store holds now (r11-restart lane instrument).
    #[doc(hidden)]
    pub fn branch_trunk_retained(&self) -> u64 {
        self.branches.trunk_retained_count()
    }

    /// Whether `slot` is on the arena free list, for membership assertions.
    #[doc(hidden)]
    pub fn branch_slot_is_free(&self, slot: u32) -> bool {
        self.branches.slot_is_free(slot)
    }

    /// Every arena slot currently owned or retained, for membership assertions.
    #[doc(hidden)]
    pub fn branch_slots_in_use(&self) -> Vec<u32> {
        self.branches.slots_in_use()
    }

    /// Re-attach a detached branch — one whose handle went through [`Branch::into_id`], or any
    /// unreleased branch after a reopen.
    pub fn branch(self: &Arc<Database>, id: BranchId) -> Result<Branch> {
        self.branches.attach(id)?;
        Ok(Branch::new(self.clone(), id))
    }

    /// Every unreleased branch, attached or not. Refused on a read-only handle of a database with
    /// branches, whose branch store is not open: "none" would be a lie there.
    pub fn branch_ids(&self) -> Result<Vec<BranchId>> {
        self.branches.ids()
    }

    /// Reap every branch whose lease has run out, deepest first. The same pass also runs at every
    /// fork and at every open, so calling this is never required for reclamation to happen.
    pub fn expire_branches(&self) -> Result<Expired> {
        self.branches.expire_now()
    }

    /// Move the lease clock forward, for tests and benchmarks. It never moves back.
    #[doc(hidden)]
    pub fn branch_lease_clock_advance(&self, by: std::time::Duration) {
        self.branches.advance_lease_clock(by);
    }

    /// The lease clock now: time this database has been open, summed across opens.
    #[doc(hidden)]
    pub fn branch_lease_now(&self) -> std::time::Duration {
        self.branches.lease_now()
    }

    /// Stop real time moving the lease clock (it keeps its value; `branch_lease_clock_advance`
    /// still moves it), so a test can reach same-millisecond orderings deterministically.
    #[doc(hidden)]
    pub fn branch_lease_clock_freeze(&self) {
        self.branches.freeze_lease_clock();
    }

    /// Arm (or clear) a crash failpoint for the durability tests.
    #[doc(hidden)]
    pub fn branch_failpoint(&self, failpoint: Option<BranchFailpoint>) {
        self.branches.set_failpoint(failpoint);
    }

    /// The arena slots the last triggered failpoint left written but unpublished.
    #[doc(hidden)]
    pub fn branch_failpoint_orphans(&self) -> Vec<u32> {
        self.branches.failpoint_orphans()
    }

    /// Compact the branch log into a snapshot now. A no-op for volatile branches.
    #[doc(hidden)]
    pub fn branch_compact_now(&self) -> Result<()> {
        self.branches.compact_now()
    }

    /// Catalog checkpoint and settle counters (r11-restart-r2 instrument): `[checkpoints
    /// installed, fuzzy flights started, store-mutex hold ns inside checkpoints (sum, max), writer
    /// ns without the mutex, catalog statements under the mutex, settle batches, settle loads, most
    /// loads in one batch]`.
    #[doc(hidden)]
    pub fn branch_checkpoint_counters(&self) -> [u64; 9] {
        self.branches.checkpoint_counters()
    }

    /// Start a fuzzy catalog checkpoint now (F-FZ): its write runs on a thread of its own. `false`:
    /// nothing started (parked Commits remain after one bounded settle batch, one is in flight, or
    /// this is not a catalog store).
    #[doc(hidden)]
    pub fn branch_checkpoint_fuzzy_now(&self) -> Result<bool> {
        self.branches.checkpoint_fuzzy_now()
    }

    /// Make a fuzzy checkpoint in flight wait at `stage`: 2, its rows written and not committed;
    /// 3, committed and not installed. Any other value (0) releases it.
    #[doc(hidden)]
    pub fn branch_checkpoint_hold(&self, stage: u8) {
        self.branches.checkpoint_hold(stage);
    }

    /// Wait for every fuzzy checkpoint started so far to install.
    #[doc(hidden)]
    pub fn branch_checkpoint_wait(&self) {
        self.branches.checkpoint_wait();
    }

    /// The fuzzy checkpoint hook's value: the stage set, with 0x80 once a checkpoint waits there.
    #[doc(hidden)]
    pub fn branch_checkpoint_held(&self) -> u8 {
        self.branches.checkpoint_held()
    }

    /// The branch log file, for tearing its tail in a test. `None` for volatile branches, and
    /// before the first fork of a durable store.
    #[doc(hidden)]
    pub fn branch_log_path(&self) -> Option<std::path::PathBuf> {
        self.branches.log_path()
    }

    /// Open a connection on branch `id`: an ordinary connection whose pager is bound to the branch
    /// and whose schema is the branch's own.
    pub(crate) fn connect_branch(self: &Arc<Database>, id: BranchId) -> Result<Arc<Connection>> {
        let schema = self.branches.open_conn(id)?;
        // Built before anything fallible below, so an error there still closes the branch.
        let binding = BranchBinding {
            store: self.branches.clone(),
            id,
        };
        let pager = self._init(None, None)?;
        // `_init` read page 1 as the TRUNK sees it. Nothing the trunk put in this pager may
        // survive into the branch's view.
        pager.clear_page_cache(false);
        pager.set_schema_cookie(None);
        pager.bind_branch(binding)?;
        let pager = Arc::new(pager);
        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();
        let Some(schema) = schema else {
            // After a reopen nothing about the branch's schema is persisted: parse it from the
            // branch's own pages. The shared schema is only a placeholder for the parse — it
            // carries the built-in table-valued functions the parse keeps — and never reaches a
            // statement: the reparse replaces it before this returns. (If the trunk created
            // sqlite_stat1 after the fork, the placeholder's stats refresh reads a page the branch
            // does not have; gather errors are ignored there and the refresh after the reparse
            // overwrites whatever it found.)
            let conn = self._connect_with_pager_and_default_cache_size(
                false,
                pager,
                None,
                default_cache_size,
                Some(self.clone_schema()),
            )?;
            conn.force_reparse_schema_without_publish()?;
            crate::stats::refresh_analyze_stats(&conn);
            self.branches.set_schema(id, conn.schema.read().clone())?;
            return Ok(conn);
        };
        self._connect_with_pager_and_default_cache_size(
            false,
            pager,
            None,
            default_cache_size,
            Some(schema),
        )
    }
}

/// fork(2) tests run ALONE, in a fresh process of this test binary (review 4 C8). `fork` duplicates
/// every descriptor of the process, so a child forked beside other running tests holds their
/// journal locks until it exits, and a neighbour that drops a store and relocks its log inside that
/// window fails. In a process that runs one test there is no neighbour.
///
/// GATE: `cfg(unix)`, and nothing narrower is needed. The tests close no descriptors (the fresh
/// process has no neighbour whose lock a child could hold), so `getdtablesize`, which Android's
/// libc lacks, is no longer called anywhere. Every libc call they make — `fork`, `waitpid`,
/// `WIFEXITED`/`WEXITSTATUS`, `kill`, `_exit` — is declared for every unix target in libc 0.2.186,
/// Android included (READ: `src/unix/mod.rs`'s unconditional `extern` block, and
/// `src/unix/linux_like/mod.rs`).
#[cfg(all(test, unix))]
pub(crate) mod fork_driver {
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    const SENTINEL: &str = "TURSO_BRANCH_FORK_TEST_SENTINEL";

    /// In the harness: run the test whose full name is `name` alone in a fresh process, assert that
    /// it passed AND that it ran (a filter that matches nothing exits 0), and return `None`. In the
    /// fresh process: return the sentinel the test hands to [`finished`] as its last act.
    pub(crate) fn alone(name: &str) -> Option<PathBuf> {
        if let Some(sentinel) = std::env::var_os(SENTINEL) {
            return Some(PathBuf::from(sentinel));
        }
        let dir = tempfile::TempDir::new().unwrap();
        let sentinel = dir.path().join("finished");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([name, "--exact", "--test-threads=1", "--nocapture"])
            .env(SENTINEL, &sentinel)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(180);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{name} did not finish in its own process");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "{name} failed in its own process: {status}");
        assert!(
            sentinel.exists(),
            "the fresh process ran no test named {name}: a run that collected nothing has not passed"
        );
        None
    }

    pub(crate) fn finished(sentinel: &Path) {
        std::fs::write(sentinel, b"finished").unwrap();
    }

    /// The exit code of a forked child. Waits at most 60 s; on the deadline it SIGKILLs and reaps
    /// the child and fails.
    pub(crate) fn exit_code(pid: libc::pid_t) -> i32 {
        let mut status = 0;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            // SAFETY: `pid` is this process's child; `status` outlives the call.
            let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if reaped == pid {
                break;
            }
            assert_eq!(reaped, 0, "waitpid failed");
            if Instant::now() > deadline {
                // SAFETY: as above.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::waitpid(pid, &mut status, 0);
                }
                panic!("the forked child did not exit");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(libc::WIFEXITED(status), "the forked child did not exit normally: status {status}");
        libc::WEXITSTATUS(status)
    }
}

#[cfg(all(test, feature = "fs"))]
mod isolation_tests;

#[cfg(all(test, feature = "fs"))]
mod mechanism_tests;

#[cfg(all(test, feature = "fs"))]
mod durability_tests;

#[cfg(all(test, feature = "fs"))]
mod catalog_tests;

#[cfg(all(test, feature = "fs"))]
mod merge_durable_tests;

#[cfg(all(test, feature = "fs"))]
mod fastest_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_mvcc_database_is_refused() {
        let err = check_branchable(true).expect_err("an MVCC database must not be branchable");
        let msg = err.to_string();
        // The message must carry the MECHANISM, not only the verdict. A caller who is told "no"
        // works around it; a caller who is told their rows would be silently missing does not.
        assert!(msg.contains("experimental_mvcc"), "does not name the mode: {msg}");
        assert!(msg.contains("journal_mode=wal"), "does not name the way out: {msg}");
        assert!(
            msg.contains("silently miss"),
            "does not say the failure is SILENT, which is the whole reason this refuses: {msg}"
        );
    }

    #[test]
    fn a_wal_database_is_admitted() {
        // The other direction. Without this the guard could refuse everything and still pass the
        // test above — a refusal that is always taken is not a gate, it is a removal.
        check_branchable(false).expect("a WAL-mode database must be branchable");
    }

    #[test]
    fn trunk_is_distinguishable_from_every_fork() {
        assert!(BranchId::TRUNK.is_trunk());
        assert!(!BranchId(1).is_trunk());
        assert_ne!(BranchId::TRUNK, BranchId(1));
    }
}
