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

pub(crate) fn count_page_io(which: usize, n: u64) {
    PAGE_IO[which].fetch_add(n, crate::sync::atomic::Ordering::Relaxed);
}
pub(crate) mod catalog;
pub(crate) mod journal;
pub(crate) mod page_map;
pub(crate) mod store;

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
    /// recovered at open. `sync: false` writes the files without fsync: a measurement arm, not a
    /// durability guarantee.
    Durable { sync: bool },
    /// Durable, with the published fixes for an open that grows with the number of branches
    /// (r11-restart lane prototype; see `catalog.rs`): the same operation log, checkpointed
    /// incrementally into a B-tree catalog `<db>-branch-cat` instead of a whole-state snapshot, and
    /// read back on demand. `sync` as for `Durable`.
    Catalog { sync: bool },
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
    /// through it); its pages are freed when the last of those goes away.
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
    /// `collect_released` (branches scanned).
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
    /// Released branches the open's collection pass examined.
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
}

/// A snapshot of the branch arena's accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BranchStats {
    /// Branch states that exist, including reaped branches kept alive by a live child.
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

    /// Catalog statements that wrote a row since open; 0 for a store that is not a catalog store.
    #[doc(hidden)]
    pub fn branch_catalog_rows_written(&self) -> u64 {
        self.branches.catalog_rows_written()
    }

    /// `(resolve calls, arena slot reads)` since open (r11-restart lane instrument).
    #[doc(hidden)]
    pub fn branch_read_counters(&self) -> (u64, u64) {
        self.branches.read_counters()
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
