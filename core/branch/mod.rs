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
pub(crate) mod page_map;
pub(crate) mod store;
pub mod walpin;

use std::cell::Cell;

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

thread_local! {
    static SESSION_PROBE: Cell<Option<fn(&'static str)>> = const { Cell::new(None) };
}

/// Install (or remove) a probe that the connect path calls at each of its step boundaries, on this
/// thread only. Observation only: the open-session harness reads its allocator between labels to
/// split a connection's bytes by component. Unset, each call site costs one thread-local read.
#[doc(hidden)]
pub fn set_session_probe(probe: Option<fn(&'static str)>) {
    SESSION_PROBE.with(|p| p.set(probe));
}

pub(crate) fn session_probe(label: &'static str) {
    SESSION_PROBE.with(|p| {
        if let Some(f) = p.get() {
            f(label)
        }
    });
}

/// What one open connection holds, as integers, for the open-session harness.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionFootprint {
    /// Pages in the connection's private page cache.
    pub cached_pages: usize,
    /// The capacity of that cache, in pages (r11-walpin-conc's K × min(capacity, pages touched)).
    pub cache_capacity: usize,
    /// The connection's pager holds a WAL read lock (a read transaction is open).
    pub holds_read_lock: bool,
    /// On a branch: the connection's schema is the very `Arc` the branch store holds, not a copy.
    pub schema_shared_with_store: bool,
    /// Entries in the connection's own symbol table, by kind.
    pub sym_functions: usize,
    pub sym_collations: usize,
    pub sym_vtabs: usize,
    pub sym_vtab_modules: usize,
    pub sym_index_methods: usize,
}

/// `size_of` of the per-connection structures, for attributing the connect path's bytes.
#[doc(hidden)]
pub fn session_type_sizes() -> Vec<(&'static str, usize)> {
    vec![
        ("Connection", std::mem::size_of::<Connection>()),
        ("Pager", std::mem::size_of::<Pager>()),
        (
            "WalFile",
            std::mem::size_of::<crate::storage::wal::WalFile>(),
        ),
        (
            "PageCache",
            std::mem::size_of::<crate::storage::page_cache::PageCache>(),
        ),
        ("SymbolTable", std::mem::size_of::<crate::SymbolTable>()),
        ("Schema", std::mem::size_of::<crate::schema::Schema>()),
        (
            "Page",
            std::mem::size_of::<crate::storage::pager::Page>(),
        ),
    ]
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
/// The `lock_*` counters describe that lock itself; only `lock_hold_ns` adds work under it.
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
    /// `retained_by_born` entries visited by `child_gone`'s range query.
    pub gc_range_entries: u64,
    /// Acquisitions of the store's lock. Every store entry point takes it (a `stats` call counts
    /// its own), so this is an integer the workload fixes and load cannot move.
    pub lock_acquisitions: u64,
    /// Acquisitions that found the lock held by another thread and waited for it.
    pub lock_contended: u64,
    /// Nanoseconds those acquisitions waited, summed. The clock is read only on the contended path,
    /// by the waiting thread.
    pub lock_wait_ns: u64,
    /// Nanoseconds the lock was held, summed over acquisitions made while lock timing was on
    /// ([`Database::set_branch_lock_timing`]); 0 while it is off. The one counter that adds work
    /// inside the critical section: two clock reads per acquisition.
    pub lock_hold_ns: u64,
    /// Resolutions of a trunk page that the shared trunk-page cache answered.
    pub trunk_page_hits: u64,
    /// Resolutions of a trunk page it did not hold, which the pager then read through the WAL or
    /// the database file.
    pub trunk_page_misses: u64,
    /// Resolutions answered with a version the TRUNK retained for this branch (a pre-image of a page
    /// the trunk rewrote after the fork), copied into the caller's private buffer (FS9 off).
    pub retained_copies: u64,
    /// FS9: such resolutions answered by reference from the retained-version clone cache.
    pub retained_shared_hits: u64,
    /// FS9: clones built, one copy per retained trunk version, on its first resolution.
    pub retained_clone_fills: u64,
    /// Resolutions answered with a version an ANCESTOR BRANCH holds for this branch (through the
    /// inherited page map), copied into the caller's private buffer (FS9B off).
    pub inherited_copies: u64,
    /// FS9B: such resolutions answered by reference from the arena's slot clones.
    pub inherited_shared_hits: u64,
    /// FS9B: slot clones built, one copy per inherited slot, on its first resolution.
    pub inherited_clone_fills: u64,
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

    /// Release this branch. Dropping the handle does the same; this form reports what was freed.
    pub fn reap(mut self) -> Result<Reaped> {
        self.released = true;
        Ok(self.db.branches.release_handle(self.id))
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
            self.db.branches.release_handle(self.id);
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
    Ok(())
}

impl Connection {
    /// Fork a branch from whatever this connection is on: the trunk, or the branch it was opened
    /// on. The branch sees the committed state at the moment of the fork.
    pub fn fork_branch(self: &Arc<Connection>) -> Result<Branch> {
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
        let reserved_space = pager.get_reserved_space().ok_or_else(|| {
            LimboError::InternalError(
                "an initialized database's pager has no reserved-space byte".to_string(),
            )
        })?;
        self.db.branches.fork_trunk(schema, page_size, reserved_space)
    }

    /// The branch this connection is open on, if any.
    pub fn branch_id(&self) -> Option<BranchId> {
        self.pager.load().branch_id()
    }

    /// After this connection loaded analyze stats into its own copy of the branch schema, publish
    /// that copy to the branch store, so the branch's later connections share it instead of
    /// loading (and copying) again (FS3). Only outside a transaction: inside one, the stats may
    /// come from uncommitted `sqlite_stat1` rows, and the branch commit publishes the schema.
    pub(crate) fn publish_branch_schema_if_idle(&self) {
        let Some(id) = self.pager.load().branch_id() else {
            return;
        };
        if !self.get_auto_commit() || self.get_tx_state() != TransactionState::None {
            return;
        }
        let schema = self.schema.read().clone();
        if let Err(e) = self.db.branches.set_schema(id, schema) {
            tracing::warn!("failed to publish a branch's analyze stats: {e}");
        }
    }

    /// What this connection holds, as integers. Observation only.
    #[doc(hidden)]
    pub fn session_footprint(&self) -> SessionFootprint {
        let pager = self.pager.load();
        let schema = self.schema.read().clone();
        let schema_shared_with_store = pager.branch_id().is_some_and(|id| {
            self.db
                .branches
                .schema(id)
                .is_ok_and(|stored| Arc::ptr_eq(&stored, &schema))
        });
        let syms = self.syms.read();
        SessionFootprint {
            cached_pages: pager.page_cache_len(),
            cache_capacity: pager.page_cache_capacity(),
            holds_read_lock: pager.holds_read_lock(),
            schema_shared_with_store,
            sym_functions: syms.functions.len(),
            sym_collations: syms.collations.len(),
            sym_vtabs: syms.vtabs.len(),
            sym_vtab_modules: syms.vtab_modules.len(),
            sym_index_methods: syms.index_methods.len(),
        }
    }
}

impl Database {
    pub fn branch_stats(&self) -> BranchStats {
        self.branches.stats()
    }

    /// Time how long each acquisition holds the branch store's lock, into
    /// [`BranchWork::lock_hold_ns`]. Observation only; off by default.
    #[doc(hidden)]
    pub fn set_branch_lock_timing(&self, on: bool) {
        self.branches.set_lock_timing(on);
    }

    /// FS9 (r11-sessions): serve trunk pre-images retained for branches by reference. Normally taken
    /// from `TURSO_R11S_FS9=1` when the store is created; tests set it per database.
    #[doc(hidden)]
    pub fn set_fs9(&self, on: bool) {
        self.branches.set_fs9(on);
    }

    /// FS10 (r11-sessions): a branch session releases its private page-cache entries for pages it
    /// holds by reference from a shared pool (F6's cache, FS9's clones) when its last statement
    /// ends. Normally taken from `TURSO_R11S_FS10=1`; tests set it per database.
    #[doc(hidden)]
    pub fn set_fs10(&self, on: bool) {
        self.branches.set_fs10(on);
    }

    /// Trunk pages held in the shared trunk-page cache (the FS10 pool's size). Observation only.
    #[doc(hidden)]
    pub fn trunk_cache_pages(&self) -> usize {
        self.branches.trunk_cache_pages()
    }

    /// FS9B (r11-sessions): also serve versions inherited from ancestor BRANCHES by reference.
    /// Normally taken from `TURSO_R11S_FS9=2` (which turns FS9 on too); tests set it per database.
    #[doc(hidden)]
    pub fn set_fs9b(&self, on: bool) {
        self.branches.set_fs9b(on);
    }

    /// FS9B: arena slots currently cloned for sharing. Observation only.
    #[doc(hidden)]
    pub fn slot_clone_count(&self) -> usize {
        self.branches.slot_clone_count()
    }

    /// FS9: retained trunk versions currently cloned for sharing. Observation only.
    #[doc(hidden)]
    pub fn retained_clone_count(&self) -> usize {
        self.branches.retained_clone_count()
    }

    /// The trunk WAL's state, for the r11-walpin instrument (observation only).
    #[doc(hidden)]
    pub fn walpin_stats(&self) -> walpin::WalPinStats {
        self.shared_wal.read().walpin_stats()
    }

    /// Turn FW3 on or off for this database's branches (tests; the harness uses the process
    /// switch). Call before any branch connection is opened.
    #[doc(hidden)]
    pub fn walpin_set_fw3(&self, on: bool) {
        self.branches.set_fw3(on);
    }

    /// FW2: open the second WAL file (`<wal>2`). Call once, right after open, before any write.
    #[doc(hidden)]
    pub fn walpin_open_wal2(&self) -> Result<()> {
        let file = self
            .io
            .open_file(&format!("{}2", self.walpin_wal_path()), crate::OpenFlags::Create, false)?;
        self.shared_wal.read().walpin_set_wal2_file(file);
        Ok(())
    }

    /// The trunk WAL's max_frame alone: one atomic load under the shared WAL lock.
    #[doc(hidden)]
    pub fn walpin_max_frame(&self) -> u64 {
        self.shared_wal
            .read()
            .metadata
            .max_frame
            .load(crate::sync::atomic::Ordering::Acquire)
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

    /// Open a connection on branch `id`: an ordinary connection whose pager is bound to the branch
    /// and whose schema is the branch's own.
    ///
    /// The pager is built by `_init_branch`, bound before it reads anything, so page 1 — like every
    /// page after it — is read as the BRANCH sees it, and nothing of the trunk's can be left in it.
    pub(crate) fn connect_branch(self: &Arc<Database>, id: BranchId) -> Result<Arc<Connection>> {
        session_probe("begin");
        let schema = self.branches.open(id)?;
        // Built before anything fallible below, so an error there still closes the branch.
        let binding = BranchBinding {
            store: self.branches.clone(),
            id,
        };
        session_probe("store_open");
        let pager = self._init_branch(binding)?;
        session_probe("init_branch");
        pager.set_schema_cookie(None);
        let pager = Arc::new(pager);
        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();
        session_probe("bind_read_page1");
        self._connect_with_pager_and_default_cache_size(
            false,
            pager,
            None,
            default_cache_size,
            Some(schema),
        )
    }
}

#[cfg(all(test, feature = "fs"))]
mod isolation_tests;

#[cfg(all(test, feature = "fs"))]
mod mechanism_tests;

#[cfg(all(test, feature = "fs"))]
mod walpin_tests;

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
