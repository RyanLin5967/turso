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
/// The `lock_*` counters describe the store's locks themselves, summed over all of them (the trunk's
/// and every shard's); `trunk_lock_*` is the trunk's alone. Only the `*_hold_ns` pair adds work under
/// a lock.
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
    /// finds it is not counted; time is the only instrument for it. Since the striped store, the
    /// trunk's retained versions are consulted only for a page the trunk rewrote after the reader's
    /// fork, so a version the unlocked check proves irrelevant is no longer compared or counted.
    pub resolve_retained_examined: u64,
    /// Retained versions released by `child_gone`: one per removal by key. (Before the born-ordered
    /// index this counted a position scan's comparisons.)
    pub gc_examined: u64,
    /// `retained_by_born` entries visited by `child_gone`'s range query.
    pub gc_range_entries: u64,
    /// Acquisitions of the store's locks. Every store entry point takes at least one (a `stats`
    /// call counts its own: one per lock), so this is an integer the workload fixes and load
    /// cannot move.
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
    /// The trunk's lock alone: trunk forks, trunk copy decisions, reaps of trunk children, and
    /// resolutions of pages the trunk rewrote after the reader's fork.
    pub trunk_lock_acquisitions: u64,
    pub trunk_lock_contended: u64,
    pub trunk_lock_wait_ns: u64,
    pub trunk_lock_hold_ns: u64,
    /// The trunk's lock per site (r11-k3-trunklock amendment 3, a328c4d05; ported unchanged in meaning so both
    /// lanes measure one quantity, r11-coherence amendment 16).
    pub trunk_sites: TrunkSites,
}

/// The sites at which the store takes the trunk's lock, in the order of [`TrunkSites`]' arrays:
/// a trunk fork, the reap of a trunk child (`child_gone`, or FK's garbage pass), a resolution of a
/// page the trunk rewrote, a trunk copy decision (`first_write_trunk`), and accounting and
/// membership queries.
pub const TRUNK_LOCK_SITES: [&str; 5] = ["fork_trunk", "reap", "resolve", "trunk_write", "observe"];

/// The trunk lock's accounting per site ([`TRUNK_LOCK_SITES`]), counted under the lock itself.
/// Observation only. `hold_ns` is counted only while lock timing is on, since timing a hold adds
/// two clock reads inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrunkSites {
    pub acquisitions: [u64; 5],
    pub contended: [u64; 5],
    pub wait_ns: [u64; 5],
    pub hold_ns: [u64; 5],
}

impl TrunkSites {
    fn add(&mut self, other: &TrunkSites) {
        for i in 0..TRUNK_LOCK_SITES.len() {
            self.acquisitions[i] += other.acquisitions[i];
            self.contended[i] += other.contended[i];
            self.wait_ns[i] += other.wait_ns[i];
            self.hold_ns[i] += other.hold_ns[i];
        }
    }
}

impl BranchWork {
    /// Add every counter of `other` into this one.
    pub fn add(&mut self, other: &BranchWork) {
        let BranchWork {
            resolve_calls,
            resolve_levels,
            resolve_retained_examined,
            gc_examined,
            gc_range_entries,
            lock_acquisitions,
            lock_contended,
            lock_wait_ns,
            lock_hold_ns,
            trunk_page_hits,
            trunk_page_misses,
            trunk_lock_acquisitions,
            trunk_lock_contended,
            trunk_lock_wait_ns,
            trunk_lock_hold_ns,
            trunk_sites,
        } = *other;
        self.resolve_calls += resolve_calls;
        self.resolve_levels += resolve_levels;
        self.resolve_retained_examined += resolve_retained_examined;
        self.gc_examined += gc_examined;
        self.gc_range_entries += gc_range_entries;
        self.lock_acquisitions += lock_acquisitions;
        self.lock_contended += lock_contended;
        self.lock_wait_ns += lock_wait_ns;
        self.lock_hold_ns += lock_hold_ns;
        self.trunk_page_hits += trunk_page_hits;
        self.trunk_page_misses += trunk_page_misses;
        self.trunk_lock_acquisitions += trunk_lock_acquisitions;
        self.trunk_lock_contended += trunk_lock_contended;
        self.trunk_lock_wait_ns += trunk_lock_wait_ns;
        self.trunk_lock_hold_ns += trunk_lock_hold_ns;
        self.trunk_sites.add(&trunk_sites);
    }
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
        crate::coherence::bump(crate::coherence::Class::DbArc, 2);
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
    pub(crate) store: crate::anchor::DbRef<BranchStore>,
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
        // The branch handle's Arc<Database> clone and its drop at the reap.
        crate::coherence::bump(crate::coherence::Class::DbArc, 2);
        Ok(Branch::new(self.db.clone(), id))
    }

    /// Fork the trunk under its WAL write lock. The lock is the point: a trunk write transaction
    /// in flight across the fork took its copy decisions for the previous epoch, so the pages it
    /// commits afterwards would be visible to the new branch. Holding the writer lock means there
    /// is no such transaction, and the read snapshot it forces is the latest commit.
    fn fork_trunk(self: &Arc<Connection>, pager: &Arc<Pager>) -> Result<BranchId> {
        const SNAPSHOT_RETRIES: usize = 8;
        let mut attempt = 0;
        if pager.fork_gate_on() {
            // r11-coherence FG: a reader of the fork gate. Trunk write transactions hold it exclusively for as long
            // as they hold the WAL write lock, so the guarantee below is the one the write lock gave, and forks no
            // longer exclude each other.
            loop {
                pager.begin_read_tx()?;
                match pager.fork_gate_enter() {
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
                pager.fork_gate_exit();
                pager.end_read_tx();
                return forked;
            }
        }
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
        // FA: clone only the schema that matches, and take the database's schema Mutex only if the connection's
        // own does not.
        let schema = if crate::coherence::fix(crate::coherence::FIX_ARC) {
            let own = self.schema.read();
            if own.schema_version == cookie {
                crate::coherence::bump(crate::coherence::Class::SchemaArc, 1);
                own.clone()
            } else {
                drop(own);
                let shared = self.db.clone_schema();
                if shared.schema_version != cookie {
                    return Err(LimboError::SchemaUpdated);
                }
                shared
            }
        } else {
            crate::coherence::bump(crate::coherence::Class::SchemaArc, 2);
            [self.schema.read().clone(), self.db.clone_schema()]
                .into_iter()
                .find(|schema| schema.schema_version == cookie)
                .ok_or(LimboError::SchemaUpdated)?
        };
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
        let schema = self.branches.open(id)?;
        // Built before anything fallible below, so an error there still closes the branch.
        // FU: the store through this thread's anchor.
        let binding = BranchBinding {
            store: match self.anchor() {
                Some(a) => crate::anchor::DbRef::anchored(&a, |a| &a.branches),
                None => {
                    crate::coherence::bump(crate::coherence::Class::DbArc, 2);
                    self.branches.clone().into()
                }
            },
            id,
        };
        let pager = self._init_branch(binding)?;
        pager.set_schema_cookie(None);
        let pager = Arc::new(pager);
        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();
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
