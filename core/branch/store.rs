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
//! live siblings — which is a range query over `retained_by_born`, not a scan.
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
//! * One `Mutex` guards every branch, held across the durable store's fsyncs. Correct, and a named
//!   wall under concurrent writers; the benchmark this lane ships is single-threaded and says so.
//! * Resolution walks the ancestor chain: cost grows with DEPTH, not with the number of branches.
//! * Recovery is eager: every branch map is materialised at open, O(live branch state).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::arena::{Arena, Slot};
use super::journal::{BranchFiles, Journal, Record, SnapBranch, SnapshotState};
use super::{BranchDurability, BranchFailpoint, BranchId, BranchStats, Expired, Reaped};
use crate::schema::Schema;
use crate::storage::pager::PageRef;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

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
    /// Trunk pre-image records wait in the journal's buffer between the trunk's `add_dirty` and its
    /// commit. The commit's barrier reads this without the lock, so a trunk commit with nothing to
    /// make durable pays one atomic load.
    unsynced: AtomicBool,
    /// Whether a durable store has any lease outstanding. A trunk commit's barrier reads it without
    /// the lock, so a trunk with no leases still pays one load for the stamp (review N2). A stale
    /// `true` costs one lock; a stale `false` misses one stamp, which only lengthens leases.
    leases_outstanding: AtomicBool,
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
}

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
}

#[derive(Default)]
struct Lineage {
    /// Advanced by each fork of this node; the pre-increment value is the child's fork epoch.
    epoch: u64,
    /// Live children by fork epoch. Fork epochs are unique within a parent.
    children: BTreeMap<u64, BranchId>,
    /// Superseded versions kept because a live child forked while they were current.
    retained: HashMap<u32, Vec<Retained>>,
    /// The same versions keyed by `born`, for the reclamation range query: born -> [(page, died)].
    retained_by_born: BTreeMap<u64, Vec<(u32, u64)>>,
}

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

/// The finest grain at which expiry passes queue a clock stamp.
const STAMP_EVERY_MS: u64 = 1000;

/// A `Duration` in lease-clock milliseconds, saturating: `as_millis() as u64` WRAPS, which turns a
/// practically-infinite lease into a short one (review R6).
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn fail_stopped(id: BranchId, what: &str) -> LimboError {
    LimboError::InternalError(format!(
        "branch store is fail-stopped after an I/O failure; branch {} takes {what} until the \
         database is reopened",
        id.0
    ))
}

struct BranchState {
    parent: BranchId,
    fork_epoch: u64,
    lineage: Lineage,
    /// The branch's current version of every page it has committed.
    current: HashMap<u32, Owned>,
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
}

impl BranchState {
    /// F4. A released branch never reads its own `current` again, never writes, and takes no new
    /// child. So each current version becomes a retained one that died at the branch's epoch — a
    /// live child forked at `f` reads it exactly as before, because `born <= f < epoch` — and every
    /// one that no live child forked inside `[born, epoch)` goes to `freed` NOW rather than when the
    /// last child goes. That is the interval rule with the free epoch set to the release epoch
    /// (ferrodb's `retire_arenas_by_rule`); `Lineage::child_gone` then frees the rest incrementally.
    /// Only for a branch with no open connection: an open one still reads `current` at `u64::MAX`.
    fn retire_current(&mut self, freed: &mut Vec<Slot>) {
        let epoch = self.lineage.epoch;
        for (page, owned) in self.current.drain() {
            if self.lineage.has_child_in(owned.born, epoch) {
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
    }
}

#[derive(Clone, Copy)]
struct Owned {
    slot: Slot,
    born: u64,
    crc: u32,
}

impl Lineage {
    /// True if a live child forked in `[from, to)` can see a version current over that range.
    fn has_child_in(&self, from: u64, to: u64) -> bool {
        from < to && self.children.range(from..to).next().is_some()
    }

    fn retain(&mut self, page: u32, v: Retained) {
        self.retained.entry(page).or_default().push(v);
        self.retained_by_born
            .entry(v.born)
            .or_default()
            .push((page, v.died));
    }

    /// The retained version of `page` visible to a child forked at `f`.
    fn retained_at(&self, page: u32, f: u64) -> Option<(Slot, u32)> {
        self.retained
            .get(&page)?
            .iter()
            .find(|v| v.born <= f && f < v.died)
            .map(|v| (v.slot, v.crc))
    }

    /// Detach the child forked at `f`; every retained version only it could see goes to `freed`.
    fn child_gone(&mut self, f: u64, freed: &mut Vec<Slot>) {
        let removed = self.children.remove(&f);
        crate::turso_assert!(removed.is_some(), "detached a child the parent does not list");
        let lo = self.children.range(..f).next_back().map(|(&e, _)| e);
        let hi = self.children.range(f..).next().map(|(&e, _)| e);
        // A version containing f is garbage iff its range [born, died) now holds no live child:
        // born > lo (nothing live below) and died <= hi (nothing live above).
        let from = match lo {
            Some(lo) => Bound::Excluded(lo),
            None => Bound::Unbounded,
        };
        let mut dead = Vec::new();
        for (&born, entries) in self.retained_by_born.range((from, Bound::Included(f))) {
            for &(page, died) in entries {
                if died > f && hi.is_none_or(|hi| died <= hi) {
                    dead.push((born, page));
                }
            }
        }
        for &(born, page) in &dead {
            let versions = self.retained.get_mut(&page).expect("indexed version is listed");
            let at = versions
                .iter()
                .position(|v| v.born == born)
                .expect("indexed version is listed");
            let v = versions.swap_remove(at);
            if versions.is_empty() {
                self.retained.remove(&page);
            }
            let by_born = self
                .retained_by_born
                .get_mut(&born)
                .expect("listed version is indexed");
            by_born.retain(|&(p, _)| p != page);
            if by_born.is_empty() {
                self.retained_by_born.remove(&born);
            }
            freed.push(v.slot);
        }
    }

    fn release_all(self, freed: &mut Vec<Slot>) {
        for (_, versions) in self.retained {
            freed.extend(versions.into_iter().map(|v| v.slot));
        }
    }

    fn retained_list(&self) -> Vec<(u32, u64, u64, Slot, u32)> {
        let mut out: Vec<(u32, u64, u64, Slot, u32)> = self
            .retained
            .iter()
            .flat_map(|(&page, vs)| vs.iter().map(move |v| (page, v.born, v.died, v.slot, v.crc)))
            .collect();
        out.sort_unstable();
        out
    }
}

fn gone(id: BranchId) -> LimboError {
    LimboError::InternalError(format!("branch {} does not exist", id.0))
}

impl BranchStore {
    /// The store for a database at `db_path`. A durable store recovers whatever its files hold;
    /// a volatile one refuses a database whose files say it has durable branches, because opened
    /// volatile, the trunk's writes would skip the pre-image barrier and silently change what
    /// those branches read.
    pub(crate) fn open(
        durability: BranchDurability,
        default_lease: Option<Duration>,
        db_path: &str,
    ) -> Result<Self> {
        let memory = crate::is_memory_like(db_path);
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
                let mut inner = StoreInner::fresh(Some(files.clone()), sync, default_lease);
                if files.exist() {
                    if let Some(recovered) = Journal::recover(&files, sync)? {
                        if let Some(snapshot) = recovered.snapshot {
                            inner.load_snapshot(snapshot)?;
                        }
                        // Frees during replay are not acted on: the free set is derived below
                        // from what the recovered state references.
                        let mut ignored = Vec::new();
                        for record in &recovered.records {
                            inner.replay(record, &mut ignored)?;
                        }
                        // A snapshot can hold a released branch that was kept only by an open
                        // connection; after a restart nothing is open.
                        inner.collect_released(&mut ignored);
                        let referenced = inner.referenced_slots();
                        inner.arena = Some(Arena::open_file(
                            &files.arena,
                            recovered.page_size,
                            false,
                            &referenced,
                        )?);
                        inner.journal = Some(recovered.journal);
                    }
                }
                inner
            }
        };
        let store = Self {
            trunk_children: AtomicUsize::new(inner.trunk.lineage.children.len()),
            inner: Mutex::new(inner),
            unsynced: AtomicBool::new(false),
            leases_outstanding: AtomicBool::new(false),
        };
        // A branch whose lease ran out before the last close — or before the last flush that
        // carried a stamp, if the process crashed — goes now, with nobody having to ask: this is
        // what makes a crashed agent's branch temporary. The clock resumed where it was last
        // stamped, so nothing expires here that had time left then.
        {
            let mut inner = store.inner.lock();
            store.expire(&mut inner, Stamp::No)?;
            store.sync_lease_flag(&inner);
        }
        Ok(store)
    }

    pub(crate) fn trunk_has_children(&self) -> bool {
        self.trunk_children.load(Ordering::Acquire) > 0
    }

    fn sync_trunk_children(&self, inner: &StoreInner) {
        self.trunk_children
            .store(inner.trunk.lineage.children.len(), Ordering::Release);
    }

    /// Call after anything that adds or removes a lease.
    fn sync_lease_flag(&self, inner: &StoreInner) {
        self.leases_outstanding.store(
            inner.journal.is_some() && !inner.leases.is_empty(),
            Ordering::Release,
        );
    }

    /// Whether any branch state exists at all, including one kept alive only by a live child.
    /// Paths that rewrite the trunk without passing through `add_dirty` refuse while this holds.
    pub(crate) fn has_branches(&self) -> bool {
        !self.inner.lock().branches.is_empty()
    }

    /// Append `records` and make them durable with ONE flush before the caller acts on any of
    /// them. A no-op when volatile.
    fn log_all(&self, inner: &mut StoreInner, records: Vec<Record>) -> Result<()> {
        let StoreInner {
            journal,
            arena,
            failpoint,
            lease,
            ..
        } = inner;
        let (Some(journal), Some(arena)) = (journal.as_mut(), arena.as_mut()) else {
            return Ok(());
        };
        injected_flush_failure(failpoint, journal)?;
        for record in &records {
            journal.buffer(record)?;
        }
        journal.flush(arena)?;
        lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Grant or extend `id`'s lease to `ttl` past the lease clock's now. A deadline only moves
    /// forward (Chubby §2.8: the master "is free to advance this timeout further into the future,
    /// but may not move it backwards in time").
    pub(crate) fn set_lease(&self, id: BranchId, ttl: Duration) -> Result<()> {
        let mut inner = self.inner.lock();
        // A lease that has run out is not renewable: reap first, so a late renewal is refused
        // rather than reviving the branch.
        // One `now` for the whole operation (review R5): the renewal is decided at the instant
        // the pass judged the lease, not after the pass's own flush.
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
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
        let mut inner = self.inner.lock();
        // A fail-stopped pass reaps nothing and stamps nothing: an empty `Expired` would say
        // "nothing was due" when the truth is "could not run" (review N4).
        if inner.poisoned() {
            return Err(LimboError::InternalError(
                "branch store is fail-stopped after an I/O failure; the expiry pass cannot make a \
                 release durable until the database is reopened"
                    .to_string(),
            ));
        }
        Ok(self.expire(&mut inner, Stamp::Flush)?.0)
    }

    /// Reap every branch whose lease has run out — non-cooperatively: attached, detached, or open
    /// (an open one takes no more writes and is freed when its connection closes). Deepest first,
    /// so a chain that expires together goes child before parent and each interior is freed whole
    /// rather than retired and then freed. Each release takes F4's path: an interior with a live
    /// child keeps exactly the versions that child can read. The Release records are made durable
    /// together, before anything is freed.
    fn expire(&self, inner: &mut StoreInner, stamp: Stamp) -> Result<(Expired, u64)> {
        let now = inner.lease.now_ms();
        // A fail-stopped store cannot make a Release durable, so it reaps nothing (and frees
        // nothing); reads stay available and the next open recovers from disk.
        if inner.poisoned() {
            return Ok((Expired::default(), now));
        }
        let mut due: Vec<BranchId> = inner
            .leases
            .range(..=(now, BranchId(u64::MAX)))
            .map(|&(_, id)| id)
            .collect();
        if due.is_empty() {
            if !inner.leases.is_empty() {
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
        due.sort_by_key(|&id| (std::cmp::Reverse(inner.depth(id)), id));
        let mut records: Vec<Record> = due
            .iter()
            .map(|id| Record::Release { branch: id.0 })
            .collect();
        records.push(Record::Clock { now_ms: now });
        inner.lease.queued(now);
        self.log_all(inner, records)?;
        let mut freed = Vec::new();
        for &id in &due {
            inner.apply_release(id, &mut freed);
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

    /// Append `record` and make it durable before the caller acts on it. A no-op when volatile.
    fn log(&self, inner: &mut StoreInner, record: Record) -> Result<()> {
        let StoreInner {
            journal,
            arena,
            failpoint,
            lease,
            ..
        } = inner;
        let (Some(journal), Some(arena)) = (journal.as_mut(), arena.as_mut()) else {
            return Ok(());
        };
        injected_flush_failure(failpoint, journal)?;
        journal.buffer(&record)?;
        journal.flush(arena)?;
        lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        Ok(())
    }

    /// Compact the log into a snapshot if it has outgrown the live state. Best effort: the
    /// operation that triggered it is already durable, and a failure before the rename leaves the
    /// log intact; a failure after it fail-stops the journal (see `Journal::compact`).
    fn maybe_compact(&self, inner: &mut StoreInner) {
        if inner.journal.as_ref().is_some_and(|j| j.wants_compaction()) {
            if let Err(e) = self.compact(inner, false) {
                tracing::warn!("branch store compaction failed: {e}");
            }
        }
    }

    fn compact(&self, inner: &mut StoreInner, fail_after_rename: bool) -> Result<()> {
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
    pub(crate) fn fork_trunk(&self, schema: Arc<Schema>, page_size: usize) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        inner.ensure_backing(page_size)?;
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        let id = BranchId(inner.next_id);
        let (records, lease) = inner.fork_records(id, BranchId::TRUNK, now);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        self.log_all(&mut inner, records)?;
        inner.apply_fork(BranchId::TRUNK, id, Some(schema), Handle::Attached)?;
        inner.apply_fork_lease(id, lease);
        self.sync_trunk_children(&inner);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok(id)
    }

    /// Fork a child of a branch. Refused while the parent has a write transaction in progress, for
    /// the same reason a trunk fork takes the WAL write lock, and refused on a released branch.
    pub(crate) fn fork_branch(&self, parent: BranchId) -> Result<BranchId> {
        let mut inner = self.inner.lock();
        // Reap what has expired first, so a parent whose lease ran out is refused rather than
        // revived by a child that would pin it (Neon refuses to "create children from expiring
        // branches").
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        let st = inner.branches.get(&parent).ok_or_else(|| gone(parent))?;
        if st.writer {
            return Err(LimboError::Busy);
        }
        if st.handle.is_released() {
            return Err(reaped(parent));
        }
        let schema = st.schema.clone();
        let id = BranchId(inner.next_id);
        let (records, lease) = inner.fork_records(id, parent, now);
        if let Some((_, stamped)) = lease {
            inner.lease.queued(stamped);
        }
        self.log_all(&mut inner, records)?;
        inner.apply_fork(parent, id, schema, Handle::Attached)?;
        inner.apply_fork_lease(id, lease);
        self.sync_lease_flag(&inner);
        self.maybe_compact(&mut inner);
        Ok(id)
    }

    /// Mark the branch open for a connection and return its committed schema (`None` after a
    /// reopen: the caller reparses it). One connection per branch: two would each hold a private
    /// page cache of the same page space, and nothing would tell one that the other had committed
    /// — a silently stale read, so it is refused.
    pub(crate) fn open_conn(&self, id: BranchId) -> Result<Option<Arc<Schema>>> {
        let mut inner = self.inner.lock();
        // Nor is an expired branch openable: the same pass, the same refusal.
        let (_, now) = self.expire(&mut inner, Stamp::Queue)?;
        let poisoned = inner.poisoned();
        let st = inner.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        if st.handle.is_released() {
            return Err(reaped(id));
        }
        // A fail-stopped pass cannot reap (it cannot make a Release durable), so the refusal that
        // reaping gives an expired branch is given here instead (review N4). Unexpired branches
        // stay readable.
        if poisoned && st.lease.is_some_and(|deadline| deadline <= now) {
            return Err(LimboError::InvalidArgument(format!(
                "branch {}'s lease has run out; the branch store is fail-stopped after an I/O \
                 failure, so it is reaped when the database is reopened",
                id.0
            )));
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
        let mut inner = self.inner.lock();
        let mut freed = Vec::new();
        if let Some(st) = inner.branches.get_mut(&id) {
            st.open = false;
            st.writer = false;
            freed.extend(st.pending.drain().map(|(_, slot)| slot));
        }
        inner.collect(id, &mut freed);
        inner.release_slots(freed);
        self.sync_trunk_children(&inner);
    }

    /// The `Branch` handle has gone: release the branch.
    ///
    /// An error when the release could not be made durable (review N4): the branch is then kept —
    /// nothing is freed in this process — and comes back, detached, at the next open.
    pub(crate) fn release_handle(&self, id: BranchId) -> Result<Reaped> {
        let mut inner = self.inner.lock();
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
            Handle::ReleasePending => return Err(fail_stopped(id, "no release")),
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
        inner.apply_release(id, &mut freed);
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

    /// Detach a live branch from its handle without releasing it.
    pub(crate) fn detach(&self, id: BranchId) {
        if let Some(st) = self.inner.lock().branches.get_mut(&id) {
            if st.handle == Handle::Attached {
                st.handle = Handle::Detached;
            }
        }
    }

    /// Give a detached branch a handle again. One handle per branch.
    pub(crate) fn attach(&self, id: BranchId) -> Result<()> {
        let mut inner = self.inner.lock();
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

    /// Every unreleased branch.
    pub(crate) fn ids(&self) -> Vec<BranchId> {
        let inner = self.inner.lock();
        let mut ids: Vec<BranchId> = inner
            .branches
            .iter()
            .filter(|(_, st)| !st.handle.is_released())
            .map(|(&id, _)| id)
            .collect();
        ids.sort();
        ids
    }

    pub(crate) fn begin_write(&self, id: BranchId) -> Result<()> {
        let mut inner = self.inner.lock();
        // A fail-stopped store takes no write: a commit would write its pages into the arena
        // before its record failed, possibly into a slot durable state still names (review R1).
        if inner.poisoned() {
            return Err(fail_stopped(id, "no write transaction"));
        }
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
        let inner = self.inner.lock();
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
        // A transaction that began before the journal failed may write no further page.
        if inner.poisoned() {
            return Err(fail_stopped(id, "no page write"));
        }
        let StoreInner {
            arena, branches, ..
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
        }
        Ok(())
    }

    /// The copy decision for the trunk's first write to `page` in a transaction: if a live child
    /// can still see the version about to be overwritten, keep a copy of it for that child. The
    /// record waits in the journal until the trunk's commit barrier makes it durable.
    pub(crate) fn first_write_trunk(&self, page: u32, pre_image: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock();
        let StoreInner {
            arena,
            trunk,
            journal,
            ..
        } = &mut *inner;
        let epoch = trunk.lineage.epoch;
        let born = trunk.written.get(&page).copied().unwrap_or(0);
        if born >= epoch {
            return Ok(());
        }
        if trunk.lineage.has_child_in(born, epoch) {
            if journal.as_ref().is_some_and(|j| j.is_poisoned()) {
                return Err(LimboError::InternalError(
                    "the trunk would overwrite a page a durable branch reads, but the branch \
                     store is fail-stopped; reopen the database"
                        .to_string(),
                ));
            }
            let arena = arena.as_mut().expect("the trunk has a child, so the arena exists");
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
        let mut inner = self.inner.lock();
        // Re-read under the lock: `first_write_trunk` sets it while holding it.
        let unsynced = self.unsynced.load(Ordering::Acquire);
        let StoreInner {
            journal,
            arena,
            failpoint,
            orphans,
            lease,
            leases,
            ..
        } = &mut *inner;
        let (Some(journal), Some(arena)) = (journal.as_mut(), arena.as_mut()) else {
            return Ok(());
        };
        if !unsynced {
            // Only stamps are at stake: the buffer holds nothing else without `unsynced`. A trunk
            // commit that needs no pre-image does not fail because its stamp could not be written;
            // a lost stamp lengthens leases and loses no data.
            if journal.is_poisoned() || leases.is_empty() {
                return Ok(());
            }
            let now = lease.now_ms();
            if now >= lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                journal.buffer(&Record::Clock { now_ms: now })?;
                lease.queued(now);
            }
            if lease.queued_ms > lease.durable_ms {
                let flushed = if *failpoint == Some(BranchFailpoint::StampFlushFails) {
                    *failpoint = None;
                    journal.poison();
                    Err(LimboError::InternalError(
                        "failpoint: the stamp-only branch log flush failed".to_string(),
                    ))
                } else {
                    journal.flush(arena)
                };
                match flushed {
                    Ok(()) => lease.flushed(),
                    Err(e) => {
                        tracing::warn!("branch lease clock not stamped at a trunk commit: {e}")
                    }
                }
            }
            return Ok(());
        }
        if *failpoint == Some(BranchFailpoint::BarrierBeforeRecords) {
            *failpoint = None;
            *orphans = journal.pending_slots.clone();
            journal.poison();
            return Err(LimboError::InternalError(
                "failpoint: the trunk commit's branch barrier stopped before its records"
                    .to_string(),
            ));
        }
        if !leases.is_empty() {
            let now = lease.now_ms();
            if now >= lease.queued_ms.saturating_add(STAMP_EVERY_MS) {
                journal.buffer(&Record::Clock { now_ms: now })?;
                lease.queued(now);
            }
        }
        journal.flush(arena)?;
        lease.flushed();
        self.unsynced.store(false, Ordering::Release);
        self.maybe_compact(&mut inner);
        Ok(())
    }

    /// Commit a branch's dirty pages: write each into the slot its copy decision reserved, make
    /// that durable with the `Commit` record, and only then move the branch's map.
    pub(crate) fn commit_pages(&self, id: BranchId, pages: &[PageRef]) -> Result<()> {
        let mut inner = self.inner.lock();
        // Refuse BEFORE any slot is written: after the journal failed, this commit's record can
        // never be durable, so its pages have no business in the arena.
        if inner.poisoned() {
            return Err(fail_stopped(id, "no commit"));
        }
        let entries = {
            let StoreInner {
                arena,
                branches,
                journal,
                failpoint,
                orphans,
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
                arena.sync()?;
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
        if !inner.leases.is_empty() {
            let now = inner.lease.now_ms();
            if now > inner.lease.queued_ms {
                records.push(Record::Clock { now_ms: now });
                inner.lease.queued(now);
            }
        }
        self.log_all(&mut inner, records)?;
        let mut freed = Vec::new();
        inner.apply_commit(id, &entries, &mut freed)?;
        inner.release_slots(freed);
        self.maybe_compact(&mut inner);
        Ok(())
    }

    /// Fill `out` with `page` as branch `id` sees it, if that version lives in the arena. `false`
    /// means the branch sees the trunk's current version, which the caller reads through the
    /// ordinary WAL / database-file path.
    pub(crate) fn resolve_into(&self, id: BranchId, page: u32, out: &mut [u8]) -> Result<bool> {
        let inner = self.inner.lock();
        let Some((slot, crc)) = inner.resolve(id, page)? else {
            return Ok(false);
        };
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

    pub(crate) fn stats(&self) -> BranchStats {
        let inner = self.inner.lock();
        BranchStats {
            live_branches: inner.branches.len(),
            arena_slots_in_use: inner.arena.as_ref().map_or(0, |a| a.in_use()),
            arena_slots_free: inner.arena.as_ref().map_or(0, |a| a.free_count()),
        }
    }

    pub(crate) fn owned_slots(&self, id: BranchId) -> Vec<u32> {
        let inner = self.inner.lock();
        let Some(st) = inner.branches.get(&id) else {
            return Vec::new();
        };
        let mut slots: Vec<u32> = st.current.values().map(|o| o.slot).collect();
        for versions in st.lineage.retained.values() {
            slots.extend(versions.iter().map(|v| v.slot));
        }
        slots
    }

    pub(crate) fn slots_in_use(&self) -> Vec<u32> {
        self.inner
            .lock()
            .arena
            .as_ref()
            .map_or_else(Vec::new, |a| a.slots_in_use())
    }

    pub(crate) fn slot_is_free(&self, slot: u32) -> bool {
        self.inner
            .lock()
            .arena
            .as_ref()
            .is_some_and(|a| a.is_free(slot))
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
        let mut inner = self.inner.lock();
        let fail = inner.failpoint == Some(BranchFailpoint::CompactAfterRenameBeforeLogReset);
        if fail {
            inner.failpoint = None;
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
}

impl Drop for BranchStore {
    /// A clean close stamps the lease clock, so the time spent open is not lost with the process.
    /// (A crash loses the time since the last stamp, which extends leases and never shortens one.)
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        let now = inner.lease.now_ms();
        // With no lease outstanding the clock's value constrains nothing, so a close writes nothing.
        // Compared with what is DURABLE: a stamp only queued dies here with the journal (N3).
        if !inner.leases.is_empty() && now > inner.lease.durable_ms {
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

fn reaped(id: BranchId) -> LimboError {
    LimboError::InvalidArgument(format!("branch {} has been reaped", id.0))
}

impl StoreInner {
    fn fresh(files: Option<BranchFiles>, sync: bool, default_lease: Option<Duration>) -> Self {
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
    }

    fn poisoned(&self) -> bool {
        self.journal.as_ref().is_some_and(|j| j.is_poisoned())
    }

    /// Ancestors between `id` and the trunk.
    fn depth(&self, mut id: BranchId) -> usize {
        let mut depth = 0;
        while let Some(st) = self.branches.get(&id) {
            depth += 1;
            id = st.parent;
        }
        depth
    }

    /// Create the arena (and, for a durable store, its files) at the first fork, when the page size
    /// is known.
    fn ensure_backing(&mut self, page_size: usize) -> Result<()> {
        if let Some(arena) = &self.arena {
            if arena.page_size() != page_size {
                return Err(LimboError::InternalError(format!(
                    "branch arena holds {}-byte pages but the database now uses {page_size}",
                    arena.page_size()
                )));
            }
            return Ok(());
        }
        match &self.files {
            None => self.arena = Some(Arena::new(page_size)),
            Some(files) => {
                // Files that exist here held no recoverable state when this store opened
                // (`Journal::recover` said so): start them over. The journal first: it takes the
                // log's lock and refuses files another store has written since (review N1), and
                // the arena must not be truncated before that refusal. It is kept even if the
                // arena then fails to open, so a retry does not take its own fresh log header for
                // another store's state.
                if self.journal.is_none() {
                    let fail = self.failpoint == Some(BranchFailpoint::CreateFailsAfterHeader);
                    if fail {
                        self.failpoint = None;
                    }
                    self.journal = Some(Journal::create_with(files, page_size, self.sync, fail)?);
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
    ) -> Result<()> {
        if self.branches.contains_key(&child) || child.is_trunk() {
            return Err(LimboError::Corrupt(format!(
                "branch {} forked twice",
                child.0
            )));
        }
        let lineage = if parent.is_trunk() {
            &mut self.trunk.lineage
        } else {
            &mut self
                .branches
                .get_mut(&parent)
                .ok_or_else(|| gone(parent))?
                .lineage
        };
        let f = lineage.epoch;
        lineage.epoch += 1;
        lineage.children.insert(f, child);
        self.next_id = self.next_id.max(child.0 + 1);
        self.branches.insert(
            child,
            BranchState {
                parent,
                fork_epoch: f,
                lineage: Lineage::default(),
                current: HashMap::new(),
                pending: HashMap::new(),
                schema,
                handle,
                open: false,
                writer: false,
                lease: None,
            },
        );
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
        let st = self.branches.get_mut(&id).ok_or_else(|| gone(id))?;
        let epoch = st.lineage.epoch;
        for &(page, slot, crc) in pages {
            let new = Owned {
                slot,
                born: epoch,
                crc,
            };
            if let Some(old) = st.current.insert(page, new) {
                if st.lineage.has_child_in(old.born, epoch) {
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
        Ok(())
    }

    fn apply_trunk_retain(&mut self, page: u32, v: Retained) {
        self.trunk.lineage.retain(page, v);
        let written = self.trunk.written.entry(page).or_insert(0);
        *written = (*written).max(v.died);
    }

    fn apply_release(&mut self, id: BranchId, freed: &mut Vec<Slot>) {
        if let Some(st) = self.branches.get_mut(&id) {
            st.handle = Handle::Released;
            if let Some(deadline) = st.lease.take() {
                self.leases.remove(&(deadline, id));
            }
        }
        self.collect(id, freed);
    }

    /// Free `id` if nothing can reach it any more, then its parent if that freed the parent's last
    /// reason to exist; a released `id` that still has live children is retired instead (see
    /// `BranchState::retire_current`). Every freed slot goes to `freed`.
    fn collect(&mut self, mut id: BranchId, freed: &mut Vec<Slot>) {
        loop {
            let Some(st) = self.branches.get_mut(&id) else {
                return;
            };
            // Exactly `Released`: a `ReleasePending` branch's release is not durable and nothing of
            // it is ever freed here (review R1).
            if st.handle != Handle::Released || st.open {
                return;
            }
            if !st.lineage.children.is_empty() {
                // F4: a released interior keeps only what a live child can still read.
                st.retire_current(freed);
                return;
            }
            let st = self.branches.remove(&id).expect("just looked it up");
            freed.extend(st.current.values().map(|o| o.slot));
            freed.extend(st.pending.values().copied());
            st.lineage.release_all(freed);
            if st.parent.is_trunk() {
                self.trunk.lineage.child_gone(st.fork_epoch, freed);
                return;
            }
            let parent = self
                .branches
                .get_mut(&st.parent)
                .expect("a live branch's parent is kept while the branch lives");
            parent.lineage.child_gone(st.fork_epoch, freed);
            id = st.parent;
        }
    }

    /// Collect every released branch that nothing reads through any more.
    fn collect_released(&mut self, freed: &mut Vec<Slot>) {
        let released: Vec<BranchId> = self
            .branches
            .iter()
            .filter(|(_, st)| st.handle == Handle::Released)
            .map(|(&id, _)| id)
            .collect();
        for id in released {
            self.collect(id, freed);
        }
    }

    fn resolve(&self, id: BranchId, page: u32) -> Result<Option<(Slot, u32)>> {
        let mut node = id;
        // A branch sees all of its own versions; its ancestors only as of the fork.
        let mut at = u64::MAX;
        loop {
            if node.is_trunk() {
                if let Some(found) = self.trunk.lineage.retained_at(page, at) {
                    return Ok(Some(found));
                }
                let born = self.trunk.written.get(&page).copied().unwrap_or(0);
                if born > at {
                    // The trunk overwrote this page after the fork and nothing was retained: the
                    // ordinary read path would return the NEW version. Refuse rather than serve it.
                    return Err(LimboError::Corrupt(format!(
                        "branch {} would read trunk page {page} written after its fork; the \
                         pre-image was not retained",
                        id.0
                    )));
                }
                return Ok(None);
            }
            let st = self.branches.get(&node).ok_or_else(|| gone(node))?;
            if let Some(owned) = st.current.get(&page) {
                if owned.born <= at {
                    return Ok(Some((owned.slot, owned.crc)));
                }
            }
            if let Some(found) = st.lineage.retained_at(page, at) {
                return Ok(Some(found));
            }
            at = st.fork_epoch;
            node = st.parent;
        }
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
                self.apply_commit(BranchId(*branch), pages, freed)
                    .map_err(corrupt)
            }
            Record::TrunkRetain {
                page,
                born,
                died,
                slot,
                crc,
            } => {
                self.apply_trunk_retain(
                    *page,
                    Retained {
                        born: *born,
                        died: *died,
                        slot: *slot,
                        crc: *crc,
                    },
                );
                Ok(())
            }
            Record::Release { branch } => {
                let id = BranchId(*branch);
                if !self.branches.contains_key(&id) {
                    return Err(corrupt(gone(id)));
                }
                self.apply_release(id, freed);
                Ok(())
            }
            Record::Lease {
                branch,
                deadline_ms,
                now_ms,
            } => {
                let id = BranchId(*branch);
                if !self.branches.contains_key(&id) {
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
        }
    }

    /// Every slot the state names: what the arena must NOT treat as free after a reopen.
    fn referenced_slots(&self) -> Vec<Slot> {
        let mut slots: Vec<Slot> = self
            .trunk
            .lineage
            .retained
            .values()
            .flat_map(|vs| vs.iter().map(|v| v.slot))
            .collect();
        for st in self.branches.values() {
            slots.extend(st.current.values().map(|o| o.slot));
            slots.extend(
                st.lineage
                    .retained
                    .values()
                    .flat_map(|vs| vs.iter().map(|v| v.slot)),
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
            );
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
                    pending: HashMap::new(),
                    schema: None,
                    handle: if b.released {
                        Handle::Released
                    } else {
                        Handle::Detached
                    },
                    open: false,
                    writer: false,
                    lease,
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
            if lineage.children.insert(f, child).is_some() {
                return Err(LimboError::Corrupt(format!(
                    "branch snapshot: two children at fork epoch {f} of branch {}",
                    parent.0
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
