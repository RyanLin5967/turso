//! fastest-budgets lane (artie-research frontier/fastest, DECISIONS.md 2026-10-04T03:55Z): the
//! test-only counters the performance-budget suite (`budget_tests`) reads, besides the engine's own
//! (`io::SYNC_COUNTS`, the catalog's query counter). TEST BUILDS ONLY; observing only.
//!
//! * **Allocations**: a counting global allocator over `System` — every `alloc`, `alloc_zeroed` and
//!   `realloc` call and its requested bytes, per thread and process-wide, and separately those made
//!   while the calling thread holds a branch store's mutex; and every `dealloc` (reported only).
//! * **Catalog statements**, the writes among them and the rows they touch, per thread and
//!   process-wide, on every catalog connection: three `cfg(test)` hook lines in `catalog::Stmt`
//!   (`catalog_statement`).
//! * **Live heap bytes** (requested sizes of every live allocation), and the largest allocation
//!   and catalog-row count inside one store-mutex hold (`take_hold_maxima`).
//! * **Store-mutex acquisitions**: `StoreMutex::lock` calls [`store_locked`] and its guard's drop
//!   [`store_unlocked`] (two `cfg(test)` hook lines in `store.rs`), per thread and process-wide.
//! * **SQL-layer counts** (engine 2b's schema re-read), per thread: statements prepared, pages read
//!   through the pager, schema rows parsed, and a WAL write lock with what was done while it was
//!   held: six `cfg(test)` hook lines in `connection.rs`, `statement.rs`, `schema.rs`, `pager.rs` and
//!   `wal.rs` ([`sql_counts`]).
//! * **Unix syscalls** (Apple only): the kernel's own count for this task (`task_info`
//!   `TASK_EVENTS_INFO`, `syscalls_unix`: incremented at every BSD syscall entry by any thread of
//!   the process, so nothing the engine does can bypass it). Reading it is a Mach trap, which that
//!   count does not include. While [`arm`]ed, the count is also sampled when a thread first takes a
//!   store mutex and when it lets go of the last one, so the syscalls issued while one is held are
//!   summed. BLIND SPOT: the count is PROCESS-WIDE, so a delta is one operation's only while no
//!   other thread of the process runs; `budget_tests` measures in a child process that runs nothing
//!   else, and checks the thread count around every sample.
//! * **Instructions retired** (Apple only): `proc_pid_rusage` `ri_instructions`, process-wide.
//!   Reported, not asserted equal: unlike the counts above it varies with interrupts and faults.
//!
//! Elsewhere than Apple, the two kernel counts are `None`, and the tests that read them do not
//! exist there; the thread count is read from `/proc/self/stat` on Linux.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::{Acquire, Relaxed, Release}};

/// The counting allocator (test builds of this crate only).
pub(crate) struct Counting;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static FREES: AtomicU64 = AtomicU64::new(0);
static CAT_STMTS: AtomicU64 = AtomicU64::new(0);
static CAT_WRITES: AtomicU64 = AtomicU64::new(0);
static CAT_ROWS: AtomicU64 = AtomicU64::new(0);
/// Bytes requested by every live allocation: + at alloc, - at dealloc, the difference at realloc.
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
/// The most allocated, and the most catalog rows touched, inside ONE store-mutex hold (outermost
/// lock to unlock, any thread) since `take_hold_maxima`.
static MAX_HOLD_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static MAX_HOLD_CAT_ROWS: AtomicU64 = AtomicU64::new(0);
/// The same, over holds by threads not marked foreground (`mark_foreground`): a store's background
/// work (the name filter's build), whose O(N) hold a larger fixed hold of the foreground thread
/// would otherwise mask in the all-threads maximum.
static MAX_BG_HOLD_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
/// Linux finding (b): the most units of held WORK (`note_held_work`) in one store-mutex hold, by any
/// thread, by background threads and by the foreground one. Work that allocates nothing and reads no
/// catalog row is invisible to the other hold maxima on every platform (a filter table pre-sized off
/// the mutex and filled under it: mutant name_filter_built_under_mutex survived on both glibc kinds).
static MAX_HOLD_WORK: AtomicU64 = AtomicU64::new(0);
static MAX_BG_HOLD_WORK: AtomicU64 = AtomicU64::new(0);
static MAX_FG_HOLD_WORK: AtomicU64 = AtomicU64::new(0);
/// budgets-rev-242345 LOW-4 and LOW-5: the most work at each site that does per-name work (`WorkSite`),
/// kept apart, so an O(N) term at one site smaller than the largest hold elsewhere is not masked by the
/// per-hold maximum.
static MAX_SITE_WORK: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
/// budgets-rev-242345 MED-7: how many times each site ran, so a site that never ran reads as a refused
/// premise, never as 0 units of work.
static SITE_RUNS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static MAX_BG_HOLD_CAT_ROWS: AtomicU64 = AtomicU64::new(0);
// The same over holds by the thread marked foreground (review 2 M7: the opener's own holds).
static MAX_FG_HOLD_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static MAX_FG_HOLD_CAT_ROWS: AtomicU64 = AtomicU64::new(0);
static LOCKS: AtomicU64 = AtomicU64::new(0);
static HELD_SYSCALLS: AtomicU64 = AtomicU64::new(0);
/// Holds whose syscalls could not be attributed to the holding thread: another thread ran during
/// the hold (`hold_attributed`); their syscalls are in neither `HELD_SYSCALLS` count.
static HELD_UNATTRIBUTED: AtomicU64 = AtomicU64::new(0);

/// One store-mutex hold, as the lifecycle audit records it (class (e), DECISIONS 994f7d41e0): where
/// it was taken (`#[track_caller]` through `StoreMutex::lock` and its wrappers), on which thread,
/// and what it did: its syscalls (attributed to the holder, `attribute`; `None` when they could not
/// be), allocations and bytes, and catalog statements, each this thread's own.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HoldRec {
    pub(crate) site: &'static std::panic::Location<'static>,
    pub(crate) thread: u64,
    pub(crate) syscalls: Option<u64>,
    pub(crate) allocs: u64,
    pub(crate) alloc_bytes: u64,
    pub(crate) cat_stmts: u64,
    /// The store whose mutex this was (`store::scope_store_id`): the audit's premise that every
    /// hold it recorded is its own store's.
    pub(crate) store: usize,
}

/// The audit's log, recorded while `AUDIT` is set; the test reserves its room first, so recording
/// allocates nothing (a push past the room is counted in `AUDIT_LOST` instead).
pub(crate) static AUDIT: AtomicBool = AtomicBool::new(false);
/// The per-store counter class (lead ruling d9cf2b609b): the audit records only the holds of the
/// store opened at this database path (`store::scope_is_audited`), never another test's store's.
pub(crate) static AUDIT_PATH: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
/// Counters review E1 and review 2 MED 3: every outermost hold of an audited store, counted by its
/// guard (`StoreGuard::drop`), outside the probe and before any of the probe's gates. The audit's
/// premise is `seen == recorded + lost + excluded`, with excluded 0 on Apple (review 3 MED-C).
pub(crate) static AUDIT_SEEN: AtomicU64 = AtomicU64::new(0);
/// Counters review 2 MED 3: outermost holds of an audited store the probe saw and did not record by
/// its own gates (not armed at its lock, or its end count unreadable): the premise is
/// `seen == recorded + lost + excluded`, with seen counted by the guard, outside the probe.
pub(crate) static AUDIT_EXCLUDED: AtomicU64 = AtomicU64::new(0);
pub(crate) static AUDIT_LOG: std::sync::Mutex<Vec<HoldRec>> = std::sync::Mutex::new(Vec::new());
pub(crate) static AUDIT_LOST: AtomicU64 = AtomicU64::new(0);
/// Holds `attribute` refused (any thread), observation only.
pub(crate) static HOLD_REFUSALS: AtomicU64 = AtomicU64::new(0);
/// Threads that declared themselves syscall-free while they spin (`spinning`): their CPU time does
/// not make a window or a hold foreign. At most 8 at once; 0 is a free slot.
static SPINNERS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static ARMED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static T_ALLOCS: Cell<u64> = const { Cell::new(0) };
    static T_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
    static T_FREES: Cell<u64> = const { Cell::new(0) };
    static T_CAT_STMTS: Cell<u64> = const { Cell::new(0) };
    static T_CAT_WRITES: Cell<u64> = const { Cell::new(0) };
    static T_CAT_ROWS: Cell<u64> = const { Cell::new(0) };
    /// `(held allocation bytes, catalog rows)` of this thread when it took its outermost store mutex.
    static HOLD_START: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// This thread's units of held work (`note_held_work`), and their count at its outermost lock.
    static T_HELD_WORK: Cell<u64> = const { Cell::new(0) };
    static HOLD_START_WORK: Cell<u64> = const { Cell::new(0) };
    /// The outermost hold's site, and this thread's (held allocations, catalog statements) at its
    /// start (the lifecycle audit).
    static HOLD_SITE: Cell<Option<&'static std::panic::Location<'static>>> = const { Cell::new(None) };
    static HOLD_START2: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// Whether this thread is the measuring (foreground) thread (`mark_foreground`).
    static FOREGROUND: Cell<bool> = const { Cell::new(false) };
    static T_FREE_BYTES: Cell<u64> = const { Cell::new(0) };
    static T_HELD_ALLOCS: Cell<u64> = const { Cell::new(0) };
    static T_HELD_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
    static T_LOCKS: Cell<u64> = const { Cell::new(0) };
    static T_HELD_SYSCALLS: Cell<u64> = const { Cell::new(0) };
    static T_HELD_UNATTRIBUTED: Cell<u64> = const { Cell::new(0) };
    /// Every thread's CPU time at this thread's window start (`begin`) and outermost hold start
    /// (`store_locked`), for the attribution by exclusion.
    static WINDOW_T0: std::cell::RefCell<[ThreadCpu; 48]> = const { std::cell::RefCell::new([ThreadCpu::EMPTY; 48]) };
    static HOLD_T0: std::cell::RefCell<[ThreadCpu; 48]> = const { std::cell::RefCell::new([ThreadCpu::EMPTY; 48]) };
    /// The per-thread shim counts (`feshim`) of the threads in `WINDOW_T0` / `HOLD_T0`, and the
    /// kernel's count read with them.
    static WINDOW_C0: Cell<[u64; 48]> = const { Cell::new([0; 48]) };
    static WINDOW_S0: Cell<Option<u64>> = const { Cell::new(None) };
    static HOLD_C0: Cell<[u64; 48]> = const { Cell::new([0; 48]) };
    /// How many threads `thread_table` reported at the window's and the hold's start, and the
    /// shim's lost count then (probe-adversary items 1 and 4).
    static WINDOW_N0: Cell<Option<usize>> = const { Cell::new(None) };
    static HOLD_N0: Cell<Option<usize>> = const { Cell::new(None) };
    static WINDOW_LOST0: Cell<u64> = const { Cell::new(0) };
    static HOLD_LOST0: Cell<u64> = const { Cell::new(0) };
    /// Store-mutex guards this thread holds now.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// The task's syscall count when this thread took its first store mutex (while armed).
    static HELD_SINCE: Cell<Option<u64>> = const { Cell::new(None) };
}

fn bump(cell: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    let _ = cell.try_with(|c| c.set(c.get().wrapping_add(by)));
}

thread_local! {
    /// The allocation recorder (`record_allocs`): on, this thread's allocation sizes in order (the
    /// first 16), and from the `T_TRACE_FROM`-th on a backtrace of each (taken with the recorder off,
    /// so its own allocations are not recorded). Diagnostics only.
    static T_REC_ON: Cell<bool> = const { Cell::new(false) };
    static T_REC: Cell<[usize; 16]> = const { Cell::new([0; 16]) };
    static T_REC_N: Cell<usize> = const { Cell::new(0) };
    static T_TRACE_FROM: Cell<usize> = const { Cell::new(usize::MAX) };
    static T_TRACES: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    /// With `record_big`: only allocations and frees of at least this many bytes are recorded, each
    /// with a backtrace.
    static T_REC_MIN: Cell<usize> = const { Cell::new(0) };
}

/// Record this thread's allocations AND frees of at least `min` bytes, each with a backtrace
/// (`recorded_allocs` reads them; `record_allocs(false, ..)` stops). Diagnostics only.
pub(crate) fn record_big(min: usize) {
    record_allocs(true, 0);
    T_REC_MIN.with(|m| m.set(min));
}

fn record_free(bytes: usize) {
    if !T_REC_ON.try_with(|r| r.get()).unwrap_or(false) || T_REC_MIN.with(|m| m.get()) == 0 {
        return;
    }
    if bytes < T_REC_MIN.with(|m| m.get()) {
        return;
    }
    T_REC_ON.with(|r| r.set(false));
    let trace = format!("free of {bytes} bytes:\n{}", std::backtrace::Backtrace::force_capture()); // lint-allow: the recorder (record_allocs, record_big), on only when a test asks, itself off here
    T_TRACES.with(|t| t.borrow_mut().push(trace)); // lint-allow: the recorder (record_allocs, record_big), on only when a test asks, itself off here
    T_REC_ON.with(|r| r.set(true));
}

/// Start (or stop) recording this thread's allocations, cleared: sizes, and a backtrace of each
/// from the `trace_from`-th (0-based) on. Diagnostics only: a backtrace costs milliseconds.
pub(crate) fn record_allocs(on: bool, trace_from: usize) {
    T_REC_MIN.with(|m| m.set(0));
    T_REC_N.with(|n| n.set(0));
    T_TRACES.with(|t| t.borrow_mut().clear());
    T_TRACE_FROM.with(|t| t.set(trace_from));
    T_REC_ON.with(|r| r.set(on));
}

/// What `record_allocs` recorded on this thread: the count, the first 16 sizes, the backtraces.
/// Stops the recorder first: the copies made here allocate.
pub(crate) fn recorded_allocs() -> (usize, Vec<usize>, Vec<String>) {
    T_REC_ON.with(|r| r.set(false));
    let n = T_REC_N.with(|n| n.get());
    let sizes = T_REC.with(|r| r.get())[..n.min(16)].to_vec();
    (n, sizes, T_TRACES.with(|t| t.borrow().clone()))
}

fn record_alloc(bytes: usize) {
    if !T_REC_ON.try_with(|r| r.get()).unwrap_or(false) {
        return;
    }
    if bytes < T_REC_MIN.with(|m| m.get()) {
        return;
    }
    let n = T_REC_N.with(|n| n.get());
    T_REC_N.with(|c| c.set(n + 1));
    if n < 16 {
        T_REC.with(|r| {
            let mut a = r.get();
            a[n] = bytes;
            r.set(a);
        });
    }
    if n >= T_TRACE_FROM.with(|t| t.get()) {
        T_REC_ON.with(|r| r.set(false));
        let trace = format!("alloc #{n} of {bytes} bytes:\n{}", std::backtrace::Backtrace::force_capture()); // lint-allow: the recorder (record_allocs, record_big), on only when a test asks, itself off here
        T_TRACES.with(|t| t.borrow_mut().push(trace)); // lint-allow: the recorder (record_allocs, record_big), on only when a test asks, itself off here
        T_REC_ON.with(|r| r.set(true));
    }
}

fn note_alloc(bytes: usize) {
    record_alloc(bytes);
    ALLOCS.fetch_add(1, Relaxed);
    ALLOC_BYTES.fetch_add(bytes as u64, Relaxed);
    bump(&T_ALLOCS, 1);
    bump(&T_ALLOC_BYTES, bytes as u64);
    if DEPTH.try_with(|d| d.get()).unwrap_or(0) > 0 {
        bump(&T_HELD_ALLOCS, 1);
        bump(&T_HELD_ALLOC_BYTES, bytes as u64);
    }
    if wal_lock_held() {
        bump(&T_WAL_ALLOCS, 1);
        bump(&T_WAL_ALLOC_BYTES, bytes as u64);
    }
}

// The SQL layer's counters (engine 2b: a trunk fork's schema re-read), per thread only: statements
// compiled (`Connection::compile_cmd`, which prepare, execute, query and batches all reach, and
// `Statement::reprepare`; review 2 M4), pages read through the pager (`Pager::read_page`
// calls that find no read of that page pending: a cache hit, or a miss's first call; a call
// re-entered on a page still loading counts again), schema rows parsed (`Schema::handle_schema_row`,
// which a reparse and the ParseSchema opcode both reach; review 2 M4),
// and a WAL write lock (from `Pager::begin_write_tx` once `Wal::begin_write_tx` succeeded, to
// `WalFile::end_write_tx`, which every release path calls: the commit's, a rollback's, a close's),
// with what this thread did while it held one; and the statements compiled while this thread held a
// store mutex (`held_prepares`, review 2 L6). Six `cfg(test)` hook lines in the engine's files.
// BLIND SPOTS: a page read without the pager's `read_page` (`read_page_no_cache`) is not counted;
// the lock is ANY database's WAL write lock, the trunk's or the branch catalog's own (a separate
// Turso database); a thread holding two at once reads as holding one until either is released.
thread_local! {
    static T_PREPARES: Cell<u64> = const { Cell::new(0) };
    static T_PAGE_READS: Cell<u64> = const { Cell::new(0) };
    static T_SCHEMA_ROWS: Cell<u64> = const { Cell::new(0) };
    /// Statements compiled while this thread held a store mutex (review 2 L6: a first create's
    /// catalog creation prepares 42 statements and runs 13 DDL commits under it).
    static T_HELD_PREPARES: Cell<u64> = const { Cell::new(0) };
    static WAL_HELD: Cell<bool> = const { Cell::new(false) };
    static T_WAL_LOCKS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_PREPARES: Cell<u64> = const { Cell::new(0) };
    static T_WAL_PAGE_READS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_SCHEMA_ROWS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_ALLOCS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

/// Whether this thread holds a WAL write lock now, any database's (review 2 L12: not only the
/// trunk's; the branch catalog is a database of its own).
pub(crate) fn wal_lock_held() -> bool {
    WAL_HELD.try_with(|h| h.get()).unwrap_or(false)
}

/// A statement was prepared on this thread.
pub(crate) fn statement_prepared() {
    bump(&T_PREPARES, 1);
    if DEPTH.try_with(|d| d.get()).unwrap_or(0) > 0 {
        bump(&T_HELD_PREPARES, 1);
    }
    if wal_lock_held() {
        bump(&T_WAL_PREPARES, 1);
    }
}

/// A page was read through the pager on this thread.
pub(crate) fn page_read() {
    bump(&T_PAGE_READS, 1);
    if wal_lock_held() {
        bump(&T_WAL_PAGE_READS, 1);
    }
}

/// A `sqlite_schema` row was parsed into a schema on this thread.
pub(crate) fn schema_row_parsed() {
    bump(&T_SCHEMA_ROWS, 1);
    if wal_lock_held() {
        bump(&T_WAL_SCHEMA_ROWS, 1);
    }
}

/// This thread took a WAL write lock (`Pager::begin_write_tx`, once the WAL's own succeeded).
pub(crate) fn wal_write_locked() {
    let _ = WAL_HELD.try_with(|h| h.set(true));
    bump(&T_WAL_LOCKS, 1);
}

/// This thread let a WAL write lock go (`WalFile::end_write_tx`).
pub(crate) fn wal_write_unlocked() {
    let _ = WAL_HELD.try_with(|h| h.set(false));
}

/// The SQL-layer counters of the calling thread at one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SqlCounts {
    pub(crate) prepares: u64,
    pub(crate) page_reads: u64,
    pub(crate) schema_rows: u64,
    pub(crate) wal_locks: u64,
    pub(crate) wal_prepares: u64,
    pub(crate) wal_page_reads: u64,
    pub(crate) wal_schema_rows: u64,
    pub(crate) wal_allocs: u64,
    pub(crate) wal_alloc_bytes: u64,
    pub(crate) held_prepares: u64,
}

pub(crate) fn sql_counts() -> SqlCounts {
    let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
    SqlCounts {
        prepares: get(&T_PREPARES),
        page_reads: get(&T_PAGE_READS),
        schema_rows: get(&T_SCHEMA_ROWS),
        wal_locks: get(&T_WAL_LOCKS),
        wal_prepares: get(&T_WAL_PREPARES),
        wal_page_reads: get(&T_WAL_PAGE_READS),
        wal_schema_rows: get(&T_WAL_SCHEMA_ROWS),
        wal_allocs: get(&T_WAL_ALLOCS),
        wal_alloc_bytes: get(&T_WAL_ALLOC_BYTES),
        held_prepares: get(&T_HELD_PREPARES),
    }
}

thread_local! {
    static T_WINDOWS_MET: Cell<u64> = const { Cell::new(0) };
    static T_FUTILE_LEADS: Cell<u64> = const { Cell::new(0) };
    static T_SLEEPS: Cell<u64> = const { Cell::new(0) };
    static T_REGISTRATIONS: Cell<u64> = const { Cell::new(0) };
}

/// A trunk fork registration attempt on this thread (`Connection::fork_trunk_registered`; review 2
/// M5: an internal retry shows as more attempts than acknowledged creates).
pub(crate) fn fork_registration() {
    bump(&T_REGISTRATIONS, 1);
}

/// This thread's count of `fork_registration`.
pub(crate) fn fork_registrations() -> u64 {
    T_REGISTRATIONS.with(|c| c.get())
}

/// This thread slept through `crate::thread::sleep` (test builds' wrapper, review 2 M3).
pub(crate) fn slept() {
    bump(&T_SLEEPS, 1);
}

/// This thread's count of `slept`.
pub(crate) fn sleeps() -> u64 {
    T_SLEEPS.with(|c| c.get())
}

/// A flight waiter on this thread took the store mutex and the group lock to lead, and led no flight:
/// it was covered meanwhile, or another flight was already in the air (review 1 #9's herd; review 2
/// H5). Two `cfg(test)` hook lines in `BranchStore::wait_durable_on`.
pub(crate) fn futile_lead() {
    bump(&T_FUTILE_LEADS, 1);
}

/// This thread's count of `futile_lead`.
pub(crate) fn futile_leads() -> u64 {
    T_FUTILE_LEADS.with(|c| c.get())
}

/// A trunk fork on this thread found no schema in hand at its snapshot's cookie (engine 2b's DDL
/// window; a `cfg(test)` hook line in `Connection::fork_trunk_registered`, review 2 M2's premise).
pub(crate) fn schema_window_met() {
    bump(&T_WINDOWS_MET, 1);
}

/// This thread's count of `schema_window_met`.
pub(crate) fn schema_windows_met() -> u64 {
    T_WINDOWS_MET.with(|c| c.get())
}

// SAFETY: every call is forwarded unchanged to `System`; the counting touches only atomics and
// const-initialised thread-locals without destructors, neither of which allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_alloc(layout.size());
        // SAFETY: the caller's contract, forwarded.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_alloc(layout.size());
        // SAFETY: the caller's contract, forwarded.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Relaxed);
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_alloc(new_size);
        // SAFETY: the caller's contract, forwarded.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        // A failed realloc leaves the old block allocated, unchanged.
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREES.fetch_add(1, Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as i64, Relaxed);
        bump(&T_FREES, 1);
        bump(&T_FREE_BYTES, layout.size() as u64);
        record_free(layout.size());
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// A branch store's mutex was just granted to this thread (`StoreMutex::lock`).
pub(crate) fn store_locked() {
    store_locked_at(std::panic::Location::caller());
}

/// `store_locked`, with the site that took the mutex (the lifecycle audit's record).
pub(crate) fn store_locked_at(site: &'static std::panic::Location<'static>) {
    LOCKS.fetch_add(1, Relaxed);
    bump(&T_LOCKS, 1);
    let depth = DEPTH.with(|d| {
        d.set(d.get() + 1);
        d.get()
    });
    if depth == 1 {
        let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
        HOLD_START.with(|h| h.set((get(&T_HELD_ALLOC_BYTES), get(&T_CAT_ROWS))));
        HOLD_START_WORK.with(|h| h.set(get(&T_HELD_WORK)));
        HOLD_SITE.with(|h| h.set(Some(site)));
        HOLD_START2.with(|h| h.set((get(&T_HELD_ALLOCS), get(&T_CAT_STMTS))));
        // Counters review 4 LOW-F: Acquire, paired with `arm`'s Release. The audit disarms as
        // `AUDIT = false` then `arm(false)`, so a lock that reads the new `ARMED` (false) happens after
        // that `AUDIT` store, and its guard's end can no longer read the old `AUDIT` (true) and count
        // an unsampled hold as excluded.
        if ARMED.load(Acquire) {
            // The table first, then the count: the hold's counted syscalls lie inside the table's
            // window (`hold_attributed`).
            // The table, then the kernel's count, then the shim's: a call between the kernel's read
            // and the shim's is in the shim's start and in the kernel's span, so it can only make
            // the shim's span smaller than the kernel's (a refusal), never inflate it (probe-
            // adversary row 6: the shim read before the kernel let a foreign call in that gap
            // compensate an unseen call of the hold's own).
            HOLD_T0.with(|t| {
                if let Ok(mut t) = t.try_borrow_mut() {
                    *t = [ThreadCpu::EMPTY; 48];
                    HOLD_N0.with(|n| n.set(thread_table(&mut t[..])));
                }
            });
            // Mutant `audit_unarm_half` (test builds only; counters review 3 MED-C): every second
            // outermost lock is left unarmed, so its hold is excluded at its end.
            static UNARM: AtomicU64 = AtomicU64::new(0);
            if !(mutant_on("audit_unarm_half") && UNARM.fetch_add(1, Relaxed) % 2 == 1) {
                HELD_SINCE.with(|s| s.set(unix_syscalls()));
            }
            HOLD_LOST0.with(|l| l.set(shim_lost()));
            HOLD_T0.with(|t| {
                if let Ok(t) = t.try_borrow() {
                    HOLD_C0.with(|c| c.set(shim_counts(&t[..])));
                }
            });
        }
    }
}

/// This thread is letting go of a branch store's mutex (its guard's drop, before the unlock).
pub(crate) fn store_unlocked() {
    store_unlocked_of(None);
}

/// `store_unlocked`, with the identity of the store whose guard this is (`StoreGuard`):
/// `(id, audited)`. `None` (a test driving the probe directly): no store.
pub(crate) fn store_unlocked_of(own: Option<(usize, bool)>) {
    let depth = DEPTH.with(|d| {
        d.set(d.get().saturating_sub(1));
        d.get()
    });
    if depth == 0 {
        // Mutant `audit_skip_half` (test builds only; counters review 2 MED 3): every second
        // outermost hold of an audited store skips the probe entirely, before any count: the hold
        // audit's completeness premise must fire.
        if own.is_some_and(|o| o.1) && mutant_on("audit_skip_half") {
            static SKIP: AtomicU64 = AtomicU64::new(0);
            if SKIP.fetch_add(1, Relaxed) % 2 == 1 {
                HELD_SINCE.with(|s| s.take());
                return;
            }
        }
        let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
        let (bytes0, rows0) = HOLD_START.with(|h| h.get());
        let (bytes, rows) = (get(&T_HELD_ALLOC_BYTES).wrapping_sub(bytes0), get(&T_CAT_ROWS).wrapping_sub(rows0));
        let work = get(&T_HELD_WORK).wrapping_sub(HOLD_START_WORK.with(|h| h.get()));
        MAX_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
        MAX_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
        MAX_HOLD_WORK.fetch_max(work, Relaxed);
        if FOREGROUND.with(|f| f.get()) {
            MAX_FG_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
            MAX_FG_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
            MAX_FG_HOLD_WORK.fetch_max(work, Relaxed);
        } else {
            MAX_BG_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
            MAX_BG_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
            MAX_BG_HOLD_WORK.fetch_max(work, Relaxed);
        }
        // Review 2 L7: the kernel count is read only when an armed hold sampled it at its lock.
        // Class (b): the hold's syscalls are this thread's only if no other thread ran during it
        // (`hold_attributed`); otherwise the hold is counted as unattributable, never as held.
        // The hold's audit decision, made once (counters review 2 MED 4: AUDIT was loaded twice, so a
        // disarm between the loads split a hold): a guard's (`own`, already AND-ed with AUDIT by
        // `StoreGuard::drop`), or for a direct test caller the scope's.
        let (store, audited) = match own {
            Some(o) => o,
            None => (crate::branch::store::scope_store_id(), AUDIT.load(Relaxed) && crate::branch::store::scope_is_audited()),
        };
        let since = HELD_SINCE.with(|s| s.take());
        if since.is_none() && audited {
            AUDIT_EXCLUDED.fetch_add(1, Relaxed);
        }
        if let Some(since) = since {
            // The shim's counts (of the threads at the start), then the kernel's, then the table:
            // the mirror of the start's order (see `store_locked_at`).
            let c1 = HOLD_T0.with(|t0| t0.try_borrow().map(|t0| shim_counts(&t0[..])).unwrap_or([0; 48]));
            let lost1 = shim_lost();
            // probe-adversary-2 L1: a hold whose end count cannot be read is unattributable, never
            // silently dropped from both counts.
            let now = unix_syscalls();
            if now.is_none() {
                HELD_UNATTRIBUTED.fetch_add(1, Relaxed);
                bump(&T_HELD_UNATTRIBUTED, 1);
                if audited {
                    AUDIT_EXCLUDED.fetch_add(1, Relaxed);
                }
            }
            if let Some(now) = now {
                let held = now.wrapping_sub(since) & u64::from(u32::MAX);
                let mut t1 = [ThreadCpu::EMPTY; 48];
                let n1 = thread_table(&mut t1);
                let ends = Ends { n0: HOLD_N0.with(|n| n.get()), n1, lost0: HOLD_LOST0.with(|l| l.get()), lost1 };
                let attributed = HOLD_T0.with(|t0| {
                    t0.try_borrow()
                        .ok()
                        .and_then(|t0| attribute(held, &t0[..], &HOLD_C0.with(|c| c.get()), &t1, &c1, ends, &[], false).0)
                });
                if attributed.is_none() {
                    HOLD_REFUSALS.fetch_add(1, Relaxed);
                }
                if let Some(held) = attributed {
                    HELD_SYSCALLS.fetch_add(held, Relaxed);
                    bump(&T_HELD_SYSCALLS, held);
                } else {
                    HELD_UNATTRIBUTED.fetch_add(1, Relaxed);
                    bump(&T_HELD_UNATTRIBUTED, 1);
                }
                // Counters review E1: the store whose hold this is comes from its guard, never from
                // whatever scope is innermost (a nested store's, a stray one).
                if audited {
                    let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
                    let (allocs0, stmts0) = HOLD_START2.with(|h| h.get());
                    let me = t1.iter().find(|t| t.me).map_or(0, |t| t.id);
                    let rec = HoldRec {
                        site: HOLD_SITE.with(|h| h.get()).unwrap_or(std::panic::Location::caller()),
                        thread: me,
                        syscalls: attributed,
                        allocs: get(&T_HELD_ALLOCS).wrapping_sub(allocs0),
                        alloc_bytes: bytes,
                        cat_stmts: get(&T_CAT_STMTS).wrapping_sub(stmts0),
                        store,
                    };
                    match AUDIT_LOG.try_lock() {
                        Ok(mut log) if log.len() < log.capacity() => log.push(rec), // lint-allow: within the reserved room, no allocation
                        _ => {
                            AUDIT_LOST.fetch_add(1, Relaxed);
                        }
                    }
                }
            }
        }
    }
}

/// A catalog statement ran on this thread (`catalog::Stmt::{rows, each_row, exec}`; `write` for
/// `exec`), touching `rows` rows (read, or 1 written), on ANY of the store's catalog connections —
/// the store's own, the checkpoint writer's, the name filter's reader — unlike the catalog's own
/// counters, which see one connection each. BLIND SPOT: transaction control and pragmas
/// (`conn.execute("BEGIN")`, `wal_checkpoint`) are not statements of `Stmt` and are not counted.
pub(crate) fn catalog_statement(write: bool, rows: u64) {
    CAT_STMTS.fetch_add(1, Relaxed);
    CAT_ROWS.fetch_add(rows, Relaxed);
    bump(&T_CAT_STMTS, 1);
    bump(&T_CAT_ROWS, rows);
    if write {
        CAT_WRITES.fetch_add(1, Relaxed);
        bump(&T_CAT_WRITES, 1);
    }
}

/// Bytes requested by every allocation alive now, process-wide (the counting allocator's own
/// arithmetic: requested sizes, not the allocator's rounding or its caches).
pub(crate) fn live_heap_bytes() -> i64 {
    LIVE_BYTES.load(Relaxed)
}

/// `(bytes allocated, catalog rows touched)` in the largest single store-mutex hold since the last
/// `take_hold_maxima`: by any thread, by threads not marked foreground, and by the foreground one.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HoldMaxima {
    pub(crate) all: (u64, u64),
    pub(crate) background: (u64, u64),
    pub(crate) foreground: (u64, u64),
    /// Units of held work (`note_held_work`) in the largest hold: `(all, background, foreground)`.
    pub(crate) work: (u64, u64, u64),
    /// Units of work at each `WorkSite` (indexed by it), each the largest one occurrence did, apart.
    pub(crate) site_work: [u64; 4],
    /// How many times each `WorkSite` ran (MED-7).
    pub(crate) site_runs: [u64; 4],
}

/// Every hold maximum at once, and start them all again from zero (review 2 L8: one call, so no
/// order of two calls can zero one maximum before it is read).
pub(crate) fn take_hold_maxima() -> HoldMaxima {
    HoldMaxima {
        all: (MAX_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_HOLD_CAT_ROWS.swap(0, Relaxed)),
        background: (MAX_BG_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_BG_HOLD_CAT_ROWS.swap(0, Relaxed)),
        foreground: (MAX_FG_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_FG_HOLD_CAT_ROWS.swap(0, Relaxed)),
        work: (MAX_HOLD_WORK.swap(0, Relaxed), MAX_BG_HOLD_WORK.swap(0, Relaxed), MAX_FG_HOLD_WORK.swap(0, Relaxed)),
        site_work: [0, 1, 2, 3].map(|i: usize| MAX_SITE_WORK[i].swap(0, Relaxed)),
        site_runs: [0, 1, 2, 3].map(|i: usize| SITE_RUNS[i].swap(0, Relaxed)),
    }
}

/// The sites whose per-name work is measured apart (budgets-rev-242345 LOW-4, LOW-5): the name filter's
/// seed hold, its build's install hold, a growth's swap and drain holds, and recovery's replay.
#[derive(Clone, Copy)]
pub(crate) enum WorkSite {
    Seed = 0,
    Install = 1,
    Growth = 2,
    Recovery = 3,
}

/// The work this thread does while it lives (`note_held_work`), recorded on every exit (its drop,
/// unwinding included) into its site's own maximum (`HoldMaxima::site_work`). Allocates nothing.
pub(crate) struct SiteWork(WorkSite, u64);

impl SiteWork {
    pub(crate) fn start(site: WorkSite) -> Self {
        SiteWork(site, site_work_begin())
    }
}

impl Drop for SiteWork {
    fn drop(&mut self) {
        site_work_end(self.0, self.1);
    }
}

/// This thread's work count now (`SiteWork::start`).
pub(crate) fn site_work_begin() -> u64 {
    T_HELD_WORK.with(|c| c.get())
}

/// The work done on this thread since `w0`, into `site`'s maximum (`SiteWork`'s drop).
pub(crate) fn site_work_end(site: WorkSite, w0: u64) {
    let now = T_HELD_WORK.with(|c| c.get());
    MAX_SITE_WORK[site as usize].fetch_max(now.wrapping_sub(w0), Relaxed);
    SITE_RUNS[site as usize].fetch_add(1, Relaxed);
}

/// `n` units of work done on this thread (test builds call it from the store's per-item loops that
/// allocate nothing, a name filter insert): counted per store-mutex hold into the hold maxima
/// (`HoldMaxima::work`), so an O(N) loop moved under the mutex is seen on every platform. Allocates
/// nothing.
pub(crate) fn note_held_work(n: u64) {
    bump(&T_HELD_WORK, n);
}

/// Mark the calling thread as the foreground (measuring) thread, or unmark it.
pub(crate) fn mark_foreground(on: bool) {
    FOREGROUND.with(|f| f.set(on));
}

/// Sample the syscall count around store-mutex holds from now on (off by default: two Mach traps
/// per hold would slow every other test).
pub(crate) fn arm(on: bool) {
    // Counters review 4 LOW-D: an armed hold is measured, so what the hold path reads is primed
    // before the first one (an armed hold before any window otherwise fell back to the store's
    // fe_mutant, whose first call allocates under the mutex). A disarm primes nothing: it may run in
    // a Drop while a refused `prime` unwinds.
    if on {
        prime();
    }
    // Release: see `store_locked_at` (counters review 4 LOW-F).
    ARMED.store(on, Release);
}

/// Every counter at one moment. The thread fields are the calling thread's; the rest are the
/// process's. Read in an order that keeps the kernel counts out of each other: instructions (a BSD
/// syscall) first, the syscall count (a Mach trap) last.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) instructions: Option<u64>,
    pub(crate) allocs: u64,
    pub(crate) alloc_bytes: u64,
    pub(crate) t_allocs: u64,
    pub(crate) t_alloc_bytes: u64,
    pub(crate) frees: u64,
    pub(crate) cat_stmts: u64,
    pub(crate) t_cat_stmts: u64,
    pub(crate) cat_writes: u64,
    pub(crate) t_cat_writes: u64,
    pub(crate) cat_rows: u64,
    pub(crate) t_cat_rows: u64,
    pub(crate) t_frees: u64,
    pub(crate) t_free_bytes: u64,
    pub(crate) t_held_allocs: u64,
    pub(crate) t_held_alloc_bytes: u64,
    pub(crate) locks: u64,
    pub(crate) t_locks: u64,
    pub(crate) held_syscalls: u64,
    pub(crate) t_held_syscalls: u64,
    /// Holds (any thread) that could not be attributed to their holder (`HELD_UNATTRIBUTED`).
    pub(crate) held_unattributed: u64,
    /// Threads other than this one, its spinners, and those the window owns, that ran inside the
    /// window (`end_owning`): when not 0, `syscalls` is `None`.
    pub(crate) foreign: u64,
    /// The window's own syscalls (`attribute`), or `None` when they cannot be attributed: the only
    /// syscall count a budget reads (the task's raw count is `syscalls`, for the fire-checks).
    pub(crate) attributed_syscalls: Option<u64>,
    /// Syncs issued by a thread while it held a store mutex (the engine's own
    /// `store::syncs_under_store_mutex`: attributed to the holder, so exact with other threads
    /// running, unlike `held_syscalls`).
    pub(crate) held_syncs: u64,
    pub(crate) fsync: u64,
    pub(crate) full_fsync: u64,
    pub(crate) barrier: u64,
    /// The process's syncs at this snapshot (`branch::sync_counts`), beside the budget's reading
    /// above: counters review MED-5's parity check in a budget child, where the process's counts
    /// are exact.
    pub(crate) p_fsync: u64,
    pub(crate) p_full_fsync: u64,
    pub(crate) p_barrier: u64,
    /// The syncs counted as the stores' this thread opened (`store::own_sync_counts`), beside them:
    /// the attribution's own parity in a budget child.
    pub(crate) o_fsync: u64,
    pub(crate) o_full_fsync: u64,
    pub(crate) o_barrier: u64,
    pub(crate) syscalls: Option<u64>,
    /// Whether the per-thread counter was loaded, and its lost count at this snapshot (probe-
    /// adversary item 1: every sample records its mode).
    pub(crate) shim_loaded: bool,
    pub(crate) shim_lost: u64,
    /// Why `attributed_syscalls` is `None`, if it is (`Refusal`).
    pub(crate) refusal: Refusal,
    /// The Linux port (2026-10-18): whether the thread rule alone (the same `attribute` checks on the
    /// two thread tables, with no count to attribute) shows the window was the caller's own: no thread
    /// gone, none new and running, no foreign thread ran. Where a kernel count exists (Apple) it is
    /// `attributed_syscalls.is_some()`; off Apple it is the only evidence, since no count exists.
    pub(crate) threads_quiet: bool,
}

impl Snapshot {
    /// Whether the window this snapshot closed was the caller's own (no other thread ran in it): its
    /// attributed syscall count where a kernel count exists (Apple), else the thread rule alone
    /// (`threads_quiet`). The one place this rule lives for every quiet flag that rests on a window
    /// snapshot (measure_owning's, recovery's two); the memory arm's quiet has no window and is its
    /// settle alone. (The Linux port: recovery's quiet read the attributed count directly, so every
    /// recovery window was refused off Apple, raws ctr8-budgets.)
    pub(crate) fn own(&self) -> bool {
        self.attributed_syscalls.is_some() || (self.syscalls.is_none() && self.threads_quiet)
    }
}

/// Take a snapshot to open a window (see [`Snapshot`]): every thread's CPU time is read just
/// before the syscall count (class (b): `end_owning` attributes the window by exclusion).
pub(crate) fn begin() -> Snapshot {
    prime();
    let instructions = instructions();
    let mut s = user_counts();
    s.instructions = instructions;
    WINDOW_T0.with(|t| {
        let mut t = t.borrow_mut();
        *t = [ThreadCpu::EMPTY; 48];
        WINDOW_N0.with(|n| n.set(thread_table(&mut t[..])));
    });
    s.syscalls = unix_syscalls();
    WINDOW_S0.with(|c| c.set(s.syscalls));
    // The shim after the kernel (see `store_locked_at`), then nothing more that counts.
    WINDOW_LOST0.with(|l| l.set(shim_lost()));
    WINDOW_T0.with(|t| WINDOW_C0.with(|c| c.set(shim_counts(&t.borrow()[..]))));
    s.shim_loaded = shim().is_some();
    s
}

/// The engine lane's per-thread syscall counter (`feshim`, artie frontier/fastest/lanes/engine/feshim:
/// a DYLD_INSERT_LIBRARIES shim over libsystem_kernel's wrappers, derived from the wire lane's scshim),
/// when the process was started with it: `feshim_syscalls_of(thread id)`. Looked up once; `prime`
/// does it before any count is read, so the lookup's own cost is in no window.
fn shim() -> Option<unsafe extern "C" fn(u64) -> u64> {
    static SHIM: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    #[cfg(target_vendor = "apple")]
    let addr = *SHIM.get_or_init(|| {
        // SAFETY: a symbol lookup by a NUL-terminated name in every loaded image.
        unsafe { libc::dlsym(libc::RTLD_DEFAULT, b"feshim_syscalls_of\0".as_ptr().cast()) as usize }
    });
    #[cfg(not(target_vendor = "apple"))]
    let addr = *SHIM.get_or_init(|| 0);
    // SAFETY: the shim exports `uint64_t feshim_syscalls_of(uint64_t)`.
    (addr != 0).then(|| unsafe { std::mem::transmute::<usize, unsafe extern "C" fn(u64) -> u64>(addr) })
}

/// The shim's count of calls it could put in no thread's slot (`feshim_lost`); 0 without the shim.
/// Looked up once, as `shim`.
fn shim_lost() -> u64 {
    static LOST: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    #[cfg(target_vendor = "apple")]
    let addr = *LOST.get_or_init(|| {
        // SAFETY: a symbol lookup by a NUL-terminated name in every loaded image.
        unsafe { libc::dlsym(libc::RTLD_DEFAULT, b"feshim_lost\0".as_ptr().cast()) as usize }
    });
    #[cfg(not(target_vendor = "apple"))]
    let addr = *LOST.get_or_init(|| 0);
    if addr == 0 {
        return 0;
    }
    // SAFETY: the shim exports `uint64_t feshim_lost(void)`, a read-only load.
    unsafe { std::mem::transmute::<usize, unsafe extern "C" fn() -> u64>(addr)() }
}

/// Look the shim up now (see `shim`), outside any window. A run labelled a shim run (`FE_SHIM_RUN`,
/// set by the feshim-run runner) whose shim is not loaded is REFUSED here, a panic, rather than
/// measured as if it were one (probe-adversary item 1: a run with the shim stripped passed exactly as
/// a shim run did, and nothing recorded the mode).
pub(crate) fn prime() {
    // Counters review 2 HIGH 1: every environment flag a window consults is read here, once, outside
    // any window (an env read allocates its value).
    let _ = IN_BUDGET_CHILD.get_or_init(|| std::env::var_os("FE_BUDGET_CHILD").is_some());
    let _ = NEW_NAMES_FILE.get_or_init(|| std::env::var_os("FE_BUDGET_NEW_NAMES"));
    let _ = FOREIGN_NAMES.get_or_init(|| std::env::var_os("FE_BUDGET_FOREIGN_NAMES").is_some());
    // Counters review 3 MED-A: the mutant name too, and the store's own reader, whose first call
    // reads FE_MUTANT (an allocation, never freed) where it would otherwise land: in attribute,
    // inside a window.
    let _ = MUTANT.get_or_init(|| std::env::var("FE_MUTANT").ok());
    let _ = crate::branch::store::fe_mutant("");
    // Counters review 4 LOW-B: the other read-once mutant selector (R13_MUTANT) too.
    let _ = crate::branch::store::mutant("");
    let loaded = shim().is_some();
    let _ = shim_lost();
    if !loaded && std::env::var_os("FE_SHIM_RUN").is_some() {
        panic!("budget_probe: REFUSED: this run is labelled a shim run (FE_SHIM_RUN) but feshim is not loaded");
    }
    // Mutant `probe_prime_allocates` (test builds only; counters review 5): a primed prime that still
    // allocates, which `arm(true)` would then do inside a window.
    if mutant_on("probe_prime_allocates") {
        std::hint::black_box(Box::new(0u8));
    }
}

/// The flags `prime` reads once (counters review 2 HIGH 1): whether this process is a budget
/// child, and the two window diagnostics' settings.
static IN_BUDGET_CHILD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
static NEW_NAMES_FILE: std::sync::OnceLock<Option<std::ffi::OsString>> = std::sync::OnceLock::new();
static FOREIGN_NAMES: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// The mutant this process runs (`FE_MUTANT`), read once by `prime` (counters review 3 MED-A).
static MUTANT: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// Whether mutant `name` is on, for the probe's window and hold paths: the name `prime` read, so no
/// window reads the environment. Unprimed (an unarmed hold before any window), the store's own reader.
pub(crate) fn mutant_on(name: &str) -> bool {
    match MUTANT.get() {
        Some(m) => m.as_deref() == Some(name),
        None => crate::branch::store::fe_mutant(name), // lint-allow: unprimed: no window and no armed hold (begin and arm(true) prime first)
    }
}

/// The syncs a budget window reads (counters review MED-5): in a budget child, which runs one
/// cell's stores at a time, the process's (`branch::sync_counts`, exact there, and blind to no
/// thread); elsewhere, a test's own stores' (`store::own_sync_counts`).
pub(crate) fn window_syncs() -> crate::branch::SyncCounts {
    let child = match IN_BUDGET_CHILD.get() {
        Some(&c) => c,
        // Never reached inside a window (`begin` primes first); a reading before any window primes.
        None => {
            prime();
            IN_BUDGET_CHILD.get().copied().unwrap_or(false)
        }
    };
    if child {
        crate::branch::sync_counts()
    } else {
        crate::branch::store::own_sync_counts()
    }
}

/// Whether the per-thread counter is loaded (budgets that cannot be attributed by exclusion need it).
pub(crate) fn per_thread_counter_loaded() -> bool {
    shim().is_some()
}

/// The shim's count of each thread in `t`, in `t`'s order; all 0 without the shim.
fn shim_counts(t: &[ThreadCpu]) -> [u64; 48] {
    let mut out = [0; 48];
    if let Some(f) = shim() {
        for (o, x) in out.iter_mut().zip(t) {
            if x.id != 0 {
                // SAFETY: the shim's read-only lookup.
                *o = unsafe { f(x.id) };
            }
        }
    }
    out
}

/// The two ends of a span, beyond the threads' own readings: how many threads `thread_table`
/// reported at each, and the shim's lost count at each.
#[derive(Clone, Copy)]
struct Ends {
    n0: Option<usize>,
    n1: Option<usize>,
    lost0: u64,
    lost1: u64,
}

/// Why a span has no count (`attribute`), recorded in every sample (probe-adversary-2 L4: a refused
/// table and a foreign thread used to look the same, `foreign_threads = 1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Refusal {
    #[default]
    None = 0,
    /// The thread table could not be read, or held more than the 48 threads it has room for.
    Table = 1,
    /// A thread present at the start is gone at the end.
    ThreadGone = 2,
    /// A thread not present at the start is still there at the end (one the span spawned that
    /// outlives it, or any new thread during a hold).
    ThreadNew = 3,
    /// Another thread ran, and there is no shim to tell its calls apart.
    ForeignNoShim = 4,
    /// The shim lost a call (`feshim_lost`).
    ShimLost = 5,
    /// Another thread made calls in the span.
    ForeignCalls = 6,
    /// The shim's deltas do not sum to the kernel's count.
    SumMismatch = 7,
    /// The kernel's count could not be read.
    NoKernelCount = 8,
}

/// Class (b)'s attribution, one rule for windows and holds: `kernel` is the task's count over the
/// span; `(t0, c0)` every thread's CPU time and shim count at its start, `t1` every thread's CPU
/// time at its end and `c1` the shim counts at its end of the threads in `t0`, in `t0`'s order.
/// Returns the count (or `None`), the foreign threads, and why a count was refused (`Refusal`).
///
/// Refused on both paths (probe-adversary items 4, probe-adversary-2 M1): a thread table that could
/// not be read or holds more than the 48 threads it has room for (a 49th thread is invisible); a
/// thread present at the start that is gone at the end (its calls, its exit included, would be
/// charged to the span); and a thread new at the end, still running (a thread the window spawned
/// that outlives it would have its later calls dropped: probe-adversary-2 row 11, Some(7) against
/// truth 22. This also covers a persistent store thread the operation spawned, which the callers'
/// `threads()` checks do not see, since `threads()` leaves out `PERSISTENT_LIVE`).
///
/// With no other thread running (`foreign` 0), the kernel count is the span's own: the EXCLUSION
/// path, trusted as an upper bound (it can only overcount).
///
/// With some, the SHIM path, which may only accept a span in which no thread but its own made a
/// call (probe-adversary items 2 and 3). It refuses when: the shim is not loaded; it lost a call
/// (`feshim_lost` above 0); any thread other than the span's own (the reading thread, and for a
/// window those named in `owned`) has a shim delta above 0; or the shim's deltas do not sum to
/// `kernel` (a foreign thread's call the shim cannot see). An accepted span's count is `kernel`,
/// so the shim path too can only overcount.
///
/// BLIND SPOTS, measured (probe-adversary rows 2c and 9, probe-adversary-2 rows 5-14, and
/// artie frontier/fastest/lanes/wire/budget/scshim/probe_contended.c):
/// - A thread's CPU time is stale while it runs: a continuously spinning foreign thread read as
///   not running in 346 of 500 windows (probe-adversary-2 row 14), and its resolution is 1 µs. Such a
///   thread is not foreign, and on the exclusion path its calls, if any, are charged to the span:
///   an overcount, never an undercount.
/// - Calls the kernel counts and the shim does not: `syscall(2)` and a raw `svc` (10 of 10), the
///   `posix_spawn` call, a thread's creation (2 unseen calls) and its exit (1 unseen); lock waits
///   (`__psynch_*`) ARE seen (probe_contended: 2449 of 2452 kernel calls seen with two threads
///   contending a mutex, 3 uncovered against the uncontended control's 2). They can only cause
///   refusals.
/// - Calls the shim counts and the kernel does not: `kdebug_trace`, `kdebug_trace_string` and
///   `kdebug_typefilter` after its first call (tracing off: shim +10, kernel 0), so the sum check
///   can be balanced by one of them against an unseen call (probe-adversary-2 rows 6-7: a span
///   holding a foreign call accepted, at one over the truth). The count is still the kernel's, so
///   the error is OVER only, a false red. A call whose shim increment lands before the end's shim
///   read and whose kernel entry after the kernel's read does the same (source-only).
/// - Work done by a child process (posix_spawn, vfork) is counted by neither counter: it is
///   another task. Mach traps are not BSD syscalls and are counted by neither.
/// - A thread the span spawned that is BLOCKED at its end is accepted (the open's persistent
///   threads, parked): work it does after waking is not the span's count, even when the span queued
///   it. Only a spawned thread still running at the end is refused.
fn attribute(kernel: u64, t0: &[ThreadCpu], c0: &[u64; 48], t1: &[ThreadCpu], c1: &[u64; 48], ends: Ends, owned: &[&str], window: bool) -> (Option<u64>, u64, Refusal) {
    // Mutants (test builds only), each a rule this replaced or a check it makes:
    // `probe_full_table_trusted` (a table past its room is read as complete),
    // `probe_gone_thread_charged` (a thread gone at the end is ignored), `probe_new_thread_kept`
    // (a thread new at the end refused on the shim path only, as before probe-adversary-2 M1),
    // `probe_shim_excludes_foreign` (the shim path as before: the own threads' deltas, a foreign
    // thread's calls excluded, only the total checked), `probe_sum_unchecked` (no sum check).
    let mutant = |m: &str| mutant_on(m);
    let room = t0.len().min(t1.len());
    match (ends.n0, ends.n1) {
        (Some(n0), Some(n1)) if (n0 <= room && n1 <= room) || mutant("probe_full_table_trusted") => {}
        _ => return (None, 0, Refusal::Table),
    }
    let present = |t: &[ThreadCpu], id: u64| t.iter().any(|x| x.id == id);
    if t0.iter().any(|x| x.id != 0 && !present(t1, x.id)) && !mutant("probe_gone_thread_charged") {
        return (None, 0, Refusal::ThreadGone);
    }
    // A thread new at the end and still RUNNING then: its work goes on past the end, so the span
    // would miss it. One the span spawned that is blocked at the end (a persistent store thread
    // parked, the open's confirmation writer) is the span's, and what it does later is a later
    // span's work.
    let new_at_end = t1.iter().any(|x| x.id != 0 && !present(t0, x.id));
    let new_running = t1.iter().any(|x| x.id != 0 && !x.waiting && !present(t0, x.id));
    let keep_new = mutant("probe_new_thread_kept");
    if new_running && !keep_new {
        return (None, 0, Refusal::ThreadNew);
    }
    let foreign = foreign_threads(t0, t1, owned, !window);
    if foreign == 0 {
        return (Some(kernel), 0, Refusal::None);
    }
    if shim().is_none() {
        return (None, foreign, Refusal::ForeignNoShim);
    }
    if ends.lost0 != 0 || ends.lost1 != 0 {
        return (None, foreign, Refusal::ShimLost);
    }
    if new_at_end {
        return (None, foreign, Refusal::ThreadNew);
    }
    let me = t1.iter().find(|t| t.me).map_or(0, |t| t.id);
    let owned_name = |t: &ThreadCpu| owned_named(t, owned);
    let mut all = 0u64;
    let mut own_sum = 0u64;
    let excludes = mutant("probe_shim_excludes_foreign");
    for (i, t) in t0.iter().enumerate().filter(|(_, t)| t.id != 0) {
        let d = c1[i].wrapping_sub(c0[i]);
        let own = t.id == me || (window && owned_name(t));
        if d > 0 && !own && !excludes {
            return (None, foreign, Refusal::ForeignCalls);
        }
        if own {
            own_sum = own_sum.wrapping_add(d);
        }
        all = all.wrapping_add(d);
    }
    if all == kernel || mutant("probe_sum_unchecked") {
        (Some(if excludes { own_sum } else { kernel }), foreign, Refusal::None)
    } else {
        (None, foreign, Refusal::SumMismatch)
    }
}

/// `end_owning` owning no other thread.
pub(crate) fn end() -> Snapshot {
    end_owning(&[])
}

/// Take a snapshot to close a window: the syscall count first, every thread's CPU time just after
/// it, instructions last. The window's syscalls are THIS thread's (and those of threads it spawned,
/// and of the threads named in `owned`, which the measurement charges to itself: a persistent
/// worker it dispatches) only if no other thread that existed at `begin` ran inside it (class (b),
/// DECISIONS 994f7d41e0: the task-wide count alone is retired). Otherwise `syscalls` is `None` and
/// `foreign` counts the threads that ran: a refusal, never a number. Blind spot: a foreign thread's
/// CPU time below the kernel's accounting resolution in the window is not seen; a declared spinner
/// (`spinning`) is trusted to make no syscall.
pub(crate) fn end_owning(owned: &[&str]) -> Snapshot {
    // The shim's counts (of the threads at `begin`) and lost count, then the kernel's, then the
    // table: the mirror of `begin` (see `store_locked_at`).
    let c1 = WINDOW_T0.with(|t0| shim_counts(&t0.borrow()[..]));
    let lost1 = shim_lost();
    let syscalls = unix_syscalls();
    let mut t1 = [ThreadCpu::EMPTY; 48];
    let n1 = thread_table(&mut t1);
    let ends = Ends { n0: WINDOW_N0.with(|n| n.get()), n1, lost0: WINDOW_LOST0.with(|l| l.get()), lost1 };
    let (attributed, foreign, refusal) = match (WINDOW_S0.with(|c| c.get()), syscalls) {
        (Some(s0), Some(s1)) => {
            let kernel = s1.wrapping_sub(s0) & u64::from(u32::MAX);
            WINDOW_T0.with(|t0| attribute(kernel, &t0.borrow()[..], &WINDOW_C0.with(|c| c.get()), &t1, &c1, ends, owned, true))
        }
        _ => (None, 0, Refusal::NoKernelCount),
    };
    // The Linux port: with no kernel count, the thread rule still decides whether the window was the
    // caller's own (the same checks, with a count of 0 that nothing reads), and names the foreign
    // threads; the syscall count stays None.
    // On Apple a missing count stays a refusal, as before: the rule replaces a count only where none exists.
    // The refusal is the rule's own where it refuses (a ThreadNew window reads ThreadNew on both platforms:
    // the Linux port's run 1 found fc_thread_new_quiet reading NoKernelCount, ecetesla4 raw ctr6-instr);
    // NoKernelCount only where the rule passes and the count alone is missing.
    let (threads_quiet, foreign, refusal) = if !cfg!(target_vendor = "apple") && syscalls.is_none() && WINDOW_N0.with(|n| n.get()).is_some() && n1.is_some() {
        let (own, f, r) = WINDOW_T0.with(|t0| attribute(0, &t0.borrow()[..], &WINDOW_C0.with(|c| c.get()), &t1, &c1, ends, owned, true));
        (own.is_some() && f == 0, f, if own.is_some() { refusal } else { r })
    } else {
        (attributed.is_some(), foreign, refusal)
    };
    // Mutants (test builds only; counters review 4 MED-1): `probe_diagnostics_first` runs the
    // window's diagnostics before its counts are read, as a regression would; `probe_box_before_counts`
    // allocates once there, the control every exact-count fire-check must see.
    let diagnostics_first = mutant_on("probe_diagnostics_first");
    if diagnostics_first {
        window_diagnostics(refusal, foreign, owned, attributed, &t1);
    }
    if mutant_on("probe_box_before_counts") {
        std::hint::black_box(Box::new(0u8)); // lint-allow: mutant probe_box_before_counts, the control
    }
    let mut s = user_counts();
    s.foreign = foreign;
    s.refusal = refusal;
    s.syscalls = syscalls;
    s.attributed_syscalls = attributed;
    s.shim_loaded = shim().is_some();
    s.shim_lost = lost1;
    s.threads_quiet = threads_quiet;
    s.instructions = instructions();
    if !diagnostics_first {
        window_diagnostics(refusal, foreign, owned, attributed, &t1);
    }
    s
}

/// A window's diagnostics, after every count of it is taken (counters review 2 HIGH 1: they
/// allocate, so they are not on the window path): the threads new at a `ThreadNew` refusal, and
/// the foreign threads' names. Off unless `prime` read their settings.
fn window_diagnostics(refusal: Refusal, foreign: u64, owned: &[&str], attributed: Option<u64>, t1: &[ThreadCpu; 48]) {
    // Diagnostics only (FE_BUDGET_NEW_NAMES names a file): a window refused for a thread new at its
    // end appends the names of the threads in the end table that were not in the start one, after
    // every count of the window is taken.
    if let Some(file) = NEW_NAMES_FILE.get().cloned().flatten().filter(|_| refusal == Refusal::ThreadNew) {
        WINDOW_T0.with(|t0| {
            let t0 = t0.borrow();
            let name = |t: &ThreadCpu| {
                let end = t.name.iter().position(|c| *c == 0).unwrap_or(t.name.len());
                String::from_utf8_lossy(&t.name[..end]).to_string()
            };
            let new: Vec<String> = t1.iter().filter(|t| t.id != 0 && !t0.iter().any(|a| a.id == t.id)).map(name).collect();
            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&file) {
                let _ = std::io::Write::write_all(&mut f, format!("thread_new {new:?}
").as_bytes());
            }
        });
    }
    // Diagnostics only (FE_BUDGET_FOREIGN_NAMES set): name the foreign threads on stderr, after every
    // count of the window is taken.
    if foreign > 0 && FOREIGN_NAMES.get().copied().unwrap_or(false) {
        WINDOW_T0.with(|t0| {
            let me = t1.iter().find(|t| t.me).map_or(0, |t| t.id);
            let ran: Vec<_> = ran_between(&t0.borrow()[..], &t1[..], me).into_iter().map(|(_, n, ns)| format!("{n}:{ns}ns")).collect();
            eprintln!("budget window foreign (owned {owned:?}, attributed {attributed:?}): {ran:?}");
        });
    }
}

/// The threads that ran between two `thread_table` readings, other than the reading thread, the
/// declared spinners, and those named in `owned`; threads new in `b` count too when `new_foreign`
/// (a hold), and are the window's own otherwise (spawned by the operation). Allocates nothing.
fn foreign_threads(a: &[ThreadCpu], b: &[ThreadCpu], owned: &[&str], new_foreign: bool) -> u64 {
    let me = b.iter().find(|t| t.me).map_or(0, |t| t.id);
    let spinning = |id: u64| SPINNERS.iter().any(|s| s.load(Relaxed) == id);
    let owned_name = |t: &ThreadCpu| owned_named(t, owned);
    let mut n = 0;
    for t in b.iter().filter(|t| t.id != 0 && t.id != me) {
        let before = a.iter().find(|x| x.id == t.id);
        let ran = match before {
            Some(x) => t.cpu_ns > x.cpu_ns,
            None => new_foreign,
        };
        if ran && !spinning(t.id) && !owned_name(t) {
            n += 1;
        }
    }
    n
}

/// Run `f` declared syscall-free on this thread (a spin on atomics and Mach-only reads, as the
/// harness's waits are): its CPU time does not make another thread's window or hold foreign.
/// Refuses (panics) if eight threads already spin, rather than lose the declaration.
pub(crate) fn spinning<R>(f: impl FnOnce() -> R) -> R {
    let mut t = [ThreadCpu::EMPTY; 48];
    let _ = thread_table(&mut t);
    let me = t.iter().find(|x| x.me).map_or(0, |x| x.id);
    let slot = SPINNERS
        .iter()
        .find(|s| s.compare_exchange(0, me, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire).is_ok())
        .expect("budget_probe::spinning: eight threads already declared");
    struct Undeclare<'a>(&'a AtomicU64);
    impl Drop for Undeclare<'_> {
        fn drop(&mut self) {
            self.0.store(0, std::sync::atomic::Ordering::Release);
        }
    }
    let _r = Undeclare(slot);
    f()
}

/// This thread's store-mutex acquisitions so far (exact per thread however many others run).
pub(crate) fn thread_locks() -> u64 {
    T_LOCKS.with(|c| c.get())
}

/// This thread's `(allocations, bytes, allocations under a store mutex, bytes under one)`: exact
/// per thread however many others run.
pub(crate) fn thread_allocs() -> (u64, u64, u64, u64) {
    let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
    (get(&T_ALLOCS), get(&T_ALLOC_BYTES), get(&T_HELD_ALLOCS), get(&T_HELD_ALLOC_BYTES))
}

fn user_counts() -> Snapshot {
    let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
    let syncs = window_syncs();
    let process = crate::branch::sync_counts();
    let own = crate::branch::store::own_sync_counts();
    Snapshot {
        p_fsync: process.fsync,
        p_full_fsync: process.full_fsync,
        p_barrier: process.barrier,
        o_fsync: own.fsync,
        o_full_fsync: own.full_fsync,
        o_barrier: own.barrier,
        instructions: None,
        allocs: ALLOCS.load(Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Relaxed),
        t_allocs: get(&T_ALLOCS),
        t_alloc_bytes: get(&T_ALLOC_BYTES),
        frees: FREES.load(Relaxed),
        cat_stmts: CAT_STMTS.load(Relaxed),
        t_cat_stmts: get(&T_CAT_STMTS),
        cat_writes: CAT_WRITES.load(Relaxed),
        t_cat_writes: get(&T_CAT_WRITES),
        cat_rows: CAT_ROWS.load(Relaxed),
        t_cat_rows: get(&T_CAT_ROWS),
        t_frees: get(&T_FREES),
        t_free_bytes: get(&T_FREE_BYTES),
        t_held_allocs: get(&T_HELD_ALLOCS),
        t_held_alloc_bytes: get(&T_HELD_ALLOC_BYTES),
        locks: LOCKS.load(Relaxed),
        t_locks: get(&T_LOCKS),
        held_syscalls: HELD_SYSCALLS.load(Relaxed),
        t_held_syscalls: get(&T_HELD_SYSCALLS),
        held_unattributed: HELD_UNATTRIBUTED.load(Relaxed),
        foreign: 0,
        attributed_syscalls: None,
        held_syncs: crate::branch::store::own_test_counts().syncs_under_store_mutex,
        fsync: syncs.fsync,
        full_fsync: syncs.full_fsync,
        barrier: syncs.barrier,
        syscalls: None,
        shim_loaded: false,
        shim_lost: 0,
        refusal: Refusal::None,
        threads_quiet: false,
    }
}

/// The kernel's count of BSD syscalls entered by every thread of this process (`syscalls_unix`,
/// 32 bits, wrapping), or `None` off Apple.
#[allow(deprecated)]
pub(crate) fn unix_syscalls() -> Option<u64> {
    #[cfg(target_vendor = "apple")]
    {
        /// `task_events_info` (mach/task_info.h): eight `integer_t`.
        #[repr(C)]
        #[derive(Default)]
        struct TaskEventsInfo {
            faults: i32,
            pageins: i32,
            cow_faults: i32,
            messages_sent: i32,
            messages_received: i32,
            syscalls_mach: i32,
            syscalls_unix: i32,
            csw: i32,
        }
        const TASK_EVENTS_INFO: u32 = 2;
        let mut info = TaskEventsInfo::default();
        let mut count = (std::mem::size_of::<TaskEventsInfo>() / std::mem::size_of::<i32>()) as u32;
        // SAFETY: `info` is a `task_events_info` and `count` its size in `integer_t`s, as the call
        // requires; the task port is this task's own.
        let kr = unsafe {
            libc::task_info(
                libc::mach_task_self(),
                TASK_EVENTS_INFO,
                (&mut info as *mut TaskEventsInfo).cast(),
                &mut count,
            )
        };
        if kr != 0 {
            return None;
        }
        Some(u64::from(info.syscalls_unix as u32))
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

/// Instructions retired by this process (`ri_instructions`), or `None` off Apple.
pub(crate) fn instructions() -> Option<u64> {
    #[cfg(target_vendor = "apple")]
    {
        // SAFETY: zeroes are a valid `rusage_info_v4` (plain integers and a uuid array).
        let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
        // SAFETY: the buffer is a `rusage_info_v4`, the flavor asked for.
        let rc = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                (&mut info as *mut libc::rusage_info_v4).cast(),
            )
        };
        if rc != 0 {
            return None;
        }
        Some(info.ri_instructions)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

/// This process's physical footprint (`ri_phys_footprint`, the kernel's own count: allocator
/// rounding, its caches and mmap'd memory included, unlike `live_heap_bytes`), or `None` off Apple.
/// A BSD syscall: read it only outside a measured window.
pub(crate) fn phys_footprint() -> Option<u64> {
    #[cfg(target_vendor = "apple")]
    {
        // SAFETY: zeroes are a valid `rusage_info_v4` (plain integers and a uuid array).
        let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
        // SAFETY: the buffer is a `rusage_info_v4`, the flavor asked for.
        let rc = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                (&mut info as *mut libc::rusage_info_v4).cast(),
            )
        };
        if rc != 0 {
            return None;
        }
        Some(info.ri_phys_footprint)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

/// The store threads that live as long as their store (the confirmation writer, review 6 #1) and
/// park between bursts of work: counted live by the engine itself (`persistent_started` before the
/// spawn, `persistent_ended` as the thread's last act; three `cfg(test)` lines in
/// `BranchStore::start_confirm_writer`), and left out of [`threads`] on every platform (review 2 M6;
/// it replaced a name allowlist read with pthread calls inside the measured window, review 2 L1).
static PERSISTENT_LIVE: AtomicI64 = AtomicI64::new(0);

/// A persistent store thread is about to start.
pub(crate) fn persistent_started() {
    PERSISTENT_LIVE.fetch_add(1, Relaxed);
}

/// A persistent store thread is ending (or never started).
pub(crate) fn persistent_ended() {
    PERSISTENT_LIVE.fetch_sub(1, Relaxed);
}

/// The persistent store threads live now (started and not yet ended).
pub(crate) fn persistent_live() -> i64 {
    PERSISTENT_LIVE.load(Relaxed)
}

/// A thread of this process held suspended (Mach `thread_suspend`) until this is dropped, which
/// resumes it and gives its port back. For fire-checks only, outside every measured window (review
/// C14: the confirmation writer delayed between a notification and its wait's return).
#[cfg(target_vendor = "apple")]
pub(crate) struct Suspended(libc::thread_act_t);

#[cfg(target_vendor = "apple")]
extern "C" {
    fn thread_suspend(target: libc::thread_act_t) -> libc::kern_return_t;
    fn thread_resume(target: libc::thread_act_t) -> libc::kern_return_t;
}

#[cfg(target_vendor = "apple")]
impl Drop for Suspended {
    #[allow(deprecated)]
    fn drop(&mut self) {
        extern "C" {
            fn mach_port_deallocate(task: libc::mach_port_t, name: libc::mach_port_t) -> libc::kern_return_t;
        }
        // SAFETY: a thread port of this task that this guard suspended once and still holds.
        unsafe {
            thread_resume(self.0);
            mach_port_deallocate(libc::mach_task_self(), self.0);
        }
    }
}

/// Suspend the one thread of this process whose pthread name is `name`; `None` unless exactly one
/// has it, or if the suspend fails. Not for a measured window (it reads thread names).
#[cfg(target_vendor = "apple")]
#[allow(deprecated)]
pub(crate) fn suspend_thread_named(name: &str) -> Option<Suspended> {
    extern "C" {
        fn mach_port_deallocate(task: libc::mach_port_t, name: libc::mach_port_t) -> libc::kern_return_t;
    }
    let mut list: libc::thread_act_array_t = std::ptr::null_mut();
    let mut count: libc::mach_msg_type_number_t = 0;
    // SAFETY: this task's own port; on success `list` holds `count` thread ports in memory the kernel
    // mapped for us, all given back below but the one kept.
    let task = unsafe { libc::mach_task_self() };
    // SAFETY: as above.
    if unsafe { libc::task_threads(task, &mut list, &mut count) } != 0 {
        return None;
    }
    let mut matching = Vec::new();
    for i in 0..count as usize {
        // SAFETY: `list` holds `count` ports.
        let port = unsafe { *list.add(i) };
        let mut buf = [0 as libc::c_char; 64];
        // SAFETY: a thread port of this task; no new right is made.
        let pt = unsafe { libc::pthread_from_mach_thread_np(port) };
        let named = pt != 0
            // SAFETY: a live pthread of this process and a buffer of the length given.
            && unsafe { libc::pthread_getname_np(pt, buf.as_mut_ptr(), buf.len()) } == 0
            // SAFETY: NUL-terminated by pthread_getname_np.
            && unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }.to_str() == Ok(name);
        if named {
            matching.push(port);
        } else {
            // SAFETY: a send right from `list`, released once.
            unsafe { mach_port_deallocate(task, port) };
        }
    }
    // SAFETY: the array the kernel mapped, of exactly this size.
    unsafe {
        libc::vm_deallocate(
            task,
            list as libc::vm_address_t,
            count as libc::vm_size_t * std::mem::size_of::<libc::thread_act_t>() as libc::vm_size_t,
        )
    };
    if matching.len() != 1 {
        for port in matching {
            // SAFETY: a send right from `list`, released once.
            unsafe { mach_port_deallocate(task, port) };
        }
        return None;
    }
    let only = matching[0];
    // SAFETY: a thread port of this task; a failed suspend leaves it running.
    if unsafe { thread_suspend(only) } != 0 {
        // SAFETY: as above.
        unsafe { mach_port_deallocate(task, only) };
        return None;
    }
    Some(Suspended(only))
}

/// The threads of this process, each as `(is_self, blocked)`, given to `each`; `None` when they cannot
/// be listed (off Apple). Mach traps and userspace only (the port list is given back with
/// `mach_port_deallocate` and `vm_deallocate`), so it can run inside a measured window without moving
/// the syscall count. `blocked` is the kernel's `TH_STATE_WAITING` for the thread.
#[allow(deprecated)]
fn each_thread(mut each: impl FnMut(bool, bool)) -> Option<()> {
    #[cfg(target_vendor = "apple")]
    {
        extern "C" {
            fn mach_port_deallocate(task: libc::mach_port_t, name: libc::mach_port_t) -> libc::kern_return_t;
        }
        let mut list: libc::thread_act_array_t = std::ptr::null_mut();
        let mut count: libc::mach_msg_type_number_t = 0;
        // SAFETY: this task's own port; on success `list` holds `count` thread ports in memory the
        // kernel mapped for us, all given back below.
        let task = unsafe { libc::mach_task_self() };
        // SAFETY: as above.
        if unsafe { libc::task_threads(task, &mut list, &mut count) } != 0 {
            return None;
        }
        // SAFETY: the calling thread's own pthread; no new port right is made.
        let me = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
        for i in 0..count as usize {
            // SAFETY: `list` holds `count` ports.
            let port = unsafe { *list.add(i) };
            // SAFETY: zeroes are a valid `thread_basic_info`; the count is its size in integers.
            let mut info: libc::thread_basic_info = unsafe { std::mem::zeroed() };
            let mut n = libc::THREAD_BASIC_INFO_COUNT;
            // SAFETY: a thread port of this task, the flavor's buffer and its size.
            let kr = unsafe {
                libc::thread_info(
                    port,
                    libc::THREAD_BASIC_INFO as libc::thread_flavor_t,
                    (&mut info as *mut libc::thread_basic_info).cast(),
                    &mut n,
                )
            };
            each(port == me, kr == 0 && info.run_state == libc::TH_STATE_WAITING);
            // SAFETY: `list` holds `count` send rights, each released once.
            unsafe { mach_port_deallocate(task, port) };
        }
        // SAFETY: the array the kernel mapped, of exactly this size.
        unsafe {
            libc::vm_deallocate(
                task,
                list as libc::vm_address_t,
                count as libc::vm_size_t * std::mem::size_of::<libc::thread_act_t>() as libc::vm_size_t,
            )
        };
        Some(())
    }
    // The Linux port (lead ruling 2026-10-18): every task in /proc/self/task, blocked when its state
    // is S or D (see `thread_table` for where this differs from Mach).
    #[cfg(target_os = "linux")]
    {
        let me = linux_tid();
        let (mut ok, mut saw_me) = (true, false);
        linux_each_tid(|tid| match linux_task_stat(tid) {
            Some((state, _)) if state != b'Z' && state != b'X' => {
                saw_me |= tid == me;
                each(tid == me, state == b'S' || state == b'D')
            }
            Some(_) => {}
            // Gone between the listing and the read (ENOENT/ESRCH only): an exited thread, as on Mach,
            // is not listed. Any other failure (EMFILE: budgets-port-adversary HIGH-1) refuses.
            None => ok &= linux_task_alive(tid) == Some(false),
        })?;
        // A reading that does not list the caller is not a reading of this process (HIGH-1).
        (ok && saw_me).then_some(())
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
    {
        let _ = &mut each;
        None
    }
}

/// One thread at a moment (`thread_table`): the kernel's thread id, its CPU time so far (user plus
/// system, ns), whether it is the caller, and its name (NUL-padded, cut at 31 bytes).
#[derive(Clone, Copy)]
pub(crate) struct ThreadCpu {
    pub(crate) id: u64,
    pub(crate) cpu_ns: u64,
    pub(crate) me: bool,
    pub(crate) name: [u8; 32],
    /// The kernel's `TH_STATE_WAITING` for it when read (blocked: parked, sleeping, or in a wait).
    pub(crate) waiting: bool,
}

impl ThreadCpu {
    pub(crate) const EMPTY: ThreadCpu = ThreadCpu { id: 0, cpu_ns: 0, me: false, name: [0; 32], waiting: false };

    pub(crate) fn name(&self) -> String {
        let end = self.name.iter().position(|b| *b == 0).unwrap_or(self.name.len());
        String::from_utf8_lossy(&self.name[..end]).into_owned()
    }
}

/// Every thread of this process now, into `out` (the first `out.len()`), and how many there are;
/// `None` where they cannot be read. On Apple Mach traps and userspace only, as `each_thread`; on Linux /proc (below); with no allocation on either, so it can
/// be read inside a window whose BSD syscalls and allocations are counted. What it is for: the
/// syscall counters here are the task's (`unix_syscalls`), so a window's count is its thread's
/// own only if no other thread ran in it: compare two tables (`ran_between`).
pub(crate) fn thread_table(out: &mut [ThreadCpu]) -> Option<usize> {
    #[cfg(target_vendor = "apple")]
    {
        extern "C" {
            fn mach_port_deallocate(task: libc::mach_port_t, name: libc::mach_port_t) -> libc::kern_return_t;
        }
        let mut list: libc::thread_act_array_t = std::ptr::null_mut();
        let mut count: libc::mach_msg_type_number_t = 0;
        // SAFETY: as `each_thread`.
        let task = unsafe { libc::mach_task_self() };
        // SAFETY: as `each_thread`.
        if unsafe { libc::task_threads(task, &mut list, &mut count) } != 0 {
            return None;
        }
        // SAFETY: as `each_thread`.
        let me = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
        for i in 0..count as usize {
            // SAFETY: `list` holds `count` ports.
            let port = unsafe { *list.add(i) };
            if let Some(slot) = out.get_mut(i) {
                // SAFETY: zeroes are valid for both flavors' structs; each count is its size in
                // integers; the port is a thread of this task.
                let mut ext: libc::thread_extended_info = unsafe { std::mem::zeroed() };
                let mut n = libc::THREAD_EXTENDED_INFO_COUNT;
                let kr = unsafe {
                    libc::thread_info(port, libc::THREAD_EXTENDED_INFO as libc::thread_flavor_t, (&mut ext as *mut libc::thread_extended_info).cast(), &mut n)
                };
                // SAFETY: as above.
                let mut ident: libc::thread_identifier_info = unsafe { std::mem::zeroed() };
                let mut m = libc::THREAD_IDENTIFIER_INFO_COUNT;
                let ki = unsafe {
                    libc::thread_info(port, libc::THREAD_IDENTIFIER_INFO as libc::thread_flavor_t, (&mut ident as *mut libc::thread_identifier_info).cast(), &mut m)
                };
                let mut name = [0u8; 32];
                if kr == 0 {
                    for (d, c) in name.iter_mut().zip(ext.pth_name.iter().take(31)) {
                        *d = *c as u8;
                    }
                }
                *slot = ThreadCpu {
                    id: if ki == 0 { ident.thread_id } else { u64::from(port) },
                    cpu_ns: if kr == 0 { ext.pth_user_time + ext.pth_system_time } else { 0 },
                    me: port == me,
                    name,
                    waiting: kr == 0 && ext.pth_run_state == libc::TH_STATE_WAITING as i32,
                };
            }
            // SAFETY: as `each_thread`.
            unsafe { mach_port_deallocate(task, port) };
        }
        // SAFETY: as `each_thread`.
        unsafe {
            libc::vm_deallocate(
                task,
                list as libc::vm_address_t,
                count as libc::vm_size_t * std::mem::size_of::<libc::thread_act_t>() as libc::vm_size_t,
            )
        };
        Some(count as usize)
    }
    // The Linux port (lead ruling 2026-10-18): /proc/self/task, read with raw syscalls into stack
    // buffers (no allocation; its syscalls are not counted off Apple, where no kernel count exists).
    // Where it differs from Mach, which stays the semantics of record:
    // - the id is the kernel tid, which Linux reuses after pid_max wraps (Mach's thread_id is unique
    //   per boot): a tid reused between two readings of one window would read as one thread;
    // - CPU time is the thread's scheduler run time (its per-thread CPU clock, ns, current even for a
    //   running thread), Mach's is user plus system time;
    // - blocked is state S or D (Mach: TH_STATE_WAITING); a stopped (T, t) thread reads running, as a
    //   Mach-suspended one does; a zombie or dead (Z, X) task is not listed, as Mach lists no exited
    //   thread;
    // - the name is the kernel's comm, at most 15 bytes (TASK_COMM_LEN), so names are compared cut
    //   to it (`owned_named`).
    // A live thread whose CPU time cannot be read refuses the whole table: None, never a guessed time.
    #[cfg(target_os = "linux")]
    {
        let me = linux_tid();
        let mut n = 0usize;
        let (mut ok, mut saw_me) = (true, false);
        linux_each_tid(|tid| {
            // Gone (ENOENT/ESRCH) is skipped; any other failure to read a task refuses the table
            // (budgets-port-adversary HIGH-1: EMFILE read as "gone" gave a complete, empty table).
            let Some((state, name)) = linux_task_stat(tid) else {
                ok &= linux_task_alive(tid) == Some(false);
                return;
            };
            if state == b'Z' || state == b'X' {
                return;
            }
            let Some(cpu_ns) = linux_task_run_ns(tid) else {
                ok &= linux_task_alive(tid) == Some(false);
                return;
            };
            if let Some(slot) = out.get_mut(n) {
                *slot = ThreadCpu { id: tid, cpu_ns, me: tid == me, name, waiting: state == b'S' || state == b'D' };
            }
            saw_me |= tid == me;
            n += 1;
        })?;
        // A table that does not list the caller is not a reading of this process (HIGH-1).
        (ok && saw_me).then_some(n)
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
    {
        let _ = out;
        None
    }
}

/// A store thread that a window may own (`end_owning`), registered by the thread itself at its start
/// under its full name; deregistered when the guard drops. budgets-port-adversary MED-1 (the red: not yet
/// consulted by `owned_named`).
pub(crate) struct OwnedThread;

/// Register the calling thread as the store thread `name` (`OwnedThread`).
pub(crate) fn register_owned_thread(name: &str) -> OwnedThread {
    let _ = name;
    OwnedThread
}

/// Whether `t` is one of the threads named in `owned`. On Linux a thread name is the kernel's comm,
/// cut at 15 bytes (TASK_COMM_LEN), so `owned` is compared cut the same way there.
fn owned_named(t: &ThreadCpu, owned: &[&str]) -> bool {
    let end = t.name.iter().position(|c| *c == 0).unwrap_or(t.name.len());
    let cut = if cfg!(target_os = "linux") { 15 } else { usize::MAX };
    owned.iter().any(|o| &o.as_bytes()[..o.len().min(cut)] == &t.name[..end])
}

/// This thread's kernel id (the raw syscall: glibc before 2.30 has no gettid wrapper).
#[cfg(target_os = "linux")]
fn linux_tid() -> u64 {
    // SAFETY: gettid takes no argument and cannot fail.
    unsafe { libc::syscall(libc::SYS_gettid) as u64 }
}

/// Read the small file at `path` (NUL-terminated) into `buf`: the bytes read, `None` if it cannot be
/// opened or read. Raw syscalls only, no allocation.
#[cfg(target_os = "linux")]
fn linux_read_into(path: &[u8], buf: &mut [u8]) -> Option<usize> {
    // SAFETY: a NUL-terminated path; the descriptor is closed below.
    let fd = unsafe { libc::open(path.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        return None;
    }
    // SAFETY: `buf` is writable for its length.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    // SAFETY: the descriptor opened above.
    unsafe { libc::close(fd) };
    usize::try_from(n).ok()
}

/// `/proc/self/task/<tid>/<leaf>` with its NUL, into `out`; the bytes used.
#[cfg(target_os = "linux")]
fn linux_task_path(tid: u64, leaf: &[u8], out: &mut [u8; 64]) -> usize {
    const HEAD: &[u8] = b"/proc/self/task/";
    out[..HEAD.len()].copy_from_slice(HEAD);
    let mut i = HEAD.len();
    let mut digits = [0u8; 20];
    let (mut v, mut d) = (tid, 0);
    loop {
        digits[d] = b'0' + (v % 10) as u8;
        d += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    while d > 0 {
        d -= 1;
        out[i] = digits[d];
        i += 1;
    }
    out[i] = b'/';
    i += 1;
    out[i..i + leaf.len()].copy_from_slice(leaf);
    i += leaf.len();
    out[i] = 0;
    i + 1
}

/// Whether task `tid` still exists: `Some(true)` if its /proc directory opens, `Some(false)` only if the
/// open fails because the task is gone (ENOENT, ESRCH), `None` on any other failure (EMFILE, ENFILE,
/// ENOMEM...: unknown, which a reader must refuse; budgets-port-adversary HIGH-1).
#[cfg(target_os = "linux")]
fn linux_task_alive(tid: u64) -> Option<bool> {
    let mut path = [0u8; 64];
    let used = linux_task_path(tid, b"", &mut path);
    // SAFETY: a NUL-terminated path; the descriptor is closed below.
    let fd = unsafe { libc::open(path[..used].as_ptr().cast(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) };
    if fd < 0 {
        // SAFETY: this thread's errno, read right after the failed call.
        let errno = unsafe { *libc::__errno_location() };
        return (errno == libc::ENOENT || errno == libc::ESRCH).then_some(false);
    }
    // SAFETY: the descriptor opened above.
    unsafe { libc::close(fd) };
    Some(true)
}

/// Task `tid`'s state letter and name (comm, NUL-padded) from its `stat`.
#[cfg(target_os = "linux")]
fn linux_task_stat(tid: u64) -> Option<(u8, [u8; 32])> {
    let mut path = [0u8; 64];
    let used = linux_task_path(tid, b"stat", &mut path);
    let mut buf = [0u8; 512];
    let n = linux_read_into(&path[..used], &mut buf)?;
    let text = buf.get(..n)?;
    // "tid (comm) S ...": comm may hold spaces and ')', so it ends at the LAST ')'.
    let open = text.iter().position(|c| *c == b'(')?;
    let close = text.iter().rposition(|c| *c == b')')?;
    let state = *text.get(close + 2)?;
    let mut name = [0u8; 32];
    for (d, c) in name.iter_mut().zip(text.get(open + 1..close)?.iter().take(31)) {
        *d = *c;
    }
    Some((state, name))
}

/// Task `tid`'s CPU time so far, ns: its per-thread CPU clock (clock_gettime on the kernel's
/// thread clock id, MAKE_THREAD_CPUCLOCK(tid, CPUCLOCK_SCHED)), which brings a RUNNING thread's time
/// up to date when read. Run 1 read `schedstat` instead, and a helper that ran for well under a tick
/// read as not having run (fc_foreign_window "had no foreign thread" on ecetesla4 and iccad6, raws
/// ctr7-instr_0b26cdece); that schedstat lags a thread not yet descheduled is the suspected cause,
/// UNVERIFIED (the red at 116534802 settles whether this clock fixes it). Our own threads only (the
/// kernel refuses another process's thread clock).
#[cfg(target_os = "linux")]
fn linux_task_run_ns(tid: u64) -> Option<u64> {
    // CPUCLOCK_PERTHREAD_MASK (4) | CPUCLOCK_SCHED (2), with the inverted tid above the low 3 bits.
    let clock = ((!(i32::try_from(tid).ok()?)) << 3) | 6;
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: a valid out-pointer; a bad or foreign clock id only fails (EINVAL).
    if unsafe { libc::clock_gettime(clock as libc::clockid_t, &mut ts) } != 0 {
        return None;
    }
    u64::try_from(ts.tv_sec).ok()?.checked_mul(1_000_000_000)?.checked_add(u64::try_from(ts.tv_nsec).ok()?)
}

/// Each task id in /proc/self/task, given to `each`; `None` if the directory cannot be read. Raw
/// getdents64 into a stack buffer: no allocation.
#[cfg(target_os = "linux")]
fn linux_each_tid(mut each: impl FnMut(u64)) -> Option<()> {
    // SAFETY: a NUL-terminated path; the descriptor is closed below.
    let fd = unsafe { libc::open(b"/proc/self/task\0".as_ptr().cast(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) };
    if fd < 0 {
        return None;
    }
    let mut buf = [0u8; 4096];
    let mut ok = true;
    loop {
        // SAFETY: `buf` is writable for its length; the kernel fills whole records.
        let n = unsafe { libc::syscall(libc::SYS_getdents64, fd, buf.as_mut_ptr(), buf.len()) };
        if n < 0 {
            ok = false;
            break;
        }
        if n == 0 {
            break;
        }
        let n = n as usize;
        let mut at = 0usize;
        // linux_dirent64: d_ino u64, d_off i64, d_reclen u16, d_type u8, d_name NUL-terminated.
        while at + 19 < n {
            let reclen = usize::from(u16::from_ne_bytes([buf[at + 16], buf[at + 17]]));
            if reclen == 0 || at + reclen > n {
                ok = false;
                break;
            }
            let mut tid: u64 = 0;
            let mut digits = 0;
            for c in &buf[at + 19..at + reclen] {
                if *c == 0 {
                    break;
                }
                if !c.is_ascii_digit() {
                    digits = 0;
                    break;
                }
                tid = tid * 10 + u64::from(c - b'0');
                digits += 1;
            }
            if digits > 0 {
                each(tid);
            }
            at += reclen;
        }
        if !ok {
            break;
        }
    }
    // SAFETY: the descriptor opened above.
    unsafe { libc::close(fd) };
    ok.then_some(())
}

/// The threads, other than `skip` (a thread id), that ran between two `thread_table` readings:
/// `(id, name, CPU ns)`, a thread new in `b` counted with all its time. Allocates: call it outside
/// a measured window.
pub(crate) fn ran_between(a: &[ThreadCpu], b: &[ThreadCpu], skip: u64) -> Vec<(u64, String, u64)> {
    b.iter()
        .filter(|t| t.id != skip && t.id != 0)
        .filter_map(|t| {
            let before = a.iter().find(|x| x.id == t.id).map_or(0, |x| x.cpu_ns);
            (t.cpu_ns > before).then(|| (t.id, t.name(), t.cpu_ns - before))
        })
        .collect()
}

/// The threads this process has now, the live persistent store threads (`PERSISTENT_LIVE`) left out:
/// through [`each_thread`] on Apple (so it can be read inside a measured window), `/proc/self/stat`
/// on Linux; `None` elsewhere.
pub(crate) fn threads() -> Option<u64> {
    let mut n = 0u64;
    let raw = if each_thread(|_, _| n += 1).is_some() { Some(n) } else { linux_threads() }?;
    u64::try_from(raw as i64 - PERSISTENT_LIVE.load(Relaxed)).ok()
}

/// Whether every thread but the caller is blocked (see [`each_thread`]): the work a notify woke has
/// run and parked again. `None` where the thread states cannot be read (neither Apple nor Linux): a
/// window that needs it must refuse there, never assume it (review 2 M6).
pub(crate) fn others_blocked() -> Option<bool> {
    let mut all = true;
    each_thread(|me, blocked| all &= me || blocked)?;
    Some(all)
}

fn linux_threads() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        // Field 20 of /proc/self/stat, read into a stack buffer: no allocation, so it can be read
        // inside a window whose allocations are counted (its syscalls are not counted off Apple).
        let mut buf = [0u8; 1024];
        // SAFETY: a NUL-terminated path; the descriptor is closed below.
        let fd = unsafe { libc::open(b"/proc/self/stat\0".as_ptr().cast(), libc::O_RDONLY) };
        if fd < 0 {
            return None;
        }
        // SAFETY: `buf` is writable for its length.
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        // SAFETY: the descriptor opened above.
        unsafe { libc::close(fd) };
        let text = std::str::from_utf8(buf.get(..usize::try_from(n).ok()?)?).ok()?;
        // The command name (field 2) is parenthesised and may hold spaces: count after its ')'.
        let rest = &text[text.rfind(')')? + 1..];
        rest.split_ascii_whitespace().nth(17)?.parse().ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
