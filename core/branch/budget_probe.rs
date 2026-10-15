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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};

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
static MAX_BG_HOLD_CAT_ROWS: AtomicU64 = AtomicU64::new(0);
// The same over holds by the thread marked foreground (review 2 M7: the opener's own holds).
static MAX_FG_HOLD_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static MAX_FG_HOLD_CAT_ROWS: AtomicU64 = AtomicU64::new(0);
static LOCKS: AtomicU64 = AtomicU64::new(0);
static HELD_SYSCALLS: AtomicU64 = AtomicU64::new(0);
/// Holds whose syscalls could not be attributed to the holding thread: another thread ran during
/// the hold (`hold_attributed`); their syscalls are in neither `HELD_SYSCALLS` count.
static HELD_UNATTRIBUTED: AtomicU64 = AtomicU64::new(0);
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
    let trace = format!("free of {bytes} bytes:\n{}", std::backtrace::Backtrace::force_capture());
    T_TRACES.with(|t| t.borrow_mut().push(trace));
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
        let trace = format!("alloc #{n} of {bytes} bytes:\n{}", std::backtrace::Backtrace::force_capture());
        T_TRACES.with(|t| t.borrow_mut().push(trace));
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
    LOCKS.fetch_add(1, Relaxed);
    bump(&T_LOCKS, 1);
    let depth = DEPTH.with(|d| {
        d.set(d.get() + 1);
        d.get()
    });
    if depth == 1 {
        let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
        HOLD_START.with(|h| h.set((get(&T_HELD_ALLOC_BYTES), get(&T_CAT_ROWS))));
        if ARMED.load(Relaxed) {
            // The table first, then the count: the hold's counted syscalls lie inside the table's
            // window (`hold_attributed`).
            HOLD_T0.with(|t| {
                if let Ok(mut t) = t.try_borrow_mut() {
                    *t = [ThreadCpu::EMPTY; 48];
                    let _ = thread_table(&mut t[..]);
                }
            });
            HELD_SINCE.with(|s| s.set(unix_syscalls()));
        }
    }
}

/// This thread is letting go of a branch store's mutex (its guard's drop, before the unlock).
pub(crate) fn store_unlocked() {
    let depth = DEPTH.with(|d| {
        d.set(d.get().saturating_sub(1));
        d.get()
    });
    if depth == 0 {
        let get = |c: &'static std::thread::LocalKey<Cell<u64>>| c.with(|c| c.get());
        let (bytes0, rows0) = HOLD_START.with(|h| h.get());
        let (bytes, rows) = (get(&T_HELD_ALLOC_BYTES).wrapping_sub(bytes0), get(&T_CAT_ROWS).wrapping_sub(rows0));
        MAX_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
        MAX_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
        if FOREGROUND.with(|f| f.get()) {
            MAX_FG_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
            MAX_FG_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
        } else {
            MAX_BG_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
            MAX_BG_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
        }
        // Review 2 L7: the kernel count is read only when an armed hold sampled it at its lock.
        // Class (b): the hold's syscalls are this thread's only if no other thread ran during it
        // (`hold_attributed`); otherwise the hold is counted as unattributable, never as held.
        if let Some(since) = HELD_SINCE.with(|s| s.take()) {
            if let Some(now) = unix_syscalls() {
                let held = now.wrapping_sub(since) & u64::from(u32::MAX);
                let mut t1 = [ThreadCpu::EMPTY; 48];
                let attributed = thread_table(&mut t1).is_some()
                    && HOLD_T0.with(|t0| t0.try_borrow().map(|t0| foreign_threads(&t0[..], &t1, &[], true) == 0).unwrap_or(false));
                if attributed {
                    HELD_SYSCALLS.fetch_add(held, Relaxed);
                    bump(&T_HELD_SYSCALLS, held);
                } else {
                    HELD_UNATTRIBUTED.fetch_add(1, Relaxed);
                    bump(&T_HELD_UNATTRIBUTED, 1);
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
}

/// Every hold maximum at once, and start them all again from zero (review 2 L8: one call, so no
/// order of two calls can zero one maximum before it is read).
pub(crate) fn take_hold_maxima() -> HoldMaxima {
    HoldMaxima {
        all: (MAX_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_HOLD_CAT_ROWS.swap(0, Relaxed)),
        background: (MAX_BG_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_BG_HOLD_CAT_ROWS.swap(0, Relaxed)),
        foreground: (MAX_FG_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_FG_HOLD_CAT_ROWS.swap(0, Relaxed)),
    }
}

/// Mark the calling thread as the foreground (measuring) thread, or unmark it.
pub(crate) fn mark_foreground(on: bool) {
    FOREGROUND.with(|f| f.set(on));
}

/// Sample the syscall count around store-mutex holds from now on (off by default: two Mach traps
/// per hold would slow every other test).
pub(crate) fn arm(on: bool) {
    ARMED.store(on, Relaxed);
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
    /// Syncs issued by a thread while it held a store mutex (the engine's own
    /// `store::syncs_under_store_mutex`: attributed to the holder, so exact with other threads
    /// running, unlike `held_syscalls`).
    pub(crate) held_syncs: u64,
    pub(crate) fsync: u64,
    pub(crate) full_fsync: u64,
    pub(crate) barrier: u64,
    pub(crate) syscalls: Option<u64>,
}

/// Take a snapshot to open a window (see [`Snapshot`]): every thread's CPU time is read just
/// before the syscall count (class (b): `end_owning` attributes the window by exclusion).
pub(crate) fn begin() -> Snapshot {
    let instructions = instructions();
    let mut s = user_counts();
    s.instructions = instructions;
    WINDOW_T0.with(|t| {
        let mut t = t.borrow_mut();
        *t = [ThreadCpu::EMPTY; 48];
        let _ = thread_table(&mut t[..]);
    });
    s.syscalls = unix_syscalls();
    s
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
    let syscalls = unix_syscalls();
    let mut t1 = [ThreadCpu::EMPTY; 48];
    let listed = thread_table(&mut t1).is_some();
    let foreign = if listed {
        WINDOW_T0.with(|t0| foreign_threads(&t0.borrow()[..], &t1, owned, false))
    } else {
        1
    };
    let mut s = user_counts();
    s.foreign = foreign;
    s.syscalls = if foreign == 0 { syscalls } else { None };
    s.instructions = instructions();
    s
}

/// The threads that ran between two `thread_table` readings, other than the reading thread, the
/// declared spinners, and those named in `owned`; threads new in `b` count too when `new_foreign`
/// (a hold), and are the window's own otherwise (spawned by the operation). Allocates nothing.
fn foreign_threads(a: &[ThreadCpu], b: &[ThreadCpu], owned: &[&str], new_foreign: bool) -> u64 {
    let me = b.iter().find(|t| t.me).map_or(0, |t| t.id);
    let spinning = |id: u64| SPINNERS.iter().any(|s| s.load(Relaxed) == id);
    let owned_name = |t: &ThreadCpu| {
        let end = t.name.iter().position(|c| *c == 0).unwrap_or(t.name.len());
        owned.iter().any(|o| o.as_bytes() == &t.name[..end])
    };
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
    let syncs = crate::branch::sync_counts();
    Snapshot {
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
        held_syncs: crate::branch::store::syncs_under_store_mutex(),
        fsync: syncs.fsync,
        full_fsync: syncs.full_fsync,
        barrier: syncs.barrier,
        syscalls: None,
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
    #[cfg(not(target_vendor = "apple"))]
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
}

impl ThreadCpu {
    pub(crate) const EMPTY: ThreadCpu = ThreadCpu { id: 0, cpu_ns: 0, me: false, name: [0; 32] };

    pub(crate) fn name(&self) -> String {
        let end = self.name.iter().position(|b| *b == 0).unwrap_or(self.name.len());
        String::from_utf8_lossy(&self.name[..end]).into_owned()
    }
}

/// Every thread of this process now, into `out` (the first `out.len()`), and how many there are;
/// `None` off Apple. Mach traps and userspace only, as `each_thread`, with no allocation, so it can
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
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = out;
        None
    }
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
/// run and parked again. `None` off Apple: a window that needs it must refuse there, never assume it
/// (review 2 M6).
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
