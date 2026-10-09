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
static LOCKS: AtomicU64 = AtomicU64::new(0);
static HELD_SYSCALLS: AtomicU64 = AtomicU64::new(0);
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
    /// Store-mutex guards this thread holds now.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// The task's syscall count when this thread took its first store mutex (while armed).
    static HELD_SINCE: Cell<Option<u64>> = const { Cell::new(None) };
}

fn bump(cell: &'static std::thread::LocalKey<Cell<u64>>, by: u64) {
    let _ = cell.try_with(|c| c.set(c.get().wrapping_add(by)));
}

fn note_alloc(bytes: usize) {
    ALLOCS.fetch_add(1, Relaxed);
    ALLOC_BYTES.fetch_add(bytes as u64, Relaxed);
    bump(&T_ALLOCS, 1);
    bump(&T_ALLOC_BYTES, bytes as u64);
    if DEPTH.try_with(|d| d.get()).unwrap_or(0) > 0 {
        bump(&T_HELD_ALLOCS, 1);
        bump(&T_HELD_ALLOC_BYTES, bytes as u64);
    }
    if wal_held() {
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
// with what this thread did while it held one. Six `cfg(test)` hook lines in the engine's files.
// BLIND SPOTS: a page read without the pager's `read_page` (`read_page_no_cache`) is not counted;
// the lock is ANY database's WAL write lock, the trunk's or the branch catalog's own (a separate
// Turso database); a thread holding two at once reads as holding one until either is released.
thread_local! {
    static T_PREPARES: Cell<u64> = const { Cell::new(0) };
    static T_PAGE_READS: Cell<u64> = const { Cell::new(0) };
    static T_SCHEMA_ROWS: Cell<u64> = const { Cell::new(0) };
    static WAL_HELD: Cell<bool> = const { Cell::new(false) };
    static T_WAL_LOCKS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_PREPARES: Cell<u64> = const { Cell::new(0) };
    static T_WAL_PAGE_READS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_SCHEMA_ROWS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_ALLOCS: Cell<u64> = const { Cell::new(0) };
    static T_WAL_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn wal_held() -> bool {
    WAL_HELD.try_with(|h| h.get()).unwrap_or(false)
}

/// A statement was prepared on this thread.
pub(crate) fn statement_prepared() {
    bump(&T_PREPARES, 1);
    if wal_held() {
        bump(&T_WAL_PREPARES, 1);
    }
}

/// A page was read through the pager on this thread.
pub(crate) fn page_read() {
    bump(&T_PAGE_READS, 1);
    if wal_held() {
        bump(&T_WAL_PAGE_READS, 1);
    }
}

/// A `sqlite_schema` row was parsed into a schema on this thread.
pub(crate) fn schema_row_parsed() {
    bump(&T_SCHEMA_ROWS, 1);
    if wal_held() {
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
    pub(crate) allocs: u64,
    pub(crate) alloc_bytes: u64,
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
        allocs: get(&T_ALLOCS),
        alloc_bytes: get(&T_ALLOC_BYTES),
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

/// Whether this thread holds a trunk's WAL write lock now (the fire-check's).
pub(crate) fn wal_write_held() -> bool {
    wal_held()
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
        if !FOREGROUND.with(|f| f.get()) {
            MAX_BG_HOLD_ALLOC_BYTES.fetch_max(bytes, Relaxed);
            MAX_BG_HOLD_CAT_ROWS.fetch_max(rows, Relaxed);
        }
        if let (Some(since), Some(now)) = (HELD_SINCE.with(|s| s.take()), unix_syscalls()) {
            let held = now.wrapping_sub(since) & u64::from(u32::MAX);
            HELD_SYSCALLS.fetch_add(held, Relaxed);
            bump(&T_HELD_SYSCALLS, held);
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
/// call, by any thread, and start again from zero.
pub(crate) fn take_hold_maxima() -> (u64, u64) {
    let _ = take_background_hold_maxima();
    (MAX_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_HOLD_CAT_ROWS.swap(0, Relaxed))
}

/// The same over holds by threads not marked foreground, and start again from zero. Reset by
/// `take_hold_maxima` too.
pub(crate) fn take_background_hold_maxima() -> (u64, u64) {
    (MAX_BG_HOLD_ALLOC_BYTES.swap(0, Relaxed), MAX_BG_HOLD_CAT_ROWS.swap(0, Relaxed))
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
    /// Syncs issued by a thread while it held a store mutex (the engine's own
    /// `store::syncs_under_store_mutex`: attributed to the holder, so exact with other threads
    /// running, unlike `held_syscalls`).
    pub(crate) held_syncs: u64,
    pub(crate) fsync: u64,
    pub(crate) full_fsync: u64,
    pub(crate) barrier: u64,
    pub(crate) syscalls: Option<u64>,
}

/// Take a snapshot to open a window (see [`Snapshot`]).
pub(crate) fn begin() -> Snapshot {
    let instructions = instructions();
    let mut s = user_counts();
    s.instructions = instructions;
    s.syscalls = unix_syscalls();
    s
}

/// Take a snapshot to close a window: the syscall count first, instructions last.
pub(crate) fn end() -> Snapshot {
    let syscalls = unix_syscalls();
    let mut s = user_counts();
    s.syscalls = syscalls;
    s.instructions = instructions();
    s
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
