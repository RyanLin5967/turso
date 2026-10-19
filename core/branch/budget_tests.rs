//! fastest-budgets lane (artie-research frontier/fastest, DECISIONS.md 2026-10-04T03:55Z and the
//! T3-READY gate item 2): the PERFORMANCE-BUDGET suite. Slowness is a failing test: for each
//! operation a server branch makes — create, connect by name, first write, disconnect, delete,
//! recovery — what it issues is counted with integer counters (never wall time) and held to a
//! budget, and the counts must not grow with the live branches (10 against 10^4) or with the
//! database's size (a few pages against 32 MiB).
//!
//! Counters (`budget_probe` and the engine's own): unix syscalls (the kernel's count), fsync(2),
//! F_FULLFSYNC and F_BARRIERFSYNC (`io::SYNC_COUNTS`), allocations and their bytes (a counting
//! global allocator), store-mutex acquisitions and the syscalls and allocations made while it is
//! held, catalog queries, rows and branch loads (the catalog's counters), branch page resolves,
//! branch-log bytes (the log file's length) and instructions retired.
//!
//! Every count is measured in a CHILD process (this test binary, running only `budget_child`), so
//! the process-wide counters see that cell's work and nothing else. Before each operation the child
//! waits out every background thread; the operation's window closes only once every thread it
//! started has exited, so a checkpoint an operation starts is charged to it. A sample in which some
//! other thread ran is marked not quiet and is not used. Each cell (live branches, size, named or
//! unnamed population) runs once per test process, and its samples are shared by every test.
//!
//! Configuration: a catalog store (the credited one), D2 (`SyncClass::FullFsync`), fuzzy
//! checkpoints (the default), one client (C=1), named server branches, the default checkpoint
//! threshold except in the checkpoint arm. `FE_BUDGET_K` (default 24) operations of each kind are
//! measured per cell, after `WARMUP` unmeasured rounds. `FE_BUDGET_RAW_DIR`, when set, receives
//! every child's raw sample file.
//!
//! Budget values come from the design (DESIGN.md §2-§3), the PREREG, and the reviews' accepted
//! fixes, each named beside its test; none comes from a measurement of the engine. The notes
//! (frontier/fastest/lanes/budgets/BUDGETS.md) hold the table, the values measured, and which slow
//! mutant (lanes/budgets/mutants.py) each budget was shown to catch.

use super::budget_probe as probe;
use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Rows of `big` in the large database: 32,768 rows of a 1,000-byte blob, about 32 MiB.
const BIG_ROWS: u64 = 32_768;
/// Named creates left in the log after the population's checkpoint, so every recovery replays the
/// same tail.
const TAIL: u64 = 16;
/// Unmeasured rounds before the measured ones.
const WARMUP: u64 = 4;
/// The live branches of the large-N cells.
const N_LARGE: u64 = 10_000;
/// The growth arm creates this many more branches from `N_LARGE`, so the live branches pass 2^15
/// and every per-branch table sized for 10^4 doubles at least once.
const GROWTH: u64 = 23_040;
/// The checkpoint arm's threshold (the default is 1 MiB) and its rounds: about 130 log bytes a
/// round, so a checkpoint every ~60 rounds.
const CKPT_THRESHOLD: u64 = 8 << 10;
/// FLAGGED FIXTURE CHANGE, lead-ruled (class (b) acceptance, DECISIONS 2026-10-15): 240 rounds made
/// about 3 checkpoints, and with attribution by thread fewer than 3 of them were attributable, so
/// the checkpoint cells refused. 960 rounds make about 12. The claims and thresholds are unchanged;
/// `checkpoint_excess` still refuses below 3 attributable installs and reports both counts, and
/// `a_checkpoint_flushes_at_most_three_times_over_every_round` bounds the unselected total.
const CKPT_ROUNDS: u64 = 960;
/// The shared-flight arm's client counts (lead, 2026-10-06: "creates per F_FULLFSYNC rising with C
/// at C = 1, 8, 64, 256, 1024").
const SHARED_CS: [u64; 5] = [1, 8, 64, 256, 1024];
// "Hold the confirmation word back": a quiet period no run reaches (2^40 ms, about 34.8 years).
use super::store::CONFIRM_HELD_MS;
/// The memory cells' live branches: 10^4 (`N_LARGE`) and this.
const N_HUGE: u64 = 100_000;
/// Every measured branch name is this long (`m-0000`), so every create logs the same bytes.
const NAME_LEN: u64 = 6;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// D2, except in a `*_d0` cell (the instruction arm: no device flush, whose kernel work makes an
/// operation's instruction count vary by ~15% from one process to the next) and a `*_d1` cell (D1,
/// plain fsync). A catalog store, except in the open-flush cells `open_d*` and their writer
/// `open_writer_*`, which use the snapshot store (`Durable`): its open of a whole log takes the
/// kept-log arm the open-flush budgets are about (see `open_flush`).
fn opts() -> DatabaseOpts {
    let spec = std::env::var("FE_BUDGET_CHILD").unwrap_or_default();
    let sync = if spec.ends_with("_d0") {
        SyncClass::Off
    } else if spec.ends_with("_d1") {
        SyncClass::Fsync
    } else {
        SyncClass::FullFsync
    };
    let durability = if spec.starts_with("open_d") || spec.starts_with("open_writer") {
        BranchDurability::Durable { sync }
    } else {
        BranchDurability::Catalog { sync }
    };
    // The plant `exc_sharp` (DECISIONS bd0d845f97): a sharp catalog store, whose compactions sync
    // inline on the operation and are counted as an exception.
    let checkpoint = if spec == "exc_sharp" { BranchCheckpoint::Sharp } else { BranchCheckpoint::Fuzzy };
    DatabaseOpts::new()
        .with_branch_durability(durability)
        .with_branch_checkpoint(checkpoint)
}

fn open_at(path: &Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn query_int(conn: &Arc<Connection>, sql: &str) -> i64 {
    conn.prepare(sql)
        .and_then(|mut s| s.run_collect_rows())
        .unwrap_or_else(|e| panic!("{sql}: {e}"))[0][0]
        .as_int()
        .unwrap()
}

/// The same schema in every cell (so what parses it does the same work): `t`, 50 short rows on one
/// page, which every first write updates; `big`, empty in the small database and `BIG_ROWS` rows
/// in the large one. The trunk's WAL is then checkpointed into the database file and emptied, so
/// every cell forks from an empty WAL.
fn seed(conn: &Arc<Connection>, large: bool) {
    exec(conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    exec(conn, "CREATE TABLE big(id INTEGER PRIMARY KEY, b BLOB)");
    exec(conn, "BEGIN");
    for id in 1..=50 {
        exec(conn, &format!("INSERT INTO t VALUES ({id}, 'trunk-{id}')"));
    }
    if large {
        for chunk in 0..BIG_ROWS / 256 {
            let rows: Vec<String> =
                (1..=256).map(|j| format!("({}, zeroblob(1000))", chunk * 256 + j)).collect();
            exec(conn, &format!("INSERT INTO big VALUES {}", rows.join(", ")));
        }
    }
    exec(conn, "COMMIT");
    let _ = conn
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .and_then(|mut s| s.run_collect_rows())
        .unwrap();
    let pages = query_int(conn, "PRAGMA page_count");
    if large {
        assert!(pages >= 8_000, "premise: the large database spans about 32 MiB ({pages} pages)");
    } else {
        assert!(pages <= 8, "premise: the small database is a few pages ({pages} pages)");
    }
}

/// One fork, named `name` or unnamed (detached). A create refused Busy or SchemaUpdated is retried,
/// at most `FORK_RETRY_CAP` times (review 2 L9: an unbounded loop turned a livelock into an 1800 s
/// child timeout); the retries are returned, for the callers that count them.
fn fork_one(c: &Arc<Connection>, name: Option<&str>) -> u64 {
    for retries in 0..=FORK_RETRY_CAP {
        let made = match name {
            Some(name) => c.create_branch(name).map(|_| ()),
            None => c.fork_branch().map(|b| {
                b.into_id();
            }),
        };
        match made {
            Ok(()) => return retries,
            Err(LimboError::Busy) | Err(LimboError::SchemaUpdated) => continue,
            Err(e) => panic!("branch {name:?}: {e}"),
        }
    }
    panic!("branch {name:?}: refused {FORK_RETRY_CAP} times in a row (a livelock)")
}

/// The most retries `fork_one` makes before it refuses.
const FORK_RETRY_CAP: u64 = 1000;

/// `n` live branches of the trunk, `<prefix>-<i>` when named, made by up to 32 threads (their
/// flights shared). The trunk's first child is made first, alone: it takes the WAL write lock.
fn populate(db: &Arc<Database>, prefix: &str, from: u64, n: u64, named: bool) {
    let name = |i: u64| named.then(|| format!("{prefix}-{i}"));
    if from == 0 {
        fork_one(&db.connect().unwrap(), name(0).as_deref());
    }
    let start = from.max(1);
    // The D0 cells (instructions) populate on one thread: a 32-thread population leaves the
    // allocator's per-CPU state such that a later connection's drop retires the same 71 blocks in
    // ~72k more instructions (probe at 3befe5c31: trunk disconnect 203.7k at N=10 by 10 threads,
    // 271.8k at N=10^4 by 32 threads; 180.7k and 180.2k both serial), a fixture effect that read as
    // an O(N) cost.
    let serial = std::env::var("FE_BUDGET_CHILD").is_ok_and(|s| s.ends_with("_d0"));
    let threads = if serial { 1 } else { (from + n).saturating_sub(start).clamp(1, 32) };
    std::thread::scope(|s| {
        for t in 0..threads {
            s.spawn(move || {
                let c = db.connect().unwrap();
                let mut i = start + t;
                while i < from + n {
                    fork_one(&c, name(i).as_deref());
                    i += threads;
                }
            });
        }
    });
}

fn log_len(db: &Arc<Database>) -> Option<u64> {
    db.branch_log_path().and_then(|p| std::fs::metadata(p).ok()).map(|m| m.len())
}

/// Every counter one measured operation moved, by name. A count the platform cannot give is absent.
type Sample = BTreeMap<&'static str, u64>;

fn delta(a: u64, b: u64) -> u64 {
    b.wrapping_sub(a)
}

fn kernel_delta(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    Some(b?.wrapping_sub(a?) & u64::from(u32::MAX))
}

/// The engine-side counters read outside a window (each takes the store mutex, or a syscall).
struct Outside {
    log: Option<u64>,
    cat: (u64, u64, u64, u64),
    ckpt: [u64; 9],
    reads: (u64, u64),
    /// The counted exceptions (`exceptions`); `None` where no database is read (the instruments).
    exc: Option<[u64; 4]>,
}

impl Outside {
    fn read(db: &Arc<Database>) -> Self {
        Self {
            log: log_len(db),
            cat: db.branch_catalog_counters(),
            ckpt: db.branch_checkpoint_counters(),
            reads: db.branch_read_counters(),
            exc: Some(exceptions(db)),
        }
    }

    /// A database's counters at its open, all 0 (they count since the open).
    fn zero(log: Option<u64>) -> Self {
        Self {
            log,
            cat: (0, 0, 0, 0),
            ckpt: [0; 9],
            reads: (0, 0),
            exc: Some([0; 4]),
        }
    }

    /// No database: the instruments' windows, which record no counted exception.
    fn none() -> Self {
        Self {
            exc: None,
            ..Self::zero(None)
        }
    }
}

/// The counted exceptions (DECISIONS bd0d845f97; `Database::branch_exception_counters`, keys
/// `EXC_KEYS`): read outside a window, as it takes the group's lock.
fn exceptions(db: &Arc<Database>) -> [u64; 4] {
    db.branch_exception_counters()
}

/// Record the counted exceptions' deltas from `a` to `b` in `s` (every key, a zero included).
fn put_exceptions(s: &mut Sample, a: [u64; 4], b: [u64; 4]) {
    for (key, (x, y)) in EXC_KEYS.into_iter().zip(a.into_iter().zip(b)) {
        s.insert(key, delta(x, y));
    }
}

fn sample_of(s0: &probe::Snapshot, s1: &probe::Snapshot, o0: &Outside, o1: &Outside) -> Sample {
    let mut s = Sample::new();
    // Class (b): the window's OWN syscalls (`probe::end_owning`'s attribution: the kernel's count
    // with no other thread running, or the per-thread counter's where it covered the window), or
    // no count at all; the task's raw delta is kept apart for the fire-checks.
    if let Some(d) = s1.attributed_syscalls {
        s.insert("syscalls", d);
    }
    if let Some(d) = kernel_delta(s0.syscalls, s1.syscalls) {
        s.insert("syscalls_task", d);
    }
    if let (Some(a), Some(b)) = (s0.instructions, s1.instructions) {
        s.insert("instructions", delta(a, b));
    }
    s.insert("fsync", delta(s0.fsync, s1.fsync));
    s.insert("full_fsync", delta(s0.full_fsync, s1.full_fsync));
    s.insert("barrier", delta(s0.barrier, s1.barrier));
    // Counters review MED-5: the process's syncs in the same window, for the parity check.
    s.insert("fsync_process", delta(s0.p_fsync, s1.p_fsync));
    s.insert("full_fsync_process", delta(s0.p_full_fsync, s1.p_full_fsync));
    s.insert("barrier_process", delta(s0.p_barrier, s1.p_barrier));
    s.insert("fsync_own", delta(s0.o_fsync, s1.o_fsync));
    s.insert("full_fsync_own", delta(s0.o_full_fsync, s1.o_full_fsync));
    s.insert("barrier_own", delta(s0.o_barrier, s1.o_barrier));
    s.insert("allocs", delta(s0.t_allocs, s1.t_allocs));
    s.insert("alloc_bytes", delta(s0.t_alloc_bytes, s1.t_alloc_bytes));
    s.insert("allocs_process", delta(s0.allocs, s1.allocs));
    // Reported, not budgeted: frees land wherever a value's last owner drops it.
    s.insert("frees", delta(s0.t_frees, s1.t_frees));
    s.insert("free_bytes", delta(s0.t_free_bytes, s1.t_free_bytes));
    s.insert("frees_process", delta(s0.frees, s1.frees));
    s.insert("held_allocs", delta(s0.t_held_allocs, s1.t_held_allocs));
    s.insert("held_alloc_bytes", delta(s0.t_held_alloc_bytes, s1.t_held_alloc_bytes));
    s.insert("locks", delta(s0.t_locks, s1.t_locks));
    s.insert("locks_process", delta(s0.locks, s1.locks));
    s.insert("held_syscalls", delta(s0.held_syscalls, s1.held_syscalls));
    // Class (b): the threads that made the window unattributable, and the holds inside it that
    // could not be attributed to their holder (`probe::end_owning`, `store_unlocked`).
    s.insert("foreign_threads", s1.foreign);
    s.insert("threads_quiet", u64::from(s1.threads_quiet));
    // Probe-adversary item 1: every sample records the probe's mode and the shim's lost count.
    s.insert("shim_loaded", u64::from(s1.shim_loaded));
    // probe-adversary-2 L4: why a count was refused (`probe::Refusal`, 0 when it was not).
    s.insert("refusal", s1.refusal as u64);
    s.insert("shim_lost", s1.shim_lost);
    s.insert("held_unattributed", delta(s0.held_unattributed, s1.held_unattributed));
    s.insert("held_syncs", delta(s0.held_syncs, s1.held_syncs));
    s.insert("catalog_loads", delta(o0.cat.0, o1.cat.0));
    s.insert("catalog_queries", delta(o0.cat.2, o1.cat.2));
    s.insert("catalog_rows", delta(o0.cat.3, o1.cat.3));
    s.insert("catalog_stmts", delta(s0.t_cat_stmts, s1.t_cat_stmts));
    s.insert("catalog_writes", delta(s0.t_cat_writes, s1.t_cat_writes));
    s.insert("catalog_stmts_process", delta(s0.cat_stmts, s1.cat_stmts));
    s.insert("catalog_writes_process", delta(s0.cat_writes, s1.cat_writes));
    s.insert("catalog_rows_touched", delta(s0.t_cat_rows, s1.t_cat_rows));
    s.insert("catalog_rows_touched_process", delta(s0.cat_rows, s1.cat_rows));
    s.insert("resolves", delta(o0.reads.0, o1.reads.0));
    s.insert("slot_reads", delta(o0.reads.1, o1.reads.1));
    s.insert("ckpt_started", delta(o0.ckpt[1], o1.ckpt[1]));
    s.insert("ckpt_installed", delta(o0.ckpt[0], o1.ckpt[0]));
    if let (Some(a), Some(b)) = (o0.log, o1.log) {
        if b >= a {
            s.insert("log_bytes", b - a);
        }
    }
    if let (Some(a), Some(b)) = (o0.exc, o1.exc) {
        put_exceptions(&mut s, a, b);
    }
    s
}

/// Settled: the thread count (persistent store threads left out) is back at `base`, and every
/// other thread is blocked, so what the work woke (the confirmation writer, at a flight's landing)
/// has run and parked again. Mach traps and userspace only.
fn settled(base: Option<u64>) -> bool {
    // Review 2 M6: where the thread state cannot be read, a window cannot be shown to have closed;
    // refuse, never fall through to "settled".
    let threads = probe::threads().unwrap_or_else(|| panic!("budget window: the thread count cannot be read on this platform; refusing"));
    let blocked = probe::others_blocked().unwrap_or_else(|| panic!("budget window: whether the other threads are blocked cannot be read on this platform; refusing"));
    assert!(base.is_some(), "budget window: no base thread count; refusing");
    Some(threads) == base && blocked
}

/// Spin, with no syscall, until `settled(base)` or the deadline; whether it settled.
fn spin_settled(base: Option<u64>, deadline: std::time::Duration) -> bool {
    // Mutant `harness_box_in_spin_settled` (test builds only; counters review 4 MED-1): the harness's
    // own wait allocates inside the window it waits in.
    if probe::mutant_on("harness_box_in_spin_settled") {
        std::hint::black_box(Box::new(0u8)); // lint-allow: mutant harness_box_in_spin_settled
    }
    let t = std::time::Instant::now();
    while !settled(base) {
        if t.elapsed() > deadline {
            return false;
        }
        std::hint::spin_loop();
    }
    true
}

/// Wait out every background thread before a window: the checkpoint threads joined, then settled
/// (a joined thread can still be exiting), and the confirmation writer parked with nothing to do
/// (`writer_settled`). `false` if it never settled.
fn quiesce(db: &Arc<Database>, base: Option<u64>) -> bool {
    db.branch_checkpoint_wait();
    let t = std::time::Instant::now();
    loop {
        // The writer first, then the thread state (`writer_settled`'s order: the thread state is what
        // sees the writer's final step into its kernel wait).
        if writer_settled(db, std::time::Duration::ZERO) && settled(base) {
            return true;
        }
        if t.elapsed() > std::time::Duration::from_secs(5) {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Run `f` as one measured operation, charged with the background work it starts: the window
/// closes once every thread it left running (a checkpoint's) has exited, waited for by spinning on
/// the thread count (Mach traps only on Apple, so the wait adds no syscall). `background` records
/// whether it left one; `quiet`, that nothing else ran in the window, so the process-wide counts
/// are its own.
fn measure<R>(db: &Arc<Database>, base: Option<u64>, f: impl FnOnce() -> R) -> (R, Sample) {
    measure_owning(db, base, &[], f)
}

/// `measure`, charging the window with the work of the store threads named in `owned` (a
/// persistent worker the operation dispatches, or the confirmation writer whose own cost a cell
/// measures). Class (b), DECISIONS 994f7d41e0: the window's syscalls are the measuring thread's,
/// its spawns' and its owned threads' only if no other thread ran inside it
/// (`probe::end_owning`); a window where one did is not quiet, and `quiet` refuses a cell where
/// fewer than half its samples are. The harness's own waits inside the window are declared
/// syscall-free (`probe::spinning`), as `fc_writer_settled_cost` and `fc_threads_1000` check.
fn measure_owning<R>(db: &Arc<Database>, base: Option<u64>, owned: &[&str], f: impl FnOnce() -> R) -> (R, Sample) {
    let settled = quiesce(db, base);
    let o0 = Outside::read(db);
    let c0 = db.branch_confirm_counts();
    let s0 = probe::begin();
    let r = f();
    let left = probe::threads() != base;
    let ((synced, parked), back) = probe::spinning(|| {
        let absorbed = absorb_confirm(db, &s0);
        (absorbed, spin_settled(base, std::time::Duration::from_secs(30)))
    });
    // Review 2 L3: the confirmation count is read inside the window, before its end snapshot.
    let c1 = db.branch_confirm_counts();
    let s1 = probe::end_owning(owned);
    let o1 = Outside::read(db);
    let mut s = sample_of(&s0, &s1, &o0, &o1);
    let confirms = delta(c0[0], c1[0]);
    s.insert("confirms_written", confirms);
    // Review 2 L10: a failed confirmation write is a fault, never a non-quiet sample to drop.
    assert_eq!(c1[1], c0[1], "a confirmation word failed to write inside a measured window");
    // A window whose work synced a flight and started no checkpoint (which may take the word
    // itself) must have absorbed the word's write.
    let absorbed = !synced || left || confirms == 1 || WORD_HELD.load(std::sync::atomic::Ordering::Acquire);
    // The Linux port (2026-10-18): the window is the caller's own by `Snapshot::own` (its attribution
    // where a kernel count exists, the thread rule where none does).
    let own = s1.own();
    let attributed = own && s1.held_unattributed == s0.held_unattributed;
    let quiet = settled && back && parked && absorbed && attributed && (left || s["allocs_process"] == s["allocs"]);

    s.insert("writer_parked", u64::from(parked));
    s.insert("background", u64::from(left));
    s.insert("quiet", u64::from(quiet));
    (r, s)
}

/// Whether the confirmation word is held back now (`hold_word`).
static WORD_HELD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Hold the flight confirmation word back (review 6 #1; the engine's test knob), or let the store's
/// confirmation thread write it after `confirm_quiet()` (5 ms) of idle, the default. Held, the writer
/// is woken by the first landing only and then sleeps with the word pending, so no background
/// thread runs in any later window and every count is the operation's own: each landing replaces
/// the pending word and closes its descriptor on the landing thread. That is NOT the engine under
/// load (review 2 H1): with the shipped quiet period the writer also wakes on the group's condition
/// variable at every landing while flights overlap, and those wake-ups never happen in a held cell.
/// Held is every cell's regime but `confirm_n10`'s (the idle tail's write against it) and
/// `confirm_load`'s (the writer under load, shipped quiet period).
fn hold_word(held: bool) {
    use std::sync::atomic::Ordering::Release;
    super::store::CONFIRM_QUIET_MS.store(if held { CONFIRM_HELD_MS } else { 0 }, Release);
    WORD_HELD.store(held, Release);
}

/// The confirmation word (review 6 #1): after a flight whose sync proved stable storage, the
/// store's confirmation thread writes the word once the group has been idle `confirm_quiet()`
/// (5 ms), on its own thread. Wait until that writer is parked with nothing left to do
/// (`writer_settled`, within 1 s, with no syscall), so the window that caused the write counts it,
/// as it counted the inline write before review 6 #1 (base10). Review 2 L3: this replaces a fixed
/// 25 ms spin, and a thread state that read "waiting" mid-pwrite. Whether the work since `s0`
/// synced, and whether the writer settled.
fn absorb_confirm(db: &Arc<Database>, s0: &probe::Snapshot) -> (bool, bool) {
    let now = probe::window_syncs();
    let synced = now.full_fsync != s0.full_fsync || now.fsync != s0.fsync;
    (synced, writer_settled(db, std::time::Duration::from_secs(1)))
}

/// Review 2 L3, review C14: spin, never blocking (the park state is read with a try-lock), until
/// `db`'s confirmation writer is parked with nothing left to do, or the deadline; whether it got
/// there. Parked with nothing left to do: in its idle wait or its quiet-period wait (never its
/// flight wait: a flight or a cut is in the air, or its landing's wake is still to be taken),
/// entered after every notification it was sent (review C14: a held create's landing notifies a
/// parked writer, and until it wakes its old wait still reads as parked), and, unheld, with no proved
/// word pending (the writer writes it after its quiet period). Held (`hold_word`), a pending word is
/// expected: it waits out a quiet period no run reaches. With no persistent thread live in the
/// process (`probe::persistent_live`), there is no writer to wait for. The final step into the
/// kernel wait is not seen here (the accessor's blind spot); every caller spins on the thread state
/// afterwards (`spin_settled`, all other threads blocked), which sees it on Apple. BLIND SPOT: with
/// another store's writer live and none of `db`'s own, this waits out its deadline and the window
/// reads not quiet (a refusal, not a false pass).
fn writer_settled(db: &Arc<Database>, deadline: std::time::Duration) -> bool {
    use super::store::{CONFIRM_PARK_IDLE, CONFIRM_PARK_QUIET};
    if probe::persistent_live() <= 0 {
        return true;
    }
    let held = WORD_HELD.load(std::sync::atomic::Ordering::Acquire);
    let t = std::time::Instant::now();
    loop {
        if let Some((wait, seen, pending, _)) = db.branches.confirm_park_for_test() {
            if (wait == CONFIRM_PARK_IDLE || wait == CONFIRM_PARK_QUIET) && seen && (held || !pending) {
                return true;
            }
        }
        if t.elapsed() >= deadline {
            return false;
        }
        std::hint::spin_loop();
    }
}

fn line(out: &mut String, cell: &str, op: &str, i: u64, s: &Sample) {
    let _ = write!(out, "sample cell={cell} op={op} i={i}");
    for (k, v) in s {
        let _ = write!(out, " {k}={v}");
    }
    out.push('\n');
}

/// A built database on disk, closed.
struct Built {
    _dir: tempfile::TempDir,
    path: PathBuf,
    log: PathBuf,
    incarnation: u64,
}

/// Build a cell's database: seeded, `n` live branches (`pop-<i>` when named), checkpointed, then
/// `TAIL` named creates left in the log; closed when this returns.
fn build(cell: &str, n: u64, large: bool, named: bool) -> Built {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    let db = open_at(&path);
    let trunk = db.connect().unwrap();
    seed(&trunk, large);
    populate(&db, "pop", 0, n, named);
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    for i in 0..TAIL {
        trunk.create_branch(&format!("tail-{i}")).unwrap();
    }
    let live = db.branch_ids().unwrap().len() as u64;
    assert_eq!(live, n + TAIL, "{cell}: premise: the live branches");
    let (incarnation, log) = (db.incarnation, db.branch_log_path().unwrap());
    Built {
        _dir: dir,
        path,
        log,
        incarnation,
    }
}

/// Recovery, `opens` times: each open, with what it starts in the background (the name filter's
/// build, any checkpoint) waited out. The catalog's counters start at the open, so what they read
/// after it is the open's. Returns the last open, its filter built, and the thread count at rest.
fn recover(cell: &str, built: &mut Built, opens: u64, out: &mut String) -> (Arc<Database>, Option<u64>) {
    let base = probe::threads();
    let mut db = None;
    for r in 0..opens {
        drop(db.take());
        let log0 = std::fs::metadata(&built.log).ok().map(|m| m.len());
        let t0 = probe::threads();
        let s0 = probe::begin();
        let opened = open_at(&built.path);
        let s_open = probe::end();
        let t_open = probe::threads();
        assert_ne!(
            opened.incarnation, built.incarnation,
            "{cell}: the registry returned the old Database: not a reopen"
        );
        built.incarnation = opened.incarnation;
        // The background threads are waited out by spinning (no syscall), not by joining them: a
        // join that blocks issues syscalls of its own, as many as the timing makes.
        let settled = probe::spinning(|| {
            let (_, parked) = absorb_confirm(&opened, &s0);
            parked && spin_settled(base, std::time::Duration::from_secs(30))
        });
        let s_all = probe::end();
        opened.branch_wait_name_filter();
        let settled = settled && quiesce(&opened, base);
        let o1 = Outside::read(&opened);
        let mut a = sample_of(&s0, &s_open, &Outside::zero(log0), &o1);
        // Class (b): a recovery window is the opening thread's and the threads the open spawned;
        // another thread that ran inside it makes it unattributable (`probe::end`).
        a.insert("quiet", u64::from(t0 == base && t_open == base && s_open.own()));
        line(out, cell, "recovery_open", r, &a);
        let mut b = sample_of(&s0, &s_all, &Outside::zero(log0), &o1);
        b.insert("quiet", u64::from(t0 == base && settled && s_all.own()));
        line(out, cell, "recovery", r, &b);
        db = Some(opened);
    }
    (db.unwrap(), base)
}

/// Lead order (2026-10-15, attribution item 1): `create_allocates_at_most_once_under_the_store_mutex`
/// and `create_is_independent_of_live_branches` flip between runs (n1e4_small's steady create
/// allocates 2 when red, 1 when green). The allocation counters are this thread's own
/// (`t_allocs`), so the extra allocation is the creating thread's. This diagnostic builds the
/// n1e4_small cell four times in one process (each build its own hash keys), reopens it as the
/// budget does, and runs the budget's rounds' create and delete, recording each create's
/// allocations on this thread with a backtrace of every one past the first
/// (`budget_probe::record_allocs`). Printed only: the per-build counts, and each distinct
/// backtrace of an extra allocation with how often it was seen.
#[test]
#[ignore = "diagnostic (lead attribution order): run by name with --ignored --nocapture"]
fn diag_n1e4_create_allocations_by_site() {
    for build_no in 0..4 {
        let mut built = build("diag_n1e4_small", 10_000, false, true);
        let mut out = String::new();
        let (db, _base) = recover("diag_n1e4_small", &mut built, 1, &mut out);
        let trunk = db.connect().unwrap();
        let mut counts: std::collections::BTreeMap<usize, u32> = Default::default();
        let mut sites: std::collections::BTreeMap<String, u32> = Default::default();
        for i in 0..40u64 {
            let name = format!("m-{i:04}");
            probe::record_allocs(true, 1);
            trunk.create_branch(&name).unwrap();
            let (n, sizes, traces) = probe::recorded_allocs();
            probe::record_allocs(false, usize::MAX);
            *counts.entry(n).or_default() += 1;
            for t in traces {
                // The frames inside this crate, the allocator's own left out: the site.
                let frames: Vec<&str> = t
                    .lines()
                    .filter(|l| l.contains("turso_core::") && !l.contains("budget_probe"))
                    .take(6)
                    .collect();
                *sites.entry(format!("{} sizes={sizes:?}\n    {}", t.lines().next().unwrap_or(""), frames.join("\n    "))).or_default() += 1;
            }
            db.drop_branch(&name).unwrap();
        }
        println!("build {build_no}: allocations per create (count: creates) {counts:?}");
        for (site, n) in &sites {
            println!("  x{n} {site}");
        }
    }
}

/// `k` rounds, each on a fresh named branch: create, connect by name, first write, disconnect,
/// delete; with a trunk connect and disconnect (the reference a branch connect is held to) and an
/// empty window (the instruction count's control).
fn rounds(cell: &str, db: &Arc<Database>, base: Option<u64>, k: u64, out: &mut String) {
    let trunk = db.connect().unwrap();
    for i in 0..WARMUP + k {
        let name = format!("m-{i:04}");
        assert_eq!(name.len() as u64, NAME_LEN);
        let sql = format!("UPDATE t SET v = 'w{i:04}' WHERE id = {}", 1 + i % 50);
        let (_, s_noop) = measure(db, base, || ());
        let (tc, s_tconnect) = measure(db, base, || db.connect().unwrap());
        let (_, s_tdisc) = measure(db, base, || drop(tc));
        let (_, s_create) = measure(db, base, || trunk.create_branch(&name).unwrap());
        let (c, s_connect) = measure(db, base, || db.connect_named(&name).unwrap());
        let (_, s_write) = measure(db, base, || c.execute(&sql).unwrap());
        let (_, s_disc) = measure(db, base, || drop(c));
        let (_, s_delete) = measure(db, base, || db.drop_branch(&name).unwrap());
        if i >= WARMUP {
            let j = i - WARMUP;
            line(out, cell, "noop", j, &s_noop);
            line(out, cell, "connect_trunk", j, &s_tconnect);
            line(out, cell, "disconnect_trunk", j, &s_tdisc);
            line(out, cell, "create", j, &s_create);
            line(out, cell, "connect_named", j, &s_connect);
            line(out, cell, "first_write", j, &s_write);
            line(out, cell, "disconnect", j, &s_disc);
            line(out, cell, "delete", j, &s_delete);
        }
    }
}

/// The growth arm: from `N_LARGE` live branches, `GROWTH` more named creates by 16 threads, each
/// create's allocations counted on its own thread (exact however many run). One line per thread:
/// its creates, the largest allocation bytes one create made under the store mutex and in all,
/// and the checkpoints started meanwhile (by any thread).
fn growth(cell: &str, db: &Arc<Database>, out: &mut String) {
    const THREADS: u64 = 16;
    let (ck0, e0) = (db.branch_checkpoint_counters(), exceptions(db));
    let lines: Vec<Sample> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                s.spawn(move || {
                    let c = db.connect().unwrap();
                    let mut m = Sample::new();
                    let (mut creates, mut max_held, mut max_all, mut max_held_n) = (0u64, 0u64, 0u64, 0u64);
                    let mut i = N_LARGE + t;
                    while i < N_LARGE + GROWTH {
                        let name = format!("g-{i}");
                        let a = probe::thread_allocs();
                        fork_one(&c, Some(&name));
                        let b = probe::thread_allocs();
                        creates += 1;
                        max_all = max_all.max(b.1 - a.1);
                        max_held = max_held.max(b.3 - a.3);
                        max_held_n = max_held_n.max(b.2 - a.2);
                        i += THREADS;
                    }
                    m.insert("creates", creates);
                    m.insert("max_alloc_bytes", max_all);
                    m.insert("max_held_alloc_bytes", max_held);
                    m.insert("max_held_allocs", max_held_n);
                    m
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    db.branch_checkpoint_wait();
    let (ck1, e1) = (db.branch_checkpoint_counters(), exceptions(db));
    // The arm is one window: every line carries its counted exceptions.
    for (t, mut s) in lines.into_iter().enumerate() {
        s.insert("ckpt_started", ck1[1] - ck0[1]);
        put_exceptions(&mut s, e0, e1);
        line(out, cell, "growth_thread", t as u64, &s);
    }
    let live = db.branch_ids().unwrap().len() as u64;
    let mut s = Sample::new();
    s.insert("live", live);
    put_exceptions(&mut s, e0, e1);
    line(out, cell, "growth_live", 0, &s);
}

/// The checkpoint arm: the threshold lowered to `CKPT_THRESHOLD`, then `CKPT_ROUNDS` rounds of a
/// named create, a first write on it and its disconnect, each measured with the checkpoint it
/// starts charged to it.
fn checkpoints(cell: &str, db: &Arc<Database>, base: Option<u64>, out: &mut String) {
    let trunk = db.connect().unwrap();
    super::journal::set_compact_threshold(CKPT_THRESHOLD);
    for i in 0..CKPT_ROUNDS {
        let name = format!("c-{i:04}");
        let sql = format!("UPDATE t SET v = 'c{i:04}' WHERE id = {}", 1 + i % 50);
        // The checkpoint each starts runs on the persistent checkpoint worker: owned.
        let (_, s_create) = measure_owning(db, base, &["branch-checkpoint"], || trunk.create_branch(&name).unwrap());
        let c = db.connect_named(&name).unwrap();
        let (_, s_write) = measure_owning(db, base, &["branch-checkpoint"], || c.execute(&sql).unwrap());
        drop(c);
        line(out, cell, "ckpt_create", i, &s_create);
        line(out, cell, "ckpt_first_write", i, &s_write);
    }
    super::journal::set_compact_threshold(0);
}

/// Stops a shared arm's trunk writer (`creating` false) and releases its clients (`go` true) when
/// dropped, on every exit of the arm's scope, a panic included: a client spawn refused after the
/// writer started had left it looping, and the scope joining it, until the child's timeout (review
/// C08 #3; plant `budget_spawn_fails`).
struct ReleaseArm<'a> {
    creating: &'a std::sync::atomic::AtomicBool,
    go: &'a std::sync::atomic::AtomicBool,
}

impl Drop for ReleaseArm<'_> {
    fn drop(&mut self) {
        self.go.store(true, std::sync::atomic::Ordering::Release);
        self.creating.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// The shared-flight arm (LEAP L3): `c` threads, released together, each making `shared_rounds(c)`
/// named creates (arm `shared_create`), then, at C <= 64, as many create-then-first-write cycles
/// (arm `shared_cfw`), closed loop. One line per arm: the durable acknowledgements (creates, plus
/// first writes in the cfw arm) and the flushes issued meanwhile, process-wide. Threads get 8 MiB
/// stacks (RUST_MIN_STACK's 64 MiB times 1024 threads is 64 GiB of reservation). At C = 8 also a
/// serialised-schedule control (arm `shared_serial`, DECISIONS 6d491917d2 proposal 2): the clients
/// take turns on a test-side mutex around each create, so no flight is shared and no waiter need
/// wait; the contention budgets must judge it without a false refusal.
fn shared(cell: &str, db: &Arc<Database>, c: u64, out: &mut String) {
    let rounds = shared_rounds(c);
    for (arm, cfw) in [("shared_create", false), ("shared_cfw", true), ("shared_commits", false), ("shared_serial", false)] {
        if cfw && c > 64 {
            continue;
        }
        let serial = arm == "shared_serial";
        if serial && c != 8 {
            continue;
        }
        let one_at_a_time = std::sync::Mutex::new(());
        // Review 2 M5: creates beside a trunk writer (one DDL, then UPDATEs until the creates end),
        // at C = 8 and 64 only.
        let commits = arm == "shared_commits";
        if commits && !(c == 8 || c == 64) {
            continue;
        }
        let creating = std::sync::atomic::AtomicBool::new(true);
        // A start flag, not a Barrier: a thread that fails to start, or panics before the start,
        // cannot leave the others parked for ever.
        let go = std::sync::atomic::AtomicBool::new(false);
        let e0 = exceptions(db);
        let s0 = probe::window_syncs();
        let p0 = super::sync_counts();
        let o0 = super::store::own_sync_counts();
        // Per client, from its start to its last acknowledgement: the store's condition-variable
        // waits (`store::thread_waits`, one per wait call, so a futile wake-up that waits again
        // counts again), and the most any one acknowledgement waited (review 2 H5: a sum is a mean,
        // and hides one convoyed acknowledgement); its store-mutex acquisitions; the creates
        // refused Busy or SchemaUpdated and retried (the count `fork_one` returns; the population
        // discards it); and its futile leads (`probe::futile_leads`: the store mutex and the group
        // lock taken to lead, and no flight led; review 1 #9).
        let per: Vec<(u64, u64, u64, u64, u64, u64)> = std::thread::scope(|s| {
            let mut clients = Vec::new();
            let creating = &creating;
            // On every exit of this scope, a panic included, the writer stops and the clients are
            // released before the scope joins them (review C08 #3).
            let _release = ReleaseArm { creating, go: &go };
            let writer = commits.then(|| {
                s.spawn(move || {
                    let trunk = db.connect().unwrap();
                    // The writer is load, not a subject: a statement refused Busy or SchemaUpdated
                    // (a fork holding the trunk's write lock) is retried, not a crashed cell.
                    let write = |sql: &str| {
                        for _ in 0..=FORK_RETRY_CAP {
                            match trunk.execute(sql) {
                                Ok(_) => return,
                                Err(LimboError::Busy) | Err(LimboError::SchemaUpdated) => continue,
                                Err(e) => panic!("{cell}: trunk writer: {sql}: {e}"),
                            }
                        }
                        panic!("{cell}: trunk writer: {sql}: refused {FORK_RETRY_CAP} times in a row");
                    };
                    write(&format!("CREATE TABLE w{c}(x INTEGER)"));
                    let mut i = 0u64;
                    while creating.load(std::sync::atomic::Ordering::Acquire) {
                        write(&format!("UPDATE t SET v = 'c{i}' WHERE id = {}", 1 + i % 50));
                        i += 1;
                    }
                })
            });
            for t in 0..c {
                let (go, one_at_a_time) = (&go, &one_at_a_time);
                let client = move || {
                    let trunk = db.connect();
                    while !go.load(std::sync::atomic::Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    let trunk = trunk.unwrap();
                    let (w0, l0, f0, g0) = (super::store::thread_waits(), probe::thread_locks(), probe::futile_leads(), probe::fork_registrations());
                    let (mut retries, mut max_ack) = (0u64, 0u64);
                    for i in 0..rounds {
                        let name = format!("{arm}-{t}-{i:04}");
                        let a0 = super::store::thread_waits();
                        let turn = serial.then(|| one_at_a_time.lock().unwrap_or_else(|e| e.into_inner()));
                        retries += fork_one(&trunk, Some(&name));
                        drop(turn);
                        max_ack = max_ack.max(delta(a0, super::store::thread_waits()));
                        if cfw {
                            let b = db.connect_named(&name).unwrap();
                            let a1 = super::store::thread_waits();
                            exec(&b, &format!("UPDATE t SET v = 's{i}' WHERE id = {}", 1 + (t * 7 + i) % 50));
                            max_ack = max_ack.max(delta(a1, super::store::thread_waits()));
                        }
                    }
                    (
                        delta(w0, super::store::thread_waits()),
                        delta(l0, probe::thread_locks()),
                        retries,
                        max_ack,
                        delta(f0, probe::futile_leads()),
                        delta(g0, probe::fork_registrations()),
                    )
                };
                // A setup-failure plant (review C08 #3, `FE_MUTANT=budget_spawn_fails`, test builds
                // only): the second client's spawn is refused, as resource exhaustion would refuse
                // it, after the trunk writer started. Its outcome is a prompt crash naming the
                // spawn, never a budget's kill.
                let spawned = if commits && t == 1 && super::store::fe_mutant("budget_spawn_fails") {
                    Err(std::io::Error::other("planted spawn failure (FE_MUTANT=budget_spawn_fails)"))
                } else {
                    std::thread::Builder::new().stack_size(8 << 20).spawn_scoped(s, client)
                };
                match spawned {
                    Ok(h) => clients.push(h),
                    // `_release` stops the writer and releases the started clients as this unwinds.
                    Err(e) => panic!("{cell}: {arm}: a thread of {c} did not start: {e}"),
                }
            }
            go.store(true, std::sync::atomic::Ordering::Release);
            // Every client is joined before the writer is told to stop, and only then is a client's
            // panic raised: unwrapping as it joined left the writer looping, and the scope waiting on
            // it, until the child's timeout.
            let joined: Vec<_> = clients.into_iter().map(|h| h.join()).collect();
            creating.store(false, std::sync::atomic::Ordering::Release);
            let per: Vec<(u64, u64, u64, u64, u64, u64)> = joined.into_iter().map(|j| j.unwrap()).collect();
            if let Some(w) = writer {
                w.join().unwrap();
            }
            per
        });
        let s1 = probe::window_syncs();
        let p1 = super::sync_counts();
        let o1 = super::store::own_sync_counts();
        let e1 = exceptions(db);
        let mut m = Sample::new();
        put_exceptions(&mut m, e0, e1);
        m.insert("threads", c);
        m.insert("acks", c * rounds * if cfw { 2 } else { 1 });
        m.insert("full_fsync", s1.full_fsync - s0.full_fsync);
        m.insert("fsync", s1.fsync - s0.fsync);
        m.insert("barrier", s1.barrier - s0.barrier);
        m.insert("fsync_process", p1.fsync - p0.fsync);
        m.insert("full_fsync_process", p1.full_fsync - p0.full_fsync);
        m.insert("barrier_process", p1.barrier - p0.barrier);
        m.insert("fsync_own", o1.fsync - o0.fsync);
        m.insert("full_fsync_own", o1.full_fsync - o0.full_fsync);
        m.insert("barrier_own", o1.barrier - o0.barrier);
        m.insert("store_waits", per.iter().map(|p| p.0).sum());
        m.insert("store_waits_max_client", per.iter().map(|p| p.0).max().unwrap_or(0));
        m.insert("store_waits_max_ack", per.iter().map(|p| p.3).max().unwrap_or(0));
        m.insert("store_locks", per.iter().map(|p| p.1).sum());
        m.insert("retries", per.iter().map(|p| p.2).sum());
        m.insert("futile_leads", per.iter().map(|p| p.4).sum());
        m.insert("registrations", per.iter().map(|p| p.5).sum());
        line(out, cell, arm, 0, &m);
    }
}

/// Rounds per thread in the shared-flight arm: 48, down to 8 at C = 1024 (C·rounds: 48, 384,
/// 3,072, 3,072, 8,192), so the start and drain flights weigh little in every cell.
fn shared_rounds(c: u64) -> u64 {
    (3_072 / c).clamp(8, 48)
}

/// The memory arm (the L4 memory ruling, DECISIONS.md 2026-10-06T02:55Z (i)): one open of the built
/// database, its background work (the name filter's build; under L4, the name map's) waited out.
/// One line: the heap bytes that stayed resident from before the open to after it settled, and the
/// largest single store-mutex hold meanwhile (allocated bytes, catalog rows), by any thread.
fn memory(cell: &str, built: &mut Built, out: &mut String) {
    let base = probe::threads();
    probe::mark_foreground(true);
    let _ = probe::take_hold_maxima();
    let foot0 = probe::phys_footprint();
    let before = probe::live_heap_bytes();
    let held0 = probe::thread_allocs().3;
    let db = open_at(&built.path);
    assert_ne!(db.incarnation, built.incarnation, "{cell}: the registry returned the old Database: not a reopen");
    built.incarnation = db.incarnation;
    db.branch_wait_name_filter();
    let settled = quiesce(&db, base);
    let after = probe::live_heap_bytes();
    let held1 = probe::thread_allocs().3;
    let maxima = probe::take_hold_maxima();
    let ((hold_bytes, hold_rows), (bg_bytes, bg_rows)) = (maxima.all, maxima.background);
    probe::mark_foreground(false);
    let foot1 = probe::phys_footprint();
    // Counted since this open (`Outside::zero`), read once its background work settled and every
    // count above was taken.
    let exc = exceptions(&db);
    let mut s = Sample::new();
    // A negative delta (the open freed more than it kept) is recorded as such, never clamped: the
    // test refuses it.
    match u64::try_from(after - before) {
        Ok(v) => s.insert("resident_bytes", v),
        Err(_) => s.insert("resident_negative", u64::try_from(before - after).unwrap_or(u64::MAX)),
    };
    if let (Some(f0), Some(f1)) = (foot0, foot1) {
        // Reported, not budgeted (allocator rounding, caches and mmap'd pages; noisier).
        s.insert(if f1 >= f0 { "footprint_bytes" } else { "footprint_negative" }, f1.abs_diff(f0));
    }
    s.insert("max_hold_alloc_bytes", hold_bytes);
    s.insert("max_hold_catalog_rows", hold_rows);
    s.insert("max_bg_hold_alloc_bytes", bg_bytes);
    s.insert("max_bg_hold_catalog_rows", bg_rows);
    s.insert("max_fg_hold_alloc_bytes", maxima.foreground.0);
    s.insert("max_fg_hold_catalog_rows", maxima.foreground.1);
    // Linux finding (b): held work that allocates nothing (filter inserts), per hold.
    s.insert("max_hold_work", maxima.work.0);
    s.insert("max_bg_hold_work", maxima.work.1);
    s.insert("max_fg_hold_work", maxima.work.2);
    // budgets-rev-242345 LOW-4 and LOW-5: each per-name site apart (`probe::WorkSite`).
    s.insert("max_seed_hold_work", maxima.site_work[0]);
    s.insert("max_install_hold_work", maxima.site_work[1]);
    s.insert("max_growth_hold_work", maxima.site_work[2]);
    s.insert("max_recovery_work", maxima.site_work[3]);
    // Review 2 M7: the opening thread's held bytes summed over every hold of the open.
    s.insert("fg_held_alloc_bytes_sum", delta(held0, held1));
    put_exceptions(&mut s, [0; 4], exc);
    s.insert("quiet", u64::from(settled));
    line(out, cell, "memory", 0, &s);
    // budgets-rev-242345 MED-7: a growth of the filter the open built, its two holds (swap, drain) measured
    // apart. The build sizes the table under half full, so the next create is made to owe one
    // (`FILTER_GROW_NEXT`); the growth then copies every name, as any growth does. `growth_runs` is the
    // premise: the growth site ran.
    let base = probe::threads();
    let _ = probe::take_hold_maxima();
    let e0 = exceptions(&db);
    super::store::FILTER_GROW_NEXT.store(true, std::sync::atomic::Ordering::Release);
    db.connect().unwrap().create_branch(&format!("{cell}-grow")).unwrap();
    db.branch_wait_name_filter();
    let settled = quiesce(&db, base);
    super::store::FILTER_GROW_NEXT.store(false, std::sync::atomic::Ordering::Release);
    let m = probe::take_hold_maxima();
    let mut g = Sample::new();
    g.insert("max_growth_hold_work", m.site_work[2]);
    g.insert("growth_runs", m.site_runs[2]);
    put_exceptions(&mut g, e0, exceptions(&db));
    g.insert("quiet", u64::from(settled));
    line(out, cell, "growth", 0, &g);
}

/// The confirmation arm (review 6 #1's fix: the word leaves the create path for a background
/// writer): `k` rounds of a create whose window counts the word's write (`confirm_written`), then
/// the same create with the word held back (`confirm_held`, the engine's test knob), then a second
/// held create (`confirm_reference`, review 2 H2), which lands on the first's pending word and so
/// wakes no thread at its landing. Written minus reference is the word's cost with the landing's
/// wake; held minus reference is the wake alone. A held window leaves the writer parked with the word pending and no deadline,
/// and a landing wakes it only when nothing provable was pending, so each round first takes the
/// pending word away with a checkpoint (`mark_durable`) and makes one unmeasured create (which pays
/// the checkpoint's deferred directory sync and is written as usual).
fn confirm(cell: &str, db: &Arc<Database>, base: Option<u64>, k: u64, out: &mut String) {
    let trunk = db.connect().unwrap();
    for i in 0..WARMUP + k {
        hold_word(false);
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        let (_, _) = measure(db, base, || trunk.create_branch(&format!("u-{i:04}")).unwrap());
        // These cells measure the confirmation writer's own cost (its word, its wake): owned.
        let (_, s_written) = measure_owning(db, base, &["branch-confirm"], || trunk.create_branch(&format!("w-{i:04}")).unwrap());
        hold_word(true);
        let (_, s_held) = measure_owning(db, base, &["branch-confirm"], || trunk.create_branch(&format!("h-{i:04}")).unwrap());
        // Review 2 H2: the reference. `h` landed with no proved word pending (w's was written), so its
        // landing woke the parked writer (`confirm_cv.notify_one`, on the acknowledging thread); this
        // second held create lands on h's pending word and wakes no one.
        let (_, s_reference) = measure_owning(db, base, &["branch-confirm"], || trunk.create_branch(&format!("r-{i:04}")).unwrap());
        if i >= WARMUP {
            line(out, cell, "confirm_written", i - WARMUP, &s_written);
            line(out, cell, "confirm_held", i - WARMUP, &s_held);
            line(out, cell, "confirm_reference", i - WARMUP, &s_reference);
        }
    }
    hold_word(false);
}

/// The confirmation writer's load arms (`confirm_load`): `(C clients per burst, the parent's delay in ms after the burst before it
/// reads the burst's counts)`. The delayed arm (review C08 #1) is a scheduling control: a parent
/// descheduled past the 5 ms quiet period after the last create must not turn the idle tail's
/// honest word into a word "while flights overlapped".
const CONFIRM_LOAD_ARMS: [(u64, u64); 4] = [(1, 0), (8, 0), (64, 0), (8, 20)];

/// The load cell's idle-hole control (DECISIONS 6d491917d2): 8 clients that all pause 20 ms
/// together half-way through their creates.
const LOAD_GAPPED: &str = "load_c8_gapped";

/// The load cell's reachability arm (lead, DECISIONS 2026-10-15: the writer's poll and its slot hold
/// were unreachable at the shipped schedule, and are not equivalent): 8 clients whose every syncing
/// flight stays in the air `SLOW_FLIGHT_US` past its sync (`journal::FLIGHT_SYNC_DELAY_US`, a test
/// hook), longer than the 5 ms quiet period.
const LOAD_SLOW: &str = "load_c8_slow_flights";

/// The slow arm's added time in the air per flight: 8 ms, past the 5 ms quiet period.
const SLOW_FLIGHT_US: u64 = 8_000;

/// Sets the engine's flight-sync delay; restores 0 when dropped, on every exit of the arm.
struct FlightDelay;

impl FlightDelay {
    fn set(us: u64) -> Self {
        super::journal::FLIGHT_SYNC_DELAY_US.store(us, std::sync::atomic::Ordering::Release);
        Self
    }
}

impl Drop for FlightDelay {
    fn drop(&mut self) {
        super::journal::FLIGHT_SYNC_DELAY_US.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// The line name of a load arm.
fn load_arm(c: u64, delay_ms: u64) -> String {
    if delay_ms == 0 {
        format!("load_c{c}")
    } else {
        format!("load_c{c}_delayed")
    }
}

/// Review 2 H1: the confirmation writer under load, with the shipped quiet period (the word NOT
/// held). For each arm of `CONFIRM_LOAD_ARMS`, C clients make back-to-back creates
/// (`shared_rounds(C)` each, started together), then the cell waits out the idle tail: at most 1 s
/// for the writer to settle with the tail's word written (`writer_settled`), and 20 ms more for a
/// second one that must not come (wall time only to settle; nothing timed is measured). One line
/// per arm (`load_c{C}`, `load_c{C}_delayed`): acknowledgements, flights (`group_counters()[0]`),
/// the words by the flight they confirm (the burst's, `confirms_burst`; the idle tail's,
/// `confirms_tail`; review C08 #1) and in all (`confirms_total`), the old reading by the time the
/// parent read them (`confirms_by_read`, reported only), whether the tail settled, the burst's idle
/// holes (`idle_holes`), and the writer's wake-ups with a word pending (`confirm_wakeups_for_test`)
/// by the time the parent read them (the delayed arm's include the first of its tail's).
fn confirm_load(cell: &str, db: &Arc<Database>, out: &mut String) {
    hold_word(false);
    let words = || db.branch_confirm_counts()[0];
    // The idle tail: the writer parked with nothing pending (its word written, `writer_settled`),
    // then 20 ms more for a second word that must not come. Whether the writer settled.
    let settle = || {
        let settled = writer_settled(db, std::time::Duration::from_secs(1));
        std::thread::sleep(std::time::Duration::from_millis(20));
        settled
    };
    // The population's own tail word is written before the first burst starts.
    settle();
    let gapped = (8, 0, true, false);
    let slow = (8, 0, false, true);
    for (c, delay_ms, gap, slow) in CONFIRM_LOAD_ARMS.iter().map(|&(c, d)| (c, d, false, false)).chain([gapped, slow]) {
        let rounds = shared_rounds(c);
        // The arm's line, and its creates' name prefix: three arms run at C = 8 on one database, so a
        // prefix by C alone made the second arm's first create NameTaken (base16 at fb31e6153).
        let arm = if slow {
            LOAD_SLOW.to_string()
        } else if gap {
            LOAD_GAPPED.to_string()
        } else {
            load_arm(c, delay_ms)
        };
        let arm = arm.as_str();
        let e0 = exceptions(db);
        let (w0, k0, f0) = (words(), db.branches.confirm_wakeups_for_test(), db.branches.group_counters()[0]);
        let go = std::sync::atomic::AtomicBool::new(false);
        // The gapped control's pause: every client meets half-way, the last to arrive sleeps 20 ms and
        // releases them all, so no create is in progress for those 20 ms (an idle hole on purpose).
        // Atomics, not a Barrier: a client that failed to start must not leave the others waiting.
        let (arrived, released) = (std::sync::atomic::AtomicU64::new(0), std::sync::atomic::AtomicBool::new(false));
        // The slow arm's flights stay in the air past the quiet period (the engine's test hook).
        let _slow = slow.then(|| FlightDelay::set(SLOW_FLIGHT_US));
        let spans: Vec<Vec<(std::time::Instant, std::time::Instant)>> = std::thread::scope(|s| {
            let mut clients = Vec::new();
            for t in 0..c {
                let (go, arrived, released) = (&go, &arrived, &released);
                let spawned = std::thread::Builder::new().stack_size(8 << 20).spawn_scoped(s, move || {
                    let trunk = db.connect();
                    while !go.load(std::sync::atomic::Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    let trunk = trunk.unwrap();
                    let mut spans = Vec::with_capacity(rounds as usize);
                    for i in 0..rounds {
                        if gap && i == rounds / 2 {
                            use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
                            if arrived.fetch_add(1, AcqRel) + 1 == c {
                                std::thread::sleep(std::time::Duration::from_millis(20));
                                released.store(true, Release);
                            }
                            while !released.load(Acquire) {
                                std::thread::yield_now();
                            }
                        }
                        let started = std::time::Instant::now();
                        trunk.create_branch(&format!("{arm}-{t}-{i:04}")).unwrap();
                        spans.push((started, std::time::Instant::now()));
                    }
                    spans
                });
                match spawned {
                    Ok(h) => clients.push(h),
                    Err(e) => {
                        go.store(true, std::sync::atomic::Ordering::Release);
                        released.store(true, std::sync::atomic::Ordering::Release);
                        panic!("{cell}: a thread of {c} did not start: {e}");
                    }
                }
            }
            go.store(true, std::sync::atomic::Ordering::Release);
            clients.into_iter().map(|h| h.join().unwrap()).collect()
        });
        drop(_slow);
        // The delayed arm's parent, descheduled on purpose (review C08 #1's schedule).
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        let (w1, k1, f1) = (words(), db.branches.confirm_wakeups_for_test(), db.branches.group_counters()[0]);
        let settled = settle();
        // Review C08 #1: the words are attributed by the flight they confirm, not by when the parent
        // read them. The tail's are the last landed flight's own (`confirm_tail_words_for_test`,
        // counted by the writer as it writes them); every other word written was a burst's.
        let (w2, tail) = (words(), db.branches.confirm_tail_words_for_test());
        let e1 = exceptions(db);
        let total = delta(w0, w2);
        let mut m = Sample::new();
        put_exceptions(&mut m, e0, e1);
        m.insert("threads", c);
        m.insert("acks", c * rounds);
        m.insert("flights", delta(f0, f1));
        m.insert("confirms_burst", total.saturating_sub(tail));
        m.insert("confirms_tail", tail);
        m.insert("confirms_total", total);
        // The old reading, words by the time the parent read them (reported only).
        m.insert("confirms_by_read", delta(w0, w1));
        m.insert("tail_settled", u64::from(settled));
        m.insert("idle_holes", idle_holes(&spans, std::time::Duration::from_millis(CONFIRM_QUIET_MS_SHIPPED)));
        m.insert("wakeups_burst", delta(k0, k1));
        // The burst's wall time, from the first create's call to the last one's return (wall time only
        // for the slow arm's premise; no budget reads a time).
        let first = spans.iter().flatten().map(|s| s.0).min();
        let last = spans.iter().flatten().map(|s| s.1).max();
        if let (Some(a), Some(b)) = (first, last) {
            m.insert("burst_us", (b - a).as_micros() as u64);
        }
        line(out, cell, arm, 0, &m);
    }
    hold_word(true);
}

/// The shipped quiet period of the confirmation writer, in ms (`CONFIRM_QUIET`, store.rs).
const CONFIRM_QUIET_MS_SHIPPED: u64 = 5;

/// Review C08 #1: the gaps of at least `quiet` in the union of the creates' spans, each client's
/// create from its call to its return. Such a gap is an idle period the writer may honestly fill
/// with a word while the burst still has creates to make; reported, not budgeted (a create stalled
/// for that long before it joins a flight is one too, and is not seen here).
fn idle_holes(spans: &[Vec<(std::time::Instant, std::time::Instant)>], quiet: std::time::Duration) -> u64 {
    let mut all: Vec<(std::time::Instant, std::time::Instant)> = spans.iter().flatten().copied().collect();
    all.sort_by_key(|s| s.0);
    let Some(&(_, mut end)) = all.first() else {
        return 0;
    };
    let mut holes = 0;
    for &(start, stop) in &all[1..] {
        if start > end && start - end >= quiet {
            holes += 1;
        }
        end = end.max(stop);
    }
    holes
}

/// The open-flush cells (lead order, DECISIONS 1334833a4d; review 16 LOW 11's a9ba5adb8): a store's
/// open of a whole log it KEEPS (the kept-log arm of `Scanned::finish`), counted absolutely, because
/// the recovery budgets compare cells and a regression hitting every open equally passes them, and
/// because the recovery cells' opens take another arm (BUDGETS.md, fixture findings). Cells:
/// * `open_d2_unconfirmed`: a snapshot store at D2 whose last flight carries no confirmation word: a
///   writer grandchild (`open_writer_d2`) builds it and is SIGKILLed before it closes, so no close
///   writes the word; one reopen measured per sample, 3 samples;
/// * `open_d2_confirmed`, `open_cat_d2`, `open_d1`: built in this process and closed cleanly (the
///   close writes the word: at once where the class proves stable storage, D2 on Apple, and at D1
///   after a full flush of the log of its own, `Confirm::write_at_close`, a81058160), then reopened 5
///   times, each reopen closed cleanly again.
///
/// Per open: the process's device flushes and fsyncs (`io::SYNC_COUNTS`), the opening thread's syncs
/// by the class asked (`journal::CLASS_SYNCS`) and directory syncs (`journal::DIR_SYNCS`), and the
/// premises: the log's inode and length unchanged across the open (it was kept: no rewrite, no
/// cut, no reset) and whether its header's word confirms its last flight (`confirmed`). Each line
/// of the confirmed D2 cells also carries this process's fcntl(F_FULLFSYNC) on a directory
/// descriptor of the cell's own temp dir, outside every measured open (`dir_full_rc`,
/// `dir_full_errno`; review of the confirmed budget's minimum: it rests on that call working).
fn open_flush(cell: &str, out: &mut String) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("open.db");
    let (opens, unconfirmed) = if cell == "open_d2_unconfirmed" { (3, true) } else { (5, false) };
    let probe_dir = dir_full_fsync(dir.path());
    let mut log = None;
    if !unconfirmed {
        let db = open_at(&path);
        let trunk = db.connect().unwrap();
        seed(&trunk, false);
        for i in 0..4 {
            trunk.create_branch(&format!("o-{i}")).unwrap();
        }
        log = db.branch_log_path();
    }
    for r in 0..opens {
        if unconfirmed {
            let sample = tempfile::TempDir::new_in(dir.path()).unwrap();
            log = Some(open_writer_killed(cell, sample.path()));
            let mut s = open_once(&sample.path().join("open.db"), log.as_ref().unwrap());
            s.insert("dir_full_rc", probe_dir.0);
            s.insert("dir_full_errno", probe_dir.1);
            line(out, cell, "open", r, &s);
        } else {
            let mut s = open_once(&path, log.as_ref().expect("the built store's log"));
            s.insert("dir_full_rc", probe_dir.0);
            s.insert("dir_full_errno", probe_dir.1);
            line(out, cell, "open", r, &s);
        }
    }
}

/// fcntl(F_FULLFSYNC) on a directory descriptor (Apple): `(rc, errno)`, `rc` 0 or 1 for -1; elsewhere
/// `(2, 0)`, not applicable.
fn dir_full_fsync(dir: &Path) -> (u64, u64) {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        let d = std::fs::File::open(dir).unwrap();
        // SAFETY: the descriptor is owned by `d` and open for the duration of the call.
        let rc = unsafe { libc::fcntl(d.as_raw_fd(), libc::F_FULLFSYNC) };
        let errno = if rc == -1 { std::io::Error::last_os_error().raw_os_error().unwrap_or(0) as u64 } else { 0 };
        (u64::from(rc == -1), errno)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = dir;
        (2, 0)
    }
}

/// The header word of `log` and the word that would confirm its last flight (`journal::confirm_word`
/// of its last 4 bytes, the end frame's checksum, and its length), read before an open.
fn log_confirmed(log: &Path) -> u64 {
    let bytes = std::fs::read(log).unwrap();
    // HEADER_CONFIRM_AT (journal.rs): the word sits at bytes 36..40 of the header.
    let word = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
    let crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    u64::from(word != 0 && word == super::journal::confirm_word(crc, bytes.len() as u64))
}

/// One measured open of the store at `path`, whose log is `log`, then a clean close.
fn open_once(path: &Path, log: &Path) -> Sample {
    use std::os::unix::fs::MetadataExt;
    let (m0, confirmed) = (std::fs::metadata(log).unwrap(), log_confirmed(log));
    let classes = || super::journal::CLASS_SYNCS.with(|c| [c[0].get(), c[1].get()]);
    let dirs = || super::journal::DIR_SYNCS.with(|c| c.get());
    let (s0, c0, d0) = (probe::window_syncs(), classes(), dirs());
    let p0 = super::sync_counts();
    let o0 = super::store::own_sync_counts();
    let db = open_at(path);
    let (s1, c1, d1) = (probe::window_syncs(), classes(), dirs());
    let p1 = super::sync_counts();
    let o1 = super::store::own_sync_counts();
    let m1 = std::fs::metadata(log).unwrap();
    // Counted since this open (`Outside::zero`).
    let exc = exceptions(&db);
    drop(db);
    let mut s = Sample::from([
        ("full_fsync", s1.full_fsync - s0.full_fsync),
        ("fsync", s1.fsync - s0.fsync),
        ("barrier", s1.barrier - s0.barrier),
        ("fsync_process", p1.fsync - p0.fsync),
        ("full_fsync_process", p1.full_fsync - p0.full_fsync),
        ("barrier_process", p1.barrier - p0.barrier),
        ("fsync_own", o1.fsync - o0.fsync),
        ("full_fsync_own", o1.full_fsync - o0.full_fsync),
        ("barrier_own", o1.barrier - o0.barrier),
        ("thread_plain", c1[0] - c0[0]),
        ("thread_full", c1[1] - c0[1]),
        ("dir_syncs", d1 - d0),
        ("same_inode", u64::from(m0.ino() == m1.ino())),
        ("same_len", u64::from(m0.len() == m1.len())),
        ("confirmed", confirmed),
    ]);
    put_exceptions(&mut s, [0; 4], exc);
    s
}

/// Kills a writer grandchild when dropped, so no test failure leaves it running.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `open_d2_unconfirmed`'s writer: this binary again, as the child `open_writer_d2` building the
/// store in `dir`; SIGKILLed once it reports ready, so it never closes the store. Returns its log.
fn open_writer_killed(cell: &str, dir: &Path) -> PathBuf {
    let ready = dir.join("ready");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::budget_tests::budget_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_BUDGET_CHILD", "open_writer_d2")
        .env("FE_BUDGET_OUT", &ready)
        .env("FE_BUDGET_DIR", dir)
        .env_remove("FE_BUDGET_RAW_DIR")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut child = KillOnDrop(child);
    let t = std::time::Instant::now();
    let log = loop {
        if let Ok(text) = std::fs::read_to_string(&ready) {
            if let Some(log) = text.strip_prefix("ready ").map(|l| l.trim_end().to_string()) {
                break PathBuf::from(log);
            }
        }
        assert!(t.elapsed() < std::time::Duration::from_secs(120), "{cell}: the writer never reported ready");
        assert!(child.0.try_wait().unwrap().is_none(), "{cell}: the writer exited before it was killed");
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    // SIGKILL: no destructor, so no close writes the confirmation word.
    child.0.kill().unwrap();
    let _ = child.0.wait();
    log
}

/// The writer child (`open_writer_d2`): builds a snapshot store at D2 with the word held (no idle
/// writer), reports its log, and parks until it is killed; it exits by itself after 120 s, without
/// running a destructor, so it can never orphan and never closes the store.
fn open_writer(out: &str) -> ! {
    let dir = PathBuf::from(std::env::var("FE_BUDGET_DIR").unwrap());
    let db = open_at(&dir.join("open.db"));
    let trunk = db.connect().unwrap();
    seed(&trunk, false);
    for i in 0..4 {
        trunk.create_branch(&format!("o-{i}")).unwrap();
    }
    let log = db.branch_log_path().unwrap();
    std::fs::write(out, format!("ready {}\n", log.display())).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(120));
    std::process::exit(0)
}

/// The schema-window cells' table counts/// The schema-window cells' table counts (`schema_t10`, `schema_t1e3`), and the samples per path and
/// mode.
const SCHEMA_TABLES: [(&str, u64); 2] = [("schema_t10", 10), ("schema_t1e3", 1000)];
const SCHEMA_ROUNDS: u64 = 3;

/// The calling thread's SQL-layer counters between two moments.
fn sql_sample(a: &probe::SqlCounts, b: &probe::SqlCounts) -> Sample {
    Sample::from([
        ("prepares", delta(a.prepares, b.prepares)),
        ("page_reads", delta(a.page_reads, b.page_reads)),
        ("schema_rows", delta(a.schema_rows, b.schema_rows)),
        ("wal_locks", delta(a.wal_locks, b.wal_locks)),
        ("wal_prepares", delta(a.wal_prepares, b.wal_prepares)),
        ("wal_page_reads", delta(a.wal_page_reads, b.wal_page_reads)),
        ("wal_schema_rows", delta(a.wal_schema_rows, b.wal_schema_rows)),
        ("wal_allocs", delta(a.wal_allocs, b.wal_allocs)),
        ("wal_alloc_bytes", delta(a.wal_alloc_bytes, b.wal_alloc_bytes)),
        ("held_prepares", delta(a.held_prepares, b.held_prepares)),
    ])
}

/// Clears the store's own test hold (`BranchStore::trunk_commit_hold`, stage
/// `store::HOLD_SCHEMA_PUBLISH`; engine review 16 #14) when dropped, so a failed sample releases its
/// DDL commit.
struct SchemaHoldDisarm(Arc<std::sync::atomic::AtomicU8>);

impl Drop for SchemaHoldDisarm {
    fn drop(&mut self) {
        self.0.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// Engine 2b (fastest-engine 00340537c, red 4f9f83ece; its request of 2026-10-09), as engine review
/// 16 MED 8 (678b18ba4) left it: a trunk fork whose snapshot's schema cookie matches neither its
/// connection's schema nor the shared one — a DDL commit between publishing its pages and its
/// schema, parked there by its store's own hold (`HOLD_SCHEMA_PUBLISH` on `trunk_commit_hold`) —
/// registers the child with no schema, and the child parses its own at its first connection
/// (`connect_branch`, `force_reparse_schema_without_publish`). This arm counts, with the SQL-layer
/// counters (`probe::sql_counts`), the create on a thread of its own and then the child's first
/// `connect_named` on another, started while the DDL is still parked (review C08 #2) and released
/// after 30 s if it waits (`connect_waited_for_ddl`), with its store waits and sleeps
/// (`connect_store_waits`, `connect_sleeps`).
///
/// Every sample is a fresh database of `tables` tables (`t` with one row, then fillers, in one
/// transaction; the WAL checkpointed), bootstrapped before anything is counted: one throwaway branch
/// created and dropped, then the database closed and reopened, so the catalog exists and is opened by
/// the open, not by the measured fork (review 2 H3: the first fork of a fresh store ran the catalog's
/// 13 DDL statements and 42 prepares inside the window). Then it is forked once by a fresh connection
/// (`m-0000`):
/// * `first_*`: the trunk has no live child, so the fork takes the WAL write lock;
///   `lockfree_*`: one live child first (`anchor`), so it registers without it;
/// * `*_window`: the fork runs while an `ALTER TABLE t ADD COLUMN` commit is parked in the window;
///   `*_control`: the same ALTER has finished first.
///
/// Premises per sample: `windows_met` (the fork found no schema in hand at its cookie,
/// `probe::schema_window_met`, a hook in `fork_trunk_registered`) is at least 1 in a window and 0 in
/// a control; `wal_locks` is exactly 1 on the first-child path and 0 lock-free. The fork runs on its
/// own thread, so one that waits for the parked commit is released after 30 s (`waited_for_ddl`);
/// its store condition-variable waits are counted (`store_waits`), and the bytes the create and the
/// first connection allocate on their threads (`alloc_bytes`, `connect_alloc_bytes`, from
/// `probe::thread_allocs`; review 2 L5). References: the schema's row
/// count (`schema_table_rows`) and one plain `SELECT * FROM sqlite_schema` on a fresh connection
/// (`ref_scan_page_reads`; it must prepare 1 statement and parse 0 schema rows).
fn schema_window(cell: &str, tables: u64, out: &mut String) {
    use std::sync::atomic::Ordering as O;
    for i in 0..SCHEMA_ROUNDS {
        for first in [true, false] {
            for window in [false, true] {
                let op = match (first, window) {
                    (true, false) => "first_control",
                    (true, true) => "first_window",
                    (false, false) => "lockfree_control",
                    (false, true) => "lockfree_window",
                };
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("schema-window.db");
                let built = {
                    let db = open_at(&path);
                    let ddl = db.connect().unwrap();
                    exec(&ddl, "BEGIN");
                    exec(&ddl, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
                    exec(&ddl, "INSERT INTO t VALUES (1, 'trunk-1')");
                    for f in 1..tables {
                        exec(&ddl, &format!("CREATE TABLE f{f}(id INTEGER PRIMARY KEY, v TEXT)"));
                    }
                    exec(&ddl, "COMMIT");
                    let _ = ddl
                        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
                        .and_then(|mut s| s.run_collect_rows())
                        .unwrap();
                    // The bootstrap (review 2 H3): the store's catalog is created by its first fork.
                    ddl.create_branch("boot").unwrap();
                    db.drop_branch("boot").unwrap();
                    db.incarnation
                };
                let db = open_at(&path);
                assert_ne!(db.incarnation, built, "{cell}: {op}: premise: the registry returned the old Database, not a reopen");
                let ddl = db.connect().unwrap();
                let forker = db.connect().unwrap();
                if !first {
                    forker.create_branch("anchor").unwrap();
                }
                let alter = "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7";
                // The sample's window for the counted exceptions: from here to the joins below.
                let e0 = exceptions(&db);
                let hold = db.branches.trunk_commit_hold.clone();
                let _disarm = SchemaHoldDisarm(hold.clone());
                let parked = if window {
                    hold.store(super::store::HOLD_SCHEMA_PUBLISH, O::Release);
                    let ddl = ddl.clone();
                    let commit = std::thread::spawn(move || ddl.execute(alter).map(|_| ()).map_err(|e| e.to_string()));
                    let t = std::time::Instant::now();
                    while hold.load(O::Acquire) != (super::store::HOLD_SCHEMA_PUBLISH | super::store::HOLD_ARRIVED) {
                        assert!(
                            t.elapsed() < std::time::Duration::from_secs(10),
                            "{cell}: {op}: premise: the DDL commit never reached its schema publication"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    Some(commit)
                } else {
                    exec(&ddl, alter);
                    None
                };
                let fork = {
                    let forker = forker.clone();
                    std::thread::spawn(move || {
                        let (c0, w0, m0, z0, a0) = (probe::sql_counts(), super::store::thread_waits(), probe::schema_windows_met(), probe::sleeps(), probe::thread_allocs());
                        let made = forker.create_branch("m-0000").map(|_| ()).map_err(|e| e.to_string());
                        let (c1, w1, m1, z1, a1) = (probe::sql_counts(), super::store::thread_waits(), probe::schema_windows_met(), probe::sleeps(), probe::thread_allocs());
                        let mut s = sql_sample(&c0, &c1);
                        s.insert("alloc_bytes", delta(a0.1, a1.1));
                        s.insert("store_waits", delta(w0, w1));
                        s.insert("sleeps", delta(z0, z1));
                        s.insert("windows_met", delta(m0, m1));
                        s.insert("held_after", u64::from(probe::wal_lock_held()));
                        (made, s)
                    })
                };
                let waited = !finished_within(&fork, std::time::Duration::from_secs(30));
                // The child's first connection, on a thread of its own and while the DDL is still
                // parked (review C08 #2: connected after the release, a connect that waits for the
                // publication found it already done): under MED 8 a window child parses its schema
                // here, and must not wait for the trunk's. (A fork that itself waited out the bound
                // is connected after the release: that sample fails on the fork's wait.)
                let spawn_connect = || {
                    let db = db.clone();
                    std::thread::spawn(move || {
                        let (k0, b0, w0, z0) = (probe::sql_counts(), probe::thread_allocs(), super::store::thread_waits(), probe::sleeps());
                        let branch = db.connect_named("m-0000").map_err(|e| e.to_string());
                        let (k1, b1, w1, z1) = (probe::sql_counts(), probe::thread_allocs(), super::store::thread_waits(), probe::sleeps());
                        let mut c = Sample::new();
                        for (key, v) in sql_sample(&k0, &k1) {
                            if let Some(k) = CONNECT_KEYS.iter().find(|k| k.strip_prefix("connect_") == Some(key)) {
                                c.insert(*k, v);
                            }
                        }
                        c.insert("connect_alloc_bytes", delta(b0.1, b1.1));
                        c.insert("connect_store_waits", delta(w0, w1));
                        c.insert("connect_sleeps", delta(z0, z1));
                        (branch, c)
                    })
                };
                let connect = (!waited).then(spawn_connect);
                let connect_waited = connect.as_ref().is_some_and(|h| !finished_within(h, std::time::Duration::from_secs(30)));
                hold.store(0, O::Release);
                let (made, mut s) = fork.join().unwrap();
                if let Some(commit) = parked {
                    commit.join().unwrap().unwrap_or_else(|e| panic!("{cell}: {op}: the parked ALTER failed: {e}"));
                }
                made.unwrap_or_else(|e| panic!("{cell}: {op}: the fork failed (waited for the DDL: {waited}): {e}"));
                let (branch, c) = connect.unwrap_or_else(spawn_connect).join().unwrap();
                let branch = branch.unwrap_or_else(|e| panic!("{cell}: {op}: the child's first connection failed: {e}"));
                put_exceptions(&mut s, e0, exceptions(&db));
                s.extend(c);
                s.insert("connect_waited_for_ddl", u64::from(connect_waited));
                // The branch has the schema its pages need (the engine's own test's check).
                assert_eq!(query_int(&branch, "SELECT c FROM t WHERE id = 1"), 7, "{cell}: {op}: the branch read the new column wrong");
                s.insert("waited_for_ddl", u64::from(waited));
                s.insert("schema_table_rows", query_int(&ddl, "SELECT count(*) FROM sqlite_schema") as u64);
                let fresh = db.connect().unwrap();
                let r0 = probe::sql_counts();
                fresh.prepare("SELECT * FROM sqlite_schema").and_then(|mut s| s.run_collect_rows()).unwrap();
                let r1 = probe::sql_counts();
                assert_eq!(delta(r0.prepares, r1.prepares), 1, "{cell}: {op}: premise: the reference scan is one statement");
                assert_eq!(delta(r0.schema_rows, r1.schema_rows), 0, "{cell}: {op}: premise: the reference scan parses no schema");
                s.insert("ref_scan_page_reads", delta(r0.page_reads, r1.page_reads));
                line(out, cell, op, i, &s);
            }
        }
    }
}

/// Review 2 L6's cell (`first_create`): per round, a fresh small database forked once
/// (`first_fresh`: this create creates the store's catalog) and, as the reference, one whose catalog
/// a throwaway branch created before a reopen (`first_reopened`, the schema-window bootstrap: the same
/// first-child path, no catalog to make). Per create, on the creating thread: the SQL-layer counters
/// (`wal_locks` counts every write transaction, the trunk's and the catalog's; `held_prepares` the
/// statements compiled under a store mutex), and the process's flushes (reported).
fn first_create(cell: &str, out: &mut String) {
    for i in 0..SCHEMA_ROUNDS {
        for op in ["first_fresh", "first_reopened"] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("first-create.db");
            let db = if op == "first_reopened" {
                let built = {
                    let db = open_at(&path);
                    let trunk = db.connect().unwrap();
                    seed(&trunk, false);
                    trunk.create_branch("boot").unwrap();
                    db.drop_branch("boot").unwrap();
                    db.incarnation
                };
                let db = open_at(&path);
                assert_ne!(db.incarnation, built, "{cell}: {op}: premise: the registry returned the old Database, not a reopen");
                db
            } else {
                let db = open_at(&path);
                seed(&db.connect().unwrap(), false);
                db
            };
            let trunk = db.connect().unwrap();
            let e0 = exceptions(&db);
            let (c0, f0) = (probe::sql_counts(), probe::window_syncs());
            let p0 = super::sync_counts();
            let o0 = super::store::own_sync_counts();
            trunk.create_branch("m-0000").unwrap();
            let (c1, f1) = (probe::sql_counts(), probe::window_syncs());
            let p1 = super::sync_counts();
            let o1 = super::store::own_sync_counts();
            let mut s = sql_sample(&c0, &c1);
            put_exceptions(&mut s, e0, exceptions(&db));
            s.insert("full_fsync", f1.full_fsync - f0.full_fsync);
            s.insert("fsync", f1.fsync - f0.fsync);
            s.insert("full_fsync_process", p1.full_fsync - p0.full_fsync);
            s.insert("fsync_process", p1.fsync - p0.fsync);
            s.insert("full_fsync_own", o1.full_fsync - o0.full_fsync);
            s.insert("fsync_own", o1.fsync - o0.fsync);
            line(out, cell, op, i, &s);
        }
    }
}

/// Whether the thread behind `h` finished within `bound` (polled every 1 ms; wall time only to
/// bound a wait, nothing timed is measured).
fn finished_within<T>(h: &std::thread::JoinHandle<T>, bound: std::time::Duration) -> bool {
    let t = std::time::Instant::now();
    while !h.is_finished() && t.elapsed() < bound {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    h.is_finished()
}

/// The first-connect keys of a schema-window sample (`sql_sample`'s, prefixed).
const CONNECT_KEYS: [&str; 4] = ["connect_prepares", "connect_page_reads", "connect_schema_rows", "connect_wal_locks"];

/// A named create on `db`, so a checkpoint has a dirty branch to write.
fn trunk_for_fc(db: &Arc<Database>) {
    db.connect().unwrap().create_branch("fc-dirty").unwrap();
}

/// This thread's id: pthread_threadid_np on Apple; off Apple the KERNEL thread id by raw syscall(2)
/// (SYS_gettid), never `libc::gettid`, which glibc has only from 2.30: the RHEL 8 iccad hosts run 2.28,
/// where a build links and the call SIGSEGVs (fastest-budgets, uwrun budgets-segv-1). Off Apple
/// `thread_table` is None, so the table checks have no data there; the id keeps them compiling and
/// distinct per thread.
fn this_thread_id() -> u64 {
    #[cfg(target_vendor = "apple")]
    {
        let mut me = 0u64;
        // SAFETY: this thread's own id into a local.
        unsafe { libc::pthread_threadid_np(0, &mut me) };
        me
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        // SAFETY: gettid(2) has no preconditions and cannot fail.
        unsafe { libc::syscall(libc::SYS_gettid) as u64 }
    }
}

/// `this_thread_id` names the calling thread, distinctly per thread, on every libc: on Linux it is
/// the kernel's id, read back from /proc/thread-self (a link to `<pid>/task/<tid>`).
#[test]
fn this_thread_id_is_the_calling_thread_s_own() {
    let ids: Vec<u64> = (0..4).map(|_| std::thread::spawn(this_thread_id).join().unwrap()).collect();
    let mut distinct = ids.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), ids.len(), "CLAIM: two threads got one id: {ids:?}");
    assert!(ids.iter().all(|&i| i != 0), "CLAIM: a thread id is 0: {ids:?}");
    #[cfg(target_os = "linux")]
    {
        let link = std::fs::read_link("/proc/thread-self").expect("premise: /proc/thread-self is readable");
        let tid: u64 = link.file_name().and_then(|f| f.to_str()).and_then(|f| f.parse().ok()).expect("premise: <pid>/task/<tid>");
        assert_eq!(this_thread_id(), tid, "CLAIM: this_thread_id is not the kernel thread id (link {link:?})");
    }
}

/// getppid by raw syscall(2), the one call the shim cannot see: BSD/macOS number 39; on Linux the
/// platform's own SYS_getppid (39 there is getpid on x86_64 and umount2 on aarch64: integ-rev-1017).
#[cfg(target_vendor = "apple")]
const RAW_GETPPID: libc::c_int = 39;
#[cfg(not(target_vendor = "apple"))]
const RAW_GETPPID: libc::c_long = libc::SYS_getppid;

/// The instruments' fire-checks, in the child (nothing else runs there): each counter moves by
/// exactly what was done, and by nothing when nothing was.
fn run_instruments(cell: &str) -> String {
    let mut out = String::new();
    let mut put = |name: &str, s: &Sample| line(&mut out, cell, name, 0, s);
    let window = |f: &mut dyn FnMut()| {
        let s0 = probe::begin();
        f();
        let s1 = probe::end();
        sample_of(&s0, &s1, &Outside::none(), &Outside::none())
    };
    put("fc_nothing", &window(&mut || {}));
    let mut loaded = Sample::new();
    loaded.insert("loaded", u64::from(probe::per_thread_counter_loaded()));
    put("fc_shim_loaded", &loaded);
    // Class (b)'s attribution by thread, forced both ways. A helper thread that exists before the
    // window waits on an atomic, then makes 10 syscalls; the window must refuse its count.
    let helper = |declared: bool, closes: usize| {
        use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
        let go = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let (g, d, r) = (go.clone(), done.clone(), ready.clone());
        let h = std::thread::spawn(move || {
            // Ready (declared, when it is) before the window opens; done, then lingering until the
            // window has closed (`go` cleared), so its declaration covers the whole window.
            let body = || {
                r.store(true, Release);
                while !g.load(Acquire) {
                    std::hint::spin_loop();
                }
                for _ in 0..closes {
                    // SAFETY: closing an invalid descriptor only fails (EBADF): one syscall each.
                    unsafe { libc::close(-1) };
                }
                d.store(true, Release);
                while g.load(Acquire) {
                    std::hint::spin_loop();
                }
            };
            if declared {
                probe::spinning(body)
            } else {
                body()
            }
        });
        while !ready.load(Acquire) {
            std::hint::spin_loop();
        }
        (go, done, h)
    };
    let wait = |done: &std::sync::atomic::AtomicBool| {
        while !done.load(std::sync::atomic::Ordering::Acquire) {
            std::hint::spin_loop();
        }
    };
    {
        let (go, done, h) = helper(false, 10);
        put(
            "fc_foreign_window",
            &window(&mut || {
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
            }),
        );
        go.store(false, std::sync::atomic::Ordering::Release);
        h.join().unwrap();
    }
    {
        // A declared spinner making no syscall does not make the window foreign.
        let (go, done, h) = helper(true, 0);
        put(
            "fc_spinner_window",
            &window(&mut || {
                for _ in 0..7 {
                    // SAFETY: as above.
                    unsafe { libc::close(-1) };
                }
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
            }),
        );
        go.store(false, std::sync::atomic::Ordering::Release);
        h.join().unwrap();
    }
    put(
        "fc_spawned_window",
        &window(&mut || {
            // A thread the window spawns is the window's own: its 5 syscalls are counted.
            std::thread::spawn(|| {
                for _ in 0..5 {
                    // SAFETY: as above.
                    unsafe { libc::close(-1) };
                }
            })
            .join()
            .unwrap();
        }),
    );
    {
        let (go, done, h) = helper(false, 10);
        put(
            "fc_foreign_hold",
            &window(&mut || {
                probe::arm(true);
                probe::store_locked();
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
                probe::store_unlocked();
                probe::arm(false);
            }),
        );
        go.store(false, std::sync::atomic::Ordering::Release);
        h.join().unwrap();
    }
    // Probe-adversary items 2-4 (artie reviews/ADV-claude-budget-probe-04385d32e.md), forced both
    // ways. (a) A foreign thread that runs (an undeclared spin) and makes NO syscall while the
    // window makes 7: the shim path may accept it, at exactly 7; without the shim it is refused.
    {
        let (go, done, h) = helper(false, 0);
        put(
            "fc_shim_quiet_foreign",
            &window(&mut || {
                for _ in 0..7 {
                    // SAFETY: as above.
                    unsafe { libc::close(-1) };
                }
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
                // Let the helper's spin run inside the window for a while: 30 ms, past a scheduler
                // quantum (at 2 ms the helper's CPU time read unmoved in 1 of 4 runs: a running
                // thread's time is not always current in thread_info, the blind spot `attribute`
                // states).
                let t = std::time::Instant::now();
                while t.elapsed() < std::time::Duration::from_millis(30) {
                    std::hint::spin_loop();
                }
            }),
        );
        go.store(false, std::sync::atomic::Ordering::Release);
        h.join().unwrap();
    }
    // (b) Row 6, the compensation: the window makes one call the shim cannot see (a raw syscall(2))
    // and 5 it can, while a foreign thread makes 10. The shim's total can then equal the kernel's;
    // a foreign thread with calls must refuse it in every mode.
    {
        let (go, done, h) = helper(false, 10);
        put(
            "fc_shim_compensation",
            &window(&mut || {
                // SAFETY: getppid (RAW_GETPPID) through syscall(2), which the shim does not
                // interpose (gen.py's SKIP): one syscall the shim cannot see.
                #[allow(deprecated)]
                unsafe {
                    libc::syscall(RAW_GETPPID)
                };
                for _ in 0..5 {
                    // SAFETY: as above.
                    unsafe { libc::close(-1) };
                }
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
            }),
        );
        go.store(false, std::sync::atomic::Ordering::Release);
        h.join().unwrap();
    }
    // (c) A thread present at the window's start that is gone at its end: refused in every mode.
    {
        let (go, done, h) = helper(false, 3);
        let mut h = Some(h);
        put(
            "fc_gone_thread",
            &window(&mut || {
                go.store(true, std::sync::atomic::Ordering::Release);
                wait(&done);
                go.store(false, std::sync::atomic::Ordering::Release);
                // Joined inside the window: present at the start, gone at the end.
                if let Some(h) = h.take() {
                    h.join().unwrap();
                }
            }),
        );
    }
    // (d) More threads than the 48 the table has room for: refused in every mode. The threads the
    // table has no room for make calls inside the window (probe-adversary-2 M2: with none, the
    // mutant `probe_full_table_trusted` kept the window's own 3, the truth, and died only of the
    // refusal rule; with them it keeps a wrong count).
    {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::{Acquire, Release}};
        let stop = Arc::new(AtomicBool::new(false));
        let ids: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let unlisted: Arc<std::sync::Mutex<std::collections::HashSet<u64>>> = Arc::default();
        let roles = Arc::new(AtomicBool::new(false));
        let go = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        // Listed workers park (no CPU time in the window); the unlisted ones spin on `go` with no
        // call, then make 10 each: invisible to a table read as complete.
        let parked: Vec<_> = (0..50)
            .map(|_| {
                let (stop, ids, unlisted, roles, go, done) = (stop.clone(), ids.clone(), unlisted.clone(), roles.clone(), go.clone(), done.clone());
                std::thread::spawn(move || {
                    let me = this_thread_id();
                    ids.lock().unwrap().push(me);
                    while !roles.load(Acquire) {
                        std::thread::park_timeout(std::time::Duration::from_millis(1));
                    }
                    if unlisted.lock().unwrap().contains(&me) {
                        while !go.load(Acquire) {
                            std::hint::spin_loop();
                        }
                        for _ in 0..10 {
                            // SAFETY: as above.
                            unsafe { libc::close(-1) };
                        }
                        done.fetch_add(1, Release);
                    }
                    while !stop.load(Acquire) {
                        std::thread::park();
                    }
                })
            })
            .collect();
        while ids.lock().unwrap().len() < 50 {
            std::thread::yield_now();
        }
        // The workers the thread table lists past its 48 slots.
        let mut table = [probe::ThreadCpu::EMPTY; 128];
        let n = probe::thread_table(&mut table).unwrap_or(0);
        let mine: std::collections::HashSet<u64> = ids.lock().unwrap().iter().copied().collect();
        let past: std::collections::HashSet<u64> = table[48.min(n)..n.min(128)].iter().map(|t| t.id).filter(|id| mine.contains(id)).collect();
        let n_past = past.len() as u64;
        *unlisted.lock().unwrap() = past;
        roles.store(true, Release);
        // The listed workers back in `park()` before the window opens.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let mut r = window(&mut || {
            for _ in 0..3 {
                // SAFETY: as above.
                unsafe { libc::close(-1) };
            }
            go.store(true, Release);
            while done.load(Acquire) < n_past {
                std::hint::spin_loop();
            }
        });
        r.insert("unlisted_callers", n_past);
        put("fc_too_many_threads", &r);
        stop.store(true, Release);
        for t in parked {
            t.thread().unpark();
            t.join().unwrap();
        }
    }
    // (e) probe-adversary-2 M1: a thread the window spawns that is still running at its end makes
    // more calls after it; the window is refused (it was Some(own + the thread's calls so far)).
    {
        use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
        let release = Arc::new(AtomicBool::new(false));
        let r2 = release.clone();
        let mut handle = None;
        put(
            "fc_spawned_outlives",
            &window(&mut || {
                let r2 = r2.clone();
                handle = Some(std::thread::spawn(move || {
                    for _ in 0..5 {
                        // SAFETY: as above.
                        unsafe { libc::close(-1) };
                    }
                    // Still RUNNING at the window's end (a spin, no call): a spawned thread blocked
                    // at the end is the window's (`probe::attribute`).
                    while !r2.load(Acquire) {
                        std::hint::spin_loop();
                    }
                    for _ in 0..10 {
                        // SAFETY: as above.
                        unsafe { libc::close(-1) };
                    }
                }));
                std::thread::sleep(std::time::Duration::from_millis(5));
            }),
        );
        release.store(true, Release);
        if let Some(h) = handle {
            h.join().unwrap();
        }
    }
    // Counters review 4 MED-1: a window refused as ThreadNew whose own thread does nothing (a helper
    // that existed before it spawns a thread still running at its end), so its allocation counts are
    // asserted and a ThreadNew diagnostic (FE_BUDGET_NEW_NAMES) inside the counts is seen.
    {
        use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
        let (go, spawned, release, ready) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
        let (g, sp, rl, r) = (go.clone(), spawned.clone(), release.clone(), ready.clone());
        let helper = std::thread::spawn(move || {
            r.store(true, Release);
            while !g.load(Acquire) {
                std::hint::spin_loop();
            }
            let rl2 = rl.clone();
            // Counters review 5: the window closes only once `late` is running, not merely spawned (a
            // thread not yet scheduled may read as waiting, and the window would be refused as foreign).
            let running = Arc::new(AtomicBool::new(false));
            let run2 = running.clone();
            let late = std::thread::spawn(move || {
                run2.store(true, Release);
                // Still RUNNING at the window's end: a spin, no call.
                while !rl2.load(Acquire) {
                    std::hint::spin_loop();
                }
            });
            while !running.load(Acquire) {
                std::hint::spin_loop();
            }
            sp.store(true, Release);
            late.join().unwrap();
        });
        while !ready.load(Acquire) {
            std::hint::spin_loop();
        }
        put(
            "fc_thread_new_quiet",
            &window(&mut || {
                go.store(true, Release);
                while !spawned.load(Acquire) {
                    std::hint::spin_loop();
                }
            }),
        );
        release.store(true, Release);
        helper.join().unwrap();
    }
    // (f) probe-adversary-2 M3: a foreign thread makes ONE call the shim cannot see (syscall(2)) and
    // none it can, while the window makes 5 it can: every foreign shim delta is 0 and only the sum
    // check refuses (5 against the kernel's 6). Mutant `probe_sum_unchecked`.
    {
        use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
        let go = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let (g, d, rd) = (go.clone(), done.clone(), ready.clone());
        let h = std::thread::spawn(move || {
            rd.store(true, Release);
            while !g.load(Acquire) {
                std::hint::spin_loop();
            }
            // SAFETY: getppid (RAW_GETPPID) through syscall(2): one call the shim cannot see.
            #[allow(deprecated)]
            unsafe {
                libc::syscall(RAW_GETPPID)
            };
            d.store(true, Release);
            while g.load(Acquire) {
                std::hint::spin_loop();
            }
        });
        while !ready.load(Acquire) {
            std::hint::spin_loop();
        }
        put(
            "fc_shim_unseen_foreign",
            &window(&mut || {
                for _ in 0..5 {
                    // SAFETY: as above.
                    unsafe { libc::close(-1) };
                }
                go.store(true, Release);
                while !done.load(Acquire) {
                    std::hint::spin_loop();
                }
                let t = std::time::Instant::now();
                while t.elapsed() < std::time::Duration::from_millis(30) {
                    std::hint::spin_loop();
                }
            }),
        );
        go.store(false, Release);
        h.join().unwrap();
    }
    put(
        "fc_close_37",
        &window(&mut || {
            for _ in 0..37 {
                // SAFETY: closing an invalid descriptor only fails (EBADF): one syscall each.
                unsafe { libc::close(-1) };
            }
        }),
    );
    put(
        "fc_threads_1000",
        &window(&mut || {
            for _ in 0..1000 {
                std::hint::black_box(probe::threads());
            }
        }),
    );
    put(
        "fc_box_23",
        &window(&mut || {
            for i in 0..23u64 {
                std::hint::black_box(Box::new(i));
            }
        }),
    );
    put(
        "fc_held_5_syscalls_3_allocs",
        &window(&mut || {
            probe::arm(true);
            probe::store_locked();
            for i in 0..5u64 {
                // SAFETY: as above.
                unsafe { libc::close(-1) };
                if i < 3 {
                    std::hint::black_box(Box::new(i));
                }
            }
            probe::store_unlocked();
            probe::arm(false);
        }),
    );
    // Live heap bytes: a 1 MiB buffer is resident while it lives, and not after; and the largest
    // single hold sees exactly what one hold allocated.
    let mut s = Sample::new();
    let l0 = probe::live_heap_bytes();
    let v: Vec<u8> = Vec::with_capacity(1 << 20);
    let l1 = probe::live_heap_bytes();
    drop(std::hint::black_box(v));
    let l2 = probe::live_heap_bytes();
    s.insert("live_while", u64::try_from(l1 - l0).unwrap_or(u64::MAX));
    s.insert("live_after", u64::try_from(l2 - l0).unwrap_or(u64::MAX));
    let l3 = probe::live_heap_bytes();
    let mut grown: Vec<u8> = Vec::with_capacity(16);
    grown.reserve_exact(1024);
    let l4 = probe::live_heap_bytes();
    let zeroed = std::hint::black_box(vec![0u8; 4096]);
    let l5 = probe::live_heap_bytes();
    drop((grown, zeroed));
    s.insert("live_realloc", u64::try_from(l4 - l3).unwrap_or(u64::MAX));
    s.insert("live_zeroed", u64::try_from(l5 - l4).unwrap_or(u64::MAX));
    let _ = probe::take_hold_maxima();
    probe::store_locked();
    for i in 0..3u64 {
        std::hint::black_box(Box::new(i));
    }
    probe::store_unlocked();
    // A nested hold is one hold: its outermost lock to its unlock.
    probe::store_locked();
    std::hint::black_box(Box::new(0u64));
    probe::store_locked();
    std::hint::black_box(Box::new(0u64));
    probe::store_unlocked();
    probe::store_unlocked();
    let (hold_bytes, hold_rows) = probe::take_hold_maxima().all;
    s.insert("max_hold_alloc_bytes", hold_bytes);
    s.insert("max_hold_catalog_rows", hold_rows);
    // A hold on a foreground-marked thread is not a background hold; one on another thread is.
    probe::mark_foreground(true);
    probe::store_locked();
    std::hint::black_box(Box::new([0u64; 8]));
    probe::store_unlocked();
    let threads_before = probe::threads();
    std::thread::spawn(|| {
        probe::store_locked();
        std::hint::black_box(Box::new([0u64; 2]));
        probe::store_unlocked();
    })
    .join()
    .unwrap();
    // A joined thread can still be exiting. The thread-count arm below reads its base next, so
    // this arm leaves only once its thread has left the count (base14 read base 3, after 2, when
    // it did not wait).
    let t = std::time::Instant::now();
    while probe::threads() != threads_before && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    s.insert("hold_thread_left", u64::from(probe::threads() == threads_before));
    probe::mark_foreground(false);
    let maxima = probe::take_hold_maxima();
    let ((bg_bytes, _), (all_bytes, _)) = (maxima.background, maxima.all);
    s.insert("bg_hold_alloc_bytes", bg_bytes);
    s.insert("all_hold_alloc_bytes", all_bytes);
    put("fc_live_and_hold", &s);
    // The thread count sees a thread that is alive, and stops seeing it once it has exited.
    let base = probe::threads();
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let parked = std::thread::spawn(move || rx.recv());
    let with = probe::threads();
    tx.send(()).unwrap();
    let _ = parked.join();
    // A joined thread can still be exiting: the count must come back, soon.
    let t = std::time::Instant::now();
    while probe::threads() != base && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let after = probe::threads();
    let mut s = Sample::new();
    s.insert("base", base.unwrap_or(0));
    s.insert("with", with.unwrap_or(0));
    s.insert("after", after.unwrap_or(0));
    // A parked thread the engine reported as a persistent store thread (`probe::persistent_started`,
    // as `start_confirm_writer` does; review 2 M6) is not counted, and a parked thread is blocked; a
    // spinning one is not.
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    probe::persistent_started();
    let named = std::thread::spawn(move || {
        let _ = rx.recv();
        probe::persistent_ended();
    });
    let t = std::time::Instant::now();
    while probe::others_blocked() != Some(true) && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    s.insert("with_persistent", probe::threads().unwrap_or(0));
    s.insert("parked_blocked", u64::from(probe::others_blocked() == Some(true)));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let spinning = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                std::hint::spin_loop();
            }
        })
    };
    let t = std::time::Instant::now();
    let mut seen_running = false;
    while t.elapsed() < std::time::Duration::from_millis(500) && !seen_running {
        seen_running = probe::others_blocked() == Some(false);
    }
    s.insert("spinning_seen_running", u64::from(seen_running));
    stop.store(true, std::sync::atomic::Ordering::Release);
    let _ = spinning.join();
    // Review 2 H1: a RUNNING persistent store thread (the confirmation writer at work) is not
    // blocked: the persistent count exempts such a thread from the thread count, never from the
    // blocked check.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    probe::persistent_started();
    let spinning_named = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                std::hint::spin_loop();
            }
            probe::persistent_ended();
        })
    };
    let t = std::time::Instant::now();
    let mut seen_running = false;
    while t.elapsed() < std::time::Duration::from_millis(500) && !seen_running {
        seen_running = probe::others_blocked() == Some(false);
    }
    s.insert("persistent_spinning_seen_running", u64::from(seen_running));
    stop.store(true, std::sync::atomic::Ordering::Release);
    let _ = spinning_named.join();
    tx.send(()).unwrap();
    let _ = named.join();
    put("fc_thread_count", &s);
    // A store API that takes the store mutex exactly once (`BranchStore::catalog_counters`); one
    // that reads every catalog page under it (`catalog_shape`); and the catalog's query counter
    // both ways: a lookup of a name the store does not hold in memory asks the catalog, one of a
    // name the filter has never seen does not.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fc.db");
    let incarnation = {
        let db = open_at(&path);
        let trunk = db.connect().unwrap();
        seed(&trunk, false);
        populate(&db, "pop", 0, 50, true);
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        db.incarnation
    };
    let db = open_at(&path);
    assert_ne!(db.incarnation, incarnation, "not a reopen");
    db.branch_wait_name_filter();
    put(
        "fc_one_store_lock",
        &window(&mut || {
            std::hint::black_box(db.branch_catalog_counters());
        }),
    );
    probe::arm(true);
    put(
        "fc_catalog_shape",
        &window(&mut || {
            std::hint::black_box(db.branch_catalog_shape().unwrap());
        }),
    );
    probe::arm(false);
    let base = probe::threads();
    let _ = probe::take_hold_maxima();
    let (_, mut s) = measure(&db, base, || db.branch_named("pop-7").unwrap());
    s.insert("max_hold_catalog_rows", probe::take_hold_maxima().all.1);
    put("fc_lookup_cataloged", &s);
    let (_, s) = measure(&db, base, || db.branch_named("never-seen").unwrap());
    put("fc_lookup_unseen", &s);
    // The catalog statement counter: a sharp checkpoint of a dirty store writes rows (through the
    // checkpoint writer's connection, on this thread), a lookup of an unseen name runs none.
    trunk_for_fc(&db);
    let (_, s) = measure(&db, base, || db.branch_compact_now().unwrap());
    put("fc_checkpoint_writes", &s);
    // The log counter: a create appends, a lookup does not.
    let trunk = db.connect().unwrap();
    let (_, s) = measure(&db, base, || trunk.create_branch("fc-new").unwrap());
    put("fc_create_logs", &s);
    // The SQL-layer counters (engine 2b), on this thread: three statements prepared count three;
    // a scan of `t` reads its page through the pager, and an empty window reads nothing; a schema
    // re-read under a read transaction (the 2b fix's own sequence) parses every `sqlite_schema` row
    // once; an autocommit INSERT takes the trunk's WAL write lock once and reads and allocates under
    // it, and lets it go; a SELECT after it reads pages, none of them under the lock.
    let sql = |f: &mut dyn FnMut()| {
        let a = probe::sql_counts();
        f();
        let b = probe::sql_counts();
        sql_sample(&a, &b)
    };
    put("fc_sql_nothing", &sql(&mut || {}));
    // Review 2 M4: execute and query reach the compiler too; review 2 M3: the sleep counter counts
    // crate::thread::sleep and (its blind spot, shown) not std's.
    put("fc_sql_execute", &sql(&mut || exec(&trunk, "SELECT 1")));
    put(
        "fc_sql_query",
        &sql(&mut || {
            std::hint::black_box(trunk.query("SELECT 1").unwrap());
        }),
    );
    let z0 = probe::sleeps();
    crate::thread::sleep(std::time::Duration::from_millis(1));
    let z1 = probe::sleeps();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let z2 = probe::sleeps();
    put("fc_sleeps", &Sample::from([("crate_sleep", delta(z0, z1)), ("std_sleep", delta(z1, z2))]));
    put(
        "fc_sql_prepare_3",
        &sql(&mut || {
            for _ in 0..3 {
                std::hint::black_box(trunk.prepare("SELECT 1").unwrap());
            }
        }),
    );
    put("fc_sql_scan", &sql(&mut || {
        std::hint::black_box(query_int(&trunk, "SELECT count(*) FROM t"));
    }));
    let mut s = sql(&mut || {
        let pager = trunk.pager.load();
        pager.begin_read_tx().unwrap();
        trunk.set_tx_state(TransactionState::Read);
        let reread = trunk.reparse_schema();
        trunk.set_tx_state(TransactionState::None);
        pager.end_read_tx();
        reread.unwrap();
    });
    s.insert("schema_table_rows", query_int(&trunk, "SELECT count(*) FROM sqlite_schema") as u64);
    put("fc_sql_reparse", &s);
    let mut s = sql(&mut || exec(&trunk, "INSERT INTO t VALUES (1000, 'fc')"));
    s.insert("held_after", u64::from(probe::wal_lock_held()));
    put("fc_sql_insert", &s);
    put("fc_sql_select_after", &sql(&mut || {
        std::hint::black_box(query_int(&trunk, "SELECT count(*) FROM t"));
    }));
    // Review 2 L4: inside an open write transaction a prepare and a schema re-read count under the
    // WAL write lock (wal_prepares and wal_schema_rows had never been seen non-zero), and a ROLLBACK
    // lets the lock go (held_after had been checked only after a commit).
    exec(&trunk, "BEGIN");
    let mut s = sql(&mut || {
        exec(&trunk, "INSERT INTO t VALUES (1001, 'fc-rollback')");
        std::hint::black_box(trunk.prepare("SELECT 1").unwrap());
        trunk.reparse_schema().unwrap();
    });
    s.insert("held_before_rollback", u64::from(probe::wal_lock_held()));
    exec(&trunk, "ROLLBACK");
    s.insert("held_after", u64::from(probe::wal_lock_held()));
    s.insert("rolled_back", u64::from(query_int(&trunk, "SELECT count(*) FROM t WHERE id = 1001") == 0));
    put("fc_sql_rollback", &s);
    // Review 2 L6: a statement compiled while this thread holds a store mutex counts as held, one
    // compiled outside does not (the guard taken through the probe's own hook pair).
    put(
        "fc_sql_held_prepare",
        &sql(&mut || {
            std::hint::black_box(trunk.prepare("SELECT 1").unwrap());
            probe::store_locked();
            std::hint::black_box(trunk.prepare("SELECT 2").unwrap());
            probe::store_unlocked();
        }),
    );
    // Review 2 L3: the confirmation writer's park state, forced both ways. Unheld, with the pending
    // word taken away first (a checkpoint, as the confirm arm does: a landing on a pending word wakes
    // no one), a create's word sends the writer to its own test hold mid-write
    // (`HOLD_CONFIRM_WRITE`): there it is not parked, and `writer_settled` must say so; released, it
    // writes the word and parks with nothing pending.
    hold_word(false);
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    db.branches.confirm_hold_for_test(super::store::HOLD_CONFIRM_WRITE);
    trunk.create_branch("fc-park").unwrap();
    let t = std::time::Instant::now();
    while db.branches.confirm_held_for_test() != (super::store::HOLD_CONFIRM_WRITE | super::store::HOLD_ARRIVED) {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the confirmation writer never reached its write");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let mid_write = writer_settled(&db, std::time::Duration::from_millis(50));
    db.branches.confirm_hold_for_test(0);
    let after = writer_settled(&db, std::time::Duration::from_secs(5));
    hold_word(true);
    put(
        "fc_writer_park",
        &Sample::from([
            ("live", u64::try_from(probe::persistent_live()).unwrap_or(0)),
            ("settled_mid_write", u64::from(mid_write)),
            ("settled_after", u64::from(after)),
        ]),
    );
    // Review C14 (MED): the held case, with the writer delayed between a notification and its
    // wait's return. After the arm above the writer is parked with no word pending; it is suspended
    // (Mach `thread_suspend`, outside any window), and a held create's landing notifies it. It
    // cannot wake, so its parked flag still reads true from the wait it entered before the create:
    // `writer_settled` must refuse that stale park. Resumed, it takes the word, enters the held
    // quiet period's wait, and is settled. The create runs on a thread of its own, bounded, so a
    // writer suspended while holding the group's lock (a spurious wake just before the suspend)
    // cannot hang the cell: the record says whether the create had landed before the check.
    #[cfg(target_vendor = "apple")]
    {
        let parked_before = writer_settled(&db, std::time::Duration::from_secs(5));
        let suspended = probe::suspend_thread_named("branch-confirm");
        let found = suspended.is_some();
        let creating = {
            let trunk = trunk.clone();
            std::thread::spawn(move || trunk.create_branch("fc-stale-park").map(|_| ()).map_err(|e| e.to_string()))
        };
        let t = std::time::Instant::now();
        while !creating.is_finished() && t.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let landed = creating.is_finished();
        let stale = writer_settled(&db, std::time::Duration::from_millis(50));
        drop(suspended);
        creating.join().unwrap().unwrap_or_else(|e| panic!("fc_writer_stale_park: the create failed: {e}"));
        let resumed = writer_settled(&db, std::time::Duration::from_secs(5));
        put(
            "fc_writer_stale_park",
            &Sample::from([
                ("parked_before", u64::from(parked_before)),
                ("found", u64::from(found)),
                ("landed_before_check", u64::from(landed)),
                ("settled_stale", u64::from(stale)),
                ("settled_resumed", u64::from(resumed)),
            ]),
        );
    }
    // Review C14: the settle check's own cost, uncontended: a thousand checks of a parked writer move
    // no counter that a window charges (a try-lock the writer contends for meanwhile may: the
    // accessor's named blind spot).
    put(
        "fc_writer_settled_cost",
        &window(&mut || {
            for _ in 0..1000 {
                std::hint::black_box(writer_settled(&db, std::time::Duration::ZERO));
            }
        }),
    );
    // Counters review 4 MED-1: one harness window with nothing in it (`measure`, an empty `f`): the
    // harness's own in-window work (the thread count, the declared spin, the confirmation absorb, the
    // settle waits, the confirm counts) is inside the counts, so it must move none of them.
    put("fc_measure_empty", &measure(&db, base, || {}).1);
    out
}

/// The child: runs one cell when the parent spawned this binary for it (`FE_BUDGET_CHILD` names the
/// cell, `FE_BUDGET_OUT` the file it writes), and returns at once otherwise.
#[test]
fn budget_child() {
    let (Ok(spec), Ok(out)) = (std::env::var("FE_BUDGET_CHILD"), std::env::var("FE_BUDGET_OUT")) else {
        return;
    };
    let k = env_u64("FE_BUDGET_K", 24);
    // The flight confirmation word (review 6 #1): held back in every cell but `confirm_n10`
    // (`hold_word`). base12 counted it where it was written instead, and the writer's wake and wait
    // then landed inside the operation's own store-mutex holds as the timing made (first write's
    // syscalls 17..19, held syscalls 4..6, in one cell).
    hold_word(spec != "confirm_n10" && spec != "confirm_load");
    let mut text = String::new();
    if spec == "open_writer_d2" {
        open_writer(&out);
    }
    if spec == "instruments" {
        text = run_instruments(&spec);
    } else if spec.starts_with("open_d") || spec.starts_with("open_cat") {
        open_flush(&spec, &mut text);
    } else if let Some(&(_, tables)) = SCHEMA_TABLES.iter().find(|(c, _)| *c == spec) {
        schema_window(&spec, tables, &mut text);
    } else if spec == "first_create" {
        first_create(&spec, &mut text);
    } else {
        // `n10_*` / `n1e4_*` cells: `_small` or `_large`, then `_unnamed` (recovery only) and `_d0`.
        let (n, large, named) = match spec.as_str() {
            "ckpt_n10" | "exc_sharp" | "confirm_n10" | "confirm_load" | "shared_c1" | "shared_c8" | "shared_c64" | "shared_c256" | "shared_c1024" => {
                (10, false, true)
            }
            "growth_n1e4" | "ckpt_n1e4" | "mem_n1e4" => (N_LARGE, false, true),
            "mem_n1e5" => (N_HUGE, false, true),
            cell => {
                let n = if cell.starts_with("n10_") {
                    10
                } else if cell.starts_with("n1e4_") {
                    N_LARGE
                } else {
                    panic!("unknown budget cell {cell:?}")
                };
                (n, cell.contains("_large"), !cell.contains("_unnamed"))
            }
        };
        let mut built = build(&spec, n, large, named);
        // Review 2 L7: the shared-flight cells read no held-syscall count, so they run unarmed (two
        // Mach traps per hold inside the measured mutex otherwise).
        // `FE_BUDGET_UNARMED` (diagnostic): any cell unarmed, to tell the probe's own per-hold reads
        // from the code's cost.
        probe::arm(!spec.starts_with("shared_") && std::env::var_os("FE_BUDGET_UNARMED").is_none());
        let opens = if spec.starts_with("n") { 5 } else { 1 };
        if spec.starts_with("mem_") {
            memory(&spec, &mut built, &mut text);
            probe::arm(false);
            std::fs::write(&out, format!("{text}done cell={spec}\n")).unwrap();
            return;
        }
        let (db, base) = recover(&spec, &mut built, opens, &mut text);
        if spec == "confirm_load" {
            confirm_load(&spec, &db, &mut text);
        } else if spec == "confirm_n10" {
            confirm(&spec, &db, base, k, &mut text);
        } else if let Some(c) = spec.strip_prefix("shared_c") {
            shared(&spec, &db, c.parse().unwrap(), &mut text);
        } else if spec.starts_with("growth") {
            growth(&spec, &db, &mut text);
        } else if spec.starts_with("ckpt") || spec == "exc_sharp" {
            checkpoints(&spec, &db, base, &mut text);
        } else if named {
            rounds(&spec, &db, base, k, &mut text);
        }
        probe::arm(false);
    }
    std::fs::write(&out, format!("{text}done cell={spec}\n")).unwrap();
}

// ---- the parent: one child per cell, shared by every test ----

/// A cell's samples by operation, in the order measured.
struct CellData {
    name: String,
    ops: BTreeMap<String, Vec<BTreeMap<String, u64>>>,
}

fn run_child(spec: &str) -> CellData {
    let exe = std::env::current_exe().unwrap();
    let out = std::env::temp_dir().join(format!("fe-budget-{}-{spec}.txt", std::process::id()));
    let _ = std::fs::remove_file(&out);
    // The child's stdout and stderr go to files and it is waited for with a deadline
    // (`FE_BUDGET_CHILD_TIMEOUT_S`, default 1800): a child that hangs is killed and its cell fails,
    // instead of stalling every test behind it.
    let log = |kind: &str| std::env::temp_dir().join(format!("fe-budget-{}-{spec}.{kind}", std::process::id()));
    let (stdout_path, stderr_path) = (log("stdout"), log("stderr"));
    let mut child = std::process::Command::new(exe)
        .args(["branch::budget_tests::budget_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_BUDGET_CHILD", spec)
        .env("FE_BUDGET_OUT", &out)
        .stdout(std::fs::File::create(&stdout_path).unwrap())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(env_u64("FE_BUDGET_CHILD_TIMEOUT_S", 1800));
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let stdout = std::fs::read_to_string(&stdout_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let _ = (std::fs::remove_file(&stdout_path), std::fs::remove_file(&stderr_path));
    let text = std::fs::read_to_string(&out).unwrap_or_default();
    if let Ok(dir) = std::env::var("FE_BUDGET_RAW_DIR") {
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(Path::new(&dir).join(format!("{spec}.txt")), &text).unwrap();
    }
    let _ = std::fs::remove_file(&out);
    assert!(
        status.is_some_and(|s| s.success()) && text.ends_with(&format!("done cell={spec}\n")),
        "budget cell {spec}: the child failed ({}):\n{stdout}\n{stderr}",
        status.map_or("killed at its deadline".to_string(), |s| s.to_string()),
    );
    let mut ops: BTreeMap<String, Vec<BTreeMap<String, u64>>> = BTreeMap::new();
    for l in text.lines().filter(|l| l.starts_with("sample ")) {
        let mut op = None;
        let mut s = BTreeMap::new();
        for kv in l.split(' ').skip(1) {
            let (k, v) = kv.split_once('=').unwrap();
            match k {
                "cell" | "i" => {}
                "op" => op = Some(v.to_string()),
                _ => {
                    s.insert(k.to_string(), v.parse().unwrap());
                }
            }
        }
        ops.entry(op.unwrap()).or_default().push(s);
    }
    CellData { name: spec.to_string(), ops }
}

fn cell(spec: &str) -> Arc<CellData> {
    static CELLS: std::sync::OnceLock<std::sync::Mutex<BTreeMap<String, Arc<CellData>>>> =
        std::sync::OnceLock::new();
    let cells = CELLS.get_or_init(Default::default);
    // Held while the child runs: two tests needing one cell run it once.
    let data = {
        let mut cells = cells.lock().unwrap_or_else(|e| e.into_inner());
        cells.entry(spec.to_string()).or_insert_with(|| Arc::new(run_child(spec))).clone()
    };
    // DECISIONS bd0d845f97: a credited cell has all four counted exceptions at 0 inside every
    // window; one that moved one, or a window that did not record them, is refused before any budget
    // reads the cell. The instruments' cell reads no database (its fire-checks are of the probes).
    if spec != "instruments" {
        let bad = uncredited(&data);
        assert!(
            bad.is_empty(),
            "budget cell {spec}: not credited (DECISIONS bd0d845f97: a credited cell needs {EXC_KEYS:?} at 0 inside every window):\n{bad}"
        );
    }
    data
}

/// The windows of `c` that moved a counted exception (`{key} moved by {n}`) or did not record one
/// (`no {key} recorded`: a counter not read is not a zero), one line each.
fn uncredited(c: &CellData) -> String {
    let mut bad = String::new();
    for (op, samples) in &c.ops {
        for (i, s) in samples.iter().enumerate() {
            for key in EXC_KEYS {
                match s.get(key) {
                    None => {
                        let _ = writeln!(bad, "  {op} #{i}: no {key} recorded");
                    }
                    Some(&0) => {}
                    Some(n) => {
                        let _ = writeln!(bad, "  {op} #{i}: {key} moved by {n}");
                    }
                }
            }
        }
    }
    bad
}

/// Whether this process is a budget child (whose parent tests must not run: they would spawn
/// children of their own).
fn in_child() -> bool {
    std::env::var_os("FE_BUDGET_CHILD").is_some()
}

/// The cells every per-operation budget is checked at: 10 live branches in a small database (the
/// reference), 10^4 in a small one, 10 in a large one.
const MAIN: [&str; 3] = ["n10_small", "n1e4_small", "n10_large"];

type Map = BTreeMap<String, u64>;

/// `op`'s quiet samples in `c`. Refuses when there are none, or when fewer than half are quiet: a
/// budget read from a few samples is not one.
fn quiet<'a>(c: &'a CellData, op: &str) -> Vec<&'a Map> {
    let all = c.ops.get(op).map(Vec::as_slice).unwrap_or_default();
    let quiet: Vec<&Map> = all.iter().filter(|s| s.get("quiet").copied().unwrap_or(1) == 1).collect();
    assert!(
        !quiet.is_empty() && 2 * quiet.len() >= all.len(),
        "{}: {op}: {} of {} samples quiet: not enough to read a budget from",
        c.name,
        quiet.len(),
        all.len()
    );
    quiet
}

/// `key` in every sample; refuses when one lacks it (a counter the run did not produce is not a
/// zero).
fn values(c: &CellData, op: &str, samples: &[&Map], key: &str) -> Vec<u64> {
    samples
        .iter()
        .map(|s| *s.get(key).unwrap_or_else(|| panic!("{}: {op}: no {key} counted", c.name)))
        .collect()
}

/// Which samples a budget binds.
#[derive(Clone, Copy)]
enum Stat {
    /// Every quiet sample.
    Every,
    /// The smallest: the operation's steady cost, without the amortized growth of a table that a
    /// few operations pay (allocation counters only; worst-case growth is its own budget).
    Steady,
}

/// A budget's limit.
#[derive(Clone, Copy)]
enum Want {
    Exactly(u64),
    AtMost(u64),
    /// At most the sample's own `key` plus a constant (a cost per page the operation resolved).
    AtMostPlus(&'static str, u64),
}

/// One budget: a counter of one operation, the samples it binds, its limit, and where the limit
/// comes from.
struct Budget {
    key: &'static str,
    stat: Stat,
    want: Want,
    why: &'static str,
}

const fn every(key: &'static str, want: Want, why: &'static str) -> Budget {
    Budget {
        key,
        stat: Stat::Every,
        want,
        why,
    }
}

const fn steady(key: &'static str, want: Want, why: &'static str) -> Budget {
    Budget {
        key,
        stat: Stat::Steady,
        want,
        why,
    }
}

/// Every budget of `op` at every main cell; all violations are reported together, with the values
/// at each cell.
fn assert_budgets(op: &str, budgets: &[Budget]) {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for spec in MAIN {
        let c = cell(spec);
        let samples = quiet(&c, op);
        for b in budgets {
            let vals = values(&c, op, &samples, b.key);
            let check = |s: &Map, v: u64| match b.want {
                Want::Exactly(n) => (v == n, format!("== {n}")),
                Want::AtMost(n) => (v <= n, format!("<= {n}")),
                Want::AtMostPlus(k, n) => {
                    let base = s[k];
                    (v <= base + n, format!("<= {k} ({base}) + {n}"))
                }
            };
            let bad: Vec<(u64, String)> = match b.stat {
                Stat::Every => samples
                    .iter()
                    .zip(&vals)
                    .filter_map(|(s, &v)| {
                        let (ok, want) = check(s, v);
                        (!ok).then_some((v, want))
                    })
                    .collect(),
                Stat::Steady => {
                    let (i, &v) = vals.iter().enumerate().min_by_key(|(_, v)| **v).unwrap();
                    let (ok, want) = check(samples[i], v);
                    if ok {
                        vec![]
                    } else {
                        vec![(v, want)]
                    }
                }
            };
            if let Some((v, want)) = bad.first() {
                let lo = vals.iter().min().unwrap();
                let hi = vals.iter().max().unwrap();
                let which = match b.stat {
                    Stat::Every => format!("{} of {} samples over budget", bad.len(), vals.len()),
                    Stat::Steady => format!("the steady value, over {} samples", vals.len()),
                };
                let _ = writeln!(
                    failures,
                    "  {spec}: {op}.{} = {v} (range {lo}..{hi}; {which}), budget {want} [{}]",
                    b.key,
                    b.why
                );
            }
        }
    }
    assert!(failures.is_empty(), "{op} is over budget:\n{failures}");
}

/// Counters whose every sample is deterministic: compared as (min, max).
const EXACT: [&str; 16] = [
    "syscalls",
    "held_syncs",
    "catalog_stmts",
    "catalog_writes",
    "fsync",
    "full_fsync",
    "barrier",
    "held_syscalls",
    "locks",
    "catalog_queries",
    "catalog_rows",
    "catalog_loads",
    "resolves",
    "slot_reads",
    "log_bytes",
    "ckpt_started",
];
/// Allocation counters: compared by their steady (smallest) value, since a table's amortized
/// growth lands on whichever operation crosses its capacity.
const ALLOC: [&str; 4] = ["allocs", "alloc_bytes", "held_allocs", "held_alloc_bytes"];
/// Instructions retired: the one counter that sees CPU work which allocates, locks and syscalls
/// nothing (an O(N) walk). Compared only between D0 cells: at D2 an operation's device flush puts
/// kernel work in its count that varied by up to 14% between processes (base1-base3, create
/// 338k-386k at the same cell), while without one the steady value of every operation varied by
/// under 4% (connect_named 822.8k-826.9k across cells and runs). Compared by the steady value
/// within this factor; an O(N) term at N = 10^4 shows far above it.
const INSTRUCTION_SLACK: f64 = 1.10;

/// `op` costs the same at `a` and at `b`: every exact counter's (min, max), every allocation
/// counter's steady value, and the steady instruction count within `INSTRUCTION_SLACK` (with the
/// empty window's instruction count beside it, the control). `skip` names counters not compared.
fn assert_same(op: &str, a: &str, b: &str, skip: &[&str]) {
    if in_child() {
        return;
    }
    let (ca, cb) = (cell(a), cell(b));
    let (sa, sb) = (quiet(&ca, op), quiet(&cb, op));
    let mut failures = String::new();
    let range = |c: &CellData, s: &[&Map], k: &str| {
        let v = values(c, op, s, k);
        (*v.iter().min().unwrap(), *v.iter().max().unwrap())
    };
    let present = |s: &[&Map], k: &str| s.first().is_some_and(|m| m.contains_key(k));
    for k in EXACT.iter().filter(|k| !skip.contains(k)) {
        if !present(&sa, k) && !present(&sb, k) {
            continue;
        }
        let (x, y) = (range(&ca, &sa, k), range(&cb, &sb, k));
        if x != y {
            let _ = writeln!(failures, "  {op}.{k}: {a} {}..{}, {b} {}..{}", x.0, x.1, y.0, y.1);
        }
    }
    for k in ALLOC.iter().filter(|k| !skip.contains(k)) {
        let (x, y) = (range(&ca, &sa, k).0, range(&cb, &sb, k).0);
        if x != y {
            let _ = writeln!(failures, "  {op}.{k} (steady): {a} {x}, {b} {y}");
        }
    }
    // Instructions only between D0 cells (see `opts`).
    let d0 = a.ends_with("_d0") && b.ends_with("_d0");
    if d0 && !skip.contains(&"instructions") && present(&sa, "instructions") {
        let (x, y) = (range(&ca, &sa, "instructions").0, range(&cb, &sb, "instructions").0);
        let ratio = y as f64 / x.max(1) as f64;
        if !(1.0 / INSTRUCTION_SLACK..=INSTRUCTION_SLACK).contains(&ratio) {
            let control = |c: &CellData| {
                c.ops.get("noop").and_then(|s| s.iter().filter_map(|m| m.get("instructions")).min().copied())
            };
            let _ = writeln!(
                failures,
                "  {op}.instructions (steady): {a} {x}, {b} {y} (x{ratio:.2}; empty window: {a} {:?}, {b} {:?})",
                control(&ca),
                control(&cb)
            );
        }
    }
    assert!(failures.is_empty(), "{op} differs between {a} and {b}:\n{failures}");
}

/// The syncs a D2 flush is on this platform: F_FULLFSYNC on Apple, fsync(2) elsewhere
/// (`journal::fsync_file`).
#[cfg(target_vendor = "apple")]
const FULL: &str = "full_fsync";
#[cfg(not(target_vendor = "apple"))]
const FULL: &str = "fsync";
#[cfg(target_vendor = "apple")]
const NOT_FULL: &str = "fsync";
#[cfg(not(target_vendor = "apple"))]
const NOT_FULL: &str = "full_fsync";

/// Frame sizes of the documented log format (journal.rs: `[len u32][crc u32][payload]`): a
/// `ForkNamed` of an `NAME_LEN`-byte name (tag, child, parent, name length, name), a one-page
/// `Commit` (tag, branch, count, page/slot/crc), a `Release` (tag, branch), and a flight's end frame
/// in format 11 (tag, flight length, crc: `END_FRAME_LEN`), the 17 B review 2 #8 asked for (format
/// 9's was 29 B).
const FORK_NAMED_FRAME: u64 = 8 + 1 + 8 + 8 + 4 + NAME_LEN;
const COMMIT_1_FRAME: u64 = 8 + 1 + 8 + 4 + 12;
const RELEASE_FRAME: u64 = 8 + 1 + 8;
const FLIGHT_END: u64 = 8 + 1 + 4 + 4;

// ---- the instruments ----

/// Class (b) (DECISIONS 994f7d41e0): the syscall counts are attributed by thread. A window in which
/// a thread that existed at its start made syscalls has NO syscall count and names the foreign
/// thread; a declared spinner making none, and a thread the window spawned, do not make it foreign
/// (the window keeps exactly its own 7, and the spawn's 5); a hold during which another thread
/// ran is unattributable, never counted as held. Forced both ways, in the instruments' child.
#[test]
fn the_budget_syscall_counts_are_attributed_by_thread() {
    if in_child() {
        return;
    }
    let c = cell("instruments");
    let row = |op: &str| c.ops.get(op).and_then(|s| s.first()).unwrap_or_else(|| panic!("{op}: no sample")).clone();
    // Two modes: the per-thread counter (`feshim`) loaded, which attributes a window another thread
    // ran in exactly (here the window's own 0, the helper's 10 excluded), or not, which refuses it.
    let shim = row("fc_shim_loaded")["loaded"] == 1;
    let foreign = row("fc_foreign_window");
    assert!(foreign["foreign_threads"] >= 1, "premise: the helper thread did not run inside the window: {foreign:?}");
    // FLAGGED, lead-ordered (probe-adversary item 3, DECISIONS newest): a foreign thread with
    // syscalls REFUSES the window in both modes. The shim used to exclude its calls (Some(0)).
    assert!(foreign.get("syscalls").is_none(), "CLAIM: a window another thread made syscalls in kept a syscall count (counter loaded: {shim}): {foreign:?}");
    assert_eq!(foreign["shim_loaded"], u64::from(shim), "CLAIM: the sample does not record the probe's mode: {foreign:?}");
    let quiet = row("fc_shim_quiet_foreign");
    assert!(quiet["foreign_threads"] >= 1, "premise: the spinning helper did not run inside the window: {quiet:?}");
    if shim {
        assert_eq!(quiet.get("syscalls").copied(), Some(7), "CLAIM: the shim path did not accept a window whose only foreign thread made no syscall, at its own 7: {quiet:?}");
    } else {
        assert!(quiet.get("syscalls").is_none(), "CLAIM: a window another thread ran in kept a count with no shim: {quiet:?}");
    }
    for op in ["fc_shim_compensation", "fc_gone_thread", "fc_too_many_threads", "fc_spawned_outlives", "fc_shim_unseen_foreign"] {
        let r = row(op);
        assert!(r.get("syscalls").is_none(), "CLAIM: {op} kept a syscall count (counter loaded: {shim}): {r:?}");
    }
    // Each refused for its own reason (probe-adversary-2 L4: `probe::Refusal`).
    let reason = |op: &str| row(op)["refusal"];
    assert!(row("fc_too_many_threads")["unlisted_callers"] >= 1, "premise: no thread past the table's 48 slots made a call");
    assert_eq!(reason("fc_too_many_threads"), probe::Refusal::Table as u64, "CLAIM: fc_too_many_threads refused for another reason");
    assert_eq!(reason("fc_gone_thread"), probe::Refusal::ThreadGone as u64, "CLAIM: fc_gone_thread refused for another reason");
    assert_eq!(reason("fc_spawned_outlives"), probe::Refusal::ThreadNew as u64, "CLAIM: fc_spawned_outlives refused for another reason");
    if shim {
        assert_eq!(reason("fc_shim_unseen_foreign"), probe::Refusal::SumMismatch as u64, "CLAIM: fc_shim_unseen_foreign refused for another reason");
    }
    let spinner = row("fc_spinner_window");
    assert_eq!(spinner.get("syscalls").copied(), Some(7), "CLAIM: a declared spinner made the window foreign, or the count moved: {spinner:?}");
    let spawned = row("fc_spawned_window");
    assert_eq!(spawned["foreign_threads"], 0, "CLAIM: a thread the window spawned was counted foreign: {spawned:?}");
    assert!(spawned.get("syscalls").is_some_and(|n| *n >= 5), "CLAIM: the spawned thread's 5 syscalls were not counted: {spawned:?}");
    let hold = row("fc_foreign_hold");
    // FLAGGED, lead-ordered (probe-adversary item 3): the helper makes syscalls inside the hold, so the
    // hold is unattributable in both modes (the shim used to attribute it, 0 held).
    assert_eq!(hold["held_unattributed"], 1, "CLAIM: a hold another thread made syscalls in was attributed (counter loaded: {shim}): {hold:?}");
    assert_eq!(hold["held_syscalls"], 0, "CLAIM: another thread's syscalls were counted as held: {hold:?}");
    let nothing = row("fc_nothing");
    assert_eq!((nothing["foreign_threads"], nothing["held_unattributed"]), (0, 0), "CLAIM: an empty window was not attributed: {nothing:?}");
}

/// Probe-adversary item 1: a run labelled a shim run (`FE_SHIM_RUN`, set by feshim-run) whose shim is
/// not loaded is refused, not measured as one. A child of this binary, with the label and with the
/// shim stripped, must panic in `probe::prime` naming the refusal; a child with neither must not.
#[test]
fn a_run_labelled_shim_without_the_shim_is_refused() {
    if std::env::var_os("FE_PRIME_CHILD").is_some() {
        probe::prime();
        return;
    }
    let run = |label: bool| {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["branch::budget_tests::a_run_labelled_shim_without_the_shim_is_refused", "--exact", "--nocapture", "--test-threads=1"])
            .env("FE_PRIME_CHILD", "1")
            .env_remove("DYLD_INSERT_LIBRARIES")
            .env_remove("FE_SHIM_RUN");
        if label {
            cmd.env("FE_SHIM_RUN", "1");
        }
        let out = cmd.output().unwrap();
        (out.status.success(), String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr))
    };
    let (ok, text) = run(true);
    assert!(!ok && text.contains("REFUSED: this run is labelled a shim run"), "CLAIM: a labelled run without the shim was not refused: {text}");
    let (ok, text) = run(false);
    assert!(ok, "premise: an unlabelled run without the shim primes: {text}");
}

/// Every counter the budgets read moves by exactly what was done, and not at all when nothing was
/// (forced both ways, in a child that runs nothing else).
#[test]
fn the_budget_counters_count_exactly_what_was_done() {
    if in_child() {
        return;
    }
    let c = cell("instruments");
    let get = |op: &str, k: &str| -> u64 {
        *c.ops.get(op).and_then(|s| s.first()).and_then(|s| s.get(k)).unwrap_or_else(|| panic!("{op}: no {k}"))
    };
    for k in ["allocs", "locks", "held_syscalls", "held_allocs", "fsync", "full_fsync", "barrier"] {
        assert_eq!(get("fc_nothing", k), 0, "an empty window moved {k}");
    }
    // Counters review 4 MED-1: windows whose counts are asserted and that reach the configured code: a
    // ThreadNew refusal and a foreign one (each diagnostic's trigger, under the flags of
    // `the_instruments_count_exactly_under_every_flag`) and a harness window with nothing in it. Each
    // window's own thread allocates nothing; any allocation counted is the instrument's or the
    // harness's.
    assert_eq!(get("fc_thread_new_quiet", "refusal"), probe::Refusal::ThreadNew as u64, "premise: fc_thread_new_quiet was not refused as ThreadNew");
    assert!(get("fc_foreign_window", "foreign_threads") >= 1, "premise: fc_foreign_window had no foreign thread");
    // The Linux port (2026-10-18): an empty harness window is quiet wherever the thread states can be
    // read. Off Apple no kernel syscall count exists, so quiet cannot rest on an attributed count there.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    assert_eq!(get("fc_measure_empty", "quiet"), 1, "CLAIM: an empty harness window is not quiet, so no cell of this platform can pass");
    for op in ["fc_thread_new_quiet", "fc_foreign_window", "fc_measure_empty"] {
        for k in ["allocs", "alloc_bytes", "held_allocs"] {
            assert_eq!(get(op, k), 0, "{op}: a window whose own work allocates nothing moved {k}: the instrument or the harness allocated inside it");
        }
    }
    assert_eq!(get("fc_box_23", "allocs"), 23, "23 boxes");
    assert_eq!(get("fc_box_23", "frees"), 23, "23 boxes dropped");
    assert_eq!(get("fc_box_23", "free_bytes"), 23 * 8, "23 boxes of a u64 dropped");
    assert_eq!(get("fc_nothing", "frees"), 0, "an empty window freed");
    assert_eq!(get("fc_box_23", "alloc_bytes"), 23 * 8, "23 boxes of a u64");
    assert_eq!(get("fc_box_23", "held_allocs"), 0, "allocations outside a store mutex counted as held");
    assert_eq!(get("fc_held_5_syscalls_3_allocs", "locks"), 1, "one hold");
    assert_eq!(get("fc_held_5_syscalls_3_allocs", "held_allocs"), 3, "three allocations while held");
    // Counters review 5: `arm(true)` primes inside these windows, so what they allocate outside the
    // hold is the probe's own: primed, nothing.
    assert_eq!(get("fc_held_5_syscalls_3_allocs", "allocs"), 3, "allocations outside the hold (arm's prime, inside the window)");
    assert_eq!(get("fc_foreign_hold", "allocs"), 0, "fc_foreign_hold's own thread allocated (arm's prime, inside the window)");
    assert_eq!(get("fc_one_store_lock", "locks"), 1, "catalog_counters takes the store mutex once");
    assert!(get("fc_catalog_shape", "locks") >= 1, "the catalog walk took no store mutex");
    assert!(get("fc_lookup_cataloged", "catalog_queries") >= 1, "a cataloged name's lookup asked nothing");
    assert_eq!(get("fc_lookup_unseen", "catalog_queries"), 0, "a name the filter never saw was asked for");
    assert!(get("fc_create_logs", "log_bytes") > 0, "a create appended nothing to the log");
    assert_eq!(get("fc_lookup_unseen", "log_bytes"), 0, "a lookup appended to the log");
    assert_eq!(get("fc_lookup_unseen", "catalog_stmts"), 0, "a lookup of an unseen name ran a catalog statement");
    assert!(get("fc_lookup_cataloged", "catalog_stmts") >= 1, "a cataloged name's lookup ran no catalog statement");
    assert!(get("fc_lookup_cataloged", "catalog_rows_touched") >= 1, "a cataloged name's lookup read no catalog row");
    assert!(
        get("fc_lookup_cataloged", "max_hold_catalog_rows") >= 1,
        "a lookup made under the store mutex read no catalog row inside a hold"
    );
    assert_eq!(get("fc_lookup_unseen", "catalog_rows_touched"), 0, "a lookup of an unseen name read a catalog row");
    assert_eq!(get("fc_lookup_cataloged", "catalog_writes"), 0, "a lookup wrote the catalog");
    assert!(get("fc_checkpoint_writes", "catalog_writes") >= 1, "a sharp checkpoint of a dirty store wrote no catalog row");
    assert_eq!(get("fc_thread_count", "with"), get("fc_thread_count", "base") + 1, "a live thread uncounted");
    assert_eq!(get("fc_live_and_hold", "live_while"), 1 << 20, "a live 1 MiB buffer");
    assert_eq!(get("fc_live_and_hold", "live_after"), 0, "a freed buffer still counted live");
    assert_eq!(get("fc_live_and_hold", "live_realloc"), 1024, "a 16 B buffer reallocated to 1 KiB");
    assert_eq!(get("fc_live_and_hold", "live_zeroed"), 4096, "a zeroed 4 KiB buffer");
    assert_eq!(get("fc_live_and_hold", "max_hold_alloc_bytes"), 24, "the largest of two holds (3 boxes of a u64; then a nested hold of 2)");
    assert_eq!(get("fc_live_and_hold", "max_hold_catalog_rows"), 0, "catalog rows in a hold that read none");
    assert_eq!(get("fc_live_and_hold", "bg_hold_alloc_bytes"), 16, "the background maximum: the spawned thread's 16 B hold, not the foreground's 64 B");
    assert_eq!(get("fc_live_and_hold", "all_hold_alloc_bytes"), 64, "the all-threads maximum: the foreground's 64 B hold");
    assert_eq!(get("fc_live_and_hold", "hold_thread_left"), 1, "the hold arm's joined thread never left the thread count");
    for k in ["prepares", "page_reads", "schema_rows", "wal_locks", "wal_prepares", "wal_page_reads", "wal_schema_rows", "wal_allocs", "held_prepares"] {
        assert_eq!(get("fc_sql_nothing", k), 0, "an empty window moved {k}");
    }
    assert_eq!(get("fc_sql_prepare_3", "prepares"), 3, "three statements prepared");
    assert_eq!(get("fc_sql_execute", "prepares"), 1, "an execute compiled no statement (review 2 M4)");
    assert_eq!(get("fc_sql_query", "prepares"), 1, "a query compiled no statement (review 2 M4)");
    assert_eq!(get("fc_sleeps", "crate_sleep"), 1, "crate::thread::sleep not counted (review 2 M3)");
    assert_eq!(get("fc_sleeps", "std_sleep"), 0, "std::thread::sleep counted: the blind spot the docs state is gone");
    assert_eq!(get("fc_sql_prepare_3", "wal_locks"), 0, "preparing took the WAL write lock");
    assert_eq!(get("fc_sql_scan", "prepares"), 1, "one query, one statement");
    assert!(get("fc_sql_scan", "page_reads") >= 1, "a scan of t read no page through the pager");
    assert_eq!(get("fc_sql_scan", "schema_rows"), 0, "a scan of t parsed schema rows");
    let rows = get("fc_sql_reparse", "schema_table_rows");
    assert!(rows >= 2, "premise: sqlite_schema has rows ({rows})");
    assert_eq!(get("fc_sql_reparse", "schema_rows"), rows, "a schema re-read parses every sqlite_schema row once");
    assert_eq!(get("fc_sql_reparse", "prepares"), 1, "a schema re-read prepares its one sqlite_schema scan");
    assert_eq!(get("fc_sql_reparse", "wal_locks"), 0, "a schema re-read under a read transaction took the WAL write lock");
    assert_eq!(get("fc_sql_insert", "wal_locks"), 1, "an autocommit INSERT took the WAL write lock once");
    assert!(get("fc_sql_insert", "wal_page_reads") >= 1, "an INSERT read no page under the WAL write lock");
    assert!(get("fc_sql_insert", "wal_allocs") >= 1, "an INSERT allocated nothing under the WAL write lock");
    assert_eq!(get("fc_sql_insert", "held_after"), 0, "the WAL write lock still read as held after the commit");
    assert!(get("fc_sql_select_after", "page_reads") >= 1, "a SELECT read no page");
    assert_eq!(get("fc_sql_select_after", "wal_page_reads"), 0, "a SELECT after the commit read under the WAL write lock");
    assert_eq!(get("fc_sql_rollback", "wal_locks"), 1, "a write transaction took the WAL write lock once");
    assert!(get("fc_sql_rollback", "wal_prepares") >= 2, "a prepare and a re-read's scan under the WAL write lock not counted under it (review 2 L4)");
    assert_eq!(get("fc_sql_rollback", "wal_schema_rows"), rows, "a schema re-read under the WAL write lock: every sqlite_schema row once, under it (review 2 L4)");
    assert_eq!(get("fc_sql_rollback", "held_before_rollback"), 1, "an open write transaction's WAL write lock not read as held");
    assert_eq!(get("fc_sql_rollback", "held_after"), 0, "the WAL write lock still read as held after a ROLLBACK (review 2 L4)");
    assert_eq!(get("fc_sql_rollback", "rolled_back"), 1, "premise: the ROLLBACK undid the INSERT");
    assert_eq!(get("fc_sql_held_prepare", "prepares"), 2, "two statements prepared");
    assert_eq!(get("fc_sql_held_prepare", "held_prepares"), 1, "one statement prepared under a store mutex, one outside (review 2 L6)");
    assert!(get("fc_writer_park", "live") >= 1, "premise: the instruments store runs a confirmation writer");
    assert_eq!(get("fc_writer_park", "settled_mid_write"), 0, "a confirmation writer held mid-write read as parked (review 2 L3)");
    assert_eq!(get("fc_writer_park", "settled_after"), 1, "a released confirmation writer never parked with nothing pending (review 2 L3)");
    #[cfg(target_vendor = "apple")]
    {
        assert_eq!(get("fc_writer_stale_park", "parked_before"), 1, "premise: the writer parked before it was suspended");
        assert_eq!(get("fc_writer_stale_park", "found"), 1, "premise: exactly one branch-confirm thread, suspended");
        assert_eq!(get("fc_writer_stale_park", "landed_before_check"), 1, "premise: the held create landed while the writer was suspended");
        assert_eq!(get("fc_writer_stale_park", "settled_stale"), 0, "a held window accepted the writer's stale park: notified, not yet woken (review C14 MED)");
        assert_eq!(get("fc_writer_stale_park", "settled_resumed"), 1, "a resumed writer never parked again (review C14)");
        assert_eq!(get("fc_writer_settled_cost", "syscalls"), 0, "1,000 settle checks of a parked writer made a syscall (review C14)");
    }
    for k in ["allocs", "locks", "fsync", "full_fsync", "barrier"] {
        assert_eq!(get("fc_writer_settled_cost", k), 0, "1,000 settle checks of a parked writer moved {k} (review C14)");
    }
    assert_eq!(get("fc_thread_count", "after"), get("fc_thread_count", "base"), "an exited thread counted");
    // FLAGGED (the Linux port, 2026-10-18): the thread-state checks run wherever the readers exist.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    {
        assert_eq!(get("fc_thread_count", "with_persistent"), get("fc_thread_count", "base"), "a persistent store thread counted");
        assert_eq!(get("fc_thread_count", "parked_blocked"), 1, "a parked thread not read as blocked");
        assert_eq!(get("fc_thread_count", "spinning_seen_running"), 1, "a spinning thread read as blocked");
        assert_eq!(get("fc_thread_count", "persistent_spinning_seen_running"), 1, "a running persistent store thread read as blocked");
    }
    #[cfg(target_vendor = "apple")]
    {
        assert_eq!(get("fc_nothing", "syscalls"), 0, "an empty window moved the syscall count");
        assert_eq!(get("fc_close_37", "syscalls"), 37, "37 close(2) calls");
        assert_eq!(get("fc_threads_1000", "syscalls"), 0, "reading the thread count is a syscall");
        assert_eq!(get("fc_held_5_syscalls_3_allocs", "held_syscalls"), 5, "five syscalls while held");
        assert_eq!(get("fc_one_store_lock", "held_syscalls"), 0, "a hold with no I/O counted syscalls");
        let walk = get("fc_catalog_shape", "held_syscalls");
        assert!(walk >= 1 && walk == get("fc_catalog_shape", "syscalls"), "the catalog walk's reads under the mutex: {walk}");
    }
}

// ---- create: a named server branch of the trunk, C=1 ----

/// DESIGN.md §3 (per flight: one pwrite, one F_FULLFSYNC; per fork, records buffered under the
/// store mutex, every lock released before the flight); PREREG §9 / M1 exit 2 (1.00 F_FULLFSYNC per
/// create); review 1 #14 (fstat + dup + close per flight: 5 -> 3 syscalls, the length guard kept),
/// #2 (no catalog query), LOW 25 (one allocation under the mutex: the name's Arc); review 2 #8 (a
/// 17 B flight end).
#[test]
fn create_flushes_once() {
    assert_budgets(
        "create",
        &[
            every(FULL, Want::Exactly(1), "DESIGN §3, PREREG §9: one flush per create"),
            every(NOT_FULL, Want::Exactly(0), "DESIGN §3: no other flush"),
            every("barrier", Want::Exactly(0), "DESIGN §3: no barrier"),
        ],
    );
}

/// DESIGN.md §3: every lock is released before the flight, so no sync is issued under the mutex
/// (the engine's own holder-attributed count; platform-independent).
#[test]
fn create_issues_no_sync_under_the_store_mutex() {
    assert_budgets("create", &[every("held_syncs", Want::Exactly(0), "DESIGN §3: release every lock before the flight")]);
}

#[test]
fn create_takes_the_store_mutex_at_most_twice() {
    assert_budgets("create", &[every("locks", Want::AtMost(2), "DESIGN §3: the fork's hold + the flight leader's take")]);
}

#[test]
fn create_asks_the_catalog_nothing() {
    assert_budgets(
        "create",
        &[
            every("catalog_queries", Want::Exactly(0), "review 1 #2: no catalog probe for a new name"),
            every("catalog_loads", Want::Exactly(0), "DESIGN §3: nothing read to fork the trunk"),
        ],
    );
}

#[test]
fn create_logs_its_record_and_one_flight_end() {
    assert_budgets(
        "create",
        &[every(
            "log_bytes",
            Want::Exactly(FORK_NAMED_FRAME + FLIGHT_END),
            "journal.rs format 11: its own ForkNamed and one 17 B flight end (review 2 #8)",
        )],
    );
}

/// Counters review 2 HIGH 1 and review 3 MED-A (DECISIONS 7d1c461378, 4e5b6e3b0a): an instrument
/// does no work inside a window. The instrument of record is DYNAMIC
/// (`the_instruments_count_exactly_under_every_flag`); this is a LINT over the source (review 3
/// LOW-1/2/3): no function on the probe's window and hold paths, callees included, calls a known
/// allocator, prints, reads the environment, or calls `fe_mutant` (whose first call reads it; the
/// path asks `budget_probe::mutant_on`, primed outside any window). A line marked `lint-allow:`
/// with its reason is skipped. Bodies are found at a definition (a line starting `fn`, `pub fn` or
/// `pub(crate) fn`, each optionally `unsafe`, the name followed by `(` or `<`), and braces inside
/// strings, char literals and comments are not counted.
///
/// Counters review 4 LOW-C and review 5, the scope: the bodies listed below, which are the window
/// and hold paths and their callees (the counting allocator's hooks: `note_alloc`, `record_alloc`,
/// `record_free` and the `GlobalAlloc` methods `alloc`, `alloc_zeroed`, `realloc`, `dealloc`; the
/// thread-table readers; the scope lookups; the harness's in-window waits and confirm reads) EXCEPT
/// `prime`, `arm` and `window_diagnostics` and `ran_between` (called only from `window_diagnostics`).
/// `window_diagnostics` allocates by design, after a window's last count. `prime` reads the
/// environment by design: first from `begin`, before any count, and again inside a window from
/// `arm(true)`, where primed it must allocate nothing: that is checked dynamically, not here
/// (`fc_held_5_syscalls_3_allocs`'s allocations outside its hold are 0). Its blind spots: a callee not
/// listed here, a raw string holding `"` or `\`, and an escaped-quote char literal (`'\''`) are not
/// parsed.
#[test]
fn nothing_on_the_probes_window_path_allocates() {
    const FORBIDDEN: [&str; 24] = [
        "env::var", "format!", ".to_string()", "String::from", "String::new", "Vec::new()", "Vec::with_capacity", "vec![",
        "Box::new", "Arc::new", ".collect", ".to_owned()", ".to_vec()", ".clone()", "eprintln!", "println!", "print!(",
        "dbg!", "write!", "writeln!", "getenv", "fe_mutant(", ".push(", ".insert(",
    ];
    let sources = [
        (
            "budget_probe.rs",
            include_str!("budget_probe.rs"),
            &[
                "begin", "end", "end_owning", "user_counts", "window_syncs", "attribute", "foreign_threads", "thread_table",
                "shim_counts", "shim", "shim_lost", "unix_syscalls", "instructions", "store_locked_at", "store_unlocked_of",
                "mutant_on", "threads", "others_blocked", "each_thread", "spinning", "bump", "record_alloc", "record_free",
                "wal_lock_held", "persistent_live", "note_alloc", "alloc", "alloc_zeroed", "realloc", "dealloc",
                // The Linux thread-state readers (the Linux port, 2026-10-18).
                "owned_named", "linux_tid", "linux_read_into", "linux_task_path", "linux_task_alive", "linux_task_stat",
                "linux_task_run_ns", "linux_each_tid", "note_held_work", "site_work_begin", "site_work_end",
            ][..],
        ),
        ("budget_tests.rs", include_str!("budget_tests.rs"), &["absorb_confirm", "writer_settled", "spin_settled", "settled"][..]),
        (
            "store.rs",
            include_str!("store.rs"),
            &["own_sync_counts", "own_test_counts", "scope_top", "scope_is_audited", "scope_store_id", "confirm_park_for_test", "confirm_counts"][..],
        ),
        ("mod.rs", include_str!("mod.rs"), &["sync_counts", "branch_confirm_counts"][..]),
    ];
    let mut found = String::new();
    let mut read = 0;
    for (file, src, fns) in sources {
        for name in fns {
            let heads = ["fn ", "pub fn ", "pub(crate) fn ", "unsafe fn ", "pub unsafe fn ", "pub(crate) unsafe fn "].map(|v| format!("{v}{name}"));
            let opens = |rest: &str| heads.iter().any(|h| rest.starts_with(h.as_str()) && matches!(rest.as_bytes().get(h.len()), Some(b'(' | b'<')));
            let Some(at) = src.match_indices('\n').map(|(i, _)| i + 1).find(|&i| opens(src[i..].trim_start())) else {
                let _ = writeln!(found, "  {file}: fn {name} not found");
                continue;
            };
            let bytes = src.as_bytes();
            let open = at + src[at..].find('{').unwrap();
            let (mut depth, mut j, mut end) = (0i32, open, open);
            let (mut in_str, mut in_comment) = (false, false);
            while j < bytes.len() {
                let c = bytes[j];
                if in_comment {
                    in_comment = c != b'\n';
                } else if in_str {
                    if c == b'\\' {
                        j += 1;
                    } else if c == b'"' {
                        in_str = false;
                    }
                } else if c == b'/' && bytes.get(j + 1) == Some(&b'/') {
                    in_comment = true;
                } else if c == b'"' {
                    in_str = true;
                } else if c == b'\'' && bytes.get(j + 2) == Some(&b'\'') {
                    j += 2;
                } else if c == b'{' {
                    depth += 1;
                } else if c == b'}' {
                    depth -= 1;
                    if depth == 0 {
                        end = j;
                        break;
                    }
                }
                j += 1;
            }
            read += 1;
            for line in src[open..=end].lines() {
                if line.contains("lint-allow:") {
                    continue;
                }
                let code = line.split("//").next().unwrap_or("");
                for bad in FORBIDDEN {
                    if code.contains(bad) {
                        let _ = writeln!(found, "  {file}: fn {name}: {bad} in `{}`", line.trim());
                    }
                }
            }
        }
    }
    assert_eq!(read, 54, "premise: every function on the window path was read:\n{found}");
    assert!(found.is_empty(), "the probe's window path allocates, prints, reads the environment or calls fe_mutant:\n{found}");
}

/// Counters review 3 MED-A, the instrument of record (DECISIONS 4e5b6e3b0a): the instruments'
/// exact-count fire-checks (`fc_nothing` moves nothing, `fc_box_23` allocates 23 boxes of 184 bytes)
/// hold in a child with FE_MUTANT set to a name no mutant has and every diagnostic flag set, so that
/// a flag's first read, or a diagnostic's work, cannot land inside a window unseen.
///
/// Counters review 4 LOW-B, the scope: "every flag" is the probe's own diagnostics
/// (FE_BUDGET_FOREIGN_NAMES, FE_BUDGET_NEW_NAMES; FE_BUDGET_CHILD is set in the cell's own child)
/// and the two mutant selectors read once per process (FE_MUTANT, R13_MUTANT), each set to a name no
/// mutant has. NOT in scope, because a run with one set is not a budget configuration: behaviour
/// knobs (R11_*, R12_PREWARM*, R13_FW1/FW2/MERGER, FE_LOSE_UNSYNCED, FE_KILL_*,
/// TURSO_MEASURE_WORD_HOLD), other lanes' instruments that record by design (R12_TRACE_READS pushes
/// every page read, R11_TRACE_*), FE_CAPTURE_VERIFY (verifies captures, and aborts), and
/// R12_MUTANT (read on every call).
#[test]
fn the_instruments_count_exactly_under_every_flag() {
    if in_child() || std::env::var_os("FE_FLAGS_CHILD").is_some() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::budget_tests::the_budget_counters_count_exactly_what_was_done", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_FLAGS_CHILD", "1")
        // A mutant run passes its mutant on (counters review 4 MED-1: the probe's and the harness's
        // window mutants are killed here); otherwise a name no mutant has, so the name is still read.
        .env("FE_MUTANT", std::env::var("FE_MUTANT").unwrap_or_else(|_| "no_mutant_has_this_name".to_string()))
        .env("R13_MUTANT", "no_mutant_has_this_name")
        .env("FE_BUDGET_FOREIGN_NAMES", "1")
        .env("FE_BUDGET_NEW_NAMES", dir.path().join("new-names.txt"))
        .env_remove("FE_BUDGET_RAW_DIR")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    // Counters review 4 LOW-A: "running 1 test" and exit 0 also accept an ignored test.
    assert!(text.contains("running 1 test"), "premise: the instruments test ran under the flags:\n{text}");
    assert!(out.status.success(), "the instruments' exact counts fail with every flag set:\n{text}");
    assert!(text.contains("test result: ok. 1 passed"), "premise: the instruments test passed, not ignored or filtered, under the flags:\n{text}");
}

/// Counters review 4 LOW-D (DECISIONS 6399fd35bf): an armed hold taken before any window allocates
/// nothing under the store mutex. Measured in a fresh process, where nothing is primed, with
/// FE_MUTANT set (so the mutant name's first read allocates): `arm(true)`, one hold through the
/// probe, and the thread's held allocations must not move. A held allocation here is the probe's
/// own (`mutant_on`'s unprimed fallback, or a lazy lookup at the lock), charged to a measured hold.
#[test]
fn an_armed_hold_before_any_window_allocates_nothing_under_the_mutex() {
    if std::env::var_os("FE_ARM_CHILD").is_some() {
        let (_, _, held0, bytes0) = probe::thread_allocs();
        probe::arm(true);
        probe::store_locked();
        probe::store_unlocked();
        let (_, _, held1, bytes1) = probe::thread_allocs();
        probe::arm(false);
        println!("ARM-CHILD held_allocs {} held_bytes {}", held1 - held0, bytes1 - bytes0);
        return;
    }
    if in_child() || std::env::var_os("FE_FLAGS_CHILD").is_some() {
        return;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::budget_tests::an_armed_hold_before_any_window_allocates_nothing_under_the_mutex", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_ARM_CHILD", "1")
        .env("FE_MUTANT", std::env::var("FE_MUTANT").unwrap_or_else(|_| "no_mutant_has_this_name".to_string()))
        .env_remove("FE_BUDGET_CHILD")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success() && text.contains("test result: ok. 1 passed"), "premise: the arm child ran:\n{text}");
    let line = text.lines().find_map(|l| l.find("ARM-CHILD ").map(|i| l[i..].to_string())).unwrap_or_else(|| panic!("premise: the arm child reported nothing:\n{text}"));
    assert_eq!(line.trim(), "ARM-CHILD held_allocs 0 held_bytes 0", "an armed hold before any window allocated under the store mutex");
}

/// The probe's thread-state readers (lead ruling 2026-10-18, the Linux port): `thread_table`,
/// `others_blocked` and `threads`, on which every budget window closes, read a parked thread as
/// blocked and a spinning one as running, each forced, list every thread with its own id and CPU
/// time, and mark the caller. The same assertions hold on Apple (Mach, the semantics of record) and
/// on Linux (/proc/self/task): this is the parity check; where the platforms differ is stated at
/// `thread_table`. Run in a child of its own, so no other test's threads run beside it. Expected ids
/// come from the platform (pthread_threadid_np, gettid), never from the readers.
#[cfg(any(target_vendor = "apple", target_os = "linux"))]
#[test]
fn the_thread_state_readers_read_blocked_and_running_threads() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::{Acquire, Release}};
    fn platform_tid() -> u64 {
        #[cfg(target_vendor = "apple")]
        let id = {
            let mut me = 0u64;
            // SAFETY: this thread's own id into a local.
            unsafe { libc::pthread_threadid_np(0, &mut me) };
            me
        };
        // SAFETY: the raw syscall (glibc before 2.30 has no gettid wrapper); it cannot fail.
        #[cfg(not(target_vendor = "apple"))]
        let id = unsafe { libc::syscall(libc::SYS_gettid) } as u64;
        id
    }
    if std::env::var_os("FE_THREAD_STATE_CHILD").is_none() {
        if in_child() || std::env::var_os("FE_FLAGS_CHILD").is_some() || std::env::var_os("FE_ARM_CHILD").is_some() {
            return;
        }
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["branch::budget_tests::the_thread_state_readers_read_blocked_and_running_threads", "--exact", "--test-threads=1", "--nocapture"])
            .env("FE_THREAD_STATE_CHILD", "1")
            .env_remove("FE_BUDGET_CHILD")
            .output()
            .unwrap();
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success() && text.contains("test result: ok. 1 passed"), "the thread-state readers, in their own child:\n{text}");
        return;
    }
    let me = platform_tid();
    let (parked_id, spinner_id) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let parked = {
        let id = parked_id.clone();
        std::thread::spawn(move || {
            id.store(platform_tid(), Release);
            let _ = rx.recv();
        })
    };
    let stop = Arc::new(AtomicBool::new(false));
    let spinner = {
        let (id, stop) = (spinner_id.clone(), stop.clone());
        std::thread::spawn(move || {
            id.store(platform_tid(), Release);
            while !stop.load(Acquire) {
                std::hint::spin_loop();
            }
        })
    };
    while parked_id.load(Acquire) == 0 || spinner_id.load(Acquire) == 0 {
        std::hint::spin_loop();
    }
    let (pid, sid) = (parked_id.load(Acquire), spinner_id.load(Acquire));
    let read = || {
        let mut t = [probe::ThreadCpu::EMPTY; 48];
        let n = probe::thread_table(&mut t).expect("CLAIM: the thread table cannot be read on this platform");
        (n, t)
    };
    let find = |t: &[probe::ThreadCpu; 48], id: u64| t.iter().copied().find(|x| x.id == id && x.id != 0);
    // Forced blocked: the parked thread reads waiting (it may still be on its way into the wait).
    let t0 = std::time::Instant::now();
    let (n, t) = loop {
        let (n, t) = read();
        if find(&t, pid).is_some_and(|x| x.waiting) || t0.elapsed() > std::time::Duration::from_secs(5) {
            break (n, t);
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    assert!(n >= 3 && n <= 48, "premise: {n} threads listed (this thread, the parked one, the spinner, the harness's)");
    let mine: Vec<_> = t.iter().filter(|x| x.me).collect();
    assert_eq!(mine.len(), 1, "CLAIM: the caller is not marked exactly once");
    assert_eq!(mine[0].id, me, "CLAIM: the caller's id is not the platform's thread id");
    let p = find(&t, pid).unwrap_or_else(|| panic!("CLAIM: the parked thread (id {pid}) is not listed"));
    assert!(p.waiting, "CLAIM: a parked thread does not read blocked");
    let s0 = find(&t, sid).unwrap_or_else(|| panic!("CLAIM: the spinning thread (id {sid}) is not listed"));
    assert!(!s0.waiting, "CLAIM: a spinning thread reads blocked");
    let mut ids: Vec<u64> = t[..n].iter().map(|x| x.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), n, "CLAIM: two listed threads share an id");
    // CPU time: the spinner's grows, the parked thread's does not.
    std::thread::sleep(std::time::Duration::from_millis(100));
    let (_, t2) = read();
    let s1 = find(&t2, sid).expect("CLAIM: the spinning thread vanished");
    let p1 = find(&t2, pid).expect("CLAIM: the parked thread vanished");
    assert!(s1.cpu_ns >= s0.cpu_ns + 20_000_000, "CLAIM: 100 ms of spinning moved its CPU time {} ns -> {} ns", s0.cpu_ns, s1.cpu_ns);
    assert_eq!(p1.cpu_ns, p.cpu_ns, "CLAIM: a parked thread's CPU time moved");
    // A running thread's CPU time is current when read, not as of its last scheduler event: between
    // two readings 200 us apart a thread that never stopped running has gained time, every time
    // (fc_foreign_window's helper runs for well under a tick; the Linux port's run 1 read it as not
    // having run, ecetesla4 and iccad6).
    for i in 0..20 {
        let (_, a) = read();
        let w = std::time::Instant::now();
        while w.elapsed() < std::time::Duration::from_micros(200) {
            std::hint::spin_loop();
        }
        let (_, b) = read();
        let (x, y) = (find(&a, sid).expect("CLAIM: the spinning thread vanished"), find(&b, sid).expect("CLAIM: the spinning thread vanished"));
        assert!(y.cpu_ns > x.cpu_ns, "CLAIM: a running thread's CPU time did not move in 200 us (try {i}: {} ns -> {} ns): it is read stale", x.cpu_ns, y.cpu_ns);
    }
    assert_eq!(probe::others_blocked(), Some(false), "CLAIM: others_blocked missed the running spinner");
    let with = probe::threads().expect("CLAIM: the thread count cannot be read on this platform");
    stop.store(true, Release);
    spinner.join().unwrap();
    // Forced blocked again: with the spinner gone, every other thread reads blocked, soon.
    let t0 = std::time::Instant::now();
    while probe::others_blocked() != Some(true) && t0.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(probe::others_blocked(), Some(true), "CLAIM: with only a parked thread beside it, others_blocked is not true");
    let t0 = std::time::Instant::now();
    while probe::threads() != Some(with - 1) && t0.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(probe::threads(), Some(with - 1), "CLAIM: the joined spinner is still counted");
    tx.send(()).unwrap();
    parked.join().unwrap();
}

/// Counters review MED-5, review 2 HIGH 2 and review 3 MED-B/LOW-4/LOW-10 (DECISIONS 7d1c461378,
/// 4e5b6e3b0a): a parity check needs an independent second count. In a budget child the process's
/// sync counts are exact (one cell's stores at a time) and the budget's reading IS them
/// (`window_syncs`), so the second count is the stores' own attribution (`_own`). Every window that
/// records the process's syncs records the budget's own key and `_own` too, and they agree; every
/// cell compares at least one window and its process syncs sum above zero (a 0 == 0 comparison
/// guards nothing). Never process against process.
///
/// Mutant `drop_journal_sync_count` (test builds only) hides every counted arm of a branch file's
/// sync from the process (fsync, F_FULLFSYNC, barrier). Its expected killer, per cell and
/// platform (`killer`): on Apple, the D1 open cell's `fsync` and the D2 cells' `full_fsync`; off
/// Apple, every cell's `fsync` (F_FULLFSYNC is fsync there). Counters review 4 LOW-E, fail-closed:
/// the table is checked, not assumed: unmutated, every cell's killer key moves in its own stores'
/// count (`{killer}_own` sums above zero), so the mutant has something to hide there; under the
/// mutant, every cell compared a window and has a mismatch on its killer key. By the mutation
/// convention a mutant run FAILS when the mutant is killed ("KILLED in every cell") and PASSES,
/// printing "SURVIVED ..." with the reason, when it survives (`parity_mutant_verdict`: a run that
/// compared nothing or read no cell survives too), so a harness that judges by exit status never
/// counts a survivor as killed. Unmutated, an empty cell list fails.
#[test]
fn every_budget_windows_syncs_equal_the_process_syncs_in_its_child() {
    if in_child() {
        return;
    }
    const D2_CELLS: [&str; 12] = [
        "n10_small", "n1e4_small", "n10_large", "ckpt_n10", "confirm_load", "confirm_n10", "first_create", "shared_c1", "shared_c8",
        "shared_c64", "shared_c256", "shared_c1024",
    ];
    let killer = |spec: &str| -> &'static str {
        if cfg!(target_vendor = "apple") && D2_CELLS.contains(&spec) {
            "full_fsync"
        } else {
            "fsync"
        }
    };
    let mutant = super::store::fe_mutant("drop_journal_sync_count");
    let mut failures = String::new();
    let mut survived = String::new();
    let (mut compared, mut cells_read) = (0, 0);
    let cells = MAIN
        .iter()
        .copied()
        .chain(["ckpt_n10", "confirm_load", "confirm_n10", "first_create", "open_d1"])
        .chain(["shared_c1", "shared_c8", "shared_c64", "shared_c256", "shared_c1024"]);
    for spec in cells {
        let c = cell(spec);
        cells_read += 1;
        let (mut in_cell, mut process_sum, mut killer_own, mut killed) = (0, 0u64, 0u64, false);
        for (op, samples) in &c.ops {
            for (i, s) in samples.iter().enumerate() {
                for k in ["fsync", "full_fsync", "barrier"] {
                    let Some(&process) = s.get(&format!("{k}_process")) else { continue };
                    if !s.contains_key(k) {
                        let _ = writeln!(failures, "  {spec}: {op} {i}: {k}_process with no {k}: the budget's own reading is missing");
                    }
                    let Some(&own) = s.get(&format!("{k}_own")) else {
                        let _ = writeln!(failures, "  {spec}: {op} {i}: {k}_process with no {k}_own: no independent count");
                        continue;
                    };
                    compared += 1;
                    in_cell += 1;
                    process_sum += process;
                    if k == killer(spec) {
                        killer_own += own;
                    }
                    if own != process {
                        if k == killer(spec) {
                            killed = true;
                        }
                        let _ = writeln!(failures, "  {spec}: {op} {i}: {k}_own {own}, the process {process}");
                    }
                }
            }
        }
        if in_cell == 0 {
            let _ = writeln!(failures, "  {spec}: no window compared");
        }
        if process_sum == 0 && !mutant {
            let _ = writeln!(failures, "  {spec}: its windows' process syncs sum to 0: the comparison guards nothing");
        }
        if killer_own == 0 && !mutant {
            let _ = writeln!(failures, "  {spec}: its killer key {} never moved in its stores' own count: the mutant could not be killed here", killer(spec));
        }
        if mutant && (in_cell == 0 || !killed) {
            let _ = writeln!(survived, "  {spec} (killer {}, {in_cell} windows compared)", killer(spec));
        }
    }
    if mutant {
        match parity_mutant_verdict(cells_read, compared, &survived, &failures) {
            Ok(survivors) => {
                // Straight to stderr: a passing test's eprintln! is captured by libtest and lost.
                let _ = std::io::Write::write_all(&mut std::io::stderr(), format!("{survivors}\n").as_bytes());
                return;
            }
            Err(killed) => panic!("{killed}"),
        }
    }
    assert!(cells_read > 0, "premise: no cell was read");
    assert!(compared > 0, "premise: no window carried both counts");
    assert!(failures.is_empty(), "budget windows whose stores' own sync counts are not the process's ({compared} compared):\n{failures}");
}

/// The parity test's verdict under `drop_journal_sync_count`, by the mutation convention: `Ok`, the
/// mutant SURVIVED (the run passes, the message names the survivors); `Err`, it was KILLED (the run
/// fails). `survived` lists the cells with no window compared or no mismatch on their killer key.
fn parity_mutant_verdict(cells_read: usize, compared: usize, survived: &str, failures: &str) -> Result<String, String> {
    // Counters review 5 LOW-E: survival is decided first; a missing premise is a reason to survive,
    // never a kill. (With cells read and nothing compared, every cell is already in `survived`.)
    if cells_read == 0 {
        return Ok("drop_journal_sync_count SURVIVED: no cell was read, so nothing could kill it".to_string());
    }
    if !survived.is_empty() {
        return Ok(format!("drop_journal_sync_count SURVIVED in ({compared} windows compared):\n{survived}"));
    }
    if compared == 0 {
        return Ok("drop_journal_sync_count SURVIVED: no window carried both counts, so nothing could kill it".to_string());
    }
    Err(format!("drop_journal_sync_count KILLED in every cell ({cells_read} cells, {compared} compared):\n{failures}"))
}

/// Counters review 5 LOW-E (DECISIONS 52616c176d): under the mutation convention no premise may turn a
/// survivor into a kill. A run that compared nothing (every cell a survivor) and a run that read no
/// cell are SURVIVED, with the reason; only a mismatch on every cell's killer key is KILLED.
#[test]
fn the_parity_verdict_counts_no_survivor_as_killed() {
    let all_uncompared = "  a (killer fsync, 0 windows compared)\n  b (killer full_fsync, 0 windows compared)\n";
    let v = parity_mutant_verdict(2, 0, all_uncompared, "");
    assert!(v.as_ref().is_ok_and(|m| m.contains("SURVIVED")), "every cell uncompared read as killed: {v:?}");
    let v = parity_mutant_verdict(0, 0, "", "");
    assert!(v.as_ref().is_ok_and(|m| m.contains("SURVIVED") && m.contains("no cell")), "an empty cell list read as killed: {v:?}");
    let v = parity_mutant_verdict(2, 7, "  a (killer fsync, 3 windows compared)\n", "  ...\n");
    assert!(v.as_ref().is_ok_and(|m| m.contains("SURVIVED in") && m.contains("  a ")), "a cell without a killer mismatch read as killed: {v:?}");
    let v = parity_mutant_verdict(2, 7, "", "  a: x\n  b: y\n");
    assert!(v.as_ref().is_err_and(|m| m.contains("KILLED in every cell")), "a mismatch on every cell's killer key did not read as killed: {v:?}");
}

#[cfg(target_vendor = "apple")]
#[test]
fn create_issues_at_most_three_syscalls() {
    assert_budgets("create", &[every("syscalls", Want::AtMost(3), "DESIGN §3 + review 1 #14: pwrite, F_FULLFSYNC, length guard")]);
}

#[cfg(target_vendor = "apple")]
#[test]
fn create_issues_no_syscall_under_the_store_mutex() {
    assert_budgets("create", &[every("held_syscalls", Want::Exactly(0), "DESIGN §3; review 1 #14, review 2 #6: fstat + dup under the locks")]);
}

#[test]
fn create_allocates_at_most_once_under_the_store_mutex() {
    assert_budgets("create", &[steady("held_allocs", Want::AtMost(1), "review 1 LOW 25: in-place frames, one Arc for the name")]);
}

#[test]
fn create_is_independent_of_live_branches() {
    assert_same("create", "n10_small", "n1e4_small", &[]);
}

#[test]
fn create_is_independent_of_database_size() {
    assert_same("create", "n10_small", "n10_large", &[]);
}

/// Review 1 #15 (a std HashMap of every name rehashes in one hold: an O(N) stall): while the live
/// branches grow from 10^4 past 2^15, no create allocates more than 64 KiB under the store mutex.
/// An O(N) rehash or copy of a per-branch table there allocates at least 8 B per branch (> 256 KiB
/// at 2^15); O(1) work does not come near it.
#[test]
fn create_allocates_no_table_of_every_branch_under_the_store_mutex() {
    if in_child() {
        return;
    }
    let c = cell("growth_n1e4");
    let live = c.ops["growth_live"][0]["live"];
    assert!(live >= N_LARGE + TAIL + GROWTH, "premise: the live branches grew to {live}");
    let threads = &c.ops["growth_thread"];
    let creates: u64 = threads.iter().map(|t| t["creates"]).sum();
    assert_eq!(creates, GROWTH, "premise: every growth create counted");
    let worst = threads.iter().map(|t| t["max_held_alloc_bytes"]).max().unwrap();
    let worst_all = threads.iter().map(|t| t["max_alloc_bytes"]).max().unwrap();
    assert!(
        worst <= 64 << 10,
        "a create allocated {worst} bytes under the store mutex ({worst_all} in all) while the live \
         branches grew to {live} ({} checkpoints started meanwhile); budget 64 KiB [review 1 #15]",
        threads[0]["ckpt_started"]
    );
}

// ---- connect by name: a resident branch, just created ----

/// A resident branch's name and state are in memory (names map, states): connecting reads,
/// writes and syncs nothing, takes the store mutex to resolve the name and to mark the branch
/// open, and costs no syscall a trunk connect does not.
#[test]
fn connect_named_makes_nothing_durable() {
    assert_budgets(
        "connect_named",
        &[
            every(FULL, Want::Exactly(0), "nothing to make durable"),
            every(NOT_FULL, Want::Exactly(0), "nothing to make durable"),
            every("barrier", Want::Exactly(0), "nothing to order"),
            every("log_bytes", Want::Exactly(0), "nothing to log"),
        ],
    );
}

#[test]
fn connect_named_asks_the_catalog_nothing() {
    assert_budgets(
        "connect_named",
        &[
            every("catalog_queries", Want::Exactly(0), "a resident branch: nothing to read"),
            every("catalog_loads", Want::Exactly(0), "a resident branch: nothing to load"),
        ],
    );
}

#[test]
fn connect_named_takes_the_store_mutex_at_most_twice() {
    assert_budgets("connect_named", &[every("locks", Want::AtMost(2), "the name's lookup + open_conn")]);
}

#[cfg(target_vendor = "apple")]
#[test]
fn connect_named_issues_no_syscall_a_trunk_connect_does_not() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for spec in MAIN {
        let c = cell(spec);
        let trunk = values(&c, "connect_trunk", &quiet(&c, "connect_trunk"), "syscalls");
        let branch = values(&c, "connect_named", &quiet(&c, "connect_named"), "syscalls");
        let (t, b) = (*trunk.iter().max().unwrap(), *branch.iter().max().unwrap());
        if b > t {
            let _ = writeln!(failures, "  {spec}: connect_named {b} syscalls, trunk connect {t}");
        }
    }
    assert!(failures.is_empty(), "connect_named is over budget:\n{failures}");
}

#[cfg(target_vendor = "apple")]
#[test]
fn connect_named_issues_no_syscall_under_the_store_mutex() {
    assert_budgets("connect_named", &[every("held_syscalls", Want::Exactly(0), "nothing to read or write under the mutex")]);
}

#[test]
fn connect_named_is_independent_of_live_branches() {
    assert_same("connect_named", "n10_small", "n1e4_small", &[]);
}

#[test]
fn connect_named_is_independent_of_database_size() {
    assert_same("connect_named", "n10_small", "n10_large", &[]);
}

// ---- first write: one-row UPDATE, autocommit, on a fresh named branch ----

/// PREREG §5 (create-then-first-write floor: 2 barriers, so the first write's is ONE full flush),
/// review 1 #1 (the arena ordered, not flushed: one F_FULLFSYNC per flight) and the arena-barrier
/// ruling (one ordering primitive on the arena: F_BARRIERFSYNC now, plain fsync under 85a032f01);
/// DESIGN §3 (a slot, the records, one barrier: no catalog read); the journal format (one
/// one-page Commit, one flight end).
#[test]
fn first_write_flushes_once_and_orders_the_arena_once() {
    #[cfg(target_vendor = "apple")]
    let order = [
        every("barrier", Want::AtMost(1), "ruling 85a032f01: one ordering sync of the arena"),
        every(NOT_FULL, Want::AtMost(1), "ruling 85a032f01: the arena's plain fsync, if not a barrier"),
    ];
    #[cfg(not(target_vendor = "apple"))]
    let order = [
        every("barrier", Want::Exactly(0), "no barrier off Apple"),
        every(NOT_FULL, Want::Exactly(0), "no F_FULLFSYNC off Apple"),
    ];
    let [a, b] = order;
    assert_budgets("first_write", &[every(FULL, Want::Exactly(1), "PREREG §5, review 1 #1: one full flush"), a, b]);
}

#[test]
fn first_write_issues_no_sync_under_the_store_mutex() {
    assert_budgets("first_write", &[every("held_syncs", Want::Exactly(0), "DESIGN §3, PREREG M2: the flight outside the mutex")]);
}

#[test]
fn first_write_asks_the_catalog_nothing() {
    assert_budgets("first_write", &[every("catalog_queries", Want::Exactly(0), "DESIGN §3: a slot and two records")]);
}

#[test]
fn first_write_logs_one_commit_and_one_flight_end() {
    assert_budgets(
        "first_write",
        &[every("log_bytes", Want::Exactly(COMMIT_1_FRAME + FLIGHT_END), "journal.rs format 11: one Commit, one 17 B flight end")],
    );
}

#[test]
fn first_write_takes_the_store_mutex_once_per_page_and_twice_more() {
    assert_budgets(
        "first_write",
        &[every("locks", Want::AtMostPlus("resolves", 2), "one hold per page resolved + commit_pages + the flight leader")],
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn first_write_issues_at_most_five_syscalls_beyond_its_page_reads() {
    assert_budgets(
        "first_write",
        &[every("syscalls", Want::AtMostPlus("resolves", 5), "slot pwrite, arena order, log pwrite, F_FULLFSYNC, length guard + one read per page")],
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn first_write_issues_no_syscall_under_the_store_mutex() {
    assert_budgets(
        "first_write",
        &[every("held_syscalls", Want::Exactly(0), "PREREG M2, review 1 LOW 26 / #14: slot writes and flight taking off the mutex")],
    );
}

#[test]
fn first_write_is_independent_of_live_branches() {
    assert_same("first_write", "n10_small", "n1e4_small", &[]);
}

#[test]
fn first_write_is_independent_of_database_size() {
    assert_same("first_write", "n10_small", "n10_large", &[]);
}

// ---- disconnect: the branch connection dropped ----

/// Closing a branch connection that wrote touches no file: no sync, no syscall, no log record, no
/// catalog query, one store-mutex hold (`close`).
#[test]
fn disconnect_makes_nothing_durable_and_reads_nothing() {
    assert_budgets(
        "disconnect",
        &[
            every(FULL, Want::Exactly(0), "nothing to make durable"),
            every(NOT_FULL, Want::Exactly(0), "nothing to make durable"),
            every("log_bytes", Want::Exactly(0), "nothing to log for an unreleased branch"),
            every("catalog_queries", Want::Exactly(0), "nothing to read"),
        ],
    );
}

#[test]
fn disconnect_issues_no_sync_under_the_store_mutex() {
    assert_budgets("disconnect", &[every("held_syncs", Want::Exactly(0), "close makes nothing durable")]);
}

#[test]
fn disconnect_takes_the_store_mutex_at_most_once() {
    assert_budgets("disconnect", &[every("locks", Want::AtMost(1), "close's one hold")]);
}

#[cfg(target_vendor = "apple")]
#[test]
fn disconnect_issues_no_syscall() {
    assert_budgets("disconnect", &[every("syscalls", Want::Exactly(0), "nothing to read or write")]);
}

#[test]
fn disconnect_is_independent_of_live_branches() {
    assert_same("disconnect", "n10_small", "n1e4_small", &[]);
}

#[test]
fn disconnect_is_independent_of_database_size() {
    assert_same("disconnect", "n10_small", "n10_large", &[]);
}

// ---- delete: drop_branch of a resident branch that wrote one page ----

/// DESIGN.md §3 Delete (a 17 B Release rides a shared flight; frees deferred until it is durable;
/// O(1) amortised); review 1 #37 (lookup and release in one hold) and #14 (the per-flight
/// syscalls); review 2 #8 (a 17 B flight end).
#[test]
fn delete_flushes_once() {
    assert_budgets(
        "delete",
        &[
            every(FULL, Want::Exactly(1), "DESIGN §3: the Release rides one flight"),
            every(NOT_FULL, Want::Exactly(0), "DESIGN §3: no other flush"),
            every("barrier", Want::Exactly(0), "no arena write to order"),
        ],
    );
}

#[test]
fn delete_issues_no_sync_under_the_store_mutex() {
    assert_budgets("delete", &[every("held_syncs", Want::Exactly(0), "DESIGN §3: the Release rides a flight outside the mutex")]);
}

#[test]
fn delete_takes_the_store_mutex_at_most_twice() {
    assert_budgets("delete", &[every("locks", Want::AtMost(2), "review 1 #37: lookup + release in one hold, + the flight leader")]);
}

#[test]
fn delete_asks_the_catalog_nothing() {
    assert_budgets("delete", &[every("catalog_queries", Want::Exactly(0), "a resident branch: nothing to read")]);
}

#[test]
fn delete_logs_its_release_and_one_flight_end() {
    assert_budgets(
        "delete",
        &[every("log_bytes", Want::Exactly(RELEASE_FRAME + FLIGHT_END), "DESIGN §3, format 11: one Release, one 17 B flight end")],
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn delete_issues_at_most_three_syscalls() {
    assert_budgets("delete", &[every("syscalls", Want::AtMost(3), "DESIGN §3 + review 1 #14: pwrite, F_FULLFSYNC, length guard")]);
}

#[cfg(target_vendor = "apple")]
#[test]
fn delete_issues_no_syscall_under_the_store_mutex() {
    assert_budgets("delete", &[every("held_syscalls", Want::Exactly(0), "DESIGN §3; review 1 #14, review 2 #6")]);
}

#[test]
fn delete_is_independent_of_live_branches() {
    assert_same("delete", "n10_small", "n1e4_small", &[]);
}

#[test]
fn delete_is_independent_of_database_size() {
    assert_same("delete", "n10_small", "n10_large", &[]);
}

// ---- recovery: the open of a database with N live branches and a 16-create log tail ----

/// `recovery` costs the same at `a` and `b`. The first open after the building process (open 0)
/// differs from a reopen of an opened store, so it is compared with its own kind: open 0 exactly,
/// the reopens by each counter's smallest value over them. Syscalls are compared only over the
/// reopens: a recovery runs two threads (the opening one and the name filter's build), and a
/// contended mutex adds syscalls of its own, as many as the timing makes, so the cost is the
/// smallest of several. `keys` are compared, plus (when `instructions`) the steady instruction
/// count within `INSTRUCTION_SLACK`.
fn assert_recovery_same(a: &str, b: &str, keys: &[&str], instructions: bool) {
    if in_child() {
        return;
    }
    let (ca, cb) = (cell(a), cell(b));
    let (sa, sb) = (&ca.ops["recovery"], &cb.ops["recovery"]);
    assert!(sa.len() == sb.len() && sa.len() >= 3, "premise: as many opens, at least three");
    for (i, (x, y)) in sa.iter().zip(sb).enumerate() {
        assert!(x["quiet"] == 1 && y["quiet"] == 1, "open {i}: a background thread outlived the wait");
    }
    let get = |s: &Map, k: &str, i: usize| *s.get(k).unwrap_or_else(|| panic!("open {i}: no {k} counted"));
    let mut failures = String::new();
    for k in keys.iter().filter(|k| **k != "syscalls") {
        let (x, y) = (get(&sa[0], k, 0), get(&sb[0], k, 0));
        if x != y {
            let _ = writeln!(failures, "  open 0: recovery.{k}: {a} {x}, {b} {y}");
        }
    }
    let mut steady = keys.to_vec();
    let d0 = a.ends_with("_d0") && b.ends_with("_d0");
    if instructions && d0 && sa[0].contains_key("instructions") {
        steady.push("instructions");
    }
    for k in steady {
        let x = (1..sa.len()).map(|i| get(&sa[i], k, i)).min().unwrap();
        let y = (1..sb.len()).map(|i| get(&sb[i], k, i)).min().unwrap();
        if k == "instructions" {
            let ratio = y as f64 / x.max(1) as f64;
            if !(1.0 / INSTRUCTION_SLACK..=INSTRUCTION_SLACK).contains(&ratio) {
                let _ = writeln!(failures, "  reopens: recovery.instructions (steady): {a} {x}, {b} {y} (x{ratio:.2})");
            }
        } else if x != y {
            let _ = writeln!(failures, "  reopens: recovery.{k} (smallest): {a} {x}, {b} {y}");
        }
    }
    assert!(failures.is_empty(), "recovery differs between {a} and {b}:\n{failures}");
}

/// DESIGN.md §3 Recovery: read the catalog's meta row and replay the log's tail; nothing grows
/// with the live branches (review 1 #2: never load every name at open). Unnamed populations, so
/// the name filter's build — Θ(names) by design, on its own thread (review 1 #2) — reads the same
/// at both N, and every process-wide counter is compared.
#[test]
fn recovery_is_independent_of_live_branches() {
    let mut keys: Vec<&str> = vec![
        "fsync", "full_fsync", "barrier", "allocs", "alloc_bytes", "held_allocs", "held_alloc_bytes",
        "locks", "locks_process", "catalog_queries", "catalog_rows", "catalog_loads", "log_bytes",
    ];
    if cfg!(target_vendor = "apple") {
        keys.push("syscalls");
    }
    assert_recovery_same("n10_small_unnamed", "n1e4_small_unnamed", &keys, true);
}

/// The same with NAMED populations, on the counters the name filter's own thread cannot move: the
/// opening thread's allocations and holds, and the store's catalog queries (the filter reads on a
/// connection of its own). Instructions are process-wide, so they carry the filter's build and are
/// not compared here.
#[test]
fn recovery_of_named_branches_is_independent_of_live_branches_on_the_opening_thread() {
    assert_recovery_same(
        "n10_small",
        "n1e4_small",
        &["allocs", "alloc_bytes", "held_allocs", "held_alloc_bytes", "locks", "catalog_queries", "catalog_rows", "catalog_loads"],
        false,
    );
}

#[test]
fn recovery_is_independent_of_database_size() {
    let mut keys: Vec<&str> = vec![
        "fsync", "full_fsync", "barrier", "allocs", "alloc_bytes", "held_allocs", "held_alloc_bytes",
        "locks", "locks_process", "catalog_queries", "catalog_rows", "catalog_loads", "log_bytes",
    ];
    if cfg!(target_vendor = "apple") {
        keys.push("syscalls");
    }
    assert_recovery_same("n10_small", "n10_large", &keys, true);
}

/// DESIGN.md §3 Recovery reads; it writes nothing to the log (a clean tail needs no cut).
#[test]
fn recovery_appends_nothing_to_the_log() {
    if in_child() {
        return;
    }
    for spec in MAIN {
        let c = cell(spec);
        for (i, s) in c.ops["recovery"].iter().enumerate() {
            let bytes = *s.get("log_bytes").unwrap_or_else(|| panic!("{spec}: open {i}: no log_bytes"));
            assert_eq!(bytes, 0, "{spec}: open {i} appended {bytes} bytes to the log");
        }
    }
}

// ---- checkpoints: the threshold lowered so creates and first writes start them ----

/// What the operations that started a checkpoint cost beyond the same operation's steady cost
/// (`ckpt_started == 0`), summed per counter, with the checkpoints installed. Refuses fewer than
/// three.
fn checkpoint_excess(spec: &str) -> (u64, BTreeMap<&'static str, i64>) {
    let (installed, excess, _) = checkpoint_excess_of(spec, true);
    (installed, excess)
}

/// The excess of the operations that started a checkpoint over their calm minimum, summed, and the
/// checkpoints they installed: over the QUIET samples only (`quiet_only`, every budget but the
/// selection-bias check) or over every sample. Reports the checkpoint-starting rounds attributable
/// and refused. With `quiet_only`, refuses below three attributable installs (class (b): never a
/// verdict from fewer).
fn checkpoint_excess_of(spec: &str, quiet_only: bool) -> (u64, BTreeMap<&'static str, i64>, (u64, u64)) {
    let c = cell(spec);
    let mut excess: BTreeMap<&'static str, i64> = BTreeMap::new();
    let mut installed = 0;
    let (mut attributable, mut refused) = (0u64, 0u64);
    for op in ["ckpt_create", "ckpt_first_write"] {
        let all = c.ops.get(op).map(Vec::as_slice).unwrap_or_default();
        let samples = quiet(&c, op);
        let calm: Vec<&&Map> = samples.iter().filter(|s| s["ckpt_started"] == 0).collect();
        assert!(calm.len() * 2 >= samples.len(), "{spec}: {op}: most operations started a checkpoint");
        for s in all.iter().filter(|s| s["ckpt_started"] > 0) {
            let quiet = s.get("quiet").copied().unwrap_or(1) == 1;
            if quiet {
                attributable += 1;
            } else {
                refused += 1;
            }
            if quiet_only && !quiet {
                continue;
            }
            installed += s["ckpt_installed"];
            for k in ["syscalls", "held_syscalls", "held_syncs", "fsync", "full_fsync", "barrier", "locks_process", "catalog_queries"] {
                let Some(&v) = s.get(k) else { continue };
                let base = calm.iter().map(|m| m[k]).min().unwrap();
                *excess.entry(k).or_default() += v as i64 - base as i64;
            }
        }
    }
    if quiet_only {
        assert!(
            installed >= 3,
            "{spec}: premise: at least three checkpoints installed in attributable rounds ({installed}; checkpoint rounds attributable {attributable}, refused {refused})"
        );
    }
    (installed, excess, (attributable, refused))
}

/// Review 1 #7 and the engine's b79dc250e (a fuzzy checkpoint issues no sync inside the store
/// mutex): no operation that started a checkpoint, and no other, saw a sync issued by a thread
/// holding the store mutex. The count is the engine's own, attributed to the holding thread, so it
/// is exact with the checkpoint's thread running.
#[test]
fn a_checkpoint_issues_no_sync_under_the_store_mutex() {
    if in_child() {
        return;
    }
    for spec in ["ckpt_n10", "ckpt_n1e4"] {
        let c = cell(spec);
        let (n, _) = checkpoint_excess(spec);
        for op in ["ckpt_create", "ckpt_first_write"] {
            let held: u64 = values(&c, op, &quiet(&c, op), "held_syncs").iter().sum();
            assert_eq!(held, 0, "{spec}: {op}: {held} syncs under the store mutex over {n} checkpoints [review 1 #7]");
        }
    }
}

/// Review 2 #5 (the cut copied and synced off the mutex; under it only the delta's append and the
/// rename) and review 1 #7 (an O(1) capture): a checkpoint issues at most two syscalls while the
/// store mutex is held. APPROXIMATE: a checkpoint's window runs two threads, and the kernel's count
/// is process-wide, so a hold also counts the other thread's syscalls (base1-base6 read 32-47 over
/// the same three checkpoints).
#[cfg(target_vendor = "apple")]
#[test]
fn a_checkpoint_issues_at_most_two_syscalls_under_the_store_mutex() {
    if in_child() {
        return;
    }
    for spec in ["ckpt_n10", "ckpt_n1e4"] {
        let (n, excess) = checkpoint_excess(spec);
        let held = excess.get("held_syscalls").copied().unwrap_or(0);
        assert!(
            held <= 2 * n as i64,
            "{spec}: {n} checkpoints issued {held} syscalls under the store mutex; budget 2 each [review 2 #5, review 1 #7]"
        );
    }
}

/// Review 1 #7 and b79dc250e's design: a checkpoint's flushes are its catalog commit's (one, and
/// one more for the WAL header after a truncation) and the cut log's temp file (one); the rename's
/// directory sync rides the next flight as one plain fsync. At most three full flushes and one
/// plain sync per checkpoint.
#[test]
fn a_checkpoint_flushes_at_most_three_times() {
    if in_child() {
        return;
    }
    for spec in ["ckpt_n10", "ckpt_n1e4"] {
        let (n, excess) = checkpoint_excess(spec);
        let full = excess.get(FULL).copied().unwrap_or(0);
        let other = excess.get(NOT_FULL).copied().unwrap_or(0) + excess.get("barrier").copied().unwrap_or(0);
        assert!(
            full <= 3 * n as i64 && other <= n as i64,
            "{spec}: {n} checkpoints issued {full} full flushes and {other} other syncs; budget 3 and 1 each [review 1 #7]"
        );
    }
}

/// The lead's selection-bias condition on the CKPT_ROUNDS ruling: the flush budget over EVERY
/// checkpoint-starting round, attributable or not (the engine's flush counters, not the syscall
/// attribution), with each operation's own calm cost subtracted, so a cost that appears only in the
/// overlapping, refused rounds cannot hide behind the selection. Same thresholds as
/// `a_checkpoint_flushes_at_most_three_times`: 3 full flushes and 1 other sync per checkpoint.
#[test]
fn a_checkpoint_flushes_at_most_three_times_over_every_round() {
    if in_child() {
        return;
    }
    for spec in ["ckpt_n10", "ckpt_n1e4"] {
        let (n, excess, (attributable, refused)) = checkpoint_excess_of(spec, false);
        assert!(n >= 3, "{spec}: premise: at least three checkpoints installed over every round ({n})");
        let full = excess.get(FULL).copied().unwrap_or(0);
        let other = excess.get(NOT_FULL).copied().unwrap_or(0) + excess.get("barrier").copied().unwrap_or(0);
        assert!(
            full <= 3 * n as i64 && other <= n as i64,
            "{spec}: over every round ({attributable} attributable, {refused} refused): {n} checkpoints issued {full} full flushes and {other} other syncs; budget 3 and 1 each [review 1 #7]"
        );
    }
}

/// A checkpoint costs the same at 10 and at 10^4 live branches: it writes what changed since the
/// last one, not the store. Syncs and catalog statements are compared exactly. Syscalls are
/// compared within `CKPT_SYSCALL_SLACK`, because the window runs two threads and contention between
/// them adds wait syscalls: base1-base6 read 163-164 at N=10 and 242-251 at 10^4. Syscalls held
/// under the mutex are not compared (see the test above).
#[test]
fn a_checkpoint_costs_the_same_at_10_and_at_10_000_live_branches() {
    if in_child() {
        return;
    }
    const CKPT_SYSCALL_SLACK: f64 = 1.10;
    let (na, a) = checkpoint_excess("ckpt_n10");
    let (nb, b) = checkpoint_excess("ckpt_n1e4");
    assert_eq!(na, nb, "premise: as many checkpoints at both N");
    let mut failures = String::new();
    for k in ["fsync", "full_fsync", "barrier", "catalog_queries", "held_syncs"] {
        if a.get(k) != b.get(k) {
            let _ = writeln!(failures, "  {k}: N=10 {:?}, N=10^4 {:?}", a.get(k), b.get(k));
        }
    }
    if let (Some(&x), Some(&y)) = (a.get("syscalls"), b.get("syscalls")) {
        let ratio = y as f64 / x.max(1) as f64;
        // Two comparisons, not a range's `contains`: a NaN ratio must not read as outside it.
        #[allow(clippy::manual_range_contains)]
        let outside = ratio > CKPT_SYSCALL_SLACK || ratio < 1.0 / CKPT_SYSCALL_SLACK;
        if outside {
            let _ = writeln!(failures, "  syscalls: N=10 {x}, N=10^4 {y} (x{ratio:.2})");
        }
    }
    assert!(failures.is_empty(), "a checkpoint's cost beyond its operation's, summed over {na} checkpoints:\n{failures}");
}

// ---- instructions: CPU work no other counter sees, at D0 (no device flush in the count) ----

/// `op`'s steady instruction count at 10^4 live branches and in the large database, against 10 in
/// the small one, all at D0.
#[cfg(target_vendor = "apple")]
fn assert_instructions(op: &str) {
    if in_child() {
        return;
    }
    let mut failures = Vec::new();
    for b in ["n1e4_small_d0", "n10_large_d0"] {
        if let Err(e) = std::panic::catch_unwind(|| assert_same(op, "n10_small_d0", b, &EXACT_AND_ALLOC)) {
            failures.push(e.downcast_ref::<String>().cloned().unwrap_or_default());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Every counter but instructions (what `assert_instructions` skips: the D2 cells check them).
#[cfg(target_vendor = "apple")]
const EXACT_AND_ALLOC: [&str; 20] = [
    "syscalls",
    "held_syncs",
    "catalog_stmts",
    "catalog_writes",
    "fsync",
    "full_fsync",
    "barrier",
    "held_syscalls",
    "locks",
    "catalog_queries",
    "catalog_rows",
    "catalog_loads",
    "resolves",
    "slot_reads",
    "log_bytes",
    "ckpt_started",
    "allocs",
    "alloc_bytes",
    "held_allocs",
    "held_alloc_bytes",
];

#[cfg(target_vendor = "apple")]
#[test]
fn create_instructions_are_independent_of_live_branches_and_size() {
    assert_instructions("create");
}

#[cfg(target_vendor = "apple")]
#[test]
fn connect_named_instructions_are_independent_of_live_branches_and_size() {
    assert_instructions("connect_named");
}

#[cfg(target_vendor = "apple")]
#[test]
fn first_write_instructions_are_independent_of_live_branches_and_size() {
    assert_instructions("first_write");
}

#[cfg(target_vendor = "apple")]
#[test]
fn disconnect_instructions_are_independent_of_live_branches_and_size() {
    assert_instructions("disconnect");
}

#[cfg(target_vendor = "apple")]
#[test]
fn delete_instructions_are_independent_of_live_branches_and_size() {
    assert_instructions("delete");
}

/// Recovery's steady instructions over the reopens: unnamed populations for the live branches
/// (instructions are process-wide, and the name filter's build is Θ(names) by design), named ones
/// (10 names each) for the size.
#[cfg(target_vendor = "apple")]
#[test]
fn recovery_instructions_are_independent_of_live_branches_and_size() {
    if in_child() {
        return;
    }
    let mut failures = Vec::new();
    for (a, b) in [("n10_small_unnamed_d0", "n1e4_small_unnamed_d0"), ("n10_small_d0", "n10_large_d0")] {
        if let Err(e) = std::panic::catch_unwind(|| assert_recovery_same(a, b, &[], true)) {
            failures.push(e.downcast_ref::<String>().cloned().unwrap_or_default());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ---- LEAP class budgets (DECISIONS.md 2026-10-05T02:58Z, BIG LEAPS): the cost CLASS each leap must
// reach. Red until its leap lands, and the gate the leap is judged by: a leap has landed when its
// class budget is green, measured. L1 (async create) has no API in the engine yet; its class
// budget waits beside the suite (lanes/budgets/leaps/L1-async-create.rs) for the stubs
// ASYNC-DESIGN.md §2 lands first. L5 (the wire fast path) is the wire lane's.

/// LEAP L2, a create path with NO store mutex (per-thread id blocks, an atomic log-buffer
/// reservation, tables that never rehash on the path, a name map readable without the lock): a
/// create takes no store-mutex acquisition at all, its flight's included. That deletes as a class
/// the reds for syscalls, allocations and rehash under the mutex.
#[test]
fn leap_l2_a_create_takes_no_store_mutex() {
    assert_budgets("create", &[every("locks", Want::Exactly(0), "LEAP L2: no store mutex on the create path")]);
}

/// LEAP L3, creates and first writes always ride shared flights: one F_FULLFSYNC carries every
/// acknowledgement in flight. At C clients in a closed loop:
/// - each flush carries at least 0.9·C acknowledgements up to C = 64, for creates and for
///   create-then-first-write cycles (PREREG §11 M2 exit 1: "(creates + deletes) per sync >= 0.9·C";
///   DESIGN §2: one flight in the air, an adaptive hold);
/// - creates per flush RISE with C at every step 1, 8, 64, 256, 1024 (lead, 2026-10-06), "near-linear
///   until CPU-bound" (DECISIONS L3): past 64 the serial section, not the flush, may bound it, so only
///   the rise is held there.
///
/// Alternating cohorts (review 1 #9) give about C/2 per flush: the 0.9·C bound up to 64 catches
/// them; the rise alone above 64 does not (C/2 rises too), and a CPU ceiling past 64 would fail
/// the rise while L3 is reached — that case is argued from its numbers, not waived.
#[test]
fn leap_l3_creates_per_flush_rise_with_the_clients() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    let mut seen = String::new();
    let mut last: Option<(u64, f64)> = None;
    for c in SHARED_CS {
        let spec = format!("shared_c{c}");
        let data = cell(&spec);
        for arm in ["shared_create", "shared_cfw"] {
            let Some(s) = data.ops.get(arm).and_then(|v| v.first()) else {
                assert!(arm == "shared_cfw" && c > 64, "{spec}: no {arm} line");
                continue;
            };
            let (acks, flushes) = (s["acks"], s[FULL]);
            assert!(flushes > 0, "{spec}: {arm}: no flush counted: the instrument saw nothing");
            let per = acks as f64 / flushes as f64;
            let _ = write!(seen, " C={c}/{arm} {per:.2} ({acks}/{flushes});");
            if c <= 64 && per < 0.9 * c as f64 {
                let _ = writeln!(failures, "  C={c}: {arm}: {acks} acknowledgements over {flushes} flushes = {per:.2} per flush, budget >= {:.1}", 0.9 * c as f64);
            }
            if arm == "shared_create" {
                if let Some((lc, lper)) = last {
                    if per <= lper {
                        let _ = writeln!(failures, "  creates per flush did not rise from C={lc} ({lper:.2}) to C={c} ({per:.2})");
                    }
                }
                last = Some((c, per));
            }
        }
    }
    assert!(failures.is_empty(), "LEAP L3 not reached (all:{seen}):\n{failures}");
}

/// LEAP L4, creation is a pure log record: no catalog read and no catalog write on create, connect
/// by name, first write, disconnect or delete, at 10 and at 10^4 live branches and in the large
/// database (budget reds it deletes: connect 1, first write 1, delete 3 queries).
#[test]
fn leap_l4_the_branch_lifecycle_reads_and_writes_no_catalog() {
    for op in ["create", "connect_named", "first_write", "disconnect", "delete"] {
        if let Err(e) = std::panic::catch_unwind(|| {
            assert_budgets(
                op,
                &[
                    every("catalog_stmts", Want::Exactly(0), "LEAP L4: no catalog statement, any connection"),
                    every("catalog_writes", Want::Exactly(0), "LEAP L4: no catalog write"),
                ],
            )
        }) {
            panic!("LEAP L4 not reached: {}", e.downcast_ref::<String>().cloned().unwrap_or_default());
        }
    }
}

/// LEAP L4's open: the in-memory name and branch map is rebuilt from the log, so an open reads the
/// catalog for its meta row only (DESIGN.md §3 Recovery, step 1): at most one catalog query, at
/// both N.
#[test]
fn leap_l4_an_open_reads_only_the_catalog_meta_row() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for spec in ["n10_small", "n1e4_small", "n10_small_unnamed", "n1e4_small_unnamed"] {
        let c = cell(spec);
        for (i, s) in c.ops["recovery"].iter().enumerate() {
            let q = s["catalog_queries"];
            if q > 1 {
                let _ = writeln!(failures, "  {spec}: open {i}: {q} catalog queries, {} rows", s["catalog_rows"]);
            }
        }
    }
    assert!(failures.is_empty(), "LEAP L4 not reached at open (budget: 1 query, the meta row):\n{failures}");
}

/// The L4 memory ruling (DECISIONS.md 2026-10-06T02:55Z (i): "about 50-100 MB at 10^6 live branches
/// ... a budget test measures resident bytes per live branch at 10^4 and 10^5 and asserts the
/// slope"): the heap an open leaves resident once its background work has settled grows by at most
/// 100 B per live branch between 10^4 and 10^5 named branches. Fixed costs cancel in the slope.
/// The count is of REQUESTED heap bytes (the counting allocator's): the allocator's rounding (a
/// 6-9 B name occupies a 16 B block) and pages the buffer pool maps itself are not in it; the
/// kernel's physical footprint, which has them, is reported beside it. BLIND SPOT: a table that
/// grows by doubling is measured at the load these two N give it (hashbrown: 16,384 and 131,072
/// buckets for 10,016 and 100,016 entries), and at 10^6 (2,097,152 buckets) costs more per entry
/// than this slope says. Memory is reported, not a won metric: this is a guard on L4, green
/// before it and required after it.
#[test]
fn leap_l4_an_open_keeps_at_most_100_bytes_resident_per_live_branch() {
    if in_child() {
        return;
    }
    let (a, b) = (cell("mem_n1e4"), cell("mem_n1e5"));
    let (sa, sb) = (&a.ops["memory"][0], &b.ops["memory"][0]);
    assert!(sa["quiet"] == 1 && sb["quiet"] == 1, "a background thread outlived the wait: the heap was still moving");
    let resident = |s: &Map, n: u64| {
        *s.get("resident_bytes").unwrap_or_else(|| {
            panic!("at {n}: the open's resident delta is negative ({:?} B): the measurement is broken", s.get("resident_negative"))
        })
    };
    let (ra, rb) = (resident(sa, N_LARGE), resident(sb, N_HUGE));
    let slope = (rb as f64 - ra as f64) / (N_HUGE - N_LARGE) as f64;
    let foot = match (sa.get("footprint_bytes"), sb.get("footprint_bytes")) {
        (Some(&fa), Some(&fb)) => format!("{:.1} B per branch", (fb as f64 - fa as f64) / (N_HUGE - N_LARGE) as f64),
        _ => "not read".to_string(),
    };
    assert!(
        slope <= 100.0,
        "an open keeps {slope:.1} B resident per live branch ({ra} B at {N_LARGE}, {rb} B at {N_HUGE}; physical footprint {foot}); budget 100 B [L4 memory ruling]"
    );
}

/// The L4 build ruling (the same entry: "the build holds no store mutex for O(N)"): from 10^4 to 10^5
/// live branches, an open and its background work (the name filter's build; under L4, the name
/// map's) hold the store mutex for no more work. Judged ABSOLUTELY (review 2 M7: the x + x/4 term
/// let an O(N) hold up to about 12.9 B per branch hide under the opener's fixed ~1 MiB hold):
/// * the largest single hold by the store's BACKGROUND threads: bytes at most + 4 KiB, catalog rows
///   at most + 64;
/// * the largest single hold by the OPENING thread (marked foreground), the same;
/// * the opening thread's held bytes summed over every hold of the open, at most + 4 KiB;
/// * the all-threads maximum, the same as the per-kind ones.
///
/// Both samples must be quiet (review 2 M7's LOW: a hold still open when the maxima are taken would
/// be lost). The opening thread's held allocations are also held exactly equal between n10 and n1e4
/// by recovery_of_named_branches_is_independent_of_live_branches_on_the_opening_thread (the
/// backstop, red at base: +14 allocations). base14 (f5e9d5693): background 147,464 B at 10^4 ->
/// 1,179,656 B at 10^5 (the filter set's table, built under the mutex; fixed by fastest-engine
/// f315c960d, unrun). Linux finding (b): the held WORK counter (`note_held_work`, one unit per name
/// filter insert) bounds an O(N) fill of the filter under the mutex on every platform. BLIND SPOTS:
/// any other O(N) walk under the mutex that allocates nothing, reads no catalog row and counts no
/// work (the D0 recovery instruction test sees that one, on Apple); a catalog statement that changes
/// many rows counts one (`Stmt::exec`); max_recovery_work counts only `note_held_work`, whose one
/// caller is `FilterTable::insert`, so it reads 0 for replay and an O(N) replay loop of any other kind
/// (map inserts) is invisible to it (budgets-rev-242345 LOW-8: named, not instrumented; replay's own
/// cost is O(records since the checkpoint), which a slack in N could not bound). Mutants env `name_filter_built_under_mutex`,
/// `opener_filter_set`. Run at 242345bec (budgets-rev-242345, ecetesla1): green at base (held work 16 at
/// both sizes), red under name_filter_built_under_mutex (10,000 against 100,000 units).
#[test]
fn leap_l4_an_open_holds_the_store_mutex_for_no_o_n_work() {
    if in_child() {
        return;
    }
    let (a, b) = (cell("mem_n1e4"), cell("mem_n1e5"));
    let (sa, sb) = (&a.ops["memory"][0], &b.ops["memory"][0]);
    assert!(sa["quiet"] == 1 && sb["quiet"] == 1, "premise: both memory samples quiet (10^4: {}, 10^5: {})", sa["quiet"], sb["quiet"]);
    let mut failures = String::new();
    for (k, slack) in [
        ("max_bg_hold_alloc_bytes", 4096u64),
        ("max_bg_hold_catalog_rows", 64),
        ("max_fg_hold_alloc_bytes", 4096),
        ("max_fg_hold_catalog_rows", 64),
        ("fg_held_alloc_bytes_sum", 4096),
        ("max_hold_alloc_bytes", 4096),
        ("max_hold_catalog_rows", 64),
        // Linux finding (b): work that allocates nothing and reads no catalog row (filter inserts). Slack 0
        // (budgets-rev-242345 LOW-1): measured 16 at 10^4 and at 10^5 with no spread, and a slack of 64 hid
        // a planted O(N) term of one unit per 2,000 names (16 -> 50).
        ("max_bg_hold_work", 0),
        ("max_fg_hold_work", 0),
        ("max_hold_work", 0),
        // budgets-rev-242345 LOW-4: the install hold's work apart, so an O(N) term there under the largest
        // other hold is seen.
        ("max_install_hold_work", 0),
        // LOW-5: every other per-name site apart too.
        ("max_seed_hold_work", 0),
        ("max_growth_hold_work", 0),
        ("max_recovery_work", 0),
    ] {
        let (x, y) = (sa[k], sb[k]);
        if y > x + slack {
            let _ = writeln!(failures, "  {k}: {x} at {N_LARGE}, {y} at {N_HUGE}; budget <= {x} + {slack}");
        }
    }
    assert!(failures.is_empty(), "an open's store-mutex holds grow with the live branches:\n{failures}");
}

/// budgets-rev-242345 MED-7: a growth of the name filter holds the store mutex for no more work at 10^5
/// live branches than at 10^4 (slack 0, as the other per-site keys): the memory cells' `growth` line,
/// one growth forced at the next create (`FILTER_GROW_NEXT`). PREMISE: the growth site ran at both sizes
/// (its two holds: at least 2 runs), so a growth that never ran cannot read as 0 units. The open
/// test's own max_growth_hold_work key reads 0/0 by construction (no growth in the open). Mutant
/// `name_filter_growth_work_one_in_10000`.
#[test]
fn leap_l4_a_name_filter_growth_holds_the_store_mutex_for_no_o_n_work() {
    if in_child() {
        return;
    }
    let (a, b) = (cell("mem_n1e4"), cell("mem_n1e5"));
    let (ga, gb) = (&a.ops["growth"][0], &b.ops["growth"][0]);
    assert!(ga["quiet"] == 1 && gb["quiet"] == 1, "premise: both growth samples quiet (10^4: {}, 10^5: {})", ga["quiet"], gb["quiet"]);
    assert!(
        ga["growth_runs"] >= 2 && gb["growth_runs"] >= 2,
        "premise: a growth ran (both its holds) at both sizes (runs: {} at {N_LARGE}, {} at {N_HUGE})",
        ga["growth_runs"],
        gb["growth_runs"]
    );
    let (x, y) = (ga["max_growth_hold_work"], gb["max_growth_hold_work"]);
    assert!(y <= x, "a name filter growth's store-mutex holds grow with the live branches: max_growth_hold_work {x} at {N_LARGE}, {y} at {N_HUGE}; budget <= {x} + 0");
}

/// The confirm arm's rounds as pairs `(a, reference)` of the same round, both quiet (review 2 H6: the
/// rounds are paired by index and the LARGEST difference is budgeted; comparing two minimums hid a
/// cost paid in all but one round). Samples are kept in the order they were measured, one of each op
/// per round, so the j-th of each op is the j-th round. Refused when fewer than half the pairs are
/// quiet, as `quiet` refuses.
fn confirm_pairs<'a>(c: &'a CellData, op: &str) -> Vec<(&'a Map, &'a Map)> {
    let (a, r) = (&c.ops[op], &c.ops["confirm_reference"]);
    assert_eq!(a.len(), r.len(), "{}: {op} and confirm_reference rounds differ", c.name);
    let q = |s: &Map| s.get("quiet").copied().unwrap_or(1) == 1;
    let pairs: Vec<(&Map, &Map)> = a.iter().zip(r).filter(|(x, y)| q(x) && q(y)).collect();
    assert!(!pairs.is_empty() && 2 * pairs.len() >= a.len(), "{}: {op}: {} of {} round pairs quiet", c.name, pairs.len(), a.len());
    pairs
}

/// The largest `x[k] - y[k]` over the pairs, as a signed number.
fn worst_diff(pairs: &[(&Map, &Map)], k: &str) -> i64 {
    pairs.iter().map(|(x, y)| x[k] as i64 - y[k] as i64).max().unwrap()
}

/// Review 6 #1's ruling: the flight's confirmation word leaves the create path for a background
/// writer, which writes it with ONE unsynced pwrite once the group is idle, under no store mutex.
/// An idle-tail create whose word is written costs, beyond the reference create that lands on a
/// pending word (review 2 H2), at most four syscalls — the landing's wake of the parked writer
/// (`confirm_cv.notify_one`), the writer's one timed wait out of the quiet period (woken at the
/// landing, it finds the group idle for less than `confirm_quiet()`), the pwrite, and its one wait
/// to park again — and no sync, no store-mutex acquisition and no syscall under it, on any thread;
/// in EVERY quiet round, paired by index (review 2 H6). CORRECTED before its first run (2026-10-15,
/// a flagged change of this lane's own unrun budget): 18e98e977 said three and left out the timed
/// wait. Four is the bound for this cell's schedule (an idle writer woken at the landing, under the
/// ruled design: a background writer that waits out a quiet period after a landing wakes it): the
/// wake, the quiet period's wait, the write and the park are each one call. It is an upper bound,
/// not a minimum (review C14): a writer first scheduled after the quiet period has passed skips
/// the timed wait. Spurious wake-ups, and the probe's own work, are not in the four. The window
/// ends only once the writer is parked with nothing pending (`writer_settled`, review 2 L3 and
/// C14), so all four land inside it. A word that owns a duplicated descriptor adds its close (and std's
/// debug-build F_GETFD at that close): base13 read +4 against the held create, and an lldb trace of
/// the confirm cell put the writer's pwrite, close and F_GETFD on `branch-confirm`; review 1 #14 /
/// review 2 #6 (one shared descriptor, no dup per flight) remove the close. Mutant
/// `confirm_sync_alternate` (a device flush on every other word). WRITTEN NOT RUN in this form.
#[cfg(target_vendor = "apple")]
#[test]
fn the_confirmation_word_costs_one_unsynced_pwrite_off_the_mutex() {
    if in_child() {
        return;
    }
    let c = cell("confirm_n10");
    let pairs = confirm_pairs(&c, "confirm_written");
    assert!(pairs.iter().all(|(w, _)| w["confirms_written"] == 1), "premise: every written window wrote one word");
    assert!(pairs.iter().all(|(_, r)| r["confirms_written"] == 0), "premise: no reference window wrote a word");
    let mut failures = String::new();
    for k in ["full_fsync", "fsync", "barrier", "locks_process", "held_syscalls"] {
        let d = worst_diff(&pairs, k);
        if d > 0 {
            let _ = writeln!(failures, "  {k}: written - reference reached {d} in a round; budget 0");
        }
    }
    let d = worst_diff(&pairs, "syscalls");
    if d > 4 {
        let _ = writeln!(failures, "  syscalls: written - reference reached {d} in a round; budget <= 4 (the landing's wake, the quiet period's one timed wait, one pwrite, one wait to park)");
    }
    // Review 2 L14: the held per-op cells leave the word out, so the figure to quote for one idle-tail
    // create against PostgreSQL 18 is the written window's own total, word included.
    let total = |k: &str| {
        let v: Vec<u64> = pairs.iter().map(|(w, _)| w[k]).collect();
        (*v.iter().min().unwrap(), *v.iter().max().unwrap())
    };
    let ((s0, s1), (f0, f1), (p0, p1)) = (total("syscalls"), total("full_fsync"), total("fsync"));
    let quote = format!("an idle-tail create, word included: {s0}..{s1} syscalls, {f0}..{f1} F_FULLFSYNC, {p0}..{p1} fsync over {} quiet rounds", pairs.len());
    println!("{quote}");
    assert!(failures.is_empty(), "the confirmation word's cost [review 6 #1]:\n{failures}({quote})");
}

/// Review 2 H2 (an engine SHAVE, filed by the lead): an idle-tail create whose flight lands with no
/// proved word pending wakes the parked confirmation writer on its own acknowledging thread
/// (`confirm_cv.notify_one` under the group lock, before the create returns): one BSD syscall that
/// no held per-op cell sees, since a word is always pending there, and the writer it woke waits
/// again (its quiet period, held: a second syscall, inside the window since review 2 L3's
/// `writer_settled`). Budget: the acknowledgement wakes no thread — held minus reference is 0
/// syscalls in every quiet round. Expected RED today (+2: the notify and the woken writer's wait);
/// the SHAVE moves the wake off the acknowledgement path. WRITTEN NOT RUN.
#[cfg(target_vendor = "apple")]
#[test]
fn an_idle_tail_create_wakes_no_thread_on_its_acknowledgement() {
    if in_child() {
        return;
    }
    let c = cell("confirm_n10");
    let pairs = confirm_pairs(&c, "confirm_held");
    assert!(pairs.iter().all(|(h, r)| h["confirms_written"] == 0 && r["confirms_written"] == 0), "premise: no held or reference window wrote a word");
    let d = worst_diff(&pairs, "syscalls");
    assert!(d <= 0, "an idle-tail create's acknowledgement paid {d} syscalls more than a create landing on a pending word (the writer's wake on the ack path) [review 2 H2, engine SHAVE]; budget 0");
}

// ---- engine 2b: a trunk fork inside a DDL commit's schema window (00340537c, then MED 8 678b18ba4) ----

/// `op`'s schema-window samples in `c`, refused when there are none (review 2 L13: `quiet` keeps every
/// one of them, these samples carry no quiet flag).
fn schema_samples<'a>(c: &'a CellData, op: &str) -> Vec<&'a Map> {
    let v: Vec<&Map> = c.ops.get(op).map(|v| v.iter().collect()).unwrap_or_default();
    assert!(!v.is_empty(), "{}: no {op} sample", c.name);
    v
}

/// The smallest value of `key` over `op`'s samples (the SQL-layer counters are the forking or
/// connecting thread's own, so no other thread's work enters them).
fn schema_min(c: &CellData, op: &str, key: &str) -> u64 {
    values(c, op, &schema_samples(c, op), key).into_iter().min().unwrap()
}

/// The premises of every schema-window sample, one failure line each:
/// * its path: a `first_*` fork took a WAL write lock exactly once (the trunk's first child forks
///   under it), a `lockfree_*` one none (exact, review 2 H3: the lock flag is a bool that any release
///   clears, so ">= 1" would hide a second lock);
/// * its mode: a `*_window` fork found no schema in hand at least once (`windows_met`; the first-child
///   path asks twice, its lock-free attempt and its locked one), a `*_control` fork never (review 2 M2:
///   the premise is a flag, not a budget's equality);
/// * the fork thread let every WAL write lock go (`held_after == 0`, review 2 L4).
fn schema_premises(c: &CellData, op: &str, failures: &mut String) {
    let lock = u64::from(op.starts_with("first"));
    let window = op.ends_with("_window");
    for (i, s) in schema_samples(c, op).into_iter().enumerate() {
        if s["wal_locks"] != lock {
            let _ = writeln!(failures, "  {}: {op} #{i}: wal_locks {}; premise: exactly {lock} (the path the op names)", c.name, s["wal_locks"]);
        }
        if (s["windows_met"] >= 1) != window {
            let _ = writeln!(failures, "  {}: {op} #{i}: windows_met {}; premise: {} (the mode the op names)", c.name, s["windows_met"], if window { ">= 1" } else { "0" });
        }
        if s["held_after"] != 0 {
            let _ = writeln!(failures, "  {}: {op} #{i}: the fork thread still read a WAL write lock as held after the create", c.name);
        }
    }
}

/// Engine 2b (00340537c: "0 otherwise"; MED 8 678b18ba4: a fork outside the window is registered with
/// the schema in hand): a fork that meets no DDL commit's schema window parses nothing, neither at
/// its create (no statement, no schema row) nor at its child's first connection (no schema row), on
/// the first-child path and the lock-free one, at 10 and at 10^3 tables; both paths prepare the same
/// (review 2 H3: the bootstrap is outside the window); and the create's page reads do not grow with
/// the tables. Mutant `schema_reread_always`. WRITTEN NOT RUN.
#[test]
fn a_fork_outside_a_schema_window_rereads_nothing() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    let mut pages = Vec::new();
    for (spec, _) in SCHEMA_TABLES {
        let c = cell(spec);
        for op in ["first_control", "lockfree_control"] {
            schema_premises(&c, op, &mut failures);
            for k in ["prepares", "schema_rows", "connect_schema_rows"] {
                let v = values(&c, op, &schema_samples(&c, op), k);
                if v.iter().any(|&x| x != 0) {
                    let _ = writeln!(failures, "  {spec}: {op}.{k} = {v:?}; budget 0");
                }
            }
            let v = values(&c, op, &schema_samples(&c, op), "page_reads");
            pages.push((spec, op, *v.iter().min().unwrap(), *v.iter().max().unwrap()));
        }
        let (f, l) = (schema_min(&c, "first_control", "prepares"), schema_min(&c, "lockfree_control", "prepares"));
        if f != l {
            let _ = writeln!(failures, "  {spec}: first_control prepares {f}, lockfree_control {l}; budget equal (no bootstrap in the window)");
        }
    }
    for op in ["first_control", "lockfree_control"] {
        let at: Vec<_> = pages.iter().filter(|p| p.1 == op).collect();
        if let [a, b] = &at[..] {
            if (a.2, a.3) != (b.2, b.3) {
                let _ = writeln!(failures, "  {op}.page_reads: {}..{} at {}, {}..{} at {}; budget equal", a.2, a.3, a.0, b.2, b.3, b.0);
            }
        }
    }
    assert!(failures.is_empty(), "a fork outside a schema window parsed something [engine 2b]:\n{failures}");
}

/// Engine 2b as MED 8 left it (678b18ba4: "create in the window 0 schema rows parsed;
/// create-then-first-connect pays the one reparse"; 00340537c: "no wait or retry bound is needed"),
/// against the same path's control:
/// * the create in the window parses no more than the control's create (MED 8's claim, held as a
///   ratchet: prepares and schema rows equal);
/// * the create and the child's first connection together re-read the schema at most once: at most
///   +1 statement, at most + every `sqlite_schema` row once, page reads at most + one plain scan + 1
///   (bounds, not requirements: an engine that publishes the schema with the pages may read less,
///   review 2 M2);
/// * the fork does not wait for the parked commit: not released at 30 s, and no more store waits and
///   no more sleeps (`crate::thread::sleep`, review 2 M3) than the control's. BLIND SPOT: a direct
///   `std::thread::sleep`, or a wait on a condition variable outside `Group::wait`, is not counted;
///   a bounded wait followed by a re-read is caught anyway by the create's parse ratchet;
/// * neither does the child's first connection, made while the DDL is still parked (review C08 #2):
///   not released at 30 s, and no more store waits and sleeps than the control's first connection.
///
/// On both paths, at 10 and at 10^3 tables. Mutants `schema_reread_twice`,
/// `schema_window_waits_for_publish`, `connect_waits_for_publish`, env `fork_rereads_schema_window`.
/// WRITTEN NOT RUN.
#[test]
fn a_fork_inside_a_schema_window_rereads_the_schema_once() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for path in ["first", "lockfree"] {
        let (op, control) = (format!("{path}_window"), format!("{path}_control"));
        for (spec, _) in SCHEMA_TABLES {
            let c = cell(spec);
            schema_premises(&c, &op, &mut failures);
            schema_premises(&c, &control, &mut failures);
            let base = |k: &str| schema_min(&c, &control, k);
            let control_waits = values(&c, &control, &schema_samples(&c, &control), "store_waits").into_iter().max().unwrap();
            let control_sleeps = values(&c, &control, &schema_samples(&c, &control), "sleeps").into_iter().max().unwrap();
            let control_connect_waits = values(&c, &control, &schema_samples(&c, &control), "connect_store_waits").into_iter().max().unwrap();
            let control_connect_sleeps = values(&c, &control, &schema_samples(&c, &control), "connect_sleeps").into_iter().max().unwrap();
            for (i, s) in schema_samples(&c, &op).into_iter().enumerate() {
                let (rows, scan) = (s["schema_table_rows"], s["ref_scan_page_reads"]);
                for k in ["prepares", "schema_rows"] {
                    if s[k] != base(k) {
                        let _ = writeln!(failures, "  {spec}: {op} #{i}: the create's {k} {} against the control's {}; budget equal (MED 8: the create parses nothing)", s[k], base(k));
                    }
                }
                let both = |k: &str| s[k] + s[&format!("connect_{k}")];
                let base_both = |k: &str| base(k) + base(&format!("connect_{k}"));
                if both("prepares") > base_both("prepares") + 1 {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: create + first connect prepared {} against the control's {}; budget <= control + 1", both("prepares"), base_both("prepares"));
                }
                if both("schema_rows") > base_both("schema_rows") + rows {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: create + first connect parsed {} schema rows against the control's {}; budget <= control + {rows} (each row once)", both("schema_rows"), base_both("schema_rows"));
                }
                if both("page_reads") > base_both("page_reads") + scan + 1 {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: create + first connect read {} pages against the control's {}; budget <= control + one scan ({scan}) + 1", both("page_reads"), base_both("page_reads"));
                }
                if s["waited_for_ddl"] != 0 {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: the fork waited for the parked DDL commit (released after 30 s)");
                }
                if s["store_waits"] > control_waits {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: {} store waits against the control's at most {control_waits}; budget no more", s["store_waits"]);
                }
                if s["sleeps"] > control_sleeps {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: {} sleeps against the control's at most {control_sleeps}; budget no more (review 2 M3: a bounded wait for the publication)", s["sleeps"]);
                }
                // Review C08 #2: the child's first connection, made while the DDL is still parked,
                // does not wait for the trunk's publication either.
                if s["connect_waited_for_ddl"] != 0 {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: the child's first connection waited for the parked DDL commit (released after 30 s)");
                }
                if s["connect_store_waits"] > control_connect_waits {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: the first connection made {} store waits against the control's at most {control_connect_waits}; budget no more", s["connect_store_waits"]);
                }
                if s["connect_sleeps"] > control_connect_sleeps {
                    let _ = writeln!(failures, "  {spec}: {op} #{i}: the first connection made {} sleeps against the control's at most {control_connect_sleeps}; budget no more", s["connect_sleeps"]);
                }
            }
        }
    }
    assert!(failures.is_empty(), "a fork inside a schema window parsed more than the schema once [engine 2b]:\n{failures}");
}

/// Engine 2b ("the re-read ... could run under the WAL write lock", MED 8): on the first-child path
/// the fork holds the trunk's WAL write lock, and a window adds NOTHING under it — the statements,
/// schema rows and page reads it makes under the lock equal the control's (review 2 M1: a bound of
/// "control + the re-read" was implied by the other two budgets, and an O(tables) scan moved under
/// the lock passed it). The lock-free path takes no WAL write lock at all, and the control's work
/// under the lock does not grow with the tables. Mutants `lockfree_fork_takes_wal_lock`,
/// `schema_reread_under_lock`, env `fork_rereads_schema_window`. WRITTEN NOT RUN.
#[test]
fn a_schema_window_adds_nothing_under_the_wal_write_lock() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    let mut control_under_lock = Vec::new();
    for (spec, _) in SCHEMA_TABLES {
        let c = cell(spec);
        for op in ["first_control", "first_window", "lockfree_control", "lockfree_window"] {
            schema_premises(&c, op, &mut failures);
        }
        for op in ["lockfree_control", "lockfree_window"] {
            for k in ["wal_locks", "wal_prepares", "wal_page_reads", "wal_schema_rows", "wal_allocs"] {
                let v = values(&c, op, &schema_samples(&c, op), k);
                if v.iter().any(|&x| x != 0) {
                    let _ = writeln!(failures, "  {spec}: {op}.{k} = {v:?}; budget 0 (the lock-free fork takes no WAL write lock)");
                }
            }
        }
        let base = |k: &str| schema_min(&c, "first_control", k);
        for (i, s) in schema_samples(&c, "first_window").into_iter().enumerate() {
            for k in ["wal_prepares", "wal_schema_rows", "wal_page_reads"] {
                if s[k] != base(k) {
                    let _ = writeln!(failures, "  {spec}: first_window #{i}: {k} {} under the WAL write lock against the control's {}; budget equal (nothing added under the lock)", s[k], base(k));
                }
            }
        }
        control_under_lock.push((spec, base("wal_page_reads"), base("wal_prepares"), base("wal_schema_rows")));
    }
    let (a, b) = (control_under_lock[0], control_under_lock[1]);
    if (a.1, a.2, a.3) != (b.1, b.2, b.3) {
        let _ = writeln!(failures, "  first_control under the WAL write lock (page reads, prepares, schema rows): {:?} at {}, {:?} at {}; budget equal", (a.1, a.2, a.3), a.0, (b.1, b.2, b.3), b.0);
    }
    assert!(failures.is_empty(), "the schema window under the WAL write lock [engine 2b]:\n{failures}");
}

/// Review 2 L5: the schema re-read's allocations, budgeted by their slope in tables. On each path the
/// window's extra bytes over its control (the create's and the child's first connection's, each on
/// its own thread) per table at 10^3 tables may exceed the per-table extra at 10 tables by at most
/// 64 B. A cost linear in tables with a fixed part meets this (the fixed part only raises the
/// 10-table figure); one that grows faster than the tables does not (a schema copied once per parsed
/// row costs about tables/2 copies per table). Read generously: the 10^3 cell's smallest extra (its
/// window's least minus its control's most) against the 10 cell's largest. A bound on these samples
/// and the paths they take, not on every allocation path (review C08); pure CPU work is invisible
/// to it. Mutant `schema_parse_quadratic`. WRITTEN NOT RUN.
#[test]
fn a_schema_window_reread_allocates_at_most_linearly_in_tables() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    let [(small, n_small), (large, n_large)] = SCHEMA_TABLES;
    let (cs, cl) = (cell(small), cell(large));
    let (cs, cl) = (&*cs, &*cl);
    let bytes = |c: &CellData, op: &str| -> Vec<u64> {
        schema_samples(c, op).into_iter().map(|s| s["alloc_bytes"] + s["connect_alloc_bytes"]).collect()
    };
    for path in ["first", "lockfree"] {
        let (op, control) = (format!("{path}_window"), format!("{path}_control"));
        for c in [cs, cl] {
            schema_premises(c, &op, &mut failures);
            schema_premises(c, &control, &mut failures);
        }
        let hi_small = bytes(cs, op.as_str()).into_iter().max().unwrap().saturating_sub(bytes(cs, control.as_str()).into_iter().min().unwrap());
        let lo_large = bytes(cl, op.as_str()).into_iter().min().unwrap().saturating_sub(bytes(cl, control.as_str()).into_iter().max().unwrap());
        // Per table, in integers: lo_large / n_large <= hi_small / n_small + 64.
        if lo_large * n_small > (hi_small + 64 * n_small) * n_large {
            let _ = writeln!(
                failures,
                "  {path}: the window's extra bytes per table: at least {} at {n_large} tables against at most {} at {n_small}; budget <= the {n_small}-table figure + 64 B",
                lo_large / n_large,
                hi_small / n_small
            );
        }
    }
    assert!(failures.is_empty(), "the schema re-read's allocations grow faster than the tables [engine 2b, review 2 L5]:\n{failures}");
}

/// Review 2 L6 (an engine FIX, one-time per database): a store's first create creates its catalog,
/// under the store mutex: `Catalog::open` runs 13 `CREATE ... IF NOT EXISTS` statements, each its
/// own commit, sets 2 pragmas and prepares 42 statements, then the meta row commits in a transaction
/// of its own. Budgets, against the reopened reference in the same cell (both creates are a trunk's
/// first child, so both take the trunk's WAL write lock once):
/// * the catalog is created in ONE commit: the fresh create's write transactions exceed the
///   reference's by at most 1 (the schema and the meta row together; today 13 + 1);
/// * nothing is compiled under the store mutex to create it: `held_prepares` equal to the
///   reference's (the catalog's creation needs nothing the mutex guards: it can be made and
///   prepared before the mutex is taken, or at the open);
/// * and it allocates at most 8 KiB under the store mutex beyond the reference's (engine-rev-l2b:
///   bytes, not only commits and statements; the bound leap_l2b uses).
///
/// Flushes are reported, not budgeted (the fresh create also makes the log and the arena). Expected
/// RED today, so no mutant against it can earn a kill before a green baseline exists (review C08). The review's split-transaction mutant is today's code, so this red at base is its
/// kill; once the FIX lands, mutant `catalog_schema_split` splits the transaction again (registered
/// against the fixed code). WRITTEN NOT RUN.
#[test]
fn a_first_create_makes_its_catalog_in_one_commit_off_the_store_mutex() {
    if in_child() {
        return;
    }
    let c = cell("first_create");
    let mut failures = String::new();
    let fresh = schema_samples(&c, "first_fresh");
    let reference = schema_samples(&c, "first_reopened");
    for (i, s) in reference.iter().enumerate() {
        if s["wal_locks"] != 1 {
            let _ = writeln!(failures, "  first_reopened #{i}: wal_locks {}; premise: exactly 1 (the trunk's first child, no catalog write)", s["wal_locks"]);
        }
    }
    let most = |v: &[&Map], k: &str| v.iter().map(|s| s[k]).max().unwrap();
    let least = |v: &[&Map], k: &str| v.iter().map(|s| s[k]).min().unwrap();
    let commits = least(&fresh[..], "wal_locks").saturating_sub(most(&reference[..], "wal_locks"));
    if commits > 1 {
        let _ = writeln!(failures, "  the catalog's creation committed {commits} times (fresh {}..{} write transactions, reference {}..{}); budget <= 1", least(&fresh[..], "wal_locks"), most(&fresh[..], "wal_locks"), least(&reference[..], "wal_locks"), most(&reference[..], "wal_locks"));
    }
    let held = least(&fresh[..], "held_prepares").saturating_sub(most(&reference[..], "held_prepares"));
    if held > 0 {
        let _ = writeln!(failures, "  the catalog's creation compiled {held} statements under the store mutex; budget 0");
    }
    // engine-rev-l2b (via fastest-engine): a byte bound under the store mutex, so a fix that commits once
    // and compiles nothing there cannot still allocate the ~2.6 MB the creation holds today (2,604,646 B at
    // create 0 with 15 live, lane raw 78d2823373). The same instrument and bound as leap_l2b (the thread's
    // allocation bytes under the mutex), over the reopened reference.
    // budgets-rev-242345 LOW-9 (FLAGGED, stricter): the MOST any fresh sample held against the LEAST any
    // reference did, so every fresh sample is bounded, not only the best one.
    let held_bytes = most(&fresh[..], "held_alloc_bytes").saturating_sub(least(&reference[..], "held_alloc_bytes"));
    if held_bytes > 8 << 10 {
        let _ = writeln!(failures, "  the catalog's creation allocated {held_bytes} B under the store mutex; budget <= 8 KiB");
    }
    println!(
        "first create: fresh {}..{} F_FULLFSYNC, {}..{} fsync; reopened {}..{} F_FULLFSYNC, {}..{} fsync",
        least(&fresh[..], "full_fsync"),
        most(&fresh[..], "full_fsync"),
        least(&fresh[..], "fsync"),
        most(&fresh[..], "fsync"),
        least(&reference[..], "full_fsync"),
        most(&reference[..], "full_fsync"),
        least(&reference[..], "fsync"),
        most(&reference[..], "fsync")
    );
    assert!(failures.is_empty(), "a first create's catalog [review 2 L6, engine FIX]:\n{failures}");
}

// ---- contention: C clients creating at once (the shared-flight cells, `SHARED_CS`) ----

/// Every shared-flight line: `(cell, C, arm, sample)`; the create-then-first-write arm runs only up
/// to C = 64. Refuses a cell that lacks an arm it must have.
fn shared_lines() -> Vec<(String, u64, &'static str, Map)> {
    let mut lines = Vec::new();
    for c in SHARED_CS {
        let spec = format!("shared_c{c}");
        let data = cell(&spec);
        for arm in ["shared_create", "shared_cfw", "shared_commits", "shared_serial"] {
            match data.ops.get(arm).and_then(|v| v.first()) {
                Some(s) => lines.push((spec.clone(), c, arm, s.clone())),
                None => assert!(
                    (arm == "shared_cfw" && c > 64) || (arm == "shared_commits" && !(c == 8 || c == 64)) || (arm == "shared_serial" && c != 8),
                    "{spec}: no {arm} line"
                ),
            }
        }
    }
    lines
}

/// Refusals a caller sees under contention (review 2 M5 narrowed this budget's citation): at every C
/// from 1 to 1,024, no create is refused Busy or SchemaUpdated and retried by its caller. The fork's
/// own contract (`BranchStore::fork_trunk`: "The fork itself waits for no trunk commit and never
/// retries"). BLIND SPOTS: the shared arm makes no trunk commit and no DDL, so it reaches no refusal
/// path the engine has today, and retries inside the engine (begin_read_tx's loop,
/// fork_trunk_locked's BusySnapshot retries, wait_durable's `continue`) are not counted here: the
/// futile-lead budget below counts the last; engine review 4's re-capture storm inside creates that
/// succeed is not counted at all. Mutant `lockfree_fork_refuses_every_other`. WRITTEN NOT RUN.
#[test]
fn contention_no_create_is_refused_and_retried_at_any_client_count() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for (spec, c, arm, s) in shared_lines() {
        if s["retries"] != 0 {
            let _ = writeln!(failures, "  {spec}: {arm}: {} creates refused and retried by {c} clients; budget 0", s["retries"]);
        }
    }
    assert!(failures.is_empty(), "creates refused under contention:\n{failures}");
}

/// Group commit's shape (DESIGN §3; PREREG M2): one flight in the air at a time, and it takes
/// everything buffered, so an acknowledgement waits for at most two flights — the one in progress,
/// then its own. At every C from 1 to 1,024 no single acknowledgement (a create, or a first write)
/// makes more than 2 store waits (`store::thread_waits`, one per condition-variable wait call, so a
/// waiter woken and waiting again counts again). A CONSTRUCTION BOUND (review 2 H5, review C08): it
/// holds by the group's shape today and is no independent evidence of contention efficiency; it
/// guards the shape, against a waiter that polls on timed waits or a flight that takes part of the
/// buffer. It cannot see review 1 #9's herd: that is the futile-lead budget's. A PREMISE, not a
/// budget: a line at C >= 8 whose flights were shared (more acknowledgements than device flushes) and
/// that made no store wait at all is refused (a rider must sleep, and a wait outside `Group::wait`, a
/// sleep-and-relock poll, would read as none). Lines with no shared flight are reported, not refused
/// (DECISIONS 6d491917d2 proposal 2; control `shared_serial`, red at 8678d5485): a serialised
/// schedule waits for nothing. Reported per
/// line: the mean per acknowledgement, the worst acknowledgement, and the worst client's waits per
/// its own acknowledgements (the lead's condition, DECISIONS 2026-10-09). Mutants
/// `group_wait_polls`, `poll_outside_group_wait`, `flush_under_mutex`. WRITTEN NOT RUN.
#[test]
fn contention_an_acknowledgement_waits_for_at_most_two_flights_at_any_client_count() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    let mut seen = String::new();
    for (spec, c, arm, s) in shared_lines() {
        // Beside trunk commits a create may also wait for a commit's publication (a different wait,
        // `wait_trunk_commit_published`): that arm is judged by the retries, registrations and
        // futile-lead budgets, not this one.
        if arm == "shared_commits" {
            continue;
        }
        let (acks, waits, locks) = (s["acks"], s["store_waits"], s["store_locks"]);
        let per_client_acks = acks / c;
        let _ = write!(
            seen,
            " C={c}/{arm}: {:.2} waits per ack (worst ack {}, worst client {:.2} per its ack), {:.2} locks per ack;",
            waits as f64 / acks as f64,
            s["store_waits_max_ack"],
            s["store_waits_max_client"] as f64 / per_client_acks as f64,
            locks as f64 / acks as f64,
        );
        if s["store_waits_max_ack"] > 2 {
            let _ = writeln!(failures, "  {spec}: {arm}: one acknowledgement made {} store waits with {c} clients; budget <= 2 [construction bound: group commit's shape]", s["store_waits_max_ack"]);
        }
        // DECISIONS 6d491917d2 (proposal 2): only where the flights were SHARED (more
        // acknowledgements than device flushes) must some waiter have slept; a serialised schedule
        // shares none and waits for none (control `shared_serial`), and the poll mutants share
        // flights, so they still trip it.
        let shared_flights = acks > s[FULL];
        if c >= 8 && shared_flights && waits == 0 {
            let _ = writeln!(failures, "  {spec}: {arm}: {c} clients shared flights ({acks} acknowledgements over {} device flushes) and made no store wait at all: a wait outside Group::wait (premise refused)", s[FULL]);
        }
    }
    assert!(failures.is_empty(), "acknowledgements under contention (all:{seen}):\n{failures}");
}

/// Review 1 #9 (no leader flag, one condition variable: every landing wakes every waiter, and an
/// uncovered one takes the store mutex and the group lock only to find a flight in the air, or a
/// covered one only to return): at every C from 1 to 1,024, no flight waiter takes the store mutex to
/// lead and leads no flight (`probe::futile_leads`, two `cfg(test)` hook lines in
/// `BranchStore::wait_durable_on`, review 2 H5). Expected RED at base: #9 is live. Its engine fix (a
/// leader flag, or waiters woken only when their flight lands) turns it green; mutant
/// `herd_relead` scores only after that fix. WRITTEN NOT RUN.
#[test]
fn contention_no_flight_waiter_leads_in_vain_at_any_client_count() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for (spec, c, arm, s) in shared_lines() {
        if s["futile_leads"] != 0 {
            let _ = writeln!(failures, "  {spec}: {arm}: {} futile leads over {} acknowledgements by {c} clients; budget 0", s["futile_leads"], s["acks"]);
        }
    }
    assert!(failures.is_empty(), "flight waiters took the store mutex to lead and led nothing [review 1 #9]:\n{failures}");
}

// ---- the confirmation writer under load (review 2 H1; the `confirm_load` cell) ----

/// Review 2 H1 and DECISIONS (review 6 #1's ruling: "under load, no confirmation word at all"): with
/// the shipped 5 ms quiet period and C clients making back-to-back creates,
/// * while flights overlap (C >= 8) the writer writes no word during the burst;
/// * the idle tail after a burst gets exactly one word, at every C (DECISIONS 6d491917d2 proposal 3:
///   with the tail settled, none is a word lost; mutant `confirm_tail_lost`);
/// * the writer wakes, with a word pending, at most twice per flight during the burst: once at the
///   landing that wakes the group's condition variable, and at most once on its quiet-period timer
///   between two flights (a stated constant: a poll wakes many times per flight).
///
/// At C = 1 the burst's words are reported and not budgeted: one client's creates do not overlap,
/// and a gap of 5 ms between two of them is a legitimate idle tail. Review C08 #1: a word is the
/// tail's when it is the burst's last landed flight's own and a burst's otherwise (the writer counts
/// them as it writes them, `confirm_tail_words_for_test`), so a parent descheduled past the quiet
/// period does not move the idle tail's honest word into the burst; `load_c8_delayed` is that
/// schedule, a control that must read like `load_c8` (red at a46f64caf, which read the words when
/// the parent did). The burst's idle holes (`idle_holes`: gaps of 5 ms or more with no create in
/// progress) are judged beside it: the C >= 8 clause assumes the creates kept a flight in the air, so
/// an arm with an idle hole is REFUSED as a premise failure naming `idle_holes` (DECISIONS
/// 6d491917d2; control `load_c8_gapped`, judged by its own test). Mutants `confirm_quiet_zero`, env
/// `confirm_poll_100us` and `confirm_holds_slot`. WRITTEN NOT RUN.
#[test]
fn the_confirmation_writer_under_load_writes_no_word_while_flights_overlap() {
    if in_child() {
        return;
    }
    let data = cell("confirm_load");
    let mut failures = String::new();
    let mut seen = String::new();
    for (c, delay_ms) in CONFIRM_LOAD_ARMS {
        let op = load_arm(c, delay_ms);
        let s = data.ops.get(&op).and_then(|v| v.first()).unwrap_or_else(|| panic!("confirm_load: no {op} line"));
        let _ = write!(
            seen,
            " {op}: {} acks, {} flights, words {} in the burst and {} in the tail ({} by the parent's read), {} idle holes, {} wake-ups;",
            s["acks"],
            s["flights"],
            s["confirms_burst"],
            s["confirms_tail"],
            s["confirms_by_read"],
            s["idle_holes"],
            s["wakeups_burst"]
        );
        judge_load_arm(c, &op, s, &mut failures);
    }
    assert!(failures.is_empty(), "the confirmation writer under load (all:{seen}):\n{failures}");
}

/// One `confirm_load` arm's judgement (`c` clients, line `op`): one failure line each, a premise's
/// marked "premise refused".
fn judge_load_arm(c: u64, op: &str, s: &Map, failures: &mut String) {
    let (flights, burst, tail, total, wakes) = (s["flights"], s["confirms_burst"], s["confirms_tail"], s["confirms_total"], s["wakeups_burst"]);
    assert!(flights > 0, "confirm_load: {op}: no flight counted: the instrument saw nothing");
    assert_eq!(burst + tail, total, "confirm_load: {op}: the words attributed do not add up");
    if s["tail_settled"] != 1 {
        let _ = writeln!(failures, "  {op}: the writer never settled in the idle tail (1 s); premise refused: the tail's words are all counted");
    }
    // DECISIONS 6d491917d2 (proposal 1): the C >= 8 clause assumes the creates kept a flight in the
    // air; a burst with an idle hole is refused, never judged by its words (no allowance per hole: a
    // stall can cause the hole). Refusing often is a finding about the load or a stall.
    if c >= 8 && s["idle_holes"] > 0 {
        let _ = writeln!(failures, "  {op}: {} idle_holes (gaps of 5 ms or more with no create in progress); premise refused: back-to-back flights", s["idle_holes"]);
    } else if c >= 8 && burst != 0 {
        let _ = writeln!(failures, "  {op}: {burst} confirmation words written while flights overlapped; budget 0");
    }
    // DECISIONS 6d491917d2 (proposal 3): exactly one; with the tail settled (unheld, nothing pending)
    // a tail with no word is a word lost.
    if tail != 1 {
        let _ = writeln!(failures, "  {op}: {tail} words in the idle tail after the burst; budget == 1");
    }
    if wakes > 2 * flights {
        let _ = writeln!(failures, "  {op}: {wakes} writer wake-ups over {flights} flights; budget <= 2 per flight");
    }
}

/// The reachability arm (lead, DECISIONS 2026-10-15): with every flight in the air past the quiet
/// period (`load_c8_slow_flights`), the writer still writes no word while flights overlap, exactly one
/// in the tail, and wakes at most twice per flight. A writer that polls while a flight is in the air
/// (env mutant `confirm_poll_100us`) wakes many times per flight here, where the shipped schedule's
/// short flights hid it. PREMISE: the flights did outlast the quiet period (the burst's wall time over
/// its flights, at least 5 ms each), or the arm proves nothing and is refused. WRITTEN NOT RUN.
#[test]
fn the_confirmation_writer_wakes_at_most_twice_per_flight_when_flights_outlast_the_quiet_period() {
    if in_child() {
        return;
    }
    let data = cell("confirm_load");
    let s = data.ops.get(LOAD_SLOW).and_then(|v| v.first()).unwrap_or_else(|| panic!("confirm_load: no {LOAD_SLOW} line"));
    let per_flight_us = s["burst_us"] / s["flights"].max(1);
    assert!(
        per_flight_us >= CONFIRM_QUIET_MS_SHIPPED * 1000,
        "premise refused: {LOAD_SLOW}'s flights took {per_flight_us} us each, not past the {CONFIRM_QUIET_MS_SHIPPED} ms quiet period"
    );
    let mut failures = String::new();
    judge_load_arm(8, LOAD_SLOW, s, &mut failures);
    assert!(
        failures.is_empty(),
        "the confirmation writer with flights longer than the quiet period ({} acks, {} flights, {per_flight_us} us each, {} wake-ups, words {} burst / {} tail):\n{failures}",
        s["acks"],
        s["flights"],
        s["wakeups_burst"],
        s["confirms_burst"],
        s["confirms_tail"]
    );
}

/// The reachability arm for the confirmation writer's slot hold (lead, DECISIONS 2026-10-15): the
/// writer is held mid-word (`HOLD_CONFIRM_WRITE`, after it took a word and before it writes it), and a
/// create meanwhile must finish: on unix the writer takes no flight slot, so nothing waits on its word.
/// The env mutant `confirm_holds_slot` holds the slot across the write, so the create waits until the
/// writer is released (the load arms never reach it: the burst writes no word). Run here, in the test
/// process, on a database of its own (no counter is read). WRITTEN NOT RUN.
#[test]
fn a_create_does_not_wait_for_a_confirmation_word_being_written() {
    if in_child() {
        return;
    }
    use super::store::{HOLD_ARRIVED, HOLD_CONFIRM_WRITE};
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("hold.db"));
    let trunk = db.connect().unwrap();
    seed(&trunk, false);
    hold_word(false);
    db.branches.confirm_hold_for_test(HOLD_CONFIRM_WRITE);
    trunk.create_branch("held-0").unwrap();
    let t = std::time::Instant::now();
    while db.branches.confirm_held_for_test() != (HOLD_CONFIRM_WRITE | HOLD_ARRIVED) {
        assert!(t.elapsed() < std::time::Duration::from_secs(5), "premise: the writer never reached its held word");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let db2 = db.clone();
    let create = std::thread::spawn(move || db2.connect().unwrap().create_branch("held-1").map_err(|e| e.to_string()));
    let finished = finished_within(&create, std::time::Duration::from_secs(5));
    let still_held = db.branches.confirm_held_for_test() == (HOLD_CONFIRM_WRITE | HOLD_ARRIVED);
    db.branches.confirm_hold_for_test(0);
    create.join().unwrap().unwrap();
    hold_word(true);
    assert!(still_held, "premise: the writer was released before the create was judged");
    assert!(finished, "a create waited (5 s) for the confirmation writer's word being written: the writer held the flight slot");
}

/// DECISIONS 6d491917d2 (proposal 1): a `confirm_load` arm whose burst had an idle hole (a gap of
/// 5 ms or more with no create in progress) is REFUSED, a premise failure naming `idle_holes`, never
/// judged by its burst words: a stall can cause the hole, and an allowance of one word per hole would
/// excuse both. The control `load_c8_gapped` makes the hole on purpose (every client pauses together
/// for 20 ms half-way through its creates); its judgement must be that refusal, whatever words the
/// gap let the writer write. WRITTEN NOT RUN.
#[test]
fn the_load_judgement_refuses_an_arm_with_an_idle_hole() {
    if in_child() {
        return;
    }
    let data = cell("confirm_load");
    let s = data.ops.get(LOAD_GAPPED).and_then(|v| v.first()).unwrap_or_else(|| panic!("confirm_load: no {LOAD_GAPPED} line"));
    assert!(s["idle_holes"] >= 1, "premise: {LOAD_GAPPED} made no idle hole (the clients' pause did not show)");
    let mut failures = String::new();
    judge_load_arm(8, LOAD_GAPPED, s, &mut failures);
    assert!(
        failures.lines().any(|l| l.contains("premise refused") && l.contains("idle_holes")),
        "an arm with {} idle holes was not refused by its premise (DECISIONS 6d491917d2); its judgement read:\n{failures}",
        s["idle_holes"]
    );
}

// ---- the open's flushes, absolutely (lead order 2026-10-09, DECISIONS 1334833a4d; the open_* cells) ----

/// The premises of an open-flush cell's samples (the log kept: same inode and length; and the tail's
/// confirmation state the cell is built for), each failure one line. `None`: any.
fn open_premises(c: &CellData, confirmed: Option<u64>, failures: &mut String) -> Vec<Map> {
    let v: Vec<Map> = c.ops.get("open").cloned().unwrap_or_default();
    assert!(!v.is_empty(), "{}: no open sample", c.name);
    for (i, s) in v.iter().enumerate() {
        if s["same_inode"] != 1 || s["same_len"] != 1 {
            let _ = writeln!(failures, "  {}: open #{i}: the log was not kept (same inode {}, same length {}); premise", c.name, s["same_inode"], s["same_len"]);
        }
        if let Some(want) = confirmed {
            if s["confirmed"] != want {
                let _ = writeln!(failures, "  {}: open #{i}: confirmed {}; premise {want}", c.name, s["confirmed"]);
            }
        }
    }
    v
}

/// Every open sample's flushes against the budget `(full_fsync, fsync, thread_full, thread_plain,
/// dir_syncs)`, exact both ways.
fn open_exact(c: &CellData, v: &[Map], want: [u64; 5], why: &str, failures: &mut String) {
    let keys = ["full_fsync", "fsync", "thread_full", "thread_plain", "dir_syncs"];
    for (i, s) in v.iter().enumerate() {
        let got: Vec<u64> = keys.iter().map(|k| s[*k]).collect();
        if got[..] != want[..] {
            let _ = writeln!(failures, "  {}: open #{i}: (full_fsync, fsync, thread_full, thread_plain, dir_syncs) = {got:?}; budget {want:?} [{why}]", c.name);
        }
    }
}

/// Engine review 16 LOW 11 (a9ba5adb8: "a syncing open makes one device flush, not two"): a D2 open of
/// a whole log whose last flight is UNCONFIRMED (the writer was killed before its close, so no word
/// proves the flight's sync returned) makes exactly 1 F_FULLFSYNC — the log's, which drains the
/// device after both writes — and exactly 1 plain fsync, the directory's, made first; on the opening
/// thread 1 FullFsync-class sync, 1 Fsync-class sync, 1 directory sync. That is the minimum: the open
/// must make durable the log's bytes (the dead writer may never have synced its last flight) and the
/// log's name (a rename it may not have synced), and on Apple only F_FULLFSYNC drains the device.
/// Mutants env `open_dir_sync_in_class` (the second device flush back) and
/// `open_forgets_unsynced_rename` (no directory sync). WRITTEN NOT RUN.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_open_of_an_unconfirmed_log_makes_one_device_flush_and_one_directory_sync() {
    if in_child() {
        return;
    }
    let c = cell("open_d2_unconfirmed");
    let mut failures = String::new();
    let v = open_premises(&c, Some(0), &mut failures);
    open_exact(&c, &v, [1, 1, 1, 1, 1], "a9ba5adb8: the directory in Fsync, then the log in FullFsync", &mut failures);
    assert!(failures.is_empty(), "a D2 open of an unconfirmed log:\n{failures}");
}

/// The confirmed case (lead, DECISIONS 1334833a4d): a D2 open of a whole log whose header's word
/// confirms its last flight (its sync returned in FullFsync: the word proves it) owes the log's bytes
/// nothing; only the log's name (a rename the closing process may not have synced) remains, so the
/// principled minimum is the directory's sync alone, in a class that drains the device. Which number
/// that is depends on one fact this process records outside every measured open (`dir_full_rc`):
/// * fcntl(F_FULLFSYNC) on a directory descriptor succeeds: exactly 1 F_FULLFSYNC, 0 plain fsync, 1
///   directory sync (on the opening thread 1 FullFsync-class, 0 Fsync-class). RED today: the open
///   re-syncs the log (1 F_FULLFSYNC + 1 fsync), the restart-path engine SHAVE the lead filed;
/// * it fails: a directory cannot be drained directly, so its entry reaches the device only through
///   a device flush after its plain fsync, and today's 1 F_FULLFSYNC + 1 fsync is the minimum.
///
/// No default: a line without `dir_full_rc` refuses. `open_cat_d2` (a catalog store) is the same case
/// and is held to the same budget when its premises hold; when they do not (its open rewrites the
/// log), its numbers stand in the raw as an observation and the test does not fail on it. Mutant env
/// `open_forgets_unsynced_rename`. WRITTEN NOT RUN.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_open_of_a_confirmed_log_syncs_only_its_directory() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for spec in ["open_d2_confirmed", "open_cat_d2"] {
        let c = cell(spec);
        let mut premise = String::new();
        let v = open_premises(&c, Some(1), &mut premise);
        if !premise.is_empty() {
            if spec == "open_cat_d2" {
                continue;
            }
            failures.push_str(&premise);
            continue;
        }
        let rc = v.iter().map(|s| *s.get("dir_full_rc").unwrap_or_else(|| panic!("{spec}: no dir_full_rc recorded (no default)"))).collect::<Vec<_>>();
        assert!(rc.iter().all(|&x| x == rc[0]), "{spec}: dir_full_rc changed between opens: {rc:?}");
        let want = match rc[0] {
            0 => [1, 0, 1, 0, 1],
            1 => [1, 1, 1, 1, 1],
            other => panic!("{spec}: dir_full_rc {other}: not applicable on this platform"),
        };
        let why = if rc[0] == 0 {
            "the directory alone, drained by F_FULLFSYNC on its descriptor (which this Mac accepts)"
        } else {
            "F_FULLFSYNC on a directory is refused here, so the log's device flush after the directory's fsync is the minimum"
        };
        open_exact(&c, &v, want, why, &mut failures);
    }
    assert!(failures.is_empty(), "a D2 open of a confirmed log [the confirmed-tail SHAVE when F_FULLFSYNC on a directory works]:\n{failures}");
}

/// A D1 open of a whole log makes exactly 2 plain fsyncs and no F_FULLFSYNC: the directory's, then
/// the log's; on the opening thread 2 Fsync-class syncs, 1 of them a directory sync. Derivation (lead,
/// accepted): an open that keeps a log it cannot prove durable exposes its records and appends after
/// them, so it must first make durable (a) the log's bytes, whose last flight the previous process may
/// never have synced, and (b) the log's name, whose rename it may never have synced. POSIX:
/// fsync(file) does not make the file's directory entry durable, and fsync(dir) does not make the
/// file's data durable, so these are two syncs. At D1 on Apple the open proves nothing of (a) from
/// its class: its own syncs do not prove stable storage (`journal::proves_stable`: only FullFsync on
/// Apple), and the open's arm is chosen by that class, not by the header's word. The word IS there:
/// the clean close that built the cell wrote it after a full flush of its own
/// (`Confirm::write_at_close`, a81058160), so the premise is `confirmed` 1 (DECISIONS ba515419c3:
/// a premise repair, the budget unchanged; it read "never confirmed", 0, before).
/// Minimum 2; the engine issues 2. Mutant env `open_forgets_unsynced_rename`. WRITTEN NOT RUN.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d1_open_makes_two_plain_fsyncs() {
    if in_child() {
        return;
    }
    let c = cell("open_d1");
    let mut failures = String::new();
    let v = open_premises(&c, Some(1), &mut failures);
    open_exact(&c, &v, [0, 2, 0, 2, 1], "the log's bytes and its name, nothing proven at D1 on Apple", &mut failures);
    assert!(failures.is_empty(), "a D1 open:\n{failures}");
}

/// Review 2 M5: an internal retry (a refusal the caller never sees, retried inside the engine) shows
/// as more trunk-fork registration attempts than creates (`probe::fork_registrations`, a `cfg(test)`
/// hook line at `Connection::fork_trunk_registered`'s entry). At every C, each create registers
/// exactly once: these clients fork a trunk that already has live children, so no create takes the
/// first-child path (which registers twice by design, its lock-free attempt and its locked one).
/// Includes the arm beside trunk commits and a DDL. Mutant `internal_retry_in_fork_trunk`. WRITTEN
/// NOT RUN.
#[test]
fn contention_each_create_registers_once_at_any_client_count() {
    if in_child() {
        return;
    }
    let mut failures = String::new();
    for (spec, c, arm, s) in shared_lines() {
        let creates = if arm == "shared_cfw" { s["acks"] / 2 } else { s["acks"] };
        if s["registrations"] != creates {
            let _ = writeln!(failures, "  {spec}: {arm}: {} fork registrations for {creates} creates by {c} clients; budget equal", s["registrations"]);
        }
    }
    assert!(failures.is_empty(), "creates registered more than once under contention (an internal retry):\n{failures}");
}

// ---- the counted exceptions (DECISIONS bd0d845f97: every run records them; a credited cell has all four at 0) ----

/// The sample keys of `Database::branch_exception_counters()`, in its order: held-free upgrades a
/// freeing operation led past the hard cap, held-free upgrades that took the flight slot to raise
/// the header's class, compactions that synced inline on an operation, operations that waited
/// behind a background compaction. Each is a window's delta.
const EXC_KEYS: [&str; 4] = ["exc_hardcap_upgrades", "exc_header_raise_exclusive", "exc_inline_compaction_syncs", "exc_compaction_waits"];

/// DECISIONS bd0d845f97 (the routing of `branch_exception_counters`): every run records the four
/// counted exceptions, so every window of every engine cell carries all four keys. Checked here on
/// the cells of each producer (the per-operation rounds, recovery, the checkpoint arm, the
/// confirmation arm and the writer under load, a shared-flight arm, the first create, an open).
/// Expected RED at this commit: no window records them. WRITTEN NOT RUN.
#[test]
fn every_window_records_the_four_exception_counters() {
    if in_child() {
        return;
    }
    let mut missing = String::new();
    for spec in MAIN.iter().copied().chain(["ckpt_n10", "confirm_n10", "confirm_load", "shared_c8", "first_create", "open_d2_unconfirmed"]) {
        let data = cell(spec);
        for (op, samples) in &data.ops {
            for (i, s) in samples.iter().enumerate() {
                for key in EXC_KEYS {
                    if !s.contains_key(key) {
                        let _ = writeln!(missing, "  {spec}: {op} #{i}: no {key} recorded");
                    }
                }
            }
        }
        assert!(!data.ops.is_empty(), "{spec}: no sample at all: the check saw nothing");
    }
    assert!(missing.is_empty(), "windows that do not record the counted exceptions (DECISIONS bd0d845f97):\n{missing}");
}

/// DECISIONS bd0d845f97: a cell whose window moved one of the four counted exceptions is not
/// credited: the cell is refused, naming the counter and the window, before any budget reads it.
/// The plant `exc_sharp` moves one for real, not by a planted line: a SHARP catalog store
/// (`BranchCheckpoint::Sharp`) at the checkpoint arm's lowered threshold, whose creates cross it, so
/// a create compacts inline and the engine counts it (`inline_compaction_syncs`; store.rs
/// `maybe_compact`: a catalog store that is not fuzzy compacts on the operation). Its refusal must
/// name `exc_inline_compaction_syncs` as moved (not as missing). The negative control is every other
/// cell: refused only when one of its windows moved one. Expected RED at this commit: the child knows
/// no cell `exc_sharp`, and nothing refuses a moved counter. WRITTEN NOT RUN.
#[test]
fn a_cell_whose_window_moved_an_exception_counter_is_not_credited() {
    if in_child() {
        return;
    }
    let refused = std::panic::catch_unwind(|| cell("exc_sharp"))
        .err()
        .map(|e| e.downcast_ref::<String>().cloned().or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default());
    let text = refused.unwrap_or_else(|| panic!("exc_sharp was credited: no window's inline compaction was counted, or nothing refused it"));
    assert!(
        text.contains("not credited") && text.contains("exc_inline_compaction_syncs moved"),
        "exc_sharp was refused, but not as a moved exception counter (DECISIONS bd0d845f97):\n{text}"
    );
}
