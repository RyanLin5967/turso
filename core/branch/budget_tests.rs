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
const CKPT_ROUNDS: u64 = 240;
/// The shared-flight arm's client counts (lead, 2026-10-06: "creates per F_FULLFSYNC rising with C
/// at C = 1, 8, 64, 256, 1024").
const SHARED_CS: [u64; 5] = [1, 8, 64, 256, 1024];
/// "Hold the confirmation word back": a quiet period no run reaches (about 31.7 years).
const CONFIRM_HELD_MS: u64 = 1 << 40;
/// The memory cells' live branches: 10^4 (`N_LARGE`) and this.
const N_HUGE: u64 = 100_000;
/// Every measured branch name is this long (`m-0000`), so every create logs the same bytes.
const NAME_LEN: u64 = 6;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// D2, except in a `*_d0` cell (the instruction arm: no device flush, whose kernel work makes an
/// operation's instruction count vary by ~15% from one process to the next).
fn opts() -> DatabaseOpts {
    let d0 = std::env::var("FE_BUDGET_CHILD").is_ok_and(|s| s.ends_with("_d0"));
    let sync = if d0 { SyncClass::Off } else { SyncClass::FullFsync };
    DatabaseOpts::new()
        .with_branch_durability(BranchDurability::Catalog { sync })
        .with_branch_checkpoint(BranchCheckpoint::Fuzzy)
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

/// One fork, named `name` or unnamed (detached). A create that loses a race is retried: the
/// population is not measured.
fn fork_one(c: &Arc<Connection>, name: Option<&str>) {
    loop {
        let made = match name {
            Some(name) => c.create_branch(name).map(|_| ()),
            None => c.fork_branch().map(|b| {
                b.into_id();
            }),
        };
        match made {
            Ok(()) => return,
            Err(LimboError::Busy) | Err(LimboError::SchemaUpdated) => continue,
            Err(e) => panic!("populating branch {name:?}: {e}"),
        }
    }
}

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
}

impl Outside {
    fn read(db: &Arc<Database>) -> Self {
        Self {
            log: log_len(db),
            cat: db.branch_catalog_counters(),
            ckpt: db.branch_checkpoint_counters(),
            reads: db.branch_read_counters(),
        }
    }

    fn zero(log: Option<u64>) -> Self {
        Self {
            log,
            cat: (0, 0, 0, 0),
            ckpt: [0; 9],
            reads: (0, 0),
        }
    }
}

fn sample_of(s0: &probe::Snapshot, s1: &probe::Snapshot, o0: &Outside, o1: &Outside) -> Sample {
    let mut s = Sample::new();
    if let Some(d) = kernel_delta(s0.syscalls, s1.syscalls) {
        s.insert("syscalls", d);
    }
    if let (Some(a), Some(b)) = (s0.instructions, s1.instructions) {
        s.insert("instructions", delta(a, b));
    }
    s.insert("fsync", delta(s0.fsync, s1.fsync));
    s.insert("full_fsync", delta(s0.full_fsync, s1.full_fsync));
    s.insert("barrier", delta(s0.barrier, s1.barrier));
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
    s
}

/// Settled: the thread count (persistent store threads left out) is back at `base`, and every
/// other thread is blocked, so what the work woke (the confirmation writer, at a flight's landing)
/// has run and parked again. Mach traps and userspace only.
fn settled(base: Option<u64>) -> bool {
    probe::threads() == base && probe::others_blocked().unwrap_or(true)
}

/// Spin, with no syscall, until `settled(base)` or the deadline; whether it settled.
fn spin_settled(base: Option<u64>, deadline: std::time::Duration) -> bool {
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
/// (a joined thread can still be exiting). `false` if it never settled.
fn quiesce(db: &Arc<Database>, base: Option<u64>) -> bool {
    db.branch_checkpoint_wait();
    let t = std::time::Instant::now();
    loop {
        if settled(base) {
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
    let settled = quiesce(db, base);
    let o0 = Outside::read(db);
    let c0 = db.branch_confirm_counts();
    let s0 = probe::begin();
    let r = f();
    let left = probe::threads() != base;
    let synced = absorb_confirm(&s0);
    let back = spin_settled(base, std::time::Duration::from_secs(30));
    let s1 = probe::end();
    let c1 = db.branch_confirm_counts();
    let o1 = Outside::read(db);
    let mut s = sample_of(&s0, &s1, &o0, &o1);
    let confirms = c1[0] - c0[0];
    s.insert("confirms_written", confirms);
    // A window whose work synced a flight and started no checkpoint (which may take the word
    // itself) must have absorbed the word's write.
    let absorbed = !synced || left || confirms == 1 || WORD_HELD.load(std::sync::atomic::Ordering::Acquire);
    let quiet = settled && back && absorbed && c1[1] == c0[1] && (left || s["allocs_process"] == s["allocs"]);
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
/// the pending word and closes its descriptor on the landing thread, as under load, when a later
/// flight lands inside the quiet period. That is every cell's regime but `confirm_n10`'s, which
/// measures the idle tail's write against it.
fn hold_word(held: bool) {
    use std::sync::atomic::Ordering::Release;
    super::store::CONFIRM_QUIET_MS.store(if held { CONFIRM_HELD_MS } else { 0 }, Release);
    WORD_HELD.store(held, Release);
}

/// The confirmation word (review 6 #1): after a flight whose sync proved stable storage, the
/// store's confirmation thread writes the word once the group has been idle `confirm_quiet()`
/// (5 ms), on its own thread. If the work since `s0` synced, wait that out (with no syscall), so
/// the window that caused the write counts it, as it counted the inline write before review 6 #1
/// (base10). Whether it synced.
fn absorb_confirm(s0: &probe::Snapshot) -> bool {
    let now = sync_counts();
    let synced = now.full_fsync != s0.full_fsync || now.fsync != s0.fsync;
    if synced && !WORD_HELD.load(std::sync::atomic::Ordering::Acquire) {
        let t = std::time::Instant::now();
        while t.elapsed() < std::time::Duration::from_millis(25) {
            std::hint::spin_loop();
        }
    }
    synced
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
        absorb_confirm(&s0);
        let settled = spin_settled(base, std::time::Duration::from_secs(30));
        let s_all = probe::end();
        opened.branch_wait_name_filter();
        let settled = settled && quiesce(&opened, base);
        let o1 = Outside::read(&opened);
        let mut a = sample_of(&s0, &s_open, &Outside::zero(log0), &o1);
        a.insert("quiet", u64::from(t0 == base && t_open == base));
        line(out, cell, "recovery_open", r, &a);
        let mut b = sample_of(&s0, &s_all, &Outside::zero(log0), &o1);
        b.insert("quiet", u64::from(t0 == base && settled));
        line(out, cell, "recovery", r, &b);
        db = Some(opened);
    }
    (db.unwrap(), base)
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
    let ck0 = db.branch_checkpoint_counters();
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
    let ck1 = db.branch_checkpoint_counters();
    for (t, mut s) in lines.into_iter().enumerate() {
        s.insert("ckpt_started", ck1[1] - ck0[1]);
        line(out, cell, "growth_thread", t as u64, &s);
    }
    let live = db.branch_ids().unwrap().len() as u64;
    let mut s = Sample::new();
    s.insert("live", live);
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
        let (_, s_create) = measure(db, base, || trunk.create_branch(&name).unwrap());
        let c = db.connect_named(&name).unwrap();
        let (_, s_write) = measure(db, base, || c.execute(&sql).unwrap());
        drop(c);
        line(out, cell, "ckpt_create", i, &s_create);
        line(out, cell, "ckpt_first_write", i, &s_write);
    }
    super::journal::set_compact_threshold(0);
}

/// The shared-flight arm (LEAP L3): `c` threads, released together, each making `shared_rounds(c)`
/// named creates (arm `shared_create`), then, at C <= 64, as many create-then-first-write cycles
/// (arm `shared_cfw`), closed loop. One line per arm: the durable acknowledgements (creates, plus
/// first writes in the cfw arm) and the flushes issued meanwhile, process-wide. Threads get 8 MiB
/// stacks (RUST_MIN_STACK's 64 MiB times 1024 threads is 64 GiB of reservation).
fn shared(cell: &str, db: &Arc<Database>, c: u64, out: &mut String) {
    let rounds = shared_rounds(c);
    for (arm, cfw) in [("shared_create", false), ("shared_cfw", true)] {
        if cfw && c > 64 {
            continue;
        }
        // A start flag, not a Barrier: a thread that fails to start, or panics before the start,
        // cannot leave the others parked for ever.
        let go = std::sync::atomic::AtomicBool::new(false);
        let s0 = sync_counts();
        std::thread::scope(|s| {
            let started = (0..c).try_for_each(|t| {
                let go = &go;
                std::thread::Builder::new()
                    .stack_size(8 << 20)
                    .spawn_scoped(s, move || {
                        let trunk = db.connect();
                        while !go.load(std::sync::atomic::Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        let trunk = trunk.unwrap();
                        for i in 0..rounds {
                            let name = format!("{arm}-{t}-{i:04}");
                            fork_one(&trunk, Some(&name));
                            if cfw {
                                let b = db.connect_named(&name).unwrap();
                                exec(&b, &format!("UPDATE t SET v = 's{i}' WHERE id = {}", 1 + (t * 7 + i) % 50));
                            }
                        }
                    })
                    .map(|_| ())
            });
            go.store(true, std::sync::atomic::Ordering::Release);
            started.unwrap_or_else(|e| panic!("{cell}: a thread of {c} did not start: {e}"));
        });
        let s1 = sync_counts();
        let mut m = Sample::new();
        m.insert("threads", c);
        m.insert("acks", c * rounds * if cfw { 2 } else { 1 });
        m.insert("full_fsync", s1.full_fsync - s0.full_fsync);
        m.insert("fsync", s1.fsync - s0.fsync);
        m.insert("barrier", s1.barrier - s0.barrier);
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
    let _ = probe::take_hold_maxima();
    let foot0 = probe::phys_footprint();
    let before = probe::live_heap_bytes();
    let db = open_at(&built.path);
    assert_ne!(db.incarnation, built.incarnation, "{cell}: the registry returned the old Database: not a reopen");
    built.incarnation = db.incarnation;
    db.branch_wait_name_filter();
    let settled = quiesce(&db, base);
    let after = probe::live_heap_bytes();
    let (hold_bytes, hold_rows) = probe::take_hold_maxima();
    let foot1 = probe::phys_footprint();
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
    s.insert("quiet", u64::from(settled));
    line(out, cell, "memory", 0, &s);
}

/// The confirmation arm (review 6 #1's fix: the word leaves the create path for a background
/// writer): `k` rounds of a create whose window counts the word's write (`confirm_written`), then
/// the same create with the word held back (`confirm_held`, the engine's test knob). The difference
/// is the word's cost. A held window leaves the writer parked with the word pending and no deadline,
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
        let (_, s_written) = measure(db, base, || trunk.create_branch(&format!("w-{i:04}")).unwrap());
        hold_word(true);
        let (_, s_held) = measure(db, base, || trunk.create_branch(&format!("h-{i:04}")).unwrap());
        if i >= WARMUP {
            line(out, cell, "confirm_written", i - WARMUP, &s_written);
            line(out, cell, "confirm_held", i - WARMUP, &s_held);
        }
    }
    hold_word(false);
}

/// A named create on `db`, so a checkpoint has a dirty branch to write.
fn trunk_for_fc(db: &Arc<Database>) {
    db.connect().unwrap().create_branch("fc-dirty").unwrap();
}

/// The instruments' fire-checks, in the child (nothing else runs there): each counter moves by
/// exactly what was done, and by nothing when nothing was.
fn run_instruments(cell: &str) -> String {
    let mut out = String::new();
    let mut put = |name: &str, s: &Sample| line(&mut out, cell, name, 0, s);
    let window = |f: &mut dyn FnMut()| {
        let s0 = probe::begin();
        f();
        let s1 = probe::end();
        sample_of(&s0, &s1, &Outside::zero(None), &Outside::zero(None))
    };
    put("fc_nothing", &window(&mut || {}));
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
    let (hold_bytes, hold_rows) = probe::take_hold_maxima();
    s.insert("max_hold_alloc_bytes", hold_bytes);
    s.insert("max_hold_catalog_rows", hold_rows);
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
    // A parked thread named as a persistent store thread is not counted, and a parked thread is
    // blocked; a spinning one is not.
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let named = std::thread::Builder::new()
        .name(probe::PERSISTENT_THREADS[0].to_string())
        .spawn(move || rx.recv())
        .unwrap();
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
    s.insert("max_hold_catalog_rows", probe::take_hold_maxima().1);
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
    hold_word(spec != "confirm_n10");
    let mut text = String::new();
    if spec == "instruments" {
        text = run_instruments(&spec);
    } else {
        // `n10_*` / `n1e4_*` cells: `_small` or `_large`, then `_unnamed` (recovery only) and `_d0`.
        let (n, large, named) = match spec.as_str() {
            "ckpt_n10" | "confirm_n10" | "shared_c1" | "shared_c8" | "shared_c64" | "shared_c256" | "shared_c1024" => {
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
        probe::arm(true);
        let opens = if spec.starts_with("n") { 5 } else { 1 };
        if spec.starts_with("mem_") {
            memory(&spec, &mut built, &mut text);
            probe::arm(false);
            std::fs::write(&out, format!("{text}done cell={spec}\n")).unwrap();
            return;
        }
        let (db, base) = recover(&spec, &mut built, opens, &mut text);
        if spec == "confirm_n10" {
            confirm(&spec, &db, base, k, &mut text);
        } else if let Some(c) = spec.strip_prefix("shared_c") {
            shared(&spec, &db, c.parse().unwrap(), &mut text);
        } else if spec.starts_with("growth") {
            growth(&spec, &db, &mut text);
        } else if spec.starts_with("ckpt") {
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
    let mut cells = cells.lock().unwrap_or_else(|e| e.into_inner());
    cells.entry(spec.to_string()).or_insert_with(|| Arc::new(run_child(spec))).clone()
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
    assert_eq!(get("fc_box_23", "allocs"), 23, "23 boxes");
    assert_eq!(get("fc_box_23", "frees"), 23, "23 boxes dropped");
    assert_eq!(get("fc_box_23", "free_bytes"), 23 * 8, "23 boxes of a u64 dropped");
    assert_eq!(get("fc_nothing", "frees"), 0, "an empty window freed");
    assert_eq!(get("fc_box_23", "alloc_bytes"), 23 * 8, "23 boxes of a u64");
    assert_eq!(get("fc_box_23", "held_allocs"), 0, "allocations outside a store mutex counted as held");
    assert_eq!(get("fc_held_5_syscalls_3_allocs", "locks"), 1, "one hold");
    assert_eq!(get("fc_held_5_syscalls_3_allocs", "held_allocs"), 3, "three allocations while held");
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
    assert_eq!(get("fc_thread_count", "after"), get("fc_thread_count", "base"), "an exited thread counted");
    #[cfg(target_vendor = "apple")]
    {
        assert_eq!(get("fc_thread_count", "with_persistent"), get("fc_thread_count", "base"), "a persistent store thread counted");
        assert_eq!(get("fc_thread_count", "parked_blocked"), 1, "a parked thread not read as blocked");
        assert_eq!(get("fc_thread_count", "spinning_seen_running"), 1, "a spinning thread read as blocked");
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
    let c = cell(spec);
    let mut excess: BTreeMap<&'static str, i64> = BTreeMap::new();
    let mut installed = 0;
    for op in ["ckpt_create", "ckpt_first_write"] {
        let samples = quiet(&c, op);
        let calm: Vec<&&Map> = samples.iter().filter(|s| s["ckpt_started"] == 0).collect();
        assert!(calm.len() * 2 >= samples.len(), "{spec}: {op}: most operations started a checkpoint");
        for s in samples.iter().filter(|s| s["ckpt_started"] > 0) {
            installed += s["ckpt_installed"];
            for k in ["syscalls", "held_syscalls", "held_syncs", "fsync", "full_fsync", "barrier", "locks_process", "catalog_queries"] {
                let Some(&v) = s.get(k) else { continue };
                let base = calm.iter().map(|m| m[k]).min().unwrap();
                *excess.entry(k).or_default() += v as i64 - base as i64;
            }
        }
    }
    assert!(installed >= 3, "{spec}: premise: at least three checkpoints installed ({installed})");
    (installed, excess)
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
        if ratio > CKPT_SYSCALL_SLACK || ratio < 1.0 / CKPT_SYSCALL_SLACK {
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

/// The L4 build ruling (the same entry: "the build holds no store mutex for O(N)"): the largest
/// single store-mutex hold during an open and its background work, by any thread, does not grow
/// with the live branches: from 10^4 to 10^5 its allocated bytes grow by at most a quarter plus 4
/// KiB, and its catalog rows by at most a quarter plus 64. An O(N) hold grows tenfold. The size of
/// an O(1) hold is not judged here: base10's opening thread allocated 1,029,243 B under the mutex
/// in 9 holds at every N, so some hold is >= 114 KB at any N (a fix item, not this guard's).
/// BLIND SPOTS: an O(N) walk under the mutex that allocates nothing and reads no catalog row (the
/// D0 recovery instruction test sees that one); a catalog statement that changes many rows counts
/// one (`Stmt::exec`).
#[test]
fn leap_l4_an_open_holds_the_store_mutex_for_no_o_n_work() {
    if in_child() {
        return;
    }
    let (a, b) = (cell("mem_n1e4"), cell("mem_n1e5"));
    let (sa, sb) = (&a.ops["memory"][0], &b.ops["memory"][0]);
    let mut failures = String::new();
    for (k, slack) in [("max_hold_alloc_bytes", 4096u64), ("max_hold_catalog_rows", 64)] {
        let (x, y) = (sa[k], sb[k]);
        if y > x + x / 4 + slack {
            let _ = writeln!(failures, "  {k}: {x} at {N_LARGE}, {y} at {N_HUGE}; budget <= {x} + {x}/4 + {slack}");
        }
    }
    assert!(failures.is_empty(), "an open's largest store-mutex hold grows with the live branches:\n{failures}");
}

/// Review 6 #1's ruling: the flight's confirmation word leaves the create path for a background
/// writer, which writes it with ONE unsynced pwrite once the group is idle, under no store mutex.
/// An idle-tail create whose word is written costs, beyond the same create with the word held, at
/// most two syscalls (the pwrite, and the writer's one wait to park again) and no sync, no
/// store-mutex acquisition and no syscall under it, on any thread. (A word that owns a descriptor
/// of its own adds its close; review 1 #14 / review 2 #6 share the log's.)
#[cfg(target_vendor = "apple")]
#[test]
fn the_confirmation_word_costs_one_unsynced_pwrite_off_the_mutex() {
    if in_child() {
        return;
    }
    let c = cell("confirm_n10");
    let (held, written) = (quiet(&c, "confirm_held"), quiet(&c, "confirm_written"));
    assert!(written.iter().all(|s| s["confirms_written"] == 1), "premise: every written window wrote one word");
    assert!(held.iter().all(|s| s["confirms_written"] == 0), "premise: no held window wrote a word");
    let min = |v: &[&Map], k: &str| values(&c, "confirm", v, k).into_iter().min().unwrap();
    let mut failures = String::new();
    for (k, extra) in [("full_fsync", 0u64), ("fsync", 0), ("barrier", 0), ("locks_process", 0), ("held_syscalls", 0)] {
        let (h, w) = (min(&held, k), min(&written, k));
        if w != h + extra {
            let _ = writeln!(failures, "  {k}: held {h}, written {w}; budget held + {extra}");
        }
    }
    let (h, w) = (min(&held, "syscalls"), min(&written, "syscalls"));
    if w > h + 2 {
        let _ = writeln!(failures, "  syscalls: held {h}, written {w}; budget held + 2 (one pwrite, one wait)");
    }
    assert!(failures.is_empty(), "the confirmation word's cost [review 6 #1]:\n{failures}");
}
