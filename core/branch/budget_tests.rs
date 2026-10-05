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
    let threads = (from + n).saturating_sub(start).clamp(1, 32);
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
    s.insert("catalog_loads", delta(o0.cat.0, o1.cat.0));
    s.insert("catalog_queries", delta(o0.cat.2, o1.cat.2));
    s.insert("catalog_rows", delta(o0.cat.3, o1.cat.3));
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

/// Wait out every background thread: the checkpoint threads joined, and the thread count back at
/// `base` (a joined thread can still be exiting). `false` if it never came back.
fn quiesce(db: &Arc<Database>, base: Option<u64>) -> bool {
    db.branch_checkpoint_wait();
    let t = std::time::Instant::now();
    loop {
        if probe::threads() == base {
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
    let s0 = probe::begin();
    let r = f();
    let left = probe::threads() != base;
    let t = std::time::Instant::now();
    while probe::threads() != base && t.elapsed() < std::time::Duration::from_secs(30) {
        std::hint::spin_loop();
    }
    let s1 = probe::end();
    let back = probe::threads() == base;
    let o1 = Outside::read(db);
    let mut s = sample_of(&s0, &s1, &o0, &o1);
    let quiet = settled && back && (left || s["allocs_process"] == s["allocs"]);
    s.insert("background", u64::from(left));
    s.insert("quiet", u64::from(quiet));
    (r, s)
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
        // The background threads are waited out by spinning on the thread count (no syscall), not by
        // joining them: a join that blocks issues syscalls of its own, as many as the timing makes.
        let t = std::time::Instant::now();
        while probe::threads() != base && t.elapsed() < std::time::Duration::from_secs(30) {
            std::hint::spin_loop();
        }
        let s_all = probe::end();
        let settled = probe::threads() == base;
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
    let (_, s) = measure(&db, base, || db.branch_named("pop-7").unwrap());
    put("fc_lookup_cataloged", &s);
    let (_, s) = measure(&db, base, || db.branch_named("never-seen").unwrap());
    put("fc_lookup_unseen", &s);
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
    let mut text = String::new();
    if spec == "instruments" {
        text = run_instruments(&spec);
    } else {
        // `n10_*` / `n1e4_*` cells: `_small` or `_large`, then `_unnamed` (recovery only) and `_d0`.
        let (n, large, named) = match spec.as_str() {
            "ckpt_n10" => (10, false, true),
            "growth_n1e4" | "ckpt_n1e4" => (N_LARGE, false, true),
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
        let (db, base) = recover(&spec, &mut built, opens, &mut text);
        if spec.starts_with("growth") {
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
    let output = std::process::Command::new(exe)
        .args(["branch::budget_tests::budget_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_BUDGET_CHILD", spec)
        .env("FE_BUDGET_OUT", &out)
        .output()
        .unwrap();
    let text = std::fs::read_to_string(&out).unwrap_or_default();
    if let Ok(dir) = std::env::var("FE_BUDGET_RAW_DIR") {
        let _ = std::fs::create_dir_all(&dir);
        std::fs::write(Path::new(&dir).join(format!("{spec}.txt")), &text).unwrap();
    }
    let _ = std::fs::remove_file(&out);
    assert!(
        output.status.success() && text.ends_with(&format!("done cell={spec}\n")),
        "budget cell {spec}: the child failed ({}):\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
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
const EXACT: [&str; 13] = [
    "syscalls",
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
/// `Commit` (tag, branch, count, page/slot/crc), a `Release` (tag, branch), and the flight end of
/// the current format (tag, start, synced, crc) and of review 2 #8's (17 B, queued in the engine's
/// format bump).
const FORK_NAMED_FRAME: u64 = 8 + 1 + 8 + 8 + 4 + NAME_LEN;
const COMMIT_1_FRAME: u64 = 8 + 1 + 8 + 4 + 12;
const RELEASE_FRAME: u64 = 8 + 1 + 8;
const FLIGHT_END_NOW: u64 = 8 + 1 + 8 + 8 + 4;
const FLIGHT_END_TARGET: u64 = 17;

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
    assert_eq!(get("fc_thread_count", "with"), get("fc_thread_count", "base") + 1, "a live thread uncounted");
    assert_eq!(get("fc_thread_count", "after"), get("fc_thread_count", "base"), "an exited thread counted");
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
fn create_is_within_its_sync_lock_catalog_and_log_budgets() {
    assert_budgets(
        "create",
        &[
            every(FULL, Want::Exactly(1), "DESIGN §3, PREREG §9: one flush per create"),
            every(NOT_FULL, Want::Exactly(0), "DESIGN §3: no other flush"),
            every("barrier", Want::Exactly(0), "DESIGN §3: no barrier"),
            every("locks", Want::AtMost(2), "DESIGN §3: the fork's hold + the flight leader's take"),
            every("catalog_queries", Want::Exactly(0), "review 1 #2: no catalog probe for a new name"),
            every("catalog_loads", Want::Exactly(0), "DESIGN §3: nothing read to fork the trunk"),
            every(
                "log_bytes",
                Want::Exactly(FORK_NAMED_FRAME + FLIGHT_END_NOW),
                "journal.rs format: its own ForkNamed and one flight end",
            ),
        ],
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
fn create_flight_end_is_review_2s_17_bytes() {
    assert_budgets("create", &[every("log_bytes", Want::AtMost(FORK_NAMED_FRAME + FLIGHT_END_TARGET), "review 2 #8: 17 B flight end")]);
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
fn connect_named_is_within_its_sync_lock_catalog_and_log_budgets() {
    assert_budgets(
        "connect_named",
        &[
            every(FULL, Want::Exactly(0), "nothing to make durable"),
            every(NOT_FULL, Want::Exactly(0), "nothing to make durable"),
            every("barrier", Want::Exactly(0), "nothing to order"),
            every("log_bytes", Want::Exactly(0), "nothing to log"),
            every("catalog_queries", Want::Exactly(0), "a resident branch: nothing to read"),
            every("catalog_loads", Want::Exactly(0), "a resident branch: nothing to load"),
            every("locks", Want::AtMost(2), "the name's lookup + open_conn"),
        ],
    );
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
        let held = *values(&c, "connect_named", &quiet(&c, "connect_named"), "held_syscalls").iter().max().unwrap();
        if held > 0 {
            let _ = writeln!(failures, "  {spec}: connect_named {held} syscalls under the store mutex");
        }
    }
    assert!(failures.is_empty(), "connect_named is over budget:\n{failures}");
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
fn first_write_is_within_its_sync_catalog_and_log_budgets() {
    #[cfg(target_vendor = "apple")]
    let order = every("barrier", Want::AtMost(1), "ruling 85a032f01: one ordering sync of the arena");
    #[cfg(not(target_vendor = "apple"))]
    let order = every("barrier", Want::Exactly(0), "no barrier off Apple");
    assert_budgets(
        "first_write",
        &[
            every(FULL, Want::Exactly(1), "PREREG §5, review 1 #1: one full flush"),
            order,
            every("catalog_queries", Want::Exactly(0), "DESIGN §3: a slot and two records"),
            every("log_bytes", Want::Exactly(COMMIT_1_FRAME + FLIGHT_END_NOW), "journal.rs: one Commit, one flight end"),
            every("locks", Want::AtMostPlus("resolves", 2), "one hold per page resolved + commit_pages + the flight leader"),
        ],
    );
    // The arena's ordering sync and the log's flush, and no other: exactly two syncs.
    #[cfg(target_vendor = "apple")]
    assert_budgets(
        "first_write",
        &[every(NOT_FULL, Want::AtMost(1), "ruling 85a032f01: the arena's plain fsync, if not a barrier")],
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
fn first_write_flight_end_is_review_2s_17_bytes() {
    assert_budgets("first_write", &[every("log_bytes", Want::AtMost(COMMIT_1_FRAME + FLIGHT_END_TARGET), "review 2 #8: 17 B flight end")]);
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
fn disconnect_is_within_its_budgets() {
    assert_budgets(
        "disconnect",
        &[
            every(FULL, Want::Exactly(0), "nothing to make durable"),
            every(NOT_FULL, Want::Exactly(0), "nothing to make durable"),
            every("log_bytes", Want::Exactly(0), "nothing to log for an unreleased branch"),
            every("catalog_queries", Want::Exactly(0), "nothing to read"),
            every("locks", Want::AtMost(1), "close's one hold"),
        ],
    );
    #[cfg(target_vendor = "apple")]
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
fn delete_is_within_its_sync_lock_catalog_and_log_budgets() {
    assert_budgets(
        "delete",
        &[
            every(FULL, Want::Exactly(1), "DESIGN §3: the Release rides one flight"),
            every(NOT_FULL, Want::Exactly(0), "DESIGN §3: no other flush"),
            every("barrier", Want::Exactly(0), "no arena write to order"),
            every("locks", Want::AtMost(2), "review 1 #37: lookup + release in one hold, + the flight leader"),
            every("catalog_queries", Want::Exactly(0), "a resident branch: nothing to read"),
            every("log_bytes", Want::Exactly(RELEASE_FRAME + FLIGHT_END_NOW), "DESIGN §3: one Release, one flight end"),
        ],
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
fn delete_flight_end_is_review_2s_17_bytes() {
    assert_budgets("delete", &[every("log_bytes", Want::AtMost(RELEASE_FRAME + FLIGHT_END_TARGET), "review 2 #8: 17 B flight end")]);
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
            for k in ["syscalls", "held_syscalls", "fsync", "full_fsync", "barrier", "locks_process", "catalog_queries"] {
                let Some(&v) = s.get(k) else { continue };
                let base = calm.iter().map(|m| m[k]).min().unwrap();
                *excess.entry(k).or_default() += v as i64 - base as i64;
            }
        }
    }
    assert!(installed >= 3, "{spec}: premise: at least three checkpoints installed ({installed})");
    (installed, excess)
}

/// Review 2 #5 (the cut copied and synced off the mutex; under it only the delta's append and the
/// rename) and review 1 #7 (no sync under the mutex; an O(1) capture): a checkpoint issues at most
/// two syscalls while the store mutex is held.
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
/// last one, not the store.
#[test]
fn a_checkpoint_costs_the_same_at_10_and_at_10_000_live_branches() {
    if in_child() {
        return;
    }
    let (na, a) = checkpoint_excess("ckpt_n10");
    let (nb, b) = checkpoint_excess("ckpt_n1e4");
    assert_eq!(na, nb, "premise: as many checkpoints at both N");
    assert_eq!(a, b, "a checkpoint's cost beyond its operation's, summed over {na} checkpoints");
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
const EXACT_AND_ALLOC: [&str; 17] = [
    "syscalls",
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
