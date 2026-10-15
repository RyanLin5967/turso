//! D48-TURSO-HITPATH — does turso's RESIDENT hit path degrade with thread count?
//!
//! ferrodb's buffer pool scaled NEGATIVELY on its resident hit path (x0.203 at 16T/1T before
//! D44) because every cache HIT took a process-wide lock to update the replacement policy.
//! D44 removed that with BP-Wrapper batching. The open question this answers is whether the
//! WALL is real in an engine nobody here wrote — because a mechanism that removes a wall only
//! ferrodb had is worth nothing.
//!
//! Reading turso's source first makes a falsifiable prediction, and this harness exists to
//! try to break it:
//!
//!   * `core/storage/pager.rs:1353`  `page_cache: Arc<RwLock<PageCache>>`
//!   * `core/storage/pager.rs:3336`  `// Fast path: cache hit.` immediately followed by
//!                                   `let mut page_cache = self.page_cache.write();`
//!   * `core/storage/page_cache.rs:406`  `pub fn get(&mut self, ...)` — `&mut self`, so the
//!     exclusive lock is structural, not incidental: a hit mutates the recency order.
//!
//! So a hit DOES take an exclusive lock on a whole page cache. But:
//!
//!   * `core/database.rs` `_connect` -> `Arc::new(self._init(..))` -> `init_pager` ->
//!     `PageCache::default()` — a NEW Pager, and a NEW PageCache, PER CONNECTION.
//!
//! PREDICTION: because the cache is per-connection, N threads on N connections share no hot
//! line on the hit path, so total throughput should RISE with threads and per-thread
//! throughput should stay roughly FLAT. If per-thread throughput instead collapses the way
//! ferrodb's did, the reading above is wrong somewhere and that is the more interesting
//! outcome.
//!
//! The harness prints the distinct-Pager count so the premise is MEASURED here and not only
//! read: if two connections ever share one Pager, the whole prediction changes and the run
//! says so rather than leaving it to the reader.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

const ROWS: i64 = 2000;
const WARMUP: Duration = Duration::from_millis(300);
const MEASURE: Duration = Duration::from_millis(1000);
const ROUNDS: usize = 3;
const POINTS: [usize; 5] = [1, 2, 4, 8, 16];

/// One thread's loop: the same prepared statement, stepped to completion and reset, until
/// `stop`. Returns (iterations, rows_seen).
fn hammer(
    db: &Arc<Database>,
    key: i64,
    start: &Barrier,
    stop: &AtomicBool,
    warm: Duration,
    hold_txn: bool,
) -> (u64, u64, u64) {
    let conn = db.connect().unwrap();
    let sql = format!("SELECT v FROM t WHERE id = {key}");
    let mut stmt = conn.prepare(&sql).unwrap();

    // D49 ablation. `op_transaction` (core/vdbe/execute.rs:4562) short-circuits when
    // `pager.holds_read_lock()` is already true, so one open read transaction removes the
    // PER-STATEMENT `begin_read_tx` and changes NOTHING else: same Database, same buffer
    // pool, same WAL, same IO, same connection count. That is the ablation the private-
    // Database control could not give, because the control duplicated every shared
    // structure at once.
    if hold_txn {
        conn.execute("BEGIN").unwrap();
    }

    // Untimed warm-up: pull the root and this key's leaf into THIS connection's cache, so the
    // timed region is a hit path and not a first-touch read.
    let t0 = Instant::now();
    while t0.elapsed() < warm {
        step_once(db, &mut stmt);
    }

    start.wait();
    let mut iters = 0u64;
    let mut rows = 0u64;
    let mut ios = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let (r, i) = step_once(db, &mut stmt);
        rows += r;
        ios += i;
        iters += 1;
    }
    (iters, rows, ios)
}

fn step_once(db: &Arc<Database>, stmt: &mut turso_core::Statement) -> (u64, u64) {
    let mut rows = 0u64;
    let mut ios = 0u64;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                std::hint::black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                ios += 1;
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => {
                eprintln!("GUARD: the statement returned Interrupt/Busy; this is not a hit path");
                std::process::exit(2);
            }
        }
    }
    stmt.reset().unwrap();
    (rows, ios)
}

/// `shared`: every thread uses index 0. `private`: thread t uses `dbs[t]`, its own file, its
/// own `Database`, its own IO, its own WAL state, its own buffer-pool arena. The control arm
/// exists because a harness can BE the wall: if the private arm scales and the shared arm does
/// not, the serialization is in state the `Database` shares; if NEITHER scales, it is in the
/// harness or the allocator and nothing may be claimed about turso at all.
fn sweep_point(
    dbs: &[Arc<Database>],
    shared: bool,
    threads: usize,
    hold_txn: bool,
) -> (f64, u64, u64) {
    let start = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total_iters = Arc::new(AtomicU64::new(0));
    let total_rows = Arc::new(AtomicU64::new(0));
    let min_iters = Arc::new(AtomicU64::new(u64::MAX));

    let total_ios = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..threads {
        let db = if shared { dbs[0].clone() } else { dbs[t].clone() };
        let start = start.clone();
        let stop = stop.clone();
        let ti = total_iters.clone();
        let tr = total_rows.clone();
        let mi = min_iters.clone();
        let tio = total_ios.clone();
        // Distinct keys so threads touch distinct leaves and share only the root, which is
        // the shape D44's ferrodb harness used.
        let key = ((t as i64) * 97) % ROWS + 1;
        handles.push(std::thread::spawn(move || {
            let (iters, rows, ios) = hammer(&db, key, &start, &stop, WARMUP, hold_txn);
            ti.fetch_add(iters, Ordering::Relaxed);
            tr.fetch_add(rows, Ordering::Relaxed);
            tio.fetch_add(ios, Ordering::Relaxed);
            mi.fetch_min(iters, Ordering::Relaxed);
        }));
    }

    start.wait();
    let t0 = Instant::now();
    std::thread::sleep(MEASURE);
    stop.store(true, Ordering::Relaxed);
    let elapsed = t0.elapsed();
    for h in handles {
        h.join().unwrap();
    }

    let iters = total_iters.load(Ordering::Relaxed);
    let rows = total_rows.load(Ordering::Relaxed);
    let slowest = min_iters.load(Ordering::Relaxed);

    // A run that collected nothing has not passed.
    if iters == 0 || slowest == 0 {
        eprintln!("GUARD: {threads} threads produced {iters} iterations (slowest thread {slowest})");
        std::process::exit(2);
    }
    // A query that finds no row is not a hit path, however fast it runs.
    if rows != iters {
        eprintln!("GUARD: {iters} iterations returned {rows} rows; each must return exactly one");
        std::process::exit(2);
    }
    (iters as f64 / elapsed.as_secs_f64(), slowest, total_ios.load(Ordering::Relaxed))
}

fn make_db(dir: &std::path::Path, n: usize) -> Arc<Database> {
    let path = dir.join(format!("bench{n}.db"));
    let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    {
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        for i in 1..=ROWS {
            conn.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 7)).unwrap();
        }
    }
    db
}

fn run_arm(dbs: &[Arc<Database>], shared: bool, label: &str) {
    run_arm_txn(dbs, shared, label, false)
}

fn run_arm_txn(dbs: &[Arc<Database>], shared: bool, label: &str, hold_txn: bool) {
    println!();
    println!("=== ARM {label} ===");
    println!("# order is rotated per round: a within-round position bias has constant sign and");
    println!("# cannot be averaged away by repeating the same order.");
    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); POINTS.len()];
    let mut ios: Vec<u64> = vec![0; POINTS.len()];
    for r in 0..ROUNDS {
        let order: Vec<usize> = (0..POINTS.len()).map(|i| (i + r) % POINTS.len()).collect();
        let shown: Vec<String> = order.iter().map(|i| POINTS[*i].to_string()).collect();
        println!("# round {r} order: {}", shown.join(" "));
        for &i in &order {
            let (ops, slowest, io) = sweep_point(dbs, shared, POINTS[i], hold_txn);
            samples[i].push(ops);
            ios[i] = io;
            println!("#   {}T -> {ops:.0} ops/s (slowest thread {slowest} iters, io_steps {io})", POINTS[i]);
        }
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let base = med(&mut samples[0]);
    println!();
    println!("threads   total_ops_s   per_thread_ops_s   total_vs_1T   per_thread_vs_1T   io_steps");
    for (i, &t) in POINTS.iter().enumerate() {
        let m = med(&mut samples[i]);
        println!(
            "{t:>7}   {m:>11.0}   {:>16.0}   {:>11.3}   {:>16.3}   {:>8}",
            m / t as f64,
            m / base,
            (m / t as f64) / base,
            ios[i]
        );
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("d48_hitpath_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let maxt = *POINTS.iter().max().unwrap();
    let dbs: Vec<Arc<Database>> = (0..maxt).map(|n| make_db(&dir, n)).collect();

    // MEASURE the premise rather than only reading it: if two connections share one Pager,
    // they share one PageCache and the prediction changes.
    let probe: Vec<_> = (0..4).map(|_| dbs[0].connect().unwrap()).collect();
    let ptrs: Vec<usize> =
        probe.iter().map(|c| Arc::as_ptr(&c.get_pager()) as *const u8 as usize).collect();
    let mut distinct = ptrs.clone();
    distinct.sort_unstable();
    distinct.dedup();
    println!("# connections probed: {}  distinct Arc<Pager>: {}", ptrs.len(), distinct.len());
    println!(
        "# => the page cache is {} across connections",
        if distinct.len() == ptrs.len() { "PRIVATE (one per connection)" } else { "SHARED" }
    );
    drop(probe);
    println!("# rows={ROWS} warmup={WARMUP:?} measure={MEASURE:?} rounds={ROUNDS} databases={maxt}");

    // Focus mode: hold the shared arm at its worst point long enough for a sampling
    // profiler to attach, so the shared structure is NAMED by a stack rather than guessed.
    if std::env::var("D48_FOCUS").is_ok() {
        println!("# FOCUS: shared arm, 16 threads, pid {}", std::process::id());
        let (ops, slowest, io) = sweep_point(&dbs, true, 16, false);
        println!("# warm: {ops:.0} ops/s (slowest {slowest}, io {io})");
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(25) {
            let (ops, _, _) = sweep_point(&dbs, true, 16, false);
            println!("# hold: {ops:.0} ops/s");
        }
        return;
    }

    run_arm(&dbs, true, "SHARED  (one Database, N connections)");
    run_arm(&dbs, false, "PRIVATE (N Databases, one connection each) -- the CONTROL");

    // Arm C answers the caveat arm A cannot: in a read-only-after-write database the WAL is
    // fully backfilled, so `try_begin_read_tx` takes the `max_frame == nbackfills` branch and
    // EVERY reader piles onto read-mark slot 0. A live writer keeps frames ahead of the
    // backfill point, which is the branch that spreads readers across slots 1..N. If the
    // collapse survives that, it is not an artifact of slot 0.
    println!();
    println!("=== ARM C (shared Database, one live WRITER thread alongside the readers) ===");
    let writer_stop = Arc::new(AtomicBool::new(false));
    let wdb = dbs[0].clone();
    let ws = writer_stop.clone();
    let writer = std::thread::spawn(move || {
        let conn = wdb.connect().unwrap();
        let mut n = 0u64;
        while !ws.load(Ordering::Relaxed) {
            // A row no reader selects, so this changes contention and not the read result.
            if conn.execute(&format!("UPDATE t SET v = {n} WHERE id = {ROWS}")).is_err() {
                break;
            }
            n += 1;
        }
        n
    });
    run_arm(&dbs, true, "SHARED + WRITER");
    writer_stop.store(true, Ordering::Relaxed);
    let wrote = writer.join().unwrap();
    println!("# writer committed {wrote} UPDATEs during arm C");

    // D49-READMARK-ABLATION: arm D.
    run_arm_txn(
        &dbs,
        true,
        "SHARED + one open read txn per reader -- D49-READMARK-ABLATION",
        true,
    );
    if wrote == 0 {
        eprintln!("GUARD: the writer never committed; arm C measured the same regime as arm A");
        std::process::exit(2);
    }

    drop(dbs);
    let _ = std::fs::remove_dir_all(&dir);
}
