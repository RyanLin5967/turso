//! Branch lifecycles per second against concurrent agents on the DURABLE branch store: what a
//! group commit with the flush outside the store mutex buys (r11-churn lane; the specification is
//! `frontier/round11/r11-churn/PREREG.md` amendment 4 in artie-research, and this file is its
//! implementation).
//!
//!   TURSO_BRANCH_FULLFSYNC=0|1 cargo run -p turso_core --release --example branch_gc -- \
//!       --threads 1,2,4,8,16 [--warm-ms 3000] [--window-ms 15000] [--lease-ms 1000] [--w 1] \
//!       [--durability durable|durable-nosync] [--reap-every-ms 100] [--trunk-every 0]
//!
//! One lifecycle is one agent's branch: fork from the trunk, connect, write `--w` rows on w distinct
//! leaves in one transaction, close; `--lease-ms` later the reaper releases it. For each T in
//! `--threads`, T agent threads run lifecycles back to back (closed loop: each starts its next the
//! moment its last is written), each with its own trunk connection. A fork that finds the trunk's
//! WAL write lock taken by another agent's fork is retried (`Busy`); the retries are counted. One
//! reaper thread wakes every `--reap-every-ms` and releases every branch past its lease with
//! `Database::reap_branches`, one flush per batch (amendment 2's F-G).
//!
//! After `--warm-ms`, the counters are read over `--window-ms`:
//!
//!   lifecycles_per_s   branches reaped per second in the window (each was forked, written and
//!                      released: a whole lifecycle)
//!   arrivals_per_s     branches forked and written per second in the window
//!   fsyncs_per_arrival branch-file fsyncs (the r11-churn instrument) per arrival
//!   flights, waits     group-commit flights led outside the mutex, and operations that waited for
//!                      durability (both 0 on a store without group commit)
//!
//! Which sync the store issues is printed from the instrument: plain fsync(2), or `F_FULLFSYNC`
//! with `TURSO_BRANCH_FULLFSYNC=1`.
//!
//! With `--trunk-every k` (k > 0; r11-churn amendment 6b) the trunk is written too: the agent whose lifecycle
//! number n has n mod k = k - 1 then commits one trunk UPDATE on its own trunk connection, to a separate table `tw`
//! that no branch reads (so the checks below stay exact), rotating over its rows. Every `tw` page is visible to every
//! live child, so each trunk write takes the copy decision a real trunk write takes. Three columns are appended:
//! trunk commits per second, Busy retries per trunk commit, and flushes under the store mutex per trunk commit.
//!
//! Checks (`NOT A RESULT`, exit 1): every 64th lifecycle reads its own row back before closing;
//! after each T the engine's branch count equals the branches still leased; after the last T,
//! every branch is released and the arena is empty.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turso_core::branch::{churn_counters, Branch, BranchDurability};
use turso_core::{
    Connection, Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, SqliteDialect, Value,
    IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
const PAGE_STRIDE: i64 = 312;
const TW_ROWS: u64 = 1_000;

struct Args {
    threads: Vec<usize>,
    warm: Duration,
    window: Duration,
    lease: Duration,
    w: usize,
    durability: BranchDurability,
    reap_every: Duration,
    trunk_every: u64,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_gc: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut args = Args {
        threads: vec![1, 2, 4, 8, 16],
        warm: Duration::from_millis(3000),
        window: Duration::from_millis(15000),
        lease: Duration::from_millis(1000),
        w: 1,
        durability: BranchDurability::Durable { sync: true },
        reap_every: Duration::from_millis(100),
        trunk_every: 0,
    };
    let ms = |s: String, what: &str| -> Duration {
        Duration::from_millis(s.parse().unwrap_or_else(|_| die(&format!("bad {what}"))))
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--threads" => {
                args.threads = val()
                    .split(',')
                    .map(|t| t.parse().unwrap_or_else(|_| die("bad --threads")))
                    .collect()
            }
            "--warm-ms" => args.warm = ms(val(), "--warm-ms"),
            "--window-ms" => args.window = ms(val(), "--window-ms"),
            "--lease-ms" => args.lease = ms(val(), "--lease-ms"),
            "--w" => args.w = val().parse().unwrap_or_else(|_| die("bad --w")),
            "--reap-every-ms" => args.reap_every = ms(val(), "--reap-every-ms"),
            "--trunk-every" => {
                args.trunk_every = val().parse().unwrap_or_else(|_| die("bad --trunk-every"))
            }
            "--durability" => {
                args.durability = match val().as_str() {
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    other => die(&format!("unknown --durability {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.threads.is_empty() || args.threads.iter().any(|&t| t == 0) {
        die("--threads must list positive counts");
    }
    if args.w == 0 || args.w > 64 || args.window.is_zero() {
        die("--w must be in 1..=64 and --window-ms positive");
    }
    args
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn branch_value(id: i64) -> String {
    format!("b{:0>width$}", id, width = VALUE_LEN - 1)
}

fn rows_of(n: u64, w: usize) -> Vec<i64> {
    let first = (n.wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64;
    (0..w as i64)
        .map(|j| (first + j * PAGE_STRIDE) % TRUNK_ROWS + 1)
        .collect()
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    let mut stmt = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {id}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {id}: {} rows", rows.len())),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((p / 100.0) * (sorted.len() - 1) as f64).round() as usize]
}

struct Shared {
    queue: Mutex<VecDeque<(Instant, Branch)>>,
    next_n: AtomicU64,
    arrivals: AtomicU64,
    reaped: AtomicU64,
    busy_retries: AtomicU64,
    trunk_commits: AtomicU64,
    trunk_busy: AtomicU64,
    cycles: Mutex<Vec<u64>>,
    stop_agents: AtomicBool,
    stop_reaper: AtomicBool,
}

fn tw_value(m: u64) -> String {
    format!("w{:0>width$}", m, width = VALUE_LEN - 1)
}

fn agent(shared: &Shared, db: &Arc<Database>, lease: Duration, w: usize, trunk_every: u64) {
    let trunk = db.connect().unwrap();
    while !shared.stop_agents.load(Ordering::Acquire) {
        let start = Instant::now();
        let branch = loop {
            match trunk.fork_branch() {
                Ok(b) => break b,
                // Another agent's fork holds the trunk's WAL write lock.
                Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => {
                    shared.busy_retries.fetch_add(1, Ordering::Relaxed);
                    std::thread::yield_now();
                }
                Err(e) => not_a_result(&format!("fork failed: {e}")),
            }
        };
        let n = shared.next_n.fetch_add(1, Ordering::Relaxed);
        let rows = rows_of(n, w);
        let conn = branch.connect().unwrap();
        if rows.len() == 1 {
            conn.execute(format!(
                "UPDATE t SET v = '{}' WHERE id = {}",
                branch_value(rows[0]),
                rows[0]
            ))
            .unwrap();
        } else {
            conn.execute("BEGIN").unwrap();
            for &r in &rows {
                conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {r}", branch_value(r)))
                    .unwrap();
            }
            conn.execute("COMMIT").unwrap();
        }
        if n % 64 == 0 {
            let other = rows[0] % TRUNK_ROWS + 1;
            if read_v(&conn, rows[0]) != branch_value(rows[0])
                || (!rows.contains(&other) && read_v(&conn, other) != trunk_value(other))
            {
                not_a_result(&format!("lifecycle {n} read a wrong version"));
            }
        }
        drop(conn);
        let cycle = start.elapsed().as_nanos() as u64;
        shared
            .queue
            .lock()
            .unwrap()
            .push_back((Instant::now() + lease, branch));
        shared.arrivals.fetch_add(1, Ordering::Release);
        shared.cycles.lock().unwrap().push(cycle);
        if trunk_every > 0 && n % trunk_every == trunk_every - 1 {
            let m = n / trunk_every;
            let sql = format!(
                "UPDATE tw SET v = '{}' WHERE id = {}",
                tw_value(m),
                m % TW_ROWS + 1
            );
            loop {
                match trunk.execute(&sql) {
                    Ok(_) => break,
                    // A fork holds the trunk's WAL write lock.
                    Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => {
                        shared.trunk_busy.fetch_add(1, Ordering::Relaxed);
                        std::thread::yield_now();
                    }
                    Err(e) => not_a_result(&format!("trunk write failed: {e}")),
                }
            }
            shared.trunk_commits.fetch_add(1, Ordering::Release);
        }
    }
}

fn reaper(shared: &Shared, db: &Arc<Database>, every: Duration) {
    loop {
        let stop = shared.stop_reaper.load(Ordering::Acquire);
        let now = Instant::now();
        let due: Vec<Branch> = {
            let mut q = shared.queue.lock().unwrap();
            let k = q.partition_point(|(deadline, _)| *deadline <= now);
            q.drain(..k).map(|(_, b)| b).collect()
        };
        if !due.is_empty() {
            let k = due.len() as u64;
            let reaped = db.reap_branches(due).unwrap();
            if reaped.iter().any(|r| r.deferred) {
                not_a_result("a flat lifecycle's reap was deferred");
            }
            shared.reaped.fetch_add(k, Ordering::Release);
        }
        if stop {
            return;
        }
        std::thread::sleep(every);
    }
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_gc.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(args.durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = OFF").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    if args.trunk_every > 0 {
        trunk
            .execute("CREATE TABLE tw(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        trunk.execute("BEGIN").unwrap();
        for id in 1..=TW_ROWS {
            trunk
                .execute(format!("INSERT INTO tw VALUES ({id}, '{}')", tw_value(0)))
                .unwrap();
        }
        trunk.execute("COMMIT").unwrap();
    }
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let c0 = churn_counters();
    println!("# branch_gc — Turso fork, DURABLE branch store, concurrent agent lifecycles, r11-churn amendment 4");
    println!(
        "# threads={:?} warm_ms={} window_ms={} lease_ms={} w={} durability={:?} reap_every_ms={} \
         trunk_every={} sync_call={} build={}",
        args.threads,
        args.warm.as_millis(),
        args.window.as_millis(),
        args.lease.as_millis(),
        args.w,
        args.durability,
        args.reap_every.as_millis(),
        args.trunk_every,
        if c0.full_fsync {
            "F_FULLFSYNC"
        } else {
            "fsync(2)"
        },
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
    );
    println!(
        "# gc columns: T lifecycles_per_s arrivals_per_s fsyncs_per_arrival fsyncs_per_s \
         flights_per_s locked_flushes_per_s waits_per_s already_durable_per_s \
         busy_retries_per_arrival cycle_p50_us cycle_p90_us cycle_p99_us cycle_max_us{}",
        if args.trunk_every > 0 {
            " trunk_commits_per_s trunk_busy_per_commit locked_flushes_per_trunk_commit"
        } else {
            ""
        }
    );
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        next_n: AtomicU64::new(0),
        arrivals: AtomicU64::new(0),
        reaped: AtomicU64::new(0),
        busy_retries: AtomicU64::new(0),
        trunk_commits: AtomicU64::new(0),
        trunk_busy: AtomicU64::new(0),
        cycles: Mutex::new(Vec::new()),
        stop_agents: AtomicBool::new(false),
        stop_reaper: AtomicBool::new(false),
    });
    let reaper_thread = {
        let (shared, db, every) = (shared.clone(), db.clone(), args.reap_every);
        std::thread::spawn(move || reaper(&shared, &db, every))
    };
    for &t in &args.threads {
        shared.stop_agents.store(false, Ordering::Release);
        let agents: Vec<_> = (0..t)
            .map(|_| {
                let (shared, db, lease, w, k) =
                    (shared.clone(), db.clone(), args.lease, args.w, args.trunk_every);
                std::thread::spawn(move || agent(&shared, &db, lease, w, k))
            })
            .collect();
        std::thread::sleep(args.warm);
        let snap = |s: &Shared| {
            (
                Instant::now(),
                s.arrivals.load(Ordering::Acquire),
                s.reaped.load(Ordering::Acquire),
                s.busy_retries.load(Ordering::Acquire),
                churn_counters(),
                s.trunk_commits.load(Ordering::Acquire),
                s.trunk_busy.load(Ordering::Acquire),
            )
        };
        shared.cycles.lock().unwrap().clear();
        let (t0, a0, r0, b0, k0, tc0, tb0) = snap(&shared);
        std::thread::sleep(args.window);
        let (t1, a1, r1, b1, k1, tc1, tb1) = snap(&shared);
        let mut cyc: Vec<f64> = std::mem::take(&mut *shared.cycles.lock().unwrap())
            .iter()
            .map(|&n| n as f64 / 1e3)
            .collect();
        cyc.sort_by(|a, b| a.partial_cmp(b).unwrap());
        shared.stop_agents.store(true, Ordering::Release);
        for a in agents {
            a.join().unwrap();
        }
        let secs = (t1 - t0).as_secs_f64();
        let arrivals = (a1 - a0) as f64;
        let per_s = |x: u64| x as f64 / secs;
        let trunk_cols = if args.trunk_every > 0 {
            let commits = (tc1 - tc0) as f64;
            format!(
                " {:.1} {:.4} {:.4}",
                commits / secs,
                (tb1 - tb0) as f64 / commits.max(1.0),
                (k1.gc_locked_flushes - k0.gc_locked_flushes) as f64 / commits.max(1.0)
            )
        } else {
            String::new()
        };
        println!(
            "# gc {t} {:.1} {:.1} {:.4} {:.1} {:.1} {:.1} {:.1} {:.1} {:.4} {:.1} {:.1} {:.1} {:.1}{trunk_cols}",
            per_s(r1 - r0),
            arrivals / secs,
            (k1.fsyncs - k0.fsyncs) as f64 / arrivals.max(1.0),
            per_s(k1.fsyncs - k0.fsyncs),
            per_s(k1.gc_flights - k0.gc_flights),
            per_s(k1.gc_locked_flushes - k0.gc_locked_flushes),
            per_s(k1.gc_waits - k0.gc_waits),
            per_s(k1.gc_already_durable - k0.gc_already_durable),
            (b1 - b0) as f64 / arrivals.max(1.0),
            percentile(&cyc, 50.0),
            percentile(&cyc, 90.0),
            percentile(&cyc, 99.0),
            cyc.last().copied().unwrap_or(0.0),
        );
        // Quiescent between thread counts: agents stopped, and the reaper releases only what is
        // due, so the engine holds exactly the branches still in the table.
        let queued = shared.queue.lock().unwrap().len();
        let engine = db.branch_stats().unwrap().live_branches;
        if engine < queued {
            not_a_result(&format!("engine holds {engine} branches, the table {queued}"));
        }
    }
    shared.stop_reaper.store(true, Ordering::Release);
    reaper_thread.join().unwrap();
    let rest: Vec<Branch> = shared.queue.lock().unwrap().drain(..).map(|(_, b)| b).collect();
    db.reap_branches(rest).unwrap();
    let end = db.branch_stats().unwrap();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown: every branch released, arena empty ({} free slots); lifecycles total {}",
        end.arena_slots_free,
        shared.reaped.load(Ordering::Acquire)
    );
}
