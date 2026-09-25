//! Lease churn at a steady live count: the reap ceiling (r11-churn lane; the specification is
//! `frontier/round11/r11-churn/PREREG.md` in artie-research, and this file is its implementation).
//!
//!   cargo run -p turso_core --release --example branch_churn -- \
//!       --lambda 10000 --lease-ms 10000 [--duration-ms ..] [--w 1] [--chain 1] \
//!       [--pause-at-ms .. --pause-ms ..] [--sample-ms ..] [--verify-every 100]
//!
//! Workload. Arrivals come at `--lambda` per second on a fixed schedule (open loop: arrival i is
//! due at t0 + i/lambda whatever happened before; a late forker catches up and its lag is printed).
//! Each arrival forks a branch, gives it a lease of `--lease-ms` from the moment of the fork, opens
//! a connection, writes `--w` rows on w distinct leaves in one transaction, and closes it. With
//! `--chain D`, arrivals form chains of D: arrival i forks from the trunk when i % D == 0 and from
//! arrival i-1 otherwise, so a chain's links expire root first (each then deferred, a live child
//! reads through it) and its tip's reap frees the whole chain at once (the cascade). By Little's
//! law the live count settles at L = lambda * lease.
//!
//! Reaping. A reaper thread pops every entry whose lease has run out (the lease table is a FIFO:
//! one lease length, so deadlines ascend with arrival order) and reaps them one by one through the
//! branch handle — the store's own release path, serially. Between `--pause-at-ms` and
//! `--pause-at-ms + --pause-ms` the reaper pops nothing, so a backlog of lambda * pause builds;
//! from the resume, the time until the reaper next pops a batch no larger than the steady-state
//! batch bound is the DRAIN, and reaps / drain time is the reaper's measured ceiling mu at this L.
//!
//! Sampling. Every `--sample-ms` a sampler thread prints one `# s` line: elapsed time, arrivals,
//! leases run out, reaps, the backlog (run out but not yet reaped), leases still running, the
//! engine's branch count (live + deferred), arena pages in use, RSS, the forker's lag, and the
//! window's reap latency p50/p99/max. At the end, `# summary` lines give the steady-state windows
//! (after the first lease period, outside the pause and drain) and the drain.
//!
//! Checks (a failure prints `NOT A RESULT` and exits 1): every `--verify-every`-th arrival reads a
//! random live branch's own row, the chain root's row and one random row, each against the
//! harness's own model (never against the engine); every reap's freed pages and deferral must match
//! the chain structure (a non-tip link is deferred and frees 0; a tip frees D*w, a flat branch w);
//! the engine's branch count must equal leases running + backlog + deferred links; teardown must
//! leave no branch and no arena page.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turso_core::branch::{Branch, BranchWork};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
/// Rows between two of one branch's writes: more than a leaf holds (~37), so w <= 64 rows land on
/// w distinct leaves (as `branch_arms --arm pages`).
const PAGE_STRIDE: i64 = 312;

struct Args {
    lambda: f64,
    lease: Duration,
    duration: Duration,
    w: usize,
    chain: usize,
    pause_at: Option<Duration>,
    pause: Duration,
    sample: Duration,
    verify_every: usize,
    seed: u64,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_churn: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut args = Args {
        lambda: 1000.0,
        lease: Duration::from_millis(1000),
        duration: Duration::ZERO,
        w: 1,
        chain: 1,
        pause_at: None,
        pause: Duration::ZERO,
        sample: Duration::ZERO,
        verify_every: 100,
        seed: 0x9E37_79B9_7F4A_7C15,
    };
    let ms = |s: String, what: &str| -> Duration {
        Duration::from_millis(s.parse().unwrap_or_else(|_| die(&format!("bad {what}"))))
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--lambda" => args.lambda = val().parse().unwrap_or_else(|_| die("bad --lambda")),
            "--lease-ms" => args.lease = ms(val(), "--lease-ms"),
            "--duration-ms" => args.duration = ms(val(), "--duration-ms"),
            "--w" => args.w = val().parse().unwrap_or_else(|_| die("bad --w")),
            "--chain" => args.chain = val().parse().unwrap_or_else(|_| die("bad --chain")),
            "--pause-at-ms" => args.pause_at = Some(ms(val(), "--pause-at-ms")),
            "--pause-ms" => args.pause = ms(val(), "--pause-ms"),
            "--sample-ms" => args.sample = ms(val(), "--sample-ms"),
            "--verify-every" => {
                args.verify_every = val().parse().unwrap_or_else(|_| die("bad --verify-every"))
            }
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if !(args.lambda > 0.0) || args.lease.is_zero() {
        die("--lambda and --lease-ms must be positive");
    }
    if args.duration.is_zero() {
        args.duration = args.lease * 3;
    }
    if args.sample.is_zero() {
        args.sample = (args.lease / 20).max(Duration::from_millis(50));
    }
    if args.w == 0 || args.w > 64 || args.chain == 0 || args.verify_every == 0 {
        die("--w must be in 1..=64, --chain and --verify-every positive");
    }
    if args.pause_at.is_some_and(|at| at < args.lease || at + args.pause >= args.duration) {
        die("the pause must start after the first lease period and end before --duration-ms");
    }
    args
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn branch_value(id: i64) -> String {
    format!("b{:0>width$}", id, width = VALUE_LEN - 1)
}

/// The rows arrival `n` writes: w rows on w distinct leaves.
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
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn clock_tick_ns() -> f64 {
    let mut min = u128::MAX;
    for _ in 0..10_000 {
        let a = Instant::now();
        let mut b = Instant::now();
        while b == a {
            b = Instant::now();
        }
        min = min.min((b - a).as_nanos());
    }
    min as f64
}

/// One leased branch in the lease table.
struct Entry {
    /// Arrival index.
    n: u64,
    deadline: Instant,
    branch: Branch,
}

/// A reap as the reaper saw it, for the sampler's window and the final checks.
#[derive(Default)]
struct ReapWindow {
    latency_ns: Vec<u64>,
    pages: u64,
    deferred: u64,
    cascades: u64,
    max_pages: u64,
}

struct Shared {
    queue: Mutex<VecDeque<Entry>>,
    /// Entries the reaper has popped and not yet reaped.
    in_hand: AtomicU64,
    arrivals: AtomicU64,
    reaped: AtomicU64,
    /// Chain links reaped while a live child still read through them, not yet freed.
    deferred_now: AtomicU64,
    window: Mutex<ReapWindow>,
    /// Forker lag behind the schedule, max over the current window, ns.
    lag_max_ns: AtomicU64,
    /// Forker cycle (fork .. connection closed), per arrival, for the window.
    cycle_ns: Mutex<Vec<u64>>,
    stop: AtomicBool,
    forking_done: AtomicBool,
    /// Set by the reaper: (resume instant, drain end instant, reaps during the drain).
    drain: Mutex<Option<(Duration, Duration, u64)>>,
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_churn.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    // The trunk is written only at setup; its sync mode is not on any measured path.
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
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let page_size = trunk.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap();

    let lease_s = args.lease.as_secs_f64();
    println!("# branch_churn — Turso fork (volatile arena), lease churn, r11-churn PREREG");
    println!(
        "# lambda={} lease_ms={} duration_ms={} w={} chain={} pause_at_ms={} pause_ms={} \
         sample_ms={} verify_every={} seed={:#x} trunk_rows={TRUNK_ROWS} page_size={page_size} \
         little_L={:.0}",
        args.lambda,
        args.lease.as_millis(),
        args.duration.as_millis(),
        args.w,
        args.chain,
        args.pause_at.map_or(-1, |d| d.as_millis() as i64),
        args.pause.as_millis(),
        args.sample.as_millis(),
        args.verify_every,
        args.seed,
        args.lambda * lease_s,
    );
    println!(
        "# clock tick {:.0} ns (Instant); build: {}; rss_base_bytes={}",
        clock_tick_ns(),
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        rss_bytes()
    );
    println!(
        "# s columns: t_ms arrivals expired reaped backlog leased engine_branches deferred \
         arena_in_use rss_bytes lag_max_ms cycle_p50_us cycle_p99_us reaps_in_window \
         reap_p50_us reap_p99_us reap_max_us pages_freed_in_window max_pages_one_reap \
         cascades_in_window gc_range_in_window"
    );

    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        in_hand: AtomicU64::new(0),
        arrivals: AtomicU64::new(0),
        reaped: AtomicU64::new(0),
        deferred_now: AtomicU64::new(0),
        window: Mutex::new(ReapWindow::default()),
        lag_max_ns: AtomicU64::new(0),
        cycle_ns: Mutex::new(Vec::new()),
        stop: AtomicBool::new(false),
        forking_done: AtomicBool::new(false),
        drain: Mutex::new(None),
    });
    let t0 = Instant::now();
    // Steady-state batch bound: what the reaper pops per wake when it keeps up (it sleeps at most
    // 200 us between pops), with a floor. Past the drain, a pop above it means it fell behind.
    let steady_batch = ((args.lambda * 0.002).ceil() as u64).max(64);
    println!("# steady_batch_bound={steady_batch}");

    let reaper = {
        let shared = shared.clone();
        let (w, chain) = (args.w as u64, args.chain as u64);
        let pause = args.pause_at.map(|at| (at, at + args.pause));
        std::thread::spawn(move || reaper_loop(&shared, t0, w, chain, pause, steady_batch))
    };
    let sampler = {
        let shared = shared.clone();
        let db = db.clone();
        let sample = args.sample;
        std::thread::spawn(move || sampler_loop(&shared, &db, t0, sample))
    };

    forker_loop(&shared, &trunk, &args, t0);
    shared.forking_done.store(true, Ordering::Release);
    reaper.join().unwrap();
    shared.stop.store(true, Ordering::Release);
    let windows = sampler.join().unwrap();
    summarize(&args, &windows, &shared, t0);

    // Quiescent: the reaper has stopped, so the engine's branch count is exactly the entries still
    // in the lease table plus the chain links deferred behind a live successor.
    let queued = shared.queue.lock().unwrap().len() as u64;
    let deferred = shared.deferred_now.load(Ordering::Acquire);
    let engine = db.branch_stats().live_branches as u64;
    if engine != queued + deferred {
        not_a_result(&format!(
            "engine holds {engine} branches, the harness {queued} leased + {deferred} deferred"
        ));
    }
    println!("# quiescent: engine_branches={engine} = leased_or_due {queued} + deferred {deferred}");
    // Teardown: every branch still leased is reaped now (tips first would defer nothing; the
    // handles drop in arrival order, so a chain's links defer until its tip goes).
    let rest: Vec<Entry> = shared.queue.lock().unwrap().drain(..).collect();
    drop(rest);
    let end = db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown: every branch freed, arena empty ({} free slots)",
        end.arena_slots_free
    );
}

fn forker_loop(shared: &Shared, trunk: &Arc<Connection>, args: &Args, t0: Instant) {
    let mut rng = Rng(args.seed);
    let chain = args.chain as u64;
    let interval = 1.0 / args.lambda;
    let mut n: u64 = 0;
    loop {
        let due = t0 + Duration::from_secs_f64(n as f64 * interval);
        if due.duration_since(t0) >= args.duration {
            return;
        }
        let now = Instant::now();
        if now < due {
            std::thread::sleep(due - now);
        } else {
            let lag = (now - due).as_nanos() as u64;
            shared.lag_max_ns.fetch_max(lag, Ordering::Relaxed);
        }
        let start = Instant::now();
        let branch = if n % chain == 0 {
            trunk.fork_branch().unwrap()
        } else {
            // The chain's previous link is the newest entry: it arrived 1/lambda ago with a lease
            // of `lease`, so it is still leased (PREREG: lease >> 1/lambda).
            let q = shared.queue.lock().unwrap();
            let parent = q.back().unwrap_or_else(|| not_a_result("chain parent missing"));
            if parent.n != n - 1 {
                not_a_result(&format!("chain parent of {n} is arrival {}", parent.n));
            }
            parent.branch.fork().unwrap()
        };
        let deadline = start + args.lease;
        let conn = branch.connect().unwrap();
        let rows = rows_of(n, args.w);
        if rows.len() == 1 {
            conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {}", branch_value(rows[0]), rows[0]))
                .unwrap();
        } else {
            conn.execute("BEGIN").unwrap();
            for &r in &rows {
                conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {r}", branch_value(r)))
                    .unwrap();
            }
            conn.execute("COMMIT").unwrap();
        }
        drop(conn);
        let cycle = start.elapsed().as_nanos() as u64;
        shared.cycle_ns.lock().unwrap().push(cycle);
        shared
            .queue
            .lock()
            .unwrap()
            .push_back(Entry { n, deadline, branch });
        shared.arrivals.fetch_add(1, Ordering::Release);
        n += 1;
        if n % args.verify_every as u64 == 0 {
            verify_one(shared, &mut rng, args);
        }
    }
}

/// Read a random leased branch's own row, its chain root's first row, and a random row, against
/// the model: a branch sees branch_value for every row its chain prefix (root .. itself) wrote, and
/// trunk_value for every other row (the trunk is never written after setup).
fn verify_one(shared: &Shared, rng: &mut Rng, args: &Args) {
    let q = shared.queue.lock().unwrap();
    if q.is_empty() {
        return;
    }
    let e = &q[rng.below(q.len())];
    let chain = args.chain as u64;
    let root = e.n - e.n % chain;
    let written = |row: i64| (root..=e.n).any(|m| rows_of(m, args.w).contains(&row));
    let conn = e.branch.connect().unwrap();
    let own = rows_of(e.n, args.w)[0];
    let root_row = rows_of(root, args.w)[0];
    let other = rng.below(TRUNK_ROWS as usize) as i64 + 1;
    for row in [own, root_row, other] {
        let want = if written(row) {
            branch_value(row)
        } else {
            trunk_value(row)
        };
        if read_v(&conn, row) != want {
            not_a_result(&format!("arrival {} read the wrong version of row {row}", e.n));
        }
    }
}

fn reaper_loop(
    shared: &Shared,
    t0: Instant,
    w: u64,
    chain: u64,
    pause: Option<(Duration, Duration)>,
    steady_batch: u64,
) {
    let mut resumed: Option<(Duration, u64)> = None;
    loop {
        // Forking has ended: stop, and let teardown reap what is left. (Reaping on would cut the
        // run's last, incomplete chain at a link that is not its tip.)
        if shared.forking_done.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let since = now - t0;
        if let Some((from, to)) = pause {
            if since >= from && since < to {
                std::thread::sleep(Duration::from_micros(500));
                continue;
            }
            if since >= to && resumed.is_none() && shared.drain.lock().unwrap().is_none() {
                resumed = Some((since, shared.reaped.load(Ordering::Acquire)));
            }
        }
        let batch: Vec<Entry> = {
            let mut q = shared.queue.lock().unwrap();
            let due = q.partition_point(|e| e.deadline <= now);
            shared.in_hand.store(due as u64, Ordering::Release);
            q.drain(..due).collect()
        };
        if let Some((from, reaped0)) = resumed {
            if (batch.len() as u64) <= steady_batch {
                let reaps = shared.reaped.load(Ordering::Acquire) - reaped0;
                *shared.drain.lock().unwrap() = Some((from, since, reaps));
                resumed = None;
            }
        }
        if batch.is_empty() {
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        for e in batch {
            let t = Instant::now();
            let r = e.branch.reap();
            let ns = t.elapsed().as_nanos() as u64;
            // The chain structure fixes every reap's outcome (PREREG): a link with a successor in
            // its chain is deferred and frees nothing; the chain's last link frees the chain.
            let pos = e.n % chain;
            let tip = pos == chain - 1;
            let expect_pages = if tip { chain * w } else { 0 };
            if r.deferred == tip || r.freed_pages as u64 != expect_pages {
                // The run's last chain may be cut short by the end of forking: its last link is
                // the tip, and it cannot reach this reaper (forking ended a lease period earlier).
                not_a_result(&format!(
                    "arrival {} (chain position {pos}) reaped {r:?}, expected {} and {expect_pages} pages",
                    e.n,
                    if tip { "freed" } else { "deferred" }
                ));
            }
            if tip {
                shared.deferred_now.fetch_sub(chain - 1, Ordering::AcqRel);
            } else {
                shared.deferred_now.fetch_add(1, Ordering::AcqRel);
            }
            {
                let mut win = shared.window.lock().unwrap();
                win.latency_ns.push(ns);
                win.pages += r.freed_pages as u64;
                win.deferred += r.deferred as u64;
                win.cascades += (tip && chain > 1) as u64;
                win.max_pages = win.max_pages.max(r.freed_pages as u64);
            }
            shared.reaped.fetch_add(1, Ordering::Release);
            shared.in_hand.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// One sampler window, kept for the summary.
struct Win {
    t: Duration,
    arrivals: u64,
    expired: u64,
    reaped: u64,
    backlog: u64,
    reap_p99_us: f64,
    reap_max_us: f64,
    pages: u64,
    reaps: u64,
}

fn sampler_loop(shared: &Shared, db: &Arc<Database>, t0: Instant, sample: Duration) -> Vec<Win> {
    let mut out = Vec::new();
    let mut next = t0 + sample;
    let mut work0: BranchWork = db.branch_stats().work;
    loop {
        let now = Instant::now();
        if now < next {
            std::thread::sleep(next - now);
        }
        next += sample;
        let done = shared.stop.load(Ordering::Acquire);
        let now = Instant::now();
        let (due_in_queue, leased) = {
            let q = shared.queue.lock().unwrap();
            let due = q.partition_point(|e| e.deadline <= now) as u64;
            (due, q.len() as u64 - due)
        };
        let in_hand = shared.in_hand.load(Ordering::Acquire);
        let reaped = shared.reaped.load(Ordering::Acquire);
        let arrivals = shared.arrivals.load(Ordering::Acquire);
        let backlog = due_in_queue + in_hand;
        let expired = reaped + backlog;
        let stats = db.branch_stats();
        let gc_range = stats.work.gc_range_entries - work0.gc_range_entries;
        work0 = stats.work;
        let win = std::mem::take(&mut *shared.window.lock().unwrap());
        let mut lat: Vec<f64> = win.latency_ns.iter().map(|&n| n as f64 / 1e3).collect();
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut cyc: Vec<f64> = std::mem::take(&mut *shared.cycle_ns.lock().unwrap())
            .iter()
            .map(|&n| n as f64 / 1e3)
            .collect();
        cyc.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let lag = shared.lag_max_ns.swap(0, Ordering::AcqRel) as f64 / 1e6;
        let t = now - t0;
        let deferred = shared.deferred_now.load(Ordering::Acquire);
        println!(
            "# s {} {arrivals} {expired} {reaped} {backlog} {leased} {} {deferred} {} {} {lag:.3} \
             {:.2} {:.2} {} {:.2} {:.2} {:.2} {} {} {} {}",
            t.as_millis(),
            stats.live_branches,
            stats.arena_slots_in_use,
            rss_bytes(),
            percentile(&cyc, 50.0),
            percentile(&cyc, 99.0),
            lat.len(),
            percentile(&lat, 50.0),
            percentile(&lat, 99.0),
            lat.last().copied().unwrap_or(0.0),
            win.pages,
            win.max_pages,
            win.cascades,
            gc_range,
        );
        out.push(Win {
            t,
            arrivals,
            expired,
            reaped,
            backlog,
            reap_p99_us: percentile(&lat, 99.0),
            reap_max_us: lat.last().copied().unwrap_or(0.0),
            pages: win.pages,
            reaps: lat.len() as u64,
        });
        if done {
            return out;
        }
    }
}

fn summarize(args: &Args, wins: &[Win], shared: &Shared, _t0: Instant) {
    let drain = *shared.drain.lock().unwrap();
    let pause = args.pause_at.map(|at| (at, at + args.pause));
    let drain_end = drain.map(|d| d.1);
    // Steady state: after the first lease period plus one window, before forking ends, and
    // outside [pause start, drain end].
    let steady: Vec<&Win> = wins
        .iter()
        .filter(|w| w.t > args.lease + args.sample && w.t <= args.duration)
        .filter(|w| match (pause, drain_end) {
            (Some((from, _)), Some(end)) => w.t < from || w.t > end + args.sample,
            (Some((from, to)), None) => w.t < from || w.t > to,
            _ => true,
        })
        .collect();
    if steady.len() < 2 {
        println!("# summary steady_windows={} (too few to summarise)", steady.len());
    } else {
        let first = steady[0];
        let last = steady[steady.len() - 1];
        let secs = (last.t - first.t).as_secs_f64();
        let exp_rate = (last.expired - first.expired) as f64 / secs;
        let reap_rate = (last.reaped - first.reaped) as f64 / secs;
        let arr_rate = (last.arrivals - first.arrivals) as f64 / secs;
        let max_backlog = steady.iter().map(|w| w.backlog).max().unwrap();
        let mean_backlog =
            steady.iter().map(|w| w.backlog as f64).sum::<f64>() / steady.len() as f64;
        let reaps: u64 = steady.iter().map(|w| w.reaps).sum();
        let pages: u64 = steady.iter().map(|w| w.pages).sum();
        let p99_max = steady.iter().map(|w| w.reap_p99_us).fold(0.0, f64::max);
        let max_us = steady.iter().map(|w| w.reap_max_us).fold(0.0, f64::max);
        println!(
            "# summary steady_windows={} span_s={secs:.1} arrivals_per_s={arr_rate:.1} \
             expiries_per_s={exp_rate:.1} reaps_per_s={reap_rate:.1} backlog_max={max_backlog} \
             backlog_mean={mean_backlog:.1} pages_per_reap={:.4} reap_p99_max_us={p99_max:.2} \
             reap_max_us={max_us:.2}",
            steady.len(),
            if reaps == 0 { 0.0 } else { pages as f64 / reaps as f64 },
        );
    }
    match (pause, drain) {
        (Some((from, to)), Some((resume, end, reaps))) => {
            let d = (end - resume).as_secs_f64();
            let backlog_at_resume = wins
                .iter()
                .filter(|w| w.t <= to)
                .last()
                .map_or(0, |w| w.backlog);
            println!(
                "# summary drain pause_from_ms={} resume_ms={} drain_end_ms={} drain_s={d:.4} \
                 reaps_in_drain={reaps} mu_per_s={:.1} backlog_last_sample_before_resume={backlog_at_resume}",
                from.as_millis(),
                resume.as_millis(),
                end.as_millis(),
                if d > 0.0 { reaps as f64 / d } else { 0.0 },
            );
        }
        (Some(_), None) => println!("# summary drain NOT OBSERVED (the reaper never caught up)"),
        _ => {}
    }
}

