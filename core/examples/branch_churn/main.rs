//! Lease churn at a steady live count on the DURABLE branch store (`ec168128b`): the reap ceiling
//! when every release is logged and fsynced (r11-churn lane; the specification is
//! `frontier/round11/r11-churn/PREREG.md` in artie-research, §6 and amendment 1, and this file is
//! its implementation). A port of the volatile store's `branch_churn` (turso branch r11-churn).
//!
//!   cargo run -p turso_core --release --example branch_churn -- \
//!       --mode serial|lease --durability durable|durable-nosync \
//!       --lambda 2000 --lease-ms 5000 [--duration-ms ..] [--w 1] [--chain 1] \
//!       [--pause-at-ms .. --pause-ms ..] [--sample-ms ..] [--expire-every-ms 100] [--verify-every 100]
//!
//! Workload: as the volatile harness. Arrivals at `--lambda` per second, open loop; each forks a
//! branch (from the trunk, or with `--chain D` from the previous arrival inside a chain of D),
//! connects, writes `--w` rows on w distinct leaves in one transaction, and closes.
//!
//! Two reapers:
//!
//!   serial  The harness keeps the lease table (deadline = fork + lease). A reaper thread pops what
//!           has run out and reaps each branch through its handle (`Branch::reap`), one release,
//!           one log record, one flush per call — ferrodb's REAP_CHUNK = 1 shape. With a pause,
//!           the drain measures the reaper's ceiling mu as in the volatile harness.
//!   lease   The STORE keeps the leases (`DatabaseOpts::with_branch_lease`): every fork runs an
//!           expiry pass inline (store.rs `fork_trunk` -> `expire`), and a background thread calls
//!           `Database::expire_branches` every `--expire-every-ms`. The harness holds no handle once a
//!           branch is written (`Branch::into_id`), except the chain link the next arrival forks
//!           from. The backlog is the store's own count of leases run out and not yet reaped. No
//!           pause: the inline pass at each fork reaps whatever is due, so no backlog can build.
//!
//! Beside the volatile columns it prints the r11-churn instrument (`turso_core::branch::
//! churn_counters`): branch-file fsyncs process-wide and on the reaper thread, the expiry passes
//! that found something due (passes, branches reaped, pages freed, fsyncs), and compactions (count,
//! total and max duration, fsyncs, last snapshot bytes); plus the log, arena and snapshot file sizes.
//!
//! Checks (`NOT A RESULT`, exit 1): the sampled reads against the harness's model; in serial mode
//! every reap's outcome against the chain structure; at quiescence, arena pages in use = w x engine
//! branches, and in serial mode engine branches = table entries + deferred links; teardown empty.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turso_core::branch::{churn_counters, Branch, BranchDurability, BranchId, ChurnCounters};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
/// Rows between two of one branch's writes: more than a leaf holds (~37), so w <= 64 rows land on
/// w distinct leaves (as `branch_arms --arm pages`).
const PAGE_STRIDE: i64 = 312;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Serial,
    Batch,
    Lease,
}

struct Args {
    mode: Mode,
    durability: BranchDurability,
    lambda: f64,
    lease: Duration,
    duration: Duration,
    w: usize,
    chain: usize,
    pause_at: Option<Duration>,
    pause: Duration,
    sample: Duration,
    expire_every: Duration,
    batch_max: usize,
    reap_every: Duration,
    /// Amendments 5/5c: one trunk UPDATE per arrival, rotating over 312 known-distinct leaves.
    trunk_writes: bool,
    synchronous: &'static str,
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
        mode: Mode::Serial,
        durability: BranchDurability::Durable { sync: true },
        lambda: 1000.0,
        lease: Duration::from_millis(1000),
        duration: Duration::ZERO,
        w: 1,
        chain: 1,
        pause_at: None,
        pause: Duration::ZERO,
        sample: Duration::ZERO,
        expire_every: Duration::from_millis(100),
        batch_max: 4096,
        reap_every: Duration::from_millis(100),
        trunk_writes: false,
        synchronous: "OFF",
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
            "--mode" => {
                args.mode = match val().as_str() {
                    "serial" => Mode::Serial,
                    "batch" => Mode::Batch,
                    "lease" => Mode::Lease,
                    other => die(&format!("unknown --mode {other}")),
                }
            }
            "--durability" => {
                args.durability = match val().as_str() {
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    other => die(&format!("unknown --durability {other}")),
                }
            }
            "--lambda" => args.lambda = val().parse().unwrap_or_else(|_| die("bad --lambda")),
            "--lease-ms" => args.lease = ms(val(), "--lease-ms"),
            "--duration-ms" => args.duration = ms(val(), "--duration-ms"),
            "--w" => args.w = val().parse().unwrap_or_else(|_| die("bad --w")),
            "--chain" => args.chain = val().parse().unwrap_or_else(|_| die("bad --chain")),
            "--pause-at-ms" => args.pause_at = Some(ms(val(), "--pause-at-ms")),
            "--pause-ms" => args.pause = ms(val(), "--pause-ms"),
            "--sample-ms" => args.sample = ms(val(), "--sample-ms"),
            "--expire-every-ms" => args.expire_every = ms(val(), "--expire-every-ms"),
            "--batch-max" => args.batch_max = val().parse().unwrap_or_else(|_| die("bad --batch-max")),
            "--reap-every-ms" => args.reap_every = ms(val(), "--reap-every-ms"),
            "--trunk-writes" => {
                args.trunk_writes = match val().as_str() {
                    "rotate" => true,
                    "none" => false,
                    other => die(&format!("unknown --trunk-writes {other}")),
                }
            }
            "--synchronous" => {
                args.synchronous = match val().as_str() {
                    "off" => "OFF",
                    "normal" => "NORMAL",
                    "full" => "FULL",
                    other => die(&format!("unknown --synchronous {other}")),
                }
            }
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
    if args.trunk_writes && args.chain != 1 {
        die("--trunk-writes needs --chain 1 (the read model tracks the trunk as of each fork)");
    }
    if args.mode == Mode::Lease && args.pause_at.is_some() {
        die("--pause-at-ms is for --mode serial (in lease mode every fork reaps what is due)");
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

/// Amendment 5: the trunk's `g`-th write, same length as the original, so it rewrites in place.
fn trunk_gen_value(g: u64) -> String {
    format!("t{:0>width$}", g, width = VALUE_LEN - 1)
}

/// Trunk pages the trunk writes rotate over (amendment 5c). Rows `1 + 64 j` for j < 312 lie on
/// 312 DISTINCT leaves whatever the B-tree's split points: a table leaf holds a contiguous rowid
/// range, and at most (4096 - 8) / 106 = 38 rows of this table fit on one (each cell is at least a
/// 100-byte text, a record header, a rowid and a 2-byte pointer), so rows 64 apart never share one.
/// Knowing the page of every trunk write is what lets the harness predict each reap exactly.
const TRUNK_PAGES: u64 = 312;
const TRUNK_ROW_STRIDE: u64 = 64;

/// The row of the trunk's `g`-th write: page `g mod 312`, in rotation.
fn trunk_row(g: u64) -> i64 {
    ((g % TRUNK_PAGES) * TRUNK_ROW_STRIDE) as i64 + 1
}

/// The trunk's write history, kept by the harness (never read from the engine): what a branch
/// forked after `at` trunk writes must read for each row the trunk rewrote.
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    history: std::collections::HashMap<i64, Vec<u64>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push(g);
        g
    }
    fn value_at(&self, row: i64, at: u64) -> String {
        match self.history.get(&row) {
            None => trunk_value(row),
            Some(h) => {
                let k = h.partition_point(|&g| g < at);
                if k == 0 {
                    trunk_value(row)
                } else {
                    trunk_gen_value(h[k - 1])
                }
            }
        }
    }
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

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
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

/// One leased branch in the harness's table: its handle in serial mode; in lease mode only its id
/// (the handle is detached once written, so the store's expiry is what reaps it).
struct Entry {
    /// Arrival index.
    n: u64,
    /// Trunk writes committed when this branch was forked (amendment 5's read model).
    trunk_at: u64,
    deadline: Instant,
    branch: Option<Branch>,
    id: BranchId,
}

#[derive(Default)]
struct ReapWindow {
    latency_ns: Vec<u64>,
    pages: u64,
    /// Trunk pre-images freed by reaps (freed pages beyond the branches' own w), amendment 5c.
    trunk_preimages: u64,
    deferred: u64,
    cascades: u64,
    max_pages: u64,
}

struct Shared {
    queue: Mutex<VecDeque<Entry>>,
    in_hand: AtomicU64,
    arrivals: AtomicU64,
    reaped: AtomicU64,
    deferred_now: AtomicU64,
    window: Mutex<ReapWindow>,
    lag_max_ns: AtomicU64,
    cycle_ns: Mutex<Vec<u64>>,
    stop: AtomicBool,
    forking_done: AtomicBool,
    drain: Mutex<Option<(Duration, Duration, u64)>>,
    /// The reaper thread's own fsync count (thread-local in the store), published after each reap.
    reaper_fsyncs: AtomicU64,
    /// Lease mode: background `expire_branches` calls and what they reported.
    bg_passes: AtomicU64,
    bg_reaped: AtomicU64,
    /// The chain link the forker keeps attached (lease mode) and must not be attached by a read.
    newest: AtomicU64,
    /// Amendment 5: the largest backlog any reaper pop found in the current window. The backlog
    /// only falls at a pop, so its peak between two pops is what the later pop finds.
    pop_max: AtomicU64,
}

struct Files {
    log: PathBuf,
    arena: PathBuf,
    snap: PathBuf,
}

impl Files {
    fn new(db: &Path) -> Self {
        let with = |suffix: &str| PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
        Self {
            log: with("-branch-log"),
            arena: with("-branch-arena"),
            snap: with("-branch-snap"),
        }
    }
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_churn.db");
    let files = Files::new(&path);
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let mut opts = DatabaseOpts::new().with_branch_durability(args.durability);
    if args.mode == Mode::Lease {
        opts = opts.with_branch_lease(Some(args.lease));
    }
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    // Without --trunk-writes the trunk is written only at setup. With them, amendment 5 runs it
    // under NORMAL: OFF re-checkpoints the whole WAL at every commit on this fork (round 10's B1).
    trunk
        .execute(format!("PRAGMA synchronous = {}", args.synchronous))
        .unwrap();
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
    println!("# branch_churn — Turso fork, DURABLE branch store (ec168128b + r11-churn instrument), lease churn");
    println!(
        "# mode={:?} durability={:?} lambda={} lease_ms={} duration_ms={} w={} chain={} \
         pause_at_ms={} pause_ms={} sample_ms={} expire_every_ms={} batch_max={} reap_every_ms={} \
         trunk_writes={} synchronous={} verify_every={} seed={:#x} trunk_rows={TRUNK_ROWS} \
         page_size={page_size} little_L={:.0}",
        args.mode,
        args.durability,
        args.lambda,
        args.lease.as_millis(),
        args.duration.as_millis(),
        args.w,
        args.chain,
        args.pause_at.map_or(-1, |d| d.as_millis() as i64),
        args.pause.as_millis(),
        args.sample.as_millis(),
        args.expire_every.as_millis(),
        args.batch_max,
        args.reap_every.as_millis(),
        args.trunk_writes,
        args.synchronous,
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
         arena_in_use rss_bytes lag_max_ms cycle_p50_us cycle_p99_us reap_calls_in_window \
         call_p50_us call_p99_us call_max_us pages_freed_in_window max_pages_one_reap \
         cascades_in_window | fsyncs_total reaper_thread_fsyncs expire_passes_with_due \
         expire_reaped expire_freed_pages expire_fsyncs bg_passes bg_reaped store_leases \
         store_leases_due compactions compact_ms_total compact_ms_max compact_fsyncs \
         snapshot_bytes_last log_bytes arena_file_bytes snap_file_bytes | pop_max_backlog \
         gc_range_in_window gc_examined_in_window"
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
        reaper_fsyncs: AtomicU64::new(0),
        bg_passes: AtomicU64::new(0),
        bg_reaped: AtomicU64::new(0),
        newest: AtomicU64::new(u64::MAX),
        pop_max: AtomicU64::new(0),
    });
    let t0 = Instant::now();
    // What one pop takes when the reaper keeps up: lambda x (2 ms) for the serial reaper, and
    // twice lambda x (window + 2 ms) for the batch reaper, which pops once per window.
    let steady_batch = if args.mode == Mode::Batch {
        ((args.lambda * (args.reap_every.as_secs_f64() + 0.002) * 2.0).ceil() as u64).max(64)
    } else {
        ((args.lambda * 0.002).ceil() as u64).max(64)
    };
    println!("# steady_batch_bound={steady_batch}");

    let reaper = {
        let shared = shared.clone();
        let db = db.clone();
        let (w, chain, mode, every) = (args.w as u64, args.chain as u64, args.mode, args.expire_every);
        let pause = args.pause_at.map(|at| (at, at + args.pause));
        let (batch_max, round) = if mode == Mode::Batch {
            (args.batch_max, args.reap_every)
        } else {
            (1, Duration::ZERO)
        };
        let trunk_written = args.trunk_writes;
        std::thread::spawn(move || match mode {
            Mode::Serial | Mode::Batch => reaper_loop(
                &shared,
                &db,
                t0,
                w,
                chain,
                pause,
                steady_batch,
                batch_max,
                round,
                trunk_written,
            ),
            Mode::Lease => expirer_loop(&shared, &db, every),
        })
    };
    let sampler = {
        let shared = shared.clone();
        let db = db.clone();
        let (sample, files) = (args.sample, Files::new(&path));
        std::thread::spawn(move || sampler_loop(&shared, &db, &files, t0, sample))
    };

    forker_loop(&shared, &db, &trunk, &args, t0);
    shared.forking_done.store(true, Ordering::Release);
    reaper.join().unwrap();
    shared.stop.store(true, Ordering::Release);
    let windows = sampler.join().unwrap();
    summarize(&args, &windows, &shared);

    let stats = db.branch_stats().unwrap();
    let engine = stats.live_branches as u64;
    // With trunk writes the arena also holds the trunk's retained pre-images; teardown still
    // requires it empty once every branch is released.
    if !args.trunk_writes && stats.arena_slots_in_use as u64 != args.w as u64 * engine {
        not_a_result(&format!(
            "arena holds {} pages for {engine} branches of w={} pages each",
            stats.arena_slots_in_use, args.w
        ));
    }
    match args.mode {
        Mode::Serial | Mode::Batch => {
            let queued = shared.queue.lock().unwrap().len() as u64;
            let deferred = shared.deferred_now.load(Ordering::Acquire);
            if engine != queued + deferred {
                not_a_result(&format!(
                    "engine holds {engine} branches, the harness {queued} leased + {deferred} deferred"
                ));
            }
            println!(
                "# quiescent: engine_branches={engine} = leased_or_due {queued} + deferred {deferred}; \
                 arena_in_use = w x engine"
            );
        }
        Mode::Lease => {
            let (leases, due) = db.branch_lease_counts();
            println!(
                "# quiescent: engine_branches={engine} store_leases={leases} store_leases_due={due}; \
                 arena_in_use = w x engine"
            );
        }
    }
    let c = churn_counters();
    println!(
        "# counters at end: fsyncs={} expire_passes_with_due={} expire_reaped={} expire_freed_pages={} \
         expire_fsyncs={} expire_piggybacked={} compactions={} compact_ms_total={:.1} compact_ms_max={:.1} compact_fsyncs={} \
         snapshot_bytes_last={}",
        c.fsyncs,
        c.expire_passes_with_due,
        c.expire_reaped,
        c.expire_freed_pages,
        c.expire_fsyncs,
        c.expire_piggybacked,
        c.compactions,
        c.compact_ns_total as f64 / 1e6,
        c.compact_ns_max as f64 / 1e6,
        c.compact_fsyncs,
        c.compact_bytes_last
    );

    // Teardown (untimed). Serial: drop the remaining handles in arrival order (a chain's links
    // defer until its tip goes). Lease: run the lease clock past every deadline and let one pass
    // reap everything.
    let rest: Vec<Entry> = shared.queue.lock().unwrap().drain(..).collect();
    if args.mode == Mode::Batch {
        // One flush for the whole remainder (a 10^6 teardown one release at a time is 10^6 fsyncs).
        let handles: Vec<Branch> = rest.into_iter().filter_map(|e| e.branch).collect();
        db.reap_branches(handles).unwrap();
    } else {
        drop(rest);
    }
    if args.mode == Mode::Lease {
        db.branch_lease_clock_advance(args.lease * 4);
        db.expire_branches().unwrap();
    }
    let end = db.branch_stats().unwrap();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown: every branch freed, arena empty ({} free slots)",
        end.arena_slots_free
    );
}

fn forker_loop(
    shared: &Shared,
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    args: &Args,
    t0: Instant,
) {
    let mut rng = Rng(args.seed);
    let chain = args.chain as u64;
    let interval = 1.0 / args.lambda;
    // Lease mode: the newest link, kept attached so the chain's next arrival can fork from it.
    let mut parent: Option<(u64, Branch)> = None;
    let mut model = TrunkModel::default();
    let mut n: u64 = 0;
    loop {
        let due = t0 + Duration::from_secs_f64(n as f64 * interval);
        if due.duration_since(t0) >= args.duration {
            break;
        }
        let now = Instant::now();
        if now < due {
            std::thread::sleep(due - now);
        } else {
            let lag = (now - due).as_nanos() as u64;
            shared.lag_max_ns.fetch_max(lag, Ordering::Relaxed);
        }
        let start = Instant::now();
        let trunk_at = model.writes;
        let branch = if n % chain == 0 {
            trunk.fork_branch().unwrap()
        } else {
            match args.mode {
                Mode::Serial | Mode::Batch => {
                    let q = shared.queue.lock().unwrap();
                    let p = q.back().unwrap_or_else(|| not_a_result("chain parent missing"));
                    if p.n != n - 1 {
                        not_a_result(&format!("chain parent of {n} is arrival {}", p.n));
                    }
                    p.branch.as_ref().unwrap().fork().unwrap()
                }
                Mode::Lease => {
                    let (pn, p) = parent.as_ref().unwrap_or_else(|| not_a_result("chain parent missing"));
                    if *pn != n - 1 {
                        not_a_result(&format!("chain parent of {n} is arrival {pn}"));
                    }
                    p.fork().unwrap()
                }
            }
        };
        let deadline = start + args.lease;
        let conn = branch.connect().unwrap();
        let rows = rows_of(n, args.w);
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
        drop(conn);
        if args.trunk_writes {
            // One trunk write per arrival, after the fork: the branch just forked can see the page
            // it rewrites, so the trunk keeps a pre-image for it (a TrunkRetain record).
            let row = trunk_row(model.writes);
            let g = model.record(row);
            trunk
                .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g)))
                .unwrap();
        }
        let id = branch.id();
        let entry = match args.mode {
            Mode::Serial | Mode::Batch => Entry {
                n,
                trunk_at,
                deadline,
                branch: Some(branch),
                id,
            },
            Mode::Lease => {
                // Detach the previous link now that nothing forks from it any more; keep this one
                // attached only if the chain continues from it.
                if let Some((_, p)) = parent.take() {
                    p.into_id();
                }
                if (n + 1) % chain != 0 {
                    shared.newest.store(id.0, Ordering::Release);
                    parent = Some((n, branch));
                } else {
                    branch.into_id();
                }
                Entry {
                    n,
                    trunk_at,
                    deadline,
                    branch: None,
                    id,
                }
            }
        };
        let cycle = start.elapsed().as_nanos() as u64;
        shared.cycle_ns.lock().unwrap().push(cycle);
        shared.queue.lock().unwrap().push_back(entry);
        shared.arrivals.fetch_add(1, Ordering::Release);
        n += 1;
        if n % args.verify_every as u64 == 0 {
            verify_one(shared, db, &mut rng, args, &model);
        }
    }
    if let Some((_, p)) = parent.take() {
        p.into_id();
    }
}

/// Read a random leased branch's own row, its chain root's first row, and a random row, against
/// the model: a branch sees branch_value for every row its chain prefix (root .. itself) wrote, and
/// trunk_value for every other row (the trunk is never written after setup).
fn verify_one(
    shared: &Shared,
    db: &Arc<Database>,
    rng: &mut Rng,
    args: &Args,
    model: &TrunkModel,
) {
    let q = shared.queue.lock().unwrap();
    if q.is_empty() {
        return;
    }
    let e = &q[rng.below(q.len())];
    let chain = args.chain as u64;
    let root = e.n - e.n % chain;
    let written = |row: i64| (root..=e.n).any(|m| rows_of(m, args.w).contains(&row));
    let own = rows_of(e.n, args.w)[0];
    let root_row = rows_of(root, args.w)[0];
    let other = rng.below(TRUNK_ROWS as usize) as i64 + 1;
    // With trunk writes: the row of the trunk's first write after this fork, which the branch must
    // read as its pre-image (a retained version) — or the trunk's value if no write came after.
    let rewritten = if model.writes > e.trunk_at {
        trunk_row(e.trunk_at)
    } else {
        other
    };
    let check = |conn: &Arc<Connection>| {
        for row in [own, root_row, other, rewritten] {
            let want = if written(row) {
                branch_value(row)
            } else {
                // The trunk as of this branch's fork (chain 1 only when the trunk is written).
                model.value_at(row, e.trunk_at)
            };
            if read_v(conn, row) != want {
                not_a_result(&format!("arrival {} read the wrong version of row {row}", e.n));
            }
        }
    };
    match &e.branch {
        Some(b) => check(&b.connect().unwrap()),
        None => {
            // Lease mode: only a branch well inside its lease, and not the attached newest link.
            if e.deadline <= Instant::now() + Duration::from_millis(200)
                || e.id.0 == shared.newest.load(Ordering::Acquire)
            {
                return;
            }
            let Ok(b) = db.branch(e.id) else { return };
            check(&b.connect().unwrap());
            b.into_id();
        }
    }
}

/// Serial (`batch_max` = 1): one `Branch::reap` per branch, popping again at once (or after
/// 200 us when nothing is due). Batch: the reaper wakes every `round` (the group-commit window, a
/// ZFS txg), and what is due goes to `Database::reap_branches` in chunks of at most `batch_max`,
/// one flush per chunk.
#[allow(clippy::too_many_arguments)]
fn reaper_loop(
    shared: &Shared,
    db: &Arc<Database>,
    t0: Instant,
    w: u64,
    chain: u64,
    pause: Option<(Duration, Duration)>,
    steady_batch: u64,
    batch_max: usize,
    round: Duration,
    trunk_written: bool,
) {
    let mut resumed: Option<(Duration, u64)> = None;
    loop {
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
            shared.pop_max.fetch_max(due as u64, Ordering::AcqRel);
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
            std::thread::sleep(round.max(Duration::from_micros(200)));
            continue;
        }
        let mut batch = batch.into_iter().peekable();
        while batch.peek().is_some() {
            let chunk: Vec<Entry> = batch.by_ref().take(batch_max).collect();
            let t = Instant::now();
            let (ns_list, outcomes): (Vec<u64>, Vec<(u64, turso_core::branch::Reaped)>) =
                if batch_max == 1 {
                    let e = chunk.into_iter().next().unwrap();
                    let r = e.branch.unwrap().reap().unwrap();
                    (vec![t.elapsed().as_nanos() as u64], vec![(e.n, r)])
                } else {
                    let ns_idx: Vec<u64> = chunk.iter().map(|e| e.n).collect();
                    let handles: Vec<Branch> =
                        chunk.into_iter().map(|e| e.branch.unwrap()).collect();
                    let rs = db.reap_branches(handles).unwrap();
                    let ns = t.elapsed().as_nanos() as u64;
                    (vec![ns], ns_idx.into_iter().zip(rs).collect())
                };
            shared
                .reaper_fsyncs
                .store(churn_counters().thread_fsyncs, Ordering::Release);
            {
                let mut win = shared.window.lock().unwrap();
                win.latency_ns.extend(ns_list);
            }
            for (n, r) in outcomes {
            let pos = n % chain;
            let tip = pos == chain - 1;
            let expect_pages = if tip { chain * w } else { 0 };
            // With the trunk written (chain 1 only; amendment 5c), the harness's own model gives
            // the reap's outcome EXACTLY. Arrival k forks child c_k and then rewrites trunk page
            // k mod 312, which keeps the pre-image V_k = [epoch of that page's previous write, k + 1).
            // Only c_{k-311} .. c_k were forked inside it, and every one but c_k is older. So when
            // FIFO lease order reaps c_k, V_k has no other live reader and is freed, while every
            // other version holding k also holds the live c_{k+1}. Each reap therefore frees its w
            // pages plus exactly ONE trunk pre-image, and is never deferred.
            let wrong = if trunk_written {
                r.deferred || r.freed_pages as u64 != w + 1
            } else {
                r.deferred == tip || r.freed_pages as u64 != expect_pages
            };
            if wrong {
                not_a_result(&format!(
                    "arrival {n} (chain position {pos}) reaped {r:?}, expected {} and {expect_pages} pages",
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
                win.pages += r.freed_pages as u64;
                if trunk_written {
                    win.trunk_preimages += r.freed_pages as u64 - w;
                }
                win.deferred += r.deferred as u64;
                win.cascades += (tip && chain > 1) as u64;
                win.max_pages = win.max_pages.max(r.freed_pages as u64);
            }
            shared.reaped.fetch_add(1, Ordering::Release);
            shared.in_hand.fetch_sub(1, Ordering::AcqRel);
            }
        }
        if !round.is_zero() {
            std::thread::sleep(round);
        }
    }
}

/// Lease mode: the store reaps. This thread calls the store's expiry pass on a timer, and trims the
/// harness's table of entries whose deadline has passed (bookkeeping only; it holds no handle).
fn expirer_loop(shared: &Shared, db: &Arc<Database>, every: Duration) {
    loop {
        if shared.forking_done.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(every);
        let t = Instant::now();
        let x = db.expire_branches().unwrap();
        let ns = t.elapsed().as_nanos() as u64;
        shared.bg_passes.fetch_add(1, Ordering::Relaxed);
        shared
            .bg_reaped
            .fetch_add(x.reaped.len() as u64, Ordering::Relaxed);
        if !x.reaped.is_empty() {
            let mut win = shared.window.lock().unwrap();
            win.latency_ns.push(ns);
            win.pages += x.freed_pages as u64;
            win.max_pages = win.max_pages.max(x.freed_pages as u64);
        }
        let now = Instant::now();
        let mut q = shared.queue.lock().unwrap();
        let due = q.partition_point(|e| e.deadline <= now);
        q.drain(..due);
    }
}

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
    c: ChurnCounters,
    reaper_fsyncs: u64,
    compactions: u64,
    pop_max: u64,
    gc_range: u64,
    gc_examined: u64,
    trunk_preimages: u64,
}

fn sampler_loop(
    shared: &Shared,
    db: &Arc<Database>,
    files: &Files,
    t0: Instant,
    sample: Duration,
) -> Vec<Win> {
    let mut out = Vec::new();
    let mut next = t0 + sample;
    let mut work0 = db.branch_stats().unwrap().work;
    let mut compactions0 = churn_counters().compactions;
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
        let (store_leases, store_due) = db.branch_lease_counts();
        let c = churn_counters();
        let in_hand = shared.in_hand.load(Ordering::Acquire);
        let arrivals = shared.arrivals.load(Ordering::Acquire);
        // Serial: the harness's own table. Lease: the store's leases run out and not yet reaped,
        // and reaped = what the store's passes reaped.
        let (backlog, reaped) = if store_leases > 0 || c.expire_reaped > 0 {
            (store_due as u64, c.expire_reaped)
        } else {
            (due_in_queue + in_hand, shared.reaped.load(Ordering::Acquire))
        };
        let expired = reaped + backlog;
        let stats = db.branch_stats().unwrap();
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
        let reaper_fsyncs = shared.reaper_fsyncs.load(Ordering::Acquire);
        // Amendment 5: the backlog peak at call granularity, and the reap path's work counters.
        let pop_max = shared.pop_max.swap(0, Ordering::AcqRel);
        let work = stats.work;
        let gc_range = work.gc_range_entries - work0.gc_range_entries;
        let gc_examined = work.gc_examined - work0.gc_examined;
        work0 = work;
        if c.compactions > compactions0 {
            println!(
                "# compaction t_ms={} count={} compact_ms_max_so_far={:.1} snapshot_bytes={} \
                 pop_max_in_window={pop_max} backlog_at_sample={backlog}",
                t.as_millis(),
                c.compactions - compactions0,
                c.compact_ns_max as f64 / 1e6,
                c.compact_bytes_last
            );
            compactions0 = c.compactions;
        }
        println!(
            "# s {} {arrivals} {expired} {reaped} {backlog} {leased} {} {deferred} {} {} {lag:.3} \
             {:.2} {:.2} {} {:.2} {:.2} {:.2} {} {} {} | {} {reaper_fsyncs} {} {} {} {} {} {} \
             {store_leases} {store_due} {} {:.1} {:.1} {} {} {} {} {} | {pop_max} {gc_range} \
             {gc_examined}",
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
            c.fsyncs,
            c.expire_passes_with_due,
            c.expire_reaped,
            c.expire_freed_pages,
            c.expire_fsyncs,
            shared.bg_passes.load(Ordering::Relaxed),
            shared.bg_reaped.load(Ordering::Relaxed),
            c.compactions,
            c.compact_ns_total as f64 / 1e6,
            c.compact_ns_max as f64 / 1e6,
            c.compact_fsyncs,
            c.compact_bytes_last,
            size_of(&files.log),
            size_of(&files.arena),
            size_of(&files.snap),
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
            c,
            reaper_fsyncs,
            compactions: c.compactions,
            pop_max,
            gc_range,
            gc_examined,
            trunk_preimages: win.trunk_preimages,
        });
        if done {
            return out;
        }
    }
}

fn summarize(args: &Args, wins: &[Win], shared: &Shared) {
    let drain = *shared.drain.lock().unwrap();
    let pause = args.pause_at.map(|at| (at, at + args.pause));
    let drain_end = drain.map(|d| d.1);
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
        let reaped = (last.reaped - first.reaped).max(1) as f64;
        let fsyncs = (last.c.fsyncs - first.c.fsyncs) as f64;
        let arrived = (last.arrivals - first.arrivals).max(1) as f64;
        println!(
            "# summary steady_windows={} span_s={secs:.1} arrivals_per_s={arr_rate:.1} \
             expiries_per_s={exp_rate:.1} reaps_per_s={reap_rate:.1} backlog_max={max_backlog} \
             backlog_mean={mean_backlog:.1} pages_per_reap_call={:.4} reap_p99_max_us={p99_max:.2} \
             reap_max_us={max_us:.2}",
            steady.len(),
            if reaps == 0 { 0.0 } else { pages as f64 / reaps as f64 },
        );
        println!(
            "# summary fsyncs steady: total={fsyncs:.0} per_arrival={:.4} reaper_thread_per_reap={:.4} \
             expire_passes_with_due={} expire_reaped={} expire_fsyncs_per_reaped={:.4} \
             reaped_per_pass={:.3} piggybacked_passes={} compactions={}",
            fsyncs / arrived,
            (last.reaper_fsyncs - first.reaper_fsyncs) as f64 / reaped,
            last.c.expire_passes_with_due - first.c.expire_passes_with_due,
            last.c.expire_reaped - first.c.expire_reaped,
            (last.c.expire_fsyncs - first.c.expire_fsyncs) as f64
                / (last.c.expire_reaped - first.c.expire_reaped).max(1) as f64,
            (last.c.expire_reaped - first.c.expire_reaped) as f64
                / (last.c.expire_passes_with_due - first.c.expire_passes_with_due).max(1) as f64,
            last.c.expire_piggybacked - first.c.expire_piggybacked,
            last.compactions - first.compactions,
        );
        // Amendment 5: the call-granularity backlog peak over the steady windows (and where), and
        // the reap path's work per reaped branch.
        let peak = steady.iter().max_by_key(|w| w.pop_max).unwrap();
        let reaped_n = (last.reaped - first.reaped).max(1) as f64;
        let gc_range: u64 = steady.iter().skip(1).map(|w| w.gc_range).sum();
        let gc_examined: u64 = steady.iter().skip(1).map(|w| w.gc_examined).sum();
        let preimages: u64 = steady.iter().skip(1).map(|w| w.trunk_preimages).sum();
        println!(
            "# summary pop steady: pop_max={} at_t_ms={} gc_range_per_reaped={:.3} \
             gc_examined_per_reaped={:.3} trunk_preimages_freed_per_reaped={:.4}",
            peak.pop_max,
            peak.t.as_millis(),
            gc_range as f64 / reaped_n,
            gc_examined as f64 / reaped_n,
            preimages as f64 / reaped_n,
        );
    }
    // Over the whole run, including the ramp, the pause and the drain.
    let all = wins.iter().max_by_key(|w| w.pop_max);
    if let Some(w) = all {
        println!(
            "# summary pop whole run: pop_max={} at_t_ms={}",
            w.pop_max,
            w.t.as_millis()
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
