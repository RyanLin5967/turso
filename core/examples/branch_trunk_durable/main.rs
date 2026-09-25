//! What a trunk commit costs when it must keep a pre-image for a live branch, per branch durability
//! mode and trunk sync mode: the measurement of F3's case (turso_sota lane, PREREG D1 next to this
//! file). Written against the durable store at `ec168128b`.
//!
//!   cargo run -p turso_core --release --example branch_trunk_durable -- \
//!       --durability volatile|durable|durable-nosync --synchronous off|normal|full \
//!       --checkpoints 100,1000,10000,30000 --samples 200
//!
//! Workload: the `hot` arm of `branch_arms` — N live branches, each forked from the trunk and given one
//! page of its own, with one trunk UPDATE of row 1 after every fork, so each trunk write keeps one
//! pre-image. During growth every fork and every trunk write is timed (`grow_fork`, `grow_trunk`), and
//! any op during which the branch snapshot file changed is listed as a compaction.
//!
//! At each checkpoint N, K samples, each: fork a sample branch (untimed), then time
//!
//!   trunk_retain  UPDATE row 1 on the trunk: a branch forked since the row's last write, so the
//!                 commit keeps a pre-image (and, durable, its barrier flushes a `TrunkRetain` record)
//!   trunk_plain   UPDATE row 1 again at once: no fork since, so the fast path keeps nothing
//!
//! Beside each op it prints the bytes the branch log and the arena file grew by per sample, read
//! from file sizes outside the timed window. The sample branches are reaped afterwards and the
//! engine must be back at exactly N branches. A sampled read of row 1 on a random live branch is
//! checked against the harness's own record of the trunk's writes. Any mismatch prints
//! `NOT A RESULT` and exits 1.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use turso_core::branch::{Branch, BranchDurability};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
const HOT_ROW: i64 = 1;

struct Args {
    checkpoints: Vec<usize>,
    samples: usize,
    seed: u64,
    durability: BranchDurability,
    synchronous: &'static str,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_trunk_durable: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut args = Args {
        checkpoints: vec![100, 1000],
        samples: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        durability: BranchDurability::Volatile,
        synchronous: "FULL",
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--checkpoints" => {
                args.checkpoints = val()
                    .split(',')
                    .map(|s| s.parse().unwrap_or_else(|_| die("bad --checkpoints")))
                    .collect()
            }
            "--samples" => args.samples = val().parse().unwrap_or_else(|_| die("bad --samples")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--durability" => {
                args.durability = match val().as_str() {
                    "volatile" => BranchDurability::Volatile,
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    other => die(&format!("unknown --durability {other}")),
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
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.samples == 0 {
        die("--samples must be positive");
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

/// The trunk's `generation`-th rewrite of the hot row: same length, so it rewrites in place.
fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
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

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

fn mtime_of(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// The branch files next to the database (`journal.rs` at `ec168128b`).
struct Files {
    log: PathBuf,
    arena: PathBuf,
    snap: PathBuf,
    wal: PathBuf,
}

impl Files {
    fn new(db: &Path) -> Self {
        let with = |suffix: &str| PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
        Self {
            log: with("-branch-log"),
            arena: with("-branch-arena"),
            snap: with("-branch-snap"),
            wal: with("-wal"),
        }
    }
    fn sizes(&self) -> (u64, u64) {
        (size_of(&self.log), size_of(&self.arena))
    }
}

/// One op's samples, the bytes the log and arena grew by across them, and the samples during which
/// the snapshot file changed (a compaction ran inside the op).
#[derive(Default)]
struct Op {
    samples: Vec<Duration>,
    log_bytes: u64,
    arena_bytes: u64,
    compactions: Vec<(usize, f64)>,
}

struct Bench {
    trunk: Arc<Connection>,
    files: Files,
    /// Trunk writes committed so far; the value of the hot row after write g is trunk_gen_value(g).
    writes: u64,
}

impl Bench {
    /// Time `f` as one sample of `op`; `at` labels a compaction with the branch count.
    fn timed<T>(&self, op: &mut Op, at: usize, f: impl FnOnce() -> T) -> T {
        let (log0, arena0) = self.files.sizes();
        let snap0 = mtime_of(&self.files.snap);
        let t = Instant::now();
        let out = f();
        let d = t.elapsed();
        let (log1, arena1) = self.files.sizes();
        op.samples.push(d);
        op.log_bytes += log1.saturating_sub(log0);
        op.arena_bytes += arena1.saturating_sub(arena0);
        if mtime_of(&self.files.snap) != snap0 {
            op.compactions.push((at, d.as_secs_f64() * 1e6));
        }
        out
    }

    fn trunk_write(&mut self, op: &mut Op, at: usize) {
        let g = self.writes;
        self.writes += 1;
        let sql = format!("UPDATE t SET v = '{}' WHERE id = {HOT_ROW}", trunk_gen_value(g));
        let trunk = self.trunk.clone();
        self.timed(op, at, || trunk.execute(sql).unwrap());
    }

    /// The hot row as a branch forked when `writes_at_fork` trunk writes had committed sees it.
    fn hot_at(writes_at_fork: u64) -> String {
        if writes_at_fork == 0 {
            trunk_value(HOT_ROW)
        } else {
            trunk_gen_value(writes_at_fork - 1)
        }
    }
}

fn print_op(n: usize, name: &str, op: &Op) {
    if op.samples.is_empty() {
        return;
    }
    let mut us: Vec<f64> = op.samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let k = us.len() as f64;
    println!(
        "{n}\t{name}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}",
        us.len(),
        percentile(&us, 50.0),
        percentile(&us, 90.0),
        percentile(&us, 99.0),
        us[us.len() - 1],
        op.log_bytes as f64 / k,
        op.arena_bytes as f64 / k,
        op.compactions.len()
    );
}

struct Live {
    branch: Branch,
    row: i64,
    writes_at_fork: u64,
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_trunk_durable.db");
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
    let int = |sql: &str| {
        trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    println!("# branch_trunk_durable — Turso fork at ec168128b (durable store), turso_sota PREREG D1");
    println!(
        "# durability={:?} trunk_synchronous={} (PRAGMA reads {}) checkpoints={:?} samples={} seed={:#x} \
         trunk_rows={TRUNK_ROWS} page_size={} trunk_pages={}",
        args.durability,
        args.synchronous,
        int("PRAGMA synchronous"),
        args.checkpoints,
        args.samples,
        args.seed,
        int("PRAGMA page_size"),
        int("PRAGMA page_count")
    );
    println!(
        "# clock tick {:.0} ns (Instant); times in microseconds; log_B/op and arena_B/op are file growth \
         per sample; compactions = samples during which the branch snapshot file changed",
        clock_tick_ns()
    );
    println!(
        "# build: {} ; rss_base_bytes={}",
        if cfg!(debug_assertions) { "DEBUG (not a timing result)" } else { "release" },
        rss_bytes()
    );
    println!("x\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us\tlog_B_per_op\tarena_B_per_op\tcompactions");

    let mut b = Bench {
        trunk: trunk.clone(),
        files: Files::new(&path),
        writes: 0,
    };
    let mut rng = Rng(args.seed);
    let mut live: Vec<Live> = Vec::new();
    let mut grown = 0usize;
    let mut compaction_log: Vec<(&'static str, usize, f64)> = Vec::new();
    for &n in &args.checkpoints {
        let (mut gfork, mut gtrunk) = (Op::default(), Op::default());
        while live.len() < n {
            let at = live.len();
            let writes_at_fork = b.writes;
            let trunk_c = b.trunk.clone();
            let branch = b.timed(&mut gfork, at, || trunk_c.fork_branch().unwrap());
            let row = row_for(grown);
            grown += 1;
            let conn = branch.connect().unwrap();
            conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(row)))
                .unwrap();
            drop(conn);
            live.push(Live {
                branch,
                row,
                writes_at_fork,
            });
            b.trunk_write(&mut gtrunk, at);
        }
        let s = db.branch_stats().unwrap();
        // Each branch owns one page, and each trunk write after a fork kept one pre-image.
        if s.live_branches != n || s.arena_slots_in_use != 2 * n {
            not_a_result(&format!("expected {n} branches and {} arena pages: {s:?}", 2 * n));
        }
        let k = args.samples;
        let (mut retain, mut plain) = (Op::default(), Op::default());
        let mut sampled: Vec<Branch> = Vec::with_capacity(k);
        for _ in 0..k {
            sampled.push(b.trunk.fork_branch().unwrap());
            b.trunk_write(&mut retain, n);
            b.trunk_write(&mut plain, n);
        }
        let probe = &live[rng.below(live.len())];
        let conn = probe.branch.connect().unwrap();
        if read_v(&conn, HOT_ROW) != Bench::hot_at(probe.writes_at_fork)
            || read_v(&conn, probe.row) != branch_value(probe.row)
        {
            not_a_result("a live branch read the wrong version of the hot row or of its own row");
        }
        drop(conn);
        for s in sampled {
            let reaped = s.reap().unwrap();
            if reaped.deferred || reaped.freed_pages != 1 {
                not_a_result(&format!("a sample reap freed {reaped:?}, expected its 1 retained version"));
            }
        }
        let s = db.branch_stats().unwrap();
        if s.live_branches != n || s.arena_slots_in_use != 2 * n {
            not_a_result(&format!("sampling did not return to {n} branches and {} pages: {s:?}", 2 * n));
        }
        print_op(n, "grow_fork", &gfork);
        print_op(n, "grow_trunk", &gtrunk);
        print_op(n, "trunk_retain", &retain);
        print_op(n, "trunk_plain", &plain);
        for (name, op) in [("grow_fork", &gfork), ("grow_trunk", &gtrunk), ("trunk_retain", &retain), ("trunk_plain", &plain)] {
            for &(at, us) in &op.compactions {
                compaction_log.push((name, at, us));
            }
        }
        println!(
            "# x={n} live={} arena_in_use={} arena_free={} log_bytes={} arena_file_bytes={} snap_bytes={} \
             wal_bytes={} rss_bytes={} trunk_writes={}",
            s.live_branches,
            s.arena_slots_in_use,
            s.arena_slots_free,
            size_of(&b.files.log),
            size_of(&b.files.arena),
            size_of(&b.files.snap),
            size_of(&b.files.wal),
            rss_bytes(),
            b.writes
        );
    }
    println!("# compactions: {}", compaction_log.len());
    for (name, at, us) in &compaction_log {
        println!("# compaction\top={name}\tn={at}\tus={us:.1}");
    }
    drop(live);
    let end = db.branch_stats().unwrap();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!("# teardown: every branch freed, arena empty");
}
