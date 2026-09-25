//! Agent-workload arms of the branch curve: `../branch_curve/PREREG.md`, amendment 1.
//!
//!   cargo run -p turso_core --release --example branch_arms -- --arm <arm> [options]
//!
//! Arms (the amendment is the specification; this is its implementation):
//!
//!   hot        the trunk rewrites ONE row between every two forks (retained pre-images of one page)
//!   spread     the trunk rewrites a row that walks the whole table between every two forks
//!   chain      one fork chain, trunk -> b1 -> ... -> bd; x axis = depth d
//!   churn      steady N: every cycle forks + writes one branch and reaps a random live one
//!   churn_hot  churn, plus the trunk rewrites the hot row in every cycle
//!   churn_spread churn, plus the trunk rewrites the `spread` walk's next row in every cycle (amendment 3)
//!   pages      every branch writes w pages in one transaction; x axis = N, one block per w
//!   spread_trunk the `spread` arm's trunk writes with NO branches (amendment 2); x = trunk writes
//!   conc       T threads churn their own shares of N live branches (amendment 7); one cell per (N, T)
//!   reapconc   T threads reap trunk children concurrently while the trunk writes, and N is held by
//!              re-forks (frontier/round11/r11-k6-measure/PREREG.md); one cell per (N, T)
//!
//! `--no-autocheckpoint` disables the trunk connection's WAL auto-actions (auto-checkpoint and WAL
//! restart), amendment 2. Every state line prints the WAL file's size.
//!
//! `--synchronous off|normal|full` sets the trunk's sync mode (default off, as amendments 1-2).
//! Ported verbatim from the turso_curve lane's amendment 3 (`48a2b97a3`); this lane's amendment 5.
//!
//! `--victim oldest|random` (churn arms only; default random) picks each cycle's reap victim: a
//! uniformly random live branch, or the oldest one, which is the order uniform-TTL lease expiry
//! reaps in (amendment 3).
//!
//! `--threads 1,2,4,...` (conc only) is the T list; each N runs it forward and then reversed.
//! `--cycles` is then the cycles EACH thread runs per cell. `--lock-timing on|off` (conc only,
//! default off) turns on the store's lock-hold timing, the one lock counter that adds work under the
//! lock (amendment 7).
//!
//! Every read a sample makes is checked against a model the harness keeps itself (never against
//! the engine), and the engine's own counts are checked against the workload before a number is
//! printed; a mismatch prints `NOT A RESULT` and exits 1. Beside each latency the harness prints
//! the engine's work counters for that op (resolutions, nodes walked, retained versions compared),
//! so a slope can be read against an integer that load cannot move.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use turso_core::branch::{take_split_walk_counts, Branch, BranchWork, SplitWalkCounts};
use turso_core::{
    Connection, Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, SqliteDialect, Value,
    IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
/// The row the trunk rewrites in `hot` and `churn_hot`.
const HOT_ROW: i64 = 1;
/// Rows between two of one branch's writes in `pages`: more than a leaf holds (~37), so w <= 64
/// rows land on w distinct leaves (64 * 312 < 20,000 with 344 rows to spare at the wrap).
const PAGE_STRIDE: i64 = 312;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Hot,
    Spread,
    Chain,
    Churn,
    ChurnHot,
    ChurnSpread,
    Pages,
    SpreadTrunk,
    Conc,
    ReapConc,
}

/// How `reapconc` holds N after its reaps (r11-k6-measure PREREG §0).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Refill {
    /// The T threads reap alone, in timed phases; between phases the main thread re-forks.
    Phase,
    /// Each thread re-forks its own replacement, and writes the trunk, in its own cycle.
    Inline,
}

/// Which live branch a churn cycle reaps.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Victim {
    Random,
    Oldest,
}

struct Args {
    arm: Arm,
    victim: Victim,
    checkpoints: Vec<usize>,
    samples: usize,
    seed: u64,
    cycles: usize,
    windows: usize,
    w_list: Vec<usize>,
    no_autocheckpoint: bool,
    synchronous: String,
    threads: Vec<usize>,
    lock_timing: bool,
    /// `reapconc`: a spread trunk write after every k-th fork; 0 = no trunk write (V = 0).
    write_every: usize,
    refill: Refill,
    /// `reapconc`, phase refill only: the UNSAFE split walk (a throughput ceiling, nothing else).
    unsafe_split: bool,
    /// `reapconc`, with the split walk only: K6's race put back (UNSAFE; a leak demonstration).
    unsafe_k6_race: bool,
    /// `reapconc`: reaps take the trunk's lock by spinning instead of parking (a control).
    trunk_spin: bool,
    /// `reapconc`: warm-up windows of N single-threaded churn cycles before each N's cells.
    warmup: usize,
    cell_reaps: usize,
    round_reaps: usize,
    read_checks: usize,
}

fn parse_list(s: &str, what: &str) -> Vec<usize> {
    s.split(',')
        .map(|x| x.parse().unwrap_or_else(|_| die(&format!("bad {what}"))))
        .collect()
}

fn parse_args() -> Args {
    let mut arm = None;
    let mut args = Args {
        arm: Arm::Hot,
        victim: Victim::Random,
        checkpoints: vec![100, 1000],
        samples: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        cycles: 10_000,
        windows: 10,
        w_list: vec![1],
        no_autocheckpoint: false,
        synchronous: "OFF".to_string(),
        threads: Vec::new(),
        lock_timing: false,
        write_every: 8,
        refill: Refill::Phase,
        unsafe_split: false,
        unsafe_k6_race: false,
        trunk_spin: false,
        warmup: 12,
        cell_reaps: 100_000,
        round_reaps: 10_000,
        read_checks: 200,
    };
    let mut reapconc_flags = false;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--arm" => {
                arm = Some(match val().as_str() {
                    "hot" => Arm::Hot,
                    "spread" => Arm::Spread,
                    "chain" => Arm::Chain,
                    "churn" => Arm::Churn,
                    "churn_hot" => Arm::ChurnHot,
                    "churn_spread" => Arm::ChurnSpread,
                    "pages" => Arm::Pages,
                    "spread_trunk" => Arm::SpreadTrunk,
                    "conc" => Arm::Conc,
                    "reapconc" => Arm::ReapConc,
                    other => die(&format!("unknown arm {other}")),
                })
            }
            "--checkpoints" => args.checkpoints = parse_list(&val(), "--checkpoints"),
            "--samples" => args.samples = val().parse().unwrap_or_else(|_| die("bad --samples")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--cycles" => args.cycles = val().parse().unwrap_or_else(|_| die("bad --cycles")),
            "--windows" => args.windows = val().parse().unwrap_or_else(|_| die("bad --windows")),
            "--w" => args.w_list = parse_list(&val(), "--w"),
            "--no-autocheckpoint" => args.no_autocheckpoint = true,
            "--synchronous" => {
                args.synchronous = match val().as_str() {
                    "off" => "OFF",
                    "normal" => "NORMAL",
                    "full" => "FULL",
                    other => die(&format!("unknown --synchronous {other}")),
                }
                .to_string()
            }
            "--threads" => args.threads = parse_list(&val(), "--threads"),
            "--lock-timing" => {
                args.lock_timing = match val().as_str() {
                    "on" => true,
                    "off" => false,
                    other => die(&format!("unknown --lock-timing {other}")),
                }
            }
            "--write-every" => {
                args.write_every = val().parse().unwrap_or_else(|_| die("bad --write-every"));
                reapconc_flags = true;
            }
            "--refill" => {
                args.refill = match val().as_str() {
                    "phase" => Refill::Phase,
                    "inline" => Refill::Inline,
                    other => die(&format!("unknown --refill {other}")),
                };
                reapconc_flags = true;
            }
            "--unsafe-split-walk" => {
                args.unsafe_split = true;
                reapconc_flags = true;
            }
            "--unsafe-k6-race" => {
                args.unsafe_k6_race = true;
                reapconc_flags = true;
            }
            "--trunk-spin" => {
                args.trunk_spin = true;
                reapconc_flags = true;
            }
            "--warmup" => {
                args.warmup = val().parse().unwrap_or_else(|_| die("bad --warmup"));
                reapconc_flags = true;
            }
            "--cell-reaps" => {
                args.cell_reaps = val().parse().unwrap_or_else(|_| die("bad --cell-reaps"));
                reapconc_flags = true;
            }
            "--round-reaps" => {
                args.round_reaps = val().parse().unwrap_or_else(|_| die("bad --round-reaps"));
                reapconc_flags = true;
            }
            "--read-checks" => {
                args.read_checks = val().parse().unwrap_or_else(|_| die("bad --read-checks"));
                reapconc_flags = true;
            }
            "--victim" => {
                args.victim = match val().as_str() {
                    "random" => Victim::Random,
                    "oldest" => Victim::Oldest,
                    other => die(&format!("unknown victim policy {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.arm = arm.unwrap_or_else(|| die("--arm is required"));
    if args.victim != Victim::Random
        && !matches!(
            args.arm,
            Arm::Churn | Arm::ChurnHot | Arm::ChurnSpread | Arm::ReapConc
        )
    {
        die("--victim applies to the churn arms and reapconc only");
    }
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.samples == 0 || args.cycles == 0 || args.windows == 0 || args.cycles % args.windows != 0
    {
        die("--samples, --cycles, --windows must be positive and --windows must divide --cycles");
    }
    if args.w_list.is_empty() || args.w_list.iter().any(|&w| w == 0 || w > 64) {
        die("--w entries must be in 1..=64");
    }
    if matches!(args.arm, Arm::Conc | Arm::ReapConc) {
        if args.threads.is_empty() || args.threads.contains(&0) {
            die("--arm conc and reapconc need --threads, every entry positive");
        }
        if args.checkpoints.iter().any(|&n| n < *args.threads.iter().max().unwrap()) {
            die("--arm conc and reapconc need every checkpoint N >= the largest T, so each thread owns a branch");
        }
    } else if !args.threads.is_empty() || args.lock_timing {
        die("--threads and --lock-timing apply to --arm conc and reapconc only");
    }
    if args.arm == Arm::ReapConc {
        let tmax = *args.threads.iter().max().unwrap();
        if args.round_reaps < tmax || args.cell_reaps < args.round_reaps {
            die("--round-reaps must be >= the largest T and <= --cell-reaps");
        }
        if args.refill == Refill::Phase && args.checkpoints.iter().any(|&n| n < args.round_reaps) {
            die("--refill phase needs every N >= --round-reaps: a round must not empty a share");
        }
        if args.unsafe_k6_race && !args.unsafe_split {
            die("--unsafe-k6-race puts K6's race into the split walk: it needs --unsafe-split-walk");
        }
        if args.unsafe_split && args.refill != Refill::Phase {
            die("--unsafe-split-walk is valid with --refill phase only: an inline trunk write would \
                 retain a version the walk's snapshot lacks");
        }
    } else if reapconc_flags {
        die("--write-every, --refill, --unsafe-split-walk, --unsafe-k6-race, --trunk-spin, --warmup, \
             --cell-reaps, --round-reaps and --read-checks apply to --arm reapconc only");
    }
    args
}

fn die(msg: &str) -> ! {
    eprintln!("branch_arms: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
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

/// The trunk's `generation`-th rewrite of a row: same length, so it rewrites the row in place.
fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

/// `spread`: the row the trunk rewrites at its g-th write. 37 is coprime with 20,000, so the walk
/// visits every row, one leaf further on each time (a leaf holds ~37 rows).
fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

/// `chain`: level `l` writes a row in the first half of the table; `read_inh` reads the second
/// half, which no level writes, so its whole descent resolves through every level to the trunk.
fn chain_row(level: usize) -> i64 {
    ((level as u64).wrapping_mul(2_654_435_761) % (TRUNK_ROWS / 2) as u64) as i64 + 1
}

fn page_rows(n: usize, w: usize) -> Vec<i64> {
    (0..w as i64)
        .map(|j| (row_for(n) - 1 + j * PAGE_STRIDE) % TRUNK_ROWS + 1)
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

fn update(conn: &Arc<Connection>, id: i64) {
    update_to(conn, id, &branch_value(id));
}

fn update_to(conn: &Arc<Connection>, id: i64, value: &str) {
    conn.execute(format!("UPDATE t SET v = '{value}' WHERE id = {id}"))
        .unwrap();
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

/// The trunk's write history, kept by the harness: what a branch forked after trunk write number
/// `seq` must read for each row the trunk rewrote. Independent of the engine by construction.
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    /// row -> [(write seq, generation)], ascending seq.
    history: HashMap<i64, Vec<(u64, u64)>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push((g, g));
        g
    }
    /// The value of `row` for a branch forked when `writes_at_fork` trunk writes had committed.
    fn value_at(&self, row: i64, writes_at_fork: u64) -> String {
        let Some(h) = self.history.get(&row) else {
            return trunk_value(row);
        };
        let n = h.partition_point(|&(seq, _)| seq < writes_at_fork);
        if n == 0 {
            trunk_value(row)
        } else {
            trunk_gen_value(h[n - 1].1)
        }
    }
}

struct Live {
    branch: Branch,
    /// Rows this branch wrote itself.
    rows: Vec<i64>,
    /// Trunk writes committed when it was forked.
    trunk_writes_at_fork: u64,
}

impl Live {
    fn expect(&self, model: &TrunkModel, row: i64) -> String {
        if self.rows.contains(&row) {
            branch_value(row)
        } else {
            model.value_at(row, self.trunk_writes_at_fork)
        }
    }
}

/// One op's samples at one checkpoint, and the engine work its samples did.
#[derive(Default)]
struct Op {
    samples: Vec<Duration>,
    work: WorkSum,
}

#[derive(Default, Clone, Copy)]
struct WorkSum {
    resolve_calls: u64,
    resolve_levels: u64,
    resolve_retained_examined: u64,
    gc_examined: u64,
    gc_range_entries: u64,
}

impl WorkSum {
    fn add(&mut self, a: BranchWork, b: BranchWork) {
        self.resolve_calls += b.resolve_calls - a.resolve_calls;
        self.resolve_levels += b.resolve_levels - a.resolve_levels;
        self.resolve_retained_examined += b.resolve_retained_examined - a.resolve_retained_examined;
        self.gc_examined += b.gc_examined - a.gc_examined;
        self.gc_range_entries += b.gc_range_entries - a.gc_range_entries;
    }
}

struct Bench {
    db: Arc<Database>,
    wal_path: PathBuf,
    trunk: Arc<Connection>,
    model: TrunkModel,
    rng: Rng,
    /// (x, op, p50_us, levels_per_op, retained_examined_per_op, gc_examined_per_op)
    summary: Vec<(usize, &'static str, f64, f64, f64, f64)>,
}

impl Bench {
    fn work(&self) -> BranchWork {
        self.db.branch_stats().work
    }

    /// Time `f` as one sample of `op`, attributing the engine work it did. The counter snapshots
    /// sit outside the timed window.
    fn timed<T>(&self, op: &mut Op, f: impl FnOnce() -> T) -> T {
        let before = self.work();
        let t = Instant::now();
        let out = f();
        op.samples.push(t.elapsed());
        op.work.add(before, self.work());
        out
    }

    fn trunk_write(&mut self, row: i64) {
        let g = self.model.record(row);
        self.trunk
            .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g)))
            .unwrap();
    }

    fn print_op(&mut self, x: usize, name: &'static str, op: &Op) {
        let mut us: Vec<f64> = op.samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
        us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = us.len() as f64;
        let p50 = percentile(&us, 50.0);
        let per = |v: u64| v as f64 / n;
        println!(
            "{x}\t{name}\t{}\t{p50:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
            us.len(),
            percentile(&us, 90.0),
            percentile(&us, 99.0),
            us[us.len() - 1],
            per(op.work.resolve_calls),
            per(op.work.resolve_levels),
            per(op.work.resolve_retained_examined),
            per(op.work.gc_examined),
            per(op.work.gc_range_entries),
        );
        self.summary.push((
            x,
            name,
            p50,
            per(op.work.resolve_levels),
            per(op.work.resolve_retained_examined),
            per(op.work.gc_examined),
        ));
    }

    fn print_state(&self, x: usize, extra: &str) {
        let s = self.db.branch_stats();
        println!(
            "# x={x} live={} arena_in_use={} arena_free={} arena_high_water={} rss_bytes={} \
             trunk_writes={} wal_bytes={} {extra}",
            s.live_branches,
            s.arena_slots_in_use,
            s.arena_slots_free,
            s.arena_slots_in_use + s.arena_slots_free,
            rss_bytes(),
            self.model.writes,
            std::fs::metadata(&self.wal_path).map_or(0, |m| m.len())
        );
    }

    fn print_slopes(&self, xs: &[usize], ops: &[&'static str]) {
        if xs.len() < 2 {
            return;
        }
        println!("# log-log slopes over x={xs:?}: p50, then each work counter that is non-zero");
        for &op in ops {
            let rows: Vec<_> = self.summary.iter().filter(|r| r.1 == op).collect();
            if rows.len() < 2 {
                continue;
            }
            let slope = |f: &dyn Fn(&(usize, &str, f64, f64, f64, f64)) -> f64| -> Option<f64> {
                let pts: Vec<(f64, f64)> = rows
                    .iter()
                    .map(|r| ((r.0 as f64).ln(), f(r)))
                    .collect();
                if pts.iter().any(|&(_, y)| y <= 0.0) {
                    return None;
                }
                let pts: Vec<(f64, f64)> = pts.into_iter().map(|(x, y)| (x, y.ln())).collect();
                let m = pts.len() as f64;
                let (sx, sy) = pts.iter().fold((0.0, 0.0), |(a, b), (x, y)| (a + x, b + y));
                let (mx, my) = (sx / m, sy / m);
                let num: f64 = pts.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
                let den: f64 = pts.iter().map(|(x, _)| (x - mx).powi(2)).sum();
                Some(num / den)
            };
            let last = rows[rows.len() - 1];
            let prev = rows[rows.len() - 2];
            let local = (last.2 / prev.2).ln() / ((last.0 as f64) / (prev.0 as f64)).ln();
            let fmt = |v: Option<f64>| v.map_or("-".to_string(), |v| format!("{v:+.3}"));
            println!(
                "# slope\t{op}\tp50 {:+.3}\tlocal_last_step {:+.3}\tlevels {}\tret_examined {}\tgc_examined {}",
                slope(&|r| r.2).unwrap(),
                local,
                fmt(slope(&|r| r.3)),
                fmt(slope(&|r| r.4)),
                fmt(slope(&|r| r.5)),
            );
        }
    }
}

const HEADER: &str = "x\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us\tresolves_per_op\tlevels_per_op\tret_examined_per_op\tgc_examined_per_op\tgc_range_per_op";

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_arms.db");
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
    if args.no_autocheckpoint {
        // Amendment 2: no auto-checkpoint and no WAL restart on the trunk connection.
        trunk.wal_auto_actions_disable();
    }
    // Amendment 1: trunk commits do not fsync. The fsync is not the mechanism under test, and the
    // `hot`/`spread`/`churn_hot` arms commit on the trunk once per fork, up to 10^6 times.
    // Amendment 5: `--synchronous` overrides the mode; the default stays OFF.
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
    let page_size = int("PRAGMA page_size");
    let trunk_pages = int("PRAGMA page_count");
    let synchronous = int("PRAGMA synchronous");

    println!("# branch_arms — Turso fork, per-branch CoW arena, PREREG amendment 1");
    println!(
        "# arm={:?} victim={:?} checkpoints={:?} samples={} seed={:#x} cycles={} windows={} w={:?} \
         trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} page_size={page_size} \
         trunk_pages={trunk_pages} trunk_synchronous={synchronous} no_autocheckpoint={}",
        args.arm,
        args.victim,
        args.checkpoints,
        args.samples,
        args.seed,
        args.cycles,
        args.windows,
        args.w_list,
        args.no_autocheckpoint
    );
    println!(
        "# clock tick {:.0} ns (Instant); times are microseconds per operation; work columns are \
         engine counters per sample of that op",
        clock_tick_ns()
    );
    println!(
        "# build: {} ; rss_base_bytes={}",
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        rss_bytes()
    );

    let mut bench = Bench {
        db: db.clone(),
        wal_path: PathBuf::from(format!("{}-wal", path.to_str().unwrap())),
        trunk,
        model: TrunkModel::default(),
        rng: Rng(args.seed),
        summary: Vec::new(),
    };
    match args.arm {
        Arm::Hot | Arm::Spread => arm_trunk_writes(&mut bench, &args),
        Arm::Chain => arm_chain(&mut bench, &args),
        Arm::Churn | Arm::ChurnHot | Arm::ChurnSpread => arm_churn(&mut bench, &args),
        Arm::Pages => arm_pages(&mut bench, &args),
        Arm::SpreadTrunk => arm_spread_trunk(&mut bench, &args),
        Arm::Conc => arm_conc(&mut bench, &args),
        Arm::ReapConc => arm_reapconc(&mut bench, &args),
    }
    let end = db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown: every branch freed, arena empty ({} free slots)",
        end.arena_slots_free
    );
}

/// Fork one branch from the trunk and give it one page of its own.
fn grow_from_trunk(b: &mut Bench, row: i64) -> Live {
    let branch = b.trunk.fork_branch().unwrap();
    let live = Live {
        branch,
        rows: vec![row],
        trunk_writes_at_fork: b.model.writes,
    };
    let conn = live.branch.connect().unwrap();
    update(&conn, row);
    drop(conn);
    live
}

/// The trunk write that follows every fork in `hot`, `spread`, `churn_hot` and `churn_spread`.
fn trunk_row(arm: Arm, g: u64) -> i64 {
    match arm {
        Arm::Hot | Arm::ChurnHot => HOT_ROW,
        Arm::Spread | Arm::ChurnSpread => spread_row(g),
        _ => unreachable!(),
    }
}

/// Arms (a1) `hot` and (a2) `spread`: N live branches, each forked from the trunk and written
/// once, with one trunk write after every fork.
fn arm_trunk_writes(b: &mut Bench, args: &Args) {
    println!("{HEADER}");
    let arm = args.arm;
    let mut live: Vec<Live> = Vec::new();
    let mut grown = 0usize;
    // Versions retained minus versions freed, from the harness's own count of trunk writes and
    // the engine's report of what each reap freed beyond the branch's own page.
    let mut retained_expected: usize = 0;
    for &n in &args.checkpoints {
        let t = Instant::now();
        while live.len() < n {
            live.push(grow_from_trunk(b, row_for(grown)));
            grown += 1;
            b.trunk_write(trunk_row(arm, b.model.writes));
            retained_expected += 1;
        }
        let grow_us = t.elapsed().as_secs_f64() * 1e6;
        let s = b.db.branch_stats();
        if s.live_branches != n {
            not_a_result(&format!("expected {n} live branches, engine has {}", s.live_branches));
        }
        // Each branch owns one page; each trunk write after a fork retained exactly the one leaf it
        // rewrote (an in-place UPDATE dirties one page).
        if s.arena_slots_in_use != n + retained_expected {
            not_a_result(&format!(
                "expected {n} own + {retained_expected} retained arena pages, engine has {}",
                s.arena_slots_in_use
            ));
        }

        let k = args.samples;
        let (mut fork, mut open, mut first_write, mut trunk_w) =
            (Op::default(), Op::default(), Op::default(), Op::default());
        let mut sampled: Vec<Live> = Vec::with_capacity(k);
        for i in 0..k {
            let row = row_for(grown + i);
            let at_fork = b.model.writes;
            let branch = b.timed(&mut fork, || b.trunk.fork_branch().unwrap());
            let conn = b.timed(&mut open, || branch.connect().unwrap());
            b.timed(&mut first_write, || update(&conn, row));
            drop(conn);
            sampled.push(Live {
                branch,
                rows: vec![row],
                trunk_writes_at_fork: at_fork,
            });
            let trow = trunk_row(arm, b.model.writes);
            let g = b.model.record(trow);
            let sql = format!("UPDATE t SET v = '{}' WHERE id = {trow}", trunk_gen_value(g));
            b.timed(&mut trunk_w, || b.trunk.execute(sql).unwrap());
            retained_expected += 1;
        }
        grown += k;

        let (mut read_open, mut read_own, mut read_inh, mut read_hot) =
            (Op::default(), Op::default(), Op::default(), Op::default());
        for _ in 0..k {
            let target = &live[b.rng.below(live.len())];
            let own = target.rows[0];
            let other = (own - 1 + TRUNK_ROWS / 2) % TRUNK_ROWS + 1;
            let conn = b.timed(&mut read_open, || target.branch.connect().unwrap());
            let got_own = b.timed(&mut read_own, || read_v(&conn, own));
            let got_inh = b.timed(&mut read_inh, || read_v(&conn, other));
            drop(conn);
            // A fresh connection, so the hot row's leaf is resolved, not served from its cache.
            let conn = target.branch.connect().unwrap();
            let got_hot = b.timed(&mut read_hot, || read_v(&conn, HOT_ROW));
            drop(conn);
            if got_own != target.expect(&b.model, own)
                || got_inh != target.expect(&b.model, other)
                || got_hot != target.expect(&b.model, HOT_ROW)
            {
                not_a_result("a sampled read returned the wrong version");
            }
        }

        let mut reap = Op::default();
        for s in sampled {
            let reaped = b.timed(&mut reap, || s.branch.reap().unwrap());
            if reaped.deferred || reaped.freed_pages < 1 {
                not_a_result(&format!("a sampled reap freed {reaped:?}"));
            }
            retained_expected -= reaped.freed_pages - 1;
        }
        let s = b.db.branch_stats();
        if s.live_branches != n || s.arena_slots_in_use != n + retained_expected {
            not_a_result(&format!(
                "sampling did not return the engine to {n} branches and {n} + {retained_expected} \
                 pages: {s:?}"
            ));
        }
        for (name, op) in [
            ("fork", &fork),
            ("open", &open),
            ("first_write", &first_write),
            ("trunk_write", &trunk_w),
            ("read_open", &read_open),
            ("read_own", &read_own),
            ("read_inh", &read_inh),
            ("read_hot", &read_hot),
            ("reap", &reap),
        ] {
            b.print_op(n, name, op);
        }
        b.print_state(
            n,
            &format!("grow_total_us={grow_us:.0} retained_pages={retained_expected}"),
        );
    }
    b.print_slopes(
        &args.checkpoints,
        &[
            "fork",
            "open",
            "first_write",
            "trunk_write",
            "read_open",
            "read_own",
            "read_inh",
            "read_hot",
            "reap",
        ],
    );
    drop(live);
}

/// Arm (b) `chain`: one chain trunk -> b1 -> ... -> bd, each level writing one row and then
/// forking the next. x = d, the depth of the tip.
fn arm_chain(b: &mut Bench, args: &Args) {
    println!("{HEADER}");
    let mut chain: Vec<Live> = Vec::new();
    for &d in &args.checkpoints {
        while chain.len() < d {
            let level = chain.len() + 1;
            let branch = match chain.last() {
                None => b.trunk.fork_branch().unwrap(),
                Some(parent) => parent.branch.fork().unwrap(),
            };
            let row = chain_row(level);
            let conn = branch.connect().unwrap();
            update(&conn, row);
            drop(conn);
            chain.push(Live {
                branch,
                rows: vec![row],
                trunk_writes_at_fork: 0,
            });
        }
        let s = b.db.branch_stats();
        if s.live_branches != d || s.arena_slots_in_use != d {
            not_a_result(&format!("expected {d} branches and {d} arena pages: {s:?}"));
        }
        let tip = chain.last().unwrap();
        let tip_row = chain_row(d);
        let anc_row = chain_row(1);

        let k = args.samples;
        let (mut fork, mut open, mut first_write, mut reap) =
            (Op::default(), Op::default(), Op::default(), Op::default());
        for i in 0..k {
            // A fresh child of the tip, at depth d + 1, writing a first-half row. The value is one no
            // row holds (amendment 2): an UPDATE to an identical payload is skipped by OpInsert's
            // no-op check, dirties nothing and copies nothing, and chain_row(10^6 + i) is
            // chain_row(i), a row level i already wrote with branch_value.
            let row = chain_row(1_000_000 + i);
            let value = format!("c{:0>width$}", i, width = VALUE_LEN - 1);
            let child = b.timed(&mut fork, || tip.branch.fork().unwrap());
            let conn = b.timed(&mut open, || child.connect().unwrap());
            b.timed(&mut first_write, || update_to(&conn, row, &value));
            drop(conn);
            let reaped = b.timed(&mut reap, || child.reap().unwrap());
            if reaped.deferred || reaped.freed_pages != 1 {
                not_a_result(&format!("a sampled child reap freed {reaped:?}, expected 1 page"));
            }
        }
        let (mut read_open, mut read_own, mut read_inh, mut read_anc) =
            (Op::default(), Op::default(), Op::default(), Op::default());
        for _ in 0..k {
            let inh = TRUNK_ROWS / 2 + 1 + b.rng.below((TRUNK_ROWS / 2) as usize) as i64;
            let conn = b.timed(&mut read_open, || tip.branch.connect().unwrap());
            let got_own = b.timed(&mut read_own, || read_v(&conn, tip_row));
            drop(conn);
            let conn = tip.branch.connect().unwrap();
            let got_inh = b.timed(&mut read_inh, || read_v(&conn, inh));
            drop(conn);
            let conn = tip.branch.connect().unwrap();
            let got_anc = b.timed(&mut read_anc, || read_v(&conn, anc_row));
            drop(conn);
            if got_own != branch_value(tip_row)
                || got_inh != trunk_value(inh)
                || got_anc != branch_value(anc_row)
            {
                not_a_result("a sampled read at the tip returned the wrong version");
            }
        }
        let s = b.db.branch_stats();
        if s.live_branches != d || s.arena_slots_in_use != d {
            not_a_result(&format!("sampling did not return the chain to {d}: {s:?}"));
        }
        for (name, op) in [
            ("fork", &fork),
            ("open", &open),
            ("first_write", &first_write),
            ("reap", &reap),
            ("read_open", &read_open),
            ("read_own", &read_own),
            ("read_inh", &read_inh),
            ("read_anc", &read_anc),
        ] {
            b.print_op(d, name, op);
        }
        b.print_state(d, "");
    }
    b.print_slopes(
        &args.checkpoints,
        &[
            "fork",
            "open",
            "first_write",
            "reap",
            "read_open",
            "read_own",
            "read_inh",
            "read_anc",
        ],
    );
    // The cascade: release every ancestor handle (each deferred: a live child reads through it),
    // then reap the tip, which frees the whole chain in one call. One sample, labelled as such.
    let d = chain.len();
    let tip = chain.pop().unwrap();
    for l in chain {
        let r = l.branch.reap().unwrap();
        if !r.deferred || r.freed_pages != 0 {
            not_a_result(&format!("an ancestor with a live child was freed: {r:?}"));
        }
    }
    let before = b.work();
    let t = Instant::now();
    let r = tip.branch.reap().unwrap();
    let us = t.elapsed().as_secs_f64() * 1e6;
    let after = b.work();
    if r.deferred || r.freed_pages != d {
        not_a_result(&format!("the cascade freed {r:?}, expected {d} pages"));
    }
    println!(
        "# cascade_reap d={d} us={us:.2} freed_pages={} gc_range_entries={} (ONE sample)",
        r.freed_pages,
        after.gc_range_entries - before.gc_range_entries
    );
}

/// Arms (c) `churn`, `churn_hot` and `churn_spread`: steady N. Each cycle forks and writes one
/// branch (plus one trunk write in `churn_hot` and `churn_spread`) and reaps one older live branch:
/// a uniformly random one, or with `--victim oldest` the oldest (amendment 3).
fn arm_churn(b: &mut Bench, args: &Args) {
    println!("{HEADER}");
    let hot = args.arm == Arm::ChurnHot;
    let spread = args.arm == Arm::ChurnSpread;
    let trunk_writes = hot || spread;
    // Fork order front to back: the front is the oldest live branch.
    let mut live: VecDeque<Live> = VecDeque::new();
    let mut grown = 0usize;
    let per_window = args.cycles / args.windows;
    let own_plus_retained = |n: usize| if hot { 2 * n } else { n };
    // churn_spread: versions retained minus versions freed, from the harness's own count of trunk
    // writes (each retains exactly one leaf: a live child forked since the leaf's last write, the
    // one forked this cycle) and the engine's report of what each reap freed beyond its own page.
    let mut retained_expected: usize = 0;
    for &n in &args.checkpoints {
        while live.len() < n {
            live.push_back(grow_from_trunk(b, row_for(grown)));
            grown += 1;
            if trunk_writes {
                b.trunk_write(trunk_row(args.arm, b.model.writes));
                retained_expected += 1;
            }
        }
        let s = b.db.branch_stats();
        let expected_in_use = if spread { n + retained_expected } else { own_plus_retained(n) };
        if s.live_branches != n || s.arena_slots_in_use != expected_in_use {
            not_a_result(&format!(
                "expected {n} branches and {expected_in_use} arena pages before churn: {s:?}"
            ));
        }
        let mut all: [Op; 8] = Default::default();
        let names = [
            "fork",
            "open",
            "first_write",
            "trunk_write",
            "reap",
            "read_own",
            "read_hot",
            "read_inh",
        ];
        let mut off_prediction_reaps = 0usize;
        let mut versions_freed = 0usize;
        let rss0 = rss_bytes();
        for wdx in 0..args.windows {
            let mut win: [Op; 8] = Default::default();
            for c in 0..per_window {
                let row = row_for(grown);
                grown += 1;
                let at_fork = b.model.writes;
                let branch = b.timed(&mut win[0], || b.trunk.fork_branch().unwrap());
                let conn = b.timed(&mut win[1], || branch.connect().unwrap());
                b.timed(&mut win[2], || update(&conn, row));
                drop(conn);
                if trunk_writes {
                    let trow = trunk_row(args.arm, b.model.writes);
                    let g = b.model.record(trow);
                    let sql = format!("UPDATE t SET v = '{}' WHERE id = {trow}", trunk_gen_value(g));
                    b.timed(&mut win[3], || b.trunk.execute(sql).unwrap());
                    retained_expected += 1;
                }
                // `swap_remove_back` is `Vec::swap_remove`: the random policy draws and removes
                // exactly as amendment 1's harness did.
                let victim = match args.victim {
                    Victim::Random => live.swap_remove_back(b.rng.below(live.len())).unwrap(),
                    Victim::Oldest => live.pop_front().unwrap(),
                };
                live.push_back(Live {
                    branch,
                    rows: vec![row],
                    trunk_writes_at_fork: at_fork,
                });
                let reaped = b.timed(&mut win[4], || victim.branch.reap().unwrap());
                if reaped.deferred || reaped.freed_pages < 1 {
                    not_a_result(&format!("a churn reap freed {reaped:?}"));
                }
                versions_freed += reaped.freed_pages - 1;
                retained_expected = retained_expected
                    .checked_sub(reaped.freed_pages - 1)
                    .unwrap_or_else(|| {
                        not_a_result(&format!(
                            "a churn reap freed {reaped:?}, more versions than were retained"
                        ))
                    });
                // Predicted from the source: the victim's own page, plus in churn_hot the one
                // version of the hot page that only it could see. churn_spread has no fixed count.
                if !spread && reaped.freed_pages != if hot { 2 } else { 1 } {
                    off_prediction_reaps += 1;
                }
                if c % 10 == 0 {
                    let target = &live[b.rng.below(live.len())];
                    let own = target.rows[0];
                    let conn = target.branch.connect().unwrap();
                    let got = b.timed(&mut win[5], || read_v(&conn, own));
                    drop(conn);
                    if got != target.expect(&b.model, own) {
                        not_a_result("a churn read of a branch's own row returned the wrong version");
                    }
                    if hot {
                        let conn = target.branch.connect().unwrap();
                        let got = b.timed(&mut win[6], || read_v(&conn, HOT_ROW));
                        drop(conn);
                        if got != target.expect(&b.model, HOT_ROW) {
                            not_a_result("a churn read of the hot row returned the wrong version");
                        }
                    }
                    if spread {
                        // A row the branch did not write, on a leaf the trunk's walk rewrites.
                        let other = (own - 1 + TRUNK_ROWS / 2) % TRUNK_ROWS + 1;
                        let conn = target.branch.connect().unwrap();
                        let got = b.timed(&mut win[7], || read_v(&conn, other));
                        drop(conn);
                        if got != target.expect(&b.model, other) {
                            not_a_result("a churn read of a trunk-written row returned the wrong version");
                        }
                    }
                }
            }
            let s = b.db.branch_stats();
            if s.live_branches != n {
                not_a_result(&format!("churn left {} live branches, expected {n}", s.live_branches));
            }
            if spread && s.arena_slots_in_use != n + retained_expected {
                not_a_result(&format!(
                    "churn_spread window {wdx}: expected {n} own + {retained_expected} retained arena \
                     pages, engine has {}",
                    s.arena_slots_in_use
                ));
            }
            let mut line = format!("# window x={n} w={wdx}");
            for (i, op) in win.iter_mut().enumerate() {
                if op.samples.is_empty() {
                    continue;
                }
                let mut us: Vec<f64> = op.samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
                us.sort_by(|a, b| a.partial_cmp(b).unwrap());
                line += &format!(
                    " {}_p50={:.2} {}_p99={:.2}",
                    names[i],
                    percentile(&us, 50.0),
                    names[i],
                    percentile(&us, 99.0)
                );
                all[i].samples.append(&mut op.samples);
                let w = op.work;
                all[i].work.resolve_calls += w.resolve_calls;
                all[i].work.resolve_levels += w.resolve_levels;
                all[i].work.resolve_retained_examined += w.resolve_retained_examined;
                all[i].work.gc_examined += w.gc_examined;
                all[i].work.gc_range_entries += w.gc_range_entries;
            }
            line += &format!(
                " arena_in_use={} arena_high_water={} rss_bytes={}",
                s.arena_slots_in_use,
                s.arena_slots_in_use + s.arena_slots_free,
                rss_bytes()
            );
            println!("{line}");
        }
        for (i, op) in all.iter().enumerate() {
            if !op.samples.is_empty() {
                b.print_op(n, names[i], op);
            }
        }
        let s = b.db.branch_stats();
        let predicted = if spread { n + retained_expected } else { own_plus_retained(n) };
        b.print_state(
            n,
            &format!(
                "cycles={} victim={:?} predicted_arena_in_use={predicted} \
                 predicted_high_water_max={} off_prediction_reaps={} versions_freed={versions_freed} \
                 rss_before_churn={rss0}",
                args.cycles,
                args.victim,
                if spread {
                    "n/a".to_string()
                } else {
                    (own_plus_retained(n) + if hot { 2 } else { 1 }).to_string()
                },
                if spread { "n/a".to_string() } else { off_prediction_reaps.to_string() },
            ),
        );
        if s.arena_slots_in_use != predicted {
            println!(
                "# ARENA-PREDICTION-MISS x={n}: in use {} != predicted {predicted}",
                s.arena_slots_in_use
            );
        }
    }
    b.print_slopes(
        &args.checkpoints,
        &[
            "fork",
            "open",
            "first_write",
            "trunk_write",
            "reap",
            "read_own",
            "read_hot",
            "read_inh",
        ],
    );
    drop(live);
}

/// Arm (d) `pages`: every branch writes w rows on w distinct leaves in one transaction. One block
/// per w; x = N inside a block. The engine is torn down to empty between blocks.
fn arm_pages(b: &mut Bench, args: &Args) {
    println!("{HEADER}");
    // (w, x, op, p50) across blocks, for the slope against w at each fixed N.
    let mut by_w: Vec<(usize, usize, &'static str, f64)> = Vec::new();
    for &w in &args.w_list {
        println!("# block w={w}");
        let mut live: Vec<Live> = Vec::new();
        let mut grown = 0usize;
        let write_w = |conn: &Arc<Connection>, rows: &[i64]| {
            conn.execute("BEGIN").unwrap();
            for &r in rows {
                update(conn, r);
            }
            conn.execute("COMMIT").unwrap();
        };
        let first_x = b.summary.len();
        for &n in &args.checkpoints {
            while live.len() < n {
                let rows = page_rows(grown, w);
                grown += 1;
                let branch = b.trunk.fork_branch().unwrap();
                let conn = branch.connect().unwrap();
                write_w(&conn, &rows);
                drop(conn);
                live.push(Live {
                    branch,
                    rows,
                    trunk_writes_at_fork: 0,
                });
            }
            let s = b.db.branch_stats();
            if s.live_branches != n || s.arena_slots_in_use != n * w {
                not_a_result(&format!("expected {n} branches and {} arena pages: {s:?}", n * w));
            }
            let k = args.samples;
            let (mut fork, mut open, mut write, mut reap, mut read_own) = (
                Op::default(),
                Op::default(),
                Op::default(),
                Op::default(),
                Op::default(),
            );
            let mut sampled = Vec::with_capacity(k);
            for i in 0..k {
                let rows = page_rows(grown + i, w);
                let branch = b.timed(&mut fork, || b.trunk.fork_branch().unwrap());
                let conn = b.timed(&mut open, || branch.connect().unwrap());
                b.timed(&mut write, || write_w(&conn, &rows));
                drop(conn);
                sampled.push(branch);
            }
            grown += k;
            for _ in 0..k {
                let target = &live[b.rng.below(live.len())];
                let row = target.rows[b.rng.below(w)];
                let conn = target.branch.connect().unwrap();
                let got = b.timed(&mut read_own, || read_v(&conn, row));
                drop(conn);
                if got != branch_value(row) {
                    not_a_result("a branch did not read its own write");
                }
            }
            for branch in sampled {
                let reaped = b.timed(&mut reap, || branch.reap().unwrap());
                if reaped.deferred || reaped.freed_pages != w {
                    not_a_result(&format!("a sampled reap freed {reaped:?}, expected {w} pages"));
                }
            }
            let s = b.db.branch_stats();
            if s.live_branches != n || s.arena_slots_in_use != n * w {
                not_a_result(&format!("sampling did not return to {n} x {w}: {s:?}"));
            }
            for (name, op) in [
                ("fork", &fork),
                ("open", &open),
                ("write_w", &write),
                ("read_own", &read_own),
                ("reap", &reap),
            ] {
                b.print_op(n, name, op);
            }
            b.print_state(n, &format!("w={w} pages_per_branch_expected={w}"));
        }
        let xs = args.checkpoints.clone();
        let block: Vec<_> = b.summary.drain(first_x..).collect();
        let saved = std::mem::replace(&mut b.summary, block);
        b.print_slopes(&xs, &["fork", "open", "write_w", "read_own", "reap"]);
        let block = std::mem::replace(&mut b.summary, saved);
        for r in block {
            println!(
                "# wsummary\tw={w}\tx={}\t{}\tp50_us={:.2}\tp50_us_per_page={:.3}",
                r.0,
                r.1,
                r.2,
                r.2 / w as f64
            );
            by_w.push((w, r.0, r.1, r.2));
        }
        drop(live);
        let s = b.db.branch_stats();
        if s.live_branches != 0 || s.arena_slots_in_use != 0 {
            not_a_result(&format!("block w={w} teardown leaked: {s:?}"));
        }
    }
    if args.w_list.len() >= 2 {
        println!("# log-log slope of p50 against w={:?}, at each N", args.w_list);
        for &n in &args.checkpoints {
            for op in ["fork", "open", "write_w", "read_own", "reap"] {
                let pts: Vec<(f64, f64)> = by_w
                    .iter()
                    .filter(|r| r.1 == n && r.2 == op)
                    .map(|r| ((r.0 as f64).ln(), r.3.ln()))
                    .collect();
                let m = pts.len() as f64;
                let (sx, sy) = pts.iter().fold((0.0, 0.0), |(a, b), (x, y)| (a + x, b + y));
                let (mx, my) = (sx / m, sy / m);
                let num: f64 = pts.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
                let den: f64 = pts.iter().map(|(x, _)| (x - mx).powi(2)).sum();
                println!("# wslope\tN={n}\t{op}\t{:+.3}", num / den);
            }
        }
    }
}

/// Amendment 2, run e1: the `spread` arm's trunk write sequence with no branch ever forked, so the
/// branch store is never consulted on a trunk write (`trunk_has_children()` is false). x = trunk
/// writes committed before the checkpoint's K timed writes.
fn arm_spread_trunk(b: &mut Bench, args: &Args) {
    println!("{HEADER}");
    for &n in &args.checkpoints {
        while (b.model.writes as usize) < n {
            b.trunk_write(spread_row(b.model.writes));
        }
        let mut tw = Op::default();
        for _ in 0..args.samples {
            let row = spread_row(b.model.writes);
            let g = b.model.record(row);
            let sql = format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g));
            b.timed(&mut tw, || b.trunk.execute(sql).unwrap());
        }
        let last = spread_row(b.model.writes - 1);
        if read_v(&b.trunk, last) != b.model.value_at(last, b.model.writes) {
            not_a_result("the trunk does not read its own last write");
        }
        if b.db.branch_stats().live_branches != 0 {
            not_a_result("a branch exists in the no-branch arm");
        }
        b.print_op(n, "trunk_write", &tw);
        b.print_state(n, "");
    }
    b.print_slopes(&args.checkpoints, &["trunk_write"]);
}

/// The ops of one `conc` cycle, in the order a cycle runs them.
const CONC_OPS: [&str; 7] = [
    "fork",
    "open",
    "first_write",
    "read_open",
    "read_own",
    "read_inh",
    "reap",
];

const CONC_HEADER: &str = "N\tT\tdraw\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us\tbusy_retries";

/// What one `conc` thread hands back: its share of the live branches, one latency per cycle per op,
/// and the `Busy` answers it retried, per op.
struct ConcOut {
    share: Vec<Live>,
    ops: [Vec<Duration>; 7],
    busy: [u64; 7],
    elapsed: Duration,
}

/// Run `f` until it does not answer `Busy`/`BusySnapshot`, counting those answers. Anything else
/// is not a result.
fn busy_retry<T>(busy: &mut u64, what: &str, mut f: impl FnMut() -> turso_core::Result<T>) -> T {
    loop {
        match f() {
            Ok(v) => return v,
            Err(LimboError::Busy | LimboError::BusySnapshot) => {
                *busy += 1;
                std::thread::yield_now();
            }
            Err(e) => not_a_result(&format!("conc {what} failed: {e}")),
        }
    }
}

fn select_v(conn: &Arc<Connection>, id: i64) -> turso_core::Result<Vec<Vec<Value>>> {
    conn.prepare(format!("SELECT v FROM t WHERE id = {id}"))?
        .run_collect_rows()
}

fn check_v(rows: &[Vec<Value>], id: i64, expect: &str) {
    match rows {
        [row] => match &row[0] {
            Value::Text(t) if t.as_str() == expect => {}
            other => not_a_result(&format!("conc read of row {id}: got {other:?}, expected {expect}")),
        },
        _ => not_a_result(&format!("conc read of row {id}: {} rows", rows.len())),
    }
}

/// One `conc` thread: `cycles` cycles against its own trunk connection and its own share. Each
/// cycle forks a branch, writes row `row_for(base + c)` on it, opens a random branch of the share
/// and reads its own row and a far row (the trunk's), then reaps a random branch of the share
/// (never the one forked this cycle, which joins the share after the draw), so the share's size is
/// fixed. Every read is checked against what the harness knows the branch holds; the trunk never
/// writes in this arm, so a far row is always the trunk's original value.
fn conc_thread(
    trunk: Arc<Connection>,
    mut share: Vec<Live>,
    barrier: &Barrier,
    seed: u64,
    base: usize,
    cycles: usize,
) -> ConcOut {
    let mut rng = Rng(seed | 1);
    let mut ops: [Vec<Duration>; 7] = Default::default();
    for op in ops.iter_mut() {
        op.reserve_exact(cycles);
    }
    let mut busy = [0u64; 7];
    barrier.wait();
    let start = Instant::now();
    for c in 0..cycles {
        let row = row_for(base + c);
        let t0 = Instant::now();
        let branch = busy_retry(&mut busy[0], "fork", || trunk.fork_branch());
        let t1 = Instant::now();
        let conn = busy_retry(&mut busy[1], "open", || branch.connect());
        let t2 = Instant::now();
        busy_retry(&mut busy[2], "first_write", || {
            conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(row)))
        });
        let t3 = Instant::now();
        drop(conn);
        let target = &share[rng.below(share.len())];
        let own = target.rows[0];
        let far = (own - 1 + TRUNK_ROWS / 2) % TRUNK_ROWS + 1;
        let t4 = Instant::now();
        let conn = busy_retry(&mut busy[3], "read_open", || target.branch.connect());
        let t5 = Instant::now();
        let got_own = busy_retry(&mut busy[4], "read_own", || select_v(&conn, own));
        let t6 = Instant::now();
        let got_far = busy_retry(&mut busy[5], "read_inh", || select_v(&conn, far));
        let t7 = Instant::now();
        drop(conn);
        check_v(&got_own, own, &branch_value(own));
        check_v(&got_far, far, &trunk_value(far));
        let victim = share.swap_remove(rng.below(share.len()));
        share.push(Live {
            branch,
            rows: vec![row],
            trunk_writes_at_fork: 0,
        });
        let t8 = Instant::now();
        let reaped = victim.branch.reap().unwrap_or_else(|e| not_a_result(&format!("conc reap failed: {e}")));
        let t9 = Instant::now();
        if reaped.deferred || reaped.freed_pages != 1 {
            not_a_result(&format!("a conc reap freed {reaped:?}, expected exactly 1 page"));
        }
        for (op, d) in ops.iter_mut().zip([
            t1 - t0,
            t2 - t1,
            t3 - t2,
            t5 - t4,
            t6 - t5,
            t7 - t6,
            t9 - t8,
        ]) {
            op.push(d);
        }
    }
    ConcOut {
        share,
        ops,
        busy,
        elapsed: start.elapsed(),
    }
}

/// The box's parallelism at this moment, with nothing shared: `t` threads each run the same fixed
/// xorshift loop. Operations per second over the slowest thread's finish.
fn null_ops_per_s(t: usize) -> f64 {
    const ITER: u64 = 100_000_000;
    let barrier = Barrier::new(t + 1);
    let wall = std::thread::scope(|s| {
        let handles: Vec<_> = (0..t)
            .map(|i| {
                let barrier = &barrier;
                s.spawn(move || {
                    let mut r = Rng(0x2545_F491_4F6C_DD1D ^ (i as u64 + 1));
                    barrier.wait();
                    let mut acc = 0u64;
                    for _ in 0..ITER {
                        acc = acc.wrapping_add(r.next());
                    }
                    std::hint::black_box(acc)
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    });
    (t as u64 * ITER) as f64 / wall.as_secs_f64()
}

/// User and system CPU time of the whole process so far, in nanoseconds.
fn cpu_ns() -> (u64, u64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        not_a_result("getrusage failed");
    }
    let ns = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000_000 + tv.tv_usec as u64 * 1_000;
    (ns(ru.ru_utime), ns(ru.ru_stime))
}

/// Arm (f) `conc` (amendment 7): T threads, each churning its own share of N live branches with
/// fork -> first write -> reads -> reap (see [`conc_thread`]). One cell per (N, T); the T list runs
/// forward and then reversed at each N (draws 0 and 1), so drift across a block shows as the
/// difference between one T's two draws. Before each cell, the null workload measures what the box
/// gives T threads that share nothing. Per cell: throughput, per-op latency, Busy retries, process
/// CPU time, and the store's lock counters (acquisitions, contended acquisitions, wait, hold).
fn arm_conc(b: &mut Bench, args: &Args) {
    let tmax = *args.threads.iter().max().unwrap();
    b.db.set_branch_lock_timing(args.lock_timing);
    // One trunk connection per thread: a connection serves one thread at a time.
    let trunks: Vec<Arc<Connection>> = (0..tmax).map(|_| b.db.connect().unwrap()).collect();
    println!(
        "# conc: threads={:?} (forward, then reversed) cycles_per_thread={} lock_timing={}",
        args.threads, args.cycles, args.lock_timing
    );
    println!("{CONC_HEADER}");
    let mut live: Vec<Live> = Vec::new();
    let mut grown = 0usize;
    let c = args.cycles;
    for &n in &args.checkpoints {
        let t = Instant::now();
        while live.len() < n {
            live.push(grow_from_trunk(b, row_for(grown)));
            grown += 1;
        }
        let grow_us = t.elapsed().as_secs_f64() * 1e6;
        let s = b.db.branch_stats();
        if s.live_branches != n || s.arena_slots_in_use != n {
            not_a_result(&format!("expected {n} branches and {n} arena pages before the block: {s:?}"));
        }
        b.print_state(n, &format!("grow_total_us={grow_us:.0}"));
        let order = args
            .threads
            .iter()
            .map(|&t| (t, 0u64))
            .chain(args.threads.iter().rev().map(|&t| (t, 1u64)));
        for (t, draw) in order {
            let null = null_ops_per_s(t);
            // Deal the live branches round-robin, so every share holds branches of every age.
            let mut shares: Vec<Vec<Live>> = (0..t).map(|_| Vec::with_capacity(n / t + 1)).collect();
            for (i, l) in live.drain(..).enumerate() {
                shares[i % t].push(l);
            }
            let barrier = Barrier::new(t + 1);
            println!("# cell N={n} T={t} draw={draw} start");
            let before = b.db.branch_stats().work;
            let cpu0 = cpu_ns();
            let (wall, outs) = std::thread::scope(|s| {
                let handles: Vec<_> = shares
                    .into_iter()
                    .enumerate()
                    .map(|(i, share)| {
                        let trunk = trunks[i].clone();
                        let barrier = &barrier;
                        let seed = args.seed
                            ^ (i as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)
                            ^ ((n as u64) << 24)
                            ^ (draw << 56)
                            ^ ((t as u64) << 48);
                        let base = grown + i * c;
                        s.spawn(move || conc_thread(trunk, share, barrier, seed, base, c))
                    })
                    .collect();
                barrier.wait();
                let start = Instant::now();
                let outs: Vec<ConcOut> = handles.into_iter().map(|h| h.join().unwrap()).collect();
                (start.elapsed(), outs)
            });
            let cpu1 = cpu_ns();
            let after = b.db.branch_stats().work;
            grown += t * c;
            let mut ops: [Vec<Duration>; 7] = Default::default();
            let mut busy = [0u64; 7];
            let (mut th_min, mut th_max) = (Duration::MAX, Duration::ZERO);
            for out in outs {
                live.extend(out.share);
                for (i, mut samples) in out.ops.into_iter().enumerate() {
                    ops[i].append(&mut samples);
                    busy[i] += out.busy[i];
                }
                th_min = th_min.min(out.elapsed);
                th_max = th_max.max(out.elapsed);
            }
            let s = b.db.branch_stats();
            if s.live_branches != n || s.arena_slots_in_use != n || live.len() != n {
                not_a_result(&format!(
                    "cell N={n} T={t} draw={draw} did not return to {n} branches and {n} arena pages \
                     (harness holds {}): {s:?}",
                    live.len()
                ));
            }
            for (i, samples) in ops.iter().enumerate() {
                let mut us: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
                us.sort_by(|a, b| a.partial_cmp(b).unwrap());
                println!(
                    "{n}\t{t}\t{draw}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}",
                    CONC_OPS[i],
                    us.len(),
                    percentile(&us, 50.0),
                    percentile(&us, 90.0),
                    percentile(&us, 99.0),
                    us[us.len() - 1],
                    busy[i]
                );
            }
            let cycles = (t * c) as f64;
            let wall_ns = wall.as_nanos() as f64;
            let d = |from: u64, to: u64| to - from;
            let acq = d(before.lock_acquisitions, after.lock_acquisitions);
            let contended = d(before.lock_contended, after.lock_contended);
            let wait_ns = d(before.lock_wait_ns, after.lock_wait_ns);
            let hold_ns = d(before.lock_hold_ns, after.lock_hold_ns);
            println!(
                "# cellsum N={n} T={t} draw={draw} cycles={} wall_ns={} cycles_per_s={:.1} \
                 null_ops_per_s={null:.0} thread_ms_min={:.3} thread_ms_max={:.3} user_ns={} sys_ns={} \
                 lock_acq={acq} lock_contended={contended} lock_wait_ns={wait_ns} lock_hold_ns={hold_ns} \
                 acq_per_cycle={:.3} contended_frac={:.4} wait_frac={:.4} hold_util={:.4} \
                 busy_fork={} busy_open={} busy_other={} rss_bytes={} resolves={} \
                 trunk_page_hits={} trunk_page_misses={} trunk_lock_acq={} trunk_lock_contended={} \
                 trunk_lock_wait_ns={} trunk_lock_hold_ns={}",
                t * c,
                wall.as_nanos(),
                cycles / wall.as_secs_f64(),
                th_min.as_secs_f64() * 1e3,
                th_max.as_secs_f64() * 1e3,
                cpu1.0 - cpu0.0,
                cpu1.1 - cpu0.1,
                acq as f64 / cycles,
                contended as f64 / acq as f64,
                wait_ns as f64 / (t as f64 * wall_ns),
                hold_ns as f64 / wall_ns,
                busy[0],
                busy[1],
                busy[2..].iter().sum::<u64>(),
                rss_bytes(),
                d(before.resolve_calls, after.resolve_calls),
                d(before.trunk_page_hits, after.trunk_page_hits),
                d(before.trunk_page_misses, after.trunk_page_misses),
                d(before.trunk_lock_acquisitions, after.trunk_lock_acquisitions),
                d(before.trunk_lock_contended, after.trunk_lock_contended),
                d(before.trunk_lock_wait_ns, after.trunk_lock_wait_ns),
                d(before.trunk_lock_hold_ns, after.trunk_lock_hold_ns),
            );
        }
    }
    drop(live);
    drop(trunks);
}

/// A live trunk child of `reapconc`. It never writes, and connects only for a read check.
struct Child {
    branch: Branch,
    /// Fork order over the whole run; the `oldest` victim policy keeps shares in this order.
    seq: u64,
    /// Trunk writes committed before its fork, or `None` for a child an `inline` thread forked,
    /// whose place among concurrent trunk writes the harness does not know (never read-checked).
    writes_at_fork: Option<u64>,
}

fn rc_pick(share: &mut VecDeque<Child>, victim: Victim, rng: &mut Rng) -> Child {
    match victim {
        Victim::Random => {
            let at = rng.below(share.len());
            share.swap_remove_back(at).unwrap()
        }
        Victim::Oldest => share.pop_front().unwrap(),
    }
}

/// Reap `child`, which nothing else reads through; returns the versions the reap freed.
fn rc_reap(child: Child) -> usize {
    let reaped = child
        .branch
        .reap()
        .unwrap_or_else(|e| not_a_result(&format!("reapconc reap failed: {e}")));
    if reaped.deferred {
        not_a_result(&format!("a reapconc reap was deferred: {reaped:?}"));
    }
    reaped.freed_pages
}

/// Fork one trunk child on the main trunk connection; after every k-th fork of the run, write the
/// `spread` walk's next row on the trunk.
fn rc_fork(b: &mut Bench, k: usize, forks: &mut u64, seq: &mut u64) -> Child {
    let branch = b
        .trunk
        .fork_branch()
        .unwrap_or_else(|e| not_a_result(&format!("reapconc fork failed: {e}")));
    let child = Child {
        branch,
        seq: *seq,
        writes_at_fork: Some(b.model.writes),
    };
    *seq += 1;
    *forks += 1;
    if k > 0 && *forks % k as u64 == 0 {
        b.trunk_write(spread_row(b.model.writes));
    }
    child
}

/// The engine's counters that `reapconc` reads, summed over the timed windows only.
#[derive(Default, Clone, Copy)]
struct RcWork {
    trunk_acq: u64,
    trunk_contended: u64,
    trunk_wait_ns: u64,
    trunk_hold_ns: u64,
    reap_acq: u64,
    reap_contended: u64,
    reap_wait_ns: u64,
    reap_hold_ns: u64,
    probe_ns: u64,
    walk_ns: u64,
    free_ns: u64,
    gc_examined: u64,
    gc_range: u64,
    lock_acq: u64,
    lock_contended: u64,
    lock_wait_ns: u64,
}

impl RcWork {
    fn add(&mut self, a: &BranchWork, b: &BranchWork) {
        self.trunk_acq += b.trunk_lock_acquisitions - a.trunk_lock_acquisitions;
        self.trunk_contended += b.trunk_lock_contended - a.trunk_lock_contended;
        self.trunk_wait_ns += b.trunk_lock_wait_ns - a.trunk_lock_wait_ns;
        self.trunk_hold_ns += b.trunk_lock_hold_ns - a.trunk_lock_hold_ns;
        self.reap_acq += b.reap_trunk_acquisitions - a.reap_trunk_acquisitions;
        self.reap_contended += b.reap_trunk_contended - a.reap_trunk_contended;
        self.reap_wait_ns += b.reap_trunk_wait_ns - a.reap_trunk_wait_ns;
        self.reap_hold_ns += b.reap_trunk_hold_ns - a.reap_trunk_hold_ns;
        self.probe_ns += b.gc_probe_ns - a.gc_probe_ns;
        self.walk_ns += b.gc_walk_ns - a.gc_walk_ns;
        self.free_ns += b.gc_free_ns - a.gc_free_ns;
        self.gc_examined += b.gc_examined - a.gc_examined;
        self.gc_range += b.gc_range_entries - a.gc_range_entries;
        self.lock_acq += b.lock_acquisitions - a.lock_acquisitions;
        self.lock_contended += b.lock_contended - a.lock_contended;
        self.lock_wait_ns += b.lock_wait_ns - a.lock_wait_ns;
    }
}

fn add_split(into: &mut SplitWalkCounts, c: SplitWalkCounts) {
    into.walks += c.walks;
    into.range_entries += c.range_entries;
    into.walk_ns += c.walk_ns;
    into.candidates += c.candidates;
    into.frees += c.frees;
    into.misses += c.misses;
}

/// One `reapconc` cell's totals over its timed windows.
#[derive(Default)]
struct RcCell {
    reaps: u64,
    rounds: u64,
    wall_ns: u128,
    /// Phase refill: per round, the latest reaper's end minus the earliest one's start.
    span_ns: u128,
    user_ns: u64,
    sys_ns: u64,
    freed: u64,
    forks: u64,
    writes: u64,
    busy_fork: u64,
    busy_write: u64,
    work: RcWork,
    split: SplitWalkCounts,
    /// Latencies: reap, then (inline) fork and trunk write.
    lat: [Vec<Duration>; 3],
}

const RC_HEADER: &str = "N\tT\tdraw\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us";

/// Audit the trunk's retained versions against its live children and the arena; returns V.
fn rc_audit(b: &Bench, n: usize, tag: &str) -> usize {
    let a = b.db.branch_audit_trunk();
    let s = b.db.branch_stats();
    println!(
        "# audit {tag} children={} versions={} by_born={} by_died={} leaked={} arena_in_use={} \
         live={} rss_bytes={}",
        a.children,
        a.versions,
        a.by_born,
        a.by_died,
        a.leaked,
        s.arena_slots_in_use,
        s.live_branches,
        rss_bytes()
    );
    if a.children != n
        || s.live_branches != n
        || a.by_born != a.versions
        || a.by_died != a.versions
        || s.arena_slots_in_use != a.versions
    {
        not_a_result(&format!(
            "audit {tag}: {a:?} against {n} live children and {} arena pages",
            s.arena_slots_in_use
        ));
    }
    if a.leaked > 0 {
        println!("# AUDIT-LEAKED {} at {tag}", a.leaked);
    }
    a.versions
}

/// Read two random rows through each of `want` random children whose fork the harness can place
/// among the trunk's writes, each on its own connection, against the harness's trunk model.
/// Returns (children checked, children eligible).
fn rc_read_checks(b: &mut Bench, live: &VecDeque<Child>, want: usize) -> (usize, usize) {
    let known: Vec<usize> = (0..live.len())
        .filter(|&i| live[i].writes_at_fork.is_some())
        .collect();
    if known.is_empty() {
        return (0, 0);
    }
    for _ in 0..want {
        let child = &live[known[b.rng.below(known.len())]];
        let at = child.writes_at_fork.unwrap();
        let conn = child
            .branch
            .connect()
            .unwrap_or_else(|e| not_a_result(&format!("reapconc read check: connect failed: {e}")));
        for _ in 0..2 {
            let row = b.rng.below(TRUNK_ROWS as usize) as i64 + 1;
            let rows = select_v(&conn, row).unwrap_or_else(|e| {
                not_a_result(&format!("reapconc read check of row {row}: {e}"))
            });
            check_v(&rows, row, &b.model.value_at(row, at));
        }
    }
    (want, known.len())
}

/// Grow to `n` live trunk children, then run `args.warmup` windows of `n` single-threaded churn
/// cycles (reap one, fork one, the trunk writing after every k-th fork), printing V per window.
fn rc_grow_and_warm(
    b: &mut Bench,
    args: &Args,
    live: &mut VecDeque<Child>,
    n: usize,
    forks: &mut u64,
    seq: &mut u64,
) {
    let k = args.write_every;
    let t0 = Instant::now();
    while live.len() < n {
        let child = rc_fork(b, k, forks, seq);
        live.push_back(child);
    }
    let s = b.db.branch_stats();
    println!(
        "# grow x={n} forks={forks} trunk_writes={} grow_us={:.0} V={} rss_bytes={}",
        b.model.writes,
        t0.elapsed().as_secs_f64() * 1e6,
        s.arena_slots_in_use,
        rss_bytes()
    );
    let mut vs = Vec::new();
    for w in 0..args.warmup {
        let t0 = Instant::now();
        let mut freed = 0usize;
        for _ in 0..n {
            let victim = rc_pick(live, args.victim, &mut b.rng);
            freed += rc_reap(victim);
            let child = rc_fork(b, k, forks, seq);
            live.push_back(child);
        }
        let s = b.db.branch_stats();
        vs.push(s.arena_slots_in_use);
        println!(
            "# warmup x={n} window={w} cycles={n} V={} freed={freed} us={:.0} trunk_writes={} \
             rss_bytes={}",
            s.arena_slots_in_use,
            t0.elapsed().as_secs_f64() * 1e6,
            b.model.writes,
            rss_bytes()
        );
    }
    if vs.len() >= 2 {
        let (prev, last) = (vs[vs.len() - 2], vs[vs.len() - 1]);
        let ratio = if prev == 0 { f64::from(u8::from(last == 0)) } else { last as f64 / prev as f64 };
        println!(
            "# steady x={n} V_prev={prev} V_last={last} ratio={ratio:.4} {}",
            if (ratio - 1.0).abs() <= 0.03 { "STEADY" } else { "NOT-STEADY" }
        );
    }
    rc_audit(b, n, &format!("warmup x={n}"));
}

/// A `phase` cell: rounds of (the T threads reap K = round_reaps / T random children of their own
/// share, concurrently, timed; then the main thread alone re-forks K·T children and deals them back).
/// The reap phase is the only thing running while it is timed, so the trunk's lock is taken by
/// reaps alone; the engine's counters are read just outside it.
fn rc_cell_phase(
    b: &mut Bench,
    args: &Args,
    live: &mut VecDeque<Child>,
    n: usize,
    t: usize,
    draw: u64,
    forks: &mut u64,
    seq: &mut u64,
) -> RcCell {
    let k_each = args.round_reaps / t;
    let rounds = args.cell_reaps / (k_each * t);
    let mut dealt: Vec<VecDeque<Child>> = (0..t).map(|_| VecDeque::with_capacity(n / t + 1)).collect();
    for (i, child) in live.drain(..).enumerate() {
        dealt[i % t].push_back(child);
    }
    let shares: Vec<std::sync::Mutex<VecDeque<Child>>> =
        dealt.into_iter().map(std::sync::Mutex::new).collect();
    let (start, end) = (Barrier::new(t + 1), Barrier::new(t + 1));
    let freed_round = std::sync::atomic::AtomicUsize::new(0);
    let victim = args.victim;
    let mut cell = RcCell::default();
    let outs = std::thread::scope(|s| {
        let handles: Vec<_> = (0..t)
            .map(|i| {
                let (shares, start, end, freed_round) = (&shares, &start, &end, &freed_round);
                let seed = args.seed
                    ^ (i as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)
                    ^ ((n as u64) << 24)
                    ^ (draw << 56)
                    ^ ((t as u64) << 48);
                s.spawn(move || {
                    let mut rng = Rng(seed | 1);
                    let mut lat = Vec::with_capacity(k_each * rounds);
                    let mut spans = Vec::with_capacity(rounds);
                    let mut split = SplitWalkCounts::default();
                    for _ in 0..rounds {
                        start.wait();
                        let t0 = Instant::now();
                        let mut share = shares[i].lock().unwrap();
                        let mut freed = 0;
                        for _ in 0..k_each {
                            let child = rc_pick(&mut share, victim, &mut rng);
                            let a = Instant::now();
                            freed += rc_reap(child);
                            lat.push(a.elapsed());
                        }
                        drop(share);
                        let t1 = Instant::now();
                        freed_round.fetch_add(freed, std::sync::atomic::Ordering::Relaxed);
                        spans.push((t0, t1));
                        add_split(&mut split, take_split_walk_counts());
                        end.wait();
                    }
                    (lat, spans, split)
                })
            })
            .collect();
        for _ in 0..rounds {
            if args.unsafe_split {
                b.db.set_branch_unsafe_split_walk(true);
            }
            freed_round.store(0, std::sync::atomic::Ordering::Relaxed);
            let before = b.db.branch_stats();
            let cpu0 = cpu_ns();
            start.wait();
            let w0 = Instant::now();
            end.wait();
            let wall = w0.elapsed();
            let cpu1 = cpu_ns();
            let after = b.db.branch_stats();
            if args.unsafe_split {
                b.db.set_branch_unsafe_split_walk(false);
            }
            let freed = freed_round.load(std::sync::atomic::Ordering::Relaxed);
            // PREREG P2: nothing is retained inside a phase, so the arena shrinks by exactly what the
            // reaps reported, and the engine released exactly that many versions.
            if before.arena_slots_in_use.checked_sub(after.arena_slots_in_use) != Some(freed)
                || (after.work.gc_examined - before.work.gc_examined) as usize != freed
            {
                not_a_result(&format!(
                    "reap phase N={n} T={t} draw={draw}: the reaps reported {freed} freed; the arena \
                     went {} -> {} and gc_examined +{}",
                    before.arena_slots_in_use,
                    after.arena_slots_in_use,
                    after.work.gc_examined - before.work.gc_examined
                ));
            }
            cell.work.add(&before.work, &after.work);
            cell.reaps += (k_each * t) as u64;
            cell.rounds += 1;
            cell.wall_ns += wall.as_nanos();
            cell.freed += freed as u64;
            cell.user_ns += cpu1.0 - cpu0.0;
            cell.sys_ns += cpu1.1 - cpu0.1;
            let writes0 = b.model.writes;
            for j in 0..k_each * t {
                let child = rc_fork(b, args.write_every, forks, seq);
                shares[j % t].lock().unwrap().push_back(child);
            }
            cell.forks += (k_each * t) as u64;
            cell.writes += b.model.writes - writes0;
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut spans: Vec<Vec<(Instant, Instant)>> = Vec::new();
    for (mut lat, s, split) in outs {
        cell.lat[0].append(&mut lat);
        spans.push(s);
        add_split(&mut cell.split, split);
    }
    for r in 0..rounds {
        let first = spans.iter().map(|s| s[r].0).min().unwrap();
        let last = spans.iter().map(|s| s[r].1).max().unwrap();
        cell.span_ns += (last - first).as_nanos();
    }
    for share in shares {
        live.extend(share.into_inner().unwrap());
    }
    live.make_contiguous().sort_unstable_by_key(|c| c.seq);
    cell
}

/// An `inline` cell: each of the T threads runs cell_reaps / T cycles of (reap a random child of its
/// own share; re-fork a replacement through its own trunk connection; after every k-th own cycle
/// write the spread walk's next row through it). `Busy` answers are retried and counted.
#[allow(clippy::too_many_arguments)]
fn rc_cell_inline(
    b: &mut Bench,
    args: &Args,
    trunks: &[Arc<Connection>],
    live: &mut VecDeque<Child>,
    n: usize,
    t: usize,
    draw: u64,
    seq: &mut u64,
) -> RcCell {
    let c_each = args.cell_reaps / t;
    let k = args.write_every;
    let mut shares: Vec<VecDeque<Child>> = (0..t).map(|_| VecDeque::with_capacity(n / t + 1)).collect();
    for (i, child) in live.drain(..).enumerate() {
        shares[i % t].push_back(child);
    }
    let g = std::sync::atomic::AtomicU64::new(b.model.writes);
    let barrier = Barrier::new(t + 1);
    let seq0 = *seq;
    let victim = args.victim;
    let before = b.db.branch_stats();
    let cpu0 = cpu_ns();
    let (wall, outs) = std::thread::scope(|s| {
        let handles: Vec<_> = shares
            .into_iter()
            .enumerate()
            .map(|(i, mut share)| {
                let trunk = trunks[i].clone();
                let (g, barrier) = (&g, &barrier);
                let seed = args.seed
                    ^ (i as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)
                    ^ ((n as u64) << 24)
                    ^ (draw << 56)
                    ^ ((t as u64) << 48);
                s.spawn(move || {
                    let mut rng = Rng(seed | 1);
                    let mut lat: [Vec<Duration>; 3] = [
                        Vec::with_capacity(c_each),
                        Vec::with_capacity(c_each),
                        Vec::with_capacity(if k > 0 { c_each / k + 1 } else { 0 }),
                    ];
                    let mut busy = [0u64; 2];
                    let (mut freed, mut writes) = (0usize, 0u64);
                    barrier.wait();
                    for c in 0..c_each {
                        let child = rc_pick(&mut share, victim, &mut rng);
                        let a = Instant::now();
                        freed += rc_reap(child);
                        lat[0].push(a.elapsed());
                        let a = Instant::now();
                        let branch = busy_retry(&mut busy[0], "fork", || trunk.fork_branch());
                        lat[1].push(a.elapsed());
                        share.push_back(Child {
                            branch,
                            seq: seq0 + (c * t + i) as u64,
                            writes_at_fork: None,
                        });
                        if k > 0 && (c + 1) % k == 0 {
                            let gen = g.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let sql = format!(
                                "UPDATE t SET v = '{}' WHERE id = {}",
                                trunk_gen_value(gen),
                                spread_row(gen)
                            );
                            let a = Instant::now();
                            busy_retry(&mut busy[1], "trunk_write", || trunk.execute(sql.as_str()));
                            lat[2].push(a.elapsed());
                            writes += 1;
                        }
                    }
                    (share, lat, busy, freed, writes)
                })
            })
            .collect();
        barrier.wait();
        let w0 = Instant::now();
        let outs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        (w0.elapsed(), outs)
    });
    let cpu1 = cpu_ns();
    let after = b.db.branch_stats();
    let mut cell = RcCell::default();
    for (share, mut lat, busy, freed, writes) in outs {
        live.extend(share);
        for (all, l) in cell.lat.iter_mut().zip(lat.iter_mut()) {
            all.append(l);
        }
        cell.busy_fork += busy[0];
        cell.busy_write += busy[1];
        cell.freed += freed as u64;
        cell.writes += writes;
    }
    live.make_contiguous().sort_unstable_by_key(|c| c.seq);
    *seq = seq0 + (c_each * t) as u64;
    // Every write the threads made has committed, each to a distinct row (`spread_row` repeats only
    // every 20,000 writes), so the model catches up in generation order.
    let g_end = g.load(std::sync::atomic::Ordering::Relaxed);
    if g_end - b.model.writes != cell.writes {
        not_a_result("inline trunk writes and the shared generation counter disagree");
    }
    while b.model.writes < g_end {
        let row = spread_row(b.model.writes);
        b.model.record(row);
    }
    cell.work.add(&before.work, &after.work);
    cell.reaps = (c_each * t) as u64;
    cell.rounds = 1;
    cell.forks = cell.reaps;
    cell.wall_ns = wall.as_nanos();
    cell.span_ns = wall.as_nanos();
    cell.user_ns = cpu1.0 - cpu0.0;
    cell.sys_ns = cpu1.1 - cpu0.1;
    // Versions retained inside the cell: what the arena gained plus what the reaps freed. Each
    // trunk write retains at most one (its leaf).
    let retained = (after.arena_slots_in_use + cell.freed as usize) as i64
        - before.arena_slots_in_use as i64;
    if retained < 0 || retained as u64 > cell.writes {
        not_a_result(&format!(
            "inline cell N={n} T={t}: {retained} versions retained by {} trunk writes",
            cell.writes
        ));
    }
    cell
}

fn rc_print_lat(n: usize, t: usize, draw: u64, op: &str, lat: &[Duration]) {
    if lat.is_empty() {
        return;
    }
    let mut us: Vec<f64> = lat.iter().map(|d| d.as_secs_f64() * 1e6).collect();
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{n}\t{t}\t{draw}\t{op}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
        us.len(),
        percentile(&us, 50.0),
        percentile(&us, 90.0),
        percentile(&us, 99.0),
        us[us.len() - 1]
    );
}

/// Arm `reapconc` (frontier/round11/r11-k6-measure/PREREG.md §0): does the serialized reap of trunk
/// children bind at T > 1? Children never write; the trunk writes the `spread` walk after every k-th
/// fork, so the trunk retains versions (V > 0) unless k = 0. At each N: grow, warm up to steady
/// state, then one cell per (T, draw), T forward and then reversed. After each cell: the trunk audit
/// (no leaked version), and read checks against the harness's trunk model.
fn arm_reapconc(b: &mut Bench, args: &Args) {
    b.db.set_branch_lock_timing(args.lock_timing);
    b.db.set_branch_trunk_spin(args.trunk_spin);
    b.db.set_branch_unsafe_k6_race(args.unsafe_k6_race);
    let tmax = *args.threads.iter().max().unwrap();
    let trunks: Vec<Arc<Connection>> = if args.refill == Refill::Inline {
        (0..tmax)
            .map(|_| {
                let conn = b.db.connect().unwrap();
                conn.execute(format!("PRAGMA synchronous = {}", args.synchronous))
                    .unwrap();
                conn
            })
            .collect()
    } else {
        Vec::new()
    };
    println!(
        "# reapconc: refill={:?} victim={:?} write_every={} unsafe_split_walk={} unsafe_k6_race={} \
         trunk_spin={} threads={:?} (forward, then reversed) warmup_windows={} cell_reaps={} \
         round_reaps={} read_checks={} lock_timing={} synchronous={}",
        args.refill,
        args.victim,
        args.write_every,
        args.unsafe_split,
        args.unsafe_k6_race,
        args.trunk_spin,
        args.threads,
        args.warmup,
        args.cell_reaps,
        args.round_reaps,
        args.read_checks,
        args.lock_timing,
        args.synchronous
    );
    if args.unsafe_split {
        println!(
            "# UNSAFE: the reap's F2 walk runs outside the trunk lock, over an index snapshot taken \
             at each reap phase's start; read ONLY for its throughput ceiling"
        );
    }
    if args.unsafe_k6_race {
        println!(
            "# UNSAFE: K6's race is on: each reap probes its neighbours and detaches in two critical \
             sections; read ONLY for the leak the audit counts"
        );
    }
    println!("{RC_HEADER}");
    let mut live: VecDeque<Child> = VecDeque::new();
    let (mut forks, mut seq) = (0u64, 0u64);
    for &n in &args.checkpoints {
        rc_grow_and_warm(b, args, &mut live, n, &mut forks, &mut seq);
        let order: Vec<(usize, u64)> = args
            .threads
            .iter()
            .map(|&t| (t, 0u64))
            .chain(args.threads.iter().rev().map(|&t| (t, 1u64)))
            .collect();
        for (t, draw) in order {
            let null = null_ops_per_s(t);
            let v0 = b.db.branch_stats().arena_slots_in_use;
            println!("# rcell N={n} T={t} draw={draw} start");
            let cell = match args.refill {
                Refill::Phase => {
                    rc_cell_phase(b, args, &mut live, n, t, draw, &mut forks, &mut seq)
                }
                Refill::Inline => {
                    let cell = rc_cell_inline(b, args, &trunks, &mut live, n, t, draw, &mut seq);
                    forks += cell.forks;
                    cell
                }
            };
            let v1 = b.db.branch_stats().arena_slots_in_use;
            let (w, sp) = (&cell.work, &cell.split);
            println!(
                "# rcell N={n} T={t} draw={draw} refill={:?} victim={:?} k={} split={} reaps={} \
                 rounds={} wall_ns={} span_ns={} reaps_per_s={:.1} reaps_per_s_span={:.1} \
                 null_ops_per_s={null:.0} user_ns={} sys_ns={} V_start={v0} V_end={v1} freed={} \
                 trunk_acq={} trunk_contended={} trunk_wait_ns={} trunk_hold_ns={} reap_acq={} \
                 reap_contended={} reap_wait_ns={} reap_hold_ns={} gc_probe_ns={} gc_walk_ns={} \
                 gc_free_ns={} gc_examined={} gc_range={} lock_acq={} lock_contended={} \
                 lock_wait_ns={} split_walks={} split_range={} split_walk_ns={} split_candidates={} \
                 split_frees={} split_misses={} forks={} trunk_writes={} busy_fork={} busy_write={} \
                 rss_bytes={}",
                args.refill,
                args.victim,
                args.write_every,
                u8::from(args.unsafe_split),
                cell.reaps,
                cell.rounds,
                cell.wall_ns,
                cell.span_ns,
                cell.reaps as f64 / (cell.wall_ns as f64 / 1e9),
                cell.reaps as f64 / (cell.span_ns as f64 / 1e9),
                cell.user_ns,
                cell.sys_ns,
                cell.freed,
                w.trunk_acq,
                w.trunk_contended,
                w.trunk_wait_ns,
                w.trunk_hold_ns,
                w.reap_acq,
                w.reap_contended,
                w.reap_wait_ns,
                w.reap_hold_ns,
                w.probe_ns,
                w.walk_ns,
                w.free_ns,
                w.gc_examined,
                w.gc_range,
                w.lock_acq,
                w.lock_contended,
                w.lock_wait_ns,
                sp.walks,
                sp.range_entries,
                sp.walk_ns,
                sp.candidates,
                sp.frees,
                sp.misses,
                cell.forks,
                cell.writes,
                cell.busy_fork,
                cell.busy_write,
                rss_bytes()
            );
            let r = cell.reaps as f64;
            let wall = cell.wall_ns as f64;
            println!(
                "# derived N={n} T={t} draw={draw} U_reap={:.4} U_trunk={:.4} hold_per_reap_ns={:.1} \
                 probe_per_reap_ns={:.1} walk_per_reap_ns={:.1} free_per_reap_ns={:.1} \
                 rest_per_reap_ns={:.1} wait_per_reap_ns={:.1} contended_frac={:.4} \
                 freed_per_reap={:.4} range_per_reap={:.4} split_walk_per_reap_ns={:.1}",
                w.reap_hold_ns as f64 / wall,
                w.trunk_hold_ns as f64 / wall,
                w.reap_hold_ns as f64 / r,
                w.probe_ns as f64 / r,
                w.walk_ns as f64 / r,
                w.free_ns as f64 / r,
                (w.reap_hold_ns as f64 - (w.probe_ns + w.walk_ns + w.free_ns) as f64) / r,
                w.reap_wait_ns as f64 / r,
                w.reap_contended as f64 / r,
                cell.freed as f64 / r,
                (w.gc_range + sp.range_entries) as f64 / r,
                sp.walk_ns as f64 / r,
            );
            // PREREG P1: the lock acquisitions the source predicts, per timed window.
            let (reaps, rounds) = (cell.reaps, cell.rounds);
            let (want_reap, want_trunk, want_all) = match args.refill {
                Refill::Phase => (
                    reaps + sp.frees,
                    reaps + sp.frees + rounds,
                    2 * reaps + sp.frees + 65 * rounds,
                ),
                // A trunk fork takes the trunk's lock once, a spread write's copy decision once.
                Refill::Inline => (reaps, reaps + cell.forks + cell.writes + 1, 0),
            };
            let all_ok = args.refill == Refill::Inline || w.lock_acq == want_all;
            println!(
                "# P1 N={n} T={t} draw={draw} reap_acq={} want={want_reap} trunk_acq={} want={want_trunk} \
                 lock_acq={} want={} {}",
                w.reap_acq,
                w.trunk_acq,
                w.lock_acq,
                if args.refill == Refill::Phase { want_all.to_string() } else { "n/a".to_string() },
                if w.reap_acq == want_reap && w.trunk_acq == want_trunk && all_ok {
                    "HOLDS"
                } else {
                    "BROKEN"
                }
            );
            rc_print_lat(n, t, draw, "reap", &cell.lat[0]);
            rc_print_lat(n, t, draw, "fork", &cell.lat[1]);
            rc_print_lat(n, t, draw, "trunk_write", &cell.lat[2]);
            rc_audit(b, n, &format!("cell N={n} T={t} draw={draw}"));
            let (checked, eligible) = rc_read_checks(b, &live, args.read_checks);
            println!("# reads N={n} T={t} draw={draw} checked={checked} eligible={eligible}");
        }
    }
    drop(live);
    drop(trunks);
}
