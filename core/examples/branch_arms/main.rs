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
//!   grow       the base curve's growth (fork + open + one-row write per branch) with EVERY fork timed,
//!              to see one-off stalls that checkpoint sampling cannot (amendment 7)
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
//! `--victim newest` (churn arms only) reaps the branch forked one cycle ago, hot in cache: the
//! turso_curve lane's newest-victim churn locality check, its amendment 3 (`48a2b97a3`, whose
//! `--victim random|newest` flag this merges into the policy above).
//!
//! `--durability volatile|durable|durable-nosync` (default volatile) opens the database with that branch
//! durability. This copy runs on the DURABLE store's line (turso `ec168128b` + the sota-durable port; round 11
//! PREREG D3); it is round 10's harness (`b9230a8bb`) with that flag and the durable store's `Result`-returning
//! `branch_stats` as the only differences.
//!
//! `--splice on|off` (default off) opens the store in the F7 splice arm or not (r11-ever amendment 15:
//! a released branch left with one live child is spliced into it). Only the chain arm's cascade
//! releases an interior branch, and it checks the rule of the arm it runs in; the header names it.
//!
//! Every read a sample makes is checked against a model the harness keeps itself (never against
//! the engine), and the engine's own counts are checked against the workload before a number is
//! printed; a mismatch prints `NOT A RESULT` and exits 1. Beside each latency the harness prints
//! the engine's work counters for that op (resolutions, nodes walked, retained versions compared),
//! so a slope can be read against an integer that load cannot move.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::{Branch, BranchDurability, BranchWork};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
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
    Grow,
}

/// Which live branch a churn cycle reaps.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Victim {
    Random,
    Oldest,
    /// The branch forked one cycle ago (turso_curve amendment 3, `48a2b97a3`).
    Newest,
}

struct Args {
    arm: Arm,
    victim: Victim,
    durability: BranchDurability,
    splice: bool,
    checkpoints: Vec<usize>,
    samples: usize,
    seed: u64,
    cycles: usize,
    windows: usize,
    w_list: Vec<usize>,
    no_autocheckpoint: bool,
    synchronous: String,
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
        durability: BranchDurability::Volatile,
        splice: false,
        checkpoints: vec![100, 1000],
        samples: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        cycles: 10_000,
        windows: 10,
        w_list: vec![1],
        no_autocheckpoint: false,
        synchronous: "OFF".to_string(),
    };
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
                    "grow" => Arm::Grow,
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
            "--durability" => {
                args.durability = match val().as_str() {
                    "volatile" => BranchDurability::Volatile,
                    "durable" => BranchDurability::Durable { sync: turso_core::branch::SyncClass::Fsync },
                    "durable-nosync" => BranchDurability::Durable { sync: turso_core::branch::SyncClass::Off },
                    other => die(&format!("unknown --durability {other}")),
                }
            }
            "--splice" => {
                args.splice = match val().as_str() {
                    "on" => true,
                    "off" => false,
                    other => die(&format!("unknown --splice {other}")),
                }
            }
            "--victim" => {
                args.victim = match val().as_str() {
                    "random" => Victim::Random,
                    "oldest" => Victim::Oldest,
                    "newest" => Victim::Newest,
                    other => die(&format!("unknown victim policy {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.arm = arm.unwrap_or_else(|| die("--arm is required"));
    if args.victim != Victim::Random
        && !matches!(args.arm, Arm::Churn | Arm::ChurnHot | Arm::ChurnSpread)
    {
        die("--victim applies to the churn arms only");
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
        self.db.branch_stats().unwrap().work
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
        let s = self.db.branch_stats().unwrap();
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
        DatabaseOpts::new()
            .with_branch_durability(args.durability)
            .with_branch_splice(args.splice),
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
        "# arm={:?} victim={:?} durability={:?} splice={} checkpoints={:?} samples={} seed={:#x} cycles={} windows={} w={:?} \
         trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} page_size={page_size} \
         trunk_pages={trunk_pages} trunk_synchronous={synchronous} no_autocheckpoint={}",
        args.arm,
        args.victim,
        args.durability,
        if args.splice { "on" } else { "off" },
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
        Arm::Grow => arm_grow(&mut bench, &args),
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
        let s = b.db.branch_stats().unwrap();
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
        let s = b.db.branch_stats().unwrap();
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
        let s = b.db.branch_stats().unwrap();
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
        let s = b.db.branch_stats().unwrap();
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
    // The cascade: release every ancestor handle, root first, then reap the tip, which frees what
    // is left of the chain in one call. One sample, labelled as such. Off the splice arm (the base's
    // rule) each ancestor is kept for its live child (`deferred`, 0 pages) and the tip's reap frees
    // the chain's d pages. In the F7 splice arm (r11-ever-durable-cat, `--splice on`) each released
    // root has one live child and is spliced into it (`deferred`), and a page of it that the child
    // wrote too is freed at that splice (the child's own copy shadows it and the child's children
    // forked after its write): at most one page per ancestor, and the chain's d pages in all.
    let d = chain.len();
    let tip = chain.pop().unwrap();
    let most_at_release = usize::from(args.splice);
    let mut freed_early = 0;
    for l in chain {
        let r = l.branch.reap().unwrap();
        if !r.deferred || r.freed_pages > most_at_release {
            not_a_result(&format!("an ancestor with a live child was freed: {r:?}"));
        }
        freed_early += r.freed_pages;
    }
    let before = b.work();
    let t = Instant::now();
    let r = tip.branch.reap().unwrap();
    let us = t.elapsed().as_secs_f64() * 1e6;
    let after = b.work();
    if r.deferred || freed_early + r.freed_pages != d {
        not_a_result(&format!(
            "the cascade freed {r:?} after {freed_early} pages at the splices, expected {d} in all"
        ));
    }
    println!(
        "# cascade_reap d={d} us={us:.2} freed_pages={} freed_at_splices={freed_early} gc_range_entries={} (ONE sample)",
        r.freed_pages,
        after.gc_range_entries - before.gc_range_entries
    );
}

/// Arms (c) `churn`, `churn_hot` and `churn_spread`: steady N. Each cycle forks and writes one
/// branch (plus one trunk write in `churn_hot` and `churn_spread`) and reaps one older live branch:
/// a uniformly random one, or with `--victim oldest` the oldest (amendment 3), or with `--victim
/// newest` the one forked last cycle (turso_curve amendment 3).
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
        let s = b.db.branch_stats().unwrap();
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
                    // Before this cycle's branch is pushed, the back is the one forked last cycle
                    // (`48a2b97a3`'s `live.pop()`; no policy but random reorders `live`).
                    Victim::Newest => live.pop_back().unwrap(),
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
            let s = b.db.branch_stats().unwrap();
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
        let s = b.db.branch_stats().unwrap();
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
            let s = b.db.branch_stats().unwrap();
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
            let s = b.db.branch_stats().unwrap();
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
        let s = b.db.branch_stats().unwrap();
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
        if b.db.branch_stats().unwrap().live_branches != 0 {
            not_a_result("a branch exists in the no-branch arm");
        }
        b.print_op(n, "trunk_write", &tw);
        b.print_state(n, "");
    }
    b.print_slopes(&args.checkpoints, &["trunk_write"]);
}

/// Amendment 7: the base curve's growth — each branch forked from the trunk, opened, and given one
/// row — with EVERY fork timed, not K samples at a checkpoint. A stall that one fork in ~2^k pays
/// (the store's branch table doubling under its mutex) is invisible to p50 and to K samples taken
/// between doublings; here each window (previous checkpoint, this one] reports its forks' p50, p99
/// and max, and every fork at or above `STALL_US` is listed with the branch count it forked at.
fn arm_grow(b: &mut Bench, args: &Args) {
    const STALL_US: f64 = 100.0;
    println!("{HEADER}");
    let mut live: Vec<Live> = Vec::new();
    let mut stalls: Vec<(usize, f64)> = Vec::new();
    for &n in &args.checkpoints {
        let mut fork = Op::default();
        let (mut max_us, mut max_at) = (0.0f64, 0usize);
        while live.len() < n {
            let at = live.len();
            let row = row_for(at);
            let branch = b.timed(&mut fork, || b.trunk.fork_branch().unwrap());
            let us = fork.samples.last().expect("just timed").as_secs_f64() * 1e6;
            if us > max_us {
                (max_us, max_at) = (us, at);
            }
            if us >= STALL_US {
                stalls.push((at, us));
            }
            let conn = branch.connect().unwrap();
            update(&conn, row);
            drop(conn);
            live.push(Live {
                branch,
                rows: vec![row],
                trunk_writes_at_fork: 0,
            });
        }
        let s = b.db.branch_stats().unwrap();
        if s.live_branches != n || s.arena_slots_in_use != n {
            not_a_result(&format!("expected {n} branches and {n} arena pages: {s:?}"));
        }
        let probe = &live[b.rng.below(live.len())];
        let conn = probe.branch.connect().unwrap();
        if read_v(&conn, probe.rows[0]) != branch_value(probe.rows[0]) {
            not_a_result("a grown branch did not read its own write");
        }
        drop(conn);
        b.print_op(n, "grow_fork", &fork);
        b.print_state(
            n,
            &format!(
                "window_forks={} window_max_us={max_us:.1} window_max_at_n={max_at}",
                fork.samples.len()
            ),
        );
    }
    b.print_slopes(&args.checkpoints, &["grow_fork"]);
    println!("# stalls >= {STALL_US} us: {} forks", stalls.len());
    for (at, us) in &stalls {
        println!("# stall\tn={at}\tus={us:.1}");
    }
    drop(live);
}
