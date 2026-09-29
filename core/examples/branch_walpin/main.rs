//! The trunk WAL pinned by branch readers: frontier/round11/r11-walpin/PREREG.md (artie-research).
//!
//!   cargo run -p turso_core --release --example branch_walpin -- --arm <arm> [options]
//!
//! Arms (the PREREG is the specification; this is its implementation):
//!
//!   pin       one branch holds `BEGIN; SELECT` open while the trunk makes H one-row autocommits
//!   nopin     the same, with the branch's SELECT autocommitted (control)
//!   overlap   K branch sessions; after each trunk commit one of them ends its read tx and begins a
//!             new one, so K read transactions are always open, each spanning K trunk commits
//!   u3        `overlap 1` in the background; the trunk rewrites one row k times, then 3,000 rows on
//!             other pages; then fresh branches read that row (the per-miss frame-list walk)
//!   conc      T trunk writer threads and R reader threads over K branch sessions (r11-walpin-conc;
//!             see conc.rs and that lane's PREREG)
//!
//! `--fix fw1,fw2,fw3` selects the lane's fixes (`turso_core::branch::walpin`); default none.
//!
//! Every branch read is checked against a model the harness keeps itself (never the engine); a
//! mismatch prints `NOT A RESULT` and exits 1. Counters are engine integers; times (`--timing`
//! only) are Instant around one trunk commit or one branch SELECT.

mod conc;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::walpin::{self, WalPinCounters, WalPinStats};
use turso_core::branch::Branch;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
/// `u3`: the hot row (the first leaf).
const HOT_ROW: i64 = 1;
/// `u3` phase B: trunk commits on rows 10,001..20,000 only.
const U3_PHASE_B: u64 = 3_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Pin,
    NoPin,
    Overlap,
    U3,
    Conc,
}

struct Args {
    arm: Arm,
    points: Vec<u64>,
    k: u64,
    samples: usize,
    fixes: (bool, bool, bool),
    /// r11-walpin-conc amendment 5: SQLite's restart rule (`--fix sqlrestart`).
    sqlrestart: bool,
    /// r11-walpin-conc amendment 26: the birth gate (`--fix fwb`).
    fwb: bool,
    timing: bool,
    dir: Option<PathBuf>,
    conc: conc::ConcArgs,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_walpin: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn default_points(max: u64) -> Vec<u64> {
    let mut v = Vec::new();
    let mut d = 1_000u64;
    while d <= max {
        for m in [1, 2, 5] {
            if d * m <= max {
                v.push(d * m);
            }
        }
        d *= 10;
    }
    v
}

fn parse_args() -> Args {
    let mut arm = None;
    let mut h_max = 100_000u64;
    let mut points = None;
    let mut args = Args {
        arm: Arm::Pin,
        points: Vec::new(),
        k: 1,
        samples: 200,
        fixes: (false, false, false),
        sqlrestart: false,
        fwb: false,
        timing: false,
        dir: None,
        conc: conc::ConcArgs::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--arm" => {
                arm = Some(match val().as_str() {
                    "pin" => Arm::Pin,
                    "nopin" => Arm::NoPin,
                    "overlap" => Arm::Overlap,
                    "u3" => Arm::U3,
                    "conc" => Arm::Conc,
                    other => die(&format!("unknown arm {other}")),
                })
            }
            "--h" => h_max = val().parse().unwrap_or_else(|_| die("bad --h")),
            "--points" => {
                points = Some(
                    val()
                        .split(',')
                        .map(|x| x.parse().unwrap_or_else(|_| die("bad --points")))
                        .collect::<Vec<u64>>(),
                )
            }
            "--k" => args.k = val().parse().unwrap_or_else(|_| die("bad --k")),
            "--samples" => args.samples = val().parse().unwrap_or_else(|_| die("bad --samples")),
            "--fix" => {
                let v = val();
                for f in v.split(',') {
                    match f {
                        "none" => {}
                        "fw1" => args.fixes.0 = true,
                        "fw2" => args.fixes.1 = true,
                        "fw3" => args.fixes.2 = true,
                        "sqlrestart" => args.sqlrestart = true,
                        "fwb" => args.fwb = true,
                        other => die(&format!("unknown fix {other}")),
                    }
                }
            }
            "--timing" => args.timing = true,
            "--t" => args.conc.t = val().parse().unwrap_or_else(|_| die("bad --t")),
            "--r" => args.conc.r = val().parse().unwrap_or_else(|_| die("bad --r")),
            "--m" => args.conc.m = val().parse().unwrap_or_else(|_| die("bad --m")),
            "--conn" => {
                args.conc.held = match val().as_str() {
                    "mux" => false,
                    "held" => true,
                    other => die(&format!("unknown --conn {other}")),
                }
            }
            "--rows" => {
                let v = val();
                args.conc.hot_leaves = match v.as_str() {
                    "all" => None,
                    _ => Some(
                        v.strip_prefix("hot:")
                            .and_then(|p| p.parse().ok())
                            .filter(|&p: &u64| p > 0)
                            .unwrap_or_else(|| die("--rows is all or hot:<leaves>")),
                    ),
                }
            }
            "--trunk-op" => {
                args.conc.trunk_op = Some(match val().as_str() {
                    "update" => false,
                    "insert" => true,
                    other => die(&format!("unknown --trunk-op {other}")),
                })
            }
            "--storm" => {
                args.conc.storm = match val().as_str() {
                    "none" => false,
                    "truncate" => true,
                    other => die(&format!("unknown --storm {other}")),
                }
            }
            "--wbusy" => {
                args.conc.wbusy_timeout = match val().as_str() {
                    "spin" => false,
                    "timeout" => true,
                    other => die(&format!("unknown --wbusy {other}")),
                }
            }
            "--dir" => args.dir = Some(PathBuf::from(val())),
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.arm = arm.unwrap_or_else(|| die("--arm is required"));
    args.points = points.unwrap_or_else(|| default_points(h_max));
    if args.points.is_empty() || args.points.windows(2).any(|w| w[0] >= w[1]) {
        die("--points must be non-empty and strictly increasing");
    }
    if args.k == 0 || args.samples == 0 {
        die("--k and --samples must be positive");
    }
    args
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

/// The trunk's `generation`-th rewrite of a row: same length, so it rewrites the row in place.
fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

/// branch_arms' walk: one leaf further on each time (a leaf holds ~37 rows); 37 is coprime with
/// 20,000, so every row is visited.
fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

/// `u3` phase B: the same walk over rows 10,001..20,000 only (never the hot row's leaf).
fn half_row(j: u64) -> i64 {
    ((j * 37) % (TRUNK_ROWS / 2) as u64) as i64 + TRUNK_ROWS / 2 + 1
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

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

/// The trunk's write history, kept by the harness: what a branch forked after trunk write number
/// `seq` must read for each row. Independent of the engine by construction.
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    /// row -> [write seq], ascending; write seq g wrote `trunk_gen_value(g)`.
    history: HashMap<i64, Vec<u64>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push(g);
        g
    }

    /// The value of `row` for a branch forked when `writes_at_fork` trunk writes had committed.
    fn value_at(&self, row: i64, writes_at_fork: u64) -> String {
        let Some(h) = self.history.get(&row) else {
            return trunk_value(row);
        };
        let n = h.partition_point(|&seq| seq < writes_at_fork);
        if n == 0 {
            trunk_value(row)
        } else {
            trunk_gen_value(h[n - 1])
        }
    }
}

/// A branch session: its handle, its connection, and what the model says it sees.
struct Session {
    _branch: Branch,
    conn: Arc<Connection>,
    writes_at_fork: u64,
    row: i64,
    in_tx: bool,
}

struct Bench {
    db: Arc<Database>,
    wal_path: PathBuf,
    trunk: Arc<Connection>,
    model: TrunkModel,
    timing: bool,
    /// ns per trunk commit, in commit order (timing runs only).
    commit_ns: Vec<u64>,
    /// Counters at the previous state line.
    last: WalPinCounters,
    /// Per-commit checkpoint work since the previous state line.
    win_commits: u64,
    win_ckpt_commits: u64,
    win_scan_min: u64,
    win_scan_max: u64,
    /// Sum of max_frame after each of the window's commits (the engine's, read outside any timed
    /// region), for B3's exact check.
    win_sum_max_frame_ckpt: u64,
}

impl Bench {
    fn stats(&self) -> WalPinStats {
        self.db.walpin_stats()
    }

    fn wal_bytes(&self) -> u64 {
        let one = |p: &PathBuf| std::fs::metadata(p).map_or(0, |m| m.len());
        let mut two = self.wal_path.clone().into_os_string();
        two.push("2");
        one(&self.wal_path) + one(&PathBuf::from(two))
    }

    fn check(&self, s: &Session, what: &str) {
        let got = read_v(&s.conn, s.row);
        let want = self.model.value_at(s.row, s.writes_at_fork);
        if got != want {
            not_a_result(&format!(
                "{what}: branch forked at trunk write {} read row {} = {got:?}, model says {want:?}",
                s.writes_at_fork, s.row
            ));
        }
    }

    fn fork_session(&self, row: i64) -> Session {
        let branch = self.trunk.fork_branch().unwrap();
        let conn = branch.connect().unwrap();
        Session {
            _branch: branch,
            conn,
            writes_at_fork: self.model.writes,
            row,
            in_tx: false,
        }
    }

    /// One trunk autocommit of `row`; the per-commit checkpoint counters are read after it.
    fn trunk_write(&mut self, row: i64) {
        let g = self.model.record(row);
        let sql = format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g));
        let before = walpin::counters();
        if self.timing {
            let t = Instant::now();
            self.trunk.execute(sql).unwrap();
            self.commit_ns.push(t.elapsed().as_nanos() as u64);
        } else {
            self.trunk.execute(sql).unwrap();
        }
        let after = walpin::counters();
        self.win_commits += 1;
        let calls = after.ckpt_calls - before.ckpt_calls;
        if calls > 0 {
            let scan = after.ckpt_frames_scanned - before.ckpt_frames_scanned;
            self.win_ckpt_commits += 1;
            self.win_scan_min = self.win_scan_min.min(scan);
            self.win_scan_max = self.win_scan_max.max(scan);
            // The frame count the checkpoint saw: the commit's own frame is the last, so max_frame
            // now is what it walked (read from the shared metadata; one atomic load under the lock).
            self.win_sum_max_frame_ckpt += self.db.walpin_max_frame();
        }
    }

    fn state(&mut self, label: &str, h: u64) {
        let s = self.stats();
        let c = walpin::counters();
        let d = |a: u64, b: u64| a - b;
        let mut line = format!(
            "# {label} H={h} max_frame={} nbackfills={} ckpt_seq={} marks={:?} mark_readers={:?} \
             fc_pages={} fc_frames={} fc_bytes={} wal_bytes={} rss_bytes={} trunk_writes={} | window \
             commits={} ckpt_commits={} d_ckpt_calls={} d_ckpt_scanned={} scan_per_ckpt_min={} \
             scan_per_ckpt_max={} sum_max_frame_at_ckpt={} d_find_calls={} d_find_scanned={} \
             d_restarts={} d_fw2_switches={} d_fw2_refused={} d_fw3_trunk_reads={} d_fw3_retries={}",
            s.max_frame,
            s.nbackfills,
            s.checkpoint_seq,
            s.mark_values
                .iter()
                .map(|&v| if v == u32::MAX { -1 } else { v as i64 })
                .collect::<Vec<_>>(),
            s.mark_readers,
            s.fc_pages,
            s.fc_frames,
            s.fc_bytes,
            self.wal_bytes(),
            rss_bytes(),
            self.model.writes,
            self.win_commits,
            self.win_ckpt_commits,
            d(c.ckpt_calls, self.last.ckpt_calls),
            d(c.ckpt_frames_scanned, self.last.ckpt_frames_scanned),
            if self.win_ckpt_commits > 0 { self.win_scan_min } else { 0 },
            self.win_scan_max,
            self.win_sum_max_frame_ckpt,
            d(c.find_calls, self.last.find_calls),
            d(c.find_scanned, self.last.find_scanned),
            d(c.restarts, self.last.restarts),
            d(c.fw2_switches, self.last.fw2_switches),
            d(c.fw2_ckpt_refused, self.last.fw2_ckpt_refused),
            d(c.fw3_trunk_reads, self.last.fw3_trunk_reads),
            d(c.fw3_retries, self.last.fw3_retries),
        );
        if self.timing && self.win_commits > 0 {
            let n = (self.win_commits as usize).min(1_000);
            let mut w: Vec<u64> = self.commit_ns[self.commit_ns.len() - n..].to_vec();
            w.sort_unstable();
            line.push_str(&format!(
                " | commit_us last={n} p50={:.2} p90={:.2} p99={:.2} max={:.2}",
                percentile(&w, 50.0) as f64 / 1e3,
                percentile(&w, 90.0) as f64 / 1e3,
                percentile(&w, 99.0) as f64 / 1e3,
                w[w.len() - 1] as f64 / 1e3
            ));
        }
        println!("{line}");
        self.last = c;
        self.win_commits = 0;
        self.win_ckpt_commits = 0;
        self.win_scan_min = u64::MAX;
        self.win_scan_max = 0;
        self.win_sum_max_frame_ckpt = 0;
    }
}

fn begin_select(bench: &Bench, s: &mut Session) {
    s.conn.execute("BEGIN").unwrap();
    s.in_tx = true;
    bench.check(s, "begin_select");
}

fn end_tx(s: &mut Session) {
    s.conn.execute("COMMIT").unwrap();
    s.in_tx = false;
}

fn main() {
    let args = parse_args();
    let (fw1, fw2, fw3) = args.fixes;
    walpin::set_fixes(fw1, fw2, fw3);
    walpin::set_sqlite_restart(args.sqlrestart);
    walpin::set_birth_gate(args.fwb);
    let base_dir = args.dir.clone().unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&base_dir).unwrap();
    let dir = tempfile::TempDir::new_in(&base_dir).unwrap();
    let path = dir.path().join("branch_walpin.db");
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
    if fw2 {
        db.walpin_open_wal2().unwrap();
    }
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
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

    println!("# branch_walpin — Turso fork, trunk WAL under branch readers, r11-walpin PREREG");
    println!(
        "# arm={:?} points={:?} k={} samples={} fixes=fw1:{} fw2:{} fw3:{} sqlrestart:{} timing={} \
         trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} page_size={page_size} trunk_pages={trunk_pages} \
         trunk_synchronous={synchronous}",
        args.arm,
        args.points,
        args.k,
        args.samples,
        fw1 || fw2,
        fw2,
        fw3,
        args.sqlrestart,
        args.timing
    );
    if args.fwb {
        // Amendment 26: the arm is identifiable from the raw, not only from its file name.
        println!("# fixes fwb:true (the birth gate)");
    }
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

    let mut bench = Bench {
        db: db.clone(),
        wal_path: PathBuf::from(format!("{}-wal", path.to_str().unwrap())),
        trunk,
        model: TrunkModel::default(),
        timing: args.timing,
        commit_ns: Vec::new(),
        last: walpin::counters(),
        win_commits: 0,
        win_ckpt_commits: 0,
        win_scan_min: u64::MAX,
        win_scan_max: 0,
        win_sum_max_frame_ckpt: 0,
    };
    bench.state("setup", 0);

    match args.arm {
        Arm::Pin | Arm::NoPin => run_pin(&mut bench, &args),
        Arm::Overlap => run_overlap(&mut bench, &args),
        Arm::U3 => run_u3(&mut bench, &args),
        Arm::Conc => conc::run_conc(&mut bench, &args),
    }
    println!("# done");
}

fn run_pin(bench: &mut Bench, args: &Args) {
    // One trunk commit first, so the branch's read begins while the WAL holds an unbackfilled frame.
    bench.trunk_write(spread_row(0));
    let r0 = spread_row(5_000);
    let mut s = bench.fork_session(r0);
    if args.arm == Arm::Pin {
        begin_select(bench, &mut s);
    } else {
        bench.check(&s, "nopin_select");
    }
    bench.state("pinned", 0);
    let h_max = *args.points.last().unwrap();
    let mut next = 0;
    for h in 1..=h_max {
        bench.trunk_write(spread_row(h));
        if h == args.points[next] {
            bench.state("point", h);
            bench.check(&s, "pin_reread");
            next += 1;
        }
    }
    if s.in_tx {
        end_tx(&mut s);
    }
    bench.state("released", h_max);
    for i in 1..=2 {
        bench.trunk_write(spread_row(h_max + i));
        bench.state("after_release", h_max + i);
    }
    bench.check(&s, "after_release");
}

fn run_overlap(bench: &mut Bench, args: &Args) {
    // As `pin`: the readers begin while the WAL holds an unbackfilled frame (never read mark 0).
    bench.trunk_write(spread_row(0));
    let k = args.k as usize;
    let mut sessions: Vec<Session> = (0..k)
        .map(|i| bench.fork_session(spread_row(7_919 * (i as u64 + 1))))
        .collect();
    for s in sessions.iter_mut() {
        begin_select(bench, s);
    }
    bench.state("opened", 0);
    let h_max = *args.points.last().unwrap();
    let mut next = 0;
    for h in 1..=h_max {
        bench.trunk_write(spread_row(h));
        let s = &mut sessions[(h as usize) % k];
        end_tx(s);
        begin_select(bench, s);
        if h == args.points[next] {
            bench.state("point", h);
            next += 1;
        }
    }
    for s in sessions.iter_mut() {
        end_tx(s);
    }
    bench.state("closed", h_max);
}

fn run_u3(bench: &mut Bench, args: &Args) {
    let k = args.k;
    bench.trunk_write(spread_row(0));
    let mut bg = bench.fork_session(spread_row(7_919));
    begin_select(bench, &mut bg);
    let seq_a = bench.stats().checkpoint_seq;
    let cycle = |bench: &mut Bench, bg: &mut Session| {
        end_tx(bg);
        begin_select(bench, bg);
    };
    for _ in 0..k {
        bench.trunk_write(HOT_ROW);
        cycle(bench, &mut bg);
    }
    let last_a = bench.stats().max_frame;
    bench.state("phase_a", k);
    for j in 0..U3_PHASE_B {
        bench.trunk_write(half_row(j));
        cycle(bench, &mut bg);
    }
    let s = bench.stats();
    bench.state("phase_b", U3_PHASE_B);
    let held = s.nbackfills >= last_a && s.checkpoint_seq == seq_a;
    println!(
        "# u3 precondition: last_frame_of_phase_a={last_a} nbackfills={} ckpt_seq_a={seq_a} ckpt_seq_now={} held={held}",
        s.nbackfills, s.checkpoint_seq
    );
    let (_, fw2, fw3) = args.fixes;
    if !held && !(fw2 || fw3) {
        not_a_result("u3 precondition: phase A's frames are not all backfilled in one WAL generation");
    }
    // Phase C: fresh branches read the hot row; counters around connect and around the SELECT.
    let want = bench.model.value_at(HOT_ROW, bench.model.writes);
    let mut sel_ns = Vec::with_capacity(args.samples);
    let (mut conn_calls, mut conn_scan, mut sel_calls, mut sel_scan) = (0u64, 0u64, 0u64, 0u64);
    let (mut sel_scan_min, mut sel_scan_max) = (u64::MAX, 0u64);
    for _ in 0..args.samples {
        let branch = bench.trunk.fork_branch().unwrap();
        let c0 = walpin::counters();
        let conn = branch.connect().unwrap();
        let c1 = walpin::counters();
        let t = Instant::now();
        let got = read_v(&conn, HOT_ROW);
        let el = t.elapsed().as_nanos() as u64;
        let c2 = walpin::counters();
        if got != want {
            not_a_result(&format!("u3: hot row read {got:?}, model says {want:?}"));
        }
        sel_ns.push(el);
        conn_calls += c1.find_calls - c0.find_calls;
        conn_scan += c1.find_scanned - c0.find_scanned;
        sel_calls += c2.find_calls - c1.find_calls;
        let sc = c2.find_scanned - c1.find_scanned;
        sel_scan += sc;
        sel_scan_min = sel_scan_min.min(sc);
        sel_scan_max = sel_scan_max.max(sc);
        drop(conn);
        branch.reap().unwrap();
    }
    let n = args.samples as f64;
    let mut line = format!(
        "# u3 phase_c k={k} samples={} connect_find_calls_per={:.2} connect_find_scanned_per={:.2} \
         select_find_calls_per={:.2} select_find_scanned_per={:.2} select_find_scanned_min={sel_scan_min} \
         select_find_scanned_max={sel_scan_max}",
        args.samples,
        conn_calls as f64 / n,
        conn_scan as f64 / n,
        sel_calls as f64 / n,
        sel_scan as f64 / n
    );
    if bench.timing {
        sel_ns.sort_unstable();
        line.push_str(&format!(
            " | select_us p50={:.2} p90={:.2} p99={:.2} max={:.2}",
            percentile(&sel_ns, 50.0) as f64 / 1e3,
            percentile(&sel_ns, 90.0) as f64 / 1e3,
            percentile(&sel_ns, 99.0) as f64 / 1e3,
            sel_ns[sel_ns.len() - 1] as f64 / 1e3
        ));
    }
    println!("{line}");
    end_tx(&mut bg);
    bench.state("end", 0);
}
