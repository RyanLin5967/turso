//! DIFF and LIST against the number of live branches: lane r11-diff-list's harness
//! (frontier/round11/r11-diff-list/PREREG.md in artie-research is the specification).
//!
//!   cargo run -p turso_core --release --example branch_difflist -- --arm <arm> [options]
//!
//! Arms:
//!
//!   hot        N trunk children, each writing one row; the trunk rewrites ONE row after every fork
//!   spread     as hot, but the trunk's row walks the table, one leaf further per write
//!   chain      one chain trunk -> b1 -> ... -> bd, each level writing one row; x = depth d
//!   delta      hot grown to the last checkpoint; a probe branch writes delta rows on distinct leaves
//!   list       N branches with metadata (1 in 64 forked from one of 16 hubs); seven list queries
//!   list_churn list grown to the last --checkpoints value, then reaped down through --down
//!   list_stall list grown to the last checkpoint; a worker forks and reaps a hub's child for
//!              STALL_SECS, alone and beside a thread listing every branch back to back
//!
//! Every DIFF sample runs the four arms (scan, died, written, fix) on the same pair and prints
//! `NOT A RESULT` unless they return the same pages. Every LIST sample runs the scan and index arms
//! and checks both against the harness's own record of every branch's parent, metadata and
//! handle. Counters are engine integers per sample; `--timing` adds microseconds per call.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{
    Branch, BranchId, Diff, DiffArm, DiffWork, ListArm, ListFilter, ListWork, Listing,
};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
const HOT_ROW: i64 = 1;
/// `list`: hubs are the first HUBS branches; every HUB_EVERY-th later branch is a hub's child.
const HUBS: usize = 16;
const HUB_EVERY: usize = 64;
const OWNERS: u64 = 1024;
const LEASE_SPACE: u64 = 1 << 32;
/// `list_stall`: seconds the worker runs per lister arm.
const STALL_SECS: f64 = 3.0;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Hot,
    Spread,
    Chain,
    Delta,
    List,
    ListChurn,
    ListStall,
}

struct Args {
    arm: Arm,
    checkpoints: Vec<usize>,
    down: Vec<usize>,
    deltas: Vec<usize>,
    samples: usize,
    seed: u64,
    timing: bool,
    dir: Option<PathBuf>,
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
        checkpoints: vec![1000],
        down: vec![],
        deltas: vec![1, 8, 64, 512],
        samples: 30,
        seed: 0x9E37_79B9_7F4A_7C15,
        timing: false,
        dir: None,
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
                    "delta" => Arm::Delta,
                    "list" => Arm::List,
                    "list_churn" => Arm::ListChurn,
                    "list_stall" => Arm::ListStall,
                    other => die(&format!("unknown arm {other}")),
                })
            }
            "--checkpoints" => args.checkpoints = parse_list(&val(), "--checkpoints"),
            "--down" => args.down = parse_list(&val(), "--down"),
            "--deltas" => args.deltas = parse_list(&val(), "--deltas"),
            "--samples" => args.samples = val().parse().unwrap_or_else(|_| die("bad --samples")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--timing" => args.timing = true,
            "--dir" => args.dir = Some(PathBuf::from(val())),
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.arm = arm.unwrap_or_else(|| die("--arm is required"));
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.down.windows(2).any(|w| w[0] <= w[1])
        || args.down.first().is_some_and(|&d| d >= *args.checkpoints.last().unwrap())
    {
        die("--down must be strictly decreasing and below the last checkpoint");
    }
    if args.down.iter().any(|&d| d < HUBS + 1) {
        die("--down must keep more than the hubs alive");
    }
    if args.samples == 0 {
        die("--samples must be positive");
    }
    if args.checkpoints[0] < 2 {
        die("--checkpoints must start at 2 or more (the pairs need two live branches)");
    }
    if args.deltas.iter().any(|&d| d == 0 || d > 512) {
        die("--deltas entries must be in 1..=512");
    }
    args
}

fn die(msg: &str) -> ! {
    eprintln!("branch_difflist: {msg}");
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

/// A fixed 64-bit mixer (splitmix64's finaliser), for per-branch metadata the harness can recompute.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn branch_value(id: i64) -> String {
    format!("b{:0>width$}", id, width = VALUE_LEN - 1)
}

/// A value no other write uses: same length as every other value, so an UPDATE rewrites the row
/// in place, and distinct, so it is never a no-op.
fn unique_value(tag: char, n: u64) -> String {
    format!("{tag}{:0>width$}", n, width = VALUE_LEN - 1)
}

fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

fn chain_row(level: usize) -> i64 {
    ((level as u64).wrapping_mul(2_654_435_761) % (TRUNK_ROWS / 2) as u64) as i64 + 1
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

/// One (pair, arm) cell at one checkpoint: every sample's counters and, with `--timing`, its time.
#[derive(Default)]
struct Cell {
    works: Vec<DiffWork>,
    us: Vec<f64>,
}

#[derive(Default)]
struct ListCell {
    works: Vec<ListWork>,
    us: Vec<f64>,
}

const DIFF_ARMS: [(&str, DiffArm); 4] = [
    ("scan", DiffArm::Scan { pages: 0 }),
    ("died", DiffArm::Died),
    ("written", DiffArm::Written),
    ("fix", DiffArm::Fix),
];

struct Bench {
    db: Arc<Database>,
    trunk: Arc<Connection>,
    trunk_pages: u32,
    trunk_writes: u64,
    rng: Rng,
    timing: bool,
    samples: usize,
    unique: u64,
}

fn mean(v: impl Iterator<Item = u64>, n: usize) -> f64 {
    v.sum::<u64>() as f64 / n as f64
}

impl Bench {
    fn trunk_write(&mut self, row: i64) {
        self.trunk_writes += 1;
        let value = unique_value('t', self.trunk_writes);
        update_to(&self.trunk, row, &value);
    }

    fn branch_write(&mut self, branch: &Branch, rows: &[i64]) {
        let conn = branch.connect().unwrap();
        conn.execute("BEGIN").unwrap();
        for &row in rows {
            self.unique += 1;
            let value = unique_value('b', self.unique);
            update_to(&conn, row, &value);
        }
        conn.execute("COMMIT").unwrap();
    }

    /// One sample of one pair: all four arms, in a rotating order, checked against one another.
    /// `written` says whether each side holds a write the other cannot see, so the diff must be
    /// non-empty — a check that does not ask the engine.
    fn diff_sample(
        &mut self,
        cells: &mut HashMap<(&'static str, &'static str), Cell>,
        pair: &'static str,
        x: BranchId,
        y: BranchId,
        must_differ: bool,
    ) {
        let start = self.rng.below(DIFF_ARMS.len());
        let mut first: Option<Diff> = None;
        for i in 0..DIFF_ARMS.len() {
            let (name, arm) = DIFF_ARMS[(start + i) % DIFF_ARMS.len()];
            let arm = match arm {
                DiffArm::Scan { .. } => DiffArm::Scan {
                    pages: self.trunk_pages,
                },
                other => other,
            };
            let t = Instant::now();
            let d = self.db.branch_diff(x, y, arm).unwrap();
            let us = t.elapsed().as_secs_f64() * 1e6;
            let cell = cells.entry((pair, name)).or_default();
            cell.works.push(d.work);
            if self.timing {
                cell.us.push(us);
            }
            if let Some(max) = d.pages.last() {
                if *max > self.trunk_pages {
                    not_a_result(&format!(
                        "{pair}: {name} reported page {max}, beyond the scanned {}",
                        self.trunk_pages
                    ));
                }
            }
            match &first {
                None => first = Some(d),
                Some(f) if f.pages != d.pages => not_a_result(&format!(
                    "{pair}: arms disagree on ({}, {}): {:?} vs {name} {:?}",
                    x.0, y.0, f.pages, d.pages
                )),
                Some(_) => {}
            }
        }
        if must_differ && first.as_ref().unwrap().pages.is_empty() {
            not_a_result(&format!(
                "{pair}: ({}, {}) each hold a write the other cannot see, but the diff is empty",
                x.0, y.0
            ));
        }
    }

    fn print_diff_cells(
        &self,
        x: usize,
        cells: &HashMap<(&'static str, &'static str), Cell>,
        pairs: &[&'static str],
    ) {
        for &pair in pairs {
            for (name, _) in DIFF_ARMS {
                let Some(c) = cells.get(&(pair, name)) else {
                    continue;
                };
                let n = c.works.len();
                let w = &c.works;
                let k: Vec<u64> = w.iter().map(|w| w.output).collect();
                let totals: Vec<u64> = w.iter().map(|w| w.total()).collect();
                let trunk: Vec<u64> = w.iter().map(|w| w.trunk_entries).collect();
                let per_k = w
                    .iter()
                    .map(|w| w.total() as f64 / w.output.max(1) as f64)
                    .sum::<f64>()
                    / n as f64;
                let mut line = format!(
                    "{x}\t{pair}\t{name}\t{n}\t{:.2}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}\t{}\t{:.2}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}",
                    mean(k.iter().copied(), n),
                    k.iter().min().unwrap(),
                    k.iter().max().unwrap(),
                    mean(w.iter().map(|w| w.pages_resolved), n),
                    mean(w.iter().map(|w| w.arena_entries), n),
                    mean(w.iter().map(|w| w.trie_nodes), n),
                    mean(w.iter().map(|w| w.trie_leaf_entries), n),
                    mean(trunk.iter().copied(), n),
                    trunk.iter().min().unwrap(),
                    trunk.iter().max().unwrap(),
                    mean(totals.iter().copied(), n),
                    totals.iter().min().unwrap(),
                    totals.iter().max().unwrap(),
                    per_k,
                    mean(w.iter().map(|w| w.trie_reported), n),
                    mean(w.iter().map(|w| w.trie_pruned), n),
                );
                if !c.us.is_empty() {
                    let mut us = c.us.clone();
                    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    line += &format!(
                        "\t{:.2}\t{:.2}\t{:.2}",
                        percentile(&us, 50.0),
                        percentile(&us, 90.0),
                        us[us.len() - 1]
                    );
                }
                println!("{line}");
            }
        }
    }

    fn print_state(&self, x: usize, extra: &str) {
        let s = self.db.branch_stats();
        let table = self
            .db
            .branch_list(ListFilter::Created { lo: 1, hi: 0 }, ListArm::Index)
            .unwrap()
            .work;
        println!(
            "# x={x} live={} arena_in_use={} arena_free={} table_len={} table_capacity={} \
             rss_bytes={} trunk_writes={} {extra}",
            s.live_branches,
            s.arena_slots_in_use,
            s.arena_slots_free,
            table.table_len,
            table.table_capacity,
            rss_bytes(),
            self.trunk_writes,
        );
    }
}

const DIFF_HEADER: &str = "x\tpair\tarm\tsamples\tk_mean\tk_min\tk_max\tresolved\tarena\ttrie_nodes\ttrie_leaf\ttrunk_mean\ttrunk_min\ttrunk_max\twork_mean\twork_min\twork_max\twork_per_k\ttrie_reported\ttrie_pruned[\tp50_us\tp90_us\tmax_us]";
const LIST_HEADER: &str = "x\tquery\tarm\tsamples\tout_mean\tout_min\tout_max\tvisited_mean\tvisited_min\tvisited_max\tvisited_per_out\ttable_len\ttable_capacity\tunder_lock_mean\tsnap_nodes_mean\tsnap_leaf_mean[\tp50_us\tp90_us\tmax_us]";
const STALL_HEADER: &str = "x\tlister\top\tops\tp50_us\tp99_us\tp999_us\tmax_us\tlists\tlist_p50_us\tlist_max_us";

fn main() {
    let args = parse_args();
    let tmp = tempfile::TempDir::new_in(args.dir.clone().unwrap_or_else(std::env::temp_dir))
        .unwrap();
    let path = tmp.path().join("branch_difflist.db");
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
    // PREREG §3: no auto-checkpoint on the trunk (the B1 checkpoint cliff is not under test), and
    // trunk commits do not fsync.
    trunk.wal_auto_actions_disable();
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
    let int = |sql: &str| {
        trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    let page_size = int("PRAGMA page_size");
    let trunk_pages = int("PRAGMA page_count") as u32;
    println!("# branch_difflist — lane r11-diff-list, Turso fork (F1+F2+F4 + F-diff + F-cat)");
    println!(
        "# arm={:?} checkpoints={:?} down={:?} deltas={:?} samples={} seed={:#x} timing={} \
         trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} page_size={page_size} trunk_pages={trunk_pages} \
         no_autocheckpoint=true synchronous=OFF",
        args.arm,
        args.checkpoints,
        args.down,
        args.deltas,
        args.samples,
        args.seed,
        args.timing
    );
    println!(
        "# clock tick {:.0} ns; build: {}; rss_base_bytes={}",
        clock_tick_ns(),
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        rss_bytes()
    );
    let mut b = Bench {
        db: db.clone(),
        trunk,
        trunk_pages,
        trunk_writes: 0,
        rng: Rng(args.seed),
        timing: args.timing,
        samples: args.samples,
        unique: 0,
    };
    match args.arm {
        Arm::Hot | Arm::Spread => arm_trunk_writes(&mut b, &args),
        Arm::Chain => arm_chain(&mut b, &args),
        Arm::Delta => arm_delta(&mut b, &args),
        Arm::List | Arm::ListChurn => arm_list(&mut b, &args),
        Arm::ListStall => arm_list_stall(&mut b, &args),
    }
    let end = db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!("# teardown: every branch freed, arena empty");
}

/// Fork a trunk child, write `rows` in it, then (hot/spread) one trunk write.
fn grow_one(b: &mut Bench, arm: Arm, n: usize) -> Branch {
    let branch = b.trunk.fork_branch().unwrap();
    b.branch_write(&branch, &[row_for(n)]);
    let trow = match arm {
        Arm::Spread => spread_row(b.trunk_writes),
        _ => HOT_ROW,
    };
    b.trunk_write(trow);
    branch
}

/// Arms `hot` and `spread` (PREREG §3): pairs H1–H6.
fn arm_trunk_writes(b: &mut Bench, args: &Args) {
    println!("{DIFF_HEADER}");
    let mut live: Vec<Branch> = Vec::new();
    let pairs = ["P1_oldest_trunk", "P2_newest_trunk", "P3_random_trunk", "P4_oldest_newest", "P5_random_random", "P6_adjacent"];
    for &n in &args.checkpoints {
        let t = Instant::now();
        while live.len() < n {
            let branch = grow_one(b, args.arm, live.len());
            live.push(branch);
        }
        let grow_s = t.elapsed().as_secs_f64();
        let s = b.db.branch_stats();
        if s.live_branches != n || s.arena_slots_in_use != 2 * n {
            not_a_result(&format!(
                "expected {n} branches and {} arena pages (one own page and one retained trunk \
                 pre-image per fork): {s:?}",
                2 * n
            ));
        }
        let mut cells = HashMap::new();
        let trunk = BranchId::TRUNK;
        for _ in 0..b.samples {
            let last = live.len() - 1;
            let (oldest, newest) = (live[0].id(), live[last].id());
            let r = live[b.rng.below(live.len())].id();
            let i = b.rng.below(live.len());
            let mut j = b.rng.below(live.len());
            if j == i {
                j = (i + 1) % live.len();
            }
            let a = b.rng.below(last);
            b.diff_sample(&mut cells, pairs[0], oldest, trunk, true);
            b.diff_sample(&mut cells, pairs[1], newest, trunk, true);
            b.diff_sample(&mut cells, pairs[2], r, trunk, true);
            b.diff_sample(&mut cells, pairs[3], oldest, newest, true);
            b.diff_sample(&mut cells, pairs[4], live[i].id(), live[j].id(), true);
            b.diff_sample(&mut cells, pairs[5], live[a].id(), live[a + 1].id(), true);
        }
        b.print_diff_cells(n, &cells, &pairs);
        b.print_state(n, &format!("grow_s={grow_s:.1}"));
    }
    drop(live);
}

/// Arm `chain` (PREREG §3): C1 (tip, trunk), C2 (tip, parent), C3 (two fresh children of the
/// tip), C4 (tip, b1).
fn arm_chain(b: &mut Bench, args: &Args) {
    println!("{DIFF_HEADER}");
    let mut chain: Vec<Branch> = Vec::new();
    let pairs = ["C1_tip_trunk", "C2_tip_parent", "C3_tip_children", "C4_tip_b1"];
    let mut fresh = 0usize;
    for &d in &args.checkpoints {
        if d < 2 {
            die("chain checkpoints must be at least 2");
        }
        while chain.len() < d {
            let level = chain.len() + 1;
            let branch = match chain.last() {
                None => b.trunk.fork_branch().unwrap(),
                Some(parent) => parent.fork().unwrap(),
            };
            b.branch_write(&branch, &[chain_row(level)]);
            chain.push(branch);
        }
        let mut cells = HashMap::new();
        let tip = chain[d - 1].id();
        for _ in 0..b.samples {
            b.diff_sample(&mut cells, pairs[0], tip, BranchId::TRUNK, true);
            b.diff_sample(&mut cells, pairs[1], tip, chain[d - 2].id(), true);
            let kids: Vec<Branch> = (0..2)
                .map(|_| {
                    let kid = chain[d - 1].fork().unwrap();
                    fresh += 1;
                    b.branch_write(&kid, &[chain_row(1_000_000 + fresh)]);
                    kid
                })
                .collect();
            b.diff_sample(&mut cells, pairs[2], kids[0].id(), kids[1].id(), false);
            for kid in kids {
                let reaped = kid.reap().unwrap();
                if reaped.deferred || reaped.freed_pages != 1 {
                    not_a_result(&format!("a fresh child's reap freed {reaped:?}"));
                }
            }
            b.diff_sample(&mut cells, pairs[3], tip, chain[0].id(), false);
        }
        b.print_diff_cells(d, &cells, &pairs);
        b.print_state(d, "");
    }
    drop(chain);
}

/// Arm `delta` (PREREG §3): hot grown to the last checkpoint, then a probe per delta.
fn arm_delta(b: &mut Bench, args: &Args) {
    println!("{DIFF_HEADER}");
    let n = *args.checkpoints.last().unwrap();
    let mut live: Vec<Branch> = Vec::new();
    while live.len() < n {
        let branch = grow_one(b, Arm::Hot, live.len());
        live.push(branch);
    }
    let pairs = ["D1_probe_trunk", "D2_probe_newest"];
    for &delta in &args.deltas {
        let probe = b.trunk.fork_branch().unwrap();
        // Rows 37 apart land on distinct leaves (a leaf holds ~37 rows), starting clear of the hot row.
        let rows: Vec<i64> = (0..delta as i64).map(|j| 2 + j * 37).collect();
        b.branch_write(&probe, &rows);
        b.trunk_write(HOT_ROW);
        let mut cells = HashMap::new();
        let newest = live[live.len() - 1].id();
        for _ in 0..b.samples {
            b.diff_sample(&mut cells, pairs[0], probe.id(), BranchId::TRUNK, true);
            b.diff_sample(&mut cells, pairs[1], probe.id(), newest, true);
        }
        b.print_diff_cells(delta, &cells, &pairs);
        b.print_state(delta, &format!("n={n}"));
        probe.reap().unwrap();
    }
    drop(live);
}

/// The harness's own record of a listed branch.
struct Rec {
    branch: Option<Branch>,
    id: BranchId,
    parent: BranchId,
    owner: u64,
    lease: u64,
}

/// Grow the `list` fixture to `n` branches.
fn grow_list(b: &mut Bench, recs: &mut Vec<Rec>, n: usize) {
    while recs.len() < n {
        let i = recs.len();
        let (branch, parent) = if i >= HUBS && i % HUB_EVERY == 0 {
            let hub = &recs[(i / HUB_EVERY) % HUBS];
            (hub.branch.as_ref().unwrap().fork().unwrap(), hub.id)
        } else {
            (b.trunk.fork_branch().unwrap(), BranchId::TRUNK)
        };
        let (owner, lease) = (mix(i as u64) % OWNERS, mix(!(i as u64)) % LEASE_SPACE);
        branch.set_meta(owner, lease).unwrap();
        recs.push(Rec {
            id: branch.id(),
            branch: Some(branch),
            parent,
            owner,
            lease,
        });
    }
}

fn percentiles(mut us: Vec<f64>) -> (usize, f64, f64, f64, f64) {
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if us.is_empty() {
        return (0, 0.0, 0.0, 0.0, 0.0);
    }
    (
        us.len(),
        percentile(&us, 50.0),
        percentile(&us, 99.0),
        percentile(&us, 99.9),
        us[us.len() - 1],
    )
}

/// Arm `list_stall` (PREREG amendment A7): what a listing of every branch costs the OTHER branch
/// operations, which wait on the same store mutex.
fn arm_list_stall(b: &mut Bench, args: &Args) {
    let n = *args.checkpoints.last().unwrap();
    let mut recs: Vec<Rec> = Vec::new();
    grow_list(b, &mut recs, n);
    b.print_state(n, "");
    println!("{STALL_HEADER}");
    let db = b.db.clone();
    let hub = recs[0].branch.as_ref().unwrap();
    let arms = [
        ("alone", None),
        ("scan", Some(ListArm::Scan)),
        ("index", Some(ListArm::Index)),
        ("snapshot", Some(ListArm::Snapshot)),
    ];
    for (label, arm) in arms {
        let stop = AtomicBool::new(false);
        let (mut fork_us, mut reap_us) = (Vec::new(), Vec::new());
        let (list_us, wrong) = std::thread::scope(|s| {
            let lister = arm.map(|arm| {
                let (db, stop) = (&db, &stop);
                s.spawn(move || {
                    let (mut us, mut wrong) = (Vec::new(), 0usize);
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        let listed = db.branch_list(ListFilter::All, arm).unwrap();
                        us.push(t.elapsed().as_secs_f64() * 1e6);
                        // The worker's child is listed or not, depending on the moment.
                        if listed.ids.len() != n && listed.ids.len() != n + 1 {
                            wrong += 1;
                        }
                    }
                    (us, wrong)
                })
            });
            let t0 = Instant::now();
            while t0.elapsed().as_secs_f64() < STALL_SECS {
                let t = Instant::now();
                let kid = hub.fork().unwrap();
                fork_us.push(t.elapsed().as_secs_f64() * 1e6);
                let t = Instant::now();
                let reaped = kid.reap().unwrap();
                reap_us.push(t.elapsed().as_secs_f64() * 1e6);
                if reaped.deferred {
                    not_a_result("a childless fresh branch's reap was deferred");
                }
            }
            stop.store(true, Ordering::Relaxed);
            lister.map_or((Vec::new(), 0), |h| h.join().unwrap())
        });
        if wrong > 0 {
            not_a_result(&format!("{label}: {wrong} listings had neither {n} nor {} ids", n + 1));
        }
        let (lists, list_p50, _, _, list_max) = percentiles(list_us);
        for (op, us) in [("fork", fork_us), ("reap", reap_us)] {
            let (ops, p50, p99, p999, max) = percentiles(us);
            println!(
                "{n}\t{label}\t{op}\t{ops}\t{p50:.2}\t{p99:.2}\t{p999:.2}\t{max:.2}\t{lists}\t{list_p50:.2}\t{list_max:.2}"
            );
        }
    }
    drop(recs);
}

fn arm_list(b: &mut Bench, args: &Args) {
    println!("{LIST_HEADER}");
    let mut recs: Vec<Rec> = Vec::new();
    for &n in &args.checkpoints {
        let t = Instant::now();
        grow_list(b, &mut recs, n);
        let grow_s = t.elapsed().as_secs_f64();
        list_queries(b, &recs, n);
        b.print_state(n, &format!("grow_s={grow_s:.1}"));
    }
    if args.arm == Arm::ListChurn {
        // Reap uniformly random non-hub branches down through each --down value.
        let mut alive: Vec<usize> = (HUBS..recs.len()).collect();
        for &d in &args.down {
            while alive.len() + HUBS > d {
                let at = b.rng.below(alive.len());
                let i = alive.swap_remove(at);
                let reaped = recs[i].branch.take().unwrap().reap().unwrap();
                if reaped.deferred {
                    not_a_result("a leaf branch's reap was deferred");
                }
            }
            list_queries(b, &recs, d);
            b.print_state(d, "after_churn");
        }
    }
    drop(recs);
}

fn list_queries(b: &mut Bench, recs: &[Rec], x: usize) {
    let live: Vec<&Rec> = recs.iter().filter(|r| r.branch.is_some()).collect();
    let n = live.len();
    let max_id = recs.last().unwrap().id.0;
    let mut cells: HashMap<(&'static str, &'static str), ListCell> = HashMap::new();
    let names = [
        "Q1_all",
        "Q2_parent_trunk",
        "Q3_parent_hub",
        "Q4_created_last100",
        "Q5_owner",
        "Q6_lease_before",
        "Q7_owner_lease",
    ];
    for _ in 0..b.samples {
        let hub = recs[b.rng.below(HUBS)].id;
        let owner = b.rng.next() % OWNERS;
        let lease_t = (LEASE_SPACE as f64 * 100.0 / n as f64) as u64;
        let queries = [
            ListFilter::All,
            ListFilter::Parent(BranchId::TRUNK),
            ListFilter::Parent(hub),
            ListFilter::Created {
                lo: max_id.saturating_sub(99),
                hi: max_id,
            },
            ListFilter::Owner(owner),
            ListFilter::LeaseBefore(lease_t),
            ListFilter::OwnerLease {
                owner,
                before: LEASE_SPACE / 16,
            },
        ];
        for (qi, &filter) in queries.iter().enumerate() {
            let mut want: Vec<BranchId> = live
                .iter()
                .filter(|r| match filter {
                    ListFilter::All => true,
                    ListFilter::Parent(p) => r.parent == p,
                    ListFilter::Created { lo, hi } => lo <= r.id.0 && r.id.0 <= hi,
                    ListFilter::Owner(o) => r.owner == o,
                    ListFilter::LeaseBefore(t) => r.lease < t,
                    ListFilter::OwnerLease { owner, before } => {
                        r.owner == owner && r.lease < before
                    }
                })
                .map(|r| r.id)
                .collect();
            want.sort_unstable();
            let mut arms = vec![("scan", ListArm::Scan), ("index", ListArm::Index)];
            if qi == 6 {
                arms.push(("index_single", ListArm::IndexSingle));
            }
            if qi <= 2 {
                arms.push(("snapshot", ListArm::Snapshot));
            }
            let start = b.rng.below(arms.len());
            for i in 0..arms.len() {
                let (name, arm) = arms[(start + i) % arms.len()];
                let t = Instant::now();
                let Listing { mut ids, work } = b.db.branch_list(filter, arm).unwrap();
                let us = t.elapsed().as_secs_f64() * 1e6;
                ids.sort_unstable();
                if ids != want {
                    not_a_result(&format!(
                        "{} {name}: {} ids, the harness's record says {}",
                        names[qi],
                        ids.len(),
                        want.len()
                    ));
                }
                let cell = cells.entry((names[qi], name)).or_default();
                cell.works.push(work);
                if b.timing {
                    cell.us.push(us);
                }
            }
        }
    }
    for q in names {
        for arm in ["scan", "index", "index_single", "snapshot"] {
            let Some(c) = cells.get(&(q, arm)) else {
                continue;
            };
            let k = c.works.len();
            let w = &c.works;
            let out: Vec<u64> = w.iter().map(|w| w.output).collect();
            let vis: Vec<u64> = w.iter().map(|w| w.entries_visited).collect();
            let per = w
                .iter()
                .map(|w| w.entries_visited as f64 / w.output.max(1) as f64)
                .sum::<f64>()
                / k as f64;
            let mut line = format!(
                "{x}\t{q}\t{arm}\t{k}\t{:.2}\t{}\t{}\t{:.2}\t{}\t{}\t{:.2}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}",
                mean(out.iter().copied(), k),
                out.iter().min().unwrap(),
                out.iter().max().unwrap(),
                mean(vis.iter().copied(), k),
                vis.iter().min().unwrap(),
                vis.iter().max().unwrap(),
                per,
                w[0].table_len,
                w[0].table_capacity,
                mean(w.iter().map(|w| w.under_lock), k),
                mean(w.iter().map(|w| w.trie_nodes), k),
                mean(w.iter().map(|w| w.trie_leaf_slots), k),
            );
            if !c.us.is_empty() {
                let mut us = c.us.clone();
                us.sort_by(|a, b| a.partial_cmp(b).unwrap());
                line += &format!(
                    "\t{:.2}\t{:.2}\t{:.2}",
                    percentile(&us, 50.0),
                    percentile(&us, 90.0),
                    us[us.len() - 1]
                );
            }
            println!("{line}");
        }
    }
}
