//! One LARGE branch transaction: lane r11-bigtxn's PREREG (artie-research
//! frontier/round11/r11-bigtxn/PREREG.md) is the specification; this is its implementation.
//!
//!   cargo run -p turso_core --release --example branch_bigtxn -- --arm <arm> [options]
//!
//! Arms:
//!
//!   branch  fork a branch from the trunk and run `BEGIN; UPDATE t SET v = 'B' || substr(v, 2)
//!           WHERE id <= D; COMMIT` on it: exactly D leaves dirtied (one 3,000-byte row per leaf).
//!           Phases: update, commit, fork_first (the branch's first fork), reap.
//!   trunk   the same UPDATE on the trunk connection (control); `--trunk-children C` keeps C live
//!           children forked before it, so the trunk retains D pre-images.
//!   probe   grow N other live branches (one written page each) and, at each N, run the branch
//!           arm's transaction at D while one other thread loops a small branch op and timestamps it.
//!   addcol  BranchBench Software Dev's shape on a branch: `ALTER TABLE t ADD COLUMN w TEXT`, then a
//!           backfill `UPDATE t SET w = substr(v, 1, 700) WHERE id <= 3D` over 1,300-byte rows
//!           (three per leaf), so the D leaves it rewrites no longer fit and split: new pages are
//!           allocated inside the transaction. Phases: alter, update, commit, fork_first, reap.
//!
//! Every phase prints the engine's counter deltas (store-mutex holds, bytes copied under the mutex,
//! the largest single hold, page-cache replacement work). Times are printed only with `--timing`,
//! which also turns on the engine's hold clock; a timed run must go through the fleet lock.
//! Every read is checked against what the harness wrote; a mismatch prints NOT A RESULT and exits 1.

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::{
    cache_work, set_hold_timing, Branch, BranchDurability, BranchWork, CacheWork, HoldMax,
};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

/// One row per 4 KiB leaf: two 3,000-byte cells cannot share a leaf, and one stays below the
/// 4,061-byte local-payload limit, so no overflow page.
const VALUE_LEN: usize = 3000;
/// `addcol`: three 1,300-byte rows fill a leaf; with a 700-byte column added, two do.
const ADDCOL_VALUE_LEN: usize = 1300;
const ADDCOL_ROWS_PER_LEAF: usize = 3;
const BUILD_BATCH: i64 = 10_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Branch,
    Trunk,
    Probe,
    Addcol,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProbeKind {
    ForkReap,
    Write1,
}

struct Args {
    arm: Arm,
    d_list: Vec<usize>,
    n_list: Vec<usize>,
    rows: usize,
    reps: usize,
    reps_last: usize,
    timing: bool,
    trunk_children: usize,
    probe: ProbeKind,
    dir: Option<PathBuf>,
    /// The branch and trunk arms' UPDATE runs as its own (autocommit) transaction: no statement
    /// journal, and one phase `update+commit` instead of two.
    autocommit: bool,
    /// `--durability volatile|durable|durable-nosync|catalog` (default volatile): the branch store
    /// the database opens with (amendment 8: the composed durable store).
    durability: BranchDurability,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_bigtxn: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_list(s: &str, what: &str) -> Vec<usize> {
    s.split(',')
        .map(|x| x.parse().unwrap_or_else(|_| die(&format!("bad {what}"))))
        .collect()
}

fn parse_args() -> Args {
    let mut arm = None;
    let mut reps_last = None;
    let mut args = Args {
        arm: Arm::Branch,
        d_list: vec![1000],
        n_list: vec![1000],
        rows: 0,
        reps: 1,
        reps_last: 1,
        timing: false,
        trunk_children: 0,
        probe: ProbeKind::ForkReap,
        dir: None,
        autocommit: false,
        durability: BranchDurability::Volatile,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--arm" => {
                arm = Some(match val().as_str() {
                    "branch" => Arm::Branch,
                    "trunk" => Arm::Trunk,
                    "probe" => Arm::Probe,
                    "addcol" => Arm::Addcol,
                    other => die(&format!("unknown arm {other}")),
                })
            }
            "--d" => args.d_list = parse_list(&val(), "--d"),
            "--n" => args.n_list = parse_list(&val(), "--n"),
            "--rows" => args.rows = val().parse().unwrap_or_else(|_| die("bad --rows")),
            "--reps" => args.reps = val().parse().unwrap_or_else(|_| die("bad --reps")),
            "--reps-last" => {
                reps_last = Some(val().parse().unwrap_or_else(|_| die("bad --reps-last")))
            }
            "--timing" => args.timing = true,
            "--trunk-children" => {
                args.trunk_children = val().parse().unwrap_or_else(|_| die("bad --trunk-children"))
            }
            "--probe" => {
                args.probe = match val().as_str() {
                    "fork_reap" => ProbeKind::ForkReap,
                    "write1" => ProbeKind::Write1,
                    other => die(&format!("unknown probe {other}")),
                }
            }
            "--dir" => args.dir = Some(PathBuf::from(val())),
            "--autocommit" => args.autocommit = true,
            "--durability" => {
                args.durability = match val().as_str() {
                    "volatile" => BranchDurability::Volatile,
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    "catalog" => BranchDurability::Catalog { sync: true },
                    other => die(&format!("unknown --durability {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.arm = arm.unwrap_or_else(|| die("--arm is required"));
    args.reps_last = reps_last.unwrap_or(args.reps);
    let max_d = *args.d_list.iter().max().unwrap_or(&0);
    let rows_per_d = if args.arm == Arm::Addcol {
        ADDCOL_ROWS_PER_LEAF
    } else {
        1
    };
    if args.rows == 0 {
        args.rows = max_d * rows_per_d;
    }
    if max_d * rows_per_d > args.rows {
        die("--rows must cover every --d");
    }
    if args.d_list.is_empty() || args.d_list.contains(&0) {
        die("--d entries must be positive");
    }
    if args.reps == 0 || args.reps_last == 0 {
        die("--reps must be positive");
    }
    // Strictly increasing grows the other branches between checkpoints; strictly decreasing
    // reaps them, so a checkpoint can run LAST at a small N (separating N from run order).
    let rising = args.n_list.windows(2).all(|w| w[0] < w[1]);
    let falling = args.n_list.windows(2).all(|w| w[0] > w[1]);
    if args.arm == Arm::Probe && (args.n_list.is_empty() || !(rising || falling)) {
        die("--n must be strictly increasing or strictly decreasing");
    }
    args
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn addcol_value(id: i64) -> String {
    format!("{:0>width$}", id, width = ADDCOL_VALUE_LEN)
}

fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap()
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

/// Engine counters at one instant.
#[derive(Clone, Copy)]
struct Snap {
    w: BranchWork,
    c: CacheWork,
    arena_in_use: usize,
    live: usize,
}

fn snap(db: &Arc<Database>) -> Snap {
    let s = db.branch_stats().expect("branch stats");
    Snap {
        w: s.work,
        c: cache_work(),
        arena_in_use: s.arena_slots_in_use,
        live: s.live_branches,
    }
}

const HEADER: &str = "phase\tD\tN\trep\tus\tlock_holds\tlocked_copy_bytes\tmax_hold_pages\tmax_hold_copy_bytes\t\
max_hold_realloc_moved\tmax_hold_ns\tresolve_calls\tevict_calls\tevict_examined\tevict_full\t\
over_capacity_admits\tevictable_scan\tspill_scan\tsubjournal_pages\tarena_in_use\tcache_len\tview_build_pages\t\
holds_timed\thold_p50_ns\thold_p99_ns\thold_p999_ns\tmax_hold_realloc_bytes\tmax_hold_node_copies\t\
max_hold_zeroed_bytes\tholds_ge_16us\tsync_locked_bytes\tsync_unlocked_bytes\tjournal_copied_bytes\tjournal_handed_bytes";

struct Ctx {
    db: Arc<Database>,
    timing: bool,
}

impl Ctx {
    /// Run `f` as one phase: reset the per-hold maxima, take counters around it (outside the timed
    /// window), and print one row.
    fn phase<T>(
        &self,
        name: &str,
        d: usize,
        n: usize,
        rep: usize,
        cache_len: impl FnOnce() -> usize,
        f: impl FnOnce() -> T,
    ) -> (T, Duration) {
        let _ = self.db.branch_take_hold_max();
        let _ = self.db.branch_take_hold_hist();
        let a = snap(&self.db);
        let t = Instant::now();
        let out = f();
        let el = t.elapsed();
        let b = snap(&self.db);
        let m: HoldMax = self.db.branch_take_hold_max();
        let hist = self.db.branch_take_hold_hist();
        let timed: u64 = hist.iter().sum();
        // Holds of 16 µs or more (buckets from floor(8 * log2(16,384)) = 112 on): a count per phase
        // separates a growing term (more long holds as D grows) from a tail (a fixed few).
        let long = if self.timing {
            hist.iter().skip(112).sum::<u64>().to_string()
        } else {
            "-".to_string()
        };
        // The lower edge (ns) of the bucket holding the q-quantile of this phase's hold durations.
        let q = |q: f64| -> String {
            if !self.timing || timed == 0 {
                return "-".to_string();
            }
            let want = ((q * timed as f64).ceil() as u64).max(1);
            let mut seen = 0;
            for (b, &c) in hist.iter().enumerate() {
                seen += c;
                if seen >= want {
                    return format!("{:.0}", 2f64.powf(b as f64 / 8.0));
                }
            }
            "-".to_string()
        };
        let us = if self.timing {
            format!("{:.1}", el.as_secs_f64() * 1e6)
        } else {
            "-".to_string()
        };
        let ns = if self.timing {
            m.ns.to_string()
        } else {
            "-".to_string()
        };
        println!(
            "{name}\t{d}\t{n}\t{rep}\t{us}\t{}\t{}\t{}\t{}\t{}\t{ns}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{timed}\t{}\t{}\t{}\t{}\t{}\t{}\t{long}\t{}\t{}\t{}\t{}",
            b.w.lock_holds - a.w.lock_holds,
            b.w.locked_copy_bytes - a.w.locked_copy_bytes,
            m.pages,
            m.copy_bytes,
            m.realloc_moved,
            b.w.resolve_calls - a.w.resolve_calls,
            b.c.evict_calls - a.c.evict_calls,
            b.c.evict_examined - a.c.evict_examined,
            b.c.evict_full - a.c.evict_full,
            b.c.over_capacity_admits - a.c.over_capacity_admits,
            b.c.evictable_scan_entries - a.c.evictable_scan_entries,
            b.c.spill_scan_entries - a.c.spill_scan_entries,
            b.c.subjournal_pages - a.c.subjournal_pages,
            b.arena_in_use,
            cache_len(),
            b.w.view_build_pages - a.w.view_build_pages,
            q(0.5),
            q(0.99),
            q(0.999),
            m.realloc_bytes,
            m.node_copies,
            m.zeroed_bytes,
            b.w.sync_locked_bytes - a.w.sync_locked_bytes,
            b.w.sync_unlocked_bytes - a.w.sync_unlocked_bytes,
            b.w.journal_copied_bytes - a.w.journal_copied_bytes,
            b.w.journal_handed_bytes - a.w.journal_handed_bytes,
        );
        (out, el)
    }
}

fn open_db(dir: &Path, durability: BranchDurability) -> (Arc<Database>, Arc<Connection>) {
    for suffix in ["", "-wal", "-shm", "-branch-arena", "-branch-log", "-branch-snap", "-branch-cat", "-branch-cat-wal", "-branch-cat-shm"] {
        let _ = std::fs::remove_file(dir.join(format!("bigtxn.db{suffix}")));
    }
    let path = dir.join("bigtxn.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    (db, trunk)
}

fn build_table(trunk: &Arc<Connection>, rows: usize, value: fn(i64) -> String) {
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let mut ins = trunk.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
    let mut id = 1i64;
    while id <= rows as i64 {
        trunk.execute("BEGIN").unwrap();
        let end = (id + BUILD_BATCH - 1).min(rows as i64);
        while id <= end {
            ins.reset().unwrap();
            ins.bind_at(NonZero::new(1).unwrap(), Value::from_i64(id))
                .unwrap();
            ins.bind_at(NonZero::new(2).unwrap(), Value::build_text(value(id)))
                .unwrap();
            ins.run_ignore_rows().unwrap();
            id += 1;
        }
        trunk.execute("COMMIT").unwrap();
    }
    drop(ins);
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let leaves_check = int(trunk, "SELECT count(*) FROM t");
    if leaves_check != rows as i64 {
        not_a_result(&format!("built {leaves_check} rows, wanted {rows}"));
    }
}

/// Rows among ids <= d whose value starts with `mark`, and whether each such row is otherwise the
/// trunk's value (checked on a sample: its tail must be the id zero-padded).
fn count_marked(conn: &Arc<Connection>, d: usize, mark: char) -> i64 {
    int(
        conn,
        &format!("SELECT count(*) FROM t WHERE id <= {d} AND substr(v, 1, 1) = '{mark}'"),
    )
}

fn check_sample(conn: &Arc<Connection>, id: i64, mark: Option<char>) {
    let mut stmt = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    let got = match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {id}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {id}: {} rows", rows.len())),
    };
    let want = trunk_value(id);
    let want = match mark {
        Some(m) => format!("{m}{}", &want[1..]),
        None => want,
    };
    if got != want {
        not_a_result(&format!(
            "row {id}: read {}… (len {}), expected {}…",
            &got[..got.len().min(12)],
            got.len(),
            &want[..12]
        ));
    }
}

/// The branch arm's unit: one big transaction on a fresh branch, then its first fork and its reap.
fn big_branch_txn(
    ctx: &Ctx,
    trunk: &Arc<Connection>,
    d: usize,
    n: usize,
    rep: usize,
    autocommit: bool,
) -> (Duration, Duration) {
    let db = &ctx.db;
    let before = snap(db);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    let sql = format!("UPDATE t SET v = 'B' || substr(v, 2) WHERE id <= {d}");
    let (t_update, t_commit) = if autocommit {
        let (_, t) = ctx.phase("update+commit", d, n, rep, || bc.page_cache_len(), || {
            bc.execute(&sql).unwrap()
        });
        (t, Duration::ZERO)
    } else {
        bc.execute("BEGIN").unwrap();
        let (_, t_update) = ctx.phase("update", d, n, rep, || bc.page_cache_len(), || {
            bc.execute(&sql).unwrap()
        });
        let (_, t_commit) = ctx.phase("commit", d, n, rep, || bc.page_cache_len(), || {
            bc.execute("COMMIT").unwrap()
        });
        (t_update, t_commit)
    };
    let after = snap(db);
    if after.arena_in_use - before.arena_in_use != d {
        not_a_result(&format!(
            "commit of D={d} added {} arena pages",
            after.arena_in_use as i64 - before.arena_in_use as i64
        ));
    }
    let marked = count_marked(&bc, d, 'B');
    if marked != d as i64 {
        not_a_result(&format!("branch reads {marked} rewritten rows of {d}"));
    }
    for id in [1, (d as i64 + 1) / 2, d as i64] {
        check_sample(&bc, id, Some('B'));
        check_sample(trunk, id, None);
    }
    let trunk_marked = count_marked(trunk, d, 'B');
    if trunk_marked != 0 {
        not_a_result(&format!("trunk reads {trunk_marked} of the branch's rows"));
    }
    let (child, _) = ctx.phase("fork_first", d, n, rep, || bc.page_cache_len(), || b.fork().unwrap());
    let (_, _) = ctx.phase("reap_child", d, n, rep, || 0, || child.reap().unwrap());
    drop(bc);
    let (reaped, _) = ctx.phase("reap", d, n, rep, || 0, || b.reap().unwrap());
    if reaped.freed_pages != d || reaped.deferred {
        not_a_result(&format!("reap of D={d} freed {} (deferred {})", reaped.freed_pages, reaped.deferred));
    }
    let end = snap(db);
    if end.arena_in_use != before.arena_in_use || end.live != before.live {
        not_a_result(&format!(
            "the rep leaked: arena {} -> {}, live {} -> {}",
            before.arena_in_use, end.arena_in_use, before.live, end.live
        ));
    }
    (t_update, t_commit)
}

/// The addcol arm's unit: add a column on a fresh branch and backfill it over the first 3D rows.
fn addcol_txn(ctx: &Ctx, trunk: &Arc<Connection>, d: usize, rep: usize) -> Duration {
    let db = &ctx.db;
    let rows = d * ADDCOL_ROWS_PER_LEAF;
    let before = snap(db);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("BEGIN").unwrap();
    let t = Instant::now();
    ctx.phase("alter", d, 0, rep, || bc.page_cache_len(), || {
        bc.execute("ALTER TABLE t ADD COLUMN w TEXT").unwrap()
    });
    let sql = format!("UPDATE t SET w = substr(v, 1, 700) WHERE id <= {rows}");
    ctx.phase("update", d, 0, rep, || bc.page_cache_len(), || bc.execute(&sql).unwrap());
    ctx.phase("commit", d, 0, rep, || bc.page_cache_len(), || bc.execute("COMMIT").unwrap());
    let el = t.elapsed();
    let written = snap(db).arena_in_use - before.arena_in_use;
    let filled = int(&bc, "SELECT count(*) FROM t WHERE w IS NOT NULL");
    if filled != rows as i64 {
        not_a_result(&format!("the branch backfilled {filled} rows of {rows}"));
    }
    let tail_ok = int(
        &bc,
        &format!("SELECT count(*) FROM t WHERE id <= {rows} AND w = substr(v, 1, 700)"),
    );
    if tail_ok != rows as i64 {
        not_a_result(&format!("{tail_ok} backfilled rows of {rows} hold the right value"));
    }
    let integrity = bc
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    if integrity.len() != 1 || integrity[0][0] != Value::build_text("ok") {
        not_a_result(&format!("branch integrity_check: {integrity:?}"));
    }
    if trunk.prepare("SELECT w FROM t").is_ok() {
        not_a_result("the trunk sees the branch's new column");
    }
    let (child, _) = ctx.phase("fork_first", d, 0, rep, || bc.page_cache_len(), || b.fork().unwrap());
    ctx.phase("reap_child", d, 0, rep, || 0, || child.reap().unwrap());
    drop(bc);
    let (reaped, _) = ctx.phase("reap", d, 0, rep, || 0, || b.reap().unwrap());
    if reaped.freed_pages != written || reaped.deferred {
        not_a_result(&format!(
            "addcol D={d}: wrote {written} arena pages, the reap freed {} (deferred {})",
            reaped.freed_pages, reaped.deferred
        ));
    }
    let end = snap(db);
    if end.arena_in_use != before.arena_in_use || end.live != before.live {
        not_a_result("the addcol rep leaked");
    }
    println!("# addcol D={d} rep={rep} arena_pages_written={written}");
    el
}

fn arm_addcol(ctx: &Ctx, trunk: &Arc<Connection>, args: &Args) {
    println!("{HEADER}");
    for (i, &d) in args.d_list.iter().enumerate() {
        for rep in 0..reps_for(args, i) {
            let el = addcol_txn(ctx, trunk, d, rep);
            if ctx.timing {
                println!(
                    "# addcol D={d} rep={rep} ns_per_leaf alter+update+commit={:.1} rss_bytes={}",
                    el.as_nanos() as f64 / d as f64,
                    rss_bytes()
                );
            } else {
                println!("# addcol D={d} rep={rep} rss_bytes={}", rss_bytes());
            }
        }
    }
}

fn reps_for(args: &Args, i: usize) -> usize {
    if i + 1 == args.d_list.len() {
        args.reps_last
    } else {
        args.reps
    }
}

fn arm_branch(ctx: &Ctx, trunk: &Arc<Connection>, args: &Args) {
    println!("{HEADER}");
    for (i, &d) in args.d_list.iter().enumerate() {
        for rep in 0..reps_for(args, i) {
            let (tu, tc) = big_branch_txn(ctx, trunk, d, 0, rep, args.autocommit);
            if ctx.timing {
                println!(
                    "# D={d} rep={rep} ns_per_page update={:.1} commit={:.1} rss_bytes={}",
                    tu.as_nanos() as f64 / d as f64,
                    tc.as_nanos() as f64 / d as f64,
                    rss_bytes()
                );
            } else {
                println!("# D={d} rep={rep} rss_bytes={}", rss_bytes());
            }
        }
    }
}

fn arm_trunk(ctx: &Ctx, trunk: &Arc<Connection>, args: &Args) {
    // The control times the pager's large transaction, not a backfill of it.
    trunk.wal_auto_actions_disable();
    println!("{HEADER}");
    let mut gen = 0u32;
    for (i, &d) in args.d_list.iter().enumerate() {
        for rep in 0..reps_for(args, i) {
            let mark = if gen % 2 == 0 { 'C' } else { 'E' };
            gen += 1;
            let children: Vec<Branch> = (0..args.trunk_children)
                .map(|_| trunk.fork_branch().unwrap())
                .collect();
            let before = snap(&ctx.db);
            let sql = format!("UPDATE t SET v = '{mark}' || substr(v, 2) WHERE id <= {d}");
            let (tu, tc) = if args.autocommit {
                let (_, t) = ctx.phase("update+commit", d, 0, rep, || trunk.page_cache_len(), || {
                    trunk.execute(&sql).unwrap()
                });
                (t, Duration::ZERO)
            } else {
                trunk.execute("BEGIN").unwrap();
                let (_, tu) = ctx.phase("update", d, 0, rep, || trunk.page_cache_len(), || {
                    trunk.execute(&sql).unwrap()
                });
                let (_, tc) = ctx.phase("commit", d, 0, rep, || trunk.page_cache_len(), || {
                    trunk.execute("COMMIT").unwrap()
                });
                (tu, tc)
            };
            let after = snap(&ctx.db);
            let retained = after.arena_in_use - before.arena_in_use;
            let want = if args.trunk_children > 0 { d } else { 0 };
            if retained != want {
                not_a_result(&format!("trunk commit of D={d} retained {retained}, expected {want}"));
            }
            let marked = count_marked(trunk, d, mark);
            if marked != d as i64 {
                not_a_result(&format!("trunk reads {marked} rewritten rows of {d}"));
            }
            for child in &children {
                let cc = child.connect().unwrap();
                if count_marked(&cc, d, mark) != 0 {
                    not_a_result("a child forked before the trunk write sees it");
                }
            }
            for child in children {
                child.reap().unwrap();
            }
            trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            let end = snap(&ctx.db);
            if end.arena_in_use != 0 || end.live != 0 {
                not_a_result(&format!("trunk rep leaked: arena {} live {}", end.arena_in_use, end.live));
            }
            if ctx.timing {
                println!(
                    "# D={d} rep={rep} ns_per_page update={:.1} commit={:.1} rss_bytes={}",
                    tu.as_nanos() as f64 / d as f64,
                    tc.as_nanos() as f64 / d as f64,
                    rss_bytes()
                );
            } else {
                println!("# D={d} rep={rep} rss_bytes={}", rss_bytes());
            }
        }
    }
}

/// Phases of one probe rep, as the main thread announces them: the probe reads the counter before
/// and after each op, so an op overlaps phase k exactly when `before <= k <= after`.
const PH_UPDATE: u64 = 1;
const PH_COMMIT: u64 = 2;

/// What the probe thread keeps: bounded, so that the harness's memory does not grow with the run
/// (a first version kept every sample, ~5e8 per rep).
struct ProbeStats {
    samples: u64,
    max_ns: u64,
    max_update_ns: u64,
    max_commit_ns: u64,
    /// Latency histogram: bucket = floor(8 * log2(ns)), so each octave has eight buckets.
    hist: Vec<u64>,
}

impl ProbeStats {
    fn new() -> Self {
        Self {
            samples: 0,
            max_ns: 0,
            max_update_ns: 0,
            max_commit_ns: 0,
            hist: vec![0; 8 * 64],
        }
    }

    fn record(&mut self, ns: u64, before: u64, after: u64) {
        self.samples += 1;
        self.max_ns = self.max_ns.max(ns);
        if before <= PH_UPDATE && PH_UPDATE <= after {
            self.max_update_ns = self.max_update_ns.max(ns);
        }
        if before <= PH_COMMIT && PH_COMMIT <= after {
            self.max_commit_ns = self.max_commit_ns.max(ns);
        }
        let last = self.hist.len() - 1;
        let b = ((ns.max(1) as f64).log2() * 8.0) as usize;
        self.hist[b.min(last)] += 1;
    }

    /// The lower edge (ns) of the bucket holding the p-th percentile.
    fn pct(&self, p: f64) -> f64 {
        let want = ((p / 100.0) * self.samples as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (b, &c) in self.hist.iter().enumerate() {
            seen += c;
            if seen >= want {
                return 2f64.powf(b as f64 / 8.0);
            }
        }
        f64::NAN
    }
}

fn arm_probe(ctx: &Ctx, trunk: &Arc<Connection>, args: &Args) {
    let d = args.d_list[0];
    let mut others: Vec<Branch> = Vec::new();
    let probe_branch = trunk.fork_branch().unwrap();
    let probe_conn = match args.probe {
        ProbeKind::Write1 => {
            let pc = probe_branch.connect().unwrap();
            // The probe's page is the branch's own before any rep, so a rep's writes rewrite it and
            // leave the arena's count where it was.
            pc.execute("UPDATE t SET v = 'P' || substr(v, 2) WHERE id = 1")
                .unwrap();
            Some(pc)
        }
        ProbeKind::ForkReap => None,
    };
    println!("{HEADER}");
    for &n in &args.n_list {
        let grow_start = Instant::now();
        while others.len() < n {
            let i = others.len();
            let b = trunk.fork_branch().unwrap();
            {
                let c = b.connect().unwrap();
                let row = (i % args.rows) as i64 + 1;
                c.execute(format!("UPDATE t SET v = 'N' || substr(v, 2) WHERE id = {row}"))
                    .unwrap();
            }
            others.push(b);
        }
        // Descending --n: reap the most recent branches down to N (each drop is a reap).
        while others.len() > n {
            others.pop();
        }
        let s = snap(&ctx.db);
        println!(
            "# N={n} live={} arena_in_use={} rss_bytes={} grow_s={:.1}",
            s.live,
            s.arena_in_use,
            rss_bytes(),
            grow_start.elapsed().as_secs_f64()
        );
        for rep in 0..args.reps {
            let stop = Arc::new(AtomicBool::new(false));
            let phase = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let before = snap(&ctx.db);
            let handle = {
                let (stop, phase) = (stop.clone(), phase.clone());
                let kind = args.probe;
                let pb = match kind {
                    ProbeKind::ForkReap => Some(probe_branch.fork().unwrap()),
                    ProbeKind::Write1 => None,
                };
                let pc = probe_conn.clone();
                std::thread::spawn(move || {
                    let mut st = ProbeStats::new();
                    let mut g = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let ph0 = phase.load(Ordering::Acquire);
                        let t0 = Instant::now();
                        match kind {
                            ProbeKind::ForkReap => {
                                let c = pb.as_ref().unwrap().fork().unwrap();
                                drop(c);
                            }
                            ProbeKind::Write1 => {
                                let m = if g % 2 == 0 { 'P' } else { 'Q' };
                                pc.as_ref()
                                    .unwrap()
                                    .execute(format!(
                                        "UPDATE t SET v = '{m}' || substr(v, 2) WHERE id = 1"
                                    ))
                                    .unwrap();
                            }
                        }
                        let ns = t0.elapsed().as_nanos() as u64;
                        let ph1 = phase.load(Ordering::Acquire);
                        g += 1;
                        st.record(ns, ph0, ph1);
                    }
                    drop(pb);
                    st
                })
            };
            let c0 = Instant::now();
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            bc.execute("BEGIN").unwrap();
            let sql = format!("UPDATE t SET v = 'B' || substr(v, 2) WHERE id <= {d}");
            phase.store(PH_UPDATE, Ordering::Release);
            let (_, t_update) =
                ctx.phase("update", d, n, rep, || bc.page_cache_len(), || bc.execute(&sql).unwrap());
            phase.store(PH_COMMIT, Ordering::Release);
            let (_, t_commit) = ctx.phase("commit", d, n, rep, || bc.page_cache_len(), || {
                bc.execute("COMMIT").unwrap()
            });
            phase.store(PH_COMMIT + 1, Ordering::Release);
            let marked = count_marked(&bc, d, 'B');
            if marked != d as i64 {
                not_a_result(&format!("branch reads {marked} rewritten rows of {d}"));
            }
            drop(bc);
            let reaped = b.reap().unwrap();
            if reaped.freed_pages != d {
                not_a_result(&format!("reap freed {}", reaped.freed_pages));
            }
            stop.store(true, Ordering::Relaxed);
            let st = handle.join().unwrap();
            let after = snap(&ctx.db);
            if after.arena_in_use != before.arena_in_use {
                not_a_result(&format!(
                    "probe rep leaked arena pages: {} -> {}",
                    before.arena_in_use, after.arena_in_use
                ));
            }
            if ctx.timing && st.samples > 0 {
                println!(
                    "# probe N={n} rep={rep} kind={:?} samples={} p50_us={:.2} p99_us={:.2} max_us={:.1} \
                     max_during_update_us={:.1} max_during_commit_us={:.1} update_ms={:.1} commit_ms={:.1} \
                     wall_s={:.1} (percentiles: lower edge of a 1/8-octave bucket)",
                    args.probe,
                    st.samples,
                    st.pct(50.0) / 1e3,
                    st.pct(99.0) / 1e3,
                    st.max_ns as f64 / 1e3,
                    st.max_update_ns as f64 / 1e3,
                    st.max_commit_ns as f64 / 1e3,
                    t_update.as_secs_f64() * 1e3,
                    t_commit.as_secs_f64() * 1e3,
                    c0.elapsed().as_secs_f64()
                );
            } else {
                println!("# probe N={n} rep={rep} kind={:?} samples={}", args.probe, st.samples);
            }
        }
    }
    drop(probe_conn);
    drop(probe_branch);
    drop(others);
}

fn main() {
    let args = parse_args();
    if args.timing {
        set_hold_timing(true);
    }
    let tmp;
    let dir = match &args.dir {
        Some(d) => {
            std::fs::create_dir_all(d).unwrap();
            d.clone()
        }
        None => {
            tmp = tempfile::TempDir::new().unwrap();
            tmp.path().to_path_buf()
        }
    };
    let (db, trunk) = open_db(&dir, args.durability);
    let build = Instant::now();
    let value = if args.arm == Arm::Addcol {
        addcol_value
    } else {
        trunk_value
    };
    build_table(&trunk, args.rows, value);
    let page_count = int(&trunk, "PRAGMA page_count");
    println!("# branch_bigtxn — Turso fork, lane r11-bigtxn PREREG");
    println!(
        "# arm={:?} d={:?} n={:?} rows={} reps={} reps_last={} timing={} trunk_children={} probe={:?} \
         autocommit={} durability={:?} value_len={VALUE_LEN} page_count={page_count} build_s={:.1}",
        args.arm,
        args.d_list,
        args.n_list,
        args.rows,
        args.reps,
        args.reps_last,
        args.timing,
        args.trunk_children,
        args.probe,
        args.autocommit,
        args.durability,
        build.elapsed().as_secs_f64()
    );
    println!(
        "# clock tick {:.0} ns (Instant); us = one phase's wall time; counters are engine deltas over \
         the phase; max_hold_* are the largest single store-mutex hold inside it",
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
    let ctx = Ctx {
        db: db.clone(),
        timing: args.timing,
    };
    match args.arm {
        Arm::Branch => arm_branch(&ctx, &trunk, &args),
        Arm::Trunk => arm_trunk(&ctx, &trunk, &args),
        Arm::Probe => arm_probe(&ctx, &trunk, &args),
        Arm::Addcol => arm_addcol(&ctx, &trunk, &args),
    }
    let end = db.branch_stats().expect("branch stats");
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown: every branch freed, arena empty ({} free slots)",
        end.arena_slots_free
    );
    drop(trunk);
    drop(db);
    for suffix in ["", "-wal", "-shm", "-branch-arena", "-branch-log", "-branch-snap", "-branch-cat", "-branch-cat-wal", "-branch-cat-shm"] {
        let _ = std::fs::remove_file(dir.join(format!("bigtxn.db{suffix}")));
    }
}
