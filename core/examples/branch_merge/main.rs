//! Merging many branches back into one trunk: r11-merge's harness (frontier/round11/r11-merge/PREREG.md).
//!
//!   cargo run -p turso_core --release --example branch_merge -- --validation key --install replay \
//!       --workload upd --inflight 1000 --merges 100000
//!
//! A FIFO window of `--inflight` L branches: cycle t forks agent t from the trunk, opens it, writes k
//! rows in one transaction and closes it; once t >= L-1 it merges the oldest agent (every g cycles,
//! the g oldest in one batch). So every merge has L-1 merge attempts between its fork and its merge
//! (L-g..L-1 with batches).
//!
//! Workloads (`--workload`):
//!   upd   agent j UPDATEs rows perm[(j*k+i) mod R] to same-length values (disjoint while L*k <= R)
//!   ins   agent j INSERTs k fresh ids into random gaps of the table (disjoint; splits leaves)
//!   rand  agent j UPDATEs k uniformly random rows (true conflicts exist; a correctness arm)
//!
//! The harness keeps its own model of which committed merge wrote each row last. A merge truly
//! conflicts iff a row it wrote was written by a merge committed after its fork. Every merge's
//! row-granular verdict must equal that, the page verdict must include it, and the trunk must end
//! equal to the model and pass `PRAGMA integrity_check`; otherwise `NOT A RESULT`, exit 1.
//!
//! Output: one line per window of M/`--windows` merges (engine counters and the harness's verdict
//! counts). `--timing` adds merge latency and throughput columns; without it the run prints no
//! timing at all, so it may run outside the fleet lock.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::merge::{Install, MergeOutcome, MergePolicy, Merger, Refusal, Validation};
use turso_core::branch::{Branch, BranchWork};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const VALUE_LEN: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Workload {
    Upd,
    Ins,
    Rand,
}

#[derive(Debug)]
struct Args {
    validation: Validation,
    install: Install,
    workload: Workload,
    inflight: usize,
    merges: usize,
    windows: usize,
    rows: usize,
    k: usize,
    g: usize,
    seed: u64,
    synchronous: String,
    timing: bool,
    straggler: bool,
    skip_final_check: bool,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_merge: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut a = Args {
        validation: Validation::KeyStamp,
        install: Install::Replay,
        workload: Workload::Upd,
        inflight: 1000,
        merges: 100_000,
        windows: 10,
        rows: 1_000_000,
        k: 1,
        g: 1,
        seed: 0x9E37_79B9_7F4A_7C15,
        synchronous: "NORMAL".to_string(),
        timing: false,
        straggler: false,
        skip_final_check: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        let num = |s: String| -> usize { s.parse().unwrap_or_else(|_| die("bad number")) };
        match flag.as_str() {
            "--validation" => {
                a.validation = match val().as_str() {
                    "scalar" => Validation::Scalar,
                    "log" => Validation::Log,
                    "page" => Validation::PageStamp,
                    "key" => Validation::KeyStamp,
                    other => die(&format!("unknown validation {other}")),
                }
            }
            "--install" => {
                a.install = match val().as_str() {
                    "phys" => Install::Physical,
                    "replay" => Install::Replay,
                    other => die(&format!("unknown install {other}")),
                }
            }
            "--workload" => {
                a.workload = match val().as_str() {
                    "upd" => Workload::Upd,
                    "ins" => Workload::Ins,
                    "rand" => Workload::Rand,
                    other => die(&format!("unknown workload {other}")),
                }
            }
            "--inflight" => a.inflight = num(val()),
            "--merges" => a.merges = num(val()),
            "--windows" => a.windows = num(val()),
            "--rows" => a.rows = num(val()),
            "--k" => a.k = num(val()),
            "--g" => a.g = num(val()),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--synchronous" => {
                a.synchronous = match val().as_str() {
                    "off" => "OFF",
                    "normal" => "NORMAL",
                    "full" => "FULL",
                    other => die(&format!("unknown --synchronous {other}")),
                }
                .to_string()
            }
            "--timing" => a.timing = true,
            "--straggler" => a.straggler = true,
            "--skip-final-check" => a.skip_final_check = true,
            other => die(&format!("unknown argument {other}")),
        }
    }
    if a.inflight == 0 || a.merges == 0 || a.windows == 0 || a.k == 0 || a.g == 0 || a.rows == 0 {
        die("every count must be positive");
    }
    if a.merges % a.windows != 0 || (a.merges / a.windows) % a.g != 0 {
        die("--windows must divide --merges, and --g must divide a window");
    }
    if a.g > a.inflight {
        die("--g must not exceed --inflight");
    }
    a
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

fn trunk_value(i: i64) -> String {
    format!("t{:0>width$}", i, width = VALUE_LEN - 1)
}

/// Agent j's i-th write: as long as a trunk value, so an UPDATE rewrites the cell in place.
fn agent_value(j: usize, i: usize) -> String {
    format!("a{:0>12}-{:0>width$}", j, i, width = VALUE_LEN - 14)
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

/// What the harness knows about an agent, apart from its branch handle.
struct Plan {
    j: usize,
    /// (row id, write index) for each row it wrote.
    ids: Vec<(i64, usize)>,
    /// Model: merges committed when it forked.
    commits_at_fork: u64,
}

struct Agent {
    branch: Branch,
    plan: Plan,
}

/// The harness's own record of the trunk: for every row a committed merge wrote, the sequence
/// number of the last such merge and what it wrote.
#[derive(Default)]
struct Model {
    commits: u64,
    last: HashMap<i64, (u64, usize, usize)>,
}

impl Model {
    fn conflicts(&self, p: &Plan) -> bool {
        p.ids
            .iter()
            .any(|(id, _)| self.last.get(id).is_some_and(|&(seq, _, _)| seq > p.commits_at_fork))
    }
    fn commit(&mut self, p: &Plan) {
        self.commits += 1;
        for &(id, i) in &p.ids {
            self.last.insert(id, (self.commits, p.j, i));
        }
    }
}

#[derive(Default)]
struct Window {
    attempts: u64,
    committed: u64,
    refused: u64,
    refused_by: HashMap<&'static str, u64>,
    true_conflicts: u64,
    false_refusals: u64,
    obs_scalar: u64,
    obs_page: u64,
    obs_key: u64,
    obs_struct: u64,
    obs_page_false: u64,
    commits_since_fork: u64,
    pages_written: u64,
    rows_written: u64,
    merge_time: Vec<Duration>,
}

fn refusal_name(r: Refusal) -> &'static str {
    match r {
        Refusal::Scope => "scope",
        Refusal::Scalar => "scalar",
        Refusal::Log => "log",
        Refusal::Page => "page",
        Refusal::Key => "key",
        Refusal::Structural => "structural",
    }
}

struct Run {
    args: Args,
    db: Arc<Database>,
    trunk: Arc<Connection>,
    wal_path: PathBuf,
    merger: Merger,
    model: Model,
    rng: Rng,
    perm: Vec<u32>,
    queue: VecDeque<Agent>,
    next_j: usize,
    /// The validator for the unmeasured warm-up merges: the log validator's decisions equal the page
    /// stamps' merge by merge (the model test asserts it), and the page stamps cost O(1) per page.
    warm_validation: Option<Validation>,
}

impl Run {
    /// Agent j's row ids.
    fn ids_for(&mut self, j: usize) -> Vec<(i64, usize)> {
        let (r, k) = (self.args.rows, self.args.k);
        (0..k)
            .map(|i| {
                let x = j * k + i;
                let id = match self.args.workload {
                    Workload::Upd => (i64::from(self.perm[x % r]) + 1) << 20,
                    Workload::Ins => ((i64::from(self.perm[x % r]) + 1) << 20) + 1 + (x / r) as i64,
                    Workload::Rand => ((self.rng.below(r) as i64) + 1) << 20,
                };
                (id, i)
            })
            .collect()
    }

    fn spawn(&mut self) -> Agent {
        let j = self.next_j;
        self.next_j += 1;
        let ids = self.ids_for(j);
        let branch = self.trunk.fork_branch().unwrap();
        let conn = branch.connect().unwrap();
        conn.execute("BEGIN").unwrap();
        for &(id, i) in &ids {
            let v = agent_value(j, i);
            let sql = match self.args.workload {
                Workload::Upd | Workload::Rand => format!("UPDATE t SET v = '{v}' WHERE id = {id}"),
                Workload::Ins => format!("INSERT INTO t VALUES ({id}, '{v}')"),
            };
            conn.execute(sql).unwrap();
        }
        conn.execute("COMMIT").unwrap();
        drop(conn);
        Agent {
            branch,
            plan: Plan {
                j,
                ids,
                commits_at_fork: self.model.commits,
            },
        }
    }

    /// Merge `agents` as one batch and account for every outcome against the model, in order.
    fn merge(&mut self, agents: Vec<Agent>, w: &mut Window) {
        let (branches, plans): (Vec<Branch>, Vec<Plan>) =
            agents.into_iter().map(|a| (a.branch, a.plan)).unzip();
        let policy = MergePolicy {
            validation: self.warm_validation.unwrap_or(self.args.validation),
            install: self.args.install,
        };
        let t = Instant::now();
        let outcomes = self.merger.merge_batch(branches, policy).unwrap();
        let took = t.elapsed();
        if self.args.timing {
            // One sample per member: the batch's time divided evenly.
            let each = took / outcomes.len() as u32;
            w.merge_time.extend(std::iter::repeat_n(each, outcomes.len()));
        }
        for (o, p) in outcomes.iter().zip(&plans) {
            self.account(o, p, w);
        }
    }

    fn account(&mut self, o: &MergeOutcome, p: &Plan, w: &mut Window) {
        let truth = self.model.conflicts(p);
        if o.key_conflict != truth {
            not_a_result(&format!(
                "agent {}: row verdict {} but the model says {truth}: {o:?}",
                p.j, o.key_conflict
            ));
        }
        if o.key_conflict && !o.page_conflict {
            not_a_result(&format!("agent {}: a row conflict without a page conflict: {o:?}", p.j));
        }
        // Upper bounds (A6): a page conflict needs a trunk write since the fork; the log and the page
        // stamps decide identically.
        if o.page_conflict && !o.scalar_conflict {
            not_a_result(&format!("agent {}: a page conflict without a trunk write since the fork: {o:?}", p.j));
        }
        if o.log_conflict.is_some_and(|log| log != o.page_conflict) {
            not_a_result(&format!("agent {}: the log and the page stamps disagree: {o:?}", p.j));
        }
        if o.rows_written != p.ids.len() {
            not_a_result(&format!("agent {}: {} rows written, planned {}", p.j, o.rows_written, p.ids.len()));
        }
        if let Some(r) = o.refused {
            if r == Refusal::Scope {
                not_a_result(&format!("agent {}: refused as out of scope: {o:?}", p.j));
            }
            w.refused += 1;
            *w.refused_by.entry(refusal_name(r)).or_default() += 1;
            if !truth {
                w.false_refusals += 1;
            }
        } else {
            if truth {
                not_a_result(&format!("agent {}: committed a true conflict: {o:?}", p.j));
            }
            w.committed += 1;
            self.model.commit(p);
        }
        w.attempts += 1;
        w.true_conflicts += u64::from(truth);
        w.obs_scalar += u64::from(o.scalar_conflict);
        w.obs_page += u64::from(o.page_conflict);
        w.obs_key += u64::from(o.key_conflict);
        w.obs_struct += u64::from(o.structural_conflict == Some(true));
        w.obs_page_false += u64::from(o.page_conflict && !truth);
        w.commits_since_fork += o.commits_since_fork;
        w.pages_written += o.pages_written as u64;
        w.rows_written += o.rows_written as u64;
    }

    fn work(&self) -> BranchWork {
        self.db.branch_stats().work
    }
}

fn print_window(run: &Run, index: usize, merged_total: usize, w: &mut Window, a: BranchWork, b: BranchWork, wall: Duration) {
    let n = w.attempts.max(1) as f64;
    let d = |f: fn(&BranchWork) -> u64| f(&b) - f(&a);
    let s = run.db.branch_stats();
    let refused_by: Vec<String> = {
        let mut v: Vec<_> = w.refused_by.iter().collect();
        v.sort();
        v.into_iter().map(|(k, c)| format!("{k}:{c}")).collect()
    };
    print!(
        "{index}\t{merged_total}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t{:.4}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}\t{}\t{}\t{}\t{}\t{}",
        w.attempts,
        w.committed,
        w.refused,
        w.true_conflicts,
        w.false_refusals,
        w.obs_scalar,
        w.obs_page,
        w.obs_key,
        w.obs_struct,
        w.obs_page_false,
        w.false_refusals as f64 / n,
        w.obs_page_false as f64 / n,
        w.commits_since_fork as f64 / n,
        w.pages_written as f64 / n,
        w.rows_written as f64 / n,
        d(|w| w.merge_probes) as f64 / n,
        d(|w| w.merge_log_entries_scanned) as f64 / n,
        d(|w| w.merge_structural_probes) as f64 / n,
        (d(|w| w.merge_pages_installed) + d(|w| w.merge_rows_installed)) as f64 / n,
        d(|w| w.trunk_commits),
        s.merge_log_entries,
        s.row_stamps,
        s.live_branches,
        s.arena_slots_in_use,
        if refused_by.is_empty() { "-".to_string() } else { refused_by.join(",") },
    );
    print!(
        "\t{}\t{}",
        rss_bytes(),
        std::fs::metadata(&run.wal_path).map_or(0, |m| m.len())
    );
    if run.args.timing {
        let mut us: Vec<f64> = w.merge_time.iter().map(|t| t.as_secs_f64() * 1e6).collect();
        us.sort_by(|x, y| x.partial_cmp(y).unwrap());
        let merge_s: f64 = us.iter().sum::<f64>() / 1e6;
        print!(
            "\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.0}\t{:.0}\t{:.0}",
            percentile(&us, 50.0),
            percentile(&us, 90.0),
            percentile(&us, 99.0),
            us[us.len() - 1],
            w.attempts as f64 / merge_s,
            w.committed as f64 / merge_s,
            w.attempts as f64 / wall.as_secs_f64(),
        );
    }
    println!();
}

const HEADER: &str = "window\tmerged_total\tattempts\tcommitted\trefused\ttrue_conflicts\tfalse_refusals\tobs_scalar\tobs_page\tobs_key\tobs_struct\tobs_page_false\tfalse_refusal_rate\tpage_false_rate\tcommits_since_fork\tpages_written\trows_written\tprobes_per_merge\tlog_entries_per_merge\tstruct_probes_per_merge\tinstalled_per_merge\ttrunk_commits\tlog_entries_held\trow_stamps_held\tlive_branches\tarena_in_use\trefused_by\trss_bytes\twal_bytes";
const TIMING_HEADER: &str = "\tmerge_p50_us\tmerge_p90_us\tmerge_p99_us\tmerge_max_us\tmerges_per_s_merge_time\tcommits_per_s_merge_time\tattempts_per_s_wall";

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_merge.db");
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
    trunk
        .execute(format!("PRAGMA synchronous = {}", args.synchronous))
        .unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let setup = Instant::now();
    trunk.execute("BEGIN").unwrap();
    {
        let mut insert = trunk.prepare("INSERT INTO t VALUES (?1, ?2)").unwrap();
        for i in 1..=args.rows as i64 {
            insert.bind_at(1.try_into().unwrap(), Value::from_i64(i << 20)).unwrap();
            insert
                .bind_at(2.try_into().unwrap(), Value::from_text(trunk_value(i)))
                .unwrap();
            insert.run_ignore_rows().unwrap();
            insert.reset().unwrap();
        }
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let setup_s = setup.elapsed().as_secs_f64();
    let int = |sql: &str| {
        trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    let page_count = int("PRAGMA page_count");
    let page_size = int("PRAGMA page_size");
    if args.install == Install::Physical {
        db.set_branch_read_tracking(true);
    }
    let mut rng = Rng(args.seed);
    let mut perm: Vec<u32> = (0..args.rows as u32).collect();
    for i in (1..perm.len()).rev() {
        perm.swap(i, rng.below(i + 1));
    }

    println!("# branch_merge — Turso fork, r11-merge PREREG (frontier/round11/r11-merge/PREREG.md)");
    println!(
        "# validation={:?} install={:?} workload={:?} inflight={} merges={} windows={} rows={} k={} g={} seed={:#x} synchronous={} straggler={} timing={}",
        args.validation, args.install, args.workload, args.inflight, args.merges, args.windows,
        args.rows, args.k, args.g, args.seed, args.synchronous, args.straggler, args.timing
    );
    println!(
        "# trunk page_count={page_count} page_size={page_size} build={}{}",
        if cfg!(debug_assertions) { "DEBUG (not a timing result)" } else { "release" },
        if args.timing { format!(" setup_s={setup_s:.1}") } else { String::new() }
    );
    println!("# rss_base_bytes={}", rss_bytes());

    let merger = Merger::new(trunk.clone()).unwrap();
    let mut run = Run {
        args,
        db: db.clone(),
        trunk,
        wal_path: PathBuf::from(format!("{}-wal", path.to_str().unwrap())),
        merger,
        model: Model::default(),
        rng,
        perm,
        queue: VecDeque::new(),
        next_j: 0,
        warm_validation: None,
    };

    // The straggler: forked first, merged last, pinning the oldest fork epoch throughout.
    let straggler = run.args.straggler.then(|| run.spawn());

    // Warm-up: fill the window.
    let warm = Instant::now();
    while run.queue.len() < run.args.inflight - 1 {
        let a = run.spawn();
        run.queue.push_back(a);
    }
    // Warm-up 2: L-1 merge cycles, unmeasured, so that every measured merge has exactly L-1 merge
    // attempts between its fork and its merge (L-g..L-1 in batches). The fill's agents were forked
    // before any merge, so without this the first L-1 measured merges would see 0..L-2.
    run.warm_validation = (run.args.validation == Validation::Log).then_some(Validation::PageStamp);
    let mut warm_window = Window::default();
    let g = run.args.g;
    let mut warm_merges = 0;
    while warm_merges + g <= run.args.inflight - 1 {
        for _ in 0..g {
            let a = run.spawn();
            run.queue.push_back(a);
        }
        let batch: Vec<Agent> = (0..g).map(|_| run.queue.pop_front().unwrap()).collect();
        run.merge(batch, &mut warm_window);
        warm_merges += g;
    }
    run.warm_validation = None;
    let warm_s = warm.elapsed().as_secs_f64();
    let s = run.db.branch_stats();
    println!(
        "# warm-up: {} in flight, {} warm-up merges ({} committed), live_branches={} arena_in_use={} rss_bytes={}{}",
        run.queue.len(),
        warm_window.attempts,
        warm_window.committed,
        s.live_branches,
        s.arena_slots_in_use,
        rss_bytes(),
        if run.args.timing { format!(" warm_s={warm_s:.1}") } else { String::new() }
    );
    println!(
        "{HEADER}{}",
        if run.args.timing { TIMING_HEADER } else { "" }
    );

    let per_window = run.args.merges / run.args.windows;
    let mut merged = 0usize;
    for index in 0..run.args.windows {
        let mut w = Window::default();
        let before = run.work();
        let t = Instant::now();
        let mut done = 0;
        while done < per_window {
            // g cycles: fork and write g agents, then merge the g oldest in one batch.
            for _ in 0..g {
                let a = run.spawn();
                run.queue.push_back(a);
            }
            let batch: Vec<Agent> = (0..g).map(|_| run.queue.pop_front().unwrap()).collect();
            run.merge(batch, &mut w);
            done += g;
        }
        merged += done;
        let wall = t.elapsed();
        let after = run.work();
        print_window(&run, index, merged, &mut w, before, after, wall);
    }

    if let Some(a) = straggler {
        let mut w = Window::default();
        let before = run.work();
        run.merge(vec![a], &mut w);
        let after = run.work();
        println!(
            "# straggler: committed={} refused_by={:?} commits_since_fork={} probes={} log_entries_scanned={} log_entries_held_before_merge_window_end={} row_stamps_held={}",
            w.committed,
            w.refused_by,
            w.commits_since_fork,
            after.merge_probes - before.merge_probes,
            after.merge_log_entries_scanned - before.merge_log_entries_scanned,
            run.db.branch_stats().merge_log_entries,
            run.db.branch_stats().row_stamps,
        );
    }

    // Drain: the agents still in flight are released unmerged.
    run.queue.clear();
    let end = run.db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }

    // The trunk against the model: every row a committed merge wrote holds that merge's value, and
    // (for upd/rand) every other row its trunk value.
    if !run.args.skip_final_check {
        let rows = run
            .trunk
            .prepare("SELECT id, v FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let mut seen_written = 0usize;
        for r in &rows {
            let (Some(id), Value::Text(v)) = (r[0].as_int(), &r[1]) else {
                not_a_result(&format!("unexpected row {r:?}"));
            };
            let expect = match run.model.last.get(&id) {
                Some(&(_, j, i)) => {
                    seen_written += 1;
                    agent_value(j, i)
                }
                None if id & ((1 << 20) - 1) == 0 => trunk_value(id >> 20),
                None => not_a_result(&format!("row {id} exists but no committed merge wrote it")),
            };
            if v.as_str() != expect {
                not_a_result(&format!("row {id}: trunk has {}, model says {expect}", v.as_str()));
            }
        }
        if seen_written != run.model.last.len() {
            not_a_result(&format!(
                "{} rows written by committed merges, {} found on the trunk",
                run.model.last.len(),
                seen_written
            ));
        }
        let expected_rows = match run.args.workload {
            Workload::Ins => run.args.rows + run.model.last.len(),
            _ => run.args.rows,
        };
        if rows.len() != expected_rows {
            not_a_result(&format!("trunk has {} rows, model {expected_rows}", rows.len()));
        }
        let ic = run
            .trunk
            .prepare("PRAGMA integrity_check")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let ok = matches!(ic.as_slice(), [row] if matches!(&row[0], Value::Text(t) if t.as_str() == "ok"));
        if !ok {
            not_a_result(&format!("integrity_check: {ic:?}"));
        }
        println!(
            "# final check: {} trunk rows equal the model ({} written by committed merges); integrity_check ok",
            rows.len(),
            run.model.last.len()
        );
    }
    println!("# teardown: every branch freed, arena empty ({} free slots)", end.arena_slots_free);
}
