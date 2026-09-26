//! The git-hosting shape against N long-lived branches of the durable branch store (githost-shape
//! lane, round 11; PREREG in artie-research frontier/round11/githost-shape/PREREG.md, Amendment G1).
//!
//!   branch_githost grow  --db PATH --to N [--page-size S]
//!   branch_githost probe --db PATH --n N [--label L]
//!
//! One deterministic schedule of steps s = 1, 2, ... (single-threaded). Step s:
//!   1. NEW PR: fork trunk child id = s; it UPDATEs two rows on two leaves (row_for(s), far_row(..)) in one
//!      transaction; the handle is detached (`into_id`): the branch idles.
//!   2. MERGE (s mod 25 < 21, i.e. 0.84 per new PR): one trunk UPDATE of merge_row(s).
//!   3. PR UPDATE (s mod 4 == 0): one branch among the newest ceil(0.0237 s) ids rewrites row_for(its id);
//!      skipped if that branch is dead.
//!   4. DEATH (s mod 50 == 0): a uniformly chosen id < s is reaped; skipped if already dead.
//! The model (trunk row values by merge step, dead set, update generations) is rebuilt in every process by
//! replaying the schedule in memory; `<db>.githost` holds only the step count.
//!
//! `grow` opens Durable{sync:false} (same file bytes; no branch fsyncs), runs steps until the model's live
//! count reaches N, checks the invariants, and closes cleanly. It prints counters (and unlocked times,
//! which are NOT timing evidence).
//!
//! `probe` opens Durable{sync:true} in a fresh process (the open is the restart at N) and measures:
//! the open; K_s = min(2500, N/10) further schedule steps, each op kind timed and counted separately;
//! read_old (200 uniform live), read_oldest (the 50 smallest live ids), list (5), diff_random (200) and
//! diff_oldest (50) by D-written, 20 of each cross-checked against D-scan; compact (2); the close.
//!
//! Every value read is checked against the model, and the arena's exact slot count against
//! 2 x live + trunk retained. Any failed check prints `NOT A RESULT` and exits 1.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{BranchDurability, BranchId, BranchOpenStats, BranchShape, BranchWork};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const ROWS: u64 = 20_000;
const VALUE_LEN: usize = 100;
const OPEN_WINDOW: f64 = 0.0237;

fn die(msg: &str) -> ! {
    eprintln!("branch_githost: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    let _ = std::io::stdout().flush();
    std::process::exit(1)
}

struct Args {
    cmd: String,
    db: PathBuf,
    n: u64,
    page_size: u64,
    label: String,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_githost grow|probe ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        page_size: 1024,
        label: String::new(),
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--page-size" => args.page_size = val().parse().unwrap_or_else(|_| die("bad --page-size")),
            "--label" => args.label = val(),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.db.as_os_str().is_empty() || args.n == 0 {
        die("--db and --n/--to are required");
    }
    args
}

// ---------------------------------------------------------------------------------------------
// The schedule: a pure function of the step number.

fn mix(mut x: u64) -> u64 {
    // splitmix64
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn row_for(id: u64) -> u64 {
    id.wrapping_mul(2_654_435_761) % ROWS + 1
}

/// A row half the table away: another leaf.
fn far_row(row: u64) -> u64 {
    (row - 1 + ROWS / 2) % ROWS + 1
}

fn is_merge(s: u64) -> bool {
    s % 25 < 21
}

fn merge_row(s: u64) -> u64 {
    mix(s ^ 0xA5A5_0001) % ROWS + 1
}

fn update_target(s: u64) -> Option<u64> {
    if s % 4 != 0 {
        return None;
    }
    let window = ((OPEN_WINDOW * s as f64).ceil() as u64).max(1);
    let back = mix(s ^ 0xA5A5_0002) % window;
    (s > back + 1).then(|| s - 1 - back)
}

fn death_target(s: u64) -> Option<u64> {
    if s % 50 != 0 || s < 2 {
        return None;
    }
    Some(1 + mix(s ^ 0xA5A5_0003) % (s - 1))
}

fn pad(prefix: char, n: u64) -> String {
    format!("{prefix}{:0>width$}", n, width = VALUE_LEN - 1)
}

fn trunk_initial(row: u64) -> String {
    format!("{:0>width$}", row, width = VALUE_LEN)
}

fn merge_value(s: u64) -> String {
    pad('t', s)
}

fn own_value(id: u64, gen: u64) -> String {
    format!("b{id:012}g{gen:012}{}", "x".repeat(VALUE_LEN - 26))
}

fn far_value(id: u64) -> String {
    pad('c', id)
}

/// Everything a process knows about the logical state, rebuilt by replaying the schedule.
struct Model {
    steps: u64,
    dead: Vec<bool>,
    gen: Vec<u64>,
    /// row -> ascending steps whose merge wrote it.
    merges: Vec<Vec<u64>>,
    live: u64,
    updates_skipped: u64,
    deaths_skipped: u64,
}

/// What a step does, decided by the model before the store is touched.
struct StepPlan {
    s: u64,
    merge: Option<u64>,
    update: Option<(u64, u64)>,
    death: Option<u64>,
}

impl Model {
    fn new() -> Self {
        Self {
            steps: 0,
            dead: vec![false],
            gen: vec![0],
            merges: vec![Vec::new(); ROWS as usize + 1],
            live: 0,
            updates_skipped: 0,
            deaths_skipped: 0,
        }
    }

    fn replay(steps: u64) -> Self {
        let mut m = Self::new();
        for _ in 0..steps {
            m.advance();
        }
        m
    }

    /// Apply the next step to the model and say what the store must do.
    fn advance(&mut self) -> StepPlan {
        let s = self.steps + 1;
        self.steps = s;
        self.dead.push(false);
        self.gen.push(0);
        self.live += 1;
        let merge = is_merge(s).then(|| {
            let r = merge_row(s);
            self.merges[r as usize].push(s);
            r
        });
        let update = match update_target(s) {
            Some(t) if !self.dead[t as usize] => {
                self.gen[t as usize] += 1;
                Some((t, self.gen[t as usize]))
            }
            Some(_) => {
                self.updates_skipped += 1;
                None
            }
            None => None,
        };
        let death = match death_target(s) {
            Some(t) if !self.dead[t as usize] => {
                self.dead[t as usize] = true;
                self.live -= 1;
                Some(t)
            }
            Some(_) => {
                self.deaths_skipped += 1;
                None
            }
            None => None,
        };
        StepPlan {
            s,
            merge,
            update,
            death,
        }
    }

    /// The trunk's value of `row` as a branch forked at step `f` sees it: the latest merge before `f`.
    fn trunk_at(&self, row: u64, f: u64) -> String {
        let ms = &self.merges[row as usize];
        let i = ms.partition_point(|&m| m < f);
        if i == 0 {
            trunk_initial(row)
        } else {
            merge_value(ms[i - 1])
        }
    }

    fn live_ids(&self) -> Vec<u64> {
        (1..=self.steps).filter(|&id| !self.dead[id as usize]).collect()
    }
}

// ---------------------------------------------------------------------------------------------
// Store access.

fn open_db(path: &Path, sync: bool) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync }),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")))
}

fn sidecar(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}.githost", db.to_str().unwrap()))
}

fn read_steps(db: &Path) -> u64 {
    match std::fs::read_to_string(sidecar(db)) {
        Ok(s) => s
            .trim()
            .strip_prefix("steps=")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| not_a_result("unparseable sidecar")),
        Err(_) => 0,
    }
}

fn write_steps(db: &Path, steps: u64) {
    std::fs::write(sidecar(db), format!("steps={steps}\n")).unwrap_or_else(|e| not_a_result(&format!("sidecar: {e}")));
}

fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap()
}

fn read_v(conn: &Arc<Connection>, row: u64) -> String {
    let mut stmt = conn.prepare(format!("SELECT v FROM t WHERE id = {row}")).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [r] => match &r[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {row}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {row}: {} rows", rows.len())),
    }
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| not_a_result(&format!("{sql:.60}: {e}")));
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

fn files_line(db: &Path) -> String {
    let with = |suffix: &str| PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
    format!(
        "db_bytes={} wal_bytes={} log_bytes={} snap_bytes={} arena_bytes={}",
        size_of(db),
        size_of(&with("-wal")),
        size_of(&with("-branch-log")),
        size_of(&with("-branch-snap")),
        size_of(&with("-branch-arena"))
    )
}

fn shape_line(s: &BranchShape) -> String {
    format!(
        "compactions={} snap_bytes_w={} snap_entries={} compact_ns={} resize_events={} resize_moved={} \
         table_len={} table_cap={} ids_calls={} ids_visited={} trunk_retained={} trunk_version_pages={} \
         trunk_versions_max={} trunk_written={} trunk_children={} log_len={} snapshot_len={}",
        s.compactions,
        s.snap_bytes,
        s.snap_entries,
        s.compact_ns,
        s.resize_events,
        s.resize_moved,
        s.table_len,
        s.table_capacity,
        s.ids_calls,
        s.ids_visited,
        s.trunk_retained,
        s.trunk_version_pages,
        s.trunk_versions_max,
        s.trunk_written,
        s.trunk_children,
        s.log_len,
        s.snapshot_len
    )
}

fn open_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} arena_high_water={} \
         arena_free={} derived_map_inserts={} recover_us={:.1} load_us={:.1} replay_us={:.1} collect_us={:.1} \
         referenced_us={:.1} arena_us={:.1} expire_us={:.1} store_total_us={:.1}",
        s.snap_bytes,
        s.log_bytes,
        s.records,
        s.snap_branches,
        s.branches,
        s.current_entries,
        s.retained_entries,
        s.trunk_retained,
        s.trunk_children,
        s.referenced_slots,
        s.arena_high_water,
        s.arena_free,
        s.derived_map_inserts,
        s.recover_ns as f64 / 1e3,
        s.load_ns as f64 / 1e3,
        s.replay_ns as f64 / 1e3,
        s.collect_ns as f64 / 1e3,
        s.referenced_ns as f64 / 1e3,
        s.arena_ns as f64 / 1e3,
        s.expire_ns as f64 / 1e3,
        s.total_ns as f64 / 1e3
    )
}

/// The counters one operation is charged: deltas of every integer the store exposes.
#[derive(Clone, Copy)]
struct Snap {
    work: BranchWork,
    reads: (u64, u64),
    in_use: i64,
    shape: BranchShape,
}

fn snap(db: &Arc<Database>) -> Snap {
    let st = db.branch_stats().unwrap();
    Snap {
        work: st.work,
        reads: db.branch_read_counters(),
        in_use: st.arena_slots_in_use as i64,
        shape: db.branch_shape(),
    }
}

const COUNTERS: [&str; 12] = [
    "resolve_calls",
    "resolve_levels",
    "retained_examined",
    "gc_examined",
    "gc_range_entries",
    "arena_reads",
    "arena_delta",
    "compactions",
    "snap_entries",
    "resize_events",
    "resize_moved",
    "ids_visited",
];

fn delta(a: &Snap, b: &Snap) -> [i64; 12] {
    let d = |x: u64, y: u64| y as i64 - x as i64;
    [
        d(a.work.resolve_calls, b.work.resolve_calls),
        d(a.work.resolve_levels, b.work.resolve_levels),
        d(a.work.resolve_retained_examined, b.work.resolve_retained_examined),
        d(a.work.gc_examined, b.work.gc_examined),
        d(a.work.gc_range_entries, b.work.gc_range_entries),
        d(a.reads.1, b.reads.1),
        b.in_use - a.in_use,
        d(a.shape.compactions, b.shape.compactions),
        d(a.shape.snap_entries, b.shape.snap_entries),
        d(a.shape.resize_events, b.shape.resize_events),
        d(a.shape.resize_moved, b.shape.resize_moved),
        d(a.shape.ids_visited, b.shape.ids_visited),
    ]
}

/// Per-op times (us) and counter deltas for one op kind.
#[derive(Default)]
struct Series {
    times: Vec<f64>,
    counters: Vec<[i64; 12]>,
    extra: Vec<Vec<(String, i64)>>,
}

impl Series {
    fn push(&mut self, t_us: f64, c: [i64; 12]) {
        self.times.push(t_us);
        self.counters.push(c);
    }

    fn print(&mut self, op: &str, n: u64, label: &str) {
        if self.times.is_empty() {
            println!("OP\tn={n}\tlabel={label}\top={op}\tk=0");
            return;
        }
        let mut t = self.times.clone();
        t.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| t[((p / 100.0) * (t.len() - 1) as f64).round() as usize];
        let mut line = format!(
            "OP\tn={n}\tlabel={label}\top={op}\tk={}\tp50_us={:.2}\tp90_us={:.2}\tp99_us={:.2}\tmax_us={:.2}\tmean_us={:.2}",
            t.len(),
            pct(50.0),
            pct(90.0),
            pct(99.0),
            t[t.len() - 1],
            t.iter().sum::<f64>() / t.len() as f64
        );
        for (i, name) in COUNTERS.iter().enumerate() {
            let v: Vec<i64> = self.counters.iter().map(|c| c[i]).collect();
            let sum: i64 = v.iter().sum();
            line.push_str(&format!(
                "\t{name}_mean={:.3}\t{name}_min={}\t{name}_max={}",
                sum as f64 / v.len() as f64,
                v.iter().min().unwrap(),
                v.iter().max().unwrap()
            ));
        }
        if let Some(first) = self.extra.first() {
            for (j, (name, _)) in first.iter().enumerate() {
                let v: Vec<i64> = self.extra.iter().map(|e| e[j].1).collect();
                line.push_str(&format!(
                    "\t{name}_mean={:.3}\t{name}_min={}\t{name}_max={}",
                    v.iter().sum::<i64>() as f64 / v.len() as f64,
                    v.iter().min().unwrap(),
                    v.iter().max().unwrap()
                ));
            }
        }
        println!("{line}");
    }
}

/// The store-level step: what `plan` says, against the store. Returns the time of each op kind done.
struct Ops {
    new_pr: Series,
    merge: Series,
    pr_update: Series,
    reap: Series,
}

fn do_new_pr(db: &Arc<Database>, trunk: &Arc<Connection>, s: u64) {
    let branch = trunk.fork_branch().unwrap_or_else(|e| not_a_result(&format!("fork {s}: {e}")));
    if branch.id() != BranchId(s) {
        not_a_result(&format!("fork returned {:?}, expected {s}", branch.id()));
    }
    let conn = branch.connect().unwrap();
    exec(&conn, "BEGIN");
    let row = row_for(s);
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {row}", own_value(s, 0)));
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {}", far_value(s), far_row(row)));
    exec(&conn, "COMMIT");
    drop(conn);
    let _ = branch.into_id();
    let _ = db;
}

fn do_merge(trunk: &Arc<Connection>, s: u64, row: u64) {
    exec(trunk, &format!("UPDATE t SET v = '{}' WHERE id = {row}", merge_value(s)));
}

fn do_update(db: &Arc<Database>, t: u64, gen: u64) {
    let branch = db
        .branch(BranchId(t))
        .unwrap_or_else(|e| not_a_result(&format!("attach {t} for update: {e}")));
    let conn = branch.connect().unwrap();
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {}", own_value(t, gen), row_for(t)));
    drop(conn);
    let _ = branch.into_id();
}

fn do_reap(db: &Arc<Database>, t: u64) {
    let branch = db
        .branch(BranchId(t))
        .unwrap_or_else(|e| not_a_result(&format!("attach {t} for reap: {e}")));
    let r = branch.reap().unwrap_or_else(|e| not_a_result(&format!("reap {t}: {e}")));
    if r.deferred {
        not_a_result(&format!("reap {t} deferred: nothing should hold an idle PR branch"));
    }
}

/// Run one schedule step; with `ops`, time and count each op kind.
fn step(db: &Arc<Database>, trunk: &Arc<Connection>, model: &mut Model, mut ops: Option<&mut Ops>) {
    let plan = model.advance();
    let mut timed = |series: Option<&mut Series>, f: &mut dyn FnMut()| {
        if let Some(series) = series {
            let a = snap(db);
            let t = Instant::now();
            f();
            let us = t.elapsed().as_secs_f64() * 1e6;
            let b = snap(db);
            series.push(us, delta(&a, &b));
        } else {
            f();
        }
    };
    timed(ops.as_deref_mut().map(|o| &mut o.new_pr), &mut || do_new_pr(db, trunk, plan.s));
    if let Some(row) = plan.merge {
        timed(ops.as_deref_mut().map(|o| &mut o.merge), &mut || do_merge(trunk, plan.s, row));
    }
    if let Some((t, gen)) = plan.update {
        timed(ops.as_deref_mut().map(|o| &mut o.pr_update), &mut || do_update(db, t, gen));
    }
    if let Some(t) = plan.death {
        timed(ops.as_deref_mut().map(|o| &mut o.reap), &mut || do_reap(db, t));
    }
}

/// Exact invariants: live states == the model's live count; every live branch owns exactly two current
/// pages and no branch retains anything (no branch has a child), so the arena holds 2 x live + the trunk's
/// retained versions.
fn check_invariants(db: &Arc<Database>, model: &Model, what: &str) {
    let st = db.branch_stats().unwrap();
    let sh = db.branch_shape();
    if st.live_branches as u64 != model.live {
        not_a_result(&format!("{what}: store has {} branch states, model {}", st.live_branches, model.live));
    }
    let expect = 2 * model.live + sh.trunk_retained;
    if st.arena_slots_in_use as u64 != expect {
        not_a_result(&format!(
            "{what}: arena in use {} != 2 x live {} + trunk retained {}",
            st.arena_slots_in_use, model.live, sh.trunk_retained
        ));
    }
    if sh.trunk_children != model.live {
        not_a_result(&format!("{what}: trunk children {} != live {}", sh.trunk_children, model.live));
    }
}

/// One read probe, planned (expected values computed) before it is timed.
struct ReadPlan {
    id: u64,
    reads: Vec<(u64, String)>,
}

/// A branch's own two rows, and one trunk row that a merge AFTER the fork rewrote (when the schedule has
/// one): the branch must still see the value from before its fork, so a lost pre-image fails the check.
fn plan_read(model: &Model, id: u64, pick: u64) -> ReadPlan {
    let row = row_for(id);
    let mut reads = vec![
        (row, own_value(id, model.gen[id as usize])),
        (far_row(row), far_value(id)),
    ];
    // A step m >= id (the merge of step `id` itself runs after its fork), advanced to the next merge.
    let mut m = id + pick % (model.steps - id + 1);
    while m <= model.steps && !is_merge(m) {
        m += 1;
    }
    let r = if m <= model.steps { merge_row(m) } else { pick % ROWS + 1 };
    if r != row && r != far_row(row) {
        reads.push((r, model.trunk_at(r, id)));
    }
    ReadPlan { id, reads }
}

fn exec_read(db: &Arc<Database>, plan: &ReadPlan) {
    let id = plan.id;
    let branch = db.branch(BranchId(id)).unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
    let conn = branch.connect().unwrap();
    for (row, want) in &plan.reads {
        let got = read_v(&conn, *row);
        if &got != want {
            not_a_result(&format!("branch {id} row {row} = {got:.30}, want {want:.30}"));
        }
    }
    drop(conn);
    let _ = branch.into_id();
}

fn read_branch(db: &Arc<Database>, model: &Model, id: u64, pick: u64) {
    exec_read(db, &plan_read(model, id, pick));
}

fn grow(args: &Args) {
    let steps0 = read_steps(&args.db);
    let db = open_db(&args.db, false);
    let trunk = db.connect().unwrap();
    let exists = !trunk
        .prepare("SELECT name FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty();
    if !exists {
        if steps0 != 0 {
            not_a_result("sidecar has steps but the database has no table");
        }
        exec(&trunk, &format!("PRAGMA page_size = {}", args.page_size));
        exec(&trunk, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
        exec(&trunk, "BEGIN");
        for row in 1..=ROWS {
            exec(&trunk, &format!("INSERT INTO t VALUES ({row}, '{}')", trunk_initial(row)));
        }
        exec(&trunk, "COMMIT");
        exec(&trunk, "PRAGMA wal_checkpoint(TRUNCATE)");
        println!(
            "# fixture created: rows={ROWS} page_size={} page_count={}",
            int(&trunk, "PRAGMA page_size"),
            int(&trunk, "PRAGMA page_count")
        );
    }
    let ps = int(&trunk, "PRAGMA page_size") as u64;
    if ps != args.page_size {
        not_a_result(&format!("page_size {ps}, expected {}", args.page_size));
    }
    // Trunk durability during growth only: the probe opens with the default.
    let _ = trunk.execute("PRAGMA synchronous = OFF");
    let mut model = Model::replay(steps0);
    check_invariants(&db, &model, "grow start");
    let s0 = db.branch_shape();
    let t0 = Instant::now();
    let mut next_report = model.live.next_power_of_two().max(1024);
    while model.live < args.n {
        step(&db, &trunk, &mut model, None);
        if model.live >= next_report {
            let sh = db.branch_shape();
            println!(
                "# grow live={} steps={} elapsed_s={:.1} rss={} {}",
                model.live,
                model.steps,
                t0.elapsed().as_secs_f64(),
                rss_bytes(),
                shape_line(&sh)
            );
            let _ = std::io::stdout().flush();
            next_report *= 2;
        }
    }
    let grow_s = t0.elapsed().as_secs_f64();
    check_invariants(&db, &model, "grow end");
    let sh = db.branch_shape();
    let st = db.branch_stats().unwrap();
    // Untimed spot checks of the model against the store.
    let live = model.live_ids();
    for i in 0..20u64 {
        let id = live[(mix(i ^ model.steps) % live.len() as u64) as usize];
        read_branch(&db, &model, id, mix(i));
    }
    println!(
        "GROW\tn={}\tsteps={}\tfrom_steps={steps0}\tgrow_s_unlocked={grow_s:.1}\tupdates_skipped={}\tdeaths_skipped={}\t\
         arena_in_use={}\tarena_free={}\tpage_count={}\trss={}\t{}\tcompactions_this_run={}\tresize_events_this_run={}",
        model.live,
        model.steps,
        model.updates_skipped,
        model.deaths_skipped,
        st.arena_slots_in_use,
        st.arena_slots_free,
        int(&trunk, "PRAGMA page_count"),
        rss_bytes(),
        shape_line(&sh).replace(' ', "\t"),
        sh.compactions - s0.compactions,
        sh.resize_events - s0.resize_events
    );
    write_steps(&args.db, model.steps);
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!("# grow closed in {:.1} us; {}", t.elapsed().as_secs_f64() * 1e6, files_line(&args.db));
}

fn probe(args: &Args) {
    let steps0 = read_steps(&args.db);
    if steps0 == 0 {
        not_a_result("probe of an ungrown database");
    }
    let files_before = files_line(&args.db);
    let rss0 = rss_bytes();
    let t = Instant::now();
    let db = open_db(&args.db, true);
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let connect_us = t.elapsed().as_secs_f64() * 1e6;
    let rss1 = rss_bytes();
    let mut model = Model::replay(steps0);
    if model.live != args.n {
        not_a_result(&format!("probe --n {} but the model has {} live", args.n, model.live));
    }
    check_invariants(&db, &model, "after open");
    let os = db.branch_open_stats();
    let page_count = int(&trunk, "PRAGMA page_count") as u32;
    let n = model.live;
    let label = &args.label;
    println!(
        "OPEN\tn={n}\tlabel={label}\tsteps={}\topen_us={open_us:.1}\ttrunk_connect_us={connect_us:.1}\trss_before={rss0}\t\
         rss_after_open={rss1}\tpage_count={page_count}\t{}\t{}\tfiles_before: {files_before}",
        model.steps,
        open_line(&os).replace(' ', "\t"),
        shape_line(&db.branch_shape()).replace(' ', "\t")
    );
    let _ = std::io::stdout().flush();

    // Steady state: the schedule continues.
    let k_s = (n / 10).clamp(1, 2500);
    let mut ops = Ops {
        new_pr: Series::default(),
        merge: Series::default(),
        pr_update: Series::default(),
        reap: Series::default(),
    };
    for _ in 0..k_s {
        step(&db, &trunk, &mut model, Some(&mut ops));
    }
    check_invariants(&db, &model, "after steady ops");
    ops.new_pr.print("new_pr", n, label);
    ops.merge.print("merge", n, label);
    ops.pr_update.print("pr_update", n, label);
    ops.reap.print("reap", n, label);

    let live = model.live_ids();
    let timed = |series: &mut Series, f: &mut dyn FnMut() -> Vec<(String, i64)>| {
        let a = snap(&db);
        let t = Instant::now();
        let extra = f();
        let us = t.elapsed().as_secs_f64() * 1e6;
        let b = snap(&db);
        series.push(us, delta(&a, &b));
        series.extra.push(extra);
    };

    // Reads of old branches, planned before they are timed.
    let mut s = Series::default();
    for i in 0..200u64 {
        let id = live[(mix(i ^ 0x51) % live.len() as u64) as usize];
        let plan = plan_read(&model, id, mix(i ^ 0x52));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
    }
    s.print("read_old", n, label);
    let mut s = Series::default();
    for &id in live.iter().take(50) {
        let plan = plan_read(&model, id, mix(id ^ 0x53));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
    }
    s.print("read_oldest", n, label);

    // List, checked against the model's live set.
    let (want_sum, want_xor) = live.iter().fold((0u64, 0u64), |(a, x), &id| (a.wrapping_add(id), x ^ mix(id)));
    let mut s = Series::default();
    for _ in 0..5 {
        timed(&mut s, &mut || {
            let ids = db.branch_ids().unwrap();
            let (sum, xor) = ids.iter().fold((0u64, 0u64), |(a, x), id| (a.wrapping_add(id.0), x ^ mix(id.0)));
            if ids.len() != live.len() || sum != want_sum || xor != want_xor {
                not_a_result(&format!("list: {} ids, model {}", ids.len(), live.len()));
            }
            vec![("out".to_string(), ids.len() as i64)]
        });
    }
    s.print("list", n, label);

    // DIFF(branch, TRUNK) by D-written; the first 20 of each series cross-checked by D-scan.
    let mut diff_series = |name: &str, ids: Vec<u64>| {
        let mut s = Series::default();
        let mut scanned = 0;
        for (i, &id) in ids.iter().enumerate() {
            let mut result = None;
            timed(&mut s, &mut || {
                let d = db
                    .branch_diff_trunk(BranchId(id))
                    .unwrap_or_else(|e| not_a_result(&format!("diff {id}: {e}")));
                let extra = vec![
                    ("k".to_string(), d.pages.len() as i64),
                    ("own_visited".to_string(), d.own_visited as i64),
                    ("written_visited".to_string(), d.written_visited as i64),
                ];
                result = Some(d);
                extra
            });
            let d = result.unwrap();
            if d.pages.len() < 2 {
                not_a_result(&format!("diff {id}: {} pages, the branch owns two", d.pages.len()));
            }
            if i < 20 {
                let scan = db
                    .branch_diff_trunk_scan(BranchId(id), page_count)
                    .unwrap_or_else(|e| not_a_result(&format!("diff scan {id}: {e}")));
                if scan.pages != d.pages {
                    not_a_result(&format!(
                        "diff {id}: D-written {} pages != D-scan {} pages",
                        d.pages.len(),
                        scan.pages.len()
                    ));
                }
                scanned += 1;
            }
        }
        println!("# {name}: {scanned} samples cross-checked against D-scan over {page_count} pages: equal");
        s.print(name, n, label);
    };
    let random: Vec<u64> = (0..200u64)
        .map(|i| live[(mix(i ^ 0x61) % live.len() as u64) as usize])
        .collect();
    diff_series("diff_random", random);
    diff_series("diff_oldest", live.iter().take(50).copied().collect());

    // Compaction: the whole-state checkpoint, on demand.
    let mut s = Series::default();
    for _ in 0..2 {
        timed(&mut s, &mut || {
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("compact: {e}")));
            let sh = db.branch_shape();
            vec![("snapshot_len".to_string(), sh.snapshot_len as i64)]
        });
    }
    s.print("compact", n, label);
    check_invariants(&db, &model, "before close");
    let sh = db.branch_shape();
    let st = db.branch_stats().unwrap();
    println!(
        "SHAPE\tn={n}\tlabel={label}\tsteps={}\tarena_in_use={}\tarena_free={}\trss={}\tupdates_skipped={}\tdeaths_skipped={}\t{}",
        model.steps,
        st.arena_slots_in_use,
        st.arena_slots_free,
        rss_bytes(),
        model.updates_skipped,
        model.deaths_skipped,
        shape_line(&sh).replace(' ', "\t")
    );
    write_steps(&args.db, model.steps);
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!(
        "CLOSE\tn={n}\tlabel={label}\tclose_us={:.1}\tfiles_after: {}",
        t.elapsed().as_secs_f64() * 1e6,
        files_line(&args.db)
    );
}

fn main() {
    let args = parse_args();
    if cfg!(debug_assertions) {
        println!("# DEBUG build: not a timing result");
    }
    match args.cmd.as_str() {
        "grow" => grow(&args),
        "probe" => probe(&args),
        other => die(&format!("unknown command {other}")),
    }
}
