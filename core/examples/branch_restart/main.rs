//! Open, crash recovery, checkpoint and first read against N live, written branches of the durable
//! branch store (r11-restart lane; PREREG in frontier/round11/r11-restart/PREREG.md).
//!
//!   branch_restart grow    --db PATH --to N
//!   branch_restart open    --db PATH --n N [--probes K] [--seed S] [--label L]
//!   branch_restart compact --db PATH --n N
//!   branch_restart ckpt    --db PATH --n N [--writes W] [--seed S]
//!
//! a12-durable-open lane: this harness is r11-restart's (catalog `--mode`, `ckpt2`, `churn`) merged with
//! sota-durable's `--trunk-every` / `--child-every`; `open` also prints PAGE_IO deltas across the timed
//! database open, and the victim prints its catalog rows written and PAGE_IO at READY.
//!
//! `grow --trunk-every K` (sota-durable lane, round 11 PREREG D3.7) also commits one trunk UPDATE after
//! every K forks, on the far row of the branch just forked (another leaf), so each such commit keeps a
//! pre-image for the children forked since that page's last write: retained trunk versions for the
//! open to rebuild. Its `open` must then run without --probes (the trunk rows have moved).
//!
//! `grow --child-every K` (sota-durable lane, PREREG D3.11): every K-th fork is a child of the branch forked just
//! before it (attached by id, forked, detached again) instead of a fork of the trunk, so the store holds parents
//! with live children, whose page maps an open after a snapshot load must derive.
//!
//! `grow` is the kill -9 VICTIM: it opens the database `Durable { sync: false }` (the files are the
//! same bytes as with sync; only fsyncs differ), grows it to N live branches — each forked from the
//! trunk, each updating one row (one leaf page), each detached so it outlives the process — prints
//! `READY n=N pid=P` and blocks on stdin until the driver SIGKILLs it. It prints counters only.
//!
//! `open` opens `Durable { sync: true }` in this fresh process and times the open, prints the
//! store's open counters, then (with --probes) times first reads on K distinct random branches:
//! attach + connect, the branch's own row, a trunk row on another leaf, and the own row again.
//! Every value is checked. It closes cleanly and times the close.
//!
//! `compact` opens, compacts the branch log into a snapshot, and closes (untimed).
//!
//! `ckpt` opens, commits W trunk UPDATEs of random rows (one transaction each; each first write of
//! a page keeps a pre-image, since every branch forked before it), then times
//! `PRAGMA wal_checkpoint(TRUNCATE)` and `Database::branch_compact_now` twice.
//!
//! `grow --clean` (r12-e3) closes the database cleanly after READY and exits, instead of blocking.
//!
//! `reaprate` (r12-e3, frontier/round12/r12-e3/PREREG.md): opens `Catalog { sync: true }`, then per
//! cell reaps `--reaps` detached children of the trunk by id (`Database::branch_release_detached`)
//! from `--threads` reaper threads in rounds of `--round`, the main thread re-forking the same number
//! between rounds (each writing its own row, a trunk UPDATE after every `--trunk-every` forks), and
//! prints the store's per-reap samples: `cold` cells run right after a checkpoint and a reopen,
//! `warm` cells after the victims were made resident (attach + detach, untimed).
//!
//! Any failed check prints `NOT A RESULT` and exits 1.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::{BranchDurability, BranchId, BranchOpenStats};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;

fn die(msg: &str) -> ! {
    eprintln!("branch_restart: {msg}");
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
    n: usize,
    probes: usize,
    writes: usize,
    seed: u64,
    label: String,
    /// `open --expect-in-use X`: the arena slots the recovered store must hold, as the victim that
    /// made the state printed them (a churn victim's branches also retain versions for their children).
    expect_in_use: Option<u64>,
    trunk_every: usize,
    child_every: usize,
    /// `catalog`: open with `BranchDurability::Catalog` (the published-fix prototype).
    catalog: bool,
    /// `grow --clean`: close cleanly after READY.
    clean: bool,
    /// `reaprate`: reaper thread counts, cell kinds, reaps per cell, reaps per round, victim
    /// order, the prefetch arm.
    threads: Vec<usize>,
    cells: Vec<String>,
    reaps: usize,
    round: usize,
    oldest: bool,
    prefetch: bool,
    /// r12-e3 amendment 9: split each cell at its first store checkpoint.
    ss: bool,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_restart grow|open|compact|ckpt ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        probes: 0,
        writes: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        label: String::new(),
        expect_in_use: None,
        trunk_every: 0,
        child_every: 0,
        catalog: false,
        clean: false,
        threads: vec![1, 4, 8],
        cells: vec!["cold".to_string(), "warm".to_string()],
        reaps: 4000,
        round: 1000,
        oldest: false,
        prefetch: false,
        ss: false,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--probes" => args.probes = val().parse().unwrap_or_else(|_| die("bad --probes")),
            "--writes" => args.writes = val().parse().unwrap_or_else(|_| die("bad --writes")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--label" => args.label = val(),
            "--expect-in-use" => {
                args.expect_in_use = Some(val().parse().unwrap_or_else(|_| die("bad --expect-in-use")))
            }
            "--child-every" => args.child_every = val().parse().unwrap_or_else(|_| die("bad --child-every")),
            "--trunk-every" => args.trunk_every = val().parse().unwrap_or_else(|_| die("bad --trunk-every")),
            "--clean" => args.clean = true,
            "--threads" => {
                args.threads = val()
                    .split(',')
                    .map(|t| t.parse().unwrap_or_else(|_| die("bad --threads")))
                    .collect()
            }
            "--cells" => args.cells = val().split(',').map(str::to_string).collect(),
            "--reaps" => args.reaps = val().parse().unwrap_or_else(|_| die("bad --reaps")),
            "--round" => args.round = val().parse().unwrap_or_else(|_| die("bad --round")),
            "--victim" => {
                args.oldest = match val().as_str() {
                    "random" => false,
                    "oldest" => true,
                    other => die(&format!("unknown --victim {other}")),
                }
            }
            "--ss" => args.ss = true,
            "--prefetch" => {
                args.prefetch = match val().as_str() {
                    "on" => true,
                    "off" => false,
                    other => die(&format!("unknown --prefetch {other}")),
                }
            }
            "--mode" => {
                args.catalog = match val().as_str() {
                    "snapshot" => false,
                    "catalog" => true,
                    other => die(&format!("unknown --mode {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.db.as_os_str().is_empty() || args.n == 0 {
        die("--db and --n/--to are required");
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

fn trunk_write_value(g: u64) -> String {
    format!("t{:0>width$}", g, width = VALUE_LEN - 1)
}

/// The row branch `id` updates (branch ids are 1, 2, ... in fork order: every fork is of the trunk).
fn row_for(id: u64) -> i64 {
    (id.wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

/// A row about half the table away, so on another leaf page.
fn far_row(row: i64) -> i64 {
    (row - 1 + TRUNK_ROWS / 2) % TRUNK_ROWS + 1
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    let mut stmt = conn.prepare(format!("SELECT v FROM t WHERE id = {id}")).unwrap();
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

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

struct Files {
    log: PathBuf,
    arena: PathBuf,
    snap: PathBuf,
    wal: PathBuf,
    cat: PathBuf,
    cat_wal: PathBuf,
}

impl Files {
    fn new(db: &Path) -> Self {
        let with = |suffix: &str| PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
        Self {
            log: with("-branch-log"),
            arena: with("-branch-arena"),
            snap: with("-branch-snap"),
            wal: with("-wal"),
            cat: with("-branch-cat"),
            cat_wal: with("-branch-cat-wal"),
        }
    }
    fn line(&self) -> String {
        format!(
            "log_bytes={} snap_bytes={} arena_bytes={} wal_bytes={} cat_bytes={} cat_wal_bytes={}",
            size_of(&self.log),
            size_of(&self.snap),
            size_of(&self.arena),
            size_of(&self.wal),
            size_of(&self.cat),
            size_of(&self.cat_wal)
        )
    }
}

fn open_db(path: &Path, sync: bool, catalog: bool) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let durability = if catalog {
        BranchDurability::Catalog { sync }
    } else {
        BranchDurability::Durable { sync }
    };
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")))
}

fn stats_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} \
         arena_high_water={} arena_free={} derived_map_inserts={} states={} released_scanned={} \
         branch_loads={} trunk_page_loads={} cat_queries={} cat_rows_read={} touched_slots={} \
         trunk_probes={} trunk_rows={} parked_records={} parked_applied={}",
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
        s.states,
        s.released_scanned,
        s.branch_loads,
        s.trunk_page_loads,
        s.cat_queries,
        s.cat_rows_read,
        s.touched_slots,
        s.trunk_probes,
        s.trunk_rows,
        s.parked_records,
        s.parked_applied
    )
}

fn phases_line(s: &BranchOpenStats) -> String {
    let us = |ns: u64| ns as f64 / 1e3;
    let sum = s.catalog_ns
        + s.recover_ns
        + s.load_ns
        + s.replay_ns
        + s.collect_ns
        + s.referenced_ns
        + s.arena_ns
        + s.expire_ns;
    format!(
        "catalog_us={:.1} recover_us={:.1} load_us={:.1} replay_us={:.1} collect_us={:.1} \
         referenced_us={:.1} arena_us={:.1} expire_us={:.1} phases_sum_us={:.1} store_total_us={:.1}",
        us(s.catalog_ns),
        us(s.recover_ns),
        us(s.load_ns),
        us(s.replay_ns),
        us(s.collect_ns),
        us(s.referenced_ns),
        us(s.arena_ns),
        us(s.expire_ns),
        us(sum),
        us(s.total_ns)
    )
}

fn grow(args: &Args) {
    let db = open_db(&args.db, false, args.catalog);
    let files = Files::new(&args.db);
    let s = db.branch_open_stats();
    println!("# grow victim pid={} target={} open counters: {}", std::process::id(), args.n, stats_line(&s));
    let trunk = db.connect().unwrap();
    let has_table = !trunk
        .prepare("SELECT name FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty();
    if !has_table {
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.execute("BEGIN").unwrap();
        for id in 1..=TRUNK_ROWS {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
                .unwrap();
        }
        trunk.execute("COMMIT").unwrap();
        trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let int = |sql: &str| trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0].as_int().unwrap();
        println!(
            "# fixture created: trunk_rows={TRUNK_ROWS} page_size={} page_count={}",
            int("PRAGMA page_size"),
            int("PRAGMA page_count")
        );
    }
    let mut live = db.branch_stats().unwrap().live_branches;
    if live > args.n {
        not_a_result(&format!("the database already has {live} branches, past the target {}", args.n));
    }
    let start = live;
    let mut trunk_writes = 0u64;
    let mut children = 0u64;
    let mut snap_changes = 0u64;
    let mut last_snap = size_of(&files.snap);
    while live < args.n {
        let expect = live as u64 + 1;
        let branch = if args.child_every > 0 && expect > 1 && expect % args.child_every as u64 == 0 {
            // A child of the branch forked just before: attach it by id, fork, detach it again
            // (dropping the handle would release it).
            let parent = db
                .branch(BranchId(expect - 1))
                .unwrap_or_else(|e| not_a_result(&format!("attach {}: {e}", expect - 1)));
            let child = parent.fork().unwrap();
            let _ = parent.into_id();
            children += 1;
            child
        } else {
            trunk.fork_branch().unwrap()
        };
        if branch.id() != BranchId(expect) {
            not_a_result(&format!("fork returned {:?}, expected id {expect}", branch.id()));
        }
        let row = row_for(expect);
        let conn = branch.connect().unwrap();
        conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(row)))
            .unwrap();
        drop(conn);
        let _ = branch.into_id();
        live += 1;
        if args.trunk_every > 0 && live % args.trunk_every == 0 {
            trunk_writes += 1;
            trunk
                .execute(format!(
                    "UPDATE t SET v = '{}' WHERE id = {}",
                    trunk_write_value(trunk_writes),
                    far_row(row)
                ))
                .unwrap();
        }
        let snap = size_of(&files.snap);
        if snap != last_snap {
            snap_changes += 1;
            println!("# compaction observed after branch {live}: snap_bytes {last_snap} -> {snap}");
            last_snap = snap;
        }
    }
    let st = db.branch_stats().unwrap();
    let tr = db.branch_trunk_retained();
    if st.live_branches != args.n || st.arena_slots_in_use as u64 != args.n as u64 + tr {
        not_a_result(&format!(
            "after growth: {st:?} with {tr} trunk pre-images, expected {} branches and {} pages",
            args.n,
            args.n as u64 + tr
        ));
    }
    println!(
        "# trunk writes this growth: {trunk_writes} (--trunk-every {}); forks of a branch: {children} (--child-every {}); \
         catalog rows written by this process: {}; page_io [db reads, db writes, wal reads, wal writes] since start: {:?}",
        args.trunk_every,
        args.child_every,
        db.branch_catalog_rows_written(),
        turso_core::branch::page_io()
    );
    println!(
        "# grown {start} -> {} branches; snapshot size changes observed: {snap_changes}; trunk_retained={tr}; {}; rss_bytes={}",
        args.n,
        files.line(),
        rss_bytes()
    );
    println!("READY n={} pid={}", args.n, std::process::id());
    std::io::stdout().flush().unwrap();
    if args.clean {
        drop(trunk);
        drop(db);
        println!("# closed cleanly (--clean)");
        return;
    }
    // Block until killed. A closed stdin (EOF) also ends here, without a clean close: the victim
    // never closes the database.
    let mut line = String::new();
    loop {
        line.clear();
        match std::io::stdin().lock().read_line(&mut line) {
            Ok(0) | Err(_) => std::thread::sleep(Duration::from_secs(3600)),
            Ok(_) => {}
        }
    }
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn summary(name: &str, n: usize, label: &str, v: &mut [f64], counters: &[(u64, u64)], cat: &[(u64, u64, u64)]) {
    if v.is_empty() {
        return;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let k = counters.len() as f64;
    let rc: u64 = counters.iter().map(|c| c.0).sum();
    let ar: u64 = counters.iter().map(|c| c.1).sum();
    let rc_min = counters.iter().map(|c| c.0).min().unwrap();
    let rc_max = counters.iter().map(|c| c.0).max().unwrap();
    let ar_min = counters.iter().map(|c| c.1).min().unwrap();
    let ar_max = counters.iter().map(|c| c.1).max().unwrap();
    let loads: u64 = cat.iter().map(|c| c.0).sum();
    let tpages: u64 = cat.iter().map(|c| c.1).sum();
    let crows: u64 = cat.iter().map(|c| c.2).sum();
    println!(
        "PROBE\tn={n}\tlabel={label}\top={name}\tk={}\tp50_us={:.2}\tp90_us={:.2}\tp99_us={:.2}\tmax_us={:.2}\t\
         resolve_per={:.3}\tresolve_min={rc_min}\tresolve_max={rc_max}\tarena_reads_per={:.3}\tarena_min={ar_min}\tarena_max={ar_max}\t\
         branch_loads_per={:.3}\ttrunk_page_loads_per={:.3}\tcat_rows_per={:.3}",
        v.len(),
        pct(v, 50.0),
        pct(v, 90.0),
        pct(v, 99.0),
        v[v.len() - 1],
        rc as f64 / k,
        ar as f64 / k,
        loads as f64 / k,
        tpages as f64 / k,
        crows as f64 / k
    );
}

fn open(args: &Args) {
    let files = Files::new(&args.db);
    let files_before = files.line();
    let rss0 = rss_bytes();
    let io0 = turso_core::branch::page_io();
    let t = Instant::now();
    let db = open_db(&args.db, true, args.catalog);
    let t_db = t.elapsed();
    let io1 = turso_core::branch::page_io();
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let t_connect = t.elapsed();
    let rss1 = rss_bytes();
    let s = db.branch_open_stats();
    let reads = db.branch_read_counters();
    // branch_stats applies the parked Commits (C-R) before counting slots: timed apart from the open.
    let (loads0, _, _, rows0) = db.branch_catalog_counters();
    let t = Instant::now();
    let st = db.branch_stats().unwrap();
    let t_settle = t.elapsed();
    let (loads1, _, _, rows1) = db.branch_catalog_counters();
    println!(
        "SETTLE\tn={}\tlabel={}\tsettle_us={:.1}\tsettle_branch_loads={}\tsettle_cat_rows={}",
        args.n,
        args.label,
        t_settle.as_secs_f64() * 1e6,
        loads1 - loads0,
        rows1 - rows0
    );
    let tr = db.branch_trunk_retained();
    // Every branch owns one page and the arena also holds the trunk's retained versions, unless the
    // victim said otherwise (a churn victim's parents retain the versions their children forked on).
    let want = args.expect_in_use.unwrap_or(args.n as u64 + tr);
    if st.live_branches != args.n || st.arena_slots_in_use as u64 != want {
        not_a_result(&format!(
            "after open: {st:?} with {tr} trunk pre-images, expected {} branches and {want} pages",
            args.n
        ));
    }
    println!(
        "OPEN\tn={}\tlabel={}\tdb_open_us={:.1}\ttrunk_connect_us={:.1}\t{}\t{}\tlive={}\tarena_in_use={}\ttrunk_retained_now={tr}\t\
         resolve_calls_since_open={}\tarena_reads_since_open={}\t\
         open_db_page_reads={}\topen_db_page_writes={}\topen_wal_frame_reads={}\topen_wal_frame_writes={}\t\
         rss_before={rss0}\trss_after_open={rss1}\tfiles_before: {files_before}",
        args.n,
        args.label,
        t_db.as_secs_f64() * 1e6,
        t_connect.as_secs_f64() * 1e6,
        stats_line(&s).replace(' ', "\t"),
        phases_line(&s).replace(' ', "\t"),
        st.live_branches,
        st.arena_slots_in_use,
        reads.0,
        reads.1,
        io1[0] - io0[0],
        io1[1] - io0[1],
        io1[2] - io0[2],
        io1[3] - io0[3]
    );
    if args.probes > 0 {
        let mut rng = Rng(args.seed);
        let mut seen = std::collections::HashSet::new();
        let (mut c_t, mut own_t, mut trunk_t, mut own2_t) = (vec![], vec![], vec![], vec![]);
        let mut attach_t = vec![];
        let (mut c_c, mut own_c, mut trunk_c, mut own2_c) = (vec![], vec![], vec![], vec![]);
        let (mut c_k, mut own_k, mut trunk_k, mut own2_k) = (vec![], vec![], vec![], vec![]);
        let cc = |db: &Arc<Database>| {
            let (l, t, _, r) = db.branch_catalog_counters();
            (l, t, r)
        };
        let k = args.probes.min(args.n);
        while seen.len() < k {
            let id = 1 + rng.below(args.n) as u64;
            if !seen.insert(id) {
                continue;
            }
            let row = row_for(id);
            let other = far_row(row);
            let ka = cc(&db);
            let t_attach = Instant::now();
            let branch = db.branch(BranchId(id)).unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
            attach_t.push(t_attach.elapsed().as_secs_f64() * 1e6);
            let (c0, k0) = (db.branch_read_counters(), cc(&db));
            let t = Instant::now();
            let conn = branch.connect().unwrap();
            c_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c1, k1) = (db.branch_read_counters(), cc(&db));
            let t = Instant::now();
            let v_own = read_v(&conn, row);
            own_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c2, k2) = (db.branch_read_counters(), cc(&db));
            let t = Instant::now();
            let v_trunk = read_v(&conn, other);
            trunk_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c3, k3) = (db.branch_read_counters(), cc(&db));
            let t = Instant::now();
            let v_own2 = read_v(&conn, row);
            own2_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c4, k4) = (db.branch_read_counters(), cc(&db));
            if v_own != branch_value(row) || v_own2 != branch_value(row) {
                not_a_result(&format!("branch {id} read its own row {row} as {v_own:.12}"));
            }
            if v_trunk != trunk_value(other) {
                not_a_result(&format!("branch {id} read trunk row {other} as {v_trunk:.12}"));
            }
            let d = |a: (u64, u64), b: (u64, u64)| (b.0 - a.0, b.1 - a.1);
            c_c.push(d(c0, c1));
            own_c.push(d(c1, c2));
            trunk_c.push(d(c2, c3));
            own2_c.push(d(c3, c4));
            let d3 = |a: (u64, u64, u64), b: (u64, u64, u64)| (b.0 - a.0, b.1 - a.1, b.2 - a.2);
            // The attach happened before k0: count it with the connect.
            c_k.push(d3(ka, k1));
            own_k.push(d3(k1, k2));
            trunk_k.push(d3(k2, k3));
            own2_k.push(d3(k3, k4));
            let _ = k0;
            drop(conn);
            let _ = branch.into_id();
        }
        let no = vec![(0u64, 0u64); attach_t.len()];
        let nok = vec![(0u64, 0u64, 0u64); attach_t.len()];
        summary("attach", args.n, &args.label, &mut attach_t, &no, &nok);
        summary("connect", args.n, &args.label, &mut c_t, &c_c, &c_k);
        summary("own_first", args.n, &args.label, &mut own_t, &own_c, &own_k);
        summary("trunk_first", args.n, &args.label, &mut trunk_t, &trunk_c, &trunk_k);
        summary("own_second", args.n, &args.label, &mut own2_t, &own2_c, &own2_k);
        let st = db.branch_stats().unwrap();
        if st.live_branches != args.n {
            not_a_result(&format!("probing changed the branch count: {st:?}"));
        }
    }
    let rss2 = rss_bytes();
    let t = Instant::now();
    drop(trunk);
    drop(db);
    let t_close = t.elapsed();
    println!(
        "CLOSE\tn={}\tlabel={}\tclose_us={:.1}\trss_end={rss2}\tfiles_after: {}",
        args.n,
        args.label,
        t_close.as_secs_f64() * 1e6,
        files.line()
    );
}

fn compact(args: &Args) {
    let files = Files::new(&args.db);
    let db = open_db(&args.db, true, args.catalog);
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("compact: {st:?}, expected {} branches", args.n));
    }
    db.branch_compact_now().unwrap();
    drop(db);
    println!("COMPACTED\tn={}\t{}", args.n, files.line());
}

fn ckpt(args: &Args) {
    let files = Files::new(&args.db);
    let db = open_db(&args.db, true, args.catalog);
    let trunk = db.connect().unwrap();
    let int = |sql: &str| trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0].as_int().unwrap();
    let before = db.branch_stats().unwrap();
    if before.live_branches != args.n {
        not_a_result(&format!("ckpt: {before:?}, expected {} branches", args.n));
    }
    let mut rng = Rng(args.seed);
    let t = Instant::now();
    for g in 0..args.writes {
        let row = 1 + rng.below(TRUNK_ROWS as usize) as i64;
        trunk
            .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_write_value(g as u64)))
            .unwrap();
    }
    let t_writes = t.elapsed();
    let after = db.branch_stats().unwrap();
    let retained = after.arena_slots_in_use - before.arena_slots_in_use;
    let wal_before = size_of(&files.wal);
    let t = Instant::now();
    let rows = trunk.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap().run_collect_rows().unwrap();
    let t_ckpt = t.elapsed();
    let cols: Vec<String> = rows[0].iter().map(|v| format!("{v:?}")).collect();
    let snap0 = size_of(&files.snap);
    let t = Instant::now();
    db.branch_compact_now().unwrap();
    let t_c1 = t.elapsed();
    let snap1 = size_of(&files.snap);
    let t = Instant::now();
    db.branch_compact_now().unwrap();
    let t_c2 = t.elapsed();
    let snap2 = size_of(&files.snap);
    println!(
        "CKPT\tn={}\twrites={}\tsynchronous={}\twrites_total_us={:.1}\tretained_by_writes={retained}\t\
         wal_bytes_before={wal_before}\twal_ckpt_us={:.1}\twal_ckpt_result={}\t\
         compact1_us={:.1}\tsnap_before={snap0}\tsnap_after1={snap1}\tcompact2_us={:.1}\tsnap_after2={snap2}\t{}",
        args.n,
        args.writes,
        int("PRAGMA synchronous"),
        t_writes.as_secs_f64() * 1e6,
        t_ckpt.as_secs_f64() * 1e6,
        cols.join(","),
        t_c1.as_secs_f64() * 1e6,
        t_c2.as_secs_f64() * 1e6,
        files.line()
    );
    drop(trunk);
    drop(db);
}

/// (c2): the store's checkpoint after `--writes` branch commits spread over random live branches
/// (each: attach, connect, rewrite the branch's own row in place, close, detach). A checkpoint is
/// taken first, so the timed one covers only these commits: in snapshot mode a compaction (all live
/// state), in catalog mode the catalog checkpoint (the dirty branches' rows).
fn ckpt2(args: &Args) {
    let files = Files::new(&args.db);
    let db = open_db(&args.db, true, args.catalog);
    let trunk = db.connect().unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("ckpt2: {st:?}, expected {} branches", args.n));
    }
    db.branch_compact_now().unwrap();
    let settled = files.line();
    let mut rng = Rng(args.seed);
    let mut distinct = std::collections::HashSet::new();
    let t = Instant::now();
    for g in 0..args.writes {
        let id = 1 + rng.below(args.n) as u64;
        distinct.insert(id);
        let branch = db
            .branch(BranchId(id))
            .unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
        let conn = branch.connect().unwrap();
        conn.execute(format!(
            "UPDATE t SET v = '{}' WHERE id = {}",
            trunk_write_value(g as u64),
            row_for(id)
        ))
        .unwrap();
        drop(conn);
        let _ = branch.into_id();
    }
    let t_commits = t.elapsed();
    let (log0, cat0, catw0, snap0) = (
        size_of(&files.log),
        size_of(&files.cat),
        size_of(&files.cat_wal),
        size_of(&files.snap),
    );
    let w0 = db.branch_catalog_rows_written();
    let io0 = turso_core::branch::page_io();
    let res0 = resident_pages(&files.cat);
    let t = Instant::now();
    db.branch_compact_now().unwrap();
    let t_ck = t.elapsed();
    let io1 = turso_core::branch::page_io();
    let res1 = resident_pages(&files.cat);
    let rows_written = db.branch_catalog_rows_written() - w0;
    let io = [io1[0] - io0[0], io1[1] - io0[1], io1[2] - io0[2], io1[3] - io0[3]];
    let (log1, cat1, catw1, snap1) = (
        size_of(&files.log),
        size_of(&files.cat),
        size_of(&files.cat_wal),
        size_of(&files.snap),
    );
    // A sample of the rewritten branches reads its new row.
    let mut rng = Rng(args.seed);
    for g in 0..args.writes.min(20) {
        let id = 1 + rng.below(args.n) as u64;
        let _ = g;
        let branch = db.branch(BranchId(id)).unwrap();
        let conn = branch.connect().unwrap();
        let v = read_v(&conn, row_for(id));
        if !v.starts_with('t') {
            not_a_result(&format!("ckpt2: branch {id} lost its rewrite: {v:.12}"));
        }
        drop(conn);
        let _ = branch.into_id();
    }
    println!(
        "CKPT2\tn={}\twrites={}\tdistinct={}\tcommits_total_us={:.1}\tlog_before_ckpt={log0}\tckpt_us={:.1}\tcat_rows_written={rows_written}\t\
         ckpt_db_page_reads={}\tckpt_db_page_writes={}\tckpt_wal_frame_reads={}\tckpt_wal_frame_writes={}\t\
         cat_resident_before={}\tcat_resident_after={}\tcat_pages={}\t\
         snap_before={snap0}\tsnap_after={snap1}\tcat_before={cat0}\tcat_after={cat1}\tcat_wal_before={catw0}\t\
         cat_wal_after={catw1}\tlog_after={log1}\tsettled: {settled}",
        args.n,
        args.writes,
        distinct.len(),
        t_commits.as_secs_f64() * 1e6,
        t_ck.as_secs_f64() * 1e6,
        io[0],
        io[1],
        io[2],
        io[3],
        res0.0,
        res1.0,
        res1.1
    );
    drop(trunk);
    drop(db);
}

/// `(resident pages, pages)` of a file in the OS page cache, by mmap + mincore; `(0, 0)` if absent.
/// Maps read-only and touches nothing (the r11-restart resident.py instrument, in-process).
fn resident_pages(path: &Path) -> (u64, u64) {
    use std::os::fd::AsRawFd;
    let Ok(file) = std::fs::File::open(path) else {
        return (0, 0);
    };
    let len = file.metadata().map_or(0, |m| m.len()) as usize;
    if len == 0 {
        return (0, 0);
    }
    let page = 16384usize; // the VM page on this arm64 host; the result is in these pages
    let pages = len.div_ceil(page);
    let mut vec = vec![0u8; pages];
    // SAFETY: a read-only shared mapping of `len` bytes of an open file, unmapped before return;
    // mincore writes one byte per page into `vec`, which holds `pages` bytes.
    unsafe {
        let addr = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        );
        if addr == libc::MAP_FAILED {
            return (0, pages as u64);
        }
        let rc = libc::mincore(addr, len, vec.as_mut_ptr() as *mut _);
        libc::munmap(addr, len);
        if rc != 0 {
            return (0, pages as u64);
        }
    }
    (vec.iter().filter(|&&b| b & 1 != 0).count() as u64, pages as u64)
}

/// (b2) victim: open, checkpoint once (so the log holds only what follows), then `--writes` branch
/// commits over random live branches (each: attach, connect, rewrite the branch's own row, close,
/// detach), print READY and block until killed. The log tail it leaves names old branches, which a
/// recovery must bring back: the steady-state crash, where the growth chain's tails only name
/// branches forked in the same tail (PREREG A11).
fn churn(args: &Args) {
    let files = Files::new(&args.db);
    let db = open_db(&args.db, false, args.catalog);
    let _trunk = db.connect().unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("churn: {st:?}, expected {} branches", args.n));
    }
    db.branch_compact_now().unwrap();
    let mut rng = Rng(args.seed);
    let mut distinct = std::collections::HashSet::new();
    for g in 0..args.writes {
        let id = 1 + rng.below(args.n) as u64;
        distinct.insert(id);
        let branch = db
            .branch(BranchId(id))
            .unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
        let conn = branch.connect().unwrap();
        conn.execute(format!(
            "UPDATE t SET v = '{}' WHERE id = {}",
            trunk_write_value(g as u64),
            row_for(id)
        ))
        .unwrap();
        drop(conn);
        let _ = branch.into_id();
    }
    println!(
        "# churn victim pid={} n={} writes={} distinct={} {}",
        std::process::id(),
        args.n,
        args.writes,
        distinct.len(),
        files.line()
    );
    let st = db.branch_stats().unwrap();
    println!(
        "# churn state: live={} arena_slots_in_use={} trunk_retained={}",
        st.live_branches,
        st.arena_slots_in_use,
        db.branch_trunk_retained()
    );
    println!("READY n={} pid={}", args.n, std::process::id());
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn main() {
    let args = parse_args();
    if cfg!(debug_assertions) {
        println!("# DEBUG build: not a timing result");
    }
    match args.cmd.as_str() {
        "grow" => grow(&args),
        "open" => open(&args),
        "compact" => compact(&args),
        "ckpt" => ckpt(&args),
        "ckpt2" => ckpt2(&args),
        "churn" => churn(&args),
        "reaprate" => reaprate(&args),
        "explain" => explain(&args),
        "pagemap" => pagemap(&args),
        other => die(&format!("unknown command {other}")),
    }
}

/// The box's parallelism now, with nothing shared: `t` threads each run the same fixed xorshift
/// loop (branch_arms' null control, copied). Operations per second over the slowest finish.
fn null_ops_per_s(t: usize) -> f64 {
    const ITER: u64 = 50_000_000;
    let barrier = std::sync::Barrier::new(t + 1);
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

/// User and system CPU time of the process so far, in nanoseconds.
fn cpu_ns() -> (u64, u64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        not_a_result("getrusage failed");
    }
    let ns = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000_000 + tv.tv_usec as u64 * 1_000;
    (ns(ru.ru_utime), ns(ru.ru_stime))
}

/// Re-fork `k` children of the trunk, each writing its own row, with a trunk UPDATE after every
/// `every` forks of the run (counted by `forks`); returns their ids.
fn refill(
    trunk: &Arc<Connection>,
    k: usize,
    every: usize,
    forks: &mut u64,
    trunk_writes: &mut u64,
) -> Vec<u64> {
    let mut ids = Vec::with_capacity(k);
    for _ in 0..k {
        let branch = trunk
            .fork_branch()
            .unwrap_or_else(|e| not_a_result(&format!("refill fork: {e}")));
        let id = branch.id().0;
        let row = row_for(id);
        let conn = branch.connect().unwrap();
        conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(row)))
            .unwrap_or_else(|e| not_a_result(&format!("refill write on {id}: {e}")));
        drop(conn);
        let _ = branch.into_id();
        ids.push(id);
        *forks += 1;
        if every > 0 && *forks % every as u64 == 0 {
            *trunk_writes += 1;
            trunk
                .execute(format!(
                    "UPDATE t SET v = '{}' WHERE id = {}",
                    trunk_write_value(1_000_000_000 + *trunk_writes),
                    far_row(row)
                ))
                .unwrap_or_else(|e| not_a_result(&format!("refill trunk write: {e}")));
        }
    }
    ids
}

fn pct_u64(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((p / 100.0) * (sorted.len() - 1) as f64).round() as usize]
}

/// r12-e3 amendment 8: classify the cell's logged in-lock misses and print them, after the cell
/// (outside every timer; the page map reads every catalog page, so only cold-only runs use it).
/// Classes: b_evicted / b_deleted / b_cleared (left the cache since the open), a_created (above the
/// page count at the prewarm), p_stayed (prewarmed, never left, still missed), c_unwarmed (existed
/// at the prewarm, never left, not read by it), d_other (a pager other than the catalog's while a
/// catalog statement runs), o_otherdb (a pager other than the catalog's outside one).
fn a8_cell(
    db: &Arc<Database>,
    n: usize,
    kind: &str,
    t: usize,
    draw: u64,
    page_reads: u64,
    del_clr: [u64; 2],
    split: Option<u64>,
) {
    let log = turso_core::branch::a8_take_log();
    let map: std::collections::HashMap<u32, (String, u8, u8)> = db
        .branch_catalog_page_map()
        .unwrap_or_else(|e| not_a_result(&format!("a8 page map: {e}")))
        .into_iter()
        .map(|(p, name, depth, ty)| (p, (name, depth, ty)))
        .collect();
    let (opens, pages_at_prewarm) = turso_core::branch::a8_opens();
    let mut classes: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
    let mut rows: std::collections::BTreeMap<(String, String, String, u8, u8), u64> = std::collections::BTreeMap::new();
    let mut created_after_left = 0u64;
    // r12-e3 amendment 9: with `split` (the cell's first checkpoint reap), misses of later reaps are
    // also counted apart as post-checkpoint.
    let mut post: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
    let mut post_n = 0u64;
    for m in &log {
        if m.catalog && m.created_after && m.left != 0 {
            created_after_left += 1;
        }
        let class = if !m.catalog {
            // (d) is a pager other than the catalog's while a catalog statement runs; outside any
            // catalog statement it is another database's read (o).
            if m.stmt != 0 {
                "d_other"
            } else {
                "o_otherdb"
            }
        } else if m.left == 1 {
            "b_evicted"
        } else if m.left == 2 {
            "b_deleted"
        } else if m.left == 3 {
            "b_cleared"
        } else if m.created_after {
            "a_created"
        } else if m.prewarmed {
            "p_stayed"
        } else {
            "c_unwarmed"
        };
        *classes.entry(class).or_default() += 1;
        let phase = match split {
            Some(s) if m.reap > s => {
                post_n += 1;
                *post.entry(class).or_default() += 1;
                "post"
            }
            Some(_) => "pre",
            None => "-",
        };
        let (btree, ty, depth) = match map.get(&m.page) {
            Some((name, depth, ty)) if m.catalog => (
                name.clone(),
                match ty {
                    2 => "index_interior",
                    5 => "table_interior",
                    10 => "index_leaf",
                    13 => "table_leaf",
                    _ => "other",
                }
                .to_string(),
                *depth,
            ),
            _ if m.catalog => ("NONE".to_string(), "-".to_string(), 0),
            _ => ("OTHER_PAGER".to_string(), "-".to_string(), 0),
        };
        *rows.entry((format!("{class} phase={phase}"), btree, ty, depth, m.stmt)).or_default() += 1;
    }
    println!(
        "# a8 N={n} kind={kind} T={t} draw={draw} misses={} page_reads_total={page_reads} complete={} \
         classes={classes:?} created_after_left={created_after_left} opens_total={opens} pages_at_prewarm={pages_at_prewarm} \
         mapped_pages={} cat_deletes={} cat_cleared={}",
        log.len(),
        log.len() as u64 == page_reads,
        map.len(),
        del_clr[0],
        del_clr[1],
    );
    if let Some(s) = split {
        println!("# a8ss N={n} kind={kind} T={t} draw={draw} first_ckpt_reap={s} post_misses={post_n} post_classes={post:?}");
    }
    for ((class, btree, ty, depth, stmt), k) in rows {
        println!("# a8row N={n} kind={kind} T={t} draw={draw} class={class} btree={btree} type={ty} depth={depth} stmt={stmt} n={k}");
    }
}

/// r12-e3 amendment 9: one cell split at its first store checkpoint (`first`, a reap sequence
/// number): checkpoints, reaps per checkpoint interval, in-lock page reads, late reads and re-runs per
/// non-checkpoint reap before and after it, and the catalog cache's deletes and cleared pages from the
/// start of the first checkpoint reap's hold to the cell's end (`cc_end`: cumulative [deletes, cleared]).
fn ss_cell(
    n: usize,
    kind: &str,
    t: usize,
    draw: u64,
    samples: &[turso_core::branch::ReapSample],
    first: Option<u64>,
    cc_end: [u64; 2],
) {
    let mut by_seq: Vec<&turso_core::branch::ReapSample> = samples.iter().collect();
    by_seq.sort_by_key(|s| s.seq);
    let ckpt_seqs: Vec<u64> = by_seq.iter().filter(|s| s.ckpt).map(|s| s.seq).collect();
    let k_mean = if ckpt_seqs.len() >= 2 {
        (ckpt_seqs[ckpt_seqs.len() - 1] - ckpt_seqs[0]) as f64 / (ckpt_seqs.len() - 1) as f64
    } else {
        f64::NAN
    };
    let f = first.unwrap_or(u64::MAX);
    let side = |post: bool| -> Vec<&turso_core::branch::ReapSample> {
        by_seq.iter().copied().filter(|s| !s.ckpt && ((s.seq > f) == post)).collect()
    };
    let per = |v: &[&turso_core::branch::ReapSample], g: &dyn Fn(&turso_core::branch::ReapSample) -> u64| -> f64 {
        v.iter().map(|s| g(s)).sum::<u64>() as f64 / v.len().max(1) as f64
    };
    let (post, pre) = (side(true), side(false));
    let tot = |v: &[&turso_core::branch::ReapSample], g: &dyn Fn(&turso_core::branch::ReapSample) -> u64| -> u64 {
        v.iter().map(|s| g(s)).sum()
    };
    let after = match by_seq.iter().find(|s| s.ckpt) {
        Some(s) => [cc_end[0] - s.cat_cc0[0], cc_end[1] - s.cat_cc0[1]],
        None => [0, 0],
    };
    println!(
        "# ss N={n} kind={kind} T={t} draw={draw} ckpts={} first_ckpt_reap={} k_mean={k_mean:.1} post_reaps={} \
         post_page_reads={:.4} pre_reaps={} pre_page_reads={:.4} post_late_passed={} post_late_giveup={} \
         post_reruns={:.3} pre_reruns={:.3} post_gave_up={} cat_deletes_after={} cat_cleared_after={}",
        ckpt_seqs.len(),
        first.map_or("none".to_string(), |s| s.to_string()),
        post.len(),
        per(&post, &|s| s.page_reads),
        pre.len(),
        per(&pre, &|s| s.page_reads),
        tot(&post, &|s| if s.gave_up { 0 } else { s.late }),
        tot(&post, &|s| if s.gave_up { s.late } else { 0 }),
        per(&post, &|s| s.reruns),
        per(&pre, &|s| s.reruns),
        post.iter().filter(|s| s.gave_up).count(),
        after[0],
        after[1],
    );
}

/// r12-e3 amendment 9: per B-tree of the branch catalog, its leaf and interior page counts, from the
/// store's own page map (the leaf counts L_i of amendment 9's coupon-collector formula). Open it on a
/// clone: the store's open may write.
fn pagemap(args: &Args) {
    let db = open_db(&args.db, true, true);
    let map = db
        .branch_catalog_page_map()
        .unwrap_or_else(|e| die(&format!("page map: {e}")));
    let mut per: std::collections::BTreeMap<String, [u64; 2]> = std::collections::BTreeMap::new();
    for (_, name, _, ty) in &map {
        let e = per.entry(name.clone()).or_default();
        if *ty == 10 || *ty == 13 {
            e[0] += 1;
        } else {
            e[1] += 1;
        }
    }
    println!("# pagemap db={} mapped_pages={}", args.db.display(), map.len());
    for (name, [leaf, interior]) in per {
        println!("# pagemap btree={name} leaf={leaf} interior={interior}");
    }
}

/// r12-e3 amendment 9 (A8-P3'): EXPLAIN QUERY PLAN, from this build's own planner, of the RES
/// prewarm statements, on the catalog database at `--db` (open it on a clone: nothing is written).
fn explain(args: &Args) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        args.db.to_str().unwrap(),
        OpenFlags::None,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| die(&format!("open {}: {e}", args.db.display())));
    let conn = db.connect().unwrap();
    for sql in turso_core::branch::RES_PREWARM_SQL {
        println!("# explain sql={sql}");
        let rows = conn
            .prepare(format!("EXPLAIN QUERY PLAN {sql}"))
            .and_then(|mut st| st.run_collect_rows())
            .unwrap_or_else(|e| die(&format!("explain {sql}: {e}")));
        for row in rows {
            let line: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            println!("#   plan {}", line.join(" | "));
        }
    }
}

/// r12-e3: reaps/s per parent on the durable catalog store; see the module doc and the PREREG.
fn reaprate(args: &Args) {
    if !args.catalog {
        die("reaprate needs --mode catalog");
    }
    if args.threads.is_empty() || args.threads.contains(&0) || args.round == 0 || args.reaps < args.round {
        die("reaprate: --threads positive, --round > 0, --reaps >= --round");
    }
    if args.reaps > args.n {
        die("reaprate: --reaps must be <= --n (a cell's victims all come from the population it began with)");
    }
    let files = Files::new(&args.db);
    let t = Instant::now();
    let mut db = open_db(&args.db, true, true);
    let mut trunk = db.connect().unwrap();
    println!(
        "# reaprate n={} open_us={:.0} threads={:?} cells={:?} reaps={} round={} victim={} prefetch={} \
         trunk_every={} seed={:#x} {} rss_bytes={} env={:?}",
        args.n,
        t.elapsed().as_secs_f64() * 1e6,
        args.threads,
        args.cells,
        args.reaps,
        args.round,
        if args.oldest { "oldest" } else { "random" },
        if args.prefetch { "on" } else { "off" },
        args.trunk_every,
        args.seed,
        files.line(),
        rss_bytes(),
        // r12-e3 amendments 3 and 7: the run-time arms, as this process saw them.
        ["R12_CAT_CACHE_KIB", "R12_CAT_PREWARM", "R12_SIEVE_BEHIND", "R12_RERUN_CAP", "R12_CAT_CKPT_KEEP", "R12_A8"]
            .map(|k| format!("{k}={}", std::env::var(k).unwrap_or_else(|_| "-".to_string())))
    );
    // r12-e3 amendment 8: name the catalog's statements once.
    let a8 = std::env::var("R12_A8").is_ok_and(|v| v == "1");
    if a8 && args.cells.iter().any(|c| c != "cold") {
        die("R12_A8 walks every catalog page after each cell, so it runs cold cells only (each reopens)");
    }
    if a8 {
        for (id, sql) in turso_core::branch::a8_stmt_names() {
            println!("# a8stmt id={id} sql={sql}");
        }
    }
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("reaprate: {st:?}, expected {} live branches", args.n));
    }
    // The population, in fork order (ids are fork order: every branch is a child of the trunk).
    let mut pop: std::collections::VecDeque<u64> = db
        .branch_ids()
        .unwrap_or_else(|e| not_a_result(&format!("branch_ids: {e}")))
        .into_iter()
        .map(|b| b.0)
        .collect();
    if pop.len() != args.n {
        not_a_result(&format!("branch_ids listed {} branches, expected {}", pop.len(), args.n));
    }
    let mut rng = Rng(args.seed | 1);
    let (mut forks, mut trunk_writes) = (0u64, 0u64);
    println!(
        "N\tkind\tT\tdraw\tmetric\tp50\tp90\tp99\tmax\tmean"
    );
    let order: Vec<(usize, u64)> = args
        .threads
        .iter()
        .map(|&t| (t, 0u64))
        .chain(args.threads.iter().rev().map(|&t| (t, 1u64)))
        .collect();
    for kind in &args.cells {
        let cold = match kind.as_str() {
            "cold" => true,
            "warm" => false,
            other => die(&format!("unknown cell kind {other}")),
        };
        for &(t, draw) in &order {
            if cold {
                // A clean shutdown's checkpoint, then a reopen: nothing is resident, and the
                // catalog connection's page cache is empty (the OS file cache is not purged).
                db.branch_compact_now()
                    .unwrap_or_else(|e| not_a_result(&format!("compact before reopen: {e}")));
                drop(trunk);
                drop(db);
                let t0 = Instant::now();
                db = open_db(&args.db, true, true);
                trunk = db.connect().unwrap();
                println!("# reopen before N={} kind=cold T={t} draw={draw}: open_us={:.0}", args.n, t0.elapsed().as_secs_f64() * 1e6);
            }
            let null = null_ops_per_s(t);
            db.branch_set_prefetch(args.prefetch);
            db.branch_reap_sampling(true);
            let _ = db.branch_take_reap_samples();
            let rounds = args.reaps / args.round;
            // Victims come from the population as the cell began; this cell's refills join it at
            // the end, so a cold cell's victims are all cold and a warm cell's all pre-touched.
            let mut fresh: Vec<u64> = Vec::with_capacity(args.reaps);
            let (mut wall_ns, mut reaps) = (0u128, 0usize);
            let (mut cpu_user, mut cpu_sys) = (0u64, 0u64);
            let io0 = turso_core::branch::page_io();
            let fs0 = turso_core::branch::FSYNCS.load(std::sync::atomic::Ordering::Relaxed);
            let cc = || turso_core::branch::CAT_CACHE.each_ref().map(|c| c.load(std::sync::atomic::Ordering::Relaxed));
            let cc0 = cc();
            if a8 {
                let _ = turso_core::branch::a8_take_log();
            }
            for _ in 0..rounds {
                // This round's victims, dealt to the reapers round-robin.
                let mut victims = Vec::with_capacity(args.round);
                for _ in 0..args.round {
                    let v = if args.oldest {
                        pop.pop_front().unwrap()
                    } else {
                        let at = rng.below(pop.len());
                        pop.swap_remove_back(at).unwrap()
                    };
                    victims.push(v);
                }
                if !cold {
                    // Warm: every victim resident before the timed phase.
                    for &v in &victims {
                        let b = db
                            .branch(BranchId(v))
                            .unwrap_or_else(|e| not_a_result(&format!("warm attach {v}: {e}")));
                        let _ = b.into_id();
                    }
                }
                let shares: Vec<Vec<u64>> = (0..t)
                    .map(|i| victims.iter().skip(i).step_by(t).copied().collect())
                    .collect();
                let barrier = std::sync::Barrier::new(t + 1);
                let c0 = cpu_ns();
                let wall = std::thread::scope(|s| {
                    let handles: Vec<_> = shares
                        .iter()
                        .map(|share| {
                            let (db, barrier) = (&db, &barrier);
                            s.spawn(move || {
                                barrier.wait();
                                for &v in share {
                                    let r = db.branch_release_detached(BranchId(v)).unwrap_or_else(
                                        |e| not_a_result(&format!("reap {v}: {e}")),
                                    );
                                    if r.deferred || r.freed_pages < 1 {
                                        not_a_result(&format!("reap {v} freed {r:?}"));
                                    }
                                }
                            })
                        })
                        .collect();
                    barrier.wait();
                    let started = Instant::now();
                    for h in handles {
                        h.join().unwrap();
                    }
                    started.elapsed()
                });
                let c1 = cpu_ns();
                cpu_user += c1.0 - c0.0;
                cpu_sys += c1.1 - c0.1;
                wall_ns += wall.as_nanos();
                reaps += victims.len();
                fresh.extend(refill(&trunk, args.round, args.trunk_every, &mut forks, &mut trunk_writes));
            }
            pop.extend(fresh);
            let io1 = turso_core::branch::page_io();
            let fs1 = turso_core::branch::FSYNCS.load(std::sync::atomic::Ordering::Relaxed);
            let samples = db.branch_take_reap_samples();
            db.branch_reap_sampling(false);
            if samples.len() != reaps {
                not_a_result(&format!("{} reap samples for {reaps} reaps", samples.len()));
            }
            let x = reaps as f64 / (wall_ns as f64 / 1e9);
            let sum = |f: &dyn Fn(&turso_core::branch::ReapSample) -> u64| -> u64 {
                samples.iter().map(f).sum()
            };
            let n_ckpt = samples.iter().filter(|s| s.ckpt).count();
            let plain: Vec<&turso_core::branch::ReapSample> = samples.iter().filter(|s| !s.ckpt).collect();
            let per = |f: &dyn Fn(&turso_core::branch::ReapSample) -> u64| -> f64 {
                plain.iter().map(|s| f(s)).sum::<u64>() as f64 / plain.len().max(1) as f64
            };
            println!(
                "# rrcell N={} kind={kind} T={t} draw={draw} prefetch={} victim={} reaps={reaps} wall_ns={wall_ns} \
                 reaps_per_s={x:.1} lifetime_s={:.3} null_ops_per_s={null:.0} user_ns={cpu_user} sys_ns={cpu_sys} \
                 ckpt_reaps={n_ckpt} gave_up={} late_total={} plan_reads_total={} \
                 page_io_delta={:?} fsyncs_global_delta={} rss_bytes={}",
                args.n,
                if args.prefetch { "on" } else { "off" },
                if args.oldest { "oldest" } else { "random" },
                args.n as f64 / x,
                samples.iter().filter(|s| s.gave_up).count(),
                sum(&|s| s.late),
                sum(&|s| s.plan_reads),
                [io1[0] - io0[0], io1[1] - io0[1], io1[2] - io0[2], io1[3] - io0[3]],
                fs1 - fs0,
                rss_bytes()
            );
            println!(
                "# rrper N={} kind={kind} T={t} draw={draw} (non-checkpoint reaps, per reap) queries={:.3} loads={:.4} \
                 page_reads={:.4} fsyncs={:.4} late={:.4} reruns={:.3} prefetch_reads={:.3} plan_reads={:.4} \
                 hold_us={:.2} plan_hold_us={:.2} wait_us={:.2} ensure_us={:.2} log_us={:.2} collect_us={:.2} \
                 prefetch_us={:.2}",
                args.n,
                per(&|s| s.queries),
                per(&|s| s.loads),
                per(&|s| s.page_reads),
                per(&|s| s.fsyncs),
                per(&|s| s.late),
                per(&|s| s.reruns),
                per(&|s| s.prefetch_reads),
                per(&|s| s.plan_reads),
                per(&|s| s.hold_ns) / 1e3,
                per(&|s| s.plan_hold_ns) / 1e3,
                per(&|s| s.wait_ns) / 1e3,
                per(&|s| s.ensure_ns) / 1e3,
                per(&|s| s.log_ns) / 1e3,
                per(&|s| s.collect_ns) / 1e3,
                per(&|s| s.prefetch_ns) / 1e3,
            );
            // r12-e3 amendment 7: the attribution counters, over every reap of the cell.
            let cc1 = cc();
            let removed: Vec<u64> = (0..turso_core::branch::PF_COUNTERS)
                .map(|i| sum(&|s| s.pf_removed[i]))
                .collect();
            let mut reruns: Vec<u64> = samples.iter().map(|s| s.reruns).collect();
            reruns.sort_unstable();
            if turso_core::branch::PF_OVERFLOW.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                not_a_result("more than 64 live threads took prefetch slots (their counts were not kept)");
            }
            if args.prefetch && sum(&|s| s.prefetch_reads) > 0 && cc1[0] == cc0[0] {
                not_a_result("prefetches read pages but the catalog cache counted no insert (instrument not attached)");
            }
            println!(
                "# rrpf N={} kind={kind} T={t} draw={draw} cat_cache_pages={} cat_inserts={} cat_evictions={} \
                 late_giveup={} late_passed={} reruns_p50={} reruns_p99={} reruns_max={} prefetch_reads_total={} \
                 pend_self={:?} pend_other={:?} pend_touched={} pend_deleted={} pend_cleared={}",
                args.n,
                turso_core::branch::CAT_CACHE_PAGES.load(std::sync::atomic::Ordering::Relaxed),
                cc1[0] - cc0[0],
                cc1[1] - cc0[1],
                sum(&|s| if s.gave_up { s.late } else { 0 }),
                sum(&|s| if s.gave_up { 0 } else { s.late }),
                pct_u64(&reruns, 50.0),
                pct_u64(&reruns, 99.0),
                reruns.last().copied().unwrap_or(0),
                sum(&|s| s.prefetch_reads),
                &removed[0..5],
                &removed[5..10],
                removed[10],
                removed[11],
                removed[12],
            );
            // r12-e3 amendment 9: the cell split at its first store checkpoint.
            let first_ckpt = samples.iter().filter(|s| s.ckpt).map(|s| s.seq).min();
            if args.ss {
                ss_cell(args.n, kind, t, draw, &samples, first_ckpt, [cc1[2], cc1[3]]);
            }
            if a8 {
                let split = if args.ss { Some(first_ckpt.unwrap_or(u64::MAX)) } else { None };
                a8_cell(&db, args.n, kind, t, draw, sum(&|s| s.page_reads), [cc1[2] - cc0[2], cc1[3] - cc0[3]], split);
            }
            if n_ckpt > 0 {
                let ck: Vec<&turso_core::branch::ReapSample> = samples.iter().filter(|s| s.ckpt).collect();
                let m = |f: &dyn Fn(&turso_core::branch::ReapSample) -> u64| -> f64 {
                    ck.iter().map(|s| f(s)).sum::<u64>() as f64 / ck.len() as f64
                };
                println!(
                    "# rrckpt N={} kind={kind} T={t} draw={draw} (checkpoint reaps, per reap) n={n_ckpt} ckpt_us={:.1} \
                     ckpt_reads={:.1} ckpt_fsyncs={:.2} ckpt_queries={:.1} hold_us={:.1}",
                    args.n,
                    m(&|s| s.ckpt_ns) / 1e3,
                    m(&|s| s.ckpt_reads),
                    m(&|s| s.ckpt_fsyncs),
                    m(&|s| s.ckpt_queries),
                    m(&|s| s.hold_ns) / 1e3,
                );
            }
            type Rs = turso_core::branch::ReapSample;
            let metrics: [(&str, &dyn Fn(&Rs) -> u64); 4] = [
                ("hold_us", &|s: &Rs| s.hold_ns),
                ("lock_total_us", &|s: &Rs| s.hold_ns + s.plan_hold_ns),
                ("wait_us", &|s: &Rs| s.wait_ns),
                ("reap_total_us", &|s: &Rs| s.wait_ns + s.hold_ns + s.plan_hold_ns + s.prefetch_ns),
            ];
            for (name, f) in metrics {
                let mut v: Vec<u64> = samples.iter().map(f).collect();
                v.sort_unstable();
                let mean = v.iter().sum::<u64>() as f64 / v.len().max(1) as f64;
                println!(
                    "{}\t{kind}\t{t}\t{draw}\t{name}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
                    args.n,
                    pct_u64(&v, 50.0) as f64 / 1e3,
                    pct_u64(&v, 90.0) as f64 / 1e3,
                    pct_u64(&v, 99.0) as f64 / 1e3,
                    *v.last().unwrap_or(&0) as f64 / 1e3,
                    mean / 1e3
                );
            }
            // Checks: the live count, and 50 random live branches read their own row.
            let st = db.branch_stats().unwrap();
            if st.live_branches != args.n || pop.len() != args.n {
                not_a_result(&format!(
                    "after N={} {kind} T={t} draw={draw}: {st:?}, harness holds {}",
                    args.n,
                    pop.len()
                ));
            }
            for _ in 0..50 {
                let v = pop[rng.below(pop.len())];
                let b = db
                    .branch(BranchId(v))
                    .unwrap_or_else(|e| not_a_result(&format!("check attach {v}: {e}")));
                let row = row_for(v);
                let got = read_v(&b.connect().unwrap(), row);
                if got != branch_value(row) {
                    not_a_result(&format!("branch {v} reads row {row} as {got}"));
                }
                let _ = b.into_id();
            }
            println!(
                "# checked N={} kind={kind} T={t} draw={draw}: live={} arena_slots_in_use={} trunk_retained={} \
                 50 own rows read right; forks={forks} trunk_writes={trunk_writes} {}",
                args.n,
                st.live_branches,
                st.arena_slots_in_use,
                db.branch_trunk_retained(),
                files.line()
            );
        }
    }
    drop(trunk);
    drop(db);
    let db = open_db(&args.db, true, true);
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("final reopen: {st:?}, expected {} live", args.n));
    }
    println!("# final reopen: live={} arena_slots_in_use={}", st.live_branches, st.arena_slots_in_use);
}
