//! Open, crash recovery, checkpoint and first read against N live, written branches of the durable
//! branch store (r11-restart lane; PREREG in frontier/round11/r11-restart/PREREG.md).
//!
//!   branch_restart grow    --db PATH --to N
//!   branch_restart open    --db PATH --n N [--probes K] [--seed S] [--label L]
//!   branch_restart compact --db PATH --n N
//!   branch_restart ckpt    --db PATH --n N [--writes W] [--seed S]
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
    fn line(&self) -> String {
        format!(
            "log_bytes={} snap_bytes={} arena_bytes={} wal_bytes={}",
            size_of(&self.log),
            size_of(&self.snap),
            size_of(&self.arena),
            size_of(&self.wal)
        )
    }
}

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

fn stats_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} \
         arena_high_water={} arena_free={}",
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
        s.arena_free
    )
}

fn phases_line(s: &BranchOpenStats) -> String {
    let us = |ns: u64| ns as f64 / 1e3;
    let sum = s.recover_ns
        + s.load_ns
        + s.replay_ns
        + s.collect_ns
        + s.referenced_ns
        + s.arena_ns
        + s.expire_ns;
    format!(
        "recover_us={:.1} load_us={:.1} replay_us={:.1} collect_us={:.1} referenced_us={:.1} \
         arena_us={:.1} expire_us={:.1} phases_sum_us={:.1} store_total_us={:.1}",
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
    let db = open_db(&args.db, false);
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
    let mut snap_changes = 0u64;
    let mut last_snap = size_of(&files.snap);
    while live < args.n {
        let expect = live as u64 + 1;
        let branch = trunk.fork_branch().unwrap();
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
        let snap = size_of(&files.snap);
        if snap != last_snap {
            snap_changes += 1;
            println!("# compaction observed after branch {live}: snap_bytes {last_snap} -> {snap}");
            last_snap = snap;
        }
    }
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n || st.arena_slots_in_use != args.n {
        not_a_result(&format!("after growth: {st:?}, expected {} branches and pages", args.n));
    }
    println!(
        "# grown {start} -> {} branches; snapshot size changes observed: {snap_changes}; {}; rss_bytes={}",
        args.n,
        files.line(),
        rss_bytes()
    );
    println!("READY n={} pid={}", args.n, std::process::id());
    std::io::stdout().flush().unwrap();
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

fn summary(name: &str, n: usize, label: &str, v: &mut [f64], counters: &[(u64, u64)]) {
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
    println!(
        "PROBE\tn={n}\tlabel={label}\top={name}\tk={}\tp50_us={:.2}\tp90_us={:.2}\tp99_us={:.2}\tmax_us={:.2}\t\
         resolve_per={:.3}\tresolve_min={rc_min}\tresolve_max={rc_max}\tarena_reads_per={:.3}\tarena_min={ar_min}\tarena_max={ar_max}",
        v.len(),
        pct(v, 50.0),
        pct(v, 90.0),
        pct(v, 99.0),
        v[v.len() - 1],
        rc as f64 / k,
        ar as f64 / k
    );
}

fn open(args: &Args) {
    let files = Files::new(&args.db);
    let files_before = files.line();
    let rss0 = rss_bytes();
    let t = Instant::now();
    let db = open_db(&args.db, true);
    let t_db = t.elapsed();
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let t_connect = t.elapsed();
    let rss1 = rss_bytes();
    let s = db.branch_open_stats();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n || st.arena_slots_in_use < args.n {
        not_a_result(&format!("after open: {st:?}, expected {} branches", args.n));
    }
    println!(
        "OPEN\tn={}\tlabel={}\tdb_open_us={:.1}\ttrunk_connect_us={:.1}\t{}\t{}\tlive={}\tarena_in_use={}\t\
         rss_before={rss0}\trss_after_open={rss1}\tfiles_before: {files_before}",
        args.n,
        args.label,
        t_db.as_secs_f64() * 1e6,
        t_connect.as_secs_f64() * 1e6,
        stats_line(&s).replace(' ', "\t"),
        phases_line(&s).replace(' ', "\t"),
        st.live_branches,
        st.arena_slots_in_use
    );
    if args.probes > 0 {
        let mut rng = Rng(args.seed);
        let mut seen = std::collections::HashSet::new();
        let (mut c_t, mut own_t, mut trunk_t, mut own2_t) = (vec![], vec![], vec![], vec![]);
        let (mut c_c, mut own_c, mut trunk_c, mut own2_c) = (vec![], vec![], vec![], vec![]);
        let k = args.probes.min(args.n);
        while seen.len() < k {
            let id = 1 + rng.below(args.n) as u64;
            if !seen.insert(id) {
                continue;
            }
            let row = row_for(id);
            let other = far_row(row);
            let branch = db.branch(BranchId(id)).unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
            let c0 = db.branch_read_counters();
            let t = Instant::now();
            let conn = branch.connect().unwrap();
            c_t.push(t.elapsed().as_secs_f64() * 1e6);
            let c1 = db.branch_read_counters();
            let t = Instant::now();
            let v_own = read_v(&conn, row);
            own_t.push(t.elapsed().as_secs_f64() * 1e6);
            let c2 = db.branch_read_counters();
            let t = Instant::now();
            let v_trunk = read_v(&conn, other);
            trunk_t.push(t.elapsed().as_secs_f64() * 1e6);
            let c3 = db.branch_read_counters();
            let t = Instant::now();
            let v_own2 = read_v(&conn, row);
            own2_t.push(t.elapsed().as_secs_f64() * 1e6);
            let c4 = db.branch_read_counters();
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
            drop(conn);
            let _ = branch.into_id();
        }
        summary("connect", args.n, &args.label, &mut c_t, &c_c);
        summary("own_first", args.n, &args.label, &mut own_t, &own_c);
        summary("trunk_first", args.n, &args.label, &mut trunk_t, &trunk_c);
        summary("own_second", args.n, &args.label, &mut own2_t, &own2_c);
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
    let db = open_db(&args.db, true);
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
    let db = open_db(&args.db, true);
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
        other => die(&format!("unknown command {other}")),
    }
}
