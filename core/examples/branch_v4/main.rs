//! V4 base-diff confirm run (r11-merge lane; PREREG A20 in frontier/round11/r11-merge/PREREG.md).
//!
//!   branch_v4 victim --db PATH (--age A [--forks F] | --versions V) [--rows R] [--keys K] [--per M] [--seed S]
//!   branch_v4 reopen (the same arguments)
//!
//! A20c's arms: `--forks F` (the N arm) forks F more branches from the trunk, detached and never
//! written, spread evenly over the A commits (floor(F*g/A) - floor(F*(g-1)/A) before commit g).
//! `--versions V` (the V arm) replaces the random commits: V rounds of one trunk commit updating
//! B's K key rows, so rewriting B's leaves, then one observer fork, detached; each of B's leaves
//! then carries V retained trunk versions and B's base is the oldest.
//!
//! `victim` is the kill -9 VICTIM. It opens a fresh catalog-mode store (`Catalog { sync: false }`;
//! the files are the same bytes as with sync), creates t(id INTEGER PRIMARY KEY, v TEXT) with R
//! rows of 100-byte values, forks B (the long-lived PR, branch id 1), and B updates K scattered
//! keys in one transaction and is detached, so it outlives the process. Then the trunk takes A
//! commits of M random-row UPDATEs each (every row gets a value no earlier commit gave it, so no
//! UPDATE is a no-op). Then `PRAGMA wal_checkpoint(TRUNCATE)` and `Database::branch_compact_now`,
//! `READY`, and it blocks until the driver SIGKILLs it.
//!
//! `reopen` opens that crash image in a fresh process (`Catalog { sync: true }`) and performs V4's
//! per-key base read: for each key B wrote, a descent of t's b-tree from its root in the base (the
//! trunk as of B's fork) through `Database::branch_base_page`, reading a page from the database
//! file whenever the store answers that the trunk's current version is the base. A minimal SQLite
//! page parser: interior 0x05, leaf 0x0d, page 1's 100-byte offset, no overflow (refused if seen).
//! It compares each base cell with the fixture's value and with the trunk's current row (ours),
//! and derives the ground truth for M: the leaves any trunk commit rewrote since the fork.
//! Counters only; nothing is timed. The key set and the trunk's rows are re-drawn from the seed.
//!
//! Any failed harness check prints `NOT A RESULT` and exits 1. A store answer that is wrong (a
//! refused base or a base cell that is not the fork's value) prints `FINDING` and exits 3 after
//! every counter is printed.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use turso_core::branch::{BranchDurability, BranchId};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const VALUE_LEN: usize = 100;
const B: BranchId = BranchId(1);

fn die(msg: &str) -> ! {
    eprintln!("branch_v4: {msg}");
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
    age: u64,
    rows: i64,
    keys: usize,
    per: usize,
    seed: u64,
    forks: u64,
    versions: u64,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_v4 victim|reopen ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        age: 0,
        rows: 100_000,
        keys: 64,
        per: 16,
        seed: 0x9E37_79B9_7F4A_7C15,
        forks: 0,
        versions: 0,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--age" => args.age = val().parse().unwrap_or_else(|_| die("bad --age")),
            "--rows" => args.rows = val().parse().unwrap_or_else(|_| die("bad --rows")),
            "--keys" => args.keys = val().parse().unwrap_or_else(|_| die("bad --keys")),
            "--per" => args.per = val().parse().unwrap_or_else(|_| die("bad --per")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--forks" => args.forks = val().parse().unwrap_or_else(|_| die("bad --forks")),
            "--versions" => args.versions = val().parse().unwrap_or_else(|_| die("bad --versions")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.versions > 0 {
        if args.age != 0 || args.forks != 0 {
            die("--versions sets the commits and the forks; it takes no --age or --forks");
        }
        args.age = args.versions;
    }
    if args.db.as_os_str().is_empty() || args.age == 0 || args.rows < 1 || args.keys == 0 || args.per == 0 {
        die("--db and --age (or --versions) are required; --rows, --keys and --per must be positive");
    }
    if args.keys as i64 > args.rows {
        die("--keys exceeds --rows");
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
    fn row(&mut self, rows: i64) -> i64 {
        (self.next() % rows as u64) as i64 + 1
    }
}

/// The run's random choices, drawn in one order by both processes: B's keys (distinct, in draw
/// order), then each trunk commit's rows.
struct Draws {
    keys: Vec<i64>,
    trunk: Vec<Vec<i64>>,
}

fn draws(args: &Args) -> Draws {
    let mut rng = Rng(args.seed);
    let mut seen = HashSet::new();
    let mut keys = Vec::with_capacity(args.keys);
    while keys.len() < args.keys {
        let k = rng.row(args.rows);
        if seen.insert(k) {
            keys.push(k);
        }
    }
    let trunk = if args.versions > 0 {
        vec![keys.clone(); args.versions as usize]
    } else {
        (0..args.age)
            .map(|_| (0..args.per).map(|_| rng.row(args.rows)).collect())
            .collect()
    };
    Draws { keys, trunk }
}

/// The N arm's forks before commit `g` (1-based): F spread evenly over the A commits.
fn forks_before(args: &Args, g: u64) -> u64 {
    args.forks * g / args.age - args.forks * (g - 1) / args.age
}

/// Live branches the run leaves: B, the N arm's forks, the V arm's observers.
fn n_live(args: &Args) -> usize {
    (1 + args.forks + args.versions) as usize
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn branch_value(id: i64) -> String {
    format!("b{:0>width$}", id, width = VALUE_LEN - 1)
}

/// Commit `g`'s value (1-based): distinct from every fixture value and every other commit's.
fn trunk_write_value(g: u64) -> String {
    format!("t{:0>width$}", g, width = VALUE_LEN - 1)
}

/// B's row `id`, read after the counters: an error or a wrong shape is the store's, so it is
/// returned for a FINDING rather than ending the run.
fn try_read_v(conn: &Arc<Connection>, id: i64) -> Result<String, String> {
    let mut stmt = conn.prepare(format!("SELECT v FROM t WHERE id = {id}")).map_err(|e| e.to_string())?;
    let rows = stmt.run_collect_rows().map_err(|e| e.to_string())?;
    match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => Ok(t.as_str().to_string()),
            other => Err(format!("expected text, got {other:?}")),
        },
        _ => Err(format!("{} rows", rows.len())),
    }
}

fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap_or_else(|| not_a_result(&format!("{sql}: not an integer")))
}

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

fn with_suffix(db: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()))
}

fn files_line(db: &Path) -> String {
    let s = |suffix| size_of(&with_suffix(db, suffix));
    format!(
        "db_bytes={} wal_bytes={} log_bytes={} snap_bytes={} arena_bytes={} cat_bytes={} cat_wal_bytes={}",
        size_of(db),
        s("-wal"),
        s("-branch-log"),
        s("-branch-snap"),
        s("-branch-arena"),
        s("-branch-cat"),
        s("-branch-cat-wal")
    )
}

fn open_db(path: &Path, sync: bool) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync }),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")))
}

fn checkpoint_truncate(trunk: &Arc<Connection>) {
    let rows = trunk.prepare("PRAGMA wal_checkpoint(TRUNCATE)").unwrap().run_collect_rows().unwrap();
    let busy = rows.first().and_then(|r| r.first()).and_then(|v| v.as_int());
    if busy != Some(0) {
        not_a_result(&format!("wal_checkpoint(TRUNCATE) returned {rows:?}"));
    }
}

fn victim(args: &Args) {
    if size_of(&args.db) != 0 {
        not_a_result(&format!("{} exists: every age needs a fresh store", args.db.display()));
    }
    let d = draws(args);
    let db = open_db(&args.db, false);
    let trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=args.rows {
        trunk.execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id))).unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    checkpoint_truncate(&trunk);
    println!(
        "# victim pid={} age={} forks={} versions={} rows={} keys={} per={} seed={} page_size={} page_count={}",
        std::process::id(),
        args.age,
        args.forks,
        args.versions,
        args.rows,
        args.keys,
        args.per,
        args.seed,
        int(&trunk, "PRAGMA page_size"),
        int(&trunk, "PRAGMA page_count")
    );
    let branch = trunk.fork_branch().unwrap();
    if branch.id() != B {
        not_a_result(&format!("fork returned {:?}, expected {B:?}", branch.id()));
    }
    let conn = branch.connect().unwrap();
    conn.execute("BEGIN").unwrap();
    for &k in &d.keys {
        conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {k}", branch_value(k))).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    drop(conn);
    let _ = branch.into_id();
    let fork_one = || {
        let b = trunk.fork_branch().unwrap();
        let _ = b.into_id();
    };
    for (g, rows) in d.trunk.iter().enumerate() {
        let g = g as u64 + 1;
        for _ in 0..if args.forks > 0 { forks_before(args, g) } else { 0 } {
            fork_one();
        }
        let value = trunk_write_value(g);
        trunk.execute("BEGIN").unwrap();
        for &r in rows {
            trunk.execute(format!("UPDATE t SET v = '{value}' WHERE id = {r}")).unwrap();
        }
        trunk.execute("COMMIT").unwrap();
        if args.versions > 0 {
            fork_one();
        }
    }
    let st = db.branch_stats().unwrap();
    if st.live_branches != n_live(args) {
        not_a_result(&format!("after the trunk commits: {st:?}, expected {} live branches", n_live(args)));
    }
    println!(
        "# victim before checkpoint: trunk_commits={} trunk_retained={} arena_slots_in_use={} {}",
        args.age,
        db.branch_trunk_retained(),
        st.arena_slots_in_use,
        files_line(&args.db)
    );
    checkpoint_truncate(&trunk);
    db.branch_compact_now().unwrap();
    println!(
        "# victim after checkpoint: trunk_retained={} catalog_rows_written={} page_io={:?} {}",
        db.branch_trunk_retained(),
        db.branch_catalog_rows_written(),
        turso_core::branch::page_io(),
        files_line(&args.db)
    );
    println!("READY age={} pid={}", args.age, std::process::id());
    std::io::stdout().flush().unwrap();
    // Block until killed; a closed stdin also ends here, without a clean close.
    let mut line = String::new();
    loop {
        line.clear();
        match std::io::stdin().lock().read_line(&mut line) {
            Ok(0) | Err(_) => std::thread::sleep(Duration::from_secs(3600)),
            Ok(_) => {}
        }
    }
}

fn varint(b: &[u8], mut at: usize) -> Result<(u64, usize), String> {
    let mut v = 0u64;
    for i in 0..9 {
        let byte = *b.get(at).ok_or("a varint runs off the page")?;
        at += 1;
        if i == 8 {
            return Ok(((v << 8) | byte as u64, at));
        }
        v = (v << 7) | (byte & 0x7f) as u64;
        if byte & 0x80 == 0 {
            return Ok((v, at));
        }
    }
    unreachable!()
}

fn bytes<const N: usize>(b: &[u8], at: usize) -> Result<[u8; N], String> {
    b.get(at..at + N)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| format!("{N} bytes at {at} run off the page"))
}

fn be16(b: &[u8], at: usize) -> Result<usize, String> {
    Ok(u16::from_be_bytes(bytes(b, at)?) as usize)
}

/// A table b-tree page: the child to descend for `key` (interior), or the key's cell's `v`
/// (leaf; `None` if the leaf does not hold the key).
enum Step {
    Child(u32),
    Leaf(Option<String>),
}

/// One page of the descent to `key`. An error names what the page does not satisfy: the caller
/// decides whose fault that is (the file's is the harness's premise; the store's is a FINDING).
fn step(page: u32, buf: &[u8], usable: usize, key: i64) -> Result<Step, String> {
    let h = if page == 1 { 100 } else { 0 };
    let n = be16(buf, h + 3)?;
    match buf.get(h).copied() {
        Some(0x05) => {
            let cells = h + 12;
            for i in 0..n {
                let off = be16(buf, cells + 2 * i)?;
                let child = u32::from_be_bytes(bytes(buf, off)?);
                let (k, _) = varint(buf, off + 4)?;
                if key <= k as i64 {
                    return Ok(Step::Child(child));
                }
            }
            Ok(Step::Child(u32::from_be_bytes(bytes(buf, h + 8)?)))
        }
        Some(0x0d) => {
            let cells = h + 8;
            for i in 0..n {
                let off = be16(buf, cells + 2 * i)?;
                let (len, at) = varint(buf, off)?;
                let (rowid, at) = varint(buf, at)?;
                if rowid as i64 != key {
                    continue;
                }
                if len as usize > usable - 35 {
                    return Err(format!("key {key}'s payload ({len} bytes) overflows"));
                }
                let rec = buf.get(at..at + len as usize).ok_or("a cell runs off the page")?;
                let (hs, p) = varint(rec, 0)?;
                let (t0, p) = varint(rec, p)?;
                let (t1, _) = varint(rec, p)?;
                if t0 != 0 || t1 < 13 || t1 % 2 == 0 {
                    return Err(format!("key {key}'s record has serial types {t0}, {t1}"));
                }
                let (body, tlen) = (hs as usize, ((t1 - 13) / 2) as usize);
                let text = rec.get(body..body + tlen).ok_or("a record runs off its cell")?;
                return Ok(Step::Leaf(Some(String::from_utf8_lossy(text).into_owned())));
            }
            Ok(Step::Leaf(None))
        }
        other => Err(format!("page type {other:?} is not a table b-tree page")),
    }
}

struct DbFile {
    f: std::fs::File,
    page_size: usize,
}

impl DbFile {
    fn read(&mut self, page: u32, out: &mut [u8]) {
        self.f.seek(SeekFrom::Start((page as u64 - 1) * self.page_size as u64)).unwrap();
        self.f.read_exact(out).unwrap_or_else(|e| not_a_result(&format!("page {page}: {e}")));
    }
}

/// The path to `key` in the trunk's current tree (the database file): its pages and the leaf's `v`.
fn current_path(file: &mut DbFile, root: u32, usable: usize, key: i64) -> (Vec<u32>, Option<String>) {
    let mut buf = vec![0u8; file.page_size];
    let (mut page, mut path) = (root, Vec::new());
    loop {
        path.push(page);
        file.read(page, &mut buf);
        match step(page, &buf, usable, key) {
            Ok(Step::Child(c)) => page = c,
            Ok(Step::Leaf(v)) => return (path, v),
            Err(e) => not_a_result(&format!("database file page {page}: {e}")),
        }
        if path.len() > 64 {
            not_a_result(&format!("key {key}: no leaf after 64 levels"));
        }
    }
}

fn reopen(args: &Args) {
    let d = draws(args);
    let files_at_open = files_line(&args.db);
    if size_of(&with_suffix(&args.db, "-wal")) != 0 {
        not_a_result("the crash image's WAL is not empty: the database file is not the trunk's current version");
    }
    let io0 = turso_core::branch::page_io();
    let db = open_db(&args.db, true);
    let trunk = db.connect().unwrap();
    let open_stats = db.branch_open_stats();
    let page_size = int(&trunk, "PRAGMA page_size") as usize;
    let root = int(&trunk, "SELECT rootpage FROM sqlite_schema WHERE name = 't'") as u32;
    let mut file = DbFile {
        f: std::fs::File::open(&args.db).unwrap(),
        page_size,
    };
    let mut hdr = vec![0u8; page_size];
    file.read(1, &mut hdr);
    let usable = page_size - hdr[20] as usize;

    // Ground truth, from the database file alone: each key's current path, and the leaves any
    // trunk commit since the fork rewrote (same-size UPDATEs move no page, checked below).
    let written: HashSet<i64> = d.trunk.iter().flatten().copied().collect();
    let mut rewritten_leaves = HashSet::new();
    for &r in &written {
        let (path, v) = current_path(&mut file, root, usable, r);
        if v.is_none() {
            not_a_result(&format!("trunk row {r} is not in the current tree"));
        }
        rewritten_leaves.insert(*path.last().unwrap());
    }
    let mut last_commit: HashMap<i64, u64> = HashMap::new();
    for (g, rows) in d.trunk.iter().enumerate() {
        for &r in rows {
            last_commit.insert(r, g as u64 + 1);
        }
    }

    let v4_0 = db.branch_v4_counters();
    let cat0 = db.branch_catalog_counters();
    let reads0 = db.branch_read_counters();
    let io1 = turso_core::branch::page_io();
    let mut buf = vec![0u8; page_size];
    let mut arena_by_depth = [0u64; 8];
    let mut current_by_depth = [0u64; 8];
    let (mut path_pages, mut base_ok, mut ours_differs, mut paths_equal, mut expect_arena_leaf) =
        (Vec::new(), 0u64, 0u64, 0u64, 0u64);
    let mut findings = Vec::new();
    for &k in &d.keys {
        let (cur_path, ours) = current_path(&mut file, root, usable, k);
        let (mut page, mut path) = (root, Vec::new());
        let base = loop {
            let depth = path.len();
            path.push(page);
            let arena = match db.branch_base_page(B, page, &mut buf) {
                Ok(a) => a,
                Err(e) => {
                    findings.push(format!("key {k}: base page {page} at depth {depth} refused: {e}"));
                    break None;
                }
            };
            if arena {
                arena_by_depth[depth.min(7)] += 1;
            } else {
                current_by_depth[depth.min(7)] += 1;
                file.read(page, &mut buf);
            }
            match step(page, &buf, usable, k) {
                Ok(Step::Child(c)) => page = c,
                Ok(Step::Leaf(v)) => break Some(v),
                Err(e) if arena => {
                    findings.push(format!("key {k}: base page {page} at depth {depth}, from the arena: {e}"));
                    break None;
                }
                Err(e) => not_a_result(&format!("database file page {page}: {e}")),
            }
            if path.len() > 64 {
                findings.push(format!("key {k}: no base leaf after 64 levels"));
                break None;
            }
        };
        path_pages.push(path.len() as u64);
        paths_equal += u64::from(path == cur_path);
        expect_arena_leaf += u64::from(rewritten_leaves.contains(cur_path.last().unwrap()));
        let want_ours = match last_commit.get(&k) {
            Some(g) => trunk_write_value(*g),
            None => trunk_value(k),
        };
        if ours.as_deref() != Some(want_ours.as_str()) {
            not_a_result(&format!("key {k}: the trunk's current row is {ours:?}, expected {want_ours}"));
        }
        match base {
            Some(Some(v)) if v == trunk_value(k) => {
                base_ok += 1;
                ours_differs += u64::from(v != want_ours);
            }
            Some(other) => findings.push(format!("key {k}: base cell {other:?}, expected the fork's {}", trunk_value(k))),
            None => {}
        }
    }
    let v4_1 = db.branch_v4_counters();
    let cat1 = db.branch_catalog_counters();
    let reads1 = db.branch_read_counters();
    let io2 = turso_core::branch::page_io();

    let n = d.keys.len() as u64;
    let arena: u64 = arena_by_depth.iter().sum();
    let h_min = *path_pages.iter().min().unwrap();
    let h_max = *path_pages.iter().max().unwrap();
    let delta = |a: [u64; 4], b: [u64; 4]| [b[0] - a[0], b[1] - a[1], b[2] - a[2], b[3] - a[3]];
    println!("# reopen open_stats {open_stats:?}");
    println!("# reopen files at open: {files_at_open}");
    println!(
        "# ground truth: trunk rows written {} distinct over {} commits; leaves rewritten {}; keys whose current leaf was rewritten {expect_arena_leaf}/{n}; \
         keys whose base path equals the current path {paths_equal}/{n}; H min {h_min} max {h_max}",
        written.len(),
        args.age,
        rewritten_leaves.len()
    );
    let (base_reads, base_arena, base_refused, base_examined) = (
        v4_1.base_reads - v4_0.base_reads,
        v4_1.base_arena - v4_0.base_arena,
        v4_1.base_refused - v4_0.base_refused,
        v4_1.base_examined - v4_0.base_examined,
    );
    let (cp_probes, cp_rows) = (v4_1.cp_probes - v4_0.cp_probes, v4_1.cp_rows - v4_0.cp_rows);
    let (probe_seeks, probe_steps, probe_page_gets) = (
        v4_1.probe_seeks - v4_0.probe_seeks,
        v4_1.probe_steps - v4_0.probe_steps,
        v4_1.probe_page_gets - v4_0.probe_page_gets,
    );
    println!(
        "V4\tage={}\tforks={}\tversions={}\tn_live={}\trows={}\tkeys={n}\tper={}\tseed={}\t\
         base_reads={base_reads}\tbase_arena={base_arena}\tbase_refused={base_refused}\tbase_examined={base_examined}\t\
         cp_probes={cp_probes}\tcp_rows={cp_rows}\tprobe_seeks={probe_seeks}\tprobe_steps={probe_steps}\tprobe_page_gets={probe_page_gets}\t\
         cat_branch_loads={}\tcat_trunk_page_loads={}\tcat_queries={}\tcat_rows_read={}\t\
         resolve_calls={}\tresolve_arena_reads={}\tarena_by_depth={:?}\tcurrent_by_depth={:?}\tpath_pages_total={}\th_min={h_min}\th_max={h_max}\t\
         expect_arena_leaf={expect_arena_leaf}\tleaves_rewritten={}\tpaths_equal={paths_equal}\tbase_ok={base_ok}\tours_differs={ours_differs}\t\
         M={:.4}\tM1={:.4}\tM2_cp_rows_per_arena={}\tM2_examined_per_arena={}\t\
         seeks_per_probe={}\tsteps_per_probe={}\tpage_gets_per_probe={}\tpage_io_open={:?}\tpage_io_v4={:?}",
        args.age,
        args.forks,
        args.versions,
        n_live(args),
        args.rows,
        args.per,
        args.seed,
        cat1.0 - cat0.0,
        cat1.1 - cat0.1,
        cat1.2 - cat0.2,
        cat1.3 - cat0.3,
        reads1.0 - reads0.0,
        reads1.1 - reads0.1,
        &arena_by_depth[..(h_max as usize).min(8)],
        &current_by_depth[..(h_max as usize).min(8)],
        path_pages.iter().sum::<u64>(),
        rewritten_leaves.len(),
        arena as f64 / n as f64,
        path_pages.iter().sum::<u64>() as f64 / n as f64,
        ratio(cp_rows, base_arena),
        ratio(base_examined, base_arena),
        ratio(probe_seeks, cp_probes),
        ratio(probe_steps, cp_probes),
        ratio(probe_page_gets, cp_probes),
        delta(io0, io1),
        delta(io1, io2)
    );
    // A wrong store answer is a FINDING, printed before any harness check can end the run.
    for f in &findings {
        println!("FINDING: {f}");
    }
    let _ = std::io::stdout().flush();
    if base_arena != arena {
        not_a_result(&format!("the store counted {base_arena} arena base reads, the harness {arena}"));
    }

    // Theirs, after the counters are taken (these reads go through the ordinary resolve path).
    let mut later = Vec::new();
    match db.branch(B) {
        Ok(branch) => {
            match branch.connect() {
                Ok(conn) => {
                    for &k in &d.keys {
                        match try_read_v(&conn, k) {
                            Ok(theirs) if theirs == branch_value(k) => {}
                            Ok(theirs) => later.push(format!("key {k}: B reads {theirs}, expected {}", branch_value(k))),
                            Err(e) => later.push(format!("key {k}: B's read failed: {e}")),
                        }
                    }
                }
                Err(e) => later.push(format!("connect to {B:?}: {e}")),
            }
            let _ = branch.into_id();
        }
        Err(e) => later.push(format!("attach {B:?}: {e}")),
    }
    match db.branch_stats() {
        Ok(st) => {
            println!(
                "# reopen after: theirs checked {n} keys; live_branches={} trunk_retained={} {}",
                st.live_branches,
                db.branch_trunk_retained(),
                files_line(&args.db)
            );
            if st.live_branches != n_live(args) {
                later.push(format!("after the reopen: {st:?}, expected {} live branches", n_live(args)));
            }
        }
        Err(e) => later.push(format!("branch_stats after the reopen: {e}")),
    }
    for f in &later {
        println!("FINDING: {f}");
    }
    if !findings.is_empty() || !later.is_empty() {
        let _ = std::io::stdout().flush();
        std::process::exit(3);
    }
    println!("DONE age={}", args.age);
}

/// `num/den` to four places, or `na` when the denominator is 0.
fn ratio(num: u64, den: u64) -> String {
    if den == 0 {
        "na".to_string()
    } else {
        format!("{:.4}", num as f64 / den as f64)
    }
}

fn main() {
    let args = parse_args();
    if cfg!(debug_assertions) {
        println!("# DEBUG build");
    }
    match args.cmd.as_str() {
        "victim" => victim(&args),
        "reopen" => reopen(&args),
        other => die(&format!("unknown command {other}")),
    }
}
