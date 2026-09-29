//! V4 base-diff confirm run (r11-merge lane; PREREG A20 in frontier/round11/r11-merge/PREREG.md).
//!
//!   branch_v4 victim --db PATH --age A [--rows R] [--keys K] [--per M] [--seed S]
//!   branch_v4 reopen --db PATH --age A [--rows R] [--keys K] [--per M] [--seed S]
//!   branch_v4 nvictim --db PATH --live N [--every C] [--sample S] [--skeys K] [--rows R] [--seed S] [--tail]
//!   branch_v4 nreopen --db PATH --live N [--every C] [--sample S] [--skeys K] [--rows R] [--seed S] [--plant]
//!
//! `nvictim`/`nreopen` are lane r12-composition's arm K8-N (frontier/round12/r12-composition/PREREG.md §3):
//! V4's base read on the N axis. N branches are forked from the trunk and detached (live); S of them,
//! evenly spaced, update K keys each (the sampled writers); after every C forks the trunk commits ONE
//! row update, so each commit's pre-image is retained and V ~ N/C. The first key of each sampled writer
//! is the row the trunk's next commit after that writer's fork updates (a true conflict per writer);
//! every other commit's row is random. Then `PRAGMA wal_checkpoint(TRUNCATE)`, and unless `--tail` the
//! branch log is compacted, READY, and the driver SIGKILLs it. `nreopen` performs V4's base read of
//! every sampled key through `Database::branch_base_page`, compares the base cell with the model's
//! value at that writer's fork and V4's verdict (base != the trunk's current row) with the model's
//! truth (the trunk wrote the key after the fork). `--plant` expects the wrong base for the first
//! sampled key (a fire-check: it must print FINDING and exit 3). `victim`/`reopen` are unchanged.
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
    /// K8-N (r12-composition): live forks, forks per trunk commit, sampled writers, keys each.
    live: u64,
    every: u64,
    sample: u64,
    skeys: usize,
    tail: bool,
    plant: bool,
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
        live: 0,
        every: 10,
        sample: 64,
        skeys: 4,
        tail: false,
        plant: false,
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
            "--live" => args.live = val().parse().unwrap_or_else(|_| die("bad --live")),
            "--every" => args.every = val().parse().unwrap_or_else(|_| die("bad --every")),
            "--sample" => args.sample = val().parse().unwrap_or_else(|_| die("bad --sample")),
            "--skeys" => args.skeys = val().parse().unwrap_or_else(|_| die("bad --skeys")),
            "--tail" => args.tail = true,
            "--plant" => args.plant = true,
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.cmd.starts_with('n') {
        if args.db.as_os_str().is_empty() || args.live == 0 || args.every == 0 || args.sample == 0 || args.skeys == 0 {
            die("--db and --live are required; --every, --sample and --skeys must be positive");
        }
        if args.live % args.every != 0 || args.live / args.sample < args.every + 1 {
            die("--live must be a multiple of --every, and the writers must be more than --every forks apart");
        }
        if args.skeys as i64 > args.rows || args.rows < 1 {
            die("--skeys exceeds --rows");
        }
        return args;
    }
    if args.db.as_os_str().is_empty() || args.age == 0 || args.rows < 1 || args.keys == 0 || args.per == 0 {
        die("--db and --age are required; --rows, --keys and --per must be positive");
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
    let trunk = (0..args.age)
        .map(|_| (0..args.per).map(|_| rng.row(args.rows)).collect())
        .collect();
    Draws { keys, trunk }
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
        "# victim pid={} age={} rows={} keys={} per={} seed={} page_size={} page_count={}",
        std::process::id(),
        args.age,
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
    for (g, rows) in d.trunk.iter().enumerate() {
        let value = trunk_write_value(g as u64 + 1);
        trunk.execute("BEGIN").unwrap();
        for &r in rows {
            trunk.execute(format!("UPDATE t SET v = '{value}' WHERE id = {r}")).unwrap();
        }
        trunk.execute("COMMIT").unwrap();
    }
    let st = db.branch_stats().unwrap();
    if st.live_branches != 1 {
        not_a_result(&format!("after the trunk commits: {st:?}, expected 1 live branch"));
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
    println!(
        "V4\tage={}\trows={}\tkeys={n}\tper={}\tseed={}\tbase_reads={}\tbase_arena={}\tbase_refused={}\tbase_examined={}\t\
         cp_probes={}\tcp_rows={}\tcat_branch_loads={}\tcat_trunk_page_loads={}\tcat_queries={}\tcat_rows_read={}\t\
         resolve_calls={}\tresolve_arena_reads={}\tarena_by_depth={:?}\tcurrent_by_depth={:?}\tpath_pages_total={}\th_min={h_min}\th_max={h_max}\t\
         expect_arena_leaf={expect_arena_leaf}\tleaves_rewritten={}\tpaths_equal={paths_equal}\tbase_ok={base_ok}\tours_differs={ours_differs}\t\
         M={:.4}\tM1={:.4}\tM2_cp_rows_per_arena={}\tM2_examined_per_arena={}\tpage_io_open={:?}\tpage_io_v4={:?}",
        args.age,
        args.rows,
        args.per,
        args.seed,
        v4_1.0 - v4_0.0,
        v4_1.1 - v4_0.1,
        v4_1.2 - v4_0.2,
        v4_1.3 - v4_0.3,
        v4_1.4 - v4_0.4,
        v4_1.5 - v4_0.5,
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
        ratio(v4_1.5 - v4_0.5, v4_1.1 - v4_0.1),
        ratio(v4_1.3 - v4_0.3, v4_1.1 - v4_0.1),
        delta(io0, io1),
        delta(io1, io2)
    );
    // A wrong store answer is a FINDING, printed before any harness check can end the run.
    for f in &findings {
        println!("FINDING: {f}");
    }
    let _ = std::io::stdout().flush();
    if v4_1.1 - v4_0.1 != arena {
        not_a_result(&format!("the store counted {} arena base reads, the harness {arena}", v4_1.1 - v4_0.1));
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
            if st.live_branches != 1 {
                later.push(format!("after the reopen: {st:?}, expected 1 live branch"));
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

/// K8-N's plan (r12-composition PREREG §3), drawn in one order by both processes.
struct NPlan {
    /// Fork index (0-based; branch id = index + 1) of each sampled writer, ascending.
    writers: Vec<u64>,
    /// Each sampled writer's keys, distinct within the writer. `keys[j][0]` is the row the trunk's
    /// next commit after writer j's fork updates, so every writer has a true conflict.
    keys: Vec<Vec<i64>>,
    /// The row trunk commit g (1-based) updates, at index g - 1.
    commits: Vec<i64>,
}

fn nplan(args: &Args) -> NPlan {
    let mut rng = Rng(args.seed);
    let gap = args.live / args.sample;
    let writers: Vec<u64> = (0..args.sample).map(|j| j * gap + gap / 2).collect();
    let mut keys = Vec::with_capacity(writers.len());
    for _ in &writers {
        let mut seen = HashSet::new();
        let mut ks = Vec::with_capacity(args.skeys);
        while ks.len() < args.skeys {
            let k = rng.row(args.rows);
            if seen.insert(k) {
                ks.push(k);
            }
        }
        keys.push(ks);
    }
    let mut commits: Vec<i64> = (0..args.live / args.every).map(|_| rng.row(args.rows)).collect();
    for (j, &i) in writers.iter().enumerate() {
        // Fork i sees commits 1..=i/every; the next commit is i/every + 1, at index i/every. The
        // writers are more than `every` forks apart, so no two claim the same commit.
        commits[(i / args.every) as usize] = keys[j][0];
    }
    NPlan {
        writers,
        keys,
        commits,
    }
}

/// Commits a fork at index `i` sees: 1..=`i / every`.
fn seen_commits(args: &Args, i: u64) -> u64 {
    i / args.every
}

fn nvictim(args: &Args) {
    if size_of(&args.db) != 0 {
        not_a_result(&format!("{} exists: every N needs a fresh store", args.db.display()));
    }
    let p = nplan(args);
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
        "# nvictim pid={} live={} every={} sample={} skeys={} rows={} seed={} tail={} page_size={} page_count={}",
        std::process::id(),
        args.live,
        args.every,
        p.writers.len(),
        args.skeys,
        args.rows,
        args.seed,
        args.tail,
        int(&trunk, "PRAGMA page_size"),
        int(&trunk, "PRAGMA page_count")
    );
    let (mut next_writer, mut g) = (0usize, 0u64);
    for i in 0..args.live {
        let branch = trunk
            .fork_branch()
            .unwrap_or_else(|e| not_a_result(&format!("fork {i}: {e}")));
        if branch.id() != BranchId(i + 1) {
            not_a_result(&format!("fork {i} returned {:?}, expected id {}", branch.id(), i + 1));
        }
        if p.writers.get(next_writer) == Some(&i) {
            let conn = branch.connect().unwrap();
            conn.execute("BEGIN").unwrap();
            for &k in &p.keys[next_writer] {
                conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {k}", branch_value(k))).unwrap();
            }
            conn.execute("COMMIT").unwrap();
            drop(conn);
            next_writer += 1;
        }
        let _ = branch.into_id();
        if (i + 1) % args.every == 0 {
            g += 1;
            let r = p.commits[(g - 1) as usize];
            trunk
                .execute(format!("UPDATE t SET v = '{}' WHERE id = {r}", trunk_write_value(g)))
                .unwrap_or_else(|e| not_a_result(&format!("trunk commit {g}: {e}")));
        }
        if (i + 1) % 100_000 == 0 {
            println!("# grown {} forks, {g} trunk commits", i + 1);
            let _ = std::io::stdout().flush();
        }
    }
    if next_writer != p.writers.len() || g as usize != p.commits.len() {
        not_a_result(&format!("wrote {next_writer} writers and {g} commits, planned {} and {}", p.writers.len(), p.commits.len()));
    }
    let st = db.branch_stats().unwrap();
    if st.live_branches as u64 != args.live {
        not_a_result(&format!("after the growth: {st:?}, expected {} live branches", args.live));
    }
    println!(
        "# nvictim before checkpoint: trunk_commits={g} arena_slots_in_use={} {}",
        st.arena_slots_in_use,
        files_line(&args.db)
    );
    checkpoint_truncate(&trunk);
    if !args.tail {
        db.branch_compact_now().unwrap();
    }
    println!(
        "# nvictim after checkpoint: compacted={} catalog_rows_written={} page_io={:?} {}",
        !args.tail,
        db.branch_catalog_rows_written(),
        turso_core::branch::page_io(),
        files_line(&args.db)
    );
    println!("READY live={} pid={}", args.live, std::process::id());
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    loop {
        line.clear();
        match std::io::stdin().lock().read_line(&mut line) {
            Ok(0) | Err(_) => std::thread::sleep(Duration::from_secs(3600)),
            Ok(_) => {}
        }
    }
}

fn nreopen(args: &Args) {
    let p = nplan(args);
    let files_at_open = files_line(&args.db);
    if size_of(&with_suffix(&args.db, "-wal")) != 0 {
        not_a_result("the crash image's WAL is not empty: the database file is not the trunk's current version");
    }
    let io0 = turso_core::branch::page_io();
    let db = open_db(&args.db, true);
    let open_stats = db.branch_open_stats();
    let cat_open = db.branch_catalog_counters();
    let io_open = turso_core::branch::page_io();
    let trunk = db.connect().unwrap();
    let page_size = int(&trunk, "PRAGMA page_size") as usize;
    let root = int(&trunk, "SELECT rootpage FROM sqlite_schema WHERE name = 't'") as u32;
    let mut file = DbFile {
        f: std::fs::File::open(&args.db).unwrap(),
        page_size,
    };
    let mut hdr = vec![0u8; page_size];
    file.read(1, &mut hdr);
    let usable = page_size - hdr[20] as usize;

    // The model, from the plan alone: each row's commits in order, and each leaf's last rewrite
    // (same-size UPDATEs move no row, so a row's current leaf was its leaf at every commit).
    let mut row_commits: HashMap<i64, Vec<u64>> = HashMap::new();
    for (g0, &r) in p.commits.iter().enumerate() {
        row_commits.entry(r).or_default().push(g0 as u64 + 1);
    }
    let mut leaf_of: HashMap<i64, u32> = HashMap::new();
    let mut leaf_last: HashMap<u32, u64> = HashMap::new();
    for (&r, gs) in &row_commits {
        let (path, v) = current_path(&mut file, root, usable, r);
        if v.as_deref() != Some(trunk_write_value(*gs.last().unwrap()).as_str()) {
            not_a_result(&format!("trunk row {r} reads {v:?}, expected commit {}'s value", gs.last().unwrap()));
        }
        let leaf = *path.last().unwrap();
        leaf_of.insert(r, leaf);
        let last = leaf_last.entry(leaf).or_insert(0);
        *last = (*last).max(*gs.last().unwrap());
    }
    let value_at = |r: i64, seen: u64| -> String {
        match row_commits.get(&r).and_then(|gs| gs.iter().rev().find(|&&g| g <= seen)) {
            Some(&g) => trunk_write_value(g),
            None => trunk_value(r),
        }
    };

    let v4_0 = db.branch_v4_counters();
    let cat0 = db.branch_catalog_counters();
    let reads0 = db.branch_read_counters();
    let io1 = turso_core::branch::page_io();
    let mut buf = vec![0u8; page_size];
    let mut arena_by_depth = [0u64; 8];
    let mut current_by_depth = [0u64; 8];
    let mut path_pages = Vec::new();
    let (mut base_ok, mut paths_equal, mut expect_arena_leaf) = (0u64, 0u64, 0u64);
    let (mut truth_conflicts, mut v4_conflicts, mut false_refusals, mut missed) = (0u64, 0u64, 0u64, 0u64);
    let (mut loads_max, mut loads_min) = (0u64, u64::MAX);
    // K8-B (r12-composition amendment 10): every base read bracketed, so db page reads can be tied to
    // the C-P probes and branch loads of that call.
    let twk0 = db.branch_twk_counters();
    let (mut pure_calls, mut pure_probes, mut pure_reads, mut pure_max) = (0u64, 0u64, 0u64, 0u64);
    let (mut load_calls, mut load_reads, mut probes_max) = (0u64, 0u64, 0u64);
    let mut pure_hist = [0u64; 10];
    let mut findings = Vec::new();
    for (j, &i) in p.writers.iter().enumerate() {
        let id = BranchId(i + 1);
        let seen = seen_commits(args, i);
        let loads_before = db.branch_catalog_counters().0;
        for (ki, &k) in p.keys[j].iter().enumerate() {
            let (cur_path, ours) = current_path(&mut file, root, usable, k);
            let ours_want = value_at(k, u64::MAX);
            if ours.as_deref() != Some(ours_want.as_str()) {
                not_a_result(&format!("key {k}: the trunk's current row is {ours:?}, expected {ours_want}"));
            }
            let mut base_want = value_at(k, seen);
            if args.plant && j == 0 && ki == 0 {
                base_want = trunk_write_value(seen + 1);
            }
            let truth = row_commits.get(&k).is_some_and(|gs| gs.iter().any(|&g| g > seen));
            let (mut page, mut path) = (root, Vec::new());
            let base = loop {
                let depth = path.len();
                path.push(page);
                let (r0, p0, l0) = (
                    turso_core::branch::page_io()[0],
                    db.branch_v4_counters().4,
                    db.branch_catalog_counters().0,
                );
                let answer = db.branch_base_page(id, page, &mut buf);
                let (dr, dp, dl) = (
                    turso_core::branch::page_io()[0] - r0,
                    db.branch_v4_counters().4 - p0,
                    db.branch_catalog_counters().0 - l0,
                );
                probes_max = probes_max.max(dp);
                if dl == 0 {
                    pure_calls += 1;
                    pure_probes += dp;
                    pure_reads += dr;
                    pure_max = pure_max.max(dr);
                    pure_hist[(dr as usize).min(9)] += 1;
                } else {
                    load_calls += 1;
                    load_reads += dr;
                }
                let arena = match answer {
                    Ok(a) => a,
                    Err(e) => {
                        findings.push(format!("writer {}: key {k}: base page {page} at depth {depth} refused: {e}", id.0));
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
                        findings.push(format!("writer {}: key {k}: base page {page} at depth {depth}, from the arena: {e}", id.0));
                        break None;
                    }
                    Err(e) => not_a_result(&format!("database file page {page}: {e}")),
                }
                if path.len() > 64 {
                    findings.push(format!("writer {}: key {k}: no base leaf after 64 levels", id.0));
                    break None;
                }
            };
            path_pages.push(path.len() as u64);
            paths_equal += u64::from(path == cur_path);
            let leaf = *cur_path.last().unwrap();
            expect_arena_leaf += u64::from(leaf_last.get(&leaf).is_some_and(|&g| g > seen));
            truth_conflicts += u64::from(truth);
            match base {
                Some(Some(v)) if v == base_want => {
                    base_ok += 1;
                    let verdict = v != ours_want;
                    v4_conflicts += u64::from(verdict);
                    false_refusals += u64::from(verdict && !truth);
                    missed += u64::from(!verdict && truth);
                }
                Some(other) => findings.push(format!(
                    "writer {} (fork {i}, sees {seen} commits): key {k}: base cell {other:?}, expected {base_want}",
                    id.0
                )),
                None => {}
            }
        }
        let loads = db.branch_catalog_counters().0 - loads_before;
        loads_max = loads_max.max(loads);
        loads_min = loads_min.min(loads);
    }
    let v4_1 = db.branch_v4_counters();
    let cat1 = db.branch_catalog_counters();
    let reads1 = db.branch_read_counters();
    let io2 = turso_core::branch::page_io();

    let n = path_pages.len() as u64;
    let arena: u64 = arena_by_depth.iter().sum();
    let h_min = *path_pages.iter().min().unwrap();
    let h_max = *path_pages.iter().max().unwrap();
    let delta = |a: [u64; 4], b: [u64; 4]| [b[0] - a[0], b[1] - a[1], b[2] - a[2], b[3] - a[3]];
    let io_v4 = delta(io1, io2);
    let probes = v4_1.4 - v4_0.4;
    println!("# nreopen open_stats {open_stats:?}");
    println!(
        "# nreopen at open: files {files_at_open}; catalog (branch_loads, trunk_page_loads, queries, rows_read) {cat_open:?}; page_io {:?}",
        delta(io0, io_open)
    );
    println!(
        "# model: trunk commits {}; rows committed {}; leaves rewritten {}; keys whose leaf was rewritten after their writer's fork {expect_arena_leaf}/{n}; \
         base path = current path {paths_equal}/{n}; H min {h_min} max {h_max}",
        p.commits.len(),
        row_commits.len(),
        leaf_last.len()
    );
    println!(
        "V4N\tlive={}\tevery={}\tsample={}\tskeys={}\trows={}\tseed={}\ttail={}\tplant={}\tkeys={n}\tbase_reads={}\tbase_arena={}\tbase_refused={}\t\
         base_examined={}\tcp_probes={probes}\tcp_rows={}\tcat_branch_loads={}\tcat_trunk_page_loads={}\tcat_queries={}\tcat_rows_read={}\t\
         resolve_calls={}\tresolve_arena_reads={}\tarena_by_depth={:?}\tcurrent_by_depth={:?}\tpath_pages_total={}\th_min={h_min}\th_max={h_max}\t\
         expect_arena_leaf={expect_arena_leaf}\tpaths_equal={paths_equal}\tbase_ok={base_ok}\ttruth_conflicts={truth_conflicts}\tv4_conflicts={v4_conflicts}\t\
         false_refusals={false_refusals}\tmissed_conflicts={missed}\tloads_per_writer_min={loads_min}\tloads_per_writer_max={loads_max}\t\
         cp_probes_per_base_read={}\tcp_rows_per_probe={}\tdb_reads_per_probe={}\tpage_io_open={:?}\tpage_io_v4={io_v4:?}",
        args.live,
        args.every,
        p.writers.len(),
        args.skeys,
        args.rows,
        args.seed,
        args.tail,
        args.plant,
        v4_1.0 - v4_0.0,
        v4_1.1 - v4_0.1,
        v4_1.2 - v4_0.2,
        v4_1.3 - v4_0.3,
        v4_1.5 - v4_0.5,
        cat1.0 - cat0.0,
        cat1.1 - cat0.1,
        cat1.2 - cat0.2,
        cat1.3 - cat0.3,
        reads1.0 - reads0.0,
        reads1.1 - reads0.1,
        &arena_by_depth[..(h_max as usize).min(8)],
        &current_by_depth[..(h_max as usize).min(8)],
        path_pages.iter().sum::<u64>(),
        ratio(probes, v4_1.0 - v4_0.0),
        ratio(v4_1.5 - v4_0.5, probes),
        ratio(io_v4[0], probes),
        delta(io0, io_open),
    );
    let twk1 = db.branch_twk_counters();
    let (twk_probes, twk_rows) = (twk1.0 - twk0.0, twk1.1 - twk0.1);
    let cp_rows = v4_1.5 - v4_0.5;
    println!(
        "K8B\tlive={}\ttail={}\tkeys={n}\tarena_per_key={:.4}\tcp_rows_per_key={:.4}\ttwk_probes={twk_probes}\ttwk_rows={twk_rows}\t\
         twk_rows_per_key={:.4}\tother_rows_per_key={:.4}\tpure_calls={pure_calls}\tpure_probes={pure_probes}\tpure_reads={pure_reads}\t\
         reads_per_pure_probe={}\tpure_max_reads_one_call={pure_max}\tpure_hist={pure_hist:?}\tload_calls={load_calls}\tload_reads={load_reads}\t\
         probes_max_one_call={probes_max}\topen_log_bytes={}\topen_records={}",
        args.live,
        args.tail,
        arena as f64 / n as f64,
        cp_rows as f64 / n as f64,
        twk_rows as f64 / n as f64,
        (cp_rows - twk_rows) as f64 / n as f64,
        ratio(pure_reads, pure_probes),
        open_stats.log_bytes,
        open_stats.records,
    );
    for f in &findings {
        println!("FINDING: {f}");
    }
    let _ = std::io::stdout().flush();
    if v4_1.1 - v4_0.1 != arena {
        not_a_result(&format!("the store counted {} arena base reads, the harness {arena}", v4_1.1 - v4_0.1));
    }
    if pure_calls + load_calls != v4_1.0 - v4_0.0 {
        not_a_result(&format!("bracketed {} calls, the store counted {} base reads", pure_calls + load_calls, v4_1.0 - v4_0.0));
    }

    // Theirs and the live count, after the counters are taken (these settle and read through the
    // ordinary paths).
    let mut later = Vec::new();
    for (j, &i) in p.writers.iter().enumerate() {
        let id = BranchId(i + 1);
        match db.branch(id) {
            Ok(branch) => {
                match branch.connect() {
                    Ok(conn) => {
                        for &k in &p.keys[j] {
                            match try_read_v(&conn, k) {
                                Ok(theirs) if theirs == branch_value(k) => {}
                                Ok(theirs) => later.push(format!("writer {}: key {k}: reads {theirs}, expected {}", id.0, branch_value(k))),
                                Err(e) => later.push(format!("writer {}: key {k}: read failed: {e}", id.0)),
                            }
                        }
                    }
                    Err(e) => later.push(format!("connect to {id:?}: {e}")),
                }
                let _ = branch.into_id();
            }
            Err(e) => later.push(format!("attach {id:?}: {e}")),
        }
    }
    match db.branch_stats() {
        Ok(st) => {
            println!(
                "# nreopen after: theirs checked {n} keys; live_branches={} trunk_retained={} {}",
                st.live_branches,
                db.branch_trunk_retained(),
                files_line(&args.db)
            );
            if st.live_branches as u64 != args.live {
                later.push(format!("after the reopen: {st:?}, expected {} live branches", args.live));
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
    println!("DONE live={}", args.live);
}

/// `num/den` to four places, or `na` when nothing was arena-resolved.
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
        "nvictim" => nvictim(&args),
        "nreopen" => nreopen(&args),
        other => die(&format!("unknown command {other}")),
    }
}
