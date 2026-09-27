//! COMP ARM (githost-shape PREREG G5.2): a12-durable-open's C-P + C-R catalog store composed on the port
//! of F1 + F2 + F4 (turso d7a2b8f6e), catalog mode. This file is r2's catalog-arm harness (turso
//! 20fea769f) with the same schedule, model and checks. Only its counters are changed, to C-P's
//! structures and to the walls G5.2 registers (W1 listing under the mutex, W2 the checkpoint's walk,
//! W3 resident states, W4 table growth). Per-op `getrusage` deltas (`ru_*`) are added: block reads and
//! writes, major faults, involuntary context switches, and CPU time. Unlike every other counter these
//! depend on the load and the page cache; they are there to attribute time, never as a result on
//! their own.
//! The git-hosting shape against N long-lived branches of the durable branch store (githost-shape
//! lane, round 11; PREREG in artie-research frontier/round11/githost-shape/PREREG.md, Amendments G1, G5).
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
//! `grow` opens Catalog{sync:false} (same file bytes; no branch fsyncs), runs steps until the model's live
//! count reaches N, checks the invariants, and closes cleanly. It prints counters (and unlocked times,
//! which are NOT timing evidence). The process keeps running across every checkpoint of the growth, so
//! its resident states are those of a long-lived server (W3).
//!
//! `probe` opens Catalog{sync:true}
//! in a fresh process (the open is the restart at N), then measures: the open; the settle of parked
//! commits (C-R) by the first slot count, as a SETTLE line; K_s = min(2500, N/10) further schedule
//! steps, each op kind timed and counted separately; read_old (200 uniform live), read_oldest (the 50
//! smallest live ids), list (5), compact (2); the close. There is no diff probe: D-written is P-bound on
//! the port, and the catalog changes only how the trunk's `written` map is filled.
//!
//! Every value read is checked against the model, and the arena's slot count against 2 x live + trunk
//! versions as a lower bound. The exact slot check is across arms: at the same step count this store
//! and the port make the same retain and free decisions (r2 found them equal at 10^4 and 5x10^5).
//! Any failed check prints `NOT A RESULT` and exits 1.
//!
//! ATTRIBUTION (r11-githost-attr lane, PREREG in artie-research frontier/round11/r11-githost-attr/):
//! pr_update's time grew with N at flat counters. These additions separate the candidate mechanisms
//! and change nothing the store does:
//!   - `--durability eager|catalog` is required: `eager` opens the port's own mode (`Durable`, every
//!     state resident after the open), where the growth was measured; `catalog` is COMP / COMP-F.
//!   - COLD vs WARM: an op on a branch created before this process opened the store and not touched by
//!     it since is COLD, else WARM. pr_update, read_old and read_oldest print per-class OP lines
//!     (`op=pr_update.cold` ...) beside the pooled ones, and one `OPREC` line per op.
//!   - PAIRS: after the reads and before the listings, 100 cold branches from the open-time update
//!     window are updated twice in a row (`pair_update.cold` then `.warm`, the same branch and the
//!     same code path), and 100 more read twice (`pair_read.*`). The model follows, and the sidecar
//!     is then marked tainted, so no later process can read these branches against a replayed
//!     schedule.
//!   - Per-op counters: `BranchIoCounters` (arena pread / pwrite / fsync and record-flush time with
//!     counts, resolution and catalog-load time, C-R's parked commits), Turso's process-wide page
//!     reads (`page_io`), and getrusage split into minor / major faults, voluntary / involuntary
//!     switches and user / system time.
//!   - PAGE CACHE: with `R11_UBC_PROBE` set, the store asks mincore about each arena slot's page just
//!     before reading or writing it (`ubc_*`); with `R11_CENSUS_OPS` set, the harness diffs the page
//!     cache residency of the arena and catalog files across each pr_update, read and pair op
//!     (`cen_*`, outside the timed window; -1 where not taken). `RESIDENT` lines give whole-file
//!     residency at fixed points of the probe, also only with `R11_CENSUS_OPS`. Both cost system
//!     calls and map files, so timed runs leave them unset and map nothing.
//!   - M7 (A1): the store counts and times each schema reparse of a branch connection whose state
//!     carries no schema. `R11_SCHEMA_SHARE` turns on arm F-S (a loaded branch adopts a schema
//!     parsed in this process from byte-identical source rows instead of reparsing).
//!   - `ubc-selftest --file DIR` fire-checks mincore on this OS: a sparse file's pages must read not
//!     resident, pages it just read must read resident, and it reports whether an APFS clone starts
//!     with its source's pages cached.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{
    file_pages_resident, file_residency, page_io, BranchCatShape, BranchDurability, BranchId,
    BranchIoCounters, BranchOpenStats,
};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const ROWS: u64 = 20_000;
const VALUE_LEN: usize = 100;
const OPEN_WINDOW: f64 = 0.0237;
/// Branches per pair series (r11-githost-attr PREREG section 3).
const PAIRS: usize = 100;

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
    /// Trunk `PRAGMA synchronous` during growth: "default" leaves the engine's default.
    trunk_sync: String,
    /// `catalog` (COMP, COMP-F) or `eager` (the port's `Durable` mode); no default is guessed for a
    /// database grown in the other mode, because the files differ and the open would refuse.
    eager: bool,
    /// `ubc-selftest` only: a scratch directory it may create files in.
    file: PathBuf,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it
        .next()
        .unwrap_or_else(|| die("usage: branch_githost grow|probe|ubc-selftest ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        page_size: 1024,
        label: String::new(),
        trunk_sync: "default".to_string(),
        eager: false,
        file: PathBuf::new(),
    };
    let mut durability: Option<String> = None;
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--page-size" => args.page_size = val().parse().unwrap_or_else(|_| die("bad --page-size")),
            "--label" => args.label = val(),
            "--trunk-sync" => args.trunk_sync = val(),
            "--durability" => durability = Some(val()),
            "--file" => args.file = PathBuf::from(val()),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.cmd == "ubc-selftest" {
        if args.file.as_os_str().is_empty() {
            die("ubc-selftest needs --file DIR");
        }
        return args;
    }
    if args.db.as_os_str().is_empty() || args.n == 0 {
        die("--db and --n/--to are required");
    }
    args.eager = match durability.as_deref() {
        Some("catalog") => false,
        Some("eager") => true,
        Some(other) => die(&format!("--durability {other}: expected catalog or eager")),
        None => die("--durability catalog|eager is required (r11-githost-attr: no mode is guessed)"),
    };
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

fn open_db(path: &Path, sync: bool, eager: bool) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    // r11-githost-attr: `eager` is the port's mode (every state resident after the open).
    let durability = if eager {
        BranchDurability::Durable { sync }
    } else {
        BranchDurability::Catalog { sync }
    };
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")));
    // F-W3's knob (githost-shape PREREG G5.3): unset keeps every touched state resident (COMP).
    if let Ok(v) = std::env::var("R11_RESIDENT_CAP") {
        if eager {
            die("R11_RESIDENT_CAP is F-W3's catalog eviction; the eager mode has no catalog to evict to");
        }
        let cap: usize = v.parse().unwrap_or_else(|_| die(&format!("bad R11_RESIDENT_CAP {v:?}")));
        db.branch_set_resident_cap(Some(cap));
    }
    db
}

fn sidecar(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}.githost", db.to_str().unwrap()))
}

/// What a probe with pair ops leaves in the sidecar: those ops rewrote branches outside the schedule,
/// so a later process replaying the schedule would check them against the wrong values.
const TAINT: &str = "tainted=r11-githost-attr pair ops rewrote branches outside the schedule";

fn read_steps(db: &Path) -> u64 {
    match std::fs::read_to_string(sidecar(db)) {
        Ok(s) if s.contains("tainted=") => {
            not_a_result("this database was probed with pair ops (r11-githost-attr); probe a fresh clone of the grown state")
        }
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

fn write_tainted_steps(db: &Path, steps: u64) {
    std::fs::write(sidecar(db), format!("steps={steps}\n{TAINT}\n"))
        .unwrap_or_else(|e| not_a_result(&format!("sidecar: {e}")));
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
        "db_bytes={} wal_bytes={} log_bytes={} snap_bytes={} arena_bytes={} cat_bytes={} catwal_bytes={} \
         hot_bytes={}",
        size_of(db),
        size_of(&with("-wal")),
        size_of(&with("-branch-log")),
        size_of(&with("-branch-snap")),
        size_of(&with("-branch-arena")),
        size_of(&with("-branch-cat")),
        size_of(&with("-branch-cat-wal")),
        size_of(&with("-branch-hot"))
    )
}

// ---------------------------------------------------------------------------------------------
// Page-cache residency (r11-githost-attr). Every answer comes from `file_residency`, the function
// the store's own `R11_UBC_PROBE` uses and `ubc-selftest` fire-checks.

/// The branch files a census reads, by suffix of the database path, in `RESIDENT` order.
const CENSUS_FILES: [(&str, &str); 7] = [
    ("arena", "-branch-arena"),
    ("cat", "-branch-cat"),
    ("catwal", "-branch-cat-wal"),
    ("log", "-branch-log"),
    ("snap", "-branch-snap"),
    ("db", ""),
    ("wal", "-wal"),
];

/// Read-only handles on the branch files that exist when the probe starts. Opened once; a file's
/// length is read at each census, so the arena's appends are seen.
struct Census {
    files: Vec<(&'static str, std::fs::File)>,
}

/// One census: each file's per-page residency, or `None` where the OS refused the query.
type CensusMaps = Vec<Option<Vec<bool>>>;

impl Census {
    fn open(db: &Path) -> Self {
        let files = CENSUS_FILES
            .iter()
            .filter_map(|&(name, suffix)| {
                let path = PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
                std::fs::File::open(path).ok().map(|f| (name, f))
            })
            .collect();
        Self { files }
    }

    fn take(&self) -> CensusMaps {
        self.files
            .iter()
            .map(|(_, f)| {
                let len = f.metadata().ok()?.len();
                file_residency(f, 0, len)
            })
            .collect()
    }

    /// Pages of file `name` that turned resident (within the earlier length), stopped being resident,
    /// and are resident past the earlier length (appended), from `a` to `b`; -1 each when the file is
    /// absent or either census was refused.
    fn diff(&self, name: &str, a: &CensusMaps, b: &CensusMaps) -> (i64, i64, i64) {
        let Some(i) = self.files.iter().position(|(n, _)| *n == name) else {
            return (-1, -1, -1);
        };
        let (Some(a), Some(b)) = (&a[i], &b[i]) else {
            return (-1, -1, -1);
        };
        let common = a.len().min(b.len());
        let new = (0..common).filter(|&p| !a[p] && b[p]).count() as i64;
        let gone = (0..common).filter(|&p| a[p] && !b[p]).count() as i64;
        let grown = b[common..].iter().filter(|&&r| r).count() as i64;
        (new, gone, grown)
    }

    /// Whole-file residency now, as a `RESIDENT` line: resident pages and pages per file.
    fn line(&self, n: u64, label: &str, at: &str) -> String {
        let mut line = format!("RESIDENT\tn={n}\tlabel={label}\tat={at}\tvm_page={}", vm_page());
        for (name, f) in &self.files {
            let answer = f
                .metadata()
                .ok()
                .and_then(|m| file_pages_resident(f, 0, m.len()));
            match answer {
                Some((r, p)) => line.push_str(&format!("\t{name}_resident_pages={r}\t{name}_pages={p}")),
                None => line.push_str(&format!("\t{name}_resident_pages=REFUSED\t{name}_pages=REFUSED")),
            }
        }
        line
    }
}

fn vm_page() -> u64 {
    // SAFETY: sysconf reads a constant.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(p).unwrap_or(0)
}

/// `ubc-selftest --file DIR`: force the residency instrument to answer both ways on this OS before
/// any run reads it (r11-githost-attr PREREG section 4). Writes three 64 MiB files and one clone
/// under DIR and deletes them. PASS needs all of:
///   (a) a sparse file, never written or read, reads 0 pages resident (the instrument can say "no");
///   (b) after reading every 4th page of a file written through an F_NOCACHE descriptor, every page
///       read reads resident (it can say "yes" to a page a read brought in);
///   (c) a file just written through the cache and fsynced reads at least half its pages resident;
///   (d) after reading every 4th page of an APFS clone of (c), every page read reads resident;
///   (e) at least one real-data file read "not resident" for at least half its pages before anything
///       read it (the F_NOCACHE file, or the clone at birth). Without (e), "resident" could mean
///       "has blocks on disk" rather than "cached", and the verdict is INCONCLUSIVE.
/// It REPORTS, as a fact and not a condition, how many of the clone's pages read resident before
/// anything read the clone: whether a fresh clone starts cold, which the probes rely on (they run
/// on clones). Reading the hole is reported too: whether a read of a hole caches zero pages.
/// Apple only (clonefile, F_NOCACHE): the lane's box.
#[cfg(not(target_vendor = "apple"))]
fn ubc_selftest(_dir: &Path) {
    die("ubc-selftest: Apple targets only");
}

#[cfg(target_vendor = "apple")]
fn ubc_selftest(dir: &Path) {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    const LEN: u64 = 64 << 20;
    let page = vm_page();
    if page == 0 {
        println!("UBC_SELFTEST\tverdict=FAIL\twhy=sysconf(_SC_PAGESIZE) failed");
        std::process::exit(1);
    }
    let pages = LEN / page;
    std::fs::create_dir_all(dir).unwrap_or_else(|e| die(&format!("selftest dir: {e}")));
    let hole = dir.join("selftest_hole");
    let nocache = dir.join("selftest_nocache");
    let written = dir.join("selftest_written");
    let clone = dir.join("selftest_clone");
    let all = [&hole, &nocache, &written, &clone];
    for p in all {
        let _ = std::fs::remove_file(p);
    }
    let count = |f: &std::fs::File| file_pages_resident(f, 0, LEN).map(|(r, _)| r);
    // Every 4th page, one byte each: the pages the positive controls require resident.
    let touch = |f: &std::fs::File| -> Vec<usize> {
        let mut b = [0u8; 1];
        (0..pages as usize)
            .step_by(4)
            .inspect(|&p| {
                f.read_exact_at(&mut b, p as u64 * page)
                    .unwrap_or_else(|e| die(&format!("selftest read: {e}")))
            })
            .collect()
    };
    let read_resident = |f: &std::fs::File, read: &[usize]| -> Option<usize> {
        let map = file_residency(f, 0, LEN)?;
        Some(read.iter().filter(|&&p| map[p]).count())
    };
    let chunk = vec![0xA5u8; 1 << 20];
    let fill = |f: &mut std::fs::File| {
        for _ in 0..(LEN >> 20) {
            f.write_all(&chunk).unwrap_or_else(|e| die(&format!("selftest write: {e}")));
        }
        f.sync_all().unwrap_or_else(|e| die(&format!("selftest fsync: {e}")));
    };

    // (a) and the hole's read, reported.
    let h = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&hole)
        .unwrap_or_else(|e| die(&format!("selftest hole: {e}")));
    h.set_len(LEN).unwrap_or_else(|e| die(&format!("selftest set_len: {e}")));
    let hole_before = count(&h);
    let hole_read = touch(&h);
    let hole_after = read_resident(&h, &hole_read);

    // (b): written with the page cache off for that descriptor, then read through another.
    {
        let mut f = std::fs::File::create(&nocache).unwrap_or_else(|e| die(&format!("selftest nocache: {e}")));
        // SAFETY: a valid descriptor owned by `f`; F_NOCACHE takes an int.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
            die("selftest: fcntl(F_NOCACHE) refused");
        }
        fill(&mut f);
    }
    let n = std::fs::File::open(&nocache).unwrap_or_else(|e| die(&format!("selftest nocache: {e}")));
    let nocache_before = count(&n);
    let nocache_read = touch(&n);
    let nocache_after = read_resident(&n, &nocache_read);

    // (c), then the clone for (d) and the report.
    // Read and write: `file_residency` maps with PROT_READ, which fails on a write-only descriptor
    // (`File::create`), and that made (c) false on every run (r11-githost-attr-refute (c), A1).
    let mut w = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&written)
        .unwrap_or_else(|e| die(&format!("selftest written: {e}")));
    fill(&mut w);
    let written_resident = count(&w);
    let (src, dst) = (
        std::ffi::CString::new(written.to_str().unwrap()).unwrap(),
        std::ffi::CString::new(clone.to_str().unwrap()).unwrap(),
    );
    // SAFETY: two NUL-terminated paths that live across the call; flags 0.
    let rc = unsafe { libc::clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    let (clone_birth, clone_after, clone_read) = if rc == 0 {
        let k = std::fs::File::open(&clone).unwrap_or_else(|e| die(&format!("selftest clone: {e}")));
        let birth = count(&k);
        let read = touch(&k);
        (birth, read_resident(&k, &read), read.len())
    } else {
        (None, None, 0)
    };
    for p in all {
        let _ = std::fs::remove_file(p);
    }

    let a = hole_before == Some(0);
    let b = nocache_after == Some(nocache_read.len());
    let c = written_resident.is_some_and(|r| r * 2 >= pages);
    let d = rc == 0 && clone_after == Some(clone_read);
    let e = nocache_before.is_some_and(|r| r * 2 < pages) || clone_birth.is_some_and(|r| r * 2 < pages);
    let verdict = match (a && b && c && d, e) {
        (true, true) => "PASS",
        (true, false) => "INCONCLUSIVE",
        (false, _) => "FAIL",
    };
    let show = |v: Option<u64>| v.map_or("REFUSED".to_string(), |r| r.to_string());
    let show_n = |v: Option<usize>| v.map_or("REFUSED".to_string(), |r| r.to_string());
    println!(
        "UBC_SELFTEST\tverdict={verdict}\tvm_page={page}\tpages={pages}\ta={a}\tb={b}\tc={c}\td={d}\te={e}\t\
         hole_resident_before_read={}\thole_pages_read={}\thole_read_pages_resident={}\t\
         nocache_resident_before_read={}\tnocache_pages_read={}\tnocache_read_pages_resident={}\t\
         written_resident={}\tclone_rc={rc}\tclone_resident_at_birth={}\tclone_pages_read={clone_read}\t\
         clone_read_pages_resident={}",
        show(hole_before),
        hole_read.len(),
        show_n(hole_after),
        show(nocache_before),
        nocache_read.len(),
        show_n(nocache_after),
        show(written_resident),
        show(clone_birth),
        show_n(clone_after),
    );
    if verdict != "PASS" {
        std::process::exit(1);
    }
}

fn shape_line(s: &BranchCatShape) -> String {
    format!(
        "checkpoints={} checkpoint_ns={} ckpt_trunk_inserted={} ckpt_trunk_deleted={} ckpt_branch_rows={} \
         ckpt_rows_written={} ckpt_states_walked={} ids_calls={} ids_resident_visited={} ids_catalog_rows={} \
         ids_build_rows={} table_grows={} table_moved={} evictions={} evicted_states={} resident_states={} \
         dirty_branches={} trunk_overlay_versions={} trunk_cache_versions={} trunk_cache_pages={} \
         trunk_known_pages={} trunk_probes={} trunk_rows={} log_len={}",
        s.checkpoints,
        s.checkpoint_ns,
        s.ckpt_trunk_inserted,
        s.ckpt_trunk_deleted,
        s.ckpt_branch_rows,
        s.ckpt_rows_written,
        s.ckpt_states_walked,
        s.ids_calls,
        s.ids_resident_visited,
        s.ids_catalog_rows,
        s.ids_build_rows,
        s.table_grows,
        s.table_moved,
        s.evictions,
        s.evicted_states,
        s.resident_states,
        s.dirty_branches,
        s.trunk_overlay_versions,
        s.trunk_cache_versions,
        s.trunk_cache_pages,
        s.trunk_known_pages,
        s.trunk_probes,
        s.trunk_rows,
        s.log_len
    )
}

fn open_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} arena_high_water={} \
         arena_free={} states={} released_scanned={} branch_loads={} trunk_page_loads={} cat_queries={} \
         cat_rows_read={} touched_slots={} trunk_probes={} trunk_rows={} parked_records={} parked_applied={} \
         catalog_us={:.1} recover_us={:.1} load_us={:.1} replay_us={:.1} \
         collect_us={:.1} referenced_us={:.1} arena_us={:.1} expire_us={:.1} store_total_us={:.1} \
         prewarm_file={} prewarm_slots={} prewarm_read_slots={} prewarm_bytes={} prewarm_ranges={} \
         prewarm_us={:.1}",
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
        s.parked_applied,
        s.catalog_ns as f64 / 1e3,
        s.recover_ns as f64 / 1e3,
        s.load_ns as f64 / 1e3,
        s.replay_ns as f64 / 1e3,
        s.collect_ns as f64 / 1e3,
        s.referenced_ns as f64 / 1e3,
        s.arena_ns as f64 / 1e3,
        s.expire_ns as f64 / 1e3,
        s.total_ns as f64 / 1e3,
        s.prewarm_file,
        s.prewarm_slots,
        s.prewarm_read_slots,
        s.prewarm_bytes,
        s.prewarm_ranges,
        s.prewarm_ns as f64 / 1e3
    )
}

/// The counters one operation is charged: deltas of every integer the catalog store exposes. Nothing here
/// queries the catalog (`branch_cat_shape` reads memory only), so taking them does not move them.
/// r11-githost-attr adds `io` (memory only) and Turso's process-wide page reads. `ru` is read FIRST,
/// so a before/after pair charges the op with the first snap's store reads and nothing after the
/// second's getrusage; `measure` takes any census outside both.
#[derive(Clone)]
struct Snap {
    cat: (u64, u64, u64, u64),
    rows_written: u64,
    reads: (u64, u64),
    in_use: i64,
    shape: BranchCatShape,
    ru: Usage,
    io: BranchIoCounters,
    pio: [u64; 4],
}

/// This process's resource usage (`getrusage(RUSAGE_SELF)`): an attribution instrument for time,
/// NOT load-immune (it sees the page cache, the scheduler and every thread of the process). On this
/// macOS `inblock` stayed 0 for a cold 188 MB read (sota-durable RESUME.md:72), so it is recorded and
/// predicted blind; `minflt`/`majflt` count faults on this process's own memory (the arena is read
/// with pread, never mapped).
#[derive(Clone, Copy, Default)]
struct Usage {
    inblock: i64,
    oublock: i64,
    majflt: i64,
    nivcsw: i64,
    cpu_us: i64,
    minflt: i64,
    nvcsw: i64,
    utime_us: i64,
    stime_us: i64,
}

fn usage() -> Usage {
    // SAFETY: `rusage` is plain old data; getrusage writes the whole struct we pass and owns nothing.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: a valid, exclusively borrowed rusage for RUSAGE_SELF.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if rc != 0 {
        not_a_result(&format!("getrusage failed: {}", std::io::Error::last_os_error()));
    }
    let us = |tv: libc::timeval| tv.tv_sec as i64 * 1_000_000 + tv.tv_usec as i64;
    Usage {
        inblock: ru.ru_inblock as i64,
        oublock: ru.ru_oublock as i64,
        majflt: ru.ru_majflt as i64,
        nivcsw: ru.ru_nivcsw as i64,
        cpu_us: us(ru.ru_utime) + us(ru.ru_stime),
        minflt: ru.ru_minflt as i64,
        nvcsw: ru.ru_nvcsw as i64,
        utime_us: us(ru.ru_utime),
        stime_us: us(ru.ru_stime),
    }
}

fn snap(db: &Arc<Database>) -> Snap {
    // getrusage before any store read (see `Snap`).
    let ru = usage();
    let st = db.branch_stats().unwrap();
    Snap {
        ru,
        cat: db.branch_catalog_counters(),
        rows_written: db.branch_catalog_rows_written(),
        reads: db.branch_read_counters(),
        in_use: st.arena_slots_in_use as i64,
        shape: db.branch_cat_shape(),
        io: db.branch_io_counters(),
        pio: page_io(),
    }
}

/// Every counter an op is charged. The first twenty are the store's own integers; `ru_*` are
/// `getrusage` deltas (load-dependent; attribution only); from `arena_file_reads` on, r11-githost-attr's:
/// `BranchIoCounters` deltas (`_ns` timers are load-dependent, their paired counts are not), Turso's
/// process-wide page reads from database files and WAL frames (`pio_*`: the trunk's and the
/// catalog's), and the census diffs (`cen_*`: pages that turned resident, left, or were appended
/// resident, across the op; -1 where no census was taken), then M7's schema reparses and F-S's
/// source-key reads and adoptions (A1), each `_ns` paired with its count.
const NC: usize = 62;
const COUNTERS: [&str; NC] = [
    "resolve_calls",
    "arena_reads",
    "arena_delta",
    "cat_branch_loads",
    "cat_trunk_page_loads",
    "cat_queries",
    "cat_rows_read",
    "cat_rows_written",
    "trunk_probes",
    "trunk_rows",
    "checkpoints",
    "ckpt_trunk_inserted",
    "ckpt_trunk_deleted",
    "ckpt_rows_written",
    "ckpt_states_walked",
    "ids_resident_visited",
    "ids_catalog_rows",
    "ids_build_rows",
    "table_moved",
    "evicted_states",
    "ru_inblock",
    "ru_oublock",
    "ru_majflt",
    "ru_nivcsw",
    "ru_cpu_us",
    "ru_minflt",
    "ru_nvcsw",
    "ru_utime_us",
    "ru_stime_us",
    "arena_file_reads",
    "arena_read_ns",
    "arena_file_writes",
    "arena_write_ns",
    "arena_syncs",
    "arena_sync_ns",
    "ubc_read_hits",
    "ubc_read_misses",
    "ubc_read_unknown",
    "ubc_write_hits",
    "ubc_write_misses",
    "ubc_write_appends",
    "ubc_write_unknown",
    "resolve_timed",
    "resolve_ns",
    "flushes",
    "flush_ns",
    "cat_loading_ensures",
    "cat_load_ns",
    "parked_applied",
    "pio_db_reads",
    "pio_wal_reads",
    "cen_arena_new",
    "cen_arena_gone",
    "cen_arena_grown",
    "cen_cat_new",
    "cen_cat_gone",
    "cen_catwal_new",
    "schema_reparses",
    "schema_reparse_ns",
    "schema_keys",
    "schema_key_ns",
    "schema_adoptions",
];

/// `cen`: the census and its maps before and after the op, when one was taken.
fn delta(a: &Snap, b: &Snap, cen: Option<(&Census, &CensusMaps, &CensusMaps)>) -> [i64; NC] {
    let d = |x: u64, y: u64| y as i64 - x as i64;
    let (s, t) = (&a.shape, &b.shape);
    let (i, j) = (&a.io, &b.io);
    let cen = |name: &str| match cen {
        Some((c, x, y)) => c.diff(name, x, y),
        None => (-1, -1, -1),
    };
    let (arena_new, arena_gone, arena_grown) = cen("arena");
    let (cat_new, cat_gone, _) = cen("cat");
    let (catwal_new, _, catwal_grown) = cen("catwal");
    [
        d(a.reads.0, b.reads.0),
        d(a.reads.1, b.reads.1),
        b.in_use - a.in_use,
        d(a.cat.0, b.cat.0),
        d(a.cat.1, b.cat.1),
        d(a.cat.2, b.cat.2),
        d(a.cat.3, b.cat.3),
        d(a.rows_written, b.rows_written),
        d(s.trunk_probes, t.trunk_probes),
        d(s.trunk_rows, t.trunk_rows),
        d(s.checkpoints, t.checkpoints),
        d(s.ckpt_trunk_inserted, t.ckpt_trunk_inserted),
        d(s.ckpt_trunk_deleted, t.ckpt_trunk_deleted),
        d(s.ckpt_rows_written, t.ckpt_rows_written),
        d(s.ckpt_states_walked, t.ckpt_states_walked),
        d(s.ids_resident_visited, t.ids_resident_visited),
        d(s.ids_catalog_rows, t.ids_catalog_rows),
        d(s.ids_build_rows, t.ids_build_rows),
        d(s.table_moved, t.table_moved),
        d(s.evicted_states, t.evicted_states),
        b.ru.inblock - a.ru.inblock,
        b.ru.oublock - a.ru.oublock,
        b.ru.majflt - a.ru.majflt,
        b.ru.nivcsw - a.ru.nivcsw,
        b.ru.cpu_us - a.ru.cpu_us,
        b.ru.minflt - a.ru.minflt,
        b.ru.nvcsw - a.ru.nvcsw,
        b.ru.utime_us - a.ru.utime_us,
        b.ru.stime_us - a.ru.stime_us,
        d(i.arena_file_reads, j.arena_file_reads),
        d(i.arena_read_ns, j.arena_read_ns),
        d(i.arena_file_writes, j.arena_file_writes),
        d(i.arena_write_ns, j.arena_write_ns),
        d(i.arena_syncs, j.arena_syncs),
        d(i.arena_sync_ns, j.arena_sync_ns),
        d(i.ubc_read_hits, j.ubc_read_hits),
        d(i.ubc_read_misses, j.ubc_read_misses),
        d(i.ubc_read_unknown, j.ubc_read_unknown),
        d(i.ubc_write_hits, j.ubc_write_hits),
        d(i.ubc_write_misses, j.ubc_write_misses),
        d(i.ubc_write_appends, j.ubc_write_appends),
        d(i.ubc_write_unknown, j.ubc_write_unknown),
        d(i.resolve_timed, j.resolve_timed),
        d(i.resolve_ns, j.resolve_ns),
        d(i.flushes, j.flushes),
        d(i.flush_ns, j.flush_ns),
        d(i.cat_loading_ensures, j.cat_loading_ensures),
        d(i.cat_load_ns, j.cat_load_ns),
        d(i.parked_applied, j.parked_applied),
        d(a.pio[0], b.pio[0]),
        d(a.pio[2], b.pio[2]),
        arena_new,
        arena_gone,
        arena_grown,
        cat_new,
        cat_gone,
        if catwal_new < 0 { catwal_new } else { catwal_new + catwal_grown },
        d(i.schema_reparses, j.schema_reparses),
        d(i.schema_reparse_ns, j.schema_reparse_ns),
        d(i.schema_keys, j.schema_keys),
        d(i.schema_key_ns, j.schema_key_ns),
        d(i.schema_adoptions, j.schema_adoptions),
    ]
}

/// The fields an `OPREC` line carries, by name from `COUNTERS`: what separates the registered
/// mechanisms (PREREG section 3), one line per op, so any split can be made after the run.
const OPREC_FIELDS: [&str; 38] = [
    "resolve_calls",
    "arena_reads",
    "cat_branch_loads",
    "cat_queries",
    "trunk_probes",
    "ru_majflt",
    "ru_minflt",
    "ru_nvcsw",
    "ru_nivcsw",
    "ru_utime_us",
    "ru_stime_us",
    "arena_file_reads",
    "arena_read_ns",
    "arena_file_writes",
    "arena_write_ns",
    "arena_syncs",
    "arena_sync_ns",
    "ubc_read_hits",
    "ubc_read_misses",
    "ubc_write_misses",
    "ubc_write_appends",
    "resolve_ns",
    "flushes",
    "flush_ns",
    "cat_load_ns",
    "parked_applied",
    "pio_db_reads",
    "pio_wal_reads",
    "cen_arena_new",
    "cen_arena_gone",
    "cen_cat_new",
    "cat_loading_ensures",
    "ubc_write_hits",
    "schema_reparses",
    "schema_reparse_ns",
    "schema_keys",
    "schema_key_ns",
    "schema_adoptions",
];

/// One `OPREC` line: the op, its sequence number in the probe, the branch, the branch's age in steps
/// when the op ran (0 for reads, which run after the steady phase), its class, its time, and
/// `OPREC_FIELDS`.
fn oprec(op: &str, seq: usize, id: u64, age: u64, cold: bool, t_us: f64, c: &[i64; NC]) {
    let mut line = format!(
        "OPREC\top={op}\tseq={seq}\tid={id}\tage={age}\tcls={}\tt_us={t_us:.2}",
        if cold { "cold" } else { "warm" }
    );
    for name in OPREC_FIELDS {
        let i = COUNTERS
            .iter()
            .position(|&n| n == name)
            .unwrap_or_else(|| die(&format!("OPREC field {name} is not a counter")));
        line.push_str(&format!("\t{name}={}", c[i]));
    }
    println!("{line}");
}

/// Time `f` between two snaps. The census, if any, is taken before the first snap and after the
/// second, so neither its time nor its getrusage is charged to the op.
fn measure(db: &Arc<Database>, census: Option<&Census>, f: &mut dyn FnMut()) -> (f64, [i64; NC]) {
    let before = census.map(Census::take);
    let a = snap(db);
    let t = Instant::now();
    f();
    let us = t.elapsed().as_secs_f64() * 1e6;
    let b = snap(db);
    let after = census.map(Census::take);
    let cen = match (census, &before, &after) {
        (Some(c), Some(x), Some(y)) => Some((c, x, y)),
        _ => None,
    };
    (us, delta(&a, &b, cen))
}

/// What this probe process has done to each branch, for the COLD / WARM class (r11-githost-attr): an
/// op is COLD when its branch was created before this process opened the store (`id <= steps0`)
/// and nothing in this process has touched it since (created, updated, read, reaped or paired).
struct Touched {
    steps0: u64,
    touched: Vec<bool>,
}

impl Touched {
    fn new(steps0: u64) -> Self {
        Self {
            steps0,
            touched: Vec::new(),
        }
    }

    fn is_cold(&self, id: u64) -> bool {
        id <= self.steps0 && !self.touched.get(id as usize).copied().unwrap_or(false)
    }

    fn touch(&mut self, id: u64) {
        let i = id as usize;
        if self.touched.len() <= i {
            self.touched.resize(i + 1, false);
        }
        self.touched[i] = true;
    }
}

/// Per-op times (us) and counter deltas for one op kind.
#[derive(Default)]
struct Series {
    times: Vec<f64>,
    counters: Vec<[i64; NC]>,
    extra: Vec<Vec<(String, i64)>>,
}

impl Series {
    fn push(&mut self, t_us: f64, c: [i64; NC]) {
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
/// r11-githost-attr: pr_update is also split by class, and the probe's context rides along.
struct Ops {
    new_pr: Series,
    merge: Series,
    pr_update: Series,
    reap: Series,
    pr_update_cold: Series,
    pr_update_warm: Series,
    touched: Touched,
    /// A census around each pr_update (`R11_CENSUS_OPS`); new_pr, merge and reap never take one.
    census: Option<Census>,
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
fn step(db: &Arc<Database>, trunk: &Arc<Connection>, model: &mut Model, ops: Option<&mut Ops>) {
    let plan = model.advance();
    let Some(ops) = ops else {
        do_new_pr(db, trunk, plan.s);
        if let Some(row) = plan.merge {
            do_merge(trunk, plan.s, row);
        }
        if let Some((t, gen)) = plan.update {
            do_update(db, t, gen);
        }
        if let Some(t) = plan.death {
            do_reap(db, t);
        }
        return;
    };
    let (us, c) = measure(db, None, &mut || do_new_pr(db, trunk, plan.s));
    ops.new_pr.push(us, c);
    ops.touched.touch(plan.s);
    if let Some(row) = plan.merge {
        let (us, c) = measure(db, None, &mut || do_merge(trunk, plan.s, row));
        ops.merge.push(us, c);
    }
    if let Some((t, gen)) = plan.update {
        let cold = ops.touched.is_cold(t);
        let (us, c) = measure(db, ops.census.as_ref(), &mut || do_update(db, t, gen));
        ops.pr_update.push(us, c);
        if cold {
            ops.pr_update_cold.push(us, c);
        } else {
            ops.pr_update_warm.push(us, c);
        }
        oprec("pr_update", ops.pr_update.times.len(), t, plan.s - t, cold, us, &c);
        ops.touched.touch(t);
    }
    if let Some(t) = plan.death {
        let (us, c) = measure(db, None, &mut || do_reap(db, t));
        ops.reap.push(us, c);
        ops.touched.touch(t);
    }
}

/// Invariants the catalog store can check alone: live states == the model's live count, and the arena
/// holds at least two own pages per live branch plus the trunk's versions. The EXACT slot check is across
/// arms: at the same step count the port's grow/probe files print its exact arena and trunk counts, and the
/// two stores make the same retain and free decisions (analysis compares them). The trunk count QUERIES
/// the catalog (`branch_trunk_retained`: overlay + catalog rows - reaped since), so this runs only
/// outside measured operations.
fn check_invariants(db: &Arc<Database>, model: &Model, what: &str) {
    let st = db.branch_stats().unwrap();
    let trunk_versions = db.branch_trunk_retained();
    if st.live_branches as u64 != model.live {
        not_a_result(&format!("{what}: store has {} branch states, model {}", st.live_branches, model.live));
    }
    if (st.arena_slots_in_use as u64) < 2 * model.live + trunk_versions {
        not_a_result(&format!(
            "{what}: arena in use {} < 2 x live {} + trunk versions {trunk_versions}",
            st.arena_slots_in_use, model.live
        ));
    }
    println!(
        "# invariants {what}: live={} steps={} arena_in_use={} trunk_versions={trunk_versions}",
        model.live, model.steps, st.arena_slots_in_use
    );
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
    let db = open_db(&args.db, false, args.eager);
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
    // Trunk durability during growth only (the probe opens with the default). The smoke run under OFF
    // never restarted the trunk WAL (githost-shape raw/smoke2_grow_1024_5000.txt): the default is the
    // registered setting, OFF an arm.
    if args.trunk_sync != "default" {
        exec(&trunk, &format!("PRAGMA synchronous = {}", args.trunk_sync));
    }
    println!("# trunk synchronous during growth: {} ({})", int(&trunk, "PRAGMA synchronous"), args.trunk_sync);
    let wal_path = PathBuf::from(format!("{}-wal", args.db.to_str().unwrap()));
    let mut model = Model::replay(steps0);
    check_invariants(&db, &model, "grow start");
    let s0 = db.branch_cat_shape();
    let t0 = Instant::now();
    let mut next_report = model.live.next_power_of_two().max(1024);
    while model.live < args.n {
        step(&db, &trunk, &mut model, None);
        if model.live >= next_report {
            let sh = db.branch_cat_shape();
            println!(
                "# grow live={} steps={} elapsed_s={:.1} rss={} wal_bytes={} {}",
                model.live,
                model.steps,
                t0.elapsed().as_secs_f64(),
                rss_bytes(),
                size_of(&wal_path),
                shape_line(&sh)
            );
            let _ = std::io::stdout().flush();
            next_report *= 2;
        }
    }
    let grow_s = t0.elapsed().as_secs_f64();
    check_invariants(&db, &model, "grow end");
    let sh = db.branch_cat_shape();
    let st = db.branch_stats().unwrap();
    // Untimed spot checks of the model against the store.
    let live = model.live_ids();
    for i in 0..20u64 {
        let id = live[(mix(i ^ model.steps) % live.len() as u64) as usize];
        read_branch(&db, &model, id, mix(i));
    }
    println!(
        "GROW\tn={}\tsteps={}\tfrom_steps={steps0}\tgrow_s_unlocked={grow_s:.1}\tupdates_skipped={}\tdeaths_skipped={}\t\
         arena_in_use={}\tarena_free={}\tpage_count={}\trss={}\t{}\tcheckpoints_this_run={}\t\
         ckpt_trunk_inserted_this_run={}\tckpt_states_walked_this_run={}\tckpt_rows_written_this_run={}",
        model.live,
        model.steps,
        model.updates_skipped,
        model.deaths_skipped,
        st.arena_slots_in_use,
        st.arena_slots_free,
        int(&trunk, "PRAGMA page_count"),
        rss_bytes(),
        shape_line(&sh).replace(' ', "\t"),
        sh.checkpoints - s0.checkpoints,
        sh.ckpt_trunk_inserted - s0.ckpt_trunk_inserted,
        sh.ckpt_states_walked - s0.ckpt_states_walked,
        sh.ckpt_rows_written - s0.ckpt_rows_written
    );
    write_steps(&args.db, model.steps);
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!("# grow closed in {:.1} us; {}", t.elapsed().as_secs_f64() * 1e6, files_line(&args.db));
}

/// One timed read of `plan`, charged to the pooled series and to its class's (`series` = pooled,
/// cold, warm), with its `OPREC` line; then the branch counts as touched.
fn timed_read(
    db: &Arc<Database>,
    census: Option<&Census>,
    touched: &mut Touched,
    op: &str,
    seq: usize,
    plan: &ReadPlan,
    series: &mut [Series; 3],
) {
    let cold = touched.is_cold(plan.id);
    let (us, c) = measure(db, census, &mut || exec_read(db, plan));
    let extra = vec![("rows_read".to_string(), plan.reads.len() as i64)];
    for k in [0, if cold { 1 } else { 2 }] {
        series[k].push(us, c);
        series[k].extra.push(extra.clone());
    }
    oprec(op, seq, plan.id, 0, cold, us, &c);
    touched.touch(plan.id);
}

fn probe(args: &Args) {
    let steps0 = read_steps(&args.db);
    if steps0 == 0 {
        not_a_result("probe of an ungrown database");
    }
    let files_before = files_line(&args.db);
    let rss0 = rss_bytes();
    let t = Instant::now();
    let db = open_db(&args.db, true, args.eager);
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let connect_us = t.elapsed().as_secs_f64() * 1e6;
    let rss1 = rss_bytes();
    // r11-githost-attr: whole-file page-cache residency at fixed points (system calls only; the
    // files' data is never read by the census). Only with R11_CENSUS_OPS set (A1): a timed run maps
    // nothing, as the banked harness did, because whether mapping and unmapping a file changes later
    // write or fsync timing on APFS is unmeasured.
    let census_on = std::env::var_os("R11_CENSUS_OPS").is_some();
    let whole = census_on.then(|| Census::open(&args.db));
    let resident = |n: u64, label: &str, at: &str| {
        if let Some(whole) = &whole {
            println!("{}", whole.line(n, label, at));
        }
    };
    resident(args.n, &args.label, "after_open");
    let mut model = Model::replay(steps0);
    if model.live != args.n {
        not_a_result(&format!("probe --n {} but the model has {} live", args.n, model.live));
    }
    // C-R parks the log tail's Commits to branches that are not resident; the first slot count applies
    // them all (a12-durable-open PREREG A7's SETTLE). Charged here: outside the open, outside every op.
    let (c0, sh0) = (db.branch_catalog_counters(), db.branch_cat_shape());
    let t = Instant::now();
    db.branch_stats().unwrap_or_else(|e| not_a_result(&format!("settle: {e}")));
    let settle_us = t.elapsed().as_secs_f64() * 1e6;
    let (c1, sh1) = (db.branch_catalog_counters(), db.branch_cat_shape());
    println!(
        "SETTLE\tn={}\tlabel={}\tsettle_us={settle_us:.1}\tbranch_loads={}\tcat_queries={}\tcat_rows_read={}\t\
         trunk_probes={}\ttrunk_rows={}\tresident_states={}",
        args.n,
        args.label,
        c1.0 - c0.0,
        c1.2 - c0.2,
        c1.3 - c0.3,
        sh1.trunk_probes - sh0.trunk_probes,
        sh1.trunk_rows - sh0.trunk_rows,
        sh1.resident_states
    );
    resident(args.n, &args.label, "after_settle");
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
        shape_line(&db.branch_cat_shape()).replace(' ', "\t")
    );
    let _ = std::io::stdout().flush();

    // Steady state: the schedule continues.
    let k_s = (n / 10).clamp(1, 2500);
    let mut ops = Ops {
        new_pr: Series::default(),
        merge: Series::default(),
        pr_update: Series::default(),
        reap: Series::default(),
        pr_update_cold: Series::default(),
        pr_update_warm: Series::default(),
        touched: Touched::new(steps0),
        census: census_on.then(|| Census::open(&args.db)),
    };
    for _ in 0..k_s {
        step(&db, &trunk, &mut model, Some(&mut ops));
    }
    check_invariants(&db, &model, "after steady ops");
    let Ops {
        mut new_pr,
        mut merge,
        mut pr_update,
        mut reap,
        mut pr_update_cold,
        mut pr_update_warm,
        mut touched,
        census,
    } = ops;
    let census = census.as_ref();
    new_pr.print("new_pr", n, label);
    merge.print("merge", n, label);
    pr_update.print("pr_update", n, label);
    reap.print("reap", n, label);
    pr_update_cold.print("pr_update.cold", n, label);
    pr_update_warm.print("pr_update.warm", n, label);
    resident(n, label, "after_steady");

    let live = model.live_ids();
    let timed = |series: &mut Series, f: &mut dyn FnMut() -> Vec<(String, i64)>| {
        let a = snap(&db);
        let t = Instant::now();
        let extra = f();
        let us = t.elapsed().as_secs_f64() * 1e6;
        let b = snap(&db);
        series.push(us, delta(&a, &b, None));
        series.extra.push(extra);
    };

    // Reads of old branches, planned before they are timed.
    let mut s: [Series; 3] = Default::default();
    for i in 0..200u64 {
        let id = live[(mix(i ^ 0x51) % live.len() as u64) as usize];
        let plan = plan_read(&model, id, mix(i ^ 0x52));
        timed_read(&db, census, &mut touched, "read_old", i as usize + 1, &plan, &mut s);
    }
    let [mut pooled, mut cold, mut warm] = s;
    pooled.print("read_old", n, label);
    cold.print("read_old.cold", n, label);
    warm.print("read_old.warm", n, label);
    let mut s: [Series; 3] = Default::default();
    for (i, &id) in live.iter().take(50).enumerate() {
        let plan = plan_read(&model, id, mix(id ^ 0x53));
        timed_read(&db, census, &mut touched, "read_oldest", i + 1, &plan, &mut s);
    }
    let [mut pooled, mut cold, mut warm] = s;
    pooled.print("read_oldest", n, label);
    cold.print("read_oldest.cold", n, label);
    warm.print("read_oldest.warm", n, label);
    resident(n, label, "after_reads");

    // PAIRS (r11-githost-attr PREREG section 3, as amended by A1): 2 x PAIRS distinct cold
    // branches from the update window as it stood at the open (ids steps0 + 1 - W0 ..= steps0, W0 =
    // ceil(0.0237 (steps0 + 1))): the pre-open part of every window the steady phase's pr_updates
    // drew from. Order: by mix(id ^ 0x71), a fixed permutation. The first PAIRS are updated twice in
    // a row, the rest read twice in a row: the second op of a pair is warm by construction, on the
    // same branch through the same path. They run BEFORE `list`, whose first call (F-W1) reads the
    // catalog. Known asymmetry (A1, d3): the warm update pops the slot the cold one just freed (LIFO
    // free list), so its write lands in place; the judge's R5 compares the read side only.
    let w0 = ((OPEN_WINDOW * (steps0 + 1) as f64).ceil() as u64).max(1);
    let mut chosen: Vec<u64> = ((steps0 + 1).saturating_sub(w0)..=steps0)
        .filter(|&id| id >= 1 && !model.dead[id as usize] && touched.is_cold(id))
        .collect();
    chosen.sort_by_key(|&id| (mix(id ^ 0x71), id));
    if chosen.len() < 2 * PAIRS {
        not_a_result(&format!(
            "pairs: only {} untouched branches in the open-time update window of {w0}",
            chosen.len()
        ));
    }
    chosen.truncate(2 * PAIRS);
    let (mut uc, mut uw) = (Series::default(), Series::default());
    for (seq, &id) in chosen[..PAIRS].iter().enumerate() {
        for warm in [false, true] {
            // The model follows each pair update, so the checks after it read the value written.
            model.gen[id as usize] += 1;
            let gen = model.gen[id as usize];
            let (us, c) = measure(&db, census, &mut || do_update(&db, id, gen));
            let (series, op) = if warm {
                (&mut uw, "pair_update.warm")
            } else {
                (&mut uc, "pair_update.cold")
            };
            series.push(us, c);
            oprec(op, seq + 1, id, model.steps - id, !warm, us, &c);
            touched.touch(id);
        }
    }
    uc.print("pair_update.cold", n, label);
    uw.print("pair_update.warm", n, label);
    let (mut rc, mut rw) = (Series::default(), Series::default());
    for (seq, &id) in chosen[PAIRS..].iter().enumerate() {
        let plan = plan_read(&model, id, mix(id ^ 0x73));
        for warm in [false, true] {
            let (us, c) = measure(&db, census, &mut || exec_read(&db, &plan));
            let (series, op) = if warm {
                (&mut rw, "pair_read.warm")
            } else {
                (&mut rc, "pair_read.cold")
            };
            series.push(us, c);
            series.extra.push(vec![("rows_read".to_string(), plan.reads.len() as i64)]);
            oprec(op, seq + 1, id, 0, !warm, us, &c);
            touched.touch(id);
        }
    }
    rc.print("pair_read.cold", n, label);
    rw.print("pair_read.warm", n, label);
    resident(n, label, "after_pairs");

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

    resident(n, label, "after_list");

    // (No DIFF in the catalog arm: its tree has no diff instrument; diff is measured on the port.)

    // Compaction: the whole-state checkpoint, on demand.
    let mut s = Series::default();
    for _ in 0..2 {
        timed(&mut s, &mut || {
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("compact: {e}")));
            let sh = db.branch_cat_shape();
            vec![("log_len_after".to_string(), sh.log_len as i64)]
        });
    }
    s.print("compact", n, label);
    check_invariants(&db, &model, "before close");
    let sh = db.branch_cat_shape();
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
    // Reopened: a compaction may have replaced the log and snapshot files under the old handles.
    if census_on {
        println!("{}", Census::open(&args.db).line(n, label, "before_close"));
    }
    // PREREG A3.10: which source key this process ran, from the engine's own count (not the
    // environment): keys_v2 = keys when R11_SCHEMA_KEY=2 took effect, 0 under key v1.
    let keys = db.branch_io_counters();
    println!(
        "SCHEMAKEY\tn={n}\tlabel={label}\tkeys={}\tkeys_v2={}\tadoptions={}\treparses={}",
        keys.schema_keys, keys.schema_keys_v2, keys.schema_adoptions, keys.schema_reparses
    );
    // The pair ops rewrote branches outside the schedule: no later process may replay against them.
    write_tainted_steps(&args.db, model.steps);
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
    // F-W3's knob (githost-shape PREREG G5.3), applied by `open_db` right after each open; recorded
    // with every run.
    println!(
        "# R11_RESIDENT_CAP={}",
        std::env::var("R11_RESIDENT_CAP").unwrap_or_else(|_| "unset".to_string())
    );
    // r11-githost-attr: the mode and the two page-cache instruments, recorded with every run.
    let set = |k: &str| if std::env::var_os(k).is_some() { "set" } else { "unset" };
    println!(
        "# r11-githost-attr: durability={} R11_UBC_PROBE={} R11_CENSUS_OPS={} R11_SCHEMA_SHARE={} \
         R11_PREWARM={} R11_PREWARM_SLOTS={} R11_SCHEMA_KEY={}",
        match (args.cmd.as_str(), args.eager) {
            ("ubc-selftest", _) => "n/a",
            (_, true) => "eager",
            (_, false) => "catalog",
        },
        set("R11_UBC_PROBE"),
        set("R11_CENSUS_OPS"),
        set("R11_SCHEMA_SHARE"),
        set("R11_PREWARM"),
        std::env::var("R11_PREWARM_SLOTS").unwrap_or_else(|_| "unset".to_string()),
        std::env::var("R11_SCHEMA_KEY").unwrap_or_else(|_| "unset".to_string())
    );
    match args.cmd.as_str() {
        "grow" => grow(&args),
        "probe" => probe(&args),
        "ubc-selftest" => ubc_selftest(&args.file),
        other => die(&format!("unknown command {other}")),
    }
}
