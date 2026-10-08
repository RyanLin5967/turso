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
//! `grow` is the kill -9 VICTIM: it opens the database `Durable { sync: turso_core::branch::SyncClass::Off }` (the files are the
//! same bytes as with sync; only fsyncs differ), grows it to N live branches — each forked from the
//! trunk, each updating one row (one leaf page), each detached so it outlives the process — prints
//! `READY n=N pid=P` and blocks on stdin until the driver SIGKILLs it. It prints counters only.
//!
//! `open` opens `Durable { sync: turso_core::branch::SyncClass::Fsync }` in this fresh process and times the open, prints the
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
//! r11-restart-r2 (PREREG A14; UNBUILT when written):
//!
//! * `ckpt2` also prints the catalog checkpoint counters and the WAL backfill's reads.
//! * `ckpt3 --writes M` (F-FZ's stall arm): a second thread commits over the upper half of the
//!   branches, timing every commit, while this thread's M commits over the lower half cross the
//!   store's own 1 MiB trigger, so the store checkpoints as it would (`R11_CKPT=fuzzy` for the AFTER
//!   arm; sharp is the default, the BEFORE arm). One line: both threads' latencies and the counters.
//! * `fifo --writes K` (K4): release the K oldest trunk children one at a time, each read by a
//!   fresh `Database::branch`, with the catalog rows each reap read and the versions it freed.
//! * `grow --lease-ms L` gives every fork a lease of L ms. `leasedown` is a kill -9 VICTIM: open,
//!   advance the lease clock past every deadline, make the clock durable with one trunk commit (its
//!   barrier stamps it; nothing expires), READY. `leaseopen` times the open that follows (F-EXP: one
//!   bounded pass; `R11_EXPIRE=unbounded` for the BEFORE arm), then `expire_branches` for the rest.
//! * `catonly --n N --writes D --phase build|measure` (A13 amended): the catalog-only fixture.
//! * `capfork --n D --forks K [--parents P] [--fix]` (PREREG A27, the F-FZ capture residual): P
//!   parents forked from the trunk each commit D one-page rows; a sharp checkpoint; K forks of each
//!   parent; one fuzzy checkpoint. Prints the entries its capture materialised under the store mutex.
//!   `--fix` is the gated arm and must agree with `R11_CAPTURE_GATE=on`, which the store reads.
//!
//! r12-catload (frontier/round12/r12-catload/PREREG.md), observing only:
//!
//! * `open` prints the catalog's OS page-cache residency before it opens (mincore), a PREWARM line
//!   (`R12_PREWARM`, see the store's catalog), and on SETTLE and on a new ATTACH line the PAGE_IO
//!   page reads, device bytes read, instructions, cycles, CPU and runnable time
//!   (proc_pid_rusage RUSAGE_INFO_V4, this thread's CPU clock) across the settle and each attach.
//! * `--window W` (open's probes, churn's commits): draw branch ids from the W consecutive ids
//!   centred in 1..=N instead of from all N (a confined working set); 0, the default, is all N.
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
    /// `open --probes`: the live counts at which each growth process of the state's chain began (r12-catload A7.20).
    grow_starts: Vec<u64>,
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
    /// `grow`/`leasedown`/`leaseopen --lease-ms L`: every fork's lease (0: none).
    lease_ms: u64,
    /// `catonly --phase build|measure`.
    phase: String,
    /// `open --probes` / `churn --window W`: draw ids from W consecutive ids (0: all N; r12-catload).
    window: usize,
    /// `capfork --forks K --parents P [--fix]`.
    forks: usize,
    parents: usize,
    fix: bool,
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_restart grow|open|compact|ckpt ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        probes: 0,
        grow_starts: Vec::new(),
        writes: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        label: String::new(),
        expect_in_use: None,
        trunk_every: 0,
        child_every: 0,
        catalog: false,
        lease_ms: 0,
        phase: String::new(),
        window: 0,
        forks: 1,
        parents: 1,
        fix: false,
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--probes" => args.probes = val().parse().unwrap_or_else(|_| die("bad --probes")),
            "--grow-starts" => {
                args.grow_starts = val()
                    .split(',')
                    .map(|v| v.parse().unwrap_or_else(|_| die("bad --grow-starts")))
                    .collect()
            }
            "--writes" => args.writes = val().parse().unwrap_or_else(|_| die("bad --writes")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--label" => args.label = val(),
            "--expect-in-use" => {
                args.expect_in_use = Some(val().parse().unwrap_or_else(|_| die("bad --expect-in-use")))
            }
            "--child-every" => args.child_every = val().parse().unwrap_or_else(|_| die("bad --child-every")),
            "--trunk-every" => args.trunk_every = val().parse().unwrap_or_else(|_| die("bad --trunk-every")),
            "--lease-ms" => args.lease_ms = val().parse().unwrap_or_else(|_| die("bad --lease-ms")),
            "--phase" => args.phase = val(),
            "--window" => args.window = val().parse().unwrap_or_else(|_| die("bad --window")),
            "--forks" => args.forks = val().parse().unwrap_or_else(|_| die("bad --forks")),
            "--parents" => args.parents = val().parse().unwrap_or_else(|_| die("bad --parents")),
            "--fix" => args.fix = true,
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
    if args.window > args.n {
        die("--window exceeds --n");
    }
    args
}

impl Args {
    /// A branch id to touch: uniform over 1..=N, or over the `--window` ids centred in it.
    fn pick(&self, rng: &mut Rng) -> u64 {
        if self.window == 0 {
            return 1 + rng.below(self.n) as u64;
        }
        let lo = 1 + (self.n - self.window) / 2;
        (lo + rng.below(self.window)) as u64
    }
}

/// This process's counters (proc_pid_rusage RUSAGE_INFO_V4) and this thread's CPU clock
/// (r12-catload, observing only). Fire-checked by frontier/round12/r12-catload/rusage_fire.py: the
/// device bytes read count a cold clone's bytes exactly and a warm re-read as 0; instructions and
/// cycles scale 10.0x with 10x the work. Times in ns (the rusage times are Mach ticks).
#[derive(Clone, Copy, Default)]
struct Ru {
    disk_read: u64,
    pageins: u64,
    instr: u64,
    cycles: u64,
    user_ns: u64,
    sys_ns: u64,
    runnable_ns: u64,
    thread_ns: u64,
}

impl Ru {
    fn now() -> Ru {
        static TIMEBASE: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
        // libc marks its binding deprecated (in favour of the mach2 crate); declared here instead.
        #[repr(C)]
        struct Timebase {
            numer: u32,
            denom: u32,
        }
        extern "C" {
            fn mach_timebase_info(info: *mut Timebase) -> i32;
        }
        let &(numer, denom) = TIMEBASE.get_or_init(|| {
            let mut tb = Timebase { numer: 0, denom: 0 };
            // SAFETY: a valid out-pointer for the call.
            let rc = unsafe { mach_timebase_info(&mut tb) };
            if rc != 0 || tb.denom == 0 {
                not_a_result("mach_timebase_info failed");
            }
            (tb.numer as u64, tb.denom as u64)
        });
        // SAFETY: a zeroed plain-integer struct, filled by the call.
        let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is a rusage_info_v4, which RUSAGE_INFO_V4 writes.
        let rc = unsafe {
            libc::proc_pid_rusage(
                std::process::id() as i32,
                libc::RUSAGE_INFO_V4,
                &mut info as *mut libc::rusage_info_v4 as *mut libc::rusage_info_t,
            )
        };
        if rc != 0 {
            not_a_result("proc_pid_rusage failed");
        }
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: a valid out-pointer for the call.
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
            not_a_result("clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
        }
        let ns = |ticks: u64| (ticks as u128 * numer as u128 / denom as u128) as u64;
        Ru {
            disk_read: info.ri_diskio_bytesread,
            pageins: info.ri_pageins,
            instr: info.ri_instructions,
            cycles: info.ri_cycles,
            user_ns: ns(info.ri_user_time),
            sys_ns: ns(info.ri_system_time),
            runnable_ns: ns(info.ri_runnable_time),
            thread_ns: ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64,
        }
    }

    fn since(&self, a: &Ru) -> Ru {
        Ru {
            disk_read: self.disk_read - a.disk_read,
            pageins: self.pageins - a.pageins,
            instr: self.instr - a.instr,
            cycles: self.cycles - a.cycles,
            user_ns: self.user_ns - a.user_ns,
            sys_ns: self.sys_ns - a.sys_ns,
            runnable_ns: self.runnable_ns - a.runnable_ns,
            thread_ns: self.thread_ns - a.thread_ns,
        }
    }

    fn fields(&self, p: &str) -> String {
        format!(
            "{p}disk_read={}\t{p}pageins={}\t{p}instr={}\t{p}cycles={}\t{p}user_us={:.1}\t{p}sys_us={:.1}\t\
             {p}runnable_us={:.1}\t{p}thread_cpu_us={:.1}",
            self.disk_read,
            self.pageins,
            self.instr,
            self.cycles,
            self.user_ns as f64 / 1e3,
            self.sys_ns as f64 / 1e3,
            self.runnable_ns as f64 / 1e3,
            self.thread_ns as f64 / 1e3
        )
    }
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
/// r12-catload A7.20: the trunk value branch `id` must read at `other` = far_row(row_for(id)), computed from the growth
/// chain's own write schedule, never read from the engine. The grower forks ids in order (id = live + 1) and, with
/// --trunk-every 1, writes far_row(row_for(X)) = trunk_write_value(X - start(X)) right after forking X, where start(X)
/// is the live count at which X's growth process began (its write counter starts at 0). row_for is a bijection mod
/// TRUNK_ROWS, so the writes landing on `other` come from the ids congruent to `id`; the latest forked before `id` is
/// id - TRUNK_ROWS. A child (--child-every 2) sees its parent id - 1's view, forked after that write as well. A write
/// made after `id`'s fork, its own included, must never be visible.
fn expected_trunk(id: u64, other: i64, starts: &[u64]) -> String {
    if id <= TRUNK_ROWS as u64 {
        return trunk_value(other);
    }
    let x = id - TRUNK_ROWS as u64;
    let start = starts
        .iter()
        .copied()
        .filter(|&s| s < x)
        .max()
        .unwrap_or_else(|| not_a_result(&format!("--grow-starts has no start below id {x}")));
    trunk_write_value(x - start)
}

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
    open_db_leased(path, sync, catalog, 0)
}

/// `open_db` with every fork given a lease of `lease_ms` (0: none).
fn open_db_leased(path: &Path, sync: bool, catalog: bool, lease_ms: u64) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let durability = if catalog {
        BranchDurability::Catalog { sync: if sync { turso_core::branch::SyncClass::Fsync } else { turso_core::branch::SyncClass::Off } }
    } else {
        BranchDurability::Durable { sync: if sync { turso_core::branch::SyncClass::Fsync } else { turso_core::branch::SyncClass::Off } }
    };
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new()
            .with_branch_durability(durability)
            .with_branch_lease((lease_ms > 0).then(|| Duration::from_millis(lease_ms))),
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
    let db = open_db_leased(&args.db, false, args.catalog, args.lease_ms);
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
    // r12-catload: how much of the catalog and the arena the OS page cache holds as this open starts.
    resident_line(args, &files, "RESIDENT");
    let rss0 = rss_bytes();
    let io0 = turso_core::branch::page_io();
    let oru0 = Ru::now();
    let t = Instant::now();
    let db = open_db(&args.db, true, args.catalog);
    let t_db = t.elapsed();
    let oru = Ru::now().since(&oru0);
    let io1 = turso_core::branch::page_io();
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let t_connect = t.elapsed();
    let rss1 = rss_bytes();
    let s = db.branch_open_stats();
    let reads = db.branch_read_counters();
    // branch_stats applies the parked Commits (C-R) before counting slots: timed apart from the open.
    let (loads0, _, q0, rows0) = db.branch_catalog_counters();
    trace_line(args, "open");
    // r12-catload: and as the settle starts (a `prefetch` prewarm may still be reading).
    resident_line(args, &files, "RESIDENT2");
    let (sio0, sru0) = (turso_core::branch::page_io(), Ru::now());
    let t = Instant::now();
    let st = db.branch_stats().unwrap();
    let t_settle = t.elapsed();
    let (sio1, sru1) = (turso_core::branch::page_io(), Ru::now());
    let (loads1, _, q1, rows1) = db.branch_catalog_counters();
    println!(
        "SETTLE\tn={}\tlabel={}\tsettle_us={:.1}\tsettle_branch_loads={}\tsettle_cat_rows={}\t\
         settle_cat_queries={}\tsettle_db_page_reads={}\tsettle_wal_frame_reads={}\t{}",
        args.n,
        args.label,
        t_settle.as_secs_f64() * 1e6,
        loads1 - loads0,
        rows1 - rows0,
        q1 - q0,
        sio1[0] - sio0[0],
        sio1[2] - sio0[2],
        sru1.since(&sru0).fields("settle_")
    );
    trace_line(args, "settle");
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
    println!("OPENRU\tn={}\tlabel={}\t{}", args.n, args.label, oru.fields("open_"));
    prewarm_line(args, db.branch_prewarm());
    if args.probes > 0 {
        if args.grow_starts.is_empty() {
            not_a_result("--probes needs --grow-starts (the state's growth starts; r12-catload A7.20)");
        }
        // Fire-check ONLY (A7.20): R12_PROBE_MUTANT=livetrunk reads the trunk row through the LIVE trunk, so a branch
        // sees trunk writes made after its fork; the probe must then fail. Registered runs never carry it.
        let live_trunk = std::env::var("R12_PROBE_MUTANT").ok().as_deref() == Some("livetrunk");
        let tconn = if live_trunk { Some(db.connect().unwrap()) } else { None };
        println!(
            "PROBECFG\tn={}\tlabel={}\tgrow_starts={:?}\tmutant={}",
            args.n,
            args.label,
            args.grow_starts,
            if live_trunk { "livetrunk" } else { "none" }
        );
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
        let k = args.probes.min(if args.window > 0 { args.window } else { args.n });
        // r12-catload: per attach, (db page reads, counters) across it.
        let mut attach_x: Vec<(u64, Ru)> = vec![];
        while seen.len() < k {
            let id = args.pick(&mut rng);
            if !seen.insert(id) {
                continue;
            }
            let row = row_for(id);
            let other = far_row(row);
            let ka = cc(&db);
            let (aio0, aru0) = (turso_core::branch::page_io(), Ru::now());
            let t_attach = Instant::now();
            let branch = db.branch(BranchId(id)).unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
            attach_t.push(t_attach.elapsed().as_secs_f64() * 1e6);
            let (aio1, aru1) = (turso_core::branch::page_io(), Ru::now());
            attach_x.push((aio1[0] - aio0[0], aru1.since(&aru0)));
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
            let v_trunk = match &tconn {
                Some(t) => read_v(t, other),
                None => read_v(&conn, other),
            };
            trunk_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c3, k3) = (db.branch_read_counters(), cc(&db));
            let t = Instant::now();
            let v_own2 = read_v(&conn, row);
            own2_t.push(t.elapsed().as_secs_f64() * 1e6);
            let (c4, k4) = (db.branch_read_counters(), cc(&db));
            if v_own != branch_value(row) || v_own2 != branch_value(row) {
                not_a_result(&format!("branch {id} read its own row {row} as {v_own:.12}"));
            }
            if v_trunk != expected_trunk(id, other, &args.grow_starts) {
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
        attach_line(args, &attach_t, &attach_x);
        trace_line(args, "probes");
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

/// r12-catload: the OS page-cache residency (mincore, in VM pages) of the catalog, its WAL and the
/// arena.
fn resident_line(args: &Args, files: &Files, tag: &str) {
    let (cr, cp) = resident_pages(&files.cat);
    let (wr, wp) = resident_pages(&files.cat_wal);
    let (ar, ap) = resident_pages(&files.arena);
    println!(
        "{tag}\tn={}\tlabel={}\tcat_resident={cr}\tcat_vm_pages={cp}\tcat_wal_resident={wr}\tcat_wal_vm_pages={wp}\t\
         arena_resident={ar}\tarena_vm_pages={ap}",
        args.n, args.label
    );
}

/// r12-catload: what the open's prewarm did (`Database::branch_prewarm`, `CatalogProbe::prewarm`).
fn prewarm_line(args: &Args, p: (&str, u64, u64, u64, u64, u64, u64, u64)) {
    let (mode, files, bytes, advised, pages, interior, cache_pages, ns) = p;
    println!(
        "PREWARM\tn={}\tlabel={}\tmode={mode}\tfiles={files}\tbytes={bytes}\tadvised={advised}\tpages={pages}\t\
         interior={interior}\tcache_pages={cache_pages}\tprewarm_us={:.1}",
        args.n,
        args.label,
        ns as f64 / 1e3
    );
}

/// r12-catload `catload --db CATALOG --n N --writes K [--window W] [--seed S]`: the catalog-only load
/// arm. Open a bare catalog (`CatalogProbe`: meta row, then the `R12_PREWARM` prewarm), then load K
/// distinct random branches one at a time, each timed and counted alone (PAGE_IO page reads, device
/// bytes, instructions, cycles, thread CPU), as a store's first touch loads one. `--db` is the catalog
/// file itself (a `catonly` fixture's, or a store's `-branch-cat`).
fn catload(args: &Args) {
    let path = args.db.clone();
    let (r, p) = resident_pages(&path);
    println!("RESIDENT\tn={}\tlabel={}\tcat_resident={r}\tcat_vm_pages={p}", args.n, args.label);
    let (io0, ru0, t) = (turso_core::branch::page_io(), Ru::now(), Instant::now());
    let mut probe = turso_core::branch::CatalogProbe::open(&path)
        .unwrap_or_else(|e| not_a_result(&format!("catload open: {e}")));
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let (io1, oru) = (turso_core::branch::page_io(), Ru::now().since(&ru0));
    println!(
        "CATOPEN\tn={}\tlabel={}\topen_us={open_us:.1}\topen_db_page_reads={}\t{}",
        args.n,
        args.label,
        io1[0] - io0[0],
        oru.fields("open_")
    );
    prewarm_line(args, probe.prewarm());
    trace_line(args, "open");
    let mut rng = Rng(args.seed);
    let mut seen = std::collections::HashSet::new();
    let k = args.writes.min(if args.window > 0 { args.window } else { args.n });
    let (mut wall, mut x) = (vec![], vec![]);
    while seen.len() < k {
        let id = args.pick(&mut rng);
        if !seen.insert(id) {
            continue;
        }
        let (a0, r0, t) = (turso_core::branch::page_io(), Ru::now(), Instant::now());
        let found = probe.load(id).unwrap_or_else(|e| not_a_result(&format!("catload load {id}: {e}")));
        wall.push(t.elapsed().as_secs_f64() * 1e6);
        let (a1, r1) = (turso_core::branch::page_io(), Ru::now());
        if !found {
            not_a_result(&format!("catload: branch {id} is not in the catalog"));
        }
        x.push((a1[0] - a0[0], r1.since(&r0)));
    }
    let (q, rows) = probe.counters();
    // The per-load summary, in the ATTACH line's shape.
    attach_line(args, &wall, &x);
    println!("CATLOAD\tn={}\tlabel={}\tk={k}\tqueries={q}\trows={rows}", args.n, args.label);
    trace_line(args, "loads");
}

/// r12-catload: with `R12_TRACE_READS` set, the database page reads since the last TRACE line: all
/// of them in order as `page:bytes`, their count and the distinct (page, bytes) pairs among them.
fn trace_line(args: &Args, phase: &str) {
    if std::env::var_os("R12_TRACE_READS").is_none() {
        return;
    }
    let t = turso_core::branch::take_page_read_trace();
    let distinct: std::collections::HashSet<(u32, u32)> = t.iter().copied().collect();
    let list: Vec<String> = t.iter().map(|(p, b)| format!("{p}:{b}")).collect();
    println!(
        "TRACE\tn={}\tlabel={}\tphase={phase}\treads={}\tdistinct={}\tlist={}",
        args.n,
        args.label,
        t.len(),
        distinct.len(),
        list.join(",")
    );
}

/// r12-catload: the attaches' counters, per attach (means over the K attaches, and the medians of
/// wall time, thread CPU and wall minus thread CPU).
fn attach_line(args: &Args, wall_us: &[f64], x: &[(u64, Ru)]) {
    if x.is_empty() {
        return;
    }
    let k = x.len() as f64;
    let mean = |v: Vec<f64>| v.iter().sum::<f64>() / k;
    let per = |f: fn(&(u64, Ru)) -> f64| mean(x.iter().map(f).collect());
    let median = |mut v: Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        pct(&v, 50.0)
    };
    let cpu: Vec<f64> = x.iter().map(|(_, r)| r.thread_ns as f64 / 1e3).collect();
    let off: Vec<f64> = wall_us.iter().zip(&cpu).map(|(w, c)| w - c).collect();
    println!(
        "ATTACH\tn={}\tlabel={}\twindow={}\tk={}\tp50_us={:.2}\tmean_us={:.2}\tthread_cpu_p50_us={:.2}\t\
         offcpu_p50_us={:.2}\tdb_page_reads_per={:.3}\tdisk_read_per={:.1}\tpageins_per={:.3}\tinstr_per={:.0}\t\
         cycles_per={:.0}\tthread_cpu_us_per={:.2}\tsys_us_per={:.2}\trunnable_us_per={:.2}",
        args.n,
        args.label,
        args.window,
        x.len(),
        median(wall_us.to_vec()),
        mean(wall_us.to_vec()),
        median(cpu),
        median(off),
        per(|(p, _)| *p as f64),
        per(|(_, r)| r.disk_read as f64),
        per(|(_, r)| r.pageins as f64),
        per(|(_, r)| r.instr as f64),
        per(|(_, r)| r.cycles as f64),
        per(|(_, r)| r.thread_ns as f64 / 1e3),
        per(|(_, r)| r.sys_ns as f64 / 1e3),
        per(|(_, r)| r.runnable_ns as f64 / 1e3)
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
    let bf0 = turso_core::branch::backfill_io();
    let ck0 = db.branch_checkpoint_counters();
    let res0 = resident_pages(&files.cat);
    let t = Instant::now();
    db.branch_compact_now().unwrap();
    let t_ck = t.elapsed();
    let io1 = turso_core::branch::page_io();
    let bf1 = turso_core::branch::backfill_io();
    let ck1 = db.branch_checkpoint_counters();
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
         cat_wal_after={catw1}\tlog_after={log1}\tbackfill_cache_hits={}\tbackfill_wal_reads={}\t\
         ckpt_counters_delta={:?}\tcat_cache_pages={}\tsettled: {settled}",
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
        res1.1,
        bf1[0] - bf0[0],
        bf1[1] - bf0[1],
        counters_delta(&ck0, &ck1),
        std::env::var("R11_CAT_CACHE_PAGES").unwrap_or_else(|_| "default".to_string())
    );
    drop(trunk);
    drop(db);
}

/// `after - before` of `Database::branch_checkpoint_counters`, except the max field (index 3) and
/// the settle max (index 8), which are reported as `after`.
fn counters_delta(before: &[u64; 9], after: &[u64; 9]) -> [u64; 9] {
    let mut d = [0; 9];
    for i in 0..9 {
        d[i] = if i == 3 || i == 8 { after[i] } else { after[i] - before[i] };
    }
    d
}

/// One committed rewrite of branch `id`'s own row; the commit's wall time in us.
fn commit_one(db: &Arc<Database>, id: u64, g: u64) -> f64 {
    let t = Instant::now();
    let branch = db
        .branch(BranchId(id))
        .unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
    let conn = branch.connect().unwrap();
    conn.execute(format!(
        "UPDATE t SET v = '{}' WHERE id = {}",
        trunk_write_value(g),
        row_for(id)
    ))
    .unwrap();
    drop(conn);
    let _ = branch.into_id();
    t.elapsed().as_secs_f64() * 1e6
}

/// (c3) F-FZ's stall arm (PREREG A14 P-FZ2). See the module doc.
fn ckpt3(args: &Args) {
    let db = open_db(&args.db, true, args.catalog);
    let _trunk = db.connect().unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n || args.n < 4 {
        not_a_result(&format!("ckpt3: {st:?}, expected {} branches", args.n));
    }
    db.branch_compact_now().unwrap();
    let half = (args.n / 2) as u64;
    let ck0 = db.branch_checkpoint_counters();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bg = {
        let db = db.clone();
        let stop = stop.clone();
        let seed = args.seed ^ 0xA5A5_A5A5;
        let upper = args.n as u64 - half;
        std::thread::spawn(move || {
            let mut rng = Rng(seed);
            let mut lat = Vec::new();
            let mut g = 1u64 << 40;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                g += 1;
                lat.push(commit_one(&db, half + 1 + rng.below(upper as usize) as u64, g));
            }
            lat
        })
    };
    let mut rng = Rng(args.seed);
    let mut main_lat = Vec::with_capacity(args.writes);
    for g in 0..args.writes {
        main_lat.push(commit_one(&db, 1 + rng.below(half as usize) as u64, g as u64));
    }
    // A fuzzy checkpoint still in flight installs before the counters are read.
    db.branch_checkpoint_wait();
    stop.store(true, std::sync::atomic::Ordering::Release);
    let mut bg_lat = bg.join().unwrap_or_else(|_| not_a_result("ckpt3: the committer thread panicked"));
    let ck1 = db.branch_checkpoint_counters();
    let d = counters_delta(&ck0, &ck1);
    if d[0] == 0 {
        not_a_result(&format!("ckpt3: no checkpoint fired in {} + {} commits", args.writes, bg_lat.len()));
    }
    bg_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    main_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let stat = |v: &[f64]| {
        if v.is_empty() {
            (0.0, 0.0, 0.0, 0.0)
        } else {
            (pct(v, 50.0), pct(v, 99.0), pct(v, 99.9), *v.last().unwrap())
        }
    };
    let (b50, b99, b999, bmax) = stat(&bg_lat);
    let (m50, m99, m999, mmax) = stat(&main_lat);
    println!(
        "CKPT3\tn={}\twrites={}\tmode={}\tbg_commits={}\tbg_p50_us={b50:.1}\tbg_p99_us={b99:.1}\tbg_p999_us={b999:.1}\t\
         bg_max_us={bmax:.1}\tmain_p50_us={m50:.1}\tmain_p99_us={m99:.1}\tmain_p999_us={m999:.1}\tmain_max_us={mmax:.1}\t\
         ckpts={}\tflights={}\thold_ns={}\thold_max_ns={}\tflight_ns={}\tstmts_locked={}\tsettle_batches={}\t\
         settle_loads={}\tsettle_max_loads={}",
        args.n,
        args.writes,
        std::env::var("R11_CKPT").unwrap_or_else(|_| "sharp (default)".to_string()),
        bg_lat.len(),
        d[0],
        d[1],
        d[2],
        d[3],
        d[4],
        d[5],
        d[6],
        d[7],
        d[8]
    );
}

/// (K4) The K oldest trunk children released one at a time, oldest first (PREREG A14): what each
/// reap read from the catalog against what it freed.
fn fifo(args: &Args) {
    let db = open_db(&args.db, true, args.catalog);
    let _trunk = db.connect().unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("fifo: {st:?}, expected {} branches", args.n));
    }
    db.branch_compact_now().unwrap();
    let retained0 = db.branch_trunk_retained();
    let k = args.writes.min(args.n);
    let (mut sum_rows, mut sum_freed, mut worst) = (0u64, 0u64, 0.0f64);
    for id in 1..=k as u64 {
        let (_, _, q0, r0) = db.branch_catalog_counters();
        let w0 = db.branch_stats().unwrap().work;
        let branch = db
            .branch(BranchId(id))
            .unwrap_or_else(|e| not_a_result(&format!("fifo: attach {id}: {e}")));
        let reaped = branch
            .reap()
            .unwrap_or_else(|e| not_a_result(&format!("fifo: reap {id}: {e}")));
        let (_, _, q1, r1) = db.branch_catalog_counters();
        let w1 = db.branch_stats().unwrap().work;
        let rows = r1 - r0;
        // The branch's own page, plus the trunk versions only it held.
        let freed = reaped.freed_pages as u64;
        sum_rows += rows;
        sum_freed += freed;
        worst = worst.max(rows as f64 / freed.max(1) as f64);
        println!(
            "FIFO\tn={}\tid={id}\tfreed={freed}\tdeferred={}\tcat_rows_read={rows}\tcat_queries={}\t\
             gc_range_entries={}\tgc_examined={}",
            args.n,
            reaped.deferred,
            q1 - q0,
            w1.gc_range_entries - w0.gc_range_entries,
            w1.gc_examined - w0.gc_examined
        );
    }
    println!(
        "FIFOSUM\tn={}\treaps={k}\ttrunk_retained_before={retained0}\ttrunk_retained_after={}\t\
         cat_rows_read={sum_rows}\tfreed={sum_freed}\tworst_rows_per_freed={worst:.3}",
        args.n,
        db.branch_trunk_retained()
    );
}

/// Lease arm VICTIM (PREREG A14): every deadline passed and durable, nothing expired; READY.
fn leasedown(args: &Args) {
    if args.lease_ms == 0 {
        die("leasedown needs --lease-ms");
    }
    let db = open_db_leased(&args.db, false, args.catalog, args.lease_ms);
    let trunk = db.connect().unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("leasedown: {st:?}, expected {} branches (an expiry ran early?)", args.n));
    }
    db.branch_lease_clock_advance(Duration::from_millis(args.lease_ms + 1_000));
    // A trunk commit's durability barrier stamps the lease clock (a Clock record) and expires
    // nothing, so the log now says every deadline has passed.
    trunk
        .execute(format!("UPDATE t SET v = '{}' WHERE id = 1", trunk_write_value(1 << 50)))
        .unwrap();
    let st = db.branch_stats().unwrap();
    if st.live_branches != args.n {
        not_a_result(&format!("leasedown: {st:?} after the stamp, expected {}", args.n));
    }
    println!(
        "# leasedown victim pid={} n={} lease_ms={} lease_now_ms={}",
        std::process::id(),
        args.n,
        args.lease_ms,
        db.branch_lease_now().as_millis()
    );
    println!("READY n={} pid={}", args.n, std::process::id());
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// The open after `leasedown`'s crash (PREREG A14): what the open reaped, then `expire_branches`.
fn leaseopen(args: &Args) {
    let io0 = turso_core::branch::page_io();
    let t = Instant::now();
    let db = open_db_leased(&args.db, true, args.catalog, args.lease_ms);
    let t_open = t.elapsed();
    let io1 = turso_core::branch::page_io();
    let s = db.branch_open_stats();
    let live_after_open = db.branch_stats().unwrap().live_branches;
    let t = Instant::now();
    let rest = db
        .expire_branches()
        .unwrap_or_else(|e| not_a_result(&format!("leaseopen: expire_branches: {e}")));
    let t_rest = t.elapsed();
    let live_end = db.branch_stats().unwrap().live_branches;
    let reaped_at_open = args.n - live_after_open;
    if reaped_at_open + rest.reaped.len() != args.n || live_end != 0 {
        not_a_result(&format!(
            "leaseopen: {reaped_at_open} reaped at open + {} after != {} (live at the end {live_end})",
            rest.reaped.len(),
            args.n
        ));
    }
    println!(
        "LEASEOPEN\tn={}\texpire={}\topen_us={:.1}\treaped_at_open={reaped_at_open}\texpire_ns_in_open={}\t\
         open_db_page_reads={}\texpire_rest_us={:.1}\treaped_after={}\tfreed_after={}\t{}",
        args.n,
        std::env::var("R11_EXPIRE").unwrap_or_else(|_| "bounded".to_string()),
        t_open.as_secs_f64() * 1e6,
        s.expire_ns,
        io1[0] - io0[0],
        t_rest.as_secs_f64() * 1e6,
        rest.reaped.len(),
        rest.freed_pages,
        stats_line(&s)
    );
}

/// A13 amended: the catalog-only fixture (see `turso_core::branch::catalog_only_fixture`).
fn catonly(args: &Args) {
    let measure = match args.phase.as_str() {
        "build" => false,
        "measure" => true,
        other => die(&format!("catonly --phase build|measure, not {other:?}")),
    };
    let line = turso_core::branch::catalog_only_fixture(
        &args.db,
        args.n as u64,
        args.writes as u64,
        measure,
        args.seed,
    )
    .unwrap_or_else(|e| not_a_result(&format!("catonly: {e}")));
    println!("{line}");
}

/// `capfork` (PREREG A27): the entries a fuzzy checkpoint's capture copies under the store mutex
/// after K forks of each of P parents that own about D pages each, and whose own state a sharp
/// checkpoint has already written. Counters only; the hold time is printed, not claimed.
fn capfork(args: &Args) {
    let gate = std::env::var("R11_CAPTURE_GATE").is_ok_and(|v| v == "on");
    if args.fix != gate {
        die("capfork: --fix and R11_CAPTURE_GATE=on go together (the flag names the arm the store reads)");
    }
    let ok = |r: turso_core::Result<()>, what: &str| r.unwrap_or_else(|e| not_a_result(&format!("capfork {what}: {e}")));
    let db = open_db(&args.db, false, true);
    let trunk = db.connect().unwrap_or_else(|e| not_a_result(&format!("capfork connect: {e}")));
    ok(trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), "create");
    ok(trunk.execute("INSERT INTO t VALUES (1, 'trunk')"), "insert");
    let d = args.n;
    let mut parents: Vec<(BranchId, usize)> = Vec::new();
    for p in 0..args.parents {
        let b = trunk.fork_branch().unwrap_or_else(|e| not_a_result(&format!("capfork fork parent: {e}")));
        let c = b.connect().unwrap_or_else(|e| not_a_result(&format!("capfork connect parent: {e}")));
        ok(c.execute(format!("CREATE TABLE big{p}(id INTEGER PRIMARY KEY, x BLOB)")), "create big");
        ok(c.execute("BEGIN"), "begin");
        for i in 0..d {
            ok(c.execute(format!("INSERT INTO big{p} VALUES ({i}, zeroblob(3000))")), "insert big");
        }
        ok(c.execute("COMMIT"), "commit");
        drop(c);
        let owned = b.owned_slots().len();
        parents.push((b.into_id(), owned));
    }
    // Any fuzzy checkpoint the commits started must install before the sharp one (it refuses to
    // overlap a flight).
    db.branch_checkpoint_wait();
    db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("capfork compact: {e}")));
    let e0 = db.branch_checkpoint_capture_entries();
    for &(id, _) in &parents {
        let b = db.branch(id).unwrap_or_else(|e| not_a_result(&format!("capfork attach: {e}")));
        let c = b.connect().unwrap_or_else(|e| not_a_result(&format!("capfork connect: {e}")));
        for _ in 0..args.forks {
            let _ = c.fork_branch().unwrap_or_else(|e| not_a_result(&format!("capfork fork child: {e}"))).into_id();
        }
        drop(c);
        let _ = b.into_id();
    }
    let before = db.branch_checkpoint_counters();
    let mut calls = 0;
    while !db.branch_checkpoint_fuzzy_now().unwrap_or_else(|e| not_a_result(&format!("capfork fuzzy: {e}"))) {
        calls += 1;
        if calls >= 16 {
            not_a_result("capfork: no fuzzy checkpoint started after 16 calls");
        }
    }
    db.branch_checkpoint_wait();
    let after = db.branch_checkpoint_counters();
    let e1 = db.branch_checkpoint_capture_entries();
    if after[0] != before[0] + 1 {
        not_a_result(&format!("capfork: {} checkpoints installed, not 1", after[0] - before[0]));
    }
    let owned: Vec<usize> = parents.iter().map(|p| p.1).collect();
    println!(
        "CAPFORK\td={d}\tforks={}\tparents={}\towned={owned:?}\tgate={}\tentries_under_mutex={}\tstmts_locked={}\thold_max_ns={}\tsettle_calls={calls}",
        args.forks,
        args.parents,
        if gate { "on" } else { "off" },
        e1 - e0,
        after[5] - before[5],
        after[3]
    );
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
        let id = args.pick(&mut rng);
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
        "# churn victim pid={} n={} writes={} window={} distinct={} {}",
        std::process::id(),
        args.n,
        args.writes,
        args.window,
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
        "ckpt3" => ckpt3(&args),
        "fifo" => fifo(&args),
        "leasedown" => leasedown(&args),
        "leaseopen" => leaseopen(&args),
        "catonly" => catonly(&args),
        "capfork" => capfork(&args),
        "churn" => churn(&args),
        "catload" => catload(&args),
        other => die(&format!("unknown command {other}")),
    }
}
