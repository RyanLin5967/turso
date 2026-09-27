//! Open branch sessions: N branch connections held open at once. Specification:
//! artie-research frontier/round11/r11-sessions/PREREG.md (this is its implementation).
//!
//!   cargo run -p turso_core --release --example branch_sessions -- --mode <mode> [options]
//!
//! Modes (each session's first action after `connect`, and the state it is then left in):
//!
//!   open   nothing: connected, never used
//!   read   one point SELECT of the session's row, autocommit
//!   write  one UPDATE of the session's row, autocommit
//!   intx   BEGIN, then the point SELECT; the transaction is left open (idle in transaction)
//!
//! `--analyze` runs ANALYZE on the trunk before the first fork; `--tables K` adds K-1 small tables with
//! one index each (schema size). `--active K` runs K cycles of the active ops at every checkpoint, with
//! every held session still open. `--timing` adds Instant percentiles; without it the run prints only
//! integer counters and may run outside the fleet lock.
//!
//! The space instrument is a counting global allocator: live bytes and live allocations, exact integers.
//! A probe inside the engine's connect path (`turso_core::branch::set_session_probe`) marks each step
//! boundary, so a connection's bytes are split by the step that allocated them. Two known blind spots:
//! the buffer pool's first 768 page buffers come from an mmap'd arena the allocator cannot see, and a
//! page buffer freed into turso's thread-local buffer cache is not a deallocation (so a buffer can be
//! allocated in one step and handed to a page in a later one). Totals over an interval are exact once
//! the arena is exhausted.
//!
//! Every read is checked against a model the harness keeps itself (never against the engine); a
//! mismatch prints `NOT A RESULT` and exits 1.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::{set_session_probe, Branch, SessionFootprint};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

// ---------------------------------------------------------------------------------------------
// The instrument: a counting allocator.

struct Counting;

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static LIVE_ALLOCS: AtomicI64 = AtomicI64::new(0);

thread_local! {
    /// This thread's own net bytes (allocated minus freed by this thread). Const-initialised and
    /// drop-free, so touching it from the allocator allocates nothing.
    static TL_BYTES: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

fn tl_add(d: i64) {
    let _ = TL_BYTES.try_with(|c| c.set(c.get() + d));
}

fn tl_bytes() -> i64 {
    TL_BYTES.with(|c| c.get())
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
            LIVE_ALLOCS.fetch_add(1, Ordering::Relaxed);
            tl_add(layout.size() as i64);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
            LIVE_ALLOCS.fetch_add(1, Ordering::Relaxed);
            tl_add(layout.size() as i64);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        LIVE_ALLOCS.fetch_sub(1, Ordering::Relaxed);
        tl_add(-(layout.size() as i64));
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
            tl_add(new_size as i64 - layout.size() as i64);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[derive(Clone, Copy, Default, Debug)]
struct Mem {
    bytes: i64,
    allocs: i64,
}

fn mem() -> Mem {
    Mem {
        bytes: LIVE_BYTES.load(Ordering::Relaxed),
        allocs: LIVE_ALLOCS.load(Ordering::Relaxed),
    }
}

/// The engine's connect-path labels, in the order the engine calls them. The probe writes into a
/// fixed array so that recording a sample allocates nothing. A build may skip labels (a store that
/// builds branch pagers differently has other steps); `REQUIRED` are the ones every build has, and a
/// session's bytes between two seen labels are charged to the later one.
const LABELS: [&str; 12] = [
    "begin",
    "store_open",
    "init.header",
    "init.wal",
    "init.pager_new",
    "init_pager",
    "init_branch",
    "clear_trunk_page1",
    "bind_read_page1",
    "connection",
    "syms",
    "analyze_stats",
];
const REQUIRED: [&str; 5] = ["begin", "store_open", "connection", "syms", "analyze_stats"];
const NL: usize = LABELS.len();
static PROBE_BYTES: [AtomicI64; NL] = [const { AtomicI64::new(0) }; NL];
static PROBE_ALLOCS: [AtomicI64; NL] = [const { AtomicI64::new(0) }; NL];
static PROBE_SEEN: AtomicUsize = AtomicUsize::new(0);
static PROBE_UNKNOWN: AtomicUsize = AtomicUsize::new(0);
/// `PRAGMA wal_checkpoint` values that were not integers (recorded as -1).
static CKPT_NONINT: AtomicUsize = AtomicUsize::new(0);

fn probe(label: &'static str) {
    let m = mem();
    match LABELS.iter().position(|l| *l == label) {
        Some(i) => {
            PROBE_BYTES[i].store(m.bytes, Ordering::Relaxed);
            PROBE_ALLOCS[i].store(m.allocs, Ordering::Relaxed);
            PROBE_SEEN.fetch_or(1 << i, Ordering::Relaxed);
        }
        None => {
            PROBE_UNKNOWN.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Arguments.

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Open,
    Read,
    Write,
    Intx,
    /// A full scan of t (count and total length of v): every leaf, ~550 pages, held in the private
    /// cache afterwards (r11-walpin-conc's K × min(capacity, pages touched) term, amendment 11).
    Scan,
    /// A write rolled back: BEGIN, an UPDATE of the session's row, ROLLBACK (amendment 17).
    Rollback,
}

struct Args {
    mode: Mode,
    checkpoints: Vec<usize>,
    analyze: bool,
    tables: usize,
    active: usize,
    timing: bool,
    seed: u64,
    swap_limit_mib: u64,
    swap_growth_mib: u64,
    level_floor: u32,
    /// Session multiplexing: a session keeps only its Branch handle; each statement connects, runs,
    /// and drops its connection.
    mux: bool,
    /// Pre-image arm (amendment 9): R of the W=4 leaves each session reads are rewritten by the
    /// trunk after every session forked.
    preimage: Option<usize>,
    /// Ancestor-chain arm (amendment 13): sessions are forked from the tip of a chain of D branches;
    /// the chain's first branch wrote, and after the sessions forked rewrote, R of the rows each
    /// session reads (so the session reads that ancestor BRANCH's retained version).
    chain: Option<usize>,
    /// Concurrent trunk writer threads during the active arm (amendment 9).
    trunk_writers: usize,
    /// Interleaved pre-image arm (amendment 14): one trunk rewrite of a leaf every session reads
    /// between consecutive forks, so the sessions see many retained versions.
    interleave: bool,
    /// Cap-engaged scan arm (amendment 14): a table of this many rows, larger than the session
    /// cache, scanned once by every held session under `PRAGMA cache_size = cache_size`.
    capscan: Option<i64>,
    cache_size: Option<i64>,
    /// Pinning arm (amendment 17): sessions fork in groups of G; after each group the trunk
    /// rewrites R rows on R distinct leaves. Every group pins one version of each of those pages.
    pin: Option<(usize, usize)>,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_sessions: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut mode = None;
    let mut args = Args {
        mode: Mode::Open,
        checkpoints: vec![1_000, 10_000, 100_000, 1_000_000],
        analyze: false,
        tables: 1,
        active: 0,
        timing: false,
        seed: 0x9E37_79B9_7F4A_7C15,
        swap_limit_mib: 7936,
        swap_growth_mib: 512,
        level_floor: 20,
        mux: false,
        preimage: None,
        chain: None,
        trunk_writers: 0,
        interleave: false,
        capscan: None,
        cache_size: None,
        pin: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--mode" => {
                mode = Some(match val().as_str() {
                    "open" => Mode::Open,
                    "read" => Mode::Read,
                    "write" => Mode::Write,
                    "intx" => Mode::Intx,
                    "scan" => Mode::Scan,
                    "rollback" => Mode::Rollback,
                    other => die(&format!("unknown mode {other}")),
                })
            }
            "--checkpoints" => {
                args.checkpoints = val()
                    .split(',')
                    .map(|x| x.parse().unwrap_or_else(|_| die("bad --checkpoints")))
                    .collect()
            }
            "--analyze" => args.analyze = true,
            "--tables" => args.tables = val().parse().unwrap_or_else(|_| die("bad --tables")),
            "--active" => args.active = val().parse().unwrap_or_else(|_| die("bad --active")),
            "--timing" => args.timing = true,
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--swap-limit-mib" => {
                args.swap_limit_mib = val().parse().unwrap_or_else(|_| die("bad --swap-limit-mib"))
            }
            "--swap-growth-mib" => {
                args.swap_growth_mib =
                    val().parse().unwrap_or_else(|_| die("bad --swap-growth-mib"))
            }
            "--mux" => args.mux = true,
            "--chain" => {
                args.chain = Some(val().parse().unwrap_or_else(|_| die("bad --chain")))
            }
            "--preimage" => {
                args.preimage = Some(val().parse().unwrap_or_else(|_| die("bad --preimage")))
            }
            "--trunk-writers" => {
                args.trunk_writers = val().parse().unwrap_or_else(|_| die("bad --trunk-writers"))
            }
            "--interleave" => args.interleave = true,
            "--pin" => {
                let v = val();
                let (g, r) = v.split_once(',').unwrap_or_else(|| die("--pin needs G,R"));
                args.pin = Some((
                    g.parse().unwrap_or_else(|_| die("bad --pin G")),
                    r.parse().unwrap_or_else(|_| die("bad --pin R")),
                ));
            }
            "--capscan" => {
                args.capscan = Some(val().parse().unwrap_or_else(|_| die("bad --capscan")))
            }
            "--cache-size" => {
                args.cache_size = Some(val().parse().unwrap_or_else(|_| die("bad --cache-size")))
            }
            "--level-floor" => {
                args.level_floor = val().parse().unwrap_or_else(|_| die("bad --level-floor"))
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.mode = mode.unwrap_or_else(|| die("--mode is required"));
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.tables == 0 {
        die("--tables must be at least 1");
    }
    if args.tables > 1 && !args.analyze {
        die("--tables applies with --analyze");
    }
    args
}

// ---------------------------------------------------------------------------------------------
// The box: swap, memory level, fds, RSS.

fn swap_used_mib() -> u64 {
    let mut xsu: libc::xsw_usage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::xsw_usage>();
    let name = c"vm.swapusage";
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut xsu as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        not_a_result("sysctl vm.swapusage failed: the memory guard cannot read swap");
    }
    xsu.xsu_used / (1024 * 1024)
}

fn memory_level() -> u32 {
    let mut level: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    let name = c"kern.memorystatus_level";
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut level as *mut _ as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        not_a_result("sysctl kern.memorystatus_level failed: the memory guard cannot read the level");
    }
    level as u32
}

/// The fleet memory rule (LANE-BRIEF, 15:47Z): a guard that polls at least once a second and stops the
/// run when swap has grown more than the allowance since the run started, exceeds the absolute limit, or
/// kern.memorystatus_level reads below the floor. A thread polls every 200 ms; the growth loop reads the
/// flag before every session. The thread allocates nothing in its loop, so it cannot move the counters.
static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static STOP_SWAP: AtomicI64 = AtomicI64::new(-1);
static STOP_LEVEL: AtomicI64 = AtomicI64::new(-1);
static MIN_LEVEL: AtomicI64 = AtomicI64::new(i64::MAX);
static MAX_SWAP: AtomicI64 = AtomicI64::new(0);

fn start_watchdog(swap0: u64, growth: u64, limit: u64, floor: u32) {
    std::thread::spawn(move || loop {
        let (swap, level) = (swap_used_mib(), memory_level());
        MIN_LEVEL.fetch_min(level as i64, Ordering::Relaxed);
        MAX_SWAP.fetch_max(swap as i64, Ordering::Relaxed);
        if swap > swap0 + growth || swap > limit || level < floor {
            STOP_SWAP.store(swap as i64, Ordering::Relaxed);
            STOP_LEVEL.store(level as i64, Ordering::Relaxed);
            STOP.store(true, Ordering::Release);
        }
        std::thread::sleep(Duration::from_millis(200));
    });
}

fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd").map_or(0, |d| d.count())
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

// ---------------------------------------------------------------------------------------------
// Workload and model.

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

fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    let mut stmt = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {id}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {id}: {} rows", rows.len())),
    }
}

/// The trunk's write history, kept by the harness.
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    history: HashMap<i64, Vec<(u64, u64)>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push((g, g));
        g
    }
    fn value_at(&self, row: i64, writes_at_fork: u64) -> String {
        let Some(h) = self.history.get(&row) else {
            return trunk_value(row);
        };
        let n = h.partition_point(|&(seq, _)| seq < writes_at_fork);
        if n == 0 {
            trunk_value(row)
        } else {
            trunk_gen_value(h[n - 1].1)
        }
    }
}

struct Session {
    branch: Branch,
    /// `None` under --mux: the session holds no connection between statements.
    conn: Option<Arc<Connection>>,
    row: i64,
    wrote: bool,
    trunk_writes_at_fork: u64,
    /// The active arm has used this session, so its footprint is no longer its idle one.
    touched: bool,
}

impl Session {
    fn expect(&self, model: &TrunkModel) -> String {
        if self.wrote {
            branch_value(self.row)
        } else {
            model.value_at(self.row, self.trunk_writes_at_fork)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Per-interval accounting.

/// Sum, min and max of one integer per session.
#[derive(Clone, Copy)]
struct Acc {
    n: u64,
    sum: i64,
    min: i64,
    max: i64,
}

impl Default for Acc {
    fn default() -> Self {
        Acc {
            n: 0,
            sum: 0,
            min: i64::MAX,
            max: i64::MIN,
        }
    }
}

impl Acc {
    fn add(&mut self, v: i64) {
        self.n += 1;
        self.sum += v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }
    fn mean(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum as f64 / self.n as f64
        }
    }
}

/// The phases a session's growth is split into: fork, each connect-path step (one per label after
/// "begin"), the tail of connect after the last probe, and the mode's first action.
const PHASES: [&str; NL + 4] = [
    "fork",
    "connect.store_open",
    "connect.init.header",
    "connect.init.wal",
    "connect.init.pager_new",
    "connect.init_pager",
    "connect.init_branch",
    "connect.clear_trunk_page1",
    "connect.bind_read_page1",
    "connect.connection",
    "connect.syms",
    "connect.analyze_stats",
    "connect.return",
    "connect.total",
    "action",
    "mux.drop",
];

#[derive(Default)]
struct Interval {
    bytes: [Acc; PHASES.len()],
    allocs: [Acc; PHASES.len()],
    times: [Vec<Duration>; 3],
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn pct(ds: &[Duration]) -> (f64, f64, f64, f64) {
    let mut us: Vec<f64> = ds.iter().map(|d| d.as_secs_f64() * 1e6).collect();
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        percentile(&us, 50.0),
        percentile(&us, 90.0),
        percentile(&us, 99.0),
        us[us.len() - 1],
    )
}

// ---------------------------------------------------------------------------------------------

struct Bench {
    args: Args,
    db: Arc<Database>,
    wal_path: PathBuf,
    trunk: Arc<Connection>,
    model: TrunkModel,
    rng: Rng,
}

impl Bench {
    fn resolves(&self) -> u64 {
        self.db.branch_stats().work.resolve_calls
    }

    /// Store-lock acquisitions (F6's counters; this harness copy is built only on the F6 store).
    fn lock_takes(&self) -> u64 {
        self.db.branch_stats().work.lock_acquisitions
    }

    fn wal_bytes(&self) -> u64 {
        std::fs::metadata(&self.wal_path).map_or(0, |m| m.len())
    }

    fn trunk_commit(&mut self) {
        let row = spread_row(self.model.writes);
        let sql = format!(
            "UPDATE t SET v = '{}' WHERE id = {row}",
            trunk_gen_value(self.model.writes)
        );
        // With concurrent trunk writers (--trunk-writers) the WAL write lock can be held: retry Busy,
        // counted. The model records the write only once it has committed.
        retry_busy(|| self.trunk.execute(&sql));
        self.model.record(row);
    }

    fn checkpoint_passive(&self) -> (i64, i64, i64) {
        let rows = self
            .trunk
            .prepare("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let r = &rows[0];
        let int = |v: &Value| {
            v.as_int().unwrap_or_else(|| {
                CKPT_NONINT.fetch_add(1, Ordering::Relaxed);
                -1
            })
        };
        (int(&r[0]), int(&r[1]), int(&r[2]))
    }

    /// Open one session: fork, connect (probed), and the mode's first action.
    fn open_session(&mut self, i: usize, iv: &mut Interval) -> Session {
        let row = row_for(i);
        let at_fork = self.model.writes;
        let m0 = mem();
        let t0 = Instant::now();
        let branch = self.trunk.fork_branch().unwrap();
        let t1 = Instant::now();
        let m1 = mem();
        PROBE_SEEN.store(0, Ordering::Relaxed);
        set_session_probe(Some(probe));
        let t2 = Instant::now();
        let conn = branch.connect().unwrap();
        let t3 = Instant::now();
        set_session_probe(None);
        let m2 = mem();
        let seen = PROBE_SEEN.load(Ordering::Relaxed);
        let required = REQUIRED
            .iter()
            .map(|r| 1usize << LABELS.iter().position(|l| l == r).unwrap())
            .fold(0, |a, b| a | b);
        if seen & required != required {
            not_a_result(&format!("the connect path missed a required probe: mask {seen:#b}"));
        }
        let mut wrote = false;
        let t4 = Instant::now();
        match self.args.mode {
            Mode::Open => {}
            Mode::Read => {
                let got = read_v(&conn, row);
                if got != self.model.value_at(row, at_fork) {
                    not_a_result(&format!("session {i} read {got}"));
                }
            }
            Mode::Write => {
                conn.execute(format!(
                    "UPDATE t SET v = '{}' WHERE id = {row}",
                    branch_value(row)
                ))
                .unwrap();
                wrote = true;
            }
            Mode::Scan => {
                let rows = conn
                    .prepare("SELECT count(*), sum(length(v)) FROM t")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap();
                let want = (TRUNK_ROWS, TRUNK_ROWS * VALUE_LEN as i64);
                if (rows[0][0].as_int(), rows[0][1].as_int()) != (Some(want.0), Some(want.1)) {
                    not_a_result(&format!("session {i} scanned {:?}", rows[0]));
                }
            }
            Mode::Rollback => {
                conn.execute("BEGIN").unwrap();
                conn.execute(format!(
                    "UPDATE t SET v = '{}' WHERE id = {row}",
                    branch_value(row)
                ))
                .unwrap();
                conn.execute("ROLLBACK").unwrap();
                let got = read_v(&conn, row);
                if got != self.model.value_at(row, at_fork) {
                    not_a_result(&format!("session {i} read {got} after its rollback"));
                }
            }
            Mode::Intx => {
                conn.execute("BEGIN").unwrap();
                let got = read_v(&conn, row);
                if got != self.model.value_at(row, at_fork) {
                    not_a_result(&format!("session {i} read {got}"));
                }
            }
        }
        let t5 = Instant::now();
        let m3 = mem();
        let conn = if self.args.mux {
            drop(conn);
            let m4 = mem();
            iv.bytes[NL + 3].add(m4.bytes - m3.bytes);
            iv.allocs[NL + 3].add(m4.allocs - m3.allocs);
            None
        } else {
            Some(conn)
        };

        let pb = |k: usize| PROBE_BYTES[k].load(Ordering::Relaxed);
        let pa = |k: usize| PROBE_ALLOCS[k].load(Ordering::Relaxed);
        iv.bytes[0].add(m1.bytes - m0.bytes);
        iv.allocs[0].add(m1.allocs - m0.allocs);
        // The probe's own "begin" is the first thing connect_branch does; anything between m1 and it
        // (Branch::connect's call) is charged to store_open.
        let mut prev = (m1.bytes, m1.allocs);
        for k in 1..NL {
            if seen & (1 << k) == 0 {
                continue;
            }
            iv.bytes[k].add(pb(k) - prev.0);
            iv.allocs[k].add(pa(k) - prev.1);
            prev = (pb(k), pa(k));
        }
        iv.bytes[NL].add(m2.bytes - prev.0);
        iv.allocs[NL].add(m2.allocs - prev.1);
        iv.bytes[NL + 1].add(m2.bytes - m1.bytes);
        iv.allocs[NL + 1].add(m2.allocs - m1.allocs);
        iv.bytes[NL + 2].add(m3.bytes - m2.bytes);
        iv.allocs[NL + 2].add(m3.allocs - m2.allocs);
        if self.args.timing {
            iv.times[0].push(t1 - t0);
            iv.times[1].push(t3 - t2);
            iv.times[2].push(t5 - t4);
        }
        Session {
            branch,
            conn,
            row,
            wrote,
            trunk_writes_at_fork: at_fork,
            touched: false,
        }
    }
}

/// Totals over every held session; the min/max of cached pages are over the sessions the active arm
/// has not used (their idle footprint), and `touched` counts the others.
fn footprints(sessions: &[Session]) -> (u64, u64, u64, u64, u64, u64, SessionFootprint) {
    let (mut pages, mut pmin, mut pmax, mut locks, mut shared) = (0u64, u64::MAX, 0u64, 0u64, 0u64);
    let mut capacity = 0u64;
    let mut touched = 0u64;
    for s in sessions {
        let Some(conn) = &s.conn else {
            continue;
        };
        let f = conn.session_footprint();
        pages += f.cached_pages as u64;
        capacity += f.cache_capacity as u64;
        if s.touched {
            touched += 1;
        } else {
            pmin = pmin.min(f.cached_pages as u64);
            pmax = pmax.max(f.cached_pages as u64);
        }
        locks += f.holds_read_lock as u64;
        shared += f.schema_shared_with_store as u64;
    }
    let first = sessions
        .first()
        .and_then(|s| s.conn.as_ref())
        .map(|c| c.session_footprint())
        .unwrap_or_default();
    CACHE_CAPACITY_TOTAL.store(capacity, Ordering::Relaxed);
    (pages, pmin, pmax, locks, shared, touched, first)
}

/// Sum of held sessions' page-cache capacities at the last footprint scan.
static CACHE_CAPACITY_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_sessions.db");
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
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    for k in 1..args.tables {
        trunk
            .execute(format!(
                "CREATE TABLE x{k}(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)"
            ))
            .unwrap();
        trunk
            .execute(format!("CREATE INDEX x{k}_a ON x{k}(a)"))
            .unwrap();
        for j in 0..4 {
            trunk
                .execute(format!("INSERT INTO x{k} VALUES ({j}, 'a{}', {j})", j % 2))
                .unwrap();
        }
    }
    if args.trunk_writers > 0 {
        trunk
            .execute("CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        for id in 1..=(args.trunk_writers as i64) * 1000 {
            trunk.execute(format!("INSERT INTO w VALUES ({id}, 0)")).unwrap();
        }
    }
    trunk.execute("COMMIT").unwrap();
    if args.analyze {
        trunk.execute("ANALYZE").unwrap();
    }
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let int = |sql: &str| {
        trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    let page_size = int("PRAGMA page_size");
    let trunk_pages = int("PRAGMA page_count");
    let synchronous = int("PRAGMA synchronous");

    println!("# branch_sessions — Turso fork, r11-sessions PREREG (frontier/round11/r11-sessions)");
    println!(
        "# mode={:?} checkpoints={:?} analyze={} tables={} active={} timing={} seed={:#x} \
         swap_limit_mib={} swap_growth_mib={} level_floor={} mux={} preimage={:?} trunk_writers={} \
         trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} \
         page_size={page_size} trunk_pages={trunk_pages} trunk_synchronous={synchronous}",
        args.mode,
        args.checkpoints,
        args.analyze,
        args.tables,
        args.active,
        args.timing,
        args.seed,
        args.swap_limit_mib,
        args.swap_growth_mib,
        args.level_floor,
        args.mux,
        args.preimage,
        args.trunk_writers
    );
    println!(
        "# build: {} ; clock tick {:.0} ns",
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        clock_tick_ns()
    );
    for (name, size) in turso_core::branch::session_type_sizes() {
        println!("# size_of {name} = {size}");
    }
    let wal_path = PathBuf::from(format!("{}-wal", path.to_str().unwrap()));
    let mut b = Bench {
        db: db.clone(),
        wal_path,
        trunk,
        model: TrunkModel::default(),
        rng: Rng(args.seed),
        args,
    };
    let swap0 = swap_used_mib();
    start_watchdog(
        swap0,
        b.args.swap_growth_mib,
        b.args.swap_limit_mib,
        b.args.level_floor,
    );
    let base = mem();
    let fds0 = open_fds();
    println!(
        "# base live_bytes={} live_allocs={} fds={fds0} rss_bytes={} swap_used_mib={} memory_level={}",
        base.bytes,
        base.allocs,
        rss_bytes(),
        swap_used_mib(),
        memory_level()
    );
    println!(
        "#\tx\tphase\tsessions\tbytes_mean\tbytes_min\tbytes_max\tallocs_mean\tallocs_min\tallocs_max"
    );

    if b.args.interleave || b.args.capscan.is_some() || b.args.pin.is_some() {
        let n = *b.args.checkpoints.last().unwrap();
        if let Some((g, r)) = b.args.pin {
            pin_arm(&mut b, g, r, n);
        } else if b.args.interleave {
            interleave_arm(&mut b, n);
        } else {
            let cache = b.args.cache_size.unwrap_or_else(|| die("--capscan needs --cache-size"));
            let rows = b.args.capscan.unwrap();
            capscan_arm(&mut b, rows, cache, n);
        }
        let end = db.branch_stats();
        if end.live_branches != 0 || end.arena_slots_in_use != 0 {
            not_a_result(&format!("arm teardown leaked: {end:?}"));
        }
        println!("# teardown: every branch freed, arena empty");
        return;
    }
    if let Some(d) = b.args.chain {
        let (r, n) = (b.args.preimage.unwrap_or(0), *b.args.checkpoints.last().unwrap());
        chain_arm(&mut b, d, r, n);
        let end = db.branch_stats();
        if end.live_branches != 0 || end.arena_slots_in_use != 0 {
            not_a_result(&format!("chain arm teardown leaked: {end:?}"));
        }
        println!("# teardown: every branch freed, arena empty");
        return;
    }
    let mut sessions: Vec<Session> = Vec::new();
    let mut limit: Option<String> = None;
    let checkpoints = b.args.checkpoints.clone();
    let mut reached = Vec::new();
    for &n in &checkpoints {
        let mut iv = Interval::default();
        let lo = sessions.len();
        let fds_before = open_fds();
        let mem_before = mem();
        let locks_before = b.lock_takes();
        let t = Instant::now();
        while sessions.len() < n {
            if STOP.load(Ordering::Acquire) {
                limit = Some(format!(
                    "N={} swap_used_mib={} memory_level={} live_bytes={} rss_bytes={}",
                    sessions.len(),
                    STOP_SWAP.load(Ordering::Relaxed),
                    STOP_LEVEL.load(Ordering::Relaxed),
                    mem().bytes,
                    rss_bytes()
                ));
                break;
            }
            let i = sessions.len();
            let s = b.open_session(i, &mut iv);
            sessions.push(s);
        }
        let grow_s = t.elapsed().as_secs_f64();
        let locks_growth = b.lock_takes() - locks_before;
        let x = sessions.len();
        let added = x - lo;
        let mem_after = mem();
        let fds_after = open_fds();
        let st = b.db.branch_stats();
        let expect_arena = if b.args.mode == Mode::Write { x } else { 0 };
        if st.live_branches != x {
            not_a_result(&format!("expected {x} live branches, engine has {}", st.live_branches));
        }
        let (pages, pmin, pmax, locks, shared, touched, first) = footprints(&sessions);
        for (k, name) in PHASES.iter().enumerate() {
            let (bb, aa) = (&iv.bytes[k], &iv.allocs[k]);
            if bb.n == 0 {
                continue;
            }
            println!(
                "phase\t{x}\t{name}\t{}\t{:.2}\t{}\t{}\t{:.3}\t{}\t{}",
                bb.n,
                bb.mean(),
                bb.min,
                bb.max,
                aa.mean(),
                aa.min,
                aa.max
            );
        }
        if b.args.timing && added > 0 {
            for (k, name) in ["fork", "connect", "action"].iter().enumerate() {
                let (p50, p90, p99, max) = pct(&iv.times[k]);
                println!("time\t{x}\t{name}\t{added}\t{p50:.2}\t{p90:.2}\t{p99:.2}\t{max:.2}");
            }
        }
        println!(
            "# x={x} added={added} interval_bytes_per_session={:.2} interval_allocs_per_session={:.3} \
             live_bytes={} live_allocs={} bytes_per_session_total={:.2} fds={} fds_delta_interval={} \
             cached_pages_total={pages} cached_pages_min={pmin} cached_pages_max={pmax} \
             sessions_touched_by_active={touched} cache_capacity_total={} \
             sessions_holding_read_lock={locks} sessions_schema_shared={shared} \
             arena_in_use={} arena_expected_own={expect_arena} rss_bytes={} swap_used_mib={} \
             memory_level={} wal_bytes={} grow_s={grow_s:.1}",
            if added > 0 {
                (mem_after.bytes - mem_before.bytes) as f64 / added as f64
            } else {
                0.0
            },
            if added > 0 {
                (mem_after.allocs - mem_before.allocs) as f64 / added as f64
            } else {
                0.0
            },
            mem_after.bytes,
            mem_after.allocs,
            (mem_after.bytes - base.bytes) as f64 / x.max(1) as f64,
            fds_after,
            fds_after as i64 - fds_before as i64,
            CACHE_CAPACITY_TOTAL.load(Ordering::Relaxed),
            st.arena_slots_in_use,
            rss_bytes(),
            swap_used_mib(),
            memory_level(),
            b.wal_bytes(),
        );
        let work = b.db.branch_stats().work;
        println!(
            "# pool x={x} trunk_cache_pages={} slot_clones={} retained_clones={} slot_ref_hits={} \
             slot_unshared_writes={}",
            b.db.trunk_cache_pages(),
            b.db.slot_clone_count(),
            b.db.retained_clone_count(),
            work.slot_ref_hits,
            work.slot_unshared_writes
        );
        println!(
            "# growth_cost x={x} store_lock_takes_per_session={:.3}",
            locks_growth as f64 / added.max(1) as f64
        );
        println!(
            "# footprint session0: functions={} collations={} vtabs={} vtab_modules={} index_methods={}",
            first.sym_functions,
            first.sym_collations,
            first.sym_vtabs,
            first.sym_vtab_modules,
            first.sym_index_methods
        );
        reached.push(x);
        if let Some(r) = b.args.preimage {
            preimage_arm(&mut b, &mut sessions, x, r);
        }
        if b.args.active > 0 && !sessions.is_empty() {
            active_arm(&mut b, &mut sessions, x);
        }
        if limit.is_some() {
            break;
        }
    }
    match &limit {
        Some(l) => println!("# LIMIT reached at {l}"),
        None => println!("# LIMIT not reached: every checkpoint grown"),
    }
    println!(
        "# guard: swap_at_start_mib={swap0} max_swap_mib={} min_memory_level={}",
        MAX_SWAP.load(Ordering::Relaxed),
        MIN_LEVEL.load(Ordering::Relaxed)
    );

    // Teardown, decomposed: close every intx transaction, then drop the connections, then the
    // branch handles, reading the allocator between steps.
    let n = sessions.len();
    if b.args.mode == Mode::Intx {
        for s in &sessions {
            if let Some(conn) = &s.conn {
                conn.execute("COMMIT").unwrap();
            }
        }
    }
    let m0 = mem();
    let fds_a = open_fds();
    let mut branches = Vec::with_capacity(n);
    for s in sessions {
        branches.push(s.branch);
        drop(s.conn);
    }
    let m1 = mem();
    let fds_b = open_fds();
    for br in branches {
        br.reap().unwrap();
    }
    let m2 = mem();
    println!(
        "# teardown sessions={n} conn_bytes_freed_per_session={:.2} conn_allocs_freed_per_session={:.3} \
         branch_bytes_freed_per_session={:.2} branch_allocs_freed_per_session={:.3} fds_freed={}",
        (m0.bytes - m1.bytes) as f64 / n.max(1) as f64,
        (m0.allocs - m1.allocs) as f64 / n.max(1) as f64,
        (m1.bytes - m2.bytes) as f64 / n.max(1) as f64,
        (m1.allocs - m2.allocs) as f64 / n.max(1) as f64,
        fds_a as i64 - fds_b as i64
    );
    let end = db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    if PROBE_UNKNOWN.load(Ordering::Relaxed) != 0 {
        not_a_result("the engine called a probe label the harness does not know");
    }
    println!("# teardown: every branch freed, arena empty; reached {reached:?}");
}

/// The pre-image arm (amendment 9): after every held session forked, the trunk rewrites one row on
/// each of `r` of the 4 leaves every session then reads. A session's read of a rewritten leaf resolves
/// to the pre-image the trunk retained for it. Per session: the allocator bytes of the read and the
/// store's counters for it.
fn preimage_arm(b: &mut Bench, sessions: &mut [Session], x: usize, r: usize) {
    const ROWS: [i64; 4] = [1, 602, 1203, 1804];
    if r > ROWS.len() {
        die("--preimage R needs R <= 4");
    }
    for &row in &ROWS[..r] {
        let g = b.model.record(row);
        b.trunk
            .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g)))
            .unwrap();
    }
    let fs9 = std::env::var("TURSO_R11S_FS9").unwrap_or_default();
    let (mut bytes, mut res, mut hits, mut misses, mut arena) =
        (Acc::default(), Acc::default(), Acc::default(), Acc::default(), Acc::default());
    let (mut copies, mut shared, mut fills) = (Acc::default(), Acc::default(), Acc::default());
    let mut first = None;
    for (i, s) in sessions.iter_mut().enumerate() {
        s.touched = true;
        let conn = s.conn.as_ref().expect("the pre-image arm holds connections");
        let (m0, w0) = (mem(), b.db.branch_stats().work);
        for &row in &ROWS {
            let got = read_v(conn, row);
            let want = b.model.value_at(row, s.trunk_writes_at_fork);
            if got != want {
                not_a_result(&format!("pre-image arm: session {i} read {got} for row {row}, want {want}"));
            }
        }
        let (m1, w1) = (mem(), b.db.branch_stats().work);
        let d = |f: fn(&turso_core::branch::BranchWork) -> u64| (f(&w1) - f(&w0)) as i64;
        let row = (
            m1.bytes - m0.bytes,
            d(|w| w.resolve_calls),
            d(|w| w.trunk_page_hits),
            d(|w| w.trunk_page_misses),
            d(|w| w.retained_copies),
            d(|w| w.retained_shared_hits),
            d(|w| w.retained_clone_fills),
        );
        if i == 0 {
            first = Some(row);
            continue;
        }
        bytes.add(row.0);
        res.add(row.1);
        hits.add(row.2);
        misses.add(row.3);
        arena.add(row.1 - row.2 - row.3);
        copies.add(row.4);
        shared.add(row.5);
        fills.add(row.6);
    }
    println!("# preimage x={x} r={r} w=4 fs9={fs9:?} first_session={first:?} (bytes, resolves, hits, misses, retained_copies, retained_shared_hits, retained_clone_fills)");
    for (name, a) in [
        ("read_bytes", &bytes),
        ("resolves", &res),
        ("trunk_page_hits", &hits),
        ("trunk_page_misses", &misses),
        ("arena_resolved", &arena),
        ("retained_copies", &copies),
        ("retained_shared_hits", &shared),
        ("retained_clone_fills", &fills),
    ] {
        println!("preimage\t{x}\t{r}\t{name}\t{}\t{:.2}\t{}\t{}", a.n, a.mean(), a.min, a.max);
    }
    println!(
        "# preimage x={x} retained_clones={} arena_in_use={}",
        b.db.retained_clone_count(),
        b.db.branch_stats().arena_slots_in_use
    );
}

/// The ancestor-chain arm (amendment 13). A chain trunk -> a1 -> ... -> ad; a1 writes one row on
/// each of `r` of the 4 leaves every session reads, then the rest of the chain and `n` held sessions
/// fork from ad and connect, then a1 rewrites those rows. A session's read of such a row resolves
/// through its inherited page map to a1's RETAINED version (an ancestor branch's pre-image), which
/// FS9 does not share and FS9B does. Per session: the read's bytes and the store's counters.
fn chain_arm(b: &mut Bench, d: usize, r: usize, n: usize) {
    const ROWS: [i64; 4] = [1, 602, 1203, 1804];
    if d == 0 || r > ROWS.len() {
        die("--chain D needs D >= 1 and --preimage R <= 4");
    }
    let a1 = b.trunk.fork_branch().unwrap();
    let a1c = a1.connect().unwrap();
    let v1 = |row: i64| format!("a{:0>width$}", row, width = VALUE_LEN - 1);
    let v2 = |row: i64| format!("z{:0>width$}", row, width = VALUE_LEN - 1);
    for &row in &ROWS[..r] {
        a1c.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", v1(row)))
            .unwrap();
    }
    let mut chain = vec![a1];
    while chain.len() < d {
        let next = chain.last().unwrap().fork().unwrap();
        chain.push(next);
    }
    let tip = chain.last().unwrap();
    let mut held = Vec::with_capacity(n);
    for _ in 0..n {
        let br = tip.fork().unwrap();
        let conn = br.connect().unwrap();
        held.push((br, conn));
    }
    for &row in &ROWS[..r] {
        a1c.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", v2(row)))
            .unwrap();
    }
    let want = |row: i64| {
        if ROWS[..r].contains(&row) {
            v1(row)
        } else {
            trunk_value(row)
        }
    };
    let fs9 = std::env::var("TURSO_R11S_FS9").unwrap_or_default();
    let names = [
        "read_bytes",
        "resolves",
        "trunk_page_hits",
        "inherited_copies",
        "inherited_shared_hits",
        "inherited_clone_fills",
        "retained_copies",
        "slot_ref_hits",
    ];
    let mut accs: [Acc; 8] = Default::default();
    let mut first = None;
    for (i, (_, conn)) in held.iter().enumerate() {
        let (m0, w0) = (mem(), b.db.branch_stats().work);
        for &row in &ROWS {
            let got = read_v(conn, row);
            if got != want(row) {
                not_a_result(&format!("chain arm: session {i} read {got} for row {row}"));
            }
        }
        let (m1, w1) = (mem(), b.db.branch_stats().work);
        let d_ = |f: fn(&turso_core::branch::BranchWork) -> u64| (f(&w1) - f(&w0)) as i64;
        let row = [
            m1.bytes - m0.bytes,
            d_(|w| w.resolve_calls),
            d_(|w| w.trunk_page_hits),
            d_(|w| w.inherited_copies),
            d_(|w| w.inherited_shared_hits),
            d_(|w| w.inherited_clone_fills),
            d_(|w| w.retained_copies),
            d_(|w| w.slot_ref_hits),
        ];
        if i == 0 {
            first = Some(row);
            continue;
        }
        for (a, v) in accs.iter_mut().zip(row) {
            a.add(v);
        }
    }
    println!(
        "# chain d={d} r={r} n={n} fs9={fs9:?} first_session={first:?} ({})",
        names.join(", ")
    );
    for (name, a) in names.iter().zip(&accs) {
        println!("chain\t{d}\t{r}\t{name}\t{}\t{:.2}\t{}\t{}", a.n, a.mean(), a.min, a.max);
    }
    println!(
        "# chain d={d} r={r} slot_clones={} retained_clones={} arena_in_use={}",
        b.db.slot_clone_count(),
        b.db.retained_clone_count(),
        b.db.branch_stats().arena_slots_in_use
    );
    // a1 still reads its own newest versions.
    for &row in &ROWS[..r] {
        if read_v(&a1c, row) != v2(row) {
            not_a_result("chain arm: a1 lost its own rewrite");
        }
    }
    for (br, conn) in held {
        drop(conn);
        br.reap().unwrap();
    }
    drop(a1c);
    while let Some(br) = chain.pop() {
        br.reap().unwrap();
    }
    println!(
        "# chain d={d} r={r} after teardown slot_clones={} retained_clones={}",
        b.db.slot_clone_count(),
        b.db.retained_clone_count()
    );
}

/// The interleaved pre-image arm (amendment 14; r11-sessions-refute via the lead). Sessions fork from
/// the trunk one at a time, and between consecutive forks the trunk rewrites one row on one of the
/// W = 4 leaves every session reads, round robin. Session i then sees, for each leaf, the version
/// current at its fork: about n distinct retained versions over the arm, each seen by about W
/// sessions. Per session: the read's bytes and the store's counters. Per arm: distinct versions
/// seen, retained versions seen, FS9's clones, and reads per version and per clone.
fn interleave_arm(b: &mut Bench, n: usize) {
    const ROWS: [i64; 4] = [1, 602, 1203, 1804];
    let fs9 = std::env::var("TURSO_R11S_FS9").unwrap_or_default();
    let fs10 = std::env::var("TURSO_R11S_FS10").unwrap_or_default();
    let m0 = mem();
    let arena0 = b.db.branch_stats().arena_slots_in_use;
    let mut held = Vec::with_capacity(n);
    for i in 0..n {
        let at_fork = b.model.writes;
        let br = b.trunk.fork_branch().unwrap();
        let conn = br.connect().unwrap();
        held.push((br, conn, at_fork));
        let row = ROWS[i % ROWS.len()];
        let g = b.model.record(row);
        b.trunk
            .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g)))
            .unwrap();
    }
    let m1 = mem();
    let arena1 = b.db.branch_stats().arena_slots_in_use;
    let names = [
        "read_bytes",
        "resolves",
        "trunk_page_hits",
        "trunk_page_misses",
        "retained_copies",
        "retained_shared_hits",
        "retained_clone_fills",
        "cached_pages_idle",
        "slot_ref_hits",
    ];
    let mut accs: [Acc; 9] = Default::default();
    let mut first = None;
    let mut versions = std::collections::HashSet::new();
    let mut retained = std::collections::HashSet::new();
    let (mut retained_reads, mut shared_hits) = (0i64, 0i64);
    for (i, (_, conn, at_fork)) in held.iter().enumerate() {
        let (ma, w0) = (mem(), b.db.branch_stats().work);
        for &row in &ROWS {
            let got = read_v(conn, row);
            let want = b.model.value_at(row, *at_fork);
            if got != want {
                not_a_result(&format!("interleave arm: session {i} read {got} for row {row}, want {want}"));
            }
            let (v, len) = b.model.history.get(&row).map_or((0, 0), |h| {
                (h.partition_point(|&(seq, _)| seq < *at_fork), h.len())
            });
            versions.insert((row, v));
            if v < len {
                retained.insert((row, v));
            }
        }
        let (mb, w1) = (mem(), b.db.branch_stats().work);
        let d = |f: fn(&turso_core::branch::BranchWork) -> u64| (f(&w1) - f(&w0)) as i64;
        let row = [
            mb.bytes - ma.bytes,
            d(|w| w.resolve_calls),
            d(|w| w.trunk_page_hits),
            d(|w| w.trunk_page_misses),
            d(|w| w.retained_copies),
            d(|w| w.retained_shared_hits),
            d(|w| w.retained_clone_fills),
            conn.session_footprint().cached_pages as i64,
            d(|w| w.slot_ref_hits),
        ];
        retained_reads += row[4] + row[5] + row[6] + row[8];
        shared_hits += row[5] + row[6] + row[8];
        if i == 0 {
            first = Some(row);
            continue;
        }
        for (a, v) in accs.iter_mut().zip(row) {
            a.add(v);
        }
    }
    let m2 = mem();
    let clones = b.db.retained_clone_count();
    println!(
        "# interleave n={n} w=4 fs9={fs9:?} fs10={fs10:?} first_session={first:?} ({})",
        names.join(", ")
    );
    for (name, a) in names.iter().zip(&accs) {
        println!("interleave\t{n}\t{name}\t{}\t{:.2}\t{}\t{}", a.n, a.mean(), a.min, a.max);
    }
    println!(
        "# interleave n={n} distinct_versions={} retained_versions={} retained_reads={retained_reads} \
         retained_clones={clones} reads_per_retained_version={:.3} shared_reads_per_clone={:.3} \
         arena_before={arena0} arena_after_forks={arena1}",
        versions.len(),
        retained.len(),
        retained_reads as f64 / retained.len().max(1) as f64,
        shared_hits as f64 / clones.max(1) as f64,
    );
    println!(
        "# interleave n={n} bytes_per_session: forks_and_rewrites={:.2} reads={:.2} total={:.2}",
        (m1.bytes - m0.bytes) as f64 / n as f64,
        (m2.bytes - m1.bytes) as f64 / n as f64,
        (m2.bytes - m0.bytes) as f64 / n as f64,
    );
    for (br, conn, _) in held {
        drop(conn);
        br.reap().unwrap();
    }
    println!(
        "# interleave n={n} after teardown retained_clones={} slot_clones={}",
        b.db.retained_clone_count(),
        b.db.slot_clone_count()
    );
}

/// The pinning arm (amendment 17; the fresh-context adversary's claim 3). `n` sessions fork from the
/// trunk in groups of `g`, each connected and held; after each group the trunk rewrites `r` rows on
/// `r` distinct leaves (rows 1, 41, 81, …). A trunk write retains the version it supersedes while a
/// child that forked since that version was written lives, so every group pins one version of each
/// of those pages whether or not its sessions read them: `n / g × r` retained pages, one page per
/// (group, page). Per arm: arena pages before and after, bytes per session, and a read check of
/// every pinned row in every session.
fn pin_arm(b: &mut Bench, g: usize, r: usize, n: usize) {
    if g == 0 || r == 0 || n % g != 0 || 1 + 40 * (r as i64 - 1) > TRUNK_ROWS {
        die("--pin G,R needs G, R >= 1, G dividing n, and R <= 500");
    }
    let rows: Vec<i64> = (0..r as i64).map(|k| 1 + 40 * k).collect();
    let fs = |k: &str| std::env::var(k).unwrap_or_default();
    let m0 = mem();
    let arena0 = b.db.branch_stats().arena_slots_in_use;
    let mut held = Vec::with_capacity(n);
    for i in 0..n {
        let at_fork = b.model.writes;
        let br = b.trunk.fork_branch().unwrap();
        let conn = br.connect().unwrap();
        held.push((br, conn, at_fork));
        if (i + 1) % g == 0 {
            b.trunk.execute("BEGIN").unwrap();
            for &row in &rows {
                let v = b.model.record(row);
                b.trunk
                    .execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(v)))
                    .unwrap();
            }
            b.trunk.execute("COMMIT").unwrap();
        }
    }
    let m1 = mem();
    let arena1 = b.db.branch_stats().arena_slots_in_use;
    for (i, (_, conn, at_fork)) in held.iter().enumerate() {
        for &row in &rows {
            let got = read_v(conn, row);
            let want = b.model.value_at(row, *at_fork);
            if got != want {
                not_a_result(&format!("pin arm: session {i} read {got} for row {row}, want {want}"));
            }
        }
    }
    let m2 = mem();
    println!(
        "# pin n={n} g={g} r={r} fs9={:?} fs10={:?} fs11={:?} groups={} arena_before={arena0} \
         arena_after_forks={arena1} pinned_pages={} pinned_per_session={:.4} \
         bytes_per_session_forks_and_rewrites={:.2} bytes_per_session_reads={:.2} \
         bytes_per_pinned_page={:.2} retained_clones={} slot_clones={}",
        fs("TURSO_R11S_FS9"),
        fs("TURSO_R11S_FS10"),
        fs("TURSO_R11S_FS11"),
        n / g,
        arena1 - arena0,
        (arena1 - arena0) as f64 / n as f64,
        (m1.bytes - m0.bytes) as f64 / n as f64,
        (m2.bytes - m1.bytes) as f64 / n as f64,
        if arena1 > arena0 {
            (m1.bytes - m0.bytes) as f64 / (arena1 - arena0) as f64
        } else {
            0.0
        },
        b.db.retained_clone_count(),
        b.db.slot_clone_count(),
    );
    for (br, conn, _) in held {
        drop(conn);
        br.reap().unwrap();
    }
    println!(
        "# pin n={n} after teardown arena_in_use={}",
        b.db.branch_stats().arena_slots_in_use
    );
}

/// The cap-engaged scan arm (amendment 14; r11-sessions-refute via the lead). A table `big` of
/// `rows` rows, many more pages than a session's cache holds. Every held session sets
/// `PRAGMA cache_size = cache` (above the 2,000-page default, below the table) and scans `big` once,
/// so the cache's cap engages and evicts during the scan. Per session: the bytes the idle session
/// keeps after its scan, its cached pages and capacity, and the store's counters for the scan.
fn capscan_arm(b: &mut Bench, rows: i64, cache: i64, n: usize) {
    let fs10 = std::env::var("TURSO_R11S_FS10").unwrap_or_default();
    b.trunk
        .execute("CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    b.trunk.execute("BEGIN").unwrap();
    for id in 1..=rows {
        b.trunk
            .execute(format!("INSERT INTO big VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    b.trunk.execute("COMMIT").unwrap();
    let int = |sql: &str| -> i64 {
        b.trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    let page_count = int("PRAGMA page_count");
    println!(
        "# capscan rows={rows} db_pages={page_count} cache_size={cache} n={n} fs10={fs10:?}"
    );
    let names = [
        "idle_bytes",
        "cached_pages_idle",
        "cache_capacity",
        "resolves",
        "trunk_page_hits",
        "trunk_page_misses",
    ];
    let mut accs: [Acc; 6] = Default::default();
    let mut first = None;
    let mut held = Vec::with_capacity(n);
    let want = (rows, rows * VALUE_LEN as i64);
    let m0 = mem();
    for i in 0..n {
        let (ma, w0) = (mem(), b.db.branch_stats().work);
        let br = b.trunk.fork_branch().unwrap();
        let conn = br.connect().unwrap();
        conn.execute(format!("PRAGMA cache_size = {cache}")).unwrap();
        let got = conn
            .prepare("SELECT count(*), sum(length(v)) FROM big")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        if (got[0][0].as_int(), got[0][1].as_int()) != (Some(want.0), Some(want.1)) {
            not_a_result(&format!("capscan: session {i} scanned {:?}", got[0]));
        }
        let (mb, w1) = (mem(), b.db.branch_stats().work);
        let f = conn.session_footprint();
        let d = |f: fn(&turso_core::branch::BranchWork) -> u64| (f(&w1) - f(&w0)) as i64;
        let row = [
            mb.bytes - ma.bytes,
            f.cached_pages as i64,
            f.cache_capacity as i64,
            d(|w| w.resolve_calls),
            d(|w| w.trunk_page_hits),
            d(|w| w.trunk_page_misses),
        ];
        held.push((br, conn));
        if i == 0 {
            first = Some(row);
            continue;
        }
        for (a, v) in accs.iter_mut().zip(row) {
            a.add(v);
        }
    }
    let m1 = mem();
    println!(
        "# capscan n={n} first_session={first:?} ({})",
        names.join(", ")
    );
    for (name, a) in names.iter().zip(&accs) {
        println!("capscan\t{n}\t{name}\t{}\t{:.2}\t{}\t{}", a.n, a.mean(), a.min, a.max);
    }
    println!(
        "# capscan n={n} bytes_per_session={:.2} trunk_cache_pages={} rss_bytes={}",
        (m1.bytes - m0.bytes) as f64 / n as f64,
        b.db.trunk_cache_pages(),
        rss_bytes()
    );
    for (br, conn) in held {
        drop(conn);
        br.reap().unwrap();
    }
}

/// Concurrent trunk writers for the active arm (amendment 9): each thread has its own trunk
/// connection and commits one-row updates to its own rows of table `w` (never read by any check)
/// until told to stop, retrying Busy.
struct Writers {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handles: Vec<std::thread::JoinHandle<(u64, u64)>>,
}

fn start_writers(db: &Arc<Database>, n: usize) -> Writers {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handles = (0..n)
        .map(|t| {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                let conn = db.connect().unwrap();
                conn.execute("PRAGMA synchronous = NORMAL").unwrap();
                let (mut commits, mut busy) = (0u64, 0u64);
                let mut i = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let id = 1 + (t as u64) * 1000 + i % 1000;
                    match conn.execute(format!("UPDATE w SET v = v + 1 WHERE id = {id}")) {
                        Ok(()) => commits += 1,
                        Err(turso_core::LimboError::Busy | turso_core::LimboError::BusySnapshot) => {
                            busy += 1;
                            std::thread::yield_now();
                        }
                        Err(e) => panic!("trunk writer {t}: {e}"),
                    }
                    i += 1;
                }
                (commits, busy)
            })
        })
        .collect();
    Writers { stop, handles }
}

/// Busy results retried by `retry_busy`, reported per active arm.
static BUSY_RETRIES: AtomicUsize = AtomicUsize::new(0);

/// Run `f`, retrying `Busy`/`BusySnapshot` (yielding between tries, at most 100,000 times) and
/// counting each retry in BUSY_RETRIES; any other error is not a result.
fn retry_busy<T>(mut f: impl FnMut() -> turso_core::Result<T>) -> T {
    for _ in 0..100_000 {
        match f() {
            Ok(v) => return v,
            Err(turso_core::LimboError::Busy | turso_core::LimboError::BusySnapshot) => {
                BUSY_RETRIES.fetch_add(1, Ordering::Relaxed);
                std::thread::yield_now();
            }
            Err(e) => not_a_result(&format!("trunk op failed: {e}")),
        }
    }
    not_a_result("trunk op still Busy after 100,000 retries")
}

/// The active ops, K cycles, with every held session still open.
fn active_arm(b: &mut Bench, sessions: &mut [Session], x: usize) {
    let k = b.args.active;
    let writers = (b.args.trunk_writers > 0).then(|| start_writers(&b.db, b.args.trunk_writers));
    let bufs0 = turso_core::io::temp_buffer_cache_counts();
    let mut op_tl: [Acc; 5] = Default::default();
    let mut ops: [(Acc, Acc, Vec<Duration>); 5] = Default::default();
    // Per op: store-lock acquisitions and allocations (K12: what a multiplexed statement pays).
    let mut op_locks: [Acc; 5] = Default::default();
    let mut op_allocs: [Acc; 5] = Default::default();
    let names = [
        "trunk_commit",
        "sess_read_cold",
        "sess_read_warm",
        "new_session",
        "ckpt",
    ];
    let (mut busy, mut log, mut ckpt) = (Acc::default(), Acc::default(), Acc::default());
    let wal0 = b.wal_bytes();
    let intx = b.args.mode == Mode::Intx;
    for c in 0..k {
        // 1. the trunk commits
        let (r0, m0, l0, t0) = (b.resolves(), mem(), b.lock_takes(), tl_bytes());
        let t = Instant::now();
        b.trunk_commit();
        ops[0].2.push(t.elapsed());
        ops[0].0.add((b.resolves() - r0) as i64);
        ops[0].1.add(mem().bytes - m0.bytes);
        op_locks[0].add((b.lock_takes() - l0) as i64);
        op_allocs[0].add(mem().allocs - m0.allocs);
        op_tl[0].add(tl_bytes() - t0);

        // 2 and 3. a held session reads its row, twice
        let si = b.rng.below(sessions.len());
        sessions[si].touched = true;
        for op in [1, 2] {
            let s = &sessions[si];
            let (r0, m0, l0, t0) = (b.resolves(), mem(), b.lock_takes(), tl_bytes());
            let t = Instant::now();
            let got = match &s.conn {
                Some(conn) => read_v(conn, s.row),
                None => read_v(&s.branch.connect().unwrap(), s.row),
            };
            ops[op].2.push(t.elapsed());
            ops[op].0.add((b.resolves() - r0) as i64);
            ops[op].1.add(mem().bytes - m0.bytes);
            op_locks[op].add((b.lock_takes() - l0) as i64);
            op_allocs[op].add(mem().allocs - m0.allocs);
            op_tl[op].add(tl_bytes() - t0);
            if got != s.expect(&b.model) {
                not_a_result(&format!("held session {si} read {got} at active cycle {c}"));
            }
        }

        // 4. a fresh session, used once and reaped
        let row = row_for(1_000_000_000 + c);
        let at_fork = b.model.writes;
        let (r0, m0, l0, t0) = (b.resolves(), mem(), b.lock_takes(), tl_bytes());
        let t = Instant::now();
        // A trunk fork takes the trunk's WAL write lock; with concurrent trunk writers it can find it
        // held (v2_mux_fw3_w2 aborted on exactly this Busy): retry, counted.
        let br = retry_busy(|| b.trunk.fork_branch());
        let conn = br.connect().unwrap();
        let got = read_v(&conn, row);
        drop(conn);
        let reaped = br.reap().unwrap();
        ops[3].2.push(t.elapsed());
        ops[3].0.add((b.resolves() - r0) as i64);
        ops[3].1.add(mem().bytes - m0.bytes);
        op_locks[3].add((b.lock_takes() - l0) as i64);
        op_allocs[3].add(mem().allocs - m0.allocs);
        op_tl[3].add(tl_bytes() - t0);
        if got != b.model.value_at(row, at_fork) || reaped.deferred {
            not_a_result(&format!("fresh session at cycle {c}: read {got}, {reaped:?}"));
        }

        // 5. a passive checkpoint
        let (r0, m0, l0, t0) = (b.resolves(), mem(), b.lock_takes(), tl_bytes());
        let t = Instant::now();
        let (bu, lg, ck) = b.checkpoint_passive();
        ops[4].2.push(t.elapsed());
        ops[4].0.add((b.resolves() - r0) as i64);
        ops[4].1.add(mem().bytes - m0.bytes);
        op_locks[4].add((b.lock_takes() - l0) as i64);
        op_allocs[4].add(mem().allocs - m0.allocs);
        op_tl[4].add(tl_bytes() - t0);
        busy.add(bu);
        log.add(lg);
        ckpt.add(ck);
    }
    for (i, name) in names.iter().enumerate() {
        let (res, bytes, times) = &ops[i];
        let timing = if b.args.timing {
            let (p50, p90, p99, max) = pct(times);
            format!("\t{p50:.2}\t{p90:.2}\t{p99:.2}\t{max:.2}")
        } else {
            String::new()
        };
        println!(
            "active\t{x}\t{name}\t{k}\t{:.2}\t{}\t{}\t{:.2}{timing}",
            res.mean(),
            res.min,
            res.max,
            bytes.mean()
        );
        println!(
            "active_cost\t{x}\t{name}\t{k}\tlock_takes {:.2} [{},{}]\tallocs {:.2} [{},{}]",
            op_locks[i].mean(),
            op_locks[i].min,
            op_locks[i].max,
            op_allocs[i].mean(),
            op_allocs[i].min,
            op_allocs[i].max
        );
        println!(
            "active_tl\t{x}\t{name}\t{k}\tmain_thread_bytes {:.2} [{},{}]",
            op_tl[i].mean(),
            op_tl[i].min,
            op_tl[i].max
        );
    }
    println!(
        "# active x={x} intx={intx} ckpt_busy_mean={:.3} ckpt_log_min={} ckpt_log_max={} \
         ckpt_checkpointed_min={} ckpt_checkpointed_max={} ckpt_nonint={} wal_bytes_before={wal0} \
         wal_bytes_after={} trunk_writes={}",
        busy.mean(),
        log.min,
        log.max,
        ckpt.min,
        ckpt.max,
        CKPT_NONINT.load(Ordering::Relaxed),
        b.wal_bytes(),
        b.model.writes
    );
    let bufs1 = turso_core::io::temp_buffer_cache_counts();
    let (mut commits, mut busy) = (0u64, 0u64);
    if let Some(w) = writers {
        w.stop.store(true, Ordering::Release);
        for h in w.handles {
            let (c, bz) = h.join().expect("a trunk writer panicked");
            commits += c;
            busy += bz;
        }
    }
    println!(
        "# active x={x} trunk_writers={} writer_commits={commits} writer_busy={busy} main_busy_retries={} \
         main_thread_buffer_cache page_before={} page_after={} walframe_before={} walframe_after={} \
         wal_bytes_end={}",
        b.args.trunk_writers,
        BUSY_RETRIES.swap(0, Ordering::Relaxed),
        bufs0.0,
        bufs1.0,
        bufs0.1,
        bufs1.1,
        b.wal_bytes()
    );
}
