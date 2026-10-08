//! fastest-linux profiling driver (lane fastest-linux, frontier/fastest): the engine's branch
//! lifecycle — create, connect by name, first write, delete — in a loop at C client threads in
//! one durability class, for the profiling job (perf stat, strace, perf record, callgrind) and
//! the T3 runner. Nothing it prints is credited: it is the subject the instruments measure.
//!
//!   fastest_profile --dir DIR [--class full|fsync|off|async] [--catalog] [--clients C]
//!       [--ops N | --ops-total T] [--warmup W | --warmup prereg:CAP_S] [--rows R] [--mode phases|cycle] [--out DIR]
//!       [--perf-ctl CTL_FIFO,ACK_FIFO [--perf-only WINDOW]] [--mark] [--phases K]
//!
//! One op per client is the cycle: `trunk.create_branch(name)` (create), `db.connect_named(name)`
//! (connect), one autocommit UPDATE of a random existing row of `t` (first write: it copies a
//! shared page), then the connection is closed and `db.drop_branch(name)` (delete).
//!
//! * `--mode phases` (default): every client does its N creates, then (after a barrier) its N
//!   connects, then its N first writes, then its N deletes, so each phase is one window of the
//!   whole process that an outside instrument can count over. Its connections live from the
//!   connect phase to the delete phase (C x N of them at once).
//! * `--mode cycle`: every client loops whole cycles; one window for the four phases together.
//! * `--ops-total T`: T ops for the whole run, ceil(T / C) per client; the measured total is recorded (gate-6
//!   review 12: ops means a run total for every system).
//! * `--warmup prereg:CAP_S`: PREREG's warm-up rule, the same for every system (gate-6 review 3): cycles run until
//!   at least 1,000 warm-up ops across all clients AND 10 s have passed, or until 10% of the run's cap CAP_S, whichever
//!   comes first; the ops and seconds it took are recorded. `--warmup W` (default 20) is W cycles per client.
//! * `--phases K` (phases mode, 1-4, default 4): only the first K phases, so an instruction counter can
//!   take a phase's cost as the difference between runs (callgrind's per-function inclusive cost is not
//!   trustworthy where it reports false recursion: arm64, run 37255309860). With K < 4 the branches are
//!   left live and the every-branch-deleted check is skipped (and says so).
//!
//! Windows are fenced for outside instruments, each fence after every client has finished the
//! previous phase and before any starts the next:
//! * `--perf-ctl CTL,ACK`: perf's `--control fifo:CTL,ACK` with `--delay=-1`: counting is enabled
//!   only over the measured windows (never over setup or warm-up), waiting for perf's ack; with
//!   `--perf-only W`, over window W alone (`create`, `connect`, `write`, `delete` or `cycle`).
//! * `--mark`: a `write(-1, "FASTEST_PHASE <phase> begin|end")` (EBADF) at each fence, so a full
//!   `strace -f` trace can be cut into windows exactly.
//! The per-op phase functions are `#[no_mangle]` (`fastest_phase_create`, `_connect`, `_write`,
//! `_delete`) so callgrind's `--toggle-collect` can count the calling thread's instructions in each.
//!
//! Fire-check plant (inert unless set): `FASTEST_PROFILE_PLANT=ir:N,syscall:M,fsync:K` makes every
//! create also spin N iterations of a black-boxed loop, make M getppid(2) calls and K fsync(2)s of a
//! scratch file, so the profiling job can show its instruction, syscall and flush gates fail on a
//! real run (workflow input `plant`, head side only).
//!
//! Output (`--out`): `ops.tsv` (client, op, phase, ns) and `summary.json` (per phase: ops,
//! p50/p90/p99/max/mean ns, window seconds, ops/s, and the engine's own sync counter delta over
//! the window). Any failed operation prints `NOT A RESULT` and exits 1; `--class async` exits 4
//! with `NOT AVAILABLE` while the engine has no async-durable class (SyncClass is Off, Fsync,
//! FullFsync at the time of writing), so a caller records the arm as not run instead of
//! measuring something else.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use turso_core::branch::{sync_counts, BranchDurability, SyncClass};
use turso_core::{
    Connection, Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, SqliteDialect, IO,
};

const PHASES: [&str; 4] = ["create", "connect", "write", "delete"];

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Phases,
    Cycle,
}

struct Args {
    dir: PathBuf,
    class: SyncClass,
    class_name: String,
    catalog: bool,
    clients: usize,
    ops: usize,
    warmup: usize,
    /// `--warmup prereg:CAP_S`: the run's cap in seconds; None for a fixed W cycles per client.
    warmup_cap_s: Option<f64>,
    ops_total: Option<usize>,
    rows: i64,
    mode: Mode,
    out: Option<PathBuf>,
    perf_ctl: Option<(String, String)>,
    perf_only: Option<String>,
    mark: bool,
    phases: usize,
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut a = Args {
        dir: PathBuf::new(),
        class: SyncClass::FullFsync,
        class_name: "full".into(),
        catalog: false,
        clients: 1,
        ops: 200,
        warmup: 20,
        warmup_cap_s: None,
        ops_total: None,
        rows: 1000,
        mode: Mode::Phases,
        out: None,
        perf_ctl: None,
        perf_only: None,
        mark: false,
        phases: 4,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    let val = |i: &mut usize| -> String {
        *i += 1;
        argv.get(*i).cloned().unwrap_or_else(|| not_a_result(&format!("{} needs a value", argv[*i - 1])))
    };
    let num = |s: String, what: &str| -> usize {
        s.parse().unwrap_or_else(|_| not_a_result(&format!("{what}: not a number: {s}")))
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "--dir" => a.dir = PathBuf::from(val(&mut i)),
            "--class" => {
                a.class_name = val(&mut i);
                a.class = match a.class_name.as_str() {
                    "full" => SyncClass::FullFsync,
                    "fsync" => SyncClass::Fsync,
                    "off" => SyncClass::Off,
                    "async" => {
                        println!(
                            "NOT AVAILABLE: the engine at this sha has no async-durable class \
                             (SyncClass: Off, Fsync, FullFsync)"
                        );
                        std::process::exit(4)
                    }
                    c => not_a_result(&format!("unknown class {c}")),
                }
            }
            "--catalog" => a.catalog = true,
            "--clients" => a.clients = num(val(&mut i), "--clients"),
            "--ops" => a.ops = num(val(&mut i), "--ops"),
            "--ops-total" => a.ops_total = Some(num(val(&mut i), "--ops-total")),
            "--warmup" => {
                let v = val(&mut i);
                match v.strip_prefix("prereg:") {
                    Some(cap) => {
                        let c: f64 = cap.parse().unwrap_or_else(|_| not_a_result(&format!("--warmup prereg:CAP_S: {cap}")));
                        if !(c > 0.0) {
                            not_a_result("--warmup prereg:CAP_S needs a positive cap");
                        }
                        a.warmup_cap_s = Some(c);
                    }
                    None => a.warmup = num(v, "--warmup"),
                }
            }
            "--rows" => a.rows = num(val(&mut i), "--rows") as i64,
            "--mode" => {
                a.mode = match val(&mut i).as_str() {
                    "phases" => Mode::Phases,
                    "cycle" => Mode::Cycle,
                    m => not_a_result(&format!("unknown mode {m}")),
                }
            }
            "--out" => a.out = Some(PathBuf::from(val(&mut i))),
            "--perf-ctl" => {
                let v = val(&mut i);
                let (c, k) = v.split_once(',').unwrap_or_else(|| not_a_result("--perf-ctl CTL,ACK"));
                a.perf_ctl = Some((c.to_string(), k.to_string()));
            }
            "--perf-only" => a.perf_only = Some(val(&mut i)),
            "--mark" => a.mark = true,
            "--phases" => a.phases = num(val(&mut i), "--phases"),
            x => not_a_result(&format!("unknown argument {x}")),
        }
        i += 1;
    }
    if a.dir.as_os_str().is_empty() {
        not_a_result("--dir is required");
    }
    if a.dir.exists() {
        not_a_result(&format!("{} exists: every run starts from a fresh directory", a.dir.display()));
    }
    if let Some(w) = &a.perf_only {
        let known = PHASES.contains(&w.as_str()) || w == "cycle";
        if a.perf_ctl.is_none() || !known {
            not_a_result(&format!("--perf-only {w}: needs --perf-ctl and a window name"));
        }
    }
    if !(1..=4).contains(&a.phases) || (a.phases < 4 && a.mode == Mode::Cycle) {
        not_a_result("--phases is 1-4, and below 4 only in phases mode");
    }
    if a.clients == 0 || a.ops == 0 || a.rows < 1 || a.ops_total == Some(0) {
        not_a_result("--clients, --ops, --ops-total and --rows must be at least 1");
    }
    if let Some(t) = a.ops_total {
        a.ops = t.div_ceil(a.clients);
    }
    a
}

/// Fences for outside instruments: perf's control FIFO and strace-visible markers.
struct Fence {
    perf: Option<(std::fs::File, std::fs::File)>,
    perf_only: Option<String>,
    mark: bool,
}

impl Fence {
    fn open(a: &Args) -> Fence {
        let perf = a.perf_ctl.as_ref().map(|(ctl, ack)| {
            let c = std::fs::OpenOptions::new()
                .write(true)
                .open(ctl)
                .unwrap_or_else(|e| not_a_result(&format!("perf ctl fifo {ctl}: {e}")));
            let k = std::fs::File::open(ack).unwrap_or_else(|e| not_a_result(&format!("perf ack fifo {ack}: {e}")));
            (c, k)
        });
        Fence { perf, perf_only: a.perf_only.clone(), mark: a.mark }
    }

    fn perf(&mut self, cmd: &str) {
        if let Some((ctl, ack)) = self.perf.as_mut() {
            ctl.write_all(format!("{cmd}\n").as_bytes())
                .unwrap_or_else(|e| not_a_result(&format!("perf ctl write: {e}")));
            let mut buf = [0u8; 5];
            ack.read_exact(&mut buf).unwrap_or_else(|e| not_a_result(&format!("perf ack read: {e}")));
            if &buf != b"ack\n\0" && !buf.starts_with(b"ack") {
                not_a_result(&format!("perf answered {:?} to {cmd}", String::from_utf8_lossy(&buf)));
            }
        }
    }

    fn marker(&self, text: &str) {
        if self.mark {
            // SAFETY: fd -1 is never open; the call only fails (EBADF), which strace records with
            // its buffer, and reads at most text.len() bytes of a live slice.
            unsafe { libc::write(-1, text.as_ptr().cast(), text.len()) };
        }
    }

    fn counts(&self, window: &str) -> bool {
        self.perf_only.as_deref().is_none_or(|w| w == window)
    }

    fn begin(&mut self, window: &str) {
        self.marker(&format!("FASTEST_PHASE {window} begin"));
        if self.counts(window) {
            self.perf("enable");
        }
    }

    fn end(&mut self, window: &str) {
        if self.counts(window) {
            self.perf("disable");
        }
        self.marker(&format!("FASTEST_PHASE {window} end"));
    }
}

fn open_db(path: &Path, a: &Args) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap_or_else(|e| not_a_result(&format!("io: {e}"))));
    let durability = if a.catalog {
        BranchDurability::Catalog { sync: a.class }
    } else {
        BranchDurability::Durable { sync: a.class }
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

/// A small deterministic generator (xorshift64*), seeded per client.
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n.max(1)
    }
}

/// Busy/SchemaUpdated retries per phase, over the whole run (warm-up included; the windows'
/// deltas are reported). A client retries them as the engine's own C0 harness does (`retrying`
/// in crash_tests.rs), inside the operation's timing (PREREG: client retries are inside one
/// operation's latency), and gives up at 30 s (PREREG: a failed operation).
/// PREREG warm-up (`--warmup prereg:CAP_S`): cycles done by every client, and the main thread's stop.
static WARM_OPS: AtomicUsize = AtomicUsize::new(0);
static WARM_STOP: AtomicBool = AtomicBool::new(false);

static RETRIES: [AtomicU64; 4] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

fn retry<T>(phase: usize, what: &str, mut f: impl FnMut() -> turso_core::Result<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match f() {
            Ok(v) => return v,
            Err(LimboError::Busy | LimboError::BusySnapshot | LimboError::SchemaUpdated) if Instant::now() < deadline => {
                RETRIES[phase].fetch_add(1, Ordering::Relaxed);
                std::thread::yield_now();
            }
            Err(e) => not_a_result(&format!("{what}: {e}")),
        }
    }
}

/// The fire-check plant, read once: (spin iterations, getppid calls, fsyncs) added to every create.
fn plant() -> (u64, u64, u64) {
    static PLANT: std::sync::OnceLock<(u64, u64, u64)> = std::sync::OnceLock::new();
    *PLANT.get_or_init(|| {
        let mut p = (0, 0, 0);
        for kv in std::env::var("FASTEST_PROFILE_PLANT").unwrap_or_default().split(',').filter(|s| !s.is_empty()) {
            match kv.split_once(':').map(|(k, v)| (k, v.parse::<u64>())) {
                Some(("ir", Ok(n))) => p.0 = n,
                Some(("syscall", Ok(n))) => p.1 = n,
                Some(("fsync", Ok(n))) => p.2 = n,
                _ => not_a_result(&format!("FASTEST_PROFILE_PLANT: bad item {kv}")),
            }
        }
        if p != (0, 0, 0) {
            eprintln!(
                "note: FASTEST_PROFILE_PLANT active: {} spin iterations, {} getppid and {} fsync per create",
                p.0, p.1, p.2
            );
        }
        p
    })
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_create(trunk: &Arc<Connection>, name: &str) {
    retry(0, &format!("create {name}"), || trunk.create_branch(name));
    let (spin, calls, syncs) = plant();
    let mut x = 0u64;
    for i in 0..spin {
        x = std::hint::black_box(x.wrapping_add(i));
    }
    std::hint::black_box(x);
    for _ in 0..calls {
        // SAFETY: getppid has no preconditions.
        unsafe { libc::getppid() };
    }
    if syncs > 0 {
        static SCRATCH: std::sync::OnceLock<std::fs::File> = std::sync::OnceLock::new();
        let f = SCRATCH.get_or_init(|| {
            let path = std::env::temp_dir().join(format!("fastest-plant-{}", std::process::id()));
            std::fs::File::create(&path).unwrap_or_else(|e| not_a_result(&format!("plant scratch file: {e}")))
        });
        for _ in 0..syncs {
            f.sync_all().unwrap_or_else(|e| not_a_result(&format!("plant fsync: {e}")));
        }
    }
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_connect(db: &Arc<Database>, name: &str) -> Arc<Connection> {
    retry(1, &format!("connect {name}"), || db.connect_named(name))
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_write(conn: &Arc<Connection>, id: i64, name: &str) {
    retry(2, &format!("first write on {name}"), || {
        conn.execute(format!("UPDATE t SET v = 'w-{name}' WHERE id = {id}"))
    });
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_delete(db: &Arc<Database>, name: &str) {
    retry(3, &format!("delete {name}"), || db.drop_branch(name));
}

/// One client thread's run: its own trunk connection (connections stay on the thread that made
/// them), the windows in lock-step with the main thread through `gate` (each window is entered and
/// left by every client and the main thread together), and its timings, `ns[phase]` in op order.
fn client(id: usize, db: Arc<Database>, a: &Args, gate: &Barrier) -> [Vec<u64>; 4] {
    let trunk = db.connect().unwrap_or_else(|e| not_a_result(&format!("connect: {e}")));
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (id as u64 + 1));
    let mut ns: [Vec<u64>; 4] = Default::default();
    let name = |tag: &str, i: usize| format!("{tag}-c{id}-{i}");
    let mut cycles = |tag: &str, n: usize, ns: Option<&mut [Vec<u64>; 4]>| {
        let mut ns = ns;
        for i in 0..n {
            let name = name(tag, i);
            let mut d = [0u64; 4];
            let t = Instant::now();
            fastest_phase_create(&trunk, &name);
            d[0] = t.elapsed().as_nanos() as u64;
            let t = Instant::now();
            let conn = fastest_phase_connect(&db, &name);
            d[1] = t.elapsed().as_nanos() as u64;
            let row = 1 + rng.below(a.rows as u64) as i64;
            let t = Instant::now();
            fastest_phase_write(&conn, row, &name);
            d[2] = t.elapsed().as_nanos() as u64;
            drop(conn);
            let t = Instant::now();
            fastest_phase_delete(&db, &name);
            d[3] = t.elapsed().as_nanos() as u64;
            if let Some(ns) = ns.as_deref_mut() {
                for p in 0..4 {
                    ns[p].push(d[p]);
                }
            }
        }
    };
    gate.wait(); // ready
    if a.warmup_cap_s.is_some() {
        let mut k = 0usize;
        while !WARM_STOP.load(Ordering::Acquire) {
            cycles(&format!("warm{k}"), 1, None);
            WARM_OPS.fetch_add(1, Ordering::AcqRel);
            k += 1;
        }
    } else {
        cycles("warm", a.warmup, None);
    }
    gate.wait(); // warm-up done
    match a.mode {
        Mode::Cycle => {
            gate.wait();
            cycles("m", a.ops, Some(&mut ns));
            gate.wait();
        }
        Mode::Phases => {
            let mut conns: Vec<Arc<Connection>> = Vec::with_capacity(a.ops);
            for p in 0..a.phases {
                gate.wait();
                for i in 0..a.ops {
                    let name = name("m", i);
                    let t = Instant::now();
                    match p {
                        0 => fastest_phase_create(&trunk, &name),
                        1 => conns.push(fastest_phase_connect(&db, &name)),
                        2 => {
                            let row = 1 + rng.below(a.rows as u64) as i64;
                            fastest_phase_write(&conns[i], row, &name);
                        }
                        _ => fastest_phase_delete(&db, &name),
                    }
                    ns[p].push(t.elapsed().as_nanos() as u64);
                    if p == 2 && i + 1 == a.ops {
                        // Every branch connection closes before the delete window opens.
                        conns.clear();
                    }
                }
                gate.wait();
            }
        }
    }
    ns
}

fn seed(trunk: &Arc<Connection>, rows: i64) {
    let ok = trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").is_ok()
        && trunk.execute("BEGIN").is_ok()
        && (1..=rows).all(|id| {
            trunk.execute(format!("INSERT INTO t VALUES ({id}, 'trunk-{id}-{}')", "x".repeat(80))).is_ok()
        })
        && trunk.execute("COMMIT").is_ok();
    if !ok {
        not_a_result("seeding the trunk failed");
    }
}

/// One integer from a one-row, one-column query.
fn scalar(conn: &Arc<Connection>, sql: &str) -> i64 {
    let rows = conn
        .prepare(sql)
        .and_then(|mut s| s.run_collect_rows())
        .unwrap_or_else(|e| not_a_result(&format!("{sql}: {e}")));
    rows.first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_int())
        .unwrap_or_else(|| not_a_result(&format!("{sql}: no integer")))
}

/// The engine's in-process sync counters (`turso_core::branch::sync_counts`), every field by name,
/// read from the struct's Debug form so the driver builds against engine shas whose `SyncCounts`
/// has a different field set (88dfe324f: fsync, full_fsync; later: + barrier).
fn engine_syncs() -> Vec<(String, u64)> {
    let text = format!("{:?}", sync_counts());
    let body = text.split_once('{').map(|(_, b)| b).unwrap_or("").trim_end_matches('}');
    body.split(',')
        .filter_map(|kv| {
            let (k, v) = kv.split_once(':')?;
            Some((k.trim().to_string(), v.trim().parse().ok()?))
        })
        .collect()
}

fn pct(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn main() {
    let a = parse_args();
    std::fs::create_dir_all(&a.dir).unwrap_or_else(|e| not_a_result(&format!("mkdir: {e}")));
    let db = open_db(&a.dir.join("profile.db"), &a);
    let trunk = db.connect().unwrap_or_else(|e| not_a_result(&format!("connect: {e}")));
    seed(&trunk, a.rows);
    // The store's files are made by the first fork: one unmeasured create and delete.
    fastest_phase_create(&trunk, "setup-0");
    fastest_phase_delete(&db, "setup-0");
    let mut fence = Fence::open(&a);

    let windows_wanted: Vec<&str> = match a.mode {
        Mode::Phases => PHASES[..a.phases].to_vec(),
        Mode::Cycle => vec!["cycle"],
    };
    let gate = Barrier::new(a.clients + 1);
    let warm_secs = std::sync::Mutex::new(0.0f64);
    // (window, secs, engine sync counter deltas by field, busy retries in the window, all phases)
    let mut windows: Vec<(String, f64, Vec<(String, u64)>, u64)> = Vec::new();
    let retries = || RETRIES.iter().map(|r| r.load(Ordering::Relaxed)).sum::<u64>();
    let per_client: Vec<[Vec<u64>; 4]> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..a.clients)
            .map(|id| {
                let (db, a, gate) = (db.clone(), &a, &gate);
                s.spawn(move || client(id, db, a, gate))
            })
            .collect();
        gate.wait(); // ready
        fence.marker("FASTEST_PHASE warmup begin");
        if let Some(cap) = a.warmup_cap_s {
            // min(max(1,000 ops, 10 s), 10% of the cap): both minimums, unless the cap's share ends it first
            let t = Instant::now();
            loop {
                let secs = t.elapsed().as_secs_f64();
                if (WARM_OPS.load(Ordering::Acquire) >= 1000 && secs >= 10.0) || secs >= 0.1 * cap {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            WARM_STOP.store(true, Ordering::Release);
            *warm_secs.lock().unwrap() = t.elapsed().as_secs_f64();
        }
        gate.wait(); // warm-up done
        fence.marker("FASTEST_PHASE warmup end");
        for w in &windows_wanted {
            let s0 = engine_syncs();
            let r0 = retries();
            fence.begin(w);
            let t = Instant::now();
            gate.wait(); // every client starts the window
            gate.wait(); // every client has finished it
            let secs = t.elapsed().as_secs_f64();
            fence.end(w);
            let s1 = engine_syncs();
            if s1.is_empty() || s1.len() != s0.len() {
                not_a_result(&format!("engine sync counters unreadable: {:?}", sync_counts()));
            }
            let delta = s1.iter().zip(&s0).map(|((k, b), (_, a))| (k.clone(), b - a)).collect();
            windows.push((w.to_string(), secs, delta, retries() - r0));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| not_a_result("a client thread panicked")))
            .collect()
    });

    // What the run must have left: every branch deleted, the trunk's rows all there and untouched
    // by any branch's first write.
    let live = db.branch_ids().unwrap_or_else(|e| not_a_result(&format!("branch_ids: {e}")));
    if a.phases == 4 && !live.is_empty() {
        not_a_result(&format!("{} branches still live after every delete", live.len()));
    }
    if a.phases < 4 {
        eprintln!("note: --phases {}: {} branches left live by design; the delete check was skipped", a.phases, live.len());
    }
    if scalar(&trunk, "SELECT count(*) FROM t") != a.rows
        || scalar(&trunk, "SELECT count(*) FROM t WHERE v LIKE 'w-%'") != 0
    {
        not_a_result("the trunk changed: a branch's write reached it, or rows were lost");
    }

    let ops_total = a.clients * a.ops;
    let warm_rule = match a.warmup_cap_s {
        Some(cap) => format!(
            "{{\"rule\":\"prereg\",\"cap_s\":{cap},\"ops\":{},\"secs\":{:.3}}}",
            WARM_OPS.load(Ordering::Acquire),
            *warm_secs.lock().unwrap()
        ),
        None => format!("{{\"rule\":\"cycles_per_client\",\"ops\":{}}}", a.clients * a.warmup),
    };
    let mut json = format!(
        "{{\"driver\":\"fastest_profile\",\"class\":\"{}\",\"catalog\":{},\"clients\":{},\"ops_per_client\":{},\"ops_total\":{ops_total},\"ops_total_asked\":{},\"warmup_per_client\":{},\"warmup\":{warm_rule},\"rows\":{},\"phases_run\":{},\"mode\":\"{}\",\"windows\":[",
        a.class_name,
        a.catalog,
        a.clients,
        a.ops,
        a.ops_total.map(|t| t.to_string()).unwrap_or_else(|| "null".into()),
        a.warmup,
        a.rows,
        a.phases,
        if a.mode == Mode::Phases { "phases" } else { "cycle" }
    );
    for (k, (name, secs, syncs, busy)) in windows.iter().enumerate() {
        let fields: Vec<String> = syncs.iter().map(|(f, v)| format!("\"{f}\":{v}")).collect();
        let total: u64 = syncs.iter().map(|(_, v)| v).sum();
        json.push_str(&format!(
            "{}{{\"window\":\"{name}\",\"secs\":{secs:.6},\"engine_syncs\":{{{}}},\"engine_syncs_total\":{total},\"busy_retries\":{busy},\"ops\":{ops_total}}}",
            if k > 0 { "," } else { "" },
            fields.join(",")
        ));
    }
    json.push_str("],\"phases\":{");
    let mut tsv = String::from("client\top\tphase\tns\n");
    let reported = if a.mode == Mode::Cycle { 4 } else { a.phases };
    for (p, name) in PHASES[..reported].iter().enumerate() {
        let mut all: Vec<u64> = Vec::with_capacity(ops_total);
        for (c, ns) in per_client.iter().enumerate() {
            for (i, v) in ns[p].iter().enumerate() {
                tsv.push_str(&format!("{c}\t{i}\t{name}\t{v}\n"));
                all.push(*v);
            }
        }
        if all.len() != ops_total {
            not_a_result(&format!("phase {name}: {} timings for {ops_total} ops", all.len()));
        }
        all.sort_unstable();
        let mean = all.iter().sum::<u64>() as f64 / all.len() as f64;
        json.push_str(&format!(
            "{}\"{name}\":{{\"ops\":{},\"p50_ns\":{},\"p90_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"mean_ns\":{mean:.0}}}",
            if p > 0 { "," } else { "" },
            all.len(),
            pct(&all, 0.5),
            pct(&all, 0.9),
            pct(&all, 0.99),
            all[all.len() - 1]
        ));
    }
    json.push_str("}}\n");
    print!("{json}");
    if let Some(out) = &a.out {
        std::fs::create_dir_all(out).unwrap_or_else(|e| not_a_result(&format!("mkdir out: {e}")));
        std::fs::write(out.join("summary.json"), &json).unwrap_or_else(|e| not_a_result(&format!("write: {e}")));
        std::fs::write(out.join("ops.tsv"), &tsv).unwrap_or_else(|e| not_a_result(&format!("write: {e}")));
    }
}
