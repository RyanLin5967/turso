//! fastest-linux profiling driver (lane fastest-linux, frontier/fastest): the engine's branch
//! lifecycle — create, connect by name, first write, delete — in a loop at C client threads in
//! one durability class, for the profiling job (perf stat, strace, perf record, callgrind) and
//! the T3 runner. Nothing it prints is credited: it is the subject the instruments measure.
//!
//!   fastest_profile --dir DIR [--class full|fsync|off|async] [--catalog] [--clients C]
//!       [--ops N | --ops-total T] [--warmup W | --warmup OPS:S:MAX_S] [--rows R] [--mode phases|cycle] [--out DIR]
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
//! * `--ops-total T`: exactly T ops for the whole run: T / C per client, plus one for each client whose id is below
//!   T % C; the measured total is recorded beside the asked one (gate-6 review 12: ops means a run total for every
//!   system; third lane review MED 3: ceil(T / C) per client ran up to C - 1 extra).
//! * `--warmup OPS:S:MAX_S`: the warm-up rule in bbload's and clonebench's own form, so one value is passed to every
//!   system (gate-6 review 3; PREREG :210's min(max(1,000 ops, 10 s), 10% of the cap) is 1000:10:180 at the 1800 s
//!   cap, from competitors/timedrun.py rule), decided as bbload decides it (its claim_op; the lead's ruling on fourth
//!   lane review LOW 14, PREREG annex A23): at each claim of a cycle, with `claimed` warm-up cycles claimed before it,
//!   the warm-up ends there when claimed >= OPS and S seconds have passed, or when MAX_S has passed (capped), and that
//!   claim is not a warm-up cycle. Recorded verbatim as `warmup_rule`, with the claim that ended it (its time, the
//!   cycles claimed, capped) and the drain (until every client finished its cycle in flight) apart (third lane review
//!   MED 4). `--warmup W` (default 20) is W cycles per client. `--warmup-replay OPS:S:MAX_S < TRACE` replays the same
//!   decision on claim times (ns since the start, one per line) and prints where it ends, for the shared conformance
//!   test against bbload and clonebench (fastest/linux/gates/warmup_conformance.py).
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
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
    /// the measured ops of each client: `ops` each, or `--ops-total`'s exact split
    ops_of: Vec<usize>,
    warmup: usize,
    /// `--warmup OPS:S:MAX_S`: (ops, seconds, max seconds, the text as given); None for W cycles per client.
    warm_rule: Option<(usize, f64, f64, String)>,
    ops_total: Option<usize>,
    rows: i64,
    mode: Mode,
    out: Option<PathBuf>,
    perf_ctl: Option<(String, String)>,
    perf_only: Option<String>,
    mark: bool,
    phases: usize,
}

/// `--warmup OPS:S:MAX_S` that does not parse (a function, not a closure: one closure has one return type, so
/// reusing it across the usize and f64 parses did not compile, E0308; fourth lane review HIGH 1).
fn bad_warmup(v: &str) -> ! {
    not_a_result(&format!("--warmup OPS:S:MAX_S: {v} (OPS an integer, S and MAX_S finite seconds, MAX_S > 0)"))
}

/// `OPS:S:MAX_S`, as `--warmup` and `--warmup-replay` take it: (ops, seconds, max seconds, the text as given).
fn parse_rule(v: &str) -> (usize, f64, f64, String) {
    let f: Vec<&str> = v.split(':').collect();
    let [o, s, m] = f.as_slice() else { bad_warmup(v) };
    let o: usize = o.parse().unwrap_or_else(|_| bad_warmup(v));
    let s: f64 = s.parse().unwrap_or_else(|_| bad_warmup(v));
    let m: f64 = m.parse().unwrap_or_else(|_| bad_warmup(v));
    if !(s.is_finite() && m.is_finite() && s >= 0.0 && m > 0.0) {
        bad_warmup(v);
    }
    (o, s, m, v.to_string())
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
        ops_of: Vec::new(),
        warmup: 20,
        warm_rule: None,
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
    let mut ops_given = false;
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
            "--ops" => {
                a.ops = num(val(&mut i), "--ops");
                ops_given = true;
            }
            "--ops-total" => a.ops_total = Some(num(val(&mut i), "--ops-total")),
            "--warmup" => {
                let v = val(&mut i);
                // a ':' makes it the rule (two or four fields are refused by parse_rule); none, W cycles per client
                if v.contains(':') {
                    a.warm_rule = Some(parse_rule(&v));
                } else {
                    a.warmup = num(v.clone(), "--warmup");
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
    if ops_given && a.ops_total.is_some() {
        not_a_result("--ops (per client) and --ops-total (the run's total) are exclusive");
    }
    if a.clients == 0 || a.ops == 0 || a.rows < 1 || a.ops_total == Some(0) {
        not_a_result("--clients, --ops, --ops-total and --rows must be at least 1");
    }
    a.ops_of = match a.ops_total {
        Some(t) => (0..a.clients).map(|id| t / a.clients + usize::from(id < t % a.clients)).collect(),
        None => vec![a.ops; a.clients],
    };
    if a.ops_of.contains(&0) {
        not_a_result("--ops-total below --clients leaves a client with no op");
    }
    if let Some(t) = a.ops_total {
        // the split is exactly T (fourth lane review MED 6: a wrong split would otherwise only show downstream)
        let got: usize = a.ops_of.iter().sum();
        if got != t {
            not_a_result(&format!("--ops-total {t} split into {got} ops"));
        }
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

/// The warm-up under `--warmup OPS:S:MAX_S`: its start, the cycles claimed, and, once a claim has ended it,
/// (capped, the cycles claimed before that claim, ns from the start to it). Claims take this lock one at a time, as
/// bbload's claims take its claim lock.
struct Warm {
    t0: Option<Instant>,
    claimed: usize,
    end: Option<(bool, usize, u64)>,
}
static WARM: Mutex<Warm> = Mutex::new(Warm { t0: None, claimed: 0, end: None });

/// bbload's warm-up decision, the definition (artie frontier/fastest/tools/loadgen/bbload.c claim_op at ec9bba5552,
/// factored there as warm_ends; the lead's ruling on fourth lane review LOW 14, PREREG annex A23), made at a claim:
/// with `claimed` warm-up ops claimed before it and `el_ns` since the start, the warm-up ends at this claim when
/// claimed >= ops and el_ns >= s_ns (done), or when el_ns >= max_ns (capped, reported only when not done).
/// None: go on, this claim is a warm-up op. Some(capped): this claim ends the warm-up and is not one.
fn warm_ends(claimed: usize, el_ns: u64, ops: usize, s_ns: u64, max_ns: u64) -> Option<bool> {
    let done = claimed >= ops && el_ns >= s_ns;
    if done || el_ns >= max_ns {
        Some(!done)
    } else {
        None
    }
}

/// Seconds as bbload turns them into nanoseconds, `(uint64_t)(S * 1e9)`: truncated.
fn secs_ns(s: f64) -> u64 {
    (s * 1e9) as u64
}

/// One client's claim of a warm-up cycle under the rule: true to run it (it is counted), false once the warm-up has
/// ended, at this claim or an earlier one.
fn warm_claim(rule: &(usize, f64, f64, String)) -> bool {
    let mut w = WARM.lock().unwrap_or_else(|e| e.into_inner());
    if w.end.is_some() {
        return false;
    }
    let el = w
        .t0
        .map(|t| t.elapsed().as_nanos() as u64)
        .unwrap_or_else(|| not_a_result("a warm-up claim before its start"));
    match warm_ends(w.claimed, el, rule.0, secs_ns(rule.1), secs_ns(rule.2)) {
        None => {
            w.claimed += 1;
            true
        }
        Some(capped) => {
            w.end = Some((capped, w.claimed, el));
            false
        }
    }
}

/// `--warmup-replay OPS:S:MAX_S`: warm_ends replayed on claim times read from stdin (integer ns since the start, one
/// per line, non-decreasing), printing `stop_at=I warm_ops=N capped=0|1`, or `stop_at=none warm_ops=N capped=none`
/// when the trace ends first. Every claim before the stop is a warm-up op, so warm_ops is the stop's index.
fn warmup_replay(rule: &str) -> ! {
    let (ops, s, m, _) = parse_rule(rule);
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text).unwrap_or_else(|e| not_a_result(&format!("stdin: {e}")));
    let (mut claimed, mut prev) = (0usize, 0u64);
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let el: u64 = line.parse().unwrap_or_else(|_| not_a_result(&format!("trace line {line:?}: not integer ns")));
        if el < prev {
            not_a_result(&format!("trace line {line:?}: earlier than the claim before it ({prev})"));
        }
        prev = el;
        if let Some(capped) = warm_ends(claimed, el, ops, secs_ns(s), secs_ns(m)) {
            println!("stop_at={claimed} warm_ops={claimed} capped={}", u8::from(capped));
            std::process::exit(0)
        }
        claimed += 1;
    }
    println!("stop_at=none warm_ops={claimed} capped=none");
    std::process::exit(0)
}

/// Busy/SchemaUpdated retries per phase, over the whole run (warm-up included; the windows'
/// deltas are reported). A client retries them as the engine's own C0 harness does (`retrying`
/// in crash_tests.rs), inside the operation's timing (PREREG: client retries are inside one
/// operation's latency), and gives up at 30 s (PREREG: a failed operation).
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
    let n = a.ops_of[id];
    gate.wait(); // ready
    if let Some(rule) = &a.warm_rule {
        let mut k = 0usize;
        while warm_claim(rule) {
            cycles(&format!("warm{k}"), 1, None);
            k += 1;
        }
    } else {
        cycles("warm", a.warmup, None);
    }
    gate.wait(); // warm-up done
    match a.mode {
        Mode::Cycle => {
            gate.wait();
            cycles("m", n, Some(&mut ns));
            gate.wait();
        }
        Mode::Phases => {
            let mut conns: Vec<Arc<Connection>> = Vec::with_capacity(n);
            for p in 0..a.phases {
                gate.wait();
                for i in 0..n {
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
                    if p == 2 && i + 1 == n {
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
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--warmup-replay") {
        match argv.get(2) {
            Some(rule) if argv.len() == 3 => warmup_replay(rule),
            _ => not_a_result("--warmup-replay OPS:S:MAX_S < TRACE"),
        }
    }
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
    // seconds from the warm-up's start to every client done (under a rule, the claim that ended it is in WARM)
    let mut warm_secs = 0.0f64;
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
        // the warm-up's clock starts before the clients are let go, so no claim precedes it; the clients decide its
        // end at their claims (warm_claim), as bbload's do
        let t = Instant::now();
        WARM.lock().unwrap_or_else(|e| e.into_inner()).t0 = Some(t);
        gate.wait(); // ready
        fence.marker("FASTEST_PHASE warmup begin");
        gate.wait(); // warm-up done
        warm_secs = t.elapsed().as_secs_f64();
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

    let ops_total: usize = a.ops_of.iter().sum();
    let (rule_text, warm_rule) = match &a.warm_rule {
        // the claim that ended it (its cycles claimed, its time, capped), the cycles over the whole warm-up (read after
        // every client finished: the same count, as no claim after the end is counted), and the drain apart
        Some((_, _, _, text)) => {
            let w = WARM.lock().unwrap_or_else(|e| e.into_inner());
            let (capped, at_stop, stop_ns) = w.end.unwrap_or_else(|| not_a_result("the warm-up never ended"));
            let stop = stop_ns as f64 / 1e9;
            (
                text.clone(),
                format!(
                    "{{\"rule\":\"{text}\",\"ops_at_stop\":{at_stop},\"ops\":{},\"stop_secs\":{stop:.3},\"drain_secs\":{:.3},\"secs\":{:.3},\"capped\":{capped}}}",
                    w.claimed,
                    warm_secs - stop,
                    warm_secs
                ),
            )
        }
        None => (
            format!("cycles_per_client:{}", a.warmup),
            format!(
                "{{\"rule\":\"cycles_per_client:{}\",\"ops\":{},\"secs\":{:.3}}}",
                a.warmup,
                a.clients * a.warmup,
                warm_secs
            ),
        ),
    };
    let even = a.ops_of.iter().all(|&n| n == a.ops_of[0]);
    let by_client: Vec<String> = a.ops_of.iter().map(|n| n.to_string()).collect();
    let mut json = format!(
        "{{\"driver\":\"fastest_profile\",\"class\":\"{}\",\"catalog\":{},\"clients\":{},\"ops_per_client\":{},\"ops_by_client\":[{}],\"ops_total\":{ops_total},\"ops_total_asked\":{},\"warmup_per_client\":{},\"warmup_rule\":\"{rule_text}\",\"warmup\":{warm_rule},\"rows\":{},\"phases_run\":{},\"mode\":\"{}\",\"windows\":[",
        a.class_name,
        a.catalog,
        a.clients,
        if even { a.ops_of[0].to_string() } else { "null".into() },
        by_client.join(","),
        a.ops_total.map(|t| t.to_string()).unwrap_or_else(|| "null".into()),
        if a.warm_rule.is_some() { "null".into() } else { a.warmup.to_string() },
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
