//! fastest-linux profiling driver (lane fastest-linux, frontier/fastest): the engine's branch
//! lifecycle — create, connect by name, first write, delete — in a loop at C client threads in
//! one durability class, for the profiling job (perf stat, strace, perf record, callgrind) and
//! the T3 runner. Nothing it prints is credited: it is the subject the instruments measure.
//!
//!   fastest_profile --dir DIR [--class full|fsync|off|async] [--catalog] [--clients C]
//!       [--ops N] [--warmup W] [--rows R] [--mode phases|cycle] [--out DIR]
//!       [--perf-ctl CTL_FIFO,ACK_FIFO] [--mark]
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
//!
//! Windows are fenced for outside instruments, each fence after every client has finished the
//! previous phase and before any starts the next:
//! * `--perf-ctl CTL,ACK`: perf's `--control fifo:CTL,ACK` with `--delay=-1`: counting is enabled
//!   only over the measured windows (never over setup or warm-up), waiting for perf's ack.
//! * `--mark`: a `write(-1, "FASTEST_PHASE <phase> begin|end")` (EBADF) at each fence, so a full
//!   `strace -f` trace can be cut into windows exactly.
//! The per-op phase functions are `#[no_mangle]` (`fastest_phase_create`, `_connect`, `_write`,
//! `_delete`) so callgrind's `--toggle-collect` can count the calling thread's instructions in each.
//!
//! Output (`--out`): `ops.tsv` (client, op, phase, ns) and `summary.json` (per phase: ops,
//! p50/p90/p99/max/mean ns, window seconds, ops/s, and the engine's own sync counter delta over
//! the window). Any failed operation prints `NOT A RESULT` and exits 1; `--class async` exits 4
//! with `NOT AVAILABLE` while the engine has no async-durable class (SyncClass is Off, Fsync,
//! FullFsync at the time of writing), so a caller records the arm as not run instead of
//! measuring something else.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use turso_core::branch::{sync_counts, BranchDurability, SyncClass};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

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
    rows: i64,
    mode: Mode,
    out: Option<PathBuf>,
    perf_ctl: Option<(String, String)>,
    mark: bool,
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
        rows: 1000,
        mode: Mode::Phases,
        out: None,
        perf_ctl: None,
        mark: false,
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
            "--warmup" => a.warmup = num(val(&mut i), "--warmup"),
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
            "--mark" => a.mark = true,
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
    if a.clients == 0 || a.ops == 0 || a.rows < 1 {
        not_a_result("--clients, --ops and --rows must be at least 1");
    }
    a
}

/// Fences for outside instruments: perf's control FIFO and strace-visible markers.
struct Fence {
    perf: Option<(std::fs::File, std::fs::File)>,
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
        Fence { perf, mark: a.mark }
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

    fn begin(&mut self, window: &str) {
        self.marker(&format!("FASTEST_PHASE {window} begin"));
        self.perf("enable");
    }

    fn end(&mut self, window: &str) {
        self.perf("disable");
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

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_create(trunk: &Arc<Connection>, name: &str) {
    if let Err(e) = trunk.create_branch(name) {
        not_a_result(&format!("create {name}: {e}"));
    }
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_connect(db: &Arc<Database>, name: &str) -> Arc<Connection> {
    db.connect_named(name).unwrap_or_else(|e| not_a_result(&format!("connect {name}: {e}")))
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_write(conn: &Arc<Connection>, id: i64, name: &str) {
    if let Err(e) = conn.execute(format!("UPDATE t SET v = 'w-{name}' WHERE id = {id}")) {
        not_a_result(&format!("first write on {name}: {e}"));
    }
}

#[no_mangle]
#[inline(never)]
pub fn fastest_phase_delete(db: &Arc<Database>, name: &str) {
    if let Err(e) = db.drop_branch(name) {
        not_a_result(&format!("delete {name}: {e}"));
    }
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
    cycles("warm", a.warmup, None);
    gate.wait(); // warm-up done
    match a.mode {
        Mode::Cycle => {
            gate.wait();
            cycles("m", a.ops, Some(&mut ns));
            gate.wait();
        }
        Mode::Phases => {
            let mut conns: Vec<Arc<Connection>> = Vec::with_capacity(a.ops);
            for p in 0..4 {
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
        Mode::Phases => PHASES.to_vec(),
        Mode::Cycle => vec!["cycle"],
    };
    let gate = Barrier::new(a.clients + 1);
    let mut windows: Vec<(String, f64, u64, u64)> = Vec::new(); // (window, secs, fsync, full_fsync)
    let per_client: Vec<[Vec<u64>; 4]> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..a.clients)
            .map(|id| {
                let (db, a, gate) = (db.clone(), &a, &gate);
                s.spawn(move || client(id, db, a, gate))
            })
            .collect();
        gate.wait(); // ready
        fence.marker("FASTEST_PHASE warmup begin");
        gate.wait(); // warm-up done
        fence.marker("FASTEST_PHASE warmup end");
        for w in &windows_wanted {
            let s0 = sync_counts();
            fence.begin(w);
            let t = Instant::now();
            gate.wait(); // every client starts the window
            gate.wait(); // every client has finished it
            let secs = t.elapsed().as_secs_f64();
            fence.end(w);
            let s1 = sync_counts();
            windows.push((w.to_string(), secs, s1.fsync - s0.fsync, s1.full_fsync - s0.full_fsync));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| not_a_result("a client thread panicked")))
            .collect()
    });

    // What the run must have left: every branch deleted, the trunk's rows all there and untouched
    // by any branch's first write.
    let live = db.branch_ids().unwrap_or_else(|e| not_a_result(&format!("branch_ids: {e}")));
    if !live.is_empty() {
        not_a_result(&format!("{} branches still live after every delete", live.len()));
    }
    if scalar(&trunk, "SELECT count(*) FROM t") != a.rows
        || scalar(&trunk, "SELECT count(*) FROM t WHERE v LIKE 'w-%'") != 0
    {
        not_a_result("the trunk changed: a branch's write reached it, or rows were lost");
    }

    let ops_total = a.clients * a.ops;
    let mut json = format!(
        "{{\"driver\":\"fastest_profile\",\"class\":\"{}\",\"catalog\":{},\"clients\":{},\"ops_per_client\":{},\"warmup_per_client\":{},\"rows\":{},\"mode\":\"{}\",\"windows\":[",
        a.class_name,
        a.catalog,
        a.clients,
        a.ops,
        a.warmup,
        a.rows,
        if a.mode == Mode::Phases { "phases" } else { "cycle" }
    );
    for (k, (name, secs, fs, ffs)) in windows.iter().enumerate() {
        json.push_str(&format!(
            "{}{{\"window\":\"{name}\",\"secs\":{secs:.6},\"engine_fsync\":{fs},\"engine_full_fsync\":{ffs},\"ops\":{ops_total}}}",
            if k > 0 { "," } else { "" }
        ));
    }
    json.push_str("],\"phases\":{");
    let mut tsv = String::from("client\top\tphase\tns\n");
    for (p, name) in PHASES.iter().enumerate() {
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
