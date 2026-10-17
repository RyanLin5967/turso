//! The server's branch cycle run embedded, for the wire-versus-embedded budgets (fastest-wire M3):
//! the same engine calls `tursopg --server` makes for each statement of the cycle, on a database
//! opened with the server's own options, with each step's start and end on the clock the V1 and
//! C1b tracers stamp (CLOCK_UPTIME_RAW), so their events can be attributed to steps.
//!
//!   embedded_ops --db PATH [--durability full|fsync|off] [--store catalog|snapshot]
//!                [--rows N] [--warmup N] [--ops N] [--siblings K] [--plant extra|extra-write]
//!                [--name-prefix P] [--pregrow N] [--stepwise] --out FILE
//!
//! `--stepwise` runs one cycle per line read from stdin (`cycle <seq>`), answering `done <seq>` on
//! stdout after it, and says `ready` once the seed is in: the work budget drives the embedded and wire
//! sides cycle by cycle inside one run (lead's pre-registered amendment, 2026-10-15), as their flush
//! steps' cost drifted between two runs one after the other.
//!
//! `--name-prefix P` (default `b_`) names op k's branch `P<k>`: the budget passes the prefix the wire
//! side's spec names its branches with (`b_{run}_{c}_{i}`, so `b_w_0_` for run tag w and one
//! client), as the bytes the two sides write per step include the name (wire review 11 item 17:
//! `b_<k>` against `b_w_0_<k>` was 4 bytes a create, which the bytes rule then read as a cost).
//!
//! `--pregrow N` creates and drops N branches (`pregrow_<i>`) after the siblings and waits for every
//! other thread to go quiet, as budget.py's wire seed does with the same N: the branch name filter
//! grows on its own thread at every power of two of names noted (512, 1024, 2048 ... past its
//! first 1024-slot table half full), and growth inside a measured window refuses the work verdict
//! (DECISIONS 2923cc91b5 (a)), so a run registers an N that leaves no growth in its cycles.
//!
//! Every row also carries the thread starts and exits inside its window and, in `gap_*`, the
//! foreign CPU, starts and exits between the previous row's end and this row's start (the time
//! between windows, the stdin wait between cycles included: DECISIONS 2923cc91b5 (d)). Differences
//! are signed (a foreign CPU sum may fall when a thread's end is accounted late), and the thread
//! columns are `NA` in a build that did not install the counter (budget.py refuses it).
//!
//! `--siblings K` creates K live branches before the ops, so every measured create forks a trunk
//! that already has children (the path credited cells take; wire review 1 item 18). `--plant extra`
//! (a budget fire-check) also commits a trunk row inside each create step, as the wire side's
//! `--plant both` does, so the two sides stay equal and only the budget's absolute counts can fail.
//! `--plant extra-write` commits a second row on the branch inside each write step, as the wire
//! side's `--plant both-write` does: the write step's registered absolute counts must catch it
//! (wire review 15 item 10).
//!
//! The cycle, per op k (the statements the wire side sends, and the engine call each maps to):
//!   create  SELECT turso_branch_create('b_k')   Connection::create_branch on the trunk connection
//!   switch  SELECT turso_branch_switch('b_k')   Database::connect_named
//!   write   UPDATE t SET v = v + 1 WHERE id = r  through the frontend, on the branch
//!   main    SELECT turso_branch_switch('main')  the branch connection dropped
//!   delete  SELECT turso_branch_delete('b_k')   Database::drop_branch
//! Before the ops it creates `t(id INT PRIMARY KEY, v INT)` with `--rows` rows and an empty `t2(id
//! INT)`, as the wire side's seed does through the server.
//!
//! Output: TSV `seq phase step start_ns end_ns cpu_ns unix_syscalls instructions branch`, phase
//! `warmup` or `measure`, `branch` the op's branch name (so budget.py can check, from the raw, that
//! the two sides named their branches alike: wire review 15 item 29); `cpu_ns` is the calling thread's CPU time across the step
//! (CLOCK_THREAD_CPUTIME_ID), and the last two are the process's unix system calls and
//! instructions retired across it (turso_pg_server::counters, 0 where not counted). Every step is
//! preceded by a `noop` window of its own, two probes and nothing between, as every wire step follows
//! an empty query of its own: each side's step is paired with its own adjacent floor (the
//! pre-registered re-judgment's symmetric pairing, DECISIONS 06de6fd608; one noop per cycle sat
//! four steps from the delete).

use std::io::Write;
use turso_core::branch::{BranchDurability, SyncClass};
use turso_pg::PgConnection;

#[cfg(target_vendor = "apple")]
extern "C" {
    fn clock_gettime_nsec_np(clock_id: libc::clockid_t) -> u64;
}

fn now_ns() -> u64 {
    #[cfg(target_vendor = "apple")]
    unsafe {
        clock_gettime_nsec_np(libc::CLOCK_UPTIME_RAW)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
}

/// This thread's CPU time, ns.
fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// One reading of the step clocks and counters.
#[derive(Clone, Copy)]
struct Probe {
    ns: u64,
    cpu_ns: u64,
    counters: Option<turso_pg_server::counters::ProcessCounters>,
    allocations: Option<u64>,
    /// This thread's own instructions (the driver runs every step on it).
    thread_instructions: u64,
    /// The branch store's confirmation words written (its background writer's work).
    confirms: u64,
    /// This thread's own allocations (counters::thread_allocations; 0 without alloc-count).
    thread_allocations: u64,
    /// Every other thread's CPU so far, read last (counters::foreign_thread_cpu_ns; 0 where not
    /// counted), as turso_branch_work() reads it.
    foreign_cpu_ns: u64,
    /// Threads started and exited so far (counters::thread_lifecycle; None without the counter).
    lifecycle: Option<(u64, u64)>,
}

/// `b - a` signed, for counters read as u64 (a sum that fell reads negative, never as a wrap).
fn signed(a: u64, b: u64) -> i128 {
    b as i128 - a as i128
}

/// The lifecycle difference of two probes, as two TSV fields (`NA` without the counter).
fn lifecycle_fields(a: &Probe, b: &Probe) -> String {
    match (a.lifecycle, b.lifecycle) {
        (Some(x), Some(y)) => format!("{}\t{}", signed(x.0, y.0), signed(x.1, y.1)),
        _ => "NA\tNA".to_string(),
    }
}

impl Probe {
    fn now(db: &turso_core::Database) -> Self {
        // The counters in turso_branch_work()'s order (process counters, allocations, thread
        // counters, confirms), then the clocks: the work budget's syscall coverage check
        // (logged = kernel + 1 + getpid per window, lead 2026-10-15) holds the two sides to one
        // reading sequence around the kernel's count.
        let counters = turso_pg_server::counters::process_counters();
        let allocations = turso_pg_server::counters::allocations();
        let thread_instructions = turso_pg_server::counters::thread_counters().map_or(0, |c| c.0);
        let confirms = db.branch_confirm_counts()[0];
        let thread_allocations = turso_pg_server::counters::thread_allocations();
        let foreign_cpu_ns = turso_pg_server::counters::foreign_thread_cpu_ns().unwrap_or(0);
        let lifecycle = turso_pg_server::counters::thread_lifecycle();
        Self {
            lifecycle,
            ns: now_ns(),
            cpu_ns: thread_cpu_ns(),
            counters,
            allocations,
            thread_instructions,
            confirms,
            foreign_cpu_ns,
            thread_allocations,
        }
    }
}

struct Args {
    db: String,
    branches: BranchDurability,
    rows: u64,
    warmup: u64,
    ops: u64,
    siblings: u64,
    pregrow: u64,
    plant_extra: bool,
    plant_extra_write: bool,
    name_prefix: String,
    stepwise: bool,
    out: String,
}

fn args() -> Args {
    let mut a = std::env::args().skip(1);
    let (mut db, mut out) = (None, None);
    let (mut durability, mut store) = ("full".to_string(), "catalog".to_string());
    let (mut rows, mut warmup, mut ops, mut siblings, mut pregrow) = (1000, 20, 200, 0, 0);
    let mut plant_extra = false;
    let mut plant_extra_write = false;
    let mut name_prefix = "b_".to_string();
    let mut stepwise = false;
    while let Some(k) = a.next() {
        if k == "--stepwise" {
            stepwise = true;
            continue;
        }
        let mut v = || a.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--db" => db = Some(v()),
            "--durability" => durability = v(),
            "--store" => store = v(),
            "--rows" => rows = v().parse().unwrap(),
            "--warmup" => warmup = v().parse().unwrap(),
            "--ops" => ops = v().parse().unwrap(),
            "--siblings" => siblings = v().parse().unwrap(),
            "--pregrow" => pregrow = v().parse().unwrap(),
            "--plant" => match v().as_str() {
                "extra" => plant_extra = true,
                "extra-write" => plant_extra_write = true,
                other => panic!("--plant {other}: extra or extra-write"),
            },
            "--name-prefix" => name_prefix = v(),
            "--out" => out = Some(v()),
            other => panic!("unknown argument {other}"),
        }
    }
    let sync = match durability.as_str() {
        "full" => SyncClass::FullFsync,
        "fsync" => SyncClass::Fsync,
        "off" => SyncClass::Off,
        other => panic!("--durability {other}: full, fsync or off"),
    };
    let branches = match store.as_str() {
        "catalog" => BranchDurability::Catalog { sync },
        "snapshot" => BranchDurability::Durable { sync },
        other => panic!("--store {other}: catalog or snapshot"),
    };
    Args {
        db: db.expect("--db"),
        branches,
        rows,
        warmup,
        ops,
        siblings,
        pregrow,
        plant_extra,
        plant_extra_write,
        name_prefix,
        stepwise,
        out: out.expect("--out"),
    }
}

fn run(conn: &PgConnection, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn main() {
    // The budget build's thread start and exit counter, before any thread exists.
    #[cfg(feature = "alloc-count")]
    turso_pg_server::counters::install_thread_lifecycle_counter();
    let a = args();
    // The server's options (database_opts), less the tursopg binary's `turso_cli()`, which exists
    // only under core's cli_only feature (this crate does not enable it, so the call did not
    // compile: the cloud's all-targets build at abefd2e71) and only allows loading extensions,
    // which no operation measured here does.
    let opts = turso_pg_server::database_opts(a.branches);
    let (_io, db) =
        turso_pg::open_database(&a.db, None, turso_core::OpenFlags::default(), opts).unwrap();
    // The session's trunk connection, as a server session opens it.
    let trunk = PgConnection::new(db.connect().unwrap());
    turso_pg::attach_schema_files(&trunk, &a.db);

    run(&trunk, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
    run(&trunk, "CREATE TABLE t2(id INT)");
    run(&trunk, "BEGIN");
    for id in 1..=a.rows {
        run(&trunk, &format!("INSERT INTO t VALUES ({id}, 0)"));
    }
    run(&trunk, "COMMIT");
    for k in 0..a.siblings {
        trunk.inner().create_branch(&format!("sib_{k}")).unwrap();
    }
    for k in 0..a.pregrow {
        let name = format!("pregrow_{k}");
        trunk.inner().create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
    }
    // Every other thread quiet (the name filter's growth done): the foreign CPU and the thread
    // counts unchanged across 200 ms, within 30 s.
    let quiet_by = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let read = || {
            (
                turso_pg_server::counters::foreign_thread_cpu_ns(),
                turso_pg_server::counters::thread_lifecycle(),
            )
        };
        let before = read();
        std::thread::sleep(std::time::Duration::from_millis(200));
        if read() == before {
            break;
        }
        assert!(std::time::Instant::now() < quiet_by, "other threads never went quiet after the seed");
    }

    let mut out = std::io::BufWriter::new(std::fs::File::create(&a.out).unwrap());
    writeln!(
        out,
        "seq\tphase\tstep\tstart_ns\tend_ns\tcpu_ns\tunix_syscalls\tinstructions\tbranch\tallocations\tthread_instructions\tconfirms\tthread_allocations\tforeign_cpu_ns\tthread_starts\tthread_exits\tgap_foreign_cpu_ns\tgap_thread_starts\tgap_thread_exits"
    )
    .unwrap();
    let prefix = a.name_prefix.clone();
    // The end of the previous row's window: the gap columns run from it to the next row's start.
    let mut last = Probe::now(&db);
    let mut rec = |seq: u64, phase: &str, step: &str, t0: Probe, t1: Probe| {
        let (s0, i0) = t0
            .counters
            .map_or((0, 0), |c| (c.unix_syscalls, c.instructions));
        let (s1, i1) = t1
            .counters
            .map_or((0, 0), |c| (c.unix_syscalls, c.instructions));
        // NA where the build counts no allocations (no `alloc-count`): budget.py refuses it.
        let allocations = match (t0.allocations, t1.allocations) {
            (Some(a0), Some(a1)) => signed(a0, a1).to_string(),
            _ => "NA".to_string(),
        };
        writeln!(
            out,
            "{seq}\t{phase}\t{step}\t{}\t{}\t{}\t{}\t{}\t{prefix}{seq}\t{allocations}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            t0.ns,
            t1.ns,
            signed(t0.cpu_ns, t1.cpu_ns),
            signed(s0, s1),
            signed(i0, i1),
            signed(t0.thread_instructions, t1.thread_instructions),
            signed(t0.confirms, t1.confirms),
            signed(t0.thread_allocations, t1.thread_allocations),
            signed(t0.foreign_cpu_ns, t1.foreign_cpu_ns),
            lifecycle_fields(&t0, &t1),
            signed(last.foreign_cpu_ns, t0.foreign_cpu_ns),
            lifecycle_fields(&last, &t0)
        )
        .unwrap();
        last = t1;
    };
    // As the server says it (budget.py --verdict full-word-held reads it from this stderr).
    if turso_core::branch::measure_word_hold_compiled() {
        eprintln!(
            "MEASURE word-hold: compiled (TURSO_MEASURE_WORD_HOLD={})",
            std::env::var("TURSO_MEASURE_WORD_HOLD").unwrap_or_default()
        );
    }
    let mut control = std::io::stdin().lines();
    if a.stepwise {
        println!("ready");
        std::io::stdout().flush().unwrap();
    }
    for seq in 0..a.warmup + a.ops {
        if a.stepwise {
            let line = control.next().expect("--stepwise: stdin ended").unwrap();
            assert_eq!(line, format!("cycle {seq}"), "--stepwise: out of step");
        }
        let phase = if seq < a.warmup { "warmup" } else { "measure" };
        let name = format!("{}{seq}", a.name_prefix);
        let id = 1 + (seq * 7919) % a.rows;

        // Two probes with nothing between, before every step: what the probes themselves cost,
        // for the budget to subtract from the step after it (the wire side subtracts the empty
        // query just before each step the same way).
        macro_rules! noop {
            () => {{
                let p0 = Probe::now(&db);
                let p1 = Probe::now(&db);
                rec(seq, phase, "noop", p0, p1);
            }};
        }

        noop!();
        let p0 = Probe::now(&db);
        trunk.inner().create_branch(&name).unwrap();
        if a.plant_extra {
            run(&trunk, &format!("INSERT INTO t2 VALUES ({seq})"));
        }
        rec(seq, phase, "create", p0, Probe::now(&db));

        noop!();
        let p0 = Probe::now(&db);
        let branch = PgConnection::new(db.connect_named(&name).unwrap());
        branch.adopt_session_of(&trunk);
        rec(seq, phase, "switch", p0, Probe::now(&db));

        noop!();
        let p0 = Probe::now(&db);
        let mut stmt = branch
            .prepare(format!("UPDATE t SET v = v + 1 WHERE id = {id}"))
            .unwrap();
        stmt.run_ignore_rows().unwrap();
        assert_eq!(stmt.n_change(), 1, "the write touched no row");
        drop(stmt);
        if a.plant_extra_write {
            run(&branch, &format!("INSERT INTO t2 VALUES ({seq})"));
        }
        rec(seq, phase, "write", p0, Probe::now(&db));

        noop!();
        let p0 = Probe::now(&db);
        trunk.adopt_session_of(&branch);
        drop(branch);
        rec(seq, phase, "main", p0, Probe::now(&db));

        noop!();
        let p0 = Probe::now(&db);
        db.drop_branch(&name).unwrap();
        rec(seq, phase, "delete", p0, Probe::now(&db));
        if a.stepwise {
            println!("done {seq}");
            std::io::stdout().flush().unwrap();
        }
    }
    out.flush().unwrap();
}

/// The wire budget's allocation counter (feature `alloc-count`, budget builds only).
#[cfg(feature = "alloc-count")]
#[global_allocator]
static ALLOC: turso_pg_server::counters::CountingAlloc = turso_pg_server::counters::CountingAlloc;
