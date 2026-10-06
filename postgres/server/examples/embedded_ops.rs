//! The server's branch cycle run embedded, for the wire-versus-embedded budgets (fastest-wire M3):
//! the same engine calls `tursopg --server` makes for each statement of the cycle, on a database
//! opened with the server's own options, with each step's start and end on the clock the V1 and
//! C1b tracers stamp (CLOCK_UPTIME_RAW), so their events can be attributed to steps.
//!
//!   embedded_ops --db PATH [--durability full|fsync|off] [--store catalog|snapshot]
//!                [--rows N] [--warmup N] [--ops N] --out FILE
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
//! Output: TSV `seq phase step start_ns end_ns cpu_ns`, phase `warmup` or `measure`; `cpu_ns` is the
//! calling thread's CPU time across the step (CLOCK_THREAD_CPUTIME_ID), for cpu_cost.py.

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

struct Args {
    db: String,
    branches: BranchDurability,
    rows: u64,
    warmup: u64,
    ops: u64,
    out: String,
}

fn args() -> Args {
    let mut a = std::env::args().skip(1);
    let (mut db, mut out) = (None, None);
    let (mut durability, mut store) = ("full".to_string(), "catalog".to_string());
    let (mut rows, mut warmup, mut ops) = (1000, 20, 200);
    while let Some(k) = a.next() {
        let mut v = || a.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--db" => db = Some(v()),
            "--durability" => durability = v(),
            "--store" => store = v(),
            "--rows" => rows = v().parse().unwrap(),
            "--warmup" => warmup = v().parse().unwrap(),
            "--ops" => ops = v().parse().unwrap(),
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
        out: out.expect("--out"),
    }
}

fn run(conn: &PgConnection, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn main() {
    let a = args();
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

    let mut out = std::io::BufWriter::new(std::fs::File::create(&a.out).unwrap());
    writeln!(out, "seq\tphase\tstep\tstart_ns\tend_ns\tcpu_ns").unwrap();
    let mut rec = |seq: u64, phase: &str, step: &str, t0: u64, t1: u64, cpu: u64| {
        writeln!(out, "{seq}\t{phase}\t{step}\t{t0}\t{t1}\t{cpu}").unwrap();
    };
    for seq in 0..a.warmup + a.ops {
        let phase = if seq < a.warmup { "warmup" } else { "measure" };
        let name = format!("b_{seq}");
        let id = 1 + (seq * 7919) % a.rows;

        let (t0, c0) = (now_ns(), thread_cpu_ns());
        trunk.inner().create_branch(&name).unwrap();
        let (t1, c1) = (now_ns(), thread_cpu_ns());
        rec(seq, phase, "create", t0, t1, c1 - c0);

        let (t0, c0) = (now_ns(), thread_cpu_ns());
        let branch = PgConnection::new(db.connect_named(&name).unwrap());
        branch.adopt_session_of(&trunk);
        let (t1, c1) = (now_ns(), thread_cpu_ns());
        rec(seq, phase, "switch", t0, t1, c1 - c0);

        let (t0, c0) = (now_ns(), thread_cpu_ns());
        let mut stmt = branch
            .prepare(format!("UPDATE t SET v = v + 1 WHERE id = {id}"))
            .unwrap();
        stmt.run_ignore_rows().unwrap();
        assert_eq!(stmt.n_change(), 1, "the write touched no row");
        drop(stmt);
        let (t1, c1) = (now_ns(), thread_cpu_ns());
        rec(seq, phase, "write", t0, t1, c1 - c0);

        let (t0, c0) = (now_ns(), thread_cpu_ns());
        trunk.adopt_session_of(&branch);
        drop(branch);
        let (t1, c1) = (now_ns(), thread_cpu_ns());
        rec(seq, phase, "main", t0, t1, c1 - c0);

        let (t0, c0) = (now_ns(), thread_cpu_ns());
        db.drop_branch(&name).unwrap();
        let (t1, c1) = (now_ns(), thread_cpu_ns());
        rec(seq, phase, "delete", t0, t1, c1 - c0);
    }
    out.flush().unwrap();
}
