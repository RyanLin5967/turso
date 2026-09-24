//! Per-operation latency of branching at N live branches, one page written per branch.
//!
//! The Turso-fork counterpart of ferrodb's million-branch curves. Pre-registration, written
//! before the first run: `PREREG.md` next to this file.
//!
//!   cargo run -p turso_core --release --example branch_curve -- \
//!       --checkpoints 100,1000,10000,100000,1000000 --samples 200
//!
//! ⚠ UNBUILT when committed: written under the no-local-compute rule; never compiled or run.
//!
//! At each checkpoint N (exactly N live branches, each holding one page of its own) it measures
//! K samples of each operation, then removes what the samples added so the next growth phase
//! starts from exactly N again:
//!
//!   fork         trunk.fork_branch()                      (the branch is kept for the next two)
//!   open         branch.connect()                         (a fresh connection on that branch)
//!   first_write  one UPDATE of one row, autocommit        (that branch's first write)
//!   read_open    branch.connect() on a random live branch
//!   read_own     SELECT of the row that branch wrote      (resolves to the branch's arena page)
//!   read_inh     SELECT of a row it did not write         (resolves to the trunk's page)
//!   reap         Branch::reap() of a sampled branch       (frees its one page)
//!
//! With `--durability durable` (or `durable-nosync`) the branches are durable and each checkpoint
//! adds a REOPEN column: every live branch is detached, the database is dropped and reopened
//! `--reopens` times (recovery replays the branch log), the N handles are re-attached, and K
//! first-connect-after-reopen reads are timed (each reparses the branch's schema):
//!
//!   reopen       Database::open + connect: trunk open + branch-store recovery   (1 per reopen)
//!   attach_all   re-attach all N detached branches                              (1 per reopen)
//!   reopen_read  connect + SELECT own row on a random branch after the reopen
//!
//! Volatile (the default, and the arm PREREG.md's original predictions are about) has no reopen.
//!
//! Both arms also time an `expire` column (PREREG amendment A2): K extra one-page branches are
//! given a lease, the lease clock is run past it, and ONE `Database::expire_branches` call reaps
//! them; the harness asserts exactly those K were reaped and K pages freed.
//!
//! It REFUSES to print a number it cannot attribute: before each checkpoint it asserts from the
//! engine that exactly N branches are live and that they hold exactly the arena pages the
//! workload predicts, and it spot-checks isolation on a random branch. Any mismatch prints
//! `NOT A RESULT` and exits non-zero.

use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{Branch, BranchDurability, BranchId};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;

struct Args {
    checkpoints: Vec<usize>,
    samples: usize,
    seed: u64,
    durability: BranchDurability,
    reopens: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        checkpoints: vec![100, 1000],
        samples: 200,
        seed: 0x9E37_79B9_7F4A_7C15,
        durability: BranchDurability::Volatile,
        reopens: 3,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--checkpoints" => {
                args.checkpoints = val()
                    .split(',')
                    .map(|s| s.parse().unwrap_or_else(|_| die("bad --checkpoints")))
                    .collect();
            }
            "--samples" => args.samples = val().parse().unwrap_or_else(|_| die("bad --samples")),
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--reopens" => args.reopens = val().parse().unwrap_or_else(|_| die("bad --reopens")),
            "--durability" => {
                args.durability = match val().as_str() {
                    "volatile" => BranchDurability::Volatile,
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    other => die(&format!(
                        "--durability must be volatile, durable or durable-nosync, not {other}"
                    )),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.samples == 0 {
        die("--samples must be positive");
    }
    args
}

fn die(msg: &str) -> ! {
    eprintln!("branch_curve: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
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

/// Same length as the trunk value, so the UPDATE rewrites its row in place and dirties exactly
/// the one leaf page that holds it.
fn branch_value(id: i64) -> String {
    format!("b{:0>width$}", id, width = VALUE_LEN - 1)
}

/// The row branch number `n` writes: spread over the table so writes land on different leaves.
fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
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

fn update(conn: &Arc<Connection>, id: i64) {
    conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {id}", branch_value(id)))
        .unwrap();
}

struct Live {
    branch: Branch,
    row: i64,
}

/// Fork one branch and give it one page of its own.
fn grow_one(trunk: &Arc<Connection>, n: usize) -> Live {
    let branch = trunk.fork_branch().unwrap();
    let row = row_for(n);
    let conn = branch.connect().unwrap();
    update(&conn, row);
    drop(conn);
    Live { branch, row }
}

/// Print one op's percentiles at `n` and keep its p50 for the slope fit.
fn report(
    n: usize,
    op: &'static str,
    samples: &[std::time::Duration],
    summary: &mut Vec<(usize, &'static str, f64)>,
) {
    let mut us: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&us, 50.0);
    println!(
        "{n}\t{op}\t{}\t{p50:.2}\t{:.2}\t{:.2}\t{:.2}",
        us.len(),
        percentile(&us, 90.0),
        percentile(&us, 99.0),
        us[us.len() - 1]
    );
    summary.push((n, op, p50));
}

/// Up to `k` distinct indices below `n`, in random order.
fn distinct_indices(rng: &mut Rng, n: usize, k: usize) -> Vec<usize> {
    let k = k.min(n);
    let mut seen = std::collections::HashSet::with_capacity(k);
    let mut out = Vec::with_capacity(k);
    while out.len() < k {
        let i = rng.below(n);
        if seen.insert(i) {
            out.push(i);
        }
    }
    out
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

/// The clock's effective tick, so a flat curve can be told apart from a flat clock.
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

fn open_db(path: &std::path::Path, durability: BranchDurability) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(durability),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn main() {
    let args = parse_args();
    let durable = matches!(args.durability, BranchDurability::Durable { .. });
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_curve.db");
    let mut db = open_db(&path, args.durability);
    let mut trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let page_size = trunk
        .prepare("PRAGMA page_size")
        .unwrap()
        .run_collect_rows()
        .unwrap()[0][0]
        .as_int()
        .unwrap();
    let trunk_pages = trunk
        .prepare("PRAGMA page_count")
        .unwrap()
        .run_collect_rows()
        .unwrap()[0][0]
        .as_int()
        .unwrap();

    println!("# branch_curve — Turso fork, per-branch CoW arena");
    println!(
        "# checkpoints={:?} samples={} seed={:#x} trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} \
         page_size={page_size} trunk_pages={trunk_pages} durability={:?} reopens={}",
        args.checkpoints, args.samples, args.seed, args.durability, args.reopens
    );
    println!(
        "# clock tick {:.0} ns (Instant); times below are microseconds per operation",
        clock_tick_ns()
    );
    println!(
        "# build: {} ; rss_base_bytes={}",
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        rss_bytes()
    );
    println!("N\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us");

    let mut rng = Rng(args.seed);
    let mut live: Vec<Live> = Vec::new();
    let mut summary: Vec<(usize, &'static str, f64)> = Vec::new();
    let mut grown = 0usize;

    for &n in &args.checkpoints {
        let t = Instant::now();
        while live.len() < n {
            live.push(grow_one(&trunk, grown));
            grown += 1;
        }
        let grow_us = t.elapsed().as_secs_f64() * 1e6;

        // Ask the engine, not the harness, what exists.
        let stats = db.branch_stats().unwrap();
        if stats.live_branches != n {
            not_a_result(&format!("expected {n} live branches, engine has {}", stats.live_branches));
        }
        // Each branch rewrote one row in place: exactly one page of its own, and the trunk wrote
        // nothing after any fork, so it retains nothing.
        if stats.arena_slots_in_use != n {
            not_a_result(&format!(
                "expected {n} arena pages (one per branch), engine has {}",
                stats.arena_slots_in_use
            ));
        }
        let probe = &live[rng.below(live.len())];
        let pc = probe.branch.connect().unwrap();
        if read_v(&pc, probe.row) != branch_value(probe.row) {
            not_a_result("a branch does not see its own write");
        }
        drop(pc);
        if read_v(&trunk, probe.row) != trunk_value(probe.row) {
            not_a_result("the trunk sees a branch's write");
        }

        let k = args.samples;
        let mut fork = Vec::with_capacity(k);
        let mut open = Vec::with_capacity(k);
        let mut first_write = Vec::with_capacity(k);
        let mut sampled: Vec<Live> = Vec::with_capacity(k);
        for i in 0..k {
            let row = row_for(grown + i);
            let t = Instant::now();
            let branch = trunk.fork_branch().unwrap();
            fork.push(t.elapsed());
            let t = Instant::now();
            let conn = branch.connect().unwrap();
            open.push(t.elapsed());
            let t = Instant::now();
            update(&conn, row);
            first_write.push(t.elapsed());
            drop(conn);
            sampled.push(Live { branch, row });
        }
        grown += k;

        let mut read_open = Vec::with_capacity(k);
        let mut read_own = Vec::with_capacity(k);
        let mut read_inh = Vec::with_capacity(k);
        for _ in 0..k {
            let target = &live[rng.below(live.len())];
            // Half the table away: never on the one leaf this branch owns.
            let other = (target.row - 1 + TRUNK_ROWS / 2) % TRUNK_ROWS + 1;
            let t = Instant::now();
            let conn = target.branch.connect().unwrap();
            read_open.push(t.elapsed());
            let t = Instant::now();
            let own = read_v(&conn, target.row);
            read_own.push(t.elapsed());
            let t = Instant::now();
            let inh = read_v(&conn, other);
            read_inh.push(t.elapsed());
            drop(conn);
            if own != branch_value(target.row) || inh != trunk_value(other) {
                not_a_result("a sampled read returned the wrong version");
            }
        }

        let mut reap = Vec::with_capacity(k);
        for s in sampled {
            let t = Instant::now();
            let reaped = s.branch.reap().unwrap();
            reap.push(t.elapsed());
            if reaped.deferred || reaped.freed_pages != 1 {
                not_a_result(&format!("a sampled reap freed {reaped:?}, expected one page"));
            }
        }
        if db.branch_stats().unwrap().live_branches != n || db.branch_stats().unwrap().arena_slots_in_use != n {
            not_a_result("sampling did not return the engine to N branches");
        }

        // The expire column: K leased one-page branches, their lease run out, one pass.
        let mut expiring = Vec::with_capacity(k);
        for i in 0..k {
            let doomed = grow_one(&trunk, grown + i);
            // An hour, not a second: the lease clock includes real open time, and forking K
            // branches must not let the first ones expire (the pass also runs at every fork).
            doomed
                .branch
                .lease(std::time::Duration::from_secs(3600))
                .unwrap();
            expiring.push(doomed.branch.id());
            // Detached: nothing but the lease will ever reap it, as for a crashed agent.
            let _ = doomed.branch.into_id();
        }
        grown += k;
        db.branch_lease_clock_advance(std::time::Duration::from_secs(7200));
        let t = Instant::now();
        let expired = db.expire_branches().unwrap();
        let expire_elapsed = t.elapsed();
        let mut reaped_ids = expired.reaped.clone();
        reaped_ids.sort();
        expiring.sort();
        if reaped_ids != expiring || expired.freed_pages != k {
            not_a_result(&format!(
                "the expiry pass reaped {} branches and freed {} pages; expected exactly the {k} \
                 leased ones and {k} pages",
                expired.reaped.len(),
                expired.freed_pages
            ));
        }
        if db.branch_stats().unwrap().live_branches != n || db.branch_stats().unwrap().arena_slots_in_use != n {
            not_a_result("the expiry pass did not return the engine to N branches");
        }
        // Reported per reaped branch, so it reads against `reap`.
        let expire_per_branch = vec![expire_elapsed / k as u32];

        for (op, samples) in [
            ("fork", fork),
            ("open", open),
            ("first_write", first_write),
            ("read_open", read_open),
            ("read_own", read_own),
            ("read_inh", read_inh),
            ("reap", reap),
            ("expire_per_branch", expire_per_branch),
        ] {
            report(n, op, &samples, &mut summary);
        }

        if durable {
            let mut reopen = Vec::with_capacity(args.reopens);
            let mut attach_all = Vec::with_capacity(args.reopens);
            for _ in 0..args.reopens {
                let detached: Vec<(BranchId, i64)> = live
                    .drain(..)
                    .map(|l| (l.branch.into_id(), l.row))
                    .collect();
                // Every holder of the Database is gone, so the open below is a real one.
                drop(trunk);
                drop(db);
                let t = Instant::now();
                db = open_db(&path, args.durability);
                trunk = db.connect().unwrap();
                reopen.push(t.elapsed());
                let t = Instant::now();
                live = detached
                    .into_iter()
                    .map(|(id, row)| Live {
                        branch: db.branch(id).unwrap(),
                        row,
                    })
                    .collect();
                attach_all.push(t.elapsed());
                let stats = db.branch_stats().unwrap();
                if stats.live_branches != n || stats.arena_slots_in_use != n {
                    not_a_result(&format!(
                        "after a reopen the engine has {stats:?}; expected {n} branches holding \
                         {n} pages"
                    ));
                }
            }
            // The first connect to a branch after a reopen reparses its schema, so each sample is a
            // DIFFERENT branch: a repeat would time the cached schema instead.
            let mut reopen_read = Vec::with_capacity(k);
            for i in distinct_indices(&mut rng, live.len(), k) {
                let target = &live[i];
                let t = Instant::now();
                let conn = target.branch.connect().unwrap();
                let own = read_v(&conn, target.row);
                reopen_read.push(t.elapsed());
                drop(conn);
                if own != branch_value(target.row) {
                    not_a_result("after a reopen a branch no longer sees its own write");
                }
            }
            report(n, "reopen", &reopen, &mut summary);
            report(n, "attach_all", &attach_all, &mut summary);
            report(n, "reopen_read", &reopen_read, &mut summary);
        }
        let rss = rss_bytes();
        println!(
            "# N={n} grow_total_us={grow_us:.0} arena_pages={} arena_free={} rss_bytes={rss} \
             rss_per_branch={:.0}",
            db.branch_stats().unwrap().arena_slots_in_use,
            db.branch_stats().unwrap().arena_slots_free,
            rss as f64 / n as f64
        );
    }

    // log-log slope of p50 against N, per op, least squares over every checkpoint.
    if args.checkpoints.len() >= 2 {
        println!("# p50 log-log slope over N={:?}", args.checkpoints);
        for op in [
            "fork",
            "open",
            "first_write",
            "read_open",
            "read_own",
            "read_inh",
            "reap",
            "expire_per_branch",
            "reopen",
            "attach_all",
            "reopen_read",
        ] {
            let pts: Vec<(f64, f64)> = summary
                .iter()
                .filter(|(_, o, _)| *o == op)
                .map(|&(n, _, p50)| ((n as f64).ln(), p50.ln()))
                .collect();
            if pts.len() < 2 {
                continue;
            }
            let m = pts.len() as f64;
            let (sx, sy) = pts.iter().fold((0.0, 0.0), |(a, b), (x, y)| (a + x, b + y));
            let (mx, my) = (sx / m, sy / m);
            let num: f64 = pts.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
            let den: f64 = pts.iter().map(|(x, _)| (x - mx).powi(2)).sum();
            println!("# slope\t{op}\t{:+.3}", num / den);
        }
    }
    drop(live);
    let end = db.branch_stats().unwrap();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!("# teardown: every branch freed, arena empty ({} free slots)", end.arena_slots_free);
}
