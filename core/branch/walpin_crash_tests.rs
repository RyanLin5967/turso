//! r11-walpin-conc amendment 21: durable wal2 (FW2). Each configuration runs a workload in a child
//! process of this test binary, with FW2 on, and kills that child at one crash point. Further
//! children then reopen the directory. The recovered database must hold every acknowledged
//! transaction, the in-flight one exactly as registered, and nothing else, and must keep working
//! across another switch. Mutant M1 (keep only the newer file where both would be recovered) must
//! fail the same check wherever the older file still holds commits that the database file lacks.
//!
//! Children are driven by `WAL2_CRASH_MODE` and are no-ops without it, so `wal2_crash_child` passes
//! trivially in an ordinary run. The engine's crash points are `walpin::crash` (test builds only).

use super::walpin;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

const CHILD: &str = "branch::walpin_crash_tests::wal2_crash_child";
const ROWS_PER_TXN: i64 = 8;
const MAX_TXNS: i64 = 6_000;
const CONTINUE_TXNS: i64 = 400;
const CHILD_LIMIT: std::time::Duration = std::time::Duration::from_secs(300);

fn open(dir: &str) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let path = Path::new(dir).join("crash.db");
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn int(conn: &Arc<crate::Connection>, sql: &str) -> i64 {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap_or_else(|| panic!("{sql}: not an integer"))
}

/// Transaction `i`: 8 rows of ~400 bytes, and meta.last = i.
fn txn(conn: &Arc<crate::Connection>, i: i64) {
    conn.execute("BEGIN").unwrap();
    for j in 0..ROWS_PER_TXN {
        conn.execute(format!(
            "INSERT INTO t VALUES ({}, {i}, '{}')",
            i * ROWS_PER_TXN + j,
            "v".repeat(400)
        ))
        .unwrap();
    }
    conn.execute(format!("UPDATE meta SET last = {i} WHERE k = 1"))
        .unwrap();
    conn.execute("COMMIT").unwrap();
}

fn say(line: &str) {
    let mut out = std::io::stdout();
    writeln!(out, "{line}").unwrap();
    out.flush().unwrap();
}

/// `WAL2_CRASH_ACK=<k>:<m>`: kill this child after the m-th COMMIT made while the switch count is
/// k has returned, before its ack. `restart:<m>` counts COMMITs after the restart arm's TRUNCATE.
fn ack_point() -> Option<(String, u64)> {
    let v = std::env::var("WAL2_CRASH_ACK").ok()?;
    let (k, m) = v.rsplit_once(':')?;
    Some((k.to_string(), m.parse().ok()?))
}

fn work(dir: &str) {
    let arm = std::env::var("WAL2_CRASH_ARM").unwrap_or_else(|_| "plain".into());
    let ack = ack_point();
    let db = open(dir);
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, txn INTEGER, v BLOB)")
        .unwrap();
    conn.execute("CREATE TABLE meta(k INTEGER PRIMARY KEY, last INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO meta VALUES (1, 0)").unwrap();
    let pin = db.connect().unwrap();
    let mut pinned = false;
    let (mut last_sw, mut since_switch) = (0u64, 0u64);
    let (mut truncated, mut since_truncate) = (false, 0u64);
    for i in 1..=MAX_TXNS {
        if arm == "pinned"
            && !pinned
            && walpin::counters().fw2_switches == 0
            && db.walpin_max_frame() >= 900
        {
            // A reader whose snapshot ends in file 0: after switch 1 it refuses file 0's checkpoint.
            pin.execute("BEGIN").unwrap();
            int(&pin, "SELECT count(*) FROM t");
            pinned = true;
            say("pinned");
        }
        txn(&conn, i);
        let sw = walpin::counters().fw2_switches;
        if sw != last_sw {
            (last_sw, since_switch) = (sw, 0);
        }
        since_switch += 1;
        if truncated {
            since_truncate += 1;
        }
        match &ack {
            Some((k, m)) if k == "restart" && truncated && since_truncate == *m => {
                walpin::crash::kill_self(&format!("acked restart:{m} (txn {i})"))
            }
            Some((k, m)) if k.parse::<u64>().ok() == Some(sw) && since_switch == *m => {
                walpin::crash::kill_self(&format!("acked {k}:{m} (txn {i})"))
            }
            _ => {}
        }
        say(&format!("ack {i}"));
        if arm == "restart" && !truncated && sw == 2 && since_switch == 10 {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            assert_eq!(db.walpin_max_frame(), 0, "the TRUNCATE restarted the log");
            truncated = true;
            say(&format!("truncated after {i}"));
        }
    }
    say("no crash point fired");
}

/// Print what the recovered database holds: the decision, the present transactions (they must be
/// exactly 1..=P, each with all its rows), and meta.last.
fn verify(dir: &str) {
    let db = open(dir);
    say(&format!(
        "decision {}",
        walpin::WAL2_RECOVERY.load(std::sync::atomic::Ordering::Relaxed)
    ));
    let conn = db.connect().unwrap();
    let rows = conn
        .prepare("SELECT txn, count(*) FROM t GROUP BY txn ORDER BY txn")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let mut present = 0i64;
    let mut complete = true;
    for (idx, row) in rows.iter().enumerate() {
        let n = |v: &Value| v.as_int().expect("an integer");
        let (t, c) = (n(&row[0]), n(&row[1]));
        if t != idx as i64 + 1 || c != ROWS_PER_TXN {
            complete = false;
        }
        present = t.max(present);
    }
    let meta = int(&conn, "SELECT last FROM meta WHERE k = 1");
    say(&format!(
        "present {present} complete {complete} meta {meta} txns {}",
        rows.len()
    ));
}

/// Reopen and commit `CONTINUE_TXNS` more transactions across at least one switch, then close.
fn continue_(dir: &str) {
    let db = open(dir);
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    let last = int(&conn, "SELECT last FROM meta WHERE k = 1");
    for i in last + 1..=last + CONTINUE_TXNS {
        txn(&conn, i);
    }
    say(&format!(
        "continued to {} switches {}",
        last + CONTINUE_TXNS,
        walpin::counters().fw2_switches
    ));
}

#[test]
fn wal2_crash_child() {
    let Ok(mode) = std::env::var("WAL2_CRASH_MODE") else {
        return;
    };
    let dir = std::env::var("WAL2_CRASH_DIR").unwrap();
    assert!(walpin::fw2(), "a child runs with TURSO_WALPIN_FIX=fw2");
    match mode.as_str() {
        "work" => work(&dir),
        "verify" => verify(&dir),
        "continue" => continue_(&dir),
        other => panic!("unknown WAL2_CRASH_MODE {other}"),
    }
}

fn child(mode: &str, dir: &Path, env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([CHILD, "--exact", "--nocapture", "--test-threads=1"])
        .env("TURSO_WALPIN_FIX", "fw2")
        .env("WAL2_CRASH_MODE", mode)
        .env("WAL2_CRASH_DIR", dir)
        .env_remove("TURSO_WALPIN_CRASH")
        .env_remove("TURSO_WALPIN_WAL2_MUTANT")
        .env_remove("WAL2_CRASH_ACK");
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Output goes to files beside the directory, and the child is killed after CHILD_LIMIT, so a
    // child that hangs (a mutant reading an inconsistent tree) fails its configuration instead of
    // the whole test.
    let (out_path, err_path) = (dir.with_extension("out"), dir.with_extension("err"));
    cmd.stdout(std::fs::File::create(&out_path).unwrap())
        .stderr(std::fs::File::create(&err_path).unwrap());
    let mut proc = cmd.spawn().unwrap();
    let start = std::time::Instant::now();
    let status = loop {
        if let Some(status) = proc.try_wait().unwrap() {
            break status;
        }
        if start.elapsed() > CHILD_LIMIT {
            let _ = proc.kill();
            let status = proc.wait().unwrap();
            std::fs::OpenOptions::new()
                .append(true)
                .open(&err_path)
                .unwrap()
                .write_all(format!("killed after {CHILD_LIMIT:?}\n").as_bytes())
                .unwrap();
            break status;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    Output {
        status,
        stdout: std::fs::read(&out_path).unwrap(),
        stderr: std::fs::read(&err_path).unwrap(),
    }
}

fn text(out: &Output) -> String {
    format!(
        "status {:?}\n--- stdout\n{}--- stderr\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The last `<key> ...` line's fields after the key.
fn line<'a>(out: &'a str, key: &str) -> Option<Vec<&'a str>> {
    out.lines()
        .rev()
        .find(|l| l.starts_with(key))
        .map(|l| l.split_whitespace().collect())
}

/// (present, complete, meta) from a verify child's stdout, None if it did not finish.
fn verified(out: &Output) -> Option<(i64, bool, i64)> {
    if !out.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let f = line(&stdout, "present ")?;
    Some((f[1].parse().ok()?, f[3] == "true", f[5].parse().ok()?))
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

struct Config {
    name: String,
    arm: &'static str,
    engine: Option<String>,
    ack: Option<String>,
    inflight_present: bool,
    m1_must_kill: bool,
}

/// The 22 registered configurations (PREREG amendment 21).
fn configs() -> Vec<Config> {
    let mut v = Vec::new();
    for k in 1..=3u64 {
        for (point, present) in [
            ("switched", false),
            ("header", false),
            ("commit_written", true),
            ("ckpt_copied", true),
            ("ckpt_done", true),
        ] {
            v.push(Config {
                name: format!("{point}:{k}"),
                arm: "plain",
                engine: Some(format!("{point}:{k}")),
                ack: None,
                inflight_present: present,
                m1_must_kill: point == "commit_written",
            });
        }
        v.push(Config {
            name: format!("acked:{k}"),
            arm: "plain",
            engine: None,
            ack: Some(format!("{k}:1")),
            inflight_present: true,
            m1_must_kill: false,
        });
    }
    v.push(Config {
        name: "pinned acked:1:20".into(),
        arm: "pinned",
        engine: None,
        ack: Some("1:20".into()),
        inflight_present: true,
        m1_must_kill: true,
    });
    for (point, present) in [("restart_wal0", false), ("restart_wal1", false)] {
        v.push(Config {
            name: format!("restart {point}:2"),
            arm: "restart",
            engine: Some(format!("{point}:2")),
            ack: None,
            inflight_present: present,
            m1_must_kill: false,
        });
    }
    v.push(Config {
        name: "restart acked:5".into(),
        arm: "restart",
        engine: None,
        ack: Some("restart:5".into()),
        inflight_present: true,
        m1_must_kill: false,
    });
    v
}

/// PREREG amendment 21: at every registered crash point, recovery keeps every acknowledged
/// transaction and the in-flight one as registered, nothing else, complete, and the database keeps
/// working across another switch and a reopen; M1 is killed wherever it is predicted to be.
#[test]
#[ignore = "spawns ~110 child processes of this binary (FW2 on in each); run explicitly"]
fn wal2_recovers_every_committed_txn_at_every_switch_point() {
    let mut failures = Vec::new();
    let mut table = Vec::new();
    for c in configs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("db");
        std::fs::create_dir_all(&dir).unwrap();
        let mut env: Vec<(&str, &str)> = vec![("WAL2_CRASH_ARM", c.arm)];
        if let Some(e) = &c.engine {
            env.push(("TURSO_WALPIN_CRASH", e.as_str()));
        }
        if let Some(a) = &c.ack {
            env.push(("WAL2_CRASH_ACK", a.as_str()));
        }
        let w = child("work", &dir, &env);
        let werr = String::from_utf8_lossy(&w.stderr).into_owned();
        let wout = String::from_utf8_lossy(&w.stdout).into_owned();
        #[cfg(unix)]
        let killed = std::os::unix::process::ExitStatusExt::signal(&w.status) == Some(9);
        #[cfg(not(unix))]
        let killed = !w.status.success();
        if !(killed && werr.contains("walpin crash point")) {
            failures.push(format!("{}: point never fired\n{}", c.name, text(&w)));
            continue;
        }
        let acked = line(&wout, "ack ").map_or(0, |f| f[1].parse::<i64>().unwrap());
        let want = acked + i64::from(c.inflight_present);
        // Mutants first, on copies of the crashed directory.
        let mut mutant_killed = [false; 2];
        for (m, name) in ["newer_only", "wal0_only"].iter().enumerate() {
            let copy = tmp.path().join(name);
            copy_dir(&dir, &copy);
            let out = child("verify", &copy, &[("TURSO_WALPIN_WAL2_MUTANT", *name)]);
            mutant_killed[m] = verified(&out) != Some((want, true, want));
        }
        let v = child("verify", &dir, &[]);
        let vout = String::from_utf8_lossy(&v.stdout).into_owned();
        let decision = line(&vout, "decision ").map_or("?".to_string(), |f| f[1].to_string());
        let got = verified(&v);
        if got != Some((want, true, want)) {
            failures.push(format!(
                "{}: acked {acked}, want exactly 1..={want} complete with meta {want}, got {got:?}\n{}",
                c.name,
                text(&v)
            ));
        }
        let cont = child("continue", &dir, &[]);
        let cout = String::from_utf8_lossy(&cont.stdout).into_owned();
        let switches = line(&cout, "continued ").and_then(|f| f[4].parse::<u64>().ok());
        if !cont.status.success() || switches.unwrap_or(0) < 1 {
            failures.push(format!(
                "{}: continuation failed or made no switch\n{}",
                c.name,
                text(&cont)
            ));
        }
        let after = verified(&child("verify", &dir, &[]));
        let want_after = want + CONTINUE_TXNS;
        if after != Some((want_after, true, want_after)) {
            failures.push(format!(
                "{}: after continuing, want 1..={want_after}, got {after:?}",
                c.name
            ));
        }
        if c.m1_must_kill && !mutant_killed[0] {
            failures.push(format!(
                "{}: M1 (newer_only) SURVIVED where it must be killed",
                c.name
            ));
        }
        table.push(format!(
            "{:<24} acked {:>5} want {:>5} decision {} M1 {} M0 {} continued {:?}",
            c.name,
            acked,
            want,
            decision,
            if mutant_killed[0] {
                "killed"
            } else {
                "survived"
            },
            if mutant_killed[1] {
                "killed"
            } else {
                "survived"
            },
            switches
        ));
    }
    eprintln!("wal2 crash table ({} configurations):", table.len());
    for row in &table {
        eprintln!("  {row}");
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(table.len(), 22, "every registered configuration ran");
}
