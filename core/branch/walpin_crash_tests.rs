//! r11-walpin-conc amendments 21 and 21a: durable wal2 (FW2). Each configuration runs a workload in
//! a child process of this test binary, with FW2 on, and kills that child at one crash point.
//! Further children then reopen the directory. The recovered database must hold every acknowledged
//! transaction, the in-flight one exactly as registered, and nothing else, and must keep working
//! across another switch. Mutants must fail the same check where they are registered to: M1 (keep
//! only the newer file where both would be recovered) where the older file still holds commits the
//! database file lacks, M2 (a restart empties -wal2 before -wal) at the restart's first step, M3
//! (no salt check) where the older file lost its last commit, M4 (no rule 1) where TRUNCATE left
//! an empty -wal beside an older -wal2 (amendment 25).
//!
//! Children are driven by `WAL2_CRASH_MODE` and are no-ops without it, so `wal2_crash_child` passes
//! trivially in an ordinary run. A child writes its results to the file named by `WAL2_CRASH_OUT`,
//! not to stdout, where libtest's own "test ... " line shares the first line. The engine's crash
//! points and mutants are `walpin::crash` (test builds only).

use super::walpin;
use crate::storage::wal::{Wal2FileScan, Wal2Recovered};
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

const CHILD: &str = "branch::walpin_crash_tests::wal2_crash_child";
const ROWS_PER_TXN: i64 = 8;
const MAX_TXNS: i64 = 6_000;
const CONTINUE_TXNS: i64 = 400;
const CHILD_LIMIT: std::time::Duration = std::time::Duration::from_secs(300);
const VERIFY_MUTANTS: [&str; 5] = [
    "newer_only",
    "wal0_only",
    "no_salt_check",
    "no_rule1",
    "uncommitted_tail",
];

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

/// Append one result line to `WAL2_CRASH_OUT`. A write(2) reaches the kernel before the next line
/// of the workload runs, so it survives this process's SIGKILL.
fn say(line: &str) {
    let path = std::env::var("WAL2_CRASH_OUT").unwrap();
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap()
        .write_all(format!("{line}\n").as_bytes())
        .unwrap();
}

/// `WAL2_CRASH_ACK=<k>:<m>`: kill this child after the m-th COMMIT made while the switch count is
/// k has returned, before its ack. `restart:<m>` counts COMMITs after the arm's checkpoint;
/// `truncated:0` kills right after the arm's checkpoint returns, before the next transaction.
fn ack_point() -> Option<(String, u64)> {
    let v = std::env::var("WAL2_CRASH_ACK").ok()?;
    let (k, m) = v.rsplit_once(':')?;
    Some((k.to_string(), m.parse().ok()?))
}

/// The workload. Arms: `plain`; `pinned` (a reader holds a snapshot from before switch 1, so the
/// old file's checkpoint is refused); `restart` (a RESTART checkpoint 10 commits after switch 1,
/// while -wal2 is the current file); `truncate` (a TRUNCATE checkpoint 10 commits after switch 2).
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
    let (mut checkpointed, mut since_checkpoint) = (false, 0u64);
    for i in 1..=MAX_TXNS {
        if arm == "pinned"
            && !pinned
            && walpin::counters().fw2_switches == 0
            && db.walpin_max_frame() >= 900
        {
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
        if checkpointed {
            since_checkpoint += 1;
        }
        match &ack {
            Some((k, m)) if k == "restart" && checkpointed && since_checkpoint == *m => {
                walpin::crash::kill_self(&format!("acked restart:{m} (txn {i})"))
            }
            Some((k, m)) if k.parse::<u64>().ok() == Some(sw) && since_switch == *m => {
                walpin::crash::kill_self(&format!("acked {k}:{m} (txn {i})"))
            }
            _ => {}
        }
        say(&format!("ack {i}"));
        let mode = match arm.as_str() {
            "restart" if sw == 1 => Some("RESTART"),
            "truncate" if sw == 2 => Some("TRUNCATE"),
            _ => None,
        };
        if let Some(mode) = mode {
            if !checkpointed && since_switch == 10 {
                conn.execute(format!("PRAGMA wal_checkpoint({mode})"))
                    .unwrap();
                assert_eq!(db.walpin_max_frame(), 0, "{mode} restarted the log");
                checkpointed = true;
                say(&format!("{mode} after {i}"));
                if matches!(&ack, Some((k, _)) if k == "truncated") {
                    walpin::crash::kill_self(&format!("truncated after txn {i}"));
                }
            }
        }
    }
    say("no crash point fired");
}

/// Record what the recovered database holds: the decision, the present transactions (they must be
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
    let check = conn
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    let first = match check.first().and_then(|r| r.first()) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        other => format!("{other:?}"),
    };
    say(&format!(
        "integrity {}",
        first.replace(char::is_whitespace, "_")
    ));
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
        say(&format!("ack {i}"));
    }
    say(&format!(
        "continued to {} switches {}",
        last + CONTINUE_TXNS,
        walpin::counters().fw2_switches
    ));
}

/// Amendment 25, in a child with FW2 on: an open with the multiprocess WAL and a reload after an
/// external restore are refused; a plain open under FW2 is the control.
fn refusals(dir: &str) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let path = Path::new(dir).join("mp.db");
    let mp = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_multiprocess_wal(true),
        None,
        Arc::new(SqliteDialect),
    );
    say(&match &mp {
        Err(e) => format!("multiprocess refused: {e}"),
        Ok(_) => "multiprocess opened".to_string(),
    });
    drop(mp);
    let db = open(dir);
    say("plain opened");
    // `reload_wal_after_external_restore` exists only with the conn_raw_api feature, which the
    // registered lib-test build does not enable (amendment 25b): then this guard is UNTESTED, said
    // so on its own line.
    #[cfg(feature = "conn_raw_api")]
    say(&match db.reload_wal_after_external_restore() {
        Err(e) => format!("reload refused: {e}"),
        Ok(()) => "reload reloaded".to_string(),
    });
    #[cfg(not(feature = "conn_raw_api"))]
    {
        drop(db);
        say("reload UNTESTED: conn_raw_api is off in this build");
    }
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
        "refusals" => refusals(&dir),
        other => panic!("unknown WAL2_CRASH_MODE {other}"),
    }
}

/// A child's exit and its result lines.
struct Ran {
    out: Output,
    results: String,
}

impl Ran {
    fn text(&self) -> String {
        format!(
            "status {:?}\n--- results\n{}--- stdout\n{}--- stderr\n{}",
            self.out.status,
            self.results,
            String::from_utf8_lossy(&self.out.stdout),
            String::from_utf8_lossy(&self.out.stderr)
        )
    }

    /// The last result line starting with `key`, split on whitespace.
    fn line(&self, key: &str) -> Option<Vec<&str>> {
        self.results
            .lines()
            .rev()
            .find(|l| l.starts_with(key))
            .map(|l| l.split_whitespace().collect())
    }

    /// The verify child's integrity_check result ("ok" when clean).
    fn integrity(&self) -> Option<String> {
        self.line("integrity ").map(|f| f[1].to_string())
    }

    /// How a mutant verify failed, for the table: survived, a wrong answer, or its exit and first
    /// panic line.
    fn kill_kind(&self, good: Option<(i64, bool, i64)>) -> String {
        match self.verified() {
            g if g == good && self.integrity().as_deref() == Some("ok") => "survived".to_string(),
            Some(g) => format!("killed(wrong {g:?} integrity {:?})", self.integrity()),
            None => {
                let err = String::from_utf8_lossy(&self.out.stderr).into_owned();
                let panic = err
                    .lines()
                    .find(|l| l.contains("panicked"))
                    .unwrap_or("")
                    .chars()
                    .take(120)
                    .collect::<String>();
                format!("killed(exit {:?}: {panic})", self.out.status.code())
            }
        }
    }

    /// (present, complete, meta) from a verify child, None if it did not finish.
    fn verified(&self) -> Option<(i64, bool, i64)> {
        if !self.out.status.success() {
            return None;
        }
        let f = self.line("present ")?;
        Some((f[1].parse().ok()?, f[3] == "true", f[5].parse().ok()?))
    }
}

/// Run one child on `dir`, its outputs in files beside it named `<dir>.<tag>.*`. The child is
/// killed after CHILD_LIMIT, so a child that hangs (a mutant reading an inconsistent tree) fails its
/// configuration instead of the whole test.
fn child(mode: &str, dir: &Path, tag: &str, env: &[(&str, &str)]) -> Ran {
    let beside = |ext: &str| PathBuf::from(format!("{}.{tag}.{ext}", dir.display()));
    let (res, out_path, err_path) = (beside("res"), beside("out"), beside("err"));
    let _ = std::fs::remove_file(&res);
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([CHILD, "--exact", "--nocapture", "--test-threads=1"])
        .env("TURSO_WALPIN_FIX", "fw2")
        .env("WAL2_CRASH_MODE", mode)
        .env("WAL2_CRASH_DIR", dir)
        .env("WAL2_CRASH_OUT", &res)
        .env_remove("TURSO_WALPIN_CRASH")
        .env_remove("TURSO_WALPIN_WAL2_MUTANT")
        .env_remove("WAL2_CRASH_ACK")
        .env_remove("WAL2_CRASH_ARM");
    for (k, v) in env {
        cmd.env(k, v);
    }
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
    Ran {
        out: Output {
            status,
            stdout: std::fs::read(&out_path).unwrap(),
            stderr: std::fs::read(&err_path).unwrap(),
        },
        results: std::fs::read_to_string(&res).unwrap_or_default(),
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// How the parent damages the crashed directory before any verify, to model a write that never
/// reached the disk (amendments 21a and 25).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Cut {
    None,
    /// -wal loses its last commit: cut after its second-to-last commit frame (tail_lost).
    LastCommit,
    /// -wal's last transaction is torn: every frame of it except its commit frame survives
    /// (amendment 25b: the most a torn write can leave, so a replay of it is visible).
    TornOlder,
    /// -wal2's only transaction is torn the same way: all its frames but the commit frame.
    TornNewer,
}

/// Apply `cut` to the WAL files in `dir`; returns the frames removed. Frames are counted only
/// while their salts match the file header's (the current generation).
fn apply_cut(dir: &Path, cut: Cut) -> u64 {
    if cut == Cut::None {
        return 0;
    }
    let wal = dir.join(if cut == Cut::TornNewer {
        "crash.db-wal2"
    } else {
        "crash.db-wal"
    });
    let bytes = std::fs::read(&wal).unwrap();
    let page_size = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let frame = 24 + page_size;
    let (salt1, salt2) = (&bytes[16..20], &bytes[20..24]);
    let (mut ends, mut commits) = (Vec::new(), Vec::new());
    let mut off = 32;
    while off + frame <= bytes.len() {
        let h = &bytes[off..off + 24];
        if &h[8..12] != salt1 || &h[12..16] != salt2 {
            break;
        }
        ends.push(off + frame);
        if h[4..8] != [0, 0, 0, 0] {
            commits.push(ends.len() - 1);
        }
        off += frame;
    }
    let keep = match cut {
        Cut::LastCommit | Cut::TornOlder => {
            assert!(
                commits.len() >= 2,
                "the file holds two commits to cut between"
            );
            let prev = commits[commits.len() - 2];
            let last = commits[commits.len() - 1];
            if cut == Cut::LastCommit {
                ends[prev]
            } else {
                assert!(
                    last - prev >= 2,
                    "the last transaction spans two frames or more"
                );
                ends[last - 1]
            }
        }
        Cut::TornNewer => {
            assert!(
                commits.len() == 1 && commits[0] >= 1,
                "-wal2 holds one transaction of two frames or more: commits at {commits:?}"
            );
            ends[commits[0] - 1]
        }
        Cut::None => unreachable!(),
    };
    let f = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
    f.set_len(keep as u64).unwrap();
    ((ends[ends.len() - 1] - keep) / frame) as u64
}

struct Config {
    name: String,
    arm: &'static str,
    engine: Option<String>,
    ack: Option<String>,
    /// P - A: +1 the in-flight transaction is present, 0 absent, -1 the last ack was cut away.
    offset: i64,
    /// A mutant in the WORK child (M2); the correct recovery must then FAIL the check.
    work_mutant: Option<&'static str>,
    cut: Cut,
    /// Amendment 25: crash the continuation too, at its own first switch's commit_written.
    recrash: bool,
    /// Verify mutants that must fail the check here.
    must_kill: &'static [&'static str],
}

fn config(
    name: String,
    arm: &'static str,
    engine: Option<String>,
    ack: Option<String>,
    offset: i64,
) -> Config {
    Config {
        name,
        arm,
        engine,
        ack,
        offset,
        work_mutant: None,
        cut: Cut::None,
        recrash: false,
        must_kill: &[],
    }
}

/// The 32 registered configurations (PREREG amendments 21, 21a and 25).
fn configs() -> Vec<Config> {
    let mut v = Vec::new();
    for k in 1..=3u64 {
        for (point, offset) in [
            ("switched", 0),
            ("header", 0),
            ("commit_written", 1),
            ("ckpt_copied", 1),
            ("ckpt_done", 1),
        ] {
            let mut c = config(
                format!("{point}:{k}"),
                "plain",
                Some(format!("{point}:{k}")),
                None,
                offset,
            );
            if point == "commit_written" {
                c.must_kill = &["newer_only"];
            }
            v.push(c);
        }
        v.push(config(
            format!("acked:{k}"),
            "plain",
            None,
            Some(format!("{k}:1")),
            1,
        ));
    }
    let mut pinned = config(
        "pinned acked:1:20".into(),
        "pinned",
        None,
        Some("1:20".into()),
        1,
    );
    pinned.must_kill = &["newer_only"];
    v.push(pinned);
    for point in ["restart_first", "restart_second"] {
        v.push(config(
            format!("restart {point}:1"),
            "restart",
            Some(format!("{point}:1")),
            None,
            0,
        ));
    }
    v.push(config(
        "restart acked:5".into(),
        "restart",
        None,
        Some("restart:5".into()),
        1,
    ));
    v.push(config(
        "truncate acked:5".into(),
        "truncate",
        None,
        Some("restart:5".into()),
        1,
    ));
    let mut tail = config(
        "tail_lost commit_written:1".into(),
        "plain",
        Some("commit_written:1".into()),
        None,
        -1,
    );
    tail.cut = Cut::LastCommit;
    tail.must_kill = &["no_salt_check"];
    v.push(tail);
    let mut m2 = config(
        "M2 restart_first:1".into(),
        "restart",
        Some("restart_first:1".into()),
        None,
        0,
    );
    m2.work_mutant = Some("wal2_first");
    v.push(m2);
    // Amendment 25.
    let mut truncated = config(
        "truncate truncated".into(),
        "truncate",
        None,
        Some("truncated:0".into()),
        0,
    );
    truncated.must_kill = &["no_rule1"];
    v.push(truncated);
    for (name, arm, point, offset, cut) in [
        (
            "recrash commit_written:1",
            "plain",
            "commit_written:1",
            1,
            Cut::None,
        ),
        ("recrash header:2", "plain", "header:2", 0, Cut::None),
        (
            "recrash tail_lost",
            "plain",
            "commit_written:1",
            -1,
            Cut::LastCommit,
        ),
        (
            "recrash restart_first:1",
            "restart",
            "restart_first:1",
            0,
            Cut::None,
        ),
    ] {
        let mut c = config(name.into(), arm, Some(point.into()), None, offset);
        c.cut = cut;
        c.recrash = true;
        v.push(c);
    }
    let mut torn_newer = config(
        "torn_newer commit_written:1".into(),
        "plain",
        Some("commit_written:1".into()),
        None,
        0,
    );
    torn_newer.cut = Cut::TornNewer;
    torn_newer.must_kill = &["uncommitted_tail"];
    v.push(torn_newer);
    let mut torn_older = config(
        "torn_older commit_written:1".into(),
        "plain",
        Some("commit_written:1".into()),
        None,
        -1,
    );
    torn_older.cut = Cut::TornOlder;
    torn_older.must_kill = &["uncommitted_tail"];
    v.push(torn_older);
    v
}

/// PREREG amendments 21, 21a and 25: at every registered crash point, recovery keeps every
/// acknowledged transaction and the in-flight one as registered, nothing else, complete and
/// integrity-clean; the database keeps working across another switch, a second crash and a reopen;
/// each mutant is killed where it is registered to be.
#[test]
#[ignore = "spawns ~300 child processes of this binary (FW2 on in each); run explicitly"]
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
        if let Some(m) = c.work_mutant {
            env.push(("TURSO_WALPIN_WAL2_MUTANT", m));
        }
        let w = child("work", &dir, "work", &env);
        #[cfg(unix)]
        let killed = std::os::unix::process::ExitStatusExt::signal(&w.out.status) == Some(9);
        #[cfg(not(unix))]
        let killed = !w.out.status.success();
        let fired = String::from_utf8_lossy(&w.out.stderr).contains("walpin crash point");
        if !(killed && fired) {
            failures.push(format!("{}: point never fired\n{}", c.name, w.text()));
            continue;
        }
        let acked = w.line("ack ").map_or(0, |f| f[1].parse::<i64>().unwrap());
        let cut = apply_cut(&dir, c.cut);
        let want = acked + c.offset;
        let good = Some((want, true, want));
        // Verify mutants first, each on its own copy of the crashed directory.
        let mut mutants = Vec::new();
        for name in VERIFY_MUTANTS {
            let copy = tmp.path().join(name);
            copy_dir(&dir, &copy);
            let out = child(
                "verify",
                &copy,
                "verify",
                &[("TURSO_WALPIN_WAL2_MUTANT", name)],
            );
            let kind = out.kill_kind(good);
            let dead = kind != "survived";
            if c.must_kill.contains(&name) && !dead {
                failures.push(format!(
                    "{}: {name} SURVIVED where it must be killed",
                    c.name
                ));
            }
            mutants.push(format!("{name} {kind}"));
        }
        let v = child("verify", &dir, "verify", &[]);
        let decision = v
            .line("decision ")
            .map_or("?".to_string(), |f| f[1].to_string());
        let got = v.verified();
        let integrity = v.integrity();
        let mut continued = None;
        let mut recrash_note = String::new();
        if c.work_mutant.is_some() {
            // M2: the correct recovery over the mutant's crash state must fail the check.
            if got == good && integrity.as_deref() == Some("ok") {
                failures.push(format!("{}: M2 SURVIVED where it must be killed", c.name));
            }
        } else {
            if got != good || integrity.as_deref() != Some("ok") {
                failures.push(format!(
                    "{}: acked {acked}, want exactly 1..={want} complete with meta {want} and integrity ok, got {got:?} {integrity:?}\n{}",
                    c.name,
                    v.text()
                ));
            }
            let mut base = want;
            if c.recrash {
                // A second crash, at the continuation's own first switch (commit_written:1).
                let rc = child(
                    "continue",
                    &dir,
                    "recrash",
                    &[("TURSO_WALPIN_CRASH", "commit_written:1")],
                );
                #[cfg(unix)]
                let rkilled =
                    std::os::unix::process::ExitStatusExt::signal(&rc.out.status) == Some(9);
                #[cfg(not(unix))]
                let rkilled = !rc.out.status.success();
                let rfired = String::from_utf8_lossy(&rc.out.stderr).contains("walpin crash point");
                let racked = rc
                    .line("ack ")
                    .map_or(want, |f| f[1].parse::<i64>().unwrap());
                let rwant = racked + 1;
                let rv = child("verify", &dir, "reverify", &[]);
                let rgot = rv.verified();
                recrash_note = format!(
                    " recrash acked {racked} want {rwant} got {rgot:?} decision {}",
                    rv.line("decision ")
                        .map_or("?".to_string(), |f| f[1].to_string())
                );
                if !(rkilled && rfired) {
                    failures.push(format!(
                        "{}: the continuation's crash point never fired\n{}",
                        c.name,
                        rc.text()
                    ));
                } else if rgot != Some((rwant, true, rwant))
                    || rv.integrity().as_deref() != Some("ok")
                {
                    failures.push(format!(
                        "{}: after the second crash, want 1..={rwant}, got {rgot:?} {:?}\n{}",
                        c.name,
                        rv.integrity(),
                        rv.text()
                    ));
                }
                base = rwant;
            }
            let cont = child("continue", &dir, "continue", &[]);
            continued = cont
                .line("continued ")
                .and_then(|f| f[4].parse::<u64>().ok());
            if !cont.out.status.success() || continued.unwrap_or(0) < 1 {
                failures.push(format!(
                    "{}: continuation failed or made no switch\n{}",
                    c.name,
                    cont.text()
                ));
            }
            let after = child("verify", &dir, "after", &[]);
            let want_after = base + CONTINUE_TXNS;
            if after.verified() != Some((want_after, true, want_after))
                || after.integrity().as_deref() != Some("ok")
            {
                failures.push(format!(
                    "{}: after continuing, want 1..={want_after}, got {:?} {:?}",
                    c.name,
                    after.verified(),
                    after.integrity()
                ));
            }
        }
        table.push(format!(
            "{:<28} acked {:>5} cut {cut} want {:>5} got {got:?} integrity {integrity:?} decision {decision}{recrash_note} | {} | continued-switches {continued:?}",
            c.name,
            acked,
            want,
            mutants.join("; ")
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
    assert_eq!(table.len(), 32, "every registered configuration ran");
}

/// Amendment 25: with FW2 off, an open refuses a non-empty `<db>-wal2`, as SQLite refuses a wal2
/// database to other builds; an empty one (the control) is accepted.
#[test]
fn refusal_fw2_off_open_refuses_a_nonempty_wal2() {
    assert!(!walpin::fw2(), "run without TURSO_WALPIN_FIX=fw2");
    for (bytes, refused) in [(0usize, false), (100, true)] {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("crash.db");
        std::fs::write(format!("{}-wal2", path.display()), vec![7u8; bytes]).unwrap();
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let r = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        );
        match (r, refused) {
            (Err(e), true) => assert!(e.to_string().contains("wal2"), "{e}"),
            (Ok(_), false) => {}
            (r, _) => panic!(
                "a {bytes}-B -wal2: want refused={refused}, got {:?}",
                r.map(|_| ())
            ),
        }
    }
}

/// Amendment 25: with FW2 off at open, `walpin_open_wal2` refuses a `-wal2` holding more than a
/// header (it was never recovered); a header-only one (the control) is accepted.
#[test]
fn refusal_walpin_open_wal2_refuses_an_unrecovered_wal2() {
    assert!(!walpin::fw2(), "run without TURSO_WALPIN_FIX=fw2");
    for (bytes, refused) in [(32usize, false), (4_200, true)] {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = open(tmp.path().to_str().unwrap());
        let wal2 = tmp.path().join("crash.db-wal2");
        std::fs::write(&wal2, vec![7u8; bytes]).unwrap();
        match (db.walpin_open_wal2(), refused) {
            (Err(e), true) => assert!(e.to_string().contains("not recovered"), "{e}"),
            (Ok(()), false) => {}
            (r, _) => panic!("a {bytes}-B -wal2: want refused={refused}, got {r:?}"),
        }
    }
}

/// Amendment 25: under FW2, the multiprocess WAL and a reload after an external restore are refused
/// (in a child, since FW2 is a process switch); a plain FW2 open is the control.
#[test]
#[ignore = "spawns a child of this binary with FW2 on; run explicitly"]
fn refusal_fw2_refuses_multiprocess_wal_and_external_restore_reload() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().join("db");
    std::fs::create_dir_all(&dir).unwrap();
    let r = child("refusals", &dir, "refusals", &[]);
    assert!(r.out.status.success(), "{}", r.text());
    let mp = r.line("multiprocess ").map(|f| f.join(" "));
    assert!(
        mp.as_deref()
            .is_some_and(|l| l.contains("refused") && l.contains("FW2")),
        "{}",
        r.text()
    );
    assert!(r.line("plain opened").is_some(), "{}", r.text());
    let reload = r.line("reload ").map(|f| f.join(" "));
    let want_reload = if cfg!(feature = "conn_raw_api") {
        "refused"
    } else {
        "UNTESTED"
    };
    assert!(
        reload.as_deref().is_some_and(
            |l| l.contains(want_reload) && (want_reload == "UNTESTED" || l.contains("FW2"))
        ),
        "{}",
        r.text()
    );
}

/// Amendment 21a: `Wal2Recovered::decide` against SQLite's walIndexRecover rules (wal.c 2001-2035),
/// one case per branch. The expected values are read off those rules, not computed by `decide`.
#[test]
fn wal2_recovery_decision_follows_sqlites_rules() {
    let f =
        |valid: bool, seq: u32, salts: (u32, u32), max_frame: u64, last: (u32, u32)| Wal2FileScan {
            valid,
            seq,
            salts,
            max_frame,
            last_checksum: last,
        };
    let (ck0, ck1) = ((11, 12), (21, 22));
    let cases = [
        // -wal2 follows -wal: both iff -wal2 has a commit and its salts are -wal's last checksum.
        (
            "wal2 follows, chained",
            f(true, 5, (1, 2), 10, ck0),
            f(true, 6, ck0, 3, ck1),
            Wal2Recovered::Both { older: 0 },
        ),
        (
            "wal2 follows, salts differ",
            f(true, 5, (1, 2), 10, ck0),
            f(true, 6, (9, 9), 3, ck1),
            Wal2Recovered::Only(0),
        ),
        (
            "wal2 follows, no commit",
            f(true, 5, (1, 2), 10, ck0),
            f(true, 6, ck0, 0, ck1),
            Wal2Recovered::Only(0),
        ),
        // -wal follows -wal2: the same the other way round; otherwise -wal2 alone.
        (
            "wal follows, chained",
            f(true, 6, ck1, 3, ck0),
            f(true, 5, (1, 2), 10, ck1),
            Wal2Recovered::Both { older: 1 },
        ),
        (
            "wal follows, salts differ",
            f(true, 6, (9, 9), 3, ck0),
            f(true, 5, (1, 2), 10, ck1),
            Wal2Recovered::Only(1),
        ),
        (
            "wal follows, no commit",
            f(true, 6, ck1, 0, ck0),
            f(true, 5, (1, 2), 10, ck1),
            Wal2Recovered::Only(1),
        ),
        // Fallback: counters not adjacent, the lower counter's file alone; invalid is highest.
        (
            "not adjacent, wal lower",
            f(true, 3, (1, 2), 10, ck0),
            f(true, 9, ck0, 3, ck1),
            Wal2Recovered::Only(0),
        ),
        (
            "not adjacent, wal2 lower",
            f(true, 9, ck1, 3, ck0),
            f(true, 3, (1, 2), 10, ck1),
            Wal2Recovered::Only(1),
        ),
        (
            "wal invalid",
            f(false, 0, (0, 0), 0, (0, 0)),
            f(true, 3, (1, 2), 10, ck1),
            Wal2Recovered::Only(1),
        ),
        (
            "wal2 invalid",
            f(true, 3, (1, 2), 10, ck0),
            f(false, 0, (0, 0), 0, (0, 0)),
            Wal2Recovered::Only(0),
        ),
        (
            "both invalid",
            f(false, 0, (0, 0), 0, (0, 0)),
            f(false, 0, (0, 0), 0, (0, 0)),
            Wal2Recovered::Only(0),
        ),
        // turso's u32 counter wraps where SQLite's 4-bit one does.
        (
            "wal2 follows across the wrap",
            f(true, u32::MAX, (1, 2), 10, ck0),
            f(true, 0, ck0, 3, ck1),
            Wal2Recovered::Both { older: 0 },
        ),
    ];
    for (what, f0, f1, want) in cases {
        assert_eq!(Wal2Recovered::decide(&f0, &f1), want, "{what}");
    }
}

/// Amendment 26/26c: the birth gate (FWB) keeps no pre-image of a page past every live child's
/// fork-time database size, keeps one for a page AT or within a child's size, forgets a reaped
/// child's size, and changes nothing when switched off. The expected counts are read off that rule:
/// child a forks at size 10, the trunk first-writes 5, 10, 11; child b forks at size 20, the trunk
/// first-writes 15, 20, 25; b is reaped (its retained versions stay: a's fork epoch still lies in
/// their range); the trunk first-writes 18. Gated: 5, 10 (a), 15, 20 (b) = 4, and 18 is past a's 10
/// once b's size is gone. Ungated: every one of the 7 writes keeps a copy.
#[test]
fn fwb_birth_gate_skips_only_pages_past_every_fork_size() {
    let before = walpin::fwb();
    let run = |gate: bool| {
        walpin::set_birth_gate(gate);
        let store = super::store::BranchStore::new();
        let schema = || Arc::new(crate::schema::Schema::default());
        let page = vec![0u8; 4096];
        let _a = store.fork_trunk_sized(schema(), 4096, 10).unwrap();
        for p in [5, 10, 11] {
            store.first_write_trunk(p, &page);
        }
        let b = store.fork_trunk_sized(schema(), 4096, 20).unwrap();
        for p in [15, 20, 25] {
            store.first_write_trunk(p, &page);
        }
        store.release_handle(b);
        store.first_write_trunk(18, &page);
        store.stats().arena_slots_in_use
    };
    let (on, off) = (run(true), run(false));
    walpin::set_birth_gate(before);
    assert_eq!(
        off, 7,
        "ungated: every first write with a live child keeps a copy"
    );
    assert_eq!(
        on, 4,
        "gated: pages 5, 10, 15 and 20, at or within a live child's fork-time size"
    );
}
