//! FW3 (r11-walpin): a branch read transaction holds no trunk WAL snapshot.
//!
//! Every value is read back through SQL on the branch's connection and compared with what the test
//! wrote, never with the store's bookkeeping. The race tests put a trunk write, a checkpoint or a
//! WAL restart into the one window the optimistic read has (between the frame lookup and the read)
//! through `fw3_test_hook`, and each is run again with the check it exercises switched off, to show
//! that the check, and not luck, is what keeps the branch's value.

use super::walpin;
use super::*;
use crate::storage::pager::fw3_test_hook;
use crate::{Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, Result, SqliteDialect, Value, IO};

const ROWS: i64 = 200;

fn open_db(fw3: bool) -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("walpin.db");
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
    db.walpin_set_fw3(fw3);
    (dir, db)
}

/// ~100-byte rows, so 200 of them span several leaves under one root.
fn original(id: i64) -> String {
    format!("trunk-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>) {
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=ROWS {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

/// The row's value, or None when the read fails, finds no row, or panics (a mutant can hand the
/// b-tree another page's bytes).
fn try_value(conn: &Arc<Connection>, id: i64) -> Option<String> {
    let conn = conn.clone();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let rows = conn
            .prepare(format!("SELECT v FROM t WHERE id = {id}"))
            .ok()?
            .run_collect_rows()
            .ok()?;
        match rows.as_slice() {
            [row] => match &row[0] {
                Value::Text(t) => Some(t.as_str().to_string()),
                _ => None,
            },
            _ => None,
        }
    }))
    .ok()
    .flatten()
}

fn value(conn: &Arc<Connection>, id: i64) -> String {
    try_value(conn, id).unwrap_or_else(|| panic!("row {id} did not read back"))
}

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
}

struct PinOutcome {
    readers_on_marks_1_to_4: u32,
    restarts_during: u32,
    max_frame_after: u64,
}

/// A branch holds `BEGIN; SELECT` open while the trunk rewrites every row 5.5 times over (1,100
/// one-row commits, past the 1,000-frame auto-checkpoint threshold).
fn pin_scenario(fw3: bool) -> PinOutcome {
    let (_dir, db) = open_db(fw3);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    set(&trunk, 1, "before-fork");
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(value(&conn, ROWS), original(ROWS));
    let pinned = db.walpin_stats();
    for i in 0..1_100 {
        set(&trunk, (i % ROWS) + 1, &format!("g{i}"));
    }
    let after = db.walpin_stats();
    // The branch still reads its fork's values, inside the transaction and after it.
    assert_eq!(value(&conn, ROWS), original(ROWS));
    assert_eq!(value(&conn, 1), "before-fork");
    assert_eq!(value(&conn, 2), original(2));
    conn.execute("COMMIT").unwrap();
    assert_eq!(value(&conn, ROWS), original(ROWS));
    assert_eq!(value(&conn, 1), "before-fork");
    PinOutcome {
        readers_on_marks_1_to_4: pinned.mark_readers[1..].iter().sum(),
        restarts_during: after.checkpoint_seq.wrapping_sub(pinned.checkpoint_seq),
        max_frame_after: after.max_frame,
    }
}

#[test]
fn an_open_branch_transaction_pins_the_trunk_wal_without_fw3() {
    // The control: without FW3 the branch holds a read mark, no restart happens, and the WAL keeps
    // every frame. Without this, the FW3 test below could pass on a harness that never pins.
    let out = pin_scenario(false);
    assert_eq!(out.readers_on_marks_1_to_4, 1, "the branch's read tx holds one mark");
    assert_eq!(out.restarts_during, 0, "a pinned WAL cannot restart");
    assert!(out.max_frame_after > 1_100, "every frame stays: {}", out.max_frame_after);
}

#[test]
fn with_fw3_an_open_branch_transaction_pins_nothing() {
    let out = pin_scenario(true);
    assert_eq!(out.readers_on_marks_1_to_4, 0, "a branch read tx takes no trunk mark");
    assert!(out.restarts_during >= 1, "the WAL restarted under the open branch transaction");
    assert!(
        out.max_frame_after <= 1_001,
        "the WAL holds only frames since the last restart: {}",
        out.max_frame_after
    );
}

/// The window, database-file case: the branch finds no frame of the leaf (the WAL is empty), and
/// before it reads the database file the trunk rewrites that leaf and a PASSIVE checkpoint copies
/// the new version into the file. PASSIVE, not TRUNCATE: a restart would bump the WAL generation
/// and the generation check would catch the window too, hiding whether the store check does
/// (PREREG amendment 4). Returns what the branch read and the retries it took.
fn trunk_write_in_the_window(mutant: u8) -> (Option<String>, u64) {
    let (_dir, db) = open_db(true);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    // Warm the root and the first leaf, so the next trunk-page read is the last row's leaf.
    assert_eq!(value(&conn, 1), original(1));
    let trunk2 = trunk.clone();
    let db2 = db.clone();
    let window = Arc::new(std::sync::Mutex::new(None));
    let seen = window.clone();
    fw3_test_hook::set(Box::new(move |_page| {
        let before = db2.walpin_stats();
        set(&trunk2, ROWS, "written-in-the-window");
        trunk2.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        let after = db2.walpin_stats();
        *seen.lock().unwrap() = Some((before.checkpoint_seq, after.checkpoint_seq, after.nbackfills));
    }));
    fw3_test_hook::set_mutant(mutant);
    let before = walpin::counters().fw3_retries;
    let got = try_value(&conn, ROWS);
    let retries = walpin::counters().fw3_retries - before;
    fw3_test_hook::set_mutant(0);
    let (seq_before, seq_after, nbackfills) = window
        .lock()
        .unwrap()
        .take()
        .expect("the hook ran inside the branch's read");
    assert_eq!(
        seq_before, seq_after,
        "the window's checkpoint must not restart the WAL, or the generation check sees it too"
    );
    assert!(nbackfills >= 1, "the post-fork frame reached the database file");
    // The trunk itself sees its own write.
    assert_eq!(value(&trunk, ROWS), "written-in-the-window");
    (got, retries)
}

#[test]
fn fw3_store_check_keeps_the_fork_version_when_the_trunk_writes_in_the_window() {
    let (got, retries) = trunk_write_in_the_window(0);
    assert_eq!(got.as_deref(), Some(original(ROWS).as_str()));
    assert!(retries >= 1, "the read was retried");
}

#[test]
fn without_the_store_check_the_window_leaks_the_trunks_write() {
    let (got, _) = trunk_write_in_the_window(1);
    assert_eq!(
        got.as_deref(),
        Some("written-in-the-window"),
        "mutant 1 must read the trunk's post-fork version, or this test cannot see the check"
    );
}

/// The window, WAL-frame case: the branch finds the leaf in WAL frame 1, and before it reads the
/// frame a TRUNCATE checkpoint restarts the WAL and the trunk writes ANOTHER page into the new
/// frame 1. Returns what the branch read and the retries it took.
fn restart_in_the_window(mutant: u8) -> (Option<String>, u64) {
    let (_dir, db) = open_db(true);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    set(&trunk, ROWS, "in-wal-at-fork");
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    assert_eq!(value(&conn, 1), original(1));
    let trunk2 = trunk.clone();
    fw3_test_hook::set(Box::new(move |_page| {
        trunk2.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        set(&trunk2, 1, "reuses-frame-1");
    }));
    fw3_test_hook::set_mutant(mutant);
    let before = walpin::counters().fw3_retries;
    let got = try_value(&conn, ROWS);
    let retries = walpin::counters().fw3_retries - before;
    fw3_test_hook::set_mutant(0);
    (got, retries)
}

#[test]
fn fw3_generation_check_keeps_the_fork_version_across_a_restart_in_the_window() {
    let (got, retries) = restart_in_the_window(0);
    assert_eq!(got.as_deref(), Some("in-wal-at-fork"));
    assert!(retries >= 1, "the read was retried");
}

#[test]
fn without_the_generation_check_a_restart_in_the_window_hands_back_another_page() {
    let (got, _) = restart_in_the_window(2);
    assert_ne!(
        got.as_deref(),
        Some("in-wal-at-fork"),
        "mutant 2 must read the reused frame, or this test cannot see the check"
    );
}

/// One SELECT of row `id` on `conn`: `Err` for an engine error (a Busy under contention), else the
/// value (None when no single text row came back).
fn select_once(conn: &Arc<Connection>, id: i64) -> Result<Option<String>> {
    let rows = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))?
        .run_collect_rows()?;
    Ok(match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => Some(t.as_str().to_string()),
            _ => None,
        },
        _ => None,
    })
}

struct ConcurrentOutcome {
    reads: u64,
    busy: u64,
    restarts: u32,
    retries: u64,
}

/// Branch readers on four threads against a trunk writer on a fifth, with the WAL's auto-checkpoint
/// and its restarts running underneath: every branch read must return its fork's value (all the
/// branches fork before the trunk writes anything, so that is the seeded value). Under FW3 no
/// branch read holds a mark, so checkpoints and restarts land while branches read.
fn concurrent_scenario(fw3: bool) -> ConcurrentOutcome {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    const THREADS: usize = 4;
    const WRITES: i64 = 5_000;
    const MIN_READS: u64 = 2_000;
    let (_dir, db) = open_db(fw3);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let branches: Vec<Branch> = (0..THREADS).map(|_| trunk.fork_branch().unwrap()).collect();
    let seq0 = db.walpin_stats().checkpoint_seq;
    let retries0 = walpin::counters().fw3_retries;
    let done = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let busy = AtomicU64::new(0);
    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..WRITES {
                let sql = format!("UPDATE t SET v = 'w{i}' WHERE id = {}", i % ROWS + 1);
                loop {
                    match trunk.execute(&sql) {
                        Ok(()) => break,
                        Err(LimboError::Busy) => {
                            busy.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("trunk write {i}: {e}"),
                    }
                }
            }
            done.store(true, Ordering::Release);
        });
        for (t, branch) in branches.iter().enumerate() {
            let (done, reads, busy) = (&done, &reads, &busy);
            s.spawn(move || {
                let conn = branch.connect().unwrap();
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                let mut n = 0u64;
                while n < MIN_READS || !done.load(Ordering::Acquire) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = (x % ROWS as u64) as i64 + 1;
                    match select_once(&conn, id) {
                        Ok(got) => {
                            assert_eq!(
                                got.as_deref(),
                                Some(original(id).as_str()),
                                "thread {t}, read {n}: row {id} is not its fork's value"
                            );
                            n += 1;
                        }
                        Err(LimboError::Busy) => {
                            busy.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("thread {t}, read {n}: {e}"),
                    }
                }
                reads.fetch_add(n, Ordering::Relaxed);
            });
        }
    });
    let after = db.walpin_stats();
    for id in [1, ROWS / 2, ROWS] {
        // Write i went to row i % ROWS + 1, so row id's last write is the largest such i.
        let last = (id - 1) + ROWS * ((WRITES - 1 - (id - 1)) / ROWS);
        assert_eq!(
            value(&trunk, id),
            format!("w{last}"),
            "the trunk reads its own last write of row {id}"
        );
    }
    ConcurrentOutcome {
        reads: reads.load(Ordering::Relaxed),
        busy: busy.load(Ordering::Relaxed),
        restarts: after.checkpoint_seq.wrapping_sub(seq0),
        retries: walpin::counters().fw3_retries - retries0,
    }
}

#[test]
fn concurrent_branch_readers_see_their_fork_without_fw3() {
    let out = concurrent_scenario(false);
    assert!(out.reads >= 4 * 2_000, "reads: {}", out.reads);
    eprintln!(
        "without FW3: reads {} busy {} restarts {} fw3_retries {}",
        out.reads, out.busy, out.restarts, out.retries
    );
}

#[test]
fn concurrent_branch_readers_see_their_fork_with_fw3() {
    let out = concurrent_scenario(true);
    assert!(out.reads >= 4 * 2_000, "reads: {}", out.reads);
    assert!(out.restarts >= 1, "the WAL restarted while branches read");
    eprintln!(
        "with FW3: reads {} busy {} restarts {} fw3_retries {}",
        out.reads, out.busy, out.restarts, out.retries
    );
}

/// The trunk's own rows survive checkpoints under whatever process switches are set
/// (`TURSO_WALPIN_FIX`): FW1 picks each page's frame from its frame log rather than a per-page scan,
/// and FW2 checkpoints and reuses whole files, so a wrong pick loses a committed row. A trunk
/// reader pins the WAL while 3,000 one-row commits land (every page gets many frames), is released,
/// 1,500 more land; then every row is read back through a fresh connection after a PASSIVE
/// checkpoint, and again after a TRUNCATE checkpoint, when the database file alone holds them.
/// Run once with no switch and once per switch.
#[test]
fn every_trunk_row_survives_checkpoints_under_the_process_switches() {
    let (_dir, db) = open_db(false);
    if walpin::fw2() {
        db.walpin_open_wal2().unwrap();
    }
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let mut model: Vec<String> = (0..=ROWS).map(original).collect();
    let pin = db.connect().unwrap();
    set(&trunk, 1, "before-pin");
    model[1] = "before-pin".to_string();
    pin.execute("BEGIN").unwrap();
    assert_eq!(value(&pin, 1), "before-pin");
    let write = |i: usize, model: &mut Vec<String>| {
        // Every third write hits row 7 (one hot page with a long frame list); the rest walk the table.
        let id = if i % 3 == 0 { 7 } else { (i as i64 * 37) % ROWS + 1 };
        let v = format!("g{i}");
        set(&trunk, id, &v);
        model[id as usize] = v;
    };
    for i in 0..3_000 {
        write(i, &mut model);
    }
    // The pinned reader still sees its snapshot.
    assert_eq!(value(&pin, 1), "before-pin");
    assert_eq!(value(&pin, 7), original(7));
    pin.execute("COMMIT").unwrap();
    for i in 3_000..4_500 {
        write(i, &mut model);
    }
    let check = |when: &str| {
        let fresh = db.connect().unwrap();
        for id in 1..=ROWS {
            assert_eq!(value(&fresh, id), model[id as usize], "{when}: row {id}");
        }
    };
    trunk.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    check("after PASSIVE");
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(db.walpin_stats().max_frame, 0, "TRUNCATE emptied the WAL");
    check("after TRUNCATE, from the database file alone");
    eprintln!(
        "switches fw1={} fw2={}: counters {:?}",
        walpin::fw1(),
        walpin::fw2(),
        walpin::counters()
    );
}

/// The window, truncation case: the branch finds the leaf in WAL frame 1, and before it reads the
/// frame a TRUNCATE checkpoint backfills it, restarts the WAL and truncates the file to nothing, so
/// the read fails for want of bytes (PREREG amendment 6). Returns what the branch read and the
/// retries it took.
fn truncation_in_the_window(mutant: u8) -> (Option<String>, u64) {
    let (_dir, db) = open_db(true);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    set(&trunk, ROWS, "in-wal-at-fork");
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    assert_eq!(value(&conn, 1), original(1));
    let trunk2 = trunk.clone();
    let db2 = db.clone();
    fw3_test_hook::set(Box::new(move |_page| {
        trunk2.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        assert_eq!(db2.walpin_stats().max_frame, 0, "the WAL is empty after TRUNCATE");
    }));
    fw3_test_hook::set_mutant(mutant);
    let before = walpin::counters().fw3_retries;
    let got = try_value(&conn, ROWS);
    let retries = walpin::counters().fw3_retries - before;
    fw3_test_hook::set_mutant(0);
    (got, retries)
}

#[test]
fn fw3_a_wal_truncated_in_the_window_is_retried_not_an_error() {
    let (got, retries) = truncation_in_the_window(0);
    assert_eq!(got.as_deref(), Some("in-wal-at-fork"));
    assert!(retries >= 1, "the failed read was retried");
}

#[test]
fn without_the_retry_a_wal_truncated_in_the_window_fails_the_read() {
    let (got, _) = truncation_in_the_window(3);
    assert_eq!(
        got, None,
        "mutant 3 must fail the read, or this test cannot see the truncation"
    );
}

/// r11-walpin-conc: a read whose generation check always fails (mutant 4, switched inside the
/// callee) gives up at the retry limit with Busy, and the per-call counters see exactly that: one
/// Busy call in the Busy bucket, 1,000 generation retries. This fires the one path the concurrent
/// arms predict never runs, so a zero there is a reading, not a blind spot. With the mutant off the
/// same read returns the fork value. The counters are process-global: run with --test-threads=1.
#[test]
fn fw3_a_read_that_never_validates_is_busy_after_the_retry_limit() {
    let (_dir, db) = open_db(true);
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    let before = walpin::counters();
    fw3_test_hook::set_mutant(4);
    let got = select_once(&conn, ROWS);
    fw3_test_hook::set_mutant(0);
    let after = walpin::counters();
    assert!(
        matches!(got, Err(LimboError::Busy)),
        "mutant 4 must exhaust the retries: {got:?}"
    );
    assert_eq!(after.fw3_busy - before.fw3_busy, 1, "one call gave up");
    assert_eq!(after.fw3_hist[9] - before.fw3_hist[9], 1, "in the Busy bucket");
    assert_eq!(
        after.fw3_retry_gen - before.fw3_retry_gen,
        1_000,
        "every attempt failed the generation check"
    );
    assert_eq!(after.fw3_retries - before.fw3_retries, 1_000);
    assert!(after.fw3_max_retries >= 1_000);
    drop(conn);
    let conn = branch.connect().unwrap();
    assert_eq!(value(&conn, ROWS), original(ROWS));
}

/// r11-walpin-conc amendment 5: a trunk reader that began on a fully backfilled WAL holds read mark
/// 0. Under turso's restart rule the next writer cannot upgrade mark 0 past it, so the log does not
/// restart (gate 3); under SQLite's rule (walRestartLog: mark 0 stays shared, marks 1..4 exclusive)
/// it restarts (gate 5). Under both rules the reader keeps its snapshot while the writer writes into
/// the restarted log and a PASSIVE checkpoint is attempted (it needs mark 0 exclusively), and every
/// row reads back after the reader ends and a TRUNCATE checkpoint. Returns whether the log
/// restarted and the restart-gate deltas. The gate counters are process-global: run with
/// --test-threads=1.
fn restart_under_a_mark0_reader(sqlite_rule: bool) -> (bool, [u64; walpin::RESTART_GATES]) {
    let (_dir, db) = open_db(false);
    db.walpin_set_sqlite_restart(sqlite_rule);
    let writer = db.connect().unwrap();
    seed(&writer);
    set(&writer, 1, "one");
    writer.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let s = db.walpin_stats();
    assert!(
        s.nbackfills > 0 && s.nbackfills == s.max_frame,
        "the WAL is fully backfilled: {s:?}"
    );
    let reader = db.connect().unwrap();
    reader.execute("BEGIN").unwrap();
    assert_eq!(value(&reader, 2), original(2));
    let seq0 = db.walpin_stats().checkpoint_seq;
    let g0 = walpin::counters().restart_gate;
    set(&writer, 2, "two");
    let restarted = db.walpin_stats().checkpoint_seq != seq0;
    let g1 = walpin::counters().restart_gate;
    assert_eq!(value(&reader, 2), original(2), "the mark-0 reader keeps its snapshot");
    set(&writer, 3, "three");
    let _ = writer.execute("PRAGMA wal_checkpoint(PASSIVE)");
    assert_eq!(value(&reader, 2), original(2), "no checkpoint rewrote the file under it");
    assert_eq!(value(&reader, 3), original(3));
    reader.execute("COMMIT").unwrap();
    assert_eq!(value(&reader, 2), "two");
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let fresh = db.connect().unwrap();
    for id in 1..=ROWS {
        let want = match id {
            1 => "one".to_string(),
            2 => "two".to_string(),
            3 => "three".to_string(),
            _ => original(id),
        };
        assert_eq!(value(&fresh, id), want, "row {id} after TRUNCATE");
    }
    (restarted, std::array::from_fn(|i| g1[i] - g0[i]))
}

// The gate counters are process-global, so another test writing in parallel can add to them: the
// assertions use `restarted` (this database's checkpoint_seq) and gate deltas as lower bounds, and
// print the deltas; under --test-threads=1 (the registered run) they are exact.
#[test]
fn turso_rule_a_mark0_reader_refuses_the_restart() {
    let (restarted, gates) = restart_under_a_mark0_reader(false);
    eprintln!("turso rule: restarted {restarted} gate deltas {gates:?}");
    assert!(!restarted, "turso's rule needs mark 0 alone");
    assert!(gates[3] >= 1, "refused at the mark-0 upgrade: {gates:?}");
}

#[test]
fn sqlite_rule_restarts_under_a_mark0_reader() {
    let (restarted, gates) = restart_under_a_mark0_reader(true);
    eprintln!("SQLite rule: restarted {restarted} gate deltas {gates:?}");
    assert!(restarted, "SQLite's rule lets mark-0 readers proceed");
    assert!(gates[5] >= 1, "restarted: {gates:?}");
}

/// r11-walpin-conc amendment 5: the wal2-specific window. FW2 is on for this process (FW1+FW2 are
/// process switches), FW3 for this database. The branch finds its leaf in a frame of wal2 file 0;
/// between that lookup and the read, the trunk fills file 0, switches to file 1, has file 0
/// checkpointed, fills file 1 and switches back, so file 0 is reused and the frame's slot holds
/// another page. Returns what the branch read, the retries it took and the switches in the window.
fn wal2_reuse_in_the_window(mutant: u8) -> (Option<String>, u64, u64) {
    let (fw1, fw2, fw3) = (walpin::fw1(), walpin::fw2(), walpin::fw3());
    walpin::set_fixes(true, true, false);
    let (_dir, db) = open_db(true);
    db.walpin_open_wal2().unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    set(&trunk, ROWS, "in-wal-at-fork");
    let branch = trunk.fork_branch().unwrap();
    let conn = branch.connect().unwrap();
    assert_eq!(value(&conn, 1), original(1));
    let trunk2 = trunk.clone();
    let switches = Arc::new(std::sync::Mutex::new(0u64));
    let seen = switches.clone();
    fw3_test_hook::set(Box::new(move |_page| {
        let sw0 = walpin::counters().fw2_switches;
        for i in 0..1_100 {
            set(&trunk2, 1, &format!("fill-a-{i}"));
        }
        trunk2.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        for i in 0..1_100 {
            set(&trunk2, 1, &format!("fill-b-{i}"));
        }
        *seen.lock().unwrap() = walpin::counters().fw2_switches - sw0;
    }));
    fw3_test_hook::set_mutant(mutant);
    let before = walpin::counters().fw3_retries;
    let got = try_value(&conn, ROWS);
    let retries = walpin::counters().fw3_retries - before;
    fw3_test_hook::set_mutant(0);
    let switched = *switches.lock().unwrap();
    drop(conn);
    drop(branch);
    walpin::set_fixes(fw1, fw2, fw3);
    (got, retries, switched)
}

#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_fw3_a_reused_wal2_file_in_the_window_is_retried() {
    let (got, retries, switched) = wal2_reuse_in_the_window(0);
    assert!(switched >= 2, "file 0 was reused in the window: {switched} switches");
    assert_eq!(got.as_deref(), Some("in-wal-at-fork"));
    assert!(retries >= 1, "the read was retried");
}

#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_fw3_the_generation_check_is_not_what_catches_a_reused_wal2_file() {
    // Mutant 2 passes every generation check. The stale frame maps to no slot of either file, so
    // the read itself fails and the failed-read retry recovers the fork value without it.
    let (got, _, switched) = wal2_reuse_in_the_window(2);
    assert!(switched >= 2);
    assert_eq!(got.as_deref(), Some("in-wal-at-fork"));
}

#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_fw3_without_the_failed_read_retry_a_reused_wal2_file_fails_the_read() {
    let (got, _, switched) = wal2_reuse_in_the_window(3);
    assert!(switched >= 2);
    assert_eq!(got, None, "mutant 3 must fail the read, or this test cannot see the retry");
}
