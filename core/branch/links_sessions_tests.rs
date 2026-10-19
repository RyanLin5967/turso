//! Links adversary 3 (DECISIONS 65eb410760; ADV-claude-links-3-df4500c39..a4a5b7dc8.md): the
//! upgrade's bounded fill must make progress across sessions and under pressure, as a class.
//! - H1: a 17-20 store whose sessions are shorter than one fill must still upgrade (its progress is
//!   monotone across opens), and its log must stop growing.
//! - Past the hard log limit, a checkpoint does not wait for the fill.
//! - L1: a second explicit checkpoint during the fill joins it, never Busy.
//! - L2: a failed fill backs off as a failed checkpoint does.
//! Each runs in a child process of its own: the fill's knobs (`LINKS_FILL_PAUSE_MS`,
//! `LINKS_FILL_FAILS`) and the compaction threshold are process-wide.

use super::*;
use crate::{Database, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn open(path: &std::path::Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync: SyncClass::Off })
            .with_branch_checkpoint(BranchCheckpoint::Fuzzy),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn meta_version(path: &std::path::Path) -> u32 {
    let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap();
    cat.meta().unwrap().map_or(0, |m| m.format)
}

/// `meta_version`, or `None` when the catalog cannot be read now (a checkpoint in flight holds it:
/// SQLite Busy, measured on iccad6 at 7a7375519, raw h1d-starved-fix): the polls inside the hold use it.
fn try_meta_version(path: &std::path::Path) -> Option<u32> {
    let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).ok()?;
    cat.meta().ok().map(|m| m.map_or(0, |m| m.format))
}

fn log_bytes(path: &std::path::Path) -> u64 {
    std::fs::metadata(format!("{}-branch-log", path.display())).map_or(0, |m| m.len())
}

/// A 19 store of `n` branches made and checkpointed by the older build (the stand-in).
fn older_store(path: &std::path::Path, n: usize) {
    super::journal::CREATE_LEGACY.with(|c| c.set(true));
    super::journal::LEGACY_FACTS.with(|c| c.set(true));
    {
        let db = open(path);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for _ in 0..n {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    }
    super::journal::CREATE_LEGACY.with(|c| c.set(false));
    super::journal::LEGACY_FACTS.with(|c| c.set(false));
    assert_eq!(meta_version(path), 19, "premise: a 19 store");
}

/// Run `child` (a test of this file) alone in a child process with `FE_LS_CHILD` set, and return its
/// output, refusing one that ran no test or failed.
fn child(child: &str, env: &[(&str, &str)]) -> String {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args([&format!("branch::links_sessions_tests::{child}"), "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_LS_CHILD", "1");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("running 1 test"), "{child}: ran no test:\n{text}");
    assert!(out.status.success(), "{child} failed:\n{text}");
    text
}

fn in_child() -> bool {
    std::env::var_os("FE_LS_CHILD").is_some()
}

fn readings(text: &str, tag: &str) -> Vec<Vec<u64>> {
    text.lines()
        .filter_map(|l| l.find(tag).map(|i| &l[i + tag.len()..]))
        .map(|r| r.split_whitespace().filter_map(|w| w.split('=').nth(1)).map(|v| v.parse().unwrap()).collect())
        .collect()
}

// ---- H1: short sessions ----

#[test]
fn ls_sessions_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("s.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(8 << 10);
    super::store::LINKS_FILL_PAUSE_MS.store(40, Ordering::Release);
    for s in 0..14u64 {
        {
            let db = open(&path);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            let starts0 = db.branches.links_fill_state_for_test().2;
            let mut k = 0;
            while db.branches.links_fill_state_for_test().2 == starts0 && k < 3_000 {
                let _ = trunk.fork_branch().unwrap().into_id();
                k += 1;
            }
            // A session shorter than the fill (about 3 of its 16 batches) that goes on working, as
            // the adversary's sessions did (2,000 creates and a hold): an operation after the fill
            // ends is what can start the checkpoint that flips the meta row.
            let t = std::time::Instant::now();
            while t.elapsed() < std::time::Duration::from_millis(130) {
                let _ = trunk.fork_branch().unwrap().into_id();
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        println!("LS-SESSION s={s} meta={} log={}", meta_version(&path), log_bytes(&path));
    }
}

/// H1: sessions that each close a few batches into the fill. Each open must go on where the last
/// left off, so the store upgrades within the 14 sessions and its log is cut.
#[test]
fn a_store_whose_sessions_are_shorter_than_its_fill_still_upgrades_and_cuts_its_log() {
    if in_child() {
        return;
    }
    let text = child("ls_sessions_child", &[]);
    let r = readings(&text, "LS-SESSION ");
    assert_eq!(r.len(), 14, "premise: 14 sessions ran:\n{text}");
    let first21 = r.iter().position(|v| v[1] >= 21);
    let peak = r.iter().map(|v| v[2]).max().unwrap();
    let last = r.last().unwrap()[2];
    println!("LS sessions: first upgraded session {first21:?}, log peak {peak}, last {last}");
    assert!(first21.is_some(), "the store never upgraded in 14 sessions shorter than its fill (meta stayed 19); log {peak} B at its peak");
    assert!(last < peak, "the log was never cut: {last} B at the end, {peak} B at its peak");
}

// ---- past the hard limit ----

#[test]
fn ls_hard_limit_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("h.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    // A fill that is stalled (5 s after each batch): only the hard limit can move the store.
    super::store::LINKS_FILL_PAUSE_MS.store(5_000, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut peak = 0;
    for _ in 0..600 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
    }
    db.branch_checkpoint_wait();
    println!("LS-HARD meta={} log={} peak={peak}", meta_version(&path), log_bytes(&path));
    super::store::LINKS_FILL_PAUSE_MS.store(0, Ordering::Release);
}

/// Past the hard log limit, a checkpoint does not wait for a fill that has not finished: it
/// finishes the fill on the writer, so the store upgrades and the log is cut within the session.
#[test]
fn past_the_hard_log_limit_the_upgrade_does_not_wait_for_the_fill() {
    if in_child() {
        return;
    }
    let text = child("ls_hard_limit_child", &[]);
    let r = readings(&text, "LS-HARD ");
    let (meta, log, peak) = (r[0][0], r[0][1], r[0][2]);
    println!("LS hard limit: meta {meta}, log {log}, peak {peak}");
    assert!(meta >= 21, "past the hard limit the store still waited for its stalled fill: meta {meta}, log peak {peak} B");
}

// ---- past the hard limit, a fill starved of the store mutex (links H1 reopened) ----

#[test]
fn ls_fill_starved_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("f.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    // The fill's committed batches are held back from the store mutex, as a foreground that takes
    // it on every operation starved them (diagnostic 4): only the hard limit can move the store.
    super::store::LINKS_FILL_HELD_BATCHES.store(0, Ordering::Release);
    // The columns made inside the first batch (the path before e2a788823), so this test reaches the
    // per-batch record, the backstop the column-only transaction now precedes.
    super::store::LINKS_FILL_COLUMNS_IN_BATCH.store(true, Ordering::Release);
    super::store::LINKS_FILL_HOLD_UNRECORDED.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut peak = 0;
    let mut k = 0;
    while !(db.branches.past_hard_limit_for_test() && super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire) >= 1) && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    let held = super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire);
    // Operations past the hard limit, with the batch still held: each may start the checkpoint.
    for _ in 0..50 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
    }
    // Read WHILE the batch is still held (l4-rev-h1's corrected variant: nothing the fill does after
    // the hold is let go can count): a checkpoint the operations started runs on a thread of its own,
    // so the meta row is polled for it (wall time only bounds the wait). Then the hold is let go, and
    // only then waited on: checkpoint_wait joins the fill thread, which a hold in place never ends.
    let t = std::time::Instant::now();
    let mut meta = try_meta_version(&path).unwrap_or(0);
    while meta < 21 && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(20));
        meta = try_meta_version(&path).unwrap_or(meta);
    }
    super::store::LINKS_FILL_HOLD_UNRECORDED.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    super::store::LINKS_FILL_COLUMNS_IN_BATCH.store(false, Ordering::Release);
    println!("LS-STARVED past={} held={held} creates={k} meta={meta} log={} peak={peak}", u64::from(past), log_bytes(&path));
}

/// Links H1 reopened (core-integrator, red alone 6/6 at 91c0ea8d2; diagnostic 3: 405 checks past the
/// hard limit waited, columns never seen as made; diagnostic 4: the first batch COMMITTED, then sat
/// waiting for the store mutex to record it): past the hard log limit, a fill batch that has
/// committed but not yet been recorded under the store mutex must not hold the upgrade. The hold is
/// a test hook, so this fails on every host, not only on a slow one.
#[test]
fn past_the_hard_log_limit_a_fill_batch_starved_of_the_store_mutex_does_not_hold_the_upgrade() {
    if in_child() {
        return;
    }
    let text = child("ls_fill_starved_child", &[]);
    let r = readings(&text, "LS-STARVED ");
    let (past, held, meta, peak) = (r[0][0], r[0][1], r[0][3], r[0][5]);
    println!("LS starved: past {past}, held {held}, meta {meta}, peak {peak}");
    assert_eq!(past, 1, "premise: the log never passed the hard limit (peak {peak} B)");
    assert!(held >= 1, "premise: no fill batch committed and was held");
    assert!(meta >= 21, "past the hard limit the upgrade waited for a fill batch it could not see: meta {meta}, log peak {peak} B");
}

#[test]
fn ls_fill_entry_starved_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("e.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    // The fill thread is held at its start, before it first takes the store mutex, as a fill
    // starved of it there would be (l4-rev-h1 MED-3).
    super::store::LINKS_FILL_ENTRY_HELD.store(0, Ordering::Release);
    super::store::LINKS_FILL_HOLD_AT_ENTRY.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut peak = 0;
    let mut k = 0;
    while !(db.branches.past_hard_limit_for_test() && super::store::LINKS_FILL_ENTRY_HELD.load(Ordering::Acquire) >= 1) && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    let held = super::store::LINKS_FILL_ENTRY_HELD.load(Ordering::Acquire);
    for _ in 0..50 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
    }
    // Read before the hold is let go: what the 50 operations started with the fill held. A checkpoint
    // they started runs on a thread of its own, so the meta row is polled for it (wall time only bounds
    // the wait; a fill that held the upgrade never gets there).
    let t = std::time::Instant::now();
    let mut meta_held = try_meta_version(&path).unwrap_or(0);
    while meta_held < 21 && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(20));
        meta_held = try_meta_version(&path).unwrap_or(meta_held);
    }
    super::store::LINKS_FILL_HOLD_AT_ENTRY.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    println!("LS-ENTRY past={} held={held} creates={k} meta_held={meta_held} meta={} peak={peak}", u64::from(past), meta_version(&path));
}

/// l4-rev-h1 MED-3: past the hard log limit, a fill held at its start, before it first takes the store
/// mutex (as one starved of it there would be), must not hold the upgrade. Read while the fill is
/// still held, so only a checkpoint the operations started can have flipped the meta row.
#[test]
fn past_the_hard_log_limit_a_fill_starved_at_its_start_does_not_hold_the_upgrade() {
    if in_child() {
        return;
    }
    let text = child("ls_fill_entry_starved_child", &[]);
    let r = readings(&text, "LS-ENTRY ");
    let (past, held, meta_held, peak) = (r[0][0], r[0][1], r[0][3], r[0][5]);
    println!("LS entry-starved: past {past}, held {held}, meta while held {meta_held}, peak {peak}");
    assert_eq!(past, 1, "premise: the log never passed the hard limit (peak {peak} B)");
    assert!(held >= 1, "premise: no fill thread reached its start and was held");
    assert!(meta_held >= 21, "past the hard limit the upgrade waited for a fill held at its start: meta {meta_held}, log peak {peak} B");
}

// ---- L1: two explicit checkpoints ----

#[test]
fn ls_two_compactions_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    older_store(&path, 1_000);
    super::store::LINKS_FILL_PAUSE_MS.store(30, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let a = {
        let db = db.clone();
        std::thread::spawn(move || db.branch_compact_now().map_err(|e| e.to_string()))
    };
    std::thread::sleep(std::time::Duration::from_millis(60));
    let (state, b) = (db.branches.links_fill_state_for_test(), db.branch_compact_now().map_err(|e| e.to_string()));
    let a = a.join().unwrap();
    println!("LS-TWO filling_when_b={} a_ok={} b_ok={} meta={}", u64::from(state.0), u64::from(a.is_ok()), u64::from(b.is_ok()), meta_version(&path));
    println!("LS-TWO-ERR a={a:?} b={b:?}");
}

/// L1: a second `compact_now` while the first one's fill runs joins that fill and checkpoints,
/// never Busy.
#[test]
fn a_second_explicit_checkpoint_during_the_fill_joins_it() {
    if in_child() {
        return;
    }
    let text = child("ls_two_compactions_child", &[]);
    let r = readings(&text, "LS-TWO ");
    assert_eq!(r[0][0], 1, "premise: the second call came while the fill ran:\n{text}");
    assert_eq!((r[0][1], r[0][2]), (1, 1), "a compact_now during the fill failed:\n{text}");
}

// ---- L2: a failing fill ----

#[test]
fn ls_failing_fill_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("f.db");
    older_store(&path, 300);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_FAILS.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let log0 = log_bytes(&path);
    let mut starts = db.branches.links_fill_state_for_test().2;
    for _ in 0..400 {
        let _ = trunk.fork_branch().unwrap().into_id();
        let now = db.branches.links_fill_state_for_test().2;
        if now != starts {
            // Diagnostic only: where in the log each start came.
            println!("LS-FAIL-START n={now} at={}", log_bytes(&path) - log0);
            starts = now;
        }
    }
    let grown = log_bytes(&path) - log0;
    println!("LS-FAIL starts={} grown={grown}", db.branches.links_fill_state_for_test().2);
    super::store::LINKS_FILL_FAILS.store(false, Ordering::Release);
}

/// L2: a fill that fails is not started again by the very next operation: it backs off a threshold's
/// worth of log, as a failed checkpoint does. 400 creates grow the log by `grown` bytes: at most one
/// start per threshold (4 KiB) of it, plus one.
#[test]
fn a_failed_fill_backs_off_as_a_failed_checkpoint_does() {
    if in_child() {
        return;
    }
    let text = child("ls_failing_fill_child", &[]);
    let r = readings(&text, "LS-FAIL ");
    let (starts, grown) = (r[0][0], r[0][1]);
    let bound = grown / (4 << 10) + 1;
    println!("LS failing fill: {starts} starts over {grown} B of log (bound {bound})");
    assert!(starts >= 1, "premise: the fill started");
    assert!(starts <= bound, "a failed fill restarted {starts} times over {grown} B of log; a failed checkpoint would retry at most {bound}");
}

// ---- L3: a busy truncation is counted ----

/// L3: when a reader keeps the catalog's WAL from being truncated, the truncation's busy result is
/// a counted event (`wal_truncate_busy`), not only a log line.
#[test]
fn a_busy_catalog_wal_truncation_is_counted() {
    if in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("t.db");
    older_store(&path, 300);
    let db = open(&path);
    db.branch_wait_name_filter();
    let before = db.branches.wal_truncate_busy_for_test();
    // A reader holding a read transaction on the catalog across the fill and the flip.
    let mut reader = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap();
    reader.begin().unwrap();
    let _ = reader.meta().unwrap();
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    let busy = db.branches.wal_truncate_busy_for_test() - before;
    reader.rollback();
    assert_eq!(meta_version(&path), 21, "premise: the store upgraded");
    assert!(busy >= 1, "a truncation held busy by a reader was not counted ({busy})");
}
