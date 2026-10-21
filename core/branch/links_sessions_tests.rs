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

// ---- h1gap-rev-c MED-1: a fill thread that panics ----

#[test]
fn ls_fill_panics_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("p.db");
    older_store(&path, 300);
    super::store::LINKS_FILL_PANICS.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let db = db.clone();
        std::thread::spawn(move || {
            let r = db.branch_compact_now().map_err(|e| e.to_string());
            let _ = tx.send(r);
        });
    }
    let got = rx.recv_timeout(std::time::Duration::from_secs(20));
    let (filling, _, starts) = db.branches.links_fill_state_for_test();
    println!("LS-PANIC returned={} filling={} starts={starts}", u64::from(got.is_ok()), u64::from(filling));
    // The checkpoint thread may still be waiting: end the child here.
    std::process::exit(0);
}

/// h1gap-rev-c MED-1: a fill thread that panics is an exit like any other: an explicit checkpoint
/// returns (within 20 s) and the store no longer reads as filling. Premise: a fill was started.
#[test]
fn a_fill_thread_that_panics_does_not_hang_an_explicit_checkpoint() {
    if in_child() {
        return;
    }
    let text = child("ls_fill_panics_child", &[]);
    let r = readings(&text, "LS-PANIC ");
    assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
    let (returned, filling, starts) = (r[0][0], r[0][1], r[0][2]);
    assert!(starts >= 1, "premise: no fill was started:\n{text}");
    assert_eq!(returned, 1, "an explicit checkpoint did not return within 20 s after the fill thread panicked (filling {filling})");
    assert_eq!(filling, 0, "the store still reads as filling after its fill thread panicked");
}

// ---- h1gap-rev-c HIGH-2: past the hard limit, no checkpoint links the whole catalog ----

#[test]
fn ls_hard_whole_child() {
    if !in_child() {
        return;
    }
    let n: usize = std::env::var("FE_LS_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("w.db");
    // FE_LS_CONTROL (observing only): a store of this build (21, no fill), the same workload, to tell the
    // fill's waits from the fuzzy checkpoint's own back-pressure.
    if std::env::var_os("FE_LS_CONTROL").is_some() {
        let db = open(&path);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for _ in 0..n {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    } else {
        older_store(&path, n);
    }
    super::journal::set_compact_threshold(4 << 10);
    // A slow fill (1 s after each batch): the hard limit is passed long before it ends.
    super::store::LINKS_FILL_PAUSE_MS.store(1_000, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let w0 = super::catalog::WHOLE_UPGRADE_BATCHES.load(Ordering::Acquire);
    let mut k = 0;
    while !db.branches.past_hard_limit_for_test() && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    // Operations past the hard limit (each a create; the waits on an install they made, this thread's
    // back-pressure waits), then the checkpoints they started are waited for.
    let waits0 = super::store::thread_waits();
    let peak = log_bytes(&path);
    for _ in 0..200 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let waits = super::store::thread_waits() - waits0;
    db.branch_checkpoint_wait();
    let whole = super::catalog::WHOLE_UPGRADE_BATCHES.load(Ordering::Acquire) - w0;
    super::store::LINKS_FILL_PAUSE_MS.store(0, Ordering::Release);
    println!("LS-WHOLE rows={n} past={} creates={k} whole_batches={whole} waits={waits} meta={} peak={peak} log={}", u64::from(past), meta_version(&path), log_bytes(&path));
}

/// h1gap-rev-c HIGH-2: past the hard log limit, with a slow fill, no checkpoint links the whole catalog
/// inside its capture (an O(N) stall of every create: under the store mutex in the sharp mode, and
/// behind the install every operation waits for in the fuzzy one; the reviewer measured 1.2-4.7 s at
/// N=1000-4000). Counted, not timed, at 1,000 and at 4,000 rows (FLAGGED stricter, lead ruling 62c11ec63d,
/// before its fix): the batches a whole-catalog upgrade ran (`WHOLE_UPGRADE_BATCHES`) are 0, and the 200
/// creates past the hard limit waited on no install (`thread_waits`). And the log was cut while the fill
/// was incomplete: the store stays at its version (19) and the log ends under its peak (the non-flip cut).
/// Premise: the log passed the hard limit.
#[test]
fn past_the_hard_log_limit_no_checkpoint_links_the_whole_catalog() {
    if in_child() {
        return;
    }
    for rows in ["1000", "4000"] {
        let text = child("ls_hard_whole_child", &[("FE_LS_ROWS", rows)]);
        let r = readings(&text, "LS-WHOLE ");
        assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
        let (n, past, whole, waits, meta, peak, log) = (r[0][0], r[0][1], r[0][3], r[0][4], r[0][5], r[0][6], r[0][7]);
        assert_eq!(past, 1, "{n} rows: premise: the log never passed the hard limit");
        assert_eq!(whole, 0, "{n} rows: past the hard limit a checkpoint linked the whole catalog ({whole} batches of {})", super::catalog::UPGRADE_BATCH);
        assert_eq!(waits, 0, "{n} rows: creates past the hard limit waited on an install {waits} times");
        assert_eq!(meta, 19, "{n} rows: the store flipped before its fill was complete (meta {meta})");
        assert!(log < peak, "{n} rows: the log was not cut past the hard limit ({log} B at the end, {peak} B at the limit)");
    }
}

// ---- h1gap-rev-c HIGH-1: the fill resumes from its cursor, below the hard limit ----

#[test]
fn ls_resume_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("r.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_PAUSE_MS.store(40, Ordering::Release);
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        // Up to the threshold only: the warm session starts no fill.
        while log_bytes(&path) < 4 << 10 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
    }
    for s in 0..14u64 {
        let past = {
            let db = open(&path);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            let _ = trunk.fork_branch().unwrap().into_id();
            std::thread::sleep(std::time::Duration::from_millis(200));
            let _ = trunk.fork_branch().unwrap().into_id();
            std::thread::sleep(std::time::Duration::from_millis(50));
            db.branches.past_hard_limit_for_test()
        };
        println!("LS-RESUME s={s} meta={} log={} past={}", meta_version(&path), log_bytes(&path), u64::from(past));
    }
    super::store::LINKS_FILL_PAUSE_MS.store(0, Ordering::Release);
}

/// h1gap-rev-c HIGH-1 (its rev_resume_progress_child): sessions of a create, a 200 ms idle and one more
/// create, each far shorter than the fill (40 ms per batch, 1,000 rows), and the log kept UNDER the hard
/// limit, so only the fill's resumed cursor can finish the upgrade (the hard-limit bypass cannot). The
/// store upgrades within 14 sessions. Premise: no session reached the hard limit before the upgrade.
/// Mutant `l4_fill_cursor_forgotten` (every open fills again from the first row) must fail it.
#[test]
fn a_fill_resumed_across_short_sessions_completes_below_the_hard_limit() {
    if in_child() {
        return;
    }
    let text = child("ls_resume_child", &[]);
    let r = readings(&text, "LS-RESUME ");
    assert_eq!(r.len(), 14, "premise: 14 sessions ran:\n{text}");
    let first21 = r.iter().position(|v| v[1] >= 21);
    println!("LS resume: first upgraded session {first21:?}; metas {:?}", r.iter().map(|v| v[1]).collect::<Vec<_>>());
    let upto = first21.unwrap_or(r.len() - 1);
    assert!(r[..=upto].iter().all(|v| v[3] == 0), "premise: a session reached the hard limit before the upgrade, so the bypass could have finished it:\n{text}");
    assert!(first21.is_some(), "the fill never completed across 14 short sessions below the hard limit (its cursor was not resumed)");
}

// ---- h1gap-rev-c HIGH-3: a session that writes in a burst and goes idle ----

#[test]
fn ls_burst_idle_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("b.db");
    older_store(&path, 300);
    super::journal::set_compact_threshold(4 << 10);
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        while log_bytes(&path) < 6 << 10 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
    }
    println!("LS-BURST-WARM meta={} log={}", meta_version(&path), log_bytes(&path));
    for s in 0..10u64 {
        let prefilled = {
            let db = open(&path);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            let _ = trunk.fork_branch().unwrap().into_id();
            std::thread::sleep(std::time::Duration::from_millis(300));
            db.branches.links_fill_state_for_test().1
        };
        println!("LS-BURST s={s} prefilled={} meta={} log={}", u64::from(prefilled), meta_version(&path), log_bytes(&path));
    }
}

/// h1gap-rev-c HIGH-3: sessions of ONE create past the threshold, then an idle before close (a process
/// that writes in a burst and exits). The fill completes in the idle, and no operation comes after it.
/// The store must still upgrade: in the session whose fill completed, the meta row is 21 once it closes.
/// Premises: the warm session left a 19 store past the threshold; some session's fill completed
/// (prefilled at its idle's end, read before close).
#[test]
fn a_store_whose_fill_completes_in_an_idle_session_upgrades_by_its_close() {
    if in_child() {
        return;
    }
    let text = child("ls_burst_idle_child", &[]);
    let warm = readings(&text, "LS-BURST-WARM ");
    assert_eq!(warm.first().map(|v| v[0]), Some(19), "premise: the warm session left a 19 store:\n{text}");
    let r = readings(&text, "LS-BURST ");
    assert_eq!(r.len(), 10, "premise: 10 sessions ran:\n{text}");
    let done = r.iter().position(|v| v[1] == 1);
    println!("LS burst-idle: first session whose fill completed {done:?}; metas {:?}", r.iter().map(|v| v[2]).collect::<Vec<_>>());
    let done = done.unwrap_or_else(|| panic!("premise: no session's fill completed in its idle:\n{text}"));
    assert!(r[done][2] >= 21, "the fill completed in session {done} and no operation came after it: the store did not upgrade by its close (meta {}, log {} B)", r[done][2], r[done][3]);
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
    let busy0 = db.branches.wal_truncate_busy_for_test();
    let trunk = db.connect().unwrap();
    let mut peak = 0;
    for _ in 0..600 {
        let _ = trunk.fork_branch().unwrap().into_id();
        peak = peak.max(log_bytes(&path));
    }
    db.branch_checkpoint_wait();
    // Observing only (l4-rev-h1 LOW 4, measured, no assertion): busy catalog WAL truncations meanwhile.
    let busy = db.branches.wal_truncate_busy_for_test() - busy0;
    println!("LS-HARD meta={} log={} peak={peak} wal_busy={busy}", meta_version(&path), log_bytes(&path));
    super::store::LINKS_FILL_PAUSE_MS.store(0, Ordering::Release);
}

/// Past the hard log limit, a checkpoint does not wait for a fill that has not finished: the log is cut
/// within the session. FLAGGED spec change, lead ruling 62c11ec63d (h1gap-rev-c HIGH-2, the non-flip cut), as authority: the cut keeps the store at its version (19) while the fill is incomplete,
/// where it used to finish the fill and flip (an O(N) stall of every create); the flip waits for the fill.
#[test]
fn past_the_hard_log_limit_the_upgrade_does_not_wait_for_the_fill() {
    if in_child() {
        return;
    }
    let text = child("ls_hard_limit_child", &[]);
    let r = readings(&text, "LS-HARD ");
    let (meta, log, peak) = (r[0][0], r[0][1], r[0][2]);
    println!("LS hard limit: meta {meta}, log {log}, peak {peak}, busy WAL truncations {}", r[0][3]);
    assert!(log < peak, "past the hard limit the log was not cut while the fill stalled: {log} B at the end, {peak} B at its peak");
    assert_eq!(meta, 19, "the store flipped with its stalled fill incomplete (meta {meta})");
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
    // (Ruling 62c11ec63d: what is polled is the log's cut; the meta row is read once it is.)
    let t = std::time::Instant::now();
    let mut log_held = log_bytes(&path);
    while log_held >= peak && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(20));
        log_held = log_bytes(&path);
    }
    let meta = try_meta_version(&path).unwrap_or(0);
    super::store::LINKS_FILL_HOLD_UNRECORDED.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    super::store::LINKS_FILL_COLUMNS_IN_BATCH.store(false, Ordering::Release);
    println!("LS-STARVED past={} held={held} creates={k} meta={meta} log={log_held} peak={peak}", u64::from(past));
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
    // FLAGGED spec change, lead ruling 62c11ec63d (h1gap-rev-c HIGH-2, the non-flip cut), as authority: the claim is the log's cut while the batch is held, the store kept at 19.
    let log_held = r[0][4];
    assert!(log_held < peak, "past the hard limit the log was not cut while a fill batch was held: {log_held} B, peak {peak} B");
    assert_eq!(meta, 19, "the store flipped with its fill incomplete (meta {meta})");
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
    // (Ruling 62c11ec63d: what is polled is the log's cut; the meta row is read once it is.)
    let t = std::time::Instant::now();
    let mut log_held = log_bytes(&path);
    while log_held >= peak && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(20));
        log_held = log_bytes(&path);
    }
    let meta_held = try_meta_version(&path).unwrap_or(0);
    super::store::LINKS_FILL_HOLD_AT_ENTRY.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    println!("LS-ENTRY past={} held={held} creates={k} meta_held={meta_held} meta={} peak={peak} log_held={log_held}", u64::from(past), meta_version(&path));
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
    // FLAGGED spec change, lead ruling 62c11ec63d (h1gap-rev-c HIGH-2, the non-flip cut), as authority: the claim is the log's cut while the fill is held, the store kept at 19.
    let log_held = r[0][6];
    assert!(log_held < peak, "past the hard limit the log was not cut while the fill was held at its start: {log_held} B, peak {peak} B");
    assert_eq!(meta_held, 19, "the store flipped with its fill incomplete (meta {meta_held})");
}

// ---- lead ruling LOW 4: a stopped fill makes no columns ----

#[test]
fn ls_fill_stopped_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    older_store(&path, 300);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_BEFORE_COLUMNS_HELD.store(0, Ordering::Release);
    super::store::LINKS_FILL_FIRST_MADE_COLUMNS.store(false, Ordering::Release);
    super::store::LINKS_FILL_HOLD_BEFORE_COLUMNS.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut k = 0;
    while super::store::LINKS_FILL_BEFORE_COLUMNS_HELD.load(Ordering::Acquire) == 0 && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let held = super::store::LINKS_FILL_BEFORE_COLUMNS_HELD.load(Ordering::Acquire);
    // Stopped as a close stops it, while it waits before its column-only transaction; then let go.
    db.branches.stop_links_fill_for_test();
    super::store::LINKS_FILL_HOLD_BEFORE_COLUMNS.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    // Only the fill that was held and stopped is judged (a later checkpoint may start a fill of its own).
    let made = super::store::LINKS_FILL_FIRST_MADE_COLUMNS.load(Ordering::Acquire);
    println!("LS-STOPPED held={held} creates={k} first_made={}", u64::from(made));
}

/// Lead ruling LOW 4 (l4-rev-h1 re-review): a fill stopped (as a close stops it) before its
/// column-only transaction makes no columns: it checks that it is still filling first.
#[test]
fn a_fill_stopped_before_its_columns_makes_none() {
    if in_child() {
        return;
    }
    let text = child("ls_fill_stopped_child", &[]);
    let r = readings(&text, "LS-STOPPED ");
    let (held, made) = (r[0][0], r[0][2]);
    println!("LS stopped: held {held}, the stopped fill made its columns {made}");
    assert!(held >= 1, "premise: no fill thread reached its column-only transaction and was held");
    assert_eq!(made, 0, "a fill stopped before its column-only transaction still made the columns");
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


// ---- h1gap-rev-c MED-2: a fill killed between batches, an older build's checkpoint, then this build ----

fn ls_cat(path: &std::path::Path) -> super::catalog::Catalog {
    super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap()
}

fn ls_first(cat: &super::catalog::Catalog, sql: &str) -> u64 {
    cat.ints(sql).ok().and_then(|v| v.first().copied()).unwrap_or(9999)
}

/// An independent link oracle (the reviewer's): every row's sib_lo, sib_hi and child_max recomputed here
/// from (id, parent, fork_epoch), read by SQL, never by the subject. `(rows, rows whose links differ)`;
/// `None`: no link columns.
fn ls_wrong_links(path: &std::path::Path) -> Option<(u64, u64)> {
    let cat = ls_cat(path);
    let enc = |c: &str| format!("SELECT CASE WHEN {c} IS NULL THEN 0 ELSE {c} + 1 END FROM branch ORDER BY id");
    let lo = cat.ints(&enc("sib_lo")).ok()?;
    let hi = cat.ints(&enc("sib_hi")).unwrap();
    let cm = cat.ints(&enc("child_max")).unwrap();
    let ids = cat.ints("SELECT id FROM branch ORDER BY id").unwrap();
    let par = cat.ints("SELECT parent FROM branch ORDER BY id").unwrap();
    let fe = cat.ints("SELECT fork_epoch FROM branch ORDER BY id").unwrap();
    let mut by_parent: std::collections::HashMap<u64, Vec<u64>> = std::collections::HashMap::new();
    for i in 0..ids.len() {
        by_parent.entry(par[i]).or_default().push(fe[i]);
    }
    for v in by_parent.values_mut() {
        v.sort_unstable();
    }
    let mut wrong = 0;
    for i in 0..ids.len() {
        let sibs = &by_parent[&par[i]];
        let a = sibs.partition_point(|&f| f < fe[i]);
        let b = sibs.partition_point(|&f| f <= fe[i]);
        let want_lo = if a > 0 { sibs[a - 1] + 1 } else { 0 };
        let want_hi = if b < sibs.len() { sibs[b] + 1 } else { 0 };
        let want_cm = by_parent.get(&ids[i]).and_then(|v| v.last()).map_or(0, |f| f + 1);
        if (lo[i], hi[i], cm[i]) != (want_lo, want_hi, want_cm) {
            wrong += 1;
        }
    }
    Some((ids.len() as u64, wrong))
}

/// A 19 store (an older build's): `n` trunk forks, and a child of each of the first 50 (parents detached).
fn ls_store_with_ids(path: &std::path::Path, n: usize) -> Vec<BranchId> {
    super::journal::CREATE_LEGACY.with(|c| c.set(true));
    super::journal::LEGACY_FACTS.with(|c| c.set(true));
    let ids = {
        let db = open(path);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        let ids: Vec<BranchId> = (0..n).map(|_| trunk.fork_branch().unwrap().into_id()).collect();
        for id in ids.iter().take(50) {
            let b = db.branch(*id).unwrap();
            let _ = b.fork().unwrap().into_id();
            let _ = b.into_id();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        ids
    };
    super::journal::CREATE_LEGACY.with(|c| c.set(false));
    super::journal::LEGACY_FACTS.with(|c| c.set(false));
    assert_eq!(meta_version(path), 19, "premise: a 19 store");
    ids
}

#[test]
fn ls_kill_fill_child() {
    let Ok(p) = std::env::var("FE_LS_KILL_PATH") else {
        return;
    };
    let path = std::path::PathBuf::from(p);
    let db = open(&path);
    db.branch_wait_name_filter();
    let _ = db.branch_compact_now();
    db.branch_checkpoint_wait();
    std::process::exit(3);
}

#[test]
fn ls_kill_resume_child() {
    if !in_child() {
        return;
    }
    // k = 5 and 16: killed mid-fill; k = 17: killed after its last batch (1,050 rows, 64 per batch), so the
    // resume finds the cursor done.
    for k in [5u64, 16, 17] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("k.db");
        let ids = ls_store_with_ids(&path, 1_000);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["branch::links_sessions_tests::ls_kill_fill_child", "--exact", "--test-threads=1", "--nocapture"])
            .env("FE_LS_KILL_PATH", &path)
            .env("FE_KILL_AT", format!("ckpt.upgrade.batch:{k}"))
            .env_remove("FE_LS_CHILD")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        let killed = status.code().is_none();
        let killed_meta = meta_version(&path);
        let (gen0, cursor_gen, cursor_done) = {
            let mut cat = ls_cat(&path);
            let g = cat.meta().unwrap().map_or(9999, |m| m.generation);
            (g, ls_first(&cat, "SELECT generation FROM link_fill"), ls_first(&cat, "SELECT done FROM link_fill"))
        };
        // An older build's checkpoint between the kill and this build: it reaps behind the cursor and adds
        // children, and moves the catalog's generation.
        super::journal::CREATE_LEGACY.with(|c| c.set(true));
        super::journal::LEGACY_FACTS.with(|c| c.set(true));
        {
            let db = open(&path);
            db.branch_wait_name_filter();
            for id in ids.iter().skip(60).take(40).step_by(3) {
                db.branch(*id).unwrap().reap().unwrap();
            }
            for id in ids.iter().take(30).chain(ids.iter().skip(100).take(30)) {
                let b = db.branch(*id).unwrap();
                let _ = b.fork().unwrap().into_id();
                let _ = b.into_id();
            }
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
        }
        super::journal::CREATE_LEGACY.with(|c| c.set(false));
        super::journal::LEGACY_FACTS.with(|c| c.set(false));
        let gen1 = ls_cat(&path).meta().unwrap().map_or(9999, |m| m.generation);
        {
            let db = open(&path);
            db.branch_wait_name_filter();
            let _ = db.branch_compact_now();
            db.branch_checkpoint_wait();
        }
        let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
        println!(
            "LS-KILL k={k} killed={} killed_meta={killed_meta} cursor_gen={cursor_gen} cursor_done={cursor_done} gen0={gen0} gen1={gen1} meta={} rows={rows} wrong={wrong}",
            u64::from(killed),
            meta_version(&path)
        );
    }
}

/// h1gap-rev-c MED-2: a fill killed after batch k (its cursor committed), then an OLDER build's checkpoint
/// that changes rows behind the cursor and moves the generation, then this build: every row's links are
/// right by an independent oracle (0 wrong) at meta 21. k = 5 and 16 resume mid-fill; k = 17 is after the
/// last batch (the done cursor). Premises: killed there, still 19 with a cursor of the killed generation,
/// the older build moved the generation; for k = 17 the cursor is done. Mutant `l4_fill_generation_ignored`
/// must fail it.
#[test]
fn a_fill_killed_between_batches_and_resumed_after_an_older_builds_checkpoint_links_every_row() {
    if in_child() {
        return;
    }
    let text = child("ls_kill_resume_child", &[]);
    let r = readings(&text, "LS-KILL ");
    assert_eq!(r.len(), 3, "premise: three arms ran:\n{text}");
    for v in &r {
        let (k, killed, killed_meta, cgen, cdone, gen0, gen1, meta, rows, wrong) = (v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9]);
        assert_eq!((killed, killed_meta), (1, 19), "k={k}: premise: killed after batch {k}, still 19");
        assert_eq!(cgen, gen0, "k={k}: premise: the cursor is of the killed store's generation");
        assert!(gen1 > gen0, "k={k}: premise: the older build's checkpoint moved the generation ({gen0} -> {gen1})");
        if k == 17 {
            assert_eq!(cdone, 1, "premise: killed after the last batch, the cursor is done");
        }
        assert!(rows > 1_000, "k={k}: premise: the rows are there ({rows})");
        assert_eq!(meta, 21, "k={k}: the store did not upgrade");
        assert_eq!(wrong, 0, "k={k}: {wrong} of {rows} rows have wrong links after the resume (a cursor resumed across a checkpoint that changed rows behind it)");
    }
}


// ---- h1gap-rev-c HIGH-2 (ruling 62c11ec63d): a crash between a non-flip cut and the flip ----

/// The process cut short: a 19 store of 1,000 rows past the hard limit with a slow fill, its log cut by a
/// checkpoint that kept it at 19, then the process exits with no close (as a crash: no close-time flip).
#[test]
fn ls_nonflip_crash_child() {
    let Ok(p) = std::env::var("FE_LS_NF_PATH") else {
        return;
    };
    let path = std::path::PathBuf::from(p);
    let dir = path.parent().unwrap().to_path_buf();
    let _ = std::fs::create_dir_all(&dir);
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_PAUSE_MS.store(1_000, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut k = 0;
    while !db.branches.past_hard_limit_for_test() && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let peak = log_bytes(&path);
    for _ in 0..200 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    // The checkpoints the creates started, installed (a fuzzy one waited for; the fill is not joined).
    let t = std::time::Instant::now();
    while log_bytes(&path) >= peak && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    // The meta row read Busy-tolerant (a checkpoint may hold the catalog: FLAGGED harness repair, measured at
    // 37917a0a6 on iccad6, "unwrap() on Err Busy" at meta_version).
    let t = std::time::Instant::now();
    let mut meta = try_meta_version(&path);
    while meta.is_none() && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(20));
        meta = try_meta_version(&path);
    }
    println!("LS-NFCUT past={} meta={} peak={peak} log={}", u64::from(db.branches.past_hard_limit_for_test()), meta.unwrap_or(0), log_bytes(&path));
    std::process::exit(0);
}

/// h1gap-rev-c HIGH-2 (ruling 62c11ec63d): a process that crashes after a NON-FLIP cut (past the hard
/// limit, the log cut, the store kept at 19 with its fill incomplete) and before the flip reopens with
/// every row's links right (0 wrong by the independent oracle) at 21, under this build directly, and after
/// an older build's sessions in between. Premises: the crashed process cut its log and stayed at 19.
/// Mutant `l4_flip_without_fill` (a checkpoint flips with its fill incomplete) must fail it.
#[test]
fn a_crash_between_a_non_flip_cut_and_the_flip_reopens_with_every_link_right() {
    if in_child() {
        return;
    }
    for older in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nf.db");
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["branch::links_sessions_tests::ls_nonflip_crash_child", "--exact", "--test-threads=1", "--nocapture"])
            .env("FE_LS_NF_PATH", &path)
            .env_remove("FE_LS_CHILD")
            .output()
            .unwrap();
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        let r = readings(&text, "LS-NFCUT ");
        assert_eq!(r.len(), 1, "premise: the crashed process reported:\n{text}");
        let (meta, peak, log) = (r[0][1], r[0][2], r[0][3]);
        assert!(log < peak, "older={older}: premise: the crashed process's log was not cut ({log} B, peak {peak} B)");
        assert_eq!(meta, 19, "older={older}: the store flipped before its fill was complete (meta {meta} at the crash)");
        if older {
            super::journal::CREATE_LEGACY.with(|c| c.set(true));
            super::journal::LEGACY_FACTS.with(|c| c.set(true));
            {
                let db = open(&path);
                db.branch_wait_name_filter();
                let trunk = db.connect().unwrap();
                for _ in 0..30 {
                    let _ = trunk.fork_branch().unwrap().into_id();
                }
                db.branch_compact_now().unwrap();
                db.branch_checkpoint_wait();
            }
            super::journal::CREATE_LEGACY.with(|c| c.set(false));
            super::journal::LEGACY_FACTS.with(|c| c.set(false));
        }
        {
            let db = open(&path);
            db.branch_wait_name_filter();
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
        }
        let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
        println!("LS non-flip crash: older={older} meta={} rows={rows} wrong={wrong}", meta_version(&path));
        assert!(rows > 1_000, "older={older}: premise: the rows are there ({rows})");
        assert_eq!(meta_version(&path), 21, "older={older}: the store did not upgrade after the crash");
        assert_eq!(wrong, 0, "older={older}: {wrong} of {rows} rows have wrong links after a crash between a non-flip cut and the flip");
    }
}


// ---- h1gap-rev-c HIGH-3 (ruling 62c11ec63d): the close-time flip relinks O(changes), not O(N) ----

#[test]
fn ls_close_flip_child() {
    if !in_child() {
        return;
    }
    let n: usize = std::env::var("FE_LS_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(300);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    older_store(&path, n);
    super::journal::set_compact_threshold(4 << 10);
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        while log_bytes(&path) < 6 << 10 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
    }
    // Sessions of one create and an idle until the fill completes; the session whose fill completed is
    // the one whose close flips, measured alone.
    for _ in 0..20 {
        let (w0, r0) = (super::catalog::WHOLE_UPGRADE_BATCHES.load(Ordering::Acquire), super::store::CKPT_RELINKED.load(Ordering::Acquire));
        let prefilled = {
            let db = open(&path);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            let _ = trunk.fork_branch().unwrap().into_id();
            let t = std::time::Instant::now();
            while !db.branches.links_fill_state_for_test().1 && t.elapsed() < std::time::Duration::from_secs(30) {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            db.branches.links_fill_state_for_test().1
        };
        if prefilled {
            let whole = super::catalog::WHOLE_UPGRADE_BATCHES.load(Ordering::Acquire) - w0;
            let relinked = super::store::CKPT_RELINKED.load(Ordering::Acquire) - r0;
            println!("LS-CLOSEFLIP rows={n} meta={} whole={whole} relinked={relinked}", meta_version(&path));
            return;
        }
    }
    println!("LS-CLOSEFLIP rows={n} meta={} whole=9999 relinked=9999", meta_version(&path));
}

/// h1gap-rev-c HIGH-3 (ruling 62c11ec63d's counter): the close-time flip of a store whose fill completed
/// in an idle session links no row by the whole-catalog upgrade, and relinks as many rows around its own
/// changes at 3,000 rows as at 300 (one create's: not O(N)). Premise: the flip happened (meta 21).
#[test]
fn the_close_time_flip_relinks_its_own_changes_not_the_catalog() {
    if in_child() {
        return;
    }
    let mut got = Vec::new();
    for rows in ["300", "3000"] {
        let text = child("ls_close_flip_child", &[("FE_LS_ROWS", rows)]);
        let r = readings(&text, "LS-CLOSEFLIP ");
        assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
        let (n, meta, whole, relinked) = (r[0][0], r[0][1], r[0][2], r[0][3]);
        assert_eq!(meta, 21, "{n} rows: premise: the close flipped the store");
        assert_eq!(whole, 0, "{n} rows: the close-time flip ran the whole-catalog upgrade ({whole} batches)");
        got.push(relinked);
    }
    assert_eq!(got[0], got[1], "the close-time flip relinked {} rows at 300 and {} at 3,000: it grows with the catalog", got[0], got[1]);
}
