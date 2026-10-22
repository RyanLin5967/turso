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
            // FE_LS_SHARP (a child's knob, h1-rev-72bfb0127 LOW-1): the sharp checkpoint mode.
            .with_branch_checkpoint(if std::env::var_os("FE_LS_SHARP").is_some() { BranchCheckpoint::Sharp } else { BranchCheckpoint::Fuzzy }),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn meta_version(path: &std::path::Path) -> u32 {
    let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap();
    cat.meta().unwrap().map_or(0, |m| m.format)
}

/// `try_meta_version` polled for up to 30 s; a catalog Busy throughout REFUSES, never reads as a value (lead
/// ruling 19:31:16Z's condition) (FLAGGED harness repair: amendment 28, the
/// 5-module batch on iccad13 at 995da809f, read "meta 0" from one Busy read while a checkpoint held the catalog).
fn poll_meta_version(path: &std::path::Path) -> u32 {
    let t = std::time::Instant::now();
    loop {
        if let Some(m) = try_meta_version(path) {
            return m;
        }
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "REFUSED: the catalog meta row of {} stayed unreadable (Busy) for 30 s", path.display());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
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

/// `log_bytes` for the polls that read a cut (h1-rev-72bfb0127 LOW-7, FLAGGED stricter): a log that cannot
/// be read is refused, never read as 0 B (under any peak, so a cut).
fn log_present(path: &std::path::Path) -> u64 {
    std::fs::metadata(format!("{}-branch-log", path.display()))
        .map(|m| m.len())
        .unwrap_or_else(|e| panic!("the branch log of {} cannot be read: {e}", path.display()))
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
            // ends is what can start the checkpoint that flips the meta row. FLAGGED harness repair
            // (core-integrator, 72bfb0127 in the 5-module batch on eceubuntu1: "never upgraded in 14
            // sessions"): the session's length is counted in the fill's committed batches, two, not
            // in wall time, which a loaded host gives the fill thread less of (30 s at most); then
            // 130 ms more of work, as before.
            let b0 = super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire);
            let t = std::time::Instant::now();
            while super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire) < b0 + 2
                && db.branches.links_fill_state_for_test().0
                && t.elapsed() < std::time::Duration::from_secs(30)
            {
                let _ = trunk.fork_branch().unwrap().into_id();
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let batches = super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire) - b0;
            let t = std::time::Instant::now();
            while t.elapsed() < std::time::Duration::from_millis(130) {
                let _ = trunk.fork_branch().unwrap().into_id();
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            println!("LS-SESSION-BATCHES s={s} batches={batches}");
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
    let sup0 = super::store::LINKS_FILL_SUPERSEDED_BATCHES.load(Ordering::Acquire);
    let mut k = 0;
    while !db.branches.past_hard_limit_for_test() && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    // Operations past the hard limit (each a create; the waits on an install they made, this thread's
    // back-pressure waits), then the checkpoints they started are waited for.
    let (waits0, fill_waits0) = (super::store::thread_waits(), super::store::thread_fill_waits());
    let peak = log_bytes(&path);
    for _ in 0..200 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    // FLAGGED harness repair (amendments 22-24, iccad13: CKPT_OUTCOMES 200,98,0,..., the fill's column-only
    // transaction made once, unfailed, after the 200 creates): until that O(1) transaction is recorded the gate
    // holds every checkpoint (the columns must exist before a fuzzy flight, which may not change the schema), and
    // a held checkpoint starts only at a later operation. So operations go on, one per 50 ms, until the log is cut
    // (30 s at most; the fill, 1 s per 64 rows, is far from done, and "meta 19" below still fails a flip).
    let t = std::time::Instant::now();
    while log_present(&path) >= peak && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    // The waits counted over every operation past the hard limit, these included (the cut may start among them:
    // amendment 25 on iccad8, where the mutant l4_nonflip_fill_untagged's waits all fell here and went uncounted).
    let waits = super::store::thread_waits() - waits0;
    let fill_waits = super::store::thread_fill_waits() - fill_waits0;
    db.branch_checkpoint_wait();
    let whole = super::catalog::WHOLE_UPGRADE_BATCHES.load(Ordering::Acquire) - w0;
    // The asserted readings, taken before the LOW-3 observation below.
    let (meta, log) = (meta_version(&path), log_present(&path));
    // Observing only (the H1 no-cut diagnostic): what the threshold-crossing calls came to, this run.
    let outcomes: Vec<String> = super::store::CKPT_OUTCOMES.iter().map(|c| c.load(Ordering::Acquire).to_string()).collect();
    println!("LS-WHOLE-OUTCOMES {}", outcomes.join(","));
    println!(
        "LS-WHOLE-FILL starts={} columns_ok={} columns_failed={} last_error={:?}",
        db.branches.links_fill_state_for_test().2,
        super::store::LINKS_FILL_COLUMN_TXNS[0].load(Ordering::Acquire),
        super::store::LINKS_FILL_COLUMN_TXNS[1].load(Ordering::Acquire),
        super::store::LINKS_FILL_COLUMN_ERROR.lock().unwrap().clone()
    );
    // h1-rev-72bfb0127 LOW-3 (observing here; asserted by its own test): fill threads started, and batches a
    // superseded fill started, through the end of the run.
    std::thread::sleep(std::time::Duration::from_millis(2_500));
    let starts = db.branches.links_fill_state_for_test().2;
    let superseded = super::store::LINKS_FILL_SUPERSEDED_BATCHES.load(Ordering::Acquire) - sup0;
    super::store::LINKS_FILL_PAUSE_MS.store(0, Ordering::Release);
    println!("LS-WHOLE rows={n} past={} creates={k} whole_batches={whole} waits={waits} meta={meta} peak={peak} log={log} fill_waits={fill_waits} starts={starts} superseded={superseded}", u64::from(past));
}

/// h1-rev-72bfb0127 LOW-1: the whole-catalog red in the SHARP checkpoint mode (the capture and the writer are
/// the fuzzy mode's; the tag is fuzzy-only, so it is not read here): past the hard limit, with a slow fill, no
/// checkpoint links the whole catalog (`WHOLE_UPGRADE_BATCHES` 0), the store stays at 19 and the log is cut,
/// at 1,000 rows. Mutant `l4_upgrade_unbatched` must fail it. Premise: the log passed the hard limit.
#[test]
fn past_the_hard_log_limit_no_sharp_checkpoint_links_the_whole_catalog() {
    if in_child() {
        return;
    }
    let text = child("ls_hard_whole_child", &[("FE_LS_ROWS", "1000"), ("FE_LS_SHARP", "1")]);
    let r = readings(&text, "LS-WHOLE ");
    assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
    let (past, whole, meta, peak, log) = (r[0][1], r[0][3], r[0][5], r[0][6], r[0][7]);
    assert_eq!(past, 1, "sharp: premise: the log never passed the hard limit");
    assert_eq!(whole, 0, "sharp: past the hard limit a checkpoint linked the whole catalog ({whole} batches)");
    assert_eq!(meta, 19, "sharp: the store flipped before its fill was complete (meta {meta})");
    assert!(log < peak, "sharp: the log was not cut past the hard limit ({log} B at the end, {peak} B at the limit)");
}

/// The LOW-3 schedule, forced: a fill's first committed batch is held before it is recorded
/// (`LINKS_FILL_HOLD_UNRECORDED`); past the hard limit the gate stops it and a checkpoint cuts the log; a later
/// threshold starts a second fill (it is held after its first batch too); then the hold is let go, and the
/// first fill, superseded, must record nothing and start no batch.
#[test]
fn ls_superseded_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("s.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_HELD_BATCHES.store(0, Ordering::Release);
    super::store::LINKS_FILL_HOLD_UNRECORDED.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let sup0 = super::store::LINKS_FILL_SUPERSEDED_BATCHES.load(Ordering::Acquire);
    let mut k = 0;
    while super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire) < 1 && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let held_first = super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire);
    // Past the hard limit (the columns are made: the gate stops the fill and lets a checkpoint cut), then on
    // until a second fill has started.
    let (past, peak) = fork_past_hard_and_cut(&db, &trunk, &path);
    let cut = log_present(&path) < peak;
    // On until a later fill has started and is the store's running one (filling), so the first, let go next, is
    // superseded by a live fill (FLAGGED repair, amendment 24 on iccad13: let go while no fill was running, the
    // first fill ended as any stopped fill does, and the mutant survived).
    let mut k = 0;
    while !(db.branches.links_fill_state_for_test().2 >= 2 && db.branches.links_fill_state_for_test().0) && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let (filling, _, starts) = db.branches.links_fill_state_for_test();
    super::store::LINKS_FILL_HOLD_UNRECORDED.store(false, Ordering::Release);
    std::thread::sleep(std::time::Duration::from_secs(2));
    let superseded = super::store::LINKS_FILL_SUPERSEDED_BATCHES.load(Ordering::Acquire) - sup0;
    println!("LS-SUP held={held_first} past={} cut={} starts={starts} superseded={superseded} filling={}", u64::from(past), u64::from(cut), u64::from(filling));
}

/// h1-rev-72bfb0127 LOW-3: a fill that a later spawn superseded (past the hard limit the gate stops a fill and a
/// later checkpoint starts a new one) starts no further batch, so two fills never run batches together or write
/// cursors at a stale generation; superseded fill threads are joined with the flights. Premises: the first fill
/// was held after a committed batch, the store passed the hard limit and cut its log, and a second fill started
/// while the first was held. Mutant `l4_fill_superseded_runs` must fail it.
#[test]
fn past_the_hard_log_limit_a_superseded_fill_starts_no_batch() {
    if in_child() {
        return;
    }
    let text = child("ls_superseded_child", &[]);
    let r = readings(&text, "LS-SUP ");
    assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
    let (held, past, cut, starts, superseded, filling) = (r[0][0], r[0][1], r[0][2], r[0][3], r[0][4], r[0][5]);
    assert_eq!(filling, 1, "premise: no later fill was running when the first was let go");
    println!("LS superseded: held {held} past {past} cut {cut} starts {starts} superseded batches {superseded}");
    assert!(held >= 1, "premise: the first fill's batch was not held");
    assert_eq!((past, cut), (1, 1), "premise: the store did not pass the hard limit and cut its log (past {past}, cut {cut})");
    assert!(starts >= 2, "premise: the fill was started {starts} time(s), never superseded");
    assert_eq!(superseded, 0, "a superseded fill started {superseded} batches (fill started {starts} times)");
}

/// h1gap-rev-c HIGH-2: past the hard log limit, with a slow fill, no checkpoint links the whole catalog
/// inside its capture (an O(N) stall of every create: under the store mutex in the sharp mode, and
/// behind the install every operation waits for in the fuzzy one; the reviewer measured 1.2-4.7 s at
/// N=1000-4000). Counted, not timed, at 1,000 and at 4,000 rows (FLAGGED stricter, lead ruling 62c11ec63d,
/// before its fix): the batches a whole-catalog upgrade ran (`WHOLE_UPGRADE_BATCHES`) are 0. FLAGGED (ruling
/// 4412c8a62b): the claim "the 200 creates past the hard limit waited on no install" is DROPPED (a wrong
/// premise: those waits are F-FZ back-pressure, as many in a store with no fill; diagnostic artie b147708822)
/// and REPLACED by its cause-tagged form: no create waited on the install of a checkpoint whose capture
/// carries fill or upgrade work (`thread_fill_waits`); the untagged waits are reported, never asserted. And
/// the log was cut while the fill was incomplete: the store stays at 19, the log under its peak.
/// Mutant `l4_upgrade_unbatched` (the fill back in the capture) must fail the tagged counter. h1-rev-72bfb0127
/// MED-1: the tag is the fill work a checkpoint's write did (rows its fill batches filled), not the predicate
/// that picks `upgrade_links`, so mutant `l4_nonflip_fill_untagged` (a non-flip write running the whole fill
/// by direct batches) must fail it too; the assertion is unchanged.
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
        let (n, past, whole, waits, meta, peak, log, fill_waits) = (r[0][0], r[0][1], r[0][3], r[0][4], r[0][5], r[0][6], r[0][7], r[0][8]);
        println!("LS whole: {n} rows: waits {waits} (F-FZ back-pressure, reported only), on fill work {fill_waits}, whole batches {whole}");
        for l in text.lines().filter(|l| l.contains("LS-WHOLE")) {
            println!("{l}");
        }
        assert_eq!(past, 1, "{n} rows: premise: the log never passed the hard limit");
        assert_eq!(fill_waits, 0, "{n} rows: creates past the hard limit waited {fill_waits} times on the install of a checkpoint carrying fill or upgrade work");
        assert_eq!(whole, 0, "{n} rows: past the hard limit a checkpoint linked the whole catalog ({whole} batches of {})", super::catalog::UPGRADE_BATCH);
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
    // FLAGGED harness repair, the ruled pattern of 01a02150d/ce77ead23 (lead ruling 2026-10-21T17:58:38Z: a
    // checkpoint the gate or a running WAL truncation defers past the hard limit starts at the next operation):
    // core-integrator, eceubuntu1 at merge f86f69356 (raw 6ad088d13b), "8776 B at the end, 8776 B at its peak": the
    // last of the 600 creates raised the peak and no operation followed it. Operations go on, one per 50 ms, until the
    // log is under that peak (30 s at most); the claim is unchanged.
    let t = std::time::Instant::now();
    while log_present(&path) >= peak && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    db.branch_checkpoint_wait();
    // Observing only (l4-rev-h1 LOW 4, measured, no assertion): busy catalog WAL truncations meanwhile.
    let busy = db.branches.wal_truncate_busy_for_test() - busy0;
    println!("LS-HARD meta={} log={} peak={peak} wal_busy={busy}", meta_version(&path), log_present(&path));
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
    // FLAGGED spec change, lead ruling 2026-10-21 (option (a), MED-3 RESTATED: a fill starved BEYOND the bound
    // does not hold the cut): past the hard limit the gate may defer the cut for the fill by at most one more
    // threshold of log, so the operations go on past that bound (the log at the limit plus 4 KiB, plus 1 KiB of
    // slack), where the 50 creates stopped inside it.
    // They stop at the first cut (FLAGGED repair, amendment 35 on iccad9: going on through 20,000 creates past
    // later cuts let a respawned fill complete and flip, read as "flipped with its fill incomplete").
    let bound = log_present(&path) + (4 << 10) + 1024;
    let (mut k2, mut last) = (0, log_present(&path));
    while last < bound && k2 < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        let now = log_present(&path);
        peak = peak.max(now);
        k2 += 1;
        if now < last {
            break;
        }
        last = now;
    }
    // Read WHILE the batch is still held (l4-rev-h1's corrected variant: nothing the fill does after
    // the hold is let go can count): a checkpoint the operations started runs on a thread of its own,
    // so the meta row is polled for it (wall time only bounds the wait). Then the hold is let go, and
    // only then waited on: checkpoint_wait joins the fill thread, which a hold in place never ends.
    // (Ruling 62c11ec63d: what is polled is the log's cut; the meta row is read once it is.)
    let t = std::time::Instant::now();
    let mut log_held = log_present(&path);
    // Operations go on while the cut is waited for (FLAGGED, the ruled pattern of 01a02150d: a deferred checkpoint
    // starts at the next operation).
    while log_held >= peak && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
        log_held = log_present(&path);
    }
    let meta = poll_meta_version(&path);
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
    // FLAGGED spec change, lead ruling 2026-10-21 (option (a), MED-3 RESTATED: a fill starved BEYOND the bound
    // does not hold the cut): past the hard limit the gate may defer the cut for the fill by at most one more
    // threshold of log, so the operations go on past that bound (the log at the limit plus 4 KiB, plus 1 KiB of
    // slack), where the 50 creates stopped inside it.
    // They stop at the first cut (FLAGGED repair, amendment 35 on iccad9: going on through 20,000 creates past
    // later cuts let a respawned fill complete and flip, read as "flipped with its fill incomplete").
    let bound = log_present(&path) + (4 << 10) + 1024;
    let (mut k2, mut last) = (0, log_present(&path));
    while last < bound && k2 < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        let now = log_present(&path);
        peak = peak.max(now);
        k2 += 1;
        if now < last {
            break;
        }
        last = now;
    }
    // Read before the hold is let go: what the 50 operations started with the fill held. A checkpoint
    // they started runs on a thread of its own, so the meta row is polled for it (wall time only bounds
    // the wait; a fill that held the upgrade never gets there).
    // (Ruling 62c11ec63d: what is polled is the log's cut; the meta row is read once it is.)
    let t = std::time::Instant::now();
    let mut log_held = log_present(&path);
    // Operations go on while the cut is waited for (FLAGGED, the ruled pattern of 01a02150d: a deferred checkpoint
    // starts at the next operation).
    while log_held >= peak && t.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
        log_held = log_present(&path);
    }
    let meta_held = poll_meta_version(&path);
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
        // h1-rev-72bfb0127 HIGH-1 (observing here; asserted by its own test): the cursor's generation after the
        // older build's checkpoint.
        let cursor_gen1 = ls_first(&ls_cat(&path), "SELECT generation FROM link_fill");
        {
            let db = open(&path);
            db.branch_wait_name_filter();
            let _ = db.branch_compact_now();
            db.branch_checkpoint_wait();
        }
        let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
        println!(
            "LS-KILL k={k} killed={} killed_meta={killed_meta} cursor_gen={cursor_gen} cursor_done={cursor_done} gen0={gen0} gen1={gen1} meta={} rows={rows} wrong={wrong} cursor_gen1={cursor_gen1}",
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


/// h1-rev-72bfb0127 HIGH-1: the older build stood in for (`CREATE_LEGACY` with `LEGACY_FACTS`: a 19 writer)
/// must checkpoint as a real 19 build does, which knows nothing of the fill: its checkpoint neither relinks
/// nor carries the fill's cursor to its new generation, so the cursor it leaves is of the generation before
/// it, and this build refills (the MED-2 red's older session, read after it). At 72bfb0127 the stand-in ran
/// this build's non-flip relink and carry, so MED-2's red no longer killed `l4_fill_generation_ignored`.
#[test]
fn an_older_builds_checkpoint_does_not_carry_the_fill_cursor() {
    if in_child() {
        return;
    }
    let text = child("ls_kill_resume_child", &[]);
    let r = readings(&text, "LS-KILL ");
    assert_eq!(r.len(), 3, "premise: three arms ran:\n{text}");
    for v in &r {
        let (k, cgen, gen0, gen1, cgen1) = (v[0], v[3], v[5], v[6], v[10]);
        assert_eq!(cgen, gen0, "k={k}: premise: the cursor is of the killed store's generation");
        assert!(gen1 > gen0, "k={k}: premise: the older build's checkpoint moved the generation ({gen0} -> {gen1})");
        assert_eq!(cgen1, cgen, "k={k}: the older build's checkpoint carried the fill cursor to its generation ({cgen} -> {cgen1}, catalog {gen1})");
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
    // The checkpoints the creates started, installed (a fuzzy one waited for; the fill is not joined). FLAGGED
    // harness repair (as in ls_hard_whole_child, amendments 22-24 on iccad13): operations go on while it waits, one
    // per 50 ms, since a checkpoint the gate held until the fill's column-only transaction starts at a later one.
    let t = std::time::Instant::now();
    while log_present(&path) >= peak && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    // The meta row read Busy-tolerant (a checkpoint may hold the catalog: FLAGGED harness repair, measured at
    // 37917a0a6 on iccad6, "unwrap() on Err Busy" at meta_version).
    // (Refusing on a 30 s Busy since the 19:31:16Z ruling's condition.)
    let meta = poll_meta_version(&path);
    println!("LS-NFCUT past={} meta={meta} peak={peak} log={}", u64::from(db.branches.past_hard_limit_for_test()), log_present(&path));
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


// ---- h1-rev-72bfb0127 HIGH-2: a non-flip cut that changed rows behind the fill's cursor, then a crash ----

/// The fill's cursor and the catalog's generation, `(cursor generation, parent, fork_epoch, done, meta
/// generation)`, or `None` while the catalog cannot be read (a checkpoint holds it: Busy) or has no cursor.
fn try_cursor(path: &std::path::Path) -> Option<(u64, u64, u64, u64, u64)> {
    let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).ok()?;
    let g = cat.meta().ok()??.generation;
    let (cg, at, done) = cat.fill_cursor().ok()??;
    let (p, f) = at.unwrap_or((0, 0));
    Some((cg, p, f, u64::from(done), g))
}

/// `try_cursor`, polled for up to 30 s.
fn poll_cursor(path: &std::path::Path) -> Option<(u64, u64, u64, u64, u64)> {
    let t = std::time::Instant::now();
    loop {
        if let Some(c) = try_cursor(path) {
            return Some(c);
        }
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "REFUSED: the fill cursor of {} stayed unreadable (Busy, or no cursor) for 30 s", path.display());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// The process cut short: a 19 store of 1,050 rows with a slow fill; once the fill's cursor is past the
/// first 130 trunk forks, the store goes past the hard limit, then reaps forks 60-99 (every third) and forks
/// a child of each of the first 30, all BEHIND the cursor; a checkpoint after them cuts the log at 19, and
/// the process exits with no close.
#[test]
fn ls_nonflip_behind_child() {
    let Ok(p) = std::env::var("FE_LS_NFB_PATH") else {
        return;
    };
    let path = std::path::PathBuf::from(p);
    let ids = ls_store_with_ids(&path, 1_000);
    let (mark_par, mark_fe) = {
        let cat = ls_cat(&path);
        let sql = |c: &str| format!("SELECT {c} FROM branch WHERE id = {}", ids[130].0);
        (ls_first(&cat, &sql("parent")), ls_first(&cat, &sql("fork_epoch")))
    };
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_PAUSE_MS.store(1_000, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    start_fill_past(&db, &trunk, &path, (mark_par, mark_fe));
    // Past the hard limit, and its first cut done (below the hard limit again: no checkpoint in flight, and
    // none starts before the next pass, the fill being waited for).
    let (past, _) = fork_past_hard_and_cut(&db, &trunk, &path);
    for id in ids.iter().skip(60).take(40).step_by(3) {
        db.branch(*id).unwrap().reap().unwrap();
    }
    for id in ids.iter().take(30) {
        let b = db.branch(*id).unwrap();
        let _ = b.fork().unwrap().into_id();
        let _ = b.into_id();
    }
    // The cut that covers the changes: every checkpoint after them starts after them.
    let (past2, peak) = fork_past_hard_and_cut(&db, &trunk, &path);
    let (cgen, cpar, cfe, cdone, mgen) = poll_cursor(&path).unwrap_or((0, 0, 0, 9, 9999));
    let meta = poll_meta_version(&path);
    println!(
        "LS-NFBEHIND past={} meta={meta} peak={peak} log={} cgen={cgen} mgen={mgen} cpar={cpar} cfe={cfe} cdone={cdone} markpar={mark_par} markfe={mark_fe}",
        u64::from(past && past2),
        log_present(&path)
    );
    std::process::exit(0);
}

/// Fork from trunk until a checkpoint has started the fill (the gate spawns it), then wait (60 s at most)
/// for its cursor to pass `mark`, a (parent, fork_epoch) key.
fn start_fill_past(db: &Arc<Database>, trunk: &Arc<crate::Connection>, path: &std::path::Path, mark: (u64, u64)) {
    let mut k = 0;
    while db.branches.links_fill_state_for_test().2 == 0 && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let t = std::time::Instant::now();
    while t.elapsed() < std::time::Duration::from_secs(60) {
        if try_cursor(path).is_some_and(|(_, p, f, _, _)| (p, f) > mark) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Fork from trunk until the log passes the hard limit (`past`, read then), then 200 more, then wait (30 s at
/// most) for a checkpoint to cut the log under `peak`, its size at the limit, forking one more every 50 ms
/// meanwhile (operations go on, as in use: a checkpoint the fill gate held starts at the next one; measured on
/// iccad13, amendment 22, where 200 creates ran before the fill's column-only transaction was recorded).
/// `(past, peak)`.
fn fork_past_hard_and_cut(db: &Arc<Database>, trunk: &Arc<crate::Connection>, path: &std::path::Path) -> (bool, u64) {
    let mut k = 0;
    while !db.branches.past_hard_limit_for_test() && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    let peak = log_present(path);
    for _ in 0..200 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let t = std::time::Instant::now();
    while log_present(path) >= peak && t.elapsed() < std::time::Duration::from_secs(30) {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    (past, peak)
}

/// h1-rev-72bfb0127 HIGH-2: the non-flip cut's relink is what keeps a crash's reopen right. Past the hard
/// limit, with the fill's cursor past the rows it changes, the store reaps and forks behind the cursor; a
/// checkpoint after those changes cuts the log at 19 and carries the cursor; the process crashes; this build
/// reopens, resumes the fill from the carried cursor, flips, and every row's links are right by the
/// independent oracle. Premises: past the hard limit, the log cut after the changes, still 19, the cursor
/// carried to the catalog's generation, not done, and past the changed rows. Mutant `l4_nonflip_no_relink`
/// (the non-flip checkpoint skips its relink, the cursor still carried) must fail it.
#[test]
fn a_crash_after_a_non_flip_cut_that_changed_rows_behind_the_cursor_reopens_with_every_link_right() {
    if in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("nfb.db");
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::links_sessions_tests::ls_nonflip_behind_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_LS_NFB_PATH", &path)
        .env_remove("FE_LS_CHILD")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let r = readings(&text, "LS-NFBEHIND ");
    assert_eq!(r.len(), 1, "premise: the crashed process reported:\n{text}");
    let v = &r[0];
    let (past, meta, peak, log, cgen, mgen, cpar, cfe, cdone, mpar, mfe) = (v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10]);
    assert_eq!(past, 1, "premise: the store was past the hard limit: {text}");
    assert!(log < peak, "premise: the log was not cut after the changes ({log} B, peak {peak} B)");
    assert_eq!(meta, 19, "premise: the store flipped before the crash (meta {meta})");
    assert_eq!((cgen, cdone), (mgen, 0), "premise: the cursor is not carried to the catalog's generation, or is done (cursor generation {cgen}, catalog {mgen}, done {cdone})");
    assert!((cpar, cfe) > (mpar, mfe), "premise: the cursor ({cpar}, {cfe}) is not past the changed rows ({mpar}, {mfe})");
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    }
    let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
    println!("LS non-flip behind the cursor: meta={} rows={rows} wrong={wrong}", meta_version(&path));
    assert!(rows > 1_000, "premise: the rows are there ({rows})");
    assert_eq!(meta_version(&path), 21, "the store did not upgrade after the crash");
    assert_eq!(wrong, 0, "{wrong} of {rows} rows have wrong links after a crash past a non-flip cut that changed rows behind the cursor");
}


// ---- h1-rev-72bfb0127 HIGH-1: a REAL older build between a non-flip cut and the flip ----

/// This build's part of the cross-build test: a 19 store a real older build made, opened past the hard limit
/// with a slow fill, its log cut by a non-flip checkpoint; then the process exits with no close.
#[test]
fn ls_xbuild_cut_child() {
    let Ok(p) = std::env::var("FE_LS_XB_PATH") else {
        return;
    };
    let path = std::path::PathBuf::from(p);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_PAUSE_MS.store(1_000, Ordering::Release);
    let mark: (u64, u64) = (
        std::env::var("FE_LS_XB_MARKPAR").unwrap().parse().unwrap(),
        std::env::var("FE_LS_XB_MARKFE").unwrap().parse().unwrap(),
    );
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    // The fill's cursor past the rows the older build changes, then past the hard limit and cut.
    start_fill_past(&db, &trunk, &path, mark);
    let (past, peak) = fork_past_hard_and_cut(&db, &trunk, &path);
    let (cgen, cpar, cfe, cdone, mgen) = poll_cursor(&path).unwrap_or((0, 0, 0, 9, 9999));
    let meta = poll_meta_version(&path);
    println!(
        "LS-XBCUT past={} meta={meta} peak={peak} log={} cgen={cgen} mgen={mgen} cpar={cpar} cfe={cfe} cdone={cdone}",
        u64::from(past),
        log_present(&path)
    );
    std::process::exit(0);
}

/// Run a phase of the REAL older build (`xbuild_old_overlay.rs`, built at 6619b0312 as its test binary,
/// named by `FE_XB_OLD_BIN`), refusing one that ran no test or failed.
fn old_build(bin: &str, phase: &str, path: &std::path::Path) -> String {
    let out = std::process::Command::new(bin)
        .args(["branch::xbuild_old_tests::xb_old_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_XB_PHASE", phase)
        .env("FE_XB_PATH", path)
        .env_remove("FE_LS_CHILD")
        .env_remove("FE_MUTANT")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("running 1 test") && text.contains(&format!("XB-OLD phase={phase} ")), "the older build's {phase} ran no test:\n{text}");
    assert!(out.status.success(), "the older build's {phase} failed:\n{text}");
    text
}

/// h1-rev-72bfb0127 HIGH-1 (condition (2)'s older-build half, ruling 62c11ec63d, under a REAL older build):
/// a 19 store made by the real 19 build (6619b0312, `FE_XB_OLD_BIN`); this build past the hard limit cuts its
/// log at 19 with its fill incomplete and the cursor carried, and crashes; the real older build then reaps and
/// forks behind the cursor and checkpoints (moving the generation, not the cursor); this build reopens, refills
/// and flips, and every row's links are right by the independent oracle. Premises: the older build made a 19
/// store; the cut, still 19, the cursor carried and not done and past the changed rows; after the older
/// session the catalog's generation moved and the cursor's did not. Mutant `l4_fill_generation_ignored` (this
/// build resumes a cursor of another generation) must fail it. Ignored in a plain run: it needs the older
/// build's binary, and refuses without it.
#[test]
#[ignore = "needs FE_XB_OLD_BIN, a real 19 build's test binary (xbuild_old_overlay.rs)"]
fn a_crash_after_a_non_flip_cut_then_a_real_older_builds_session_reopens_with_every_link_right() {
    if in_child() {
        return;
    }
    let bin = std::env::var("FE_XB_OLD_BIN").expect("REFUSED: FE_XB_OLD_BIN (a real 19 build's test binary) is not set");
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("xb.db");
    old_build(&bin, "make", &path);
    assert_eq!(meta_version(&path), 19, "premise: the older build made a 19 store");
    let ids: Vec<u64> = std::fs::read_to_string(format!("{}.ids", path.display())).unwrap().lines().map(|l| l.parse().unwrap()).collect();
    let (mpar, mfe) = {
        let cat = ls_cat(&path);
        let sql = |c: &str| format!("SELECT {c} FROM branch WHERE id = {}", ids[130]);
        (ls_first(&cat, &sql("parent")), ls_first(&cat, &sql("fork_epoch")))
    };
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::links_sessions_tests::ls_xbuild_cut_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_LS_XB_PATH", &path)
        .env("FE_LS_XB_MARKPAR", mpar.to_string())
        .env("FE_LS_XB_MARKFE", mfe.to_string())
        .env_remove("FE_LS_CHILD")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let r = readings(&text, "LS-XBCUT ");
    assert_eq!(r.len(), 1, "premise: the crashed process reported:\n{text}");
    let v = &r[0];
    let (past, meta, peak, log, cgen, mgen, cpar, cfe, cdone) = (v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8]);
    assert_eq!(past, 1, "premise: the store was past the hard limit: {text}");
    assert!(log < peak, "premise: the log was not cut ({log} B, peak {peak} B)");
    assert_eq!(meta, 19, "premise: the store flipped before the crash (meta {meta})");
    assert_eq!((cgen, cdone), (mgen, 0), "premise: the cut did not carry an unfinished cursor (cursor generation {cgen}, catalog {mgen}, done {cdone})");
    assert!((cpar, cfe) > (mpar, mfe), "premise: the cursor ({cpar}, {cfe}) is not past the rows the older build changes ({mpar}, {mfe})");
    old_build(&bin, "session", &path);
    let (cgen1, gen1) = {
        let mut cat = ls_cat(&path);
        (ls_first(&cat, "SELECT generation FROM link_fill"), cat.meta().unwrap().map_or(0, |m| m.generation))
    };
    assert!(gen1 > mgen, "premise: the older build's checkpoint did not move the generation ({mgen} -> {gen1})");
    assert_eq!(cgen1, cgen, "premise: the older build moved the fill cursor ({cgen} -> {cgen1})");
    assert_eq!(meta_version(&path), 19, "premise: the older build left the store at 19");
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    }
    let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
    println!("LS real older build: gen {mgen} -> {gen1}, cursor {cgen1}; meta={} rows={rows} wrong={wrong}", meta_version(&path));
    assert!(rows > 1_000, "premise: the rows are there ({rows})");
    assert_eq!(meta_version(&path), 21, "the store did not upgrade after the older build's session");
    assert_eq!(wrong, 0, "{wrong} of {rows} rows have wrong links after a real older build's session between a non-flip cut and the flip");
}


// ---- lead ruling 2026-10-21T17:58:38Z: a store closed while its checkpoint is held ----

/// A 19 store past the hard limit whose checkpoints the fill gate holds (the fill held before its column-only
/// transaction, so the columns are never recorded), closed so: the close must cut its log.
#[test]
fn ls_close_held_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_HOLD_BEFORE_COLUMNS.store(true, Ordering::Release);
    let held0 = super::store::CKPT_OUTCOMES[0].load(Ordering::Acquire);
    let (past, peak, log_open) = {
        let db = open(&path);
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        let mut k = 0;
        while !(db.branches.past_hard_limit_for_test() && super::store::LINKS_FILL_BEFORE_COLUMNS_HELD.load(Ordering::Acquire) >= 1) && k < 20_000 {
            let _ = trunk.fork_branch().unwrap().into_id();
            k += 1;
        }
        let past = db.branches.past_hard_limit_for_test();
        let peak = log_present(&path);
        for _ in 0..50 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        let log_open = log_present(&path);
        // The close (the store's Drop) stops the fill, then joins it: let the hold go once the close has begun.
        let release = std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(300));
            super::store::LINKS_FILL_HOLD_BEFORE_COLUMNS.store(false, Ordering::Release);
        });
        drop(trunk);
        drop(db);
        release.join().unwrap();
        (past, peak, log_open)
    };
    let held = super::store::CKPT_OUTCOMES[0].load(Ordering::Acquire) - held0;
    let (log_closed, meta_closed) = (log_present(&path), meta_version(&path));
    {
        let db = open(&path);
        db.branch_wait_name_filter();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    }
    let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
    println!(
        "LS-CLOSE past={} held={held} peak={peak} log_open={log_open} log_closed={log_closed} meta_closed={meta_closed} meta={} rows={rows} wrong={wrong}",
        u64::from(past),
        meta_version(&path)
    );
}

/// Lead ruling 2026-10-21T17:58:38Z (the held checkpoint is deferral, not a stall), its condition: a store
/// CLOSED while the fill gate holds its checkpoint past the hard limit cuts its log at the close (at its
/// current 19, no flip), and reopens with every row's links right by the independent oracle once its fill is
/// done. Premises: past the hard limit, checkpoints held by the gate, the log uncut while open. Mutant
/// `l4_no_cut_at_close` (the close skips the held checkpoint) must fail it.
#[test]
fn a_store_closed_with_its_checkpoint_held_cuts_its_log_at_the_close() {
    if in_child() {
        return;
    }
    let text = child("ls_close_held_child", &[]);
    let r = readings(&text, "LS-CLOSE ");
    assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
    let (past, held, peak, log_open, log_closed, meta_closed, meta, rows, wrong) = (r[0][0], r[0][1], r[0][2], r[0][3], r[0][4], r[0][5], r[0][6], r[0][7], r[0][8]);
    println!("LS close-held: past {past} held {held} peak {peak} open {log_open} closed {log_closed} meta {meta_closed} -> {meta} rows {rows} wrong {wrong}");
    assert_eq!(past, 1, "premise: the store never passed the hard limit");
    assert!(held > 0, "premise: no checkpoint was held by the fill gate past the hard limit");
    assert!(log_open >= peak, "premise: the log was cut while open ({log_open} B, peak {peak} B)");
    assert!(log_closed < peak, "a store closed past the hard limit with its checkpoint held kept its uncut log ({log_closed} B, peak {peak} B)");
    assert_eq!(meta_closed, 19, "the close flipped a store whose fill was not complete (meta {meta_closed})");
    assert!(rows > 1_000, "premise: the rows are there ({rows})");
    assert_eq!(meta, 21, "the reopened store did not upgrade");
    assert_eq!(wrong, 0, "{wrong} of {rows} rows have wrong links after a close that cut a held checkpoint's log");
}

// ---- lead ruling 2026-10-21T18:01:45Z: the fill's liveness under sustained create load ----

/// Sustained creates on a 19 store with its fill running unpaused: every cut and fill spawn counted.
#[test]
fn ls_fill_live_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("l.db");
    let n: usize = std::env::var("FE_LS_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000);
    older_store(&path, n);
    super::journal::set_compact_threshold(4 << 10);
    let (e0, b0) = (super::store::LINKS_FILL_EMPTY_SPAWNS.load(Ordering::Acquire), super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire));
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let (mut cuts, mut last, mut grown, mut grown_creates) = (0u64, log_present(&path), 0u64, 0u64);
    let mut creates = 0u64;
    // Until the fill completes (cuts after it need no fill) or 60,000 creates.
    while creates < 60_000 && !db.branches.links_fill_state_for_test().1 {
        let _ = trunk.fork_branch().unwrap().into_id();
        creates += 1;
        let now = log_present(&path);
        if now < last {
            cuts += 1;
        } else {
            grown += now - last;
            grown_creates += 1;
        }
        last = now;
    }
    let (_, prefilled, starts) = db.branches.links_fill_state_for_test();
    let empty = super::store::LINKS_FILL_EMPTY_SPAWNS.load(Ordering::Acquire) - e0;
    let batches = super::store::LINKS_FILL_HELD_BATCHES.load(Ordering::Acquire) - b0;
    let per_create = grown / grown_creates.max(1);
    // Completed, the flip and the links: the flip's checkpoint, then the independent oracle (FLAGGED stricter: a
    // fill that completes early must still leave every row's links right).
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    drop(trunk);
    drop(db);
    let (rows, wrong) = ls_wrong_links(&path).unwrap_or((0, 9999));
    println!("LS-LIVE-LINKS meta={} rows={rows} wrong={wrong}", meta_version(&path));
    println!("LS-LIVE creates={creates} cuts={cuts} starts={starts} empty={empty} batches={batches} prefilled={} per_create={per_create}", u64::from(prefilled));
}

/// Lead ruling 2026-10-21T18:01:45Z (fill LIVENESS). A cut stopped the fill at once, so it gained about one
/// batch (64 rows) per hard-limit cycle while the catalog gained the cycle's creates, about 195: under sustained
/// creates the store never upgraded (amendment 30, both kinds). Fixed by the fill's HORIZON (dfcee5b8d; the
/// bounded deferral first tried with it is removed as dead code, lead ruling: B = 0 changed no outcome). CLAIM
/// (FLAGGED spec change of 3440b3aad's red): under sustained creates the fill completes, flips, and leaves every
/// row's links right, within the registered bound below, at 1,000 and 4,000 rows (FE_LS_LIVE_ROWS overrides the
/// row counts: the held-out check). Premise: the log was cut. Mutants `l4_fill_starved`,
/// `l4_fill_horizon_ignored` and `l4_fill_horizon_low` must fail it.
#[test]
fn under_sustained_creates_the_fill_completes() {
    if in_child() {
        return;
    }
    let list = std::env::var("FE_LS_LIVE_ROWS").unwrap_or_else(|_| "1000,4000".to_string());
    for rows in list.split(',') {
        let text = child("ls_fill_live_child", &[("FE_LS_ROWS", rows)]);
        let r = readings(&text, "LS-LIVE ");
        assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
        let (creates, cuts, starts, empty, batches, prefilled, per_create) = (r[0][0], r[0][1], r[0][2], r[0][3], r[0][4], r[0][5], r[0][6]);
        let k = (2 * (4u64 << 10)) / per_create.max(1);
        println!("LS live: {rows} rows: creates {creates} cuts {cuts} starts {starts} empty spawns {empty} batches {batches} prefilled {prefilled}; {per_create} B per create, K = {k}");
        assert!(cuts >= 1, "{rows} rows: premise: the log was never cut under {creates} creates");
        assert_eq!(prefilled, 1, "{rows} rows: under {creates} sustained creates the fill never completed ({batches} batches over {cuts} hard-limit cycles, K = {k} creates each)");
        // The bound: at least one batch per hard-limit cycle; the fill's rows are the store's at the horizon,
        // rows + 50 children; so (ceil((rows + 50) / 64) + 1) cycles. A cycle's length is NOT derived: its 3 x
        // threshold form was under amendment 35's 300-320 creates per cycle, and the term it missed is not
        // named. The 4 x threshold / bytes-per-create cycle is REGISTERED (linksh1run amendment 37) and held out
        // on hosts and row counts not used to choose it (lead ruling 2026-10-21).
        let n: u64 = rows.parse().unwrap();
        let cycle = (4 * (4u64 << 10)) / per_create.max(1);
        let bound = ((n + 50).div_ceil(64) + 1) * cycle;
        assert!(creates <= bound, "{rows} rows: the fill took {creates} creates to complete, past its bound {bound} ({cycle} creates per cycle)");
        let l = readings(&text, "LS-LIVE-LINKS ");
        let (meta, all, wrong) = (l[0][0], l[0][1], l[0][2]);
        assert_eq!(meta, 21, "{rows} rows: the completed store did not flip (meta {meta})");
        assert_eq!(wrong, 0, "{rows} rows: {wrong} of {all} rows have wrong links after a fill completed under sustained creates");
    }
}

/// Lead ruling 2026-10-21 (MED-3 restated; the deferral is removed, so this holds with room): a fill that never
/// commits a batch (held at its start) does not hold the cut past the hard limit beyond one more threshold of log: the largest log before the
/// cut is at most the log at the limit plus 4 KiB plus 2 KiB of slack (the creates that land while the cut's
/// flight runs). Premises: past the hard limit with the fill held, and a cut. Mutant
/// `l4_fill_waits_past_hard_limit` (the gate waits for the fill, unbounded) must fail it.
#[test]
fn ls_defer_bound_child() {
    if !in_child() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("d.db");
    older_store(&path, 1_000);
    super::journal::set_compact_threshold(4 << 10);
    super::store::LINKS_FILL_HOLD_AT_ENTRY.store(true, Ordering::Release);
    let db = open(&path);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut k = 0;
    while !(db.branches.past_hard_limit_for_test() && super::store::LINKS_FILL_ENTRY_HELD.load(Ordering::Acquire) >= 1) && k < 20_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        k += 1;
    }
    let past = db.branches.past_hard_limit_for_test();
    let at_limit = log_present(&path);
    let (mut max, mut last, mut cut, mut n) = (at_limit, at_limit, false, 0);
    while !cut && n < 2_000 {
        let _ = trunk.fork_branch().unwrap().into_id();
        n += 1;
        let now = log_present(&path);
        cut = now < last;
        max = max.max(now);
        last = now;
    }
    super::store::LINKS_FILL_HOLD_AT_ENTRY.store(false, Ordering::Release);
    db.branch_checkpoint_wait();
    println!("LS-DEFER past={} at_limit={at_limit} max={max} cut={} creates={n}", u64::from(past), u64::from(cut));
}

#[test]
fn past_the_hard_log_limit_a_starved_fill_defers_the_cut_by_at_most_one_threshold() {
    if in_child() {
        return;
    }
    let text = child("ls_defer_bound_child", &[]);
    let r = readings(&text, "LS-DEFER ");
    assert_eq!(r.len(), 1, "premise: the child reported:\n{text}");
    let (past, at_limit, max, cut, n) = (r[0][0], r[0][1], r[0][2], r[0][3], r[0][4]);
    println!("LS defer bound: past {past} at the limit {at_limit} B, max {max} B, cut {cut} after {n} creates");
    assert_eq!(past, 1, "premise: the store never passed the hard limit");
    assert_eq!(cut, 1, "past the hard limit a starved fill held the cut through {n} creates (log {max} B, {at_limit} B at the limit)");
    assert!(max <= at_limit + (4 << 10) + (2 << 10), "the cut was deferred past one threshold of log: {max} B, {at_limit} B at the limit");
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
