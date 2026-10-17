//! The per-store counter class (lead ruling d9cf2b609b; the L2(d) review's LOW 4, REVIEW-claude-engine-
//! L2d-1dedc140a..f666f19bd.md): every counter a budget or a test reads is ONE store's. Another store
//! doing work, on another thread, must not move it: else a `== before` claim fails falsely and a
//! `== before + 1` premise passes on another test's capture.

use super::*;
use crate::{Database, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::sync::Arc;

fn open(path: &std::path::Path, sync: SyncClass, ckpt: BranchCheckpoint) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync })
            .with_branch_checkpoint(ckpt),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// Store B's work, on a thread of its own: creates, trunk commits, a fuzzy checkpoint, and a sharp
/// one (syncs, captures, frozen rows, an install, syncs under its store mutex).
fn work_elsewhere(path: std::path::PathBuf) {
    std::thread::spawn(move || {
        let db = open(&path, SyncClass::Fsync, BranchCheckpoint::Fuzzy);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for i in 0..20 {
            trunk.execute(format!("INSERT INTO t VALUES ({i}, 'x')")).unwrap();
            trunk.create_branch(&format!("b{i}")).unwrap();
        }
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: B's fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        for i in 20..30 {
            trunk.create_branch(&format!("b{i}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    })
    .join()
    .unwrap();
}

/// A's store counters do not move while B works.
#[test]
fn another_stores_work_moves_none_of_this_stores_counters() {
    let dir = tempfile::TempDir::new().unwrap();
    let a = open(&dir.path().join("a.db"), SyncClass::Fsync, BranchCheckpoint::Fuzzy);
    let trunk = a.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    trunk.create_branch("a0").unwrap();
    a.branch_compact_now().unwrap();
    a.branch_checkpoint_wait();
    let before = a.branches.test_counts();
    assert!(before.capture_entered > 0, "premise: A's own checkpoint was counted ({before:?})");
    work_elsewhere(dir.path().join("b.db"));
    let after = a.branches.test_counts();
    assert_eq!(after, before, "store B's work on another thread moved store A's counters");
}

/// A's sync count does not move while B syncs, and it does move with A's own sync.
#[test]
fn another_stores_syncs_are_not_this_stores() {
    let dir = tempfile::TempDir::new().unwrap();
    let a = open(&dir.path().join("a.db"), SyncClass::Fsync, BranchCheckpoint::Fuzzy);
    let trunk = a.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let before = super::sync_counts_of(&a);
    work_elsewhere(dir.path().join("b.db"));
    let after = super::sync_counts_of(&a);
    assert_eq!(after, before, "store B's syncs on another thread were counted as store A's");
    trunk.create_branch("a0").unwrap();
    let own = super::sync_counts_of(&a);
    assert!(own.fsync > after.fsync, "control: A's own create synced and was not counted ({after:?} -> {own:?})");
}

fn open_with(path: &std::path::Path, durability: BranchDurability, ckpt: BranchCheckpoint) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new().with_branch_durability(durability).with_branch_checkpoint(ckpt),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// The child of `every_store_count_has_a_store`: every store lifecycle this suite drives (catalog
/// stores fuzzy and sharp in each sync class, a snapshot store, a volatile one: creates, trunk
/// commits, first writes, deletes, checkpoints, a close and a recovery), then the count of
/// store counts made in no store's scope.
#[test]
fn counts_lifecycle_child() {
    if std::env::var_os("FE_COUNTS_CHILD").is_none() {
        return;
    }
    let dir = tempfile::TempDir::new().unwrap();
    let arms: &[(BranchDurability, BranchCheckpoint)] = &[
        (BranchDurability::Catalog { sync: SyncClass::Fsync }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::FullFsync }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::Off }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::Fsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Durable { sync: SyncClass::Fsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Durable { sync: SyncClass::FullFsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Volatile, BranchCheckpoint::Sharp),
    ];
    for (i, &(durability, ckpt)) in arms.iter().enumerate() {
        let path = dir.path().join(format!("s{i}.db"));
        for round in 0..2 {
            let db = open_with(&path, durability, ckpt);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            if round == 0 {
                trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
            }
            for j in 0..12 {
                trunk.execute(format!("INSERT INTO t VALUES ({}, 'x')", round * 100 + j)).unwrap();
                trunk.create_branch(&format!("r{round}b{j}")).unwrap();
            }
            let b = db.connect_named(&format!("r{round}b3")).unwrap();
            b.execute("UPDATE t SET v = 'mine' WHERE id = 1").unwrap();
            drop(b);
            db.drop_branch(&format!("r{round}b5")).unwrap();
            if ckpt == BranchCheckpoint::Fuzzy {
                let _ = db.branch_checkpoint_fuzzy_now();
                db.branch_checkpoint_wait();
            }
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            drop(trunk);
        }
    }
    println!("COUNTS-UNSCOPED {}", super::store::UNSCOPED.load(std::sync::atomic::Ordering::Relaxed));
}

/// Every count a store makes is made in a store's scope (its mutex held, one of its threads, or a
/// call of its that writes outside the mutex): the child's lifecycle across every arm leaves
/// `UNSCOPED` at 0. Run alone in a child, since a test's own direct sync of a bare file is no
/// store's.
#[test]
fn every_store_count_has_a_store() {
    if std::env::var_os("FE_COUNTS_CHILD").is_some() {
        return;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::counter_scope_tests::counts_lifecycle_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_COUNTS_CHILD", "1")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "the lifecycle child failed:\n{text}");
    let n: u64 = text
        .lines()
        .find_map(|l| l.find("COUNTS-UNSCOPED ").map(|i| l[i + 16..].trim().parse().unwrap()))
        .unwrap_or_else(|| panic!("premise: the lifecycle child ran:\n{text}"));
    assert_eq!(n, 0, "{n} store counts were made in no store's scope: a counter is not per store there");
}

/// The per-store counter class, the held-syscall budget (lead ruling 67664df569): in a parallel test
/// run, `a_fuzzy_checkpoint_issues_at_most_two_syscalls_under_the_store_mutex` REFUSED (raws
/// pc-branch-base_177f007c8 and pc-branch2_eb2f14846: "only 0 of up to 12 rounds could be
/// attributed by thread"), because its count is the task's and another test's threads ran. Here
/// another store works on another thread while that test runs: it must reach a verdict (its CLAIM,
/// red or green), never the refusal. The test itself is called unchanged; its threshold is not read
/// here.
#[test]
fn the_fuzzy_held_syscall_budget_is_judged_beside_another_stores_work() {
    // Counters review E3 (FLAGGED strengthening of this red; its claim unchanged): the other store's
    // work is proven to overlap the RELAY (the test's own call, which relays its measurement to a
    // child process: review 2 LOW 6, it does not reach the measurement itself), by a start signal
    // after its first create and its create count rising between the call's entry and exit; and only
    // a verdict passes: Ok, or a panic whose message starts with "CLAIM:". It guards the relay: with
    // the measurement back in this process the refusal returns.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as O};
    let stop = Arc::new(AtomicBool::new(false));
    let creates = Arc::new(AtomicU64::new(0));
    // Review 2 MED 5: a channel with a deadline, not a barrier, so a busy thread that fails before
    // its first create fails the test instead of hanging it.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let dir = tempfile::TempDir::new().unwrap();
    let busy = {
        let (stop, creates, started, path) = (stop.clone(), creates.clone(), started_tx, dir.path().join("busy.db"));
        std::thread::spawn(move || {
            let db = open(&path, SyncClass::Fsync, BranchCheckpoint::Fuzzy);
            let trunk = db.connect().unwrap();
            trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
            let mut first = true;
            while !stop.load(O::Acquire) {
                trunk.create_branch(&format!("busy{}", creates.load(O::Relaxed))).unwrap();
                creates.fetch_add(1, O::Release);
                if first {
                    let _ = started.send(());
                    first = false;
                }
            }
        })
    };
    if started_rx.recv_timeout(std::time::Duration::from_secs(60)).is_err() {
        let why = if busy.is_finished() { "it ended (panicked?)" } else { "no first create in 60 s" };
        stop.store(true, O::Release);
        panic!("premise: the other store never started working: {why}");
    }
    let at_entry = creates.load(O::Acquire);
    let judged = std::panic::catch_unwind(super::fastest_tests::a_fuzzy_checkpoint_issues_at_most_two_syscalls_under_the_store_mutex);
    let at_exit = creates.load(O::Acquire);
    stop.store(true, O::Release);
    busy.join().unwrap();
    assert!(at_exit > at_entry, "premise: the other store's work overlaps the relay ({at_entry} -> {at_exit} creates)");
    if let Err(e) = judged {
        let msg = e.downcast_ref::<String>().cloned().or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
        assert!(
            msg.starts_with("CLAIM:"),
            "beside another store's work ({} creates during the call) the budget reached no verdict: {msg}",
            at_exit - at_entry
        );
    }
}

/// The child of `a_single_store_runs_own_counts_equal_the_process_counts` (counters review HIGH-2):
/// each arm's whole life, opened through a SYMLINKED directory (so the path a file is opened by is not
/// the store's real one, as macOS's /var is /private/var), including a trunk WAL checkpoint and the
/// close; then, per arm, the syncs counted as the store's against the process's.
#[cfg(unix)]
#[test]
fn parity_lifecycle_child() {
    if std::env::var_os("FE_PARITY_CHILD").is_none() {
        return;
    }
    let real = tempfile::TempDir::new().unwrap();
    let link_root = tempfile::TempDir::new().unwrap();
    let link = link_root.path().join("via");
    std::os::unix::fs::symlink(real.path(), &link).unwrap();
    let arms: &[(BranchDurability, BranchCheckpoint)] = &[
        (BranchDurability::Catalog { sync: SyncClass::Fsync }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::FullFsync }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::Off }, BranchCheckpoint::Fuzzy),
        (BranchDurability::Catalog { sync: SyncClass::Fsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Durable { sync: SyncClass::Fsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Durable { sync: SyncClass::FullFsync }, BranchCheckpoint::Sharp),
        (BranchDurability::Volatile, BranchCheckpoint::Sharp),
    ];
    let all = |s: SyncCounts| (s.fsync, s.full_fsync, s.barrier, s.barrier_fallback);
    let mut bad = 0;
    for (i, &(durability, ckpt)) in arms.iter().enumerate() {
        let path = link.join(format!("p{i}.db"));
        let (own0, proc0) = (all(super::store::own_sync_counts()), all(super::sync_counts()));
        for round in 0..2 {
            let db = open_with(&path, durability, ckpt);
            db.branch_wait_name_filter();
            let trunk = db.connect().unwrap();
            if round == 0 {
                trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
            }
            for j in 0..12 {
                trunk.execute(format!("INSERT INTO t VALUES ({}, 'x')", round * 100 + j)).unwrap();
                trunk.create_branch(&format!("r{round}b{j}")).unwrap();
            }
            db.drop_branch(&format!("r{round}b5")).unwrap();
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            let _ = trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            drop(trunk);
            drop(db);
        }
        let (own1, proc1) = (all(super::store::own_sync_counts()), all(super::sync_counts()));
        let d = |a: (u64, u64, u64, u64), b: (u64, u64, u64, u64)| (b.0 - a.0, b.1 - a.1, b.2 - a.2, b.3 - a.3);
        let (own, process) = (d(own0, own1), d(proc0, proc1));
        if own != process {
            bad += 1;
        }
        println!("PARITY arm={i} {durability:?}/{ckpt:?}: own {own:?} process {process:?}");
    }
    println!("PARITY-BAD {bad}");
}

/// Counters review HIGH-2: in a run where one store at a time is the only thing syncing, the syncs
/// counted as the stores' equal the process's, arm by arm. A sync attributed to no store (a path the
/// registry does not know, HIGH-1) breaks the equality where `UNSCOPED` cannot see it.
#[cfg(unix)]
#[test]
fn a_single_store_runs_own_counts_equal_the_process_counts() {
    if std::env::var_os("FE_PARITY_CHILD").is_some() {
        return;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::counter_scope_tests::parity_lifecycle_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_PARITY_CHILD", "1")
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "the parity child failed:\n{text}");
    let bad: u64 = text
        .lines()
        .find_map(|l| l.find("PARITY-BAD ").map(|i| l[i + 11..].trim().parse().unwrap()))
        .unwrap_or_else(|| panic!("premise: the parity child ran:\n{text}"));
    assert_eq!(bad, 0, "arms whose own counts differ from the process's:\n{text}");
}
