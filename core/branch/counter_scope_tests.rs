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
