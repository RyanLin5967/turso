//! The r11-bigtxn range ported onto the durable composed store (artie-research r11-bigtxn PREREG
//! amendments 8, 8a, 8b). UNBUILT when written. Each property runs on the durable store and, where
//! the property is not about durability, on the same store's memory arena too (the same commit
//! path). New tests for the port: r11-bigtxn's own tests on its branch are not edited, and their
//! first-fork and reap bounds are NOT asserted here (F-fork1 and F-reclaim are not ported).

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::BTreeMap;
use std::path::Path;

const DURABLE: BranchDurability = BranchDurability::Durable { sync: true };
const VOLATILE: BranchDurability = BranchDurability::Volatile;

fn open(path: &Path, durability: BranchDurability) -> Arc<Database> {
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

fn original(id: i64) -> String {
    format!("trunk-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>, rows: i64) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=rows {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn table(conn: &Arc<Connection>) -> BTreeMap<i64, String> {
    rows(conn, "SELECT id, v FROM t ORDER BY id")
        .into_iter()
        .map(|row| {
            let v = match &row[1] {
                Value::Text(t) => t.as_str().to_string(),
                other => panic!("expected text, got {other:?}"),
            };
            (row[0].as_int().expect("integer id"), v)
        })
        .collect()
}

fn integrity_ok(conn: &Arc<Connection>) {
    assert_eq!(rows(conn, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
}

/// Every file of the database, copied while it is still OPEN (as `durability_tests` does).
fn crash_image(src: &Path, dir: &Path) -> std::path::PathBuf {
    let dst = dir.join("crash-image.db");
    for suffix in [
        "",
        "-wal",
        "-branch-log",
        "-branch-arena",
        "-branch-snap",
        "-branch-cat",
        "-branch-cat-wal",
    ] {
        let from = std::path::PathBuf::from(format!("{}{suffix}", src.display()));
        if from.exists() {
            std::fs::copy(&from, format!("{}{suffix}", dst.display())).unwrap();
        }
    }
    dst
}

/// The commit bound (amendment 8's two counters plus the two the port adds): a branch transaction
/// of many hold batches commits in holds of at most HOLD_BATCH pages, copies no page under the store
/// mutex, and on the durable store syncs no byte under it and copies no record byte into the
/// journal's buffer under it (the D-entry record goes in by move); its flight syncs the pages and
/// the record with no lock held.
fn commit_is_bounded(durability: BranchDurability) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("t.db"), durability);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'big-' || id").unwrap();
    let w0 = db.branch_stats().unwrap().work;
    let _ = db.branch_take_hold_max();
    bc.execute("COMMIT").unwrap();
    let commit = db.branch_take_hold_max();
    let w1 = db.branch_stats().unwrap().work;
    let owned = db.branch_stats().unwrap().arena_slots_in_use as u64;
    assert!(owned > 256, "the transaction owns only {owned} pages: too small to test the bound");
    assert!(commit.pages <= 64, "one commit hold mapped {} pages", commit.pages);
    assert_eq!(commit.copy_bytes, 0, "the commit copied pages under the store mutex");
    assert_eq!(w1.locked_copy_bytes, w0.locked_copy_bytes, "pages copied under the mutex");
    if durability != VOLATILE {
        assert_eq!(
            w1.sync_locked_bytes - w0.sync_locked_bytes,
            0,
            "the commit synced bytes under the store mutex"
        );
        assert_eq!(
            w1.journal_copied_bytes - w0.journal_copied_bytes,
            0,
            "the commit copied record bytes into the journal under the store mutex"
        );
        let handed = w1.journal_handed_bytes - w0.journal_handed_bytes;
        assert!(handed >= 12 * owned, "the Commit record went in by move: {handed} bytes");
        let synced = w1.sync_unlocked_bytes - w0.sync_unlocked_bytes;
        assert!(
            synced >= 4096 * owned + handed,
            "the flight synced {synced} bytes, fewer than the pages and the record"
        );
    }
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("big-{id}"), "row {id} after the bounded commit");
    }
    // A second large commit supersedes every page the first one wrote: with no child, each old
    // version is freed, deferred until the record is durable, and then returned in bounded holds
    // by the committer (not all at once by whichever hold comes next).
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'big2-' || id").unwrap();
    let _ = db.branch_take_hold_max();
    bc.execute("COMMIT").unwrap();
    let rewrite = db.branch_take_hold_max();
    assert!(
        rewrite.pages <= 64,
        "one hold of the rewriting commit (map, or the drain of its frees) touched {} pages",
        rewrite.pages
    );
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as u64,
        owned,
        "the rewrite's frees were not all returned"
    );
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("big2-{id}"), "row {id} after the rewriting commit");
    }
    integrity_ok(&bc);
}

#[test]
fn a_large_durable_branch_commit_is_bounded_and_syncs_nothing_under_the_mutex() {
    commit_is_bounded(DURABLE);
}

#[test]
fn a_large_volatile_branch_commit_on_the_composed_store_is_bounded() {
    commit_is_bounded(VOLATILE);
}

/// STEAL on the composed store: a branch transaction far larger than its page cache spills into its
/// own slots (for a durable store, positional writes into the arena file), commits its last image
/// of every page, a rollback returns exactly the slots it took, a savepoint rollback restores pages
/// spilled after the savepoint, a child forked before a large commit keeps what it saw, and a
/// durable store reads all of it back after a reopen.
fn spilling_transaction(durability: BranchDurability) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, durability);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    let pages = rows(&bc, "PRAGMA page_count")[0][0].as_int().unwrap();
    bc.execute("PRAGMA cache_size = 10").unwrap();
    let expect = |id: i64, every7: &str, rest: &str| {
        if id % 7 == 0 {
            format!("{every7}-{id}")
        } else {
            format!("{rest}-{id}")
        }
    };
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'a-' || id").unwrap();
    let cached = bc.page_cache_len() as i64;
    assert!(
        cached < pages / 2,
        "the cache holds {cached} of {pages} pages after dirtying them all: nothing spilled"
    );
    bc.execute("UPDATE t SET v = 'b-' || id WHERE id % 7 = 0").unwrap();
    bc.execute("COMMIT").unwrap();
    let t = table(&bc);
    assert_eq!(t.len(), 20_000);
    for (id, v) in &t {
        assert_eq!(v, &expect(*id, "b", "a"), "row {id} after the spilling commit");
    }

    let child = b.fork().unwrap();
    let committed = db.branch_stats().unwrap().arena_slots_in_use;
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'c-' || id").unwrap();
    bc.execute("ROLLBACK").unwrap();
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use,
        committed,
        "a rolled-back transaction kept or lost slots"
    );

    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'd-' || id").unwrap();
    bc.execute("SAVEPOINT sp").unwrap();
    bc.execute("UPDATE t SET v = 'e-' || id").unwrap();
    bc.execute("ROLLBACK TO sp").unwrap();
    bc.execute("RELEASE sp").unwrap();
    bc.execute("COMMIT").unwrap();
    drop(bc);
    let bc = b.connect().unwrap();
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("d-{id}"), "row {id} after the savepoint rollback");
    }
    integrity_ok(&bc);
    let cc = child.connect().unwrap();
    for (id, v) in table(&cc) {
        assert_eq!(v, expect(id, "b", "a"), "the child's row {id}");
    }
    integrity_ok(&cc);
    if durability == VOLATILE {
        return;
    }
    let incarnation = db.incarnation;
    drop(cc);
    drop(bc);
    let (b_id, child_id) = (b.into_id(), child.into_id());
    drop(trunk);
    drop(db);
    let db = open(&path, durability);
    assert_ne!(db.incarnation, incarnation, "not a reopen");
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("d-{id}"), "row {id} after the reopen");
    }
    integrity_ok(&bc);
    let cc = db.branch(child_id).unwrap().connect().unwrap();
    for (id, v) in table(&cc) {
        assert_eq!(v, expect(id, "b", "a"), "the child's row {id} after the reopen");
    }
}

#[test]
fn a_spilling_durable_branch_transaction_commits_rolls_back_forks_and_reopens_intact() {
    spilling_transaction(DURABLE);
}

#[test]
fn a_spilling_volatile_branch_transaction_on_the_composed_store_is_intact() {
    spilling_transaction(VOLATILE);
}

/// K13 on the composed store: a transaction larger than its page cache spills, then rolls back, and
/// every row the branch held before it reads back intact, for pages the branch already owned and
/// pages it did not; `same_length` keeps every row's length (leaf pages only, no rebalance).
fn spilled_rollback_restores(durability: BranchDurability, same_length: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("t.db"), durability);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    let (own, rewrite) = if same_length {
        (
            "UPDATE t SET v = 'O' || substr(v, 2) WHERE id <= 10000",
            "UPDATE t SET v = 'R' || substr(v, 2)",
        )
    } else {
        (
            "UPDATE t SET v = 'owned-' || id WHERE id <= 10000",
            "UPDATE t SET v = 'rolled-back-' || id",
        )
    };
    bc.execute(own).unwrap();
    let before = table(&bc);
    assert_eq!(before.len(), 20_000);
    let pages = rows(&bc, "PRAGMA page_count")[0][0].as_int().unwrap();
    bc.execute("PRAGMA cache_size = 10").unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute(rewrite).unwrap();
    let cached = bc.page_cache_len() as i64;
    assert!(
        cached < pages / 2,
        "nothing spilled: the cache holds {cached} of {pages} pages"
    );
    bc.execute("ROLLBACK").unwrap();
    let check = |conn: &Arc<Connection>, when: &str| {
        let after = table(conn);
        assert_eq!(after.len(), before.len(), "{when}: row count");
        let (mut owned_bad, mut not_owned_bad, mut first) = (0, 0, None);
        for (id, v) in &before {
            if after.get(id) != Some(v) {
                if *id <= 10_000 {
                    owned_bad += 1;
                } else {
                    not_owned_bad += 1;
                }
                first.get_or_insert((*id, after.get(id).cloned()));
            }
        }
        assert!(
            owned_bad == 0 && not_owned_bad == 0,
            "{when}: after ROLLBACK, {owned_bad} owned-page rows and {not_owned_bad} not-owned rows \
             read the rolled-back write (first: {first:?})"
        );
    };
    check(&bc, "same connection");
    drop(bc);
    let bc = b.connect().unwrap();
    check(&bc, "fresh connection");
    integrity_ok(&bc);
}

#[test]
fn a_spilled_durable_branch_transaction_rolls_back_to_every_pre_transaction_row() {
    spilled_rollback_restores(DURABLE, false);
}

#[test]
fn a_spilled_durable_same_length_rewrite_rolls_back_every_owned_and_not_owned_row() {
    spilled_rollback_restores(DURABLE, true);
}

/// A crash between two holds of a commit's map (amendment 8b): the record is either durable, and
/// recovery replays the whole commit, or it is not, and recovery shows none of it. Never half.
fn crash_between_map_holds(durable_record: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, DURABLE);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let b_id = b.id();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'before-' || id").unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'after-' || id").unwrap();
    db.branch_failpoint(Some(if durable_record {
        BranchFailpoint::CommitBetweenMapHolds
    } else {
        BranchFailpoint::CommitBetweenMapHoldsUndurable
    }));
    assert!(bc.execute("COMMIT").is_err(), "the failpoint did not stop the commit");
    let image = crash_image(&path, dir.path());
    let db = open(&image, DURABLE);
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    let want = if durable_record { "after" } else { "before" };
    let t = table(&bc);
    assert_eq!(t.len(), 20_000);
    let wrong: Vec<i64> = t
        .iter()
        .filter(|(id, v)| **v != format!("{want}-{id}"))
        .map(|(id, _)| *id)
        .collect();
    assert!(
        wrong.is_empty(),
        "a crash between map holds recovered half a commit: {} rows are not {want}- (first {:?})",
        wrong.len(),
        wrong.first()
    );
    integrity_ok(&bc);
}

#[test]
fn a_crash_between_map_holds_after_the_record_is_durable_recovers_the_whole_commit() {
    crash_between_map_holds(true);
}

#[test]
fn a_crash_between_map_holds_before_the_record_is_durable_recovers_none_of_it() {
    crash_between_map_holds(false);
}

/// The compaction guard (amendment 8a, hazard 4): a compaction asked for between two holds of a
/// commit's map must not run there. A snapshot taken then would carry the commit's first batch,
/// drop its buffered record, and a crash after the commit would recover half of it.
#[test]
fn a_compaction_between_map_holds_waits_so_a_crash_after_the_commit_recovers_all_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, DURABLE);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let b_id = b.id();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'before-' || id").unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'after-' || id").unwrap();
    db.branch_failpoint(Some(BranchFailpoint::CompactBetweenMapHolds));
    bc.execute("COMMIT").unwrap();
    let image = crash_image(&path, dir.path());
    let db = open(&image, DURABLE);
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    let t = table(&bc);
    assert_eq!(t.len(), 20_000);
    let wrong: Vec<i64> = t
        .iter()
        .filter(|(id, v)| **v != format!("after-{id}"))
        .map(|(id, _)| *id)
        .collect();
    assert!(
        wrong.is_empty(),
        "a compaction between map holds lost part of a committed transaction: {} rows (first {:?})",
        wrong.len(),
        wrong.first()
    );
    integrity_ok(&bc);
}
