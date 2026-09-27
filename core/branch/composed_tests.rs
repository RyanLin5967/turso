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
const CATALOG: BranchDurability = BranchDurability::Catalog { sync: true };
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
            (w1.compactions - w0.compactions, w1.compact_locked_bytes - w0.compact_locked_bytes),
            (0, 0),
            "premise: no compaction ran inside the commit (its syncs are counted apart)"
        );
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
    // Read before any observation call that returns matured frees itself (`branch_stats`).
    assert_eq!(
        db.branch_pending_frees(),
        0,
        "the rewriting commit left its frees for a later hold to return all at once"
    );
    let rewrite = db.branch_take_hold_max();
    assert!(
        rewrite.pages <= 64,
        "one hold of the rewriting commit (map, or the drain of its frees) touched {} pages",
        rewrite.pages
    );
    assert_eq!(rewrite.copy_bytes, 0, "the rewriting commit copied pages under the store mutex");
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
fn a_large_catalog_branch_commit_is_bounded_and_syncs_nothing_under_the_mutex() {
    commit_is_bounded(CATALOG);
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
fn a_spilling_catalog_branch_transaction_commits_rolls_back_forks_and_reopens_intact() {
    spilling_transaction(CATALOG);
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

#[test]
fn a_spilled_catalog_branch_transaction_rolls_back_to_every_pre_transaction_row() {
    spilled_rollback_restores(CATALOG, false);
}

/// A crash between two holds of a commit's map (amendment 8b): the record is either durable, and
/// recovery replays the whole commit, or it is not, and recovery shows none of it. Never half.
fn crash_between_map_holds(mode: BranchDurability, durable_record: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, mode);
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
    let db = open(&image, mode);
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
    crash_between_map_holds(DURABLE, true);
}

#[test]
fn a_crash_between_map_holds_before_the_record_is_durable_recovers_none_of_it() {
    crash_between_map_holds(DURABLE, false);
}

#[test]
fn a_catalog_crash_between_map_holds_after_the_record_is_durable_recovers_the_whole_commit() {
    crash_between_map_holds(CATALOG, true);
}

#[test]
fn a_catalog_crash_between_map_holds_before_the_record_is_durable_recovers_none_of_it() {
    crash_between_map_holds(CATALOG, false);
}

/// The compaction guards (amendment 8a, hazard 4): a compaction asked for between two holds of a
/// commit's map must not run there - neither an explicit one (`compact_now`) nor the automatic
/// check (`maybe_compact`, which another thread's operation runs). A snapshot then would carry the
/// commit's first batch and drop its buffered record, and a crash after the commit would recover
/// half of it. Premises asserted: the failpoint fired (the commit took more than one map hold)
/// and the guard refused.
fn compaction_between_map_holds_waits(mode: BranchDurability, failpoint: BranchFailpoint) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, mode);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let b_id = b.id();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'before-' || id").unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'after-' || id").unwrap();
    let refused = db.branch_stats().unwrap().work.compactions_refused;
    db.branch_failpoint(Some(failpoint));
    bc.execute("COMMIT").unwrap();
    assert_eq!(
        db.branch_failpoint_pending(),
        None,
        "the failpoint never fired: the commit did not take a second map hold"
    );
    assert!(
        db.branch_stats().unwrap().work.compactions_refused > refused,
        "the compaction asked for between map holds was not refused"
    );
    let image = crash_image(&path, dir.path());
    let db = open(&image, mode);
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

#[test]
fn an_explicit_compaction_between_map_holds_is_refused_and_the_commit_recovers_whole() {
    compaction_between_map_holds_waits(DURABLE, BranchFailpoint::CompactBetweenMapHolds);
}

#[test]
fn an_automatic_compaction_between_map_holds_is_refused_and_the_commit_recovers_whole() {
    compaction_between_map_holds_waits(DURABLE, BranchFailpoint::MaybeCompactBetweenMapHolds);
}

#[test]
fn a_catalog_checkpoint_between_map_holds_is_refused_and_the_commit_recovers_whole() {
    compaction_between_map_holds_waits(CATALOG, BranchFailpoint::CompactBetweenMapHolds);
}

#[test]
fn an_automatic_catalog_checkpoint_between_map_holds_is_refused_and_the_commit_recovers_whole() {
    compaction_between_map_holds_waits(CATALOG, BranchFailpoint::MaybeCompactBetweenMapHolds);
}

/// Review H3: a branch released while its connection is open, whose Release record never became
/// durable (its flight failed), must keep every slot when that connection closes: a slot freed
/// then could be reused while recovery still names it.
#[test]
fn a_close_after_a_release_whose_flight_failed_frees_none_of_the_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("t.db"), DURABLE);
    let trunk = db.connect().unwrap();
    seed(&trunk, 2_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'mine-' || id").unwrap();
    let owned: Vec<u32> = db.branch_slots_in_use();
    assert!(!owned.is_empty(), "premise: the branch owns slots");
    db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
    assert!(
        db.reap_branches(vec![b]).is_err(),
        "premise: the release's flight failed"
    );
    drop(bc);
    let freed: Vec<u32> = owned.iter().copied().filter(|&s| db.branch_slot_is_free(s)).collect();
    assert!(
        freed.is_empty(),
        "closing a branch whose Release is not durable freed {} of its slots (first {:?})",
        freed.len(),
        freed.first()
    );
}

/// Review H4: a compaction that fails before its snapshot is the truth (here its temporary file
/// cannot be created) loses nothing and makes nothing durable: later commits must go on working.
#[test]
fn a_compaction_that_fails_before_its_snapshot_leaves_later_commits_working() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let db = open(&path, DURABLE);
    let trunk = db.connect().unwrap();
    seed(&trunk, 2_000);
    let b = trunk.fork_branch().unwrap();
    let b_id = b.id();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'one-' || id").unwrap();
    let tmp = format!("{}-branch-snap.tmp", path.display());
    std::fs::create_dir(&tmp).unwrap();
    assert!(db.branch_compact_now().is_err(), "premise: the compaction failed");
    std::fs::remove_dir(&tmp).unwrap();
    bc.execute("UPDATE t SET v = 'two-' || id")
        .expect("a commit after a compaction that failed before its snapshot");
    let image = crash_image(&path, dir.path());
    let db = open(&image, DURABLE);
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("two-{id}"), "row {id} after the reopen");
    }
}

// ---- Merge 1b(ii): the fresh-context review of c38fe12b7 (PREREG amendment 8f). Red first. ----

fn in_use(db: &Database) -> std::collections::BTreeSet<u32> {
    db.branch_slots_in_use().into_iter().collect()
}

/// Review F1 (catalog recovery; present at d7a2b8f6e): a branch released while its connection is
/// open is checkpointed as released, then collected at its close with no record, so its slots go
/// back to the free list and a later commit takes them. The catalog names them as the released
/// branch's until the next checkpoint. Recovery collected the catalog's released branches AFTER the
/// log's replay, and those frees overrode the replay's "in use": after a reopen the later commit's
/// slots were free, and the next writes overwrote its pages. With `retire`, the released branch has
/// a live child, so the close retires it (frees only what the child cannot read) instead.
fn a_slot_a_close_freed_and_a_commit_took_survives_a_reopen(retire: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let (b2_id, child_id, reused);
    {
        let db = open(&path, CATALOG);
        let trunk = db.connect().unwrap();
        seed(&trunk, 2_000);
        let b1 = trunk.fork_branch().unwrap();
        let b1c = b1.connect().unwrap();
        b1c.execute("UPDATE t SET v = 'b1-' || id WHERE id <= 1000").unwrap();
        let child = retire.then(|| b1.fork().unwrap());
        let before = in_use(&db);
        b1c.execute("UPDATE t SET v = 'b1late-' || id WHERE id > 1000").unwrap();
        // What the close frees: everything b1 owns, or (retired) what its child cannot read.
        let freed: std::collections::BTreeSet<u32> = if retire {
            in_use(&db).difference(&before).copied().collect()
        } else {
            in_use(&db)
        };
        assert!(!freed.is_empty(), "premise: b1 owns slots its close frees");
        drop(b1);
        db.branch_compact_now().unwrap();
        drop(b1c);
        assert!(
            freed.iter().all(|&s| db.branch_slot_is_free(s)),
            "premise: the close freed b1's slots"
        );
        let b2 = trunk.fork_branch().unwrap();
        let b2c = b2.connect().unwrap();
        b2c.execute("UPDATE t SET v = 'b2-' || id").unwrap();
        reused = in_use(&db)
            .intersection(&freed)
            .copied()
            .collect::<Vec<u32>>();
        assert!(!reused.is_empty(), "premise: b2's commit took none of the slots b1's close freed");
        drop(b2c);
        b2_id = b2.into_id();
        child_id = child.map(|c| c.into_id());
    }
    // A clean close with no checkpoint after b2's commit: b2 lives in the log's suffix only.
    let db = open(&path, CATALOG);
    let taken: Vec<u32> = reused.iter().copied().filter(|&s| db.branch_slot_is_free(s)).collect();
    assert!(
        taken.is_empty(),
        "after a reopen, {} slots b2 committed into are free (first {:?})",
        taken.len(),
        taken.first()
    );
    let b2c = db.branch(b2_id).unwrap().connect().unwrap();
    let check = |when: &str| {
        for (id, v) in table(&b2c) {
            assert_eq!(v, format!("b2-{id}"), "b2's row {id} {when}");
        }
        integrity_ok(&b2c);
    };
    check("after the reopen");
    let b3 = db.connect().unwrap().fork_branch().unwrap();
    b3.connect().unwrap().execute("UPDATE t SET v = 'b3-' || id").unwrap();
    check("after another branch's writes");
    if let Some(child_id) = child_id {
        // The child's release collects b1 now: none of what it frees may be b2's.
        db.branch(child_id).unwrap().reap().unwrap();
        let b4 = db.connect().unwrap().fork_branch().unwrap();
        b4.connect().unwrap().execute("UPDATE t SET v = 'b4-' || id").unwrap();
        check("after the child's release and more writes");
    }
}

#[test]
fn a_slot_a_close_freed_and_a_later_commit_took_is_that_commits_after_a_reopen() {
    a_slot_a_close_freed_and_a_commit_took_survives_a_reopen(false);
}

#[test]
fn a_slot_a_retiring_close_freed_and_a_later_commit_took_is_that_commits_after_a_reopen() {
    a_slot_a_close_freed_and_a_commit_took_survives_a_reopen(true);
}

/// Review F4: a catalog checkpoint's arena fsync, and its COMMIT, can fail with an outcome that
/// cannot be known (a later fsync can report success over pages this one dropped; a failed COMMIT
/// may be on disk): the store must fail-stop. `CompactArenaSyncFails` reached only a snapshot
/// compaction, and nothing reached the COMMIT, so no test made either fail-stop fire. Sharp
/// (`compact_now`) and fuzzy (`checkpoint_fuzzy_now`, its fsync on the writer's thread).
fn a_catalog_checkpoint_in_doubt_fail_stops(fuzzy: bool, failpoint: BranchFailpoint) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let (b_id, image);
    {
        let db = open(&path, CATALOG);
        let trunk = db.connect().unwrap();
        seed(&trunk, 2_000);
        let b = trunk.fork_branch().unwrap();
        b_id = b.id();
        let bc = b.connect().unwrap();
        bc.execute("UPDATE t SET v = 'before-' || id").unwrap();
        db.branch_failpoint(Some(failpoint));
        if fuzzy {
            assert!(
                db.branch_checkpoint_fuzzy_now().unwrap(),
                "premise: no fuzzy checkpoint started"
            );
            db.branch_checkpoint_wait();
        } else {
            assert!(
                db.branch_compact_now().is_err(),
                "a checkpoint whose outcome is unknown reported success"
            );
        }
        assert_eq!(db.branch_failpoint_pending(), None, "the failpoint never fired");
        assert!(
            bc.execute("UPDATE t SET v = 'after-' || id").is_err(),
            "a commit was acknowledged after a checkpoint whose outcome is unknown"
        );
        drop(bc);
        let _ = b.into_id();
        image = crash_image(&path, dir.path());
    }
    let db = open(&image, CATALOG);
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    for (id, v) in table(&bc) {
        assert_eq!(v, format!("before-{id}"), "row {id} after the reopen");
    }
    integrity_ok(&bc);
}

#[test]
fn a_sharp_catalog_checkpoint_whose_arena_sync_fails_fail_stops_the_store() {
    a_catalog_checkpoint_in_doubt_fail_stops(false, BranchFailpoint::CompactArenaSyncFails);
}

#[test]
fn a_fuzzy_catalog_checkpoint_whose_arena_sync_fails_fail_stops_the_store() {
    a_catalog_checkpoint_in_doubt_fail_stops(true, BranchFailpoint::CompactArenaSyncFails);
}

#[test]
fn a_sharp_catalog_checkpoint_whose_commit_fails_fail_stops_the_store() {
    a_catalog_checkpoint_in_doubt_fail_stops(false, BranchFailpoint::CheckpointCommitFails);
}

#[test]
fn a_fuzzy_catalog_checkpoint_whose_commit_fails_fail_stops_the_store() {
    a_catalog_checkpoint_in_doubt_fail_stops(true, BranchFailpoint::CheckpointCommitFails);
}

/// Review F6 (the port's own guard): a compaction the automatic check asks for while a commit is
/// between its holds is refused, and must run once no commit is: here when this commit drains.
/// Before, the refusal was forgotten, and overlapping commits could starve compaction for good.
fn a_refused_compaction_runs_when_the_commits_drain(mode: BranchDurability) {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("t.db"), mode);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET v = 'before-' || id").unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'after-' || id").unwrap();
    let w0 = db.branch_stats().unwrap().work;
    db.branch_failpoint(Some(BranchFailpoint::MaybeCompactBetweenMapHolds));
    bc.execute("COMMIT").unwrap();
    assert_eq!(db.branch_failpoint_pending(), None, "premise: the failpoint fired");
    let w1 = db.branch_stats().unwrap().work;
    assert!(
        w1.compactions_refused > w0.compactions_refused,
        "premise: the compaction between the holds was refused"
    );
    assert!(
        w1.compactions > w0.compactions,
        "a compaction refused while a commit was between its holds never ran once it drained"
    );
}

#[test]
fn a_refused_compaction_runs_when_the_commits_drain_durable() {
    a_refused_compaction_runs_when_the_commits_drain(DURABLE);
}

#[test]
fn a_refused_catalog_checkpoint_runs_when_the_commits_drain() {
    a_refused_compaction_runs_when_the_commits_drain(CATALOG);
}

/// Review F6, the overlap: while a refused compaction waits for the commits between their holds, a
/// new commit waits before its first hold, so the commits in flight drain and the compaction runs
/// (otherwise commits that keep overlapping starve it). Commit A pauses before its third map hold
/// with a compaction refused; commit B must wait at its first hold until A is released.
#[test]
fn a_commit_waits_while_a_refused_compaction_waits_for_the_commits_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("t.db"), DURABLE);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let a = trunk.fork_branch().unwrap();
    let b = trunk.fork_branch().unwrap();
    let w0 = db.branch_stats().unwrap().work;
    let wait = |what: &str, done: &dyn Fn() -> bool| {
        let t = std::time::Instant::now();
        while !done() {
            assert!(t.elapsed() < std::time::Duration::from_secs(20), "{what}");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    };
    /// Releases the commit hook however the scope ends, so a failed assertion never leaves commit
    /// A paused while the scope waits to join it.
    struct Release<'a>(&'a Database);
    impl Drop for Release<'_> {
        fn drop(&mut self) {
            self.0.branch_publish_hold(0);
        }
    }
    std::thread::scope(|s| {
        let _release = Release(&db);
        db.branch_publish_hold(store::HOLD_BETWEEN_MAP_HOLDS);
        db.branch_failpoint(Some(BranchFailpoint::MaybeCompactBetweenMapHolds));
        let ta = s.spawn(|| a.connect().unwrap().execute("UPDATE t SET v = 'a-' || id"));
        wait("premise: commit A never paused between its holds", &|| {
            db.branch_publish_held() == store::HOLD_BETWEEN_MAP_HOLDS | store::HOLD_ARRIVED
        });
        assert!(
            db.branch_stats().unwrap().work.compactions_refused > w0.compactions_refused,
            "premise: the compaction between A's holds was refused"
        );
        let tb = s.spawn(|| b.connect().unwrap().execute("UPDATE t SET v = 'b-' || id WHERE id = 1"));
        wait(
            "commit B started its holds while a refused compaction waited for commit A",
            &|| db.branch_stats().unwrap().work.publish_gate_waits > w0.publish_gate_waits,
        );
        assert!(!tb.is_finished(), "commit B finished while commit A was between its holds");
        db.branch_publish_hold(0);
        ta.join().unwrap().unwrap();
        tb.join().unwrap().unwrap();
    });
    assert!(
        db.branch_stats().unwrap().work.compactions > w0.compactions,
        "the refused compaction never ran"
    );
}
