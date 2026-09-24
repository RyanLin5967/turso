//! Durable branches: what a reopen and a crash must leave behind. Written failing-first.
//!
//! ⚠ UNBUILT when written (no-local-compute rule). Against the durability SKELETON commit every
//! test here must fail on its first durability assertion; against the mechanism, all must pass.
//!
//! A "reopen" here is a real one: every handle, connection and statement holding the `Database` is
//! dropped, so the in-process registry cannot hand back the old instance, and the test asserts the
//! new instance's incarnation differs. A "crash" is a failpoint that stops an operation at a named
//! point, followed by a reopen that runs no shutdown code of the failed operation.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

fn durable() -> DatabaseOpts {
    DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: true })
}

fn open_at(path: &Path, opts: DatabaseOpts) -> Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
}

/// Reopen and prove it is a new instance, not the registry's cached one.
fn reopen(path: &Path, previous_incarnation: u64) -> Arc<Database> {
    let db = open_at(path, durable()).expect("reopen");
    assert_ne!(
        db.incarnation, previous_incarnation,
        "the registry returned the old Database: this is not a reopen"
    );
    db
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

fn value(conn: &Arc<Connection>, id: i64) -> Option<String> {
    let r = rows(conn, &format!("SELECT v FROM t WHERE id = {id}"));
    assert!(r.len() <= 1);
    r.first().map(|row| match &row[0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?}"),
    })
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

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
}

fn in_use(db: &Database) -> BTreeSet<u32> {
    db.branch_slots_in_use().into_iter().collect()
}

fn integrity_ok(conn: &Arc<Connection>) {
    assert_eq!(rows(conn, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
}

#[test]
fn reopen_sees_every_branchs_own_pages_and_its_ancestors_as_of_each_fork() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let c_id;
    let live;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);

        let b = trunk.fork_branch().unwrap();
        {
            let bc = b.connect().unwrap();
            set(&bc, 7, "b-before-fork");
            bc.execute("CREATE TABLE bt(x)").unwrap();
            bc.execute("INSERT INTO bt VALUES (1)").unwrap();
        }
        let c = b.fork().unwrap();
        {
            let bc = b.connect().unwrap();
            set(&bc, 7, "b-after-fork");
            set(&bc, 8, "b-after-fork");
        }
        set(&trunk, 7, "trunk-after-fork");
        set(&trunk, 150, "trunk-after-fork");
        // Move the trunk's post-fork pages into the database file: the branches must be
        // protected by what the branch store persisted, not by an old WAL snapshot.
        trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        {
            let cc = c.connect().unwrap();
            set(&cc, 10, "c");
        }
        live = in_use(&db);
        assert!(!live.is_empty());
        b_id = b.into_id();
        c_id = c.into_id();
    }

    let db = reopen(&path, incarnation);
    let ids: BTreeSet<BranchId> = db.branch_ids().into_iter().collect();
    assert_eq!(ids, BTreeSet::from([b_id, c_id]), "branches lost or invented by the reopen");
    // Replay re-executes the retain/free decisions; it must land on exactly the live set.
    assert_eq!(in_use(&db), live, "replay recovered a different set of live arena slots");

    let b = db.branch(b_id).unwrap();
    let c = db.branch(c_id).unwrap();
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 7), Some("b-after-fork".to_string()));
    assert_eq!(value(&bc, 8), Some("b-after-fork".to_string()));
    assert_eq!(value(&bc, 150), Some(original(150)), "b sees a trunk write made after its fork");
    assert_eq!(value(&bc, 10), Some(original(10)), "b sees its child's write");
    assert_eq!(rows(&bc, "SELECT count(*) FROM bt")[0][0].as_int(), Some(1));
    integrity_ok(&bc);

    let cc = c.connect().unwrap();
    assert_eq!(value(&cc, 7), Some("b-before-fork".to_string()), "c must see b as of c's fork");
    assert_eq!(value(&cc, 8), Some(original(8)));
    assert_eq!(value(&cc, 150), Some(original(150)));
    assert_eq!(value(&cc, 10), Some("c".to_string()));
    // c inherited b's DDL; after a reopen its schema is reparsed from its own pages.
    assert_eq!(rows(&cc, "SELECT count(*) FROM bt")[0][0].as_int(), Some(1));
    integrity_ok(&cc);

    let trunk = db.connect().unwrap();
    assert_eq!(value(&trunk, 7), Some("trunk-after-fork".to_string()));
    assert_eq!(value(&trunk, 150), Some("trunk-after-fork".to_string()));
    assert_eq!(value(&trunk, 8), Some(original(8)));
    assert_eq!(value(&trunk, 10), Some(original(10)));
    assert!(trunk.prepare("SELECT x FROM bt").is_err(), "the trunk sees a branch's table");
    integrity_ok(&trunk);
}

#[test]
fn a_crash_between_allocation_and_publication_leaks_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let published;
    let orphans;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        set(&bc, 7, "published");
        published = in_use(&db);

        db.branch_failpoint(Some(BranchFailpoint::CommitAfterSlotsBeforeRecord));
        let crashed = bc.execute("UPDATE t SET v = 'never-published' WHERE id = 7");
        assert!(crashed.is_err(), "the failpoint did not stop the commit");
        orphans = db.branch_failpoint_orphans();
        assert!(!orphans.is_empty(), "the failed commit allocated no slots: nothing was tested");
        for slot in &orphans {
            assert!(!published.contains(slot), "an orphan slot was already published");
        }
        // In this process too: an unpublished commit's slots are free, not held.
        assert_eq!(in_use(&db), published, "the failed commit's slots are still held");
        assert_eq!(value(&bc, 7), Some("published".to_string()));
        drop(bc);
        b_id = b.into_id();
    }

    let db = reopen(&path, incarnation);
    assert_eq!(in_use(&db), published, "the unpublished commit's slots survived the crash");
    for slot in &orphans {
        assert!(db.branch_slot_is_free(*slot), "orphan slot {slot} leaked");
    }
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    assert_eq!(value(&bc, 7), Some("published".to_string()));
    integrity_ok(&bc);
}

#[test]
fn a_trunk_commit_that_dies_at_its_barrier_changes_nothing_and_leaks_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let orphans;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();

        // The trunk overwrites a page b can see: its pre-image is written to the arena, and the
        // barrier that would publish it before the trunk's WAL commit is made to fail.
        db.branch_failpoint(Some(BranchFailpoint::BarrierBeforeRecords));
        let crashed = trunk.execute("UPDATE t SET v = 'lost' WHERE id = 7");
        assert!(crashed.is_err(), "the trunk committed past a failed barrier");
        orphans = db.branch_failpoint_orphans();
        assert!(!orphans.is_empty(), "no pre-image was written: nothing was tested");
        assert_eq!(value(&trunk, 7), Some(original(7)), "the trunk commit happened anyway");
        b_id = b.into_id();
    }

    let db = reopen(&path, incarnation);
    assert!(in_use(&db).is_empty(), "the unpublished pre-image survived the crash");
    for slot in &orphans {
        assert!(db.branch_slot_is_free(*slot), "orphan slot {slot} leaked");
    }
    let trunk = db.connect().unwrap();
    assert_eq!(value(&trunk, 7), Some(original(7)));
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    assert_eq!(value(&bc, 7), Some(original(7)));
}

#[test]
fn reap_after_reopen_frees_retained_versions() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let a_id;
    let b_id;
    let v0;
    let v1;
    let b_own;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let base = in_use(&db);
        let a = trunk.fork_branch().unwrap(); // sees v0 of row 7
        set(&trunk, 7, "v1");
        let after_v0 = in_use(&db);
        let b = trunk.fork_branch().unwrap(); // sees v1
        set(&trunk, 7, "v2");
        let after_v1 = in_use(&db);
        {
            let bc = b.connect().unwrap();
            set(&bc, 150, "b-own");
        }
        let all = in_use(&db);
        v0 = after_v0.difference(&base).copied().collect::<BTreeSet<u32>>();
        v1 = after_v1.difference(&after_v0).copied().collect::<BTreeSet<u32>>();
        b_own = all.difference(&after_v1).copied().collect::<BTreeSet<u32>>();
        assert!(!v0.is_empty() && !v1.is_empty() && !b_own.is_empty());
        a_id = a.into_id();
        b_id = b.into_id();
    }

    let db = reopen(&path, incarnation);
    let all: BTreeSet<u32> = v0.union(&v1).chain(b_own.iter()).copied().collect();
    assert_eq!(in_use(&db), all);

    let a = db.branch(a_id).unwrap();
    assert_eq!(value(&a.connect().unwrap(), 7), Some(original(7)));
    let reaped = a.reap().unwrap();
    assert!(!reaped.deferred);
    for slot in &v0 {
        assert!(db.branch_slot_is_free(*slot), "v0 outlived its only reader across a reopen");
    }
    for slot in v1.iter().chain(b_own.iter()) {
        assert!(!db.branch_slot_is_free(*slot), "slot {slot} freed while b can still read it");
    }

    let b = db.branch(b_id).unwrap();
    {
        let bc = b.connect().unwrap();
        assert_eq!(value(&bc, 7), Some("v1".to_string()));
        assert_eq!(value(&bc, 150), Some("b-own".to_string()));
    }
    drop(b);
    assert!(in_use(&db).is_empty(), "reaping every branch left slots in use");
    let incarnation = db.incarnation;
    drop(db);

    // The releases were durable: a second reopen finds nothing.
    let db = reopen(&path, incarnation);
    assert!(db.branch_ids().is_empty(), "a reaped branch came back");
    assert!(in_use(&db).is_empty());
    assert_eq!(value(&db.connect().unwrap(), 7), Some("v2".to_string()));
}

#[test]
fn a_torn_log_tail_is_discarded_and_the_branch_is_at_its_last_whole_commit() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let after_first;
    let log;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        set(&bc, 7, "first");
        after_first = in_use(&db);
        set(&bc, 150, "second");
        drop(bc);
        log = db.branch_log_path().expect("a durable store has a log file");
        b_id = b.into_id();
    }
    // Tear the last record (the second commit) mid-frame.
    let len = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&log)
        .unwrap()
        .set_len(len - 3)
        .unwrap();

    let db = reopen(&path, incarnation);
    assert_eq!(in_use(&db), after_first, "the torn commit's slots were recovered as live");
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    assert_eq!(value(&bc, 7), Some("first".to_string()));
    assert_eq!(value(&bc, 150), Some(original(150)), "a torn commit was applied");
    // And the store keeps working past the truncated tail.
    set(&bc, 150, "after-recovery");
    assert_eq!(value(&bc, 150), Some("after-recovery".to_string()));
}

#[test]
fn a_database_with_durable_branches_refuses_to_open_without_them() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        b_id = trunk.fork_branch().unwrap().into_id();
    }
    // Opened volatile, the trunk's writes would skip the pre-image barrier and silently change
    // what the persisted branch reads.
    let err = match open_at(&path, DatabaseOpts::new()) {
        Ok(_) => panic!("a database with durable branches opened without branch durability"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("branch durability"), "{err}");
    let db = reopen(&path, incarnation);
    assert_eq!(db.branch_ids(), vec![b_id]);
}

#[test]
fn compaction_preserves_state_and_a_crash_inside_it_recovers() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let ids;
    let live;
    let views;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let a = trunk.fork_branch().unwrap();
        set(&a.connect().unwrap(), 7, "a");
        let b = a.fork().unwrap();
        set(&trunk, 7, "trunk");
        db.branch_compact_now().unwrap();
        // Written after the compaction: lives only in the new log.
        set(&b.connect().unwrap(), 8, "b");
        set(&a.connect().unwrap(), 9, "a");
        db.branch_failpoint(Some(BranchFailpoint::CompactAfterRenameBeforeLogReset));
        assert!(db.branch_compact_now().is_err(), "the failpoint did not fire");
        live = in_use(&db);
        views = [
            table(&a.connect().unwrap()),
            table(&b.connect().unwrap()),
            table(&trunk),
        ];
        ids = [a.into_id(), b.into_id()];
    }

    let db = reopen(&path, incarnation);
    assert_eq!(in_use(&db), live);
    let a = db.branch(ids[0]).unwrap();
    let b = db.branch(ids[1]).unwrap();
    assert_eq!(table(&a.connect().unwrap()), views[0]);
    assert_eq!(table(&b.connect().unwrap()), views[1]);
    assert_eq!(table(&db.connect().unwrap()), views[2]);
    assert_eq!(value(&b.connect().unwrap(), 7), Some("a".to_string()));
}

/// A small deterministic PRNG, so a failing seed replays exactly.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Random forks, writes, deletes, growth, checkpoints, compactions and reaps, with every live node
/// compared against a model of independent copies after every step — and a full reopen every 25
/// steps, after which the replayed slot set must equal the one before it.
#[test]
fn a_random_workload_survives_repeated_reopens() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let mut db = open_at(&path, durable()).unwrap();
    let mut trunk = db.connect().unwrap();
    seed(&trunk, 300);
    let mut rng = Rng(0xA076_1D64_78BD_642F);
    let mut handles: Vec<Option<Branch>> = vec![None];
    let mut models: Vec<Option<BTreeMap<i64, String>>> = vec![Some(table(&trunk))];
    let mut next_row = 10_000i64;

    for step in 0..200 {
        if step % 25 == 24 {
            let live = in_use(&db);
            let detached: Vec<Option<BranchId>> = handles
                .iter_mut()
                .map(|h| h.take().map(Branch::into_id))
                .collect();
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            db = reopen(&path, incarnation);
            trunk = db.connect().unwrap();
            assert_eq!(in_use(&db), live, "step {step}: replay changed the live slot set");
            handles = detached
                .into_iter()
                .map(|id| id.map(|id| db.branch(id).unwrap()))
                .collect();
        }
        let live: Vec<usize> = (0..models.len()).filter(|&i| models[i].is_some()).collect();
        let who = live[rng.below(live.len() as u64) as usize];
        let op = rng.below(100);
        let conn = if who == 0 {
            trunk.clone()
        } else {
            handles[who].as_ref().unwrap().connect().unwrap()
        };
        if op < 12 && live.len() < 10 {
            let child = conn.fork_branch().unwrap();
            models.push(models[who].clone());
            handles.push(Some(child));
        } else if op < 20 && who != 0 {
            drop(conn);
            handles[who] = None;
            models[who] = None;
        } else if op < 23 && who == 0 {
            trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        } else if op < 25 {
            db.branch_compact_now().unwrap();
        } else {
            let model = models[who].as_mut().unwrap();
            let before = model.clone();
            conn.execute("BEGIN").unwrap();
            for _ in 0..(1 + rng.below(10)) {
                let keys: Vec<i64> = model.keys().copied().collect();
                match rng.below(3) {
                    0 if !keys.is_empty() => {
                        let id = keys[rng.below(keys.len() as u64) as usize];
                        let v = format!("{who}-{step}-{}", "u".repeat(rng.below(200) as usize));
                        set(&conn, id, &v);
                        model.insert(id, v);
                    }
                    1 if !keys.is_empty() => {
                        let id = keys[rng.below(keys.len() as u64) as usize];
                        conn.execute(format!("DELETE FROM t WHERE id = {id}")).unwrap();
                        model.remove(&id);
                    }
                    _ => {
                        next_row += 1;
                        let v = format!("{who}-{step}-{}", "n".repeat(rng.below(300) as usize));
                        conn.execute(format!("INSERT INTO t VALUES ({next_row}, '{v}')"))
                            .unwrap();
                        model.insert(next_row, v);
                    }
                }
            }
            if rng.below(8) == 0 {
                conn.execute("ROLLBACK").unwrap();
                *model = before;
            } else {
                conn.execute("COMMIT").unwrap();
            }
        }
        for i in 0..models.len() {
            let Some(model) = &models[i] else { continue };
            let reader = if i == 0 {
                db.connect().unwrap()
            } else if i == who {
                continue;
            } else {
                handles[i].as_ref().unwrap().connect().unwrap()
            };
            assert_eq!(&table(&reader), model, "step {step}: node {i} diverged from its model");
        }
        if who != 0 && models[who].is_some() {
            assert_eq!(
                &table(&conn),
                models[who].as_ref().unwrap(),
                "step {step}: node {who} diverged"
            );
        }
    }
    drop(handles);
    assert!(db.branch_ids().is_empty());
    assert!(in_use(&db).is_empty());
    integrity_ok(&trunk);
}

#[test]
fn a_corrupted_arena_slot_is_an_error_not_a_wrong_page() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let slots;
    let page_size;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        page_size = rows(&trunk, "PRAGMA page_size")[0][0].as_int().unwrap() as u64;
        let b = trunk.fork_branch().unwrap();
        set(&b.connect().unwrap(), 150, "branch");
        slots = b.owned_slots();
        assert_eq!(slots.len(), 1, "one in-place UPDATE should own exactly one page");
        b_id = b.into_id();
    }
    // Flip one byte in the middle of the branch's only page.
    let arena = format!("{}-branch-arena", path.to_str().unwrap());
    let offset = slots[0] as u64 * page_size + page_size / 2;
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&arena)
            .unwrap();
        let mut byte = [0u8; 1];
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&byte).unwrap();
    }
    let db = reopen(&path, incarnation);
    let b = db.branch(b_id).unwrap();
    // The branch's schema is reparsed on connect (page 1 is the trunk's, intact); the damaged page
    // is the leaf holding row 150, so the read of that row must fail loudly.
    let bc = b.connect().unwrap();
    let read = bc
        .prepare("SELECT v FROM t WHERE id = 150")
        .and_then(|mut s| s.run_collect_rows());
    let err = match read {
        Ok(rows) => panic!("a corrupted branch page was served: {rows:?}"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("checksum"), "{err}");
    // A page the branch does not own still reads from the trunk.
    assert_eq!(value(&bc, 7), Some(original(7)));
}

#[test]
fn a_garbled_last_record_is_discarded_like_a_short_one() {
    // A torn sector can leave a frame of the right LENGTH with the wrong bytes; only the CRC can
    // tell it from a whole record. (The truncation test above is caught by the length alone.)
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let after_first;
    let log;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        set(&bc, 7, "first");
        after_first = in_use(&db);
        set(&bc, 150, "second");
        drop(bc);
        log = db.branch_log_path().expect("a durable store has a log file");
        b_id = b.into_id();
    }
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log)
            .unwrap();
        let len = f.metadata().unwrap().len();
        let mut byte = [0u8; 1];
        f.seek(SeekFrom::Start(len - 1)).unwrap();
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x5A;
        f.seek(SeekFrom::Start(len - 1)).unwrap();
        f.write_all(&byte).unwrap();
    }
    let db = reopen(&path, incarnation);
    assert_eq!(in_use(&db), after_first, "a garbled commit was replayed");
    let bc = db.branch(b_id).unwrap().connect().unwrap();
    assert_eq!(value(&bc, 7), Some("first".to_string()));
    assert_eq!(value(&bc, 150), Some(original(150)));
}

#[test]
fn a_release_deferred_by_an_open_connection_is_still_a_release_after_compaction() {
    // Dropping a handle while its connection is open logs the release but defers the free. A
    // compaction in that window snapshots a RELEASED branch; after a restart nothing is open, so
    // recovery must free it rather than keep it forever.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 50);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        set(&bc, 7, "b");
        drop(b);
        assert_eq!(db.branch_stats().live_branches, 1, "the open connection did not defer it");
        db.branch_compact_now().unwrap();
        drop(bc);
        assert_eq!(db.branch_stats().live_branches, 0);
        assert!(in_use(&db).is_empty());
    }
    let db = reopen(&path, incarnation);
    assert_eq!(db.branch_stats().live_branches, 0, "a released branch came back from a snapshot");
    assert!(in_use(&db).is_empty(), "its pages came back with it");
    assert!(db.branch_ids().is_empty());
}
