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
    // r11-restart lane: R11_BRANCH_CATALOG=1 runs this whole file against catalog mode.
    let durability = if std::env::var_os("R11_BRANCH_CATALOG").is_some() {
        BranchDurability::Catalog { sync: crate::branch::SyncClass::Fsync }
    } else {
        BranchDurability::Durable { sync: crate::branch::SyncClass::Fsync }
    };
    DatabaseOpts::new().with_branch_durability(durability)
}

fn open_read_only(path: &Path, opts: DatabaseOpts) -> Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::ReadOnly,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
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
    let ids: BTreeSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
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
    assert!(db.branch_ids().unwrap().is_empty(), "a reaped branch came back");
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
    // fastest-engine (PREREG v1 amendment 39, a base red fixed, not registered): the handle is
    // HELD. The base connected through a temporary `db.branch(b_id).unwrap()`, whose drop released
    // the branch at the end of that statement, so the write below was refused ("branch 1 has been
    // reaped") and the store's life past the truncated tail was never tested in any arm.
    let b = db.branch(b_id).unwrap();
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 7), Some("first".to_string()));
    assert_eq!(value(&bc, 150), Some(original(150)), "a torn commit was applied");
    // And the store keeps working past the truncated tail.
    set(&bc, 150, "after-recovery");
    assert_eq!(value(&bc, 150), Some("after-recovery".to_string()));
    // The law the line above was reaching for: recovery cut the log at its last whole frame, so
    // what was appended after it survives the NEXT recovery (a tail left torn would stop that
    // replay at the torn frame, or refuse the append; mutant `no_torn_tail_cut`).
    drop(bc);
    let b_id = b.into_id();
    let incarnation = db.incarnation;
    drop(db);
    let db = reopen(&path, incarnation);
    let b = db.branch(b_id).unwrap();
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 7), Some("first".to_string()));
    assert_eq!(
        value(&bc, 150),
        Some("after-recovery".to_string()),
        "a commit made after the torn tail was cut did not survive the next recovery"
    );
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
    assert_eq!(db.branch_ids().unwrap(), vec![b_id]);
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
        // An Option so the drop below ends the connection and the checks after it still compile
        // (compile fix, r11-restart lane: the base's `drop(conn)` then `&table(&conn)` did not).
        let mut conn = Some(if who == 0 {
            trunk.clone()
        } else {
            handles[who].as_ref().unwrap().connect().unwrap()
        });
        if op < 12 && live.len() < 10 {
            let child = conn.as_ref().unwrap().fork_branch().unwrap();
            models.push(models[who].clone());
            handles.push(Some(child));
        } else if op < 20 && who != 0 {
            conn = None;
            handles[who] = None;
            models[who] = None;
        } else if op < 23 && who == 0 {
            trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        } else if op < 25 {
            db.branch_compact_now().unwrap();
        } else {
            let conn = conn.as_ref().unwrap();
            let model = models[who].as_mut().unwrap();
            let before = model.clone();
            conn.execute("BEGIN").unwrap();
            for _ in 0..(1 + rng.below(10)) {
                let keys: Vec<i64> = model.keys().copied().collect();
                match rng.below(3) {
                    0 if !keys.is_empty() => {
                        let id = keys[rng.below(keys.len() as u64) as usize];
                        let v = format!("{who}-{step}-{}", "u".repeat(rng.below(200) as usize));
                        set(conn, id, &v);
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
                &table(conn.as_ref().unwrap()),
                models[who].as_ref().unwrap(),
                "step {step}: node {who} diverged"
            );
        }
    }
    drop(handles);
    assert!(db.branch_ids().unwrap().is_empty());
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
        assert_eq!(db.branch_stats().unwrap().live_branches, 1, "the open connection did not defer it");
        db.branch_compact_now().unwrap();
        drop(bc);
        assert_eq!(db.branch_stats().unwrap().live_branches, 0);
        assert!(in_use(&db).is_empty());
    }
    let db = reopen(&path, incarnation);
    assert_eq!(db.branch_stats().unwrap().live_branches, 0, "a released branch came back from a snapshot");
    assert!(in_use(&db).is_empty(), "its pages came back with it");
    assert!(db.branch_ids().unwrap().is_empty());
}

// ---- F5: leases. A crashed agent's branch must not pin its ancestors forever. ----
//
// The lease clock is the store's own: it advances only while the database is open (Chubby's rule:
// a stopped timer is equivalent to extending the lease) and is persisted, so a reopen resumes it
// rather than restarting it at zero or charging the downtime.

use std::time::Duration;

#[test]
fn an_expired_lease_reaps_a_detached_branch_at_the_next_open() {
    // The crashed agent: its branch is detached, its lease runs out, and nothing ever releases it.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        b.lease(Duration::from_secs(10)).unwrap();
        set(&b.connect().unwrap(), 150, "orphaned");
        assert!(!in_use(&db).is_empty());
        let _abandoned = b.into_id();
        db.branch_lease_clock_advance(Duration::from_secs(11));
    }
    let db = reopen(&path, incarnation);
    assert!(db.branch_ids().unwrap().is_empty(), "an expired, abandoned branch survived the restart");
    assert!(in_use(&db).is_empty(), "its pages survived the restart");
}

#[test]
fn the_lease_timer_stops_while_the_database_is_closed() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let b = trunk.fork_branch().unwrap();
        b.lease(Duration::from_secs(10)).unwrap();
        db.branch_lease_clock_advance(Duration::from_secs(6));
        assert!(db.expire_branches().unwrap().reaped.is_empty(), "reaped at 6 of 10 s");
        b_id = b.into_id();
    }
    let db = reopen(&path, incarnation);
    // Resumed, not restarted: the 6 s already spent are still spent...
    assert!(
        db.branch_lease_now() >= Duration::from_secs(6),
        "the lease clock went back to {:?} across a reopen",
        db.branch_lease_now()
    );
    // ...and the time the database was closed was not charged (9 < 10: still alive)...
    db.branch_lease_clock_advance(Duration::from_secs(3));
    assert!(db.expire_branches().unwrap().reaped.is_empty(), "reaped at 9 of 10 s");
    assert_eq!(db.branch_ids().unwrap(), vec![b_id]);
    // ...and it does run out (11 > 10).
    db.branch_lease_clock_advance(Duration::from_secs(2));
    assert_eq!(db.expire_branches().unwrap().reaped, vec![b_id]);
    assert!(db.branch_ids().unwrap().is_empty());
}

#[test]
fn an_expired_interior_is_partially_reclaimed_while_its_live_child_reads_through_it() {
    // The one thing D37 (corrected) leaves this fork to show: in-engine, page-granular, PARTIAL
    // reclamation of an interior branch when its lease runs out, with a live child below it.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let p_id;
    let c_id;
    let kept;
    let fresh;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        p.lease(Duration::from_secs(10)).unwrap();
        set(&p.connect().unwrap(), 7, "p-before-fork");
        kept = p.owned_slots().into_iter().collect::<BTreeSet<u32>>();
        let c = p.fork().unwrap();
        c.lease(Duration::from_secs(100)).unwrap();
        {
            let pc = p.connect().unwrap();
            set(&pc, 60, "p-after-fork");
            set(&pc, 110, "p-after-fork");
        }
        let all: BTreeSet<u32> = p.owned_slots().into_iter().collect();
        fresh = all.difference(&kept).copied().collect::<BTreeSet<u32>>();
        assert_eq!(fresh.len(), 2);

        db.branch_lease_clock_advance(Duration::from_secs(11));
        let expired = db.expire_branches().unwrap();
        assert_eq!(expired.reaped, vec![p.id()], "only the interior's lease ran out");
        assert_eq!(expired.freed_pages, fresh.len(), "partial reclamation freed the wrong amount");
        for slot in &fresh {
            assert!(db.branch_slot_is_free(*slot), "slot {slot}: unreadable, still held");
        }
        for slot in &kept {
            assert!(!db.branch_slot_is_free(*slot), "slot {slot}: the child reads it, freed");
        }
        // The reaped interior takes no new children, and its handle can no longer open it.
        assert!(p.fork().is_err(), "a reaped interior was forked");
        assert!(p.connect().is_err(), "a reaped interior was opened");
        assert_eq!(
            value(&c.connect().unwrap(), 7),
            Some("p-before-fork".to_string()),
            "the live child lost the interior's page it reads"
        );
        p_id = p.id();
        c_id = c.into_id();
        drop(p);
    }

    // Across a restart the partial reclamation holds and the child still reads through.
    let db = reopen(&path, incarnation);
    assert_eq!(db.branch_ids().unwrap(), vec![c_id]);
    assert_eq!(in_use(&db), kept, "a reopen changed what the interior still holds");
    let c = db.branch(c_id).unwrap();
    assert_eq!(value(&c.connect().unwrap(), 7), Some("p-before-fork".to_string()));
    assert_eq!(value(&c.connect().unwrap(), 60), Some(original(60)));
    assert!(db.branch(p_id).is_err(), "the reaped interior came back");

    // When the child's lease runs out too, the chain goes, the interior's last page with it.
    let c_id_again = c.into_id();
    db.branch_lease_clock_advance(Duration::from_secs(100));
    let expired = db.expire_branches().unwrap();
    assert_eq!(expired.reaped, vec![c_id_again]);
    assert!(in_use(&db).is_empty());
    assert_eq!(db.branch_stats().unwrap().live_branches, 0);
}

#[test]
fn expiry_reaps_deepest_first_and_a_fork_never_revives_an_expired_parent() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let p = trunk.fork_branch().unwrap();
    p.lease(Duration::from_secs(5)).unwrap();
    set(&p.connect().unwrap(), 7, "p");
    let c = p.fork().unwrap();
    c.lease(Duration::from_secs(5)).unwrap();
    set(&c.connect().unwrap(), 30, "c");
    db.branch_lease_clock_advance(Duration::from_secs(6));

    // A fork from an expired (not yet reaped) parent is refused: the fork runs the expiry pass
    // first, as Neon refuses "create children from expiring branches".
    assert!(p.fork().is_err(), "forked a child from an expired parent");
    // That pass reaped both, the child first: the interior then goes whole, not in two steps.
    assert!(db.branch_ids().unwrap().is_empty());
    assert!(in_use(&db).is_empty());

    // Expiry order, observed directly on a fresh chain.
    let p = trunk.fork_branch().unwrap();
    p.lease(Duration::from_secs(5)).unwrap();
    let c = p.fork().unwrap();
    c.lease(Duration::from_secs(5)).unwrap();
    let g = c.fork().unwrap();
    g.lease(Duration::from_secs(5)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(6));
    let expired = db.expire_branches().unwrap();
    assert_eq!(expired.reaped, vec![g.id(), c.id(), p.id()], "not deepest first");
}

#[test]
fn a_lease_never_moves_backwards() {
    // Chubby §2.8: the deadline may be advanced, "but may not [be moved] backwards in time". A
    // renewal with a SHORTER ttl must not bring the reap closer.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(100)).unwrap();
    b.lease(Duration::from_secs(1)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(5));
    assert!(
        db.expire_branches().unwrap().reaped.is_empty(),
        "a shorter renewal moved the deadline back"
    );
    // A LONGER renewal does move it forward.
    b.lease(Duration::from_secs(200)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(150));
    assert!(db.expire_branches().unwrap().reaped.is_empty(), "renewal did not extend it");
    db.branch_lease_clock_advance(Duration::from_secs(60));
    assert_eq!(db.expire_branches().unwrap().reaped, vec![b.id()]);
}

#[test]
fn an_expired_branch_is_neither_renewed_nor_opened() {
    // Every structural operation runs the expiry pass first: a lease that ran out is not revived
    // by a late renewal, and a branch whose lease ran out cannot be opened.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let a = trunk.fork_branch().unwrap();
    let b = trunk.fork_branch().unwrap();
    a.lease(Duration::from_secs(5)).unwrap();
    b.lease(Duration::from_secs(5)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(6));
    assert!(a.lease(Duration::from_secs(100)).is_err(), "an expired lease was renewed");
    assert!(b.connect().is_err(), "an expired branch was opened");
    assert!(db.branch_ids().unwrap().is_empty());
}

#[test]
fn an_expired_branch_cannot_be_opened_when_connect_is_the_first_operation() {
    // Separate from the renewal test on purpose: there the renewal's own pass reaps both branches
    // first, so a connect that skipped the pass would still be refused and nothing would notice.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(5)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(6));
    assert!(b.connect().is_err(), "an expired branch was opened");
    assert!(db.branch_ids().unwrap().is_empty());
}

// ---- Review lane_turso_f4f5_review.md @ 56d3252: R1, R2, R3, R4, R6 ----

/// A crash image: every file of the database, copied while it is still OPEN, under a new name.
/// Nothing a clean close would write is in it.
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

/// R1 (HIGH). A release whose log flush fails is not durable, so nothing may be freed for it —
/// not at the release, and not later, when its own close or a child's climb reaches it. The
/// review's schedule reused such a slot and a reopen read Corrupt.
#[test]
fn a_release_whose_log_failed_frees_nothing_now_or_later() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let p_id;
    let c1_id;
    let c2_id;
    let held;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        let pc = p.connect().unwrap();
        set(&pc, 7, "p-pre");
        let c1 = p.fork().unwrap();
        let c2 = p.fork().unwrap();
        // Written after both forks: no child can read these, so a durable release would free them.
        set(&pc, 60, "p-post");
        set(&pc, 110, "p-post");
        held = in_use(&db);
        p_id = p.id();
        c1_id = c1.id();

        db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
        drop(p); // its Release record fails: the journal is poisoned and the release not durable
        assert_eq!(in_use(&db), held, "the failed release freed pages");
        drop(pc); // exit 1: the interior's own close
        assert_eq!(in_use(&db), held, "the interior's close freed pages the journal still names");
        let c1c = c1.connect().unwrap();
        drop(c1); // this Release fails too: the journal is fail-stopped
        // Not the climb exit: c1's own Release fails too (the journal is fail-stopped), so c1 is
        // ReleasePending and its close returns at c1. The climb from a DURABLY released child is
        // `a_durable_child_release_climbing_to_a_pending_interior_frees_nothing_of_it`.
        drop(c1c);
        assert_eq!(in_use(&db), held, "a child's climb retired the interior");

        // And the fail-stopped store takes no write that could reuse a slot.
        let c2c = c2.connect().unwrap();
        assert!(
            c2c.execute("UPDATE t SET v = 'x' WHERE id = 150").is_err(),
            "a write was accepted on a fail-stopped branch store"
        );
        drop(c2c);
        c2_id = c2.into_id();
    }
    let db = reopen(&path, incarnation);
    let ids: BTreeSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
    assert_eq!(ids, BTreeSet::from([p_id, c1_id, c2_id]), "a release that never became durable held");
    assert_eq!(in_use(&db), held, "recovery sees different live slots");
    let pc = db.branch(p_id).unwrap().connect().unwrap();
    assert_eq!(value(&pc, 7), Some("p-pre".to_string()));
    assert_eq!(value(&pc, 60), Some("p-post".to_string()));
    assert_eq!(value(&pc, 110), Some("p-post".to_string()));
    integrity_ok(&pc);
    drop(pc);
    let cc = db.branch(c1_id).unwrap().connect().unwrap();
    assert_eq!(value(&cc, 7), Some("p-pre".to_string()));
    assert_eq!(value(&cc, 60), Some(original(60)));
}

/// R1, the refusal half: after the journal is poisoned, a branch write transaction is refused at
/// its START (not only when its commit record fails, after its pages are already in the arena).
#[test]
fn a_poisoned_store_refuses_a_branch_write_transaction_at_its_start() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let a = trunk.fork_branch().unwrap();
    let d = trunk.fork_branch().unwrap();
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(d); // poisons the journal
    let ac = a.connect().unwrap();
    let err = match ac.execute("UPDATE t SET v = 'x' WHERE id = 7") {
        Ok(()) => panic!("a write was accepted on a fail-stopped branch store"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("no write transaction"), "refused late, or for another reason: {err}");
}

/// R1, the other two refusals: a transaction that began BEFORE the poisoning may write no further
/// page, and its commit writes nothing into the arena file.
#[test]
fn a_transaction_open_across_the_poisoning_writes_no_page_and_no_slot() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let a = trunk.fork_branch().unwrap();
    let d = trunk.fork_branch().unwrap();
    let arena = format!("{}-branch-arena", path.display());
    let ac = a.connect().unwrap();
    ac.execute("BEGIN").unwrap();
    set(&ac, 7, "before-poison"); // reserves a slot; nothing is written until commit
    let len_before = std::fs::metadata(&arena).unwrap().len();

    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(d); // poisons the journal
    let err = match ac.execute("UPDATE t SET v = 'after-poison' WHERE id = 150") {
        Ok(()) => panic!("a page write was accepted on a fail-stopped branch store"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("no page write"), "refused for another reason: {err}");
    assert!(ac.execute("COMMIT").is_err(), "a commit succeeded on a fail-stopped store");
    assert_eq!(
        std::fs::metadata(&arena).unwrap().len(),
        len_before,
        "a fail-stopped store wrote a page into the arena file"
    );
}

/// R2. The lease clock must survive a CRASH, not only a clean close: a branch commit's flush
/// stamps it. The image is copied while the database is open, so no close ever runs.
#[test]
fn a_crash_image_keeps_the_open_time_a_branch_commit_stamped() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let doomed = trunk.fork_branch().unwrap();
    doomed.lease(Duration::from_secs(10)).unwrap();
    set(&doomed.connect().unwrap(), 150, "doomed");
    let doomed_slots = doomed.owned_slots();
    assert!(!doomed_slots.is_empty(), "the doomed branch owns no page: its membership loop is vacuous");
    let doomed_id = doomed.into_id(); // the crashed agent
    let x = trunk.fork_branch().unwrap();
    let xc = x.connect().unwrap(); // connected BEFORE the deadline: no pass runs after it
    db.branch_lease_clock_advance(Duration::from_secs(11));
    set(&xc, 7, "x"); // the only thing that happens past the deadline: a commit
    let image = crash_image(&path, dir.path());

    let crashed = open_at(&image, durable()).unwrap();
    assert!(
        !crashed.branch_ids().unwrap().contains(&doomed_id),
        "a crash lost the open time, and the expired branch survived the restart"
    );
    for slot in &doomed_slots {
        assert!(crashed.branch_slot_is_free(*slot), "slot {slot} survived its branch's expiry");
    }
    drop(xc);
}

/// R2, the pass half: an expiry pass with nothing due (here, a fork's) stamps the clock too, and
/// the fork's own flush makes the stamp durable.
#[test]
fn a_crash_image_keeps_the_open_time_a_fork_stamped() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(10)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(6));
    let _other = trunk.fork_branch().unwrap(); // its pass has nothing due at 6 of 10 s
    let image = crash_image(&path, dir.path());

    let crashed = open_at(&image, durable()).unwrap();
    assert!(
        crashed.branch_lease_now() >= Duration::from_secs(6),
        "the crash image's lease clock went back to {:?}",
        crashed.branch_lease_now()
    );
    drop(b);
}

/// R3. An interior whose lease runs out while its own connection is open: nothing may be retired
/// until that connection closes, because it still reads its own pages — and after the close,
/// exactly the pages no child reads are freed.
#[test]
fn an_interior_expired_under_its_own_open_connection_keeps_reading_its_own_pages() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let p = trunk.fork_branch().unwrap();
    p.lease(Duration::from_secs(10)).unwrap();
    let pc = p.connect().unwrap();
    set(&pc, 7, "p-pre");
    let pre: BTreeSet<u32> = p.owned_slots().into_iter().collect();
    assert!(!pre.is_empty(), "the interior owns no pre-fork page: its membership loop is vacuous");
    let c = p.fork().unwrap();
    c.lease(Duration::from_secs(1000)).unwrap();
    set(&pc, 60, "p-post");
    set(&pc, 110, "p-post");
    let post: BTreeSet<u32> = p
        .owned_slots()
        .into_iter()
        .filter(|s| !pre.contains(s))
        .collect();
    assert_eq!(post.len(), 2);

    db.branch_lease_clock_advance(Duration::from_secs(11));
    let expired = db.expire_branches().unwrap();
    assert_eq!(expired.reaped, vec![p.id()]);
    assert_eq!(expired.freed_pages, 0, "freed pages while the interior's own connection is open");

    // Make pc re-read through the store, not its page cache: a trunk commit changes the WAL, so
    // pc's next read transaction clears its cache.
    set(&trunk, 199, "trunk-moved");
    assert_eq!(value(&pc, 60), Some("p-post".to_string()), "the open connection lost its own write");
    assert_eq!(value(&pc, 110), Some("p-post".to_string()), "the open connection lost its own write");
    assert!(
        pc.execute("UPDATE t SET v = 'late' WHERE id = 150").is_err(),
        "an expired branch took a write"
    );
    let cc = c.connect().unwrap();
    assert_eq!(value(&cc, 7), Some("p-pre".to_string()));
    assert_eq!(value(&cc, 60), Some(original(60)));
    drop(cc);

    drop(pc); // now the retire runs
    for slot in &post {
        assert!(db.branch_slot_is_free(*slot), "slot {slot}: unreadable after the close, still held");
    }
    for slot in &pre {
        assert!(!db.branch_slot_is_free(*slot), "slot {slot}: the child reads it, freed");
    }
}

/// R4. The earlier timer test is closed for milliseconds against a 1 s margin, so a clock that
/// charged downtime would pass it. This one stays closed 2.5 s: a correct clock reads 9 s after
/// the reopen (alive), a downtime-charging one 11.5 s (reaped).
#[test]
fn the_lease_clock_does_not_charge_the_time_the_database_was_closed() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let b_id;
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let b = trunk.fork_branch().unwrap();
        b.lease(Duration::from_secs(10)).unwrap();
        db.branch_lease_clock_advance(Duration::from_secs(6));
        db.expire_branches().unwrap();
        b_id = b.into_id();
    }
    std::thread::sleep(Duration::from_millis(2500));
    let db = reopen(&path, incarnation);
    db.branch_lease_clock_advance(Duration::from_secs(3));
    assert!(
        db.expire_branches().unwrap().reaped.is_empty(),
        "the 2.5 s the database was closed were charged to the lease"
    );
    assert_eq!(db.branch_ids().unwrap(), vec![b_id]);
}

/// R6. A lease whose millisecond count exceeds u64 must saturate to "practically forever", not
/// wrap: 18,446,744,073,709,552 s wraps to a 384 ms lease under `as u64`.
#[test]
fn a_practically_infinite_lease_does_not_wrap_into_a_short_one() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(18_446_744_073_709_552)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(10));
    assert!(db.expire_branches().unwrap().reaped.is_empty(), "the lease wrapped and ran out");
    assert_eq!(db.branch_ids().unwrap(), vec![b.id()]);
}

/// R1, the commit refusal on its own: a transaction with a dirty page, begun before the poisoning,
/// commits with no further statement in between — so nothing else can refuse first, and only the
/// commit's own check stands between its page and the arena file.
#[test]
fn a_commit_open_across_the_poisoning_writes_no_slot() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let a = trunk.fork_branch().unwrap();
    let d = trunk.fork_branch().unwrap();
    let arena = format!("{}-branch-arena", path.display());
    let ac = a.connect().unwrap();
    ac.execute("BEGIN").unwrap();
    set(&ac, 7, "before-poison");
    let len_before = std::fs::metadata(&arena).unwrap().len();
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(d); // poisons the journal
    assert!(ac.execute("COMMIT").is_err(), "a commit succeeded on a fail-stopped store");
    assert_eq!(
        std::fs::metadata(&arena).unwrap().len(),
        len_before,
        "the commit wrote its page into the arena before its record failed"
    );
}

// ---- Re-review lane_turso_rereview.md @ 6c7ec0d: N1-N4, the climb exit, R7's format version ----

/// The climb R1's main test does not reach: a child released DURABLY while its connection was open,
/// BEFORE the journal failed, and closed after the interior's own release failed. Its close climbs
/// to a ReleasePending interior, which must keep everything.
#[test]
fn a_durable_child_release_climbing_to_a_pending_interior_frees_nothing_of_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let p = trunk.fork_branch().unwrap();
    set(&p.connect().unwrap(), 7, "p-pre");
    let c1 = p.fork().unwrap();
    let c2 = p.fork().unwrap();
    {
        let pc = p.connect().unwrap();
        set(&pc, 60, "p-post");
        set(&pc, 110, "p-post");
    }
    let c1c = c1.connect().unwrap();
    drop(c1); // durable Release; deferred because c1c is open
    let held = in_use(&db);
    // Review 3 F6: `held` comes from the subject, so pin what it must contain. P's post-fork
    // pages are what a wrongly retired interior frees (no live child's interval covers them).
    let p_slots = p.owned_slots();
    assert!(!p_slots.is_empty(), "the interior owns no page: the climb has nothing to free");
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(p); // the interior's Release fails: ReleasePending
    drop(c1c); // c1 is freed (durably released), and its close climbs to P
    assert_eq!(in_use(&db), held, "a durable child's climb retired a pending interior");
    for slot in &p_slots {
        assert!(!db.branch_slot_is_free(*slot), "slot {slot} of the pending interior was freed");
    }
    drop(c2);
}

/// N2. Trunk commits must stamp the lease clock too: a workload that writes ONLY the trunk would
/// otherwise never make open time durable, and a crash would keep an abandoned leased branch alive.
#[test]
fn a_crash_image_keeps_the_open_time_trunk_commits_stamped() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let abandoned = trunk.fork_branch().unwrap();
    abandoned.lease(Duration::from_secs(10)).unwrap();
    let abandoned_id = abandoned.into_id();
    // The first write of this page after the fork buffers its pre-image, so that commit's barrier
    // flushes anyway. The ones after the deadline rewrite the same page in the same epoch: nothing
    // is buffered, and the barrier's fast path is the path under test (INFERRED: no hook exposes
    // `unsynced`; the test is red on the unfixed code either way).
    set(&trunk, 150, "trunk-first");
    db.branch_lease_clock_advance(Duration::from_secs(11));
    // Only the trunk is written after the deadline: no fork, connect, renew or expiry call.
    set(&trunk, 150, "trunk-again");
    set(&trunk, 150, "trunk-again-2");
    let image = crash_image(&path, dir.path());

    let crashed = open_at(&image, durable()).unwrap();
    assert!(
        !crashed.branch_ids().unwrap().contains(&abandoned_id),
        "trunk-only traffic left the lease clock unstamped, and the abandoned branch survived"
    );
}

/// N3, clean-close half. A stamp that was only QUEUED (by a pass) is not durable; a clean close
/// must still write it even when the clock has not moved since it was queued.
#[test]
fn a_queued_stamp_is_not_lost_at_a_clean_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        db.branch_lease_clock_freeze();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let b = trunk.fork_branch().unwrap();
        b.lease(Duration::from_secs(10)).unwrap();
        db.branch_lease_clock_advance(Duration::from_secs(2));
        drop(b.connect().unwrap()); // its pass QUEUES a stamp at exactly 2 s
        let _ = b.into_id(); // then the close happens at the same (frozen) instant
    }
    let db = reopen(&path, incarnation);
    assert!(
        db.branch_lease_now() >= Duration::from_secs(2),
        "the queued stamp died with the journal at a clean close: clock {:?}",
        db.branch_lease_now()
    );
}

/// N3, explicit-flush half. `expire_branches` promises a durable stamp; one QUEUED at the same
/// instant must not make it skip the flush.
#[test]
fn expire_branches_flushes_a_stamp_that_was_only_queued() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    db.branch_lease_clock_freeze();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(10)).unwrap();
    db.branch_lease_clock_advance(Duration::from_secs(2));
    drop(b.connect().unwrap()); // queues a stamp at 2 s
    db.expire_branches().unwrap(); // must flush it
    let image = crash_image(&path, dir.path());
    let crashed = open_at(&image, durable()).unwrap();
    assert!(
        crashed.branch_lease_now() >= Duration::from_secs(2),
        "expire_branches returned without making the stamp durable: clock {:?}",
        crashed.branch_lease_now()
    );
    drop(b);
}

/// N4. On a fail-stopped store the expiry pass cannot make a Release durable. It must SAY so,
/// not report "nothing was due".
#[test]
fn a_fail_stopped_store_refuses_to_report_an_empty_expiry() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    let d = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(5)).unwrap();
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(d); // poisons
    db.branch_lease_clock_advance(Duration::from_secs(6));
    assert!(db.expire_branches().is_err(), "a fail-stopped pass reported an empty expiry");
    drop(b);
}

/// N4. Nor may a fail-stopped store open a branch whose lease has run out: the pass that would have
/// reaped it cannot run, so the connect must refuse on its own.
#[test]
fn a_fail_stopped_store_does_not_open_an_expired_branch() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    let d = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(5)).unwrap();
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    drop(d); // poisons
    db.branch_lease_clock_advance(Duration::from_secs(6));
    assert!(b.connect().is_err(), "an expired branch was opened on a fail-stopped store");
}

/// N4. A reap whose Release could not be made durable did not happen durably: it is an error, not
/// a "deferred" success.
#[test]
fn a_reap_that_is_not_durable_is_an_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    db.branch_failpoint(Some(BranchFailpoint::LogFlushFails));
    assert!(b.reap().is_err(), "a reap that is not durable reported success");
}

/// N1. A second branch store over the same files — a reopen inside the registry's `Weak` window,
/// or `Database::do_open`, which skips the registry — must refuse at open while the first lives,
/// and open once it has gone.
#[test]
fn a_second_store_over_live_branch_files_refuses_at_open() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    set(&b.connect().unwrap(), 3, "b");
    // fastest-engine (PREREG v1 amendment 39, a base red fixed, not registered): the second store
    // opens in the SAME mode as the first. The base opened it in snapshot mode whatever
    // `R11_BRANCH_CATALOG` said, so in the catalog arm the first refusal below came from the mode
    // check (a catalog store's files), not from the lock, and the open after the drop failed the
    // same way: the lock was never tested in that arm. The refusal must now be the lock's.
    let durability = durable().branch_durability;
    let refused = store::BranchStore::open(durability, None, path.to_str().unwrap());
    assert!(
        matches!(refused, Err(LimboError::LockingError(_))),
        "a second store over a live store's branch log was not refused by its lock: {:?}",
        refused.map(|_| ())
    );
    let b_id = b.into_id();
    drop(trunk);
    drop(db);
    let second = store::BranchStore::open(durability, None, path.to_str().unwrap())
        .expect("the first store is gone, so its lock is too");
    assert!(second.ids().unwrap().contains(&b_id), "the refused open damaged the first store's state");
}

/// N1, the arena half of the lazy door. A store that opened before any branch file existed takes
/// no lock until its first fork; that fork must be refused by the live store's log lock BEFORE it
/// truncates the live store's arena. (Written after the fix: its red state against 417bcea9a is
/// INFERRED from reading, where `ensure_backing` truncated the arena and then re-created the log.)
#[test]
fn a_refused_lazy_create_leaves_the_live_stores_arena_intact() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let durability = BranchDurability::Durable { sync: crate::branch::SyncClass::Fsync };
    let late = store::BranchStore::open(durability, None, path.to_str().unwrap()).unwrap();
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    set(&b.connect().unwrap(), 3, "b");
    let schema = trunk.schema.read().clone();
    assert!(
        late.fork_trunk_locked(schema, 4096).is_err(),
        "a second store forked over a live store's branch files"
    );
    // A fresh connection has an empty page cache, so this read comes from the arena file.
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 3), Some("b".to_string()));
    integrity_ok(&bc);
}

/// N2, the other half of the barrier: a trunk commit that DOES buffer a pre-image (the first write
/// of a page since the fork) must carry the stamp in that same flush. A workload whose every commit
/// touches a fresh page never takes the stamp-only path. (Written after the fix: its red state
/// against 417bcea9a is INFERRED from reading, where that flush carried no `Clock`.)
#[test]
fn a_crash_image_keeps_the_open_time_a_trunk_preimage_commit_stamped() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let abandoned = trunk.fork_branch().unwrap();
    abandoned.lease(Duration::from_secs(10)).unwrap();
    let abandoned_id = abandoned.into_id();
    db.branch_lease_clock_advance(Duration::from_secs(11));
    // The only commit after the deadline, and the first write of its page since the fork.
    set(&trunk, 150, "trunk-first-after-deadline");
    let image = crash_image(&path, dir.path());

    let crashed = open_at(&image, durable()).unwrap();
    assert!(
        !crashed.branch_ids().unwrap().contains(&abandoned_id),
        "a trunk commit's pre-image flush carried no stamp, and the abandoned branch survived"
    );
}

// ---- Review 3 (lane_turso_review3.md @ 387913e) ----

/// Review 3 F3. A branch-log creation that fails after writing its header is this store's own
/// I/O failure. The retry must report fail-stop — not blame "another store instance" for the
/// header it wrote itself — and a reopen must recover the created log.
#[test]
fn a_failed_log_create_reports_its_own_fail_stop_not_another_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        db.branch_failpoint(Some(BranchFailpoint::CreateFailsAfterHeader));
        assert!(trunk.fork_branch().is_err(), "the failpoint did not fire");
        let err = match trunk.fork_branch() {
            Ok(_) => panic!("forked on a store whose log creation failed"),
            Err(err) => err.to_string(),
        };
        assert!(!err.contains("another store"), "blamed another store for its own header: {err}");
        assert!(err.contains("fail-stopped"), "refused for another reason: {err}");
    }
    let db = reopen(&path, incarnation);
    let trunk = db.connect().unwrap();
    let b = trunk.fork_branch().expect("a reopen recovers the created log");
    set(&b.connect().unwrap(), 3, "b");
    assert_eq!(value(&b.connect().unwrap(), 3), Some("b".to_string()));
}

/// Review 3, read-only opens — REVISED by review 4 C2, the lead's decision (this test's two
/// "refused" assertions were that earlier spec and are replaced, not weakened). A read-only open
/// of a database WITH branch files opens without its branch store: no recovery, no lock, no branch
/// file touched, the trunk readable, and every branch operation refused by name. Durable +
/// read-only with NO branch files stays refused: a fork would create them, and nothing below the
/// connection refuses a fork on a read-only database.
#[test]
fn a_read_only_open_of_a_database_with_branches_reads_the_trunk_and_nothing_else() {
    let dir = tempfile::TempDir::new().unwrap();
    // The control: a read-only open of a database without branch files works at all.
    let plain = dir.path().join("plain.db");
    {
        let db = open_at(&plain, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 5);
    }
    {
        let ro = open_read_only(&plain, DatabaseOpts::new()).expect("read-only opens work");
        assert_eq!(value(&ro.connect().unwrap(), 3), Some(original(3)));
    }
    assert!(
        open_read_only(&plain, durable()).is_err(),
        "a read-only open was given a durable store it could fork into"
    );

    let path = dir.path().join("durable.db");
    let b_id;
    {
        let db = open_at(&path, durable()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let b = trunk.fork_branch().unwrap();
        set(&b.connect().unwrap(), 3, "b");
        b_id = b.into_id();
    }
    let log = std::path::PathBuf::from(format!("{}-branch-log", path.display()));
    let arena = std::path::PathBuf::from(format!("{}-branch-arena", path.display()));
    // A torn tail: what a read-write recovery cuts off, and a read-only open must leave alone.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
        f.write_all(&[0xAB; 11]).unwrap();
    }
    let log_before = std::fs::read(&log).unwrap();
    let arena_before = std::fs::read(&arena).unwrap();
    // Review 5 T-1: nor may it create a snapshot or a temp snapshot. Neither exists beforehand.
    let snap = std::path::PathBuf::from(format!("{}-branch-snap", path.display()));
    let snap_tmp = std::path::PathBuf::from(format!("{}-branch-snap.tmp", path.display()));
    assert!(!snap.exists() && !snap_tmp.exists(), "the fixture already has a snapshot");
    let refused = |what: &str, err: Option<String>| {
        let err = err.unwrap_or_else(|| panic!("{what} was allowed on a read-only trunk-only handle"));
        assert!(err.contains("branch store was not opened"), "{what} refused for another reason: {err}");
    };
    for opts in [durable(), DatabaseOpts::new()] {
        let ro = open_read_only(&path, opts)
            .expect("a read-only open of a database with branches must still read its trunk");
        let conn = ro.connect().unwrap();
        assert_eq!(value(&conn, 3), Some(original(3)), "the trunk read wrong");
        refused("fork", conn.fork_branch().err().map(|e| e.to_string()));
        refused("attaching a branch", ro.branch(b_id).err().map(|e| e.to_string()));
        refused("the expiry pass", ro.expire_branches().err().map(|e| e.to_string()));
    }
    assert_eq!(std::fs::read(&log).unwrap(), log_before, "a read-only open wrote the branch log");
    assert_eq!(std::fs::read(&arena).unwrap(), arena_before, "a read-only open wrote the arena");
    assert!(!snap.exists(), "a read-only open wrote a branch snapshot");
    assert!(!snap_tmp.exists(), "a read-only open left a temp snapshot");
}

/// Review 3, path identity. The registry knows a database by (dev, ino), but the branch files
/// were named from the path STRING: a volatile open through a symlink found no branch files, was
/// admitted, and its trunk writes would change what the real path's branches read.
#[cfg(unix)]
#[test]
fn a_symlinked_open_finds_the_real_paths_branch_files() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("real.db");
    let b_id;
    {
        let db = open_at(&path, durable()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let b = trunk.fork_branch().unwrap();
        set(&b.connect().unwrap(), 3, "b");
        b_id = b.into_id();
    }
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(
        open_at(&link, DatabaseOpts::new()).is_err(),
        "a volatile open through a symlink ignored the real path's durable branches"
    );
    let db = open_at(&link, durable()).unwrap();
    assert!(db.branch_ids().unwrap().contains(&b_id), "a durable open through a symlink lost the branches");
}

/// Review 3 F6. A failed stamp-only flush must not fail the trunk commit that carried it — but
/// the flush poisoned the journal, so the store is fail-stopped from then on.
#[test]
fn a_failed_stamp_flush_keeps_the_trunk_commit_and_fail_stops_the_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let b = trunk.fork_branch().unwrap();
    b.lease(Duration::from_secs(10)).unwrap();
    set(&trunk, 150, "first"); // the page's pre-image: its barrier takes the pre-image path
    db.branch_lease_clock_advance(Duration::from_secs(2));
    db.branch_failpoint(Some(BranchFailpoint::StampFlushFails));
    // The same page again: nothing to retain, a stamp due — the stamp-only path, and it fails.
    set(&trunk, 150, "second");
    assert_eq!(value(&trunk, 150), Some("second".to_string()), "the trunk commit was lost");
    let bc = b.connect().unwrap();
    let err = match bc.execute("UPDATE t SET v = 'x' WHERE id = 7") {
        Ok(()) => panic!("a branch write was accepted after the stamp flush failed"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("no write transaction"), "refused for another reason: {err}");
}

// ---- Review 4 (lane_turso_review4.md @ 059dd56) ----

/// Review 4 C1 and C6. The store-level half of the fork gate: a forked child holding a whole
/// `Database` must write nothing — not the arena (a slot from its copy of the free list is the
/// parent's next slot), not the log — and its refusals must name the fork, not an I/O failure.
/// Runs alone in a fresh process (see `fork_driver`). Gate: `cfg(unix)` — see `fork_driver` for why
/// no narrower gate is needed (Android included).
#[cfg(unix)]
#[test]
fn a_forked_child_cannot_write_through_an_inherited_database() {
    use crate::branch::fork_driver;
    let Some(sentinel) = fork_driver::alone(
        "branch::durability_tests::a_forked_child_cannot_write_through_an_inherited_database",
    ) else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 3, "b");
    let arena = std::path::PathBuf::from(format!("{}-branch-arena", path.display()));
    let log = db.branch_log_path().expect("a durable store has a log");
    let arena_before = std::fs::read(&arena).unwrap();
    let log_before = std::fs::read(&log).unwrap();
    // SAFETY: the child runs two statements and `_exit`s; it never returns into the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let refusal = |conn: &Arc<Connection>, sql: &str| conn.execute(sql).err().map(|e| e.to_string());
            // A branch write, and a trunk write of a page the branch still reads.
            let on_branch = refusal(&bc, "UPDATE t SET v = 'child' WHERE id = 7");
            let on_trunk = refusal(&trunk, "UPDATE t SET v = 'child' WHERE id = 150");
            let mut code = 0;
            if let Some(err) = on_branch {
                code |= 1;
                if err.contains("fork") {
                    code |= 4;
                }
            }
            if let Some(err) = on_trunk {
                code |= 2;
                if err.contains("fork") {
                    code |= 8;
                }
            }
            code
        }))
        .unwrap_or(100);
        // SAFETY: ends the child without running the harness or any destructor.
        unsafe { libc::_exit(code) };
    }
    let code = fork_driver::exit_code(pid);
    assert_ne!(code, 100, "the forked child panicked");
    assert_eq!(code & 3, 3, "a forked child's write was accepted (bits: 1 branch, 2 trunk): {code}");
    assert_eq!(std::fs::read(&arena).unwrap(), arena_before, "a forked child wrote the parent's arena");
    assert_eq!(std::fs::read(&log).unwrap(), log_before, "a forked child wrote the parent's log");
    assert_eq!(code & 12, 12, "a forked child's refusal did not name the fork (bits: 4 branch, 8 trunk): {code}");
    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 3), Some("b".to_string()));
    integrity_ok(&bc);
    fork_driver::finished(&sentinel);
}

/// Review 4 C3 (the lead's decision): every sidecar is named from ONE canonical path computed at
/// open. A symlinked open must write the real path's WAL — not a WAL of its own that a crash would
/// leave holding frames the real path never sees.
#[cfg(unix)]
#[test]
fn a_symlinked_open_writes_the_real_paths_wal() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 20);
    }
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    {
        let db = open_at(&link, DatabaseOpts::new()).unwrap();
        set(&db.connect().unwrap(), 3, "via-link");
        assert!(
            !Path::new(&format!("{}-wal", link.display())).exists(),
            "a symlinked open wrote a WAL of its own beside the real path's"
        );
    }
    let db = open_at(&real, DatabaseOpts::new()).unwrap();
    assert_eq!(value(&db.connect().unwrap(), 3), Some("via-link".to_string()));
}

/// Review 4 C3. A sidecar already present under a NON-canonical name (a WAL or branch file written
/// through a link before sidecars were named canonically) is refused, naming both files: opening
/// past it would silently miss the frames or branches it holds.
#[cfg(unix)]
#[test]
fn an_open_past_a_sidecar_under_another_name_is_refused_naming_both() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 20);
    }
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    std::fs::write(format!("{}-wal", link.display()), [0x5A; 4096]).unwrap();
    let err = match open_at(&link, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past a WAL under another name"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("link.db-wal") && err.contains("real.db-wal"),
        "the refusal must name both WAL files: {err}"
    );

    let link2 = dir.path().join("link2.db");
    std::os::unix::fs::symlink(&real, &link2).unwrap();
    std::fs::write(format!("{}-branch-log", link2.display()), b"x").unwrap();
    let err = match open_at(&link2, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past a branch log under another name"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("link2.db-branch-log") && err.contains("real.db-branch-log"),
        "the refusal must name both branch logs: {err}"
    );
}

/// Review 4 C4. When the database path is not a file on this filesystem (here a `MemoryIO` name),
/// the branch files — always real files — must still be named from an ABSOLUTE path, or they are
/// resolved against whatever the working directory is at the first fork. Gate: `cfg(unix)` — the
/// relative path is built by stripping the root `/` (review 5 T-1: it panicked on Windows).
#[cfg(unix)]
#[test]
fn branch_files_of_a_relative_path_are_named_absolutely() {
    let dir = tempfile::TempDir::new().unwrap();
    // A relative path from the working directory into the temp dir, so nothing lands in the tree.
    let cwd = std::env::current_dir().unwrap();
    let mut rel = std::path::PathBuf::new();
    for _ in 1..cwd.components().count() {
        rel.push("..");
    }
    rel.push(dir.path().join("mem.db").strip_prefix("/").unwrap());
    let rel = rel.to_str().unwrap().to_string();
    let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        &rel,
        OpenFlags::Create,
        durable(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 5);
    let b = trunk.fork_branch().unwrap();
    let log = db.branch_log_path().expect("a durable store has a log");
    assert!(log.is_absolute(), "branch files named from a relative path: {}", log.display());
    drop(b);
}

/// Review 4 C2: on a trunk-only handle the branch QUERIES refuse too — "no branches" would be a
/// lie about a database that has them. (New API: its red is its mutant, not an older commit.)
#[test]
fn a_trunk_only_handle_refuses_branch_queries() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    {
        let db = open_at(&path, durable()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let ro = open_read_only(&path, DatabaseOpts::new()).unwrap();
    assert!(ro.branch_ids().is_err(), "a trunk-only handle listed branches it never opened");
    assert!(ro.branch_stats().is_err(), "a trunk-only handle reported branch statistics");
}

/// Review 4 C4: only "no such file" may fall back to the path as given. Any other failure to
/// resolve — here a symlink loop — refuses, rather than naming sidecars from an unresolved path.
#[cfg(unix)]
#[test]
fn a_path_that_cannot_be_resolved_is_refused_not_used_as_given() {
    let dir = tempfile::TempDir::new().unwrap();
    let a = dir.path().join("a.db");
    let b = dir.path().join("b.db");
    std::os::unix::fs::symlink(&b, &a).unwrap();
    std::os::unix::fs::symlink(&a, &b).unwrap();
    assert!(
        crate::database::sidecar_base(a.to_str().unwrap()).is_err(),
        "a path in a symlink loop was used as given"
    );
    let missing = dir.path().join("missing.db");
    assert_eq!(crate::database::sidecar_base(missing.to_str().unwrap()).unwrap(), None);
    assert!(Path::new(&crate::database::absolute_path("relative.db").unwrap()).is_absolute());
}

// ---- Review 5 (lane_turso_review5.md @ 616a9fd) ----

/// Review 5 C3-1 (lead's decision). Every clean close leaves a 0-byte WAL (a Truncate checkpoint
/// never removes the file), and code before review 4 named it from the path as given. A symlinked
/// database closed cleanly that way must reopen: an empty or header-only WAL carries nothing to
/// lose, so it is left in place and the open proceeds. A WAL that holds frames is still refused,
/// and the refusal names the safe action.
#[cfg(unix)]
#[test]
fn a_symlinked_database_closed_cleanly_under_the_old_naming_reopens() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 20);
    }
    for (name, stale) in [("empty.db", &[][..]), ("header.db", &[0u8; 32][..])] {
        let link = dir.path().join(name);
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let stale_wal = std::path::PathBuf::from(format!("{}-wal", link.display()));
        std::fs::write(&stale_wal, stale).unwrap();
        {
            let db = open_at(&link, DatabaseOpts::new())
                .unwrap_or_else(|e| panic!("{name}: an empty stale WAL refused the open: {e}"));
            assert_eq!(value(&db.connect().unwrap(), 3), Some(original(3)));
        }
        assert_eq!(
            std::fs::read(&stale_wal).unwrap(),
            stale,
            "{name}: the stale WAL was not left in place, untouched"
        );
    }
    let link = dir.path().join("frames.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    std::fs::write(format!("{}-wal", link.display()), [0x5A; 4096]).unwrap();
    let err = match open_at(&link, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past a WAL that holds frames under another name"),
        Err(err) => err.to_string(),
    };
    // Review 6 item 4 (lead's decision) replaced the remedy this line pinned ("open through the
    // old name and close cleanly, which checkpoints") because close does not always checkpoint and
    // the link may have been retargeted: the refusal must name a remedy that is true.
    assert!(
        err.contains("rename") && err.contains("aside") && !err.contains("open the database through"),
        "the refusal must name a true remedy, not one acted through the link: {err}"
    );
}

/// Review 5 C3-4 (lead's decision): the MVCC logical log is a sidecar too. One found under the
/// given name that is not the real path's is refused, naming both.
#[cfg(unix)]
#[test]
fn an_open_past_a_logical_log_under_another_name_is_refused_naming_both() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 20);
    }
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    std::fs::write(dir.path().join("link.db-log"), [0x5A; 64]).unwrap();
    let err = match open_at(&link, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past a logical log under another name"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("link.db-log") && err.contains("real.db-log"),
        "the refusal must name both logs: {err}"
    );
}

/// Review 5 C3-5 (lead's decision): a sidecar is "the same file" by identity, (dev, ino), not by
/// canonical path string. A WAL reached under the link's name through a HARD link to the real
/// path's WAL is the real WAL, holding its frames, and must not be refused.
#[cfg(unix)]
#[test]
fn a_sidecar_hard_linked_to_the_real_one_is_the_same_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("live.db");
    let db = open_at(&path, DatabaseOpts::new()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    set(&trunk, 3, "in-the-wal");
    // A copy taken while open: its WAL holds the frames of the last commit.
    let image = crash_image(&path, dir.path());
    let image_wal = std::path::PathBuf::from(format!("{}-wal", image.display()));
    assert!(std::fs::metadata(&image_wal).unwrap().len() > 32, "the WAL holds no frame");
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&image, &link).unwrap();
    std::fs::hard_link(&image_wal, format!("{}-wal", link.display())).unwrap();
    let copy = open_at(&link, DatabaseOpts::new())
        .unwrap_or_else(|e| panic!("a hard link to the real WAL was taken for another WAL: {e}"));
    assert_eq!(value(&copy.connect().unwrap(), 3), Some("in-the-wal".to_string()));
}

/// Review 5 C3-3 (lead's decision): a VOLATILE open does not pay for branch files it will never
/// write. With no working directory to resolve against (as on wasm32-unknown-unknown, or here a
/// deleted one), a volatile open of a relative non-filesystem name must still open. Runs alone in
/// a fresh process, because it changes the process's working directory. Gate: `cfg(unix)`.
#[cfg(unix)]
#[test]
fn a_volatile_open_needs_no_working_directory() {
    use crate::branch::fork_driver;
    let Some(sentinel) = fork_driver::alone(
        "branch::durability_tests::a_volatile_open_needs_no_working_directory",
    ) else {
        return;
    };
    // FLAGGED TEST EDIT (fixture only, the premise and the assertion unchanged): the doomed working
    // directory sits in a fresh, otherwise empty parent. macOS's getcwd of a deleted directory
    // scans its parent's entries (sampled: realpath -> __private_getcwd -> readdir/fstatat), and
    // under a shared TMPDIR of 166,390 entries that scan outlived the fresh process's 180 s
    // deadline, so the test failed for the size of TMPDIR, not for the open.
    let parent = tempfile::TempDir::new().unwrap();
    let gone = tempfile::TempDir::new_in(parent.path()).unwrap();
    std::env::set_current_dir(gone.path()).unwrap();
    std::fs::remove_dir(gone.path()).unwrap();
    assert!(std::env::current_dir().is_err(), "premise: the working directory still resolves");
    let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "relative.db",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| panic!("a volatile open needed the working directory: {e}"));
    seed(&db.connect().unwrap(), 3);
    fork_driver::finished(&sentinel);
}

/// Review 5 C2-1 (lead's decision): a read-write open must never inherit a trunk-only instance
/// from the process registry — its connections would be read-only and its branch operations
/// refused. It is refused by name until the read-only handle is closed.
#[test]
fn a_read_write_open_does_not_inherit_a_trunk_only_instance() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    {
        let db = open_at(&path, durable()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let ro = open_read_only(&path, DatabaseOpts::new()).unwrap();
    let err = match open_at(&path, durable()) {
        Ok(_) => panic!("a read-write open inherited the read-only, trunk-only instance"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("close the read-only handle first"), "refused for another reason: {err}");
    drop(ro);
    open_at(&path, durable()).expect("with the read-only handle closed, a read-write open works");
}

/// The empty-log wedge (queued by the lead): a branch-log creation that fails to lock leaves an
/// EMPTY log behind. An empty log holds no branch state, so a later read-write VOLATILE open must
/// not refuse because of it.
#[test]
fn an_empty_branch_log_does_not_refuse_a_volatile_open() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    {
        let db = open_at(&path, durable()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 20);
        db.branch_failpoint(Some(BranchFailpoint::CreateLockFails));
        assert!(trunk.fork_branch().is_err(), "the failpoint did not fire");
    }
    let log = std::path::PathBuf::from(format!("{}-branch-log", path.display()));
    assert_eq!(std::fs::metadata(&log).unwrap().len(), 0, "premise: the failed create left an empty log");
    let db = open_at(&path, DatabaseOpts::new())
        .unwrap_or_else(|e| panic!("an empty branch log refused a volatile open: {e}"));
    assert_eq!(value(&db.connect().unwrap(), 3), Some(original(3)));
}

/// Lead finding on 676a6573f, as REBUILT after review 6 §2: the first construction's premise was
/// false (a clean boot after commits leaves records; neither drop nor close truncates them), so it
/// failed at every commit on its premise. What holds: a FIRST MVCC bootstrap that commits nothing
/// leaves the logical log at exactly `LOG_HDR_SIZE` bytes. Under a name the log does not use, such
/// a header-only log carries no transaction, so it is left in place and the open proceeds. A log
/// that holds records is still refused, naming both. (Old-name logs came only from the paths that
/// named the log from the GIVEN path before review 5: the existence check at open, the external
/// restore and a fresh ATTACH's conversion; upstream's Init and journal-mode switch already used the
/// canonical path.)
#[cfg(unix)]
#[test]
fn a_symlinked_mvcc_database_closed_cleanly_under_the_old_naming_reopens() {
    use crate::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE;
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    let real_log = dir.path().join("real.db-log");
    // Rows written in WAL mode live in the database file once the switch to MVCC checkpoints them.
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'kept')").unwrap();
    }
    // A first MVCC bootstrap that commits nothing.
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        db.connect().unwrap().execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    let header_only = std::fs::read(&real_log).unwrap();
    assert_eq!(
        header_only.len(),
        LOG_HDR_SIZE,
        "premise: a first MVCC bootstrap with no commit leaves a header-only log"
    );

    // The old naming: the log sits under the link's name, and there is none under the real one.
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let stale_log = dir.path().join("link.db-log");
    std::fs::rename(&real_log, &stale_log).unwrap();
    {
        let db = open_at(&link, DatabaseOpts::new())
            .unwrap_or_else(|e| panic!("a header-only MVCC log under the old name refused the open: {e}"));
        let found = rows(&db.connect().unwrap(), "SELECT v FROM t WHERE id = 1");
        assert_eq!(found, vec![vec![Value::from_text("kept")]]);
    }
    assert_eq!(
        std::fs::read(&stale_log).unwrap(),
        header_only,
        "the stale header-only log was not left in place, untouched"
    );

    // A log longer than its header can hold records: still refused, naming both.
    let link2 = dir.path().join("link2.db");
    std::os::unix::fs::symlink(&real, &link2).unwrap();
    std::fs::write(dir.path().join("link2.db-log"), vec![0x5A; LOG_HDR_SIZE + 64]).unwrap();
    let err = match open_at(&link2, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past an MVCC log longer than its header under another name"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("link2.db-log") && err.contains("real.db-log"),
        "the refusal must name both logs: {err}"
    );
}

// ---- Review 6 (lane_turso_review6.md @ 439a4b3) ----

/// Review 6 item 4 (lead's decision). A refusal of a sidecar under another name must name a remedy
/// that is TRUE for its kind of file, and must never advise acting through the link, which may
/// have been retargeted since the file was written. (A clean close does not always checkpoint: not
/// for MVCC, the sync engine, or a connection whose checkpoints are disabled.)
#[cfg(unix)]
#[test]
fn a_sidecar_refusal_names_a_remedy_true_for_its_file() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 5);
    }
    let refusal = |name: &str, suffix: &str, bytes: &[u8]| {
        let link = dir.path().join(name);
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let sidecar = if suffix == "-log" {
            link.with_extension("db-log")
        } else {
            std::path::PathBuf::from(format!("{}{suffix}", link.display()))
        };
        std::fs::write(&sidecar, bytes).unwrap();
        match open_at(&link, DatabaseOpts::new()) {
            Ok(_) => panic!("{name}: opened past {} under another name", sidecar.display()),
            Err(err) => err.to_string(),
        }
    };
    for (name, suffix) in [("wal.db", "-wal"), ("log.db", "-log")] {
        let err = refusal(name, suffix, &[0x5A; 4096]);
        assert!(!err.contains("open the database through"), "{name}: a remedy through the link: {err}");
        assert!(err.contains("rename"), "{name}: no way to keep what it holds: {err}");
        assert!(err.contains("aside"), "{name}: no way to proceed without it: {err}");
    }
    let err = refusal("branch.db", "-branch-log", b"x");
    assert!(err.contains("aside"), "a branch file's refusal names no remedy: {err}");
    assert!(
        !err.contains("remove one of them"),
        "a branch file's refusal may advise removing this database's own branch files: {err}"
    );
}

/// Review 6 item 5 (lead's decision). A database whose file name is 244–250 bytes long (244–251
/// where the multiprocess `-tshm` probe does not run; see `journal::cannot_exist`) has names that
/// fit NAME_MAX for itself and its `-wal`, but not for `-branch-snap` (12 bytes), nor, from 245
/// bytes, for `-branch-log` (11 bytes; review 10 F3): `stat` fails with ENAMETOOLONG, which means
/// the branch file cannot exist. A volatile open must not refuse. This test's name is 248 bytes,
/// where neither fits (review 9 finding 7a).
#[cfg(unix)]
#[test]
fn a_name_too_long_for_branch_files_opens_volatile() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join(format!("{}.db", "n".repeat(245)));
    // Review 7 item 5: the premise, asserted — on a filesystem with a larger NAME_MAX this test
    // would otherwise pass without testing anything.
    let branch_log = format!("{}-branch-log", path.display());
    assert_eq!(
        std::fs::metadata(&branch_log).unwrap_err().raw_os_error(),
        Some(libc::ENAMETOOLONG),
        "premise: the branch log's name is too long for this filesystem"
    );
    let db = open_at(&path, DatabaseOpts::new())
        .unwrap_or_else(|e| panic!("a name too long for branch files refused a volatile open: {e}"));
    seed(&db.connect().unwrap(), 3);
    assert_eq!(value(&db.connect().unwrap(), 2), Some(original(2)));
}

/// Review 6 item 6 (lead's decision). The process registry must not hand a read-write open an
/// instance opened with ANOTHER branch durability: a durable caller receiving a volatile instance
/// forks branches that vanish on a crash, and a `sync: true` caller receiving a `sync: false` one
/// forks branches that are not synced — both silently. Refused by name, like C2-1.
#[test]
fn a_registry_hit_of_another_branch_durability_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("volatile.db");
    let volatile = open_at(&path, DatabaseOpts::new()).unwrap();
    seed(&volatile.connect().unwrap(), 3);
    let err = match open_at(&path, durable()) {
        Ok(_) => panic!("a durable open received the volatile instance"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("branch durability"), "refused for another reason: {err}");
    drop(volatile);
    open_at(&path, durable()).expect("with the volatile handle closed, a durable open works");

    let path = dir.path().join("nosync.db");
    let nosync = DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: crate::branch::SyncClass::Off });
    let unsynced = open_at(&path, nosync).unwrap();
    seed(&unsynced.connect().unwrap(), 3);
    let err = match open_at(&path, durable()) {
        Ok(_) => panic!("a sync: true open received the sync: false instance"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("branch durability"), "refused for another reason: {err}");
}

// ---- Review 7 (lane_turso_review7.md @ 51196f4) ----

/// Review 7 item 4 (lead's decision). The "keep them" half of the WAL/log remedy must hold only
/// while the canonical file does NOT exist: any open of the real path creates it, even empty, and
/// its pages may then be newer than the frames. A 0-byte canonical file must not read as "nothing
/// was written since".
#[cfg(unix)]
#[test]
fn a_sidecar_refusal_keeps_frames_only_while_the_real_file_does_not_exist() {
    let dir = tempfile::TempDir::new().unwrap();
    let real = dir.path().join("real.db");
    {
        let db = open_at(&real, DatabaseOpts::new()).unwrap();
        seed(&db.connect().unwrap(), 5);
    }
    let link = dir.path().join("link.db");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    std::fs::write(format!("{}-wal", link.display()), [0x5A; 4096]).unwrap();
    let err = match open_at(&link, DatabaseOpts::new()) {
        Ok(_) => panic!("opened past a WAL under another name"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("does not exist"), "the rename is not conditioned on absence: {err}");
    assert!(err.contains("even empty"), "a 0-byte canonical file could read as untouched: {err}");
    // Review 8 F1 (lead's decision): absence is evidence only against THIS build's opens. A build
    // that names the WAL from the path as given (earlier builds, upstream Turso) can open the
    // database under a third name, write newer pages and leave {ours} absent; so the rename also
    // needs that nothing else has opened it since.
    // Review 9 (lead's decision 2; PREREG A4): "under any other name" left out an open by the REAL
    // name, which can also leave {ours} absent while it writes newer pages. The condition counts
    // every name, {real} included.
    assert!(
        err.contains("by any name"),
        "the rename is not conditioned on no open since by ANY name: {err}"
    );
    assert!(
        err.contains("real.db included"),
        "the real name is not counted among the names: {err}"
    );
    // Review 10 (lead's decision F8; PREREG A5): "no other build or program" left out THIS build's
    // own opens that never create {ours} — one with a custom WAL path writes newer pages into
    // {real} and leaves {ours} absent. So nothing at all may have opened the database since.
    assert!(
        err.contains("nothing has opened this database since"),
        "the condition still exempts this build's own opens: {err}"
    );
    // Review 11 (lead's decision, reversing F8's exemption; PREREG A6): an open of {real} by
    // SQLite goes through {real}-wal, which IS {ours}, and SQLite deletes it at its last close; so
    // "except through {ours}" re-admitted exactly the openers review 9 closed. No exemption.
    assert!(
        !err.contains("except through"),
        "the condition exempts opens through {{ours}}, which a program can delete after writing: \
         {err}"
    );
    // Review 12 finding 1 (PREREG A7): the same message serves the MVCC log, which a read-write
    // open by this build creates only while the database header says MVCC; condition 2's reason
    // must say so rather than claim every open creates {ours}.
    assert!(
        err.contains("in MVCC mode"),
        "condition 2's reason is not scoped to the MVCC log's mode: {err}"
    );
}

/// Review 7 item 2 (lead's decision). A registry hit must not hand a read-write open an instance
/// opened with ANOTHER default lease: a caller whose branches must never expire would receive one
/// whose forks are leased and reaped when the lease runs out, silently (and the reverse leaks
/// branches meant to be temporary).
#[test]
fn a_registry_hit_of_another_lease_setting_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("leased.db");
    let leased = open_at(
        &path,
        DatabaseOpts::new().with_branch_lease(Some(Duration::from_secs(60))),
    )
    .unwrap();
    seed(&leased.connect().unwrap(), 3);
    let err = match open_at(&path, DatabaseOpts::new()) {
        Ok(_) => panic!("an open with no default lease received the leased instance"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("lease"), "refused for another reason: {err}");
    drop(leased);
    open_at(&path, DatabaseOpts::new()).expect("with the leased handle closed, the open works");
}

/// Review 4 #8: a registry hit must not hand an open that asks for one checkpoint mode a catalog
/// instance running the other (an explicit Sharp would silently receive fuzzy checkpoints). Both
/// directions, and the open works once the other handle is closed.
#[test]
fn a_registry_hit_of_another_checkpoint_mode_is_refused() {
    use crate::branch::BranchCheckpoint;
    let catalog = || {
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync: crate::branch::SyncClass::Fsync })
    };
    for (held, asked) in [(BranchCheckpoint::Fuzzy, BranchCheckpoint::Sharp), (BranchCheckpoint::Sharp, BranchCheckpoint::Fuzzy)] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ckpt-mode.db");
        let first = open_at(&path, catalog().with_branch_checkpoint(held)).unwrap();
        seed(&first.connect().unwrap(), 3);
        let err = match open_at(&path, catalog().with_branch_checkpoint(asked)) {
            Ok(_) => panic!("an open asking for {asked:?} received the {held:?} instance"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("checkpoint"), "refused for another reason: {err}");
        drop(first);
        open_at(&path, catalog().with_branch_checkpoint(asked)).expect("with the other handle closed, the open works");
    }
}

/// Review 4 #8: `R11_CKPT` names the mode exactly ("fuzzy" or "sharp"); anything else refuses the
/// open, where it silently meant fuzzy. A guard restores the variable.
///
/// FLAGGED TEST EDIT (engine review 7 #13): the variable is process-wide (every open in the binary
/// reads it), so the test runs alone in a fresh process (`fork_driver::alone`).
///
/// FLAGGED TEST EDIT (engine review 7 #13's judge): no longer `cfg(unix)`; `fork_driver::alone`
/// needs only `std::process`, so the test runs on every target.
#[test]
fn an_unknown_checkpoint_mode_in_the_environment_refuses_the_open() {
    let Some(sentinel) = crate::branch::fork_driver::alone(
        "branch::durability_tests::an_unknown_checkpoint_mode_in_the_environment_refuses_the_open",
    ) else {
        return;
    };
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            match self.0.take() {
                Some(v) => std::env::set_var("R11_CKPT", v),
                None => std::env::remove_var("R11_CKPT"),
            }
        }
    }
    let _restore = Restore(std::env::var_os("R11_CKPT"));
    std::env::set_var("R11_CKPT", "Sharp");
    let dir = tempfile::TempDir::new().unwrap();
    let opened = open_at(
        &dir.path().join("ckpt-env.db"),
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync: crate::branch::SyncClass::Fsync }),
    );
    match opened {
        Ok(_) => panic!("R11_CKPT=Sharp opened (as fuzzy)"),
        Err(e) => assert!(matches!(e, LimboError::InvalidArgument(_)), "refused for another reason: {e}"),
    }
    crate::branch::fork_driver::finished(&sentinel);
}

/// Review 4 #8: S-12's refusal (the F7 splice arm and fuzzy checkpoints together) holds for a
/// durable store, and a volatile one, which checkpoints nothing, opens.
#[test]
fn the_splice_arm_refuses_fuzzy_checkpoints_on_a_durable_store_only() {
    use crate::branch::BranchCheckpoint;
    let dir = tempfile::TempDir::new().unwrap();
    let catalog = DatabaseOpts::new()
        .with_branch_durability(BranchDurability::Catalog { sync: crate::branch::SyncClass::Fsync })
        .with_branch_splice(true)
        .with_branch_checkpoint(BranchCheckpoint::Fuzzy);
    match open_at(&dir.path().join("splice-fuzzy.db"), catalog) {
        Ok(_) => panic!("a catalog store opened with the splice arm and fuzzy checkpoints"),
        Err(e) => assert!(e.to_string().contains("refused together"), "refused for another reason: {e}"),
    }
    let volatile = DatabaseOpts::new().with_branch_splice(true).with_branch_checkpoint(BranchCheckpoint::Fuzzy);
    open_at(&dir.path().join("splice-volatile.db"), volatile).expect("a volatile splice store with fuzzy asked opens");
}

/// Open through `Database::open_async`, the registry's second hit path, driving its IO loop.
fn open_async_at(path: &Path, opts: DatabaseOpts) -> Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let file = io.open_file(path.to_str().unwrap(), OpenFlags::Create, true)?;
    let options = crate::OpenOptions::new(Arc::new(SqliteDialect))
        .storage(Arc::new(crate::storage::database::DatabaseFile::new(file)))
        .db_opts(opts);
    let mut state = crate::OpenDbAsyncState::new();
    loop {
        match Database::open_async(&mut state, io.clone(), path.to_str().unwrap(), &options)? {
            crate::types::IOResult::Done(db) => return Ok(db),
            crate::types::IOResult::IO(completion) => completion.wait(&*io)?,
        }
    }
}

/// Review 7 item 5 (lead's decision): the async registry-hit path (`open_async`'s `Ready` arm)
/// makes the same branch-store check as `Database::open`'s. A guard: green since 4e45e59a7; it is
/// here to kill the mutant that deletes that call.
#[test]
fn an_async_registry_hit_of_another_branch_durability_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("volatile.db");
    let volatile = open_at(&path, DatabaseOpts::new()).unwrap();
    seed(&volatile.connect().unwrap(), 3);
    let err = match open_async_at(&path, durable()) {
        Ok(_) => panic!("an async durable open received the volatile instance"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("branch durability"), "refused for another reason: {err}");
}

// ---- Review 8 (lane_turso_review8.md @ 8448139) ----

/// Review 8 F2 (lead's decision). ATTACH opens with default options, so it can request neither a
/// lease nor a durability, and it cannot fork the attached database (`fork_branch` forks the
/// connection's MAIN database). So a registry hit on a database this process holds open with a
/// lease — or durable — must not refuse the ATTACH.
///
/// Review 9 (lead's decisions 1 and 4; PREREG A4). Main opens with ATTACH enabled, as every other
/// ATTACH test in the tree does: without it, translation refuses the statement before any registry
/// lookup, and this test was red for that reason at both review-8 commits. Before each ATTACH, a
/// plain open of the same path with the same default options must be REFUSED. That is the negative
/// control: it proves the ATTACH is a registry hit (a registry cleared in between would make it a
/// miss, which succeeds without exercising the exemption), and that the exemption is ATTACH's
/// alone. Both ATTACHes run before the verdict, so a red names every refused one.
///
/// Review 10 (lead's decisions F2 and F4; PREREG A5). The control matches each registry refusal's
/// hit-specific phrase, "is open in this process with …": a registry MISS of a database with
/// durable branch files is refused by `BranchStore::open` with text that also says "branch
/// durability". And after each ATTACH, the attached instance must BE the held one (`Arc::ptr_eq`),
/// which proves this ATTACH's own lookup hit, not only the lookup just before it.
#[test]
fn attach_of_a_database_held_open_with_a_lease_or_durable_is_not_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let main = open_at(
        &dir.path().join("main.db"),
        DatabaseOpts::new().with_attach(true),
    )
    .unwrap();
    let conn = main.connect().unwrap();
    let mut refused = Vec::new();
    for (name, opts, plain_refusal) in [
        (
            "leased.db",
            DatabaseOpts::new().with_branch_lease(Some(Duration::from_secs(60))),
            "is open in this process with default branch lease",
        ),
        (
            "durable.db",
            durable(),
            "is open in this process with branch durability",
        ),
    ] {
        let path = dir.path().join(name);
        let held = open_at(&path, opts).unwrap();
        seed(&held.connect().unwrap(), 3);
        match open_at(&path, DatabaseOpts::new()) {
            Ok(_) => panic!(
                "premise: a plain open of {name} was not refused, so the ATTACH below would not \
                 be a registry hit"
            ),
            Err(e) => assert!(
                e.to_string().contains(plain_refusal),
                "premise: a plain open of {name} was refused for another reason: {e}"
            ),
        }
        let alias = name.trim_end_matches(".db");
        match conn.execute(format!("ATTACH '{}' AS {alias}", path.display())) {
            Ok(_) => {
                let id = conn.get_database_id_by_name(alias).unwrap();
                assert!(
                    Arc::ptr_eq(&conn.get_source_database(id), &held),
                    "premise: the ATTACH of {name} received another instance, not the registry's"
                );
                let found = rows(&conn, &format!("SELECT v FROM {alias}.t WHERE id = 2"));
                assert_eq!(
                    found,
                    vec![vec![Value::from_text(original(2))]],
                    "{name}: attached, but read the wrong rows"
                );
            }
            Err(e) => refused.push(format!("{name}: {e}")),
        }
        drop(held);
    }
    assert!(
        refused.is_empty(),
        "ATTACH of a database held open in this process was refused: {}",
        refused.join(" | ")
    );
}
// ---- r11-ever: the F7 durable port on the composed base (UNBUILT; r11-ever amendment 14) ----
//
// r11-ever-refute item 5: this store kept every released branch that still had a live child, so a
// workload that forks from its newest branch and releases its oldest (r11-restart's E5) kept every
// branch it ever created: as snapshot entries, catalog rows and children-index entries. The port
// splices a released branch with exactly one live child out of the tree. Every test here runs in
// snapshot mode, and in catalog mode under R11_BRANCH_CATALOG, like the rest of this file. The
// splice is an ARM, off by default (the lead's ruling, amendment 15): the tests that assert a splice
// open in it explicitly (`spliced()`); the switch's own tests name both arms. (Amendment 18: no env
// variable moves this file's arm: a test that opens a second store through `BranchStore::open`, in the
// default arm, would meet files the other arm wrote.)

fn catalog_run() -> bool {
    std::env::var_os("R11_BRANCH_CATALOG").is_some()
}

/// `durable()` in the F7 splice arm.
fn spliced() -> DatabaseOpts {
    durable().with_branch_splice(true)
}

/// `reopen`, in the arm `opts` names: a store opens only in the arm its files were written in.
fn reopen_in(path: &Path, previous_incarnation: u64, opts: DatabaseOpts) -> Arc<Database> {
    let db = open_at(path, opts).expect("reopen");
    assert_ne!(
        db.incarnation, previous_incarnation,
        "the registry returned the old Database: this is not a reopen"
    );
    db
}

/// The log records a recovery of the (closed) store at `path`, written in the splice arm, would
/// replay, in either mode.
fn log_records(path: &Path) -> Vec<journal::Record> {
    let files = journal::BranchFiles::for_db(path.to_str().unwrap());
    let format = journal::format_version(true);
    let recovered = if catalog_run() {
        let meta = catalog::Catalog::open(&files.cat, crate::branch::SyncClass::Off).unwrap().meta().unwrap();
        journal::Journal::recover_catalog_as(
            &files,
            crate::branch::SyncClass::Off,
            meta.map(|m| (m.page_size, m.generation)),
            format,
        )
    } else {
        journal::Journal::recover_as(&files, crate::branch::SyncClass::Off, format)
    };
    recovered.unwrap().expect("the store has files").records
}

/// What the last snapshot or catalog checkpoint of the (closed) store at `path`, written in the
/// splice arm, holds for `id`: `Some(held)`, held meaning released while a connection held it open,
/// or `None` without it.
fn checkpointed_hold(path: &Path, id: BranchId) -> Option<bool> {
    let files = journal::BranchFiles::for_db(path.to_str().unwrap());
    if catalog_run() {
        let b = catalog::Catalog::open(&files.cat, crate::branch::SyncClass::Off).unwrap().load_branch(id.0).unwrap();
        return b.map(|b| b.released && b.held_open);
    }
    let snapshot = journal::Journal::recover_as(&files, crate::branch::SyncClass::Off, journal::format_version(true))
        .unwrap()
        .unwrap()
        .snapshot?;
    snapshot
        .branches
        .into_iter()
        .find(|b| b.id == id.0)
        .map(|b| b.released && b.held_open)
}

/// Reads every live branch against its expected view, keeping every handle (a dropped handle is a
/// release, so each one is detached again).
fn check_views(db: &Arc<Database>, expect: &BTreeMap<BranchId, BTreeMap<i64, String>>, what: &str) {
    for (&id, view) in expect {
        let b = db.branch(id).unwrap();
        let c = b.connect().unwrap();
        for (&row, v) in view {
            assert_eq!(value(&c, row).as_ref(), Some(v), "{what}: branch {id:?} row {row}");
        }
        drop(c);
        let _ = b.into_id();
    }
}

/// E5's shape, eleven releases deep: after every step the kept states are exactly the live ones,
/// every live branch reads its own view (its ancestors' writes as of each fork, then its own, and
/// the trunk as of the first fork), and a replay-only restart and then a snapshot restart land on
/// the same slots and the same reads. Each parent also writes once after its child's fork (a
/// version its release must free before the splice), and rows repeat every four steps (versions
/// the splice finds shadowed by the child's own).
#[test]
fn a_chain_that_releases_its_oldest_keeps_only_its_live_branches() {
    const LIVE: usize = 3;
    const STEPS: usize = 14;
    let rows = [7i64, 60, 110, 170];
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let slots;
    let mut expect: BTreeMap<BranchId, BTreeMap<i64, String>> = BTreeMap::new();
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let mut chain: std::collections::VecDeque<Branch> = Default::default();
        // What the newest branch reads once it has written: every row it or an ancestor wrote
        // before the fork below it, and the trunk as of the first fork for the rest.
        let mut view: BTreeMap<i64, String> = [7i64, 60, 110, 130, 170]
            .into_iter()
            .map(|row| (row, original(row)))
            .collect();
        for i in 0..STEPS {
            let b = match chain.back() {
                None => trunk.fork_branch().unwrap(),
                Some(newest) => newest.fork().unwrap(),
            };
            if i == 0 {
                // A trunk write after the chain's first fork, on a leaf no branch writes (rows
                // 112-148; the branches write 7, 60, 110 and 170, on leaves 1, 2, 3 and 5): every
                // branch reads it through the relinked head of the chain, and must keep reading
                // the pre-image through each splice (a relink at the wrong fork epoch reads this).
                set(&trunk, 130, "trunk-late");
            }
            let row = rows[i % rows.len()];
            set(&b.connect().unwrap(), row, &format!("s{i}"));
            view.insert(row, format!("s{i}"));
            expect.insert(b.id(), view.clone());
            if let Some(parent) = chain.back() {
                let late = rows[(i + 1) % rows.len()];
                set(&parent.connect().unwrap(), late, &format!("late{i}"));
                expect.get_mut(&parent.id()).unwrap().insert(late, format!("late{i}"));
            }
            chain.push_back(b);
            if chain.len() > LIVE {
                let oldest = chain.pop_front().unwrap();
                expect.remove(&oldest.id());
                let reaped = oldest.reap().unwrap();
                assert!(reaped.deferred, "step {i}: an interior with a live child was freed whole");
            }
            assert_eq!(
                db.branch_stats().unwrap().live_branches,
                chain.len(),
                "step {i}: a released branch with one kept child was kept"
            );
            for b in &chain {
                let c = b.connect().unwrap();
                for (&row, v) in &expect[&b.id()] {
                    assert_eq!(value(&c, row).as_ref(), Some(v), "step {i}: branch {:?} row {row}", b.id());
                }
            }
        }
        assert_eq!(expect.len(), LIVE);
        for b in &chain {
            let c = b.connect().unwrap();
            for (&row, v) in &expect[&b.id()] {
                assert_eq!(value(&c, row).as_ref(), Some(v), "branch {:?} row {row}", b.id());
            }
        }
        assert_eq!(value(&trunk, 130), Some("trunk-late".to_string()));
        slots = in_use(&db);
        for b in chain {
            let _ = b.into_id();
        }
    }

    // Replay only (the log is far below the compaction threshold): the same splices at the same
    // points, so the same slots.
    let db = reopen_in(&path, incarnation, spliced());
    let ids: Vec<BranchId> = expect.keys().copied().collect();
    assert_eq!(db.branch_ids().unwrap(), ids, "replay kept or lost a branch");
    assert_eq!(db.branch_stats().unwrap().live_branches, LIVE, "replay kept a spliced branch");
    assert_eq!(in_use(&db), slots, "replay spliced to a different set of slots");
    check_views(&db, &expect, "after replay");

    // A snapshot of the spliced state, then a restart from it.
    db.branch_compact_now().unwrap();
    let incarnation = db.incarnation;
    drop(db);
    let db = reopen_in(&path, incarnation, spliced());
    assert_eq!(db.branch_stats().unwrap().live_branches, LIVE, "the snapshot kept a spliced branch");
    assert_eq!(in_use(&db), slots, "the snapshot restart landed on other slots");
    check_views(&db, &expect, "after the snapshot");
}

/// A branch released while its connection is open is held (`ReleaseOpen`) until that close, which
/// is logged (`Close`) and then retires and splices it: the version born after its child's fork is
/// freed, the version its child has overwritten is freed (the child has no child of its own to
/// read it), and the one its child reads moves into the child. A snapshot taken inside the window
/// records the hold (`held_open`), the log after it carries the `Close`, and a crash image taken
/// inside the window, with no `Close` in it, reaches the same state at the end of recovery.
#[test]
fn a_branch_released_under_its_open_connection_is_spliced_at_the_close() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let p_id;
    let c_id;
    let p7;
    let p60;
    let p110;
    let c60;
    let slots;
    let image_before;
    let image_after;
    for sub in ["before", "after"] {
        std::fs::create_dir(dir.path().join(sub)).unwrap();
    }
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        let pc = p.connect().unwrap();
        let s0 = in_use(&db);
        set(&pc, 7, "p-pre");
        let s1 = in_use(&db);
        set(&pc, 60, "p-pre");
        let s2 = in_use(&db);
        let c = p.fork().unwrap();
        set(&c.connect().unwrap(), 60, "c");
        let s3 = in_use(&db);
        set(&pc, 110, "p-post");
        let s4 = in_use(&db);
        let diff = |a: &BTreeSet<u32>, b: &BTreeSet<u32>| -> BTreeSet<u32> {
            b.difference(a).copied().collect()
        };
        p7 = diff(&s0, &s1);
        p60 = diff(&s1, &s2);
        c60 = diff(&s2, &s3);
        p110 = diff(&s3, &s4);
        // The premise: an inherited version, a shadowed one, and one born after the fork, each on
        // its own slots.
        for (name, written) in [("p7", &p7), ("p60", &p60), ("c60", &c60), ("p110", &p110)] {
            assert!(!written.is_empty(), "{name}: the write took no slot of its own");
        }
        assert_eq!(
            s4.len(),
            s0.len() + p7.len() + p60.len() + c60.len() + p110.len(),
            "a write freed a slot, so the four sets above are not what each write added"
        );
        p_id = p.id();
        drop(p); // released while pc is open: held
        assert_eq!(db.branch_stats().unwrap().live_branches, 2, "the open connection did not hold it");
        // The child reads through the held branch (r11-ever-refute coverage caveat iii).
        assert_eq!(value(&c.connect().unwrap(), 7), Some("p-pre".to_string()));
        assert_eq!(value(&pc, 110), Some("p-post".to_string()), "the held branch lost its own write");
        image_before = crash_image(&path, &dir.path().join("before"));
        db.branch_compact_now().unwrap(); // a snapshot inside the window
        image_after = crash_image(&path, &dir.path().join("after"));
        drop(pc); // the close: logged, then retire and splice
        assert_eq!(db.branch_stats().unwrap().live_branches, 1, "the close did not splice it");
        for slot in p110.iter().chain(p60.iter()) {
            assert!(db.branch_slot_is_free(*slot), "slot {slot}: nobody reads it, still held");
        }
        for slot in p7.iter().chain(c60.iter()) {
            assert!(!db.branch_slot_is_free(*slot), "slot {slot}: the child reads it, freed");
        }
        let cc = c.connect().unwrap();
        assert_eq!(value(&cc, 7), Some("p-pre".to_string()));
        assert_eq!(value(&cc, 60), Some("c".to_string()));
        assert_eq!(value(&cc, 110), Some(original(110)));
        drop(cc);
        slots = in_use(&db);
        c_id = c.into_id();
    }

    // What the files say: the release under the open connection, the hold in the checkpoint (a
    // snapshot's held_open, a catalog row's released = 2), and the close.
    {
        let before = log_records(&image_before);
        assert!(
            before.contains(&journal::Record::ReleaseOpen { branch: p_id.0 }),
            "the release of a branch held open was logged as a plain Release: {before:?}"
        );
        assert_eq!(checkpointed_hold(&path, p_id), Some(true), "the checkpoint does not record the hold");
        let after = log_records(&path);
        assert!(
            after.contains(&journal::Record::Close { branch: p_id.0 }),
            "the close of a held branch was not logged: {after:?}"
        );
    }

    let expect = BTreeMap::from([(
        c_id,
        BTreeMap::from([
            (7, "p-pre".to_string()),
            (60, "c".to_string()),
            (110, original(110)),
        ]),
    )]);
    let db = reopen_in(&path, incarnation, spliced());
    assert_eq!(db.branch_ids().unwrap(), vec![c_id]);
    assert_eq!(db.branch_stats().unwrap().live_branches, 1, "replay kept the spliced branch");
    assert_eq!(in_use(&db), slots, "replay of the Close landed on other slots");
    check_views(&db, &expect, "after the snapshot and the Close");
    drop(db);
    for image in [&image_before, &image_after] {
        let crashed = open_at(image, spliced()).unwrap();
        assert_eq!(
            crashed.branch_stats().unwrap().live_branches,
            1,
            "{image:?}: recovery kept a released branch nothing holds any more"
        );
        assert_eq!(in_use(&crashed), slots, "{image:?}: recovery spliced to other slots");
        check_views(&crashed, &expect, &format!("{image:?}"));
    }
}

/// Epoch inheritance is load-bearing for the splice: the configuration of the volatile store's
/// mutant M6 (a child's epochs starting at 0), killed there at F7'. The parent writes after three
/// forks, so its version is born at its fourth epoch; its child, forked right after, forks a
/// grandchild. With inheritance the grandchild's fork epoch lies above the version's birth and it
/// reads the parent's write once the parent is spliced into its child; with epochs starting at 0 it
/// lies below, and the grandchild would read the trunk's row instead.
#[test]
fn a_grandchild_reads_a_spliced_parents_write() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let expect;
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        let siblings: Vec<Branch> = (0..3).map(|_| p.fork().unwrap()).collect();
        set(&p.connect().unwrap(), 7, "p-after-three-forks");
        let c = p.fork().unwrap();
        let g = c.fork().unwrap();
        drop(siblings); // leaves, freed whole: p is left with one kept child
        let reaped = p.reap().unwrap();
        assert!(reaped.deferred && reaped.freed_pages == 0, "{reaped:?}");
        assert_eq!(db.branch_stats().unwrap().live_branches, 2, "p was not spliced");
        let want = Some("p-after-three-forks".to_string());
        assert_eq!(value(&g.connect().unwrap(), 7), want, "the grandchild lost the spliced write");
        assert_eq!(value(&c.connect().unwrap(), 7), want);
        let row = BTreeMap::from([(7, "p-after-three-forks".to_string())]);
        expect = BTreeMap::from([(c.into_id(), row.clone()), (g.into_id(), row)]);
    }
    let db = reopen_in(&path, incarnation, spliced());
    assert_eq!(db.branch_stats().unwrap().live_branches, 2);
    check_views(&db, &expect, "after replay");
    // The page maps a replay builds come from each fork, as live; a snapshot or catalog checkpoint
    // makes the next open DERIVE them from the lineages (`derive_page_maps`, `insert_loaded`), from
    // the born epochs: that is where epochs starting at 0 would send the grandchild to the trunk
    // (review of c47df7e64, finding 3).
    db.branch_compact_now().unwrap();
    let incarnation = db.incarnation;
    drop(db);
    let db = reopen_in(&path, incarnation, spliced());
    assert_eq!(db.branch_stats().unwrap().live_branches, 2);
    check_views(&db, &expect, "after a checkpoint and a reopen");
}

/// The splice's stream direction (the zombie's side is the smaller) and the shadow rule with a
/// retained version in the child: C overwrites the page it inherited from P, forks D, and
/// overwrites it again, so C holds that page twice (current, and the version D reads). When P is
/// spliced into C, P's version of the page is read by nobody: C's children all forked after C's
/// FIRST own version of it, which is retained, not current. It is freed, D reads C's first version,
/// C its second, and a restart agrees.
#[test]
fn a_splice_frees_a_version_the_child_shadowed_before_its_own_child_forked() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let p7;
    let slots;
    let expect;
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        let s0 = in_use(&db);
        set(&p.connect().unwrap(), 7, "p");
        p7 = in_use(&db).difference(&s0).copied().collect::<BTreeSet<u32>>();
        assert!(!p7.is_empty(), "p's write took no slot of its own");
        let c = p.fork().unwrap();
        set(&c.connect().unwrap(), 7, "c-first");
        let d = c.fork().unwrap();
        set(&c.connect().unwrap(), 7, "c-second");
        let work = db.branch_stats().unwrap().work;
        let reaped = p.reap().unwrap();
        assert!(reaped.deferred, "{reaped:?}");
        let after = db.branch_stats().unwrap().work;
        assert_eq!(after.splices, work.splices + 1);
        assert_eq!(after.splice_commits, work.splice_commits, "premise: the splice took the commit direction");
        assert_eq!(db.branch_stats().unwrap().live_branches, 2, "p was not spliced");
        for slot in &p7 {
            assert!(db.branch_slot_is_free(*slot), "slot {slot}: shadowed for every reader, still held");
        }
        assert_eq!(value(&d.connect().unwrap(), 7), Some("c-first".to_string()));
        assert_eq!(value(&c.connect().unwrap(), 7), Some("c-second".to_string()));
        slots = in_use(&db);
        expect = BTreeMap::from([
            (c.into_id(), BTreeMap::from([(7, "c-second".to_string())])),
            (d.into_id(), BTreeMap::from([(7, "c-first".to_string())])),
        ]);
    }
    let db = reopen_in(&path, incarnation, spliced());
    assert_eq!(db.branch_stats().unwrap().live_branches, 2);
    assert_eq!(in_use(&db), slots, "replay of the splice landed on other slots");
    check_views(&db, &expect, "after replay");
}

/// Review finding 3 (of a84bad66f): the end of recovery collects every branch a crash left held,
/// outside any record of the log it replays, so it logs each of those `Close`s itself. Session 1
/// releases p under its open connection (p has one child, c) and crashes (an image taken inside the
/// window); session 2 opens the image, whose recovery splices p into c, then works on c (a commit, a
/// fork, and a release that splices c into its child d); session 3 must land on session 2's state,
/// replaying session 2's records on the tree they were made on. The image's log shows the
/// recovery's `Close` of p ahead of every record session 2 made.
#[test]
fn a_recovery_logs_the_close_of_a_branch_a_crash_left_held() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    std::fs::create_dir(dir.path().join("img")).unwrap();
    let image;
    let p_id;
    {
        let db = open_at(&path, spliced()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        let pc = p.connect().unwrap();
        set(&pc, 7, "p");
        let c = p.fork().unwrap();
        p_id = p.id();
        drop(p); // released, held by pc
        image = crash_image(&path, &dir.path().join("img"));
        let _ = c.into_id();
        drop(pc);
    }
    let c_id;
    let d_id;
    let slots;
    let incarnation;
    {
        let db = open_at(&image, spliced()).unwrap(); // session 2: recovery splices p into c
        incarnation = db.incarnation;
        assert_eq!(db.branch_stats().unwrap().live_branches, 1, "recovery kept the held branch");
        let ids = db.branch_ids().unwrap();
        assert_eq!(ids.len(), 1, "{ids:?}");
        c_id = ids[0];
        let c = db.branch(c_id).unwrap();
        set(&c.connect().unwrap(), 60, "c");
        let d = c.fork().unwrap();
        d_id = d.id();
        set(&d.connect().unwrap(), 110, "d");
        let reaped = c.reap().unwrap(); // one kept child: spliced into d
        assert!(reaped.deferred, "{reaped:?}");
        assert_eq!(db.branch_stats().unwrap().live_branches, 1, "c was not spliced into d");
        let dc = d.connect().unwrap();
        assert_eq!(value(&dc, 7), Some("p".to_string()));
        assert_eq!(value(&dc, 60), Some("c".to_string()));
        assert_eq!(value(&dc, 110), Some("d".to_string()));
        drop(dc);
        slots = in_use(&db);
        let _ = d.into_id();
    }
    {
        let records = log_records(&image);
        let at = |want: &journal::Record| records.iter().position(|r| r == want);
        let released = at(&journal::Record::ReleaseOpen { branch: p_id.0 });
        let closed = at(&journal::Record::Close { branch: p_id.0 });
        // Session 2's first record: c's commit (c made none in session 1).
        let first_of_session_2 = records
            .iter()
            .position(|r| matches!(r, journal::Record::Commit { branch, .. } if *branch == c_id.0));
        assert!(
            released.is_some()
                && closed > released
                && first_of_session_2.is_some()
                && closed < first_of_session_2,
            "the recovery's Close of p is missing or not ahead of session 2's records: {records:?}"
        );
        // Session 3's state checks below cannot tell a missing Close from a present one on this
        // schedule (the splice commutes with session 2's operations here); this order check can.
    }
    let db = reopen_in(&image, incarnation, spliced()); // session 3
    assert_eq!(db.branch_ids().unwrap(), vec![d_id]);
    assert_eq!(db.branch_stats().unwrap().live_branches, 1, "replay kept a spliced branch");
    assert_eq!(in_use(&db), slots, "session 3 replayed session 2 onto another tree");
    let expect = BTreeMap::from([(
        d_id,
        BTreeMap::from([
            (7, "p".to_string()),
            (60, "c".to_string()),
            (110, "d".to_string()),
        ]),
    )]);
    check_views(&db, &expect, "session 3");
}

/// U5 (r11-invariant-matrix): F1's per-page map took retained versions only in `born` order, and a
/// splice retains a version OLDER than the child's own. Z writes a page and forks C; C forks D1,
/// writes the page, forks D2 and writes it again, so C holds the page twice (current, and retained
/// for D2) while D1, forked before C's first write, still reads Z's version. Z's release splices it
/// into C, which must keep Z's version below its own: the two-sided check, where the append-only one
/// fired a turso_assert (on in release builds too) and, since replay repeats the splice, left the
/// store unable to reopen. D1's reap then frees it: only D1 read it.
#[test]
fn a_splice_keeps_the_zombies_version_below_the_childs_own() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let z7;
    let slots;
    let expect;
    let d1_id;
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let z = trunk.fork_branch().unwrap();
        let s0 = in_use(&db);
        set(&z.connect().unwrap(), 7, "z");
        z7 = in_use(&db).difference(&s0).copied().collect::<BTreeSet<u32>>();
        assert!(!z7.is_empty(), "z's write took no slot of its own");
        let c = z.fork().unwrap();
        let d1 = c.fork().unwrap();
        set(&c.connect().unwrap(), 7, "c1");
        let d2 = c.fork().unwrap();
        set(&c.connect().unwrap(), 7, "c2");
        let reaped = z.reap().unwrap();
        assert!(reaped.deferred && reaped.freed_pages == 0, "{reaped:?}");
        assert_eq!(db.branch_stats().unwrap().live_branches, 3, "z was not spliced");
        for slot in &z7 {
            assert!(!db.branch_slot_is_free(*slot), "slot {slot}: d1 reads it, freed");
        }
        assert_eq!(value(&d1.connect().unwrap(), 7), Some("z".to_string()));
        assert_eq!(value(&d2.connect().unwrap(), 7), Some("c1".to_string()));
        assert_eq!(value(&c.connect().unwrap(), 7), Some("c2".to_string()));
        slots = in_use(&db);
        d1_id = d1.id();
        expect = BTreeMap::from([
            (c.into_id(), BTreeMap::from([(7, "c2".to_string())])),
            (d1.into_id(), BTreeMap::from([(7, "z".to_string())])),
            (d2.into_id(), BTreeMap::from([(7, "c1".to_string())])),
        ]);
    }
    let db = reopen_in(&path, incarnation, spliced()); // replay repeats the splice
    assert_eq!(db.branch_stats().unwrap().live_branches, 3);
    assert_eq!(in_use(&db), slots, "replay of the splice landed on other slots");
    check_views(&db, &expect, "after replay");
    let reaped = db.branch(d1_id).unwrap().reap().unwrap();
    assert!(!reaped.deferred, "{reaped:?}");
    for slot in &z7 {
        assert!(db.branch_slot_is_free(*slot), "slot {slot}: its only reader is gone, still held");
    }
}

/// r11-ever amendment 15 (the lead's ruling): the splice is an ARM, off by default. P writes row 7
/// and forks C, and P is released: off, P is kept (retired) as on the base, with no splice, and a
/// reopen keeps it; on, P is spliced into C. Either way the reap is deferred and C reads P's write.
/// The on arm is the control that the off arm's counts can tell the two apart.
#[test]
fn the_splice_arm_is_off_by_default_and_splices_only_when_on() {
    assert!(!DatabaseOpts::new().branch_splice, "the splice arm is on by default");
    for (splice, live, splices) in [(false, 2, 0), (true, 1, 1)] {
        let opts = durable().with_branch_splice(splice);
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("durable.db");
        let (incarnation, c_id);
        {
            let db = open_at(&path, opts).unwrap();
            incarnation = db.incarnation;
            let trunk = db.connect().unwrap();
            seed(&trunk, 200);
            let p = trunk.fork_branch().unwrap();
            set(&p.connect().unwrap(), 7, "p");
            let c = p.fork().unwrap();
            let reaped = p.reap().unwrap();
            assert!(reaped.deferred, "splice={splice}: {reaped:?}");
            let s = db.branch_stats().unwrap();
            assert_eq!(s.live_branches, live, "splice={splice}: {s:?}");
            assert_eq!(s.work.splices, splices, "splice={splice}: {s:?}");
            assert_eq!(value(&c.connect().unwrap(), 7).as_deref(), Some("p"), "splice={splice}");
            c_id = c.into_id();
        }
        let db = reopen_in(&path, incarnation, opts);
        assert_eq!(db.branch_stats().unwrap().live_branches, live, "splice={splice}: after the reopen");
        let c = db.branch(c_id).unwrap();
        assert_eq!(value(&c.connect().unwrap(), 7).as_deref(), Some("p"), "splice={splice}: reopened");
        let _ = c.into_id();
    }
}

/// r11-ever amendment 15: replay repeats every splice, so a store's files open only in the arm they
/// were written in (the log's and snapshot's format version: 3 off, 4 on). The other arm is an error
/// that names the splice arm, never a replay under the other rule: both directions, from the log
/// alone and after a checkpoint (a snapshot, or a catalog checkpoint whose new log header carries
/// the version). The refused open changes nothing: the store's own arm then opens it.
#[test]
fn a_store_opens_only_in_the_splice_arm_its_files_were_written_in() {
    for splice in [false, true] {
        for checkpoint in [false, true] {
            let what = format!("splice={splice} checkpoint={checkpoint}");
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("durable.db");
            let written = durable().with_branch_splice(splice);
            let b_id;
            {
                let db = open_at(&path, written).unwrap();
                let trunk = db.connect().unwrap();
                seed(&trunk, 50);
                let b = trunk.fork_branch().unwrap();
                set(&b.connect().unwrap(), 7, "b");
                b_id = b.into_id();
                if checkpoint {
                    db.branch_compact_now().unwrap();
                }
            }
            let err = match open_at(&path, durable().with_branch_splice(!splice)) {
                Ok(_) => panic!("{what}: the other arm opened the store"),
                Err(err) => err.to_string(),
            };
            assert!(err.contains("splice"), "{what}: refused for another reason: {err}");
            let db = open_at(&path, written).expect("its own arm opens it");
            assert_eq!(db.branch_ids().unwrap(), vec![b_id], "{what}");
            let b = db.branch(b_id).unwrap();
            assert_eq!(value(&b.connect().unwrap(), 7).as_deref(), Some("b"), "{what}");
            let _ = b.into_id();
        }
    }
}

/// r11-ever amendment 15: a registry hit must not hand an open the instance of the other splice arm,
/// whose releases would be collected by the other rule (review 7 item 2's rule for the lease, applied
/// to the new option). Volatile too: a volatile store collects in process.
#[test]
fn a_registry_hit_of_the_other_splice_arm_is_refused() {
    for durable_store in [false, true] {
        for splice in [false, true] {
            let what = format!("durable={durable_store} splice={splice}");
            let base = if durable_store { durable() } else { DatabaseOpts::new() };
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("held.db");
            let held = open_at(&path, base.with_branch_splice(splice)).unwrap();
            seed(&held.connect().unwrap(), 3);
            let err = match open_at(&path, base.with_branch_splice(!splice)) {
                Ok(_) => panic!("{what}: an open in the other arm received the held instance"),
                Err(err) => err.to_string(),
            };
            assert!(
                err.contains("is open in this process with the branch splice arm"),
                "{what}: refused for another reason: {err}"
            );
            open_at(&path, base.with_branch_splice(splice)).expect("the same arm receives it");
            drop(held);
        }
    }
}

/// The lead's ruling on r11-churn's batch test (amendment 15), on this base. The composed base has
/// no batch release (`reap_branches` is on the pre-F1 line only, r11-ever-durable 0905219cd), so
/// this is that test's splice-mode twin, one release at a time, run in both arms. P writes row 100
/// and forks C, which writes row 101 on the same leaf (the premise, asserted by a probe branch that
/// writes both rows into one slot). Off (the base's rule): P's reap is deferred and frees nothing,
/// and C's frees both pages. On: P's reap splices P into C and frees P's page, which C's own copy
/// shadows (deferred, 1), and C's frees C's (1). The totals match: a splice moves a free to an
/// earlier release and never adds or loses one.
#[test]
fn a_splice_frees_what_the_base_rule_frees_one_release_earlier() {
    let mut totals = Vec::new();
    for (splice, p_want, c_want) in [(false, (true, 0), (false, 2)), (true, (true, 1), (false, 1))] {
        let what = format!("splice={splice}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("durable.db");
        let db = open_at(&path, durable().with_branch_splice(splice)).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let probe = trunk.fork_branch().unwrap();
        {
            let q = probe.connect().unwrap();
            set(&q, 100, "q");
            set(&q, 101, "q");
        }
        assert_eq!(probe.owned_slots().len(), 1, "{what}: premise: rows 100 and 101 are on two leaves");
        let r = probe.reap().unwrap();
        assert!(!r.deferred && r.freed_pages == 1, "{what}: the probe's reap: {r:?}");
        let p = trunk.fork_branch().unwrap();
        set(&p.connect().unwrap(), 100, "p");
        let c = p.fork().unwrap();
        set(&c.connect().unwrap(), 101, "c");
        assert_eq!(in_use(&db).len(), 2, "{what}: premise: p and c own one page each");
        let rp = p.reap().unwrap();
        assert_eq!((rp.deferred, rp.freed_pages), p_want, "{what}: p's reap: {rp:?}");
        assert_eq!(value(&c.connect().unwrap(), 100).as_deref(), Some("p"), "{what}: c lost p's row");
        let rc = c.reap().unwrap();
        assert_eq!((rc.deferred, rc.freed_pages), c_want, "{what}: c's reap: {rc:?}");
        assert!(in_use(&db).is_empty(), "{what}: pages left in use: {:?}", in_use(&db));
        totals.push(rp.freed_pages + rc.freed_pages);
    }
    assert_eq!(totals[0], totals[1], "the two arms freed different totals: {totals:?}");
}

/// r11-adversarial's chainw (amendment 17), in the splice arm: at each of D levels the newest branch
/// writes a page no level wrote before, forks, and is released, so it is spliced into the new child,
/// which inherits every page the dead levels above had moved into it. Each level's first fork builds
/// its page map; from all of its current versions that build inserts the level's own page plus the
/// j - 1 moved in, D(D+1)/2 in all over the chain; from the versions born after `inherited_at`, the
/// one page its level wrote: exactly D. The last level reads every level's page, and so does a child
/// forked from it, in the store that built the maps and after a checkpoint and a reopen, which
/// re-derives them from the checkpoint (a snapshot load's `derive_page_maps`, or a catalog load's
/// `insert_loaded`: `inherited_at` then starts at the fork epoch, and the child's map must still name
/// every page). Without the checkpoint the reopen only replays the log (review 3, C2).
#[test]
fn a_chain_that_writes_forks_and_releases_builds_each_view_from_its_own_pages() {
    const D: i64 = 48;
    // One row per leaf page: ~3 KiB values on 4 KiB pages, rewritten at the same length.
    let val = |tag: &str, id: i64| format!("{id:010}{}", tag.repeat(2990));
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let tip_id;
    {
        let db = open_at(&path, spliced()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE w(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.execute("BEGIN").unwrap();
        for id in 1..=D + 1 {
            trunk.execute(format!("INSERT INTO w VALUES ({id}, '{}')", val("s", id))).unwrap();
        }
        trunk.execute("COMMIT").unwrap();
        let write = |b: &Branch, id: i64| {
            b.connect()
                .unwrap()
                .execute(format!("UPDATE w SET v = '{}' WHERE id = {id}", val("w", id)))
                .unwrap();
        };
        let read = |b: &Branch, id: i64| -> String {
            match &rows(&b.connect().unwrap(), &format!("SELECT v FROM w WHERE id = {id}"))[0][0] {
                Value::Text(t) => t.as_str().to_string(),
                other => panic!("expected text, got {other:?}"),
            }
        };
        let probe = trunk.fork_branch().unwrap();
        write(&probe, D + 1);
        assert_eq!(probe.owned_slots().len(), 1, "premise: a row's rewrite takes more than its leaf");
        assert!(!probe.reap().unwrap().deferred);
        let before = db.branch_stats().unwrap().work;
        let mut prev = trunk.fork_branch().unwrap();
        for j in 1..=D {
            write(&prev, j);
            let next = prev.fork().unwrap();
            let r = prev.reap().unwrap();
            assert!(r.deferred, "level {j}: the released level was not kept for its child: {r:?}");
            prev = next;
        }
        let w = db.branch_stats().unwrap().work;
        assert_eq!(w.splices - before.splices, D as u64, "premise: every level was spliced");
        assert_eq!(
            w.view_build_entries - before.view_build_entries,
            D as u64,
            "each level's first fork built its map from more than its own page"
        );
        assert_eq!(db.branch_stats().unwrap().live_branches, 1);
        for id in 1..=D {
            assert_eq!(read(&prev, id), val("w", id), "the tip misread level {id}'s page");
        }
        let child = prev.fork().unwrap();
        for id in 1..=D {
            assert_eq!(read(&child, id), val("w", id), "a child of the tip misread level {id}'s page");
        }
        assert_eq!(read(&child, D + 1), val("s", D + 1), "a child of the tip misread a trunk page");
        let _ = child.reap().unwrap();
        db.branch_compact_now().unwrap(); // the reopen loads the maps' state, not only a log
        tip_id = prev.into_id();
    }
    let db = reopen_in(&path, incarnation, spliced());
    let tip = db.branch(tip_id).unwrap();
    let child = tip.fork().unwrap();
    for id in 1..=D {
        let got = match &rows(&child.connect().unwrap(), &format!("SELECT v FROM w WHERE id = {id}"))[0][0] {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        assert_eq!(got, val("w", id), "after the reopen a child of the tip misread level {id}'s page");
    }
    let _ = child.reap().unwrap();
    let _ = tip.into_id();
}
