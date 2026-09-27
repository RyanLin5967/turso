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
        BranchDurability::Catalog { sync: true }
    } else {
        BranchDurability::Durable { sync: true }
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
    let durability = BranchDurability::Durable { sync: true };
    assert!(
        store::BranchStore::open(durability, None, path.to_str().unwrap()).is_err(),
        "a second store opened over a live store's branch log"
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
    let durability = BranchDurability::Durable { sync: true };
    let late = store::BranchStore::open(durability, None, path.to_str().unwrap()).unwrap();
    let db = open_at(&path, durable()).unwrap();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20);
    let b = trunk.fork_branch().unwrap();
    set(&b.connect().unwrap(), 3, "b");
    let schema = trunk.schema.read().clone();
    assert!(
        late.fork_trunk(schema, 4096).is_err(),
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
    let gone = tempfile::TempDir::new().unwrap();
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
    let nosync = DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: false });
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

/// r11-churn PREREG amendment 2 (group commit for reaps): `Database::reap_branches` makes every
/// Release in the batch durable with ONE flush — one log fsync on this thread, the arena being
/// clean — frees exactly what one release per branch would, in order (a chain's root deferred
/// behind its tip, which then frees both), and a restart comes back with only the branch kept.
#[test]
fn a_batch_release_is_one_flush_and_frees_what_single_releases_free() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let mut batch = Vec::new();
        for i in 0..6 {
            let b = trunk.fork_branch().unwrap();
            set(&b.connect().unwrap(), 10 + i, &format!("b{i}"));
            batch.push(b);
        }
        let p = trunk.fork_branch().unwrap();
        set(&p.connect().unwrap(), 100, "p");
        let c = p.fork().unwrap();
        set(&c.connect().unwrap(), 101, "c");
        batch.push(p);
        batch.push(c);
        let keep = trunk.fork_branch().unwrap();
        set(&keep.connect().unwrap(), 150, "keep");
        assert_eq!(in_use(&db).len(), 9);
        let fsyncs = crate::branch::churn_counters().thread_fsyncs;
        let reaped = db.reap_branches(batch).unwrap();
        assert_eq!(
            crate::branch::churn_counters().thread_fsyncs - fsyncs,
            1,
            "a batch release is one flush: one log fsync, the arena being clean"
        );
        assert!(reaped[..6].iter().all(|r| !r.deferred && r.freed_pages == 1));
        assert!(reaped[6].deferred && reaped[6].freed_pages == 0, "the chain root: {:?}", reaped[6]);
        assert!(!reaped[7].deferred && reaped[7].freed_pages == 2, "the chain tip: {:?}", reaped[7]);
        assert_eq!(db.branch_ids().unwrap(), vec![keep.id()]);
        assert_eq!(in_use(&db).len(), 1);
        let _kept = keep.into_id();
    }
    let db = reopen(&path, incarnation);
    let ids = db.branch_ids().unwrap();
    assert_eq!(ids.len(), 1, "the batch's releases did not all survive the restart: {ids:?}");
    let keep = db.branch(ids[0]).unwrap();
    assert_eq!(value(&keep.connect().unwrap(), 150).as_deref(), Some("keep"));
}

/// r11-churn PREREG amendment 2 (group commit for the expiry pass): the pass a fork runs rides on
/// the fork's own flush, so a fork that reaps an expired lease costs ONE log fsync, not two (the
/// pass's, then the fork's), and the reap is durable: a restart does not bring the branch back.
#[test]
fn an_expiry_pass_rides_on_the_forks_own_flush() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let a = trunk.fork_branch().unwrap();
        a.lease(Duration::from_secs(5)).unwrap();
        set(&a.connect().unwrap(), 20, "a");
        let a = a.into_id();
        db.branch_lease_clock_advance(Duration::from_secs(6));
        let fsyncs = crate::branch::churn_counters().thread_fsyncs;
        let b = trunk.fork_branch().unwrap();
        assert_eq!(
            crate::branch::churn_counters().thread_fsyncs - fsyncs,
            1,
            "the expiry pass took a flush of its own"
        );
        assert_eq!(db.branch_ids().unwrap(), vec![b.id()], "the fork's pass did not reap {a:?}");
        assert!(in_use(&db).is_empty());
        let _b = b.into_id();
    }
    let db = reopen(&path, incarnation);
    assert_eq!(db.branch_ids().unwrap().len(), 1, "the reaped branch came back");
}

/// r11-churn PREREG amendment 4 (group commit, flush outside the store mutex): eight agents fork,
/// write and release branches concurrently, so their records share flights. Every operation that
/// returned is durable — a restart brings back exactly the branches kept, each with its own row —
/// and no released branch comes back.
#[test]
fn concurrent_lifecycles_under_group_commit_survive_a_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let kept: std::sync::Mutex<Vec<(BranchId, i64, String)>> = std::sync::Mutex::new(Vec::new());
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        seed(&db.connect().unwrap(), 400);
        std::thread::scope(|s| {
            for t in 0..8i64 {
                let (db, kept) = (&db, &kept);
                s.spawn(move || {
                    let trunk = db.connect().unwrap();
                    let mut release = Vec::new();
                    for i in 0..20i64 {
                        let row = 1 + t * 40 + i;
                        let b = loop {
                            match trunk.fork_branch() {
                                Ok(b) => break b,
                                Err(crate::LimboError::Busy)
                                | Err(crate::LimboError::BusySnapshot) => {
                                    std::thread::yield_now()
                                }
                                Err(e) => panic!("fork: {e}"),
                            }
                        };
                        let v = format!("t{t}i{i}");
                        set(&b.connect().unwrap(), row, &v);
                        if i % 2 == 0 {
                            kept.lock().unwrap().push((b.into_id(), row, v));
                        } else {
                            release.push(b);
                        }
                        if release.len() == 5 {
                            db.reap_branches(std::mem::take(&mut release)).unwrap();
                        }
                    }
                    db.reap_branches(release).unwrap();
                });
            }
        });
        assert_eq!(db.branch_ids().unwrap().len(), 80);
    }
    let db = reopen(&path, incarnation);
    let kept = kept.into_inner().unwrap();
    let ids: BTreeSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
    assert_eq!(ids, kept.iter().map(|k| k.0).collect(), "the restart's branches");
    for (id, row, v) in kept {
        let b = db.branch(id).unwrap();
        let c = b.connect().unwrap();
        assert_eq!(value(&c, row), Some(v));
        assert_eq!(value(&c, 399), Some(original(399)));
        drop(c);
        let _ = b.into_id();
    }
}

/// r11-churn PREREG amendment 4: an early-released batch release whose flight fails reports the
/// failure, frees none of the slots its durable state still names (rule 2 under early release),
/// fail-stops the store, and a restart brings the branches back with their pages.
#[test]
fn an_early_released_release_whose_flight_fails_frees_nothing_now_or_later() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let (a_id, b_id);
    let held;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let a = trunk.fork_branch().unwrap();
        set(&a.connect().unwrap(), 10, "a");
        let b = trunk.fork_branch().unwrap();
        set(&b.connect().unwrap(), 20, "b");
        held = in_use(&db);
        assert_eq!(held.len(), 2);
        (a_id, b_id) = (a.id(), b.id());
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(
            db.reap_branches(vec![a, b]).is_err(),
            "a batch whose flight failed reported success"
        );
        assert_eq!(in_use(&db), held, "the failed batch freed slots its durable state names");
        assert!(trunk.fork_branch().is_err(), "a fork was accepted after a failed flight");
        assert_eq!(in_use(&db), held);
    }
    let db = reopen(&path, incarnation);
    let ids: BTreeSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
    assert_eq!(ids, BTreeSet::from([a_id, b_id]), "a release that never became durable held");
    let a = db.branch(a_id).unwrap();
    assert_eq!(value(&a.connect().unwrap(), 10), Some("a".to_string()));
    let _ = a.into_id();
}

/// Review r12-merge1 N1 (artie-research frontier/round11/r11-bigtxn/merge1_review.md, part 6). An early-released
/// batch release applies Release(c) and drops the trunk's child count before its flight lands, so a trunk write that
/// follows retains no pre-image for c. That trunk commit must not become durable ahead of Release(c): when the flight
/// fails, c comes back at the reopen and must still read the page as of its fork.
#[test]
fn a_trunk_commit_after_a_failed_batch_release_does_not_reach_the_branch() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let c_id;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        // The trunk's only child: it reads original(7).
        let c = trunk.fork_branch().unwrap();
        c_id = c.id();
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(db.reap_branches(vec![c]).is_err(), "a batch whose flight failed reported success");
        // Refused or not, it must not reach c.
        let _ = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
    }
    let db = reopen(&path, incarnation);
    let c = db.branch(c_id).expect("its Release never became durable, so the branch is back");
    assert_eq!(
        value(&c.connect().unwrap(), 7),
        Some(original(7)),
        "the branch reads a trunk write made after its fork"
    );
    let _ = c.into_id();
}

/// N1 through the other early release: the expiry pass that rides on a BRANCH fork. `p`, an older trunk child, keeps
/// the trunk's child count above zero, so the trunk's copy decision still runs, but it asks only for children forked
/// since the page's last write: `c` alone, which the failed fork's pass has already released in memory.
#[test]
fn a_trunk_commit_after_a_failed_fork_that_reaped_a_trunk_child_does_not_reach_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let c_id;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let p = trunk.fork_branch().unwrap();
        // Retained for p; the page's last trunk write is now after p's fork.
        set(&trunk, 7, "t1");
        let c = trunk.fork_branch().unwrap();
        assert_eq!(value(&c.connect().unwrap(), 7), Some("t1".to_string()), "premise: c forked after 't1'");
        c.lease(Duration::from_secs(10)).unwrap();
        c_id = c.into_id();
        db.branch_lease_clock_advance(Duration::from_secs(11));
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        // Its expiry pass reaps c, riding on this fork's flight, which fails.
        assert!(p.fork().is_err(), "a fork whose flight failed reported success");
        let _ = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
        let _ = p.into_id();
    }
    let db = reopen(&path, incarnation);
    let c = db.branch(c_id).expect("its Release never became durable, so the branch is back");
    assert_eq!(
        value(&c.connect().unwrap(), 7),
        Some("t1".to_string()),
        "the branch reads a trunk write made after its fork"
    );
    let _ = c.into_id();
}

/// Review r12-merge1 R2 (part 3). An expiry pass riding on a fork releases an OPEN branch early; its slots must not go
/// back to the arena at the connection's close before that Release is durable (rule 2). Here the fork's flight fails,
/// so the Release never becomes durable, and the close must free nothing.
#[test]
fn a_close_frees_nothing_of_a_branch_whose_early_release_is_not_durable() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let b_id;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let cb = b.connect().unwrap();
        set(&cb, 10, "b");
        b.lease(Duration::from_secs(10)).unwrap();
        let held = in_use(&db);
        let own: BTreeSet<u32> = b.owned_slots().into_iter().collect();
        assert!(!own.is_empty() && own.is_subset(&held), "premise: b owns slots in use");
        db.branch_lease_clock_advance(Duration::from_secs(11));
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        // Its expiry pass releases b, which is open and so kept, riding on this fork's flight, which fails.
        assert!(trunk.fork_branch().is_err(), "a fork whose flight failed reported success");
        drop(cb);
        assert_eq!(in_use(&db), held, "the close freed slots whose Release never became durable");
        b_id = b.into_id();
    }
    let db = reopen(&path, incarnation);
    let b = db.branch(b_id).expect("its Release never became durable, so the branch is back");
    assert_eq!(value(&b.connect().unwrap(), 10), Some("b".to_string()));
    let _ = b.into_id();
}

/// Review r12-merge1 R1 (part 2). A compaction that fails BEFORE its rename leaves the log, its buffer and the journal
/// intact, so it must not fail-stop the store: a later branch write is taken and survives a reopen.
#[test]
fn a_compaction_that_fails_before_its_rename_does_not_fail_stop_later_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let b_id;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let cb = b.connect().unwrap();
        set(&cb, 10, "before");
        // The temporary snapshot cannot be created: the compaction fails before its rename.
        let tmp = std::path::PathBuf::from(format!("{}-branch-snap.tmp", path.display()));
        std::fs::create_dir(&tmp).unwrap();
        assert!(db.branch_compact_now().is_err(), "the compaction did not fail");
        std::fs::remove_dir(&tmp).unwrap();
        let after = cb.execute("UPDATE t SET v = 'after' WHERE id = 11");
        assert!(
            after.is_ok(),
            "a write after a compaction that failed before its rename was refused: {after:?}"
        );
        drop(cb);
        b_id = b.into_id();
    }
    let db = reopen(&path, incarnation);
    let b = db.branch(b_id).unwrap();
    let cb = b.connect().unwrap();
    assert_eq!(value(&cb, 10), Some("before".to_string()));
    assert_eq!(value(&cb, 11), Some("after".to_string()));
    drop(cb);
    let _ = b.into_id();
}

/// The other half of R1, found by a fresh review of its fix (artie-research PREREG amendment 6a). A compaction can also
/// fail before its rename at its ARENA sync. A failed sync may have dropped the pages it was writing back, and a later
/// fsync of the same file can then report success without them, so that failure must fail-stop the store as a failed
/// log flush does: a later branch write is refused, never acknowledged.
#[test]
fn a_compaction_whose_arena_sync_fails_fail_stops_the_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("durable.db");
    let incarnation;
    let b_id;
    {
        let db = open_at(&path, durable()).unwrap();
        incarnation = db.incarnation;
        let trunk = db.connect().unwrap();
        seed(&trunk, 200);
        let b = trunk.fork_branch().unwrap();
        let cb = b.connect().unwrap();
        set(&cb, 10, "before");
        db.branch_failpoint(Some(BranchFailpoint::CompactArenaSyncFails));
        assert!(db.branch_compact_now().is_err(), "the failpoint did not fire");
        assert!(
            cb.execute("UPDATE t SET v = 'after' WHERE id = 11").is_err(),
            "a write was acknowledged after the compaction's arena sync failed"
        );
        drop(cb);
        b_id = b.into_id();
    }
    let db = reopen(&path, incarnation);
    let b = db.branch(b_id).unwrap();
    let cb = b.connect().unwrap();
    assert_eq!(value(&cb, 10), Some("before".to_string()));
    assert_eq!(value(&cb, 11), Some(original(11)));
    drop(cb);
    let _ = b.into_id();
}
