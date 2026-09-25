//! Tests for the paths the specification in `isolation_tests.rs` does not reach: retained-version
//! reclamation, schema changes on either side of a fork, rollback, page-number collisions between
//! a growing branch and a growing trunk, the refusals, and a model-checked random workload.
//!
//! As there, every claim about what a connection sees is read back through SQL, and every claim
//! about freed pages is checked by membership in the free list rather than by a net count.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::{BTreeMap, BTreeSet};

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    open_db_with(DatabaseOpts::new())
}

fn open_db_with(opts: DatabaseOpts) -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branching.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    (dir, db)
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
            let id = row[0].as_int().expect("integer id");
            let v = match &row[1] {
                Value::Text(t) => t.as_str().to_string(),
                other => panic!("expected text, got {other:?}"),
            };
            (id, v)
        })
        .collect()
}

fn table_names(conn: &Arc<Connection>) -> BTreeSet<String> {
    rows(conn, "SELECT name FROM sqlite_schema WHERE type = 'table'")
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("expected text, got {other:?}"),
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

#[test]
fn reaping_the_last_child_frees_what_the_trunk_retained_for_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let before = in_use(&db);

    let b = trunk.fork_branch().unwrap();
    for id in [1, 50, 100, 150, 200] {
        set(&trunk, id, "trunk-after-fork");
    }
    let retained = in_use(&db);
    // The trunk's writes had to keep pre-images for the branch; if they did not, the branch
    // would read them — and the isolation tests say it does not.
    assert!(
        retained.len() > before.len(),
        "the trunk overwrote pages a live branch can see and retained nothing"
    );
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 100), Some(original(100)));
    drop(bc);

    let reaped = b.reap().unwrap();
    assert!(!reaped.deferred);
    for slot in retained.difference(&before) {
        assert!(db.branch_slot_is_free(*slot), "retained slot {slot} leaked");
    }
    assert_eq!(in_use(&db), before);
}

#[test]
fn a_retained_version_lives_exactly_as_long_as_a_child_that_can_see_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let before = in_use(&db);

    let a = trunk.fork_branch().unwrap(); // sees v0
    set(&trunk, 7, "v1");
    let after_first = in_use(&db);
    let b = trunk.fork_branch().unwrap(); // sees v1
    set(&trunk, 7, "v2");
    let after_second = in_use(&db);
    let v0_slots: BTreeSet<u32> = after_first.difference(&before).copied().collect();
    let v1_slots: BTreeSet<u32> = after_second.difference(&after_first).copied().collect();
    assert!(!v0_slots.is_empty() && !v1_slots.is_empty());

    assert_eq!(value(&a.connect().unwrap(), 7), Some(original(7)));
    assert_eq!(value(&b.connect().unwrap(), 7), Some("v1".to_string()));
    assert_eq!(value(&trunk, 7), Some("v2".to_string()));

    // Only `a` could see v0. v1 is still `b`'s.
    drop(a);
    for slot in &v0_slots {
        assert!(db.branch_slot_is_free(*slot), "v0 outlived its only reader");
    }
    for slot in &v1_slots {
        assert!(!db.branch_slot_is_free(*slot), "v1 freed while b can still see it");
    }
    assert_eq!(value(&b.connect().unwrap(), 7), Some("v1".to_string()));

    drop(b);
    assert_eq!(in_use(&db), before);
}

#[test]
fn a_reaped_parent_is_kept_while_its_child_reads_through_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let before = in_use(&db);

    let b = trunk.fork_branch().unwrap();
    set(&b.connect().unwrap(), 7, "b");
    let c = b.fork().unwrap();
    let parent_slots = b.owned_slots();
    assert!(!parent_slots.is_empty());

    let reaped = b.reap().unwrap();
    assert!(reaped.deferred, "a parent with a live child was freed");
    assert_eq!(reaped.freed_pages, 0);
    for slot in &parent_slots {
        assert!(!db.branch_slot_is_free(*slot), "the child's view was freed under it");
    }
    let cc = c.connect().unwrap();
    assert_eq!(value(&cc, 7), Some("b".to_string()));
    set(&cc, 8, "c");
    drop(cc);

    drop(c);
    assert_eq!(in_use(&db), before, "the chain was not freed when its last child went");
    assert_eq!(db.branch_stats().live_branches, 0);
}

#[test]
fn a_trunk_schema_change_after_the_fork_is_invisible_to_the_branch() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let b = trunk.fork_branch().unwrap();

    trunk.execute("CREATE TABLE u(x)").unwrap();
    trunk.execute("INSERT INTO u VALUES (1)").unwrap();
    assert!(table_names(&trunk).contains("u"));

    let bc = b.connect().unwrap();
    assert!(
        !table_names(&bc).contains("u"),
        "the branch read the trunk's post-fork sqlite_schema"
    );
    assert!(bc.prepare("SELECT x FROM u").is_err(), "the branch adopted the trunk's schema");
    set(&bc, 3, "branch");
    assert_eq!(value(&bc, 3), Some("branch".to_string()));
    assert_eq!(value(&trunk, 3), Some(original(3)));
}

#[test]
fn a_branch_schema_change_is_private_to_the_branch() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let b = trunk.fork_branch().unwrap();
    let sibling = trunk.fork_branch().unwrap();

    let bc = b.connect().unwrap();
    bc.execute("CREATE TABLE bt(x)").unwrap();
    bc.execute("INSERT INTO bt VALUES (42)").unwrap();
    drop(bc);

    let bc = b.connect().unwrap();
    assert_eq!(rows(&bc, "SELECT x FROM bt")[0][0].as_int(), Some(42));
    let c = b.fork().unwrap();
    assert_eq!(
        rows(&c.connect().unwrap(), "SELECT x FROM bt")[0][0].as_int(),
        Some(42),
        "a child did not inherit its parent's schema"
    );

    for (who, conn) in [
        ("trunk", trunk.clone()),
        ("fresh trunk", db.connect().unwrap()),
        ("sibling", sibling.connect().unwrap()),
    ] {
        assert!(!table_names(&conn).contains("bt"), "{who} sees the branch's table");
        assert!(conn.prepare("SELECT x FROM bt").is_err(), "{who} can query the branch's table");
    }
}

#[test]
fn a_rolled_back_branch_transaction_leaves_the_branch_as_it_was() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 5, "committed");

    bc.execute("BEGIN").unwrap();
    set(&bc, 5, "rolled-back");
    set(&bc, 6, "rolled-back");
    bc.execute("CREATE TABLE gone(x)").unwrap();
    bc.execute("ROLLBACK").unwrap();

    assert_eq!(value(&bc, 5), Some("committed".to_string()));
    assert_eq!(value(&bc, 6), Some(original(6)));
    assert!(!table_names(&bc).contains("gone"));
    assert!(bc.prepare("SELECT x FROM gone").is_err());
    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 5), Some("committed".to_string()));
    assert_eq!(value(&bc, 6), Some(original(6)));
    assert!(!table_names(&bc).contains("gone"));
    // And the branch still writes after a rollback released its write lock.
    set(&bc, 6, "after-rollback");
    assert_eq!(value(&bc, 6), Some("after-rollback".to_string()));
}

#[test]
fn a_growing_branch_and_a_growing_trunk_do_not_share_new_page_numbers() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 100);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    // Both sides allocate pages past the fork-time end of the file. The page NUMBERS collide;
    // the contents must not.
    bc.execute("BEGIN").unwrap();
    for id in 1001..=1600 {
        bc.execute(format!("INSERT INTO t VALUES ({id}, 'branch-{id}')"))
            .unwrap();
    }
    bc.execute("COMMIT").unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 2001..=2600 {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, 'trunk-{id}')"))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    drop(bc);
    let bc = b.connect().unwrap();
    let branch = table(&bc);
    assert_eq!(branch.len(), 700);
    assert!(branch.keys().all(|&id| id <= 100 || (1001..=1600).contains(&id)));
    assert_eq!(branch[&1300], "branch-1300");
    let trunk_rows = table(&db.connect().unwrap());
    assert_eq!(trunk_rows.len(), 700);
    assert!(trunk_rows.keys().all(|&id| id <= 100 || (2001..=2600).contains(&id)));
    assert_eq!(trunk_rows[&2300], "trunk-2300");
    assert_eq!(rows(&bc, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
    assert_eq!(rows(&trunk, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
}

#[test]
fn a_branch_transaction_larger_than_its_page_cache_commits_intact() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    // A cache far smaller than the transaction: the pager wants to spill dirty pages, and a
    // branch's pages have nowhere to spill to (the WAL is the trunk's).
    bc.execute("PRAGMA cache_size = 10").unwrap();
    bc.execute("BEGIN").unwrap();
    for id in 1001..=3000 {
        bc.execute(format!("INSERT INTO t VALUES ({id}, 'big-{id}-{}')", "b".repeat(80)))
            .unwrap();
    }
    bc.execute("COMMIT").unwrap();
    drop(bc);

    let bc = b.connect().unwrap();
    let rows_seen = table(&bc);
    assert_eq!(rows_seen.len(), 2050);
    assert_eq!(rows_seen[&2500], format!("big-2500-{}", "b".repeat(80)));
    assert_eq!(rows(&bc, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
    let trunk_rows = table(&db.connect().unwrap());
    assert_eq!(trunk_rows.len(), 50, "the branch's big transaction reached the trunk");
}

#[test]
fn a_trunk_write_during_an_open_branch_transaction_stays_out_of_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    bc.execute("BEGIN").unwrap();
    set(&bc, 7, "branch");
    // Rows 7 and 8 share a leaf: the trunk overwrites the very page the branch has dirty.
    set(&trunk, 8, "trunk");
    set(&trunk, 150, "trunk");
    assert_eq!(value(&bc, 150), Some(original(150)));
    bc.execute("COMMIT").unwrap();

    assert_eq!(value(&bc, 7), Some("branch".to_string()));
    assert_eq!(value(&bc, 8), Some(original(8)));
    assert_eq!(value(&trunk, 7), Some(original(7)));
    assert_eq!(value(&trunk, 8), Some("trunk".to_string()));
}

#[test]
fn a_trunk_write_in_flight_cannot_leak_into_a_fork() {
    let (_dir, db) = open_db();
    let writer = db.connect().unwrap();
    seed(&writer, 50);
    let forker = db.connect().unwrap();

    writer.execute("BEGIN").unwrap();
    set(&writer, 7, "in-flight");
    // Either the fork is refused while the write is open, or the branch must not see it.
    let forked = forker.fork_branch();
    writer.execute("COMMIT").unwrap();
    match forked {
        Err(LimboError::Busy) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(b) => assert_eq!(value(&b.connect().unwrap(), 7), Some(original(7))),
    }
    let b = forker.fork_branch().unwrap();
    assert_eq!(value(&b.connect().unwrap(), 7), Some("in-flight".to_string()));
}

#[test]
fn a_deep_chain_sees_each_ancestor_as_of_its_own_fork() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 64);
    const DEPTH: i64 = 24;
    let mut chain: Vec<Branch> = vec![trunk.fork_branch().unwrap()];
    for level in 1..=DEPTH {
        let parent = chain.last().unwrap();
        let pc = parent.connect().unwrap();
        set(&pc, level, &format!("level-{level}"));
        let child = parent.fork().unwrap();
        // After the fork: must not reach the child.
        set(&pc, level + 32, "after-fork");
        drop(pc);
        chain.push(child);
    }
    let leaf = chain.last().unwrap().connect().unwrap();
    for level in 1..=DEPTH {
        assert_eq!(value(&leaf, level), Some(format!("level-{level}")));
        assert_eq!(value(&leaf, level + 32), Some(original(level + 32)));
    }
    drop(leaf);
    drop(chain);
    assert_eq!(db.branch_stats().live_branches, 0);
    assert_eq!(db.branch_stats().arena_slots_in_use, 0);
}

#[test]
fn a_branch_serves_one_connection_at_a_time() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 10);
    let b = trunk.fork_branch().unwrap();
    let first = b.connect().unwrap();
    let err = match b.connect() {
        Ok(_) => panic!("a second connection on one branch was allowed"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("already has an open connection"), "{err}");
    drop(first);
    b.connect().expect("the branch reopens once its connection is gone");
}

#[test]
fn forking_inside_a_transaction_is_refused() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 10);
    trunk.execute("BEGIN").unwrap();
    set(&trunk, 1, "uncommitted");
    assert!(trunk.fork_branch().is_err());
    trunk.execute("ROLLBACK").unwrap();
    trunk.fork_branch().unwrap();
}

#[test]
fn paths_that_bypass_the_copy_decision_are_refused_while_branches_exist() {
    // VACUUM is refused by the parser unless enabled; without this the VACUUM assertion below
    // would pass on the parser's refusal and never reach the branch guard.
    let (_dir, db) = open_db_with(DatabaseOpts::new().with_vacuum(true));
    let trunk = db.connect().unwrap();
    seed(&trunk, 10);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    // PRAGMA wal_checkpoint never raises: SQLite's contract reports a failed checkpoint as a row
    // whose first column (busy) is 1, and `op_checkpoint` maps every error to that. So the
    // refusal is checked on both surfaces — the pragma's busy flag, and the error text through
    // the API that does propagate it.
    let pragma = rows(&bc, "PRAGMA wal_checkpoint(TRUNCATE)");
    assert_eq!(pragma[0][0].as_int(), Some(1), "checkpoint on a branch ran: {pragma:?}");
    let api = bc.checkpoint(crate::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    });
    assert!(
        api.as_ref().is_err_and(|e| e.to_string().contains("trunk connection")),
        "checkpoint on a branch: {api:?}"
    );
    // The same pragma on the trunk is not refused: the flag above is the refusal, not noise.
    let trunk_pragma = rows(&trunk, "PRAGMA wal_checkpoint(TRUNCATE)");
    assert_eq!(trunk_pragma[0][0].as_int(), Some(0), "{trunk_pragma:?}");
    let vacuum = trunk.execute("VACUUM");
    assert!(
        vacuum.as_ref().is_err_and(|e| e.to_string().contains("copy-on-write")),
        "VACUUM with a live branch: {vacuum:?}"
    );
    let mode = trunk.execute("PRAGMA journal_mode = 'mvcc'");
    assert!(
        mode.as_ref().is_err_and(|e| e.to_string().contains("branches")),
        "journal_mode change with a live branch: {mode:?}"
    );
    drop(bc);
    drop(b);
    // The refusals are about live branches, not a latch: with none left, VACUUM runs.
    trunk.execute("VACUUM").unwrap();
}

#[test]
fn an_empty_database_is_refused() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    let err = match trunk.fork_branch() {
        Ok(_) => panic!("an empty database was branched"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("empty database"), "{err}");
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

/// Random forks, writes, deletes, growth, checkpoints and reaps across the trunk and a set of
/// branches, with every read compared against a model in which each branch is a plain copy of its
/// parent's table at the fork. Deletes followed by inserts put freed pages back into use, which is
/// the path where a page number changes meaning under a branch.
#[test]
fn a_random_workload_matches_a_model_of_independent_copies() {
    for seed in [0x9E3779B97F4A7C15u64, 0xD1B54A32D192ED03, 0x2545F4914F6CDD1D] {
        run_model(seed);
    }
}

fn run_model(seed: u64) {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed_table(&trunk);
    let mut rng = Rng(seed);
    // Slot 0 is the trunk. Some(handle) is a live branch.
    let mut handles: Vec<Option<Branch>> = vec![None];
    let mut models: Vec<Option<BTreeMap<i64, String>>> = vec![Some(table(&trunk))];
    let mut next_row = 10_000i64;

    let conn_for = |handles: &Vec<Option<Branch>>, i: usize| -> Arc<Connection> {
        if i == 0 {
            trunk.clone()
        } else {
            handles[i].as_ref().unwrap().connect().unwrap()
        }
    };

    for step in 0..400 {
        let live: Vec<usize> = (0..models.len()).filter(|&i| models[i].is_some()).collect();
        let who = live[rng.below(live.len() as u64) as usize];
        let op = rng.below(100);
        let ctx = format!("seed {seed:#x} step {step} node {who} op {op}");
        if op < 12 && live.len() < 12 {
            let parent = conn_for(&handles, who);
            let child = parent.fork_branch().unwrap();
            drop(parent);
            models.push(models[who].clone());
            handles.push(Some(child));
        } else if op < 20 && who != 0 {
            handles[who] = None;
            models[who] = None;
        } else if op < 24 && who == 0 {
            trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        } else {
            let conn = conn_for(&handles, who);
            let model = models[who].as_mut().unwrap();
            let before = model.clone();
            conn.execute("BEGIN").unwrap();
            for _ in 0..(1 + rng.below(12)) {
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
        // Every live node, read through a fresh connection, equals its model.
        for i in 0..models.len() {
            let Some(model) = &models[i] else { continue };
            let conn = if i == 0 { db.connect().unwrap() } else { conn_for(&handles, i) };
            assert_eq!(&table(&conn), model, "{ctx}: node {i} diverged from its model");
        }
    }
    drop(handles);
    assert_eq!(db.branch_stats().live_branches, 0, "seed {seed:#x}: branches leaked");
    assert_eq!(db.branch_stats().arena_slots_in_use, 0, "seed {seed:#x}: slots leaked");
    assert_eq!(rows(&trunk, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
}

fn seed_table(conn: &Arc<Connection>) {
    seed(conn, 300);
}

#[test]
fn a_reprepared_branch_statement_keeps_the_branch_schema_and_never_publishes_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let b = trunk.fork_branch().unwrap();

    // The trunk moves its schema on after the fork...
    trunk.execute("CREATE TABLE tr1(x)").unwrap();
    trunk.execute("CREATE TABLE tr2(x)").unwrap();

    let bc = b.connect().unwrap();
    let mut stale = bc.prepare("SELECT count(*) FROM t").unwrap();
    // ...and the branch moves its own further, so its cookie passes the trunk's. A statement
    // prepared before the branch's DDL must be reprepared, which is the path that consults the
    // shared schema and the path that can publish one.
    for name in ["b1", "b2", "b3"] {
        bc.execute(format!("CREATE TABLE {name}(x)")).unwrap();
    }
    let counted = stale.run_collect_rows().unwrap();
    assert_eq!(counted[0][0].as_int(), Some(50), "the reprepared statement read the wrong tree");
    drop(stale);

    let branch_tables = table_names(&bc);
    for name in ["b1", "b2", "b3"] {
        assert!(branch_tables.contains(name), "branch lost its own table {name}");
    }
    for name in ["tr1", "tr2"] {
        assert!(!branch_tables.contains(name), "branch sees the trunk's post-fork {name}");
        assert!(bc.prepare(format!("SELECT x FROM {name}")).is_err());
    }
    for (who, conn) in [("trunk", trunk.clone()), ("fresh trunk", db.connect().unwrap())] {
        for name in ["b1", "b2", "b3"] {
            assert!(
                conn.prepare(format!("SELECT x FROM {name}")).is_err(),
                "{who} can prepare against the branch's table {name}: the branch schema leaked"
            );
        }
        assert_eq!(rows(&conn, "SELECT count(*) FROM tr1")[0][0].as_int(), Some(0));
    }
}

#[test]
fn savepoint_and_statement_rollback_on_a_branch_undo_only_their_own_work() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 200);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    bc.execute("BEGIN").unwrap();
    set(&bc, 10, "kept");
    bc.execute("SAVEPOINT sp").unwrap();
    set(&bc, 10, "undone");
    set(&bc, 11, "undone");
    bc.execute("ROLLBACK TO sp").unwrap();
    bc.execute("RELEASE sp").unwrap();
    // A statement that fails half way: the first row inserts, the second collides.
    let failed = bc.execute(format!(
        "INSERT INTO t VALUES (5000, 'partial'), (1, '{}')",
        original(1)
    ));
    assert!(failed.is_err(), "the colliding insert succeeded");
    bc.execute("COMMIT").unwrap();

    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 10), Some("kept".to_string()));
    assert_eq!(value(&bc, 11), Some(original(11)));
    assert_eq!(value(&bc, 5000), None, "a failed statement's first row survived");
    assert_eq!(value(&trunk, 10), Some(original(10)));
}

#[test]
fn indexes_and_overflow_pages_branch_like_table_leaves() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE big(id INTEGER PRIMARY KEY, k TEXT, body TEXT)")
        .unwrap();
    trunk.execute("CREATE INDEX big_k ON big(k)").unwrap();
    // `replace(hex(zeroblob(n)), '0', 'a')` is 2n 'a's: a TEXT long enough that every row spills
    // onto a chain of overflow pages, with a distinguishing last character.
    let body = |n: usize, tail: &str| format!("replace(hex(zeroblob({n})), '0', 'a') || '{tail}'");
    trunk.execute("BEGIN").unwrap();
    for id in 1..=40 {
        trunk
            .execute(format!(
                "INSERT INTO big VALUES ({id}, 'k{id:03}', {})",
                body(5000, &format!("{}", id % 10))
            ))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    bc.execute(format!("UPDATE big SET k = 'branch', body = {} WHERE id = 7", body(6000, "B")))
        .unwrap();
    trunk
        .execute(format!("UPDATE big SET k = 'trunk', body = {} WHERE id = 8", body(4500, "T")))
        .unwrap();

    let probe = |conn: &Arc<Connection>, id: i64| -> (String, i64, String) {
        let r = rows(
            conn,
            &format!("SELECT k, length(body), substr(body, -1) FROM big WHERE id = {id}"),
        );
        let text = |v: &Value| match v {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("{other:?}"),
        };
        (text(&r[0][0]), r[0][1].as_int().unwrap(), text(&r[0][2]))
    };
    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(probe(&bc, 7), ("branch".to_string(), 12001, "B".to_string()));
    assert_eq!(probe(&bc, 8), ("k008".to_string(), 10001, "8".to_string()));
    assert_eq!(probe(&trunk, 7), ("k007".to_string(), 10001, "7".to_string()));
    assert_eq!(probe(&trunk, 8), ("trunk".to_string(), 9001, "T".to_string()));
    // Lookups by the indexed column, on both sides; integrity_check below also cross-checks
    // every index entry against its row, which is what catches an index page that branched
    // differently from its table.
    let by_k = |conn: &Arc<Connection>, k: &str| {
        rows(conn, &format!("SELECT id FROM big WHERE k = '{k}'"))
            .into_iter()
            .map(|r| r[0].as_int().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(by_k(&bc, "branch"), vec![7]);
    assert_eq!(by_k(&bc, "trunk"), Vec::<i64>::new());
    assert_eq!(by_k(&bc, "k008"), vec![8]);
    assert_eq!(by_k(&trunk, "trunk"), vec![8]);
    assert_eq!(by_k(&trunk, "branch"), Vec::<i64>::new());
    assert_eq!(by_k(&trunk, "k007"), vec![7]);
    for conn in [&bc, &trunk] {
        assert_eq!(rows(conn, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
    }
}

#[test]
fn branches_write_concurrently_with_each_other_and_the_trunk() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 400);
    const WRITERS: i64 = 6;
    const ROUNDS: i64 = 40;
    let branches: Vec<Branch> = (0..WRITERS).map(|_| trunk.fork_branch().unwrap()).collect();

    std::thread::scope(|scope| {
        for (w, branch) in branches.iter().enumerate() {
            let w = w as i64;
            scope.spawn(move || {
                let conn = branch.connect().unwrap();
                for round in 0..ROUNDS {
                    // Every writer rewrites the same rows: only isolation keeps them apart.
                    let id = 1 + (round * 7) % 400;
                    exec_retrying_busy(&conn, &format!("UPDATE t SET v = 'w{w}-r{round}' WHERE id = {id}"));
                }
            });
        }
        let db = db.clone();
        scope.spawn(move || {
            let conn = db.connect().unwrap();
            for round in 0..ROUNDS {
                let id = 1 + (round * 7) % 400;
                exec_retrying_busy(&conn, &format!("UPDATE t SET v = 'trunk-r{round}' WHERE id = {id}"));
            }
        });
    });

    for (w, branch) in branches.iter().enumerate() {
        let conn = branch.connect().unwrap();
        for round in 0..ROUNDS {
            let id = 1 + (round * 7) % 400;
            // A later round can rewrite the same id; the expected value is the LAST round's.
            let last = (0..ROUNDS).filter(|r| 1 + (r * 7) % 400 == id).max().unwrap();
            assert_eq!(value(&conn, id), Some(format!("w{w}-r{last}")), "writer {w} row {id}");
        }
        assert_eq!(value(&conn, 399), Some(original(399)));
    }
    let fresh = db.connect().unwrap();
    for round in 0..ROUNDS {
        let id = 1 + (round * 7) % 400;
        let last = (0..ROUNDS).filter(|r| 1 + (r * 7) % 400 == id).max().unwrap();
        assert_eq!(value(&fresh, id), Some(format!("trunk-r{last}")));
    }
}

/// Concurrent connections share one WAL, so a read-lock acquisition can transiently report Busy;
/// that is contention, not a verdict, and the statement is retried.
fn exec_retrying_busy(conn: &Arc<Connection>, sql: &str) {
    for _ in 0..10_000 {
        match conn.execute(sql) {
            Ok(()) => return,
            Err(LimboError::Busy | LimboError::BusySnapshot) => std::thread::yield_now(),
            Err(e) => panic!("{sql}: {e}"),
        }
    }
    panic!("{sql}: still Busy after 10,000 attempts");
}

/// A branch transaction far larger than its page cache spills into its own slots (a STEAL policy):
/// the cache stays bounded, a spilled page rewritten later in the transaction commits its last
/// image, a rollback returns exactly the slots the transaction took, a savepoint rollback restores
/// pages spilled after the savepoint, and a child forked before a large commit keeps what it saw.
#[test]
fn a_spilling_branch_transaction_commits_rolls_back_and_forks_intact() {
    let (_dir, db) = open_db();
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
    assert_eq!(value(&trunk, 7), Some(original(7)));

    let child = b.fork().unwrap();
    let committed = in_use(&db);
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'c-' || id").unwrap();
    bc.execute("ROLLBACK").unwrap();
    assert_eq!(in_use(&db), committed, "a rolled-back transaction kept or lost slots");

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
    assert_eq!(rows(&bc, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
    let cc = child.connect().unwrap();
    for (id, v) in table(&cc) {
        assert_eq!(v, expect(id, "b", "a"), "the child's row {id}");
    }
    assert_eq!(rows(&cc, "PRAGMA integrity_check")[0][0], Value::from_text("ok"));
    drop(cc);
    drop(child);
    drop(bc);
    drop(b);
    assert!(in_use(&db).is_empty(), "slots leaked");
    assert_eq!(db.branch_stats().live_branches, 0);
}

/// A branch transaction larger than one hold's batch: its commit, its first fork and its reap each
/// hold the store mutex for a bounded number of pages, and nothing is copied under the mutex.
#[test]
fn a_large_branch_commit_fork_and_reap_hold_the_store_mutex_briefly() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("BEGIN").unwrap();
    bc.execute("UPDATE t SET v = 'big-' || id").unwrap();
    let copied = db.branch_stats().work.locked_copy_bytes;
    let _ = db.branch_take_hold_max();
    bc.execute("COMMIT").unwrap();
    let commit = db.branch_take_hold_max();
    let owned = db.branch_stats().arena_slots_in_use as u64;
    assert!(owned > 256, "the transaction owns only {owned} pages: too small to test the bound");
    assert!(commit.pages <= 64, "one commit hold mapped {} pages", commit.pages);
    assert_eq!(commit.copy_bytes, 0, "the commit copied pages under the store mutex");
    assert_eq!(
        db.branch_stats().work.locked_copy_bytes,
        copied,
        "the commit copied pages under the store mutex"
    );

    let built = db.branch_stats().work.view_build_pages;
    let child = b.fork().unwrap();
    let fork = db.branch_take_hold_max();
    assert!(fork.pages <= 1, "the first fork held the mutex over {} pages", fork.pages);
    assert_eq!(db.branch_stats().work.view_build_pages - built, owned);
    assert_eq!(value(&child.connect().unwrap(), 7), Some("big-7".to_string()));

    drop(child);
    drop(bc);
    let _ = db.branch_take_hold_max();
    let reaped = b.reap().unwrap();
    let reap = db.branch_take_hold_max();
    assert_eq!(reaped.freed_pages as u64, owned);
    assert!(reap.pages <= 64, "one reap hold freed {} pages", reap.pages);
    assert_eq!(db.branch_stats().arena_slots_in_use, 0);
}
