//! A branch pager's page cache across trunk commits (lane r12-branch-noclear).
//!
//! `Pager::begin_read_tx` empties a connection's whole page cache whenever the WAL changed since
//! its last read transaction. A branch reads trunk pages through the trunk's WAL, so every trunk
//! commit emptied every branch connection's cache, and its next statement re-resolved every page
//! it had already read. But nothing a branch can see changes with a trunk commit: a trunk write
//! retains the version a live branch sees before the write reaches the WAL, and a branch's own
//! writes come through its own (single) connection. So a page cached by a branch stays right.
//!
//! What a connection sees is read back through SQL, as in `isolation_tests.rs`. Whether a read was
//! served from the cache is read from the store's resolve count: a branch pager asks the store for
//! every page it does not hold, and for nothing else. Each test also shows the branch's read
//! transaction saw the trunk's commit (its WAL snapshot moved), without which a cache hit would
//! prove nothing.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::BTreeSet;

const ROWS: i64 = 400;
/// Two rows on different leaf pages (rows of ~110 bytes, ~33 to a 4 KiB leaf).
const X: i64 = 7;
const Y: i64 = 300;

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branching.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    (dir, db)
}

fn original(id: i64) -> String {
    format!("trunk-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=ROWS {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn value(conn: &Arc<Connection>, id: i64) -> String {
    let r = rows(conn, &format!("SELECT v FROM t WHERE id = {id}"));
    assert_eq!(r.len(), 1, "row {id} must exist exactly once");
    match &r[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("row {id}: expected text, got {other:?}"),
    }
}

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
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

/// Pages the branch store has resolved for branch pagers, all branches together.
fn resolves(db: &Database) -> u64 {
    db.branch_stats().work.resolve_calls
}

/// The connection's WAL snapshot: the last frame its latest read transaction could see.
fn snapshot(conn: &Arc<Connection>) -> u64 {
    conn.pager.load().wal_state().unwrap().max_frame
}

/// Read row `id` on `conn` and return what it cost the store: the pages resolved for that read.
fn read_counting(db: &Database, conn: &Arc<Connection>, id: i64) -> (String, u64) {
    let before = resolves(db);
    let v = value(conn, id);
    (v, resolves(db) - before)
}

#[test]
fn a_trunk_rewrite_of_a_page_a_branch_cached_is_a_cache_hit_and_the_fork_version() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    let (v, first) = read_counting(&db, &bc, X);
    assert_eq!(v, original(X));
    assert!(
        first > 0,
        "the first read resolved no page: the branch pager did not read through the store"
    );
    let before = snapshot(&bc);

    // The trunk rewrites X's leaf and commits: the WAL has changed under the branch.
    set(&trunk, X, "trunk-after-fork");
    assert_eq!(value(&trunk, X), "trunk-after-fork");

    let (v, again) = read_counting(&db, &bc, X);
    assert_eq!(
        v,
        original(X),
        "the branch read the trunk's rewrite of its fork"
    );
    assert!(
        snapshot(&bc) > before,
        "the branch's read transaction did not see the trunk's commit, so nothing was tested"
    );
    assert_eq!(
        again, 0,
        "the branch re-resolved {again} pages it had cached: a trunk commit emptied its cache"
    );

    // Reopen: a new connection on the branch starts with an empty cache, resolves again, and reads
    // the same fork version (now from the version the trunk retained for it).
    drop(bc);
    let bc = b.connect().unwrap();
    let (v, reopened) = read_counting(&db, &bc, X);
    assert_eq!(
        v,
        original(X),
        "a reopened branch connection read the trunk's rewrite"
    );
    assert!(reopened > 0, "a new connection's cache must start empty");
}

#[test]
fn a_branch_keeps_its_own_committed_page_cached_across_a_trunk_rewrite_of_that_page() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    set(&bc, Y, "branch-own");
    assert_eq!(value(&bc, Y), "branch-own");
    let before = snapshot(&bc);

    // The trunk rewrites the same page (Y's leaf) after the branch's commit.
    set(&trunk, Y, "trunk-after-fork");

    let (v, again) = read_counting(&db, &bc, Y);
    assert_eq!(
        v, "branch-own",
        "the branch lost its own write to the trunk's"
    );
    assert!(
        snapshot(&bc) > before,
        "the branch's read transaction did not see the trunk's commit, so nothing was tested"
    );
    assert_eq!(
        again, 0,
        "the branch re-resolved {again} pages of its own it had cached: a trunk commit emptied its cache"
    );

    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(
        value(&bc, Y),
        "branch-own",
        "the branch's write did not survive a reopen"
    );
    assert_eq!(value(&bc, X), original(X));
}

#[test]
fn trunk_ddl_after_the_fork_leaves_a_branch_cache_and_schema_as_forked() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    assert_eq!(value(&bc, X), original(X));
    let before = snapshot(&bc);

    // DDL rewrites page 1 (the schema cookie and sqlite_schema) and allocates a root page.
    trunk.execute("CREATE TABLE u(a INTEGER)").unwrap();
    trunk.execute("INSERT INTO u VALUES (1)").unwrap();
    assert!(table_names(&trunk).contains("u"));

    let (v, again) = read_counting(&db, &bc, X);
    assert_eq!(v, original(X));
    assert!(
        snapshot(&bc) > before,
        "the branch's read transaction did not see the trunk's DDL, so nothing was tested"
    );
    assert_eq!(
        again, 0,
        "the branch re-resolved {again} pages after trunk DDL: its cache or schema cookie was dropped"
    );
    // The branch's schema is its fork's: the trunk's new table does not exist on it.
    assert_eq!(
        table_names(&bc),
        BTreeSet::from(["t".to_string()]),
        "the branch saw the trunk's DDL"
    );
    assert!(
        bc.execute("SELECT count(*) FROM u").is_err(),
        "the branch could query a table the trunk created after the fork"
    );
}

/// The clear that must stay: a trunk connection's view moves with the WAL, so another trunk
/// connection's commit must still empty its cache. A live branch is open throughout, so the
/// trunk's writes take the branch copy decision as in every test above.
#[test]
fn a_trunk_connection_still_reads_another_trunk_connections_commit() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, X), original(X));

    assert_eq!(value(&trunk, X), original(X));
    let other = db.connect().unwrap();
    set(&other, X, "other-trunk-connection");
    assert_eq!(
        value(&trunk, X),
        "other-trunk-connection",
        "a trunk connection served a page cached before another trunk connection's commit"
    );
    assert_eq!(value(&bc, X), original(X));
}

/// The clear that must stay on a branch: rolling back its own write transaction drops the pages
/// that transaction dirtied, before and after a trunk commit.
#[test]
fn a_branch_rollback_still_drops_the_pages_it_dirtied() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, X), original(X));

    bc.execute("BEGIN").unwrap();
    set(&bc, X, "uncommitted");
    assert_eq!(value(&bc, X), "uncommitted");
    bc.execute("ROLLBACK").unwrap();
    assert_eq!(
        value(&bc, X),
        original(X),
        "the branch served a page its rolled-back transaction had dirtied"
    );

    set(&trunk, X, "trunk-after-rollback");
    assert_eq!(value(&bc, X), original(X));
    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(
        value(&bc, X),
        original(X),
        "the rolled-back write reached the branch"
    );
}

/// `rows`, retrying a statement that found the shared WAL's read lock busy: contention between
/// connections, not a verdict.
fn rows_retrying(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    for _ in 0..10_000 {
        match conn
            .prepare(sql)
            .and_then(|mut stmt| stmt.run_collect_rows())
        {
            Ok(rows) => return rows,
            Err(LimboError::Busy | LimboError::BusySnapshot) => std::thread::yield_now(),
            Err(e) => panic!("{sql}: {e}"),
        }
    }
    panic!("{sql}: still Busy after 10,000 attempts");
}

/// Under a trunk that commits continuously, a branch connection keeps its cache across every one
/// of those commits and still reads its fork in every statement: the pages it resolves for the
/// first time are read at a WAL snapshot that moves under it, the pages it holds stay the fork's,
/// and its cache is never emptied.
#[test]
fn a_branch_reads_its_fork_in_every_statement_while_the_trunk_commits_concurrently() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let conn = db.connect().unwrap();
            let mut rng = 0x2545_F491_4F6C_DD1Du64;
            let mut commits = 0u64;
            while !stop.load(Ordering::Acquire) {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let id = (rng % ROWS as u64) as i64 + 1;
                match conn.execute(format!("UPDATE t SET v = 'w{commits}' WHERE id = {id}")) {
                    Ok(()) => commits += 1,
                    Err(LimboError::Busy | LimboError::BusySnapshot) => std::thread::yield_now(),
                    Err(e) => panic!("trunk writer: {e}"),
                }
            }
            commits
        })
    };

    let before = db.branch_stats();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    for read in 0..2_000 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let id = (rng % ROWS as u64) as i64 + 1;
        let r = rows_retrying(&bc, &format!("SELECT v FROM t WHERE id = {id}"));
        assert_eq!(
            r.len(),
            1,
            "statement {read}: row {id} must exist exactly once"
        );
        match &r[0][0] {
            Value::Text(t) => assert_eq!(
                t.as_str(),
                original(id),
                "statement {read}: the branch read row {id} as the trunk rewrote it"
            ),
            other => panic!("row {id}: expected text, got {other:?}"),
        }
    }
    stop.store(true, Ordering::Release);
    let commits = writer.join().unwrap();
    let after = db.branch_stats();

    let kept = after.branch_cache_clears_skipped - before.branch_cache_clears_skipped;
    assert!(
        commits > 0 && kept > 0,
        "nothing was tested: {commits} trunk commits, {kept} WAL changes seen by the branch"
    );
    assert_eq!(
        after.branch_cache_clears, before.branch_cache_clears,
        "the branch's cache was emptied during the run ({kept} WAL changes kept across)"
    );
    let all = rows_retrying(&bc, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(all.len(), ROWS as usize);
    for row in all {
        let id = row[0].as_int().expect("integer id");
        match &row[1] {
            Value::Text(t) => assert_eq!(t.as_str(), original(id), "row {id} at the end"),
            other => panic!("row {id}: expected text, got {other:?}"),
        }
    }
}
