//! T-K1 of lane r12-branch-noclear, copied VERBATIM with its helpers from turso 6f3555343:core/branch/noclear_tests.rs
//! (lines 16-72 and 232-251; the two doc-comment lines that followed introduce the next test, not copied), as that lane asked, so r12-phasefair's gate runs it under TURSO_R12_XI=on (PREREG amendment 10).
//! A trunk connection must read another trunk connection's commit: the correctness guard F-XI (trunk pagers) and noclear
//! (branch pagers) share. The rest of noclear_tests.rs needs noclear's branch arm, which this tip does not carry.

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
