//! F-S source key v2 (r11-githost-attr PREREG A3.10). v2 reads the A1 key's inputs raw, so it must
//! be a refinement of v1: equal v2 keys only where v1 keys are equal. It must still tell apart what
//! A1.3 had to tell apart (one schema cookie, different DDL), read overflowing `sqlite_schema` cells
//! whole, and ignore header bytes that are not a key input (a branch that grew has its own page 1).
//! The `R11_SCHEMA_KEY` gate itself is read once per process and is not exercised here.
//!
//! ⚠ UNBUILT when written.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::path::Path;

fn open(path: &Path, opts: DatabaseOpts) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn durable() -> DatabaseOpts {
    DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: true })
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn cookie(conn: &Arc<Connection>) -> i64 {
    rows(conn, "PRAGMA schema_version")[0][0].as_int().expect("integer cookie")
}

/// (v1, v2) for the connection's current schema source.
fn keys(conn: &Arc<Connection>) -> (Vec<u8>, Vec<u8>) {
    (
        conn.branch_schema_source_key(1).unwrap(),
        conn.branch_schema_source_key(2).unwrap(),
    )
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn v2_keys_are_equal_exactly_where_v1_keys_are_on_one_cookie() {
    let dir = tempfile::TempDir::new().unwrap();
    let conns: Vec<Arc<Connection>> = ["a.db", "b.db", "c.db"]
        .iter()
        .map(|name| open(&dir.path().join(name), DatabaseOpts::new()).connect().unwrap())
        .collect();
    conns[0].execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conns[1].execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conns[2].execute("CREATE TABLE u(w)").unwrap();
    // Premise: one cookie for all three, so only the rows can tell c apart.
    assert_eq!(cookie(&conns[0]), cookie(&conns[1]));
    assert_eq!(cookie(&conns[0]), cookie(&conns[2]));
    let (a1, a2) = keys(&conns[0]);
    let (b1, b2) = keys(&conns[1]);
    let (c1, c2) = keys(&conns[2]);
    assert_eq!(a1, b1, "premise: v1 shares a and b");
    assert_eq!(a2, b2, "v2 must share what v1 shares on byte-identical sources");
    assert_ne!(a1, c1, "premise: v1 tells a from c");
    assert_ne!(a2, c2, "v2 shared two different schemas under one cookie");
    // The connection still runs statements afterwards (the key read restores its schema).
    assert_eq!(rows(&conns[0], "SELECT count(*) FROM t")[0][0].as_int(), Some(0));
    assert!(conns[0].branch_schema_source_key(3).is_err(), "an unknown key version was accepted");
}

#[test]
fn v2_reads_an_overflowing_sqlite_schema_cell_whole() {
    let dir = tempfile::TempDir::new().unwrap();
    let conn = open(&dir.path().join("wide.db"), DatabaseOpts::new()).connect().unwrap();
    let cols: Vec<String> = (0..600).map(|i| format!("column_{i:04} INTEGER")).collect();
    conn.execute(format!("CREATE TABLE wide({})", cols.join(", "))).unwrap();
    let sql = match &rows(&conn, "SELECT sql FROM sqlite_schema WHERE name = 'wide'")[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?}"),
    };
    let page_size = rows(&conn, "PRAGMA page_size")[0][0].as_int().unwrap() as usize;
    assert!(sql.len() > page_size, "premise: the cell must overflow its page");
    let (k1, k2) = keys(&conn);
    assert!(contains(&k1, sql.as_bytes()), "premise: v1 carries the whole statement");
    assert!(contains(&k2, sql.as_bytes()), "v2 lost the overflow part of the cell");
}

#[test]
fn branches_share_a_v2_key_by_schema_not_by_page_one() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("branches.db"), durable());
    let trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let x = trunk.fork_branch().unwrap();
    let y = trunk.fork_branch().unwrap();
    let grown = trunk.fork_branch().unwrap();
    let xc = x.connect().unwrap();
    let yc = y.connect().unwrap();
    let gc = grown.connect().unwrap();
    xc.execute("CREATE TABLE x(a)").unwrap();
    yc.execute("CREATE TABLE y(b)").unwrap();
    // A branch that only writes rows, enough to allocate pages: its page 1 header differs from the
    // trunk's (database_size), its schema does not.
    gc.execute("BEGIN").unwrap();
    for id in 1..=2000 {
        gc.execute(format!("INSERT INTO t VALUES ({id}, '{}')", "g".repeat(200))).unwrap();
    }
    gc.execute("COMMIT").unwrap();
    let pages = |c: &Arc<Connection>| rows(c, "PRAGMA page_count")[0][0].as_int().unwrap();
    assert!(pages(&gc) > pages(&trunk), "premise: the grown branch allocated pages");
    // Premise: x and y carry one cookie and different DDL (A1.3's hazard).
    assert_eq!(cookie(&xc), cookie(&yc));
    let (x1, x2) = keys(&xc);
    let (y1, y2) = keys(&yc);
    assert_ne!(x1, y1, "premise: v1 tells x from y");
    assert_ne!(x2, y2, "v2 shared x's schema with y under one cookie");
    let (t1, t2) = keys(&trunk);
    let (g1, g2) = keys(&gc);
    assert_eq!(t1, g1, "premise: v1 shares the grown branch with the trunk");
    assert_eq!(t2, g2, "v2 keyed on something beyond the schema source");
}
