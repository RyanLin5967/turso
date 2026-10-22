//! PROBE, not for delivery: does the frontend keep quoted table and column names exact?
use std::sync::Arc;

#[test]
fn probe_ident_case() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("pg.db");
    let db = turso_pg::open_database_with_io(Arc::new(turso_core::PlatformIO::new().unwrap()), path.to_str().unwrap(), turso_core::OpenFlags::default(), turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true)).unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    for sql in [
        "CREATE TABLE \"T\" (x int)", "INSERT INTO \"T\" VALUES (1)", "CREATE TABLE t (x int)", "INSERT INTO t VALUES (2)",
        "CREATE TABLE k (\"A\" int, a int)", "INSERT INTO k VALUES (10, 20)",
        "CREATE SCHEMA \"Q1\"", "CREATE SCHEMA q1",
    ] {
        println!("PROBE exec {sql} => {:?}", c.execute(sql).map(|_| ()).map_err(|e| e.to_string()));
    }
    for sql in ["SELECT x FROM \"T\"", "SELECT x FROM t", "SELECT \"A\" FROM k", "SELECT a FROM k"] {
        let r = match c.query(sql) { Ok(Some(mut st)) => st.run_collect_rows().map(|rows| rows.iter().map(|r| r[0].to_string()).collect::<Vec<_>>()).map_err(|e| e.to_string()), Ok(None) => Err("none".into()), Err(e) => Err(e.to_string()) };
        println!("PROBE query {sql} => {r:?}");
    }
}
