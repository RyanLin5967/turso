//! fastest-engine red (lead RESUME item 10; fastest-wire beb4 LOW-6, SILENT): through the
//! PostgreSQL frontend a quoted schema name stays exact, as PostgreSQL keeps it: "Q1" and q1 are
//! two schemas. The engine's attached-database catalog folded every name to lower case
//! (DatabaseCatalog's name_to_index, keyed by normalize_ident), so schema "Q1" was attached as q1
//! and q1.u read it. PostgreSQL 18 semantics (identifiers: an unquoted name folds to lower case, a
//! quoted one is exact): q1.u does not exist until q1 is created, and then each reads its own rows.

use std::sync::Arc;

#[test]
fn a_quoted_schema_name_stays_exact_through_the_frontend() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("pg.db");
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    let rows = |sql: &str| -> Result<Vec<String>, String> {
        match c.query(sql) {
            Ok(Some(mut st)) => st
                .run_collect_rows()
                .map(|rows| rows.iter().map(|r| r[0].to_string()).collect())
                .map_err(|e| e.to_string()),
            Ok(None) => Err("no statement".to_string()),
            Err(e) => Err(e.to_string()),
        }
    };
    let mut wrong = Vec::new();
    for sql in ["CREATE SCHEMA \"Q1\"", "CREATE TABLE \"Q1\".u (x int)", "INSERT INTO \"Q1\".u VALUES (1)"] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    match rows("SELECT x FROM q1.u") {
        Err(_) => {}
        Ok(r) => wrong.push(format!("SELECT x FROM q1.u answered {r:?}: PostgreSQL has no schema q1 yet (\"Q1\" is another)")),
    }
    for sql in ["CREATE SCHEMA q1", "CREATE TABLE q1.u (x int)", "INSERT INTO q1.u VALUES (2)"] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("{sql}: {e}; PostgreSQL creates q1 beside \"Q1\""));
        }
    }
    for (sql, want) in [("SELECT x FROM \"Q1\".u", "1"), ("SELECT x FROM q1.u", "2"), ("SELECT x FROM Q1.u", "2")] {
        match rows(sql) {
            Ok(r) if r == [want.to_string()] => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want}")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: a quoted schema name stays exact, an unquoted one folds: {wrong:#?}");
}
