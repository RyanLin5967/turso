//! fastest-engine red (RESUME item 11, fastest-wire numfuzz seed 402): under the PostgreSQL
//! dialect a division or remainder by zero is refused, as PostgreSQL 18.6 raises 22012 "division
//! by zero" for int2, int4, int8, float8 and numeric operands (recorded on iccad11 and eceubuntu2,
//! artie b7d6007cd2 and b09c790590). SQLite's rule, which the engine followed for every dialect,
//! answers NULL: seed 402's `(-(-2147483648) % round(2.70247e-300)) % 834932.693` read NULL. A
//! NULL operand still gives NULL, a row the WHERE drops raises nothing, and -2147483648 % -1 is 0,
//! each as PostgreSQL answers. The SQLite dialect keeps NULL: its control.

use std::sync::Arc;

/// A value as the test compares it: NULL spelled out (Value's Display prints NULL as empty).
fn shown(v: &turso_core::Value) -> String {
    if matches!(v, turso_core::Value::Null) {
        "NULL".to_string()
    } else {
        v.to_string()
    }
}

#[test]
fn a_division_or_remainder_by_zero_is_refused_under_the_postgresql_dialect_only() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    let answer = |sql: &str| -> Result<Vec<String>, String> {
        match c.query(sql) {
            Ok(Some(mut st)) => st
                .run_collect_rows()
                .map(|rows| rows.iter().map(|r| shown(&r[0])).collect())
                .map_err(|e| e.to_string()),
            Ok(None) => Err("no statement".to_string()),
            Err(e) => Err(e.to_string()),
        }
    };
    let mut wrong = Vec::new();
    for sql in ["CREATE TABLE z (id int PRIMARY KEY, a int, b int)", "INSERT INTO z VALUES (1, 5, 0), (2, 7, 2)"] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    // PostgreSQL 18.6: ERROR 22012 "division by zero" for each.
    for sql in [
        "SELECT 5 % 0",
        "SELECT 5::int8 % 0::int8",
        "SELECT 5::int2 % 0::int2",
        "SELECT 5 / 0",
        "SELECT 5::int8 / 0::int8",
        "SELECT 5.0::float8 / 0",
        "SELECT 5.5::numeric % 0",
        "SELECT ((-((-2147483648)) % round(2.70247e-300)) % 00834932.693) AS v",
        "SELECT a % b FROM z WHERE id = 1",
    ] {
        match answer(sql) {
            Err(e) if e.contains("division by zero") => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL 22012 division by zero")),
        }
    }
    // PostgreSQL 18.6's answers, no error.
    for (sql, want) in [
        ("SELECT NULL::int % 0", vec!["NULL"]),
        ("SELECT 5 % NULL::int", vec!["NULL"]),
        ("SELECT a % b FROM z WHERE id = 2", vec!["1"]),
        ("SELECT a % b FROM z WHERE false", vec![]),
        ("SELECT -2147483648 % -1", vec!["0"]),
    ] {
        match answer(sql) {
            Ok(rows) if rows == want => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want:?}")),
        }
    }
    // Control: the SQLite dialect answers NULL.
    let sdb = turso_core::Database::open_file_with_flags(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("sqlite.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let sc = sdb.connect().unwrap();
    for sql in ["SELECT 5 % 0", "SELECT 5 / 0", "SELECT 5.0 / 0"] {
        let got = sc.prepare(sql).and_then(|mut s| s.run_collect_rows()).map(|r| shown(&r[0][0]));
        if got.as_ref().ok().map(String::as_str) != Some("NULL") {
            wrong.push(format!("control {sql} under the SQLite dialect: {got:?}, SQLite NULL"));
        }
    }
    assert!(wrong.is_empty(), "CLAIM: division and remainder by zero raise under the PostgreSQL dialect only: {wrong:#?}");
}
