//! fastest-engine red (lead order 2026-10-21; numeric DESIGN addendum 3 (a); PostgreSQL 18.6
//! recording artie c49a20fd70): under the PostgreSQL dialect an int8 overflow is refused, as
//! PostgreSQL raises 22003 "bigint out of range", wherever the expression sits: in a SELECT list,
//! inside a function, in a WHERE clause. SQLite's rule, which the engine followed for every dialect,
//! turns it into a REAL silently (round(9223372036854775807 * 2) answered 1.84467440737096e+19).
//! The SQLite dialect keeps SQLite's rule: the same expression there is the REAL, its control.

use std::sync::Arc;

#[test]
fn an_int8_overflow_is_refused_under_the_postgresql_dialect_only() {
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
    let answer = |sql: &str| -> Result<Vec<String>, String> {
        match c.query(sql) {
            Ok(Some(mut st)) => st
                .run_collect_rows()
                .map(|rows| rows.iter().map(|r| r[0].to_string()).collect())
                .map_err(|e| format!("{e:?}")),
            Ok(None) => Err("no statement".to_string()),
            Err(e) => Err(format!("{e:?}")),
        }
    };
    let mut wrong = Vec::new();
    // PostgreSQL 18.6 raises 22003 "bigint out of range" on each (recorded: the round(...) case).
    for sql in [
        "SELECT round(9223372036854775807 * 2) AS v",
        "SELECT 9223372036854775807 + 1 AS v",
        "SELECT -9223372036854775807 - 2 AS v",
        "SELECT abs(4611686018427387904 * 2) AS v",
        "SELECT count(*) FROM (SELECT 1 AS one) s WHERE 9223372036854775807 * 3 > 0",
    ] {
        match answer(sql) {
            Err(e) if e.contains("IntegerOverflow") => {}
            other => wrong.push(format!("{sql}: {other:?}; PostgreSQL 18.6 raises 22003")),
        }
    }
    for (sql, want) in [("SELECT 9223372036854775806 + 1 AS v", "9223372036854775807"), ("SELECT -9223372036854775807 - 1 AS v", "-9223372036854775808")] {
        match answer(sql) {
            Ok(rows) if rows == [want.to_string()] => {}
            other => wrong.push(format!("control {sql}: {other:?}, want {want}")),
        }
    }
    // The SQLite dialect keeps SQLite's rule: the overflow is a REAL.
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
    let got = sc.prepare("SELECT typeof(9223372036854775807 + 1)").and_then(|mut s| s.run_collect_rows()).map(|r| r[0][0].to_string());
    if got.as_ref().ok().map(String::as_str) != Some("real") {
        wrong.push(format!("SQLite dialect control: typeof(9223372036854775807 + 1) is {got:?}, SQLite gives real"));
    }
    assert!(wrong.is_empty(), "CLAIM: an int8 overflow is refused under the PostgreSQL dialect only: {wrong:#?}");
}
