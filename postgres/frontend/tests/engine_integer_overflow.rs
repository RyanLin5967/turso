//! fastest-engine red (lead RESUME item 9; wire numfuzz; PostgreSQL 18.6 recording artie
//! c49a20fd70, rec25-numeric-text): through the PostgreSQL frontend an integer overflow inside a
//! function is refused, as PostgreSQL raises 22003, never answered. SQLite's rule, which the engine
//! follows, turns an int64 overflow into REAL silently and has no int4 width. The controls (int4
//! widening into int8, and an int8 product that fits) answer; they are from PostgreSQL's operator
//! typing (an integer literal past int4 is int8), not recorded.

use std::sync::Arc;

#[test]
fn an_integer_overflow_inside_a_function_is_refused_through_the_frontend() {
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
                .map_err(|e| e.to_string()),
            Ok(None) => Err("no statement".to_string()),
            Err(e) => Err(e.to_string()),
        }
    };
    let mut wrong = Vec::new();
    // PostgreSQL 18.6: 22003 "integer out of range" / "bigint out of range".
    for sql in [
        "SELECT floor((2147483647 * 32767)) AS v",
        "SELECT abs(2147483647 + 1) AS v",
        "SELECT round(9223372036854775807 * 2) AS v",
    ] {
        if let Ok(rows) = answer(sql) {
            wrong.push(format!("{sql}: answered {rows:?}; PostgreSQL 18.6 raises 22003"));
        }
    }
    // Controls: PostgreSQL widens int4 + int8 to int8, and an int8 product that fits is answered.
    for (sql, want) in [
        ("SELECT 2147483647 + 2147483648 AS v", "4294967295"),
        ("SELECT abs(3037000499 * 3037000499) AS v", "9223372030926249001"),
    ] {
        match answer(sql) {
            Ok(rows) if rows == [want.to_string()] => {}
            other => wrong.push(format!("control {sql}: {other:?}, PostgreSQL {want}")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: integer overflow is refused as PostgreSQL refuses it: {wrong:#?}");
}
