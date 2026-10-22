//! fastest-engine red (RESUME item 12, fastest-wire numfuzz seed 1715): a numeric NaN never reaches
//! an integer. PostgreSQL 18.6 refuses ('NaN'::numeric)::int, ::int8 and ::int2 with 0A000 "cannot
//! convert NaN to ...", in a SELECT and in seed 1715's UPDATE, even one matching no row (recorded on
//! iccad11, artie raw uw-pg18-nan_to_integer-rec12-iccad11.txt). This engine has no NaN value: its
//! float input already refuses 'NaN' with "NaN is not supported by this engine" (PostgresDialect
//! cast_text_to_float), which the server maps to 0A000 (a ParseError naming "not supported"). The
//! claim is that a numeric 'NaN' is refused the same way, so the integer never holds a value
//! PostgreSQL refuses; seed 1715's UPDATE answered without an error.
//!
//! The second test confirms seed 848 (lead order): (((9223372036854775807 + 7) - (-0.87176649)) -
//! 6.156) is PostgreSQL's 22003 "bigint out of range" (recorded), this engine's IntegerOverflow
//! since 04952fadc.

use std::sync::Arc;

fn conn(dir: &std::path::Path) -> turso_pg::Connection {
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    turso_pg::Connection::new(db.connect().unwrap())
}

fn run(c: &turso_pg::Connection, sql: &str) -> Result<Vec<String>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r[0].to_string()).collect())
            .map_err(|e| e.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[test]
fn a_numeric_nan_is_refused_before_it_reaches_an_integer() {
    let dir = tempfile::TempDir::new().unwrap();
    let c = conn(dir.path());
    let mut wrong = Vec::new();
    let refused = |r: &Result<Vec<String>, String>| matches!(r, Err(e) if e.contains("NaN") && e.contains("not supported"));
    if let Err(e) = c.execute("CREATE TABLE nt (id int PRIMARY KEY, c1 numeric(38,10))") {
        wrong.push(format!("setup: {e}"));
    }
    // PostgreSQL 18.6: ERROR 0A000 for each.
    for sql in [
        "SELECT ('NaN'::numeric)::int",
        "SELECT ('NaN'::numeric)::int8",
        "SELECT ('NaN'::numeric)::int2",
        "SELECT -(ceil(('NaN'::numeric)::int))",
        "UPDATE nt SET c1 = -(ceil(('NaN'::numeric)::int)) WHERE id = 1",
    ] {
        let got = run(&c, sql);
        if !refused(&got) {
            wrong.push(format!("{sql}: {got:?}, PostgreSQL 0A000 cannot convert NaN"));
        }
    }
    if let Err(e) = c.execute("INSERT INTO nt VALUES (1, 5)") {
        wrong.push(format!("setup insert: {e}"));
    }
    let sql = "UPDATE nt SET c1 = -(ceil(('NaN'::numeric)::int)) WHERE id = 1";
    let got = run(&c, sql);
    if !refused(&got) {
        wrong.push(format!("{sql} over a row: {got:?}, PostgreSQL 0A000 cannot convert NaN"));
    }
    match run(&c, "SELECT c1 FROM nt WHERE id = 1") {
        Ok(rows) if rows == ["5.0000000000"] => {}
        other => wrong.push(format!("the row after the refused UPDATE: {other:?}, PostgreSQL 5.0000000000 (recorded, artie uw-pg18-nan_row-rec13-iccad11)")),
    }
    assert!(wrong.is_empty(), "CLAIM: a numeric NaN is refused (0A000) before any integer holds it: {wrong:#?}");
}

#[test]
fn seed_848s_int8_constant_overflow_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let c = conn(dir.path());
    let got = run(&c, "SELECT (((9223372036854775807 + 7) - (-0.87176649)) - 6.156) AS v");
    assert!(
        matches!(&got, Err(e) if e.contains("integer overflow")),
        "CLAIM: seed 848 is refused as an int8 overflow (PostgreSQL 22003): {got:?}"
    );
}
