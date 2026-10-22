//! fastest-engine red (RESUME item 13, fastest-wire numfuzz seed 2159; SILENT): under the
//! PostgreSQL dialect a numeric cast to an integer rounds half away from zero, as PostgreSQL 18.6
//! does (recorded on iccad11, artie raw uw-pg18-numeric_to_int-rec14-iccad11.txt): ' 2.50 '::int
//! is 3, -2.5 is -3, 0.5 is 1, -0.5 is -1, for int2, int4 and int8, in a constant, a column, and an
//! INSERT ... SELECT into an integer column; a rounded value past the width is 22003. Seed 2159's
//! `((trunc(566.309) * (' 2.50 '::numeric)::int))::int` is 1698. The engine truncates (SQLite's
//! CAST), so seed 2159 answered 1132.
//!
//! The second test is float8, a different rule: PostgreSQL rounds a float8 to an integer half to
//! EVEN (rint): 2.5 is 2, 3.5 is 4, -2.5 is -2 (recorded in the same file).

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

fn rows(c: &turso_pg::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r.iter().map(|v| v.to_string()).collect()).collect())
            .map_err(|e| e.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn row(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn a_numeric_cast_to_an_integer_rounds_half_away_from_zero_under_the_postgresql_dialect() {
    let dir = tempfile::TempDir::new().unwrap();
    let c = conn(dir.path());
    let mut wrong = Vec::new();
    for sql in [
        "CREATE TABLE n (id int PRIMARY KEY, v numeric(10,2))",
        "INSERT INTO n VALUES (1, 2.50), (2, -2.50), (3, 7.49), (4, 7.50)",
        "CREATE TABLE i (id int PRIMARY KEY, x int)",
        "INSERT INTO i SELECT id, v FROM n",
    ] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    let cases: Vec<(&str, Vec<Vec<String>>)> = vec![
        ("SELECT (' 2.50 '::numeric)::int", vec![row(&["3"])]),
        (
            "SELECT ('2.5'::numeric)::int, ('-2.5'::numeric)::int, ('3.5'::numeric)::int, ('2.4999'::numeric)::int, ('-0.5'::numeric)::int, ('0.5'::numeric)::int",
            vec![row(&["3", "-3", "4", "2", "-1", "1"])],
        ),
        ("SELECT ('-2.5'::numeric)::int2, ('2.5'::numeric)::int8, ('-3.5'::numeric)::int8", vec![row(&["-3", "3", "-4"])]),
        ("SELECT ('-2147483648.4'::numeric)::int", vec![row(&["-2147483648"])]),
        ("SELECT 2.5::int, CAST(-2.5 AS integer), CAST(2.5 AS bigint)", vec![row(&["3", "-3", "3"])]),
        ("SELECT ((trunc(000566.309) * (' 2.50 '::numeric)::int))::int AS v", vec![row(&["1698"])]),
        (
            "SELECT id, v::int, v::int2, v::int8 FROM n ORDER BY id",
            vec![row(&["1", "3", "3", "3"]), row(&["2", "-3", "-3", "-3"]), row(&["3", "7", "7", "7"]), row(&["4", "8", "8", "8"])],
        ),
        ("SELECT id, x FROM i ORDER BY id", vec![row(&["1", "3"]), row(&["2", "-3"]), row(&["3", "7"]), row(&["4", "8"])]),
    ];
    for (sql, want) in cases {
        match rows(&c, sql) {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want:?}")),
        }
    }
    // PostgreSQL 18.6: 22003 (smallint / integer out of range) once rounded past the width.
    for sql in ["SELECT ('32767.5'::numeric)::int2", "SELECT ('2147483647.5'::numeric)::int"] {
        match rows(&c, sql) {
            Err(e) if e.contains("out of range") || e.contains("integer overflow") => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL 22003 out of range")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: numeric to integer rounds half away from zero: {wrong:#?}");
}

#[test]
fn a_float8_cast_to_an_integer_rounds_half_to_even_under_the_postgresql_dialect() {
    let dir = tempfile::TempDir::new().unwrap();
    let c = conn(dir.path());
    let got = rows(&c, "SELECT 2.5::float8::int, 3.5::float8::int, (-2.5)::float8::int");
    assert_eq!(got, Ok(vec![row(&["2", "4", "-2"])]), "CLAIM: float8 to integer rounds half to even (PostgreSQL's rint)");
}
