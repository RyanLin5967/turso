//! fastest-engine red (RESUME item 15, fastest-wire numfuzz seeds 2551 and 2730): under the
//! PostgreSQL dialect trunc, floor, ceil, ceiling, round and sign of an integer resolve over
//! float8 (pg_typeof: double precision; recorded on iccad11, artie raws
//! uw-pg18-trunc_floor_int-rec14-iccad11.txt and uw-pg18-int_math_float-rec15-iccad11.txt), so
//! what follows computes in float8: trunc(7) / 2 is 3.5, and seed 2730's trunc(31) /
//! -9223372036854775808 is -3.3610267347050637e-18. The engine returns the integer itself
//! (SQLite's rule for ceil, floor and trunc of an integer), so integer division followed: 3, and
//! 0. abs keeps the integer, in PostgreSQL too: its control.
//!
//! The value is the engine's. The result type the wire reports (RowDescription oid 701) is the
//! frontend's (fastest-wire's function resolution, 5d2f9d25f).

use std::sync::Arc;

fn floats(c: &turso_pg::Connection, sql: &str) -> Result<Vec<Vec<f64>>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r.iter().map(|v| v.as_float()).collect()).collect())
            .map_err(|e| e.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[test]
fn integer_trunc_floor_ceil_round_and_sign_are_float8_under_the_postgresql_dialect() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    let mut wrong = Vec::new();
    for sql in ["CREATE TABLE t (id int PRIMARY KEY, x int)", "INSERT INTO t VALUES (1, 7), (2, -7)"] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    let cases: Vec<(&str, Vec<Vec<f64>>)> = vec![
        (
            "SELECT trunc(7) / 2, floor(7) / 2, ceil(7) / 2, round(7) / 2, sign(5) / 2, abs(5) / 2",
            vec![vec![3.5, 3.5, 3.5, 3.5, 0.5, 2.0]],
        ),
        (
            "SELECT trunc(5::bigint) / 2, floor(5::smallint) / 2, ceiling(-7) / 2, trunc(-7) / 2",
            vec![vec![2.5, 2.5, -3.5, -3.5]],
        ),
        ("SELECT (trunc(0x1F) / (-9223372036854775808)) AS v", vec![vec![-3.3610267347050637e-18]]),
        ("WITH w AS (SELECT 2147483648 AS v) SELECT floor((v * v)) - 4611686018427387903 AS d FROM w", vec![vec![0.0]]),
        (
            "SELECT id, trunc(x) / 2, floor(x) / 2, ceil(x) / 2 FROM t ORDER BY id",
            vec![vec![1.0, 3.5, 3.5, 3.5], vec![2.0, -3.5, -3.5, -3.5]],
        ),
    ];
    for (sql, want) in cases {
        match floats(&c, sql) {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want:?}")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: integer trunc/floor/ceil/round/sign resolve over float8: {wrong:#?}");
}
