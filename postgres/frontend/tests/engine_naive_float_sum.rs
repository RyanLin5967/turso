//! fastest-engine red (lead LEAD-RULINGS 8b41cde54c item 1): under the PostgreSQL dialect a float
//! sum and avg add left to right, uncompensated, as PostgreSQL 18.6's float8pl and float8_accum do
//! (recorded on iccad11, artie raw uw-pg18-naive_float_sum-rec12-iccad11.txt): sum(1e16, 1, -1e16)
//! is 0 and sum(0.1, 0.2, 0.3) is 0.6000000000000001. The engine sums with Kahan-Babuska-Neumaier
//! compensation for every dialect (execute.rs apply_kbn_step), SQLite's rule, which answers 1 and
//! 0.6. Aggregates, GROUP BY and running window sums are checked; each expected value is
//! PostgreSQL's float8, compared exactly. The SQLite dialect keeps its compensated sum: its control.

use std::sync::Arc;

fn floats(c: &turso_pg::Connection, sql: &str, col: usize) -> Result<Vec<f64>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r[col].as_float()).collect())
            .map_err(|e| format!("{e:?}")),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(format!("{e:?}")),
    }
}

#[test]
fn a_float_sum_adds_naively_under_the_postgresql_dialect_only() {
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
    for sql in [
        "CREATE TABLE f (id int PRIMARY KEY, g int, v double precision)",
        "INSERT INTO f VALUES (1, 1, 1e16), (2, 1, 1.0), (3, 1, -1e16), (4, 2, 0.1), (5, 2, 0.2), (6, 2, 0.3)",
    ] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    // (query, result column, PostgreSQL 18.6's answer)
    let cases: [(&str, usize, &[f64]); 9] = [
        ("SELECT sum(v) FROM f WHERE g = 1", 0, &[0.0]),
        ("SELECT avg(v) FROM f WHERE g = 1", 0, &[0.0]),
        ("SELECT sum(v) FROM f WHERE g = 2", 0, &[0.6000000000000001]),
        ("SELECT avg(v) FROM f WHERE g = 2", 0, &[0.20000000000000004]),
        ("SELECT g, sum(v), avg(v) FROM f GROUP BY g ORDER BY g", 1, &[0.0, 0.6000000000000001]),
        ("SELECT g, sum(v), avg(v) FROM f GROUP BY g ORDER BY g", 2, &[0.0, 0.20000000000000004]),
        (
            "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY id) FROM f ORDER BY id",
            1,
            &[1e16, 1e16, 0.0, 0.1, 0.30000000000000004, 0.6000000000000001],
        ),
        ("SELECT sum(v) OVER (PARTITION BY g) FROM f WHERE id IN (3, 6) ORDER BY id", 0, &[-1e16, 0.3]),
        ("SELECT sum(x) FROM (VALUES (1e16::float8), (1.0::float8), (-1e16::float8)) AS t(x)", 0, &[0.0]),
    ];
    for (sql, col, want) in cases {
        match floats(&c, sql, col) {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{sql} (column {col}): {other:?}, PostgreSQL {want:?}")),
        }
    }
    // Control: the SQLite dialect compensates, as SQLite does: 1e16 + 1 - 1e16 is 1.
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
    sc.execute("CREATE TABLE f (id INTEGER PRIMARY KEY, v REAL)").unwrap();
    sc.execute("INSERT INTO f VALUES (1, 1e16), (2, 1.0), (3, -1e16)").unwrap();
    let got = sc.prepare("SELECT sum(v) FROM f").and_then(|mut s| s.run_collect_rows()).map(|r| r[0][0].as_float());
    if got.as_ref().ok() != Some(&1.0) {
        wrong.push(format!("control: the SQLite dialect's sum(1e16, 1, -1e16) is {got:?}, SQLite 1.0"));
    }
    assert!(wrong.is_empty(), "CLAIM: float sums add naively under the PostgreSQL dialect only: {wrong:#?}");
}
