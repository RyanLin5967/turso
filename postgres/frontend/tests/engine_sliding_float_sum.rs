//! fastest-engine red (LEAD-RULINGS ef1e848258 item 3): under the PostgreSQL dialect a float sum
//! or avg over a sliding window frame is the naive sum of the frame's rows, as PostgreSQL 18.6
//! computes it (no inverse transition for float sums; recorded on iccad11, artie raw
//! uw-pg18-sliding_window_sum-rec14-iccad11.txt). The engine keeps one running total per window and
//! subtracts each row as it leaves the frame (SQLite's xInverse). A running float total that has
//! absorbed 1e16 and then subtracted it does not give back the frame's own naive sum. An integer
//! sliding sum is exact either way: the control.

use std::sync::Arc;

fn floats(c: &turso_pg::Connection, sql: &str) -> Result<Vec<f64>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r[1].as_float()).collect())
            .map_err(|e| format!("{e:?}")),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(format!("{e:?}")),
    }
}

#[test]
fn a_sliding_float_sum_is_the_naive_sum_of_its_frame_under_the_postgresql_dialect() {
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
        "CREATE TABLE w (id int PRIMARY KEY, v float8, k int)",
        "INSERT INTO w VALUES (1, 1e16, 1), (2, 1, 2), (3, -1e16, 3), (4, 1, 4), (5, 0.1, 5), (6, 0.2, 6), (7, 0.3, 7)",
    ] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    let cases: [(&str, [f64; 7]); 5] = [
        (
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            [1e16, 1e16, -1e16, -1e16, 1.1, 0.30000000000000004, 0.5],
        ),
        (
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            [1e16, 1e16, 0.0, -1e16, -1e16, 1.3, 0.6000000000000001],
        ),
        (
            "SELECT id, avg(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            [1e16, 5e15, 0.0, -3.3333333333333335e15, -3.3333333333333335e15, 0.43333333333333335, 0.20000000000000004],
        ),
        (
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM w ORDER BY id",
            [1e16, -1e16, -1e16, 1.1, 0.30000000000000004, 0.5, 0.3],
        ),
        (
            "SELECT id, sum(k) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id",
            [1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0],
        ),
    ];
    for (sql, want) in cases {
        match floats(&c, sql) {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want:?}")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: a sliding float sum is the naive sum of its frame: {wrong:#?}");
}
