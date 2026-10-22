//! fastest-engine confirmation, UNRUN (UW unavailable; lead order 2026-10-22T10:31Z): the
//! non-constant and no-row division-by-zero seeds wire's numfuzz found at d5600475b (shards
//! s2821-3020 and s3021-3220), against 60c99ec89's division_by_zero_raises. PostgreSQL 18.6
//! answered 22012 for each (the fuzzer's own PG answer). Seed 2999 and 3042 run over an EMPTY
//! table: PostgreSQL folds the constant at plan time and raises with no row; here the constant is
//! hoisted before the loop, so it raises with no row too. Seed 3006 is in no banked shard and is
//! not covered.

use std::sync::Arc;

#[test]
fn the_numfuzz_division_by_zero_seeds_raise_under_the_postgresql_dialect() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    let run = |sql: &str| -> Result<(), String> {
        match c.query(sql) {
            Ok(Some(mut st)) => st.run_collect_rows().map(|_| ()).map_err(|e| e.to_string()),
            Ok(None) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    };
    let mut wrong = Vec::new();
    for (seed, setup, sql) in [
        (2976, "CREATE TABLE nt1 (id int PRIMARY KEY, c0 int)", "WITH w AS (SELECT 0 AS v) SELECT v, -((-(v) / -(v))) AS u FROM w"),
        (2999, "CREATE TABLE nt (id int PRIMARY KEY, c0 int, c1 bigint, c2 numeric, c3 int)", "SELECT id, (ceil(((-1) / 0)))::text AS v FROM nt ORDER BY id"),
        (
            3042,
            "CREATE TABLE nt3 (id int PRIMARY KEY, c0 numeric(38,10), c1 real, c2 bigint)",
            "UPDATE nt3 SET c2 = ('-0.000001'::numeric % (trunc((-0.27)))::int) WHERE id = 1",
        ),
    ] {
        if let Err(e) = run(setup) {
            wrong.push(format!("seed {seed} setup: {e}"));
            continue;
        }
        match run(sql) {
            Err(e) if e.contains("division by zero") => {}
            other => wrong.push(format!("seed {seed}: {sql}: {other:?}, PostgreSQL 22012 division by zero")),
        }
    }
    assert!(wrong.is_empty(), "CLAIM: the division-by-zero seeds raise 22012: {wrong:#?}");
}
