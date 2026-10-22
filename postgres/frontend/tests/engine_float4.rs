//! fastest-engine red (RESUME item 11, fastest-wire numfuzz seeds 339 and 343): under the
//! PostgreSQL dialect a `real` (float4) is a float4. A real column stores its value rounded to
//! float4, and + - * / and sum over two reals compute in float4, as PostgreSQL 18.6 does (recorded
//! on iccad11 and eceubuntu2, artie b7d6007cd2 and b09c790590; the float8 widenings of sum and of
//! + - * / at artie 3331ca89b0). The engine maps real to SQLite's REAL, an f64, so it
//! stored 0.708719602195 unrounded and summed in f64: sum answered -98.10626 where PostgreSQL
//! answers -98.10627 (seed 339), and 0.1 and 0.10000000149 stayed two values where PostgreSQL has
//! one float4 (seed 343's class). Every expected value is PostgreSQL's float8 widening of its
//! float4 answer, compared exactly as an f64. The SQLite dialect keeps its REAL: its control.

use std::sync::Arc;

fn open(dir: &std::path::Path) -> turso_pg::Connection {
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    turso_pg::Connection::new(db.connect().unwrap())
}

fn floats(c: &turso_pg::Connection, sql: &str) -> Result<Vec<f64>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rows| rows.iter().map(|r| r[0].as_float()).collect())
            .map_err(|e| format!("{e:?}")),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(format!("{e:?}")),
    }
}

#[test]
fn a_real_is_stored_and_computed_as_a_float4_under_the_postgresql_dialect() {
    let dir = tempfile::TempDir::new().unwrap();
    let c = open(dir.path());
    let mut wrong = Vec::new();
    for sql in [
        "CREATE TABLE nt (id int PRIMARY KEY, c1 real)",
        "INSERT INTO nt (id, c1) VALUES (1, 0.708719602195), (4, '-98.8149842867718753')",
        "CREATE TABLE d (id int PRIMARY KEY, v real)",
        "INSERT INTO d VALUES (1, 0.1), (2, 0.10000000149), (3, 1.70e10)",
    ] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    // (query, PostgreSQL 18.6's answer as float8)
    let cases: [(&str, &[f64]); 10] = [
        ("SELECT c1::float8 FROM nt ORDER BY id", &[0.7087196111679077, -98.81498718261719]),
        ("SELECT sum(c1)::float8 AS v FROM nt", &[-98.10626983642578]),
        ("SELECT (0.708719602195::real + '-98.8149842867718753'::real)::float8", &[-98.10626983642578]),
        ("SELECT (c1 + c1)::float8 FROM nt ORDER BY id", &[1.4174392223358154, -197.62997436523438]),
        ("SELECT (c1 * c1)::float8 FROM nt ORDER BY id", &[0.50228351354599, 9764.4013671875]),
        ("SELECT (c1 - 1::real)::float8 FROM nt ORDER BY id", &[-0.2912803888320923, -99.81498718261719]),
        ("SELECT (c1 / 3::real)::float8 FROM nt ORDER BY id", &[0.23623986542224884, -32.93832778930664]),
        ("SELECT avg(c1) AS v FROM nt", &[-49.05313378572464]),
        ("SELECT v::float8 FROM d ORDER BY id", &[0.10000000149011612, 0.10000000149011612, 16999999488.0]),
        ("SELECT count(DISTINCT v) FROM d WHERE id IN (1, 2)", &[1.0]),
    ];
    for (sql, want) in cases {
        match floats(&c, sql) {
            Ok(got) if got == want => {}
            other => wrong.push(format!("{sql}: {other:?}, PostgreSQL {want:?}")),
        }
    }
    match floats(&c, "SELECT count(*) FROM d WHERE v = 0.1::real") {
        Ok(got) if got == [2.0] => {}
        other => wrong.push(format!("v = 0.1::real counted {other:?}, PostgreSQL 2")),
    }
    // Control: the SQLite dialect's REAL is an f64, unrounded.
    let sqlite = turso_core::Database::open_file_with_flags(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("sqlite.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(turso_core::SqliteDialect),
    )
    .unwrap()
    .connect()
    .unwrap();
    sqlite.execute("CREATE TABLE nt (id INTEGER PRIMARY KEY, c1 REAL)").unwrap();
    sqlite.execute("INSERT INTO nt VALUES (1, 0.708719602195)").unwrap();
    let mut st = sqlite.prepare("SELECT c1 FROM nt").unwrap();
    let got = st.run_collect_rows().unwrap()[0][0].as_float();
    if got != 0.708719602195 {
        wrong.push(format!("control: the SQLite dialect's REAL read {got}, not 0.708719602195"));
    }
    assert!(wrong.is_empty(), "CLAIM: a real is a float4 under the PostgreSQL dialect: {wrong:#?}");
}
