//! fastest-engine red (engine-rev-qaf-fix2 HIGH C, REVIEW-engine-qaf-fix2-18556a956): in the
//! PostgreSQL frontend one ADD or DROP COLUMN made the database unopenable. Core's rewrite stored
//! SQLite-printed text (`... STRICT`) under the frontend's own prefix, and the frontend's parser then
//! refused it at the reopen ("syntax error at or near "STRICT""). Each ALTER is reopened and read.

use std::sync::Arc;

#[test]
fn a_frontend_table_reopens_after_add_and_drop_column() {
    let cases: &[(&str, &[&str])] = &[
        ("add", &["CREATE TABLE k (id integer PRIMARY KEY, v text)", "INSERT INTO k (id, v) VALUES (1, 'a')", "ALTER TABLE k ADD COLUMN extra integer DEFAULT 3"]),
        ("drop", &["CREATE TABLE k (id integer PRIMARY KEY, v text, w text)", "INSERT INTO k (id, v, w) VALUES (1, 'a', 'b')", "ALTER TABLE k DROP COLUMN w"]),
    ];
    let mut failed = Vec::new();
    for (name, setup) in cases {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("pg.db");
        let p = path.to_str().unwrap().to_string();
        let open = || {
            turso_pg::open_database_with_io(
                Arc::new(turso_core::PlatformIO::new().unwrap()),
                &p,
                turso_core::OpenFlags::default(),
                turso_core::DatabaseOpts::new().with_custom_types(true),
            )
        };
        {
            let db = open().unwrap();
            let c = turso_pg::Connection::new(db.connect().unwrap());
            for sql in *setup {
                if let Err(e) = c.execute(sql) {
                    failed.push(format!("{name}: `{sql}`: {e}"));
                }
            }
        }
        match open().and_then(|db| db.connect().map(|c| (db, c))) {
            Ok((_db, raw)) => {
                let c = turso_pg::Connection::new(raw);
                match c.query("SELECT count(*) FROM k") {
                    Ok(Some(mut st)) => {
                        if st.run_collect_rows().map(|r| r.len()).unwrap_or(0) != 1 {
                            failed.push(format!("{name}: SELECT after the reopen read no row"));
                        }
                    }
                    other => failed.push(format!("{name}: SELECT after the reopen: {:?}", other.err())),
                }
            }
            Err(e) => failed.push(format!("{name}: the reopen failed: {e}")),
        }
    }
    assert!(failed.is_empty(), "CLAIM: the frontend's table reopens after ADD and DROP COLUMN: {failed:#?}");
}
