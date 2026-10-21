//! fastest-engine red (found fixing REVIEW-engine-2bdcd32c6 HIGH 3): in the PostgreSQL frontend a
//! RENAME TO or RENAME COLUMN stored the renamed table's SQLite-printed text (`... STRICT`) under the
//! frontend's raw PostgreSQL prefix, so the next reader of the row, a reopen or a second RENAME in the
//! same session, refused it with the PostgreSQL parser ("syntax error at or near "STRICT""). The
//! table has an integer key and no DEFAULT, so HIGH 3's DEFAULT printing plays no part. After each
//! reopen the rows are read, a new row is written, and an explicit NULL key is still refused (the
//! frontend's NOT NULL key survives the rewrite).

use std::sync::Arc;

fn count(c: &turso_pg::Connection, sql: &str) -> Result<String, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|r| r.first().and_then(|r| r.first()).map(|v| format!("{v}")).unwrap_or_default())
            .map_err(|e| e.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[test]
fn a_frontend_table_reopens_after_rename_to_and_rename_column() {
    // (case, sessions): each session is (statements, (count query, answer)...); a reopen between.
    type Session = (&'static [&'static str], &'static [(&'static str, &'static str)]);
    let cases: &[(&str, &[Session])] = &[
        (
            "rename column, reopen, rename to, reopen",
            &[
                (&["CREATE TABLE r (id integer PRIMARY KEY, v text)", "INSERT INTO r (id, v) VALUES (1, 'a')", "ALTER TABLE r RENAME COLUMN v TO v2"], &[]),
                (&["ALTER TABLE r RENAME TO r2"], &[("SELECT count(*) FROM r2 WHERE id = 1 AND v2 = 'a'", "1")]),
                (&["INSERT INTO r2 (id, v2) VALUES (2, 'b')"], &[("SELECT count(*) FROM r2 WHERE v2 IN ('a', 'b')", "2")]),
            ],
        ),
        (
            "rename to and rename column in one session, reopen twice",
            &[
                (&["CREATE TABLE r (id integer PRIMARY KEY, v text)", "INSERT INTO r (id, v) VALUES (1, 'a')", "ALTER TABLE r RENAME TO r2", "ALTER TABLE r2 RENAME COLUMN v TO v2"], &[("SELECT count(*) FROM r2 WHERE v2 = 'a'", "1")]),
                (&["INSERT INTO r2 (id, v2) VALUES (2, 'b')"], &[("SELECT count(*) FROM r2", "2")]),
                (&[], &[("SELECT count(*) FROM r2 WHERE v2 IN ('a', 'b')", "2")]),
            ],
        ),
    ];
    let mut failed = Vec::new();
    for (name, sessions) in cases {
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
            .and_then(|db| db.connect().map(|c| (db, c)))
        };
        for (i, (stmts, checks)) in sessions.iter().enumerate() {
            let (_db, raw) = match open() {
                Ok(x) => x,
                Err(e) => {
                    failed.push(format!("{name}: open {i} failed: {e}"));
                    break;
                }
            };
            let c = turso_pg::Connection::new(raw);
            // Every reopen: the frontend's key still refuses an explicit NULL (23502).
            if i > 0 {
                let t = if count(&c, "SELECT count(*) FROM r2").is_ok() { "r2" } else { "r" };
                if c.execute(format!("INSERT INTO {t} (id) VALUES (NULL)")).is_ok() {
                    failed.push(format!("{name}: session {i}: an explicit NULL key was taken"));
                }
            }
            for sql in *stmts {
                if let Err(e) = c.execute(sql) {
                    failed.push(format!("{name}: session {i}: `{sql}`: {e}"));
                }
            }
            for (sql, want) in *checks {
                match count(&c, sql) {
                    Ok(got) if got == *want => {}
                    Ok(got) => failed.push(format!("{name}: session {i}: `{sql}` answered {got}, want {want}")),
                    Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
                }
            }
        }
    }
    assert!(failed.is_empty(), "CLAIM: a frontend table reopens after RENAME TO and RENAME COLUMN, and a second RENAME reads the first's row: {failed:#?}");
}
