//! fastest-engine red (adversary of the REVIEW-engine-2bdcd32c6 HIGH 3 fix): the SQLite text a
//! PostgreSQL-frontend table is stored in after ALTER ADD/DROP COLUMN or a RENAME must mean what the
//! table meant. pg_query drops parentheses, and the printer wrote operands with no regard to
//! precedence, so a CHECK ((a + b) * 2 < 100) was stored as `a + b * 2 < 100` and, after the reopen,
//! took a row PostgreSQL refuses; a DEFAULT (1 + 2) * 3 would read back as 7. The RENAME case renames
//! a never-ALTERed table with a `serial` key and such a DEFAULT, so the rename's own printer is the
//! one under test. After each reopen the values are read through new rows, the CHECK must refuse,
//! an explicit NULL key must be refused as NOT NULL, and every column_default reads as before.

use std::collections::HashMap;
use std::sync::Arc;

enum Step {
    Exec(&'static str),
    /// `SELECT count(*) ...` must answer this many rows.
    Count(&'static str, i64),
    /// The statement must fail, its error naming this (lowercase) text.
    Refused(&'static str, &'static str),
    /// The table's column_defaults: the first call records them, each later one compares every
    /// column still present.
    Defaults(&'static str),
}
use Step::*;

const P_ROW: &str = "d1 = 9 AND d2 = -3 AND d3 = 8 AND d4 = 'a3' AND d5 = 1";

fn cases() -> Vec<(&'static str, Vec<Vec<Step>>)> {
    let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
    let p_id = |id: i64| leak(format!("SELECT count(*) FROM p WHERE id = {id} AND {P_ROW}"));
    vec![
        (
            "alter: precedence DEFAULTs and CHECK",
            vec![
                vec![
                    Exec("CREATE TABLE p (id integer PRIMARY KEY, a integer, b integer, d1 integer DEFAULT (1 + 2) * 3, d2 integer DEFAULT -(1 + 2), d3 integer DEFAULT 10 - (3 - 1), d4 text DEFAULT 'a' || 1 + 2, d5 integer DEFAULT (2 + 3) % 4, w text, CHECK ((a + b) * 2 < 100))"),
                    Exec("INSERT INTO p (id, a, b) VALUES (1, 10, 20)"),
                    Count(p_id(1), 1),
                    Refused("INSERT INTO p (id, a, b) VALUES (9, 40, 20)", "check"),
                    Defaults("p"),
                    Exec("ALTER TABLE p ADD COLUMN extra integer DEFAULT 3"),
                ],
                vec![
                    Count(p_id(1), 1),
                    Exec("INSERT INTO p (id, a, b) VALUES (2, 10, 20)"),
                    Count(p_id(2), 1),
                    Refused("INSERT INTO p (id, a, b) VALUES (9, 40, 20)", "check"),
                    Defaults("p"),
                    Exec("ALTER TABLE p DROP COLUMN w"),
                ],
                vec![
                    Exec("INSERT INTO p (id, a, b) VALUES (3, 10, 20)"),
                    Count(p_id(3), 1),
                    Refused("INSERT INTO p (id, a, b) VALUES (9, 40, 20)", "check"),
                    Count("SELECT count(*) FROM p", 3),
                    Defaults("p"),
                ],
            ],
        ),
        (
            "alter: CHECK alone (no DEFAULT)",
            vec![
                vec![
                    Exec("CREATE TABLE q (id integer PRIMARY KEY, a integer, b integer, w text, CHECK ((a + b) * 2 < 100))"),
                    Exec("ALTER TABLE q ADD COLUMN extra integer"),
                ],
                vec![
                    Refused("INSERT INTO q (id, a, b) VALUES (1, 40, 20)", "check"),
                    Exec("INSERT INTO q (id, a, b) VALUES (2, 10, 20)"),
                    Exec("ALTER TABLE q DROP COLUMN w"),
                ],
                vec![
                    Refused("INSERT INTO q (id, a, b) VALUES (3, 40, 20)", "check"),
                    Count("SELECT count(*) FROM q", 1),
                ],
            ],
        ),
        (
            "rename: a never-ALTERed serial-keyed table with a precedence DEFAULT",
            vec![
                vec![
                    Exec("CREATE TABLE s (id serial PRIMARY KEY, v text, d integer DEFAULT (1 + 2) * 3)"),
                    Exec("INSERT INTO s (v) VALUES ('a')"),
                    Defaults("s"),
                    Exec("ALTER TABLE s RENAME TO s2"),
                ],
                vec![
                    Count("SELECT count(*) FROM s2 WHERE id = 1 AND d = 9", 1),
                    Exec("INSERT INTO s2 (v) VALUES ('b')"),
                    Count("SELECT count(*) FROM s2 WHERE id = 2 AND d = 9", 1),
                    Refused("INSERT INTO s2 (id, v) VALUES (NULL, 'x')", "not null"),
                    Defaults("s2"),
                    Exec("ALTER TABLE s2 RENAME COLUMN v TO v2"),
                ],
                vec![
                    Exec("INSERT INTO s2 (v2) VALUES ('c')"),
                    Count("SELECT count(*) FROM s2 WHERE id = 3 AND d = 9 AND v2 = 'c'", 1),
                    Refused("INSERT INTO s2 (id, v2) VALUES (NULL, 'x')", "not null"),
                    Defaults("s2"),
                ],
            ],
        ),
    ]
}

fn rows(c: &turso_pg::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    match c.query(sql) {
        Ok(Some(mut st)) => st
            .run_collect_rows()
            .map(|rs| rs.iter().map(|r| r.iter().map(|v| format!("{v:?}")).collect()).collect())
            .map_err(|e| e.to_string()),
        Ok(None) => Err("no statement".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

#[test]
fn a_reopened_frontend_table_keeps_what_its_expressions_mean() {
    let mut failed = Vec::new();
    for (name, sessions) in cases() {
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
        let mut defaults: HashMap<String, String> = HashMap::new();
        'case: for (i, session) in sessions.into_iter().enumerate() {
            let (_db, raw) = match open() {
                Ok(x) => x,
                Err(e) => {
                    failed.push(format!("{name}: open {i} failed: {e}"));
                    break 'case;
                }
            };
            let c = turso_pg::Connection::new(raw);
            for step in session {
                match step {
                    Exec(sql) => {
                        if let Err(e) = c.execute(sql) {
                            failed.push(format!("{name}: session {i}: `{sql}`: {e}"));
                        }
                    }
                    Count(sql, want) => match rows(&c, sql) {
                        Ok(r) => {
                            let got = r.first().and_then(|r| r.first()).cloned().unwrap_or_default();
                            // The cell prints as Debug (`Numeric(Integer(1))`): its digits are the count.
                            if got.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>() != want.to_string() {
                                failed.push(format!("{name}: session {i}: `{sql}` answered {got}, want {want}"));
                            }
                        }
                        Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
                    },
                    Refused(sql, needle) => match c.execute(sql) {
                        Ok(()) => failed.push(format!("{name}: session {i}: `{sql}` was taken")),
                        Err(e) if !e.to_string().to_lowercase().contains(needle) => {
                            failed.push(format!("{name}: session {i}: `{sql}` failed for another reason: {e}"))
                        }
                        Err(_) => {}
                    },
                    Defaults(table) => {
                        let sql = format!("SELECT column_name, column_default FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position");
                        match rows(&c, &sql) {
                            Ok(r) if r.is_empty() => failed.push(format!("{name}: session {i}: information_schema has no column of {table}")),
                            Ok(r) => {
                                let first = defaults.is_empty();
                                for row in r {
                                    let (col, dflt) = (row[0].clone(), row[1].clone());
                                    if first {
                                        defaults.insert(col, dflt);
                                    } else if let Some(before) = defaults.get(&col) {
                                        if *before != dflt {
                                            failed.push(format!("{name}: session {i}: {col}'s column_default {dflt} was {before} before"));
                                        }
                                    }
                                }
                            }
                            Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
                        }
                    }
                }
            }
        }
    }
    assert!(
        failed.is_empty(),
        "CLAIM: after ALTER ADD/DROP COLUMN and RENAME, and each reopen, a frontend table's DEFAULTs and CHECK mean what they meant: {failed:#?}"
    );
}
