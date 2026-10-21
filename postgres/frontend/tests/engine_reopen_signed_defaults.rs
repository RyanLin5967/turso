//! fastest-engine red (second adversary of the REVIEW-engine-2bdcd32c6 HIGH 3 fix): PostgreSQL folds
//! a leading `-` into a number constant but not a leading `+`, so `DEFAULT + -5` translates to plus
//! over the literal -5. SQLite's DEFAULT grammar takes only an unsigned term after a sign, so that
//! DEFAULT printed bare (`DEFAULT + -5`) made the table unopenable after ALTER ADD/DROP COLUMN or a
//! RENAME. Each case ADDs, reopens, RENAMEs (a never-ALTERed table in the `rename` case), reopens,
//! and reads the DEFAULT's value through new rows and its column_default after each reopen.

use std::collections::HashMap;
use std::sync::Arc;

/// (column type, DEFAULT, predicate on c)
const FORMS: &[(&str, &str, &str)] = &[
    ("integer", "+ -5", "c = -5"),
    ("double precision", "+ -5.5", "c = -5.5"),
    ("integer", "+ 5", "c = 5"),
    ("integer", "-(-5)", "c = 5"),
    ("integer", "- -5", "c = 5"),
];

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

/// Runs `sessions` (statements, then `SELECT count(*)` checks answering 1), reopening between them,
/// and compares `table`'s column_defaults after each reopen with the first session's.
fn run(name: &str, sessions: &[(Vec<String>, Vec<String>, &str)], failed: &mut Vec<String>) {
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
    for (i, (stmts, checks, table)) in sessions.iter().enumerate() {
        let (_db, raw) = match open() {
            Ok(x) => x,
            Err(e) => {
                failed.push(format!("{name}: open {i} failed: {e}"));
                return;
            }
        };
        let c = turso_pg::Connection::new(raw);
        for sql in stmts {
            if let Err(e) = c.execute(sql) {
                failed.push(format!("{name}: session {i}: `{sql}`: {e}"));
            }
        }
        for sql in checks {
            match rows(&c, sql) {
                Ok(r) => {
                    let got = r.first().and_then(|r| r.first()).cloned().unwrap_or_default();
                    if got.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>() != "1" {
                        failed.push(format!("{name}: session {i}: `{sql}` answered {got}, want 1"));
                    }
                }
                Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
            }
        }
        let sql = format!("SELECT column_name, column_default FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position");
        match rows(&c, &sql) {
            Ok(r) if r.is_empty() => failed.push(format!("{name}: session {i}: no column of {table}")),
            Ok(r) => {
                let first = defaults.is_empty();
                for row in r {
                    if first {
                        defaults.insert(row[0].clone(), row[1].clone());
                    } else if let Some(before) = defaults.get(&row[0]) {
                        if *before != row[1] {
                            failed.push(format!("{name}: session {i}: {}'s column_default {} was {before}", row[0], row[1]));
                        }
                    }
                }
            }
            Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
        }
    }
}

#[test]
fn a_signed_default_reopens_after_alter_and_rename() {
    let mut failed = Vec::new();
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    for (ty, default, pred) in FORMS {
        let alter = [
            (
                vec![format!("CREATE TABLE t (id integer PRIMARY KEY, c {ty} DEFAULT {default}, w text)"), "INSERT INTO t (id) VALUES (1)".to_string()],
                vec![format!("SELECT count(*) FROM t WHERE id = 1 AND {pred}")],
                "t",
            ),
            (
                s(&["ALTER TABLE t ADD COLUMN extra integer"]),
                vec![format!("SELECT count(*) FROM t WHERE id = 1 AND {pred}")],
                "t",
            ),
            (
                s(&["INSERT INTO t (id) VALUES (2)", "ALTER TABLE t DROP COLUMN w"]),
                vec![format!("SELECT count(*) FROM t WHERE id = 2 AND {pred}")],
                "t",
            ),
            (s(&["INSERT INTO t (id) VALUES (3)"]), vec![format!("SELECT count(*) FROM t WHERE id = 3 AND {pred}")], "t"),
        ];
        run(&format!("alter, DEFAULT {default}"), &alter, &mut failed);
        let rename = [
            (
                vec![format!("CREATE TABLE r (id serial PRIMARY KEY, c {ty} DEFAULT {default}, v text)"), "INSERT INTO r (v) VALUES ('a')".to_string()],
                vec![format!("SELECT count(*) FROM r WHERE id = 1 AND {pred}")],
                "r",
            ),
            (s(&["ALTER TABLE r RENAME TO r2"]), vec![format!("SELECT count(*) FROM r2 WHERE id = 1 AND {pred}")], "r2"),
            (
                s(&["INSERT INTO r2 (v) VALUES ('b')", "ALTER TABLE r2 RENAME COLUMN v TO v2"]),
                vec![format!("SELECT count(*) FROM r2 WHERE id = 2 AND {pred}")],
                "r2",
            ),
            (s(&["INSERT INTO r2 (v2) VALUES ('c')"]), vec![format!("SELECT count(*) FROM r2 WHERE id = 3 AND {pred}")], "r2"),
        ];
        run(&format!("rename, DEFAULT {default}"), &rename, &mut failed);
    }
    assert!(failed.is_empty(), "CLAIM: a signed DEFAULT reopens after ALTER and RENAME, keeping its value and column_default: {failed:#?}");
}
