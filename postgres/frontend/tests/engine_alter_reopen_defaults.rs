//! fastest-engine red (REVIEW-engine-2bdcd32c6 HIGH 3, the rest of engine-rev-qaf-fix2 HIGH C): a
//! PostgreSQL-frontend table whose DEFAULT is not a bare literal could not be reopened after ALTER
//! TABLE ADD or DROP COLUMN. The rewrite printed `DEFAULT nextval ('k_id_seq')` (a `serial` key's
//! DEFAULT) without the parentheses SQLite's DEFAULT grammar needs around anything but a literal, so
//! the reopen and a later RENAME in the same session failed near "(". engine_alter_reopen.rs uses an
//! integer key only, so it cannot see this.
//!
//! Each DEFAULT form below gets its own table with an integer key, so a form is seen alone; a
//! `serial` and a `bigserial` key get their own tables, and the `rename` case has a `bigserial` key, a
//! function-call DEFAULT and RENAMEs in the ALTER's session. `NULL`, `'it''s'`, `true` and
//! CURRENT_TIMESTAMP pass at the base: they are the controls. `-5` and `-2.5` reopen at the base, but
//! their column_default then reads `- 5` (the reparse reads the literal back as minus 5). Each case ADDs, reopens, DROPs, reopens
//! again, and reads every DEFAULT's value through a fresh INSERT after each reopen. A column's
//! information_schema `column_default` must read after each reopen as it did before the first ALTER.

use std::collections::HashMap;
use std::sync::Arc;

enum Step {
    Exec(&'static str),
    /// `SELECT count(*) ...` must answer this many rows.
    Count(String, i64),
    /// The table's `column_default`s: the first call records them, each later one compares every
    /// column still present.
    Defaults(&'static str),
}

/// (column type, DEFAULT expression, a predicate on `c` that its value meets, setup before CREATE)
const FORMS: &[(&str, &str, &str, &str)] = &[
    ("bigint", "abs(-7)", "c = 7", ""),
    ("text", "lower('ABC')", "c = 'abc'", ""),
    ("integer", "coalesce(NULL, 4)", "c = 4", ""),
    ("integer", "-abs(2)", "c = -2", ""),
    ("bigint", "nextval('s_extra')", "c >= 1", "CREATE SEQUENCE s_extra"),
    ("integer", "1 + 2", "c = 3", ""),
    ("integer", "(2 * 3)", "c = 6", ""),
    ("text", "'a' || 'b'", "c = 'ab'", ""),
    ("integer", "CASE WHEN 1 = 1 THEN 8 ELSE 9 END", "c = 8", ""),
    ("text", "'x'::text", "c = 'x'", ""),
    ("integer", "-5", "c = -5", ""),
    ("double precision", "-2.5", "c = -2.5", ""),
    ("text", "'it''s'", "c = 'it''s'", ""),
    ("boolean", "true", "c = true", ""),
    ("integer", "NULL", "c IS NULL", ""),
    ("timestamp", "CURRENT_TIMESTAMP", "c IS NOT NULL", ""),
];

fn form_case(key: &str, ty: &str, default: &str, pred: &str, setup: &str) -> Vec<Vec<Step>> {
    let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
    let mut s1 = Vec::new();
    if !setup.is_empty() {
        s1.push(Step::Exec(leak(setup.to_string())));
    }
    s1.push(Step::Exec(leak(format!("CREATE TABLE t (id {key} PRIMARY KEY, c {ty} DEFAULT {default}, w text)"))));
    s1.push(Step::Exec("INSERT INTO t (w) VALUES ('a')"));
    s1.push(Step::Defaults("t"));
    s1.push(Step::Exec("ALTER TABLE t ADD COLUMN extra integer DEFAULT 3"));
    vec![
        s1,
        vec![
            Step::Count(format!("SELECT count(*) FROM t WHERE id = 1 AND {pred} AND extra = 3"), 1),
            Step::Exec("INSERT INTO t (w) VALUES ('b')"),
            Step::Count(format!("SELECT count(*) FROM t WHERE id = 2 AND {pred} AND extra = 3"), 1),
            Step::Defaults("t"),
            Step::Exec("ALTER TABLE t DROP COLUMN w"),
        ],
        vec![
            Step::Exec("INSERT INTO t (extra) VALUES (4)"),
            Step::Count(format!("SELECT count(*) FROM t WHERE id = 3 AND {pred} AND extra = 4"), 1),
            Step::Count("SELECT count(*) FROM t".to_string(), 3),
            Step::Defaults("t"),
        ],
    ]
}

fn rename_case() -> Vec<Vec<Step>> {
    vec![
        vec![
            Step::Exec("CREATE TABLE m (id bigserial PRIMARY KEY, v text, s text DEFAULT lower('ABC'), w text)"),
            Step::Exec("INSERT INTO m (v) VALUES ('a')"),
            Step::Defaults("m"),
            Step::Exec("ALTER TABLE m ADD COLUMN extra integer DEFAULT 3"),
            Step::Exec("ALTER TABLE m RENAME COLUMN v TO v2"),
            Step::Exec("ALTER TABLE m RENAME TO m2"),
            Step::Exec("INSERT INTO m2 (v2) VALUES ('b')"),
            Step::Count("SELECT count(*) FROM m2 WHERE id = 2 AND s = 'abc' AND extra = 3".to_string(), 1),
        ],
        vec![
            Step::Count("SELECT count(*) FROM m2".to_string(), 2),
            Step::Exec("INSERT INTO m2 (v2) VALUES ('c')"),
            Step::Count("SELECT count(*) FROM m2 WHERE id = 3 AND s = 'abc' AND extra = 3".to_string(), 1),
            Step::Defaults("m2"),
            Step::Exec("ALTER TABLE m2 DROP COLUMN w"),
            Step::Exec("ALTER TABLE m2 RENAME TO m3"),
        ],
        vec![
            Step::Exec("INSERT INTO m3 (v2) VALUES ('d')"),
            Step::Count("SELECT count(*) FROM m3 WHERE id = 4 AND v2 = 'd' AND s = 'abc' AND extra = 3".to_string(), 1),
            Step::Count("SELECT count(*) FROM m3".to_string(), 4),
            Step::Defaults("m3"),
        ],
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

fn run_case(name: &str, sessions: Vec<Vec<Step>>, failed: &mut Vec<String>) {
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
    for (i, session) in sessions.into_iter().enumerate() {
        let (_db, raw) = match open() {
            Ok(x) => x,
            Err(e) => {
                failed.push(format!("{name}: open {i} (reopen {i}) failed: {e}"));
                return;
            }
        };
        let c = turso_pg::Connection::new(raw);
        for step in session {
            match step {
                Step::Exec(sql) => {
                    if let Err(e) = c.execute(sql) {
                        failed.push(format!("{name}: session {i}: `{sql}`: {e}"));
                    }
                }
                Step::Count(sql, want) => match rows(&c, &sql) {
                    Ok(r) => {
                        let got = r.first().and_then(|r| r.first()).cloned().unwrap_or_default();
                        // The cell prints as Debug (`Numeric(Integer(1))`): its digits are the count.
                        if got.chars().filter(|ch| ch.is_ascii_digit()).collect::<String>() != want.to_string() {
                            failed.push(format!("{name}: session {i}: `{sql}` answered {got}, want {want}"));
                        }
                    }
                    Err(e) => failed.push(format!("{name}: session {i}: `{sql}`: {e}")),
                },
                Step::Defaults(table) => {
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
                                        failed.push(format!("{name}: session {i}: {col}'s column_default {dflt} was {before} before the ALTER"));
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

#[test]
fn a_serial_keyed_table_reopens_after_add_and_drop_column_with_every_default_form() {
    let mut failed = Vec::new();
    for (ty, default, pred, setup) in FORMS {
        run_case(&format!("DEFAULT {default}"), form_case("integer", ty, default, pred, setup), &mut failed);
    }
    run_case("serial key", form_case("serial", "text", "'x'", "c = 'x'", ""), &mut failed);
    run_case("bigserial key", form_case("bigserial", "integer", "abs(-7)", "c = 7", ""), &mut failed);
    run_case("rename (bigserial key)", rename_case(), &mut failed);
    assert!(
        failed.is_empty(),
        "CLAIM: a serial-keyed frontend table reopens after ADD and DROP COLUMN, twice, a RENAME works in the ALTER's session, and every DEFAULT keeps its value and its column_default: {failed:#?}"
    );
}

/// The translated AST's own printer: a frontend CREATE TABLE printed as SQLite text (as a
/// schema-qualified table's replay prints it, catalog.rs `table_sql_for_replay`) must parse as
/// SQLite with every DEFAULT form above.
#[test]
fn every_translated_default_form_prints_as_sqlite_that_parses() {
    let mut failed = Vec::new();
    let mut forms: Vec<(String, String)> = FORMS.iter().map(|(ty, d, _, _)| (ty.to_string(), d.to_string())).collect();
    forms.push(("serial".to_string(), String::new()));
    forms.push(("bigserial".to_string(), String::new()));
    for (ty, default) in forms {
        let pg = if default.is_empty() {
            format!("CREATE TABLE t (id {ty} PRIMARY KEY, w text)")
        } else {
            format!("CREATE TABLE t (id integer PRIMARY KEY, c {ty} DEFAULT {default})")
        };
        let stmt = match turso_pg_parser::parse(&pg).map_err(|e| format!("{e:?}")).and_then(|r| {
            turso_pg_parser::translator::PostgreSQLTranslator::new().translate(&r).map_err(|e| format!("{e:?}"))
        }) {
            Ok(s) => s,
            Err(e) => {
                failed.push(format!("`{pg}`: translate: {e}"));
                continue;
            }
        };
        let text = stmt.to_string();
        if let Err(e) = turso_core::dialect::sqlite::parse_table_sql_ast(&text) {
            failed.push(format!("`{pg}` printed `{text}`, which SQLite refuses: {e}"));
        }
    }
    assert!(failed.is_empty(), "CLAIM: every translated DEFAULT prints as SQLite text that parses: {failed:#?}");
}
