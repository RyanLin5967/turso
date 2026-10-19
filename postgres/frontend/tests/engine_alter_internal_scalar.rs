//! fastest-engine red (engine-rev 2bdcd32c6 MED 5, REVIEW-engine-2bdcd32c6): the internal scalar
//! `turso_alter_column_sql`, which only the engine's own ALTER TABLE UPDATE of sqlite_schema needs,
//! resolved from user SQL in the PostgreSQL frontend too (`SELECT turso_alter_column_sql(...)`
//! answered `CREATE TABLE t (a, b INT)`). User SQL is refused it; ADD and DROP COLUMN, which use it,
//! still work and reopen.

use std::sync::Arc;

#[test]
fn frontend_sql_cannot_call_the_alter_rewrite_scalar() {
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
    let mut failed = Vec::new();
    {
        let db = open().unwrap();
        let c = turso_pg::Connection::new(db.connect().unwrap());
        let sql = "SELECT turso_alter_column_sql('CREATE TABLE t(a)', 2, 'add', 't', '', 'b INT', '')";
        match c.query(sql) {
            Ok(Some(mut st)) => match st.run_collect_rows() {
                Ok(rows) => failed.push(format!("`{sql}` was answered: {rows:?}")),
                Err(e) if e.to_string().contains("turso_alter_column_sql") => {}
                Err(e) => failed.push(format!("`{sql}` failed, but not naming the function: {e}")),
            },
            Ok(None) => failed.push(format!("`{sql}` prepared no statement")),
            Err(e) if e.to_string().contains("turso_alter_column_sql") => {}
            Err(e) => failed.push(format!("`{sql}` failed, but not naming the function: {e}")),
        }
        for sql in [
            "CREATE TABLE k (id integer PRIMARY KEY, v text, w text)",
            "INSERT INTO k (id, v, w) VALUES (1, 'a', 'b')",
            "ALTER TABLE k ADD COLUMN extra integer DEFAULT 3",
            "ALTER TABLE k DROP COLUMN w",
        ] {
            if let Err(e) = c.execute(sql) {
                failed.push(format!("control `{sql}`: {e}"));
            }
        }
    }
    match open().and_then(|db| db.connect().map(|c| (db, c))) {
        Ok((_db, raw)) => {
            let c = turso_pg::Connection::new(raw);
            match c.query("SELECT id, v, extra FROM k") {
                Ok(Some(mut st)) => match st.run_collect_rows() {
                    Ok(rows) if rows.len() == 1 => {}
                    other => failed.push(format!("control: SELECT after the reopen: {other:?}")),
                },
                other => failed.push(format!("control: SELECT after the reopen: {:?}", other.err())),
            }
        }
        Err(e) => failed.push(format!("control: the reopen failed: {e}")),
    }
    assert!(failed.is_empty(), "CLAIM: frontend SQL cannot call turso_alter_column_sql: {failed:#?}");
}
