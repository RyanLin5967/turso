//! Additive lifecycle controls for the existing explicitly enabled VACUUM path.
//! The strict wire tests remain unchanged, including the wire VACUUM refusal.
//! SQL/type facts come from the committed c52-view-lifecycle/aliases/contracts PG18.6 recordings.

use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, Value, IO};

fn open(path: &std::path::Path) -> (Arc<Database>, crate::Connection) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = crate::session::open_database_with_io(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new()
            .with_views(true)
            .with_custom_types(true)
            .with_vacuum(true),
    )
    .unwrap();
    let conn = crate::Connection::new(db.connect().unwrap());
    (db, conn)
}

fn answer(conn: &crate::Connection, sql: &str, oids: &[u32], values: Vec<Value>) {
    let (mut stmt, types) = conn.prepare_typed(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(types.columns, oids.iter().copied().map(Some).collect::<Vec<_>>(), "{sql}");
    assert_eq!(stmt.run_collect_rows().unwrap(), vec![values], "{sql}");
}

fn integer(value: i64) -> Value {
    Value::from_i64(value)
}

#[test]
fn c52_embedded_view_domain_alias_scope_survives_vacuum_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scoped-view.db");
    {
        let (_db, conn) = open(&path);
        for sql in [
            "CREATE DOMAIN c52_view_domain AS integer CHECK (VALUE > 0)",
            "CREATE TABLE c52_view_t(a integer)",
            "CREATE TABLE c52_view_u(a integer)",
            "INSERT INTO c52_view_t VALUES (2)",
            "INSERT INTO c52_view_u VALUES (3)",
            "CREATE VIEW c52_vsource(flag,n) AS SELECT true,a::c52_view_domain FROM c52_view_t",
            "CREATE VIEW c52_valias(flag) AS SELECT q.x > 0 FROM c52_view_t AS q(x)",
            "CREATE VIEW c52_vscope(q,untouched,literal,yes) AS SELECT t.a,(SELECT a FROM c52_view_u LIMIT 1),'a'::text,true FROM c52_view_t t",
            "CREATE VIEW c52_vdependent(flag,n) AS SELECT flag,n FROM c52_vsource",
            "CREATE VIEW c52_vimplicit AS SELECT a,true AS flag FROM c52_view_t",
            "CREATE VIEW c52_vcorrelated(n) AS SELECT (SELECT a WHERE c52_view_t.a > 0) FROM c52_view_t",
            "ALTER TABLE c52_view_t RENAME COLUMN a TO b",
        ] {
            conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        answer(&conn, "SELECT flag,n::int4 FROM c52_vsource", &[16,23], vec![integer(1),integer(2)]);
        answer(&conn, "SELECT flag FROM c52_valias", &[16], vec![integer(1)]);
        answer(&conn, "SELECT q,untouched,literal,yes FROM c52_vscope", &[23,23,25,16],
            vec![integer(2),integer(3),Value::from_text("a"),integer(1)]);
        answer(&conn, "SELECT n FROM c52_vcorrelated", &[23], vec![integer(2)]);
        answer(&conn, "SELECT a,flag FROM c52_vimplicit", &[23,16], vec![integer(2),integer(1)]);
        // The wire translator explicitly refuses VACUUM. Exercise the existing
        // enabled engine replay API while retaining typed frontend observations.
        conn.inner().execute("VACUUM").unwrap();
        for sql in ["SELECT flag,n::int4 FROM c52_vsource", "SELECT flag,n::int4 FROM c52_vdependent"] {
            answer(&conn, sql, &[16,23], vec![integer(1),integer(2)]);
        }
        answer(&conn, "SELECT flag FROM c52_valias", &[16], vec![integer(1)]);
        answer(&conn, "SELECT q,untouched,literal,yes FROM c52_vscope", &[23,23,25,16],
            vec![integer(2),integer(3),Value::from_text("a"),integer(1)]);
        answer(&conn, "SELECT n FROM c52_vcorrelated", &[23], vec![integer(2)]);
        answer(&conn, "SELECT a,flag FROM c52_vimplicit", &[23,16], vec![integer(2),integer(1)]);
        conn.close().unwrap();
    }
    let (_db, conn) = open(&path);
    for sql in ["SELECT flag,n::int4 FROM c52_vsource", "SELECT flag,n::int4 FROM c52_vdependent"] {
        answer(&conn, sql, &[16,23], vec![integer(1),integer(2)]);
    }
    answer(&conn, "SELECT flag FROM c52_valias", &[16], vec![integer(1)]);
    answer(&conn, "SELECT q,untouched,literal,yes FROM c52_vscope", &[23,23,25,16],
        vec![integer(2),integer(3),Value::from_text("a"),integer(1)]);
    answer(&conn, "SELECT n FROM c52_vcorrelated", &[23], vec![integer(2)]);
    answer(&conn, "SELECT a,flag FROM c52_vimplicit", &[23,16], vec![integer(2),integer(1)]);
    let (_, types) = conn.prepare_typed("SELECT 1 FROM c52_vsource WHERE flag=$1").unwrap();
    assert_eq!(types.params.get(&1), Some(&16));
    conn.close().unwrap();
}

#[test]
fn c52_embedded_view_output_contracts_survive_repeated_rename_vacuum_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("view-contracts.db");
    {
        let (_db, conn) = open(&path);
        for sql in [
            "CREATE TABLE c52_contract_t(a integer,b integer)",
            "INSERT INTO c52_contract_t VALUES(1,2)",
            "CREATE VIEW c52_contract_partial AS SELECT q.b FROM c52_contract_t AS q(x)",
            "CREATE VIEW c52_contract_nested AS SELECT s.b FROM (SELECT b::int4 FROM c52_contract_t) s",
            "CREATE VIEW c52_contract_coalesce AS SELECT COALESCE(TRUE,FALSE)",
            "CREATE VIEW c52_contract_nested_name AS SELECT d.coalesce AS flag FROM (SELECT COALESCE(TRUE,FALSE)) d",
        ] {
            conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in ["SELECT coalesce FROM c52_contract_coalesce", "SELECT flag FROM c52_contract_nested_name"] {
            answer(&conn, sql, &[16], vec![integer(1)]);
        }
        for rename in ["ALTER TABLE c52_contract_t RENAME COLUMN b TO c", "ALTER TABLE c52_contract_t RENAME COLUMN c TO d"] {
            conn.execute(rename).unwrap_or_else(|e| panic!("{rename}: {e}"));
            for sql in ["SELECT b FROM c52_contract_partial", "SELECT b FROM c52_contract_nested"] {
                answer(&conn, sql, &[23], vec![integer(2)]);
            }
        }
        // This direct engine API intentionally exercises an enabled replay path;
        // the unchanged wire test still requires PostgreSQL's VACUUM behavior.
        conn.inner().execute("VACUUM").unwrap();
        for sql in ["SELECT b FROM c52_contract_partial", "SELECT b FROM c52_contract_nested"] {
            answer(&conn, sql, &[23], vec![integer(2)]);
        }
        for sql in ["SELECT coalesce FROM c52_contract_coalesce", "SELECT flag FROM c52_contract_nested_name"] {
            answer(&conn, sql, &[16], vec![integer(1)]);
        }
        conn.close().unwrap();
    }
    let (_db, conn) = open(&path);
    for sql in ["SELECT b FROM c52_contract_partial", "SELECT b FROM c52_contract_nested"] {
        answer(&conn, sql, &[23], vec![integer(2)]);
    }
    for sql in ["SELECT coalesce FROM c52_contract_coalesce", "SELECT flag FROM c52_contract_nested_name"] {
        answer(&conn, sql, &[16], vec![integer(1)]);
    }
    answer(&conn, "SELECT pg_typeof(flag) FROM c52_contract_nested_name", &[2206], vec![Value::from_text("boolean")]);
    conn.close().unwrap();
}
