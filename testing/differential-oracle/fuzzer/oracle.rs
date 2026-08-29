//! Oracle implementations for validating database behavior.
//!
//! Oracles are predicates that verify properties of database execution.
//! The primary oracle is the DifferentialOracle which compares Turso
//! results against SQLite.

use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::Result;
use sql_gen::Schema;
use sql_gen_prop::SqlValue;
use sql_gen_prop::result::diff_results;
use turso_core::{Numeric, Value};

use crate::generate::GeneratedStatement;

/// Result of an oracle check.
#[derive(Debug, Clone)]
pub enum OracleResult {
    /// The oracle check passed.
    Pass,
    /// EXPLAIN failed in at least one engine, so neither engine ran the statement.
    Skipped(String),
    /// The oracle check passed but with a warning (e.g., LIMIT without ORDER BY).
    Warning(String),
    /// The oracle check failed with a reason.
    Fail(String),
}

impl OracleResult {
    pub fn is_pass(&self) -> bool {
        matches!(self, OracleResult::Pass)
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, OracleResult::Skipped(_))
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, OracleResult::Warning(_))
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, OracleResult::Fail(_))
    }
}

/// A row of values from a query result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Row(pub Vec<SqlValue>);

/// Trait for oracles that can check database properties.
pub trait Oracle {
    /// Check the oracle after executing a statement.
    ///
    /// Returns Pass if the property holds, Warning for non-fatal issues,
    /// or Fail with a reason otherwise.
    fn check(
        &self,
        stmt: &GeneratedStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult;
}

/// Result of executing a query on a database.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Query executed successfully with rows.
    Rows(Vec<Row>),
    /// Query executed successfully with no rows (e.g., INSERT, UPDATE, DELETE).
    Ok,
    /// Query failed with an error.
    Error(String),
}

impl QueryResult {
    pub fn is_error(&self) -> bool {
        matches!(self, QueryResult::Error(_))
    }
}

/// Differential oracle that compares Turso results with SQLite.
///
/// This oracle verifies that Turso produces the same results as SQLite
/// for all queries. It's the primary correctness check for the fuzzer.
pub struct DifferentialOracle;

impl Oracle for DifferentialOracle {
    fn check(
        &self,
        stmt: &GeneratedStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult {
        let has_unordered_limit = stmt.has_unordered_limit;
        let count_is_guaranteed = stmt.count_is_guaranteed;

        match (turso_result, sqlite_result) {
            (QueryResult::Rows(turso_rows), QueryResult::Rows(sqlite_rows)) => {
                let diff = diff_results(turso_rows, sqlite_rows);
                if !diff.is_empty() {
                    // For non-deterministic LIMIT queries, the result set may legitimately differ
                    // since the chosen rows are not stable across engines. Return a warning instead
                    // of failure.
                    // An unordered LIMIT genuinely does not fix WHICH rows come back,
                    // so a differing row set is not a bug. It does fix HOW MANY: `LIMIT n`
                    // must return min(n, count) rows on both engines whatever it picks. The
                    // counts were already being passed into the warning text and never
                    // compared, so a row-count divergence was reported as PASSED.
                    //
                    // Two verified Turso bugs have exactly this shape:
                    //   SELECT x FROM aa CROSS JOIN bb LIMIT 10 OFFSET 1  -> 3 rows vs 5
                    //   SELECT count(a) FROM t LIMIT 0                     -> 1 row  vs 0
                    // Both set has_unordered_limit, so both passed. Writing the second with
                    // an ORDER BY over the primary key clears the flag without fixing the
                    // bug, and it is then reported -- so the exemption was the thing hiding
                    // it, not a limitation of the comparison.
                    if has_unordered_limit
                        && (turso_rows.len() == sqlite_rows.len() || !count_is_guaranteed)
                    {
                        return OracleResult::Warning(format_nondet_limit_warning(
                            stmt,
                            "row_set_mismatch",
                            turso_rows.len(),
                            sqlite_rows.len(),
                            diff.only_in_first.len(),
                            diff.only_in_second.len(),
                        ));
                    }
                    if has_unordered_limit {
                        return OracleResult::Fail(format!(
                            "Row COUNT mismatch under an unordered LIMIT. Which rows come back is \
                             not stable across engines, but how many is:\n  SQL: {stmt}\n  \
                             Turso returned {} row(s), SQLite {}",
                            turso_rows.len(),
                            sqlite_rows.len()
                        ));
                    }
                    return OracleResult::Fail(format!(
                        "Row set mismatch:\n  SQL: {stmt}\n  Only in Turso: {:?}\n  Only in SQLite: {:?}",
                        diff.only_in_first, diff.only_in_second
                    ));
                }

                OracleResult::Pass
            }
            (QueryResult::Ok, QueryResult::Ok) => OracleResult::Pass,
            (QueryResult::Error(turso_err), QueryResult::Error(sqlite_err)) => {
                // Both errored. Usually that is agreement: two engines rejecting the same
                // invalid SQL. It is NOT agreement when Turso's error says its own
                // invariant broke, because that is a bug whatever SQLite thinks of the
                // statement.
                //
                // Demonstrated: `SELECT DISTINCT count(*) FROM t UNION;` makes Turso
                // report "Corrupt database: Reference to undefined or unresolved label in
                // HashDistinct" while SQLite reports a syntax error, and this arm scored
                // it a Pass -- differential_probe said "0 diverged", exit 0. Drop the
                // trailing UNION and the identical internal error is reported, because
                // then only one side errors. One token decided whether a corruption
                // message was surfaced or swallowed.
                //
                // Deliberately a narrow list of internal-failure markers rather than an
                // allowlist of acceptable errors: enumerating everything SQLite may
                // legitimately reject is not tractable, while "the engine says its own
                // invariant broke" is a small and stable set. The fuller fix is to
                // compare error CLASSES on both sides; this covers the case that is never
                // acceptable in either direction.
                if is_internal_failure(turso_err) {
                    return OracleResult::Fail(format!(
                        "Turso reported an internal failure. SQLite rejected the statement \
                         for its own reasons, which does not excuse it:\n  SQL: {stmt}\n  \
                         Turso: {turso_err}\n  SQLite: {sqlite_err}"
                    ));
                }
                tracing::debug!("Both databases errored on: {stmt}: {turso_err}");
                OracleResult::Pass
            }
            (QueryResult::Error(turso_err), _) => OracleResult::Fail(format!(
                "Turso errored but SQLite succeeded:\n  SQL: {stmt}\n  Error: {turso_err}"
            )),
            (_, QueryResult::Error(sqlite_err)) => OracleResult::Fail(format!(
                "SQLite errored but Turso succeeded:\n  SQL: {stmt}\n  Error: {sqlite_err}"
            )),
            (QueryResult::Rows(rows), QueryResult::Ok) => {
                // Rows on one side and none on the other is a COUNT divergence, which a
                // top-level unordered LIMIT does not excuse -- this is where
                // `SELECT count(a) FROM t LIMIT 0` lands (Turso 1 row, SQLite 0), because
                // execute_sqlite maps an empty result to Ok and never to Rows(vec![]).
                // A LIMIT inside a subquery is a different matter and still only warns.
                if rows.is_empty() {
                    OracleResult::Pass
                } else if has_unordered_limit && !count_is_guaranteed {
                    OracleResult::Warning(format_nondet_limit_warning(
                        stmt,
                        "rows_vs_ok",
                        rows.len(),
                        0,
                        rows.len(),
                        0,
                    ))
                } else {
                    OracleResult::Fail(format!(
                        "Turso returned {} rows but SQLite returned no rows:\n  SQL: {stmt}",
                        rows.len()
                    ))
                }
            }
            (QueryResult::Ok, QueryResult::Rows(rows)) => {
                if rows.is_empty() {
                    OracleResult::Pass
                } else if has_unordered_limit && !count_is_guaranteed {
                    OracleResult::Warning(format_nondet_limit_warning(
                        stmt,
                        "ok_vs_rows",
                        0,
                        rows.len(),
                        0,
                        rows.len(),
                    ))
                } else {
                    OracleResult::Fail(format!(
                        "SQLite returned {} rows but Turso returned no rows:\n  SQL: {stmt}",
                        rows.len()
                    ))
                }
            }
        }
    }
}

fn sql_hash(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn short_sql(sql: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (i, ch) in sql.chars().enumerate() {
        if i >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

fn format_skipped_statement(
    stmt: &GeneratedStatement,
    turso_error: Option<&str>,
    sqlite_error: Option<&str>,
) -> String {
    let error_prefix = |error: &str| {
        let error = error
            .find(&stmt.sql)
            .map(|sql_start| error[..sql_start].trim_end_matches(" in ").trim())
            .unwrap_or(error);
        short_sql(error, 240)
    };
    let turso_error = turso_error.map(error_prefix);
    let sqlite_error = sqlite_error.map(error_prefix);
    format!(
        "Statement skipped because EXPLAIN failed: sql_hash={:016x} Turso error={turso_error:?} SQLite error={sqlite_error:?}\n  SQL: {}",
        sql_hash(&stmt.sql),
        short_sql(&stmt.sql, 240),
    )
}

fn format_nondet_limit_warning(
    stmt: &GeneratedStatement,
    kind: &str,
    turso_rows: usize,
    sqlite_rows: usize,
    only_in_turso: usize,
    only_in_sqlite: usize,
) -> String {
    let reason = stmt
        .unordered_limit_reason
        .as_deref()
        .unwrap_or("unordered_limit");
    format!(
        "NONDET_LIMIT_WARNING reason={reason} kind={kind} sql_hash={:016x} turso_rows={turso_rows} sqlite_rows={sqlite_rows} only_in_turso={only_in_turso} only_in_sqlite={only_in_sqlite}\n  SQL(prefix): {}",
        sql_hash(&stmt.sql),
        short_sql(&stmt.sql, 240),
    )
}

impl DifferentialOracle {
    /// Execute a query on Turso and return the result.
    pub fn execute_turso(conn: &Arc<turso_core::Connection>, sql: &str) -> QueryResult {
        let execute = || {
            let mut stmt = conn.prepare(sql)?;

            let mut rows = Vec::new();
            stmt.run_with_row_callback(|row| {
                let mut values = Vec::new();
                for i in 0..row.len() {
                    let value = Self::convert_turso_value(row.get_value(i).clone());
                    values.push(value);
                }
                rows.push(Row(values));
                Ok(())
            })?;

            let res = if rows.is_empty() {
                QueryResult::Ok
            } else {
                QueryResult::Rows(rows)
            };
            Ok(res)
        };
        let result: Result<QueryResult, turso_core::LimboError> = execute();
        match result {
            Ok(res) => res,
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    /// Execute a query on SQLite and return the result.
    pub fn execute_sqlite(conn: &rusqlite::Connection, sql: &str) -> QueryResult {
        // First try as a query that returns rows
        let execute = || {
            let mut stmt = conn.prepare(sql)?;
            let column_count = stmt.column_count();
            let res = if column_count == 0 {
                // Statement doesn't return rows (INSERT, UPDATE, DELETE, etc.)
                stmt.execute([])?;
                QueryResult::Ok
            } else {
                let mut query_rows = stmt.query([])?;
                let mut rows = Vec::new();
                while let Some(row) = query_rows.next()? {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        let value = Self::convert_sqlite_value(row.get_ref(i).ok());
                        values.push(value);
                    }
                    rows.push(Row(values));
                }
                if rows.is_empty() {
                    QueryResult::Ok
                } else {
                    QueryResult::Rows(rows)
                }
            };
            stmt.finalize()?;
            Ok(res)
        };
        let result: Result<QueryResult, rusqlite::Error> = execute();
        match result {
            Ok(res) => res,
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    fn convert_turso_value(value: Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::Numeric(Numeric::Integer(i)) => SqlValue::Integer(i),
            Value::Numeric(Numeric::Float(f)) => SqlValue::Real(f64::from(f)),
            Value::Text(s) => SqlValue::Text(s.as_str().to_string()),
            Value::Blob(b) => SqlValue::Blob(b),
        }
    }

    fn convert_sqlite_value(value: Option<rusqlite::types::ValueRef<'_>>) -> SqlValue {
        match value {
            None => SqlValue::Null,
            Some(rusqlite::types::ValueRef::Null) => SqlValue::Null,
            Some(rusqlite::types::ValueRef::Integer(i)) => SqlValue::Integer(i),
            Some(rusqlite::types::ValueRef::Real(f)) => SqlValue::Real(f),
            Some(rusqlite::types::ValueRef::Text(s)) => {
                SqlValue::Text(String::from_utf8_lossy(s).to_string())
            }
            Some(rusqlite::types::ValueRef::Blob(b)) => SqlValue::Blob(b.to_vec()),
        }
    }

    fn snapshot_query(table: &sql_gen::Table) -> String {
        format!(
            "SELECT rowid, * FROM {} ORDER BY rowid",
            table.qualified_name()
        )
    }

    fn verify_table_snapshots(
        turso_conn: &Arc<turso_core::Connection>,
        sqlite_conn: &rusqlite::Connection,
        schema: &Schema,
        stmt: &GeneratedStatement,
    ) -> OracleResult {
        for table in &schema.tables {
            let snapshot_sql = Self::snapshot_query(table);
            let turso_rows = Self::execute_turso(turso_conn, &snapshot_sql);
            let sqlite_rows = Self::execute_sqlite(sqlite_conn, &snapshot_sql);
            match (turso_rows, sqlite_rows) {
                (QueryResult::Rows(turso_rows), QueryResult::Rows(sqlite_rows)) => {
                    let diff = diff_results(&turso_rows, &sqlite_rows);
                    if !diff.is_empty() {
                        return OracleResult::Fail(format!(
                            "Post-DML table snapshot mismatch for {}:\n  SQL: {stmt}\n  Only in Turso: {:?}\n  Only in SQLite: {:?}",
                            table.qualified_name(),
                            diff.only_in_first,
                            diff.only_in_second
                        ));
                    }
                }
                (QueryResult::Ok, QueryResult::Ok) => {}
                (QueryResult::Error(turso_err), QueryResult::Error(sqlite_err)) => {
                    return OracleResult::Fail(format!(
                        "Post-DML snapshot failed on both engines for {}:\n  SQL: {stmt}\n  Turso: {turso_err}\n  SQLite: {sqlite_err}",
                        table.qualified_name()
                    ));
                }
                (QueryResult::Error(turso_err), _) => {
                    return OracleResult::Fail(format!(
                        "Turso snapshot failed for {} after DML:\n  SQL: {stmt}\n  Error: {turso_err}",
                        table.qualified_name()
                    ));
                }
                (_, QueryResult::Error(sqlite_err)) => {
                    return OracleResult::Fail(format!(
                        "SQLite snapshot failed for {} after DML:\n  SQL: {stmt}\n  Error: {sqlite_err}",
                        table.qualified_name()
                    ));
                }
                (QueryResult::Rows(turso_rows), QueryResult::Ok) => {
                    if !turso_rows.is_empty() {
                        return OracleResult::Fail(format!(
                            "Turso snapshot returned rows for {} but SQLite returned none:\n  SQL: {stmt}",
                            table.qualified_name()
                        ));
                    }
                }
                (QueryResult::Ok, QueryResult::Rows(sqlite_rows)) => {
                    if !sqlite_rows.is_empty() {
                        return OracleResult::Fail(format!(
                            "SQLite snapshot returned rows for {} but Turso returned none:\n  SQL: {stmt}",
                            table.qualified_name()
                        ));
                    }
                }
            }
        }

        OracleResult::Pass
    }
}

/// Execute a statement on both databases and check the differential oracle.
pub fn check_differential(
    turso_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    schema: &Schema,
    stmt: &GeneratedStatement,
) -> OracleResult {
    // Generated SQL can contain an error in a branch that never runs. SQLite
    // may remove that branch before checking it, while Turso may reject it.
    // If the accepted statement writes data, running it in only one database
    // would spoil every comparison that follows. EXPLAIN asks each engine to
    // prepare the statement without changing data. Run it only if both agree
    // that it can run.
    let explain_sql = format!("EXPLAIN {}", stmt.sql);
    let turso_explain = DifferentialOracle::execute_turso(turso_conn, &explain_sql);
    let sqlite_explain = DifferentialOracle::execute_sqlite(sqlite_conn, &explain_sql);
    match (&turso_explain, &sqlite_explain) {
        (QueryResult::Error(turso_error), QueryResult::Error(sqlite_error)) => {
            if is_internal_failure(&turso_error) {
                return OracleResult::Fail(format!(
                    "Turso reported an internal failure while preparing; SQLite \
                     rejected the statement for its own reasons:\n  SQL: {stmt}\n  \
                     Turso: {turso_error}"
                ));
            }
            return OracleResult::Skipped(format_skipped_statement(
                stmt,
                Some(turso_error),
                Some(sqlite_error),
            ));
        }
        (QueryResult::Error(turso_error), _) => {
            // An internal invariant violation during prepare is a bug whatever SQLite
            // thinks of the statement, so it must not be filed under "skipped". Without
            // this the rule below is unreachable from the fuzzer for the whole
            // prepare-time class: EXPLAIN SELECT DISTINCT count(*) FROM t itself returns
            // "Corrupt database: Reference to undefined or unresolved label", so the gate
            // fires before check() ever runs. Only differential_probe, which has no
            // EXPLAIN gate, ever reached it.
            if is_internal_failure(&turso_error) {
                return OracleResult::Fail(format!(
                    "Turso reported an internal failure while preparing:\n  SQL: {stmt}\n  \
                     Turso: {turso_error}"
                ));
            }
            return OracleResult::Skipped(format_skipped_statement(stmt, Some(turso_error), None));
        }
        (_, QueryResult::Error(sqlite_error)) => {
            return OracleResult::Skipped(format_skipped_statement(stmt, None, Some(sqlite_error)));
        }
        _ => {}
    }

    let turso_result = DifferentialOracle::execute_turso(turso_conn, &stmt.sql);
    let sqlite_result = DifferentialOracle::execute_sqlite(sqlite_conn, &stmt.sql);

    let oracle = DifferentialOracle;
    let direct_result = oracle.check(stmt, &turso_result, &sqlite_result);
    if !stmt.mutates_data || !direct_result.is_pass() {
        return direct_result;
    }

    DifferentialOracle::verify_table_snapshots(turso_conn, sqlite_conn, schema, stmt)
}

/// True if this Turso error reports a broken internal invariant rather than a rejection of
/// the statement. Such an error is a bug even when SQLite also refuses the statement, so it
/// must not be absorbed by the both-errored arm.
///
/// Kept narrow on purpose. "not yet implemented" and similar are deliberately absent: they
/// are honest limitations, and treating them as failures would end runs on unimplemented
/// features rather than on bugs.
pub fn is_internal_failure(err: &str) -> bool {
    // Matched case-insensitively and against what the engine ACTUALLY renders. The first
    // version of this list was written from memory and was nearly inert: LimboError
    // renders `#[error("Internal error: {0}")]` with a capital I, `str::contains` is
    // case-sensitive, so the lowercase "internal error" marker never fired. And
    // "assertion failed" / "panicked" never appear in a QueryResult::Error at all --
    // runner.rs catches panics through catch_unwind and reports them as a separate Err,
    // so those two markers could not match by construction. Effectively a five-marker
    // list was doing the work of one.
    const MARKERS: &[&str] = &[
        "corrupt database",
        "internal error",
        "undefined or unresolved label",
        "unreachable code",
    ];
    let err = err.to_ascii_lowercase();
    MARKERS.iter().any(|marker| err.contains(marker))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use turso_core::SqliteDialect;

    use core::f64;

    use super::*;
    use crate::memory::MemorySimIO;
    use sql_gen::{ColumnDef, DataType, SchemaBuilder, Table};
    use turso_core::Database;

    #[test]
    fn test_sql_value_equality() {
        assert_eq!(SqlValue::Null, SqlValue::Null);
        assert_eq!(SqlValue::Integer(42), SqlValue::Integer(42));
        assert_ne!(SqlValue::Integer(42), SqlValue::Integer(43));
        assert_eq!(
            SqlValue::Text("hello".into()),
            SqlValue::Text("hello".into())
        );
        assert_eq!(
            SqlValue::Real(f64::consts::PI),
            SqlValue::Real(f64::consts::PI)
        );
    }

    #[test]
    fn test_oracle_result() {
        assert!(OracleResult::Pass.is_pass());
        assert!(!OracleResult::Pass.is_fail());
        assert!(!OracleResult::Pass.is_skipped());
        assert!(!OracleResult::Pass.is_warning());

        assert!(OracleResult::Skipped("test".into()).is_skipped());
        assert!(!OracleResult::Skipped("test".into()).is_pass());
        assert!(!OracleResult::Skipped("test".into()).is_fail());
        assert!(!OracleResult::Skipped("test".into()).is_warning());

        assert!(OracleResult::Warning("test".into()).is_warning());
        assert!(!OracleResult::Warning("test".into()).is_pass());
        assert!(!OracleResult::Warning("test".into()).is_fail());

        assert!(OracleResult::Fail("test".into()).is_fail());
        assert!(!OracleResult::Fail("test".into()).is_pass());
        assert!(!OracleResult::Fail("test".into()).is_warning());
    }

    #[test]
    fn test_nondet_warning_is_structured_and_reasoned() {
        let stmt = GeneratedStatement {
            sql: "SELECT 1 LIMIT 1".to_string(),
            is_ddl: false,
            mutates_data: false,
            has_unordered_limit: true,

            count_is_guaranteed: true,
            unordered_limit_reason: Some("limit_order_by_scalar_subquery".to_string()),
        };
        let turso = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(1)])]);
        let sqlite = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(2)])]);

        let oracle = DifferentialOracle;
        let res = oracle.check(&stmt, &turso, &sqlite);
        match res {
            OracleResult::Warning(msg) => {
                assert!(msg.contains("NONDET_LIMIT_WARNING"));
                assert!(msg.contains("reason=limit_order_by_scalar_subquery"));
                assert!(msg.contains("kind=row_set_mismatch"));
                assert!(msg.contains("sql_hash="));
                assert!(msg.contains("SQL(prefix): SELECT 1 LIMIT 1"));
            }
            other => panic!("expected warning, got {other:?}"),
        }
    }

    #[test]
    fn test_check_differential_fails_on_hidden_table_state_mismatch() {
        let io = Arc::new(MemorySimIO::new(123));
        let turso_db = Database::open_file_with_flags(
            io,
            "oracle-state-mismatch.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let turso_conn = turso_db.connect().unwrap();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let schema = SchemaBuilder::new()
            .table(Table::new(
                "t",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("v", DataType::Integer),
                ],
            ))
            .build();

        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1, 10)",
        ] {
            assert!(matches!(
                DifferentialOracle::execute_turso(&turso_conn, sql),
                QueryResult::Ok
            ));
            assert!(matches!(
                DifferentialOracle::execute_sqlite(&sqlite_conn, sql),
                QueryResult::Ok
            ));
        }

        assert!(matches!(
            DifferentialOracle::execute_turso(&turso_conn, "UPDATE t SET v = 11 WHERE id = 1"),
            QueryResult::Ok
        ));

        let stmt = GeneratedStatement {
            sql: "UPDATE t SET v = v WHERE id = 999".to_string(),
            is_ddl: false,
            mutates_data: true,
            has_unordered_limit: false,

            count_is_guaranteed: false,
            unordered_limit_reason: None,
        };

        let result = check_differential(&turso_conn, &sqlite_conn, &schema, &stmt);
        assert!(
            result.is_fail(),
            "post-DML state verification should catch hidden row mismatches"
        );
    }

    #[test]
    fn statement_rejected_by_one_engine_is_skipped() {
        let io = Arc::new(MemorySimIO::new(456));
        let turso_db = Database::open_file_with_flags(
            io,
            "oracle-validation-skip.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let turso_conn = turso_db.connect().unwrap();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "t",
                vec![ColumnDef::new("a", DataType::Integer)],
            ))
            .build();

        for sql in ["CREATE TABLE t(a)", "INSERT INTO t VALUES (1)"] {
            assert!(matches!(
                DifferentialOracle::execute_turso(&turso_conn, sql),
                QueryResult::Ok
            ));
            assert!(matches!(
                DifferentialOracle::execute_sqlite(&sqlite_conn, sql),
                QueryResult::Ok
            ));
        }

        let stmt = GeneratedStatement {
            sql: "WITH cte(x) AS (SELECT 1, 2) \
                  UPDATE t SET a = 2 WHERE 0 AND EXISTS (SELECT * FROM cte)"
                .to_string(),
            is_ddl: false,
            mutates_data: true,
            has_unordered_limit: false,

            count_is_guaranteed: false,
            unordered_limit_reason: None,
        };

        let result = check_differential(&turso_conn, &sqlite_conn, &schema, &stmt);
        match result {
            OracleResult::Skipped(reason) => {
                assert!(reason.contains("Statement skipped because EXPLAIN failed"));
                assert!(reason.contains("Turso error=Some"));
                assert!(reason.contains("SQLite error=None"));
            }
            other => panic!("expected skipped statement, got {other:?}"),
        }
    }

    /// A run that compared nothing must not report success. Before this, `-n 0` printed
    /// PASSED with zero statements executed and exited 0.
    #[test]
    fn a_run_that_executed_nothing_is_not_a_success() {
        let mut stats = crate::runner::SimStats::default();
        assert!(
            !stats.is_success(),
            "zero statements executed must not be a pass"
        );
        stats.statements_executed = 1;
        assert!(stats.is_success(), "one clean statement is a pass");
        stats.oracle_failures = 1;
        assert!(!stats.is_success(), "a failure is still a failure");
    }

    /// Turso's own invariant violations must not hide behind a SQLite rejection.
    ///
    /// The strings below are what the engine ACTUALLY renders, not invented ones. An
    /// earlier version of this test asserted on "assertion failed" and "panicked", which
    /// never reach a QueryResult::Error at all -- runner.rs catches panics through
    /// catch_unwind and reports them separately -- so the test passed while the marker
    /// list it was checking was largely inert.
    #[test]
    fn internal_failures_are_not_agreement() {
        // Verified against tursodb: SELECT DISTINCT count(*) FROM t emits the first of
        // these. LimboError renders `#[error("Internal error: {0}")]`, capital I, which is
        // why the match is case-insensitive.
        for err in [
            "Corrupt database: Reference to undefined or unresolved label in HashDistinct: 5",
            "Internal error: entered unreachable code",
            "internal error: entered unreachable code: state is ReadHeader",
        ] {
            assert!(is_internal_failure(err), "should be internal: {err}");
        }
        for err in [
            "no such table: t",
            "near \";\": syntax error",
            "datatype mismatch",
            "FOREIGN KEY constraint failed",
            "parser stack overflow",
            "integer overflow",
            "not yet implemented: window functions",
        ] {
            assert!(
                !is_internal_failure(err),
                "legitimate rejection must stay agreement: {err}"
            );
        }
    }
    /// An unordered LIMIT excuses WHICH rows come back, never HOW MANY. `LIMIT n` must
    /// return min(n, count) rows on both engines whatever it picks, so a count mismatch is
    /// a real bug even when the flag is set. Two verified Turso bugs have this shape --
    /// `... CROSS JOIN ... LIMIT 10 OFFSET 1` gives 3 rows against 5, and
    /// `SELECT count(a) FROM t LIMIT 0` gives 1 against 0 -- and both were reported as
    /// PASSED before this.
    #[test]
    fn unordered_limit_excuses_which_rows_but_not_how_many() {
        let stmt = GeneratedStatement {
            sql: "SELECT x FROM aa CROSS JOIN bb LIMIT 10 OFFSET 1".to_string(),
            is_ddl: false,
            mutates_data: false,
            has_unordered_limit: true,

            count_is_guaranteed: true,
            unordered_limit_reason: Some("limit_without_order_by".to_string()),
        };
        let rows =
            |n: i64| QueryResult::Rows((0..n).map(|i| Row(vec![SqlValue::Integer(i)])).collect());
        let oracle = DifferentialOracle;

        // Different COUNT under an unordered LIMIT: a bug, and must fail.
        match oracle.check(&stmt, &rows(3), &rows(5)) {
            OracleResult::Fail(msg) => {
                assert!(msg.contains("Row COUNT mismatch"), "{msg}");
                assert!(msg.contains("3 row(s)") && msg.contains("5"), "{msg}");
            }
            other => panic!("count mismatch must fail, got {other:?}"),
        }

        // Same count, different rows: still legitimately a warning, not a failure.
        let a = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(1)])]);
        let b = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(2)])]);
        assert!(
            matches!(oracle.check(&stmt, &a, &b), OracleResult::Warning(_)),
            "same count with different rows is what the exemption is for"
        );

        // And with the flag clear, a count mismatch fails as it always did.
        let ordered = GeneratedStatement {
            has_unordered_limit: false,

            count_is_guaranteed: false,
            unordered_limit_reason: None,
            ..stmt
        };
        assert!(matches!(
            oracle.check(&ordered, &rows(3), &rows(5)),
            OracleResult::Fail(_)
        ));
    }

    /// The shape the (Rows, Rows) test above cannot reach. `execute_sqlite` maps an empty
    /// result to `QueryResult::Ok`, never `Rows(vec![])`, so
    /// `SELECT count(a) FROM t LIMIT 0` -- Turso 1 row, SQLite 0 -- arrives as (Rows, Ok)
    /// and used to be excused by has_unordered_limit. One side having rows and the other
    /// none is always a count divergence, so there is nothing for the exemption to excuse.
    #[test]
    fn rows_versus_no_rows_is_always_a_count_divergence() {
        let stmt = GeneratedStatement {
            sql: "SELECT count(a) FROM t LIMIT 0".to_string(),
            is_ddl: false,
            mutates_data: false,
            has_unordered_limit: true,

            count_is_guaranteed: true,
            unordered_limit_reason: Some("limit_without_order_by".to_string()),
        };
        let one_row = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(0)])]);
        let oracle = DifferentialOracle;

        assert!(
            matches!(
                oracle.check(&stmt, &one_row, &QueryResult::Ok),
                OracleResult::Fail(_)
            ),
            "turso 1 row vs sqlite none must fail even under an unordered LIMIT"
        );
        assert!(
            matches!(
                oracle.check(&stmt, &QueryResult::Ok, &one_row),
                OracleResult::Fail(_)
            ),
            "the mirror direction must fail too"
        );
        // Both empty is still agreement.
        assert!(matches!(
            oracle.check(&stmt, &QueryResult::Rows(vec![]), &QueryResult::Ok),
            OracleResult::Pass
        ));
    }
}
