//! Audit probe: does the unordered-LIMIT exemption in oracle.rs swallow a real
//! ROW-COUNT divergence?
//!
//! Both statements below trip the SAME verified Turso bug (LIMIT 0 on an
//! ungrouped aggregate: Turso emits one row, SQLite emits none). They differ
//! only in whether the generator's own classifier flags them as
//! "unordered LIMIT".

use std::sync::Arc;

use differential_fuzzer::{
    Oracle,
    DifferentialOracle, GeneratedStatement, MemorySimIO, OracleResult, check_differential,
    oracle::QueryResult,
};
use sql_gen::ast::{
    ColumnRef, FromClause, FunctionCallExpr, OrderByItem, OrderDirection, SelectColumn, SelectStmt,
    Stmt,
};
use sql_gen::{ColumnDef, DataType, Expr, Schema, SchemaBuilder, Table};
use turso_core::{Database, SqliteDialect};

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(Table::new(
            "t",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("a", DataType::Integer),
            ],
        ))
        .build()
}

fn empty_select() -> SelectStmt {
    SelectStmt {
        with_clause: None,
        distinct: false,
        columns: vec![SelectColumn {
            expr: Expr::FunctionCall(FunctionCallExpr {
                name: "count".to_string(),
                args: vec![Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "a".to_string(),
                })],
                filter: None,
            }),
            alias: None,
        }],
        from: Some(FromClause {
            table: "t".to_string(),
            alias: None,
        }),
        joins: vec![],
        where_clause: None,
        group_by: None,
        compounds: vec![],
        order_by: vec![],
        limit: Some(0),
        offset: None,
    }
}

/// `SELECT count(a) FROM t LIMIT 0` -- LIMIT, no ORDER BY.
fn stmt_a() -> Stmt {
    Stmt::Select(empty_select())
}

/// `SELECT count(a) FROM t ORDER BY id ASC LIMIT 0` -- LIMIT with an ORDER BY
/// that includes the INTEGER PRIMARY KEY, so the classifier does not flag it.
fn stmt_b() -> Stmt {
    let mut s = empty_select();
    s.order_by = vec![OrderByItem {
        expr: Expr::ColumnRef(ColumnRef {
            table: None,
            column: "id".to_string(),
        }),
        direction: OrderDirection::Asc,
        nulls: None,
    }];
    Stmt::Select(s)
}

/// Exactly the classification generate.rs:224-235 performs for every statement
/// the fuzzer runs. Nothing here is hand-set.
fn classify(stmt: &Stmt, schema: &Schema) -> GeneratedStatement {
    let has_unordered_limit =
        stmt.has_unordered_limit() || stmt.non_unique_order_by_reason(schema).is_some();
    let unordered_limit_reason = stmt
        .unordered_limit_reason()
        .or_else(|| stmt.non_unique_order_by_reason(schema))
        .map(str::to_string);
    GeneratedStatement {
        sql: stmt.to_string(),
        is_ddl: false,
        mutates_data: false,
        has_unordered_limit,
        unordered_limit_reason,
    }
}

fn row_count(r: &QueryResult) -> String {
    match r {
        QueryResult::Rows(rows) => format!("{} row(s)", rows.len()),
        QueryResult::Ok => "0 row(s) (QueryResult::Ok)".to_string(),
        QueryResult::Error(e) => format!("ERROR {e}"),
    }
}

#[test]
fn unordered_limit_exemption_swallows_a_row_count_divergence() {
    let schema = schema();
    let io = Arc::new(MemorySimIO::new(7));
    let turso_db = Database::open_file_with_flags(
        io,
        "oracle-audit-unordered-limit.db",
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let turso_conn = turso_db.connect().unwrap();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

    for sql in [
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
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

    for (label, stmt) in [("A (no ORDER BY)", stmt_a()), ("B (ORDER BY id)", stmt_b())] {
        let gstmt = classify(&stmt, &schema);
        let turso = DifferentialOracle::execute_turso(&turso_conn, &gstmt.sql);
        let sqlite = DifferentialOracle::execute_sqlite(&sqlite_conn, &gstmt.sql);
        let verdict = check_differential(&turso_conn, &sqlite_conn, &schema, &gstmt);
        println!("=== {label} ===");
        println!("  SQL                  : {}", gstmt.sql);
        println!("  has_unordered_limit  : {}", gstmt.has_unordered_limit);
        println!("  reason               : {:?}", gstmt.unordered_limit_reason);
        println!("  turso returned       : {}", row_count(&turso));
        println!("  sqlite returned      : {}", row_count(&sqlite));
        println!(
            "  oracle verdict       : {}",
            match &verdict {
                OracleResult::Pass => "PASS".to_string(),
                OracleResult::Skipped(r) => format!("SKIPPED {r}"),
                OracleResult::Warning(r) => format!("WARNING {r}"),
                OracleResult::Fail(r) => format!("FAIL {r}"),
            }
        );
        println!(
            "  runner.rs would      : {}",
            match &verdict {
                OracleResult::Fail(_) => "abort the run (oracle_failures += 1, return Err)",
                OracleResult::Warning(_) => "count a warning and keep going (run still PASSES)",
                _ => "keep going",
            }
        );

        // Both statements really do disagree on the NUMBER of rows.
        let (tn, sn) = (turso, sqlite);
        assert!(
            matches!(&tn, QueryResult::Rows(r) if r.len() == 1),
            "{label}: expected Turso to emit 1 row, got {}",
            row_count(&tn)
        );
        assert!(
            matches!(&sn, QueryResult::Ok) || matches!(&sn, QueryResult::Rows(r) if r.is_empty()),
            "{label}: expected SQLite to emit 0 rows, got {}",
            row_count(&sn)
        );

        match label {
            // DEMONSTRATION: flagged -> the cardinality bug becomes a warning.
            "A (no ORDER BY)" => {
                assert!(gstmt.has_unordered_limit);
                assert!(
                    verdict.is_warning(),
                    "expected Warning, got {verdict:?} -- claim would be refuted"
                );
            }
            // CONTROL: same bug, same 1-vs-0 row counts, not flagged -> Fail.
            _ => {
                assert!(!gstmt.has_unordered_limit);
                assert!(
                    verdict.is_fail(),
                    "control did not fire: expected Fail, got {verdict:?}"
                );
            }
        }
    }
}

/// The reachable case. `LIMIT 10` on a cross join with `OFFSET 1`: Turso drops
/// whole outer-loop rows, so it returns 3 rows where SQLite returns 5.
/// `LIMIT 10` is inside the generator's own limit range (1..=max_limit), and
/// the generator makes joins and offsets, so nothing here is a corner case the
/// fuzzer could not reach.
#[test]
fn unordered_limit_exemption_swallows_a_join_limit_row_count_divergence() {
    use sql_gen::ast::{JoinClause, JoinType};

    let schema = SchemaBuilder::new()
        .table(Table::new(
            "aa",
            vec![ColumnDef::new("x", DataType::Integer)],
        ))
        .table(Table::new(
            "bb",
            vec![ColumnDef::new("y", DataType::Integer)],
        ))
        .build();

    let io = Arc::new(MemorySimIO::new(11));
    let turso_db = Database::open_file_with_flags(
        io,
        "oracle-audit-join-limit.db",
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let turso_conn = turso_db.connect().unwrap();
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        "CREATE TABLE aa(x INTEGER)",
        "CREATE TABLE bb(y INTEGER)",
        "INSERT INTO aa VALUES (1),(2)",
        "INSERT INTO bb VALUES (10),(20),(30)",
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

    let select = SelectStmt {
        with_clause: None,
        distinct: false,
        columns: vec![SelectColumn {
            expr: Expr::ColumnRef(ColumnRef {
                table: None,
                column: "x".to_string(),
            }),
            alias: None,
        }],
        from: Some(FromClause {
            table: "aa".to_string(),
            alias: None,
        }),
        joins: vec![JoinClause {
            join_type: JoinType::Cross,
            table: "bb".to_string(),
            alias: None,
            constraint: None,
        }],
        where_clause: None,
        group_by: None,
        compounds: vec![],
        order_by: vec![],
        limit: Some(10),
        offset: Some(1),
    };
    let stmt = Stmt::Select(select);
    let gstmt = classify(&stmt, &schema);

    let turso = DifferentialOracle::execute_turso(&turso_conn, &gstmt.sql);
    let sqlite = DifferentialOracle::execute_sqlite(&sqlite_conn, &gstmt.sql);
    let verdict = check_differential(&turso_conn, &sqlite_conn, &schema, &gstmt);

    println!("=== C (join + LIMIT 10 OFFSET 1) ===");
    println!("  SQL                  : {}", gstmt.sql);
    println!("  has_unordered_limit  : {}", gstmt.has_unordered_limit);
    println!("  reason               : {:?}", gstmt.unordered_limit_reason);
    println!("  turso returned       : {}", row_count(&turso));
    println!("  sqlite returned      : {}", row_count(&sqlite));
    println!("  min(LIMIT, rows)     : 5 rows is the only correct answer");
    println!(
        "  oracle verdict       : {}",
        match &verdict {
            OracleResult::Pass => "PASS".to_string(),
            OracleResult::Skipped(r) => format!("SKIPPED {r}"),
            OracleResult::Warning(r) => format!("WARNING {r}"),
            OracleResult::Fail(r) => format!("FAIL {r}"),
        }
    );

    let turso_n = match &turso {
        QueryResult::Rows(r) => r.len(),
        other => panic!("expected rows from Turso, got {}", row_count(other)),
    };
    let sqlite_n = match &sqlite {
        QueryResult::Rows(r) => r.len(),
        other => panic!("expected rows from SQLite, got {}", row_count(other)),
    };
    assert_eq!((turso_n, sqlite_n), (3, 5), "row counts should be 3 vs 5");
    assert!(gstmt.has_unordered_limit);
    assert!(
        verdict.is_warning(),
        "expected Warning, got {verdict:?} -- claim would be refuted"
    );

    // CONTROL for this exact pair of result sets: hand the SAME 3-row and
    // 5-row results to the SAME oracle with the flag cleared. It fails.
    let mut unflagged = gstmt.clone();
    unflagged.has_unordered_limit = false;
    unflagged.unordered_limit_reason = None;
    let control = DifferentialOracle.check(&unflagged, &turso, &sqlite);
    println!(
        "  CONTROL, same rows, flag cleared: {}",
        match &control {
            OracleResult::Fail(r) => format!("FAIL {r}"),
            other => format!("{other:?}"),
        }
    );
    assert!(
        control.is_fail(),
        "control did not fire: expected Fail, got {control:?}"
    );
}

/// How wide is the blind spot? Count how many statements the real generator
/// produces with the flag set, over a fixed schema.
#[test]
fn measure_share_of_generated_statements_with_the_row_count_check_disabled() {
    use differential_fuzzer::generate::{SqlGenBackend, SqlGenerator};

    assert!(
        std::env::var("ORACLE_AUDIT_INJECT").is_err(),
        "injection hook must be off for this measurement"
    );
    let schema = SchemaBuilder::new()
        .table(Table::new(
            "t1",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("a", DataType::Integer),
                ColumnDef::new("b", DataType::Text),
            ],
        ))
        .table(Table::new(
            "t2",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("c", DataType::Integer),
            ],
        ))
        .build();

    let mut backend = SqlGenBackend::new(42);
    let mut total = 0usize;
    let mut selects = 0usize;
    let mut flagged = 0usize;
    let mut selects_with_limit = 0usize;
    for _ in 0..5000 {
        let s = backend.generate(&schema).unwrap();
        total += 1;
        let is_select = s.sql.trim_start().to_uppercase().starts_with("SELECT")
            || s.sql.trim_start().to_uppercase().starts_with("WITH");
        if is_select {
            selects += 1;
            if s.sql.to_uppercase().contains(" LIMIT ") {
                selects_with_limit += 1;
            }
        }
        if s.has_unordered_limit {
            flagged += 1;
        }
    }
    println!("generated statements            : {total}");
    println!("of which SELECT/WITH            : {selects}");
    println!("SELECTs containing ' LIMIT '    : {selects_with_limit}");
    println!("has_unordered_limit == true     : {flagged}");
    println!(
        "share of SELECTs with the row-count check disabled: {:.1}%",
        100.0 * flagged as f64 / selects as f64
    );
    assert!(flagged > 0, "generator never sets the flag at all");
}
