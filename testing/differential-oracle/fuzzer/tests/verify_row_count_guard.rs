//! Independent verification of the unordered-LIMIT row-COUNT guard added in
//! 8ee8aa656. Written from scratch, not derived from the author's test, and it
//! drives the REAL engines rather than handing the oracle made-up rows.
//!
//! The claim under test, from the commit message: an unordered LIMIT excuses
//! WHICH rows come back but not HOW MANY, and the two verified Turso bugs
//! below "have exactly this shape", so both must now be reported.

use std::sync::Arc;

use differential_fuzzer::oracle::QueryResult;
use differential_fuzzer::{
    DifferentialOracle, GeneratedStatement, MemorySimIO, Oracle, OracleResult, check_differential,
};
use sql_gen::ast::{
    ColumnRef, FromClause, FunctionCallExpr, JoinClause, JoinType, OrderByItem, OrderDirection,
    SelectColumn, SelectStmt, Stmt,
};
use sql_gen::{ColumnDef, DataType, Expr, Schema, SchemaBuilder, Table};
use turso_core::{Database, SqliteDialect};

/// The exact four lines generate.rs runs for every statement the fuzzer makes.
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

fn shape(r: &QueryResult) -> String {
    match r {
        QueryResult::Rows(rows) => format!("Rows({} row(s))", rows.len()),
        QueryResult::Ok => "Ok  <- an EMPTY result set, 0 rows".to_string(),
        QueryResult::Error(e) => format!("Error({e})"),
    }
}

fn n_rows(r: &QueryResult) -> usize {
    match r {
        QueryResult::Rows(rows) => rows.len(),
        QueryResult::Ok => 0,
        QueryResult::Error(_) => usize::MAX,
    }
}

fn verdict_str(v: &OracleResult) -> String {
    match v {
        OracleResult::Pass => "PASS".to_string(),
        OracleResult::Skipped(r) => format!("SKIPPED {r}"),
        OracleResult::Warning(r) => format!("WARNING {r}"),
        OracleResult::Fail(r) => format!("FAIL {r}"),
    }
}

struct Engines {
    turso: Arc<turso_core::Connection>,
    sqlite: rusqlite::Connection,
}

fn engines(tag: &str, seed: u64, setup: &[&str]) -> Engines {
    let io = Arc::new(MemorySimIO::new(seed));
    let db = Database::open_file_with_flags(
        io,
        &format!("verify-row-count-guard-{tag}.db"),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let turso = db.connect().unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        assert!(
            !DifferentialOracle::execute_turso(&turso, sql).is_error(),
            "turso setup failed: {sql}"
        );
        assert!(
            !DifferentialOracle::execute_sqlite(&sqlite, sql).is_error(),
            "sqlite setup failed: {sql}"
        );
    }
    Engines { turso, sqlite }
}

/// BUG 1, the join case. aa has 2 rows, bb has 3, so the cross join has 6 and
/// `LIMIT 10 OFFSET 1` must return 5 on any correct engine. Turso returns 3.
/// Both sides are non-empty, so this lands in the (Rows, Rows) arm the fix
/// actually edited.
#[test]
fn bug1_join_limit_offset_count_divergence_is_reported() {
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
    let e = engines(
        "bug1",
        11,
        &[
            "CREATE TABLE aa(x INTEGER)",
            "CREATE TABLE bb(y INTEGER)",
            "INSERT INTO aa VALUES (1),(2)",
            "INSERT INTO bb VALUES (10),(20),(30)",
        ],
    );

    let stmt = Stmt::Select(SelectStmt {
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
    });
    let g = classify(&stmt, &schema);
    let t = DifferentialOracle::execute_turso(&e.turso, &g.sql);
    let s = DifferentialOracle::execute_sqlite(&e.sqlite, &g.sql);
    let v = check_differential(&e.turso, &e.sqlite, &schema, &g);

    println!("=== BUG 1: {} ===", g.sql);
    println!("  has_unordered_limit : {}", g.has_unordered_limit);
    println!("  reason              : {:?}", g.unordered_limit_reason);
    println!("  turso               : {}", shape(&t));
    println!("  sqlite              : {}", shape(&s));
    println!("  oracle              : {}", verdict_str(&v));

    assert!(
        g.has_unordered_limit,
        "the generator's own classifier must flag this"
    );
    assert_eq!(
        (n_rows(&t), n_rows(&s)),
        (3, 5),
        "engine ground truth changed"
    );
    assert!(
        v.is_fail(),
        "PATCHED oracle must report this. Got: {}",
        verdict_str(&v)
    );
    match &v {
        OracleResult::Fail(m) => assert!(m.contains("Row COUNT mismatch"), "wrong reason: {m}"),
        _ => unreachable!(),
    }
}

/// BUG 2, the `LIMIT 0` aggregate. SQLite returns zero rows; Turso returns one.
/// The commit says this bug "has exactly this shape" and is now reported.
/// It is a row-COUNT divergence, 1 against 0, with the flag set.
#[test]
fn bug2_limit_zero_aggregate_count_divergence_is_reported() {
    let schema = SchemaBuilder::new()
        .table(Table::new(
            "t",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("a", DataType::Integer),
            ],
        ))
        .build();
    let e = engines(
        "bug2",
        7,
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
            "INSERT INTO t VALUES (1, 10)",
        ],
    );

    let base = SelectStmt {
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
    };

    // Same bug written two ways. The ORDER BY over the INTEGER PRIMARY KEY
    // clears the flag without changing what either engine returns, so it
    // isolates the exemption as the cause.
    let mut ordered = base.clone();
    ordered.order_by = vec![OrderByItem {
        expr: Expr::ColumnRef(ColumnRef {
            table: None,
            column: "id".to_string(),
        }),
        direction: OrderDirection::Asc,
        nulls: None,
    }];

    let mut flagged_verdict = None;
    for (label, st) in [
        ("A: no ORDER BY, flag SET", Stmt::Select(base)),
        ("B: ORDER BY id,  flag CLEAR", Stmt::Select(ordered)),
    ] {
        let g = classify(&st, &schema);
        let t = DifferentialOracle::execute_turso(&e.turso, &g.sql);
        let s = DifferentialOracle::execute_sqlite(&e.sqlite, &g.sql);
        let v = check_differential(&e.turso, &e.sqlite, &schema, &g);
        println!("=== BUG 2 {label} ===");
        println!("  SQL                 : {}", g.sql);
        println!("  has_unordered_limit : {}", g.has_unordered_limit);
        println!("  turso               : {}", shape(&t));
        println!("  sqlite              : {}", shape(&s));
        println!(
            "  row counts          : turso {} vs sqlite {}",
            n_rows(&t),
            n_rows(&s)
        );
        println!(
            "  match arm taken     : {}",
            match (&t, &s) {
                (QueryResult::Rows(_), QueryResult::Rows(_)) =>
                    "(Rows, Rows) <- the arm the fix edited",
                (QueryResult::Rows(_), QueryResult::Ok) => "(Rows, Ok)   <- a DIFFERENT arm",
                _ => "other",
            }
        );
        println!("  oracle              : {}", verdict_str(&v));
        assert_eq!(
            (n_rows(&t), n_rows(&s)),
            (1, 0),
            "engine ground truth changed"
        );
        if g.has_unordered_limit {
            flagged_verdict = Some(v);
        } else {
            assert!(
                v.is_fail(),
                "flag-clear control must fail: {}",
                verdict_str(&v)
            );
        }
    }

    let v = flagged_verdict.expect("statement A must be flagged");
    assert!(
        v.is_fail(),
        "PATCHED oracle must report bug 2's 1-vs-0 row count. Got: {}",
        verdict_str(&v)
    );
}

/// The half of the exemption that must SURVIVE: same count, different rows,
/// flag set. Both result sets here were produced by the real engines, then
/// paired, so nothing is invented. This must stay a Warning or the fix has
/// turned legitimate non-determinism into noise.
#[test]
fn same_count_different_rows_must_still_be_only_a_warning() {
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
    let e = engines(
        "warn",
        13,
        &[
            "CREATE TABLE aa(x INTEGER)",
            "CREATE TABLE bb(y INTEGER)",
            "INSERT INTO aa VALUES (1),(2)",
            "INSERT INTO bb VALUES (10),(20),(30)",
        ],
    );

    // Real 3-row results from each engine, with no row in common.
    let turso_side = DifferentialOracle::execute_turso(
        &e.turso,
        "SELECT x FROM aa CROSS JOIN bb LIMIT 10 OFFSET 1",
    );
    let sqlite_side = DifferentialOracle::execute_sqlite(&e.sqlite, "SELECT y FROM bb LIMIT 3");
    assert_eq!(n_rows(&turso_side), 3);
    assert_eq!(n_rows(&sqlite_side), 3);

    let stmt = Stmt::Select(SelectStmt {
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
        joins: vec![],
        where_clause: None,
        group_by: None,
        compounds: vec![],
        order_by: vec![],
        limit: Some(3),
        offset: None,
    });
    let g = classify(&stmt, &schema);
    assert!(g.has_unordered_limit);

    let v = DifferentialOracle.check(&g, &turso_side, &sqlite_side);
    println!("=== CONTROL: same count (3 vs 3), disjoint rows, flag SET ===");
    println!("  oracle              : {}", verdict_str(&v));
    assert!(
        v.is_warning(),
        "the exemption must survive for same-count-different-rows. Got: {}",
        verdict_str(&v)
    );
}
