//! The parser stack-guard class on the wire's route (fastest-engine, lead order 2026-10-15: client
//! SQL must never crash the server). A PostgreSQL client's statement goes through libpg_query, the
//! translator to the engine's AST (`turso_pg_parser::translator`), and the engine. Each recursive
//! entry checks the stack left (`turso_stack_guard`) and a statement too deep for its thread is
//! refused with 54001, not an overflow, which aborts the server process and every session in it.
//!
//! As in core's `stack_guard_tests`: each statement runs in a CHILD process (this test binary re-run
//! on one test by name, `FE_STACK_CHILD` = "<stack KiB>|<sql>"), on a thread of that stack size,
//! through `PgConnection` on a PostgreSQL-dialect database, and writes its outcome to the file
//! `FE_STACK_OUT` names. A dead child is the red.

use crate::session::{open_database_with_io, PgConnection};
use std::sync::Arc;

const CHILD_ENV: &str = "FE_STACK_CHILD";

fn run_child() -> bool {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return false;
    };
    let (kib, sql) = spec.split_once('|').expect("FE_STACK_CHILD is <KiB>|<sql>");
    let kib: usize = kib.parse().expect("FE_STACK_CHILD's stack size is KiB");
    let sql = sql.to_string();
    let outcome = std::thread::Builder::new()
        .stack_size(kib * 1024)
        .spawn(move || {
            let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
            let got = open_database_with_io(io, ":memory:", turso_core::OpenFlags::Create, turso_core::DatabaseOpts::new())
                .and_then(|db| db.connect())
                .and_then(|conn| {
                    let conn = PgConnection::new(conn);
                    conn.prepare(&sql).and_then(|mut s| s.run_collect_rows())
                });
            match got {
                Ok(rows) => format!("ok {rows:?}"),
                Err(e) => format!("err {e:?}"),
            }
        })
        .unwrap()
        .join()
        .unwrap();
    std::fs::write(std::env::var("FE_STACK_OUT").expect("FE_STACK_OUT names the outcome file"), outcome).unwrap();
    true
}

fn in_child(test: &str, kib: usize, sql: &str) -> String {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("outcome");
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--include-ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, format!("{kib}|{sql}"))
        .env("FE_STACK_OUT", &file)
        .output()
        .unwrap();
    match std::fs::read_to_string(&file) {
        Ok(line) if out.status.success() => line,
        _ => {
            use std::os::unix::process::ExitStatusExt;
            format!("died ({:?}, signal {:?})", out.status.code(), out.status.signal())
        }
    }
}

/// Each nesting shape, `depth` levels deep, in PostgreSQL's syntax.
fn shapes(depth: usize) -> Vec<(&'static str, String)> {
    let wrap = |inner: &str, f: &dyn Fn(&str) -> String| {
        let mut s = inner.to_string();
        for _ in 0..depth {
            s = f(&s);
        }
        s
    };
    let mut ctes = vec!["c0 AS (SELECT 1 AS x)".to_string()];
    for i in 1..depth {
        ctes.push(format!("c{i} AS (SELECT x FROM c{})", i - 1));
    }
    vec![
        ("scalar subqueries", format!("SELECT {}", wrap("1", &|s| format!("(SELECT {s})")))),
        ("parentheses", format!("SELECT {}", wrap("1", &|s| format!("({s})")))),
        ("right-nested sums", format!("SELECT {}", wrap("1", &|s| format!("1 + ({s})")))),
        ("function calls", format!("SELECT {}", wrap("1", &|s| format!("abs({s})")))),
        ("CASE", format!("SELECT {}", wrap("1", &|s| format!("CASE WHEN true THEN {s} END")))),
        ("IN subqueries", format!("SELECT {}", wrap("1", &|s| format!("1 IN (SELECT {s})")))),
        ("EXISTS", format!("SELECT {}", wrap("true", &|s| format!("EXISTS (SELECT {s})")))),
        ("derived tables", wrap("SELECT 1 AS x", &|s| format!("SELECT x FROM ({s}) AS t"))),
        ("CTE chain", format!("WITH {} SELECT x FROM c{}", ctes.join(", "), depth.saturating_sub(1))),
    ]
}

/// The class on the wire's route: every shape, 20, 60 and 200 levels deep, on session-sized and
/// smaller threads (the server's session thread is 8 MiB, `SESSION_STACK`): an answer or a refusal,
/// never a dead process. Premise: every shape answers at depth 3 on 8 MiB.
#[test]
fn no_nesting_shape_kills_the_process_on_the_postgresql_route() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::no_nesting_shape_kills_the_process_on_the_postgresql_route";
    for (shape, sql) in shapes(3) {
        let got = in_child(test, 8 * 1024, &sql);
        assert!(got.starts_with("ok"), "premise: {shape} at depth 3 on 8 MiB did not answer: {got} ({sql})");
    }
    let mut died = Vec::new();
    let mut refused = 0;
    for kib in [256usize, 512, 1024, 2048, 8192] {
        for (shape, sql) in [20, 60, 200].into_iter().flat_map(shapes) {
            let got = in_child(test, kib, &sql);
            if got.starts_with("died") {
                died.push(format!("{shape} ({} bytes of SQL) on {kib} KiB: {got}", sql.len()));
            } else if got.contains("StackDepthLimitExceeded") || got.contains("stack depth limit exceeded") {
                refused += 1;
            }
        }
    }
    // libpg_query's own recursion (its C output functions overflowed 8 MiB at 3,000 nested scalar
    // subqueries), on the session's 8 MiB, up to bison's limit (PostgreSQL 18: 3,000 nested scalar
    // subqueries are 54001, 10,000 levels are 42601 "memory exhausted"; raw pg18-nesting-depth.txt).
    for depth in [1000usize, 3000, 6000, 9000] {
        for (shape, sql) in shapes(depth)
            .into_iter()
            .filter(|(s, _)| matches!(*s, "scalar subqueries" | "parentheses" | "right-nested sums" | "function calls"))
        {
            let got = in_child(test, 8 * 1024, &sql);
            if got.starts_with("died") {
                died.push(format!("{shape} at depth {depth} ({} bytes of SQL) on 8 MiB: {got}", sql.len()));
            }
        }
    }
    assert!(died.is_empty(), "CLAIM: client SQL killed the process on the PostgreSQL route ({} cases): {died:?}", died.len());
    assert!(refused > 0, "premise: no case reached a stack guard, so nothing here tested it");
}

/// Diagnostic (prints; asserts nothing): what a session-sized (8 MiB) thread answers to scalar
/// subqueries and parentheses nested ever deeper, so each refusal's layer is known. Run by name
/// with --ignored --nocapture.
#[test]
#[ignore = "diagnostic: run by name with --ignored --nocapture"]
fn diag_postgresql_route_refusals_by_depth() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::diag_postgresql_route_refusals_by_depth";
    for depth in [50usize, 100, 200, 400, 1000, 3000, 10000] {
        for (shape, sql) in shapes(depth).into_iter().filter(|(s, _)| *s == "scalar subqueries" || *s == "parentheses") {
            let got = in_child(test, 8 * 1024, &sql);
            println!("FE_STACK_DIAG depth {depth} {shape}: {}", &got[..got.len().min(160)]);
        }
    }
}


/// PG 18.6 answers this statement: the protobuf codec must not add its own smaller limit.
#[test]
fn a_thousand_nested_scalar_subqueries_answer_one() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::a_thousand_nested_scalar_subqueries_answer_one";
    let sql = format!("SELECT {}1{}", "(SELECT ".repeat(1000), ")".repeat(1000));
    let got = in_child(test, 8 * 1024, &sql);
    assert_eq!(got, "ok [[Numeric(Integer(1))]]", "PG 18.6 answers 1 at depth 1000");
}

#[test]
fn three_thousand_nested_scalar_subqueries_keep_the_stack_refusal() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::three_thousand_nested_scalar_subqueries_keep_the_stack_refusal";
    let sql = format!("SELECT {}1{}", "(SELECT ".repeat(3000), ")".repeat(3000));
    let got = in_child(test, 8 * 1024, &sql);
    assert_eq!(got, "err StackDepthLimitExceeded", "PG 18.6 gives SQLSTATE 54001 at depth 3000");
}

#[test]
fn ten_thousand_nested_scalar_subqueries_keep_the_parser_refusal() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::ten_thousand_nested_scalar_subqueries_keep_the_parser_refusal";
    let sql = format!("SELECT {}1{}", "(SELECT ".repeat(10000), ")".repeat(10000));
    let got = in_child(test, 8 * 1024, &sql);
    assert!(got.starts_with("err ParseError(") && got.contains("memory exhausted"), "PG 18.6 gives SQLSTATE 42601 at depth 10000: {got}");
}
