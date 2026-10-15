//! The parser stack-guard class (lead order 2026-10-15): client SQL must never crash the server.
//! Every recursive entry of the parser and the engine checks the stack it has left
//! (`turso_stack_guard`), and a statement too deep for its thread is refused with PostgreSQL's
//! 54001 stack_depth_limit_exceeded rather than overflowing the thread, which aborts the process.
//!
//! A stack overflow aborts the whole process, so each statement runs in a CHILD process: this test
//! binary re-run on one test by name, with `FE_STACK_CHILD` = "<stack KiB>|<sql>". The child runs
//! the statement on a thread of that stack size and prints `FE_STACK_RESULT <outcome>`. The parent
//! judges the child's exit and its line.

use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect, IO};
use std::sync::Arc;

const CHILD_ENV: &str = "FE_STACK_CHILD";

/// In the child: run the statement and print its outcome. Returns false in the parent.
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
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io,
                ":memory:",
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            match conn.prepare(&sql).and_then(|mut s| s.run_collect_rows()) {
                Ok(rows) => format!("ok {rows:?}"),
                Err(e) => format!("err {:?}", e),
            }
        })
        .unwrap()
        .join()
        .unwrap();
    println!("FE_STACK_RESULT {outcome}");
    true
}

/// In the parent: the child's outcome line, or how it died.
fn in_child(test: &str, kib: usize, sql: &str) -> String {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, format!("{kib}|{sql}"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    match stdout.lines().find_map(|l| l.strip_prefix("FE_STACK_RESULT ")) {
        Some(line) if out.status.success() => line.to_string(),
        _ => {
            use std::os::unix::process::ExitStatusExt;
            format!("died ({:?}, signal {:?})", out.status.code(), out.status.signal())
        }
    }
}

/// `SELECT (SELECT ... (SELECT 1) ...)`, `depth` scalar subqueries deep.
fn nested_scalar_subqueries(depth: usize) -> String {
    let mut sql = "1".to_string();
    for _ in 0..depth {
        sql = format!("(SELECT {sql})");
    }
    format!("SELECT {sql}")
}

/// Red first (lead order): a 2-level scalar subquery on a 256 KiB thread overflowed the stack and
/// aborted the process. It must be refused with 54001 (`LimboError::StackDepthLimitExceeded`), and
/// the same statement on an 8 MiB thread (the main thread's size) must still answer 1, the control.
#[test]
fn a_statement_too_deep_for_its_stack_is_refused_with_54001_not_a_crash() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::a_statement_too_deep_for_its_stack_is_refused_with_54001_not_a_crash";
    let sql = nested_scalar_subqueries(2);
    let control = in_child(test, 8 * 1024, &sql);
    assert!(control.starts_with("ok [[Numeric(Integer(1))]]") || control.contains("Integer(1)"), "premise: {sql} on 8 MiB gave {control}");
    let small = in_child(test, 256, &sql);
    assert!(
        small.starts_with("err StackDepthLimitExceeded"),
        "CLAIM: {sql} on a 256 KiB thread was not refused with 54001 (stack_depth_limit_exceeded): {small}"
    );
}

/// Diagnostic (prints; asserts nothing): which nesting depth each thread stack size survives, to
/// size the guard's red zone from what one level costs. Run by name with --ignored --nocapture.
#[test]
#[ignore = "diagnostic: run by name with --ignored --nocapture"]
fn diag_stack_needed_per_nesting_level() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::diag_stack_needed_per_nesting_level";
    for kib in [64usize, 128, 192, 256, 384, 512, 1024, 2048] {
        let mut row = Vec::new();
        for depth in [0usize, 1, 2, 3, 4, 6, 8] {
            let got = in_child(test, kib, &nested_scalar_subqueries(depth));
            row.push(format!("d{depth}={}", if got.starts_with("ok") { "ok" } else if got.starts_with("died") { "DIED" } else { "err" }));
        }
        println!("FE_STACK_DIAG {kib} KiB: {}", row.join(" "));
    }
}

/// Each nesting shape this class knows, `depth` levels deep.
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
        ("scalar subqueries", nested_scalar_subqueries(depth)),
        ("parentheses", format!("SELECT {}", wrap("1", &|s| format!("({s})")))),
        ("right-nested sums", format!("SELECT {}", wrap("1", &|s| format!("1 + ({s})")))),
        ("function calls", format!("SELECT {}", wrap("1", &|s| format!("abs({s})")))),
        ("CASE", format!("SELECT {}", wrap("1", &|s| format!("CASE WHEN 1 THEN {s} END")))),
        ("IN subqueries", format!("SELECT {}", wrap("1", &|s| format!("1 IN (SELECT {s})")))),
        ("EXISTS", format!("SELECT {}", wrap("1", &|s| format!("EXISTS (SELECT {s})")))),
        ("derived tables", wrap("SELECT 1 AS x", &|s| format!("SELECT x FROM ({s})"))),
        ("parenthesised FROM", format!("SELECT 1 FROM {}", wrap("(SELECT 1)", &|s| format!("({s})")))),
        ("CTE chain", format!("WITH {} SELECT x FROM c{}", ctes.join(", "), depth.saturating_sub(1))),
    ]
}

/// The class (lead order): client SQL must never crash the server. Every nesting shape above, 60
/// levels deep, on threads of 128 KiB to 1 MiB: each outcome is an answer or a refusal, never a
/// dead process. Premise: each shape answers, or is refused by a fixed depth bound, on 8 MiB at
/// depth 3 (so the shapes are valid SQL, and the guard has a case it must pass).
#[test]
fn no_nesting_shape_kills_the_process_on_any_thread_stack() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::no_nesting_shape_kills_the_process_on_any_thread_stack";
    for (shape, sql) in shapes(3) {
        let got = in_child(test, 8 * 1024, &sql);
        assert!(got.starts_with("ok"), "premise: {shape} at depth 3 on 8 MiB did not answer: {got} ({sql})");
    }
    let mut died = Vec::new();
    let mut refused = 0;
    for kib in [128usize, 256, 512, 1024] {
        for (shape, sql) in shapes(60) {
            let got = in_child(test, kib, &sql);
            if got.starts_with("died") {
                died.push(format!("{shape} on {kib} KiB: {got}"));
            } else if got.starts_with("err StackDepthLimitExceeded") {
                refused += 1;
            }
        }
    }
    assert!(died.is_empty(), "CLAIM: client SQL killed the process ({} cases): {died:?}", died.len());
    assert!(refused > 0, "premise: no case reached the stack guard, so nothing here tested it");
}
