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
