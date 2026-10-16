//! The parser stack guard: client SQL must never crash the server.
//! Every recursive entry of the parser and the engine checks the stack it has left
//! (`turso_stack_guard`), and a statement too deep for its thread is refused with PostgreSQL's
//! 54001 stack_depth_limit_exceeded rather than overflowing the thread, which aborts the process.
//!
//! A stack overflow aborts the whole process, so each statement runs in a CHILD process: this test
//! binary re-run on one test by name, with `FE_STACK_CHILD` = "<stack KiB>|<sql>". The child runs
//! the statement on a thread of that stack size and writes its outcome to the file `FE_STACK_OUT`
//! names (a file, not stdout: libtest joins a child's printed line to its own "test ..." line). The
//! parent judges the child's exit and that file.

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
            // The open and the connect are outcomes too: on a thread inside the red zone they are
            // refused like any statement.
            let got = Database::open_file_with_flags(
                io,
                ":memory:",
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .and_then(|db| db.connect())
            .and_then(|conn| conn.prepare(&sql).and_then(|mut s| s.run_collect_rows()));
            match got {
                Ok(rows) => format!("ok {rows:?}"),
                Err(e) => format!("err {:?}", e),
            }
        })
        .unwrap()
        .join()
        .unwrap();
    std::fs::write(std::env::var("FE_STACK_OUT").expect("FE_STACK_OUT names the outcome file"), outcome).unwrap();
    true
}

/// In the parent: the child's outcome line, or how it died.
fn in_child(test: &str, kib: usize, sql: &str) -> String {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("outcome");
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        // --include-ignored: the diagnostic is #[ignore]d, and its child runs it by name too.
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

/// `SELECT (SELECT ... (SELECT 1) ...)`, `depth` scalar subqueries deep.
fn nested_scalar_subqueries(depth: usize) -> String {
    let mut sql = "1".to_string();
    for _ in 0..depth {
        sql = format!("(SELECT {sql})");
    }
    format!("SELECT {sql}")
}

/// A 2-level scalar subquery on a 256 KiB thread overflowed the stack and aborted the process. A
/// statement too deep for its thread must be refused with 54001
/// (`LimboError::StackDepthLimitExceeded`), and the same statement on an 8 MiB thread (the main
/// thread's size) must still answer, the control. Nested deeper until the 256 KiB thread refuses
/// it: a debug build refuses at depth 2, and a release build's smaller frames answer depth 8
/// there (`diag_stack_needed_per_nesting_level`, release).
#[test]
fn a_statement_too_deep_for_its_stack_is_refused_with_54001_not_a_crash() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::a_statement_too_deep_for_its_stack_is_refused_with_54001_not_a_crash";
    let mut answered = Vec::new();
    for depth in [2usize, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64] {
        let sql = nested_scalar_subqueries(depth);
        let small = in_child(test, 256, &sql);
        if small.starts_with("ok") {
            answered.push(depth);
            continue;
        }
        assert!(
            small.starts_with("err StackDepthLimitExceeded"),
            "CLAIM: {depth} nested scalar subqueries on a 256 KiB thread were not refused with 54001 (stack_depth_limit_exceeded): {small}"
        );
        let control = in_child(test, 8 * 1024, &sql);
        assert!(control.starts_with("ok [[Numeric(Integer(1))]]"), "premise: {sql} on 8 MiB gave {control}");
        println!("FE_STACK_REFUSED at depth {depth} on 256 KiB (answered {answered:?})");
        return;
    }
    panic!("premise: a 256 KiB thread answered every depth tried ({answered:?}), so nothing was refused");
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

/// Client SQL must never crash the server. Every nesting shape above, 20,
/// 60 and 200 levels deep, on threads of 128 KiB to 8 MiB: each outcome is an answer or a refusal,
/// never a dead process. Premise: each shape answers on 8 MiB at depth 3 (so the shapes are valid SQL, and
/// the guard has a case it must pass), but a parenthesised FROM, which the planner refuses after
/// the parser has recursed through it.
#[test]
fn no_nesting_shape_kills_the_process_on_any_thread_stack() {
    if run_child() {
        return;
    }
    let test = "stack_guard_tests::no_nesting_shape_kills_the_process_on_any_thread_stack";
    for (shape, sql) in shapes(3) {
        let got = in_child(test, 8 * 1024, &sql);
        // A parenthesised FROM parses (its recursion is the parser's) and the planner refuses it.
        let valid = got.starts_with("ok")
            || (shape == "parenthesised FROM" && got.contains("Parenthesized FROM clause subqueries are not supported"));
        assert!(valid, "premise: {shape} at depth 3 on 8 MiB did not answer: {got} ({sql})");
    }
    let mut died = Vec::new();
    let mut refused = 0;
    for kib in [128usize, 256, 512, 1024, 2048, 8192] {
        for (shape, sql) in [20, 60, 200].into_iter().flat_map(shapes) {
            let got = in_child(test, kib, &sql);
            if got.starts_with("died") {
                died.push(format!("{shape} ({} bytes of SQL) on {kib} KiB: {got}", sql.len()));
            } else if got.starts_with("err StackDepthLimitExceeded") {
                refused += 1;
            }
        }
    }
    assert!(died.is_empty(), "CLAIM: client SQL killed the process ({} cases): {died:?}", died.len());
    assert!(refused > 0, "premise: no case reached the stack guard, so nothing here tested it");
}

/// A DDL statement's schema reparse is a prepare inside a
/// step, so it is checked from its first step (`Executing`), and refused with less than `margin()`
/// left. Diagnostic (prints; asserts nothing): the stack each reparse really uses from that point,
/// measured by painting the unused stack of an 8 MiB thread with a pattern, running the DDL, and
/// finding the lowest byte it overwrote; minus the stack left at the re-entered step
/// (`test_hooks::take_reentered_left`). Run by name with --ignored --nocapture, in a debug and a
/// release build.
#[test]
#[ignore = "diagnostic: run by name with --ignored --nocapture (debug and --release)"]
fn diag_schema_reparse_stack_cost() {
    const PATTERN: u8 = 0xA5;
    // The bottom of this thread's stack and its current depth.
    #[inline(never)]
    fn bounds() -> (usize, usize) {
        assert!(turso_stack_guard::stack_size().is_some(), "premise: a stack the platform reports");
        let left = turso_stack_guard::remaining_stack();
        let here = 0u8;
        let sp = std::ptr::addr_of!(here) as usize;
        (sp - left, sp)
    }
    #[inline(never)]
    fn paint() {
        let (bottom, sp) = bounds();
        let (from, to) = (bottom + (64 << 10), sp - (32 << 10));
        // SAFETY: [from, to) is this thread's own mapped stack, below every live frame (32 KiB
        // under this one, which calls nothing more), above its guard page.
        unsafe { std::ptr::write_bytes(from as *mut u8, PATTERN, to - from) };
    }
    #[inline(never)]
    fn untouched() -> usize {
        let (bottom, _) = bounds();
        let mut at = bottom + (64 << 10);
        // SAFETY: as in `paint`, read-only.
        while unsafe { std::ptr::read_volatile(at as *const u8) } == PATTERN {
            at += 1;
        }
        at - bottom
    }
    std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(|| {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
            let wide = (0..100).map(|i| format!("c{i} INTEGER CHECK (c{i} > 0)")).collect::<Vec<_>>().join(", ");
            conn.execute("CREATE TABLE base(a INTEGER, b TEXT)").unwrap();
            let mut nested = "SELECT a FROM base".to_string();
            for _ in 0..8 {
                nested = format!("SELECT a FROM ({nested}) WHERE a IN (SELECT a FROM base)");
            }
            for (label, sql) in [
                ("CREATE TABLE", "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)".to_string()),
                ("CREATE TABLE, 100 CHECK columns", format!("CREATE TABLE t2({wide})")),
                ("CREATE INDEX", "CREATE INDEX i1 ON base(a, b)".to_string()),
                ("CREATE VIEW, 8 nested levels", format!("CREATE VIEW v1 AS {nested}")),
                ("ALTER TABLE ADD COLUMN", "ALTER TABLE base ADD COLUMN c INTEGER".to_string()),
            ] {
                turso_stack_guard::test_hooks::arm(true);
                turso_stack_guard::test_hooks::take_reentered_left();
                paint();
                conn.execute(&sql).unwrap();
                let low = untouched();
                let at_reentry = turso_stack_guard::test_hooks::take_reentered_left();
                turso_stack_guard::test_hooks::arm(false);
                match at_reentry {
                    Some(left) => println!(
                        "FE_REPARSE {label}: {} B from the re-entered step to the deepest byte (left at re-entry {left} B, deepest {low} B above the bottom)",
                        left.saturating_sub(low)
                    ),
                    None => println!("FE_REPARSE {label}: no re-entered step (deepest {low} B above the bottom)"),
                }
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
