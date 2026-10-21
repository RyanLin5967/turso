macro_rules! trace_stack {
    () => {
        let _stack = $crate::stack::trace_scope(concat!("line:", line!()));
    };
    ($label:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope($label);
    };
    ($label:expr, $detail:expr $(,)?) => {
        let _stack = $crate::stack::trace_scope_with_detail($label, $detail);
    };
}
pub(crate) use trace_stack;

#[cfg(feature = "stacker")]
pub(crate) struct TraceGuard {
    label: &'static str,
    detail: Option<&'static str>,
}

#[cfg(feature = "stacker")]
impl TraceGuard {
    fn emit(&self, phase: &'static str) {
        if std::env::var_os("TURSO_TRACE_STACK").is_none() {
            return;
        }

        tracing::debug!(
            target: "turso_stack",
            label = self.label,
            detail = self.detail,
            phase,
            remaining_stack = stacker::remaining_stack(),
            "stacker remaining stack"
        );
    }
}

#[cfg(feature = "stacker")]
impl Drop for TraceGuard {
    fn drop(&mut self) {
        self.emit("exit");
    }
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope(label: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: None,
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_scope_with_detail(label: &'static str, detail: &'static str) -> TraceGuard {
    let guard = TraceGuard {
        label,
        detail: Some(detail),
    };
    guard.emit("enter");
    guard
}

#[cfg(feature = "stacker")]
pub(crate) fn trace_remaining(label: &'static str) {
    if std::env::var_os("TURSO_TRACE_STACK").is_none() {
        return;
    }

    tracing::debug!(
        target: "turso_stack",
        label,
        phase = "sample",
        remaining_stack = stacker::remaining_stack(),
        "stacker remaining stack"
    );
}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope(_label: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_scope_with_detail(_label: &'static str, _detail: &'static str) {}

#[cfg(not(feature = "stacker"))]
#[inline]
pub(crate) fn trace_remaining(_label: &'static str) {}

/// One recursive step of a nested query (a view's expansion, a subquery's planning, optimizing or
/// emission; the crash batch, 2026-10-15) in progress: `turso_stack_guard::NestGuard`, the one
/// primitive the parser, the engine and the PostgreSQL translator share (fastest-engine's
/// stack-guard class, built on this guard as revised on fastest-wire through 17572da73). A refusal
/// is `LimboError::StackDepthLimitExceeded`, PostgreSQL's 54001. Leaving it, on every exit, pops it.
pub(crate) struct NestGuard(#[allow(dead_code)] turso_stack_guard::NestGuard);

impl NestGuard {
    /// `what` names the step for a reader of the call site; PostgreSQL's message names none.
    #[inline]
    #[track_caller]
    pub(crate) fn enter(_what: &'static str) -> crate::Result<Self> {
        turso_stack_guard::NestGuard::enter()
            .map(Self)
            .map_err(|_| crate::LimboError::StackDepthLimitExceeded)
    }
}

/// A statement stepping on this thread (`Program::step`): a statement prepared meanwhile (a schema
/// reparse, a callback running SQL, a trigger compiled at its first fire) is checked from its
/// outermost step.
pub(crate) use turso_stack_guard::Executing;

#[cfg(test)]
mod tests {
    use turso_stack_guard::test_hooks;

    /// The stack between two consecutive nested checks, over views, FROM subqueries, WHERE and
    /// scalar subqueries and a mix of them, stays under LEVEL_COST (fastest-wire 17572da73,
    /// measured in debug on an 8 MiB thread: 73,584 B at most). A lower bound on a level's cost.
    #[test]
    fn two_consecutive_nested_checks_are_less_than_one_level_cost_apart() {
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| {
                let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
                let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
                let conn = db.connect().unwrap();
                conn.execute("CREATE TABLE t(a INTEGER)").unwrap();
                conn.execute("INSERT INTO t VALUES (1)").unwrap();
                conn.execute("CREATE VIEW v0 AS SELECT a FROM t").unwrap();
                for i in 1..12 {
                    conn.execute(format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1)).unwrap();
                }
                let mut from = "SELECT a FROM t".to_string();
                let mut scalar = "SELECT a FROM t".to_string();
                let mut mixed = "SELECT a FROM v3".to_string();
                for _ in 0..12 {
                    from = format!("SELECT a FROM ({from})");
                    scalar = format!("SELECT a FROM t WHERE a = ({scalar})");
                    mixed = format!("SELECT a FROM ({mixed}) WHERE a IN (SELECT a FROM v2 WHERE a = ({mixed}))");
                }
                test_hooks::arm(true);
                test_hooks::take_worst();
                let mut over = Vec::new();
                for sql in ["SELECT a FROM v11".to_string(), from, scalar, mixed] {
                    conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
                    let (worst, at) = test_hooks::take_worst_at();
                    println!("FE_LEVEL_GAP {worst} B {:?}: {}", at.map(|(o, i)| format!("{o} -> {i}")), &sql[..sql.len().min(40)]);
                    if worst == 0 || worst >= turso_stack_guard::LEVEL_COST {
                        let at = at.map(|(o, i)| format!("{o} -> {i}"));
                        over.push(format!("{worst} B between two checks {at:?}: {}", &sql[..sql.len().min(60)]));
                    }
                }
                test_hooks::arm(false);
                assert!(over.is_empty(), "CLAIM: checks further apart than LEVEL_COST {}: {over:?}", turso_stack_guard::LEVEL_COST);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Lead order (ultrareview instance (ii), artie reviews/ULTRAREVIEW-1-wire.md): under INSERT,
    /// UPDATE and DELETE the first subquery's planning and emission held no outer guard, so its
    /// guard was a statement's outermost step, exempt from the check: one level ran unchecked on
    /// whatever stack was left. Each statement's prepare must enter exactly its two outermost steps
    /// (the parser's `parse_stmt` and `translate_inner`), so every subquery step is nested and
    /// checked. Mutant `nest_statement_unguarded` (no guard in translate_inner) must turn it red.
    #[test]
    fn a_dml_statements_first_subquery_is_a_nested_step() {
        let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(a INTEGER)").unwrap();
        let mut bad = Vec::new();
        test_hooks::arm(true);
        for sql in [
            "SELECT (SELECT 1)",
            "INSERT INTO t SELECT (SELECT 1)",
            "INSERT INTO t VALUES ((SELECT 1))",
            "UPDATE t SET a = (SELECT 1)",
            "UPDATE t SET a = 1 WHERE a IN (SELECT 1)",
            "DELETE FROM t WHERE a IN (SELECT 1)",
            "DELETE FROM t WHERE a = (SELECT (SELECT 1))",
        ] {
            test_hooks::take_outermost();
            let prepared = conn.prepare(sql).map(|_| ());
            let outermost = test_hooks::take_outermost();
            if prepared.is_err() || outermost != 2 {
                bad.push(format!("{sql}: {outermost} outermost steps ({prepared:?})"));
            }
        }
        test_hooks::arm(false);
        assert!(bad.is_empty(), "CLAIM: a statement's subquery step was entered as an outermost, unchecked step: {bad:?}");
    }

    /// A real re-entry: a CREATE TABLE's schema reparse prepares a statement inside
    /// `Program::step` (op_parse_schema), and that statement's outermost step is seen as entered
    /// while a statement steps; a flat SELECT is not (fastest-wire 17572da73).
    #[test]
    fn a_schema_reparse_inside_a_step_is_a_reentered_prepare() {
        let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        test_hooks::arm(true);
        test_hooks::take_reentered();
        conn.execute("CREATE TABLE r(a INTEGER)").unwrap();
        let reparse = test_hooks::take_reentered();
        conn.execute("SELECT a FROM r").unwrap();
        let flat = test_hooks::take_reentered();
        test_hooks::arm(false);
        assert!(reparse > 0, "the schema reparse's prepare was not seen inside a step");
        assert_eq!(flat, 0, "a flat SELECT counted as re-entered");
    }
}

/// fastest-wire's own NestGuard tests (db2fad5cf, core/stack.rs), kept verbatim below and run
/// against the one implementation that survived the merge, `turso_stack_guard` (fastest-core
/// integrate, 2026-10-16: wire's NestGuard and the engine's crate were two copies of one class).
/// This shim binds the names wire's private implementation gave those tests to the crate's: its
/// constants, `margin` and `remaining_stack`; `STACK_BOUNDS` to the crate's stack size (the tests
/// read only the size); `FORCE_UNKNOWN` to the crate's force-unknown hook; `level_cost` to the
/// crate's hooks, armed by the first take (the crate records only when armed). No assertion or
/// input of wire's tests is changed, but three tests' sizes, which are derived from the crate's
/// constants since review F1 (FLAGGED there, each at its test).
///
/// `LEVEL_COST` is wire's own literal, 128 KiB, not the crate's (192 KiB in debug): it is the bound
/// of the level-gap test, the one wire test that reads it, and through the crate's constant that
/// bound was loosened by half with no text changed (review of 96b69304b, F2; FLAGGED, a test
/// strength restore).
#[cfg(test)]
mod fastest_wire_shim {
    pub(super) use turso_stack_guard::{margin, remaining_stack, STACK_MARGIN};

    /// fastest-wire's LEVEL_COST (db2fad5cf core/stack.rs), the level-gap test's bound, or the
    /// crate's where that is tighter (release: 32 KiB), so the copy is never looser than the
    /// engine's own (integrate-reviewer-2 L8). Measured worst gap, debug, at 1fcb709ff: 121,504 B
    /// (wire's doc above says 73,584 B, measured before the engine's added checks).
    pub(super) const LEVEL_COST: usize = if (128 << 10) < turso_stack_guard::LEVEL_COST {
        128 << 10
    } else {
        turso_stack_guard::LEVEL_COST
    };

    pub(super) struct StackBounds;
    impl StackBounds {
        pub(super) fn with<R>(&'static self, f: impl FnOnce(&Option<(usize, usize)>) -> R) -> R {
            f(&turso_stack_guard::stack_size().map(|size| (0, size)))
        }
    }
    pub(super) static STACK_BOUNDS: StackBounds = StackBounds;

    pub(super) struct ForceUnknown;
    impl ForceUnknown {
        pub(super) fn with<R>(&'static self, f: impl FnOnce(&std::cell::Cell<bool>) -> R) -> R {
            let cell = std::cell::Cell::new(false);
            let r = f(&cell);
            turso_stack_guard::test_hooks::force_unknown(cell.get());
            r
        }
    }
    pub(super) static FORCE_UNKNOWN: ForceUnknown = ForceUnknown;

    pub(super) mod level_cost {
        use turso_stack_guard::test_hooks;

        pub(crate) fn take_worst() -> usize {
            test_hooks::arm(true);
            test_hooks::take_worst()
        }

        pub(crate) fn take_reentered() -> usize {
            test_hooks::arm(true);
            test_hooks::take_reentered()
        }
    }
}

#[cfg(test)]
mod fastest_wire_tests {
    use super::fastest_wire_shim::*;
    use super::*;

    /// Burns 16 KiB of stack per level until a NestGuard refuses; the levels entered.
    fn nest_until_refused(levels: usize) -> usize {
        let mut frame = [0u8; 16 << 10];
        std::hint::black_box(&mut frame);
        match NestGuard::enter("test") {
            Ok(_guard) => nest_until_refused(levels + 1),
            Err(_) => levels,
        }
    }

    /// On a thread whose whole stack is under 512 KiB (Apple's GCD and NSThread default is 512 KiB;
    /// Rust rounds a 512 KiB request up to 524 KiB, so 480 KiB is asked for) a statement's
    /// outermost step is entered, and nesting is still refused before the stack runs out. The
    /// fixed 512 KiB margin refused the outermost step itself, so no SELECT could prepare there
    /// (ultrareview of fastest-wire, finding 3).
    ///
    /// FLAGGED SPEC CHANGE (review of 96b69304b, F1; lead DECISIONS cc4653c2d3): the thread is
    /// STACK_MARGIN less 32 KiB, the crate's margin of this build (480 KiB in debug, as wire's; 96 KiB
    /// in release, whose STACK_MARGIN is 128 KiB), so the premise holds in both builds: wire's fixed
    /// 480 KiB was not under release's margin, and the premise failed there.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_enters_its_outermost_step_and_still_refuses_deep_nesting() {
        std::thread::Builder::new()
            .stack_size(STACK_MARGIN - (32 << 10))
            .spawn(|| {
                let size = STACK_BOUNDS.with(|b| b.map(|(_, size)| size));
                assert!(
                    size.is_some_and(|size| size < STACK_MARGIN),
                    "premise: a known stack under STACK_MARGIN ({STACK_MARGIN} B), got {size:?}"
                );
                let outer = NestGuard::enter("outermost");
                assert!(
                    outer.is_ok(),
                    "the outermost step was refused on a {size:?} B thread"
                );
                let levels = nest_until_refused(1);
                assert!(levels >= 2, "only {levels} levels on a {size:?} B thread");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// C32 MED (a Claude review, 2026-10-15): where the stack is unknown, the 64-deep fallback admitted
    /// about 32 views (two steps each), which overflow a 2 MiB thread in a debug build (Windows'
    /// spawned-thread default; wasm32 has no guard page). On a 2 MiB thread taken as unknown, nested
    /// steps of 120 KiB each must be refused before the stack runs out. Overflowing aborts the test
    /// process: that is the red.
    #[test]
    fn an_unknown_stack_refuses_nesting_instead_of_guessing_a_depth() {
        #[inline(never)]
        fn nest(levels: usize) -> usize {
            let mut frame = [0u8; 120 << 10];
            std::hint::black_box(&mut frame);
            match NestGuard::enter("nested") {
                Ok(_guard) => nest(levels + 1),
                Err(_) => levels,
            }
        }
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(|| {
                FORCE_UNKNOWN.with(|u| u.set(true));
                assert_eq!(remaining_stack(), None, "premise: the stack is unknown");
                let outer = NestGuard::enter("outermost");
                assert!(outer.is_ok(), "the outermost step was refused");
                let levels = nest(0);
                assert!(levels * (120 << 10) < (2 << 20), "{levels} nested levels of 120 KiB on a 2 MiB thread");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A nested step that would leave less than one level's stack is refused on a small thread:
    /// on a 256 KiB thread a quarter of the stack (64 KiB) is less than one level of the deepest
    /// phase (a debug view expansion, about 93 KiB), so with the quarter-stack margin alone a
    /// nested step was admitted with ~100 KiB left and a 120 KiB level then overflowed (fastest-
    /// engine's review of ed4e4e9a5). Overflowing aborts the test process: that is the red.
    ///
    /// FLAGGED SPEC CHANGE (review of 96b69304b, F1; lead DECISIONS cc4653c2d3): every size is
    /// derived from the crate's constants of this build, so the test can fail in release too (wire's
    /// literals overflowed there: release's margin on 256 KiB is 64 KiB, and 100 KiB left was
    /// admitted by design). The thread is 3 x LEVEL_COST, so a quarter of it (0.75 LEVEL_COST) is
    /// under one level and the floor decides; the step is tried with 0.875 LEVEL_COST left, which
    /// the floor refuses and the quarter-stack rule alone would admit; one synthetic level is
    /// LEVEL_COST + 8 KiB, more than LEVEL_COST and more than that, so a wrong admission overflows.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_refuses_a_nested_step_it_cannot_afford_one_level_for() {
        const LEVEL: usize = turso_stack_guard::LEVEL_COST;
        const FRAME: usize = LEVEL + (8 << 10);
        const THREAD: usize = 3 * LEVEL;
        #[inline(never)]
        fn one_level() {
            let mut frame = [0u8; FRAME];
            std::hint::black_box(&mut frame);
        }
        #[inline(never)]
        fn descend_to(left: usize) -> bool {
            if remaining_stack().unwrap() > left {
                let mut frame = [0u8; 1 << 10];
                std::hint::black_box(&mut frame);
                return descend_to(left);
            }
            match NestGuard::enter("nested") {
                Ok(_guard) => {
                    one_level();
                    true
                }
                Err(_) => false,
            }
        }
        std::thread::Builder::new()
            .stack_size(THREAD)
            .spawn(|| {
                let size = STACK_BOUNDS.with(|b| b.map(|(_, size)| size)).unwrap();
                let short = (size / 4 + LEVEL) / 2;
                let enough = LEVEL + FRAME + (8 << 10);
                assert!(FRAME > LEVEL, "premise: one synthetic level ({FRAME} B) exceeds LEVEL_COST ({LEVEL} B)");
                // From the constants alone, not from the margin under test: a guard that lost its
                // floor admits the step below and overflows on one level (that is the red).
                assert!(
                    size / 4 < short && short < LEVEL,
                    "premise: the floor decides: quarter {} < {short} B left < LEVEL_COST {LEVEL}",
                    size / 4
                );
                assert!(FRAME > short, "premise: a wrong admission with {short} B left overflows on {FRAME} B");
                let _outer = NestGuard::enter("outermost").unwrap();
                assert!(
                    remaining_stack().unwrap() > enough,
                    "premise: {enough} B left to start from on a {size} B thread"
                );
                assert!(
                    !descend_to(short),
                    "a nested step was admitted with {short} B left"
                );
                assert!(
                    descend_to(enough),
                    "premise: a nested step with {enough} B left runs"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// The stack between two consecutive nested checks, over views, FROM subqueries, WHERE and
    /// scalar subqueries and a mix of them, stays under LEVEL_COST (measured in debug on an 8 MiB
    /// thread: 73,584 B at most). A lower bound on a level's cost: what the innermost level uses
    /// below its own check is not measured. A WHERE or scalar subquery's emission was not checked, so two
    /// checks were 481,216 B apart on the mix (fastest-engine, on the NestGuard margin). Not
    /// covered: the parser, which recurses with no check (debug: 211 KB for a flat SELECT,
    /// about 180 KB per scalar subquery level), and release frames (no release build here).
    #[test]
    fn two_consecutive_nested_checks_are_less_than_one_level_cost_apart() {
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| {
                let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
                let db = crate::Database::open_file(
                    io,
                    ":memory:",
                    std::sync::Arc::new(crate::SqliteDialect),
                )
                .unwrap();
                let conn = db.connect().unwrap();
                conn.execute("CREATE TABLE t(a INTEGER)").unwrap();
                conn.execute("INSERT INTO t VALUES (1)").unwrap();
                conn.execute("CREATE VIEW v0 AS SELECT a FROM t").unwrap();
                for i in 1..12 {
                    conn.execute(format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1))
                        .unwrap();
                }
                let mut from = "SELECT a FROM t".to_string();
                let mut scalar = "SELECT a FROM t".to_string();
                let mut mixed = "SELECT a FROM v3".to_string();
                for _ in 0..12 {
                    from = format!("SELECT a FROM ({from})");
                    scalar = format!("SELECT a FROM t WHERE a = ({scalar})");
                    mixed = format!(
                        "SELECT a FROM ({mixed}) WHERE a IN (SELECT a FROM v2 WHERE a = ({mixed}))"
                    );
                }
                level_cost::take_worst();
                for sql in ["SELECT a FROM v11".to_string(), from, scalar, mixed] {
                    conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
                    let worst = level_cost::take_worst();
                    assert!(
                        worst > 0 && worst < LEVEL_COST,
                        "{worst} B between two checks (LEVEL_COST {LEVEL_COST}): {sql}"
                    );
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A statement prepared while another steps on the thread is checked from its outermost step:
    /// with ~100 KiB left (under LEVEL_COST) it is refused, where a flat statement on a quiet
    /// thread is not. It was exempt, so a prepare re-entered deep in the stack went unchecked
    /// (fastest-engine, on the NestGuard margin).
    ///
    /// FLAGGED SPEC CHANGE (review of 96b69304b, F1; lead DECISIONS cc4653c2d3): the stack left is
    /// LEVEL_COST less 8 KiB, the crate's floor of this build, with the premise that it is under the
    /// thread's margin: wire's 100 KiB is above release's 64 KiB margin on 256 KiB, so it was admitted
    /// there by design and the assertion failed.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_prepare_reentered_while_a_statement_steps_is_checked_from_its_first_step() {
        #[inline(never)]
        fn at(left: usize, executing: bool) -> bool {
            if remaining_stack().unwrap() > left {
                let mut frame = [0u8; 1 << 10];
                std::hint::black_box(&mut frame);
                return at(left, executing);
            }
            let _executing = executing.then(Executing::enter);
            NestGuard::enter("reentered").is_ok()
        }
        std::thread::Builder::new()
            .stack_size(256 << 10)
            .spawn(|| {
                let short = turso_stack_guard::LEVEL_COST - (8 << 10);
                assert!(short < margin(), "premise: {short} B left is under the margin {}", margin());
                assert!(
                    at(short, false),
                    "premise: a quiet thread's outermost step runs"
                );
                assert!(
                    !at(short, true),
                    "a re-entered prepare's first step was exempt"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A real re-entry: a CREATE TABLE's schema reparse prepares a statement inside
    /// `Program::step` (op_parse_schema), and that statement's outermost step is seen as entered
    /// while a statement steps. Without `Executing` held by `Program::step` it is seen as a quiet
    /// thread's, exempt (fresh-context review of bace2e9d0: the earlier test held `Executing` by
    /// hand, so it passed with the line in `step` deleted).
    #[test]
    fn a_schema_reparse_inside_a_step_is_a_reentered_prepare() {
        let io: std::sync::Arc<dyn crate::IO> = std::sync::Arc::new(crate::MemoryIO::new());
        let db = crate::Database::open_file(io, ":memory:", std::sync::Arc::new(crate::SqliteDialect))
            .unwrap();
        let conn = db.connect().unwrap();
        level_cost::take_reentered();
        conn.execute("CREATE TABLE r(a INTEGER)").unwrap();
        assert!(
            level_cost::take_reentered() > 0,
            "the schema reparse's prepare was not seen inside a step"
        );
        conn.execute("SELECT a FROM r").unwrap();
        assert_eq!(level_cost::take_reentered(), 0, "a flat SELECT counted as re-entered");
    }

    /// On an 8 MiB thread the margin is the full STACK_MARGIN, as before.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_large_thread_keeps_the_full_margin() {
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| assert_eq!(margin(), STACK_MARGIN))
            .unwrap()
            .join()
            .unwrap();
    }
}
