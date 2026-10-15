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

/// PostgreSQL's message for a statement nested past what its stack allows (SQLSTATE 54001, which the
/// PostgreSQL frontend reads from this prefix).
pub(crate) const STACK_DEPTH_EXCEEDED: &str = "stack depth limit exceeded";

/// The stack a recursive step of planning, optimizing or emitting a nested query may still need
/// below a check: one level of each phase, the deepest of which (a debug build's view expansion)
/// takes about 93 KiB (a 90-view chain overflowed 8 MiB), with margin. Measured by
/// `every_depth_of_a_view_chain_answers_or_is_refused_never_overflows` (postgres/server), which
/// sweeps every depth on 2 and 8 MiB threads and must never overflow. A thread whose whole stack
/// is under 4 times this keeps a quarter of its stack instead: a fixed 512 KiB refused every
/// statement on a 512 KiB thread (Apple's GCD and NSThread default; ultrareview of fastest-wire,
/// finding 3).
const STACK_MARGIN: usize = 512 << 10;

/// One level of the deepest phase (a debug build's view expansion, about 93 KiB), with slack: the
/// least a nested step is ever entered with, however small the thread. A thread that cannot afford
/// one level refuses every nested step and still runs flat statements (fastest-engine's review of
/// ed4e4e9a5: a quarter of a 256 KiB stack is 64 KiB, less than one level).
const LEVEL_COST: usize = 128 << 10;

/// Where no stack bound is known (no platform answer): nested views and subqueries are refused past
/// this depth instead, the only bound left.
const FALLBACK_DEPTH: usize = 64;

std::thread_local! {
    /// The lowest address of this thread's stack and its size, read once (`None` where the
    /// platform does not say).
    static STACK_BOUNDS: Option<(usize, usize)> = stack_bounds();
    /// Nested steps in progress on this thread, for the fallback bound.
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Test builds (C32 MED): this thread's stack taken as unknown, as on a platform with no answer.
    #[cfg(test)]
    pub(crate) static FORCE_UNKNOWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Statements stepping on this thread ([`Executing`]).
    static EXECUTING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(target_vendor = "apple")]
fn stack_bounds() -> Option<(usize, usize)> {
    // SAFETY: pthread_self is always valid for the calling thread; the two getters read its
    // attributes and allocate nothing.
    unsafe {
        let me = libc::pthread_self();
        let top = libc::pthread_get_stackaddr_np(me) as usize;
        let size = libc::pthread_get_stacksize_np(me);
        top.checked_sub(size).map(|bottom| (bottom, size))
    }
}

#[cfg(target_os = "linux")]
fn stack_bounds() -> Option<(usize, usize)> {
    let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    // SAFETY: getattr initializes `attr` on success, which getstack then reads and destroy frees.
    unsafe {
        if libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()) != 0 {
            return None;
        }
        let mut addr = std::ptr::null_mut();
        let mut size = 0;
        let got = libc::pthread_attr_getstack(attr.as_ptr(), &mut addr, &mut size);
        libc::pthread_attr_destroy(attr.as_mut_ptr());
        (got == 0).then_some((addr as usize, size))
    }
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn stack_bounds() -> Option<(usize, usize)> {
    None
}

/// The bytes of stack left on this thread below the caller, where the platform says.
pub(crate) fn remaining_stack() -> Option<usize> {
    let here = 0u8;
    let sp = std::ptr::addr_of!(here) as usize;
    #[cfg(test)]
    if FORCE_UNKNOWN.with(|u| u.get()) {
        return None;
    }
    STACK_BOUNDS.with(|b| b.and_then(|(bottom, _)| sp.checked_sub(bottom)))
}

/// The stack a nested step must find left on this thread: `STACK_MARGIN`, or a quarter of a
/// smaller thread's whole stack, never less than `LEVEL_COST`.
fn margin() -> usize {
    STACK_BOUNDS.with(|b| {
        b.map_or(STACK_MARGIN, |(_, size)| {
            STACK_MARGIN.min(size / 4).max(LEVEL_COST)
        })
    })
}

/// One recursive step of a nested query (a view's expansion, a subquery's planning, optimizing or
/// emission) in progress: entered only with [`margin`] of stack left, so a statement nested past
/// what the thread's stack allows is refused, as PostgreSQL's check_stack_depth refuses it
/// (54001), instead of overflowing (the crash batch, 2026-10-15). The outermost step is never
/// refused: a statement with no nesting runs on whatever stack it was called with, as before the
/// check, unless a statement is stepping on the thread ([`Executing`]): one prepared then (a
/// schema reparse, a callback running SQL) starts deep in the stack, and is checked like a nested
/// step (fastest-engine, on the NestGuard margin). Where the stack is unknown, the bound is
/// `FALLBACK_DEPTH` nested steps. Leaving it, on every exit, pops it.
pub(crate) struct NestGuard;

impl NestGuard {
    pub(crate) fn enter(what: &'static str) -> crate::Result<Self> {
        let depth = DEPTH.with(|d| d.get());
        let refuse = match remaining_stack() {
            Some(left) => (depth > 0 || EXECUTING.with(|e| e.get()) > 0) && left < margin(),
            None => depth >= FALLBACK_DEPTH,
        };
        if refuse {
            crate::bail_parse_error!("{STACK_DEPTH_EXCEEDED}: {what} nested too deep for this thread's stack");
        }
        #[cfg(test)]
        level_cost::record(depth);
        DEPTH.with(|d| d.set(depth + 1));
        Ok(Self)
    }
}

impl Drop for NestGuard {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get() - 1));
    }
}

/// A statement stepping on this thread (`Program::step`), so a statement prepared meanwhile is no
/// outermost step. Leaving it, on every exit, pops it.
pub(crate) struct Executing;

impl Executing {
    pub(crate) fn enter() -> Self {
        EXECUTING.with(|e| e.set(e.get() + 1));
        Self
    }
}

impl Drop for Executing {
    fn drop(&mut self) {
        EXECUTING.with(|e| e.set(e.get() - 1));
    }
}

/// The stack used between two consecutive nested checks on this thread, the largest seen: what
/// one level costs, which the margin must cover (tests only).
#[cfg(test)]
pub(crate) mod level_cost {
    std::thread_local! {
        static LEFT_AT: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
        static WORST: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static REENTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// Outermost steps entered while a statement stepped on this thread, since the last take.
    pub(crate) fn take_reentered() -> usize {
        REENTERED.with(|r| r.replace(0))
    }

    pub(super) fn record(depth: usize) {
        let Some(left) = super::remaining_stack() else {
            return;
        };
        LEFT_AT.with(|at| {
            let mut at = at.borrow_mut();
            at.truncate(depth);
            if depth == 0 && super::EXECUTING.with(|e| e.get()) > 0 {
                REENTERED.with(|r| r.set(r.get() + 1));
            }
            if let Some(&outer) = at.last() {
                WORST.with(|w| w.set(w.get().max(outer.saturating_sub(left))));
            }
            at.push(left);
        });
    }

    pub(crate) fn take_worst() -> usize {
        LEFT_AT.with(|at| at.borrow_mut().clear());
        WORST.with(|w| w.replace(0))
    }
}

#[cfg(test)]
mod tests {
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
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_enters_its_outermost_step_and_still_refuses_deep_nesting() {
        std::thread::Builder::new()
            .stack_size(480 << 10)
            .spawn(|| {
                let size = STACK_BOUNDS.with(|b| b.map(|(_, size)| size));
                assert!(
                    size.is_some_and(|size| size < STACK_MARGIN),
                    "premise: a known stack under 512 KiB, got {size:?}"
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
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_refuses_a_nested_step_it_cannot_afford_one_level_for() {
        #[inline(never)]
        fn one_level() {
            let mut frame = [0u8; 120 << 10];
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
            .stack_size(256 << 10)
            .spawn(|| {
                let _outer = NestGuard::enter("outermost").unwrap();
                assert!(
                    !descend_to(100 << 10),
                    "a nested step was admitted with ~100 KiB left"
                );
                assert!(
                    descend_to(200 << 10),
                    "premise: a nested step with ~200 KiB left runs"
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
                assert!(
                    at(100 << 10, false),
                    "premise: a quiet thread's outermost step runs"
                );
                assert!(
                    !at(100 << 10, true),
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
