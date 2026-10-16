//! The one stack guard of the parser and the engine: `NestGuard`, refusing a recursive step that
//! would overflow the thread's stack, which aborts the whole server process: client SQL must never
//! crash the server.
//!
//! This is core's `stack::NestGuard` (the small-thread margin, `LEVEL_COST`, the outermost
//! exemption, `Executing`, refusing all nesting where the stack is unknown) moved into a crate of
//! its own, so that `turso_parser` and `turso_pg_parser`, which core depends on and which cannot
//! depend on core, use the same primitive with the same thread-local depth. Core wraps it
//! (`core::stack::NestGuard`) and reports a refusal as `LimboError::StackDepthLimitExceeded`, which
//! a PostgreSQL client receives as 54001 stack_depth_limit_exceeded, as PostgreSQL's own
//! `check_stack_depth()` refuses.
//!
//! A step is refused when the thread has less than [`margin`] of stack left, except the outermost
//! step of a statement on a thread where no statement is stepping: a flat statement runs on
//! whatever stack it was called with, as before any check. One prepared while a statement steps
//! (a schema reparse, a callback running SQL) starts deep in the stack, and is checked like a
//! nested step ([`Executing`]).
//!
//! BLIND SPOTS, stated: (1) a recursion cycle with no `NestGuard` on it is not guarded; the stack
//! guard tests (core and postgres/frontend `stack_guard_tests`) find only the shapes they run. (2)
//! Where the platform cannot report the stack (no pthread answer and no `stacker` answer), every
//! nested step is refused and the outermost one runs unchecked. (3) A thread whose whole stack is
//! under one outermost statement's need overflows before any check (a 64 KiB thread died opening a
//! database, measured). (4) The margins were sized in debug builds, whose frames are the largest.
//! (5) [`with_stack`] sizes a segment from a bound its caller states; a wrong bound is an overflow
//! the stack guard tests must find.

/// The stack a recursive step may still need below a check: one level of each phase, with margin,
/// and a thread whose whole stack is under 4 times this keeps a quarter of its stack instead (a
/// fixed 512 KiB refused every statement on a 512 KiB thread, Apple's GCD and NSThread default).
/// Debug: 512 KiB (a debug build's view expansion took about 93 KiB a level; a 90-view chain
/// overflowed 8 MiB). Release: 128 KiB, sized as debug's is, at 4 times [`LEVEL_COST`].
#[cfg(debug_assertions)]
pub const STACK_MARGIN: usize = 512 << 10;
#[cfg(not(debug_assertions))]
pub const STACK_MARGIN: usize = 128 << 10;

/// One level of the deepest phase, with slack: the least a checked step is ever entered with,
/// however small the thread, and the floor of a statement prepared inside a step (a DDL's schema
/// reparse). A thread that cannot afford one level refuses every nested step and still runs flat
/// statements. Sized from what was measured (core
/// `stack::tests::two_consecutive_nested_checks_are_less_than_one_level_cost_apart` and
/// `stack_guard_tests::diag_schema_reparse_stack_cost`):
/// - debug: the widest gap between two consecutive checks is 117,200 B, and a schema reparse uses
///   250,127 B (a CREATE TABLE) to 496,367 B (an 8-level view) in all, each level checked: 192 KiB;
/// - release: the widest gap is 6,816 B, and a reparse uses 23,743 B to 36,839 B in all: 32 KiB,
///   so a DDL re-entered with under 128 KiB left (the debug floor, applied to release before) runs
///   instead of being refused.
#[cfg(debug_assertions)]
pub const LEVEL_COST: usize = 192 << 10;
#[cfg(not(debug_assertions))]
pub const LEVEL_COST: usize = 32 << 10;

/// PostgreSQL's message for 54001, without its hint about `max_stack_depth`.
pub const MESSAGE: &str = "stack depth limit exceeded";

/// A recursive step found less than [`margin`] of its thread's stack left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StackDepthExceeded;

std::thread_local! {
    /// The lowest address of this thread's stack and its size, read once (`None` where the
    /// platform does not say).
    static STACK_BOUNDS: Option<(usize, usize)> = stack_bounds();
    /// Nested steps in progress on this thread.
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Statements stepping on this thread ([`Executing`]).
    static EXECUTING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Test hook: this thread's stack taken as unknown, as on a platform with no answer.
    static FORCE_UNKNOWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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

/// The bytes of stack left on this thread below the caller, where the platform says (the pthread
/// answer, then `stacker`'s).
#[inline]
pub fn remaining_stack() -> Option<usize> {
    if FORCE_UNKNOWN.with(|u| u.get()) {
        return None;
    }
    let here = 0u8;
    let sp = std::ptr::addr_of!(here) as usize;
    STACK_BOUNDS
        .with(|b| b.and_then(|(bottom, _)| sp.checked_sub(bottom)))
        .or_else(stacker::remaining_stack)
}

/// This thread's whole stack, where the platform says.
pub fn stack_size() -> Option<usize> {
    STACK_BOUNDS.with(|b| b.map(|(_, size)| size))
}

/// The stack a checked step must find left on this thread: [`STACK_MARGIN`], or a quarter of a
/// smaller thread's whole stack, never less than [`LEVEL_COST`].
pub fn margin() -> usize {
    stack_size().map_or(STACK_MARGIN, |size| STACK_MARGIN.min(size / 4).max(LEVEL_COST))
}

/// One recursive step (an expression or a SELECT parsed, a view expanded, a subquery planned,
/// optimized or emitted) in progress: entered only with [`margin`] of stack left, except a
/// statement's outermost step on a thread where no statement is stepping. Where the stack is
/// unknown, every checked step but that one is refused. Leaving it, on every exit, pops it.
pub struct NestGuard(());

impl NestGuard {
    #[inline]
    #[track_caller]
    pub fn enter() -> Result<Self, StackDepthExceeded> {
        let depth = DEPTH.with(|d| d.get());
        let checked = depth > 0 || EXECUTING.with(|e| e.get()) > 0;
        let refuse = match remaining_stack() {
            Some(left) => checked && left < margin(),
            None => checked,
        };
        if refuse {
            return Err(StackDepthExceeded);
        }
        test_hooks::record(depth, std::panic::Location::caller());
        DEPTH.with(|d| d.set(depth + 1));
        Ok(Self(()))
    }
}

impl Drop for NestGuard {
    #[inline]
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get() - 1));
    }
}

/// A statement stepping on this thread (`Program::step`), so a statement prepared meanwhile is no
/// outermost step. Leaving it, on every exit, pops it.
pub struct Executing(());

impl Executing {
    #[inline]
    pub fn enter() -> Self {
        EXECUTING.with(|e| e.set(e.get() + 1));
        Self(())
    }
}

impl Drop for Executing {
    #[inline]
    fn drop(&mut self) {
        EXECUTING.with(|e| e.set(e.get() - 1));
    }
}

/// Run `f` with at least `needs` bytes of stack: on this thread's stack when it has that much
/// left, otherwise on a new stack segment of `needs` bytes (`stacker::maybe_grow`). For code with
/// no `NestGuard` of ours inside it whose depth is bounded but large (libpg_query's C tree walks
/// and its protobuf decode), where a refusal cannot be placed.
pub fn with_stack<R>(needs: usize, f: impl FnOnce() -> R) -> R {
    stacker::maybe_grow(needs, needs, f)
}

/// Test hooks, compiled in every build (each is a thread-local read when unarmed): the levels a
/// statement's checks were apart, the outermost steps re-entered inside a step, and the stack
/// taken as unknown. Core's and the frontends' tests read them; nothing else does.
#[doc(hidden)]
pub mod test_hooks {
    std::thread_local! {
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        static LEFT_AT: std::cell::RefCell<Vec<(usize, &'static std::panic::Location<'static>)>> = const { std::cell::RefCell::new(Vec::new()) };
        static WORST: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static WORST_AT: std::cell::Cell<Option<(&'static std::panic::Location<'static>, &'static std::panic::Location<'static>)>> = const { std::cell::Cell::new(None) };
        static REENTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static OUTERMOST: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static REENTERED_LEFT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    }

    /// The stack left at the first outermost step entered while a statement stepped, since the
    /// last take (a schema reparse's prepare, for its stack cost).
    pub fn take_reentered_left() -> Option<usize> {
        REENTERED_LEFT.with(|r| r.replace(None))
    }

    /// Outermost steps entered on a quiet thread (exempt from the check), since the last take.
    pub fn take_outermost() -> usize {
        OUTERMOST.with(|o| o.replace(0))
    }

    /// Record on this thread from now on (`take_worst`, `take_reentered`).
    pub fn arm(on: bool) {
        ARMED.with(|a| a.set(on));
    }

    /// This thread's stack taken as unknown (`remaining_stack` answers `None`).
    pub fn force_unknown(on: bool) {
        super::FORCE_UNKNOWN.with(|u| u.set(on));
    }

    /// Outermost steps entered while a statement stepped on this thread, since the last take.
    pub fn take_reentered() -> usize {
        REENTERED.with(|r| r.replace(0))
    }

    /// The most stack used between two consecutive nested checks on this thread, since the last
    /// take: what one level costs, which the margin must cover. A lower bound on a level's cost:
    /// what the innermost level uses below its own check is not measured.
    pub fn take_worst() -> usize {
        take_worst_at().0
    }

    /// `take_worst`, with the two checks (outer, inner) it was measured between.
    pub fn take_worst_at() -> (usize, Option<(&'static std::panic::Location<'static>, &'static std::panic::Location<'static>)>) {
        LEFT_AT.with(|at| at.borrow_mut().clear());
        (WORST.with(|w| w.replace(0)), WORST_AT.with(|w| w.replace(None)))
    }

    #[inline]
    pub(super) fn record(depth: usize, site: &'static std::panic::Location<'static>) {
        if !ARMED.with(|a| a.get()) {
            return;
        }
        let Some(left) = super::remaining_stack() else {
            return;
        };
        LEFT_AT.with(|at| {
            let mut at = at.borrow_mut();
            at.truncate(depth);
            if depth == 0 && super::EXECUTING.with(|e| e.get()) > 0 {
                REENTERED.with(|r| r.set(r.get() + 1));
                REENTERED_LEFT.with(|r| {
                    if r.get().is_none() {
                        r.set(Some(left));
                    }
                });
            } else if depth == 0 {
                OUTERMOST.with(|o| o.set(o.get() + 1));
            }
            if let Some(&(outer, outer_site)) = at.last() {
                let gap = outer.saturating_sub(left);
                if gap > WORST.with(|w| w.get()) {
                    WORST.with(|w| w.set(gap));
                    WORST_AT.with(|w| w.set(Some((outer_site, site))));
                }
            }
            at.push((left, site));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Burns 16 KiB of stack per level until a NestGuard refuses; the levels entered.
    fn nest_until_refused(levels: usize) -> usize {
        let mut frame = [0u8; 16 << 10];
        std::hint::black_box(&mut frame);
        match NestGuard::enter() {
            Ok(_guard) => nest_until_refused(levels + 1),
            Err(_) => levels,
        }
    }

    /// Recursion that never stops on its own ends in the refusal, not an overflow, on threads from
    /// 256 KiB to 8 MiB; the outermost step is always entered.
    #[test]
    fn unbounded_recursion_ends_in_the_refusal_on_every_thread_size() {
        for kib in [256usize, 480, 1024, 8192] {
            let levels = std::thread::Builder::new()
                .stack_size(kib << 10)
                .spawn(|| nest_until_refused(0))
                .unwrap()
                .join()
                .unwrap();
            assert!(levels >= 2, "only {levels} levels on a {kib} KiB thread");
        }
    }

    /// On a thread whose whole stack is under 512 KiB (Apple's GCD and NSThread default; 480 KiB
    /// asked, as Rust rounds 512 up) the outermost step is entered and nesting is still refused
    /// before the stack runs out.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_enters_its_outermost_step_and_still_refuses_deep_nesting() {
        std::thread::Builder::new()
            .stack_size(480 << 10)
            .spawn(|| {
                let size = stack_size();
                // The quarter-stack rule applies (in debug, the thread is under 512 KiB).
                assert!(size.is_some_and(|size| size / 4 < STACK_MARGIN), "premise: a known stack under 4 x STACK_MARGIN, got {size:?}");
                let outer = NestGuard::enter();
                assert!(outer.is_ok(), "the outermost step was refused on a {size:?} B thread");
                let levels = nest_until_refused(1);
                assert!(levels >= 2, "only {levels} levels on a {size:?} B thread");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A nested step that would leave less than one level's stack is refused on a small thread: a
    /// quarter of 256 KiB is less than one level.
    /// Overflowing aborts the test process: that is the red.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_small_thread_refuses_a_nested_step_it_cannot_afford_one_level_for() {
        #[inline(never)]
        fn one_level() {
            // One level just under LEVEL_COST.
            let mut frame = [0u8; LEVEL_COST - (8 << 10)];
            std::hint::black_box(&mut frame);
        }
        #[inline(never)]
        fn descend_to(left: usize) -> bool {
            if remaining_stack().unwrap() > left {
                let mut frame = [0u8; 1 << 10];
                std::hint::black_box(&mut frame);
                return descend_to(left);
            }
            match NestGuard::enter() {
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
                let _outer = NestGuard::enter().unwrap();
                let short = LEVEL_COST - (24 << 10);
                assert!(!descend_to(short), "a nested step was admitted with {short} B left");
                let enough = margin() + (16 << 10);
                assert!(descend_to(enough), "premise: a nested step with {enough} B left (margin and one level's slack) runs");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// A step entered while a statement steps is checked from its outermost step: with ~100 KiB
    /// left (under LEVEL_COST) it is refused, where a flat statement on a quiet thread is not.
    #[cfg(any(target_vendor = "apple", target_os = "linux"))]
    #[test]
    fn a_step_reentered_while_a_statement_steps_is_checked_from_its_first_step() {
        #[inline(never)]
        fn at(left: usize, executing: bool) -> bool {
            if remaining_stack().unwrap() > left {
                let mut frame = [0u8; 1 << 10];
                std::hint::black_box(&mut frame);
                return at(left, executing);
            }
            let _executing = executing.then(Executing::enter);
            NestGuard::enter().is_ok()
        }
        std::thread::Builder::new()
            .stack_size(256 << 10)
            .spawn(|| {
                let short = LEVEL_COST - (16 << 10);
                assert!(at(short, false), "premise: a quiet thread's outermost step runs");
                assert!(!at(short, true), "a re-entered step's first step was exempt");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Where the stack is unknown, the outermost step runs and every nested one is refused.
    #[test]
    fn an_unknown_stack_refuses_every_nested_step() {
        test_hooks::force_unknown(true);
        let outer = NestGuard::enter();
        let inner = NestGuard::enter().is_ok();
        test_hooks::force_unknown(false);
        assert!(outer.is_ok(), "the outermost step was refused on an unknown stack");
        assert!(!inner, "a nested step was admitted on an unknown stack");
    }

    /// On an 8 MiB thread the margin is the full STACK_MARGIN.
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
