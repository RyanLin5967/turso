//! The one stack check of the parser and the engine (fastest-engine, lead order 2026-10-15: client
//! SQL could overflow a thread's stack, which aborts the whole server process).
//!
//! Every recursive entry of `turso_parser` (an expression, a SELECT, a subquery) and of the engine
//! (translation, planning, the optimizer) calls [`check`] first. It refuses once the calling thread
//! has less than [`RED_ZONE`] bytes of stack left, and the caller returns its "stack depth limit
//! exceeded" error, which a PostgreSQL client receives as 54001 stack_depth_limit_exceeded, as
//! PostgreSQL's own `check_stack_depth()` refuses. The thread survives and the statement fails.
//!
//! The red zone must hold the deepest stack used between two checks on any recursion cycle; it is
//! sized from a measurement (core `stack_guard_tests::diag_stack_needed_per_nesting_level`), not a
//! guess, and the class test runs every recursion shape it knows on thread stacks from 128 KiB up,
//! where any outcome but an answer or this refusal (a dead process) is red.
//!
//! BLIND SPOTS, stated: (1) a recursion cycle with no `check` on it is not guarded; the class test
//! finds only the shapes it runs. (2) Where the platform cannot report the stack's bounds
//! (`stacker::remaining_stack` returns `None`: wasm, some targets), `check` cannot refuse and
//! allows; the parser's and the planner's fixed depth bounds (`MAX_EXPR_DEPTH`) remain the only
//! limit there.

/// The stack, in bytes, that must be left when a recursive entry begins.
pub const RED_ZONE: usize = 128 * 1024;

/// A recursive entry found less than [`RED_ZONE`] of its thread's stack left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StackDepthExceeded;

/// The message PostgreSQL gives for 54001, without its hint about `max_stack_depth`.
pub const MESSAGE: &str = "stack depth limit exceeded";

/// Refuse a recursive entry with less than [`RED_ZONE`] of stack left on this thread.
#[inline]
pub fn check() -> Result<(), StackDepthExceeded> {
    match stacker::remaining_stack() {
        Some(left) if left < RED_ZONE => Err(StackDepthExceeded),
        _ => Ok(()),
    }
}

/// Run `f` with at least `needs` bytes of stack: on this thread's stack when it has that much
/// left, otherwise on a new stack segment of `needs` bytes (`stacker::maybe_grow`). For code with no
/// `check` of ours inside it whose depth is bounded but large (libpg_query's protobuf decode,
/// bounded by prost's recursion limit), where a refusal cannot be placed.
pub fn with_stack<R>(needs: usize, f: impl FnOnce() -> R) -> R {
    stacker::maybe_grow(needs, needs, f)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A thread whose stack is smaller than the red zone is refused at its first check; one with
    /// room is not (both directions, so the check is shown to fire and not to fire spuriously).
    #[test]
    fn the_check_refuses_inside_the_red_zone_and_allows_outside_it() {
        let small = std::thread::Builder::new()
            .stack_size(RED_ZONE / 2 + 16 * 1024)
            .spawn(check)
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(small, Err(StackDepthExceeded), "a thread inside the red zone was not refused");
        let big = std::thread::Builder::new().stack_size(8 << 20).spawn(check).unwrap().join().unwrap();
        assert_eq!(big, Ok(()), "a thread with 8 MiB of stack was refused");
    }

    /// Recursion that never stops on its own ends with the refusal, not an overflow.
    #[test]
    fn unbounded_recursion_ends_in_the_refusal() {
        fn dive(n: u64) -> Result<u64, StackDepthExceeded> {
            check()?;
            let pad = std::hint::black_box([n; 64]);
            Ok(dive(n + 1)? + pad[0])
        }
        let got = std::thread::Builder::new().stack_size(512 * 1024).spawn(|| dive(0)).unwrap().join().unwrap();
        assert_eq!(got, Err(StackDepthExceeded));
    }
}
