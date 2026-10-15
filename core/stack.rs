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

/// Where no stack bound is known (no platform answer): nested views and subqueries are refused past
/// this depth instead, the only bound left.
const FALLBACK_DEPTH: usize = 64;

std::thread_local! {
    /// The lowest address of this thread's stack and its size, read once (`None` where the
    /// platform does not say).
    static STACK_BOUNDS: Option<(usize, usize)> = stack_bounds();
    /// Nested steps in progress on this thread, for the fallback bound.
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
    STACK_BOUNDS.with(|b| b.and_then(|(bottom, _)| sp.checked_sub(bottom)))
}

/// The stack a nested step must find left on this thread: `STACK_MARGIN`, or a quarter of a
/// smaller thread's whole stack.
fn margin() -> usize {
    STACK_BOUNDS.with(|b| b.map_or(STACK_MARGIN, |(_, size)| STACK_MARGIN.min(size / 4)))
}

/// One recursive step of a nested query (a view's expansion, a subquery's planning, optimizing or
/// emission) in progress: entered only with [`margin`] of stack left, so a statement nested past
/// what the thread's stack allows is refused, as PostgreSQL's check_stack_depth refuses it
/// (54001), instead of overflowing (the crash batch, 2026-10-15). The outermost step is never
/// refused: a statement with no nesting runs on whatever stack it was called with, as before the
/// check. Where the stack is unknown, the bound is `FALLBACK_DEPTH` nested steps. Leaving it, on
/// every exit, pops it.
pub(crate) struct NestGuard;

impl NestGuard {
    pub(crate) fn enter(what: &'static str) -> crate::Result<Self> {
        let depth = DEPTH.with(|d| d.get());
        let refuse = match remaining_stack() {
            Some(left) => depth > 0 && left < margin(),
            None => depth >= FALLBACK_DEPTH,
        };
        if refuse {
            crate::bail_parse_error!("{STACK_DEPTH_EXCEEDED}: {what} nested too deep for this thread's stack");
        }
        DEPTH.with(|d| d.set(depth + 1));
        Ok(Self)
    }
}

impl Drop for NestGuard {
    fn drop(&mut self) {
        DEPTH.with(|d| d.set(d.get() - 1));
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
                assert!(outer.is_ok(), "the outermost step was refused on a {size:?} B thread");
                let levels = nest_until_refused(1);
                assert!(levels >= 2, "only {levels} levels on a {size:?} B thread");
            })
            .unwrap()
            .join()
            .unwrap();
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
