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
/// sweeps every depth on 2 and 8 MiB threads and must never overflow.
const STACK_MARGIN: usize = 512 << 10;

/// Where no stack bound is known (no platform answer): nested views and subqueries are refused past
/// this depth instead, the only bound left.
const FALLBACK_DEPTH: usize = 64;

std::thread_local! {
    /// The lowest address of this thread's stack, read once (`None` where the platform does not say).
    static STACK_BOTTOM: Option<usize> = stack_bottom();
    /// Nested steps in progress on this thread, for the fallback bound.
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(target_vendor = "apple")]
fn stack_bottom() -> Option<usize> {
    // SAFETY: pthread_self is always valid for the calling thread; the two getters read its
    // attributes and allocate nothing.
    unsafe {
        let me = libc::pthread_self();
        let top = libc::pthread_get_stackaddr_np(me) as usize;
        let size = libc::pthread_get_stacksize_np(me);
        top.checked_sub(size)
    }
}

#[cfg(target_os = "linux")]
fn stack_bottom() -> Option<usize> {
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
        (got == 0).then_some(addr as usize)
    }
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn stack_bottom() -> Option<usize> {
    None
}

/// The bytes of stack left on this thread below the caller, where the platform says.
pub(crate) fn remaining_stack() -> Option<usize> {
    let here = 0u8;
    let sp = std::ptr::addr_of!(here) as usize;
    STACK_BOTTOM.with(|b| b.and_then(|bottom| sp.checked_sub(bottom)))
}

/// One recursive step of a nested query (a view's expansion, a subquery's planning, optimizing or
/// emission) in progress: entered only with `STACK_MARGIN` of stack left, so a statement nested
/// past what the thread's stack allows is refused, as PostgreSQL's check_stack_depth refuses it
/// (54001), instead of overflowing (the crash batch, 2026-10-15). Where the stack is unknown, the
/// bound is `FALLBACK_DEPTH` nested steps. Leaving it, on every exit, pops it.
pub(crate) struct NestGuard;

impl NestGuard {
    pub(crate) fn enter(what: &'static str) -> crate::Result<Self> {
        let depth = DEPTH.with(|d| d.get());
        let refuse = match remaining_stack() {
            Some(left) => left < STACK_MARGIN,
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
