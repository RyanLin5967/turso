//! This process's own work counters, for the wire-versus-embedded performance budgets
//! (`turso_branch_stats()` on the server, the same reads in `examples/embedded_ops`): unix
//! system calls and mach traps (`task_info(TASK_EVENTS_INFO)` of the task itself), instructions
//! retired and cycles (`proc_pid_rusage(RUSAGE_INFO_V4)` of the process itself). Both need no
//! privilege. Process-wide: every thread's work counts. Apple platforms only; elsewhere `None`.

/// The counters at one moment: unix system calls, mach traps, instructions, cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessCounters {
    pub unix_syscalls: u64,
    pub mach_syscalls: u64,
    pub instructions: u64,
    pub cycles: u64,
}

#[cfg(target_vendor = "apple")]
mod apple {
    /// `struct task_events_info` (mach/task_info.h): eight `integer_t`; the kernel fills all of
    /// them, two are read.
    #[repr(C)]
    #[derive(Default)]
    #[allow(dead_code)]
    pub(super) struct TaskEventsInfo {
        pub faults: i32,
        pub pageins: i32,
        pub cow_faults: i32,
        pub messages_sent: i32,
        pub messages_received: i32,
        pub syscalls_mach: i32,
        pub syscalls_unix: i32,
        pub csw: i32,
    }

    /// `struct rusage_info_v4` (sys/resource.h): a 16-byte uuid, then 35 `uint64_t`, of which
    /// `ri_instructions` and `ri_cycles` are the 30th and 31st.
    #[repr(C)]
    pub(super) struct RusageInfoV4 {
        pub uuid: [u8; 16],
        pub fields: [u64; 35],
    }

    pub(super) const TASK_EVENTS_INFO: i32 = 2;
    pub(super) const RUSAGE_INFO_V4: i32 = 4;
    pub(super) const RI_INSTRUCTIONS: usize = 29;
    pub(super) const RI_CYCLES: usize = 30;

    extern "C" {
        pub(super) static mach_task_self_: u32;
        pub(super) fn task_info(task: u32, flavor: i32, info: *mut i32, count: *mut u32) -> i32;
        pub(super) fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut RusageInfoV4) -> i32;
    }
}

/// This process's counters now; `None` where the platform does not count them.
pub fn process_counters() -> Option<ProcessCounters> {
    #[cfg(target_vendor = "apple")]
    unsafe {
        let mut events = apple::TaskEventsInfo::default();
        let mut count = (std::mem::size_of::<apple::TaskEventsInfo>() / 4) as u32;
        let rc = apple::task_info(
            apple::mach_task_self_,
            apple::TASK_EVENTS_INFO,
            &mut events as *mut apple::TaskEventsInfo as *mut i32,
            &mut count,
        );
        if rc != 0 {
            return None;
        }
        let mut usage = apple::RusageInfoV4 {
            uuid: [0; 16],
            fields: [0; 35],
        };
        if apple::proc_pid_rusage(std::process::id() as i32, apple::RUSAGE_INFO_V4, &mut usage) != 0
        {
            return None;
        }
        Some(ProcessCounters {
            unix_syscalls: events.syscalls_unix as u32 as u64,
            mach_syscalls: events.syscalls_mach as u32 as u64,
            instructions: usage.fields[apple::RI_INSTRUCTIONS],
            cycles: usage.fields[apple::RI_CYCLES],
        })
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

/// A global allocator that counts the allocations (alloc, alloc_zeroed and realloc calls) it serves,
/// over the system allocator, for the per-step allocation budget (budget.py work). A binary installs
/// it as its `#[global_allocator]` only in a build with the `alloc-count` feature: one relaxed atomic
/// add per allocation, shared by every thread, which a credited build must not pay.
pub struct CountingAlloc;

static ALLOCATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// The allocations served to this thread (a const-initialized Cell: reading it allocates
    /// nothing, and it has no destructor to run while the thread ends).
    static THREAD_ALLOCATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn count() {
    ALLOCATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = THREAD_ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
}

/// The allocations [`CountingAlloc`] has served to this thread (0 when it is not the allocator):
/// for an in-process test of one statement's allocations, which other threads cannot disturb.
pub fn thread_allocations() -> u64 {
    THREAD_ALLOCATIONS.try_with(|n| n.get()).unwrap_or(0)
}

unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        count();
        unsafe { std::alloc::System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        count();
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { std::alloc::System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
}

/// The allocations [`CountingAlloc`] has served in this process; None when it is not this process's
/// allocator (it has served none: every process allocates before it could ask).
pub fn allocations() -> Option<u64> {
    match ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed) {
        0 => None,
        n => Some(n),
    }
}

/// This thread's own instructions retired and cycles (`thread_selfcounts`, the kernel's per-thread
/// fixed counters; no privilege), so a budget window counts the session's work alone: the engine's
/// background threads (the branch store's confirm writer) land in windows at random and made the
/// process-wide counts swing by tens of thousands of instructions. `None` off Apple platforms.
pub fn thread_counters() -> Option<(u64, u64)> {
    #[cfg(target_vendor = "apple")]
    {
        extern "C" {
            fn thread_selfcounts(kind: i32, buf: *mut u64, size: usize) -> i32;
        }
        let mut c = [0u64; 2];
        let rc = unsafe { thread_selfcounts(1, c.as_mut_ptr(), std::mem::size_of_val(&c)) };
        (rc == 0).then_some((c[0], c[1]))
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

/// The CPU time, in nanoseconds, every OTHER thread of this process has used so far, the dead ones
/// included (user plus system): the CPU of the threads that have ended (`task_info
/// (MACH_TASK_BASIC_INFO)`, the kernel's total for terminated threads, in microseconds) plus that of
/// every live thread `PROC_PIDLISTTHREADIDS` lists but the calling one (`proc_pidinfo
/// (PROC_PIDTHREADID64INFO)`, skipped by its id). That is the process's CPU less the calling
/// thread's, without reading two counters at two moments (the instruction gap that did so
/// disagreed by about +/-10k at random, DECISIONS 308c3232a3). Read at both ends of a budget window,
/// a difference above zero is foreign work inside it, including a thread born and ended between the
/// reads (wire-verdict-adversary HIGH, DECISIONS 2923cc91b5: the branch-name-filter growth thread
/// ran in 4 of 4 real windows while the live-threads-only sum read 0), and a thread that exits
/// inside the window no longer takes its earlier CPU out of the sum. The terminated total is read
/// before and after the listing, and the read is retried while a thread ended in between. BLIND
/// SPOT: a thread's CPU is in the terminated total only once the kernel accounts its end, so a
/// window holding an exit can read low and the next one high; [`thread_lifecycle`] counts the exit
/// itself, which the budget refuses. `None` off Apple platforms, when the kernel refuses a read, when
/// more than 256 threads exist, or when every retry raced an exit.
pub fn foreign_thread_cpu_ns() -> Option<u64> {
    #[cfg(target_vendor = "apple")]
    {
        const PROC_PIDLISTTHREADIDS: libc::c_int = 28;
        const PROC_PIDTHREADID64INFO: libc::c_int = 15;
        extern "C" {
            fn pthread_threadid_np(thread: libc::pthread_t, id: *mut u64) -> libc::c_int;
        }
        fn terminated_ns() -> Option<u64> {
            let mut info: libc::mach_task_basic_info = unsafe { std::mem::zeroed() };
            let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
            let rc = unsafe {
                apple::task_info(
                    apple::mach_task_self_,
                    libc::MACH_TASK_BASIC_INFO as i32,
                    (&mut info as *mut libc::mach_task_basic_info).cast(),
                    &mut count,
                )
            };
            let us = |t: libc::time_value_t| t.seconds as u64 * 1_000_000 + t.microseconds as u64;
            (rc == 0).then(|| (us(info.user_time) + us(info.system_time)) * 1000)
        }
        let pid = std::process::id() as libc::c_int;
        let mut me = 0u64;
        if unsafe { pthread_threadid_np(0 as libc::pthread_t, &mut me) } != 0 {
            return None;
        }
        for _ in 0..16 {
            let dead = terminated_ns()?;
            let mut ids = [0u64; 256];
            let bytes = unsafe {
                libc::proc_pidinfo(
                    pid,
                    PROC_PIDLISTTHREADIDS,
                    0,
                    ids.as_mut_ptr().cast(),
                    std::mem::size_of_val(&ids) as libc::c_int,
                )
            };
            if bytes <= 0 || bytes as usize >= std::mem::size_of_val(&ids) {
                return None;
            }
            let n = bytes as usize / std::mem::size_of::<u64>();
            let mut live = 0u64;
            for &id in &ids[..n] {
                if id == me {
                    continue;
                }
                let mut info: libc::proc_threadinfo = unsafe { std::mem::zeroed() };
                let got = unsafe {
                    libc::proc_pidinfo(
                        pid,
                        PROC_PIDTHREADID64INFO,
                        id,
                        (&mut info as *mut libc::proc_threadinfo).cast(),
                        std::mem::size_of::<libc::proc_threadinfo>() as libc::c_int,
                    )
                };
                // A thread that ended after the listing reads nothing; its CPU moved into the
                // terminated total, which the second read below sees moved, and the read retries.
                if got as usize == std::mem::size_of::<libc::proc_threadinfo>() {
                    live += info.pth_user_time + info.pth_system_time;
                }
            }
            if terminated_ns()? == dead {
                return Some(dead + live);
            }
        }
        None
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        None
    }
}

static THREAD_STARTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static THREAD_EXITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LIFECYCLE_INSTALLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(target_vendor = "apple")]
mod lifecycle {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(super) type Hook = unsafe extern "C" fn(
        event: libc::c_uint,
        thread: libc::pthread_t,
        addr: *mut libc::c_void,
        size: libc::size_t,
    );
    extern "C" {
        pub(super) fn pthread_introspection_hook_install(hook: Option<Hook>) -> Option<Hook>;
    }
    // pthread/introspection.h.
    const PTHREAD_INTROSPECTION_THREAD_START: libc::c_uint = 2;
    const PTHREAD_INTROSPECTION_THREAD_TERMINATE: libc::c_uint = 3;
    /// The hook installed before this one (0: none), called after the count.
    pub(super) static PREVIOUS: AtomicUsize = AtomicUsize::new(0);

    pub(super) unsafe extern "C" fn count(
        event: libc::c_uint,
        thread: libc::pthread_t,
        addr: *mut libc::c_void,
        size: libc::size_t,
    ) {
        match event {
            PTHREAD_INTROSPECTION_THREAD_START => {
                super::THREAD_STARTS.fetch_add(1, Ordering::Relaxed);
            }
            PTHREAD_INTROSPECTION_THREAD_TERMINATE => {
                super::THREAD_EXITS.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let previous = PREVIOUS.load(Ordering::Acquire);
        if previous != 0 {
            let previous: Hook = unsafe { std::mem::transmute(previous) };
            unsafe { previous(event, thread, addr, size) };
        }
    }
}

/// Installs the thread start and exit counter ([`thread_lifecycle`]): libpthread's introspection
/// hook (`pthread_introspection_hook_install`, chained to any hook installed before), which runs on
/// each thread as it starts and as it terminates. A measurement binary calls it first thing in
/// `main`, before any thread exists (tursopg and embedded_ops, in builds with `alloc-count`); a
/// thread started before the call is not counted when it starts, only when it exits. Idempotent;
/// does nothing off Apple platforms.
pub fn install_thread_lifecycle_counter() {
    #[cfg(target_vendor = "apple")]
    {
        use std::sync::atomic::Ordering;
        if LIFECYCLE_INSTALLED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // A thread may start between the install and the store of the previous hook: it then
        // skips the previous hook once, never the count.
        let previous =
            unsafe { lifecycle::pthread_introspection_hook_install(Some(lifecycle::count)) };
        lifecycle::PREVIOUS.store(previous.map_or(0, |h| h as usize), Ordering::Release);
    }
}

/// The threads this process has started and that have exited since
/// [`install_thread_lifecycle_counter`] (`None` before it, or off Apple platforms). Read at both
/// ends of a budget window, any difference is a thread born or ended inside it: the budget refuses
/// every such window (DECISIONS 2923cc91b5 (a)), whatever CPU the thread used.
pub fn thread_lifecycle() -> Option<(u64, u64)> {
    use std::sync::atomic::Ordering;
    LIFECYCLE_INSTALLED.load(Ordering::Acquire).then(|| {
        (
            THREAD_STARTS.load(Ordering::Relaxed),
            THREAD_EXITS.load(Ordering::Relaxed),
        )
    })
}
