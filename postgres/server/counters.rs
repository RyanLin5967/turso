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
