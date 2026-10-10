use std::{
    fmt,
    sync::atomic::{AtomicU32, Ordering},
};

use crate::rsapi::TursoError;

mod busy_timer;
pub mod capi;
pub mod rsapi;

/// This thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`), read as core's busy red reads it: the
/// instrument of this crate's busy-wait reds (engine review 11 MED 4).
#[cfg(all(test, unix))]
pub(crate) fn thread_cpu() -> std::time::Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is plain old data that clock_gettime writes whole.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

#[macro_export]
macro_rules! assert_send {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Send>() {}
            $( check::<$ty>(); )+
        };
    };
}

#[macro_export]
macro_rules! assert_sync {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Sync>() {}
            $( check::<$ty>(); )+
        };
    };
}

#[derive(Clone, Default)]
pub enum IoBackend {
    #[default]
    Default,
    /// In-memory backend
    Memory,
    /// Generic syscall backend
    Syscall,
    /// IO uring (supported only on Linux)
    IoUring,
    /// Experimental iocp (supported only on windows)
    IOCP,
    Other(String),
}

impl fmt::Display for IoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => write!(f, "memory"),
            Self::Syscall => write!(f, "syscall"),
            Self::IoUring => write!(f, "io_uring"),
            Self::IOCP => write!(f, "experimental_win_iocp"),
            Self::Other(other) => write!(f, "{other}"),
            Self::Default => write!(f, "default"),
        }
    }
}

impl<T: AsRef<str>> From<T> for IoBackend {
    fn from(vfs: T) -> Self {
        match vfs.as_ref() {
            "memory" => IoBackend::Memory,
            "syscall" => IoBackend::Syscall,
            "io_uring" => IoBackend::IoUring,
            "experimental_win_iocp" => IoBackend::IOCP,
            vfs => IoBackend::Other(vfs.to_string()),
        }
    }
}

/// simple helper which return MISUSE error in case of concurrent access to some operation
struct ConcurrentGuard {
    in_use: AtomicU32,
}

struct ConcurrentGuardToken<'a> {
    guard: &'a ConcurrentGuard,
}

impl ConcurrentGuard {
    pub fn new() -> Self {
        Self {
            in_use: AtomicU32::new(0),
        }
    }
    pub fn try_use(&self) -> Result<ConcurrentGuardToken<'_>, TursoError> {
        if self
            .in_use
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(TursoError::Misuse("concurrent use forbidden".to_string()));
        };
        Ok(ConcurrentGuardToken { guard: self })
    }
}

impl<'a> Drop for ConcurrentGuardToken<'a> {
    fn drop(&mut self) {
        let before = self.guard.in_use.swap(0, Ordering::SeqCst);
        assert!(
            before == 1,
            "invalid db state: guard wasn't in use while token is active"
        );
    }
}
