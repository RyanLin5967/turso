#[cfg(shuttle)]
pub(crate) use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub(crate) use std_adapter::*;

#[expect(unused_imports)]
#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::hint::spin_loop;
    pub use shuttle::thread::{
        current, panicking, park, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use shuttle::thread_local;
}

#[expect(unused_imports)]
#[cfg(not(shuttle))]
mod std_adapter {
    pub use std::hint::spin_loop;
    #[cfg(not(test))]
    pub use std::thread::sleep;
    pub use std::thread::{
        current, panicking, park, scope, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };
    pub use std::thread_local;

    /// Test builds: std's sleep, counted per thread (fastest-budgets, review 2 M3: a fork that
    /// waits out a bounded poll must be visible to an integer counter). BLIND SPOT: a direct
    /// `std::thread::sleep` is not counted.
    #[cfg(test)]
    pub fn sleep(d: std::time::Duration) {
        crate::branch::budget_probe::slept();
        std::thread::sleep(d)
    }
}
