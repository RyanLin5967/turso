//! Instructions retired by this thread, attributed to the phase of a statement it is in (L5's
//! phase table, lead 2026-10-15): read, parse, translate, plan, execute, encode, write, and session
//! for everything else (the server's own bookkeeping). A diagnostic only: with the `phase-count`
//! feature every phase boundary reads the thread's counters (thread_selfcounts, one system call),
//! which perturbs the very system calls the work budget counts, so a phase build is never a
//! verdict. Without the feature every call here is an empty inline function.

/// A phase of a statement on the session thread.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    Session = 0,
    Read = 1,
    Parse = 2,
    Translate = 3,
    Plan = 4,
    Execute = 5,
    Encode = 6,
    Write = 7,
}

/// The phases' names, in [`Phase`]'s order: the columns of [`totals`].
pub const NAMES: [&str; 8] = [
    "session",
    "read",
    "parse",
    "translate",
    "plan",
    "execute",
    "encode",
    "write",
];

/// The current phase until the guard drops, then the one before it: phases nest (an engine call
/// inside a translation is `execute` while it runs).
#[must_use]
pub struct Guard {
    #[cfg(feature = "phase-count")]
    previous: Phase,
}

#[cfg(feature = "phase-count")]
mod imp {
    use super::Phase;
    use std::cell::Cell;

    thread_local! {
        /// The phase now, and this thread's instructions when it began.
        static NOW: Cell<(Phase, u64)> = const { Cell::new((Phase::Session, 0)) };
        static TOTALS: [Cell<u64>; 8] = const { [const { Cell::new(0) }; 8] };
    }

    fn instructions() -> u64 {
        #[cfg(target_vendor = "apple")]
        {
            extern "C" {
                fn thread_selfcounts(kind: i32, buf: *mut u64, size: usize) -> i32;
            }
            let mut c = [0u64; 2];
            let rc = unsafe { thread_selfcounts(1, c.as_mut_ptr(), std::mem::size_of_val(&c)) };
            if rc == 0 {
                c[0]
            } else {
                0
            }
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            0
        }
    }

    /// Charge the instructions since the current phase began to it, and make `next` current;
    /// returns the phase it replaced.
    pub fn switch(next: Phase) -> Phase {
        let now = instructions();
        let (current, since) = NOW.with(Cell::get);
        if since != 0 {
            TOTALS.with(|t| {
                let cell = &t[current as usize];
                cell.set(cell.get() + now.saturating_sub(since));
            });
        }
        NOW.with(|n| n.set((next, now)));
        current
    }

    pub fn totals() -> Option<[u64; 8]> {
        // Charge the current phase up to now, so a reading is complete.
        let current = NOW.with(Cell::get).0;
        switch(current);
        Some(TOTALS.with(|t| std::array::from_fn(|i| t[i].get())))
    }
}

/// Make `phase` current until the guard drops.
#[inline]
pub fn enter(phase: Phase) -> Guard {
    #[cfg(feature = "phase-count")]
    {
        Guard {
            previous: imp::switch(phase),
        }
    }
    #[cfg(not(feature = "phase-count"))]
    {
        let _ = phase;
        Guard {}
    }
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "phase-count")]
        imp::switch(self.previous);
    }
}

/// This thread's instructions per phase so far, in [`NAMES`]' order; None in a build without the
/// `phase-count` feature.
pub fn totals() -> Option<[u64; 8]> {
    #[cfg(feature = "phase-count")]
    {
        imp::totals()
    }
    #[cfg(not(feature = "phase-count"))]
    {
        None
    }
}

#[cfg(all(test, feature = "phase-count", target_vendor = "apple"))]
mod tests {
    use super::*;

    /// Work done inside a guard is charged to its phase, a nested guard's to the inner phase, and
    /// the outer phase resumes when the inner guard drops.
    #[test]
    fn instructions_are_charged_to_the_phase_they_ran_in() {
        let burn = |n: u64| (0..n).fold(0u64, |a, i| std::hint::black_box(a ^ i.wrapping_mul(31)));
        let before = totals().unwrap();
        {
            let _p = enter(Phase::Parse);
            burn(200_000);
            {
                let _e = enter(Phase::Execute);
                burn(400_000);
            }
            burn(200_000);
        }
        let after = totals().unwrap();
        let d = |p: Phase| after[p as usize] - before[p as usize];
        assert!(d(Phase::Parse) > 400_000, "parse {}", d(Phase::Parse));
        assert!(d(Phase::Execute) > 400_000, "execute {}", d(Phase::Execute));
        assert!(d(Phase::Execute) < d(Phase::Parse) * 3, "execute {}", d(Phase::Execute));
        assert_eq!(d(Phase::Translate), 0);
    }
}
