//! A sequence lock for a small `Copy` value (r11-coherence fix FW, the WAL header).
//!
//! Prior art, no novelty claimed: Linux's seqcount/seqlock (Lameter, 2002+). Writers serialise on a spin lock and
//! make the sequence odd for the length of their write; a reader copies the value between two reads of an even,
//! unchanged sequence and retries otherwise, so a reader writes nothing.
//!
//! [`SeqLock::lock`] keeps the SpinLock API the WAL code already uses (a guard that derefs to the value), so every
//! existing writer and read-modify-write site is unchanged; only readers that switch to [`SeqLock::read`] stop
//! writing the line. With the fix off ([`crate::coherence::FIX_WAL`] unset at construction) `read` takes the lock,
//! exactly as the SpinLock did.

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};

pub struct SeqLock<T: Copy> {
    locked: AtomicBool,
    seq: AtomicU64,
    optimistic: bool,
    value: UnsafeCell<T>,
}

unsafe impl<T: Copy + Send> Send for SeqLock<T> {}
unsafe impl<T: Copy + Send> Sync for SeqLock<T> {}

pub struct SeqLockGuard<'a, T: Copy> {
    lock: &'a SeqLock<T>,
}

impl<T: Copy> SeqLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            seq: AtomicU64::new(0),
            optimistic: crate::coherence::fix(crate::coherence::FIX_WAL),
            value: UnsafeCell::new(value),
        }
    }

    /// Exclusive access, as `SpinLock::lock`. The sequence is odd while the guard lives.
    pub fn lock(&self) -> SeqLockGuard<'_, T> {
        // k3 amendment 21: failed swaps and the time spent spinning, outside the WalHdr count (which stays the two
        // writes of an uncontended acquisition, as before, so `shared_rmw` is unchanged). Addendum 21b: the clock is
        // read only after a failed swap, never while the lock is held.
        use crate::coherence::{Class, SpinTimer};
        let mut timer = SpinTimer::new(Class::WalHdrSpinNs, Class::WalHdrContended);
        while self.locked.swap(true, Ordering::Acquire) {
            crate::coherence::bump(Class::WalHdrFail, 1);
            timer.failed();
            std::hint::spin_loop();
        }
        drop(timer);
        // The swap and the release store, as the SpinLock this replaces.
        crate::coherence::bump(crate::coherence::Class::WalHdr, 2);
        if self.optimistic {
            // And the two sequence stores, only when readers run optimistically.
            crate::coherence::bump(crate::coherence::Class::WalHdr, 2);
            let s = self.seq.load(Ordering::Relaxed);
            self.seq.store(s + 1, Ordering::Relaxed);
            fence(Ordering::Release);
        }
        SeqLockGuard { lock: self }
    }

    /// `lock`, counted as the SpinLock's `lock_class` was (the instrument's call sites are unchanged).
    pub fn lock_class(
        &self,
        _class: crate::coherence::Class,
        _fail: Option<crate::coherence::Class>,
    ) -> SeqLockGuard<'_, T> {
        self.lock()
    }

    /// A copy of the value. With the fix on, optimistic: no write to the lock's line.
    #[inline]
    pub fn read(&self) -> T {
        if !self.optimistic {
            return *self.lock();
        }
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 == 0 {
                // A racing writer is detected by the sequence re-check; the copy is discarded then.
                let v = unsafe { std::ptr::read_volatile(self.value.get()) };
                fence(Ordering::Acquire);
                if self.seq.load(Ordering::Relaxed) == s1 {
                    return v;
                }
            }
            std::hint::spin_loop();
        }
    }
}

impl<T: Copy> Drop for SeqLockGuard<'_, T> {
    fn drop(&mut self) {
        if self.lock.optimistic {
            let s = self.lock.seq.load(Ordering::Relaxed);
            self.lock.seq.store(s + 1, Ordering::Release);
        }
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T: Copy> Deref for SeqLockGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: Copy> DerefMut for SeqLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: Copy + std::fmt::Debug> std::fmt::Debug for SeqLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SeqLock").field(&self.read()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn readers_never_see_a_torn_value() {
        crate::coherence::force_fixes_for_test(crate::coherence::FIX_WAL);
        let l = Arc::new(SeqLock::new((0u64, 0u64, 0u64, 0u64)));
        assert!(l.optimistic, "the test must run the optimistic path");
        let stop = Arc::new(AtomicBool::new(false));
        // Writes start once every reader has read: under load a reader thread could otherwise start after the
        // last write and read nothing (tests_fix8_none).
        let started = Arc::new(AtomicU64::new(0));
        let hs: Vec<_> = (0..4)
            .map(|_| {
                let (l, stop, started) = (l.clone(), stop.clone(), started.clone());
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    let v = l.read();
                    assert!(v.0 == v.1 && v.1 == v.2 && v.2 == v.3, "torn read {v:?}");
                    n += 1;
                    started.fetch_add(1, Ordering::Relaxed);
                    while !stop.load(Ordering::Relaxed) {
                        let v = l.read();
                        assert!(v.0 == v.1 && v.1 == v.2 && v.2 == v.3, "torn read {v:?}");
                        n += 1;
                    }
                    n
                })
            })
            .collect();
        while started.load(Ordering::Relaxed) < 4 {
            std::hint::spin_loop();
        }
        for i in 1..20_000u64 {
            let mut g = l.lock();
            *g = (i, i, i, i);
        }
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            assert!(h.join().unwrap() > 0);
        }
    }
}
