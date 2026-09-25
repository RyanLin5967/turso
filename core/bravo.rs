//! BRAVO reader-writer lock and distributed reader indicators (r11-coherence fix FW).
//!
//! Prior art, no novelty claimed:
//! - [`BravoRwLock`]: BRAVO, "Biased Locking for Reader-Writer Locks" (Dice & Kogan, USENIX ATC 2019). A reader
//!   publishes itself in a visible-readers slot instead of writing the lock word; while the lock is read-biased that
//!   slot write is the whole acquisition. A writer takes the underlying lock, revokes the bias and waits until no
//!   visible reader names the lock. The bias comes back after an inhibit window of 9x the revocation's cost.
//! - [`ReaderIndicators`]: per-thread reader counters summed by the writer, as Linux's percpu_rw_semaphore and the
//!   2.4-era big-reader lock (brlock). A release may run on another thread than its acquire: counters are signed
//!   and only their sum means anything.
//!
//! Visible-reader slots and indicators are laid out one 128-byte line per thread, so a reader's fast path writes
//! only a line no other thread writes.
//!
//! Whether a lock uses the fast path at all is chosen once, from [`crate::coherence::fix`] at construction, so a
//! build carries both the published fix and the code it replaces, and one binary measures both.

// std atomics on purpose: the tables are statics, and the lock is not modelled under shuttle.
use std::sync::atomic::{fence, AtomicBool, AtomicI64, AtomicPtr, AtomicU64, Ordering};
use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::time::Instant;

/// Thread slots: threads past this share lines (still correct, only slower).
pub const THREADS: usize = 64;
/// Visible-reader slots per thread: one 128-byte line of pointers.
const SLOTS_PER_THREAD: usize = 16;

#[repr(align(128))]
struct Line<T>(T);

static NEXT_THREAD: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_INDEX: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// This thread's slot index, assigned on first use.
#[inline]
pub fn thread_index() -> usize {
    THREAD_INDEX.with(|c| {
        let v = c.get();
        if v != usize::MAX {
            return v;
        }
        let v = (NEXT_THREAD.fetch_add(1, Ordering::Relaxed) as usize) % THREADS;
        c.set(v);
        v
    })
}

/// The visible-readers table: THREADS lines of SLOTS_PER_THREAD pointers, each naming a lock read-held on the fast
/// path, or null.
static VISIBLE: [Line<[AtomicPtr<()>; SLOTS_PER_THREAD]>; THREADS] =
    [const { Line([const { AtomicPtr::new(std::ptr::null_mut()) }; SLOTS_PER_THREAD]) }; THREADS];

/// Inhibit window after a revocation, as a multiple of the revocation's own duration (BRAVO's N).
const INHIBIT_MULTIPLIER: u32 = 9;

pub struct BravoRwLock<T: ?Sized> {
    /// Readers may take the fast path while this holds.
    rbias: AtomicBool,
    /// Whether this lock uses BRAVO at all (fixed at construction).
    enabled: bool,
    /// Monotonic nanoseconds (since [`epoch`]) before which a slow reader must not re-enable the bias.
    inhibit_until: AtomicU64,
    inner: parking_lot::RwLock<T>,
}

unsafe impl<T: ?Sized + Send> Send for BravoRwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for BravoRwLock<T> {}

fn epoch() -> Instant {
    static E: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *E.get_or_init(Instant::now)
}

fn now_ns() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

pub enum BravoReadGuard<'a, T: ?Sized> {
    Fast {
        lock: &'a BravoRwLock<T>,
        slot: &'static AtomicPtr<()>,
    },
    Slow(parking_lot::RwLockReadGuard<'a, T>),
}

pub struct BravoWriteGuard<'a, T: ?Sized>(parking_lot::RwLockWriteGuard<'a, T>);

impl<T> BravoRwLock<T> {
    pub fn new(value: T) -> Self {
        let enabled = crate::coherence::fix(crate::coherence::FIX_WAL);
        Self {
            rbias: AtomicBool::new(enabled),
            enabled,
            inhibit_until: AtomicU64::new(0),
            inner: parking_lot::RwLock::new(value),
        }
    }
}

impl<T: ?Sized> BravoRwLock<T> {
    fn id(&self) -> *mut () {
        self as *const Self as *const () as *mut ()
    }

    #[inline]
    pub fn read(&self) -> BravoReadGuard<'_, T> {
        if self.enabled && self.rbias.load(Ordering::Acquire) {
            let me = &VISIBLE[thread_index()].0;
            let h = (self.id() as usize >> 4) % SLOTS_PER_THREAD;
            let slot = &me[h];
            if slot
                .compare_exchange(std::ptr::null_mut(), self.id(), Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                // Dekker with the writer: our slot store, then its bias; its bias store, then our slot.
                fence(Ordering::SeqCst);
                if self.rbias.load(Ordering::Acquire) {
                    return BravoReadGuard::Fast { lock: self, slot };
                }
                slot.store(std::ptr::null_mut(), Ordering::Release);
            }
        }
        // Coherence instrument: a slow read is an acquire and a release on the lock word.
        crate::coherence::bump(crate::coherence::Class::WalRwRead, 2);
        let g = self.inner.read();
        if self.enabled
            && !self.rbias.load(Ordering::Relaxed)
            && now_ns() >= self.inhibit_until.load(Ordering::Relaxed)
        {
            self.rbias.store(true, Ordering::Release);
        }
        BravoReadGuard::Slow(g)
    }

    /// `read` without blocking: None while a writer holds the lock.
    #[inline]
    pub fn try_read(&self) -> Option<BravoReadGuard<'_, T>> {
        if self.enabled && self.rbias.load(Ordering::Acquire) {
            let me = &VISIBLE[thread_index()].0;
            let h = (self.id() as usize >> 4) % SLOTS_PER_THREAD;
            let slot = &me[h];
            if slot
                .compare_exchange(std::ptr::null_mut(), self.id(), Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                fence(Ordering::SeqCst);
                if self.rbias.load(Ordering::Acquire) {
                    return Some(BravoReadGuard::Fast { lock: self, slot });
                }
                slot.store(std::ptr::null_mut(), Ordering::Release);
            }
        }
        crate::coherence::bump(crate::coherence::Class::WalRwRead, 2);
        self.inner.try_read().map(BravoReadGuard::Slow)
    }

    #[inline]
    pub fn write(&self) -> BravoWriteGuard<'_, T> {
        crate::coherence::bump(crate::coherence::Class::WalRwWrite, 2);
        let g = self.inner.write();
        if self.enabled && self.rbias.load(Ordering::Acquire) {
            let start = Instant::now();
            self.rbias.store(false, Ordering::Release);
            fence(Ordering::SeqCst);
            let id = self.id();
            for line in VISIBLE.iter() {
                for slot in line.0.iter() {
                    while slot.load(Ordering::Acquire) == id {
                        std::hint::spin_loop();
                    }
                }
            }
            let cost = start.elapsed().as_nanos() as u64;
            self.inhibit_until
                .store(now_ns() + cost * INHIBIT_MULTIPLIER as u64, Ordering::Relaxed);
        }
        BravoWriteGuard(g)
    }

    /// Shared access without the fast path, for code that needs parking_lot's guard.
    pub fn is_biased(&self) -> bool {
        self.rbias.load(Ordering::Relaxed)
    }
}

impl<T: ?Sized> Deref for BravoReadGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        match self {
            // A fast reader holds the lock: the writer waits for its slot before touching the data.
            BravoReadGuard::Fast { lock, .. } => unsafe { &*lock.inner.data_ptr() },
            BravoReadGuard::Slow(g) => g,
        }
    }
}

impl<T: ?Sized> Drop for BravoReadGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        if let BravoReadGuard::Fast { slot, .. } = self {
            slot.store(std::ptr::null_mut(), Ordering::Release);
        }
    }
}

impl<T: ?Sized> Deref for BravoWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: ?Sized> DerefMut for BravoWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: ?Sized + std::fmt::Debug> std::fmt::Debug for BravoRwLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BravoRwLock")
            .field("biased", &self.rbias.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Per-thread reader counters, one line per thread, summed by a writer. Signed: a hold taken on one thread may be
/// released on another, which leaves one counter at +1 and another at -1, and the sum right.
pub struct ReaderIndicators(Box<[Line<AtomicI64>]>);

impl ReaderIndicators {
    pub fn new() -> Self {
        Self((0..THREADS).map(|_| Line(AtomicI64::new(0))).collect())
    }

    #[inline]
    pub fn arrive(&self) {
        self.0[thread_index()].0.fetch_add(1, Ordering::SeqCst);
    }

    #[inline]
    pub fn depart(&self) {
        self.0[thread_index()].0.fetch_sub(1, Ordering::SeqCst);
    }

    /// Readers holding now. Exact only when no reader can arrive, which the caller's writer flag ensures.
    pub fn sum(&self) -> i64 {
        self.0.iter().map(|l| l.0.load(Ordering::SeqCst)).sum()
    }
}

impl Default for ReaderIndicators {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ReaderIndicators {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReaderIndicators(sum={})", self.sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn writers_exclude_fast_readers() {
        // With the fix forced on, many readers and a writer that checks an invariant only a writer can break.
        crate::coherence::force_fixes_for_test(crate::coherence::FIX_WAL);
        let lock = Arc::new(BravoRwLock::new((0u64, 0u64)));
        assert!(lock.enabled, "the test must run the fast path");
        let stop = Arc::new(AtomicBool::new(false));
        let mut hs = Vec::new();
        for _ in 0..8 {
            let (lock, stop) = (lock.clone(), stop.clone());
            hs.push(std::thread::spawn(move || {
                let mut fast = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let g = lock.read();
                    if matches!(g, BravoReadGuard::Fast { .. }) {
                        fast += 1;
                    }
                    assert_eq!(g.0, g.1, "a reader saw a half-written pair");
                }
                fast
            }));
        }
        for i in 0..2000u64 {
            let mut g = lock.write();
            let pair: &mut (u64, u64) = &mut g;
            pair.0 = i;
            std::hint::spin_loop();
            pair.1 = i;
        }
        stop.store(true, Ordering::Relaxed);
        let fast: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(fast > 0, "no reader ever took the fast path: the fix never ran");
    }

    #[test]
    fn indicators_sum_across_threads() {
        let ind = Arc::new(ReaderIndicators::new());
        ind.arrive();
        let i2 = ind.clone();
        std::thread::spawn(move || i2.depart()).join().unwrap();
        assert_eq!(ind.sum(), 0, "a release on another thread must cancel the acquire");
        ind.arrive();
        assert_eq!(ind.sum(), 1);
        ind.depart();
    }
}
