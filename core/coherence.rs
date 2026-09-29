//! Coherence instrument (r11-coherence PREREG §0 (a)): thread-local counts of the atomic writes that land on
//! memory other threads also write, one counter per class. Observation only.
//!
//! Without the `coherence` feature every [`bump`] is an empty inline function and [`snapshot`] returns zeros, so
//! a timed build carries no instrument. With it, each class is one `Cell<u64>` in a const-initialised
//! `thread_local` (no allocation, no registration, no shared write): a thread reads its own totals with
//! [`snapshot`], and the harness sums the threads it ran.
//!
//! Units: RMWs and plain stores to a shared line, as issued. A parking_lot read acquisition and its release are 2;
//! a failed CAS attempt counts, and also counts in its class's `*_FAIL` twin, so retries are visible apart.
//! A site that is not instrumented is not counted; that is the instrument's blind spot.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Class {
    /// RwLock<WalFileShared>, read side: acquire + release.
    WalRwRead,
    /// RwLock<WalFileShared>, write side: acquire + release.
    WalRwWrite,
    /// Arc<RwLock<WalFileShared>> strong-count increments and decrements on the connection paths.
    WalArc,
    /// The WAL header SpinLock: every swap attempt, and the release store.
    WalHdr,
    /// WAL header SpinLock swaps that found the lock held.
    WalHdrFail,
    /// TursoRwLock (read marks, vacuum lock, write lock, checkpoint lock): every CAS attempt and every
    /// fetch_sub / fetch_and / release store.
    WalMark,
    /// TursoRwLock CAS attempts that failed.
    WalMarkFail,
    /// The WAL frame_cache SpinLock: swap attempts and release stores.
    WalFc,
    /// Buffer pool alloc/free: bitmap CAS attempts, hint stores, allocated_slots, Arc<Arena> clone/drop.
    BufPool,
    /// Builtin symbols copied into a connection: the builtin_syms RwLock pair, one Arc clone per symbol, and the
    /// symbol Arc drops when the connection's table goes.
    Builtin,
    /// Arc<Schema> clones and drops at the sweep's sites.
    SchemaArc,
    /// Arc clones and drops of handles that live as long as the Database, per connection: the buffer pool, IO,
    /// storage and branch-store handles of a connection's pager and WAL, the connection's and a branch handle's
    /// Arc<Database> (the five lines the census names; amendment 16).
    DbArc,
    /// Database::n_connections and the Database::schema Mutex.
    DbHot,
    /// The branch store's global atomics: next_id, live, trunk_children.
    StoreGlobal,
    /// The pager's init-lock and page-1-slot Arc clones and drops, per connection (amendment 16).
    DbArcInit,
    /// Store shard-lock acquisitions whose previous acquirer was another thread slot (a handoff of the stripe's
    /// lines between cores; amendment 22). Not a shared RMW count: `shared_rmw` excludes it.
    ShardXfer,
    /// Heap allocations (counted by a harness allocator through [`bump`]).
    Malloc,
    /// Heap frees.
    Free,
    // --- k3 amendment 21 (r12-e1-spin): the WAL's user-space spin paths. Observation only; none is a shared-line
    // write, so `shared_rmw` excludes all five (the harness skips `*_fail`, `*_ns` and `readtx_*`).
    /// WAL frame_cache SpinLock swaps that found the lock held (the `*_FAIL` twin of [`Class::WalFc`]).
    WalFcFail,
    /// Nanoseconds spent spinning on the WAL header lock: from the first failed swap to the acquisition. The clock is
    /// read only on that contended path.
    WalHdrSpinNs,
    /// The same for the WAL frame_cache SpinLock.
    WalFcSpinNs,
    /// begin_read_tx retries taken at once (the first five of a call, no yield or sleep).
    ReadTxRetry,
    /// begin_read_tx retries after the fifth (yield, then sleep).
    ReadTxBackoff,
    // --- k3 addendum 21b: what makes the spin-ns lower bounds usable, and begin_read_tx's time.
    /// WAL header acquisitions with at least one failed swap.
    WalHdrContended,
    /// frame_cache acquisitions with at least one failed swap.
    WalFcContended,
    /// Nanoseconds between a begin_read_tx call's first and last immediate Retry (a lower bound on its retry time).
    ReadTxRetryNs,
    /// begin_read_tx calls with at least one immediate Retry.
    ReadTxCalls,
}

pub const CLASSES: usize = 27;

pub const NAMES: [&str; CLASSES] = [
    "wal_rw_read",
    "wal_rw_write",
    "wal_arc",
    "wal_hdr",
    "wal_hdr_fail",
    "wal_mark",
    "wal_mark_fail",
    "wal_fc",
    "bufpool",
    "builtin",
    "schema_arc",
    "db_arc",
    "db_hot",
    "store_global",
    "db_arc_init",
    "shard_xfer",
    "malloc",
    "free",
    "wal_fc_fail",
    "wal_hdr_spin_ns",
    "wal_fc_spin_ns",
    "readtx_retry",
    "readtx_backoff",
    "wal_hdr_contended",
    "wal_fc_contended",
    "readtx_retry_ns",
    "readtx_calls",
];

#[cfg(feature = "coherence")]
mod imp {
    use super::CLASSES;
    use std::cell::Cell;

    thread_local! {
        pub(super) static COUNTS: [Cell<u64>; CLASSES] = const { [const { Cell::new(0) }; CLASSES] };
    }
}

/// Add `n` to this thread's count of `class`.
#[inline(always)]
pub fn bump(class: Class, n: u64) {
    #[cfg(feature = "coherence")]
    imp::COUNTS.with(|c| {
        let cell = &c[class as usize];
        cell.set(cell.get() + n);
    });
    #[cfg(not(feature = "coherence"))]
    let _ = (class, n);
}

/// This thread's totals so far, in [`NAMES`] order. All zeros without the `coherence` feature.
pub fn snapshot() -> [u64; CLASSES] {
    #[cfg(feature = "coherence")]
    {
        imp::COUNTS.with(|c| std::array::from_fn(|i| c[i].get()))
    }
    #[cfg(not(feature = "coherence"))]
    {
        [0; CLASSES]
    }
}

/// Whether this build carries the instrument.
pub const ENABLED: bool = cfg!(feature = "coherence");

/// The spin counters' timer (k3 amendment 21, addendum 21b). The clock is read only after a FAILED attempt, never once
/// the lock is held (or, for begin_read_tx, once a read transaction began), so the time it adds to `ns` when dropped,
/// first failed attempt to last failed attempt, is a LOWER BOUND on the spin: it misses the first failed attempt and the
/// one that succeeds. The first failure of an acquisition also counts it in `contended`, so the bound can be scaled.
/// Without the `coherence` feature it reads no clock and counts nothing.
pub struct SpinTimer {
    ns: Class,
    contended: Class,
    first: Option<std::time::Instant>,
    last: Option<std::time::Instant>,
}

impl SpinTimer {
    #[inline(always)]
    pub fn new(ns: Class, contended: Class) -> Self {
        Self { ns, contended, first: None, last: None }
    }

    /// Call after each failed attempt, before the next one.
    #[inline(always)]
    pub fn failed(&mut self) {
        if !ENABLED {
            return;
        }
        let now = std::time::Instant::now();
        if self.first.is_none() {
            self.first = Some(now);
            bump(self.contended, 1);
        } else {
            self.last = Some(now);
        }
    }
}

impl Drop for SpinTimer {
    /// No clock read here: the interval between the two stored readings.
    #[inline(always)]
    fn drop(&mut self) {
        if let (Some(a), Some(b)) = (self.first, self.last) {
            bump(self.ns, b.duration_since(a).as_nanos() as u64);
        }
    }
}

// --- The published fixes, selected at run time (r11-coherence amendment 2) -----------------------------------------
// Every fix is compiled into every build, instrumented or not, and chosen once per process from TURSO_R11_FIX (or
// `set_fixes` before first use), so one binary measures each arm under the same code generation. A structure that
// depends on a fix reads the choice once, at its construction.

/// FW: BRAVO on RwLock<WalFileShared>, per-thread reader indicators on the WAL read marks, a seqlock for the WAL
/// header, no Arc clone per read transaction.
pub const FIX_WAL: u32 = 1;
/// FB: builtin symbols shared by every connection (copy-on-write maps) instead of copied into each.
pub const FIX_BUILTIN: u32 = 2;
/// FP: per-thread buffer magazines and sloppy reference counts in the buffer pool.
pub const FIX_POOL: u32 = 4;
/// FS: the branch store's global counters on lines of their own.
pub const FIX_STORE: u32 = 8;
/// FA: Arc<Schema> borrowed rather than cloned where a statement or a connect only reads it (amendment 6).
pub const FIX_ARC: u32 = 16;
/// FG: trunk forks enter a reader-biased gate instead of the WAL write lock; trunk writers hold it exclusively
/// (amendment 6). Not part of `all`, which stays amendment 2's four.
pub const FIX_GATE: u32 = 32;
/// FK: the trunk's children index as a concurrent skiplist with an atomic fork-epoch counter (amendment 9).
pub const FIX_TRUNKIDX: u32 = 64;
/// FX: trunk-cache hits copy their page after the shard lock is released (amendment 13).
pub const FIX_COPYOUT: u32 = 128;
/// FM: the branch store striped 1024 ways instead of 64 (amendment 15).
pub const FIX_STRIPES: u32 = 256;
/// FY: the connection's pager held as a plain Arc (it is never swapped), so a connection's drop walks no arc-swap
/// debt lists (amendment 15).
pub const FIX_PAGER: u32 = 512;
/// FU: the Pager and WalFile reach Database-owned objects through handles anchored per (thread, database), so a
/// connection's open and close write no shared reference count of those objects (amendment 15).
pub const FIX_ANCHOR: u32 = 1024;
/// FH: per-thread heaps (the harness's global allocator; the engine ignores it) (amendment 15).
pub const FIX_HEAP: u32 = 2048;
/// R: the harness forks from per-thread replicas of the trunk (the engine ignores it) (amendment 15).
pub const FIX_REPLICA: u32 = 4096;
/// V: the harness gives each thread private databases (the engine ignores it) (amendment 15).
pub const FIX_PRIVATE: u32 = 8192;
/// FO: owner-affine store stripes: a thread's id block maps to one stripe (amendment 22).
pub const FIX_OWNER: u32 = 32768;
/// FZ (U-ARC): the last shared reference counts off the conc path: the connection's and branch handles' database
/// through a per-thread keeper, branch connections outside n_connections, the branch schema from a per-thread copy,
/// and the store's live counter sloppy (amendment 21).
pub const FIX_UARC: u32 = 16384;

static FIXES: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Parse a fix list: `none`, `all`, or letters from `W`, `B`, `P`, `S` separated by commas.
pub fn parse_fixes(s: &str) -> Option<u32> {
    let s = s.trim();
    if s.is_empty() || s == "none" {
        return Some(0);
    }
    if s == "all" {
        return Some(FIX_WAL | FIX_BUILTIN | FIX_POOL | FIX_STORE);
    }
    let mut m = 0;
    for part in s.split(',') {
        m |= match part.trim() {
            "W" => FIX_WAL,
            "B" => FIX_BUILTIN,
            "P" => FIX_POOL,
            "S" => FIX_STORE,
            "A" => FIX_ARC,
            "G" => FIX_GATE,
            "K" => FIX_TRUNKIDX,
            "X" => FIX_COPYOUT,
            "M" => FIX_STRIPES,
            "Y" => FIX_PAGER,
            "U" => FIX_ANCHOR,
            "H" => FIX_HEAP,
            "R" => FIX_REPLICA,
            "V" => FIX_PRIVATE,
            "Z" => FIX_UARC,
            "O" => FIX_OWNER,
            _ => return None,
        };
    }
    Some(m)
}

/// Choose the fixes for this process. Only before anything has read them; returns false if too late.
pub fn set_fixes(mask: u32) -> bool {
    FIXES.set(mask).is_ok() || fixes() == mask
}

/// The fixes this process runs with.
pub fn fixes() -> u32 {
    *FIXES.get_or_init(|| match std::env::var("TURSO_R11_FIX") {
        Ok(s) => parse_fixes(&s).unwrap_or_else(|| panic!("TURSO_R11_FIX={s}: expected none, all or letters from W,B,P,S,A,G,K,X,M,Y,U,H,R,V,Z,O")),
        Err(_) => 0,
    })
}

#[cfg(test)]
thread_local! {
    static TEST_FIXES: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}

/// Tests: run this thread's constructions with `mask`, whatever the process chose.
#[cfg(test)]
pub fn force_fixes_for_test(mask: u32) {
    TEST_FIXES.with(|c| c.set(Some(mask)));
}

/// Whether fix `bit` is on (for structures, read once at construction).
#[inline]
pub fn fix(bit: u32) -> bool {
    #[cfg(test)]
    if let Some(m) = TEST_FIXES.with(|c| c.get()) {
        return m & bit != 0;
    }
    fixes() & bit != 0
}

#[cfg(all(test, feature = "coherence"))]
mod tests {
    use super::*;

    #[test]
    fn counts_are_per_thread_and_exact() {
        let before = snapshot();
        bump(Class::WalMark, 3);
        bump(Class::WalMark, 1);
        bump(Class::Builtin, 2);
        let after = snapshot();
        assert_eq!(after[Class::WalMark as usize] - before[Class::WalMark as usize], 4);
        assert_eq!(after[Class::Builtin as usize] - before[Class::Builtin as usize], 2);
        let other = std::thread::spawn(|| {
            bump(Class::WalMark, 5);
            snapshot()[Class::WalMark as usize]
        })
        .join()
        .unwrap();
        assert_eq!(other, 5, "another thread's count starts at zero");
        assert_eq!(snapshot()[Class::WalMark as usize], after[Class::WalMark as usize]);
    }
}
