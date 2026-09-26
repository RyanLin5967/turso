//! The trunk WAL as branch readers see it: observation-only counters, and the runtime switches for
//! the lane's three fixes (frontier/round11/r11-walpin/PREREG.md in artie-research).
//!
//! Counters: process-global relaxed atomics, each bumped once per call from an index the call
//! computes anyway, so counting adds no per-element step. Nothing in the WAL or the store reads them.
//!
//! Switches (all off by default, which is the base engine):
//! * FW1, reader-horizon checkpoint that never rescans: a checkpoint walks only the frames in
//!   `(nbackfills, max_safe]` of a frame -> page log (SQLite's `walIteratorInit` over the wal-index's
//!   page array), and `find_frame` binary-searches a page's ascending frame list.
//! * FW2, wal2 (SQLite's wal2 branch): two WAL files; see `storage::wal`. Implies FW1.
//! * FW3, branch-aware read transactions: a branch pager takes no trunk WAL snapshot and no read
//!   mark; it reads a trunk page optimistically and validates afterwards (see `Pager`).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

pub(crate) static CKPT_CALLS: AtomicU64 = AtomicU64::new(0);
pub(crate) static CKPT_FRAMES_SCANNED: AtomicU64 = AtomicU64::new(0);
pub(crate) static FIND_CALLS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FIND_SCANNED: AtomicU64 = AtomicU64::new(0);
pub(crate) static RESTARTS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW2_SWITCHES: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW2_CKPT_REFUSED: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_TRUNK_READS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_RETRIES: AtomicU64 = AtomicU64::new(0);
// r11-walpin-conc: FW3's retries per call, by cause (frontier/round11/r11-walpin-conc/PREREG.md).
pub(crate) static FW3_CALLS_TRUNK: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_RETRY_STORE: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_RETRY_GEN: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_RETRY_READERR: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_BUSY: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_MULTI_STORE: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_MAX_RETRIES: AtomicU64 = AtomicU64::new(0);
pub(crate) static FW3_HIST: [AtomicU64; FW3_HIST_BUCKETS] = [const { AtomicU64::new(0) }; FW3_HIST_BUCKETS];

/// r11-walpin-conc: store-mutex acquisitions and contended acquisitions, by call class (0
/// `resolve_into`, 1 `sees_trunk`, 2 other); see `BranchStore::lock_inner`.
pub(crate) static STORE_LOCKS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
pub(crate) static STORE_CONTENDED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

/// Buckets of `WalPinCounters::fw3_hist`: a trunk call's retries 0, 1, 2, 3, 4-7, 8-15, 16-63,
/// 64-255, 256-999, and Busy (the retry limit).
pub const FW3_HIST_BUCKETS: usize = 10;

/// Record one FW3 call that read the trunk at least once: `retries` failed attempts, `store` of them
/// failed the store check alone; `busy` when it gave up.
pub(crate) fn fw3_record_call(retries: u64, store: u64, busy: bool) {
    FW3_CALLS_TRUNK.fetch_add(1, Relaxed);
    let bucket = match retries {
        _ if busy => 9,
        0..=3 => retries as usize,
        4..=7 => 4,
        8..=15 => 5,
        16..=63 => 6,
        64..=255 => 7,
        _ => 8,
    };
    FW3_HIST[bucket].fetch_add(1, Relaxed);
    FW3_MAX_RETRIES.fetch_max(retries, Relaxed);
    if store >= 2 {
        FW3_MULTI_STORE.fetch_add(1, Relaxed);
    }
    if busy {
        FW3_BUSY.fetch_add(1, Relaxed);
    }
}

static FW1: AtomicBool = AtomicBool::new(false);
static FW2: AtomicBool = AtomicBool::new(false);
static FW3: AtomicBool = AtomicBool::new(false);
static ENV_INIT: std::sync::Once = std::sync::Once::new();

/// `TURSO_WALPIN_FIX=fw1,fw2,fw3` sets the switches at first use, so an existing test suite can be
/// run under a fix; `set_fixes` afterwards overrides it.
fn init_from_env() {
    ENV_INIT.call_once(|| {
        if let Ok(v) = std::env::var("TURSO_WALPIN_FIX") {
            let has = |f: &str| v.split(',').any(|x| x.trim() == f);
            FW1.store(has("fw1") || has("fw2"), Relaxed);
            FW2.store(has("fw2"), Relaxed);
            FW3.store(has("fw3"), Relaxed);
        }
    });
}

/// Cumulative counts since the process started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalPinCounters {
    /// Checkpoint candidate collections (`iter_latest_frames`), one per checkpoint that reached it.
    pub ckpt_calls: u64,
    /// Frame-list elements those collections examined (base: every list walked by `rfind`; FW1: every
    /// frame of the `(min, max]` range walked).
    pub ckpt_frames_scanned: u64,
    /// `find_frame` calls on the shared frame index.
    pub find_calls: u64,
    /// Elements those calls examined (base: `rfind` steps; FW1: binary-search probes).
    pub find_scanned: u64,
    /// WAL generation restarts (`restart_wal_header`).
    pub restarts: u64,
    /// FW2: writer switches to the other WAL file.
    pub fw2_switches: u64,
    /// FW2: checkpoints refused because a reader's snapshot ends in the file to be checkpointed.
    pub fw2_ckpt_refused: u64,
    /// FW3: branch reads of a trunk page (WAL frame or database file).
    pub fw3_trunk_reads: u64,
    /// FW3: those reads that failed validation and were retried.
    pub fw3_retries: u64,
    /// FW3: calls (branch page misses) that read the trunk at least once.
    pub fw3_calls_trunk: u64,
    /// FW3 retries whose store check failed while the generation held.
    pub fw3_retry_store: u64,
    /// FW3 retries whose generation check failed (the read itself succeeded).
    pub fw3_retry_gen: u64,
    /// FW3 retries of a frame read that failed in a changed generation.
    pub fw3_retry_readerr: u64,
    /// FW3 calls that reached the retry limit and returned Busy.
    pub fw3_busy: u64,
    /// FW3 calls with two or more store-check retries.
    pub fw3_multi_store: u64,
    /// The most retries any FW3 call has made.
    pub fw3_max_retries: u64,
    /// FW3 calls by their retries (buckets in `FW3_HIST_BUCKETS`'s doc).
    pub fw3_hist: [u64; FW3_HIST_BUCKETS],
    /// Store-mutex acquisitions by call class: `resolve_into`, `sees_trunk`, other.
    pub store_locks: [u64; 3],
    /// Those acquisitions whose `try_lock` failed (the caller waited).
    pub store_contended: [u64; 3],
}

pub fn counters() -> WalPinCounters {
    WalPinCounters {
        ckpt_calls: CKPT_CALLS.load(Relaxed),
        ckpt_frames_scanned: CKPT_FRAMES_SCANNED.load(Relaxed),
        find_calls: FIND_CALLS.load(Relaxed),
        find_scanned: FIND_SCANNED.load(Relaxed),
        restarts: RESTARTS.load(Relaxed),
        fw2_switches: FW2_SWITCHES.load(Relaxed),
        fw2_ckpt_refused: FW2_CKPT_REFUSED.load(Relaxed),
        fw3_trunk_reads: FW3_TRUNK_READS.load(Relaxed),
        fw3_retries: FW3_RETRIES.load(Relaxed),
        fw3_calls_trunk: FW3_CALLS_TRUNK.load(Relaxed),
        fw3_retry_store: FW3_RETRY_STORE.load(Relaxed),
        fw3_retry_gen: FW3_RETRY_GEN.load(Relaxed),
        fw3_retry_readerr: FW3_RETRY_READERR.load(Relaxed),
        fw3_busy: FW3_BUSY.load(Relaxed),
        fw3_multi_store: FW3_MULTI_STORE.load(Relaxed),
        fw3_max_retries: FW3_MAX_RETRIES.load(Relaxed),
        fw3_hist: std::array::from_fn(|i| FW3_HIST[i].load(Relaxed)),
        store_locks: std::array::from_fn(|i| STORE_LOCKS[i].load(Relaxed)),
        store_contended: std::array::from_fn(|i| STORE_CONTENDED[i].load(Relaxed)),
    }
}

/// Select the fixes. Call before opening the database: FW1 and FW2 change what the WAL indexes as
/// frames are appended, so switching them on a live WAL is refused by the harness, not handled here.
pub fn set_fixes(fw1: bool, fw2: bool, fw3: bool) {
    init_from_env();
    FW1.store(fw1 || fw2, Relaxed);
    FW2.store(fw2, Relaxed);
    FW3.store(fw3, Relaxed);
}

#[inline]
pub(crate) fn fw1() -> bool {
    init_from_env();
    FW1.load(Relaxed)
}

#[inline]
pub(crate) fn fw2() -> bool {
    init_from_env();
    FW2.load(Relaxed)
}

#[inline]
pub(crate) fn fw3() -> bool {
    init_from_env();
    FW3.load(Relaxed)
}

/// The shared WAL's state at one instant, read under its lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalPinStats {
    pub max_frame: u64,
    pub nbackfills: u64,
    pub checkpoint_seq: u32,
    /// Read-mark values of slots 0..4 (`u32::MAX` = not used).
    pub mark_values: [u32; 5],
    /// Readers holding each slot.
    pub mark_readers: [u32; 5],
    /// Pages with a frame list in `frame_cache`.
    pub fc_pages: u64,
    /// Frames in `frame_cache` (sum of list lengths).
    pub fc_frames: u64,
    /// Bytes the lists and the map hold by capacity: sum of capacity * 8, plus buckets * 33
    /// (a `(u64, Vec<u64>)` entry is 32 bytes, plus one control byte).
    pub fc_bytes: u64,
    /// FW1's frame -> page log: entries and bytes by capacity.
    pub log_frames: u64,
    pub log_bytes: u64,
}
