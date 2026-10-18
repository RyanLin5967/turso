//! AD-T5, the async create's creating-thread counts (PREREG-v1 amendment 2; ASYNC-DESIGN T5 as
//! R1.4 restates it).
//!
//! Build and run it twice from one sha, both binaries' sha256 recorded before the first run:
//! - the reading binary: `cargo test --release -p turso_core --test async_s0 -- --test-threads=1`;
//! - the fire-check binary: the same with `--features turso_core/fe-mutants`, run once unmutated and
//!   once per `FE_MUTANT` named below.
//!
//! What this binary counts, and what it does not:
//! - It counts the creating thread's heap allocations, through a counting `#[global_allocator]`
//!   over `mimalloc::MiMalloc` that counts per thread.
//! - It counts the store's `chunk_inline_allocs`, `pool_refills` and `pending_reallocs`, and the
//!   group's flights (the take count, whose parity Green on T1 reads), from
//!   `Database::branch_async_stats`.
//! - It does NOT count the creating thread's flushes and file writes. Those belong to the external
//!   tracer: c1btrace on T1, perf or strace by tid on Linux (correctness evidence only there).
//!   The tracer reads them between the markers this binary prints on the creating thread's stderr,
//!   which is outside the trace root ⟨root⟩, so not a file write under it.
//! - Beside the readings it prints the process's sync counts over each window: every thread's,
//!   the flusher's included. They are not the creating thread's ⟨s⟩.
//!
//! Green on T1 (asserted here, unmutated runs only):
//! - the zero-create control reads 0 allocations on the creating thread;
//! - `chunk_inline_allocs` = 0 and `pending_reallocs` = 0 in the live and strict arms, and
//!   `pool_refills` = 0 in the strict arm;
//! - the live arm's runs include an even and an odd take count, with up to 20 runs made for that.
//!
//! A VOID arm fails here: a void arm has not passed. The creating thread's counts are READINGS
//! (K-A3 judges them), never assertions.
//!
//! At the stub commit, the async machinery does not exist (no flusher, no housekeeping, the table's
//! D2 pool only; `core/branch/async_api.rs`), so every arm is void for its stated reason.
//!
//! Fire-check mutants (fire-check binary only): `create_path_pwrite`, `create_path_fullfsync`,
//! `record_alloc_on_create`, `chunk_inline`, `async_waits_durable`. Their comparisons, a mutated
//! run's counts against the same arm unmutated, are made across processes by the driver that runs
//! both binaries; FE_MUTANT is read once per process.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use turso_core::branch::async_api::{BranchStatus, fe_mutants_compiled};
use turso_core::branch::{BranchDurability, BranchId, SyncClass};
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

// ---- the counting allocator: per-thread allocation counts over MiMalloc ----

struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn thread_allocs() -> u64 {
    ALLOCS.with(|n| n.get())
}

// ---- registered constants (ASYNC-DESIGN §3, R1.2; PREREG-v1 amendment 2) ----

/// The live arm's creates, and the strict arm's.
const N_LIVE: u64 = 10_000;
const N_STRICT: u64 = 1_000;
/// The ε arm's creates, with the pool at its registered depth only.
const N_EPS: u64 = 100_000;
/// Warm-up creates before every window (FE_MUTANT's one-time read falls here).
const WARMUP: u64 = 100;
/// MAX_LAG (W_ack): the strict arm must end within half of it.
const MAX_LAG: Duration = Duration::from_millis(50);
/// POLL: how often the uncounted thread polls the N-th create's status.
const POLL: Duration = Duration::from_millis(5);
/// Runs per arm; the live arm makes more, to 20, only to see both take-count parities.
const RUNS: usize = 5;
const MAX_LIVE_RUNS: usize = 20;
/// A reading's give-up bound on the poll for durability: past it the arm is void.
const DURABLE_DEADLINE: Duration = Duration::from_secs(10);

// ---- one arm's run ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    Live,
    Strict(u64),
    Eps,
}

impl Arm {
    fn name(self) -> String {
        match self {
            Arm::Live => "live".to_string(),
            Arm::Strict(n) => format!("strict_n{n}"),
            Arm::Eps => "eps".to_string(),
        }
    }
    fn n(self) -> u64 {
        match self {
            Arm::Live => N_LIVE,
            Arm::Strict(n) => n,
            Arm::Eps => N_EPS,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Reading {
    creates: u64,
    allocs: u64,
    chunk_inline_allocs: u64,
    pool_refills: u64,
    pending_reallocs: u64,
    group_flights: u64,
    flusher_flights: u64,
    lag_fallbacks: u64,
    process_fsync: u64,
    process_full_fsync: u64,
    process_barrier: u64,
    window: Duration,
    /// The same-duration zero-create control made right after this run (engine-rev-l2b MED: one
    /// per run, in the strict arm too): the creating thread's allocations over it.
    control_allocs: u64,
}

/// Why a run cannot be read.
type Void = String;

fn store_class() -> SyncClass {
    // ASYNC-DESIGN §2: async is refused but at FullFsync on Apple, Fsync or FullFsync on Linux.
    if cfg!(target_vendor = "apple") {
        SyncClass::FullFsync
    } else {
        SyncClass::Fsync
    }
}

fn open_store(root: &Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("platform IO"));
    let path = root.join("s0.db");
    Database::open_file_with_flags(
        io,
        path.to_str().expect("a UTF-8 root"),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync: store_class() }),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("open the store under the trace root")
}

fn creating_tid() -> u64 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid takes no arguments.
        (unsafe { libc::syscall(libc::SYS_gettid) }) as u64
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut tid: u64 = 0;
        #[cfg(target_vendor = "apple")]
        // SAFETY: a valid out-pointer for the calling thread's id.
        unsafe {
            libc::pthread_threadid_np(0, &mut tid);
        }
        tid
    }
}

fn feature_line() -> String {
    format!(
        "features=[{}] FE_MUTANT={}",
        if fe_mutants_compiled() { "fe-mutants" } else { "" },
        std::env::var("FE_MUTANT").unwrap_or_default()
    )
}

/// One run of `arm`, in a fresh trace root: warm-up creates, the window of N creates, then the
/// creating thread parks on an untimed condvar while an uncounted thread polls the N-th id's
/// status every POLL until it reads durable, and wakes it. The window closes at that wake.
fn run_arm(arm: Arm, run: usize) -> Result<Reading, Void> {
    let root = tempfile::TempDir::new().map_err(|e| format!("no trace root: {e}"))?;
    let db = open_store(root.path());
    let conn = db.connect().map_err(|e| format!("connect: {e}"))?;
    // A branch starts from a committed trunk page: one table first.
    conn.execute("CREATE TABLE t(x)").map_err(|e| format!("seed the trunk: {e}"))?;
    for i in 0..WARMUP {
        conn.create_branch_async(&format!("warm-{run}-{i}")).map_err(|e| format!("warm-up create: {e}"))?;
    }
    // Before any window: a build whose async machinery does not exist is void at once, rather
    // than after N durable creates (10^5 of them in the ε arm).
    if !db.branch_async_stats().measured {
        println!("AD-T5 READING arm={} run={run} void=stub {}", arm.name(), feature_line());
        return Err("the async machinery is absent (stub build: branch_async_stats().measured is false)".to_string());
    }
    let n = arm.n();
    match arm {
        Arm::Live | Arm::Strict(_) => {
            let covered = db.branch_pool_provision(n);
            if covered < n {
                return Err(format!("the pool covers {covered} ids, fewer than the window's {n} (R1.2's pre-provisioning)"));
            }
        }
        Arm::Eps => {}
    }
    if let Arm::Strict(_) = arm {
        let flusher = db.branch_flusher_hold(true);
        let housekeeping = db.branch_housekeeping_hold(true);
        if !flusher || !housekeeping {
            return Err(format!("the hold hooks held nothing (flusher {flusher}, housekeeping {housekeeping})"));
        }
    }
    // Everything the window needs is made before it opens (engine-rev-l2b HIGH 5): the names, and
    // the poller that wakes the parked creating thread, waiting on a preallocated channel for the
    // N-th id. The creating thread then allocates nothing of its own inside the window.
    let names: Vec<String> = (0..n).map(|i| format!("c-{run}-{i}")).collect();
    let parked = Arc::new((Mutex::new(false), Condvar::new()));
    let (last_tx, last_rx) = std::sync::mpsc::sync_channel::<BranchId>(1);
    let poller = {
        let parked = parked.clone();
        let db = db.clone();
        std::thread::spawn(move || {
            let Ok(last) = last_rx.recv() else { return false };
            let until = Instant::now() + DURABLE_DEADLINE;
            let mut durable = false;
            while Instant::now() < until {
                if matches!(db.branch_status(last), Ok(BranchStatus::Durable)) {
                    durable = true;
                    break;
                }
                std::thread::sleep(POLL);
            }
            let (m, cv) = &*parked;
            *m.lock().unwrap() = true;
            cv.notify_one();
            durable
        })
    };
    let tid = creating_tid();
    let s0 = db.branch_async_stats();
    let k0 = turso_core::branch::sync_counts();
    eprintln!("AD-T5 WINDOW OPEN arm={} run={run} tid={tid} root={}", arm.name(), root.path().display());
    let a0 = thread_allocs();
    let t0 = Instant::now();
    let mut last = BranchId(0);
    let mut created = 0;
    for name in &names {
        match conn.create_branch_async(name) {
            Ok(acked) => {
                last = acked.id;
                created += 1;
            }
            Err(_) => break,
        }
    }
    let created_in = t0.elapsed();
    // The live and epsilon arms park until the N-th create reads durable. The strict arm does not
    // (engine-rev-l2b HIGH 4): with the flusher held nothing becomes durable, and amendment 2's
    // strict arm closes its window at its N-th create.
    let durable = match arm {
        Arm::Strict(_) => {
            drop(last_tx);
            let _ = poller.join();
            true
        }
        Arm::Live | Arm::Eps => {
            let _ = last_tx.send(last);
            {
                let (m, cv) = &*parked;
                let mut woken = m.lock().unwrap();
                while !*woken {
                    woken = cv.wait(woken).unwrap();
                }
            }
            poller.join().map_err(|_| "the poller panicked".to_string())?
        }
    };
    let window = t0.elapsed();
    let allocs = thread_allocs() - a0;
    eprintln!("AD-T5 WINDOW CLOSE arm={} run={run} tid={tid}", arm.name());
    let s1 = db.branch_async_stats();
    let k1 = turso_core::branch::sync_counts();
    if let Arm::Strict(_) = arm {
        db.branch_flusher_hold(false);
        db.branch_housekeeping_hold(false);
    }
    let r = Reading {
        creates: created,
        allocs,
        chunk_inline_allocs: s1.chunk_inline_allocs - s0.chunk_inline_allocs,
        pool_refills: s1.pool_refills - s0.pool_refills,
        pending_reallocs: s1.pending_reallocs - s0.pending_reallocs,
        group_flights: s1.group_flights - s0.group_flights,
        flusher_flights: s1.flusher_flights - s0.flusher_flights,
        lag_fallbacks: s1.async_fallbacks.lag - s0.async_fallbacks.lag,
        process_fsync: k1.fsync - k0.fsync,
        process_full_fsync: k1.full_fsync - k0.full_fsync,
        process_barrier: k1.barrier - k0.barrier,
        window,
        control_allocs: 0,
    };
    println!(
        "AD-T5 READING arm={} run={run} n={n} creates={} root={} tid={tid} allocs={} chunk_inline_allocs={} pool_refills={} pending_reallocs={} group_flights={} flusher_flights={} lag_fallbacks={} process_fsync={} process_full_fsync={} process_barrier={} window_us={} creates_us={} measured={} {}",
        arm.name(),
        r.creates,
        root.path().display(),
        r.allocs,
        r.chunk_inline_allocs,
        r.pool_refills,
        r.pending_reallocs,
        r.group_flights,
        r.flusher_flights,
        r.lag_fallbacks,
        r.process_fsync,
        r.process_full_fsync,
        r.process_barrier,
        window.as_micros(),
        created_in.as_micros(),
        s1.measured,
        feature_line()
    );
    if created < n {
        return Err(format!("{created} of {n} creates completed"));
    }
    if !durable {
        return Err(format!("the {n}-th create did not read durable within {DURABLE_DEADLINE:?}"));
    }
    match arm {
        Arm::Live | Arm::Eps if r.flusher_flights == 0 => {
            return Err("the flusher led no flight over the window".to_string());
        }
        Arm::Strict(_) if r.lag_fallbacks != 0 => {
            return Err(format!("async_fallbacks.lag read {} (premise: 0)", r.lag_fallbacks));
        }
        Arm::Strict(_) if created_in > MAX_LAG / 2 => {
            return Err(format!("the creates took {created_in:?}, past MAX_LAG/2"));
        }
        _ => {}
    }
    Ok(r)
}

/// The same-duration zero-create control: the creating thread opens a window, makes no store
/// call, and parks on the same untimed condvar for `window`, woken by another thread.
fn zero_create_control(window: Duration) -> u64 {
    let parked = Arc::new((Mutex::new(false), Condvar::new()));
    let waker = {
        let parked = parked.clone();
        std::thread::spawn(move || {
            std::thread::sleep(window);
            let (m, cv) = &*parked;
            *m.lock().unwrap() = true;
            cv.notify_one();
        })
    };
    let tid = creating_tid();
    eprintln!("AD-T5 WINDOW OPEN arm=control tid={tid}");
    let a0 = thread_allocs();
    {
        let (m, cv) = &*parked;
        let mut woken = m.lock().unwrap();
        while !*woken {
            woken = cv.wait(woken).unwrap();
        }
    }
    let allocs = thread_allocs() - a0;
    eprintln!("AD-T5 WINDOW CLOSE arm=control tid={tid}");
    let _ = waker.join();
    println!("AD-T5 READING arm=control tid={tid} allocs={allocs} window_us={} {}", window.as_micros(), feature_line());
    allocs
}

fn mutated() -> bool {
    std::env::var_os("FE_MUTANT").is_some_and(|m| !m.is_empty())
}

/// Runs `first..first + count` of `arm`, each followed by its zero-create control, or the first
/// void (which fails the arm).
fn runs(arm: Arm, first: usize, count: usize) -> Vec<Reading> {
    let mut out = Vec::new();
    for run in first..first + count {
        match run_arm(arm, run) {
            Ok(mut r) => {
                r.control_allocs = zero_create_control(r.window);
                out.push(r);
            }
            Err(void) => panic!("AD-T5 VOID arm={} run={run}: {void}", arm.name()),
        }
    }
    out
}

fn assert_green(arm: Arm, readings: &[Reading]) {
    if mutated() {
        return;
    }
    for (run, r) in readings.iter().enumerate() {
        assert_eq!(r.control_allocs, 0, "AD-T5 arm={} run={run}: the zero-create control allocated on the creating thread", arm.name());
        assert_eq!(r.chunk_inline_allocs, 0, "AD-T5 arm={} run={run}: chunk_inline_allocs must be 0 (Green on T1)", arm.name());
        assert_eq!(r.pending_reallocs, 0, "AD-T5 arm={} run={run}: pending_reallocs must be 0 (R1.7)", arm.name());
        if let Arm::Strict(_) = arm {
            assert_eq!(r.pool_refills, 0, "AD-T5 arm={} run={run}: pool_refills must be 0 in the strict arm (R1.2)", arm.name());
        }
    }
}

/// The live arm, the one ⟨s⟩, ⟨α⟩ and K-A3 read: flusher and housekeeping running, 5 runs (to 20
/// for the take-count parities), each followed by its zero-create control.
#[test]
fn ad_t5_live_arm_and_control() {
    let mut readings = runs(Arm::Live, 0, RUNS);
    assert_green(Arm::Live, &readings);
    let parities = |rs: &[Reading]| (rs.iter().any(|r| r.group_flights % 2 == 0), rs.iter().any(|r| r.group_flights % 2 == 1));
    while parities(&readings) != (true, true) && readings.len() < MAX_LIVE_RUNS {
        let more = runs(Arm::Live, readings.len(), 1);
        assert_green(Arm::Live, &more);
        readings.extend(more);
    }
    if !mutated() {
        assert_eq!(
            parities(&readings),
            (true, true),
            "AD-T5 live arm: the take counts over {} runs never showed both parities (R1.7)",
            readings.len()
        );
    }
}

/// The strict arm: the hold hooks set, N = 1,000, `async_fallbacks.lag == 0` and an end within
/// MAX_LAG/2 as premises.
#[test]
fn ad_t5_strict_arm() {
    let readings = runs(Arm::Strict(N_STRICT), 0, RUNS);
    assert_green(Arm::Strict(N_STRICT), &readings);
}

/// The strict arm at N = 2, run on the fire-check binary only (create_path_fullfsync's arm, and
/// async_waits_durable's kill): unmutated as well as mutated.
#[test]
fn ad_t5_strict_arm_n2() {
    if !fe_mutants_compiled() {
        println!("AD-T5 strict_n2: fire-check binary only (built without fe-mutants), not run");
        return;
    }
    let readings = runs(Arm::Strict(2), 0, RUNS);
    assert_green(Arm::Strict(2), &readings);
}

/// The ε arm: the live arm's procedure with the pool at its registered depth only and N_ε = 10^5,
/// beyond the pre-provisioned window. Its counts are printed, not asserted.
#[test]
fn ad_t5_epsilon_arm() {
    let readings = runs(Arm::Eps, 0, RUNS);
    for r in &readings {
        println!(
            "AD-T5 EPSILON flushes_per_create=external writes_per_create=external allocs_per_create={:.3e} chunk_inline_allocs={}",
            r.allocs as f64 / r.creates.max(1) as f64,
            r.chunk_inline_allocs
        );
    }
}
