//! Arm `conc` (frontier/round11/r11-walpin-conc/PREREG.md §0): T trunk writer threads, R reader threads
//! serving K branch sessions, and optionally a TRUNCATE-checkpoint storm, all on one database.
//!
//! The model check never asks the engine: each writer publishes how many of its writes it has
//! started and committed, a fork is bracketed by reading `committed` before it and `started` after
//! it, and a branch read must be a value some write inside that bracket could have left.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::walpin::{
    self, WalPinCounters, CKPT_OUTCOMES, FW3_HIST_BUCKETS, RESTART_GATES,
};
use turso_core::branch::Branch;
use turso_core::{Connection, Database, LimboError, Value};

use super::{
    clock_tick_ns, die, not_a_result, percentile, trunk_value, Args, Bench, TRUNK_ROWS, VALUE_LEN,
};

/// Rows a session remembers for the re-read (stability) check.
const MEMO: usize = 8;
/// Rows the end-of-run trunk check samples.
const TRUNK_SAMPLES: u64 = 64;

pub(crate) struct ConcArgs {
    pub t: u64,
    pub r: usize,
    /// Session life in read transactions; 0 = never refork.
    pub m: u64,
    pub held: bool,
    /// `--rows hot:P`: P leaves.
    pub hot_leaves: Option<u64>,
    pub storm: bool,
    pub wbusy_timeout: bool,
    /// r11-walpin-conc amendment 26 (`--trunk-op update|insert`): None keeps every earlier cell's
    /// behaviour (UPDATE, no page_count, no integrity_check); Some(false) is the UPDATE control,
    /// Some(true) makes writer w's j-th commit INSERT row `inserted_row(t, w, j)` past the seed.
    /// Either Some prints page_count on every conc line and ends with PRAGMA integrity_check.
    pub trunk_op: Option<bool>,
    /// r11-walpin-conc amendment 30 (`--active S`): the readers visit only the first S of the K
    /// sessions; the other K - S stay live branches, forked at setup and never read. None visits
    /// all K (every earlier cell). Holds a fork's age at its reads fixed while K varies.
    pub active: Option<usize>,
}

impl Default for ConcArgs {
    fn default() -> Self {
        ConcArgs {
            t: 1,
            r: 1,
            m: 4,
            held: false,
            hot_leaves: None,
            storm: false,
            wbusy_timeout: false,
            trunk_op: None,
            active: None,
        }
    }
}

/// Of the sessions dealt round-robin to reader `tid` of `r` (global index `j * r + tid` for its
/// `j`-th), how many are among the first `active` globally.
fn active_in_thread(active: usize, r: usize, tid: usize) -> usize {
    if tid >= active {
        0
    } else {
        (active - tid).div_ceil(r)
    }
}

/// Which rows each writer owns and the order it writes them in.
struct Layout {
    t: u64,
    /// Rows in play (all, or the hot ones): 1..=rows.
    rows: u64,
    /// Rows per writer.
    n_w: u64,
    hot: bool,
    /// For row r, the first j at which its owner writes it (its writes are j0, j0 + n_w, ...).
    first_j: Vec<u64>,
}

impl Layout {
    fn new(t: u64, hot_leaves: Option<u64>) -> Self {
        let rows = match hot_leaves {
            Some(p) => (37 * p / t) * t,
            None => TRUNK_ROWS as u64,
        };
        if t == 0 || rows == 0 || rows % t != 0 {
            die(&format!("--t {t} must divide the {rows} rows in play"));
        }
        let n_w = rows / t;
        let hot = hot_leaves.is_some();
        if !hot && gcd(37, n_w) != 1 {
            die(&format!("the walk step 37 must be coprime with {n_w} rows per writer"));
        }
        let mut lay = Layout {
            t,
            rows,
            n_w,
            hot,
            first_j: vec![u64::MAX; rows as usize],
        };
        for w in 0..t {
            for j in 0..n_w {
                let r = lay.row(w, j) as usize;
                if lay.first_j[r - 1] != u64::MAX {
                    die("the writer walk is not a permutation");
                }
                lay.first_j[r - 1] = j;
            }
        }
        lay
    }

    fn row(&self, w: u64, j: u64) -> i64 {
        let idx = if self.hot { j % self.n_w } else { (37 * j) % self.n_w };
        (idx * self.t + w + 1) as i64
    }

    fn owner(&self, row: i64) -> usize {
        ((row as u64 - 1) % self.t) as usize
    }

    /// The last write of `row` among its owner's first `c` writes.
    fn last_write(&self, row: i64, c: u64) -> Option<u64> {
        let j0 = self.first_j[(row - 1) as usize];
        (c > j0).then(|| j0 + self.n_w * ((c - 1 - j0) / self.n_w))
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

fn writer_value(w: u64, j: u64) -> String {
    format!("w{w:02}j{j:0>width$}", width = VALUE_LEN - 4)
}

/// r11-walpin-conc amendment 26: the row writer `w` INSERTs at its `j`-th commit in the insert arm,
/// past the seed's rows, unique across writers.
fn inserted_row(t: u64, w: u64, j: u64) -> i64 {
    TRUNK_ROWS + 1 + (j * t + w) as i64
}

fn parse_writer_value(v: &str) -> Option<(u64, u64)> {
    if v.len() != VALUE_LEN || !v.starts_with('w') || v.as_bytes()[3] != b'j' {
        return None;
    }
    Some((v[1..3].parse().ok()?, v[4..].parse().ok()?))
}

/// What every thread publishes. Counters are relaxed; `started`/`committed` carry the model.
struct Shared {
    started: Vec<AtomicU64>,
    committed: Vec<AtomicU64>,
    commits_total: AtomicU64,
    writers_done: AtomicBool,
    w_busy: AtomicU64,
    w_busy_snapshot: AtomicU64,
    forks: AtomicU64,
    fork_busy: AtomicU64,
    reaps: AtomicU64,
    reads: AtomicU64,
    read_busy: AtomicU64,
    connects: AtomicU64,
    storm_ok: AtomicU64,
    storm_busy: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct SharedSnap {
    commits: u64,
    w_busy: u64,
    w_busy_snapshot: u64,
    forks: u64,
    fork_busy: u64,
    reaps: u64,
    reads: u64,
    read_busy: u64,
    connects: u64,
    storm_ok: u64,
    storm_busy: u64,
}

impl Shared {
    fn new(t: u64) -> Self {
        let zeros = || (0..t).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        Shared {
            started: zeros(),
            committed: zeros(),
            commits_total: AtomicU64::new(0),
            writers_done: AtomicBool::new(false),
            w_busy: AtomicU64::new(0),
            w_busy_snapshot: AtomicU64::new(0),
            forks: AtomicU64::new(0),
            fork_busy: AtomicU64::new(0),
            reaps: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            read_busy: AtomicU64::new(0),
            connects: AtomicU64::new(0),
            storm_ok: AtomicU64::new(0),
            storm_busy: AtomicU64::new(0),
        }
    }

    fn snap(&self) -> SharedSnap {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        SharedSnap {
            commits: l(&self.commits_total),
            w_busy: l(&self.w_busy),
            w_busy_snapshot: l(&self.w_busy_snapshot),
            forks: l(&self.forks),
            fork_busy: l(&self.fork_busy),
            reaps: l(&self.reaps),
            reads: l(&self.reads),
            read_busy: l(&self.read_busy),
            connects: l(&self.connects),
            storm_ok: l(&self.storm_ok),
            storm_busy: l(&self.storm_busy),
        }
    }
}

fn bump(a: &AtomicU64) {
    a.fetch_add(1, Ordering::Relaxed);
}

/// A branch session: its handle, its connection while one is open, the fork bracket, and the rows
/// it has read.
struct Sess {
    branch: Option<Branch>,
    conn: Option<Arc<Connection>>,
    lo: Box<[u64]>,
    hi: Box<[u64]>,
    reads: u64,
    memo: Vec<(i64, String)>,
}

fn select_once(conn: &Arc<Connection>, id: i64) -> turso_core::Result<Option<String>> {
    let rows = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))?
        .run_collect_rows()?;
    Ok(match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => Some(t.as_str().to_string()),
            _ => None,
        },
        _ => None,
    })
}

/// r11-walpin-conc amendment 26: an inserted row committed before the session's fork must read
/// back; one whose insert started after the fork must be absent; in between either, but the same
/// answer on every re-read in the session (the memo, as for seed rows; absent is memoized as "").
fn check_inserted(s: &mut Sess, row: i64, w: u64, j: u64, got: Option<String>) {
    let (lo, hi) = (s.lo[w as usize], s.hi[w as usize]);
    let ok = match &got {
        Some(v) => *v == writer_value(w, j) && j < hi,
        None => j >= lo,
    };
    if !ok {
        not_a_result(&format!(
            "branch read of inserted row {row} (writer {w} insert {j}) = {got:?}; fork bracket committed {lo} .. started {hi}"
        ));
    }
    let seen = got.unwrap_or_default();
    if let Some((_, v)) = s.memo.iter().find(|(r, _)| *r == row) {
        if *v != seen {
            not_a_result(&format!("branch re-read inserted row {row} = {seen:?}, first read {v:?}"));
        }
    } else if s.memo.len() < MEMO {
        s.memo.push((row, seen));
    }
}

fn check(lay: &Layout, s: &mut Sess, row: i64, got: Option<String>, insert: bool) {
    let Some(got) = got else {
        not_a_result(&format!("branch read of row {row}: not one text row"));
    };
    let w = lay.owner(row);
    // Insert arm: the seed rows are never written.
    let base = if insert { None } else { lay.last_write(row, s.lo[w]) };
    let ok = if insert {
        got == trunk_value(row)
    } else if got == trunk_value(row) {
        base.is_none()
    } else if let Some((gw, gj)) = parse_writer_value(&got) {
        gw as usize == w && lay.row(w as u64, gj) == row && gj < s.hi[w] && base.is_none_or(|b| gj >= b)
    } else {
        false
    };
    if !ok {
        not_a_result(&format!(
            "branch read row {row} = {got:?}; fork bracket for writer {w}: committed {} .. started {}, \
             last write below the bracket {base:?}",
            s.lo[w], s.hi[w]
        ));
    }
    if let Some((_, v)) = s.memo.iter().find(|(r, _)| *r == row) {
        if *v != got {
            not_a_result(&format!("branch re-read row {row} = {got:?}, first read {v:?}"));
        }
    } else if s.memo.len() < MEMO {
        s.memo.push((row, got));
    }
}

fn is_busy(e: &LimboError) -> bool {
    matches!(e, LimboError::Busy | LimboError::BusySnapshot)
}

fn refork(s: &mut Sess, trunk: &Arc<Connection>, sh: &Shared, held: bool) {
    s.conn = None;
    if let Some(b) = s.branch.take() {
        b.reap().unwrap_or_else(|e| not_a_result(&format!("reap: {e}")));
        bump(&sh.reaps);
    }
    let lo: Box<[u64]> = sh.committed.iter().map(|c| c.load(Ordering::Acquire)).collect();
    let branch = loop {
        match trunk.fork_branch() {
            Ok(b) => break b,
            Err(e) if is_busy(&e) => {
                bump(&sh.fork_busy);
                std::thread::yield_now();
            }
            Err(e) => not_a_result(&format!("fork: {e}")),
        }
    };
    let hi: Box<[u64]> = sh.started.iter().map(|c| c.load(Ordering::Acquire)).collect();
    bump(&sh.forks);
    if held {
        s.conn = Some(branch.connect().unwrap_or_else(|e| not_a_result(&format!("connect: {e}"))));
    }
    s.branch = Some(branch);
    s.lo = lo;
    s.hi = hi;
    s.reads = 0;
    s.memo.clear();
}

/// Writer `w`: `n` one-row autocommits. Returns (global commit seq, svc ns, e2e ns) per commit when
/// timing.
#[allow(clippy::too_many_arguments)]
fn writer(
    db: &Arc<Database>,
    sh: &Shared,
    lay: &Layout,
    w: u64,
    n: u64,
    wbusy_timeout: bool,
    timing: bool,
    insert: bool,
) -> Vec<(u64, u32, u32)> {
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    if wbusy_timeout {
        conn.set_busy_timeout(Duration::from_secs(5));
    }
    let mut samples = Vec::with_capacity(if timing { n as usize } else { 0 });
    let ns = |d: Duration| d.as_nanos().min(u32::MAX as u128) as u32;
    for j in 0..n {
        let sql = if insert {
            format!(
                "INSERT INTO t VALUES ({}, '{}')",
                inserted_row(lay.t, w, j),
                writer_value(w, j)
            )
        } else {
            format!(
                "UPDATE t SET v = '{}' WHERE id = {}",
                writer_value(w, j),
                lay.row(w, j)
            )
        };
        sh.started[w as usize].store(j + 1, Ordering::Release);
        let t0 = Instant::now();
        let svc = loop {
            let t1 = Instant::now();
            match conn.execute(&sql) {
                Ok(()) => break t1.elapsed(),
                Err(LimboError::Busy) => {
                    bump(&sh.w_busy);
                    std::thread::yield_now();
                }
                Err(LimboError::BusySnapshot) => {
                    bump(&sh.w_busy_snapshot);
                    std::thread::yield_now();
                }
                Err(e) => not_a_result(&format!("writer {w} write {j}: {e}")),
            }
        };
        let e2e = t0.elapsed();
        sh.committed[w as usize].store(j + 1, Ordering::Release);
        let seq = sh.commits_total.fetch_add(1, Ordering::AcqRel);
        if timing {
            samples.push((seq, ns(svc), ns(e2e)));
        }
    }
    samples
}

/// Reader thread: round-robin over its sessions until the writers finish. Returns (commits at the
/// read's start, ns) per read when timing, and its sessions for teardown.
fn reader(
    db: &Arc<Database>,
    sh: &Shared,
    lay: &Layout,
    c: &ConcArgs,
    mut sessions: Vec<Sess>,
    tid: usize,
    timing: bool,
) -> (Vec<(u32, u32)>, Vec<Sess>) {
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
    let mut samples = Vec::new();
    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ ((tid as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
    let mut i = 0;
    let n = c
        .active
        .map_or(sessions.len(), |a| active_in_thread(a, c.r, tid).min(sessions.len()));
    if n == 0 {
        return (samples, sessions);
    }
    while !sh.writers_done.load(Ordering::Acquire) {
        let s = &mut sessions[i];
        i = (i + 1) % n;
        if c.m > 0 && s.reads >= c.m {
            refork(s, &trunk, sh, c.held);
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Insert arm (amendment 26): every other read asks for an inserted row, one the session's fork
        // committed (it must read back) or one started after it (it must be absent).
        let ins = (c.trunk_op == Some(true) && (x >> 1) & 1 == 0).then(|| {
            let w = (x >> 8) % lay.t;
            let j = (x >> 20) % (s.hi[w as usize] + 8);
            (w, j)
        });
        let row = match ins {
            Some((w, j)) => inserted_row(lay.t, w, j),
            None => (x % lay.rows) as i64 + 1,
        };
        let at = sh.commits_total.load(Ordering::Relaxed);
        let t = Instant::now();
        let conn = match &s.conn {
            Some(conn) => conn.clone(),
            None => {
                bump(&sh.connects);
                let branch = s.branch.as_ref().expect("a session always has a branch");
                branch
                    .connect()
                    .unwrap_or_else(|e| not_a_result(&format!("connect: {e}")))
            }
        };
        let got = loop {
            match select_once(&conn, row) {
                Ok(v) => break v,
                Err(LimboError::Busy) => bump(&sh.read_busy),
                Err(e) => not_a_result(&format!("branch read of row {row}: {e}")),
            }
        };
        drop(conn);
        let el = t.elapsed();
        if timing {
            samples.push((at as u32, el.as_nanos().min(u32::MAX as u128) as u32));
        }
        match ins {
            Some((w, j)) => check_inserted(s, row, w, j, got),
            None => check(lay, s, row, got, c.trunk_op == Some(true)),
        }
        s.reads += 1;
        bump(&sh.reads);
    }
    (samples, sessions)
}

/// `PRAGMA wal_checkpoint(TRUNCATE)` in a loop until the writers finish.
fn storm(db: &Arc<Database>, sh: &Shared) {
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    while !sh.writers_done.load(Ordering::Acquire) {
        let r = conn
            .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
            .and_then(|mut s| s.run_collect_rows());
        match r {
            // SQLite's result row: (busy, log frames, checkpointed frames).
            Ok(rows) if rows.first().and_then(|r| r[0].as_int()) == Some(0) => bump(&sh.storm_ok),
            Ok(_) => bump(&sh.storm_busy),
            Err(e) if is_busy(&e) => bump(&sh.storm_busy),
            Err(e) => not_a_result(&format!("storm checkpoint: {e}")),
        }
        std::thread::yield_now();
    }
}

struct Lines {
    last_c: WalPinCounters,
    last_s: SharedSnap,
    /// r11-walpin-conc amendment 26: print the trunk's page_count on every line (`--trunk-op`).
    page_count: bool,
}

impl Lines {
    fn print(&mut self, bench: &mut Bench, sh: &Shared, label: &str, h: u64) {
        let c = walpin::counters();
        let s = sh.snap();
        let b = bench.db.branch_stats();
        let hist: Vec<u64> = (0..FW3_HIST_BUCKETS)
            .map(|i| c.fw3_hist[i] - self.last_c.fw3_hist[i])
            .collect();
        let locks: Vec<u64> = (0..3).map(|i| c.store_locks[i] - self.last_c.store_locks[i]).collect();
        let contended: Vec<u64> = (0..3)
            .map(|i| c.store_contended[i] - self.last_c.store_contended[i])
            .collect();
        let gates: Vec<u64> = (0..RESTART_GATES)
            .map(|i| c.restart_gate[i] - self.last_c.restart_gate[i])
            .collect();
        let ckpt: Vec<u64> = (0..CKPT_OUTCOMES)
            .map(|i| c.ckpt_outcome[i] - self.last_c.ckpt_outcome[i])
            .collect();
        let (lc, ls) = (&self.last_c, &self.last_s);
        let pc = if self.page_count {
            let rows = bench
                .trunk
                .prepare("PRAGMA page_count")
                .and_then(|mut st| st.run_collect_rows())
                .unwrap_or_else(|e| not_a_result(&format!("page_count: {e}")));
            format!(" page_count={}", rows[0][0].as_int().unwrap_or(-1))
        } else {
            String::new()
        };
        println!(
            "# conc {label} H={h} commits_now={} | d_commits={} d_w_busy={} d_w_busy_snapshot={} d_forks={} \
             d_fork_busy={} d_reaps={} d_reads={} d_read_busy={} d_connects={} d_storm_ok={} d_storm_busy={} | \
             fw3 d_trunk_reads={} d_calls_trunk={} d_retries={} d_store={} d_gen={} d_readerr={} d_busy={} \
             d_multi_store={} max_retries={} d_hist={hist:?} | store d_locks={locks:?} d_contended={contended:?} | \
             restart d_gates={gates:?} ckpt d_outcomes={ckpt:?} | live_branches={} arena_in_use={} arena_free={}{pc}",
            s.commits,
            s.commits - ls.commits,
            s.w_busy - ls.w_busy,
            s.w_busy_snapshot - ls.w_busy_snapshot,
            s.forks - ls.forks,
            s.fork_busy - ls.fork_busy,
            s.reaps - ls.reaps,
            s.reads - ls.reads,
            s.read_busy - ls.read_busy,
            s.connects - ls.connects,
            s.storm_ok - ls.storm_ok,
            s.storm_busy - ls.storm_busy,
            c.fw3_trunk_reads - lc.fw3_trunk_reads,
            c.fw3_calls_trunk - lc.fw3_calls_trunk,
            c.fw3_retries - lc.fw3_retries,
            c.fw3_retry_store - lc.fw3_retry_store,
            c.fw3_retry_gen - lc.fw3_retry_gen,
            c.fw3_retry_readerr - lc.fw3_retry_readerr,
            c.fw3_busy - lc.fw3_busy,
            c.fw3_multi_store - lc.fw3_multi_store,
            c.fw3_max_retries,
            b.live_branches,
            b.arena_slots_in_use,
            b.arena_slots_free,
        );
        bench.state(label, h);
        self.last_c = c;
        self.last_s = s;
    }
}

fn pcts(v: &mut [u32]) -> String {
    if v.is_empty() {
        return "n=0".to_string();
    }
    v.sort_unstable();
    let s: Vec<u64> = v.iter().map(|&x| x as u64).collect();
    format!(
        "n={} p50={:.2} p90={:.2} p99={:.2} max={:.2}",
        s.len(),
        percentile(&s, 50.0) as f64 / 1e3,
        percentile(&s, 90.0) as f64 / 1e3,
        percentile(&s, 99.0) as f64 / 1e3,
        s[s.len() - 1] as f64 / 1e3
    )
}

pub(crate) fn run_conc(bench: &mut Bench, args: &Args) {
    let c = &args.conc;
    let lay = Layout::new(c.t, c.hot_leaves);
    let h_max = *args.points.last().unwrap();
    if h_max % c.t != 0 {
        die("--h must be a multiple of --t");
    }
    let per_writer = h_max / c.t;
    let k = args.k as usize;
    if c.r == 0 || k < c.r {
        die("--r must be positive and at most --k");
    }
    println!(
        "# conc t={} r={} k={k} m={} conn={} rows={} (hot_leaves={:?}) storm={} wbusy={} per_writer={per_writer}",
        c.t,
        c.r,
        c.m,
        if c.held { "held" } else { "mux" },
        lay.rows,
        c.hot_leaves,
        c.storm,
        if c.wbusy_timeout { "timeout" } else { "spin" },
    );
    if let Some(insert) = c.trunk_op {
        println!("# conc trunk_op={}", if insert { "insert" } else { "update" });
    }
    if let Some(a) = c.active {
        if a < c.r || a > k {
            die("--active must be at least --r and at most --k");
        }
        println!("# conc active={a} idle={}", k - a);
    }
    let zeros: Box<[u64]> = vec![0; c.t as usize].into();
    let mut per_thread: Vec<Vec<Sess>> = (0..c.r).map(|_| Vec::with_capacity(k / c.r + 1)).collect();
    for i in 0..k {
        let branch = bench.trunk.fork_branch().unwrap();
        let conn = c.held.then(|| branch.connect().unwrap());
        per_thread[i % c.r].push(Sess {
            branch: Some(branch),
            conn,
            lo: zeros.clone(),
            hi: zeros.clone(),
            reads: 0,
            memo: Vec::new(),
        });
    }
    let sh = Shared::new(c.t);
    let mut lines = Lines {
        last_c: walpin::counters(),
        last_s: sh.snap(),
        page_count: c.trunk_op.is_some(),
    };
    lines.print(bench, &sh, "opened", 0);
    let db = bench.db.clone();
    let timing = args.timing;
    let insert = c.trunk_op == Some(true);
    let t_start = Instant::now();
    let (w_samples, r_samples, sessions) = std::thread::scope(|scope| {
        let writers: Vec<_> = (0..c.t)
            .map(|w| {
                let (db, sh, lay) = (&db, &sh, &lay);
                scope.spawn(move || {
                    writer(db, sh, lay, w, per_writer, c.wbusy_timeout, timing, insert)
                })
            })
            .collect();
        let readers: Vec<_> = per_thread
            .into_iter()
            .enumerate()
            .map(|(tid, sess)| {
                let (db, sh, lay) = (&db, &sh, &lay);
                scope.spawn(move || reader(db, sh, lay, c, sess, tid, timing))
            })
            .collect();
        let stormer = c.storm.then(|| {
            let (db, sh) = (&db, &sh);
            scope.spawn(move || storm(db, sh))
        });
        let mut next = 0;
        loop {
            let done = sh.commits_total.load(Ordering::Acquire);
            while next < args.points.len() && done >= args.points[next] {
                lines.print(bench, &sh, "point", args.points[next]);
                next += 1;
            }
            if next == args.points.len() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let w_samples: Vec<_> = writers.into_iter().flat_map(|h| h.join().unwrap()).collect();
        sh.writers_done.store(true, Ordering::Release);
        let mut r_samples = Vec::new();
        let mut sessions = Vec::new();
        for h in readers {
            let (s, sess) = h.join().unwrap();
            r_samples.extend(s);
            sessions.extend(sess);
        }
        if let Some(h) = stormer {
            h.join().unwrap();
        }
        (w_samples, r_samples, sessions)
    });
    let wall = t_start.elapsed();
    lines.print(bench, &sh, "end", h_max);
    let s = sh.snap();
    println!(
        "# conc totals wall_s={:.3} commits={} reads={} forks={} reaps={} w_busy={} read_busy={} fork_busy={} \
         storm_ok={} storm_busy={}",
        wall.as_secs_f64(),
        s.commits,
        s.reads,
        s.forks,
        s.reaps,
        s.w_busy,
        s.read_busy,
        s.fork_busy,
        s.storm_ok,
        s.storm_busy
    );

    // The trunk reads every writer's last write (or the seed value) for sampled rows.
    let step = (lay.rows / TRUNK_SAMPLES).max(1);
    for i in 0..lay.rows.min(TRUNK_SAMPLES) {
        let row = (i * step) as i64 + 1;
        let w = lay.owner(row);
        let want = match lay.last_write(row, per_writer) {
            Some(j) if !insert => writer_value(w as u64, j),
            _ => trunk_value(row),
        };
        let got = super::read_v(&bench.trunk, row);
        if got != want {
            not_a_result(&format!("trunk row {row} = {got:?}, its writer's last write is {want:?}"));
        }
    }
    println!("# conc trunk_check rows={} ok", lay.rows.min(TRUNK_SAMPLES));
    if insert {
        // Amendment 26: sampled inserted rows read back with their writer's value.
        for i in 0..TRUNK_SAMPLES {
            let (w, j) = (i % c.t, i * per_writer / TRUNK_SAMPLES);
            let row = inserted_row(c.t, w, j);
            let got = super::read_v(&bench.trunk, row);
            if got != writer_value(w, j) {
                not_a_result(&format!("trunk inserted row {row} = {got:?}, want {:?}", writer_value(w, j)));
            }
        }
        println!("# conc trunk_check inserted rows={TRUNK_SAMPLES} ok");
    }
    if c.trunk_op.is_some() {
        // Amendment 26: the trunk passes PRAGMA integrity_check at the end.
        let rows = bench
            .trunk
            .prepare("PRAGMA integrity_check")
            .and_then(|mut st| st.run_collect_rows())
            .unwrap_or_else(|e| not_a_result(&format!("integrity_check: {e}")));
        let first = match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(t)) => t.as_str().to_string(),
            other => format!("{other:?}"),
        };
        if first != "ok" {
            not_a_result(&format!("trunk integrity_check: {first}"));
        }
        println!("# conc integrity ok");
    }

    if timing {
        println!("# clock tick {:.0} ns (Instant)", clock_tick_ns());
        let mut w_sorted = w_samples;
        w_sorted.sort_unstable_by_key(|x| x.0);
        let mut lo = 0u64;
        for &p in &args.points {
            let win: Vec<_> = w_sorted.iter().filter(|x| x.0 >= lo && x.0 < p).collect();
            let mut svc: Vec<u32> = win.iter().map(|x| x.1).collect();
            let mut e2e: Vec<u32> = win.iter().map(|x| x.2).collect();
            let mut rd: Vec<u32> = r_samples
                .iter()
                .filter(|x| (x.0 as u64) >= lo && (x.0 as u64) < p)
                .map(|x| x.1)
                .collect();
            println!(
                "# timing H=[{lo},{p}) commit_svc_us {} | commit_e2e_us {} | read_us {}",
                pcts(&mut svc),
                pcts(&mut e2e),
                pcts(&mut rd)
            );
            lo = p;
        }
    }

    // Resident page-cache pages of the sessions still holding a connection (held arms).
    let pages: Vec<(usize, usize)> = sessions
        .iter()
        .filter_map(|s| s.conn.as_ref().map(|c| c.walpin_cache_pages()))
        .collect();
    let total: usize = pages.iter().map(|p| p.0).sum();
    println!(
        "# conc cache_pages held={} total={total} max={} min={} capacity={}",
        pages.len(),
        pages.iter().map(|p| p.0).max().unwrap_or(0),
        pages.iter().map(|p| p.0).min().unwrap_or(0),
        pages.iter().map(|p| p.1).max().unwrap_or(0),
    );
    drop(sessions);
    let b = bench.db.branch_stats();
    println!(
        "# conc teardown live_branches={} arena_in_use={} arena_free={}",
        b.live_branches, b.arena_slots_in_use, b.arena_slots_free
    );
}
