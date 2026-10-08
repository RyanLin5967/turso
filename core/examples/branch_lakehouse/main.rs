//! r12-lakehouse: the lakehouse time-travel shape on the composed branch store — FIFO (TTL)
//! snapshots of a trunk written every tick, with a periodic whole-table rewrite — counting the three
//! per-operation terms the round-12 scout's reopen condition names. PREREG: artie-research
//! `frontier/round12/r12-lakehouse/PREREG.md`, registered before any build or run.
//!
//! Counters only: nothing here is timed.
//!
//! * K1: F2 index entries visited per reap (`BranchWork::gc_range_entries`, the delta over the reap).
//! * K2 (durable arm): catalog trunk-version rows read per reap (`branch_catalog_trunk_counters`);
//!   the first reap after each open is also printed on its own line.
//! * K3 (volatile arm): FS9 clone fills per read of a past trunk page (`retained_clone_fills` over
//!   `retained_clone_fills + retained_shared_hits`).
//!
//! A tick: one trunk transaction (INSERT `k` rows at the high end, DELETE the `k` lowest, UPDATE `k`
//! distinct random live rows to fresh values); every `P` ticks one more transaction that rewrites
//! every row (each value rotated by 37 characters, so every byte of every row changes); then one
//! snapshot is forked; then the oldest snapshot beyond `R` is reaped; then `readers` reads, each on
//! a random live snapshot: connect, scan `scan` consecutive ids, check every row against the
//! harness's own model, close. A mismatch prints NOT A RESULT and exits 1.
//!
//! The table holds `rows` rows at every tick (ids `1 + k*t ..= rows + k*t` after tick `t`), so a
//! snapshot forked after tick `s` sees exactly those ids for `t = s`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use turso_core::branch::{Branch, BranchId};
use turso_core::{Connection, Database, Value};

/// Requested bytes of every live heap allocation.
static HEAP_LIVE: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            HEAP_LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            HEAP_LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        HEAP_LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            HEAP_LIVE.fetch_add(new_size, Ordering::Relaxed);
            HEAP_LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn heap_live_bytes() -> usize {
    HEAP_LIVE.load(Ordering::Relaxed)
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

const VALUE_LEN: usize = 100;
/// Characters a whole-table rewrite rotates each value by (coprime with VALUE_LEN).
const ROTATE: usize = 37;

pub struct Args {
    r: usize,
    ticks: u64,
    windows: u64,
    rows: i64,
    k: i64,
    rewrite_every: u64,
    readers: usize,
    scan: i64,
    seed: u64,
    fs9: bool,
    reopen_every: u64,
    sync: bool,
    /// The trunk's `PRAGMA synchronous`. NORMAL by default: under OFF the trunk's WAL never restarted
    /// and grew by every frame written (raw/v_r7200.txt, `files_bytes` +63 KB per tick).
    synchronous: String,
    dir: Option<PathBuf>,
    counters_only: bool,
    /// Reap every snapshot at the end and require an empty store (default). `none` skips it: the durable
    /// catalog's WAL grew by 3.6 GB over 43,200 consecutive teardown reaps (PREREG A7).
    teardown: bool,
    /// PREREG A9.2 (E2 straggler): the first snapshot is a TAG, kept until teardown and never read;
    /// FIFO expiry keeps `r` other snapshots live, so the tag pins the oldest live fork.
    tag: bool,
    /// PREREG A8/A10 mass-expiry mode: K (0 = off), catalog checkpoints after the cut, and ticks
    /// between them.
    mass_expiry: u64,
    mass_checkpoints: u64,
    mass_between: u64,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_lakehouse: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut a = Args {
        r: 0,
        ticks: 0,
        windows: 20,
        rows: 20_000,
        k: 10,
        rewrite_every: 1_440,
        readers: 2,
        scan: 32,
        seed: 0x9E37_79B9_7F4A_7C15,
        fs9: true,
        reopen_every: 0,
        sync: false,
        synchronous: "NORMAL".to_string(),
        dir: None,
        counters_only: false,
        teardown: true,
        tag: false,
        mass_expiry: 0,
        mass_checkpoints: 5,
        mass_between: 100,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        let num = |v: String| -> u64 { v.parse().unwrap_or_else(|_| die(&format!("bad number {v}"))) };
        match flag.as_str() {
            "--r" => a.r = num(val()) as usize,
            "--ticks" => a.ticks = num(val()),
            "--windows" => a.windows = num(val()),
            "--rows" => a.rows = num(val()) as i64,
            "--k" => a.k = num(val()) as i64,
            "--rewrite-every" => a.rewrite_every = num(val()),
            "--readers" => a.readers = num(val()) as usize,
            "--scan" => a.scan = num(val()) as i64,
            "--seed" => a.seed = num(val()),
            "--fs9" => {
                a.fs9 = match val().as_str() {
                    "on" => true,
                    "off" => false,
                    v => die(&format!("--fs9 on|off, not {v}")),
                }
            }
            "--reopen-every" => a.reopen_every = num(val()),
            "--sync" => {
                a.sync = match val().as_str() {
                    "on" => true,
                    "off" => false,
                    v => die(&format!("--sync on|off, not {v}")),
                }
            }
            "--synchronous" => {
                a.synchronous = match val().to_uppercase().as_str() {
                    s @ ("OFF" | "NORMAL" | "FULL") => s.to_string(),
                    v => die(&format!("--synchronous OFF|NORMAL|FULL, not {v}")),
                }
            }
            "--dir" => a.dir = Some(PathBuf::from(val())),
            "--counters-only" => a.counters_only = true,
            "--tag" => a.tag = true,
            "--mass-expiry" => a.mass_expiry = num(val()),
            "--mass-checkpoints" => a.mass_checkpoints = num(val()),
            "--mass-between" => a.mass_between = num(val()),
            "--teardown" => {
                a.teardown = match val().as_str() {
                    "full" => true,
                    "none" => false,
                    v => die(&format!("--teardown full|none, not {v}")),
                }
            }
            f => die(&format!("unknown flag {f}")),
        }
    }
    if a.r == 0 {
        die("--r <live snapshots> is required");
    }
    if !a.counters_only {
        die("--counters-only is required: this harness times nothing");
    }
    if a.ticks == 0 {
        a.ticks = 2 * a.r as u64;
    }
    if a.windows == 0 || a.ticks % a.windows != 0 {
        die("--ticks must be a positive multiple of --windows");
    }
    if a.rows < 2 * a.scan || a.k < 1 || a.k * 4 > a.rows || a.rewrite_every == 0 {
        die("need rows >= 2*scan, 1 <= k <= rows/4, rewrite-every >= 1");
    }
    if a.reopen_every > 0 && !arm::REOPENS {
        die("--reopen-every: this arm's store does not survive a reopen");
    }
    a
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The 100-character value of `seed` after `rot` whole-table rewrites: hex digits of a splitmix64
/// stream seeded by `seed`, rotated left by `ROTATE * rot` characters.
fn value(seed: u64, rot: u32) -> String {
    let mut s = Vec::with_capacity(VALUE_LEN);
    let mut x = seed;
    while s.len() < VALUE_LEN {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        for i in 0..16 {
            if s.len() == VALUE_LEN {
                break;
            }
            s.push(char::from_digit(((z >> (4 * i)) & 0xF) as u32, 16).unwrap() as u8);
        }
    }
    s.rotate_left((ROTATE * rot as usize) % VALUE_LEN);
    String::from_utf8(s).unwrap()
}

/// What every snapshot must read: each row's versions as `(tick, seed, rotations)`, oldest first,
/// pruned to what a live snapshot (or a later one) can still see.
struct Model {
    rows: i64,
    k: i64,
    hist: HashMap<i64, Vec<(u64, u64, u32)>>,
    /// The lowest id `hist` still holds.
    kept_from: i64,
}

impl Model {
    fn low(&self, tick: u64) -> i64 {
        1 + self.k * tick as i64
    }
    fn high(&self, tick: u64) -> i64 {
        self.rows + self.k * tick as i64
    }
    /// Record `id`'s value from `tick` on. A second write in the same tick replaces the first.
    fn put(&mut self, id: i64, tick: u64, seed: u64, rot: u32, oldest: Option<u64>) {
        let v = self.hist.entry(id).or_default();
        match v.last_mut() {
            Some(last) if last.0 == tick => *last = (tick, seed, rot),
            _ => v.push((tick, seed, rot)),
        }
        if let Some(o) = oldest {
            let drop = v.iter().skip(1).take_while(|e| e.0 <= o).count();
            v.drain(..drop);
        }
    }
    fn at(&self, id: i64, tick: u64) -> Option<(u64, u32)> {
        let v = self.hist.get(&id)?;
        v.iter().rev().find(|e| e.0 <= tick).map(|e| (e.1, e.2))
    }
    /// Forget rows no live or later snapshot can see: deleted at or before the oldest live fork.
    fn prune(&mut self, oldest: u64) {
        while (self.kept_from - 1) / self.k + 1 <= oldest as i64 {
            self.hist.remove(&self.kept_from);
            self.kept_from += 1;
        }
    }
}

enum Snap {
    Handle(Branch),
    Detached(BranchId),
}

struct Live {
    snap: Snap,
    tick: u64,
}

/// Per-reap samples of one counter.
#[derive(Default)]
struct Dist {
    v: Vec<u64>,
}

impl Dist {
    fn add(&mut self, x: u64) {
        self.v.push(x);
    }
    fn summary(&self) -> String {
        if self.v.is_empty() {
            return "n=0".to_string();
        }
        let mut s = self.v.clone();
        s.sort_unstable();
        let pct = |p: f64| s[((p / 100.0) * (s.len() - 1) as f64).round() as usize];
        let mean = s.iter().sum::<u64>() as f64 / s.len() as f64;
        format!(
            "n={} mean={mean:.3} p50={} p99={} max={}",
            s.len(),
            pct(50.0),
            pct(99.0),
            s[s.len() - 1]
        )
    }
}

struct Run {
    args: Args,
    path: PathBuf,
    db: Arc<Database>,
    trunk: Arc<Connection>,
    model: Model,
    rng: Rng,
    snaps: VecDeque<Live>,
    /// `--tag`: the pinned first snapshot.
    tag: Option<Live>,
    tick: u64,
    opens: u64,
    first_reap_after_open: bool,
}

impl Run {
    fn exec(&self, sql: &str) {
        if let Err(e) = self.trunk.execute(sql) {
            not_a_result(&format!("trunk statement failed at tick {}: {e}: {sql:.120}", self.tick));
        }
    }

    fn int(&self, sql: &str) -> i64 {
        self.trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    }

    fn oldest(&self) -> Option<u64> {
        self.snaps.front().map(|l| l.tick)
    }

    /// One tick's trunk transaction, then the rewrite when it is due.
    fn trunk_tick(&mut self) {
        let t = self.tick;
        let k = self.args.k;
        let oldest = self.oldest();
        let (low_before, high_before) = (self.model.low(t - 1), self.model.high(t - 1));
        let mut sql = String::from("INSERT INTO t VALUES ");
        let mut inserted = Vec::new();
        for i in 1..=k {
            let id = high_before + i;
            let seed = self.rng.next();
            if i > 1 {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, '{}')", value(seed, 0)));
            inserted.push((id, seed));
        }
        self.exec("BEGIN");
        self.exec(&sql);
        self.exec(&format!("DELETE FROM t WHERE id < {}", low_before + k));
        let (lo, hi) = (low_before + k, high_before + k);
        let mut picked: Vec<i64> = Vec::new();
        while (picked.len() as i64) < k {
            let id = lo + self.rng.below((hi - lo + 1) as u64) as i64;
            if !picked.contains(&id) {
                picked.push(id);
            }
        }
        let mut updated = Vec::new();
        for &id in &picked {
            let seed = self.rng.next();
            self.exec(&format!("UPDATE t SET v = '{}' WHERE id = {id}", value(seed, 0)));
            updated.push((id, seed));
        }
        self.exec("COMMIT");
        for (id, seed) in inserted.into_iter().chain(updated) {
            self.model.put(id, t, seed, 0, oldest);
        }
        if t % self.args.rewrite_every == 0 {
            self.rewrite(lo, hi, oldest);
        }
    }

    /// The whole-table rewrite at the current tick: every live row's value rotated by ROTATE.
    fn rewrite(&mut self, lo: i64, hi: i64, oldest: Option<u64>) {
        let t = self.tick;
        self.exec(&format!(
            "UPDATE t SET v = substr(v, {}) || substr(v, 1, {ROTATE})",
            ROTATE + 1
        ));
        for id in lo..=hi {
            let (seed, rot) = self.model.at(id, t).expect("a live row is modelled");
            self.model.put(id, t, seed, (rot + 1) % VALUE_LEN as u32, oldest);
        }
    }

    fn handle(&mut self, i: usize) -> &Branch {
        if let Snap::Detached(id) = self.snaps[i].snap {
            let b = arm::attach(&self.db, id);
            self.snaps[i].snap = Snap::Handle(b);
        }
        match &self.snaps[i].snap {
            Snap::Handle(b) => b,
            Snap::Detached(_) => unreachable!(),
        }
    }

    /// Scan `scan` consecutive ids on a random live snapshot and check every row.
    fn read(&mut self) -> usize {
        let i = self.rng.below(self.snaps.len() as u64) as usize;
        let s = self.snaps[i].tick;
        let a = self.model.low(s) + self.rng.below((self.args.rows - self.args.scan + 1) as u64) as i64;
        let b = a + self.args.scan - 1;
        let conn = match self.handle(i).connect() {
            Ok(c) => c,
            Err(e) => not_a_result(&format!("connect to the snapshot of tick {s} failed: {e}")),
        };
        let got = conn
            .prepare(format!("SELECT id, v FROM t WHERE id BETWEEN {a} AND {b} ORDER BY id"))
            .and_then(|mut st| st.run_collect_rows())
            .unwrap_or_else(|e| not_a_result(&format!("scan on the snapshot of tick {s}: {e}")));
        drop(conn);
        if got.len() as i64 != b - a + 1 {
            not_a_result(&format!(
                "tick {}: the snapshot of tick {s} returned {} rows for ids {a}..={b}",
                self.tick,
                got.len()
            ));
        }
        for (j, row) in got.iter().enumerate() {
            let id = a + j as i64;
            let (seed, rot) = self
                .model
                .at(id, s)
                .unwrap_or_else(|| not_a_result(&format!("the model lost row {id} at tick {s}")));
            let want = value(seed, rot);
            let ok = row[0].as_int() == Some(id)
                && matches!(&row[1], Value::Text(t) if t.as_str() == want);
            if !ok {
                not_a_result(&format!(
                    "tick {}: the snapshot of tick {s} read row {id} as {row:?}, the model says {want}",
                    self.tick
                ));
            }
        }
        got.len()
    }
}

/// Close the database — every snapshot handle detached, the trunk connection and the database
/// handle dropped — and open it again; the snapshots are re-attached as they are next used.
fn reopen(run: Run) -> Run {
    let Run {
        args,
        path,
        db,
        trunk,
        model,
        rng,
        snaps,
        tag,
        tick,
        opens,
        ..
    } = run;
    let tag = tag.map(|l| Live {
        snap: match l.snap {
            Snap::Handle(b) => Snap::Detached(arm::detach(b)),
            d => d,
        },
        tick: l.tick,
    });
    let snaps = snaps
        .into_iter()
        .map(|l| Live {
            snap: match l.snap {
                Snap::Handle(b) => Snap::Detached(arm::detach(b)),
                d => d,
            },
            tick: l.tick,
        })
        .collect();
    // The process-wide registry holds the database weakly: unless this was the last handle, the
    // "reopen" below would hand back the same instance, and nothing would have been reopened.
    let weak = Arc::downgrade(&db);
    drop(trunk);
    drop(db);
    if weak.upgrade().is_some() {
        not_a_result(&format!("tick {tick}: the database outlived its close, so a reopen would share it"));
    }
    let db = arm::open(&path, args.sync, args.fs9);
    let trunk = db.connect().unwrap();
    let run = Run {
        args,
        path,
        db,
        trunk,
        model,
        rng,
        snaps,
        tag,
        tick,
        opens: opens + 1,
        first_reap_after_open: true,
    };
    run.exec(&format!("PRAGMA synchronous = {}", run.args.synchronous));
    println!("OPEN n={} tick={} {}", run.opens, run.tick, arm::open_stats(&run.db));
    run
}

/// The checker must be able to fire. A snapshot forked before the trunk rewrites rows `1..=scan`
/// must read every old value, checked as `read` checks, and none of the new ones; then it is reaped
/// and the model takes the new values (a second write at tick 0).
fn self_check(run: &mut Run) {
    let n = run.args.scan;
    let branch = run.trunk.fork_branch().unwrap();
    let mut new = Vec::new();
    run.exec("BEGIN");
    for id in 1..=n {
        let seed = run.rng.next();
        run.exec(&format!("UPDATE t SET v = '{}' WHERE id = {id}", value(seed, 0)));
        new.push(seed);
    }
    run.exec("COMMIT");
    let conn = branch.connect().unwrap();
    let got = conn
        .prepare(format!("SELECT id, v FROM t WHERE id BETWEEN 1 AND {n} ORDER BY id"))
        .and_then(|mut st| st.run_collect_rows())
        .unwrap_or_else(|e| not_a_result(&format!("self-check scan: {e}")));
    drop(conn);
    let (mut old, mut fresh) = (0, 0);
    for (j, row) in got.iter().enumerate() {
        let id = 1 + j as i64;
        let (seed, rot) = run.model.at(id, 0).expect("the model holds every initial row");
        let text = match &row[1] {
            Value::Text(t) => t.as_str().to_string(),
            _ => String::new(),
        };
        old += (row[0].as_int() == Some(id) && text == value(seed, rot)) as i64;
        fresh += (text == value(new[j], 0)) as i64;
    }
    if got.len() as i64 != n || old != n || fresh != 0 {
        not_a_result(&format!(
            "self-check: a snapshot forked before a trunk rewrite of rows 1..={n} read {} rows, {old} \
             old values and {fresh} new ones",
            got.len()
        ));
    }
    if let Err(e) = branch.reap() {
        not_a_result(&format!("self-check reap failed: {e}"));
    }
    for (j, seed) in new.into_iter().enumerate() {
        run.model.put(1 + j as i64, 0, seed, 0, None);
    }
    println!(
        "# self-check: a snapshot forked before a trunk rewrite of rows 1..={n} read {old}/{n} old \
         values and {fresh} new ones, so the row check can fire"
    );
}

/// PREREG A8/A10: the mass-expiry fixture on the durable arm, counters only.
/// Phase 1: fork `r + K` snapshots, one per tick (the run gives `--k 1` and a rewrite period past the
/// run, so the ticks write little and pages keep old births). Phase 2: reap the OLDEST K at once,
/// oldest first (a TTL mass expiry). Phase 3: one whole-table rewrite, whose first writes look up
/// live children below pages born before or inside the cut range (A9.1's removal links). Then
/// `mass_checkpoints` catalog checkpoints, each after `mass_between` ticks and a rewrite, with two
/// model-checked reads after each. One MASS line per phase: catalog checkpoints, free rows
/// upserted, the last TRUNCATE row, child-index resolves and removal links followed (deltas).
fn mass_expiry(run: &mut Run) {
    let k = run.args.mass_expiry as usize;
    let before = arm::mass_counters(&run.db);
    let line = |phase: &str, b: &(u64, u64, Vec<i64>, u64, u64), a: &(u64, u64, Vec<i64>, u64, u64)| {
        let resolves = a.3 - b.3;
        println!(
            "MASS phase={phase} k={k} checkpoints={} free_puts={} truncate={:?} resolves={resolves} \
             link_hops={} hops_per_resolve={:.3}",
            a.0 - b.0,
            a.1 - b.1,
            a.2,
            a.4 - b.4,
            (a.4 - b.4) as f64 / resolves.max(1) as f64
        );
    };
    for _ in 0..run.args.r + k {
        run.tick += 1;
        run.trunk_tick();
        let branch = run.trunk.fork_branch().unwrap_or_else(|e| {
            not_a_result(&format!("fork at tick {} failed: {e}", run.tick))
        });
        run.snaps.push_back(Live {
            snap: Snap::Handle(branch),
            tick: run.tick,
        });
    }
    let fill = arm::mass_counters(&run.db);
    line("fill", &before, &fill);
    for _ in 0..k {
        let victim = run.snaps.pop_front().expect("r + K snapshots are live");
        let branch = match victim.snap {
            Snap::Handle(b) => b,
            Snap::Detached(id) => arm::attach(&run.db, id),
        };
        if let Err(e) = branch.reap() {
            not_a_result(&format!("mass reap of the snapshot of tick {} failed: {e}", victim.tick));
        }
    }
    let oldest = run.oldest().expect("r snapshots stay live");
    run.model.prune(oldest);
    let cut = arm::mass_counters(&run.db);
    line("cut", &fill, &cut);
    // A tick of its own: the snapshot forked at the last tick must not see the rewrite.
    run.tick += 1;
    run.trunk_tick();
    let (lo, hi) = (run.model.low(run.tick), run.model.high(run.tick));
    run.rewrite(lo, hi, Some(oldest));
    let first = arm::mass_counters(&run.db);
    line("rewrite_after_cut", &cut, &first);
    let mut last = first;
    for c in 1..=run.args.mass_checkpoints {
        arm::compact(&run.db);
        let now = arm::mass_counters(&run.db);
        line(format!("checkpoint{c}").as_str(), &last, &now);
        for _ in 0..2 {
            run.read();
        }
        for _ in 0..run.args.mass_between.max(1) {
            run.tick += 1;
            run.trunk_tick();
        }
        let (lo, hi) = (run.model.low(run.tick), run.model.high(run.tick));
        let oldest = run.oldest();
        run.rewrite(lo, hi, oldest);
        let after = arm::mass_counters(&run.db);
        line(format!("ticks_and_rewrite_after_checkpoint{c}").as_str(), &now, &after);
        last = after;
    }
    println!("# mass-expiry done: live={} {}", run.snaps.len(), arm::state(&run.db));
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |rd| {
        rd.filter_map(|e| e.ok())
            .filter_map(|e| e.metadata().ok())
            .filter(|m| m.is_file())
            .map(|m| m.len())
            .sum()
    })
}

fn main() {
    let args = parse_args();
    let tmp = tempfile::TempDir::new_in(args.dir.clone().unwrap_or_else(std::env::temp_dir)).unwrap();
    let path = tmp.path().join("lakehouse.db");
    let db = arm::open(&path, args.sync, args.fs9);
    let trunk = db.connect().unwrap();
    let mut run = Run {
        model: Model {
            rows: args.rows,
            k: args.k,
            hist: HashMap::new(),
            kept_from: 1,
        },
        rng: Rng(args.seed),
        args,
        path,
        db,
        trunk,
        snaps: VecDeque::new(),
        tag: None,
        tick: 0,
        opens: 1,
        first_reap_after_open: false,
    };
    run.exec(&format!("PRAGMA synchronous = {}", run.args.synchronous));
    run.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    run.exec("BEGIN");
    for id in 1..=run.args.rows {
        let seed = run.rng.next();
        run.exec(&format!("INSERT INTO t VALUES ({id}, '{}')", value(seed, 0)));
        run.model.put(id, 0, seed, 0, None);
    }
    run.exec("COMMIT");
    run.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    self_check(&mut run);
    let a = &run.args;
    println!("# branch_lakehouse — r12-lakehouse PREREG; arm {}", arm::NAME);
    println!(
        "# r={} ticks={} windows={} rows={} k={} rewrite_every={} readers={} scan={} seed={:#x} \
         fs9={} reopen_every={} sync={} synchronous={} (reads back {}) value_len={VALUE_LEN} page_size={} \
         pages={} build={}",
        a.r,
        a.ticks,
        a.windows,
        a.rows,
        a.k,
        a.rewrite_every,
        a.readers,
        a.scan,
        a.seed,
        a.fs9,
        a.reopen_every,
        a.sync,
        a.synchronous,
        run.int("PRAGMA synchronous"),
        run.int("PRAGMA page_size"),
        run.int("PRAGMA page_count"),
        if cfg!(debug_assertions) { "DEBUG" } else { "release" }
    );
    println!(
        "# tag={} clone_evict={} teardown={}",
        run.args.tag,
        arm::clone_evict(&run.db),
        if run.args.teardown { "full" } else { "none" }
    );
    println!("# counters only: nothing is timed");
    if run.args.mass_expiry > 0 {
        mass_expiry(&mut run);
        return;
    }
    let per_window = run.args.ticks / run.args.windows;
    let steady_from = run.args.windows / 2;
    // Steady-state (second half) per-reap samples, for the final summary.
    let (mut s_k1, mut s_k1_ord, mut s_k2) = (Dist::default(), Dist::default(), Dist::default());
    let (mut s_fills, mut s_hits) = (0u64, 0u64);
    let mut s_bursts = Dist::default();
    let mut w0 = arm::work(&run.db);
    for window in 0..run.args.windows {
        let (mut k1, mut k1_ord, mut k2, mut bursts) =
            (Dist::default(), Dist::default(), Dist::default(), Dist::default());
        let mut rows_checked = 0usize;
        let mut reads = 0u64;
        for _ in 0..per_window {
            run.tick += 1;
            run.trunk_tick();
            let branch = run.trunk.fork_branch().unwrap_or_else(|e| {
                not_a_result(&format!("fork at tick {} failed: {e}", run.tick))
            });
            let live = Live {
                snap: Snap::Handle(branch),
                tick: run.tick,
            };
            if run.args.tag && run.tag.is_none() {
                run.tag = Some(live);
            } else {
                run.snaps.push_back(live);
            }
            if run.snaps.len() > run.args.r {
                let before = arm::work(&run.db);
                let victim = run.snaps.pop_front().expect("more than r snapshots are live");
                let branch = match victim.snap {
                    Snap::Handle(b) => b,
                    Snap::Detached(id) => arm::attach(&run.db, id),
                };
                if let Err(e) = branch.reap() {
                    not_a_result(&format!("reap of the snapshot of tick {} failed: {e}", victim.tick));
                }
                let after = arm::work(&run.db);
                let visited = after.gc_range_entries - before.gc_range_entries;
                let cat_rows = after.cat_trunk_rows - before.cat_trunk_rows;
                // The reap that frees a rewrite: the snapshot forked just before it.
                let burst = (victim.tick + 1) % run.args.rewrite_every == 0;
                k1.add(visited);
                k2.add(cat_rows);
                if burst {
                    bursts.add(visited);
                } else {
                    k1_ord.add(visited);
                }
                if window >= steady_from {
                    s_k1.add(visited);
                    s_k2.add(cat_rows);
                    if burst {
                        s_bursts.add(visited);
                    } else {
                        s_k1_ord.add(visited);
                    }
                }
                if run.first_reap_after_open {
                    run.first_reap_after_open = false;
                    println!(
                        "FIRST_REAP_AFTER_OPEN open={} tick={} victim_tick={} k1={visited} \
                         cat_trunk_rows={cat_rows} cat_trunk_probes={} burst={burst}",
                        run.opens,
                        run.tick,
                        victim.tick,
                        after.cat_trunk_probes - before.cat_trunk_probes
                    );
                }
                let oldest = run.oldest().expect("r >= 1 snapshots stay live");
                run.model.prune(oldest);
            }
            // With `--tag` the first tick's only snapshot is the tag, which is never read.
            for _ in 0..run.args.readers {
                if run.snaps.is_empty() {
                    break;
                }
                rows_checked += run.read();
                reads += 1;
            }
            if run.args.reopen_every > 0 && run.tick % run.args.reopen_every == 0 {
                // A reopen restarts the store's counters: close the window's accounting first.
                let w = arm::work(&run.db);
                if window >= steady_from {
                    s_fills += w.retained_clone_fills - w0.retained_clone_fills;
                    s_hits += w.retained_shared_hits - w0.retained_shared_hits;
                }
                run = reopen(run);
                w0 = arm::work(&run.db);
            }
        }
        let w = arm::work(&run.db);
        let (fills, hits) = (
            w.retained_clone_fills - w0.retained_clone_fills,
            w.retained_shared_hits - w0.retained_shared_hits,
        );
        if window >= steady_from {
            s_fills += fills;
            s_hits += hits;
        }
        println!(
            "WINDOW w={window} tick={} live={} reads={reads} rows_checked={rows_checked} \
             k1[{}] k1_ordinary[{}] k1_bursts[{}] k2_cat_rows[{}] fills={fills} hits={hits} \
             fill_ratio={:.4} copies={} evictions={} interval_evictions={} clone_gc_entries={} \
             orphaned_clones={} overlaid={} gc_examined={} heap_live_bytes={} rss_bytes={} model_rows={} \
             files_bytes={} {}",
            run.tick,
            run.snaps.len(),
            k1.summary(),
            k1_ord.summary(),
            bursts.summary(),
            k2.summary(),
            fills as f64 / ((fills + hits).max(1)) as f64,
            w.retained_copies - w0.retained_copies,
            w.retained_clone_evictions - w0.retained_clone_evictions,
            w.retained_clone_interval_evictions - w0.retained_clone_interval_evictions,
            w.clone_gc_entries - w0.clone_gc_entries,
            arm::orphaned_clones(&run.db),
            w.chunks_overlaid - w0.chunks_overlaid,
            w.gc_examined - w0.gc_examined,
            heap_live_bytes(),
            rss_bytes(),
            run.model.hist.len(),
            dir_bytes(tmp.path()),
            arm::state(&run.db)
        );
        w0 = w;
    }
    println!(
        "STEADY windows={steady_from}..{} k1[{}] k1_ordinary[{}] k1_bursts[{}] k2_cat_rows[{}] \
         fills={s_fills} hits={s_hits} fill_ratio={:.4}",
        run.args.windows - 1,
        s_k1.summary(),
        s_k1_ord.summary(),
        s_bursts.summary(),
        s_k2.summary(),
        s_fills as f64 / ((s_fills + s_hits).max(1)) as f64
    );
    if !run.args.teardown {
        println!(
            "# teardown: skipped (--teardown none); {} snapshots live at the end: {}",
            run.snaps.len(),
            arm::state(&run.db)
        );
        return;
    }
    // Teardown: every snapshot reaped, oldest first; the store must end empty.
    while let Some(l) = run.snaps.pop_front() {
        let branch = match l.snap {
            Snap::Handle(b) => b,
            Snap::Detached(id) => arm::attach(&run.db, id),
        };
        if let Err(e) = branch.reap() {
            not_a_result(&format!("teardown reap of tick {} failed: {e}", l.tick));
        }
    }
    if let Some(l) = run.tag.take() {
        let branch = match l.snap {
            Snap::Handle(b) => b,
            Snap::Detached(id) => arm::attach(&run.db, id),
        };
        if let Err(e) = branch.reap() {
            not_a_result(&format!("teardown reap of the tag (tick {}) failed: {e}", l.tick));
        }
    }
    if let Some(leak) = arm::leaked(&run.db) {
        not_a_result(&format!("teardown leaked: {leak}"));
    }
    println!("# teardown: every snapshot reaped, the store is empty: {}", arm::state(&run.db));
}

/// The store counters the harness reads, whatever the arm (0 where the arm has none).
pub struct Work {
    gc_range_entries: u64,
    gc_examined: u64,
    retained_clone_fills: u64,
    retained_shared_hits: u64,
    retained_copies: u64,
    retained_clone_evictions: u64,
    retained_clone_interval_evictions: u64,
    clone_gc_entries: u64,
    chunks_overlaid: u64,
    cat_trunk_probes: u64,
    cat_trunk_rows: u64,
}

// ARM-ADAPTER-BEGIN
/// The durable arm D: a12 durable-open (F2 + C-P + C-R), catalog mode.
mod arm {
    use super::*;
    use turso_core::branch::{BranchDurability, SyncClass};
    use turso_core::{DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

    pub const NAME: &str = "D (a12 durable-open: F2 + C-P + C-R, catalog mode)";
    pub const REOPENS: bool = true;

    pub fn open(path: &Path, sync: bool, _fs9: bool) -> Arc<Database> {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            // SyncClass replaced the sync bool on main (bcddba7ff): true -> Fsync, false -> Off.
            DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog {
                sync: if sync { SyncClass::Fsync } else { SyncClass::Off },
            }),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")))
    }

    pub fn detach(b: Branch) -> BranchId {
        b.into_id()
    }

    pub fn attach(db: &Arc<Database>, id: BranchId) -> Branch {
        db.branch(id)
            .unwrap_or_else(|e| not_a_result(&format!("re-attaching branch {} failed: {e}", id.0)))
    }

    pub fn open_stats(db: &Arc<Database>) -> String {
        let s = db.branch_open_stats();
        format!(
            "total_ns={} records={} log_bytes={} snap_bytes={} branches={} trunk_children={} \
             trunk_retained={} branch_loads={} trunk_page_loads={} cat_queries={} cat_rows_read={} \
             trunk_probes={} trunk_rows={} parked_records={} parked_applied={}",
            s.total_ns,
            s.records,
            s.log_bytes,
            s.snap_bytes,
            s.branches,
            s.trunk_children,
            s.trunk_retained,
            s.branch_loads,
            s.trunk_page_loads,
            s.cat_queries,
            s.cat_rows_read,
            s.trunk_probes,
            s.trunk_rows,
            s.parked_records,
            s.parked_applied
        )
    }

    pub fn work(db: &Arc<Database>) -> Work {
        let w = db
            .branch_stats()
            .unwrap_or_else(|e| not_a_result(&format!("branch_stats: {e}")))
            .work;
        let (probes, rows) = db.branch_catalog_trunk_counters();
        Work {
            gc_range_entries: w.gc_range_entries,
            gc_examined: w.gc_examined,
            retained_clone_fills: 0,
            retained_shared_hits: 0,
            retained_copies: 0,
            retained_clone_evictions: 0,
            retained_clone_interval_evictions: 0,
            clone_gc_entries: 0,
            chunks_overlaid: 0,
            cat_trunk_probes: probes,
            cat_trunk_rows: rows,
        }
    }

    pub fn state(db: &Arc<Database>) -> String {
        let s = db
            .branch_stats()
            .unwrap_or_else(|e| not_a_result(&format!("branch_stats: {e}")));
        let (loads, page_loads, queries, rows_read) = db.branch_catalog_counters();
        format!(
            "live_branches={} arena_in_use={} arena_free={} trunk_retained={} branch_loads={loads} \
             trunk_page_loads={page_loads} cat_queries={queries} cat_rows_read={rows_read} \
             cat_rows_written={}",
            s.live_branches,
            s.arena_slots_in_use,
            s.arena_slots_free,
            db.branch_trunk_retained(),
            db.branch_catalog_rows_written()
        )
    }

    pub fn orphaned_clones(_db: &Arc<Database>) -> usize {
        0
    }

    pub fn mass_counters(db: &Arc<Database>) -> (u64, u64, Vec<i64>, u64, u64) {
        db.branch_mass_expiry_counters()
    }

    pub fn compact(db: &Arc<Database>) {
        db.branch_compact_now()
            .unwrap_or_else(|e| not_a_result(&format!("branch_compact_now failed: {e}")));
    }

    pub fn clone_evict(_db: &Arc<Database>) -> &'static str {
        "none (no FS9)"
    }

    pub fn leaked(db: &Arc<Database>) -> Option<String> {
        let s = db
            .branch_stats()
            .unwrap_or_else(|e| not_a_result(&format!("branch_stats: {e}")));
        (s.live_branches != 0 || s.arena_slots_in_use != 0 || db.branch_trunk_retained() != 0)
            .then(|| format!("{s:?} trunk_retained={}", db.branch_trunk_retained()))
    }
}
// ARM-ADAPTER-END
