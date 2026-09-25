//! `--threads T`: the bb and cat shapes on T threads (lane r11-bushy-conc, PREREG §2d).
//!
//!   bb   ROOT parallelization (Chaslot, Winands & van den Herik, "Parallel Monte-Carlo Tree
//!        Search", CG 2008): thread k runs BranchBench's step loop on its own tree under the shared
//!        trunk, with its own trunk connection and its own stream (seed XOR k * SEED_MIX, so thread
//!        0 runs the sequential stream), to x_k = floor(x / T) + [k < x mod T] nodes at checkpoint x.
//!   cat  TREE parallelization with a level barrier (same paper): the main thread draws each level's
//!        choice and the non-chosen children's prune draws in exactly the sequential order, and
//!        assigns ids, generations and fork epochs in that order; children are dealt round-robin to
//!        T workers, which fork from the SHARED current node, write, read (checked), disconnect and
//!        keep or prune. The tree, and every value read, equal the sequential run's.
//!
//! Nothing here calls the store inside a step: the checks that need `branch_stats()` run at the
//! checkpoints, where every worker is parked on a barrier. With `--timing`, every op of every step
//! is timed (not a sample), and each checkpoint prints the interval's wall and process CPU time.

use std::sync::{Barrier, Mutex, RwLock};

use turso_core::branch::{LockCounts, LOCK_SITES};

use super::*;

/// Mixes the thread index into bb's seed; thread 0 keeps the sequential seed.
const SEED_MIX: u64 = 0xD1B5_4A32_D192_ED03;

const OPS: [&str; 7] = ["fork", "open", "write", "read_own", "read_other", "close", "reap"];

/// The harness's own counts, summed over threads.
#[derive(Default, Clone, Copy)]
struct Harness {
    handles: u64,
    states: u64,
    reaps: u64,
    max_cascade: u64,
    cascades: u64,
    reads_checked: u64,
    nodes: u64,
    max_depth: u32,
    fork_busy: u64,
    steps: u64,
}

impl Harness {
    fn add(&mut self, o: &Harness) {
        self.handles += o.handles;
        self.states += o.states;
        self.reaps += o.reaps;
        self.max_cascade = self.max_cascade.max(o.max_cascade);
        self.cascades += o.cascades;
        self.reads_checked += o.reads_checked;
        self.nodes += o.nodes;
        self.max_depth = self.max_depth.max(o.max_depth);
        self.fork_busy += o.fork_busy;
        self.steps += o.steps;
    }
}

fn harness_of(b: &Bench) -> Harness {
    Harness {
        handles: b.handles,
        states: b.states,
        reaps: b.reaps,
        max_cascade: b.max_cascade,
        cascades: b.cascades,
        reads_checked: b.reads_checked,
        nodes: (b.nodes.len() - 1) as u64,
        max_depth: b.nodes.iter().skip(1).map(|n| n.depth).max().unwrap_or(0),
        fork_busy: b.fork_busy,
        steps: 0,
    }
}

fn new_times() -> OpTimes {
    Default::default()
}

fn drain_times(from: &mut OpTimes, into: &mut OpTimes) {
    for (a, b) in from.iter_mut().zip(into.iter_mut()) {
        b.append(a);
    }
}

/// Process CPU time (user + system), all threads.
fn cpu_ns() -> u64 {
    // SAFETY: getrusage fills the struct it is given; a zeroed rusage is a valid value.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// The box's parallelism at this moment, with nothing shared: `t` threads each run the same fixed
/// xorshift loop; operations per second over the slowest thread's finish. (branch_arms'
/// `null_ops_per_s`, r11-coherence.) A load control for the timed runs, printed before and after.
fn null_ops_per_s(t: usize) -> f64 {
    const ITER: u64 = 20_000_000;
    let barrier = Barrier::new(t + 1);
    let wall = std::thread::scope(|s| {
        let handles: Vec<_> = (0..t)
            .map(|i| {
                let barrier = &barrier;
                s.spawn(move || {
                    let mut r = Rng(0x2545_F491_4F6C_DD1D ^ (i as u64 + 1));
                    barrier.wait();
                    let mut acc = 0u64;
                    for _ in 0..ITER {
                        acc = acc.wrapping_add(r.next());
                    }
                    std::hint::black_box(acc)
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    });
    (t as u64 * ITER) as f64 / wall.as_secs_f64()
}

/// One checkpoint interval's clocks and counters at its start.
struct Interval {
    start: Instant,
    cpu: u64,
    lock: LockCounts,
}

impl Interval {
    fn start(db: &Database) -> Self {
        Self {
            lock: db.branch_stats().work.lock,
            start: Instant::now(),
            cpu: cpu_ns(),
        }
    }

    /// Print the interval (`--timing` only): its wall and CPU time, the lock's waits and holds and
    /// its per-site counts over the interval, and every op's latency percentiles.
    fn print(&self, db: &Database, args: &Args, x: u64, steps: u64, times: &mut OpTimes) {
        let wall = self.start.elapsed().as_nanos() as u64;
        let cpu = cpu_ns() - self.cpu;
        if !args.timing {
            return;
        }
        let lock = db.branch_stats().work.lock;
        let acq: u64 = (0..LOCK_SITES.len()).map(|i| lock.acquisitions[i] - self.lock.acquisitions[i]).sum();
        let cont: u64 = (0..LOCK_SITES.len()).map(|i| lock.contended[i] - self.lock.contended[i]).sum();
        println!(
            "# interval x={x} steps={steps} wall_ns={wall} cpu_ns={cpu} lock_acq={acq} lock_contended={cont} \
             lock_wait_ns={} lock_hold_ns={}",
            lock.wait_ns - self.lock.wait_ns,
            lock.hold_ns - self.lock.hold_ns,
        );
        for (op, samples) in OPS.iter().zip(times.iter_mut()) {
            if samples.is_empty() {
                continue;
            }
            samples.sort_unstable();
            let pct = |p: f64| samples[((p / 100.0) * (samples.len() - 1) as f64).round() as usize] as f64 / 1e3;
            println!(
                "{x}\t{op}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
                samples.len(),
                pct(50.0),
                pct(90.0),
                pct(99.0),
                *samples.last().unwrap() as f64 / 1e3
            );
            samples.clear();
        }
    }
}

fn sites(counts: &[u64], base: &[u64]) -> String {
    LOCK_SITES
        .iter()
        .enumerate()
        .filter(|&(i, _)| counts[i] != base[i])
        .map(|(i, name)| format!("{name}:{}", counts[i] - base[i]))
        .collect::<Vec<_>>()
        .join(",")
}

/// The checkpoint line: the sequential harness's fields, in its order, then the threaded mode's.
/// Checks the engine against the harness (NOT A RESULT on a mismatch).
#[allow(clippy::too_many_arguments)]
fn checkpoint(
    db: &Database,
    args: &Args,
    t: usize,
    x: u64,
    level: Option<u64>,
    w0: &BranchWork,
    m0: &MapWork,
    h: &Harness,
    extra: &str,
) {
    let s = db.branch_stats();
    if s.live_branches as u64 != h.states {
        not_a_result(&format!("engine has {} branch states, the harness expects {}", s.live_branches, h.states));
    }
    let zombies = db.branch_zombies() as u64;
    if zombies != h.states - h.handles {
        not_a_result(&format!("engine has {zombies} zombies, the harness expects {}", h.states - h.handles));
    }
    let w = s.work;
    if w.states_freed - w0.states_freed != h.reaps {
        not_a_result(&format!(
            "the engine freed {} states over {} reaps; reference-counted maps free exactly one per reap",
            w.states_freed - w0.states_freed,
            h.reaps
        ));
    }
    let needed = if args.needed {
        let needed = db.branch_needed_slots();
        if needed != s.arena_slots_in_use {
            not_a_result(&format!(
                "arena holds {} slots but live branches can read {needed}: waste under threads",
                s.arena_slots_in_use
            ));
        }
        needed.to_string()
    } else {
        "skipped".to_string()
    };
    let resolves = w.resolve_calls - w0.resolve_calls;
    let per = |v: u64| if resolves == 0 { 0.0 } else { v as f64 / resolves as f64 };
    let (lock, lock0) = (w.lock, w0.lock);
    let total = |a: &[u64], b: &[u64]| -> u64 { a.iter().zip(b).map(|(a, b)| a - b).sum() };
    let m = s.map_work;
    println!(
        "# ckpt x={x} level={} handles={} states={} zombies={zombies} arena_in_use={} \
         arena_high_water={} needed={needed} page_map_nodes={} resolves={resolves} \
         levels_per_resolve={:.4} examined_per_resolve={:.4} reaps={} states_freed={} \
         max_cascade={} cascades={} gc_range={} gc_examined={} nodes={} max_depth={} \
         reads_checked={} rss_bytes={} first_writes={} in_place={} nodes_copied={} nodes_built={} \
         refs_touched={} nodes_released={} refs_released={}{extra} threads={t} steps={} fork_busy={} \
         arcs_cloned={} arcs_dropped_shared={} shared_probes={} lock_acq_total={} lock_contended_total={} \
         lock_acq={} lock_contended={}",
        level.map_or("-".to_string(), |l| l.to_string()),
        h.handles,
        h.states,
        s.arena_slots_in_use,
        s.arena_slots_in_use + s.arena_slots_free,
        s.page_map_nodes,
        per(w.resolve_levels - w0.resolve_levels),
        per(w.resolve_retained_examined - w0.resolve_retained_examined),
        h.reaps,
        w.states_freed - w0.states_freed,
        h.max_cascade,
        h.cascades,
        w.gc_range_entries - w0.gc_range_entries,
        w.gc_examined - w0.gc_examined,
        h.nodes,
        h.max_depth,
        h.reads_checked,
        rss_bytes(),
        w.branch_first_writes - w0.branch_first_writes,
        w.in_place_writes - w0.in_place_writes,
        m.nodes_copied - m0.nodes_copied,
        m.nodes_built - m0.nodes_built,
        m.refs_touched - m0.refs_touched,
        m.nodes_released - m0.nodes_released,
        m.refs_released - m0.refs_released,
        h.steps,
        h.fork_busy,
        m.arcs_cloned - m0.arcs_cloned,
        m.arcs_dropped_shared - m0.arcs_dropped_shared,
        m.shared_probes - m0.shared_probes,
        total(&lock.acquisitions, &lock0.acquisitions),
        total(&lock.contended, &lock0.contended),
        sites(&lock.acquisitions, &lock0.acquisitions),
        sites(&lock.contended, &lock0.contended),
    );
}

/// The store must be empty once every handle is gone.
fn check_empty(db: &Database, h: &Harness) {
    let s = db.branch_stats();
    if s.live_branches != 0 || s.arena_slots_in_use != 0 || h.states != 0 {
        not_a_result(&format!("teardown leaked: {s:?}, harness states {}", h.states));
    }
    println!(
        "# teardown: every branch freed, arena empty ({} free slots), reaps={} max_cascade={} \
         cascades={} reads_checked={}",
        s.arena_slots_free, h.reaps, h.max_cascade, h.cascades, h.reads_checked
    );
}

pub(super) fn run(args: &Args, db: Arc<Database>, trunk: Arc<Connection>, t: usize) {
    let null_before = args.timing.then(|| null_ops_per_s(t));
    match args.shape {
        Shape::Bb => bb(args, &db, t),
        Shape::Cat => cat(args, &db, trunk, t),
        Shape::Beam => die("--threads runs bb and cat"),
    }
    if let Some(before) = null_before {
        println!("# null_ops_per_s threads={t} before={before:.0} after={:.0}", null_ops_per_s(t));
    }
}

/// What a bb worker hands the main thread at a checkpoint.
struct BbReport {
    h: Harness,
    eligible: usize,
    times: OpTimes,
}

fn bb(args: &Args, db: &Arc<Database>, t: usize) {
    let barrier = Barrier::new(t + 1);
    let slots: Vec<Mutex<Option<BbReport>>> = (0..t).map(|_| Mutex::new(None)).collect();
    let s0 = db.branch_stats();
    let (w0, m0) = (s0.work, s0.map_work);
    std::thread::scope(|s| {
        for (k, slot) in slots.iter().enumerate() {
            let (barrier, db) = (&barrier, db.clone());
            s.spawn(move || bb_worker(k, t, db, args, barrier, slot));
        }
        barrier.wait();
        let mut iv = Interval::start(db);
        for &x in &args.checkpoints {
            barrier.wait();
            let mut h = Harness::default();
            let mut eligible = 0;
            let mut times = new_times();
            for slot in &slots {
                let mut r = slot.lock().unwrap().take().expect("a worker reports at every checkpoint");
                h.add(&r.h);
                eligible += r.eligible;
                drain_times(&mut r.times, &mut times);
            }
            iv.print(db, args, x, h.steps, &mut times);
            checkpoint(db, args, t, x, None, &w0, &m0, &h, &format!(" eligible={eligible}"));
            iv = Interval::start(db);
            barrier.wait();
        }
        // Teardown: every worker drops its own handles, oldest first, all at once.
        barrier.wait();
        let mut h = Harness::default();
        let mut times = new_times();
        for slot in &slots {
            let mut r = slot.lock().unwrap().take().expect("a worker reports its teardown");
            h.add(&r.h);
            drain_times(&mut r.times, &mut times);
        }
        println!("# teardown_threads");
        iv.print(db, args, 0, h.steps, &mut times);
        check_empty(db, &h);
    });
}

fn bb_worker(k: usize, t: usize, db: Arc<Database>, args: &Args, barrier: &Barrier, slot: &Mutex<Option<BbReport>>) {
    let trunk = db.connect().unwrap();
    let seed = args.seed ^ (k as u64).wrapping_mul(SEED_MIX);
    let mut b = Bench::new(db, trunk, args, seed);
    b.per_reap_check = false;
    b.times = args.timing.then(new_times);
    let mut walk = BbWalk::new();
    let mut steps_before = 0;
    let report = |b: &mut Bench, eligible: usize, steps_before: &mut u64| {
        let mut h = harness_of(b);
        h.steps = h.nodes - *steps_before;
        *steps_before = h.nodes;
        let mut times = new_times();
        if let Some(own) = b.times.as_mut() {
            drain_times(own, &mut times);
        }
        BbReport { h, eligible, times }
    };
    barrier.wait();
    for &x in &args.checkpoints {
        let xk = x / t as u64 + u64::from((k as u64) < x % t as u64);
        walk.advance(&mut b, args, xk);
        *slot.lock().unwrap() = Some(report(&mut b, walk.eligible.len(), &mut steps_before));
        barrier.wait();
        barrier.wait();
    }
    let ids: Vec<u32> = (1..b.nodes.len() as u32).filter(|&i| b.nodes[i as usize].branch.is_some()).collect();
    let released = ids.len() as u64;
    for id in ids {
        let branch = b.nodes[id as usize].branch.take().unwrap();
        b.release(id, branch);
    }
    let mut r = report(&mut b, 0, &mut steps_before);
    r.h.steps = released;
    *slot.lock().unwrap() = Some(r);
    barrier.wait();
}

/// The cat level the workers run, or the teardown they share.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Level,
    Teardown,
    Done,
}

/// What the workers read during a level. The main thread writes it only between levels, while
/// every worker is parked on the barrier.
struct CatShared {
    nodes: Vec<Node>,
    rows: Rows,
    x_node: u32,
    /// The level's children in sequential order: (id, kept).
    plan: Vec<(u32, bool)>,
    phase: Phase,
}

/// What a cat worker hands back: after each level the children it kept and pruned, at each
/// checkpoint its latencies; for the teardown, the handles it is dealt.
#[derive(Default)]
struct CatOut {
    kept: Vec<(u32, Branch)>,
    pruned: Vec<u32>,
    reads_checked: u64,
    times: OpTimes,
    teardown: Vec<(u32, Branch)>,
}

fn lap(times: &mut OpTimes, op: usize, since: Option<Instant>) {
    if let Some(since) = since {
        times[op].push(ns(since.elapsed()));
    }
}

fn cat_worker(k: usize, t: usize, timing: bool, shared: &RwLock<CatShared>, barrier: &Barrier, out: &Mutex<CatOut>) {
    let clock = || timing.then(Instant::now);
    let mut times = new_times();
    loop {
        barrier.wait();
        let sh = shared.read().unwrap();
        let mut kept = Vec::new();
        let mut pruned = Vec::new();
        let mut reads = 0;
        match sh.phase {
            Phase::Done => break,
            Phase::Level => {
                let parent = sh.nodes[sh.x_node as usize].branch.as_ref().expect("the level's node is live");
                for i in (k..sh.plan.len()).step_by(t) {
                    let (id, keep) = sh.plan[i];
                    let n = &sh.nodes[id as usize];
                    let a = clock();
                    let branch = parent.fork().unwrap();
                    lap(&mut times, OP_FORK, a);
                    let a = clock();
                    let conn = branch.connect().unwrap();
                    lap(&mut times, OP_OPEN, a);
                    let a = clock();
                    update_to(&conn, n.row, &node_value(id, n.writes[0].1));
                    lap(&mut times, OP_WRITE, a);
                    for (j, row) in read_rows_in(&sh.nodes, sh.rows, id).into_iter().enumerate() {
                        let a = clock();
                        let got = read_v(&conn, row);
                        if j < 2 {
                            lap(&mut times, OP_READ_OWN + j, a);
                        }
                        let expect = expect_in(&sh.nodes, sh.rows, id, row);
                        if got != expect {
                            not_a_result(&format!("node {id} read row {row} as {got}, expected {expect}"));
                        }
                        reads += 1;
                    }
                    let a = clock();
                    drop(conn);
                    lap(&mut times, OP_CLOSE, a);
                    if keep {
                        kept.push((id, branch));
                    } else {
                        let a = clock();
                        let reaped = branch.reap().unwrap();
                        lap(&mut times, OP_REAP, a);
                        if reaped.deferred {
                            not_a_result(&format!("pruning node {id} was deferred with no connection open"));
                        }
                        pruned.push(id);
                    }
                }
            }
            Phase::Teardown => {
                let dealt = std::mem::take(&mut out.lock().unwrap().teardown);
                for (id, branch) in dealt {
                    let a = clock();
                    let reaped = branch.reap().unwrap();
                    lap(&mut times, OP_REAP, a);
                    if reaped.deferred {
                        not_a_result(&format!("tearing down node {id} was deferred"));
                    }
                    pruned.push(id);
                }
            }
        }
        drop(sh);
        let mut o = out.lock().unwrap();
        o.kept.append(&mut kept);
        o.pruned.append(&mut pruned);
        o.reads_checked += reads;
        drain_times(&mut times, &mut o.times);
        drop(o);
        barrier.wait();
    }
}

fn cat(args: &Args, db: &Arc<Database>, trunk: Arc<Connection>, t: usize) {
    let mut b = Bench::new(db.clone(), trunk, args, args.seed);
    b.per_reap_check = false;
    b.times = args.timing.then(new_times);
    let s0 = db.branch_stats();
    let (w0, m0) = (s0.work, s0.map_work);
    let barrier = Barrier::new(t + 1);
    let outs: Vec<Mutex<CatOut>> = (0..t).map(|_| Mutex::new(CatOut::default())).collect();
    // The root, on the main thread, exactly as the sequential shape makes it.
    let (x_node, branch) = b.fork(TRUNK);
    b.step_child(x_node, &branch);
    b.keep(x_node, branch);
    let shared = RwLock::new(CatShared {
        nodes: std::mem::take(&mut b.nodes),
        rows: args.rows,
        x_node,
        plan: Vec::new(),
        phase: Phase::Level,
    });
    let fanout = args.fanout;
    std::thread::scope(|s| {
        for (k, out) in outs.iter().enumerate() {
            let (shared, barrier) = (&shared, &barrier);
            let timing = args.timing;
            s.spawn(move || cat_worker(k, t, timing, shared, barrier, out));
        }
        let mut iv = Interval::start(db);
        let mut steps = 1u64;
        let mut level = 0u64;
        for &target in &args.checkpoints {
            while level < target {
                let choice = b.rng.below(fanout as u64) as usize;
                {
                    let mut sh = shared.write().unwrap();
                    let keep: Vec<bool> =
                        (0..fanout as usize).map(|i| i == choice || !b.draw_prune(args.gamma_milli)).collect();
                    let x = sh.x_node as usize;
                    let (x_epoch, x_depth) = (sh.nodes[x].epoch, sh.nodes[x].depth);
                    let base = sh.nodes.len() as u32;
                    let mut plan = Vec::with_capacity(fanout as usize);
                    for i in 0..fanout {
                        let id = base + i;
                        let row = match args.rows {
                            Rows::Hot => HOT_ROW,
                            Rows::Spread => row_for(id),
                        };
                        sh.nodes.push(Node {
                            parent: x as u32,
                            fork_epoch: x_epoch + i,
                            depth: x_depth + 1,
                            row,
                            epoch: 0,
                            kept_children: 0,
                            child_states: 0,
                            writes: vec![(0, b.generation + i as u64)],
                            branch: None,
                            state: true,
                        });
                        plan.push((id, keep[i as usize]));
                    }
                    let xn = &mut sh.nodes[x];
                    xn.epoch += fanout;
                    xn.kept_children += fanout;
                    xn.child_states += fanout;
                    b.generation += fanout as u64;
                    b.handles += fanout as u64;
                    b.states += fanout as u64;
                    sh.plan = plan;
                }
                barrier.wait();
                barrier.wait();
                let mut sh = shared.write().unwrap();
                for out in &outs {
                    let mut o = out.lock().unwrap();
                    for (id, branch) in o.kept.drain(..) {
                        sh.nodes[id as usize].branch = Some(branch);
                    }
                    for id in o.pruned.drain(..) {
                        let parent = sh.nodes[id as usize].parent as usize;
                        sh.nodes[parent].kept_children -= 1;
                        sh.nodes[id as usize].state = false;
                        b.handles -= 1;
                        b.states -= 1;
                        b.reaps += 1;
                        b.max_cascade = b.max_cascade.max(1);
                    }
                    b.reads_checked += std::mem::take(&mut o.reads_checked);
                }
                steps += u64::from(fanout);
                let prev = sh.x_node;
                sh.x_node = sh.plan[choice].0;
                if args.interior == Interior::Release {
                    let branch = sh.nodes[prev as usize].branch.take().expect("released twice");
                    let a = b.clock();
                    let reaped = branch.reap().unwrap();
                    b.lap(OP_REAP, a);
                    if reaped.deferred {
                        not_a_result(&format!("releasing node {prev} was deferred"));
                    }
                    sh.nodes[prev as usize].state = false;
                    b.handles -= 1;
                    b.states -= 1;
                    b.reaps += 1;
                    b.max_cascade = b.max_cascade.max(1);
                }
                level += 1;
            }
            let sh = shared.read().unwrap();
            let mut times = new_times();
            if let Some(own) = b.times.as_mut() {
                drain_times(own, &mut times);
            }
            for out in &outs {
                drain_times(&mut out.lock().unwrap().times, &mut times);
            }
            let h = Harness {
                handles: b.handles,
                states: b.states,
                reaps: b.reaps,
                max_cascade: b.max_cascade,
                cascades: b.cascades,
                reads_checked: b.reads_checked,
                nodes: (sh.nodes.len() - 1) as u64,
                max_depth: sh.nodes.iter().skip(1).map(|n| n.depth).max().unwrap_or(0),
                fork_busy: b.fork_busy,
                steps,
            };
            let x = h.nodes;
            iv.print(db, args, x, steps, &mut times);
            checkpoint(db, args, t, x, Some(level), &w0, &m0, &h, "");
            drop(sh);
            steps = 0;
            iv = Interval::start(db);
        }
        // Teardown: every remaining handle, oldest first, dealt round-robin to the workers.
        {
            let mut sh = shared.write().unwrap();
            let ids: Vec<u32> =
                (1..sh.nodes.len() as u32).filter(|&i| sh.nodes[i as usize].branch.is_some()).collect();
            for (j, id) in ids.into_iter().enumerate() {
                let branch = sh.nodes[id as usize].branch.take().unwrap();
                outs[j % t].lock().unwrap().teardown.push((id, branch));
            }
            sh.phase = Phase::Teardown;
        }
        barrier.wait();
        barrier.wait();
        let mut released = 0;
        {
            let mut sh = shared.write().unwrap();
            for out in &outs {
                for id in out.lock().unwrap().pruned.drain(..) {
                    sh.nodes[id as usize].state = false;
                    b.handles -= 1;
                    b.states -= 1;
                    b.reaps += 1;
                    b.max_cascade = b.max_cascade.max(1);
                    released += 1;
                }
            }
            sh.phase = Phase::Done;
        }
        barrier.wait();
        let mut times = new_times();
        for out in &outs {
            drain_times(&mut out.lock().unwrap().times, &mut times);
        }
        println!("# teardown_threads");
        iv.print(db, args, 0, released, &mut times);
        let h = Harness {
            handles: b.handles,
            states: b.states,
            reaps: b.reaps,
            max_cascade: b.max_cascade,
            cascades: b.cascades,
            reads_checked: b.reads_checked,
            ..Harness::default()
        };
        check_empty(db, &h);
    });
}
