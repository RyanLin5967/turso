//! Worst-case inputs for the branch store's data structures (r11-adversarial).
//!
//! Pre-registration: `artie-research/frontier/round11/r11-adversarial/PREREG.md`, committed before
//! this file was built. Each arm builds the input that maximises one structure's work, through the
//! store's own entry points (`turso_core::branch::bench::StoreBench`), and prints per-op deltas of
//! the store's observation counters. With `--time` it also prints each op's wall and thread-CPU
//! time; without it the output is integer counters only, which load cannot move.
//!
//!   cargo run -p turso_core --release --example branch_adv -- --arm binrev --levels 7,10,13
//!
//! Arms: grow, churn, binrev, fifo, arena, bigwrite, view, pathcopy, chain (see the PREREG).
//! Every arm checks the store's own accounting against what its input implies and prints
//! `NOT A RESULT` and exits non-zero on a mismatch.

use std::time::Instant;

use turso_core::branch::bench::StoreBench;
use turso_core::branch::{BranchId, BranchStats, BranchWork};

struct Args {
    arm: String,
    n: usize,
    list: Vec<usize>,
    cycles: usize,
    seed: u64,
    page_size: usize,
    time: bool,
    stall_us: f64,
}

fn die(msg: &str) -> ! {
    eprintln!("NOT A RESULT: {msg}");
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut a = Args {
        arm: String::new(),
        n: 0,
        list: Vec::new(),
        cycles: 0,
        seed: 0x9E37_79B9_7F4A_7C15,
        page_size: 64,
        time: false,
        stall_us: 100.0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--arm" => a.arm = val(),
            "--n" => a.n = val().parse().unwrap_or_else(|_| die("bad --n")),
            "--list" => {
                a.list = val()
                    .split(',')
                    .map(|s| s.parse().unwrap_or_else(|_| die("bad --list")))
                    .collect()
            }
            "--cycles" => a.cycles = val().parse().unwrap_or_else(|_| die("bad --cycles")),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--page-size" => a.page_size = val().parse().unwrap_or_else(|_| die("bad --page-size")),
            "--stall-us" => a.stall_us = val().parse().unwrap_or_else(|_| die("bad --stall-us")),
            "--time" => a.time = true,
            _ => die(&format!("unknown flag {flag}")),
        }
    }
    if a.arm.is_empty() {
        die("--arm is required");
    }
    a
}

const NF: usize = 18;

const FIELDS: [&str; NF] = [
    "resolve_calls",
    "resolve_levels",
    "resolve_retained_examined",
    "gc_examined",
    "gc_range_entries",
    "branch_table_moved",
    "page_table_moved",
    "arena_free_moved",
    "arena_bits_moved",
    "arena_chunks_moved",
    "map_nodes_copied",
    "view_inserts",
    "gc_heap_examined",
    "gc_meld_steps",
    "branch_table_resizes",
    "branch_table_rehashes",
    "arena_frames_copied",
    "arena_chunks_freed",
];

fn fields(w: &BranchWork) -> [u64; NF] {
    [
        w.resolve_calls,
        w.resolve_levels,
        w.resolve_retained_examined,
        w.gc_examined,
        w.gc_range_entries,
        w.branch_table_moved,
        w.page_table_moved,
        w.arena_free_moved,
        w.arena_bits_moved,
        w.arena_chunks_moved,
        w.map_nodes_copied,
        w.view_inserts,
        w.gc_heap_examined,
        w.gc_meld_steps,
        w.branch_table_resizes,
        w.branch_table_rehashes,
        w.arena_frames_copied,
        w.arena_chunks_freed,
    ]
}

fn idx(name: &str) -> usize {
    FIELDS.iter().position(|f| *f == name).expect("known field")
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer for the duration of the call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        die("clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// One measured op: counter deltas, and (with `--time`) wall and thread-CPU microseconds.
struct Op<T> {
    out: T,
    d: [u64; NF],
    wall_us: f64,
    cpu_us: f64,
}

struct Meter<'a> {
    s: &'a StoreBench,
    time: bool,
}

impl Meter<'_> {
    fn op<T>(&self, f: impl FnOnce() -> T) -> Op<T> {
        let before = fields(&self.s.stats().work);
        let (out, wall_us, cpu_us) = if self.time {
            let c0 = thread_cpu_ns();
            let t0 = Instant::now();
            let out = f();
            let wall = t0.elapsed().as_nanos() as f64 / 1e3;
            let cpu = (thread_cpu_ns() - c0) as f64 / 1e3;
            (out, wall, cpu)
        } else {
            (f(), 0.0, 0.0)
        };
        let after = fields(&self.s.stats().work);
        let mut d = [0u64; NF];
        for i in 0..NF {
            d[i] = after[i] - before[i];
        }
        Op {
            out,
            d,
            wall_us,
            cpu_us,
        }
    }
}

/// Running per-op summary of one kind of op.
#[derive(Default)]
struct Summary {
    ops: u64,
    sum: [u64; NF],
    max: [u64; NF],
    cpu: Vec<f32>,
    wall_max: f64,
    cpu_max: f64,
    stalls: u64,
}

impl Summary {
    fn add<T>(&mut self, op: &Op<T>, time: bool, stall_us: f64) -> bool {
        self.ops += 1;
        for i in 0..NF {
            self.sum[i] += op.d[i];
            self.max[i] = self.max[i].max(op.d[i]);
        }
        if time {
            self.cpu.push(op.cpu_us as f32);
            self.wall_max = self.wall_max.max(op.wall_us);
            self.cpu_max = self.cpu_max.max(op.cpu_us);
            if op.cpu_us >= stall_us && op.wall_us >= stall_us {
                self.stalls += 1;
                return true;
            }
        }
        false
    }

    fn print(&mut self, label: &str, time: bool) {
        let mut line = format!("summary {label} ops={}", self.ops);
        for i in 0..NF {
            if self.sum[i] > 0 {
                line += &format!(" {}_sum={} {}_max={}", FIELDS[i], self.sum[i], FIELDS[i], self.max[i]);
            }
        }
        if time && !self.cpu.is_empty() {
            self.cpu.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p = |q: f64| self.cpu[((self.cpu.len() - 1) as f64 * q) as usize];
            line += &format!(
                " cpu_p50_us={:.2} cpu_p99_us={:.2} cpu_max_us={:.1} wall_max_us={:.1} stalls={}",
                p(0.5),
                p(0.99),
                self.cpu_max,
                self.wall_max,
                self.stalls
            );
        }
        println!("{line}");
    }
}

fn nonzero(d: &[u64; NF]) -> String {
    let mut s = String::new();
    for i in 0..NF {
        if d[i] > 0 {
            s += &format!(" {}={}", FIELDS[i], d[i]);
        }
    }
    s
}

fn timing<T>(op: &Op<T>, time: bool) -> String {
    if time {
        format!(" cpu_us={:.1} wall_us={:.1}", op.cpu_us, op.wall_us)
    } else {
        String::new()
    }
}

fn check(cond: bool, what: &str) {
    if !cond {
        die(what);
    }
    println!("# check ok: {what}");
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn fork_trunk(s: &StoreBench) -> BranchId {
    s.fork_trunk().unwrap_or_else(|e| die(&format!("fork_trunk: {e}")))
}

fn fork_branch(s: &StoreBench, p: BranchId) -> BranchId {
    s.fork_branch(p).unwrap_or_else(|e| die(&format!("fork_branch: {e}")))
}

fn held(st: &BranchStats) -> usize {
    st.arena_slots_in_use + st.arena_slots_free
}

/// W1: every fork of a growth to n live trunk children.
fn arm_grow(a: &Args) {
    let s = StoreBench::new(a.page_size);
    let m = Meter { s: &s, time: a.time };
    let mut sum = Summary::default();
    let mut live = Vec::with_capacity(a.n);
    for n in 1..=a.n {
        let op = m.op(|| fork_trunk(&s));
        live.push(op.out);
        let stall = sum.add(&op, a.time, a.stall_us);
        if op.d[idx("branch_table_moved")] > 0 || stall {
            println!("event fork n={n}{}{}", nonzero(&op.d), timing(&op, a.time));
        }
    }
    check(s.stats().live_branches == a.n, "live_branches equals the forks made");
    sum.print("fork", a.time);
}

/// W1 at a constant live count: reap a random live child, fork one, `cycles` times.
fn arm_churn(a: &Args) {
    let s = StoreBench::new(a.page_size);
    let m = Meter { s: &s, time: a.time };
    let mut live: Vec<BranchId> = (0..a.n).map(|_| fork_trunk(&s)).collect();
    let grown = s.stats().work;
    println!(
        "# grown to {} live; branch_table_moved during growth {}",
        a.n, grown.branch_table_moved
    );
    let mut rng = Rng(a.seed);
    let (mut reaps, mut forks) = (Summary::default(), Summary::default());
    for c in 0..a.cycles {
        let at = rng.below(live.len() as u64) as usize;
        let victim = live.swap_remove(at);
        let op = m.op(|| s.reap(victim));
        if op.out.deferred || op.out.freed_pages != 0 {
            die("a churn reap was deferred or freed pages");
        }
        if reaps.add(&op, a.time, a.stall_us) || op.d[idx("branch_table_moved")] > 0 {
            println!("event reap cycle={c}{}{}", nonzero(&op.d), timing(&op, a.time));
        }
        let op = m.op(|| fork_trunk(&s));
        live.push(op.out);
        if forks.add(&op, a.time, a.stall_us) || op.d[idx("branch_table_moved")] > 0 {
            println!("event fork cycle={c}{}{}", nonzero(&op.d), timing(&op, a.time));
        }
    }
    check(s.stats().live_branches == a.n, "live_branches stayed at the churn level");
    reaps.print("reap", a.time);
    forks.print("fork", a.time);
}

/// W2: the bit-reversal input (or, with `fifo`, the same input reaped in fork order).
fn arm_binrev(a: &Args, fifo: bool) {
    for &levels in &a.list {
        let half = 1u32 << levels;
        let s = StoreBench::new(a.page_size);
        let m = Meter { s: &s, time: a.time };
        let anchor_a = fork_trunk(&s);
        let mut child = vec![anchor_a];
        for i in 1..half {
            s.trunk_write(i);
            child.push(fork_trunk(&s));
        }
        s.trunk_write(half);
        let anchor_z = fork_trunk(&s);
        for i in 1..=half {
            s.trunk_write(i);
        }
        let v = 2 * half as usize;
        println!("# L={levels} N={} children, V={v} retained versions", half as usize + 1);
        check(s.stats().arena_slots_in_use == v, "arena holds exactly V = 2^(L+1) versions");
        let order: Vec<u32> = if fifo {
            (1..half).collect()
        } else {
            (0..levels)
                .flat_map(|l| (0..(half >> (l + 1))).map(move |j| (2 * j + 1) << l))
                .collect()
        };
        check(order.len() == half as usize - 1, "every interior child is reaped once");
        let mut levels_sum: Vec<Summary> = (0..levels).map(|_| Summary::default()).collect();
        let mut interior = Summary::default();
        let last = *order.last().unwrap();
        for &x in &order {
            let op = m.op(|| s.reap(child[x as usize]));
            if op.out.deferred {
                die("an interior reap was deferred");
            }
            let level = x.trailing_zeros() as usize;
            let stall = interior.add(&op, a.time, a.stall_us);
            levels_sum[if fifo { 0 } else { level }].add(&op, a.time, a.stall_us);
            if x == last || stall {
                println!(
                    "event reap L={levels} x={x} level={level} freed_pages={}{}{}",
                    op.out.freed_pages,
                    nonzero(&op.d),
                    timing(&op, a.time)
                );
            }
        }
        check(s.stats().arena_slots_in_use == v, "no interior reap freed a version");
        if !fifo {
            for (l, sum) in levels_sum.iter_mut().enumerate() {
                sum.print(&format!("L={levels} level={l}"), a.time);
            }
        }
        interior.print(&format!("L={levels} interior"), a.time);
        for (name, id) in [("A", anchor_a), ("Z", anchor_z)] {
            let op = m.op(|| s.reap(id));
            println!(
                "event reap L={levels} anchor={name} freed_pages={}{}{}",
                op.out.freed_pages,
                nonzero(&op.d),
                timing(&op, a.time)
            );
        }
        check(s.stats().arena_slots_in_use == 0, "the anchors freed every version");
        check(s.stats().live_branches == 0, "no branch is left");
    }
}

/// W3, W4: a burst of `v` one-page branches, then every one but each 256th reaped.
fn arm_arena(a: &Args) {
    for &v in &a.list {
        let s = StoreBench::new(a.page_size);
        let m = Meter { s: &s, time: a.time };
        let mut alloc = Summary::default();
        let mut ids = Vec::with_capacity(v);
        for i in 0..v {
            let op = m.op(|| {
                let id = fork_trunk(&s);
                s.branch_write(id, &[1]).unwrap_or_else(|e| die(&format!("write: {e}")));
                id
            });
            ids.push(op.out);
            let d = &op.d;
            let stall = alloc.add(&op, a.time, a.stall_us);
            if d[idx("arena_bits_moved")] > 0 || d[idx("arena_chunks_moved")] > 0 || stall {
                println!("event alloc V={v} i={i}{}{}", nonzero(d), timing(&op, a.time));
            }
        }
        check(s.stats().arena_slots_in_use == v, "one slot per branch");
        let mut reap = Summary::default();
        let mut kept = Vec::new();
        for (i, id) in ids.into_iter().enumerate() {
            if i % 256 == 0 {
                kept.push(id);
                continue;
            }
            let op = m.op(|| s.reap(id));
            if op.out.freed_pages != 1 {
                die("a one-page branch did not free exactly one page");
            }
            let stall = reap.add(&op, a.time, a.stall_us);
            if op.d[idx("arena_free_moved")] > 0 || stall {
                println!("event reap V={v} i={i}{}{}", nonzero(&op.d), timing(&op, a.time));
            }
        }
        let st = s.stats();
        let live = st.arena_slots_in_use;
        check(live == v.div_ceil(256), "live slots = ceil(V/256)");
        println!(
            "space V={v} live_slots={live} free_slots={} held_slots={} held_over_live={:.2} \
             held_bytes={} live_bytes={}",
            st.arena_slots_free,
            held(&st),
            held(&st) as f64 / live as f64,
            held(&st) * a.page_size,
            live * a.page_size
        );
        alloc.print(&format!("V={v} alloc"), a.time);
        reap.print(&format!("V={v} reap"), a.time);
        // Re-grow to V: the free list is reused, so the peak is retained, not leaked.
        let mut regrow = Vec::new();
        for _ in 0..(v - live) {
            let id = fork_trunk(&s);
            s.branch_write(id, &[1]).unwrap_or_else(|e| die(&format!("write: {e}")));
            regrow.push(id);
        }
        let st2 = s.stats();
        println!(
            "space V={v} after_regrow live_slots={} held_slots={}",
            st2.arena_slots_in_use,
            held(&st2)
        );
        check(
            held(&st2) <= v + 256,
            "re-growth to V holds at most V slots plus one chunk (the peak is reused, not exceeded)",
        );
    }
}

/// W5: one branch writes pages 1..=n in one transaction.
fn arm_bigwrite(a: &Args) {
    let s = StoreBench::new(a.page_size);
    let m = Meter { s: &s, time: a.time };
    let id = fork_trunk(&s);
    s.begin_write(id).unwrap_or_else(|e| die(&format!("begin: {e}")));
    let mut sum = Summary::default();
    for p in 1..=a.n as u32 {
        let op = m.op(|| s.write_page(id, p));
        op.out.as_ref().unwrap_or_else(|e| die(&format!("write: {e}")));
        let stall = sum.add(&op, a.time, a.stall_us);
        if op.d[idx("page_table_moved")] > 0 || stall {
            println!("event write page={p}{}{}", nonzero(&op.d), timing(&op, a.time));
        }
    }
    s.end_write(id);
    check(s.stats().arena_slots_in_use == a.n, "one slot per written page");
    sum.print("write", a.time);
}

/// W6: a branch writes w pages, then forks twice.
fn arm_view(a: &Args) {
    for &w in &a.list {
        let s = StoreBench::new(a.page_size);
        let m = Meter { s: &s, time: a.time };
        let id = fork_trunk(&s);
        let pages: Vec<u32> = (1..=w as u32).collect();
        s.branch_write(id, &pages).unwrap_or_else(|e| die(&format!("write: {e}")));
        let mut kids = Vec::new();
        for k in 1..=2 {
            let op = m.op(|| fork_branch(&s, id));
            kids.push(op.out);
            println!("event fork W={w} fork={k}{}{}", nonzero(&op.d), timing(&op, a.time));
        }
        check(s.stats().live_branches == 3, "the branch and its two children are live");
    }
}

/// W7: alternate a fork and a write of one page of a 1,024-page set spread over 2^20.
fn arm_pathcopy(a: &Args) {
    let s = StoreBench::new(a.page_size);
    let m = Meter { s: &s, time: a.time };
    let id = fork_trunk(&s);
    let pages: Vec<u32> = (0..1024u32).map(|j| j.wrapping_mul(40_503) & ((1 << 20) - 1)).collect();
    s.branch_write(id, &pages).unwrap_or_else(|e| die(&format!("write: {e}")));
    let (mut forks, mut writes) = (Summary::default(), Summary::default());
    let mut kids = Vec::with_capacity(a.n);
    let mut min_copied = u64::MAX;
    for j in 0..a.n {
        let op = m.op(|| fork_branch(&s, id));
        kids.push(op.out);
        forks.add(&op, a.time, a.stall_us);
        let op = m.op(|| s.branch_write(id, &[pages[j % 1024]]));
        op.out.as_ref().unwrap_or_else(|e| die(&format!("write: {e}")));
        min_copied = min_copied.min(op.d[idx("map_nodes_copied")]);
        writes.add(&op, a.time, a.stall_us);
    }
    println!("# map_nodes_copied per write: min {min_copied}");
    forks.print("fork", a.time);
    writes.print("write", a.time);
}

/// W8: a chain of depth d whose intermediate handles are all released, then its leaf reaped.
fn arm_chain(a: &Args) {
    for &d in &a.list {
        let s = StoreBench::new(a.page_size);
        let m = Meter { s: &s, time: a.time };
        let mut prev = fork_trunk(&s);
        for _ in 0..d {
            let next = fork_branch(&s, prev);
            if !s.reap(prev).deferred {
                die("releasing a branch with a live child was not deferred");
            }
            prev = next;
        }
        let before = s.stats().live_branches;
        check(before == d + 1, "d+1 branch states are kept for one live handle");
        let op = m.op(|| s.reap(prev));
        let after = s.stats().live_branches;
        println!(
            "event reap d={d} states_before={before} states_after={after} freed_pages={}{}{} \
             entry_bytes={}",
            op.out.freed_pages,
            nonzero(&op.d),
            timing(&op, a.time),
            StoreBench::branch_entry_bytes()
        );
        check(after == 0, "the leaf's reap freed the whole chain");
    }
}

fn main() {
    let a = parse_args();
    let head = std::process::Command::new("git")
        .args(["rev-parse", "--short=9", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    println!(
        "# branch_adv arm={} n={} list={:?} cycles={} seed={:#x} page_size={} time={} stall_us={} \
         cwd_head={head} branch_entry_bytes={}",
        a.arm,
        a.n,
        a.list,
        a.cycles,
        a.seed,
        a.page_size,
        a.time,
        a.stall_us,
        StoreBench::branch_entry_bytes()
    );
    match a.arm.as_str() {
        "grow" => arm_grow(&a),
        "churn" => arm_churn(&a),
        "binrev" => arm_binrev(&a, false),
        "fifo" => arm_binrev(&a, true),
        "arena" => arm_arena(&a),
        "bigwrite" => arm_bigwrite(&a),
        "view" => arm_view(&a),
        "pathcopy" => arm_pathcopy(&a),
        "chain" => arm_chain(&a),
        other => die(&format!("unknown arm {other}")),
    }
    println!("# done");
}
