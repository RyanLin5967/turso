//! Bushy version trees: lane r11-bushy, artie-research `frontier/round11/r11-bushy/PREREG.md`.
//!
//!   cargo run -p turso_core --release --example branch_bushy -- --shape <bb|cat|beam> [options]
//!
//! Shapes (the PREREG is the specification; this is its implementation):
//!
//!   bb    BranchBench's step (arXiv:2604.17180 §4.2, Table 3): pick a node uniformly among those
//!         with fewer live children than their fanout (`--fr` for the trunk, `--fi` below it) and
//!         depth below `--depth`, fork a child, write, read, and prune the child with probability
//!         `--gamma` (per mille). x = nodes created.
//!   cat   the caterpillar (MCTS/UCT's expand-then-descend): at each level the current node forks
//!         `--fanout` children, each writes and reads; one, drawn before the level, becomes the next
//!         level's node; each other child is pruned with probability `--gamma`. x = levels.
//!   beam  beam search: each of the `--beam` nodes forks `--expand` children, each writes and reads;
//!         `--beam` of the pool, drawn uniformly, form the next beam, the rest are pruned. x = levels.
//!
//! `--rows hot` makes every node write the same row (one page, the most shadowing); `--rows spread`
//! makes node n write `row_for(n)`. `--interior release` drops a node's handle once the workload
//! will never fork from it again (bb: its live children reached its fanout; cat and beam: the level
//! after it). `--continue` makes a node rewrite its own row after each child it forks.
//!
//! `--refcounted` (PREREG amendment 1) tells the harness the store frees a branch state as soon as
//! its handle and connection are gone (reference-counted page maps, no kept ancestors), so the
//! harness's own count of states follows that rule instead of "a state lives while it has a handle
//! or a child state".
//!
//! Without `--timing` the run prints integer counters only, never a time: such a run may go
//! without the fleet lock (LANE-BRIEF). Every read is checked against a model the harness keeps
//! (never against the engine), and the engine's branch-state count against the harness's own
//! count of states that must exist; a mismatch prints `NOT A RESULT` and exits 1.

use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::branch::{Branch, BranchWork};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;
/// The row every node writes under `--rows hot`.
const HOT_ROW: i64 = 1;
/// Node 0 is the trunk.
const TRUNK: u32 = 0;
/// One node in this many also reads the row its root-level ancestor wrote, through a full model walk.
const DEEP_READ_EVERY: u32 = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Bb,
    Cat,
    Beam,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Rows {
    Hot,
    Spread,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Interior {
    Hold,
    Release,
}

struct Args {
    shape: Shape,
    rows: Rows,
    interior: Interior,
    cont: bool,
    fr: u32,
    fi: u32,
    depth: u32,
    gamma_milli: u64,
    fanout: u32,
    beam: u32,
    expand: u32,
    checkpoints: Vec<u64>,
    seed: u64,
    timing: bool,
    samples: usize,
    needed: bool,
    refcounted: bool,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_bushy: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut args = Args {
        shape: Shape::Bb,
        rows: Rows::Hot,
        interior: Interior::Hold,
        cont: false,
        fr: 10,
        fi: 10,
        depth: 25,
        gamma_milli: 100,
        fanout: 1000,
        beam: 10,
        expand: 100,
        checkpoints: vec![100, 1000],
        seed: 0x9E37_79B9_7F4A_7C15,
        timing: false,
        samples: 200,
        needed: true,
        refcounted: false,
    };
    let mut shape = None;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        let num = |v: String, what: &str| -> u64 { v.parse().unwrap_or_else(|_| die(&format!("bad {what}"))) };
        match flag.as_str() {
            "--shape" => {
                shape = Some(match val().as_str() {
                    "bb" => Shape::Bb,
                    "cat" => Shape::Cat,
                    "beam" => Shape::Beam,
                    other => die(&format!("unknown shape {other}")),
                })
            }
            "--rows" => {
                args.rows = match val().as_str() {
                    "hot" => Rows::Hot,
                    "spread" => Rows::Spread,
                    other => die(&format!("unknown rows {other}")),
                }
            }
            "--interior" => {
                args.interior = match val().as_str() {
                    "hold" => Interior::Hold,
                    "release" => Interior::Release,
                    other => die(&format!("unknown interior {other}")),
                }
            }
            "--continue" => args.cont = true,
            "--fr" => args.fr = num(val(), "--fr") as u32,
            "--fi" => args.fi = num(val(), "--fi") as u32,
            "--depth" => args.depth = num(val(), "--depth") as u32,
            "--gamma" => args.gamma_milli = num(val(), "--gamma"),
            "--fanout" => args.fanout = num(val(), "--fanout") as u32,
            "--beam" => args.beam = num(val(), "--beam") as u32,
            "--expand" => args.expand = num(val(), "--expand") as u32,
            "--checkpoints" => {
                args.checkpoints = val().split(',').map(|x| num(x.to_string(), "--checkpoints")).collect()
            }
            "--seed" => args.seed = num(val(), "--seed"),
            "--timing" => args.timing = true,
            "--samples" => args.samples = num(val(), "--samples") as usize,
            "--no-needed" => args.needed = false,
            "--refcounted" => args.refcounted = true,
            other => die(&format!("unknown argument {other}")),
        }
    }
    args.shape = shape.unwrap_or_else(|| die("--shape is required"));
    if args.checkpoints.is_empty() || args.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    if args.gamma_milli > 1000 || args.fr == 0 || args.fi == 0 || args.depth == 0 {
        die("--gamma is per mille (0..=1000); --fr, --fi, --depth must be positive");
    }
    if args.fanout == 0 || args.beam == 0 || args.expand == 0 || args.samples == 0 {
        die("--fanout, --beam, --expand, --samples must be positive");
    }
    if args.shape == Shape::Beam && args.beam > args.expand {
        die("--beam must not exceed --expand (the first level's pool is --expand children)");
    }
    args
}

/// The harness's random stream: xorshift64, as in `branch_arms`. `model.py` replays it draw for
/// draw, so every draw the workload makes is one of the draws listed in the PREREG, in that order.
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

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

/// The value node `node` writes as its `generation`-th write: unique, and the trunk's length, so
/// every write rewrites the row in place and dirties exactly its leaf.
fn node_value(node: u32, generation: u64) -> String {
    let s = format!("n{node:0>11}g{generation:0>11}");
    format!("{s:.<width$}", width = VALUE_LEN)
}

fn row_for(n: u32) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

/// A row no node writes under `--rows hot`, for a read that resolves to the trunk.
fn trunk_row(n: u32) -> i64 {
    (n as i64 * 7919) % (TRUNK_ROWS - 1) + 2
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    let mut stmt = conn.prepare(format!("SELECT v FROM t WHERE id = {id}")).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match &row[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {id}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {id}: {} rows", rows.len())),
    }
}

fn update_to(conn: &Arc<Connection>, id: i64, value: &str) {
    conn.execute(format!("UPDATE t SET v = '{value}' WHERE id = {id}")).unwrap();
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

struct Node {
    parent: u32,
    /// The parent's epoch (forks made so far) when this node was forked.
    fork_epoch: u32,
    depth: u32,
    row: i64,
    /// Forks this node has made.
    epoch: u32,
    /// Children not pruned (bb's fanout test counts these).
    kept_children: u32,
    /// Children whose branch state exists (live or kept for a live descendant).
    child_states: u32,
    /// (epoch at the write, generation), ascending.
    writes: Vec<(u32, u64)>,
    branch: Option<Branch>,
    state: bool,
}

struct Bench {
    db: Arc<Database>,
    trunk: Arc<Connection>,
    nodes: Vec<Node>,
    rng: Rng,
    rows: Rows,
    cont: bool,
    refcounted: bool,
    generation: u64,
    handles: u64,
    states: u64,
    reaps: u64,
    max_cascade: u64,
    /// Reaps that freed more than one state.
    cascades: u64,
    reads_checked: u64,
}

impl Bench {
    fn work(&self) -> BranchWork {
        self.db.branch_stats().work
    }

    fn node_row(&self, id: u32) -> i64 {
        match self.rows {
            Rows::Hot => HOT_ROW,
            Rows::Spread => row_for(id),
        }
    }

    /// What `reader` reads for `row`, from the harness's own write log.
    fn expect(&self, reader: u32, row: i64) -> String {
        let n = &self.nodes[reader as usize];
        if n.row == row {
            let &(_, g) = n.writes.last().expect("a node writes its row when it is created");
            return node_value(reader, g);
        }
        if self.rows == Rows::Hot {
            // Under `--rows hot` no node writes any other row.
            return trunk_value(row);
        }
        let (mut a, mut f) = (n.parent, n.fork_epoch);
        loop {
            if a == TRUNK {
                return trunk_value(row);
            }
            let an = &self.nodes[a as usize];
            if an.row == row {
                let seen = an.writes.partition_point(|&(e, _)| e <= f);
                return node_value(a, an.writes[seen - 1].1);
            }
            (a, f) = (an.parent, an.fork_epoch);
        }
    }

    /// Fork a child of `parent` (a node with a live handle, or the trunk) and record it.
    fn fork(&mut self, parent: u32) -> (u32, Branch) {
        let branch = if parent == TRUNK {
            self.trunk.fork_branch().unwrap()
        } else {
            self.nodes[parent as usize].branch.as_ref().expect("forked from a live handle").fork().unwrap()
        };
        let id = self.nodes.len() as u32;
        let p = &mut self.nodes[parent as usize];
        let (fork_epoch, depth) = (p.epoch, p.depth + 1);
        p.epoch += 1;
        p.kept_children += 1;
        p.child_states += 1;
        let row = self.node_row(id);
        self.nodes.push(Node {
            parent,
            fork_epoch,
            depth,
            row,
            epoch: 0,
            kept_children: 0,
            child_states: 0,
            writes: Vec::new(),
            branch: None,
            state: true,
        });
        self.handles += 1;
        self.states += 1;
        (id, branch)
    }

    /// The node's write (M_d = 1): one UPDATE of its own row.
    fn write(&mut self, id: u32, conn: &Arc<Connection>) {
        let g = self.generation;
        self.generation += 1;
        let n = &mut self.nodes[id as usize];
        n.writes.push((n.epoch, g));
        let row = n.row;
        update_to(conn, row, &node_value(id, g));
    }

    /// The node's evaluate reads: its own row, then its parent's row (spread) or a trunk row (hot),
    /// and one node in DEEP_READ_EVERY also the row of its root-level ancestor.
    fn read_rows(&self, id: u32) -> Vec<i64> {
        let n = &self.nodes[id as usize];
        let mut rows = vec![n.row];
        rows.push(match self.rows {
            Rows::Hot => trunk_row(id),
            Rows::Spread if n.parent != TRUNK => self.nodes[n.parent as usize].row,
            Rows::Spread => trunk_row(id),
        });
        if id % DEEP_READ_EVERY == 0 && self.rows == Rows::Spread {
            let mut a = id;
            while self.nodes[a as usize].parent != TRUNK {
                a = self.nodes[a as usize].parent;
            }
            rows.push(self.nodes[a as usize].row);
        }
        rows
    }

    fn evaluate(&mut self, id: u32, conn: &Arc<Connection>) {
        for row in self.read_rows(id) {
            let got = read_v(conn, row);
            if got != self.expect(id, row) {
                not_a_result(&format!("node {id} read row {row} as {got}, expected {}", self.expect(id, row)));
            }
            self.reads_checked += 1;
        }
    }

    /// One node's step after its fork: connect, write, evaluate, disconnect.
    fn step_child(&mut self, id: u32, branch: &Branch) {
        let conn = branch.connect().unwrap();
        self.write(id, &conn);
        self.evaluate(id, &conn);
        drop(conn);
    }

    /// `--continue`: the parent rewrites its own row after forking a child.
    fn continue_write(&mut self, parent: u32) {
        if !self.cont || parent == TRUNK {
            return;
        }
        let conn = self.nodes[parent as usize].branch.as_ref().expect("live parent").connect().unwrap();
        self.write(parent, &conn);
        drop(conn);
    }

    /// Drop `id`'s handle and check the engine freed exactly the states the harness says must go.
    fn release(&mut self, id: u32, branch: Branch) {
        let before = self.work().states_freed;
        let reaped = branch.reap().unwrap();
        let freed = self.work().states_freed - before;
        self.handles -= 1;
        let expected = self.free_states(id);
        if freed != expected || reaped.deferred != (expected == 0) {
            not_a_result(&format!(
                "reaping node {id} freed {freed} states (deferred {}), the harness expects {expected}",
                reaped.deferred
            ));
        }
        self.reaps += 1;
        self.max_cascade = self.max_cascade.max(freed);
        if freed > 1 {
            self.cascades += 1;
        }
    }

    /// The harness's own rule for which states exist: a state lives while it has a handle or a
    /// child state (under `--refcounted`: while it has a handle). Returns how many states `id`'s
    /// handle drop frees.
    fn free_states(&mut self, mut id: u32) -> u64 {
        if self.refcounted {
            let n = &mut self.nodes[id as usize];
            if n.branch.is_some() || !n.state {
                return 0;
            }
            n.state = false;
            self.states -= 1;
            return 1;
        }
        let mut freed = 0;
        loop {
            let n = &mut self.nodes[id as usize];
            if n.branch.is_some() || n.child_states > 0 || !n.state {
                return freed;
            }
            n.state = false;
            freed += 1;
            self.states -= 1;
            let parent = n.parent;
            if parent == TRUNK {
                return freed;
            }
            self.nodes[parent as usize].child_states -= 1;
            id = parent;
        }
    }

    /// Prune a fresh child: its handle goes, and it no longer counts against its parent's fanout.
    fn prune(&mut self, id: u32, branch: Branch) {
        let parent = self.nodes[id as usize].parent;
        self.nodes[parent as usize].kept_children -= 1;
        self.release(id, branch);
    }

    fn keep(&mut self, id: u32, branch: Branch) {
        self.nodes[id as usize].branch = Some(branch);
    }

    fn release_interior(&mut self, id: u32) {
        let branch = self.nodes[id as usize].branch.take().expect("released twice");
        self.release(id, branch);
    }

    fn draw_prune(&mut self, gamma_milli: u64) -> bool {
        gamma_milli > 0 && self.rng.below(1000) < gamma_milli
    }

    fn checkpoint(&mut self, args: &Args, x: u64, level: Option<u64>, w0: BranchWork, extra: &str) {
        let s = self.db.branch_stats();
        if s.live_branches as u64 != self.states {
            not_a_result(&format!("engine has {} branch states, the harness expects {}", s.live_branches, self.states));
        }
        let zombies = self.db.branch_zombies() as u64;
        if zombies != self.states - self.handles {
            not_a_result(&format!("engine has {zombies} zombies, the harness expects {}", self.states - self.handles));
        }
        let needed = if args.needed { self.db.branch_needed_slots().to_string() } else { "skipped".to_string() };
        let w = s.work;
        let resolves = w.resolve_calls - w0.resolve_calls;
        let per = |v: u64| if resolves == 0 { 0.0 } else { v as f64 / resolves as f64 };
        let max_depth = self.nodes.iter().skip(1).map(|n| n.depth).max().unwrap_or(0);
        println!(
            "# ckpt x={x} level={} handles={} states={} zombies={zombies} arena_in_use={} \
             arena_high_water={} needed={needed} page_map_nodes={} resolves={resolves} \
             levels_per_resolve={:.4} examined_per_resolve={:.4} reaps={} states_freed={} \
             max_cascade={} cascades={} gc_range={} gc_examined={} nodes={} max_depth={max_depth} \
             reads_checked={} rss_bytes={}{extra}",
            level.map_or("-".to_string(), |l| l.to_string()),
            self.handles,
            self.states,
            s.arena_slots_in_use,
            s.arena_slots_in_use + s.arena_slots_free,
            s.page_map_nodes,
            per(w.resolve_levels - w0.resolve_levels),
            per(w.resolve_retained_examined - w0.resolve_retained_examined),
            self.reaps,
            w.states_freed - w0.states_freed,
            self.max_cascade,
            self.cascades,
            w.gc_range_entries - w0.gc_range_entries,
            w.gc_examined - w0.gc_examined,
            self.nodes.len() - 1,
            self.reads_checked,
            rss_bytes(),
        );
    }

    /// `--timing`: K steps off the workload's stream, each timed per op, each child pruned at once
    /// so the tree the counters describe is unchanged (the parents' epochs advance).
    fn sample(&mut self, args: &Args, x: u64, parents: &[u32], rng: &mut Rng) {
        let names = ["fork", "open", "write", "read_own", "read_other", "reap"];
        let mut t: [Vec<Duration>; 6] = Default::default();
        for _ in 0..args.samples {
            let p = parents[rng.below(parents.len() as u64) as usize];
            let a = Instant::now();
            let (id, branch) = self.fork(p);
            t[0].push(a.elapsed());
            let a = Instant::now();
            let conn = branch.connect().unwrap();
            t[1].push(a.elapsed());
            let g = self.generation;
            self.generation += 1;
            let row = self.nodes[id as usize].row;
            self.nodes[id as usize].writes.push((0, g));
            let value = node_value(id, g);
            let a = Instant::now();
            update_to(&conn, row, &value);
            t[2].push(a.elapsed());
            let rows = self.read_rows(id);
            for (k, &r) in rows.iter().take(2).enumerate() {
                let a = Instant::now();
                let got = read_v(&conn, r);
                t[3 + k].push(a.elapsed());
                if got != self.expect(id, r) {
                    not_a_result(&format!("sampled node {id} read row {r} wrong"));
                }
            }
            drop(conn);
            self.nodes[p as usize].kept_children -= 1;
            let a = Instant::now();
            let before = self.work().states_freed;
            let reaped = branch.reap().unwrap();
            t[5].push(a.elapsed());
            self.handles -= 1;
            let expected = self.free_states(id);
            if self.work().states_freed - before != expected || reaped.deferred {
                not_a_result("a sampled reap freed the wrong states");
            }
        }
        for (name, samples) in names.iter().zip(t.iter()) {
            let mut us: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1e6).collect();
            us.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "{x}\t{name}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
                us.len(),
                percentile(&us, 50.0),
                percentile(&us, 90.0),
                percentile(&us, 99.0),
                us[us.len() - 1]
            );
        }
    }

    /// Drop every remaining handle, oldest node first, and check the store empties.
    fn teardown(&mut self, timing: bool) {
        let ids: Vec<u32> = (1..self.nodes.len() as u32).filter(|&i| self.nodes[i as usize].branch.is_some()).collect();
        let mut worst: (u64, f64) = (0, 0.0);
        for id in ids {
            let branch = self.nodes[id as usize].branch.take().unwrap();
            let before = self.work().states_freed;
            let a = Instant::now();
            self.release(id, branch);
            let us = a.elapsed().as_secs_f64() * 1e6;
            let freed = self.work().states_freed - before;
            if freed > worst.0 {
                worst = (freed, us);
            }
        }
        let s = self.db.branch_stats();
        if s.live_branches != 0 || s.arena_slots_in_use != 0 || self.states != 0 {
            not_a_result(&format!("teardown leaked: {s:?}, harness states {}", self.states));
        }
        if timing {
            println!("# teardown_max_cascade states={} us={:.2} (ONE sample)", worst.0, worst.1);
        } else {
            println!("# teardown_max_cascade states={}", worst.0);
        }
        println!(
            "# teardown: every branch freed, arena empty ({} free slots), reaps={} max_cascade={} \
             cascades={} reads_checked={}",
            s.arena_slots_free, self.reaps, self.max_cascade, self.cascades, self.reads_checked
        );
    }
}

/// BranchBench's step loop. The trunk is node 0 with fanout `--fr`; every other node has `--fi`.
fn shape_bb(b: &mut Bench, args: &Args) {
    let w0 = b.work();
    let mut timing_rng = Rng(args.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    // The nodes a step may pick, and each node's index in it (u32::MAX: not eligible).
    let mut eligible: Vec<u32> = vec![TRUNK];
    let mut pos: Vec<u32> = vec![0];
    let cap = |id: u32| if id == TRUNK { args.fr } else { args.fi };
    let remove = |eligible: &mut Vec<u32>, pos: &mut Vec<u32>, id: u32| {
        let i = pos[id as usize] as usize;
        let last = *eligible.last().unwrap();
        eligible.swap_remove(i);
        if last != id {
            pos[last as usize] = i as u32;
        }
        pos[id as usize] = u32::MAX;
    };
    for &x in &args.checkpoints {
        while ((b.nodes.len() - 1) as u64) < x {
            if eligible.is_empty() {
                not_a_result(&format!("the tree saturated at {} nodes", b.nodes.len() - 1));
            }
            let p = eligible[b.rng.below(eligible.len() as u64) as usize];
            let (id, branch) = b.fork(p);
            pos.push(u32::MAX);
            b.step_child(id, &branch);
            if b.draw_prune(args.gamma_milli) {
                b.prune(id, branch);
            } else {
                b.keep(id, branch);
                if b.nodes[id as usize].depth < args.depth {
                    pos[id as usize] = eligible.len() as u32;
                    eligible.push(id);
                }
            }
            b.continue_write(p);
            if b.nodes[p as usize].kept_children == cap(p) {
                remove(&mut eligible, &mut pos, p);
                if args.interior == Interior::Release && p != TRUNK {
                    b.release_interior(p);
                }
            }
        }
        b.checkpoint(args, x, None, w0, &format!(" eligible={}", eligible.len()));
        if args.timing {
            let parents = eligible.clone();
            b.sample(args, x, &parents, &mut timing_rng);
            // The sampled children were pruned; they are never eligible.
            pos.resize(b.nodes.len(), u32::MAX);
        }
    }
}

/// The caterpillar: `--fanout` children per level, one descended into.
fn shape_cat(b: &mut Bench, args: &Args) {
    let w0 = b.work();
    let mut timing_rng = Rng(args.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let (mut x_node, branch) = b.fork(TRUNK);
    b.step_child(x_node, &branch);
    b.keep(x_node, branch);
    let mut level = 0u64;
    for &target in &args.checkpoints {
        while level < target {
            let choice = b.rng.below(args.fanout as u64) as u32;
            let mut next = None;
            for i in 0..args.fanout {
                let (id, branch) = b.fork(x_node);
                b.step_child(id, &branch);
                if i == choice {
                    b.keep(id, branch);
                    next = Some(id);
                } else if b.draw_prune(args.gamma_milli) {
                    b.prune(id, branch);
                } else {
                    b.keep(id, branch);
                }
                b.continue_write(x_node);
            }
            let prev = x_node;
            x_node = next.unwrap();
            if args.interior == Interior::Release {
                b.release_interior(prev);
            }
            level += 1;
        }
        let x = (b.nodes.len() - 1) as u64;
        b.checkpoint(args, x, Some(level), w0, "");
        if args.timing {
            b.sample(args, x, &[x_node], &mut timing_rng);
        }
    }
}

/// Beam search: each beam node forks `--expand` children; `--beam` of the pool survive.
fn shape_beam(b: &mut Bench, args: &Args) {
    let w0 = b.work();
    let mut timing_rng = Rng(args.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let (root, branch) = b.fork(TRUNK);
    b.step_child(root, &branch);
    b.keep(root, branch);
    let mut beam = vec![root];
    let mut level = 0u64;
    for &target in &args.checkpoints {
        while level < target {
            let mut pool: Vec<(u32, Branch)> = Vec::new();
            for &p in &beam {
                for _ in 0..args.expand {
                    let (id, branch) = b.fork(p);
                    b.step_child(id, &branch);
                    pool.push((id, branch));
                    b.continue_write(p);
                }
            }
            // A partial Fisher-Yates: the first `--beam` positions are the survivors.
            let keep = (args.beam as usize).min(pool.len());
            for i in 0..keep {
                let j = i + b.rng.below((pool.len() - i) as u64) as usize;
                pool.swap(i, j);
            }
            let losers = pool.split_off(keep);
            for (id, branch) in losers {
                b.prune(id, branch);
            }
            let old = std::mem::take(&mut beam);
            for (id, branch) in pool {
                b.keep(id, branch);
                beam.push(id);
            }
            if args.interior == Interior::Release {
                for p in old {
                    b.release_interior(p);
                }
            }
            level += 1;
        }
        let x = (b.nodes.len() - 1) as u64;
        b.checkpoint(args, x, Some(level), w0, "");
        if args.timing {
            let parents = beam.clone();
            b.sample(args, x, &parents, &mut timing_rng);
        }
    }
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_bushy.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = OFF").unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk.execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id))).unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let page_count = trunk.prepare("PRAGMA page_count").unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap();
    println!("# branch_bushy — Turso fork, lane r11-bushy (frontier/round11/r11-bushy/PREREG.md)");
    println!(
        "# shape={:?} rows={:?} interior={:?} continue={} fr={} fi={} depth={} gamma_milli={} \
         fanout={} beam={} expand={} checkpoints={:?} seed={:#x} timing={} samples={} \
         refcounted={} trunk_rows={TRUNK_ROWS} value_len={VALUE_LEN} trunk_pages={page_count} build={}",
        args.shape,
        args.rows,
        args.interior,
        args.cont,
        args.fr,
        args.fi,
        args.depth,
        args.gamma_milli,
        args.fanout,
        args.beam,
        args.expand,
        args.checkpoints,
        args.seed,
        args.timing,
        args.samples,
        args.refcounted,
        if cfg!(debug_assertions) { "DEBUG" } else { "release" },
    );
    if args.timing {
        println!("# timing rows: x\top\tsamples\tp50_us\tp90_us\tp99_us\tmax_us");
    } else {
        println!("# counters only: this run prints no time");
    }
    let mut b = Bench {
        db: db.clone(),
        trunk,
        nodes: vec![Node {
            parent: TRUNK,
            fork_epoch: 0,
            depth: 0,
            row: 0,
            epoch: 0,
            kept_children: 0,
            child_states: 0,
            writes: Vec::new(),
            branch: None,
            state: false,
        }],
        rng: Rng(args.seed),
        rows: args.rows,
        cont: args.cont,
        refcounted: args.refcounted,
        generation: 0,
        handles: 0,
        states: 0,
        reaps: 0,
        max_cascade: 0,
        cascades: 0,
        reads_checked: 0,
    };
    match args.shape {
        Shape::Bb => shape_bb(&mut b, &args),
        Shape::Cat => shape_cat(&mut b, &args),
        Shape::Beam => shape_beam(&mut b, &args),
    }
    b.teardown(args.timing);
}
