//! Branches EVER created against branches LIVE (lane r11-ever; pre-registration in artie-research
//! `frontier/round11/r11-ever/PREREG.md`, committed before this was built).
//!
//!   cargo run -p turso_core --release --example branch_ever -- --shape flat|moran|refine \
//!       --victim random|oldest --live N --checkpoints a,b,... [--trunk none|spread] [--untimed]
//!
//! Holds N_live branches live and churns: every cycle forks ONE branch and reaps ONE older live
//! branch, so N_ever (branches ever created, the x axis) grows while N_live stays fixed.
//!
//!   --shape flat     every fork is from the trunk
//!   --shape moran    every fork is from a uniformly random live branch, which then rewrites its own
//!                    row (an agent that keeps working after spawning a sub-agent)
//!   --shape refine   every fork is from the NEWEST live branch (an agent iterating on its last try)
//!   --victim random  a uniformly random live branch (drawn before the new one joins)
//!   --victim oldest  the oldest live branch (uniform-TTL lease expiry)
//!   --trunk spread   the trunk also rewrites the next row of a walk over the whole table each cycle
//!
//! Every new branch writes its own row. At each checkpoint (a value of N_ever) the harness prints
//! every resident structure of the store (`Database::branch_resident`), and REFUSES to print a
//! number unless the engine's kept states, zombies and arena pages equal the harness's own model of
//! the reclamation rule, and no arena slot is unowned. Unless `--untimed`, it also prints per-op
//! latency over the last `--window` cycles before the checkpoint, and every sample at or above
//! `--stall-us`. With `--untimed` no clock is read: the run prints integers only.
//!
//! Every cycle, outside any timed region, it reads the branch table's `(len, capacity)`; capacity is
//! items plus growth left, so at a fixed item count it falls by one per hashbrown tombstone and jumps
//! back at a rehash. Rehashes are counted (in place vs resize) and tombstones printed.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{Branch, BranchResident};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
/// Branches write rows 1..=BRANCH_ROWS; the rows above are never written by a branch, so a read of
/// one resolves through every level to the trunk.
const BRANCH_ROWS: i64 = 18_000;
const VALUE_LEN: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Flat,
    Moran,
    Refine,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Victim {
    Random,
    Oldest,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    /// The store as of 0f4232957: a state is kept while its handle lives or it has a kept child.
    Keep,
    /// Pre-registered fix F7: a zombie with exactly one kept child is spliced out.
    Splice,
}

struct Args {
    shape: Shape,
    victim: Victim,
    live: usize,
    checkpoints: Vec<usize>,
    window: usize,
    read_every: usize,
    stall_us: f64,
    untimed: bool,
    spread: bool,
    synchronous: String,
    seed: u64,
    expect: Expect,
}

fn parse_args() -> Args {
    let mut a = Args {
        shape: Shape::Flat,
        victim: Victim::Random,
        live: 1000,
        checkpoints: vec![],
        window: 5000,
        read_every: 10,
        stall_us: 100.0,
        untimed: false,
        spread: false,
        synchronous: "NORMAL".into(),
        seed: 0x9E37_79B9_7F4A_7C15,
        expect: Expect::Keep,
    };
    let mut shape = None;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--shape" => {
                shape = Some(match val().as_str() {
                    "flat" => Shape::Flat,
                    "moran" => Shape::Moran,
                    "refine" => Shape::Refine,
                    o => die(&format!("unknown shape {o}")),
                })
            }
            "--victim" => {
                a.victim = match val().as_str() {
                    "random" => Victim::Random,
                    "oldest" => Victim::Oldest,
                    o => die(&format!("unknown victim {o}")),
                }
            }
            "--trunk" => {
                a.spread = match val().as_str() {
                    "none" => false,
                    "spread" => true,
                    o => die(&format!("unknown --trunk {o}")),
                }
            }
            "--synchronous" => {
                a.synchronous = match val().as_str() {
                    "off" => "OFF",
                    "normal" => "NORMAL",
                    "full" => "FULL",
                    o => die(&format!("unknown --synchronous {o}")),
                }
                .into()
            }
            "--expect" => {
                a.expect = match val().as_str() {
                    "keep" => Expect::Keep,
                    "splice" => Expect::Splice,
                    o => die(&format!("unknown --expect {o}")),
                }
            }
            "--live" => a.live = val().parse().unwrap_or_else(|_| die("bad --live")),
            "--checkpoints" => {
                a.checkpoints = val()
                    .split(',')
                    .map(|x| x.parse().unwrap_or_else(|_| die("bad --checkpoints")))
                    .collect()
            }
            "--window" => a.window = val().parse().unwrap_or_else(|_| die("bad --window")),
            "--read-every" => {
                a.read_every = val().parse().unwrap_or_else(|_| die("bad --read-every"))
            }
            "--stall-us" => a.stall_us = val().parse().unwrap_or_else(|_| die("bad --stall-us")),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--untimed" => a.untimed = true,
            o => die(&format!("unknown argument {o}")),
        }
    }
    a.shape = shape.unwrap_or_else(|| die("--shape is required"));
    if a.live < 2 || a.checkpoints.is_empty() {
        die("--live must be >= 2 and --checkpoints non-empty");
    }
    if a.checkpoints[0] <= a.live || a.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing and above --live");
    }
    if a.window == 0 || a.read_every == 0 {
        die("--window and --read-every must be positive");
    }
    a
}

fn die(msg: &str) -> ! {
    eprintln!("branch_ever: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

/// Branch `id`'s `generation`-th value: same length every time, so every write is in place.
fn branch_value(id: u64, generation: u64) -> String {
    format!("b{id:0>12}g{generation:0>86}")
}

fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

/// The row the `n`-th branch ever created writes.
fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % BRANCH_ROWS as u64) as i64 + 1
}

/// `--trunk spread`: the row the trunk rewrites at its g-th write (walks every row; 37 is coprime
/// with 20,000).
fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    let mut stmt = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap();
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
    conn.execute(format!("UPDATE t SET v = '{value}' WHERE id = {id}"))
        .unwrap();
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn clock_tick_ns() -> f64 {
    let mut min = u128::MAX;
    for _ in 0..10_000 {
        let a = Instant::now();
        let mut b = Instant::now();
        while b == a {
            b = Instant::now();
        }
        min = min.min((b - a).as_nanos());
    }
    min as f64
}

/// The trunk's write history, kept by the harness (independent of the engine).
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    history: HashMap<i64, Vec<u64>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push(g);
        g
    }
    /// Forget writes no live branch can read: keep, per row, the last write before `oldest` (the
    /// oldest live branch's fork point) and every write after it.
    fn prune(&mut self, oldest: u64) {
        for h in self.history.values_mut() {
            let n = h.partition_point(|&g| g < oldest);
            if n > 1 {
                h.drain(..n - 1);
            }
        }
    }

    fn bytes(&self) -> usize {
        self.history.capacity() * (std::mem::size_of::<(i64, Vec<u64>)>() + 1)
            + self.history.values().map(|h| h.capacity() * 8).sum::<usize>()
    }

    fn value_at(&self, row: i64, writes_at: u64) -> String {
        let Some(h) = self.history.get(&row) else {
            return trunk_value(row);
        };
        let n = h.partition_point(|&g| g < writes_at);
        if n == 0 {
            trunk_value(row)
        } else {
            trunk_gen_value(h[n - 1])
        }
    }
}

/// A live branch and what it must read, in O(1) state: its own row, the row its parent wrote as the
/// parent saw it at the fork (the nearest ancestor's row), and the trunk as of its root's fork.
struct Live {
    branch: Branch,
    id: u64,
    own_row: i64,
    generation: u64,
    parent_row: Option<(i64, String)>,
    trunk_writes_at_root: u64,
}

impl Live {
    fn own(&self) -> String {
        branch_value(self.id, self.generation)
    }
}

/// The harness's model of which branch states the store keeps, from the reclamation rule alone.
#[derive(Default)]
struct KeptModel {
    /// id -> (parent id, 0 for the trunk; kept children; handle alive)
    nodes: HashMap<u64, (u64, Vec<u64>, bool)>,
}

impl KeptModel {
    fn fork(&mut self, id: u64, parent: u64) {
        self.nodes.insert(id, (parent, Vec::new(), true));
        if parent != 0 {
            self.nodes.get_mut(&parent).unwrap().1.push(id);
        }
    }

    fn detach(&mut self, parent: u64, child: u64) {
        let kids = &mut self.nodes.get_mut(&parent).unwrap().1;
        let at = kids.iter().position(|&k| k == child).unwrap();
        kids.swap_remove(at);
    }

    /// Release `id`'s handle and apply the rule; returns the states freed.
    fn release(&mut self, id: u64, expect: Expect) -> usize {
        self.nodes.get_mut(&id).unwrap().2 = false;
        let mut freed = 0;
        let mut at = id;
        loop {
            let (parent, kids, handle) = match self.nodes.get(&at) {
                Some((p, k, h)) => (*p, k.len(), *h),
                None => return freed,
            };
            if handle {
                return freed;
            }
            if kids > 0 {
                if expect == Expect::Splice && kids == 1 {
                    let child = self.nodes[&at].1[0];
                    self.nodes.remove(&at);
                    self.nodes.get_mut(&child).unwrap().0 = parent;
                    if parent != 0 {
                        let pk = &mut self.nodes.get_mut(&parent).unwrap().1;
                        let i = pk.iter().position(|&k| k == at).unwrap();
                        pk[i] = child;
                    }
                }
                return freed;
            }
            self.nodes.remove(&at);
            freed += 1;
            if parent == 0 {
                return freed;
            }
            self.detach(parent, at);
            at = parent;
        }
    }

    fn zombies(&self) -> usize {
        self.nodes.values().filter(|n| !n.2).count()
    }

    /// Approximate heap bytes of this model, so RSS can be read net of it.
    fn bytes(&self) -> usize {
        let per = std::mem::size_of::<(u64, (u64, Vec<u64>, bool))>() + 1;
        self.nodes.capacity() * per
            + self.nodes.values().map(|n| n.1.capacity() * 8).sum::<usize>()
    }
}

/// One op's samples inside the current window (bounded by the window size).
#[derive(Default)]
struct Op {
    us: Vec<f64>,
    count_all: u64,
    max_all: f64,
}

const OPS: [&str; 10] = [
    "fork",
    "open",
    "write",
    "pwrite",
    "trunk_write",
    "reap",
    "read_open",
    "read_own",
    "read_anc",
    "read_inh",
];

struct Timer {
    untimed: bool,
    in_window: bool,
    stall_us: f64,
    ops: [Op; 10],
    stalls: Vec<(usize, &'static str, f64, usize, usize)>,
    stall_count: u64,
}

impl Timer {
    fn time<T>(&mut self, op: usize, n_ever: usize, f: impl FnOnce() -> T) -> T {
        if self.untimed {
            return f();
        }
        let t = Instant::now();
        let out = f();
        let us = t.elapsed().as_secs_f64() * 1e6;
        let o = &mut self.ops[op];
        o.count_all += 1;
        if us > o.max_all {
            o.max_all = us;
        }
        if self.in_window {
            o.us.push(us);
        }
        if us >= self.stall_us {
            self.stall_count += 1;
            if self.stalls.len() < 20_000 {
                self.stalls.push((n_ever, OPS[op], us, 0, 0));
            }
        }
        out
    }
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[((p / 100.0) * (sorted.len() - 1) as f64).round() as usize]
}

struct TableTrack {
    full_cap: usize,
    prev_cap: usize,
    in_place: u64,
    resizes: u64,
    events: Vec<(usize, usize, usize, usize)>,
    min_cap_since_ckpt: usize,
}

impl TableTrack {
    /// Called once per cycle, outside any timed region.
    fn observe(&mut self, n_ever: usize, len: usize, cap: usize) {
        if cap > self.prev_cap + 1 {
            if cap > self.full_cap {
                self.resizes += 1;
            } else {
                self.in_place += 1;
            }
            if self.events.len() < 2000 {
                self.events.push((n_ever, len, self.prev_cap, cap));
            }
        }
        self.full_cap = self.full_cap.max(cap);
        self.min_cap_since_ckpt = self.min_cap_since_ckpt.min(cap);
        self.prev_cap = cap;
    }
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_ever.db");
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
    trunk
        .execute(format!("PRAGMA synchronous = {}", args.synchronous))
        .unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let int = |sql: &str| {
        trunk.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    };
    let (page_size, trunk_pages, synchronous) = (
        int("PRAGMA page_size"),
        int("PRAGMA page_count"),
        int("PRAGMA synchronous"),
    );
    let wal_path = PathBuf::from(format!("{}-wal", path.to_str().unwrap()));
    println!("# branch_ever — Turso fork, r11-ever (frontier/round11/r11-ever/PREREG.md)");
    println!(
        "# shape={:?} victim={:?} live={} checkpoints={:?} window={} read_every={} stall_us={} \
         untimed={} trunk_spread={} expect={:?} seed={:#x} trunk_rows={TRUNK_ROWS} \
         branch_rows={BRANCH_ROWS} value_len={VALUE_LEN} page_size={page_size} \
         trunk_pages={trunk_pages} trunk_synchronous={synchronous}",
        args.shape,
        args.victim,
        args.live,
        args.checkpoints,
        args.window,
        args.read_every,
        args.stall_us,
        args.untimed,
        args.spread,
        args.expect,
        args.seed
    );
    if !args.untimed {
        println!("# clock tick {:.0} ns (Instant); times in microseconds", clock_tick_ns());
    }
    println!(
        "# build: {} ; rss_base_bytes={}",
        if cfg!(debug_assertions) {
            "DEBUG (not a timing result)"
        } else {
            "release"
        },
        rss_bytes()
    );

    let mut rng = Rng(args.seed);
    let mut trunk_model = TrunkModel::default();
    let mut kept = KeptModel::default();
    let mut live: VecDeque<Live> = VecDeque::new();
    let mut timer = Timer {
        untimed: args.untimed,
        in_window: false,
        stall_us: args.stall_us,
        ops: Default::default(),
        stalls: Vec::new(),
        stall_count: 0,
    };
    let (_, cap0) = db.branch_table_shape();
    let mut table = TableTrack {
        full_cap: cap0,
        prev_cap: cap0,
        in_place: 0,
        resizes: 0,
        events: Vec::new(),
        min_cap_since_ckpt: usize::MAX,
    };
    let mut created = 0usize;
    let mut reads_checked = 0u64;
    let mut freed_states_total = 0usize;

    // One cycle's fork: pick the parent by shape, fork, and have the child write its own row.
    // Returns the new Live; the caller decides the victim.
    let fork_one = |created: usize,
                        live: &mut VecDeque<Live>,
                        rng: &mut Rng,
                        kept: &mut KeptModel,
                        trunk_model: &mut TrunkModel,
                        timer: &mut Timer|
     -> Live {
        let parent_idx = match args.shape {
            _ if live.is_empty() => None,
            Shape::Flat => None,
            Shape::Moran => Some(rng.below(live.len())),
            Shape::Refine => Some(live.len() - 1),
        };
        let n_ever = created + 1;
        let (branch, parent_id, parent_row, trunk_at) = match parent_idx {
            None => {
                let b = timer.time(0, n_ever, || trunk.fork_branch().unwrap());
                (b, 0u64, None, trunk_model.writes)
            }
            Some(i) => {
                let p = &live[i];
                let b = timer.time(0, n_ever, || p.branch.fork().unwrap());
                (b, p.id, Some((p.own_row, p.own())), p.trunk_writes_at_root)
            }
        };
        let id = branch.id().0;
        kept.fork(id, parent_id);
        let own_row = row_for(created);
        let conn = timer.time(1, n_ever, || branch.connect().unwrap());
        let value = branch_value(id, 0);
        timer.time(2, n_ever, || update_to(&conn, own_row, &value));
        drop(conn);
        if args.shape == Shape::Moran {
            if let Some(i) = parent_idx {
                // The parent keeps working after the fork: it rewrites its own row, so it must keep
                // the version the child saw.
                let p = &mut live[i];
                p.generation += 1;
                let conn = p.branch.connect().unwrap();
                let v = p.own();
                timer.time(3, n_ever, || update_to(&conn, p.own_row, &v));
                drop(conn);
            }
        }
        if args.spread {
            let row = spread_row(trunk_model.writes);
            let g = trunk_model.record(row);
            let sql = format!("UPDATE t SET v = '{}' WHERE id = {row}", trunk_gen_value(g));
            timer.time(4, n_ever, || trunk.execute(sql).unwrap());
        }
        Live {
            branch,
            id,
            own_row,
            generation: 0,
            parent_row,
            trunk_writes_at_root: trunk_at,
        }
    };

    // Growth to N_live.
    while live.len() < args.live {
        let l = fork_one(
            created,
            &mut live,
            &mut rng,
            &mut kept,
            &mut trunk_model,
            &mut timer,
        );
        created += 1;
        live.push_back(l);
        let (len, cap) = db.branch_table_shape();
        table.observe(created, len, cap);
    }
    // Discard growth samples: every op's statistics start at the churn.
    timer.ops = Default::default();
    timer.stalls.clear();
    timer.stall_count = 0;
    println!(
        "# grown: live={} created={} table_events_during_growth={:?}",
        live.len(),
        created,
        table.events
    );
    table.events.clear();
    table.in_place = 0;
    table.resizes = 0;
    table.min_cap_since_ckpt = usize::MAX;

    // (n_ever, per-op p50) and (n_ever, resident) for the slope table at the end.
    let mut p50s: Vec<(usize, [f64; 10])> = Vec::new();
    let mut residents: Vec<(usize, BranchResident, u64)> = Vec::new();
    let mut prev_ckpt = created;
    for &ckpt in &args.checkpoints {
        let window_start = ckpt.saturating_sub(args.window).max(prev_ckpt);
        while created < ckpt {
            timer.in_window = created >= window_start;
            let new = fork_one(
                created,
                &mut live,
                &mut rng,
                &mut kept,
                &mut trunk_model,
                &mut timer,
            );
            created += 1;
            let n_ever = created;
            let victim = match args.victim {
                Victim::Random => {
                    let k = rng.below(live.len());
                    live.swap_remove_back(k).unwrap()
                }
                Victim::Oldest => live.pop_front().unwrap(),
            };
            live.push_back(new);
            let vid = victim.id;
            let reaped = timer.time(5, n_ever, || victim.branch.reap().unwrap());
            let freed_model = kept.release(vid, args.expect);
            freed_states_total += freed_model;
            if reaped.deferred != kept.nodes.contains_key(&vid) && args.expect == Expect::Keep {
                not_a_result(&format!(
                    "cycle {n_ever}: engine deferred={} but the model {} branch {vid}",
                    reaped.deferred,
                    if kept.nodes.contains_key(&vid) { "keeps" } else { "frees" }
                ));
            }
            if (created - prev_ckpt) % args.read_every == 0 {
                let t = &live[rng.below(live.len())];
                let conn = timer.time(6, n_ever, || t.branch.connect().unwrap());
                let got_own = timer.time(7, n_ever, || read_v(&conn, t.own_row));
                if got_own != t.own() {
                    not_a_result(&format!("cycle {n_ever}: branch {} misread its own row", t.id));
                }
                if let Some((prow, pval)) = &t.parent_row {
                    let got = timer.time(8, n_ever, || read_v(&conn, *prow));
                    let want = if *prow == t.own_row { t.own() } else { pval.clone() };
                    if got != want {
                        not_a_result(&format!(
                            "cycle {n_ever}: branch {} misread its parent's row {prow}",
                            t.id
                        ));
                    }
                }
                let inh = BRANCH_ROWS + 1 + rng.below((TRUNK_ROWS - BRANCH_ROWS) as usize) as i64;
                let got = timer.time(9, n_ever, || read_v(&conn, inh));
                if got != trunk_model.value_at(inh, t.trunk_writes_at_root) {
                    not_a_result(&format!(
                        "cycle {n_ever}: branch {} misread trunk row {inh}",
                        t.id
                    ));
                }
                drop(conn);
                reads_checked += 1;
            }
            let (len, cap) = db.branch_table_shape();
            let cap_before = table.prev_cap;
            table.observe(n_ever, len, cap);
            // Attribute this cycle's stalls to the table: its capacity before and after the cycle.
            for s in timer.stalls.iter_mut().rev() {
                if s.0 != n_ever {
                    break;
                }
                s.3 = cap_before;
                s.4 = cap;
            }
            if created % 65_536 == 0 {
                let oldest = live.iter().map(|l| l.trunk_writes_at_root).min().unwrap();
                trunk_model.prune(oldest);
            }
        }
        prev_ckpt = ckpt;
        timer.in_window = false;

        // Checkpoint: the engine against the model, then every resident structure.
        let r = db.branch_resident();
        let model_states = kept.nodes.len();
        let model_zombies = kept.zombies();
        let model_children: usize = kept.nodes.values().map(|n| n.1.len()).sum();
        let roots = kept.nodes.values().filter(|n| n.0 == 0).count();
        let unowned = r
            .arena_in_use
            .wrapping_sub(r.branch_current_pages + r.branch_retained_versions + r.trunk_retained_versions);
        // Arena pages the rule predicts: every kept state holds its own row's page; under moran every
        // kept child also pins one retained version of its parent's row (the parent rewrites that
        // row after every fork). The trunk's retained versions are counted by the engine (`spread`).
        let predicted_pages = match (args.shape, args.expect) {
            (_, Expect::Splice) => None,
            (Shape::Moran, Expect::Keep) => Some(model_states + model_children),
            (_, Expect::Keep) => Some(model_states),
        };
        println!(
            "# ckpt n_ever={ckpt} live={} states={} zombies={} open={} table_len={} \
             table_capacity={} table_full_cap={} table_tombstones={} table_min_cap_window={} \
             rehash_in_place={} rehash_resize={} next_id={} trunk_epoch={} trunk_children={} \
             trunk_retained_versions={} trunk_retained_pages={} trunk_written_pages={} \
             branch_children={} branch_retained_versions={} branch_current_pages={} views={} \
             page_map_nodes={} index_mismatch={} arena_high_water={} arena_in_use={} \
             arena_free_list_len={} arena_free_list_capacity={} arena_free_bits_words={} \
             arena_chunks={} arena_unowned={} model_states={model_states} \
             model_zombies={model_zombies} model_children={model_children} model_roots={roots} \
             predicted_arena_pages={} freed_states_total={freed_states_total} \
             reads_checked={reads_checked} rss_bytes={} harness_model_bytes={} \
             harness_trunk_model_bytes={} db_bytes={} wal_bytes={} trunk_writes={}",
            live.len(),
            r.states,
            r.zombies,
            r.open,
            r.states,
            r.table_capacity,
            table.full_cap,
            table.full_cap - r.table_capacity,
            table.min_cap_since_ckpt,
            table.in_place,
            table.resizes,
            r.next_id,
            r.trunk_epoch,
            r.trunk_children,
            r.trunk_retained_versions,
            r.trunk_retained_pages,
            r.trunk_written_pages,
            r.branch_children,
            r.branch_retained_versions,
            r.branch_current_pages,
            r.views,
            r.page_map_nodes,
            r.index_mismatch,
            r.arena_high_water,
            r.arena_in_use,
            r.arena_free_list_len,
            r.arena_free_list_capacity,
            r.arena_free_bits_words,
            r.arena_chunks,
            unowned as isize,
            predicted_pages.map_or("n/a".to_string(), |p| p.to_string()),
            rss_bytes(),
            kept.bytes(),
            trunk_model.bytes(),
            std::fs::metadata(&path).map_or(0, |m| m.len()),
            std::fs::metadata(&wal_path).map_or(0, |m| m.len()),
            trunk_model.writes,
        );
        table.min_cap_since_ckpt = usize::MAX;
        if live.len() != args.live {
            not_a_result(&format!("live {} != {}", live.len(), args.live));
        }
        if r.open != 0 || r.index_mismatch || unowned != 0 {
            not_a_result(&format!(
                "open {} index_mismatch {} unowned arena slots {}",
                r.open, r.index_mismatch, unowned as isize
            ));
        }
        if r.states != model_states || r.zombies != model_zombies {
            not_a_result(&format!(
                "engine keeps {} states ({} zombies); the rule predicts {model_states} \
                 ({model_zombies})",
                r.states, r.zombies
            ));
        }
        if let Some(p) = predicted_pages {
            if r.arena_in_use != p + r.trunk_retained_versions {
                not_a_result(&format!(
                    "arena holds {} pages; the rule predicts {p} + {} trunk-retained",
                    r.arena_in_use, r.trunk_retained_versions
                ));
            }
        }
        if !args.untimed {
            let mut row = [f64::NAN; 10];
            for (i, op) in timer.ops.iter_mut().enumerate() {
                if op.us.is_empty() {
                    continue;
                }
                let mut v = std::mem::take(&mut op.us);
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                row[i] = pct(&v, 50.0);
                println!(
                    "{ckpt}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}",
                    OPS[i],
                    v.len(),
                    pct(&v, 50.0),
                    pct(&v, 90.0),
                    pct(&v, 99.0),
                    v[v.len() - 1],
                    op.max_all,
                    op.count_all
                );
            }
            p50s.push((ckpt, row));
        }
        residents.push((ckpt, r, rss_bytes()));
    }

    // Slopes against N_ever over the checkpoints: log-log, and the last step's local slope.
    let slope = |pts: &[(f64, f64)]| -> Option<(f64, f64)> {
        if pts.len() < 2 || pts.iter().any(|&(_, y)| y <= 0.0 || y.is_nan()) {
            return None;
        }
        let lp: Vec<(f64, f64)> = pts.iter().map(|&(x, y)| (x.ln(), y.ln())).collect();
        let m = lp.len() as f64;
        let (mx, my) = lp
            .iter()
            .fold((0.0, 0.0), |(a, b), (x, y)| (a + x / m, b + y / m));
        let num: f64 = lp.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
        let den: f64 = lp.iter().map(|(x, _)| (x - mx).powi(2)).sum();
        let (a, b) = (lp[lp.len() - 2], lp[lp.len() - 1]);
        Some((num / den, (b.1 - a.1) / (b.0 - a.0)))
    };
    let fmt = |s: Option<(f64, f64)>| {
        s.map_or("-\t-".to_string(), |(g, l)| format!("{g:+.3}\t{l:+.3}"))
    };
    println!("# slopes against N_ever over {:?}: log-log over all, then last step", args.checkpoints);
    if !args.untimed {
        for (i, name) in OPS.iter().enumerate() {
            let pts: Vec<(f64, f64)> = p50s.iter().map(|(x, r)| (*x as f64, r[i])).collect();
            if pts.iter().all(|p| p.1.is_nan()) {
                continue;
            }
            println!("# slope\tp50\t{name}\t{}", fmt(slope(&pts)));
        }
    }
    let counters: [(&str, fn(&BranchResident) -> usize); 14] = [
        ("states", |r| r.states),
        ("zombies", |r| r.zombies),
        ("table_capacity", |r| r.table_capacity),
        ("trunk_children", |r| r.trunk_children),
        ("trunk_retained_versions", |r| r.trunk_retained_versions),
        ("trunk_written_pages", |r| r.trunk_written_pages),
        ("branch_children", |r| r.branch_children),
        ("branch_retained_versions", |r| r.branch_retained_versions),
        ("branch_current_pages", |r| r.branch_current_pages),
        ("page_map_nodes", |r| r.page_map_nodes),
        ("arena_high_water", |r| r.arena_high_water),
        ("arena_in_use", |r| r.arena_in_use),
        ("arena_free_list_capacity", |r| r.arena_free_list_capacity),
        ("arena_chunks", |r| r.arena_chunks),
    ];
    for (name, f) in counters {
        let pts: Vec<(f64, f64)> = residents
            .iter()
            .map(|(x, r, _)| (*x as f64, f(r) as f64))
            .collect();
        println!("# slope\tcounter\t{name}\t{}", fmt(slope(&pts)));
    }
    let pts: Vec<(f64, f64)> = residents
        .iter()
        .map(|(x, _, rss)| (*x as f64, *rss as f64))
        .collect();
    println!("# slope\trss\trss_bytes\t{}", fmt(slope(&pts)));
    if !args.untimed {
        println!(
            "# stalls >= {} us: {} (listed: {})",
            args.stall_us,
            timer.stall_count,
            timer.stalls.len()
        );
        for (n, op, us, c0, c1) in &timer.stalls {
            println!("# stall n_ever={n} op={op} us={us:.1} table_cap_before={c0} table_cap_after={c1}");
        }
    }
    println!(
        "# table events (n_ever, len, cap_before, cap_after), first {}: {:?}",
        table.events.len(),
        table.events
    );

    // Teardown: drop every live branch, oldest first, timing each drop (untimed: counting only). The
    // last drop may free every kept ancestor at once.
    let mut max_us = 0.0f64;
    let mut max_freed = 0usize;
    let before = db.branch_resident().states;
    let mut n = 0;
    while let Some(l) = live.pop_front() {
        let s0 = db.branch_stats().live_branches;
        let t = (!args.untimed).then(Instant::now);
        l.branch.reap().unwrap();
        let us = t.map_or(0.0, |t| t.elapsed().as_secs_f64() * 1e6);
        let freed = s0 - db.branch_stats().live_branches;
        if us > max_us {
            max_us = us;
        }
        max_freed = max_freed.max(freed);
        n += 1;
    }
    let end = db.branch_stats();
    println!(
        "# teardown: {n} drops freed {before} states; largest single drop freed {max_freed} states; \
         slowest drop {max_us:.1} us; end {end:?}"
    );
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
}
