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
//! every resident structure of the store (`Database::branch_resident`), then exits `NOT A RESULT`
//! (rc 1, after that line) unless the engine's kept states and zombies equal the harness's own
//! model of the reclamation rule, no arena slot is unowned, and (under `--expect keep` only) the
//! arena pages equal the rule's count; under `--expect splice` the arena is not modelled. A read
//! that disagrees with the model exits the same way at once. Unless `--untimed`, it also prints
//! per-op latency over the last `--window` cycles before the checkpoint, and every sample at or
//! above `--stall-us`. With `--untimed` no clock is read: the run prints integers only.
//!
//! Every value a branch writes is keyed by the harness's own count of branches created, never by
//! the engine's id: since F8 an id is `generation << 32 | slot`, which passes 12 digits after a
//! slot's ~233rd reuse, and a longer value is no longer an in-place rewrite (review finding H1).
//!
//! Every cycle, outside any timed region, it reads the branch table's `(len, capacity)`. Before F8,
//! capacity was items plus growth left, so at a fixed item count it fell by one per hashbrown
//! tombstone and jumped back at a rehash; rehashes are counted (in place vs resize) and tombstones
//! printed. Since F8 capacity is slots allocated, so tombstones read 0 and a jump is a chunk append.
//!
//! On this store (resolve-vol-bushy-ever, merging r11-adv-x-f7fix): the volatile store's successor,
//! where the F7 splice is an ARM, off by default. `--expect splice|keep` therefore also SELECTS the
//! arm (`DatabaseOpts::with_branch_splice`), so the model and the store follow one rule. The keep
//! arm is not the 0f4232957 store either: a released branch with a live child is RETIRED (F4's
//! `retire_current`: its current versions born after its newest live child's fork are freed at the
//! release), so under `--shape moran --expect keep` a zombie holds no page of its own row, which the
//! arena prediction follows (see `predicted_pages`). The table is F8' (ids are never reused and a
//! chunk is freed when its states are all gone), so `table_capacity` can FALL: `table_tombstones`
//! (full capacity minus capacity) then counts the slots of freed chunks, not tombstones, and
//! `next_id` is an id again, one more than the branches ever created. `branch_stats` and
//! `branch_resident` return `Result` here.

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
/// Trunk rows per leaf in the seeded table (20,000 rows of 100-byte values fill 541 leaves:
/// `trunk_retained_pages=541` in raw/uf8b_e3_1k.txt). Used only by `TrunkModel::floor_versions`,
/// whose agreement with the engine's `trunk_retained_versions` checks it.
const ROWS_PER_LEAF: i64 = 37;
/// Rows above the parent's that a live branch reads through deeper ancestors, checked by the
/// periodic read (r11-ever-refute's coverage caveat (ii): those are the pages a splice moves).
const ANCESTOR_ROWS: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shape {
    Flat,
    Moran,
    Refine,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Victim {
    Random,
    Oldest,
    /// Earliest deadline first: every branch draws a lifetime from `Law` at birth (mean N_live
    /// cycles), and each cycle reaps the live branch whose birth + lifetime is smallest, so the live
    /// count stays fixed while the order of deaths follows the law (r11-ever amendment 13, after
    /// r11-ever-refute's ever_heavy.py `edf:LAW`, whose samplers this copies).
    Edf(Law),
}

/// A lifetime law with mean `n` cycles (r11-ever-refute w/ever_heavy.py `lifetime_sampler`).
#[derive(Clone, Copy, PartialEq, Debug)]
enum Law {
    Exp,
    /// Shape a, scale n (a - 1) / a.
    Pareto(f64),
    /// With probability p an exponential of mean k m, else of mean m; m = n / (p k + 1 - p).
    Hyper(f64, f64),
    /// Lognormal with shape s and mean n.
    Lognorm(f64),
}

impl Law {
    fn parse(spec: &str) -> Law {
        let parts: Vec<&str> = spec.split(':').collect();
        let num = |i: usize| -> f64 {
            parts
                .get(i)
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| die(&format!("bad lifetime law {spec}")))
        };
        match parts[0] {
            "exp" => Law::Exp,
            "pareto" => Law::Pareto(num(1)),
            "hyper" => Law::Hyper(num(1), num(2)),
            "lognorm" => Law::Lognorm(num(1)),
            o => die(&format!("unknown lifetime law {o}")),
        }
    }

    fn sample(self, n: f64, rng: &mut Rng) -> f64 {
        match self {
            Law::Exp => -rng.unit().ln() * n,
            Law::Pareto(a) => n * (a - 1.0) / a * rng.unit().powf(-1.0 / a),
            Law::Hyper(p, k) => {
                let m = n / (p * k + 1.0 - p);
                let mean = if rng.unit() < p { k * m } else { m };
                -rng.unit().ln() * mean
            }
            Law::Lognorm(sigma) => {
                let mu = n.ln() - sigma * sigma / 2.0;
                // Box-Muller.
                let z = (-2.0 * rng.unit().ln()).sqrt()
                    * (2.0 * std::f64::consts::PI * rng.unit()).cos();
                (mu + sigma * z).exp()
            }
        }
    }
}

/// A deadline ordered by `f64::total_cmp`, for the EDF heap.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Deadline(f64);
impl Eq for Deadline {}
impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Expect {
    /// The store as of 0f4232957: a state is kept while its handle lives or it has a kept child.
    /// Here, the store's splice arm OFF (its retirement frees more pages, not more states).
    Keep,
    /// Pre-registered fix F7: a zombie with exactly one kept child is spliced out. Here, the
    /// store's splice arm ON.
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
                let v = val();
                a.victim = match v.as_str() {
                    "random" => Victim::Random,
                    "oldest" => Victim::Oldest,
                    o if o.starts_with("edf:") => Victim::Edf(Law::parse(&o[4..])),
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
    if a.seed == 0 {
        // The workload stream's salt is 0, so seed 0 is xorshift's zero state: every draw 0.
        die("--seed 0 is refused: the workload stream would be xorshift's zero state");
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
    /// Uniform in (0, 1].
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }
    /// A stream derived from `seed` for one purpose; xorshift needs a nonzero state.
    fn stream(seed: u64, salt: u64) -> Rng {
        let s = seed ^ salt;
        Rng(if s == 0 { salt } else { s })
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

    /// E6 (r11-ever amendment 13): the trunk page versions that some live branch's root fork reads
    /// and that are no longer current, from the harness's own write history and fork points alone
    /// (`forks`: every live branch's `trunk_writes_at_root`, sorted). A fork at write count `w` reads
    /// the version its page had after the writes `g < w`, so the version a write `g` replaced is read
    /// by the forks in `(previous write, g]`. This is the page-level minimum ANY exact-snapshot store
    /// must hold for the trunk (r11-space-refute's m[S, rho, R] at page grain, for this run's fork
    /// times), so an engine whose trunk retention is exact reports it as `trunk_retained_versions`.
    /// `prune` keeps, per row, the last write before the oldest fork: enough, since no fork reads an
    /// older version. Cost: the kept history, at checkpoints only.
    fn floor_versions(&self, forks: &[u64]) -> u64 {
        let mut pages: HashMap<i64, Vec<u64>> = HashMap::new();
        for (&row, h) in &self.history {
            pages
                .entry((row - 1) / ROWS_PER_LEAF)
                .or_default()
                .extend_from_slice(h);
        }
        let mut floor = 0;
        for writes in pages.values_mut() {
            writes.sort_unstable();
            let mut after: Option<u64> = None;
            for &g in writes.iter() {
                let first = after.map_or(0, |a| forks.partition_point(|&f| f <= a));
                if first < forks.len() && forks[first] <= g {
                    floor += 1;
                }
                after = Some(g);
            }
        }
        floor
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
    /// The engine's id, for the kept-state model.
    id: u64,
    /// This branch's number among all branches ever created (1-based), for its row values: before
    /// F8 it equalled the engine's id.
    tag: u64,
    own_row: i64,
    generation: u64,
    parent_row: Option<(i64, String)>,
    /// Up to `ANCESTOR_ROWS` rows written by ancestors above the parent, nearest first, each with the
    /// value this branch must read (its nearest writer's, as of each fork on the way down). No
    /// ancestor between that writer and this branch wrote the row: each branch writes only its own.
    ancestors: Vec<(i64, String)>,
    trunk_writes_at_root: u64,
    /// Birth cycle plus the drawn lifetime (EDF victims only; 0 otherwise).
    deadline: f64,
}

impl Live {
    fn own(&self) -> String {
        branch_value(self.tag, self.generation)
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
        DatabaseOpts::new().with_branch_splice(args.expect == Expect::Splice),
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

    // Three RNG streams (r11-ever amendment 13): the workload's parent and uniform-victim draws, the
    // lifetime draws, and the periodic read's draws. So neither the read cadence nor the checkpoint
    // list can move the fork and reap sequence, and a replicate differs only by --seed.
    let mut rng = Rng::stream(args.seed, 0);
    let mut lrng = Rng::stream(args.seed, 0xA076_1D64_78BD_642F);
    let mut rrng = Rng::stream(args.seed, 0xE703_7ED1_A0B4_28DB);
    // EDF bookkeeping: deadlines in a min-heap, and each live branch's index in `live` by tag.
    let mut edf_heap: std::collections::BinaryHeap<std::cmp::Reverse<(Deadline, u64)>> =
        std::collections::BinaryHeap::new();
    let mut edf_pos: HashMap<u64, usize> = HashMap::new();
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
        let (branch, parent_id, parent_row, ancestors, trunk_at) = match parent_idx {
            None => {
                let b = timer.time(0, n_ever, || trunk.fork_branch().unwrap());
                (b, 0u64, None, Vec::new(), trunk_model.writes)
            }
            Some(i) => {
                let p = &live[i];
                let b = timer.time(0, n_ever, || p.branch.fork().unwrap());
                // The parent's parent row and its own ancestors, nearest first, minus any row the
                // parent itself writes (its value is `parent_row`'s), one entry per row.
                let mut ancestors: Vec<(i64, String)> = Vec::with_capacity(ANCESTOR_ROWS);
                for (row, v) in p.parent_row.iter().chain(p.ancestors.iter()) {
                    if ancestors.len() == ANCESTOR_ROWS {
                        break;
                    }
                    if *row != p.own_row && ancestors.iter().all(|(r, _)| r != row) {
                        ancestors.push((*row, v.clone()));
                    }
                }
                (b, p.id, Some((p.own_row, p.own())), ancestors, p.trunk_writes_at_root)
            }
        };
        let id = branch.id().0;
        let tag = created as u64 + 1;
        kept.fork(id, parent_id);
        let own_row = row_for(created);
        let conn = timer.time(1, n_ever, || branch.connect().unwrap());
        let value = branch_value(tag, 0);
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
            tag,
            own_row,
            generation: 0,
            parent_row,
            ancestors,
            trunk_writes_at_root: trunk_at,
            deadline: 0.0,
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
        let mut l = l;
        if let Victim::Edf(law) = args.victim {
            l.deadline = created as f64 + law.sample(args.live as f64, &mut lrng);
            edf_heap.push(std::cmp::Reverse((Deadline(l.deadline), l.tag)));
            edf_pos.insert(l.tag, live.len());
        }
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
                Victim::Edf(_) => {
                    let std::cmp::Reverse((_, tag)) = edf_heap.pop().expect("a live branch");
                    let k = edf_pos.remove(&tag).expect("indexed");
                    let v = live.swap_remove_back(k).unwrap();
                    if let Some(moved) = live.get(k) {
                        edf_pos.insert(moved.tag, k);
                    }
                    v
                }
            };
            let mut new = new;
            if let Victim::Edf(law) = args.victim {
                new.deadline = (created - 1) as f64 + law.sample(args.live as f64, &mut lrng);
                edf_heap.push(std::cmp::Reverse((Deadline(new.deadline), new.tag)));
                edf_pos.insert(new.tag, live.len());
            }
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
            if created % args.read_every == 0 {
                let t = &live[rrng.below(live.len())];
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
                if !t.ancestors.is_empty() {
                    // Untimed: no op slot is spent on it, and the read stream alone picks the row.
                    let (arow, aval) = &t.ancestors[rrng.below(t.ancestors.len())];
                    let got = read_v(&conn, *arow);
                    let want = if *arow == t.own_row { t.own() } else { aval.clone() };
                    if got != want {
                        not_a_result(&format!(
                            "cycle {n_ever}: branch {} misread ancestor row {arow}",
                            t.id
                        ));
                    }
                }
                let inh = BRANCH_ROWS + 1 + rrng.below((TRUNK_ROWS - BRANCH_ROWS) as usize) as i64;
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
        let r = db.branch_resident().unwrap();
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
        // On this store's keep arm a zombie is retired at its release: under moran its row was last
        // rewritten after its newest fork, so that version is freed and a zombie holds only the
        // versions its kept children pin (on 0f4232957 it was `model_states + model_children`).
        // Under flat there are no zombies, and under refine a parent never rewrites after a fork,
        // so its row is kept: `model_states` for both, as before.
        let predicted_pages = match (args.shape, args.expect) {
            (_, Expect::Splice) => None,
            (Shape::Moran, Expect::Keep) => Some(model_states - model_zombies + model_children),
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
             arena_chunks={} arena_unowned={} visible_slots={} waste_zombie_current_after_last_fork={} \
             waste_zombie_current_shadowed={} waste_zombie_retained={} waste_live_retained={} \
             waste_trunk_retained={} waste_live_current={} model_states={model_states} \
             model_zombies={model_zombies} model_children={model_children} model_roots={roots} \
             predicted_arena_pages={} freed_states_total={freed_states_total} \
             reads_checked={reads_checked} rss_bytes={} harness_model_bytes={} \
             harness_trunk_model_bytes={} db_bytes={} wal_bytes={} trunk_writes={} oldest_live_age={} \
             trunk_floor_versions={}",
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
            r.visible_slots,
            r.waste_zombie_current_after_last_fork,
            r.waste_zombie_current_shadowed,
            r.waste_zombie_retained,
            r.waste_live_retained,
            r.waste_trunk_retained,
            r.waste_live_current,
            predicted_pages.map_or("n/a".to_string(), |p| p.to_string()),
            rss_bytes(),
            kept.bytes(),
            trunk_model.bytes(),
            std::fs::metadata(&path).map_or(0, |m| m.len()),
            std::fs::metadata(&wal_path).map_or(0, |m| m.len()),
            trunk_model.writes,
            ckpt as u64 + 1 - live.iter().map(|l| l.tag).min().unwrap_or(ckpt as u64 + 1),
            {
                let mut forks: Vec<u64> = live.iter().map(|l| l.trunk_writes_at_root).collect();
                forks.sort_unstable();
                trunk_model.floor_versions(&forks)
            },
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
        let waste_parts = r.waste_zombie_current_after_last_fork
            + r.waste_zombie_current_shadowed
            + r.waste_zombie_retained
            + r.waste_live_retained
            + r.waste_trunk_retained
            + r.waste_live_current;
        if waste_parts != r.arena_in_use - r.visible_slots || r.waste_live_current != 0 {
            not_a_result(&format!(
                "waste parts sum to {waste_parts}, arena - visible is {}; live current waste {}",
                r.arena_in_use - r.visible_slots,
                r.waste_live_current
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
    let counters: [(&str, fn(&BranchResident) -> usize); 15] = [
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
        ("visible_slots", |r| r.visible_slots),
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
    let before = db.branch_resident().unwrap().states;
    let mut n = 0;
    while let Some(l) = live.pop_front() {
        let s0 = db.branch_stats().unwrap().live_branches;
        let t = (!args.untimed).then(Instant::now);
        l.branch.reap().unwrap();
        let us = t.map_or(0.0, |t| t.elapsed().as_secs_f64() * 1e6);
        let freed = s0 - db.branch_stats().unwrap().live_branches;
        if us > max_us {
            max_us = us;
        }
        max_freed = max_freed.max(freed);
        n += 1;
    }
    let end = db.branch_stats().unwrap();
    println!(
        "# teardown: {n} drops freed {before} states; largest single drop freed {max_freed} states; \
         slowest drop {max_us:.1} us; end {end:?}"
    );
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
}
