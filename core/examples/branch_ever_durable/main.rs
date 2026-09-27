//! Branches EVER created against branches LIVE on the DURABLE branch store (lane r11-ever, PREREG
//! amendments 13-14: r11-ever-refute's item 5). A port of the volatile store's `branch_ever` (turso
//! branch r11-ever @ 9e206a60b): the same cycle, shapes, victims and seed streams, the same model of
//! the kept states (the F7 splice rule, or the base's) and the same read checks, run against the F7 port on the
//! COMPOSED durable store (branch r11-ever-durable-cat, from a12's d7a2b8f6e: F1/F2/F4 + catalog
//! v3/v4 + C-P + C-R), in snapshot mode or catalog mode. (First written for the port on the pre-F1
//! store, r11-ever-durable ae3309970, whose counters this base does not have.)
//!
//!   cargo run -p turso_core --release --example branch_ever_durable -- \
//!       --shape flat|moran|refine --victim random|oldest --live N --checkpoints a,b,... \
//!       --splice on|off [--durability durable|durable-nosync|catalog|catalog-nosync] [--seed S] \
//!       [--read-every K] [--window W] [--untimed] [--reopen]
//!
//! ⚠ UNBUILT when committed: written under quiet mode (no local compute); never compiled or run.
//!
//! Every cycle forks ONE branch (from the trunk, a random live branch, or the newest) that writes
//! its own row, and reaps ONE live branch (a random one drawn before the new one joins, or the
//! oldest); under `moran` the parent then rewrites its own row. The workload stream draws in the
//! volatile harness's order, so a seed gives the same fork and reap sequence there and here, and
//! the kept states must agree between the two stores at every checkpoint.
//!
//! At each checkpoint it prints the store's own counts (kept branch states, arena slots in use),
//! the durable files' sizes (log, snapshot, arena, catalog and its WAL), the store's work counters
//! (`BranchWork`: splices and what they visited; resolutions and the nodes they consulted, at most
//! two per read under the page maps), the catalog's rows written, and, over the window before the
//! checkpoint, unless `--untimed`, fork and reap latency (a checkpoint or compaction inside the
//! window shows in those tails: this base has no compaction counter). It exits `NOT A
//! RESULT` (rc 1) unless the kept states equal the harness's model of the rule, every reap's
//! `deferred` equals the model's (kept, or spliced into its child), and every sampled read (own
//! row, parent's row, one of up to 4 deeper ancestors' rows, one trunk-only row) matches.
//!
//! `--splice on|off` is REQUIRED (r11-ever amendment 15: the splice is an arm of the store, off by
//! default there): it opens the store in the F7 splice arm or not, and the kept-state model follows
//! the same rule (the volatile harness's `--expect splice` or `--expect keep`). The header names it.
//!
//! With `--reopen`, after the last checkpoint every live branch is detached, every holder of the
//! database is dropped, and the database is reopened (recovery loads the snapshot and replays the
//! log): the harness asserts the same branch ids, the same arena slots in use and the same kept
//! count, prints the reopen time, and re-checks every live branch's own and parent rows.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{Branch, BranchDurability, BranchId};
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO,
};

const TRUNK_ROWS: i64 = 20_000;
/// Branches write rows 1..=BRANCH_ROWS; the rows above are read as the trunk's, never written.
const BRANCH_ROWS: i64 = 18_000;
const VALUE_LEN: usize = 100;
/// Rows above the parent's that a live branch reads through deeper ancestors (the pages a splice
/// moves), checked by the periodic read.
const ANCESTOR_ROWS: usize = 4;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Shape {
    Flat,
    Moran,
    Refine,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Victim {
    Random,
    Oldest,
}

struct Args {
    shape: Shape,
    victim: Victim,
    live: usize,
    checkpoints: Vec<usize>,
    window: usize,
    read_every: usize,
    untimed: bool,
    seed: u64,
    durability: BranchDurability,
    splice: bool,
    reopen: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        shape: Shape::Flat,
        victim: Victim::Random,
        live: 1000,
        checkpoints: vec![],
        window: 5000,
        read_every: 10,
        untimed: false,
        seed: 0x9E37_79B9_7F4A_7C15,
        durability: BranchDurability::Durable { sync: true },
        splice: false,
        reopen: false,
    };
    let mut shape = None;
    let mut splice = None;
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
            "--durability" => {
                a.durability = match val().as_str() {
                    "durable" => BranchDurability::Durable { sync: true },
                    "durable-nosync" => BranchDurability::Durable { sync: false },
                    "catalog" => BranchDurability::Catalog { sync: true },
                    "catalog-nosync" => BranchDurability::Catalog { sync: false },
                    o => die(&format!(
                        "--durability must be durable, durable-nosync, catalog or catalog-nosync, not {o}"
                    )),
                }
            }
            "--splice" => {
                splice = Some(match val().as_str() {
                    "on" => true,
                    "off" => false,
                    o => die(&format!("--splice must be on or off, not {o}")),
                })
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
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--untimed" => a.untimed = true,
            "--reopen" => a.reopen = true,
            o => die(&format!("unknown argument {o}")),
        }
    }
    a.shape = shape.unwrap_or_else(|| die("--shape is required"));
    a.splice = splice.unwrap_or_else(|| die("--splice on|off is required: the arm is named, never defaulted"));
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
    eprintln!("branch_ever_durable: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

/// The volatile harness's generator, streams and all (turso r11-ever 064462ae6).
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
    /// A stream derived from `seed` for one purpose; xorshift needs a nonzero state.
    fn stream(seed: u64, salt: u64) -> Rng {
        let s = seed ^ salt;
        Rng(if s == 0 { salt } else { s })
    }
}

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

/// Branch `tag`'s `generation`-th value: same length every time, so every write is in place.
fn branch_value(tag: u64, generation: u64) -> String {
    format!("b{tag:0>12}g{generation:0>86}")
}

/// The row the `n`-th branch ever created writes.
fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % BRANCH_ROWS as u64) as i64 + 1
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

fn size_of(path: &Path, suffix: &str) -> u64 {
    std::fs::metadata(format!("{}{suffix}", path.display())).map_or(0, |m| m.len())
}

fn open_db(path: &Path, args: &Args) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new()
            .with_branch_durability(args.durability)
            .with_branch_splice(args.splice),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// A live branch and what it must read (the volatile harness's `Live`, trunk writes left out: the
/// trunk is written only at setup here).
struct Live {
    branch: Branch,
    /// The engine's id, for the kept-state model.
    id: u64,
    /// This branch's number among all branches ever created (1-based), for its row values.
    tag: u64,
    own_row: i64,
    generation: u64,
    parent_row: Option<(i64, String)>,
    /// Up to `ANCESTOR_ROWS` rows written by ancestors above the parent, nearest first, each with
    /// the value this branch must read. Each branch writes only its own row.
    ancestors: Vec<(i64, String)>,
}

impl Live {
    fn own(&self) -> String {
        branch_value(self.tag, self.generation)
    }
}

/// The harness's model of which branch states the store keeps, from the reclamation rule alone:
/// a state is kept while it has a handle or a kept child, and, in the splice arm, a released one
/// left with exactly one kept child is spliced out, the child taking its place (the volatile
/// harness's model under `--expect splice`; off, `--expect keep`).
#[derive(Default)]
struct KeptModel {
    splice: bool,
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

    /// Release `id`'s handle and apply the rule; returns the states freed whole, and whether `id`
    /// itself was spliced out.
    fn release(&mut self, id: u64) -> (usize, bool) {
        self.nodes.get_mut(&id).unwrap().2 = false;
        let mut freed = 0;
        let mut at = id;
        loop {
            let (parent, kids, handle) = match self.nodes.get(&at) {
                Some((p, k, h)) => (*p, k.len(), *h),
                None => return (freed, false),
            };
            if handle {
                return (freed, false);
            }
            if kids > 0 {
                if kids == 1 && self.splice {
                    let child = self.nodes[&at].1[0];
                    self.nodes.remove(&at);
                    self.nodes.get_mut(&child).unwrap().0 = parent;
                    if parent != 0 {
                        let pk = &mut self.nodes.get_mut(&parent).unwrap().1;
                        let i = pk.iter().position(|&k| k == at).unwrap();
                        pk[i] = child;
                    }
                    return (freed, at == id);
                }
                return (freed, false);
            }
            self.nodes.remove(&at);
            freed += 1;
            if parent == 0 {
                return (freed, false);
            }
            self.detach(parent, at);
            at = parent;
        }
    }

    fn zombies(&self) -> usize {
        self.nodes.values().filter(|n| !n.2).count()
    }
}

/// Fork and reap latencies inside the window before a checkpoint (none with `--untimed`).
#[derive(Default)]
struct Window {
    on: bool,
    fork_ns: Vec<u64>,
    reap_ns: Vec<u64>,
}

impl Window {
    fn time<T>(&mut self, reap: bool, f: impl FnOnce() -> T) -> T {
        if !self.on {
            return f();
        }
        let t = Instant::now();
        let out = f();
        let ns = t.elapsed().as_nanos() as u64;
        if reap {
            self.reap_ns.push(ns);
        } else {
            self.fork_ns.push(ns);
        }
        out
    }
}

/// p50, p99 and max of `v`, or dashes when there is none.
fn tails(v: &mut [u64]) -> (String, String, String) {
    if v.is_empty() {
        return ("-".into(), "-".into(), "-".into());
    }
    v.sort_unstable();
    let at = |p: f64| v[((p * (v.len() - 1) as f64).round()) as usize].to_string();
    (at(0.50), at(0.99), v[v.len() - 1].to_string())
}

#[allow(clippy::too_many_arguments)]
fn fork_one(
    args: &Args,
    trunk: &Arc<Connection>,
    created: usize,
    live: &mut VecDeque<Live>,
    rng: &mut Rng,
    kept: &mut KeptModel,
    win: &mut Window,
) -> Live {
    let parent_idx = match args.shape {
        _ if live.is_empty() => None,
        Shape::Flat => None,
        Shape::Moran => Some(rng.below(live.len())),
        Shape::Refine => Some(live.len() - 1),
    };
    let (branch, parent_id, parent_row, ancestors) = match parent_idx {
        None => {
            let b = win.time(false, || trunk.fork_branch().unwrap());
            (b, 0u64, None, Vec::new())
        }
        Some(i) => {
            let p = &live[i];
            let b = win.time(false, || p.branch.fork().unwrap());
            let mut ancestors: Vec<(i64, String)> = Vec::with_capacity(ANCESTOR_ROWS);
            for (row, v) in p.parent_row.iter().chain(p.ancestors.iter()) {
                if ancestors.len() == ANCESTOR_ROWS {
                    break;
                }
                if *row != p.own_row && ancestors.iter().all(|(r, _)| r != row) {
                    ancestors.push((*row, v.clone()));
                }
            }
            (b, p.id, Some((p.own_row, p.own())), ancestors)
        }
    };
    let id = branch.id().0;
    let tag = created as u64 + 1;
    kept.fork(id, parent_id);
    let own_row = row_for(created);
    let conn = branch.connect().unwrap();
    update_to(&conn, own_row, &branch_value(tag, 0));
    drop(conn);
    if args.shape == Shape::Moran {
        if let Some(i) = parent_idx {
            // The parent keeps working after the fork: it rewrites its own row.
            let p = &mut live[i];
            p.generation += 1;
            let conn = p.branch.connect().unwrap();
            let v = p.own();
            update_to(&conn, p.own_row, &v);
            drop(conn);
        }
    }
    Live {
        branch,
        id,
        tag,
        own_row,
        generation: 0,
        parent_row,
        ancestors,
    }
}

/// The periodic read: own row, parent's row, one deeper ancestor's row, one trunk-only row.
fn check_reads(t: &Live, rrng: &mut Rng, cycle: usize) {
    let conn = t.branch.connect().unwrap();
    if read_v(&conn, t.own_row) != t.own() {
        not_a_result(&format!("cycle {cycle}: branch {} misread its own row", t.id));
    }
    if let Some((prow, pval)) = &t.parent_row {
        let want = if *prow == t.own_row { t.own() } else { pval.clone() };
        if read_v(&conn, *prow) != want {
            not_a_result(&format!("cycle {cycle}: branch {} misread its parent's row {prow}", t.id));
        }
    }
    if !t.ancestors.is_empty() {
        let (arow, aval) = &t.ancestors[rrng.below(t.ancestors.len())];
        let want = if *arow == t.own_row { t.own() } else { aval.clone() };
        if read_v(&conn, *arow) != want {
            not_a_result(&format!("cycle {cycle}: branch {} misread ancestor row {arow}", t.id));
        }
    }
    let inh = BRANCH_ROWS + 1 + rrng.below((TRUNK_ROWS - BRANCH_ROWS) as usize) as i64;
    if read_v(&conn, inh) != trunk_value(inh) {
        not_a_result(&format!("cycle {cycle}: branch {} misread trunk row {inh}", t.id));
    }
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_ever_durable.db");
    let mut db = open_db(&path, &args);
    let mut trunk = db.connect().unwrap();
    // The trunk is written only at setup; its sync mode is not on any measured path.
    trunk.execute("PRAGMA synchronous = OFF").unwrap();
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
    println!("# branch_ever_durable — Turso fork, DURABLE branch store with the F7 port (r11-ever)");
    println!(
        "# shape={:?} victim={:?} live={} checkpoints={:?} window={} read_every={} untimed={} \
         durability={:?} splice={} reopen={} seed={:#x} trunk_rows={TRUNK_ROWS} branch_rows={BRANCH_ROWS}",
        args.shape,
        args.victim,
        args.live,
        args.checkpoints,
        args.window,
        args.read_every,
        args.untimed,
        args.durability,
        if args.splice { "on" } else { "off" },
        args.reopen,
        args.seed
    );

    // The volatile harness's streams: the workload, and the reads (lifetimes are not used here).
    let mut rng = Rng::stream(args.seed, 0);
    let mut rrng = Rng::stream(args.seed, 0xE703_7ED1_A0B4_28DB);
    let mut live: VecDeque<Live> = VecDeque::new();
    let mut kept = KeptModel {
        splice: args.splice,
        ..KeptModel::default()
    };
    let mut win = Window::default();
    let mut created = 0usize;
    let mut reads_checked = 0u64;
    while live.len() < args.live {
        let l = fork_one(&args, &trunk, created, &mut live, &mut rng, &mut kept, &mut win);
        created += 1;
        live.push_back(l);
    }
    println!("# grown: live={} created={created}", live.len());

    let mut prev_ckpt = created;
    for &ckpt in &args.checkpoints {
        let window_start = ckpt.saturating_sub(args.window).max(prev_ckpt);
        win = Window::default();
        while created < ckpt {
            win.on = !args.untimed && created >= window_start;
            let new = fork_one(&args, &trunk, created, &mut live, &mut rng, &mut kept, &mut win);
            created += 1;
            let victim = match args.victim {
                Victim::Random => {
                    let k = rng.below(live.len());
                    live.swap_remove_back(k).unwrap()
                }
                Victim::Oldest => live.pop_front().unwrap(),
            };
            live.push_back(new);
            let vid = victim.id;
            let reaped = win.time(true, || victim.branch.reap().unwrap());
            let (_, spliced) = kept.release(vid);
            let model_deferred = spliced || kept.nodes.contains_key(&vid);
            if reaped.deferred != model_deferred {
                not_a_result(&format!(
                    "cycle {created}: engine deferred={} but the model {} branch {vid}",
                    reaped.deferred,
                    if model_deferred { "keeps or splices" } else { "frees" }
                ));
            }
            if created % args.read_every == 0 {
                let t = &live[rrng.below(live.len())];
                check_reads(t, &mut rrng, created);
                reads_checked += 1;
            }
        }
        prev_ckpt = created;
        let stats = db.branch_stats().unwrap();
        let w = stats.work;
        let (fork50, fork99, forkmax) = tails(&mut win.fork_ns);
        let (reap50, reap99, reapmax) = tails(&mut win.reap_ns);
        println!(
            "# ckpt n_ever={ckpt} live={} kept={} model_states={} model_zombies={} \
             arena_in_use={} arena_free={} splices={} splice_commits={} splice_entries={} \
             resolve_calls={} resolve_levels={} gc_range_entries={} catalog_rows_written={} \
             log_bytes={} snap_bytes={} arena_bytes={} cat_bytes={} cat_wal_bytes={} rss_bytes={} \
             fork_p50_ns={fork50} fork_p99_ns={fork99} fork_max_ns={forkmax} reap_p50_ns={reap50} \
             reap_p99_ns={reap99} reap_max_ns={reapmax} reads_checked={reads_checked}",
            live.len(),
            stats.live_branches,
            kept.nodes.len(),
            kept.zombies(),
            stats.arena_slots_in_use,
            stats.arena_slots_free,
            w.splices,
            w.splice_commits,
            w.splice_entries,
            w.resolve_calls,
            w.resolve_levels,
            w.gc_range_entries,
            db.branch_catalog_rows_written(),
            size_of(&path, "-branch-log"),
            size_of(&path, "-branch-snap"),
            size_of(&path, "-branch-arena"),
            size_of(&path, "-branch-cat"),
            size_of(&path, "-branch-cat-wal"),
            rss_bytes(),
        );
        if stats.live_branches != kept.nodes.len() {
            not_a_result(&format!(
                "n_ever {ckpt}: the store keeps {} branch states, the rule {}",
                stats.live_branches,
                kept.nodes.len()
            ));
        }
    }

    if args.reopen {
        let mut ids: Vec<BranchId> = live.iter().map(|l| l.branch.id()).collect();
        ids.sort();
        let mut slots = db.branch_slots_in_use();
        slots.sort_unstable();
        let kept_before = db.branch_stats().unwrap().live_branches;
        let detached: Vec<(BranchId, i64, String, Option<(i64, String)>)> = live
            .drain(..)
            .map(|l| {
                let own = l.own();
                let Live {
                    branch,
                    own_row,
                    parent_row,
                    ..
                } = l;
                (branch.into_id(), own_row, own, parent_row)
            })
            .collect();
        // Every holder of the Database is gone, so the open below is a real one.
        drop(trunk);
        drop(db);
        let t = Instant::now();
        db = open_db(&path, &args);
        trunk = db.connect().unwrap();
        let reopen_ns = t.elapsed().as_nanos();
        let o = db.branch_open_stats();
        let mut ids_after = db.branch_ids().unwrap();
        ids_after.sort();
        let mut slots_after = db.branch_slots_in_use();
        slots_after.sort_unstable();
        let kept_after = db.branch_stats().unwrap().live_branches;
        println!(
            "# reopen ns={reopen_ns} ids_equal={} slots_equal={} kept_before={kept_before} \
             kept_after={kept_after} log_bytes={} snap_bytes={} cat_bytes={} open_stats={o:?}",
            ids_after == ids,
            slots_after == slots,
            size_of(&path, "-branch-log"),
            size_of(&path, "-branch-snap"),
            size_of(&path, "-branch-cat"),
        );
        if ids_after != ids || slots_after != slots || kept_after != kept_before {
            not_a_result("the reopen recovered a different branch set, slot set or kept count");
        }
        for (id, own_row, own, parent_row) in detached {
            let b = db.branch(id).unwrap();
            let conn = b.connect().unwrap();
            if read_v(&conn, own_row) != own {
                not_a_result(&format!("after the reopen branch {} misread its own row", id.0));
            }
            if let Some((prow, pval)) = parent_row {
                let want = if prow == own_row { own } else { pval };
                if read_v(&conn, prow) != want {
                    not_a_result(&format!("after the reopen branch {} misread row {prow}", id.0));
                }
            }
            drop(conn);
            let _ = b.into_id();
        }
        drop(trunk);
    }
    println!("# end reads_checked={reads_checked}");
}
