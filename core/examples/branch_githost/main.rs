//! COMP ARM (githost-shape PREREG G5.2): a12-durable-open's C-P + C-R catalog store composed on the port
//! of F1 + F2 + F4 (turso d7a2b8f6e), catalog mode. This file is r2's catalog-arm harness (turso
//! 20fea769f) with the same schedule, model and checks. Only its counters are changed, to C-P's
//! structures and to the walls G5.2 registers (W1 listing under the mutex, W2 the checkpoint's walk,
//! W3 resident states, W4 table growth). Per-op `getrusage` deltas (`ru_*`) are added: block reads and
//! writes, major faults, involuntary context switches, and CPU time. Unlike every other counter these
//! depend on the load and the page cache; they are there to attribute time, never as a result on
//! their own.
//! The git-hosting shape against N long-lived branches of the durable branch store (githost-shape
//! lane, round 11; PREREG in artie-research frontier/round11/githost-shape/PREREG.md, Amendments G1, G5).
//!
//!   branch_githost grow  --db PATH --to N [--page-size S]
//!   branch_githost probe --db PATH --n N [--label L]
//!
//! One deterministic schedule of steps s = 1, 2, ... (single-threaded). Step s:
//!   1. NEW PR: fork trunk child id = s; it UPDATEs two rows on two leaves (row_for(s), far_row(..)) in one
//!      transaction; the handle is detached (`into_id`): the branch idles.
//!   2. MERGE (s mod 25 < 21, i.e. 0.84 per new PR): one trunk UPDATE of merge_row(s).
//!   3. PR UPDATE (s mod 4 == 0): one branch among the newest ceil(0.0237 s) ids rewrites row_for(its id);
//!      skipped if that branch is dead.
//!   4. DEATH (s mod 50 == 0): a uniformly chosen id < s is reaped; skipped if already dead.
//! The model (trunk row values by merge step, dead set, update generations) is rebuilt in every process by
//! replaying the schedule in memory; `<db>.githost` holds only the step count.
//!
//! `grow` opens Catalog{sync:false} (same file bytes; no branch fsyncs), runs steps until the model's live
//! count reaches N, checks the invariants, and closes cleanly. It prints counters (and unlocked times,
//! which are NOT timing evidence). The process keeps running across every checkpoint of the growth, so
//! its resident states are those of a long-lived server (W3).
//!
//! `probe` opens Catalog{sync:true}
//! in a fresh process (the open is the restart at N), then measures: the open; the settle of parked
//! commits (C-R) by the first slot count, as a SETTLE line; K_s = min(2500, N/10) further schedule
//! steps, each op kind timed and counted separately; read_old (200 uniform live), read_oldest (the 50
//! smallest live ids), list (5), compact (2); the close. There is no diff probe: D-written is P-bound on
//! the port, and the catalog changes only how the trunk's `written` map is filled.
//!
//! Every value read is checked against the model, and the arena's slot count against 2 x live + trunk
//! versions as a lower bound. The exact slot check is across arms: at the same step count this store
//! and the port make the same retain and free decisions (r2 found them equal at 10^4 and 5x10^5).
//! Any failed check prints `NOT A RESULT` and exits 1.
<<<<<<< HEAD
//!
//! r13-compose CENSUS ADDITIONS (PREREG §3 and amendments 2-6; UNBUILT when written):
//! * `--merge update|real|replay-update` (A1, A2.F4/A3.F17). `update` is 310705857's trunk UPDATE of
//!   merge_row(s). `real` merges an open-window PR through the durable Merger (keep-merged, A2), the
//!   model predicting each merge's refusal exactly (content semantics: a PR conflicts iff the trunk
//!   wrote one of its two rows after its fork). `replay-update` installs exactly the rows `real` would,
//!   by plain trunk UPDATEs, skipping exactly the merges `real` refuses (the -MRG arm, with
//!   R13_MERGER=off). A merged PR is closed: later PR UPDATEs of it are skipped.
//! * `--validator v4|v3h` (MV4 base read, or KeyStamp with the restart horizon); `--deaths 0.02|0` (A3).
//! * `--seed K` (A13, A4.S1): W_K = K x 0x9E3779B97F4A7C15 is XORed into the SCHEDULE draws (merge row,
//!   update and death targets, the merged PR, the probe's read draws), never into the listing
//!   checksum. K = 0 is 310705857's schedule. K and the merge mode are persisted in the sidecar, and a
//!   probe whose --seed or --merge differs is refused.
//! * A10/A11 (A2.F7, A5.7): the probe forces a checkpoint, runs a fixed run-in of 25,000 steps,
//!   measures C_arm (steps between consecutive auto checkpoints), and measures over a window that
//!   starts at an auto checkpoint and spans m x C_arm steps, m = ceil(50,000 / C_arm). A window with
//!   fewer than 2 checkpoints, or fewer than 2 evicting ones under a set cap, is NOT A RESULT.
//! * I9's sizes and the R13_* knobs are printed with every run.
||||||| 484270f95
=======
//!
//! ATTRIBUTION (r11-githost-attr lane, PREREG in artie-research frontier/round11/r11-githost-attr/):
//! pr_update's time grew with N at flat counters. These additions separate the candidate mechanisms
//! and change nothing the store does:
//!   - `--durability eager|catalog` is required: `eager` opens the port's own mode (`Durable`, every
//!     state resident after the open), where the growth was measured; `catalog` is COMP / COMP-F.
//!   - COLD vs WARM: an op on a branch created before this process opened the store and not touched by
//!     it since is COLD, else WARM. pr_update, read_old and read_oldest print per-class OP lines
//!     (`op=pr_update.cold` ...) beside the pooled ones, and one `OPREC` line per op.
//!   - PAIRS: after the reads and before the listings, 100 cold branches from the open-time update
//!     window are updated twice in a row (`pair_update.cold` then `.warm`, the same branch and the
//!     same code path), and 100 more read twice (`pair_read.*`). The model follows, and the sidecar
//!     is then marked tainted, so no later process can read these branches against a replayed
//!     schedule.
//!   - Per-op counters: `BranchIoCounters` (arena pread / pwrite / fsync and record-flush time with
//!     counts, resolution and catalog-load time, C-R's parked commits), Turso's process-wide page
//!     reads (`page_io`), and getrusage split into minor / major faults, voluntary / involuntary
//!     switches and user / system time.
//!   - PAGE CACHE: with `R11_UBC_PROBE` set, the store asks mincore about each arena slot's page just
//!     before reading or writing it (`ubc_*`); with `R11_CENSUS_OPS` set, the harness diffs the page
//!     cache residency of the arena and catalog files across each pr_update, read and pair op
//!     (`cen_*`, outside the timed window; -1 where not taken). `RESIDENT` lines give whole-file
//!     residency at fixed points of the probe, also only with `R11_CENSUS_OPS`. Both cost system
//!     calls and map files, so timed runs leave them unset and map nothing.
//!   - M7 (A1): the store counts and times each schema reparse of a branch connection whose state
//!     carries no schema. `R11_SCHEMA_SHARE` turns on arm F-S (a loaded branch adopts a schema
//!     parsed in this process from byte-identical source rows instead of reparsing).
//!   - `ubc-selftest --file DIR` fire-checks mincore on this OS: a sparse file's pages must read not
//!     resident, pages it just read must read resident, and it reports whether an APFS clone starts
//!     with its source's pages cached.
>>>>>>> r11-githost-attr-kv2.noindex

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

<<<<<<< HEAD
use turso_core::branch::merge::{MergePolicy, Merger, Refusal, Validation};
use turso_core::branch::{
    id_set_census, BranchCatShape, BranchDurability, BranchId, BranchMergeWork, BranchOpenStats,
};
||||||| 484270f95
use turso_core::branch::{BranchCatShape, BranchDurability, BranchId, BranchOpenStats};
=======
use turso_core::branch::{
    file_pages_resident, file_residency, page_io, BranchCatShape, BranchDurability, BranchId,
    BranchIoCounters, BranchOpenStats,
};
>>>>>>> r11-githost-attr-kv2.noindex
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const ROWS: u64 = 20_000;
const VALUE_LEN: usize = 100;
const OPEN_WINDOW: f64 = 0.0237;
/// Branches per pair series (r11-githost-attr PREREG section 3).
const PAIRS: usize = 100;

fn die(msg: &str) -> ! {
    eprintln!("branch_githost: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    let _ = std::io::stdout().flush();
    std::process::exit(1)
}

struct Args {
    cmd: String,
    db: PathBuf,
    n: u64,
    page_size: u64,
    label: String,
    /// Trunk `PRAGMA synchronous` during growth: "default" leaves the engine's default.
    trunk_sync: String,
<<<<<<< HEAD
    /// r13-compose A13: the schedule seed (0: 310705857's schedule).
    seed: u64,
    /// r13-compose A1: how a MERGE step merges.
    merge: MergeMode,
    /// r13-compose: the Merger's validator in `real` mode.
    validator: Validation,
    /// r13-compose A3: deaths per step, 0.02 (the schedule's) or 0.
    deaths: bool,
    /// r13-compose A10: the probe's fixed run-in, in steps.
    runin: u64,
    /// r13-compose A2.F10 (R1's H axis): merges on each merge step (1: the schedule's).
    mps: u64,
    /// grow: stop at this step count instead of a live count (R1's H axis grows N = 2e5 STEPS).
    to_steps: u64,
    /// r13-compose: open in the F7 splice arm (S1's zombie sub-arm; a fixture of its own).
    splice: bool,
    /// S1: the stack depths, the measured ops per kind per depth, and the land-tops per depth.
    depths: Vec<u64>,
    touches: u64,
    lands: u64,
    /// R1: the crash image (a, b, e, e2) and image e's untouched-branch commits.
    image: String,
    bumps: u64,
    /// R1 (A4.C3): ops of each kind in the P-window.
    pwindow: u64,
    /// R1's d axis: restart reads only the S1 stack of this depth (0: every stack, round-robin).
    stack_d: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum MergeMode {
    Update,
    Real,
    ReplayUpdate,
}

impl MergeMode {
    fn name(self) -> &'static str {
        match self {
            MergeMode::Update => "update",
            MergeMode::Real => "real",
            MergeMode::ReplayUpdate => "replay-update",
        }
    }
}

/// W_K (A13): set once at startup from --seed, read by every schedule draw.
static W: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static MODE: std::sync::OnceLock<(MergeMode, bool)> = std::sync::OnceLock::new();

fn w() -> u64 {
    *W.get().unwrap_or(&0)
}

fn merge_mode() -> MergeMode {
    MODE.get().map_or(MergeMode::Update, |m| m.0)
}

fn deaths_on() -> bool {
    MODE.get().is_none_or(|m| m.1)
}

fn deaths_name() -> &'static str {
    if deaths_on() {
        "0.02"
    } else {
        "0"
    }
||||||| 484270f95
=======
    /// `catalog` (COMP, COMP-F) or `eager` (the port's `Durable` mode); no default is guessed for a
    /// database grown in the other mode, because the files differ and the open would refuse.
    eager: bool,
    /// `ubc-selftest` only: a scratch directory it may create files in.
    file: PathBuf,
>>>>>>> r11-githost-attr-kv2.noindex
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it
        .next()
        .unwrap_or_else(|| die("usage: branch_githost grow|probe|ubc-selftest ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        page_size: 1024,
        label: String::new(),
        trunk_sync: "default".to_string(),
<<<<<<< HEAD
        seed: 0,
        merge: MergeMode::Update,
        validator: Validation::BaseRead,
        deaths: true,
        runin: 25_000,
        mps: 1,
        to_steps: 0,
        splice: false,
        depths: vec![1, 12, 48, 144],
        touches: 1000,
        lands: 1,
        image: String::new(),
        bumps: 1000,
        pwindow: 1000,
        stack_d: 0,
||||||| 484270f95
=======
        eager: false,
        file: PathBuf::new(),
>>>>>>> r11-githost-attr-kv2.noindex
    };
    let mut durability: Option<String> = None;
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--page-size" => args.page_size = val().parse().unwrap_or_else(|_| die("bad --page-size")),
            "--label" => args.label = val(),
            "--trunk-sync" => args.trunk_sync = val(),
<<<<<<< HEAD
            "--seed" => args.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--merge" => {
                args.merge = match val().as_str() {
                    "update" => MergeMode::Update,
                    "real" => MergeMode::Real,
                    "replay-update" => MergeMode::ReplayUpdate,
                    other => die(&format!("bad --merge {other}")),
                }
            }
            "--validator" => {
                args.validator = match val().as_str() {
                    "v4" => Validation::BaseRead,
                    "v3h" => Validation::KeyStamp,
                    other => die(&format!("bad --validator {other}")),
                }
            }
            "--deaths" => {
                args.deaths = match val().as_str() {
                    "0.02" => true,
                    "0" => false,
                    other => die(&format!("bad --deaths {other} (0.02 or 0)")),
                }
            }
            "--runin" => args.runin = val().parse().unwrap_or_else(|_| die("bad --runin")),
            "--mps" => args.mps = val().parse().unwrap_or_else(|_| die("bad --mps")),
            "--to-steps" => args.to_steps = val().parse().unwrap_or_else(|_| die("bad --to-steps")),
            "--splice" => args.splice = true,
            "--depths" => {
                args.depths = val()
                    .split(',')
                    .map(|d| d.parse().unwrap_or_else(|_| die(&format!("bad depth {d:?}"))))
                    .collect()
            }
            "--touches" => args.touches = val().parse().unwrap_or_else(|_| die("bad --touches")),
            "--lands" => args.lands = val().parse().unwrap_or_else(|_| die("bad --lands")),
            "--image" => args.image = val(),
            "--bumps" => args.bumps = val().parse().unwrap_or_else(|_| die("bad --bumps")),
            "--pwindow" => args.pwindow = val().parse().unwrap_or_else(|_| die("bad --pwindow")),
            "--stack-d" => args.stack_d = val().parse().unwrap_or_else(|_| die("bad --stack-d")),
||||||| 484270f95
=======
            "--durability" => durability = Some(val()),
            "--file" => args.file = PathBuf::from(val()),
>>>>>>> r11-githost-attr-kv2.noindex
            other => die(&format!("unknown argument {other}")),
        }
    }
<<<<<<< HEAD
    if args.cmd != "golden" && (args.db.as_os_str().is_empty() || (args.n == 0 && args.to_steps == 0)) {
        die("--db and --n/--to (or grow's --to-steps) are required");
||||||| 484270f95
    if args.db.as_os_str().is_empty() || args.n == 0 {
        die("--db and --n/--to are required");
=======
    if args.cmd == "ubc-selftest" {
        if args.file.as_os_str().is_empty() {
            die("ubc-selftest needs --file DIR");
        }
        return args;
    }
    if args.db.as_os_str().is_empty() || args.n == 0 {
        die("--db and --n/--to are required");
>>>>>>> r11-githost-attr-kv2.noindex
    }
<<<<<<< HEAD
    if args.mps == 0 || args.depths.iter().any(|&d| d == 0) {
        die("--mps and every depth must be >= 1");
    }
    let _ = W.set(args.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let _ = SEED.set(args.seed);
    let _ = MODE.set((args.merge, args.deaths));
    let _ = MPS.set(args.mps);
    let _ = SPLICE.set(args.splice);
||||||| 484270f95
=======
    args.eager = match durability.as_deref() {
        Some("catalog") => false,
        Some("eager") => true,
        Some(other) => die(&format!("--durability {other}: expected catalog or eager")),
        None => die("--durability catalog|eager is required (r11-githost-attr: no mode is guessed)"),
    };
>>>>>>> r11-githost-attr-kv2.noindex
    args
}

static MPS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static SPLICE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn mps() -> u64 {
    *MPS.get().unwrap_or(&1)
}

// ---------------------------------------------------------------------------------------------
// The schedule: a pure function of the step number.

fn mix(mut x: u64) -> u64 {
    // splitmix64
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn row_for(id: u64) -> u64 {
    id.wrapping_mul(2_654_435_761) % ROWS + 1
}

/// A row half the table away: another leaf.
fn far_row(row: u64) -> u64 {
    (row - 1 + ROWS / 2) % ROWS + 1
}

fn is_merge(s: u64) -> bool {
    s % 25 < 21
}

fn merge_row(s: u64) -> u64 {
    mix(s ^ 0xA5A5_0001 ^ w()) % ROWS + 1
}

/// r13-compose (R1's H axis): the j-th merge of step s; j = 0 is the schedule's own draw.
fn merge_row_j(s: u64, j: u64) -> u64 {
    if j == 0 {
        merge_row(s)
    } else {
        mix(s ^ 0xA5A5_0001 ^ w() ^ (j << 48)) % ROWS + 1
    }
}

/// r13-compose A1: the PR a `real`/`replay-update` MERGE step merges, from the open window (the newest
/// ceil(0.0237 s) PRs, as PR UPDATEs choose), never the PR this step forked.
fn merge_pick(s: u64, j: u64) -> Option<u64> {
    let window = ((OPEN_WINDOW * s as f64).ceil() as u64).max(1);
    let back = mix(s ^ 0xA5A5_0004 ^ w() ^ (j << 48)) % window;
    (s > back + 1).then(|| s - 1 - back)
}

fn update_target(s: u64) -> Option<u64> {
    if s % 4 != 0 {
        return None;
    }
    let window = ((OPEN_WINDOW * s as f64).ceil() as u64).max(1);
    let back = mix(s ^ 0xA5A5_0002 ^ w()) % window;
    (s > back + 1).then(|| s - 1 - back)
}

fn death_target(s: u64) -> Option<u64> {
    if s % 50 != 0 || s < 2 || !deaths_on() {
        return None;
    }
    Some(1 + mix(s ^ 0xA5A5_0003 ^ w()) % (s - 1))
}

fn pad(prefix: char, n: u64) -> String {
    format!("{prefix}{:0>width$}", n, width = VALUE_LEN - 1)
}

fn trunk_initial(row: u64) -> String {
    format!("{:0>width$}", row, width = VALUE_LEN)
}

fn merge_value(s: u64) -> String {
    pad('t', s)
}

fn own_value(id: u64, gen: u64) -> String {
    format!("b{id:012}g{gen:012}{}", "x".repeat(VALUE_LEN - 26))
}

fn far_value(id: u64) -> String {
    pad('c', id)
}

/// Everything a process knows about the logical state, rebuilt by replaying the schedule.
struct Model {
    steps: u64,
    dead: Vec<bool>,
    gen: Vec<u64>,
    /// row -> its trunk writes in step order, (step, value) (r13-compose A1: a merge of a PR writes
    /// the PR's values; 310705857's `update` mode writes merge_value(step)).
    writes: Vec<Vec<(u64, String)>>,
    /// r13-compose A1: PRs a `real`/`replay-update` merge installed (closed: no later PR UPDATE).
    merged: Vec<bool>,
    live: u64,
    updates_skipped: u64,
    deaths_skipped: u64,
    /// r13-compose A1: merge steps whose PR was dead, merged or not yet forked, and the merges the
    /// model predicts refused (a trunk write to the PR's rows after its fork).
    merges_skipped: u64,
    merges_refused: u64,
    /// r13-compose S1: live branches outside the schedule (stack levels), from the sidecar.
    extra: Vec<u64>,
    /// The pages those levels own, summed (amendment 8.17: the arena floor's stack term).
    extra_pages: u64,
    /// r13-compose R1: trunk writes made outside the schedule (restart's merges) are stamped at
    /// virtual steps after the last schedule step, so no schedule fork sees them.
    virt: u64,
    /// The (schedule or virtual) step of every trunk commit, ascending: R1's realised H is the
    /// commits since the oldest live child's fork (third review: H is measured, not assumed).
    commits: Vec<u64>,
}

/// What a MERGE step does.
#[derive(Clone)]
enum MergePlan {
    /// 310705857's: a trunk UPDATE of `row`.
    Update(u64),
    /// r13-compose A1: merge PR `p` (generation `gen`); `refused` is the model's prediction.
    Pr { p: u64, gen: u64, refused: bool },
}

/// What a step does, decided by the model before the store is touched.
struct StepPlan {
    s: u64,
    /// r13-compose: one entry per merge done (`--mps`); 310705857's had at most one.
    merges: Vec<MergePlan>,
    update: Option<(u64, u64)>,
    death: Option<u64>,
}

impl Model {
    fn new() -> Self {
        Self {
            steps: 0,
            dead: vec![false],
            gen: vec![0],
            writes: vec![Vec::new(); ROWS as usize + 1],
            merged: vec![false],
            live: 0,
            updates_skipped: 0,
            deaths_skipped: 0,
            merges_skipped: 0,
            merges_refused: 0,
            extra: Vec::new(),
            extra_pages: 0,
            virt: 0,
            commits: Vec::new(),
        }
    }

    fn replay(steps: u64) -> Self {
        let mut m = Self::new();
        for _ in 0..steps {
            m.advance();
        }
        m
    }

    /// Apply the next step to the model and say what the store must do.
    fn advance(&mut self) -> StepPlan {
        let s = self.steps + 1;
        self.steps = s;
        self.dead.push(false);
        self.gen.push(0);
        self.merged.push(false);
        self.live += 1;
        let mut merges = Vec::new();
        for k in 0..if is_merge(s) { mps() } else { 0 } {
            // Merges j >= 1 of a step (R1's H axis, --mps) are plain trunk UPDATEs in every mode, so
            // H grows with M whatever the merge mode (third review: PR merges alone cap H at the
            // step count). j = 0 is the schedule's merge, by the mode.
            if merge_mode() == MergeMode::Update || k > 0 {
                let r = merge_row_j(s, k);
                self.writes[r as usize].push((s, merge_value(s)));
                self.commits.push(s);
                merges.push(MergePlan::Update(r));
            } else {
                match merge_pick(s, k) {
                    Some(p) if !self.dead[p as usize] && !self.merged[p as usize] => merges.push(self.plan_pr_merge(s, p)),
                    _ => self.merges_skipped += 1,
                }
            }
        }
        let update = match update_target(s) {
            Some(t) if !self.dead[t as usize] && !self.merged[t as usize] => {
                self.gen[t as usize] += 1;
                Some((t, self.gen[t as usize]))
            }
            Some(_) => {
                self.updates_skipped += 1;
                None
            }
            None => None,
        };
        let death = match death_target(s) {
            Some(t) if !self.dead[t as usize] => {
                self.dead[t as usize] = true;
                self.live -= 1;
                Some(t)
            }
            Some(_) => {
                self.deaths_skipped += 1;
                None
            }
            None => None,
        };
        StepPlan {
            s,
            merges,
            update,
            death,
        }
    }

    /// r13-compose A1: merge PR `p` at (schedule or virtual) step `at`, predicting the refusal by
    /// content semantics (A6.4): the PR's two rows differ from their base (their values are unique),
    /// so it conflicts iff the trunk wrote one of them after its fork, at or after step p (step p's
    /// own merge runs after p's fork).
    fn plan_pr_merge(&mut self, at: u64, p: u64) -> MergePlan {
        let row = row_for(p);
        let refused = [row, far_row(row)]
            .iter()
            .any(|&r| self.writes[r as usize].last().is_some_and(|&(m, _)| m >= p));
        let gen = self.gen[p as usize];
        if refused {
            self.merges_refused += 1;
        } else {
            self.writes[row as usize].push((at, own_value(p, gen)));
            self.writes[far_row(row) as usize].push((at, far_value(p)));
            self.merged[p as usize] = true;
            self.commits.push(at);
        }
        MergePlan::Pr { p, gen, refused }
    }

    /// The next virtual step (R1's out-of-schedule trunk writes).
    fn next_virtual(&mut self) -> u64 {
        self.virt += 1;
        self.steps + self.virt
    }

    /// The trunk's value of `row` as a branch forked at step `f` sees it: the latest trunk write before
    /// `f` (a merge at step f runs after fork f).
    fn trunk_at(&self, row: u64, f: u64) -> String {
        let ws = &self.writes[row as usize];
        let i = ws.partition_point(|&(m, _)| m < f);
        if i == 0 {
            trunk_initial(row)
        } else {
            ws[i - 1].1.clone()
        }
    }

    /// A row the trunk wrote at a step >= `id` (after `id`'s fork), the first such write at or after a
    /// step drawn from `pick`, if any.
    fn later_write(&self, id: u64, pick: u64) -> Option<u64> {
        let mut m = id + pick % (self.steps - id + 1);
        while m <= self.steps {
            if is_merge(m) {
                match merge_mode() {
                    MergeMode::Update => return Some(merge_row(m)),
                    _ => {
                        if let Some(p) = merge_pick(m, 0) {
                            let row = row_for(p);
                            if self.writes[row as usize].iter().any(|&(w, _)| w == m) {
                                return Some(row);
                            }
                        }
                    }
                }
            }
            m += 1;
        }
        None
    }

    fn live_ids(&self) -> Vec<u64> {
        (1..=self.steps).filter(|&id| !self.dead[id as usize]).collect()
    }

    /// R1's realised H: trunk commits since the oldest live schedule child forked.
    fn h(&self) -> u64 {
        let Some(&oldest) = self.live_ids().first() else {
            return 0;
        };
        (self.commits.len() - self.commits.partition_point(|&m| m < oldest)) as u64
    }

    /// A31's bound from the model (amendment 8): the trunk's row writes since the oldest live
    /// schedule child forked. The store keeps one stamp entry per key re-stamped at a new epoch, so
    /// `stamp_entries` is at most this (same-epoch rewrites of a key collapse).
    fn trunk_writes_since_oldest(&self) -> u64 {
        let Some(&oldest) = self.live_ids().first() else {
            return 0;
        };
        self.writes
            .iter()
            .map(|ws| ws.iter().filter(|&&(m, _)| m >= oldest).count() as u64)
            .sum()
    }
}

// ---------------------------------------------------------------------------------------------
// Store access.

<<<<<<< HEAD
/// r13-compose: F7's splice arm exists only in binaries with F7 (B_ALL, B_noF8). An inherent
/// `DatabaseOpts::with_branch_splice` wins method resolution over this trait's method, so this
/// fallback is reached only in B_noF7, where it refuses a splice-arm run. One harness source then
/// builds against all three binaries (A2.F6: one harness commit).
#[allow(dead_code)]
trait NoSpliceArm: Sized {
    fn with_branch_splice(self, splice: bool) -> Self;
}

impl NoSpliceArm for DatabaseOpts {
    fn with_branch_splice(self, splice: bool) -> Self {
        if splice {
            die("this binary has no F7 splice arm (B_noF7): --splice is refused");
        }
        self
    }
}

fn open_db(path: &Path, sync: bool) -> Arc<Database> {
||||||| 484270f95
fn open_db(path: &Path, sync: bool) -> Arc<Database> {
=======
fn open_db(path: &Path, sync: bool, eager: bool) -> Arc<Database> {
>>>>>>> r11-githost-attr-kv2.noindex
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
<<<<<<< HEAD
    let splice = SPLICE.get().copied().unwrap_or(false);
||||||| 484270f95
=======
    // r11-githost-attr: `eager` is the port's mode (every state resident after the open).
    let durability = if eager {
        BranchDurability::Durable { sync }
    } else {
        BranchDurability::Catalog { sync }
    };
>>>>>>> r11-githost-attr-kv2.noindex
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
<<<<<<< HEAD
        DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync: if sync { turso_core::branch::SyncClass::Fsync } else { turso_core::branch::SyncClass::Off } })
            .with_branch_splice(splice),
||||||| 484270f95
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync }),
=======
        DatabaseOpts::new().with_branch_durability(durability),
>>>>>>> r11-githost-attr-kv2.noindex
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")));
    // F-W3's knob (githost-shape PREREG G5.3): unset keeps every touched state resident (COMP).
<<<<<<< HEAD
    if let Some(cap) = arm_cap() {
||||||| 484270f95
    if let Ok(v) = std::env::var("R11_RESIDENT_CAP") {
        let cap: usize = v.parse().unwrap_or_else(|_| die(&format!("bad R11_RESIDENT_CAP {v:?}")));
=======
    if let Ok(v) = std::env::var("R11_RESIDENT_CAP") {
        if eager {
            die("R11_RESIDENT_CAP is F-W3's catalog eviction; the eager mode has no catalog to evict to");
        }
        let cap: usize = v.parse().unwrap_or_else(|_| die(&format!("bad R11_RESIDENT_CAP {v:?}")));
>>>>>>> r11-githost-attr-kv2.noindex
        db.branch_set_resident_cap(Some(cap));
    }
    db
}

fn sidecar(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}.githost", db.to_str().unwrap()))
}

<<<<<<< HEAD
/// The sidecar `<db>.githost`: `steps=N`, and (r13-compose) the schedule's parameters `seed=K`,
/// `merge=<mode>`, `deaths=<rate>` and `mps=M`, and the state outside the schedule: `shift=S0,L`
/// (S1's stack command consumed the ids S0+1..=S0+L, so every later schedule fork s > S0 gets the
/// id s + L), `extra=<id>,...` (stack levels still live), one `stack=<d>,<serial>,<root id>,...,<top
/// id>` per S1 main stack with its `pages=<serial>,<p_0>,...,<p_{d-1}>` (each level's own pages, the
/// arena floor's stack term: amendment 8.17), and `bump=<count>` (R1 image e's untouched-branch commits, made after the
/// last step). Missing parameter lines are 310705857's (K = 0, update, 0.02, 1). A run whose schedule
/// parameters differ from the sidecar's is refused: its schedule would not be the one the fixture
/// grew under.
#[derive(Default, Clone)]
struct Sidecar {
    steps: u64,
    shift: Option<(u64, u64)>,
    extra: Vec<u64>,
    stacks: Vec<StackRec>,
    bump: u64,
    /// R1: the arena slots in use when a crash image was killed (`slots=N`), which the restart must
    /// find exactly after its settle (§3.3 R1: slot accounting is exact).
    slots: Option<u64>,
}

#[derive(Clone)]
struct StackRec {
    d: u64,
    serial: u64,
    /// Level ids, root first, top last.
    ids: Vec<u64>,
    /// Each level's own pages (`branch_state_pages`), root first, top last (amendment 8.17).
    pages: Vec<u64>,
}

fn ids_of(v: &str, what: &str) -> Vec<u64> {
    v.trim()
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.parse().unwrap_or_else(|_| not_a_result(&format!("unparseable sidecar {what}: {x:?}"))))
        .collect()
}

fn read_sidecar(db: &Path) -> Sidecar {
    let Ok(text) = std::fs::read_to_string(sidecar(db)) else {
        return Sidecar::default();
    };
    let mut sc = Sidecar::default();
    let mut steps = None;
    let mut pages_lines: Vec<(u64, Vec<u64>)> = Vec::new();
    let (mut seed, mut merge, mut deaths, mut m) = (0u64, "update".to_string(), "0.02".to_string(), 1u64);
    for line in text.lines() {
        let Some((k, v)) = line.split_once('=') else {
            not_a_result(&format!("unparseable sidecar line {line:?}"));
        };
        match k {
            "steps" => steps = v.trim().parse().ok(),
            "seed" => seed = v.trim().parse().unwrap_or_else(|_| not_a_result("unparseable sidecar seed")),
            "merge" => merge = v.trim().to_string(),
            "deaths" => deaths = v.trim().to_string(),
            "mps" => m = v.trim().parse().unwrap_or_else(|_| not_a_result("unparseable sidecar mps")),
            "shift" => match ids_of(v, "shift")[..] {
                [after, l] => sc.shift = Some((after, l)),
                _ => not_a_result("sidecar shift needs two numbers"),
            },
            "extra" => sc.extra = ids_of(v, "extra"),
            "stack" => match &ids_of(v, "stack")[..] {
                [d, serial, ids @ ..] if ids.len() as u64 == *d => sc.stacks.push(StackRec {
                    d: *d,
                    serial: *serial,
                    ids: ids.to_vec(),
                    pages: Vec::new(),
                }),
                _ => not_a_result(&format!("sidecar stack line {v:?}: needs d, serial and d ids")),
            },
            "pages" => match &ids_of(v, "pages")[..] {
                [serial, pages @ ..] => pages_lines.push((*serial, pages.to_vec())),
                _ => not_a_result(&format!("sidecar pages line {v:?}: needs a serial")),
            },
            "bump" => sc.bump = v.trim().parse().unwrap_or_else(|_| not_a_result("unparseable sidecar bump")),
            "slots" => sc.slots = Some(v.trim().parse().unwrap_or_else(|_| not_a_result("unparseable sidecar slots"))),
            other => not_a_result(&format!("unknown sidecar key {other:?}")),
        }
    }
    sc.steps = steps.unwrap_or_else(|| not_a_result("unparseable sidecar"));
    // Amendment 8.17: every stack has exactly one pages line with d counts, no pages line is
    // orphaned, and `extra` is exactly the stacks' ids; otherwise the floor's stack term is unknown.
    if pages_lines.len() != sc.stacks.len() {
        not_a_result(&format!("sidecar: {} stack lines, {} pages lines", sc.stacks.len(), pages_lines.len()));
    }
    for (serial, pages) in pages_lines {
        let Some(st) = sc.stacks.iter_mut().find(|st| st.serial == serial && st.pages.is_empty()) else {
            not_a_result(&format!("sidecar pages line for serial {serial} matches no stack"));
        };
        if pages.len() as u64 != st.d {
            not_a_result(&format!("sidecar pages line for serial {serial}: {} counts, d = {}", pages.len(), st.d));
        }
        st.pages = pages;
    }
    let stack_ids: Vec<u64> = sc.stacks.iter().flat_map(|st| st.ids.iter().copied()).collect();
    if stack_ids != sc.extra {
        not_a_result("sidecar: extra is not exactly the stacks' level ids");
    }
    if seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) != w()
        || merge != merge_mode().name()
        || deaths != deaths_name()
        || m != mps()
    {
        not_a_result(&format!(
            "the fixture grew under seed={seed} merge={merge} deaths={deaths} mps={m}; this run is seed W={:#x} \
             merge={} deaths={} mps={}",
            w(),
            merge_mode().name(),
            deaths_name(),
            mps()
        ));
    }
    if let Some(sh) = sc.shift {
        let _ = SHIFT.set(sh);
    }
    sc
}

/// The sidecar for a command that continues the schedule: refused on R1 image e's state, whose
/// untouched-branch commits sit after the last step (only `restart` reads it).
fn read_stepping_sidecar(db: &Path) -> Sidecar {
    let sc = read_sidecar(db);
    if sc.bump > 0 {
        not_a_result("this state carries R1 image e's untouched-branch commits; only `restart` reads it");
    }
    sc
}

fn write_sidecar(db: &Path, sc: &Sidecar) {
    let seed = SEED.get().copied().unwrap_or(0);
    let mut text = format!(
        "steps={}\nseed={seed}\nmerge={}\ndeaths={}\nmps={}\n",
        sc.steps,
        merge_mode().name(),
        deaths_name(),
        mps()
    );
    let join = |ids: &[u64]| ids.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
    if let Some((after, l)) = sc.shift {
        text.push_str(&format!("shift={after},{l}\n"));
    }
    if !sc.extra.is_empty() {
        text.push_str(&format!("extra={}\n", join(&sc.extra)));
    }
    for st in &sc.stacks {
        text.push_str(&format!("stack={},{},{}\n", st.d, st.serial, join(&st.ids)));
        text.push_str(&format!("pages={},{}\n", st.serial, join(&st.pages)));
    }
    if sc.bump > 0 {
        text.push_str(&format!("bump={}\n", sc.bump));
    }
    if let Some(n) = sc.slots {
        text.push_str(&format!("slots={n}\n"));
    }
    std::fs::write(sidecar(db), text).unwrap_or_else(|e| not_a_result(&format!("sidecar: {e}")));
}

/// S1's id shift (see `Sidecar`), set once from the sidecar or by the stack command.
static SHIFT: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();

/// The store's id for the schedule's branch `x` (the step that forked it).
fn sid(x: u64) -> BranchId {
    match SHIFT.get() {
        Some(&(after, l)) if x > after => BranchId(x + l),
        _ => BranchId(x),
||||||| 484270f95
fn read_steps(db: &Path) -> u64 {
    match std::fs::read_to_string(sidecar(db)) {
        Ok(s) => s
            .trim()
            .strip_prefix("steps=")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| not_a_result("unparseable sidecar")),
        Err(_) => 0,
=======
/// What a probe with pair ops leaves in the sidecar: those ops rewrote branches outside the schedule,
/// so a later process replaying the schedule would check them against the wrong values.
const TAINT: &str = "tainted=r11-githost-attr pair ops rewrote branches outside the schedule";

fn read_steps(db: &Path) -> u64 {
    match std::fs::read_to_string(sidecar(db)) {
        Ok(s) if s.contains("tainted=") => {
            not_a_result("this database was probed with pair ops (r11-githost-attr); probe a fresh clone of the grown state")
        }
        Ok(s) => s
            .trim()
            .strip_prefix("steps=")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| not_a_result("unparseable sidecar")),
        Err(_) => 0,
>>>>>>> r11-githost-attr-kv2.noindex
    }
}

static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

/// The state a command leaves on disk, for the census driver: the next command's `--n` is `live`.
fn state_line(model: &Model) -> String {
    format!(
        "STATE\tlive={}\tsteps={}\textra={}\th={}\tmodel_trunk_writes_since_oldest={}",
        model.live,
        model.steps,
        model.extra.len(),
        model.h(),
        model.trunk_writes_since_oldest()
    )
}

impl Model {
    /// The model of a sidecar's state: the schedule replayed, the stacks' live levels, and image
    /// e's untouched-branch commits (the `bump` smallest live unmerged schedule ids, one more
    /// generation each; `bump_targets` picks them).
    fn from_sidecar(sc: &Sidecar) -> Self {
        let mut m = Self::replay(sc.steps);
        m.extra = sc.extra.clone();
        m.extra_pages = sc.stacks.iter().flat_map(|st| st.pages.iter()).sum();
        for t in m.bump_targets(sc.bump) {
            m.gen[t as usize] += 1;
        }
        m
    }

    fn bump_targets(&self, count: u64) -> Vec<u64> {
        (1..=self.steps)
            .filter(|&id| !self.dead[id as usize] && !self.merged[id as usize])
            .take(count as usize)
            .collect()
    }

    /// The store ids of every live branch, sorted: the schedule's (through `sid`) and the stacks'.
    fn store_live_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.live_ids().into_iter().map(|x| sid(x).0).collect();
        ids.extend(self.extra.iter().copied());
        ids.sort_unstable();
        ids
    }
}

fn write_tainted_steps(db: &Path, steps: u64) {
    std::fs::write(sidecar(db), format!("steps={steps}\n{TAINT}\n"))
        .unwrap_or_else(|e| not_a_result(&format!("sidecar: {e}")));
}

fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap()
}

fn read_v(conn: &Arc<Connection>, row: u64) -> String {
    let mut stmt = conn.prepare(format!("SELECT v FROM t WHERE id = {row}")).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [r] => match &r[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("row {row}: expected text, got {other:?}")),
        },
        _ => not_a_result(&format!("row {row}: {} rows", rows.len())),
    }
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap_or_else(|e| not_a_result(&format!("{sql:.60}: {e}")));
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn size_of(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}

fn files_line(db: &Path) -> String {
    let with = |suffix: &str| PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
    format!(
        "db_bytes={} wal_bytes={} log_bytes={} snap_bytes={} arena_bytes={} cat_bytes={} catwal_bytes={} \
         hot_bytes={}",
        size_of(db),
        size_of(&with("-wal")),
        size_of(&with("-branch-log")),
        size_of(&with("-branch-snap")),
        size_of(&with("-branch-arena")),
        size_of(&with("-branch-cat")),
        size_of(&with("-branch-cat-wal")),
        size_of(&with("-branch-hot"))
    )
}

<<<<<<< HEAD
/// githost-shape r3 (lead 2026-09-27, the round-12 scout's scale lens; observation only): F-W1's live-id trie
/// measured. `model`: the set F-W1's first listing would build for the model's live ids (built here, off the
/// store); `store`: the store's own set, once a listing of this process has built it. Bytes are node
/// allocations (the node plus two reference counts); the allocator's rounding is not counted. PREREG G7 registers
/// bytes per live id flat in N.
fn idset_line(db: &Arc<Database>, model: &Model, n: u64, label: &str, at: &str) -> String {
    let (len, nodes, bytes) = id_set_census(model.store_live_ids().into_iter().map(|id| {
        u32::try_from(id).unwrap_or_else(|_| not_a_result(&format!("branch id {id} past u32")))
    }));
    let per = |b: u64, l: u64| b as f64 / l.max(1) as f64;
    let own = match db.branch_live_id_census() {
        Some((l, nd, b)) => format!(
            "idset_store_len={l}\tidset_store_nodes={nd}\tidset_store_bytes={b}\tidset_store_bytes_per_id={:.3}",
            per(b, l)
        ),
        None => "idset_store_len=none".to_string(),
    };
    format!(
        "IDSET\tn={n}\tlabel={label}\tat={at}\tidset_model_len={len}\tidset_model_nodes={nodes}\t\
         idset_model_bytes={bytes}\tidset_model_bytes_per_id={:.3}\t{own}",
        per(bytes, len)
    )
}

||||||| 484270f95
=======
// ---------------------------------------------------------------------------------------------
// Page-cache residency (r11-githost-attr). Every answer comes from `file_residency`, the function
// the store's own `R11_UBC_PROBE` uses and `ubc-selftest` fire-checks.

/// The branch files a census reads, by suffix of the database path, in `RESIDENT` order.
const CENSUS_FILES: [(&str, &str); 7] = [
    ("arena", "-branch-arena"),
    ("cat", "-branch-cat"),
    ("catwal", "-branch-cat-wal"),
    ("log", "-branch-log"),
    ("snap", "-branch-snap"),
    ("db", ""),
    ("wal", "-wal"),
];

/// Read-only handles on the branch files that exist when the probe starts. Opened once; a file's
/// length is read at each census, so the arena's appends are seen.
struct Census {
    files: Vec<(&'static str, std::fs::File)>,
}

/// One census: each file's per-page residency, or `None` where the OS refused the query.
type CensusMaps = Vec<Option<Vec<bool>>>;

impl Census {
    fn open(db: &Path) -> Self {
        let files = CENSUS_FILES
            .iter()
            .filter_map(|&(name, suffix)| {
                let path = PathBuf::from(format!("{}{suffix}", db.to_str().unwrap()));
                std::fs::File::open(path).ok().map(|f| (name, f))
            })
            .collect();
        Self { files }
    }

    fn take(&self) -> CensusMaps {
        self.files
            .iter()
            .map(|(_, f)| {
                let len = f.metadata().ok()?.len();
                file_residency(f, 0, len)
            })
            .collect()
    }

    /// Pages of file `name` that turned resident (within the earlier length), stopped being resident,
    /// and are resident past the earlier length (appended), from `a` to `b`; -1 each when the file is
    /// absent or either census was refused.
    fn diff(&self, name: &str, a: &CensusMaps, b: &CensusMaps) -> (i64, i64, i64) {
        let Some(i) = self.files.iter().position(|(n, _)| *n == name) else {
            return (-1, -1, -1);
        };
        let (Some(a), Some(b)) = (&a[i], &b[i]) else {
            return (-1, -1, -1);
        };
        let common = a.len().min(b.len());
        let new = (0..common).filter(|&p| !a[p] && b[p]).count() as i64;
        let gone = (0..common).filter(|&p| a[p] && !b[p]).count() as i64;
        let grown = b[common..].iter().filter(|&&r| r).count() as i64;
        (new, gone, grown)
    }

    /// Whole-file residency now, as a `RESIDENT` line: resident pages and pages per file.
    fn line(&self, n: u64, label: &str, at: &str) -> String {
        let mut line = format!("RESIDENT\tn={n}\tlabel={label}\tat={at}\tvm_page={}", vm_page());
        for (name, f) in &self.files {
            let answer = f
                .metadata()
                .ok()
                .and_then(|m| file_pages_resident(f, 0, m.len()));
            match answer {
                Some((r, p)) => line.push_str(&format!("\t{name}_resident_pages={r}\t{name}_pages={p}")),
                None => line.push_str(&format!("\t{name}_resident_pages=REFUSED\t{name}_pages=REFUSED")),
            }
        }
        line
    }
}

fn vm_page() -> u64 {
    // SAFETY: sysconf reads a constant.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(p).unwrap_or(0)
}

/// `ubc-selftest --file DIR`: force the residency instrument to answer both ways on this OS before
/// any run reads it (r11-githost-attr PREREG section 4). Writes three 64 MiB files and one clone
/// under DIR and deletes them. PASS needs all of:
///   (a) a sparse file, never written or read, reads 0 pages resident (the instrument can say "no");
///   (b) after reading every 4th page of a file written through an F_NOCACHE descriptor, every page
///       read reads resident (it can say "yes" to a page a read brought in);
///   (c) a file just written through the cache and fsynced reads at least half its pages resident;
///   (d) after reading every 4th page of an APFS clone of (c), every page read reads resident;
///   (e) at least one real-data file read "not resident" for at least half its pages before anything
///       read it (the F_NOCACHE file, or the clone at birth). Without (e), "resident" could mean
///       "has blocks on disk" rather than "cached", and the verdict is INCONCLUSIVE.
/// It REPORTS, as a fact and not a condition, how many of the clone's pages read resident before
/// anything read the clone: whether a fresh clone starts cold, which the probes rely on (they run
/// on clones). Reading the hole is reported too: whether a read of a hole caches zero pages.
/// Apple only (clonefile, F_NOCACHE): the lane's box.
#[cfg(not(target_vendor = "apple"))]
fn ubc_selftest(_dir: &Path) {
    die("ubc-selftest: Apple targets only");
}

#[cfg(target_vendor = "apple")]
fn ubc_selftest(dir: &Path) {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    const LEN: u64 = 64 << 20;
    let page = vm_page();
    if page == 0 {
        println!("UBC_SELFTEST\tverdict=FAIL\twhy=sysconf(_SC_PAGESIZE) failed");
        std::process::exit(1);
    }
    let pages = LEN / page;
    std::fs::create_dir_all(dir).unwrap_or_else(|e| die(&format!("selftest dir: {e}")));
    let hole = dir.join("selftest_hole");
    let nocache = dir.join("selftest_nocache");
    let written = dir.join("selftest_written");
    let clone = dir.join("selftest_clone");
    let all = [&hole, &nocache, &written, &clone];
    for p in all {
        let _ = std::fs::remove_file(p);
    }
    let count = |f: &std::fs::File| file_pages_resident(f, 0, LEN).map(|(r, _)| r);
    // Every 4th page, one byte each: the pages the positive controls require resident.
    let touch = |f: &std::fs::File| -> Vec<usize> {
        let mut b = [0u8; 1];
        (0..pages as usize)
            .step_by(4)
            .inspect(|&p| {
                f.read_exact_at(&mut b, p as u64 * page)
                    .unwrap_or_else(|e| die(&format!("selftest read: {e}")))
            })
            .collect()
    };
    let read_resident = |f: &std::fs::File, read: &[usize]| -> Option<usize> {
        let map = file_residency(f, 0, LEN)?;
        Some(read.iter().filter(|&&p| map[p]).count())
    };
    let chunk = vec![0xA5u8; 1 << 20];
    let fill = |f: &mut std::fs::File| {
        for _ in 0..(LEN >> 20) {
            f.write_all(&chunk).unwrap_or_else(|e| die(&format!("selftest write: {e}")));
        }
        f.sync_all().unwrap_or_else(|e| die(&format!("selftest fsync: {e}")));
    };

    // (a) and the hole's read, reported.
    let h = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&hole)
        .unwrap_or_else(|e| die(&format!("selftest hole: {e}")));
    h.set_len(LEN).unwrap_or_else(|e| die(&format!("selftest set_len: {e}")));
    let hole_before = count(&h);
    let hole_read = touch(&h);
    let hole_after = read_resident(&h, &hole_read);

    // (b): written with the page cache off for that descriptor, then read through another.
    {
        let mut f = std::fs::File::create(&nocache).unwrap_or_else(|e| die(&format!("selftest nocache: {e}")));
        // SAFETY: a valid descriptor owned by `f`; F_NOCACHE takes an int.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) } != 0 {
            die("selftest: fcntl(F_NOCACHE) refused");
        }
        fill(&mut f);
    }
    let n = std::fs::File::open(&nocache).unwrap_or_else(|e| die(&format!("selftest nocache: {e}")));
    let nocache_before = count(&n);
    let nocache_read = touch(&n);
    let nocache_after = read_resident(&n, &nocache_read);

    // (c), then the clone for (d) and the report.
    // Read and write: `file_residency` maps with PROT_READ, which fails on a write-only descriptor
    // (`File::create`), and that made (c) false on every run (r11-githost-attr-refute (c), A1).
    let mut w = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&written)
        .unwrap_or_else(|e| die(&format!("selftest written: {e}")));
    fill(&mut w);
    let written_resident = count(&w);
    let (src, dst) = (
        std::ffi::CString::new(written.to_str().unwrap()).unwrap(),
        std::ffi::CString::new(clone.to_str().unwrap()).unwrap(),
    );
    // SAFETY: two NUL-terminated paths that live across the call; flags 0.
    let rc = unsafe { libc::clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    let (clone_birth, clone_after, clone_read) = if rc == 0 {
        let k = std::fs::File::open(&clone).unwrap_or_else(|e| die(&format!("selftest clone: {e}")));
        let birth = count(&k);
        let read = touch(&k);
        (birth, read_resident(&k, &read), read.len())
    } else {
        (None, None, 0)
    };
    for p in all {
        let _ = std::fs::remove_file(p);
    }

    let a = hole_before == Some(0);
    let b = nocache_after == Some(nocache_read.len());
    let c = written_resident.is_some_and(|r| r * 2 >= pages);
    let d = rc == 0 && clone_after == Some(clone_read);
    let e = nocache_before.is_some_and(|r| r * 2 < pages) || clone_birth.is_some_and(|r| r * 2 < pages);
    let verdict = match (a && b && c && d, e) {
        (true, true) => "PASS",
        (true, false) => "INCONCLUSIVE",
        (false, _) => "FAIL",
    };
    let show = |v: Option<u64>| v.map_or("REFUSED".to_string(), |r| r.to_string());
    let show_n = |v: Option<usize>| v.map_or("REFUSED".to_string(), |r| r.to_string());
    println!(
        "UBC_SELFTEST\tverdict={verdict}\tvm_page={page}\tpages={pages}\ta={a}\tb={b}\tc={c}\td={d}\te={e}\t\
         hole_resident_before_read={}\thole_pages_read={}\thole_read_pages_resident={}\t\
         nocache_resident_before_read={}\tnocache_pages_read={}\tnocache_read_pages_resident={}\t\
         written_resident={}\tclone_rc={rc}\tclone_resident_at_birth={}\tclone_pages_read={clone_read}\t\
         clone_read_pages_resident={}",
        show(hole_before),
        hole_read.len(),
        show_n(hole_after),
        show(nocache_before),
        nocache_read.len(),
        show_n(nocache_after),
        show(written_resident),
        show(clone_birth),
        show_n(clone_after),
    );
    if verdict != "PASS" {
        std::process::exit(1);
    }
}

>>>>>>> r11-githost-attr-kv2.noindex
fn shape_line(s: &BranchCatShape) -> String {
    format!(
        "checkpoints={} checkpoint_ns={} ckpt_trunk_inserted={} ckpt_trunk_deleted={} ckpt_branch_rows={} \
         ckpt_rows_written={} ckpt_states_walked={} ids_calls={} ids_resident_visited={} ids_catalog_rows={} \
         ids_build_rows={} table_grows={} table_moved={} evictions={} evicted_states={} resident_states={} \
         dirty_branches={} trunk_overlay_versions={} trunk_cache_versions={} trunk_cache_pages={} \
         trunk_known_pages={} trunk_probes={} trunk_rows={} log_len={} ensure_cold={} ensure_chain_sum={} \
         ensure_chain_max={} evicted_with_resident_descendant={} walk_items_yielded={} derived_inserts={} \
         table_chunks={} chunk_allocs={} table_slots_allocated={} table_slot_bytes={} walk_slots_scanned={} \
         instrument_walk_items={} settle_sharp_calls={} settle_sharp_loads={} settle_sharp_max_loads={}",
        s.checkpoints,
        s.checkpoint_ns,
        s.ckpt_trunk_inserted,
        s.ckpt_trunk_deleted,
        s.ckpt_branch_rows,
        s.ckpt_rows_written,
        s.ckpt_states_walked,
        s.ids_calls,
        s.ids_resident_visited,
        s.ids_catalog_rows,
        s.ids_build_rows,
        s.table_grows,
        s.table_moved,
        s.evictions,
        s.evicted_states,
        s.resident_states,
        s.dirty_branches,
        s.trunk_overlay_versions,
        s.trunk_cache_versions,
        s.trunk_cache_pages,
        s.trunk_known_pages,
        s.trunk_probes,
        s.trunk_rows,
        s.log_len,
        s.ensure_cold,
        s.ensure_chain_sum,
        s.ensure_chain_max,
        s.evicted_with_resident_descendant,
        s.walk_items_yielded,
        s.derived_inserts,
        s.table_chunks,
        s.chunk_allocs,
        s.table_slots_allocated,
        s.table_slot_bytes,
        s.walk_slots_scanned,
        s.instrument_walk_items,
        s.settle_sharp_calls,
        s.settle_sharp_loads,
        s.settle_sharp_max_loads
    )
}

/// r13-compose: the Merger's cumulative work (BranchMergeWork, every field).
fn merge_work_line(w: &BranchMergeWork) -> String {
    format!(
        "merge_attempts={} merge_commits={} merge_refused_scope={} merge_refused_key={} merge_refused_base={} \
         merge_refused_install={} refusals_same_change={} v3_horizon_fallbacks={} stamp_commits={} stamp_prunes={} \
         stamps_held={} stamp_entries={} derive_pages_read={} derive_attribution_seeks={} derive_rows_compared={} \
         derive_subtrees_enumerated={} derive_subtrees_cancelled={} derive_freelist_reads={} derive_refusals_ddl={} \
         derive_refusals_unattributed={} derive_refusals_without_rowid={} derive_refusals_clear_or_delete_all={} \
         derive_keys={} mv4_keys={} mv4_base_reads={}",
        w.merge_attempts,
        w.merge_commits,
        w.merge_refused_scope,
        w.merge_refused_key,
        w.merge_refused_base,
        w.merge_refused_install,
        w.refusals_same_change,
        w.v3_horizon_fallbacks,
        w.stamp_commits,
        w.stamp_prunes,
        w.stamps_held,
        w.stamp_entries,
        w.derive_pages_read,
        w.derive_attribution_seeks,
        w.derive_rows_compared,
        w.derive_subtrees_enumerated,
        w.derive_subtrees_cancelled,
        w.derive_freelist_reads,
        w.derive_refusals_ddl,
        w.derive_refusals_unattributed,
        w.derive_refusals_without_rowid,
        w.derive_refusals_clear_or_delete_all,
        w.derive_keys,
        w.mv4_keys,
        w.mv4_base_reads
    )
}

/// r13-compose A2.F5: the gauges, scored at the END of a steady-state window, never as per-op means.
fn gauge_line(db: &Arc<Database>, n: u64, label: &str, at: &str) -> String {
    let s = db.branch_cat_shape();
    let w = db.branch_merge_work();
    let per = |x: u64| x as f64 / s.resident_states.max(1) as f64;
    format!(
        "GAUGE\tn={n}\tlabel={label}\tat={at}\tresident_states={}\ttable_chunks={}\ttable_slots_allocated={}\t\
         table_slot_bytes={}\ttable_chunks_per_resident={:.6}\ttable_slots_per_resident={:.3}\t\
         table_slot_bytes_per_resident={:.3}\tstamps_held={}\tstamp_entries={}\tensure_chain_max={}\tdirty_branches={}\tlog_len={}\t\
         trunk_overlay_versions={}\trss={}",
        s.resident_states,
        s.table_chunks,
        s.table_slots_allocated,
        s.table_slot_bytes,
        per(s.table_chunks),
        per(s.table_slots_allocated),
        per(s.table_slot_bytes),
        w.stamps_held,
        w.stamp_entries,
        s.ensure_chain_max,
        s.dirty_branches,
        s.log_len,
        s.trunk_overlay_versions,
        rss_bytes()
    )
}

/// r13-compose (third review): what this open's prewarm (P, `R12_PREWARM`) did.
fn prewarm_line(db: &Arc<Database>) -> String {
    let (mode, files, bytes, advised, pages, interior, cache_pages, ns) = db.branch_prewarm();
    format!(
        "prewarm_mode={mode} prewarm_files={files} prewarm_bytes={bytes} prewarm_advised={advised} \
         prewarm_pages={pages} prewarm_interior={interior} prewarm_cache_pages={cache_pages} prewarm_us={:.1}",
        ns as f64 / 1e3
    )
}

fn open_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} arena_high_water={} \
         arena_free={} states={} released_scanned={} branch_loads={} trunk_page_loads={} cat_queries={} \
         cat_rows_read={} touched_slots={} trunk_probes={} trunk_rows={} parked_records={} parked_applied={} \
         catalog_us={:.1} recover_us={:.1} load_us={:.1} replay_us={:.1} \
         collect_us={:.1} referenced_us={:.1} arena_us={:.1} expire_us={:.1} store_total_us={:.1} \
         prewarm_file={} prewarm_slots={} prewarm_read_slots={} prewarm_bytes={} prewarm_ranges={} \
         prewarm_us={:.1}",
        s.snap_bytes,
        s.log_bytes,
        s.records,
        s.snap_branches,
        s.branches,
        s.current_entries,
        s.retained_entries,
        s.trunk_retained,
        s.trunk_children,
        s.referenced_slots,
        s.arena_high_water,
        s.arena_free,
        s.states,
        s.released_scanned,
        s.branch_loads,
        s.trunk_page_loads,
        s.cat_queries,
        s.cat_rows_read,
        s.touched_slots,
        s.trunk_probes,
        s.trunk_rows,
        s.parked_records,
        s.parked_applied,
        s.catalog_ns as f64 / 1e3,
        s.recover_ns as f64 / 1e3,
        s.load_ns as f64 / 1e3,
        s.replay_ns as f64 / 1e3,
        s.collect_ns as f64 / 1e3,
        s.referenced_ns as f64 / 1e3,
        s.arena_ns as f64 / 1e3,
        s.expire_ns as f64 / 1e3,
        s.total_ns as f64 / 1e3,
        s.prewarm_file,
        s.prewarm_slots,
        s.prewarm_read_slots,
        s.prewarm_bytes,
        s.prewarm_ranges,
        s.prewarm_ns as f64 / 1e3
    )
}

/// The counters one operation is charged: deltas of every integer the catalog store exposes. Nothing here
/// queries the catalog (`branch_cat_shape` reads memory only), so taking them does not move them.
/// r11-githost-attr adds `io` (memory only) and Turso's process-wide page reads. `ru` is read FIRST,
/// so a before/after pair charges the op with the first snap's store reads and nothing after the
/// second's getrusage; `measure` takes any census outside both.
#[derive(Clone)]
struct Snap {
    cat: (u64, u64, u64, u64),
    rows_written: u64,
    reads: (u64, u64),
    in_use: i64,
    shape: BranchCatShape,
    mw: BranchMergeWork,
    ru: Usage,
<<<<<<< HEAD
    /// r13-compose (third review): page I/O (process-wide: db reads/writes, WAL reads/writes), the
    /// K8 `written` probe and its split, and V4's base-read counters, so P's cold reads and the KNOWN
    /// K8/MV4 terms have their counters.
    io: [u64; 4],
    twk: (u64, u64),
    split: (u64, u64, u64, u64),
    v4: turso_core::branch::V4Counters,
||||||| 484270f95
=======
    io: BranchIoCounters,
    pio: [u64; 4],
>>>>>>> r11-githost-attr-kv2.noindex
}

/// This process's resource usage (`getrusage(RUSAGE_SELF)`): an attribution instrument for time,
/// NOT load-immune (it sees the page cache, the scheduler and every thread of the process). On this
/// macOS `inblock` stayed 0 for a cold 188 MB read (sota-durable RESUME.md:72), so it is recorded and
/// predicted blind; `minflt`/`majflt` count faults on this process's own memory (the arena is read
/// with pread, never mapped).
#[derive(Clone, Copy, Default)]
struct Usage {
    inblock: i64,
    oublock: i64,
    majflt: i64,
    nivcsw: i64,
    cpu_us: i64,
    minflt: i64,
    nvcsw: i64,
    utime_us: i64,
    stime_us: i64,
}

fn usage() -> Usage {
    // SAFETY: `rusage` is plain old data; getrusage writes the whole struct we pass and owns nothing.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: a valid, exclusively borrowed rusage for RUSAGE_SELF.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if rc != 0 {
        not_a_result(&format!("getrusage failed: {}", std::io::Error::last_os_error()));
    }
    let us = |tv: libc::timeval| tv.tv_sec as i64 * 1_000_000 + tv.tv_usec as i64;
    Usage {
        inblock: ru.ru_inblock as i64,
        oublock: ru.ru_oublock as i64,
        majflt: ru.ru_majflt as i64,
        nivcsw: ru.ru_nivcsw as i64,
        cpu_us: us(ru.ru_utime) + us(ru.ru_stime),
        minflt: ru.ru_minflt as i64,
        nvcsw: ru.ru_nvcsw as i64,
        utime_us: us(ru.ru_utime),
        stime_us: us(ru.ru_stime),
    }
}

fn snap(db: &Arc<Database>) -> Snap {
    // getrusage before any store read (see `Snap`).
    let ru = usage();
    let st = db.branch_stats().unwrap();
    Snap {
        ru,
        cat: db.branch_catalog_counters(),
        rows_written: db.branch_catalog_rows_written(),
        reads: db.branch_read_counters(),
        in_use: st.arena_slots_in_use as i64,
        shape: db.branch_cat_shape(),
<<<<<<< HEAD
        mw: db.branch_merge_work(),
        ru: usage(),
        io: turso_core::branch::page_io(),
        twk: db.branch_twk_counters(),
        split: db.branch_probe_split_counters(),
        v4: db.branch_v4_counters(),
||||||| 484270f95
        ru: usage(),
=======
        io: db.branch_io_counters(),
        pio: page_io(),
>>>>>>> r11-githost-attr-kv2.noindex
    }
}

<<<<<<< HEAD
/// Every counter an op is charged. All but the last five are the store's own integers; the `ru_*`
/// five are `getrusage` deltas (load-dependent; attribution only). r13-compose adds I3-I6, I11's
/// walk counter, and the Merger's work (I7, I8, the derivation), all cumulative deltas; the gauges
/// are printed by `gauge_line` instead.
const NC: usize = 69;
||||||| 484270f95
/// Every counter an op is charged. All but the last five are the store's own integers; the `ru_*`
/// five are `getrusage` deltas (load-dependent; attribution only).
const NC: usize = 25;
=======
/// Every counter an op is charged. The first twenty are the store's own integers; `ru_*` are
/// `getrusage` deltas (load-dependent; attribution only); from `arena_file_reads` on, r11-githost-attr's:
/// `BranchIoCounters` deltas (`_ns` timers are load-dependent, their paired counts are not), Turso's
/// process-wide page reads from database files and WAL frames (`pio_*`: the trunk's and the
/// catalog's), and the census diffs (`cen_*`: pages that turned resident, left, or were appended
/// resident, across the op; -1 where no census was taken), then M7's schema reparses and F-S's
/// source-key reads and adoptions (A1), each `_ns` paired with its count.
const NC: usize = 62;
>>>>>>> r11-githost-attr-kv2.noindex
const COUNTERS: [&str; NC] = [
    "resolve_calls",
    "arena_reads",
    "arena_delta",
    "cat_branch_loads",
    "cat_trunk_page_loads",
    "cat_queries",
    "cat_rows_read",
    "cat_rows_written",
    "trunk_probes",
    "trunk_rows",
    "checkpoints",
    "ckpt_trunk_inserted",
    "ckpt_trunk_deleted",
    "ckpt_rows_written",
    "ckpt_states_walked",
    "ids_resident_visited",
    "ids_catalog_rows",
    "ids_build_rows",
    "table_moved",
    "evicted_states",
    "ensure_cold",
    "ensure_chain_sum",
    "derived_inserts",
    "evicted_with_resident_descendant",
    "walk_items_yielded",
    "walk_slots_scanned",
    "instrument_walk_items",
    "settle_sharp_loads",
    "chunk_allocs",
    "merge_attempts",
    "merge_commits",
    "merge_refused_scope",
    "merge_refused_key",
    "merge_refused_base",
    "merge_refused_install",
    "refusals_same_change",
    "v3_horizon_fallbacks",
    "stamp_commits",
    "stamp_prunes",
    "derive_pages_read",
    "derive_attribution_seeks",
    "derive_rows_compared",
    "derive_subtrees_enumerated",
    "derive_subtrees_cancelled",
    "derive_freelist_reads",
    "derive_keys",
    "mv4_keys",
    "mv4_base_reads",
    "io_db_reads",
    "io_db_writes",
    "io_wal_reads",
    "io_wal_writes",
    "twk_probes",
    "twk_rows",
    "split_twk_reads",
    "split_tva_probes",
    "split_tva_rows",
    "split_tva_reads",
    "v4_base_reads",
    "v4_arena_resolved",
    "v4_refused",
    "v4_retained_examined",
    "v4_cp_trunk_probes",
    "v4_cp_trunk_rows",
    "ru_inblock",
    "ru_oublock",
    "ru_majflt",
    "ru_nivcsw",
    "ru_cpu_us",
    "ru_minflt",
    "ru_nvcsw",
    "ru_utime_us",
    "ru_stime_us",
    "arena_file_reads",
    "arena_read_ns",
    "arena_file_writes",
    "arena_write_ns",
    "arena_syncs",
    "arena_sync_ns",
    "ubc_read_hits",
    "ubc_read_misses",
    "ubc_read_unknown",
    "ubc_write_hits",
    "ubc_write_misses",
    "ubc_write_appends",
    "ubc_write_unknown",
    "resolve_timed",
    "resolve_ns",
    "flushes",
    "flush_ns",
    "cat_loading_ensures",
    "cat_load_ns",
    "parked_applied",
    "pio_db_reads",
    "pio_wal_reads",
    "cen_arena_new",
    "cen_arena_gone",
    "cen_arena_grown",
    "cen_cat_new",
    "cen_cat_gone",
    "cen_catwal_new",
    "schema_reparses",
    "schema_reparse_ns",
    "schema_keys",
    "schema_key_ns",
    "schema_adoptions",
];

/// `cen`: the census and its maps before and after the op, when one was taken.
fn delta(a: &Snap, b: &Snap, cen: Option<(&Census, &CensusMaps, &CensusMaps)>) -> [i64; NC] {
    let d = |x: u64, y: u64| y as i64 - x as i64;
    let (s, t) = (&a.shape, &b.shape);
<<<<<<< HEAD
    let (m, n) = (&a.mw, &b.mw);
||||||| 484270f95
=======
    let (i, j) = (&a.io, &b.io);
    let cen = |name: &str| match cen {
        Some((c, x, y)) => c.diff(name, x, y),
        None => (-1, -1, -1),
    };
    let (arena_new, arena_gone, arena_grown) = cen("arena");
    let (cat_new, cat_gone, _) = cen("cat");
    let (catwal_new, _, catwal_grown) = cen("catwal");
>>>>>>> r11-githost-attr-kv2.noindex
    [
        d(a.reads.0, b.reads.0),
        d(a.reads.1, b.reads.1),
        b.in_use - a.in_use,
        d(a.cat.0, b.cat.0),
        d(a.cat.1, b.cat.1),
        d(a.cat.2, b.cat.2),
        d(a.cat.3, b.cat.3),
        d(a.rows_written, b.rows_written),
        d(s.trunk_probes, t.trunk_probes),
        d(s.trunk_rows, t.trunk_rows),
        d(s.checkpoints, t.checkpoints),
        d(s.ckpt_trunk_inserted, t.ckpt_trunk_inserted),
        d(s.ckpt_trunk_deleted, t.ckpt_trunk_deleted),
        d(s.ckpt_rows_written, t.ckpt_rows_written),
        d(s.ckpt_states_walked, t.ckpt_states_walked),
        d(s.ids_resident_visited, t.ids_resident_visited),
        d(s.ids_catalog_rows, t.ids_catalog_rows),
        d(s.ids_build_rows, t.ids_build_rows),
        d(s.table_moved, t.table_moved),
        d(s.evicted_states, t.evicted_states),
        d(s.ensure_cold, t.ensure_cold),
        d(s.ensure_chain_sum, t.ensure_chain_sum),
        d(s.derived_inserts, t.derived_inserts),
        d(s.evicted_with_resident_descendant, t.evicted_with_resident_descendant),
        d(s.walk_items_yielded, t.walk_items_yielded),
        d(s.walk_slots_scanned, t.walk_slots_scanned),
        d(s.instrument_walk_items, t.instrument_walk_items),
        d(s.settle_sharp_loads, t.settle_sharp_loads),
        d(s.chunk_allocs, t.chunk_allocs),
        d(m.merge_attempts, n.merge_attempts),
        d(m.merge_commits, n.merge_commits),
        d(m.merge_refused_scope, n.merge_refused_scope),
        d(m.merge_refused_key, n.merge_refused_key),
        d(m.merge_refused_base, n.merge_refused_base),
        d(m.merge_refused_install, n.merge_refused_install),
        d(m.refusals_same_change, n.refusals_same_change),
        d(m.v3_horizon_fallbacks, n.v3_horizon_fallbacks),
        d(m.stamp_commits, n.stamp_commits),
        d(m.stamp_prunes, n.stamp_prunes),
        d(m.derive_pages_read, n.derive_pages_read),
        d(m.derive_attribution_seeks, n.derive_attribution_seeks),
        d(m.derive_rows_compared, n.derive_rows_compared),
        d(m.derive_subtrees_enumerated, n.derive_subtrees_enumerated),
        d(m.derive_subtrees_cancelled, n.derive_subtrees_cancelled),
        d(m.derive_freelist_reads, n.derive_freelist_reads),
        d(m.derive_keys, n.derive_keys),
        d(m.mv4_keys, n.mv4_keys),
        d(m.mv4_base_reads, n.mv4_base_reads),
        d(a.io[0], b.io[0]),
        d(a.io[1], b.io[1]),
        d(a.io[2], b.io[2]),
        d(a.io[3], b.io[3]),
        d(a.twk.0, b.twk.0),
        d(a.twk.1, b.twk.1),
        d(a.split.0, b.split.0),
        d(a.split.1, b.split.1),
        d(a.split.2, b.split.2),
        d(a.split.3, b.split.3),
        d(a.v4.base_reads, b.v4.base_reads),
        d(a.v4.base_arena, b.v4.base_arena),
        d(a.v4.base_refused, b.v4.base_refused),
        d(a.v4.base_examined, b.v4.base_examined),
        d(a.v4.cp_probes, b.v4.cp_probes),
        d(a.v4.cp_rows, b.v4.cp_rows),
        b.ru.inblock - a.ru.inblock,
        b.ru.oublock - a.ru.oublock,
        b.ru.majflt - a.ru.majflt,
        b.ru.nivcsw - a.ru.nivcsw,
        b.ru.cpu_us - a.ru.cpu_us,
        b.ru.minflt - a.ru.minflt,
        b.ru.nvcsw - a.ru.nvcsw,
        b.ru.utime_us - a.ru.utime_us,
        b.ru.stime_us - a.ru.stime_us,
        d(i.arena_file_reads, j.arena_file_reads),
        d(i.arena_read_ns, j.arena_read_ns),
        d(i.arena_file_writes, j.arena_file_writes),
        d(i.arena_write_ns, j.arena_write_ns),
        d(i.arena_syncs, j.arena_syncs),
        d(i.arena_sync_ns, j.arena_sync_ns),
        d(i.ubc_read_hits, j.ubc_read_hits),
        d(i.ubc_read_misses, j.ubc_read_misses),
        d(i.ubc_read_unknown, j.ubc_read_unknown),
        d(i.ubc_write_hits, j.ubc_write_hits),
        d(i.ubc_write_misses, j.ubc_write_misses),
        d(i.ubc_write_appends, j.ubc_write_appends),
        d(i.ubc_write_unknown, j.ubc_write_unknown),
        d(i.resolve_timed, j.resolve_timed),
        d(i.resolve_ns, j.resolve_ns),
        d(i.flushes, j.flushes),
        d(i.flush_ns, j.flush_ns),
        d(i.cat_loading_ensures, j.cat_loading_ensures),
        d(i.cat_load_ns, j.cat_load_ns),
        d(i.parked_applied, j.parked_applied),
        d(a.pio[0], b.pio[0]),
        d(a.pio[2], b.pio[2]),
        arena_new,
        arena_gone,
        arena_grown,
        cat_new,
        cat_gone,
        if catwal_new < 0 { catwal_new } else { catwal_new + catwal_grown },
        d(i.schema_reparses, j.schema_reparses),
        d(i.schema_reparse_ns, j.schema_reparse_ns),
        d(i.schema_keys, j.schema_keys),
        d(i.schema_key_ns, j.schema_key_ns),
        d(i.schema_adoptions, j.schema_adoptions),
    ]
}

/// The fields an `OPREC` line carries, by name from `COUNTERS`: what separates the registered
/// mechanisms (PREREG section 3), one line per op, so any split can be made after the run.
const OPREC_FIELDS: [&str; 38] = [
    "resolve_calls",
    "arena_reads",
    "cat_branch_loads",
    "cat_queries",
    "trunk_probes",
    "ru_majflt",
    "ru_minflt",
    "ru_nvcsw",
    "ru_nivcsw",
    "ru_utime_us",
    "ru_stime_us",
    "arena_file_reads",
    "arena_read_ns",
    "arena_file_writes",
    "arena_write_ns",
    "arena_syncs",
    "arena_sync_ns",
    "ubc_read_hits",
    "ubc_read_misses",
    "ubc_write_misses",
    "ubc_write_appends",
    "resolve_ns",
    "flushes",
    "flush_ns",
    "cat_load_ns",
    "parked_applied",
    "pio_db_reads",
    "pio_wal_reads",
    "cen_arena_new",
    "cen_arena_gone",
    "cen_cat_new",
    "cat_loading_ensures",
    "ubc_write_hits",
    "schema_reparses",
    "schema_reparse_ns",
    "schema_keys",
    "schema_key_ns",
    "schema_adoptions",
];

/// One `OPREC` line: the op, its sequence number in the probe, the branch, the branch's age in steps
/// when the op ran (0 for reads, which run after the steady phase), its class, its time, and
/// `OPREC_FIELDS`.
fn oprec(op: &str, seq: usize, id: u64, age: u64, cold: bool, t_us: f64, c: &[i64; NC]) {
    let mut line = format!(
        "OPREC\top={op}\tseq={seq}\tid={id}\tage={age}\tcls={}\tt_us={t_us:.2}",
        if cold { "cold" } else { "warm" }
    );
    for name in OPREC_FIELDS {
        let i = COUNTERS
            .iter()
            .position(|&n| n == name)
            .unwrap_or_else(|| die(&format!("OPREC field {name} is not a counter")));
        line.push_str(&format!("\t{name}={}", c[i]));
    }
    println!("{line}");
}

/// Time `f` between two snaps. The census, if any, is taken before the first snap and after the
/// second, so neither its time nor its getrusage is charged to the op.
fn measure(db: &Arc<Database>, census: Option<&Census>, f: &mut dyn FnMut()) -> (f64, [i64; NC]) {
    let before = census.map(Census::take);
    let a = snap(db);
    let t = Instant::now();
    f();
    let us = t.elapsed().as_secs_f64() * 1e6;
    let b = snap(db);
    let after = census.map(Census::take);
    let cen = match (census, &before, &after) {
        (Some(c), Some(x), Some(y)) => Some((c, x, y)),
        _ => None,
    };
    (us, delta(&a, &b, cen))
}

/// What this probe process has done to each branch, for the COLD / WARM class (r11-githost-attr): an
/// op is COLD when its branch was created before this process opened the store (`id <= steps0`)
/// and nothing in this process has touched it since (created, updated, read, reaped or paired).
struct Touched {
    steps0: u64,
    touched: Vec<bool>,
}

impl Touched {
    fn new(steps0: u64) -> Self {
        Self {
            steps0,
            touched: Vec::new(),
        }
    }

    fn is_cold(&self, id: u64) -> bool {
        id <= self.steps0 && !self.touched.get(id as usize).copied().unwrap_or(false)
    }

    fn touch(&mut self, id: u64) {
        let i = id as usize;
        if self.touched.len() <= i {
            self.touched.resize(i + 1, false);
        }
        self.touched[i] = true;
    }
}

/// Per-op times (us) and counter deltas for one op kind.
#[derive(Default)]
struct Series {
    times: Vec<f64>,
    counters: Vec<[i64; NC]>,
    extra: Vec<Vec<(String, i64)>>,
}

impl Series {
    fn push(&mut self, t_us: f64, c: [i64; NC]) {
        self.times.push(t_us);
        self.counters.push(c);
    }

    fn print(&mut self, op: &str, n: u64, label: &str) {
        if self.times.is_empty() {
            println!("OP\tn={n}\tlabel={label}\top={op}\tk=0");
            return;
        }
        let mut t = self.times.clone();
        t.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| t[((p / 100.0) * (t.len() - 1) as f64).round() as usize];
        let mut line = format!(
            "OP\tn={n}\tlabel={label}\top={op}\tk={}\tp50_us={:.2}\tp90_us={:.2}\tp99_us={:.2}\tmax_us={:.2}\tmean_us={:.2}",
            t.len(),
            pct(50.0),
            pct(90.0),
            pct(99.0),
            t[t.len() - 1],
            t.iter().sum::<f64>() / t.len() as f64
        );
        for (i, name) in COUNTERS.iter().enumerate() {
            let v: Vec<i64> = self.counters.iter().map(|c| c[i]).collect();
            let sum: i64 = v.iter().sum();
            line.push_str(&format!(
                "\t{name}_mean={:.3}\t{name}_min={}\t{name}_max={}",
                sum as f64 / v.len() as f64,
                v.iter().min().unwrap(),
                v.iter().max().unwrap()
            ));
        }
        if let Some(first) = self.extra.first() {
            for (j, (name, _)) in first.iter().enumerate() {
                let v: Vec<i64> = self.extra.iter().map(|e| e[j].1).collect();
                line.push_str(&format!(
                    "\t{name}_mean={:.3}\t{name}_min={}\t{name}_max={}",
                    v.iter().sum::<i64>() as f64 / v.len() as f64,
                    v.iter().min().unwrap(),
                    v.iter().max().unwrap()
                ));
            }
        }
        println!("{line}");
    }
}

/// The store-level step: what `plan` says, against the store. Returns the time of each op kind done.
/// r11-githost-attr: pr_update is also split by class, and the probe's context rides along.
struct Ops {
    new_pr: Series,
    /// Merges that installed, and (real mode) merges the Merger refused: apart, since their mix moves
    /// with N (third review). replay-update's refused merges do no work and are not timed.
    merge: Series,
    merge_refused: Series,
    pr_update: Series,
    reap: Series,
    pr_update_cold: Series,
    pr_update_warm: Series,
    touched: Touched,
    /// A census around each pr_update (`R11_CENSUS_OPS`); new_pr, merge and reap never take one.
    census: Option<Census>,
}

fn do_new_pr(db: &Arc<Database>, trunk: &Arc<Connection>, s: u64) {
    let branch = trunk.fork_branch().unwrap_or_else(|e| not_a_result(&format!("fork {s}: {e}")));
    if branch.id() != sid(s) {
        not_a_result(&format!("fork returned {:?}, expected {:?} (step {s})", branch.id(), sid(s)));
    }
    let conn = branch.connect().unwrap();
    exec(&conn, "BEGIN");
    let row = row_for(s);
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {row}", own_value(s, 0)));
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {}", far_value(s), far_row(row)));
    exec(&conn, "COMMIT");
    drop(conn);
    let _ = branch.into_id();
    let _ = db;
}

fn do_merge(trunk: &Arc<Connection>, s: u64, row: u64) {
    exec(trunk, &format!("UPDATE t SET v = '{}' WHERE id = {row}", merge_value(s)));
}

/// The merging side of a run (r13-compose A1): the Merger in `real` mode, its policy, and how many
/// merges went in and were refused.
struct MergeSide {
    merger: Option<Merger>,
    policy: MergePolicy,
    installed: u64,
    refused: u64,
    /// |O| over the Merger's merges (A6.5's bound is in |O|): sum, min, max.
    owned: (u64, u64, u64),
}

impl MergeSide {
    fn new(trunk: &Arc<Connection>, validator: Validation) -> Self {
        let merger = (merge_mode() == MergeMode::Real)
            .then(|| Merger::new(trunk.clone()).unwrap_or_else(|e| not_a_result(&format!("merger: {e}"))));
        Self {
            merger,
            policy: MergePolicy {
                validation: validator,
                keep_merged: true,
            },
            installed: 0,
            refused: 0,
            owned: (0, u64::MAX, 0),
        }
    }

    fn owned_line(&self) -> String {
        let (sum, min, max) = self.owned;
        format!(
            "owned_sum={sum}\towned_min={}\towned_max={max}",
            if min == u64::MAX { 0 } else { min }
        )
    }
}

/// r13-compose A1: merge PR `p` the way the mode says, and check the outcome against the model's
/// prediction. `real`: the durable Merger (keep-merged). `replay-update`: the same two rows by plain
/// trunk UPDATEs, in one transaction, skipped where the Merger would refuse (A3.F17).
fn do_merge_pr(db: &Arc<Database>, trunk: &Arc<Connection>, side: &mut MergeSide, p: u64, gen: u64, refused: bool) {
    match side.merger.as_mut() {
        Some(merger) => {
            let branch = db
                .branch(sid(p))
                .unwrap_or_else(|e| not_a_result(&format!("attach {p} for merge: {e}")));
            let out = merger
                .merge(branch, side.policy)
                .unwrap_or_else(|e| not_a_result(&format!("merge {p}: {e}")));
            let o = out.owned as u64;
            side.owned = (side.owned.0 + o, side.owned.1.min(o), side.owned.2.max(o));
            match (out.refused, refused) {
                (None, false) => {
                    if out.rows_changed != 2 {
                        not_a_result(&format!("merge {p} installed {} rows, the model 2: {out:?}", out.rows_changed));
                    }
                    side.installed += 1;
                }
                // The refusal must be the validator's own: MV4's Base under v4; under v3h KeyStamp's
                // Key, or Base when the horizon handed the verdict to MV4 (third review).
                (Some(r), true) => {
                    let ok = matches!(
                        (side.policy.validation, r, out.decided_by),
                        (Validation::BaseRead, Refusal::Base, Validation::BaseRead)
                            | (Validation::KeyStamp, Refusal::Key, Validation::KeyStamp)
                            | (Validation::KeyStamp, Refusal::Base, Validation::BaseRead)
                    );
                    if !ok {
                        not_a_result(&format!("merge {p}: refused by the wrong verdict: {out:?}"));
                    }
                    side.refused += 1;
                }
                (got, want) => not_a_result(&format!(
                    "merge {p}: {got:?}, the model predicts refused={want} ({out:?})"
                )),
            }
        }
        None => {
            if refused {
                side.refused += 1;
                return;
            }
            let row = row_for(p);
            exec(trunk, "BEGIN");
            exec(trunk, &format!("UPDATE t SET v = '{}' WHERE id = {row}", own_value(p, gen)));
            exec(trunk, &format!("UPDATE t SET v = '{}' WHERE id = {}", far_value(p), far_row(row)));
            exec(trunk, "COMMIT");
            side.installed += 1;
        }
    }
}

/// N13's gate and A2.F4's identity (third review): an installed merge's two rows read back on the
/// trunk as the PR wrote them, in both merge modes (untimed). A mismatch is NOT A RESULT.
fn installed_rows_are(trunk: &Arc<Connection>, p: u64, gen: u64) {
    let row = row_for(p);
    for (r, want) in [(row, own_value(p, gen)), (far_row(row), far_value(p))] {
        let got = read_v(trunk, r);
        if got != want {
            not_a_result(&format!("merge {p}: the trunk's row {r} = {got:.30}, the PR wrote {want:.30}"));
        }
    }
}

/// The trunk table's content, as one digest (rows, FNV-1a 64 over "id:v\n"): the -MRG arm's trunk
/// must equal the real arm's at the same step (A2.F4), across raws.
fn trunk_digest(trunk: &Arc<Connection>) -> (u64, u64) {
    let rows = trunk.prepare("SELECT id, v FROM t ORDER BY id").unwrap().run_collect_rows().unwrap();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for r in &rows {
        let v = match &r[1] {
            Value::Text(t) => t.as_str().to_string(),
            other => not_a_result(&format!("trunk digest: v is {other:?}")),
        };
        for b in format!("{}:{v}\n", r[0].as_int().unwrap()).bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    (rows.len() as u64, h)
}

fn do_update(db: &Arc<Database>, t: u64, gen: u64) {
    let branch = db
        .branch(sid(t))
        .unwrap_or_else(|e| not_a_result(&format!("attach {t} for update: {e}")));
    let conn = branch.connect().unwrap();
    exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {}", own_value(t, gen), row_for(t)));
    drop(conn);
    let _ = branch.into_id();
}

fn do_reap(db: &Arc<Database>, t: u64) {
    let branch = db
        .branch(sid(t))
        .unwrap_or_else(|e| not_a_result(&format!("attach {t} for reap: {e}")));
    let r = branch.reap().unwrap_or_else(|e| not_a_result(&format!("reap {t}: {e}")));
    if r.deferred {
        not_a_result(&format!("reap {t} deferred: nothing should hold an idle PR branch"));
    }
}

/// Run one schedule step; with `ops`, time and count each op kind.
<<<<<<< HEAD
fn step(
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    model: &mut Model,
    side: &mut MergeSide,
    mut ops: Option<&mut Ops>,
) {
||||||| 484270f95
fn step(db: &Arc<Database>, trunk: &Arc<Connection>, model: &mut Model, mut ops: Option<&mut Ops>) {
=======
fn step(db: &Arc<Database>, trunk: &Arc<Connection>, model: &mut Model, ops: Option<&mut Ops>) {
>>>>>>> r11-githost-attr-kv2.noindex
    let plan = model.advance();
    let Some(ops) = ops else {
        do_new_pr(db, trunk, plan.s);
        if let Some(row) = plan.merge {
            do_merge(trunk, plan.s, row);
        }
        if let Some((t, gen)) = plan.update {
            do_update(db, t, gen);
        }
        if let Some(t) = plan.death {
            do_reap(db, t);
        }
        return;
    };
<<<<<<< HEAD
    timed(ops.as_deref_mut().map(|o| &mut o.new_pr), &mut || do_new_pr(db, trunk, plan.s));
    for merge in &plan.merges {
        match *merge {
            MergePlan::Update(row) => {
                timed(ops.as_deref_mut().map(|o| &mut o.merge), &mut || do_merge(trunk, plan.s, row))
            }
            MergePlan::Pr { p, gen, refused } => {
                if refused && side.merger.is_none() {
                    // replay-update skips what the Merger would refuse: no work, so no timed op.
                    side.refused += 1;
                    continue;
                }
                timed(
                    ops.as_deref_mut().map(|o| if refused { &mut o.merge_refused } else { &mut o.merge }),
                    &mut || do_merge_pr(db, trunk, side, p, gen, refused),
                );
                if !refused {
                    installed_rows_are(trunk, p, gen);
                }
            }
        }
||||||| 484270f95
    timed(ops.as_deref_mut().map(|o| &mut o.new_pr), &mut || do_new_pr(db, trunk, plan.s));
    if let Some(row) = plan.merge {
        timed(ops.as_deref_mut().map(|o| &mut o.merge), &mut || do_merge(trunk, plan.s, row));
=======
    let (us, c) = measure(db, None, &mut || do_new_pr(db, trunk, plan.s));
    ops.new_pr.push(us, c);
    ops.touched.touch(plan.s);
    if let Some(row) = plan.merge {
        let (us, c) = measure(db, None, &mut || do_merge(trunk, plan.s, row));
        ops.merge.push(us, c);
>>>>>>> r11-githost-attr-kv2.noindex
    }
    if let Some((t, gen)) = plan.update {
        let cold = ops.touched.is_cold(t);
        let (us, c) = measure(db, ops.census.as_ref(), &mut || do_update(db, t, gen));
        ops.pr_update.push(us, c);
        if cold {
            ops.pr_update_cold.push(us, c);
        } else {
            ops.pr_update_warm.push(us, c);
        }
        oprec("pr_update", ops.pr_update.times.len(), t, plan.s - t, cold, us, &c);
        ops.touched.touch(t);
    }
    if let Some(t) = plan.death {
        let (us, c) = measure(db, None, &mut || do_reap(db, t));
        ops.reap.push(us, c);
        ops.touched.touch(t);
    }
}

/// Invariants the catalog store can check alone: live states == the model's live count, and the arena
/// holds at least two own pages per live schedule branch, plus the pages each live stack level owns
/// (amendment 8.17: the store's own count, carried in the sidecar), plus the trunk's versions. The
/// EXACT slot check is across arms: at the same step count the port's grow/probe files print its exact
/// arena and trunk counts, and the two stores make the same retain and free decisions (analysis
/// compares them). The trunk count QUERIES the catalog (`branch_trunk_retained`: overlay + catalog rows
/// - reaped since), so this runs only outside measured operations.
fn check_invariants(db: &Arc<Database>, model: &Model, what: &str) {
    let st = db.branch_stats().unwrap();
    let trunk_versions = db.branch_trunk_retained();
    let live = model.live + model.extra.len() as u64;
    if st.live_branches as u64 != live {
        not_a_result(&format!(
            "{what}: store has {} branch states, model {live} ({} schedule + {} stack levels)",
            st.live_branches,
            model.live,
            model.extra.len()
        ));
    }
    let floor = 2 * model.live + model.extra_pages + trunk_versions;
    if (st.arena_slots_in_use as u64) < floor {
        println!(
            "FINDING\tarena floor\t{what}\tarena_in_use={}\tfloor={floor}\tdeficit={}",
            st.arena_slots_in_use,
            floor - st.arena_slots_in_use as u64
        );
        not_a_result(&format!(
            "{what}: arena in use {} < 2 x schedule live {} + stack level pages {} + trunk versions {trunk_versions}",
            st.arena_slots_in_use, model.live, model.extra_pages
        ));
    }
    println!(
        "# invariants {what}: live={} extra={} steps={} arena_in_use={} trunk_versions={trunk_versions} extra_pages={} \
         floor_slack={}",
        model.live,
        model.extra.len(),
        model.steps,
        st.arena_slots_in_use,
        model.extra_pages,
        st.arena_slots_in_use as u64 - floor
    );
}

/// One read probe, planned (expected values computed) before it is timed.
struct ReadPlan {
    id: u64,
    reads: Vec<(u64, String)>,
}

/// A branch's own two rows, and one trunk row that a merge AFTER the fork rewrote (when the schedule has
/// one): the branch must still see the value from before its fork, so a lost pre-image fails the check.
fn plan_read(model: &Model, id: u64, pick: u64) -> ReadPlan {
    let row = row_for(id);
    let mut reads = vec![
        (row, own_value(id, model.gen[id as usize])),
        (far_row(row), far_value(id)),
    ];
    // A trunk write after the fork (the merge of step `id` itself runs after its fork), if any.
    let r = model.later_write(id, pick).unwrap_or(pick % ROWS + 1);
    if r != row && r != far_row(row) {
        reads.push((r, model.trunk_at(r, id)));
    }
    ReadPlan { id, reads }
}

fn exec_read(db: &Arc<Database>, plan: &ReadPlan) {
    let id = plan.id;
    let branch = db.branch(sid(id)).unwrap_or_else(|e| not_a_result(&format!("attach {id}: {e}")));
    let conn = branch.connect().unwrap();
    for (row, want) in &plan.reads {
        let got = read_v(&conn, *row);
        if &got != want {
            not_a_result(&format!("branch {id} row {row} = {got:.30}, want {want:.30}"));
        }
    }
    drop(conn);
    let _ = branch.into_id();
}

fn read_branch(db: &Arc<Database>, model: &Model, id: u64, pick: u64) {
    exec_read(db, &plan_read(model, id, pick));
}

fn grow(args: &Args) {
<<<<<<< HEAD
    let mut sc = read_stepping_sidecar(&args.db);
    let steps0 = sc.steps;
    let db = open_db(&args.db, false);
||||||| 484270f95
    let steps0 = read_steps(&args.db);
    let db = open_db(&args.db, false);
=======
    let steps0 = read_steps(&args.db);
    let db = open_db(&args.db, false, args.eager);
>>>>>>> r11-githost-attr-kv2.noindex
    let trunk = db.connect().unwrap();
    let exists = !trunk
        .prepare("SELECT name FROM sqlite_schema WHERE name = 't'")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .is_empty();
    if !exists {
        if steps0 != 0 {
            not_a_result("sidecar has steps but the database has no table");
        }
        exec(&trunk, &format!("PRAGMA page_size = {}", args.page_size));
        exec(&trunk, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
        exec(&trunk, "BEGIN");
        for row in 1..=ROWS {
            exec(&trunk, &format!("INSERT INTO t VALUES ({row}, '{}')", trunk_initial(row)));
        }
        exec(&trunk, "COMMIT");
        exec(&trunk, "PRAGMA wal_checkpoint(TRUNCATE)");
        println!(
            "# fixture created: rows={ROWS} page_size={} page_count={}",
            int(&trunk, "PRAGMA page_size"),
            int(&trunk, "PRAGMA page_count")
        );
    }
    let ps = int(&trunk, "PRAGMA page_size") as u64;
    if ps != args.page_size {
        not_a_result(&format!("page_size {ps}, expected {}", args.page_size));
    }
    // Trunk durability during growth only (the probe opens with the default). The smoke run under OFF
    // never restarted the trunk WAL (githost-shape raw/smoke2_grow_1024_5000.txt): the default is the
    // registered setting, OFF an arm.
    if args.trunk_sync != "default" {
        exec(&trunk, &format!("PRAGMA synchronous = {}", args.trunk_sync));
    }
    println!("# trunk synchronous during growth: {} ({})", int(&trunk, "PRAGMA synchronous"), args.trunk_sync);
    let wal_path = PathBuf::from(format!("{}-wal", args.db.to_str().unwrap()));
    let mut model = Model::from_sidecar(&sc);
    check_invariants(&db, &model, "grow start");
    let s0 = db.branch_cat_shape();
    let t0 = Instant::now();
    let mut next_report = model.live.next_power_of_two().max(1024);
    let mut side = MergeSide::new(&trunk, args.validator);
    // --to-steps (R1's H axis) bounds the step count instead of the live count.
    while if args.to_steps > 0 { model.steps < args.to_steps } else { model.live < args.n } {
        step(&db, &trunk, &mut model, &mut side, None);
        if model.live >= next_report {
            let sh = db.branch_cat_shape();
            println!(
                "# grow live={} steps={} elapsed_s={:.1} rss={} wal_bytes={} {}",
                model.live,
                model.steps,
                t0.elapsed().as_secs_f64(),
                rss_bytes(),
                size_of(&wal_path),
                shape_line(&sh)
            );
            println!("{}", idset_line(&db, &model, model.live, &args.label, "grow"));
            let _ = std::io::stdout().flush();
            next_report *= 2;
        }
    }
    let grow_s = t0.elapsed().as_secs_f64();
    check_invariants(&db, &model, "grow end");
    let sh = db.branch_cat_shape();
    let st = db.branch_stats().unwrap();
    // Untimed spot checks of the model against the store.
    let live = model.live_ids();
    for i in 0..20u64 {
        let id = live[(mix(i ^ model.steps) % live.len() as u64) as usize];
        read_branch(&db, &model, id, mix(i));
    }
    println!(
        "GROW\tn={}\tsteps={}\tfrom_steps={steps0}\tgrow_s_unlocked={grow_s:.1}\tupdates_skipped={}\tdeaths_skipped={}\t\
         arena_in_use={}\tarena_free={}\tpage_count={}\trss={}\t{}\tcheckpoints_this_run={}\t\
         ckpt_trunk_inserted_this_run={}\tckpt_states_walked_this_run={}\tckpt_rows_written_this_run={}\t\
         merges_installed={}\tmerges_refused={}\tmerges_skipped={}\tmodel_refused={}\tseed={}\tmerge={}",
        model.live,
        model.steps,
        model.updates_skipped,
        model.deaths_skipped,
        st.arena_slots_in_use,
        st.arena_slots_free,
        int(&trunk, "PRAGMA page_count"),
        rss_bytes(),
        shape_line(&sh).replace(' ', "\t"),
        sh.checkpoints - s0.checkpoints,
        sh.ckpt_trunk_inserted - s0.ckpt_trunk_inserted,
        sh.ckpt_states_walked - s0.ckpt_states_walked,
        sh.ckpt_rows_written - s0.ckpt_rows_written,
        side.installed,
        side.refused,
        model.merges_skipped,
        model.merges_refused,
        args.seed,
        merge_mode().name()
    );
    println!("{}", idset_line(&db, &model, model.live, &args.label, "grow_end"));
    drop(side);
    sc.steps = model.steps;
    write_sidecar(&args.db, &sc);
    println!("{}", state_line(&model));
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!("# grow closed in {:.1} us; {}", t.elapsed().as_secs_f64() * 1e6, files_line(&args.db));
}

<<<<<<< HEAD
/// The resident cap this process runs under (F-W3's knob, applied by `open_db`).
fn arm_cap() -> Option<usize> {
    std::env::var("R11_RESIDENT_CAP").ok().map(|v| v.parse().unwrap_or_else(|_| die(&format!("bad R11_RESIDENT_CAP {v:?}"))))
}

/// r13-compose A10, A11 and A5.7: force a checkpoint, run the fixed run-in (`--runin`, 25,000 steps)
/// untimed while recording the steps at which AUTO checkpoints ran, and take C_arm as their mean
/// spacing. Then step (untimed) to the next auto checkpoint, and time a window of m x C_arm steps,
/// m = ceil(52,000 / C_arm) (A8.7), starting right after it. NOT A RESULT: fewer than 2 auto checkpoints in
/// the run-in (C_arm unmeasurable), no auto checkpoint within 2 x C_arm after it, fewer than 2
/// checkpoints in the window, or (cap set) fewer than 2 EVICTING ones. A period inside the window more
/// than 5% from C_arm is reported with the phase bound (W mod C_win) / W, C_win the window's mean
/// period; the analysis treats an effect inside that bound as NOT A RESULT (A5.7). Returns the WINDOW
/// line.
fn steady_window(
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    model: &mut Model,
    side: &mut MergeSide,
    ops: &mut Ops,
    args: &Args,
    n: u64,
) -> String {
    let ckpts = || db.branch_cat_shape().checkpoints;
    let t = Instant::now();
    db.branch_compact_now()
        .unwrap_or_else(|e| not_a_result(&format!("A10 forced checkpoint: {e}")));
    let forced_us = t.elapsed().as_secs_f64() * 1e6;
    let runin_from = model.steps;
    let mut auto_at = Vec::new();
    for _ in 0..args.runin {
        let c0 = ckpts();
        step(db, trunk, model, side, None);
        let c1 = ckpts();
        if c1 > c0 + 1 {
            not_a_result(&format!("step {} ran {} checkpoints", model.steps, c1 - c0));
        }
        if c1 > c0 {
            auto_at.push(model.steps);
        }
    }
    if auto_at.len() < 2 {
        not_a_result(&format!(
            "C_arm unmeasurable: {} auto checkpoints in the {}-step run-in (steps {:?})",
            auto_at.len(),
            args.runin,
            auto_at
        ));
    }
    let c_arm = (auto_at[auto_at.len() - 1] - auto_at[0]) / (auto_at.len() as u64 - 1);
    // Align: the window starts right after an auto checkpoint.
    let mut align = 0u64;
    loop {
        let c0 = ckpts();
        step(db, trunk, model, side, None);
        align += 1;
        if ckpts() > c0 {
            break;
        }
        if align > 2 * c_arm {
            not_a_result(&format!("no auto checkpoint within 2 x C_arm = {} steps after the run-in", 2 * c_arm));
        }
    }
    let start = model.steps;
    // A8.7: m = ceil(52,000 / C_arm), so a window holds >= 1,000 reaps (0.02 per step, less skips),
    // A2.F8's floor (third review: at 50,000 a C_arm near 10,000 left it at ~980).
    let m = 52_000u64.div_ceil(c_arm);
    let w_steps = m * c_arm;
    let cap = arm_cap();
    let mut at = Vec::new();
    let mut evicting = 0u64;
    for _ in 0..w_steps {
        let sh0 = db.branch_cat_shape();
        let (c0, e0) = (sh0.checkpoints, sh0.evicted_states);
        step(db, trunk, model, side, Some(&mut *ops));
        let sh = db.branch_cat_shape();
        if sh.checkpoints > c0 {
            at.push(model.steps);
            if sh.evicted_states > e0 {
                evicting += 1;
            }
            // Per checkpoint (third review: totals alone hide the per-checkpoint bounds, A2.F8/F9).
            println!(
                "CKPT\tn={n}\tstep={}\tdirty_before={}\tresident_before={}\tckpt_branch_rows={}\tckpt_states_walked={}\t\
                 ckpt_rows_written={}\tckpt_trunk_inserted={}\tckpt_trunk_deleted={}\tevicted_states={}\tcheckpoint_us={:.1}\t\
                 walk_slots_scanned={}\tinstrument_walk_items={}\tresident_after={}",
                model.steps,
                sh0.dirty_branches,
                sh0.resident_states,
                sh.ckpt_branch_rows - sh0.ckpt_branch_rows,
                sh.ckpt_states_walked - sh0.ckpt_states_walked,
                sh.ckpt_rows_written - sh0.ckpt_rows_written,
                sh.ckpt_trunk_inserted - sh0.ckpt_trunk_inserted,
                sh.ckpt_trunk_deleted - sh0.ckpt_trunk_deleted,
                sh.evicted_states - sh0.evicted_states,
                (sh.checkpoint_ns - sh0.checkpoint_ns) as f64 / 1e3,
                sh.walk_slots_scanned - sh0.walk_slots_scanned,
                sh.instrument_walk_items - sh0.instrument_walk_items,
                sh.resident_states
            );
        }
    }
    let mut periods = Vec::new();
    let mut prev = start;
    for &c in &at {
        periods.push(c - prev);
        prev = c;
    }
    let drift = periods
        .iter()
        .map(|&p| (p as f64 - c_arm as f64).abs() / c_arm as f64)
        .fold(0.0f64, f64::max);
    let c_win = if periods.is_empty() { 0 } else { periods.iter().sum::<u64>() / periods.len() as u64 };
    let phase_bound = if c_win == 0 { 1.0 } else { (w_steps % c_win) as f64 / w_steps as f64 };
    let line = format!(
        "WINDOW\tn={n}\tlabel={}\tforced_ckpt_us={forced_us:.1}\trunin={}\trunin_from_step={runin_from}\t\
         runin_auto_ckpt_steps={:?}\tc_arm={c_arm}\talign_steps={align}\tstart_step={start}\tm={m}\tw_steps={w_steps}\t\
         cap={}\twindow_ckpts={}\twindow_ckpt_steps={:?}\tevicting_ckpts={evicting}\tperiods={:?}\tmax_drift={drift:.4}\t\
         c_win={c_win}\tphase_bound={phase_bound:.6}\tdrift_over_5pct={}",
        args.label,
        args.runin,
        auto_at,
        cap.map_or("unset".to_string(), |c| c.to_string()),
        at.len(),
        at,
        periods,
        drift > 0.05
    );
    if at.len() < 2 {
        println!("{line}");
        not_a_result(&format!("{} checkpoints in the window (A11 needs >= 2)", at.len()));
    }
    if cap.is_some() && evicting < 2 {
        println!("{line}");
        not_a_result(&format!("{evicting} evicting checkpoints in the window at cap {cap:?} (A11 needs >= 2)"));
    }
    line
}

||||||| 484270f95
=======
/// One timed read of `plan`, charged to the pooled series and to its class's (`series` = pooled,
/// cold, warm), with its `OPREC` line; then the branch counts as touched.
fn timed_read(
    db: &Arc<Database>,
    census: Option<&Census>,
    touched: &mut Touched,
    op: &str,
    seq: usize,
    plan: &ReadPlan,
    series: &mut [Series; 3],
) {
    let cold = touched.is_cold(plan.id);
    let (us, c) = measure(db, census, &mut || exec_read(db, plan));
    let extra = vec![("rows_read".to_string(), plan.reads.len() as i64)];
    for k in [0, if cold { 1 } else { 2 }] {
        series[k].push(us, c);
        series[k].extra.push(extra.clone());
    }
    oprec(op, seq, plan.id, 0, cold, us, &c);
    touched.touch(plan.id);
}

>>>>>>> r11-githost-attr-kv2.noindex
fn probe(args: &Args) {
    let mut sc = read_stepping_sidecar(&args.db);
    let steps0 = sc.steps;
    if steps0 == 0 {
        not_a_result("probe of an ungrown database");
    }
    let files_before = files_line(&args.db);
    let rss0 = rss_bytes();
    let t = Instant::now();
    let db = open_db(&args.db, true, args.eager);
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let connect_us = t.elapsed().as_secs_f64() * 1e6;
    let rss1 = rss_bytes();
<<<<<<< HEAD
    let mut model = Model::from_sidecar(&sc);
||||||| 484270f95
    let mut model = Model::replay(steps0);
=======
    // r11-githost-attr: whole-file page-cache residency at fixed points (system calls only; the
    // files' data is never read by the census). Only with R11_CENSUS_OPS set (A1): a timed run maps
    // nothing, as the banked harness did, because whether mapping and unmapping a file changes later
    // write or fsync timing on APFS is unmeasured.
    let census_on = std::env::var_os("R11_CENSUS_OPS").is_some();
    let whole = census_on.then(|| Census::open(&args.db));
    let resident = |n: u64, label: &str, at: &str| {
        if let Some(whole) = &whole {
            println!("{}", whole.line(n, label, at));
        }
    };
    resident(args.n, &args.label, "after_open");
    let mut model = Model::replay(steps0);
>>>>>>> r11-githost-attr-kv2.noindex
    if model.live != args.n {
        not_a_result(&format!("probe --n {} but the model has {} live", args.n, model.live));
    }
    // C-R parks the log tail's Commits to branches that are not resident; the first slot count applies
    // them all (a12-durable-open PREREG A7's SETTLE). Charged here: outside the open, outside every op.
    let (c0, sh0) = (db.branch_catalog_counters(), db.branch_cat_shape());
    let t = Instant::now();
    db.branch_stats().unwrap_or_else(|e| not_a_result(&format!("settle: {e}")));
    let settle_us = t.elapsed().as_secs_f64() * 1e6;
    let (c1, sh1) = (db.branch_catalog_counters(), db.branch_cat_shape());
    println!(
        "SETTLE\tn={}\tlabel={}\tsettle_us={settle_us:.1}\tbranch_loads={}\tcat_queries={}\tcat_rows_read={}\t\
         trunk_probes={}\ttrunk_rows={}\tresident_states={}",
        args.n,
        args.label,
        c1.0 - c0.0,
        c1.2 - c0.2,
        c1.3 - c0.3,
        sh1.trunk_probes - sh0.trunk_probes,
        sh1.trunk_rows - sh0.trunk_rows,
        sh1.resident_states
    );
    resident(args.n, &args.label, "after_settle");
    check_invariants(&db, &model, "after open");
    let os = db.branch_open_stats();
    let page_count = int(&trunk, "PRAGMA page_count") as u32;
    let n = model.live;
    let label = &args.label;
    println!(
        "OPEN\tn={n}\tlabel={label}\tsteps={}\topen_us={open_us:.1}\ttrunk_connect_us={connect_us:.1}\trss_before={rss0}\t\
         rss_after_open={rss1}\tpage_count={page_count}\t{}\t{}\tfiles_before: {files_before}",
        model.steps,
        format!("{} {}", open_line(&os), prewarm_line(&db)).replace(' ', "\t"),
        shape_line(&db.branch_cat_shape()).replace(' ', "\t")
    );
    println!("{}", idset_line(&db, &model, n, label, "open"));
    let _ = std::io::stdout().flush();

    // r13-compose A10/A11: the forced checkpoint, the run-in, and the steady-state window.
    let mut side = MergeSide::new(&trunk, args.validator);
    let mut ops = Ops {
        new_pr: Series::default(),
        merge: Series::default(),
        merge_refused: Series::default(),
        pr_update: Series::default(),
        reap: Series::default(),
        pr_update_cold: Series::default(),
        pr_update_warm: Series::default(),
        touched: Touched::new(steps0),
        census: census_on.then(|| Census::open(&args.db)),
    };
    let window = steady_window(&db, &trunk, &mut model, &mut side, &mut ops, args, n);
    check_invariants(&db, &model, "after steady ops");
<<<<<<< HEAD
    ops.new_pr.print("new_pr", n, label);
    ops.merge.print("merge", n, label);
    ops.merge_refused.print("merge_refused", n, label);
    ops.pr_update.print("pr_update", n, label);
    ops.reap.print("reap", n, label);
    println!("{window}");
    println!("{}", gauge_line(&db, n, label, "window_end"));
    let mw = db.branch_merge_work();
    println!(
        "A31\tn={n}\tlabel={label}\tstamp_entries={}\tstamps_held={}\tmodel_trunk_writes_since_oldest={}\twithin_bound={}",
        mw.stamp_entries,
        mw.stamps_held,
        model.trunk_writes_since_oldest(),
        mw.stamp_entries <= model.trunk_writes_since_oldest()
    );
    let (rows, digest) = trunk_digest(&trunk);
    println!("TRUNKDIGEST\tn={n}\tlabel={label}\tat=window_end\tsteps={}\trows={rows}\tdigest={digest:#018x}", model.steps);
    println!(
        "MERGEWORK\tn={n}\tlabel={label}\tat=window_end\tmerges_installed={}\tmerges_refused={}\tmerges_skipped={}\t\
         model_refused={}\t{}\t{}",
        side.installed,
        side.refused,
        model.merges_skipped,
        model.merges_refused,
        side.owned_line(),
        merge_work_line(&db.branch_merge_work()).replace(' ', "\t")
    );
||||||| 484270f95
    ops.new_pr.print("new_pr", n, label);
    ops.merge.print("merge", n, label);
    ops.pr_update.print("pr_update", n, label);
    ops.reap.print("reap", n, label);
=======
    let Ops {
        mut new_pr,
        mut merge,
        mut pr_update,
        mut reap,
        mut pr_update_cold,
        mut pr_update_warm,
        mut touched,
        census,
    } = ops;
    let census = census.as_ref();
    new_pr.print("new_pr", n, label);
    merge.print("merge", n, label);
    pr_update.print("pr_update", n, label);
    reap.print("reap", n, label);
    pr_update_cold.print("pr_update.cold", n, label);
    pr_update_warm.print("pr_update.warm", n, label);
    resident(n, label, "after_steady");
>>>>>>> r11-githost-attr-kv2.noindex

    let live = model.live_ids();
    let timed = |series: &mut Series, f: &mut dyn FnMut() -> Vec<(String, i64)>| {
        let a = snap(&db);
        let t = Instant::now();
        let extra = f();
        let us = t.elapsed().as_secs_f64() * 1e6;
        let b = snap(&db);
        series.push(us, delta(&a, &b, None));
        series.extra.push(extra);
    };

<<<<<<< HEAD
    // Reads of old branches, planned before they are timed. r13-compose: 1,000 of each (A2.F8's
    // floor of 1,000 measured ops per kind); the draws carry W_K (A4.S1). read_oldest reads the
    // 1,000 smallest live ids once each.
    let mut s = Series::default();
    for i in 0..1000u64 {
        let id = live[(mix(i ^ 0x51 ^ w()) % live.len() as u64) as usize];
        let plan = plan_read(&model, id, mix(i ^ 0x52 ^ w()));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
||||||| 484270f95
    // Reads of old branches, planned before they are timed.
    let mut s = Series::default();
    for i in 0..200u64 {
        let id = live[(mix(i ^ 0x51) % live.len() as u64) as usize];
        let plan = plan_read(&model, id, mix(i ^ 0x52));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
=======
    // Reads of old branches, planned before they are timed.
    let mut s: [Series; 3] = Default::default();
    for i in 0..200u64 {
        let id = live[(mix(i ^ 0x51) % live.len() as u64) as usize];
        let plan = plan_read(&model, id, mix(i ^ 0x52));
        timed_read(&db, census, &mut touched, "read_old", i as usize + 1, &plan, &mut s);
>>>>>>> r11-githost-attr-kv2.noindex
    }
<<<<<<< HEAD
    s.print("read_old", n, label);
    let mut s = Series::default();
    for &id in live.iter().take(1000) {
        let plan = plan_read(&model, id, mix(id ^ 0x53 ^ w()));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
||||||| 484270f95
    s.print("read_old", n, label);
    let mut s = Series::default();
    for &id in live.iter().take(50) {
        let plan = plan_read(&model, id, mix(id ^ 0x53));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
=======
    let [mut pooled, mut cold, mut warm] = s;
    pooled.print("read_old", n, label);
    cold.print("read_old.cold", n, label);
    warm.print("read_old.warm", n, label);
    let mut s: [Series; 3] = Default::default();
    for (i, &id) in live.iter().take(50).enumerate() {
        let plan = plan_read(&model, id, mix(id ^ 0x53));
        timed_read(&db, census, &mut touched, "read_oldest", i + 1, &plan, &mut s);
>>>>>>> r11-githost-attr-kv2.noindex
    }
    let [mut pooled, mut cold, mut warm] = s;
    pooled.print("read_oldest", n, label);
    cold.print("read_oldest.cold", n, label);
    warm.print("read_oldest.warm", n, label);
    resident(n, label, "after_reads");

    // PAIRS (r11-githost-attr PREREG section 3, as amended by A1): 2 x PAIRS distinct cold
    // branches from the update window as it stood at the open (ids steps0 + 1 - W0 ..= steps0, W0 =
    // ceil(0.0237 (steps0 + 1))): the pre-open part of every window the steady phase's pr_updates
    // drew from. Order: by mix(id ^ 0x71), a fixed permutation. The first PAIRS are updated twice in
    // a row, the rest read twice in a row: the second op of a pair is warm by construction, on the
    // same branch through the same path. They run BEFORE `list`, whose first call (F-W1) reads the
    // catalog. Known asymmetry (A1, d3): the warm update pops the slot the cold one just freed (LIFO
    // free list), so its write lands in place; the judge's R5 compares the read side only.
    let w0 = ((OPEN_WINDOW * (steps0 + 1) as f64).ceil() as u64).max(1);
    let mut chosen: Vec<u64> = ((steps0 + 1).saturating_sub(w0)..=steps0)
        .filter(|&id| id >= 1 && !model.dead[id as usize] && touched.is_cold(id))
        .collect();
    chosen.sort_by_key(|&id| (mix(id ^ 0x71), id));
    if chosen.len() < 2 * PAIRS {
        not_a_result(&format!(
            "pairs: only {} untouched branches in the open-time update window of {w0}",
            chosen.len()
        ));
    }
    chosen.truncate(2 * PAIRS);
    let (mut uc, mut uw) = (Series::default(), Series::default());
    for (seq, &id) in chosen[..PAIRS].iter().enumerate() {
        for warm in [false, true] {
            // The model follows each pair update, so the checks after it read the value written.
            model.gen[id as usize] += 1;
            let gen = model.gen[id as usize];
            let (us, c) = measure(&db, census, &mut || do_update(&db, id, gen));
            let (series, op) = if warm {
                (&mut uw, "pair_update.warm")
            } else {
                (&mut uc, "pair_update.cold")
            };
            series.push(us, c);
            oprec(op, seq + 1, id, model.steps - id, !warm, us, &c);
            touched.touch(id);
        }
    }
    uc.print("pair_update.cold", n, label);
    uw.print("pair_update.warm", n, label);
    let (mut rc, mut rw) = (Series::default(), Series::default());
    for (seq, &id) in chosen[PAIRS..].iter().enumerate() {
        let plan = plan_read(&model, id, mix(id ^ 0x73));
        for warm in [false, true] {
            let (us, c) = measure(&db, census, &mut || exec_read(&db, &plan));
            let (series, op) = if warm {
                (&mut rw, "pair_read.warm")
            } else {
                (&mut rc, "pair_read.cold")
            };
            series.push(us, c);
            series.extra.push(vec![("rows_read".to_string(), plan.reads.len() as i64)]);
            oprec(op, seq + 1, id, 0, !warm, us, &c);
            touched.touch(id);
        }
    }
    rc.print("pair_read.cold", n, label);
    rw.print("pair_read.warm", n, label);
    resident(n, label, "after_pairs");

    // List, checked against the model's live set (store ids).
    let listed = model.store_live_ids();
    let (want_sum, want_xor) = listed.iter().fold((0u64, 0u64), |(a, x), &id| (a.wrapping_add(id), x ^ mix(id)));
    let mut s = Series::default();
    for _ in 0..5 {
        timed(&mut s, &mut || {
            let ids = db.branch_ids().unwrap();
            let (sum, xor) = ids.iter().fold((0u64, 0u64), |(a, x), id| (a.wrapping_add(id.0), x ^ mix(id.0)));
            if ids.len() != listed.len() || sum != want_sum || xor != want_xor {
                not_a_result(&format!("list: {} ids, model {}", ids.len(), listed.len()));
            }
            vec![("out".to_string(), ids.len() as i64)]
        });
    }
    s.print("list", n, label);

    resident(n, label, "after_list");

    // (No DIFF in the catalog arm: its tree has no diff instrument; diff is measured on the port.)

    // Compaction: the whole-state checkpoint, on demand.
    let mut s = Series::default();
    for _ in 0..2 {
        timed(&mut s, &mut || {
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("compact: {e}")));
            let sh = db.branch_cat_shape();
            vec![("log_len_after".to_string(), sh.log_len as i64)]
        });
    }
    s.print("compact", n, label);
    check_invariants(&db, &model, "before close");
    let sh = db.branch_cat_shape();
    let st = db.branch_stats().unwrap();
    println!(
        "SHAPE\tn={n}\tlabel={label}\tsteps={}\tarena_in_use={}\tarena_free={}\trss={}\tupdates_skipped={}\tdeaths_skipped={}\t{}",
        model.steps,
        st.arena_slots_in_use,
        st.arena_slots_free,
        rss_bytes(),
        model.updates_skipped,
        model.deaths_skipped,
        shape_line(&sh).replace(' ', "\t")
    );
<<<<<<< HEAD
    println!("{}", idset_line(&db, &model, n, label, "shape"));
    sc.steps = model.steps;
    write_sidecar(&args.db, &sc);
    drop(side);
||||||| 484270f95
    write_steps(&args.db, model.steps);
=======
    // Reopened: a compaction may have replaced the log and snapshot files under the old handles.
    if census_on {
        println!("{}", Census::open(&args.db).line(n, label, "before_close"));
    }
    // PREREG A3.10: which source key this process ran, from the engine's own count (not the
    // environment): keys_v2 = keys when R11_SCHEMA_KEY=2 took effect, 0 under key v1.
    let keys = db.branch_io_counters();
    println!(
        "SCHEMAKEY\tn={n}\tlabel={label}\tkeys={}\tkeys_v2={}\tadoptions={}\treparses={}",
        keys.schema_keys, keys.schema_keys_v2, keys.schema_adoptions, keys.schema_reparses
    );
    // The pair ops rewrote branches outside the schedule: no later process may replay against them.
    write_tainted_steps(&args.db, model.steps);
>>>>>>> r11-githost-attr-kv2.noindex
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!(
        "CLOSE\tn={n}\tlabel={label}\tclose_us={:.1}\tfiles_after: {}",
        t.elapsed().as_secs_f64() * 1e6,
        files_line(&args.db)
    );
}

// ---------------------------------------------------------------------------------------------
// r13-compose S1: per-ref stacks (PREREG §3.3, A2.F10, A3.L1, A4.X3).

/// Stack rows are INSERTed with ids above the schedule's table, so a stack never conflicts with the
/// schedule's rows or with another stack, and the model's trunk rows stay exact after a land-top.
const STACK_ROW_BASE: u64 = 1_000_000_000;

fn stack_row(serial: u64, level: u64, x: u64) -> u64 {
    STACK_ROW_BASE + serial * 10_000 + level * 2 + x
}

fn stack_value(serial: u64, level: u64, x: u64, gen: u64) -> String {
    let p = format!("s{serial:08}l{level:05}x{x}g{gen:06}");
    format!("{p}{}", "y".repeat(VALUE_LEN - p.len()))
}

/// One stack built by this process: its levels and I13's pages per level.
struct Stack {
    serial: u64,
    /// Level ids, root (a trunk child) first, top last.
    ids: Vec<BranchId>,
    /// I13 (A4.X3): each level's distinct pages over current ∪ retained, read from its resident
    /// state when its child forked (the store's own count, not a model).
    pages: Vec<u64>,
    /// The top's own pages, read the same way at the end of the build (amendment 8.17).
    top_pages: u64,
    /// Arena slots and trunk versions the build added (amendment 8.17's exact build identity).
    arena_added: u64,
    trunk_added: u64,
}

impl Stack {
    fn d(&self) -> u64 {
        self.ids.len() as u64
    }

    fn top(&self) -> BranchId {
        *self.ids.last().unwrap()
    }

    /// The expected derived_inserts of a cold load of the top: I13 summed over the d - 1 ancestors.
    fn ancestors_pages(&self) -> u64 {
        self.pages.iter().sum()
    }

    /// Every level's own pages, root first, top last: the arena floor's stack term (amendment 8.17).
    fn level_pages(&self) -> Vec<u64> {
        let mut v = self.pages.clone();
        v.push(self.top_pages);
        v
    }
}

/// Insert level `level`'s k = 2 own rows into `branch`, in one transaction.
fn insert_level_rows(branch: &turso_core::branch::Branch, serial: u64, level: u64, gen: u64) {
    let conn = branch.connect().unwrap_or_else(|e| not_a_result(&format!("connect level {level}: {e}")));
    exec(&conn, "BEGIN");
    for x in 0..2 {
        exec(
            &conn,
            &format!(
                "INSERT INTO t VALUES ({}, '{}')",
                stack_row(serial, level, x),
                stack_value(serial, level, x, gen)
            ),
        );
    }
    exec(&conn, "COMMIT");
}

/// Build a stack of depth d: a trunk child, then d - 1 levels each forked from the last, every level
/// inserting its own two rows before its child forks. Untimed. Amendment 8.17's exact build identity:
/// a level writes its rows in one transaction before its child forks, so the arena slots the build
/// adds, less the trunk versions it adds, equal the sum of its levels' own pages exactly; any
/// difference is a FINDING and NOT A RESULT.
fn build_stack(db: &Arc<Database>, trunk: &Arc<Connection>, serial: u64, d: u64) -> Stack {
    let arena = |db: &Arc<Database>| db.branch_stats().unwrap().arena_slots_in_use as u64;
    let (arena0, trunk0) = (arena(db), db.branch_trunk_retained());
    let mut st = Stack {
        serial,
        ids: Vec::new(),
        pages: Vec::new(),
        top_pages: 0,
        arena_added: 0,
        trunk_added: 0,
    };
    let mut level = trunk.fork_branch().unwrap_or_else(|e| not_a_result(&format!("stack root: {e}")));
    for i in 0..d {
        insert_level_rows(&level, serial, i, 0);
        if i + 1 == d {
            st.top_pages = db
                .branch_state_pages(level.id())
                .unwrap_or_else(|| not_a_result(&format!("stack top (level {i}) not resident after its build")));
            st.ids.push(level.into_id());
            break;
        }
        let next = level.fork().unwrap_or_else(|e| not_a_result(&format!("stack level {}: {e}", i + 1)));
        st.pages.push(
            db.branch_state_pages(level.id())
                .unwrap_or_else(|| not_a_result(&format!("stack level {i} not resident at its child's fork"))),
        );
        st.ids.push(level.into_id());
        level = next;
    }
    let (arena1, trunk1) = (arena(db), db.branch_trunk_retained());
    st.arena_added = arena1.wrapping_sub(arena0);
    st.trunk_added = trunk1.wrapping_sub(trunk0);
    let want: u64 = st.level_pages().iter().sum();
    if arena1 < arena0 || trunk1 < trunk0 || st.arena_added - st.trunk_added != want {
        println!(
            "FINDING\tS1 stack pages\td={d}\tserial={serial}\tarena_before={arena0}\tarena_after={arena1}\t\
             trunk_before={trunk0}\ttrunk_after={trunk1}\tlevel_pages_sum={want}"
        );
        not_a_result(&format!(
            "stack d={d} serial={serial}: the build added {arena0}->{arena1} arena slots and {trunk0}->{trunk1} trunk \
             versions, but its levels own {want} pages"
        ));
    }
    st
}

/// G-1 (amendment 8.17): the -MRG arms' land-top, by plain trunk writes of exactly the rows the Merger
/// installs for a fresh stack (A2.F4's replay rule): its 2d rows at generation 0, INSERTs (no stack
/// row is in the trunk), in one transaction. The top's handle is then dropped, as `land_top` drops it
/// with keep_merged off. Returns the rows written.
fn replay_land_top(trunk: &Arc<Connection>, top: turso_core::branch::Branch, serial: u64, d: u64) -> u64 {
    exec(trunk, "BEGIN");
    let mut n = 0u64;
    for level in 0..d {
        for x in 0..2 {
            exec(
                trunk,
                &format!(
                    "INSERT INTO t VALUES ({}, '{}')",
                    stack_row(serial, level, x),
                    stack_value(serial, level, x, 0)
                ),
            );
            n += 1;
        }
    }
    exec(trunk, "COMMIT");
    drop(top);
    n
}

/// A TRUNKDIGEST line (probe's format) at `at`.
fn print_trunk_digest(trunk: &Arc<Connection>, n: u64, label: &str, at: &str, steps: u64) {
    let (rows, digest) = trunk_digest(trunk);
    println!("TRUNKDIGEST\tn={n}\tlabel={label}\tat={at}\tsteps={steps}\trows={rows}\tdigest={digest:#018x}");
}

/// What the top of `st` must read: its own second row (an amend at d/2 = 1 rewrites the top's FIRST
/// row), the root's first row, the middle level's second row (the top sees every level as of the next
/// level's fork, so an amend below it is invisible to it), and one trunk row of the schedule's table
/// as the stack's root saw it at its fork, `fork_step` (S1 forks every stack after step s0, so it is
/// s0 + 1; a later schedule write or a restart's merge is invisible to the stack: second review).
fn stack_read_plan(model: &Model, st_serial: u64, d: u64, pick: u64, fork_step: u64) -> Vec<(u64, String)> {
    let mut reads = vec![(stack_row(st_serial, d - 1, 1), stack_value(st_serial, d - 1, 1, 0))];
    if d > 1 {
        reads.push((stack_row(st_serial, 0, 0), stack_value(st_serial, 0, 0, 0)));
        let mid = (d / 2).max(1) - 1;
        reads.push((stack_row(st_serial, mid, 1), stack_value(st_serial, mid, 1, 0)));
        // The amended row itself, at generation 0: an amend below the top is invisible to it, so a
        // top that resolved the amended level's new version reads a lost pre-image (third review).
        reads.push((stack_row(st_serial, mid, 0), stack_value(st_serial, mid, 0, 0)));
    }
    let r = pick % ROWS + 1;
    reads.push((r, model.trunk_at(r, fork_step)));
    reads
}

fn read_rows(db: &Arc<Database>, id: BranchId, reads: &[(u64, String)]) {
    let branch = db.branch(id).unwrap_or_else(|e| not_a_result(&format!("attach {id:?}: {e}")));
    let conn = branch.connect().unwrap();
    for (row, want) in reads {
        let got = read_v(&conn, *row);
        if &got != want {
            not_a_result(&format!("branch {id:?} row {row} = {got:.30}, want {want:.30}"));
        }
    }
    drop(conn);
    let _ = branch.into_id();
}

/// Time `f`, charging it every counter (as the steady-state ops are charged).
fn timed_op(db: &Arc<Database>, series: &mut Series, f: &mut dyn FnMut() -> Vec<(String, i64)>) -> [i64; NC] {
    let a = snap(db);
    let t = Instant::now();
    let extra = f();
    let us = t.elapsed().as_secs_f64() * 1e6;
    let b = snap(db);
    let c = delta(&a, &b);
    series.push(us, c);
    series.extra.push(extra);
    c
}

fn counter(c: &[i64; NC], name: &str) -> i64 {
    c[COUNTERS.iter().position(|&n| n == name).unwrap_or_else(|| die(&format!("no counter {name}")))]
}

/// `branch_githost stack`: on a clone of a grown fixture, A10 (forced checkpoint + run-in), then per
/// depth d: one main stack, and per op kind `--touches` measured ops on it: push (fork a child of
/// the top and insert its two rows; each pushed child is reaped untimed before the next push), amend
/// at d/2 (a new generation of that level's first row), warm top read, cold top read (cap 0 and a
/// forced checkpoint, the arm's cap restored, then the read, with no op between: A4.X3), and then
/// `--lands` land-tops, each on a fresh stack of depth d (reaped afterwards). Scored identities on
/// every cold top read (A3.L1, A4.X3): ensure_chain_sum = d and derived_inserts = I13's sum over the
/// d - 1 ancestors; a deviation prints a FINDING line. With --splice (B_ALL's splice fixture), the
/// zombie sub-arm: per d >= 3, a fresh stack whose level d/2 is released (a zombie spliced into its
/// child), a cold top read, and a land-top that must install every level's rows (D-T3's shape).
/// The main stacks stay live and go to the sidecar (with the id shift), for R1's d axis.
fn stack(args: &Args) {
    let mut sc = read_stepping_sidecar(&args.db);
    if sc.shift.is_some() {
        not_a_result("this state already carries S1 stacks");
    }
    let db = open_db(&args.db, true);
    let trunk = db.connect().unwrap();
    let mut model = Model::from_sidecar(&sc);
    if model.live != args.n {
        not_a_result(&format!("stack --n {} but the model has {} live", args.n, model.live));
    }
    let n = model.live;
    let label = &args.label;
    check_invariants(&db, &model, "stack open");
    // A10: a forced checkpoint and the fixed run-in (no window: S1 measures stack ops).
    db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("A10 forced checkpoint: {e}")));
    let mut side = MergeSide::new(&trunk, args.validator);
    for _ in 0..args.runin {
        step(&db, &trunk, &mut model, &mut side, None);
    }
    check_invariants(&db, &model, "stack after run-in");
    let s0 = model.steps;
    // Every id this command makes is s0 + 1, s0 + 2, ... in order (the store's ids are sequential and
    // never reused, the schedule's own premise); `made` counts them, and each Stack's ids are checked.
    let mut made = 0u64;
    let mut serial = 0u64;
    let expect_ids = |ids: &[BranchId], made: &mut u64| {
        for id in ids {
            *made += 1;
            if id.0 != s0 + *made {
                not_a_result(&format!("stack id {id:?}, expected {} (ids not sequential)", s0 + *made));
            }
        }
    };
    let policy = |keep| MergePolicy {
        validation: args.validator,
        keep_merged: keep,
    };
    let mut merger = Merger::new(trunk.clone()).unwrap_or_else(|e| not_a_result(&format!("merger: {e}")));
    let mut findings = 0u64;
    // The -MRG arms (R13_MERGER=off) have no Merger: their land-tops are REPLAYED as plain trunk
    // writes of the Merger's rows (G-1, amendment 8.17), so the IN-NONE / IN-MRG trunk identity holds
    // by construction and is checked by the TRUNKDIGEST lines. The zombie sub-arm is Merger-only.
    let merger_off = std::env::var("R13_MERGER").is_ok_and(|v| v == "off");
    if merger_off && args.splice {
        not_a_result("--splice runs on the Merger only: the zombie sub-arm has no replay (amendment 8.17)");
    }
    let lands = args.lands;
    for &d in &args.depths {
        serial += 1;
        let st = build_stack(&db, &trunk, serial, d);
        expect_ids(&st.ids, &mut made);
        println!(
            "STACK\tn={n}\tlabel={label}\td={}\tserial={}\ttop={}\tI13_pages_per_level={:?}\tI13_ancestors_sum={}\t\
             I13_top_pages={}\tarena_added={}\ttrunk_added={}",
            st.d(),
            st.serial,
            st.top().0,
            st.pages,
            st.ancestors_pages(),
            st.top_pages,
            st.arena_added,
            st.trunk_added
        );
        // push: a child of the top, then reaped (untimed) before the next push.
        let mut s = Series::default();
        for _ in 0..args.touches {
            let mut child = None;
            timed_op(&db, &mut s, &mut || {
                let top = db.branch(st.top()).unwrap_or_else(|e| not_a_result(&format!("attach top: {e}")));
                let c = top.fork().unwrap_or_else(|e| not_a_result(&format!("push: {e}")));
                insert_level_rows(&c, st.serial, d, 0);
                let _ = top.into_id();
                child = Some(c);
                vec![]
            });
            let c = child.unwrap();
            expect_ids(&[c.id()], &mut made);
            let r = c.reap().unwrap_or_else(|e| not_a_result(&format!("pop: {e}")));
            if r.deferred {
                not_a_result("a pushed child with no child of its own was deferred");
            }
        }
        s.print(&format!("push_d{d}"), n, label);
        // amend at d/2.
        let h = (d / 2).max(1) - 1;
        let mut s = Series::default();
        for g in 1..=args.touches {
            timed_op(&db, &mut s, &mut || {
                let b = db.branch(st.ids[h as usize]).unwrap_or_else(|e| not_a_result(&format!("attach amend: {e}")));
                let conn = b.connect().unwrap();
                exec(
                    &conn,
                    &format!(
                        "UPDATE t SET v = '{}' WHERE id = {}",
                        stack_value(st.serial, h, 0, g),
                        stack_row(st.serial, h, 0)
                    ),
                );
                drop(conn);
                let _ = b.into_id();
                vec![]
            });
        }
        s.print(&format!("amend_d{d}"), n, label);
        // The amended level reads its last generation; the top still reads generation 0.
        read_rows(
            &db,
            st.ids[h as usize],
            &[(stack_row(st.serial, h, 0), stack_value(st.serial, h, 0, args.touches))],
        );
        // warm top read (one untimed read first).
        read_rows(&db, st.top(), &stack_read_plan(&model, st.serial, d, 0, s0 + 1));
        let mut s = Series::default();
        for i in 0..args.touches {
            let plan = stack_read_plan(&model, st.serial, d, mix(i ^ 0x54 ^ w()), s0 + 1);
            timed_op(&db, &mut s, &mut || {
                read_rows(&db, st.top(), &plan);
                vec![]
            });
        }
        s.print(&format!("warm_top_read_d{d}"), n, label);
        // cold top read: cap 0, a forced checkpoint, the arm's cap back, then the read.
        let mut s = Series::default();
        let (mut chain_dev, mut ins_dev) = (0u64, 0u64);
        for i in 0..args.touches {
            db.branch_set_resident_cap(Some(0));
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("cold-read checkpoint: {e}")));
            db.branch_set_resident_cap(arm_cap());
            let plan = stack_read_plan(&model, st.serial, d, mix(i ^ 0x55 ^ w()), s0 + 1);
            let c = timed_op(&db, &mut s, &mut || {
                read_rows(&db, st.top(), &plan);
                vec![]
            });
            let (chain, ins) = (counter(&c, "ensure_chain_sum"), counter(&c, "derived_inserts"));
            if chain != d as i64 {
                chain_dev += 1;
                if chain_dev <= 5 {
                    println!("FINDING\tS1 A4.X3\td={d}\ttouch={i}\tensure_chain_sum={chain}\twant={d}");
                }
            }
            if ins != st.ancestors_pages() as i64 {
                ins_dev += 1;
                if ins_dev <= 5 {
                    println!(
                        "FINDING\tS1 A4.X3\td={d}\ttouch={i}\tderived_inserts={ins}\twant_I13={}",
                        st.ancestors_pages()
                    );
                }
            }
        }
        findings += chain_dev + ins_dev;
        s.print(&format!("cold_top_read_d{d}"), n, label);
        println!(
            "S1_IDENTITY\tn={n}\tlabel={label}\td={d}\tcold_reads={}\tchain_sum_deviations={chain_dev}\t\
             derived_inserts_deviations={ins_dev}",
            args.touches
        );
        // land-top, each on a fresh stack (untimed build), the stack reaped afterwards.
        let mut s = Series::default();
        for k in 0..lands {
            serial += 1;
            let ls = build_stack(&db, &trunk, serial, d);
            expect_ids(&ls.ids, &mut made);
            let top = db.branch(ls.top()).unwrap_or_else(|e| not_a_result(&format!("attach land top: {e}")));
            let mut top = Some(top);
            if merger_off {
                let mut wrote = 0;
                timed_op(&db, &mut s, &mut || {
                    wrote = replay_land_top(&trunk, top.take().unwrap(), ls.serial, d);
                    vec![]
                });
                if wrote != 2 * d {
                    not_a_result(&format!("replayed land-top wrote {wrote} rows, want 2d = {}", 2 * d));
                }
            } else {
                let mut out = None;
                timed_op(&db, &mut s, &mut || {
                    out = Some(
                        merger
                            .land_top(top.take().unwrap(), policy(false))
                            .unwrap_or_else(|e| not_a_result(&format!("land_top: {e}"))),
                    );
                    vec![]
                });
                let out = out.unwrap();
                if out.refused.is_some() || out.rows_changed as u64 != 2 * d {
                    findings += 1;
                    println!("FINDING\tS1 land-top\td={d}\tserial={}\twant=2d={}\tgot={out:?}", ls.serial, 2 * d);
                }
            }
            print_trunk_digest(&trunk, n, label, &format!("land_top_d{d}_l{k}"), model.steps);
            for &id in ls.ids[..ls.ids.len() - 1].iter().rev() {
                let b = db.branch(id).unwrap_or_else(|e| not_a_result(&format!("attach {id:?} to reap: {e}")));
                let r = b.reap().unwrap_or_else(|e| not_a_result(&format!("reap {id:?}: {e}")));
                if r.deferred {
                    not_a_result(&format!("land stack level {id:?} deferred after its child's release"));
                }
            }
        }
        s.print(&format!("land_top_d{d}"), n, label);
        // The zombie sub-arm (splice fixture only).
        if args.splice && d >= 3 {
            let mut s = Series::default();
            for k in 0..lands {
                serial += 1;
                let zs = build_stack(&db, &trunk, serial, d);
                expect_ids(&zs.ids, &mut made);
                let z = (d / 2) as usize;
                let r = db
                    .branch(zs.ids[z])
                    .unwrap_or_else(|e| not_a_result(&format!("attach zombie: {e}")))
                    .reap()
                    .unwrap_or_else(|e| not_a_result(&format!("release zombie: {e}")));
                if !r.deferred {
                    not_a_result("a released level with a live child was not deferred");
                }
                db.branch_set_resident_cap(Some(0));
                db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("zombie checkpoint: {e}")));
                db.branch_set_resident_cap(arm_cap());
                let mut plan = stack_read_plan(&model, zs.serial, d, 7, s0 + 1);
                plan.push((stack_row(zs.serial, z as u64, 1), stack_value(zs.serial, z as u64, 1, 0)));
                read_rows(&db, zs.top(), &plan);
                let top = db.branch(zs.top()).unwrap_or_else(|e| not_a_result(&format!("attach zombie top: {e}")));
                let mut out = None;
                let mut top = Some(top);
                timed_op(&db, &mut s, &mut || {
                    out = Some(
                        merger
                            .land_top(top.take().unwrap(), policy(false))
                            .unwrap_or_else(|e| not_a_result(&format!("zombie land_top: {e}"))),
                    );
                    vec![]
                });
                let out = out.unwrap();
                if out.refused.is_some() || out.rows_changed as u64 != 2 * d {
                    findings += 1;
                    println!("FINDING\tS1 zombie land-top\td={d}\tserial={}\twant=2d={}\tgot={out:?}", zs.serial, 2 * d);
                }
                print_trunk_digest(&trunk, n, label, &format!("zombie_land_top_d{d}_l{k}"), model.steps);
                for (i, &id) in zs.ids[..zs.ids.len() - 1].iter().enumerate().rev() {
                    if i == z {
                        continue;
                    }
                    let b = db.branch(id).unwrap_or_else(|e| not_a_result(&format!("attach {id:?} to reap: {e}")));
                    let _ = b.reap().unwrap_or_else(|e| not_a_result(&format!("reap {id:?}: {e}")));
                }
            }
            s.print(&format!("zombie_land_top_d{d}"), n, label);
        }
        sc.stacks.push(StackRec {
            d,
            serial: st.serial,
            ids: st.ids.iter().map(|id| id.0).collect(),
            pages: st.level_pages(),
        });
        model.extra.extend(st.ids.iter().map(|id| id.0));
        model.extra_pages += st.level_pages().iter().sum::<u64>();
    }
    print_trunk_digest(&trunk, n, label, "window_end", model.steps);
    drop(merger);
    drop(side);
    // The S1 state: the shift for later schedule forks, and the main stacks still live.
    let _ = SHIFT.set((s0, made));
    sc.steps = model.steps;
    sc.shift = Some((s0, made));
    sc.extra = model.extra.clone();
    check_invariants(&db, &model, "stack end");
    println!("{}", gauge_line(&db, n, label, "stack_end"));
    println!(
        "S1\tn={n}\tlabel={label}\tsteps={}\tids_made={made}\tfindings={findings}\t{}\t{}",
        model.steps,
        shape_line(&db.branch_cat_shape()).replace(' ', "\t"),
        merge_work_line(&db.branch_merge_work()).replace(' ', "\t")
    );
    write_sidecar(&args.db, &sc);
    println!("{}", state_line(&model));
    victims_line(s0, made, n, label);
    drop(trunk);
    drop(db);
}

/// A3.L1's attribution, from I12's victim log (`R13_VICTIM_LOG`, one line per evicting checkpoint:
/// the checkpoint count, then the victims' ids): how many victims this run evicted, and how many of
/// them were S1's (ids in (s0, s0 + made]). The census keeps this line, not the log (third review:
/// the logs are megabytes per arm).
fn victims_line(s0: u64, made: u64, n: u64, label: &str) {
    let Ok(path) = std::env::var("R13_VICTIM_LOG") else {
        println!("VICTIMS\tn={n}\tlabel={label}\tlog=unset");
        return;
    };
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let (mut lines, mut all, mut stack) = (0u64, 0u64, 0u64);
    for line in text.lines() {
        // "<checkpoint> <id>,<id>,..." (store.rs's victim_log); a checkpoint that evicted nothing
        // writes its number alone.
        if line.split_whitespace().nth(1).is_some() {
            lines += 1;
        }
        for id in line.split_whitespace().skip(1).flat_map(|x| x.split(',')).filter_map(|x| x.parse::<u64>().ok()) {
            all += 1;
            if id > s0 && id <= s0 + made {
                stack += 1;
            }
        }
    }
    println!("VICTIMS\tn={n}\tlabel={label}\tevicting_checkpoints={lines}\tvictims={all}\tstack_victims={stack}");
}

// ---------------------------------------------------------------------------------------------
// r13-compose R1: crash images and the restart (PREREG §3.3, A2.F7's A12, A2.F10, A4.C3).

/// r13-compose A5.8: this process's device bytes written and read (proc_pid_rusage RUSAGE_INFO_V4,
/// as r12-catload's branch_restart reads them): D_c's instrument, an upper bound on a clone's
/// copy-on-write divergence (a rewrite counts again).
fn diskio_line(cmd: &str) -> String {
    // SAFETY: a zeroed plain-integer struct, filled by the call.
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is a rusage_info_v4, which RUSAGE_INFO_V4 writes.
    let rc = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as i32,
            libc::RUSAGE_INFO_V4,
            &mut info as *mut libc::rusage_info_v4 as *mut libc::rusage_info_t,
        )
    };
    if rc != 0 {
        not_a_result("proc_pid_rusage failed");
    }
    format!(
        "DISKIO\tcmd={cmd}\tbytes_written={}\tbytes_read={}",
        info.ri_diskio_byteswritten, info.ri_diskio_bytesread
    )
}

/// Kill this process at an op boundary (the harness failpoint): every op before it returned, so the
/// image holds exactly the model's state. The sidecar is written first.
fn sigkill(db_path: &Path, sc: &Sidecar) -> ! {
    write_sidecar(db_path, sc);
    println!("{}", diskio_line("crash"));
    println!("# SIGKILL at an op boundary");
    let _ = std::io::stdout().flush();
    // SAFETY: kill(2) on our own pid with a valid signal number.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
    unreachable!("SIGKILL returned")
}

/// `branch_githost crash --image a|b|e|e2`: on a clone, make an R1 crash image. (a) a forced
/// checkpoint, then the kill. (b) A12: a forced checkpoint and a fixed 5,000 schedule steps, then the
/// kill. (e) as (b), then `--bumps` untouched-branch commits (the oldest live unmerged PRs, one new
/// generation each), then the kill; (e2) the second crash of image e: the open, then the kill.
fn crash(args: &Args) {
    let mut sc = if args.image == "e2" { read_sidecar(&args.db) } else { read_stepping_sidecar(&args.db) };
    let db = open_db(&args.db, true);
    let trunk = db.connect().unwrap();
    let mut model = Model::from_sidecar(&sc);
    if model.live != args.n {
        not_a_result(&format!("crash --n {} but the model has {} live", args.n, model.live));
    }
    check_invariants(&db, &model, "crash open");
    let mut side = MergeSide::new(&trunk, args.validator);
    let ck0 = db.branch_cat_shape().checkpoints;
    match args.image.as_str() {
        "a" => {
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("image a checkpoint: {e}")));
        }
        "b" | "e" => {
            db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("A12 checkpoint: {e}")));
            for _ in 0..5_000 {
                step(&db, &trunk, &mut model, &mut side, None);
            }
            if args.image == "e" {
                for t in model.bump_targets(args.bumps) {
                    model.gen[t as usize] += 1;
                    do_update(&db, t, model.gen[t as usize]);
                }
                sc.bump = args.bumps;
            }
        }
        "e2" => {
            if sc.bump == 0 {
                not_a_result("image e2 is the second crash of an image e state");
            }
        }
        other => die(&format!("bad --image {other:?} (a, b, e, e2)")),
    }
    sc.steps = model.steps;
    // The slots in use at the kill (after a settle, which a restart's settle repeats), for R1's exact
    // slot check.
    let st = db.branch_stats().unwrap_or_else(|e| not_a_result(&format!("crash settle: {e}")));
    sc.slots = Some(st.arena_slots_in_use as u64);
    println!(
        "CRASH\timage={}\tsteps={}\tlive={}\tbump={}\tslots={}\tcheckpoints_before_kill={}",
        args.image,
        model.steps,
        model.live,
        sc.bump,
        st.arena_slots_in_use,
        db.branch_cat_shape().checkpoints - ck0
    );
    println!("{}", state_line(&model));
    sigkill(&args.db, &sc)
}

/// The listing a restart must see, kept current as reaps remove ids (so no O(N) model scan is
/// timed): count, wrapping sum and xor of mix(id), over store ids.
struct ListWant {
    len: usize,
    sum: u64,
    xor: u64,
}

impl ListWant {
    fn of(ids: &[u64]) -> Self {
        let (sum, xor) = ids.iter().fold((0u64, 0u64), |(a, x), &id| (a.wrapping_add(id), x ^ mix(id)));
        Self { len: ids.len(), sum, xor }
    }

    fn remove(&mut self, id: u64) {
        self.len -= 1;
        self.sum = self.sum.wrapping_sub(id);
        self.xor ^= mix(id);
    }
}

/// R1's op kinds after an open. Each op is planned against the model untimed, then its store call is
/// timed and charged its counters (as `step` does).
struct Restart {
    side: MergeSide,
    /// The smallest schedule id that may still be live (reaps take the oldest).
    oldest: u64,
    /// The smallest schedule id that may still be an open (live, unmerged) PR.
    merge_cursor: u64,
    want: ListWant,
}

impl Restart {
    fn oldest_live(&mut self, model: &Model) -> u64 {
        while self.oldest <= model.steps && model.dead[self.oldest as usize] {
            self.oldest += 1;
        }
        if self.oldest > model.steps {
            not_a_result("no live schedule branch left");
        }
        self.oldest
    }

    /// Op kind `k` (0 reap_oldest, 1 read_oldest, 2 merge_oldest, 3 list, 4 stack_top_read), the
    /// i-th of its kind.
    #[allow(clippy::too_many_arguments)]
    fn op(
        &mut self,
        k: usize,
        i: u64,
        db: &Arc<Database>,
        trunk: &Arc<Connection>,
        model: &mut Model,
        sc: &Sidecar,
        series: &mut Series,
    ) {
        match k {
            0 => {
                let t = self.oldest_live(model);
                model.dead[t as usize] = true;
                model.live -= 1;
                self.want.remove(sid(t).0);
                timed_op(db, series, &mut || {
                    do_reap(db, t);
                    vec![("id".to_string(), t as i64)]
                });
            }
            1 => {
                let id = self.oldest_live(model);
                let plan = plan_read(model, id, mix(id ^ 0x56 ^ w()));
                timed_op(db, series, &mut || {
                    exec_read(db, &plan);
                    vec![("id".to_string(), id as i64)]
                });
            }
            2 => {
                let at = model.next_virtual();
                if merge_mode() == MergeMode::Update {
                    let r = mix(at ^ 0xA5A5_0005 ^ w()) % ROWS + 1;
                    model.writes[r as usize].push((at, merge_value(at)));
                    model.commits.push(at);
                    timed_op(db, series, &mut || {
                        do_merge(trunk, at, r);
                        vec![("row".to_string(), r as i64)]
                    });
                    return;
                }
                while self.merge_cursor <= model.steps
                    && (model.dead[self.merge_cursor as usize] || model.merged[self.merge_cursor as usize])
                {
                    self.merge_cursor += 1;
                }
                if self.merge_cursor > model.steps {
                    not_a_result("no open PR left to merge");
                }
                let p = self.merge_cursor;
                self.merge_cursor += 1;
                let MergePlan::Pr { p, gen, refused } = model.plan_pr_merge(at, p) else {
                    unreachable!("plan_pr_merge plans a PR merge")
                };
                if refused && self.side.merger.is_none() {
                    // replay-update skips what the Merger would refuse: no work, no timed op (as step).
                    self.side.refused += 1;
                    return;
                }
                let side = &mut self.side;
                timed_op(db, series, &mut || {
                    do_merge_pr(db, trunk, side, p, gen, refused);
                    vec![("pr".to_string(), p as i64), ("refused".to_string(), refused as i64)]
                });
                if !refused {
                    installed_rows_are(trunk, p, gen);
                }
            }
            3 => {
                let want = &self.want;
                timed_op(db, series, &mut || {
                    let ids = db.branch_ids().unwrap();
                    let (sum, xor) = ids.iter().fold((0u64, 0u64), |(a, x), id| (a.wrapping_add(id.0), x ^ mix(id.0)));
                    if ids.len() != want.len || sum != want.sum || xor != want.xor {
                        not_a_result(&format!("list: {} ids, model {}", ids.len(), want.len));
                    }
                    vec![("out".to_string(), ids.len() as i64)]
                });
            }
            _ => {
                if sc.stacks.is_empty() {
                    return;
                }
                let st = &sc.stacks[(i as usize) % sc.stacks.len()];
                let top = BranchId(*st.ids.last().unwrap());
                let fork_step = sc.shift.map_or(model.steps + 1, |(after, _)| after + 1);
                let plan = stack_read_plan(model, st.serial, st.d, mix(i ^ 0x57 ^ w()), fork_step);
                timed_op(db, series, &mut || {
                    read_rows(db, top, &plan);
                    vec![("d".to_string(), st.d as i64)]
                });
            }
        }
    }
}

/// `branch_githost restart`: open a crash image (timed, with its open stats and the settle), then the
/// post-open sequence, one op each: the oldest-child reap, read_oldest, a merge of the oldest open PR
/// (pre-horizon: MV4 for both validators, I7), a list, a cold stack-top read (S1 states; `--stack-d`
/// picks one depth for R1's d axis). Then A4.C3's
/// P-window: `--pwindow` ops of each kind, round-robin, with ZERO checkpoints (else NOT A RESULT for
/// P, printed, not fatal), and then the first checkpoint, forced and timed. P on and off are separate
/// runs (R12_PREWARM), each on its own clone.
fn restart(args: &Args) {
    let sc = read_sidecar(&args.db);
    if sc.steps == 0 {
        not_a_result("restart of an ungrown database");
    }
    // R1's d axis reads one depth's stack; the model still lists every stack's levels.
    let mut reads = sc.clone();
    if args.stack_d > 0 {
        reads.stacks.retain(|st| st.d == args.stack_d);
        if reads.stacks.is_empty() {
            not_a_result(&format!("--stack-d {}: this state has no stack of that depth", args.stack_d));
        }
    }
    let files_before = files_line(&args.db);
    let t = Instant::now();
    let db = open_db(&args.db, true);
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let trunk = db.connect().unwrap();
    let mut model = Model::from_sidecar(&sc);
    if model.live != args.n {
        not_a_result(&format!("restart --n {} but the model has {} live", args.n, model.live));
    }
    let n = model.live;
    let label = &args.label;
    let (c0, sh0) = (db.branch_catalog_counters(), db.branch_cat_shape());
    let t = Instant::now();
    db.branch_stats().unwrap_or_else(|e| not_a_result(&format!("settle: {e}")));
    let settle_us = t.elapsed().as_secs_f64() * 1e6;
    let (c1, sh1) = (db.branch_catalog_counters(), db.branch_cat_shape());
    println!(
        "RESTART_OPEN\tn={n}\tlabel={label}\tsteps={}\tbump={}\tstacks={}\th={}\topen_us={open_us:.1}\t\
         settle_us={settle_us:.1}\tsettle_branch_loads={}\tsettle_cat_queries={}\tsettle_trunk_probes={}\t\
         settle_sharp_max_loads={}\tcr_sharp_bound={}\t{}\tfiles_before: {files_before}",
        model.steps,
        sc.bump,
        sc.stacks.len(),
        model.h(),
        c1.0 - c0.0,
        c1.2 - c0.2,
        sh1.trunk_probes - sh0.trunk_probes,
        sh1.settle_sharp_max_loads,
        // A8.3's sharp C-R bound: the parked branches x (1 + the deepest parked chain); a trunk
        // child's chain is 1, a stack's its depth.
        // (parked records the open left for the settle bound its parked branches from above.)
        {
            let os = db.branch_open_stats();
            (os.parked_records - os.parked_applied) * (1 + sc.stacks.iter().map(|st| st.d).max().unwrap_or(1))
        },
        format!("{} {}", open_line(&db.branch_open_stats()), prewarm_line(&db)).replace(' ', "\t")
    );
    // Before the post-open sequence only memory is checked: the full invariants query the catalog
    // (branch_trunk_retained) and would warm it in every arm before P is measured (third review); they
    // run after the first checkpoint.
    let st = db.branch_stats().unwrap();
    if st.live_branches as u64 != model.live + model.extra.len() as u64 {
        not_a_result(&format!("restart open: {} branch states, model {}", st.live_branches, model.live + model.extra.len() as u64));
    }
    if let Some(want) = sc.slots {
        if st.arena_slots_in_use as u64 != want {
            println!("FINDING\tR1 slot accounting\tslots_after_restart={}\tslots_at_kill={want}", st.arena_slots_in_use);
            not_a_result("R1: the arena's slots in use after the restart differ from the kill's (§3.3)");
        }
    }
    let ck0 = db.branch_cat_shape().checkpoints;
    let mut r = Restart {
        side: MergeSide::new(&trunk, args.validator),
        oldest: 1,
        merge_cursor: 1,
        want: ListWant::of(&model.store_live_ids()),
    };
    // The post-open sequence: one op of each kind.
    let kinds = ["reap_oldest", "read_oldest", "merge_oldest", "list", "stack_top_read"];
    for (k, name) in kinds.iter().enumerate() {
        let mut s = Series::default();
        r.op(k, 0, &db, &trunk, &mut model, &reads, &mut s);
        s.print(&format!("postopen_{name}"), n, label);
    }
    // A4.C3's P-window, round-robin over the kinds.
    let mut ser: Vec<Series> = (0..kinds.len()).map(|_| Series::default()).collect();
    for i in 0..args.pwindow {
        for (k, s) in ser.iter_mut().enumerate() {
            r.op(k, i + 1, &db, &trunk, &mut model, &reads, s);
        }
    }
    for (s, name) in ser.iter_mut().zip(kinds) {
        s.print(&format!("pwindow_{name}"), n, label);
    }
    let window_ckpts = db.branch_cat_shape().checkpoints - ck0;
    println!(
        "PWINDOW\tn={n}\tlabel={label}\tops_per_kind={}\tcheckpoints={window_ckpts}\tverdict={}\tR12_PREWARM={}",
        args.pwindow,
        if window_ckpts == 0 { "ok" } else { "NOT_A_RESULT_FOR_P" },
        std::env::var("R12_PREWARM").unwrap_or_else(|_| "unset".to_string())
    );
    let side = r.side;
    let mut s = Series::default();
    timed_op(&db, &mut s, &mut || {
        db.branch_compact_now().unwrap_or_else(|e| not_a_result(&format!("first checkpoint: {e}")));
        vec![]
    });
    s.print("first_checkpoint", n, label);
    check_invariants(&db, &model, "restart end");
    let (rows, digest) = trunk_digest(&trunk);
    println!("TRUNKDIGEST\tn={n}\tlabel={label}\tat=restart_end\tsteps={}\trows={rows}\tdigest={digest:#018x}", model.steps);
    println!(
        "MERGEWORK\tn={n}\tlabel={label}\tat=restart_end\tmerges_installed={}\tmerges_refused={}\t{}\t{}",
        side.installed,
        side.refused,
        side.owned_line(),
        merge_work_line(&db.branch_merge_work()).replace(' ', "\t")
    );
    drop(side);
    drop(trunk);
    drop(db);
}

/// The R11_/R12_/R13_ variables a census run may carry (third review: the release binaries hold
/// runtime mutants and other knobs). An ALLOWLIST: anything else with those prefixes is refused.
/// Blind spot, stated: other variables (RUST_*, TURSO_*) are not checked here; census.sh runs every
/// command under `env -i` with its own allowlist.
const CENSUS_ENV: [&str; 6] = ["R11_RESIDENT_CAP", "R12_PREWARM", "R13_FW1", "R13_FW2", "R13_MERGER", "R13_VICTIM_LOG"];

fn main() {
    for (k, _) in std::env::vars_os() {
        let k = k.to_string_lossy();
        let ours = ["R11_", "R12_", "R13_"].iter().any(|p| k.starts_with(p));
        if ours && !CENSUS_ENV.contains(&&*k) {
            die(&format!("{k} is set: a census run carries only {CENSUS_ENV:?}"));
        }
    }
    let args = parse_args();
    if cfg!(debug_assertions) {
        println!("# DEBUG build: not a timing result");
    }
    // F-W3's knob (githost-shape PREREG G5.3), applied by `open_db` right after each open; recorded
    // with every run. r13-compose: every knob the census sets, the schedule's seed and modes, and
    // I9's sizes.
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| "unset".to_string());
    let (state_bytes, slot_bytes) = Database::branch_state_sizes();
    println!(
        "# R11_RESIDENT_CAP={} R12_PREWARM={} R13_FW1={} R13_FW2={} R13_MERGER={} \
         R13_VICTIM_LOG={} seed={} W={:#x} merge={} validator={:?} deaths={} runin={} \
         size_of_branch_state={state_bytes} size_of_table_slot={slot_bytes}",
        env("R11_RESIDENT_CAP"),
        env("R12_PREWARM"),
        env("R13_FW1"),
        env("R13_FW2"),
        env("R13_MERGER"),
        env("R13_VICTIM_LOG"),
        args.seed,
        w(),
        merge_mode().name(),
        args.validator,
        deaths_name(),
        args.runin
    );
<<<<<<< HEAD
    // A3.F17/A5.4: the -MRG arm is the Merger off AND the merge mode replay-update, together.
    let merger_off = std::env::var("R13_MERGER").is_ok_and(|v| v == "off");
    if merger_off != (merge_mode() == MergeMode::ReplayUpdate) && merge_mode() != MergeMode::Update {
        die("R13_MERGER=off goes with --merge replay-update, and --merge real with the Merger on (A3.F17)");
    }
||||||| 484270f95
=======
    // r11-githost-attr: the mode and the two page-cache instruments, recorded with every run.
    let set = |k: &str| if std::env::var_os(k).is_some() { "set" } else { "unset" };
    println!(
        "# r11-githost-attr: durability={} R11_UBC_PROBE={} R11_CENSUS_OPS={} R11_SCHEMA_SHARE={} \
         R11_PREWARM={} R11_PREWARM_SLOTS={} R11_SCHEMA_KEY={}",
        match (args.cmd.as_str(), args.eager) {
            ("ubc-selftest", _) => "n/a",
            (_, true) => "eager",
            (_, false) => "catalog",
        },
        set("R11_UBC_PROBE"),
        set("R11_CENSUS_OPS"),
        set("R11_SCHEMA_SHARE"),
        set("R11_PREWARM"),
        std::env::var("R11_PREWARM_SLOTS").unwrap_or_else(|_| "unset".to_string()),
        std::env::var("R11_SCHEMA_KEY").unwrap_or_else(|_| "unset".to_string())
    );
>>>>>>> r11-githost-attr-kv2.noindex
    match args.cmd.as_str() {
        "grow" => grow(&args),
        "probe" => probe(&args),
<<<<<<< HEAD
        "golden" => golden(),
        "stack" => stack(&args),
        "crash" => crash(&args),
        "restart" => restart(&args),
||||||| 484270f95
=======
        "ubc-selftest" => ubc_selftest(&args.file),
>>>>>>> r11-githost-attr-kv2.noindex
        other => die(&format!("unknown command {other}")),
    }
    println!("{}", diskio_line(&args.cmd));
}

/// r13-compose A4.S1's K = 0 check: a digest of `Model::advance`'s StepPlan for s <= 10,000 in
/// `--merge update` mode, seed 0, deaths 0.02. Each step contributes the line
/// "s merge_row update_target:gen death\n" (0 for an absent part, "0:0" for no update) to an FNV-1a
/// 64 digest. tools/golden.sh computes the golden from 310705857's schedule functions verbatim, with
/// the same line format, and the two must be equal.
fn golden() {
    if w() != 0 || merge_mode() != MergeMode::Update || !deaths_on() {
        die("golden runs with --seed 0 --merge update --deaths 0.02");
    }
    let mut model = Model::new();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for _ in 0..10_000 {
        let p = model.advance();
        let m = match p.merges[..] {
            [MergePlan::Update(r)] => r,
            [] => 0,
            _ => die("golden: update mode with one merge per step makes at most one trunk UPDATE"),
        };
        let (u, g) = p.update.unwrap_or((0, 0));
        let line = format!("{} {} {}:{} {}\n", p.s, m, u, g, p.death.unwrap_or(0));
        for b in line.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    println!("GOLDEN\tsteps=10000\tdigest={h:#018x}\tlive={}\tupdates_skipped={}\tdeaths_skipped={}", model.live, model.updates_skipped, model.deaths_skipped);
}
