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

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::merge::{MergePolicy, Merger, Refusal, Validation};
use turso_core::branch::{
    id_set_census, BranchCatShape, BranchDurability, BranchId, BranchMergeWork, BranchOpenStats,
};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const ROWS: u64 = 20_000;
const VALUE_LEN: usize = 100;
const OPEN_WINDOW: f64 = 0.0237;

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
}

fn parse_args() -> Args {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| die("usage: branch_githost grow|probe ..."));
    let mut args = Args {
        cmd,
        db: PathBuf::new(),
        n: 0,
        page_size: 1024,
        label: String::new(),
        trunk_sync: "default".to_string(),
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
    };
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--db" => args.db = PathBuf::from(val()),
            "--to" | "--n" => args.n = val().parse().unwrap_or_else(|_| die("bad --n/--to")),
            "--page-size" => args.page_size = val().parse().unwrap_or_else(|_| die("bad --page-size")),
            "--label" => args.label = val(),
            "--trunk-sync" => args.trunk_sync = val(),
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
            other => die(&format!("unknown argument {other}")),
        }
    }
    if args.cmd != "golden" && (args.db.as_os_str().is_empty() || (args.n == 0 && args.to_steps == 0)) {
        die("--db and --n/--to (or grow's --to-steps) are required");
    }
    if args.mps == 0 || args.depths.iter().any(|&d| d == 0) {
        die("--mps and every depth must be >= 1");
    }
    let _ = W.set(args.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let _ = SEED.set(args.seed);
    let _ = MODE.set((args.merge, args.deaths));
    let _ = MPS.set(args.mps);
    let _ = SPLICE.set(args.splice);
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
    /// r13-compose R1: trunk writes made outside the schedule (restart's merges) are stamped at
    /// virtual steps after the last schedule step, so no schedule fork sees them.
    virt: u64,
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
            virt: 0,
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
            if merge_mode() == MergeMode::Update {
                let r = merge_row_j(s, k);
                self.writes[r as usize].push((s, merge_value(s)));
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
}

// ---------------------------------------------------------------------------------------------
// Store access.

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
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let splice = SPLICE.get().copied().unwrap_or(false);
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync })
            .with_branch_splice(splice),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap_or_else(|e| not_a_result(&format!("open failed: {e}")));
    // F-W3's knob (githost-shape PREREG G5.3): unset keeps every touched state resident (COMP).
    if let Some(cap) = arm_cap() {
        db.branch_set_resident_cap(Some(cap));
    }
    db
}

fn sidecar(db: &Path) -> PathBuf {
    PathBuf::from(format!("{}.githost", db.to_str().unwrap()))
}

/// The sidecar `<db>.githost`: `steps=N`, and (r13-compose) the schedule's parameters `seed=K`,
/// `merge=<mode>`, `deaths=<rate>` and `mps=M`, and the state outside the schedule: `shift=S0,L`
/// (S1's stack command consumed the ids S0+1..=S0+L, so every later schedule fork s > S0 gets the
/// id s + L), `extra=<id>,...` (stack levels still live), one `stack=<d>,<serial>,<root id>,...,<top
/// id>` per S1 main stack, and `bump=<count>` (R1 image e's untouched-branch commits, made after the
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
}

#[derive(Clone)]
struct StackRec {
    d: u64,
    serial: u64,
    /// Level ids, root first, top last.
    ids: Vec<u64>,
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
                }),
                _ => not_a_result(&format!("sidecar stack line {v:?}: needs d, serial and d ids")),
            },
            "bump" => sc.bump = v.trim().parse().unwrap_or_else(|_| not_a_result("unparseable sidecar bump")),
            other => not_a_result(&format!("unknown sidecar key {other:?}")),
        }
    }
    sc.steps = steps.unwrap_or_else(|| not_a_result("unparseable sidecar"));
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
    }
    if sc.bump > 0 {
        text.push_str(&format!("bump={}\n", sc.bump));
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
    }
}

static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

impl Model {
    /// The model of a sidecar's state: the schedule replayed, the stacks' live levels, and image
    /// e's untouched-branch commits (the `bump` smallest live unmerged schedule ids, one more
    /// generation each; `bump_targets` picks them).
    fn from_sidecar(sc: &Sidecar) -> Self {
        let mut m = Self::replay(sc.steps);
        m.extra = sc.extra.clone();
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
        "db_bytes={} wal_bytes={} log_bytes={} snap_bytes={} arena_bytes={}",
        size_of(db),
        size_of(&with("-wal")),
        size_of(&with("-branch-log")),
        size_of(&with("-branch-snap")),
        size_of(&with("-branch-arena"))
    )
}

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

fn shape_line(s: &BranchCatShape) -> String {
    format!(
        "checkpoints={} checkpoint_ns={} ckpt_trunk_inserted={} ckpt_trunk_deleted={} ckpt_branch_rows={} \
         ckpt_rows_written={} ckpt_states_walked={} ids_calls={} ids_resident_visited={} ids_catalog_rows={} \
         ids_build_rows={} table_grows={} table_moved={} evictions={} evicted_states={} resident_states={} \
         dirty_branches={} trunk_overlay_versions={} trunk_cache_versions={} trunk_cache_pages={} \
         trunk_known_pages={} trunk_probes={} trunk_rows={} log_len={} ensure_cold={} ensure_chain_sum={} \
         ensure_chain_max={} evicted_with_resident_descendant={} walk_items_yielded={} derived_inserts={} \
         table_chunks={} chunk_allocs={} table_slots_allocated={} table_slot_bytes={}",
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
        s.table_slot_bytes
    )
}

/// r13-compose: the Merger's cumulative work (BranchMergeWork, every field).
fn merge_work_line(w: &BranchMergeWork) -> String {
    format!(
        "merge_attempts={} merge_commits={} merge_refused_scope={} merge_refused_key={} merge_refused_base={} \
         merge_refused_install={} refusals_same_change={} v3_horizon_fallbacks={} stamp_commits={} stamp_prunes={} \
         stamps_held={} derive_pages_read={} derive_attribution_seeks={} derive_rows_compared={} \
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
         table_slot_bytes_per_resident={:.3}\tstamps_held={}\tensure_chain_max={}\tdirty_branches={}\tlog_len={}\t\
         trunk_overlay_versions={}\trss={}",
        s.resident_states,
        s.table_chunks,
        s.table_slots_allocated,
        s.table_slot_bytes,
        per(s.table_chunks),
        per(s.table_slots_allocated),
        per(s.table_slot_bytes),
        w.stamps_held,
        s.ensure_chain_max,
        s.dirty_branches,
        s.log_len,
        s.trunk_overlay_versions,
        rss_bytes()
    )
}

fn open_line(s: &BranchOpenStats) -> String {
    format!(
        "snap_bytes={} log_bytes={} records={} snap_branches={} branches={} current_entries={} \
         retained_entries={} trunk_retained={} trunk_children={} referenced_slots={} arena_high_water={} \
         arena_free={} states={} released_scanned={} branch_loads={} trunk_page_loads={} cat_queries={} \
         cat_rows_read={} touched_slots={} trunk_probes={} trunk_rows={} parked_records={} parked_applied={} \
         catalog_us={:.1} recover_us={:.1} load_us={:.1} replay_us={:.1} \
         collect_us={:.1} referenced_us={:.1} arena_us={:.1} expire_us={:.1} store_total_us={:.1}",
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
        s.total_ns as f64 / 1e3
    )
}

/// The counters one operation is charged: deltas of every integer the catalog store exposes. Nothing here
/// queries the catalog (`branch_cat_shape` reads memory only), so taking them does not move them.
#[derive(Clone, Copy)]
struct Snap {
    cat: (u64, u64, u64, u64),
    rows_written: u64,
    reads: (u64, u64),
    in_use: i64,
    shape: BranchCatShape,
    mw: BranchMergeWork,
    ru: Usage,
}

/// This process's resource usage (`getrusage(RUSAGE_SELF)`): an attribution instrument for time,
/// NOT load-immune (it sees the page cache, the scheduler and every thread of the process).
#[derive(Clone, Copy, Default)]
struct Usage {
    inblock: i64,
    oublock: i64,
    majflt: i64,
    nivcsw: i64,
    cpu_us: i64,
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
    }
}

fn snap(db: &Arc<Database>) -> Snap {
    let st = db.branch_stats().unwrap();
    Snap {
        cat: db.branch_catalog_counters(),
        rows_written: db.branch_catalog_rows_written(),
        reads: db.branch_read_counters(),
        in_use: st.arena_slots_in_use as i64,
        shape: db.branch_cat_shape(),
        mw: db.branch_merge_work(),
        ru: usage(),
    }
}

/// Every counter an op is charged. All but the last five are the store's own integers; the `ru_*`
/// five are `getrusage` deltas (load-dependent; attribution only). r13-compose adds I3-I6, I11's
/// walk counter, and the Merger's work (I7, I8, the derivation), all cumulative deltas; the gauges
/// are printed by `gauge_line` instead.
const NC: usize = 50;
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
    "ru_inblock",
    "ru_oublock",
    "ru_majflt",
    "ru_nivcsw",
    "ru_cpu_us",
];

fn delta(a: &Snap, b: &Snap) -> [i64; NC] {
    let d = |x: u64, y: u64| y as i64 - x as i64;
    let (s, t) = (&a.shape, &b.shape);
    let (m, n) = (&a.mw, &b.mw);
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
        b.ru.inblock - a.ru.inblock,
        b.ru.oublock - a.ru.oublock,
        b.ru.majflt - a.ru.majflt,
        b.ru.nivcsw - a.ru.nivcsw,
        b.ru.cpu_us - a.ru.cpu_us,
    ]
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
struct Ops {
    new_pr: Series,
    merge: Series,
    pr_update: Series,
    reap: Series,
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
        }
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
            match (out.refused, refused) {
                (None, false) => {
                    if out.rows_changed != 2 {
                        not_a_result(&format!("merge {p} installed {} rows, the model 2: {out:?}", out.rows_changed));
                    }
                    side.installed += 1;
                }
                (Some(Refusal::Base | Refusal::Key), true) => side.refused += 1,
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
fn step(
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    model: &mut Model,
    side: &mut MergeSide,
    mut ops: Option<&mut Ops>,
) {
    let plan = model.advance();
    let timed = |series: Option<&mut Series>, f: &mut dyn FnMut()| {
        if let Some(series) = series {
            let a = snap(db);
            let t = Instant::now();
            f();
            let us = t.elapsed().as_secs_f64() * 1e6;
            let b = snap(db);
            series.push(us, delta(&a, &b));
        } else {
            f();
        }
    };
    timed(ops.as_deref_mut().map(|o| &mut o.new_pr), &mut || do_new_pr(db, trunk, plan.s));
    for merge in &plan.merges {
        match *merge {
            MergePlan::Update(row) => {
                timed(ops.as_deref_mut().map(|o| &mut o.merge), &mut || do_merge(trunk, plan.s, row))
            }
            MergePlan::Pr { p, gen, refused } => timed(ops.as_deref_mut().map(|o| &mut o.merge), &mut || {
                do_merge_pr(db, trunk, side, p, gen, refused)
            }),
        }
    }
    if let Some((t, gen)) = plan.update {
        timed(ops.as_deref_mut().map(|o| &mut o.pr_update), &mut || do_update(db, t, gen));
    }
    if let Some(t) = plan.death {
        timed(ops.as_deref_mut().map(|o| &mut o.reap), &mut || do_reap(db, t));
    }
}

/// Invariants the catalog store can check alone: live states == the model's live count, and the arena
/// holds at least two own pages per live branch plus the trunk's versions. The EXACT slot check is across
/// arms: at the same step count the port's grow/probe files print its exact arena and trunk counts, and the
/// two stores make the same retain and free decisions (analysis compares them). The trunk count QUERIES
/// the catalog (`branch_trunk_retained`: overlay + catalog rows - reaped since), so this runs only
/// outside measured operations.
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
    if (st.arena_slots_in_use as u64) < 2 * live + trunk_versions {
        not_a_result(&format!(
            "{what}: arena in use {} < 2 x live {live} + trunk versions {trunk_versions}",
            st.arena_slots_in_use
        ));
    }
    println!(
        "# invariants {what}: live={} extra={} steps={} arena_in_use={} trunk_versions={trunk_versions}",
        model.live,
        model.extra.len(),
        model.steps,
        st.arena_slots_in_use
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
    let mut sc = read_stepping_sidecar(&args.db);
    let steps0 = sc.steps;
    let db = open_db(&args.db, false);
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
    let t = Instant::now();
    drop(trunk);
    drop(db);
    println!("# grow closed in {:.1} us; {}", t.elapsed().as_secs_f64() * 1e6, files_line(&args.db));
}

/// The resident cap this process runs under (F-W3's knob, applied by `open_db`).
fn arm_cap() -> Option<usize> {
    std::env::var("R11_RESIDENT_CAP").ok().map(|v| v.parse().unwrap_or_else(|_| die(&format!("bad R11_RESIDENT_CAP {v:?}"))))
}

/// r13-compose A10, A11 and A5.7: force a checkpoint, run the fixed run-in (`--runin`, 25,000 steps)
/// untimed while recording the steps at which AUTO checkpoints ran, and take C_arm as their mean
/// spacing. Then step (untimed) to the next auto checkpoint, and time a window of m x C_arm steps,
/// m = ceil(50,000 / C_arm), starting right after it. NOT A RESULT: fewer than 2 auto checkpoints in
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
    let m = 50_000u64.div_ceil(c_arm);
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

fn probe(args: &Args) {
    let mut sc = read_stepping_sidecar(&args.db);
    let steps0 = sc.steps;
    if steps0 == 0 {
        not_a_result("probe of an ungrown database");
    }
    let files_before = files_line(&args.db);
    let rss0 = rss_bytes();
    let t = Instant::now();
    let db = open_db(&args.db, true);
    let open_us = t.elapsed().as_secs_f64() * 1e6;
    let t = Instant::now();
    let trunk = db.connect().unwrap();
    let connect_us = t.elapsed().as_secs_f64() * 1e6;
    let rss1 = rss_bytes();
    let mut model = Model::from_sidecar(&sc);
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
    check_invariants(&db, &model, "after open");
    let os = db.branch_open_stats();
    let page_count = int(&trunk, "PRAGMA page_count") as u32;
    let n = model.live;
    let label = &args.label;
    println!(
        "OPEN\tn={n}\tlabel={label}\tsteps={}\topen_us={open_us:.1}\ttrunk_connect_us={connect_us:.1}\trss_before={rss0}\t\
         rss_after_open={rss1}\tpage_count={page_count}\t{}\t{}\tfiles_before: {files_before}",
        model.steps,
        open_line(&os).replace(' ', "\t"),
        shape_line(&db.branch_cat_shape()).replace(' ', "\t")
    );
    println!("{}", idset_line(&db, &model, n, label, "open"));
    let _ = std::io::stdout().flush();

    // r13-compose A10/A11: the forced checkpoint, the run-in, and the steady-state window.
    let mut side = MergeSide::new(&trunk, args.validator);
    let mut ops = Ops {
        new_pr: Series::default(),
        merge: Series::default(),
        pr_update: Series::default(),
        reap: Series::default(),
    };
    let window = steady_window(&db, &trunk, &mut model, &mut side, &mut ops, args, n);
    check_invariants(&db, &model, "after steady ops");
    ops.new_pr.print("new_pr", n, label);
    ops.merge.print("merge", n, label);
    ops.pr_update.print("pr_update", n, label);
    ops.reap.print("reap", n, label);
    println!("{window}");
    println!("{}", gauge_line(&db, n, label, "window_end"));
    println!(
        "MERGEWORK\tn={n}\tlabel={label}\tat=window_end\tmerges_installed={}\tmerges_refused={}\tmerges_skipped={}\t\
         model_refused={}\t{}",
        side.installed,
        side.refused,
        model.merges_skipped,
        model.merges_refused,
        merge_work_line(&db.branch_merge_work()).replace(' ', "\t")
    );

    let live = model.live_ids();
    let timed = |series: &mut Series, f: &mut dyn FnMut() -> Vec<(String, i64)>| {
        let a = snap(&db);
        let t = Instant::now();
        let extra = f();
        let us = t.elapsed().as_secs_f64() * 1e6;
        let b = snap(&db);
        series.push(us, delta(&a, &b));
        series.extra.push(extra);
    };

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
    }
    s.print("read_old", n, label);
    let mut s = Series::default();
    for &id in live.iter().take(1000) {
        let plan = plan_read(&model, id, mix(id ^ 0x53 ^ w()));
        timed(&mut s, &mut || {
            exec_read(&db, &plan);
            vec![("rows_read".to_string(), plan.reads.len() as i64)]
        });
    }
    s.print("read_oldest", n, label);

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
    println!("{}", idset_line(&db, &model, n, label, "shape"));
    sc.steps = model.steps;
    write_sidecar(&args.db, &sc);
    drop(side);
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
/// inserting its own two rows before its child forks. Untimed.
fn build_stack(db: &Arc<Database>, trunk: &Arc<Connection>, serial: u64, d: u64) -> Stack {
    let mut st = Stack {
        serial,
        ids: Vec::new(),
        pages: Vec::new(),
    };
    let mut level = trunk.fork_branch().unwrap_or_else(|e| not_a_result(&format!("stack root: {e}")));
    for i in 0..d {
        insert_level_rows(&level, serial, i, 0);
        if i + 1 == d {
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
    st
}

/// What the top of `st` must read: its own second row (an amend at d/2 = 1 rewrites the top's FIRST
/// row), the root's first row, the middle level's second row (the top sees every level as of the next
/// level's fork, so an amend below it is invisible to it), and one trunk row of the schedule's table.
fn stack_read_plan(model: &Model, st_serial: u64, d: u64, pick: u64) -> Vec<(u64, String)> {
    let mut reads = vec![(stack_row(st_serial, d - 1, 1), stack_value(st_serial, d - 1, 1, 0))];
    if d > 1 {
        reads.push((stack_row(st_serial, 0, 0), stack_value(st_serial, 0, 0, 0)));
        let mid = (d / 2).max(1) - 1;
        reads.push((stack_row(st_serial, mid, 1), stack_value(st_serial, mid, 1, 0)));
    }
    let r = pick % ROWS + 1;
    reads.push((r, model.trunk_at(r, model.steps + model.virt + 1)));
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
    for &d in &args.depths {
        serial += 1;
        let st = build_stack(&db, &trunk, serial, d);
        expect_ids(&st.ids, &mut made);
        println!(
            "STACK\tn={n}\tlabel={label}\td={d}\tserial={}\ttop={}\tI13_pages_per_level={:?}\tI13_ancestors_sum={}",
            st.serial,
            st.top().0,
            st.pages,
            st.ancestors_pages()
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
        read_rows(&db, st.top(), &stack_read_plan(&model, st.serial, d, 0));
        let mut s = Series::default();
        for i in 0..args.touches {
            let plan = stack_read_plan(&model, st.serial, d, mix(i ^ 0x54 ^ w()));
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
            let plan = stack_read_plan(&model, st.serial, d, mix(i ^ 0x55 ^ w()));
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
        for _ in 0..args.lands {
            serial += 1;
            let ls = build_stack(&db, &trunk, serial, d);
            expect_ids(&ls.ids, &mut made);
            let top = db.branch(ls.top()).unwrap_or_else(|e| not_a_result(&format!("attach land top: {e}")));
            let mut out = None;
            let mut top = Some(top);
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
            for _ in 0..args.lands {
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
                let mut plan = stack_read_plan(&model, zs.serial, d, 7);
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
        });
        model.extra.extend(st.ids.iter().map(|id| id.0));
    }
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
    drop(trunk);
    drop(db);
}

// ---------------------------------------------------------------------------------------------
// r13-compose R1: crash images and the restart (PREREG §3.3, A2.F7's A12, A2.F10, A4.C3).

/// Kill this process at an op boundary (the harness failpoint): every op before it returned, so the
/// image holds exactly the model's state. The sidecar is written first.
fn sigkill(db_path: &Path, sc: &Sidecar) -> ! {
    write_sidecar(db_path, sc);
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
    println!("CRASH\timage={}\tsteps={}\tlive={}\tbump={}", args.image, model.steps, model.live, sc.bump);
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
                let side = &mut self.side;
                timed_op(db, series, &mut || {
                    do_merge_pr(db, trunk, side, p, gen, refused);
                    vec![("pr".to_string(), p as i64), ("refused".to_string(), refused as i64)]
                });
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
                let plan = stack_read_plan(model, st.serial, st.d, mix(i ^ 0x57 ^ w()));
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
/// (pre-horizon: MV4 for both validators, I7), a list, a cold stack-top read (S1 states). Then A4.C3's
/// P-window: `--pwindow` ops of each kind, round-robin, with ZERO checkpoints (else NOT A RESULT for
/// P, printed, not fatal), and then the first checkpoint, forced and timed. P on and off are separate
/// runs (R12_PREWARM), each on its own clone.
fn restart(args: &Args) {
    let sc = read_sidecar(&args.db);
    if sc.steps == 0 {
        not_a_result("restart of an ungrown database");
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
        "RESTART_OPEN\tn={n}\tlabel={label}\tsteps={}\tbump={}\tstacks={}\topen_us={open_us:.1}\tsettle_us={settle_us:.1}\t\
         settle_branch_loads={}\tsettle_cat_queries={}\tsettle_trunk_probes={}\t{}\tfiles_before: {files_before}",
        model.steps,
        sc.bump,
        sc.stacks.len(),
        c1.0 - c0.0,
        c1.2 - c0.2,
        sh1.trunk_probes - sh0.trunk_probes,
        open_line(&db.branch_open_stats()).replace(' ', "\t")
    );
    check_invariants(&db, &model, "restart open");
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
        r.op(k, 0, &db, &trunk, &mut model, &sc, &mut s);
        s.print(&format!("postopen_{name}"), n, label);
    }
    // A4.C3's P-window, round-robin over the kinds.
    let mut ser: Vec<Series> = (0..kinds.len()).map(|_| Series::default()).collect();
    for i in 0..args.pwindow {
        for (k, s) in ser.iter_mut().enumerate() {
            r.op(k, i + 1, &db, &trunk, &mut model, &sc, s);
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
    println!(
        "MERGEWORK\tn={n}\tlabel={label}\tat=restart_end\tmerges_installed={}\tmerges_refused={}\t{}",
        side.installed,
        side.refused,
        merge_work_line(&db.branch_merge_work()).replace(' ', "\t")
    );
    drop(side);
    drop(trunk);
    drop(db);
}

fn main() {
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
        "# R11_RESIDENT_CAP={} R11_CKPT={} R12_PREWARM={} R13_FW1={} R13_FW2={} R13_MERGER={} R13_MUTANT={} \
         R13_VICTIM_LOG={} seed={} W={:#x} merge={} validator={:?} deaths={} runin={} \
         size_of_branch_state={state_bytes} size_of_table_slot={slot_bytes}",
        env("R11_RESIDENT_CAP"),
        env("R11_CKPT"),
        env("R12_PREWARM"),
        env("R13_FW1"),
        env("R13_FW2"),
        env("R13_MERGER"),
        env("R13_MUTANT"),
        env("R13_VICTIM_LOG"),
        args.seed,
        w(),
        merge_mode().name(),
        args.validator,
        deaths_name(),
        args.runin
    );
    // A3.F17/A5.4: the -MRG arm is the Merger off AND the merge mode replay-update, together.
    let merger_off = std::env::var("R13_MERGER").is_ok_and(|v| v == "off");
    if merger_off != (merge_mode() == MergeMode::ReplayUpdate) && merge_mode() != MergeMode::Update {
        die("R13_MERGER=off goes with --merge replay-update, and --merge real with the Merger on (A3.F17)");
    }
    match args.cmd.as_str() {
        "grow" => grow(&args),
        "probe" => probe(&args),
        "golden" => golden(),
        "stack" => stack(&args),
        "crash" => crash(&args),
        "restart" => restart(&args),
        other => die(&format!("unknown command {other}")),
    }
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
