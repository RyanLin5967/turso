//! fastest-engine C0 and C1 (PREREG v1 §8 with amendments 9, 12, 35-38): the differential model
//! and the SIGKILL crash harness for durable branch creation.
//!
//! # The workload, shared by both
//!
//! The trunk holds `t(id, v)` (wide rows, many pages) and `seq(id, n)`. Every trunk transaction
//! first increments `seq.n` and reads it back, so each committed trunk transaction carries its
//! position `k` in the trunk's commit order (writers serialise on the WAL write lock), and every
//! branch carries in its own `seq` row the `k` of the trunk state it was forked at. The model of
//! the trunk is the commit log `k -> writes`; the trunk state at `k` is the seed plus commits
//! `1..=k`. A branch's model is the state it was forked at plus its own acknowledged writes.
//!
//! # C0 (in process, no crash)
//!
//! `WRITERS` trunk writers commit one-to-three-statement transactions (UPDATE, INSERT, DELETE and,
//! rarely, CREATE TABLE) on pages the branches share, while `DRIVERS` threads create branches (from
//! the trunk and from their own branches, named and unnamed), write them, read them back whole
//! against their models and delete them; a forced `PRAGMA wal_checkpoint(TRUNCATE)` runs every
//! `checkpoint_every` operations, and the database is reopened between epochs. Checked:
//! * E4 on every create from the trunk: the branch's `k` lies between the last trunk commit
//!   acknowledged before the create began and the commits begun before it was acknowledged, and its
//!   whole content is the trunk state at `k` (tables included);
//! * a create from a branch equals that branch's model;
//! * every read of a live branch equals its model, at any time (I1 and I3 under trunk commits and
//!   checkpoints);
//! * after every reopen: every live branch equals its model (by id, and by name for a named one),
//!   every deleted one is gone (I4), and the trunk equals its last committed state.
//!
//! Size from the environment: `FE_C0_OPS` (operations, default 3000), `FE_C0_EPOCHS` (reopens + 1,
//! default 3), `FE_C0_CLASS` (`off`, `fsync`, `full`; default `fsync`), `FE_C0_SEED`. The
//! registered run is `FE_C0_OPS=100000` over at least 1,000 created branches; the test prints its
//! counts, and refuses a run that created fewer branches than `FE_C0_MIN_BRANCHES`.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64, Ordering as O};
use std::sync::Mutex as StdMutex;

const ROWS: i64 = 240;
const PAD: usize = 300;
const WRITERS: usize = 4;
const DRIVERS: usize = 3;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_class() -> SyncClass {
    match std::env::var("FE_C0_CLASS").as_deref() {
        Ok("off") => SyncClass::Off,
        Ok("full") => SyncClass::FullFsync,
        _ => SyncClass::Fsync,
    }
}

fn open_at(path: &Path, opts: DatabaseOpts) -> Arc<Database> {
    try_open_at(path, opts).unwrap()
}

fn try_open_at(path: &Path, opts: DatabaseOpts) -> Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
}

fn opts(catalog: bool, sync: SyncClass) -> DatabaseOpts {
    DatabaseOpts::new().with_branch_durability(if catalog {
        BranchDurability::Catalog { sync }
    } else {
        BranchDurability::Durable { sync }
    })
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// What a database (the trunk, or a branch) holds, as the model and the checks see it: `t`'s rows
/// by id (each value's short form, without its padding), the user tables' names, and `seq.n`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct State {
    rows: BTreeMap<i64, String>,
    tables: BTreeSet<String>,
    seq: u64,
}

/// One write of a transaction, as the model applies it.
#[derive(Clone, Debug)]
enum Op {
    Set(i64, String),
    Del(i64),
    Table(String),
}

impl State {
    fn apply(&mut self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Set(id, v) => {
                    self.rows.insert(*id, v.clone());
                }
                Op::Del(id) => {
                    self.rows.remove(id);
                }
                Op::Table(name) => {
                    self.tables.insert(name.clone());
                }
            }
        }
    }
}

fn padded(short: &str) -> String {
    format!("{short}-{}", "x".repeat(PAD))
}

fn sql(op: &Op) -> String {
    match op {
        Op::Set(id, v) => format!("INSERT OR REPLACE INTO t(id, v) VALUES ({id}, '{}')", padded(v)),
        Op::Del(id) => format!("DELETE FROM t WHERE id = {id}"),
        Op::Table(name) => format!("CREATE TABLE {name}(x)"),
    }
}

/// Read a database whole: `t`, the user tables, and `seq`.
fn read_state(conn: &Arc<Connection>) -> Result<State> {
    let mut state = State::default();
    for row in conn.prepare("SELECT id, v FROM t ORDER BY id")?.run_collect_rows()? {
        let id = row[0].as_int().expect("integer id");
        let v = match &row[1] {
            Value::Text(t) => t.as_str().split("-x").next().unwrap_or("").to_string(),
            other => panic!("expected text, got {other:?}"),
        };
        state.rows.insert(id, v);
    }
    for row in conn
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT IN ('t', 'seq')")?
        .run_collect_rows()?
    {
        if let Value::Text(t) = &row[0] {
            state.tables.insert(t.as_str().to_string());
        }
    }
    let seq = conn.prepare("SELECT n FROM seq WHERE id = 1")?.run_collect_rows()?;
    state.seq = seq[0][0].as_int().expect("integer seq") as u64;
    Ok(state)
}

fn seed_trunk(conn: &Arc<Connection>) -> State {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.execute("CREATE TABLE seq(id INTEGER PRIMARY KEY, n INTEGER)").unwrap();
    conn.execute("BEGIN").unwrap();
    let mut state = State::default();
    for id in 1..=ROWS {
        let v = format!("s{id}");
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", padded(&v))).unwrap();
        state.rows.insert(id, v);
    }
    conn.execute("INSERT INTO seq VALUES (1, 0)").unwrap();
    conn.execute("COMMIT").unwrap();
    state
}

/// The trunk's commit log: the seed and every committed transaction's writes by `k`, with the
/// states at every `CACHE_EVERY`-th `k` cached once every commit up to them is known.
pub(crate) struct TrunkModel {
    commits: BTreeMap<u64, Vec<Op>>,
    cache: BTreeMap<u64, State>,
}

const CACHE_EVERY: u64 = 32;

impl TrunkModel {
    fn new(seed: State) -> Self {
        let mut cache = BTreeMap::new();
        cache.insert(0, seed);
        Self {
            commits: BTreeMap::new(),
            cache,
        }
    }

    /// The trunk state at `k`, if every commit up to `k` is known.
    fn state_at(&mut self, k: u64) -> Option<State> {
        let (&base_k, base) = self.cache.range(..=k).next_back()?;
        let mut state = base.clone();
        for j in base_k + 1..=k {
            let ops = self.commits.get(&j)?;
            state.apply(ops);
            state.seq = j;
            if j % CACHE_EVERY == 0 {
                self.cache.insert(j, state.clone());
            }
        }
        state.seq = k;
        Some(state)
    }

    fn last(&self) -> u64 {
        self.commits.keys().next_back().copied().unwrap_or(0)
    }
}

/// One live branch a driver owns.
#[derive(Clone)]
struct Owned {
    id: BranchId,
    name: Option<String>,
    model: State,
    depth: usize,
}

/// Shared between the C0 threads.
struct Shared {
    trunk: StdMutex<TrunkModel>,
    /// The highest `k` whose COMMIT has returned (a lower bound for a later fork's `k`).
    acked: StdU64,
    /// The highest `k` a writer has taken (BEGIN ... SELECT n), committed or not.
    begun: StdU64,
    ops: StdU64,
    stop: StdBool,
    mismatches: StdMutex<Vec<String>>,
    /// E4 checks of trunk forks whose `k` is not in the commit log yet: (what, k, observed).
    pending: StdMutex<Vec<(String, u64, State)>>,
    created: StdU64,
    created_named: StdU64,
    created_from_branch: StdU64,
    deleted: StdU64,
    reads: StdU64,
    branch_writes: StdU64,
    trunk_commits: StdU64,
    checkpoints: StdU64,
    checkpoint_rounds: StdU64,
}

impl Shared {
    fn mismatch(&self, what: String) {
        let mut m = self.mismatches.lock().unwrap();
        if m.len() < 50 {
            m.push(what);
        }
        self.stop.store(true, O::Release);
    }
}

/// One trunk transaction: take `k`, apply 1-3 writes, commit. `Ok(None)` when it lost to Busy (it
/// rolled back; the caller retries).
fn trunk_txn(conn: &Arc<Connection>, rng: &mut Rng, shared: &Shared) -> Result<Option<(u64, Vec<Op>)>> {
    let attempt = (|| -> Result<(u64, Vec<Op>)> {
        conn.execute("BEGIN")?;
        conn.execute("UPDATE seq SET n = n + 1 WHERE id = 1")?;
        let k = conn.prepare("SELECT n FROM seq WHERE id = 1")?.run_collect_rows()?[0][0]
            .as_int()
            .expect("integer seq") as u64;
        shared.begun.fetch_max(k, O::AcqRel);
        let mut ops = Vec::new();
        for j in 0..=rng.below(3) {
            let op = match rng.below(100) {
                0..=69 => Op::Set(1 + rng.below(ROWS as u64) as i64, format!("k{k}j{j}")),
                70..=89 => Op::Set(10_000 + (k * 4 + j) as i64, format!("k{k}i{j}")),
                90..=97 => Op::Del(1 + rng.below(ROWS as u64) as i64),
                // A name per ATTEMPT: a rolled-back CREATE TABLE has been seen to leave its name
                // taken in the connection that rolled it back (Turso behaviour this harness does
                // not test; recorded in the lane notes).
                _ => Op::Table(format!("d{k}a{}", rng.below(1 << 30))),
            };
            conn.execute(sql(&op))?;
            ops.push(op);
        }
        conn.execute("COMMIT")?;
        Ok((k, ops))
    })();
    match attempt {
        Ok(done) => Ok(Some(done)),
        Err(LimboError::Busy) | Err(LimboError::BusySnapshot) | Err(LimboError::SchemaUpdated) => {
            let _ = conn.execute("ROLLBACK");
            Ok(None)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK");
            Err(e)
        }
    }
}

fn writer_loop(db: Arc<Database>, shared: Arc<Shared>, seed: u64, quota: u64, checkpoint_every: u64) {
    let conn = db.connect().unwrap();
    let mut rng = Rng(seed | 1);
    while !shared.stop.load(O::Acquire) && shared.ops.load(O::Acquire) < quota {
        match trunk_txn(&conn, &mut rng, &shared) {
            Ok(Some((k, ops))) => {
                shared.trunk.lock().unwrap().commits.insert(k, ops);
                shared.acked.fetch_max(k, O::AcqRel);
                shared.trunk_commits.fetch_add(1, O::Relaxed);
                let n = shared.ops.fetch_add(1, O::AcqRel) + 1;
                // A forced trunk checkpoint each time the operation count crosses a multiple of
                // `checkpoint_every` (amendment 12), by whichever writer sees the crossing first.
                let due = n / checkpoint_every;
                let done = shared.checkpoint_rounds.load(O::Acquire);
                if due > done
                    && shared
                        .checkpoint_rounds
                        .compare_exchange(done, due, O::AcqRel, O::Acquire)
                        .is_ok()
                    && conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").is_ok()
                {
                    shared.checkpoints.fetch_add(1, O::Relaxed);
                }
            }
            Ok(None) => std::thread::yield_now(),
            Err(e) => {
                shared.mismatch(format!("trunk writer failed: {e}"));
                return;
            }
        }
        // The writers run at a fraction of their solo rate (amendment 12 asks >= 50%).
        if rng.below(4) == 0 {
            std::thread::yield_now();
        }
    }
}

/// Retry a fork that lost to a DDL commit in flight (`SchemaUpdated`) or to the WAL write lock.
fn retrying<T>(mut f: impl FnMut() -> Result<T>) -> Result<T> {
    let mut tries = 0;
    loop {
        match f() {
            Err(LimboError::SchemaUpdated) | Err(LimboError::Busy) if tries < 200 => {
                tries += 1;
                std::thread::yield_now();
            }
            other => return other,
        }
    }
}

fn check(shared: &Shared, what: &str, got: &State, want: &State) {
    if got != want {
        let diff: Vec<String> = want
            .rows
            .iter()
            .filter(|(id, v)| got.rows.get(id) != Some(v))
            .take(3)
            .map(|(id, v)| format!("row {id}: want {v:?} got {:?}", got.rows.get(id)))
            .chain(
                got.rows
                    .keys()
                    .filter(|id| !want.rows.contains_key(id))
                    .take(3)
                    .map(|id| format!("row {id}: want absent got present")),
            )
            .collect();
        shared.mismatch(format!(
            "{what}: seq want {} got {}; tables want {:?} got {:?}; {}",
            want.seq,
            got.seq,
            want.tables,
            got.tables,
            diff.join("; ")
        ));
    }
}

/// A connection on a branch this driver owns.
fn connect(db: &Arc<Database>, b: &Owned) -> Result<Arc<Connection>> {
    match &b.name {
        Some(name) => db.connect_named(name),
        None => {
            // Re-attached for the connection and detached again at once: a dropped handle reaps.
            let h = db.branch(b.id)?;
            let c = h.connect();
            let _ = h.into_id();
            c
        }
    }
}

fn release(db: &Arc<Database>, b: &Owned) -> Result<()> {
    match &b.name {
        Some(name) => db.drop_branch(name).map(|_| ()),
        None => db.branch(b.id)?.reap().map(|_| ()),
    }
}

#[allow(clippy::too_many_arguments)]
fn driver_loop(
    db: Arc<Database>,
    shared: Arc<Shared>,
    mut owned: Vec<Owned>,
    gone: Arc<StdMutex<Vec<Owned>>>,
    driver: usize,
    seed: u64,
    quota: u64,
    target: usize,
) -> Vec<Owned> {
    let trunk = db.connect().unwrap();
    let mut rng = Rng(seed | 1);
    let mut serial_name = 0u64;
    while !shared.stop.load(O::Acquire) && shared.ops.load(O::Acquire) < quota {
        let roll = rng.below(100);
        let result: Result<()> = (|| {
            if owned.len() < target / 2 || (roll < 30 && owned.len() < target) {
                // Create: from the trunk (two in three) or from an owned branch.
                let named = rng.below(2) == 0;
                serial_name += 1;
                let name = format!("d{driver}-{seed}-{serial_name}");
                if owned.is_empty() || rng.below(3) != 0 {
                    let lo = shared.acked.load(O::Acquire);
                    let id = retrying(|| {
                        if named {
                            trunk.create_branch(&name)
                        } else {
                            trunk.fork_branch().map(|b| b.into_id())
                        }
                    })?;
                    let hi = shared.begun.load(O::Acquire) + WRITERS as u64;
                    let b = Owned {
                        id,
                        name: named.then(|| name.clone()),
                        model: State::default(),
                        depth: 1,
                    };
                    let got = read_state(&connect(&db, &b)?)?;
                    let k = got.seq;
                    if k < lo || k > hi {
                        shared.mismatch(format!(
                            "E4: branch {} forked at k={k}, outside [{lo}, {hi}]",
                            id.0
                        ));
                    }
                    let want = shared.trunk.lock().unwrap().state_at(k);
                    match want {
                        Some(want) => check(&shared, &format!("E4 create {}", id.0), &got, &want),
                        None => shared.pending.lock().unwrap().push((
                            format!("E4 create {}", id.0),
                            k,
                            got.clone(),
                        )),
                    }
                    shared.created.fetch_add(1, O::Relaxed);
                    shared.created_named.fetch_add(named as u64, O::Relaxed);
                    owned.push(Owned { model: got, ..b });
                } else {
                    let i = rng.below(owned.len() as u64) as usize;
                    let parent = owned[i].clone();
                    let pc = connect(&db, &parent)?;
                    let id = retrying(|| {
                        if named {
                            pc.create_branch(&name)
                        } else {
                            pc.fork_branch().map(|b| b.into_id())
                        }
                    })?;
                    drop(pc);
                    let b = Owned {
                        id,
                        name: named.then(|| name.clone()),
                        model: parent.model.clone(),
                        depth: parent.depth + 1,
                    };
                    let got = read_state(&connect(&db, &b)?)?;
                    check(&shared, &format!("create {} from branch {}", id.0, parent.id.0), &got, &b.model);
                    shared.created.fetch_add(1, O::Relaxed);
                    shared.created_named.fetch_add(named as u64, O::Relaxed);
                    shared.created_from_branch.fetch_add(1, O::Relaxed);
                    owned.push(b);
                }
            } else if roll < 65 && !owned.is_empty() {
                // Write an owned branch: 1-3 statements in one transaction.
                let i = rng.below(owned.len() as u64) as usize;
                let c = connect(&db, &owned[i])?;
                let mut ops = Vec::new();
                c.execute("BEGIN")?;
                for j in 0..=rng.below(3) {
                    let op = match rng.below(100) {
                        0..=74 => Op::Set(1 + rng.below(ROWS as u64) as i64, format!("b{}w{j}r{}", owned[i].id.0, rng.below(1000))),
                        75..=89 => Op::Set(20_000 + rng.below(5000) as i64, format!("b{}n{j}", owned[i].id.0)),
                        90..=97 => Op::Del(1 + rng.below(ROWS as u64) as i64),
                        _ => Op::Table(format!("e{}x{}", owned[i].id.0, rng.below(1_000_000))),
                    };
                    c.execute(sql(&op))?;
                    ops.push(op);
                }
                c.execute("COMMIT")?;
                owned[i].model.apply(&ops);
                shared.branch_writes.fetch_add(1, O::Relaxed);
            } else if roll < 90 && !owned.is_empty() {
                // Read an owned branch whole against its model.
                let i = rng.below(owned.len() as u64) as usize;
                let got = read_state(&connect(&db, &owned[i])?)?;
                check(&shared, &format!("read {}", owned[i].id.0), &got, &owned[i].model);
                shared.reads.fetch_add(1, O::Relaxed);
            } else if !owned.is_empty() {
                // Delete an owned branch (its children, if any, stay live).
                let i = rng.below(owned.len() as u64) as usize;
                let b = owned.swap_remove(i);
                release(&db, &b)?;
                shared.deleted.fetch_add(1, O::Relaxed);
                gone.lock().unwrap().push(b);
            }
            Ok(())
        })();
        if let Err(e) = result {
            shared.mismatch(format!("driver {driver}: {e}"));
            break;
        }
        shared.ops.fetch_add(1, O::AcqRel);
    }
    owned
}

/// The whole-database check after a reopen (I1, I3, I4).
fn verify_after_reopen(db: &Arc<Database>, shared: &Shared, owned: &[Vec<Owned>], gone: &[Owned], trunk_want: &State) {
    let trunk = db.connect().unwrap();
    match read_state(&trunk) {
        Ok(got) => check(shared, "trunk after reopen", &got, trunk_want),
        Err(e) => shared.mismatch(format!("trunk unreadable after reopen: {e}")),
    }
    for b in owned.iter().flatten() {
        match connect(db, b).and_then(|c| read_state(&c)) {
            Ok(got) => check(shared, &format!("branch {} after reopen", b.id.0), &got, &b.model),
            Err(e) => shared.mismatch(format!("branch {} lost at reopen: {e}", b.id.0)),
        }
        if let Some(name) = &b.name {
            if db.branch_named(name).ok().flatten() != Some(b.id) {
                shared.mismatch(format!("name {name} does not name branch {} after reopen", b.id.0));
            }
        }
    }
    let live: BTreeSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
    for b in gone {
        if live.contains(&b.id) {
            shared.mismatch(format!("deleted branch {} is back after reopen (I4)", b.id.0));
        }
        if let Some(name) = &b.name {
            if let Ok(Some(id)) = db.branch_named(name) {
                shared.mismatch(format!("deleted name {name} names branch {} after reopen", id.0));
            }
        }
    }
}

/// C0, the differential model, at the size the environment sets (see the module doc).
fn run_c0(catalog: bool) -> String {
    let ops_total = env_u64("FE_C0_OPS", 3000);
    let epochs = env_u64("FE_C0_EPOCHS", 3).max(1);
    let seed = env_u64("FE_C0_SEED", 0x00C0_FFEE);
    let class = env_class();
    let target = env_u64("FE_C0_LIVE", 24) as usize;
    let checkpoint_every = env_u64("FE_C0_CHECKPOINT_EVERY", 1000);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c0.db");
    let db = open_at(&path, opts(catalog, class));
    let trunk_seed = seed_trunk(&db.connect().unwrap());
    // The trunk's first child, forked under the WAL write lock, so later trunk forks are lock-free.
    let anchor = db.connect().unwrap().fork_branch().unwrap().into_id();
    let shared = Arc::new(Shared {
        trunk: StdMutex::new(TrunkModel::new(trunk_seed)),
        acked: StdU64::new(0),
        begun: StdU64::new(0),
        ops: StdU64::new(0),
        stop: StdBool::new(false),
        mismatches: StdMutex::new(Vec::new()),
        pending: StdMutex::new(Vec::new()),
        created: StdU64::new(0),
        created_named: StdU64::new(0),
        created_from_branch: StdU64::new(0),
        deleted: StdU64::new(0),
        reads: StdU64::new(0),
        branch_writes: StdU64::new(0),
        trunk_commits: StdU64::new(0),
        checkpoints: StdU64::new(0),
        checkpoint_rounds: StdU64::new(0),
    });
    let gone = Arc::new(StdMutex::new(Vec::new()));
    let mut owned: Vec<Vec<Owned>> = vec![Vec::new(); DRIVERS];
    let mut db = db;
    for epoch in 0..epochs {
        let quota = ops_total * (epoch + 1) / epochs;
        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let (db, shared) = (db.clone(), shared.clone());
                let s = seed ^ ((epoch + 1) * 0x9E37_79B9 + w as u64);
                std::thread::spawn(move || writer_loop(db, shared, s, quota, checkpoint_every))
            })
            .collect();
        let drivers: Vec<_> = (0..DRIVERS)
            .map(|d| {
                let (db, shared, gone) = (db.clone(), shared.clone(), gone.clone());
                let mine = std::mem::take(&mut owned[d]);
                let s = seed ^ ((epoch + 1) * 0xD1B5_4A32 + 0x1000 * (d as u64 + 1));
                std::thread::spawn(move || driver_loop(db, shared, mine, gone, d, s, quota, target))
            })
            .collect();
        for w in writers {
            w.join().expect("trunk writer panicked");
        }
        for (d, t) in drivers.into_iter().enumerate() {
            owned[d] = t.join().expect("driver panicked");
        }
        if shared.stop.load(O::Acquire) {
            break;
        }
        // Every commit is in the log now: settle the E4 checks still pending.
        let pending = std::mem::take(&mut *shared.pending.lock().unwrap());
        for (what, k, got) in pending {
            let want = shared.trunk.lock().unwrap().state_at(k);
            match want {
                Some(want) => check(&shared, &what, &got, &want),
                None => shared.mismatch(format!("{what}: trunk commit {k} never logged")),
            }
        }
        // Reopen, and check everything that must have survived.
        let incarnation = db.incarnation;
        drop(db);
        db = open_at(&path, opts(catalog, class));
        assert_ne!(db.incarnation, incarnation, "the registry returned the old Database");
        let last = shared.trunk.lock().unwrap().last();
        let trunk_want = shared.trunk.lock().unwrap().state_at(last).expect("every commit logged");
        verify_after_reopen(&db, &shared, &owned, &gone.lock().unwrap(), &trunk_want);
        if shared.stop.load(O::Acquire) {
            break;
        }
    }
    let _ = anchor;
    let mismatches = shared.mismatches.lock().unwrap().clone();
    let summary = format!(
        "C0 catalog={catalog} class={class:?} ops={} created={} (named {}, from a branch {}) deleted={} \
         branch_writes={} reads={} trunk_commits={} trunk_checkpoints={} epochs={epochs} mismatches={}",
        shared.ops.load(O::Relaxed),
        shared.created.load(O::Relaxed),
        shared.created_named.load(O::Relaxed),
        shared.created_from_branch.load(O::Relaxed),
        shared.deleted.load(O::Relaxed),
        shared.branch_writes.load(O::Relaxed),
        shared.reads.load(O::Relaxed),
        shared.trunk_commits.load(O::Relaxed),
        shared.checkpoints.load(O::Relaxed),
        mismatches.len(),
    );
    println!("{summary}");
    assert!(mismatches.is_empty(), "{summary}\n{}", mismatches.join("\n"));
    let min_branches = env_u64("FE_C0_MIN_BRANCHES", 0);
    assert!(
        shared.created.load(O::Relaxed) >= min_branches,
        "{summary}: fewer than FE_C0_MIN_BRANCHES={min_branches} branches were created"
    );
    assert!(shared.trunk_commits.load(O::Relaxed) > 0, "{summary}: no trunk commit ran");
    summary
}

/// C0 in snapshot mode (the size from the environment; small by default, for every suite run).
#[test]
fn c0_differential_model_snapshot_mode() {
    let _s = serial();
    run_c0(false);
}

/// C0 in catalog mode.
#[test]
fn c0_differential_model_catalog_mode() {
    let _s = serial();
    run_c0(true);
}

// ---- C1: SIGKILL aimed at named code points (PREREG v1 §8; amendments 35-38) ----
//
// A trial: the parent seeds a database and closes it; a CHILD (this test binary, running only
// `c1_child`) opens it and runs the workload on `FE_C1_C` threads, writing every operation to the
// crash log twice — an attempt line before it is issued and an ACK line after it returned — with
// one `write(2)` each, so a line is in the file before the next step. `FE_KILL_AT=<point>:<n>`
// SIGKILLs the child the n-th time it reaches `point` (`store::kill_point`); a child that never
// gets there in `C1_TRIAL_SECS` is SIGKILLed by the parent instead (an UNAIMED kill, counted
// apart). Optionally (`FE_C1_RECOVER_KILL`) a second child is killed inside recovery's replay.
// Then the parent opens the database (a real recovery) and checks, from the log alone:
// * I1: every acknowledged, undeleted branch exists (by name, or by id when unnamed) and reads its
//   fork-point state plus its acknowledged writes;
// * I2: an operation attempted and not acknowledged is all there or not at all;
// * I3: the trunk is exactly its state at the last commit it recovered, which is at or past every
//   acknowledged one; every branch is checked whole, so a sibling's or parent's write would show;
// * I4: an acknowledged delete stays deleted, and frees its name;
// * I5: the integrity check passes on the trunk and on every branch;
// * I7: a dropped and re-created name names the newest branch.
//
// Log lines: `T <k> <ops>` (trunk attempt), `TA <k>`, `C <name> <parent|->` (create attempt),
// `CA <name> <id> <k>`, `W <name> <ops>`, `WA <name>`, `D <name>`, `DA <name>`, and the kill
// point's own `KILL-AT <point> <n>`. Ops: `s:<id>:<v>`, `d:<id>`, `t:<name>`.

/// The kill points C1 aims at, by the phase they sit in (amendment 37).
const C1_POINTS: &[&str] = &[
    "fork.applied",
    "flight.taken",
    "flight.arena_synced",
    "flight.log_written",
    "flight.before_log_sync",
    "flight.log_synced",
    "flight.landed",
    "commit.slots_written",
    "commit.applied",
    "release.applied",
    "trunk.decided",
    "trunk.barrier_done",
    "trunk.wal_written",
    "trunk.published",
    "ckpt.captured",
    "ckpt.written",
    "ckpt.installed",
    "compact.renamed",
];

const C1_TRIAL_SECS: u64 = 20;

fn encode_ops(ops: &[Op]) -> String {
    ops.iter()
        .map(|op| match op {
            Op::Set(id, v) => format!("s:{id}:{v}"),
            Op::Del(id) => format!("d:{id}"),
            Op::Table(name) => format!("t:{name}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn decode_ops(words: &[&str]) -> Vec<Op> {
    words
        .iter()
        .map(|w| {
            let mut parts = w.splitn(3, ':');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("s"), Some(id), Some(v)) => Op::Set(id.parse().unwrap(), v.to_string()),
                (Some("d"), Some(id), None) => Op::Del(id.parse().unwrap()),
                (Some("t"), Some(name), None) => Op::Table(name.to_string()),
                other => panic!("undecodable op {w:?}: {other:?}"),
            }
        })
        .collect()
}

fn c1_opts() -> DatabaseOpts {
    let catalog = std::env::var("FE_C1_CATALOG").is_ok_and(|v| v == "1");
    let opts = opts(catalog, env_class_named("FE_C1_CLASS"));
    // E3's database gives every fork a lease, which a named branch must not take (E3 a).
    if std::env::var_os("FE_E3_CHILD").is_some() {
        return opts.with_branch_lease(Some(std::time::Duration::from_secs(1)));
    }
    opts
}

fn env_class_named(var: &str) -> SyncClass {
    match std::env::var(var).as_deref() {
        Ok("off") => SyncClass::Off,
        Ok("full") => SyncClass::FullFsync,
        _ => SyncClass::Fsync,
    }
}

fn log(line: &str) {
    super::store::crash_log(line);
}

/// The child's workload: runs until it is killed (it never returns on its own).
fn c1_workload(db: Arc<Database>, threads: usize, seed: u64) {
    let writers = if threads == 1 { 0 } else { (threads / 4).clamp(1, 4) };
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut rng = Rng((seed ^ (t as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1);
                let trunk = db.connect().unwrap();
                // (name, id) of this thread's live branches.
                let mut mine: Vec<(String, BranchId, bool)> = Vec::new();
                let mut counter = 0u64;
                loop {
                    let roll = rng.below(100);
                    let trunk_turn = if t < writers { roll < 80 } else { threads == 1 && roll < 25 };
                    if trunk_turn {
                        let attempt = (|| -> Result<()> {
                            trunk.execute("BEGIN")?;
                            trunk.execute("UPDATE seq SET n = n + 1 WHERE id = 1")?;
                            let k = trunk.prepare("SELECT n FROM seq WHERE id = 1")?.run_collect_rows()?[0][0]
                                .as_int()
                                .unwrap();
                            let mut ops = Vec::new();
                            for j in 0..=rng.below(3) {
                                ops.push(match rng.below(100) {
                                    0..=79 => Op::Set(1 + rng.below(ROWS as u64) as i64, format!("k{k}j{j}")),
                                    80..=94 => Op::Set(10_000 + k as i64 * 4 + j as i64, format!("k{k}i{j}")),
                                    _ => Op::Del(1 + rng.below(ROWS as u64) as i64),
                                });
                            }
                            for op in &ops {
                                trunk.execute(sql(op))?;
                            }
                            log(&format!("T {k} {}", encode_ops(&ops)));
                            trunk.execute("COMMIT")?;
                            log(&format!("TA {k}"));
                            Ok(())
                        })();
                        if attempt.is_err() {
                            let _ = trunk.execute("ROLLBACK");
                        }
                        continue;
                    }
                    let res: Result<()> = (|| {
                        if mine.len() < 6 || (roll < 40 && mine.len() < 12) {
                            counter += 1;
                            let name = format!("c{t}-{counter}");
                            let named = rng.below(4) != 0;
                            let from = (!mine.is_empty() && rng.below(3) == 0)
                                .then(|| mine[rng.below(mine.len() as u64) as usize].clone());
                            log(&format!(
                                "C {name} {}",
                                from.as_ref().map_or("-".to_string(), |p| p.0.clone())
                            ));
                            let id = retrying(|| {
                                let conn = match &from {
                                    None => trunk.clone(),
                                    Some((pname, pid, pnamed)) => c1_connect(&db, pname, *pid, *pnamed)?,
                                };
                                if named {
                                    conn.create_branch(&name)
                                } else {
                                    conn.fork_branch().map(|b| b.into_id())
                                }
                            })?;
                            let k = read_state(&c1_connect(&db, &name, id, named)?)?.seq;
                            log(&format!("CA {name} {} {k}", id.0));
                            mine.push((name, id, named));
                        } else if roll < 75 {
                            let i = rng.below(mine.len() as u64) as usize;
                            let (name, id, named) = mine[i].clone();
                            let c = c1_connect(&db, &name, id, named)?;
                            let mut ops = Vec::new();
                            for j in 0..=rng.below(3) {
                                ops.push(match rng.below(100) {
                                    0..=84 => Op::Set(1 + rng.below(ROWS as u64) as i64, format!("w{t}x{counter}j{j}r{}", rng.below(1000))),
                                    _ => Op::Del(1 + rng.below(ROWS as u64) as i64),
                                });
                            }
                            c.execute("BEGIN")?;
                            for op in &ops {
                                c.execute(sql(op))?;
                            }
                            log(&format!("W {name} {}", encode_ops(&ops)));
                            c.execute("COMMIT")?;
                            log(&format!("WA {name}"));
                        } else if roll < 85 {
                            let i = rng.below(mine.len() as u64) as usize;
                            let (name, id, named) = mine.swap_remove(i);
                            log(&format!("D {name}"));
                            if named {
                                db.drop_branch(&name)?;
                            } else {
                                db.branch(id)?.reap()?;
                            }
                            log(&format!("DA {name}"));
                        } else if roll < 87 {
                            // A store checkpoint (catalog) or snapshot compaction (snapshot mode).
                            db.branch_compact_now()?;
                        } else {
                            let i = rng.below(mine.len() as u64) as usize;
                            let (name, id, named) = mine[i].clone();
                            read_state(&c1_connect(&db, &name, id, named)?)?;
                        }
                        Ok(())
                    })();
                    if let Err(e) = res {
                        log(&format!("ERR thread {t}: {e}"));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
}

fn c1_connect(db: &Arc<Database>, name: &str, id: BranchId, named: bool) -> Result<Arc<Connection>> {
    if named {
        db.connect_named(name)
    } else {
        let h = db.branch(id)?;
        let c = h.connect();
        let _ = h.into_id();
        c
    }
}

/// The C1 child: only when the parent spawned this binary to run it (`FE_C1_CHILD`).
#[test]
fn c1_child() {
    let Ok(path) = std::env::var("FE_C1_CHILD") else {
        return;
    };
    let db = open_at(Path::new(&path), c1_opts());
    if std::env::var_os("FE_C1_RECOVER_ONLY").is_some() {
        // A recovery that the parent aims a kill into (phase 8); the open above was it.
        log("RECOVERED");
        return;
    }
    let threads = env_u64("FE_C1_C", 16) as usize;
    c1_workload(db, threads, env_u64("FE_C1_SEED", 1));
}

/// What the log says, replayed into models.
#[derive(Default)]
struct C1Log {
    /// The last attempt of each trunk commit `k`.
    trunk_try: BTreeMap<u64, Vec<Op>>,
    trunk_acked: u64,
    /// name -> branch: its id once acknowledged, its acknowledged model, a pending write, a
    /// pending delete; in creation order (a re-created name is a new entry, the older one gone).
    branches: Vec<C1Branch>,
    killed_at: Option<String>,
    errors: Vec<String>,
}

#[derive(Clone, Debug)]
struct C1Branch {
    name: String,
    parent: Option<String>,
    id: Option<BranchId>,
    /// None until its create is acknowledged.
    model: Option<State>,
    pending_write: Option<Vec<Op>>,
    delete: Option<bool>, // Some(false): attempted; Some(true): acknowledged
}

fn trunk_state(log: &C1Log, seed: &State, k: u64) -> Option<State> {
    let mut state = seed.clone();
    for j in 1..=k {
        state.apply(log.trunk_try.get(&j)?);
    }
    state.seq = k;
    Some(state)
}

fn parse_c1_log(text: &str, seed: &State) -> C1Log {
    let mut log = C1Log::default();
    for line in text.lines() {
        let words: Vec<&str> = line.split(' ').collect();
        match words.as_slice() {
            ["T", k, ops @ ..] => {
                log.trunk_try.insert(k.parse().unwrap(), decode_ops(ops));
            }
            ["TA", k] => log.trunk_acked = log.trunk_acked.max(k.parse().unwrap()),
            ["C", name, parent] => log.branches.push(C1Branch {
                name: name.to_string(),
                parent: (*parent != "-").then(|| parent.to_string()),
                id: None,
                model: None,
                pending_write: None,
                delete: None,
            }),
            ["CA", name, id, k] => {
                let k: u64 = k.parse().unwrap();
                let i = log.branches.iter().rposition(|b| b.name == *name).expect("create attempt");
                let model = match log.branches[i].parent.clone() {
                    None => trunk_state(&log, seed, k),
                    Some(p) => log
                        .branches
                        .iter()
                        .rev()
                        .find(|b| b.name == p && b.model.is_some())
                        .and_then(|b| b.model.clone()),
                };
                let b = &mut log.branches[i];
                b.id = Some(BranchId(id.parse().unwrap()));
                if model.is_none() {
                    log.errors.push(format!("{line}: no model for its fork point"));
                }
                b.model = model;
            }
            ["W", name, ops @ ..] => {
                if let Some(b) = log.branches.iter_mut().rev().find(|b| b.name == *name) {
                    b.pending_write = Some(decode_ops(ops));
                }
            }
            ["WA", name] => {
                if let Some(b) = log.branches.iter_mut().rev().find(|b| b.name == *name) {
                    if let (Some(model), Some(ops)) = (b.model.as_mut(), b.pending_write.take()) {
                        model.apply(&ops);
                    }
                }
            }
            ["D", name] => {
                if let Some(b) = log.branches.iter_mut().rev().find(|b| b.name == *name) {
                    b.delete = Some(false);
                }
            }
            ["DA", name] => {
                if let Some(b) = log.branches.iter_mut().rev().find(|b| b.name == *name) {
                    b.delete = Some(true);
                }
            }
            ["KILL-AT", point, n] => log.killed_at = Some(format!("{point}:{n}")),
            _ => {}
        }
    }
    log
}

fn integrity(conn: &Arc<Connection>) -> Result<String> {
    let rows = conn.prepare("PRAGMA integrity_check")?.run_collect_rows()?;
    Ok(match &rows[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        other => format!("{other:?}"),
    })
}

/// Check the recovered database against the log (see the C1 section's invariants). Returns the
/// violations.
fn verify_c1(db: &Arc<Database>, log: &C1Log, seed: &State) -> Vec<String> {
    let mut bad = log.errors.clone();
    let trunk = db.connect().unwrap();
    match read_state(&trunk) {
        Ok(got) => {
            if got.seq < log.trunk_acked {
                bad.push(format!("I3: trunk recovered at k={} < acknowledged {}", got.seq, log.trunk_acked));
            }
            match trunk_state(log, seed, got.seq) {
                Some(want) if want == got => {}
                Some(_) => bad.push(format!("I3: trunk at k={} differs from its commits", got.seq)),
                None => bad.push(format!("I3: trunk at k={} has commits the log never attempted", got.seq)),
            }
        }
        Err(e) => bad.push(format!("trunk unreadable: {e}")),
    }
    match integrity(&trunk) {
        Ok(v) if v == "ok" => {}
        other => bad.push(format!("I5: trunk integrity {other:?}")),
    }
    let live: BTreeSet<BranchId> = db.branch_ids().unwrap_or_default().into_iter().collect();
    // The newest entry of each name decides what the name must name (I7).
    let mut newest: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, b) in log.branches.iter().enumerate() {
        newest.insert(&b.name, i);
    }
    for (i, b) in log.branches.iter().enumerate() {
        let acked = b.model.is_some();
        let deleted = b.delete == Some(true);
        let maybe_deleted = b.delete == Some(false);
        let named = db.branch_named(&b.name).ok().flatten();
        let found = b.id.filter(|id| live.contains(id)).or_else(|| match (named, newest[b.name.as_str()] == i) {
            (Some(id), true) => Some(id),
            _ => None,
        });
        if deleted {
            if let Some(id) = b.id.filter(|id| live.contains(id)) {
                bad.push(format!("I4: deleted branch {} ({}) is back", id.0, b.name));
            }
            continue;
        }
        let Some(id) = found else {
            if acked && !maybe_deleted {
                bad.push(format!("I1: acknowledged branch {} ({:?}) is missing", b.name, b.id));
            }
            continue;
        };
        let conn = match db.branch(id) {
            Ok(h) => {
                let c = h.connect();
                let _ = h.into_id();
                c
            }
            Err(_) => db.connect_named(&b.name),
        };
        let got = match conn.and_then(|c| {
            let state = read_state(&c)?;
            let ok = integrity(&c)?;
            Ok((state, ok))
        }) {
            Ok((state, ok)) => {
                if ok != "ok" {
                    bad.push(format!("I5: branch {} integrity {ok}", b.name));
                }
                state
            }
            Err(e) => {
                bad.push(format!("branch {} unreadable: {e}", b.name));
                continue;
            }
        };
        match &b.model {
            Some(model) => {
                let mut with_pending = model.clone();
                if let Some(ops) = &b.pending_write {
                    with_pending.apply(ops);
                }
                if got != *model && got != with_pending {
                    bad.push(format!(
                        "I1/I2: branch {} ({}) reads neither its model nor its model plus its pending write",
                        id.0, b.name
                    ));
                }
            }
            None => {
                // An unacknowledged create that recovered: whole (I2), at a valid fork point.
                let fork_point = match &b.parent {
                    None => trunk_state(log, seed, got.seq),
                    Some(p) => log
                        .branches
                        .iter()
                        .rev()
                        .find(|x| x.name == *p && x.model.is_some())
                        .and_then(|x| x.model.clone()),
                };
                if fork_point.as_ref() != Some(&got) {
                    bad.push(format!("I2: unacknowledged create {} recovered torn", b.name));
                }
            }
        }
    }
    bad
}

/// One C1 trial. Returns (landed, violations, what was aimed).
fn c1_trial(exe: &Path, catalog: bool, class: SyncClass, threads: usize, point: &str, n: u64, seed: u64, recover_kill: Option<u64>) -> (bool, Vec<String>, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c1.db");
    let log_path = dir.path().join("c1.log");
    let base = {
        let db = open_at(&path, opts(catalog, class));
        let trunk = db.connect().unwrap();
        let base = seed_trunk(&trunk);
        let _ = trunk.fork_branch().unwrap().into_id();
        base
    };
    let class_name = match class {
        SyncClass::Off => "off",
        SyncClass::Fsync => "fsync",
        SyncClass::FullFsync => "full",
    };
    let spawn = |kill_at: String, recover_only: bool| {
        let mut cmd = std::process::Command::new(exe);
        cmd.args(["branch::crash_tests::c1_child", "--exact", "--test-threads=1", "--nocapture"])
            .env("FE_C1_CHILD", &path)
            .env("FE_CRASH_LOG", &log_path)
            .env("FE_KILL_AT", kill_at)
            .env("FE_C1_C", threads.to_string())
            .env("FE_C1_SEED", seed.to_string())
            .env("FE_C1_CATALOG", if catalog { "1" } else { "0" })
            .env("FE_C1_CLASS", class_name)
            .stdout(std::process::Stdio::null())
            // The child's stderr is kept: a worker thread that panics dies silently otherwise.
            .stderr(std::fs::File::create(dir.path().join("child.stderr")).unwrap());
        if recover_only {
            cmd.env("FE_C1_RECOVER_ONLY", "1");
        }
        // Simulated power loss of the branch files' unsynced writes (journal.rs `lose_unsynced`).
        if std::env::var("FE_C1_POWER").is_ok_and(|v| v == "1") {
            cmd.env("FE_LOSE_UNSYNCED", "1");
        }
        if let Ok(ms) = std::env::var("FE_KILL_DELAY_MS") {
            cmd.env("FE_KILL_DELAY_MS", ms);
        }
        cmd.spawn().unwrap()
    };
    let mut child = spawn(format!("{point}:{n}"), false);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(C1_TRIAL_SECS);
    let mut unaimed = false;
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            unaimed = true;
            let _ = child.kill();
            let _ = child.wait();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let text = std::fs::read_to_string(&log_path).unwrap_or_default();
    let log = parse_c1_log(&text, &base);
    let landed = !unaimed && log.killed_at.is_some();
    let mut what = format!("{point}:{n}{}", if unaimed { " (unaimed)" } else { "" });
    if unaimed {
        // What the child did instead, for the report (a point it never reached).
        let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
        let mut first_err = None;
        for line in text.lines() {
            let tag = line.split(' ').next().unwrap_or("");
            *counts.entry(tag).or_default() += 1;
            if tag == "ERR" && first_err.is_none() {
                first_err = Some(line.to_string());
            }
        }
        what.push_str(&format!(" log={counts:?} first_err={first_err:?}"));
    }
    // A panic in the child (a dead worker thread) is a harness or engine failure, never noise.
    let stderr = std::fs::read_to_string(dir.path().join("child.stderr")).unwrap_or_default();
    let child_panic = stderr
        .lines()
        .skip_while(|l| !l.contains("panicked"))
        .take(2)
        .collect::<Vec<_>>()
        .join(" | ");
    if let Some(m) = recover_kill {
        // Phase 8: a second kill inside recovery's replay, then the parent's own recovery.
        let mut rec = spawn(format!("recover.replay:{m}"), true);
        let _ = rec.wait();
        what.push_str(&format!(" + recover.replay:{m}"));
    }
    // A recovery that refuses the store lost every acknowledged create at once: a violation the
    // count must carry, not a harness panic that reads as no result (the D0 control's power cut
    // can leave the snapshot header unwritten).
    let mut bad = match try_open_at(&path, opts(catalog, class)) {
        Ok(db) => verify_c1(&db, &log, &base),
        Err(e) => vec![format!("recovery refused the store: {e:?}")],
    };
    if !child_panic.is_empty() {
        bad.push(format!("the child panicked: {child_panic}"));
    }
    (landed, bad, what)
}

/// C1: SIGKILLs aimed at every kill point, the database recovered and checked after each. Size
/// from the environment: `FE_C1_TRIALS` (default 34), `FE_C1_C` (threads, default 16),
/// `FE_C1_CATALOG`, `FE_C1_CLASS`, `FE_C1_SEED`, `FE_C1_RECOVER_KILL` (1: every third trial also
/// kills a recovery mid-replay). Refuses a run in which no kill landed where it was aimed.
#[test]
fn c1_sigkill_at_aimed_points() {
    if std::env::var_os("FE_C1_CHILD").is_some() {
        return;
    }
    let _s = serial();
    let exe = std::env::current_exe().unwrap();
    let trials = env_u64("FE_C1_TRIALS", 34);
    let threads = env_u64("FE_C1_C", 16) as usize;
    let catalog = std::env::var("FE_C1_CATALOG").is_ok_and(|v| v == "1");
    let class = env_class_named("FE_C1_CLASS");
    let seed = env_u64("FE_C1_SEED", 7);
    let recover_kill = std::env::var("FE_C1_RECOVER_KILL").is_ok_and(|v| v == "1");
    let mut rng = Rng(seed | 1);
    let (mut landed, mut unaimed, mut violations) = (0u64, 0u64, Vec::new());
    let mut per_point: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for trial in 0..trials {
        let only = std::env::var("FE_C1_POINT").ok();
        let point: &str = match only.as_deref() {
            Some(p) => C1_POINTS.iter().copied().find(|q| *q == p).expect("a known kill point"),
            None => C1_POINTS[trial as usize % C1_POINTS.len()],
        };
        let n = 1 + rng.below(40);
        let rk = (recover_kill && trial % 3 == 2).then(|| 1 + rng.below(20));
        let (hit, bad, what) = c1_trial(&exe, catalog, class, threads, point, n, seed ^ trial, rk);
        let e = per_point.entry(point).or_default();
        if hit {
            landed += 1;
            e.0 += 1;
        } else {
            unaimed += 1;
            e.1 += 1;
        }
        if !hit {
            println!("C1 trial {trial} unaimed: {what}");
        }
        for b in bad {
            violations.push(format!("trial {trial} [{what}]: {b}"));
        }
    }
    let power = std::env::var("FE_C1_POWER").is_ok_and(|v| v == "1");
    let summary = format!(
        "C1 catalog={catalog} class={class:?} power_loss_simulated={power} threads={threads} \
         trials={trials} landed={landed} unaimed={unaimed} violations={} per point (landed, \
         unaimed): {per_point:?}",
        violations.len()
    );
    println!("{summary}");
    assert!(violations.is_empty(), "{summary}\n{}", violations.join("\n"));
    assert!(landed > 0, "{summary}: no kill landed where it was aimed");
}

// ---- E3: create, kill -9, restart, connect by name from a new process (PREREG M1 exit 5) ----

/// The E3 child: create the named branch `FE_E3_NAME` from the trunk, write one row on it, record
/// the acknowledgement, and SIGKILL itself (no destructor, flush or clean close runs).
#[test]
fn e3_child() {
    let Ok(path) = std::env::var("FE_E3_CHILD") else {
        return;
    };
    let name = std::env::var("FE_E3_NAME").unwrap();
    let db = open_at(Path::new(&path), c1_opts());
    let trunk = db.connect().unwrap();
    trunk.create_branch(&name).unwrap();
    let c = db.connect_named(&name).unwrap();
    let row = 1 + (name.len() as i64 * 7 + name.bytes().map(i64::from).sum::<i64>()) % ROWS;
    c.execute(sql(&Op::Set(row, name.replace('-', "")))).unwrap();
    super::store::crash_log(&format!("E3A {name} {row}"));
    // SAFETY: signals this process; nothing after it runs.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// E3 (M1 exit 5; amendment 54's create-kill-restart-connect half): `FE_E3_TRIALS` times (default
/// 20; registered 1000), a child process creates a named branch, writes it, and is killed by
/// SIGKILL; then THIS process — a different one — opens the database (a recovery) and connects to
/// the new branch by name, and to up to ten older ones, each reading its write, none carrying a
/// lease. Every trial must pass.
#[test]
fn e3_kill9_restart_connect_by_name() {
    if std::env::var_os("FE_E3_CHILD").is_some() || std::env::var_os("FE_C1_CHILD").is_some() {
        return;
    }
    let _s = serial();
    let trials = env_u64("FE_E3_TRIALS", 20);
    let exe = std::env::current_exe().unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("e3.db");
    let log_path = dir.path().join("e3.log");
    let catalog = std::env::var("FE_C1_CATALOG").is_ok_and(|v| v == "1");
    let class = env_class_named("FE_C1_CLASS");
    // Every fork gets a one-second lease; a named branch must take none (E3 a).
    let opts = || opts(catalog, class).with_branch_lease(Some(std::time::Duration::from_secs(1)));
    {
        let db = open_at(&path, opts());
        let trunk = db.connect().unwrap();
        seed_trunk(&trunk);
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let mut rng = Rng(0xE3E3_E3E3 | 1);
    let mut created: Vec<(String, i64)> = Vec::new();
    let mut checks = 0u64;
    for trial in 0..trials {
        let name = format!("e3-{trial}");
        let status = std::process::Command::new(&exe)
            .args(["branch::crash_tests::e3_child", "--exact", "--test-threads=1", "--nocapture"])
            .env("FE_E3_CHILD", &path)
            .env("FE_E3_NAME", &name)
            .env("FE_CRASH_LOG", &log_path)
            .env("FE_C1_CATALOG", if catalog { "1" } else { "0" })
            .env("FE_C1_CLASS", std::env::var("FE_C1_CLASS").unwrap_or_default())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL), "trial {trial}: the child was not killed: {status:?}");
        let acked = std::fs::read_to_string(&log_path).unwrap_or_default();
        let line = acked
            .lines()
            .find(|l| l.starts_with(&format!("E3A {name} ")))
            .unwrap_or_else(|| panic!("trial {trial}: the child never acknowledged {name}"));
        let row: i64 = line.rsplit(' ').next().unwrap().parse().unwrap();
        created.push((name.clone(), row));
        // The restart, in this process: every check reads through a fresh open.
        let db = open_at(&path, opts());
        let mut sample = vec![created.len() - 1];
        for _ in 0..10.min(created.len() - 1) {
            sample.push(rng.below(created.len() as u64 - 1) as usize);
        }
        for i in sample {
            let (name, row) = &created[i];
            let c = db
                .connect_named(name)
                .unwrap_or_else(|e| panic!("trial {trial}: {name} is not connectable by name after kill -9: {e}"));
            let got = read_state(&c).unwrap();
            assert_eq!(
                got.rows.get(row).map(String::as_str),
                Some(name.replace('-', "").as_str()),
                "trial {trial}: {name} lost its acknowledged write"
            );
            checks += 1;
        }
        // E3 (a): a server branch carries no lease, even after its restart, though every unnamed
        // fork does (the setup's anchor is reaped by the first pass, which shows leases expire).
        // The named branches' ids BEFORE the pass (a reaped one would free its name).
        let named: BTreeSet<BranchId> = created
            .iter()
            .map(|(name, _)| db.branch_named(name).unwrap().expect("named"))
            .collect();
        db.branch_lease_clock_advance(std::time::Duration::from_secs(1 << 30));
        let expired = db.expire_branches().unwrap();
        for id in &expired.reaped {
            assert!(!named.contains(id), "trial {trial}: an expiry pass reaped named branch {}", id.0);
        }
        if trial == 0 {
            assert_eq!(expired.reaped.len(), 1, "premise: the leased anchor expires");
        }
    }
    println!("E3 catalog={catalog} class={class:?} trials={trials} kills={trials} checks={checks} failures=0");
}
