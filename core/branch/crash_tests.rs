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
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
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
                _ => Op::Table(format!("d{k}")),
            };
            conn.execute(sql(&op))?;
            ops.push(op);
        }
        conn.execute("COMMIT")?;
        Ok((k, ops))
    })();
    match attempt {
        Ok(done) => Ok(Some(done)),
        Err(LimboError::Busy) | Err(LimboError::BusySnapshot) => {
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
                if n % checkpoint_every == 0 {
                    // A forced trunk checkpoint (amendment 12); a busy one is skipped.
                    if conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").is_ok() {
                        shared.checkpoints.fetch_add(1, O::Relaxed);
                    }
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
