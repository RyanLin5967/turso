//! Merges against a model the test keeps itself, judged by CONTENT, never by the validators' rules.
//!
//! The model holds three maps per merge: the trunk as the branch forked it (base), the trunk now
//! (ours), and the branch's own view (theirs). The module promises a three-way merge that refuses
//! write-write conflicts, so the oracle is merge3 by content:
//!
//! * COMPLETENESS: a row both sides changed, to different contents, is a true conflict, and every
//!   validator must refuse the merge. (Refusing more is allowed; a refusal is always safe.)
//! * SOUNDNESS: after a committed merge the trunk equals merge3(base, ours, theirs): theirs for
//!   every row the branch changed by content, ours for every other row. The model computes that
//!   from its own maps; it does not mirror how the replay or the page copy works.
//!
//! The trunk and every live branch are read back through SQL after every step, with `PRAGMA
//! integrity_check`. Page verdicts are checked by implication (a row conflict writes the row's
//! page, so it is a page conflict; a page conflict needs a trunk write since the fork, inside a
//! batch too). An earlier version checked the row verdict against a copy of the row-stamp rule,
//! which could only confirm the rule against itself (frontier/round11/r11-merge-refute).
//!
//! The tests after the model test each reproduce one install defect the refuter found by reading:
//! a re-fired trigger, a stale statement after a trunk ALTER, an FK action or a REPLACE reverting a
//! trunk-only row, a UNIQUE move replayed in rowid order, a batch member's error dropping the batch,
//! and colliding automatic rowids; c3-c5 add the foreign-key cases the first check missed (a
//! branch child under a parent the trunk deleted; a key matched under the parent's collation and
//! affinity). Each asserts a refusal or the three-way result, and each compiles against the base
//! (`f5b0708d5`), so its red step runs this file's own text there.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::merge::{Install, MergeOutcome, MergePolicy, Merger, Refusal, Validation};
use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("merge.db");
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
    (dir, db)
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

/// Rows start 16 apart so inserts can land between any two of them.
const ROWS: i64 = 300;
const GAP: i64 = 16;

fn val(tag: &str, n: u64, len: usize) -> String {
    let head = format!("{tag}{n}-");
    format!("{head}{}", "y".repeat(len.saturating_sub(head.len())))
}

fn read_all(conn: &Arc<Connection>) -> BTreeMap<i64, String> {
    conn.prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|r| match &r[1] {
            Value::Text(t) => (r[0].as_int().unwrap(), t.as_str().to_string()),
            other => panic!("unexpected value {other:?}"),
        })
        .collect()
}

/// The first column of every row of `sql`, as text.
fn texts(conn: &Arc<Connection>, sql: &str) -> Vec<String> {
    conn.prepare(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

/// Every cell of every row of `sql`, as text: NULL as "NULL", an integer in decimal.
fn cells(conn: &Arc<Connection>, sql: &str) -> Vec<String> {
    conn.prepare(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .flat_map(|r| r.into_iter())
        .map(|v| match &v {
            Value::Null => "NULL".to_string(),
            Value::Text(t) => t.as_str().to_string(),
            other => other
                .as_int()
                .map_or_else(|| format!("{other:?}"), |i| i.to_string()),
        })
        .collect()
}

fn integrity(conn: &Arc<Connection>) -> Vec<String> {
    texts(conn, "PRAGMA integrity_check")
}

/// What the model knows about a branch: the trunk as it forked, and its own view since.
struct Model {
    base: BTreeMap<i64, String>,
    view: BTreeMap<i64, String>,
    /// Rows a cursor wrote on the branch (the engine's row write set must equal it).
    written: HashSet<i64>,
}

/// Rows the branch changed by content: its view differs from the fork's.
fn changed(m: &Model) -> BTreeSet<i64> {
    m.base
        .keys()
        .chain(m.view.keys())
        .copied()
        .filter(|id| m.base.get(id) != m.view.get(id))
        .collect()
}

struct Live {
    branch: Branch,
    m: Model,
}

/// One statement against `map`'s table on `conn`; the row, if a cursor wrote it.
fn write_one(
    rng: &mut Rng,
    conn: &Arc<Connection>,
    map: &mut BTreeMap<i64, String>,
    gen: &mut u64,
    tag: &str,
) -> Option<i64> {
    *gen += 1;
    let len = 60 + rng.below(160) as usize;
    match rng.below(10) {
        0..=5 => {
            let id = (1 + rng.below(ROWS as u64) as i64) * GAP;
            let v = val(tag, *gen, len);
            conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
                .unwrap();
            map.contains_key(&id).then(|| {
                map.insert(id, v);
                id
            })
        }
        6..=8 => {
            let id = (1 + rng.below(ROWS as u64) as i64) * GAP + 1 + rng.below(GAP as u64 - 1) as i64;
            if map.contains_key(&id) {
                return None;
            }
            let v = val(tag, *gen, len);
            conn.execute(format!("INSERT INTO t VALUES ({id}, '{v}')"))
                .unwrap();
            map.insert(id, v);
            Some(id)
        }
        _ => {
            let keys: Vec<i64> = map.keys().copied().collect();
            let id = keys[rng.below(keys.len() as u64) as usize];
            conn.execute(format!("DELETE FROM t WHERE id = {id}"))
                .unwrap();
            map.remove(&id);
            Some(id)
        }
    }
}

const POLICIES: [MergePolicy; 7] = [
    MergePolicy {
        validation: Validation::Scalar,
        install: Install::Replay,
    },
    MergePolicy {
        validation: Validation::Log,
        install: Install::Replay,
    },
    MergePolicy {
        validation: Validation::PageStamp,
        install: Install::Replay,
    },
    MergePolicy {
        validation: Validation::KeyStamp,
        install: Install::Replay,
    },
    MergePolicy {
        validation: Validation::Scalar,
        install: Install::Physical,
    },
    MergePolicy {
        validation: Validation::Log,
        install: Install::Physical,
    },
    MergePolicy {
        validation: Validation::PageStamp,
        install: Install::Physical,
    },
];

/// The trunk's model state.
struct Trunk {
    rows: BTreeMap<i64, String>,
}

impl Trunk {
    /// A true conflict by content: a row both sides changed since the fork, to different contents.
    fn semantic_conflict(&self, m: &Model) -> bool {
        changed(m).into_iter().any(|id| {
            let ours = self.rows.get(&id);
            ours != m.base.get(&id) && ours != m.view.get(&id)
        })
    }

    /// merge3(base, ours, theirs): theirs where the branch changed a row, ours everywhere else.
    fn merge3(&self, m: &Model) -> BTreeMap<i64, String> {
        let mut out = self.rows.clone();
        for id in changed(m) {
            match m.view.get(&id) {
                Some(v) => {
                    out.insert(id, v.clone());
                }
                None => {
                    out.remove(&id);
                }
            }
        }
        out
    }

    /// A committed merge: the trunk becomes merge3. The next read-back compares the real trunk.
    fn apply(&mut self, m: &Model) {
        self.rows = self.merge3(m);
    }

    /// Check one merge's verdicts, before applying it. `batched`: an earlier member of the same
    /// batch may have written the trunk without a commit yet.
    fn check(&self, seed: u64, o: &MergeOutcome, m: &Model, policy: MergePolicy, batched: bool) {
        if self.semantic_conflict(m) {
            assert!(
                o.refused.is_some(),
                "seed {seed:#x}: a true conflict (both sides changed a row, by content) was merged: \
                 {o:?}"
            );
        }
        assert!(
            !o.key_conflict || o.page_conflict,
            "seed {seed:#x}: a row conflict without a page conflict: {o:?}"
        );
        assert!(
            !o.page_conflict || o.scalar_conflict,
            "seed {seed:#x}: a page conflict without a trunk write since the fork: {o:?}"
        );
        if batched {
            assert!(o.scalar_conflict || o.commits_since_fork == 0, "{o:?}");
        } else {
            assert_eq!(o.scalar_conflict, o.commits_since_fork > 0, "{o:?}");
        }
        if policy.validation == Validation::Log {
            assert_eq!(
                o.log_conflict,
                Some(o.page_conflict),
                "seed {seed:#x}: the log and the page stamps disagree: {o:?}"
            );
        }
        assert_eq!(
            o.rows_written,
            m.written.len(),
            "seed {seed:#x}: the engine's row write set is not the rows the branch wrote: {o:?}"
        );
        let active = match policy.validation {
            Validation::Scalar => o.scalar_conflict.then_some(Refusal::Scalar),
            Validation::Log => o.page_conflict.then_some(Refusal::Log),
            Validation::PageStamp => o.page_conflict.then_some(Refusal::Page),
            Validation::KeyStamp => o.key_conflict.then_some(Refusal::Key),
        };
        let expected =
            active.or((o.structural_conflict == Some(true)).then_some(Refusal::Structural));
        assert_eq!(o.refused, expected, "seed {seed:#x}: {o:?}");
        assert_eq!(o.scope, None, "seed {seed:#x}: {o:?}");
    }
}

#[derive(Default)]
struct Tally {
    committed: HashMap<(Validation, Install), u64>,
    refused: HashMap<Refusal, u64>,
    batch_committed: u64,
    batch_refused: u64,
}

fn run(seed: u64, with_index: bool) -> Tally {
    let (_dir, db) = open_db();
    db.set_branch_read_tracking(true);
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    if with_index {
        trunk.execute("CREATE INDEX tv ON t(v)").unwrap();
    }
    let mut t = Trunk {
        rows: BTreeMap::new(),
    };
    trunk.execute("BEGIN").unwrap();
    for i in 1..=ROWS {
        let v = val("t", i as u64, 100);
        trunk
            .execute(format!("INSERT INTO t VALUES ({}, '{v}')", i * GAP))
            .unwrap();
        t.rows.insert(i * GAP, v);
    }
    trunk.execute("COMMIT").unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let mut rng = Rng(seed);
    let mut live: Vec<Live> = Vec::new();
    let mut gen = 0u64;
    let mut tally = Tally::default();

    for step in 0..1500 {
        match rng.below(20) {
            0..=3 if live.len() < 24 => {
                live.push(Live {
                    branch: trunk.fork_branch().unwrap(),
                    m: Model {
                        base: t.rows.clone(),
                        view: t.rows.clone(),
                        written: HashSet::new(),
                    },
                });
            }
            4..=9 if !live.is_empty() => {
                let i = rng.below(live.len() as u64) as usize;
                let l = &mut live[i];
                let conn = l.branch.connect().unwrap();
                conn.execute("BEGIN").unwrap();
                for _ in 0..=rng.below(3) {
                    if let Some(id) = write_one(&mut rng, &conn, &mut l.m.view, &mut gen, "b") {
                        l.m.written.insert(id);
                    }
                }
                conn.execute("COMMIT").unwrap();
            }
            10..=12 => {
                write_one(&mut rng, &trunk, &mut t.rows, &mut gen, "t");
            }
            13..=16 if !live.is_empty() => {
                let Live { branch, m } = live.swap_remove(rng.below(live.len() as u64) as usize);
                let policy = POLICIES[rng.below(POLICIES.len() as u64) as usize];
                let o = merger.merge(branch, policy).unwrap();
                t.check(seed, &o, &m, policy, false);
                match o.refused {
                    None => {
                        t.apply(&m);
                        *tally
                            .committed
                            .entry((policy.validation, policy.install))
                            .or_default() += 1;
                    }
                    Some(r) => *tally.refused.entry(r).or_default() += 1,
                }
            }
            17..=18 if live.len() >= 2 => {
                // Group commit: two or three branches in one trunk transaction, each validated
                // after the ones before it installed.
                let n = 2 + rng.below(2.min(live.len() as u64 - 1)) as usize;
                let members: Vec<Live> = (0..n)
                    .map(|_| live.swap_remove(rng.below(live.len() as u64) as usize))
                    .collect();
                let policy = [POLICIES[2], POLICIES[3], POLICIES[6]][rng.below(3) as usize];
                let (branches, models): (Vec<Branch>, Vec<Model>) =
                    members.into_iter().map(|l| (l.branch, l.m)).unzip();
                let outcomes = merger.merge_batch(branches, policy).unwrap();
                for (o, m) in outcomes.iter().zip(&models) {
                    t.check(seed, o, m, policy, true);
                    if o.refused.is_none() {
                        t.apply(m);
                        tally.batch_committed += 1;
                    } else {
                        tally.batch_refused += 1;
                    }
                }
            }
            _ => {}
        }
        assert_eq!(
            read_all(&trunk),
            t.rows,
            "seed {seed:#x} step {step}: the trunk disagrees with the model"
        );
        if step % 50 == 0 {
            assert_eq!(integrity(&trunk), ["ok"], "seed {seed:#x} step {step}");
            for l in &live {
                let conn = l.branch.connect().unwrap();
                assert_eq!(
                    read_all(&conn),
                    l.m.view,
                    "seed {seed:#x} step {step}: a live branch lost its snapshot"
                );
            }
        }
    }
    assert_eq!(integrity(&trunk), ["ok"], "seed {seed:#x}");
    drop(live);
    let end = db.branch_stats();
    assert_eq!(end.live_branches, 0, "seed {seed:#x}: branches leaked");
    assert_eq!(end.arena_slots_in_use, 0, "seed {seed:#x}: arena pages leaked");
    tally
}

/// Every policy, alone and in batches, against the model, on a table with and without an index.
/// The shapes the verdicts exist for must all have occurred, or a green run says nothing.
#[test]
fn merges_match_a_model_under_every_policy() {
    let mut all = Tally::default();
    for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
        for with_index in [false, true] {
            let t = run(seed, with_index);
            for (k, v) in t.committed {
                *all.committed.entry(k).or_default() += v;
            }
            for (k, v) in t.refused {
                *all.refused.entry(k).or_default() += v;
            }
            all.batch_committed += t.batch_committed;
            all.batch_refused += t.batch_refused;
        }
    }
    for p in POLICIES {
        assert!(
            all.committed.get(&(p.validation, p.install)).copied().unwrap_or(0) > 0,
            "no merge committed under {p:?}: {:?}",
            all.committed
        );
    }
    for r in [
        Refusal::Scalar,
        Refusal::Log,
        Refusal::Page,
        Refusal::Key,
        Refusal::Structural,
    ] {
        assert!(
            all.refused.get(&r).copied().unwrap_or(0) > 0,
            "no {r:?} refusal occurred: {:?}",
            all.refused
        );
    }
    assert!(all.batch_committed > 0 && all.batch_refused > 0);
}

/// Disjoint rows on one page: the page stamps refuse, the row stamps merge, and the trunk ends with
/// both branches' rows.
#[test]
fn row_granularity_merges_what_page_granularity_refuses() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let (x, y) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
    x.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'x' WHERE id = 1")
        .unwrap();
    y.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'y' WHERE id = 2")
        .unwrap();
    let replay = |validation| MergePolicy {
        validation,
        install: Install::Replay,
    };
    let o = merger.merge(x, replay(Validation::KeyStamp)).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    let o = merger.merge(y, replay(Validation::KeyStamp)).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert!(o.page_conflict && !o.key_conflict, "{o:?}");
    assert_eq!(texts(&trunk, "SELECT v FROM t ORDER BY id"), ["x", "y"]);
}

/// What a merge refuses to take, it refuses as out of scope rather than merging wrongly.
#[test]
fn out_of_scope_merges_are_refused_not_merged() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let key = MergePolicy {
        validation: Validation::KeyStamp,
        install: Install::Replay,
    };
    // Open connection.
    let b = trunk.fork_branch().unwrap();
    let conn = b.connect().unwrap();
    conn.execute("UPDATE t SET v = 'b' WHERE id = 1").unwrap();
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    drop(conn);
    // DDL.
    let b = trunk.fork_branch().unwrap();
    b.connect()
        .unwrap()
        .execute("CREATE TABLE u(x)")
        .unwrap();
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    // A live child.
    let b = trunk.fork_branch().unwrap();
    let child = b.fork().unwrap();
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    drop(child);
    // A write that names no row (what the clear, destroy and blob-write hooks report).
    let b = trunk.fork_branch().unwrap();
    b.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'c' WHERE id = 1")
        .unwrap();
    b.db.branches.branch_bulk_written(b.id);
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    // A trunk DDL since the fork.
    let b = trunk.fork_branch().unwrap();
    b.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'd' WHERE id = 1")
        .unwrap();
    trunk.execute("CREATE TABLE w(x)").unwrap();
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    // A write to an AUTOINCREMENT table also writes sqlite_sequence, which a merge does not take.
    trunk
        .execute("CREATE TABLE ai(id INTEGER PRIMARY KEY AUTOINCREMENT, x TEXT)")
        .unwrap();
    let b = trunk.fork_branch().unwrap();
    b.connect()
        .unwrap()
        .execute("INSERT INTO ai(x) VALUES ('b')")
        .unwrap();
    let o = merger.merge(b, key).unwrap();
    assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
    assert_eq!(cells(&trunk, "SELECT count(*) FROM ai"), ["0"]);
    // Nothing above reached the trunk.
    assert_eq!(texts(&trunk, "SELECT v FROM t"), ["a"]);
    // The physical install without read tracking, and a batch under a validator that cannot see
    // the batch, are refused before anything runs.
    let b = trunk.fork_branch().unwrap();
    assert!(merger
        .merge(
            b,
            MergePolicy {
                validation: Validation::PageStamp,
                install: Install::Physical
            }
        )
        .is_err());
    let (b1, b2) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
    assert!(merger
        .merge_batch(
            vec![b1, b2],
            MergePolicy {
                validation: Validation::Scalar,
                install: Install::Replay
            }
        )
        .is_err());
    assert_eq!(db.branch_stats().live_branches, 0);
}

/// The physical install's structural guard, where it is conservative: a trunk delete next to the
/// branch's row rebalances the leaves around it and writes their parent, which the branch read.
/// `balance_non_root` dirties every sibling it touches, so the branch's own leaf is either written
/// (a page conflict) or untouched, and the guard refuses the untouched cases too. Every committed
/// merge must be correct, and the guard must decide some merges alone here. (With the guard off,
/// `R11_MERGE_MUTANT=5`, exactly these merges commit, and correctly: frontier/round11/r11-merge.)
#[test]
fn the_structural_guard_also_refuses_a_restructure_that_left_the_branch_leaf_alone() {
    let mut guard_alone = 0;
    for offset in (4..=80).step_by(4) {
        for below in [false, true] {
            let (_dir, db) = open_db();
            db.set_branch_read_tracking(true);
            let trunk = db.connect().unwrap();
            trunk
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            trunk.execute("BEGIN").unwrap();
            for id in 1..=1000 {
                trunk
                    .execute(format!("INSERT INTO t VALUES ({id}, '{}')", val("t", id, 100)))
                    .unwrap();
            }
            trunk.execute("COMMIT").unwrap();
            let b = trunk.fork_branch().unwrap();
            b.connect()
                .unwrap()
                .execute(format!("UPDATE t SET v = '{}' WHERE id = 500", val("b", 500, 100)))
                .unwrap();
            let (lo, hi) = if below {
                (500 - offset - 60, 500 - offset)
            } else {
                (500 + offset, 500 + offset + 60)
            };
            trunk
                .execute(format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"))
                .unwrap();
            let mut merger = Merger::new(trunk.clone()).unwrap();
            let o = merger
                .merge(
                    b,
                    MergePolicy {
                        validation: Validation::PageStamp,
                        install: Install::Physical,
                    },
                )
                .unwrap();
            if o.refused.is_none() {
                assert_eq!(
                    texts(&trunk, "SELECT v FROM t WHERE id = 500"),
                    [val("b", 500, 100)],
                    "offset {offset} below {below}: a committed merge lost the update: {o:?}"
                );
            } else if o.refused == Some(Refusal::Structural) && !o.page_conflict {
                guard_alone += 1;
            }
            assert_eq!(integrity(&trunk), ["ok"], "offset {offset} below {below}: {o:?}");
        }
    }
    assert!(guard_alone > 0, "the sweep never restructured above an untouched branch leaf");
}

/// The refusal as its Debug text: the repro tests name the install refusal this way so that they
/// compile against the base, which has no `Refusal::Install` (and fail there, as predicted).
fn refusal(o: &MergeOutcome) -> String {
    format!("{:?}", o.refused)
}

fn key_replay() -> MergePolicy {
    MergePolicy {
        validation: Validation::KeyStamp,
        install: Install::Replay,
    }
}

/// A batch member is validated against the members installed before it in the same transaction:
/// the second of two branches that wrote one row is refused, and the scalar verdict sees the first
/// member's uncommitted write (no trunk commit happened since either fork).
#[test]
fn a_batch_member_sees_the_members_installed_before_it() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let (x, y) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
    x.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'x' WHERE id = 1")
        .unwrap();
    y.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'y' WHERE id = 1")
        .unwrap();
    let outcomes = merger.merge_batch(vec![x, y], key_replay()).unwrap();
    assert_eq!(outcomes[0].refused, None, "{outcomes:?}");
    let o = &outcomes[1];
    assert_eq!(o.refused, Some(Refusal::Key), "{o:?}");
    assert_eq!(o.commits_since_fork, 0, "{o:?}");
    assert!(o.page_conflict && o.scalar_conflict, "{o:?}");
    assert_eq!(texts(&trunk, "SELECT v FROM t WHERE id = 1"), ["x"]);
}

/// A trunk transaction that rolled back wrote nothing, so it refuses nothing: a row is stamped
/// at its transaction's commit, never at the write (PREREG A15). Stamped at the write, the stale
/// stamp refused this merge as a row conflict.
#[test]
fn a_rolled_back_trunk_write_refuses_no_merge() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    let b = trunk.fork_branch().unwrap();
    trunk.execute("BEGIN").unwrap();
    trunk.execute("UPDATE t SET v = 'gone' WHERE id = 1").unwrap();
    trunk.execute("ROLLBACK").unwrap();
    b.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'b1' WHERE id = 1")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert!(!o.key_conflict, "a rolled-back write left a row stamp: {o:?}");
    assert_eq!(texts(&trunk, "SELECT v FROM t WHERE id = 1"), ["b1"]);
}

/// (A15) Row stamps with forks that take no WAL write lock (r11-forklock's F-L): B is forked while
/// a trunk transaction sits between its row write and its commit. The commit retains its pages for
/// B, so B never sees the trunk's value, and the row stamp must say the same, or B's merge installs
/// over a committed update that no validator saw. Stamped at the write (mutant 30), the stamp
/// equals B's fork epoch and this merge commits B's value over the trunk's.
#[test]
fn a_fork_inside_a_trunk_transaction_is_refused_on_the_rows_it_wrote() {
    let (_dir, db) = open_db();
    let writer = db.connect().unwrap();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for id in 1..=40 {
        writer
            .execute(format!("INSERT INTO t VALUES ({id}, 'v{id}')"))
            .unwrap();
    }
    let forker = db.connect().unwrap();
    // The trunk's first child is forked under the WAL write lock; the next one takes none.
    let _first = forker.fork_branch().unwrap();
    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE t SET v = 'trunk' WHERE id = 5")
        .unwrap();
    let b = forker
        .fork_branch()
        .expect("a fork inside a trunk transaction is admitted");
    writer.execute("COMMIT").unwrap();
    let work = db.branch_stats().work;
    assert_eq!(
        work.trunk_forks_fast, 1,
        "premise: B was forked without the WAL write lock: {work:?}"
    );
    let bc = b.connect().unwrap();
    assert_eq!(
        texts(&bc, "SELECT v FROM t WHERE id = 5"),
        ["v5"],
        "premise: B does not see the commit"
    );
    bc.execute("UPDATE t SET v = 'branch' WHERE id = 5").unwrap();
    drop(bc);
    let mut merger = Merger::new(writer.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert!(
        o.page_conflict,
        "control: the page stamp, taken at the commit, sees the commit: {o:?}"
    );
    assert_eq!(refusal(&o), "Some(Key)", "{o:?}");
    assert_eq!(texts(&writer, "SELECT v FROM t WHERE id = 5"), ["trunk"]);
}

/// (a1) A trigger fires when a statement runs, and the rows it wrote are rows too. The branch
/// changes only w; the trunk's trigger on v recorded the trunk's own later update of another row.
/// Applying the branch's row must not fire that trigger again: s.last stays the trunk's 't7'.
#[test]
fn a_trunk_trigger_does_not_fire_when_the_branch_rows_are_applied() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w TEXT)")
        .unwrap();
    trunk
        .execute("CREATE TABLE s(id INTEGER PRIMARY KEY, last TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO s VALUES (1, 'none')").unwrap();
    trunk
        .execute(
            "CREATE TRIGGER tv AFTER UPDATE OF v ON t BEGIN \
             UPDATE s SET last = NEW.v WHERE id = 1; END",
        )
        .unwrap();
    for id in 1..=9 {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, 'v{id}', 'w{id}')"))
            .unwrap();
    }
    let b = trunk.fork_branch().unwrap();
    trunk.execute("UPDATE t SET v = 't7' WHERE id = 7").unwrap();
    assert_eq!(texts(&trunk, "SELECT last FROM s"), ["t7"], "premise: the trigger fired");
    b.connect()
        .unwrap()
        .execute("UPDATE t SET w = 'b5' WHERE id = 5")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert_eq!(
        texts(&trunk, "SELECT last FROM s"),
        ["t7"],
        "applying the branch's row fired the trunk's trigger again"
    );
    assert_eq!(texts(&trunk, "SELECT v || '/' || w FROM t WHERE id = 5"), ["v5/b5"]);
    assert_eq!(texts(&trunk, "SELECT v FROM t WHERE id = 7"), ["t7"]);
}

/// (a2) The branch's own BEFORE INSERT trigger already counted its insert (c.n = 1 in the branch,
/// a row of its write set, written before t's row). Replayed with the trigger live, the counter's
/// image goes in first in either order (the branch's, or the base's root order, since c is
/// created first) and then t's insert fires the trigger again: n = 2. The trunk must end with
/// n = 1: counted once.
#[test]
fn a_branch_trigger_effect_is_applied_once() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE c(id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO c VALUES (1, 0)").unwrap();
    trunk
        .execute(
            "CREATE TRIGGER ti BEFORE INSERT ON t BEGIN \
             UPDATE c SET n = n + 1 WHERE id = 1; END",
        )
        .unwrap();
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("INSERT INTO t VALUES (10, 'x')").unwrap();
    assert_eq!(cells(&bc, "SELECT n FROM c"), ["1"], "premise: the branch's trigger fired");
    drop(bc);
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert_eq!(cells(&trunk, "SELECT n FROM c"), ["1"], "the insert was counted twice");
    assert_eq!(texts(&trunk, "SELECT v FROM t WHERE id = 10"), ["x"]);
}

/// (b) A merger that lives across a trunk ALTER TABLE ADD COLUMN must not write a later branch's
/// rows with statements prepared for the old column list: the branch's w must reach the trunk.
#[test]
fn a_merger_prepares_again_after_a_trunk_alter() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let b0 = trunk.fork_branch().unwrap();
    b0.connect()
        .unwrap()
        .execute("UPDATE t SET v = 'a0' WHERE id = 1")
        .unwrap();
    let o = merger.merge(b0, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    trunk.execute("ALTER TABLE t ADD COLUMN w TEXT").unwrap();
    let b1 = trunk.fork_branch().unwrap();
    b1.connect()
        .unwrap()
        .execute("UPDATE t SET w = 'bw' WHERE id = 2")
        .unwrap();
    let o = merger.merge(b1, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert_eq!(
        cells(&trunk, "SELECT v, w FROM t WHERE id = 2"),
        ["b", "bw"],
        "the branch's write to the new column was dropped"
    );
    assert_eq!(cells(&trunk, "SELECT v, w FROM t WHERE id = 1"), ["a0", "NULL"]);
}

/// (c1) With foreign keys enforced and ON DELETE CASCADE, the trunk adds a child of p1 after the
/// fork and the branch deletes p1. A three-way merge keeps the trunk's child and takes the
/// branch's delete, which leaves the child without a parent, so the merge must be refused, and
/// the trunk must keep both rows. (Re-running the cascade would delete a row only the trunk wrote.)
#[test]
fn a_cascade_does_not_delete_a_trunk_only_child() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA foreign_keys = ON").unwrap();
    trunk
        .execute("CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    trunk
        .execute(
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
        )
        .unwrap();
    trunk
        .execute("INSERT INTO p VALUES (1, 'p1'), (2, 'p2')")
        .unwrap();
    let b = trunk.fork_branch().unwrap();
    trunk.execute("INSERT INTO c VALUES (10, 1)").unwrap();
    b.connect()
        .unwrap()
        .execute("DELETE FROM p WHERE id = 1")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(refusal(&o), "Some(Install)", "{o:?}");
    assert!(format!("{o:?}").contains("lost its parent"), "{o:?}");
    assert_eq!(cells(&trunk, "SELECT id FROM p ORDER BY id"), ["1", "2"]);
    assert_eq!(cells(&trunk, "SELECT id, pid FROM c"), ["10", "1"]);
    assert!(trunk.foreign_keys_enabled(), "the merger must leave the caller's foreign_keys on");
}

/// (c3) The child side: the trunk deletes p2 after the fork (no child held it then), and the branch
/// inserts a child of p2. The rows differ, so validation passes; the three-way result has a child
/// without a parent, so the merge must be refused and the trunk keep neither the child nor p2.
#[test]
fn a_branch_child_of_a_trunk_deleted_parent_is_refused() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA foreign_keys = ON").unwrap();
    trunk
        .execute("CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    trunk
        .execute("CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))")
        .unwrap();
    trunk
        .execute("INSERT INTO p VALUES (1, 'p1'), (2, 'p2')")
        .unwrap();
    let b = trunk.fork_branch().unwrap();
    trunk.execute("DELETE FROM p WHERE id = 2").unwrap();
    b.connect()
        .unwrap()
        .execute("INSERT INTO c VALUES (20, 2)")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(refusal(&o), "Some(Install)", "{o:?}");
    assert!(format!("{o:?}").contains("has no parent after the merge"), "{o:?}");
    assert_eq!(cells(&trunk, "SELECT id FROM p"), ["1"]);
    assert_eq!(cells(&trunk, "SELECT count(*) FROM c"), ["0"]);
}

/// A parent key the branch deletes, held by a child row the trunk has, where the child's value
/// equals the key only as SQLite's foreign-key comparison sees it. The trunk turns foreign keys on
/// after the setup, so the rows went in unchecked; the merge must refuse, as the engine would have
/// refused the delete. `setup` creates p and c and inserts the parent row 1 and the child row 10.
fn parent_delete_is_refused(setup: &[&str]) {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    for sql in setup {
        trunk.execute(*sql).unwrap();
    }
    let b = trunk.fork_branch().unwrap();
    b.connect()
        .unwrap()
        .execute("DELETE FROM p WHERE rowid = 1")
        .unwrap();
    trunk.execute("PRAGMA foreign_keys = ON").unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(refusal(&o), "Some(Install)", "{o:?}");
    assert!(format!("{o:?}").contains("lost its parent"), "{o:?}");
    assert_eq!(cells(&trunk, "SELECT count(*) FROM p WHERE rowid = 1"), ["1"]);
    assert_eq!(cells(&trunk, "SELECT count(*) FROM c"), ["1"]);
}

/// (c4) The parent key's collation decides: 'abc' holds the NOCASE key 'ABC'.
#[test]
fn a_parent_key_is_matched_under_the_parent_collation() {
    parent_delete_is_refused(&[
        "CREATE TABLE p(id INTEGER PRIMARY KEY, k TEXT COLLATE NOCASE UNIQUE)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pk TEXT REFERENCES p(k))",
        "INSERT INTO p VALUES (1, 'ABC'), (2, 'x')",
        "INSERT INTO c VALUES (10, 'abc')",
    ]);
}

/// (c5) The parent key's affinity decides: the text '1' in a column with no type holds the
/// INTEGER key 1.
#[test]
fn a_parent_key_is_matched_under_the_parent_affinity() {
    parent_delete_is_refused(&[
        "CREATE TABLE p(id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid REFERENCES p(id))",
        "INSERT INTO p VALUES (1, 'one'), (2, 'two')",
        "INSERT INTO c VALUES (10, '1')",
    ]);
}

/// (c2) A column declared UNIQUE ON CONFLICT REPLACE: the trunk inserts q with u = 'x' after the
/// fork, and the branch sets u = 'x' on another row. The constraint's REPLACE would delete the
/// trunk's row; the install must refuse instead and leave both rows as they were.
#[test]
fn a_replace_constraint_does_not_delete_a_trunk_only_row() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE ON CONFLICT REPLACE, v TEXT)")
        .unwrap();
    trunk
        .execute("INSERT INTO t VALUES (1, 'a', '1'), (2, 'b', '2')")
        .unwrap();
    let b = trunk.fork_branch().unwrap();
    trunk.execute("INSERT INTO t VALUES (3, 'x', 'trunk')").unwrap();
    b.connect()
        .unwrap()
        .execute("UPDATE t SET u = 'x' WHERE id = 2")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(refusal(&o), "Some(Install)", "{o:?}");
    assert_eq!(
        cells(&trunk, "SELECT id, u FROM t ORDER BY id"),
        ["1", "a", "2", "b", "3", "x"]
    );
}

fn unique_table(trunk: &Arc<Connection>, rows: i64) {
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)")
        .unwrap();
    for id in 1..=rows {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, NULL, 'v{id}')"))
            .unwrap();
    }
    trunk.execute("UPDATE t SET u = 'x' WHERE id = 9").unwrap();
}

/// (d1) The branch moves a UNIQUE value from rowid 9 to rowid 5: first it clears 9, then it sets 5.
/// Replayed in rowid order, 5 would take 'x' while 9 still holds it. In the branch's own order the
/// merge goes through.
#[test]
fn a_unique_move_is_replayed_in_the_branch_order() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    unique_table(&trunk, 12);
    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    bc.execute("UPDATE t SET u = NULL WHERE id = 9").unwrap();
    bc.execute("UPDATE t SET u = 'x' WHERE id = 5").unwrap();
    drop(bc);
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    assert_eq!(cells(&trunk, "SELECT id FROM t WHERE u = 'x'"), ["5"]);
    assert_eq!(cells(&trunk, "SELECT u FROM t WHERE id = 9"), ["NULL"]);
}

/// (d2) Four branches in one batch. The third inserts u = 'z' under a new rowid, as the second did:
/// validation passes (different rows) and the install hits the UNIQUE index. Only that member is
/// refused; the other three, the UNIQUE move among them, commit.
#[test]
fn one_member_refused_at_install_leaves_the_rest_of_the_batch() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    unique_table(&trunk, 20);
    let branches: Vec<Branch> = (0..4).map(|_| trunk.fork_branch().unwrap()).collect();
    let sql = [
        vec![
            "UPDATE t SET u = NULL WHERE id = 9",
            "UPDATE t SET u = 'x' WHERE id = 5",
        ],
        vec!["INSERT INTO t VALUES (31, 'z', 'b2')"],
        vec!["INSERT INTO t VALUES (30, 'z', 'b3')"],
        vec!["UPDATE t SET v = 'b4' WHERE id = 2"],
    ];
    for (b, stmts) in branches.iter().zip(&sql) {
        let conn = b.connect().unwrap();
        for s in stmts {
            conn.execute(*s).unwrap();
        }
    }
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let outcomes = merger.merge_batch(branches, key_replay()).unwrap();
    let refused: Vec<String> = outcomes.iter().map(refusal).collect();
    assert_eq!(
        refused,
        ["None", "None", "Some(Install)", "None"],
        "{outcomes:?}"
    );
    assert!(
        format!("{:?}", outcomes[2]).contains("install_error: Some("),
        "{outcomes:?}"
    );
    assert_eq!(cells(&trunk, "SELECT id FROM t WHERE u = 'x'"), ["5"]);
    assert_eq!(cells(&trunk, "SELECT id FROM t WHERE u = 'z'"), ["31"]);
    assert_eq!(cells(&trunk, "SELECT count(*) FROM t WHERE id = 30"), ["0"]);
    assert_eq!(texts(&trunk, "SELECT v FROM t WHERE id = 2"), ["b4"]);
    assert_eq!(integrity(&trunk), ["ok"]);
}

/// (e) Two branches from one fork each INSERT with no id: both take max(rowid) + 1, so they wrote
/// the same row, and the second merge is refused as a row conflict. This is the stated scope limit
/// (automatic rowids are not partitioned between branches), not a defect this lane fixes.
#[test]
fn automatic_rowids_from_one_fork_collide() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let (b1, b2) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
    b1.connect()
        .unwrap()
        .execute("INSERT INTO t(v) VALUES ('one')")
        .unwrap();
    b2.connect()
        .unwrap()
        .execute("INSERT INTO t(v) VALUES ('two')")
        .unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let o = merger.merge(b1, key_replay()).unwrap();
    assert_eq!(o.refused, None, "{o:?}");
    let o = merger.merge(b2, key_replay()).unwrap();
    assert_eq!(o.refused, Some(Refusal::Key), "{o:?}");
    assert_eq!(cells(&trunk, "SELECT id, v FROM t WHERE id = 4"), ["4", "one"]);
}
