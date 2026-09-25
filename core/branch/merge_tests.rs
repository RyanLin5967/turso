//! Merges against a model the test keeps itself.
//!
//! The model is a map per branch and one for the trunk, and a per-row "last trunk write" time. It
//! never asks the engine what a merge should do: it decides from its own history whether a merge
//! conflicts at row level, applies a committed merge's rows itself, and then reads the trunk and
//! every live branch back through SQL. Page-granular verdicts cannot be modelled without the
//! engine's page layout, so they are checked by implication (a row conflict writes the row's page,
//! so it is a page conflict; a page conflict is a commit since the fork) and by soundness: after
//! every step the trunk must equal the model and pass `PRAGMA integrity_check`, which a physical
//! install that copied a page the trunk had changed or restructured would break.

use std::collections::{BTreeMap, HashMap, HashSet};

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

fn integrity(conn: &Arc<Connection>) -> Vec<String> {
    texts(conn, "PRAGMA integrity_check")
}

/// What the model knows about a branch.
struct Model {
    view: BTreeMap<i64, String>,
    written: HashSet<i64>,
    /// Model trunk time at the fork.
    forked_at: u64,
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
    /// Advances at every trunk row write; `stamp` is each row's last.
    time: u64,
    stamp: HashMap<i64, u64>,
}

impl Trunk {
    fn key_conflict(&self, m: &Model) -> bool {
        m.written
            .iter()
            .any(|r| self.stamp.get(r).is_some_and(|&s| s > m.forked_at))
    }

    /// A committed merge: every row the branch wrote takes the branch's value, and is stamped when
    /// a trunk cursor wrote it (the row exists on one side or the other). The physical install
    /// stamps every row the branch wrote, having no cursor to say which exist.
    fn apply(&mut self, m: &Model, install: Install) {
        let mut ids: Vec<i64> = m.written.iter().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let had = self.rows.contains_key(&id);
            match m.view.get(&id) {
                Some(v) => self.rows.insert(id, v.clone()),
                None => self.rows.remove(&id),
            };
            if had || m.view.contains_key(&id) || install == Install::Physical {
                self.time += 1;
                self.stamp.insert(id, self.time);
            }
        }
    }

    /// Check one merge's verdicts against the model, before applying it.
    fn check(&self, seed: u64, o: &MergeOutcome, m: &Model, policy: MergePolicy) {
        assert_eq!(
            o.key_conflict,
            self.key_conflict(m),
            "seed {seed:#x}: the row verdict disagrees with the model: {o:?}"
        );
        assert!(
            !o.key_conflict || o.page_conflict,
            "seed {seed:#x}: a row conflict without a page conflict: {o:?}"
        );
        assert!(
            !o.page_conflict || o.scalar_conflict,
            "seed {seed:#x}: a page conflict without a commit since the fork: {o:?}"
        );
        assert_eq!(o.scalar_conflict, o.commits_since_fork > 0, "{o:?}");
        if policy.validation == Validation::Log {
            assert_eq!(
                o.log_conflict,
                Some(o.page_conflict),
                "seed {seed:#x}: the log and the page stamps disagree: {o:?}"
            );
        }
        assert_eq!(o.rows_written, m.written.len(), "seed {seed:#x}: {o:?}");
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
        time: 0,
        stamp: HashMap::new(),
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
                        view: t.rows.clone(),
                        written: HashSet::new(),
                        forked_at: t.time,
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
                if let Some(id) = write_one(&mut rng, &trunk, &mut t.rows, &mut gen, "t") {
                    t.time += 1;
                    t.stamp.insert(id, t.time);
                }
            }
            13..=16 if !live.is_empty() => {
                let Live { branch, m } = live.swap_remove(rng.below(live.len() as u64) as usize);
                let policy = POLICIES[rng.below(POLICIES.len() as u64) as usize];
                let o = merger.merge(branch, policy).unwrap();
                t.check(seed, &o, &m, policy);
                match o.refused {
                    None => {
                        t.apply(&m, policy.install);
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
                    t.check(seed, o, m, policy);
                    if o.refused.is_none() {
                        t.apply(m, policy.install);
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
