//! r13-compose's registered red tests for the durable Merger and the derived write set (PREREG
//! §2, amendments 2-6). Each test names the amendment item it is the red of, and the
//! `R13_MUTANT=<name>` switch that must turn it red.
//!
//! The derivation's oracle (A6.2) is OWNED BY THE TEST: the SQL each test committed builds model(B),
//! applied by the test to model(base), and a SELECT snapshot of the trunk taken on a TRUNK connection
//! at the fork builds model(base). Neither model is ever read through the branch under test; the
//! store's reads of a branch are compared with model(B) as a separate assertion. Fixture premises
//! (tree depth, freelist length) come from the test's own walk of the trunk's file.

use super::merge::{MergePolicy, Merger, Refusal, Validation};
use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Volatile,
    Durable,
    Catalog,
    CatalogEvict,
}

const MODES: [Mode; 4] = [Mode::Volatile, Mode::Durable, Mode::Catalog, Mode::CatalogEvict];

impl Mode {
    fn opts(self) -> DatabaseOpts {
        let d = match self {
            Mode::Volatile => BranchDurability::Volatile,
            Mode::Durable => BranchDurability::Durable { sync: false },
            Mode::Catalog | Mode::CatalogEvict => BranchDurability::Catalog { sync: false },
        };
        DatabaseOpts::new().with_branch_durability(d)
    }

    fn durable(self) -> bool {
        self != Mode::Volatile
    }
}

fn open(path: &Path, mode: Mode) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        mode.opts(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    if mode == Mode::CatalogEvict {
        db.branch_set_resident_cap(Some(0));
    }
    db
}

/// Evict whatever a cap-0 catalog store can (a checkpoint does it); a no-op elsewhere.
fn settle(db: &Arc<Database>, mode: Mode) {
    if mode.durable() {
        db.branch_compact_now().unwrap();
    }
}

fn policy(validation: Validation) -> MergePolicy {
    MergePolicy {
        validation,
        keep_merged: false,
    }
}

fn root_of(conn: &Arc<Connection>, table: &str) -> i64 {
    let rows = conn
        .prepare(format!(
            "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = '{table}'"
        ))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows[0][0].as_int().unwrap()
}

/// `table`'s rows as (id, rendered row), read through `conn`.
fn snapshot(conn: &Arc<Connection>, table: &str) -> BTreeMap<i64, String> {
    let rows = conn
        .prepare(format!("SELECT * FROM {table} ORDER BY rowid"))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.into_iter()
        .map(|r| (r[0].as_int().unwrap(), format!("{r:?}")))
        .collect()
}

/// The keys whose rows differ between two snapshots (or models) of one table.
fn differing<V: PartialEq>(root: i64, a: &BTreeMap<i64, V>, b: &BTreeMap<i64, V>) -> BTreeSet<(i64, i64)> {
    a.keys()
        .chain(b.keys())
        .filter(|k| a.get(k) != b.get(k))
        .map(|&k| (root, k))
        .collect()
}

/// A deterministic generator.
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

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap();
}

/// Two tables (`a` created first, so `t`'s root is not the first table root, A6.3 (ii)), an index on
/// `t.w`, and enough rows for interior pages at a small page size.
fn seed(conn: &Arc<Connection>, rows: i64) {
    exec(conn, "PRAGMA page_size = 1024");
    exec(conn, "CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)");
    exec(conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INTEGER)");
    exec(conn, "CREATE INDEX t_w ON t(w)");
    exec(conn, "BEGIN");
    for id in 1..=50 {
        exec(conn, &format!("INSERT INTO a VALUES ({id}, 'a{id}')"));
    }
    for id in 1..=rows {
        exec(conn, &format!("INSERT INTO t VALUES ({id}, 'base-{id:05}', {})", id % 97));
    }
    exec(conn, "COMMIT");
}

fn text(n: usize, tag: u64) -> String {
    let unit = format!("{tag:x}");
    unit.repeat(n / unit.len().max(1) + 1)[..n].to_string()
}

/// A row as the tests model it: `v`, and `w` for table t (`None` for table a).
type Row = (String, Option<i64>);

/// `table`'s rows (t or a) read through `conn`, as values, not renderings.
fn rows_of(conn: &Arc<Connection>, table: &str) -> BTreeMap<i64, Row> {
    let sql = if table == "t" {
        "SELECT id, v, w FROM t ORDER BY id"
    } else {
        "SELECT id, v FROM a ORDER BY id"
    };
    conn.prepare(sql)
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|r| {
            let v = match &r[1] {
                crate::Value::Text(t) => t.as_str().to_string(),
                other => panic!("{table}: v is {other:?}"),
            };
            let w = (table == "t").then(|| r[2].as_int().unwrap());
            (r[0].as_int().unwrap(), (v, w))
        })
        .collect()
}

/// One statement of D-T1's branch workload, and what it does to the model: A6.2's model(B) is built
/// from the SQL the test committed, never read through B.
#[derive(Debug, Clone)]
enum Op {
    SetV(i64, String),
    Upsert(i64, String, i64),
    DelRange(i64, i64),
    AddW(i64),
    Revert(i64),
    SetA(i64, String),
    Noop(i64),
}

impl Op {
    fn next(rng: &mut Rng, step: u64) -> Op {
        let id = 1 + rng.below(900) as i64;
        match rng.below(10) {
            0 | 1 => Op::SetV(id, format!("u{step}-{}", text(rng.below(40) as usize, step))),
            2 => Op::Upsert(800 + rng.below(400) as i64, format!("ins{step}"), (step % 97) as i64),
            3 => Op::DelRange(id, id + rng.below(30) as i64),
            4 => Op::SetV(id, text(1500 + rng.below(3000) as usize, step)),
            5 => Op::AddW(id),
            6 => Op::Revert(id),
            7 => Op::SetA(1 + rng.below(50) as i64, format!("b{step}")),
            _ => Op::Noop(id),
        }
    }

    /// The row the statement aims at (the trunk writes next to it, A6.3 (i)).
    fn id(&self) -> i64 {
        match self {
            Op::SetV(id, _) | Op::Upsert(id, _, _) | Op::AddW(id) | Op::Revert(id) | Op::SetA(id, _) | Op::Noop(id) => *id,
            Op::DelRange(lo, _) => *lo,
        }
    }

    fn sql(&self) -> String {
        match self {
            Op::SetV(id, v) => format!("UPDATE t SET v = '{v}' WHERE id = {id}"),
            Op::Upsert(id, v, w) => format!("INSERT OR REPLACE INTO t VALUES ({id}, '{v}', {w})"),
            Op::DelRange(lo, hi) => format!("DELETE FROM t WHERE id BETWEEN {lo} AND {hi}"),
            Op::AddW(id) => format!("UPDATE t SET w = w + 1000 WHERE id = {id}"),
            Op::Revert(id) => format!("UPDATE t SET v = 'base-{id:05}', w = {} WHERE id = {id}", id % 97),
            Op::SetA(id, v) => format!("UPDATE a SET v = '{v}' WHERE id = {id}"),
            Op::Noop(id) => format!("UPDATE t SET v = v WHERE id = {id}"),
        }
    }

    /// What the statement does to the models of t and a.
    fn apply(&self, t: &mut BTreeMap<i64, Row>, a: &mut BTreeMap<i64, Row>) {
        match self {
            Op::SetV(id, v) => {
                if let Some(r) = t.get_mut(id) {
                    r.0 = v.clone();
                }
            }
            Op::Upsert(id, v, w) => {
                t.insert(*id, (v.clone(), Some(*w)));
            }
            Op::DelRange(lo, hi) => t.retain(|k, _| k < lo || k > hi),
            Op::AddW(id) => {
                if let Some(r) = t.get_mut(id) {
                    r.1 = r.1.map(|w| w + 1000);
                }
            }
            Op::Revert(id) => {
                if let Some(r) = t.get_mut(id) {
                    *r = (format!("base-{id:05}"), Some(id % 97));
                }
            }
            Op::SetA(id, v) => {
                if let Some(r) = a.get_mut(id) {
                    r.0 = v.clone();
                }
            }
            Op::Noop(_) => {}
        }
    }
}

/// How a D-T1 run ends: derived live, after a checkpoint and a reopen with a tail, or after a kill at
/// a failpoint (A5.4, A6.2): the statement the failpoint stops is IN DOUBT, so the derived set must
/// be the acknowledged model's diff or that plus the in-doubt statement, and B must read one of the
/// two whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    Live,
    Reopen,
    Kill(BranchFailpoint),
}

/// D-T1 (A5.4 as amended by A6.2/A6.3): the derived write set equals the test-owned content diff,
/// model(B) against model(base), under random workloads with splits, deletes (balances that free
/// pages), overflow payloads, an indexed column and reverts, in two tables, with TRUNK commits after
/// the fork on the leaves the branch owns and a trunk revert to the fork-time value (so a base read
/// from the trunk now is caught), in every mode, live, after a reopen, and across a kill at each
/// failpoint a branch or trunk commit can die at. model(B) is the SQL the test committed (review
/// wf_5c230f31 H2: it was read through B's own view); the store's reads of B are a separate assertion.
/// Mutants that must turn it red: r13_base_trunk_now, r13_first_root, r13_skip_freed_base
/// (r13_leaves_only is equivalent on admitted workloads, amendment 8; its red is
/// `derive_enumerates_a_subtree_the_branch_unlinked_without_writing`).
#[test]
fn a_derived_write_set_equals_the_recorded_one_under_random_workloads() {
    d_t1(open);
}

/// D-T1's body over an opener: the step-9 tree runs it again in the F7 splice arm.
fn d_t1(open: fn(&Path, Mode) -> Arc<Database>) {
    let ends = [
        End::Live,
        End::Reopen,
        End::Kill(BranchFailpoint::CommitAfterSlotsBeforeRecord),
        End::Kill(BranchFailpoint::LogFlushFails),
        End::Kill(BranchFailpoint::BarrierBeforeRecords),
    ];
    for mode in MODES {
        for seed_no in 1..=3u64 {
            for end in ends {
                if end != End::Live && !mode.durable() {
                    continue;
                }
                let what = format!("{mode:?} seed {seed_no} {end:?}");
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("m.db");
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed_no);
                let (b_id, roots, base_t, base_a);
                let (mut ack_t, mut ack_a);
                let mut doubt: Option<Op> = None;
                {
                    let db = open(&path, mode);
                    let trunk = db.connect().unwrap();
                    seed(&trunk, 800);
                    roots = (root_of(&trunk, "t"), root_of(&trunk, "a"));
                    let b = trunk.fork_branch().unwrap();
                    // model(base): the trunk as B forked it, read on a TRUNK connection before any
                    // later trunk commit (A6.2).
                    base_t = rows_of(&trunk, "t");
                    base_a = rows_of(&trunk, "a");
                    ack_t = base_t.clone();
                    ack_a = base_a.clone();
                    let conn = b.connect().unwrap();
                    let kill_at = 25 + 7 * seed_no;
                    let mut trunk_changed: Option<i64> = None;
                    for step in 0..60u64 {
                        let op = Op::next(&mut rng, step);
                        if let End::Kill(fp) = end {
                            if step == kill_at {
                                db.branch_failpoint(Some(fp));
                                if fp == BranchFailpoint::BarrierBeforeRecords {
                                    // A trunk commit dies at its barrier; B's model is unchanged.
                                    let _ = trunk.execute("UPDATE t SET v = 'killed' WHERE id = 1");
                                } else if conn.execute(op.sql()).is_ok() {
                                    // Nothing to commit reached the failpoint: acknowledged.
                                    op.apply(&mut ack_t, &mut ack_a);
                                } else {
                                    doubt = Some(op);
                                }
                                db.branch_failpoint(None);
                                break;
                            }
                        }
                        exec(&conn, &op.sql());
                        op.apply(&mut ack_t, &mut ack_a);
                        // Trunk commits after the fork, on rows next to the branch's (A6.3 (i)), and
                        // a revert of the row the last round changed to its fork-time value.
                        if step % 7 == 0 {
                            let x = (op.id() + 1).min(800);
                            exec(&trunk, &format!("UPDATE t SET v = 'trunk{step}' WHERE id = {x}"));
                            exec(&trunk, &format!("INSERT OR REPLACE INTO t VALUES ({}, 'tins', 1)", 2000 + step));
                            exec(&trunk, &format!("DELETE FROM t WHERE id = {}", (op.id() + 2).min(800)));
                            if let Some(y) = trunk_changed.take() {
                                if let Some((v, w)) = base_t.get(&y) {
                                    exec(&trunk, &format!("UPDATE t SET v = '{v}', w = {} WHERE id = {y}", w.unwrap()));
                                }
                            }
                            trunk_changed = Some(x);
                        }
                    }
                    drop(conn);
                    b_id = b.into_id();
                    match end {
                        End::Live => {
                            settle(&db, mode);
                            let b = db.branch(b_id).unwrap();
                            reads_are(&b, &[(&ack_t, &ack_a)], &what);
                            check_derived(&db, &trunk, &b, roots, (&base_t, &base_a), &[(&ack_t, &ack_a)], &what);
                            let _ = b.into_id();
                            continue;
                        }
                        End::Reopen => {
                            // Rows both checkpointed and in the log's tail.
                            settle(&db, mode);
                            let op = Op::next(&mut rng, 99);
                            let b = db.branch(b_id).unwrap();
                            exec(&b.connect().unwrap(), &op.sql());
                            op.apply(&mut ack_t, &mut ack_a);
                            let _ = b.into_id();
                        }
                        End::Kill(_) => {}
                    }
                }
                let db = open(&path, mode);
                let trunk = db.connect().unwrap();
                let b = db.branch(b_id).unwrap();
                let (mut op_t, mut op_a) = (ack_t.clone(), ack_a.clone());
                if let Some(op) = &doubt {
                    op.apply(&mut op_t, &mut op_a);
                }
                let models: Vec<(&BTreeMap<i64, Row>, &BTreeMap<i64, Row>)> = if doubt.is_some() {
                    vec![(&ack_t, &ack_a), (&op_t, &op_a)]
                } else {
                    vec![(&ack_t, &ack_a)]
                };
                reads_are(&b, &models, &what);
                settle(&db, mode);
                check_derived(&db, &trunk, &b, roots, (&base_t, &base_a), &models, &what);
                let _ = b.into_id();
            }
        }
    }
}

/// The store's reads of B equal one of the models, whole (A6.2's second assertion).
fn reads_are(b: &Branch, models: &[(&BTreeMap<i64, Row>, &BTreeMap<i64, Row>)], what: &str) {
    let conn = b.connect().unwrap();
    let (t, a) = (rows_of(&conn, "t"), rows_of(&conn, "a"));
    assert!(
        models.iter().any(|(mt, ma)| **mt == t && **ma == a),
        "{what}: B reads neither model ({} rows of t; first model {} rows)",
        t.len(),
        models[0].0.len()
    );
}

/// The derived set equals the content diff of base against one of the models (one model unless a
/// statement is in doubt).
fn check_derived(
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    b: &Branch,
    roots: (i64, i64),
    base: (&BTreeMap<i64, Row>, &BTreeMap<i64, Row>),
    models: &[(&BTreeMap<i64, Row>, &BTreeMap<i64, Row>)],
    what: &str,
) {
    let wants: Vec<BTreeSet<(i64, i64)>> = models
        .iter()
        .map(|(mt, ma)| {
            let mut want = differing(roots.0, base.0, mt);
            want.extend(differing(roots.1, base.1, ma));
            want
        })
        .collect();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(b).unwrap();
    assert_eq!(got.scope, None, "{what}: refused");
    let got: BTreeSet<(i64, i64)> = got.keys.into_iter().collect();
    assert!(
        wants.iter().any(|want| *want == got),
        "{what}: derived {} keys; the model's diffs have {:?} keys; first model only {:?}, derived only {:?} ({:?})",
        got.len(),
        wants.iter().map(BTreeSet::len).collect::<Vec<_>>(),
        wants[0].difference(&got).take(5).collect::<Vec<_>>(),
        got.difference(&wants[0]).take(5).collect::<Vec<_>>(),
        db.branch_merge_work()
    );
    assert!(!wants[0].is_empty(), "{what}: a workload that changed nothing tests nothing");
}

/// Checkpoint the sharp way.
fn sharp(db: &Arc<Database>) {
    db.branch_compact_now().unwrap();
}

/// Checkpoint the fuzzy way (F-FZ), and wait for its install.
fn fuzzy(db: &Arc<Database>) {
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no fuzzy flight started");
    db.branch_checkpoint_wait();
}

/// A sharp checkpoint whose write fails before its catalog commit (A5.4's failed-write order).
fn fail_write(db: &Arc<Database>) {
    db.branch_failpoint(Some(BranchFailpoint::CheckpointWriteFails));
    assert!(db.branch_compact_now().is_err(), "the checkpoint-write failpoint did not fire");
}

/// A merge of `b_id` commits and installs both of D-T2's rows.
fn merge_installs_both(db: &Arc<Database>, trunk: &Arc<Connection>, b_id: BranchId, what: &str) {
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, None, "{what}: {out:?}");
    assert_eq!(out.rows_changed, 2, "{what}: {out:?}");
    let rows = snapshot(trunk, "t");
    assert!(rows[&10].contains("first") && rows[&250].contains("second"), "{what}");
}

/// D-T2 (A5.4): a merge after an eviction, after a reopen with rows both checkpointed and in the
/// log's tail, during a fuzzy checkpoint's flight (held before and after its catalog commit), and
/// after a checkpoint whose write failed (then merged at once, after an evicting checkpoint, and
/// after a reopen), installs every row the branch changed. The eviction and restart orders run with
/// sharp and with fuzzy checkpoints. The four adversary rounds' loss orders, end to end.
#[test]
fn a_merge_after_eviction_restart_and_fuzzy_capture_installs_every_row() {
    for (mode, case) in [
        (Mode::CatalogEvict, "evict-then-update"),
        (Mode::CatalogEvict, "evict-then-update-fuzzy"),
        (Mode::Catalog, "restart-with-tail"),
        (Mode::Catalog, "restart-with-tail-fuzzy"),
        (Mode::Catalog, "fuzzy-before-commit"),
        (Mode::Catalog, "fuzzy-after-commit"),
        (Mode::Catalog, "failed-write"),
        (Mode::CatalogEvict, "failed-write-then-evict"),
        (Mode::Catalog, "failed-write-restart"),
    ] {
        let what = case.to_string();
        let ck: fn(&Arc<Database>) = if case.ends_with("-fuzzy") { fuzzy } else { sharp };
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("m.db");
        let db = open(&path, mode);
        let trunk = db.connect().unwrap();
        seed(&trunk, 300);
        let b = trunk.fork_branch().unwrap();
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'first' WHERE id = 10");
        let b_id = b.into_id();
        ck(&db);
        let b = db.branch(b_id).unwrap();
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'second' WHERE id = 250");
        let _ = b.into_id();
        let (db, trunk) = match case {
            "evict-then-update" | "evict-then-update-fuzzy" => {
                ck(&db);
                (db, trunk)
            }
            "restart-with-tail" | "restart-with-tail-fuzzy" | "failed-write-restart" => {
                if case == "failed-write-restart" {
                    fail_write(&db);
                }
                drop(trunk);
                drop(db);
                let db = open(&path, mode);
                let trunk = db.connect().unwrap();
                (db, trunk)
            }
            "failed-write" => {
                fail_write(&db);
                (db, trunk)
            }
            "failed-write-then-evict" => {
                fail_write(&db);
                sharp(&db);
                (db, trunk)
            }
            _ => {
                let stage = if case == "fuzzy-before-commit" {
                    store::HOLD_BEFORE_COMMIT
                } else {
                    store::HOLD_AFTER_COMMIT
                };
                db.branch_checkpoint_hold(stage);
                assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "{what}: no flight");
                let t = std::time::Instant::now();
                while db.branch_checkpoint_held() != stage | store::HOLD_ARRIVED {
                    assert!(t.elapsed() < std::time::Duration::from_secs(10), "{what}: never held");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let mut merger = Merger::new(trunk.clone()).unwrap();
                let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
                db.branch_checkpoint_hold(0);
                db.branch_checkpoint_wait();
                assert_eq!(out.refused, None, "{what}: {out:?}");
                assert_eq!(out.rows_changed, 2, "{what}: {out:?}");
                let rows = snapshot(&trunk, "t");
                assert!(rows[&10].contains("first") && rows[&250].contains("second"), "{what}");
                continue;
            }
        };
        merge_installs_both(&db, &trunk, b_id, &what);
    }
}

/// A6.4: content semantics' accept path. B writes a row and writes it back (and runs a no-op
/// UPDATE), the trunk changes that row after the fork, and the merge commits with the trunk keeping
/// its change. Twins: a delete then an identical re-insert, and an indexed column changed then
/// reverted.
#[test]
fn a_branch_write_equal_to_base_does_not_conflict_and_trunk_keeps_its_change() {
    for (case, sqls) in [
        ("revert", vec!["UPDATE t SET v = 'x' WHERE id = 5", "UPDATE t SET v = 'base-00005' WHERE id = 5", "UPDATE t SET v = v WHERE id = 5"]),
        ("delete-then-identical-reinsert", vec!["DELETE FROM t WHERE id = 5", "INSERT INTO t VALUES (5, 'base-00005', 5)"]),
        ("indexed-column-updated-then-reverted", vec!["UPDATE t SET w = 999 WHERE id = 5", "UPDATE t SET w = 5 WHERE id = 5"]),
    ] {
        for mode in [Mode::Catalog, Mode::CatalogEvict] {
            let what = format!("{case} {mode:?}");
            let dir = tempfile::TempDir::new().unwrap();
            let db = open(&dir.path().join("m.db"), mode);
            let trunk = db.connect().unwrap();
            seed(&trunk, 200);
            let b = trunk.fork_branch().unwrap();
            {
                let conn = b.connect().unwrap();
                for sql in &sqls {
                    exec(&conn, sql);
                }
                // One real change, so the merge has something to install.
                exec(&conn, "UPDATE t SET v = 'real' WHERE id = 150");
            }
            let b_id = b.into_id();
            exec(&trunk, "UPDATE t SET v = 'trunk-change' WHERE id = 5");
            settle(&db, mode);
            let mut merger = Merger::new(trunk.clone()).unwrap();
            let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
            assert_eq!(out.refused, None, "{what}: {out:?}");
            assert_eq!(out.rows_changed, 1, "{what}: only row 150 changed: {out:?}");
            let rows = snapshot(&trunk, "t");
            assert!(rows[&5].contains("trunk-change"), "{what}: the trunk lost its change");
            assert!(rows[&150].contains("real"), "{what}: the branch's change is missing");
        }
    }
}

/// A6.4 (ii): MV4 refuses whenever ours differs from base, even where theirs equals ours (both made
/// the same change); counted as refusals_same_change.
#[test]
fn the_same_change_on_both_sides_is_refused_and_counted() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 100);
    let b = trunk.fork_branch().unwrap();
    exec(&b.connect().unwrap(), "UPDATE t SET v = 'same' WHERE id = 7");
    let b_id = b.into_id();
    exec(&trunk, "UPDATE t SET v = 'same' WHERE id = 7");
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Base), "{out:?}");
    let w1 = db.branch_merge_work();
    assert_eq!(w1.refusals_same_change - w0.refusals_same_change, 1, "{w1:?}");
}

/// D-T5 and A6.4 (i): one derivation refusal, by case: DDL, a clear, a DML delete of every row (the
/// pages cannot tell it from a clear), and an incremental blob write on a row whose leaf the branch
/// did not write. Each is refused as Scope and counted under its own reason.
fn derivation_refusal(case: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 300);
    exec(&trunk, "CREATE TABLE big(id INTEGER PRIMARY KEY, b BLOB)");
    exec(&trunk, "INSERT INTO big VALUES (1, zeroblob(6000))");
    let b = trunk.fork_branch().unwrap();
    {
        let conn = b.connect().unwrap();
        match case {
            "ddl" => exec(&conn, "CREATE TABLE extra(x)"),
            "clear" => exec(&conn, "DELETE FROM a"),
            "delete-all" => exec(&conn, "DELETE FROM a WHERE id > 0"),
            _ => {
                let mut blob = conn.blob_open("big", "b", 1, true).unwrap();
                blob.write(5000, b"xyz").unwrap();
            }
        }
    }
    let b_id = b.into_id();
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Scope), "{case}: {out:?}");
    let w1 = db.branch_merge_work();
    let counted = match case {
        "ddl" => w1.derive_refusals_ddl - w0.derive_refusals_ddl,
        "clear" | "delete-all" => w1.derive_refusals_clear_or_delete_all - w0.derive_refusals_clear_or_delete_all,
        _ => w1.derive_refusals_unattributed - w0.derive_refusals_unattributed,
    };
    assert_eq!(counted, 1, "{case}: refused for another reason: {out:?} {w1:?}");
}

/// D-T5 (review wf_5c230f31: one test per registered red, so each mutant's red is its own).
#[test]
fn a_ddl_branch_is_refused() {
    derivation_refusal("ddl");
}

/// D-T5.
#[test]
fn a_clear_is_refused() {
    derivation_refusal("clear");
}

/// A6.4 (i): a DML delete of every row is refused as a clear is.
#[test]
fn a_delete_of_every_row_is_refused() {
    derivation_refusal("delete-all");
}

/// D-T5.
#[test]
fn an_incremental_blob_write_on_an_unowned_leaf_is_refused() {
    derivation_refusal("blob");
}

/// D-T5: an index-kind write when the schema has a WITHOUT ROWID table is refused.
#[test]
fn a_without_rowid_write_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    exec(&trunk, "CREATE TABLE wr(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID");
    exec(&trunk, "INSERT INTO wr VALUES ('a', '1')");
    let b = trunk.fork_branch().unwrap();
    exec(&b.connect().unwrap(), "INSERT INTO wr VALUES ('b', '2')");
    let b_id = b.into_id();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Scope), "{out:?}");
    assert!(db.branch_merge_work().derive_refusals_without_rowid >= 1);
}

/// Amendment 8 (review wf_5c230f31 M, algorithm D as built): an index write of a ROWID table is
/// refused too when the schema has any WITHOUT ROWID table, though the branch never touched it: the
/// derivation does not attribute index pages, so it cannot tell whose index page it owns. Wider than
/// A5.2 registered (a refusal, never a lost row); G1's schema has no WITHOUT ROWID table.
#[test]
fn an_index_write_beside_an_unrelated_without_rowid_table_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    exec(&trunk, "CREATE TABLE wr(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID");
    exec(&trunk, "INSERT INTO wr VALUES ('a', '1')");
    let b = trunk.fork_branch().unwrap();
    // An indexed column of the ROWID table t: the branch writes a t_w index page, and nothing of wr.
    exec(&b.connect().unwrap(), "UPDATE t SET w = 500 WHERE id = 9");
    let b_id = b.into_id();
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Scope), "{out:?}");
    let w1 = db.branch_merge_work();
    assert_eq!(w1.derive_refusals_without_rowid - w0.derive_refusals_without_rowid, 1, "{w1:?}");
}

/// A6.3: a DELETE that makes a balance free sibling leaves: the derived set holds every deleted row.
/// Mutant r13_skip_freed_base must turn it red (the freed owned pages' base rows go missing).
/// Amendment 8: r13_leaves_only is EQUIVALENT here (balance dirties every sibling before it frees
/// any, so every freed page is owned, and no unowned subtree is ever dropped on an admitted
/// workload); its red is `derive_enumerates_a_subtree_the_branch_unlinked_without_writing`.
#[test]
fn a_delete_that_makes_a_balance_free_a_sibling_derives_every_deleted_row() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 2000);
    let root = root_of(&trunk, "t");
    let b = trunk.fork_branch().unwrap();
    let base = snapshot(&trunk, "t");
    let conn = b.connect().unwrap();
    // Wide deletes: whole leaves empty out and are freed, siblings rebalance.
    exec(&conn, "DELETE FROM t WHERE id BETWEEN 400 AND 900");
    exec(&conn, "DELETE FROM t WHERE id % 3 = 0 AND id BETWEEN 1200 AND 1600");
    drop(conn);
    // model(B): the SQL above applied to model(base) by the test (A6.2), never read through B.
    let mut model = base.clone();
    model.retain(|&k, _| !(400..=900).contains(&k) && !(k % 3 == 0 && (1200..=1600).contains(&k)));
    let b_id = b.into_id();
    db.branch_compact_now().unwrap();
    let want = differing(root, &base, &model);
    assert!(want.len() > 600);
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(&db.branch(b_id).unwrap()).unwrap();
    assert_eq!(got.scope, None);
    let got: BTreeSet<(i64, i64)> = got.keys.into_iter().collect();
    assert_eq!(got, want, "deleted rows missed or extra: {:?}", db.branch_merge_work());
    let w = db.branch_merge_work();
    // The premise r13_skip_freed_base needs: the branch's own freelist was walked (its freed pages).
    assert!(w.derive_freelist_reads > 0, "no freed page was exercised: {w:?}");
}

/// The trunk's database file, read by the TEST (a premise the test owns; never the store under
/// test): after a WAL checkpoint, page n is at (n - 1) x the page size.
struct DbFile {
    bytes: Vec<u8>,
    ps: usize,
}

impl DbFile {
    fn read(trunk: &Arc<Connection>, path: &Path) -> DbFile {
        exec(trunk, "PRAGMA wal_checkpoint(TRUNCATE)");
        let bytes = std::fs::read(path).unwrap();
        let ps = u16::from_be_bytes([bytes[16], bytes[17]]) as usize;
        DbFile {
            bytes,
            ps: if ps == 1 { 65536 } else { ps },
        }
    }

    fn page(&self, n: u32) -> &[u8] {
        let at = (n as usize - 1) * self.ps;
        &self.bytes[at..at + self.ps]
    }

    /// A table interior page's (left child, key) cells and its rightmost child; `None` otherwise.
    fn interior(&self, n: u32) -> Option<(Vec<(u32, i64)>, u32)> {
        let (p, h) = (self.page(n), if n == 1 { 100 } else { 0 });
        if p[h] != 0x05 {
            return None;
        }
        let count = u16::from_be_bytes([p[h + 3], p[h + 4]]) as usize;
        let right = u32::from_be_bytes(p[h + 8..h + 12].try_into().unwrap());
        let cells = (0..count)
            .map(|i| {
                let at = u16::from_be_bytes([p[h + 12 + 2 * i], p[h + 13 + 2 * i]]) as usize;
                let left = u32::from_be_bytes(p[at..at + 4].try_into().unwrap());
                (left, varint(&p[at + 4..]) as i64)
            })
            .collect();
        Some((cells, right))
    }

    /// Levels from `root` down its leftmost path to a leaf.
    fn depth(&self, root: u32) -> u64 {
        let (mut d, mut n) = (1, root);
        while let Some((cells, right)) = self.interior(n) {
            n = cells.first().map_or(right, |c| c.0);
            d += 1;
        }
        d
    }

    /// The trunk pages of the freelist chain page 1's header starts.
    fn freelist_trunks(&self) -> u64 {
        let mut n = u32::from_be_bytes(self.bytes[32..36].try_into().unwrap());
        let mut k = 0;
        while n != 0 {
            k += 1;
            assert!(k < 1 << 20, "a freelist chain that does not end");
            n = u32::from_be_bytes(self.page(n)[0..4].try_into().unwrap());
        }
        k
    }
}

/// A SQLite varint's value.
fn varint(b: &[u8]) -> u64 {
    let mut v = 0u64;
    for (i, &x) in b.iter().enumerate().take(9) {
        if i == 8 {
            return (v << 8) | x as u64;
        }
        v = (v << 7) | (x & 0x7f) as u64;
        if x & 0x80 == 0 {
            return v;
        }
    }
    v
}

/// D-T4 (A6.3): an interior balance that relinks subtrees the branch did not write derives no change
/// for their rows; the cancellation fired, nothing was enumerated, and derive_pages_read stays under
/// A6.5's KNOWN bound for the case. The fixture is three levels deep (asserted by the test's own walk
/// of the trunk file), and the branch deletes ~75% of the rows under ONE level-2 interior page, so
/// that page underflows and an interior balance moves its siblings' children (review wf_5c230f31 M:
/// the old 6,000-row fixture could not reach an interior balance). Mutant r13_no_relink_cancel must
/// turn it red (its output is the same; only the counters tell).
#[test]
fn an_interior_balance_that_relinks_a_subtree_derives_no_change_for_its_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let db = open(&path, Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 20_000);
    let root = root_of(&trunk, "t");
    let file = DbFile::read(&trunk, &path);
    let depth = file.depth(root as u32);
    assert!(depth >= 3, "the fixture is {depth} levels deep, not 3");
    let (cells, _) = file.interior(root as u32).expect("t's root is an interior page");
    assert!(cells.len() >= 2, "the root has {} cells", cells.len());
    let (p, lo, hi) = (cells[1].0, cells[0].1, cells[1].1);
    let (under, _) = file.interior(p).expect("the root's second child is a level-2 interior page");
    assert!(under.len() >= 8, "the level-2 page has only {} children", under.len());
    let base = snapshot(&trunk, "t");
    let b = trunk.fork_branch().unwrap();
    exec(&b.connect().unwrap(), &format!("DELETE FROM t WHERE id > {lo} AND id <= {hi} AND id % 4 != 0"));
    let b_id = b.into_id();
    // model(B): the SQL above applied to model(base) (A6.2).
    let mut model = base.clone();
    model.retain(|&k, _| !(k > lo && k <= hi && k % 4 != 0));
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(&db.branch(b_id).unwrap()).unwrap();
    let owned = got.owned;
    let got: BTreeSet<(i64, i64)> = got.keys.into_iter().collect();
    assert_eq!(got, differing(root, &base, &model));
    let w1 = db.branch_merge_work();
    assert!(w1.derive_subtrees_cancelled > w0.derive_subtrees_cancelled, "no relink was cancelled: {w1:?}");
    assert_eq!(w1.derive_subtrees_enumerated, w0.derive_subtrees_enumerated, "a relink was enumerated: {w1:?}");
    // A6.5's KNOWN bound with no dropped subtree and no overflow: 2|O| + 2 R depth |O| + (owned
    // freelist prefix + 1), R = the table b-trees attribution descends (sqlite_schema, a, t).
    let r_kind = 3;
    let freelist = w1.derive_freelist_reads - w0.derive_freelist_reads;
    let bound = 2 * owned + 2 * r_kind * depth * owned + freelist + 1;
    let read = w1.derive_pages_read - w0.derive_pages_read;
    assert!(read <= bound, "derive_pages_read {read} > A6.5's bound {bound} (|O| {owned}, depth {depth})");
}

/// A6.5: the freelist walk reads only the branch's owned prefix of the chain, however long the
/// trunk's freelist is. The trunk's chain is at least 3 trunk pages (asserted by the test's own walk
/// of the trunk file; review wf_5c230f31 M: a 1-trunk chain let the mutant pass). Mutant
/// r13_freelist_whole must turn it red.
#[test]
fn a_branch_over_a_long_trunk_freelist_reads_only_its_owned_prefix() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let db = open(&path, Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 30_000);
    // A long trunk freelist.
    exec(&trunk, "DELETE FROM t WHERE id > 200");
    let trunks = DbFile::read(&trunk, &path).freelist_trunks();
    assert!(trunks >= 3, "the trunk's freelist has {trunks} trunk pages, not 3");
    let b = trunk.fork_branch().unwrap();
    exec(&b.connect().unwrap(), "UPDATE t SET v = 'x' WHERE id = 3");
    let b_id = b.into_id();
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(&db.branch(b_id).unwrap()).unwrap();
    assert_eq!(got.keys.len(), 1);
    let w1 = db.branch_merge_work();
    // The branch owns no freelist trunk page: at most the first, non-owned one is looked at.
    assert!(w1.derive_freelist_reads - w0.derive_freelist_reads <= 1, "read the trunk's freelist: {w1:?}");
}

/// A3.F9's corrected order (A2.R1): fork C1, checkpoint (C1 leaves the in-memory child index),
/// trunk write k on a row C1 also writes, fork C2, one trunk commit (which prunes), then merge C1
/// under KeyStamp: Conflict. Mutant r13_prune_map_only (oldest live child from the map alone) must
/// turn it red.
#[test]
fn a_keystamp_merge_refuses_a_write_stamped_before_a_younger_fork_once_the_older_child_left_the_map() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 100);
    let c1 = trunk.fork_branch().unwrap();
    exec(&c1.connect().unwrap(), "UPDATE t SET v = 'c1' WHERE id = 9");
    let c1_id = c1.into_id();
    db.branch_compact_now().unwrap();
    exec(&trunk, "UPDATE t SET v = 'k' WHERE id = 9");
    let c2 = trunk.fork_branch().unwrap();
    exec(&trunk, "UPDATE t SET v = 'later' WHERE id = 50");
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(c1_id).unwrap(), policy(Validation::KeyStamp)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Key), "{out:?}");
    assert_eq!(out.decided_by, Validation::KeyStamp);
    drop(c2);
}

/// D-M5's horizon: a conflict the trunk wrote before a restart is refused under KeyStamp, because a
/// branch forked before the open is decided by the base read. Mutant r13_no_horizon must turn it
/// red.
#[test]
fn a_pre_restart_conflict_is_refused_under_keystamp() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let b_id;
    {
        let db = open(&path, Mode::Catalog);
        let trunk = db.connect().unwrap();
        seed(&trunk, 100);
        let b = trunk.fork_branch().unwrap();
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'b' WHERE id = 4");
        b_id = b.into_id();
        exec(&trunk, "UPDATE t SET v = 'trunk' WHERE id = 4");
    }
    let db = open(&path, Mode::Catalog);
    let trunk = db.connect().unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::KeyStamp)).unwrap();
    assert_eq!(out.refused, Some(Refusal::Base), "{out:?}");
    assert_eq!(out.decided_by, Validation::BaseRead);
    assert!(db.branch_merge_work().v3_horizon_fallbacks >= 1);
}

/// D-M6: a second merge of a kept merged branch is refused (its rows were written by the trunk
/// after its fork).
#[test]
fn a_second_merge_of_a_kept_ref_is_refused() {
    for validation in [Validation::BaseRead, Validation::KeyStamp] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open(&dir.path().join("m.db"), Mode::Catalog);
        let trunk = db.connect().unwrap();
        seed(&trunk, 100);
        let b = trunk.fork_branch().unwrap();
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'pr' WHERE id = 6");
        let b_id = b.into_id();
        let keep = MergePolicy {
            validation,
            keep_merged: true,
        };
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let out = merger.merge(db.branch(b_id).unwrap(), keep).unwrap();
        assert_eq!(out.refused, None, "{validation:?}: first merge {out:?}");
        let again = merger.merge(db.branch(b_id).unwrap(), keep).unwrap();
        assert!(again.refused.is_some(), "{validation:?}: a second merge went in: {again:?}");
        assert_eq!(snapshot(&trunk, "t")[&6].contains("pr"), true);
    }
}

/// A6.1 (splice-off part; the splice-on arm is added with F7's merge): land a stack top over cap-0
/// evictions, a reopen, and a released middle level: every level's rows go in. The lower half of
/// the stack is checkpointed; the upper half's commits are only in the log's tail when the process
/// ends without a checkpoint (review wf_5c230f31 M: "a kill and reopen", so the replay parks Commits
/// on stack levels). Mutant r13_owned_current_only must turn it red.
#[test]
fn a_land_top_installs_every_levels_rows_across_evict_restart_and_a_released_middle_level() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let (top_id, d) = (std::cell::Cell::new(BranchId(0)), 12i64);
    {
        let db = open(&path, Mode::CatalogEvict);
        let trunk = db.connect().unwrap();
        seed(&trunk, 400);
        let mut level = trunk.fork_branch().unwrap();
        let mut _held = Vec::new();
        for i in 1..=d {
            exec(&level.connect().unwrap(), &format!("UPDATE t SET v = 'level{i}' WHERE id = {}", i * 20));
            let next = level.fork().unwrap();
            if i == d / 2 {
                // A released middle level: retired and kept while its child lives (splice off).
                drop(level);
                db.branch_compact_now().unwrap();
            } else {
                _held.push(level.into_id());
            }
            level = next;
        }
        exec(&level.connect().unwrap(), "UPDATE t SET v = 'top' WHERE id = 399");
        top_id.set(level.into_id());
        // No checkpoint: the process ends with the upper levels' commits in the tail.
    }
    let db = open(&path, Mode::CatalogEvict);
    let trunk = db.connect().unwrap();
    db.branch_compact_now().unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger
        .land_top(db.branch(top_id.get()).unwrap(), policy(Validation::BaseRead))
        .unwrap();
    assert_eq!(out.refused, None, "{out:?}");
    assert_eq!(out.rows_changed as i64, d + 1, "{out:?}");
    let rows = snapshot(&trunk, "t");
    for i in 1..=d {
        assert!(rows[&(i * 20)].contains(&format!("level{i}")), "level {i}'s row is missing");
    }
    assert!(rows[&399].contains("top"));
}

/// D-M6 (§2): a crash after a merge's trunk COMMIT and before its release neither loses nor
/// re-applies the merge. A keep-merged merge leaves exactly that durable state (the trunk holds the
/// rows; the branch is live), the process ends without a checkpoint, and after the reopen a second
/// merge of the branch is refused under both validators (MV4: ours differs from base; KeyStamp: the
/// stamps died with the process, so the restart horizon hands the verdict to MV4, I7), and the trunk
/// is unchanged.
#[test]
fn a_crash_between_a_merges_commit_and_its_release_neither_loses_nor_reapplies_it() {
    for validation in [Validation::BaseRead, Validation::KeyStamp] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("m.db");
        let b_id;
        {
            let db = open(&path, Mode::Catalog);
            let trunk = db.connect().unwrap();
            seed(&trunk, 100);
            let b = trunk.fork_branch().unwrap();
            exec(&b.connect().unwrap(), "UPDATE t SET v = 'once' WHERE id = 8");
            b_id = b.into_id();
            let keep = MergePolicy {
                validation,
                keep_merged: true,
            };
            let out = Merger::new(trunk.clone()).unwrap().merge(db.branch(b_id).unwrap(), keep).unwrap();
            assert_eq!(out.refused, None, "{validation:?}: {out:?}");
        }
        let db = open(&path, Mode::Catalog);
        let trunk = db.connect().unwrap();
        let before = snapshot(&trunk, "t");
        assert!(before[&8].contains("once"), "{validation:?}: the committed merge was lost");
        let w0 = db.branch_merge_work();
        let out = Merger::new(trunk.clone())
            .unwrap()
            .merge(db.branch(b_id).unwrap(), policy(validation))
            .unwrap();
        assert!(out.refused.is_some(), "{validation:?}: the merge went in twice: {out:?}");
        assert_eq!(snapshot(&trunk, "t"), before, "{validation:?}: the trunk changed");
        if validation == Validation::KeyStamp {
            let w1 = db.branch_merge_work();
            assert!(w1.v3_horizon_fallbacks > w0.v3_horizon_fallbacks, "no horizon fallback: {w1:?}");
        }
    }
}

/// Review wf_5c230f31 (A31, amendment 8): while an old child lives, one key written at k trunk
/// epochs holds k stamp entries (A31's term) and one distinct key (`stamps_held`).
#[test]
fn one_key_written_at_k_epochs_under_an_old_child_holds_k_stamp_entries() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 50);
    let old = trunk.fork_branch().unwrap().into_id();
    let w0 = db.branch_merge_work();
    let k = 12u64;
    let mut kids = Vec::new();
    for i in 0..k {
        // Each fork moves the trunk's epoch; the write after it is stamped at the new one.
        kids.push(trunk.fork_branch().unwrap().into_id());
        exec(&trunk, &format!("UPDATE t SET v = 'e{i}' WHERE id = 5"));
    }
    let w1 = db.branch_merge_work();
    assert_eq!(w1.stamp_entries - w0.stamp_entries, k, "{w1:?}");
    assert_eq!(w1.stamps_held - w0.stamps_held, 1, "{w1:?}");
    let _ = (old, kids);
}

/// A version of the database made of synthetic pages (the derivation's unit tests).
struct Pages(HashMap<u32, Arc<Vec<u8>>>);

impl super::derive::PageSource for Pages {
    fn read(&mut self, page: u32) -> crate::Result<Option<Arc<Vec<u8>>>> {
        Ok(self.0.get(&page).cloned())
    }
}

/// A 1 KiB table leaf page holding `rows` (rowids and payload lengths below 128: one-byte varints).
fn leaf_page(rows: &[(i64, &[u8])]) -> Arc<Vec<u8>> {
    let mut p = vec![0u8; 1024];
    p[0] = 0x0D;
    let mut end = p.len();
    for (i, (rowid, payload)) in rows.iter().enumerate() {
        let mut cell = vec![payload.len() as u8, *rowid as u8];
        cell.extend_from_slice(payload);
        end -= cell.len();
        p[end..end + cell.len()].copy_from_slice(&cell);
        p[8 + 2 * i..10 + 2 * i].copy_from_slice(&(end as u16).to_be_bytes());
    }
    p[3..5].copy_from_slice(&(rows.len() as u16).to_be_bytes());
    p[5..7].copy_from_slice(&(end as u16).to_be_bytes());
    Arc::new(p)
}

/// A 1 KiB table interior page: (left child, key) cells and the rightmost child.
fn interior_page(cells: &[(u32, i64)], right: u32) -> Arc<Vec<u8>> {
    let mut p = vec![0u8; 1024];
    p[0] = 0x05;
    let mut end = p.len();
    for (i, (left, key)) in cells.iter().enumerate() {
        let mut cell = left.to_be_bytes().to_vec();
        cell.push(*key as u8);
        end -= cell.len();
        p[end..end + cell.len()].copy_from_slice(&cell);
        p[12 + 2 * i..14 + 2 * i].copy_from_slice(&(end as u16).to_be_bytes());
    }
    p[3..5].copy_from_slice(&(cells.len() as u16).to_be_bytes());
    p[5..7].copy_from_slice(&(end as u16).to_be_bytes());
    p[8..12].copy_from_slice(&right.to_be_bytes());
    Arc::new(p)
}

/// Amendment 8 (review wf_5c230f31 M): r13_leaves_only's red. No admitted SQL workload drops a
/// subtree the branch did not write (balance dirties every sibling first), so the derivation is run
/// on synthetic pages: the base's root links leaves 3, 4 and 5; the branch rewrote only the root,
/// which no longer links leaf 4. Leaf 4's rows are deleted on the branch, found by enumerating the
/// unlinked base subtree. Mutant r13_leaves_only (no link accounting) derives nothing and must turn
/// it red.
#[test]
fn derive_enumerates_a_subtree_the_branch_unlinked_without_writing() {
    let leaves = [
        (3u32, leaf_page(&[(1, b"r1"), (2, b"r2"), (3, b"r3")])),
        (4, leaf_page(&[(4, b"r4"), (5, b"r5"), (6, b"r6")])),
        (5, leaf_page(&[(7, b"r7"), (8, b"r8"), (9, b"r9")])),
    ];
    let mut base = Pages(leaves.iter().cloned().collect());
    base.0.insert(2, interior_page(&[(3, 3), (4, 6)], 5));
    let mut theirs = Pages(leaves.iter().cloned().collect());
    theirs.0.insert(2, interior_page(&[(3, 3)], 5));
    let trees = [super::derive::Tree {
        root: 2,
        table: true,
        without_rowid: false,
    }];
    let d = super::derive::derive(&mut theirs, &mut base, &[2], &trees, 1024).unwrap();
    assert_eq!(d.refusal, None);
    let keys: Vec<(u32, i64)> = d.changes.keys().copied().collect();
    assert_eq!(keys, vec![(2, 4), (2, 5), (2, 6)], "{:?}", d.counters);
    assert!(d.changes.values().all(|c| c.theirs.is_none() && c.base.is_some()));
    assert_eq!(d.counters.subtrees_enumerated, 1, "{:?}", d.counters);
}

/// D-M10 (§2): b161e861d's model test, restricted to the ported validators {KeyStamp, MV4} x Replay,
/// in Catalog and CatalogEvict, single members (batches are not ported, D-M8). Up to 24 live
/// branches write, the trunk writes, and branches merge under a random validator. A merge is refused
/// exactly when the model says: MV4 when the trunk's row now differs from the base on a row the
/// branch changed (content), KeyStamp when the trunk wrote such a row after the fork (a merge's
/// install is a trunk write). After each step the trunk equals the model; every 50 steps each live
/// branch reads its own view. Each validator must both commit and refuse at least once.
#[test]
fn merges_match_a_model_under_every_policy() {
    let mut tally: BTreeMap<(String, bool), u64> = BTreeMap::new();
    for mode in [Mode::Catalog, Mode::CatalogEvict] {
        for seed in [0x9E37_79B9_7F4A_7C15u64, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
            for with_index in [false, true] {
                model_run(mode, seed, with_index, &mut tally);
            }
        }
    }
    for v in ["BaseRead", "KeyStamp"] {
        for refused in [false, true] {
            let n = tally.get(&(v.to_string(), refused)).copied().unwrap_or(0);
            assert!(n > 0, "no {v} merge with refused={refused}: {tally:?}");
        }
    }
}

const GAP: i64 = 4;
const MODEL_ROWS: i64 = 200;

/// One live branch of the model test: its handle, the trunk it forked from (base), its own view,
/// and the keys the trunk wrote since its fork.
struct LiveB {
    branch: Branch,
    base: BTreeMap<i64, String>,
    view: BTreeMap<i64, String>,
    trunk_wrote: BTreeSet<i64>,
}

fn read_all(conn: &Arc<Connection>) -> BTreeMap<i64, String> {
    conn.prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|r| match &r[1] {
            crate::Value::Text(t) => (r[0].as_int().unwrap(), t.as_str().to_string()),
            other => panic!("v is {other:?}"),
        })
        .collect()
}

/// One write through `conn`, mirrored in `map` (b161e861d's write_one): an UPDATE of a key, an
/// INSERT of a key between the seeded ones, or a DELETE. Returns the key the SQL wrote, if any.
fn write_one(rng: &mut Rng, conn: &Arc<Connection>, map: &mut BTreeMap<i64, String>, gen: &mut u64, tag: &str) -> Option<i64> {
    *gen += 1;
    let v = format!("{tag}{:08}{}", *gen, "x".repeat(10 + rng.below(50) as usize));
    match rng.below(10) {
        0..=5 => {
            let id = (1 + rng.below(MODEL_ROWS as u64) as i64) * GAP;
            exec(conn, &format!("UPDATE t SET v = '{v}' WHERE id = {id}"));
            map.contains_key(&id).then(|| {
                map.insert(id, v);
                id
            })
        }
        6..=8 => {
            let id = (1 + rng.below(MODEL_ROWS as u64) as i64) * GAP + 1 + rng.below(GAP as u64 - 1) as i64;
            if map.contains_key(&id) {
                return None;
            }
            exec(conn, &format!("INSERT INTO t VALUES ({id}, '{v}')"));
            map.insert(id, v);
            Some(id)
        }
        _ => {
            let keys: Vec<i64> = map.keys().copied().collect();
            let id = keys[rng.below(keys.len() as u64) as usize];
            exec(conn, &format!("DELETE FROM t WHERE id = {id}"));
            map.remove(&id);
            Some(id)
        }
    }
}

fn model_run(mode: Mode, seed: u64, with_index: bool, tally: &mut BTreeMap<(String, bool), u64>) {
    let what = format!("{mode:?} seed {seed:#x} index {with_index}");
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), mode);
    let trunk = db.connect().unwrap();
    exec(&trunk, "PRAGMA page_size = 1024");
    exec(&trunk, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    if with_index {
        exec(&trunk, "CREATE INDEX tv ON t(v)");
    }
    let mut rows = BTreeMap::new();
    exec(&trunk, "BEGIN");
    for i in 1..=MODEL_ROWS {
        let v = format!("t-{i}");
        exec(&trunk, &format!("INSERT INTO t VALUES ({}, '{v}')", i * GAP));
        rows.insert(i * GAP, v);
    }
    exec(&trunk, "COMMIT");
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let mut rng = Rng(seed);
    let mut live: Vec<LiveB> = Vec::new();
    let mut gen = 0u64;
    for step in 0..600 {
        match rng.below(20) {
            0..=3 if live.len() < 24 => live.push(LiveB {
                branch: trunk.fork_branch().unwrap(),
                base: rows.clone(),
                view: rows.clone(),
                trunk_wrote: BTreeSet::new(),
            }),
            4..=9 if !live.is_empty() => {
                let i = rng.below(live.len() as u64) as usize;
                let l = &mut live[i];
                let conn = l.branch.connect().unwrap();
                exec(&conn, "BEGIN");
                for _ in 0..=rng.below(3) {
                    write_one(&mut rng, &conn, &mut l.view, &mut gen, "b");
                }
                exec(&conn, "COMMIT");
            }
            10..=12 => {
                if let Some(id) = write_one(&mut rng, &trunk, &mut rows, &mut gen, "t") {
                    for l in &mut live {
                        l.trunk_wrote.insert(id);
                    }
                }
            }
            13..=16 if !live.is_empty() => {
                let LiveB { branch, base, view, trunk_wrote } = live.swap_remove(rng.below(live.len() as u64) as usize);
                let validation = if rng.below(2) == 0 { Validation::BaseRead } else { Validation::KeyStamp };
                let changed: BTreeSet<i64> =
                    base.keys().chain(view.keys()).filter(|k| base.get(k) != view.get(k)).copied().collect();
                let want_refused = changed.iter().any(|k| match validation {
                    Validation::BaseRead => rows.get(k) != base.get(k),
                    Validation::KeyStamp => trunk_wrote.contains(k),
                });
                let o = merger.merge(branch, policy(validation)).unwrap();
                assert_eq!(o.scope, None, "{what} step {step}: {o:?}");
                assert_eq!(o.rows_changed, changed.len(), "{what} step {step}: {o:?}");
                assert_eq!(o.refused.is_some(), want_refused, "{what} step {step}: {validation:?} {o:?}");
                if o.refused.is_none() {
                    for k in &changed {
                        match view.get(k) {
                            Some(v) => rows.insert(*k, v.clone()),
                            None => rows.remove(k),
                        };
                    }
                    for l in &mut live {
                        l.trunk_wrote.extend(changed.iter().copied());
                    }
                }
                *tally.entry((format!("{validation:?}"), o.refused.is_some())).or_default() += 1;
            }
            _ => {}
        }
        if mode == Mode::CatalogEvict && step % 60 == 0 {
            db.branch_compact_now().unwrap();
        }
        assert_eq!(read_all(&trunk), rows, "{what} step {step}: the trunk disagrees with the model");
        if step % 50 == 0 {
            for l in &live {
                let conn = l.branch.connect().unwrap();
                assert_eq!(read_all(&conn), l.view, "{what} step {step}: a live branch lost its snapshot");
            }
        }
    }
}

// ---- r13-compose step 9: the reds that need F7's splice arm (A3.F16: their red evidence is a mutant
// on the step-9 tree) ----

fn spliced_opts(mode: Mode) -> DatabaseOpts {
    mode.opts().with_branch_splice(true)
}

fn open_spliced(path: &Path, mode: Mode) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        spliced_opts(mode),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    if mode == Mode::CatalogEvict {
        db.branch_set_resident_cap(Some(0));
    }
    db
}

/// D-T3 (A5.4, A6.6): a spliced child merges the zombie's rows, in the splice-on arm, on the
/// in-memory spliced child and after splice, checkpoint, cap-0 eviction and a reopen. The zombie's
/// write after the child's fork is not the child's (retire_current freed it) and is not merged.
/// Mutant r13_splice_no_stream (the splice does not stream the zombie's versions into the child's
/// current) must turn it red.
#[test]
fn a_spliced_child_merges_the_zombies_rows() {
    for reload in [false, true] {
        let what = format!("reload {reload}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("m.db");
        let c_id;
        {
            let db = open_spliced(&path, Mode::CatalogEvict);
            let trunk = db.connect().unwrap();
            seed(&trunk, 300);
            let z = trunk.fork_branch().unwrap();
            exec(&z.connect().unwrap(), "UPDATE t SET v = 'zombie-before' WHERE id = 30");
            let c = z.fork().unwrap();
            exec(&z.connect().unwrap(), "UPDATE t SET v = 'zombie-after' WHERE id = 31");
            exec(&c.connect().unwrap(), "UPDATE t SET v = 'child' WHERE id = 200");
            c_id = c.into_id();
            let r = z.reap().unwrap();
            assert!(r.deferred, "{what}: the zombie had a live child: {r:?}");
            if !reload {
                let mut merger = Merger::new(trunk.clone()).unwrap();
                let out = merger.merge(db.branch(c_id).unwrap(), policy(Validation::BaseRead)).unwrap();
                assert_eq!(out.refused, None, "{what}: {out:?}");
                assert_eq!(out.rows_changed, 2, "{what}: the zombie's row and the child's: {out:?}");
                let rows = snapshot(&trunk, "t");
                assert!(rows[&30].contains("zombie-before"), "{what}: the zombie's row is missing");
                assert!(rows[&200].contains("child"), "{what}");
                assert!(rows[&31].contains("base-00031"), "{what}: the zombie's post-fork write leaked");
                continue;
            }
            db.branch_compact_now().unwrap();
        }
        let db = open_spliced(&path, Mode::CatalogEvict);
        let trunk = db.connect().unwrap();
        db.branch_compact_now().unwrap();
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let out = merger.merge(db.branch(c_id).unwrap(), policy(Validation::BaseRead)).unwrap();
        assert_eq!(out.refused, None, "{what}: {out:?}");
        assert_eq!(out.rows_changed, 2, "{what}: {out:?}");
        let rows = snapshot(&trunk, "t");
        assert!(rows[&30].contains("zombie-before") && rows[&200].contains("child"), "{what}");
        assert!(rows[&31].contains("base-00031"), "{what}");
    }
}

/// A6.1's splice-on arm: land a stack top whose middle level was released and spliced into its
/// child; every level's rows go in, across a cap-0 eviction and a reopen.
#[test]
fn a_land_top_installs_every_levels_rows_in_the_splice_arm() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let d = 12i64;
    let top;
    {
        let db = open_spliced(&path, Mode::CatalogEvict);
        let trunk = db.connect().unwrap();
        seed(&trunk, 400);
        let mut level = trunk.fork_branch().unwrap();
        let mut _held = Vec::new();
        for i in 1..=d {
            exec(&level.connect().unwrap(), &format!("UPDATE t SET v = 'level{i}' WHERE id = {}", i * 20));
            let next = level.fork().unwrap();
            if i == d / 2 {
                drop(level); // released with one live child: spliced into it
            } else {
                _held.push(level.into_id());
            }
            level = next;
        }
        exec(&level.connect().unwrap(), "UPDATE t SET v = 'top' WHERE id = 399");
        top = level.into_id();
        db.branch_compact_now().unwrap();
    }
    let db = open_spliced(&path, Mode::CatalogEvict);
    let trunk = db.connect().unwrap();
    db.branch_compact_now().unwrap();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let out = merger.land_top(db.branch(top).unwrap(), policy(Validation::BaseRead)).unwrap();
    assert_eq!(out.refused, None, "{out:?}");
    assert_eq!(out.rows_changed as i64, d + 1, "{out:?}");
    let rows = snapshot(&trunk, "t");
    for i in 1..=d {
        assert!(rows[&(i * 20)].contains(&format!("level{i}")), "level {i}'s row is missing");
    }
}

/// A4.G's G-a: the public fuzzy checkpoint on a splice-arm catalog store returns G-a's own error,
/// and G-b's counter does not move (so G-b cannot mask G-a's mutant r13_guard_a_debug, run in a
/// release test build).
#[test]
fn branch_checkpoint_fuzzy_now_on_a_splice_arm_store_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_spliced(&dir.path().join("m.db"), Mode::Catalog);
    seed(&db.connect().unwrap(), 50);
    let before = db.branches.fuzzy_refused_splice();
    let err = db.branch_checkpoint_fuzzy_now().expect_err("a splice-arm store started a fuzzy checkpoint");
    assert!(err.to_string().contains("fuzzy checkpoint refused: splice arm"), "{err}");
    assert_eq!(db.branches.fuzzy_refused_splice(), before, "G-b fired: G-a did not stop it first");
}

/// A4.G's G-b: `start_flight` called directly refuses a splice-arm store, counts it, and enters no
/// capture (so G-c cannot mask G-b's mutant r13_guard_b_debug, run in a release test build).
#[test]
fn start_flight_refuses_a_splice_arm_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_spliced(&dir.path().join("m.db"), Mode::Catalog);
    seed(&db.connect().unwrap(), 50);
    let before = db.branches.fuzzy_refused_splice();
    let entered = store::CAPTURE_ENTERED.with(|c| c.get());
    assert!(!db.branches.start_flight_for_test(), "a flight started on a splice-arm store");
    assert_eq!(db.branches.fuzzy_refused_splice(), before + 1, "G-b did not count its refusal");
    assert_eq!(store::CAPTURE_ENTERED.with(|c| c.get()), entered, "the capture was entered");
}

/// A4.G's G-c: a fuzzy capture of a splice-arm store returns Err (in a release test build too: its
/// mutant r13_guard_c_debug must turn this red there).
#[test]
fn a_fuzzy_capture_of_a_splice_arm_store_returns_err() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_spliced(&dir.path().join("m.db"), Mode::Catalog);
    seed(&db.connect().unwrap(), 50);
    let err = db.branches.capture_fuzzy_for_test().expect_err("a fuzzy capture of a splice-arm store");
    assert!(err.to_string().contains("fuzzy capture refused: splice arm"), "{err}");
}

/// A1.1 (S-3 widened): a splice-arm catalog store reopens after a checkpoint, and after crash state
/// S1 (the catalog committed, the log not yet cut: recovery rewrites the log at open). Its log's
/// header must keep the splice arm's format. Mutant r13_log_header_fixed must turn it red.
#[test]
fn a_splice_arm_catalog_store_reopens_after_a_checkpoint_and_after_crash_state_s1() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("m.db");
    let b_id;
    {
        let db = open_spliced(&path, Mode::Catalog);
        let trunk = db.connect().unwrap();
        seed(&trunk, 100);
        let b = trunk.fork_branch().unwrap();
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'one' WHERE id = 3");
        b_id = b.into_id();
        db.branch_compact_now().unwrap();
    }
    {
        let db = open_spliced(&path, Mode::Catalog);
        let b = db.branch(b_id).unwrap();
        assert!(snapshot(&b.connect().unwrap(), "t")[&3].contains("one"), "after a checkpoint");
        exec(&b.connect().unwrap(), "UPDATE t SET v = 'two' WHERE id = 4");
        let _ = b.into_id();
        // Crash state S1: the catalog commits, the log is not cut.
        db.branches
            .set_failpoint(Some(BranchFailpoint::CompactAfterRenameBeforeLogReset));
        assert!(db.branch_compact_now().is_err(), "the failpoint did not fire");
    }
    for round in 0..2 {
        let db = open_spliced(&path, Mode::Catalog);
        let b = db.branch(b_id).unwrap();
        let rows = snapshot(&b.connect().unwrap(), "t");
        assert!(rows[&3].contains("one") && rows[&4].contains("two"), "round {round} after S1");
        let _ = b.into_id();
        db.branch_compact_now().unwrap();
    }
}

/// D-T1 in the F7 splice arm (A5.4: "in both splice arms"; review wf_5c230f31 M): the same
/// workloads, ends and kills, on stores opened with the splice arm on.
#[test]
fn a_derived_write_set_equals_the_recorded_one_in_the_splice_arm() {
    d_t1(open_spliced);
}
