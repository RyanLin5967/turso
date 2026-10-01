//! r13-compose's registered red tests for the durable Merger and the derived write set (PREREG
//! §2, amendments 2-6). Each test names the amendment item it is the red of, and the
//! `R13_MUTANT=<name>` switch that must turn it red.
//!
//! The derivation's oracle (A6.2) is OWNED BY THE TEST: the SQL each test committed builds model(B),
//! and a SELECT snapshot of the trunk taken at the fork builds model(base). Neither is ever read
//! through the branch store under test.

use super::merge::{MergePolicy, Merger, Refusal, Validation};
use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::collections::{BTreeMap, BTreeSet};
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

/// The keys whose rendered rows differ between two snapshots of one table.
fn differing(root: i64, a: &BTreeMap<i64, String>, b: &BTreeMap<i64, String>) -> BTreeSet<(i64, i64)> {
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

/// D-T1 (A5.4 as amended by A6.2/A6.3): the derived write set equals the test-owned content diff,
/// model(B) against model(base), under random workloads with splits, deletes (balances that free
/// pages), overflow payloads, an indexed column and reverts, in two tables, with TRUNK commits after
/// the fork on the leaves the branch owns (so a base read from the trunk now is caught), in every
/// mode, live and after a reopen. It also asserts that the store's reads of B equal model(B).
/// Mutants that must turn it red: r13_base_trunk_now, r13_first_root, r13_leaves_only,
/// r13_skip_freed_base.
#[test]
fn a_derived_write_set_equals_the_recorded_one_under_random_workloads() {
    for mode in MODES {
        for seed_no in 1..=3u64 {
            for reopen in [false, true] {
                if reopen && !mode.durable() {
                    continue;
                }
                let what = format!("{mode:?} seed {seed_no} reopen {reopen}");
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("m.db");
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed_no);
                let (b_id, roots, base_t, base_a, mut model_t, mut model_a);
                {
                    let db = open(&path, mode);
                    let trunk = db.connect().unwrap();
                    seed(&trunk, 800);
                    roots = (root_of(&trunk, "t"), root_of(&trunk, "a"));
                    let b = trunk.fork_branch().unwrap();
                    base_t = snapshot(&trunk, "t");
                    base_a = snapshot(&trunk, "a");
                    let conn = b.connect().unwrap();
                    for step in 0..60u64 {
                        let id = 1 + rng.below(900) as i64;
                        match rng.below(10) {
                            0 | 1 => exec(&conn, &format!("UPDATE t SET v = 'u{step}-{}' WHERE id = {id}", text(rng.below(40) as usize, step))),
                            2 => exec(&conn, &format!("INSERT OR REPLACE INTO t VALUES ({}, 'ins{step}', {})", 800 + rng.below(400), step % 97)),
                            3 => exec(&conn, &format!("DELETE FROM t WHERE id BETWEEN {id} AND {}", id + rng.below(30) as i64)),
                            4 => exec(&conn, &format!("UPDATE t SET v = '{}' WHERE id = {id}", text(1500 + rng.below(3000) as usize, step))),
                            5 => exec(&conn, &format!("UPDATE t SET w = w + 1000 WHERE id = {id}")),
                            6 => exec(&conn, &format!("UPDATE t SET v = 'base-{id:05}', w = {} WHERE id = {id}", id % 97)),
                            7 => exec(&conn, &format!("UPDATE a SET v = 'b{step}' WHERE id = {}", 1 + rng.below(50))),
                            _ => exec(&conn, &format!("UPDATE t SET v = v WHERE id = {id}")),
                        }
                        // Trunk commits after the fork, on rows next to the branch's (A6.3 (i)).
                        if step % 7 == 0 {
                            exec(&trunk, &format!("UPDATE t SET v = 'trunk{step}' WHERE id = {}", (id + 1).min(800)));
                            exec(&trunk, &format!("INSERT OR REPLACE INTO t VALUES ({}, 'tins', 1)", 2000 + step));
                            exec(&trunk, &format!("DELETE FROM t WHERE id = {}", (id + 2).min(800)));
                        }
                    }
                    model_t = snapshot(&conn, "t");
                    model_a = snapshot(&conn, "a");
                    drop(conn);
                    b_id = b.into_id();
                    settle(&db, mode);
                    if !reopen {
                        let b = db.branch(b_id).unwrap();
                        check_derived(&db, &trunk, &b, roots, (&base_t, &base_a), (&model_t, &model_a), &what);
                        let _ = b.into_id();
                        continue;
                    }
                }
                let db = open(&path, mode);
                let trunk = db.connect().unwrap();
                let b = db.branch(b_id).unwrap();
                // The store's reads of B after the reopen are model(B) (A6.2).
                {
                    let conn = b.connect().unwrap();
                    assert_eq!(snapshot(&conn, "t"), model_t, "{what}: B reads t after the reopen");
                    assert_eq!(snapshot(&conn, "a"), model_a, "{what}: B reads a after the reopen");
                    model_t = snapshot(&conn, "t");
                    model_a = snapshot(&conn, "a");
                }
                settle(&db, mode);
                check_derived(&db, &trunk, &b, roots, (&base_t, &base_a), (&model_t, &model_a), &what);
                let _ = b.into_id();
            }
        }
    }
}

fn check_derived(
    db: &Arc<Database>,
    trunk: &Arc<Connection>,
    b: &Branch,
    roots: (i64, i64),
    base: (&BTreeMap<i64, String>, &BTreeMap<i64, String>),
    model: (&BTreeMap<i64, String>, &BTreeMap<i64, String>),
    what: &str,
) {
    let mut want = differing(roots.0, base.0, model.0);
    want.extend(differing(roots.1, base.1, model.1));
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(b).unwrap();
    assert_eq!(got.scope, None, "{what}: refused");
    let got: BTreeSet<(i64, i64)> = got.keys.into_iter().collect();
    assert_eq!(
        got.symmetric_difference(&want).copied().collect::<Vec<_>>(),
        Vec::<(i64, i64)>::new(),
        "{what}: derived {} keys, the model {} ({:?})",
        got.len(),
        want.len(),
        db.branch_merge_work()
    );
    assert!(!want.is_empty(), "{what}: a workload that changed nothing tests nothing");
}

/// D-T2 (A5.4): a merge after an eviction, after a reopen with rows both checkpointed and in the
/// log's tail, and during a fuzzy checkpoint's flight (held before and after its catalog commit),
/// installs every row the branch changed. The four adversary rounds' loss orders, end to end.
#[test]
fn a_merge_after_eviction_restart_and_fuzzy_capture_installs_every_row() {
    for (mode, case) in [
        (Mode::CatalogEvict, "evict-then-update"),
        (Mode::Catalog, "restart-with-tail"),
        (Mode::Catalog, "fuzzy-before-commit"),
        (Mode::Catalog, "fuzzy-after-commit"),
    ] {
        let what = format!("{case}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("m.db");
        let b_id;
        {
            let db = open(&path, mode);
            let trunk = db.connect().unwrap();
            seed(&trunk, 300);
            let b = trunk.fork_branch().unwrap();
            exec(&b.connect().unwrap(), "UPDATE t SET v = 'first' WHERE id = 10");
            b_id = b.into_id();
            db.branch_compact_now().unwrap();
            let b = db.branch(b_id).unwrap();
            exec(&b.connect().unwrap(), "UPDATE t SET v = 'second' WHERE id = 250");
            let _ = b.into_id();
            match case {
                "evict-then-update" => db.branch_compact_now().unwrap(),
                "restart-with-tail" => {}
                _ => {}
            }
            if case.starts_with("fuzzy") {
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
            if case == "restart-with-tail" {
                drop(trunk);
                drop(db);
                let db = open(&path, mode);
                let trunk = db.connect().unwrap();
                let mut merger = Merger::new(trunk.clone()).unwrap();
                let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
                assert_eq!(out.refused, None, "{what}: {out:?}");
                assert_eq!(out.rows_changed, 2, "{what}: {out:?}");
                let rows = snapshot(&trunk, "t");
                assert!(rows[&10].contains("first") && rows[&250].contains("second"), "{what}");
                continue;
            }
            let mut merger = Merger::new(trunk.clone()).unwrap();
            let out = merger.merge(db.branch(b_id).unwrap(), policy(Validation::BaseRead)).unwrap();
            assert_eq!(out.refused, None, "{what}: {out:?}");
            assert_eq!(out.rows_changed, 2, "{what}: {out:?}");
            let rows = snapshot(&trunk, "t");
            assert!(rows[&10].contains("first") && rows[&250].contains("second"), "{what}");
        }
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

/// D-T5 and A6.4 (i): the derivation's refusals: DDL, a clear (and a DML delete of every row, which
/// the pages cannot tell apart), and an incremental blob write on a row whose leaf the branch did
/// not write.
#[test]
fn derivation_refusals_ddl_clear_delete_all_and_unowned_blob_write() {
    for case in ["ddl", "clear", "delete-all", "blob"] {
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
            "clear" | "delete-all" => {
                w1.derive_refusals_clear_or_delete_all - w0.derive_refusals_clear_or_delete_all
            }
            _ => w1.derive_refusals_unattributed - w0.derive_refusals_unattributed,
        };
        assert_eq!(counted, 1, "{case}: refused for another reason: {out:?} {w1:?}");
    }
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

/// A6.3: a DELETE that makes a balance free a sibling leaf (free_page's AddToTrunk path does not
/// write the freed page): the derived set holds every deleted row. Mutants r13_leaves_only and
/// r13_skip_freed_base must turn it red.
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
    let model = snapshot(&conn, "t");
    drop(conn);
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
    assert!(w.derive_subtrees_enumerated + w.derive_freelist_reads > 0, "no freed page was exercised: {w:?}");
}

/// D-T4 (A6.3): an interior balance that relinks a subtree the branch did not write derives no
/// change for its rows; the cancellation fired and nothing was enumerated. Mutant
/// r13_no_relink_cancel must turn it red (its output is the same; only the counters tell).
#[test]
fn an_interior_balance_that_relinks_a_subtree_derives_no_change_for_its_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    // Three levels at 1 KiB pages: interior pages over many leaves.
    seed(&trunk, 6000);
    let root = root_of(&trunk, "t");
    let b = trunk.fork_branch().unwrap();
    let base = snapshot(&trunk, "t");
    let conn = b.connect().unwrap();
    // Empty most leaves under one interior page, so that interior page underflows and its
    // siblings take its remaining children: those children move without being written.
    exec(&conn, "DELETE FROM t WHERE id BETWEEN 3000 AND 3370");
    let model = snapshot(&conn, "t");
    drop(conn);
    let b_id = b.into_id();
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(&db.branch(b_id).unwrap()).unwrap();
    let got: BTreeSet<(i64, i64)> = got.keys.into_iter().collect();
    assert_eq!(got, differing(root, &base, &model));
    let w1 = db.branch_merge_work();
    assert!(w1.derive_subtrees_cancelled > w0.derive_subtrees_cancelled, "no relink was cancelled: {w1:?}");
    assert_eq!(w1.derive_subtrees_enumerated, w0.derive_subtrees_enumerated, "a relink was enumerated: {w1:?}");
}

/// A6.5: the freelist walk reads only the branch's owned prefix of the chain, however long the
/// trunk's freelist is. Mutant r13_freelist_whole must turn it red.
#[test]
fn a_branch_over_a_long_trunk_freelist_reads_only_its_owned_prefix() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = open(&dir.path().join("m.db"), Mode::Catalog);
    let trunk = db.connect().unwrap();
    seed(&trunk, 4000);
    // A long trunk freelist.
    exec(&trunk, "DELETE FROM t WHERE id > 200");
    let b = trunk.fork_branch().unwrap();
    exec(&b.connect().unwrap(), "UPDATE t SET v = 'x' WHERE id = 3");
    let b_id = b.into_id();
    let w0 = db.branch_merge_work();
    let mut merger = Merger::new(trunk.clone()).unwrap();
    let got = merger.derive_only(&db.branch(b_id).unwrap()).unwrap();
    assert_eq!(got.keys.len(), 1);
    let w1 = db.branch_merge_work();
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
/// evictions, a reopen, and a released middle level: every level's rows go in. Mutant
/// r13_owned_current_only must turn it red.
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
            } else {
                _held.push(level.into_id());
            }
            level = next;
        }
        exec(&level.connect().unwrap(), "UPDATE t SET v = 'top' WHERE id = 399");
        top_id.set(level.into_id());
        db.branch_compact_now().unwrap();
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
