//! Catalog mode (r11-restart lane): what the published fixes must make true. The whole of
//! `durability_tests.rs` also runs against catalog mode when `R11_BRANCH_CATALOG` is set; these
//! are the properties only catalog mode claims.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::path::Path;

fn catalog() -> DatabaseOpts {
    DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync: true })
}

fn open_at(path: &Path, opts: DatabaseOpts) -> Result<Arc<Database>> {
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

fn value(conn: &Arc<Connection>, id: i64) -> String {
    let rows = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    match &rows[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn seed(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=400 {
        conn.execute(format!("INSERT INTO t VALUES ({id}, 'trunk-{id:04}-{}')", "x".repeat(80)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

/// Grow `n` branches, each writing row `1 + i % 400`, detached; return their ids.
fn grow(db: &Arc<Database>, n: usize) -> Vec<BranchId> {
    let trunk = db.connect().unwrap();
    (0..n)
        .map(|i| {
            let b = trunk.fork_branch().unwrap();
            let row = 1 + (i % 400) as i64;
            b.connect()
                .unwrap()
                .execute(format!("UPDATE t SET v = 'b{}' WHERE id = {row}", b.id().0))
                .unwrap();
            b.into_id()
        })
        .collect()
}

/// The claim: after a checkpoint, an open reads the catalog's meta row and the log's header, and
/// no branch state — whatever the branch count. It is the counters that are asserted, at two
/// branch counts ten times apart, not a time.
#[test]
fn an_open_after_a_checkpoint_reads_no_branch_state() {
    for n in [50usize, 500] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("c.db");
        let ids;
        {
            let db = open_at(&path, catalog()).unwrap();
            seed(&db.connect().unwrap());
            ids = grow(&db, n);
            db.branch_compact_now().unwrap();
        }
        let db = open_at(&path, catalog()).unwrap();
        let s = db.branch_open_stats();
        assert_eq!(s.branch_loads, 0, "n={n}: the open read branch states: {s:?}");
        assert_eq!(s.trunk_page_loads, 0, "n={n}: the open read trunk pages: {s:?}");
        assert_eq!(s.records, 0, "n={n}: a checkpointed log replayed records: {s:?}");
        assert_eq!(s.snap_bytes, 0, "n={n}: a catalog store read a snapshot: {s:?}");
        assert_eq!(s.states, n as u64, "n={n}: {s:?}");
        assert_eq!(db.branch_stats().unwrap().live_branches, n);
        // And the state is all there, read on demand.
        for (i, &id) in ids.iter().enumerate().step_by(7) {
            let b = db.branch(id).unwrap();
            let row = 1 + (i % 400) as i64;
            assert_eq!(value(&b.connect().unwrap(), row), format!("b{}", id.0));
            let _ = b.into_id();
        }
        let (loads, _, _, _) = db.branch_catalog_counters();
        assert_eq!(loads as usize, ids.iter().step_by(7).count(), "n={n}: loads are not per touch");
    }
}

/// The log is bounded by the checkpoint threshold, not by twice the live state, and no snapshot
/// file is ever written.
#[test]
fn the_log_stays_under_the_checkpoint_threshold() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_at(&path, catalog()).unwrap();
    seed(&db.connect().unwrap());
    let files = journal::BranchFiles::for_db(path.to_str().unwrap());
    let trunk = db.connect().unwrap();
    let mut max_log = 0;
    for i in 0..30_000u64 {
        let b = trunk.fork_branch().unwrap();
        let row = 1 + (i % 400) as i64;
        b.connect()
            .unwrap()
            .execute(format!("UPDATE t SET v = 'b{i}' WHERE id = {row}"))
            .unwrap();
        let _ = b.into_id();
        max_log = max_log.max(std::fs::metadata(&files.log).unwrap().len());
    }
    assert!(!files.snap.exists(), "a catalog store wrote a snapshot");
    assert!(max_log <= (1 << 20) + 64, "the log grew to {max_log} bytes");
    assert_eq!(db.branch_stats().unwrap().live_branches, 30_000);
}

/// Slots freed by reaps are in the catalog's free table after a reopen, and new branches reuse
/// them instead of growing the arena.
#[test]
fn freed_slots_survive_a_reopen_and_are_reused() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (keep, arena_len);
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        let ids = grow(&db, 40);
        for &id in &ids[..20] {
            db.branch(id).unwrap().reap().unwrap();
        }
        keep = ids[20..].to_vec();
        db.branch_compact_now().unwrap();
        arena_len = std::fs::metadata(journal::BranchFiles::for_db(path.to_str().unwrap()).arena)
            .unwrap()
            .len();
    }
    let db = open_at(&path, catalog()).unwrap();
    let st = db.branch_stats().unwrap();
    assert_eq!(st.live_branches, 20);
    let retained = db.branch_trunk_retained() as usize;
    assert_eq!(st.arena_slots_in_use, 20 + retained, "{st:?}");
    assert!(st.arena_slots_free >= 20, "the reaped branches' slots are not free: {st:?}");
    grow(&db, 20);
    let files = journal::BranchFiles::for_db(path.to_str().unwrap());
    assert_eq!(
        std::fs::metadata(&files.arena).unwrap().len(),
        arena_len,
        "new branches grew the arena instead of reusing freed slots"
    );
    for &id in &keep {
        let b = db.branch(id).unwrap();
        let _ = b.connect().unwrap();
        let _ = b.into_id();
    }
}

/// A crash image: every file of the database, the catalog's included, copied while it is OPEN.
fn crash_image(src: &Path, dir: &Path) -> std::path::PathBuf {
    let dst = dir.join("crash-image.db");
    for suffix in ["", "-wal", "-branch-log", "-branch-arena", "-branch-cat", "-branch-cat-wal"] {
        let from = std::path::PathBuf::from(format!("{}{suffix}", src.display()));
        if from.exists() {
            std::fs::copy(&from, format!("{}{suffix}", dst.display())).unwrap();
        }
    }
    dst
}

/// A crash leaves a store a reopen recovers by replaying only the log's tail after the last
/// checkpoint: the image is taken while the database is open, so no close code has run.
#[test]
fn a_crash_after_a_checkpoint_replays_only_the_tail() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (ids, tail, image);
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        // (id, row): `grow` numbers rows from its own first branch, so each call's rows restart at 1.
        let rows = |v: Vec<BranchId>| -> Vec<(BranchId, i64)> {
            v.into_iter().enumerate().map(|(i, id)| (id, 1 + (i % 400) as i64)).collect()
        };
        let mut all = rows(grow(&db, 30));
        db.branch_compact_now().unwrap();
        tail = rows(grow(&db, 5));
        all.extend(&tail);
        ids = all;
        let trunk = db.connect().unwrap();
        trunk.execute("UPDATE t SET v = 'after' WHERE id = 3").unwrap();
        image = crash_image(&path, dir.path());
    }
    let db = open_at(&image, catalog()).unwrap();
    let s = db.branch_open_stats();
    assert!(s.records > 0 && s.records <= 20, "the tail was not what was replayed: {s:?}");
    assert!(s.branch_loads <= tail.len() as u64 + 1, "recovery loaded more than the tail: {s:?}");
    assert_eq!(db.branch_stats().unwrap().live_branches, 35);
    for &(id, row) in &ids {
        let b = db.branch(id).unwrap();
        let c = b.connect().unwrap();
        assert_eq!(value(&c, row), format!("b{}", id.0));
        // Every branch forked before the trunk's write sees row 3 as it was.
        if row != 3 {
            assert!(value(&c, 3).starts_with("trunk-0003"), "branch {} saw the trunk's write", id.0);
        }
        drop(c);
        let _ = b.into_id();
    }
}

/// One mode per set of files: a catalog store refuses a snapshot store's files and vice versa.
#[test]
fn the_two_durable_modes_refuse_each_others_files() {
    let dir = tempfile::TempDir::new().unwrap();
    let cat_path = dir.path().join("c.db");
    {
        let db = open_at(&cat_path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        grow(&db, 3);
    }
    let durable = DatabaseOpts::new().with_branch_durability(BranchDurability::Durable { sync: true });
    assert!(open_at(&cat_path, durable).is_err(), "a snapshot store opened a catalog store's files");
    let snap_path = dir.path().join("s.db");
    {
        let db = open_at(&snap_path, durable).unwrap();
        seed(&db.connect().unwrap());
        grow(&db, 3);
        db.branch_compact_now().unwrap();
    }
    assert!(open_at(&snap_path, catalog()).is_err(), "a catalog store opened a snapshot store's files");
}

/// The claim rests on every per-operation catalog query being an index seek: a query that walks a
/// table, or sorts its matches, is Θ(N) and would put back what the catalog removes. Read from the
/// planner itself, not assumed: no plan line may be a bare SCAN of a catalog table or a temp
/// B-tree sort.
#[test]
fn every_catalog_lookup_is_a_seek() {
    let dir = tempfile::TempDir::new().unwrap();
    let cat = super::catalog::Catalog::open(&dir.path().join("cat"), false).unwrap();
    let mut bad = Vec::new();
    for (sql, lines) in cat.plans().unwrap() {
        eprintln!("{sql}\n    {}", lines.join("\n    "));
        assert!(!lines.is_empty(), "no plan for {sql}");
        for line in &lines {
            let walks = ["branch", "cur", "ret", "free"]
                .iter()
                .any(|t| line.contains(&format!("SCAN {t}")));
            if walks || line.contains("TEMP B-TREE") {
                bad.push(format!("{sql}: {line}"));
            }
        }
    }
    assert!(bad.is_empty(), "catalog lookups that walk or sort:\n{}", bad.join("\n"));
}

/// Fix v4 (PREREG A11): releasing K children of one parent after a checkpoint reads O(K) catalog
/// rows. Before, each release's neighbour lookup skipped every sibling released since the
/// checkpoint one row at a time — about K^2/2 rows for releases in ascending order.
#[test]
fn releasing_many_siblings_reads_linear_catalog_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_at(&path, catalog()).unwrap();
    seed(&db.connect().unwrap());
    let k = 3000;
    let ids = grow(&db, k);
    db.branch_compact_now().unwrap();
    let (_, _, _, rows0) = db.branch_catalog_counters();
    for &id in &ids {
        db.branch(id).unwrap().reap().unwrap();
    }
    let (_, _, _, rows1) = db.branch_catalog_counters();
    let read = rows1 - rows0;
    assert!(
        read < 20 * k as u64,
        "releasing {k} siblings read {read} catalog rows: not linear"
    );
    assert_eq!(db.branch_stats().unwrap().live_branches, 0);
}
