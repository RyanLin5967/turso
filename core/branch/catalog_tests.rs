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

/// a12-durable-open C-P: the trunk's retained versions are read in place, one version per probe,
/// never a page's whole version list. K versions of ONE trunk page, each held by its own child;
/// after a checkpoint and a reopen: the open reads none; a child's first read of that page, the
/// oldest child's reap, and a crash recovery replaying one pre-image record of that page each read
/// a number of catalog rows that does not depend on K; and every child still reads its version.
#[test]
fn trunk_versions_are_read_in_place_whatever_a_page_holds() {
    let mut per_k = Vec::new();
    for k in [40usize, 400] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("c.db");
        let mut kids = Vec::new();
        let seen = |i: usize| -> String {
            if i == 0 {
                format!("trunk-0001-{}", "x".repeat(80))
            } else {
                format!("u{}", i - 1)
            }
        };
        {
            let db = open_at(&path, catalog()).unwrap();
            let trunk = db.connect().unwrap();
            seed(&trunk);
            for i in 0..k {
                kids.push(trunk.fork_branch().unwrap().into_id());
                trunk.execute(format!("UPDATE t SET v = 'u{i}' WHERE id = 1")).unwrap();
            }
            assert!(db.branch_trunk_retained() >= k as u64, "k={k}: the trunk writes retained too little");
            db.branch_compact_now().unwrap();
        }
        let db = open_at(&path, catalog()).unwrap();
        let s = db.branch_open_stats();
        assert_eq!(
            (s.records, s.trunk_probes, s.trunk_rows, s.trunk_page_loads, s.branch_loads),
            (0, 0, 0, 0, 0),
            "k={k}: the open read state: {s:?}"
        );
        let rows = |db: &Arc<Database>| db.branch_catalog_counters().3;
        let mid = k / 2;
        let r0 = rows(&db);
        let b = db.branch(kids[mid]).unwrap();
        assert_eq!(value(&b.connect().unwrap(), 1), seen(mid), "k={k}: child {mid} read the wrong row 1");
        let _ = b.into_id();
        let r1 = rows(&db);
        db.branch(kids[0]).unwrap().reap().unwrap();
        let r2 = rows(&db);
        // One more child, then a trunk write of the hot page: its pre-image record is the tail.
        let trunk = db.connect().unwrap();
        kids.push(trunk.fork_branch().unwrap().into_id());
        trunk.execute("UPDATE t SET v = 'after' WHERE id = 1").unwrap();
        let image = crash_image(&path, dir.path());
        drop(trunk);
        drop(db);
        let db = open_at(&image, catalog()).unwrap();
        let s = db.branch_open_stats();
        per_k.push((r1 - r0, r2 - r1, s.records, s.cat_rows_read, s.trunk_rows, s.trunk_page_loads));
        for i in [1, mid, k - 1, k] {
            let b = db.branch(kids[i]).unwrap();
            assert_eq!(value(&b.connect().unwrap(), 1), seen(i), "k={k}: after recovery child {i}");
            let _ = b.into_id();
        }
        assert_eq!(db.branch_stats().unwrap().live_branches, k, "k={k}");
    }
    assert_eq!(
        per_k[0], per_k[1],
        "catalog rows read (first read, oldest reap, recovery records / rows / trunk rows / page \
         loads) depend on the versions one trunk page holds: {per_k:?}"
    );
    assert!(per_k[0].0 <= 8 && per_k[0].1 <= 8, "{per_k:?}");
}

/// a12-durable-open C-R: a crash after commits to OLD branches (the steady state: agents committing to
/// branches that existed at the last checkpoint) recovers without reading those branches — their Commits
/// are parked and applied at first touch — and every branch then reads its last commit, the slot set
/// equals the crashed store's, and the next checkpoint and reopen keep all of it.
#[test]
fn a_crash_after_commits_to_old_branches_reads_no_branch_at_recovery() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (ids, image, slots_before);
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        ids = grow(&db, 60);
        // A child of every tenth branch, so some committing branches retain versions for a child.
        for &id in ids.iter().step_by(10) {
            let b = db.branch(id).unwrap();
            let _ = b.fork().unwrap().into_id();
            let _ = b.into_id();
        }
        db.branch_compact_now().unwrap();
        for round in 0..3 {
            for (i, &id) in ids.iter().enumerate().step_by(3) {
                let b = db.branch(id).unwrap();
                let row = 1 + (i % 400) as i64;
                b.connect()
                    .unwrap()
                    .execute(format!("UPDATE t SET v = 'r{round}-{}' WHERE id = {row}", id.0))
                    .unwrap();
                let _ = b.into_id();
            }
        }
        slots_before = db.branch_stats().unwrap().arena_slots_in_use;
        image = crash_image(&path, dir.path());
    }
    let db = open_at(&image, catalog()).unwrap();
    let s = db.branch_open_stats();
    assert_eq!(s.branch_loads, 0, "recovery read branches: {s:?}");
    assert_eq!(s.parked_records, 60, "20 branches x 3 rounds were not all parked: {s:?}");
    // No branch state read: exactly the rows an open with an empty tail reads (meta, lease floor,
    // released list). The queries also hold one free-table probe per slot the tail names below the
    // checkpoint's high-water mark (the arena rebuild's `free_has`, which returns no row for a slot
    // in use), so they are bounded by the tail's slots, not by its records' branches.
    // (Was `cat_queries <= 5`: a wrong premise, which the free-table probes broke: PREREG A8.)
    assert_eq!(s.cat_rows_read, 9, "recovery read catalog rows beyond the open's own: {s:?}");
    assert!(s.cat_queries <= 3 + s.touched_slots, "recovery queried the catalog beyond the tail's slots: {s:?}");
    assert_eq!(db.branch_stats().unwrap().arena_slots_in_use, slots_before, "the slot count moved");
    for (i, &id) in ids.iter().enumerate() {
        let b = db.branch(id).unwrap();
        let row = 1 + (i % 400) as i64;
        let want = if i % 3 == 0 { format!("r2-{}", id.0) } else { format!("b{}", id.0) };
        assert_eq!(value(&b.connect().unwrap(), row), want, "branch {} after recovery", id.0);
        let _ = b.into_id();
    }
    db.branch_compact_now().unwrap();
    drop(db);
    let db = open_at(&image, catalog()).unwrap();
    assert_eq!(db.branch_stats().unwrap().arena_slots_in_use, slots_before, "after a checkpoint and reopen");
    for (i, &id) in ids.iter().enumerate().step_by(3) {
        let b = db.branch(id).unwrap();
        let row = 1 + (i % 400) as i64;
        assert_eq!(value(&b.connect().unwrap(), row), format!("r2-{}", id.0));
        let _ = b.into_id();
    }
}

// ---- r11-restart-r2 (PREREG A14; UNBUILT when written) ----

/// Branch `id`'s own row rewritten to `v` and committed.
fn write(db: &Arc<Database>, id: BranchId, row: i64, v: &str) {
    let b = db.branch(id).unwrap();
    b.connect()
        .unwrap()
        .execute(format!("UPDATE t SET v = '{v}' WHERE id = {row}"))
        .unwrap();
    let _ = b.into_id();
}

/// Every branch of `model` reads its own row as the model says, and no other branch is live.
fn check(db: &Arc<Database>, model: &std::collections::HashMap<BranchId, (i64, String)>) {
    assert_eq!(db.branch_stats().unwrap().live_branches, model.len(), "live branches");
    for (&id, (row, want)) in model {
        let b = db.branch(id).unwrap();
        assert_eq!(&value(&b.connect().unwrap(), *row), want, "branch {}", id.0);
        let _ = b.into_id();
    }
}

/// Wait, 10 s at most, for a fuzzy checkpoint to arrive at hook `stage`.
fn wait_held(db: &Arc<Database>, stage: u8) {
    let t = std::time::Instant::now();
    while db.branch_checkpoint_held() != stage | store::HOLD_ARRIVED {
        assert!(
            t.elapsed() < std::time::Duration::from_secs(10),
            "the fuzzy checkpoint never reached stage {stage}"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// `grow` into a model: each branch's row and value.
fn grown(db: &Arc<Database>, n: usize) -> std::collections::HashMap<BranchId, (i64, String)> {
    grow(db, n)
        .into_iter()
        .enumerate()
        .map(|(i, id)| (id, (1 + (i % 400) as i64, format!("b{}", id.0))))
        .collect()
}

/// F-FZ, P-FZ1: a fuzzy checkpoint holds no store lock while it writes. Held after writing the
/// captured rows and before committing them, it lets the store commit, fork, read and reap; after
/// its install every branch reads as the model says, and after a reopen too. It ran at most 4
/// catalog statements under the store mutex.
#[test]
fn a_fuzzy_checkpoint_holds_no_store_lock_while_it_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let mut model;
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        model = grown(&db, 120);
        db.branch_compact_now().unwrap();
        let mut ids: Vec<BranchId> = model.keys().copied().collect();
        ids.sort();
        for &id in ids.iter().step_by(3) {
            let v = format!("c{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
        }
        let before = db.branch_checkpoint_counters();
        db.branch_checkpoint_hold(store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no fuzzy checkpoint started");
        wait_held(&db, store::HOLD_BEFORE_COMMIT);
        // The writer sits inside its catalog transaction: each of these takes the store mutex.
        for &id in ids.iter().skip(1).step_by(3) {
            let v = format!("d{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
        }
        model.extend(grown(&db, 10));
        for &id in ids.iter().skip(2).step_by(30) {
            let _ = db.branch(id).unwrap().reap().unwrap();
            model.remove(&id);
        }
        check(&db, &model);
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        let after = db.branch_checkpoint_counters();
        assert_eq!(after[0] - before[0], 1, "installed: {before:?} -> {after:?}");
        assert_eq!(after[1] - before[1], 1, "flights: {before:?} -> {after:?}");
        assert!(
            after[5] - before[5] <= 4,
            "catalog statements under the store mutex: {before:?} -> {after:?}"
        );
        check(&db, &model);
    }
    let db = open_at(&path, catalog()).unwrap();
    check(&db, &model);
}

/// F-FZ crash state S1: the catalog committed, the log not yet cut. The image replays exactly the
/// records written after the capture, and reads as the store did.
#[test]
fn a_crash_between_a_fuzzy_commit_and_its_install_replays_only_the_suffix() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let image;
    let mut model;
    let mut after = 0u64;
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        model = grown(&db, 80);
        db.branch_compact_now().unwrap();
        let mut ids: Vec<BranchId> = model.keys().copied().collect();
        ids.sort();
        for &id in ids.iter().step_by(2) {
            let v = format!("c{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
        }
        db.branch_checkpoint_hold(store::HOLD_AFTER_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no fuzzy checkpoint started");
        wait_held(&db, store::HOLD_AFTER_COMMIT);
        for &id in ids.iter().step_by(5) {
            let v = format!("s{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
            after += 1;
        }
        image = crash_image(&path, dir.path());
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        check(&db, &model);
    }
    let db = open_at(&image, catalog()).unwrap();
    let s = db.branch_open_stats();
    assert_eq!(s.records, after, "replayed other than the suffix after the capture: {s:?}");
    check(&db, &model);
    db.branch_compact_now().unwrap();
    drop(db);
    let db = open_at(&image, catalog()).unwrap();
    check(&db, &model);
}

/// F-FZ crash state S0: the catalog transaction written, not committed. The image replays every
/// record since the previous checkpoint: the captured ones and the ones after.
#[test]
fn a_crash_before_a_fuzzy_commit_replays_the_whole_log() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let image;
    let mut model;
    let mut records = 0u64;
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        model = grown(&db, 80);
        db.branch_compact_now().unwrap();
        let mut ids: Vec<BranchId> = model.keys().copied().collect();
        ids.sort();
        for &id in ids.iter().step_by(2) {
            let v = format!("c{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
            records += 1;
        }
        db.branch_checkpoint_hold(store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no fuzzy checkpoint started");
        wait_held(&db, store::HOLD_BEFORE_COMMIT);
        for &id in ids.iter().step_by(5) {
            let v = format!("s{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
            records += 1;
        }
        image = crash_image(&path, dir.path());
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
    }
    let db = open_at(&image, catalog()).unwrap();
    let s = db.branch_open_stats();
    // Plus the checkpoint's own marker (`Record::Checkpoint`), which the first write after the
    // capture flushed with its own record.
    assert_eq!(s.records, records + 1, "the uncommitted checkpoint cut the log: {s:?}");
    check(&db, &model);
}

/// F-FZ's log bound: with fuzzy checkpoints the log may pass the threshold while one is in flight,
/// but an operation that finds it past twice the threshold waits for the install, so the log never
/// exceeds twice the threshold plus one operation's records. (The sharp checkpoint's bound, the
/// threshold plus one operation, is `the_log_stays_under_the_checkpoint_threshold`, which holds with
/// `R11_CKPT=sharp` only: PREREG A14 addendum.)
#[test]
fn the_log_stays_under_twice_the_threshold_with_fuzzy_checkpoints() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_at(&path, catalog()).unwrap();
    seed(&db.connect().unwrap());
    let files = journal::BranchFiles::for_db(path.to_str().unwrap());
    let trunk = db.connect().unwrap();
    let mut max_log = 0;
    for i in 0..60_000u64 {
        let b = trunk.fork_branch().unwrap();
        let row = 1 + (i % 400) as i64;
        b.connect()
            .unwrap()
            .execute(format!("UPDATE t SET v = 'b{i}' WHERE id = {row}"))
            .unwrap();
        let _ = b.into_id();
        max_log = max_log.max(std::fs::metadata(&files.log).unwrap().len());
    }
    db.branch_checkpoint_wait();
    let c = db.branch_checkpoint_counters();
    assert!(c[1] >= 2, "fewer than two fuzzy checkpoints ran: {c:?}");
    assert!(max_log <= (2 << 20) + 256, "the log grew to {max_log} bytes: {c:?}");
}

/// C-R's settle in bounded batches (PREREG A14): after a crash whose tail commits to 200 old
/// branches, a fuzzy checkpoint starts only once they are applied, 64 branches per call at most.
#[test]
fn parked_commits_settle_in_bounded_batches_before_a_fuzzy_checkpoint() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let image;
    let mut model;
    {
        let db = open_at(&path, catalog()).unwrap();
        seed(&db.connect().unwrap());
        model = grown(&db, 200);
        db.branch_compact_now().unwrap();
        let ids: Vec<BranchId> = model.keys().copied().collect();
        for id in ids {
            let v = format!("r{}", id.0);
            write(&db, id, model[&id].0, &v);
            model.get_mut(&id).unwrap().1 = v;
        }
        image = crash_image(&path, dir.path());
    }
    let db = open_at(&image, catalog()).unwrap();
    assert_eq!(db.branch_open_stats().parked_records, 200);
    let before = db.branch_checkpoint_counters();
    let mut calls = 0;
    while !db.branch_checkpoint_fuzzy_now().unwrap() {
        calls += 1;
        assert!(calls < 10, "no checkpoint after {calls} settle batches");
    }
    db.branch_checkpoint_wait();
    let after = db.branch_checkpoint_counters();
    assert_eq!(calls, 3, "64 + 64 + 64, then 8 and the checkpoint");
    assert_eq!(after[6] - before[6], 4, "settle batches: {before:?} -> {after:?}");
    assert_eq!(after[7] - before[7], 200, "settle loads: {before:?} -> {after:?}");
    assert_eq!(after[8], 64, "most loads in one batch: {after:?}");
    check(&db, &model);
}

/// F-EXP: an open after every lease ran out reaps one bounded batch (256), not every branch; a
/// due branch the pass did not reach is reaped when an operation names it; `expire_branches`
/// reaps the rest.
#[test]
fn an_open_after_every_lease_ran_out_reaps_one_bounded_batch() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let lease = std::time::Duration::from_secs(3600);
    let opts = || catalog().with_branch_lease(Some(lease));
    let n = 600;
    let ids;
    {
        let db = open_at(&path, opts()).unwrap();
        seed(&db.connect().unwrap());
        ids = grow(&db, n);
        db.branch_compact_now().unwrap();
        db.branch_lease_clock_advance(lease + std::time::Duration::from_secs(1));
        // The close stamps the clock and expires nothing.
    }
    let db = open_at(&path, opts()).unwrap();
    assert_eq!(
        db.branch_stats().unwrap().live_branches,
        n - 256,
        "the open's expiry pass is not bounded: {:?}",
        db.branch_open_stats()
    );
    // The connection's own expiry pass reaps the next batch (the 256 earliest deadlines left), and
    // the branch it names, due but later than all of those, is reaped on access.
    let last = *ids.last().unwrap();
    let b = db.branch(last).unwrap();
    assert!(b.connect().is_err(), "a branch past its lease opened a connection");
    drop(b);
    let live = db.branch_stats().unwrap().live_branches;
    assert_eq!(live, n - 2 * 256 - 1, "the connection's pass or the reap on access");
    let rest = db.expire_branches().unwrap();
    assert_eq!(rest.reaped.len(), live, "expire_branches did not reap the rest");
    assert_eq!(db.branch_stats().unwrap().live_branches, 0);
}
