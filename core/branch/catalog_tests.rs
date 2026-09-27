//! Catalog mode (r11-restart lane): what the published fixes must make true. The whole of
//! `durability_tests.rs` also runs against catalog mode when `R11_BRANCH_CATALOG` is set; these
//! are the properties only catalog mode claims.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::path::Path;

fn catalog() -> DatabaseOpts {
    // r11-ever amendment 15: R11_SPLICE=1 runs this file in the F7 splice arm (off by default).
    DatabaseOpts::new()
        .with_branch_durability(BranchDurability::Catalog { sync: true })
        .with_branch_splice(std::env::var_os("R11_SPLICE").is_some())
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

// ---- r11-ever: the F7 durable port on the composed base (UNBUILT; r11-ever amendment 14) ----

/// `catalog()` in the F7 splice arm, whatever R11_SPLICE says: the tests that assert a splice
/// (r11-ever amendment 15).
fn spliced() -> DatabaseOpts {
    catalog().with_branch_splice(true)
}

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}")).unwrap();
}

/// U6 (r11-invariant-matrix): a splice moves a child to its spliced-out parent's key, and a catalog
/// store must move the child's row, and its `branch_children` entry, with it. P writes a page, forks
/// Z, Q1 and Q2, and rewrites the page, so P keeps its first version for Z's key and both Q's; Z
/// forks C. After a checkpoint (all of them catalog rows) Z's release splices C into Z's key under
/// P. Q1's reap, BEFORE the next checkpoint (the matrix's interleaving: the catalog still lists Z
/// at that key, the in-memory index C), keeps P's first version: C, at Z's key, is its neighbour
/// below. A second checkpoint writes the splice. After a reopen, C is read from the catalog under P
/// (a row still naming the deleted Z would be "a missing parent"), Q2's reap keeps the version the
/// same way (now from the re-keyed catalog row), and C still reads it and releases cleanly.
#[test]
fn a_spliced_child_is_listed_under_its_new_parent_after_a_checkpoint_and_a_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (p_id, q_id, c_id, p7);
    {
        let db = open_at(&path, spliced()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let p = trunk.fork_branch().unwrap();
        let s0: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
        set(&p.connect().unwrap(), 7, "p-first");
        p7 = db
            .branch_slots_in_use()
            .into_iter()
            .filter(|s| !s0.contains(s))
            .collect::<Vec<u32>>();
        assert!(!p7.is_empty(), "p's write took no slot of its own");
        let z = p.fork().unwrap();
        let q1 = p.fork().unwrap();
        let q = p.fork().unwrap();
        set(&p.connect().unwrap(), 7, "p-second"); // P retains its first version for Z, Q1 and Q
        let c = z.fork().unwrap();
        db.branch_compact_now().unwrap(); // P, Z, Q1, Q and C are catalog rows
        let reaped = z.reap().unwrap(); // one live child: C takes Z's key under P
        assert!(reaped.deferred, "{reaped:?}");
        let reaped = q1.reap().unwrap();
        assert!(!reaped.deferred, "{reaped:?}");
        for slot in &p7 {
            assert!(!db.branch_slot_is_free(*slot), "slot {slot}: C still reads it, freed by Q1's reap");
        }
        assert_eq!(value(&c.connect().unwrap(), 7), "p-first");
        db.branch_compact_now().unwrap(); // Z deleted, C re-keyed
        assert_eq!(db.branch_stats().unwrap().live_branches, 3);
        p_id = p.into_id();
        q_id = q.into_id();
        c_id = c.into_id();
    }
    let db = open_at(&path, spliced()).unwrap();
    let c = db.branch(c_id).expect("the spliced child names a missing parent");
    assert_eq!(value(&c.connect().unwrap(), 7), "p-first");
    let _ = c.into_id();
    let reaped = db.branch(q_id).unwrap().reap().unwrap();
    assert!(!reaped.deferred, "{reaped:?}");
    for slot in &p7 {
        assert!(!db.branch_slot_is_free(*slot), "slot {slot}: C still reads it, freed by Q's reap");
    }
    let c = db.branch(c_id).unwrap();
    assert_eq!(value(&c.connect().unwrap(), 7), "p-first", "C lost P's first version");
    let reaped = c.reap().unwrap();
    assert!(!reaped.deferred, "{reaped:?}");
    for slot in &p7 {
        assert!(db.branch_slot_is_free(*slot), "slot {slot}: no reader is left, still held");
    }
    assert_eq!(db.branch_stats().unwrap().live_branches, 1, "P alone should be left");
    let _ = db.branch(p_id).unwrap().into_id();
}

/// U7 (r11-invariant-matrix, as the refuter corrected it: the node never made resident is the
/// zombie's CHILD): redo on demand parks a replayed Commit of a branch recovery did not read, and a
/// splice moves that branch's maps. Z and its one child C are catalog rows; C's Commit, rewriting
/// the page it read through Z, sits in the log's tail; a crash follows. Recovery parks the Commit
/// (C is not read). Z's release then splices Z into C, which must make C resident, its parked Commit
/// applied, before any map moves: C keeps its own version, and Z's version, which only C's own write
/// shadowed, is freed.
#[test]
fn a_splice_after_recovery_applies_the_childs_parked_commit_first() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (z_id, c_id, z7, image);
    {
        let db = open_at(&path, spliced()).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let z = trunk.fork_branch().unwrap();
        let s0: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
        set(&z.connect().unwrap(), 7, "z");
        z7 = db
            .branch_slots_in_use()
            .into_iter()
            .filter(|s| !s0.contains(s))
            .collect::<Vec<u32>>();
        assert!(!z7.is_empty(), "z's write took no slot of its own");
        let c = z.fork().unwrap();
        db.branch_compact_now().unwrap(); // Z and C are catalog rows
        set(&c.connect().unwrap(), 7, "c"); // a Commit in the tail
        image = crash_image(&path, dir.path());
        z_id = z.into_id();
        c_id = c.into_id();
    }
    let db = open_at(&image, spliced()).unwrap();
    let s = db.branch_open_stats();
    assert_eq!(s.parked_records, 1, "premise: C's Commit was not parked: {s:?}");
    assert_eq!(s.branch_loads, 0, "premise: recovery read a branch: {s:?}");
    let reaped = db.branch(z_id).unwrap().reap().unwrap();
    assert!(reaped.deferred, "{reaped:?}");
    assert_eq!(db.branch_stats().unwrap().live_branches, 1, "z was not spliced");
    for slot in &z7 {
        assert!(db.branch_slot_is_free(*slot), "slot {slot}: shadowed by C's own write, still held");
    }
    let c = db.branch(c_id).unwrap();
    assert_eq!(value(&c.connect().unwrap(), 7), "c", "C lost its parked Commit");
    let _ = c.into_id();
}

/// Review of c47df7e64, finding 1: a held branch's catalog row must stop saying held once its
/// connection closes. Otherwise a checkpoint after the close leaves `released = 2` with the `Close`
/// gone from the log, a restart holds the branch again, and replay skips the collects the live store
/// made since: here C2's reap, which retires P a second time (freeing P's page written between
/// C1's fork and C2's) and, in the splice arm, splices it into C1. D then reuses that slot, and at
/// the end of that recovery the late collect would free it under D's page. P, released while its
/// connection is open, has two children; the close retires it; checkpoint; C2's reap; D's commit;
/// crash. The image must keep D's slot in use and read D's page. The hold and its end are in both
/// arms, so the test runs in both (amendment 15): off, P stays, retired, beside C1 and D.
#[test]
fn a_closed_branch_is_not_held_again_after_a_checkpoint_and_a_crash() {
    for (splice, live) in [(false, 3), (true, 2)] {
        closed_branch_not_held_again(catalog().with_branch_splice(splice), live);
    }
}

fn closed_branch_not_held_again(opts: DatabaseOpts, live: usize) {
    let what = format!("splice={}", opts.branch_splice);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (image, d_id, d_slots, c1_id, in_use_live);
    {
        let db = open_at(&path, opts).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let p = trunk.fork_branch().unwrap();
        let c1 = p.fork().unwrap();
        let pc = p.connect().unwrap();
        let s0: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
        set(&pc, 7, "p-between");
        let x: Vec<u32> = db
            .branch_slots_in_use()
            .into_iter()
            .filter(|s| !s0.contains(s))
            .collect();
        assert_eq!(x.len(), 1, "{what}: premise: p's write took one slot");
        let c2 = p.fork().unwrap();
        drop(p); // released under pc: held
        db.branch_compact_now().unwrap(); // the row says released = 2
        drop(pc); // Close: P retired, two children kept
        db.branch_compact_now().unwrap(); // the row must say 1 now; the Close leaves the log
        let reaped = c2.reap().unwrap(); // P retired again (x is freed), spliced into C1 if on
        assert!(!reaped.deferred, "{what}: {reaped:?}");
        assert!(db.branch_slot_is_free(x[0]), "{what}: premise: C2's reap did not free p's page");
        let d = trunk.fork_branch().unwrap();
        set(&d.connect().unwrap(), 150, "d");
        d_slots = d.owned_slots();
        assert!(d_slots.contains(&x[0]), "{what}: premise: d did not reuse the freed slot: {d_slots:?}");
        in_use_live = db.branch_slots_in_use();
        image = crash_image(&path, dir.path());
        d_id = d.into_id();
        c1_id = c1.into_id();
    }
    let db = open_at(&image, opts).unwrap();
    for slot in &d_slots {
        assert!(!db.branch_slot_is_free(*slot), "{what}: slot {slot}: d's page, freed by the recovery");
    }
    let mut got = db.branch_slots_in_use();
    got.sort_unstable();
    let mut want = in_use_live;
    want.sort_unstable();
    assert_eq!(got, want, "{what}: the recovery's slot set is not the crashed store's");
    assert_eq!(db.branch_stats().unwrap().live_branches, live, "{what}: c1 and d (and P if off)");
    let d = db.branch(d_id).unwrap();
    assert_eq!(value(&d.connect().unwrap(), 150), "d", "{what}");
    let _ = d.into_id();
    let _ = db.branch(c1_id).unwrap().into_id();
}
