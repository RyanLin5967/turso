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

// ---- r11-ever: the F7 durable port on the composed base (UNBUILT; r11-ever amendment 14) ----

/// `catalog()` in the F7 splice arm: the tests that assert a splice
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

/// Review 3 (of 9ae27b4b2..3f3036b18), finding A1: the red test above ends the hold in the live
/// store only (`close`), so the two other places a hold ends, a replayed `Close` and the end of
/// recovery (`close_held`), each marking the row DIRTY_ROW, had no killer. Session 1: P, with
/// children C1 and C2, writes page x between the two forks and is released under its open
/// connection; a checkpoint writes `released = 2`; then either the connection closes (the `Close`
/// is in the log's tail) or not (P is still held), and the process crashes. Session 2 recovers,
/// which ends the hold (by replaying the `Close`, or at the end of recovery), checkpoints (the row
/// must say 1 now), reaps C2 (x is freed) and lets D reuse x; crash. Session 3's recovery must
/// not hold P again and free x under D's page. Both arms, both ways the hold ends.
#[test]
fn a_held_row_is_cleared_when_a_recovery_ends_the_hold() {
    for (splice, live) in [(false, 3), (true, 2)] {
        for closed_before_crash in [true, false] {
            held_row_cleared_by_recovery(catalog().with_branch_splice(splice), live, closed_before_crash);
        }
    }
}

fn held_row_cleared_by_recovery(opts: DatabaseOpts, live: usize, closed_before_crash: bool) {
    let what = format!("splice={} closed_before_crash={closed_before_crash}", opts.branch_splice);
    let dir = tempfile::TempDir::new().unwrap();
    let (one, two) = (dir.path().join("one"), dir.path().join("two"));
    std::fs::create_dir(&one).unwrap();
    std::fs::create_dir(&two).unwrap();
    let path = dir.path().join("c.db");
    let (image1, x, c1_id, c2_id);
    {
        let db = open_at(&path, opts).unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let p = trunk.fork_branch().unwrap();
        let c1 = p.fork().unwrap();
        let pc = p.connect().unwrap();
        let s0: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
        set(&pc, 7, "p-between");
        let fresh: Vec<u32> = db.branch_slots_in_use().into_iter().filter(|s| !s0.contains(s)).collect();
        assert_eq!(fresh.len(), 1, "{what}: premise: p's write took one slot");
        x = fresh[0];
        let c2 = p.fork().unwrap();
        drop(p); // released under pc: held
        db.branch_compact_now().unwrap(); // the row says released = 2
        if closed_before_crash {
            drop(pc); // the Close is in the log's tail
        }
        image1 = crash_image(&path, &one);
        c1_id = c1.into_id();
        c2_id = c2.into_id();
    }
    let (image2, d_id, d_slots, in_use_live);
    {
        let db = open_at(&image1, opts).unwrap(); // the recovery ends P's hold
        db.branch_compact_now().unwrap(); // the row must say 1 now
        let reaped = db.branch(c2_id).unwrap().reap().unwrap(); // P retired again: x is freed
        assert!(!reaped.deferred, "{what}: {reaped:?}");
        assert!(db.branch_slot_is_free(x), "{what}: premise: C2's reap did not free p's page");
        let trunk = db.connect().unwrap();
        let d = trunk.fork_branch().unwrap();
        set(&d.connect().unwrap(), 150, "d");
        d_slots = d.owned_slots();
        assert!(d_slots.contains(&x), "{what}: premise: d did not reuse the freed slot: {d_slots:?}");
        in_use_live = db.branch_slots_in_use();
        image2 = crash_image(&image1, &two);
        d_id = d.into_id();
    }
    let db = open_at(&image2, opts).unwrap();
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

/// Review 3, finding A2: in the U6 test above Q1's reap cannot tell whether the lookup found C at
/// Z's key, because Q, forked later, keeps P's first version either way. Here Q1 is P's NEWEST
/// child and C, at Z's key, the only other child P forked inside that version's life, so the
/// version survives Q1's reap only if the lookup finds C (the in-memory index says C; the catalog,
/// until the next checkpoint, still says Z). Then a checkpoint and a reopen, and C still reads it;
/// C's reap frees it.
#[test]
fn a_spliced_childs_key_keeps_its_parents_version_when_the_newest_sibling_goes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let (p_id, c_id, p7);
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
        let q1 = p.fork().unwrap(); // the newest child
        set(&p.connect().unwrap(), 7, "p-second"); // P retains its first version for Z and Q1
        let c = z.fork().unwrap();
        db.branch_compact_now().unwrap(); // P, Z, Q1 and C are catalog rows
        let reaped = z.reap().unwrap(); // one live child: C takes Z's key under P
        assert!(reaped.deferred, "{reaped:?}");
        let reaped = q1.reap().unwrap(); // lo = C at Z's key, hi = none
        assert!(!reaped.deferred, "{reaped:?}");
        for slot in &p7 {
            assert!(!db.branch_slot_is_free(*slot), "slot {slot}: C still reads it, freed by Q1's reap");
        }
        assert_eq!(value(&c.connect().unwrap(), 7), "p-first");
        db.branch_compact_now().unwrap();
        p_id = p.into_id();
        c_id = c.into_id();
    }
    let db = open_at(&path, spliced()).unwrap();
    let c = db.branch(c_id).expect("the spliced child names a missing parent");
    assert_eq!(value(&c.connect().unwrap(), 7), "p-first", "C lost P's first version");
    let reaped = c.reap().unwrap();
    assert!(!reaped.deferred, "{reaped:?}");
    for slot in &p7 {
        assert!(db.branch_slot_is_free(*slot), "slot {slot}: no reader is left, still held");
    }
    assert_eq!(db.branch_stats().unwrap().live_branches, 1, "P alone should be left");
    let _ = db.branch(p_id).unwrap().into_id();
}

/// Amendment 17 (the lead's decision on the catalog format key): a catalog store refuses the other
/// splice arm, and a catalog written before the F7 port, from its meta row alone, which carries the
/// format version in the page-size key's high bits. Here the log is cut to nothing after a
/// checkpoint, a torn header with no version to check (a crash while the log was being reset leaves
/// one), so only the catalog can refuse. The store's own arm then opens it and reads its branch;
/// last, the meta's key is set to 0, as the base wrote it, and even the own arm is refused. The open
/// reads the same nine meta rows as before: the key rides in a value, not a row.
#[test]
fn a_catalog_with_a_torn_log_header_opens_only_in_its_own_arm() {
    for splice in [false, true] {
        let what = format!("splice={splice}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("c.db");
        let own = catalog().with_branch_splice(splice);
        let b_id;
        {
            let db = open_at(&path, own).unwrap();
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            set(&b.connect().unwrap(), 7, "b");
            b_id = b.into_id();
            db.branch_compact_now().unwrap();
        }
        let files = journal::BranchFiles::for_db(path.to_str().unwrap());
        std::fs::write(&files.log, b"").unwrap();
        let err = match open_at(&path, own.with_branch_splice(!splice)) {
            Ok(_) => panic!("{what}: the other arm opened a catalog whose log header is torn"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("splice"), "{what}: refused for another reason: {err}");
        {
            let db = open_at(&path, own).expect("its own arm opens it");
            let b = db.branch(b_id).unwrap();
            assert_eq!(value(&b.connect().unwrap(), 7), "b", "{what}");
            let _ = b.into_id();
        }
        {
            let mut cat = catalog::Catalog::open(&files.cat, false).unwrap();
            let mut m = cat.meta().unwrap().expect("a checkpointed catalog has a meta row");
            assert_eq!(m.format, journal::format_version(splice), "{what}: the key was not written");
            m.format = 0;
            cat.begin().unwrap();
            cat.put_meta(&m).unwrap();
            cat.commit().unwrap();
        }
        std::fs::write(&files.log, b"").unwrap();
        assert!(open_at(&path, own).is_err(), "{what}: a catalog written before the port opened");
    }
}

/// Amendment 35's census, made to fire before it is trusted: on a grown catalog every table tree's
/// leaf cells are its rows, and every index tree's are its table's rows, so no leaf cell is missed
/// or counted twice; the census walks the same pages as the shape (the ledger still closes).
#[test]
fn the_catalog_census_counts_every_row_once() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_at(&path, catalog()).unwrap();
    seed(&db.connect().unwrap());
    let _ids = grow(&db, 300);
    db.branch_compact_now().unwrap();
    let s = db.branch_catalog_shape().unwrap().expect("a catalog store has a shape");
    assert_eq!(s.unaccounted, 0, "the ledger does not close: {s:?}");
    let rows = |t: &str| s.rows.iter().find(|(n, _)| n == t).map(|(_, n)| *n).unwrap();
    let cells = |t: &str| s.census.iter().find(|(n, _)| n == t).map(|(_, c)| c.cells).unwrap();
    for (tree, table) in [
        ("meta", "meta"),
        ("branch", "branch"),
        ("branch_children", "branch"),
        ("branch_lease", "branch"),
        ("branch_released", "branch"),
        ("cur", "cur"),
        ("ret", "ret"),
        ("ret_page", "ret"),
        ("ret_born", "ret"),
        ("ret_died", "ret"),
        ("free", "free"),
    ] {
        assert_eq!(cells(tree), rows(table), "{tree}'s leaf cells against {table}'s rows: {s:?}");
    }
    assert_eq!(rows("branch"), 300, "premise: {s:?}");
    for (name, c) in &s.census {
        assert_eq!(c.overflow_cells, 0, "{name} spilled a cell: {c:?}");
        let levels = &s.trees.iter().find(|(n, _)| n == name).unwrap().1;
        assert_eq!(c.leaf_pages, *levels.last().unwrap(), "{name}'s leaves against its last level: {s:?}");
        assert!(c.used_bytes <= c.leaf_pages * s.page_size, "{name} uses more than its pages hold: {c:?}");
    }
}

/// Amendment 19's instrument, made to fire before it is trusted: the catalog shape's ledger closes
/// (every page is a tree page or a free-list page) on a grown catalog, its row counts are the
/// store's, and a mass reap moves pages onto the free list without shrinking the file (no
/// auto_vacuum), so a freelist count of zero in the run is a reading, not a blind spot.
#[test]
fn the_catalog_shape_closes_its_ledger_and_sees_freed_pages() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_at(&path, catalog()).unwrap();
    seed(&db.connect().unwrap());
    let ids = grow(&db, 300);
    db.branch_compact_now().unwrap();
    let grown = db.branch_catalog_shape().unwrap().expect("a catalog store has a shape");
    assert_eq!(grown.unaccounted, 0, "the ledger does not close: {grown:?}");
    let rows = |s: &CatalogShape, t: &str| s.rows.iter().find(|(n, _)| n == t).map(|(_, n)| *n);
    assert_eq!(rows(&grown, "branch"), Some(300), "{grown:?}");
    assert_eq!(rows(&grown, "meta"), Some(9), "{grown:?}");
    assert!(rows(&grown, "cur").unwrap() >= 300, "each branch wrote a page: {grown:?}");
    let cur = grown.trees.iter().find(|(n, _)| n == "cur").expect("the cur table's tree");
    assert_eq!(cur.1[0], 1, "a tree has one root: {grown:?}");
    assert!(cur.1.len() >= 2, "premise: the cur table outgrew one page: {grown:?}");
    for id in ids {
        db.branch(id).unwrap().reap().unwrap();
    }
    db.branch_compact_now().unwrap();
    let reaped = db.branch_catalog_shape().unwrap().unwrap();
    assert_eq!(reaped.unaccounted, 0, "the ledger does not close: {reaped:?}");
    assert_eq!(rows(&reaped, "branch"), Some(0), "{reaped:?}");
    assert!(reaped.freelist_count > 0, "a mass delete freed no page: {reaped:?}");
    assert_eq!(reaped.page_count, grown.page_count, "the file shrank without a vacuum: {reaped:?}");
}
