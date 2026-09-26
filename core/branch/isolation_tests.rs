//! The specification for branching, written before the mechanism.
//!
//! Every assertion here reads the database back through SQL on a connection, never through the
//! branch machinery's own bookkeeping, so a test cannot pass by agreeing with the thing it checks.
//! Where an isolation claim could be satisfied by a connection's private page cache alone, the
//! test also reads through a FRESH connection: per-connection caches already isolate unflushed
//! pages, so a check on the writing connection's own cache would pass with no branching at all.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const ROWS: i64 = 200;

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branching.db");
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

/// Rows padded to ~100 bytes so the table spans many leaf pages: a one-page table would make
/// "write one page" and "write the table" the same event and hide a whole class of bug.
fn original(id: i64) -> String {
    format!("trunk-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=ROWS {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn value(conn: &Arc<Connection>, id: i64) -> String {
    let rows = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1, "row {id} must exist exactly once");
    match &rows[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("row {id}: expected text, got {other:?}"),
    }
}

fn count(conn: &Arc<Connection>) -> i64 {
    let rows = conn
        .prepare("SELECT count(*) FROM t")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows[0][0]
        .as_int()
        .unwrap_or_else(|| panic!("count(*): expected integer, got {:?}", rows[0][0]))
}

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
}

#[test]
fn a_branch_write_is_invisible_to_its_parent() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);

    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 7, "branch");
    // The write happened: without this the rest could pass on a branch that dropped it.
    assert_eq!(value(&bc, 7), "branch");

    assert_eq!(value(&trunk, 7), original(7), "trunk saw the branch's write");
    let fresh = db.connect().unwrap();
    assert_eq!(value(&fresh, 7), original(7), "a fresh trunk connection saw the branch's write");

    // The write lives in the BRANCH, not in the writing connection's cache: reopen and read it.
    drop(bc);
    let bc2 = b.connect().unwrap();
    assert_eq!(value(&bc2, 7), "branch", "the branch lost its own committed write");
    assert_eq!(count(&bc2), ROWS);
}

#[test]
fn a_branch_write_is_invisible_to_a_sibling() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);

    let b1 = trunk.fork_branch().unwrap();
    let b2 = trunk.fork_branch().unwrap();
    assert_ne!(b1.id(), b2.id());

    let c1 = b1.connect().unwrap();
    set(&c1, 7, "b1");
    let c2 = b2.connect().unwrap();
    assert_eq!(value(&c2, 7), original(7), "sibling saw b1's write");
    set(&c2, 8, "b2");
    assert_eq!(value(&c1, 8), original(8), "b1 saw its sibling's write");

    drop((c1, c2));
    let c1 = b1.connect().unwrap();
    let c2 = b2.connect().unwrap();
    assert_eq!(value(&c1, 7), "b1");
    assert_eq!(value(&c1, 8), original(8));
    assert_eq!(value(&c2, 7), original(7));
    assert_eq!(value(&c2, 8), "b2");
}

#[test]
fn a_parent_write_after_the_fork_is_invisible_to_the_branch() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);

    let b = trunk.fork_branch().unwrap();

    set(&trunk, 7, "trunk-after-fork");
    // Growth, not just an in-place update: new pages, interior-page splits, a changed page 1.
    trunk.execute("BEGIN").unwrap();
    for id in ROWS + 1..=ROWS + 500 {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    assert_eq!(value(&trunk, 7), "trunk-after-fork");
    assert_eq!(count(&trunk), ROWS + 500);

    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 7), original(7), "branch saw a parent write made after the fork");
    assert_eq!(count(&bc), ROWS, "branch saw rows the parent inserted after the fork");

    // The checkpoint moves the parent's post-fork pages from the WAL into the database file. A
    // branch that was only protected by reading an older WAL snapshot loses that protection here.
    trunk
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    drop(bc);
    let bc = b.connect().unwrap();
    assert_eq!(value(&bc, 7), original(7), "checkpoint leaked a parent write into the branch");
    assert_eq!(count(&bc), ROWS, "checkpoint leaked parent rows into the branch");
}

#[test]
fn a_branch_parent_write_after_the_fork_is_invisible_to_its_child() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);

    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 7, "b-before-fork");
    let c = b.fork().unwrap();
    set(&bc, 7, "b-after-fork");
    set(&bc, 8, "b-after-fork");

    let cc = c.connect().unwrap();
    assert_eq!(value(&cc, 7), "b-before-fork", "child must see its parent as of the fork");
    assert_eq!(value(&cc, 8), original(8), "child saw a parent write made after the fork");
    set(&cc, 9, "c");
    assert_eq!(value(&bc, 9), original(9), "parent saw its child's write");
    assert_eq!(value(&trunk, 9), original(9), "trunk saw a grandchild's write");
    assert_eq!(value(&trunk, 7), original(7), "trunk saw a branch's write");
}

#[test]
fn reaping_a_branch_frees_its_pages() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let base = db.branch_stats();

    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 7, "branch");
    set(&bc, ROWS, "branch");
    drop(bc);

    let owned = b.owned_slots();
    let held = db.branch_stats();
    // The branch really holds pages of its own; a reap that frees nothing is only a pass if there
    // was nothing to free, and that is exactly the case this rules out.
    assert!(!owned.is_empty(), "a branch that wrote owns no pages");
    assert_eq!(
        held.arena_slots_in_use,
        base.arena_slots_in_use + owned.len(),
        "the arena's count disagrees with the branch's own page list"
    );

    let reaped = b.reap().unwrap();
    assert!(!reaped.deferred);
    assert_eq!(reaped.freed_pages, owned.len());
    // Membership, not a net count: a leak and a double release would cancel in a count.
    for slot in &owned {
        assert!(db.branch_slot_is_free(*slot), "slot {slot} was not freed");
    }
    assert_eq!(db.branch_stats().arena_slots_in_use, base.arena_slots_in_use);
    assert_eq!(db.branch_stats().live_branches, base.live_branches);
}

#[test]
fn dropping_a_branch_frees_its_pages() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let base = db.branch_stats();

    let b = trunk.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    set(&bc, 7, "branch");
    drop(bc);
    let owned = b.owned_slots();
    assert!(!owned.is_empty(), "a branch that wrote owns no pages");

    drop(b);
    for slot in &owned {
        assert!(db.branch_slot_is_free(*slot), "slot {slot} was not freed");
    }
    assert_eq!(db.branch_stats().arena_slots_in_use, base.arena_slots_in_use);
    assert_eq!(db.branch_stats().live_branches, base.live_branches);
}

#[test]
fn forking_an_mvcc_database_is_refused() {
    let (_dir, db) = open_db();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    let err = match trunk.fork_branch() {
        Ok(_) => panic!("an MVCC-mode database must not be branchable"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("experimental_mvcc"), "unexpected refusal: {err}");
}

/// r11-coherence FG fire-check (amendment 16): with the fork gate, a trunk fork is refused while a trunk write
/// transaction is open, exactly as the WAL write lock refused it before the gate. Red if a trunk write transaction
/// does not take the gate (`fork_gate_write_enter` removed from `begin_write_tx`): the fork would then run inside
/// the writer's transaction, whose copy decisions were taken for the previous epoch.
#[test]
fn with_the_fork_gate_a_trunk_fork_waits_for_an_open_trunk_write() {
    for mask in [0, crate::coherence::FIX_GATE] {
        crate::coherence::force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let forker = db.connect().unwrap();
        trunk.execute("BEGIN").unwrap();
        set(&trunk, 7, "in-flight");
        match forker.fork_branch() {
            Err(LimboError::Busy) => {}
            Err(e) => panic!("mask {mask}: fork during an open trunk write failed with {e}, expected Busy"),
            Ok(_) => panic!("mask {mask}: a trunk fork ran inside an open trunk write transaction"),
        }
        trunk.execute("COMMIT").unwrap();
        let b = forker.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        assert_eq!(value(&bc, 7), "in-flight", "mask {mask}: a fork after the commit must see it");
        drop(bc);
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}

/// r11-coherence FK (amendment 16): the schedule r11-coherence's model finds stale when a trunk write that meets no
/// live child is not stamped (model/k_cond_a.py, FIRE_K_without_stamping), replayed deterministically: A, the trunk's
/// only child, has the page cached; A is reaped, and between its depart and its generation bump the trunk rewrites
/// the row, B forks, and B reads it. B must read the new value. With FK every trunk write stamps `written`, so B's
/// cache key is not A's; red if FK's writes are stamped only while a child lives.
#[test]
fn with_fk_a_fork_between_the_last_depart_and_the_bump_reads_the_new_trunk_page() {
    for mask in [0, crate::coherence::FIX_TRUNKIDX] {
        crate::coherence::force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let a = trunk.fork_branch().unwrap();
        let ac = a.connect().unwrap();
        assert_eq!(value(&ac, 7), original(7));
        drop(ac);
        let seen = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
        // The hook runs only on FK's reap path; without FK the reap bumps the generation under the trunk lock.
        if mask != 0 {
            let (trunk, seen) = (trunk.clone(), seen.clone());
            crate::branch::store::set_k_depart_hook_for_test(Box::new(move || {
                set(&trunk, 7, "trunk-after-the-last-depart");
                let b = trunk.fork_branch().unwrap();
                let bc = b.connect().unwrap();
                *seen.lock().unwrap() = Some(value(&bc, 7));
                drop(bc);
                drop(b);
            }));
        }
        a.reap().unwrap();
        if mask == 0 {
            set(&trunk, 7, "trunk-after-the-last-depart");
        } else {
            assert_eq!(
                seen.lock().unwrap().as_deref(),
                Some("trunk-after-the-last-depart"),
                "a fork inside the reap's depart window read the page A had cached"
            );
        }
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        assert_eq!(value(&bc, 7), "trunk-after-the-last-depart");
        drop(bc);
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}
