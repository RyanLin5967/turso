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

/// Every row of `t` as the connection sees it, through SQL. A Busy anywhere is named, not unwrapped.
fn all_rows(conn: &Arc<Connection>, who: &str) -> Vec<(i64, String)> {
    let rows = match conn
        .prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
    {
        Ok(rows) => rows,
        Err(LimboError::Busy) => panic!("{who}: the read returned Busy"),
        Err(e) => panic!("{who}: the read failed with {e}"),
    };
    rows.iter()
        .map(|r| match (r[0].as_int(), &r[1]) {
            (Some(id), Value::Text(v)) => (id, v.as_str().to_string()),
            other => panic!("{who}: unexpected row {other:?}"),
        })
        .collect()
}

/// The bytes of every page of the connection's database as its pager reads them (for a branch: its own page space,
/// the store's shared trunk-page cache, or the WAL / database file under its WAL read snapshot), bypassing the
/// connection's page cache, inside one read transaction.
fn page_bytes(conn: &Arc<Connection>, who: &str) -> Vec<Vec<u8>> {
    let n = conn
        .prepare("PRAGMA page_count")
        .unwrap()
        .run_collect_rows()
        .unwrap_or_else(|e| panic!("{who}: page_count failed with {e}"))[0][0]
        .as_int()
        .unwrap();
    let pager = conn.get_pager();
    pager
        .begin_read_tx()
        .unwrap_or_else(|e| panic!("{who}: begin_read_tx failed with {e}"));
    let mut out = Vec::new();
    for i in 1..=n {
        let (page, c) = pager.read_page_no_cache(i, None, false).unwrap();
        pager.io.wait_for_completion(c).unwrap();
        out.push(page.get_contents().as_slice().to_vec());
    }
    pager.end_read_tx();
    out
}

fn wal_len(dir: &tempfile::TempDir) -> u64 {
    std::fs::metadata(dir.path().join("branching.db-wal")).map_or(0, |m| m.len())
}

/// The masks every Z test runs under (amendment 28): none, Z alone, and Z with every fix a Z number is quoted with
/// (W,B,P,S,G,A,X,M,Y,U; H, R and V live in the harness). `force_fixes_for_test` REPLACES the process mask, so the
/// gate's TURSO_R11_FIX does not reach these tests: each mask must be named here.
const Z_MASKS: [u32; 3] = {
    use crate::coherence::*;
    [
        0,
        FIX_UARC,
        FIX_WAL | FIX_BUILTIN | FIX_POOL | FIX_STORE | FIX_ARC | FIX_GATE | FIX_COPYOUT | FIX_STRIPES | FIX_PAGER
            | FIX_ANCHOR | FIX_UARC,
    ]
};

/// r11-coherence Z (amendment 25): Z leaves branch connections out of `n_connections`, so the LAST TRUNK close runs
/// the shutdown checkpoint (TRUNCATE) while a branch connection is still open. Without Z the open branch connection
/// counts and the trunk's close does not checkpoint. Premise: the WAL file is truncated with Z, and not without.
/// Then the branch re-reads every row and every page, byte for byte, with no Busy; then a new trunk connection
/// rewrites every row into the restarted WAL (its frames start again at the WAL's beginning), and the open branch
/// connection and a fresh one still read the bytes of the fork. What keeps them: the checkpoint copies bytes without
/// changing them, and every trunk page a live branch sees is copied into the arena at the trunk's first write
/// (`Pager::copy_on_write_decision`).
#[test]
fn with_z_the_last_trunk_close_checkpoints_under_an_open_branch_and_the_branch_reads_the_same_bytes() {
    for mask in Z_MASKS {
        crate::coherence::force_fixes_for_test(mask);
        let (dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let rows = all_rows(&bc, "branch before the close");
        assert_eq!(rows.len() as i64, ROWS);
        let pages = page_bytes(&bc, "branch before the close");
        assert!(pages.len() > 2, "mask {mask}: premise: the table spans several pages");
        assert!(wal_len(&dir) > 0, "mask {mask}: premise: the seed is in the WAL before the close");

        trunk.close().unwrap();
        drop(trunk);
        let truncated = wal_len(&dir) == 0;
        assert_eq!(
            truncated,
            mask != 0,
            "mask {mask}: premise: the last trunk close truncates the WAL with Z (the branch does not count) and not \
             without Z (the open branch connection counts)"
        );

        assert_eq!(all_rows(&bc, "branch after the close"), rows, "mask {mask}: rows moved under the branch");
        assert!(
            page_bytes(&bc, "branch after the close") == pages,
            "mask {mask}: a page's bytes moved under the branch across the last trunk close"
        );

        let t2 = db.connect().unwrap();
        t2.execute("BEGIN").unwrap();
        for id in 1..=ROWS {
            set(&t2, id, "after-the-close");
        }
        t2.execute("COMMIT").unwrap();
        assert_eq!(value(&t2, 7), "after-the-close", "mask {mask}: premise: the trunk rewrote the rows");
        assert!(wal_len(&dir) > 0, "mask {mask}: premise: the rewrite is in the WAL");

        assert_eq!(all_rows(&bc, "open branch after the rewrite"), rows, "mask {mask}: the open branch saw the rewrite");
        assert!(
            page_bytes(&bc, "open branch after the rewrite") == pages,
            "mask {mask}: a page's bytes moved under the open branch after the trunk rewrote the restarted WAL"
        );
        // A branch serves one connection at a time: the fresh one opens after the open one closes.
        bc.close().unwrap();
        drop(bc);
        let bc2 = b.connect().unwrap();
        assert_eq!(all_rows(&bc2, "fresh branch after the rewrite"), rows, "mask {mask}: a fresh branch connection saw the rewrite");
        assert!(
            page_bytes(&bc2, "fresh branch after the rewrite") == pages,
            "mask {mask}: a fresh branch connection read different bytes"
        );
        drop((bc2, t2));
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}

/// r11-coherence Z (amendment 25): the last trunk close while a branch statement is part-way through its rows. The
/// branch's WAL read lock is what keeps the frames it reads: with Z the close's TRUNCATE checkpoint must find the
/// lock and leave the WAL (it retries three times on Busy and gives up without an error), so the WAL is not
/// truncated while the statement is open, and the statement finishes with the fork's rows. Red with Z if a branch
/// statement reads without holding the WAL read lock.
#[test]
fn with_z_the_last_trunk_close_leaves_the_wal_a_branch_statement_is_reading() {
    for mask in Z_MASKS {
        crate::coherence::force_fixes_for_test(mask);
        let (dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let rows = all_rows(&bc, "branch before the close");
        let mut st = bc.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        let mut got = Vec::new();
        let take = |st: &mut crate::Statement, got: &mut Vec<(i64, String)>, limit: Option<usize>| loop {
            if limit.is_some_and(|l| got.len() >= l) {
                return;
            }
            match st.step() {
                Ok(crate::StepResult::Row) => {
                    let r: Vec<Value> = st.row().unwrap().get_values().cloned().collect();
                    match (r[0].as_int(), &r[1]) {
                        (Some(id), Value::Text(v)) => got.push((id, v.as_str().to_string())),
                        other => panic!("mask {mask}: unexpected row {other:?}"),
                    }
                }
                Ok(crate::StepResult::Done) => return,
                Ok(crate::StepResult::IO | crate::StepResult::Yield | crate::StepResult::Sleep { .. }) => db.io.step().unwrap(),
                Ok(crate::StepResult::Busy | crate::StepResult::Interrupt) => panic!("mask {mask}: the branch statement returned Busy"),
                Err(e) => panic!("mask {mask}: the branch statement failed with {e}"),
            }
        };
        take(&mut st, &mut got, Some(1));
        assert_eq!(got.len(), 1, "mask {mask}: premise: the statement is part-way through");
        assert!(wal_len(&dir) > 0, "mask {mask}: premise: the seed is in the WAL");

        trunk.close().unwrap();
        drop(trunk);
        assert!(
            wal_len(&dir) > 0,
            "mask {mask}: the last trunk close truncated the WAL while a branch statement was reading it"
        );
        take(&mut st, &mut got, None);
        assert_eq!(got, rows, "mask {mask}: the branch statement's rows moved across the last trunk close");
        drop(st);
        assert_eq!(all_rows(&bc, "branch after its statement"), rows);
        drop(bc);
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}

/// r11-coherence Z (amendment 25, P25.4): as the first Z test, but the branch reads NOTHING before the last trunk close
/// and the rewrite, so no page of the fork sits in the branch's page cache or in the store's shared trunk-page cache.
/// Every byte it reads afterwards must come from the trunk's first-write pre-images in the arena or from the database
/// file. The expected rows are written out, not read from the subject.
#[test]
fn with_z_a_branch_that_read_nothing_reads_its_fork_after_the_last_trunk_close_and_a_rewrite() {
    for mask in Z_MASKS {
        crate::coherence::force_fixes_for_test(mask);
        let (dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let expected: Vec<(i64, String)> = (1..=ROWS).map(|id| (id, original(id))).collect();
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        trunk.close().unwrap();
        drop(trunk);
        assert_eq!(
            wal_len(&dir) == 0,
            mask != 0,
            "mask {mask}: premise: the last trunk close truncates the WAL with Z and not without"
        );
        let t2 = db.connect().unwrap();
        t2.execute("BEGIN").unwrap();
        for id in 1..=ROWS {
            set(&t2, id, "after-the-close");
        }
        t2.execute("COMMIT").unwrap();
        assert_eq!(value(&t2, 7), "after-the-close", "mask {mask}: premise: the trunk rewrote the rows");
        assert_eq!(all_rows(&bc, "open branch after the rewrite"), expected, "mask {mask}: the branch saw the rewrite");
        bc.close().unwrap();
        drop(bc);
        let bc2 = b.connect().unwrap();
        assert_eq!(all_rows(&bc2, "fresh branch after the rewrite"), expected, "mask {mask}: a fresh branch connection saw the rewrite");
        drop((bc2, t2));
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}

/// r11-coherence Z (amendment 28, T25d): the one read that is new under Z. A branch connection holds a WAL snapshot (it
/// read one row of `t`) when the last trunk close runs; afterwards it reads table `u`, which nothing read before the
/// close and nothing writes after the fork, so no arena slot and no cache holds u's pages: with Z the WAL is empty
/// and they come from the database file the checkpoint backfilled. Premise: the store's trunk-page misses rise during
/// that read. Then a new trunk connection rewrites `t` into the restarted WAL (frame numbers reused) and the branch
/// reads `u` again. Expected rows are written out, not read from the subject.
#[test]
fn with_z_an_open_branch_reads_an_untouched_table_through_the_backfilled_file() {
    fn other(id: i64) -> String {
        format!("u-{id:04}-{}", "y".repeat(90))
    }
    fn u_rows(conn: &Arc<Connection>, who: &str) -> Vec<(i64, String)> {
        let rows = match conn.prepare("SELECT id, w FROM u ORDER BY id").unwrap().run_collect_rows() {
            Ok(rows) => rows,
            Err(LimboError::Busy) => panic!("{who}: the read of u returned Busy"),
            Err(e) => panic!("{who}: the read of u failed with {e}"),
        };
        rows.iter()
            .map(|r| match (r[0].as_int(), &r[1]) {
                (Some(id), Value::Text(v)) => (id, v.as_str().to_string()),
                other => panic!("{who}: unexpected row {other:?}"),
            })
            .collect()
    }
    for mask in Z_MASKS {
        crate::coherence::force_fixes_for_test(mask);
        let (dir, db) = open_db();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, w TEXT)").unwrap();
        trunk.execute("BEGIN").unwrap();
        for id in 1..=ROWS {
            trunk.execute(format!("INSERT INTO u VALUES ({id}, '{}')", other(id))).unwrap();
        }
        trunk.execute("COMMIT").unwrap();
        let expected: Vec<(i64, String)> = (1..=ROWS).map(|id| (id, other(id))).collect();
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        assert_eq!(value(&bc, 7), original(7), "mask {mask}: premise: the branch reads t before the close");
        trunk.close().unwrap();
        drop(trunk);
        assert_eq!(
            wal_len(&dir) == 0,
            mask != 0,
            "mask {mask}: premise: the last trunk close truncates the WAL with Z and not without"
        );
        let misses = db.branch_stats().work.trunk_page_misses;
        assert_eq!(u_rows(&bc, "open branch, u after the close"), expected, "mask {mask}: u's rows moved across the close");
        assert!(
            db.branch_stats().work.trunk_page_misses > misses,
            "mask {mask}: premise: u's pages were read through the WAL or the database file, not a cache"
        );
        let t2 = db.connect().unwrap();
        t2.execute("BEGIN").unwrap();
        for id in 1..=ROWS {
            set(&t2, id, "after-the-close");
        }
        t2.execute("COMMIT").unwrap();
        assert_eq!(value(&t2, 7), "after-the-close", "mask {mask}: premise: the trunk rewrote t");
        assert_eq!(u_rows(&bc, "open branch, u after the rewrite"), expected, "mask {mask}: u's rows moved after the rewrite");
        assert_eq!(value(&bc, 7), original(7), "mask {mask}: the branch saw the trunk's rewrite of t");
        drop((bc, t2));
        drop(b);
    }
    crate::coherence::force_fixes_for_test(0);
}
