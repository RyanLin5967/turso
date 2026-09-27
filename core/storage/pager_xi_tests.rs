//! F-XI (lane r12-phasefair, PREREG amendment 5): a trunk connection whose read transaction follows another
//! connection's commit evicts only the pages that commit rewrote, not its whole cache.
//!
//! T1 is the red test: without F-XI every such read transaction empties the cache, so page 1 comes back as a new
//! page object. T2-T4 read every row back through SQL against a model the test keeps itself, so a kept page that is
//! stale shows as a wrong value, never through the mechanism's own bookkeeping. Each test forces F-XI on for the
//! pagers it names; `XI_MUTANT` (see `xi_mutant`) selects a mutant of the mechanism in test builds.

use super::*;
use crate::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::{BTreeMap, BTreeSet};

const ROWS: i64 = 200;
/// More than the pages the seeded table spans, so a scan of the cache by page number sees every cached page.
const MAX_PAGE: usize = 256;

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("xi.db");
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

/// Rows padded to ~100 bytes so the table spans several leaves: row 1 and row ROWS sit on different pages.
fn original(id: i64) -> String {
    format!("row-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>, model: &mut BTreeMap<i64, String>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=ROWS {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
        model.insert(id, original(id));
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

/// Every row, in id order, read through SQL on `conn`.
fn all_rows(conn: &Arc<Connection>) -> BTreeMap<i64, String> {
    let rows = conn
        .prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.iter()
        .map(|r| match (r[0].as_int(), &r[1]) {
            (Some(id), Value::Text(v)) => (id, v.as_str().to_string()),
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

fn set(conn: &Arc<Connection>, model: &mut BTreeMap<i64, String>, id: i64, v: String) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
    model.insert(id, v);
}

fn insert(conn: &Arc<Connection>, model: &mut BTreeMap<i64, String>, id: i64, v: String) {
    conn.execute(format!("INSERT INTO t VALUES ({id}, '{v}')"))
        .unwrap();
    model.insert(id, v);
}

/// `conn`'s pager, with F-XI forced on.
fn xi_on(conn: &Arc<Connection>) -> Arc<Pager> {
    let pager = conn.pager.load().clone();
    pager.xi.store(true, Ordering::Relaxed);
    pager
}

fn cached(pager: &Pager, page: usize) -> Option<PageRef> {
    pager.page_cache.write().peek(&PageCacheKey::new(page), false)
}

fn cached_pages(pager: &Pager) -> BTreeSet<usize> {
    (1..=MAX_PAGE).filter(|&p| cached(pager, p).is_some()).collect()
}

fn assert_rows(conn: &Arc<Connection>, model: &BTreeMap<i64, String>, what: &str) {
    let got = all_rows(conn);
    assert_eq!(got.len(), model.len(), "{what}: row count");
    for (id, want) in model {
        assert_eq!(got.get(id), Some(want), "{what}: row {id}");
    }
}

/// T1 (red without F-XI): B rewrites a row on another leaf; A's next read transaction keeps page 1 (the same page
/// object) and every page B did not rewrite, and drops the leaf B rewrote.
#[test]
fn xi_a_commit_that_misses_page_1_keeps_it_cached() {
    let (_dir, db) = open_db();
    let b = db.connect().unwrap();
    let mut model = BTreeMap::new();
    seed(&b, &mut model);
    let a = db.connect().unwrap();
    let pager = xi_on(&a);
    assert_eq!(value(&a, 1), model[&1]);
    assert_eq!(value(&a, ROWS), model[&ROWS]);
    let before = cached_pages(&pager);
    let page_1 = cached(&pager, 1).expect("A's reads cache page 1");
    // Same length as the original, so the rewrite stays on its leaf and rewrites that one page.
    set(&b, &mut model, ROWS, format!("rewritten-{}", "y".repeat(89)));
    pager.begin_read_tx().unwrap();
    let after = cached_pages(&pager);
    let page_1_after = cached(&pager, 1);
    pager.end_read_tx();
    assert!(
        page_1_after.is_some_and(|p| Arc::ptr_eq(&p, &page_1)),
        "page 1 must stay cached, the same page, across a commit that did not rewrite it"
    );
    let dropped: BTreeSet<usize> = before.difference(&after).copied().collect();
    assert!(after.is_subset(&before), "a read tx adds no page before it reads one: {before:?} -> {after:?}");
    assert_eq!(dropped.len(), 1, "exactly the rewritten leaf is dropped: {before:?} -> {after:?}");
    assert_eq!(value(&a, ROWS), model[&ROWS], "the dropped leaf is read again, rewritten");
    assert_eq!(value(&a, 1), model[&1]);
}

/// T2: rounds of B's single-row updates (one frame each), multi-row updates and inserts that split pages and grow the
/// file (page 1's header changes); after every round A reads every row, and a single-commit round checks the first new
/// frame exactly.
#[test]
fn xi_every_trunk_connection_reads_what_was_committed() {
    let (_dir, db) = open_db();
    let b = db.connect().unwrap();
    let mut model = BTreeMap::new();
    seed(&b, &mut model);
    let a = db.connect().unwrap();
    xi_on(&a);
    assert_rows(&a, &model, "after seed");
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut next_id = ROWS + 1;
    for round in 0..40 {
        match round % 4 {
            // One commit, one row: its frame is the first and only new frame A's next read tx sees.
            0 => {
                let id = next(ROWS as u64) as i64 + 1;
                set(&b, &mut model, id, format!("r{round}-{id}-{}", "z".repeat(80)));
            }
            1 => {
                for k in 0..12 {
                    let id = next(ROWS as u64) as i64 + 1;
                    set(&b, &mut model, id, format!("r{round}-{k}-{id}-{}", "w".repeat(80)));
                }
            }
            2 => {
                for _ in 0..30 {
                    insert(&b, &mut model, next_id, format!("new-{next_id}-{}", "n".repeat(90)));
                    next_id += 1;
                }
            }
            _ => {
                b.execute("BEGIN").unwrap();
                for k in 0..8 {
                    let id = next(next_id as u64 - 1) as i64 + 1;
                    set(&b, &mut model, id, format!("tx{round}-{k}-{id}-{}", "t".repeat(80)));
                }
                b.execute("COMMIT").unwrap();
            }
        }
        assert_rows(&a, &model, &format!("round {round}"));
    }
}

/// T3: a TRUNCATE checkpoint restarts the log between A's reads; B then rewrites row 1's leaf first and another leaf
/// more times than A's old snapshot had frames, so frame numbers alone would call row 1's leaf unchanged.
#[test]
fn xi_a_log_restart_empties_the_cache() {
    let (_dir, db) = open_db();
    let b = db.connect().unwrap();
    let mut model = BTreeMap::new();
    seed(&b, &mut model);
    let a = db.connect().unwrap();
    xi_on(&a);
    assert_rows(&a, &model, "after seed");
    let rows = b
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].as_int(), Some(0), "the TRUNCATE checkpoint must not be refused: {rows:?}");
    set(&b, &mut model, 1, format!("after-restart-{}", "r".repeat(80)));
    for k in 0..64 {
        set(&b, &mut model, ROWS, format!("again-{k}-{}", "a".repeat(80)));
    }
    assert_eq!(value(&a, 1), model[&1], "row 1 after the restart");
    assert_rows(&a, &model, "after the restart");
}

/// T4: a PASSIVE checkpoint backfills between A's reads without restarting the log. The read tx after it sees a
/// snapshot that moved only by the backfill: A keeps its cache (page 1 the same page) and reads every row right.
#[test]
fn xi_a_backfill_alone_keeps_the_cache() {
    let (_dir, db) = open_db();
    let b = db.connect().unwrap();
    let mut model = BTreeMap::new();
    seed(&b, &mut model);
    let a = db.connect().unwrap();
    let pager = xi_on(&a);
    assert_rows(&a, &model, "after seed");
    set(&b, &mut model, 1, format!("before-backfill-{}", "p".repeat(80)));
    assert_eq!(value(&a, 1), model[&1], "row 1 after B's commit");
    let page_1 = cached(&pager, 1).expect("page 1 cached");
    let rows = b
        .prepare("PRAGMA wal_checkpoint(PASSIVE)")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows[0][0].as_int(), Some(0), "the PASSIVE checkpoint must not be refused: {rows:?}");
    assert_eq!(value(&a, 2), model[&2], "row 2 after the backfill");
    assert!(
        cached(&pager, 1).is_some_and(|p| Arc::ptr_eq(&p, &page_1)),
        "a backfill rewrites no page's latest version: page 1 stays the same page"
    );
    assert_rows(&a, &model, "after the backfill");
    set(&b, &mut model, ROWS, format!("after-backfill-{}", "q".repeat(80)));
    assert_rows(&a, &model, "after the next commit");
}
