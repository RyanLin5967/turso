//! FL (r11-forklock's F-L, ported for r12-phasefair's SOTA arm; r12-phasefair PREREG section "F-L (r11-forklock)"):
//! trunk forks without the WAL write lock and without FG's gate, through SQL.
//!
//! Every test runs under the masks of [`fl_masks`]: FL alone, FL with FG, and FL with E1's engine letters. A mask is
//! forced on every thread that constructs anything (`force_fixes_for_test` is per thread). `FL_RED=1` removes L from
//! every mask: the red column, run from the same binary, where the fork path is 4bfbeda7c's.

use super::*;
use crate::coherence::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::BTreeMap;

/// E1's engine letters (W,B,P,S,G,A,X,M,Y,U,Z; H lives in the harness).
const E1_ALL: u32 = FIX_WAL
    | FIX_BUILTIN
    | FIX_POOL
    | FIX_STORE
    | FIX_GATE
    | FIX_ARC
    | FIX_COPYOUT
    | FIX_STRIPES
    | FIX_PAGER
    | FIX_ANCHOR
    | FIX_UARC;

fn fl_masks() -> [u32; 3] {
    let l = if std::env::var("FL_RED").is_ok_and(|v| v == "1") {
        0
    } else {
        FIX_FORKOCC
    };
    [l, FIX_GATE | l, E1_ALL | l]
}

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

fn original(id: i64) -> String {
    format!("trunk-{id:04}-{}", "x".repeat(90))
}

fn seed(conn: &Arc<Connection>, rows: i64) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=rows {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", original(id)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

fn value(conn: &Arc<Connection>, id: i64) -> Option<String> {
    let rows = conn
        .prepare(format!("SELECT v FROM t WHERE id = {id}"))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert!(rows.len() <= 1);
    rows.first().map(|row| match &row[0] {
        Value::Text(t) => t.as_str().to_string(),
        other => panic!("expected text, got {other:?}"),
    })
}

fn set(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))
        .unwrap();
}

fn exec_retrying_busy(conn: &Arc<Connection>, sql: &str) {
    for _ in 0..10_000 {
        match conn.execute(sql) {
            Ok(()) => return,
            Err(LimboError::Busy | LimboError::BusySnapshot) => std::thread::yield_now(),
            Err(e) => panic!("{sql}: {e}"),
        }
    }
    panic!("{sql}: still Busy after 10,000 attempts");
}

/// K10-1: a fork while a trunk write transaction is open is ADMITTED, and never sees that transaction. Child A
/// exists before the transaction (the trunk's first child forks the old way); B is forked inside it and read inside
/// it, which fills the shared trunk-page cache with the pages the transaction is rewriting; C is forked inside it
/// after that. After the commit, A, B and C read every rewritten row as it was, on two connections, and a fork D after
/// the commit reads the new rows, never the versions B cached. RED without L: FG's gate or the WAL write lock
/// refuses B with Busy.
#[test]
fn fl_a_fork_during_an_open_trunk_transaction_is_admitted_and_never_sees_it() {
    for mask in fl_masks() {
        force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let writer = db.connect().unwrap();
        seed(&writer, 200);
        let forker = db.connect().unwrap();
        // Three rows on three different leaves (about 37 rows fit a leaf).
        let ids = [7, 100, 190];
        let a = forker.fork_branch().unwrap();
        writer.execute("BEGIN").unwrap();
        for id in ids {
            set(&writer, id, &format!("in-flight-{id}"));
        }
        let b = forker.fork_branch().unwrap_or_else(|e| {
            panic!("mask {mask}: a fork while a trunk write is open must be admitted, got {e}")
        });
        let bc = b.connect().unwrap();
        for id in ids {
            assert_eq!(value(&bc, id), Some(original(id)), "mask {mask}: B, inside the transaction");
        }
        drop(bc);
        let c = forker.fork_branch().unwrap_or_else(|e| {
            panic!("mask {mask}: a fork while a trunk write is open must be admitted, got {e}")
        });
        writer.execute("COMMIT").unwrap();
        for (name, branch) in [("A", &a), ("B", &b), ("C", &c)] {
            for _ in 0..2 {
                let conn = branch.connect().unwrap();
                for id in ids {
                    assert_eq!(
                        value(&conn, id),
                        Some(original(id)),
                        "mask {mask}: {name} sees a trunk transaction that committed after its fork"
                    );
                }
            }
        }
        let d = forker.fork_branch().unwrap();
        let dc = d.connect().unwrap();
        for id in ids {
            assert_eq!(
                value(&dc, id),
                Some(format!("in-flight-{id}")),
                "mask {mask}: a fork after the commit was served a version cached before it"
            );
        }
        let work = db.branch_stats().work;
        assert_eq!(
            (work.trunk_forks_fast, work.trunk_forks_locked),
            (3, 1),
            "mask {mask}: B, C and D lock-free; A, the first child, the old way"
        );
        assert!(
            work.trunk_pre_images_retained >= 3,
            "mask {mask}: the commit retained nothing: {work:?}"
        );
    }
    force_fixes_for_test(0);
}

/// K10-2: the commit's decision for a page last committed in the epoch just before its own. C0 rewrites row 7 with a
/// child alive; then, with no fork in between, C1 opens and rewrites it again, X is forked inside C1, and C1 commits.
/// X must read C0's row: not the original, which only the first child keeps, and not C1's.
#[test]
fn fl_a_fork_inside_a_transaction_reads_the_commit_just_before_it() {
    for mask in fl_masks() {
        force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let writer = db.connect().unwrap();
        seed(&writer, 200);
        let forker = db.connect().unwrap();
        let first = forker.fork_branch().unwrap();
        set(&writer, 7, "c0");
        writer.execute("BEGIN").unwrap();
        set(&writer, 7, "c1");
        let x = forker
            .fork_branch()
            .unwrap_or_else(|e| panic!("mask {mask}: a fork inside C1 must be admitted, got {e}"));
        writer.execute("COMMIT").unwrap();
        assert_eq!(value(&x.connect().unwrap(), 7).as_deref(), Some("c0"), "mask {mask}");
        assert_eq!(value(&first.connect().unwrap(), 7), Some(original(7)), "mask {mask}");
        let after = forker.fork_branch().unwrap();
        assert_eq!(value(&after.connect().unwrap(), 7).as_deref(), Some("c1"), "mask {mask}");
    }
    force_fixes_for_test(0);
}

/// K10-3: a commit that retains nothing still stamps its pages with its epoch, so a version the shared trunk-page
/// cache holds for an older epoch never reaches a later fork. X (forked before the row's last commit) keeps the
/// trunk's child count above zero, so no generation bump hides the defect; Y, forked after that commit, reads the
/// row (caching its page) and is reaped; the trunk rewrites the row with no live child able to see the old version;
/// Z, forked after, must read the new row.
#[test]
fn fl_a_commit_that_retains_nothing_still_restamps_so_a_cached_page_never_reaches_a_later_fork() {
    for mask in fl_masks() {
        force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let writer = db.connect().unwrap();
        seed(&writer, 200);
        let forker = db.connect().unwrap();
        let x = forker.fork_branch().unwrap();
        set(&writer, 7, "v1");
        let y = forker.fork_branch().unwrap();
        assert_eq!(value(&y.connect().unwrap(), 7).as_deref(), Some("v1"), "mask {mask}");
        drop(y);
        let before = db.branch_stats().work;
        set(&writer, 7, "v2");
        let after = db.branch_stats().work;
        assert_eq!(
            after.trunk_pre_images_retained, before.trunk_pre_images_retained,
            "mask {mask}: the fixture is wrong: the second rewrite retained a version"
        );
        let z = forker.fork_branch().unwrap();
        assert_eq!(value(&z.connect().unwrap(), 7).as_deref(), Some("v2"), "mask {mask}");
        assert_eq!(value(&x.connect().unwrap(), 7), Some(original(7)), "mask {mask}");
    }
    force_fixes_for_test(0);
}

/// K10-6, the interleaving of r11-fi-refute-code's model (snzi_f6.py): the last child goes after caching a page (the
/// generation moves on), the trunk opens a write transaction that rewrites the page with no child alive (nothing
/// captured), and a fork races it. A fork inside the transaction is the trunk's first child, so it must answer Busy
/// (it forks the old way, excluding trunk writers) or, if admitted, read the page as it was; a fork after the commit
/// must read the new row, never the page cached under the old generation. With the racing fork inside the
/// transaction and after the commit.
#[test]
fn fl_a_fork_racing_the_first_trunk_write_after_the_last_child_goes_never_reads_a_stale_page() {
    for mask in fl_masks() {
        for fork_inside in [true, false] {
            force_fixes_for_test(mask);
            let (_dir, db) = open_db();
            let writer = db.connect().unwrap();
            seed(&writer, 200);
            let forker = db.connect().unwrap();
            let a = forker.fork_branch().unwrap();
            assert_eq!(value(&a.connect().unwrap(), 7), Some(original(7)));
            drop(a);
            assert_eq!(db.branch_stats().live_branches, 0);
            writer.execute("BEGIN").unwrap();
            set(&writer, 7, "after-the-last-child");
            let inside = if fork_inside {
                match forker.fork_branch() {
                    Ok(b) => Some(b),
                    Err(LimboError::Busy) => None,
                    Err(e) => panic!("mask {mask}: a fork inside the write failed: {e}"),
                }
            } else {
                None
            };
            writer.execute("COMMIT").unwrap();
            if let Some(b) = &inside {
                assert_eq!(
                    value(&b.connect().unwrap(), 7),
                    Some(original(7)),
                    "mask {mask}: a fork inside the trunk's write saw it"
                );
            }
            let c = forker.fork_branch().unwrap();
            assert_eq!(
                value(&c.connect().unwrap(), 7).as_deref(),
                Some("after-the-last-child"),
                "mask {mask}: a fork after the write was served the page cached under the old generation \
                 (fork_inside={fork_inside})"
            );
        }
    }
    force_fixes_for_test(0);
}

/// K10-5: lock-free forks racing a trunk writer under threads. The writer commits transactions that set 8 rows on 8
/// different leaves to its transaction number k, back to back. Four threads fork continuously, each reading the
/// writer's clock around its fork (`done`, commits returned, before the call; `started`, transactions begun, after
/// it returns) and reading each branch at once, while the writer keeps committing, and again at the end: every read
/// must show one k in all 8 rows, with done <= k <= started, the same k every time. Every thread forces the mask.
#[test]
fn fl_lock_free_forks_racing_a_trunk_writer_read_one_committed_state_each() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    const FORKERS: usize = 4;
    const FORKS: usize = 40;
    for mask in fl_masks() {
        force_fixes_for_test(mask);
        let ids: Vec<i64> = (0..8).map(|i| 7 + 50 * i).collect();
        let (_dir, db) = open_db();
        let writer = db.connect().unwrap();
        seed(&writer, 400);
        let first = writer.fork_branch().unwrap();
        let started = AtomicU64::new(0);
        let done = AtomicU64::new(0);
        let stop = AtomicBool::new(false);
        let k_of = |id: i64, v: &str| -> u64 {
            if v == original(id) {
                0
            } else {
                v.strip_prefix('k')
                    .and_then(|k| k.parse().ok())
                    .unwrap_or_else(|| panic!("mask {mask}: row {id} holds {v}"))
            }
        };
        let read_k = |branch: &Branch| -> u64 {
            let conn = branch.connect().unwrap();
            let ks: Vec<u64> = ids
                .iter()
                .map(|&id| k_of(id, &value(&conn, id).unwrap()))
                .collect();
            assert!(ks.iter().all(|&k| k == ks[0]), "mask {mask}: a torn trunk state: {ks:?}");
            ks[0]
        };
        let checked = std::thread::scope(|s| {
            let w = s.spawn(|| {
                force_fixes_for_test(mask);
                let mut commits = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let k = started.fetch_add(1, Ordering::AcqRel) + 1;
                    exec_retrying_busy(&writer, "BEGIN IMMEDIATE");
                    for &id in &ids {
                        set(&writer, id, &format!("k{k}"));
                    }
                    writer.execute("COMMIT").unwrap();
                    done.fetch_add(1, Ordering::AcqRel);
                    commits += 1;
                }
                commits
            });
            let forkers: Vec<_> = (0..FORKERS)
                .map(|_| {
                    s.spawn(|| {
                        force_fixes_for_test(mask);
                        let conn = db.connect().unwrap();
                        let mut held = Vec::new();
                        for _ in 0..FORKS {
                            let lo = done.load(Ordering::Acquire);
                            let branch = loop {
                                match conn.fork_branch() {
                                    Ok(b) => break b,
                                    Err(LimboError::Busy | LimboError::BusySnapshot) => std::thread::yield_now(),
                                    Err(e) => panic!("mask {mask}: fork failed: {e}"),
                                }
                            };
                            let hi = started.load(Ordering::Acquire);
                            let k = read_k(&branch);
                            assert!(lo <= k && k <= hi, "mask {mask}: branch reads k={k}, outside [{lo}, {hi}]");
                            held.push((branch, k));
                        }
                        held
                    })
                })
                .collect();
            let held: Vec<(Branch, u64)> = forkers
                .into_iter()
                .flat_map(|f| f.join().unwrap())
                .collect();
            stop.store(true, Ordering::Release);
            let commits = w.join().unwrap();
            assert!(commits > 0, "mask {mask}: the writer never committed");
            for (branch, k) in &held {
                assert_eq!(read_k(branch), *k, "mask {mask}: branch {} changed after its fork", branch.id().0);
            }
            held.len()
        });
        assert_eq!(checked, FORKERS * FORKS);
        let work = db.branch_stats().work;
        assert!(work.trunk_forks_fast > 0, "mask {mask}: no fork took the lock-free path: {work:?}");
        drop(first);
    }
    force_fixes_for_test(0);
}

/// FL keeps every row of a branch forked mid-transaction, not only the rows the transaction wrote: a table read
/// through a fresh connection equals the trunk's committed table at the fork, row for row.
#[test]
fn fl_a_branch_forked_mid_transaction_reads_the_whole_table_as_committed() {
    for mask in fl_masks() {
        force_fixes_for_test(mask);
        let (_dir, db) = open_db();
        let writer = db.connect().unwrap();
        seed(&writer, 300);
        let forker = db.connect().unwrap();
        let _anchor = forker.fork_branch().unwrap();
        let committed: BTreeMap<i64, String> = (1..=300).map(|id| (id, original(id))).collect();
        writer.execute("BEGIN").unwrap();
        for id in (1..=300).step_by(11) {
            set(&writer, id, "mid");
        }
        let b = forker
            .fork_branch()
            .unwrap_or_else(|e| panic!("mask {mask}: a fork mid-transaction must be admitted, got {e}"));
        writer.execute("COMMIT").unwrap();
        let bc = b.connect().unwrap();
        let rows: BTreeMap<i64, String> = bc
            .prepare("SELECT id, v FROM t ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| {
                let id = row[0].as_int().expect("integer id");
                let v = match &row[1] {
                    Value::Text(t) => t.as_str().to_string(),
                    other => panic!("expected text, got {other:?}"),
                };
                (id, v)
            })
            .collect();
        assert_eq!(rows, committed, "mask {mask}: the branch's table is not the trunk's committed table");
    }
    force_fixes_for_test(0);
}
