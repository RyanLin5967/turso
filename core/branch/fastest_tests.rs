//! fastest-engine lane (artie-research frontier/fastest, PREREG-DRAFT-v0 §4, §8, §11 M1): the
//! durability class, the lock-free durable trunk fork, the flight outside the store mutex, named
//! server branches and the per-fork bracketing counters. Written failing-first: each block names the
//! commit whose behaviour it pins.
//!
//! The sync counts read here are PROCESS-WIDE (`branch::sync_counts`), so these tests assume no other
//! test syncs concurrently: the suite runs `--test-threads=1` (as every branch gate does), and each
//! test here also takes `SERIAL` so a parallel run of this file alone cannot interleave them.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::path::Path;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn open_at(path: &Path, opts: DatabaseOpts) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn opts(catalog: bool, sync: SyncClass) -> DatabaseOpts {
    DatabaseOpts::new().with_branch_durability(if catalog {
        BranchDurability::Catalog { sync }
    } else {
        BranchDurability::Durable { sync }
    })
}

fn seed(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=50 {
        conn.execute(format!("INSERT INTO t VALUES ({id}, 'trunk-{id}')")).unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

/// The syncs `f` issued, as `(fsync, F_FULLFSYNC)`.
fn syncs_of(f: impl FnOnce()) -> (u64, u64) {
    let before = sync_counts();
    f();
    let after = sync_counts();
    (after.fsync - before.fsync, after.full_fsync - before.full_fsync)
}

// ---- the durability class (fastest-engine M1 item 3) ----

/// The instrument the class tests read (V1-in-process): each primitive a branch file is synced
/// with is counted under its own name, and `Off` issues none. Forced to fire both ways.
#[cfg(target_vendor = "apple")]
#[test]
fn the_sync_counter_counts_each_primitive_a_branch_file_is_synced_with() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let file = super::journal::open_rw(&dir.path().join("f"), true).unwrap();
    super::journal::write_at(&file, b"x", 0).unwrap();
    assert_eq!(
        syncs_of(|| super::journal::fsync_file(&file, SyncClass::Off).unwrap()),
        (0, 0),
        "D0 synced a branch file"
    );
    assert_eq!(
        syncs_of(|| super::journal::fsync_file(&file, SyncClass::Fsync).unwrap()),
        (1, 0),
        "D1 is one fsync(2)"
    );
    assert_eq!(
        syncs_of(|| super::journal::fsync_file(&file, SyncClass::FullFsync).unwrap()),
        (0, 1),
        "D2 is one F_FULLFSYNC and no fsync(2)"
    );
}

/// D2: a trunk fork of a store that already exists costs exactly one F_FULLFSYNC and no plain
/// fsync — the create's one barrier (PREREG §9 "at the floor": 1.00 F_FULLFSYNC per create). The
/// first fork, which creates the files, is not counted.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_trunk_fork_is_exactly_one_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let mut second = None;
        let counted = syncs_of(|| second = Some(trunk.fork_branch().unwrap()));
        assert_eq!(counted, (0, 1), "catalog={catalog}: a D2 fork's syncs (fsync, F_FULLFSYNC)");
        drop((first, second));
    }
}

/// D2: a trunk commit syncs the WAL with F_FULLFSYNC, never fsync(2): the trunk honours the class
/// the database was opened with, with no PRAGMA.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_trunk_commit_syncs_its_wal_with_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2t.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let counted = syncs_of(|| {
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
        });
        assert_eq!(counted.0, 0, "catalog={catalog}: a D2 trunk commit issued fsync(2): {counted:?}");
        assert!(counted.1 >= 1, "catalog={catalog}: a D2 trunk commit issued no F_FULLFSYNC");
    }
}

/// D2: a branch's first write is synced with F_FULLFSYNC only (arena, then log: two barriers until
/// M2's no-force page images).
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_branch_commit_syncs_only_with_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2b.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let counted = syncs_of(|| {
            bc.execute("UPDATE t SET v = 'mine' WHERE id = 3").unwrap();
        });
        assert_eq!(counted.0, 0, "catalog={catalog}: a D2 branch commit issued fsync(2): {counted:?}");
        assert!(counted.1 >= 1, "catalog={catalog}: a D2 branch commit issued no F_FULLFSYNC");
    }
}

/// D0 syncs nothing anywhere: not a fork, not a branch commit, not a trunk commit (the trunk's
/// connections are opened `synchronous = OFF`).
#[test]
fn d0_syncs_nothing_on_the_branch_store_or_the_trunk() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d0.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let counted = syncs_of(|| {
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            bc.execute("UPDATE t SET v = 'mine' WHERE id = 3").unwrap();
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
            drop(bc);
            drop(b);
        });
        assert_eq!(counted, (0, 0), "catalog={catalog}: D0 synced");
        drop(first);
    }
}

/// D1 never issues F_FULLFSYNC (the control for the D2 tests: the counter does not read F_FULLFSYNC
/// where none was asked for), and does sync.
#[test]
fn d1_syncs_with_fsync_and_never_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d1.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let counted = syncs_of(|| {
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            bc.execute("UPDATE t SET v = 'mine' WHERE id = 3").unwrap();
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
            drop(bc);
            drop(b);
        });
        assert_eq!(counted.1, 0, "catalog={catalog}: D1 issued F_FULLFSYNC: {counted:?}");
        assert!(counted.0 >= 3, "catalog={catalog}: D1 fork, branch commit and trunk commit synced {counted:?}");
        drop(first);
    }
}

/// The ordering guard: a trunk connection that asks for a STRONGER flush than the branch store's
/// class (`PRAGMA fullfsync` on a D1 store) has the pre-image barrier raised to match, so the trunk
/// commit that overwrites a page a branch reads is never more durable than the pre-image kept for
/// it. Every sync of that commit, branch files and WAL, is F_FULLFSYNC.
#[cfg(target_vendor = "apple")]
#[test]
fn a_full_fsync_trunk_connection_raises_the_pre_image_barrier() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("raise.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let counted = syncs_of(|| {
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
        });
        assert_eq!(
            counted.0, 0,
            "catalog={catalog}: a pre-image barrier under a fullfsync trunk issued fsync(2): {counted:?}"
        );
        assert!(counted.1 >= 2, "catalog={catalog}: barrier and WAL: {counted:?}");
        let bc = b.connect().unwrap();
        let v = bc
            .prepare("SELECT v FROM t WHERE id = 7")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(v[0][0], crate::Value::from_text("trunk-7"), "the branch read the trunk's new page");
    }
}

// ---- per-fork bracketing counters (fastest-engine M1 item 5) ----

/// The histogram the hold counters keep: every value lands in a bucket whose range holds it, each
/// bucket at or above 16 ns is at most 1/16 of its value wide, and a quantile read from it brackets
/// the true one from above by at most that width (the maximum is exact).
#[test]
fn hold_buckets_bound_every_value_and_a_quantile_brackets_the_true_one() {
    let mut values: Vec<u64> = (0..200).collect();
    for e in 4..63 {
        let p = 1u64 << e;
        values.extend([p - 1, p, p + 1, p + p / 3]);
    }
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..10_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        values.push(x >> (x % 40));
    }
    for &v in &values {
        let b = hold_bucket(v);
        assert!(b < HOLD_BUCKETS, "{v} has bucket {b}, past {HOLD_BUCKETS}");
        assert!(hold_bucket_upper(b) >= v, "{v} is above its bucket {b}'s upper bound");
        if b > 0 {
            assert!(hold_bucket_upper(b - 1) < v, "{v} also fits bucket {}", b - 1);
        }
        if v >= 16 {
            let width = hold_bucket_upper(b) - v;
            assert!(width as f64 <= v as f64 / 16.0, "{v}: bucket {b} is {width} wide above it");
        }
    }
    // 1..=1000 microseconds, once each: the true median is 500 us.
    let mut stats = HoldStats {
        buckets: vec![0; HOLD_BUCKETS],
        ..HoldStats::default()
    };
    for us in 1..=1000u64 {
        let ns = us * 1000;
        stats.count += 1;
        stats.sum_ns += ns;
        stats.max_ns = stats.max_ns.max(ns);
        stats.buckets[hold_bucket(ns)] += 1;
    }
    let p50 = stats.quantile_ns(0.5);
    assert!(
        (500_000..=500_000 + 500_000 / 16).contains(&p50),
        "the median of 1..=1000 us read as {p50} ns"
    );
    assert_eq!(stats.quantile_ns(1.0), 1_000_000, "the maximum is exact");
}

/// Every fork records ONE store-mutex hold, and every trunk fork ONE WAL-lock hold (0 when it took
/// no WAL lock); the WAL histogram reads a non-zero hold exactly when some trunk fork took the lock.
#[test]
fn every_fork_records_its_store_hold_and_every_trunk_fork_its_wal_hold() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("holds.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let before = db.branch_fork_holds();
        let mut kept = Vec::new();
        for _ in 0..5 {
            kept.push(trunk.fork_branch().unwrap());
        }
        for _ in 0..3 {
            let child = kept[0].fork().unwrap();
            kept.push(child);
        }
        let after = db.branch_fork_holds();
        assert_eq!(after.store.count - before.store.count, 8, "catalog={catalog}: store holds");
        assert_eq!(after.wal.count - before.wal.count, 5, "catalog={catalog}: WAL holds");
        assert!(after.store.max_ns > 0, "catalog={catalog}: no store hold was timed");
        assert!(after.store.sum_ns >= after.store.max_ns);
        assert_eq!(
            after.wal.max_ns > 0,
            after.locked_trunk_forks > 0,
            "catalog={catalog}: a WAL hold without a locked fork, or the reverse: max {} ns, {} locked",
            after.wal.max_ns,
            after.locked_trunk_forks
        );
        assert!(after.locked_trunk_forks <= 5);
        assert_eq!(after.store.buckets.iter().sum::<u64>(), after.store.count);
    }
}

// ---- the lock-free durable trunk fork (fastest-engine M1 item 1: F-L 573642f19 on the durable
// store) ----

fn read_t(conn: &Arc<Connection>) -> Vec<(i64, String)> {
    conn.prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            let v = match &row[1] {
                crate::Value::Text(t) => t.as_str().to_string(),
                other => panic!("expected text, got {other:?}"),
            };
            (row[0].as_int().unwrap(), v)
        })
        .collect()
}

fn read_v(conn: &Arc<Connection>, id: i64) -> String {
    read_t(conn).into_iter().find(|(i, _)| *i == id).unwrap().1
}

/// Reopen `path` and prove it is a new instance, not the registry's cached one.
fn reopen(path: &Path, opts: DatabaseOpts, previous: u64) -> Arc<Database> {
    let db = open_at(path, opts);
    assert_ne!(db.incarnation, previous, "the registry returned the old Database: not a reopen");
    db
}

/// K10-D1 and K10-D2 (r11-forklock's K10-1/K10-2, durable): a trunk fork made while another
/// connection's trunk write transaction is OPEN does not wait it out and is not refused; the
/// transaction's commit takes its copy decisions against the epoch at the commit, so the child
/// forked inside it reads the version the commit overwrote — both when the page's last commit was
/// in the epoch just before and when it was epochs back — and a child forked after the commit reads
/// the new version. All of it holds again after a reopen. (The trunk's FIRST child is still forked
/// under the WAL write lock, before the transaction.)
#[test]
fn a_trunk_fork_inside_an_open_trunk_transaction_reads_what_the_commit_overwrote() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("fl.db");
        let (ids, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let a = db.connect().unwrap();
            let b = db.connect().unwrap();
            seed(&a);
            let first = a.fork_branch().unwrap();
            // C1: page of id 7 last committed in the epoch just before.
            a.execute("BEGIN").unwrap();
            a.execute("UPDATE t SET v = 'c1' WHERE id = 7").unwrap();
            let x = b
                .fork_branch()
                .expect("a trunk fork waited out, or was refused by, an open trunk write transaction");
            a.execute("COMMIT").unwrap();
            // C2: a fork between C1 and C2, so the page's last commit is epochs back at C2.
            let between = b.fork_branch().unwrap();
            a.execute("BEGIN").unwrap();
            a.execute("UPDATE t SET v = 'c2' WHERE id = 7").unwrap();
            let y = b.fork_branch().unwrap();
            let z = b.fork_branch().unwrap();
            a.execute("COMMIT").unwrap();
            let after = b.fork_branch().unwrap();
            let expect = [
                (&first, "trunk-7"),
                (&x, "trunk-7"),
                (&between, "c1"),
                (&y, "c1"),
                (&z, "c1"),
                (&after, "c2"),
            ];
            for (i, (br, want)) in expect.iter().enumerate() {
                let c = br.connect().unwrap();
                assert_eq!(read_v(&c, 7), *want, "catalog={catalog}: fork #{i} before the reopen");
            }
            let ids: Vec<(BranchId, &str)> = expect.iter().map(|(br, w)| (br.id(), *w)).collect();
            let ids: Vec<(BranchId, String)> = ids.into_iter().map(|(i, w)| (i, w.to_string())).collect();
            for br in [first, x, between, y, z, after] {
                let _ = br.into_id();
            }
            (ids, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        for (i, (id, want)) in ids.iter().enumerate() {
            let c = db.branch(*id).unwrap().connect().unwrap();
            assert_eq!(&read_v(&c, 7), want, "catalog={catalog}: fork #{i} after the reopen");
        }
    }
}

/// The trunk's FIRST live child is still forked under the WAL write lock: with no live child, a
/// writer captured no pre-image, so no child may appear before its commit. A fork while a trunk
/// write transaction is open is then refused Busy (before the port and after it).
#[test]
fn the_trunks_first_child_still_waits_for_an_open_trunk_write_transaction() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("first.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        let b = db.connect().unwrap();
        seed(&a);
        a.execute("BEGIN").unwrap();
        a.execute("UPDATE t SET v = 'mid' WHERE id = 7").unwrap();
        let forked = b.fork_branch();
        assert!(
            matches!(forked, Err(LimboError::Busy)),
            "catalog={catalog}: the trunk's first child was forked inside an open trunk write \
             transaction: {:?}",
            forked.map(|b| b.id())
        );
        a.execute("COMMIT").unwrap();
        let c = b.fork_branch().unwrap().connect().unwrap();
        assert_eq!(read_v(&c, 7), "mid");
    }
}

/// After the first child, trunk forks take no WAL write lock: one locked fork in five.
#[test]
fn trunk_forks_after_the_first_take_no_wal_write_lock() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("nolock.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed(&a);
        let kept: Vec<Branch> = (0..5).map(|_| a.fork_branch().unwrap()).collect();
        let holds = db.branch_fork_holds();
        assert_eq!(holds.wal.count, 5);
        assert_eq!(
            holds.locked_trunk_forks, 1,
            "catalog={catalog}: trunk forks that took the WAL write lock (of {})", holds.wal.count
        );
        drop(kept);
    }
}

/// K10-D5 (r11-forklock's K10, durable, through SQL): a writer commits one-to-three-row trunk
/// transactions in a loop while four threads fork from the trunk on their own connections. Each
/// child must read, at once, exactly one committed trunk state between the last commit returned
/// before its fork began and the first one not yet returned when it ended; it must read that same
/// state after every later commit and after a reopen. The shapes the port exists for must occur:
/// lock-free forks, and forks inside an open trunk transaction.
#[test]
fn lock_free_durable_forks_racing_trunk_commits_read_every_fork_as_it_was() {
    use std::sync::atomic::{AtomicBool as StdBool, AtomicU64 as StdU64};
    use std::sync::RwLock;
    let _s = serial();
    const FORKERS: usize = 4;
    const FORKS: usize = 40;
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("race.db");
        let opts = || opts(catalog, SyncClass::Fsync);
        let (kept, incarnation) = {
            let db = open_at(&path, opts());
            let seed_conn = db.connect().unwrap();
            seed(&seed_conn);
            let first = seed_conn.fork_branch().unwrap().into_id();
            // states[j]: the trunk's table after the j-th commit (states[0]: the seed).
            let states = Arc::new(RwLock::new(vec![read_t(&seed_conn)]));
            let done = Arc::new(StdBool::new(false));
            let mid_txn_forks = Arc::new(StdU64::new(0));
            let writer = {
                let (db, states, done, mid) =
                    (db.clone(), states.clone(), done.clone(), mid_txn_forks.clone());
                std::thread::spawn(move || {
                    let conn = db.connect().unwrap();
                    let mut x = 0x2545_F491_4F6C_DD1Du64;
                    let mut generation = 0u64;
                    while !done.load(std::sync::atomic::Ordering::Acquire) {
                        let forks_before = db.branch_fork_holds().store.count;
                        let mut next = states.read().unwrap().last().unwrap().clone();
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let mut writes = Vec::new();
                        for _ in 0..=(x % 3) {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            generation += 1;
                            writes.push((1 + (x % 50) as i64, format!("g{generation}")));
                        }
                        // A fork that fell back to the WAL write lock makes a statement Busy: the
                        // transaction is retried whole, as a client would.
                        let committed = (|| -> Result<()> {
                            conn.execute("BEGIN")?;
                            for (id, v) in &writes {
                                conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}"))?;
                                std::thread::yield_now();
                            }
                            conn.execute("COMMIT")
                        })();
                        match committed {
                            Ok(()) => {}
                            Err(LimboError::Busy) => {
                                let _ = conn.execute("ROLLBACK");
                                std::thread::yield_now();
                                continue;
                            }
                            Err(e) => panic!("trunk writer: {e}"),
                        }
                        for (id, v) in writes {
                            next[(id - 1) as usize].1 = v;
                        }
                        states.write().unwrap().push(next);
                        if db.branch_fork_holds().store.count > forks_before {
                            mid.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        std::thread::yield_now();
                    }
                })
            };
            let forkers: Vec<_> = (0..FORKERS)
                .map(|_| {
                    let (db, states) = (db.clone(), states.clone());
                    std::thread::spawn(move || {
                        let conn = db.connect().unwrap();
                        let mut kept = Vec::new();
                        for _ in 0..FORKS {
                            let lo = states.read().unwrap().len() - 1;
                            let branch = loop {
                                match conn.fork_branch() {
                                    Ok(b) => break b,
                                    // Only a fork that falls back to the WAL write lock can see Busy.
                                    Err(LimboError::Busy) => std::thread::yield_now(),
                                    Err(e) => panic!("fork failed: {e}"),
                                }
                            };
                            // Commits recorded by the time the fork returned; the one at index `hi`
                            // may have been in flight (published, its writer not yet recording it),
                            // so it is waited for (bounded) before the child's state is matched.
                            let hi = states.read().unwrap().len();
                            let seen = read_t(&branch.connect().unwrap());
                            let waited = std::time::Instant::now();
                            while states.read().unwrap().len() <= hi
                                && waited.elapsed() < std::time::Duration::from_millis(500)
                            {
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                            let states = states.read().unwrap();
                            let j = (lo..=hi.min(states.len() - 1))
                                .find(|&j| states[j] == seen)
                                .unwrap_or_else(|| {
                                    panic!("a child forked between commits {lo} and {hi} reads no committed state")
                                });
                            kept.push((branch.into_id(), j));
                        }
                        kept
                    })
                })
                .collect();
            let mut kept: Vec<(BranchId, usize)> = Vec::new();
            for f in forkers {
                kept.extend(f.join().unwrap());
            }
            done.store(true, std::sync::atomic::Ordering::Release);
            writer.join().unwrap();
            let states = states.read().unwrap().clone();
            for &(id, j) in &kept {
                // Re-attached, read, and detached again: a dropped handle would reap the branch.
                let b = db.branch(id).unwrap();
                let c = b.connect().unwrap();
                assert_eq!(read_t(&c), states[j], "catalog={catalog}: branch {} moved after its fork", id.0);
                drop(c);
                let _ = b.into_id();
            }
            let holds = db.branch_fork_holds();
            let fast = holds.wal.count - holds.locked_trunk_forks;
            assert!(
                fast > 0,
                "catalog={catalog}: no trunk fork took the lock-free path ({} of {} locked)",
                holds.locked_trunk_forks,
                holds.wal.count
            );
            assert!(
                mid_txn_forks.load(std::sync::atomic::Ordering::Relaxed) > 0,
                "catalog={catalog}: no fork landed inside an open trunk transaction"
            );
            let _ = first;
            let kept: Vec<(BranchId, Vec<(i64, String)>)> =
                kept.into_iter().map(|(id, j)| (id, states[j].clone())).collect();
            (kept, db.incarnation)
        };
        let db = reopen(&path, opts(), incarnation);
        for (id, want) in &kept {
            let c = db.branch(*id).unwrap().connect().unwrap();
            assert_eq!(&read_t(&c), want, "catalog={catalog}: branch {} after the reopen", id.0);
        }
    }
}

// ---- group commit with the flush outside the store mutex (fastest-engine M1 item 2: gc
// 389b474b4 on the durable store) ----

/// A fork's store-mutex hold does not include its flush: at D2 every flush is an F_FULLFSYNC of
/// about 3 ms on this Mac (device floor, banked by the tools lane's V3), so a mean hold per fork
/// below 1 ms is impossible for a store that syncs under the mutex. Forks one at a time, so no
/// other fork's flight can carry this one's records.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_forks_store_mutex_hold_excludes_its_flush() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("hold.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let before = db.branch_fork_holds();
        let mut kept = Vec::new();
        let counted = syncs_of(|| {
            for _ in 0..20 {
                kept.push(trunk.fork_branch().unwrap());
            }
            for _ in 0..10 {
                kept.push(first.fork().unwrap());
            }
        });
        let after = db.branch_fork_holds();
        let forks = after.store.count - before.store.count;
        let mean = (after.store.sum_ns - before.store.sum_ns) / forks;
        assert_eq!(forks, 30);
        assert!(counted.1 >= 30, "catalog={catalog}: each fork alone is one F_FULLFSYNC: {counted:?}");
        assert!(
            mean < 1_000_000,
            "catalog={catalog}: a D2 fork held the store mutex {mean} ns on average: its flush is \
             inside the mutex"
        );
    }
}

/// Concurrent creates share flushes: eight threads forking at once at D2 need far fewer than one
/// F_FULLFSYNC per fork, since every fork that arrives while a flight is in the air rides the next
/// one. A store that flushes each fork under its mutex issues exactly one per fork.
#[cfg(target_vendor = "apple")]
#[test]
fn concurrent_d2_forks_share_their_flushes() {
    let _s = serial();
    const THREADS: usize = 8;
    const FORKS: usize = 20;
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("group.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let counted = syncs_of(|| {
            let start = Arc::new(std::sync::Barrier::new(THREADS));
            let threads: Vec<_> = (0..THREADS)
                .map(|_| {
                    let (db, start) = (db.clone(), start.clone());
                    std::thread::spawn(move || {
                        let conn = db.connect().unwrap();
                        start.wait();
                        (0..FORKS)
                            .map(|_| conn.fork_branch().unwrap().into_id())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            for t in threads {
                assert_eq!(t.join().unwrap().len(), FORKS);
            }
        });
        let forks = (THREADS * FORKS) as u64;
        assert!(
            counted.1 * 2 <= forks,
            "catalog={catalog}: {forks} concurrent D2 forks issued {} F_FULLFSYNC: no flush was shared",
            counted.1
        );
        drop(first);
    }
}

/// Concurrent first writes on different branches share flushes too: each branch commit at D2 is
/// an arena sync and a log sync, both F_FULLFSYNC, and eight committers at once must issue far
/// fewer than two per commit. A store that syncs each commit under its mutex issues exactly two.
#[cfg(target_vendor = "apple")]
#[test]
fn concurrent_d2_branch_commits_share_their_flushes() {
    let _s = serial();
    const THREADS: usize = 8;
    const COMMITS: usize = 10;
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("commits.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let branches: Vec<Branch> = (0..THREADS).map(|_| trunk.fork_branch().unwrap()).collect();
        let ids: Vec<BranchId> = branches.into_iter().map(|b| b.into_id()).collect();
        let counted = syncs_of(|| {
            let start = Arc::new(std::sync::Barrier::new(THREADS));
            let threads: Vec<_> = ids
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    let (db, start) = (db.clone(), start.clone());
                    std::thread::spawn(move || {
                        let b = db.branch(id).unwrap();
                        let c = b.connect().unwrap();
                        start.wait();
                        for k in 0..COMMITS {
                            c.execute(format!("UPDATE t SET v = 'b{i}-{k}' WHERE id = {}", 1 + k))
                                .unwrap();
                        }
                        drop(c);
                        let _ = b.into_id();
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
        });
        let commits = (THREADS * COMMITS) as u64;
        assert!(
            counted.1 < commits * 3 / 2,
            "catalog={catalog}: {commits} concurrent D2 branch commits issued {} F_FULLFSYNC (two \
             each when no flush is shared)",
            counted.1
        );
    }
}

/// G1 (gc 389b474b4's failpoint test, durable store): a group flight that fails after an
/// early-released release was applied fail-stops the store, reports the release as not durable,
/// and frees NOTHING of the branch — now or later in this process — since no flight will ever cover
/// its Release (rule 2). After a reopen the branch is back with its write. Mutant
/// `free_before_durable` (M-g) frees at once, and this test must fail on it.
#[test]
fn a_failed_group_flight_fail_stops_the_store_and_frees_nothing() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("gff.db");
        let (id, owned, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            bc.execute("UPDATE t SET v = 'mine' WHERE id = 3").unwrap();
            drop(bc);
            let owned = b.owned_slots();
            assert!(!owned.is_empty(), "premise: the branch owns a slot");
            let id = b.id();
            db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
            assert!(b.reap().is_err(), "catalog={catalog}: a release whose flight failed was reported");
            for slot in &owned {
                assert!(
                    !db.branch_slot_is_free(*slot),
                    "catalog={catalog}: slot {slot} was freed though its Release is not durable"
                );
            }
            assert!(trunk.fork_branch().is_err(), "catalog={catalog}: a fail-stopped store forked");
            (id, owned, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let b = db.branch(id).expect("the branch whose release failed is back after a reopen");
        assert_eq!(read_v(&b.connect().unwrap(), 3), "mine", "catalog={catalog}");
        assert_eq!(b.owned_slots().len(), owned.len());
    }
}

/// G2 (gc3's N1, durable store): a trunk commit that retains nothing — its only child was just
/// released, early, and no live child can see the page — must still not become durable ahead of
/// that Release. Its barrier waits for every record buffered before its decisions; when that flight
/// failed, the commit is refused, because after a restart the child comes back (its Release lost)
/// and nothing kept the page it reads. After the reopen the child reads its fork-point page. Mutant
/// `barrier_own_only` (the barrier flushes only the commit's own pre-images) must fail this.
#[test]
fn a_trunk_commit_is_refused_when_an_early_release_it_relied_on_failed() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("n1.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let x = trunk.fork_branch().unwrap();
            let id = x.id();
            db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
            assert!(x.reap().is_err(), "premise: the release's flight failed");
            let committed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
            assert!(
                committed.is_err(),
                "catalog={catalog}: a trunk commit became durable ahead of a Release it relied on"
            );
            (id, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let x = db.branch(id).expect("the child whose release failed is back");
        assert_eq!(
            read_v(&x.connect().unwrap(), 7),
            "trunk-7",
            "catalog={catalog}: the child reads a trunk page written after its fork"
        );
    }
}

// ---- detached, named, persistent server branches (fastest-engine M1 item 4) ----

fn write_v(conn: &Arc<Connection>, id: i64, v: &str) {
    conn.execute(format!("UPDATE t SET v = '{v}' WHERE id = {id}")).unwrap();
}

/// A table whose rows sit on many pages: 60 rows of ~1 KiB, about three to a 4 KiB leaf, so rows 3,
/// 40 and 50 are on different pages and a branch's write to one leaves the others' pages to its
/// ancestors. Values read back as `wide-<id>`'s first 7 bytes via `read_wide`.
fn seed_wide(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.execute("BEGIN").unwrap();
    for id in 1..=60 {
        conn.execute(format!("INSERT INTO t VALUES ({id}, 'trunk-{id}-{}')", "x".repeat(1000)))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    let pages = conn
        .prepare("PRAGMA page_count")
        .unwrap()
        .run_collect_rows()
        .unwrap()[0][0]
        .as_int()
        .unwrap();
    assert!(pages >= 15, "premise: the table spans many pages ({pages})");
}

/// Row `id`'s value up to its first '-x' padding: `trunk-<id>` for an untouched seed_wide row.
fn read_wide(conn: &Arc<Connection>, id: i64) -> String {
    let v = read_v(conn, id);
    v.split("-x").next().unwrap().to_string()
}

/// N1 (E3's core, and mutant M-d `fork_without_parent`): a named branch needs no handle — nothing
/// reaps it when the creating connection, or the whole database, goes away — survives a restart,
/// and is connected to by name, with its own writes and nothing the trunk wrote after its fork. A
/// named branch of it, created from its connection, reads its parent's write from a page it never
/// wrote itself, so the recovered parent pointer is what it reads through.
#[test]
fn a_named_branch_is_detached_persistent_and_connectable_by_name() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("named.db");
        let incarnation = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed_wide(&trunk);
            let alpha = trunk.create_branch("alpha").unwrap();
            assert_eq!(db.branch_named("alpha").unwrap(), Some(alpha));
            {
                let c = db.connect_named("alpha").unwrap();
                write_v(&c, 3, "alpha-3");
                // A branch of the named branch, named, created from its connection.
                c.create_branch("alpha.beta").unwrap();
            }
            write_v(&trunk, 40, "trunk-later");
            {
                let c = db.connect_named("alpha.beta").unwrap();
                write_v(&c, 50, "beta-50");
            }
            assert!(db.branch_ids().unwrap().contains(&alpha));
            db.incarnation
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let a = db.connect_named("alpha").unwrap();
        assert_eq!(read_wide(&a, 3), "alpha-3", "catalog={catalog}");
        assert_eq!(read_wide(&a, 40), "trunk-40", "catalog={catalog}: a trunk write after the fork");
        assert_eq!(read_wide(&a, 50), "trunk-50", "catalog={catalog}: the child's write leaked up");
        drop(a);
        let b = db.connect_named("alpha.beta").unwrap();
        assert_eq!(
            read_wide(&b, 3),
            "alpha-3",
            "catalog={catalog}: the child lost its parent's write (read through its parent pointer)"
        );
        assert_eq!(read_wide(&b, 40), "trunk-40", "catalog={catalog}");
        assert_eq!(read_wide(&b, 50), "beta-50", "catalog={catalog}");
        assert!(db.connect_named("gamma").is_err(), "catalog={catalog}: an unknown name connected");
    }
}

/// N2 (I7, and mutant M-i `name_check_outside`): a name is unique among unreleased branches; a
/// dropped name is free at once, and re-created it names the NEW branch — before and after a
/// checkpoint, and after a reopen.
#[test]
fn branch_names_are_unique_and_a_dropped_name_names_its_new_branch() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("unique.db");
        let (second, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let first = trunk.create_branch("n").unwrap();
            let dup = trunk.create_branch("n");
            assert!(dup.is_err(), "catalog={catalog}: a taken name was given twice: {dup:?}");
            db.branch_compact_now().unwrap();
            assert!(trunk.create_branch("n").is_err(), "catalog={catalog}: after a checkpoint");
            {
                let c = db.connect_named("n").unwrap();
                write_v(&c, 3, "first");
            }
            db.drop_branch("n").unwrap();
            assert_eq!(db.branch_named("n").unwrap(), None, "catalog={catalog}: a dropped name");
            assert!(db.drop_branch("n").is_err(), "catalog={catalog}: dropped twice");
            let second = trunk.create_branch("n").unwrap();
            assert_ne!(second, first);
            assert_eq!(db.branch_named("n").unwrap(), Some(second));
            assert_eq!(read_v(&db.connect_named("n").unwrap(), 3), "trunk-3", "catalog={catalog}");
            (second, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        assert_eq!(db.branch_named("n").unwrap(), Some(second), "catalog={catalog}: after a reopen");
        assert_eq!(read_v(&db.connect_named("n").unwrap(), 3), "trunk-3", "catalog={catalog}");
        db.branch_compact_now().unwrap();
        assert_eq!(db.branch_named("n").unwrap(), Some(second), "catalog={catalog}: after a checkpoint");
    }
}

/// N3 (E3 a): a named server branch carries no lease, even when the database gives every fork one,
/// so no expiry pass reaps it however long it sits idle; an unnamed fork beside it is reaped.
#[test]
fn a_named_branch_carries_no_lease_even_with_a_default_lease() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("lease.db");
        let leased = |catalog| opts(catalog, SyncClass::Fsync).with_branch_lease(Some(std::time::Duration::from_secs(1)));
        let (named, incarnation) = {
            let db = open_at(&path, leased(catalog));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let unnamed = trunk.fork_branch().unwrap().into_id();
            let named = trunk.create_branch("server").unwrap();
            db.branch_lease_clock_advance(std::time::Duration::from_secs(3600));
            let expired = db.expire_branches().unwrap();
            assert!(expired.reaped.contains(&unnamed), "catalog={catalog}: premise: leases expire");
            assert!(!expired.reaped.contains(&named), "catalog={catalog}: a named branch was leased");
            db.branch_compact_now().unwrap();
            (named, db.incarnation)
        };
        let db = reopen(&path, leased(catalog), incarnation);
        db.branch_lease_clock_advance(std::time::Duration::from_secs(3600));
        db.expire_branches().unwrap();
        assert_eq!(db.branch_named("server").unwrap(), Some(named), "catalog={catalog}");
    }
}

// ---- E2's scope rule (PREREG v1 amendments 10-11): an attached database is not branched ----

/// A branch connection cannot ATTACH a database (a Postgres frontend's non-public schema is one):
/// the file would not be branched, so its writes would be shared by every branch and its parent.
/// Refused at the statement.
#[test]
fn attach_on_a_branch_is_refused_at_the_statement() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("e2a.db"), opts(catalog, SyncClass::Off).with_attach(true));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let other = dir.path().join("s2.db");
        let attached = bc.execute(format!("ATTACH DATABASE '{}' AS s2", other.display()));
        assert!(
            attached.is_err(),
            "catalog={catalog}: a branch attached an unbranched database file"
        );
        // The control: the trunk may attach (nothing is forked from it while it is attached).
        trunk
            .execute(format!("ATTACH DATABASE '{}' AS s2", other.display()))
            .unwrap();
        trunk.execute("CREATE TABLE s2.u(x)").unwrap();
    }
}

/// A fork from a connection with an attached database is refused at the fork: the attached schema
/// would not be branched with it. Once detached, the fork goes through.
#[test]
fn a_fork_with_an_attached_database_is_refused() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("e2b.db"), opts(catalog, SyncClass::Off).with_attach(true));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let other = dir.path().join("s2.db");
        trunk
            .execute(format!("ATTACH DATABASE '{}' AS s2", other.display()))
            .unwrap();
        assert!(
            trunk.fork_branch().is_err(),
            "catalog={catalog}: a fork went through with an unbranched schema attached"
        );
        assert!(
            first.connect().unwrap().fork_branch().is_ok(),
            "catalog={catalog}: a branch with nothing attached was refused a fork"
        );
        trunk.execute("DETACH DATABASE s2").unwrap();
        assert!(trunk.fork_branch().is_ok(), "catalog={catalog}: refused after the DETACH");
    }
}

/// K10-D6 (mutant M-f `gate_admits_inflight`): a fork that arrives while a trunk commit is inside
/// its commit gate — its copy decisions taken without the fork, its frames not yet published —
/// forks AFTER that commit, and is not handed out until the commit is published, or the child would
/// read the commit's pages one way before the publication and another after it. A hook holds a
/// commit inside its gate while another thread forks and reads at once; the child's first read
/// must equal every later one, and show the commit.
#[test]
fn a_fork_waits_out_a_trunk_commit_inside_its_gate() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("gate.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let first = a.fork_branch().unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_TRUNK_DECIDED, std::sync::atomic::Ordering::Release);
        let writer = {
            let a = a.clone();
            std::thread::spawn(move || write_v(&a, 7, "c1"))
        };
        let arrived = super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED;
        let started = std::time::Instant::now();
        while hold.load(std::sync::atomic::Ordering::Acquire) != arrived {
            assert!(started.elapsed() < std::time::Duration::from_secs(10), "the commit never reached its gate hold");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let forker = {
            let db = db.clone();
            std::thread::spawn(move || {
                let b = db.connect().unwrap();
                let child = b.fork_branch().unwrap();
                let first_read = read_wide(&child.connect().unwrap(), 7);
                (child.into_id(), first_read)
            })
        };
        // Well inside GATE_WAIT: a fork that waits for the gate is still waiting when released.
        std::thread::sleep(std::time::Duration::from_millis(300));
        hold.store(0, std::sync::atomic::Ordering::Release);
        writer.join().unwrap();
        let (child, first_read) = forker.join().unwrap();
        let b = db.branch(child).unwrap();
        let later = read_wide(&b.connect().unwrap(), 7);
        assert_eq!(
            first_read, later,
            "catalog={catalog}: the child read a trunk commit's page before its publication and after it"
        );
        assert_eq!(later, "c1", "catalog={catalog}: the child registered inside the commit's gate");
        write_v(&a, 7, "c2");
        assert_eq!(read_wide(&b.connect().unwrap(), 7), "c1", "catalog={catalog}: moved after a later commit");
        drop(first);
    }
}

/// M1 exit 2, the in-process half (the DYLD shim, V1, is the registered instrument and the tools
/// lane's): `FE_V1_CREATES` (default 200) creates at C=1 at D2, from the trunk and named, issue
/// 1.00 to 1.01 F_FULLFSYNC each and no fsync(2) — the store's and the trunk's syncs alike, since
/// the counter sees both. Counted after the store's files exist (the first create makes them).
#[cfg(target_vendor = "apple")]
#[test]
fn v1_in_process_one_full_fsync_per_create_at_c1() {
    let _s = serial();
    let creates = std::env::var("FE_V1_CREATES").ok().and_then(|v| v.parse().ok()).unwrap_or(200u64);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("v1.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let counted = syncs_of(|| {
            for i in 0..creates {
                trunk.create_branch(&format!("v1-{i}")).unwrap();
            }
        });
        let per_create = counted.1 as f64 / creates as f64;
        assert_eq!(counted.0, 0, "catalog={catalog}: fsync(2) issued during D2 creates: {counted:?}");
        assert!(
            (1.0..=1.01).contains(&per_create),
            "catalog={catalog}: {} F_FULLFSYNC over {creates} creates = {per_create:.4} per create",
            counted.1
        );
    }
}

// ---- the M1 review (four fresh-context readers of ad9829f3b..88dfe324f): each block names the
// finding it pins; written failing-first ----

fn wait_hold(hold: &std::sync::atomic::AtomicU8, stage: u8) {
    let t = std::time::Instant::now();
    while hold.load(std::sync::atomic::Ordering::Acquire) != stage | super::store::HOLD_ARRIVED {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "the hook never reached stage {stage}");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

fn arena_path(db: &Arc<Database>) -> std::path::PathBuf {
    let log = db.branch_log_path().expect("a durable store");
    std::path::PathBuf::from(log.to_str().unwrap().replace("-branch-log", "-branch-arena"))
}

/// Review A-F3 (E4): a branch forked while a trunk commit is inside its commit gate forks AFTER that
/// commit, so nothing reads it before the commit is published — not its creator, and not another
/// connection that finds it by name meanwhile. A hook holds a commit inside its gate; a named branch
/// is created, and another thread looks it up and connects to it at once. Nothing retained the
/// overwritten page for the child (its epoch is past the commit's decisions), so a read before the
/// publication would see the trunk's old page and, after it, the new one.
#[test]
fn a_branch_forked_inside_a_commit_gate_is_opened_by_name_only_after_the_commit() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("gate-named.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        // The trunk's first child: later forks take no WAL write lock.
        let first = a.fork_branch().unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_TRUNK_DECIDED, std::sync::atomic::Ordering::Release);
        let writer = {
            let a = a.clone();
            std::thread::spawn(move || write_v(&a, 7, "c1"))
        };
        wait_hold(&hold, super::store::HOLD_TRUNK_DECIDED);
        let creator = {
            let db = db.clone();
            std::thread::spawn(move || db.connect().unwrap().create_branch("feat").unwrap())
        };
        let reader = {
            let db = db.clone();
            std::thread::spawn(move || {
                let t = std::time::Instant::now();
                let id = loop {
                    if let Some(id) = db.branch_named("feat").unwrap() {
                        break id;
                    }
                    assert!(t.elapsed() < std::time::Duration::from_secs(10), "the name never appeared");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                };
                let c = db.connect_named("feat").unwrap();
                (id, read_wide(&c, 7))
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(300));
        hold.store(0, std::sync::atomic::Ordering::Release);
        writer.join().unwrap();
        let created = creator.join().unwrap();
        let (found, first_read) = reader.join().unwrap();
        assert_eq!(found, created, "catalog={catalog}");
        assert_eq!(
            first_read, "c1",
            "catalog={catalog}: a branch forked after a trunk commit was read before that commit was published"
        );
        drop(first);
    }
}

/// Review C-F4: a named branch is found by name only once its fork is durable. A hook holds the
/// create's flight after it is taken from the buffer; a lookup made meanwhile must wait for the
/// flight, not return a branch a crash would lose.
#[test]
fn a_named_branch_is_found_by_name_only_once_its_fork_is_durable() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("named-durable.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let creator = {
            let db = db.clone();
            std::thread::spawn(move || db.connect().unwrap().create_branch("n").unwrap())
        };
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let lookup = {
            let db = db.clone();
            std::thread::spawn(move || db.branch_named("n").unwrap())
        };
        std::thread::sleep(std::time::Duration::from_millis(300));
        let early = lookup.is_finished();
        hold.store(0, std::sync::atomic::Ordering::Release);
        let created = creator.join().unwrap();
        let found = lookup.join().unwrap();
        assert!(!early, "catalog={catalog}: the name was found while its fork's records were in the air");
        assert_eq!(found, Some(created), "catalog={catalog}");
    }
}

/// Review B-F1: a flight that cannot even be taken (its descriptor's duplication failed, as EMFILE
/// would) fail-stops the WHOLE store at once: no later commit writes an arena slot, and the close
/// of a branch whose release it carried frees nothing — its Release is not durable, and after a
/// restart the branch is back and names those slots.
#[test]
fn a_flight_that_cannot_be_taken_fail_stops_every_later_write() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("take.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let x = trunk.fork_branch().unwrap();
        let xc = x.connect().unwrap();
        write_v(&xc, 3, "x");
        let y = trunk.fork_branch().unwrap();
        let yc = y.connect().unwrap();
        write_v(&yc, 4, "y");
        let owned = x.owned_slots();
        assert!(!owned.is_empty(), "premise: the branch owns a slot");
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightTakeFails));
        assert!(x.reap().is_err(), "catalog={catalog}: premise: the release's flight was not taken");
        let arena = arena_path(&db);
        let before = std::fs::read(&arena).unwrap();
        assert!(
            yc.execute("UPDATE t SET v = 'y2' WHERE id = 9").is_err(),
            "catalog={catalog}: a branch committed after the store fail-stopped"
        );
        assert!(
            std::fs::read(&arena).unwrap() == before,
            "catalog={catalog}: an arena slot was written after the store fail-stopped"
        );
        drop(xc);
        for slot in &owned {
            assert!(
                !db.branch_slot_is_free(*slot),
                "catalog={catalog}: slot {slot} was freed though its Release is not durable"
            );
        }
        drop(yc);
        let _ = y.into_id();
    }
}

/// Review B-F2: a release of a branch that is already released — two drops of one name racing —
/// returns only once that release is durable. A hook holds the first release's flight; the second
/// release must wait for it, not acknowledge a Release a crash would lose.
#[test]
fn a_second_release_returns_only_once_the_first_is_durable() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("rerelease.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let id = trunk.create_branch("n").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let first = {
            let db = db.clone();
            std::thread::spawn(move || db.branches.release_handle(id))
        };
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let second = {
            let db = db.clone();
            std::thread::spawn(move || db.branches.release_handle(id))
        };
        std::thread::sleep(std::time::Duration::from_millis(300));
        let early = second.is_finished();
        hold.store(0, std::sync::atomic::Ordering::Release);
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(
            !early,
            "catalog={catalog}: a second release returned while the first one's Release was in the air"
        );
    }
}

/// Review B-F3: a pre-image whose barrier was raised to a stronger trunk class (`PRAGMA fullfsync`
/// on a D1 store) is re-saved in that class by every rewrite that replaces the record holding it — a
/// snapshot compaction or a catalog checkpoint and its log cut — in this process and after a
/// restart, or a power cut can keep the rewrite's cut and lose the copy while the trunk commit that
/// relied on it survives.
#[cfg(target_vendor = "apple")]
#[test]
fn a_raised_pre_image_is_rewritten_in_the_raised_class() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("raised.db");
        let incarnation = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
            let counted = syncs_of(|| db.branch_compact_now().unwrap());
            assert_eq!(
                counted.0, 0,
                "catalog={catalog}: a rewrite re-saved a raised pre-image with fsync(2): {counted:?}"
            );
            assert!(counted.1 >= 1, "catalog={catalog}: premise: the rewrite synced: {counted:?}");
            let _ = b.into_id();
            db.incarnation
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let counted = syncs_of(|| db.branch_compact_now().unwrap());
        assert_eq!(
            counted.0, 0,
            "catalog={catalog}: after a restart a rewrite re-saved a raised pre-image with fsync(2): {counted:?}"
        );
    }
}

/// Review C-F1 and C-F2: a name released before a fuzzy checkpoint's capture is free while the
/// checkpoint is in flight — it resolves to nothing and can be given again — and the new branch
/// keeps it after the install and after a reopen. (The splice arm takes no fuzzy checkpoint: there
/// the refusal is the premise checked, and the property cannot arise.)
#[test]
fn a_released_name_is_free_while_a_fuzzy_checkpoint_is_in_flight() {
    let _s = serial();
    let splice = std::env::var("R11_SPLICE").is_ok_and(|v| v == "1");
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fuzzy-name.db");
    let (b, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let a = trunk.create_branch("x").unwrap();
        db.branch_compact_now().unwrap();
        db.drop_branch("x").unwrap();
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        let started = db.branch_checkpoint_fuzzy_now();
        if splice {
            assert!(started.is_err(), "premise of the splice arm: no fuzzy checkpoint");
            return;
        }
        assert!(started.unwrap(), "premise: a fuzzy checkpoint started");
        let t = std::time::Instant::now();
        while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "the checkpoint never arrived");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(
            db.branch_named("x").unwrap(),
            None,
            "a released name resolved while a checkpoint was in flight"
        );
        let b = trunk
            .create_branch("x")
            .expect("a released name was taken while a checkpoint was in flight");
        assert_ne!(a, b);
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        assert_eq!(db.branch_named("x").unwrap(), Some(b), "after the install");
        (b, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    assert_eq!(db.branch_named("x").unwrap(), Some(b), "after a reopen");
}

/// Review C-F3: a named server branch takes no `Branch` handle — a handle's drop would release it
/// and a handle could lease it, and nothing but `Database::drop_branch` may end a named branch.
#[test]
fn a_named_branch_takes_no_handle() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nohandle.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let id = trunk.create_branch("srv").unwrap();
            assert!(db.branch(id).is_err(), "catalog={catalog}: a named branch was given a handle");
            assert_eq!(db.branch_named("srv").unwrap(), Some(id), "catalog={catalog}");
            (id, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        assert!(db.branch(id).is_err(), "catalog={catalog}: after a reopen");
        assert_eq!(db.branch_named("srv").unwrap(), Some(id), "catalog={catalog}: after a reopen");
    }
}

/// Review A-F2: a trunk commit whose copy-decision pass is refused part-way (`Busy`, as a catalog
/// read the catalog's lock refused would be) and retried by its statement decides EVERY page on the
/// retry, so a live child keeps reading its fork-point version of each page the commit wrote.
#[test]
fn a_trunk_commit_retried_after_a_busy_decision_pass_retains_every_page() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("busy-decide.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let child = a.fork_branch().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
        let mut stmt = a.prepare("UPDATE t SET v = 'new' WHERE id IN (3, 40, 50)").unwrap();
        let first = stmt.run_ignore_rows();
        assert!(
            matches!(first, Err(LimboError::Busy)),
            "catalog={catalog}: premise: the decision pass was refused: {first:?}"
        );
        stmt.run_ignore_rows().unwrap();
        drop(stmt);
        let c = child.connect().unwrap();
        for id in [3, 40, 50] {
            assert_eq!(
                read_wide(&c, id),
                format!("trunk-{id}"),
                "catalog={catalog}: the child reads a page the retried commit wrote"
            );
        }
    }
}

/// Review A-F1: a raw WAL session's commit (`wal_insert_end(true)`) closes the commit gate it opens,
/// so the next trunk commit takes its own copy decisions and no fork waits on a gate nobody holds.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_raw_wal_session_closes_the_commit_gate() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("raw.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let child = a.fork_branch().unwrap();
        a.wal_insert_begin().unwrap();
        a.execute("UPDATE t SET v = 'raw' WHERE id = 3").unwrap();
        a.wal_insert_end(true).unwrap();
        assert_eq!(
            db.branches.trunk_commit_seq() % 2,
            0,
            "catalog={catalog}: the raw session left the commit gate open"
        );
        write_v(&a, 40, "after");
        let c = child.connect().unwrap();
        assert_eq!(read_wide(&c, 3), "trunk-3", "catalog={catalog}");
        assert_eq!(read_wide(&c, 40), "trunk-40", "catalog={catalog}: the next commit took no decisions");
    }
}

/// Review A-F1: a raw WAL session is the trunk's alone. On a branch connection it would write the
/// branch's pages into the trunk's WAL and open the trunk's commit gate without its write lock.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_raw_wal_session_is_refused_on_a_branch() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("rawb.db"), opts(false, SyncClass::Fsync));
    let a = db.connect().unwrap();
    seed(&a);
    let b = a.fork_branch().unwrap();
    let bc = b.connect().unwrap();
    assert!(bc.wal_insert_begin().is_err(), "a raw WAL session began on a branch");
}

/// Review A-F1, lead review 1 item 5 (b): a raw WAL session that commits nothing still leaves the
/// commit gate closed, and a commit from another trunk connection afterwards goes through (an open
/// gate makes it panic on "two trunk commits inside the commit gate at once").
#[cfg(feature = "conn_raw_api")]
#[test]
fn an_empty_raw_wal_session_leaves_the_gate_closed_for_another_connection() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("raw0.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let child = a.fork_branch().unwrap();
        a.wal_insert_begin().unwrap();
        a.wal_insert_end(true).unwrap();
        assert_eq!(db.branches.trunk_commit_seq() % 2, 0, "catalog={catalog}: the gate was left open");
        let other = db.connect().unwrap();
        write_v(&other, 3, "other");
        let c = child.connect().unwrap();
        assert_eq!(read_wide(&c, 3), "trunk-3", "catalog={catalog}");
    }
}

/// Review B-F1's other door (a guard, green before the fix: the failed flight's leader poisoned the
/// journal itself then): a flight that fails to WRITE fail-stops the whole store as one taken-and-
/// failed does — no later commit writes an arena slot, and the close of the branch whose release it
/// carried frees nothing. Mutant `split_fail_stop` must fail it.
#[test]
fn a_flight_that_fails_to_write_fail_stops_every_later_write() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("write-fail.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let x = trunk.fork_branch().unwrap();
        let xc = x.connect().unwrap();
        write_v(&xc, 3, "x");
        let y = trunk.fork_branch().unwrap();
        let yc = y.connect().unwrap();
        write_v(&yc, 4, "y");
        let owned = x.owned_slots();
        assert!(!owned.is_empty(), "premise: the branch owns a slot");
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(x.reap().is_err(), "catalog={catalog}: premise: the release's flight failed");
        let arena = arena_path(&db);
        let before = std::fs::read(&arena).unwrap();
        assert!(
            yc.execute("UPDATE t SET v = 'y2' WHERE id = 9").is_err(),
            "catalog={catalog}: a branch committed after the store fail-stopped"
        );
        assert!(
            std::fs::read(&arena).unwrap() == before,
            "catalog={catalog}: an arena slot was written after the store fail-stopped"
        );
        drop(xc);
        for slot in &owned {
            assert!(
                !db.branch_slot_is_free(*slot),
                "catalog={catalog}: slot {slot} was freed though its Release is not durable"
            );
        }
        drop(yc);
        let _ = y.into_id();
    }
}

// ---- lead review 1 item 1: one full flush per flight (the arena ordered by a barrier) ----

/// Lead review 1 item 1: a D2 branch's first write is exactly ONE F_FULLFSYNC and no fsync(2). Its
/// slots need only be ORDERED before the log record that names them (F_BARRIERFSYNC on Apple); the
/// log's F_FULLFSYNC then drains the device's cache, slots included. Two full flushes (arena, then
/// log) were the shape before.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_first_write_is_exactly_one_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2fw.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let counted = syncs_of(|| write_v(&bc, 3, "mine"));
        assert_eq!(counted, (0, 1), "catalog={catalog}: a D2 first write's (fsync, F_FULLFSYNC)");
    }
}

/// Lead review 1 item 1 (V1 in-process, the CFW arm): `FE_V1_CREATES` (default 200) create-then-
/// first-write pairs at C=1, D2, issue exactly 2.00 to 2.01 F_FULLFSYNC per pair (the create's one
/// and the first write's one) and no fsync(2).
#[cfg(target_vendor = "apple")]
#[test]
fn v1_in_process_create_then_first_write_is_two_full_fsyncs_at_c1() {
    let _s = serial();
    let pairs = std::env::var("FE_V1_CREATES").ok().and_then(|v| v.parse().ok()).unwrap_or(200u64);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("v1cfw.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let counted = syncs_of(|| {
            for i in 0..pairs {
                let name = format!("cfw-{i}");
                trunk.create_branch(&name).unwrap();
                let c = db.connect_named(&name).unwrap();
                write_v(&c, 1 + (i as i64 % 50), "cfw");
            }
        });
        let per_pair = counted.1 as f64 / pairs as f64;
        assert_eq!(counted.0, 0, "catalog={catalog}: fsync(2) issued during D2 CFW: {counted:?}");
        assert!(
            (2.0..=2.01).contains(&per_pair),
            "catalog={catalog}: {} F_FULLFSYNC over {pairs} create+first-write pairs = {per_pair:.4} per pair",
            counted.1
        );
    }
}

/// The barrier counter (lead review 1 item 1): `barrier_file` is one F_BARRIERFSYNC at D2, one
/// fsync(2) at D1 and nothing at D0, each counted under its own primitive. Forced to fire each way.
#[cfg(target_vendor = "apple")]
#[test]
fn the_sync_counter_counts_a_barrier_under_its_own_primitive() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let file = super::journal::open_rw(&dir.path().join("f"), true).unwrap();
    super::journal::write_at(&file, b"x", 0).unwrap();
    let counted = |class| {
        let before = sync_counts();
        super::journal::barrier_file(&file, class).unwrap();
        let after = sync_counts();
        (after.fsync - before.fsync, after.full_fsync - before.full_fsync, after.barrier - before.barrier)
    };
    assert_eq!(counted(SyncClass::Off), (0, 0, 0), "D0 ordered a branch file");
    assert_eq!(counted(SyncClass::Fsync), (1, 0, 0), "a D1 barrier is one fsync(2)");
    assert_eq!(counted(SyncClass::FullFsync), (0, 0, 1), "a D2 barrier is one F_BARRIERFSYNC");
}

/// Lead review 1 item 1, the other half: a D2 first write still ORDERS its slots before its record
/// — exactly one F_BARRIERFSYNC beside its one F_FULLFSYNC. Mutant `no_arena_barrier` must fail it.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_first_write_orders_its_slots_with_one_barrier() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2fwb.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let before = sync_counts();
        write_v(&bc, 3, "mine");
        let after = sync_counts();
        assert_eq!(after.barrier - before.barrier, 1, "catalog={catalog}: the slots were not ordered");
        assert_eq!(after.full_fsync - before.full_fsync, 1, "catalog={catalog}");
    }
}

// ---- lead review 1 item 11: an early-released operation whose wait fails is undone or fenced ----

/// Lead review 1 item 11: a fork whose flight fails is not handed out, and it is released, so it is
/// not listed. (The release landed in 22434231a; mutant `fork_failure_kept` restores the leak.)
#[test]
fn a_fork_whose_flight_fails_is_released_not_leaked() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("fork-fail.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let first = trunk.fork_branch().unwrap();
        let before = db.branch_ids().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(trunk.create_branch("lost").is_err(), "catalog={catalog}: premise: the fork's flight failed");
        assert_eq!(db.branch_ids().unwrap(), before, "catalog={catalog}: a fork that was not handed out is listed");
        drop(first);
    }
}

/// Lead review 1 item 11: a branch commit whose flight fails returned an error, so nothing reads it
/// afterwards — the branch is refused (its state is in doubt until a reopen recovers it from disk),
/// never shown with the failed commit's write. After a reopen it reads its last durable commit.
#[test]
fn a_branch_commit_whose_flight_fails_is_never_read() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("commit-fail.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            {
                let bc = b.connect().unwrap();
                write_v(&bc, 3, "durable");
                db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
                assert!(
                    bc.execute("UPDATE t SET v = 'failed' WHERE id = 3").is_err(),
                    "catalog={catalog}: premise: the commit's flight failed"
                );
                let seen = bc.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows());
                if let Ok(rows) = seen {
                    assert_eq!(
                        rows[0][0],
                        crate::Value::from_text("durable"),
                        "catalog={catalog}: the failed commit's write was read on its own connection"
                    );
                }
            }
            let reopened = b.connect().and_then(|c| {
                c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows())
            });
            if let Ok(rows) = reopened {
                assert_eq!(
                    rows[0][0],
                    crate::Value::from_text("durable"),
                    "catalog={catalog}: the failed commit's write was read on a new connection"
                );
            }
            (b.into_id(), db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let b = db.branch(id).unwrap();
        assert_eq!(read_v(&b.connect().unwrap(), 3), "durable", "catalog={catalog}: after a reopen");
    }
}

/// Lead review 1 item 11: a lock-free fork that times out waiting for the trunk commit it forks
/// after is `Busy`, not handed out, and released: it is not listed, and after a reopen it is gone.
/// The publication wait is shortened for the test (`BranchStore::set_publish_wait`).
#[test]
fn a_fork_that_times_out_on_a_trunk_commit_is_busy_and_released() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("publish-timeout.db");
        let incarnation = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let a = db.connect().unwrap();
            seed_wide(&a);
            let first = a.fork_branch().unwrap().into_id();
            db.branches.set_publish_wait(std::time::Duration::from_millis(200));
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_TRUNK_DECIDED, std::sync::atomic::Ordering::Release);
            let writer = {
                let a = a.clone();
                std::thread::spawn(move || write_v(&a, 7, "c1"))
            };
            wait_hold(&hold, super::store::HOLD_TRUNK_DECIDED);
            let forked = db.connect().unwrap().create_branch("late");
            hold.store(0, std::sync::atomic::Ordering::Release);
            writer.join().unwrap();
            assert!(
                matches!(forked, Err(LimboError::Busy)),
                "catalog={catalog}: a fork past its publication wait: {forked:?}"
            );
            assert_eq!(db.branch_ids().unwrap(), vec![first], "catalog={catalog}: the timed-out fork is listed");
            assert_eq!(db.branch_named("late").unwrap(), None, "catalog={catalog}");
            db.incarnation
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        assert_eq!(db.branch_ids().unwrap().len(), 1, "catalog={catalog}: after a reopen");
        assert_eq!(db.branch_named("late").unwrap(), None, "catalog={catalog}: after a reopen");
    }
}
