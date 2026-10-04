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
                            let hi = states.read().unwrap().len();
                            let seen = read_t(&branch.connect().unwrap());
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
