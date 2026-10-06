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

/// `open_at`, returning the refusal rather than panicking on it.
fn try_open_at(path: &Path, opts: DatabaseOpts) -> crate::Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(io, path.to_str().unwrap(), OpenFlags::Create, opts, None, Arc::new(SqliteDialect))
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

/// D2: a branch's first write flushes its log with F_FULLFSYNC, its arena slots reaching the device
/// first by one plain fsync(2) (ruling 85a032f01).
///
/// FLAGGED TEST EDIT (lead ruling 85a032f01, a registered law change): it asserted no fsync(2)
/// (the arena was F_FULLFSYNC'd, then barriered); it pins the ruling's (1 fsync, 1 F_FULLFSYNC).
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
        assert_eq!(counted, (1, 1), "catalog={catalog}: a D2 branch commit's (fsync, F_FULLFSYNC)");
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
/// it. No sync of that commit is fsync(2).
///
/// FLAGGED TEST EDIT (lead review 1 item 6, a registered law change): the pre-image is now ORDERED
/// before the commit's frames (F_BARRIERFSYNC) and made durable by the WAL's own F_FULLFSYNC, so
/// the commit issues one F_FULLFSYNC and at least one barrier, where it issued two F_FULLFSYNC.
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
        let before = sync_counts();
        trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
        let after = sync_counts();
        let counted = (
            after.fsync - before.fsync,
            after.full_fsync - before.full_fsync,
            after.barrier - before.barrier,
        );
        assert_eq!(
            counted.0, 0,
            "catalog={catalog}: a pre-image barrier under a fullfsync trunk issued fsync(2): {counted:?}"
        );
        assert_eq!(counted.1, 1, "catalog={catalog}: the WAL's F_FULLFSYNC: {counted:?}");
        assert!(counted.2 >= 1, "catalog={catalog}: the pre-image's barrier: {counted:?}");
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
/// keeps it after the install and after a reopen. (The store here is never a splice store:
/// `R11_SPLICE` steers only the store's model tests.)
#[test]
fn a_released_name_is_free_while_a_fuzzy_checkpoint_is_in_flight() {
    let _s = serial();
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
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
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

/// Lead review 1 item 1: a D2 branch's first write is exactly ONE F_FULLFSYNC: its slots only reach
/// the device first (one plain fsync, ruling 85a032f01), and the log's F_FULLFSYNC drains the
/// device's cache, slots included. Two full flushes (arena, then log) were the shape before.
///
/// FLAGGED TEST EDIT (lead ruling 85a032f01): it pinned (fsync 0, F_FULLFSYNC 1) for the barrier
/// design; the ruling's design is (1, 1).
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
        assert_eq!(counted, (1, 1), "catalog={catalog}: a D2 first write's (fsync, F_FULLFSYNC)");
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
        // FLAGGED TEST EDIT (lead ruling 85a032f01): each first write's arena takes one plain
        // fsync(2) ahead of its log's F_FULLFSYNC; it asserted none (the barrier design).
        assert_eq!(counted.0, pairs, "catalog={catalog}: fsync(2) issued during D2 CFW: {counted:?}");
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

/// Lead review 1 item 1, the other half: a D2 first write still sends its slots to the device before
/// its record's flush — exactly one plain fsync(2) beside its one F_FULLFSYNC, and no barrier.
/// Mutant `no_arena_sync` must fail it.
///
/// FLAGGED TEST EDIT (lead ruling 85a032f01): it pinned one F_BARRIERFSYNC (the barrier design).
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
        assert_eq!(after.fsync - before.fsync, 1, "catalog={catalog}: the slots were not synced");
        assert_eq!(after.barrier - before.barrier, 0, "catalog={catalog}");
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

// ---- lead review 1 item 2: a named create runs no catalog query under the store mutex ----

/// Lead review 1 item 2: in a catalog store, a create of a NEW name — every successful server
/// create — asks the catalog nothing: the name filter (every name the store ever held, insert-only)
/// says no branch has it. Measured after a checkpoint, an eviction-free reopen and the filter's
/// build, over `FE_NAMED_CREATES` (default 300) creates: 0 catalog queries. A taken name is still
/// refused, before and after the reopen.
#[test]
fn a_named_create_of_a_new_name_asks_the_catalog_nothing() {
    let _s = serial();
    let creates = std::env::var("FE_NAMED_CREATES").ok().and_then(|v| v.parse().ok()).unwrap_or(300u64);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("filter.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..50 {
            trunk.create_branch(&format!("old-{i}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    assert!(trunk.create_branch("old-7").is_err(), "a name held before the reopen was given again");
    let before = db.branch_catalog_counters().2;
    for i in 0..creates {
        trunk.create_branch(&format!("new-{i}")).unwrap();
    }
    let queries = db.branch_catalog_counters().2 - before;
    assert_eq!(queries, 0, "{creates} named creates of new names made {queries} catalog queries");
    assert!(trunk.create_branch("new-3").is_err(), "a name created since the reopen was given again");
}

// ---- lead review 1 item 6: a retaining trunk commit is one full flush, and forks inside its gate
// ride it ----

/// Lead review 1 item 6: a D2 trunk commit that retains a pre-image for a live child is exactly
/// ONE F_FULLFSYNC (its WAL's) and no fsync(2): the pre-image is ORDERED before the commit's frames
/// (F_BARRIERFSYNC on the branch files), and the WAL's F_FULLFSYNC makes it durable with them.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_retaining_trunk_commit_is_one_full_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2rt.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let before = db.branch_trunk_retained();
        let counted = syncs_of(|| write_v(&trunk, 7, "new"));
        assert!(db.branch_trunk_retained() > before, "catalog={catalog}: premise: the commit retained");
        assert_eq!(counted, (0, 1), "catalog={catalog}: a retaining D2 trunk commit's (fsync, F_FULLFSYNC)");
        assert_eq!(read_v(&b.connect().unwrap(), 7), "trunk-7", "catalog={catalog}");
    }
}

/// Lead review 1 item 6: forks registered while a D2 trunk commit is inside its gate — before its
/// pre-image barrier, or after it and before its WAL flush — are made durable by that commit's
/// WAL F_FULLFSYNC: the commit and the forks together issue ONE F_FULLFSYNC.
#[cfg(target_vendor = "apple")]
#[test]
fn forks_inside_a_d2_commit_gate_ride_its_wal_flush() {
    let _s = serial();
    for stage in [super::store::HOLD_TRUNK_DECIDED, super::store::HOLD_TRUNK_BARRIER_DONE] {
        for catalog in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let db = open_at(&dir.path().join("ride.db"), opts(catalog, SyncClass::FullFsync));
            let a = db.connect().unwrap();
            seed_wide(&a);
            let _first = a.fork_branch().unwrap().into_id();
            let hold = db.branches.trunk_commit_hold.clone();
            let forks_before = db.branch_stats().unwrap().work.trunk_forks;
            let counted = syncs_of(|| {
                hold.store(stage, std::sync::atomic::Ordering::Release);
                let writer = {
                    let a = a.clone();
                    std::thread::spawn(move || write_v(&a, 7, "c1"))
                };
                wait_hold(&hold, stage);
                let forkers: Vec<_> = (0..3)
                    .map(|i| {
                        let db = db.clone();
                        std::thread::spawn(move || db.connect().unwrap().create_branch(&format!("ride-{i}")).unwrap())
                    })
                    .collect();
                // The forks have registered before the commit goes on.
                let t = std::time::Instant::now();
                while db.branch_stats().unwrap().work.trunk_forks < forks_before + 3 {
                    assert!(t.elapsed() < std::time::Duration::from_secs(10), "the forks never registered");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                hold.store(0, std::sync::atomic::Ordering::Release);
                writer.join().unwrap();
                for f in forkers {
                    f.join().unwrap();
                }
            });
            assert_eq!(
                counted,
                (0, 1),
                "stage={stage} catalog={catalog}: a D2 trunk commit and three forks inside its gate"
            );
        }
    }
}

/// Lead review 1 item 6, the count half of M-j (the ordering itself is C1b's): a retaining D2
/// trunk commit barriers the arena (the pre-image's slot) AND the log (its `TrunkRetain` record)
/// before its frames — two F_BARRIERFSYNC. Mutant `no_log_barrier` must fail it.
#[cfg(target_vendor = "apple")]
#[test]
fn a_retaining_d2_trunk_commit_barriers_its_slot_and_its_record() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2rb.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _b = trunk.fork_branch().unwrap();
        let before = sync_counts();
        write_v(&trunk, 7, "new");
        let after = sync_counts();
        assert_eq!(after.barrier - before.barrier, 2, "catalog={catalog}: the slot's and the record's barriers");
    }
}

// ---- lead review 1 item 10: the trunk commit path pays nothing it does not need ----

/// Lead review 1 item 10: a trunk commit on a store whose trunk has no live child opens no commit
/// gate (nothing can fork lock-free while it is in flight: the first child needs its WAL write
/// lock) and takes no copy decision.
#[test]
fn a_trunk_commit_with_no_child_opens_no_commit_gate() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("nogate.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        drop(trunk.fork_branch().unwrap());
        let before = db.branches.trunk_commit_seq();
        write_v(&trunk, 7, "new");
        assert_eq!(db.branches.trunk_commit_seq(), before, "catalog={catalog}: a childless commit opened the gate");
    }
}

/// Lead review 1 item 10: a trunk commit that retained nothing and relies on no release still in
/// the air takes no store mutex for its barrier: nothing is to be made durable.
#[test]
fn a_trunk_commit_with_nothing_to_make_durable_takes_no_store_mutex() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("nolock.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _b = trunk.fork_branch().unwrap();
        // The first write after the fork retains; the second, with no fork since, does not.
        write_v(&trunk, 7, "first");
        let before = db.branches.barrier_locks();
        write_v(&trunk, 7, "second");
        assert_eq!(db.branches.barrier_locks(), before, "catalog={catalog}: the barrier took the store mutex");
    }
}

/// Lead review 1 item 10's guard (mutant M-f's schema hazard, which a commit that opens no gate
/// would reopen): a lock-free fork whose read snapshot predates a DDL commit made while the trunk
/// had no child — so no gate moved — and which registers after the trunk's first child was forked
/// under the WAL write lock, still sees the DDL: that locked fork moves the gate's count. Green
/// before the gate skip (the DDL commit moved it); mutant `no_locked_fork_bump` must fail it after.
#[test]
fn a_lock_free_fork_sees_ddl_committed_while_the_trunk_had_no_child() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("ddl0.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        drop(trunk.fork_branch().unwrap());
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FORK_REGISTERING, std::sync::atomic::Ordering::Release);
        // F's lock-free attempt holds at the hook with its snapshot taken while the trunk had no
        // child.
        let f = {
            let db = db.clone();
            std::thread::spawn(move || db.connect().unwrap().create_branch("f").unwrap())
        };
        wait_hold(&hold, super::store::HOLD_FORK_REGISTERING);
        trunk.execute("CREATE TABLE u(x)").unwrap();
        trunk.execute("INSERT INTO u VALUES (1)").unwrap();
        // The trunk's first child, under the WAL write lock (its own lock-free attempt passes the
        // hook, which F holds).
        let g = trunk.fork_branch().unwrap();
        hold.store(0, std::sync::atomic::Ordering::Release);
        f.join().unwrap();
        let c = db.connect_named("f").unwrap();
        let n = c
            .prepare("SELECT count(*) FROM u")
            .and_then(|mut s| s.run_collect_rows())
            .unwrap_or_else(|e| panic!("catalog={catalog}: the fork missed a DDL commit: {e}"));
        assert_eq!(n[0][0].as_int(), Some(1), "catalog={catalog}");
        drop(g);
    }
}

// ---- lead review 1 item 7: a checkpoint never flushes inside the store mutex ----

/// Puts the compaction threshold back when a test that lowered it ends, however it ends.
struct Threshold;

impl Threshold {
    fn set(bytes: u64) -> Self {
        super::journal::set_compact_threshold(bytes);
        Threshold
    }
}

impl Drop for Threshold {
    fn drop(&mut self) {
        super::journal::set_compact_threshold(0);
    }
}

/// Lead review 1 item 7: a catalog store's checkpoints — started by the create or first write that
/// crossed the threshold, at the default (fuzzy) mode — issue NO sync while any thread holds the
/// store mutex: neither the settling of what is buffered, nor the log's cut at the install. With
/// the threshold lowered so `FE_CKPT_FORKS` (default 400) create-then-first-write pairs run at least
/// three checkpoints, the syncs under the store mutex do not move.
#[test]
fn a_checkpoint_issues_no_sync_inside_the_store_mutex() {
    let _s = serial();
    // FLAGGED TEST EDIT (own test, premise only): 800 pairs, not 400, since end frames no longer
    // count toward the threshold (review 2 #8) and 400 ran two checkpoints.
    let pairs = std::env::var("FE_CKPT_FORKS").ok().and_then(|v| v.parse().ok()).unwrap_or(800u64);
    for class in [SyncClass::Fsync, SyncClass::FullFsync] {
        let dir = tempfile::TempDir::new().unwrap();
        // FLAGGED TEST EDIT (lead-directed, review 4 #8): pinned to fuzzy checkpoints, the mode its
        // claim is about; it followed R11_CKPT, and went red in the catsharp arm.
        let db = open_at(
            &dir.path().join("ckpt-nosync.db"),
            opts(true, class).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _t = Threshold::set(8 << 10);
        let installed = db.branch_checkpoint_counters()[0];
        let before = super::store::syncs_under_store_mutex();
        for i in 0..pairs {
            let b = trunk.fork_branch().unwrap();
            write_v(&b.connect().unwrap(), 1 + (i as i64 % 50), "w");
            let _ = b.into_id();
        }
        db.branch_checkpoint_wait();
        let under = super::store::syncs_under_store_mutex() - before;
        let ran = db.branch_checkpoint_counters()[0] - installed;
        assert!(ran >= 3, "class={class:?}: premise: at least three checkpoints ran ({ran})");
        assert_eq!(under, 0, "class={class:?}: {ran} checkpoints issued {under} syncs inside the store mutex");
    }
}

/// Lead review 1 item 7, rule 2 under the new capture: a fuzzy checkpoint captured while a release's
/// flight is still in the air neither waits for that flight nor frees what the release frees: the
/// slots stay out of the allocator until the release is durable, and then are free exactly once —
/// not in use, after the install and after a reopen. Mutant `deferred_matured_at_capture` must
/// fail it; at the head before item 7 the capture waited for the flight.
#[test]
fn a_capture_neither_waits_for_nor_frees_a_release_in_the_air() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("deferred.db");
    let (owned, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 3, "x");
        let owned = x.owned_slots();
        assert!(!owned.is_empty(), "premise: the branch owns a slot");
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let release = std::thread::spawn(move || x.reap().map(|_| ()));
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let starter = {
            let db = db.clone();
            std::thread::spawn(move || db.branch_checkpoint_fuzzy_now())
        };
        let t = std::time::Instant::now();
        while !starter.is_finished() && t.elapsed() < std::time::Duration::from_secs(2) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        if !starter.is_finished() {
            // The capture is waiting for the flight, holding the store mutex: let it go first.
            hold.store(0, std::sync::atomic::Ordering::Release);
            let _ = release.join();
            let _ = starter.join();
            panic!("the capture waited for a flight in the air");
        }
        for slot in &owned {
            assert!(
                !db.branch_slot_is_free(*slot),
                "slot {slot} was freed before the release that frees it was durable"
            );
        }
        hold.store(0, std::sync::atomic::Ordering::Release);
        release.join().unwrap().unwrap();
        assert!(starter.join().unwrap().unwrap(), "premise: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        let in_use = db.branch_slots_in_use();
        for slot in &owned {
            assert!(!in_use.contains(slot), "slot {slot} is still in use after its release");
        }
        (owned, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    let in_use = db.branch_slots_in_use();
    for slot in &owned {
        assert!(!in_use.contains(slot), "slot {slot} is in use after a reopen");
    }
    // Free exactly once: reused by new branches without two of them sharing it.
    let trunk = db.connect().unwrap();
    let mut seen = std::collections::HashSet::new();
    for i in 0..8 {
        let b = trunk.fork_branch().unwrap();
        write_v(&b.connect().unwrap(), 1 + i, "y");
        for s in b.owned_slots() {
            assert!(seen.insert(s), "slot {s} handed to two live branches");
        }
        let _ = b.into_id();
    }
}

/// Review 2 #5: a checkpoint that failed is not retried by the very next operation — the log is
/// past the threshold still, so without a back-off every create would start (and pay for) another
/// one: it waits for another threshold's worth of log.
#[test]
fn a_failed_checkpoint_is_not_retried_by_the_next_create() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    // FLAGGED TEST EDIT (lead-directed, review 4 #8): pinned to fuzzy checkpoints (it followed
    // R11_CKPT).
    let db = open_at(
        &dir.path().join("ckpt-backoff.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    let _t = Threshold::set(8 << 10);
    db.branch_failpoint(Some(BranchFailpoint::CheckpointWriteFails));
    // Forks until a checkpoint has started and failed.
    let started = db.branch_checkpoint_counters()[1];
    let t = std::time::Instant::now();
    while db.branch_checkpoint_counters()[1] == started {
        let _ = trunk.fork_branch().unwrap().into_id();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "no checkpoint started");
    }
    db.branch_checkpoint_wait();
    let after_failure = db.branch_checkpoint_counters()[1];
    for _ in 0..5 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branch_checkpoint_counters()[1],
        after_failure,
        "the creates right after a failed checkpoint started another"
    );
}

// ---- lead ruling 85a032f01 (review 1 item 1, revised): the arena is plain-fsynced, and recovery
// checks the last flight's slots ----

/// Ruling 85a032f01: a D2 first write is exactly one F_FULLFSYNC (the log's) and one plain
/// fsync(2) (the arena's), and no barrier: the log's device-wide flush makes the arena durable too.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d2_first_write_is_one_full_fsync_and_one_plain_fsync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("d2pf.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        let bc = b.connect().unwrap();
        let before = sync_counts();
        write_v(&bc, 3, "mine");
        let after = sync_counts();
        assert_eq!(
            (
                after.fsync - before.fsync,
                after.full_fsync - before.full_fsync,
                after.barrier - before.barrier
            ),
            (1, 1, 0),
            "catalog={catalog}: a D2 first write's (fsync, F_FULLFSYNC, barrier)"
        );
    }
}

/// Ruling 85a032f01: with the arena only plain-fsynced, a power cut can keep the last flight's log
/// record and lose the slot it names. Recovery checks every slot the LAST flight names and drops
/// the flight if one fails: the branch reads its previous version, with no error. (A slot named by
/// an older flight may have been reused since; only the last flight's are checked.)
#[cfg(unix)]
#[test]
fn a_last_flight_whose_slot_never_reached_the_disk_is_dropped() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("lostslot.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            write_v(&bc, 3, "durable");
            let before: std::collections::HashSet<u32> = b.owned_slots().into_iter().collect();
            write_v(&bc, 3, "lost");
            let fresh: Vec<u32> = b.owned_slots().into_iter().filter(|s| !before.contains(s)).collect();
            assert_eq!(fresh.len(), 1, "catalog={catalog}: premise: the last write took one fresh slot");
            drop(bc);
            let arena = arena_path(&db);
            let id = b.into_id();
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            // The slot's bytes never reached the disk — and neither did the confirmation the flight
            // writes, unsynced, once its flush returned (FLAGGED TEST EDIT, own test from
            // f112173f6: the power cut it models loses that write too; a confirmed flight's bad
            // slot is damage, refused when read, by a_corrupted_arena_slot_is_an_error_not_a_wrong_page).
            let f = std::fs::OpenOptions::new().write(true).open(&arena).unwrap();
            use std::os::unix::fs::FileExt;
            f.write_all_at(&vec![0u8; 4096], fresh[0] as u64 * 4096).unwrap();
            let log = std::fs::OpenOptions::new()
                .write(true)
                .open(arena.to_str().unwrap().replace("-branch-arena", "-branch-log"))
                .unwrap();
            log.write_all_at(&[0u8; 4], 36).unwrap();
            (id, incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let b = db.branch(id).unwrap();
        let got = b.connect().and_then(|c| c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows()));
        match got {
            Ok(rows) => assert_eq!(
                rows[0][0],
                crate::Value::from_text("durable"),
                "catalog={catalog}: the dropped flight's write was read"
            ),
            Err(e) => panic!("catalog={catalog}: the last flight's lost slot was not dropped at recovery: {e}"),
        }
    }
}

/// Review 2 #12: a catalog store's refusals name the files it really has (`-branch-cat`, not a
/// snapshot), and a catalog store whose log and arena were moved aside is refused at open instead
/// of opening with every branch's slots gone.
#[test]
fn a_catalog_store_names_its_catalog_and_refuses_a_missing_arena() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("catfiles.db");
    {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        write_v(&b.connect().unwrap(), 3, "x");
        db.branch_compact_now().unwrap();
        let _ = b.into_id();
    }
    let base = path.to_str().unwrap();
    for suffix in ["-branch-log", "-branch-arena"] {
        std::fs::rename(format!("{base}{suffix}"), format!("{base}{suffix}.aside")).unwrap();
    }
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let opened = Database::open_file_with_flags(
        io,
        base,
        OpenFlags::Create,
        opts(true, SyncClass::Fsync),
        None,
        Arc::new(SqliteDialect),
    );
    assert!(opened.is_err(), "a catalog store opened with its log and arena gone");
}

// ---- review 3 #1 (= skill 2 #1): a pre-image kept by a refused decision pass is still made durable
// before the commit's frames ----

/// Review 3 #1, hardened by review 5 #3: a trunk commit whose decision pass is refused after it kept
/// page P's pre-image, and is retried, takes no decision for P again (P's written epoch is the
/// commit's own). The pre-image's record must still be durable before the commit's frames: the
/// process is then killed (forked child, `_exit`: nothing more flushed, no destructor runs), and
/// after the reopen the live child reads its fork-point row. Premises: the refused pass kept exactly
/// one pre-image, the retry none (or, in the refused-probe arm, only the page it had not reached),
/// and the trunk read the new row before the kill. Arms: the statement retried; the refused
/// transaction rolled back and a later one rewriting the page; and (catalog) the refusal a catalog
/// probe of the commit's SECOND page makes after its first page was kept. Mutant `no_retain_floor`
/// must fail every arm.
///
/// FLAGGED TEST EDIT (lead-directed, review 5 #3): this replaces the single four-arm test (whose
/// first failing arm hid the others), ending its process by a kill instead of a drop, with the
/// premise asserts above and the refused-probe arm added.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Refusal {
    Retry,
    Rollback,
    SecondPageProbe,
}

fn a_refused_decision_pass_survives_a_kill(name: &str, catalog: bool, refusal: Refusal) {
    use crate::branch::fork_driver;
    let Some(sentinel) = fork_driver::alone(name) else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("retain-floor.db");
    let id_file = dir.path().join("child-id");
    // SAFETY: the child runs the workload and `_exit`s; it never returns into the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let id = refused_pass_workload(&path, catalog, refusal);
            std::fs::write(&id_file, id.0.to_string()).unwrap();
        }))
        .map_or(100, |()| 0);
        // SAFETY: ends the child as a kill would: nothing more is flushed, no destructor runs.
        unsafe { libc::_exit(code) };
    }
    let code = fork_driver::exit_code(pid);
    assert_eq!(code, 0, "catalog={catalog} {refusal:?}: the workload failed in the child (100: a panic)");
    let child = BranchId(std::fs::read_to_string(&id_file).unwrap().parse().unwrap());
    let db = open_at(&path, opts(catalog, SyncClass::Fsync));
    let c = db.branch(child).unwrap().connect().unwrap();
    assert_eq!(read_wide(&c, 3), "trunk-3", "catalog={catalog} {refusal:?}: the child reads the trunk's new row after a kill");
    if refusal == Refusal::SecondPageProbe {
        assert_eq!(read_wide(&c, 40), "trunk-40", "catalog={catalog} {refusal:?}: row 40");
    }
    fork_driver::finished(&sentinel);
}

/// The forked child's part: the refused pass and its retry, then nothing more. Returns the child.
fn refused_pass_workload(path: &Path, catalog: bool, refusal: Refusal) -> BranchId {
    let retained = |db: &Arc<Database>| db.branch_stats().unwrap().work.trunk_pre_images_retained;
    let mut db = open_at(path, opts(catalog, SyncClass::Fsync));
    let a = db.connect().unwrap();
    seed_wide(&a);
    let child = a.fork_branch().unwrap().into_id();
    let mut a = a;
    if refusal == Refusal::SecondPageProbe {
        // Reopened, so no page's written epoch is known; the child's point read of row 3 dates
        // row 3's page (and every page above it), and leaves row 40's leaf to the commit's probe.
        db.branch_compact_now().unwrap();
        let incarnation = db.incarnation;
        drop(a);
        drop(db);
        db = reopen(path, opts(catalog, SyncClass::Fsync), incarnation);
        // The handle is kept, and detached again after the read: dropped, it would release the child.
        let b = db.branch(child).unwrap();
        let c = b.connect().unwrap();
        c.prepare("SELECT v FROM t WHERE id = 3").unwrap().run_collect_rows().unwrap();
        drop(c);
        let _ = b.into_id();
        a = db.connect().unwrap();
    }
    let before = retained(&db);
    match refusal {
        Refusal::Retry => {
            db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
            let mut stmt = a.prepare("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
            assert!(matches!(stmt.run_ignore_rows(), Err(LimboError::Busy)), "premise: the pass was refused");
            assert_eq!(retained(&db) - before, 1, "premise: the refused pass kept the pre-image");
            stmt.run_ignore_rows().unwrap();
            assert_eq!(retained(&db) - before, 1, "premise: the retry kept nothing more");
        }
        Refusal::Rollback => {
            db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
            a.execute("BEGIN").unwrap();
            a.execute("UPDATE t SET v = 'refused' WHERE id = 3").unwrap();
            assert!(matches!(a.execute("COMMIT"), Err(LimboError::Busy)), "premise: the pass was refused");
            assert_eq!(retained(&db) - before, 1, "premise: the refused pass kept the pre-image");
            let _ = a.execute("ROLLBACK");
            write_v(&a, 3, "new");
        }
        Refusal::SecondPageProbe => {
            db.branch_failpoint(Some(BranchFailpoint::TrunkProbeBusy));
            let mut stmt = a.prepare("UPDATE t SET v = 'new' WHERE id IN (3, 40)").unwrap();
            assert!(matches!(stmt.run_ignore_rows(), Err(LimboError::Busy)), "premise: the probe was refused");
            assert_eq!(retained(&db) - before, 1, "premise: the first page was kept before the refusal");
            stmt.run_ignore_rows().unwrap();
            assert_eq!(retained(&db) - before, 2, "premise: the retry kept only the second page");
            assert_eq!(read_wide(&a, 40), "new", "premise: the trunk reads row 40's new value");
        }
    }
    assert_eq!(read_wide(&a, 3), "new", "premise: the trunk reads the new row");
    // The process ends here, with nothing else flushed.
    child
}

#[cfg(unix)]
#[test]
fn a_refused_decision_pass_survives_a_kill_snapshot_retry() {
    a_refused_decision_pass_survives_a_kill(
        "branch::fastest_tests::a_refused_decision_pass_survives_a_kill_snapshot_retry",
        false,
        Refusal::Retry,
    );
}

#[cfg(unix)]
#[test]
fn a_refused_decision_pass_survives_a_kill_snapshot_rollback() {
    a_refused_decision_pass_survives_a_kill(
        "branch::fastest_tests::a_refused_decision_pass_survives_a_kill_snapshot_rollback",
        false,
        Refusal::Rollback,
    );
}

#[cfg(unix)]
#[test]
fn a_refused_decision_pass_survives_a_kill_catalog_retry() {
    a_refused_decision_pass_survives_a_kill(
        "branch::fastest_tests::a_refused_decision_pass_survives_a_kill_catalog_retry",
        true,
        Refusal::Retry,
    );
}

#[cfg(unix)]
#[test]
fn a_refused_decision_pass_survives_a_kill_catalog_rollback() {
    a_refused_decision_pass_survives_a_kill(
        "branch::fastest_tests::a_refused_decision_pass_survives_a_kill_catalog_rollback",
        true,
        Refusal::Rollback,
    );
}

#[cfg(unix)]
#[test]
fn a_refused_decision_pass_survives_a_kill_catalog_second_page_probe() {
    a_refused_decision_pass_survives_a_kill(
        "branch::fastest_tests::a_refused_decision_pass_survives_a_kill_catalog_second_page_probe",
        true,
        Refusal::SecondPageProbe,
    );
}

// ---- review 3 #2: a catalog probe refused part-way is made again, not skipped for the process ----

/// Review 3 #2: the once-per-page catalog probe that dates a trunk page's last write
/// (`trunk_written_known`) is refused (`Busy`, as the catalog's lock would refuse it). The page must
/// not be taken as dated: when the probe was skipped for the rest of the process, the retried trunk
/// commit dated the page's last write at 0 and kept its pre-image for EVERY child back to 0, so a
/// child forked before the page's checkpointed version read the newer row. Arms: the refused probe
/// is the trunk commit's own (retried by its statement), or a branch read's before the commit.
#[test]
fn a_refused_catalog_probe_is_made_again() {
    let _s = serial();
    for refused_by_read in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("probe-busy.db");
        let (old, incarnation) = {
            let db = open_at(&path, opts(true, SyncClass::Fsync));
            let a = db.connect().unwrap();
            seed_wide(&a);
            let old = a.fork_branch().unwrap();
            write_v(&a, 3, "mid");
            // The pre-image kept for `old` (trunk-3) goes to the catalog; the reopen forgets every
            // page's written epoch.
            db.branch_compact_now().unwrap();
            (old.into_id(), db.incarnation)
        };
        let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
        let a = db.connect().unwrap();
        let young = a.fork_branch().unwrap();
        let y = young.connect().unwrap();
        // A point read (`read_wide` scans the table): every page the operations below touch but
        // row 3's leaf is probed here, so the failpoint's probe is that leaf's.
        let r50 = y.prepare("SELECT v FROM t WHERE id = 50").unwrap().run_collect_rows().unwrap();
        assert!(r50[0][0].to_string().starts_with("trunk-50-"), "premise: {:?}", r50[0][0]);
        let probes = db.branch_twk_counters().0;
        db.branch_failpoint(Some(BranchFailpoint::TrunkProbeBusy));
        if refused_by_read {
            let seen = y.prepare("SELECT v FROM t WHERE id = 3").unwrap().run_collect_rows();
            assert!(matches!(seen, Err(LimboError::Busy)), "premise: the read's probe was refused: {seen:?}");
            write_v(&a, 3, "new");
        } else {
            let mut stmt = a.prepare("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
            let first = stmt.run_ignore_rows();
            assert!(matches!(first, Err(LimboError::Busy)), "premise: the decision's probe was refused: {first:?}");
            stmt.run_ignore_rows().unwrap();
        }
        assert_eq!(read_wide(&y, 3), "mid", "refused_by_read={refused_by_read}: the young child");
        let b = db.branch(old).unwrap();
        assert_eq!(
            read_wide(&b.connect().unwrap(), 3),
            "trunk-3",
            "refused_by_read={refused_by_read}: the old child reads a row the trunk wrote after its fork"
        );
        assert!(
            db.branch_twk_counters().0 > probes,
            "refused_by_read={refused_by_read}: the refused probe was not made again"
        );
    }
}

// ---- review 4 #1 (= skill 2 #2): a fuzzy checkpoint commits nothing a failed flight carried ----

/// Review 4 #1 (= skill 2 #2): a fuzzy checkpoint captured while an operation's flight is in the
/// air commits the catalog only once everything it captured is durable. When that flight FAILS the
/// commit is refused, so an operation whose caller was told it failed never becomes durable through
/// the catalog. `fork`: the operation is a fork, else the release of a branch that wrote a page.
/// Returns the branch that must be there after a reopen, the live count it must show, and the
/// incarnation.
fn checkpoint_over_a_failed_flight(path: &Path, fork: bool) -> (BranchId, usize, u64) {
    let db = open_at(path, opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _anchor = trunk.fork_branch().unwrap().into_id();
    let x = trunk.fork_branch().unwrap();
    write_v(&x.connect().unwrap(), 3, "x");
    let owned = x.owned_slots();
    assert!(!owned.is_empty(), "premise: the branch owns a slot");
    let live = db.branch_stats().unwrap().live_branches;
    let hold = db.branches.trunk_commit_hold.clone();
    db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
    hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
    let (x_id, op) = if fork {
        let x_id = x.into_id();
        let db = db.clone();
        (x_id, std::thread::spawn(move || db.connect()?.fork_branch().map(|b| { let _ = b.into_id(); })))
    } else {
        (x.id(), std::thread::spawn(move || x.reap().map(|_| ())))
    };
    wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
    let started = db.branch_checkpoint_fuzzy_now();
    hold.store(0, std::sync::atomic::Ordering::Release);
    assert!(started.unwrap(), "fork={fork}: premise: a fuzzy checkpoint started");
    assert!(op.join().unwrap().is_err(), "fork={fork}: premise: the operation's flight failed");
    db.branch_checkpoint_wait();
    if !fork {
        for slot in &owned {
            assert!(
                !db.branch_slot_is_free(*slot),
                "slot {slot} was freed by a checkpoint though the release that frees it failed"
            );
        }
    }
    (x_id, live, db.incarnation)
}

fn after_the_reopen(path: &Path, fork: bool, (x_id, live, incarnation): (BranchId, usize, u64)) {
    let db = reopen(path, opts(true, SyncClass::Fsync), incarnation);
    assert_eq!(
        db.branch_stats().unwrap().live_branches,
        live,
        "fork={fork}: the failed operation became durable through the checkpoint"
    );
    let x = db.branch(x_id).expect("the branch is there after a reopen");
    assert_eq!(read_v(&x.connect().unwrap(), 3), "x", "fork={fork}");
}

/// Review 4 #1, release arm: the released branch's slots stay out of the allocator, and the branch
/// is back after a reopen.
#[test]
fn a_fuzzy_checkpoint_never_commits_a_release_whose_flight_failed() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("ckpt-failed-release.db");
    let kept = checkpoint_over_a_failed_flight(&path, false);
    after_the_reopen(&path, false, kept);
}

/// Review 4 #1, fork arm: the fork whose flight failed is not a live branch after a reopen.
#[test]
fn a_fuzzy_checkpoint_never_commits_a_fork_whose_flight_failed() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("ckpt-failed-fork.db");
    let kept = checkpoint_over_a_failed_flight(&path, true);
    after_the_reopen(&path, true, kept);
}

/// Review 4 #1: a fuzzy checkpoint whose store fail-stops after its wait and before its commit
/// commits nothing: a fail-stopped store writes nothing more (B-F1), the catalog included, so no
/// install is counted, and after a reopen the branch whose release failed is back. Mutant
/// `commit_poisoned` (the check before the commit left out) must fail it.
#[test]
fn a_fuzzy_checkpoint_commits_nothing_after_a_fail_stop() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("ckpt-after-fail-stop.db");
    let (id, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 3, "x");
        let id = x.id();
        let installed = db.branch_checkpoint_counters()[0];
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        let t = std::time::Instant::now();
        while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "the checkpoint never arrived");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(x.reap().is_err(), "premise: the release's flight failed");
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        assert_eq!(
            db.branch_checkpoint_counters()[0],
            installed,
            "a checkpoint committed and installed after the store fail-stopped"
        );
        (id, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    let x = db.branch(id).expect("the branch whose release failed is back after a reopen");
    assert_eq!(read_v(&x.connect().unwrap(), 3), "x");
}

// ---- skill review 2 #3: a logged Release raises the trunk's barrier floor ----

/// Skill review 2 #3: a Release logged in the store's class (a lease expiry's, through `log_all`)
/// rather than early-released is still durable in the trunk's class before the next trunk commit's
/// frames, so the commit's barrier covers it: the first trunk commit after the expiry syncs the
/// branch log (a barrier, or a flush) on top of its WAL's own sync, and the next one does not.
/// Arms: a D1 store under a fullfsync trunk, and a D0 store under a synchronous trunk, in which the
/// Release is only written. Mutant `log_all_no_floor` (the raise left out) must fail it.
#[test]
fn a_logged_release_is_durable_in_the_trunks_class_before_the_next_trunk_commit() {
    let _s = serial();
    let all = |c: SyncCounts| c.fsync + c.full_fsync + c.barrier;
    for catalog in [false, true] {
        for (store, pragma) in [
            (SyncClass::Fsync, "PRAGMA fullfsync = ON"),
            (SyncClass::Off, "PRAGMA synchronous = FULL"),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let lease = Some(std::time::Duration::from_secs(1));
            let db = open_at(&dir.path().join("logged-release.db"), opts(catalog, store).with_branch_lease(lease));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.execute(pragma).unwrap();
            let x = trunk.fork_branch().unwrap().into_id();
            db.branch_lease_clock_advance(std::time::Duration::from_secs(3600));
            assert!(db.expire_branches().unwrap().reaped.contains(&x), "premise: the lease expired");
            let before = all(sync_counts());
            write_v(&trunk, 7, "new");
            let first = all(sync_counts()) - before;
            let before = all(sync_counts());
            write_v(&trunk, 8, "new");
            let second = all(sync_counts()) - before;
            assert!(second >= 1, "catalog={catalog} store={store:?}: premise: the trunk commit synced its WAL");
            assert!(
                first > second,
                "catalog={catalog} store={store:?}: the first trunk commit after a logged Release made \
                 it no more durable than the store's class ({first} syncs, then {second})"
            );
        }
    }
}

// ---- review 3 #4: a failed barrier fail-stops; only an unsupported one falls back ----

/// Review 3 #4: an ordered flight's `F_BARRIERFSYNC` that fails as an I/O error does (EIO) is not
/// retried as an F_FULLFSYNC that may report success for pages the failed call lost: the flight
/// fails, the trunk commit relying on it is refused, and the store fail-stops (the next branch
/// commit is refused). Only a barrier the file system does not support (ENOTSUP) falls back to the
/// full sync, and the fallback is counted.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_barrier_fail_stops_and_only_an_unsupported_one_falls_back() {
    let _s = serial();
    for catalog in [false, true] {
        for (errno, name) in [(libc::EIO, "EIO"), (libc::ENOTSUP, "ENOTSUP")] {
            let dir = tempfile::TempDir::new().unwrap();
            let db = open_at(&dir.path().join("barrier-errno.db"), opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            super::journal::fail_next_barrier(errno);
            let before = sync_counts();
            let committed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
            let after = sync_counts();
            let fallbacks = after.barrier_fallback - before.barrier_fallback;
            if errno == libc::ENOTSUP {
                committed.unwrap();
                assert_eq!(fallbacks, 1, "catalog={catalog} {name}: the fallback was not counted");
                assert_eq!(read_v(&b.connect().unwrap(), 7), "trunk-7", "catalog={catalog} {name}");
                continue;
            }
            assert_eq!(fallbacks, 0, "catalog={catalog} {name}: a failed barrier fell back");
            assert!(
                committed.is_err(),
                "catalog={catalog} {name}: a trunk commit relied on a barrier that failed"
            );
            let mine = b.connect().and_then(|bc| bc.execute("UPDATE t SET v = 'mine' WHERE id = 3"));
            assert!(
                mine.is_err(),
                "catalog={catalog} {name}: the store did not fail-stop after a failed barrier"
            );
        }
    }
}

// ---- review 3 #3: ordered trunk mode trusts only a real F_FULLFSYNC of the branch files' device ----

/// An IO over the platform's whose files report, as `File::full_fsync_device`, what `device` says
/// (`None`: the file cannot say; `Some(d)`: d), and whose sync is a no-op when `noop_sync` is set.
struct DeviceIo {
    inner: Arc<dyn IO>,
    noop_sync: bool,
    device: fn(&dyn crate::io::File) -> Option<u64>,
}

struct DeviceFile {
    inner: Arc<dyn crate::io::File>,
    noop_sync: bool,
    device: fn(&dyn crate::io::File) -> Option<u64>,
}

impl crate::io::Clock for DeviceIo {
    fn current_time_monotonic(&self) -> crate::io::clock::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::io::clock::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for DeviceIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> crate::Result<Arc<dyn crate::io::File>> {
        Ok(Arc::new(DeviceFile {
            inner: self.inner.open_file(path, flags, direct)?,
            noop_sync: self.noop_sync,
            device: self.device,
        }))
    }
    fn remove_file(&self, path: &str) -> crate::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> crate::Result<()> {
        self.inner.step()
    }
    fn file_id(&self, path: &str) -> crate::Result<crate::io::FileId> {
        self.inner.file_id(path)
    }
}

impl crate::io::File for DeviceFile {
    fn lock_file(&self, exclusive: bool) -> crate::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> crate::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: crate::Completion) -> crate::Result<crate::Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: crate::Completion) -> crate::Result<crate::Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<crate::Buffer>>, c: crate::Completion) -> crate::Result<crate::Completion> {
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: crate::Completion, sync_type: crate::io::FileSyncType) -> crate::Result<crate::Completion> {
        if self.noop_sync {
            c.complete(0);
            return Ok(c);
        }
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> crate::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: crate::Completion) -> crate::Result<crate::Completion> {
        self.inner.truncate(len, c)
    }
    fn full_fsync_device(&self) -> Option<u64> {
        (self.device)(&*self.inner)
    }
}

/// Review 3 #3: a trunk commit under a fullfsync trunk lets its WAL F_FULLFSYNC make a kept
/// pre-image durable (ordered mode: the pre-image is only barriered) only when that flush is a real
/// F_FULLFSYNC of the branch files' own device, as the opened WAL file itself reports. A WAL whose
/// sync does nothing (any IO that is not the platform's), or one on another device (a symlinked
/// `-wal` on another volume), must cost the pre-image its own F_FULLFSYNC. Control: the platform's
/// file, reporting its own device, stays ordered.
#[cfg(target_vendor = "apple")]
#[test]
fn ordered_trunk_mode_trusts_only_a_full_fsync_of_the_branch_files_device() {
    let _s = serial();
    let other = |f: &dyn crate::io::File| -> Option<u64> { Some(f.full_fsync_device().map_or(1, |d| d ^ 1)) };
    let arms: [(&str, bool, fn(&dyn crate::io::File) -> Option<u64>); 3] = [
        ("no-op sync", true, |_| None),
        ("another device", false, other),
        ("control", false, |f| f.full_fsync_device()),
    ];
    for catalog in [false, true] {
        for (arm, noop_sync, device) in arms {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("ordered.db");
            let io: Arc<dyn IO> = Arc::new(DeviceIo {
                inner: Arc::new(PlatformIO::new().unwrap()),
                noop_sync,
                device,
            });
            let db = Database::open_file_with_flags(
                io,
                path.to_str().unwrap(),
                OpenFlags::Create,
                opts(catalog, SyncClass::Fsync),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            let before = sync_counts();
            trunk.execute("UPDATE t SET v = 'new' WHERE id = 7").unwrap();
            let after = sync_counts();
            let (full, barrier) = (after.full_fsync - before.full_fsync, after.barrier - before.barrier);
            let wal = u64::from(!noop_sync);
            if arm == "control" {
                assert!(barrier >= 1 && full == wal, "catalog={catalog} {arm}: not ordered: full={full} barrier={barrier}");
            } else {
                assert!(
                    full > wal && barrier == 0,
                    "catalog={catalog} {arm}: the pre-image counted on a WAL flush that cannot carry it: \
                     full={full} (the WAL's own: {wal}) barrier={barrier}"
                );
            }
            assert_eq!(read_v(&b.connect().unwrap(), 7), "trunk-7", "catalog={catalog} {arm}");
        }
    }
}

// ---- review 3 #5: an arena sync failure in a compaction or a checkpoint fail-stops ----

/// Review 3 #5: an arena sync that fails during a compaction (snapshot store) or a catalog
/// checkpoint (here a fuzzy one, captured while a branch commit's flight is in the air, so it syncs
/// the arena) fail-stops the store: a later sync of the same file may report success for pages the
/// failed one lost, so nothing more may be acknowledged. The next branch commit is refused, and a
/// branch released afterwards frees nothing. Mutant `checkpoint_sync_error_kept` (the checkpoint's
/// fail-stop left out) must fail the catalog arm.
#[test]
fn an_arena_sync_failure_in_a_compaction_or_checkpoint_fail_stops() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("arena-sync-fails.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 3, "x");
        let owned = x.owned_slots();
        if catalog {
            let y = trunk.fork_branch().unwrap();
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
            let writer = std::thread::spawn(move || {
                let r = y.connect()?.execute("UPDATE t SET v = 'y' WHERE id = 5");
                let _ = y.into_id();
                r
            });
            wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
            db.branch_failpoint(Some(BranchFailpoint::ArenaSyncFails));
            let started = db.branch_checkpoint_fuzzy_now();
            hold.store(0, std::sync::atomic::Ordering::Release);
            assert!(started.unwrap(), "premise: a fuzzy checkpoint started");
            writer.join().unwrap().unwrap();
            db.branch_checkpoint_wait();
        } else {
            db.branch_failpoint(Some(BranchFailpoint::ArenaSyncFails));
            assert!(db.branch_compact_now().is_err(), "premise: the compaction's arena sync failed");
        }
        let committed = x.connect().and_then(|xc| xc.execute("UPDATE t SET v = 'after' WHERE id = 4"));
        assert!(
            committed.is_err(),
            "catalog={catalog}: a branch commit was acknowledged after an arena sync failed"
        );
        let _ = x.reap();
        for slot in &owned {
            assert!(
                !db.branch_slot_is_free(*slot),
                "catalog={catalog}: slot {slot} was freed after the store should have fail-stopped"
            );
        }
    }
}

// ---- review 3 #7: the name filter is bounded, sharded, built off the mutex and retried ----

/// Review 3 #7 (a): under churn of unique names (each created and dropped again), the name filter
/// never stalls one create with a rehash of every name it holds, and it holds about the names
/// held, not every name ever held. Over `n` cycles the most entries one insert moved stays within a
/// small multiple of n/256 (256 shards), and the filter ends under n/2 entries (a rebuild keeps it
/// near twice the larger of the live names and its rebuild floor). Before: one insert moved half
/// of all the names ever created, and the filter held all n.
#[test]
fn the_name_filter_stays_bounded_under_churn() {
    let _s = serial();
    let n = std::env::var("FE_NAME_CHURN").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000u64);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("name-churn.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for i in 0..n {
        let name = format!("churn-{i}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
    }
    db.branch_wait_name_filter();
    let (built, entries, moved, _, _) = db.branch_name_filter_stats();
    assert!(built, "premise: the name filter is built");
    assert!(
        moved <= 4 * n / 256 + 64,
        "one insert into the name filter moved {moved} entries ({n} names created)"
    );
    assert!(entries < n / 2, "the name filter holds {entries} entries after {n} create-and-drop cycles");
}

/// Review 3 #7 (d): a name filter whose catalog scan fails once is built by a retry, and the
/// failure is counted. Before, it stayed unbuilt for the life of the process, and every named
/// create went back to one catalog query under the store mutex.
#[test]
fn a_failed_name_scan_is_retried() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("name-retry.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("held").unwrap();
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    super::store::NAME_SCAN_FAILS.store(true, std::sync::atomic::Ordering::Release);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    db.branch_wait_name_filter();
    let consumed = !super::store::NAME_SCAN_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel);
    assert!(consumed, "premise: the open's build met the failing scan");
    let (built, _, _, builds, failed) = db.branch_name_filter_stats();
    assert_eq!(failed, 1, "the failed scan was not counted");
    assert!(built && builds == 1, "the name filter was not built after its scan failed once");
    assert!(db.connect().unwrap().create_branch("held").is_err(), "a held name was given again");
}

/// Review 3 #7 (e): each way a held name reaches the filter is load-bearing once the name's state
/// is evicted (resident cap 0) and only the filter stands between a create and the catalog: a name
/// created after the build (`note` into the built set), one created while the build ran (`note`
/// into `pending`, merged at the install), and one created before the build but not yet
/// checkpointed (the build's seed). Each must still be refused after a checkpoint evicted it.
/// Mutants `no_note_built`, `no_note_pending`, `no_pending_merge` and `no_seed` must fail it.
#[test]
fn every_held_name_reaches_the_name_filter() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for arm in ["built", "pending", "seeded"] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("name-inputs.db");
        let incarnation = {
            let db = open_at(&path, opts(true, SyncClass::Off));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.create_branch("old").unwrap();
            db.branch_compact_now().unwrap();
            if arm == "built" {
                db.branch_set_resident_cap(Some(0));
                trunk.create_branch("x").unwrap();
                db.branch_compact_now().unwrap();
                assert!(trunk.create_branch("x").is_err(), "{arm}: an evicted held name was given again");
                continue;
            }
            if arm == "seeded" {
                // In the log only: replayed at the reopen, before the filter's build starts.
                trunk.create_branch("x").unwrap();
            }
            db.incarnation
        };
        super::store::NAME_SCAN_HOLD.store(1, O::Release);
        let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
        let t = std::time::Instant::now();
        while super::store::NAME_SCAN_HOLD.load(O::Acquire) != 1 | super::store::HOLD_ARRIVED {
            if t.elapsed() > std::time::Duration::from_secs(10) {
                super::store::NAME_SCAN_HOLD.store(0, O::Release);
                panic!("{arm}: the name filter's build never reached its scan");
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let trunk = db.connect().unwrap();
        if arm == "pending" {
            trunk.create_branch("x").unwrap();
        }
        super::store::NAME_SCAN_HOLD.store(0, O::Release);
        db.branch_wait_name_filter();
        assert!(db.branch_name_filter_stats().0, "{arm}: premise: the name filter is built");
        db.branch_set_resident_cap(Some(0));
        db.branch_compact_now().unwrap();
        assert!(trunk.create_branch("x").is_err(), "{arm}: an evicted held name was given again");
        assert!(trunk.create_branch("old").is_err(), "{arm}: a checkpointed name was given again");
    }
}

// ---- review 5 #1, #2: recovery replays, then checks the slots the replayed state references,
// then decides; and it refuses before it changes anything ----

/// Review 5 #1: a last flight that keeps a trunk pre-image (TrunkRetain{S}) and releases the only
/// child it was kept for frees S inside that same flight. Once the flight lands, the next slot taken
/// is S (the free list is LIFO), and a branch commit overwrites it. A power cut before that commit's
/// flight leaves this flight last, its confirmation lost (written unsynced once its flush returned),
/// and S holding newer bytes than the record says. The flight was acknowledged — the release
/// returned — and nothing the replayed state references is bad, so it is kept: the released child
/// stays released. Before, every slot the flight named was checked, the flight was dropped, and
/// the child came back. Mutant `check_all_named_slots` (the old check) must fail it.
#[cfg(unix)]
#[test]
fn a_last_flight_that_frees_a_slot_it_names_is_kept_when_the_slot_is_reused() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("freed-in-flight.db");
        let (x_id, w_id, f_end, log, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed_wide(&trunk);
            // W sees the trunk as of before row 3's first rewrite, so the second rewrite keeps a
            // pre-image for X alone.
            let w = trunk.fork_branch().unwrap();
            write_v(&trunk, 3, "mid");
            let x = trunk.fork_branch().unwrap();
            let x_id = x.id();
            let log = db.branch_log_path().unwrap();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_TRUNK_DECIDED, std::sync::atomic::Ordering::Release);
            let committer = {
                let db = db.clone();
                std::thread::spawn(move || db.connect()?.execute("UPDATE t SET v = 'new' WHERE id = 3"))
            };
            wait_hold(&hold, super::store::HOLD_TRUNK_DECIDED);
            let kept: Vec<u32> =
                db.branch_slots_in_use().into_iter().filter(|s| !before.contains(s)).collect();
            assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the commit kept one pre-image");
            // X's release rides the flight that carries the pre-image's record, and frees its slot.
            let released = x.reap();
            hold.store(0, std::sync::atomic::Ordering::Release);
            released.unwrap();
            committer.join().unwrap().unwrap();
            assert!(db.branch_slot_is_free(kept[0]), "catalog={catalog}: premise: the release freed the slot");
            let f_end = std::fs::metadata(&log).unwrap().len();
            let wc = w.connect().unwrap();
            let owned: std::collections::HashSet<u32> = w.owned_slots().into_iter().collect();
            write_v(&wc, 40, "w");
            let fresh: Vec<u32> = w.owned_slots().into_iter().filter(|s| !owned.contains(s)).collect();
            assert_eq!(fresh, kept, "catalog={catalog}: premise: W's commit took the freed slot");
            drop(wc);
            (x_id, w.into_id(), f_end, log, db.incarnation)
        };
        // The power cut: the log as that flight left it (W's commit never flew), and its
        // confirmation lost with the power; the arena holds W's page in the slot.
        let f = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
        f.set_len(f_end).unwrap();
        use std::os::unix::fs::FileExt;
        f.write_all_at(&[0u8; 4], 36).unwrap();
        drop(f);
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        assert!(db.branch(x_id).is_err(), "catalog={catalog}: a child whose release was acknowledged came back");
        let w = db.branch(w_id).unwrap();
        let wc = w.connect().unwrap();
        assert_eq!(read_wide(&wc, 3), "trunk-3", "catalog={catalog}: W's fork point");
        assert_eq!(read_wide(&wc, 40), "trunk-40", "catalog={catalog}: W's unflown commit");
    }
}

/// Review 5 #2: an arena recovery cannot open (no permission) is an error, never a lost slot: the
/// open fails with the log byte-for-byte as it was, and once the arena can be opened again the
/// last flight's write is there. The confirmation is lost first, as after a power cut, so the
/// last flight's slots are checked. Before, the unopenable arena counted as a lost slot: the flight
/// was cut from the log, then the open failed anyway.
#[cfg(unix)]
#[test]
fn an_unopenable_arena_refuses_the_open_and_leaves_the_log_alone() {
    use std::os::unix::fs::{FileExt, PermissionsExt};
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("arena-eacces.db");
        let (id, log, arena, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            write_v(&b.connect().unwrap(), 3, "kept");
            (b.into_id(), db.branch_log_path().unwrap(), arena_path(&db), db.incarnation)
        };
        std::fs::OpenOptions::new().write(true).open(&log).unwrap().write_all_at(&[0u8; 4], 36).unwrap();
        let bytes = std::fs::read(&log).unwrap();
        std::fs::set_permissions(&arena, std::fs::Permissions::from_mode(0o000)).unwrap();
        let opened = try_open_at(&path, opts(catalog, SyncClass::Fsync));
        std::fs::set_permissions(&arena, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(opened.is_err(), "catalog={catalog}: a store opened over an arena it cannot open");
        drop(opened);
        assert!(std::fs::read(&log).unwrap() == bytes, "catalog={catalog}: the refused open changed the log");
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let b = db.branch(id).unwrap();
        assert_eq!(read_v(&b.connect().unwrap(), 3), "kept", "catalog={catalog}: the last flight's write");
    }
}

/// Review 5 #2: a store whose log or arena is missing is refused at open, before anything is changed,
/// and the refusal names the missing file: a catalog store before its first checkpoint (its meta row
/// names no state yet; the log holds all of it) and after one, each file alone, and a snapshot store
/// whose log is missing since its last compaction. Before, a missing file was created empty and the
/// store opened with what it held gone, or was refused only once a checkpoint had counted states.
/// Once the file is back, the store opens with every write.
#[test]
fn a_store_missing_its_log_or_arena_is_refused_at_open() {
    let _s = serial();
    for (catalog, checkpointed, missing) in [
        (true, false, "-branch-arena"),
        (true, false, "-branch-log"),
        (true, true, "-branch-arena"),
        (true, true, "-branch-log"),
        (false, true, "-branch-log"),
    ] {
        let arm = format!("catalog={catalog} checkpointed={checkpointed} missing={missing}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("missing.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            write_v(&bc, 3, "first");
            if checkpointed {
                db.branch_compact_now().unwrap();
            }
            write_v(&bc, 4, "second");
            drop(bc);
            (b.into_id(), db.incarnation)
        };
        let base = path.to_str().unwrap();
        let gone = format!("{base}{missing}");
        let log = format!("{base}-branch-log");
        std::fs::rename(&gone, format!("{gone}.aside")).unwrap();
        let log_bytes = (missing != "-branch-log").then(|| std::fs::read(&log).unwrap());
        let refused = match try_open_at(&path, opts(catalog, SyncClass::Fsync)) {
            Ok(_) => panic!("{arm}: the store opened"),
            Err(e) => e.to_string(),
        };
        assert!(refused.contains(&gone), "{arm}: the refusal does not name the missing file: {refused}");
        if let Some(bytes) = log_bytes {
            assert!(std::fs::read(&log).unwrap() == bytes, "{arm}: the refused open changed the log");
        }
        assert!(!std::path::Path::new(&gone).exists(), "{arm}: the refused open created the missing file");
        std::fs::rename(format!("{gone}.aside"), &gone).unwrap();
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let c = db.branch(id).unwrap().connect().unwrap();
        assert_eq!(read_v(&c, 3), "first", "{arm}");
        assert_eq!(read_v(&c, 4), "second", "{arm}");
    }
}

/// Review 5 #10: an arena whose last writes may never have reached the disk — a D0 store's, whose
/// flights never sync it — counts as unsynced at the reopen, so the first flight, compaction or
/// checkpoint that syncs syncs the arena too. Before, the mark started clear, and a raised flight
/// naming no slot, a compaction or a checkpoint made the earlier D0 flight's records durable without
/// its slots. A synced store's reopened arena stays clean (its flights synced it).
#[test]
fn a_reopened_arena_counts_as_unsynced_unless_its_flights_synced_it() {
    let _s = serial();
    for catalog in [false, true] {
        for class in [SyncClass::Off, SyncClass::Fsync] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("arena-dirty.db");
            let incarnation = {
                let db = open_at(&path, opts(catalog, class));
                let trunk = db.connect().unwrap();
                seed(&trunk);
                let b = trunk.fork_branch().unwrap();
                write_v(&b.connect().unwrap(), 3, "x");
                let _ = b.into_id();
                db.incarnation
            };
            let db = reopen(&path, opts(catalog, class), incarnation);
            assert_eq!(
                db.branches.arena_dirty(),
                class == SyncClass::Off,
                "catalog={catalog} class={class:?}: the reopened arena's unsynced mark"
            );
        }
    }
}

// ---- review 3 #17: listings and lookups report nothing that is not yet durable ----

/// Waits until `waiter` is blocked in `wait_durable` (the group counted its wait, not as already
/// durable) or has finished; true when it is blocked.
fn blocked_in_wait_durable(db: &Arc<Database>, before: [u64; 5], waiter: &std::thread::JoinHandle<impl Sized>) -> bool {
    let t = std::time::Instant::now();
    loop {
        let now = db.branches.group_counters();
        if now[2] > before[2] && now[3] == before[3] {
            return !waiter.is_finished();
        }
        if waiter.is_finished() {
            return false;
        }
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "the waiter neither waited nor finished");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Review 3 #17 (C-F4's wait, untested until now): a listing does not return a fork before the
/// fork's records are durable. With the fork's flight held after it was taken, a listing on
/// another thread waits; once the flight lands it lists the fork. Mutant `list_no_durable_wait`.
#[test]
fn a_listing_waits_for_the_forks_it_lists() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("list-wait.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let listed: std::collections::HashSet<BranchId> = db.branch_ids().unwrap().into_iter().collect();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let creator = {
            let db = db.clone();
            std::thread::spawn(move || db.connect().unwrap().fork_branch().map(|b| b.into_id()))
        };
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let before = db.branches.group_counters();
        let lister = {
            let db = db.clone();
            std::thread::spawn(move || db.branch_ids())
        };
        let blocked = blocked_in_wait_durable(&db, before, &lister);
        hold.store(0, std::sync::atomic::Ordering::Release);
        let id = creator.join().unwrap().unwrap();
        let got: std::collections::HashSet<BranchId> = lister.join().unwrap().unwrap().into_iter().collect();
        assert!(blocked, "catalog={catalog}: a listing returned while a fork it lists was not durable");
        assert!(got.contains(&id) && got.is_superset(&listed), "catalog={catalog}: the listing after the flight");
    }
}

/// Review 3 #17: a lookup by name does not report a dropped name free before the drop's Release
/// is durable: after a crash the branch would come back under that name.
#[test]
fn a_lookup_by_name_waits_for_the_release_it_reports() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("name-wait.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("x").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let dropper = {
            let db = db.clone();
            std::thread::spawn(move || db.drop_branch("x").map(|_| ()))
        };
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let before = db.branches.group_counters();
        let finder = {
            let db = db.clone();
            std::thread::spawn(move || db.branch_named("x"))
        };
        let blocked = blocked_in_wait_durable(&db, before, &finder);
        hold.store(0, std::sync::atomic::Ordering::Release);
        dropper.join().unwrap().unwrap();
        let found = finder.join().unwrap().unwrap();
        assert!(blocked, "catalog={catalog}: a dropped name was reported free before its release was durable");
        assert_eq!(found, None, "catalog={catalog}: after the release");
    }
}

/// Review 3 #17: a fail-stopped store lists no fork whose flight failed (its creator was told it
/// failed, and a reopen would not have it).
#[test]
fn a_fail_stopped_store_lists_no_fork_whose_flight_failed() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("list-failed.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let mut before = db.branch_ids().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(trunk.fork_branch().is_err(), "catalog={catalog}: premise: the fork's flight failed");
        let mut after = db.branch_ids().unwrap();
        before.sort();
        after.sort();
        assert_eq!(after, before, "catalog={catalog}: a fork whose flight failed is listed");
    }
}

/// Review 4 #16: a panic in a fuzzy checkpoint's cut (an unwinding build) still reaches the
/// install, so the checkpoint is not left in flight for good: the next one starts and installs.
/// Before, the cut ran outside the writer's panic guard, `flight` stayed set, and no checkpoint
/// ever started again (and the catalog's read snapshot stayed pinned).
#[test]
fn a_panic_in_a_fuzzy_checkpoints_cut_does_not_stop_checkpoints() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("cut-panic.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _a = trunk.fork_branch().unwrap().into_id();
    super::store::CUT_PANICS.store(true, std::sync::atomic::Ordering::Release);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert!(
        !super::store::CUT_PANICS.load(std::sync::atomic::Ordering::Acquire),
        "premise: the cut panicked"
    );
    let installed = db.branch_checkpoint_counters()[0];
    let _b = trunk.fork_branch().unwrap().into_id();
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no checkpoint started after a cut panicked");
    db.branch_checkpoint_wait();
    assert!(db.branch_checkpoint_counters()[0] > installed, "the checkpoint after the panic did not install");
}

// ---- review 4 #11: the install's free accounting, every branch forced ----

/// Review 4 #11: a fuzzy checkpoint captured while a release's flight is in the air lists that
/// release's slots free in the catalog (deferred). Between the capture and the install, held at
/// the commit: (a) the deferred free matures; (b) its slot is allocated again by a new branch's
/// write; (c) another release, buffered after the capture, is still pending. The install must
/// count every slot once: in use exactly when a live branch owns it, before and after the install
/// and after a reopen, and no slot handed to two live branches. Mutants
/// `forget_listed_kept_in_use`, `in_use_keeps_deferred`, `deferred_matured_at_capture`.
#[test]
fn the_installs_free_accounting_counts_every_slot_once() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("free-accounting.db");
    let owned_all = |db: &Arc<Database>, live: &[BranchId]| -> std::collections::BTreeSet<u32> {
        let mut out = std::collections::BTreeSet::new();
        for &id in live {
            let b = db.branch(id).unwrap();
            for s in b.owned_slots() {
                assert!(out.insert(s), "slot {s} owned by two live branches");
            }
            let _ = b.into_id();
        }
        out
    };
    let (live, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let anchor = trunk.fork_branch().unwrap();
        write_v(&anchor.connect().unwrap(), 1, "anchor");
        let anchor = anchor.into_id();
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 3, "x");
        let y = trunk.fork_branch().unwrap();
        write_v(&y.connect().unwrap(), 4, "y");
        let y = y.into_id();
        // x's release is in the air at the capture.
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
        let release = std::thread::spawn(move || x.reap().map(|_| ()));
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        hold.store(0, O::Release);
        release.join().unwrap().unwrap();
        let t = std::time::Instant::now();
        while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "the checkpoint never arrived");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        // (a) the deferred free matures; (b) a new branch's write takes a slot (the freed one, LIFO);
        // (c) y's release is buffered after the capture.
        let in_use_held = db.branch_slots_in_use();
        let z = trunk.fork_branch().unwrap();
        write_v(&z.connect().unwrap(), 5, "z");
        let z = z.into_id();
        db.branch(y).unwrap().reap().unwrap();
        let live = vec![anchor, z];
        let owned = owned_all(&db, &live);
        let in_use: std::collections::BTreeSet<u32> = db.branch_slots_in_use().into_iter().collect();
        assert_eq!(in_use, owned, "before the install: in use is not what the live branches own ({in_use_held:?} at the hold)");
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        let in_use: std::collections::BTreeSet<u32> = db.branch_slots_in_use().into_iter().collect();
        assert_eq!(in_use, owned_all(&db, &live), "after the install");
        assert_eq!(
            db.branch_stats().unwrap().arena_slots_in_use as usize,
            in_use.len(),
            "after the install: the in-use count disagrees with the slots in use"
        );
        (live, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    let owned = owned_all(&db, &live);
    let in_use: std::collections::BTreeSet<u32> = db.branch_slots_in_use().into_iter().collect();
    assert_eq!(in_use, owned, "after a reopen");
    assert_eq!(db.branch_stats().unwrap().arena_slots_in_use as usize, in_use.len(), "after a reopen: the count");
    // And new branches never share a slot with the live ones.
    let trunk = db.connect().unwrap();
    for i in 0..6 {
        let b = trunk.fork_branch().unwrap();
        write_v(&b.connect().unwrap(), 10 + i, "new");
        for s in b.owned_slots() {
            assert!(!owned.contains(&s), "slot {s} handed to a new branch while a live one owns it");
        }
        let _ = b.into_id();
    }
}

// ---- review 5 #18: a D0 free of a slot a synced record names waits for a sync ----

/// Review 5 #18, the D0 half: in a D0 store whose trunk is synchronous, a trunk commit keeps a
/// pre-image (TrunkRetain{S}) in a RAISED, synced flight; the child it was kept for is then
/// released by a D0 flight, which nothing syncs. If S were reused at once, a power cut that keeps
/// the synced record and loses the unsynced Release would leave the child alive over S's new
/// bytes. So S stays out of the allocator until a sync covers its Release: the image after such a
/// cut (log as of the raised flight, the arena as it is) still reads the child's fork-point row.
#[cfg(unix)]
#[test]
fn a_d0_release_of_a_slot_a_synced_record_names_waits_for_a_sync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("d0-synced-free.db");
        let (x_id, cut, log, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Off));
            let trunk = db.connect().unwrap();
            seed_wide(&trunk);
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            let x = trunk.fork_branch().unwrap();
            let x_id = x.id();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            write_v(&trunk, 3, "new");
            let kept: Vec<u32> = db.branch_slots_in_use().into_iter().filter(|s| !before.contains(s)).collect();
            assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the commit kept one pre-image");
            let log = db.branch_log_path().unwrap();
            // The raised flight is the last synced one: the cut keeps the log up to here.
            let cut = std::fs::metadata(&log).unwrap().len();
            x.reap().unwrap();
            // A new branch's write takes a slot; the freed one, if it was handed back at once.
            let y = trunk.fork_branch().unwrap();
            let fresh_before: std::collections::HashSet<u32> = y.owned_slots().into_iter().collect();
            write_v(&y.connect().unwrap(), 40, "y");
            let took: Vec<u32> = y.owned_slots().into_iter().filter(|s| !fresh_before.contains(s)).collect();
            assert!(
                !took.contains(&kept[0]),
                "catalog={catalog}: the slot a synced record names was reused before a sync covered its free"
            );
            let _ = y.into_id();
            (x_id, cut, log, db.incarnation)
        };
        // The power cut: nothing D0 wrote after the raised flight survives in the log.
        let f = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
        f.set_len(cut).unwrap();
        drop(f);
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        let x = db.branch(x_id).expect("the child whose Release was lost is back");
        assert_eq!(read_wide(&x.connect().unwrap(), 3), "trunk-3", "catalog={catalog}: the child's fork point");
    }
}

/// Review 4 #2: a fuzzy checkpoint that cannot start — its capture fails, or its thread cannot be
/// spawned — backs off as a failed write does, instead of being retried by every later operation
/// (each retry a capture under the store mutex, O(branches dirty since the last checkpoint): N
/// creates cost O(N^2)). After the failed attempt, the next creates enter no capture.
#[test]
fn a_checkpoint_that_cannot_start_is_not_retried_by_the_next_create() {
    let _s = serial();
    for fp in [BranchFailpoint::CaptureFails, BranchFailpoint::SpawnFails] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("ckpt-start-fails.db"),
            opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _t = Threshold::set(8 << 10);
        db.branch_failpoint(Some(fp));
        let entered = || super::store::CAPTURE_ENTERED.with(|c| c.get());
        let before = entered();
        let t = std::time::Instant::now();
        while entered() == before {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "{fp:?}: no checkpoint was attempted");
        }
        db.branch_checkpoint_wait();
        let after = entered();
        for _ in 0..5 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        assert_eq!(entered(), after, "{fp:?}: the next creates retried the checkpoint's capture");
    }
}

/// Review 4 #2: a capture that fails has no effect on the log. Its checkpoint marker was buffered
/// before the capture's fallible steps (the arena's sync handle, the catalog's read snapshot), so
/// each failed attempt appended a marker that no checkpoint ever followed.
#[test]
fn a_capture_that_fails_appends_no_checkpoint_marker() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("capture-fails.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    db.branch_failpoint(Some(BranchFailpoint::CaptureFails));
    let entered = || super::store::CAPTURE_ENTERED.with(|c| c.get());
    let (lsn, before) = (db.branches.log_lsn_for_test(), entered());
    assert!(!db.branch_checkpoint_fuzzy_now().unwrap(), "a checkpoint started through the failpoint");
    assert_eq!(entered(), before + 1, "premise: the capture was entered");
    assert_eq!(db.branches.log_lsn_for_test(), lsn, "the failed capture appended to the log");
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no checkpoint started after the failed one");
    db.branch_checkpoint_wait();
}
