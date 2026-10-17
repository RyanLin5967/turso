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

/// L2(d) (the capture freezes `fresh` and `gone` instead of copying them, `NameIndex`): a fuzzy
/// checkpoint whose write fails gives its frozen names back, so the next checkpoint writes them. A
/// name created before the failed capture (`kept`) is still found once its state is evicted (resident
/// cap 0) and after a reopen, and a name released before it (`freed`) is still free: it resolves to
/// nothing and can be given again, also after a reopen. Mutant `frozen_names_dropped_on_failure`.
#[test]
fn a_failed_fuzzy_checkpoint_gives_its_frozen_names_back() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("frozen-back.db");
    let fuzzy = || opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy);
    let (kept, incarnation) = {
        let db = open_at(&path, fuzzy());
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("freed").unwrap();
        db.branch_compact_now().unwrap();
        db.drop_branch("freed").unwrap();
        let kept = trunk.create_branch("kept").unwrap();
        db.branch_failpoint(Some(BranchFailpoint::CheckpointWriteFails));
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the failing fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        db.branch_failpoint(None);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the next fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        db.branch_set_resident_cap(Some(0));
        db.branch_compact_now().unwrap();
        assert_eq!(db.branch_named("kept").unwrap(), Some(kept), "CLAIM: a name created before a failed checkpoint was lost once evicted");
        assert_eq!(db.branch_named("freed").unwrap(), None, "CLAIM: a name released before a failed checkpoint resolved again");
        (kept, db.incarnation)
    };
    let db = reopen(&path, fuzzy(), incarnation);
    assert_eq!(db.branch_named("kept").unwrap(), Some(kept), "CLAIM: after a reopen, a name created before a failed checkpoint was lost");
    assert_eq!(db.branch_named("freed").unwrap(), None, "CLAIM: after a reopen, a name released before a failed checkpoint resolved again");
    assert!(db.connect().unwrap().create_branch("freed").is_ok(), "CLAIM: a released name could not be given again");
}

/// L2(d) slice 2: a fuzzy checkpoint's rows are exact to its capture although the states change
/// while it is in flight. The writer is held before it encodes the frozen rows
/// (`HOLD_BEFORE_ROWS`); meanwhile branch `a` is written again, `b` is dropped, and `a` forks a
/// child: each change reaches a frozen state, so the table encodes its pre-image first. The rows
/// the writer gets must equal the rows the capture would have built (`CAPTURE_VERIFY_FORCED`, the
/// capture builds them too, the old way), and after a reopen every acknowledged change is there.
/// Premise: the copy-on-write encoded at least one pre-image (`COW_PRESERVES`). Mutant
/// `cow_preserve_skipped`.
#[test]
fn a_branch_changed_while_its_row_is_frozen_is_written_as_of_the_capture() {
    use std::sync::atomic::Ordering::{Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::store::CAPTURE_VERIFY_FORCED.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("cow-rows.db");
    let fuzzy = || opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy);
    let incarnation = {
        let db = open_at(&path, fuzzy());
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("a").unwrap();
        trunk.create_branch("b").unwrap();
        let a = db.connect_named("a").unwrap();
        a.execute("UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
        super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clear();
        super::store::CAPTURE_VERIFY_FORCED.store(true, Release);
        let preserves0 = super::table::COW_PRESERVES.load(Relaxed);
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_ROWS);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        let t = std::time::Instant::now();
        while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_ROWS | super::store::HOLD_ARRIVED {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the writer never reached its rows");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        a.execute("UPDATE t SET v = 'a2' WHERE id = 2").unwrap();
        let child = a.fork_branch().unwrap().into_id();
        db.drop_branch("b").unwrap();
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        super::store::CAPTURE_VERIFY_FORCED.store(false, Release);
        let preserved = super::table::COW_PRESERVES.load(Relaxed) - preserves0;
        assert!(preserved >= 1, "premise: no change reached a frozen state while the writer was held");
        let failures = super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clone();
        assert!(failures.is_empty(), "CLAIM: a fuzzy checkpoint's rows were not exact to its capture: {failures:?}");
        let _ = child;
        drop(a);
        db.incarnation
    };
    let db = reopen(&path, fuzzy(), incarnation);
    assert_eq!(db.branch_named("b").unwrap(), None, "after a reopen, a dropped branch resolved");
    let a = db.connect_named("a").unwrap();
    assert_eq!((read_v(&a, 1), read_v(&a, 2)), ("a1".to_string(), "a2".to_string()), "after a reopen, a's writes are not all there");
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

/// Wire review 7 HIGH 3 (engine LEAD-ORDER item 2, the commit-started flag): an explicit COMMIT
/// whose trunk commit is refused at its copy-decision pass (`Busy`) and is then stepped again
/// resumes that commit. Before the fix, the first step had already set `auto_commit` while
/// `commit_state` was still `Ready`, so the second step read "no transaction is active" (TxError),
/// its abort rolled nothing back, and the session kept its write transaction (and the WAL write
/// lock) with autocommit on. The commit must happen exactly once: one WAL commit frame for the
/// transaction, its write visible to another connection, no transaction left open, and the live
/// child still reading its fork-point version.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_commit_re_stepped_after_a_busy_decision_pass_commits_once() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("busy-commit.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let child = a.fork_branch().unwrap();
        let page_size = a.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap() as usize;
        let before = a.wal_state().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
        a.execute("BEGIN").unwrap();
        a.execute("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
        let mut commit = a.prepare("COMMIT").unwrap();
        let first = commit.run_ignore_rows();
        assert!(
            matches!(first, Err(LimboError::Busy)),
            "catalog={catalog}: premise: the decision pass was refused: {first:?}"
        );
        let second = commit.run_ignore_rows();
        assert!(
            second.is_ok(),
            "catalog={catalog}: CLAIM: the re-stepped COMMIT resumes its commit: {second:?}"
        );
        drop(commit);
        let after = a.wal_state().unwrap();
        assert_eq!(
            after.checkpoint_seq_no, before.checkpoint_seq_no,
            "catalog={catalog}: premise: no checkpoint restarted the WAL in between"
        );
        let mut frame = vec![0u8; 24 + page_size];
        let commit_frames = (before.max_frame + 1..=after.max_frame)
            .filter(|&n| a.wal_get_frame(n, &mut frame).unwrap().is_commit_frame())
            .count();
        assert_eq!(commit_frames, 1, "catalog={catalog}: the transaction committed exactly once");
        assert!(a.get_auto_commit(), "catalog={catalog}: the session is back in autocommit");
        let rollback = a.execute("ROLLBACK");
        assert!(
            matches!(rollback, Err(LimboError::TxError(_))),
            "catalog={catalog}: the COMMIT left a transaction open: ROLLBACK gave {rollback:?}"
        );
        let b = db.connect().unwrap();
        assert_eq!(read_wide(&b, 3), "new", "catalog={catalog}: another connection reads the commit");
        let c = child.connect().unwrap();
        assert_eq!(read_wide(&c, 3), "trunk-3", "catalog={catalog}: the child reads its fork point");
    }
}

/// Engine review 16 HIGH 1's fixture: a trunk with a live child, `BEGIN; UPDATE t SET v = 'new'
/// WHERE id = 3`, and a COMMIT refused at the trunk's copy-decision pass (`Busy`, the premise).
/// `held`: the COMMIT is a prepared statement kept for a re-step; otherwise it ran through
/// `Connection::execute`, which drops its statement.
#[cfg(feature = "conn_raw_api")]
struct BusyCommit {
    commit: Option<crate::Statement>,
    _child: Branch,
    a: Arc<Connection>,
    db: Arc<Database>,
    from_frame: u64,
    checkpoint_seq: u32,
    page_size: usize,
    catalog: bool,
    _dir: tempfile::TempDir,
}

#[cfg(feature = "conn_raw_api")]
impl BusyCommit {
    fn new(catalog: bool, held: bool) -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("busy-commit.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let child = a.fork_branch().unwrap();
        let page_size = a.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap() as usize;
        let before = a.wal_state().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
        a.execute("BEGIN").unwrap();
        a.execute("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
        let (commit, first) = if held {
            let mut commit = a.prepare("COMMIT").unwrap();
            let first = commit.run_ignore_rows();
            (Some(commit), first)
        } else {
            (None, a.execute("COMMIT"))
        };
        assert!(
            matches!(first, Err(LimboError::Busy)),
            "catalog={catalog}: premise: the decision pass was refused: {first:?}"
        );
        Self {
            commit,
            _child: child,
            a,
            db,
            from_frame: before.max_frame,
            checkpoint_seq: before.checkpoint_seq_no,
            page_size,
            catalog,
            _dir: dir,
        }
    }

    /// Step the held COMMIT again.
    fn restep(&mut self) -> crate::Result<()> {
        self.commit.as_mut().expect("a held COMMIT").run_ignore_rows()
    }

    /// The WAL commit frames written since the fixture's first BEGIN, by any connection
    /// (`commit_frames_since`'s fresh-connection view: FLAGGED TEST EDIT, the same instrument fix).
    fn commit_frames(&self) -> usize {
        commit_frames_since(&self.a, self.from_frame, self.checkpoint_seq, self.page_size)
    }

    /// Row `id`'s value as a fresh trunk connection reads it: what is committed.
    fn committed(&self, id: i64) -> String {
        read_wide(&self.db.connect().unwrap(), id)
    }
}

/// Step `stmt` until it yields its first row (the premise of a statement left mid-way).
#[cfg(feature = "conn_raw_api")]
fn step_to_row(db: &Arc<Database>, stmt: &mut crate::Statement) {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Row => return,
            crate::StepResult::IO | crate::StepResult::Yield => db.io.step().unwrap(),
            other => panic!("premise: the statement yields a row, not {other:?}"),
        }
    }
}

/// Engine review 16 HIGH 1 (a): a sibling INSERT fails between a Busy COMMIT and its re-step.
/// The re-stepped COMMIT must not report success for a transaction that is not durable.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_re_stepped_after_a_failed_sibling_insert_reports_only_a_durable_commit() {
    let _s = serial();
    for catalog in [false, true] {
        let mut f = BusyCommit::new(catalog, true);
        let sibling = f.a.execute("INSERT INTO t VALUES (3, 'dup')");
        assert!(
            matches!(sibling, Err(LimboError::Constraint(_))),
            "catalog={catalog}: premise: the sibling INSERT fails: {sibling:?}"
        );
        let second = f.restep();
        let committed = f.committed(3);
        assert!(
            second.is_err() || committed == "new",
            "catalog={catalog}: CLAIM: the re-stepped COMMIT returned {second:?} while row 3 reads \
             {committed:?}"
        );
    }
}

/// Engine review 16 HIGH 1 (b): the same with a sibling SELECT dropped mid-scan.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_re_stepped_after_a_dropped_sibling_scan_reports_only_a_durable_commit() {
    let _s = serial();
    for catalog in [false, true] {
        let mut f = BusyCommit::new(catalog, true);
        let mut scan = f.a.prepare("SELECT id FROM t").unwrap();
        step_to_row(&f.db, &mut scan);
        drop(scan);
        let second = f.restep();
        let committed = f.committed(3);
        assert!(
            second.is_err() || committed == "new",
            "catalog={catalog}: CLAIM: the re-stepped COMMIT returned {second:?} while row 3 reads \
             {committed:?}"
        );
    }
}

/// Engine review 16 HIGH 1 (c): `execute("COMMIT")` drops its statement on Busy; a second
/// `execute("COMMIT")` must commit the transaction, exactly once.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_second_execute_commit_after_a_busy_one_commits_once() {
    let _s = serial();
    for catalog in [false, true] {
        let f = BusyCommit::new(catalog, false);
        let second = f.a.execute("COMMIT");
        assert!(
            second.is_ok(),
            "catalog={catalog}: CLAIM: a second execute(COMMIT) after a Busy one commits: {second:?}"
        );
        assert_eq!(f.commit_frames(), 1, "catalog={catalog}: the transaction committed exactly once");
        assert_eq!(f.committed(3), "new", "catalog={catalog}: the write is committed");
    }
}

/// Engine review 16 HIGH 1 (d): after a Busy COMMIT, ROLLBACK ends the transaction: it returns Ok,
/// the write is gone, and another connection can take the write lock.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_rollback_after_a_busy_commit_rolls_the_transaction_back() {
    let _s = serial();
    for catalog in [false, true] {
        let f = BusyCommit::new(catalog, false);
        let rollback = f.a.execute("ROLLBACK");
        assert!(
            rollback.is_ok(),
            "catalog={catalog}: CLAIM: ROLLBACK after a Busy COMMIT rolls back: {rollback:?}"
        );
        assert_eq!(f.committed(3), "trunk-3", "catalog={catalog}: the write is gone");
        let b = f.db.connect().unwrap();
        let other = b.execute("UPDATE t SET v = 'other' WHERE id = 40");
        assert!(other.is_ok(), "catalog={catalog}: another connection takes the write lock: {other:?}");
        assert_eq!(f.commit_frames(), 1, "catalog={catalog}: only the other connection committed");
    }
}

/// Engine review 16 HIGH 1 (e): while a Busy COMMIT waits for its re-step, its write transaction is
/// still open, so the connection does not report autocommit.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_leaves_the_connection_out_of_autocommit() {
    let _s = serial();
    for catalog in [false, true] {
        let mut f = BusyCommit::new(catalog, true);
        assert!(
            !f.a.get_auto_commit(),
            "catalog={catalog}: CLAIM: autocommit reads true while the write transaction is open"
        );
        f.restep().unwrap();
        assert_eq!(f.commit_frames(), 1, "catalog={catalog}: the re-step commits once");
    }
}

/// Engine review 16 HIGH 1 (f): a BEGIN between a Busy COMMIT and its re-step is refused, because
/// the write transaction is still open.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_begin_after_a_busy_commit_is_refused() {
    let _s = serial();
    for catalog in [false, true] {
        let f = BusyCommit::new(catalog, true);
        let begin = f.a.execute("BEGIN");
        assert!(
            matches!(begin, Err(LimboError::TxError(_))),
            "catalog={catalog}: CLAIM: BEGIN over the open write transaction gave {begin:?}"
        );
    }
}

/// Engine review 16 HIGH 1 (g): a sibling writer left mid-statement when the Busy COMMIT is
/// re-stepped makes the COMMIT refuse (StatementsInProgress), and nothing is committed: neither the
/// transaction's write nor the sibling's half-done one.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_re_stepped_over_a_paused_sibling_writer_is_refused() {
    let _s = serial();
    for catalog in [false, true] {
        let mut f = BusyCommit::new(catalog, true);
        let mut sibling =
            f.a.prepare("UPDATE t SET v = 'mid' WHERE id IN (40, 50) RETURNING id").unwrap();
        step_to_row(&f.db, &mut sibling);
        let second = f.restep();
        assert!(
            matches!(second, Err(LimboError::StatementsInProgress(_))),
            "catalog={catalog}: CLAIM: the COMMIT re-stepped over a paused writer gave {second:?}"
        );
        assert_eq!(f.committed(3), "trunk-3", "catalog={catalog}: the transaction's write is not committed");
        assert_eq!(f.committed(40), "trunk-40", "catalog={catalog}: the sibling's write is not committed");
        assert_eq!(f.commit_frames(), 0, "catalog={catalog}: no commit frame");
        drop(sibling);
    }
}

/// Engine review 16 HIGH 1 (h): statement S1's COMMIT is Busy; a BEGIN on another statement is
/// refused (the transaction is still open, as (f) shows), and a COMMIT on another statement S2
/// commits it. Re-stepping S1 finds no transaction.
///
/// FLAGGED TEST EDIT (engine review 19 LOW 20 b, a strengthening): the BEGIN's result was swallowed
/// (`let _ =`); it is asserted refused, and the doc says what it does.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_re_stepped_after_another_statement_committed_finds_no_transaction() {
    let _s = serial();
    for catalog in [false, true] {
        let mut f = BusyCommit::new(catalog, true);
        let begin = f.a.execute("BEGIN");
        assert!(
            matches!(begin, Err(LimboError::TxError(_))),
            "catalog={catalog}: premise: a BEGIN over the open transaction is refused: {begin:?}"
        );
        let s2 = f.a.execute("COMMIT");
        assert!(s2.is_ok(), "catalog={catalog}: premise: S2's COMMIT commits: {s2:?}");
        let s1 = f.restep();
        assert!(
            matches!(s1, Err(LimboError::TxError(_))),
            "catalog={catalog}: CLAIM: S1 re-stepped after S2's COMMIT gave {s1:?}"
        );
        assert_eq!(f.commit_frames(), 1, "catalog={catalog}: one commit, S2's");
        assert_eq!(f.committed(3), "new", "catalog={catalog}: S2 committed the write");
    }
}

/// Engine review 16 HIGH 1 (i): an explicit transaction feeding a materialized view, whose view
/// merge at COMMIT yields IO (`VIEW_MERGE_YIELDS`) before the trunk's decision pass refuses it, is
/// committed once by the re-stepped COMMIT: one commit frame and the view counts each row once.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_whose_view_merge_yielded_commits_once() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("busy-view.db"),
            opts(catalog, SyncClass::Fsync).with_views(true),
        );
        let a = db.connect().unwrap();
        seed_wide(&a);
        a.execute("CREATE TABLE counters(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)").unwrap();
        a.execute(
            "CREATE MATERIALIZED VIEW sums AS SELECT grp, SUM(val) AS total FROM counters GROUP BY grp",
        )
        .unwrap();
        let child = a.fork_branch().unwrap();
        let page_size = a.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap() as usize;
        let before = a.wal_state().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
        a.execute("BEGIN").unwrap();
        a.execute("INSERT INTO counters VALUES (1, 'a', 5), (2, 'a', 7)").unwrap();
        a.execute("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
        crate::vdbe::VIEW_MERGE_YIELDS.with(|n| n.set(1));
        let mut commit = a.prepare("COMMIT").unwrap();
        let first = commit.run_ignore_rows();
        let yielded = crate::vdbe::VIEW_MERGE_YIELDS.with(|n| n.replace(0)) == 0;
        assert!(yielded, "catalog={catalog}: premise: the view merge yielded");
        assert!(
            matches!(first, Err(LimboError::Busy)),
            "catalog={catalog}: premise: the decision pass was refused after the yield: {first:?}"
        );
        let second = commit.run_ignore_rows();
        assert!(
            second.is_ok(),
            "catalog={catalog}: CLAIM: the re-stepped COMMIT commits: {second:?}"
        );
        drop(commit);
        let after = a.wal_state().unwrap();
        assert_eq!(after.checkpoint_seq_no, before.checkpoint_seq_no, "catalog={catalog}: premise");
        let mut frame = vec![0u8; 24 + page_size];
        let commit_frames = (before.max_frame + 1..=after.max_frame)
            .filter(|&n| a.wal_get_frame(n, &mut frame).unwrap().is_commit_frame())
            .count();
        assert_eq!(commit_frames, 1, "catalog={catalog}: the transaction committed exactly once");
        let b = db.connect().unwrap();
        let totals: Vec<(Option<String>, Option<i64>)> = b
            .prepare("SELECT grp, CAST(total AS INTEGER) FROM sums")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| (row[0].to_text().map(str::to_string), row[1].as_int()))
            .collect();
        assert_eq!(
            totals,
            vec![(Some("a".to_string()), Some(12))],
            "catalog={catalog}: the view counts each committed row once"
        );
        assert_eq!(read_wide(&b, 3), "new", "catalog={catalog}: the write is committed");
        drop(child);
    }
}

/// The WAL commit frames `conn` reads past frame `from`, with no checkpoint since (`seq`).
#[cfg(feature = "conn_raw_api")]
fn commit_frames_since(conn: &Arc<Connection>, from: u64, seq: u32, page_size: usize) -> usize {
    // FLAGGED TEST EDIT (an instrument reaching what it counts; for the lead's ruling): `wal_state`
    // is the connection's own view (its last read mark or commit), so another connection's commit
    // was invisible to it: the attached half committed through the main connection read 0 on aux's
    // direct connection (red at bb6af4d94 and 9d27592bf, artie raws l1-branch*). The frames are
    // counted on a fresh connection to the same database, after a read installs the newest mark.
    let fresh = conn.db.connect().unwrap();
    fresh.execute("SELECT count(*) FROM sqlite_schema").unwrap();
    let now = fresh.wal_state().unwrap();
    assert_eq!(now.checkpoint_seq_no, seq, "premise: no checkpoint restarted the WAL in between");
    let mut frame = vec![0u8; 24 + page_size];
    (from + 1..=now.max_frame)
        .filter(|&n| fresh.wal_get_frame(n, &mut frame).unwrap().is_commit_frame())
        .count()
}

/// Engine review 19 HIGH 2's fixture: main and an attached `aux`, each a durable store with a live
/// child (so each half takes copy decisions), `BEGIN`, a write to each, and a COMMIT whose main
/// half commits while aux's decision pass is refused (`TrunkDecisionBusy` on aux's store only).
/// Premises: the COMMIT is `Busy`, and main's commit frame is in its WAL. `held`: the COMMIT is a
/// prepared statement kept for a re-step. Returns the databases, the main connection, a direct aux
/// connection, the held COMMIT, both WALs' starting frames and checkpoint sequences, the page size,
/// and the children (kept alive).
#[cfg(feature = "conn_raw_api")]
#[allow(clippy::type_complexity)]
fn attached_busy_commit(
    dir: &Path,
    catalog: bool,
    held: bool,
) -> (
    Arc<Database>,
    Arc<Database>,
    Arc<Connection>,
    Arc<Connection>,
    Option<crate::Statement>,
    [(u64, u32); 2],
    usize,
    [Branch; 2],
) {
    let aux_path = dir.join("aux.db");
    let db = open_at(&dir.join("main.db"), opts(catalog, SyncClass::Fsync).with_attach(true));
    let aux = open_at(&aux_path, opts(catalog, SyncClass::Fsync).with_attach(true));
    let a = db.connect().unwrap();
    seed_wide(&a);
    let child = a.fork_branch().unwrap();
    let x = aux.connect().unwrap();
    seed_wide(&x);
    let aux_child = x.fork_branch().unwrap();
    a.execute(format!("ATTACH DATABASE '{}' AS aux", aux_path.display())).unwrap();
    let page_size = a.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap() as usize;
    let (main_before, aux_before) = (a.wal_state().unwrap(), x.wal_state().unwrap());
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
    a.execute("UPDATE aux.t SET v = 'aux-new' WHERE id = 3").unwrap();
    aux.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
    let (commit, first) = if held {
        let mut commit = a.prepare("COMMIT").unwrap();
        let first = commit.run_ignore_rows();
        (Some(commit), first)
    } else {
        (None, a.execute("COMMIT"))
    };
    assert!(
        matches!(first, Err(LimboError::Busy)),
        "catalog={catalog}: premise: aux's decision pass was refused: {first:?}"
    );
    assert_eq!(
        commit_frames_since(&a, main_before.max_frame, main_before.checkpoint_seq_no, page_size),
        1,
        "catalog={catalog}: premise: main's half committed before aux's refusal"
    );
    (
        db,
        aux,
        a,
        x,
        commit,
        [
            (main_before.max_frame, main_before.checkpoint_seq_no),
            (aux_before.max_frame, aux_before.checkpoint_seq_no),
        ],
        page_size,
        [child, aux_child],
    )
}

/// Engine review 19 HIGH 2 (a): 19e2616f4 undid a Busy COMMIT's transition whenever `commit_state`
/// was still `Ready`, inferring that nothing committed. With an attached database, main's
/// `commit_tx` is done (frames published, locks released) before aux's decision pass is refused,
/// with `commit_state` still `Ready`, so the restore reopened an explicit transaction over a durable
/// main half: autocommit read false, and ROLLBACK returned Ok, undoing aux's half only. Neither may
/// claim the transaction is still open.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_after_its_main_half_committed_is_not_reopened() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _aux, a, _x, _commit, _from, _page_size, _children) = attached_busy_commit(dir.path(), catalog, false);
        assert!(
            a.get_auto_commit(),
            "catalog={catalog}: CLAIM: autocommit reads false after a COMMIT whose main half is durable"
        );
        let rollback = a.execute("ROLLBACK");
        assert!(
            rollback.is_err(),
            "catalog={catalog}: CLAIM: ROLLBACK returned Ok over a durable main half"
        );
        assert_eq!(read_wide(&db.connect().unwrap(), 3), "new", "catalog={catalog}: main's half stays committed");
    }
}

/// Engine review 19 HIGH 2 (b): the held COMMIT, re-stepped, commits aux's half exactly once and
/// main's no second time.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_busy_commit_after_its_main_half_committed_re_steps_the_attached_half_once() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (_db, aux, a, x, commit, from, page_size, _children) = attached_busy_commit(dir.path(), catalog, true);
        let mut commit = commit.expect("a held COMMIT");
        assert!(
            a.get_auto_commit(),
            "catalog={catalog}: CLAIM: autocommit reads false after a COMMIT whose main half is durable"
        );
        let second = commit.run_ignore_rows();
        assert!(second.is_ok(), "catalog={catalog}: CLAIM: the re-stepped COMMIT commits aux's half: {second:?}");
        drop(commit);
        assert_eq!(
            commit_frames_since(&a, from[0].0, from[0].1, page_size),
            1,
            "catalog={catalog}: main's half committed exactly once"
        );
        assert_eq!(
            commit_frames_since(&x, from[1].0, from[1].1, page_size),
            1,
            "catalog={catalog}: aux's half committed exactly once"
        );
        assert_eq!(read_wide(&aux.connect().unwrap(), 3), "aux-new", "catalog={catalog}: aux's half is committed");
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
        // FLAGGED TEST EDIT (engine review 7 #7's judge): the capture call's own waits are counted
        // on its thread (`thread_waits`) and must be 0; the deadline below is liveness only.
        let starter = {
            let db = db.clone();
            std::thread::spawn(move || {
                let waits = super::store::thread_waits();
                let started = db.branch_checkpoint_fuzzy_now();
                (started, super::store::thread_waits() - waits)
            })
        };
        // FLAGGED TEST EDIT (engine review 7 #7; review 4 asked for it): 60 s, not 2 s. A capture
        // that waits for the held flight never finishes, so the check still fires; a slow box no
        // longer fails it falsely.
        let t = std::time::Instant::now();
        while !starter.is_finished() && t.elapsed() < std::time::Duration::from_secs(60) {
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
        let (started, waits) = starter.join().unwrap();
        hold.store(0, std::sync::atomic::Ordering::Release);
        release.join().unwrap().unwrap();
        assert!(started.unwrap(), "premise: a fuzzy checkpoint started");
        assert_eq!(waits, 0, "the capture waited ({waits} times) for a flight in the air");
        db.branch_checkpoint_wait();
        let in_use = db.branch_slots_in_use();
        for slot in &owned {
            assert!(!in_use.contains(slot), "slot {slot} is still in use after its release");
        }
        // FLAGGED TEST EDIT (engine review 7 #7, a strengthening): the count agrees with the slots.
        assert_eq!(
            db.branch_stats().unwrap().arena_slots_in_use as usize,
            in_use.len(),
            "after the install: the in-use count disagrees with the slots in use"
        );
        (owned, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    let in_use = db.branch_slots_in_use();
    for slot in &owned {
        assert!(!in_use.contains(slot), "slot {slot} is in use after a reopen");
    }
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as usize,
        in_use.len(),
        "after a reopen: the in-use count disagrees with the slots in use"
    );
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

/// Review REVIEW-claude-engine-L2d M1 (DECISIONS 07c88bb280): every operation that owes a fuzzy
/// checkpoint gets it started. Since 969e7a370 `maybe_compact` only owes the start, and only the
/// paths that called `start_owed_flight` sent it; an expiry pass (`expire_branches`, `set_lease`,
/// `open_conn`, the open's expiry, an erroring create) owed one that never started, so a workload of
/// leases and expiries grew the log without bound. Here 1,000 leased branches expire in one
/// `expire_branches`, whose Releases cross the 8 KiB threshold: a checkpoint must start after it,
/// with no other operation. Mutant `flight_start_inline` (as before 969e7a370) is green too.
#[test]
fn an_expiry_that_crosses_the_threshold_starts_a_checkpoint() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("expiry-owes.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _t = Threshold::set(8 << 10);
    let mut held = Vec::new();
    for _ in 0..1000 {
        let b = trunk.fork_branch().unwrap();
        b.lease(std::time::Duration::from_secs(60)).unwrap();
        held.push(b);
    }
    db.branch_checkpoint_wait();
    let started = db.branch_checkpoint_counters()[1];
    db.branch_lease_clock_advance(std::time::Duration::from_secs(3600));
    let expired = db.expire_branches().unwrap();
    assert!(expired.reaped.len() >= 1000, "premise: the expiry reaped {} of 1,000 leased branches", expired.reaped.len());
    let t = std::time::Instant::now();
    while db.branch_checkpoint_counters()[1] == started && t.elapsed() < std::time::Duration::from_secs(10) {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    db.branch_checkpoint_wait();
    assert!(
        db.branch_checkpoint_counters()[1] > started,
        "CLAIM: an expiry whose Releases crossed the threshold started no checkpoint ({started} started before it, as many after)"
    );
    drop(held);
}

/// Review REVIEW-claude-engine-M1 MED-1 (lead order after c1809b546): no checkpoint worker
/// exists after a store closes. A thread that outlives the store's close with its own handle on
/// the store mutex (the name filter's and the confirmation writer's do) sends a start it owed;
/// held between the starter's `closing` check and its dispatch while the store closes, at
/// c1809b546 it then found the worker slot empty and started a NEW worker, unjoined, holding the
/// store's files after the close. Here the send must hand no job out.
#[test]
fn a_start_sent_after_the_store_closed_starts_no_worker() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("closed-starter.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let probe = db.branches.starter_probe();
    probe.hold();
    let sender = {
        let probe = probe.clone();
        std::thread::spawn(move || probe.owe())
    };
    let t = std::time::Instant::now();
    while !probe.arrived() && t.elapsed() < std::time::Duration::from_secs(10) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(probe.arrived(), "premise: the owed start reached the starter's hold");
    let before = probe.handed_out();
    drop(db);
    assert!(probe.closed(), "premise: the store closed while the start was held");
    probe.release();
    sender.join().unwrap();
    assert_eq!(
        probe.handed_out(),
        before,
        "CLAIM: a start sent after the store closed was handed to a checkpoint worker"
    );
}

/// Review REVIEW-claude-engine-M1 LOW-2: a hold that panicked starts no checkpoint while its
/// stack unwinds (it would capture a half-changed store); the start it owed stays owed, and the
/// next hold's unlock sends it. At c1809b546 the unwinding guard sent it.
#[test]
fn a_hold_that_panicked_sends_its_owed_start_only_at_the_next_hold() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("panicked-hold.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let probe = db.branches.starter_probe();
    let before = probe.handed_out();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| probe.owe_then_panic()));
    assert!(unwound.is_err(), "premise: the hold panicked");
    assert_eq!(
        probe.handed_out(),
        before,
        "CLAIM: a hold that panicked handed its owed start to the checkpoint worker while unwinding"
    );
    assert!(probe.owed(), "CLAIM: the start a panicked hold owed was dropped, not kept for the next hold");
    probe.touch();
    assert_eq!(probe.handed_out(), before + 1, "CLAIM: the next hold did not send the start the panicked one owed");
    db.branch_checkpoint_wait();
}

/// Review REVIEW-claude-engine-M1 LOW-3: the lease floor a fuzzy checkpoint's writer reads after
/// its commit (177f007c8, off the install's hold) is the catalog's at the install, i.e. nothing
/// wrote the catalog between them. Checked at every install under the capture check
/// (`CAPTURE_VERIFY_FORCED`), with leases in flight: branches leased before the checkpoint and
/// one leased while it is held before its rows. Mutant `lease_floor_after_stale`.
#[test]
fn a_fuzzy_checkpoint_s_lease_floor_is_the_catalog_s_at_its_install() {
    use std::sync::atomic::Ordering::{Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::store::CAPTURE_VERIFY_FORCED.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("lease-floor.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let mut held = Vec::new();
    for i in 0..8u64 {
        let b = trunk.fork_branch().unwrap();
        b.lease(std::time::Duration::from_secs(60 + i)).unwrap();
        held.push(b);
    }
    super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clear();
    super::store::CAPTURE_VERIFY_FORCED.store(true, Release);
    let verified0 = super::store::LEASE_FLOORS_VERIFIED.load(Relaxed);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_ROWS);
    let late = trunk.fork_branch().unwrap();
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a second fuzzy checkpoint started");
    let t = std::time::Instant::now();
    while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_ROWS | super::store::HOLD_ARRIVED {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the writer never reached its rows");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    late.lease(std::time::Duration::from_secs(30)).unwrap();
    held[0].lease(std::time::Duration::from_secs(3600)).unwrap();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    super::store::CAPTURE_VERIFY_FORCED.store(false, Release);
    let verified = super::store::LEASE_FLOORS_VERIFIED.load(Relaxed) - verified0;
    assert!(verified >= 2, "premise: the capture check compared {verified} installs' lease floors, not both");
    let failures = super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clone();
    assert!(failures.is_empty(), "CLAIM: a fuzzy checkpoint's writer-read lease floor was not the catalog's at its install: {failures:?}");
    drop(late);
    drop(held);
}

/// Review REVIEW-claude-engine-m1-2 LOW-B: the lease-floor check exercised in the window it guards,
/// between the writer's commit and the install (`HOLD_AFTER_COMMIT`), where real operations run:
/// a lease renewed, a named create, and an expiry that reaps, in each of two checkpoints' windows.
/// The check must compare both installs (`LEASE_FLOORS_VERIFIED` +2) and find no difference.
/// Mutant `lease_floor_after_stale` (a floor one past the catalog's) must still fail it.
#[test]
fn the_lease_floor_check_holds_with_operations_between_the_commit_and_the_install() {
    use std::sync::atomic::Ordering::{Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::store::CAPTURE_VERIFY_FORCED.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("lease-floor-window.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let keep = trunk.fork_branch().unwrap();
    keep.lease(std::time::Duration::from_secs(36_000)).unwrap();
    let mut short = Vec::new();
    for _ in 0..8 {
        let b = trunk.fork_branch().unwrap();
        b.lease(std::time::Duration::from_secs(60)).unwrap();
        short.push(b.into_id());
    }
    super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clear();
    super::store::CAPTURE_VERIFY_FORCED.store(true, Release);
    let verified0 = super::store::LEASE_FLOORS_VERIFIED.load(Relaxed);
    for round in 0..2u32 {
        db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: fuzzy checkpoint {round} started");
        let t = std::time::Instant::now();
        while db.branch_checkpoint_held() != super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: checkpoint {round}'s writer never committed");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        keep.lease(std::time::Duration::from_secs(36_000 + u64::from(round))).unwrap();
        trunk.create_branch(&format!("in-window-{round}")).unwrap();
        db.branch_lease_clock_advance(std::time::Duration::from_secs(120));
        let reaped = db.expire_branches().unwrap().reaped.len();
        if round == 0 {
            assert!(reaped >= 8, "premise: the expiry in the window reaped {reaped} of the 8 short leases");
        }
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
    }
    super::store::CAPTURE_VERIFY_FORCED.store(false, Release);
    let verified = super::store::LEASE_FLOORS_VERIFIED.load(Relaxed) - verified0;
    let failures = super::store::CAPTURE_VERIFY_FAILURES.lock().unwrap().clone();
    assert!(verified >= 2, "premise: the check compared {verified} installs' lease floors, not 2");
    assert!(failures.is_empty(), "CLAIM: the writer-read lease floor was not the catalog's at the install, with operations in the window: {failures:?}");
    let _ = short;
}

/// Review REVIEW-claude-engine-m1-2 MED-A (DECISIONS bda6df71dc), as a class: under the whole-run
/// capture check (`FE_CAPTURE_VERIFY`) every check that fails records the failure and ABORTS the
/// process, as the frozen-rows check does, so no later test passes over it. At ee1d54b6c the
/// lease-floor check panicked instead, inside the install under the store mutex, on the checkpoint
/// worker, whose `catch_unwind` swallowed it (and left the install half done). Here a child of this
/// test binary runs the lease-floor test with the check on and mutant `lease_floor_after_stale`
/// (a mismatch at every install): it must die of SIGABRT, not exit with a test failure.
#[cfg(unix)]
#[test]
fn a_failed_capture_check_under_the_whole_run_switch_aborts_the_process() {
    use std::os::unix::process::ExitStatusExt;
    let _s = serial();
    let exe = std::env::current_exe().unwrap();
    // Review engine-rev-setter LOW-6: a deadline (generous: it costs nothing when the child aborts), so a child the half-done install hangs fails this
    // test instead of hanging it.
    let mut child = std::process::Command::new(exe)
        .args([
            "--exact",
            "branch::fastest_tests::a_fuzzy_checkpoint_s_lease_floor_is_the_catalog_s_at_its_install",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("FE_CAPTURE_VERIFY", "1")
        .env("FE_MUTANT", "lease_floor_after_stale")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr_pipe = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut s = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stderr_pipe, &mut s);
        s
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("CLAIM: the child hung (300 s) instead of aborting on the failed capture check");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let out = std::process::Output { status, stdout: Vec::new(), stderr: reader.join().unwrap() };
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("FE_CAPTURE_VERIFY: the lease floor"),
        "premise: the child's lease-floor check never fired (status {:?}); stderr tail: {}",
        out.status,
        &stderr[stderr.len().saturating_sub(2000)..]
    );
    assert_eq!(
        out.status.signal(),
        Some(libc::SIGABRT),
        "CLAIM: a failed capture check under FE_CAPTURE_VERIFY did not abort the process (status {:?})",
        out.status
    );
}

/// Lead ruling "cache-key and reprepare classes" (wire-rework review B-H2): a statement prepared
/// with a search path is re-prepared, after a prepare-generation bump, under that search path, as
/// PG re-analyses under the session's search_path; never under defaults. At 67353b158
/// `Statement::reprepare` translated with `PrepareOptions::default()`, so an unqualified name then
/// resolved to main: wrong rows for every connection with a search path. Two databases each hold
/// `t` with a different row; `SELECT x FROM t` prepared with search path [s] reads s.t, then a real
/// setting change bumps the generation and the next run must still read s.t.
#[test]
fn a_statement_reprepared_after_a_setting_change_keeps_its_search_path() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    // Setup completed before its first run (lead ruling "reprepare red invalid"): ATTACH is off by
    // default (DatabaseOpts derives Default; translate/attach.rs refuses), so the red never reached
    // its claim.
    let db = open_at(&dir.path().join("search-path-main.db"), DatabaseOpts::new().with_attach(true));
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(x INTEGER)").unwrap();
    c.execute("INSERT INTO t VALUES (1)").unwrap();
    let other = dir.path().join("search-path-s.db");
    let attached = c.execute(format!("ATTACH '{}' AS s", other.display()));
    assert!(attached.is_ok(), "premise: ATTACH succeeded: {attached:?}");
    c.execute("CREATE TABLE s.t(x INTEGER)").unwrap();
    c.execute("INSERT INTO s.t VALUES (2)").unwrap();
    let sql = "SELECT x FROM t";
    let (cmd, _) = c.parse_sql(sql).unwrap();
    let options = crate::connection::PrepareOptions { unqualified_database_search_path: Some(vec!["s".to_string()]) };
    let mut stmt = c.prepare_translated_cmd_with_options(cmd.unwrap(), sql, options).unwrap();
    let read = |stmt: &mut crate::Statement| -> Vec<i64> {
        let mut out = Vec::new();
        loop {
            match stmt.step().unwrap() {
                crate::StepResult::Row => out.push(stmt.row().unwrap().get_value(0).as_int().expect("an integer x")),
                crate::StepResult::IO | crate::StepResult::Yield => db.io.step().unwrap(),
                crate::StepResult::Done => break,
                other => panic!("premise: the SELECT runs to completion, not {other:?}"),
            }
        }
        stmt.reset().unwrap();
        out
    };
    assert_eq!(read(&mut stmt), vec![2], "premise: prepared with search path [s], the SELECT reads s.t");
    let reprepares0 = stmt.metrics().reprepares;
    let g0 = c.prepare_context_generation();
    c.set_cache_size(c.get_cache_size() + 1);
    assert_ne!(c.prepare_context_generation(), g0, "premise: the setting change bumped the prepare generation");
    let after = read(&mut stmt);
    assert!(stmt.metrics().reprepares > reprepares0, "premise: the statement was re-prepared after the bump");
    assert_eq!(after, vec![2], "CLAIM: re-prepared after a setting change, the SELECT read {after:?}, not s.t's row: the search path was dropped");
}

/// Review C42 LOW-7: off Apple the pager has one sync type (fsync) and `set_sync_type` changes
/// nothing PRAGMA fullfsync compiles in, so it must never bump: at 8a77618a5 setting FullFsync passed
/// the "differs from the pager's" check on every call and re-prepared everything each time.
#[cfg(not(target_vendor = "apple"))]
#[test]
fn off_apple_set_sync_type_never_bumps_the_prepare_generation() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("sync-type-off-apple.db"), DatabaseOpts::new());
    let c = db.connect().unwrap();
    let g0 = c.prepare_context_generation();
    c.set_sync_type(crate::io::FileSyncType::FullFsync);
    c.set_sync_type(crate::io::FileSyncType::FullFsync);
    assert_eq!(c.get_sync_type(), crate::io::FileSyncType::Fsync, "premise: off Apple the pager's sync type stays fsync");
    assert_eq!(c.prepare_context_generation(), g0, "CLAIM: set_sync_type bumped the prepare generation though nothing it compiles in changed");
}

/// Review C42 MED-7 (lead ruling): a journal-mode switch changes what translation compiles in for
/// EVERY connection of the database (`Connection::mvcc_enabled` reads the Database's MV store), so it
/// bumps a database-wide generation every connection's prepare key reads. At f6186c900 only the
/// switching connection's own setters bumped, so B's cached programs kept the WAL compilation.
#[test]
fn a_journal_mode_switch_on_one_connection_moves_every_connection_s_prepare_generation() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("journal-switch-generation.db"), DatabaseOpts::new());
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();
    a.execute("CREATE TABLE t(x INTEGER)").unwrap();
    let _cached = b.prepare("INSERT INTO t VALUES (1)").unwrap();
    let g0 = b.prepare_context_generation();
    let switched = a.execute("PRAGMA journal_mode = 'mvcc'");
    assert!(switched.is_ok(), "premise: the journal-mode switch succeeded: {switched:?}");
    assert!(a.mvcc_enabled(), "premise: the database is in MVCC mode after the switch");
    assert_ne!(
        b.prepare_context_generation(),
        g0,
        "CLAIM: another connection's journal-mode switch left this connection's prepare generation alone: its cached programs keep the old mode"
    );
}

/// Review C42 LOW-4: `PRAGMA auto_vacuum` compiles the pager's mode in as a constant, so setting a
/// different mode (allowed until page 1 exists) bumps the prepare generation. At 1b0e4f2e7
/// `persist_auto_vacuum_mode` changed it with no bump.
#[test]
fn setting_auto_vacuum_moves_the_prepare_generation() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("auto-vacuum-generation.db"), DatabaseOpts::new().with_autovacuum(true));
    let c = db.connect().unwrap();
    let mode0 = c.get_pager().get_auto_vacuum_mode();
    let g0 = c.prepare_context_generation();
    c.execute("PRAGMA auto_vacuum = FULL").unwrap();
    assert_ne!(c.get_pager().get_auto_vacuum_mode(), mode0, "premise: the auto-vacuum mode changed");
    assert_ne!(c.prepare_context_generation(), g0, "CLAIM: PRAGMA auto_vacuum changed the mode and left the prepare generation alone");
}

/// Wire-rev-line M1 (lead order): `maybe_update_schema` compared only the MAIN schema's version, so
/// another session's DDL on an ATTACHED database was never adopted between statements: this
/// connection's cached programs kept the old attached schema. Two connections, each with s
/// attached; B adds a column to s.t; A's adoption step must move A's prepare generation, and A's
/// next statement reads the new column.
#[test]
fn another_session_s_ddl_on_an_attached_schema_is_adopted_between_statements() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("attached-adopt-main.db"), DatabaseOpts::new().with_attach(true));
    let other = dir.path().join("attached-adopt-s.db");
    let attach = format!("ATTACH '{}' AS s", other.display());
    let a = db.connect().unwrap();
    let b = db.connect().unwrap();
    let attached = (a.execute(&attach), b.execute(&attach));
    assert!(attached.0.is_ok() && attached.1.is_ok(), "premise: both connections attached s: {attached:?}");
    a.execute("CREATE TABLE s.t(x INTEGER)").unwrap();
    a.execute("INSERT INTO s.t VALUES (1)").unwrap();
    a.maybe_update_schema();
    let g0 = a.prepare_context_generation();
    let altered = b.execute("ALTER TABLE s.t ADD COLUMN y INTEGER");
    assert!(altered.is_ok(), "premise: B's ALTER on the attached table succeeded: {altered:?}");
    a.maybe_update_schema();
    let moved = a.prepare_context_generation() != g0;
    let read = a.prepare("SELECT y FROM s.t").and_then(|mut st| st.run_collect_rows());
    assert!(read.is_ok(), "CLAIM: A's next statement did not see B's new column on the attached table: {read:?}");
    assert!(moved, "CLAIM: A's adoption step left its prepare generation alone after B's DDL on the attached schema: A's cached programs keep the old one");
}

/// Review c42b MED-3 and LOW-2, as a class: whether the database is MVCC is compiled into every
/// connection's programs (`Connection::mvcc_enabled` reads `Database::mv_store`), so EVERY install
/// or removal of the MV store bumps the database-wide prepare generation (C42 MED-7) next to it.
/// At 0b3abfefc the journal switch's two sites did, but not the bootstrap guard's removal
/// (vdbe/execute.rs), the external-restore reload's install and removal (database.rs), the open's
/// lazy install, or the attach's. Static: every `mv_store.store(` in core's source is followed,
/// within three lines, by a `bump_prepare_context_generation()`.
#[test]
fn every_mv_store_install_or_removal_bumps_the_database_wide_generation() {
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sites = 0usize;
    let mut unbumped = Vec::new();
    let mut dirs = vec![core.to_path_buf()];
    while let Some(d) = dirs.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "target") {
                    dirs.push(path);
                }
                continue;
            }
            if !path.extension().is_some_and(|x| x == "rs") || path.ends_with("branch/fastest_tests.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            let lines: Vec<&str> = text.lines().collect();
            for (i, l) in lines.iter().enumerate() {
                if l.trim_start().starts_with("//") || !l.contains("mv_store.store(") {
                    continue;
                }
                sites += 1;
                let bumped = lines[i + 1..(i + 4).min(lines.len())].iter().any(|n| n.contains("bump_prepare_context_generation()"));
                if !bumped {
                    unbumped.push(format!("{}:{}", path.strip_prefix(core).unwrap().display(), i + 1));
                }
            }
        }
    }
    assert!(sites >= 5, "premise: the scan found {sites} MV-store stores, fewer than the 7 known");
    assert!(unbumped.is_empty(), "CLAIM: MV-store installs or removals with no database-wide generation bump: {unbumped:?}");
}

/// The prepare-generation bump sites in one source file's text, per enclosing function, as
/// `every_prepare_generation_bump_site_is_audited` pins them (review c42b LOW-4). Read with comment
/// lines dropped and all whitespace removed, so a call or a raw write rustfmt splits over lines is
/// one site; a raw write is any atomic write method on the field; the bump functions' own bodies are
/// their definitions, not sites. Each site is keyed by the nearest `fn NAME` above it.
fn bump_sites(text: &str) -> std::collections::BTreeMap<String, usize> {
    const WRITES: &[&str] = &["fetch_add(", "fetch_sub(", "fetch_max(", "fetch_or(", "fetch_update(", "store(", "swap("];
    let lines: Vec<&str> = text.lines().collect();
    let mut s = String::new();
    let mut line_of = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if l.trim_start().starts_with("//") {
            continue;
        }
        for c in l.chars().filter(|c| !c.is_whitespace()) {
            line_of.extend(std::iter::repeat_n(i, c.len_utf8()));
            s.push(c);
        }
    }
    let definition = "fnbump_prepare_context_generation(&self){";
    let mut from = 0;
    while let Some(k) = s[from..].find(definition) {
        let start = from + k;
        let end = s[start..].find('}').map_or(s.len(), |e| start + e + 1);
        s.replace_range(start..end, &"#".repeat(end - start));
        from = end;
    }
    let mut patterns = vec!["bump_prepare_context_generation()".to_string()];
    for close in ["", ")"] {
        for w in WRITES {
            patterns.push(format!("prepare_context_generation{close}.{w}"));
        }
    }
    let fn_name = |line: &str| -> Option<String> {
        let mut rest = line;
        while let Some(k) = rest.find("fn ") {
            let starts_word = !rest[..k].chars().next_back().is_some_and(|c| c.is_alphanumeric() || c == '_');
            let after = &rest[k + 3..];
            let name = &after[..after.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(after.len())];
            if starts_word && !name.is_empty() {
                return Some(name.to_string());
            }
            rest = after;
        }
        None
    };
    let mut sites = std::collections::BTreeMap::new();
    for pattern in &patterns {
        for (k, _) in s.match_indices(pattern.as_str()) {
            let name = (0..=line_of[k])
                .rev()
                .filter(|&j| !lines[j].trim_start().starts_with("//"))
                .find_map(|j| fn_name(lines[j]))
                .unwrap_or_else(|| "?".to_string());
            *sites.entry(name).or_insert(0) += 1;
        }
    }
    sites
}

/// All of a file's bump sites (the planted-form red of review c42b LOW-4 counts with this).
fn bump_site_count(text: &str) -> usize {
    bump_sites(text).values().sum()
}

/// Review c42b LOW-4, second half: the gate pins each site to its enclosing function, so a bump
/// removed in one function and added in another of the same file changes what the gate sees.
#[test]
fn the_bump_site_scan_sees_a_site_move_between_functions_of_one_file() {
    let before = "fn a(&self) {\n    self.bump_prepare_context_generation();\n}\nfn b(&self) {}\n";
    let after = "fn a(&self) {}\nfn b(&self) {\n    self.bump_prepare_context_generation();\n}\n";
    assert_eq!(bump_site_count(before), bump_site_count(after), "premise: the move keeps the count");
    assert_ne!(bump_sites(before), bump_sites(after), "CLAIM: a bump moved between functions of one file is invisible to the scan");
}

/// Review c42b LOW-4: the bump-site scan counts every form a bump is written in, so a site rustfmt
/// splits over two lines (`self.prepare_context_generation` then `.fetch_add(`, as the two bump
/// definitions themselves are written) cannot hide from the audit gate. Planted texts: each
/// form counts once; comments and the bump functions' own bodies count zero. At 01d412f8e the
/// scan read one line at a time, so the two-line form counted zero.
#[test]
fn the_bump_site_scan_counts_every_form_of_a_bump() {
    let one_line = "fn a(&self) {\n    self.prepare_context_generation.fetch_add(1, Ordering::Release);\n}\n";
    let two_line = "fn a(&self) {\n    self.prepare_context_generation\n        .fetch_add(1, Ordering::Release);\n}\n";
    let ext_two_line = "fn a() {\n    (*ctx.prepare_context_generation)\n        .fetch_add(1, Ordering::Release);\n}\n";
    let call = "fn a(&self) {\n    self.db\n        .bump_prepare_context_generation();\n}\n";
    let stored = "fn a(&self) {\n    self.prepare_context_generation\n        .store(0, Ordering::Release);\n}\n";
    let comment = "fn a(&self) {\n    // self.prepare_context_generation.fetch_add(1, Ordering::Release);\n}\n";
    let definition = "pub(crate) fn bump_prepare_context_generation(&self) {\n    self.prepare_context_generation\n        .fetch_add(1, Ordering::Release);\n}\n";
    let mut wrong = Vec::new();
    for (name, text, want) in [
        ("one line", one_line, 1),
        ("two lines", two_line, 1),
        ("an extension's two lines", ext_two_line, 1),
        ("a split call", call, 1),
        ("a store", stored, 1),
        ("a comment", comment, 0),
        ("the bump's own definition", definition, 0),
    ] {
        let got = bump_site_count(text);
        if got != want {
            wrong.push(format!("{name}: counted {got}, want {want}"));
        }
    }
    assert!(wrong.is_empty(), "CLAIM: the bump-site scan miscounts a form of a bump: {wrong:?}");
}

/// Review engine-rev-setter LOW-2: the audit of every prepare-generation bump (49e10e02a's message
/// said 37 and missed five) is pinned here, per file, so a new bump site fails this gate until it
/// is audited (and the setter class above says whether it should be change-only). Counts calls of
/// `bump_prepare_context_generation()` and the extensions' raw `prepare_context_generation)
/// .fetch_add(`, outside comments and the function's own definition. Audited at 09e8b26b0:
/// - MV-store installs and removals (review c42b MED-3, LOW-2): connection.rs +1 (the attach's),
///   database.rs 3 (the open's lazy install, the restore reload's install and removal),
///   vdbe/execute.rs +1 (the bootstrap guard's removal).
/// - connection.rs 39: the change-only setters (17: set_temp_store and the 16 of 49e10e02a and
///   09e8b26b0); an attached schema's adoption on a change (wire-rev-line M1); schema, attach,
///   detach, rollback and collation events that are themselves changes; set_busy_handler, the
///   encryption setters and the mvcc thresholds, unconditional (not comparable, or rare).
/// - ext/mod.rs 4: extension registration (functions, vtabs), each a change.
/// - mvcc/database/checkpoint_state_machine.rs 1: an unconditional schema reload, itself a change.
/// - storage/pager.rs 1: rollback of a schema change. translate/pragma.rs 2: cache_spill and
///   auto_vacuum, each on a change (review C42 LOW-4). vdbe/execute.rs 3: savepoint schema restore, and the journal-mode switch's two
///   Database-wide bumps (MV store installed, removed; review C42 MED-7). vdbe/mod.rs 1: an
///   abandoned MVCC commit's schema removed.
/// - Also counted (review C42 LOW-6): a raw `prepare_context_generation.fetch_add(` on one line.
/// - Since review c42b LOW-4: every form (`bump_sites`: split over lines, any atomic write), pinned
///   per enclosing function, so a site moved within a file is seen.
#[test]
fn every_prepare_generation_bump_site_is_audited() {
    // Per enclosing function (review c42b LOW-4). The per-file totals are the audited ones below
    // (connection.rs 39, database.rs 3, ext/mod.rs 4, mvcc/database/checkpoint_state_machine.rs 1,
    // storage/pager.rs 1, translate/pragma.rs 2, vdbe/execute.rs 4, vdbe/mod.rs 1); the split by
    // function came from an independent scan (a Python script over the source, not this one), checked
    // against those classes. `database.rs` `header_validation` is the open's lazy install; `vdbe/
    // execute.rs` `drop` is the bootstrap guard's.
    const AUDITED: &[(&str, &str, usize)] = &[
        ("connection.rs", "attach_database_with_config", 2),
        ("connection.rs", "detach_database", 1),
        ("connection.rs", "maybe_update_schema", 2),
        ("connection.rs", "publish_database_schema", 1),
        ("connection.rs", "refresh_schema_from_shared_for_reprepare", 1),
        ("connection.rs", "register_external_collation", 1),
        ("connection.rs", "register_internal_vtab", 1),
        ("connection.rs", "register_virtual_table", 1),
        ("connection.rs", "reset_page_size", 1),
        ("connection.rs", "rollback_attached_mvcc_txs", 1),
        ("connection.rs", "rollback_attached_wal_txns", 1),
        ("connection.rs", "rollback_temp_schema", 1),
        ("connection.rs", "set_busy_handler", 1),
        ("connection.rs", "set_busy_timeout", 1),
        ("connection.rs", "set_cache_size", 1),
        ("connection.rs", "set_capture_data_changes_info", 1),
        ("connection.rs", "set_check_constraints_ignored", 1),
        ("connection.rs", "set_count_changes", 1),
        ("connection.rs", "set_data_sync_retry", 1),
        ("connection.rs", "set_dml_require_where", 1),
        ("connection.rs", "set_dqs_dml", 1),
        ("connection.rs", "set_encryption_cipher", 1),
        ("connection.rs", "set_encryption_key", 1),
        ("connection.rs", "set_foreign_keys_enabled", 1),
        ("connection.rs", "set_full_column_names", 1),
        ("connection.rs", "set_mvcc_checkpoint_threshold", 1),
        ("connection.rs", "set_mvcc_gc_threshold", 1),
        ("connection.rs", "set_query_only", 1),
        ("connection.rs", "set_recipe_backfill", 1),
        ("connection.rs", "set_row_image_apply", 1),
        ("connection.rs", "set_short_column_names", 1),
        ("connection.rs", "set_sync_mode", 1),
        ("connection.rs", "set_sync_type", 1),
        ("connection.rs", "set_temp_store", 1),
        ("connection.rs", "unregister_external_collation", 1),
        ("connection.rs", "with_database_schema_mut", 2),
        ("database.rs", "header_validation", 1),
        ("database.rs", "reload_wal_after_external_restore", 2),
        ("ext/mod.rs", "register_aggregate_function", 1),
        ("ext/mod.rs", "register_scalar_function_with_options", 1),
        ("ext/mod.rs", "register_vtab_module", 1),
        ("ext/mod.rs", "unregister_function", 1),
        ("mvcc/database/checkpoint_state_machine.rs", "publish_checkpointed_schema_roots", 1),
        ("storage/pager.rs", "rollback", 1),
        ("translate/pragma.rs", "update_pragma", 2),
        ("vdbe/execute.rs", "drop", 1),
        ("vdbe/execute.rs", "op_journal_mode_inner", 2),
        ("vdbe/execute.rs", "op_savepoint", 1),
        ("vdbe/mod.rs", "cleanup_abandoned_mvcc_commit", 1),
    ];
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found: std::collections::BTreeMap<(String, String), usize> = Default::default();
    let mut dirs = vec![core.to_path_buf()];
    while let Some(d) = dirs.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "target") {
                    dirs.push(path);
                }
            } else if path.extension().is_some_and(|x| x == "rs") && !path.ends_with("branch/fastest_tests.rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                let rel = path.strip_prefix(core).unwrap().to_string_lossy().replace('\\', "/");
                for (function, n) in bump_sites(&text) {
                    found.insert((rel.clone(), function), n);
                }
            }
        }
    }
    let audited: std::collections::BTreeMap<(String, String), usize> =
        AUDITED.iter().map(|&(f, function, n)| ((f.to_string(), function.to_string()), n)).collect();
    assert!(!found.is_empty(), "premise: the scan found no bump site: it is broken");
    assert_eq!(found, audited, "CLAIM: the prepare-generation bump sites differ from the audited list: audit the new ones (and the setter class) and update AUDITED with each one's reason");
}

/// Review engine-rev-setter MED-1 (lead order), as a class: which setters must bump the prepare
/// generation is DERIVED from what translation reads, not from a list of the bump's callers (an
/// audit of callers cannot see a setter that never bumps). Every `Connection` setter `set_X` whose
/// value translation reads (a call `.X()`, `.get_X()` or `.is_X()` anywhere under core/translate/)
/// must bump in its body. At 19f7a7bfc set_check_constraints_ignored, set_dml_require_where,
/// set_count_changes and set_sync_type never bumped, so a cached INSERT compiled with CHECK ignored
/// kept skipping it after the setting was turned back on. Static: reads the source.
#[test]
fn every_setter_whose_value_translation_reads_bumps_the_prepare_generation() {
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let conn = std::fs::read_to_string(core.join("connection.rs")).unwrap();
    let mut translate = String::new();
    let mut dirs = vec![core.join("translate")];
    while let Some(d) = dirs.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let path = e.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|x| x == "rs") {
                translate.push_str(&std::fs::read_to_string(&path).unwrap());
            }
        }
    }
    // Review C42 LOW-5: pub(super) and generic setters, and the Arc-receiver form, are scanned too.
    let setter = regex::Regex::new(r"\n    pub(?:\((?:crate|super)\))? fn set_(\w+)(?:<[^>]*>)?\((?:&self|self: &Arc<Self>)[^{]*\{").unwrap();
    let mut checked = Vec::new();
    let mut missing = Vec::new();
    for m in setter.captures_iter(&conn) {
        let name = m.get(1).unwrap().as_str();
        let start = m.get(0).unwrap().end();
        let (mut depth, mut end) = (1i32, start);
        for (i, ch) in conn[start..].char_indices() {
            depth += match ch {
                '{' => 1,
                '}' => -1,
                _ => 0,
            };
            if depth == 0 {
                end = start + i;
                break;
            }
        }
        let body = &conn[start..end];
        let read = [name.to_string(), format!("get_{name}"), format!("is_{name}")]
            .iter()
            .any(|g| translate.contains(&format!(".{g}()")));
        if read {
            checked.push(name.to_string());
            if !body.contains("bump_prepare_context_generation") {
                missing.push(name.to_string());
            }
        }
    }
    // Review C42 LOW-5: the EXACT derived set (a Python run of the same scan over 498074b78), so a
    // partial break of the scan, or a new setter read in translation, fails here until it is looked
    // at, not only a scan that finds fewer than ten.
    const DERIVED: &[&str] = &[
        "busy_timeout", "cache_size", "capture_data_changes_info", "check_constraints_ignored", "count_changes",
        "data_sync_retry", "dml_require_where", "dqs_dml", "foreign_keys_enabled", "full_column_names",
        "mvcc_checkpoint_threshold", "mvcc_gc_threshold", "query_only", "row_image_apply", "short_column_names",
        "sync_mode", "sync_type", "temp_store",
    ];
    let mut sorted = checked.clone();
    sorted.sort();
    assert_eq!(sorted, DERIVED, "premise: the setters read under translate/ differ from the derived set: a broken scan, or a new setter to classify");
    assert!(
        missing.is_empty(),
        "CLAIM: setters whose value translation reads never bump the prepare generation (cached programs keep the old value): {missing:?}"
    );
}

/// Lead ruling DECISIONS 179e043b9e (wire-rev-fkguard MED 1), as a class: a setter whose side
/// effect invalidates compiled programs bumps the prepare generation only on a CHANGE. The wire's
/// publish sets foreign_keys at every transaction end, so an unconditional bump re-prepared every
/// cached statement after every commit. Each setter given the value it holds must leave the
/// generation alone, and (positive control) given another must move it.
#[test]
fn a_setter_given_the_value_it_holds_leaves_the_prepare_generation_alone() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("prepare-generation.db"), opts(true, SyncClass::Off));
    let c = db.connect().unwrap();
    let mut same_bumped = Vec::new();
    let mut change_missed = Vec::new();
    let mut check = |name: &'static str, same: &dyn Fn(), other: &dyn Fn(), back: &dyn Fn()| {
        let g0 = c.prepare_context_generation();
        same();
        if c.prepare_context_generation() != g0 {
            same_bumped.push(name);
        }
        let g1 = c.prepare_context_generation();
        other();
        if c.prepare_context_generation() == g1 {
            change_missed.push(name);
        }
        back();
    };
    let v = c.foreign_keys_enabled();
    check("set_foreign_keys_enabled", &|| c.set_foreign_keys_enabled(v), &|| c.set_foreign_keys_enabled(!v), &|| c.set_foreign_keys_enabled(v));
    let v = c.row_image_apply();
    check("set_row_image_apply", &|| c.set_row_image_apply(v), &|| c.set_row_image_apply(!v), &|| c.set_row_image_apply(v));
    let v = c.get_cache_size();
    check("set_cache_size", &|| c.set_cache_size(v), &|| c.set_cache_size(v + 1), &|| c.set_cache_size(v));
    let v = c.get_query_only();
    check("set_query_only", &|| c.set_query_only(v), &|| c.set_query_only(!v), &|| c.set_query_only(v));
    let v = c.recipe_backfill();
    check("set_recipe_backfill", &|| c.set_recipe_backfill(v), &|| c.set_recipe_backfill(!v), &|| c.set_recipe_backfill(v));
    let v = c.get_dqs_dml();
    check("set_dqs_dml", &|| c.set_dqs_dml(v), &|| c.set_dqs_dml(!v), &|| c.set_dqs_dml(v));
    let v = c.get_full_column_names();
    check("set_full_column_names", &|| c.set_full_column_names(v), &|| c.set_full_column_names(!v), &|| c.set_full_column_names(v));
    let v = c.get_short_column_names();
    check("set_short_column_names", &|| c.set_short_column_names(v), &|| c.set_short_column_names(!v), &|| c.set_short_column_names(v));
    let v = c.get_sync_mode();
    let w = if v == crate::SyncMode::Full { crate::SyncMode::Normal } else { crate::SyncMode::Full };
    check("set_sync_mode", &|| c.set_sync_mode(v), &|| c.set_sync_mode(w), &|| c.set_sync_mode(v));
    let v = c.get_data_sync_retry();
    check("set_data_sync_retry", &|| c.set_data_sync_retry(v), &|| c.set_data_sync_retry(!v), &|| c.set_data_sync_retry(v));
    // Review engine-rev-setter LOW-3: the two changed sites the red did not cover. The current CDC
    // value is cloned on its own line: inline, its read guard would still be held when the setter
    // takes the write lock.
    let v = c.get_capture_data_changes_info().clone();
    let w = crate::CaptureDataChangesInfo::parse("id", None).unwrap();
    check(
        "set_capture_data_changes_info",
        &|| c.set_capture_data_changes_info(v.clone()),
        &|| c.set_capture_data_changes_info(w.clone()),
        &|| c.set_capture_data_changes_info(v.clone()),
    );
    let v = c.get_pager().get_spill_enabled();
    let pragma = |on: bool| c.execute(format!("PRAGMA cache_spill = {}", if on { 1 } else { 0 })).unwrap();
    check("PRAGMA cache_spill", &|| pragma(v), &|| pragma(!v), &|| pragma(v));
    // Review C42 MED-6: the four setters 09e8b26b0 made change-only (a mutant bumping any of them
    // unconditionally survived the static red, which reads only that the body bumps).
    let v = c.check_constraints_ignored();
    check("set_check_constraints_ignored", &|| c.set_check_constraints_ignored(v), &|| c.set_check_constraints_ignored(!v), &|| c.set_check_constraints_ignored(v));
    let v = c.get_dml_require_where();
    check("set_dml_require_where", &|| c.set_dml_require_where(v), &|| c.set_dml_require_where(!v), &|| c.set_dml_require_where(v));
    let v = c.get_count_changes();
    check("set_count_changes", &|| c.set_count_changes(v), &|| c.set_count_changes(!v), &|| c.set_count_changes(v));
    // Only Apple's pager distinguishes the two (elsewhere both are fsync, and the setter does not
    // change what PRAGMA fullfsync answers, so it must not bump at all: review C42 LOW-7).
    #[cfg(target_vendor = "apple")]
    {
        let v = c.get_sync_type();
        let w = if v == crate::io::FileSyncType::FullFsync { crate::io::FileSyncType::Fsync } else { crate::io::FileSyncType::FullFsync };
        check("set_sync_type", &|| c.set_sync_type(v), &|| c.set_sync_type(w), &|| c.set_sync_type(v));
    }
    let v = c.get_busy_timeout();
    let w = v + std::time::Duration::from_millis(7);
    check("set_busy_timeout", &|| c.set_busy_timeout(v), &|| c.set_busy_timeout(w), &|| c.set_busy_timeout(v));
    assert!(change_missed.is_empty(), "premise: a setter given ANOTHER value did not move the prepare generation: {change_missed:?}");
    assert!(same_bumped.is_empty(), "CLAIM: setters given the value they hold re-prepared every statement: {same_bumped:?}");
}

/// Review REVIEW-claude-engine-M1 LOW-5: each path the M1 fix names gets its own owed start sent,
/// not only `expire_branches`. The store is built at the default threshold with 300 forks leased
/// for a minute and one branch `keep` with ten hours; the lease clock then passes the minute, the
/// threshold drops to 1 byte (the log is past it, and past twice the snapshot), and ONE operation
/// of the path under test runs: its expiry pass reaps the 300 and owes the checkpoint, which must
/// start with no other operation. Mutant `owed_start_unsent`: red on every path.
struct LeasedPastDue {
    db: Arc<Database>,
    trunk: Arc<Connection>,
    keep: Branch,
    held: Vec<Branch>,
    /// One of the 300, detached (`into_id`): reaped by the operation's expiry, `db.branch` refuses
    /// it (review m1-2 LOW-C's premise).
    probe: BranchId,
    started: u64,
}

/// Review engine-rev-setter LOW-7: the operation's OWN unlock sent the start it owed, read with no
/// store-mutex hold (any hold's unlock would send an owed start and rescue the claim below):
/// nothing is still owed, and a job was handed out since `handed0`. Mutant `owed_start_sent_late`.
fn assert_the_operation_sent_its_start(probe: &super::store::StarterProbe, handed0: u64, path: &str) {
    assert!(!probe.owed(), "CLAIM: {path} left its owed checkpoint start unsent at its own unlock");
    assert!(probe.handed_out() > handed0, "CLAIM: {path}'s unlock handed no checkpoint start to the worker");
}

/// Premise (review m1-2 LOW-C): the operation's expiry pass reaped the 300, so a failure below is
/// the start path's, not the fixture's.
fn assert_the_expiry_reaped(db: &Arc<Database>, probe: BranchId, path: &str) {
    assert!(db.branch(probe).is_err(), "premise: {path}'s expiry pass did not reap the 300 leased branches");
}

fn leased_past_due(path: &Path) -> LeasedPastDue {
    let db = open_at(path, opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let keep = trunk.fork_branch().unwrap();
    keep.lease(std::time::Duration::from_secs(36_000)).unwrap();
    trunk.create_branch("named-keep").unwrap();
    let mut held = Vec::new();
    let mut probe = None;
    for _ in 0..300 {
        let b = trunk.fork_branch().unwrap();
        b.lease(std::time::Duration::from_secs(60)).unwrap();
        if probe.is_none() {
            probe = Some(b.into_id());
        } else {
            held.push(b);
        }
    }
    db.branch_checkpoint_wait();
    let started = db.branch_checkpoint_counters()[1];
    db.branch_lease_clock_advance(std::time::Duration::from_secs(3600));
    LeasedPastDue { db, trunk, keep, held, probe: probe.expect("300 forks"), started }
}

/// After the one operation: a checkpoint started (fuzzy flights started rose past `started`).
fn assert_a_checkpoint_started(db: &Database, started: u64, path: &str) {
    let t = std::time::Instant::now();
    while db.branch_checkpoint_counters()[1] == started && t.elapsed() < std::time::Duration::from_secs(10) {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    db.branch_checkpoint_wait();
    assert!(
        db.branch_checkpoint_counters()[1] > started,
        "CLAIM: {path}'s expiry pass reaped past the threshold and no checkpoint started ({started} before, as many after)"
    );
}

#[test]
fn a_lease_renewal_whose_expiry_crosses_the_threshold_starts_a_checkpoint() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let s = leased_past_due(&dir.path().join("owed-set-lease.db"));
    let _t = Threshold::set(1);
    let starter = s.db.branches.starter_probe();
    let handed0 = starter.handed_out();
    s.keep.lease(std::time::Duration::from_secs(60)).unwrap();
    assert_the_operation_sent_its_start(&starter, handed0, "set_lease");
    assert_the_expiry_reaped(&s.db, s.probe, "set_lease");
    assert_a_checkpoint_started(&s.db, s.started, "set_lease");
    drop(s.held);
}

#[test]
fn a_connect_whose_expiry_crosses_the_threshold_starts_a_checkpoint() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let s = leased_past_due(&dir.path().join("owed-open-conn.db"));
    let _t = Threshold::set(1);
    let starter = s.db.branches.starter_probe();
    let handed0 = starter.handed_out();
    let c = s.keep.connect().unwrap();
    assert_the_operation_sent_its_start(&starter, handed0, "open_conn");
    assert_the_expiry_reaped(&s.db, s.probe, "open_conn");
    assert_a_checkpoint_started(&s.db, s.started, "open_conn");
    drop(c);
    drop(s.held);
}

#[test]
fn a_named_create_refused_after_its_expiry_crossed_the_threshold_starts_a_checkpoint() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let s = leased_past_due(&dir.path().join("owed-create-error.db"));
    let _t = Threshold::set(1);
    let starter = s.db.branches.starter_probe();
    let handed0 = starter.handed_out();
    assert!(s.trunk.create_branch("named-keep").is_err(), "premise: the name was taken, so the create returned an error");
    assert_the_operation_sent_its_start(&starter, handed0, "a refused named create");
    assert_the_expiry_reaped(&s.db, s.probe, "a refused named create");
    assert_a_checkpoint_started(&s.db, s.started, "a refused named create");
    drop(s.held);
}

#[test]
fn an_open_whose_expiry_crosses_the_threshold_starts_a_checkpoint() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("owed-open.db");
    let (incarnation, probe) = {
        let s = leased_past_due(&path);
        // Detached, not released: the 300 outlive the close, and the reopen's expiry reaps them
        // (the close stamps the lease clock, an hour on).
        for b in s.held {
            let _ = b.into_id();
        }
        let _ = s.keep.into_id();
        drop(s.trunk);
        (s.db.incarnation, s.probe)
    };
    let _t = Threshold::set(1);
    let db = reopen(&path, opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy), incarnation);
    assert_the_expiry_reaped(&db, probe, "the open");
    assert_a_checkpoint_started(&db, 0, "the open");
}

/// Review REVIEW-claude-engine-L2d M2: a fuzzy install takes the names and child keys it let go
/// of under the store mutex with no allocation (they are freed after it). At f666f19bd
/// `ChildIndex::take_retired` boxed the generation, one heap allocation inside every install's
/// hold (the lifecycle audit's install site, store.rs:4111 at c1809b546: 18 allocations, raw
/// l111). Premises: the take ran at three installs, one with a retired child generation.
#[test]
fn a_fuzzy_install_takes_its_retired_names_and_children_without_allocating() {
    use std::sync::atomic::Ordering::Relaxed;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("retire-take.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let (takes0, with0, allocs0) = (
        super::store::RETIRE_TAKES.load(Relaxed),
        super::store::RETIRE_TAKES_WITH_CHILDREN.load(Relaxed),
        super::store::RETIRE_TAKE_ALLOCS.load(Relaxed),
    );
    for round in 0..3 {
        if round < 2 {
            for i in 0..16 {
                trunk.create_branch(&format!("r{round}-{i}")).unwrap();
                let _ = trunk.fork_branch().unwrap().into_id();
            }
        }
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: fuzzy checkpoint {round} started");
        db.branch_checkpoint_wait();
    }
    let takes = super::store::RETIRE_TAKES.load(Relaxed) - takes0;
    let with = super::store::RETIRE_TAKES_WITH_CHILDREN.load(Relaxed) - with0;
    let allocs = super::store::RETIRE_TAKE_ALLOCS.load(Relaxed) - allocs0;
    // Review engine-rev-setter LOW-8: exactly the three checkpoints' installs, so an extra one (a
    // threshold's) fails as a premise, not as the with == 2 claim below.
    assert_eq!(takes, 3, "premise: {takes} installs took their retired generations, not the 3 checkpoints'");
    // Review m1-2 LOW-D: exactly the two rounds that forked children retire a generation; round 2,
    // with none forked since, retires nothing (`retire_frozen` keeps `None` for an empty one).
    assert_eq!(with, 2, "CLAIM: {with} of 3 installs retired a child generation, not the 2 that forked children");
    assert_eq!(allocs, 0, "CLAIM: taking the retired names and children allocated {allocs} times under the store mutex over {takes} installs");
}

/// Review C42-M1 (lead order): a client's CHECKPOINT waits for a fuzzy checkpoint in flight
/// within its bound, never unboundedly: `branch_compact_now_within` returns `Busy` at its deadline
/// and `Interrupt` as soon as its interrupt says so, with the flight still held (here at
/// `HOLD_BEFORE_ROWS`). Before 2026-10-17 the join had no deadline, and neither cancel nor statement_timeout
/// ended it. Each call runs on its own thread; the test refuses one that has not returned in 30 s.
#[test]
fn a_client_checkpoint_s_wait_for_a_flight_ends_at_its_deadline_or_its_interrupt() {
    let _s = serial();
    struct Release(Arc<Database>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.branch_checkpoint_hold(0);
        }
    }
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-within.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for _ in 0..8 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let _release = Release(db.clone());
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_ROWS);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    let t = std::time::Instant::now();
    while db.branch_checkpoint_held() != super::store::HOLD_BEFORE_ROWS | super::store::HOLD_ARRIVED {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the flight never reached its hold");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let call = |until: std::time::Duration, interrupt_after: Option<std::time::Duration>| {
        let (tx, rx) = std::sync::mpsc::channel();
        let db = db.clone();
        std::thread::spawn(move || {
            let begun = std::time::Instant::now();
            let interrupted = move || interrupt_after.is_some_and(|d| begun.elapsed() >= d);
            let r = db.branch_compact_now_within(begun + until, &interrupted);
            let _ = tx.send((r, begun.elapsed()));
        });
        rx.recv_timeout(std::time::Duration::from_secs(30))
    };
    let at_deadline = call(std::time::Duration::from_millis(300), None);
    let interrupted = call(std::time::Duration::from_secs(600), Some(std::time::Duration::from_millis(100)));
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    let (at_deadline, took) = at_deadline.expect("CLAIM: a client's CHECKPOINT waited past its 300 ms deadline for the flight in flight (no return in 30 s)");
    let (interrupted, _) = interrupted.expect("CLAIM: a client's CHECKPOINT went on waiting for the flight after its interrupt (no return in 30 s)");
    // Review C42 MED-4 (FLAGGED: this red's own assertions sharpened before it first ran): by
    // variant, not by text; not before the deadline; and a positive control.
    assert!(matches!(at_deadline, Err(LimboError::Busy)), "CLAIM: at its deadline the CHECKPOINT did not report Busy: {at_deadline:?}");
    assert!(took >= std::time::Duration::from_millis(300), "CLAIM: the CHECKPOINT gave up before its 300 ms deadline ({took:?})");
    assert!(matches!(interrupted, Err(LimboError::Interrupt)), "CLAIM: an interrupted CHECKPOINT did not report the interrupt: {interrupted:?}");
    // New records since the held checkpoint's cut, so a compaction has something to cut.
    for _ in 0..8 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let log0 = db.branches.log_len_for_test();
    let unheld = db.branch_compact_now_within(std::time::Instant::now() + std::time::Duration::from_secs(10), &|| false);
    assert!(unheld.is_ok(), "CLAIM (positive control): with no flight held, a bounded CHECKPOINT failed: {unheld:?}");
    assert!(db.branches.log_len_for_test() < log0, "CLAIM (positive control): the bounded CHECKPOINT compacted nothing (log {log0} -> {})", db.branches.log_len_for_test());
}

/// Review C42 MED-2 (lead ruling "C42-M1 review, bounded end to end"): a client's CHECKPOINT is
/// bounded END TO END, its wait for the store mutex too, not only its wait for a flight. Another
/// thread holds the store mutex (as a background compaction's sync or a sharp compact would);
/// `branch_compact_now_within` must return `Busy` at its 300 ms deadline (not before it), and
/// `Interrupt` once its interrupt says so, each within 30 s. At 7c698b9b5 the mutex was taken with
/// no bound after the bounded join.
#[test]
fn a_client_checkpoint_s_wait_for_the_store_mutex_ends_at_its_deadline_or_its_interrupt() {
    use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-mutex-within.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _ = trunk.fork_branch().unwrap().into_id();
    let (held, release) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    // Named apart from `Ordering::Release`, which this test imports (review c42b HIGH-2: E0255).
    struct ReleaseOnDrop(Arc<AtomicBool>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let _release = ReleaseOnDrop(release.clone());
    let holder = {
        let (db, held, release) = (db.clone(), held.clone(), release.clone());
        std::thread::spawn(move || db.branches.hold_store_mutex_for_test(&held, &release))
    };
    let t = std::time::Instant::now();
    while !held.load(Acquire) {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the holder never took the store mutex");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let call = |until: std::time::Duration, interrupt_after: Option<std::time::Duration>| {
        let (tx, rx) = std::sync::mpsc::channel();
        let db = db.clone();
        std::thread::spawn(move || {
            let begun = std::time::Instant::now();
            let interrupted = move || interrupt_after.is_some_and(|d| begun.elapsed() >= d);
            let r = db.branch_compact_now_within(begun + until, &interrupted);
            let _ = tx.send((r, begun.elapsed()));
        });
        rx.recv_timeout(std::time::Duration::from_secs(30))
    };
    let at_deadline = call(std::time::Duration::from_millis(300), None);
    let interrupted = call(std::time::Duration::from_secs(600), Some(std::time::Duration::from_millis(100)));
    release.store(true, Release);
    holder.join().unwrap();
    let (r, took) = at_deadline.expect("CLAIM: a client's CHECKPOINT waited for the store mutex past its 300 ms deadline (no return in 30 s)");
    assert!(matches!(r, Err(LimboError::Busy)), "CLAIM: at its deadline the CHECKPOINT did not report Busy: {r:?}");
    assert!(took >= std::time::Duration::from_millis(300), "CLAIM: the CHECKPOINT gave up before its deadline ({took:?})");
    let (r, _) = interrupted.expect("CLAIM: a client's CHECKPOINT went on waiting for the store mutex after its interrupt (no return in 30 s)");
    assert!(matches!(r, Err(LimboError::Interrupt)), "CLAIM: an interrupted CHECKPOINT did not report the interrupt: {r:?}");
}

/// Review c42b MED-2: BOTH client checkpoints are bounded end to end. `checkpoint_now(deadline)` (the
/// wire's CHECKPOINT on a catalog store) took the store mutex unbounded, at its first line and in
/// `checkpoint_fuzzy_now`. Another thread holds the mutex; with a 300 ms deadline it must answer
/// `Ok(false)` (no install before the deadline) within 30 s, not before the deadline.
#[test]
fn checkpoint_now_s_wait_for_the_store_mutex_ends_at_its_deadline() {
    use std::sync::atomic::{AtomicBool, Ordering::Acquire};
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-now-mutex.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _ = trunk.fork_branch().unwrap().into_id();
    let (held, release) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    struct ReleaseOnDrop(Arc<AtomicBool>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    let _release = ReleaseOnDrop(release.clone());
    let holder = {
        let (db, held, release) = (db.clone(), held.clone(), release.clone());
        std::thread::spawn(move || db.branches.hold_store_mutex_for_test(&held, &release))
    };
    let t = std::time::Instant::now();
    while !held.load(Acquire) {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the holder never took the store mutex");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let db = db.clone();
        std::thread::spawn(move || {
            let begun = std::time::Instant::now();
            let r = db.branch_checkpoint_now(std::time::Duration::from_millis(300));
            let _ = tx.send((r, begun.elapsed()));
        });
    }
    let answered = rx.recv_timeout(std::time::Duration::from_secs(30));
    release.store(true, std::sync::atomic::Ordering::Release);
    holder.join().unwrap();
    let (r, took) = answered.expect("CLAIM: checkpoint_now waited for the store mutex past its 300 ms deadline (no answer in 30 s)");
    assert!(took >= std::time::Duration::from_millis(300), "CLAIM: checkpoint_now gave up before its deadline ({took:?})");
    assert!(matches!(r, Ok(false)), "CLAIM: checkpoint_now held off by the mutex past its deadline answered {r:?}, not Ok(false)");
}

/// `branch_checkpoint_now(deadline)` on its own thread while the flight it starts is held at
/// `stage`, released after `release_after`; its answer and how long it took (`None`: no answer in
/// 30 s). Premise (review c42b LOW-6): the held flight is `checkpoint_now`'s own. Every flight the
/// forks started is waited for first, so the fuzzy-flights-started counter moves by exactly one
/// by the time the hold is reached; a `maybe_compact` flight in the air would have sent
/// `checkpoint_now` down its other branch, and a revert of the own-flight join would survive.
fn checkpoint_now_with_its_flight_held(
    db: &Arc<Database>,
    stage: u8,
    deadline: std::time::Duration,
    release_after: std::time::Duration,
) -> Option<(crate::Result<bool>, std::time::Duration)> {
    let trunk = db.connect().unwrap();
    for _ in 0..8 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    db.branch_checkpoint_wait();
    let started0 = db.branch_checkpoint_counters()[1];
    db.branch_checkpoint_hold(stage);
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let db = db.clone();
        std::thread::spawn(move || {
            let begun = std::time::Instant::now();
            let r = db.branch_checkpoint_now(deadline);
            let _ = tx.send((r, begun.elapsed()));
        });
    }
    let t = std::time::Instant::now();
    while db.branch_checkpoint_held() != stage | super::store::HOLD_ARRIVED {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "premise: the checkpoint_now flight never reached hold {stage}");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let started = db.branch_checkpoint_counters()[1] - started0;
    if started != 1 {
        db.branch_checkpoint_hold(0);
        panic!("premise: {started} fuzzy flights started after the settle, not exactly checkpoint_now's own");
    }
    std::thread::sleep(release_after.saturating_sub(t.elapsed()));
    db.branch_checkpoint_hold(0);
    let r = rx.recv_timeout(std::time::Duration::from_secs(30)).ok();
    db.branch_checkpoint_wait();
    r
}

/// Review c42b LOW-5 (C42 LOW-1 was fixed with no red): `checkpoint_now` takes any deadline, so a
/// client asking to wait without bound (`Duration::MAX`) gets its checkpoint (`Ok(true)`) rather
/// than an `Instant` overflow. Answered on its own thread within 30 s. Expected red at
/// 5eca7a09a^ (`started + deadline` panicked: the thread dies and no answer comes).
#[test]
fn checkpoint_now_with_no_bound_on_its_wait_installs() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-now-max.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for _ in 0..8 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    db.branch_checkpoint_wait();
    let installed0 = db.branch_checkpoint_counters()[0];
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let db = db.clone();
        std::thread::spawn(move || {
            let _ = tx.send(db.branch_checkpoint_now(std::time::Duration::MAX));
        });
    }
    let r = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("CLAIM: checkpoint_now(Duration::MAX) gave no answer in 30 s (its thread died or hung)");
    assert!(matches!(r, Ok(true)), "CLAIM: checkpoint_now(Duration::MAX) on a fuzzy store answered {r:?}, not Ok(true)");
    assert!(db.branch_checkpoint_counters()[0] > installed0, "CLAIM: Ok(true) with no checkpoint installed");
}

/// Review C42 MED-3: `checkpoint_now` whose own flight INSTALLED but whose WAL truncation ran past
/// the deadline reports the install (`Ok(true)`), as its doc says (`Ok(false)`: none did before
/// the deadline). The flight is held at `HOLD_BEFORE_WAL_TRUNCATE`, after its install, past a
/// 300 ms deadline. At b3fad32f8 the bounded join's Busy mapped to `Ok(false)`.
#[test]
fn checkpoint_now_reports_an_install_whose_truncation_ran_past_its_deadline() {
    let _s = serial();
    // The truncation's hold is reached only when the catalog WAL is due (review c42b HIGH-3: 50 rows
    // and 8 forks never reach 4 MiB, so the red failed at its premise at its base and its fix).
    let _always = CatalogWalTruncateAlways::set();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-now-installed.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    seed(&db.connect().unwrap());
    let installed0 = db.branch_checkpoint_counters()[0];
    let (r, took) = checkpoint_now_with_its_flight_held(
        &db,
        super::store::HOLD_BEFORE_WAL_TRUNCATE,
        std::time::Duration::from_millis(300),
        std::time::Duration::from_secs(1),
    )
    .expect("premise: checkpoint_now answered within 30 s");
    assert!(db.branch_checkpoint_counters()[0] > installed0, "premise: the flight installed");
    assert!(took >= std::time::Duration::from_millis(300), "premise: the answer came after the deadline ({took:?})");
    assert!(matches!(r, Ok(true)), "CLAIM: checkpoint_now reported {r:?} for a checkpoint that installed before its deadline ran out");
}

/// Review C42 MED-5: `checkpoint_now`'s join of the flight IT started is bounded by its deadline
/// (C42-M1 bounded it; nothing tested it, so reverting to the unbounded join survived). The flight
/// is held before its commit (`HOLD_BEFORE_COMMIT`), so nothing installs; with a 300 ms deadline
/// `checkpoint_now` must answer `Ok(false)` within 30 s, the hold released only after 5 s.
#[test]
fn checkpoint_now_s_join_of_its_own_flight_ends_at_its_deadline() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("checkpoint-now-bounded.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    seed(&db.connect().unwrap());
    let (r, took) = checkpoint_now_with_its_flight_held(
        &db,
        super::store::HOLD_BEFORE_COMMIT,
        std::time::Duration::from_millis(300),
        std::time::Duration::from_secs(5),
    )
    .expect("premise: checkpoint_now answered within 30 s of its flight's release");
    assert!(took < std::time::Duration::from_secs(5), "CLAIM: checkpoint_now waited for its own flight past its 300 ms deadline ({took:?}: the hold was released at 5 s)");
    assert!(matches!(r, Ok(false)), "CLAIM: checkpoint_now with no install before its deadline answered {r:?}");
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
/// commit is refused). Only a barrier the file system does not support (ENOTSUP, EOPNOTSUPP,
/// EINVAL, ENOTTY) falls back to the full sync, and the fallback is counted.
///
/// FLAGGED TEST EDIT (own test, review 3 #4; review 6 #6): the fallback is right only on a kernel
/// below Darwin 23, which does not promote an unsupported barrier itself; this test now forces
/// that kernel (its new-kernel twin is a_failed_barrier_on_a_kernel_that_promotes_it_fail_stops_
/// whatever_its_errno), and gains the EOPNOTSUPP, EINVAL and ENOTTY arms the review asked for.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_barrier_fail_stops_and_only_an_unsupported_one_falls_back() {
    let _s = serial();
    let _k = DarwinMajor::force(22);
    for catalog in [false, true] {
        for (errno, name) in [
            (libc::EIO, "EIO"),
            (libc::ENOTSUP, "ENOTSUP"),
            (libc::EOPNOTSUPP, "EOPNOTSUPP"),
            (libc::EINVAL, "EINVAL"),
            (libc::ENOTTY, "ENOTTY"),
        ] {
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
            assert_eq!(
                super::journal::barrier_errno_pending(),
                0,
                "catalog={catalog} {name}: premise: the commit took the armed barrier"
            );
            let fallbacks = after.barrier_fallback - before.barrier_fallback;
            if errno != libc::EIO {
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
/// checkpoint fail-stops the store: a later sync of the same file may report success for pages the
/// failed one lost, so nothing more may be acknowledged. The next branch commit is refused, and a
/// branch released afterwards frees nothing. Mutant `checkpoint_sync_error_kept` (the checkpoint's
/// fail-stop left out) must fail the catalog arm.
///
/// FLAGGED TEST EDIT (own test, review 3 #5; review 6 #3 (b)): the catalog arm was a D1 fuzzy
/// checkpoint captured with a flight in the air. Review 6 #3 (b) takes that checkpoint's own arena
/// sync away (its wait for the group's flights covers its slots), so the arm would fail no sync. The
/// one checkpoint that still syncs the arena itself is a sharp one over writes no flight synced: a
/// D0 store whose log was raised, here by a trunk commit under a synchronous trunk.
#[test]
fn an_arena_sync_failure_in_a_compaction_or_checkpoint_fail_stops() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let class = if catalog { SyncClass::Off } else { SyncClass::Fsync };
        let db = open_at(&dir.path().join("arena-sync-fails.db"), opts(catalog, class));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 3, "x");
        let owned = x.owned_slots();
        if catalog {
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("UPDATE t SET v = 'trunk-new' WHERE id = 9").unwrap();
            assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the D0 log");
            // Another branch's write, so x's slots stay as they were.
            let z = trunk.fork_branch().unwrap();
            write_v(&z.connect().unwrap(), 5, "unsynced");
            let _ = z.into_id();
            assert!(db.branches.arena_dirty(), "premise: the D0 commit left its slot unsynced");
            db.branch_failpoint(Some(BranchFailpoint::ArenaSyncFails));
            assert!(db.branch_compact_now().is_err(), "premise: the sharp checkpoint's arena sync failed");
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
///
/// RE-SPECIFIED (lead order 2026-10-15, review REVIEW-claude-namefilter-2 LOW; a flagged edit of a
/// red's instrument, not of its claim): the filter's growth moved off the mutex, and `moved_max`
/// became the entries one growth copies on its own thread, which grows with n by design, so the
/// first bound read a number the claim is not about. The claim, "never stalls one create with a
/// rehash", is now read where it lives: the most entries one note moved inside its store-mutex hold
/// (`name_filter_held_moves_for_test`), within the same n/256 bound (0 by construction; mutant
/// `name_filter_grows_in_note` moves them all). The bound on entries is unchanged.
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
    let (built, entries, _, _, _) = db.branch_name_filter_stats();
    let moved = db.branches.name_filter_held_moves_for_test();
    assert!(built, "premise: the name filter is built");
    assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "premise (review m1-2c N5): built means Ready, not Stale or an overflow's answer");
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
    assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "premise (review m1-2c N5): built means Ready, not Stale or an overflow's answer");
    assert!(db.connect().unwrap().create_branch("held").is_err(), "a held name was given again");
}

/// Review REVIEW-claude-namefilter-2 MED-1: a close joins EVERY name-filter thread. The handle was
/// one slot, so a rebuild started while the open's build was held overwrote that build's handle,
/// and close returned with the build still alive, holding a catalog connection and the store's
/// files. Here the open's build is held before its scan (`NAME_SCAN_HOLD`), creates fill its
/// pending table until the filter overflows and a rebuild starts, and the held build is released
/// 300 ms after the close begins. When the close returns, no filter thread may be alive
/// (`FILTER_THREADS_LIVE`, counted from each thread's first statement to its last).
#[test]
fn a_close_joins_every_name_filter_thread() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("filter-join.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("held").unwrap();
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    struct Release;
    impl Drop for Release {
        fn drop(&mut self) {
            super::store::NAME_SCAN_HOLD.store(0, std::sync::atomic::Ordering::Release);
        }
    }
    let _r = Release;
    let live0 = super::store::FILTER_THREADS_LIVE.load(O::Acquire);
    super::store::NAME_SCAN_HOLD.store(1, O::Release);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    let t = std::time::Instant::now();
    while super::store::NAME_SCAN_HOLD.load(O::Acquire) != 1 | super::store::HOLD_ARRIVED {
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: the open's build never reached its scan");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let trunk = db.connect().unwrap();
    let mut i = 0u32;
    while db.branches.name_filter_state_for_test().1 == 0 {
        assert!(i < 200_000, "premise: {i} creates never overflowed the held build's pending table");
        trunk.create_branch(&format!("fill-{i}")).unwrap();
        i += 1;
    }
    // The rebuild's thread, started by the create that took the owed rebuild, runs to its end;
    // the held build stays alive until the release below.
    let t = std::time::Instant::now();
    while super::store::FILTER_THREADS_LIVE.load(O::Acquire) > live0 + 1 {
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: the rebuild did not finish");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        super::store::FILTER_THREADS_LIVE.load(O::Acquire),
        live0 + 1,
        "premise: the open's held build is not alive"
    );
    let releaser = std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(300));
        super::store::NAME_SCAN_HOLD.store(0, std::sync::atomic::Ordering::Release);
    });
    drop(trunk);
    let store = Arc::downgrade(&db.branches);
    drop(db);
    assert!(store.upgrade().is_none(), "premise: the store was not closed (a reference outlived the database)");
    let live = super::store::FILTER_THREADS_LIVE.load(O::Acquire);
    releaser.join().unwrap();
    assert_eq!(live, live0, "CLAIM: {} name-filter thread(s) outlived the store's close", live - live0);
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
        assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "{arm}: premise (review m1-2c N5): built means Ready");
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

/// Review delete MED-1: a DROP of a name whose Release is still in the air does not answer
/// NoSuchBranch before that Release is durable (after a crash the branch would come back under the
/// name), as a lookup does not (`a_lookup_by_name_waits_for_the_release_it_reports`). 838c41999
/// copied the lookup's wait into `drop_named`, where neither lookup mutant reached it. Mutants
/// `drop_miss_no_release_wait` and (once the lookup is shared) `lookup_no_release_wait`.
#[test]
fn a_drop_miss_waits_for_its_own_names_release() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("drop-miss-wait.db"), opts(catalog, SyncClass::Fsync));
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
        let second = {
            let db = db.clone();
            std::thread::spawn(move || db.drop_branch("x").map(|_| ()))
        };
        let blocked = blocked_in_wait_durable(&db, before, &second);
        hold.store(0, std::sync::atomic::Ordering::Release);
        dropper.join().unwrap().unwrap();
        let missed = second.join().unwrap();
        assert!(blocked, "catalog={catalog}: a drop answered for a name whose Release was not yet durable");
        assert!(
            matches!(missed, Err(LimboError::NoSuchBranch(ref n)) if n == "x"),
            "catalog={catalog}: the second drop, after the release, is NoSuchBranch: {missed:?}"
        );
    }
}

/// Review delete LOW-6: a drop of a branch whose fork never became durable (its create's flight
/// failed) does not say the branch "comes back at the next open": the reopen will not have that
/// fork. Since 838c41999 the drop runs the release without settling the fork first, so the release's
/// failure message is reached for such a branch.
#[test]
fn a_drop_of_a_branch_whose_fork_failed_does_not_promise_it_comes_back() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("drop-failed-fork.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        assert!(trunk.create_branch("x").is_err(), "catalog={catalog}: premise: the create's flight failed");
        let dropped = db.drop_branch("x");
        // The drop reached the release (a NoSuchBranch would pass the claim without testing it).
        assert!(
            matches!(dropped, Err(LimboError::InternalError(ref m)) if m.contains("not released durably")),
            "catalog={catalog}: premise: the drop of x reached the release and failed there: {dropped:?}"
        );
        assert!(
            !dropped.as_ref().is_err_and(|e| e.to_string().contains("comes back")),
            "catalog={catalog}: CLAIM: the drop of a branch whose fork never became durable said it comes back: {dropped:?}"
        );
    }
}

/// fastest-wire (the typed branch errors): `Database::drop_branch` released a named branch that
/// still had an open connection, as a handle's release does (the branch kept whole until the
/// connection closes). A server's DROP of a database in use must be refused instead, as
/// PostgreSQL refuses it: refused while the connection is open, dropped once it has closed.
/// Mutant `drop_while_open`.
#[test]
fn a_named_branch_with_an_open_connection_is_not_dropped() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("drop-in-use.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("busy").unwrap();
        let c = db.connect_named("busy").unwrap();
        match db.drop_branch("busy") {
            Ok(_) => panic!("catalog={catalog}: a branch with an open connection was dropped"),
            Err(e) => assert!(
                e.to_string().contains("already has an open connection"),
                "catalog={catalog}: refused for another reason: {e}"
            ),
        }
        assert!(db.branch_named("busy").unwrap().is_some(), "catalog={catalog}: the refused drop released the branch");
        drop(c);
        db.drop_branch("busy").unwrap_or_else(|e| panic!("catalog={catalog}: a branch whose connection closed was not dropped: {e}"));
        assert_eq!(db.branch_named("busy").unwrap(), None, "catalog={catalog}: after the drop");
    }
}

/// Engine review 14 MED 7: nothing showed that `Database::drop_branch`'s open check and its
/// release are one hold of the store mutex. Under a regression to check-then-act across two holds,
/// a drop and a concurrent `connect_named` would both succeed and the Release would be written
/// under a live connection. A drop parked at `HOLD_DROP_LOOKED_UP` while a `connect_named` runs on
/// this thread: the connect wins, and the drop is then refused as `BranchInUse`; the two never both
/// succeed. Since 838c41999 the lookup and the release are ONE hold and the park is before it, so on
/// the default arm this is a sequential connect-then-drop (its subject, the window, is gone; review
/// delete LOW-4); it still kills mutant `drop_check_separate_hold`, which reopens the window.
#[test]
fn a_drop_and_a_connect_never_both_win_a_named_branch() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("drop-race.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("race").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_DROP_LOOKED_UP, std::sync::atomic::Ordering::Release);
        let dropper = {
            let db = db.clone();
            std::thread::spawn(move || db.drop_branch("race").map(|_| ()))
        };
        wait_hold(&hold, super::store::HOLD_DROP_LOOKED_UP);
        let conn = db.connect_named("race");
        hold.store(0, std::sync::atomic::Ordering::Release);
        let dropped = dropper.join().unwrap();
        assert!(
            !(conn.is_ok() && dropped.is_ok()),
            "catalog={catalog}: the drop and the connect both succeeded: a Release under a live connection"
        );
        assert!(
            conn.is_ok(),
            "catalog={catalog}: premise: the connect, made while the drop waited before its release, succeeds: {:?}",
            conn.as_ref().err()
        );
        assert!(
            matches!(
                dropped,
                Err(LimboError::BranchInUse { ref name, op: crate::BranchOp::Drop, .. })
                    if name.as_deref() == Some("race")
            ),
            "catalog={catalog}: the drop after the connect was not refused as BranchInUse: {dropped:?}"
        );
        assert!(
            db.branch_named("race").unwrap().is_some(),
            "catalog={catalog}: the refused drop released the branch"
        );
    }
}

/// Engine review 14 MED 9: `connect_named` promises `NoSuchBranch` when no unreleased branch has
/// the name, but a drop that lands between its lookup and its open made the open fail untyped
/// (`InternalError("... does not exist")` or `InvalidArgument("... has been reaped")`), which a
/// server mapping by variant answers XX000. A connect parked between its lookup and its open
/// (`HOLD_CONNECT_LOOKED_UP`) while the drop completes: the connect is refused as
/// `NoSuchBranch("gone")`. Mutant `connect_race_untyped`.
#[test]
fn a_connect_racing_a_drop_is_refused_as_no_such_branch() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("connect-race.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("gone").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_CONNECT_LOOKED_UP, std::sync::atomic::Ordering::Release);
        let connector = {
            let db = db.clone();
            std::thread::spawn(move || db.connect_named("gone").map(|_| ()))
        };
        wait_hold(&hold, super::store::HOLD_CONNECT_LOOKED_UP);
        db.drop_branch("gone").unwrap_or_else(|e| {
            panic!("catalog={catalog}: premise: the drop, made while the connect waited, succeeds: {e}")
        });
        hold.store(0, std::sync::atomic::Ordering::Release);
        let connected = connector.join().unwrap();
        assert!(
            matches!(connected, Err(LimboError::NoSuchBranch(ref n)) if n == "gone"),
            "catalog={catalog}: a connect that lost the race to a drop was not refused as NoSuchBranch: {connected:?}"
        );
    }
}

/// Engine review 14 LOW 12: `BranchInUse` gave a DROP the connect's reason ("a branch serves one
/// connection at a time"), and 620e69a81's claim that the old texts were kept was false: the
/// second connection's refusal lost why ("a second one's page cache would silently miss the first
/// one's commits"). Each refusal gives its own reason, with the branch named in the message.
/// Mutant `drop_refused_as_connect`.
#[test]
fn a_drop_in_use_is_refused_in_its_own_words() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("in-use-words.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("a").unwrap();
        let c = db.connect_named("a").unwrap();
        let connect = match db.connect_named("a") {
            Ok(_) => panic!("catalog={catalog}: premise: a second connection is refused"),
            Err(e) => e.to_string(),
        };
        let dropped = match db.drop_branch("a") {
            Ok(_) => panic!("catalog={catalog}: premise: a drop while connected is refused"),
            Err(e) => e.to_string(),
        };
        assert!(
            connect.contains("\"a\"") && connect.contains("page cache would silently miss"),
            "catalog={catalog}: the second connection's refusal lost the branch or its reason: {connect}"
        );
        assert!(
            dropped.contains("\"a\"") && dropped.contains("cannot be dropped"),
            "catalog={catalog}: the drop's refusal does not say why a drop is refused: {dropped}"
        );
        assert!(
            !dropped.contains("serves one connection at a time"),
            "catalog={catalog}: the drop was refused with the connect's reason: {dropped}"
        );
        drop(c);
    }
}

/// fastest-wire: the named-branch refusals are typed, so a server answers each with its own code
/// without matching message text: `NameTaken` from `create_branch`, `NoSuchBranch` from
/// `connect_named` and `drop_branch`, `BranchInUse` (the name as given, and the refused
/// operation: engine review 14 LOW 12) from a second `connect_named` and from `drop_branch` while
/// connected.
#[test]
fn named_branch_refusals_are_typed() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("typed.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("a").unwrap();
        assert!(
            matches!(trunk.create_branch("a"), Err(LimboError::NameTaken(ref n)) if n == "a"),
            "catalog={catalog}: a taken name was not refused as NameTaken"
        );
        assert!(
            matches!(db.connect_named("nope"), Err(LimboError::NoSuchBranch(ref n)) if n == "nope"),
            "catalog={catalog}: connect_named of no branch was not refused as NoSuchBranch"
        );
        assert!(
            matches!(db.drop_branch("nope"), Err(LimboError::NoSuchBranch(ref n)) if n == "nope"),
            "catalog={catalog}: drop_branch of no branch was not refused as NoSuchBranch"
        );
        let c = db.connect_named("a").unwrap();
        assert!(
            matches!(
                db.connect_named("a"),
                Err(LimboError::BranchInUse { ref name, op: crate::BranchOp::Connect, .. })
                    if name.as_deref() == Some("a")
            ),
            "catalog={catalog}: a second connection was not refused as BranchInUse"
        );
        assert!(
            matches!(
                db.drop_branch("a"),
                Err(LimboError::BranchInUse { ref name, op: crate::BranchOp::Drop, .. })
                    if name.as_deref() == Some("a")
            ),
            "catalog={catalog}: a drop while connected was not refused as BranchInUse"
        );
        drop(c);
    }
}

/// Engine review 7 #11: a lookup miss was not "one compare". After the first Release in a process,
/// every miss in `branch_named` (and so in `connect_named` and `drop_branch`) took the group's lock
/// and counted a wait, and waited out whatever Release was in the air, another name's too, or led
/// a flight itself. A miss waits only for a Release of its own name still in the air, and with
/// nothing of the kind takes no lock. Mutant `lookup_waits_any_release`.
#[test]
fn a_lookup_miss_waits_only_for_its_own_names_release() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("miss-scope.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("gone").unwrap();
        db.drop_branch("gone").unwrap();
        // Budget: with no Release in the air, misses count no group wait.
        let waits = db.branches.group_counters()[2];
        for _ in 0..100 {
            assert_eq!(db.branch_named("nobody").unwrap(), None, "catalog={catalog}: premise: a miss");
        }
        assert_eq!(
            db.branches.group_counters()[2] - waits,
            0,
            "catalog={catalog}: 100 lookup misses with no Release in the air counted group waits"
        );
        // A miss of a fresh name while another name's Release is in the air returns at once.
        trunk.create_branch("busy").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let dropper = {
            let db = db.clone();
            std::thread::spawn(move || db.drop_branch("busy").map(|_| ()))
        };
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let (tx, rx) = std::sync::mpsc::channel();
        let finder = {
            let db = db.clone();
            std::thread::spawn(move || {
                let _ = tx.send(db.branch_named("fresh").map_err(|e| e.to_string()));
            })
        };
        let found = rx.recv_timeout(std::time::Duration::from_secs(10));
        hold.store(0, std::sync::atomic::Ordering::Release);
        dropper.join().unwrap().unwrap();
        finder.join().unwrap();
        assert_eq!(
            found,
            Ok(Ok(None)),
            "catalog={catalog}: a miss of a fresh name waited for another name's Release in the air (or failed)"
        );
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
///
/// FLAGGED TEST EDIT (engine review 7 #13): `CUT_PANICS` is process-wide, so the test runs alone in
/// a fresh process (`fork_driver::alone`), where no neighbour can consume the hook or be hit by it,
/// and a Drop guard disarms it whatever happens.
///
/// FLAGGED TEST EDIT (engine review 7 #13's judge): no longer `cfg(unix)`; `fork_driver::alone`
/// needs only `std::process`, so the test runs on every target.
#[test]
fn a_panic_in_a_fuzzy_checkpoints_cut_does_not_stop_checkpoints() {
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            super::store::CUT_PANICS.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let Some(sentinel) =
        super::fork_driver::alone("branch::fastest_tests::a_panic_in_a_fuzzy_checkpoints_cut_does_not_stop_checkpoints")
    else {
        return;
    };
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("cut-panic.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _a = trunk.fork_branch().unwrap().into_id();
    let _disarm = Disarm;
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
    super::fork_driver::finished(&sentinel);
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

// ---- engine review 8 #5-#8: every D0 free in a raised log waits for a sync, and none is lost ----

/// A D0 store whose trunk is synchronous (engine review 8 #5-#8, after review 5 #18): a child x,
/// with its own commit to row 40 first when `x_writes` (its slot S), and a trunk commit to row 3
/// that keeps x a pre-image (slot T) in a raised, synced flight. Returns the database, its trunk,
/// x, and the slots x's release will free (T, and S when x wrote).
fn raised_d0_with_a_kept_pre_image(path: &Path, catalog: bool, x_writes: bool) -> (Arc<Database>, Arc<Connection>, Branch, Vec<u32>) {
    let db = open_at(path, opts(catalog, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    let x = trunk.fork_branch().unwrap();
    let mut slots = Vec::new();
    if x_writes {
        write_v(&x.connect().unwrap(), 40, "x");
        slots = x.owned_slots();
        assert_eq!(slots.len(), 1, "catalog={catalog}: premise: x's commit took one slot");
    }
    let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
    write_v(&trunk, 3, "new");
    let kept: Vec<u32> = db.branch_slots_in_use().into_iter().filter(|s| !before.contains(s)).collect();
    assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the trunk commit kept one pre-image");
    assert!(db.branches.rewrite_class_for_test().syncs(), "catalog={catalog}: premise: the trunk commit raised the log");
    slots.extend(kept);
    (db, trunk, x, slots)
}

/// The slots of `slots` a new branch's write takes (freed slots are handed out last in, first out).
fn reused_by_a_new_branch(trunk: &Arc<Connection>, slots: &[u32]) -> Vec<u32> {
    let y = trunk.fork_branch().unwrap();
    let before: std::collections::HashSet<u32> = y.owned_slots().into_iter().collect();
    write_v(&y.connect().unwrap(), 50, "y");
    let took: Vec<u32> = y.owned_slots().into_iter().filter(|s| !before.contains(s) && slots.contains(s)).collect();
    let _ = y.into_id();
    took
}

/// Engine review 8 #5 (i): a raised flight's sync makes the WHOLE log durable, so an earlier D0
/// commit's slot S is named by a synced record too, yet only the raised take's own slots were
/// marked: S went back to the allocator at x's unsynced Release. Neither S nor T may be reused
/// before a sync covers that Release. Mutant `d0_frees_at_written`.
#[test]
fn a_d0_release_in_a_raised_log_holds_every_slot_until_a_sync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("hold-all.db"), catalog, true);
        x.reap().unwrap();
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(took.is_empty(), "catalog={catalog}: slots {took:?} were reused before a sync covered the Release that freed them");
        drop(db);
    }
}

/// Engine review 8 #5 (iv): the marks of synced records started empty at every open, so a slot a
/// durable record named before a close was freed at once after it. A reopened raised log still
/// holds the frees. Mutant `d0_frees_at_written`.
#[test]
fn a_d0_release_in_a_raised_log_holds_its_slots_after_a_reopen() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("hold-reopen.db");
        let (x_id, slots, incarnation) = {
            let (db, _trunk, x, slots) = raised_d0_with_a_kept_pre_image(&path, catalog, false);
            (x.into_id(), slots, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        assert!(db.branches.rewrite_class_for_test().syncs(), "catalog={catalog}: premise: the reopened log is raised");
        let trunk = db.connect().unwrap();
        db.branch(x_id).unwrap().reap().unwrap();
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(took.is_empty(), "catalog={catalog}: slots {took:?} were reused after a reopen before a sync covered the Release");
    }
}

/// Engine review 8 #6 (and #5 (iii)): a compaction or checkpoint syncs its rewrite in the log's
/// rewrite class, which makes every Release it covers durable, yet it reported durability in the
/// store's class (Off in D0), so a held free never matured on it. After x's release and a sharp
/// checkpoint or compaction, x's slot is free. Mutant `rewrite_marks_store_class`.
#[test]
fn a_rewrite_of_a_raised_d0_log_matures_the_frees_it_covers() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("rewrite-matures.db"), catalog, false);
        x.reap().unwrap();
        db.branch_compact_now().unwrap();
        for &slot in &slots {
            assert!(db.branch_slot_is_free(slot), "catalog={catalog}: slot {slot} is still held after a rewrite synced its Release");
        }
    }
}

/// Engine review 13 MED 4: a D0 open of a log an earlier D1 or D2 run synced (never raised in its
/// header) freed at written once its first rewrite had landed: the class recovery decided was
/// not kept, so the free class was the store's (Off). But the earlier run's records naming x's
/// slots are durable (now in the rewrite's snapshot or catalog), and a D0 Release a power cut
/// loses brings x back over a reused slot. The free is held until a sync covers its Release.
/// Mutant `free_class_ignores_synced_open`.
#[test]
fn a_d0_open_of_a_synced_log_holds_frees_past_its_first_rewrite() {
    let _s = serial();
    // FLAGGED TEST EDIT (engine review 18 LOW 9; for the lead's ruling): each arm's claim failure is
    // collected and all are asserted at the end, so a run shows which arms are red, where the first
    // panic hid the rest. Premises still stop the test. No claim or expected value changed.
    let mut failed = Vec::new();
    // Lead ruling 46628e056e on LOW 9: a planted failure in EACH arm must still fail the test,
    // planted by FE_MUTANT=low9_plant_arm<i> (test builds only: a fake reused slot in arm i alone).
    let arms = [(false, SyncClass::Fsync), (true, SyncClass::Fsync), (false, SyncClass::FullFsync), (true, SyncClass::FullFsync)];
    for (arm, (catalog, written)) in arms.into_iter().enumerate() {
        let what = format!("catalog={catalog} written={written:?}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("synced-open.db");
        let (id, slots, incarnation) = {
            let db = open_at(&path, opts(catalog, written));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            let slots = x.owned_slots();
            assert!(!slots.is_empty(), "{what}: premise: x's write took a slot");
            (x.into_id(), slots, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        db.branch_compact_now().unwrap();
        // FLAGGED TEST EDIT (engine review 18 MED 2): the premise was that the first rewrite cleared the
        // class; now the class outlives it (written into the header), which this premise states.
        assert!(db.branches.rewrite_class_for_test().syncs(), "{what}: premise: the synced class outlives the first rewrite");
        let trunk = db.connect().unwrap();
        db.branch(id).unwrap().reap().unwrap();
        let mut took = reused_by_a_new_branch(&trunk, &slots);
        if super::store::fe_mutant(&format!("low9_plant_arm{arm}")) {
            took.push(u32::MAX);
        }
        if !took.is_empty() {
            failed.push(format!("{what}: slots {took:?}"));
        }
    }
    assert!(
        failed.is_empty(),
        "slots were reused before a sync covered the Release, over records an earlier run made durable: {failed:?}"
    );
}

/// Engine review 18 MED 2 (a): the class an earlier run synced the log in (`synced_base`) lived only
/// in the memory of the process that opened over it. A second D0 process, opening the log the first
/// D0 process compacted, found no synced flight and no raised header, so it freed at written while
/// the snapshot or catalog the first one synced still named the slots. Three runs: D1 writes x; D0
/// opens, compacts and closes; D0 opens and reaps x: a new branch reuses none of x's slots. Both
/// stores.
#[test]
fn a_second_d0_process_over_a_synced_log_still_holds_frees() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("second-d0.db");
        let (id, slots, first) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            let slots = x.owned_slots();
            assert!(!slots.is_empty(), "{what}: premise: x's write took a slot");
            (x.into_id(), slots, db.incarnation)
        };
        let second = {
            let db = reopen(&path, opts(catalog, SyncClass::Off), first);
            db.branch_compact_now().unwrap();
            db.incarnation
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), second);
        let trunk = db.connect().unwrap();
        db.branch(id).unwrap().reap().unwrap();
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(
            took.is_empty(),
            "{what}: slots {took:?} were reused before a sync covered the Release, over records a D1 run made durable two processes ago"
        );
    }
}

/// Engine review 18 MED 2 (a), the header-only arm: a D1 or D2 run whose last write came before its
/// checkpoint leaves a log holding no flight, so a D0 open decided no synced class from any tag,
/// and the header said nothing either: it freed at written while the synced catalog or snapshot
/// still named the slots. D1 or D2 writes x, compacts and closes (premise: the reopened log holds no
/// flight); D0 reopens and reaps x: a new branch reuses none of x's slots. Both stores, both classes.
#[test]
fn a_d0_open_of_a_header_only_synced_log_holds_frees() {
    let _s = serial();
    for (catalog, written) in [(false, SyncClass::Fsync), (true, SyncClass::Fsync), (false, SyncClass::FullFsync), (true, SyncClass::FullFsync)] {
        let what = format!("catalog={catalog} written={written:?}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("header-only.db");
        let (id, slots, incarnation) = {
            let db = open_at(&path, opts(catalog, written));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            let slots = x.owned_slots();
            assert!(!slots.is_empty(), "{what}: premise: x's write took a slot");
            db.branch_compact_now().unwrap();
            (x.into_id(), slots, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        assert_eq!(db.branches.log_len_for_test(), 40, "{what}: premise: the reopened log holds no flight");
        let trunk = db.connect().unwrap();
        db.branch(id).unwrap().reap().unwrap();
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(
            took.is_empty(),
            "{what}: slots {took:?} were reused before a sync covered the Release, over a log a {written:?} run synced"
        );
    }
}

/// Engine review 8 #7: in a catalog store, a held free (above) was in no list a checkpoint writes:
/// the capture took only `pending_free` as deferred, so the slot stayed counted in use, no free
/// table row named it, and the cut dropped its Release. A catalog open has no reachability sweep,
/// so after a close and a reopen the slot was lost for good. Mutant `capture_skips_held`.
#[test]
fn a_held_free_survives_a_checkpoint_and_a_reopen_in_a_raised_d0_catalog_store() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("held-checkpoint.db");
    let (slots, incarnation) = {
        let (db, _trunk, x, slots) = raised_d0_with_a_kept_pre_image(&path, true, false);
        x.reap().unwrap();
        db.branch_compact_now().unwrap();
        (slots, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    for &slot in &slots {
        assert!(db.branch_slot_is_free(slot), "slot {slot}, freed by x's release, is lost after a checkpoint and a reopen");
    }
    let in_use = db.branch_slots_in_use();
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as usize,
        in_use.len(),
        "after the reopen: the in-use count disagrees with the slots in use"
    );
}

/// Engine review 11 MED 1: a raised D0 store's FUZZY checkpoint syncs only the arena
/// (`settle_arena`) and leads no flight, so with D0-only traffic no sync in the free class ever
/// came, and a held free never matured: an unbounded slot leak, lost on disk too (a catalog open
/// has no reachability sweep). The checkpoint must return every held free it covers, with no
/// raised operation: (a) one released before the capture, which the catalog then lists free, also
/// after a reopen (mutant `capture_skips_held`); (b) is its own test
/// (`a_raised_d0_fuzzy_checkpoint_commit_makes_its_releases_durable`). A Release AFTER the capture
/// is made durable by nothing the checkpoint does (engine review 13 HIGH 1; see
/// `a_raised_d0_fuzzy_install_reuses_no_slot_of_a_later_release`).
#[test]
fn a_raised_d0_fuzzy_checkpoint_returns_held_frees_with_no_raised_op() {
    let _s = serial();
    // (a) released before the capture, then a reopen.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fuzzy-held-a.db");
    let (slots, incarnation) = {
        let (db, _trunk, x, slots) = raised_d0_with_a_kept_pre_image(&path, true, false);
        x.reap().unwrap();
        for &slot in &slots {
            assert!(!db.branch_slot_is_free(slot), "(a) premise: slot {slot} is held for a sync");
        }
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "(a) premise: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        for &slot in &slots {
            assert!(db.branch_slot_is_free(slot), "(a) slot {slot} is still held after a fuzzy checkpoint covered its Release");
        }
        (slots, db.incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    for &slot in &slots {
        assert!(db.branch_slot_is_free(slot), "(a) slot {slot} is lost after a fuzzy checkpoint and a reopen");
    }
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as usize,
        db.branch_slots_in_use().len(),
        "(a) after the reopen: the in-use count disagrees with the slots in use"
    );
}

/// Engine review 11 MED 1 (b), split from the test above (engine review 15 LOW 4): the fuzzy
/// checkpoint's catalog commit made a Release before the capture durable in the class it synced
/// in, and the install says so, so the next FULL trunk commit, whose barrier covers the Release,
/// leads no flight for it. Mutant `settle_arena_marks_no_durable`.
#[test]
fn a_raised_d0_fuzzy_checkpoint_commit_makes_its_releases_durable() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("fuzzy-held-b.db"), true, false);
    x.reap().unwrap();
    // The premise the split from arm (a) dropped (engine review 17 LOW 7): the Release's frees are
    // held, so its durability is what the checkpoint's commit must provide.
    for &slot in &slots {
        assert!(!db.branch_slot_is_free(slot), "(b) premise: slot {slot} is held for a sync");
    }
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "(b) premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    let led = db.branches.group_counters();
    write_v(&trunk, 9, "after");
    let now = db.branches.group_counters();
    assert_eq!(
        [now[0] - led[0], now[1] - led[1]],
        [0, 0],
        "(b) a FULL trunk commit led a flight for a Release the fuzzy checkpoint's commit made durable"
    );
}

/// Engine review 13 HIGH 1: a raised D0 store's fuzzy install marked everything written durable in
/// the rewrite class. But what the D0 flights after the capture wrote was synced by nothing: the
/// arena was settled before them, and the cut syncs its temp log and renames it without syncing
/// the directory (`finish_cut` leaves that to the next flight). Returns the store after a fuzzy
/// checkpoint whose capture came BEFORE x's Release, with x's slots.
fn raised_d0_with_a_release_after_a_fuzzy_capture(path: &Path, arm: char) -> (Arc<Database>, Arc<Connection>, Vec<u32>) {
    let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(path, true, false);
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "({arm}) premise: a fuzzy checkpoint started");
    eventually(&format!("({arm}) the checkpoint never arrived"), || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    // The Release follows the capture: its D0 flight syncs nothing.
    x.reap().unwrap();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert!(db.branches.rewrite_class_for_test().syncs(), "({arm}) premise: the log is still raised");
    (db, trunk, slots)
}

/// Engine review 13 HIGH 1 (A), split from its arm (B) (engine review 15 LOW 4): a held free whose
/// Release followed the capture matured, and its slot was reused, where a power cut can bring the
/// released branch back over it. Mutant `fuzzy_install_marks_rewrite_class`.
#[test]
fn a_raised_d0_fuzzy_install_reuses_no_slot_of_a_later_release() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (_db, trunk, slots) = raised_d0_with_a_release_after_a_fuzzy_capture(&dir.path().join("fuzzy-claim-a.db"), 'A');
    let took = reused_by_a_new_branch(&trunk, &slots);
    assert!(
        took.is_empty(),
        "(A) slots {took:?} were reused although nothing synced the Release that freed them"
    );
}

/// Engine review 13 HIGH 1 (B): the next FULL trunk commit, whose barrier covers a Release that
/// followed the capture, took the fast path: no flight, so no directory or arena sync before its
/// WAL flush. Mutant `fuzzy_install_marks_rewrite_class`.
#[test]
fn a_raised_d0_fuzzy_install_leaves_a_later_release_to_the_next_full_barrier() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (_db, trunk, _slots) = raised_d0_with_a_release_after_a_fuzzy_capture(&dir.path().join("fuzzy-claim-b.db"), 'B');
    let dirs = || super::journal::DIR_SYNCS.with(|c| c.get());
    let before = dirs();
    write_v(&trunk, 9, "after");
    assert!(
        dirs() > before,
        "(B) a FULL trunk commit took the fast path over a Release that nothing synced (no directory sync)"
    );
}

/// Engine review 15 MED 2: the fuzzy install marked its catalog commit durable in the rewrite class
/// read AT THE INSTALL, not the class the commit synced in (the capture's). A FullFsync flight taken
/// between the two raises it: here an ordered trunk commit (synchronous FULL, fullfsync) whose WAL
/// F_FULLFSYNC then fails, which fail-stops the store. The commit synced in Off, yet the install
/// marked everything up to the capture FullFsync-durable, on a fail-stopped store (mark_committed
/// skips the fail-stop check), so a later FULL trunk commit over a pre-capture Release could take
/// the fast path. Mutant `fuzzy_committed_class_at_install`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_fuzzy_install_marks_its_commit_in_the_class_it_synced_in() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, armed) = open_failing_wal(&dir.path().join("fuzzy-class.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.fork_branch().unwrap().reap().unwrap();
    assert!(!db.branches.rewrite_class_for_test().syncs(), "premise: the D0 store is not raised at the capture");
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never committed", || {
        db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED
    });
    let full_before = db.branches.durable_for_test(SyncClass::FullFsync);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
    armed.store(1, O::Release);
    let failed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
    assert_eq!(armed.load(O::Acquire), 0, "premise: the trunk commit's WAL sync was reached");
    assert!(failed.is_err(), "premise: its failed WAL sync failed the commit");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the commit's ordered flight raised the log");
    assert_fail_stopped(db.connect().unwrap().fork_branch().map(|x| x.into_id()), "premise: the store fail-stopped");
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branches.durable_for_test(SyncClass::FullFsync),
        full_before,
        "the fuzzy install marked its Off catalog commit FullFsync-durable on a fail-stopped store"
    );
    // The consequence (engine review 17 MED 6): with no live child, the next FULL trunk commit's
    // barrier covers only the pre-capture Release, which nothing made FullFsync-durable.
    assert_fail_stopped(
        trunk.execute("UPDATE t SET v = 'again' WHERE id = 9").map(|_| ()),
        "the next childless FULL trunk commit over a Release the install did not make durable",
    );
}

/// Engine review 17 MED 6: the arm of review 15 MED 2 that runs on every target. A D0 catalog
/// store, unraised at the capture, releases x (its Release at `r`); the fuzzy checkpoint is held
/// after its catalog commit (synced in Off); a childless FULL trunk commit then leads the flight
/// its barrier needs, raising the log (`take_flight` notes the class before the write) and failing
/// (`BarrierFlightFails`), which fail-stops the store. The install must mark its commit in the class
/// it synced in: durable(Fsync) stays below `r`, and the next childless FULL trunk commit, whose
/// barrier covers `r`, leads a flight and is refused. Mutant `fuzzy_committed_class_at_install`.
/// FLAGGED TEST EDIT (engine review 21 MED 14, a premise repair): it armed `GroupFlightFails`,
/// which only buffer_records consumes, so the childless commit's barrier flight never failed and
/// the "barrier's raised flight failed the commit" premise panicked at every sha (measured on
/// Linux at 2da787db3); the new failpoint is consumed by that barrier's own flight.
#[test]
fn a_fuzzy_install_marks_its_commit_in_its_capture_class_after_a_failed_raised_flight() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("fuzzy-class-portable.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.fork_branch().unwrap().reap().unwrap();
    let r = db.branches.last_release_lsn_for_test();
    assert!(r > 0, "premise: x's Release is logged");
    assert!(!db.branches.rewrite_class_for_test().syncs(), "premise: the D0 store is not raised at the capture");
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("premise: the checkpoint never committed", || {
        db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED
    });
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    db.branch_failpoint(Some(BranchFailpoint::BarrierFlightFails));
    let failed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
    assert!(failed.is_err(), "premise: the barrier's raised flight failed the commit");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the failed flight's take raised the log");
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert!(
        db.branches.durable_for_test(SyncClass::Fsync) < r,
        "the fuzzy install marked its Off catalog commit durable in the raised class on a fail-stopped store"
    );
    assert_fail_stopped(
        trunk.execute("UPDATE t SET v = 'again' WHERE id = 9").map(|_| ()),
        "the next childless FULL trunk commit over a Release the install did not make durable",
    );
}

/// Sets `store::HOLD_BOUND_FORCED` for one test, and clears it when dropped.
struct HoldBound;

impl HoldBound {
    fn set(slots: usize) -> Self {
        super::store::HOLD_BOUND_FORCED.store(slots, std::sync::atomic::Ordering::Release);
        Self
    }
}

impl Drop for HoldBound {
    fn drop(&mut self) {
        super::store::HOLD_BOUND_FORCED.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// Engine review 8 #5's bound: a raised D0 store that then runs D0 alone held its frees for ever
/// (no sync ever came). Past the bound of held slots (forced to 1 here; 4096 in a build), one
/// upgrade flight in the rewrite class is owed, led by the store's background writer (engine
/// review 13 MED 2), and the held slots are free after it. Mutant `hold_unbounded`.
#[test]
fn a_raised_d0_store_holds_no_more_than_its_bound_for_a_sync() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("hold-bound.db"), catalog, true);
        assert_eq!(slots.len(), 2, "catalog={catalog}: premise: x's release frees two slots, past the bound");
        x.reap().unwrap();
        for _ in 0..2 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        for &slot in &slots {
            eventually(&format!("catalog={catalog}: slot {slot} is still held past the bound"), || {
                db.branch_slot_is_free(slot)
            });
        }
    }
}

/// Engine review 13 MED 2: the held-free bound's upgrade flight (an arena fsync, a directory
/// fsync, a log F_FULLFSYNC, and a bound's worth of slots released under the store mutex) was led
/// by whichever operation waited for durability next: usually the create whose `mature` armed it,
/// or a connect, a commit, a delete, a trunk barrier under the WAL write lock. It is led off the
/// acknowledgement paths, by the store's background writer: past the bound (forced to 1) a create
/// on this thread syncs nothing, and the held slots come free once the upgrade lands. Mutant
/// `upgrade_on_ack_path`.
#[test]
fn the_held_free_upgrade_is_led_off_the_acknowledgement_path() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-off-ack.db"), catalog, true);
        assert_eq!(slots.len(), 2, "catalog={catalog}: premise: x's release frees two slots, past the bound");
        x.reap().unwrap();
        let syncs = super::store::thread_syncs();
        let _y = trunk.fork_branch().unwrap().into_id();
        assert_eq!(
            super::store::thread_syncs() - syncs,
            0,
            "catalog={catalog}: a create past the bound synced on its own thread: it led the upgrade flight"
        );
        eventually(&format!("catalog={catalog}: the held slots never came free"), || {
            slots.iter().all(|&s| db.branch_slot_is_free(s))
        });
    }
}

/// Engine review 16 MED 4: a D0 open of a log a syncing run wrote holds its frees in the class that
/// run synced in (f9592ff20, engine review 13 MED 4), so past the bound an upgrade flight in that
/// class is owed: in D2's case an F_FULLFSYNC, on whichever operation waited next, a create among
/// them. It is led off the acknowledgement paths, here too: past the bound (forced to 1) a create
/// on this thread syncs nothing, and the held slots come free once the background writer's
/// upgrade lands. Mutant `upgrade_on_ack_path`.
#[test]
fn a_d0_open_of_a_synced_log_leads_no_upgrade_on_a_create() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for (catalog, written) in [(false, SyncClass::Fsync), (true, SyncClass::Fsync), (false, SyncClass::FullFsync), (true, SyncClass::FullFsync)] {
        let what = format!("catalog={catalog} written={written:?}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("synced-open-upgrade.db");
        let (id, slots, incarnation) = {
            let db = open_at(&path, opts(catalog, written));
            let trunk = db.connect().unwrap();
            seed_wide(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            write_v(&x.connect().unwrap(), 40, "x");
            let slots = x.owned_slots();
            assert!(slots.len() >= 2, "{what}: premise: x's writes took two slots, past the bound");
            (x.into_id(), slots, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        let trunk = db.connect().unwrap();
        // FLAGGED TEST EDIT (engine review 19 LOW 12's assertion, its count moved; for the lead's
        // ruling): the upgrade count is read before the reap. Read after it, it raced an upgrade the
        // reap had already armed (taken before the read, landing after the held-slot premise): the
        // count read 0 in the catalog arm at bb6af4d94 and 9d27592bf (artie raws l1-branch*), and in
        // the snapshot arm in a dev-tree run. The claim, exactly one upgrade, is unchanged.
        let upgrades = db.branches.group_counters()[4];
        db.branch(id).unwrap().reap().unwrap();
        // FLAGGED TEST EDIT (engine review 19 LOW 10, a premise added): the slots are held after the
        // reap, deterministically: a release never matures frees, so nothing arms the upgrade
        // until the create below. Without it a free class regressed to Off armed nothing and the
        // test still passed (mutant `free_class_ignores_synced_open`).
        for &s in &slots {
            assert!(!db.branch_slot_is_free(s), "{what}: premise: slot {s} is held after x's reap");
        }
        let syncs = super::store::thread_syncs();
        let _y = trunk.fork_branch().unwrap().into_id();
        assert_eq!(
            super::store::thread_syncs() - syncs,
            0,
            "{what}: a create past the bound synced on its own thread: it led the upgrade flight"
        );
        eventually(&format!("{what}: the held slots never came free"), || {
            slots.iter().all(|&s| db.branch_slot_is_free(s))
        });
        // FLAGGED TEST EDIT (engine review 19 LOW 12, an assertion added): where the upgrade's flush
        // went, the store's own count (PREREG §4 V2 credits a flush off the operation's thread only
        // to a registered background writer): exactly one upgrade, the background writer's.
        assert_eq!(
            db.branches.group_counters()[4] - upgrades,
            1,
            "{what}: the held slots came free by other than one held-free upgrade"
        );
    }
}

/// Engine review 20 MED 5, RULED (a) (DECISIONS afd0e67f81): a D1 process over a log a D2 run
/// wrote freed what its Release, only fsynced, made durable. On Apple a power cut can lose that
/// Release while the D2 records naming the slot survive, so a slot a new branch reused would come
/// back under a D2-acknowledged branch: D1 breaking D2's promise. The free class held frees only in
/// D0 (`!self.sync.syncs()`), so the synced base was never read in D1. Here x, written at D2, is
/// reaped by a D1 reopen: a new branch reuses none of x's slots until durable(FullFsync) covers
/// the Release (no upgrade runs here: the bound is not reached). Both stores. Mutant
/// `free_hold_d0_only`.
#[test]
fn a_d1_open_of_a_d2_log_holds_frees_until_a_full_flush() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("d1-over-d2.db");
        let (id, slots, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::FullFsync));
            let trunk = db.connect().unwrap();
            seed_wide(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            write_v(&x.connect().unwrap(), 40, "x");
            let slots = x.owned_slots();
            assert!(!slots.is_empty(), "{what}: premise: x's writes took slots");
            (x.into_id(), slots, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        assert_eq!(
            db.branches.synced_base_for_test(),
            SyncClass::FullFsync,
            "{what}: premise: the D1 open decided the D2 run's class"
        );
        let trunk = db.connect().unwrap();
        db.branch(id).unwrap().reap().unwrap();
        let release = db.branches.last_release_lsn_for_test();
        assert!(release > 0, "{what}: premise: x's Release is logged");
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(
            took.is_empty() || db.branches.durable_for_test(SyncClass::FullFsync) >= release,
            "{what}: slots {took:?} were reused before a full flush covered the Release that freed them"
        );
    }
}

/// Engine review 18 HIGH 1 (review 19 MED 5): the held-free upgrade was led off the
/// acknowledgement paths (engine review 13 MED 2) but still as an ordinary group flight: it took
/// everything buffered and the group's one flight slot (`flushing`), so every D0 create, connect,
/// first write and delete waited through its arena fsync, directory fsync and log sync, and those
/// whose records were buffered at its take rode it. It syncs what is written and takes no flight
/// slot: held once taken and before its sync, a create on another connection is acknowledged,
/// having waited for nothing and synced nothing on its thread, and the held slots come free once
/// it lands. The writer is first held where it found the upgrade due, so the create that armed it
/// cannot be carried. Mutant `upgrade_takes_flight_slot`.
#[test]
fn a_create_does_not_wait_for_the_held_free_upgrade() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-slot.db"), catalog, true);
        assert_eq!(slots.len(), 2, "catalog={catalog}: premise: x's release frees two slots, past the bound");
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("catalog={catalog}: premise: the background writer never found the upgrade due"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
        });
        // Moving the hook releases the first stop; the writer then stops once its upgrade is taken.
        db.branches.confirm_hold_for_test(super::store::HOLD_FLIGHT_TAKEN);
        eventually(&format!("catalog={catalog}: premise: the background writer never took the upgrade"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
        });
        let (tx, rx) = std::sync::mpsc::channel();
        let creator = {
            let db = db.clone();
            std::thread::spawn(move || {
                let (waits, syncs) = (super::store::thread_waits(), super::store::thread_syncs());
                let created = db.connect().and_then(|t| t.fork_branch()).map(|y| y.into_id());
                let _ = tx.send((
                    created.is_ok(),
                    super::store::thread_waits() - waits,
                    super::store::thread_syncs() - syncs,
                ));
            })
        };
        let created = rx.recv_timeout(std::time::Duration::from_secs(10));
        db.branches.confirm_hold_for_test(0);
        creator.join().unwrap();
        assert_eq!(
            created,
            Ok((true, 0, 0)),
            "catalog={catalog}: a create during the held-free upgrade was not acknowledged at once \
             (created, waits, syncs on its thread; a timeout: it waited for the upgrade)"
        );
        eventually(&format!("catalog={catalog}: the held slots never came free"), || {
            slots.iter().all(|&s| db.branch_slot_is_free(s))
        });
    }
}

/// `a_create_does_not_wait_for_the_held_free_upgrade`'s stops, for the tests that follow it: x is
/// reaped past the bound, a fork arms the upgrade while the writer is held where it found it due
/// (so nothing rides it), and the writer is then held once the upgrade is taken and before its sync.
/// Returns the fork that armed it, still live.
fn hold_the_held_free_upgrade(db: &Arc<Database>, trunk: &Arc<Connection>, x: Branch, what: &str) -> BranchId {
    db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
    x.reap().unwrap();
    let armed = trunk.fork_branch().unwrap().into_id();
    eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
        db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
    });
    db.branches.confirm_hold_for_test(super::store::HOLD_FLIGHT_TAKEN);
    eventually(&format!("{what}: premise: the background writer never took the upgrade"), || {
        db.branches.confirm_held_for_test() == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
    });
    armed
}

/// Run `f` on a thread of its own and wait for it at most 10 s: `Err` on a timeout. The caller joins
/// the thread once whatever holds it is released.
fn within_10s<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> (Result<T, std::sync::mpsc::RecvTimeoutError>, std::thread::JoinHandle<()>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    (rx.recv_timeout(std::time::Duration::from_secs(10)), handle)
}

/// Engine review 21 MED 3 (review 18 HIGH 1's residual, wider than 7fa0b2853 stated): the upgrade
/// took the arena's dirty mark and the directory's, so every flight that syncs waited for its sync
/// (`syncing_outside_flight`): a trunk commit that keeps a pre-image, whose barrier syncs, waited
/// through the upgrade's arena fsync and log sync holding the WAL write lock, and so did every fork
/// riding an ordered trunk commit (Apple). Held once taken and before its sync, a trunk commit
/// that keeps a pre-image for the live child that armed it commits within 10 s. Both stores.
#[test]
fn a_trunk_commit_that_keeps_a_pre_image_does_not_wait_for_the_held_free_upgrade() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-barrier.db"), catalog, true);
        assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
        let _armed = hold_the_held_free_upgrade(&db, &trunk, x, &what);
        let (committed, handle) = {
            let db = db.clone();
            within_10s(move || {
                let t = db.connect().unwrap();
                t.execute("PRAGMA synchronous = FULL").unwrap();
                t.execute("UPDATE t SET v = 'during' WHERE id = 50").map_err(|e| e.to_string())
            })
        };
        db.branches.confirm_hold_for_test(0);
        handle.join().unwrap();
        assert_eq!(
            committed,
            Ok(Ok(())),
            "{what}: a trunk commit that keeps a pre-image waited for the held-free upgrade's sync (a timeout) or failed"
        );
    }
}

/// Engine review 21 MED 2: begin_cut waited only for `flushing` and `cutting`, so a fuzzy
/// checkpoint went on to its install while a held-free upgrade synced, and the install, holding the
/// store mutex, waited in `quiesce` for the upgrade's sync: every create, connect, first write and
/// delete queued behind it on the mutex. The checkpoint is held after its catalog commit, the
/// upgrade is armed and held once taken, and the checkpoint released: its install completes within
/// 10 s while the upgrade is still in the air, and so does a create on another connection.
#[test]
fn a_fuzzy_install_does_not_wait_for_the_held_free_upgrade() {
    let _s = serial();
    let _b = HoldBound::set(1);
    let what = "catalog=true";
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-install.db"), true, true);
    assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "{what}: premise: a fuzzy checkpoint started");
    eventually(&format!("{what}: premise: the checkpoint never committed"), || {
        db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED
    });
    let _armed = hold_the_held_free_upgrade(&db, &trunk, x, what);
    db.branch_checkpoint_hold(0);
    let (installed, install) = {
        let db = db.clone();
        within_10s(move || db.branch_checkpoint_wait())
    };
    let (created, create) = {
        let db = db.clone();
        within_10s(move || db.connect().and_then(|t| t.fork_branch()).map(|y| y.into_id()).is_ok())
    };
    db.branches.confirm_hold_for_test(0);
    install.join().unwrap();
    create.join().unwrap();
    assert_eq!(installed, Ok(()), "{what}: the fuzzy install waited for the held-free upgrade's sync");
    assert_eq!(created, Ok(true), "{what}: a create waited behind the install for the upgrade (or failed)");
}

/// Wire review 18 HIGH 3, the store's half: a client's CHECKPOINT ran the sharp catalog checkpoint
/// (`branch_compact_now`), which holds the store mutex across its settle, its catalog write and
/// sync, its cut and the cut's directory fsync, so every create, switch and connect waited for it.
/// `branch_checkpoint_now` runs the fuzzy one: held before its catalog commit, a create on another
/// connection returns within 10 s, and once released the checkpoint installs. Mutant
/// `checkpoint_now_sharp`.
#[test]
fn a_requested_checkpoint_lets_creates_run_while_it_writes() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("checkpoint-now.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    let before = db.branch_checkpoint_counters();
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    let checkpoint = {
        let db = db.clone();
        std::thread::spawn(move || db.branch_checkpoint_now(std::time::Duration::from_secs(30)))
    };
    eventually("CLAIM: the requested checkpoint never paused before its catalog commit: it was not fuzzy", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let (created, create) = {
        let db = db.clone();
        within_10s(move || db.connect().and_then(|t| t.fork_branch()).map(|y| y.into_id()).is_ok())
    };
    db.branch_checkpoint_hold(0);
    create.join().unwrap();
    let installed = checkpoint.join().unwrap();
    assert_eq!(created, Ok(true), "a create waited for the requested checkpoint (a timeout) or failed");
    assert!(matches!(installed, Ok(true)), "the requested checkpoint did not install: {installed:?}");
    let after = db.branch_checkpoint_counters();
    assert!(
        after[0] > before[0] && after[1] > before[1],
        "the requested checkpoint installed no fuzzy flight: counters {before:?} -> {after:?}"
    );
}

/// Engine review 21 MED 4: the held-free upgrade's sync is the only step that makes a raised D0
/// store's held frees safe to reuse, and nothing could fail it. With its sync failing
/// (`UpgradeSyncFails`), the store fail-stops, and the slots it was to free stay held. Both stores.
#[test]
fn a_failed_held_free_upgrade_fail_stops_the_store_and_frees_nothing() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-fails.db"), catalog, true);
        assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
        db.branch_failpoint(Some(BranchFailpoint::UpgradeSyncFails));
        // C15 MED 3 (a fixture reaching its path, ruled at fastest DECISIONS 2026-10-15T04:15:07Z):
        // the writer is held where it finds the upgrade due until the arming fork has returned, so
        // its failing sync cannot fail-stop the store under that fork's unwrap.
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
        });
        db.branches.confirm_hold_for_test(0);
        eventually(&format!("{what}: a failed upgrade sync left the store running"), || {
            db.connect().unwrap().fork_branch().is_err()
        });
        assert_fail_stopped(trunk.fork_branch().map(|y| y.into_id()), &format!("{what}: the next fork"));
        for &slot in &slots {
            assert!(!db.branch_slot_is_free(slot), "{what}: slot {slot} came free after a failed upgrade");
        }
    }
}

/// Engine review 21 MED 4, the mutants' killer (a guard at the base, where the upgrade's sync makes
/// both): a held-free upgrade over a dirty arena makes exactly two syncs in a store whose free
/// class is Fsync, the arena's and the log's. Mutants (test builds only) `upgrade_no_arena_sync`
/// and `upgrade_no_log_sync`.
#[test]
fn a_held_free_upgrade_syncs_the_arena_and_the_log() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-syncs.db"), catalog, true);
        assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
        assert_eq!(db.branches.rewrite_class_for_test(), SyncClass::Fsync, "{what}: premise: the free class is Fsync");
        let z = trunk.fork_branch().unwrap();
        write_v(&z.connect().unwrap(), 40, "z");
        assert!(db.branches.arena_dirty_for_test(), "{what}: premise: z's D0 write left the arena unsynced");
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
        });
        let before = sync_counts();
        db.branches.confirm_hold_for_test(0);
        eventually(&format!("{what}: the held slots never came free"), || {
            slots.iter().all(|&s| db.branch_slot_is_free(s))
        });
        let after = sync_counts();
        assert_eq!(
            (after.fsync - before.fsync, after.full_fsync - before.full_fsync),
            (2, 0),
            "{what}: the upgrade's syncs (fsync, full fsync): the arena's and the log's"
        );
        drop(z);
    }
}

/// Engine review 16 LOW 12:a compaction marked what it made durable in the rewrite class read
/// AFTER the rewrite, whose own log reset clears the inherited floor: a D0 store over a log a D2
/// run wrote compacted in FullFsync and said Off, so the next FULL trunk commit, whose barrier
/// covers a Release the snapshot carries, led a flight for it. The class is read before the
/// rewrite. Both modes (a catalog store's sharp checkpoint marks its commit in the class captured
/// before it already: the control). Mutant `rewrite_class_read_after`.
#[test]
fn a_compaction_over_an_inherited_floor_marks_what_it_synced() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("inherited-compaction.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::FullFsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let x = trunk.fork_branch().unwrap();
            write_v(&x.connect().unwrap(), 3, "x");
            (x.into_id(), db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        assert!(db.branches.rewrite_class_for_test().syncs(), "catalog={catalog}: premise: the open inherited the D2 run's class");
        db.branch(id).unwrap().reap().unwrap();
        db.branch_compact_now().unwrap();
        // FLAGGED TEST EDIT (engine review 18 MED 2): the premise was that the rewrite cleared the inherited
        // floor; now the D2 run's class outlives it (written into the header), which this premise states.
        assert_eq!(db.branches.rewrite_class_for_test(), SyncClass::FullFsync, "catalog={catalog}: premise: the D2 run's class outlives the rewrite");
        let trunk = db.connect().unwrap();
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let led = db.branches.group_counters();
        write_v(&trunk, 9, "after");
        let now = db.branches.group_counters();
        assert_eq!(
            [now[0] - led[0], now[1] - led[1], now[4] - led[4]],
            [0, 0, 0],
            "catalog={catalog}: a FULL trunk commit led a flight for a Release the compaction made durable in FullFsync"
        );
    }
}

/// Engine review 8 #8: three free paths skipped the hold: a lease's expiry (every fork's expiry
/// pass), `reap_if_due`, and a close's collection of a branch released while a connection was
/// open. They logged the Release with a write only, then freed at once. Here a lease runs out and
/// the next fork's expiry pass reaps x; and x is released with a connection open, freed when that
/// connection closes. Neither frees a slot before a sync covers its Release. Mutant
/// `expiry_frees_at_once`.
#[test]
fn an_expiry_or_a_close_in_a_raised_d0_log_holds_its_slots_until_a_sync() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("expiry-hold.db"), catalog, false);
        x.lease(std::time::Duration::from_millis(1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let _z = trunk.fork_branch().unwrap().into_id();
        assert!(db.branch(x.id()).is_err(), "catalog={catalog}: premise: the fork's expiry pass reaped x");
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(took.is_empty(), "catalog={catalog}: slots {took:?} an expired lease freed were reused before a sync");
        drop(x);
        drop(db);

        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("close-hold.db"), catalog, false);
        let xc = x.connect().unwrap();
        x.reap().unwrap();
        drop(xc);
        let took = reused_by_a_new_branch(&trunk, &slots);
        assert!(took.is_empty(), "catalog={catalog}: slots {took:?} a close collected were reused before a sync");
        drop(db);
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
        let entered = || db.branches.captures_entered_for_test();
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
        // FLAGGED TEST EDIT (lead order, review L2d LOW 4; strengthening only): since slice 3 a
        // retried capture runs on the checkpoint worker after the create returns, so the read waits
        // for every start handed out; without it the retry mutant was caught only by timing.
        db.branch_checkpoint_wait();
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
    let entered = || db.branches.captures_entered_for_test();
    let (lsn, before) = (db.branches.log_lsn_for_test(), entered());
    assert!(!db.branch_checkpoint_fuzzy_now().unwrap(), "a checkpoint started through the failpoint");
    assert_eq!(entered(), before + 1, "premise: the capture was entered");
    assert_eq!(db.branches.log_lsn_for_test(), lsn, "the failed capture appended to the log");
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no checkpoint started after the failed one");
    db.branch_checkpoint_wait();
}

/// Engine review 8 #3: the back-off after a failed checkpoint start ENDS. It lasts one threshold
/// of log (`defer_compaction`), no less (every create would retry the capture) and no more: forks
/// go on, a capture is entered again once the log grew by about a threshold past the failure, and
/// that checkpoint starts and installs. Its cut ends the back-off, so the next start comes within
/// a threshold of the cut. The earlier tests stop after five creates, or start checkpoints by hand
/// past `wants_compaction`, so a back-off that never ended survived them. Mutants
/// `backoff_never_resumes` and `no_backoff_reset` (a cut keeps the back-off, measured in the old
/// log's length).
#[test]
fn a_failed_checkpoint_start_backs_off_for_one_threshold_and_no_longer() {
    let _s = serial();
    let threshold: u64 = 8 << 10;
    for fp in [BranchFailpoint::CaptureFails, BranchFailpoint::SpawnFails] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("backoff-ends.db"),
            opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _t = Threshold::set(threshold);
        let entered = || db.branches.captures_entered_for_test();
        let len = || db.branches.log_len_for_test();
        let fork = || {
            let _ = trunk.fork_branch().unwrap().into_id();
        };
        // The failed start.
        db.branch_failpoint(Some(fp));
        let before = entered();
        let t = std::time::Instant::now();
        while entered() == before {
            fork();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "{fp:?}: premise: no checkpoint was attempted");
        }
        db.branch_checkpoint_wait();
        assert_eq!(db.branch_checkpoint_start_failures(), 1, "{fp:?}: the failed start was not counted");
        let (failed_at, installed) = (len(), db.branch_checkpoint_counters()[0]);
        // The next capture comes about a threshold later, and starts.
        let tried = entered();
        while entered() == tried {
            fork();
            assert!(
                len() - failed_at <= 4 * threshold,
                "{fp:?}: the back-off never ended: no capture within four thresholds of log past the failure"
            );
        }
        let grown = len() - failed_at;
        assert!(
            grown >= threshold / 2 && grown <= threshold + threshold / 2,
            "{fp:?}: the back-off lasted {grown} bytes of log, not about one threshold ({threshold})"
        );
        db.branch_checkpoint_wait();
        assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "{fp:?}: premise: the retried checkpoint installed");
        // Its cut ended the back-off: the next start comes within a threshold of the cut.
        let (cut_len, tried) = (len(), entered());
        while entered() == tried {
            fork();
            assert!(
                len() <= cut_len + 4 * threshold,
                "{fp:?}: no checkpoint within four thresholds of log after the cut"
            );
        }
        assert!(
            len() <= threshold + threshold / 2,
            "{fp:?}: the cut kept the back-off: the next start came at {} bytes of log (threshold {threshold})",
            len()
        );
        db.branch_checkpoint_wait();
    }
}

/// Engine review 8 #3 (review 4 #2): the capture's OTHER fallible step, its handle on the arena
/// file, fails before the capture has any effect on the log, as a failed read snapshot does: no
/// marker, no generation, and the next checkpoint goes through. Only a sharp checkpoint takes the
/// handle, over slots no flight synced: a D0 store whose log was raised. Mutant
/// `capture_marker_first`.
#[test]
fn a_capture_whose_arena_handle_fails_appends_no_checkpoint_marker() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, _trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    db.branch_failpoint(Some(BranchFailpoint::ArenaHandleFails));
    let entered = || db.branches.captures_entered_for_test();
    let (lsn, before) = (db.branches.log_lsn_for_test(), entered());
    assert!(db.branch_compact_now().is_err(), "premise: the capture's arena handle failed");
    assert_eq!(entered(), before + 1, "premise: the capture was entered");
    assert_eq!(db.branches.log_lsn_for_test(), lsn, "the failed capture appended to the log");
    db.branch_compact_now().expect("no checkpoint went through after the failed one");
}

/// The branch log's header confirmation word, the checksum of its last end frame and its length
/// (review 6 #1).
fn log_confirmation_at(log: &Path) -> (u32, u32, u64) {
    let bytes = std::fs::read(log).unwrap();
    let n = bytes.len();
    let word = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
    (word, u32::from_le_bytes(bytes[n - 4..].try_into().unwrap()), n as u64)
}

fn log_confirmation(db: &Arc<Database>) -> (u32, u32, u64) {
    log_confirmation_at(&db.branch_log_path().expect("a durable store"))
}

/// The word that confirms a last flight whose end frame's checksum is `crc` and which ends at byte
/// `end` of the log (review 6 #1: bound to where it ends, so no other flight can match it).
fn bound_word(crc: u32, end: u64) -> u32 {
    crc32c::crc32c_append(crc, &end.to_le_bytes())
}

/// Sets `store::CONFIRM_QUIET_MS` for one test, and clears it when dropped.
struct ConfirmQuiet;

impl ConfirmQuiet {
    fn set(ms: u64) -> Self {
        super::store::CONFIRM_QUIET_MS.store(ms, std::sync::atomic::Ordering::Release);
        Self
    }
}

impl Drop for ConfirmQuiet {
    fn drop(&mut self) {
        super::store::CONFIRM_QUIET_MS.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// Poll `f` until it holds, for up to 10 s.
fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let t = std::time::Instant::now();
    while !f() {
        assert!(t.elapsed() < std::time::Duration::from_secs(10), "{what}");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Review 6 #1: a create is acknowledged before anything is written into the log's header. The
/// flight wrote its confirmation word after its sync and before its acknowledgement, so every D2
/// acknowledgement followed an unsynced write (PREREG V2), and every flight paid a pwrite and a
/// second dirty block. Here the word is held back for 60 s: whatever is in the header right after
/// the acknowledgement was there before the flight.
#[test]
fn a_flights_acknowledgement_comes_before_any_confirmation_word() {
    let _s = serial();
    let _q = ConfirmQuiet::set(60_000);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("ackwin.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _second = trunk.fork_branch().unwrap().into_id();
        let (word, last, len) = log_confirmation(&db);
        assert!(
            word != last && word != bound_word(last, len),
            "catalog={catalog}: the create was acknowledged after its flight's confirmation word was written ({word:#x})"
        );
    }
}

/// Review 6 #1: once the log is idle, its last flight is confirmed, by a word bound to the
/// flight's end frame AND to the offset where it ends.
#[test]
fn an_idle_logs_last_flight_is_confirmed_bound_to_where_it_ends() {
    let _s = serial();
    let _q = ConfirmQuiet::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("idletail.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _second = trunk.fork_branch().unwrap().into_id();
        // FLAGGED TEST EDIT (engine review 8 #12): waited for until the word is the LAST flight's,
        // not merely nonzero: with a 1 ms quiet period the first fork's word could satisfy the
        // wait, and the assert then compared it with the second flight's (a flake on correct
        // code). A word never written, or never bound, still fails, on the deadline.
        eventually(&format!("catalog={catalog}: the idle log's last flight was never confirmed"), || {
            let (word, last, len) = log_confirmation(&db);
            word == bound_word(last, len)
        });
        let (word, last, len) = log_confirmation(&db);
        assert_eq!(word, bound_word(last, len), "catalog={catalog}: the confirmation is not bound to the last flight's end");
    }
}

/// Engine review 8 #10: while a flight or a cut is in the air, the confirmation writer, with a
/// proved word pending past its quiet period, re-took the group's mutex every 100 µs (the lock the
/// flight's leader and its riders need) for the whole flush: about 5, 30 and 220 lock cycles per
/// flight on the internal SSD, under load and on the APFS image. It waits for the landing instead.
/// Held at a flight's take for 70 ms with a word pending, the writer wakes a handful of times at
/// most, not hundreds. Mutant `confirm_poll_100us`.
#[test]
fn the_confirmation_writer_does_not_poll_while_a_flight_is_in_the_air() {
    let _s = serial();
    let _q = ConfirmQuiet::set(20);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("nopoll.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let hold = db.branches.trunk_commit_hold.clone();
    // A proved word pending, which the writer writes once the log is idle for 20 ms.
    let _first = trunk.fork_branch().unwrap().into_id();
    let written = db.branch_confirm_counts()[0];
    hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
    let creator = {
        let db = db.clone();
        std::thread::spawn(move || db.connect().and_then(|t| t.fork_branch()).map(|x| x.into_id()))
    };
    wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
    let woken = db.branches.confirm_wakeups_for_test();
    std::thread::sleep(std::time::Duration::from_millis(70));
    let wakeups = db.branches.confirm_wakeups_for_test() - woken;
    let still = db.branch_confirm_counts()[0];
    hold.store(0, std::sync::atomic::Ordering::Release);
    creator.join().unwrap().unwrap();
    assert_eq!(still, written, "premise: the pending word was still pending while the flight was held");
    assert!(
        wakeups <= 11,
        "the confirmation writer woke {wakeups} times in 70 ms while a flight was in the air"
    );
}

/// Engine review 8 #11: the confirmation writer held the group's flight slot (`flushing`) across
/// its pwrite of the word, and that hold protected nothing: the word names a flight whose sync has
/// returned, and on unix every replacement of the log is a rename, so a word written meanwhile
/// reaches the old file only. Every create waited on it. Held here between taking the word and
/// writing it, a create on another connection must be acknowledged. Mutant `confirm_holds_slot`.
#[cfg(unix)]
#[test]
fn a_create_does_not_wait_for_a_confirmation_word_being_written() {
    let _s = serial();
    let _q = ConfirmQuiet::set(1);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("wordhold.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    db.branches.confirm_hold_for_test(super::store::HOLD_CONFIRM_WRITE);
    // A proved word pending: the writer takes it once the log is idle, and waits there.
    let _first = trunk.fork_branch().unwrap().into_id();
    eventually("the confirmation writer never took the word", || {
        db.branches.confirm_held_for_test() == super::store::HOLD_CONFIRM_WRITE | super::store::HOLD_ARRIVED
    });
    let (tx, rx) = std::sync::mpsc::channel();
    let creator = {
        let db = db.clone();
        std::thread::spawn(move || {
            let created = db.connect().and_then(|t| t.fork_branch()).map(|x| x.into_id());
            let _ = tx.send(created.is_ok());
        })
    };
    let created = rx.recv_timeout(std::time::Duration::from_secs(10));
    db.branches.confirm_hold_for_test(0);
    creator.join().unwrap();
    assert_eq!(created, Ok(true), "a create waited for the confirmation writer's word (or failed)");
}

// FLAGGED TEST EDIT (lead ruling f329b0d2a8 reverses review 6 #1): the test
// `a_confirmation_that_cannot_be_written_fails_no_flight`, which asserted that the store goes on
// after a failed confirmation word, is REMOVED. Its injection (ConfirmWriteFails on the second
// create, both arms, D2) now asserts the opposite, below.

/// Lead ruling f329b0d2a8 (order 2, 2026-10-15T07:29:43Z; gates ultrareview row 1), reversing
/// review 6 #1: a confirmation word that cannot be written FAIL-STOPS the store, the rule of a
/// failed WAL sync, and `confirm_done` is raised only once a word is written. Injected
/// (`ConfirmWriteFails`): after the failed write is counted, `confirm_done` still names the last
/// word written, and the next create is refused. The create whose word failed was acknowledged
/// before its word was due (the word follows its sync), so it is not the claim.
#[test]
fn a_confirmation_that_cannot_be_written_fail_stops_the_store() {
    let _s = serial();
    let _q = ConfirmQuiet::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("confirmstop.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("catalog={catalog}: premise: no confirmation word was written"), || {
            db.branch_confirm_counts()[0] >= 1
        });
        let done = db.branches.confirm_done_for_test();
        assert_ne!(done, 0, "catalog={catalog}: premise: a written word left confirm_done at 0");
        db.branch_failpoint(Some(BranchFailpoint::ConfirmWriteFails));
        let _ = trunk.fork_branch().map(|b| b.into_id());
        eventually(&format!("catalog={catalog}: premise: the failed confirmation was not counted"), || {
            db.branch_confirm_counts()[1] == 1
        });
        assert_eq!(
            db.branches.confirm_done_for_test(),
            done,
            "CLAIM: catalog={catalog}: confirm_done was raised over a word whose write failed"
        );
        let after = trunk.fork_branch().map(|b| b.into_id());
        assert!(after.is_err(), "CLAIM: catalog={catalog}: a create was acknowledged after a confirmation word failed: {after:?}");
    }
}

/// Review 6 #1: on Apple a plain fsync does not drain the device's cache, so a D1 flight proves
/// nothing reached stable storage and is not confirmed while the store runs. A clean close makes
/// the log stable with a full flush and then confirms its last flight.
#[cfg(target_vendor = "apple")]
#[test]
fn a_plain_fsync_confirms_no_flight_until_a_close_on_apple() {
    let _s = serial();
    let _q = ConfirmQuiet::set(1);
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let log = {
            let db = open_at(&dir.path().join("d1confirm.db"), opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let _first = trunk.fork_branch().unwrap().into_id();
            let _second = trunk.fork_branch().unwrap().into_id();
            std::thread::sleep(std::time::Duration::from_millis(200));
            assert_eq!(log_confirmation(&db).0, 0, "catalog={catalog}: a D1 flight was confirmed on Apple");
            db.branch_log_path().unwrap()
        };
        let (word, last, len) = log_confirmation_at(&log);
        assert_eq!(word, bound_word(last, len), "catalog={catalog}: the closed log's last flight is not confirmed");
    }
}

/// Review 6 #1: a cut confirms the flight it keeps last only when its own sync proves stable
/// storage. A D0 cut syncs nothing, yet wrote the word: recovery then takes a bad slot of that
/// flight for damage and refuses the branch, where the slot was only never written.
#[test]
fn a_d0_cut_confirms_no_flight() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("d0cut.db"),
        opts(true, SyncClass::Off).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    let installed = db.branch_checkpoint_counters()[0];
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never arrived", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    // A flight after the capture: the cut keeps it, as its last.
    let _second = trunk.fork_branch().unwrap().into_id();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "premise: the checkpoint installed");
    let (word, _, len) = log_confirmation(&db);
    assert!(len > 40, "premise: the cut log keeps the flight after the capture");
    assert_eq!(word, 0, "a D0 cut confirmed a flight no sync proved");
}

/// Review 6 #1's rule where the second guard does not hide it (the 2026-10-15 mutant sweep:
/// `cut_confirms_unsynced` survived `a_d0_cut_confirms_no_flight`, whose D0 flights are written
/// unsynced, so `all_synced` refused the word anyway): on Apple a D1 cut's fsync(2) proves no stable
/// storage, though every flight it keeps is tagged synced. The D1 cut leaves the last flight it
/// keeps unconfirmed. Mutant `cut_confirms_unsynced`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_d1_cut_on_apple_confirms_no_flight() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("d1cut.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    let installed = db.branch_checkpoint_counters()[0];
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never arrived", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let _second = trunk.fork_branch().unwrap().into_id();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "premise: the checkpoint installed");
    let (word, _, len) = log_confirmation(&db);
    assert!(len > 40, "premise: the cut log keeps the flight after the capture");
    assert_eq!(word, 0, "CLAIM: a D1 cut on Apple confirmed a flight its fsync(2) did not prove");
}

/// Engine review 7 #4: a snapshot store's open deep-copied the whole recovered snapshot state
/// before its replay whenever the last flight was unconfirmed (every D0 open whose last flight
/// names a slot), to replay again only if that flight's check failed. The copy is O(state) and
/// doubles peak memory, at every such open, though the check passes on nearly all of them. Budget:
/// no copy when the check passes, at 10 and at 1000 branches. Mutant `snapshot_copied_at_open`.
#[test]
fn a_snapshot_stores_open_copies_no_state_when_the_last_flight_checks_out() {
    let _s = serial();
    for n in [10usize, 1000] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("snapcopy.db");
        let incarnation = {
            let db = open_at(&path, opts(false, SyncClass::Off));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            for _ in 0..n {
                let _ = trunk.fork_branch().unwrap().into_id();
            }
            db.branch_compact_now().unwrap();
            let b = trunk.fork_branch().unwrap();
            write_v(&b.connect().unwrap(), 3, "last");
            let _ = b.into_id();
            db.incarnation
        };
        let db = reopen(&path, opts(false, SyncClass::Off), incarnation);
        let stats = db.branch_open_stats();
        assert!(stats.snap_bytes > 0, "n={n}: premise: the open read a snapshot");
        assert!(stats.checked_slots > 0, "n={n}: premise: the open checked the last flight's slots");
        assert_eq!(stats.replays, 1, "n={n}: premise: the check passed (one replay)");
        assert_eq!(
            stats.snapshot_copies, 0,
            "n={n}: the open copied the recovered snapshot state though the last flight checked out"
        );
    }
}

/// Engine review 7 #4 (its confirm half): an open that checked an unconfirmed last flight's slots
/// and found them whole confirmed nothing, so every later open read them again. When the open's
/// own sync of the kept log proves stable storage and the arena holds no unsynced write, it now
/// writes that flight's word, and the next open checks no slot. Here the word is lost once (zeroed
/// after a clean close). Mutant `open_confirms_nothing`.
#[cfg(unix)]
#[test]
fn an_open_that_checks_its_last_flight_whole_confirms_it_for_the_next() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("confirm-at-open.db");
    let (log, incarnation) = {
        let db = open_at(&path, opts(false, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        write_v(&b.connect().unwrap(), 3, "b");
        let _ = b.into_id();
        (db.branch_log_path().unwrap(), db.incarnation)
    };
    // The last flight's word is lost.
    {
        use std::os::unix::fs::FileExt;
        let l = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
        l.write_all_at(&[0u8; 4], 36).unwrap();
    }
    let incarnation = {
        let db = reopen(&path, opts(false, SyncClass::FullFsync), incarnation);
        let stats = db.branch_open_stats();
        assert!(stats.checked_slots > 0, "premise: the first open checked the unconfirmed last flight's slots");
        assert_eq!(stats.replays, 1, "premise: the check passed");
        db.incarnation
    };
    let db = reopen(&path, opts(false, SyncClass::FullFsync), incarnation);
    assert_eq!(
        db.branch_open_stats().checked_slots,
        0,
        "the next open checked again a last flight the previous open had found whole"
    );
}

/// Engine review 8 #4: once a D0 store's log is raised (here by a trunk commit under a fullfsync
/// trunk), its rewrite class proves stable storage, so a cut confirmed whatever flight it kept
/// last by the class alone: a plain D0 flight too, whose slot no sync covered (it landed after
/// the checkpoint's arena sync). Recovery then took the flight as confirmed and refused its lost
/// slot as damage. Now the cut confirms only a kept suffix whose flights were all written synced,
/// so the D0 flight is checked at the open, its lost slot drops it, and the branch reads the row
/// before it. Mutant `cut_confirms_by_class`.
#[cfg(unix)]
#[test]
fn a_raised_d0_cut_confirms_no_d0_flight_it_keeps() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("raisedcut.db");
    let (id, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Off).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _b = trunk.fork_branch().unwrap().into_id();
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        trunk.execute("UPDATE t SET v = 'trunk-new' WHERE id = 9").unwrap();
        assert_eq!(db.branches.rewrite_class_for_test(), SyncClass::FullFsync, "premise: the trunk commit raised the log to F_FULLFSYNC");
        let c = trunk.fork_branch().unwrap();
        let cc = c.connect().unwrap();
        write_v(&cc, 4, "kept");
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        eventually("the checkpoint never arrived", || {
            db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
        });
        // A D0 flight after the checkpoint's arena sync: the cut keeps it, as its last.
        let before: std::collections::HashSet<u32> = c.owned_slots().into_iter().collect();
        write_v(&cc, 4, "lost");
        let fresh: Vec<u32> = c.owned_slots().into_iter().filter(|s| !before.contains(s)).collect();
        assert_eq!(fresh.len(), 1, "premise: the D0 write took one fresh slot");
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        drop(cc);
        let arena = arena_path(&db);
        let id = c.into_id();
        let incarnation = db.incarnation;
        drop(trunk);
        drop(db);
        // Its slot never reached the device.
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(&arena).unwrap();
        f.write_all_at(&vec![0u8; 4096], fresh[0] as u64 * 4096).unwrap();
        (id, incarnation)
    };
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    let c = db.branch(id).unwrap();
    let got = c.connect().and_then(|cc| cc.prepare("SELECT v FROM t WHERE id = 4").and_then(|mut s| s.run_collect_rows()));
    match got {
        Ok(rows) => assert_eq!(rows[0][0], crate::Value::from_text("kept"), "the dropped D0 flight's write was read"),
        Err(e) => panic!("a raised D0 cut confirmed a D0 flight it kept, and its lost slot was refused: {e}"),
    }
}

/// Review 6 #1: a header word that names the last flight's end frame but not where that flight
/// ends (the word as it was written before) confirms nothing: the flight is checked, and its lost
/// slot drops it, as for a flight never confirmed.
#[cfg(unix)]
#[test]
fn a_word_not_bound_to_the_last_flights_end_confirms_nothing() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("unbound.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::FullFsync));
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
            let log = db.branch_log_path().unwrap();
            let id = b.into_id();
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&arena).unwrap();
            f.write_all_at(&vec![0u8; 4096], fresh[0] as u64 * 4096).unwrap();
            let (_, last, _) = log_confirmation_at(&log);
            let l = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
            l.write_all_at(&last.to_le_bytes(), 36).unwrap();
            (id, incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::FullFsync), incarnation);
        let b = db.branch(id).unwrap();
        let got = b.connect().and_then(|c| c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows()));
        match got {
            Ok(rows) => assert_eq!(rows[0][0], crate::Value::from_text("durable"), "catalog={catalog}: the dropped flight's write was read"),
            Err(e) => panic!("catalog={catalog}: an unbound word confirmed the last flight, and its lost slot was refused: {e}"),
        }
    }
}

// ---- review 6 #2: a failed trunk WAL sync, or a replacement's, fail-stops the store ----

/// An IO whose WAL file's next sync fails, once (review 6 #2): `armed` 1 fails it at once, as
/// UnixIO's F_FULLFSYNC does on Apple, 2 fails its completion before returning it, 3 returns it
/// unfinished and fails it at the IO's next step, after the statement yielded on it (engine review
/// 9 #10); 4, 5 and 6 pass one sync and fail the next as 1, 2 and 3 do; it reads 0 once spent.
/// Every other file, and every other call, is the platform's.
struct FailWalSyncIo {
    inner: Arc<dyn IO>,
    armed: Arc<std::sync::atomic::AtomicU8>,
    /// Only a WAL whose path ends with this (`/<file name>-wal`) is armed (`None`: every `-wal`
    /// file): `open_failing_source_wal`. A suffix, as the IO sees the path canonicalized.
    only: Option<String>,
    /// Mode 3's sync, failed at the next `step`.
    held: Arc<std::sync::Mutex<Option<crate::Completion>>>,
}

struct FailWalSyncFile {
    inner: Arc<dyn crate::io::File>,
    armed: Option<Arc<std::sync::atomic::AtomicU8>>,
    held: Arc<std::sync::Mutex<Option<crate::Completion>>>,
}

impl crate::io::Clock for FailWalSyncIo {
    fn current_time_monotonic(&self) -> crate::io::clock::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::io::clock::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FailWalSyncIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> crate::Result<Arc<dyn crate::io::File>> {
        Ok(Arc::new(FailWalSyncFile {
            inner: self.inner.open_file(path, flags, direct)?,
            armed: (path.ends_with("-wal") && self.only.as_deref().is_none_or(|only| path.ends_with(only)))
                .then(|| self.armed.clone()),
            held: self.held.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> crate::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> crate::Result<()> {
        if let Some(c) = self.held.lock().unwrap().take() {
            c.error(crate::CompletionError::IOError(std::io::ErrorKind::Other, "sync"));
        }
        self.inner.step()
    }
    fn file_id(&self, path: &str) -> crate::Result<crate::io::FileId> {
        self.inner.file_id(path)
    }
}

impl crate::io::File for FailWalSyncFile {
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
        let failed = || crate::CompletionError::IOError(std::io::ErrorKind::Other, "sync");
        match self.armed.as_ref().map_or(0, |a| a.swap(0, std::sync::atomic::Ordering::AcqRel)) {
            1 => Err(crate::LimboError::CompletionError(failed())),
            2 => {
                c.error(failed());
                Ok(c)
            }
            3 => {
                *self.held.lock().unwrap() = Some(c.clone());
                Ok(c)
            }
            // This sync passes; the next one fails as mode 1 does (a site after another sync).
            4 => {
                if let Some(a) = self.armed.as_ref() {
                    a.store(1, std::sync::atomic::Ordering::Release);
                }
                self.inner.sync(c, sync_type)
            }
            // This sync passes; the next one fails as mode 2 (5) or mode 3 (6) does: a site after
            // another sync, failing after its issue (engine review 19 HIGH 1).
            m @ (5 | 6) => {
                if let Some(a) = self.armed.as_ref() {
                    a.store(m - 3, std::sync::atomic::Ordering::Release);
                }
                self.inner.sync(c, sync_type)
            }
            _ => self.inner.sync(c, sync_type),
        }
    }
    fn size(&self) -> crate::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: crate::Completion) -> crate::Result<crate::Completion> {
        self.inner.truncate(len, c)
    }
    fn full_fsync_device(&self) -> Option<u64> {
        self.inner.full_fsync_device()
    }
}

/// A store opened through `FailWalSyncIo`, its WAL's next sync failing once `armed` is set.
/// `open_failing_wal`, arming only the database's own WAL: the WAL of a database it opens on the
/// same IO (an in-place VACUUM's target build) is left alone.
fn open_failing_source_wal(path: &Path, opts: DatabaseOpts) -> (Arc<Database>, Arc<std::sync::atomic::AtomicU8>) {
    let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let io: Arc<dyn IO> = Arc::new(FailWalSyncIo {
        only: Some(format!("/{}-wal", path.file_name().unwrap().to_str().unwrap())),
        inner: Arc::new(PlatformIO::new().unwrap()),
        armed: armed.clone(),
        held: Arc::new(std::sync::Mutex::new(None)),
    });
    let db = Database::open_file_with_flags(io, path.to_str().unwrap(), OpenFlags::Create, opts, None, Arc::new(SqliteDialect))
        .unwrap();
    (db, armed)
}

fn open_failing_wal(path: &Path, opts: DatabaseOpts) -> (Arc<Database>, Arc<std::sync::atomic::AtomicU8>) {
    let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let io: Arc<dyn IO> = Arc::new(FailWalSyncIo {
        only: None,
        inner: Arc::new(PlatformIO::new().unwrap()),
        armed: armed.clone(),
        held: Arc::new(std::sync::Mutex::new(None)),
    });
    let db = Database::open_file_with_flags(io, path.to_str().unwrap(), OpenFlags::Create, opts, None, Arc::new(SqliteDialect))
        .unwrap();
    (db, armed)
}

/// Every fuzzy checkpoint runs its phase 4 (the catalog WAL's backfill and truncation) while this
/// lives: `CATALOG_WAL_TRUNCATE_AT_FORCED` at 1 byte, restored on drop.
struct CatalogWalTruncateAlways;

impl CatalogWalTruncateAlways {
    fn set() -> Self {
        super::store::CATALOG_WAL_TRUNCATE_AT_FORCED.store(1, std::sync::atomic::Ordering::Release);
        Self
    }
}

impl Drop for CatalogWalTruncateAlways {
    fn drop(&mut self) {
        super::store::CATALOG_WAL_TRUNCATE_AT_FORCED.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// `what` was refused because the store is fail-stopped.
fn assert_fail_stopped<T: std::fmt::Debug>(got: crate::Result<T>, what: &str) {
    match got {
        Err(e) => assert!(e.to_string().contains("fail-stopped"), "{what}: refused, but not as fail-stopped: {e}"),
        Ok(v) => panic!("{what}: acknowledged after a failed drain of the branch files' device: {v:?}"),
    }
}

/// Review 6 #2: a trunk commit whose barrier only ORDERED a pre-image ahead of its WAL F_FULLFSYNC
/// relies on that flush to make the pre-image durable. When the flush fails, what the device's
/// drain covered may be lost, and a later flush may report success over the loss: the store
/// fail-stops, as after a failed flight. Before, the commit's gate cleared `pending_full` and the
/// store went on: the next fork, and the next trunk commit, were acknowledged. Arms: the sync
/// failing at once (data_sync_retry off and on) and its completion failing (data_sync_retry on).
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_trunk_wal_sync_under_an_ordered_barrier_fail_stops_the_store() {
    let _s = serial();
    for catalog in [false, true] {
        for (mode, retry) in [(1u8, false), (1, true), (2, true)] {
            let what = format!("catalog={catalog} mode={mode} retry={retry}");
            let dir = tempfile::TempDir::new().unwrap();
            let (db, armed) = open_failing_wal(&dir.path().join("walfail.db"), opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            if retry {
                trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
            }
            armed.store(mode, std::sync::atomic::Ordering::Release);
            let before = sync_counts();
            let failed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
            let after = sync_counts();
            assert_eq!(armed.load(std::sync::atomic::Ordering::Acquire), 0, "{what}: premise: the WAL's sync was reached");
            assert!(failed.is_err(), "{what}: premise: the failed WAL sync failed the commit");
            assert!(after.barrier > before.barrier, "{what}: premise: the commit's pre-image was ordered, not flushed");
            assert_fail_stopped(db.connect().unwrap().fork_branch().map(|x| x.into_id()), &format!("{what}: the next fork"));
            assert_fail_stopped(
                db.connect().unwrap().execute("UPDATE t SET v = 'later' WHERE id = 9"),
                &format!("{what}: the next trunk commit with a live child"),
            );
            drop(b);
        }
    }
}

/// Engine review 9 #10: each site that acts on a failed trunk WAL sync is reached on its own, so
/// none can be lost behind another (the arms above all reach the commit's own two):
/// * mode 2, data_sync_retry off: the completion fails before the commit waits on it, and the
///   commit's inline check fail-stops the store before its panic;
/// * mode 3: the sync fails after the commit yielded on it, so the statement aborts without coming
///   back to the commit, and the commit gate's close acts on the noted sync;
/// * the WAL header's sync, reached by emptying the WAL first: at its issue (mode 1), and noted
///   (mode 2: the commit yields on the failed completion, as in mode 3).
///
/// Mutants (test builds only): `wal_fail_stop_not_inline` (the mode 2 commit arm) and
/// `wal_fail_stop_not_at_close` (mode 3 and the header's mode 2).
#[cfg(target_vendor = "apple")]
#[test]
fn every_trunk_wal_sync_failure_site_fail_stops_on_its_own() {
    let _s = serial();
    for catalog in [false, true] {
        for (header, mode, retry) in [
            (false, 2u8, false),
            (false, 3, false),
            (false, 3, true),
            (true, 1, true),
            (true, 2, true),
        ] {
            let what = format!("catalog={catalog} header={header} mode={mode} retry={retry}");
            let dir = tempfile::TempDir::new().unwrap();
            let (db, armed) = open_failing_wal(&dir.path().join("walsite.db"), opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            if retry {
                trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
            }
            if header {
                // The WAL is reset, so the next commit writes and syncs its header first.
                trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            armed.store(mode, std::sync::atomic::Ordering::Release);
            let before = sync_counts();
            let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                trunk.execute("UPDATE t SET v = 'new' WHERE id = 7")
            }));
            let after = sync_counts();
            assert_eq!(armed.load(std::sync::atomic::Ordering::Acquire), 0, "{what}: premise: the WAL's sync was reached");
            match failed {
                Ok(r) => assert!(r.is_err(), "{what}: premise: the failed WAL sync failed the commit"),
                Err(_) => assert!(
                    mode == 2 && !retry && !header,
                    "{what}: premise: only the commit's inline check panics (data_sync_retry off)"
                ),
            }
            assert!(after.barrier > before.barrier, "{what}: premise: the commit's pre-image was ordered, not flushed");
            assert_fail_stopped(db.connect().unwrap().fork_branch().map(|x| x.into_id()), &format!("{what}: the next fork"));
            drop(b);
        }
    }
}

/// Engine review 10 #5, rewritten for engine review 17 HIGH 1 (FLAGGED, pre-approved): the trunk
/// WAL syncs issued inside the WAL itself and at shutdown were not acted on: a checkpoint's sync of
/// the WAL before its backfill, a TRUNCATE checkpoint's sync of the truncated log, and the last
/// connection's shutdown sync. Each fails through `?` past the pager's noted sync, so a failed
/// drain of the device that holds branch records written and not yet drained (a D1 fork,
/// plain-fsynced on Apple) left the store running, and a later flush on any connection could
/// promote them. An explicit `PRAGMA wal_checkpoint` answers such a failure with a busy row, so the
/// failure is acted on where the sync is issued, never later. One arm per site and way in:
/// `pragma` (PRAGMA wal_checkpoint; premise: its busy row), `api` (Connection::checkpoint; premise:
/// its error), `shutdown` (the last connection's close). Both a fork from a fresh connection and
/// another connection's trunk commit are refused after it. Mutants (test builds only):
/// `checkpoint_sync_unwatched`, `truncate_sync_unwatched`, `shutdown_sync_unwatched`.
#[cfg(target_vendor = "apple")]
fn a_wal_internal_sync_failure_fail_stops(catalog: bool, site: &str, way: &str) {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let what = format!("catalog={catalog} site={site} way={way}");
    let dir = tempfile::TempDir::new().unwrap();
    let (db, armed) = open_failing_wal(&dir.path().join("walinternal.db"), opts(catalog, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
    // Frames in the WAL, then a D1 fork: written and plain-fsynced, not drained.
    trunk.execute("UPDATE t SET v = 'walled' WHERE id = 7").unwrap();
    let _b = trunk.fork_branch().unwrap().into_id();
    crate::storage::wal::WAL_SYNC_SITE.with(|s| s.set(""));
    // The checkpoint site fails the first WAL sync; the truncate site passes it and fails the next.
    armed.store(if site == "truncate" { 4 } else { 1 }, O::Release);
    match way {
        "pragma" => {
            let mode = if site == "truncate" { "TRUNCATE" } else { "PASSIVE" };
            let rows = trunk
                .prepare(format!("PRAGMA wal_checkpoint({mode})"))
                .and_then(|mut s| s.run_collect_rows())
                .unwrap_or_else(|e| panic!("{what}: premise: the PRAGMA answers rows: {e}"));
            assert_eq!(rows[0][0].as_int(), Some(1), "{what}: premise: the PRAGMA answered busy=1 for the failed sync");
        }
        "api" => {
            let mode = if site == "truncate" {
                crate::CheckpointMode::Truncate { upper_bound_inclusive: None }
            } else {
                crate::CheckpointMode::Passive { upper_bound_inclusive: None }
            };
            assert!(trunk.checkpoint(mode).is_err(), "{what}: premise: the failed sync failed the checkpoint");
        }
        _ => {
            assert!(trunk.close().is_err(), "{what}: premise: the failed sync failed the close");
        }
    }
    assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the armed sync was reached");
    assert_eq!(
        crate::storage::wal::WAL_SYNC_SITE.with(|s| s.get()),
        site,
        "{what}: premise: the failure reached its site"
    );
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        &format!("{what}: the next fork after a failed WAL-internal drain"),
    );
    assert_fail_stopped(
        db.connect().unwrap().execute("UPDATE t SET v = 'later' WHERE id = 9"),
        &format!("{what}: another connection's trunk commit after a failed WAL-internal drain"),
    );
}

/// Engine review 17 LOW 10: `wal_insert_frame` (the raw WAL API, open once no branch exists)
/// writes and syncs the WAL's header first when the WAL was truncated, and that sync was unwatched
/// while undrained branch records could remain. A D0 store over a fullfsync trunk (so the sync is
/// a drain on Apple too) with x's Release undrained, the WAL truncated, its header's sync failing:
/// the next fork is refused. Mutant `raw_header_sync_unwatched`.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_failed_raw_frame_header_sync_fail_stops_a_store_with_an_undrained_record() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("raw-header.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let page_size = trunk.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap() as usize;
        let mut frame = vec![0u8; 24 + page_size];
        trunk.wal_get_frame(1, &mut frame).unwrap();
        trunk.fork_branch().unwrap().reap().unwrap();
        assert!(drained(&db) < db.branches.durable_for_test(SyncClass::Off), "{what}: premise: x's Release is undrained");
        trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        trunk.wal_insert_begin().unwrap();
        armed.store(1, O::Release);
        let inserted = trunk.wal_insert_frame(1, &frame);
        assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the WAL header's sync was reached");
        assert!(inserted.is_err(), "{what}: premise: the failed header sync failed the insert");
        let _ = trunk.wal_insert_end(false);
        assert_fail_stopped(
            db.connect().unwrap().fork_branch().map(|x| x.into_id()),
            &format!("{what}: the next fork after a failed raw WAL header sync with an undrained Release"),
        );
    }
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_pragma_checkpoint_wal_sync_fail_stops_a_snapshot_store() {
    a_wal_internal_sync_failure_fail_stops(false, "checkpoint", "pragma");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_pragma_checkpoint_wal_sync_fail_stops_a_catalog_store() {
    a_wal_internal_sync_failure_fail_stops(true, "checkpoint", "pragma");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_pragma_truncate_wal_sync_fail_stops_a_snapshot_store() {
    a_wal_internal_sync_failure_fail_stops(false, "truncate", "pragma");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_pragma_truncate_wal_sync_fail_stops_a_catalog_store() {
    a_wal_internal_sync_failure_fail_stops(true, "truncate", "pragma");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_api_checkpoint_wal_sync_fail_stops_a_snapshot_store() {
    a_wal_internal_sync_failure_fail_stops(false, "checkpoint", "api");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_api_truncate_wal_sync_fail_stops_a_catalog_store() {
    a_wal_internal_sync_failure_fail_stops(true, "truncate", "api");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_shutdown_wal_sync_fail_stops_a_snapshot_store() {
    a_wal_internal_sync_failure_fail_stops(false, "shutdown", "close");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_shutdown_wal_sync_fail_stops_a_catalog_store() {
    a_wal_internal_sync_failure_fail_stops(true, "shutdown", "close");
}

/// Engine review 19 HIGH 1: 910dbf21c acts at issue on the syncs the WAL issues itself (a
/// checkpoint's sync before its backfill, a TRUNCATE checkpoint's of the truncated log), but no
/// longer notes a completion it returned, so one that fails after it yielded (io_uring, an
/// extension VFS) reached the checkpoint-failure path with nothing noted, and the store stayed
/// live over a failed drain. A D0 store, whose fork is written and never drained on every target,
/// under a fullfsync trunk (so the sync is a drain on Apple too); the armed sync's completion
/// fails before it is returned (mode 2) or after the statement yielded on it (mode 3), at the
/// checkpoint's sync, or at the truncate's once the checkpoint's passed (modes 5 and 6). The PRAGMA
/// reports it (an error, or a busy row) and the API returns an error; either way a fork from a
/// fresh connection and another connection's trunk commit are refused after it. Mutant
/// `yielded_wal_sync_unnoted` (test builds only).
fn a_yielded_wal_internal_sync_failure_fail_stops(catalog: bool, site: &str, way: &str, mode: u8) {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let what = format!("catalog={catalog} site={site} way={way} mode={mode}");
    let dir = tempfile::TempDir::new().unwrap();
    let (db, armed) = open_failing_wal(&dir.path().join("walyield.db"), opts(catalog, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
    // Frames in the WAL, then a D0 fork: written, not drained.
    trunk.execute("UPDATE t SET v = 'walled' WHERE id = 7").unwrap();
    let _b = trunk.fork_branch().unwrap().into_id();
    assert!(
        drained(&db) < db.branches.durable_for_test(SyncClass::Off),
        "{what}: premise: the fork is written and not drained"
    );
    crate::storage::wal::WAL_SYNC_SITE.with(|s| s.set(""));
    armed.store(if site == "truncate" { mode + 3 } else { mode }, O::Release);
    let truncate = site == "truncate";
    match way {
        "pragma" => {
            let checkpoint = if truncate { "TRUNCATE" } else { "PASSIVE" };
            let reported = trunk
                .prepare(format!("PRAGMA wal_checkpoint({checkpoint})"))
                .and_then(|mut s| s.run_collect_rows());
            assert!(
                reported.as_ref().map_or(true, |rows| rows[0][0].as_int() == Some(1)),
                "{what}: premise: the PRAGMA reported the failed sync (an error, or busy=1): {reported:?}"
            );
        }
        _ => {
            let checkpoint = if truncate {
                crate::CheckpointMode::Truncate { upper_bound_inclusive: None }
            } else {
                crate::CheckpointMode::Passive { upper_bound_inclusive: None }
            };
            assert!(trunk.checkpoint(checkpoint).is_err(), "{what}: premise: the failed sync failed the checkpoint");
        }
    }
    assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the armed sync was reached");
    assert_eq!(
        crate::storage::wal::WAL_SYNC_SITE.with(|s| s.get()),
        site,
        "{what}: premise: the failure reached its site"
    );
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        &format!("{what}: the next fork after a WAL-internal drain that failed after it yielded"),
    );
    assert_fail_stopped(
        db.connect().unwrap().execute("UPDATE t SET v = 'later' WHERE id = 9"),
        &format!("{what}: another connection's trunk commit after a WAL-internal drain that failed after it yielded"),
    );
}

#[test]
fn a_yielded_checkpoint_wal_sync_failure_reported_by_the_pragma_fail_stops() {
    for catalog in [false, true] {
        for mode in [2, 3] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "checkpoint", "pragma", mode);
        }
    }
}

#[test]
fn a_yielded_checkpoint_wal_sync_failure_reported_by_the_api_fail_stops() {
    for catalog in [false, true] {
        for mode in [2, 3] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "checkpoint", "api", mode);
        }
    }
}

#[test]
fn a_yielded_truncate_wal_sync_failure_reported_by_the_pragma_fail_stops() {
    for catalog in [false, true] {
        for mode in [2, 3] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "truncate", "pragma", mode);
        }
    }
}

#[test]
fn a_yielded_truncate_wal_sync_failure_reported_by_the_api_fail_stops() {
    for catalog in [false, true] {
        for mode in [2, 3] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "truncate", "api", mode);
        }
    }
}

/// Engine review 8 #14 (review 3 #9's pager half): a trunk commit with no frames to sync whose
/// barrier ORDERED a Release (its child just released, so the trunk is childless and the commit
/// gate-less) relied on a WAL F_FULLFSYNC that never comes. The pager drops the `pending_full` it
/// set when the commit ends (`close_trunk_gate`), or the release's waiter stalls for the whole
/// pending-full wait (60 s here) before it leads its own flush. The store-level red never built a
/// pager, so it could not reach that drop. Mutant `ordered_pending_kept`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_frameless_trunk_commit_that_ordered_a_release_stalls_no_waiter() {
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::PENDING_FULL_WAIT_MS.store(0, std::sync::atomic::Ordering::Release);
        }
    }
    let _restore = Restore;
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("frameless.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let x = trunk.fork_branch().unwrap();
        // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): a FULL commit first
        // leaves the WAL synced. The seed's commits ran before the PRAGMA, unsynced, so the WAL was
        // dirty, and a commit with no frames still syncs a dirty WAL (pager WaitSync: frames or
        // `wal.is_dirty()`, for write_frame_raw's barrier): the commit below took an F_FULLFSYNC and
        // the test died on its premise at bb6af4d94 and 9d27592bf (artie raws l1-branch*).
        write_v(&trunk, 1, "clean");
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_RELEASE_BUFFERED, std::sync::atomic::Ordering::Release);
        let release = std::thread::spawn(move || x.reap().map(|_| ()));
        wait_hold(&hold, super::store::HOLD_RELEASE_BUFFERED);
        super::store::PENDING_FULL_WAIT_MS.store(60_000, std::sync::atomic::Ordering::Release);
        let before = sync_counts();
        trunk.execute("UPDATE t SET v = v WHERE id = -1").unwrap();
        let after = sync_counts();
        let t = std::time::Instant::now();
        hold.store(0, std::sync::atomic::Ordering::Release);
        release.join().unwrap().unwrap();
        let waited = t.elapsed();
        super::store::PENDING_FULL_WAIT_MS.store(0, std::sync::atomic::Ordering::Release);
        assert!(after.barrier > before.barrier, "catalog={catalog}: premise: the commit's barrier ordered the Release");
        assert_eq!(
            after.full_fsync - before.full_fsync,
            0,
            "catalog={catalog}: premise: the frameless commit's WAL took no F_FULLFSYNC"
        );
        assert!(
            waited < std::time::Duration::from_secs(10),
            "catalog={catalog}: the release's waiter stalled ({waited:?}) on a WAL flush a frameless commit never makes"
        );
    }
}

/// Review 6 #2, the control: a trunk commit whose pre-image was made durable by a flush of its own
/// (fullfsync off: nothing ordered, nothing noted to ride the WAL's sync) does not fail-stop the
/// branch store when its WAL sync fails: the branch files never relied on it.
#[test]
fn a_failed_trunk_wal_sync_that_carried_nothing_leaves_the_store_running() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("walctl.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let b = trunk.fork_branch().unwrap();
        trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
        armed.store(1, std::sync::atomic::Ordering::Release);
        let failed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
        assert_eq!(armed.load(std::sync::atomic::Ordering::Acquire), 0, "catalog={catalog}: premise: the WAL's sync was reached");
        assert!(failed.is_err(), "catalog={catalog}: premise: the failed WAL sync failed the commit");
        db.connect().unwrap().fork_branch().unwrap_or_else(|e| panic!("catalog={catalog}: the store stopped over a WAL sync it never relied on: {e}"));
        assert_eq!(read_v(&b.connect().unwrap(), 7), "trunk-7", "catalog={catalog}: the child's fork point");
    }
}

/// Review 6 #2: a temp file that is to replace a branch file failing its sync fail-stops the store.
/// On Apple an F_FULLFSYNC that fails is a failed drain of the whole device, so what earlier flights
/// only barriered or plain-fsynced may be lost with it, and a later sync would report success over
/// the loss (PostgreSQL panics on any fsync failure for the same reason). Before, the rewrite
/// failed, the old file stayed, and the store went on. A snapshot store's compaction; a catalog
/// store's fuzzy checkpoint, whose cut is prepared off the mutex.
#[test]
fn a_replacement_that_fails_its_sync_fail_stops_the_store() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("replfail.db"),
            opts(catalog, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _second = trunk.fork_branch().unwrap().into_id();
        db.branch_failpoint(Some(BranchFailpoint::ReplacementSyncFails));
        if catalog {
            let installed = db.branch_checkpoint_counters()[0];
            assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
            db.branch_checkpoint_wait();
            assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "premise: the checkpoint's catalog commit installed");
        } else {
            assert!(db.branch_compact_now().is_err(), "premise: the compaction's snapshot failed its sync");
        }
        assert_fail_stopped(trunk.fork_branch().map(|x| x.into_id()), &format!("catalog={catalog}: the next fork"));
    }
}

// ---- review 6 #3: a fail-stop stops flights in the air; no checkpoint arena sync outside the group ----

/// Review 6 #3 (b): a fuzzy checkpoint in a D1 or D2 store makes the slots it captured durable by
/// waiting for the group's flights, which sync the arena themselves: it takes no arena handle of
/// its own. Its own sync ran outside the group, beside a flight's sync of the same file — on Linux
/// one fsync of an open file description can consume the error another would have reported, and the
/// flight then lands as durable — and the capture paid a dup under the store mutex for it. Captured
/// here with a branch commit's flight in the air, which made the capture take a handle.
#[test]
fn a_fuzzy_checkpoint_takes_no_arena_handle_of_its_own() {
    let _s = serial();
    for class in [SyncClass::Fsync, SyncClass::FullFsync] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("nohandle.db"),
            opts(true, class).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let y = trunk.fork_branch().unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, std::sync::atomic::Ordering::Release);
        let writer = std::thread::spawn(move || {
            let r = y.connect()?.execute("UPDATE t SET v = 'y' WHERE id = 5");
            let _ = y.into_id();
            r
        });
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let handles = || super::store::CAPTURE_ARENA_HANDLES.load(std::sync::atomic::Ordering::Relaxed);
        let before = handles();
        let started = db.branch_checkpoint_fuzzy_now();
        hold.store(0, std::sync::atomic::Ordering::Release);
        assert!(started.unwrap(), "{class:?}: premise: a fuzzy checkpoint started");
        writer.join().unwrap().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(handles(), before, "{class:?}: the fuzzy capture took an arena handle to sync outside the group");
    }
}

/// Review 6 #3 (b): in a D0 store whose log was raised (a trunk commit under a synchronous trunk
/// made its pre-image durable), the slots a fuzzy checkpoint captures were written by D0 flights,
/// which sync nothing; the catalog commit must not name them before they are on the device. They
/// are synced before the commit, under the group's exclusion: at the commit the arena holds no
/// write the group has not synced.
///
/// FLAGGED TEST EDIT (doc only; engine review 9 #8): this said a flight of the group in Fsync
/// syncs them. Since engine review 9 #8 the checkpoint syncs the arena alone, under group
/// exclusion (`settle_arena`). No assertion changed.
#[test]
fn a_raised_d0_fuzzy_checkpoint_syncs_its_slots_through_the_group_before_its_commit() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("d0raised.db"),
        opts(true, SyncClass::Off).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("UPDATE t SET v = 'trunk-new' WHERE id = 9").unwrap();
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    let c = trunk.fork_branch().unwrap();
    write_v(&c.connect().unwrap(), 4, "d0");
    assert!(db.branches.arena_dirty(), "premise: the D0 commit left its slot unsynced");
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never arrived", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let dirty = db.branches.arena_dirty();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert!(!dirty, "at the catalog commit the arena held D0 writes no flight of the group had synced");
    drop((b, c));
}

// ---- engine review 9 #8: a raised-D0 fuzzy checkpoint syncs the arena only, and only when dirty ----

/// A D0 catalog store whose log a trunk commit raised (engine review 9 #8), with a D0 branch commit
/// since: its slot is written and synced by no flight. Returns the database and its trunk, and the
/// branches kept alive.
fn raised_d0_with_an_unsynced_slot(dir: &Path) -> (Arc<Database>, Arc<Connection>, Vec<BranchId>) {
    let db = open_at(
        &dir.join("d0settle.db"),
        opts(true, SyncClass::Off).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let b = trunk.fork_branch().unwrap();
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("UPDATE t SET v = 'trunk-new' WHERE id = 9").unwrap();
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    let c = trunk.fork_branch().unwrap();
    write_v(&c.connect().unwrap(), 4, "d0");
    assert!(db.branches.arena_dirty(), "premise: the D0 commit left its slot unsynced");
    (db, trunk, vec![b.into_id(), c.into_id()])
}

/// Engine review 9 #8: in a D0 store whose log was raised, a fuzzy checkpoint settled its capture by
/// leading a flight of the group in Fsync: an fsync of the arena, which the catalog commit needs,
/// and an fsync of the whole log, which nothing needs (the cut is about to supersede those bytes).
/// It syncs the arena alone: one sync.
#[cfg(unix)]
#[test]
fn a_raised_d0_fuzzy_checkpoint_settles_with_one_arena_sync_and_no_log_sync() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, _trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    let before = db.branches.settle_syncs_for_test();
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branches.settle_syncs_for_test() - before,
        1,
        "the checkpoint's settle synced more than the arena it needs"
    );
}

/// Engine review 9 #8: the same store with no slot written since the arena's last sync: the settle
/// has nothing to sync (lead review 1 item 7(4)), yet it led an Fsync flight that synced the log.
#[cfg(unix)]
#[test]
fn a_raised_d0_fuzzy_checkpoint_over_a_clean_arena_settles_with_no_sync() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the first fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert!(!db.branches.arena_dirty(), "premise: the first checkpoint synced the arena");
    // A fork writes no slot; it gives the next checkpoint a row to capture.
    let _d = trunk.fork_branch().unwrap();
    assert!(!db.branches.arena_dirty(), "premise: nothing was written into the arena since");
    let before = db.branches.settle_syncs_for_test();
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the second fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branches.settle_syncs_for_test() - before,
        0,
        "the checkpoint's settle synced a file with no slot to sync"
    );
}

/// Engine review 9 #8: while a raised-D0 fuzzy checkpoint's settle sync is in progress, a D0 create
/// waited for it: the settle was a flight of the group, so every operation needing a flight waited
/// for its two fsyncs instead of writing its record. Held here at the settle's sync, a create on
/// another connection must be acknowledged before the hold is released.
#[test]
fn a_d0_create_does_not_wait_for_a_raised_checkpoints_arena_sync() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, _trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    db.branch_checkpoint_hold(super::store::HOLD_FLIGHT_TAKEN);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint's settle never took its sync", || {
        db.branch_checkpoint_held() == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
    });
    let (tx, rx) = std::sync::mpsc::channel();
    let db2 = db.clone();
    let creator = std::thread::spawn(move || {
        let created = db2.connect().and_then(|t| t.fork_branch()).map(|x| x.into_id());
        let _ = tx.send(created.is_ok());
    });
    let created = rx.recv_timeout(std::time::Duration::from_secs(10));
    db.branch_checkpoint_hold(0);
    creator.join().unwrap();
    db.branch_checkpoint_wait();
    assert_eq!(
        created,
        Ok(true),
        "a D0 create waited for the checkpoint's settle sync (or failed)"
    );
}

/// Engine review 9 #8: while a raised-D0 checkpoint syncs the arena outside any flight, no flight
/// that syncs is taken. The arena's dirty mark is cleared by then, so such a flight would sync the
/// log alone and land its records durable over slots whose sync has not returned. Held at that
/// arena sync, a wait for Fsync durability does not return; once released, it does. Mutant
/// `arena_sync_ungated`.
#[test]
fn a_syncing_flight_waits_for_a_raised_checkpoints_arena_sync() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, _trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    db.branch_checkpoint_hold(super::store::HOLD_FLIGHT_TAKEN);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never took its arena sync", || {
        db.branch_checkpoint_held() == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
    });
    let lsn = db.branches.log_lsn_for_test();
    let (tx, rx) = std::sync::mpsc::channel();
    let db2 = db.clone();
    let waiter = std::thread::spawn(move || {
        let _ = tx.send(db2.branches.wait_durable(lsn, SyncClass::Fsync).is_ok());
    });
    let early = rx.recv_timeout(std::time::Duration::from_millis(300));
    db.branch_checkpoint_hold(0);
    let late = rx.recv_timeout(std::time::Duration::from_secs(10));
    waiter.join().unwrap();
    db.branch_checkpoint_wait();
    assert_eq!(
        early,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "a flight that syncs landed while the checkpoint's arena sync was in progress"
    );
    assert_eq!(late, Ok(true), "the wait for Fsync did not return once the arena sync landed");
}

/// Engine review 9 #8 (review 3 #5): the arena sync a raised-D0 fuzzy checkpoint makes itself
/// fail-stops the store when it fails, as a failed flight's does: a later sync of the file may
/// report success for pages this one lost. Mutant `checkpoint_sync_error_kept`.
#[test]
fn a_failed_arena_sync_in_a_raised_d0_fuzzy_checkpoint_fail_stops() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, _keep) = raised_d0_with_an_unsynced_slot(dir.path());
    db.branch_failpoint(Some(BranchFailpoint::ArenaSyncFails));
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert_fail_stopped(
        trunk.fork_branch().map(|x| x.into_id()),
        "the next fork after a checkpoint's arena sync failed",
    );
}

// ---- review 6 #6: a kernel that promotes an unsupported barrier itself gets no userland retry ----

/// Forces the Darwin major version `barrier_file` takes the kernel for, for one test (review 6 #6).
#[cfg(target_vendor = "apple")]
struct DarwinMajor;

#[cfg(target_vendor = "apple")]
impl DarwinMajor {
    fn force(major: u32) -> Self {
        super::journal::DARWIN_MAJOR_FORCED.store(major, std::sync::atomic::Ordering::Release);
        Self
    }
}

#[cfg(target_vendor = "apple")]
impl Drop for DarwinMajor {
    fn drop(&mut self) {
        super::journal::DARWIN_MAJOR_FORCED.store(0, std::sync::atomic::Ordering::Release);
    }
}

/// Review 6 #6: from Darwin 23 (macOS 14) the kernel itself turns an F_BARRIERFSYNC the file
/// system does not support into an F_FULLFSYNC, and latches that per mount (xnu-10002 on). So an
/// ENOTSUP, EOPNOTSUPP, EINVAL or ENOTTY that reaches the store is that full sync's own failure,
/// and the userland fallback re-issued it: the fsyncgate retry, which can report success for pages
/// the failed call lost. On such a kernel every failed barrier fails its flight, the trunk commit
/// relying on it is refused, and the store fail-stops; nothing falls back.
///
/// FLAGGED TEST EDIT (own tests, engine review 9 #19): an EIO arm here (mutant
/// `barrier_retries_any` kills it), and in both barrier tests a premise that the commit took the
/// armed errno, so an arm cannot pass without reaching a barrier or leak its errno onward.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_barrier_on_a_kernel_that_promotes_it_fail_stops_whatever_its_errno() {
    let _s = serial();
    let _k = DarwinMajor::force(23);
    for catalog in [false, true] {
        for (errno, name) in [
            (libc::EIO, "EIO"),
            (libc::ENOTSUP, "ENOTSUP"),
            (libc::EOPNOTSUPP, "EOPNOTSUPP"),
            (libc::EINVAL, "EINVAL"),
            (libc::ENOTTY, "ENOTTY"),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let db = open_at(&dir.path().join("barrier-new-kernel.db"), opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let b = trunk.fork_branch().unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            super::journal::fail_next_barrier(errno);
            let before = sync_counts();
            let committed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
            let after = sync_counts();
            assert_eq!(
                super::journal::barrier_errno_pending(),
                0,
                "catalog={catalog} {name}: premise: the commit took the armed barrier"
            );
            assert_eq!(
                after.barrier_fallback - before.barrier_fallback,
                0,
                "catalog={catalog} {name}: a failed barrier was retried on a kernel that promotes it"
            );
            assert!(committed.is_err(), "catalog={catalog} {name}: a trunk commit relied on a barrier that failed");
            let mine = b.connect().and_then(|bc| bc.execute("UPDATE t SET v = 'mine' WHERE id = 3"));
            assert!(mine.is_err(), "catalog={catalog} {name}: the store did not fail-stop after a failed barrier");
        }
    }
}


// ---- engine review 7 #3: a stopped store reports what a reopen would ----

/// Engine review 7 #3 (review 3 #17's release half): a fail-stopped store lists, and finds by
/// name, a branch whose Release never became durable, because a reopen brings it back. Before,
/// the Release's apply took the name and the listing entry away before its flight was written,
/// so after that flight failed the stopped store reported the branch absent and its name free.
/// Both a named branch (dropped by name) and an unnamed one (reaped).
#[test]
fn a_stopped_store_lists_and_finds_a_branch_whose_release_failed() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("relfail.db");
        let (named, unnamed, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Fsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            let named = trunk.create_branch("goes-away").unwrap();
            let x = trunk.fork_branch().unwrap();
            let unnamed = x.id();
            db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
            assert!(db.drop_branch("goes-away").is_err(), "premise: the named release's flight failed");
            assert!(x.reap().is_err(), "catalog={catalog}: premise: the store is stopped");
            let listed = db.branch_ids().unwrap();
            assert!(listed.contains(&named), "catalog={catalog}: a branch a reopen brings back is not listed");
            assert!(listed.contains(&unnamed), "catalog={catalog}: a reaped branch a reopen brings back is not listed");
            assert_eq!(
                db.branch_named("goes-away").unwrap(),
                Some(named),
                "catalog={catalog}: the stopped store reports a name free that a reopen still holds"
            );
            (named, unnamed, db.incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
        let listed = db.branch_ids().unwrap();
        assert!(listed.contains(&named) && listed.contains(&unnamed), "catalog={catalog}: premise: the reopen brings both back");
        assert_eq!(db.branch_named("goes-away").unwrap(), Some(named), "catalog={catalog}: premise");
    }
}

// ---- engine review 7 #6: a detector for the cut's directory sync ----

/// Engine review 7 #6: a cut's rename is made durable by exactly one directory sync, before the
/// first acknowledgement after it, and never by one per flight. A fuzzy checkpoint's cut
/// (`finish_cut`) leaves it to the next flight; a sharp one (`rewrite_from`) syncs the directory
/// itself. Mutant `no_cut_dir_sync` (the next flight syncs no directory) must fail the fuzzy arm.
/// Counted on this thread, which runs the sharp cut and leads the flights after it.
#[test]
fn a_cut_syncs_its_directory_once_before_the_next_acknowledgement() {
    let _s = serial();
    for (fuzzy, mode) in [(true, super::BranchCheckpoint::Fuzzy), (false, super::BranchCheckpoint::Sharp)] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("cutdir.db"), opts(true, SyncClass::Fsync).with_branch_checkpoint(mode));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _a = trunk.fork_branch().unwrap().into_id();
        let dirs = || super::journal::DIR_SYNCS.with(|c| c.get());
        let installed = db.branch_checkpoint_counters()[0];
        let before = dirs();
        if fuzzy {
            assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
            db.branch_checkpoint_wait();
        } else {
            db.branch_compact_now().unwrap();
        }
        assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "fuzzy={fuzzy}: premise: the checkpoint installed");
        let _b = trunk.fork_branch().unwrap().into_id();
        assert_eq!(
            dirs() - before,
            1,
            "fuzzy={fuzzy}: the cut's rename was not made durable by exactly one directory sync before the next acknowledgement"
        );
        let _c = trunk.fork_branch().unwrap().into_id();
        assert_eq!(dirs() - before, 1, "fuzzy={fuzzy}: a directory sync per flight");
    }
}

// ---- engine review 7 #7: the install's forget_listed, reached on purpose ----

/// Engine review 7 #7 (review 4 #11): a fuzzy checkpoint captured while a release's flight is in
/// the air lists that release's slots free in the catalog, and the install takes them out of
/// memory once (`forget_listed`): free exactly once, in use nowhere, reused by a new branch. The
/// release lands before the install with NO maturing call in between (every listing accessor
/// matures first, which is why the older test never reached `forget_listed`); probes here read
/// the deferred frees and the arena's bitmap without maturing. Mutants `forget_listed_kept_in_use`
/// (the install leaves them counted in use) and `deferred_matured_at_capture` (the capture frees
/// them before their release is durable) must each fail it on a claim.
#[test]
fn the_install_forgets_a_captured_deferred_free_exactly_once() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("forget-listed.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let anchor = trunk.fork_branch().unwrap();
    write_v(&anchor.connect().unwrap(), 1, "anchor");
    let anchor = anchor.into_id();
    let x = trunk.fork_branch().unwrap();
    write_v(&x.connect().unwrap(), 3, "x");
    let owned_x = x.owned_slots();
    assert!(!owned_x.is_empty(), "premise: x owns a slot");
    // x's release is in the air at the capture.
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
    let release = std::thread::spawn(move || x.reap().map(|_| ()));
    wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    for &slot in &owned_x {
        assert!(
            !db.branches.arena_slot_free_for_test(slot),
            "slot {slot} was freed at the capture, before the release that frees it was durable"
        );
    }
    hold.store(0, O::Release);
    release.join().unwrap().unwrap();
    eventually("the checkpoint never arrived", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let deferred = db.branches.deferred_slots_for_test();
    assert!(
        owned_x.iter().all(|s| deferred.contains(s)),
        "premise: x's frees still wait at the install ({deferred:?}, x owned {owned_x:?})"
    );
    let forgotten = db.branches.forget_listed_for_test();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert!(
        db.branches.forget_listed_for_test() - forgotten >= owned_x.len() as u64,
        "premise: the install took x's listed slots out of memory"
    );
    let in_use = db.branch_slots_in_use();
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as usize,
        in_use.len(),
        "after the install: the in-use count disagrees with the slots in use"
    );
    for slot in &owned_x {
        assert!(!in_use.contains(slot), "slot {slot} is in use after its release and the install");
    }
    let z = trunk.fork_branch().unwrap();
    write_v(&z.connect().unwrap(), 5, "z");
    assert!(
        z.owned_slots().iter().any(|s| owned_x.contains(s)),
        "a new branch did not reuse a slot the install listed free (z owns {:?}, x owned {owned_x:?})",
        z.owned_slots()
    );
    let _ = (anchor, z.into_id());
}

/// Engine review 7 #7 (c), as the flagged-edit judge of 268e9e053 asked: a release buffered AFTER
/// a fuzzy checkpoint's capture, its flight held from before the install, is not the install's to
/// list or forget. The install waits for that flight (no flight may write the log it cuts), and
/// then leaves its frees to mature on their own: no `forget_listed` call (a per-store count, read
/// without maturing), and once matured y's slots are free, counted in use nowhere. Mutant
/// `install_forgets_after_capture`.
#[test]
fn the_install_leaves_a_release_after_its_capture_to_mature_on_its_own() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("after-capture.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let anchor = trunk.fork_branch().unwrap();
    write_v(&anchor.connect().unwrap(), 1, "anchor");
    let anchor = anchor.into_id();
    let y = trunk.fork_branch().unwrap();
    write_v(&y.connect().unwrap(), 4, "y");
    let owned_y = y.owned_slots();
    assert!(!owned_y.is_empty(), "premise: y owns a slot");
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("the checkpoint never arrived", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let forgotten = db.branches.forget_listed_for_test();
    // y's release, buffered after the capture, its flight held across the install. Nothing below
    // takes the store mutex until the flight is let go: the install holds it while it waits.
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
    let release = std::thread::spawn(move || y.reap().map(|_| ()));
    wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
    db.branch_checkpoint_hold(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    hold.store(0, O::Release);
    release.join().unwrap().unwrap();
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branches.forget_listed_for_test() - forgotten,
        0,
        "the install forgot frees of a release it did not capture"
    );
    let in_use = db.branch_slots_in_use();
    for slot in &owned_y {
        assert!(!in_use.contains(slot), "slot {slot} is in use after y's release matured");
        assert!(db.branch_slot_is_free(*slot), "slot {slot} is not free after y's release matured");
    }
    assert_eq!(
        db.branch_stats().unwrap().arena_slots_in_use as usize,
        in_use.len(),
        "the in-use count disagrees with the slots in use"
    );
    let _ = anchor;
}

// ---- engine review 7 #12: the release half of the listing's durability wait ----

/// Engine review 7 #12 (review 3 #17's release half): a listing never omits a branch whose Release
/// is still in the air: it waits for that Release to be durable (or lists the branch). A crash
/// before the flight lands brings the branch back, so a listing that omitted it would report
/// state a crash undoes. Mutant `list_no_release_wait` (the listing waits for forks only) must
/// fail it: its listing returns at once, without the branch.
#[test]
fn a_listing_waits_out_a_release_in_the_air() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("listrel.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _anchor = trunk.fork_branch().unwrap().into_id();
        let x = trunk.fork_branch().unwrap();
        let id = x.id();
        assert!(db.branch_ids().unwrap().contains(&id), "catalog={catalog}: premise: x is listed");
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
        let release = std::thread::spawn(move || x.reap().map(|_| ()));
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        let lister = {
            let db = db.clone();
            std::thread::spawn(move || db.branch_ids())
        };
        let t = std::time::Instant::now();
        while !lister.is_finished() && t.elapsed() < std::time::Duration::from_millis(300) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let early = lister.is_finished();
        hold.store(0, O::Release);
        release.join().unwrap().unwrap();
        let listed = lister.join().unwrap().unwrap();
        if early {
            assert!(
                listed.contains(&id),
                "catalog={catalog}: a listing taken while x's Release was in the air omitted x"
            );
        }
    }
}

// ---- engine review 8 #1: a retried checkpoint does not stall every operation ----

/// Engine review 8 #1: after a fuzzy checkpoint fails to start (its capture or its spawn), the
/// retry, a threshold's worth of log later, starts past twice the threshold, which was measured
/// from the last cut. Every guarded operation then found the store past its hard limit and waited
/// in back-pressure for that whole checkpoint's install (up to 60 s). The hard limit is measured
/// from where the retry was due: an operation during the retry's flight does not wait. Held at
/// its commit, the retry is in flight; a create from another thread returns within 1 s.
#[test]
fn a_retried_checkpoint_does_not_stall_every_operation() {
    let _s = serial();
    for fp in [BranchFailpoint::CaptureFails, BranchFailpoint::SpawnFails] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(
            &dir.path().join("retry-stall.db"),
            opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
        );
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _first = trunk.fork_branch().unwrap().into_id();
        let _t = Threshold::set(8 << 10);
        db.branch_failpoint(Some(fp));
        let entered = || db.branches.captures_entered_for_test();
        let failed_at = entered();
        let t = std::time::Instant::now();
        while entered() == failed_at {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "{fp:?}: premise: no checkpoint was attempted");
        }
        // The retry: held at its commit once it starts.
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        let started = db.branch_checkpoint_counters()[1];
        let t = std::time::Instant::now();
        while db.branch_checkpoint_counters()[1] == started {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "{fp:?}: premise: the checkpoint was never retried");
        }
        eventually(&format!("{fp:?}: the retry never reached its hold"), || {
            db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
        });
        let other = {
            let db = db.clone();
            std::thread::spawn(move || db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id()))
        };
        let t = std::time::Instant::now();
        while !other.is_finished() && t.elapsed() < std::time::Duration::from_secs(1) {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let finished = other.is_finished();
        let stalled = db.branches.over_hard_for_test();
        db.branch_checkpoint_hold(0);
        other.join().unwrap().unwrap();
        db.branch_checkpoint_wait();
        assert!(finished, "{fp:?}: a create waited for the retried checkpoint's install");
        assert!(!stalled, "{fp:?}: the retried checkpoint started past the hard limit");
    }
}

// ---- engine review 8 #2: a failed capture does no O(dirty) work ----

/// Engine review 8 #2: a capture that fails (here at its read snapshot) has built no catalog row:
/// its fallible steps come before it takes the dirty set. Before, it took the set, built a row
/// for every dirty branch, then failed and threw them away, under the store mutex, at every retry:
/// under a persistent fault N creates cost O(N^2 / threshold). Mutant `capture_rows_first` (as
/// before) must fail it.
#[test]
fn a_failed_capture_builds_no_row() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("capture-rows.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for _ in 0..20 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    // FLAGGED instrument change (L2(d) slice 2): a fuzzy capture builds no row since 94b34fa3b;
    // its rows are encoded on the writer's thread (`FROZEN_ROWS_WRITTEN`), so both counters are
    // read: the capture's own (this thread) and the writer's. The claim is unchanged.
    let rows = || super::store::CAPTURE_ROWS_BUILT.load(std::sync::atomic::Ordering::Relaxed) + super::store::FROZEN_ROWS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
    let entered = || db.branches.captures_entered_for_test();
    db.branch_failpoint(Some(BranchFailpoint::CaptureFails));
    let (rows0, entered0) = (rows(), entered());
    assert!(!db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the capture failed");
    assert_eq!(entered(), entered0 + 1, "premise: the capture was entered");
    assert_eq!(rows() - rows0, 0, "a failed capture built rows for the dirty branches and threw them away");
    // And the next one, healthy, captures those branches.
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "no checkpoint started after the failed capture");
    db.branch_checkpoint_wait();
    assert!(rows() - rows0 >= 20, "premise: the healthy capture wrote the 20 dirty branches' rows");
}

// ---- engine review 9 #2 and #5: which failed trunk WAL syncs fail-stop the branch store ----

/// Engine review 9 #2: a failed F_FULLFSYNC of the trunk's WAL HEADER (a cache flush or a spill
/// after the WAL was reset, not only a commit) is a failed drain of the branch files' device too:
/// branch records written and not yet drained by a full flush (here a D1 fork, only plain-fsynced)
/// may be lost with it, and a later successful flush would promote them as durable. The store
/// fail-stops. Before, only the commit's own syncs did.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_wal_header_sync_in_a_cache_flush_fail_stops_a_store_with_undrained_records() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("hdrfail.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let _b = trunk.fork_branch().unwrap().into_id();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        // The WAL is reset, so the next flush of a dirty page writes and syncs its header first.
        trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        trunk.execute("BEGIN").unwrap();
        trunk.execute("UPDATE t SET v = 'flushed' WHERE id = 7").unwrap();
        armed.store(1, std::sync::atomic::Ordering::Release);
        let flushed = trunk.cacheflush();
        assert_eq!(armed.load(std::sync::atomic::Ordering::Acquire), 0, "catalog={catalog}: premise: the cache flush synced the WAL's header");
        assert!(flushed.is_err(), "catalog={catalog}: premise: the header's sync failed the cache flush");
        let _ = trunk.execute("ROLLBACK");
        assert_fail_stopped(
            db.connect().unwrap().fork_branch().map(|x| x.into_id()),
            &format!("catalog={catalog}: the next fork after a failed drain of undrained records"),
        );
    }
}

/// Engine review 9 #5: in D2 (the registered target: Apple, a fullfsync trunk on the branch files'
/// device), a failed trunk WAL F_FULLFSYNC with no branch record undrained (every flight already
/// F_FULLFSYNCed, nothing ordered, nothing in the air) puts nothing at risk, and the store goes
/// on. Before, the frontier the commit noted was the Fsync-durable one, nonzero after any flight,
/// so one WAL sync error refused every create until a restart.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_wal_sync_with_nothing_undrained_leaves_a_d2_store_running() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("d2ctl.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let x = trunk.fork_branch().unwrap();
        x.reap().unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
        armed.store(1, std::sync::atomic::Ordering::Release);
        let failed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 7");
        assert_eq!(armed.load(std::sync::atomic::Ordering::Acquire), 0, "catalog={catalog}: premise: the WAL's sync was reached");
        assert!(failed.is_err(), "catalog={catalog}: premise: the failed WAL sync failed the commit");
        db.connect()
            .unwrap()
            .fork_branch()
            .unwrap_or_else(|e| panic!("catalog={catalog}: one WAL sync error with nothing at risk refused the next create: {e}"));
    }
}

/// Engine review 10 #6: a raised D0 store relies on its Releases being durable before a trunk
/// commit (the barrier asks for max(store class, trunk class)), and they are at best plain-fsynced
/// (a settle, a rewrite): on Apple not drained. A failed drain of the device then may lose a
/// Release the next commit promotes as durable, so the store fail-stops; the at-risk test counted
/// undrained records only in a store whose own class syncs. Raised outside a flight, as a failed
/// trunk WAL F_FULLFSYNC raises it. Mutant `drain_risk_by_store_class`.
#[test]
fn a_failed_drain_fail_stops_a_raised_d0_store_with_an_undrained_release() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, _slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("d0-drain.db"), catalog, false);
        x.reap().unwrap();
        assert!(db.branches.rewrite_class_for_test().syncs(), "catalog={catalog}: premise: the log is raised");
        db.connect()
            .unwrap()
            .fork_branch()
            .unwrap_or_else(|e| panic!("catalog={catalog}: premise: the store runs before the failed drain: {e}"));
        db.branches.trunk_wal_sync_failed(true);
        assert_fail_stopped(
            db.connect().unwrap().fork_branch().map(|x| x.into_id()),
            &format!("catalog={catalog}: the next fork after a failed drain of an undrained Release"),
        );
    }
}

/// The log prefix a drain of the device has covered, as `trunk_wal_sync_failed` reads it: on Apple
/// only an F_FULLFSYNC drains (fsync(2) does not), elsewhere any sync does.
fn drained(db: &Database) -> u64 {
    let full = db.branches.durable_for_test(SyncClass::FullFsync);
    if cfg!(target_vendor = "apple") {
        full
    } else {
        db.branches.durable_for_test(SyncClass::Fsync).max(full)
    }
}

/// `raised_d0_with_a_kept_pre_image`, then drained (engine review 17 HIGH 2): its trunk commit's
/// barrier is a plain fsync, which on Apple drains nothing, so the fixture alone left every record
/// undrained there, and a drain-failure test passed whatever its route wrote. One wait in the full
/// class (an F_FULLFSYNC on Apple, an fsync elsewhere) drains them all, the TrunkRetain and any
/// Release included, so a route's own record is what a failed drain finds at risk.
fn drained_raised_d0(path: &Path, catalog: bool, x_writes: bool) -> (Arc<Database>, Arc<Connection>, Branch, Vec<u32>) {
    let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(path, catalog, x_writes);
    db.branches
        .wait_durable(db.branches.durable_for_test(SyncClass::Off), SyncClass::FullFsync)
        .unwrap();
    assert_eq!(
        db.branches.durable_for_test(SyncClass::FullFsync),
        db.branches.durable_for_test(SyncClass::Off),
        "catalog={catalog}: premise: every record the fixture wrote is drained"
    );
    let retain = db.branches.retain_floor_for_test();
    assert!(retain > 0, "catalog={catalog}: premise: the fixture's trunk commit logged a TrunkRetain");
    assert!(retain <= drained(&db), "catalog={catalog}: premise: the fixture's TrunkRetain is drained");
    assert!(db.branches.last_release_lsn_for_test() <= drained(&db), "catalog={catalog}: premise: no Release is undrained");
    (db, trunk, x, slots)
}

/// What a route's premises assert of the barrier floor's marks and the newest fork, each against
/// the drained prefix: `true` for the one mark the route's record moved past it.
fn undrained_marks(db: &Database, what: &str, release: bool, retain: bool, fork: bool) {
    let drained = drained(db);
    assert!(db.branches.durable_for_test(SyncClass::Off) > drained, "{what}: premise: the route's record is not drained");
    for (name, mark, past) in [
        ("Release", db.branches.last_release_lsn_for_test(), release),
        ("TrunkRetain", db.branches.retain_floor_for_test(), retain),
        ("fork", db.branches.last_fork_lsn_for_test(), fork),
    ] {
        assert_eq!(mark > drained, past, "{what}: premise: the newest {name} is undrained = {past}");
    }
}

/// The failed drain itself, and the store's answer to the next fork.
fn the_next_fork_after_a_failed_drain_is_refused(db: &Arc<Database>, what: &str) {
    db.branches.trunk_wal_sync_failed(true);
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        &format!("{what}: the next fork after a failed drain of an undrained record"),
    );
}

/// Engine review 15 HIGH 1: recovery replays the branch log as a prefix and stops at the first
/// damaged frame, so ANY record written and not drained when a drain fails is a hole every later
/// record sits behind, not only a Release. Route (i), a TrunkRetain: a trunk commit that does not
/// sync keeps a pre-image for x over the drained fixture, and the store fail-stops on a failed
/// drain. Split from routes (ii) and (iii) by engine review 17 HIGH 2, so each route kills alone.
/// Mutants `drain_risk_by_store_class` and `drain_risk_release_only` (the review 10 #6 predicate).
#[test]
fn a_failed_drain_fail_stops_a_d0_store_with_an_undrained_trunk_retain() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("route=retain catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, _slots) = drained_raised_d0(&dir.path().join("d0-undrained-retain.db"), catalog, false);
        let other = db.connect().unwrap();
        other.execute("PRAGMA synchronous = OFF").unwrap();
        let before = db.branch_slots_in_use().len();
        write_v(&other, 50, "undrained");
        assert!(db.branch_slots_in_use().len() > before, "{what}: premise: the commit kept a pre-image for x");
        undrained_marks(&db, &what, false, true, false);
        the_next_fork_after_a_failed_drain_is_refused(&db, &what);
        drop(x);
    }
}

/// Engine review 15 HIGH 1, route (ii): a fork over the drained fixture, its record undrained, and
/// neither a Release nor a TrunkRetain past the drain. Mutants `drain_risk_by_store_class`,
/// `drain_risk_release_only` and `drain_risk_barrier_floor_only` (Releases and TrunkRetains only).
#[test]
fn a_failed_drain_fail_stops_a_d0_store_with_an_undrained_fork() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("route=fork catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, _slots) = drained_raised_d0(&dir.path().join("d0-undrained-fork.db"), catalog, false);
        let _y = trunk.fork_branch().unwrap().into_id();
        undrained_marks(&db, &what, false, false, true);
        the_next_fork_after_a_failed_drain_is_refused(&db, &what);
        drop(x);
    }
}

/// Engine review 15 HIGH 1, route (iii): x's second commit over the drained fixture, holding the
/// slot it superseded, its record undrained, and no Release, TrunkRetain or fork past the drain:
/// the one route whose record no mark the predicate could read names. Mutants
/// `drain_risk_by_store_class`, `drain_risk_release_only`, `drain_risk_barrier_floor_only` and
/// `drain_risk_ignores_held_frees` (Releases, TrunkRetains and forks, each by its mark).
#[test]
fn a_failed_drain_fail_stops_a_d0_store_with_an_undrained_commit_holding_a_free() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("route=commit catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, slots) = drained_raised_d0(&dir.path().join("d0-undrained-commit.db"), catalog, true);
        write_v(&x.connect().unwrap(), 40, "again");
        assert!(!db.branch_slot_is_free(slots[0]), "{what}: premise: x's superseded slot is held");
        undrained_marks(&db, &what, false, false, false);
        the_next_fork_after_a_failed_drain_is_refused(&db, &what);
        drop(x);
    }
}

/// Engine review 10 #6 over the drained fixture (engine review 17 HIGH 2): x's Release, its record
/// undrained, and no TrunkRetain or fork past the drain. The undrained fixture made
/// `a_failed_drain_fail_stops_a_raised_d0_store_with_an_undrained_release` pass on Apple whatever
/// its route; this one fails there only if the Release is not counted. Mutant
/// `drain_risk_by_store_class`.
#[test]
fn a_failed_drain_fail_stops_a_d0_store_with_an_undrained_release() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("route=release catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, _slots) = drained_raised_d0(&dir.path().join("d0-undrained-release.db"), catalog, false);
        x.reap().unwrap();
        undrained_marks(&db, &what, true, false, false);
        the_next_fork_after_a_failed_drain_is_refused(&db, &what);
    }
}

/// Engine review 15 #6, the control: a D0 store whose every record is drained puts nothing at
/// risk, and a failed drain leaves it running. The drain is the fixture's own full-class wait
/// (engine review 17 HIGH 2): its trunk commit's plain fsync drains nothing on Apple, where this
/// control stopped. Mutant `drain_failure_ignores_risk` (it always stops).
#[test]
fn a_failed_drain_with_nothing_undrained_leaves_a_d0_store_running() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let (db, _trunk, x, _slots) = drained_raised_d0(&dir.path().join("d0-drained.db"), catalog, false);
        assert_eq!(
            db.branches.durable_for_test(SyncClass::Off),
            drained(&db),
            "catalog={catalog}: premise: nothing is undrained at the failed drain"
        );
        db.branches.trunk_wal_sync_failed(true);
        db.connect()
            .unwrap()
            .fork_branch()
            .unwrap_or_else(|e| panic!("catalog={catalog}: a failed drain with nothing undrained stopped a D0 store: {e}"));
        drop(x);
    }
}

/// Engine review 17 MED 3: a catalog checkpoint's own WAL syncs (its PASSIVE and TRUNCATE phase 4,
/// on the branch files' device) only logged a failure: the catalog's Database has a volatile store
/// of its own, which is where its pager reported, and phase 4 holds only the writer, so records can
/// be undrained then. A failed catalog sync that drains the device (an F_FULLFSYNC on Apple, any
/// sync elsewhere) goes through the main store's at-risk decision: here a raised (FullFsync on
/// Apple, Fsync elsewhere) D0 catalog store with an undrained fork made while its fuzzy checkpoint waits after the catalog
/// commit, and the catalog's next WAL sync failing. The next fork is refused. Mutant
/// `catalog_sync_failure_logged`.
#[test]
fn a_failed_catalog_wal_sync_fail_stops_a_store_with_an_undrained_record() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): phase 4 runs only once
    // the catalog WAL reaches CATALOG_WAL_TRUNCATE_AT (fa0897b69); this test fails phase 4's WAL sync,
    // so it forces every checkpoint's (measured: its premise failed at 5344a5878, artie raw
    // l17-branch_5344a5878.txt). No assertion changed.
    let _always = CatalogWalTruncateAlways::set();
    let dir = tempfile::TempDir::new().unwrap();
    let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
    // FLAGGED TEST EDIT (engine review 21 LOW 21, fixture only): the hook is keyed by the catalog's path.
    *super::catalog::NEXT_CATALOG_IO.lock().unwrap() = Some((
        std::path::PathBuf::from("cat-sync.db-branch-cat"),
        Arc::new(FailWalSyncIo {
            only: None,
            inner: Arc::new(PlatformIO::new().unwrap()),
            armed: armed.clone(),
            held: Arc::new(std::sync::Mutex::new(None)),
        }),
    ));
    let db = open_at(&dir.path().join("cat-sync.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    let _x = trunk.fork_branch().unwrap().into_id();
    assert!(super::catalog::NEXT_CATALOG_IO.lock().unwrap().is_none(), "premise: the store's catalog opened through the failing IO");
    write_v(&trunk, 3, "raised");
    // FLAGGED TEST EDIT (Codex C07 MED 1, a premise made portable; for the lead's ruling): `PRAGMA
    // fullfsync` exists on Apple platforms only (translate/pragma.rs), so elsewhere the FULL trunk
    // commit raises the log to Fsync, and there any sync drains the device (`drain_failed`): the
    // claim is reached in the class the platform has. It required FullFsync everywhere, so on
    // Linux the test died on this premise (cloud raw catsync-premise-repair.txt).
    let raised = if cfg!(target_vendor = "apple") { SyncClass::FullFsync } else { SyncClass::Fsync };
    assert_eq!(db.branches.rewrite_class_for_test(), raised, "premise: the trunk commit raised the log to {raised:?}");
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("premise: the checkpoint's catalog commit never landed", || {
        db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED
    });
    let _y = db.connect().unwrap().fork_branch().unwrap().into_id();
    assert!(drained(&db) < db.branches.durable_for_test(SyncClass::Off), "premise: the fork's record is undrained");
    armed.store(1, O::Release);
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(armed.load(O::Acquire), 0, "premise: the catalog's phase 4 reached its WAL sync");
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        "the next fork after a failed catalog WAL sync with an undrained fork",
    );
}

/// Engine review 17 MED 3, the D2 arm: every record of a D2 store is drained by its own flight, so
/// what a failed catalog sync puts at risk there is a flight in the air. A fork's flight is held
/// once taken (`HOLD_FLIGHT_TAKEN`) while the fuzzy checkpoint's phase 4 runs (held before it,
/// `HOLD_BEFORE_WAL_TRUNCATE`, so the install's quiesce is behind it) and the catalog's WAL sync
/// fails: the store fail-stops, the held fork is refused, and so is the next. Mutant
/// `catalog_sync_failure_logged`.
#[test]
fn a_failed_catalog_wal_sync_fail_stops_a_d2_store_with_a_flight_in_the_air() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): phase 4 now runs only
    // once the catalog WAL reaches CATALOG_WAL_TRUNCATE_AT (checkpoint FIX); this test holds a
    // checkpoint inside it, so it forces every checkpoint's. No assertion changed.
    let _always = CatalogWalTruncateAlways::set();
    let dir = tempfile::TempDir::new().unwrap();
    let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
    // FLAGGED TEST EDIT (engine review 21 LOW 21, fixture only): the hook is keyed by the catalog's path.
    *super::catalog::NEXT_CATALOG_IO.lock().unwrap() = Some((
        std::path::PathBuf::from("cat-sync-d2.db-branch-cat"),
        Arc::new(FailWalSyncIo {
            only: None,
            inner: Arc::new(PlatformIO::new().unwrap()),
            armed: armed.clone(),
            held: Arc::new(std::sync::Mutex::new(None)),
        }),
    ));
    let db = open_at(&dir.path().join("cat-sync-d2.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _x = trunk.fork_branch().unwrap().into_id();
    assert!(super::catalog::NEXT_CATALOG_IO.lock().unwrap().is_none(), "premise: the store's catalog opened through the failing IO");
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_WAL_TRUNCATE);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("premise: the checkpoint never installed", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_WAL_TRUNCATE | super::store::HOLD_ARRIVED
    });
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
    let forker = db.connect().unwrap();
    let forking = std::thread::spawn(move || forker.fork_branch().map(|x| x.into_id()));
    eventually("premise: the fork's flight was never taken", || {
        hold.load(O::Acquire) == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
    });
    armed.store(1, O::Release);
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(armed.load(O::Acquire), 0, "premise: the catalog's phase 4 reached its WAL sync");
    hold.store(0, O::Release);
    assert_fail_stopped(forking.join().unwrap(), "the fork in the air when the catalog's WAL sync failed");
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        "the next fork after a failed catalog WAL sync with a flight in the air",
    );
}

/// Engine review 21 MED 5: the failed catalog sync was decided late, once the PASSIVE backfill and
/// the whole TRUNCATE had returned, so a flight in the air when the sync failed could land in
/// between (accepted: the store was not yet fail-stopped), and the late decision then saw nothing
/// at risk. The D2 arm above with the fork's flight released between the PASSIVE backfill, whose
/// sync fails, and the TRUNCATE (`HOLD_AFTER_WAL_PASSIVE`): the fork in the air must be refused,
/// and so must the next. Mutant `catalog_sync_failure_decided_late`.
#[test]
fn a_failed_catalog_wal_sync_is_decided_before_a_flight_in_the_air_lands() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): phase 4 now runs only
    // once the catalog WAL reaches CATALOG_WAL_TRUNCATE_AT (checkpoint FIX); this test holds a
    // checkpoint inside it, so it forces every checkpoint's. No assertion changed.
    let _always = CatalogWalTruncateAlways::set();
    let dir = tempfile::TempDir::new().unwrap();
    let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
    // FLAGGED TEST EDIT (engine review 21 LOW 21, fixture only): the hook is keyed by the catalog's path.
    *super::catalog::NEXT_CATALOG_IO.lock().unwrap() = Some((
        std::path::PathBuf::from("cat-sync-late.db-branch-cat"),
        Arc::new(FailWalSyncIo {
            only: None,
            inner: Arc::new(PlatformIO::new().unwrap()),
            armed: armed.clone(),
            held: Arc::new(std::sync::Mutex::new(None)),
        }),
    ));
    let db = open_at(&dir.path().join("cat-sync-late.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _x = trunk.fork_branch().unwrap().into_id();
    assert!(super::catalog::NEXT_CATALOG_IO.lock().unwrap().is_none(), "premise: the store's catalog opened through the failing IO");
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_WAL_TRUNCATE);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("premise: the checkpoint never installed", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_WAL_TRUNCATE | super::store::HOLD_ARRIVED
    });
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
    let forker = db.connect().unwrap();
    let forking = std::thread::spawn(move || forker.fork_branch().map(|x| x.into_id()));
    eventually("premise: the fork's flight was never taken", || {
        hold.load(O::Acquire) == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
    });
    armed.store(1, O::Release);
    // Moving the hook releases phase 4's first stop; it stops again after the PASSIVE backfill.
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_WAL_PASSIVE);
    eventually("premise: phase 4 never finished its PASSIVE backfill", || {
        db.branch_checkpoint_held() == super::store::HOLD_AFTER_WAL_PASSIVE | super::store::HOLD_ARRIVED
    });
    assert_eq!(armed.load(O::Acquire), 0, "premise: the PASSIVE backfill reached the catalog's WAL sync");
    hold.store(0, O::Release);
    let forked = forking.join().unwrap();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_fail_stopped(forked, "the fork whose flight landed after the catalog's WAL sync failed");
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        "the next fork after the catalog's WAL sync failed with a flight in the air",
    );
}

/// Engine review 11 MED 8: a raised D0 store's fuzzy checkpoint settles in the store's class
/// (Off), so it took an ORDERED flight in the air (landed, its trunk commit's WAL F_FULLFSYNC still
/// to come) as settled, and committed the catalog over records whose callers may yet be told they
/// failed (review 4 #1's rule). It waits for that flush first: here the flush fails, the store
/// fail-stops, and the catalog does not commit. Mutant `settle_ignores_ordered`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_raised_d0_fuzzy_checkpoint_waits_out_an_ordered_flight_in_the_air() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    // FLAGGED TEST EDIT (engine review 18 MED 4, a strengthening; for the lead's ruling): at the
    // 100 ms default bound the checkpoint failed Busy about 1.9 s before the trunk commit's armed
    // WAL failure ran, so the doc's path (the flush fails, the store fail-stops) was never reached
    // and a Busy-at-once settle passed. The bound is 60 s here, and the fail-stop is asserted.
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::PENDING_FULL_WAIT_MS.store(0, O::Release);
        }
    }
    let _restore = Restore;
    super::store::PENDING_FULL_WAIT_MS.store(60_000, O::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let (db, armed) = open_failing_wal(&dir.path().join("settle-ordered.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    trunk.execute("PRAGMA data_sync_retry = 1").unwrap();
    let _x = trunk.fork_branch().unwrap().into_id();
    write_v(&trunk, 3, "raised");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    let installed = db.branch_checkpoint_counters()[0];
    // A trunk commit keeping a pre-image for x: its flight ordered and landed, its WAL flush to come.
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_TRUNK_BARRIER_DONE, O::Release);
    let committing = std::thread::spawn(move || {
        armed.store(1, O::Release);
        let got = trunk.execute("UPDATE t SET v = 'ordered' WHERE id = 40").map(|_| ());
        (got, armed.load(O::Acquire))
    });
    wait_hold(&hold, super::store::HOLD_TRUNK_BARRIER_DONE);
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    let t = std::time::Instant::now();
    let mut committed = false;
    while t.elapsed() < std::time::Duration::from_secs(2) {
        if db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED {
            committed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    hold.store(0, O::Release);
    let (got, armed_left) = committing.join().unwrap();
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert_eq!(armed_left, 0, "premise: the trunk commit's WAL sync was reached");
    assert!(got.is_err(), "premise: its failed WAL sync failed the commit");
    assert!(
        !committed,
        "a fuzzy checkpoint committed the catalog while an ordered flight's WAL flush was still to come"
    );
    assert_eq!(
        db.branch_checkpoint_counters()[0],
        installed,
        "a fuzzy checkpoint installed over an ordered flight whose WAL flush failed"
    );
    assert_fail_stopped(
        db.connect().unwrap().fork_branch().map(|x| x.into_id()),
        "the next fork after the ordered flight's WAL flush failed under the settling checkpoint",
    );
}

// ---- engine review 9 #3: a flush under the store mutex honours a refused landing ----

/// Engine review 9 #3: a flush made under the store mutex (`flush_locked`: a lease, an expiry, a
/// reap) whose flight is in the air when another path fail-stops the store lands refused, and its
/// caller gets the error: nothing it would apply on success (the lease, freed slots) is applied.
/// Before, `land` refused the landing but `flush_locked` returned Ok and the lease was set. Mutant
/// `locked_flush_ignores_refusal` (as before) must fail it.
#[test]
fn a_locked_flush_refused_at_its_landing_fails_its_operation() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("lockedflush.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let x = trunk.fork_branch().unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_LOCKED_FLUSH, O::Release);
        let leasing = std::thread::spawn(move || x.lease(std::time::Duration::from_secs(3600)).map(|()| x));
        wait_hold(&hold, super::store::HOLD_LOCKED_FLUSH);
        // Raised outside the flight, as a failed drain of the device raises it.
        db.branches.trunk_wal_sync_failed(true);
        hold.store(0, O::Release);
        let got = leasing.join().unwrap();
        assert!(got.is_err(), "catalog={catalog}: a lease whose flush landed refused was acknowledged");
    }
}

/// Engine review 9 #3, the end-to-end arm review 6 #3 asked for: a branch commit whose group flight
/// is in the air when the store fail-stops outside it is refused (its flight's landing is refused).
#[test]
fn a_commit_whose_flight_lands_after_a_fail_stop_is_refused() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("inair.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let y = trunk.fork_branch().unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
        let writer = std::thread::spawn(move || {
            let r = y.connect()?.execute("UPDATE t SET v = 'y' WHERE id = 5");
            let _ = y.into_id();
            r
        });
        wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
        db.branches.trunk_wal_sync_failed(true);
        hold.store(0, O::Release);
        assert!(writer.join().unwrap().is_err(), "catalog={catalog}: a commit whose flight landed after a fail-stop was acknowledged");
    }
}

// ---- engine review 9 #6: a sharp checkpoint's commit makes what it captured durable ----

/// Engine review 9 #6: a sharp catalog checkpoint captures a fork still buffered (its waiter could
/// not lead a flight: the checkpoint holds the store mutex), and its catalog commit makes that fork
/// durable. A failure of the cut after the commit fail-stops the store, but the fork was durable
/// before it: its waiter is told so, and a reopen has it. Before, the cut's failure reached the
/// waiter as the fail-stop error, for a fork the next open brings back.
#[test]
fn a_sharp_checkpoint_whose_cut_fails_still_acknowledges_what_it_committed() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("sharpcut.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _anchor = trunk.fork_branch().unwrap().into_id();
    // op1's flight in the air; op2 buffered behind it, its waiter waiting.
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_FLIGHT_TAKEN, O::Release);
    let op1 = {
        let db = db.clone();
        std::thread::spawn(move || db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id()))
    };
    wait_hold(&hold, super::store::HOLD_FLIGHT_TAKEN);
    let flights = db.branches.group_counters()[0];
    let lsn = db.branches.log_lsn_for_test();
    let op2 = {
        let db = db.clone();
        std::thread::spawn(move || db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id()))
    };
    eventually("premise: op2 never buffered its fork", || db.branches.log_lsn_for_test() > lsn);
    db.branch_failpoint(Some(BranchFailpoint::ReplacementSyncFails));
    let sharp = {
        let db = db.clone();
        std::thread::spawn(move || db.branch_compact_now())
    };
    // The sharp checkpoint takes the store mutex and waits out op1's flight before it captures.
    std::thread::sleep(std::time::Duration::from_millis(200));
    hold.store(0, O::Release);
    op1.join().unwrap().unwrap();
    assert!(sharp.join().unwrap().is_err(), "premise: the sharp checkpoint's cut failed");
    assert_eq!(
        db.branches.group_counters()[0] - flights,
        0,
        "premise: op2 led no flight of its own (the checkpoint captured it buffered)"
    );
    let got = op2.join().unwrap();
    assert!(got.is_ok(), "a fork the checkpoint's catalog commit made durable was reported failed: {:?}", got.err());
    assert_fail_stopped(trunk.fork_branch().map(|x| x.into_id()), "the next fork after the failed cut");
}

// ---- engine 2b: a trunk fork inside a DDL commit's schema window ----

/// A DDL commit on `db`'s trunk parked at `HOLD_SCHEMA_PUBLISH`, its pages published and its schema
/// not (engine review 16 #14: per store, on `trunk_commit_hold`, where the process-wide
/// `SCHEMA_PUBLISH_HOLD` could catch any test's DDL commit). Released when dropped, so a failed
/// assertion releases the DDL commit too.
struct SchemaPublishHold(Arc<std::sync::atomic::AtomicU8>);

impl SchemaPublishHold {
    fn arm(db: &Database) -> Self {
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_SCHEMA_PUBLISH, std::sync::atomic::Ordering::Release);
        Self(hold)
    }

    fn arrived(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire) == super::store::HOLD_SCHEMA_PUBLISH | super::store::HOLD_ARRIVED
    }

    fn release(&self) {
        self.0.store(0, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for SchemaPublishHold {
    fn drop(&mut self) {
        self.release();
    }
}

/// Engine 2b (lead ruling 2026-10-08; fastest-linux C0 run 37518195105: 24 of 1,440 creates;
/// fastest-wire's branch_creates_racing_trunk_ddl_all_succeed: XX000): a DDL commit publishes its
/// pages, with the new schema cookie, before it publishes its schema (`Pager::commit_tx`). A trunk
/// fork in that window found neither its connection's schema nor the shared one at the cookie its
/// snapshot reads, and returned SchemaUpdated for the caller to retry. The fork must succeed, with
/// the schema its pages need: on its first-child path (under the WAL write lock) and lock-free.
/// Mutant `fork_refuses_schema_window`.
#[test]
fn a_trunk_fork_inside_a_ddl_commits_schema_window_succeeds() {
    let _s = serial();
    for (catalog, first) in [(false, true), (false, false), (true, true), (true, false)] {
        let what = format!("catalog={catalog} first={first}");
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("schema-window.db"), opts(catalog, SyncClass::Fsync));
        let ddl = db.connect().unwrap();
        seed(&ddl);
        let forker = db.connect().unwrap();
        // A live child already: the fork registers lock-free. None: it is the trunk's first child.
        let _live = (!first).then(|| forker.fork_branch().unwrap().into_id());
        let published = db.clone_schema().schema_version;
        let window = SchemaPublishHold::arm(&db);
        let alter = std::thread::spawn(move || {
            ddl.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7").map(|_| ())
        });
        eventually(&format!("{what}: premise: the DDL commit never reached its schema publication"), || window.arrived());
        assert_eq!(db.clone_schema().schema_version, published, "{what}: premise: the DDL's schema is not yet published at the fork");
        let name = format!("window-{catalog}-{first}");
        let forked = forker.create_branch(&name);
        window.release();
        alter.join().unwrap().unwrap();
        assert_ne!(db.clone_schema().schema_version, published, "{what}: premise: the DDL commit published its schema after the fork");
        forked.unwrap_or_else(|e| panic!("{what}: a fork inside a DDL commit's schema window failed: {e}"));
        let branch = db.connect_named(&name).unwrap();
        let rows = branch
            .prepare("SELECT c FROM t WHERE id = 1")
            .and_then(|mut s| s.run_collect_rows())
            .unwrap_or_else(|e| panic!("{what}: the branch's schema is not the one its pages need: {e}"));
        assert_eq!(rows[0][0].as_int(), Some(7), "{what}: the branch read the new column wrong");
    }
}

/// Engine review 16 MED 8: a fork in a DDL commit's schema window re-read the whole schema
/// (`reparse_schema`: every sqlite_schema row, the sequences, types and stats) into the forker's
/// connection, on the first-child path in the lock-free pre-pass, and its cost grows with the
/// schema. The child is registered with no schema instead, as any fork after a trunk commit is,
/// and reads its own at its first connection: the create parses no schema row, under the WAL write
/// lock or not, and leaves the forker's connection its schema. Counted on the forking thread
/// (`budget_probe::sql_counts`). Mutant `fork_rereads_schema_window`.
#[test]
fn a_fork_inside_a_ddl_commits_schema_window_parses_no_schema() {
    let _s = serial();
    for (catalog, first) in [(false, true), (false, false), (true, true), (true, false)] {
        let what = format!("catalog={catalog} first={first}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("schema-window-cost.db");
        let db = open_at(&path, opts(catalog, SyncClass::Fsync));
        let ddl = db.connect().unwrap();
        seed(&ddl);
        let forker = db.connect().unwrap();
        // FLAGGED TEST EDIT (a premise repair, ruled ACCEPTED by the lead 2026-10-15T03:14:51Z in
        // fastest DECISIONS): a catalog store creates its catalog at its first fork, and that
        // one-time creation parses the catalog's own 14 schema rows (cloud gdb, 2026-10-10; budgets
        // L6 covers its cost). The catalog first=true arm forks and reaps a throwaway child before
        // the window, so the catalog exists and the window's fork is still the trunk's first live
        // child.
        if catalog && first {
            forker.fork_branch().unwrap().reap().unwrap();
            assert!(
                std::path::PathBuf::from(format!("{}-branch-cat", path.display())).exists(),
                "{what}: premise: the catalog exists before the window"
            );
        }
        let _live = (!first).then(|| forker.fork_branch().unwrap().into_id());
        let version = forker.schema.read().schema_version;
        let published = db.clone_schema().schema_version;
        let window = SchemaPublishHold::arm(&db);
        let alter = std::thread::spawn(move || {
            ddl.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7").map(|_| ())
        });
        eventually(&format!("{what}: premise: the DDL commit never reached its schema publication"), || window.arrived());
        assert_eq!(db.clone_schema().schema_version, published, "{what}: premise: the DDL's schema is not yet published at the fork");
        let name = format!("window-cost-{catalog}-{first}");
        let parsed = super::budget_probe::sql_counts().schema_rows;
        let forked = forker.create_branch(&name);
        let parsed = super::budget_probe::sql_counts().schema_rows - parsed;
        let kept = forker.schema.read().schema_version;
        window.release();
        alter.join().unwrap().unwrap();
        assert_ne!(db.clone_schema().schema_version, published, "{what}: premise: the DDL commit published its schema after the fork");
        forked.unwrap_or_else(|e| panic!("{what}: premise: the fork inside the window succeeded: {e}"));
        assert_eq!(parsed, 0, "{what}: the fork parsed {parsed} schema rows inside the window");
        assert_eq!(kept, version, "{what}: the fork replaced its forker's schema");
        let branch = db.connect_named(&name).unwrap();
        let rows = branch
            .prepare("SELECT c FROM t WHERE id = 1")
            .and_then(|mut s| s.run_collect_rows())
            .unwrap_or_else(|e| panic!("{what}: the child's first connection did not read the schema its pages need: {e}"));
        assert_eq!(rows[0][0].as_int(), Some(7), "{what}: the branch read the new column wrong");
    }
}

// ---- review 6 #7: an ordered last flight is not checked like an unconfirmed one ----

/// Review 6 #7: an ORDERED flight (a trunk commit's pre-image barriered ahead of the trunk WAL's
/// F_FULLFSYNC, so never confirmed) that is the log's last flight had its slots checked at open as
/// an unconfirmed flight's are, so a damaged TrunkRetain slot dropped the flight silently and the
/// child read the trunk's newer page. Its slots were barriered ahead of its records, so its
/// records being there proves its slots were: a slot failing then is damage, refused when read.
/// Mutant `ordered_tag_ignored`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_of_an_ordered_last_flight_is_refused_when_read() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ordered-last.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::FullFsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            let x = trunk.fork_branch().unwrap();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let barriers = sync_counts().barrier;
            write_v(&trunk, 3, "new");
            assert!(sync_counts().barrier > barriers, "catalog={catalog}: premise: the trunk commit's pre-image was ordered");
            let kept: Vec<u32> = db.branch_slots_in_use().into_iter().filter(|s| !before.contains(s)).collect();
            assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the trunk commit kept one pre-image for x");
            let arena = arena_path(&db);
            let id = x.into_id();
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            // The kept pre-image's slot is damaged on the device after its flight was ordered.
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().read(true).write(true).open(&arena).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact_at(&mut byte, kept[0] as u64 * 4096 + 100).unwrap();
            f.write_all_at(&[byte[0] ^ 0xFF], kept[0] as u64 * 4096 + 100).unwrap();
            (id, incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::FullFsync), incarnation);
        let got = db
            .branch(id)
            .unwrap()
            .connect()
            .and_then(|c| c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows()));
        assert!(
            got.is_err(),
            "catalog={catalog}: a damaged slot of an ordered last flight was dropped silently, the child read {got:?}"
        );
    }
}

/// Review 6 #7's rule where the confirmation does not hide it (the 2026-10-15 mutant sweep:
/// `ordered_tag_ignored` survived the test above, whose clean close writes the ordered flight's
/// word, proved by the trunk's WAL flush since engine review 21 MEDs 7-8, so the open checks no slot
/// whatever the tag). The same schedule, the word cleared from the log's header after the close, as
/// a crash before the confirmation writer wrote it leaves it: the ordered tag alone keeps the open
/// from dropping the flight on its damaged slot, and the read is refused. Mutant
/// `ordered_tag_ignored`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_of_an_unconfirmed_ordered_last_flight_is_refused_when_read() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ordered-unconfirmed.db");
        let (id, incarnation, log) = {
            let db = open_at(&path, opts(catalog, SyncClass::FullFsync));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            let x = trunk.fork_branch().unwrap();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let barriers = sync_counts().barrier;
            write_v(&trunk, 3, "new");
            assert!(sync_counts().barrier > barriers, "catalog={catalog}: premise: the trunk commit's pre-image was ordered");
            let kept: Vec<u32> = db.branch_slots_in_use().into_iter().filter(|s| !before.contains(s)).collect();
            assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the trunk commit kept one pre-image for x");
            let arena = arena_path(&db);
            let log = db.branch_log_path().expect("a durable store");
            let id = x.into_id();
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().read(true).write(true).open(&arena).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact_at(&mut byte, kept[0] as u64 * 4096 + 100).unwrap();
            f.write_all_at(&[byte[0] ^ 0xFF], kept[0] as u64 * 4096 + 100).unwrap();
            (id, incarnation, log)
        };
        // As a crash before the confirmation writer: no word in the header.
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
            f.write_all_at(&[0u8; 4], 36).unwrap();
        }
        assert_eq!(log_confirmation_at(&log).0, 0, "catalog={catalog}: premise: the log's last flight is unconfirmed");
        let db = reopen(&path, opts(catalog, SyncClass::FullFsync), incarnation);
        let got = db
            .branch(id)
            .unwrap()
            .connect()
            .and_then(|c| c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows()));
        assert!(
            got.is_err(),
            "catalog={catalog}: CLAIM: a damaged slot of an unconfirmed ordered last flight was dropped silently, the child read {got:?}"
        );
    }
}

/// Engine review 17 MED 5: a trunk commit's barrier finds its TrunkRetain already written by a
/// concurrent D0 flight (here a branch commit on z, led in the window between the commit's decision
/// and its barrier, its records tagged unsynced), so the barrier is a zero-byte ORDERED upgrade:
/// it barriers the arena and the log, the commit's WAL F_FULLFSYNC makes them durable, and it wrote
/// no end frame. The last flight stayed an unsynced one, slot-checked at open, so a damaged slot
/// of the acknowledged TrunkRetain dropped the flight silently and x read the trunk's newer page.
/// The upgrade closes with an empty end frame tagged ordered: the damaged slot is refused when x
/// reads it. Mutant `ordered_upgrade_writes_no_end`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_an_ordered_upgrade_made_durable_is_refused_when_read() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ordered-upgrade.db");
        let (id, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Off));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            let x = trunk.fork_branch().unwrap();
            let z = trunk.fork_branch().unwrap();
            let zc = z.connect().unwrap();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_TRUNK_DECIDED, O::Release);
            let committing = std::thread::spawn(move || {
                write_v(&trunk, 3, "new");
                trunk
            });
            eventually(&format!("catalog={catalog}: premise: the trunk commit never decided"), || {
                hold.load(O::Acquire) == super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED
            });
            // z's commit leads a D0 flight in the window: it carries the trunk commit's TrunkRetain.
            write_v(&zc, 40, "z");
            let barriers = sync_counts().barrier;
            hold.store(0, O::Release);
            let trunk = committing.join().unwrap();
            assert!(sync_counts().barrier > barriers, "catalog={catalog}: premise: the trunk commit's barrier was ordered");
            let z_slots: std::collections::HashSet<u32> = z.owned_slots().into_iter().collect();
            let kept: Vec<u32> = db
                .branch_slots_in_use()
                .into_iter()
                .filter(|s| !before.contains(s) && !z_slots.contains(s))
                .collect();
            assert_eq!(kept.len(), 1, "catalog={catalog}: premise: the trunk commit kept one pre-image for x");
            let arena = arena_path(&db);
            let id = x.into_id();
            let _z = z.into_id();
            drop(zc);
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().read(true).write(true).open(&arena).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact_at(&mut byte, kept[0] as u64 * 4096 + 100).unwrap();
            f.write_all_at(&[byte[0] ^ 0xFF], kept[0] as u64 * 4096 + 100).unwrap();
            (id, incarnation)
        };
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        let got = db
            .branch(id)
            .unwrap()
            .connect()
            .and_then(|c| c.prepare("SELECT v FROM t WHERE id = 3").and_then(|mut s| s.run_collect_rows()));
        assert!(
            got.is_err(),
            "catalog={catalog}: a damaged slot an ordered upgrade made durable was dropped silently, x read {got:?}"
        );
    }
}

// ---- engine review 21 MEDs 6-9: every route by which a slot-naming last flight is made durable
// confirms it, or closes it ordered ----

/// Flip one byte of arena slot `slot` in the file at `arena` (a slot damaged on the device).
fn flip_slot_byte(arena: &Path, slot: u32) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(arena).unwrap();
    let at = slot as u64 * 4096 + 100;
    let mut byte = [0u8; 1];
    f.seek(SeekFrom::Start(at)).unwrap();
    f.read_exact(&mut byte).unwrap();
    f.seek(SeekFrom::Start(at)).unwrap();
    f.write_all(&[byte[0] ^ 0xFF]).unwrap();
}

/// What branch `id` reads for row `row` of `t` in `db`.
fn read_row_of(db: &Arc<Database>, id: BranchId, row: i64) -> crate::Result<Vec<Vec<crate::Value>>> {
    db.branch(id)
        .unwrap()
        .connect()
        .and_then(|c| c.prepare(format!("SELECT v FROM t WHERE id = {row}")).and_then(|mut s| s.run_collect_rows()))
}

/// Engine review 21 MED 6 (a): a trunk commit's barrier finds its TrunkRetain already written by a
/// concurrent D0 flight (a branch commit on z, led in the window between the commit's decision and
/// its barrier), and its WAL is not F_FULLFSYNCed on the branch files' device (`fullfsync` off; off
/// Apple, always), so the barrier is a zero-byte UPGRADE flight: it syncs the arena and the log,
/// which makes z's flight durable, and writes no end frame and no confirmation. The last flight
/// stayed z's unsynced one, slot-checked at open, so a damaged slot of the acknowledged TrunkRetain
/// dropped the flight silently and x read the trunk's newer page. The upgrade's confirmation of the
/// log's last flight, written by the confirmation writer or at the close, makes open keep that
/// flight's records: the damaged slot is refused when x reads it. Every target (84e7826f9's arm is
/// Apple's ordered one), both modes. Mutant `upgrade_confirms_nothing`.
#[test]
fn a_damaged_slot_a_zero_byte_upgrade_made_durable_is_refused_when_read() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("zero-byte-upgrade.db");
        let (id, kept, arena, incarnation) = {
            let db = open_at(&path, opts(catalog, SyncClass::Off));
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("PRAGMA fullfsync = OFF").unwrap();
            let x = trunk.fork_branch().unwrap();
            let z = trunk.fork_branch().unwrap();
            let zc = z.connect().unwrap();
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_TRUNK_DECIDED, O::Release);
            let committing = std::thread::spawn(move || {
                write_v(&trunk, 3, "new");
                trunk
            });
            eventually(&format!("{what}: premise: the trunk commit never decided"), || {
                hold.load(O::Acquire) == super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED
            });
            // z's commit leads a D0 flight in the window: it carries the trunk commit's TrunkRetain.
            write_v(&zc, 40, "z");
            assert!(db.branches.arena_dirty_for_test(), "{what}: premise: z's D0 flight left the arena unsynced");
            let upgrades = db.branches.group_counters()[4];
            hold.store(0, O::Release);
            let trunk = committing.join().unwrap();
            assert!(
                db.branches.group_counters()[4] > upgrades,
                "{what}: premise: the trunk commit's barrier was a zero-byte upgrade"
            );
            let z_slots: std::collections::HashSet<u32> = z.owned_slots().into_iter().collect();
            let kept: Vec<u32> = db
                .branch_slots_in_use()
                .into_iter()
                .filter(|s| !before.contains(s) && !z_slots.contains(s))
                .collect();
            assert_eq!(kept.len(), 1, "{what}: premise: the trunk commit kept one pre-image for x");
            let arena = arena_path(&db);
            let id = x.into_id();
            let _z = z.into_id();
            drop(zc);
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            (id, kept[0], arena, incarnation)
        };
        flip_slot_byte(&arena, kept);
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        let got = read_row_of(&db, id, 3);
        assert!(
            got.is_err(),
            "{what}: a damaged slot a zero-byte upgrade made durable was dropped silently, x read {got:?}"
        );
    }
}

/// Engine review 21 MED 6 (b): as (a), but z's flight is made durable by a raised D0 store's
/// held-free upgrade (its bound forced to 1, `HoldBound`), led by the background writer after z's
/// flight and landed before the trunk commit's barrier, which then finds the TrunkRetain durable and
/// takes its no-flight path. The upgrade syncs the arena and the log and confirms nothing, so the
/// last flight stayed z's unsynced one, slot-checked at open, and a damaged slot of the TrunkRetain
/// dropped it silently. Every target, both modes. Mutant `held_free_upgrade_confirms_nothing`.
#[test]
fn a_damaged_slot_a_held_free_upgrade_made_durable_is_refused_when_read() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("held-free-upgrade.db");
        let (id, kept, arena, incarnation) = {
            let (db, trunk, w, w_slots) = raised_d0_with_a_kept_pre_image(&path, catalog, true);
            assert_eq!(w_slots.len(), 2, "{what}: premise: w's release frees two slots, past the bound");
            let x = trunk.fork_branch().unwrap();
            let z = trunk.fork_branch().unwrap();
            let zc = z.connect().unwrap();
            // w's release arms the upgrade; the writer is held where it found it due, so the upgrade
            // is taken only after z's flight.
            db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
            w.reap().unwrap();
            let _armed = trunk.fork_branch().unwrap().into_id();
            eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
                db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
            });
            let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
            let hold = db.branches.trunk_commit_hold.clone();
            hold.store(super::store::HOLD_TRUNK_DECIDED, O::Release);
            let committing = std::thread::spawn(move || {
                write_v(&trunk, 5, "new");
                trunk
            });
            eventually(&format!("{what}: premise: the trunk commit never decided"), || {
                hold.load(O::Acquire) == super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED
            });
            // z's commit leads a D0 flight in the window: it carries the trunk commit's TrunkRetain.
            write_v(&zc, 40, "z");
            assert!(db.branches.arena_dirty_for_test(), "{what}: premise: z's D0 flight left the arena unsynced");
            let upgrades = db.branches.group_counters()[4];
            db.branches.confirm_hold_for_test(0);
            eventually(&format!("{what}: premise: the held-free upgrade never landed"), || {
                w_slots.iter().all(|&s| db.branch_slot_is_free(s))
            });
            assert!(db.branches.group_counters()[4] > upgrades, "{what}: premise: a held-free upgrade ran");
            let flights = db.branches.group_counters()[0];
            hold.store(0, O::Release);
            let trunk = committing.join().unwrap();
            assert_eq!(
                db.branches.group_counters()[0],
                flights,
                "{what}: premise: the trunk commit's barrier led no flight (the upgrade made its TrunkRetain durable)"
            );
            let z_slots: std::collections::HashSet<u32> = z.owned_slots().into_iter().collect();
            let kept: Vec<u32> = db
                .branch_slots_in_use()
                .into_iter()
                .filter(|s| !before.contains(s) && !z_slots.contains(s))
                .collect();
            assert_eq!(kept.len(), 1, "{what}: premise: the trunk commit kept one pre-image for x");
            let arena = arena_path(&db);
            let id = x.into_id();
            let _z = z.into_id();
            drop(zc);
            let incarnation = db.incarnation;
            drop(trunk);
            drop(db);
            (id, kept[0], arena, incarnation)
        };
        flip_slot_byte(&arena, kept);
        let db = reopen(&path, opts(catalog, SyncClass::Off), incarnation);
        let got = read_row_of(&db, id, 5);
        assert!(
            got.is_err(),
            "{what}: a damaged slot a held-free upgrade made durable was dropped silently, x read {got:?}"
        );
    }
}

/// Engine review 21 MEDs 7 and 8: how a forked child makes a slot-naming last flight durable by a
/// trunk commit's WAL F_FULLFSYNC alone, in a store whose trunk F_FULLFSYNCs its WAL on the branch
/// files' device (Apple, `fullfsync`), so the commit's barrier is ORDERED.
#[cfg(target_vendor = "apple")]
#[derive(Clone, Copy, PartialEq, Debug)]
enum Unconfirmed {
    /// MED 7: a D1 store. A branch commit on z, in the window between the trunk commit's decision
    /// and its barrier, leads a synced flight carrying the TrunkRetain and takes the arena's mark;
    /// the barrier is then an ordered zero-byte upgrade with no arena to barrier, so no empty end
    /// frame closes z's flight, whose own confirmation is unproved (a plain fsync, on Apple) and was
    /// written only at a clean close.
    D1,
    /// MED 7: a raised D0 catalog store. A fuzzy checkpoint captured after the trunk commit's
    /// decision writes the TrunkRetain in its own D0 flight and settles the arena (`settle_arena`
    /// takes the mark); the barrier is then an ordered zero-byte upgrade with no arena to barrier,
    /// and nothing confirms the checkpoint's flight.
    SettledUpgrade,
    /// MED 8: a raised D0 catalog store. The capture comes before the trunk commit's decision and
    /// the settle after it: the barrier's ordered flight carries the TrunkRetain over an arena the
    /// settle took, so it is tagged as a synced flight (a7423baa8), slot-checked at open, and has
    /// no confirmation.
    SettledFlight,
}

/// Engine review 21 MEDs 7 and 8: a forked child (`unconfirmed_workload`) has a trunk commit
/// acknowledged whose WAL F_FULLFSYNC made a slot-naming last flight durable, gives the
/// confirmation writer its chance, and ends as a kill does (`_exit`: no close, so nothing a close
/// would confirm); then x's pre-image slot is flipped and the store reopened. x's read must be
/// refused: that flush proved the last flight's confirmation, which the writer wrote. At the base
/// nothing confirmed the flight before a close, so open's slot check dropped it silently and x read
/// the trunk's newer page. Mutants `trunk_flush_promotes_nothing` (every arm) and
/// `ordered_confirms_nothing` (the settled arms).
#[cfg(target_vendor = "apple")]
fn a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill(name: &str, catalog: bool, arm: Unconfirmed) {
    use crate::branch::fork_driver;
    let Some(sentinel) = fork_driver::alone(name) else {
        return;
    };
    let what = format!("catalog={catalog} {arm:?}");
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("unconfirmed.db");
    let out = dir.path().join("child-out");
    let class = if arm == Unconfirmed::D1 { SyncClass::Fsync } else { SyncClass::Off };
    // SAFETY: the child runs the workload, which `_exit`s; it never returns into the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed");
    if pid == 0 {
        let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unconfirmed_workload(&path, &out, catalog, class, arm)
        }))
        .map_or(100, |()| 0);
        // SAFETY: ends the child as a kill would: nothing more is flushed, no destructor runs.
        unsafe { libc::_exit(code) };
    }
    let code = fork_driver::exit_code(pid);
    assert_eq!(code, 0, "{what}: the workload failed in the child (100: a panic)");
    let text = std::fs::read_to_string(&out).unwrap();
    let mut lines = text.lines();
    let id = BranchId(lines.next().unwrap().parse().unwrap());
    let kept: u32 = lines.next().unwrap().parse().unwrap();
    let arena = std::path::PathBuf::from(lines.next().unwrap());
    flip_slot_byte(&arena, kept);
    let db = open_at(&path, opts(catalog, class));
    let got = read_row_of(&db, id, 3);
    assert!(
        got.is_err(),
        "{what}: a damaged slot a trunk commit's flush made durable was dropped silently after a kill, x read {got:?}"
    );
    fork_driver::finished(&sentinel);
}

/// The forked child's part of `a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill`:
/// the arm's stops up to the trunk commit's acknowledgement, then up to 2 s for the confirmation
/// writer to write a word (it writes a proved one once the group has been idle `confirm_quiet()`,
/// 5 ms; at the base it writes none), then x's id, its pre-image's slot and the arena's path into
/// `out`, and `_exit` with every handle open.
#[cfg(target_vendor = "apple")]
fn unconfirmed_workload(path: &Path, out: &Path, catalog: bool, class: SyncClass, arm: Unconfirmed) -> ! {
    use std::sync::atomic::Ordering as O;
    let what = format!("catalog={catalog} {arm:?}");
    let db = open_at(path, opts(catalog, class));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    if arm != Unconfirmed::D1 {
        // A trunk commit that keeps w a pre-image raises the log (its ordered flight is
        // FullFsync), so a fuzzy checkpoint settles the arena itself (`Captured::settle_arena`).
        let w = trunk.fork_branch().unwrap();
        write_v(&trunk, 30, "warm");
        let _w = w.into_id();
        assert!(db.branches.rewrite_class_for_test().syncs(), "{what}: premise: the log is raised");
    }
    let x = trunk.fork_branch().unwrap();
    let z = (arm == Unconfirmed::D1).then(|| trunk.fork_branch().unwrap());
    let zc = z.as_ref().map(|z| z.connect().unwrap());
    let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
    if arm == Unconfirmed::SettledFlight {
        // Captured before the trunk commit decides: its TrunkRetain is not the checkpoint's to write.
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_SETTLE);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "{what}: premise: a fuzzy checkpoint started");
        eventually(&format!("{what}: premise: the checkpoint never reached its settle"), || {
            db.branch_checkpoint_held() == super::store::HOLD_BEFORE_SETTLE | super::store::HOLD_ARRIVED
        });
    }
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_TRUNK_DECIDED, O::Release);
    let committing = std::thread::spawn(move || {
        write_v(&trunk, 3, "new");
        trunk
    });
    eventually(&format!("{what}: premise: the trunk commit never decided"), || {
        hold.load(O::Acquire) == super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED
    });
    match arm {
        Unconfirmed::D1 => {
            // z's commit leads a synced flight in the window: it carries the TrunkRetain.
            write_v(zc.as_ref().unwrap(), 40, "z");
        }
        Unconfirmed::SettledUpgrade => {
            // Captured after the decision: the checkpoint's own D0 flight writes the TrunkRetain,
            // and its settle takes the arena's mark.
            db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
            assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "{what}: premise: a fuzzy checkpoint started");
            eventually(&format!("{what}: premise: the checkpoint never reached its commit"), || {
                db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
            });
            assert_eq!(
                db.branches.log_lsn_for_test(),
                db.branches.durable_for_test(SyncClass::Off),
                "{what}: premise: the checkpoint's flight wrote the TrunkRetain"
            );
        }
        Unconfirmed::SettledFlight => {
            // Settled after the decision: the settle takes the mark over the TrunkRetain's slot,
            // and the record stays buffered for the barrier's own flight.
            db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
            eventually(&format!("{what}: premise: the checkpoint never reached its commit"), || {
                db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
            });
            assert!(
                db.branches.log_lsn_for_test() > db.branches.durable_for_test(SyncClass::Off),
                "{what}: premise: the TrunkRetain is still buffered"
            );
        }
    }
    assert!(
        !db.branches.arena_dirty_for_test(),
        "{what}: premise: the trunk commit's barrier finds the arena's mark taken"
    );
    let barriers = sync_counts().barrier;
    let written = db.branches.confirm_counts()[0];
    hold.store(0, O::Release);
    let trunk = committing.join().unwrap();
    assert!(sync_counts().barrier > barriers, "{what}: premise: the trunk commit's barrier was ordered");
    assert_eq!(read_v(&trunk, 3), "new", "{what}: premise: the trunk commit was acknowledged");
    let t = std::time::Instant::now();
    while db.branches.confirm_counts()[0] == written && t.elapsed() < std::time::Duration::from_secs(2) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let z_slots: std::collections::HashSet<u32> = z.as_ref().map(|z| z.owned_slots()).unwrap_or_default().into_iter().collect();
    let kept: Vec<u32> = db
        .branch_slots_in_use()
        .into_iter()
        .filter(|s| !before.contains(s) && !z_slots.contains(s))
        .collect();
    assert_eq!(kept.len(), 1, "{what}: premise: the trunk commit kept one pre-image for x");
    std::fs::write(out, format!("{}\n{}\n{}\n", x.into_id().0, kept[0], arena_path(&db).display())).unwrap();
    // SAFETY: ends the child as a kill would: nothing more is flushed, no destructor runs.
    unsafe { libc::_exit(0) }
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_d1_snapshot() {
    a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill(
        "branch::fastest_tests::a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_d1_snapshot",
        false,
        Unconfirmed::D1,
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_d1_catalog() {
    a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill(
        "branch::fastest_tests::a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_d1_catalog",
        true,
        Unconfirmed::D1,
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_settled_upgrade() {
    a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill(
        "branch::fastest_tests::a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_settled_upgrade",
        true,
        Unconfirmed::SettledUpgrade,
    );
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_settled_flight() {
    a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill(
        "branch::fastest_tests::a_damaged_slot_a_trunk_flush_made_durable_is_refused_after_a_kill_settled_flight",
        true,
        Unconfirmed::SettledFlight,
    );
}

/// Engine review 21 MED 9: a rewrite of the log dropped 84e7826f9's empty ordered end frame (an end
/// frame that closes no record was skipped), so the kept flight it closed came back tagged from its
/// own unsynced end frame, with no cut word, and was slot-checked at open: a damaged slot of the
/// acknowledged TrunkRetain dropped it silently and x read the trunk's newer page. Here a fuzzy
/// checkpoint captured before the trunk commit's decision installs after the trunk's ordered
/// upgrade, so its cut keeps z's flight and the empty end frame after it. Catalog mode (a cut).
/// Mutant `reframe_drops_empty_end`.
#[cfg(target_vendor = "apple")]
#[test]
fn a_damaged_slot_an_ordered_upgrade_made_durable_is_refused_after_a_cut() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("ordered-upgrade-cut.db");
    let (id, kept, arena, incarnation) = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let x = trunk.fork_branch().unwrap();
        let z = trunk.fork_branch().unwrap();
        let zc = z.connect().unwrap();
        let before: std::collections::HashSet<u32> = db.branch_slots_in_use().into_iter().collect();
        // Captured before the trunk commit decides: its cut keeps every record written after.
        db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        eventually("premise: the checkpoint never reached its commit", || {
            db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
        });
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_TRUNK_DECIDED, O::Release);
        let committing = std::thread::spawn(move || {
            write_v(&trunk, 3, "new");
            trunk
        });
        eventually("premise: the trunk commit never decided", || {
            hold.load(O::Acquire) == super::store::HOLD_TRUNK_DECIDED | super::store::HOLD_ARRIVED
        });
        // z's commit leads a D0 flight in the window: it carries the trunk commit's TrunkRetain.
        write_v(&zc, 40, "z");
        let barriers = sync_counts().barrier;
        let len = db.branches.log_len_for_test();
        hold.store(0, O::Release);
        let trunk = committing.join().unwrap();
        assert!(sync_counts().barrier > barriers, "premise: the trunk commit's barrier was ordered");
        assert!(
            db.branches.log_len_for_test() > len,
            "premise: the ordered upgrade closed z's flight with an empty end frame"
        );
        db.branch_checkpoint_hold(0);
        db.branch_checkpoint_wait();
        assert!(db.branches.log_len_for_test() < len, "premise: the checkpoint's install cut the log");
        let z_slots: std::collections::HashSet<u32> = z.owned_slots().into_iter().collect();
        let kept: Vec<u32> = db
            .branch_slots_in_use()
            .into_iter()
            .filter(|s| !before.contains(s) && !z_slots.contains(s))
            .collect();
        assert_eq!(kept.len(), 1, "premise: the trunk commit kept one pre-image for x");
        let arena = arena_path(&db);
        let id = x.into_id();
        let _z = z.into_id();
        drop(zc);
        let incarnation = db.incarnation;
        drop(trunk);
        drop(db);
        (id, kept[0], arena, incarnation)
    };
    flip_slot_byte(&arena, kept);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    let got = read_row_of(&db, id, 3);
    assert!(
        got.is_err(),
        "a damaged slot an ordered upgrade made durable was dropped silently after a cut, x read {got:?}"
    );
}

/// Engine review 21 LOW 19: in-place VACUUM (open once no branch exists, while D0 records may be
/// undrained) syncs the trunk's WAL itself twice, its header's after a truncation and its image's
/// before the image is published, and neither sync was watched, though on the branch files' device
/// either is a drain. A D0 store with x's Release undrained, the trunk FULL with fullfsync (so the
/// sync drains on Apple as well), the WAL's next sync failing at issue: the VACUUM fails and the
/// next fork is refused as fail-stopped. Arms: the header's sync (the WAL truncated first) and the
/// publish sync (it is the first WAL sync VACUUM issues over an initialized WAL). Both modes.
/// Mutant `vacuum_sync_unwatched`.
#[test]
fn a_failed_vacuum_wal_sync_fail_stops_a_store_with_an_undrained_record() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        for header in [true, false] {
            let what = format!("catalog={catalog} header={header}");
            let dir = tempfile::TempDir::new().unwrap();
            // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): only the source's
            // WAL is armed. Run at 9d27592bf (artie raw l1-branch_9d27592bf.txt), the armed sync was
            // the VACUUM target build's own commit (a database opened on the same IO, its WAL also
            // `-wal`; backtrace through vacuum_target_build_step, dev tree, unbanked), synced in
            // Fsync, which drains nothing on Apple: never the two syncs this test is about.
            let (db, armed) = open_failing_source_wal(
                &dir.path().join("vacuum-sync.db"),
                opts(catalog, SyncClass::Off).with_vacuum(true),
            );
            let trunk = db.connect().unwrap();
            seed(&trunk);
            trunk.fork_branch().unwrap().reap().unwrap();
            assert!(drained(&db) < db.branches.durable_for_test(SyncClass::Off), "{what}: premise: x's Release is undrained");
            if header {
                trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            }
            trunk.execute("PRAGMA synchronous = FULL").unwrap();
            trunk.execute("PRAGMA fullfsync = ON").unwrap();
            armed.store(1, O::Release);
            let vacuumed = trunk.execute("VACUUM");
            assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the VACUUM reached a WAL sync");
            assert!(vacuumed.is_err(), "{what}: premise: the failed WAL sync failed the VACUUM");
            assert_fail_stopped(
                db.connect().unwrap().fork_branch().map(|x| x.into_id()),
                &format!("{what}: the next fork after a failed VACUUM WAL sync with an undrained Release"),
            );
        }
    }
}

/// L2 slice (budget create_allocates_no_table_of_every_branch_under_the_store_mutex, 64 KiB under
/// the store mutex per create; budgets measured 2,195,727 bytes as the live branches reached
/// 33,056): no per-branch table regrows, or allocates a chunk, of more than 64 KiB inside a create.
/// Unnamed creates in D0 from 0 past 2^15 live branches, both modes; after each create the tables'
/// capacities (`per_branch_tables_for_test`) are compared with the last ones, and every growth is
/// priced at the bytes its new allocation takes. The message names the tables, so the fix is aimed.
#[test]
fn no_per_branch_table_regrows_past_64_kib_inside_a_create() {
    let _s = serial();
    let mut over = Vec::new();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("growth.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let mut last = db.branches.per_branch_tables_for_test();
        let mut worst: std::collections::BTreeMap<&str, usize> = Default::default();
        for _ in 0..34_000 {
            let _ = trunk.fork_branch().unwrap().into_id();
            let now = db.branches.per_branch_tables_for_test();
            for (name, cap, bytes) in &now {
                let grew = last.iter().find(|(m, ..)| m == name).is_none_or(|(_, c, _)| cap > c);
                if grew && *bytes > 0 {
                    let e = worst.entry(name).or_default();
                    *e = (*e).max(*bytes);
                }
            }
            last = now;
        }
        for (name, bytes) in worst {
            if bytes > 64 << 10 {
                over.push(format!("catalog={catalog} {name}: {bytes} bytes"));
            }
        }
    }
    assert!(over.is_empty(), "CLAIM: a create regrew a per-branch table past 64 KiB under the store mutex: {over:?}");
}

/// The NAMED twin of `no_per_branch_table_regrows_past_64_kib_inside_a_create`: budgets' growth cell
/// still measured 1,638,591 bytes under the store mutex at 22c5c5da3 (artie raw
/// l10-branch_22c5c5da3.txt), about one regrowth of a 65,536-bucket map of (Arc<str>, BranchId), so
/// the name index is measured here: 34,000 named creates, both modes, every per-branch table and the
/// name filter's sets priced at each growth.
#[test]
fn no_name_table_regrows_past_64_kib_inside_a_named_create() {
    let _s = serial();
    let mut over = Vec::new();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("named-growth.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let mut last = db.branches.per_branch_tables_for_test();
        let mut worst: std::collections::BTreeMap<&str, usize> = Default::default();
        for n in 0..34_000u32 {
            // FLAGGED TEST EDIT, lead-ordered (the name-filter acceptance condition, DECISIONS
            // 2026-10-15): a table's growth counts against this create only if it happened under the
            // store mutex on this create's thread, which its held allocations measure: a growth of
            // `bytes` needs at least `bytes` allocated in the create's holds. A table grown on
            // another thread (the name filter's, off the mutex) is not this create's.
            let held0 = super::budget_probe::thread_allocs().3;
            let _ = trunk.create_branch(&format!("g{n}")).unwrap();
            let held = super::budget_probe::thread_allocs().3 - held0;
            let now = db.branches.per_branch_tables_for_test();
            for (name, cap, bytes) in &now {
                let grew = last.iter().find(|(m, ..)| m == name).is_none_or(|(_, c, _)| cap > c);
                if grew && *bytes > 0 && held >= *bytes as u64 {
                    let e = worst.entry(name).or_default();
                    *e = (*e).max(*bytes);
                }
            }
            last = now;
        }
        for (name, bytes) in worst {
            if bytes > 64 << 10 {
                over.push(format!("catalog={catalog} {name}: {bytes} bytes"));
            }
        }
    }
    assert!(over.is_empty(), "CLAIM: a named create regrew a per-branch table past 64 KiB under the store mutex: {over:?}");
}

/// C1 coverage question (engine review 20 LOW 16): the kill point `cut.dir_synced` (a syncing flight
/// after a cut, between its directory sync and its log sync) never landed in any C1 run, D0 or D1
/// (artie raws l14-low16d1_7076641a5.txt: 0 of 6 in a D1 child; earlier catalog C1 runs: 0 landed).
/// Either no flight after a fuzzy cut syncs the log's directory, which would leave the cut's rename
/// undurable under acknowledged flights, or the window is not reached in a C1 trial. Here, in
/// process: a D1 catalog store, a fuzzy checkpoint that cut the log (premise: the log shortened),
/// then one create, whose flight this thread leads: it must sync the log's directory.
#[test]
fn a_d1_flight_after_a_fuzzy_cut_syncs_the_log_directory() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("cut-dir.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for _ in 0..30 {
        let _ = trunk.fork_branch().unwrap().into_id();
    }
    let len = db.branches.log_len_for_test();
    let cuts = super::store::INSTALL_CUTS.load(std::sync::atomic::Ordering::Relaxed);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    assert!(db.branches.log_len_for_test() < len, "premise: the checkpoint cut the log");
    // Strengthened (my own test, 0ac4f207d, unaccepted): the shortening must be a CUT (a prepared
    // cut finished at the install), not the install's fallback rewrite, which also shortens it.
    assert!(
        super::store::INSTALL_CUTS.load(std::sync::atomic::Ordering::Relaxed) > cuts,
        "premise: the checkpoint finished a prepared cut (it fell back to a rewrite: {} rewrites so far)",
        super::store::INSTALL_REWRITES.load(std::sync::atomic::Ordering::Relaxed)
    );
    let dirs = || super::journal::DIR_SYNCS.with(|c| c.get());
    let before = dirs();
    let _ = trunk.fork_branch().unwrap().into_id();
    assert!(dirs() > before, "CLAIM: the first D1 flight after a fuzzy cut synced no directory: the cut's rename is not durable under it");
}

/// Review REVIEW-claude-namefilter-729f78659..4cb167221 H1 (a fresh adversary): when the growth's drain
/// copy failed, `draining` was cleared in one store-mutex hold and `overflowed` set in the next, so
/// between them the filter answered "absent" for a name only the pending table held. A create then
/// asked no catalog, and an evicted name was given twice. The adversary's interleaving, forced by the
/// hooks: the growth paused before its swap (stage 3), so `victim` is noted after both first copies
/// and lives in the pending table only; then the drain copy fails (`FILTER_DRAIN_FAILS`) and the
/// growth pauses after it (stage 5). There, the filter must not answer "absent" for `victim`.
#[test]
fn a_failed_filter_drain_never_answers_absent_for_a_held_name() {
    use super::store::{FILTER_DRAIN_FAILS, FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_DRAIN_FAILS.store(false, Release);
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("filter-drain.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    FILTER_DRAIN_FAILS.store(true, Release);
    FILTER_PAUSE_AT.store(3, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 3 {
        trunk.create_branch(&format!("fill-{n:05}")).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no filter growth reached its swap ({n} creates)");
    }
    trunk.create_branch("victim").unwrap();
    FILTER_PAUSE_AT.store(5, Release);
    while FILTER_PAUSED.load(Acquire) != 5 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the growth never finished its drain copy");
    }
    let absent = db.branches.name_filter_says_absent_for_test("victim");
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
    assert!(!absent, "CLAIM: between a failed filter drain's two holds the filter answered absent for a held name: a create would ask no catalog");
}

/// Lead order (a) and the filter-fallback proof (2026-10-16): a NATURAL name-filter growth, with no
/// fault injected, sent every named create of its window to the catalog under the store mutex,
/// because `says_absent` answered only in `Ready`. While `Growing` the old table is still complete
/// (notes go into it and the pending table, and a refusal overflows); while `Draining` the new table
/// and the pending one together are (what `Ready` ends up holding). A fresh name created in each
/// state must ask the catalog nothing; a held name created again must still be refused. Hooks:
/// `FILTER_PAUSE_AT` stage 1 (Growing) and 4 (Draining).
#[test]
fn a_named_create_during_a_natural_filter_growth_asks_the_catalog_nothing() {
    use super::store::{FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("filter-growth-asks.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    db.branch_wait_name_filter();
    let stmts = || super::budget_probe::thread_cat_stmts();
    FILTER_PAUSE_AT.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 1 {
        trunk.create_branch(&format!("fill-{n:05}")).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter growth began ({n} creates)");
    }
    assert_eq!(db.branches.name_filter_state_for_test().0, "Growing", "premise: the filter is Growing at stage 1");
    let s0 = stmts();
    trunk.create_branch("while-growing").unwrap();
    let growing = stmts() - s0;
    FILTER_PAUSE_AT.store(4, Release);
    while FILTER_PAUSED.load(Acquire) != 4 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the growth never reached its drain");
    }
    assert_eq!(db.branches.name_filter_state_for_test().0, "Draining", "premise: the filter is Draining at stage 4");
    let s1 = stmts();
    trunk.create_branch("while-draining").unwrap();
    let draining = stmts() - s1;
    let taken = trunk.create_branch("while-growing").is_err() && trunk.create_branch("fill-00000").is_err();
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
    assert!(taken, "CLAIM: a name held before or during the growth was given again while the filter was Draining");
    assert_eq!(
        (growing, draining),
        (0, 0),
        "CLAIM: a named create of a fresh name ran catalog statements while the filter was Growing ({growing}) or Draining ({draining})"
    );
}

/// Lead order (a), the filter-fallback proof's second natural path: create-and-delete cycles (the
/// wire's benchmark loop) leave the filter's table past half full of released names while few are
/// live, and the next note SHRINKS it, by a rebuild from the catalog. At 0cca39a23 the shrink
/// overflowed the filter, so every named create until the rebuild landed ran a catalog statement
/// under the store mutex, though the old table was still complete (a shrink refuses nothing). No
/// fault injected; the rebuild is held after its `Building` began (`FILTER_PAUSE_AT` stage 7), and
/// a fresh name created there must ask the catalog nothing. Premise: a rebuild was started.
#[test]
fn a_named_create_during_a_natural_filter_shrink_asks_the_catalog_nothing() {
    use super::store::{FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("filter-shrink-asks.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.create_branch("held").unwrap();
    db.branch_wait_name_filter();
    let rebuilds0 = db.branches.name_filter_state_for_test().1;
    FILTER_PAUSE_AT.store(7, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 7 {
        let name = format!("cycle-{n:05}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter rebuild began ({n} cycles)");
    }
    assert!(db.branches.name_filter_state_for_test().1 > rebuilds0, "premise: the rebuild held at stage 7 was not one a create owed");
    let s0 = super::budget_probe::thread_cat_stmts();
    trunk.create_branch("while-shrinking").unwrap();
    let asked = super::budget_probe::thread_cat_stmts() - s0;
    let taken = trunk.create_branch("held").is_err();
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
    assert!(taken, "CLAIM: a held name was given again during the shrink's rebuild");
    assert_eq!(asked, 0, "CLAIM: a named create of a fresh name ran {asked} catalog statements during a natural filter shrink's rebuild ({n} cycles)");
}

/// Lead order (a), the filter-fallback proof's third natural path (artie engine NOTES df996a0f91,
/// case 3): after every reopen of a catalog store the filter is Unbuilt and then Building with no
/// table until the open's build lands, so every named create meanwhile runs a catalog statement
/// under the store mutex. The build is held after Building began (stage 7); a fresh name created
/// there must ask the catalog nothing. REGISTERED RED with no fix written: the fix is a LEAP (the
/// filter carried in the checkpoint, so an open starts Ready).
#[test]
#[ignore = "registered red (review m1-2b LOW-F3): fails at HEAD by design until the LEAP in artie engine NOTES df996a0f91/e312ad20c6 (the filter carried across a reopen) lands; run by name with --ignored"]
fn a_named_create_before_the_open_s_filter_build_lands_asks_the_catalog_nothing() {
    use super::store::{FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("filter-open-asks.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..64 {
            trunk.create_branch(&format!("kept-{i:02}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    FILTER_PAUSE_AT.store(7, Release);
    let db = reopen(&path, opts(true, SyncClass::Fsync), incarnation);
    let t = std::time::Instant::now();
    while FILTER_PAUSED.load(Acquire) != 7 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: the open's filter build never began");
    }
    let trunk = db.connect().unwrap();
    let s0 = super::budget_probe::thread_cat_stmts();
    trunk.create_branch("after-open").unwrap();
    let asked = super::budget_probe::thread_cat_stmts() - s0;
    let taken = trunk.create_branch("kept-07").is_err();
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
    assert!(taken, "CLAIM: a checkpointed name was given again before the open's filter build landed");
    assert_eq!(asked, 0, "CLAIM: a named create of a fresh name ran {asked} catalog statements before the open's filter build landed");
}

/// Lead order (a), the filter-fallback proof's fourth natural path (artie engine NOTES df996a0f91,
/// case 4): a burst of creates while a natural growth is slow (held at stage 1, `Growing`) fills a
/// table to its refusal (7/8 full), and the filter overflowed with no table answering, so every
/// named create until the rebuild landed ran a catalog statement under the store mutex. With the
/// refusing tables and a pre-allocated spill table of what they refused, the answer stays complete:
/// a fresh name created once the refusal owed its rebuild, and again while that rebuild is held
/// `Building` before its scan (`NAME_SCAN_HOLD`, armed before the burst: the rebuild starts at the
/// refusing create), asks the catalog nothing, and the filter never says "absent" for a name
/// created in the burst.
#[test]
fn a_named_create_after_a_natural_filter_refusal_asks_the_catalog_nothing() {
    use super::store::{FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
            super::store::NAME_SCAN_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("filter-refusal-asks.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.create_branch("first").unwrap();
    db.branch_wait_name_filter();
    FILTER_PAUSE_AT.store(1, Release);
    let t = std::time::Instant::now();
    let mut names = Vec::new();
    while FILTER_PAUSED.load(Acquire) != 1 {
        let name = format!("fill-{:05}", names.len());
        trunk.create_branch(&name).unwrap();
        names.push(name);
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter growth began");
    }
    // The burst, with the growth held: until a table refuses and owes its rebuild.
    let rebuilds0 = db.branches.name_filter_state_for_test().1;
    super::store::NAME_SCAN_HOLD.store(1, Release);
    while db.branches.name_filter_state_for_test().1 == rebuilds0 {
        let name = format!("burst-{:05}", names.len());
        trunk.create_branch(&name).unwrap();
        names.push(name);
        assert!(names.len() < 1 << 20, "premise: no table refused a key during the held growth");
    }
    let stmts = || super::budget_probe::thread_cat_stmts();
    let s0 = stmts();
    trunk.create_branch("while-overflowed").unwrap();
    let overflowed = stmts() - s0;
    names.push("while-overflowed".to_string());
    while super::store::NAME_SCAN_HOLD.load(Acquire) != 1 | super::store::HOLD_ARRIVED {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the refusal's rebuild never reached its scan");
    }
    assert_eq!(db.branches.name_filter_state_for_test().0, "Building", "premise: the rebuild is held Building");
    let s1 = stmts();
    trunk.create_branch("while-rebuilding").unwrap();
    let rebuilding = stmts() - s1;
    names.push("while-rebuilding".to_string());
    let said_absent: Vec<&String> = names.iter().filter(|n| db.branches.name_filter_says_absent_for_test(n)).collect();
    super::store::NAME_SCAN_HOLD.store(0, Release);
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
    assert!(said_absent.is_empty(), "CLAIM: the filter said absent for {} held names after the refusal: {:?}", said_absent.len(), &said_absent[..said_absent.len().min(8)]);
    assert_eq!(
        (overflowed, rebuilding),
        (0, 0),
        "CLAIM: a named create of a fresh name ran catalog statements after a natural refusal: {overflowed} while Overflowed, {rebuilding} while its rebuild was Building"
    );
}

/// Review m1-2b LOW-F1 (DECISIONS 6c1dbf7449, must-fix before credit): the filter tests' controls
/// were resident names, which `name_lookup` refuses from `names.map` before the filter answers, so
/// no false "absent" could show. These are `k` named branches held ONLY in the catalog: created,
/// checkpointed, then evicted (resident cap 1 and one more checkpoint).
fn catalog_only_names(db: &Arc<Database>, trunk: &Arc<Connection>, k: usize) -> Vec<String> {
    let names: Vec<String> = (0..k).map(|i| format!("cat-only-{i:03}")).collect();
    for n in &names {
        trunk.create_branch(n).unwrap();
    }
    db.branch_compact_now().unwrap();
    db.branch_set_resident_cap(Some(1));
    // The eviction runs at a fuzzy checkpoint's install (`evict_clean`).
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the evicting fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    for n in &names {
        assert!(!db.branches.name_resident_for_test(n), "premise: {n} is still resident after the eviction");
    }
    names
}

/// In the filter's current `state`: no name held only in the catalog is answered "absent", and
/// creating one again is refused.
fn assert_no_false_absent(db: &Arc<Database>, trunk: &Arc<Connection>, names: &[String], state: &str) {
    assert_eq!(db.branches.name_filter_state_for_test().0, state, "premise: the filter is {state}");
    for n in names {
        assert!(!db.branches.name_filter_says_absent_for_test(n), "CLAIM: the filter answered absent while {state} for {n}, held only in the catalog");
    }
    assert!(trunk.create_branch(&names[0]).is_err(), "CLAIM: while {state}, a name held only in the catalog was given again");
}

/// LOW-F1 in a natural growth: `Growing` (stage 1) and `Draining` (stage 4). Mutants
/// `filter_lies_growing`, `filter_lies_draining`.
#[test]
fn a_growing_or_draining_filter_never_answers_absent_for_a_catalog_only_name() {
    use super::store::{FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("lie-growth.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let names = catalog_only_names(&db, &trunk, 4);
    db.branch_wait_name_filter();
    FILTER_PAUSE_AT.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 1 {
        trunk.create_branch(&format!("fill-{n:05}")).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter growth began ({n} creates)");
    }
    assert_no_false_absent(&db, &trunk, &names, "Growing");
    FILTER_PAUSE_AT.store(4, Release);
    while FILTER_PAUSED.load(Acquire) != 4 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the growth never reached its drain");
    }
    assert_no_false_absent(&db, &trunk, &names, "Draining");
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
}

/// LOW-F1 in a natural shrink: `Stale` (the build held before its seed, `BUILD_SEED_HOLD`) and
/// `Building` with the old table (stage 7). Mutants `filter_lies_stale`, `filter_lies_building_old`.
#[test]
fn a_shrinking_filter_never_answers_absent_for_a_catalog_only_name() {
    use super::store::{BUILD_SEED_HOLD, FILTER_PAUSED, FILTER_PAUSE_AT, HOLD_ARRIVED};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BUILD_SEED_HOLD.store(0, Release);
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("lie-shrink.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let names = catalog_only_names(&db, &trunk, 4);
    db.branch_wait_name_filter();
    BUILD_SEED_HOLD.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while BUILD_SEED_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        let name = format!("cycle-{n:05}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter shrink owed a rebuild ({n} cycles)");
    }
    assert_no_false_absent(&db, &trunk, &names, "Stale");
    FILTER_PAUSE_AT.store(7, Release);
    BUILD_SEED_HOLD.store(0, Release);
    while FILTER_PAUSED.load(Acquire) != 7 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the shrink's rebuild never reached Building");
    }
    assert!(db.branches.name_filter_stats().0, "premise: the rebuild is Building with the old table answering");
    assert_no_false_absent(&db, &trunk, &names, "Building");
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
}

/// LOW-F1 after a natural refusal (5a0d874e0's `Answer` and spare): `Overflowed` with an answer
/// (the rebuild held before its seed) and `Building` with that answer as its old one (held at its
/// scan). Mutants `filter_lies_overflowed_answer`, `filter_lies_building_old`.
#[test]
fn a_refusing_filter_never_answers_absent_for_a_catalog_only_name() {
    use super::store::{BUILD_SEED_HOLD, FILTER_PAUSED, FILTER_PAUSE_AT, HOLD_ARRIVED, NAME_SCAN_HOLD};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BUILD_SEED_HOLD.store(0, Release);
            NAME_SCAN_HOLD.store(0, Release);
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("lie-refusal.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let names = catalog_only_names(&db, &trunk, 4);
    db.branch_wait_name_filter();
    FILTER_PAUSE_AT.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 1 {
        trunk.create_branch(&format!("fill-{n:05}")).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural filter growth began");
    }
    BUILD_SEED_HOLD.store(1, Release);
    while BUILD_SEED_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        trunk.create_branch(&format!("burst-{n:05}")).unwrap();
        n += 1;
        assert!(n < 1 << 20, "premise: no table refused a key during the held growth");
    }
    assert!(db.branches.name_filter_stats().0, "premise: the refusal kept an answer (a spare was kept)");
    assert_no_false_absent(&db, &trunk, &names, "Overflowed");
    NAME_SCAN_HOLD.store(1, Release);
    BUILD_SEED_HOLD.store(0, Release);
    while NAME_SCAN_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the refusal's rebuild never reached its scan");
    }
    assert_no_false_absent(&db, &trunk, &names, "Building");
    NAME_SCAN_HOLD.store(0, Release);
    FILTER_PAUSE_AT.store(0, Release);
    db.branch_wait_name_filter();
}

/// Review m1-2b LOW-F2 (DECISIONS 6c1dbf7449), as a class: no filter table is freed under the
/// store mutex. A shrink's rebuild is held at its scan (`Building` with the shrink's table as its
/// old answer); a burst of named creates then fills that old table until it refuses a note, which
/// ends the answer (counted, `name_filter_refusals_for_test`). At bf7a1ef1f the answer's table was
/// dropped inside that create's note, its last reference: a free of the whole table under the mutex.
/// No note may free anything (`FILTER_NOTE_FREED_MAX`).
#[test]
fn a_refusing_answer_s_table_is_not_freed_under_the_store_mutex() {
    use super::store::{FILTER_NOTE_FREED_MAX, HOLD_ARRIVED, NAME_SCAN_HOLD};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            NAME_SCAN_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("refusal-free.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.create_branch("held").unwrap();
    db.branch_wait_name_filter();
    NAME_SCAN_HOLD.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while NAME_SCAN_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        let name = format!("cycle-{n:05}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural shrink's rebuild reached its scan ({n} cycles)");
    }
    assert!(db.branches.name_filter_stats().0, "premise: the shrink's rebuild is Building with its old table answering");
    let refused0 = db.branches.name_filter_refusals_for_test().1;
    FILTER_NOTE_FREED_MAX.store(0, Release);
    while db.branches.name_filter_refusals_for_test().1 == refused0 {
        trunk.create_branch(&format!("burst-{n:05}")).unwrap();
        n += 1;
        assert!(n < 1 << 20, "premise: the old table never refused a note");
    }
    let freed = FILTER_NOTE_FREED_MAX.load(Relaxed);
    NAME_SCAN_HOLD.store(0, Release);
    db.branch_wait_name_filter();
    assert_eq!(freed, 0, "CLAIM: a note freed {freed} bytes under the store mutex (a refusing answer's table)");
}

/// Review m1-2c N1 (DECISIONS 0f341519ea): a shrink's rebuild held at its scan (`Building` with the
/// shrink's table as its old answer, and the spare its seed hold installed); a burst of fresh names
/// fills that table until it refuses a note. At d94431cbe the refusal ended the answer, and every
/// create until Draining asked the catalog; the spare is there to take it. After the refusal, a
/// fresh name must run no catalog statement, and no held name is answered absent.
#[test]
fn a_named_create_after_a_building_answer_refuses_asks_the_catalog_nothing() {
    use super::store::{HOLD_ARRIVED, NAME_SCAN_HOLD};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            NAME_SCAN_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("building-refusal-asks.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.create_branch("held").unwrap();
    db.branch_wait_name_filter();
    NAME_SCAN_HOLD.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while NAME_SCAN_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        let name = format!("cycle-{n:05}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural shrink's rebuild reached its scan ({n} cycles)");
    }
    let refused0 = db.branches.name_filter_refusals_for_test().1;
    let mut names = Vec::new();
    while db.branches.name_filter_refusals_for_test().1 == refused0 {
        let name = format!("burst-{n:05}");
        trunk.create_branch(&name).unwrap();
        names.push(name);
        n += 1;
        assert!(n < 1 << 20, "premise: the old table never refused a note");
    }
    let s0 = super::budget_probe::thread_cat_stmts();
    trunk.create_branch("after-refusal").unwrap();
    let asked = super::budget_probe::thread_cat_stmts() - s0;
    names.push("after-refusal".to_string());
    let said_absent: Vec<&String> = names.iter().filter(|n| db.branches.name_filter_says_absent_for_test(n)).collect();
    NAME_SCAN_HOLD.store(0, Release);
    db.branch_wait_name_filter();
    assert!(said_absent.is_empty(), "CLAIM: {} held names answered absent after the refusal: {:?}", said_absent.len(), &said_absent[..said_absent.len().min(8)]);
    assert_eq!(asked, 0, "CLAIM: a fresh name ran {asked} catalog statements after a Building answer's table refused a note");
}

/// Review m1-2c N2: a build whose table cannot take its unbuilt hashes overflows at its install;
/// at 5a0d874e0 that return lost its `drop(inner)`, so the scanned table (and the unbuilt hashes)
/// were freed inside the filter thread's store-mutex hold. Forced by `BUILD_TABLE_REFUSES` on a
/// reopen's build; no filter-thread hold may free anything (`FILTER_HOLD_FREED_MAX`).
#[test]
fn a_build_that_overflows_at_its_install_frees_nothing_under_the_store_mutex() {
    use super::store::{BUILD_TABLE_REFUSES, FILTER_HOLD_FREED_MAX, FILTER_HOLD_FREES_ARMED};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_HOLD_FREES_ARMED.store(false, Release);
            BUILD_TABLE_REFUSES.store(false, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("build-overflow-free.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..64 {
            trunk.create_branch(&format!("kept-{i:02}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    FILTER_HOLD_FREED_MAX.store(0, Release);
    FILTER_HOLD_FREES_ARMED.store(true, Release);
    BUILD_TABLE_REFUSES.store(true, Release);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    db.branch_wait_name_filter();
    FILTER_HOLD_FREES_ARMED.store(false, Release);
    assert!(!BUILD_TABLE_REFUSES.load(Acquire), "premise: the reopen's build never reached its install");
    let freed = FILTER_HOLD_FREED_MAX.load(Relaxed);
    assert_eq!(freed, 0, "CLAIM: a name-filter thread freed {freed} bytes inside a store-mutex hold (the overflowing build's table)");
}

/// Review m1-2c N3: after a growth's failed drain the filter overflows answering from the new table
/// and the pending one (5a0d874e0); a name noted only into the pending table (held at stage 3, as
/// in `a_failed_filter_drain_never_answers_absent_for_a_held_name`) must still not be answered
/// absent once `Overflowed`, held there by the rebuild's seed hold. The H1 test reads only the
/// `Draining` state before the overflow, so it cannot see mutant `filter_answer_drops_pending`;
/// this is its killer.
#[test]
fn a_failed_filter_drain_s_overflow_still_answers_for_a_pending_name() {
    use super::store::{BUILD_SEED_HOLD, FILTER_DRAIN_FAILS, FILTER_PAUSED, FILTER_PAUSE_AT, HOLD_ARRIVED};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_DRAIN_FAILS.store(false, Release);
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
            BUILD_SEED_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("drain-overflow-answer.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    db.branch_wait_name_filter();
    FILTER_DRAIN_FAILS.store(true, Release);
    FILTER_PAUSE_AT.store(3, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while FILTER_PAUSED.load(Acquire) != 3 {
        trunk.create_branch(&format!("fill-{n:05}")).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no filter growth reached its swap ({n} creates)");
    }
    trunk.create_branch("victim").unwrap();
    BUILD_SEED_HOLD.store(1, Release);
    FILTER_PAUSE_AT.store(0, Release);
    while db.branches.name_filter_state_for_test().0 != "Overflowed" {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the failed drain never overflowed the filter");
    }
    // The overflow owes a rebuild; a create starts it, and it holds before its seed.
    trunk.create_branch("starts-the-rebuild").unwrap();
    while BUILD_SEED_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the overflow's rebuild never began");
    }
    assert_eq!(db.branches.name_filter_state_for_test().0, "Overflowed", "premise: still Overflowed, held before the rebuild's seed");
    let absent = db.branches.name_filter_says_absent_for_test("victim");
    BUILD_SEED_HOLD.store(0, Release);
    db.branch_wait_name_filter();
    assert!(!absent, "CLAIM: after a failed drain the overflowed filter answered absent for a name only its pending table held");
}

/// Review filter3 MED-1: LOW-F2's class has a red that can fire again. Since N1 the first refusal
/// in `a_refusing_answer_s_table_is_not_freed_under_the_store_mutex` respills into the spare, so
/// nothing is retired and that test passes whatever `retire_table` does. Here the burst goes on
/// until the answer ENDS (`name_filter_refusals_for_test().3`: the spare is used by the first
/// refusal, so a later one ends it and retires the old sink and the spare, both their last
/// references). Premise: the filter then answers nothing. No note may free anything. Mutant
/// `filter_retire_frees_in_place` (retired tables freed under the mutex) is its killer.
#[test]
fn an_answer_that_ends_on_a_refusal_frees_nothing_under_the_store_mutex() {
    use super::store::{FILTER_NOTE_FREED_MAX, HOLD_ARRIVED, NAME_SCAN_HOLD};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            NAME_SCAN_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("answer-ends-free.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    trunk.create_branch("held").unwrap();
    db.branch_wait_name_filter();
    NAME_SCAN_HOLD.store(1, Release);
    let t = std::time::Instant::now();
    let mut n = 0u32;
    while NAME_SCAN_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        let name = format!("cycle-{n:05}");
        trunk.create_branch(&name).unwrap();
        db.drop_branch(&name).unwrap();
        n += 1;
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no natural shrink's rebuild reached its scan ({n} cycles)");
    }
    let ended0 = db.branches.name_filter_refusals_for_test().3;
    FILTER_NOTE_FREED_MAX.store(0, Release);
    while db.branches.name_filter_refusals_for_test().3 == ended0 {
        trunk.create_branch(&format!("burst-{n:05}")).unwrap();
        n += 1;
        assert!(n < 1 << 20, "premise: the Building answer never ended");
    }
    let built = db.branches.name_filter_stats().0;
    let freed = FILTER_NOTE_FREED_MAX.load(Relaxed);
    let respilled = db.branches.name_filter_refusals_for_test().2;
    NAME_SCAN_HOLD.store(0, Release);
    db.branch_wait_name_filter();
    assert!(respilled >= 1, "premise: the first refusal respilled into the spare before a later one ended the answer");
    assert!(!built, "premise: the ended answer leaves the filter answering nothing");
    assert_eq!(freed, 0, "CLAIM: a note freed {freed} bytes under the store mutex when the answer ended (its tables are retired, freed after the unlock)");
}

/// Review filter3 L3: the BUILD's failed drain overflows answering from its new table and its pending
/// one, as the growth's does; a name noted into the pending table only (created while the reopen's
/// build is held at its scan, so the scan and the table lack it) must not be answered absent once
/// Overflowed, held there by the rebuild's seed hold. Failpoint `BUILD_DRAIN_FAILS`; killer of
/// mutant `filter_build_answer_drops_pending` (the growth site's mutant had the only killer).
#[test]
fn a_failed_build_drain_s_overflow_still_answers_for_a_pending_name() {
    use super::store::{BUILD_DRAIN_FAILS, BUILD_SEED_HOLD, HOLD_ARRIVED, NAME_SCAN_HOLD};
    use std::sync::atomic::Ordering::{Acquire, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BUILD_DRAIN_FAILS.store(false, Release);
            BUILD_SEED_HOLD.store(0, Release);
            NAME_SCAN_HOLD.store(0, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("build-drain-overflow.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..16 {
            trunk.create_branch(&format!("kept-{i:02}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    NAME_SCAN_HOLD.store(1, Release);
    BUILD_DRAIN_FAILS.store(true, Release);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    let t = std::time::Instant::now();
    while NAME_SCAN_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: the reopen's build never reached its scan");
    }
    let trunk = db.connect().unwrap();
    trunk.create_branch("victim").unwrap();
    NAME_SCAN_HOLD.store(0, Release);
    while db.branches.name_filter_state_for_test().0 != "Overflowed" {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the failed build drain never overflowed the filter");
    }
    assert!(!BUILD_DRAIN_FAILS.load(Acquire), "premise: the failpoint was consumed by the build");
    BUILD_SEED_HOLD.store(1, Release);
    trunk.create_branch("starts-the-rebuild").unwrap();
    while BUILD_SEED_HOLD.load(Acquire) != 1 | HOLD_ARRIVED {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: the overflow's rebuild never began");
    }
    assert_eq!(db.branches.name_filter_state_for_test().0, "Overflowed", "premise: still Overflowed, held before the rebuild's seed");
    let absent = db.branches.name_filter_says_absent_for_test("victim");
    BUILD_SEED_HOLD.store(0, Release);
    db.branch_wait_name_filter();
    assert!(!absent, "CLAIM: after a failed build drain the overflowed filter answered absent for a name only its pending table held");
}

/// Review filter3 L5: a build attempt whose catalog scan failed frees nothing inside its store-mutex
/// hold (N2's rule: no filter-thread hold frees anything). At dd9d97b1d the failed-scan arm's error
/// (a String) dropped before the guard on `return FilterBuild::Failed`. Failpoint
/// `NAME_SCAN_FAILS` on a reopen's build; the retry builds it.
#[test]
fn a_failed_build_scan_frees_nothing_under_the_store_mutex() {
    use super::store::{FILTER_HOLD_FREED_MAX, FILTER_HOLD_FREES_ARMED, NAME_SCAN_FAILS};
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_HOLD_FREES_ARMED.store(false, Release);
            NAME_SCAN_FAILS.store(false, Release);
        }
    }
    let _r = Reset;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("failed-scan-free.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..16 {
            trunk.create_branch(&format!("kept-{i:02}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    FILTER_HOLD_FREED_MAX.store(0, Release);
    FILTER_HOLD_FREES_ARMED.store(true, Release);
    NAME_SCAN_FAILS.store(true, Release);
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    db.branch_wait_name_filter();
    FILTER_HOLD_FREES_ARMED.store(false, Release);
    assert!(!NAME_SCAN_FAILS.load(Acquire), "premise: the reopen's build met the failing scan");
    assert_eq!(db.branch_name_filter_stats().4, 1, "premise: one failed scan was counted");
    let freed = FILTER_HOLD_FREED_MAX.load(Relaxed);
    assert_eq!(freed, 0, "CLAIM: a name-filter thread freed {freed} bytes inside a store-mutex hold (a failed scan's error)");
}

/// Class (e), the store mutex's lifecycle audit (DECISIONS 994f7d41e0; the lead's order with
/// nf-adversary-2's MED-2 and M4 folded in): every store-mutex hold on the create path does NO syscall,
/// NO allocation and NO catalog statement. One instrument for the whole class: every outermost hold
/// is recorded with the site that took it (`#[track_caller]` through `StoreMutex::lock` and its
/// wrappers), its thread, and what it did, each counted for the holding thread
/// (`budget_probe::HoldRec`: syscalls attributed by `attribute`, exact under feshim). Audited, at `n`
/// live named branches of a D2 catalog store:
/// - a reopen (the name filter's open seed, M4);
/// - 20 named creates, unnamed forks, connects by name, first writes, disconnects and deletes;
/// - 3 fuzzy checkpoints (capture and install);
/// - a name-filter growth, and a rebuild after a forced overflow (the drain failpoint; MED-2).
/// The claim lists every (operation, site) whose worst hold did any of the three, so each instance of
/// the class is named where it lives.
fn lifecycle_audit(n: u32) {
    use super::budget_probe::{self as probe, HoldRec, AUDIT, AUDIT_LOG, AUDIT_LOST};
    use std::sync::atomic::Ordering::{Relaxed, Release};
    let _s = serial();
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            AUDIT.store(false, Release);
            super::budget_probe::arm(false);
            super::store::FILTER_DRAIN_FAILS.store(false, Release);
        }
    }
    let _d = Disarm;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("audit.db");
    // The population, before anything is measured, at durability OFF (D0), then checkpointed;
    // every audited operation runs after a reopen at D2 (lead order 2026-10-16; FLAGGED fixture
    // change: the 10^6 arm populated one create per F_FULLFSYNC and twice hit its budget still
    // populating, 90 min and 4 h, raws l70 and l87). What the audit claims is unchanged: the
    // store-mutex holds of the create path at n LIVE branches.
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..n {
            trunk.create_branch(&format!("pop-{i:07}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    // Premise: the n branches are live after the reopen, and checkpointed (the reopen replays a
    // short log, not n creates).
    {
        let db = reopen(&path, opts(true, SyncClass::FullFsync), incarnation);
        let live = db.branch_stats().unwrap().live_branches as u64;
        let replayed = db.branch_open_stats().records;
        assert!(live >= u64::from(n), "premise: {live} branches live after the population, not {n}");
        assert!(replayed < 1_000, "premise: the population was not checkpointed: the reopen replayed {replayed} records");
    }
    {
        let mut log = AUDIT_LOG.lock().unwrap();
        log.clear();
        log.reserve(1 << 20);
    }
    let mut marks: Vec<(&str, usize)> = Vec::new();
    let mark = |marks: &mut Vec<(&'static str, usize)>, op: &'static str| marks.push((op, AUDIT_LOG.lock().unwrap().len()));
    probe::prime();
    probe::arm(true);
    AUDIT.store(true, Release);
    mark(&mut marks, "open");
    let db = reopen(&path, opts(true, SyncClass::FullFsync), incarnation);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    for i in 0..20u32 {
        let name = format!("a-{i:03}");
        mark(&mut marks, "named_create");
        trunk.create_branch(&name).unwrap();
        mark(&mut marks, "unnamed_fork");
        let _ = trunk.fork_branch().unwrap().into_id();
        mark(&mut marks, "connect_named");
        let c = db.connect_named(&name).unwrap();
        mark(&mut marks, "first_write");
        c.execute(&format!("UPDATE t SET v = 'a{i}' WHERE id = {}", 1 + i % 50)).unwrap();
        mark(&mut marks, "disconnect");
        drop(c);
        mark(&mut marks, "delete");
        db.drop_branch(&name).unwrap();
    }
    for _ in 0..3 {
        mark(&mut marks, "fuzzy_checkpoint");
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
    }
    mark(&mut marks, "filter_growth");
    let (_, entries0, _, builds0, _) = db.branches.name_filter_stats();
    // FLAGGED fixture change (2026-10-16): at n >= 10^5 the growth is owed by the test hook, not
    // by about n more D2 creates filling the table (the 10^6 arm timed out in this loop at D0
    // population too, raw l96; the D0 population itself costs about 0.7 ms a create in debug, raw
    // l102). Its creates, and its growth's holds, are the same; only how it is started differs.
    let force = n >= 100_000;
    // Review L2d M3: a threshold the growth's creates cross, so a checkpoint the worker starts
    // (`run_owed_start`'s hold, slice 3) falls in the audited window; the forced arm's handful of
    // creates crosses only a 1-byte one. Asserted below as a premise.
    let _threshold = Threshold::set(if force { 1 } else { 8 << 10 });
    let growth_from = AUDIT_LOG.lock().unwrap().len();
    if force {
        db.branches.force_name_filter_growth_for_test();
    }
    let mut k = 0u32;
    while db.branches.name_filter_stats().3 == builds0 && k < 4 * n.max(1024) {
        trunk.create_branch(&format!("g-{k:07}")).unwrap();
        k += 1;
    }
    db.branch_wait_name_filter();
    mark(&mut marks, "filter_rebuild");
    super::store::FILTER_DRAIN_FAILS.store(true, Release);
    let rebuilds0 = db.branches.name_filter_state_for_test().1;
    if force {
        db.branches.force_name_filter_growth_for_test();
    }
    let mut r = 0u32;
    while db.branches.name_filter_state_for_test().1 == rebuilds0 && r < 8 * n.max(1024) {
        trunk.create_branch(&format!("r-{r:07}")).unwrap();
        r += 1;
    }
    super::store::FILTER_DRAIN_FAILS.store(false, Release);
    trunk.create_branch("after-rebuild").unwrap();
    db.branch_wait_name_filter();
    mark(&mut marks, "end");
    AUDIT.store(false, Release);
    probe::arm(false);
    let log: Vec<HoldRec> = AUDIT_LOG.lock().unwrap().clone();
    assert_eq!(AUDIT_LOST.load(Relaxed), 0, "premise: the audit log lost holds (its room was too small)");
    assert!(db.branches.name_filter_stats().3 > builds0, "premise: no filter growth happened ({entries0} entries)");
    assert!(db.branches.name_filter_state_for_test().1 > rebuilds0, "premise: the forced overflow never rebuilt the filter");
    // Group each hold under the operation whose span it fell in.
    let mut worst: std::collections::BTreeMap<(&str, String), (u64, u64, u64, u64, u64, bool)> = Default::default();
    for (w, pair) in marks.windows(2).enumerate() {
        let _ = w;
        let (op, from) = pair[0];
        let to = pair[1].1;
        for h in &log[from..to] {
            let site = format!("{}:{}", h.site.file().rsplit('/').next().unwrap_or(""), h.site.line());
            let e = worst.entry((op, site)).or_default();
            e.0 += 1;
            e.1 = e.1.max(h.syscalls.unwrap_or(0));
            e.2 = e.2.max(h.allocs);
            e.3 = e.3.max(h.alloc_bytes);
            e.4 = e.4.max(h.cat_stmts);
            e.5 |= h.syscalls.is_none();
        }
    }
    let mut over = Vec::new();
    for ((op, site), (holds, sys, allocs, bytes, stmts, unattributed)) in &worst {
        let line = format!(
            "{op} @ {site}: {holds} holds, worst {sys} syscalls{}, {allocs} allocs ({bytes} B), {stmts} catalog stmts",
            if *unattributed { " (some unattributable)" } else { "" }
        );
        println!("audit n={n}: {line}");
        if *sys > 0 || *allocs > 0 || *stmts > 0 {
            over.push(line);
        }
    }
    assert!(!worst.is_empty(), "premise: the audit recorded no hold");
    let owed_site = super::store::OWED_START_SITE.get().copied();
    let owed_holds = owed_site.map_or(0, |s| log[growth_from..].iter().filter(|h| std::ptr::eq(h.site, s) || (h.site.file() == s.file() && h.site.line() == s.line())).count());
    assert!(
        owed_holds >= 1,
        "premise: no hold at run_owed_start's site ({owed_site:?}) after the growth began: the checkpoint worker's start was not audited"
    );
    assert!(
        over.is_empty(),
        "CLAIM: {} store-mutex sites on the create path did a syscall, an allocation or a catalog statement while held (n = {n}): {over:#?}",
        over.len()
    );
}

#[test]
fn the_create_path_s_store_mutex_holds_do_no_syscall_allocation_or_catalog_statement() {
    lifecycle_audit(10_000);
}

/// The audit at 10^6 live branches (the lead's second scale): long to populate in a debug build, so
/// run by name.
#[test]
#[ignore = "class (e) audit at 10^6: run by name with --ignored"]
fn the_create_path_s_store_mutex_holds_do_no_syscall_allocation_or_catalog_statement_at_a_million() {
    lifecycle_audit(1_000_000);
}

/// Lead conditions on the name filter (DECISIONS, the acceptance of the one-state fix): the table
/// listing the regrowth tests read (`per_branch_tables_for_test`) prices the filter's tables at their
/// REAL bytes (8 per slot; 4cb167221 listed them at 0, which blinded the regrowth test), and a growth
/// records how many entries it moved (`moved_max`, which the churn check reads). A catalog store's
/// filter grown past one doubling, waited out.
#[test]
fn the_name_filter_lists_its_real_bytes_and_records_its_growth() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("filter-bytes.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    for i in 0..1_500u32 {
        trunk.create_branch(&format!("b{i:05}")).unwrap();
    }
    db.branch_wait_name_filter();
    let (built, entries, moved_max, builds, _) = db.branches.name_filter_stats();
    assert!(built && entries >= 1_500, "premise: the filter holds the 1,500 names ({entries})");
    assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "premise (review m1-2c N5): built means Ready, not Stale or an overflow's answer");
    assert!(builds >= 1, "premise: the filter grew past its first table ({builds} growths)");
    let listed: Vec<_> = db
        .branches
        .per_branch_tables_for_test()
        .into_iter()
        .filter(|(name, ..)| name.starts_with("names.filter"))
        .collect();
    let priced = listed.iter().any(|(_, cap, bytes)| *cap > 0 && *bytes == *cap * 8);
    assert!(priced, "CLAIM: the filter's table is not listed at its real bytes (8 per slot): {listed:?}");
    assert!(moved_max > 0, "CLAIM: a growth of the filter recorded no entries moved (moved_max {moved_max})");
}

/// The name filter's concurrency oracle (lead condition on the filter, and review REVIEW-claude-namefilter
/// M3): since `says_absent` lets a create skip the catalog, a name the filter loses is a DUPLICATE
/// name. Four threads create named branches while a growth is held at each of its stages (1-5,
/// `FILTER_PAUSE_AT`), and once with the drain copy failing (`FILTER_DRAIN_FAILS`: the filter
/// overflows, and its owed rebuild must restore it). Once everything has settled, every name any
/// thread created is asked of the FILTER itself (`name_filter_says_absent_for_test`), so the resident
/// names (`names.map`, which `name_lookup` consults first) cannot answer for it: none may be
/// "absent". Mutant `name_filter_swap_skips_pending` (the planted race: the swap installs the new
/// table without copying the pending one) must turn it red.
#[test]
fn the_name_filter_never_answers_absent_for_a_noted_name_across_its_growth() {
    use super::store::{FILTER_DRAIN_FAILS, FILTER_PAUSED, FILTER_PAUSE_AT};
    use std::sync::atomic::{AtomicBool, Ordering::{Acquire, Release}};
    let _s = serial();
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FILTER_DRAIN_FAILS.store(false, Release);
            FILTER_PAUSE_AT.store(0, Release);
            FILTER_PAUSED.store(0, Release);
        }
    }
    let _r = Reset;
    let mut lost = Vec::new();
    // Stages 1-5 hold a growth; 6 and 7 hold a BUILD, in `Draining` before its pending copy and
    // in `Building` before its scan (review REVIEW-claude-namefilter-2 MED-3), reached by the
    // rebuild a failed drain owes. Mutant `name_filter_build_skips_pending` must turn stage 7 red.
    for (stage, drain_fails) in [(1u8, false), (2, false), (3, false), (4, false), (5, false), (5, true), (6, true), (7, true)] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("filter-oracle.db"), opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("first").unwrap();
        FILTER_PAUSED.store(0, Release);
        FILTER_DRAIN_FAILS.store(drain_fails, Release);
        FILTER_PAUSE_AT.store(stage, Release);
        let stop = Arc::new(AtomicBool::new(false));
        // Every create any worker made (review MED-3: the premise that the held window saw one).
        let creates = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let workers: Vec<_> = (0..4)
            .map(|k| {
                let (db, stop, creates) = (db.clone(), stop.clone(), creates.clone());
                std::thread::spawn(move || {
                    let conn = db.connect().unwrap();
                    let mut made = Vec::new();
                    let mut i = 0u32;
                    // Uncapped while a stage is held (the cap ended the window early, review MED-3);
                    // `stop` ends it.
                    while !stop.load(Acquire) && i < 40_000 {
                        let name = format!("s{stage}-t{k}-{i}");
                        conn.create_branch(&name).unwrap();
                        made.push(name);
                        creates.fetch_add(1, Release);
                        i += 1;
                    }
                    made
                })
            })
            .collect();
        let t = std::time::Instant::now();
        while FILTER_PAUSED.load(Acquire) != stage {
            assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: stage {stage}: no filter thread reached the stage");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        // The window is held until at least 16 creates landed in it (not for a fixed time).
        let at_pause = creates.load(Acquire);
        let t = std::time::Instant::now();
        while creates.load(Acquire) < at_pause + 16 {
            assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: stage {stage}: no create ran while the stage was held");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let in_window = creates.load(Acquire) - at_pause;
        FILTER_PAUSE_AT.store(0, Release);
        assert!(in_window >= 1, "premise: stage {stage}: no create landed while the stage was held");
        std::thread::sleep(std::time::Duration::from_millis(30));
        stop.store(true, Release);
        let made: Vec<String> = workers.into_iter().flat_map(|w| w.join().unwrap()).collect();
        FILTER_DRAIN_FAILS.store(false, Release);
        // One more create starts whatever the filter owes (a rebuild after the overflow), then all
        // filter work is waited out.
        trunk.create_branch(&format!("s{stage}-last")).unwrap();
        db.branch_wait_name_filter();
        let (state, rebuilds) = db.branches.name_filter_state_for_test();
        assert_eq!(state, "Ready", "premise: stage {stage} (drain fails: {drain_fails}): the filter did not settle Ready");
        if drain_fails {
            assert!(rebuilds >= 1, "premise: the failed drain did not overflow the filter into a rebuild");
        }
        for name in made.iter().chain(std::iter::once(&"first".to_string())) {
            if db.branches.name_filter_says_absent_for_test(name) {
                lost.push(format!("stage {stage} (drain fails: {drain_fails}): {name}"));
            }
        }
        assert!(made.len() > 512, "premise: stage {stage}: too few creates to grow the filter ({})", made.len());
        // Judged per stage, so a later stage's premise cannot hide what an earlier one lost.
        assert!(
            lost.is_empty(),
            "CLAIM: the name filter answered absent for {} names it was given (a create would make a duplicate): {:?}",
            lost.len(),
            &lost[..lost.len().min(8)]
        );
    }
}

/// Lead ruling (DECISIONS 2026-10-15, after the attribution: the n1e4 create's extra allocation is
/// the name filter's B-tree splitting a full leaf, raws l35-diag-alloc): a create allocates NOTHING
/// in the name filter, by construction, also while the filter grows. A catalog store with 10,000
/// named branches, reopened so the filter is built from the catalog as the budgets' n1e4 cell
/// builds it, then 8,000 more named creates (past any doubling of a table sized for 10,000): no
/// allocation inside `NameFilter::note` (`FILTER_NOTE_ALLOCS`). Then, every name a branch holds is
/// still refused to a second create, and the filter grew rather than stopped answering.
#[test]
fn a_named_create_allocates_nothing_in_the_name_filter_as_it_grows() {
    use std::sync::atomic::Ordering::Relaxed;
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("filter-grow.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..10_000 {
            trunk.create_branch(&format!("pre-{i:05}")).unwrap();
        }
        db.incarnation
    };
    let db = reopen(&path, opts(true, SyncClass::Off), incarnation);
    db.branch_wait_name_filter();
    let (built, entries0, _, builds0, _) = db.branches.name_filter_stats();
    assert!(built && entries0 >= 10_000, "premise: the reopened store's name filter was built from its 10,000 names ({entries0})");
    assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "premise (review m1-2c N5): built means Ready, not Stale or an overflow's answer");
    let trunk = db.connect().unwrap();
    let before = super::store::FILTER_NOTE_ALLOCS.load(Relaxed);
    for i in 0..8_000 {
        trunk.create_branch(&format!("new-{i:05}")).unwrap();
    }
    let allocs = super::store::FILTER_NOTE_ALLOCS.load(Relaxed) - before;
    assert_eq!(allocs, 0, "CLAIM: 8,000 named creates allocated {allocs} times inside the name filter (under the store mutex)");
    db.branch_wait_name_filter();
    for i in (0..10_000).step_by(97) {
        assert!(trunk.create_branch(&format!("pre-{i:05}")).is_err(), "a held name pre-{i:05} was created twice");
    }
    for i in (0..8_000).step_by(89) {
        assert!(trunk.create_branch(&format!("new-{i:05}")).is_err(), "a held name new-{i:05} was created twice");
    }
    let (built, entries, _, builds, _) = db.branches.name_filter_stats();
    assert!(built && entries >= 18_000, "the name filter stopped answering, or lost names, as it grew: built {built}, {entries} entries");
    assert_eq!(db.branches.name_filter_state_for_test().0, "Ready", "premise (review m1-2c N5): built means Ready, not Stale or an overflow's answer");
    assert!(builds > builds0, "premise: the name filter never grew past the table it was built with ({builds0} -> {builds} builds)");
}

/// Class (e)'s delete site (release_checked: 657 allocations and 4 catalog statements in one hold at
/// 10^4 and 10^6, raws l104, l103): the delete the audit makes, traced span by span (`held_trace`:
/// allocations and catalog statements at each point), at 10^4 named branches populated at D0, at
/// D2. Prints each of 20 deletes' spans.
#[test]
#[ignore = "diagnostic: run by name with --ignored --nocapture"]
fn diag_the_delete_hold_by_span() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("del.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..10_000u32 {
            trunk.create_branch(&format!("pop-{i:07}")).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.incarnation
    };
    let db = reopen(&path, opts(true, SyncClass::FullFsync), incarnation);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    for i in 0..20u32 {
        let name = format!("a-{i:03}");
        trunk.create_branch(&name).unwrap();
        let c = db.connect_named(&name).unwrap();
        c.execute(&format!("UPDATE t SET v = 'a{i}' WHERE id = {}", 1 + i % 50)).unwrap();
        drop(c);
        let a0 = super::budget_probe::thread_allocs().2;
        let s0 = super::budget_probe::thread_cat_stmts();
        super::store::HELD_TRACE.with(|t| *t.borrow_mut() = Some(Vec::with_capacity(64)));
        db.drop_branch(&name).unwrap();
        let trace = super::store::HELD_TRACE.with(|t| t.borrow_mut().take()).unwrap_or_default();
        let (mut la, mut ls) = (a0, s0);
        let steps: Vec<String> = trace
            .iter()
            .map(|(l, a, st)| {
                let r = format!("{l}+{}a/{}s", a - la, st - ls);
                la = *a;
                ls = *st;
                r
            })
            .collect();
        println!("FE_DELETE {name}: [{}]", steps.join(" "));
    }
}

/// Diagnostic for the 10^6 audit arm's population (it timed out at D0 in 90 min, raw l96): the wall
/// time of each 20,000 named creates at durability OFF into a catalog store, up to `FE_POP_N`
/// (default 200,000), so the cost per create and its slope in N are measured, not guessed. Prints.
#[test]
#[ignore = "diagnostic: run by name with --ignored --nocapture"]
fn diag_d0_population_rate() {
    let _s = serial();
    let n: u32 = std::env::var("FE_POP_N").ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("pop.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let mut t = std::time::Instant::now();
    for i in 0..n {
        trunk.create_branch(&format!("pop-{i:07}")).unwrap();
        if (i + 1) % 20_000 == 0 {
            println!("FE_POP {} creates: last 20,000 in {:.1} s ({:.0} us each)", i + 1, t.elapsed().as_secs_f64(), t.elapsed().as_secs_f64() * 1e6 / 20_000.0);
            t = std::time::Instant::now();
        }
    }
}

/// Lead order (2026-10-15, attribution item 1): `a_fuzzy_checkpoint_issues_at_most_two_syscalls_under_
/// the_store_mutex` fails about 3 runs in 10 at one sha (raws l28-ckpt-r*), its extra syscalls in
/// the capture spans. Its instrument is the TASK's syscall count, so a span counts every thread's.
/// This diagnostic runs its schedule for 20 rounds with every thread's CPU time read at each point
/// as well (`CKPT_HELD_THREADS`, Mach traps only), and prints each round's spans with the threads
/// other than the checkpoint thread that ran in them. The hypothesis under test, as its CLAIM: every
/// span holding more syscalls than the reading's own had another thread running in it. Blind spot:
/// a table is read just after its point's syscall count, so a thread that ran between the count and
/// the table at the span's START is missed, and one between them at its END is charged to the span.
#[test]
#[ignore = "diagnostic (lead attribution order): run by name with --ignored --nocapture"]
fn diag_fuzzy_checkpoint_held_syscalls_by_thread() {
    use super::budget_probe::ran_between;
    let _s = serial();
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            super::store::CKPT_THREADS_ARMED.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _d = Disarm;
    struct BigMin;
    impl Drop for BigMin {
        fn drop(&mut self) {
            super::store::CKPT_BIG_MIN.store(16 << 10, std::sync::atomic::Ordering::Release);
        }
    }
    let _b = BigMin;
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("ckpt-diag.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    super::store::CKPT_THREADS_ARMED.store(true, std::sync::atomic::Ordering::Release);
    let (mut over, mut unexplained, mut rows) = (0u32, Vec::new(), Vec::new());
    for round in 0..20 {
        for _ in 0..30 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        super::store::CKPT_HELD_SYSCALLS.lock().unwrap().clear();
        {
            let mut t = super::store::CKPT_HELD_THREADS.lock().unwrap();
            t.clear();
            t.reserve(32);
        }
        // Round 0 is the recorder's fire-check: every allocation and free of the span recorded.
        super::store::CKPT_BIG_MIN.store(if round == 0 { 1 } else { 16 << 10 }, std::sync::atomic::Ordering::Release);
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: round {round}: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        if round == 0 {
            assert!(
                !super::store::CKPT_BIG.lock().unwrap().is_empty(),
                "premise: the span recorder recorded nothing at a 1-byte threshold: it cannot be trusted at 16 KiB"
            );
        }
        let points = super::store::CKPT_HELD_SYSCALLS.lock().unwrap().clone();
        let tables = super::store::CKPT_HELD_THREADS.lock().unwrap().clone();
        assert_eq!(points.len(), tables.len(), "premise: round {round}: a point without its thread table");
        // Each span is skipped for the thread that recorded its end (capture points are the caller's,
        // install points the checkpoint thread's).
        let recorder = |j: usize| tables[j].2.iter().find(|x| x.me).map_or(0, |x| x.id);
        let at = |label: &str| points.iter().position(|(l, _)| *l == label);
        // The original test's five spans and its arithmetic (each span minus one reading), for the
        // over-budget verdict; the finer install sub-spans printed as raw deltas.
        let spans = [
            ("capture.begin", "capture.matured"),
            ("capture.matured", "capture.captured"),
            ("install.locked", "install.quiesced"),
            ("install.quiesced", "install.installed"),
            ("install.installed", "install.marked"),
        ];
        let fine = [
            ("install.quiesced", "install.snapshot_ended"),
            ("install.snapshot_ended", "install.before_cut"),
            ("install.before_cut", "install.cut_finished"),
            ("install.cut_finished", "install.installed"),
        ];
        let mut total = 0u64;
        let mut line = format!("round {round}:");
        let mut spans_over = Vec::new();
        let names = |ran: &[(u64, String, u64)]| ran.iter().map(|(id, name, ns)| format!("{name}#{id}:{ns}ns")).collect::<Vec<_>>();
        for (a, b) in spans {
            let (Some(i), Some(j)) = (at(a), at(b)) else {
                line += &format!(" [{a}..{b} missing]");
                continue;
            };
            let n = (points[j].1 - points[i].1).saturating_sub(1);
            total += n;
            let ran = ran_between(&tables[i].1, &tables[j].2, recorder(j));
            line += &format!(" {a}..{b}={n} ran={:?}", names(&ran));
            if n > 0 {
                spans_over.push((a, b, n, ran.is_empty()));
            }
        }
        line += " | raw:";
        for (a, b) in fine {
            if let (Some(i), Some(j)) = (at(a), at(b)) {
                let ran = ran_between(&tables[i].1, &tables[j].2, recorder(j));
                line += &format!(" {a}..{b}={} ran={:?}", points[j].1 - points[i].1, names(&ran));
            }
        }
        line += &format!(" total={total}");
        let big = std::mem::take(&mut *super::store::CKPT_BIG.lock().unwrap());
        let frames = |t: &str| {
            t.lines()
                .take(1)
                .chain(t.lines().filter(|l| l.contains("turso_core::") && !l.contains("budget_probe")).take(5))
                .collect::<Vec<_>>()
                .join(" <- ")
        };
        line += &format!(" | big in cut_finished..installed: {:?}", big.iter().map(|t| frames(t)).collect::<Vec<_>>());
        if total > 2 {
            over += 1;
            for (a, b, n, alone) in &spans_over {
                if *alone {
                    unexplained.push(format!("round {round} {a}..{b}: {n} syscalls with no other thread running"));
                }
            }
        }
        rows.push(line);
    }
    let r0 = super::budget_probe::unix_syscalls().unwrap();
    let r1 = super::budget_probe::unix_syscalls().unwrap();
    println!("one reading of the task's syscall count costs {} BSD syscalls (two back-to-back readings)", r1 - r0);
    for r in &rows {
        println!("{r}");
    }
    assert!(over > 0, "premise: no round went over two held syscalls in 20, so the attribution was not exercised");
    assert!(unexplained.is_empty(), "CLAIM: ATTRIBUTION: an over-budget span's syscalls were the checkpoint thread's own: {unexplained:?}");
}

/// Lead finding (artie 79ef7d2caf, the wire's per-window data): the embedded create, write and
/// delete are bimodal in syscalls (create 8 or 10). The task's syscall count per operation, with
/// every OTHER thread's CPU time read around it (`budget_probe::thread_table`, Mach traps only),
/// at D2 over a catalog store and a snapshot store: each operation's count grouped by value, and
/// for each value how many of its operations had another thread run inside them, and which.
/// Printed only: which mode is foreign is what this measures.
#[test]
#[ignore = "diagnostic (lead attribution order): run by name with --ignored --nocapture"]
fn diag_create_write_delete_syscalls_by_thread() {
    use super::budget_probe::{ran_between, thread_table, unix_syscalls, ThreadCpu};
    let _s = serial();
    struct Quiet;
    impl Drop for Quiet {
        fn drop(&mut self) {
            super::store::CONFIRM_QUIET_MS.store(0, std::sync::atomic::Ordering::Release);
        }
    }
    let _q = Quiet;
    // Second pass: the confirmation word held back (the budgets' regime, `hold_word`), so the
    // writer is woken by the first landing only and sleeps after.
    for (catalog, held) in [(true, false), (false, false), (true, true), (false, true)] {
        super::store::CONFIRM_QUIET_MS.store(if held { 1 << 40 } else { 0 }, std::sync::atomic::Ordering::Release);
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("ops-diag.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let mut by: std::collections::BTreeMap<(&str, u64), (u32, u32, std::collections::BTreeMap<String, u32>)> = Default::default();
        let mut measure = |op: &'static str, f: &mut dyn FnMut()| {
            let mut t0 = [ThreadCpu::EMPTY; 48];
            let mut t1 = [ThreadCpu::EMPTY; 48];
            let s0 = unix_syscalls().unwrap();
            let _ = thread_table(&mut t0);
            f();
            let s1 = unix_syscalls().unwrap();
            let _ = thread_table(&mut t1);
            let me = t0.iter().find(|x| x.me).map_or(0, |x| x.id);
            let ran = ran_between(&t0, &t1, me);
            let e = by.entry((op, (s1 - s0).saturating_sub(1))).or_default();
            e.0 += 1;
            if !ran.is_empty() {
                e.1 += 1;
            }
            for (_, name, _) in ran {
                *e.2.entry(name).or_default() += 1;
            }
        };
        for i in 0..200 {
            let name = format!("d-{i:04}");
            measure("create", &mut || {
                trunk.create_branch(&name).unwrap();
            });
            let c = db.connect_named(&name).unwrap();
            measure("write", &mut || {
                c.execute(&format!("UPDATE t SET v = 'w{i}' WHERE id = {}", 1 + i % 50)).unwrap();
            });
            drop(c);
            measure("delete", &mut || {
                db.drop_branch(&name).unwrap();
            });
        }
        for ((op, n), (count, foreign, names)) in &by {
            println!("catalog={catalog} word_held={held} {op}: {n} syscalls x{count}, {foreign} with another thread running {names:?}");
        }
    }
}

/// Checkpoint FIX (budget a_checkpoint_issues_at_most_two_syscalls_under_the_store_mutex: 52 over 3
/// checkpoints in ckpt_n10, budget 2 each): a fuzzy checkpoint's syscalls inside its two store-mutex
/// holds, the capture's and the install's, attributed to points (`store::CKPT_HELD_SYSCALLS`). A D2
/// catalog store, checkpoints after 30 creates each.
///
/// FLAGGED TEST EDIT, lead-ordered (class (b) and ruling (b), DECISIONS 994f7d41e0): the count is
/// the TASK's, so a span's syscalls are the recording thread's only if no other thread ran inside
/// it (`store::CKPT_HELD_THREADS`, a thread table before and after every point; attribution by
/// exclusion). A round with an unattributable span is not judged: rounds run until 3 are fully
/// attributable (at most 12), and fewer is a refusal, never a pass. And the span no longer
/// subtracts 1 "for the reading", which costs 0 BSD syscalls (measured: raw l35-diag-ckpt): a
/// tightening, so newly red cells are real.
#[test]
fn a_fuzzy_checkpoint_issues_at_most_two_syscalls_under_the_store_mutex() {
    use super::budget_probe::ran_between;
    let _s = serial();
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            super::store::CKPT_THREADS_ARMED.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _d = Disarm;
    super::store::CKPT_THREADS_ARMED.store(true, std::sync::atomic::Ordering::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("ckpt-held.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let (mut over, mut judged, mut refused) = (Vec::new(), 0, Vec::new());
    for round in 0..12 {
        if judged == 3 {
            break;
        }
        for _ in 0..30 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        {
            // Reserved too (review delete LOW-7): a push that reallocates mid-checkpoint would land
            // its syscalls, if any, in the next span.
            let mut s = super::store::CKPT_HELD_SYSCALLS.lock().unwrap();
            s.clear();
            s.reserve(32);
        }
        {
            let mut t = super::store::CKPT_HELD_THREADS.lock().unwrap();
            t.clear();
            t.reserve(32);
        }
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: round {round}: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        let points = super::store::CKPT_HELD_SYSCALLS.lock().unwrap().clone();
        let tables = super::store::CKPT_HELD_THREADS.lock().unwrap().clone();
        assert_eq!(points.len(), tables.len(), "premise: round {round}: a point without its thread tables");
        let at = |label: &str| points.iter().position(|(l, _)| *l == label);
        let mut foreign = Vec::new();
        let mut span = |a: &str, b: &str| match (at(a), at(b)) {
            (Some(i), Some(j)) => {
                let me = tables[j].2.iter().find(|x| x.me).map_or(0, |x| x.id);
                for (_, name, _) in ran_between(&tables[i].1, &tables[j].2, me) {
                    foreign.push(format!("{a}..{b}: {name}"));
                }
                points[j].1 - points[i].1
            }
            _ => u64::MAX,
        };
        let capture = (span("capture.begin", "capture.matured"), span("capture.matured", "capture.captured"));
        let install = (
            span("install.locked", "install.quiesced"),
            span("install.quiesced", "install.installed"),
            span("install.installed", "install.marked"),
        );
        // FLAGGED instrument change (diagnostic only; the claim and its sum are unchanged): the
        // capture's span split at its read snapshot's two steps, so a red names its syscalls. Read
        // with no foreign check (review delete MED-2): a sub-span's own thread check would refuse
        // rounds the judged spans accept, so the judged rounds stay as they were.
        let quiet = |a: &str, b: &str| match (at(a), at(b)) {
            (Some(i), Some(j)) => points[j].1 - points[i].1,
            _ => u64::MAX,
        };
        let in_capture = (
            quiet("capture.matured", "capture.tx_begun"),
            quiet("capture.tx_begun", "capture.snapshot_pinned"),
            quiet("capture.snapshot_pinned", "capture.captured"),
        );
        let inside = (
            span("install.quiesced", "install.snapshot_ended"),
            span("install.snapshot_ended", "install.before_cut"),
            span("install.before_cut", "install.cut_finished"),
            span("install.cut_finished", "install.installed"),
        );
        if !foreign.is_empty() {
            refused.push(format!("round {round}: {foreign:?}"));
            continue;
        }
        judged += 1;
        let total = capture.0 + capture.1 + install.0 + install.1 + install.2;
        if total > 2 {
            over.push(format!(
                "round {round}: {total} syscalls held: capture [to matured {}, capture {} (to tx begun {}, snapshot pin {}, after {})], install [quiesce {}, install {} (snapshot end {}, to cut {}, cut {}, after {}), mark {}]",
                capture.0, capture.1, in_capture.0, in_capture.1, in_capture.2, install.0, install.1, inside.0, inside.1, inside.2, inside.3, install.2
            ));
        }
    }
    assert!(
        judged == 3,
        "premise: only {judged} of up to 12 rounds could be attributed by thread (refusing; unattributable: {refused:?})"
    );
    assert!(over.is_empty(), "CLAIM: a fuzzy checkpoint issued more than two syscalls under the store mutex: {over:?}");
}

/// Checkpoint FIX (budget a_checkpoint_flushes_at_most_three_times: 20 full flushes over 3
/// checkpoints in ckpt_n10, budget 3 each, review 1 #7): a fuzzy checkpoint of a D2 catalog store,
/// its full flushes attributed to its thread's phases (`store::CKPT_PHASES`: start, settled,
/// written, cut, installed, truncated). Budget: the catalog commit's one, one for the WAL header after
/// the truncation, the cut log's one. Three checkpoints, each after 30 creates; nothing else runs.
#[test]
fn a_fuzzy_checkpoint_issues_at_most_three_full_flushes() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("ckpt-flushes.db"), opts(true, SyncClass::FullFsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let mut over = Vec::new();
    for round in 0..3 {
        for _ in 0..30 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        super::store::CKPT_PHASES.lock().unwrap().clear();
        assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: round {round}: a fuzzy checkpoint started");
        db.branch_checkpoint_wait();
        eventually("premise: the checkpoint thread reached its truncation", || {
            super::store::CKPT_PHASES.lock().unwrap().iter().any(|(p, ..)| *p == "truncated")
        });
        let phases = super::store::CKPT_PHASES.lock().unwrap().clone();
        let (_, _, f0) = phases[0];
        let fulls = phases.last().unwrap().2 - f0;
        let mut last = f0;
        let steps: Vec<String> = phases[1..]
            .iter()
            .map(|(p, _, f)| {
                let d = f - last;
                last = *f;
                format!("{p}+{d}")
            })
            .collect();
        if fulls > 3 {
            over.push(format!("round {round}: {fulls} full flushes [{}]", steps.join(" ")));
        }
    }
    assert!(over.is_empty(), "CLAIM: a fuzzy checkpoint issued more than three full flushes: {over:?}");
}

/// L2 slice (budget create_allocates_at_most_once_under_the_store_mutex: 7 held allocations per
/// create in every cell, budget 1): a steady create's allocations under a store mutex, attributed to
/// its steps by the `held_trace` points (fork_trunk's hold and the flight lead's hold), for both
/// modes and D0/D2. The message lists each step's share, so the fix is aimed.
#[test]
fn a_steady_create_allocates_at_most_once_under_the_store_mutex() {
    let _s = serial();
    let mut over = Vec::new();
    for (catalog, class) in [(false, SyncClass::FullFsync), (true, SyncClass::FullFsync), (false, SyncClass::Off)] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("held.db"), opts(catalog, class));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for _ in 0..20 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        let before = super::budget_probe::thread_allocs().2;
        super::store::HELD_TRACE.with(|t| *t.borrow_mut() = Some(Vec::with_capacity(64)));
        let _ = trunk.fork_branch().unwrap().into_id();
        let trace = super::store::HELD_TRACE.with(|t| t.borrow_mut().take()).unwrap_or_default();
        let total = super::budget_probe::thread_allocs().2 - before;
        let mut last = before;
        let steps: Vec<String> = trace
            .iter()
            .map(|(label, n, _)| {
                let d = n - last;
                last = *n;
                format!("{label}+{d}")
            })
            .collect();
        if total > 1 {
            over.push(format!("catalog={catalog} {class:?}: {total} held allocations [{}], {} after the last point", steps.join(" "), total - (last - before)));
        }
    }
    assert!(over.is_empty(), "CLAIM: a steady create allocated more than once under the store mutex: {over:?}");
}

/// Class (b) step 1 (lead order 2026-10-15): the create-hold stall the hold audit found in its
/// filter_rebuild run (fork_trunk's hold: 6,408 allocations, 11,088,085 B, 1 catalog statement, raw
/// l69-audit). Hypothesis under test, as its CLAIM: that hold is a checkpoint STARTED inside the
/// create's hold (`maybe_compact` -> `start_flight`, its capture), not the filter. The audit's store
/// (10,000 named branches, catalog, D2) takes 16,384 named creates, each traced (`held_trace`);
/// the worst create's spans are printed, and the claim is that its allocations fall in
/// `compact.flight_starting` .. `compact.flight_start_returned`.
#[test]
#[ignore = "class (b) step 1 and L2(d): run by name with --ignored --nocapture"]
fn diag_the_create_hold_stall_is_a_checkpoint_start() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("stall.db");
    let incarnation = {
        let db = open_at(&path, opts(true, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        for i in 0..10_000u32 {
            trunk.create_branch(&format!("pop-{i:07}")).unwrap();
        }
        db.incarnation
    };
    let db = reopen(&path, opts(true, SyncClass::FullFsync), incarnation);
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    let mut worst: (u64, String, Vec<(&'static str, u64, u64)>, u64) = (0, String::new(), Vec::new(), 0);
    let mut over_64 = 0u32;
    let mut started_in_a_create = 0u32;
    for i in 0..16_384u32 {
        let name = format!("r-{i:07}");
        let before = super::budget_probe::thread_allocs().2;
        super::store::HELD_TRACE.with(|t| *t.borrow_mut() = Some(Vec::with_capacity(64)));
        trunk.create_branch(&name).unwrap();
        let trace = super::store::HELD_TRACE.with(|t| t.borrow_mut().take()).unwrap_or_default();
        let total = trace.last().map_or(0, |(_, n, _)| n - before);
        if total > 64 {
            over_64 += 1;
        }
        if trace.iter().any(|(l, _, _)| *l == "compact.flight_starting" || l.starts_with("capture.")) {
            started_in_a_create += 1;
        }
        if total > worst.0 {
            worst = (total, name, trace, before);
        }
    }
    let (total, name, trace, before) = worst;
    let mut last = before;
    let mut in_start = 0;
    let steps: Vec<String> = trace
        .iter()
        .map(|(label, n, _)| {
            let d = n - last;
            last = *n;
            if *label == "compact.flight_start_returned" {
                in_start = d;
            }
            format!("{label}+{d}")
        })
        .collect();
    println!("FE_STALL worst create {name}: {total} allocations to its last trace point [{}]; {over_64} of 16384 creates over 64", steps.join(" "));
    // Confirmed at f48184da2 (raw l82): 6,283 of the worst create's 6,288 held allocations were its
    // checkpoint start. After L2(d) slice 1 (raw l92) the worst create allocates 60 (the capture's
    // read snapshot 26, its rows 14, its hand-off 12) and none of 16,384 passes 64; the claim is now
    // that bound, which the rows and the snapshot (slices 2-3) must bring to 0.
    // After L2(d)'s owed start the claim is structural: no create's hold runs any part of a
    // checkpoint's start (no `compact.flight_starting` or `capture.*` trace point in any of the
    // 16,384 holds). Mutant `flight_start_inline` (the start back in the create's hold) puts them
    // back. The worst create's allocations are printed (17 measured: its own apply, raw l100), and
    // slice 1's bound stays asserted (review L2d M4: 969e7a370 had dropped it): no create over 64.
    let _ = in_start;
    assert_eq!(started_in_a_create, 0, "CLAIM: {started_in_a_create} creates ran a checkpoint's start in their store-mutex hold (worst: [{}])", steps.join(" "));
    assert_eq!(over_64, 0, "CLAIM: {over_64} of 16,384 creates made more than 64 held allocations (worst {total}: [{}])", steps.join(" "));
}

/// Review REVIEW-claude-engine-L2d M4: slice 3's claim (969e7a370) gated, not only in the ignored
/// diag above. 512 named creates on a fuzzy catalog store at an 8 KiB threshold, each create's
/// store-mutex hold traced (`HELD_TRACE`): none runs any part of a checkpoint's start (no
/// `compact.flight_starting` or `capture.*` point). Premise: checkpoints were captured meanwhile
/// (this store's `captures_entered` rose), so the starts happened, elsewhere. Mutant `flight_start_inline`.
#[test]
fn no_create_runs_a_checkpoint_start_in_its_store_mutex_hold() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(
        &dir.path().join("no-start-in-create.db"),
        opts(true, SyncClass::Fsync).with_branch_checkpoint(super::BranchCheckpoint::Fuzzy),
    );
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _t = Threshold::set(8 << 10);
    let captures0 = db.branches.captures_entered_for_test();
    let mut started_in = Vec::new();
    for i in 0..512u32 {
        let name = format!("g-{i:04}");
        super::store::HELD_TRACE.with(|t| *t.borrow_mut() = Some(Vec::with_capacity(64)));
        trunk.create_branch(&name).unwrap();
        let trace = super::store::HELD_TRACE.with(|t| t.borrow_mut().take()).unwrap_or_default();
        if trace.iter().any(|(l, _, _)| *l == "compact.flight_starting" || l.starts_with("capture.")) {
            started_in.push(name);
        }
    }
    db.branch_checkpoint_wait();
    let captures = db.branches.captures_entered_for_test() - captures0;
    assert!(captures >= 1, "premise: no checkpoint was captured during 512 creates past an 8 KiB threshold");
    assert!(
        started_in.is_empty(),
        "CLAIM: {} of 512 creates ran a checkpoint's start in their store-mutex hold ({captures} captures): {:?}",
        started_in.len(),
        &started_in[..started_in.len().min(8)]
    );
}

/// Codex C25 HIGH 1 at the store (lead order 00: its "crash arm"): the background writer held after
/// it installed its snapshot and before it takes its cut (`HOLD_COMPACT_INSTALLED`, store mutex
/// free), creates go on past the hard limit until one compacts the log INLINE (counted,
/// `branch_exception_counters()[2]`). Then creates pad the NEW log to exactly the held cut's offset
/// (`held_cut_from_for_test`), the last one named so its length fits, as Codex's equal-length frames
/// did: the stale offset is then a record boundary of the new log, which is what lets the old cut
/// copy it (without the alignment its re-framing fails and nothing is lost, so the arm would not
/// discriminate). Then one more create, the victim. The writer released, its compaction finishes;
/// the store closed and reopened: EVERY create acknowledged since the hold is a live branch. Before
/// the fix the stale cut installed the older generation over the inline one. Mutant
/// `bg_cut_identity_unchecked`.
#[test]
fn a_create_after_an_inline_compaction_survives_a_stale_background_cut() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("stale-cut.db");
    let (made, incarnation) = {
        let db = open_at(&path, opts(false, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_INSTALLED);
        let _t = Threshold::set(8 << 10);
        let t = std::time::Instant::now();
        while db.branches.confirm_held_for_test() != super::store::HOLD_COMPACT_INSTALLED | super::store::HOLD_ARRIVED {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no background compaction reached its install");
        }
        let from = db.branches.held_cut_from_for_test();
        let mut made = Vec::new();
        let inline = db.branch_exception_counters()[2];
        while db.branch_exception_counters()[2] == inline {
            made.push(trunk.fork_branch().unwrap().into_id());
            assert!(t.elapsed() < std::time::Duration::from_secs(60), "premise: no create compacted inline past the hard limit");
        }
        let inline = db.branch_exception_counters()[2];
        let len = || db.branches.log_len_for_test();
        assert!(len() < from, "premise: the inline log ({}) is already past the held cut's offset {from}", len());
        // One unnamed and one named create measure the two frame lengths: a named create's grows by
        // one byte per byte of name (names stay under 128 bytes, so no length prefix widens).
        let l0 = len();
        made.push(trunk.fork_branch().unwrap().into_id());
        let unnamed = len() - l0;
        let l1 = len();
        made.push(trunk.create_branch("pad-measure").unwrap());
        let named_fixed = (len() - l1).checked_sub(11).expect("premise: a named create's frame holds its name");
        assert!(unnamed < 58, "premise: an unnamed create's frame ({unnamed} bytes) fits the padding's slack");
        let mut pads = 0u32;
        loop {
            let at = len();
            assert!(at <= from, "premise: the padding overshot the held cut's offset {from} (at {at})");
            let rem = from - at;
            if rem == 0 {
                break;
            }
            if rem > named_fixed + 64 {
                made.push(trunk.fork_branch().unwrap().into_id());
            } else {
                let n = rem.checked_sub(named_fixed).filter(|n| *n >= 6).expect("premise: the padding left room for a named create") as usize;
                pads += 1;
                let name = format!("p{pads}-{}", "x".repeat(n));
                made.push(trunk.create_branch(&name[..n]).unwrap());
            }
            assert!(t.elapsed() < std::time::Duration::from_secs(90), "premise: the padding never reached the held cut's offset");
        }
        assert_eq!(db.branch_exception_counters()[2], inline, "premise: the padding compacted the log inline again");
        made.push(trunk.fork_branch().unwrap().into_id());
        db.branches.confirm_hold_for_test(0);
        eventually("premise: the held background compaction never finished", || !db.branches.compaction_owed_for_test());
        (made, db.incarnation)
    };
    let db = reopen(&path, opts(false, SyncClass::Fsync), incarnation);
    let live = db.branch_ids().unwrap();
    let lost: Vec<_> = made.iter().filter(|id| !live.contains(id)).collect();
    assert!(
        lost.is_empty(),
        "CLAIM: {} of {} creates acknowledged after an inline compaction were lost to the stale background cut: {lost:?}",
        lost.len(),
        made.len()
    );
}

/// L2 slope (budget create_allocates_no_table_of_every_branch_under_the_store_mutex): the create
/// that allocates the most under the store mutex as a D0 snapshot store grows past 2^15 branches,
/// and whether it compacted the log inline (a D0 compaction syncs nothing, so `maybe_compact` runs it
/// in the create's own hold, encoding a snapshot of every branch: O(N) bytes). Per create: this
/// thread's bytes allocated under a store mutex (`budget_probe::thread_allocs`, exact per thread)
/// and the log's length before and after (a compaction shortens it). Budget: 64 KiB per create.
#[test]
fn a_d0_create_allocates_at_most_64_kib_under_the_store_mutex_as_branches_grow() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("growth-d0.db"), opts(false, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let (mut worst, mut worst_at, mut worst_compacted, mut compactions) = (0u64, 0u64, false, 0u64);
    for n in 0..34_000u64 {
        let len0 = db.branches.log_len_for_test();
        let held0 = super::budget_probe::thread_allocs().3;
        let _ = trunk.fork_branch().unwrap().into_id();
        let held = super::budget_probe::thread_allocs().3 - held0;
        let compacted = db.branches.log_len_for_test() < len0;
        compactions += u64::from(compacted);
        if held > worst {
            (worst, worst_at, worst_compacted) = (held, n, compacted);
        }
    }
    assert!(compactions > 0, "premise: the log was compacted while the store grew");
    assert!(
        worst <= 64 << 10,
        "CLAIM: create {worst_at} allocated {worst} bytes under the store mutex (it compacted the log inline: \
         {worst_compacted}; {compactions} compacting creates in 34,000); budget 64 KiB"
    );
}

/// Lead order (2026-10-15T08:22:05Z, from the wire's default-stack raws; debug, lldb): the create
/// path's frames were ensure_backing 797 KB, open_resolved 831 KB and start_name_filter at least
/// 422 KB, so a create overflowed a 2 MiB thread (Rust's default spawn stack). On a 2 MiB thread, in
/// its own process (an overflow aborts it): open, the first create (the catalog's creation in
/// catalog mode), a named create, a write on it, a reopen and a connect by name. The frame BUDGET
/// beside it is clippy's `large_stack_frames` (deny, 512,000 bytes, workspace lints), which `cargo
/// clippy -p turso_core` now passes. A child that overflows prints "has overflowed its stack".
#[cfg(unix)]
#[test]
fn a_create_runs_on_a_2_mib_thread() {
    let Some(sentinel) = super::fork_driver::alone("branch::fastest_tests::a_create_runs_on_a_2_mib_thread") else {
        return;
    };
    let ran = std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(|| {
            for catalog in [false, true] {
                let dir = tempfile::TempDir::new().unwrap();
                let path = dir.path().join("stack.db");
                let incarnation = {
                    let db = open_at(&path, opts(catalog, SyncClass::Fsync));
                    let trunk = db.connect().unwrap();
                    seed(&trunk);
                    let _ = trunk.fork_branch().unwrap().into_id();
                    let named = trunk.create_branch("small-stack").unwrap();
                    assert_eq!(db.branch_named("small-stack").unwrap(), Some(named), "catalog={catalog}: premise");
                    write_v(&db.connect_named("small-stack").unwrap(), 3, "deep");
                    db.incarnation
                };
                let db = reopen(&path, opts(catalog, SyncClass::Fsync), incarnation);
                assert_eq!(read_v(&db.connect_named("small-stack").unwrap(), 3), "deep", "catalog={catalog}: premise: the write is back");
            }
        })
        .unwrap()
        .join();
    assert!(ran.is_ok(), "CLAIM: the create path failed on a 2 MiB thread");
    super::fork_driver::finished(&sentinel);
}

/// Lead finding and order 5 (fastest DECISIONS, 2026-10-15T07:2xZ): in-place VACUUM builds its
/// image in a target database (`etilqs_<name>`, on the source's IO), whose own WAL commit syncs a
/// DIFFERENT file. A failure of that sync must not reach the trunk store's at-risk decision: the
/// trunk's durability state is not that file's. Only the target's WAL armed (its first sync fails
/// at issue), with x's Release undrained in the trunk store (the state in which a counted failure
/// can fail-stop it): the VACUUM fails, the trunk store counts no sync failure, and the next fork
/// is acknowledged. Both modes.
#[test]
fn a_failed_vacuum_target_wal_sync_leaves_the_trunk_store_untouched() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("vacuum-target.db");
        let armed = Arc::new(std::sync::atomic::AtomicU8::new(0));
        let io: Arc<dyn IO> = Arc::new(FailWalSyncIo {
            only: Some("/etilqs_vacuum-target.db-wal".to_string()),
            inner: Arc::new(PlatformIO::new().unwrap()),
            armed: armed.clone(),
            held: Arc::new(std::sync::Mutex::new(None)),
        });
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            opts(catalog, SyncClass::Off).with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.fork_branch().unwrap().reap().unwrap();
        assert!(drained(&db) < db.branches.durable_for_test(SyncClass::Off), "{what}: premise: x's Release is undrained");
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let failures = db.branches.sync_failures();
        armed.store(1, O::Release);
        let vacuumed = trunk.execute("VACUUM");
        assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the VACUUM target's WAL sync was reached");
        assert!(vacuumed.is_err(), "{what}: premise: the failed target WAL sync failed the VACUUM");
        assert_eq!(
            db.branches.sync_failures(),
            failures,
            "CLAIM: {what}: the VACUUM target's WAL sync failure reached the trunk store"
        );
        let next = db.connect().unwrap().fork_branch().map(|x| x.into_id());
        assert!(next.is_ok(), "CLAIM: {what}: the trunk store refused a fork after the VACUUM target's sync failed: {:?}", next.err());
    }
}

/// Engine review 21 LOW 20: a raw-frame WAL header sync whose completion failed was reported
/// twice: `watched` noted the completion, and `write_frame_raw_watched` reported the failed wait
/// directly without taking that note, so the next check of the connection's noted syncs (the next
/// trunk commit on it) reported it again. A second report re-reads the group's state and can
/// fail-stop later, never less often. The recipe of
/// `a_failed_raw_frame_header_sync_fail_stops_a_store_with_an_undrained_record`, its sync failing
/// after issue (mode 2): the store counts exactly one trunk sync failure. Both modes. Mutant
/// `raw_header_sync_reported_twice`.
#[cfg(feature = "conn_raw_api")]
#[test]
fn a_failed_raw_frame_header_sync_is_reported_once() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("raw-header-once.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let page_size = trunk.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap() as usize;
        let mut frame = vec![0u8; 24 + page_size];
        trunk.wal_get_frame(1, &mut frame).unwrap();
        trunk.fork_branch().unwrap().reap().unwrap();
        trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        let failures = db.branches.sync_failures();
        trunk.wal_insert_begin().unwrap();
        armed.store(2, O::Release);
        let inserted = trunk.wal_insert_frame(1, &frame);
        assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the WAL header's sync was reached");
        assert!(inserted.is_err(), "{what}: premise: the failed header sync failed the insert");
        let _ = trunk.wal_insert_end(false);
        // The next trunk commit on the same connection checks its noted syncs.
        let _ = trunk.execute("UPDATE t SET v = 'later' WHERE id = 9");
        let reported = db.branches.sync_failures() - failures;
        assert_eq!(reported, 1, "{what}: one failed raw WAL header sync was reported {reported} times");
    }
}

/// Engine review 19 MED 4 (a regression): every fresh D0 store starts with its directory marked
/// unsynced (`reset_log` at its creation, a class that syncs nothing), and its first syncing flight
/// (here a trunk commit under synchronous FULL that keeps a pre-image for a live child) then opened
/// the log's directory as a file to sync it. Off unix that open fails (Windows needs
/// FILE_FLAG_BACKUP_SEMANTICS; recalled, not run) and nothing there syncs a directory anyway
/// (`fsync_dir_of` is a no-op), so the flight fail-stopped the store: the commit failed, and so did
/// every later fork. Off unix only (the Windows CI arm). Mutant `dir_opened_off_unix`.
#[cfg(not(unix))]
#[test]
fn a_fresh_d0_stores_first_syncing_flight_off_unix_does_not_fail_stop_it() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("dir-off-unix.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        let x = trunk.fork_branch().unwrap();
        let committed = trunk.execute("UPDATE t SET v = 'new' WHERE id = 3");
        assert!(committed.is_ok(), "{what}: the first trunk commit that keeps a pre-image failed: {committed:?}");
        assert!(
            db.branches.rewrite_class_for_test().syncs(),
            "{what}: premise: that commit's flight synced (it raised the log)"
        );
        let forked = trunk.fork_branch().map(|y| y.into_id());
        assert!(forked.is_ok(), "{what}: a fork after the first syncing flight failed: {forked:?}");
        assert_eq!(read_v(&x.connect().unwrap(), 3), "trunk-3", "{what}: x reads its fork point");
    }
}

/// Engine review 19 MED 7: a D0 compaction writes its snapshot and syncs nothing, and nothing synced
/// it later. A reopen in a class that syncs, or a raised flight in the same process, then made
/// records durable on top of it (the log's reset, a create's flight), so a power cut after their
/// acknowledgement could leave the snapshot every branch lives in torn, and the next open refuses
/// the whole store as Corrupt. The snapshot is synced before anything syncs on top of it: (a) at an
/// open in FullFsync over a D0 compaction's snapshot, (b) by the first syncing flight after a D0
/// compaction in the same process. Counted on the syncing thread (`journal::SNAP_SYNCS`). Snapshot
/// mode: a catalog store has no snapshot. Mutants `snapshot_left_unsynced_at_open` (a) and
/// `snapshot_left_unsynced_by_flight` (b).
#[test]
fn a_d0_compactions_snapshot_is_synced_before_anything_syncs_on_top_of_it() {
    let _s = serial();
    let snaps = || super::journal::SNAP_SYNCS.with(|c| c.get());
    let dir = tempfile::TempDir::new().unwrap();
    // (a) a reopen in FullFsync.
    let path = dir.path().join("snap-open.db");
    let incarnation = {
        let db = open_at(&path, opts(false, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        let x = trunk.fork_branch().unwrap();
        write_v(&x.connect().unwrap(), 40, "x");
        let _x = x.into_id();
        db.branch_compact_now().unwrap();
        assert!(
            std::path::PathBuf::from(format!("{}-branch-snap", path.display())).exists(),
            "(a) premise: the D0 compaction wrote a snapshot"
        );
        db.incarnation
    };
    let before = snaps();
    let db = reopen(&path, opts(false, SyncClass::FullFsync), incarnation);
    assert_eq!(snaps() - before, 1, "(a) a FullFsync open over a D0 compaction's snapshot synced it {} times, not once", snaps() - before);
    drop(db);
    // (b) the first syncing flight after a D0 compaction.
    let path = dir.path().join("snap-flight.db");
    let db = open_at(&path, opts(false, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let x = trunk.fork_branch().unwrap();
    db.branch_compact_now().unwrap();
    assert!(
        std::path::PathBuf::from(format!("{}-branch-snap", path.display())).exists(),
        "(b) premise: the D0 compaction wrote a snapshot"
    );
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    let before = snaps();
    // Keeps x a pre-image in a raised flight, which syncs.
    write_v(&trunk, 3, "new");
    assert!(db.branches.rewrite_class_for_test().syncs(), "(b) premise: the trunk commit's flight synced (it raised the log)");
    assert_eq!(snaps() - before, 1, "(b) the first syncing flight after a D0 compaction synced its snapshot {} times, not once", snaps() - before);
    drop(x);
}

/// Engine review 19 MED 3: a COMMIT that yielded inside the main pager's commit before its
/// copy-decision pass (a spill's wait on an async IO: io_uring, MemoryYieldIO, the simulator; here
/// the store's test hook yields there once) re-entered as Committing, and the re-entered decision
/// pass's Busy then skipped the restore a first attempt's Busy makes
/// (`undo_commit_transition_after_busy`): the connection read autocommit while its write
/// transaction stayed open, review 16 HIGH 1's gap state. The Busy leaves the connection out of
/// autocommit, and the re-step commits. Mutant `busy_after_yield_keeps_committing`.
#[test]
fn a_busy_commit_after_a_yield_leaves_the_connection_out_of_autocommit() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("busy-yield.db"), opts(catalog, SyncClass::Fsync));
        let a = db.connect().unwrap();
        seed_wide(&a);
        let _child = a.fork_branch().unwrap();
        db.branch_failpoint(Some(BranchFailpoint::TrunkDecisionBusy));
        a.execute("BEGIN").unwrap();
        a.execute("UPDATE t SET v = 'new' WHERE id = 3").unwrap();
        db.branches.arm_commit_yield_for_test();
        let mut commit = a.prepare("COMMIT").unwrap();
        let first = commit.run_ignore_rows();
        assert!(!db.branches.commit_yield_armed_for_test(), "{what}: premise: the commit yielded before its decision pass");
        assert!(matches!(first, Err(LimboError::Busy)), "{what}: premise: the decision pass was refused: {first:?}");
        assert!(
            !a.get_auto_commit(),
            "{what}: autocommit reads true while the write transaction is open, after a Busy that followed a yield"
        );
        commit.run_ignore_rows().unwrap();
        assert_eq!(read_wide(&db.connect().unwrap(), 3), "new", "{what}: the re-step committed the transaction");
    }
}

// ---- engine review 19 LOW 9: the WAL-internal sync coverage the 49ecca580 set lacks ----

/// The two (catalog, site) pairs of the `api` way the Apple set had no arm for.
#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_api_checkpoint_wal_sync_fail_stops_a_catalog_store() {
    a_wal_internal_sync_failure_fail_stops(true, "checkpoint", "api");
}

#[cfg(target_vendor = "apple")]
#[test]
fn a_failed_api_truncate_wal_sync_fail_stops_a_snapshot_store() {
    a_wal_internal_sync_failure_fail_stops(false, "truncate", "api");
}

/// Every target (the Apple set's D1 fork is drained by its own fsync off Apple, so those arms have no
/// undrained record there): the checkpoint's and the truncate's WAL sync failing AT ISSUE (mode 1)
/// over a D0 store, whose fork is written and never drained, through the PRAGMA and the API, both
/// modes. The site mutants `checkpoint_sync_unwatched` and `truncate_sync_unwatched` get a killer
/// on Linux.
#[test]
fn a_failed_checkpoint_wal_sync_at_issue_fail_stops_a_d0_store() {
    for catalog in [false, true] {
        for way in ["pragma", "api"] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "checkpoint", way, 1);
        }
    }
}

#[test]
fn a_failed_truncate_wal_sync_at_issue_fail_stops_a_d0_store() {
    for catalog in [false, true] {
        for way in ["pragma", "api"] {
            a_yielded_wal_internal_sync_failure_fail_stops(catalog, "truncate", way, 1);
        }
    }
}

/// The auto-checkpoint arm: a trunk commit at synchronous NORMAL (no WAL sync of its own) over a D0
/// store whose Release is written and not drained, the WAL's auto-checkpoint threshold at 1 page, so
/// the commit runs the checkpoint, whose sync of the WAL before its backfill (the first WAL sync
/// issued) fails at issue. The commit returns Ok (an auto-checkpoint's failure is logged, not
/// returned), so only the watched sync can act on it: the next fork is refused. Every target, both
/// modes. Mutant `checkpoint_sync_unwatched`.
#[test]
fn a_failed_autocheckpoint_wal_sync_fail_stops_a_d0_store() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    // FLAGGED TEST EDIT (fixture reaching its path; for the lead's ruling): `PRAGMA
    // wal_autocheckpoint = 1` below is accepted and does nothing here (the WAL's threshold is a
    // fixed 1000 frames), so no auto-checkpoint ran (red at bb6af4d94 and 9d27592bf on the premise
    // "the auto-checkpoint's WAL sync was reached", artie raws l1-branch*). The threshold is set
    // through the test seam `AUTO_CHECKPOINT_FRAMES_FOR_TEST`, restored by a guard; the PRAGMA stays.
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::storage::wal::AUTO_CHECKPOINT_FRAMES_FOR_TEST.store(0, O::Release);
        }
    }
    let _restore = Restore;
    crate::storage::wal::AUTO_CHECKPOINT_FRAMES_FOR_TEST.store(1, O::Release);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, armed) = open_failing_wal(&dir.path().join("walauto.db"), opts(catalog, SyncClass::Off));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        trunk.execute("PRAGMA wal_autocheckpoint = 1").unwrap();
        trunk.fork_branch().unwrap().reap().unwrap();
        assert!(drained(&db) < db.branches.durable_for_test(SyncClass::Off), "{what}: premise: x's Release is undrained");
        crate::storage::wal::WAL_SYNC_SITE.with(|s| s.set(""));
        armed.store(1, O::Release);
        let committed = trunk.execute("UPDATE t SET v = 'auto' WHERE id = 9");
        assert!(committed.is_ok(), "{what}: premise: the commit returned Ok: {committed:?}");
        assert_eq!(armed.load(O::Acquire), 0, "{what}: premise: the auto-checkpoint's WAL sync was reached");
        assert_eq!(
            crate::storage::wal::WAL_SYNC_SITE.with(|s| s.get()),
            "checkpoint",
            "{what}: premise: the failure reached the checkpoint's site"
        );
        assert_fail_stopped(
            db.connect().unwrap().fork_branch().map(|x| x.into_id()),
            &format!("{what}: the next fork after a failed auto-checkpoint drain with an undrained Release"),
        );
    }
}

/// Engine review 19 LOW 11 (a cost regime, pinned): past UPGRADE_HARD_CAP (4) times the hold
/// bound in held frees, the background writer has fallen behind, and the freeing operations, a
/// delete or a branch commit, lead the held-free upgrade on their own thread after their own wait
/// and before they return (`upgrade_if_overdue`), so 5dc0a1a3c's "0 syncs on create, connect, first
/// write and delete" does not hold for a delete there. The bound forced to 1, the writer held where
/// it found the upgrade due, four branches of two slots each reaped and one more fork and delete,
/// all on this thread: at least one upgrade is led, all of them here (the writer is held), and this
/// thread synced. Both modes.
///
/// FLAGGED TEST EDIT (fixture; for the lead's ruling): its premise "a release matures nothing, so
/// only the last delete leads" was false, and the test was red at bb6af4d94 already (artie raw
/// l1-branch-base_bb6af4d94.txt: "more than 4 slots are held (8)", slots had come free): a reap
/// past the cap already led the upgrade. The counts are taken across the whole freeing sequence
/// instead of around the last delete; the claim (operations past the cap lead it on their own
/// thread) is unchanged.
#[test]
fn a_delete_past_the_hard_cap_leads_one_held_free_upgrade() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, mut held) = raised_d0_with_a_kept_pre_image(&dir.path().join("hard-cap.db"), catalog, true);
        let others: Vec<Branch> = (0..3i64)
            .map(|i| {
                let b = trunk.fork_branch().unwrap();
                write_v(&b.connect().unwrap(), 20 + i, "b");
                write_v(&b.connect().unwrap(), 45 + i, "b");
                b
            })
            .collect();
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        let (upgrades, syncs) = (db.branches.group_counters()[4], super::store::thread_syncs());
        x.reap().unwrap();
        for b in others {
            held.extend(b.owned_slots());
            b.reap().unwrap();
        }
        let d = trunk.fork_branch().unwrap();
        d.reap().unwrap();
        let led = db.branches.group_counters()[4] - upgrades;
        let synced = super::store::thread_syncs() - syncs;
        db.branches.confirm_hold_for_test(0);
        assert!(held.len() > 4, "{what}: premise: more than 4 slots were freed ({}), past the hard cap", held.len());
        assert!(led >= 1, "{what}: no freeing operation past the hard cap led a held-free upgrade");
        assert!(synced > 0, "{what}: the upgrades the freeing operations led did not sync on their thread");
        eventually(&format!("{what}: the held slots never came free"), || {
            held.iter().all(|&s| db.branch_slot_is_free(s))
        });
    }
}

/// The groups and totals view `view` (`grp`, `total`) holds, as `conn` reads them.
fn view_totals(conn: &Arc<Connection>, view: &str) -> Vec<(Option<String>, Option<i64>)> {
    conn.prepare(format!("SELECT grp, CAST(total AS INTEGER) FROM {view} ORDER BY grp"))
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .into_iter()
        .map(|row| (row[0].to_text().map(str::to_string), row[1].as_int()))
        .collect()
}

/// Engine review 19 LOW 15: a COMMIT merges each materialized view's transaction delta in turn, and
/// a merge that fails part-way (here the second view's returns Busy, `VIEW_MERGE_BUSY`) left the
/// first view merged into the transaction's pages while its delta stayed in the connection's view
/// states and the commit's progress stayed at the second view. A COMMIT run again as a new
/// statement merged both views from the start (the first one twice); a held COMMIT re-stepped
/// resumed at the second view, so a write made in between never reached the first. Two views over
/// one table, both arms, both modes: each view counts every committed row once. Mutant
/// `view_merge_restarts_merged`.
#[test]
fn a_view_merge_refused_part_way_merges_each_view_once() {
    let _s = serial();
    for catalog in [false, true] {
        for held in [false, true] {
            let what = format!("catalog={catalog} held={held}");
            let dir = tempfile::TempDir::new().unwrap();
            let db = open_at(&dir.path().join("view-busy.db"), opts(catalog, SyncClass::Fsync).with_views(true));
            let a = db.connect().unwrap();
            a.execute("CREATE TABLE counters(id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)").unwrap();
            a.execute("CREATE MATERIALIZED VIEW sums AS SELECT grp, SUM(val) AS total FROM counters GROUP BY grp")
                .unwrap();
            a.execute("CREATE MATERIALIZED VIEW counts AS SELECT grp, COUNT(*) AS total FROM counters GROUP BY grp")
                .unwrap();
            a.execute("BEGIN").unwrap();
            a.execute("INSERT INTO counters VALUES (1, 'a', 5), (2, 'a', 7)").unwrap();
            crate::vdbe::VIEW_MERGE_BUSY.with(|n| n.set(2));
            let mut commit = a.prepare("COMMIT").unwrap();
            let first = commit.run_ignore_rows();
            let refused = crate::vdbe::VIEW_MERGE_BUSY.with(|n| n.replace(0)) == 0;
            assert!(refused, "{what}: premise: the second view's merge was refused");
            assert!(matches!(first, Err(LimboError::Busy)), "{what}: premise: the COMMIT was refused Busy: {first:?}");
            assert!(!a.get_auto_commit(), "{what}: premise: the transaction is still open");
            let second = if held {
                a.execute("INSERT INTO counters VALUES (3, 'a', 1)").unwrap();
                commit.run_ignore_rows()
            } else {
                drop(commit);
                a.execute("COMMIT")
            };
            assert!(second.is_ok(), "{what}: the second COMMIT failed: {second:?}");
            let b = db.connect().unwrap();
            let (sum, count) = if held { (13, 3) } else { (12, 2) };
            assert_eq!(view_totals(&b, "sums"), vec![(Some("a".to_string()), Some(sum))], "{what}: sums counts each row once");
            assert_eq!(view_totals(&b, "counts"), vec![(Some("a".to_string()), Some(count))], "{what}: counts counts each row once");
        }
    }
}

/// DECISIONS c9c821dd8d (lead, 2026-10-15T04:28:01Z; the cost 82fa78c66 stated): a raised D0 store's
/// rewrites sync in its raised class, and in a snapshot store the compaction ran inline, under the
/// store mutex, on the operation that crossed the threshold: a create whose own work syncs nothing
/// paid the snapshot's, the arena's and the directory's syncs before it returned. It must leave the
/// acknowledgement path: led by the background writer, the counted hard cap aside. With the
/// threshold lowered, creates on this thread run until the log has been compacted (it shrank), and
/// none of them synced on this thread. The catalog store is the control: its fuzzy checkpoint
/// syncs on a thread of its own already. Mutant `compaction_on_ack_path`.
#[test]
fn a_raised_d0_compaction_syncs_off_the_acknowledgement_path() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("raised-compact.db"), catalog, true);
        // x is kept: nothing is freed, so no held-free upgrade runs.
        let _x = x.into_id();
        let _t = Threshold::set(8 << 10);
        let mut longest = db.branches.log_len_for_test();
        let mut synced = Vec::new();
        let t = std::time::Instant::now();
        loop {
            let syncs = super::store::thread_syncs();
            let _ = trunk.fork_branch().unwrap().into_id();
            let n = super::store::thread_syncs() - syncs;
            if n > 0 {
                synced.push(n);
            }
            let len = db.branches.log_len_for_test();
            if len < longest {
                break;
            }
            longest = len;
            assert!(
                t.elapsed() < std::time::Duration::from_secs(30),
                "{what}: premise: the log was never compacted ({len} bytes)"
            );
        }
        assert!(db.branches.rewrite_class_for_test().syncs(), "{what}: premise: the compaction synced (a raised log)");
        assert!(
            synced.is_empty(),
            "{what}: creates synced on their own thread ({synced:?} syncs): one ran the raised log's compaction inline"
        );
    }
}

/// DECISIONS c9c821dd8d, the counted exception: with the background writer held where it found the
/// owed compaction, the log grows past the hard limit, and the operation that finds it there
/// compacts inline (the log's growth stays bounded when the writer falls behind) and is counted
/// (`branch_exception_counters()[2]`). The creates before it, the one that owed the compaction
/// among them, synced nothing on their own thread and moved no counter (the control). Raised D0
/// snapshot store.
#[test]
fn a_compaction_past_the_hard_limit_runs_inline_and_is_counted() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("compact-hard.db"), false, true);
    let _x = x.into_id();
    db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_DUE);
    let _t = Threshold::set(8 << 10);
    let base = db.branch_exception_counters();
    let (mut owed, mut synced_before) = (false, Vec::new());
    let t = std::time::Instant::now();
    let synced = loop {
        let syncs = super::store::thread_syncs();
        let _ = trunk.fork_branch().unwrap().into_id();
        let n = super::store::thread_syncs() - syncs;
        if db.branch_exception_counters()[2] != base[2] {
            break n;
        }
        if n > 0 {
            synced_before.push(n);
        }
        owed |= db.branches.compaction_owed_for_test();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no create reached the hard limit");
    };
    let after = db.branch_exception_counters();
    let held = db.branches.confirm_held_for_test();
    db.branches.confirm_hold_for_test(0);
    assert!(owed, "premise: no create owed the compaction to the background writer below the hard limit");
    assert_eq!(held, super::store::HOLD_COMPACT_DUE | super::store::HOLD_ARRIVED, "premise: the writer was held at the owed compaction");
    assert!(synced_before.is_empty(), "creates below the hard limit synced on their own thread: {synced_before:?}");
    assert_eq!(after[2] - base[2], 1, "the create past the hard limit was not counted exactly once");
    assert!(synced > 0, "the create past the hard limit compacted the raised log without syncing on its thread");
    for i in [0, 1, 3] {
        assert_eq!(after[i], base[i], "exception counter {i} moved: {base:?} -> {after:?}");
    }
}

/// DECISIONS c9c821dd8d: an operation arriving while the background compaction holds the store
/// mutex for a step that may sync waits behind it, and is counted (`branch_exception_counters()[3]`).
/// The writer held inside that hold, a create from another thread is counted as it arrives, waits,
/// and completes once the hold ends. The creates before the hold, the one that owed the compaction
/// among them, moved no counter (the control).
///
/// FLAGGED TEST EDIT (DECISIONS bd0d845f97, a premise the fix changes; for the lead's ruling): since
/// the compaction syncs outside the mutex, the only such hold is its cut's last pass, which copies
/// what flights left after the catch-ups. The plant takes it every time (`COMPACT_CATCH_UPS_OFF`,
/// restored by a guard) and holds the writer inside it (`HOLD_COMPACT_TAIL`), where it held it at
/// `HOLD_COMPACT_LOCKED` before. The assertions are unchanged.
#[test]
fn a_create_behind_a_background_compaction_is_counted() {
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::COMPACT_CATCH_UPS_OFF.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _restore = Restore;
    super::store::COMPACT_CATCH_UPS_OFF.store(true, std::sync::atomic::Ordering::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("compact-wait.db"), false, true);
    let _x = x.into_id();
    db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_TAIL);
    let _t = Threshold::set(8 << 10);
    let base = db.branch_exception_counters();
    let t = std::time::Instant::now();
    while !db.branches.compaction_owed_for_test() {
        let _ = trunk.fork_branch().unwrap().into_id();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no create owed a compaction");
    }
    eventually("premise: the background compaction never reached its hold", || {
        db.branches.confirm_held_for_test() == super::store::HOLD_COMPACT_TAIL | super::store::HOLD_ARRIVED
    });
    let before = db.branch_exception_counters();
    let other = {
        let db = db.clone();
        std::thread::spawn(move || db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id()))
    };
    let t = std::time::Instant::now();
    while db.branch_exception_counters()[3] == before[3] && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let arrived = db.branch_exception_counters();
    let finished_while_held = other.is_finished();
    db.branches.confirm_hold_for_test(0);
    let joined = other.join().unwrap();
    assert_eq!(before, base, "the creates before the background compaction moved an exception counter");
    assert!(joined.is_ok(), "the create behind the compaction failed: {joined:?}");
    assert!(!finished_while_held, "the create finished while the compaction held the store mutex");
    assert!(arrived[3] > before[3], "a create that waited behind the background compaction was not counted");
    eventually("the background compaction never ran", || db.branches.background_compactions() >= 1);
}

/// Review c42b MED-1: a bounded CHECKPOINT that waited behind the background compaction's sync
/// under the mutex is counted as an operation that waited (`branch_exception_counters()[3]`), as
/// `lock()` counts one: "an overcount, never a miss". `lock_within` read `compacting` only at the
/// successful `try_lock`, after the compaction had cleared it. The fixture of
/// `a_create_behind_a_background_compaction_is_counted`, with `branch_compact_now_within` as the
/// waiter.
#[test]
fn a_bounded_checkpoint_behind_a_background_compaction_is_counted() {
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::COMPACT_CATCH_UPS_OFF.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let _restore = Restore;
    super::store::COMPACT_CATCH_UPS_OFF.store(true, std::sync::atomic::Ordering::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("bounded-compact-wait.db"), false, true);
    let _x = x.into_id();
    db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_TAIL);
    let _t = Threshold::set(8 << 10);
    let t = std::time::Instant::now();
    while !db.branches.compaction_owed_for_test() {
        let _ = trunk.fork_branch().unwrap().into_id();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no create owed a compaction");
    }
    eventually("premise: the background compaction never reached its hold", || {
        db.branches.confirm_held_for_test() == super::store::HOLD_COMPACT_TAIL | super::store::HOLD_ARRIVED
    });
    let before = db.branch_exception_counters();
    let waiter = {
        let db = db.clone();
        std::thread::spawn(move || db.branch_compact_now_within(std::time::Instant::now() + std::time::Duration::from_secs(10), &|| false))
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    let waited_while_held = !waiter.is_finished();
    db.branches.confirm_hold_for_test(0);
    let joined = waiter.join().unwrap();
    let after = db.branch_exception_counters();
    assert!(waited_while_held, "premise: the bounded CHECKPOINT did not wait behind the compaction's hold");
    assert!(joined.is_ok(), "premise: the bounded CHECKPOINT failed: {joined:?}");
    assert!(after[3] > before[3], "CLAIM: a bounded CHECKPOINT that waited behind the background compaction was not counted");
}

/// C15 ruling (fastest DECISIONS 2026-10-15T04:15:07Z), exception (a), the hard-cap fallback: with
/// the bound forced to 1 and the background writer held where it found the upgrade due, every
/// held-free upgrade is led by a freeing operation past the hard cap, and each is counted
/// (`branch_exception_counters()[0]` moves exactly as the upgrade count does, at least once) across
/// `a_delete_past_the_hard_cap_leads_one_held_free_upgrade`'s schedule. The control: below the cap
/// (x's two slots, and a fork), no upgrade is led and nothing is counted. Both modes.
///
/// FLAGGED TEST EDIT (fixture; for the lead's ruling; this session's own test, red at 9d27592bf,
/// artie raw l1-branch_9d27592bf.txt, left [1, 0, 0, 0]): it took its control as "the operations
/// before the last delete", which already lead past the cap. The control is now below the cap.
#[test]
fn a_hard_cap_upgrade_is_counted() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, mut held) = raised_d0_with_a_kept_pre_image(&dir.path().join("hard-cap-count.db"), catalog, true);
        let others: Vec<Branch> = (0..3i64)
            .map(|i| {
                let b = trunk.fork_branch().unwrap();
                write_v(&b.connect().unwrap(), 20 + i, "b");
                write_v(&b.connect().unwrap(), 45 + i, "b");
                b
            })
            .collect();
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        let (base, upgrades) = (db.branch_exception_counters(), db.branches.group_counters()[4]);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        let (below, below_upgrades) = (db.branch_exception_counters(), db.branches.group_counters()[4]);
        for b in others {
            held.extend(b.owned_slots());
            b.reap().unwrap();
        }
        let d = trunk.fork_branch().unwrap();
        d.reap().unwrap();
        let (after, after_upgrades) = (db.branch_exception_counters(), db.branches.group_counters()[4]);
        db.branches.confirm_hold_for_test(0);
        assert!(held.len() > 4, "{what}: premise: more than 4 slots were freed ({}), past the hard cap", held.len());
        assert_eq!((below, below_upgrades), (base, upgrades), "{what}: below the hard cap an upgrade was led or counted");
        let led = after_upgrades - below_upgrades;
        assert!(led >= 1, "{what}: premise: no freeing operation past the hard cap led a held-free upgrade");
        assert_eq!(after[0] - below[0], led, "{what}: CLAIM: hard-cap upgrades counted {:?} -> {after:?}, led {led}", below);
        for i in [1, 2, 3] {
            assert_eq!(after[i], below[i], "{what}: exception counter {i} moved: {below:?} -> {after:?}");
        }
    }
}

/// C15 ruling, exception (b): a held-free upgrade that takes the header's raised-class patch holds
/// the flight slot (`Upgrade::exclusive`; flights wait for it), and is counted
/// (`branch_exception_counters()[1]`). Its natural schedule (a log whose header field was written
/// before 82fa78c66, opened under a weaker class) is not built here; the plant reaches the same
/// branch through the real store path by marking the header stale just before the upgrade's take
/// (`stale_log_header_for_test`), with the writer held where it found the upgrade due. The same
/// schedule without the stale header is the control: an ordinary upgrade, not counted. Both modes.
#[test]
fn a_header_raising_upgrade_is_counted() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        for stale in [false, true] {
            let what = format!("catalog={catalog} stale={stale}");
            let dir = tempfile::TempDir::new().unwrap();
            let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("header-raise.db"), catalog, true);
            assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
            db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
            x.reap().unwrap();
            let _armed = trunk.fork_branch().unwrap().into_id();
            eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
                db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
            });
            if stale {
                db.branches.stale_log_header_for_test();
            }
            let before = db.branch_exception_counters();
            db.branches.confirm_hold_for_test(0);
            eventually(&format!("{what}: the held slots never came free"), || {
                slots.iter().all(|&s| db.branch_slot_is_free(s))
            });
            let after = db.branch_exception_counters();
            assert_eq!(
                after[1] - before[1],
                u64::from(stale),
                "{what}: CLAIM: header-raising upgrades counted {before:?} -> {after:?}"
            );
            for i in [0, 2, 3] {
                assert_eq!(after[i], before[i], "{what}: exception counter {i} moved: {before:?} -> {after:?}");
            }
        }
    }
}

/// Codex C15 MED 2: `branch_checkpoint_now`'s deadline bounds only its retries: when a fuzzy
/// flight is already in the air, its failed start joined that flight before reading the clock, so
/// with the flight held before its catalog commit a 1 ms request waited for as long as the hold.
/// With an existing flight held, a request with a 1 ms deadline returns `Ok(false)` (the caller's
/// Busy) within 2 s; released, the held flight installs.
#[test]
fn a_requested_checkpoint_does_not_wait_past_its_deadline_for_a_flight_in_the_air() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("checkpoint-now-deadline.db"), opts(true, SyncClass::Fsync));
    let trunk = db.connect().unwrap();
    seed(&trunk);
    let _first = trunk.fork_branch().unwrap().into_id();
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: the existing flight started");
    eventually("premise: the existing flight never reached its hold", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_COMMIT | super::store::HOLD_ARRIVED
    });
    let request = {
        let db = db.clone();
        std::thread::spawn(move || db.branch_checkpoint_now(std::time::Duration::from_millis(1)))
    };
    let t = std::time::Instant::now();
    while !request.is_finished() && t.elapsed() < std::time::Duration::from_secs(2) {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let returned = request.is_finished();
    db.branch_checkpoint_hold(0);
    let answer = request.join().unwrap();
    db.branch_checkpoint_wait();
    assert!(returned, "CLAIM: a 1 ms checkpoint request waited past 2 s for the flight in the air");
    assert!(matches!(answer, Ok(false)), "the request past its deadline answered {answer:?}, not Ok(false)");
}

/// Engine review 18 MED 7: after a held-free upgrade lands, nothing re-armed one while more than
/// the bound was still held: `mature` arms only when none is in the air, and the writer leads once.
/// Frees made while an upgrade is in the air (y's, after its take) stayed held until some later
/// operation's `mature`. Held once taken (`HOLD_FLIGHT_TAKEN`), y is reaped and a fork matures
/// past the bound; released, the upgrade lands (x's slots come free) and, with no other operation,
/// y's slots come free too. Observed passively (`slot_is_free_passive_for_test`: no `mature`, which
/// would arm it; review 18 LOW 8). Both modes. Mutant `writer_upgrades_once`.
#[test]
fn a_held_free_upgrade_is_rearmed_when_frees_stay_past_the_bound() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, xs) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-rearm.db"), catalog, true);
        let y = trunk.fork_branch().unwrap();
        write_v(&y.connect().unwrap(), 20, "y");
        write_v(&y.connect().unwrap(), 45, "y");
        let ys = y.owned_slots();
        assert!(ys.len() >= 2, "{what}: premise: y's writes took two slots, past the bound");
        let free = |s: &u32| db.branches.slot_is_free_passive_for_test(*s);
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
        });
        db.branches.confirm_hold_for_test(super::store::HOLD_FLIGHT_TAKEN);
        eventually(&format!("{what}: premise: the background writer never took the upgrade"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_FLIGHT_TAKEN | super::store::HOLD_ARRIVED
        });
        y.reap().unwrap();
        let _past = trunk.fork_branch().unwrap().into_id();
        assert!(!ys.iter().any(free), "{what}: premise: y's slots are held while the upgrade is in the air");
        db.branches.confirm_hold_for_test(0);
        eventually(&format!("{what}: premise: the first upgrade never freed x's slots"), || xs.iter().all(free));
        eventually(
            &format!("{what}: CLAIM: y's frees, past the bound when the upgrade landed, never came free: nothing re-armed it"),
            || ys.iter().all(free),
        );
    }
}

/// Engine review 18 MED 7: every `mature` past the bound took the group's lock and notified the
/// background writer, even with the upgrade already owed or in the air: inside the store mutex, on
/// every create, first write and trunk commit, where one notification per crossing is enough. The
/// writer held where it found the upgrade due (so it stays owed), five forks past the bound notify
/// it not at all. Both modes.
#[test]
fn creates_past_the_bound_do_not_rewake_the_writer_for_an_owed_upgrade() {
    let _s = serial();
    let _b = HoldBound::set(1);
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let (db, trunk, x, slots) = raised_d0_with_a_kept_pre_image(&dir.path().join("upgrade-rewake.db"), catalog, true);
        assert_eq!(slots.len(), 2, "{what}: premise: x's release frees two slots, past the bound");
        db.branches.confirm_hold_for_test(super::store::HOLD_UPGRADE_DUE);
        x.reap().unwrap();
        let _armed = trunk.fork_branch().unwrap().into_id();
        eventually(&format!("{what}: premise: the background writer never found the upgrade due"), || {
            db.branches.confirm_held_for_test() == super::store::HOLD_UPGRADE_DUE | super::store::HOLD_ARRIVED
        });
        assert!(
            slots.iter().all(|s| !db.branches.slot_is_free_passive_for_test(*s)),
            "{what}: premise: x's slots are held, past the bound, with the upgrade owed"
        );
        let n0 = db.branches.confirm_notifies_for_test();
        for _ in 0..5 {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        let n = db.branches.confirm_notifies_for_test() - n0;
        db.branches.confirm_hold_for_test(0);
        assert_eq!(n, 0, "{what}: CLAIM: five creates past the bound, with the upgrade owed, notified the writer {n} times");
    }
}

/// Engine review 18 MED 3: a raised D0 catalog store's fuzzy checkpoint waits in `settle_ordered`
/// for a FULL trunk commit T's WAL flush, while T, past its barrier and before that flush, waited
/// in back-pressure for this checkpoint's install (`Backpressure`, the log past the hard limit):
/// a cycle only the bound broke (`pending_full_wait`, 100 ms; set to 5 s here so it shows). The
/// checkpoint then failed Busy, its install was lost for a threshold of log, and T stalled under
/// the WAL write lock. Here the checkpoint is held before its settle with the log past the hard
/// limit (a filler thread's creates; the one that crosses waits in back-pressure), T is held once
/// its ordered barrier is done (its WAL flush to come), then both are released: T returns within
/// 2 s, and the checkpoint installs.
#[cfg(target_vendor = "apple")]
#[test]
fn a_trunk_commit_does_not_wait_on_a_checkpoint_that_waits_on_its_flush() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::PENDING_FULL_WAIT_MS.store(0, O::Release);
        }
    }
    let _restore = Restore;
    super::store::PENDING_FULL_WAIT_MS.store(5_000, O::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("settle-cycle.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    let _x = trunk.fork_branch().unwrap().into_id();
    write_v(&trunk, 3, "raised");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    // A D0 record past the last full flush, so the checkpoint's capture covers one an ordered
    // flight's flush must make durable (`settle_ordered` waits only then).
    let _y = trunk.fork_branch().unwrap().into_id();
    assert!(
        db.branches.durable_for_test(SyncClass::FullFsync) < db.branches.durable_for_test(SyncClass::Off),
        "premise: a D0 record is written past the last full flush"
    );
    let _t = Threshold::set(8 << 10);
    db.branch_checkpoint_hold(super::store::HOLD_BEFORE_SETTLE);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    eventually("premise: the checkpoint never reached its settle", || {
        db.branch_checkpoint_held() == super::store::HOLD_BEFORE_SETTLE | super::store::HOLD_ARRIVED
    });
    let filler = {
        let db = db.clone();
        std::thread::spawn(move || {
            let t = std::time::Instant::now();
            while !db.branches.over_hard_for_test() && t.elapsed() < std::time::Duration::from_secs(30) {
                let _ = db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id());
            }
        })
    };
    eventually("premise: the log never passed the hard limit with the checkpoint in flight", || {
        db.branches.over_hard_for_test()
    });
    let installed = db.branch_checkpoint_counters()[0];
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_TRUNK_BARRIER_DONE, O::Release);
    let committing = std::thread::spawn(move || trunk.execute("UPDATE t SET v = 'ordered' WHERE id = 40").map(|_| ()));
    wait_hold(&hold, super::store::HOLD_TRUNK_BARRIER_DONE);
    db.branch_checkpoint_hold(0);
    let t = std::time::Instant::now();
    hold.store(0, O::Release);
    let got = committing.join().unwrap();
    let took = t.elapsed();
    db.branch_checkpoint_wait();
    filler.join().unwrap();
    assert!(got.is_ok(), "premise: the trunk commit committed: {got:?}");
    assert!(
        took < std::time::Duration::from_secs(2),
        "CLAIM: the trunk commit waited {took:?} past its barrier on the checkpoint that waited on its WAL flush"
    );
    assert!(
        db.branch_checkpoint_counters()[0] > installed,
        "the checkpoint never installed: it failed at the settle bound"
    );
}

/// Engine review 18 MED 4, the success arm the review 11 MED 8 red lacked: with the trunk commit's
/// WAL flush NOT failing, a raised D0 fuzzy checkpoint whose capture covers the commit's ordered
/// flight does not commit before that flush (it is still held at 50 ms), and installs once the
/// commit is released, with no settle timeout. Mutants `settle_ignores_ordered` (it commits early)
/// and `settle_ordered_busy_at_once` (it never installs).
#[cfg(target_vendor = "apple")]
#[test]
fn a_raised_d0_fuzzy_checkpoint_installs_once_the_ordered_flush_lands() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::PENDING_FULL_WAIT_MS.store(0, O::Release);
        }
    }
    let _restore = Restore;
    super::store::PENDING_FULL_WAIT_MS.store(60_000, O::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("settle-lands.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    let _x = trunk.fork_branch().unwrap().into_id();
    write_v(&trunk, 3, "raised");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    let (installed, timeouts) = (db.branch_checkpoint_counters()[0], db.branch_checkpoint_settle_timeouts());
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_TRUNK_BARRIER_DONE, O::Release);
    let committing = std::thread::spawn(move || trunk.execute("UPDATE t SET v = 'ordered' WHERE id = 40").map(|_| ()));
    wait_hold(&hold, super::store::HOLD_TRUNK_BARRIER_DONE);
    db.branch_checkpoint_hold(super::store::HOLD_AFTER_COMMIT);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    std::thread::sleep(std::time::Duration::from_millis(50));
    let early = db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED;
    hold.store(0, O::Release);
    let got = committing.join().unwrap();
    let t = std::time::Instant::now();
    let mut committed = false;
    while t.elapsed() < std::time::Duration::from_secs(10) {
        if db.branch_checkpoint_held() == super::store::HOLD_AFTER_COMMIT | super::store::HOLD_ARRIVED {
            committed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    db.branch_checkpoint_hold(0);
    db.branch_checkpoint_wait();
    assert!(got.is_ok(), "premise: the trunk commit committed: {got:?}");
    assert!(!early, "CLAIM: the checkpoint committed the catalog before the ordered flight's WAL flush");
    assert!(committed, "CLAIM: the checkpoint never committed once the ordered flight's WAL flush landed");
    assert_eq!(db.branch_checkpoint_counters()[0], installed + 1, "CLAIM: the checkpoint did not install");
    assert_eq!(db.branch_checkpoint_settle_timeouts(), timeouts, "the settle timed out");
}

/// Engine review 18 MED 4, the bound arm: with the bound at 20 ms and the trunk commit held past
/// it before its WAL flush, the checkpoint fails Busy, installs nothing, and the timeout is counted
/// (`branch_checkpoint_settle_timeouts`).
#[cfg(target_vendor = "apple")]
#[test]
fn a_raised_d0_fuzzy_checkpoint_fails_at_the_settle_bound_and_counts_it() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::PENDING_FULL_WAIT_MS.store(0, O::Release);
        }
    }
    let _restore = Restore;
    super::store::PENDING_FULL_WAIT_MS.store(20, O::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let db = open_at(&dir.path().join("settle-bound.db"), opts(true, SyncClass::Off));
    let trunk = db.connect().unwrap();
    seed_wide(&trunk);
    trunk.execute("PRAGMA synchronous = FULL").unwrap();
    trunk.execute("PRAGMA fullfsync = ON").unwrap();
    let _x = trunk.fork_branch().unwrap().into_id();
    write_v(&trunk, 3, "raised");
    assert!(db.branches.rewrite_class_for_test().syncs(), "premise: the trunk commit raised the log");
    let (installed, timeouts) = (db.branch_checkpoint_counters()[0], db.branch_checkpoint_settle_timeouts());
    let hold = db.branches.trunk_commit_hold.clone();
    hold.store(super::store::HOLD_TRUNK_BARRIER_DONE, O::Release);
    let committing = std::thread::spawn(move || trunk.execute("UPDATE t SET v = 'ordered' WHERE id = 40").map(|_| ()));
    wait_hold(&hold, super::store::HOLD_TRUNK_BARRIER_DONE);
    assert!(db.branch_checkpoint_fuzzy_now().unwrap(), "premise: a fuzzy checkpoint started");
    db.branch_checkpoint_wait();
    let (after_installed, after_timeouts) = (db.branch_checkpoint_counters()[0], db.branch_checkpoint_settle_timeouts());
    hold.store(0, O::Release);
    let got = committing.join().unwrap();
    assert!(got.is_ok(), "premise: the trunk commit committed: {got:?}");
    assert_eq!(after_installed, installed, "a checkpoint installed over an ordered flight whose flush was still to come");
    assert_eq!(after_timeouts, timeouts + 1, "CLAIM: the settle bound's Busy was not counted once");
}

/// Engine review 20 item 4: `connect_named`'s error path mapped any open error to NoSuchBranch once
/// the branch read as gone or released in memory (`is_gone_or_released`), which neither waits for
/// the drop's Release to be durable nor applies the fail-stop rule. A connect parked between its
/// lookup and its open while a drop's Release flight FAILS (the store fail-stops; the drop returns
/// Err, so the name was never freed): the connect fails, and not as NoSuchBranch, whose promise is
/// that no unreleased branch has the name. Both stores. Mutant `connect_race_maps_released`.
#[test]
fn a_connect_racing_a_failed_drop_is_not_refused_as_no_such_branch() {
    let _s = serial();
    for catalog in [false, true] {
        let what = format!("catalog={catalog}");
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("connect-failed-drop.db"), opts(catalog, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.create_branch("kept").unwrap();
        let hold = db.branches.trunk_commit_hold.clone();
        hold.store(super::store::HOLD_CONNECT_LOOKED_UP, std::sync::atomic::Ordering::Release);
        let connector = {
            let db = db.clone();
            std::thread::spawn(move || db.connect_named("kept").map(|_| ()))
        };
        wait_hold(&hold, super::store::HOLD_CONNECT_LOOKED_UP);
        db.branch_failpoint(Some(BranchFailpoint::GroupFlightFails));
        let dropped = db.drop_branch("kept");
        hold.store(0, std::sync::atomic::Ordering::Release);
        let connected = connector.join().unwrap();
        assert!(dropped.is_err(), "{what}: premise: the drop's failed Release flight failed the drop: {dropped:?}");
        assert!(
            connected.is_err() && !matches!(connected, Err(LimboError::NoSuchBranch(_))),
            "{what}: CLAIM: a connect racing a drop whose Release failed was answered {connected:?}: \
             the name was never freed, so it is not NoSuchBranch"
        );
    }
}

/// DECISIONS bd0d845f97 (lead, ordered after c9c821dd8d): the background writer synced the snapshot
/// compaction holding the store mutex, so `compaction_waits` counted a stall it did not remove: at
/// C=1 the next operation after the threshold-crossing create waited behind the compaction's syncs.
/// With the writer held mid-compaction (`HOLD_COMPACT_LOCKED`, the stage that held the mutex at
/// c9c821dd8d), a create on another connection is acknowledged at once, having waited for nothing
/// and synced nothing on its thread; released, the compaction completes (the log shrinks), and no
/// operation was counted waiting behind it. Raised D0 snapshot store. Mutant
/// `compaction_syncs_under_mutex`.
#[test]
fn a_create_does_not_wait_for_a_background_compaction() {
    let _s = serial();
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("compact-no-wait.db"), false, true);
    let _x = x.into_id();
    db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_LOCKED);
    let _t = Threshold::set(8 << 10);
    let base = db.branch_exception_counters();
    let longest = db.branches.log_len_for_test();
    let t = std::time::Instant::now();
    while !db.branches.compaction_owed_for_test() {
        let _ = trunk.fork_branch().unwrap().into_id();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no create owed a compaction");
    }
    eventually("premise: the background compaction never reached its hold", || {
        db.branches.confirm_held_for_test() == super::store::HOLD_COMPACT_LOCKED | super::store::HOLD_ARRIVED
    });
    let (tx, rx) = std::sync::mpsc::channel();
    let creator = {
        let db = db.clone();
        std::thread::spawn(move || {
            let (waits, syncs) = (super::store::thread_waits(), super::store::thread_syncs());
            let created = db.connect().and_then(|c| c.fork_branch()).map(|b| b.into_id());
            let _ = tx.send((created.is_ok(), super::store::thread_waits() - waits, super::store::thread_syncs() - syncs));
        })
    };
    let created = rx.recv_timeout(std::time::Duration::from_secs(10));
    db.branches.confirm_hold_for_test(0);
    creator.join().unwrap();
    assert_eq!(
        created,
        Ok((true, 0, 0)),
        "CLAIM: a create during the background compaction was not acknowledged at once \
         (created, waits, syncs on its thread; a timeout: it waited behind the compaction)"
    );
    eventually("the background compaction never shrank the log", || {
        !db.branches.compaction_owed_for_test() && db.branches.log_len_for_test() < longest.max(8 << 10)
    });
    let after = db.branch_exception_counters();
    assert_eq!(after[3], base[3], "an operation was counted waiting behind the background compaction");
    assert_eq!(after[2], base[2], "a compaction synced inline on an operation");
}

/// DECISIONS bd0d845f97, the recovery rule the background compaction rests on: it renames its
/// snapshot in before it cuts the log, so a crash between the two leaves the snapshot of generation
/// g + 1 beside the log of generation g, whose records past the compaction's `Record::Checkpoint`
/// marker follow the snapshot. Recovery replays them (as a catalog store's older log), where a
/// snapshot store reset an older log before. A D1 snapshot store whose compaction stops there
/// (`COMPACT_STOP_BEFORE_CUT`), then branches created and written after it, then a reopen: every
/// branch is back, and the one written after reads its row. Mutant `snapshot_older_log_reset`.
#[test]
fn a_background_compaction_stopped_before_its_cut_loses_nothing() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::COMPACT_STOP_BEFORE_CUT.store(false, O::Release);
        }
    }
    let _restore = Restore;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("compact-stop.db");
    let (ids, written, incarnation) = {
        let db = open_at(&path, opts(false, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        super::store::COMPACT_STOP_BEFORE_CUT.store(true, O::Release);
        let _t = Threshold::set(8 << 10);
        let t = std::time::Instant::now();
        while db.branches.background_compactions() == 0 {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no background compaction ran");
        }
        eventually("premise: the background compaction never stopped", || !db.branches.compaction_owed_for_test());
        // After the stopped compaction: created and written into the older log only.
        let y = trunk.fork_branch().unwrap();
        write_v(&y.connect().unwrap(), 3, "after");
        let written = y.into_id();
        let ids = db.branch_ids().unwrap();
        assert!(ids.contains(&written), "premise: the branch written after the compaction is live");
        (ids, written, db.incarnation)
    };
    super::store::COMPACT_STOP_BEFORE_CUT.store(false, O::Release);
    let db = reopen(&path, opts(false, SyncClass::Fsync), incarnation);
    let back = db.branch_ids().unwrap();
    let lost: Vec<_> = ids.iter().filter(|id| !back.contains(id)).collect();
    assert!(lost.is_empty(), "CLAIM: branches lost after a compaction stopped before its cut: {lost:?}");
    assert_eq!(
        read_v(&db.branch(written).unwrap().connect().unwrap(), 3),
        "after",
        "CLAIM: the write made after the stopped compaction was lost"
    );
}

/// Lead order 1 (2026-10-15T07:29:43Z; gates ultrareview row 1): 1f70dde48 made recovery replay a
/// snapshot store's OLDER log past the marker of the snapshot's generation. A binary from before
/// it resets that log and loses every record past the marker (1f70dde48's own disclosed blind
/// spot), so it must REFUSE such a store, never reset it: the format version moves with the
/// replay. The crash image of `a_background_compaction_stopped_before_its_cut_loses_nothing`,
/// read at the last default-arm format before the replay (15), is refused; read at this build's
/// own, it recovers (the control).
#[test]
fn a_store_needing_the_older_log_replay_is_refused_by_an_older_format() {
    use std::sync::atomic::Ordering as O;
    const BEFORE_OLDER_LOG_REPLAY: u32 = 15;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::COMPACT_STOP_BEFORE_CUT.store(false, O::Release);
        }
    }
    let _restore = Restore;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("compact-format.db");
    {
        let db = open_at(&path, opts(false, SyncClass::Fsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        super::store::COMPACT_STOP_BEFORE_CUT.store(true, O::Release);
        let _t = Threshold::set(8 << 10);
        let t = std::time::Instant::now();
        while db.branches.background_compactions() == 0 {
            let _ = trunk.fork_branch().unwrap().into_id();
            assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no background compaction ran");
        }
        eventually("premise: the background compaction never stopped", || !db.branches.compaction_owed_for_test());
        let y = trunk.fork_branch().unwrap();
        write_v(&y.connect().unwrap(), 3, "after");
    }
    super::store::COMPACT_STOP_BEFORE_CUT.store(false, O::Release);
    let files = super::journal::BranchFiles::for_db(path.to_str().unwrap());
    assert!(files.snap.exists(), "premise: the stopped compaction installed no snapshot");
    let old = super::journal::Journal::recover_as(&files, SyncClass::Fsync, BEFORE_OLDER_LOG_REPLAY);
    assert!(
        old.is_err(),
        "CLAIM: a reader of format {BEFORE_OLDER_LOG_REPLAY}, which resets an older log, read a store \
         whose records past the marker only the older-log replay keeps"
    );
    drop(old);
    let own = super::journal::Journal::recover_as(&files, SyncClass::Fsync, super::journal::format_version(false));
    assert!(matches!(own, Ok(Some(_))), "premise: this build does not recover the store: {:?}", own.err());
}

/// DECISIONS bd0d845f97: the cut's last pass copies under the mutex what flights wrote after its
/// catch-ups, and syncs it only when a flight there claimed a sync. A raised D0 store's flights are
/// only written, so that copy syncs nothing, and no operation is counted waiting behind it. With no
/// catch-up pass (`COMPACT_NO_CATCH_UP`), the writer held once its cut is prepared
/// (`HOLD_COMPACT_PREPARED`), one create's flight lands in the old log; released, the compaction
/// completes with that tail, having synced nothing under the store mutex
/// (`syncs_under_store_mutex`), and `compaction_waits` has not moved. Mutant
/// `cut_tail_always_synced` (the tail synced whatever it claims).
#[test]
fn a_raised_d0_cut_tail_is_copied_without_a_counted_sync() {
    use std::sync::atomic::Ordering as O;
    let _s = serial();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            super::store::COMPACT_NO_CATCH_UP.store(false, O::Release);
        }
    }
    let _restore = Restore;
    super::store::COMPACT_NO_CATCH_UP.store(true, O::Release);
    let dir = tempfile::TempDir::new().unwrap();
    let (db, trunk, x, _) = raised_d0_with_a_kept_pre_image(&dir.path().join("compact-tail.db"), false, true);
    let _x = x.into_id();
    db.branches.confirm_hold_for_test(super::store::HOLD_COMPACT_PREPARED);
    let _t = Threshold::set(8 << 10);
    let base = db.branch_exception_counters();
    let t = std::time::Instant::now();
    while !db.branches.compaction_owed_for_test() {
        let _ = trunk.fork_branch().unwrap().into_id();
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "premise: no create owed a compaction");
    }
    eventually("premise: the background compaction never prepared its cut", || {
        db.branches.confirm_held_for_test() == super::store::HOLD_COMPACT_PREPARED | super::store::HOLD_ARRIVED
    });
    let len = db.branches.log_len_for_test();
    let _ = trunk.fork_branch().unwrap().into_id();
    assert!(db.branches.log_len_for_test() > len, "premise: a create's flight wrote past the prepared cut");
    let under = super::store::syncs_under_store_mutex();
    db.branches.confirm_hold_for_test(0);
    eventually("the background compaction never completed", || !db.branches.compaction_owed_for_test());
    let synced_under = super::store::syncs_under_store_mutex() - under;
    let after = db.branch_exception_counters();
    assert!(db.branches.log_len_for_test() < len, "premise: the log was cut");
    assert_eq!(synced_under, 0, "CLAIM: the cut synced {synced_under} times under the store mutex for a tail of only-written flights");
    assert_eq!(after[3], base[3], "an operation was counted waiting behind the compaction: {base:?} -> {after:?}");
}

/// A commit's WAL fsync covers the frames it wrote, so the WAL is not left dirty after it: the WAL
/// published the commit's frames AFTER its sync (`commit_prepared_frames` -> `complete_append_frame`
/// set `dirty` again), so the next commit with no frames of its own at synchronous FULL synced the
/// WAL once more for nothing (pager WaitSync: `need_fsync = frames || wal.is_dirty()`): one extra
/// F_FULLFSYNC per frameless FULL commit. Two FULL commits in a row (the first leaves the WAL
/// synced), then an UPDATE matching no row: it makes no sync. Trunk only, both stores.
#[test]
fn a_frameless_full_commit_after_a_synced_one_syncs_nothing() {
    let _s = serial();
    for catalog in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let db = open_at(&dir.path().join("frameless-clean.db"), opts(catalog, SyncClass::FullFsync));
        let trunk = db.connect().unwrap();
        seed(&trunk);
        trunk.execute("PRAGMA synchronous = FULL").unwrap();
        trunk.execute("PRAGMA fullfsync = ON").unwrap();
        write_v(&trunk, 1, "a");
        write_v(&trunk, 2, "b");
        let before = sync_counts();
        trunk.execute("UPDATE t SET v = v WHERE id = -1").unwrap();
        let after = sync_counts();
        assert_eq!(
            (after.fsync - before.fsync, after.full_fsync - before.full_fsync),
            (0, 0),
            "catalog={catalog}: CLAIM: a commit with no frames after a synced one synced the WAL (fsync, full fsync)"
        );
    }
}
