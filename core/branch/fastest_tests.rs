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
            "catalog={catalog}: a WAL hold without a locked fork, or the reverse: {after:?}"
        );
        assert!(after.locked_trunk_forks <= 5);
        assert_eq!(after.store.buckets.iter().sum::<u64>(), after.store.count);
    }
}
