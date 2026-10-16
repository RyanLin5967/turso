//! Links adversary H1 (DECISIONS 7e77068514): a store's format version has ONE authority, the
//! version its record carries (a catalog store's meta row). Every file is written at it; it advances
//! only in the checkpoint transaction that upgrades the catalog, with the log rewrite after it; an
//! open refuses only combinations no build writes, and says which.
//!
//! The first three tests are the adversary's reproductions, verbatim (`links_adv_tests.rs`, 17
//! stand-in). The rest run the same three paths for every older version this build reads (18, 19,
//! 20: the splice arm's legacy one and the L4 meta facts' pair), and add the invariant they share:
//! until this build's first checkpoint, the log stays at the catalog's version, so the older build
//! still opens the store.

use super::*;
use crate::{Database, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::sync::Arc;

// ---- the adversary's reproductions (links_adv_tests.rs, verbatim) ----

fn open_class(path: &std::path::Path, sync: SyncClass) -> crate::Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync })
            .with_branch_checkpoint(BranchCheckpoint::Fuzzy),
        None,
        Arc::new(SqliteDialect),
    )
}

fn image(src: &std::path::Path, dst: &std::path::Path) {
    for suffix in ["", "-wal", "-branch-log", "-branch-arena", "-branch-cat", "-branch-cat-wal"] {
        let from = std::path::PathBuf::from(format!("{}{suffix}", src.display()));
        let to = std::path::PathBuf::from(format!("{}{suffix}", dst.display()));
        let _ = std::fs::remove_file(&to);
        if from.exists() {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

fn log_version(path: &std::path::Path) -> u32 {
    let bytes = std::fs::read(format!("{}-branch-log", path.display())).unwrap();
    u32::from_le_bytes(bytes[8..12].try_into().unwrap())
}

/// A 17 store (the legacy stand-in made and checkpointed it, then logged more flights) whose last
/// flight is torn. Returns the path of the crash image.
fn legacy_store_with_tail(dir: &std::path::Path, sync: SyncClass) -> std::path::PathBuf {
    let path = dir.join("l.db");
    let img = dir.join("img.db");
    super::journal::CREATE_LEGACY.with(|c| c.set(true));
    {
        let db = open_class(&path, sync).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        for name in ["a0", "a1", "a2"] {
            trunk.create_branch(name).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(db.branches.links_known_and_catalog_format_for_test().1, Some(17), "premise: a 17 catalog");
        for name in ["a3", "a4"] {
            trunk.create_branch(name).unwrap();
        }
        image(&path, &img);
    }
    super::journal::CREATE_LEGACY.with(|c| c.set(false));
    img
}

/// ADVERSARY: a 17 store whose torn tail this build cuts at its open (in a class that does not sync,
/// the cut is a rewrite of the log), then crashes before its first checkpoint. The rewrite is at
/// 21 (`Journal::upgrade_format`) while the catalog's meta row still says 17: does the store open?
#[test]
fn adv_torn_tail_cut_of_a_17_store_then_a_crash_reopens() {
    let dir = tempfile::TempDir::new().unwrap();
    let img = legacy_store_with_tail(dir.path(), SyncClass::Off);
    assert_eq!(log_version(&img), 17, "premise: a 17 log");
    // Tear the last flight.
    let log = format!("{}-branch-log", img.display());
    let len = std::fs::metadata(&log).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
    f.set_len(len - 7).unwrap();
    drop(f);
    let img2 = dir.path().join("img2.db");
    {
        let db = open_class(&img, SyncClass::Off).unwrap();
        db.branch_wait_name_filter();
        let (_, fmt) = db.branches.links_known_and_catalog_format_for_test();
        println!("ADV torn: after this build's open: catalog meta {fmt:?}, log header {}", log_version(&img));
        // Crash before any checkpoint of this build.
        image(&img, &img2);
    }
    match open_class(&img2, SyncClass::Off) {
        Ok(db) => {
            for name in ["a0", "a1", "a2"] {
                db.connect_named(name).unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
        Err(e) => panic!("ADV torn: the store no longer opens after this build's recovery: {e}"),
    }
}

/// The child of the next test: the legacy stand-in (an older build) opens the store, logs two
/// creates and checkpoints, killed by the parent's FE_KILL_AT at `ckpt.written` (committed, the log
/// not cut).
#[test]
fn adv_legacy_kill_child() {
    let Ok(path) = std::env::var("FE_ADV_CHILD") else {
        return;
    };
    super::journal::CREATE_LEGACY.with(|c| c.set(true));
    let db = open_class(std::path::Path::new(&path), SyncClass::Fsync).unwrap();
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    for name in ["k5", "k6"] {
        trunk.create_branch(name).unwrap();
    }
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    std::process::exit(3);
}

/// ADVERSARY: an older build's checkpoint killed after its commit and before its cut (F-FZ crash
/// state S1, the marker kept): this build's open replays the tail and rewrites the log (at 21),
/// the meta row still 17; then a crash before this build's first checkpoint. Does it reopen?
#[cfg(unix)]
#[test]
fn adv_older_builds_interrupted_checkpoint_then_this_builds_crash_reopens() {
    let dir = tempfile::TempDir::new().unwrap();
    let img = legacy_store_with_tail(dir.path(), SyncClass::Fsync);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::format_authority_tests::adv_legacy_kill_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_ADV_CHILD", &img)
        .env("FE_KILL_AT", "ckpt.written:1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), None, "premise: the legacy child was killed at ckpt.written ({status:?})");
    let before = log_version(&img);
    let img2 = dir.path().join("img2.db");
    {
        let db = open_class(&img, SyncClass::Fsync).unwrap();
        db.branch_wait_name_filter();
        let (_, fmt) = db.branches.links_known_and_catalog_format_for_test();
        println!("ADV marker: log header {before} before, {} after this build's open; catalog meta {fmt:?}", log_version(&img));
        image(&img, &img2);
    }
    match open_class(&img2, SyncClass::Fsync) {
        Ok(db) => {
            for name in ["a0", "a1", "k5", "k6"] {
                db.connect_named(name).unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
        Err(e) => panic!("ADV marker: the store no longer opens after this build's recovery: {e}"),
    }
}

/// ADVERSARY: as the torn-tail test, but this build CLOSES cleanly after its recovery (no crash).
#[test]
fn adv_torn_tail_cut_of_a_17_store_then_a_clean_close_reopens() {
    let dir = tempfile::TempDir::new().unwrap();
    let img = legacy_store_with_tail(dir.path(), SyncClass::Off);
    let log = format!("{}-branch-log", img.display());
    let len = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&log).unwrap().set_len(len - 7).unwrap();
    {
        let db = open_class(&img, SyncClass::Off).unwrap();
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        trunk.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        drop(trunk);
        drop(db);
    }
    println!("ADV clean: after a clean close, log header {}", log_version(&img));
    if let Err(e) = open_class(&img, SyncClass::Off) {
        panic!("ADV clean: the store no longer opens after a clean close: {e}");
    }
}

// ---- every older version this build reads: 18, 19, 20 (and 17 again, with the invariant) ----

/// Stand in for the older build that writes `version` (17-20) on this thread, or for this build
/// (`None`).
fn stand_in(version: Option<u32>) {
    super::journal::CREATE_LEGACY.with(|c| c.set(version.is_some()));
    super::journal::LEGACY_FACTS.with(|c| c.set(version.is_some_and(|v| v >= 19)));
}

fn splice_of(version: u32) -> bool {
    version % 2 == 0
}

/// This build's version of `version`'s arm.
fn current_of(version: u32) -> u32 {
    if splice_of(version) {
        22
    } else {
        21
    }
}

/// A catalog store in `version`'s arm: fuzzy checkpoints, or sharp ones in the splice arm (S-12).
fn open_arm(path: &std::path::Path, sync: SyncClass, version: u32) -> crate::Result<Arc<Database>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let splice = splice_of(version);
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync })
            .with_branch_splice(splice)
            .with_branch_checkpoint(if splice { BranchCheckpoint::Sharp } else { BranchCheckpoint::Fuzzy }),
        None,
        Arc::new(SqliteDialect),
    )
}

/// The catalog meta row's version, read by a handle of this build.
fn catalog_version(db: &Arc<Database>) -> Option<u32> {
    db.branches.links_known_and_catalog_format_for_test().1
}

/// `legacy_store_with_tail` for the older build that writes `version`: a store it made and
/// checkpointed, then logged two more creates into. Returns the crash image's path.
fn older_store_with_tail(dir: &std::path::Path, sync: SyncClass, version: u32) -> std::path::PathBuf {
    let path = dir.join("o.db");
    let img = dir.join("oimg.db");
    stand_in(Some(version));
    {
        let db = open_arm(&path, sync, version).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        for name in ["a0", "a1", "a2"] {
            trunk.create_branch(name).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(catalog_version(&db), Some(version), "premise: a {version} catalog");
        assert!(!db.branches.catalog_links_for_test(), "premise: the {version} catalog has no link columns");
        for name in ["a3", "a4"] {
            trunk.create_branch(name).unwrap();
        }
        image(&path, &img);
    }
    stand_in(None);
    assert_eq!(log_version(&img), version, "premise: a {version} log");
    img
}

fn tear_last_flight(img: &std::path::Path) {
    let log = format!("{}-branch-log", img.display());
    let len = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&log).unwrap().set_len(len - 7).unwrap();
}

/// This build's open of an older store, before any checkpoint of its own: the files stay at the
/// catalog's version (one authority), so the image taken here is still the older build's store.
fn open_and_crash_image(img: &std::path::Path, img2: &std::path::Path, sync: SyncClass, version: u32) {
    let db = open_arm(img, sync, version).unwrap();
    db.branch_wait_name_filter();
    assert_eq!(catalog_version(&db), Some(version), "this build's open changed the catalog's version");
    assert_eq!(
        log_version(img),
        version,
        "this build's recovery rewrote the log away from the catalog's version {version} (two authorities)"
    );
    image(img, img2);
}

/// After this build's crash: the store opens in this build with every branch, still at `version`;
/// this build's first checkpoint upgrades it, catalog first and log after, and it reopens.
fn reopens_then_upgrades(img: &std::path::Path, sync: SyncClass, version: u32, names: &[&str]) {
    {
        let db = open_arm(img, sync, version).unwrap_or_else(|e| panic!("{version}: the store no longer opens: {e}"));
        db.branch_wait_name_filter();
        for name in names {
            db.connect_named(name).unwrap_or_else(|e| panic!("{version} {name}: {e}"));
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        let now = current_of(version);
        assert_eq!(catalog_version(&db), Some(now), "{version}: this build's first checkpoint did not upgrade the catalog");
        assert_eq!(log_version(img), now, "{version}: the upgrading checkpoint's cut left the log at another version");
        assert_eq!(db.branches.format_for_test(), Some(now), "{version}: the journal does not write the catalog's version");
    }
    let db = open_arm(img, sync, version).unwrap_or_else(|e| panic!("{version}: the upgraded store does not open: {e}"));
    for name in names {
        db.connect_named(name).unwrap_or_else(|e| panic!("{version} upgraded {name}: {e}"));
    }
    db.branches.l4_facts_check_for_test().unwrap_or_else(|e| panic!("{version} upgraded: oracle: {e}"));
}

/// The torn-tail cut, then a crash: the older build still opens the crash image (its files are its
/// own version), and so does this build, which then upgrades it.
fn torn_tail_then_a_crash(version: u32) {
    let dir = tempfile::TempDir::new().unwrap();
    let img = older_store_with_tail(dir.path(), SyncClass::Off, version);
    tear_last_flight(&img);
    let img2 = dir.path().join("oimg2.db");
    open_and_crash_image(&img, &img2, SyncClass::Off, version);
    let img3 = dir.path().join("oimg3.db");
    image(&img2, &img3);
    stand_in(Some(version));
    let older = open_arm(&img3, SyncClass::Off, version).map(|db| {
        for name in ["a0", "a1", "a2"] {
            db.connect_named(name).unwrap_or_else(|e| panic!("{version}: the older build lost {name}: {e}"));
        }
    });
    stand_in(None);
    older.unwrap_or_else(|e| panic!("{version}: the older build no longer opens the store this build recovered: {e}"));
    reopens_then_upgrades(&img2, SyncClass::Off, version, &["a0", "a1", "a2"]);
}

#[test]
fn a_torn_tail_cut_of_a_17_store_then_a_crash_leaves_it_17_and_it_reopens() {
    torn_tail_then_a_crash(17);
}

#[test]
fn a_torn_tail_cut_of_an_18_store_then_a_crash_leaves_it_18_and_it_reopens() {
    torn_tail_then_a_crash(18);
}

#[test]
fn a_torn_tail_cut_of_a_19_store_then_a_crash_leaves_it_19_and_it_reopens() {
    torn_tail_then_a_crash(19);
}

#[test]
fn a_torn_tail_cut_of_a_20_store_then_a_crash_leaves_it_20_and_it_reopens() {
    torn_tail_then_a_crash(20);
}

/// The child of `an_older_builds_interrupted_checkpoint`: the older build of `FE_FA_VERSION` opens
/// the store at `FE_FA_CHILD`, logs two creates and checkpoints, killed by FE_KILL_AT.
#[test]
fn fa_older_build_kill_child() {
    let (Ok(path), Ok(version)) = (std::env::var("FE_FA_CHILD"), std::env::var("FE_FA_VERSION")) else {
        return;
    };
    let version: u32 = version.parse().unwrap();
    stand_in(Some(version));
    let db = open_arm(std::path::Path::new(&path), SyncClass::Fsync, version).unwrap();
    db.branch_wait_name_filter();
    let trunk = db.connect().unwrap();
    for name in ["k5", "k6"] {
        trunk.create_branch(name).unwrap();
    }
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    std::process::exit(3);
}

/// The older build's checkpoint killed after its commit and before its cut (F-FZ crash state S1),
/// this build's recovery (which rewrites the log), then a crash before its first checkpoint.
#[cfg(unix)]
fn an_older_builds_interrupted_checkpoint(version: u32) {
    let dir = tempfile::TempDir::new().unwrap();
    let img = older_store_with_tail(dir.path(), SyncClass::Fsync, version);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::format_authority_tests::fa_older_build_kill_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_FA_CHILD", &img)
        .env("FE_FA_VERSION", version.to_string())
        .env("FE_KILL_AT", "ckpt.written:1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), None, "premise: the {version} child was killed at ckpt.written ({status:?})");
    let img2 = dir.path().join("oimg2.db");
    open_and_crash_image(&img, &img2, SyncClass::Fsync, version);
    reopens_then_upgrades(&img2, SyncClass::Fsync, version, &["a0", "a1", "k5", "k6"]);
}

#[cfg(unix)]
#[test]
fn an_older_17_builds_interrupted_checkpoint_then_this_builds_crash_reopens() {
    an_older_builds_interrupted_checkpoint(17);
}

#[cfg(unix)]
#[test]
fn an_older_18_builds_interrupted_checkpoint_then_this_builds_crash_reopens() {
    an_older_builds_interrupted_checkpoint(18);
}

#[cfg(unix)]
#[test]
fn an_older_19_builds_interrupted_checkpoint_then_this_builds_crash_reopens() {
    an_older_builds_interrupted_checkpoint(19);
}

#[cfg(unix)]
#[test]
fn an_older_20_builds_interrupted_checkpoint_then_this_builds_crash_reopens() {
    an_older_builds_interrupted_checkpoint(20);
}

/// The torn-tail cut, writes, and a clean close (which does not checkpoint).
fn torn_tail_then_a_clean_close(version: u32) {
    let dir = tempfile::TempDir::new().unwrap();
    let img = older_store_with_tail(dir.path(), SyncClass::Off, version);
    tear_last_flight(&img);
    {
        let db = open_arm(&img, SyncClass::Off, version).unwrap();
        db.branch_wait_name_filter();
        let trunk = db.connect().unwrap();
        trunk.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        trunk.create_branch("c0").unwrap();
    }
    assert_eq!(log_version(&img), version, "{version}: a clean close left the log away from the catalog's version");
    reopens_then_upgrades(&img, SyncClass::Off, version, &["a0", "a1", "a2", "c0"]);
}

#[test]
fn a_torn_tail_cut_of_a_17_store_then_a_clean_close_reopens() {
    torn_tail_then_a_clean_close(17);
}

#[test]
fn a_torn_tail_cut_of_an_18_store_then_a_clean_close_reopens() {
    torn_tail_then_a_clean_close(18);
}

#[test]
fn a_torn_tail_cut_of_a_19_store_then_a_clean_close_reopens() {
    torn_tail_then_a_clean_close(19);
}

#[test]
fn a_torn_tail_cut_of_a_20_store_then_a_clean_close_reopens() {
    torn_tail_then_a_clean_close(20);
}

// ---- the open's refusal: only combinations no build writes, named as what they are ----

/// A log at a version newer than its catalog's (here, a 21 store whose meta row is set back to 17)
/// is written by no build: refused, and named so; never taken for the other splice arm's.
#[test]
fn a_log_newer_than_its_catalog_is_refused_as_such() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("n.db");
    {
        let db = open_arm(&path, SyncClass::Off, 21).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.create_branch("b0").unwrap();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        trunk.create_branch("b1").unwrap();
        assert_eq!(catalog_version(&db), Some(21), "premise: a 21 catalog");
    }
    assert_eq!(log_version(&path), 21, "premise: a 21 log");
    {
        let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap();
        let mut meta = cat.meta().unwrap().expect("premise: a meta row");
        meta.format = 17;
        cat.begin().unwrap();
        cat.put_meta(&meta).unwrap();
        cat.commit().unwrap();
    }
    let err = match open_arm(&path, SyncClass::Off, 21) {
        Ok(_) => panic!("a 21 log under a 17 catalog opened"),
        Err(e) => e.to_string(),
    };
    println!("refusal: {err}");
    assert!(err.contains("log format version 21"), "the refusal does not name the log's version: {err}");
    assert!(err.contains("newer than"), "the refusal does not say the log is newer than its catalog: {err}");
    assert!(!err.contains("other F7 splice arm"), "21 is not the other arm of a 17 store: {err}");
}

/// The other arm's log under a catalog of this arm is still named as the other arm's.
#[test]
fn the_other_arms_log_is_still_named_as_the_other_arms() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("a.db");
    {
        let db = open_arm(&path, SyncClass::Off, 22).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        trunk.create_branch("b0").unwrap();
    }
    let err = match open_arm(&path, SyncClass::Off, 21) {
        Ok(_) => panic!("a 22 store opened in the 21 arm"),
        Err(e) => e.to_string(),
    };
    println!("refusal: {err}");
    assert!(err.contains("other F7 splice arm"), "the other arm's store is not named as such: {err}");
}

// ---- L2: link validity keys on the recorded format, not on the columns existing ----

/// A 17 catalog that has the link columns (a state no build writes: the columns come only with the
/// meta row at 21) holds NULL links that mean nothing: they are not taken as "no neighbour", and
/// this build's first checkpoint upgrades it as any 17 catalog.
#[test]
fn a_17_catalog_with_the_link_columns_is_not_read_as_linked_and_is_upgraded() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("l2.db");
    stand_in(Some(17));
    {
        let db = open_arm(&path, SyncClass::Off, 17).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for name in ["a0", "a1", "a2", "a3"] {
            trunk.create_branch(name).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(catalog_version(&db), Some(17), "premise: a 17 catalog");
    }
    stand_in(None);
    {
        let mut cat = super::catalog::Catalog::open(&std::path::PathBuf::from(format!("{}-branch-cat", path.display())), SyncClass::Off).unwrap();
        cat.add_link_columns_for_test().unwrap();
    }
    let db = open_arm(&path, SyncClass::Off, 17).unwrap();
    db.branch_wait_name_filter();
    for name in ["a0", "a1", "a2", "a3"] {
        drop(db.connect_named(name).unwrap());
    }
    let (known, fmt) = db.branches.links_known_and_catalog_format_for_test();
    assert_eq!(fmt, Some(17), "premise: the meta row is 17");
    assert!(db.branches.catalog_links_for_test(), "premise: the catalog has the link columns");
    assert_eq!(known, 0, "a 17 catalog's NULL link columns were read as links");
    // A delete in the middle, then this build's first checkpoint: the upgrade.
    db.drop_branch("a1").unwrap();
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    assert_eq!(catalog_version(&db), Some(21), "the checkpoint did not upgrade the catalog");
    drop(db);
    let db = open_arm(&path, SyncClass::Off, 17).unwrap();
    for name in ["a0", "a2", "a3"] {
        drop(db.connect_named(name).unwrap());
    }
    let (known, _) = db.branches.links_known_and_catalog_format_for_test();
    assert!(known > 0, "premise: the upgraded catalog's links are read");
    db.branches.l4_facts_check_for_test().unwrap_or_else(|e| panic!("upgraded from the columns-only state: {e}"));
}

// ---- L1: a splice's records are kept only where a checkpoint consumes them ----

fn open_mode(path: &std::path::Path, durability: BranchDurability) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(durability)
            .with_branch_splice(true)
            .with_branch_checkpoint(BranchCheckpoint::Sharp),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// Splices in a store with no catalog (volatile; eager log and snapshot) leave no splice records:
/// nothing would ever drain them.
#[test]
fn splices_without_a_catalog_keep_no_splice_records() {
    for durability in [BranchDurability::Volatile, BranchDurability::Durable { sync: SyncClass::Off }] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("s.db");
        let db = open_mode(&path, durability);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        let p = trunk.fork_branch().unwrap();
        let mut spliced = 0;
        for _ in 0..8 {
            let z = p.fork().unwrap();
            let c = z.fork().unwrap();
            let reaped = z.reap().unwrap();
            assert!(reaped.deferred, "premise: the zombie's one child is spliced into its place ({reaped:?})");
            spliced += 1;
            let _ = c.into_id();
        }
        assert_eq!(spliced, 8);
        assert_eq!(
            db.branches.children_moved_relinked_for_test(),
            (0, 0),
            "{durability:?}: splice records kept with no checkpoint to consume them"
        );
        let _ = p.into_id();
    }
}

/// A catalog store: a spliced child deleted before the next checkpoint leaves no splice record
/// after it (the checkpoint's capture cannot name a child that is gone).
#[test]
fn a_spliced_child_deleted_before_a_checkpoint_leaves_no_splice_record() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("c.db");
    let db = open_mode(&path, BranchDurability::Catalog { sync: SyncClass::Off });
    let trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let p = trunk.fork_branch().unwrap();
    let z = p.fork().unwrap();
    let c = z.fork().unwrap();
    db.branch_compact_now().unwrap();
    let reaped = z.reap().unwrap();
    assert!(reaped.deferred, "premise: C is spliced into Z's place ({reaped:?})");
    assert_eq!(db.branches.children_moved_relinked_for_test().1, 1, "premise: C's key is a splice record");
    c.reap().unwrap();
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    assert_eq!(
        db.branches.children_moved_relinked_for_test(),
        (0, 0),
        "a deleted spliced child's key outlived the checkpoint after it"
    );
    db.branches.l4_facts_check_for_test().unwrap_or_else(|e| panic!("{e}"));
    let _ = p.into_id();
}

// ---- links adversary L4 (ruled a FIX, class f398de1ff0: mutex work growing with N): the upgrade's
// fill runs in bounded batches with the store mutex free between them; the meta row flips in the
// checkpoint transaction that commits the last of it ----

/// The child of `the_first_checkpoint_of_an_18_store_holds_the_store_mutex_for_no_o_n_upgrade_work`:
/// an 18 store of `FE_FA_HOLD_N` branches made by the older build, then this build's first
/// checkpoint, with the largest single store-mutex hold measured around it (any thread).
#[test]
fn fa_upgrade_hold_child() {
    let (Ok(dir), Ok(n)) = (std::env::var("FE_FA_HOLD_DIR"), std::env::var("FE_FA_HOLD_N")) else {
        return;
    };
    let n: usize = n.parse().unwrap();
    // The store's version: 18 (the older build's, upgraded by the checkpoint), or 22 (this build's
    // own, the control: no upgrade).
    let version: u32 = std::env::var("FE_FA_HOLD_VERSION").map_or(18, |v| v.parse().unwrap());
    let path = std::path::Path::new(&dir).join("h.db");
    stand_in((version == 18).then_some(18));
    {
        let db = open_arm(&path, SyncClass::Off, 18).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for _ in 0..n {
            let _ = trunk.fork_branch().unwrap().into_id();
        }
        db.branch_compact_now().unwrap();
        assert_eq!(catalog_version(&db), Some(version), "premise: a {version} catalog");
    }
    stand_in(None);
    let db = open_arm(&path, SyncClass::Off, 18).unwrap();
    db.branch_wait_name_filter();
    let _ = super::budget_probe::take_hold_maxima();
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    let m = super::budget_probe::take_hold_maxima();
    println!("FA-HOLD n={n} rows={} bytes={} format={:?}", m.all.1, m.all.0, catalog_version(&db));
}

fn upgrade_hold(n: usize) -> (u64, u64, String) {
    upgrade_hold_at(n, 18)
}

fn upgrade_hold_at(n: usize, version: u32) -> (u64, u64, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::format_authority_tests::fa_upgrade_hold_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_FA_HOLD_DIR", dir.path())
        .env("FE_FA_HOLD_N", n.to_string())
        .env("FE_FA_HOLD_VERSION", version.to_string())
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success(), "the hold child at {n} failed:\n{text}{}", String::from_utf8_lossy(&out.stderr));
    // libtest prints the child's line after its own "test ... " prefix.
    let line = text
        .lines()
        .find_map(|l| l.find("FA-HOLD ").map(|i| &l[i..]))
        .unwrap_or_else(|| panic!("no reading at {n}:\n{text}"));
    let field = |k: &str| line.split_whitespace().find_map(|w| w.strip_prefix(k)).unwrap().to_string();
    (field("rows=").parse().unwrap(), field("bytes=").parse().unwrap(), field("format="))
}

/// L4: the first checkpoint of an 18 store (the splice arm, whose checkpoints are sharp) upgrades
/// it, and no single store-mutex hold does work that grows with the store: the largest hold
/// touches as many catalog rows at 10^4 branches as at 10^3 (+ 256), where one transaction filling
/// every row's links under the mutex touched about five per branch. 10^3, not 10, as the smaller
/// cell: a bounded batch is not smaller than a store of 10.
#[test]
fn the_first_checkpoint_of_an_18_store_holds_the_store_mutex_for_no_o_n_upgrade_work() {
    if std::env::var_os("FE_FA_HOLD_N").is_some() {
        return;
    }
    let (ra, ba, fa) = upgrade_hold(1_000);
    let (rb, bb, fb) = upgrade_hold(10_000);
    println!("L4 upgrade hold: 10^3 rows {ra} bytes {ba}; 10^4 rows {rb} bytes {bb}");
    assert_eq!((fa.as_str(), fb.as_str()), ("Some(22)", "Some(22)"), "premise: the checkpoint upgraded both stores");
    assert!(
        rb <= ra + 256,
        "the first checkpoint's largest store-mutex hold touches {ra} catalog rows at 10^3 branches and {rb} at 10^4: upgrade work under the mutex grows with N"
    );
}

/// The child of the upgrade kill tests: this build opens the older store at `FE_FA_KILL_DIR` and
/// checkpoints it, killed by the parent's FE_KILL_AT.
#[test]
fn fa_upgrade_kill_child() {
    let (Ok(dir), Ok(version)) = (std::env::var("FE_FA_KILL_DIR"), std::env::var("FE_FA_VERSION")) else {
        return;
    };
    let version: u32 = version.parse().unwrap();
    let db = open_arm(&std::path::Path::new(&dir).join("k.db"), SyncClass::Fsync, version).unwrap();
    db.branch_wait_name_filter();
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    std::process::exit(3);
}

/// An older store of 200 branches (several upgrade batches), this build's first checkpoint killed at
/// `point`: the store is still `version` (meta row and log), the older build still opens it, and
/// this build then finishes the upgrade, every branch kept and every link true.
#[cfg(unix)]
fn a_killed_upgrade_leaves_an_older_store(version: u32, point: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("k.db");
    stand_in(Some(version));
    let ids: Vec<BranchId> = {
        let db = open_arm(&path, SyncClass::Fsync, version).unwrap();
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        let ids: Vec<BranchId> = (0..200).map(|_| trunk.fork_branch().unwrap().into_id()).collect();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(catalog_version(&db), Some(version), "premise: a {version} catalog");
        ids
    };
    stand_in(None);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["branch::format_authority_tests::fa_upgrade_kill_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_FA_KILL_DIR", dir.path())
        .env("FE_FA_VERSION", version.to_string())
        .env("FE_KILL_AT", point)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), None, "premise: this build's upgrade was killed at {point} ({status:?})");
    let img = dir.path().join("img.db");
    image(&path, &img);
    {
        let db = open_arm(&path, SyncClass::Fsync, version).unwrap();
        db.branch_wait_name_filter();
        assert_eq!(catalog_version(&db), Some(version), "{version} {point}: the kill left the catalog at another version");
        assert_eq!(log_version(&path), version, "{version} {point}: the kill left the log at another version");
        assert_eq!(db.branches.links_known_and_catalog_format_for_test().0, 0, "{version} {point}: links of an unfinished upgrade were read");
    }
    stand_in(Some(version));
    let older = open_arm(&img, SyncClass::Fsync, version).map(|db| {
        for &id in &ids {
            let _ = db.branch(id).unwrap_or_else(|e| panic!("{version} {point}: the older build lost branch {}: {e}", id.0)).into_id();
        }
    });
    stand_in(None);
    older.unwrap_or_else(|e| panic!("{version} {point}: the older build no longer opens the store: {e}"));
    {
        let db = open_arm(&path, SyncClass::Fsync, version).unwrap();
        db.branch_wait_name_filter();
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
        assert_eq!(catalog_version(&db), Some(current_of(version)), "{version} {point}: the upgrade was not finished");
    }
    let db = open_arm(&path, SyncClass::Fsync, version).unwrap();
    for &id in &ids {
        let _ = db.branch(id).unwrap_or_else(|e| panic!("{version} {point}: branch {} lost: {e}", id.0)).into_id();
    }
    db.branches.l4_facts_check_for_test().unwrap_or_else(|e| panic!("{version} {point}: {e}"));
}

#[cfg(unix)]
#[test]
fn a_kill_between_upgrade_batches_leaves_an_18_store_that_finishes_later() {
    a_killed_upgrade_leaves_an_older_store(18, "ckpt.upgrade.batch:2");
}

#[cfg(unix)]
#[test]
fn a_kill_between_upgrade_batches_leaves_a_17_store_that_finishes_later() {
    a_killed_upgrade_leaves_an_older_store(17, "ckpt.upgrade.batch:2");
}

#[cfg(unix)]
#[test]
fn a_kill_at_the_upgrade_flip_leaves_an_18_store_that_finishes_later() {
    a_killed_upgrade_leaves_an_older_store(18, "ckpt.upgrade.flip:1");
}

#[cfg(unix)]
#[test]
fn a_kill_at_the_upgrade_flip_leaves_a_17_store_that_finishes_later() {
    a_killed_upgrade_leaves_an_older_store(17, "ckpt.upgrade.flip:1");
}

/// Diagnostic (no verdict): the same first checkpoint's largest hold for a store this build made
/// (22, no upgrade), beside the 18 store's, to place the hold BYTES the L4 red reads beside its
/// rows (522,955 at 10^3, 868,707 at 10^4 at 3582a98b5): the upgrade's, or every first checkpoint's.
#[test]
fn diag_first_checkpoint_hold_bytes_with_and_without_the_upgrade() {
    if std::env::var_os("FE_FA_HOLD_N").is_some() {
        return;
    }
    for version in [22, 18] {
        for n in [1_000, 10_000] {
            let (rows, bytes, fmt) = upgrade_hold_at(n, version);
            println!("DIAG hold version={version} n={n} rows={rows} bytes={bytes} format={fmt}");
        }
    }
}
