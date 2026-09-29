//! r11-walpin-conc amendment 28: the fire-check of FW2's refusal attribution (`walpin::origin`).
//!
//! One trunk connection holds a read transaction begun on a fully checkpointed WAL, so its snapshot
//! ends in the current wal2 file. The trunk then fills that file (a switch) and checkpoints: every
//! refusal from then on is a checkpoint of the file the reader's snapshot ends in. The counters
//! must name the tag the reader began under, and a mutant that drops the tag must move them all to
//! TRUNK, so they read the tag and not the schedule. Every count is a delta of the process-global
//! counters; run with `--test-threads=1 --include-ignored`.

use super::walpin::{self, origin};
use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

#[derive(Debug)]
struct Refusals {
    refused: u64,
    init: u64,
    fork: u64,
    trunk: u64,
    init_only: u64,
}

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("walpin_attr.db");
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

/// The refusals counted while a reader begun under `tag` (None: no tag) holds its snapshot across a
/// switch and the checkpoints after it; `untagged` switches the mutant on while the reader begins.
fn refusals_blocked_by(tag: Option<u8>, untagged: bool) -> Refusals {
    let (fw1, fw2, fw3) = (walpin::fw1(), walpin::fw2(), walpin::fw3());
    walpin::set_fixes(true, true, false);
    let (_dir, db) = open_db();
    db.walpin_open_wal2().unwrap();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let reader = db.connect().unwrap();
    let pager = reader.pager.load().clone();
    origin::mutant::set_untagged(untagged);
    let begun = match tag {
        Some(tag) => origin::with(tag, || pager.begin_read_tx()),
        None => pager.begin_read_tx(),
    };
    origin::mutant::set_untagged(false);
    begun.unwrap();

    let c0 = walpin::counters();
    for i in 0..1_100 {
        trunk
            .execute(format!("UPDATE t SET v = 'fill-{i}' WHERE id = 1"))
            .unwrap();
    }
    trunk.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let c1 = walpin::counters();
    pager.end_read_tx();
    drop(reader);
    drop(trunk);
    walpin::set_fixes(fw1, fw2, fw3);

    assert!(
        c1.fw2_switches > c0.fw2_switches,
        "the trunk switched files while the reader held its snapshot: {c0:?} -> {c1:?}"
    );
    Refusals {
        refused: c1.fw2_ckpt_refused - c0.fw2_ckpt_refused,
        init: c1.fw2_refused_init - c0.fw2_refused_init,
        fork: c1.fw2_refused_fork - c0.fw2_refused_fork,
        trunk: c1.fw2_refused_trunk - c0.fw2_refused_trunk,
        init_only: c1.fw2_refused_init_only - c0.fw2_refused_init_only,
    }
}

/// (a) A reader tagged INIT (the trunk read of every branch connection) blocks every refusal, and
/// nothing else does: the writer's own read transaction never blocks its checkpoints here.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_an_init_read_are_counted_as_init() {
    let r = refusals_blocked_by(Some(origin::INIT), false);
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (r.refused, r.refused, 0, 0),
        "{r:?}"
    );
}

/// (b) The same reader untagged is a TRUNK reader.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_an_untagged_read_are_counted_as_trunk() {
    let r = refusals_blocked_by(None, false);
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (0, 0, 0, r.refused),
        "{r:?}"
    );
}

/// (c) The mutant drops the tag: (a)'s schedule must then count TRUNK and no INIT, or the counters
/// would not be reading the tag.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusal_attribution_follows_the_tag_not_the_schedule() {
    let r = refusals_blocked_by(Some(origin::INIT), true);
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!((r.init, r.init_only, r.trunk), (0, 0, r.refused), "{r:?}");
}

/// (d) A reader tagged FORK is counted as FORK alone.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_a_fork_read_are_counted_as_fork() {
    let r = refusals_blocked_by(Some(origin::FORK), false);
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (0, 0, r.refused, 0),
        "{r:?}"
    );
}

/// The tag is scoped: restored after `with`, also when the closure unwinds.
#[test]
fn the_origin_tag_is_restored_after_with() {
    assert_eq!(origin::current(), origin::TRUNK);
    origin::with(origin::FORK, || {
        assert_eq!(origin::current(), origin::FORK);
        origin::with(origin::INIT, || assert_eq!(origin::current(), origin::INIT));
        assert_eq!(origin::current(), origin::FORK);
    });
    assert_eq!(origin::current(), origin::TRUNK);
    let unwound = std::panic::catch_unwind(|| {
        origin::with(origin::INIT, || -> u8 { panic!("unwind") })
    });
    assert!(unwound.is_err());
    assert_eq!(origin::current(), origin::TRUNK);
}
