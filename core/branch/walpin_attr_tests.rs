//! r11-walpin-conc amendment 28: the fire-check of FW2's refusal attribution (`walpin::origin`).
//!
//! Trunk connections hold read transactions begun on a fully checkpointed WAL, so their snapshots
//! end in the current wal2 file. The trunk then fills that file (a switch) and checkpoints: every
//! refusal from then on is a checkpoint of the file those snapshots end in. The counters must name
//! the tags the readers began under, and a mutant that drops the tag must move them all to TRUNK,
//! so they read the tag and not the schedule. A second test pins the production tags to their call
//! sites: a trunk fork begins its read under FORK, a branch connection its `_init` read under INIT.
//! Counts are deltas of the process-global counters; run with `--test-threads=1 --include-ignored`.

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
    underflow: u64,
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

/// The refusals counted while one reader per entry of `tags` (None: no tag) holds its snapshot
/// across a switch and the checkpoints after it; `untagged` switches the mutant on while they begin.
fn refusals_blocked_by(tags: &[Option<u8>], untagged: bool) -> Refusals {
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

    let readers: Vec<_> = tags.iter().map(|_| db.connect().unwrap()).collect();
    let pagers: Vec<_> = readers.iter().map(|r| r.pager.load().clone()).collect();
    origin::mutant::set_untagged(untagged);
    let begun: Vec<_> = tags
        .iter()
        .zip(&pagers)
        .map(|(tag, pager)| match tag {
            Some(tag) => origin::with(*tag, || pager.begin_read_tx()),
            None => pager.begin_read_tx(),
        })
        .collect();
    origin::mutant::set_untagged(false);
    for b in begun {
        b.unwrap();
    }

    let c0 = walpin::counters();
    for i in 0..1_100 {
        trunk
            .execute(format!("UPDATE t SET v = 'fill-{i}' WHERE id = 1"))
            .unwrap();
    }
    trunk.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    for pager in &pagers {
        pager.end_read_tx();
    }
    let c1 = walpin::counters();
    drop(pagers);
    drop(readers);
    drop(trunk);
    walpin::set_fixes(fw1, fw2, fw3);

    assert!(
        c1.fw2_switches > c0.fw2_switches,
        "the trunk switched files while the readers held their snapshots: {c0:?} -> {c1:?}"
    );
    Refusals {
        refused: c1.fw2_ckpt_refused - c0.fw2_ckpt_refused,
        init: c1.fw2_refused_init - c0.fw2_refused_init,
        fork: c1.fw2_refused_fork - c0.fw2_refused_fork,
        trunk: c1.fw2_refused_trunk - c0.fw2_refused_trunk,
        init_only: c1.fw2_refused_init_only - c0.fw2_refused_init_only,
        underflow: c1.fw2_origin_underflow - c0.fw2_origin_underflow,
    }
}

fn assert_refused(r: &Refusals) {
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(r.underflow, 0, "every read ended under the origin it began under: {r:?}");
}

/// (a) A reader tagged INIT (the trunk read of every branch connection) blocks every refusal, and
/// nothing else does: the writer's own read transaction never blocks its checkpoints here.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_an_init_read_are_counted_as_init() {
    let r = refusals_blocked_by(&[Some(origin::INIT)], false);
    assert_refused(&r);
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
    let r = refusals_blocked_by(&[None], false);
    assert_refused(&r);
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
    let r = refusals_blocked_by(&[Some(origin::INIT)], true);
    assert_refused(&r);
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (0, 0, 0, r.refused),
        "{r:?}"
    );
}

/// (d) A reader tagged FORK is counted as FORK alone.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_a_fork_read_are_counted_as_fork() {
    let r = refusals_blocked_by(&[Some(origin::FORK)], false);
    assert_refused(&r);
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (0, 0, r.refused, 0),
        "{r:?}"
    );
}

/// (e) INIT and TRUNK readers together: both are counted in every refusal, and none is INIT-only.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_a_refusal_with_mixed_blockers_is_not_init_only() {
    let r = refusals_blocked_by(&[Some(origin::INIT), None], false);
    assert_refused(&r);
    assert_eq!(
        (r.init, r.init_only, r.fork, r.trunk),
        (r.refused, 0, 0, r.refused),
        "{r:?}"
    );
}

/// (g) The underflow detector fires: a read begun under INIT and ended under TRUNK is one counted
/// underflow (never an abort), the reader classes still balance, and INIT keeps the leaked count,
/// which is why an underflow anywhere in a run voids that run's attribution. Ended under the tag it
/// began under, the same read counts nothing.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_an_origin_mismatch_is_counted_not_asserted() {
    let (fw1, fw2, fw3) = (walpin::fw1(), walpin::fw2(), walpin::fw3());
    walpin::set_fixes(true, true, false);
    let mismatched = crate::storage::wal::walpin_origin_mismatch_probe(origin::INIT, origin::TRUNK);
    let matched = crate::storage::wal::walpin_origin_mismatch_probe(origin::INIT, origin::INIT);
    walpin::set_fixes(fw1, fw2, fw3);
    assert_eq!(mismatched, (1, [0; 4], 1), "begun INIT, ended TRUNK");
    assert_eq!(matched, (0, [0; 4], 0), "begun and ended INIT");
}

/// The tags the calling thread's read transactions began under during `f`.
fn tags_during(f: impl FnOnce()) -> Vec<u8> {
    origin::recorder::start();
    f();
    origin::recorder::take()
}

/// (f) The production call sites: a trunk fork begins every read under FORK, and a branch
/// connection begins its `_init` read under INIT; under the mutant neither tag appears.
#[test]
fn the_production_read_paths_carry_their_tags() {
    for untagged in [false, true] {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        trunk.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
        origin::mutant::set_untagged(untagged);
        let mut branch = None;
        let forked = tags_during(|| branch = Some(trunk.fork_branch().unwrap()));
        let branch = branch.unwrap();
        let connected = tags_during(|| drop(branch.connect().unwrap()));
        origin::mutant::set_untagged(false);
        if untagged {
            assert!(
                !forked.contains(&origin::FORK) && !connected.contains(&origin::INIT),
                "mutant: fork {forked:?}, connect {connected:?}"
            );
        } else {
            assert!(
                !forked.is_empty() && forked.iter().all(|&t| t == origin::FORK),
                "fork {forked:?}"
            );
            assert!(connected.contains(&origin::INIT), "connect {connected:?}");
            assert!(!connected.contains(&origin::FORK), "connect {connected:?}");
        }
        drop(branch);
    }
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
