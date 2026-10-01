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
    writer: u64,
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
        writer: c1.fw2_refused_writer - c0.fw2_refused_writer,
    }
}

fn assert_refused(r: &Refusals) {
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(r.underflow, 0, "every read ended under the origin it began under: {r:?}");
    assert_eq!(r.writer, 0, "no reader was tagged WRITER unless the arm tagged one: {r:?}");
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

/// (h) r11-walpin-conc amendment 33a: a reader tagged WRITER (a trunk writer's own read transaction, as the
/// harness tags it) is counted as WRITER alone, so the untagged TRUNK bucket counts only readers nobody tagged.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_refusals_blocked_by_a_writer_tagged_read_are_counted_as_writer() {
    let r = refusals_blocked_by(&[Some(origin::WRITER)], false);
    assert!(r.refused >= 1, "the checkpoints after the switch were refused: {r:?}");
    assert_eq!(r.underflow, 0, "{r:?}");
    assert_eq!(
        (r.writer, r.init, r.init_only, r.fork, r.trunk),
        (r.refused, 0, 0, 0, 0),
        "{r:?}"
    );
}

/// (i) r11-walpin-conc amendment 33a: a trunk child's required retention is one version per trunk page written since
/// its fork that existed at its fork. A rewrite in the same epoch adds nothing; a later fork leaves the older child's
/// count as it was; a page past the child's fork-time size is not required.
#[test]
fn required_pages_count_each_page_written_since_the_fork_once() {
    let store = super::store::BranchStore::new();
    let schema = || Arc::new(crate::schema::Schema::default());
    let page = vec![0u8; 4096];
    let a = store.fork_trunk_sized(schema(), 4096, 50).unwrap();
    assert_eq!(store.walpin_required_pages(a).unwrap().1, 0);
    for p in [5, 6, 7] {
        store.first_write_trunk(p, &page);
    }
    store.first_write_trunk(5, &page);
    assert_eq!(store.walpin_required_pages(a).unwrap().1, 3, "5, 6, 7: the rewrite of 5 adds nothing");
    let b = store.fork_trunk_sized(schema(), 4096, 50).unwrap();
    store.first_write_trunk(5, &page);
    store.first_write_trunk(8, &page);
    store.first_write_trunk(60, &page);
    assert_eq!(store.walpin_required_pages(a).unwrap().1, 4, "a: 5, 6, 7, 8 (60 is past its size)");
    assert_eq!(store.walpin_required_pages(b).unwrap().1, 2, "b: 5 and 8 since its fork");
    assert!(store.walpin_required_pages(BranchId(999)).is_err(), "an unknown branch is refused");
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

/// (j) r11-walpin-conc amendment 33f (G9): the required retention over ALL live trunk children is one version per
/// superseded (page, version) that some live child can see. Children forked inside one version's range share it; a page
/// rewritten between two forks needs one version per child; a version only a reaped child could see stops counting; a
/// page past every fork-time size in its range counts nothing. Expected counts are worked by hand from the epochs.
#[test]
fn required_versions_count_one_per_version_some_live_child_sees() {
    let store = super::store::BranchStore::new();
    let schema = || Arc::new(crate::schema::Schema::default());
    let page = vec![0u8; 4096];
    assert_eq!(store.walpin_required_versions(), (0, 0));
    let a = store.fork_trunk_sized(schema(), 4096, 50).unwrap(); // fork epoch 0
    let b = store.fork_trunk_sized(schema(), 4096, 50).unwrap(); // fork epoch 1
    store.first_write_trunk(5, &page); // supersedes (5, 0, 2): a and b see it
    store.first_write_trunk(5, &page); // the same epoch: nothing superseded
    assert_eq!(store.walpin_required_versions(), (2, 1), "a and b share page 5's one version");
    let c = store.fork_trunk_sized(schema(), 4096, 50).unwrap(); // fork epoch 2
    store.first_write_trunk(5, &page); // supersedes (5, 2, 3): c sees it
    store.first_write_trunk(60, &page); // supersedes (60, 0, 3): past a's, b's and c's size of 50
    assert_eq!(store.walpin_required_versions(), (3, 2), "page 5: one version for a and b, one for c");
    let d = store.fork_trunk_sized(schema(), 4096, 50).unwrap(); // fork epoch 3
    store.first_write_trunk(5, &page); // supersedes (5, 3, 4): d sees it
    assert_eq!(store.walpin_required_versions(), (4, 3), "one version of page 5 per rewrite with a live child");
    store.release_handle(c);
    assert_eq!(store.walpin_required_versions(), (3, 2), "(5, 2, 3) was c's alone");
    store.release_handle(a);
    assert_eq!(store.walpin_required_versions(), (2, 2), "b still sees (5, 0, 2)");
    store.release_handle(b);
    store.release_handle(d);
    assert_eq!(store.walpin_required_versions(), (0, 0));
}

/// (k) r11-walpin-conc amendment 33f (G6): the harness's own write path, `with_writer_tag` around a trunk UPDATE,
/// begins every read transaction under WRITER; under the mutant that drops the tag, none is WRITER.
#[test]
fn a_writer_tagged_trunk_write_begins_every_read_under_writer() {
    for untagged in [false, true] {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        trunk.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
        origin::mutant::set_untagged(untagged);
        let tags = tags_during(|| {
            walpin::with_writer_tag(|| trunk.execute("UPDATE t SET v = 'w' WHERE id = 1")).unwrap();
        });
        origin::mutant::set_untagged(false);
        if untagged {
            assert!(!tags.is_empty() && !tags.contains(&origin::WRITER), "mutant: {tags:?}");
        } else {
            assert!(
                !tags.is_empty() && tags.iter().all(|&t| t == origin::WRITER),
                "write: {tags:?}"
            );
        }
    }
}

/// (l) r11-walpin-conc amendment 33f (G6): FW2 counts each read transaction it registers under the origin it began
/// under: a WRITER-tagged trunk UPDATE moves only the WRITER count, an untagged one only TRUNK, and the mutant moves
/// the tagged write to TRUNK.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_begins_are_counted_by_origin() {
    let (fw1, fw2, fw3) = (walpin::fw1(), walpin::fw2(), walpin::fw3());
    walpin::set_fixes(true, true, false);
    let (_dir, db) = open_db();
    db.walpin_open_wal2().unwrap();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    let begins = |f: &dyn Fn()| -> [u64; 4] {
        let c0 = walpin::counters().fw2_begins;
        f();
        let c1 = walpin::counters().fw2_begins;
        std::array::from_fn(|i| c1[i] - c0[i])
    };
    let tagged = begins(&|| {
        walpin::with_writer_tag(|| trunk.execute("UPDATE t SET v = 'w' WHERE id = 1")).unwrap();
    });
    let untagged = begins(&|| {
        trunk.execute("UPDATE t SET v = 'u' WHERE id = 1").unwrap();
    });
    origin::mutant::set_untagged(true);
    let mutant = begins(&|| {
        walpin::with_writer_tag(|| trunk.execute("UPDATE t SET v = 'm' WHERE id = 1")).unwrap();
    });
    origin::mutant::set_untagged(false);
    drop(trunk);
    walpin::set_fixes(fw1, fw2, fw3);
    let (t, w) = (usize::from(origin::TRUNK), usize::from(origin::WRITER));
    assert!(tagged[w] >= 1 && tagged.iter().sum::<u64>() == tagged[w], "tagged: {tagged:?}");
    assert!(untagged[t] >= 1 && untagged.iter().sum::<u64>() == untagged[t], "untagged: {untagged:?}");
    assert!(mutant[t] >= 1 && mutant[w] == 0, "mutant: {mutant:?}");
}

/// (m) r11-walpin-conc amendment 33f (F9): `walpin_stats` reports FW2's open readers by class and by origin, and
/// reading it changes nothing: two reads agree, no process counter moves, and the reader's end empties both.
#[test]
#[ignore = "sets the process-global FW2 switch: run alone with --test-threads=1 --include-ignored"]
fn fw2_reader_stats_report_open_readers_and_change_nothing() {
    let (fw1, fw2, fw3) = (walpin::fw1(), walpin::fw2(), walpin::fw3());
    walpin::set_fixes(true, true, false);
    let (_dir, db) = open_db();
    db.walpin_open_wal2().unwrap();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let reader = db.connect().unwrap();
    let pager = reader.pager.load().clone();
    let idle = db.walpin_stats();
    origin::with(origin::FORK, || pager.begin_read_tx()).unwrap();
    let c0 = walpin::counters();
    let open = db.walpin_stats();
    let again = db.walpin_stats();
    let c1 = walpin::counters();
    pager.end_read_tx();
    let closed = db.walpin_stats();
    drop(pager);
    drop(reader);
    drop(trunk);
    walpin::set_fixes(fw1, fw2, fw3);
    assert_eq!(idle.wal2_readers, [0; 4], "{idle:?}");
    assert_eq!(open.wal2_readers.iter().sum::<u32>(), 1, "{open:?}");
    assert_eq!(
        open.wal2_origin_readers[usize::from(origin::FORK)],
        open.wal2_readers,
        "the one reader is FORK's: {open:?}"
    );
    assert_eq!(open.wal2_origin_readers.iter().flatten().sum::<u32>(), 1, "{open:?}");
    assert_eq!(open, again, "a second read sees the same state");
    assert_eq!(c0, c1, "reading the stats moved no counter");
    assert_eq!(
        (closed.wal2_readers, closed.wal2_origin_readers),
        ([0; 4], [[0; 4]; 4]),
        "{closed:?}"
    );
}
