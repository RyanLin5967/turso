//! LEAP L4's oracle (lead, fastest DECISIONS 2026-10-15): every in-memory fact that lets the store
//! skip a catalog query must equal that query, at every step of a randomized lifecycle history. The
//! facts and their checks are `BranchStore::l4_facts_check_for_test`; this drives the history: named
//! creates, trunk writes that retain versions for live children, branch writes, deletes, catalog
//! checkpoints and reopens, on a catalog store, seeded so a failure replays.

use super::*;
use crate::{Database, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::sync::Arc;

fn open(path: &std::path::Path) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync: SyncClass::Off })
            .with_branch_checkpoint(BranchCheckpoint::Fuzzy),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

/// Spin until `f` holds, for at most 30 s.
fn until(what: &str, mut f: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(start.elapsed() < std::time::Duration::from_secs(30), "timed out waiting: {what}");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// What a history counts at its checks (each must be set at some check: an oracle over facts never
/// set checks nothing).
#[derive(Default, Debug, Clone, Copy)]
struct Seen {
    pages_complete: u64,
    all_complete: u64,
    free_exhausted: u64,
    meta_facts: u64,
    mirror_built: u64,
    free_inferred: u64,
    /// Review L4 M1: steps run with a fuzzy checkpoint held in flight, and reopens whose name scan
    /// was held across a checkpoint (its mirror discarded).
    in_flight: u64,
    mirror_held: u64,
    /// Review L4 M2: trims of the trunk-version cache past the resident cap.
    trims: u64,
    /// Operations run under a held flight, and (mutant `l4_child_facts_in_flight` only) slice 3's
    /// facts used in a flight.
    flight_ops: u64,
    facts_in_flight: u64,
    /// Review L4 M1: fact use sites reached under a held flight, each asserting the facts equal
    /// their queries (counted for the last store of a history).
    site_checks: u64,
    /// LEAP L4 (the MCTS deletes): resident branches with known links, summed over the checks; and
    /// checks of a legacy store before its first checkpoint, at the version it was made at.
    links_known: u64,
    read_at_made: u64,
}

impl Seen {
    fn add(&mut self, o: Seen) {
        self.pages_complete += o.pages_complete;
        self.all_complete += o.all_complete;
        self.free_exhausted += o.free_exhausted;
        self.meta_facts += o.meta_facts;
        self.mirror_built += o.mirror_built;
        self.free_inferred += o.free_inferred;
        self.in_flight += o.in_flight;
        self.mirror_held += o.mirror_held;
        self.trims += o.trims;
        self.flight_ops += o.flight_ops;
        self.facts_in_flight += o.facts_in_flight;
        self.site_checks += o.site_checks;
        self.links_known += o.links_known;
        self.read_at_made += o.read_at_made;
    }
}

/// xorshift64*: a deterministic history from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One history of `steps` operations from `seed`; the facts checked after every one. Returns how
/// often each fact was set at a check. `cap`: the store's resident cap, set at every open (review
/// L4 M2: the trunk-version cache is trimmed past it).
fn history(seed: u64, steps: u32, legacy: bool, cap: Option<usize>) -> Seen {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("oracle.db");
    let mut db = open(&path);
    db.branch_set_resident_cap(cap);
    let mut trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for id in 1..=40 {
        trunk.execute(&format!("INSERT INTO t VALUES ({id}, '{}')", "x".repeat(200))).unwrap();
    }
    let mut rng = Rng(seed | 1);
    // LEAP L4 2b: every open on this thread also checks its first-touch inference against the free
    // table (the disagreement is reported by the facts check below).
    super::store::CHECK_FREE_INFERENCE.with(|c| c.set(true));
    // The store (and its catalog) is made at the first fork: at the legacy version 17 when asked,
    // as an older build made it; every later step runs this build's own path over it.
    super::journal::CREATE_LEGACY.with(|c| c.set(legacy));
    trunk.create_branch("first").unwrap();
    super::journal::CREATE_LEGACY.with(|c| c.set(false));
    // One write path (DECISIONS e977c1d010): a store is read at the version it was made at (17 for
    // a legacy one) until its first checkpoint, which upgrades it; from then on it is 21.
    let made_at = if legacy { 17 } else { 21 };
    let mut checkpointed = false;
    let mut live: Vec<String> = vec!["first".to_string()];
    let mut ids: Vec<BranchId> = Vec::new();
    // Connections on unnamed branches whose handle was dropped: released while held open.
    let mut held: Vec<Arc<crate::Connection>> = Vec::new();
    let mut made = 0u64;
    let mut seen = Seen::default();
    for step in 0..steps {
        let op = rng.below(100);
        let what = if op < 24 {
            let name = format!("b{made}");
            made += 1;
            ids.push(trunk.create_branch(&name).unwrap());
            live.push(name);
            "create"
        } else if op < 30 && !ids.is_empty() {
            // A lease on a named branch (its deadline in the catalog's lease index); one dropped
            // since is refused by `branch`, and skipped.
            let id = ids[rng.below(ids.len() as u64) as usize];
            if let Ok(b) = db.branch(id) {
                b.lease(std::time::Duration::from_secs(3_600 + rng.below(10_000))).unwrap();
                b.into_id();
            }
            "lease"
        } else if op < 35 {
            // An unnamed branch released while a connection holds it open.
            let b = trunk.fork_branch().unwrap();
            held.push(b.connect().unwrap());
            drop(b);
            "release held open"
        } else if op < 38 && !held.is_empty() {
            held.swap_remove(rng.below(held.len() as u64) as usize);
            "close a held branch"
        } else if op < 50 {
            let id = 1 + rng.below(40);
            trunk.execute(&format!("UPDATE t SET v = 'trunk-{step}' WHERE id = {id}")).unwrap();
            "trunk write"
        } else if op < 70 && !live.is_empty() {
            let name = live[rng.below(live.len() as u64) as usize].clone();
            let c = db.connect_named(&name).unwrap();
            let id = 1 + rng.below(40);
            c.execute(&format!("UPDATE t SET v = 'branch-{step}' WHERE id = {id}")).unwrap();
            "branch write"
        } else if op < 82 && !live.is_empty() {
            let name = live.swap_remove(rng.below(live.len() as u64) as usize);
            db.drop_branch(&name).unwrap();
            "delete"
        } else if op < 88 {
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            "checkpoint"
        } else if op < 94 {
            // Review L4 M1: a fuzzy checkpoint held in flight (before or after its commit), with creates,
            // deletes and named connects (and writes) under it; checked once it has landed.
            // Held before its commit, or after it and before its install.
            let stage = if rng.below(2) == 0 { super::store::HOLD_BEFORE_COMMIT } else { super::store::HOLD_AFTER_COMMIT };
            db.branch_checkpoint_hold(stage);
            if db.branch_checkpoint_fuzzy_now().unwrap() {
                until(&format!("seed {seed} step {step}: the flight never held at {stage}"), || {
                    db.branch_checkpoint_held() == stage | super::store::HOLD_ARRIVED
                });
                seen.in_flight += 1;
                // Every fact use site the operations reach under the hold asserts the facts equal
                // their queries (`CatState::assert_facts_in_flight`).
                super::store::CHECK_FACTS_IN_FLIGHT.with(|c| c.set(true));
                for _ in 0..1 + rng.below(4) {
                    match rng.below(3) {
                        0 => {
                            let name = format!("b{made}");
                            made += 1;
                            ids.push(trunk.create_branch(&name).unwrap());
                            live.push(name);
                        }
                        1 if !live.is_empty() => {
                            let name = live.swap_remove(rng.below(live.len() as u64) as usize);
                            db.drop_branch(&name).unwrap();
                        }
                        _ if !live.is_empty() => {
                            let name = live[rng.below(live.len() as u64) as usize].clone();
                            let c = db.connect_named(&name).unwrap();
                            let id = 1 + rng.below(40);
                            c.execute(&format!("UPDATE t SET v = 'flight-{step}' WHERE id = {id}")).unwrap();
                        }
                        _ => {}
                    }
                    seen.flight_ops += 1;
                    // The facts a flight keeps the store from using, checked in the flight anyway
                    // (does the guard protect anything?).
                    if let Err(e) = db.branches.l4_facts_check_for_test() {
                        panic!("seed {seed} step {step} (in flight): an L4 fact disagrees with the catalog: {e}");
                    }
                }
                super::store::CHECK_FACTS_IN_FLIGHT.with(|c| c.set(false));
            }
            db.branch_checkpoint_hold(0);
            db.branch_checkpoint_wait();
            "checkpoint in flight"
        } else {
            held.clear();
            drop(trunk);
            let incarnation = db.incarnation;
            drop(db);
            // Review L4 M1: every other reopen holds its name scan's install across a checkpoint
            // that commits fresh names, so the scan's mirror is out of date (and discarded).
            let hold_install = rng.below(2) == 0;
            let hold = Arc::new(std::sync::atomic::AtomicU8::new(u8::from(hold_install)));
            super::store::NAME_INSTALL_HOLD.with(|h| *h.borrow_mut() = Some(hold.clone()));
            db = open(&path);
            super::store::NAME_INSTALL_HOLD.with(|h| *h.borrow_mut() = None);
            assert_ne!(db.incarnation, incarnation, "seed {seed} step {step}: premise: not a reopen");
            db.branch_set_resident_cap(cap);
            seen.free_inferred += db.branches.free_inferred_for_test();
            trunk = db.connect().unwrap();
            if hold_install {
                use std::sync::atomic::Ordering as O;
                let arrived = 1 | super::store::HOLD_ARRIVED;
                until(&format!("seed {seed} step {step}: the name scan never held"), || hold.load(O::Acquire) == arrived);
                for _ in 0..2 {
                    let name = format!("b{made}");
                    made += 1;
                    ids.push(trunk.create_branch(&name).unwrap());
                    live.push(name);
                }
                db.branch_compact_now().unwrap();
                db.branch_checkpoint_wait();
                hold.store(0, O::Release);
            }
            // The name mirror is built with the filter, off the mutex, after the open.
            db.branch_wait_name_filter();
            if hold_install {
                let (misses, _) = db.branches.mirror_misses_and_trims_for_test();
                seen.mirror_held += misses;
            }
            "reopen"
        };
        checkpointed |= what == "checkpoint";
        let (links_known, catalog_format) = db.branches.links_known_and_catalog_format_for_test();
        if checkpointed {
            // The upgrade claim: the catalog, and every file the store writes, at 21.
            assert_eq!(catalog_format, Some(21), "seed {seed} step {step} ({what}): the catalog was not upgraded by a checkpoint");
            assert_eq!(db.branches.format_for_test(), Some(21), "seed {seed} step {step} ({what}): the store writes an older format after a checkpoint");
        } else {
            // The read claim: until then the store reads what it was made at (a background
            // checkpoint may already have upgraded it).
            assert!(
                catalog_format.is_none_or(|f| f == made_at || f == 21),
                "seed {seed} step {step} ({what}): catalog format {catalog_format:?}, made at {made_at}"
            );
            seen.read_at_made += u64::from(legacy && catalog_format.is_none_or(|f| f == made_at));
        }
        seen.links_known += links_known;
        match db.branches.l4_facts_check_for_test() {
            Ok((pages, all, exhausted, meta)) => {
                seen.mirror_built += u64::from(db.branches.name_mirror_built_for_test());
                seen.pages_complete += u64::from(pages > 0);
                seen.all_complete += u64::from(all);
                seen.free_exhausted += u64::from(exhausted);
                seen.meta_facts += u64::from(meta);
            }
            Err(e) => panic!("seed {seed} step {step} ({what}): an L4 fact disagrees with the catalog: {e}"),
        }
    }
    super::store::CHECK_FREE_INFERENCE.with(|c| c.set(false));
    // Trims are counted per process lifetime of the store; the last one's count is read here.
    seen.trims += db.branches.mirror_misses_and_trims_for_test().1;
    seen.facts_in_flight += db.branches.facts_used_in_flight_for_test();
    seen.site_checks += db.branches.in_flight_fact_checks_for_test();
    seen
}

/// The facts equal the queries they replace at every step of 6 seeded histories of 300 operations,
/// and each fact was set at some step (an oracle over facts never set checks nothing). Plants (test
/// builds only), each of which must fail this test: `l4_all_complete_at_open` takes every page of a
/// reopened catalog as complete (the oracle names it at the reopen, before anything reads);
/// `l4_free_ignores_puts` reads the free table empty although the checkpoint wrote slots to it.
/// (`l4_complete_over_a_row`, a page marked complete at its probe over a version the memory lacks,
/// is refused by the store's own read first, as Corrupt.) Slice 2 (formats 19/20): the meta row's
/// lease minimum and held-release count equal their queries at every step (plant
/// `l4_stale_meta_facts`), the store keeps writing the version it was made at, and two histories
/// run over a store made at the legacy 17 (`journal::CREATE_LEGACY`), opened by the older path.
/// 2b: at every reopen, each touched slot's checkpoint state inferred from its first touch in the
/// tail equals the free table's answer (plant `l4_free_first_touch_inverted`), and some open inferred one.
/// Review L4 (DECISIONS df1a954ddb): M1, steps under a fuzzy checkpoint held in flight and reopens
/// whose name scan is held past a checkpoint (plants `l4_child_facts_in_flight`,
/// `l4_mirror_ignores_generation`); M2, two histories under a resident cap of 2 that trim the
/// trunk-version cache (plant `l4_trim_keeps_complete`); L4, the legacy histories' premise.
#[test]
fn every_l4_fact_equals_the_catalog_query_it_replaces() {
    let mut total = Seen::default();
    for seed in [1u64, 7, 42, 1009, 65_537, 0xdead_beef] {
        total.add(history(seed, 300, false, None));
    }
    let premise = |what: &str, s: &Seen| {
        assert!(
            s.pages_complete > 0 && s.all_complete > 0 && s.free_exhausted > 0 && s.meta_facts > 0 && s.mirror_built > 0,
            "premise ({what}): every fact was set at some check: {s:?}"
        );
    };
    premise("format 19", &total);
    println!("L4 oracle, format 19: {total:?}");
    assert!(
        total.free_inferred > 0 && total.in_flight > 0 && total.mirror_held > 0 && total.site_checks > 0 && total.links_known > 0,
        "premise: an open inferred a slot's state, a step ran under a held flight, a held name scan's mirror was discarded, and a fact use site was reached under a held flight: {total:?}"
    );
    // Review L4 M2: with a small resident cap the trunk-version cache is trimmed, and the facts
    // still hold after every trim.
    let mut capped = Seen::default();
    for seed in [5u64, 77] {
        capped.add(history(seed, 300, false, Some(2)));
    }
    println!("L4 oracle, resident cap 2: {capped:?}");
    assert!(capped.trims > 0, "premise: the resident cap trimmed the trunk-version cache: {capped:?}");
    // LEAP L4 slice 2: a store made at the legacy format 17 opens by the older path and stays 17;
    // review L4 L4: the same premise over it (its meta row carries no facts, so that one is not asked).
    let mut legacy = Seen::default();
    for seed in [3u64, 99] {
        legacy.add(history(seed, 200, true, None));
    }
    println!("L4 oracle, legacy 17: {legacy:?}");
    assert!(
        legacy.pages_complete > 0
            && legacy.all_complete > 0
            && legacy.free_exhausted > 0
            && legacy.mirror_built > 0
            && legacy.read_at_made > 0
            && legacy.links_known > 0,
        "premise (legacy 17): every fact was set at some check: {legacy:?}"
    );
}

/// The child process of the kill tests below: open the store at `FE_LINKS_CHILD`, delete the
/// branch `FE_LINKS_DELETE` names (if any), and run a sharp checkpoint, which the parent's
/// `FE_KILL_AT` kills at its point. Returns at once in any other test run.
#[test]
fn links_kill_child() {
    let Ok(path) = std::env::var("FE_LINKS_CHILD") else {
        return;
    };
    let db = open(std::path::Path::new(&path));
    db.branch_wait_name_filter();
    if let Ok(name) = std::env::var("FE_LINKS_DELETE") {
        db.drop_branch(&name).unwrap();
    }
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    // Not killed: the point was never reached, which the parent refuses.
    std::process::exit(3);
}

/// Run `links_kill_child` on `path`, killed at `point`'s first hit; the child's exit status.
fn kill_child_at(path: &std::path::Path, point: &str, delete: Option<&str>) -> std::process::ExitStatus {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args(["branch::l4_oracle_tests::links_kill_child", "--exact", "--test-threads=1", "--nocapture"])
        .env("FE_LINKS_CHILD", path)
        .env("FE_KILL_AT", format!("{point}:1"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    if let Some(name) = delete {
        cmd.env("FE_LINKS_DELETE", name);
    }
    cmd.status().unwrap()
}

/// The facts and the links of the store at `path`, reopened, checked against the catalog; then a
/// checkpoint, and the same checks. `what` names the case.
fn reopen_and_check(path: &std::path::Path, what: &str) -> Arc<Database> {
    let db = open(path);
    db.branch_wait_name_filter();
    if let Err(e) = db.branches.l4_facts_check_for_test() {
        panic!("{what}: after the kill, an L4 fact disagrees with the catalog: {e}");
    }
    db.branch_compact_now().unwrap();
    db.branch_checkpoint_wait();
    if let Err(e) = db.branches.l4_facts_check_for_test() {
        panic!("{what}: after the next checkpoint, an L4 fact disagrees with the catalog: {e}");
    }
    db
}

/// DECISIONS e977c1d010: a kill between a child's removal and its siblings' relink (kill point
/// `ckpt.unlinked`, inside the checkpoint writer's transaction) leaves the catalog as it was, and
/// the reopened store's links equal the catalog's rows; the next checkpoint relinks them. Three
/// named trunk children, checkpointed; the child process deletes the middle one and checkpoints.
#[cfg(unix)]
#[test]
fn a_kill_between_a_removal_and_its_siblings_relink_leaves_the_links_true() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("links.db");
    {
        let db = open(&path);
        let trunk = db.connect().unwrap();
        trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for name in ["k0", "k1", "k2"] {
            trunk.create_branch(name).unwrap();
        }
        db.branch_compact_now().unwrap();
        db.branch_checkpoint_wait();
    }
    let status = kill_child_at(&path, "ckpt.unlinked", Some("k1"));
    assert_eq!(status.code(), None, "premise: the child was killed at ckpt.unlinked ({status:?})");
    let db = reopen_and_check(&path, "killed at ckpt.unlinked");
    assert!(db.connect_named("k1").is_err(), "the delete, logged before the kill, was lost");
    for name in ["k0", "k2"] {
        db.connect_named(name).unwrap_or_else(|e| panic!("{name} is gone after the kill: {e}"));
    }
}

/// DECISIONS e977c1d010 (one write path): a 17 store (made as an older build made it,
/// `CREATE_LEGACY`) is upgraded at its first checkpoint in ONE transaction. Killed at every step
/// of it (the columns added; every row linked; committed before the install; installed), the
/// reopened store is either still 17 with no link column, or 21 with every row's links true, and
/// its next checkpoint leaves it 21 with true links.
/// FLAGGED spec change (lead ruling 2026-10-19T03:16:48Z, HIGH 1 of l4-rev-h1's re-review): the fill
/// commits the link columns in a transaction of their own first (e2a788823), so "17 with the link
/// columns" is a state every fill writes, and a kill after that commit (the new point
/// `ckpt.upgrade.colonly`, and every later one) may leave it. It is admitted ONLY with, for that same
/// state: (i) the L4 facts check passes on reopen and after the next checkpoint, (ii) that checkpoint
/// reaches 21, (iii) every branch survives (all three below, for every point). A kill BEFORE that
/// commit (`ckpt.upgrade.columns`, inside it) still admits only 17 without columns, or 21.
#[cfg(unix)]
#[test]
fn a_kill_at_every_step_of_the_upgrade_leaves_the_store_17_or_21() {
    for point in ["ckpt.upgrade.columns", "ckpt.upgrade.colonly", "ckpt.upgrade.rows", "ckpt.written", "ckpt.installed"] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("upgrade.db");
        {
            let db = open(&path);
            let trunk = db.connect().unwrap();
            trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
            super::journal::CREATE_LEGACY.with(|c| c.set(true));
            trunk.create_branch("first").unwrap();
            super::journal::CREATE_LEGACY.with(|c| c.set(false));
            for name in ["u0", "u1", "u2"] {
                trunk.create_branch(name).unwrap();
            }
            // A 17 catalog with rows: written by a checkpoint while the test still makes legacy.
            super::journal::CREATE_LEGACY.with(|c| c.set(true));
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            super::journal::CREATE_LEGACY.with(|c| c.set(false));
            assert_eq!(db.branches.links_known_and_catalog_format_for_test().1, Some(17), "{point}: premise: a 17 catalog");
        }
        let status = kill_child_at(&path, point, None);
        assert_eq!(status.code(), None, "{point}: premise: the child was killed there ({status:?})");
        let (format, links) = {
            let db = open(&path);
            db.branch_wait_name_filter();
            let format = db.branches.links_known_and_catalog_format_for_test().1;
            let links = db.branches.catalog_links_for_test();
            (format, links)
        };
        println!("{point}: after the kill the catalog is format {format:?}, link columns {links}");
        match (format, links) {
            (Some(17), false) | (Some(21), true) => {}
            (Some(17), true) if point != "ckpt.upgrade.columns" => {
                println!("{point}: 17 with the link columns (column-only commit): checked by (i)-(iii) below");
            }
            other => panic!("{point}: after the kill the catalog is {other:?} (format, link columns): neither 17 nor 21"),
        }
        // (i) the L4 facts check, on reopen and after the next checkpoint:
        let db = reopen_and_check(&path, point);
        // (ii) and (iii):
        assert_eq!(db.branches.links_known_and_catalog_format_for_test().1, Some(21), "{point}: not upgraded by the next checkpoint");
        let mut conns = Vec::new();
        for name in ["first", "u0", "u1", "u2"] {
            conns.push(db.connect_named(name).unwrap_or_else(|e| panic!("{point}: {name} is gone: {e}")));
        }
        // l4-rev-h1 MED-1 (its h1rev10/strengthen.py; FLAGGED stricter): (i) again with every branch
        // RESIDENT, so their links, read from the link columns, are compared with the catalog's rows (with
        // none resident the check reads no link content: mutant l4_upgrade_batch_links_nothing survived it).
        let known = db.branches.links_known_and_catalog_format_for_test();
        println!("{point}: resident check, links known/format {known:?}");
        if let Err(e) = db.branches.l4_facts_check_for_test() {
            panic!("{point}: with the branches resident, an L4 fact disagrees: {e}");
        }
        drop(conns);
    }
}
