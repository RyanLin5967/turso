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
/// often each fact was set at a check: (pages complete, all complete, free exhausted).
fn history(seed: u64, steps: u32, legacy: bool) -> (u64, u64, u64, u64, u64, u64) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("oracle.db");
    let mut db = open(&path);
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
    let want_format = if legacy { 17 } else { 19 };
    let mut live: Vec<String> = vec!["first".to_string()];
    let mut ids: Vec<BranchId> = Vec::new();
    // Connections on unnamed branches whose handle was dropped: released while held open.
    let mut held: Vec<Arc<crate::Connection>> = Vec::new();
    let mut made = 0u64;
    let mut seen = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
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
        } else if op < 94 {
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            "checkpoint"
        } else {
            held.clear();
            drop(trunk);
            let incarnation = db.incarnation;
            drop(db);
            db = open(&path);
            assert_ne!(db.incarnation, incarnation, "seed {seed} step {step}: premise: not a reopen");
            // The name mirror is built with the filter, off the mutex, after the open.
            db.branch_wait_name_filter();
            seen.5 += db.branches.free_inferred_for_test();
            trunk = db.connect().unwrap();
            "reopen"
        };
        assert_eq!(
            db.branches.format_for_test(),
            Some(want_format),
            "seed {seed} step {step} ({what}): the store writes another format than it was made at"
        );
        match db.branches.l4_facts_check_for_test() {
            Ok((pages, all, exhausted, meta)) => {
                seen.4 += u64::from(db.branches.name_mirror_built_for_test());
                seen.0 += u64::from(pages > 0);
                seen.1 += u64::from(all);
                seen.2 += u64::from(exhausted);
                seen.3 += u64::from(meta);
            }
            Err(e) => panic!("seed {seed} step {step} ({what}): an L4 fact disagrees with the catalog: {e}"),
        }
    }
    super::store::CHECK_FREE_INFERENCE.with(|c| c.set(false));
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
#[test]
fn every_l4_fact_equals_the_catalog_query_it_replaces() {
    let mut total = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    for seed in [1u64, 7, 42, 1009, 65_537, 0xdead_beef] {
        let seen = history(seed, 300, false);
        total = (
            total.0 + seen.0,
            total.1 + seen.1,
            total.2 + seen.2,
            total.3 + seen.3,
            total.4 + seen.4,
            total.5 + seen.5,
        );
    }
    // LEAP L4 slice 2: a store made at the legacy format 17 opens by the older path and stays 17.
    for seed in [3u64, 99] {
        history(seed, 200, true);
    }
    assert!(
        total.0 > 0 && total.1 > 0 && total.2 > 0 && total.3 > 0 && total.4 > 0 && total.5 > 0,
        "premise: every fact was set at some check (pages complete {}, all complete {}, free exhausted {}, a lease or held release in the meta row {}, the name mirror built {}, slots whose checkpoint state an open inferred {})",
        total.0,
        total.1,
        total.2,
        total.3,
        total.4,
        total.5
    );
}
