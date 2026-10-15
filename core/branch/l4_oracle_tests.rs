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
fn history(seed: u64, steps: u32) -> (u64, u64, u64) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("oracle.db");
    let mut db = open(&path);
    let mut trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for id in 1..=40 {
        trunk.execute(&format!("INSERT INTO t VALUES ({id}, '{}')", "x".repeat(200))).unwrap();
    }
    let mut rng = Rng(seed | 1);
    let mut live: Vec<String> = Vec::new();
    let mut made = 0u64;
    let mut seen = (0u64, 0u64, 0u64);
    for step in 0..steps {
        let op = rng.below(100);
        let what = if op < 30 {
            let name = format!("b{made}");
            made += 1;
            trunk.create_branch(&name).unwrap();
            live.push(name);
            "create"
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
            drop(trunk);
            let incarnation = db.incarnation;
            drop(db);
            db = open(&path);
            assert_ne!(db.incarnation, incarnation, "seed {seed} step {step}: premise: not a reopen");
            trunk = db.connect().unwrap();
            "reopen"
        };
        match db.branches.l4_facts_check_for_test() {
            Ok((pages, all, exhausted)) => {
                seen.0 += u64::from(pages > 0);
                seen.1 += u64::from(all);
                seen.2 += u64::from(exhausted);
            }
            Err(e) => panic!("seed {seed} step {step} ({what}): an L4 fact disagrees with the catalog: {e}"),
        }
    }
    seen
}

/// The facts equal the queries they replace at every step of 6 seeded histories of 300 operations,
/// and each fact was set at some step (an oracle over facts never set checks nothing). Plants (test
/// builds only): `l4_complete_over_a_row` marks a page complete although the catalog holds a
/// version the memory does not; `l4_free_ignores_puts` reads the free table empty although the
/// checkpoint wrote slots to it. Each must fail this test.
#[test]
fn every_l4_fact_equals_the_catalog_query_it_replaces() {
    let mut total = (0u64, 0u64, 0u64);
    for seed in [1u64, 7, 42, 1009, 65_537, 0xdead_beef] {
        let seen = history(seed, 300);
        total = (total.0 + seen.0, total.1 + seen.1, total.2 + seen.2);
    }
    assert!(
        total.0 > 0 && total.1 > 0 && total.2 > 0,
        "premise: every fact was set at some check (pages complete {}, all complete {}, free exhausted {})",
        total.0,
        total.1,
        total.2
    );
}
