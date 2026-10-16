//! Links adversary M1 (DECISIONS 7e77068514), in the suite: randomized NESTED histories (children
//! of branches, so a fresh child's parent `child_max` path, the `child_max` relink, splices and
//! nested deletes all run), each delete's known neighbours cross-checked against the catalog query
//! path it replaces (`store::links_crosscheck`), every live branch's rows checked against a model,
//! and the L4 oracle at every step. Four arms: splice on/off x the legacy stand-in on/off. The
//! history is the adversary's (`links_adv_tests.rs`), its counters made per thread.
//! Plants: `l4_known_drops_hi` (the known path drops a row's upper link) and the existing
//! `l4_stale_sibling_link` must fail it.

use super::*;
use crate::{Database, OpenFlags, PlatformIO, SqliteDialect, Value, IO};
use std::collections::HashMap;
use std::sync::Arc;

fn open(path: &std::path::Path, splice: bool) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let ckpt = if splice { BranchCheckpoint::Sharp } else { BranchCheckpoint::Fuzzy };
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        crate::DatabaseOpts::new()
            .with_branch_durability(BranchDurability::Catalog { sync: SyncClass::Off })
            .with_branch_splice(splice)
            .with_branch_checkpoint(ckpt),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn rows(conn: &Arc<crate::Connection>) -> Vec<String> {
    conn.prepare("SELECT v FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap()
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

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

const N: usize = 30;

/// One nested history. Returns the deletes released.
fn history(seed: u64, steps: u32, splice: bool, legacy: bool) -> u64 {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("adv.db");
    let mut db = open(&path, splice);
    let mut trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for id in 1..=N {
        trunk.execute(format!("INSERT INTO t VALUES ({id}, 'init')")).unwrap();
    }
    let mut trunk_model: Vec<String> = vec!["init".to_string(); N];
    let mut model: HashMap<BranchId, Vec<String>> = HashMap::new();
    let mut live: Vec<BranchId> = Vec::new();
    let mut rng = Rng(seed | 1);
    super::journal::CREATE_LEGACY.with(|c| c.set(legacy));
    let first = trunk.fork_branch().unwrap().into_id();
    model.insert(first, trunk_model.clone());
    live.push(first);
    let mut releases = 0u64;
    let mut legacy_steps = if legacy { 40 } else { 0 };
    macro_rules! op {
        ($k:expr, $step:expr) => {{
            let k: u64 = $k;
            if k < 20 {
                let c = trunk.fork_branch().unwrap().into_id();
                model.insert(c, trunk_model.clone());
                live.push(c);
            } else if k < 45 && !live.is_empty() {
                let p = live[rng.below(live.len() as u64) as usize];
                let pb = db.branch(p).unwrap();
                let c = pb.fork().unwrap().into_id();
                let _ = pb.into_id();
                let m = model[&p].clone();
                model.insert(c, m);
                live.push(c);
            } else if k < 65 && !live.is_empty() {
                let i = rng.below(live.len() as u64) as usize;
                let id = live.swap_remove(i);
                model.remove(&id);
                drop(db.branch(id).unwrap());
                releases += 1;
            } else if k < 75 {
                let r = 1 + rng.below(N as u64) as usize;
                let v = format!("t{}", $step);
                trunk.execute(format!("UPDATE t SET v = '{v}' WHERE id = {r}")).unwrap();
                trunk_model[r - 1] = v;
            } else if !live.is_empty() {
                let id = live[rng.below(live.len() as u64) as usize];
                let r = 1 + rng.below(N as u64) as usize;
                let v = format!("b{}-{}", id.0, $step);
                let b = db.branch(id).unwrap();
                b.connect().unwrap().execute(format!("UPDATE t SET v = '{v}' WHERE id = {r}")).unwrap();
                let _ = b.into_id();
                model.get_mut(&id).unwrap()[r - 1] = v;
            }
        }};
    }
    for step in 0..steps {
        if legacy_steps > 0 {
            legacy_steps -= 1;
            if legacy_steps == 0 {
                // The legacy stand-in checkpoints its 17 catalog, then this build takes over.
                db.branch_compact_now().unwrap();
                db.branch_checkpoint_wait();
                super::journal::CREATE_LEGACY.with(|c| c.set(false));
            }
        }
        let k = rng.below(110);
        if k < 85 {
            op!(k, step);
        } else if k < 92 {
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
        } else if k < 101 && !splice && legacy_steps == 0 {
            let stage = if rng.below(2) == 0 { super::store::HOLD_BEFORE_COMMIT } else { super::store::HOLD_AFTER_COMMIT };
            db.branch_checkpoint_hold(stage);
            if db.branch_checkpoint_fuzzy_now().unwrap() {
                let start = std::time::Instant::now();
                while db.branch_checkpoint_held() != stage | super::store::HOLD_ARRIVED {
                    assert!(start.elapsed().as_secs() < 30, "seed {seed} step {step}: never held");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                for _ in 0..1 + rng.below(6) {
                    let k2 = rng.below(85);
                    op!(k2, step);
                }
            }
            db.branch_checkpoint_hold(0);
            db.branch_checkpoint_wait();
        } else if legacy_steps == 0 {
            drop(trunk);
            drop(db);
            db = open(&path, splice);
            db.branch_wait_name_filter();
            trunk = db.connect().unwrap();
        }
        if let Err(e) = db.branches.l4_facts_check_for_test() {
            panic!("seed {seed} step {step} (splice {splice}, legacy {legacy}): oracle: {e}");
        }
        if step % 10 == 9 {
            assert_eq!(rows(&trunk), trunk_model, "seed {seed} step {step}: trunk");
            for &id in &live {
                let b = db.branch(id).unwrap();
                let got = rows(&b.connect().unwrap());
                let _ = b.into_id();
                assert_eq!(got, model[&id], "seed {seed} step {step} (splice {splice}, legacy {legacy}): branch {} rows", id.0);
            }
        }
        if let Some(m) = super::store::links_crosscheck::FIRST.with(|f| f.borrow().clone()) {
            panic!("seed {seed} step {step} (splice {splice}, legacy {legacy}): crosscheck: {m}");
        }
    }
    releases
}

#[test]
fn nested_histories_known_links_equal_the_query_path() {
    use super::store::links_crosscheck as x;
    x::ON.with(|on| on.set(true));
    let arms: &[(bool, bool)] = &[(false, false), (true, false), (false, true), (true, true)];
    let mut releases = 0;
    for &(splice, legacy) in arms {
        for seed in [3u64, 11, 99, 4242] {
            releases += history(seed, 300, splice, legacy);
        }
    }
    x::ON.with(|on| on.set(false));
    super::journal::CREATE_LEGACY.with(|c| c.set(false));
    let (checks, branch_parent) = x::CHECKS.with(std::cell::Cell::get);
    let mism = x::MISMATCHES.with(std::cell::Cell::get);
    println!("NESTED: releases {releases}, known-path deletes {checks}, of a branch's child {branch_parent}, mismatches {mism}");
    assert!(checks > 0 && branch_parent > 0, "premise: the known path ran, for a branch's child too");
    assert_eq!(mism, 0, "{:?}", x::FIRST.with(|f| f.borrow().clone()));
}
