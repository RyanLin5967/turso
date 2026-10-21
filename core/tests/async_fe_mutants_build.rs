//! engine-2bdcd32c6 MED 4: HIGH F's refusal (`BranchStore::async_refusal`) refused every build
//! without the lib's cfg(test), so AD-T5's fire-check binary (`--features turso_core/fe-mutants`,
//! PREREG-v1 amendment 2) could make no async create: every async_s0 arm was VOID at its warm-up
//! and the strict arm at N = 2 never ran. In that build, which also sets
//! `RUSTFLAGS="--cfg fe_ad_t5_firecheck"` (async_s0_firecheck.py; never a cargo feature, which
//! `--all-features` would turn on: lead ruling, artie DECISIONS 66b4671687), an async create is
//! accepted. This file is empty without both; the production half, refused, is async_refusal.rs,
//! unchanged.
#![cfg(all(feature = "fe-mutants", fe_ad_t5_firecheck))]

use std::sync::Arc;
use turso_core::branch::async_api::fe_mutants_compiled;
use turso_core::branch::{BranchDurability, SyncClass};
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

#[test]
fn an_async_create_is_accepted_in_the_fire_check_build() {
    assert!(fe_mutants_compiled(), "premise: this binary carries fe-mutants");
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("firecheck.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let class = if cfg!(target_vendor = "apple") { SyncClass::FullFsync } else { SyncClass::Fsync };
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_branch_durability(BranchDurability::Catalog { sync: class }),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    let got = trunk.create_branch_async("accepted");
    assert!(got.is_ok(), "CLAIM: the fire-check build accepts an async create: {:?}", got.as_ref().err());
    db.branch_wait_durable(got.unwrap().token).unwrap();
    assert!(db.branch_named("accepted").unwrap().is_some(), "CLAIM: the create made its branch");
}
