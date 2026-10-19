//! engine-rev-qaf-fix2 HIGH F (REVIEW-engine-qaf-fix2-18556a956): an async create can lose its id in
//! a crash and, until the id floor lands (ASYNC-DESIGN §9, the registered red
//! an_id_lost_in_a_crash_is_never_reissued), have it issued again to another branch. So outside the
//! lib's own tests an async create is refused: a non-test build cannot reach that state. This is an
//! integration test, built without the lib's cfg(test), as every user of the crate is.

use std::sync::Arc;
use turso_core::branch::{BranchDurability, SyncClass};
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

#[test]
fn an_async_create_is_refused_until_the_id_floor_lands() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("refuse.db");
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
    let got = trunk.create_branch_async("refused");
    assert!(got.is_err(), "CLAIM: an async create is refused outside the lib's tests: {:?}", got.map(|c| c.id));
    assert!(db.branch_named("refused").unwrap().is_none(), "CLAIM: nothing was created");
}
