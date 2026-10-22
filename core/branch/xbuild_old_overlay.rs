//! h1-rev-72bfb0127 HIGH-1: a REAL older build's sessions, for `links_sessions_tests`' cross-build test
//! (`a_crash_after_a_non_flip_cut_then_a_real_older_builds_session_reopens_with_every_link_right`).
//! NOT compiled in this build (no `mod` names it). The run's script copies it onto 6619b0312, the last
//! build that writes format 19 (LEAP L4 slice 2; 21 came at ba7976aef), as `core/branch/xbuild_old_tests.rs`
//! with `#[cfg(test)] mod xbuild_old_tests;` appended to its `core/branch/mod.rs`: a test module only, no
//! line of that build's code changed. One phase per run, by `FE_XB_PHASE`, on the store at `FE_XB_PATH`:
//! - `make`: 1,000 trunk forks and a child of each of the first 50, checkpointed (a 19 store as that build
//!   makes one); the fork ids written to `<path>.ids`, one per line.
//! - `session`: reap forks 60-99 (every third) and fork a child of each of the first 30, checkpointed: rows
//!   behind any fill cursor past fork 130 changed by a writer that knows nothing of the links.

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

#[test]
fn xb_old_child() {
    let (Ok(phase), Ok(p)) = (std::env::var("FE_XB_PHASE"), std::env::var("FE_XB_PATH")) else {
        return;
    };
    let path = std::path::PathBuf::from(p);
    let ids_file = format!("{}.ids", path.display());
    match phase.as_str() {
        "make" => {
            let db = open(&path);
            let trunk = db.connect().unwrap();
            trunk.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
            let ids: Vec<BranchId> = (0..1_000).map(|_| trunk.fork_branch().unwrap().into_id()).collect();
            for id in ids.iter().take(50) {
                let b = db.branch(*id).unwrap();
                let _ = b.fork().unwrap().into_id();
                let _ = b.into_id();
            }
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            let text: Vec<String> = ids.iter().map(|i| i.0.to_string()).collect();
            std::fs::write(&ids_file, text.join("\n")).unwrap();
            println!("XB-OLD phase=make ids={}", ids.len());
        }
        "session" => {
            let ids: Vec<BranchId> =
                std::fs::read_to_string(&ids_file).unwrap().lines().map(|l| BranchId(l.parse().unwrap())).collect();
            let db = open(&path);
            db.branch_wait_name_filter();
            for id in ids.iter().skip(60).take(40).step_by(3) {
                db.branch(*id).unwrap().reap().unwrap();
            }
            for id in ids.iter().take(30) {
                let b = db.branch(*id).unwrap();
                let _ = b.fork().unwrap().into_id();
                let _ = b.into_id();
            }
            db.branch_compact_now().unwrap();
            db.branch_checkpoint_wait();
            println!("XB-OLD phase=session ids={}", ids.len());
        }
        other => panic!("FE_XB_PHASE {other}: not a phase"),
    }
}
