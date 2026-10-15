//! The PostgreSQL time aliases (now, clock_timestamp, transaction_timestamp, statement_timestamp)
//! are registered once per Database, not on every connection (lead, 2026-10-15; the wire's L5 work:
//! each new engine connection ran `aliases::install`, 13 allocations per branch switch).

use super::*;

/// The open registers the aliases once for its database, three PostgreSQL connections on it register
/// nothing more, a plain engine connection made after the open already resolves `now()` (the aliases
/// are the Database's), and each PgConnection's own allocations are printed with the per-thread
/// counter (`counters::thread_allocations`). Expected RED before the Database-level registration:
/// three registrations, and a plain connection that has no `now()`. The registrations are counted on
/// the Database (`static_extension_registrations`, lead 2026-10-15: the first count was
/// process-wide, so servers opened by tests running in parallel moved it).
#[test]
fn the_pg_time_aliases_are_registered_once_per_database() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("a.db").to_string_lossy().into_owned();
    let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
        sync: turso_core::branch::SyncClass::Off,
    });
    let (_io, db) = turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts).unwrap();
    let before = db.static_extension_registrations();
    assert_eq!(before, 1, "the open registered the aliases {before} times for its database");
    let mut allocs = Vec::new();
    for _ in 0..3 {
        let conn = db.connect().unwrap();
        let a0 = counters::thread_allocations();
        let pg = turso_pg::PgConnection::new(conn);
        allocs.push(counters::thread_allocations() - a0);
        drop(pg);
    }
    println!("PgConnection::new allocations: {allocs:?}");
    let registrations = db.static_extension_registrations() - before;
    assert_eq!(registrations, 0, "3 connections registered static extensions {registrations} times on their database");
    let plain = db.connect().unwrap();
    let rows = plain.prepare("SELECT typeof(now())").unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::build_text("text")]], "a plain engine connection resolves now()");
}
