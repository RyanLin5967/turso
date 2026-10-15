//! The PostgreSQL time aliases (now, clock_timestamp, transaction_timestamp, statement_timestamp)
//! are registered once per Database, not on every connection (lead, 2026-10-15; the wire's L5 work:
//! each new engine connection ran `aliases::install`, 13 allocations per branch switch).

use super::*;

/// Three PostgreSQL connections on one database register the aliases at most once between them,
/// a plain engine connection made after the open already resolves `now()` (the aliases are the
/// Database's), and each PgConnection's own allocations are printed with the per-thread counter
/// (`counters::thread_allocations`). Expected RED before the Database-level registration: three
/// registrations, and a plain connection that has no `now()`.
#[test]
fn the_pg_time_aliases_are_registered_once_per_database() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("a.db").to_string_lossy().into_owned();
    let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
        sync: turso_core::branch::SyncClass::Off,
    });
    let (_io, db) = turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts).unwrap();
    let before = turso_pg::alias_registrations();
    let mut allocs = Vec::new();
    for _ in 0..3 {
        let conn = db.connect().unwrap();
        let a0 = counters::thread_allocations();
        let pg = turso_pg::PgConnection::new(conn);
        allocs.push(counters::thread_allocations() - a0);
        drop(pg);
    }
    println!("PgConnection::new allocations: {allocs:?}");
    let registrations = turso_pg::alias_registrations() - before;
    assert!(registrations <= 1, "3 connections registered the aliases {registrations} times");
    let plain = db.connect().unwrap();
    let rows = plain.prepare("SELECT typeof(now())").unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::build_text("text")]], "a plain engine connection resolves now()");
}
