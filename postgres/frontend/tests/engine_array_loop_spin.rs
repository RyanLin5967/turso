//! fastest-engine red (lead HIGH 2026-10-22T09:50Z, from fastest-wire; raws artie 8ede325795,
//! 9a274c4c2b, 31a7116f0b, c98ca44b05, 5acdfb2457, fb4e3a133b). UNRUN when committed: the UW
//! account was disabled, so it was written from the source alone.
//!
//! A statement over an array column whose element type has an ENCODE or DECODE runs that
//! transform in a per-element loop (`emit_array_element_loop`). The loop's back edge jumped to a
//! raw instruction offset taken with `program.offset()`, which `emit_constant_insns` does not
//! remap when it hoists constants (labels are remapped; the builder says so at
//! `link_label_to_other_label`). A constant hoisted from code emitted BEFORE the loop (numeric(5,2)'s
//! own encode arguments for an earlier numeric(5,2)[] column; b[1]'s index beside b) shifts every
//! later instruction up, so the back edge lands past the loop's exit test, and the loop never
//! ends: the session thread spins in Program::normal_step.
//!
//! BOUNDED: each statement runs under a progress handler that interrupts it after 20,000 calls of
//! 100 VM steps (2,000,000 steps), far more than these one-row statements take. An interrupt is the
//! spin, reported as a failure, never a hang.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[test]
fn an_array_element_loop_after_a_hoisted_constant_terminates() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    let core = db.connect().unwrap();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    core.set_progress_handler_shared(100, Some(Arc::new(move || seen.fetch_add(1, Ordering::Relaxed) >= 20_000)));
    let c = turso_pg::Connection::new(core);
    let mut wrong = Vec::new();
    let mut run = |sql: &str| {
        calls.store(0, Ordering::Relaxed);
        let got = match c.query(sql) {
            Ok(Some(mut st)) => st.run_collect_rows().map(|_| ()).map_err(|e| e.to_string()),
            Ok(None) => Ok(()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = got {
            let steps = calls.load(Ordering::Relaxed) * 100;
            wrong.push(format!("{sql}: {e} (about {steps} VM steps: interrupted, the loop did not end)"));
        }
    };
    // The repro: a numeric(5,2)[] column before the boolean[] column written.
    run("CREATE TABLE na (id int PRIMARY KEY, n numeric(5,2)[], b boolean[])");
    run("INSERT INTO na (id, b) VALUES (4, '{t,f}')");
    run("INSERT INTO na (id, n, b) VALUES (5, '{1.01,2.5}', '{t}')");
    // The read-back spin (c98ca44b05): the decode loop of n.
    run("SELECT n, n[1] FROM na");
    // The second stall (5acdfb2457, fb4e3a133b): boolean[] alone, b beside b[1].
    run("CREATE TABLE nb (id int PRIMARY KEY, b boolean[])");
    run("INSERT INTO nb VALUES (1, '{t,f}')");
    run("SELECT b, b[1] FROM nb");
    run("SELECT id, b, b[1] FROM nb");
    run("SELECT b, array_length(b, 1) FROM nb");
    assert!(wrong.is_empty(), "CLAIM: every statement over an array with a per-element transform ends: {wrong:#?}");
}
