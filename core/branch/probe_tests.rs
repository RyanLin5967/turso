//! r11-ever amendment 34's fire-check for instrument I and its arms: the phase timers record
//! nothing while off and something while on, their sum never exceeds the reap's own elapsed time,
//! a stall planted in the drop phase is found there, arm G parks states without changing what a
//! reap frees, arm R reserves the free lists, and the locality counter separates a scattered live
//! set from a compact one.

use super::*;
use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};
use std::time::Instant;

fn open_db() -> (tempfile::TempDir, Arc<Database>, Arc<Connection>) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("probe.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=200 {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", "x".repeat(100)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    (dir, db, trunk)
}

/// A trunk child that has written one row, so it owns one arena page.
fn writer(trunk: &Arc<Connection>, n: usize) -> Branch {
    let b = trunk.fork_branch().unwrap();
    let conn = b.connect().unwrap();
    conn.execute(format!("UPDATE t SET v = 'b{n}' WHERE id = {}", n % 200 + 1))
        .unwrap();
    drop(conn);
    b
}

#[test]
fn the_phase_timers_record_nothing_while_off_and_never_more_than_the_reap_took() {
    let (_dir, db, trunk) = open_db();
    let b = writer(&trunk, 0);
    assert_eq!(b.reap().unwrap().freed_pages, 1);
    assert_eq!(db.branch_last_reap_phases(), [0; 5], "a reap with the timers off recorded time");

    db.branch_set_reap_phases(true);
    let mut seen = [false; 5];
    for n in 1..=200 {
        let b = writer(&trunk, n);
        let t = Instant::now();
        let r = b.reap().unwrap();
        let elapsed = t.elapsed().as_nanos() as u64;
        assert_eq!(r.freed_pages, 1);
        let ph = db.branch_last_reap_phases();
        assert!(
            ph.iter().sum::<u64>() <= elapsed,
            "the phases {ph:?} sum past the reap's own {elapsed} ns"
        );
        for (s, &p) in seen.iter_mut().zip(&ph) {
            *s |= p > 0;
        }
    }
    assert_eq!(seen, [true; 5], "some phase never recorded time over 200 reaps");

    db.branch_set_reap_phases(false);
    let b = writer(&trunk, 201);
    let before = db.branch_last_reap_phases();
    b.reap().unwrap();
    assert_eq!(db.branch_last_reap_phases(), before, "a reap with the timers off recorded time");
}

#[test]
fn a_stall_planted_in_the_drop_phase_is_found_in_the_drop_phase() {
    let (_dir, db, trunk) = open_db();
    db.branch_set_reap_phases(true);
    db.branch_plant_drop_sleep_us(2_000);
    let b = writer(&trunk, 0);
    b.reap().unwrap();
    let ph = db.branch_last_reap_phases();
    assert!(ph[4] >= 2_000_000, "the planted 2 ms is not in drop: {ph:?}");
    assert!(ph[..4].iter().all(|&p| p < 1_000_000), "the planted 2 ms leaked into another phase: {ph:?}");
    db.branch_plant_drop_sleep_us(0);
    let b = writer(&trunk, 1);
    b.reap().unwrap();
    assert!(db.branch_last_reap_phases()[4] < 1_000_000, "the planted stall outlived its switch");
}

#[test]
fn the_graveyard_parks_states_without_changing_what_a_reap_frees() {
    let (_dir, db, trunk) = open_db();
    assert_eq!(db.branch_set_graveyard(Some(8)), 0);
    let branches: Vec<Branch> = (0..5).map(|n| writer(&trunk, n)).collect();
    assert_eq!(db.branch_resident().arena_in_use, 5);
    for b in branches {
        let r = b.reap().unwrap();
        assert_eq!((r.freed_pages, r.deferred), (1, false));
    }
    let r = db.branch_resident();
    assert_eq!((r.states, r.zombies, r.arena_in_use), (0, 0, 0));
    assert_eq!(db.branch_set_graveyard(None), 5, "the graveyard did not park every reaped state");
    let b = writer(&trunk, 9);
    b.reap().unwrap();
    assert_eq!(db.branch_set_graveyard(None), 0, "a state was parked after the arm was turned off");

    assert_eq!(db.branch_set_graveyard(Some(4)), 0);
    for n in 10..12 {
        writer(&trunk, n).reap().unwrap();
    }
    assert_eq!(db.branch_leak_graveyard(), 2, "the leak did not take every parked state");
    writer(&trunk, 12).reap().unwrap();
    assert_eq!(db.branch_set_graveyard(None), 0, "a state was parked after the leak");
    assert_eq!(db.branch_resident().arena_in_use, 0);
}

#[test]
fn the_free_list_reserve_and_the_touch_read_nothing_they_should_not() {
    let (_dir, db, trunk) = open_db();
    let b = writer(&trunk, 0);
    assert_ne!(db.branch_touch(&b), 0);
    db.branch_reserve_free(10_000);
    assert!(db.branch_resident().arena_free_list_capacity >= 10_000);
    let kept = writer(&trunk, 1);
    b.reap().unwrap();
    assert_eq!(db.branch_resident().arena_free_list_len, 1);
    assert_ne!(db.branch_touch(&kept), 0);
}

#[test]
fn the_locality_counter_separates_a_scattered_live_set_from_a_compact_one() {
    let page = 16 * 1024;
    let (_d1, compact_db, compact_trunk) = open_db();
    let compact: Vec<Branch> = (0..100).map(|n| writer(&compact_trunk, n)).collect();
    let (c_entries, _) = compact_db.branch_live_entry_pages(page);

    let (_d2, db, trunk) = open_db();
    let mut live: Vec<Branch> = (0..10_000).map(|n| writer(&trunk, n)).collect();
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    while live.len() > 100 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = (x % live.len() as u64) as usize;
        live.swap_remove(k).reap().unwrap();
    }
    let (s_entries, s_maps) = db.branch_live_entry_pages(page);
    assert!(
        s_entries >= 10 * c_entries,
        "scattered {s_entries} entry pages vs compact {c_entries}: the counter does not separate them"
    );
    assert!(s_maps > 0);
    drop(compact);
}
