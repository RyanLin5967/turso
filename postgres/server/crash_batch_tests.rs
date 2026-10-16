//! The crash batch (lead, fastest DECISIONS 2026-10-15): client SQL that took the whole server or a
//! session down. Each case runs on a thread with the default 8 MiB stack (the size a pg-session
//! thread gets from the platform, not the test harness's RUST_MIN_STACK) and must come back as a
//! clean PostgreSQL-coded error, with the session still usable after it.

use super::*;

/// The default main-thread stack on macOS and Linux, which the cases run under.
const STACK: usize = 8 << 20;

/// Run `f` on a fresh session of its own, on a thread with `STACK` bytes of stack. A panic in it
/// fails the test with the panic's text.
fn on_session(f: impl FnOnce(&Session) + Send + 'static) {
    let joined = std::thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("crash.db").to_string_lossy().into_owned();
            let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
                sync: turso_core::branch::SyncClass::Off,
            });
            let (_io, db) =
                turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts)
                    .unwrap();
            let session = Session::new(Arc::new(Shared::new(
                db,
                path,
                1,
                std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            )));
            f(&session);
        })
        .unwrap()
        .join();
    if let Err(e) = joined {
        let text = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        panic!("the session's thread panicked: {text}");
    }
}

/// The SQLSTATE and message of the first error `sql` replies with, if any.
fn first_error(s: &Session, sql: &str) -> Option<(String, String)> {
    s.simple(sql).into_iter().find_map(|r| match r {
        Response::Error(e) => Some((e.code.clone(), e.message.clone())),
        _ => None,
    })
}

fn ok(s: &Session, sql: &str) {
    if let Some((code, message)) = first_error(s, sql) {
        panic!("{sql}: {code} {message}");
    }
}

/// `sql` is refused with `code`.
fn refused(s: &Session, sql: &str, code: &str) {
    match first_error(s, sql) {
        Some((c, m)) => assert_eq!(c, code, "{sql}: refused {c} ({m}); want {code}"),
        None => panic!("{sql}: accepted; want a refusal {code}"),
    }
}

/// The session still answers after a refusal.
fn alive(s: &Session) {
    ok(s, "SELECT 1");
}

/// Views nested 90 deep: planning the outermost overflowed the stack in
/// `translate::planner::parse_table` (raw artie lanes/wire/raw/loud-20261015/viewchain-probe). The
/// bound is the stack (lead, 2026-10-15, as PostgreSQL's): the chain either answers or is refused
/// 54001 (a debug build refuses it at 8 MiB; a release build, with smaller frames, may answer), at
/// whichever statement first plans too deep, and never overflows. A shallow chain still answers.
/// FLAGGED SPEC CHANGE (ruled): it read "must be refused" under the fixed 32-deep cap; the depth
/// sweep (`every_depth_of_a_view_chain_answers_or_is_refused_never_overflows`) is its oracle.
#[test]
fn a_ninety_view_chain_answers_or_is_refused_with_54001() {
    on_session(|s| {
        ok(s, "CREATE TABLE t(a int)");
        ok(s, "INSERT INTO t VALUES (1)");
        ok(s, "CREATE VIEW v0 AS SELECT a FROM t");
        let mut refusal = None;
        for i in 1..90 {
            if let Some(e) = first_error(s, &format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1)) {
                refusal = Some((i, e));
                break;
            }
        }
        let refusal = refusal.or_else(|| first_error(s, "SELECT a FROM v89").map(|e| (90, e)));
        if let Some((at, (code, message))) = refusal {
            assert_eq!(code, "54001", "the chain was refused at depth {at} with {code} ({message}); want 54001");
        }
        ok(s, "SELECT a FROM v5");
        alive(s);
    });
}

/// `make_date` at the extremes of its int4 year: `year - 1` overflowed at core/time/internal.rs:219
/// inside an extern "C" function, so the panic could not unwind and the whole server aborted. Each
/// must be PostgreSQL's 22008 (date field value out of range).
#[test]
fn make_date_at_the_int4_extremes_is_22008() {
    on_session(|s| {
        refused(s, "SELECT make_date(-2147483648, 1, 1)", "22008");
        refused(s, "SELECT make_date(2147483647, 1, 1)", "22008");
        refused(s, "SELECT make_date(2020, -2147483648, 1)", "22008");
        alive(s);
    });
}

/// A table with no columns (PostgreSQL allows `CREATE TABLE t ()`; the wire's LIKE created one
/// too): ALTER TABLE ... DROP of a column it does not have hit `turso_assert_ne!` at
/// core/translate/alter.rs:941 and killed the session (alter_table, insert and updatable_views
/// regress scripts). PostgreSQL answers 42703.
#[test]
fn dropping_a_column_of_a_table_with_no_columns_is_42703() {
    on_session(|s| {
        ok(s, "CREATE TABLE p1(a int, b int)");
        if first_error(s, "CREATE TABLE p11 ()").is_none() {
            refused(s, "ALTER TABLE p11 DROP a", "42703");
        }
        // The fastest-wire repro, whatever the translator now does with LIKE.
        if first_error(s, "CREATE TABLE p12 (LIKE p1)").is_none() {
            let _ = first_error(s, "ALTER TABLE p12 DROP a");
        }
        alive(s);
    });
}

/// Text 'Infinity' and 'NaN' cast to a float answered 0 (the engine casts text by SQLite's
/// affinity rules). PostgreSQL 18.6 reads 'Infinity', '-Infinity' and 'inf' in any case as the
/// infinities; the engine has no NaN value, so 'NaN' is refused 0A000, as the wire's literal path
/// already refuses it. Read from a column, so no translator rewrite of a literal takes part.
#[test]
fn text_infinity_casts_to_infinity_and_nan_is_refused() {
    on_session(|s| {
        ok(s, "CREATE TABLE f(t text)");
        ok(s, "INSERT INTO f VALUES ('Infinity'), ('-Infinity'), ('inf'), ('-INF')");
        let conn = s.shared.db.connect().unwrap();
        let rows = conn
            .prepare("SELECT CAST(t AS double precision) FROM f")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let got: Vec<String> = rows.iter().map(|r| format!("{:?}", r[0])).collect();
        let want = [f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY, f64::NEG_INFINITY];
        let want: Vec<String> = want.iter().map(|v| format!("{:?}", Value::from_f64(*v))).collect();
        assert_eq!(got, want, "text infinities cast to a float");
        ok(s, "CREATE TABLE g(t text)");
        ok(s, "INSERT INTO g VALUES ('NaN')");
        refused(s, "SELECT CAST(t AS double precision) FROM g", "0A000");
        alive(s);
    });
}

/// An INSERT naming more columns than its VALUES row gives, every value DEFAULT (insert.sql statement
/// 7; found by find_killer): the translated VALUES had no row, and `values[0]` panicked at
/// core/translate/select.rs:868, killing the session. PostgreSQL refuses it 42601 ("INSERT has more
/// target columns than expressions").
#[test]
fn an_insert_whose_values_translate_to_no_row_is_42601() {
    on_session(|s| {
        ok(s, "create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing')");
        refused(s, "insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT)", "42601");
        alive(s);
    });
}

/// Diagnostic, not a budget: run a PostgreSQL regress script statement by statement (the
/// fastest-wire find_killer.py split) and name the first statement whose reply panics the session.
/// `CRASH_SCRIPT=insert cargo test -p turso_pg_server crash_batch_tests::find_killer -- --ignored
/// --nocapture`.
#[test]
#[ignore]
fn find_killer() {
    let name = std::env::var("CRASH_SCRIPT").expect("CRASH_SCRIPT names a script");
    let path = format!(
        "{}/../conformance/upstream/{name}.sql",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap();
    let text: String = text
        .lines()
        // Meta-commands and whole-line comments go; a statement after a comment block stays
        // (find_killer.py dropped such a statement whole, so a script's first CREATE never ran).
        .filter(|l| !l.starts_with('\\') && !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut stmts = Vec::new();
    let mut rest = text.as_str();
    while let Some(at) = rest.find(";\n") {
        stmts.push(rest[..at].trim().to_string());
        rest = &rest[at + 2..];
    }
    stmts.push(rest.trim().to_string());
    let stmts: Vec<String> = stmts
        .into_iter()
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect();
    on_session(move |s| {
        for (i, stmt) in stmts.iter().enumerate() {
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.simple(stmt)));
            if caught.is_err() {
                println!("KILLED BY statement {i}: {stmt}");
                return;
            }
        }
        println!("survived {} statements", stmts.len());
    });
}

/// The SQLite dialect keeps SQLite's rule: a text that is no number casts to 0.0, 'Infinity' and
/// 'NaN' included (sqlite3 3.51.0 answers 0.0 for both). Only the PostgreSQL dialect reads them.
#[test]
fn the_sqlite_dialect_still_casts_infinity_text_to_zero() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(turso_core::SqliteDialect))
        .unwrap();
    let conn = db.connect().unwrap();
    let rows = conn
        .prepare("SELECT CAST('Infinity' AS REAL), CAST('NaN' AS REAL), CAST('-inf' AS REAL)")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_f64(0.0); 3]]);
}

/// C32 HIGH (a Claude review, verified, 2026-10-15): the time operators did unchecked chrono
/// arithmetic, so a result past chrono's range (or a u32 month span, or a blob's seconds near
/// i64::MIN) panicked inside an extern "C" scalar, which cannot unwind: the process aborted. Each
/// case must answer NULL or a clean error, and the connection must answer after it. Expected RED:
/// the test process aborts at the first case.
#[test]
fn the_time_operators_answer_an_overflow_with_null_or_an_error() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(turso_core::SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    for sql in [
        // 10^8 days past now (about 273,800 years, past chrono's year 262142).
        "SELECT time_add_date(time_now(), 0, 0, 100000000)",
        // 2^63 - 1 ns past year 262000.
        "SELECT time_add(time_date(262000, 1, 1), 9223372036854775807)",
        // ... and before year -262000.
        "SELECT time_add(time_date(-262000, 1, 1), -9223372036854775807)",
        // 4 * 10^8 years: the month span overflows a u32.
        "SELECT time_add_date(time_now(), 400000000)",
        // 2^31 years: truncated to an i32 before.
        "SELECT time_add_date(time_now(), 2147483648)",
        // A day count that fits a Duration, added past chrono's range.
        "SELECT time_date(2020, 1, 100000000)",
        // A blob whose seconds are i64::MIN.
        "SELECT time_get_year(x'01800000000000000000000000')",
    ] {
        match conn.prepare(sql).and_then(|mut st| st.run_collect_rows()) {
            Ok(rows) => assert_eq!(rows, vec![vec![Value::Null]], "{sql}: answered {rows:?}; want NULL or an error"),
            Err(_) => {}
        }
        let rows = conn.prepare("SELECT 1").unwrap().run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(1)]], "after {sql}: the connection does not answer");
    }
}

/// The lead, 2026-10-15 (the checked-time class): SQLite's date functions added caller values with
/// wrapping_add, so arithmetic past the date range could wrap to a silent wrong date where SQLite
/// answers NULL. Each case's expected NULL is sqlite3 3.51.0's (/usr/bin/sqlite3, 2025-06-12, raw
/// artie lanes/budgets/raw/sqlite3-date-oracle.txt), the chained ones included. The 12 single
/// modifiers were already NULL at 1f38e9d83 (each modifier's own limit keeps one add in range); the
/// chained ones are the red, where the engine wraps.
#[test]
fn date_arithmetic_past_the_range_is_null_as_in_sqlite() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(turso_core::SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let mut wrong = Vec::new();
    for sql in [
        "SELECT date('9999-12-31','+1 day')",
        "SELECT julianday('2000-01-01','+5373484 days')",
        "SELECT date('2000-01-01','+176545 months')",
        "SELECT date('2000-01-01','+14712 years')",
        "SELECT datetime('2000-01-01','+464269999999999 seconds')",
        "SELECT datetime('2000-01-01','-464269999999999 seconds')",
        "SELECT julianday('2000-01-01','+128969999999 hours')",
        "SELECT date('2000-01-01','-14712 years')",
        "SELECT datetime(5373484.4, '+1 day')",
        "SELECT datetime('2000-01-01', '+99999:00:00')",
        "SELECT datetime('2000-01-01', '+1000000000000:00')",
        "SELECT date('-4713-11-24','-1 day')",
    ]
    .into_iter()
    .map(String::from)
    // Each modifier within its own limit, 25 of them chained: the sum passes i64 (about 20 do).
    .chain(["+464269999999999 seconds", "-464269999999999 seconds", "+128969999999 hours"].iter().map(|m| {
        let mods: Vec<String> = (0..25).map(|_| format!("'{m}'")).collect();
        format!("SELECT julianday('2000-01-01', {})", mods.join(", "))
    })) {
        let sql = sql.as_str();
        match conn.prepare(sql).and_then(|mut st| st.run_collect_rows()) {
            Ok(rows) if rows == vec![vec![Value::Null]] => {}
            other => wrong.push(format!("{sql}: {other:?}")),
        }
    }
    assert!(wrong.is_empty(), "sqlite3 answers NULL for each:\n{}", wrong.join("\n"));
}

/// The one case found where the chained adds wrap i64 and land back in range: 64 modifiers of
/// 2^58 ms each sum to exactly 2^64 ms. sqlite3 3.51.0 (/usr/bin/sqlite3) answers
/// '2000-01-01 00:00:00' here, not NULL: SQLite's own `iJD +=` wraps as well (raw artie
/// lanes/budgets/raw/sqlite3-date-oracle.txt). This pins the engine's answer to sqlite3's.
#[test]
fn date_arithmetic_that_wraps_answers_as_sqlite3_does() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(turso_core::SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let mods: Vec<&str> = (0..64).map(|_| "'+288230376151711.744 seconds'").collect();
    let sql = format!("SELECT datetime('2000-01-01', {})", mods.join(", "));
    let rows = conn.prepare(&sql).unwrap().run_collect_rows().unwrap();
    assert_eq!(rows, vec![vec![Value::build_text("2000-01-01 00:00:00")]], "sqlite3 3.51.0's answer");
}

/// Run `f` on a fresh session on a thread of `stack` bytes ([`on_session`] at another size).
fn on_session_with_stack(stack: usize, f: impl FnOnce(&Session) + Send + 'static) {
    let joined = std::thread::Builder::new()
        .stack_size(stack)
        .spawn(move || {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("crash.db").to_string_lossy().into_owned();
            let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
                sync: turso_core::branch::SyncClass::Off,
            });
            let (_io, db) =
                turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts)
                    .unwrap();
            let session = Session::new(Arc::new(Shared::new(
                db,
                path,
                1,
                std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            )));
            f(&session);
        })
        .unwrap()
        .join();
    if let Err(e) = joined {
        let text = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        panic!("the session's thread panicked: {text}");
    }
}

/// A chain of `n` views over `t`, v0 .. v{n-1}, each a SELECT of the one before. Creating one plans
/// the chain under it, so a creation may itself be refused: the chain is made as far as it goes,
/// and the depth reached is returned with the refusal, if any.
fn view_chain(s: &Session, n: usize) -> (usize, Option<(String, String)>) {
    ok(s, "CREATE TABLE t(a int)");
    ok(s, "INSERT INTO t VALUES (1)");
    ok(s, "CREATE VIEW v0 AS SELECT a FROM t");
    for i in 1..n {
        if let Some(e) = first_error(s, &format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1)) {
            return (i, Some(e));
        }
    }
    (n, None)
}

/// Lead, 2026-10-15: the view bound is the STACK, as PostgreSQL's (max_stack_depth), not a fixed
/// depth: a 40-view chain answers on the default 8 MiB session stack, as it does in PostgreSQL
/// (fastest-wire's a_view_chain_of_any_depth_is_typed). Expected RED with the fixed 32-deep cap.
#[test]
fn a_forty_view_chain_answers_on_the_session_stack() {
    on_session(|s| {
        let (made, refused) = view_chain(s, 40);
        assert_eq!(refused, None, "the 40-view chain was refused at depth {made}");
        ok(s, "SELECT a FROM v39");
        alive(s);
    });
}

/// A thousand views: refused 54001 at the default 8 MiB session stack and on a 2 MiB thread,
/// whichever statement first plans too deep, never a stack overflow. Expected RED with the fixed cap
/// on the 2 MiB thread: 32 views overflow 2 MiB in a debug build before the cap refuses.
#[test]
fn a_thousand_view_chain_is_refused_54001_on_8_and_on_2_mib() {
    for stack in [8usize << 20, 2 << 20] {
        on_session_with_stack(stack, move |s| {
            let (made, refused) = view_chain(s, 1000);
            let refusal = refused.or_else(|| first_error(s, "SELECT a FROM v999"));
            let Some((code, message)) = refusal else {
                panic!("a 1000-view chain was planned on a {} MiB stack", stack >> 20);
            };
            assert_eq!(code, "54001", "refused at depth {made} on {} MiB with {code} ({message})", stack >> 20);
            alive(s);
        });
    }
}

/// The bound's boundary, swept: on a 2 MiB thread and on 8 MiB, a SELECT from every depth of a
/// chain either answers or is refused 54001, never overflows (an overflow aborts the test process:
/// RED), and once one depth is refused every deeper one is too.
#[test]
fn every_depth_of_a_view_chain_answers_or_is_refused_never_overflows() {
    for (stack, n) in [(2usize << 20, 120usize), (8 << 20, 200)] {
        on_session_with_stack(stack, move |s| {
            let (made, _) = view_chain(s, n);
            let mut refused_at = None;
            for k in 0..made {
                match first_error(s, &format!("SELECT a FROM v{k}")) {
                    None => assert!(refused_at.is_none(), "v{k} answered after v{} was refused", refused_at.unwrap()),
                    Some((code, m)) => {
                        assert_eq!(code, "54001", "v{k} on {} MiB: {code} {m}", stack >> 20);
                        refused_at.get_or_insert(k);
                    }
                }
            }
            alive(s);
        });
    }
}

/// `generate_series` with an infinite bound (lead, 2026-10-15; the wire's red ebc1845e9 against
/// PostgreSQL 18.6's recording pg18-gsinf): an infinite stop saturated to i64::MAX and the session
/// thread spun through ~10^18 rows while the client got nothing more, so any client could wedge a
/// session thread; an infinite step answered 1 row. PostgreSQL refuses each with 22023 ("stop value
/// cannot be infinity", "step size ...", "start value ..."). Each must be refused 22023 within 10 s
/// (a wedged session fails here at the deadline instead of hanging the test), and a finite series
/// still answers.
#[test]
fn an_infinite_generate_series_bound_is_refused_22023_promptly() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .stack_size(STACK)
        .spawn(move || {
            on_session(move |s| {
                for sql in [
                    "select * from generate_series(0::numeric, 'inf'::numeric, 10::numeric)",
                    "select * from generate_series(1, 3, 'inf'::numeric)",
                    "select * from generate_series('-inf'::numeric, 0, 10)",
                ] {
                    let got = first_error(s, sql);
                    tx.send(Some((sql, got))).unwrap();
                }
                let rows = s.shared.db.connect().unwrap().prepare("select count(*) from generate_series(1, 5)").unwrap().run_collect_rows().unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(5)]], "a finite series");
                tx.send(None).unwrap();
            });
        })
        .unwrap();
    for _ in 0..4 {
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(Some((sql, got))) => match got {
                Some((code, m)) => assert_eq!(code, "22023", "{sql}: {code} {m}"),
                None => panic!("{sql}: answered; want 22023"),
            },
            Ok(None) => return,
            Err(_) => panic!("a generate_series with an infinite bound did not answer within 10 s: the session thread is wedged"),
        }
    }
    panic!("the finite control never reported");
}

/// The SQLite dialect keeps SQLite's clamp for an infinite generate_series bound (lead, 2026-10-15:
/// the refusal is the PostgreSQL dialect's, as `cast_text_to_float` is): an infinite stop is
/// i64::MAX, so `LIMIT 3` answers 0, 1, 2, as sqlite3 3.51.0 does.
#[test]
fn the_sqlite_dialect_still_clamps_an_infinite_series_bound() {
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(turso_core::SqliteDialect))
        .unwrap();
    let conn = db.connect().unwrap();
    let rows = conn
        .prepare("SELECT value FROM generate_series(0, 9e999) LIMIT 3")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(0)], vec![Value::from_i64(1)], vec![Value::from_i64(2)]]);
}
