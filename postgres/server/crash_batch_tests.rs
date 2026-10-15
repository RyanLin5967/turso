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
/// `translate::planner::parse_table` (raw artie lanes/wire/raw/loud-20261015/viewchain-probe). It
/// must be refused as PostgreSQL's stack depth limit (54001), at whichever statement first plans
/// the whole chain, and a shallow chain must still answer.
#[test]
fn a_ninety_view_chain_is_refused_with_54001() {
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
        let Some((at, (code, message))) = refusal else {
            panic!("a 90-view chain was planned without a refusal");
        };
        assert_eq!(code, "54001", "the chain was refused at depth {at} with {code} ({message}); want 54001");
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
        .filter(|l| !l.starts_with('\\'))
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

/// The view limit's boundary (turso_core's MAX_VIEW_DEPTH, 32): a chain of exactly 32 views is
/// planned on the 8 MiB stack and answers; one more is refused 54001. So the limit sits under the
/// depth the stack takes in a debug build, which is what makes the refusal come before an overflow.
#[test]
fn a_view_chain_at_the_limit_answers_and_one_more_is_refused() {
    on_session(|s| {
        ok(s, "CREATE TABLE t(a int)");
        ok(s, "INSERT INTO t VALUES (7)");
        ok(s, "CREATE VIEW v0 AS SELECT a FROM t");
        for i in 1..32 {
            ok(s, &format!("CREATE VIEW v{i} AS SELECT a FROM v{}", i - 1));
        }
        // v31 expands 32 views.
        let conn = s.shared.db.connect().unwrap();
        let rows = conn.prepare("SELECT a FROM v31").unwrap().run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(7)]], "the 32-view chain");
        let more = first_error(s, "CREATE VIEW v32 AS SELECT a FROM v31")
            .or_else(|| first_error(s, "SELECT a FROM v32"));
        assert_eq!(more.map(|e| e.0), Some("54001".to_string()), "a 33-view chain");
        alive(s);
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
