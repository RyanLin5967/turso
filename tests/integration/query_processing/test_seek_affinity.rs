use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn counts(conn: &std::sync::Arc<turso_core::Connection>, query: &str) -> i64 {
    match limbo_exec_rows(conn, query)[0][0] {
        Value::Integer(n) => n,
        ref other => panic!("expected an integer count for `{query}`, got {other:?}"),
    }
}

fn query_plan(conn: &std::sync::Arc<turso_core::Connection>, query: &str) -> String {
    limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN {query}"))
        .iter()
        .filter_map(|row| match row.get(3) {
            Some(Value::Text(plan)) => Some(plan.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn text_column_compared_to_integer_subquery_does_not_seek_index() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(x TEXT)");
    limbo_exec_rows(&conn, "CREATE TABLE u(k INTEGER)");
    limbo_exec_rows(&conn, "INSERT INTO t VALUES('20')");
    limbo_exec_rows(&conn, "INSERT INTO u VALUES(20)");
    limbo_exec_rows(&conn, "CREATE INDEX i ON t(x)");

    let eq = "SELECT count(*) FROM t WHERE x = (SELECT k FROM u)";
    let gt = "SELECT count(*) FROM t WHERE x > (SELECT k FROM u)";

    for query in [eq, gt] {
        let plan = query_plan(&conn, query);
        assert!(
            !plan.contains("SEARCH t"),
            "a TEXT index cannot answer a numeric comparison for `{query}`, got:\n{plan}"
        );
    }

    assert_eq!(counts(&conn, eq), 1, "`{eq}` must match the converted row");
    assert_eq!(counts(&conn, gt), 0, "`{gt}` must not match a larger value");

    limbo_exec_rows(&conn, "DELETE FROM t WHERE x = (SELECT k FROM u)");
    assert_eq!(
        counts(&conn, "SELECT count(*) FROM t"),
        0,
        "the delete must remove the row the equality matched"
    );
}

#[test]
fn text_column_compared_to_text_subquery_still_seeks_index() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(x TEXT)");
    limbo_exec_rows(&conn, "CREATE TABLE v(k TEXT)");
    limbo_exec_rows(&conn, "INSERT INTO t VALUES('20')");
    limbo_exec_rows(&conn, "INSERT INTO v VALUES('20')");
    limbo_exec_rows(&conn, "CREATE INDEX i ON t(x)");

    let eq = "SELECT count(*) FROM t WHERE x = (SELECT k FROM v)";
    let plan = query_plan(&conn, eq);
    assert!(
        plan.contains("SEARCH t"),
        "a TEXT index still answers a text comparison for `{eq}`, got:\n{plan}"
    );
    assert_eq!(counts(&conn, eq), 1, "`{eq}` must match the row");
}

#[test]
fn two_text_columns_compared_to_integer_subqueries_do_not_intersect_indexes() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(&conn, "CREATE TABLE t(x TEXT, y TEXT)");
    limbo_exec_rows(&conn, "CREATE TABLE u(k INTEGER)");
    limbo_exec_rows(&conn, "INSERT INTO t VALUES('20','20')");
    limbo_exec_rows(&conn, "INSERT INTO u VALUES(20)");
    limbo_exec_rows(&conn, "CREATE INDEX ix ON t(x)");
    limbo_exec_rows(&conn, "CREATE INDEX iy ON t(y)");

    let both = "SELECT count(*) FROM t WHERE x = (SELECT k FROM u) AND y = (SELECT k FROM u)";
    let plan = query_plan(&conn, both);
    assert!(
        !plan.contains("MULTI-INDEX AND"),
        "TEXT indexes cannot answer numeric comparisons for `{both}`, got:\n{plan}"
    );
    assert_eq!(counts(&conn, both), 1, "`{both}` must match the row");

    let one = "SELECT count(*) FROM t WHERE x = (SELECT k FROM u) AND y = '20'";
    let plan = query_plan(&conn, one);
    assert!(
        plan.contains("SEARCH t"),
        "the text-compared column still seeks its index for `{one}`, got:\n{plan}"
    );
    assert_eq!(counts(&conn, one), 1, "`{one}` must match the row");
}
