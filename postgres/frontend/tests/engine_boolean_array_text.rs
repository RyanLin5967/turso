//! fastest-engine red (lead HIGH 2026-10-22T10:31Z, from fastest-wire; SILENT; raws artie
//! 91d4d2d984, 0a3de27db0). UNRUN when committed: UW was unavailable.
//!
//! A boolean[] reads back as PostgreSQL 18.6 writes it: '{yes,off,NULL}' is '{t,f,NULL}'
//! (recorded: fastest-wire raw rec46-array-assign, pg18-wirefuzz-an_array_of_non_integer_elements
//! _is_read_by_its_element_input_routine.out, row 4). The engine read '{1,0,0}': the per-element
//! decode loop runs boolean's DECODE (`CASE WHEN value THEN 1 ELSE 0 END`) on a NULL element too,
//! which makes it 0, and writes the element as its integer. b[3] IS NULL stayed true, so the
//! element is stored NULL: the loss is in the output, and a client reads 0 where the value is NULL.

use std::sync::Arc;

#[test]
fn a_boolean_array_reads_back_t_f_and_null_as_postgresql_writes_them() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = turso_pg::open_database_with_io(
        Arc::new(turso_core::PlatformIO::new().unwrap()),
        dir.path().join("pg.db").to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true).with_attach(true),
    )
    .unwrap();
    let c = turso_pg::Connection::new(db.connect().unwrap());
    let mut wrong = Vec::new();
    for sql in ["CREATE TABLE na (id int PRIMARY KEY, b boolean[])", "INSERT INTO na (id, b) VALUES (4, '{yes,off,NULL}')"] {
        if let Err(e) = c.execute(sql) {
            wrong.push(format!("setup {sql}: {e}"));
        }
    }
    let shown = |v: &turso_core::Value| if matches!(v, turso_core::Value::Null) { "NULL".to_string() } else { v.to_string() };
    match c.query("SELECT b, b[3] IS NULL FROM na WHERE id = 4") {
        Ok(Some(mut st)) => match st.run_collect_rows() {
            Ok(rows) => {
                let got: Vec<String> = rows.first().map(|r| r.iter().map(shown).collect()).unwrap_or_default();
                if got.first().map(String::as_str) != Some("{t,f,NULL}") {
                    wrong.push(format!("b read {got:?}, PostgreSQL {{t,f,NULL}}"));
                }
            }
            Err(e) => wrong.push(format!("read: {e}")),
        },
        other => wrong.push(format!("read: {:?}", other.err().map(|e| e.to_string()))),
    }
    assert!(wrong.is_empty(), "CLAIM: a boolean[] reads back t, f and NULL as PostgreSQL writes them: {wrong:#?}");
}
