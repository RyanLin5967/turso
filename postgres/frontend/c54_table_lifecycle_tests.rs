use crate::session::{open_database_with_io, PgConnection};
use std::sync::Arc;
use turso_core::{DatabaseOpts, OpenFlags, Value, IO};

// The exact CREATE, INSERT and both renames succeed in the independent PG18.6
// c52-view-contracts recording. Its views keep reading integer 2 after each rename.
// This case isolates the table without requiring the view source mechanism.
#[test]
fn c54_table_definition_keeps_its_dialect_through_repeated_rename() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table-replay.db");
    let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    {
        let db = open_database_with_io(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_vacuum(true),
        )
        .unwrap();
        let core = db.connect().unwrap();
        let pg = PgConnection::new(core.clone());
        pg.execute("CREATE TABLE c52_contract_t(a integer,b integer)")
            .unwrap();
        pg.execute("INSERT INTO c52_contract_t VALUES(1,2)")
            .unwrap();
        for (rename, column) in [
            ("ALTER TABLE c52_contract_t RENAME COLUMN b TO c", "c"),
            ("ALTER TABLE c52_contract_t RENAME COLUMN c TO d", "d"),
        ] {
            pg.execute(rename)
                .unwrap_or_else(|error| panic!("{rename}: {error:?}"));
            let stored = core
                .prepare("SELECT sql FROM sqlite_schema WHERE name='c52_contract_t'")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(stored.len(), 1);
            eprintln!("C54 table definition after {rename}: {stored:?}");
            let rows = pg
                .prepare(format!("SELECT {column} FROM c52_contract_t"))
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![Value::from_i64(2)]]);
        }
        // The existing embedded VACUUM option exercises table replay with the
        // same PG dialect; it does not assert wire-level VACUUM support.
        core.execute("VACUUM").unwrap();
        assert_eq!(
            pg.prepare("SELECT d FROM c52_contract_t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(2)]]
        );
        drop(pg);
        core.close().unwrap();
    }
    let db = open_database_with_io(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
    )
    .unwrap();
    let pg = PgConnection::new(db.connect().unwrap());
    assert_eq!(
        pg.prepare("SELECT d FROM c52_contract_t")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
    pg.close().unwrap();
}
