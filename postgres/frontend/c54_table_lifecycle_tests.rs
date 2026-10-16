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

#[test]
fn c54_native_table_types_and_constraints_survive_rename_rebuild_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native-table.db");
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
        pg.execute(
            "CREATE TABLE c54_native(id INTEGER PRIMARY KEY, a SMALLINT, note VARCHAR(5), \
             CONSTRAINT positive CHECK(a > 0))",
        )
        .unwrap();
        pg.execute("INSERT INTO c54_native VALUES(1,3,'abc')")
            .unwrap();
        assert_native_table_contract(&core, &pg, "a", false);
        pg.execute("ALTER TABLE c54_native RENAME COLUMN a TO c")
            .unwrap();
        assert_native_table_contract(&core, &pg, "c", false);
        pg.execute("ALTER TABLE c54_native ADD CONSTRAINT upper_bound CHECK(c < 10)")
            .unwrap();
        assert_native_table_contract(&core, &pg, "c", true);
        core.execute("VACUUM").unwrap();
        assert_native_table_contract(&core, &pg, "c", true);
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
    let core = db.connect().unwrap();
    let pg = PgConnection::new(core.clone());
    assert_native_table_contract(&core, &pg, "c", true);
    pg.close().unwrap();
}

#[test]
fn c54_virtual_definition_survives_rename_constraint_rebuild_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("renamed-virtual.db");
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
        // The named virtual definition and row (3, 6) are independently recorded
        // in c54-pg18-class.out. Rename must retain that definition for rebuild.
        pg.execute(
            "CREATE TABLE vn(a INT,b INT CONSTRAINT vg GENERATED ALWAYS AS(a*2) VIRTUAL)",
        )
        .unwrap();
        pg.execute("INSERT INTO vn(a) VALUES(3)").unwrap();
        assert_virtual_table_contract(&core, &pg, "a", 3);
        pg.execute("ALTER TABLE vn RENAME COLUMN a TO c").unwrap();
        assert_virtual_table_contract(&core, &pg, "c", 3);
        pg.execute("ALTER TABLE vn ADD CONSTRAINT ck CHECK(c>0)")
            .unwrap();
        assert_virtual_table_contract(&core, &pg, "c", 3);
        core.execute("VACUUM").unwrap();
        assert_virtual_table_contract(&core, &pg, "c", 3);
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
    let core = db.connect().unwrap();
    let pg = PgConnection::new(core.clone());
    assert_virtual_table_contract(&core, &pg, "c", 3);
    pg.execute("UPDATE vn SET c=4").unwrap();
    assert_virtual_table_contract(&core, &pg, "c", 4);
    let error = pg.execute("INSERT INTO vn(c) VALUES(-1)").unwrap_err();
    assert_eq!(
        error.sql_error_kind(),
        turso_pg_parser::SqlErrorKind::CheckViolation
    );
    assert_virtual_table_contract(&core, &pg, "c", 4);
    pg.close().unwrap();
}

fn assert_native_table_contract(
    core: &Arc<turso_core::Connection>,
    pg: &PgConnection,
    renamed: &str,
    added_constraint: bool,
) {
    use turso_pg_parser::pg_query::protobuf::node::Node;

    let parsed = native_table_definition(core, "c54_native");
    let Some(Node::CreateStmt(create)) = parsed.protobuf.stmts[0]
        .stmt
        .as_ref()
        .and_then(|stmt| stmt.node.as_ref())
    else {
        panic!("native table source must remain CREATE TABLE");
    };
    let columns: Vec<_> = create
        .table_elts
        .iter()
        .filter_map(|node| match node.node.as_ref() {
            Some(Node::ColumnDef(column)) => Some(column),
            _ => None,
        })
        .collect();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[1].colname, renamed);
    for (column, expected) in [(columns[1], "int2"), (columns[2], "varchar")] {
        let actual = column.type_name.as_ref().unwrap().names.last().unwrap();
        assert!(matches!(actual.node.as_ref(), Some(Node::String(name)) if name.sval == expected));
    }
    let typmods = &columns[2].type_name.as_ref().unwrap().typmods;
    assert_eq!(typmods.len(), 1);
    assert!(matches!(typmods[0].node.as_ref(), Some(Node::AConst(value))
        if matches!(&value.val, Some(turso_pg_parser::pg_query::protobuf::a_const::Val::Ival(n)) if n.ival == 5)));
    let names: Vec<_> = create
        .table_elts
        .iter()
        .filter_map(|node| match node.node.as_ref() {
            Some(Node::Constraint(constraint)) => Some(constraint.conname.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"positive"));
    if added_constraint {
        assert!(names.contains(&"upper_bound"));
    }
    assert_eq!(
        pg.prepare(format!("SELECT id,{renamed},note FROM c54_native"))
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![
            Value::from_i64(1),
            Value::from_i64(3),
            Value::build_text("abc")
        ]]
    );
    for (id, value, kind) in [
        ("NULL", 4, turso_pg_parser::SqlErrorKind::NotNullViolation),
        ("2", -1, turso_pg_parser::SqlErrorKind::CheckViolation),
    ] {
        let error = pg
            .execute(format!("INSERT INTO c54_native VALUES({id},{value},'x')"))
            .unwrap_err();
        assert_eq!(error.sql_error_kind(), kind);
    }
    if added_constraint {
        let error = pg
            .execute("INSERT INTO c54_native VALUES(2,11,'x')")
            .unwrap_err();
        assert_eq!(
            error.sql_error_kind(),
            turso_pg_parser::SqlErrorKind::CheckViolation
        );
    }
}

fn assert_virtual_table_contract(
    core: &Arc<turso_core::Connection>,
    pg: &PgConnection,
    renamed: &str,
    value: i64,
) {
    use turso_parser::ast::{ColumnConstraint, CreateTableBody, GeneratedColumnType, Stmt};
    use turso_pg_parser::pg_query::protobuf::node::Node;

    let parsed = native_table_definition(core, "vn");
    let Some(Node::CreateStmt(create)) = parsed.protobuf.stmts[0]
        .stmt
        .as_ref()
        .and_then(|stmt| stmt.node.as_ref())
    else {
        panic!("native virtual definition must remain CREATE TABLE");
    };
    assert!(create.table_elts.iter().any(|node| match node.node.as_ref() {
        Some(Node::ColumnDef(column)) if column.colname == "b" => {
            column.constraints.iter().any(|node| {
                matches!(node.node.as_ref(), Some(Node::Constraint(constraint)) if constraint.conname == "vg")
            })
        }
        _ => false,
    }));
    let stmt = turso_pg_parser::translator::PostgreSQLTranslator::new()
        .translate(&parsed)
        .unwrap();
    let Stmt::CreateTable {
        body: CreateTableBody::ColumnsAndConstraints { columns, .. },
        ..
    } = stmt
    else {
        panic!("virtual source must translate to CREATE TABLE");
    };
    assert!(columns.iter().any(|column| column.col_name.as_str() == "b"
        && column.constraints.iter().any(|constraint| {
            matches!(
                constraint.constraint,
                ColumnConstraint::Generated {
                    typ: Some(GeneratedColumnType::Virtual),
                    ..
                }
            )
        })));
    assert_eq!(
        pg.prepare(format!("SELECT {renamed},b FROM vn"))
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(value), Value::from_i64(value * 2)]]
    );
}

fn native_table_definition(
    core: &Arc<turso_core::Connection>,
    table: &str,
) -> turso_pg_parser::ParseResult {
    let rows = core
        .prepare(format!("SELECT sql FROM sqlite_schema WHERE name='{table}'"))
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    let [Value::Text(stored)] = rows[0].as_slice() else {
        panic!("table must retain one text definition: {rows:?}");
    };
    let source = crate::catalog::decode_stored_pg_schema_sql(stored.as_str())
        .expect("native table source must remain available to ADD CONSTRAINT");
    turso_pg_parser::parse(source).expect("retained native definition must parse as PostgreSQL")
}
