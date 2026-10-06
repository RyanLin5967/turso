//! The SQL-standard `information_schema` views clients read to introspect tables (columns, tables,
//! key_column_usage, table_constraints), as virtual tables named `information_schema_<view>`; the
//! translator maps `information_schema.<view>` to them. Each is computed from the connection's
//! schema when scanned, with PostgreSQL's names for implicit constraints (`<table>_pkey`,
//! `<table>_<cols>_key`, `<table>_<cols>_fkey`), the names pg_constraint reports.

use crate::catalog::user_tables_sorted;
use parking_lot::RwLock;
use std::sync::Arc;
use turso_core::{
    schema::{BTreeTable, Table},
    Connection, InternalVirtualTable, InternalVirtualTableCursor, LimboError, Result, Value,
    VirtualTable,
};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind};

/// The database every view reports as table_catalog.
const CATALOG: &str = "turso";

type Load = fn(&Connection) -> Result<Vec<Vec<Value>>>;

/// A virtual table whose rows `load` computes at every scan.
#[derive(Debug)]
struct ViewTable {
    name: &'static str,
    sql: &'static str,
    load: Load,
}

impl InternalVirtualTable for ViewTable {
    fn name(&self) -> String {
        self.name.to_string()
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(ViewCursor {
            conn,
            load: self.load,
            rows: Vec::new(),
            at: 0,
        })))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1000.0,
            estimated_rows: 1000,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }

    fn sql(&self) -> String {
        self.sql.to_string()
    }
}

struct ViewCursor {
    conn: Arc<Connection>,
    load: Load,
    rows: Vec<Vec<Value>>,
    at: usize,
}

impl InternalVirtualTableCursor for ViewCursor {
    fn next(&mut self) -> std::result::Result<bool, LimboError> {
        self.at += 1;
        Ok(self.at < self.rows.len())
    }

    fn rowid(&self) -> i64 {
        self.at as i64
    }

    fn column(&self, column: usize) -> std::result::Result<Value, LimboError> {
        Ok(self
            .rows
            .get(self.at)
            .and_then(|row| row.get(column))
            .cloned()
            .unwrap_or(Value::Null))
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> std::result::Result<bool, LimboError> {
        self.at = 0;
        self.rows = (self.load)(&self.conn)?;
        Ok(!self.rows.is_empty())
    }
}

fn text(s: impl Into<String>) -> Value {
    Value::build_text(s.into())
}

fn int(i: i64) -> Value {
    Value::from_i64(i)
}

fn opt_int(i: Option<i64>) -> Value {
    i.map_or(Value::Null, Value::from_i64)
}

/// A column's declared type as information_schema reports it: (data_type, udt_name,
/// character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale).
type ColumnType = (
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

fn column_type(col: &turso_core::schema::Column) -> ColumnType {
    let lower = col.ty_str.trim().to_ascii_lowercase();
    // A custom type's parameters (varchar(n), numeric(p, s), bpchar(n)) are kept apart from its
    // name; a plain type's are part of the declared text.
    let (base, params) = match lower.find('(') {
        Some(p) => (
            lower[..p].trim().to_string(),
            lower[p + 1..]
                .trim_end_matches(')')
                .split(',')
                .filter_map(|x| x.trim().parse::<i64>().ok())
                .collect::<Vec<_>>(),
        ),
        None => (
            lower.clone(),
            col.ty_params
                .iter()
                .filter_map(|p| match p.as_ref() {
                    turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Numeric(n)) => {
                        n.parse::<i64>().ok()
                    }
                    _ => None,
                })
                .collect(),
        ),
    };
    let s = |a: &str, b: &str| (a.to_string(), b.to_string());
    let (data_type, udt) = match base.as_str() {
        "integer" | "int" | "int4" | "serial" => s("integer", "int4"),
        "smallint" | "int2" => s("smallint", "int2"),
        "bigint" | "int8" => s("bigint", "int8"),
        "varchar" | "character varying" => s("character varying", "varchar"),
        "bpchar" | "char" | "character" => s("character", "bpchar"),
        "text" | "" => s("text", "text"),
        "numeric" | "decimal" => s("numeric", "numeric"),
        "real" | "float8" | "double precision" | "double" | "float" => {
            s("double precision", "float8")
        }
        "float4" => s("real", "float4"),
        "boolean" | "bool" => s("boolean", "bool"),
        "timestamp" => s("timestamp without time zone", "timestamp"),
        "timestamptz" => s("timestamp with time zone", "timestamptz"),
        "date" => s("date", "date"),
        "time" => s("time without time zone", "time"),
        "bytea" | "blob" => s("bytea", "bytea"),
        "uuid" => s("uuid", "uuid"),
        "json" => s("json", "json"),
        "jsonb" => s("jsonb", "jsonb"),
        other => s("USER-DEFINED", other),
    };
    let (len, prec, radix, scale) = match udt.as_str() {
        "varchar" | "bpchar" => (params.first().copied(), None, None, None),
        "int2" => (None, Some(16), Some(2), Some(0)),
        "int4" => (None, Some(32), Some(2), Some(0)),
        "int8" => (None, Some(64), Some(2), Some(0)),
        "float8" => (None, Some(53), Some(2), None),
        "float4" => (None, Some(24), Some(2), None),
        "numeric" => (
            None,
            params.first().copied(),
            Some(10),
            params.first().map(|_| params.get(1).copied().unwrap_or(0)),
        ),
        _ => (None, None, None, None),
    };
    (data_type, udt, len, prec, radix, scale)
}

/// Every PRIMARY KEY, UNIQUE and FOREIGN KEY constraint of `table`: (name, type, columns).
fn constraints(name: &str, table: &BTreeTable) -> Vec<(String, &'static str, Vec<String>)> {
    let mut out = Vec::new();
    let pk_in_sets = table.unique_sets.iter().any(|u| u.is_primary_key);
    if !pk_in_sets && !table.primary_key_columns.is_empty() {
        out.push((
            format!("{name}_pkey"),
            "PRIMARY KEY",
            table
                .primary_key_columns
                .iter()
                .map(|(c, _)| c.clone())
                .collect(),
        ));
    }
    for set in &table.unique_sets {
        let cols: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        if set.is_primary_key {
            out.push((format!("{name}_pkey"), "PRIMARY KEY", cols));
        } else {
            out.push((format!("{name}_{}_key", cols.join("_")), "UNIQUE", cols));
        }
    }
    for fk in &table.foreign_keys {
        out.push((
            format!("{name}_{}_fkey", fk.child_columns.join("_")),
            "FOREIGN KEY",
            fk.child_columns.to_vec(),
        ));
    }
    out
}

fn load_columns(conn: &Connection) -> Result<Vec<Vec<Value>>> {
    let schema = conn.current_schema();
    let mut rows = Vec::new();
    for (name, table) in user_tables_sorted(&schema) {
        for (i, col) in table.columns().iter().enumerate() {
            let (data_type, udt, len, prec, radix, scale) = column_type(col);
            rows.push(vec![
                text(CATALOG),
                text("public"),
                text(name.as_str()),
                text(col.name.clone().unwrap_or_default()),
                int(i as i64 + 1),
                col.default
                    .as_ref()
                    .map_or(Value::Null, |d| text(d.to_string())),
                // A key column is NOT NULL, as in PostgreSQL (the engine's STRICT tables refuse a
                // NULL key) though no NOT NULL is declared on it (wire review 2 item 9).
                text(if col.notnull() || col.primary_key() {
                    "NO"
                } else {
                    "YES"
                }),
                text(data_type),
                opt_int(len),
                opt_int(prec),
                opt_int(radix),
                opt_int(scale),
                text(CATALOG),
                text("pg_catalog"),
                text(udt),
            ]);
        }
    }
    Ok(rows)
}

fn load_tables(conn: &Connection) -> Result<Vec<Vec<Value>>> {
    let schema = conn.current_schema();
    let mut rows: Vec<Vec<Value>> = user_tables_sorted(&schema)
        .into_iter()
        .map(|(name, _)| {
            vec![
                text(CATALOG),
                text("public"),
                text(name.as_str()),
                text("BASE TABLE"),
            ]
        })
        .collect();
    let mut views: Vec<&String> = schema.views.keys().collect();
    views.sort();
    for view in views {
        rows.push(vec![
            text(CATALOG),
            text("public"),
            text(view.as_str()),
            text("VIEW"),
        ]);
    }
    Ok(rows)
}

fn load_table_constraints(conn: &Connection) -> Result<Vec<Vec<Value>>> {
    let schema = conn.current_schema();
    let mut rows = Vec::new();
    for (name, table) in user_tables_sorted(&schema) {
        let Table::BTree(bt) = table.as_ref() else {
            continue;
        };
        for (conname, kind, _) in constraints(name, bt) {
            rows.push(vec![
                text(CATALOG),
                text("public"),
                text(conname),
                text(CATALOG),
                text("public"),
                text(name.as_str()),
                text(kind),
                text("NO"),
                text("NO"),
            ]);
        }
    }
    Ok(rows)
}

fn load_key_column_usage(conn: &Connection) -> Result<Vec<Vec<Value>>> {
    let schema = conn.current_schema();
    let mut rows = Vec::new();
    for (name, table) in user_tables_sorted(&schema) {
        let Table::BTree(bt) = table.as_ref() else {
            continue;
        };
        for (conname, _, cols) in constraints(name, bt) {
            for (i, col) in cols.iter().enumerate() {
                rows.push(vec![
                    text(CATALOG),
                    text("public"),
                    text(conname.clone()),
                    text(CATALOG),
                    text("public"),
                    text(name.as_str()),
                    text(col.clone()),
                    int(i as i64 + 1),
                ]);
            }
        }
    }
    Ok(rows)
}

/// pg_catalog.pg_indexes: every index of every user table, under PostgreSQL's names — the
/// primary key's `<table>_pkey` (also for a primary key the engine keeps as the rowid, which has
/// no index of its own), a UNIQUE constraint's `<table>_<cols>_key`, a created index's own name.
fn load_pg_indexes(conn: &Connection) -> Result<Vec<Vec<Value>>> {
    let schema = conn.current_schema();
    let mut rows = Vec::new();
    for (name, table) in user_tables_sorted(&schema) {
        let Table::BTree(bt) = table.as_ref() else {
            continue;
        };
        let mut seen_pk = false;
        let mut push = |indexname: String, unique: bool, cols: Vec<String>| {
            rows.push(vec![
                text("public"),
                text(name.as_str()),
                text(indexname.clone()),
                Value::Null,
                text(format!(
                    "CREATE {}INDEX {indexname} ON public.{name} USING btree ({})",
                    if unique { "UNIQUE " } else { "" },
                    cols.join(", ")
                )),
            ]);
        };
        for idx in schema.get_indices(name) {
            if idx.ephemeral {
                continue;
            }
            let cols: Vec<String> = idx.columns.iter().map(|c| c.name.clone()).collect();
            let indexname = if idx.name.starts_with("sqlite_autoindex_") {
                let is_pk = bt.unique_sets.iter().any(|u| {
                    u.is_primary_key
                        && u.columns
                            .iter()
                            .map(|c| c.name.as_str())
                            .eq(cols.iter().map(String::as_str))
                }) || (!bt.primary_key_columns.is_empty()
                    && bt
                        .primary_key_columns
                        .iter()
                        .map(|(c, _)| c.as_str())
                        .eq(cols.iter().map(String::as_str)));
                if is_pk {
                    seen_pk = true;
                    format!("{name}_pkey")
                } else {
                    format!("{name}_{}_key", cols.join("_"))
                }
            } else {
                idx.name.clone()
            };
            push(indexname, idx.unique, cols);
        }
        if !seen_pk && !bt.primary_key_columns.is_empty() {
            let cols = bt
                .primary_key_columns
                .iter()
                .map(|(c, _)| c.clone())
                .collect();
            push(format!("{name}_pkey"), true, cols);
        }
    }
    Ok(rows)
}

pub(crate) fn virtual_tables() -> Vec<Arc<VirtualTable>> {
    let views = [
        ViewTable {
            name: "information_schema_columns",
            sql: "CREATE TABLE information_schema_columns (table_catalog TEXT, table_schema TEXT, \
                  table_name TEXT, column_name TEXT, ordinal_position INTEGER, column_default TEXT, \
                  is_nullable TEXT, data_type TEXT, character_maximum_length INTEGER, \
                  numeric_precision INTEGER, numeric_precision_radix INTEGER, numeric_scale INTEGER, \
                  udt_catalog TEXT, udt_schema TEXT, udt_name TEXT)",
            load: load_columns,
        },
        ViewTable {
            name: "information_schema_tables",
            sql: "CREATE TABLE information_schema_tables (table_catalog TEXT, table_schema TEXT, \
                  table_name TEXT, table_type TEXT)",
            load: load_tables,
        },
        ViewTable {
            name: "information_schema_table_constraints",
            sql: "CREATE TABLE information_schema_table_constraints (constraint_catalog TEXT, \
                  constraint_schema TEXT, constraint_name TEXT, table_catalog TEXT, \
                  table_schema TEXT, table_name TEXT, constraint_type TEXT, is_deferrable TEXT, \
                  initially_deferred TEXT)",
            load: load_table_constraints,
        },
        ViewTable {
            name: "pg_indexes",
            sql: "CREATE TABLE pg_indexes (schemaname TEXT, tablename TEXT, indexname TEXT, \
                  tablespace TEXT, indexdef TEXT)",
            load: load_pg_indexes,
        },
        ViewTable {
            name: "information_schema_key_column_usage",
            sql: "CREATE TABLE information_schema_key_column_usage (constraint_catalog TEXT, \
                  constraint_schema TEXT, constraint_name TEXT, table_catalog TEXT, \
                  table_schema TEXT, table_name TEXT, column_name TEXT, ordinal_position INTEGER)",
            load: load_key_column_usage,
        },
    ];
    views
        .into_iter()
        .map(|v| {
            let name = v.name.to_string();
            let sql = v.sql.to_string();
            Arc::new(
                VirtualTable::new_internal(
                    name,
                    sql,
                    VTabKind::VirtualTable,
                    Arc::new(RwLock::new(v)),
                )
                .expect("information_schema virtual table creation should not fail"),
            )
        })
        .collect()
}
