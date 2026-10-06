//! PostgreSQL's types for the result columns the engine cannot type itself: aggregates.
//!
//! The engine types a result column that is a table column, a literal, a cast or an operator over
//! typed operands; it leaves an aggregate untyped. PostgreSQL's aggregate result types follow from
//! the function and its argument's type (docs, "Aggregate Functions"), and both are in the
//! statement's parse, which the frontend already has: so they are read from there, once, as the
//! statement is prepared (wire review 1 item 14: the same type over both protocols, whatever the
//! rows hold).

use crate::catalog::sqlite_type_to_pg_oid;
use turso_core::schema::Schema;
use turso_pg_parser::pg_query::protobuf::{node::Node, Node as PgNode, SelectStmt};
use turso_pg_parser::pg_query::ParseResult;

const INT2: u32 = 21;
const INT4: u32 = 23;
const INT8: u32 = 20;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const NUMERIC: u32 = 1700;

/// For each result column of a plain SELECT (one statement, no set operation, no `*` target), the
/// PostgreSQL type OID of an aggregate over a table column: count is bigint; sum of smallint or
/// integer is bigint, of bigint or numeric numeric, of a float its own type; avg of an integer or
/// numeric is numeric, of a float double precision; min and max are their argument's type. `None`
/// for every other column, which the engine types. Empty when the statement is not such a SELECT.
pub fn aggregate_types(parse: &ParseResult, schema: &Schema) -> Vec<Option<u32>> {
    let [raw] = parse.protobuf.stmts.as_slice() else {
        return Vec::new();
    };
    let Some(Node::SelectStmt(select)) = raw.stmt.as_ref().and_then(|s| s.node.as_ref()) else {
        return Vec::new();
    };
    if select.larg.is_some() || select.rarg.is_some() {
        return Vec::new();
    }
    let mut targets = Vec::with_capacity(select.target_list.len());
    for target in &select.target_list {
        let Some(Node::ResTarget(t)) = target.node.as_ref() else {
            return Vec::new();
        };
        let val = t.val.as_ref().and_then(|v| v.node.as_ref());
        // A `*` expands to columns this list cannot line up with.
        if let Some(Node::ColumnRef(c)) = val {
            if matches!(
                c.fields.last().and_then(|f| f.node.as_ref()),
                Some(Node::AStar(_))
            ) {
                return Vec::new();
            }
        }
        targets.push(val);
    }
    if !targets.iter().any(|v| matches!(v, Some(Node::FuncCall(_)))) {
        return vec![None; targets.len()];
    }
    let tables = from_tables(select);
    targets
        .into_iter()
        .map(|val| match val {
            Some(Node::FuncCall(call)) => aggregate_type(call, &tables, schema),
            _ => None,
        })
        .collect()
}

fn aggregate_type(
    call: &turso_pg_parser::pg_query::protobuf::FuncCall,
    tables: &[(String, String)],
    schema: &Schema,
) -> Option<u32> {
    if call.over.is_some() {
        return None;
    }
    let name = match call.funcname.as_slice() {
        [name] | [_, name] => match name.node.as_ref()? {
            Node::String(s) => s.sval.as_str(),
            _ => return None,
        },
        _ => return None,
    };
    if name == "count" {
        return Some(INT8);
    }
    let [arg] = call.args.as_slice() else {
        return None;
    };
    let arg = column_type(arg, tables, schema)?;
    match (name, arg) {
        ("sum", INT2 | INT4) => Some(INT8),
        ("sum", INT8 | NUMERIC) => Some(NUMERIC),
        ("sum", FLOAT4 | FLOAT8) => Some(arg),
        ("avg", INT2 | INT4 | INT8 | NUMERIC) => Some(NUMERIC),
        ("avg", FLOAT4 | FLOAT8) => Some(FLOAT8),
        ("min" | "max", _) => Some(arg),
        _ => None,
    }
}

/// The type OID of a table column an aggregate reads: `col` when exactly one FROM table has it, or
/// `t.col` by a FROM table's alias or name.
fn column_type(arg: &PgNode, tables: &[(String, String)], schema: &Schema) -> Option<u32> {
    let Some(Node::ColumnRef(c)) = arg.node.as_ref() else {
        return None;
    };
    let names: Vec<&str> = c
        .fields
        .iter()
        .map(|f| match f.node.as_ref() {
            Some(Node::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let declared = |relname: &str, column: &str| -> Option<String> {
        let table = schema.get_btree_table(relname)?;
        let (_, col) = table.get_column(column)?;
        Some(col.ty_str.clone())
    };
    let ty = match names.as_slice() {
        [column] => {
            let mut found = tables.iter().filter_map(|(_, rel)| declared(rel, *column));
            let ty = found.next()?;
            if found.next().is_some() {
                return None;
            }
            ty
        }
        [table, column] => {
            let (_, rel) = tables.iter().find(|(name, _)| name.as_str() == *table)?;
            declared(rel, *column)?
        }
        _ => return None,
    };
    u32::try_from(sqlite_type_to_pg_oid(&ty)).ok()
}

/// The FROM clause's tables, as (the name a column reference qualifies them by, relation name),
/// through joins; only tables of the default schema.
fn from_tables(select: &SelectStmt) -> Vec<(String, String)> {
    fn walk(node: &PgNode, out: &mut Vec<(String, String)>) {
        match node.node.as_ref() {
            Some(Node::RangeVar(rv)) if rv.schemaname.is_empty() || rv.schemaname == "public" => {
                let name = rv
                    .alias
                    .as_ref()
                    .map_or_else(|| rv.relname.clone(), |a| a.aliasname.clone());
                out.push((name, rv.relname.clone()));
            }
            Some(Node::JoinExpr(j)) => {
                for side in [&j.larg, &j.rarg].into_iter().flatten() {
                    walk(side, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for item in &select.from_clause {
        walk(item, &mut out);
    }
    out
}
