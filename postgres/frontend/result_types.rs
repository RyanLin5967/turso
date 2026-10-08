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
    from_items(&select.from_clause)
}

/// [`from_tables`] of a list of FROM items (a SELECT's FROM, UPDATE's FROM, DELETE's USING).
fn from_items(items: &[PgNode]) -> Vec<(String, String)> {
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
    for item in items {
        walk(item, &mut out);
    }
    out
}

const BOOL: u32 = 16;
const TEXT: u32 = 25;

/// The types a statement's parse gives: its result columns' (see [`aggregate_types`]) and its
/// parameters' (see [`parameter_types`]).
#[derive(Debug, Clone, Default)]
pub struct StatementTypes {
    pub columns: Vec<Option<u32>>,
    /// The type OID of each $n its context types, by n; one missing is untyped (text, as
    /// PostgreSQL resolves a parameter nothing types). Sparse, so a statement's highest $n sizes
    /// nothing (wire review 8 item 3).
    pub params: std::collections::BTreeMap<u32, u32>,
    /// Every $n the statement's parse tree holds, sorted, each in 1..=MAX_PARAMETER (see
    /// turso_pg_parser::param_numbers): the parameter count and the gaps are read from this, not
    /// from the engine's slots, which a clause it folds away does not get (wire review 8 item 5).
    pub used: Vec<u32>,
}

/// The highest parameter number a statement may hold: Bind counts parameters in 16 bits.
pub const MAX_PARAMETER: u32 = 65535;

/// The type PostgreSQL infers for each parameter the client did not declare, from its context
/// (docs, "Prepared statements": the parameter's type is inferred from where it is used): the
/// column it is compared with, assigned to (UPDATE ... SET) or inserted into; an aggregate it is
/// compared with (count(*) is bigint); a literal it is compared with; a cast's type; LIMIT's and
/// OFFSET's bigint; text for LIKE. The first context that types a parameter wins. Bind guessed
/// from the value instead (an integer, a float, a boolean), and Describe called every one text
/// (wire review 4 item 3).
pub fn parameter_types(
    parse: &ParseResult,
    schema: &Schema,
) -> std::collections::BTreeMap<u32, u32> {
    let mut infer = Infer {
        schema,
        types: std::collections::BTreeMap::new(),
    };
    if let [raw] = parse.protobuf.stmts.as_slice() {
        if let Some(stmt) = raw.stmt.as_deref() {
            infer.statement(stmt);
        }
    }
    infer.types
}

struct Infer<'a> {
    schema: &'a Schema,
    types: std::collections::BTreeMap<u32, u32>,
}

impl Infer<'_> {
    /// Type `node` as `ty` if it is a parameter its first context has not typed yet.
    fn set(&mut self, node: &PgNode, ty: Option<u32>) {
        let (Some(Node::ParamRef(p)), Some(ty)) = (node.node.as_ref(), ty) else {
            return;
        };
        let Ok(n) = u32::try_from(p.number) else {
            return;
        };
        self.types.entry(n).or_insert(ty);
    }

    fn statement(&mut self, stmt: &PgNode) {
        match stmt.node.as_ref() {
            Some(Node::SelectStmt(s)) => self.select(s, &[]),
            Some(Node::InsertStmt(i)) => self.insert(i),
            Some(Node::UpdateStmt(u)) => {
                let Some(rel) = u.relation.as_ref() else {
                    return;
                };
                let mut tables = vec![relation_name(rel)];
                tables.extend(from_items(&u.from_clause));
                for target in &u.target_list {
                    if let Some(Node::ResTarget(t)) = target.node.as_ref() {
                        if let Some(val) = t.val.as_deref() {
                            self.set(val, declared_type(self.schema, &rel.relname, &t.name));
                            self.expr(val, &tables);
                        }
                    }
                }
                self.exprs(u.where_clause.as_deref(), &tables);
                self.targets(&u.returning_list, &tables);
            }
            Some(Node::DeleteStmt(d)) => {
                let Some(rel) = d.relation.as_ref() else {
                    return;
                };
                let mut tables = vec![relation_name(rel)];
                tables.extend(from_items(&d.using_clause));
                self.exprs(d.where_clause.as_deref(), &tables);
                self.targets(&d.returning_list, &tables);
            }
            _ => {}
        }
    }

    /// INSERT: each VALUES item, or each item of the SELECT's target list, takes the type of the
    /// column it goes into (the column list's, or the table's in order).
    fn insert(&mut self, insert: &turso_pg_parser::pg_query::protobuf::InsertStmt) {
        let Some(rel) = insert.relation.as_ref() else {
            return;
        };
        let columns: Vec<Option<u32>> = if insert.cols.is_empty() {
            self.schema
                .get_btree_table(&rel.relname)
                .map(|t| {
                    t.columns()
                        .iter()
                        .map(|c| u32::try_from(sqlite_type_to_pg_oid(&c.ty_str)).ok())
                        .collect()
                })
                .unwrap_or_default()
        } else {
            insert
                .cols
                .iter()
                .map(|c| match c.node.as_ref() {
                    Some(Node::ResTarget(t)) => declared_type(self.schema, &rel.relname, &t.name),
                    _ => None,
                })
                .collect()
        };
        let tables = vec![relation_name(rel)];
        if let Some(Node::SelectStmt(s)) =
            insert.select_stmt.as_deref().and_then(|s| s.node.as_ref())
        {
            for row in &s.values_lists {
                if let Some(Node::List(l)) = row.node.as_ref() {
                    for (item, ty) in l.items.iter().zip(&columns) {
                        self.set(item, *ty);
                    }
                }
            }
            if s.values_lists.is_empty() {
                for (target, ty) in s.target_list.iter().zip(&columns) {
                    if let Some(Node::ResTarget(t)) = target.node.as_ref() {
                        if let Some(val) = t.val.as_deref() {
                            self.set(val, *ty);
                        }
                    }
                }
            }
            self.select(s, &[]);
        }
        self.targets(&insert.returning_list, &tables);
    }

    /// A SELECT, `outer` the FROM tables of the statements it is nested in (a correlated
    /// reference reads them).
    fn select(&mut self, s: &SelectStmt, outer: &[(String, String)]) {
        if let (Some(l), Some(r)) = (s.larg.as_deref(), s.rarg.as_deref()) {
            self.select(l, outer);
            self.select(r, outer);
            return;
        }
        let mut tables = from_tables(s);
        tables.extend(outer.iter().cloned());
        self.targets(&s.target_list, &tables);
        self.exprs(s.where_clause.as_deref(), &tables);
        self.exprs(s.having_clause.as_deref(), &tables);
        for limit in [s.limit_count.as_deref(), s.limit_offset.as_deref()]
            .into_iter()
            .flatten()
        {
            self.set(limit, Some(INT8));
            self.expr(limit, &tables);
        }
        for row in &s.values_lists {
            self.expr(row, &tables);
        }
        for item in &s.from_clause {
            self.from_item(item, &tables);
        }
    }

    fn from_item(&mut self, item: &PgNode, tables: &[(String, String)]) {
        match item.node.as_ref() {
            Some(Node::JoinExpr(j)) => {
                for side in [j.larg.as_deref(), j.rarg.as_deref()].into_iter().flatten() {
                    self.from_item(side, tables);
                }
                self.exprs(j.quals.as_deref(), tables);
            }
            Some(Node::RangeSubselect(r)) => {
                if let Some(Node::SelectStmt(s)) =
                    r.subquery.as_deref().and_then(|s| s.node.as_ref())
                {
                    self.select(s, tables);
                }
            }
            Some(Node::RangeFunction(f)) => {
                for item in &f.functions {
                    self.expr(item, tables);
                }
            }
            _ => {}
        }
    }

    fn targets(&mut self, targets: &[PgNode], tables: &[(String, String)]) {
        for target in targets {
            if let Some(Node::ResTarget(t)) = target.node.as_ref() {
                self.exprs(t.val.as_deref(), tables);
            }
        }
    }

    fn exprs(&mut self, node: Option<&PgNode>, tables: &[(String, String)]) {
        if let Some(node) = node {
            self.expr(node, tables);
        }
    }

    fn expr(&mut self, node: &PgNode, tables: &[(String, String)]) {
        use turso_pg_parser::pg_query::protobuf::AExprKind;
        match node.node.as_ref() {
            Some(Node::AExpr(e)) => {
                let (Some(l), Some(r)) = (e.lexpr.as_deref(), e.rexpr.as_deref()) else {
                    self.exprs(e.lexpr.as_deref(), tables);
                    self.exprs(e.rexpr.as_deref(), tables);
                    return;
                };
                let kind = AExprKind::try_from(e.kind).unwrap_or(AExprKind::Undefined);
                if matches!(
                    kind,
                    AExprKind::AexprLike | AExprKind::AexprIlike | AExprKind::AexprSimilar
                ) {
                    self.set(l, Some(TEXT));
                    self.set(r, Some(TEXT));
                } else if matches!(kind, AExprKind::AexprOpAny | AExprKind::AexprOpAll) {
                    // `x op ANY(r)` / `x op ALL(r)`: r is an ARRAY of x's type, as PostgreSQL types
                    // an undeclared $n there (int4 -> int4[]); it was given the element type, so
                    // '{1,2}' failed at Bind (wire review 8 item 6). A parameter on the left takes
                    // the element type of a typed array on the right.
                    let left = self.type_of(l, tables);
                    self.set(r, left.and_then(array_of));
                    let right = self.type_of(r, tables);
                    self.set(l, right.and_then(element_of));
                } else if let Some(Node::List(list)) = r.node.as_ref() {
                    // IN (...) and BETWEEN: each item takes the left side's type, and a parameter
                    // on the left the first typed item's.
                    let left = self.type_of(l, tables);
                    for item in &list.items {
                        self.set(item, left);
                    }
                    let item_type = list.items.iter().find_map(|i| self.type_of(i, tables));
                    self.set(l, item_type);
                } else {
                    let (lt, rt) = (self.type_of(l, tables), self.type_of(r, tables));
                    self.set(l, rt);
                    self.set(r, lt);
                }
                self.expr(l, tables);
                self.expr(r, tables);
            }
            Some(Node::TypeCast(c)) => {
                if let Some(arg) = c.arg.as_deref() {
                    self.set(arg, cast_type(c));
                    self.expr(arg, tables);
                }
            }
            Some(Node::List(l)) => {
                for item in &l.items {
                    self.expr(item, tables);
                }
            }
            Some(Node::BoolExpr(b)) => {
                for arg in &b.args {
                    self.expr(arg, tables);
                }
            }
            Some(Node::NullTest(n)) => self.exprs(n.arg.as_deref(), tables),
            Some(Node::FuncCall(f)) => {
                for arg in &f.args {
                    self.expr(arg, tables);
                }
            }
            Some(Node::CoalesceExpr(c)) => {
                for arg in &c.args {
                    self.expr(arg, tables);
                }
            }
            Some(Node::CaseExpr(c)) => {
                self.exprs(c.arg.as_deref(), tables);
                for when in &c.args {
                    if let Some(Node::CaseWhen(w)) = when.node.as_ref() {
                        self.exprs(w.expr.as_deref(), tables);
                        self.exprs(w.result.as_deref(), tables);
                    }
                }
                self.exprs(c.defresult.as_deref(), tables);
            }
            Some(Node::SubLink(l)) => {
                self.exprs(l.testexpr.as_deref(), tables);
                if let Some(Node::SelectStmt(s)) =
                    l.subselect.as_deref().and_then(|s| s.node.as_ref())
                {
                    self.select(s, tables);
                }
            }
            _ => {}
        }
    }

    /// The type `node` has where a parameter is compared with it, if the parse says.
    fn type_of(&self, node: &PgNode, tables: &[(String, String)]) -> Option<u32> {
        use turso_pg_parser::pg_query::protobuf::a_const::Val;
        match node.node.as_ref()? {
            Node::ColumnRef(_) => column_type(node, tables, self.schema),
            Node::FuncCall(call) => aggregate_type(call, tables, self.schema),
            Node::AConst(c) => match c.val.as_ref()? {
                Val::Ival(_) => Some(INT4),
                Val::Fval(_) => Some(NUMERIC),
                Val::Boolval(_) => Some(BOOL),
                _ => None,
            },
            Node::TypeCast(c) => cast_type(c),
            Node::ParamRef(p) => u32::try_from(p.number)
                .ok()
                .and_then(|n| self.types.get(&n).copied()),
            // Arithmetic: the type of an operand that has one.
            Node::AExpr(e) => e
                .lexpr
                .as_deref()
                .and_then(|l| self.type_of(l, tables))
                .or_else(|| e.rexpr.as_deref().and_then(|r| self.type_of(r, tables))),
            _ => None,
        }
    }
}

/// The array type of an element type, for the types a column here can have (pg_type's typarray).
fn array_of(element: u32) -> Option<u32> {
    ARRAYS.iter().find(|(e, _)| *e == element).map(|(_, a)| *a)
}

/// The element type of an array type (the inverse of [`array_of`]).
fn element_of(array: u32) -> Option<u32> {
    ARRAYS.iter().find(|(_, a)| *a == array).map(|(e, _)| *e)
}

/// (element type, its array type) from pg_type: bool, bytea, int2, int4, int8, text, varchar,
/// bpchar, float4, float8, numeric, json, jsonb, uuid, date, time, timestamp, timestamptz.
const ARRAYS: [(u32, u32); 18] = [
    (16, 1000),
    (17, 1001),
    (21, 1005),
    (23, 1007),
    (20, 1016),
    (25, 1009),
    (1043, 1015),
    (1042, 1014),
    (700, 1021),
    (701, 1022),
    (1700, 1231),
    (114, 199),
    (3802, 3807),
    (2950, 2951),
    (1082, 1182),
    (1083, 1183),
    (1114, 1115),
    (1184, 1185),
];

/// The type OID of a cast's target type, by its name (no arrays).
fn cast_type(cast: &turso_pg_parser::pg_query::protobuf::TypeCast) -> Option<u32> {
    let type_name = cast.type_name.as_ref()?;
    if !type_name.array_bounds.is_empty() {
        return None;
    }
    let name = match type_name.names.last()?.node.as_ref()? {
        Node::String(s) => s.sval.as_str(),
        _ => return None,
    };
    u32::try_from(sqlite_type_to_pg_oid(name)).ok()
}

/// The type OID of column `column` of table `relname`, as declared.
fn declared_type(schema: &Schema, relname: &str, column: &str) -> Option<u32> {
    let table = schema.get_btree_table(relname)?;
    let (_, col) = table.get_column(column)?;
    u32::try_from(sqlite_type_to_pg_oid(&col.ty_str)).ok()
}

/// A statement's target relation as (the name column references qualify it by, relation name).
fn relation_name(rel: &turso_pg_parser::pg_query::protobuf::RangeVar) -> (String, String) {
    let name = rel
        .alias
        .as_ref()
        .map_or_else(|| rel.relname.clone(), |a| a.aliasname.clone());
    (name, rel.relname.clone())
}
