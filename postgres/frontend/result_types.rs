//! PostgreSQL's types for the result columns the engine cannot type itself: aggregates.
//!
//! The engine types a result column that is a table column, a literal, a cast or an operator over
//! typed operands; it leaves an aggregate untyped. PostgreSQL's aggregate result types follow from
//! the function and its argument's type (docs, "Aggregate Functions"), and both are in the
//! statement's parse, which the frontend already has: so they are read from there, once, as the
//! statement is prepared (wire review 1 item 14: the same type over both protocols, whatever the
//! rows hold).

use crate::catalog::sqlite_type_to_pg_oid;
use turso_core::schema::{Column, Schema};
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
    let declared = |relname: &str, column: &str| -> Option<Option<u32>> {
        let table = schema.get_btree_table(relname)?;
        let (_, col) = table.get_column(column)?;
        Some(column_oid(col))
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
    ty
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
    /// Every $n compared with an expression no context types: refused (42P18) unless the client
    /// declared its type, which the server reads from Parse (wire review 8 item 7, review 11 item
    /// 1: refused at prepare from the text alone, a declared one could never run).
    pub untyped: std::collections::BTreeSet<u32>,
}

pub use turso_pg_parser::MAX_PARAMETER;

/// The type PostgreSQL infers for each parameter the client did not declare, from its context
/// (docs, "Prepared statements": the parameter's type is inferred from where it is used): the
/// column it is compared with, assigned to (UPDATE ... SET, ON CONFLICT DO UPDATE SET, a multi-column
/// SET) or inserted into; the type of whatever else it is compared with: an aggregate (count(*) is
/// bigint), a scalar function's result, COALESCE / GREATEST / CASE arms, a scalar subquery's
/// column, a derived table's or a CTE's column, a literal, a cast; LIMIT's and OFFSET's bigint (of
/// a set operation too); boolean where a condition stands (WHERE, HAVING, JOIN ON, AND/OR/NOT,
/// CASE WHEN, IS TRUE); text for LIKE and for text functions' arguments; an array of x's type in
/// `x op ANY($n)`. The first context that types a parameter wins. Bind guessed from the value (an
/// integer, a float, a boolean), Describe called every one text (wire review 4 item 3), and the
/// contexts above fell to text, so a value sent as text compared as text against an integer
/// expression (wire review 8 item 7).
///
/// A parameter compared with an expression no context types (a function this walk does not know)
/// is returned apart, in the second set: the server refuses it (42P18) rather than compare it as
/// text, unless the client declared its type (fail closed; review 8 item 7, review 11 item 1). A
/// parameter compared with a column the walk cannot resolve (a relation it does not model) is left
/// untyped (text), as before.
pub fn parameter_types(
    parse: &ParseResult,
    schema: &Schema,
) -> (
    std::collections::BTreeMap<u32, u32>,
    std::collections::BTreeSet<u32>,
) {
    let mut infer = Infer {
        schema,
        types: std::collections::BTreeMap::new(),
        compared_untyped: std::collections::BTreeSet::new(),
        ctes: Vec::new(),
    };
    if let [raw] = parse.protobuf.stmts.as_slice() {
        if let Some(stmt) = raw.stmt.as_deref() {
            infer.statement(stmt);
        }
    }
    let untyped = infer
        .compared_untyped
        .iter()
        .filter(|n| !infer.types.contains_key(n))
        .copied()
        .collect();
    (infer.types, untyped)
}

/// A relation a column reference can name, by the name references qualify it by: a table (its
/// columns read from the schema), or a derived table or CTE (its columns' types as its query gives
/// them).
#[derive(Clone)]
enum Rel {
    Table {
        name: String,
        relname: String,
    },
    Derived {
        name: String,
        columns: Vec<(String, Option<u32>)>,
    },
}

impl Rel {
    fn name(&self) -> &str {
        match self {
            Rel::Table { name, .. } | Rel::Derived { name, .. } => name,
        }
    }
}

/// The relations visible at a point of the walk, one level per enclosing SELECT, innermost last: a
/// column reference resolves innermost first, as PostgreSQL resolves a correlated reference.
type Scope = Vec<Vec<Rel>>;

/// A column's type: `Some(Some(t))` resolved, `Some(None)` found with no type the walk knows,
/// `None` not found (or ambiguous).
type Found = Option<Option<u32>>;

struct Infer<'a> {
    schema: &'a Schema,
    types: std::collections::BTreeMap<u32, u32>,
    /// Parameters compared with an expression no context types (see [`parameter_types`]).
    compared_untyped: std::collections::BTreeSet<u32>,
    /// The CTEs in scope, innermost last: name and columns.
    ctes: Vec<(String, Vec<(String, Option<u32>)>)>,
}

/// The parameter number of a bare `$n`.
fn param(node: &PgNode) -> Option<u32> {
    match node.node.as_ref() {
        Some(Node::ParamRef(p)) => u32::try_from(p.number).ok(),
        _ => None,
    }
}

/// The function name of a call, without a pg_catalog qualifier, lowercased.
fn func_name(call: &turso_pg_parser::pg_query::protobuf::FuncCall) -> Option<String> {
    let names: Vec<&str> = call
        .funcname
        .iter()
        .filter_map(|n| match &n.node {
            Some(Node::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect();
    match names.as_slice() {
        [name] | ["pg_catalog", name] => Some(name.to_ascii_lowercase()),
        _ => None,
    }
}

/// Scalar functions whose result type does not depend on their arguments.
fn scalar_result(name: &str) -> Option<u32> {
    Some(match name {
        "length" | "char_length" | "character_length" | "octet_length" | "bit_length"
        | "position" | "strpos" | "ascii" | "array_length" | "cardinality" => INT4,
        "lower" | "upper" | "btrim" | "ltrim" | "rtrim" | "trim" | "substr" | "substring"
        | "replace" | "concat" | "concat_ws" | "left" | "right" | "lpad" | "rpad" | "repeat"
        | "reverse" | "md5" | "initcap" | "translate" | "split_part" | "chr" | "to_char"
        | "quote_ident" | "quote_literal" | "format" | "current_schema" | "current_database"
        | "version" => TEXT,
        "row_number" | "rank" | "dense_rank" | "ntile" => INT8,
        "percent_rank" | "cume_dist" => FLOAT8,
        "now" | "statement_timestamp" | "transaction_timestamp" | "clock_timestamp" => 1184,
        _ => return None,
    })
}

/// Scalar functions whose arguments are text: an undeclared parameter passed to one is text.
fn takes_text(name: &str) -> bool {
    matches!(
        name,
        "length"
            | "char_length"
            | "character_length"
            | "octet_length"
            | "lower"
            | "upper"
            | "btrim"
            | "ltrim"
            | "rtrim"
            | "initcap"
            | "md5"
            | "reverse"
            | "ascii"
            | "strpos"
            | "replace"
            | "split_part"
            | "translate"
    )
}

/// The wider of two numeric types (PostgreSQL's arithmetic promotes int to numeric to float8);
/// otherwise the first.
fn wider(a: u32, b: u32) -> u32 {
    let rank = |t: u32| match t {
        INT2 => 1,
        INT4 => 2,
        INT8 => 3,
        NUMERIC => 4,
        FLOAT4 => 5,
        FLOAT8 => 6,
        _ => 0,
    };
    if rank(a) > 0 && rank(b) > rank(a) {
        b
    } else {
        a
    }
}

impl Infer<'_> {
    /// Type `node` as `ty` if it is a parameter its first context has not typed yet.
    fn set(&mut self, node: &PgNode, ty: Option<u32>) {
        if let (Some(n), Some(ty)) = (param(node), ty) {
            self.types.entry(n).or_insert(ty);
        }
    }

    /// `node` stands where a condition stands: a bare parameter there is boolean.
    fn qual(&mut self, node: Option<&PgNode>, scope: &Scope) {
        if let Some(node) = node {
            self.set(node, Some(BOOL));
            self.expr(node, scope);
        }
    }

    /// `a` and `b` are compared (or are the two sides of `a op b`): each parameter takes the
    /// other side's type; a parameter whose other side has none it can be given is noted (see
    /// [`parameter_types`]).
    fn compare(&mut self, a: &PgNode, b: &PgNode, scope: &Scope) {
        let (ta, tb) = (self.type_of(a, scope), self.type_of(b, scope));
        self.set(a, tb);
        self.set(b, ta);
        for (side, other, other_type) in [(a, b, tb), (b, a, ta)] {
            if let Some(n) = param(side) {
                if other_type.is_none() && self.untypable(other, scope) {
                    self.compared_untyped.insert(n);
                }
            }
        }
    }

    /// Whether `node` is an expression no context types, as opposed to a column of a relation the
    /// walk does not model (which keeps the text fallback) or another parameter that a later
    /// context may type.
    fn untypable(&self, node: &PgNode, scope: &Scope) -> bool {
        match node.node.as_ref() {
            Some(Node::ColumnRef(_)) => self.column(node, scope).is_some(),
            Some(Node::ParamRef(_)) => false,
            _ => true,
        }
    }

    fn statement(&mut self, stmt: &PgNode) {
        let scope: Scope = Vec::new();
        match stmt.node.as_ref() {
            Some(Node::SelectStmt(s)) => {
                self.select(s, &scope);
            }
            Some(Node::InsertStmt(i)) => {
                let ctes = self.with(i.with_clause.as_ref(), &scope);
                self.insert(i, &scope);
                self.ctes.truncate(ctes);
            }
            Some(Node::UpdateStmt(u)) => {
                let ctes = self.with(u.with_clause.as_ref(), &scope);
                if let Some(rel) = u.relation.as_ref() {
                    let mut level = vec![self.range_var(rel)];
                    level.extend(self.from_items(&u.from_clause, &scope));
                    let inner = vec![level];
                    self.set_targets(&u.target_list, &rel.relname, &inner);
                    self.qual(u.where_clause.as_deref(), &inner);
                    self.targets(&u.returning_list, &inner);
                }
                self.ctes.truncate(ctes);
            }
            Some(Node::DeleteStmt(d)) => {
                let ctes = self.with(d.with_clause.as_ref(), &scope);
                if let Some(rel) = d.relation.as_ref() {
                    let mut level = vec![self.range_var(rel)];
                    level.extend(self.from_items(&d.using_clause, &scope));
                    let inner = vec![level];
                    self.qual(d.where_clause.as_deref(), &inner);
                    self.targets(&d.returning_list, &inner);
                }
                self.ctes.truncate(ctes);
            }
            _ => {}
        }
    }

    /// Walk a WITH clause and bring its CTEs into scope; returns the depth to truncate back to.
    fn with(
        &mut self,
        with: Option<&turso_pg_parser::pg_query::protobuf::WithClause>,
        scope: &Scope,
    ) -> usize {
        let depth = self.ctes.len();
        let recursive = with.is_some_and(|w| w.recursive);
        for cte in with.map_or(&[][..], |w| w.ctes.as_slice()) {
            let Some(Node::CommonTableExpr(c)) = cte.node.as_ref() else {
                continue;
            };
            let columns = match c.ctequery.as_deref().and_then(|q| q.node.as_ref()) {
                // WITH RECURSIVE: the set operation's left arm, the non-recursive term, types the
                // CTE's columns, as PostgreSQL types them; the CTE is in scope for the right arm,
                // which references it, and for the set operation's LIMIT and OFFSET. Walked whole
                // before it was in scope, the self-reference was untyped, its $n bound as text,
                // and `x < $1` never ended the recursion (wire review 11 item 2).
                Some(Node::SelectStmt(s)) if recursive => {
                    match (s.larg.as_deref(), s.rarg.as_deref()) {
                        (Some(l), Some(r)) => {
                            let columns = self.select(l, scope);
                            self.ctes.push((
                                c.ctename.clone(),
                                rename(columns.clone(), &c.aliascolnames),
                            ));
                            self.select(r, scope);
                            for limit in [s.limit_count.as_deref(), s.limit_offset.as_deref()]
                                .into_iter()
                                .flatten()
                            {
                                self.set(limit, Some(INT8));
                                self.expr(limit, scope);
                            }
                            self.ctes.pop();
                            columns
                        }
                        _ => self.select(s, scope),
                    }
                }
                Some(Node::SelectStmt(s)) => self.select(s, scope),
                _ => Vec::new(),
            };
            let columns = rename(columns, &c.aliascolnames);
            self.ctes.push((c.ctename.clone(), columns));
        }
        depth
    }

    /// A target relation, or a FROM table: a CTE of its name when one is in scope, else the table.
    fn range_var(&self, rv: &turso_pg_parser::pg_query::protobuf::RangeVar) -> Rel {
        let name = rv
            .alias
            .as_ref()
            .map_or_else(|| rv.relname.clone(), |a| a.aliasname.clone());
        if rv.schemaname.is_empty() {
            if let Some((_, columns)) = self.ctes.iter().rev().find(|(n, _)| *n == rv.relname) {
                let columns = match &rv.alias {
                    Some(a) => rename(columns.clone(), &a.colnames),
                    None => columns.clone(),
                };
                return Rel::Derived { name, columns };
            }
        }
        Rel::Table {
            name,
            relname: rv.relname.clone(),
        }
    }

    /// The relations a list of FROM items brings into scope, walking the subqueries, functions and
    /// join conditions among them (the subqueries with the enclosing scope, for LATERAL ones).
    fn from_items(&mut self, items: &[PgNode], scope: &Scope) -> Vec<Rel> {
        let mut level = Vec::new();
        for item in items {
            self.from_item(item, scope, &mut level);
        }
        level
    }

    fn from_item(&mut self, item: &PgNode, scope: &Scope, level: &mut Vec<Rel>) {
        match item.node.as_ref() {
            Some(Node::RangeVar(rv)) => level.push(self.range_var(rv)),
            Some(Node::JoinExpr(j)) => {
                for side in [j.larg.as_deref(), j.rarg.as_deref()].into_iter().flatten() {
                    self.from_item(side, scope, level);
                }
                let mut inner = scope.clone();
                inner.push(level.clone());
                self.qual(j.quals.as_deref(), &inner);
            }
            Some(Node::RangeSubselect(r)) => {
                let columns = match r.subquery.as_deref().and_then(|s| s.node.as_ref()) {
                    Some(Node::SelectStmt(s)) => self.select(s, scope),
                    _ => Vec::new(),
                };
                if let Some(a) = &r.alias {
                    level.push(Rel::Derived {
                        name: a.aliasname.clone(),
                        columns: rename(columns, &a.colnames),
                    });
                }
            }
            Some(Node::RangeFunction(f)) => {
                for item in &f.functions {
                    self.expr(item, scope);
                }
            }
            _ => {}
        }
    }

    /// INSERT: each VALUES item, or each item of the SELECT's target list, takes the type of the
    /// column it goes into (the column list's, or the table's in order); ON CONFLICT DO UPDATE's
    /// SET takes its columns' types and its WHERE is a condition over the table and EXCLUDED.
    fn insert(&mut self, insert: &turso_pg_parser::pg_query::protobuf::InsertStmt, scope: &Scope) {
        let Some(rel) = insert.relation.as_ref() else {
            return;
        };
        let columns: Vec<Option<u32>> = if insert.cols.is_empty() {
            self.schema
                .get_btree_table(&rel.relname)
                .map(|t| t.columns().iter().map(column_oid).collect())
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
            self.select(s, scope);
        }
        let target = vec![vec![
            self.range_var(rel),
            Rel::Table {
                name: "excluded".to_string(),
                relname: rel.relname.clone(),
            },
        ]];
        if let Some(c) = &insert.on_conflict_clause {
            self.set_targets(&c.target_list, &rel.relname, &target);
            self.qual(c.where_clause.as_deref(), &target);
        }
        self.targets(&insert.returning_list, &target);
    }

    /// UPDATE-style SET targets of `relname`: each value takes its column's type, a multi-column
    /// `SET (a, b) = ($1, $2)` element by element.
    fn set_targets(&mut self, targets: &[PgNode], relname: &str, scope: &Scope) {
        for target in targets {
            let Some(Node::ResTarget(t)) = target.node.as_ref() else {
                continue;
            };
            let Some(val) = t.val.as_deref() else {
                continue;
            };
            let column = declared_type(self.schema, relname, &t.name);
            if let Some(Node::MultiAssignRef(m)) = val.node.as_ref() {
                if let Some(Node::RowExpr(row)) = m.source.as_deref().and_then(|s| s.node.as_ref())
                {
                    if let Some(item) = usize::try_from(m.colno - 1)
                        .ok()
                        .and_then(|i| row.args.get(i))
                    {
                        self.set(item, column);
                        self.expr(item, scope);
                    }
                }
                continue;
            }
            self.set(val, column);
            self.expr(val, scope);
        }
    }

    /// A SELECT, in `scope` (the enclosing statements' relations, for correlated references).
    /// Returns its result columns' names and types, for a derived table or a CTE over it.
    fn select(&mut self, s: &SelectStmt, scope: &Scope) -> Vec<(String, Option<u32>)> {
        let ctes = self.with(s.with_clause.as_ref(), scope);
        let columns = if let (Some(l), Some(r)) = (s.larg.as_deref(), s.rarg.as_deref()) {
            // A set operation: its arms, then its own LIMIT, OFFSET and ORDER BY.
            let columns = self.select(l, scope);
            self.select(r, scope);
            for limit in [s.limit_count.as_deref(), s.limit_offset.as_deref()]
                .into_iter()
                .flatten()
            {
                self.set(limit, Some(INT8));
                self.expr(limit, scope);
            }
            columns
        } else {
            let level = self.from_items(&s.from_clause, scope);
            let mut inner = scope.clone();
            inner.push(level);
            let columns = self.targets(&s.target_list, &inner);
            self.qual(s.where_clause.as_deref(), &inner);
            for g in &s.group_clause {
                self.expr(g, &inner);
            }
            self.qual(s.having_clause.as_deref(), &inner);
            for w in &s.window_clause {
                self.window(w, &inner);
            }
            for sort in &s.sort_clause {
                if let Some(Node::SortBy(b)) = sort.node.as_ref() {
                    if let Some(n) = b.node.as_deref() {
                        self.expr(n, &inner);
                    }
                }
            }
            for limit in [s.limit_count.as_deref(), s.limit_offset.as_deref()]
                .into_iter()
                .flatten()
            {
                self.set(limit, Some(INT8));
                self.expr(limit, &inner);
            }
            for row in &s.values_lists {
                self.expr(row, &inner);
            }
            if !s.values_lists.is_empty() {
                // VALUES: the first row's types name its columns column1, column2, ...
                if let Some(Node::List(first)) =
                    s.values_lists.first().and_then(|r| r.node.as_ref())
                {
                    first
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| (format!("column{}", i + 1), self.type_of(item, &inner)))
                        .collect()
                } else {
                    columns
                }
            } else {
                columns
            }
        };
        self.ctes.truncate(ctes);
        columns
    }

    fn window(&mut self, w: &PgNode, scope: &Scope) {
        if let Some(Node::WindowDef(d)) = w.node.as_ref() {
            for p in &d.partition_clause {
                self.expr(p, scope);
            }
            for o in &d.order_clause {
                if let Some(Node::SortBy(b)) = o.node.as_ref() {
                    if let Some(n) = b.node.as_deref() {
                        self.expr(n, scope);
                    }
                }
            }
        }
    }

    /// Walk a target list; returns each target's name and type.
    fn targets(&mut self, targets: &[PgNode], scope: &Scope) -> Vec<(String, Option<u32>)> {
        let mut columns = Vec::with_capacity(targets.len());
        for target in targets {
            if let Some(Node::ResTarget(t)) = target.node.as_ref() {
                if let Some(val) = t.val.as_deref() {
                    self.expr(val, scope);
                    let name = if t.name.is_empty() {
                        target_name(val)
                    } else {
                        t.name.clone()
                    };
                    columns.push((name, self.type_of(val, scope)));
                }
            }
        }
        columns
    }

    fn exprs(&mut self, node: Option<&PgNode>, scope: &Scope) {
        if let Some(node) = node {
            self.expr(node, scope);
        }
    }

    fn expr(&mut self, node: &PgNode, scope: &Scope) {
        use turso_pg_parser::pg_query::protobuf::{AExprKind, SubLinkType};
        match node.node.as_ref() {
            Some(Node::AExpr(e)) => {
                let (Some(l), Some(r)) = (e.lexpr.as_deref(), e.rexpr.as_deref()) else {
                    // A prefix operator (-$1, NOT handled as BoolExpr): its operand's own walk.
                    self.exprs(e.lexpr.as_deref(), scope);
                    self.exprs(e.rexpr.as_deref(), scope);
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
                    let left = self.type_of(l, scope);
                    self.set(r, left.and_then(array_of));
                    let right = self.type_of(r, scope);
                    self.set(l, right.and_then(element_of));
                } else if let Some(Node::List(list)) = r.node.as_ref() {
                    // IN (...) and BETWEEN: each item is compared with the left side.
                    for item in &list.items {
                        self.compare(l, item, scope);
                    }
                } else {
                    self.compare(l, r, scope);
                }
                self.expr(l, scope);
                self.expr(r, scope);
            }
            Some(Node::TypeCast(c)) => {
                if let Some(arg) = c.arg.as_deref() {
                    self.set(arg, cast_type(c));
                    self.expr(arg, scope);
                }
            }
            Some(Node::List(l)) => {
                for item in &l.items {
                    self.expr(item, scope);
                }
            }
            Some(Node::RowExpr(r)) => {
                for arg in &r.args {
                    self.expr(arg, scope);
                }
            }
            Some(Node::BoolExpr(b)) => {
                for arg in &b.args {
                    self.qual(Some(arg), scope);
                }
            }
            Some(Node::BooleanTest(b)) => self.qual(b.arg.as_deref(), scope),
            Some(Node::NullTest(n)) => self.exprs(n.arg.as_deref(), scope),
            Some(Node::FuncCall(f)) => {
                let name = func_name(f);
                if name.as_deref().is_some_and(takes_text) {
                    for arg in &f.args {
                        self.set(arg, Some(TEXT));
                    }
                }
                for arg in &f.args {
                    self.expr(arg, scope);
                }
                if let Some(w) = &f.over {
                    for p in &w.partition_clause {
                        self.expr(p, scope);
                    }
                }
                self.exprs(f.agg_filter.as_deref(), scope);
            }
            Some(Node::CoalesceExpr(c)) => {
                let first = c.args.iter().find_map(|a| self.type_of(a, scope));
                for arg in &c.args {
                    self.set(arg, first);
                    self.expr(arg, scope);
                }
            }
            Some(Node::MinMaxExpr(m)) => {
                let first = m.args.iter().find_map(|a| self.type_of(a, scope));
                for arg in &m.args {
                    self.set(arg, first);
                    self.expr(arg, scope);
                }
            }
            Some(Node::CaseExpr(c)) => {
                let whens: Vec<_> = c
                    .args
                    .iter()
                    .filter_map(|w| match w.node.as_ref() {
                        Some(Node::CaseWhen(w)) => Some(w),
                        _ => None,
                    })
                    .collect();
                match c.arg.as_deref() {
                    // `CASE x WHEN v ...`: each v is compared with x.
                    Some(arg) => {
                        for w in &whens {
                            if let Some(v) = w.expr.as_deref() {
                                self.compare(arg, v, scope);
                            }
                        }
                        self.expr(arg, scope);
                    }
                    // `CASE WHEN cond ...`: each cond is a condition.
                    None => {
                        for w in &whens {
                            self.qual(w.expr.as_deref(), scope);
                        }
                    }
                }
                // The results share the first typed one's type.
                let result = whens
                    .iter()
                    .filter_map(|w| w.result.as_deref())
                    .chain(c.defresult.as_deref())
                    .find_map(|r| self.type_of(r, scope));
                for r in whens
                    .iter()
                    .filter_map(|w| w.result.as_deref())
                    .chain(c.defresult.as_deref())
                {
                    self.set(r, result);
                    self.expr(r, scope);
                }
            }
            Some(Node::SubLink(l)) => {
                let kind = SubLinkType::try_from(l.sub_link_type).unwrap_or(SubLinkType::Undefined);
                let columns = match l.subselect.as_deref().and_then(|s| s.node.as_ref()) {
                    Some(Node::SelectStmt(s)) => self.select(s, scope),
                    _ => Vec::new(),
                };
                if let Some(t) = l.testexpr.as_deref() {
                    // `x IN (SELECT c ...)`, `x op ANY (SELECT c ...)`: x is compared with c.
                    if matches!(kind, SubLinkType::AnySublink | SubLinkType::AllSublink) {
                        if let [(_, ty)] = columns.as_slice() {
                            self.set(t, *ty);
                        }
                    }
                    self.expr(t, scope);
                }
            }
            Some(Node::CollateClause(c)) => self.exprs(c.arg.as_deref(), scope),
            Some(Node::AIndirection(i)) => self.exprs(i.arg.as_deref(), scope),
            _ => {}
        }
    }

    /// A column reference's type: `t.c` from the innermost level naming t, a bare `c` from the
    /// innermost level holding it (ambiguous within one level: none).
    fn column(&self, node: &PgNode, scope: &Scope) -> Found {
        let Some(Node::ColumnRef(c)) = node.node.as_ref() else {
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
        let of = |rel: &Rel, column: &str| -> Found {
            match rel {
                Rel::Table { relname, .. } => {
                    let table = self.schema.get_table(relname)?;
                    let col = table.columns().iter().find(|col| {
                        col.name
                            .as_deref()
                            .is_some_and(|n| n.eq_ignore_ascii_case(column))
                    })?;
                    Some(column_oid(col))
                }
                Rel::Derived { columns, .. } => columns
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(column))
                    .map(|(_, t)| *t),
            }
        };
        match names.as_slice() {
            [column] => {
                for level in scope.iter().rev() {
                    let mut found = level.iter().filter_map(|rel| of(rel, column));
                    if let Some(ty) = found.next() {
                        return found.next().is_none().then_some(ty);
                    }
                }
                None
            }
            [table, column] => {
                for level in scope.iter().rev() {
                    if let Some(rel) = level.iter().find(|rel| rel.name() == *table) {
                        return of(rel, column);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// The type `node` has where a parameter is compared with it, if the parse says.
    fn type_of(&self, node: &PgNode, scope: &Scope) -> Option<u32> {
        use turso_pg_parser::pg_query::protobuf::a_const::Val;
        use turso_pg_parser::pg_query::protobuf::{AExprKind, SubLinkType};
        match node.node.as_ref()? {
            Node::ColumnRef(_) => self.column(node, scope).flatten(),
            Node::FuncCall(call) => {
                let name = func_name(call)?;
                if let Some(t) = scalar_result(&name) {
                    return Some(t);
                }
                if name == "count" {
                    return Some(INT8);
                }
                let first = call.args.first().and_then(|a| self.type_of(a, scope));
                match (name.as_str(), first?) {
                    ("sum", INT2 | INT4) => Some(INT8),
                    ("sum", INT8 | NUMERIC) => Some(NUMERIC),
                    ("sum", t @ (FLOAT4 | FLOAT8)) => Some(t),
                    ("avg", INT2 | INT4 | INT8 | NUMERIC) => Some(NUMERIC),
                    ("avg", FLOAT4 | FLOAT8) => Some(FLOAT8),
                    ("min" | "max" | "abs" | "sign", t) => Some(t),
                    ("ceil" | "ceiling" | "floor" | "round" | "trunc", t @ (NUMERIC | FLOAT8)) => {
                        Some(t)
                    }
                    _ => None,
                }
            }
            Node::AConst(c) => match c.val.as_ref()? {
                Val::Ival(_) => Some(INT4),
                Val::Fval(_) => Some(NUMERIC),
                Val::Boolval(_) => Some(BOOL),
                // An untyped literal resolves as text against an undeclared parameter.
                Val::Sval(_) => Some(TEXT),
                Val::Bsval(_) => None,
            },
            Node::TypeCast(c) => cast_type(c),
            Node::ParamRef(p) => u32::try_from(p.number)
                .ok()
                .and_then(|n| self.types.get(&n).copied()),
            Node::AExpr(e) => {
                let kind = AExprKind::try_from(e.kind).unwrap_or(AExprKind::Undefined);
                let op = e.name.iter().find_map(|n| match &n.node {
                    Some(Node::String(s)) => Some(s.sval.as_str()),
                    _ => None,
                });
                let comparison = !matches!(kind, AExprKind::AexprOp)
                    || matches!(op, Some("=" | "<>" | "!=" | "<" | "<=" | ">" | ">="));
                if comparison && !matches!(kind, AExprKind::AexprNullif) {
                    return Some(BOOL);
                }
                if op == Some("||") {
                    return Some(TEXT);
                }
                let l = e.lexpr.as_deref().and_then(|l| self.type_of(l, scope));
                let r = e.rexpr.as_deref().and_then(|r| self.type_of(r, scope));
                match (l, r) {
                    (Some(a), Some(b)) => Some(wider(a, b)),
                    (a, b) => a.or(b),
                }
            }
            Node::BoolExpr(_) | Node::NullTest(_) | Node::BooleanTest(_) => Some(BOOL),
            Node::CoalesceExpr(c) => c.args.iter().find_map(|a| self.type_of(a, scope)),
            Node::MinMaxExpr(m) => m.args.iter().find_map(|a| self.type_of(a, scope)),
            Node::CaseExpr(c) => c
                .args
                .iter()
                .filter_map(|w| match w.node.as_ref() {
                    Some(Node::CaseWhen(w)) => w.result.as_deref(),
                    _ => None,
                })
                .chain(c.defresult.as_deref())
                .find_map(|r| self.type_of(r, scope)),
            Node::SubLink(l) => {
                let kind = SubLinkType::try_from(l.sub_link_type).unwrap_or(SubLinkType::Undefined);
                match kind {
                    SubLinkType::ExprSublink => {
                        let Some(Node::SelectStmt(s)) =
                            l.subselect.as_deref().and_then(|s| s.node.as_ref())
                        else {
                            return None;
                        };
                        self.subselect_type(s, scope)
                    }
                    SubLinkType::ExistsSublink
                    | SubLinkType::AnySublink
                    | SubLinkType::AllSublink => Some(BOOL),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// The type of a scalar subquery's one result column, read in its own FROM (not walked:
    /// [`Infer::expr`] walks it).
    fn subselect_type(&self, s: &SelectStmt, scope: &Scope) -> Option<u32> {
        if s.larg.is_some() || s.with_clause.is_some() {
            return None;
        }
        let [target] = s.target_list.as_slice() else {
            return None;
        };
        let Some(Node::ResTarget(t)) = target.node.as_ref() else {
            return None;
        };
        let mut level = Vec::new();
        for item in &s.from_clause {
            if let Some(Node::RangeVar(rv)) = item.node.as_ref() {
                level.push(self.range_var(rv));
            }
        }
        let mut inner = scope.clone();
        inner.push(level);
        self.type_of(t.val.as_deref()?, &inner)
    }
}

/// A target's output name when it has no alias, as PostgreSQL names it: a column reference's last
/// field, a function's name, else "?column?".
fn target_name(val: &PgNode) -> String {
    match val.node.as_ref() {
        Some(Node::ColumnRef(c)) => c
            .fields
            .last()
            .and_then(|f| match f.node.as_ref() {
                Some(Node::String(s)) => Some(s.sval.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "?column?".to_string()),
        Some(Node::FuncCall(f)) => func_name(f).unwrap_or_else(|| "?column?".to_string()),
        Some(Node::TypeCast(c)) => c.arg.as_deref().map_or("?column?".to_string(), target_name),
        _ => "?column?".to_string(),
    }
}

/// Columns renamed by an alias's column list (`AS d(a, b)`), the rest keeping their names.
fn rename(mut columns: Vec<(String, Option<u32>)>, names: &[PgNode]) -> Vec<(String, Option<u32>)> {
    for (column, name) in columns.iter_mut().zip(names) {
        if let Some(Node::String(s)) = name.node.as_ref() {
            column.0 = s.sval.clone();
        }
    }
    columns
}

/// The array type of an element type, for the types a column here can have (pg_type's typarray).
fn array_of(element: u32) -> Option<u32> {
    ARRAYS.iter().find(|(e, _)| *e == element).map(|(_, a)| *a)
}

/// The element type of an array type (the inverse of [`array_of`]); Bind reads an array
/// parameter's elements by it, so Bind and Describe use one table.
pub fn element_of(array: u32) -> Option<u32> {
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
    column_oid(col)
}

/// The type OID of a table column as declared: its type name's, or for an array column the array
/// type of it (the dimensions are kept apart from the name: an int[] column's is INTEGER), so
/// `$1 = ANY(xs)` takes xs's element type, as in PostgreSQL. Read from the name alone, an int[]
/// column was int4, its element untyped, and $1 text, which matched no integer element (wire
/// review 10 item 3). None for an array of a type with no array type here.
fn column_oid(col: &Column) -> Option<u32> {
    let base = u32::try_from(sqlite_type_to_pg_oid(&col.ty_str)).ok()?;
    if col.array_dimensions() > 0 {
        array_of(base)
    } else {
        Some(base)
    }
}
