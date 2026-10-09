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
    /// Set by the caller, not the parse: whether a Bind supplies this statement's parameters (the
    /// extended protocol). A statement performed at prepare is refused when it holds a parameter,
    /// 42P02 without a Bind and 0A000 with one (wire review 13 item 9).
    pub bound: bool,
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
    /// The translated statement is a COMMIT (`commits`) or a ROLLBACK of the whole transaction
    /// (`rolls_back`), whatever its text: the server keys its COMMIT rules on this, never on its own
    /// reading of the text, which `COMMIT<NBSP>` and other spellings it did not know slipped past
    /// (wire review 13 item 1).
    pub commits: bool,
    pub rolls_back: bool,
    /// The table a CREATE TABLE creates with foreign keys: the server resolves every key once the
    /// table exists, in the CREATE's own transaction, and refuses the CREATE (42830) if one does
    /// not (turso_pg::check_table_keys; wire review 13 item 2).
    pub new_table_keys: Option<String>,
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
/// text, unless the client declared its type (fail closed; review 8 item 7, review 11 item 1). So
/// is one compared with a column of a relation the walk cannot open (another schema's, a function
/// in FROM it does not model, a circular view): the column is found with no type, never bound to
/// an outer relation's column of the name (review 11 item 3, review 14 items 1 and 2). A column
/// reference no relation in scope has is left untyped (text).
pub fn parameter_types(
    parse: &ParseResult,
    schema: &std::sync::Arc<Schema>,
    search_path: &[String],
    cache: &std::sync::Mutex<ViewCache>,
) -> (
    std::collections::BTreeMap<u32, u32>,
    std::collections::BTreeSet<u32>,
) {
    let memo = {
        let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
        let same = cache
            .schema
            .as_ref()
            .is_some_and(|s| std::sync::Arc::ptr_eq(s, schema))
            && cache.search_path == search_path;
        if !same {
            cache.schema = Some(schema.clone());
            cache.search_path = search_path.to_vec();
            cache.views.clear();
        }
        std::mem::take(&mut cache.views)
    };
    let view_memo = std::rc::Rc::new(std::cell::RefCell::new(memo));
    let result = parameter_types_with(parse, schema, search_path, &view_memo);
    cache.lock().unwrap_or_else(|e| e.into_inner()).views = view_memo.take();
    result
}

/// Each view's columns as the parameter-type walk read them, for one schema snapshot and search
/// path: a statement over views parsed each view every time it was prepared, and a view reached
/// k times was parsed k times (2^depth for a view joining the one before it twice; wire review 14
/// item 10). Keyed by the snapshot itself, the Arc the connection hands out until DDL replaces it,
/// which the cache holds so that it cannot be mistaken for a later one; a view read where the walk
/// was cut (a cycle, the depth bound) is not kept.
#[derive(Default)]
pub struct ViewCache {
    schema: Option<std::sync::Arc<Schema>>,
    search_path: Vec<String>,
    views: ViewMemo,
}

type ViewMemo = std::collections::HashMap<String, Option<Vec<(String, Option<u32>)>>>;

fn parameter_types_with(
    parse: &ParseResult,
    schema: &Schema,
    search_path: &[String],
    view_memo: &std::rc::Rc<std::cell::RefCell<ViewMemo>>,
) -> (
    std::collections::BTreeMap<u32, u32>,
    std::collections::BTreeSet<u32>,
) {
    let mut infer = Infer {
        schema,
        types: std::collections::BTreeMap::new(),
        compared_untyped: std::collections::BTreeSet::new(),
        ctes: Vec::new(),
        views: Vec::new(),
        subselects: Default::default(),
        search_path: search_path.to_vec(),
        view_memo: view_memo.clone(),
        cuts: Default::default(),
        cut_views: Default::default(),
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
/// columns read from the schema), a derived table, CTE or view (its columns' types as its query
/// gives them; an alias-less subquery's name is empty), or one the walk cannot open (a relation of
/// another schema, one it cannot find or parse, a `*` it cannot expand), whose columns are unknown.
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
    Unknown {
        name: String,
    },
}

impl Rel {
    fn name(&self) -> &str {
        match self {
            Rel::Table { name, .. } | Rel::Derived { name, .. } | Rel::Unknown { name } => name,
        }
    }

    /// A derived relation of these columns, or one the walk cannot open if a `*` among them could
    /// not be expanded ([`STAR`]).
    fn derived(name: String, columns: Vec<(String, Option<u32>)>) -> Rel {
        if columns.iter().any(|(n, _)| n == STAR) {
            Rel::Unknown { name }
        } else {
            Rel::Derived { name, columns }
        }
    }
}

/// The name a target-list `*` keeps when the walk cannot expand it (a relation it cannot open):
/// the columns of whatever derives from that list are then unknown ([`Rel::derived`]).
const STAR: &str = "*";

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
    /// The views being opened, outermost first ([`Infer::view_columns`]).
    views: Vec<String>,
    /// Each scalar subquery's column type once typed, by its SelectStmt's address in this
    /// statement's tree, shared with the walks [`Infer::subselect_type`] starts: each such walk
    /// walks the subquery's FROM, which can hold scalar subqueries of its own, so unshared the
    /// walks would double with each level of nesting.
    subselects: std::rc::Rc<std::cell::RefCell<std::collections::HashMap<usize, Option<u32>>>>,
    /// The session's search path, as SET search_path left it; empty is public alone
    /// ([`Infer::main_relname`]).
    search_path: Vec<String>,
    /// The views read so far for this snapshot ([`ViewCache`]), shared with every nested walk.
    view_memo: std::rc::Rc<std::cell::RefCell<ViewMemo>>,
    /// How many times a view was cut (a cycle, the depth bound), shared with every nested walk: a
    /// view whose reading saw a cut is not memoised for the snapshot.
    cuts: std::rc::Rc<std::cell::Cell<usize>>,
    /// The views whose reading saw a cut, for this statement's walk only, shared with every nested
    /// walk ([`Infer::view_columns`]).
    cut_views: std::rc::Rc<std::cell::RefCell<ViewMemo>>,
}

/// How many views deep the walk opens a view inside a view; a deeper one is a relation it cannot
/// open.
const MAX_VIEW_DEPTH: usize = 32;

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

    /// `node`, if it is a parameter, is one no context types: refused (42P18) unless declared.
    fn refuse_untyped(&mut self, node: &PgNode) {
        if let Some(n) = param(node) {
            self.compared_untyped.insert(n);
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
                    let mut level = vec![self.target(rel)];
                    level.extend(self.from_items(&u.from_clause, &scope));
                    let inner = vec![level];
                    let relname = self.main_relname(&rel.schemaname, &rel.relname);
                    self.set_targets(&u.target_list, relname.as_deref(), &inner);
                    self.qual(u.where_clause.as_deref(), &inner);
                    self.targets(&u.returning_list, &inner);
                }
                self.ctes.truncate(ctes);
            }
            Some(Node::DeleteStmt(d)) => {
                let ctes = self.with(d.with_clause.as_ref(), &scope);
                if let Some(rel) = d.relation.as_ref() {
                    let mut level = vec![self.target(rel)];
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

    /// A target relation, or a FROM table: a CTE of its name when one is in scope, else the table,
    /// else a view (walked from its query), else a relation the walk cannot open. The schemas a
    /// relation here can live in are public (the default) and pg_catalog, whose relations keep
    /// their own names, and information_schema, whose views are information_schema_<view>; a
    /// relation of any other schema is not one this walk can open. The schema was dropped, so `s.t`
    /// was typed as public's t (wire review 11 item 3).
    fn range_var(&self, rv: &turso_pg_parser::pg_query::protobuf::RangeVar) -> Rel {
        self.relation(rv, true)
    }

    /// The table an INSERT, UPDATE or DELETE writes: a range variable read with no CTE lookup, as
    /// PostgreSQL never resolves a DML target to a CTE; one of a CTE's name was typed from the CTE
    /// (wire review 14 item 3).
    fn target(&self, rv: &turso_pg_parser::pg_query::protobuf::RangeVar) -> Rel {
        self.relation(rv, false)
    }

    /// The main schema's name for the relation `schemaname.relname` names, as the engine resolves
    /// it: public's and pg_catalog's relations keep their names and information_schema's views are
    /// information_schema_<view>; an unqualified name is read along the session's search path,
    /// public's relation of the name if public comes before any other schema there, or the name as
    /// is when no path schema has it. None for a relation of any other schema, and for an
    /// unqualified name a schema other than public would be searched for first: that is an
    /// attached schema, which this walk cannot read, so the relation is one it cannot open. Every
    /// relation was read in public alone, so a parameter into s.t, or into t after `SET
    /// search_path TO s, public`, was typed from public.t and stored as public's type (wire review
    /// 14 item 3).
    fn main_relname(&self, schemaname: &str, relname: &str) -> Option<String> {
        match schemaname.to_lowercase().as_str() {
            "public" | "pg_catalog" => Some(relname.to_string()),
            "information_schema" => Some(format!("information_schema_{}", relname.to_lowercase())),
            "" => {
                for entry in &self.search_path {
                    if entry.eq_ignore_ascii_case("public") {
                        if self.schema.get_table(relname).is_some()
                            || self.schema.get_view(relname).is_some()
                        {
                            return Some(relname.to_string());
                        }
                    } else if !entry.eq_ignore_ascii_case("pg_catalog") {
                        return None;
                    }
                }
                Some(relname.to_string())
            }
            _ => None,
        }
    }

    /// [`Infer::range_var`] and [`Infer::target`]: `ctes` false skips the CTE lookup.
    fn relation(&self, rv: &turso_pg_parser::pg_query::protobuf::RangeVar, ctes: bool) -> Rel {
        let name = rv
            .alias
            .as_ref()
            .map_or_else(|| rv.relname.clone(), |a| a.aliasname.clone());
        let aliased = |columns: Vec<(String, Option<u32>)>| match &rv.alias {
            Some(a) => rename(columns, &a.colnames),
            None => columns,
        };
        if ctes && rv.schemaname.is_empty() {
            if let Some((_, columns)) = self.ctes.iter().rev().find(|(n, _)| *n == rv.relname) {
                return Rel::derived(name, aliased(columns.clone()));
            }
        }
        let Some(relname) = self.main_relname(&rv.schemaname, &rv.relname) else {
            return Rel::Unknown { name };
        };
        if self.schema.get_table(&relname).is_some() {
            return Rel::Table { name, relname };
        }
        match self.view_columns(&relname) {
            Some(columns) => Rel::derived(name, aliased(columns)),
            None => Rel::Unknown { name },
        }
    }

    /// A view's columns, typed by walking its query (stored as SQL text) as a statement of its own:
    /// the query sees no enclosing scope and none of this statement's CTEs. None for no view of
    /// that name, one whose text does not parse as a view, one already being opened (a circular
    /// view) and one nested past MAX_VIEW_DEPTH: the relation is then one the walk cannot open, and
    /// a parameter compared with its columns fails closed (42P18 unless declared). A view was not
    /// opened at all, so a parameter compared with its count(*) column fell to the text fallback,
    /// and rows went missing (wire review 11 item 3); then a circular view recursed until the
    /// session thread's stack overflowed, which aborts the process, every session with it, before
    /// the engine's own "circularly defined" refusal could run (wire review 14 item 1).
    fn view_columns(&self, relname: &str) -> Option<Vec<(String, Option<u32>)>> {
        if self.views.len() >= MAX_VIEW_DEPTH
            || self.views.iter().any(|v| v.eq_ignore_ascii_case(relname))
        {
            self.cuts.set(self.cuts.get() + 1);
            return None;
        }
        let key = relname.to_ascii_lowercase();
        if let Some(columns) = self.view_memo.borrow().get(&key) {
            return columns.clone();
        }
        // A view read with a cut is kept for this statement: a view sees a cut only inside a cycle
        // through it, which any later reading of it meets too, or past the depth bound, where a
        // shallower reading might not; either way its columns are fail-closed (whatever a cut
        // relation held is unknown), so reusing them can only refuse more. Its reader counts the
        // cut, so nothing read from it reaches the snapshot's memo. Not kept, every sibling FROM
        // item read its view again: about e*(k-1)! parses for k views that each list all k (wire
        // review 16 item 3).
        if let Some(columns) = self.cut_views.borrow().get(&key) {
            self.cuts.set(self.cuts.get() + 1);
            return columns.clone();
        }
        let view = self.schema.get_view(relname)?;
        let cuts = self.cuts.get();
        let columns = self.read_view(&view, relname);
        let memo = if self.cuts.get() == cuts {
            &self.view_memo
        } else {
            &self.cut_views
        };
        memo.borrow_mut().insert(key, columns.clone());
        columns
    }

    /// [`Infer::view_columns`] of `view`, read: its stored query walked, or, when libpg_query cannot
    /// read the text the engine stored (SQLite's rendering: `IS TRUE` is `IS 1`), the engine's own
    /// columns for the view, typed by their declared types. Such a view became one the walk
    /// cannot open, and a parameter compared with its columns was refused 42P18 (wire review 14
    /// item 10).
    fn read_view(
        &self,
        view: &turso_core::schema::View,
        relname: &str,
    ) -> Option<Vec<(String, Option<u32>)>> {
        let engine_columns = || {
            Some(
                view.columns
                    .iter()
                    .map(|c| (c.name.clone().unwrap_or_default(), column_oid(c)))
                    .collect(),
            )
        };
        let sql = crate::catalog::decode_stored_pg_schema_sql(&view.sql).unwrap_or(&view.sql);
        let Ok(parsed) = turso_pg_parser::parse(sql) else {
            return engine_columns();
        };
        let Some(stmt) = parsed
            .protobuf
            .stmts
            .first()
            .and_then(|raw| raw.stmt.as_deref())
        else {
            return engine_columns();
        };
        let Some(Node::ViewStmt(v)) = stmt.node.as_ref() else {
            return engine_columns();
        };
        let Some(Node::SelectStmt(query)) = v.query.as_deref().and_then(|q| q.node.as_ref()) else {
            return engine_columns();
        };
        let mut views = self.views.clone();
        views.push(relname.to_string());
        let mut walk = Infer {
            schema: self.schema,
            types: std::collections::BTreeMap::new(),
            compared_untyped: std::collections::BTreeSet::new(),
            ctes: Vec::new(),
            views,
            subselects: Default::default(),
            search_path: self.search_path.clone(),
            view_memo: self.view_memo.clone(),
            cuts: self.cuts.clone(),
            cut_views: self.cut_views.clone(),
        };
        let columns = walk.select(query, &Vec::new());
        Some(rename(columns, &v.aliases))
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
                // An alias-less subquery (PostgreSQL 16 and later) is nameless: its columns are
                // still in scope for a bare reference. It was dropped, so its columns fell to the
                // text fallback (wire review 11 item 3).
                level.push(match &r.alias {
                    Some(a) => Rel::derived(a.aliasname.clone(), rename(columns, &a.colnames)),
                    None => Rel::derived(String::new(), columns),
                });
            }
            Some(Node::RangeFunction(f)) => {
                for item in &f.functions {
                    self.expr(item, scope);
                }
                // A function in FROM reads the items before it (PostgreSQL's functions in FROM are
                // implicitly LATERAL).
                let mut inner = scope.clone();
                inner.push(level.clone());
                level.push(self.range_function(f, &inner));
            }
            _ => {}
        }
    }

    /// The relation a function in FROM brings into scope (wire review 14 item 2: it brought none, so
    /// a parameter compared with its column bound to an outer column of the name, or fell to
    /// text): generate_series over integers is a relation of one column, int4 (int8 if an argument
    /// is bigint, numeric if one is numeric), named by the alias's column list, else the alias, else
    /// the function, as PostgreSQL names it; any other function, and a form the walk does not model
    /// (LATERAL, WITH ORDINALITY, ROWS FROM, an argument it cannot type), is a relation it cannot
    /// open, so a parameter compared with a bare column there fails closed (42P18).
    fn range_function(
        &self,
        f: &turso_pg_parser::pg_query::protobuf::RangeFunction,
        scope: &Scope,
    ) -> Rel {
        let call = match f.functions.as_slice() {
            [item] => match item.node.as_ref() {
                Some(Node::List(l)) => l.items.first().and_then(|i| match i.node.as_ref() {
                    Some(Node::FuncCall(c)) => Some(c),
                    _ => None,
                }),
                Some(Node::FuncCall(c)) => Some(c),
                _ => None,
            },
            _ => None,
        };
        let function = call.and_then(func_name);
        let alias = f.alias.as_ref().filter(|a| !a.aliasname.is_empty());
        let name = alias
            .map(|a| a.aliasname.clone())
            .or_else(|| function.clone())
            .unwrap_or_default();
        let (Some(call), Some("generate_series")) = (call, function.as_deref()) else {
            return Rel::Unknown { name };
        };
        if f.lateral || f.ordinality || !(2..=3).contains(&call.args.len()) {
            return Rel::Unknown { name };
        }
        let mut widest = INT4;
        for arg in &call.args {
            widest = match self.type_of(arg, scope) {
                Some(INT2 | INT4) => widest,
                Some(INT8) if widest != NUMERIC => INT8,
                Some(INT8) => NUMERIC,
                Some(NUMERIC) => NUMERIC,
                _ => return Rel::Unknown { name },
            };
        }
        let column = alias
            .and_then(|a| a.colnames.first())
            .and_then(|c| match c.node.as_ref() {
                Some(Node::String(s)) => Some(s.sval.clone()),
                _ => None,
            })
            .unwrap_or_else(|| name.clone());
        Rel::Derived {
            name,
            columns: vec![(column, Some(widest))],
        }
    }

    /// INSERT: each VALUES item, or each item of the SELECT's target list, takes the type of the
    /// column it goes into (the column list's, or the table's in order); ON CONFLICT DO UPDATE's
    /// SET takes its columns' types and its WHERE is a condition over the table and EXCLUDED.
    fn insert(&mut self, insert: &turso_pg_parser::pg_query::protobuf::InsertStmt, scope: &Scope) {
        let Some(rel) = insert.relation.as_ref() else {
            return;
        };
        // The table written, as the engine resolves it; one this walk cannot read (another
        // schema's) types nothing, and every value parameter is refused untyped (42P18) rather
        // than typed from public's table of the name (wire review 14 item 3).
        let relname = self.main_relname(&rel.schemaname, &rel.relname);
        let columns: Vec<Option<u32>> = match &relname {
            None => Vec::new(),
            Some(relname) if insert.cols.is_empty() => self
                .schema
                .get_btree_table(relname)
                .map(|t| t.columns().iter().map(column_oid).collect())
                .unwrap_or_default(),
            Some(relname) => insert
                .cols
                .iter()
                .map(|c| match c.node.as_ref() {
                    Some(Node::ResTarget(t)) => declared_type(self.schema, relname, &t.name),
                    _ => None,
                })
                .collect(),
        };
        if relname.is_none() {
            if let Some(Node::SelectStmt(s)) =
                insert.select_stmt.as_deref().and_then(|s| s.node.as_ref())
            {
                for row in &s.values_lists {
                    if let Some(Node::List(l)) = row.node.as_ref() {
                        for item in &l.items {
                            self.refuse_untyped(item);
                        }
                    }
                }
                for target in &s.target_list {
                    if let Some(Node::ResTarget(t)) = target.node.as_ref() {
                        if let Some(val) = t.val.as_deref() {
                            self.refuse_untyped(val);
                        }
                    }
                }
            }
        }
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
        let excluded = match &relname {
            Some(relname) => Rel::Table {
                name: "excluded".to_string(),
                relname: relname.clone(),
            },
            None => Rel::Unknown {
                name: "excluded".to_string(),
            },
        };
        let target = vec![vec![self.target(rel), excluded]];
        if let Some(c) = &insert.on_conflict_clause {
            self.set_targets(&c.target_list, relname.as_deref(), &target);
            self.qual(c.where_clause.as_deref(), &target);
        }
        self.targets(&insert.returning_list, &target);
    }

    /// UPDATE-style SET targets of `relname`: each value takes its column's type, a multi-column
    /// `SET (a, b) = ($1, $2)` element by element.
    fn set_targets(&mut self, targets: &[PgNode], relname: Option<&str>, scope: &Scope) {
        for target in targets {
            let Some(Node::ResTarget(t)) = target.node.as_ref() else {
                continue;
            };
            let Some(val) = t.val.as_deref() else {
                continue;
            };
            // A table the walk cannot read (another schema's): its columns type nothing, and a
            // parameter assigned to one is refused untyped (wire review 14 item 3).
            let Some(relname) = relname else {
                match val.node.as_ref() {
                    Some(Node::MultiAssignRef(m)) => {
                        if let Some(Node::RowExpr(row)) =
                            m.source.as_deref().and_then(|s| s.node.as_ref())
                        {
                            for item in &row.args {
                                self.refuse_untyped(item);
                            }
                        }
                    }
                    _ => self.refuse_untyped(val),
                }
                self.expr(val, scope);
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
                    // `*` and `t.*` are the columns of the current FROM level's relations, in
                    // order; each was one `?column?` (wire review 11 item 3).
                    if let Some(expanded) = self.star(val, scope) {
                        columns.extend(expanded);
                        continue;
                    }
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

    /// A target-list `*` or `t.*`, expanded to the current FROM level's columns (those of `t`
    /// alone); a relation the walk cannot open, or a `t` the level does not have, gives [`STAR`].
    /// None for any other target.
    fn star(&self, val: &PgNode, scope: &Scope) -> Option<Vec<(String, Option<u32>)>> {
        let Some(Node::ColumnRef(c)) = val.node.as_ref() else {
            return None;
        };
        if !matches!(
            c.fields.last().and_then(|f| f.node.as_ref()),
            Some(Node::AStar(_))
        ) {
            return None;
        }
        let qualifier = match c.fields.as_slice() {
            [_] => None,
            [q, _] => match q.node.as_ref() {
                Some(Node::String(s)) => Some(s.sval.as_str()),
                _ => return Some(vec![(STAR.to_string(), None)]),
            },
            _ => return Some(vec![(STAR.to_string(), None)]),
        };
        let level = scope.last().map_or(&[][..], |l| l.as_slice());
        let mut columns = Vec::new();
        let mut any = false;
        for rel in level
            .iter()
            .filter(|r| qualifier.is_none_or(|q| r.name() == q))
        {
            any = true;
            match rel {
                Rel::Table { relname, .. } => match self.schema.get_table(relname) {
                    Some(table) => columns.extend(
                        table
                            .columns()
                            .iter()
                            .filter(|col| !col.hidden())
                            .map(|col| (col.name.clone().unwrap_or_default(), column_oid(col))),
                    ),
                    None => columns.push((STAR.to_string(), None)),
                },
                Rel::Derived { columns: c, .. } => columns.extend(c.iter().cloned()),
                Rel::Unknown { .. } => columns.push((STAR.to_string(), None)),
            }
        }
        if !any {
            columns.push((STAR.to_string(), None));
        }
        Some(columns)
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
                // Whatever it has is unknown: found, with no type the walk knows.
                Rel::Unknown { .. } => Some(None),
            }
        };
        match names.as_slice() {
            [column] => {
                for level in scope.iter().rev() {
                    // A level with a relation the walk cannot open may hold the column: the search
                    // stops there, untyped, rather than bind it to an outer relation's column of
                    // that name or to text (wire review 11 item 3).
                    if level.iter().any(|rel| matches!(rel, Rel::Unknown { .. })) {
                        return Some(None);
                    }
                    // In two relations of the level: one column a join merged (USING, NATURAL)
                    // when they agree on its type, which it then has; untyped when they do not. A
                    // column no join merges is the engine's 42702 whichever is returned. It was
                    // read as not found, which fell to text (wire review 14 item 9).
                    let found: Vec<Option<u32>> =
                        level.iter().filter_map(|rel| of(rel, column)).collect();
                    if let [first, rest @ ..] = found.as_slice() {
                        return Some(if rest.iter().all(|t| t == first) {
                            *first
                        } else {
                            None
                        });
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
            // `public.t.c` is t's c; a reference through any other schema (or deeper) names a
            // relation this walk cannot read: found, untyped, so a parameter compared with it is
            // refused (42P18). It was not found, which fell to text (wire review 14 item 3).
            [schema, table, column] if schema.eq_ignore_ascii_case("public") => {
                for level in scope.iter().rev() {
                    if let Some(rel) = level.iter().find(|rel| rel.name() == *table) {
                        return of(rel, column);
                    }
                }
                None
            }
            _ => Some(None),
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

    /// The type of a scalar subquery's one result column, read in its own FROM as the SELECT walk
    /// reads a FROM ([`Infer::from_items`]: every kind of item), in a walk of its own whose
    /// parameter types are dropped ([`Infer::expr`] types them when it walks the subquery). Only
    /// its tables were read, so a derived table, a join or a function there added nothing, and the
    /// column bound to an outer relation's column of the name (wire review 14 item 2).
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
        let val = t.val.as_deref()?;
        let key = s as *const SelectStmt as usize;
        if let Some(ty) = self.subselects.borrow().get(&key) {
            return *ty;
        }
        let mut walk = Infer {
            schema: self.schema,
            types: std::collections::BTreeMap::new(),
            compared_untyped: std::collections::BTreeSet::new(),
            ctes: self.ctes.clone(),
            views: self.views.clone(),
            subselects: self.subselects.clone(),
            search_path: self.search_path.clone(),
            view_memo: self.view_memo.clone(),
            cuts: self.cuts.clone(),
            cut_views: self.cut_views.clone(),
        };
        let level = walk.from_items(&s.from_clause, scope);
        let mut inner = scope.clone();
        inner.push(level);
        let ty = walk.type_of(val, &inner);
        self.subselects.borrow_mut().insert(key, ty);
        ty
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
/// A list holding the [`STAR`] marker (a `*` the walk could not expand) is returned as it is: the
/// marker is what makes the relation one the walk cannot open, and renamed away it read as a
/// relation of the aliased columns, untyped (wire review 14 item 8).
fn rename(mut columns: Vec<(String, Option<u32>)>, names: &[PgNode]) -> Vec<(String, Option<u32>)> {
    if columns.iter().any(|(n, _)| n == STAR) {
        return columns;
    }
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
