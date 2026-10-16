//! One PostgreSQL type inference over the parsed tree. Result columns, parameters and
//! translator expressions all receive the OIDs from this walk; the engine remains the fallback
//! only for expressions whose PostgreSQL type the walk cannot yet determine.

use crate::catalog::builtin_type_oid;
use turso_core::schema::{Column, Schema};
use turso_parser::sql_error::{SqlErrorKind, SqlErrorMessage};
use turso_pg_parser::pg_query::protobuf::{node::Node, Node as PgNode, SelectStmt};
use turso_pg_parser::pg_query::ParseResult;

const INT2: u32 = 21;
const INT4: u32 = 23;
const INT8: u32 = 20;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const NUMERIC: u32 = 1700;

const BOOL: u32 = 16;
const TEXT: u32 = 25;

pub fn param_targets(parse: &ParseResult) -> Vec<Option<u32>> {
    let [raw] = parse.protobuf.stmts.as_slice() else {
        return Vec::new();
    };
    let Some(Node::SelectStmt(select)) = raw.stmt.as_ref().and_then(|s| s.node.as_ref()) else {
        return Vec::new();
    };
    if select.larg.is_some()
        || select.rarg.is_some()
        || select.target_list.iter().any(|target| {
            matches!(target.node.as_ref(), Some(Node::ResTarget(t)) if matches!(
            t.val.as_deref().and_then(|v| v.node.as_ref()), Some(Node::ColumnRef(c))
                if matches!(c.fields.last().and_then(|f| f.node.as_ref()), Some(Node::AStar(_)))))
        })
    {
        return Vec::new();
    }
    select
        .target_list
        .iter()
        .map(|target| match target.node.as_ref() {
            Some(Node::ResTarget(t)) => match t.val.as_deref().and_then(|v| v.node.as_ref()) {
                Some(Node::ParamRef(p)) => u32::try_from(p.number).ok(),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Whether `text` (a literal pg_query gives as a float's text) is an integer literal, decimal or
/// 0x / 0o / 0b with underscores, whose value fits int8.
fn integer_literal_fits_i64(text: &str) -> bool {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let lower = digits.to_ascii_lowercase();
    let (radix, body) = if let Some(b) = lower.strip_prefix("0x") {
        (16, b)
    } else if let Some(b) = lower.strip_prefix("0o") {
        (8, b)
    } else if let Some(b) = lower.strip_prefix("0b") {
        (2, b)
    } else {
        (10, lower.as_str())
    };
    let clean: String = body.chars().filter(|c| *c != '_').collect();
    if clean.is_empty() || !clean.chars().all(|c| c.is_digit(radix)) {
        return false;
    }
    match u128::from_str_radix(&clean, radix) {
        Ok(v) if negative => v <= i64::MAX as u128 + 1,
        Ok(v) => v <= i64::MAX as u128,
        Err(_) => false,
    }
}

/// What the parameter walk gives a statement ([`parameter_types`]).
pub struct Walked {
    #[cfg(test)]
    work: usize,
    /// The type OID of each $n its context types, by n ([`StatementTypes::params`]).
    pub params: std::collections::BTreeMap<u32, u32>,
    /// The $n compared with an expression no context types ([`StatementTypes::untyped`]).
    pub untyped: std::collections::BTreeSet<u32>,
    /// SELECT or RETURNING columns, in order, from the same scoped inference as parameters.
    pub columns: Vec<Option<u32>>,
    pub expressions: std::sync::Arc<turso_pg_parser::translator::ExpressionTypes>,
}

#[derive(Debug, Clone, Default)]
pub struct StatementTypes {
    pub columns: Vec<Option<u32>>,
    /// The type OID of each $n its context types, by n; one missing is untyped (text, as
    /// PostgreSQL resolves a parameter nothing types). Sparse, so a statement's highest $n sizes
    /// nothing (wire review 8 item 3).
    pub params: std::collections::BTreeMap<u32, u32>,
    /// Every $n the statement's parse tree holds, sorted, each in 1..=MAX_PARAMETER (see
    /// turso_pg_parser::checked_param_numbers): the parameter count and the gaps are read from
    /// this, not from the engine's slots, which a clause it folds away does not get (wire review 8
    /// item 5).
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
    /// not (turso_pg::check_table_keys; wire review 13 item 2). None for one the frontend performed
    /// whole at prepare, keys resolved, because it has prerequisites (a SERIAL column's sequence;
    /// wire review 16 item 15).
    pub new_table_keys: Option<String>,
    /// The frontend performed the statement while preparing it (a CREATE TABLE with SERIAL
    /// prerequisites), and the statement it returns is a no-op: the server answers it with its
    /// command tag alone, never as a query (the no-op's one column made it `SELECT 0`).
    pub performed: bool,
    /// For each result column of a plain SELECT, the parameter number when the column is a bare
    /// `$n`: the server announces such a column as that parameter's type, declared or inferred,
    /// as PostgreSQL does (it was text; the PG 18 differential's finding (c)). Empty when the
    /// statement is no such SELECT.
    pub param_targets: Vec<Option<u32>>,
}

impl StatementTypes {
    pub fn from_declared(declared: &[Option<u32>]) -> Self {
        Self {
            params: declared
                .iter()
                .enumerate()
                .filter_map(|(i, oid)| {
                    oid.filter(|oid| *oid != 0 && *oid != 705)
                        .map(|oid| (i as u32 + 1, oid))
                })
                .collect(),
            ..Self::default()
        }
    }
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
    cache: &parking_lot::Mutex<ViewCache>,
    attached: &Attached<'_>,
    declared: &std::collections::BTreeMap<u32, u32>,
) -> Result<Walked, turso_core::LimboError> {
    let memo = {
        let mut cache = cache.lock();
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
    let result = parameter_types_with(parse, schema, search_path, &view_memo, attached, declared);
    cache.lock().views = view_memo.take();
    result
}

/// Each view's columns as the parameter-type walk read them, for one schema snapshot and search
/// path: a statement over views parsed each view every time it was prepared, and a view reached
/// k times was parsed k times (2^depth for a view joining the one before it twice; wire review 14
/// item 10). Keyed by the snapshot itself, the Arc the connection hands out until DDL replaces it,
/// which the cache holds so that it cannot be mistaken for a later one; a view read where the walk
/// was cut (a cycle) is not kept.
#[derive(Default)]
pub struct ViewCache {
    schema: Option<std::sync::Arc<Schema>>,
    search_path: Vec<String>,
    views: ViewMemo,
}

type ViewMemo = std::collections::HashMap<String, Option<Vec<(String, Option<u32>)>>>;

/// The session's attached schemas, for the walk's unqualified names ([`Infer::main_relname`]):
/// their names, and `holds(schema, relation)`, whether one holds a relation of the name. The walk
/// cannot read an attached schema's relations, but it must know which schema the engine resolves a
/// name to: the engine passes over an attached schema that lacks the name (wire review 17 item 3).
pub struct Attached<'a> {
    pub names: &'a [String],
    pub holds: &'a dyn Fn(&str, &str) -> bool,
}

fn parameter_types_with(
    parse: &ParseResult,
    schema: &Schema,
    search_path: &[String],
    view_memo: &std::rc::Rc<std::cell::RefCell<ViewMemo>>,
    attached: &Attached<'_>,
    declared: &std::collections::BTreeMap<u32, u32>,
) -> Result<Walked, turso_core::LimboError> {
    let mut infer = Infer {
        #[cfg(test)]
        work: Default::default(),
        stack_limited: Default::default(),
        expressions: Default::default(),
        schema,
        types: declared.clone(),
        parameter_links: Default::default(),
        compared_untyped: std::collections::BTreeSet::new(),
        ctes: Vec::new(),
        views: Vec::new(),
        subselects: Default::default(),
        search_path: search_path.to_vec(),
        view_memo: view_memo.clone(),
        cuts: Default::default(),
        cut_views: Default::default(),
        attached,
        missing_rte: Default::default(),
    };
    let mut columns = Vec::new();
    if let [raw] = parse.protobuf.stmts.as_slice() {
        if let Some(stmt) = raw.stmt.as_deref() {
            // Context can propagate through a chain of bare parameters. `set`
            // resolves those recorded relationships with a worklist, without
            // revisiting the whole statement once per parameter. The final pass
            // publishes every occurrence against that solved map.
            infer.statement(stmt);
            infer.subselects.borrow_mut().clear();
            infer.expressions.take();
            columns = infer.statement(stmt);
        }
    }
    let untyped = infer
        .compared_untyped
        .iter()
        .filter(|n| !infer.types.contains_key(n))
        .copied()
        .collect();
    if infer.stack_limited.get() {
        return Err(turso_core::LimboError::StackDepthLimitExceeded);
    }
    if let Some(missing) = infer.missing_rte.take() {
        return Err(turso_core::LimboError::ParseError(missing));
    }
    Ok(Walked {
        #[cfg(test)]
        work: infer.work.get(),
        params: infer.types,
        untyped,
        columns,
        expressions: std::sync::Arc::new(infer.expressions.take()),
    })
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
    /// An unaliased USING/NATURAL join has one merged output namespace, but its
    /// original members remain available to qualified references. Share members
    /// across scope snapshots so cloning a scope does not recursively clone joins.
    Joined {
        members: std::sync::Arc<Vec<Rel>>,
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
            Rel::Joined { .. } => "",
        }
    }

    /// The visible relation named by a qualifier. A join alias is a Derived
    /// relation, so its members cannot leak out through this search. Use an
    /// iterative walk rather than add unguarded recursion for nested joins.
    fn named(&self, name: &str) -> Option<&Rel> {
        let mut current = self;
        let mut pending = Vec::new();
        loop {
            match current {
                Rel::Joined { members, .. } => pending.extend(members.iter().rev()),
                _ if current.name() == name => return Some(current),
                _ => {}
            }
            current = pending.pop()?;
        }
    }

    fn unknown_columns(&self) -> bool {
        match self {
            Rel::Unknown { .. } => true,
            Rel::Joined { columns, .. } => columns.iter().any(|(name, _)| name == STAR),
            _ => false,
        }
    }

    /// A derived relation of these columns, or one the walk cannot open if a `*` among them could
    /// not be expanded ([`STAR`]).
    fn derived(name: String, columns: Vec<(String, Option<u32>)>) -> Rel {
        if columns.iter().any(|(n, _)| n == STAR) {
            Rel::Unknown { name }
        } else {
            Rel::Derived { name, columns: columns.into_iter().map(|(name, ty)| (name, ty.map(resolve_output))).collect() }
        }
    }
}

/// The name a target-list `*` keeps when the walk cannot expand it (a relation it cannot open):
/// the columns of whatever derives from that list are then unknown ([`Rel::derived`]).
const STAR: &str = "*";

/// The relations visible at a point of the walk, one level per enclosing SELECT, innermost last: a
/// column reference resolves innermost first, as PostgreSQL resolves a correlated reference.
type Scope = Vec<Vec<Rel>>;

/// A column's type: `Some(Some(t))` resolved, `Some(None)` found with no type the walk knows (or in
/// two relations of one level with types that differ), `None` not found.
type Found = Option<Option<u32>>;

struct Infer<'a> {
    #[cfg(test)]
    work: std::rc::Rc<std::cell::Cell<usize>>,
    stack_limited: std::rc::Rc<std::cell::Cell<bool>>,
    expressions: std::rc::Rc<std::cell::RefCell<turso_pg_parser::translator::ExpressionTypes>>,
    schema: &'a Schema,
    types: std::collections::BTreeMap<u32, u32>,
    /// Bare parameters compared before either has a context share that later
    /// context. Each edge is recorded once; `set` visits a newly typed node once.
    parameter_links: std::collections::BTreeMap<u32, std::collections::BTreeSet<u32>>,
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
    /// How many times a view was cut (a cycle), shared with every nested walk: a view whose
    /// reading saw a cut is not memoised for the snapshot.
    cuts: std::rc::Rc<std::cell::Cell<usize>>,
    /// The views whose reading saw a cut, for this statement's walk only, shared with every nested
    /// walk ([`Infer::view_columns`]).
    cut_views: std::rc::Rc<std::cell::RefCell<ViewMemo>>,
    /// The session's attached schemas ([`Attached`]).
    attached: &'a Attached<'a>,
    /// A column reference that names no FROM entry (`s.t.c` with no relation s.t in scope),
    /// PostgreSQL's errorMissingRTE (42P01): the statement is refused with it. Shared with the walks
    /// of the statement's own subqueries, not with a view's (wire review 17 item 5).
    missing_rte: std::rc::Rc<std::cell::RefCell<Option<SqlErrorMessage>>>,
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
        | "position" | "strpos" | "ascii" | "array_length" | "array_position" | "cardinality" => {
            INT4
        }
        "lower" | "upper" | "btrim" | "ltrim" | "rtrim" | "trim" | "substr" | "substring"
        | "replace" | "concat" | "concat_ws" | "left" | "right" | "lpad" | "rpad" | "repeat"
        | "reverse" | "md5" | "initcap" | "translate" | "split_part" | "chr" | "to_char"
        | "quote_ident" | "quote_literal" | "format" | "current_schema" | "current_database"
        | "version" => TEXT,
        "row_number" | "rank" | "dense_rank" | "ntile" | "pg_database_size" | "nextval" | "currval" | "lastval" => INT8,
        "pg_input_is_valid" => BOOL,
        "current_schemas" => 1003,
        "pg_typeof" => 2206,
        // float8-only in PostgreSQL, whatever numeric argument they are given, and computed as a
        // float by the engine (random() as the translator scales it).
        "percent_rank" | "cume_dist" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan"
        | "atan2" | "degrees" | "radians" | "pi" | "random" => FLOAT8,
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

fn resolve_output(oid: u32) -> u32 {
    if oid == 705 { TEXT } else { oid }
}

/// The wider of two numeric types (PostgreSQL's arithmetic promotes int to numeric to float8);
/// otherwise the first.
fn wider(a: u32, b: u32) -> u32 {
    if a == 705 { return b; }
    if b == 705 { return a; }
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

/// The type all arms can share. Unknown literals do not select the type; unrelated
/// categories stay unresolved instead of publishing the first arm's potentially wrong OID.
fn common_type(types: impl IntoIterator<Item = Option<u32>>) -> Option<u32> {
    let mut result = None;
    for oid in types {
        let oid = oid?;
        if oid == 705 {
            continue;
        }
        result = Some(match result {
            None => oid,
            Some(previous) if previous == oid => oid,
            Some(previous)
                if matches!(previous, INT2 | INT4 | INT8 | NUMERIC | FLOAT4 | FLOAT8)
                    && matches!(oid, INT2 | INT4 | INT8 | NUMERIC | FLOAT4 | FLOAT8) =>
            {
                wider(previous, oid)
            }
            Some(previous)
                if matches!(previous, TEXT | 1043 | 1042 | 19)
                    && matches!(oid, TEXT | 1043 | 1042 | 19) =>
            {
                TEXT
            }
            Some(_) => return None,
        });
    }
    Some(result.unwrap_or(TEXT))
}

fn unknown_literal(node: &PgNode) -> bool {
    matches!(node.node.as_ref(), Some(Node::AConst(c)) if c.isnull || matches!(
        c.val.as_ref(), Some(turso_pg_parser::pg_query::protobuf::a_const::Val::Sval(_))))
}

impl Infer<'_> {
    // Inference shares the parser/engine's thread guard. Keep each entry alive across its
    // recursive calls; a refusal is carried back to preparation as the typed 54001 error.
    fn enter(&self) -> Option<turso_stack_guard::NestGuard> {
        #[cfg(test)]
        self.work.set(self.work.get() + 1);
        if self.stack_limited.get() { return None; }
        match turso_stack_guard::NestGuard::enter() {
            Ok(guard) => Some(guard),
            Err(_) => { self.stack_limited.set(true); None }
        }
    }

    fn common_expr_type<'n>(
        &self,
        nodes: impl IntoIterator<Item = &'n PgNode>,
        scope: &Scope,
    ) -> Option<u32> {
        common_type(nodes.into_iter().map(|node| {
            if unknown_literal(node)
                || param(node).is_some_and(|number| !self.types.contains_key(&number))
            {
                Some(705)
            } else {
                self.type_of(node, scope)
            }
        }))
    }

    /// Type `node` as `ty` if it is a parameter its first context has not typed yet.
    fn set(&mut self, node: &PgNode, ty: Option<u32>) {
        if let (Some(n), Some(ty)) = (param(node), ty) {
            if self.types.contains_key(&n) {
                return;
            }
            let mut pending = smallvec::SmallVec::<[u32; 4]>::from_slice(&[n]);
            while let Some(number) = pending.pop() {
                if let std::collections::btree_map::Entry::Vacant(entry) = self.types.entry(number) {
                    entry.insert(resolve_output(ty));
                    if let Some(linked) = self.parameter_links.get(&number) {
                        pending.extend(linked.iter().copied());
                    }
                }
            }
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
        if ta.is_none() && tb.is_none() {
            if let (Some(a), Some(b)) = (param(a), param(b)) {
                self.parameter_links.entry(a).or_default().insert(b);
                self.parameter_links.entry(b).or_default().insert(a);
            }
        }
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

    /// Walk a statement; returns a SELECT's result columns' types (empty for any other).
    fn statement(&mut self, stmt: &PgNode) -> Vec<Option<u32>> {
        let Some(_guard) = self.enter() else { return Vec::new(); };
        let scope: Scope = Vec::new();
        match stmt.node.as_ref() {
            Some(Node::ViewStmt(v)) => {
                if let Some(query) = v.query.as_deref() { self.statement(query); }
            }
            Some(Node::CreateTableAsStmt(c)) => {
                if let Some(query) = c.query.as_deref() { self.statement(query); }
            }
            Some(Node::SelectStmt(s)) => {
                let columns = self.select(s, &scope);
                return if columns.iter().any(|(name, _)| name == STAR) {
                    Vec::new()
                } else {
                    columns.into_iter().map(|(_, ty)| ty.map(resolve_output)).collect()
                };
            }
            Some(Node::InsertStmt(i)) => {
                let ctes = self.with(i.with_clause.as_ref(), &scope);
                let columns = self.insert(i, &scope);
                self.ctes.truncate(ctes);
                return columns;
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
                    let columns = self.targets(&u.returning_list, &inner);
                    self.ctes.truncate(ctes);
                    return columns.into_iter().map(|(_, ty)| ty.map(resolve_output)).collect();
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
                    let columns = self.targets(&d.returning_list, &inner);
                    self.ctes.truncate(ctes);
                    return columns.into_iter().map(|(_, ty)| ty.map(resolve_output)).collect();
                }
                self.ctes.truncate(ctes);
            }
            _ => {}
        }
        Vec::new()
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
    /// information_schema_<view>. An unqualified name is a catalog relation's first (PostgreSQL
    /// searches pg_catalog before the path); then, with no path set, main's, else the attached
    /// schemas' (the engine reads main, then each attached schema); with a path, each entry in turn,
    /// public being main, an attached schema being passed over when it lacks the name, and any
    /// other entry (`"$user"`, PostgreSQL's default, or a schema not attached) skipped, as the
    /// engine skips it. None for a relation of an attached schema (which this walk cannot read: a
    /// parameter compared with it or written into it is refused untyped, 42P18) and for a name the
    /// engine resolves nowhere. Every relation was read in public alone (wire review 14 item 3);
    /// then the walk stopped at the first path entry that was not public, so `"$user", public`
    /// refused every parameter, and with no path took a name absent from main as main's, which
    /// typed a parameter into an attached schema's table as text (wire review 17 item 3). A probe of
    /// an attached schema counts as a cut: its answer can change with no new main snapshot, so a
    /// view read through it is not kept for the snapshot ([`ViewCache`]).
    fn main_relname(&self, schemaname: &str, relname: &str) -> Option<String> {
        use turso_pg_parser::SchemaQualifier;
        match turso_pg_parser::schema_qualifier(schemaname) {
            // main is the engine's name for public, which a view's stored text holds since the
            // translator names public's relations so (wire review 17 item 1, review 18 item 5).
            SchemaQualifier::Main | SchemaQualifier::PgCatalog => Some(relname.to_string()),
            SchemaQualifier::InformationSchema => {
                Some(format!("information_schema_{}", relname.to_lowercase()))
            }
            SchemaQualifier::Unqualified => {
                let in_main = || {
                    self.schema.get_table(relname).is_some()
                        || self.schema.get_view(relname).is_some()
                };
                if crate::catalog::is_catalog_table_name(relname) {
                    return Some(relname.to_string());
                }
                let attached_holds = |name: &String| {
                    self.cuts.set(self.cuts.get() + 1);
                    (self.attached.holds)(name, relname)
                };
                if self.search_path.is_empty() {
                    if in_main() {
                        return Some(relname.to_string());
                    }
                    return None;
                }
                for entry in &self.search_path {
                    if entry.eq_ignore_ascii_case("public") {
                        if in_main() {
                            return Some(relname.to_string());
                        }
                    } else if let Some(name) = self
                        .attached
                        .names
                        .iter()
                        .find(|n| n.eq_ignore_ascii_case(entry))
                    {
                        if attached_holds(name) {
                            return None;
                        }
                    }
                }
                None
            }
            SchemaQualifier::Attached(_) => None,
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
        if let Some(table) = self.schema.get_table(&relname) {
            if matches!(table.as_ref(), turso_core::schema::Table::Virtual(_)) {
                let columns = table
                    .columns()
                    .iter()
                    .filter(|c| !c.hidden())
                    .map(|column| {
                        let name = column.name.clone().unwrap_or_default();
                        let oid = crate::catalog::catalog_column_oid(&relname, &name)
                            .or_else(|| column_oid(self.schema, column, table.is_strict()));
                        (name, oid)
                    })
                    .collect();
                return Rel::derived(name, aliased(columns));
            }
            return Rel::Table { name, relname };
        }
        match self.view_columns(&relname) {
            Some(columns) => Rel::derived(name, aliased(columns)),
            None => Rel::Unknown { name },
        }
    }

    /// A view's columns, typed by walking its query (stored as SQL text) as a statement of its own:
    /// the query sees no enclosing scope and none of this statement's CTEs. None for no view of
    /// that name, one whose text does not parse as a view, and one already being opened (a
    /// circular view): the relation is then one the walk cannot open, and a parameter compared with
    /// its columns fails closed (42P18 unless declared). A view was not opened at all, so a
    /// parameter compared with its count(*) column fell to the text fallback, and rows went missing
    /// (wire review 11 item 3); then a circular view recursed until the session thread's stack
    /// overflowed, which aborts the process, every session with it, before the engine's own
    /// "circularly defined" refusal could run (wire review 14 item 1). A view nested 32 deep was
    /// cut too, against the stack, so whether a parameter over a deep chain was typed depended on
    /// which of its views earlier statements had memoised (wire review 18 item 14): the cycle cut
    /// bounds cycles by the views in the schema; recursion shares the parser/engine NestGuard.
    fn view_columns(&self, relname: &str) -> Option<Vec<(String, Option<u32>)>> {
        let Some(_guard) = self.enter() else { return None; };
        if self.views.iter().any(|v| v.eq_ignore_ascii_case(relname)) {
            self.cuts.set(self.cuts.get() + 1);
            return None;
        }
        let key = relname.to_ascii_lowercase();
        if let Some(columns) = self.view_memo.borrow().get(&key) {
            return columns.clone();
        }
        // A view read with a cut is kept for this statement: a view sees a cut only inside a cycle
        // through it, which any later reading of it meets too; its columns are fail-closed
        // (whatever a cut relation held is unknown), so reusing them can only refuse more. Its
        // reader counts the cut, so nothing read from it reaches the snapshot's memo. Not kept,
        // every sibling FROM item read its view again: about e*(k-1)! parses for k views that each
        // list all k (wire review 16 item 3).
        if let Some(columns) = self.cut_views.borrow().get(&key) {
            self.cuts.set(self.cuts.get() + 1);
            return columns.clone();
        }
        let view = self.schema.get_view(relname)?;
        let cuts = self.cuts.get();
        let columns = self.read_view(&view, relname);
        if self.stack_limited.get() { return None; }
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
        // The engine's columns carry a table column's type only where it traced the column to a
        // table (view_columns_traced); every other one it types with a placeholder TEXT, which
        // typed a parameter compared with a computed column as text and bound it as a string
        // (wire review 18 item 4). Untyped, such a parameter is refused (42P18).
        let engine_columns = || {
            let traced = view_columns_traced(self.schema, &view.select_stmt);
            Some(
                view.columns
                    .iter()
                    .map(|c| {
                        // This fallback has no owning table's strictness metadata. The normal
                        // parsed-view walk resolves its source columns with that metadata;
                        // here retain known built-ins and leave custom names unresolved.
                        let ty = if traced {
                            column_oid(self.schema, c, false)
                        } else {
                            None
                        };
                        (c.name.clone().unwrap_or_default(), ty)
                    })
                    .collect(),
            )
        };
        let sql = crate::catalog::decode_stored_pg_schema_sql(&view.sql).unwrap_or(&view.sql);
        let parsed = match turso_pg_parser::parse(sql) {
            Ok(parsed) => parsed,
            Err(turso_pg_parser::ParseError::StackDepthLimitExceeded) => {
                self.stack_limited.set(true);
                return None;
            }
            Err(_) => return engine_columns(),
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
            #[cfg(test)]
            work: self.work.clone(),
            stack_limited: self.stack_limited.clone(),
            expressions: Default::default(),
            schema: self.schema,
            types: std::collections::BTreeMap::new(),
            parameter_links: Default::default(),
            compared_untyped: std::collections::BTreeSet::new(),
            ctes: Vec::new(),
            views,
            subselects: Default::default(),
            search_path: self.search_path.clone(),
            view_memo: self.view_memo.clone(),
            cuts: self.cuts.clone(),
            cut_views: self.cut_views.clone(),
            attached: self.attached,
            missing_rte: Default::default(),
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

    /// The current engine expands a join's star in left-column order followed
    /// by the right columns not merged by USING/NATURAL. Keep metadata aligned
    /// with those values; moving USING columns to PostgreSQL's first positions
    /// requires the translator to move the output expressions too. Qualified
    /// member lookup is independent of this output order. The common-type rule
    /// is the same one used by all other inference arms.
    fn join_columns(
        &self,
        join: &turso_pg_parser::pg_query::protobuf::JoinExpr,
        left: &[Rel],
        right: &[Rel],
    ) -> Vec<(String, Option<u32>)> {
        let left: Vec<_> = left
            .iter()
            .flat_map(|rel| self.relation_columns(rel))
            .collect();
        let right: Vec<_> = right
            .iter()
            .flat_map(|rel| self.relation_columns(rel))
            .collect();
        if left.iter().chain(&right).any(|(name, _)| name == STAR) {
            return vec![(STAR.to_string(), None)];
        }
        let names: Vec<&str> = if join.is_natural {
            left.iter()
                .filter(|(name, _)| right.iter().any(|(other, _)| other == name))
                .map(|(name, _)| name.as_str())
                .collect()
        } else {
            let Some(names) = join
                .using_clause
                .iter()
                .map(|node| match node.node.as_ref() {
                    Some(Node::String(name)) => Some(name.sval.as_str()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
            else {
                return vec![(STAR.to_string(), None)];
            };
            names
        };
        let mut columns = left.clone();
        for (at, name) in names.iter().enumerate() {
            let mut lhs = left.iter().filter(|(column, _)| column.as_str() == *name);
            let mut rhs = right.iter().filter(|(column, _)| column.as_str() == *name);
            let (Some((_, a)), Some((_, b))) = (lhs.next(), rhs.next()) else {
                return vec![(STAR.to_string(), None)];
            };
            // A missing/duplicated USING column is the engine's error. Do not
            // publish guessed metadata while that statement is being prepared.
            if lhs.next().is_some() || rhs.next().is_some() || names[..at].contains(name) {
                return vec![(STAR.to_string(), None)];
            }
            let column = columns
                .iter_mut()
                .find(|(column, _)| column.as_str() == *name)
                .expect("the unique left USING column was checked above");
            column.1 = common_type([*a, *b]);
        }
        columns.extend(
            right
                .iter()
                .filter(|(name, _)| !names.contains(&name.as_str()))
                .cloned(),
        );
        columns
    }

    fn from_item(&mut self, item: &PgNode, scope: &Scope, level: &mut Vec<Rel>) {
        let Some(_guard) = self.enter() else { return ; };
        match item.node.as_ref() {
            Some(Node::RangeVar(rv)) => level.push(self.range_var(rv)),
            Some(Node::JoinExpr(j)) => {
                // Keep preceding FROM entries while collecting each side: a FROM
                // function on the right is implicitly LATERAL and sees them and
                // the left side. Only this join's members are grouped afterward.
                let start = level.len();
                if let Some(left) = j.larg.as_deref() {
                    self.from_item(left, scope, level);
                }
                let middle = level.len();
                if let Some(right) = j.rarg.as_deref() {
                    self.from_item(right, scope, level);
                }
                let mut inner = scope.clone();
                inner.push(level.clone());
                self.qual(j.quals.as_deref(), &inner);
                if j.alias.is_some() || j.is_natural || !j.using_clause.is_empty() {
                    let columns = self.join_columns(j, &level[start..middle], &level[middle..]);
                    let mut members = level.split_off(start);
                    if let Some(alias) = &j.alias {
                        // An alias names the whole join and hides its member names.
                        level.push(if columns.iter().any(|(name, _)| name == STAR) {
                            Rel::Unknown {
                                name: alias.aliasname.clone(),
                            }
                        } else {
                            Rel::derived(alias.aliasname.clone(), rename(columns, &alias.colnames))
                        });
                    } else {
                        if let Some(alias) = &j.join_using_alias {
                            // USING (...) AS name exposes only the merged columns;
                            // it does not hide the original member names.
                            let using_columns = j
                                .using_clause
                                .iter()
                                .map(|node| {
                                    if let Some(Node::String(name)) = node.node.as_ref() {
                                        columns
                                            .iter()
                                            .find(|(column, _)| column == &name.sval)
                                            .cloned()
                                            .unwrap_or_else(|| (STAR.to_string(), None))
                                    } else {
                                        (STAR.to_string(), None)
                                    }
                                })
                                .collect();
                            members.push(Rel::derived(alias.aliasname.clone(), using_columns));
                        }
                        level.push(Rel::Joined {
                            members: std::sync::Arc::new(members),
                            columns,
                        });
                    }
                }
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
                // A function in FROM reads the items before it (PostgreSQL's functions in FROM are
                // implicitly LATERAL), its arguments too: they were walked in the enclosing scope
                // alone, so a bare name bound to an outer relation's column (wire review 16 item
                // 24).
                let mut inner = scope.clone();
                inner.push(level.clone());
                for item in &f.functions {
                    self.expr(item, &inner);
                }
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
        let function = call.and_then(|c| func_name(c));
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
    fn insert(
        &mut self,
        insert: &turso_pg_parser::pg_query::protobuf::InsertStmt,
        scope: &Scope,
    ) -> Vec<Option<u32>> {
        let Some(rel) = insert.relation.as_ref() else {
            return Vec::new();
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
                .map(|t| {
                    t.columns()
                        .iter()
                        .map(|col| column_oid(self.schema, col, t.is_strict))
                        .collect()
                })
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
        let returning = vec![vec![self.target(rel)]];
        self.targets(&insert.returning_list, &returning)
            .into_iter()
            .map(|(_, ty)| ty.map(resolve_output))
            .collect()
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
        let Some(_guard) = self.enter() else { return Vec::new(); };
        let ctes = self.with(s.with_clause.as_ref(), scope);
        let columns = if let (Some(l), Some(r)) = (s.larg.as_deref(), s.rarg.as_deref()) {
            // A set operation: its arms, then its own LIMIT, OFFSET and ORDER BY.
            let columns = self.select(l, scope);
            let right = self.select(r, scope);
            let columns = if columns.len() == right.len() {
                columns
                    .into_iter()
                    .zip(right)
                    .map(|((name, left), (_, right))| (name, common_type([left, right])))
                    .collect()
            } else {
                Vec::new()
            };
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
                // VALUES uses all rows to type both its result columns and parameter elements.
                if let Some(Node::List(first)) =
                    s.values_lists.first().and_then(|r| r.node.as_ref())
                {
                    first
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, _)| {
                            let items: Vec<_> = s
                                .values_lists
                                .iter()
                                .filter_map(|row| match row.node.as_ref() {
                                    Some(Node::List(row)) => row.items.get(i),
                                    _ => None,
                                })
                                .collect();
                            let ty = self.common_expr_type(items.iter().copied(), &inner);
                            for item in items {
                                self.set(item, ty);
                            }
                            (format!("column{}", i + 1), ty)
                        })
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

    /// One relation's unqualified output; Joined keeps member metadata separate
    /// so both `*` and a containing derived relation see the merged shape.
    fn relation_columns(&self, rel: &Rel) -> Vec<(String, Option<u32>)> {
        match rel {
            Rel::Table { relname, .. } => match self.schema.get_table(relname) {
                Some(table) => table
                    .columns()
                    .iter()
                    .filter(|column| !column.hidden())
                    .map(|column| {
                        (
                            column.name.clone().unwrap_or_default(),
                            column_oid(self.schema, column, table.is_strict()),
                        )
                    })
                    .collect(),
                None => vec![(STAR.to_string(), None)],
            },
            Rel::Derived { columns, .. } | Rel::Joined { columns, .. } => columns.clone(),
            Rel::Unknown { .. } => vec![(STAR.to_string(), None)],
        }
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
        for rel in level {
            let Some(rel) = qualifier.map_or(Some(rel), |name| rel.named(name)) else {
                continue;
            };
            any = true;
            columns.extend(self.relation_columns(rel));
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
        let Some(_guard) = self.enter() else { return ; };
        use turso_pg_parser::pg_query::protobuf::{AExprKind, SubLinkType};
        match node.node.as_ref() {
            Some(Node::AExpr(e)) => {
                let (Some(l), Some(r)) = (e.lexpr.as_deref(), e.rexpr.as_deref()) else {
                    // A prefix operator (-$1, NOT handled as BoolExpr): its operand's own walk.
                    self.exprs(e.lexpr.as_deref(), scope);
                    self.exprs(e.rexpr.as_deref(), scope);
                    self.type_of(node, scope);
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
                    self.set(arg, cast_type(self.schema, c));
                    self.expr(arg, scope);
                }
            }
            Some(Node::List(l)) => {
                for item in &l.items {
                    self.expr(item, scope);
                }
            }
            Some(Node::AArrayExpr(a)) => {
                let element = self.common_expr_type(a.elements.iter(), scope);
                for arg in &a.elements {
                    self.set(arg, element);
                    self.expr(arg, scope);
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
                let first = self.common_expr_type(c.args.iter(), scope);
                for arg in &c.args {
                    self.set(arg, first);
                    self.expr(arg, scope);
                }
            }
            Some(Node::MinMaxExpr(m)) => {
                let first = self.common_expr_type(m.args.iter(), scope);
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
                // CASE results use one common type, independently of which arm is taken.
                let result = self.common_expr_type(
                    whens
                        .iter()
                        .filter_map(|w| w.result.as_deref())
                        .chain(c.defresult.as_deref()),
                    scope,
                );
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
        self.type_of(node, scope);
    }

    /// A column reference's type: `t.c` from the innermost level naming t, a bare `c` from the
    /// innermost level holding it (in two of its relations: their type when they agree, else
    /// untyped).
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
                    Some(column_oid(self.schema, col, table.is_strict()))
                }
                Rel::Derived { columns, .. } | Rel::Joined { columns, .. } => {
                    if columns.iter().any(|(name, _)| name == STAR) {
                        Some(None)
                    } else {
                        columns
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(column))
                            .map(|(_, ty)| *ty)
                    }
                }
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
                    if level.iter().any(Rel::unknown_columns) {
                        return Some(None);
                    }
                    // A Joined relation contributes each USING/NATURAL column once,
                    // already common-typed. A remaining duplicate belongs to different
                    // relations and is the engine's ambiguous-column error; retain the
                    // prior conservative behavior when their metadata disagrees.
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
                    if let Some(rel) = level.iter().find_map(|rel| rel.named(table)) {
                        return of(rel, column);
                    }
                }
                None
            }
            // `schema.t.c` names the FROM entry t of that schema, as PostgreSQL's
            // refnameNamespaceItem reads it. Through public, `main` (a view's stored spelling) or
            // pg_catalog it is the relation t in scope; through another schema it is a relation
            // this walk cannot read (found, untyped: 42P18 for a parameter compared with it), unless
            // the t in scope is main's or a derived one, which is no relation of that schema. With
            // no entry t in scope at all, it names none: 42P01, PostgreSQL's errorMissingRTE. The
            // path hard-coded public, so the pg_catalog form was 42P18, and a reference through a
            // schema whose t was not in FROM was typed untyped instead of refused (wire review 14
            // item 3, review 17 item 5, review 18 item 5).
            [schema, table, column] => {
                let found = scope
                    .iter()
                    .rev()
                    .find_map(|level| level.iter().find_map(|rel| rel.named(table)));
                let main = matches!(
                    turso_pg_parser::schema_qualifier(schema),
                    turso_pg_parser::SchemaQualifier::Main
                        | turso_pg_parser::SchemaQualifier::PgCatalog
                );
                match found {
                    Some(rel) if main => of(rel, column),
                    Some(Rel::Unknown { .. }) => Some(None),
                    _ => {
                        self.missing_rte.borrow_mut().get_or_insert_with(|| {
                            SqlErrorMessage::classified(
                                SqlErrorKind::UndefinedTable,
                                format!("missing FROM-clause entry for table \"{table}\""),
                            )
                        });
                        Some(None)
                    }
                }
            }
            _ => Some(None),
        }
    }

    /// The type `node` has where a parameter is compared with it, if the parse says.
    fn type_of(&self, node: &PgNode, scope: &Scope) -> Option<u32> {
        let Some(_guard) = self.enter() else { return None; };
        let oid = self.infer_type(node, scope);
        if let Some(oid) = oid {
            self.expressions.borrow_mut().insert(node, oid);
        }
        oid
    }

    fn infer_type(&self, node: &PgNode, scope: &Scope) -> Option<u32> {
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
                    // PostgreSQL has these for float8 and numeric: an integer or float argument
                    // resolves to the float8 one (float8 is the numeric category's preferred type),
                    // a numeric one keeps numeric, left untyped here (the engine computes a float).
                    (
                        "sqrt" | "exp" | "ln" | "log" | "log10" | "power" | "pow",
                        INT2 | INT4 | INT8 | FLOAT4 | FLOAT8,
                    ) => Some(FLOAT8),
                    _ => None,
                }
            }
            Node::AConst(c) if c.isnull => Some(705),
            Node::AConst(c) => match c.val.as_ref()? {
                Val::Ival(_) => Some(INT4),
                Val::Fval(f) => Some(if integer_literal_fits_i64(&f.fval) {
                    INT8
                } else {
                    NUMERIC
                }),
                Val::Boolval(_) => Some(BOOL),
                // An untyped literal resolves as text against an undeclared parameter.
                Val::Sval(_) => Some(705),
                Val::Bsval(_) => None,
            },
            Node::AArrayExpr(array) => self
                .common_expr_type(array.elements.iter(), scope)
                .and_then(array_of),
            Node::TypeCast(c) => cast_type(self.schema, c),
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
                // A side with no type types the expression only when it is a parameter, which
                // takes the other side's type (`$1 + 1`); any other untyped operand (a function the
                // walk does not know) leaves the expression untyped, so a parameter compared with
                // it fails closed (42P18): `sqrt(n) + 1` was typed by the 1 alone, int4, where
                // PostgreSQL types it float8 (wire review 14 item 17).
                let param = |x: Option<&PgNode>| {
                    matches!(x.and_then(|x| x.node.as_ref()), Some(Node::ParamRef(_)))
                };
                match (l, r) {
                    // PostgreSQL has equality overloads for every integer-width pair.
                    // NULLIF returns the comparison's left operand without widening it.
                    (Some(a), Some(b))
                        if matches!(kind, AExprKind::AexprNullif)
                            && matches!(a, INT2 | INT4 | INT8)
                            && matches!(b, INT2 | INT4 | INT8) =>
                    {
                        Some(a)
                    }
                    (Some(a), Some(b))
                        if matches!(kind, AExprKind::AexprOp)
                            && matches!(op, Some("+" | "-" | "*" | "/")) =>
                    {
                        // Arithmetic overload selection differs from common-type
                        // selection: real plus an integer/numeric uses float8.
                        // Engine execution consumes this same shared OID rule.
                        Some(turso_parser::pg_types::numeric_arithmetic_result_oid(a, b)
                            .unwrap_or_else(|| wider(a, b)))
                    }
                    (Some(a), Some(b)) => Some(wider(a, b)),
                    (Some(a), None) if param(e.rexpr.as_deref()) => Some(a),
                    (None, Some(b)) if param(e.lexpr.as_deref()) => Some(b),
                    // A unary operator (`-x`) has no left side.
                    (None, Some(b)) if e.lexpr.is_none() => Some(b),
                    _ => None,
                }
            }
            Node::BoolExpr(_) | Node::NullTest(_) | Node::BooleanTest(_) => Some(BOOL),
            Node::CoalesceExpr(c) => self.common_expr_type(c.args.iter(), scope),
            Node::MinMaxExpr(m) => self.common_expr_type(m.args.iter(), scope),
            Node::CaseExpr(c) => self.common_expr_type(
                c.args
                    .iter()
                    .filter_map(|w| match w.node.as_ref() {
                        Some(Node::CaseWhen(w)) => w.result.as_deref(),
                        _ => None,
                    })
                    .chain(c.defresult.as_deref()),
                scope,
            ),
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
            #[cfg(test)]
            work: self.work.clone(),
            stack_limited: self.stack_limited.clone(),
            expressions: self.expressions.clone(),
            schema: self.schema,
            types: self.types.clone(),
            parameter_links: self.parameter_links.clone(),
            compared_untyped: std::collections::BTreeSet::new(),
            ctes: self.ctes.clone(),
            views: self.views.clone(),
            subselects: self.subselects.clone(),
            search_path: self.search_path.clone(),
            view_memo: self.view_memo.clone(),
            cuts: self.cuts.clone(),
            cut_views: self.cut_views.clone(),
            attached: self.attached,
            missing_rte: self.missing_rte.clone(),
        };
        let level = walk.from_items(&s.from_clause, scope);
        let mut inner = scope.clone();
        inner.push(level);
        let ty = walk.type_of(val, &inner).map(resolve_output);
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
const ARRAYS: [(u32, u32); 23] = [
    (16, 1000),
    (19, 1003),
    // inet, cidr, macaddr and oid (wire review 13 item 18: without them a parameter against such
    // an array column was 42P18).
    (869, 1041),
    (650, 651),
    (829, 1040),
    (26, 1028),
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

/// A registered custom/domain type uses the schema's canonical base chain.
/// Preserve a known PostgreSQL ancestor (numeric, UUID, bpchar, etc.) before its
/// SQLite storage primitive; do not guess TEXT for an unresolved terminal name.
fn resolved_type_oid(resolved: &turso_core::schema::ResolvedType) -> Option<u32> {
    if resolved.chain.iter().any(|ty| ty.is_struct() || ty.is_union()) {
        // The existing server result metadata exposes these structured values as JSONB.
        return Some(3802);
    }
    resolved
        .chain
        .iter()
        .skip(1)
        .find_map(|ty| builtin_type_oid(&ty.name))
        .or_else(|| builtin_type_oid(&resolved.primitive))
}

/// A cast is not column-scoped: resolve its registered type without a table's
/// strictness gate, as the engine's cast path does. Array bounds wrap the scalar OID.
fn cast_type(
    schema: &Schema,
    cast: &turso_pg_parser::pg_query::protobuf::TypeCast,
) -> Option<u32> {
    let type_name = cast.type_name.as_ref()?;
    let name = match type_name.names.last()?.node.as_ref()? {
        Node::String(s) => s.sval.as_str(),
        _ => return None,
    };
    let scalar = builtin_type_oid(name).or_else(|| {
        let resolved = schema.resolve_type_unchecked(name).ok()??;
        resolved_type_oid(&resolved)
    })?;
    if type_name.array_bounds.is_empty() {
        Some(scalar)
    } else {
        array_of(scalar)
    }
}

/// The type OID of column `column` of table `relname`, as declared.
fn declared_type(schema: &Schema, relname: &str, column: &str) -> Option<u32> {
    let table = schema.get_btree_table(relname)?;
    let (_, col) = table.get_column(column)?;
    column_oid(schema, col, table.is_strict)
}

/// Whether the engine's columns for the view whose query (as the engine parsed it) is `select`
/// carry real types: core types a view column from its table column only when the column is a
/// bare reference into a table (core util.rs, extract_view_columns), and gives every other one, a
/// computed column or one read through an inner view, a subquery or a table function, a
/// placeholder TEXT. So the types are kept only for a plain SELECT (no WITH, no compound) whose
/// every FROM item is a table and whose every result column is `*`, `t.*` or a bare column
/// reference; for any other view none is, all or nothing (the view's columns cannot be matched to
/// its result columns one by one once a `*` expands).
fn view_columns_traced(schema: &Schema, select: &turso_parser::ast::Select) -> bool {
    use turso_parser::ast::{Expr, OneSelect, ResultColumn, SelectTable};
    if select.with.is_some() || !select.body.compounds.is_empty() {
        return false;
    }
    let OneSelect::Select {
        columns,
        from: Some(from),
        ..
    } = &select.body.select
    else {
        return false;
    };
    let a_table = |item: &SelectTable| match item {
        SelectTable::Table(name, _, _) => {
            name.db_name
                .as_ref()
                .is_none_or(|db| db.as_str().eq_ignore_ascii_case("main"))
                && schema.get_btree_table(name.name.as_str()).is_some()
        }
        _ => false,
    };
    a_table(&*from.select)
        && from.joins.iter().all(|join| a_table(&*join.table))
        && columns.iter().all(|column| match column {
            ResultColumn::Star | ResultColumn::TableStar(_) => true,
            ResultColumn::Expr(expr, _) => matches!(
                expr.as_ref(),
                Expr::Id(_) | Expr::Qualified(_, _) | Expr::DoublyQualified(_, _, _)
            ),
        })
}

/// The type OID of a table column as declared: its type name's, or for an array column the array
/// type of it (the dimensions are kept apart from the name: an int[] column's is INTEGER), so
/// `$1 = ANY(xs)` takes xs's element type, as in PostgreSQL. Read from the name alone, an int[]
/// column was int4, its element untyped, and $1 text, which matched no integer element (wire
/// review 10 item 3). None for an array of a type with no array type here.
fn column_oid(schema: &Schema, col: &Column, is_strict: bool) -> Option<u32> {
    let base = builtin_type_oid(&col.ty_str).or_else(|| {
        // Custom declarations have type semantics only on STRICT tables. A view
        // fallback without this provenance leaves them untyped for engine metadata.
        let resolved = schema.resolve_type(&col.ty_str, is_strict).ok()??;
        resolved_type_oid(&resolved)
    })?;
    if col.array_dimensions() > 0 {
        array_of(base)
    } else {
        Some(base)
    }
}


#[cfg(test)]
mod c52_tests {
    use super::*;

    #[test]
    fn c52_parameter_chain_has_linear_inference_work() {
        const COUNT: u32 = 256;
        let constraints = (1..COUNT).map(|n| format!("${n}=${}", n + 1))
            .collect::<Vec<_>>().join(" AND ");
        let parse = turso_pg_parser::parse(&format!("SELECT $1 WHERE {constraints}")).unwrap();
        let schema = std::sync::Arc::new(Schema::default());
        let attached = Attached { names: &[], holds: &|_, _| false };
        let walked = parameter_types(&parse, &schema, &[], &Default::default(),
            &attached, &(1..=COUNT).map(|n| (n, INT4)).collect()).unwrap();
        assert_eq!(walked.params.len(), COUNT as usize);
        assert!(walked.params.values().all(|oid| *oid == INT4));
        assert_eq!(walked.columns, vec![Some(INT4)]);
        // Count tree visits instead of wall time; the accepted, fully declared
        // chain must remain within a generous linear work bound.
        assert!(walked.work < 100 * COUNT as usize, "{} inference visits", walked.work);
    }

    #[test]
    fn c52_publishes_parameter_columns_and_expressions_from_the_same_solution() {
        // Corrected from the independent PG18.6 recording: unknown reverse chains
        // are refused, so this consistency control declares all accepted types.
        // The exact unknown-chain refusals are exercised by the wire control.
        let parse = turso_pg_parser::parse("SELECT $1, $2 WHERE $1=$2 AND $2=$3").unwrap();
        let schema = std::sync::Arc::new(Schema::default());
        let attached = Attached { names: &[], holds: &|_, _| false };
        let walked = parameter_types(&parse, &schema, &[], &Default::default(),
            &attached, &std::collections::BTreeMap::from([(1, INT4), (2, INT4), (3, INT4)])).unwrap();
        assert_eq!(walked.params.get(&1), Some(&INT4));
        assert_eq!(walked.columns, vec![Some(INT4), Some(INT4)]);
        let Some(Node::SelectStmt(select)) = parse.protobuf.stmts[0].stmt.as_ref().unwrap().node.as_ref() else {
            panic!("SELECT tree");
        };
        let Some(Node::ResTarget(target)) = select.target_list[0].node.as_ref() else {
            panic!("result target");
        };
        assert_eq!(walked.expressions.get(target.val.as_deref().unwrap()), Some(INT4));
    }

    #[test]
    fn c52_scalar_select_inference_work_stays_linear() {
        let count = |depth: usize| {
            // This measures Infer, independently of the native decoder's depth
            // gate. Build the same SELECT nesting from two shallow parsed trees.
            let mut parse = turso_pg_parser::parse("SELECT 1").unwrap();
            let wrapper = turso_pg_parser::parse("SELECT (SELECT 1)").unwrap();
            let mut query = parse.protobuf.stmts[0].stmt.take().unwrap();
            for _ in 0..depth {
                let mut outer = wrapper.protobuf.stmts[0].stmt.clone().unwrap();
                let Some(Node::SelectStmt(select)) = outer.node.as_mut() else { panic!("SELECT"); };
                let Some(Node::ResTarget(target)) = select.target_list[0].node.as_mut() else { panic!("target"); };
                let Some(Node::SubLink(scalar)) = target.val.as_deref_mut().unwrap().node.as_mut() else { panic!("scalar"); };
                scalar.subselect = Some(query);
                query = outer;
            }
            parse.protobuf.stmts[0].stmt = Some(query);
            let schema = std::sync::Arc::new(Schema::default());
            let attached = Attached { names: &[], holds: &|_, _| false };
            let walked = parameter_types(&parse, &schema, &[], &Default::default(),
                &attached, &Default::default()).unwrap();
            assert_eq!(walked.columns, vec![Some(INT4)]);
            walked.work
        };
        let short = count(16);
        let long = count(32);
        assert!(long <= 3 * short, "nested scalar work: depth16={short}, depth32={long}");
    }


    #[test]
    fn c52_scalar_children_keep_recorded_parameter_contexts() {
        // Independent PG18.6 PREPARE output: pg18/c52-scalar-children.txt.
        let cases = [
            ("SELECT $1 UNION SELECT 1", INT4),
            ("SELECT row_number() OVER (ORDER BY (SELECT $1::int4))", INT4),
            ("SELECT array_agg(1 ORDER BY (SELECT $1::int4))", INT4),
            ("SELECT sum(1) OVER (ORDER BY 1 ROWS (SELECT $1::int8) PRECEDING)", INT8),
            ("SELECT (ARRAY[1,2])[(SELECT $1::int4)]", INT4),
            ("SELECT CASE WHEN true THEN $1::int4 WHEN (SELECT $1 WHERE $1=1)=1 THEN 2 ELSE 3 END", INT4),
        ];
        let schema = std::sync::Arc::new(Schema::default());
        let attached = Attached { names: &[], holds: &|_, _| false };
        let actual: Vec<_> = cases.iter().map(|(sql, _)| {
            let parse = turso_pg_parser::parse(sql).unwrap();
            let walked = parameter_types(&parse, &schema, &[], &Default::default(),
                &attached, &Default::default()).unwrap();
            walked.params.get(&1).copied()
        }).collect();
        assert_eq!(actual, cases.iter().map(|(_, oid)| Some(*oid)).collect::<Vec<_>>());
    }

}
