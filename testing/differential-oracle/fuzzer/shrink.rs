//! Statement minimization for oracle failures.
//!
//! A failing statement from the generator is often thousands of bytes of
//! nested expressions, most of which have nothing to do with the divergence.
//! This module rebuilds both engines from the failure-state dump, confirms the
//! divergence still reproduces there, and then repeatedly simplifies the
//! statement, keeping each edit only if the same kind of divergence still
//! occurs. The result is written next to the other run artifacts as
//! `minimized.sql`.
//!
//! Candidates come from the SQL parser, not from text manipulation: the
//! statement is parsed, one AST node is simplified — a clause dropped, an
//! expression replaced by `1` or `''` or one of its own children — and the
//! tree is printed back to SQL. Every candidate is therefore syntactically
//! valid, and edits cannot be confused by quotes or keywords inside literals.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::{Database, SqliteDialect};
use turso_parser::ast::{
    Cmd, Expr, InsertBody, Literal, OneSelect, ResultColumn, Select, SelectTable, Stmt, With,
};
use turso_parser::parser::Parser;

use crate::memory::MemorySimIO;
use crate::oracle::{DifferentialOracle, QueryResult};

/// Upper bound on candidate executions per shrink, so a pathological
/// statement cannot stall a fuzzing loop.
const MAX_CANDIDATES: usize = 800;

/// How the two engines diverged on a statement. Shrinking only accepts an
/// edit if the candidate reproduces the same class (and, for errors, the same
/// error prefix), so the reduction cannot drift onto a different bug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// Turso returned an error, SQLite did not. Holds an error prefix.
    TursoErr(String),
    /// SQLite returned an error, Turso did not. Holds an error prefix.
    SqliteErr(String),
    /// Both succeeded but returned different rows, or left the databases in
    /// different states.
    ResultMismatch,
}

/// Take a stable prefix of an error message for matching candidates against
/// the original failure. Long enough to pin the error kind ("integer
/// overflow", "UNIQUE constraint failed: ..."), short enough to tolerate
/// differing identifiers further into the message.
fn error_prefix(msg: &str) -> String {
    msg.chars().take(40).collect()
}

/// A fresh pair of engines with the failure state loaded.
pub struct EnginePair {
    turso: Arc<turso_core::Connection>,
    sqlite: rusqlite::Connection,
    /// Keeps the database (and its in-memory IO) alive for `turso`.
    _turso_db: Arc<Database>,
}

impl EnginePair {
    /// Build both engines and replay the state script into each. Individual
    /// statement errors during replay are ignored: the script was produced
    /// from a live database, so failures here would only come from replay
    /// artifacts, and the baseline check below decides whether the replayed
    /// state is good enough to shrink against.
    pub fn build(state_sql: &str) -> Result<Self> {
        let lease = NameLease::claim();
        let io = Arc::new(MemorySimIO::new(0));
        let opts = turso_core::DatabaseOpts::new().with_attach(true);
        let turso_db = Database::open_file_with_flags(
            io,
            &lease.name(),
            turso_core::OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )?;
        lease.hold(&turso_db);
        let turso = turso_db.connect()?;
        let sqlite = rusqlite::Connection::open_in_memory()?;
        for stmt in state_sql.lines() {
            let stmt = stmt.trim();
            if stmt.is_empty() || stmt.starts_with("--") {
                continue;
            }
            let _ = DifferentialOracle::execute_turso(&turso, stmt);
            let _ = DifferentialOracle::execute_sqlite(&sqlite, stmt);
        }
        Ok(Self {
            turso,
            sqlite,
            _turso_db: turso_db,
        })
    }

    /// Run `sql` on both engines and return both raw results (turso, sqlite).
    pub fn run_both(&self, sql: &str) -> (QueryResult, QueryResult) {
        (
            DifferentialOracle::execute_turso(&self.turso, sql),
            DifferentialOracle::execute_sqlite(&self.sqlite, sql),
        )
    }

    /// Run `sql` on both engines and classify the outcome. `None` means the
    /// engines agreed (no divergence).
    pub fn classify(&self, sql: &str) -> Option<Divergence> {
        let t = DifferentialOracle::execute_turso(&self.turso, sql);
        let s = DifferentialOracle::execute_sqlite(&self.sqlite, sql);
        match (&t, &s) {
            // Both rejected the candidate: agreement, unless Turso's error says its own
            // invariant broke. The oracle fails on those, so shrinking has to match it --
            // otherwise an internal-failure finding classifies as "no divergence" against
            // both the state dump and the history, and gets reported unminimized.
            (QueryResult::Error(te), QueryResult::Error(_)) => {
                if crate::oracle::is_internal_failure(te) {
                    Some(Divergence::TursoErr(error_prefix(te)))
                } else {
                    None
                }
            }
            (QueryResult::Error(te), _) => Some(Divergence::TursoErr(error_prefix(te))),
            (_, QueryResult::Error(se)) => Some(Divergence::SqliteErr(error_prefix(se))),
            _ => {
                if query_results_differ(&t, &s) || self.states_differ() {
                    Some(Divergence::ResultMismatch)
                } else {
                    None
                }
            }
        }
    }

    /// Compare the full contents of every table on both engines.
    pub fn states_differ(&self) -> bool {
        for db in ["main", "temp", "aux"] {
            let master = if db == "main" {
                "sqlite_master".to_string()
            } else {
                format!("{db}.sqlite_master")
            };
            let names_sql = format!("SELECT name FROM {master} WHERE type='table' ORDER BY name");
            let turso_names = DifferentialOracle::execute_turso(&self.turso, &names_sql);
            let sqlite_names = DifferentialOracle::execute_sqlite(&self.sqlite, &names_sql);
            if query_results_differ(&turso_names, &sqlite_names) {
                return true;
            }
            let QueryResult::Rows(names) = sqlite_names else {
                continue;
            };
            for row in &names {
                let Some(sql_gen_prop::SqlValue::Text(name)) = row.0.first() else {
                    continue;
                };
                let table_sql = format!("SELECT * FROM {db}.{name} ORDER BY rowid");
                let t = DifferentialOracle::execute_turso(&self.turso, &table_sql);
                let s = DifferentialOracle::execute_sqlite(&self.sqlite, &table_sql);
                if query_results_differ(&t, &s) {
                    return true;
                }
            }
        }
        false
    }
}

/// One entry per database name this process has minted for an `EnginePair`,
/// holding a weak handle to the `Database` opened under it. Slot `i` is the
/// name `shrink-{i}.db`, and it is handed out again only once that `Database`
/// is gone.
///
/// Two properties have to hold at once, and one name per pair only gives the
/// first of them:
///
/// * No two *live* pairs may share a name. `Database::open` keys a
///   process-wide registry on the path, so a second pair opening a name that
///   is still live gets handed the first pair's `Database` -- the state script
///   is then replayed into it twice while that pair's SQLite side saw it once,
///   which manufactures a `ResultMismatch` on statements the engines agree on.
/// * The number of distinct names must stay bounded. That same registry keeps
///   a `Weak` per key and never prunes, and a live `Weak` holds the whole
///   `Database` allocation open after the value inside it is dropped. A name
///   per candidate therefore stranded one `Database` per candidate -- up to
///   1600 per shrink pass, times eight passes, times every failing iteration
///   of `loop N`.
///
/// Reusing a dead name satisfies both: the registry's `Weak` for it can no
/// longer upgrade, so the reopen builds a fresh `Database` rather than sharing
/// one, and the key count stops at the number of pairs alive at the same time.
static DATABASE_NAMES: Mutex<Vec<NameSlot>> = Mutex::new(Vec::new());

enum NameSlot {
    /// A pair is being built on this name. Not free even though no `Database`
    /// exists yet.
    Building,
    /// A pair was built on this name; free again once the handle is dead.
    Held(Weak<Database>),
}

/// Holds one name for as long as a pair needs it. Dropping the lease before
/// [`NameLease::hold`] runs -- the open failed -- frees the name immediately.
struct NameLease(usize);

impl NameLease {
    fn claim() -> Self {
        let mut slots = lock_database_names();
        let free = slots
            .iter()
            .position(|slot| matches!(slot, NameSlot::Held(db) if db.strong_count() == 0));
        match free {
            Some(index) => {
                slots[index] = NameSlot::Building;
                Self(index)
            }
            None => {
                slots.push(NameSlot::Building);
                Self(slots.len() - 1)
            }
        }
    }

    fn name(&self) -> String {
        format!("shrink-{}.db", self.0)
    }

    /// Tie the name to the database opened under it, so it comes free again
    /// when that database is dropped and not before.
    fn hold(&self, db: &Arc<Database>) {
        lock_database_names()[self.0] = NameSlot::Held(Arc::downgrade(db));
    }
}

impl Drop for NameLease {
    fn drop(&mut self) {
        let mut slots = lock_database_names();
        if matches!(slots[self.0], NameSlot::Building) {
            slots[self.0] = NameSlot::Held(Weak::new());
        }
    }
}

/// A test that panics mid-build poisons the pool; the remaining tests still
/// need names, and a poisoned pool is not a corrupt one.
fn lock_database_names() -> std::sync::MutexGuard<'static, Vec<NameSlot>> {
    DATABASE_NAMES.lock().unwrap_or_else(|e| e.into_inner())
}

/// How many distinct database names the shrinker has minted, which is also how
/// many keys it has added to `turso_core`'s process-wide database registry.
#[cfg(test)]
fn database_names_minted() -> usize {
    lock_database_names().len()
}

/// How many of those names a live pair is holding right now.
#[cfg(test)]
fn database_names_in_use() -> usize {
    lock_database_names()
        .iter()
        .filter(|slot| !matches!(slot, NameSlot::Held(db) if db.strong_count() == 0))
        .count()
}

pub fn query_results_differ(a: &QueryResult, b: &QueryResult) -> bool {
    match (a, b) {
        (QueryResult::Rows(ra), QueryResult::Rows(rb)) => {
            !sql_gen_prop::result::diff_results(ra, rb).is_empty()
        }
        (QueryResult::Ok, QueryResult::Ok) => false,
        // Both errored is agreement, unless Turso's error says its own invariant broke.
        // differential_probe compares through this function rather than through
        // oracle.rs, so both comparators need the same rule or the probe reports
        // "0 diverged" and exits 0 on a real internal failure.
        (QueryResult::Error(turso_err), QueryResult::Error(_)) => {
            crate::oracle::is_internal_failure(turso_err)
        }
        // Ok vs empty Rows means the same thing here: no differing rows.
        (QueryResult::Ok, QueryResult::Rows(r)) | (QueryResult::Rows(r), QueryResult::Ok) => {
            !r.is_empty()
        }
        _ => true,
    }
}

/// `true` if `candidate` reproduces the same kind of divergence as `original`.
fn matches_divergence(original: &Divergence, candidate: Option<Divergence>) -> bool {
    match (original, candidate) {
        (Divergence::TursoErr(p), Some(Divergence::TursoErr(q))) => p == &q,
        (Divergence::SqliteErr(p), Some(Divergence::SqliteErr(q))) => p == &q,
        (Divergence::ResultMismatch, Some(Divergence::ResultMismatch)) => true,
        _ => false,
    }
}

// --- candidate generation ------------------------------------------------

fn parse_one(sql: &str) -> Option<Stmt> {
    let mut parser = Parser::new(sql.as_bytes());
    match parser.next()?.ok()? {
        Cmd::Stmt(stmt) => Some(stmt),
        _ => None,
    }
}

/// Ways to simplify one expression node.
#[derive(Clone, Copy)]
enum ExprAction {
    /// Replace the node with the literal 1.
    One,
    /// Replace the node with the literal ''.
    EmptyText,
    /// Replace the node with its n-th expression child (`x AND y` -> `x`,
    /// `RTRIM(a)` -> `a`).
    Child(usize),
    /// Remove the n-th element of the node's own list (a function argument or
    /// an IN-list value), keeping the node itself.
    DropListItem(usize),
}

/// Direct expression children that can stand in for the whole node.
fn expr_children(e: &mut Expr) -> Vec<&mut Expr> {
    match e {
        Expr::Between {
            lhs, start, end, ..
        } => vec![lhs, start, end],
        Expr::Binary(l, _, r) => vec![l, r],
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let mut v: Vec<&mut Expr> = Vec::new();
            if let Some(b) = base {
                v.push(b);
            }
            for (w, t) in when_then_pairs {
                v.push(w);
                v.push(t);
            }
            if let Some(x) = else_expr {
                v.push(x);
            }
            v
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr)
        | Expr::FieldAccess { base: expr, .. } => vec![expr],
        Expr::FunctionCall { args, .. } => args.iter_mut().map(|a| &mut **a).collect(),
        Expr::InList { lhs, rhs, .. } => {
            let mut v: Vec<&mut Expr> = vec![lhs];
            v.extend(rhs.iter_mut().map(|a| &mut **a));
            v
        }
        Expr::InSelect { lhs, .. } => vec![lhs],
        Expr::InTable { lhs, args, .. } => {
            let mut v: Vec<&mut Expr> = vec![lhs];
            v.extend(args.iter_mut().map(|a| &mut **a));
            v
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            let mut v: Vec<&mut Expr> = vec![lhs, rhs];
            if let Some(esc) = escape {
                v.push(esc);
            }
            v
        }
        Expr::Parenthesized(exprs) => exprs.iter_mut().map(|a| &mut **a).collect(),
        Expr::Raise(_, Some(expr)) => vec![expr],
        Expr::Subscript { base, index } => vec![base, index],
        Expr::Array { elements } => elements.iter_mut().map(|a| &mut **a).collect(),
        _ => vec![],
    }
}

/// Actions worth trying on this node, cheapest-to-verify structural wins
/// first. Skips no-ops like replacing `1` with `1`.
fn expr_actions(e: &Expr) -> Vec<ExprAction> {
    let mut actions = Vec::new();
    let child_count = match e {
        Expr::Between { .. } => 3,
        Expr::Binary(..) | Expr::Subscript { .. } => 2,
        Expr::Case { .. } => 0, // covered by One; branches vary in count
        Expr::Cast { .. }
        | Expr::Collate(..)
        | Expr::IsNull(..)
        | Expr::NotNull(..)
        | Expr::Unary(..)
        | Expr::FieldAccess { .. } => 1,
        Expr::FunctionCall { args, .. } => args.len().min(1),
        Expr::InList { .. } | Expr::InSelect { .. } | Expr::InTable { .. } => 1,
        Expr::Like { .. } => 2,
        Expr::Parenthesized(exprs) => exprs.len().min(1),
        _ => 0,
    };
    for i in 0..child_count {
        actions.push(ExprAction::Child(i));
    }
    match e {
        Expr::FunctionCall { args, .. } if args.len() > 1 => {
            for i in 0..args.len() {
                actions.push(ExprAction::DropListItem(i));
            }
        }
        Expr::InList { rhs, .. } if rhs.len() > 1 => {
            for i in 0..rhs.len() {
                actions.push(ExprAction::DropListItem(i));
            }
        }
        _ => {}
    }
    if !matches!(e, Expr::Literal(Literal::Numeric(n)) if n == "1") {
        actions.push(ExprAction::One);
    }
    if !matches!(e, Expr::Literal(Literal::String(s)) if s == "''") {
        actions.push(ExprAction::EmptyText);
    }
    actions
}

fn apply_expr_action(e: &mut Expr, action: ExprAction) {
    match action {
        ExprAction::One => *e = Expr::Literal(Literal::Numeric("1".to_string())),
        ExprAction::EmptyText => *e = Expr::Literal(Literal::String("''".to_string())),
        ExprAction::Child(i) => {
            let mut children = expr_children(e);
            if i < children.len() {
                let child = std::mem::take(children[i]);
                *e = child;
            }
        }
        ExprAction::DropListItem(i) => match e {
            Expr::FunctionCall { args, .. } if i < args.len() && args.len() > 1 => {
                args.remove(i);
            }
            Expr::InList { rhs, .. } if i < rhs.len() && rhs.len() > 1 => {
                rhs.remove(i);
            }
            _ => {}
        },
    }
}

/// Clauses that can be dropped from one SELECT/UPDATE/DELETE site.
#[derive(Clone, Copy)]
enum ClauseDrop {
    Where,
    GroupBy,
    Having,
    Distinct,
    SelectColumn(usize),
    OrderByItem(usize),
    Limit,
    Compound(usize),
    Cte(usize),
    Join(usize),
    SetItem(usize),
    ReturningItem(usize),
}

/// A place where clause-level edits apply.
enum Site<'a> {
    Core(&'a mut OneSelect),
    Outer(&'a mut Select),
    Update(&'a mut turso_parser::ast::Update),
    Delete {
        where_clause: &'a mut Option<Box<Expr>>,
        returning: &'a mut Vec<ResultColumn>,
    },
    InsertReturning(&'a mut Vec<ResultColumn>),
}

fn site_actions(site: &Site<'_>) -> Vec<ClauseDrop> {
    let mut actions = Vec::new();
    match site {
        Site::Core(OneSelect::Select {
            distinctness,
            columns,
            where_clause,
            group_by,
            ..
        }) => {
            if where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            if let Some(gb) = group_by {
                actions.push(ClauseDrop::GroupBy);
                if gb.having.is_some() {
                    actions.push(ClauseDrop::Having);
                }
            }
            if distinctness.is_some() {
                actions.push(ClauseDrop::Distinct);
            }
            if columns.len() > 1 {
                for i in 0..columns.len() {
                    actions.push(ClauseDrop::SelectColumn(i));
                }
            }
        }
        Site::Core(OneSelect::Values(..)) => {}
        Site::Outer(select) => {
            for i in 0..select.order_by.len() {
                actions.push(ClauseDrop::OrderByItem(i));
            }
            if select.limit.is_some() {
                actions.push(ClauseDrop::Limit);
            }
            for i in 0..select.body.compounds.len() {
                actions.push(ClauseDrop::Compound(i));
            }
            if let Some(with) = &select.with {
                for i in 0..with.ctes.len() {
                    actions.push(ClauseDrop::Cte(i));
                }
            }
            if let OneSelect::Select {
                from: Some(from), ..
            } = &select.body.select
            {
                for i in 0..from.joins.len() {
                    actions.push(ClauseDrop::Join(i));
                }
            }
        }
        Site::Update(update) => {
            if update.where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            if update.sets.len() > 1 {
                for i in 0..update.sets.len() {
                    actions.push(ClauseDrop::SetItem(i));
                }
            }
            for i in 0..update.returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
        Site::Delete {
            where_clause,
            returning,
        } => {
            if where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            for i in 0..returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
        Site::InsertReturning(returning) => {
            for i in 0..returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
    }
    actions
}

fn apply_clause_action(site: &mut Site<'_>, action: ClauseDrop) {
    match (site, action) {
        (Site::Core(OneSelect::Select { where_clause, .. }), ClauseDrop::Where) => {
            *where_clause = None
        }
        (Site::Core(OneSelect::Select { group_by, .. }), ClauseDrop::GroupBy) => *group_by = None,
        (
            Site::Core(OneSelect::Select {
                group_by: Some(gb), ..
            }),
            ClauseDrop::Having,
        ) => gb.having = None,
        (Site::Core(OneSelect::Select { distinctness, .. }), ClauseDrop::Distinct) => {
            *distinctness = None
        }
        (Site::Core(OneSelect::Select { columns, .. }), ClauseDrop::SelectColumn(i)) => {
            if i < columns.len() && columns.len() > 1 {
                columns.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::OrderByItem(i)) => {
            if i < select.order_by.len() {
                select.order_by.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::Limit) => select.limit = None,
        (Site::Outer(select), ClauseDrop::Compound(i)) => {
            if i < select.body.compounds.len() {
                select.body.compounds.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::Cte(i)) => {
            if let Some(with) = &mut select.with {
                if i < with.ctes.len() {
                    with.ctes.remove(i);
                    if with.ctes.is_empty() {
                        select.with = None;
                    }
                }
            }
        }
        (Site::Outer(select), ClauseDrop::Join(i)) => {
            if let OneSelect::Select {
                from: Some(from), ..
            } = &mut select.body.select
            {
                if i < from.joins.len() {
                    from.joins.remove(i);
                }
            }
        }
        (Site::Update(update), ClauseDrop::Where) => update.where_clause = None,
        (Site::Update(update), ClauseDrop::SetItem(i)) => {
            if i < update.sets.len() && update.sets.len() > 1 {
                update.sets.remove(i);
            }
        }
        (Site::Update(update), ClauseDrop::ReturningItem(i)) => {
            if i < update.returning.len() {
                update.returning.remove(i);
            }
        }
        (Site::Delete { where_clause, .. }, ClauseDrop::Where) => **where_clause = None,
        (Site::Delete { returning, .. }, ClauseDrop::ReturningItem(i))
        | (Site::InsertReturning(returning), ClauseDrop::ReturningItem(i)) => {
            if i < returning.len() {
                returning.remove(i);
            }
        }
        _ => {}
    }
}

/// What one walk over a statement reports: expressions, selects, or both.
///
/// The expression edits and the recursive-CTE guard that has to see them run
/// over this same traversal. They used to run over two -- the guard over
/// `walk_sites`, the edits over `walk_exprs` -- and `walk_exprs` reaches
/// strictly more selects, so a `WITH RECURSIVE` inside an `EXISTS`, a scalar
/// subquery or an `IN (SELECT ...)` was invisible to the guard and editable by
/// the edits.
trait Visitor {
    /// Returning `true` stops the walk.
    fn expr(&mut self, _e: &mut Expr) -> bool {
        false
    }

    /// Returning `true` stops the walk.
    fn select(&mut self, _select: &mut Select) -> bool {
        false
    }
}

/// Pre-order walk over every expression in the statement, including inside
/// subqueries and CTE bodies. `f` returning `true` stops the walk (used to
/// apply an edit at one position).
fn walk_exprs(stmt: &mut Stmt, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    struct Exprs<'f>(&'f mut dyn FnMut(&mut Expr) -> bool);
    impl Visitor for Exprs<'_> {
        fn expr(&mut self, e: &mut Expr) -> bool {
            (self.0)(e)
        }
    }
    walk_stmt(stmt, &mut Exprs(f))
}

/// Pre-order walk over the whole statement, reporting to `v`.
fn walk_stmt(stmt: &mut Stmt, v: &mut dyn Visitor) -> bool {
    match stmt {
        Stmt::Select(select) => walk_select(select, v),
        Stmt::Insert {
            body, returning, ..
        } => {
            if let InsertBody::Select(select, _) = body {
                if walk_select(select, v) {
                    return true;
                }
            }
            walk_result_columns(returning, v)
        }
        Stmt::Update(update) => {
            if let Some(with) = &mut update.with {
                for cte in &mut with.ctes {
                    if walk_select(&mut cte.select, v) {
                        return true;
                    }
                }
            }
            for set in &mut update.sets {
                if walk_expr(&mut set.expr, v) {
                    return true;
                }
            }
            if let Some(from) = &mut update.from {
                if walk_from(from, v) {
                    return true;
                }
            }
            if let Some(w) = &mut update.where_clause {
                if walk_expr(w, v) {
                    return true;
                }
            }
            walk_result_columns(&mut update.returning, v)
        }
        Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            if let Some(with) = with {
                for cte in &mut with.ctes {
                    if walk_select(&mut cte.select, v) {
                        return true;
                    }
                }
            }
            if let Some(w) = where_clause {
                if walk_expr(w, v) {
                    return true;
                }
            }
            walk_result_columns(returning, v)
        }
        _ => false,
    }
}

fn walk_limit(
    limit: &mut Option<turso_parser::ast::Limit>,
    v: &mut dyn Visitor,
) -> bool {
    if let Some(limit) = limit {
        if walk_expr(&mut limit.expr, v) {
            return true;
        }
        if let Some(offset) = &mut limit.offset {
            if walk_expr(offset, v) {
                return true;
            }
        }
    }
    false
}

fn walk_result_columns(columns: &mut [ResultColumn], v: &mut dyn Visitor) -> bool {
    for rc in columns {
        if let ResultColumn::Expr(expr, _) = rc {
            if walk_expr(expr, v) {
                return true;
            }
        }
    }
    false
}

fn walk_select(select: &mut Select, v: &mut dyn Visitor) -> bool {
    if v.select(select) {
        return true;
    }
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            if walk_select(&mut cte.select, v) {
                return true;
            }
        }
    }
    if walk_one_select(&mut select.body.select, v) {
        return true;
    }
    for compound in &mut select.body.compounds {
        if walk_one_select(&mut compound.select, v) {
            return true;
        }
    }
    for sc in &mut select.order_by {
        if walk_expr(&mut sc.expr, v) {
            return true;
        }
    }
    walk_limit(&mut select.limit, v)
}

fn walk_one_select(one: &mut OneSelect, v: &mut dyn Visitor) -> bool {
    match one {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            ..
        } => {
            if walk_result_columns(columns, v) {
                return true;
            }
            if let Some(from) = from {
                if walk_from(from, v) {
                    return true;
                }
            }
            if let Some(w) = where_clause {
                if walk_expr(w, v) {
                    return true;
                }
            }
            if let Some(gb) = group_by {
                for e in &mut gb.exprs {
                    if walk_expr(e, v) {
                        return true;
                    }
                }
                if let Some(h) = &mut gb.having {
                    if walk_expr(h, v) {
                        return true;
                    }
                }
            }
            false
        }
        OneSelect::Values(rows) => {
            for row in rows {
                for e in row {
                    if walk_expr(e, v) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn walk_from(
    from: &mut turso_parser::ast::FromClause,
    v: &mut dyn Visitor,
) -> bool {
    if walk_select_table(&mut from.select, v) {
        return true;
    }
    for join in &mut from.joins {
        if walk_select_table(&mut join.table, v) {
            return true;
        }
        if let Some(turso_parser::ast::JoinConstraint::On(e)) = &mut join.constraint {
            if walk_expr(e, v) {
                return true;
            }
        }
    }
    false
}

fn walk_select_table(table: &mut SelectTable, v: &mut dyn Visitor) -> bool {
    match table {
        SelectTable::Table(..) => false,
        SelectTable::TableCall(_, args, _) => {
            for a in args {
                if walk_expr(a, v) {
                    return true;
                }
            }
            false
        }
        SelectTable::Select(select, _) => walk_select(select, v),
        SelectTable::Sub(from, _) => walk_from(from, v),
    }
}

fn walk_expr(e: &mut Expr, v: &mut dyn Visitor) -> bool {
    if v.expr(e) {
        return true;
    }
    // Subquery-carrying variants recurse through the select walker; everything
    // else recurses through its expression children.
    match e {
        Expr::Exists(select) | Expr::Subquery(select) => return walk_select(select, v),
        Expr::InSelect { lhs, rhs, .. } => {
            if walk_expr(lhs, v) {
                return true;
            }
            return walk_select(rhs, v);
        }
        _ => {}
    }
    for child in expr_children(e) {
        if walk_expr(child, v) {
            return true;
        }
    }
    false
}

/// Walk every clause site. `f` returning `true` stops the walk.
fn walk_sites(stmt: &mut Stmt, f: &mut dyn FnMut(&mut Site<'_>) -> bool) -> bool {
    match stmt {
        Stmt::Select(select) => walk_select_sites(select, f),
        Stmt::Insert {
            body, returning, ..
        } => {
            if let InsertBody::Select(select, _) = body {
                if walk_select_sites(select, f) {
                    return true;
                }
            }
            f(&mut Site::InsertReturning(returning))
        }
        Stmt::Update(update) => {
            if let Some(with) = &mut update.with {
                for cte in &mut with.ctes {
                    if walk_select_sites(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            if let Some(from) = &mut update.from {
                if walk_from_sites(from, f) {
                    return true;
                }
            }
            f(&mut Site::Update(update))
        }
        Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            if let Some(with) = with {
                for cte in &mut with.ctes {
                    if walk_select_sites(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            f(&mut Site::Delete {
                where_clause,
                returning,
            })
        }
        _ => false,
    }
}

fn walk_select_sites(select: &mut Select, f: &mut dyn FnMut(&mut Site<'_>) -> bool) -> bool {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            if walk_select_sites(&mut cte.select, f) {
                return true;
            }
        }
    }
    if f(&mut Site::Outer(select)) {
        return true;
    }
    if f(&mut Site::Core(&mut select.body.select)) {
        return true;
    }
    for compound in &mut select.body.compounds {
        if f(&mut Site::Core(&mut compound.select)) {
            return true;
        }
    }
    // Subquery sites inside FROM.
    if let OneSelect::Select {
        from: Some(from), ..
    } = &mut select.body.select
    {
        if walk_from_sites(from, f) {
            return true;
        }
    }
    false
}

fn walk_from_sites(
    from: &mut turso_parser::ast::FromClause,
    f: &mut dyn FnMut(&mut Site<'_>) -> bool,
) -> bool {
    if let SelectTable::Select(select, _) = &mut *from.select {
        if walk_select_sites(select, f) {
            return true;
        }
    }
    for join in &mut from.joins {
        if let SelectTable::Select(select, _) = &mut *join.table {
            if walk_select_sites(select, f) {
                return true;
            }
        }
    }
    false
}

/// All simplified renderings of `sql`, structural (clause) edits first, then
/// expression edits in pre-order so outer nodes are tried before inner ones.
fn candidates(sql: &str) -> Vec<String> {
    let Some(stmt) = parse_one(sql) else {
        return vec![];
    };
    if has_recursive_cte(&stmt) {
        // Editing a recursive CTE can delete what makes it stop -- dropping `WHERE x < 3`
        // from the recursive arm, or replacing that guard with `1` -- and the candidate
        // then runs forever inside a single `classify`, where a deadline checked between
        // candidates never gets a turn. The original statement came from a completed run,
        // so it terminates; no edit of it can promise the same.
        tracing::info!(
            "Statement contains a recursive CTE; leaving it alone and reducing only the state script"
        );
        return vec![];
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    // Clause edits.
    let mut site_action_counts = Vec::new();
    {
        let mut probe = stmt.clone();
        walk_sites(&mut probe, &mut |site| {
            site_action_counts.push(site_actions(site).len());
            false
        });
    }
    for (site_idx, &count) in site_action_counts.iter().enumerate() {
        for action_idx in 0..count {
            let mut cand = stmt.clone();
            let mut pos = 0usize;
            walk_sites(&mut cand, &mut |site| {
                if pos == site_idx {
                    let actions = site_actions(site);
                    if let Some(&action) = actions.get(action_idx) {
                        apply_clause_action(site, action);
                    }
                    return true;
                }
                pos += 1;
                false
            });
            push_candidate(&cand, sql, &mut seen, &mut out);
        }
    }

    // Expression edits.
    let mut expr_action_counts = Vec::new();
    {
        let mut probe = stmt.clone();
        walk_exprs(&mut probe, &mut |e| {
            expr_action_counts.push(expr_actions(e).len());
            false
        });
    }
    for (expr_idx, &count) in expr_action_counts.iter().enumerate() {
        for action_idx in 0..count {
            let mut cand = stmt.clone();
            let mut pos = 0usize;
            walk_exprs(&mut cand, &mut |e| {
                if pos == expr_idx {
                    let actions = expr_actions(e);
                    if let Some(&action) = actions.get(action_idx) {
                        apply_expr_action(e, action);
                    }
                    return true;
                }
                pos += 1;
                false
            });
            push_candidate(&cand, sql, &mut seen, &mut out);
        }
    }
    out
}

/// `true` when any part of `stmt` is a `WITH RECURSIVE`.
///
/// Runs over [`walk_stmt`], the traversal the expression edits use. That
/// traversal reaches every select `walk_sites` reaches and more -- the selects
/// under `EXISTS`, a scalar subquery or `IN (SELECT ...)`, and the FROM
/// subqueries of compound arms -- so one walk covers both families of edit. A
/// guard on the narrower walk missed, for instance, the `WITH RECURSIVE`
/// inside `SELECT 1 FROM t WHERE EXISTS (WITH RECURSIVE ...)`, and the edits
/// went on to replace that CTE's stopping condition with `1`.
fn has_recursive_cte(stmt: &Stmt) -> bool {
    fn is_recursive(with: &Option<With>) -> bool {
        with.as_ref().is_some_and(|w| w.recursive)
    }
    match stmt {
        Stmt::Insert { with, .. } | Stmt::Delete { with, .. } if is_recursive(with) => return true,
        Stmt::Update(update) if is_recursive(&update.with) => return true,
        _ => {}
    }

    struct FindRecursive(bool);
    impl Visitor for FindRecursive {
        fn select(&mut self, select: &mut Select) -> bool {
            if is_recursive(&select.with) {
                self.0 = true;
            }
            self.0
        }
    }
    let mut find = FindRecursive(false);
    walk_stmt(&mut stmt.clone(), &mut find);
    find.0
}

/// Render a candidate and keep it when it is new and strictly shorter, which
/// also guarantees the shrink loop terminates.
fn push_candidate(cand: &Stmt, original: &str, seen: &mut HashSet<String>, out: &mut Vec<String>) {
    let rendered = cand.to_string();
    if rendered.len() < original.len() && seen.insert(rendered.clone()) {
        out.push(rendered);
    }
}

// --- driver -------------------------------------------------------------

/// When a shrink must stop regardless of how much reduction is left. Every
/// candidate rebuilds both engines and replays the whole state script, so the
/// attempt budgets bound the number of candidates but not the wall clock.
/// Whatever a shrink holds when time runs out still reproduces the divergence,
/// so stopping early ships a larger `minimized.sql` rather than none.
struct Deadline {
    at: Instant,
    /// Expire after this many `expired()` calls instead of at `at`.
    ///
    /// Only tests set it. A test that wants a sweep cut in the middle cannot
    /// say so in milliseconds: `candidates` parses, clones, renders and
    /// de-duplicates the whole list before the first check, and in a debug
    /// build on a loaded machine that alone can outlast any budget small
    /// enough to cut the sweep -- so the sweep stops having judged nothing and
    /// the test fails on a machine, not on a bug.
    checks_allowed: Option<usize>,
    checks_done: AtomicUsize,
}

impl Deadline {
    fn after(budget: Duration) -> Self {
        Self {
            at: Instant::now() + budget,
            checks_allowed: None,
            checks_done: AtomicUsize::new(0),
        }
    }

    /// A deadline that passes once `judges` candidates have been checked
    /// against it, whatever the clock says.
    #[cfg(test)]
    fn after_judging(judges: usize) -> Self {
        Self {
            at: Instant::now() + Duration::from_secs(3600),
            checks_allowed: Some(judges),
            checks_done: AtomicUsize::new(0),
        }
    }

    fn expired(&self) -> bool {
        if let Some(allowed) = self.checks_allowed {
            return self.checks_done.fetch_add(1, Ordering::Relaxed) >= allowed;
        }
        Instant::now() >= self.at
    }
}

/// Repeatedly apply the first candidate edit that `judge` accepts, until no
/// edit is accepted, the attempt budget runs out, or `deadline` passes.
fn shrink_with(
    initial: &str,
    deadline: &Deadline,
    mut judge: impl FnMut(&str) -> Result<bool>,
) -> Result<String> {
    let mut current = initial.to_string();
    let mut attempts = 0usize;
    let mut progress = true;
    while progress && attempts < MAX_CANDIDATES {
        progress = false;
        for candidate in candidates(&current) {
            attempts += 1;
            if attempts >= MAX_CANDIDATES || deadline.expired() {
                break;
            }
            if judge(&candidate)? {
                current = candidate;
                progress = true;
                break;
            }
        }
    }
    tracing::info!(
        "Shrink finished: {} -> {} bytes in {attempts} attempts",
        initial.len(),
        current.len()
    );
    Ok(current)
}

/// Classic ddmin-style list reduction: repeatedly try dropping chunks of
/// lines from the state script, keeping a deletion when `judge` still accepts
/// the remaining script. Halves the chunk size down to single lines, and stops
/// early once `deadline` passes.
fn reduce_state_lines(
    state_sql: &str,
    deadline: &Deadline,
    mut judge: impl FnMut(&str) -> Result<bool>,
) -> Result<String> {
    let mut lines: Vec<&str> = state_sql
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))
        .collect();
    let mut chunk = lines.len().div_ceil(2).max(1);
    let mut attempts = 0usize;
    loop {
        let mut i = 0;
        let mut deleted_any = false;
        while i < lines.len() && attempts < MAX_CANDIDATES && !deadline.expired() {
            let end = (i + chunk).min(lines.len());
            let mut candidate: Vec<&str> = Vec::with_capacity(lines.len());
            candidate.extend_from_slice(&lines[..i]);
            candidate.extend_from_slice(&lines[end..]);
            attempts += 1;
            if judge(&candidate.join("\n"))? {
                lines = candidate;
                deleted_any = true;
                // Do not advance: the next chunk slid into position i.
            } else {
                i = end;
            }
        }
        if attempts >= MAX_CANDIDATES || (chunk == 1 && !deleted_any) {
            break;
        }
        chunk = (chunk / 2).max(1);
    }
    Ok(lines.join("\n"))
}

/// The minimized reproduction for an oracle failure.
pub struct Minimized {
    /// The reduced state script the divergence needs.
    pub state_sql: String,
    /// The reduced failing statement.
    pub statement: String,
}

/// Minimize `failing_sql` — and then the state script it needs — against the
/// state in `state_sql`, falling back to `history_sql` (the run's executed
/// statements) when the divergence needs the statement history rather than
/// just the final data. Returns None when neither replay reproduces it.
///
/// The fallback matters for history-dependent bugs: a wrong trigger firing
/// order, for example, leaves a table whose *contents* replay cleanly from a
/// dump, so the dump-based baseline shows no divergence — but replaying the
/// statements that built the table hits the divergence again. The ddmin pass
/// then deletes every history line the divergence does not need.
pub fn shrink_statement(
    state_sql: &str,
    history_sql: &str,
    failing_sql: &str,
) -> Result<Option<Minimized>> {
    let deadline = Deadline::after(SHRINK_TIME_BUDGET);
    let (best, _) = shrink_passes(state_sql, failing_sql, &deadline, |state_in, stmt_in| {
        shrink_one_pass(state_in, history_sql, stmt_in, &deadline)
    })?;
    Ok(best)
}

/// How many times to re-enter the shrinker before giving up on further
/// progress. A bound rather than a `while`, so a pathological input cannot
/// loop forever.
const MAX_SHRINK_PASSES: usize = 8;

/// Wall clock the whole shrink may spend: a fifth of the 30-minute cap on the
/// fuzzer jobs in `.github/workflows/rust.yml`.
///
/// What a pass costs is set by how many candidates it judges times how long
/// one `EnginePair::build` takes, because every candidate rebuilds both
/// engines and replays the whole state script. The `measure_one_shrink_pass`
/// test below timed a build at 19 ms against a 200-line state script and 90 ms
/// against a 1000-line one, and timed a whole pass over a 33-candidate
/// statement at 7.5 s and 83 s (debug build, `cargo test -p differential-fuzzer
/// --lib measure_one_shrink_pass -- --ignored --nocapture`, Apple M5 Pro,
/// 2026-08-29, load average 18 -- an idle machine would be faster). A pass over
/// a real failing statement judges more: at worst `MAX_CANDIDATES` statement
/// candidates plus `MAX_CANDIDATES` state deletions, so about 30 s on a
/// 200-line script and about two and a half minutes on a 1000-line one. This
/// budget is therefore several passes either way.
///
/// It bounds the shrink, not the job: the run that failed has already spent an
/// unknown part of the cap, and nothing here can see how much is left, so a
/// divergence found late enough still loses its artifacts to the job timeout.
const SHRINK_TIME_BUDGET: Duration = Duration::from_secs(6 * 60);

/// Re-enter `run_pass` until it stops making the reproduction smaller.
/// `MAX_CANDIDATES` bounds one pass, so a pass that ends by exhausting that
/// budget stopped on the budget rather than on the algorithm's own fixpoint,
/// and feeding its output back in reduces further.
///
/// Each pass starts from the smallest reproduction so far, and that only moves
/// to a strictly smaller one. A pass that has to fall back to the statement
/// history returns an artifact on a different basis, which is routinely larger
/// than the one it replaced; the smaller reproduction is the one worth
/// keeping, and a pass that cannot beat it is the signal to stop.
fn shrink_passes(
    state_sql: &str,
    failing_sql: &str,
    deadline: &Deadline,
    mut run_pass: impl FnMut(&str, &str) -> Result<Option<PassResult>>,
) -> Result<(Option<Minimized>, Stop)> {
    let mut best: Option<Minimized> = None;
    let mut completed = 0usize;
    let mut stop = Stop::PassBound;
    for pass in 1..=MAX_SHRINK_PASSES {
        if pass > 1 && deadline.expired() {
            stop = Stop::OutOfTime;
            break;
        }
        let (state_in, stmt_in) = match &best {
            None => (state_sql, failing_sql),
            Some(m) => (m.state_sql.as_str(), m.statement.as_str()),
        };
        let Some(next) = run_pass(state_in, stmt_in)? else {
            // Nothing on the FIRST pass means the divergence does not reproduce at
            // all. A later pass finding nothing means this pass lost a divergence
            // the previous one still had, so keep the earlier result.
            stop = if best.is_some() {
                Stop::LostDivergence
            } else {
                Stop::NoReproduction
            };
            break;
        };
        completed = pass;
        // Measure against what the pass actually shrank. A pass that switched to the
        // statement history reduced the history, so comparing its output with the
        // state dump it was handed compares unrelated quantities.
        let before = best.as_ref().map_or(next.input_len, artifact_len);
        let after = artifact_len(&next.minimized);
        let progressed = after < before;
        // A pass that fell back to the statement history did not reduce the artifact
        // this loop is tracking; it reduced the other script. Its result is comparable
        // to nothing the loop holds, so failing to beat the best is not the algorithm
        // running out of edits.
        let switched_basis = best.is_some() && next.basis == Basis::History;
        // A first pass that reduced nothing still produced the only reproduction there is.
        if progressed || best.is_none() {
            best = Some(next.minimized);
        }
        if !progressed {
            // A pass the deadline cut short returns its input unchanged, which looks
            // exactly like a fixpoint and is not one.
            stop = if deadline.expired() {
                Stop::OutOfTime
            } else if switched_basis {
                Stop::SwitchedBasisAndDidNotShrink
            } else {
                Stop::Fixpoint
            };
            break;
        }
        tracing::info!("Shrink pass {pass}: {before} -> {after} bytes");
    }
    match stop {
        Stop::Fixpoint => tracing::info!("Shrink reached a fixpoint after {completed} pass(es)"),
        Stop::PassBound => tracing::warn!(
            "Shrink stopped at the {MAX_SHRINK_PASSES}-pass bound while still making \
             progress; this result is not a fixpoint"
        ),
        Stop::OutOfTime => tracing::warn!(
            "Shrink ran out of time after {completed} pass(es); this result is not a fixpoint"
        ),
        Stop::LostDivergence => tracing::warn!(
            "Shrink pass {} no longer reproduces the divergence; keeping the result from \
             pass {completed}",
            completed + 1
        ),
        Stop::SwitchedBasisAndDidNotShrink => tracing::warn!(
            "Shrink pass {completed} reduced the statement history and came back no \
             smaller than pass {}'s result; keeping that one. No pass reached a fixpoint",
            completed - 1
        ),
        Stop::NoReproduction => {}
    }
    Ok((best, stop))
}

/// Why the pass loop stopped. Only one of these means the reproduction is as small as
/// this algorithm gets it, and logging the wrong one is the same class of false claim
/// the loop itself exists to fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// A pass changed nothing.
    Fixpoint,
    /// Still shrinking when the pass bound ran out.
    PassBound,
    /// Still shrinking when the clock ran out.
    OutOfTime,
    /// A later pass stopped reproducing the divergence; an earlier result is kept.
    LostDivergence,
    /// A later pass fell back to the statement history and came back no smaller than
    /// the result already held, which is kept. The pass before it was still making
    /// progress and this one measured a different script, so nothing here reached a
    /// fixpoint.
    SwitchedBasisAndDidNotShrink,
    /// The first pass reproduced nothing, so there is nothing to minimize.
    NoReproduction,
}

/// What one pass produced, and the size of what it actually reduced.
struct PassResult {
    minimized: Minimized,
    /// Length of the state script and statement this pass started from, on
    /// whichever basis it chose, so the driver can compare like with like.
    input_len: usize,
    /// Which of the two scripts this pass reduced. The driver cannot infer it
    /// from `input_len`: a history that happens to be the same length as the
    /// state script it replaced reads as no switch at all, and the pass loop
    /// would then log a fixpoint no pass reached.
    basis: Basis,
}

/// Which script a pass reduced.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Basis {
    /// The state script the driver handed the pass.
    Handed,
    /// The statement history, because the handed script stopped reproducing.
    History,
}

fn artifact_len(minimized: &Minimized) -> usize {
    minimized.state_sql.len() + minimized.statement.len()
}

/// The script to shrink against and the divergence it reproduces: the state dump when
/// that replays the failure, otherwise the statement history. Returning them together
/// is what keeps a pass from measuring its progress against a script it did not use.
fn choose_basis<'a>(
    state_sql: &'a str,
    history_sql: &'a str,
    failing_sql: &str,
) -> Result<Option<(&'a str, Divergence, Basis)>> {
    let pair = EnginePair::build(state_sql)?;
    if let Some(divergence) = pair.classify(failing_sql) {
        return Ok(Some((state_sql, divergence, Basis::Handed)));
    }
    let pair = EnginePair::build(history_sql)?;
    let Some(divergence) = pair.classify(failing_sql) else {
        return Ok(None);
    };
    tracing::info!(
        "Divergence needs the statement history; shrinking against it instead of the state dump"
    );
    Ok(Some((history_sql, divergence, Basis::History)))
}

fn shrink_one_pass(
    state_sql: &str,
    history_sql: &str,
    failing_sql: &str,
    deadline: &Deadline,
) -> Result<Option<PassResult>> {
    let Some((basis, original, which)) = choose_basis(state_sql, history_sql, failing_sql)? else {
        tracing::info!(
            "Shrink skipped: divergence reproduces on neither the rebuilt state nor the statement history"
        );
        return Ok(None);
    };
    Ok(Some(shrink_against(
        basis,
        which,
        &original,
        failing_sql,
        deadline,
    )?))
}

/// Reduce the statement, then the script it needs, against `basis` alone. Neither script
/// the caller chose between is in scope here, so the reported `input_len` cannot be taken
/// from the one this pass rejected.
fn shrink_against(
    basis: &str,
    which: Basis,
    original: &Divergence,
    failing_sql: &str,
    deadline: &Deadline,
) -> Result<PassResult> {
    let input_len = basis.len() + failing_sql.len();
    tracing::info!(
        "Shrinking {} byte statement ({original:?})",
        failing_sql.len()
    );

    let statement = shrink_with(failing_sql, deadline, |candidate| {
        // Fresh engines per attempt: a DML candidate that ran on both engines
        // would otherwise contaminate the next attempt's state.
        let pair = EnginePair::build(basis)?;
        Ok(matches_divergence(original, pair.classify(candidate)))
    })?;

    // With the statement fixed, drop every state line it does not need.
    let state_sql = reduce_state_lines(basis, deadline, |candidate_state| {
        let pair = EnginePair::build(candidate_state)?;
        Ok(matches_divergence(original, pair.classify(&statement)))
    })?;
    tracing::info!(
        "State script reduced to {} lines",
        state_sql.lines().count()
    );
    Ok(PassResult {
        minimized: Minimized {
            state_sql,
            statement,
        },
        input_len,
        basis: which,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Normalize SQL through the parser so tests compare renderings, not
    /// hand-written spacing.
    fn norm(sql: &str) -> String {
        parse_one(sql).expect("test SQL must parse").to_string()
    }

    /// A deadline no test can reach, so a test that expects work to happen
    /// still exercises the real `Deadline::after` rather than a stub.
    fn no_deadline() -> Deadline {
        Deadline::after(Duration::from_secs(3600))
    }

    /// A deadline that has already passed.
    fn out_of_time() -> Deadline {
        Deadline::after(Duration::ZERO)
    }

    fn minimized(state_sql: &str, statement: &str) -> Minimized {
        Minimized {
            state_sql: state_sql.to_string(),
            statement: statement.to_string(),
        }
    }

    #[test]
    fn function_calls_shrink_to_one_and_their_argument() {
        let results = candidates("SELECT ABS(x + 1) FROM t");
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT x + 1 FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn nested_case_shrinks_as_a_unit() {
        let sql = "SELECT CASE WHEN CASE WHEN a THEN b END THEN c END FROM t";
        let results = candidates(sql);
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT CASE WHEN 1 THEN c END FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn literals_inside_strings_do_not_confuse_edits() {
        // The string contains parens and would break a text-based scanner.
        let results = candidates("SELECT f('((', x) FROM t");
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT f('', x) FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn and_operands_can_replace_the_conjunction() {
        let results = candidates("SELECT a FROM t WHERE x AND f(y) ORDER BY z");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY z")),
            "{results:?}"
        );
        let results = candidates("SELECT a FROM t WHERE x OR y OR z");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x OR z")),
            "{results:?}"
        );
    }

    #[test]
    fn clauses_and_list_items_can_be_dropped() {
        let results = candidates("SELECT a FROM t WHERE x ORDER BY a DESC, b ASC LIMIT 3");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY a DESC LIMIT 3")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT a FROM t ORDER BY a DESC, b ASC LIMIT 3")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY a DESC, b ASC")),
            "{results:?}"
        );
        let results = candidates("SELECT MAX(a, b) FROM t");
        assert!(
            results.contains(&norm("SELECT MAX(a) FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn function_calls_also_shrink_to_empty_string() {
        let results = candidates("SELECT c <= RTRIM('ab') FROM t");
        assert!(
            results.contains(&norm("SELECT c <= '' FROM t")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT c <= 'ab' FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn shrink_loop_keeps_only_the_load_bearing_kernel() {
        // Synthetic judge: the "divergence" needs ABS( to survive. Everything
        // else — the CASE block, the other function call, the literals — must
        // be simplified away.
        let sql = "SELECT ABS(CASE WHEN LENGTH('hello world') THEN 123456 \
                   ELSE UPPER('junk') END), COALESCE(999999, X'DEADBEEF') FROM t";
        let out = shrink_with(sql, &no_deadline(), |cand| Ok(cand.contains("ABS"))).unwrap();
        assert!(out.contains("ABS"), "{out}");
        assert!(!out.contains("CASE"), "{out}");
        assert!(!out.contains("hello world"), "{out}");
        assert!(!out.contains("123456"), "{out}");
        assert!(!out.contains("999999"), "{out}");
    }

    #[test]
    fn shrink_loop_terminates_when_everything_is_accepted() {
        let sql = "SELECT MAX(1, MIN(2, 3)), 'literal', X'AB' FROM t";
        let out = shrink_with(sql, &no_deadline(), |_| Ok(true)).unwrap();
        assert!(out.len() < sql.len(), "{out}");
    }

    #[test]
    fn shrink_statement_returns_none_without_divergence() {
        let _turn = pair_test_turn();
        // Same statement behaves identically on both engines, so there is
        // nothing to shrink against.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n";
        let out = shrink_statement(state, state, "SELECT x + 1 FROM t").unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn state_reduction_keeps_only_needed_lines() {
        // Synthetic judge: the reproduction needs the CREATE and one INSERT;
        // every other line must be deleted.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n\
                     CREATE TABLE junk(y);\nINSERT INTO junk VALUES (2);\n\
                     CREATE INDEX i ON junk(y);";
        let out = reduce_state_lines(state, &no_deadline(), |cand| {
            Ok(cand.contains("CREATE TABLE t(x);") && cand.contains("INSERT INTO t VALUES (1);"))
        })
        .unwrap();
        assert_eq!(
            out, "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);",
            "{out}"
        );
    }

    /// Build a pass whose result is `state`/`statement`, reporting that it
    /// started from `input_len` bytes of `basis`.
    fn pass_result(
        state_sql: &str,
        statement: &str,
        input_len: usize,
        basis: Basis,
    ) -> PassResult {
        PassResult {
            minimized: minimized(state_sql, statement),
            input_len,
            basis,
        }
    }

    /// A pass that shrank exactly what it was handed, on the same basis.
    fn shrank(state_in: &str, stmt_in: &str, statement: &str) -> PassResult {
        pass_result(
            state_in,
            statement,
            state_in.len() + stmt_in.len(),
            Basis::Handed,
        )
    }

    #[test]
    fn each_pass_re_enters_the_shrinker_with_the_previous_pass_result() {
        // Halve the statement each pass; the statement stops changing at one
        // byte, which is the fixpoint.
        let mut inputs: Vec<String> = Vec::new();
        let (out, stop) = shrink_passes("S", "aaaaaaaa", &no_deadline(), |state_in, stmt_in| {
            inputs.push(stmt_in.to_string());
            let half = &stmt_in[..stmt_in.len().div_ceil(2)];
            Ok(Some(shrank(state_in, stmt_in, half)))
        })
        .unwrap();
        let out = out.expect("a reproducing pass must yield a result");
        assert_eq!(stop, Stop::Fixpoint);
        assert_eq!(
            inputs,
            ["aaaaaaaa", "aaaa", "aa", "a"],
            "each pass must start from the previous pass's output, not the original"
        );
        assert_eq!(out.statement, "a");
    }

    #[test]
    fn a_pass_that_switches_to_the_history_is_measured_against_the_history() {
        // The production shape: the state dump does not reproduce, so pass 1
        // shrinks the much larger statement history instead. Its output is
        // bigger than the dump it was handed, yet it is real progress and
        // there is more to give.
        let dump = "CREATE TABLE t(x);";
        let history = "h".repeat(20_000);
        let mut passes = 0;
        let (out, stop) = shrink_passes(dump, "SELECT 1 FROM t", &no_deadline(), |_, stmt_in| {
            passes += 1;
            Ok(Some(match passes {
                // Pass 1 falls back to the history; passes 2 and 3 reduce the history
                // they are then handed, which is no longer a switch.
                1 => pass_result(
                    &"h".repeat(2000),
                    stmt_in,
                    history.len() + stmt_in.len(),
                    Basis::History,
                ),
                2 => pass_result(
                    &"h".repeat(800),
                    stmt_in,
                    2000 + stmt_in.len(),
                    Basis::Handed,
                ),
                _ => pass_result(
                    &"h".repeat(800),
                    stmt_in,
                    800 + stmt_in.len(),
                    Basis::Handed,
                ),
            }))
        })
        .unwrap();
        let out = out.expect("a reproducing pass must yield a result");
        assert_eq!(stop, Stop::Fixpoint);
        assert_eq!(
            passes, 3,
            "pass 1's output is larger than the state dump it was handed, which is not a fixpoint"
        );
        assert_eq!(out.state_sql.len(), 800);
    }

    #[test]
    fn a_pass_that_comes_back_larger_does_not_replace_the_smaller_result() {
        let mut passes = 0;
        let (out, stop) = shrink_passes(
            "state-dump",
            "SELECT 1",
            &no_deadline(),
            |state_in, stmt_in| {
                passes += 1;
                Ok(Some(match passes {
                    1 => shrank(state_in, stmt_in, "S"),
                    // Fell back to the history: still a reproduction, but a bigger one.
                    _ => pass_result(&"h".repeat(5000), "SELECT 1", 20_000, Basis::History),
                }))
            },
        )
        .unwrap();
        let out = out.expect("a reproducing pass must yield a result");
        assert_eq!(
            (passes, stop),
            (2, Stop::SwitchedBasisAndDidNotShrink),
            "pass 1 was still shrinking and pass 2 measured a different script, so \
             nothing here is a fixpoint"
        );
        assert_eq!(
            (out.state_sql.as_str(), out.statement.as_str()),
            ("state-dump", "S"),
            "the smaller reproduction must survive a later, larger pass"
        );
    }

    #[test]
    fn a_history_pass_that_lands_on_the_same_size_is_not_a_fixpoint() {
        // The driver cannot read "which script did you reduce?" out of a length. This
        // pass falls back to the history and its result happens to weigh exactly what
        // the driver already holds, so a length comparison sees no switch and no
        // shrink -- and calls that a fixpoint no pass reached.
        let mut passes = 0;
        let (out, stop) = shrink_passes(
            "state-dump",
            "SELECT 1",
            &no_deadline(),
            |state_in, stmt_in| {
                passes += 1;
                Ok(Some(match passes {
                    1 => shrank(state_in, stmt_in, "S"),
                    _ => pass_result("state-dum", "SS", 11, Basis::History),
                }))
            },
        )
        .unwrap();
        let out = out.expect("a reproducing pass must yield a result");
        assert_eq!((passes, stop), (2, Stop::SwitchedBasisAndDidNotShrink));
        assert_eq!(
            (out.state_sql.as_str(), out.statement.as_str()),
            ("state-dump", "S"),
            "a tie on a different script must not replace the result already held"
        );
    }

    #[test]
    fn the_pass_bound_stops_a_shrink_that_is_still_making_progress() {
        let original = "a".repeat(100);
        let mut passes = 0;
        let (out, stop) = shrink_passes("S", &original, &no_deadline(), |state_in, stmt_in| {
            passes += 1;
            Ok(Some(shrank(state_in, stmt_in, &stmt_in[1..])))
        })
        .unwrap();
        let out = out.expect("a reproducing pass must yield a result");
        assert_eq!(
            (passes, stop),
            (MAX_SHRINK_PASSES, Stop::PassBound),
            "an always-shrinking input must stop at the pass bound, and say so"
        );
        assert_eq!(out.statement.len(), original.len() - MAX_SHRINK_PASSES);
    }

    #[test]
    fn a_later_pass_losing_the_divergence_keeps_the_previous_result() {
        let mut passes = 0;
        let (out, stop) = shrink_passes(
            "state-dump",
            "SELECT 1 FROM t",
            &no_deadline(),
            |state_in, stmt_in| {
                passes += 1;
                if passes == 1 {
                    return Ok(Some(shrank(state_in, stmt_in, "SELECT 1")));
                }
                Ok(None)
            },
        )
        .unwrap();
        assert_eq!((passes, stop), (2, Stop::LostDivergence));
        let out = out.expect("losing the divergence on pass 2 must not discard pass 1's result");
        assert_eq!(
            (out.state_sql.as_str(), out.statement.as_str()),
            ("state-dump", "SELECT 1")
        );
    }

    #[test]
    fn no_divergence_on_the_first_pass_yields_nothing() {
        let mut passes = 0;
        let (out, stop) = shrink_passes("state-dump", "SELECT 1", &no_deadline(), |_, _| {
            passes += 1;
            Ok(None)
        })
        .unwrap();
        assert_eq!((passes, stop), (1, Stop::NoReproduction));
        assert!(
            out.is_none(),
            "nothing reproduced, so there is nothing to write"
        );
    }

    #[test]
    fn an_expired_deadline_stops_the_loop_but_keeps_the_first_result() {
        let mut passes = 0;
        let (out, stop) = shrink_passes("S", "aaaaaaaa", &out_of_time(), |state_in, stmt_in| {
            passes += 1;
            Ok(Some(shrank(state_in, stmt_in, &stmt_in[1..])))
        })
        .unwrap();
        let out = out.expect("running out of time must still return the pass that did run");
        assert_eq!(
            (passes, stop),
            (1, Stop::OutOfTime),
            "a second pass must not start after the deadline"
        );
        assert_eq!(out.statement, "aaaaaaa");
    }

    #[test]
    fn an_expired_deadline_stops_the_candidate_loop() {
        let sql = "SELECT MAX(1, MIN(2, 3)), 'literal', X'AB' FROM t";
        let mut judged = 0;
        let out = shrink_with(sql, &out_of_time(), |_| {
            judged += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(judged, 0, "no candidate may be built after the deadline");
        assert_eq!(out, sql, "the unshrunk statement still reproduces");
    }

    #[test]
    fn an_expired_deadline_stops_the_state_reduction() {
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\nCREATE TABLE junk(y);";
        let mut judged = 0;
        let out = reduce_state_lines(state, &out_of_time(), |_| {
            judged += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(
            judged, 0,
            "no candidate state may be replayed after the deadline"
        );
        assert_eq!(out, state, "the unreduced state script still reproduces");
    }

    #[test]
    fn a_first_pass_that_reduces_nothing_still_returns_its_reproduction() {
        // The state dump replays the failure but no edit survives, so the pass hands
        // back exactly what it was given. That is a fixpoint, not a failure, and the
        // unreduced reproduction is the only one there is.
        let mut passes = 0;
        let (out, stop) = shrink_passes(
            "state-dump",
            "SELECT 1",
            &no_deadline(),
            |state_in, stmt_in| {
                passes += 1;
                Ok(Some(shrank(state_in, stmt_in, stmt_in)))
            },
        )
        .unwrap();
        let out =
            out.expect("a pass that reproduced must not be thrown away for failing to shrink");
        assert_eq!((passes, stop), (1, Stop::Fixpoint));
        assert_eq!(
            (out.state_sql.as_str(), out.statement.as_str()),
            ("state-dump", "SELECT 1")
        );
    }

    #[test]
    fn a_pass_the_deadline_cut_short_is_not_reported_as_a_fixpoint() {
        // A pass stopped by the clock returns its input unchanged, which is
        // indistinguishable from a fixpoint by size alone.
        let mut passes = 0;
        let (out, stop) = shrink_passes(
            "state-dump",
            "SELECT 1",
            &out_of_time(),
            |state_in, stmt_in| {
                passes += 1;
                Ok(Some(shrank(state_in, stmt_in, stmt_in)))
            },
        )
        .unwrap();
        assert_eq!(
            (passes, stop),
            (1, Stop::OutOfTime),
            "no reduction plus an expired deadline is a timeout, not a fixpoint"
        );
        assert!(out.is_some(), "the reproduction is still worth writing");
    }

    #[test]
    fn the_candidate_sweep_stops_partway_when_the_deadline_passes() {
        let sql = "SELECT MAX(1, MIN(2, 3)), 'literal', X'AB', LENGTH('pad') \
                   FROM t WHERE a AND b ORDER BY a LIMIT 3";
        let sweep = candidates(sql).len();
        assert!(sweep > 20, "the sweep must be long enough to cut: {sweep}");
        let mut judged = 0;
        let out = shrink_with(sql, &Deadline::after_judging(5), |_| {
            judged += 1;
            Ok(false)
        })
        .unwrap();
        assert_eq!(judged, 5, "the deadline must cut the sweep at the fifth judge");
        assert!(
            judged < sweep,
            "the deadline must cut the sweep short: judged {judged} of {sweep}"
        );
        assert_eq!(
            out, sql,
            "nothing was accepted, so the statement is unchanged"
        );
    }

    #[test]
    fn the_state_reduction_stops_partway_when_the_deadline_passes() {
        // Rejecting every deletion leaves ddmin far more than five candidates to try.
        let state: String = (0..40)
            .map(|i| format!("CREATE TABLE t{i}(x);\n"))
            .collect();
        let mut judged = 0;
        let out = reduce_state_lines(&state, &Deadline::after_judging(5), |_| {
            judged += 1;
            Ok(false)
        })
        .unwrap();
        assert_eq!(
            judged, 5,
            "the deadline must cut the reduction at the fifth judge"
        );
        assert_eq!(
            out.lines().count(),
            40,
            "nothing was deleted, so every line survives"
        );
    }

    /// A `WITH RECURSIVE` whose arm stops at `n < 3`, for splicing into a
    /// larger statement.
    const RECURSIVE: &str =
        "WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT n FROM r";

    #[test]
    fn a_recursive_cte_is_never_edited() {
        // Dropping the WHERE from the recursive arm, or replacing it with 1, leaves a
        // query that never returns -- and it runs inside one classify call, where the
        // deadline cannot reach it.
        for sql in [
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 3) \
             SELECT x FROM seq",
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 3) \
             DELETE FROM t WHERE a IN (SELECT x FROM seq)",
        ] {
            assert!(candidates(sql).is_empty(), "{sql} -> {:?}", candidates(sql));
        }
        // A plain CTE carries no such risk and still shrinks.
        let plain = "WITH c(x) AS (SELECT 1) SELECT x FROM c WHERE x AND x";
        assert!(!candidates(plain).is_empty());
    }

    #[test]
    fn a_recursive_cte_inside_an_expression_subquery_is_never_edited() {
        // The guard used to run over walk_sites, which never visits a select that only
        // an expression reaches. Every statement here parses, the guard said no
        // recursive CTE, and the edits then replaced `n < 3` with `1`.
        for sql in [
            format!("SELECT 1 FROM t WHERE EXISTS ({RECURSIVE})"),
            format!("SELECT ({RECURSIVE}) FROM t"),
            format!("SELECT 1 FROM t WHERE a IN ({RECURSIVE})"),
            format!("SELECT 1 FROM t WHERE NOT EXISTS (SELECT 1 FROM ({RECURSIVE}))"),
            format!("SELECT 1 FROM t UNION ALL SELECT 1 FROM t WHERE EXISTS ({RECURSIVE})"),
            format!("SELECT 1 FROM t UNION ALL SELECT 1 FROM ({RECURSIVE})"),
            format!("INSERT INTO t SELECT 1 FROM u WHERE EXISTS ({RECURSIVE})"),
            format!("UPDATE t SET x = ({RECURSIVE})"),
            format!("DELETE FROM t WHERE EXISTS ({RECURSIVE})"),
            format!("SELECT 1 FROM t ORDER BY ({RECURSIVE})"),
            format!("SELECT 1 FROM t LIMIT ({RECURSIVE})"),
            format!("INSERT INTO t VALUES (({RECURSIVE}))"),
            format!("UPDATE t SET x = 1 WHERE EXISTS ({RECURSIVE})"),
            format!("SELECT 1 FROM (SELECT 1 FROM t WHERE EXISTS ({RECURSIVE}))"),
            format!("SELECT 1 FROM t JOIN ({RECURSIVE}) ON 1"),
            format!("SELECT 1 FROM t WHERE a IN (SELECT 1 FROM u WHERE b IN ({RECURSIVE}))"),
            format!("SELECT 1 FROM t GROUP BY a HAVING EXISTS ({RECURSIVE})"),
            format!("DELETE FROM t WHERE a IN (SELECT n FROM ({RECURSIVE}))"),
        ] {
            let stmt = parse_one(&sql).unwrap_or_else(|| panic!("test SQL must parse: {sql}"));
            assert!(has_recursive_cte(&stmt), "guard missed: {sql}");
            // Render through the parser first: a statement handed to the shrinker comes
            // from the generator already normalized, and `push_candidate` keeps only
            // renderings shorter than their input, so hand-written spacing hides edits.
            let normalized = norm(&sql);
            assert!(
                candidates(&normalized).is_empty(),
                "{normalized} -> {:?}",
                candidates(&normalized)
            );
        }
    }

    #[test]
    fn the_exact_statement_the_guard_used_to_miss_produces_no_endless_candidate() {
        let sql = norm(&format!("SELECT 1 FROM t WHERE EXISTS ({RECURSIVE})"));
        // Before the guard walked the same traversal as the edits, this sweep offered
        // `WHERE n`, `WHERE 3`, `WHERE 1` and `WHERE ''` in place of `WHERE n < 3`.
        // Each keeps the recursive arm and removes what stops it, and the candidate
        // then spins inside a single classify call that the deadline never re-enters.
        for endless in ["WHERE n)", "WHERE 3)", "WHERE 1)", "WHERE '')"] {
            assert!(
                !candidates(&sql).iter().any(|c| c.contains(endless)),
                "candidate keeping the recursive arm without its guard: {endless}"
            );
        }
        assert!(candidates(&sql).is_empty(), "{:?}", candidates(&sql));
    }

    #[test]
    fn a_plain_cte_inside_an_expression_subquery_still_shrinks() {
        // The guard must not swallow every subquery, only the recursive ones.
        let sql = norm("SELECT 1 FROM t WHERE EXISTS (WITH c(x) AS (SELECT 1) \
                        SELECT x FROM c WHERE x AND x)");
        assert!(!candidates(&sql).is_empty());
    }

    /// The database-name pool is process-wide and `cargo test` runs this module
    /// in parallel, so the tests that count names take turns.
    static ONE_PAIR_TEST_AT_A_TIME: Mutex<()> = Mutex::new(());

    fn pair_test_turn() -> std::sync::MutexGuard<'static, ()> {
        ONE_PAIR_TEST_AT_A_TIME
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn two_live_engine_pairs_do_not_contaminate_each_other() {
        let _turn = pair_test_turn();
        // Both pairs replay the same state script. While they shared a database name,
        // each Turso side saw that script applied twice and reported a mismatch against
        // its own SQLite side, for a statement the engines agree on.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n";
        let sql = "SELECT x + 1 FROM t";
        let first = EnginePair::build(state).unwrap();
        let second = EnginePair::build(state).unwrap();
        assert_eq!(
            second.classify(sql),
            None,
            "the second pair must not see the first pair's state"
        );
        assert_eq!(
            first.classify(sql),
            None,
            "nor the first pair the second pair's"
        );
    }

    /// Wall clock for one shrink pass, which is where the numbers quoted on
    /// `SHRINK_TIME_BUDGET` come from.
    ///
    /// Measures the most expensive shape a pass has: no candidate is accepted,
    /// so the statement sweep runs to the end of its candidate list and ddmin
    /// runs down to single lines, and every one of those attempts rebuilds
    /// both engines and replays the whole state script. Ignored by default --
    /// it takes minutes. Re-measure with
    /// `cargo test -p differential-fuzzer --lib measure_one_shrink_pass -- \
    /// --ignored --nocapture`.
    #[test]
    #[ignore = "minutes of wall clock; run by hand to re-measure SHRINK_TIME_BUDGET"]
    fn measure_one_shrink_pass() {
        let _turn = pair_test_turn();
        let statement = norm(
            "SELECT a, MAX(b, c) FROM t WHERE a AND b OR LENGTH(c) < 5 \
             GROUP BY a HAVING COUNT(*) > 1 ORDER BY a DESC, b LIMIT 3",
        );
        for lines in [200usize, 1000] {
            let state = state_script(lines);
            const BUILDS: u32 = 10;
            let build_start = Instant::now();
            for _ in 0..BUILDS {
                drop(EnginePair::build(&state).unwrap());
            }
            let per_build = build_start.elapsed() / BUILDS;
            println!("{lines}-line state script: EnginePair::build {per_build:?}");

            let pass_start = Instant::now();
            let out = shrink_against(
                &state,
                Basis::Handed,
                &Divergence::ResultMismatch,
                &statement,
                &no_deadline(),
            )
            .unwrap();
            println!(
                "{lines}-line state script: one pass {:?} ({} statement candidates, \
                 state reduced to {} of {lines} lines)",
                pass_start.elapsed(),
                candidates(&statement).len(),
                out.minimized.state_sql.lines().count(),
            );
        }
    }

    fn state_script(lines: usize) -> String {
        let mut script = String::from("CREATE TABLE t(a, b, c);\n");
        for i in 1..lines {
            script.push_str(&format!(
                "INSERT INTO t VALUES ({i}, {}, 'row{i}');\n",
                i * 3 % 17
            ));
        }
        script
    }

    #[test]
    fn building_pairs_one_at_a_time_mints_one_database_name() {
        let _turn = pair_test_turn();
        // Every distinct name is a key `turso_core`'s registry inserts and never
        // removes, and the `Weak` it stores there keeps the whole `Database`
        // allocation alive. A name per pair meant a stranded `Database` per shrink
        // candidate, thousands per shrink.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n";
        let before = database_names_minted();
        for _ in 0..64 {
            drop(EnginePair::build(state).unwrap());
        }
        assert_eq!(
            database_names_minted(),
            before.max(1),
            "64 pairs built one at a time must reuse a single name"
        );
    }

    #[test]
    fn a_reused_database_name_starts_from_an_empty_database() {
        let _turn = pair_test_turn();
        // Reusing a name reopens a path `turso_core`'s registry has already seen. If
        // that reopen shared anything with the pair that held the name last, the state
        // script would land on top of the previous replay -- exactly the contamination
        // a name per pair existed to prevent, arriving through the back door. `classify`
        // compares every table's contents on both engines, so a Turso side carrying two
        // rows where SQLite carries one shows up as a divergence here.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n";
        let mut minted = None;
        for round in 0..8 {
            let pair = EnginePair::build(state).unwrap();
            let now = database_names_minted();
            assert_eq!(
                *minted.get_or_insert(now),
                now,
                "round {round} must reuse the name, or this test proves nothing"
            );
            assert_eq!(
                pair.classify("SELECT count(*) FROM t"),
                None,
                "round {round} saw the previous pair's rows"
            );
        }
    }

    #[test]
    fn a_database_name_is_reused_only_once_its_pair_is_gone() {
        let _turn = pair_test_turn();
        let state = "CREATE TABLE t(x);\n";
        assert_eq!(
            database_names_in_use(),
            0,
            "the turn lock must leave no other pair alive"
        );
        let first = EnginePair::build(state).unwrap();
        let second = EnginePair::build(state).unwrap();
        assert_eq!(
            database_names_in_use(),
            2,
            "a second live pair must not take the first pair's name"
        );
        let minted = database_names_minted();
        drop(first);
        drop(second);
        assert_eq!(
            database_names_in_use(),
            0,
            "dropping a pair must free its name"
        );
        drop(EnginePair::build(state).unwrap());
        assert_eq!(
            database_names_minted(),
            minted,
            "a name whose pair is gone must be handed out again"
        );
    }
}
