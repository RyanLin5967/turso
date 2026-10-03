//! Recipe backfill: a bulk UPDATE stored as a recipe instead of a table rewrite (lane
//! k1-recipe-build; artie-research `frontier/round14/k1-recipe-build/PREREG.md` §2).
//!
//! # What it is
//!
//! On a page copy-on-write branch an `UPDATE t SET c = f(row)` over every row copies every leaf of
//! `t` into the branch: BranchBench's Software Dev step (ALTER ADD COLUMN + backfill) costs the
//! whole table per branch. An eligible bulk UPDATE here instead becomes recipe `g`: one
//! `sqlite_schema` row of type `recipe` holding the statement's SET/WHERE text. No table page is
//! written. The first recipe on a table also appends one hidden column, [`GEN_COLUMN`].
//!
//! # The invariant
//!
//! A record's *physical generation* is its stored [`GEN_COLUMN`] value, or 0 when the record is
//! short or the field is NULL. Every record written stores the table's current generation `G`
//! (INSERT through the column default, which is `G`; UPDATE because the column's logical value is
//! always `G`). So a record of physical generation `p` has not been written since recipe `p`, and
//! its stored fields are exactly the inputs the eager UPDATEs `p+1..=G` would have seen. Its logical
//! row is recipes `p+1..=G` applied in order to its stored row.
//!
//! # Where it is enforced
//!
//! At the one choke point every column read of a table b-tree passes through: `op_column` (and
//! `op_column_range`) on a [`CursorType::BTreeTable`] cursor opened on the table's own b-tree
//! ([`column_fetch`]). A column whose recipe closure (the recipes that target it, or a column
//! those recipes read, transitively) is empty reads its stored value unchanged. Otherwise a stale
//! record's value is computed by running the closure's recipes, each compiled by `translate_expr`
//! through the generated-column self-table context so column references keep their affinity, and
//! the target column's affinity applied as UPDATE applies it on store. Nothing is written by a
//! read. A write (UPDATE) reads every unchanged column through the same choke point, so a stale
//! row is written with its logical values and generation `G`: the freeze happens inside the page
//! write the row was getting anyway.
//!
//! Refused rather than approximated: see [`translate_update_as_recipe`]'s eligibility rules and
//! the guards in `alter.rs`, `vacuum.rs` and `op_row_data`.

use crate::schema::{
    resolve_gencol_expr_columns, validate_generated_expr, BTreeTable, Column, Schema, Table,
    Type,
};
use crate::storage::pager::Pager;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr, WalkControl};
use crate::types::IOResult;
use crate::util::normalize_ident;
use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::execute::InsnFunctionStepResult;
use crate::vdbe::insn::{to_u32, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{Program, ProgramState, Register};
use crate::{Connection, LimboError, QueryMode, Result, Value, MAIN_DB_ID};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, Expr};

/// The hidden per-row generation column.
pub const GEN_COLUMN: &str = "__turso_gen";
/// `sqlite_schema.type` of a recipe row.
pub const SCHEMA_TYPE: &str = "recipe";
/// `sqlite_schema.name` of a recipe row is this prefix, the table name, `_`, the generation.
pub const NAME_PREFIX: &str = "__turso_recipe_";

/// Integer counters, process-wide, observing only (PREREG §7).
pub static RECIPE_IO: [AtomicU64; 10] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Indexes into [`RECIPE_IO`].
pub mod counter {
    /// `Pager::read_page` calls: page accesses by the b-tree, cache hits plus misses.
    pub const PAGE_FETCH: usize = 0;
    /// Recipe sub-program runs.
    pub const RECIPE_EVALS: usize = 1;
    /// Column reads of a stale record served by the recipe path.
    pub const STALE_READS: usize = 2;
    /// Recipes installed (UPDATEs that became a recipe).
    pub const INSTALLED: usize = 3;
    /// UPDATEs on a recipe-enabled connection that ran eagerly because a rule refused them.
    pub const FALLBACKS: usize = 4;
    /// Dirty pages committed by branch transactions.
    pub const BRANCH_DIRTY: usize = 5;
    /// Dirty pages committed by trunk (WAL) transactions.
    pub const TRUNK_DIRTY: usize = 6;
    /// Recipe sub-programs compiled.
    pub const COMPILES: usize = 7;
    /// Stale column reads answered from the per-row cache.
    pub const CACHE_HITS: usize = 8;
    /// Eligible UPDATEs whose read-only pass matched no row (nothing installed).
    pub const EMPTY_MATCH: usize = 9;
}

/// A snapshot of [`RECIPE_IO`].
pub fn recipe_io() -> [u64; 10] {
    let mut out = [0u64; 10];
    for (o, c) in out.iter_mut().zip(RECIPE_IO.iter()) {
        *o = c.load(Ordering::Relaxed);
    }
    out
}

pub(crate) fn count(which: usize, n: u64) {
    RECIPE_IO[which].fetch_add(n, Ordering::Relaxed);
}

/// Planted mutants for the KC1 fire-check (PREREG §8), one compile, chosen at run time by
/// `K1_RECIPE_MUTANT=M1..M8`. Unset means the mechanism as designed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutant {
    None,
    /// A write keeps the stale physical generation (the gen column reads its stored value).
    M1,
    /// INSERT writes generation 0.
    M2,
    /// No target affinity on a recipe's output.
    M3,
    /// Recipes applied in reverse generation order.
    M4,
    /// Generation ignored: every closure recipe applied to every row.
    M5,
    /// WHERE ignored.
    M6,
    /// Per-row cache keyed without the row.
    M7,
    /// A write reads unchanged columns physically: no freeze (compute-on-read "screen").
    M8,
}

fn debug() -> bool {
    static D: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *D.get_or_init(|| std::env::var_os("K1_RECIPE_DEBUG").is_some())
}

pub fn mutant() -> Mutant {
    static M: std::sync::OnceLock<Mutant> = std::sync::OnceLock::new();
    *M.get_or_init(|| match std::env::var("K1_RECIPE_MUTANT").as_deref() {
        Ok("M1") => Mutant::M1,
        Ok("M2") => Mutant::M2,
        Ok("M3") => Mutant::M3,
        Ok("M4") => Mutant::M4,
        Ok("M5") => Mutant::M5,
        Ok("M6") => Mutant::M6,
        Ok("M7") => Mutant::M7,
        Ok("M8") => Mutant::M8,
        _ => Mutant::None,
    })
}

/// One installed recipe: `UPDATE t SET targets = exprs [WHERE pred]`, applied when read.
#[derive(Debug)]
pub struct Recipe {
    pub generation: u64,
    /// The persisted statement text, re-parsed at every schema load.
    pub sql: String,
    pub targets: Vec<usize>,
    /// SET expressions, column references resolved to `Expr::Column { SELF_TABLE }`.
    pub exprs: Vec<Expr>,
    /// WHERE, resolved the same way.
    pub pred: Option<Expr>,
    /// Columns the recipe reads: those its expressions and WHERE reference, plus its targets
    /// (an unmatched row keeps the old target value). Sorted, unique.
    pub inputs: Vec<usize>,
}

/// The recipes of one table, in generation order, with each column's closure precomputed.
#[derive(Debug, Clone)]
pub struct TableRecipes {
    pub recipes: Vec<Arc<Recipe>>,
    /// Index of [`GEN_COLUMN`] in the table.
    pub gen_col: usize,
    /// Per column: indexes into `recipes`, ascending, of the recipes its value depends on.
    closure: Vec<Vec<usize>>,
    /// Per column: the columns those recipes read (what a stale read must decode), sorted.
    deps: Vec<Vec<usize>>,
    /// Per column: the newest generation in its closure (0 when the closure is empty).
    newest: Vec<u64>,
    /// Per column: the value a short record reads (the constant DEFAULT with affinity), exactly
    /// what the `Column` instruction applies.
    defaults: Vec<Option<Value>>,
    /// Per column: whether it is the rowid alias (its record field is NULL; the value is the rowid).
    rowid_alias: Vec<bool>,
}

impl TableRecipes {
    /// The table's current generation `G`.
    pub fn generation(&self) -> u64 {
        self.recipes.last().map_or(0, |r| r.generation)
    }

    pub fn closure_of(&self, column: usize) -> &[usize] {
        self.closure.get(column).map_or(&[], |c| c.as_slice())
    }

    fn newest_of(&self, column: usize) -> u64 {
        self.newest.get(column).copied().unwrap_or(0)
    }

    fn rebuild(&mut self, columns: &[Column]) {
        let n = columns.len();
        self.closure = vec![Vec::new(); n];
        self.deps = vec![Vec::new(); n];
        self.newest = vec![0; n];
        for c in 0..n {
            // Newest to oldest: a recipe joins the closure when it targets a column already in
            // the dependency set, and then adds what it reads. A newer recipe on an input does
            // not affect an older recipe's view of it, which this order respects.
            let mut in_set = vec![false; n];
            in_set[c] = true;
            let mut members = Vec::new();
            for (ri, r) in self.recipes.iter().enumerate().rev() {
                if r.targets.iter().any(|&t| t < n && in_set[t]) {
                    members.push(ri);
                    for &i in &r.inputs {
                        if i < n {
                            in_set[i] = true;
                        }
                    }
                }
            }
            members.reverse();
            if !members.is_empty() {
                self.newest[c] = self.recipes[*members.last().unwrap()].generation;
                self.deps[c] = (0..n).filter(|&i| in_set[i]).collect();
            }
            self.closure[c] = members;
        }
        self.defaults = (0..n).map(|i| column_read_default(columns, i)).collect();
        self.rowid_alias = columns.iter().map(|c| c.is_rowid_alias()).collect();
    }
}

/// What a short record reads for column `i`: the same value the `Column` instruction's default
/// carries (see `ProgramBuilder::emit_column`).
fn column_read_default(columns: &[Column], i: usize) -> Option<Value> {
    crate::vdbe::builder::btree_column_read_default(&columns[i])
}

/// The hidden generation column appended by a table's first recipe.
pub fn gen_column() -> Result<Column> {
    let sql = format!("CREATE TABLE x (\"{GEN_COLUMN}\" INTEGER HIDDEN)");
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let Some(ast::Cmd::Stmt(ast::Stmt::CreateTable { body, .. })) = parser.next_cmd()? else {
        return Err(LimboError::InternalError("recipe: gen column did not parse".into()));
    };
    let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Err(LimboError::InternalError("recipe: gen column did not parse".into()));
    };
    let col = Column::try_from(&columns[0])?;
    if !col.hidden() {
        return Err(LimboError::InternalError("recipe: gen column is not hidden".into()));
    }
    Ok(col)
}

pub fn is_gen_column(column: &Column) -> bool {
    column.hidden()
        && column
            .name
            .as_deref()
            .is_some_and(|n| n.eq_ignore_ascii_case(GEN_COLUMN))
}

/// Debug aid (observing only): the number of recipes the connection's schema holds for `table`,
/// and whether the database's shared schema agrees.
#[doc(hidden)]
pub fn debug_recipe_count(conn: &Arc<Connection>, table: &str) -> (Option<usize>, Option<usize>) {
    let local = conn
        .schema
        .read()
        .get_btree_table(table)
        .and_then(|t| t.recipes.as_ref().map(|r| r.recipes.len()));
    let shared = conn
        .db
        .clone_schema()
        .get_btree_table(table)
        .and_then(|t| t.recipes.as_ref().map(|r| r.recipes.len()));
    (local, shared)
}

pub fn recipe_name(table: &str, generation: u64) -> String {
    format!("{NAME_PREFIX}{}_{generation}", normalize_ident(table))
}

pub fn parse_recipe_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(NAME_PREFIX)?;
    let (_, gen) = rest.rsplit_once('_')?;
    gen.parse().ok()
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The persisted text of a recipe: `UPDATE "t" SET "c" = e, ... [WHERE p]`, from the statement's
/// own (unresolved) expressions.
fn recipe_sql(table: &str, sets: &[(String, &Expr)], pred: Option<&Expr>) -> String {
    let mut sql = format!("UPDATE {} SET ", quote_ident(table));
    for (i, (name, expr)) in sets.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("{} = {}", quote_ident(name), expr));
    }
    if let Some(pred) = pred {
        sql.push_str(&format!(" WHERE {pred}"));
    }
    sql
}

fn referenced_columns(expr: &Expr, out: &mut Vec<usize>) -> Result<()> {
    walk_expr(expr, &mut |e| {
        if let Expr::Column { table, column, .. } = e {
            if table.is_self_table() {
                out.push(*column);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Resolve one SET/WHERE expression for a recipe: deterministic, row-local, every name a column.
fn resolve_recipe_expr(expr: &Expr, columns: &[Column]) -> Result<Expr> {
    validate_generated_expr(expr)?;
    let mut e = expr.clone();
    resolve_gencol_expr_columns(&mut e, columns)?;
    Ok(e)
}

/// Parse a persisted recipe and resolve it against the table's current columns. Install and
/// schema load both build the in-memory recipe through here, so the two cannot disagree.
pub fn parse_recipe(btree: &BTreeTable, generation: u64, sql: &str) -> Result<Recipe> {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = parser.next_cmd()? else {
        return Err(LimboError::Corrupt(format!("recipe row is not an UPDATE: {sql}")));
    };
    let columns = btree.columns();
    let mut targets = Vec::with_capacity(update.sets.len());
    let mut exprs = Vec::with_capacity(update.sets.len());
    let mut inputs = Vec::new();
    for set in &update.sets {
        let [name] = set.col_names.as_slice() else {
            return Err(LimboError::Corrupt(format!("recipe row has a row-value SET: {sql}")));
        };
        let target = btree
            .get_column(&normalize_ident(name.as_str()))
            .map(|(i, _)| i)
            .ok_or_else(|| LimboError::Corrupt(format!("recipe target missing: {sql}")))?;
        let e = resolve_recipe_expr(&set.expr, columns)?;
        referenced_columns(&e, &mut inputs)?;
        targets.push(target);
        inputs.push(target);
        exprs.push(e);
    }
    let pred = match update.where_clause.as_deref() {
        Some(w) => {
            let e = resolve_recipe_expr(w, columns)?;
            referenced_columns(&e, &mut inputs)?;
            Some(e)
        }
        None => None,
    };
    inputs.sort_unstable();
    inputs.dedup();
    Ok(Recipe {
        generation,
        sql: sql.to_string(),
        targets,
        exprs,
        pred,
        inputs,
    })
}

/// Attach a recipe (schema load and install share this). Generations must ascend, and the table
/// must already hold [`GEN_COLUMN`]. The gen column's in-memory DEFAULT becomes the new `G`, which
/// is what INSERT writes.
pub fn attach_recipe(btree: &mut BTreeTable, recipe: Recipe) -> Result<()> {
    let gen_col = btree
        .columns()
        .iter()
        .position(is_gen_column)
        .ok_or_else(|| {
            LimboError::Corrupt(format!(
                "table {} has a recipe but no {GEN_COLUMN} column",
                btree.name
            ))
        })?;
    let mut rs = btree.recipes.as_deref().cloned().unwrap_or(TableRecipes {
        recipes: Vec::new(),
        gen_col,
        closure: Vec::new(),
        deps: Vec::new(),
        newest: Vec::new(),
        defaults: Vec::new(),
        rowid_alias: Vec::new(),
    });
    if recipe.generation <= rs.generation() {
        return Err(LimboError::Corrupt(format!(
            "table {}: recipe generation {} after {}",
            btree.name,
            recipe.generation,
            rs.generation()
        )));
    }
    let g = recipe.generation;
    rs.gen_col = gen_col;
    rs.recipes.push(Arc::new(recipe));
    {
        let mut cols = btree.columns_mut();
        cols[gen_col].default = Some(Box::new(Expr::Literal(ast::Literal::Numeric(
            g.to_string(),
        ))));
    }
    rs.rebuild(btree.columns());
    btree.recipes = Some(Arc::new(rs));
    Ok(())
}

/// A schema row of type `recipe`: attach it to its table.
pub(crate) fn handle_schema_row(
    schema: &mut Schema,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> Result<()> {
    let sql = sql.ok_or_else(|| LimboError::Corrupt(format!("recipe row {name} has no sql")))?;
    let generation = parse_recipe_name(name)
        .ok_or_else(|| LimboError::Corrupt(format!("recipe row name {name}")))?;
    let key = normalize_ident(table_name);
    let table_ref = schema
        .tables
        .get_mut(&key)
        .ok_or_else(|| LimboError::Corrupt(format!("recipe row {name}: no table {table_name}")))?;
    let table_ref = Arc::make_mut(table_ref);
    let Table::BTree(btree) = table_ref else {
        return Err(LimboError::Corrupt(format!("recipe row {name}: not a b-tree table")));
    };
    let btree = Arc::make_mut(btree);
    let recipe = parse_recipe(btree, generation, sql)?;
    attach_recipe(btree, recipe)
}

/// The in-memory half of an install (`Insn::RecipeInstall`).
pub(crate) fn install_in_schema(
    schema: &mut Schema,
    table_name: &str,
    generation: u64,
    sql: &str,
    add_gen_col: bool,
) -> Result<()> {
    let key = normalize_ident(table_name);
    let table_ref = schema
        .tables
        .get_mut(&key)
        .ok_or_else(|| LimboError::InternalError(format!("recipe install: no table {key}")))?;
    let table_ref = Arc::make_mut(table_ref);
    let Table::BTree(btree) = table_ref else {
        return Err(LimboError::InternalError("recipe install: not a b-tree".into()));
    };
    let btree = Arc::make_mut(btree);
    if add_gen_col {
        btree.columns_mut().push(gen_column()?);
    }
    let recipe = parse_recipe(btree, generation, sql)?;
    attach_recipe(btree, recipe)?;
    count(counter::INSTALLED, 1);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Translation: an eligible UPDATE becomes a read-only pass plus a schema write.
// ---------------------------------------------------------------------------------------------

struct RecipePlan {
    btree: Arc<BTreeTable>,
    /// SET expressions and WHERE, resolved (for the read-only pass).
    exprs: Vec<Expr>,
    pred: Option<Expr>,
    /// The columns the SET expressions and WHERE read: all the read-only pass decodes.
    reads: Vec<usize>,
    sql: String,
    generation: u64,
    add_gen_col: bool,
}

fn refuse(_why: &'static str) -> Result<Option<RecipePlan>> {
    count(counter::FALLBACKS, 1);
    Ok(None)
}

/// PREREG §2.5. Every rule refuses (eager UPDATE) rather than approximates.
fn plan_recipe(
    body: &ast::Update,
    resolver: &Resolver,
    program: &ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<Option<RecipePlan>> {
    if body.with.is_some()
        || body.or_conflict.is_some()
        || body.indexed.is_some()
        || body.from.is_some()
        || !body.returning.is_empty()
    {
        return refuse("clause");
    }
    if body
        .tbl_name
        .db_name
        .as_ref()
        .is_some_and(|d| !d.as_str().eq_ignore_ascii_case("main"))
        || body.tbl_name.alias.is_some()
    {
        return refuse("not main");
    }
    if connection.mvcc_enabled()
        || program.capture_data_changes_info().is_some()
        || program.trigger.is_some()
        || program.flags.is_subprogram()
        || connection.is_nested_stmt()
    {
        return refuse("context");
    }
    let schema = resolver.schema();
    let name = normalize_ident(body.tbl_name.name.as_str());
    if name.starts_with("sqlite_") || name.starts_with("__turso_internal") {
        return refuse("system table");
    }
    let Some(btree) = schema.get_btree_table(&name) else {
        return refuse("not a b-tree table");
    };
    if !btree.has_rowid || btree.is_strict || btree.has_virtual_columns {
        return refuse("table shape");
    }
    if !btree.check_constraints.is_empty() {
        return refuse("check constraints");
    }
    if !schema.get_dependent_materialized_views(&name).is_empty() {
        return refuse("materialized views");
    }
    if crate::translate::trigger_exec::has_triggers_including_temp(
        resolver,
        MAIN_DB_ID,
        ast::TriggerEvent::Update,
        None,
        &btree,
    ) {
        return refuse("triggers");
    }
    let indexes: Vec<_> = schema.get_indices(&name).cloned().collect();
    if indexes
        .iter()
        .any(|ix| ix.where_clause.is_some() || ix.columns.iter().any(|c| c.expr.is_some()))
    {
        return refuse("expression or partial index");
    }
    let indexed = |col: usize| indexes.iter().any(|ix| ix.columns.iter().any(|c| c.pos_in_table == col));
    let fk_child = |col: &str| {
        btree
            .foreign_keys
            .iter()
            .any(|fk| fk.child_columns.iter().any(|c| normalize_ident(c) == col))
    };
    let fk_parent: Vec<usize> = schema
        .resolved_fks_referencing(&name)
        .map(|v| v.iter().flat_map(|r| r.parent_pos.iter().copied()).collect())
        .unwrap_or_default();
    let columns = btree.columns();
    let mut sets: Vec<(String, &Expr)> = Vec::new();
    let mut exprs = Vec::new();
    let mut targets = Vec::new();
    let mut reads: Vec<usize> = Vec::new();
    for set in &body.sets {
        let [col_name] = set.col_names.as_slice() else {
            return refuse("row-value SET");
        };
        let col_name = normalize_ident(col_name.as_str());
        let Some((idx, col)) = btree.get_column(&col_name) else {
            return refuse("unknown target");
        };
        if col.hidden()
            || col.is_rowid_alias()
            || col.primary_key()
            || col.notnull()
            || col.is_generated()
            || indexed(idx)
            || fk_child(&col_name)
            || fk_parent.contains(&idx)
            || targets.contains(&idx)
            || schema.get_type_def(&col.ty_str, btree.is_strict).is_some()
        {
            return refuse("target");
        }
        let Ok(e) = resolve_recipe_expr(&set.expr, columns) else {
            return refuse("SET expression");
        };
        let mut refs = Vec::new();
        referenced_columns(&e, &mut refs)?;
        // A self-table column reference carries the column's affinity but not its declared
        // collation (`get_expr_collation_ctx_with_symbols` skips SELF_TABLE), so a comparison over
        // a COLLATE column would differ from the eager UPDATE's. Refused.
        if refs
            .iter()
            .any(|&r| is_gen_column(&columns[r]) || columns[r].has_explicit_collation())
        {
            return refuse("gen column or collated column referenced");
        }
        reads.extend(refs);
        targets.push(idx);
        sets.push((col.name.clone().unwrap_or_default(), set.expr.as_ref()));
        exprs.push(e);
    }
    let pred = match body.where_clause.as_deref() {
        Some(w) => {
            let Ok(e) = resolve_recipe_expr(w, columns) else {
                return refuse("WHERE");
            };
            let mut refs = Vec::new();
            referenced_columns(&e, &mut refs)?;
            // A WHERE on the rowid, a key column or an indexed column can be a seek; only plans
            // that scan the table become recipes.
            if refs.iter().any(|&r| {
                let c = &columns[r];
                c.is_rowid_alias()
                    || c.primary_key()
                    || indexed(r)
                    || is_gen_column(c)
                    || c.has_explicit_collation()
            }) {
                return refuse("WHERE could seek, or reads a collated column");
            }
            reads.extend(refs);
            Some(e)
        }
        None => None,
    };
    let generation = btree.recipes.as_ref().map_or(0, |r| r.generation()) + 1;
    let sql = recipe_sql(&btree.name, &sets, body.where_clause.as_deref());
    // The text must round-trip to the same targets, or install would disagree with this plan.
    {
        let mut probe = (*btree).clone();
        if btree.recipes.is_none() {
            probe.columns_mut().push(gen_column()?);
        }
        let Ok(parsed) = parse_recipe(&probe, generation, &sql) else {
            return refuse("persisted text does not round-trip");
        };
        if parsed.targets != targets {
            return refuse("persisted text does not round-trip");
        }
    }
    reads.sort_unstable();
    reads.dedup();
    Ok(Some(RecipePlan {
        add_gen_col: btree.recipes.is_none(),
        btree,
        exprs,
        pred,
        reads,
        sql,
        generation,
    }))
}

/// Translate an UPDATE as a recipe when the connection asks for it and every rule allows it.
/// Returns false, having emitted nothing, when the stock eager UPDATE must run instead.
pub(crate) fn translate_update_as_recipe(
    body: &ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<bool> {
    if !connection.recipe_backfill() {
        return Ok(false);
    }
    let Some(plan) = plan_recipe(body, resolver, program, connection)? else {
        return Ok(false);
    };
    emit_recipe_update(plan, resolver, program)?;
    Ok(true)
}

fn emit_recipe_update(
    plan: RecipePlan,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let schema_version = resolver.schema().schema_version;
    program.begin_write_on_database(MAIN_DB_ID, schema_version)?;
    program.begin_write_operation()?;
    program.extend(&ProgramBuilderOpts::new(2, 32, 4));
    let btree = plan.btree.clone();
    let columns = btree.columns();
    let n = columns.len();

    // 1. Read-only pass: count the matched rows (changes()) and evaluate every SET expression on
    //    them, so an expression error fails the statement now, as the eager UPDATE would.
    let cursor = program.alloc_cursor_id(CursorType::BTreeTable(btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: cursor,
        root_page: btree.root_page,
        db: MAIN_DB_ID,
    });
    let count_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: count_reg,
    });
    let base = program.alloc_registers(n);
    let rowid_reg = program.alloc_register();
    let tmp = program.alloc_register();
    let layout = btree.column_layout()?;
    let scan_end = program.allocate_label();
    let next = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor,
        pc_if_empty: scan_end,
    });
    let top = program.allocate_label();
    program.preassign_label_to_next_insn(top);
    program.emit_insn(Insn::RowId {
        cursor_id: cursor,
        dest: rowid_reg,
    });
    // Only the columns the statement reads: a column it does not read cannot change its result,
    // and reading every column would evaluate every older recipe on every stale row.
    for &i in &plan.reads {
        program.emit_column_or_rowid(cursor, i, base + i);
        if columns[i].ty() == Type::Real {
            program.emit_insn(Insn::RealAffinity { register: base + i });
        }
    }
    if let Some(pred) = &plan.pred {
        crate::translate::emitter::gencol::emit_gencol_expr_from_registers(
            program, pred, tmp, base, columns, resolver, rowid_reg, &layout, &btree,
        )?;
        program.emit_insn(Insn::IfNot {
            reg: tmp,
            target_pc: next,
            jump_if_null: true,
        });
    }
    for e in &plan.exprs {
        crate::translate::emitter::gencol::emit_gencol_expr_from_registers(
            program, e, tmp, base, columns, resolver, rowid_reg, &layout, &btree,
        )?;
    }
    program.emit_insn(Insn::AddImm {
        register: count_reg,
        value: 1,
    });
    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor,
        pc_if_next: top,
        fullscan: true,
    });
    program.preassign_label_to_next_insn(scan_end);

    // 2. Nothing matched: nothing to install, changes() = 0.
    let done = program.allocate_label();
    let empty = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: count_reg,
        target_pc: empty,
        jump_if_null: true,
    });

    // 3. The schema write: the table row gains the gen column (first recipe only), then the
    //    recipe row. Both inserts are invisible to changes(), total_changes() and
    //    last_insert_rowid(), as the eager UPDATE's would be.
    let schema_table = resolver
        .schema()
        .get_btree_table(crate::schema::SCHEMA_TABLE_NAME)
        .ok_or_else(|| LimboError::InternalError("recipe: no sqlite_schema".into()))?;
    let sch = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sch,
        root_page: RegisterOrLiteral::Literal(1),
        db: MAIN_DB_ID,
    });
    let flags = InsertFlags::new()
        .skip_all_change_counts()
        .skip_last_rowid();
    let row = program.alloc_registers(5);
    let rid = program.alloc_register();
    let rec = program.alloc_register();
    if plan.add_gen_col {
        let mut with_gen = (*btree).clone();
        with_gen.columns_mut().push(gen_column()?);
        let new_sql = with_gen.to_sql();
        let type_reg = program.emit_string8_new_reg("table".to_string());
        let name_reg = program.emit_string8_new_reg(btree.name.clone());
        let s_end = program.allocate_label();
        let s_next = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: sch,
            pc_if_empty: s_end,
        });
        let s_top = program.allocate_label();
        program.preassign_label_to_next_insn(s_top);
        program.emit_column_or_rowid(sch, 0, row);
        program.emit_insn(Insn::Ne {
            lhs: row,
            rhs: type_reg,
            target_pc: s_next,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_column_or_rowid(sch, 1, row + 1);
        program.emit_insn(Insn::Ne {
            lhs: row + 1,
            rhs: name_reg,
            target_pc: s_next,
            flags: CmpInsFlags::default(),
            collation: Some(crate::translate::collate::CollationSeq::NoCase),
        });
        for i in 2..4 {
            program.emit_column_or_rowid(sch, i, row + i);
        }
        program.emit_string8(new_sql, row + 4);
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(row),
            count: to_u32(5),
            dest_reg: to_u32(rec),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::RowId {
            cursor_id: sch,
            dest: rid,
        });
        program.emit_insn(Insn::Insert {
            cursor: sch,
            key_reg: rid,
            record_reg: rec,
            flag: flags,
            table_name: crate::schema::SCHEMA_TABLE_NAME.to_string(),
        });
        program.preassign_label_to_next_insn(s_next);
        program.emit_insn(Insn::Next {
            cursor_id: sch,
            pc_if_next: s_top,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(s_end);
    }
    program.emit_insn(Insn::NewRowid {
        cursor: sch,
        rowid_reg: rid,
        prev_largest_reg: 0,
    });
    program.emit_string8(SCHEMA_TYPE.to_string(), row);
    program.emit_string8(recipe_name(&btree.name, plan.generation), row + 1);
    program.emit_string8(normalize_ident(&btree.name), row + 2);
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: row + 3,
    });
    program.emit_string8(plan.sql.clone(), row + 4);
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(row),
        count: to_u32(5),
        dest_reg: to_u32(rec),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: sch,
        key_reg: rid,
        record_reg: rec,
        flag: flags,
        table_name: crate::schema::SCHEMA_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });
    program.emit_insn(Insn::RecipeInstall {
        data: Box::new(crate::vdbe::insn::RecipeInstallData {
            table: btree.name.clone(),
            generation: plan.generation,
            sql: plan.sql,
            add_gen_col: plan.add_gen_col,
            count_reg,
            empty: false,
        }),
    });
    program.emit_insn(Insn::Goto { target_pc: done });
    program.preassign_label_to_next_insn(empty);
    program.emit_insn(Insn::RecipeInstall {
        data: Box::new(crate::vdbe::insn::RecipeInstallData {
            table: btree.name.clone(),
            generation: 0,
            sql: String::new(),
            add_gen_col: false,
            count_reg,
            empty: true,
        }),
    });
    program.preassign_label_to_next_insn(done);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Execution: the read path at the column-read choke point.
// ---------------------------------------------------------------------------------------------

/// One recipe compiled for this statement: a pure sub-program over a register block.
struct CompiledRecipe {
    program: Program,
    state: ProgramState,
    base: usize,
    rowid_reg: usize,
    outs: Vec<usize>,
    flag: usize,
    inputs: Vec<usize>,
    targets: Vec<usize>,
}

impl CompiledRecipe {
    fn compile(conn: &Arc<Connection>, table: &Arc<BTreeTable>, recipe: &Recipe) -> Result<Self> {
        let m = mutant();
        let schema = conn.schema.read().clone();
        let syms = conn.syms.read();
        let resolver = Resolver::new(
            &schema,
            conn.database_schemas(),
            &conn.temp.database,
            conn.attached_databases(),
            &syms,
            conn.experimental_custom_types_enabled(),
            conn.get_dqs_dml().into(),
            Arc::new(crate::dialect::SqliteDialect),
            &None,
        );
        let columns = table.columns();
        let n = columns.len();
        let layout = table.column_layout()?;
        let mut b = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 32, 0));
        let base = b.alloc_registers(n);
        let rowid_reg = b.alloc_register();
        let pred_reg = b.alloc_register();
        let flag = b.alloc_register();
        let outs: Vec<usize> = recipe.targets.iter().map(|_| b.alloc_register()).collect();
        // Column references in the eager UPDATE read through `Column` + RealAffinity.
        for &i in &recipe.inputs {
            if columns[i].ty() == Type::Real {
                b.emit_insn(Insn::RealAffinity { register: base + i });
            }
        }
        let unmatched = b.allocate_label();
        let end = b.allocate_label();
        if let (Some(pred), false) = (&recipe.pred, m == Mutant::M6) {
            crate::translate::emitter::gencol::emit_gencol_expr_from_registers(
                &mut b, pred, pred_reg, base, columns, &resolver, rowid_reg, &layout, table,
            )?;
            b.emit_insn(Insn::IfNot {
                reg: pred_reg,
                target_pc: unmatched,
                jump_if_null: true,
            });
        }
        for (k, e) in recipe.exprs.iter().enumerate() {
            crate::translate::emitter::gencol::emit_gencol_expr_from_registers(
                &mut b, e, outs[k], base, columns, &resolver, rowid_reg, &layout, table,
            )?;
            let aff = columns[recipe.targets[k]].affinity();
            if aff != crate::vdbe::affinity::Affinity::Blob && m != Mutant::M3 {
                b.emit_column_affinity(outs[k], aff);
            }
        }
        b.emit_insn(Insn::Integer { value: 1, dest: flag });
        b.emit_insn(Insn::Goto { target_pc: end });
        b.preassign_label_to_next_insn(unmatched);
        b.emit_insn(Insn::Integer { value: 0, dest: flag });
        b.preassign_label_to_next_insn(end);
        b.emit_insn(Insn::Noop);
        let program = b.build(conn.clone(), false, "")?;
        let state = ProgramState::new(program.max_registers, 0);
        count(counter::COMPILES, 1);
        Ok(Self {
            program,
            state,
            base,
            rowid_reg,
            outs,
            flag,
            inputs: recipe.inputs.clone(),
            targets: recipe.targets.clone(),
        })
    }

    /// Apply this recipe to `values` (the row's state before it).
    fn run(&mut self, values: &mut [Value], rowid: i64, pager: &Arc<Pager>) -> Result<()> {
        for &i in &self.inputs {
            self.state
                .set_register(self.base + i, Register::Value(values[i].clone()));
        }
        self.state
            .set_register(self.rowid_reg, Register::Value(Value::from_i64(rowid)));
        self.state.pc = 0;
        let insns = &self.program.insns;
        while (self.state.pc as usize) < insns.len() {
            let (insn, _) = &insns[self.state.pc as usize];
            match insn.to_function()(&self.program, &mut self.state, insn, pager)? {
                InsnFunctionStepResult::Step => {}
                InsnFunctionStepResult::Done => break,
                InsnFunctionStepResult::IO(_) | InsnFunctionStepResult::Row => {
                    return Err(LimboError::InternalError(
                        "recipe sub-program yielded".to_string(),
                    ))
                }
            }
        }
        count(counter::RECIPE_EVALS, 1);
        let matched = matches!(
            self.state.get_register(self.flag).get_value(),
            Value::Numeric(crate::numeric::Numeric::Integer(1))
        );
        if matched {
            for (k, &t) in self.targets.iter().enumerate() {
                values[t] = self.state.get_register(self.outs[k]).get_value().clone();
            }
        }
        Ok(())
    }
}

/// The current row's decoded physical values and the logical values computed from them.
struct RowCache {
    cursor_id: usize,
    rowid: i64,
    phys_gen: u64,
    physical: Vec<Value>,
    logical: Vec<Option<Value>>,
}

/// Per-statement execution state of the read path (lives in `ProgramState`, cleared on reset).
#[derive(Default)]
pub(crate) struct RecipeExec {
    compiled: HashMap<(usize, usize), CompiledRecipe>,
    rows: Vec<RowCache>,
}

impl RecipeExec {
    pub(crate) fn clear(&mut self) {
        self.compiled.clear();
        self.rows.clear();
    }
}

fn physical_generation(record: &crate::types::ImmutableRecord, gen_col: usize) -> Result<u64> {
    match record.get_value_opt(gen_col) {
        None => Ok(0),
        Some(crate::types::ValueRef::Null) => Ok(0),
        Some(crate::types::ValueRef::Numeric(crate::numeric::Numeric::Integer(g))) if g >= 0 => {
            Ok(g as u64)
        }
        Some(other) => Err(LimboError::Corrupt(format!(
            "{GEN_COLUMN} holds {other:?}, not a generation"
        ))),
    }
}

/// The physical generation of the row a cursor is on, or `None` when the cursor is not on a row of
/// the table's own b-tree (an ephemeral table typed as this one, a NULL row, no row).
fn current_row(
    state: &mut ProgramState,
    cursor_id: usize,
    table: &BTreeTable,
    rs: &TableRecipes,
    pager: &Arc<Pager>,
) -> Result<IOResult<Option<(i64, u64)>>> {
    let cursor = state.get_cursor(cursor_id);
    let crate::types::Cursor::BTree(btc) = cursor else {
        return Ok(IOResult::Done(None));
    };
    if btc.root_page() != table.root_page
        || !Arc::ptr_eq(&btc.get_pager(), pager)
        || btc.get_null_flag()
    {
        return Ok(IOResult::Done(None));
    }
    let Some(rowid) = crate::return_if_io!(btc.rowid()) else {
        return Ok(IOResult::Done(None));
    };
    let Some(record) = crate::return_if_io!(btc.record()) else {
        return Ok(IOResult::Done(None));
    };
    Ok(IOResult::Done(Some((rowid, physical_generation(record, rs.gen_col)?))))
}

/// Is the cursor's current row of a recipe table stale (physical generation below `G`)? A raw
/// copy of such a record would carry pre-recipe values, so raw-record consumers refuse it.
pub(crate) fn row_is_stale(
    state: &mut ProgramState,
    cursor_id: usize,
    table: &BTreeTable,
    pager: &Arc<Pager>,
) -> Result<IOResult<bool>> {
    let Some(rs) = table.recipes.as_deref() else {
        return Ok(IOResult::Done(false));
    };
    let row = crate::return_if_io!(current_row(state, cursor_id, table, rs, pager));
    Ok(IOResult::Done(
        row.is_some_and(|(_, g)| g < rs.generation()),
    ))
}

/// The read path. Returns `Done(true)` when `dest` has been set, `Done(false)` when the stock
/// column fetch must run (the column has no closure, the record is current, or the cursor is not
/// on the table's own b-tree).
#[allow(clippy::too_many_arguments)]
pub(crate) fn column_fetch(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    table: &Arc<BTreeTable>,
    rs: &Arc<TableRecipes>,
    column: usize,
    dest: usize,
    pager: &Arc<Pager>,
) -> Result<IOResult<bool>> {
    let m = mutant();
    if column == rs.gen_col {
        if m == Mutant::M1 {
            return Ok(IOResult::Done(false));
        }
        // Only on the table's own b-tree; an ephemeral copy reads what it stored.
        let on_table = crate::return_if_io!(current_row(state, cursor_id, table, rs, pager));
        if on_table.is_none() {
            return Ok(IOResult::Done(false));
        }
        state.set_register(dest, Register::Value(Value::from_i64(rs.generation() as i64)));
        return Ok(IOResult::Done(true));
    }
    let closure = rs.closure_of(column);
    if debug() {
        eprintln!(
            "recipe dbg: table={} col={column} closure={closure:?} recipes={} gen_col={} cols={}",
            table.name,
            rs.recipes.len(),
            rs.gen_col,
            table.columns().len()
        );
    }
    if closure.is_empty() {
        return Ok(IOResult::Done(false));
    }
    if m == Mutant::M8 && !program.readonly {
        return Ok(IOResult::Done(false));
    }
    let Some((rowid, phys_gen)) =
        crate::return_if_io!(current_row(state, cursor_id, table, rs, pager))
    else {
        if debug() {
            eprintln!("recipe dbg: not on the table's own b-tree (cursor {cursor_id})");
        }
        return Ok(IOResult::Done(false));
    };
    if debug() {
        eprintln!("recipe dbg: rowid={rowid} phys_gen={phys_gen} newest={}", rs.newest_of(column));
    }
    let apply_all = m == Mutant::M5;
    if phys_gen >= rs.newest_of(column) && !apply_all {
        return Ok(IOResult::Done(false));
    }
    count(counter::STALE_READS, 1);

    let mut exec = std::mem::take(&mut state.recipe_exec);
    let result = (|| -> Result<IOResult<bool>> {
        let slot = exec.rows.iter().position(|r| r.cursor_id == cursor_id);
        let hit = slot.is_some_and(|s| {
            let r = &exec.rows[s];
            m == Mutant::M7 || (r.rowid == rowid && r.phys_gen == phys_gen)
        });
        if hit {
            if let Some(v) = &exec.rows[slot.unwrap()].logical[column] {
                count(counter::CACHE_HITS, 1);
                state.set_register(dest, Register::Value(v.clone()));
                return Ok(IOResult::Done(true));
            }
        } else {
            // Decode the record's fields; a short record reads each missing column's default.
            let physical = {
                let cursor = state.get_cursor(cursor_id);
                let btc = cursor.as_btree_mut();
                let Some(record) = crate::return_if_io!(btc.record()) else {
                    return Ok(IOResult::Done(false));
                };
                let mut vals = record.get_values_owned()?;
                let n = rs.defaults.len().max(table.columns().len());
                for i in vals.len()..n {
                    vals.push(rs.defaults.get(i).cloned().flatten().unwrap_or(Value::Null));
                }
                for (i, alias) in rs.rowid_alias.iter().enumerate() {
                    if *alias && i < vals.len() {
                        vals[i] = Value::from_i64(rowid);
                    }
                }
                vals
            };
            let entry = RowCache {
                cursor_id,
                rowid,
                phys_gen,
                logical: vec![None; physical.len()],
                physical,
            };
            match slot {
                Some(s) => exec.rows[s] = entry,
                None => exec.rows.push(entry),
            }
        }
        let s = exec
            .rows
            .iter()
            .position(|r| r.cursor_id == cursor_id)
            .expect("row cache entry was just ensured");
        let mut vals = vec![Value::Null; exec.rows[s].physical.len()];
        for &i in rs.deps.get(column).map_or(&[][..], |d| d.as_slice()) {
            vals[i] = exec.rows[s].physical[i].clone();
        }
        let order: Vec<usize> = if m == Mutant::M4 {
            closure.iter().rev().copied().collect()
        } else {
            closure.to_vec()
        };
        let key_base = Arc::as_ptr(rs) as usize;
        for ri in order {
            let recipe = &rs.recipes[ri];
            if recipe.generation <= phys_gen && !apply_all {
                continue;
            }
            if !exec.compiled.contains_key(&(key_base, ri)) {
                let c = CompiledRecipe::compile(&program.connection, table, recipe)?;
                exec.compiled.insert((key_base, ri), c);
            }
            exec.compiled
                .get_mut(&(key_base, ri))
                .expect("compiled recipe was just inserted")
                .run(&mut vals, rowid, pager)?;
        }
        let v = vals[column].clone();
        state.set_register(dest, Register::Value(v.clone()));
        exec.rows[s].logical[column] = Some(v);
        Ok(IOResult::Done(true))
    })();
    state.recipe_exec = exec;
    result
}
