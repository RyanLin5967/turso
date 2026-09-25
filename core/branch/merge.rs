//! Merging a branch back into the trunk.
//!
//! # What a merge is here
//!
//! A branch B forked from the trunk at trunk epoch `trunk_at` has written some pages and some rows.
//! Merging it makes B's changes the trunk's, inside ONE trunk write transaction (`BEGIN IMMEDIATE`
//! … `COMMIT`), so that validation and install both run under the trunk's WAL write lock: no trunk
//! commit — and no fork, which takes the same lock — can fall between deciding and writing. The
//! semantics are a three-way merge's (git, Dolt): base = the trunk as B forked it, ours = the trunk
//! now, theirs = B. B's change is refused when the trunk changed the same thing after the fork; what
//! B only read is not validated (write-write conflicts only, snapshot-isolation shaped).
//!
//! Scope: a merge takes a direct child of the trunk with no open connection, no live child, no DDL
//! and no write that names no row (clear, destroy, incremental blob I/O), whose index writes are all
//! to indexes of rowid tables. Anything else is refused as [`Refusal::Scope`], not merged.
//!
//! # Validation: what "the trunk changed the same thing" is checked against
//!
//! * [`Validation::Scalar`] — any trunk commit since the fork. ferrodb's D174 gate and git's
//!   fast-forward-only rule: one comparison, and every merge conflicts with every other.
//! * [`Validation::Log`] — Kung & Robinson's backward validation (TODS 1981), which is also how
//!   Iceberg and Delta validate a commit against every snapshot since its base: the committed trunk
//!   write sets since the fork, page by page, against B's written pages.
//! * [`Validation::PageStamp`] — the trunk's per-page last-write epoch (`TrunkState::written`), which
//!   the copy-on-write decision already keeps: B's written pages, one lookup each. Silo's and
//!   Hekaton's per-record timestamps, at page granularity.
//! * [`Validation::KeyStamp`] — the same at row granularity: a stamp per `(table root, rowid)` that
//!   `BTreeCursor::insert`/`delete` set on the trunk while it has a child, checked against the rows
//!   B's table cursors wrote.
//!
//! Every merge reports the scalar, page and key verdicts whichever validator decides, so page- and
//! row-granular refusal rates can be read on one stream of merges.
//!
//! # Install
//!
//! * [`Install::Physical`] — the page-level three-way merge: B's pages are copied over the trunk's.
//!   Sound only where the trunk did not write the page since the fork (so ours = base), which a
//!   page-granular validator establishes, and only if the trunk did not restructure the tree above
//!   B's pages: a split or a free on the trunk moves or orphans a leaf B wrote WITHOUT writing that
//!   leaf, but it always writes an interior page B read to reach it. So the physical install needs
//!   B's page reads (see [`crate::Database::set_branch_read_tracking`]) and refuses as
//!   [`Refusal::Structural`] when the trunk wrote an interior page B read.
//! * [`Install::Replay`] — the logical install: each row B wrote is copied from B into the trunk
//!   through the trunk's own SQL path (UPDATE, INSERT if the trunk lacks it, DELETE if B does), so
//!   splits, page allocation and indexes are the trunk's. Works under every validator.
//!
//! [`Merger::merge_batch`] validates and installs several branches in one trunk transaction (group
//! commit); each member is validated after the members before it installed, which only the stamp
//! validators can see, so a batch requires one of them.

use std::collections::HashMap;

use super::store::BranchStore;
use super::{Branch, BranchId};
use crate::schema::{BTreeTable, Schema, Table};
use crate::sync::Arc;
use crate::{Connection, LimboError, Result, Statement, Value};

/// What a merge is validated against. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Validation {
    Scalar,
    Log,
    PageStamp,
    KeyStamp,
}

/// How a validated merge's changes reach the trunk. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Install {
    Physical,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergePolicy {
    pub validation: Validation,
    pub install: Install,
}

/// Why a merge was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// Outside what a merge takes (see the module doc); not a conflict.
    Scope,
    Scalar,
    Log,
    Page,
    Key,
    /// The physical install's guard: the trunk restructured the tree above B's pages.
    Structural,
}

/// One merge's result, with every validator's verdict beside the one that decided.
#[derive(Debug, Clone)]
pub struct MergeOutcome {
    pub branch: BranchId,
    /// `None` if the merge committed.
    pub refused: Option<Refusal>,
    pub scope: Option<&'static str>,
    /// Trunk write transactions committed between the fork and this merge's validation.
    pub commits_since_fork: u64,
    pub scalar_conflict: bool,
    pub page_conflict: bool,
    pub key_conflict: bool,
    /// The log validator's verdict, when it was the active one.
    pub log_conflict: Option<bool>,
    /// The structural guard's verdict, under the physical install.
    pub structural_conflict: Option<bool>,
    /// Pages the branch wrote.
    pub pages_written: usize,
    /// Rows the branch's table cursors wrote.
    pub rows_written: usize,
}

/// What [`BranchStore::merge_prepare`] reads for a merge under one hold of the store lock.
pub(super) struct Prepared {
    pub(super) scope: Option<&'static str>,
    pub(super) commits_since_fork: u64,
    pub(super) scalar: bool,
    pub(super) log: Option<bool>,
    pub(super) page: bool,
    pub(super) key: bool,
    pub(super) structural: Option<bool>,
    pub(super) pages_written: usize,
    /// The branch's pages, for the physical install (empty otherwise).
    pub(super) pages: Vec<(u32, Vec<u8>)>,
    /// The branch's rows, sorted.
    pub(super) rows: Vec<(i64, i64)>,
    pub(super) index_roots: Vec<i64>,
    pub(super) schema: Arc<Schema>,
}

/// The trunk statements that write one table's rows.
struct TableStmts {
    update: Statement,
    insert: Statement,
    delete: Statement,
    /// Columns in order; the rowid alias, if any, carries the rowid.
    columns: usize,
    alias: Option<usize>,
}

/// Merges branches into the trunk through one trunk connection, keeping the trunk statements the
/// replay install prepares.
pub struct Merger {
    trunk: Arc<Connection>,
    store: Arc<BranchStore>,
    stmts: HashMap<i64, TableStmts>,
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The ordinary rowid table rooted at `root`, if there is one a merge may write.
fn table_at(schema: &Schema, root: i64) -> Option<Arc<BTreeTable>> {
    schema.tables.values().find_map(|t| match t.as_ref() {
        Table::BTree(bt)
            if bt.root_page == root
                && bt.has_rowid
                && !bt.name.starts_with("sqlite_")
                && !bt.columns().iter().any(|c| c.is_generated()) =>
        {
            Some(bt.clone())
        }
        _ => None,
    })
}

/// Why `prep`'s rows and index writes are outside what a merge takes, if they are.
fn row_scope(prep: &Prepared) -> Option<&'static str> {
    let mut last = None;
    for &(root, _) in &prep.rows {
        if last == Some(root) {
            continue;
        }
        last = Some(root);
        if table_at(&prep.schema, root).is_none() {
            return Some("the branch wrote rows of a table a merge does not take");
        }
    }
    let indexes_of_rowid_tables = |root: i64| {
        prep.schema.indexes.values().flatten().any(|ix| {
            ix.root_page == root
                && ix.has_rowid
                && prep
                    .schema
                    .get_btree_table(&ix.table_name)
                    .is_some_and(|t| t.has_rowid)
        })
    };
    if prep.index_roots.iter().any(|&r| !indexes_of_rowid_tables(r)) {
        return Some("the branch wrote a b-tree that is not an index of a rowid table");
    }
    None
}

fn step_to_end(stmt: &mut Statement) -> Result<()> {
    stmt.run_ignore_rows()?;
    stmt.reset()
}

impl Merger {
    /// A merger writing through `trunk`, which must be a trunk connection.
    pub fn new(trunk: Arc<Connection>) -> Result<Self> {
        if trunk.branch_id().is_some() {
            return Err(LimboError::InvalidArgument(
                "a merger writes the trunk; this connection is on a branch".to_string(),
            ));
        }
        let store = trunk.db.branches.clone();
        Ok(Self {
            trunk,
            store,
            stmts: HashMap::new(),
        })
    }

    /// Merge one branch. The branch is released afterwards, merged or refused.
    pub fn merge(&mut self, branch: Branch, policy: MergePolicy) -> Result<MergeOutcome> {
        Ok(self
            .merge_batch(vec![branch], policy)?
            .pop()
            .expect("one branch in, one outcome out"))
    }

    /// Merge `branches` in one trunk transaction, in order (group commit). Every branch is
    /// released afterwards, merged or refused.
    pub fn merge_batch(
        &mut self,
        branches: Vec<Branch>,
        policy: MergePolicy,
    ) -> Result<Vec<MergeOutcome>> {
        let physical = policy.install == Install::Physical;
        if physical && policy.validation == Validation::KeyStamp {
            return Err(LimboError::InvalidArgument(
                "the physical install copies whole pages, so it needs a page-granular validator"
                    .to_string(),
            ));
        }
        if physical && !self.store.tracks_reads() {
            return Err(LimboError::InvalidArgument(
                "the physical install's structural guard needs branch read tracking \
                 (Database::set_branch_read_tracking)"
                    .to_string(),
            ));
        }
        if branches.len() > 1
            && !matches!(
                policy.validation,
                Validation::PageStamp | Validation::KeyStamp
            )
        {
            return Err(LimboError::InvalidArgument(
                "a batch member must be validated against the members installed before it in the \
                 same transaction, which only the stamp validators see"
                    .to_string(),
            ));
        }
        self.trunk.execute("BEGIN IMMEDIATE")?;
        let result = self.validate_and_install(&branches, policy);
        match &result {
            Ok(outcomes) if outcomes.iter().any(|o| o.refused.is_none()) => {
                if let Err(err) = self.trunk.execute("COMMIT") {
                    let _ = self.trunk.execute("ROLLBACK");
                    return Err(err);
                }
            }
            _ => self.trunk.execute("ROLLBACK")?,
        }
        let outcomes = result?;
        self.store.merge_counted(|w| {
            for o in &outcomes {
                match o.refused {
                    None => w.merge_commits += 1,
                    Some(Refusal::Scope) => w.merge_refused_scope += 1,
                    Some(Refusal::Scalar) => w.merge_refused_scalar += 1,
                    Some(Refusal::Log) => w.merge_refused_log += 1,
                    Some(Refusal::Page) => w.merge_refused_page += 1,
                    Some(Refusal::Key) => w.merge_refused_key += 1,
                    Some(Refusal::Structural) => w.merge_refused_structural += 1,
                }
            }
        });
        drop(branches);
        Ok(outcomes)
    }

    fn validate_and_install(
        &mut self,
        branches: &[Branch],
        policy: MergePolicy,
    ) -> Result<Vec<MergeOutcome>> {
        let physical = policy.install == Install::Physical;
        let mut outcomes = Vec::with_capacity(branches.len());
        for branch in branches {
            let prep = self
                .store
                .merge_prepare(branch.id, policy.validation, physical)?;
            let scope = prep.scope.or_else(|| row_scope(&prep));
            let refused = if scope.is_some() {
                Some(Refusal::Scope)
            } else {
                let conflict = match policy.validation {
                    Validation::Scalar => prep.scalar.then_some(Refusal::Scalar),
                    Validation::Log => prep.log.unwrap_or(true).then_some(Refusal::Log),
                    Validation::PageStamp => prep.page.then_some(Refusal::Page),
                    Validation::KeyStamp => prep.key.then_some(Refusal::Key),
                };
                conflict.or((prep.structural == Some(true)).then_some(Refusal::Structural))
            };
            if refused.is_none() {
                if physical {
                    self.install_pages(&prep)?;
                } else {
                    self.install_rows(branch, &prep)?;
                }
            }
            outcomes.push(MergeOutcome {
                branch: branch.id,
                refused,
                scope,
                commits_since_fork: prep.commits_since_fork,
                scalar_conflict: prep.scalar,
                page_conflict: prep.page,
                key_conflict: prep.key,
                log_conflict: prep.log,
                structural_conflict: prep.structural,
                pages_written: prep.pages_written,
                rows_written: prep.rows.len(),
            });
        }
        Ok(outcomes)
    }

    fn install_pages(&self, prep: &Prepared) -> Result<()> {
        let pager = self.trunk.pager.load().clone();
        let db_size = pager.trunk_db_size()?;
        for (page, image) in &prep.pages {
            pager.install_page_image(*page, image, db_size)?;
        }
        self.store.trunk_rows_written(&prep.rows);
        self.store
            .merge_counted(|w| w.merge_pages_installed += prep.pages.len() as u64);
        Ok(())
    }

    fn install_rows(&mut self, branch: &Branch, prep: &Prepared) -> Result<()> {
        if prep.rows.is_empty() {
            return Ok(());
        }
        let from = branch.connect()?;
        let mut selects: HashMap<i64, Statement> = HashMap::new();
        for &(root, rowid) in &prep.rows {
            let table = table_at(&prep.schema, root).expect("row_scope admitted this table");
            if !self.stmts.contains_key(&root) {
                let stmts = self.prepare_table(&table)?;
                self.stmts.insert(root, stmts);
            }
            if !selects.contains_key(&root) {
                let cols: Vec<String> = table
                    .columns()
                    .iter()
                    .map(|c| quote(c.name.as_deref().unwrap_or("")))
                    .collect();
                let sql = format!(
                    "SELECT {} FROM {} WHERE rowid = ?1",
                    cols.join(", "),
                    quote(&table.name)
                );
                selects.insert(root, from.prepare(sql)?);
            }
            let select = selects.get_mut(&root).expect("just prepared");
            select.bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
            let found = select.run_collect_rows()?;
            select.reset()?;
            let stmts = self.stmts.get_mut(&root).expect("just prepared");
            match found.as_slice() {
                [] => {
                    stmts
                        .delete
                        .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                    step_to_end(&mut stmts.delete)?;
                }
                [row] => {
                    stmts
                        .update
                        .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                    for (i, v) in row.iter().enumerate() {
                        stmts
                            .update
                            .bind_at((i + 2).try_into().unwrap(), v.clone())?;
                    }
                    step_to_end(&mut stmts.update)?;
                    if self.trunk.changes() == 0 {
                        let base = match stmts.alias {
                            Some(_) => 1,
                            None => {
                                stmts
                                    .insert
                                    .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                                2
                            }
                        };
                        for (i, v) in row.iter().enumerate() {
                            let v = if stmts.alias == Some(i) {
                                Value::from_i64(rowid)
                            } else {
                                v.clone()
                            };
                            stmts.insert.bind_at((i + base).try_into().unwrap(), v)?;
                        }
                        step_to_end(&mut stmts.insert)?;
                    }
                    debug_assert_eq!(row.len(), stmts.columns);
                }
                _ => {
                    return Err(LimboError::Corrupt(format!(
                        "branch {} has {} rows with rowid {rowid}",
                        branch.id.0,
                        found.len()
                    )))
                }
            }
        }
        drop(selects);
        drop(from);
        self.store
            .merge_counted(|w| w.merge_rows_installed += prep.rows.len() as u64);
        Ok(())
    }

    fn prepare_table(&self, table: &BTreeTable) -> Result<TableStmts> {
        let names: Vec<String> = table
            .columns()
            .iter()
            .map(|c| quote(c.name.as_deref().unwrap_or("")))
            .collect();
        let alias = table.get_rowid_alias_column().map(|(i, _)| i);
        let t = quote(&table.name);
        let sets: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(i, n)| format!("{n} = ?{}", i + 2))
            .collect();
        let update = format!("UPDATE {t} SET {} WHERE rowid = ?1", sets.join(", "));
        let insert = match alias {
            Some(_) => {
                let params: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT INTO {t}({}) VALUES ({})",
                    names.join(", "),
                    params.join(", ")
                )
            }
            None => {
                let params: Vec<String> = (2..=names.len() + 1).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT INTO {t}(rowid, {}) VALUES (?1, {})",
                    names.join(", "),
                    params.join(", ")
                )
            }
        };
        let delete = format!("DELETE FROM {t} WHERE rowid = ?1");
        Ok(TableStmts {
            update: self.trunk.prepare(update)?,
            insert: self.trunk.prepare(insert)?,
            delete: self.trunk.prepare(delete)?,
            columns: names.len(),
            alias,
        })
    }
}
