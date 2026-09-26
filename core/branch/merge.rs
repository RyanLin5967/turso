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
//! to indexes of rowid tables, and whose schema is still the trunk's (no trunk DDL since the fork).
//! Anything else is refused as [`Refusal::Scope`], not merged.
//!
//! # Validation: what "the trunk changed the same thing" is checked against
//!
//! * [`Validation::Scalar`] — any trunk commit since the fork (or, inside a batch, any page an
//!   earlier member wrote). ferrodb's D174 gate and git's fast-forward-only rule: one comparison,
//!   and every merge conflicts with every other.
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
//!   page-granular validator establishes, and only if the page is still where B's tree put it. A
//!   balance dirties every sibling it touches (`balance_non_root`), so a trunk delete or split that
//!   moves B's rows writes B's page and is a page conflict. What a page stamp cannot see is a page
//!   freed without a balance: `Pager::free_page` does not write the page it frees, so after a
//!   `clear_btree` or `btree_destroy` on the trunk, B's page could sit on the freelist unwritten and
//!   copying it back would orphan B's rows. Every such operation writes the b-tree's root, an
//!   interior page B read to reach its leaf. So the physical install needs B's page reads (see
//!   [`crate::Database::set_branch_read_tracking`]) and refuses as [`Refusal::Structural`] when the
//!   trunk wrote an interior page B read. That is conservative: a split or a merge of other leaves
//!   under the same parent also writes it. Through SQL on the trunk, without DDL (which a merge
//!   refuses as out of scope), no free-without-write path was found in this fork, so in the
//!   measured workloads every structural refusal is a false conflict (frontier/round11/r11-merge).
//! * [`Install::Replay`] — the logical install: each row B wrote is copied from B into the trunk
//!   as a ROW IMAGE through the trunk's own SQL path (UPDATE, INSERT if the trunk lacks it, DELETE
//!   if B does), so splits, page allocation and indexes are the trunk's. Works under every
//!   validator. A row image is final: B's triggers and foreign-key actions already wrote their own
//!   rows, which are in B's write set, so the trunk compiles the install with no triggers and no
//!   foreign-key actions (PostgreSQL's `session_replication_role = replica`), and when the
//!   caller's connection enforces foreign keys they are checked after each member instead, over
//!   the member's own rows and the parent keys they replaced. Statements say OR ABORT, so a
//!   constraint-level REPLACE can never delete a row only the trunk has; rows go in B's last-write
//!   order, so a UNIQUE value B moved leaves its old row before it reaches the new one.
//!
//! [`Merger::merge_batch`] validates and installs several branches in one trunk transaction (group
//! commit); each member is validated after the members before it installed, which only the stamp
//! validators can see, so a batch requires one of them. Each member installs inside its own
//! SAVEPOINT: a constraint that refuses one member ([`Refusal::Install`]) rolls back only that
//! member, never the batch.

use std::collections::HashMap;

use super::store::BranchStore;
use super::{Branch, BranchId};
use crate::schema::{BTreeTable, ForeignKey, Schema, Table};
use crate::sync::atomic::Ordering;
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
    /// Validation passed but installing refused: a trunk constraint (UNIQUE, NOT NULL, CHECK, or a
    /// foreign key checked after the row images went in) rejects the branch's rows, or the rows no
    /// longer fit the table's column list. Only this member is rolled back (its own SAVEPOINT).
    Install,
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
    /// Why the install refused, when `refused` is [`Refusal::Install`].
    pub install_error: Option<String>,
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
    /// The branch's rows, in the order of each row's last write on the branch.
    pub(super) rows: Vec<(i64, i64)>,
    pub(super) index_roots: Vec<i64>,
    pub(super) schema: Arc<Schema>,
}

/// The trunk statements that write one table's rows.
struct TableStmts {
    /// Sets every column but the rowid alias. `None` when the alias is the only column.
    update: Option<Statement>,
    insert: Statement,
    delete: Statement,
    /// Columns in order; the rowid alias, if any, carries the rowid.
    columns: usize,
    alias: Option<usize>,
}

/// One foreign key of the trunk's schema, as the per-member check runs it.
struct FkCheck {
    /// "child referencing parent", for the refusal's reason.
    name: String,
    /// `Err(reason)` when the key cannot be checked (its parent does not exist, or its column
    /// count differs from the child's): SQLite then refuses every write to either table, and a
    /// member that writes one is refused.
    stmts: std::result::Result<FkStmts, String>,
}

struct FkStmts {
    /// `?1` = a parent rowid: that row's key. `None` for a parent without rowids.
    key: Option<Statement>,
    /// `?1` = a child rowid: a row back when that child's key is wholly non-NULL and no parent
    /// holds it. `None` for a child without rowids.
    child: Option<Statement>,
    /// `?1..?n` = a parent key: a row back when some child holds it and no parent does.
    parent: Statement,
}

/// Every foreign key of one trunk schema version, indexed by the tables it touches.
struct FkChecks {
    version: u32,
    checks: Vec<FkCheck>,
    by_child: HashMap<i64, Vec<usize>>,
    by_parent: HashMap<i64, Vec<usize>>,
}

/// Merges branches into the trunk, keeping the trunk statements the replay install prepares, per
/// table root AND trunk schema version: a statement prepared for an older column list must never
/// write a row of a newer one.
pub struct Merger {
    /// The caller's trunk connection. Read at every batch for its settings (foreign_keys,
    /// synchronous, WAL auto-actions, data-sync retry); never written through.
    caller: Arc<Connection>,
    /// The merger's OWN trunk connection, which compiles for row images for its whole life: no
    /// triggers and no foreign-key actions (see the module doc). Its own connection, so no statement
    /// of the caller's ever compiles that way, a panic cannot leave the caller's connection in that
    /// state, and the merger's cached statements are not prepared again at every batch (the setters
    /// bump the prepare generation, so toggling them per batch would re-prepare every statement).
    trunk: Arc<Connection>,
    store: Arc<BranchStore>,
    stmts: HashMap<(i64, u32), TableStmts>,
    /// The foreign-key checks, compiled for one schema version when foreign keys are enforced.
    fks: Option<FkChecks>,
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

/// Run `stmt` to completion and reset it, even when it fails, so a cached statement is reusable.
/// The run's own error wins over a reset error: it is the one that says whether the member or the
/// batch must go.
fn step_to_end(stmt: &mut Statement) -> Result<()> {
    let ran = stmt.run_ignore_rows();
    let reset = stmt.reset();
    ran?;
    reset
}

/// A constraint error while installing refuses the member (`Ok(Some(reason))`); any other error
/// aborts the batch.
fn member_error(err: LimboError) -> Result<Option<String>> {
    match err {
        LimboError::Constraint(msg) | LimboError::ForeignKeyConstraint(msg) => Ok(Some(msg)),
        other => Err(other),
    }
}

/// The savepoint that brackets one batch member's install.
const MEMBER: &str = "r11_merge_member";

impl Merger {
    /// A merger for the trunk `trunk` is connected to, which must be a trunk connection. The
    /// merger writes through a connection of its own (see [`Merger`]'s fields).
    pub fn new(trunk: Arc<Connection>) -> Result<Self> {
        if trunk.branch_id().is_some() {
            return Err(LimboError::InvalidArgument(
                "a merger writes the trunk; this connection is on a branch".to_string(),
            ));
        }
        let store = trunk.db.branches.clone();
        let own = trunk.db.connect()?;
        own.set_foreign_keys_enabled(false);
        own.set_row_image_apply(true);
        Ok(Self {
            caller: trunk,
            trunk: own,
            store,
            stmts: HashMap::new(),
            fks: None,
        })
    }

    /// Give the merger's connection the caller's durability, WAL maintenance and busy-wait
    /// settings, which a merge's commit must honour exactly as the caller's own commit would. Each
    /// is set only when it differs, since a setter makes every cached statement prepare again. A
    /// custom busy callback cannot be shared (it is a `Box`) and reads as no timeout, so under one
    /// the merger's `BEGIN IMMEDIATE` returns Busy at once.
    fn mirror_settings(&self) {
        let mode = self.caller.get_sync_mode();
        if self.trunk.get_sync_mode() != mode {
            self.trunk.set_sync_mode(mode);
        }
        let retry = self.caller.get_data_sync_retry();
        if self.trunk.get_data_sync_retry() != retry {
            self.trunk.set_data_sync_retry(retry);
        }
        let busy = self.caller.get_busy_timeout();
        if self.trunk.get_busy_timeout() != busy {
            self.trunk.set_busy_timeout(busy);
        }
        self.trunk.wal_auto_actions.store(
            self.caller.wal_auto_actions.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
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
    ///
    /// The branch's rows are applied as ROW IMAGES: what the branch's own statements, triggers and
    /// foreign-key actions wrote is already in its row write set, so the merger's connection
    /// compiles with no triggers and no foreign-key actions (PostgreSQL's
    /// `session_replication_role = replica`). Firing them again would run a trigger twice, or run a
    /// trunk-side action on rows only the trunk changed. When the caller's connection enforces
    /// foreign keys, each member is checked after its rows go in, and a violation refuses that
    /// member.
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
        self.mirror_settings();
        let fk_on = self.caller.foreign_keys_enabled();
        let outcomes = self.run_batch(&branches, policy, fk_on)?;
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
                    Some(Refusal::Install) => w.merge_refused_install += 1,
                }
            }
        });
        drop(branches);
        Ok(outcomes)
    }

    fn run_batch(
        &mut self,
        branches: &[Branch],
        policy: MergePolicy,
        fk_on: bool,
    ) -> Result<Vec<MergeOutcome>> {
        // BEGIN IMMEDIATE checks the schema cookie once it holds the write lock and prepares again
        // on a mismatch, so from here to COMMIT the merger's schema is the trunk's: a schema change
        // the caller committed is seen by the scope gate and by the statement cache's key.
        self.trunk.execute("BEGIN IMMEDIATE")?;
        let result = self.validate_and_install(branches, policy, fk_on);
        match &result {
            Ok(outcomes) if outcomes.iter().any(|o| o.refused.is_none()) => {
                if let Err(err) = self.trunk.execute("COMMIT") {
                    let _ = self.trunk.execute("ROLLBACK");
                    return Err(err);
                }
            }
            _ => self.trunk.execute("ROLLBACK")?,
        }
        result
    }

    fn validate_and_install(
        &mut self,
        branches: &[Branch],
        policy: MergePolicy,
        fk_on: bool,
    ) -> Result<Vec<MergeOutcome>> {
        let physical = policy.install == Install::Physical;
        let mut outcomes = Vec::with_capacity(branches.len());
        for branch in branches {
            let prep = self
                .store
                .merge_prepare(branch.id, policy.validation, physical)?;
            let trunk_schema = self.trunk.schema.read().schema_version;
            let scope = prep
                .scope
                .or_else(|| {
                    if super::store::mutant(19) {
                        None
                    } else {
                        row_scope(&prep)
                    }
                })
                .or_else(|| {
                    (prep.schema.schema_version != trunk_schema && !super::store::mutant(20))
                        .then_some("the trunk changed its schema since the fork")
                });
            let mut refused = if scope.is_some() {
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
            let mut install_error = None;
            if refused.is_none() {
                if let Some(reason) = self.install_member(branch, &prep, physical, fk_on)? {
                    refused = Some(Refusal::Install);
                    install_error = Some(reason);
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
                install_error,
            });
        }
        Ok(outcomes)
    }

    /// Install one validated member inside its own SAVEPOINT. `Ok(Some(reason))`: the install
    /// refused, and everything this member wrote is rolled back while the members before it stay.
    fn install_member(
        &mut self,
        branch: &Branch,
        prep: &Prepared,
        physical: bool,
        fk_on: bool,
    ) -> Result<Option<String>> {
        let savepoints = !super::store::mutant(21);
        if savepoints {
            self.trunk.execute(format!("SAVEPOINT {MEMBER}"))?;
        }
        let installed = self.install_checked(branch, prep, physical, fk_on);
        match installed {
            Ok(None) => {
                if savepoints {
                    self.trunk.execute(format!("RELEASE {MEMBER}"))?;
                }
                Ok(None)
            }
            Ok(Some(reason)) if savepoints => {
                self.trunk.execute(format!("ROLLBACK TO {MEMBER}"))?;
                self.trunk.execute(format!("RELEASE {MEMBER}"))?;
                Ok(Some(reason))
            }
            // Mutant 21: without a savepoint a member's refusal can only fail the whole batch.
            Ok(Some(reason)) => Err(LimboError::Constraint(reason)),
            Err(err) => {
                if savepoints {
                    let _ = self.trunk.execute(format!("ROLLBACK TO {MEMBER}"));
                    let _ = self.trunk.execute(format!("RELEASE {MEMBER}"));
                }
                Err(err)
            }
        }
    }

    /// Install one member's rows or pages and, when the caller's connection enforces foreign
    /// keys, check them (mutant 23 skips the whole check).
    fn install_checked(
        &mut self,
        branch: &Branch,
        prep: &Prepared,
        physical: bool,
        fk_on: bool,
    ) -> Result<Option<String>> {
        let fk = fk_on && !super::store::mutant(23);
        let held = if fk {
            self.parent_keys_before(prep)?
        } else {
            Vec::new()
        };
        let installed = if physical {
            self.install_pages(prep).map(|()| None)?
        } else {
            self.install_rows(branch, prep)?
        };
        match installed {
            None if fk => self.foreign_key_violation(prep, &held),
            other => Ok(other),
        }
    }

    fn install_pages(&self, prep: &Prepared) -> Result<()> {
        let pager = self.trunk.pager.load().clone();
        let db_size = pager.trunk_db_size()?;
        for (page, image) in &prep.pages {
            pager.install_page_image(*page, image, db_size)?;
        }
        if !super::store::mutant(6) {
            self.store.trunk_rows_written(&prep.rows);
        }
        self.store
            .merge_counted(|w| w.merge_pages_installed += prep.pages.len() as u64);
        Ok(())
    }

    /// Replay the branch's rows as row images, in the branch's last-write order. `Ok(Some(reason))`
    /// when a trunk constraint refuses a row or a row no longer fits its table's column list.
    fn install_rows(&mut self, branch: &Branch, prep: &Prepared) -> Result<Option<String>> {
        if prep.rows.is_empty() {
            return Ok(None);
        }
        let version = self.trunk.schema.read().schema_version;
        let from = branch.connect()?;
        let mut selects: HashMap<i64, Statement> = HashMap::new();
        let mut tables: HashMap<i64, Arc<BTreeTable>> = HashMap::new();
        for &(root, rowid) in &prep.rows {
            let table = match tables.get(&root) {
                Some(t) => t.clone(),
                None => {
                    let t = table_at(&prep.schema, root).expect("row_scope admitted this table");
                    tables.insert(root, t.clone());
                    t
                }
            };
            let key = (root, if super::store::mutant(18) { 0 } else { version });
            if !self.stmts.contains_key(&key) {
                let stmts = self.prepare_table(&table)?;
                // Statements for an older schema version of this table can never run again.
                self.stmts.retain(|&(r, v), _| r != root || v == key.1);
                self.stmts.insert(key, stmts);
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
            let found = select.run_collect_rows();
            select.reset()?;
            let found = found?;
            let stmts = self.stmts.get_mut(&key).expect("just prepared");
            match found.as_slice() {
                [] => {
                    if !super::store::mutant(17) {
                        stmts
                            .delete
                            .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                        if let Err(err) = step_to_end(&mut stmts.delete) {
                            return member_error(err);
                        }
                    }
                }
                [row] => {
                    if row.len() != stmts.columns {
                        return Ok(Some(format!(
                            "branch {} row {rowid} has {} columns; the trunk statement for {} has {}",
                            branch.id.0,
                            row.len(),
                            table.name,
                            stmts.columns
                        )));
                    }
                    let updated = match stmts.update.as_mut() {
                        Some(update) => {
                            update.bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                            let mut param = 2usize;
                            for (i, v) in row.iter().enumerate() {
                                if stmts.alias == Some(i) {
                                    continue;
                                }
                                update.bind_at(param.try_into().unwrap(), v.clone())?;
                                param += 1;
                            }
                            if let Err(err) = step_to_end(update) {
                                return member_error(err);
                            }
                            self.trunk.changes() > 0
                        }
                        None => false,
                    };
                    if !updated && !super::store::mutant(16) {
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
                        if let Err(err) = step_to_end(&mut stmts.insert) {
                            return member_error(err);
                        }
                    }
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
        drop(tables);
        drop(from);
        self.store
            .merge_counted(|w| w.merge_rows_installed += prep.rows.len() as u64);
        Ok(None)
    }

    /// The foreign-key checks for the trunk's current schema, compiled once per schema version.
    fn fk_checks(&mut self) -> Result<&mut FkChecks> {
        let version = self.trunk.schema.read().schema_version;
        if self.fks.as_ref().map(|f| f.version) != Some(version) {
            let schema = self.trunk.schema.read().clone();
            let mut fks = FkChecks {
                version,
                checks: Vec::new(),
                by_child: HashMap::new(),
                by_parent: HashMap::new(),
            };
            for t in schema.tables.values() {
                let Table::BTree(child) = t.as_ref() else {
                    continue;
                };
                for fk in &child.foreign_keys {
                    let parent = schema.get_btree_table(&fk.parent_table);
                    let i = fks.checks.len();
                    fks.by_child.entry(child.root_page).or_default().push(i);
                    if let Some(p) = &parent {
                        fks.by_parent.entry(p.root_page).or_default().push(i);
                    }
                    let stmts = match &parent {
                        Some(p) => self.prepare_fk(child, p, fk),
                        None => Err(format!(
                            "a foreign key of {} names a table that does not exist: {}",
                            child.name, fk.parent_table
                        )),
                    };
                    fks.checks.push(FkCheck {
                        name: format!("{} referencing {}", child.name, fk.parent_table),
                        stmts,
                    });
                }
            }
            self.fks = Some(fks);
        }
        Ok(self.fks.as_mut().expect("just compiled"))
    }

    /// Before a member's rows go in: the key of every parent row among them that already exists,
    /// which the rows may be about to change or remove. Mutant 25 skips it (the parent side).
    fn parent_keys_before(&mut self, prep: &Prepared) -> Result<Vec<(usize, Vec<Value>)>> {
        let mut held = Vec::new();
        if super::store::mutant(25) {
            return Ok(held);
        }
        let fks = self.fk_checks()?;
        for &(root, rowid) in &prep.rows {
            let Some(ids) = fks.by_parent.get(&root) else {
                continue;
            };
            for &i in ids {
                // An uncheckable key refuses the member after its install, below.
                let Ok(stmts) = fks.checks[i].stmts.as_mut() else {
                    continue;
                };
                let Some(key) = stmts.key.as_mut() else {
                    continue;
                };
                key.bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                let rows = key.run_collect_rows();
                key.reset()?;
                if let Some(row) = rows?.into_iter().next() {
                    if !row.iter().any(|v| matches!(v, Value::Null)) {
                        held.push((i, row));
                    }
                }
            }
        }
        Ok(held)
    }

    /// After a member's rows went in with no FK action and no FK check: the foreign keys the
    /// caller's connection enforces, over this member's rows only, so the cost is O(write set)
    /// probes rather than a scan of every table it touched. A child row the member wrote whose key
    /// is wholly non-NULL must find a parent (mutant 26 skips this side), and a parent key one of
    /// its rows held before the install must be held by no child that lost its parent. The trunk
    /// satisfied its constraints before the member, so any violation now is the member's.
    ///
    /// A child row that was already an orphan (written while foreign keys were off) refuses every
    /// member that writes it and leaves it an orphan, as SQLite refuses such a write. Values are
    /// matched as `parent.key = child.key`, so the parent column's collation applies (as in
    /// SQLite); where the two columns' affinities differ, SQL comparison rules decide a match,
    /// which can differ from SQLite's conversion to the parent's affinity. That the parent key is
    /// a PRIMARY KEY or UNIQUE, which SQLite requires, is not checked.
    fn foreign_key_violation(
        &mut self,
        prep: &Prepared,
        held: &[(usize, Vec<Value>)],
    ) -> Result<Option<String>> {
        let fks = self.fk_checks()?;
        for &(root, rowid) in &prep.rows {
            let parents = fks.by_parent.get(&root).into_iter().flatten();
            for &i in fks.by_child.get(&root).into_iter().flatten().chain(parents) {
                if let Err(reason) = &fks.checks[i].stmts {
                    return Ok(Some(reason.clone()));
                }
            }
            if super::store::mutant(26) {
                continue;
            }
            for &i in fks.by_child.get(&root).into_iter().flatten() {
                let check = &mut fks.checks[i];
                let Ok(stmts) = check.stmts.as_mut() else {
                    continue;
                };
                let Some(child) = stmts.child.as_mut() else {
                    continue;
                };
                child.bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                let rows = child.run_collect_rows();
                child.reset()?;
                if !rows?.is_empty() {
                    return Ok(Some(format!(
                        "foreign key {}: row {rowid} has no parent after the merge",
                        check.name
                    )));
                }
            }
        }
        for (i, key) in held {
            let check = &mut fks.checks[*i];
            let Ok(stmts) = check.stmts.as_mut() else {
                continue;
            };
            for (param, v) in key.iter().enumerate() {
                stmts
                    .parent
                    .bind_at((param + 1).try_into().unwrap(), v.clone())?;
            }
            let rows = stmts.parent.run_collect_rows();
            stmts.parent.reset()?;
            if !rows?.is_empty() {
                return Ok(Some(format!(
                    "foreign key {}: a child still holds a key the merge removed from its parent",
                    check.name
                )));
            }
        }
        Ok(None)
    }

    /// The statements that check one foreign key, or why it cannot be checked. A statement that
    /// does not prepare (a parent column that does not exist, say) makes the key uncheckable: it
    /// depends on the schema alone, so it refuses the members that touch the key rather than every
    /// batch.
    fn prepare_fk(
        &self,
        child: &BTreeTable,
        parent: &BTreeTable,
        fk: &ForeignKey,
    ) -> std::result::Result<FkStmts, String> {
        let parent_cols: Vec<String> = if !fk.parent_columns.is_empty() {
            fk.parent_columns.iter().map(|c| quote(c)).collect()
        } else {
            // No parent columns means the parent's PRIMARY KEY; with none declared SQLite reports a
            // foreign key mismatch, and so does this check (the counts differ).
            parent
                .primary_key_columns
                .iter()
                .map(|(c, _)| quote(c))
                .collect()
        };
        if parent_cols.len() != fk.child_columns.len() {
            return Err(format!(
                "foreign key mismatch: {} referencing {}",
                child.name, parent.name
            ));
        }
        let prepare = |sql: String| {
            self.trunk.prepare(sql).map_err(|err| {
                format!(
                    "foreign key {} referencing {} cannot be checked: {err}",
                    child.name, parent.name
                )
            })
        };
        let child_cols: Vec<String> = fk.child_columns.iter().map(|c| quote(c)).collect();
        let (c, p) = (quote(&child.name), quote(&parent.name));
        let matches: Vec<String> = child_cols
            .iter()
            .zip(&parent_cols)
            .map(|(cc, pc)| format!("p.{pc} = c.{cc}"))
            .collect();
        let orphan = format!(
            "NOT EXISTS (SELECT 1 FROM {p} AS p WHERE {})",
            matches.join(" AND ")
        );
        let not_null: Vec<String> = child_cols
            .iter()
            .map(|cc| format!("c.{cc} IS NOT NULL"))
            .collect();
        let holds: Vec<String> = child_cols
            .iter()
            .enumerate()
            .map(|(i, cc)| format!("c.{cc} = ?{}", i + 1))
            .collect();
        // A table without rowids is never a member's (row_scope), so it needs no rowid statement.
        let key = parent
            .has_rowid
            .then(|| {
                prepare(format!(
                    "SELECT {} FROM {p} WHERE rowid = ?1",
                    parent_cols.join(", ")
                ))
            })
            .transpose()?;
        let child_stmt = child
            .has_rowid
            .then(|| {
                prepare(format!(
                    "SELECT 1 FROM {c} AS c WHERE c.rowid = ?1 AND {} AND {orphan}",
                    not_null.join(" AND ")
                ))
            })
            .transpose()?;
        let parent_stmt = prepare(format!(
            "SELECT 1 FROM {c} AS c WHERE {} AND {orphan} LIMIT 1",
            holds.join(" AND ")
        ))?;
        Ok(FkStmts {
            key,
            child: child_stmt,
            parent: parent_stmt,
        })
    }

    fn prepare_table(&self, table: &BTreeTable) -> Result<TableStmts> {
        let names: Vec<String> = table
            .columns()
            .iter()
            .map(|c| quote(c.name.as_deref().unwrap_or("")))
            .collect();
        let alias = table.get_rowid_alias_column().map(|(i, _)| i);
        let t = quote(&table.name);
        // The rowid alias stays out of the SET list: the row keeps its rowid, and assigning the alias
        // makes the engine run a two-pass update through an ephemeral table, a temporary file per
        // statement, which was 82% of the replay's time (frontier/round11/r11-merge A10).
        // OR ABORT overrides a constraint-level ON CONFLICT REPLACE (the statement's clause wins, as
        // in SQLite), so installing a row can never delete a row only the trunk has: a collision
        // is an error, and the member is refused.
        let sets: Vec<String> = names
            .iter()
            .enumerate()
            .filter(|(i, _)| alias != Some(*i))
            .enumerate()
            .map(|(param, (_, n))| format!("{n} = ?{}", param + 2))
            .collect();
        let update = (!sets.is_empty())
            .then(|| format!("UPDATE OR ABORT {t} SET {} WHERE rowid = ?1", sets.join(", ")));
        let insert = match alias {
            Some(_) if names.len() == 1 => {
                // The alias is the only column: the row is its rowid, and an existing one is kept.
                format!("INSERT OR IGNORE INTO {t}({}) VALUES (?1)", names[0])
            }
            Some(_) => {
                let params: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT OR ABORT INTO {t}({}) VALUES ({})",
                    names.join(", "),
                    params.join(", ")
                )
            }
            None => {
                let params: Vec<String> = (2..=names.len() + 1).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT OR ABORT INTO {t}(rowid, {}) VALUES (?1, {})",
                    names.join(", "),
                    params.join(", ")
                )
            }
        };
        let delete = format!("DELETE FROM {t} WHERE rowid = ?1");
        Ok(TableStmts {
            update: update.map(|sql| self.trunk.prepare(sql)).transpose()?,
            insert: self.trunk.prepare(insert)?,
            delete: self.trunk.prepare(delete)?,
            columns: names.len(),
            alias,
        })
    }
}
