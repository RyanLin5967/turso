//! Merging a branch back into the trunk.
//!
//! # What a merge is here
//!
//! A branch B forked from the trunk at trunk epoch `trunk_at` has written some pages and some rows.
//! Merging it makes B's changes the trunk's, inside ONE trunk write transaction (`BEGIN IMMEDIATE`
//! … `COMMIT`), so that validation and install both run under the trunk's WAL write lock: no trunk
//! commit can fall between deciding and writing. A fork can, where forks take no WAL lock; the
//! merge's writes are stamped at its commit's decision like any trunk commit's, so a child forked
//! meanwhile is refused on them (frontier/round11/r11-merge PREREG A15). The
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
//!   foreign-key actions (PostgreSQL's `session_replication_role = replica`), through a
//!   connection of the merger's own that carries the caller's commit settings; when the caller's
//!   connection enforces foreign keys they are checked after each member instead, over the
//!   member's own rows and the children of the parent keys it changed. Statements say OR ABORT,
//!   so a constraint-level REPLACE can never delete a row only the trunk has; rows go in B's
//!   last-write order, so a UNIQUE value B moved leaves its old row before it reaches the new one.
//!
//! [`Merger::merge_batch`] validates and installs several branches in one trunk transaction (group
//! commit); each member is validated after the members before it installed, which only the stamp
//! validators can see, so a batch requires one of them. Each member installs inside its own
//! SAVEPOINT: a constraint that refuses one member ([`Refusal::Install`]) rolls back only that
//! member, never the batch.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use super::store::BranchStore;
use super::{Branch, BranchId};
use crate::schema::{BTreeTable, ResolvedFkRef, Schema, Table};
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

/// What [`BranchStore::merge_prepare`] reads for a merge (its shard's, the trunk's and the stamps'
/// holds, one at a time).
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

/// A foreign key a table declares, as the per-member check runs it on that table's rows.
struct ChildFk {
    /// "child referencing parent", for the refusal's reason.
    name: String,
    /// `?1` = a child rowid: one row back when that row's key is wholly non-NULL and no parent row
    /// holds it (see [`Merger::orphan_sql`]).
    orphan: Statement,
}

/// A foreign key that references a table, as the per-member check runs it on that table's rows.
struct ParentFk {
    name: String,
    /// The same statement as [`ChildFk::orphan`], for the children this key's parent row had.
    orphan: Statement,
    /// `?1` = a parent rowid: that row's key. Also prepared on the branch, per member.
    key_sql: String,
    key: Statement,
    /// `?1` = a parent rowid: the rowids of the children whose key that row holds.
    children: Statement,
}

/// The foreign-key checks of one trunk schema version, per table root, compiled the first time a
/// member writes the table. `Err(reason)`: the engine cannot resolve the table's foreign keys
/// (`Schema::resolved_fks_for_child` / `resolved_fks_referencing`: a missing parent, a column
/// count that differs, a parent key without a UNIQUE index), so with foreign keys on it refuses
/// every write to the table, and a member that writes it is refused.
struct FkChecks {
    version: u32,
    child: HashMap<i64, std::result::Result<Vec<ChildFk>, String>>,
    parent: HashMap<i64, std::result::Result<Vec<ParentFk>, String>>,
}

/// Merges branches into the trunk, keeping the trunk statements the replay install prepares, per
/// table root AND trunk schema version: a statement prepared for an older column list must never
/// write a row of a newer one.
pub struct Merger {
    /// The caller's trunk connection. Read at every batch for its settings (see
    /// [`Merger::mirror_settings`]); never written through.
    caller: Arc<Connection>,
    /// The merger's OWN trunk connection, which compiles for row images for its whole life: no
    /// triggers and no foreign-key actions (see the module doc). Its own connection, so no
    /// statement of the caller's ever compiles that way, a panic cannot leave the caller's
    /// connection in that state, and the merger's cached statements are not prepared again at
    /// every batch (the setters bump the prepare generation, so toggling them per batch would
    /// re-prepare every statement).
    trunk: Arc<Connection>,
    store: Arc<BranchStore>,
    stmts: HashMap<(i64, u32), TableStmts>,
    /// The foreign-key checks, compiled for one schema version when foreign keys are enforced.
    fks: Option<FkChecks>,
    /// Test-only: run between the pre-probes and BEGIN IMMEDIATE (A19's re-check test).
    #[cfg(test)]
    after_preprobe: Option<Box<dyn FnMut()>>,
}

impl Drop for Merger {
    /// Close the merger's own connection, statements first, so that when it is the database's last
    /// connection it shuts down as any other would.
    fn drop(&mut self) {
        self.stmts.clear();
        self.fks = None;
        let _ = self.trunk.close();
    }
}

/// "child referencing parent", for a refusal's reason.
fn fk_name(r: &ResolvedFkRef) -> String {
    format!("{} referencing {}", r.child_table.name, r.fk.parent_table)
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
    // Rows are in write order, so tables interleave: look each one up once.
    let mut seen = HashSet::new();
    for &(root, _) in &prep.rows {
        if !seen.insert(root) {
            continue;
        }
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
/// aborts the batch. Mutant 28 aborts the batch on a constraint error too.
fn member_error(err: LimboError) -> Result<Option<String>> {
    match err {
        LimboError::Constraint(msg) | LimboError::ForeignKeyConstraint(msg)
            if !super::store::mutant(28) =>
        {
            Ok(Some(msg))
        }
        other => Err(other),
    }
}

/// Run `stmt` with `?1 = rowid` and return its first row, leaving it reset for the next use.
fn first_row(stmt: &mut Statement, rowid: i64) -> Result<Option<Vec<Value>>> {
    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
    let rows = stmt.run_collect_rows();
    let reset = stmt.reset();
    let rows = rows?;
    reset?;
    Ok(rows.into_iter().next())
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
            #[cfg(test)]
            after_preprobe: None,
        })
    }

    /// Give the merger's connection the caller's settings that decide how a merge writes and
    /// commits, so it commits exactly as the caller's own statement would: synchronous, fullfsync,
    /// data-sync retry, WAL auto-actions (checkpoint), busy timeout, change capture (CDC),
    /// query_only and ignore_check_constraints. Each is set only when it differs, since some
    /// setters make every cached statement prepare again. This is an allowlist: a setting added
    /// later is NOT carried until it is added here. Known gaps: a custom busy callback cannot be
    /// shared (it is a `Box`) and reads as no timeout, so under one the merger's `BEGIN IMMEDIATE`
    /// returns Busy at once; foreign_keys is read, not copied (the merger's connection keeps it off
    /// and checks keys itself). Mutant 29 copies nothing.
    fn mirror_settings(&self) {
        if super::store::mutant(29) {
            return;
        }
        let fsync = self.caller.get_sync_type();
        if self.trunk.get_sync_type() != fsync {
            self.trunk.set_sync_type(fsync);
        }
        let query_only = self.caller.get_query_only();
        if self.trunk.get_query_only() != query_only {
            self.trunk.set_query_only(query_only);
        }
        let checks = self.caller.check_constraints_ignored();
        if self.trunk.check_constraints_ignored() != checks {
            self.trunk.set_check_constraints_ignored(checks);
        }
        let cdc = self.caller.get_capture_data_changes_info().clone();
        let same_cdc = *self.trunk.get_capture_data_changes_info() == cdc;
        if !same_cdc {
            self.trunk.set_capture_data_changes_info(cdc);
        }
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
        if !self.caller.get_auto_commit() {
            // The merge commits on its own connection; with the caller inside a transaction it
            // would wait on the caller's own write lock, or read around its uncommitted writes.
            return Err(LimboError::InvalidArgument(
                "the caller's connection has a transaction open; a merge commits its own"
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
        // A19 (1): V3's probes before the WAL write lock (Silo/OCC backward validation); each is
        // re-checked in O(1) under it and replaced by an in-lock probe if anything committed or
        // stamped since. A branch the store no longer has gets no pre-probe (merge_prepare reports).
        let pres: Vec<Option<super::store::PreProbe>> = branches
            .iter()
            .map(|b| self.store.v3_preprobe(b.id).ok())
            .collect();
        #[cfg(test)]
        if let Some(hook) = self.after_preprobe.as_mut() {
            hook();
        }
        // BEGIN IMMEDIATE checks the schema cookie once it holds the write lock and prepares again
        // on a mismatch, so from here to COMMIT the merger's schema is the trunk's: a schema change
        // the caller committed is seen by the scope gate and by the statement cache's key.
        self.trunk.execute("BEGIN IMMEDIATE")?;
        let result = self.validate_and_install(branches, policy, fk_on, &pres);
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
        pres: &[Option<super::store::PreProbe>],
    ) -> Result<Vec<MergeOutcome>> {
        let physical = policy.install == Install::Physical;
        let mut outcomes = Vec::with_capacity(branches.len());
        for (branch, &pre) in branches.iter().zip(pres) {
            // The batch's earlier members' writes are pending in this connection's transaction.
            let pager = self.trunk.pager.load().clone();
            crate::turso_assert!(
                pager.holds_write_lock(),
                "a merge validates only inside its trunk write transaction: the WAL write lock is \
                 what orders it after every commit's post-gate row stamps (PREREG A18)"
            );
            let prep = pager.with_trunk_pending(|batch| {
                self.store
                    .merge_prepare(branch.id, policy.validation, physical, batch, pre)
            })?;
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
                // Counted for members that stay, never for one rolled back to its savepoint.
                self.store.merge_counted(|w| {
                    if physical {
                        w.merge_pages_installed += prep.pages.len() as u64;
                    } else {
                        w.merge_rows_installed += prep.rows.len() as u64;
                    }
                });
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
        if prep.rows.is_empty() && !physical {
            return Ok(None);
        }
        let fk = fk_on && !super::store::mutant(23);
        let from = if fk || !physical {
            Some(branch.connect()?)
        } else {
            None
        };
        let held = match &from {
            Some(from) if fk => self.children_before(prep, from)?,
            _ => Vec::new(),
        };
        let installed = match &from {
            Some(from) if !physical => self.install_rows(branch, from, prep)?,
            _ => self.install_pages(prep).map(|()| None)?,
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
            pager.note_rows_installed(&prep.rows);
        }
        Ok(())
    }

    /// Replay the branch's rows as row images, in the branch's last-write order. `Ok(Some(reason))`
    /// when a trunk constraint refuses a row or a row no longer fits its table's column list.
    fn install_rows(
        &mut self,
        branch: &Branch,
        from: &Arc<Connection>,
        prep: &Prepared,
    ) -> Result<Option<String>> {
        let version = self.trunk.schema.read().schema_version;
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
        Ok(None)
    }

    /// The foreign-key checks for `root`'s table under the trunk's current schema.
    fn fk_sides(&mut self, root: i64) -> &mut FkChecks {
        let version = self.trunk.schema.read().schema_version;
        if self.fks.as_ref().map(|f| f.version) != Some(version) {
            self.fks = Some(FkChecks {
                version,
                child: HashMap::new(),
                parent: HashMap::new(),
            });
        }
        if !self.fks.as_ref().expect("set above").child.contains_key(&root) {
            let schema = self.trunk.schema.read().clone();
            // row_scope admits a member's rows only from tables `table_at` finds.
            let (child, parent) = match table_at(&schema, root) {
                Some(table) => (
                    self.child_fks(&schema, &table),
                    self.parent_fks(&schema, &table),
                ),
                None => (Ok(Vec::new()), Ok(Vec::new())),
            };
            let fks = self.fks.as_mut().expect("set above");
            fks.child.insert(root, child);
            fks.parent.insert(root, parent);
        }
        self.fks.as_mut().expect("set above")
    }

    fn child_fks(
        &self,
        schema: &Schema,
        table: &BTreeTable,
    ) -> std::result::Result<Vec<ChildFk>, String> {
        let refs = schema
            .resolved_fks_for_child(&table.name)
            .map_err(|err| err.to_string())?;
        let mut out = Vec::with_capacity(refs.len());
        for r in &refs {
            out.push(ChildFk {
                name: fk_name(r),
                orphan: self.prepare_fk(r, Self::orphan_sql(schema, r)?)?,
            });
        }
        Ok(out)
    }

    fn parent_fks(
        &self,
        schema: &Schema,
        table: &BTreeTable,
    ) -> std::result::Result<Vec<ParentFk>, String> {
        let refs = schema
            .resolved_fks_referencing(&table.name)
            .map_err(|err| err.to_string())?;
        let mut out = Vec::with_capacity(refs.len());
        for r in &refs {
            if !r.child_table.has_rowid {
                // Its children cannot be named by rowid; a merge never writes such a table, and
                // refuses to change the keys it references rather than leave them unchecked.
                return Err(format!(
                    "foreign key {}: the child has no rowids, which a merge does not check",
                    fk_name(r)
                ));
            }
            let parent_cols: Vec<String> = r.parent_cols.iter().map(|c| quote(c)).collect();
            let key_sql = format!(
                "SELECT {} FROM {} WHERE rowid = ?1",
                parent_cols.join(", "),
                quote(&table.name)
            );
            out.push(ParentFk {
                name: fk_name(r),
                orphan: self.prepare_fk(r, Self::orphan_sql(schema, r)?)?,
                key: self.prepare_fk(r, key_sql.clone())?,
                key_sql,
                children: self.prepare_fk(r, Self::children_sql(table, r))?,
            });
        }
        Ok(out)
    }

    fn prepare_fk(&self, r: &ResolvedFkRef, sql: String) -> std::result::Result<Statement, String> {
        self.trunk
            .prepare(sql)
            .map_err(|err| format!("foreign key {} cannot be checked: {err}", fk_name(r)))
    }

    /// One row of the child `c` whose key is wholly non-NULL and held by no parent row. The match
    /// is SQLite's for a foreign key: `p.key = +c.key` applies the parent column's affinity to
    /// the child's value (the unary `+` strips the child column's), under the parent column's
    /// collation (the left operand's). The parent lookup can still use the parent's key index.
    fn orphan_sql(schema: &Schema, r: &ResolvedFkRef) -> std::result::Result<String, String> {
        let parent = schema
            .get_btree_table(&r.fk.parent_table)
            .ok_or_else(|| format!("foreign key {}: no parent table", fk_name(r)))?;
        let pairs = r.fk.child_columns.iter().zip(r.parent_cols.iter());
        let matches: Vec<String> = pairs
            .map(|(cc, pc)| format!("p.{} = +c.{}", quote(pc), quote(cc)))
            .collect();
        let not_null: Vec<String> = r
            .fk
            .child_columns
            .iter()
            .map(|cc| format!("c.{} IS NOT NULL", quote(cc)))
            .collect();
        Ok(format!(
            "SELECT 1 FROM {} AS c WHERE c.rowid = ?1 AND {} AND NOT EXISTS \
             (SELECT 1 FROM {} AS p WHERE {})",
            quote(&r.child_table.name),
            not_null.join(" AND "),
            quote(&parent.name),
            matches.join(" AND ")
        ))
    }

    /// The children of parent row `?1`. A column pair whose affinity and collation agree is
    /// matched as `c.key = p.key`, which a child key index can serve and which is then the same
    /// comparison as SQLite's; otherwise as in [`Merger::orphan_sql`], which scans the child, as
    /// SQLite's own check does when the child key has no usable index.
    fn children_sql(parent: &BTreeTable, r: &ResolvedFkRef) -> String {
        let child = &r.child_table;
        let matches: Vec<String> = r
            .fk
            .child_columns
            .iter()
            .zip(r.parent_cols.iter())
            .map(|(cc, pc)| {
                let same = match (child.get_column(cc), parent.get_column(pc)) {
                    (Some((_, c)), Some((_, p))) => {
                        c.affinity_with_strict(child.is_strict)
                            == p.affinity_with_strict(parent.is_strict)
                            && c.collation() == p.collation()
                    }
                    _ => false,
                };
                if same {
                    format!("c.{} = p.{}", quote(cc), quote(pc))
                } else {
                    format!("p.{} = +c.{}", quote(pc), quote(cc))
                }
            })
            .collect();
        format!(
            "SELECT c.rowid FROM {} AS p JOIN {} AS c ON {} WHERE p.rowid = ?1",
            quote(&parent.name),
            quote(&child.name),
            matches.join(" AND ")
        )
    }

    /// Before a member's rows go in: for each parent row among them whose key the branch changed
    /// or removed, the children that hold the trunk's key now, as (parent root, key, child rowid).
    /// A key the branch kept cannot orphan anything, so its children are not listed; the cost is
    /// two key probes per written parent row, plus the children of the keys that change. Mutant 25
    /// lists nothing (the parent side).
    fn children_before(
        &mut self,
        prep: &Prepared,
        from: &Arc<Connection>,
    ) -> Result<Vec<(i64, usize, i64)>> {
        let mut held = Vec::new();
        if super::store::mutant(25) {
            return Ok(held);
        }
        let mut theirs: HashMap<(i64, usize), Statement> = HashMap::new();
        for &(root, rowid) in &prep.rows {
            let fks = self.fk_sides(root);
            // An unresolved key refuses the member after its install.
            let Some(Ok(sides)) = fks.parent.get_mut(&root) else {
                continue;
            };
            for (i, side) in sides.iter_mut().enumerate() {
                let Some(ours) = first_row(&mut side.key, rowid)? else {
                    continue;
                };
                if ours.iter().any(|v| matches!(v, Value::Null)) {
                    continue;
                }
                let stmt = match theirs.entry((root, i)) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => e.insert(from.prepare(&side.key_sql)?),
                };
                if first_row(stmt, rowid)?.as_ref() == Some(&ours) {
                    continue;
                }
                side.children
                    .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                let rows = side.children.run_collect_rows();
                let reset = side.children.reset();
                let rows = rows?;
                reset?;
                for row in rows {
                    if let Some(child) = row.first().and_then(|v| v.as_int()) {
                        held.push((root, i, child));
                    }
                }
            }
        }
        Ok(held)
    }

    /// After a member's rows went in with no FK action and no FK check: the foreign keys the
    /// caller's connection enforces, over this member's rows only. A child row the member wrote
    /// must find its parent (mutant 26 skips this side), and each child listed by
    /// [`Merger::children_before`] must still find one. The trunk satisfied its constraints before
    /// the member, so a violation now is the member's. The probes are O(write set) where the keys
    /// are indexed (see [`Merger::children_sql`] for where they are not).
    ///
    /// Stricter than SQLite in one place: a child row that was already an orphan (written while
    /// foreign keys were off) refuses a member that rewrites it, even with its key unchanged, where
    /// SQLite checks an UPDATE only when it sets a key column. A refusal is always safe here.
    fn foreign_key_violation(
        &mut self,
        prep: &Prepared,
        held: &[(i64, usize, i64)],
    ) -> Result<Option<String>> {
        for &(root, rowid) in &prep.rows {
            let fks = self.fk_sides(root);
            if let Some(Err(reason)) = fks.child.get(&root) {
                return Ok(Some(reason.clone()));
            }
            if let Some(Err(reason)) = fks.parent.get(&root) {
                return Ok(Some(reason.clone()));
            }
            if super::store::mutant(26) {
                continue;
            }
            let Some(Ok(sides)) = fks.child.get_mut(&root) else {
                continue;
            };
            for side in sides.iter_mut() {
                if first_row(&mut side.orphan, rowid)?.is_some() {
                    return Ok(Some(format!(
                        "foreign key {}: row {rowid} has no parent after the merge",
                        side.name
                    )));
                }
            }
        }
        for &(root, i, child) in held {
            let fks = self.fk_sides(root);
            let Some(Ok(sides)) = fks.parent.get_mut(&root) else {
                continue;
            };
            let side = &mut sides[i];
            if first_row(&mut side.orphan, child)?.is_some() {
                return Ok(Some(format!(
                    "foreign key {}: child row {child} lost its parent in the merge",
                    side.name
                )));
            }
        }
        Ok(None)
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
        // Mutant 27: no OR ABORT, so a constraint's own ON CONFLICT clause applies.
        let or_abort = if super::store::mutant(27) {
            ""
        } else {
            " OR ABORT"
        };
        let update = (!sets.is_empty())
            .then(|| format!("UPDATE{or_abort} {t} SET {} WHERE rowid = ?1", sets.join(", ")));
        let insert = match alias {
            Some(_) if names.len() == 1 => {
                // The alias is the only column: the row is its rowid, and an existing one is kept.
                format!("INSERT OR IGNORE INTO {t}({}) VALUES (?1)", names[0])
            }
            Some(_) => {
                let params: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT{or_abort} INTO {t}({}) VALUES ({})",
                    names.join(", "),
                    params.join(", ")
                )
            }
            None => {
                let params: Vec<String> = (2..=names.len() + 1).map(|i| format!("?{i}")).collect();
                format!(
                    "INSERT{or_abort} INTO {t}(rowid, {}) VALUES (?1, {})",
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

/// The merger's own connection: the settings it takes from the caller, the caller's transaction,
/// and a foreign key the engine cannot resolve. (The install tests are in `merge_tests.rs`, which
/// also compiles against the base, so none of these can live there.)
#[cfg(all(test, feature = "fs"))]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::io::FileSyncType;
    use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

    fn open_db() -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("merge.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        (dir, db)
    }

    fn key_replay() -> MergePolicy {
        MergePolicy {
            validation: Validation::KeyStamp,
            install: Install::Replay,
        }
    }

    fn int(conn: &Arc<Connection>, sql: &str) -> i64 {
        conn.prepare(sql).unwrap().run_collect_rows().unwrap()[0][0]
            .as_int()
            .unwrap()
    }

    /// One row, and a branch that updates it.
    fn one_row_and_a_branch(trunk: &Arc<Connection>) -> Branch {
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        trunk.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let b = trunk.fork_branch().unwrap();
        b.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'b' WHERE id = 1")
            .unwrap();
        b
    }

    /// After a batch, every setting `mirror_settings` names is the caller's on the merger's
    /// connection, and the merged row reaches the caller's change feed.
    #[test]
    fn the_merger_connection_takes_the_callers_settings() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();
        let b = one_row_and_a_branch(&trunk);
        let mut merger = Merger::new(trunk.clone()).unwrap();
        trunk.execute("PRAGMA synchronous = OFF").unwrap();
        trunk.set_sync_type(FileSyncType::FullFsync);
        trunk.set_data_sync_retry(!trunk.get_data_sync_retry());
        trunk.set_busy_timeout(Duration::from_millis(1234));
        trunk.set_check_constraints_ignored(true);
        trunk.wal_auto_actions_disable();
        let fresh = db.connect().unwrap();
        // Premise: each setting differs from a new connection's, so equality below is a copy.
        assert_ne!(trunk.get_sync_mode(), fresh.get_sync_mode());
        assert_ne!(trunk.get_sync_type(), fresh.get_sync_type());
        assert_ne!(trunk.get_data_sync_retry(), fresh.get_data_sync_retry());
        assert_ne!(trunk.get_busy_timeout(), fresh.get_busy_timeout());
        assert_ne!(
            trunk.check_constraints_ignored(),
            fresh.check_constraints_ignored()
        );
        assert_ne!(
            trunk.wal_auto_actions.load(Ordering::SeqCst),
            fresh.wal_auto_actions.load(Ordering::SeqCst)
        );
        assert_ne!(
            *trunk.get_capture_data_changes_info(),
            *fresh.get_capture_data_changes_info()
        );
        drop(fresh);
        let captured = int(&trunk, "SELECT count(*) FROM turso_cdc");
        let o = merger.merge(b, key_replay()).unwrap();
        assert_eq!(o.refused, None, "{o:?}");
        let (c, m) = (&merger.caller, &merger.trunk);
        assert_eq!(m.get_sync_mode(), c.get_sync_mode());
        assert_eq!(m.get_sync_type(), c.get_sync_type());
        assert_eq!(m.get_data_sync_retry(), c.get_data_sync_retry());
        assert_eq!(m.get_busy_timeout(), c.get_busy_timeout());
        assert_eq!(m.check_constraints_ignored(), c.check_constraints_ignored());
        assert_eq!(
            m.wal_auto_actions.load(Ordering::SeqCst),
            c.wal_auto_actions.load(Ordering::SeqCst)
        );
        assert_eq!(
            *m.get_capture_data_changes_info(),
            *c.get_capture_data_changes_info()
        );
        assert!(
            int(&trunk, "SELECT count(*) FROM turso_cdc") > captured,
            "the merged row did not reach the caller's change feed"
        );
    }

    /// A caller that may not write cannot merge: the merge fails and the trunk keeps its row.
    #[test]
    fn a_query_only_caller_cannot_merge() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        let b = one_row_and_a_branch(&trunk);
        let mut merger = Merger::new(trunk.clone()).unwrap();
        trunk.set_query_only(true);
        let r = merger.merge(b, key_replay());
        assert!(r.is_err(), "{r:?}");
        trunk.set_query_only(false);
        assert_eq!(int(&trunk, "SELECT count(*) FROM t WHERE v = 'a'"), 1);
    }

    /// Inside the caller's own transaction a merge is refused up front: it would commit on
    /// another connection, around the caller's uncommitted writes.
    #[test]
    fn a_merge_inside_the_callers_transaction_is_refused() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        let b = one_row_and_a_branch(&trunk);
        let mut merger = Merger::new(trunk.clone()).unwrap();
        trunk.execute("BEGIN").unwrap();
        trunk.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
        let r = merger.merge(b, key_replay());
        assert!(matches!(r, Err(LimboError::InvalidArgument(_))), "{r:?}");
        trunk.execute("ROLLBACK").unwrap();
        assert_eq!(int(&trunk, "SELECT count(*) FROM t WHERE v = 'a'"), 1);
    }

    /// The physical install's guard reads the pages a branch read, so a branch forked before read
    /// tracking was on is refused as out of scope; one forked after merges.
    #[test]
    fn a_branch_forked_before_read_tracking_cannot_merge_physically() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        let b = one_row_and_a_branch(&trunk);
        db.set_branch_read_tracking(true);
        let physical = MergePolicy {
            validation: Validation::PageStamp,
            install: Install::Physical,
        };
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge(b, physical).unwrap();
        assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
        assert!(o.scope.is_some_and(|s| s.contains("tracked")), "{o:?}");
        let c = trunk.fork_branch().unwrap();
        c.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'c' WHERE id = 1")
            .unwrap();
        let o = merger.merge(c, physical).unwrap();
        assert_eq!(o.refused, None, "{o:?}");
        assert_eq!(int(&trunk, "SELECT count(*) FROM t WHERE v = 'c'"), 1);
    }

    /// Tracking turned off and on again after a fork leaves a gap in that branch's reads, so its
    /// physical merge is refused as out of scope, although tracking is on at the merge.
    #[test]
    fn a_gap_in_read_tracking_refuses_the_physical_merge() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        db.set_branch_read_tracking(true);
        let b = one_row_and_a_branch(&trunk);
        db.set_branch_read_tracking(false);
        db.set_branch_read_tracking(true);
        let physical = MergePolicy {
            validation: Validation::PageStamp,
            install: Install::Physical,
        };
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge(b, physical).unwrap();
        assert_eq!(o.refused, Some(Refusal::Scope), "{o:?}");
        assert!(o.scope.is_some_and(|s| s.contains("tracked")), "{o:?}");
    }

    /// A physical batch: the first member's inserts split a leaf, which writes the table's root, an
    /// interior page; the second member only read that root on its way to another leaf. The
    /// second must be refused as structural: the root it navigated is not the one the batch now
    /// holds. Where page stamps wait for the commit (r11-merge-fl), only the kind the pager
    /// recorded at the root's first write in the batch says so (mutant 31 ignores it).
    #[test]
    fn a_batch_member_that_read_a_root_an_earlier_member_split_is_refused() {
        let (_dir, db) = open_db();
        db.set_branch_read_tracking(true);
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        trunk.execute("BEGIN").unwrap();
        for id in 1..=400 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({}, '{}')", id * 100, "x".repeat(100)))
                .unwrap();
        }
        trunk.execute("COMMIT").unwrap();
        let (splits, reads) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
        let conn = splits.connect().unwrap();
        conn.execute("BEGIN").unwrap();
        for id in 1..=99 {
            conn.execute(format!("INSERT INTO t VALUES ({id}, '{}')", "y".repeat(100)))
                .unwrap();
        }
        conn.execute("COMMIT").unwrap();
        drop(conn);
        reads
            .connect()
            .unwrap()
            .execute("UPDATE t SET v = 'r' WHERE id = 39900")
            .unwrap();
        let physical = MergePolicy {
            validation: Validation::PageStamp,
            install: Install::Physical,
        };
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge_batch(vec![splits, reads], physical).unwrap();
        assert_eq!(o[0].refused, None, "{o:?}");
        assert!(!o[1].page_conflict, "premise: the two wrote no page in common: {o:?}");
        assert_eq!(o[1].structural_conflict, Some(true), "{o:?}");
        assert_eq!(o[1].refused, Some(Refusal::Structural), "{o:?}");
    }

    /// U14 (PREREG A18): a merge commit's trunk-lock hold stamps no row, whatever the merge's row
    /// count; its rows are all stamped after the commit gate closes. A counter test: the hold's
    /// row count is 0 at k = 1 and at k = 300, and the post-gate count is k.
    #[test]
    fn a_merge_commit_stamps_its_rows_outside_the_trunk_lock_hold() {
        for k in [1i64, 300] {
            let (_dir, db) = open_db();
            let trunk = db.connect().unwrap();
            trunk
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            trunk.execute("BEGIN").unwrap();
            for id in 1..=1000 {
                trunk
                    .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                    .unwrap();
            }
            trunk.execute("COMMIT").unwrap();
            let b = trunk.fork_branch().unwrap();
            let bc = b.connect().unwrap();
            bc.execute("BEGIN").unwrap();
            for id in 1..=k {
                bc.execute(format!("UPDATE t SET v = 'b' WHERE id = {id}"))
                    .unwrap();
            }
            bc.execute("COMMIT").unwrap();
            drop(bc);
            let before = db.branch_stats().work;
            let mut merger = Merger::new(trunk.clone()).unwrap();
            let o = merger.merge(b, key_replay()).unwrap();
            assert_eq!(o.refused, None, "{o:?}");
            let after = db.branch_stats().work;
            assert_eq!(
                after.trunk_commit_rows_stamped - before.trunk_commit_rows_stamped,
                0,
                "k = {k}: rows stamped inside the trunk-lock hold"
            );
            assert_eq!(
                after.merge_rows_stamped_post_gate - before.merge_rows_stamped_post_gate,
                k as u64,
                "k = {k}: rows stamped after the gate"
            );
        }
    }

    /// A19 (1): a trunk commit between the merger's pre-probe and its WAL write lock is caught by
    /// the O(1) re-check, which falls back to probing under the lock. Mutant 34 accepts the stale
    /// pre-probe, installs over the trunk's update and fails here.
    #[test]
    fn a_commit_between_the_pre_probe_and_the_lock_is_not_missed() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=10 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let b = trunk.fork_branch().unwrap();
        b.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'b' WHERE id = 5")
            .unwrap();
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let writer = trunk.clone();
        merger.after_preprobe = Some(Box::new(move || {
            writer
                .execute("UPDATE t SET v = 'trunk' WHERE id = 5")
                .unwrap();
        }));
        let o = merger.merge(b, key_replay()).unwrap();
        assert_eq!(o.refused, Some(Refusal::Key), "{o:?}");
        assert_eq!(
            int(&trunk, "SELECT count(*) FROM t WHERE id = 5 AND v = 'trunk'"),
            1
        );
        let w = db.branch_stats().work;
        assert_eq!(
            (w.merge_preprobe_hits, w.merge_preprobe_misses),
            (0, 1),
            "the re-check did not reject the stale pre-probe"
        );
    }

    /// A19 (2): the stamps are still pruned, by the committing connection after it releases the
    /// WAL write lock: 200 single-row merges leave one merge's worth. Mutant 35 skips the prune
    /// and keeps all 200. Every pre-probe here is current (the merger is the only writer).
    #[test]
    fn stamps_are_pruned_after_the_wal_lock_is_released() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=300 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let mut merger = Merger::new(trunk.clone()).unwrap();
        for id in 1..=200 {
            let b = trunk.fork_branch().unwrap();
            b.connect()
                .unwrap()
                .execute(format!("UPDATE t SET v = 'b' WHERE id = {id}"))
                .unwrap();
            let o = merger.merge(b, key_replay()).unwrap();
            assert_eq!(o.refused, None, "{o:?}");
        }
        let s = db.branch_stats();
        assert!(s.row_stamps <= 2, "{} stamps held after 200 merges", s.row_stamps);
        assert_eq!(s.work.merge_preprobe_hits, 200, "{:?}", s.work);
    }

    /// W12 (PREREG A21): every stamps prune runs with the trunk's WAL write lock and the trunk
    /// lock both free. Each prune calls the thread-local probe, which reads the real lock state at
    /// that moment: the shared WAL write lock (tried, and released at once if it was free) and the
    /// trunk lock (`try_lock`). Mutant 37 prunes at the pre-A19 site, under the committer's WAL
    /// write lock; T-A19b, which checks only that the stamps stay bounded, passes on it. Mutant 38
    /// prunes inside the trunk-lock hold. The trunk assert comes first, so each mutant's panic
    /// names its own lock.
    #[test]
    fn the_stamps_prune_runs_with_the_wal_write_lock_free() {
        let (_dir, db) = open_db();
        let seen = std::rc::Rc::new(std::cell::Cell::new((0u64, 0u64, 0u64)));
        {
            let (db, seen) = (db.clone(), seen.clone());
            crate::branch::store::PRUNE_PROBE.with(|p| {
                *p.borrow_mut() = Some(Box::new(move || {
                    let wal_free = match db.shared_wal.try_read() {
                        Some(shared) => {
                            let free = shared.runtime.write_lock.write();
                            if free {
                                shared.runtime.write_lock.unlock();
                            }
                            free
                        }
                        None => false,
                    };
                    let trunk_free = db.branches.trunk_lock_free();
                    let (n, wal_held, trunk_held) = seen.get();
                    seen.set((
                        n + 1,
                        wal_held + u64::from(!wal_free),
                        trunk_held + u64::from(!trunk_free),
                    ));
                }));
            });
        }
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=300 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let mut merger = Merger::new(trunk.clone()).unwrap();
        for id in 1..=200 {
            let b = trunk.fork_branch().unwrap();
            b.connect()
                .unwrap()
                .execute(format!("UPDATE t SET v = 'b' WHERE id = {id}"))
                .unwrap();
            let o = merger.merge(b, key_replay()).unwrap();
            assert_eq!(o.refused, None, "{o:?}");
        }
        crate::branch::store::PRUNE_PROBE.with(|p| *p.borrow_mut() = None);
        let (n, wal_held, trunk_held) = seen.get();
        // One prune after the lock release per commit that stamped rows: every merge stamps.
        assert!(n >= 200, "{n} stamps prunes ran for 200 merges");
        assert_eq!(trunk_held, 0, "{trunk_held} of {n} stamps prunes ran under the trunk lock");
        assert_eq!(wal_held, 0, "{wal_held} of {n} stamps prunes ran under the WAL write lock");
    }

    /// A22: the trunk lock's per-site accounting closes. Every trunk-lock acquisition is counted
    /// against exactly one call site, and with lock timing on every hold against its site, so the
    /// site arrays sum to the trunk lock's totals exactly; the merge path's sites are the ones taken.
    #[test]
    fn trunk_lock_sites_sum_to_the_trunk_lock_totals() {
        let (_dir, db) = open_db();
        db.set_branch_lock_timing(true);
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=50 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let live = trunk.fork_branch().unwrap();
        let mut merger = Merger::new(trunk.clone()).unwrap();
        for id in 1..=20 {
            let b = trunk.fork_branch().unwrap();
            b.connect()
                .unwrap()
                .execute(format!("UPDATE t SET v = 'b' WHERE id = {id}"))
                .unwrap();
            let o = merger.merge(b, key_replay()).unwrap();
            assert_eq!(o.refused, None, "{o:?}");
        }
        drop(live);
        let w = db.branch_stats().work;
        assert_eq!(w.trunk_site_acq.iter().sum::<u64>(), w.trunk_lock_acquisitions, "{w:?}");
        assert_eq!(w.trunk_site_hold_ns.iter().sum::<u64>(), w.trunk_lock_hold_ns, "{w:?}");
        let site = |name: &str| crate::branch::TRUNK_SITES.iter().position(|s| *s == name).unwrap();
        for name in ["fork", "decide", "prepare", "release"] {
            assert!(w.trunk_site_acq[site(name)] >= 20, "{name}: {:?}", w.trunk_site_acq);
        }
        assert!(w.trunk_lock_hold_ns > 0, "lock timing was on");
    }

    /// A19a (mutant 36): the prune after the WAL lock keeps a stamp one epoch above the oldest live
    /// child. The sole child c forks at f; the trunk's update of row 5 is decided at f + 1 and
    /// pruned after with oldest f; c's update of row 5 must be refused. Mutant 36 prunes the stamp.
    #[test]
    fn a_stamp_just_above_the_oldest_live_child_is_kept() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=10 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let c = trunk.fork_branch().unwrap();
        trunk
            .execute("UPDATE t SET v = 'trunk' WHERE id = 5")
            .unwrap();
        assert_eq!(
            db.branch_stats().row_stamps,
            1,
            "the prune after the WAL lock removed a stamp a live child needs"
        );
        c.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'c' WHERE id = 5")
            .unwrap();
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge(c, key_replay()).unwrap();
        assert_eq!(o.refused, Some(Refusal::Key), "{o:?}");
        assert_eq!(
            int(&trunk, "SELECT count(*) FROM t WHERE id = 5 AND v = 'trunk'"),
            1
        );
    }

    /// R3's survivor (mutant 6): a physical merge's rows join the commit's stamps too, so a later
    /// row-granular merge of the same row, by a branch forked before it, is refused.
    #[test]
    fn a_physical_merge_stamps_the_rows_it_installs() {
        let (_dir, db) = open_db();
        db.set_branch_read_tracking(true);
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for id in 1..=10 {
            trunk
                .execute(format!("INSERT INTO t VALUES ({id}, 'a')"))
                .unwrap();
        }
        let (b1, b2) = (trunk.fork_branch().unwrap(), trunk.fork_branch().unwrap());
        b1.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'p' WHERE id = 5")
            .unwrap();
        b2.connect()
            .unwrap()
            .execute("UPDATE t SET v = 'k' WHERE id = 5")
            .unwrap();
        let physical = MergePolicy {
            validation: Validation::PageStamp,
            install: Install::Physical,
        };
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge(b1, physical).unwrap();
        assert_eq!(o.refused, None, "{o:?}");
        let o = merger.merge(b2, key_replay()).unwrap();
        assert_eq!(o.refused, Some(Refusal::Key), "{o:?}");
        assert_eq!(int(&trunk, "SELECT count(*) FROM t WHERE id = 5 AND v = 'p'"), 1);
    }

    /// A foreign key the engine cannot resolve (its parent table does not exist) refuses a member
    /// that writes its table, as the engine refuses such a write with foreign keys on.
    #[test]
    fn an_unresolvable_foreign_key_refuses_the_member() {
        let (_dir, db) = open_db();
        let trunk = db.connect().unwrap();
        trunk
            .execute("CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES nowhere(id))")
            .unwrap();
        let b = trunk.fork_branch().unwrap();
        b.connect()
            .unwrap()
            .execute("INSERT INTO c VALUES (1, 5)")
            .unwrap();
        trunk.execute("PRAGMA foreign_keys = ON").unwrap();
        let mut merger = Merger::new(trunk.clone()).unwrap();
        let o = merger.merge(b, key_replay()).unwrap();
        assert_eq!(o.refused, Some(Refusal::Install), "{o:?}");
        assert!(format!("{o:?}").contains("mismatch"), "{o:?}");
        assert_eq!(int(&trunk, "SELECT count(*) FROM c"), 0);
    }
}
