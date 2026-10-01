//! Merging a branch back into the trunk, on the durable store (r13-compose: the Merger of turso
//! b161e861d, ported onto the composed catalog store; PREREG §2 and amendments 2-6).
//!
//! # What a merge is here
//!
//! A branch B forked from the trunk at trunk epoch `trunk_at` has changed some rows. Merging it makes
//! B's changes the trunk's, inside ONE trunk write transaction (`BEGIN IMMEDIATE` … `COMMIT`), so
//! validation and install both run under the trunk's WAL write lock: no trunk commit and no fork
//! (forks take that lock on this store) can fall between deciding and writing. The semantics are a
//! three-way merge's: base = the trunk as B forked it, ours = the trunk now, theirs = B. B's change
//! to a row is refused when the trunk changed that row after the fork.
//!
//! # B's write set is DERIVED (A5)
//!
//! The rows B changed are derived at merge time from the pages B owns, against its base (see
//! `derive.rs`): nothing about B's writes is recorded or persisted apart from B's own pages, so an
//! eviction, a restart, a fuzzy checkpoint or a splice cannot lose it. A row B wrote back to its base
//! value is no change (content semantics, A6.4).
//!
//! # Validation
//!
//! * [`Validation::BaseRead`] (MV4, primary): per changed row, the base record (from the derivation)
//!   against the trunk's record now. Stateless, so exact across a restart. It refuses whenever ours
//!   differs from base, even where theirs equals ours (A6.4 (ii); counted as `refusals_same_change`).
//! * [`Validation::KeyStamp`] (MV3): the trunk's per-row stamps (set at each trunk commit while the
//!   trunk has a child). Stamps live in memory, so for a B forked before this process opened the
//!   store the stamps cannot answer, and the base read decides instead (D-M5's horizon).
//!
//! # Install
//!
//! Replay only: each changed row is copied from B into the trunk as a ROW IMAGE through the trunk's
//! own SQL path, deletes first, then updates, then inserts, each in key order (B's last-write order
//! is not derivable from pages; a UNIQUE value moved between rows can then be refused, as
//! `Refusal::Install`). The trunk compiles the install with no triggers and no foreign-key actions
//! (PostgreSQL's `session_replication_role = replica`): B's triggers already wrote their rows. When
//! the caller's connection enforces foreign keys they are checked after the member instead.
//!
//! # Scope
//!
//! A merge takes a direct child of the trunk with no open connection and no live child, whose schema
//! is still the trunk's. The derivation refuses DDL, a clear or a delete of every row, an owned page
//! no tree reaches (an incremental blob write), and index-kind writes when the schema has a WITHOUT
//! ROWID table. Batches of more than one member, the physical install and the page-granular
//! validators of b161e861d are not ported (D-M8).

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::derive::{self, Change, PageSource, Tree};
use super::store::BranchStore;
use super::{Branch, BranchId};
use crate::schema::{BTreeTable, ResolvedFkRef, Schema, Table};
use crate::storage::pager::Pager;
use crate::{Connection, LimboError, Result, Statement, Value};

/// What a merge is validated against. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Validation {
    /// MV4: the base read, stateless.
    BaseRead,
    /// MV3: the trunk's row stamps, with the restart horizon.
    KeyStamp,
}

/// How a merge runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergePolicy {
    pub validation: Validation,
    /// Keep the merged branch live (detached) instead of releasing it: refs that are never
    /// deleted (A2). A second merge of a kept branch is refused by either validator, since the
    /// trunk wrote its rows after its fork.
    pub keep_merged: bool,
}

/// Why a merge was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// Outside what a merge takes (see the module doc); not a conflict.
    Scope,
    /// The stamps say the trunk wrote a row the branch changed after the fork.
    Key,
    /// The base read says the trunk's row now differs from its base.
    Base,
    /// Validation passed, but a trunk constraint refused the rows (or the rows no longer fit the
    /// table). The member's own savepoint is rolled back.
    Install,
}

/// One merge's result.
#[derive(Debug, Clone)]
pub struct MergeOutcome {
    pub branch: BranchId,
    /// `None` if the merge committed.
    pub refused: Option<Refusal>,
    pub scope: Option<&'static str>,
    /// Rows the derivation found changed, and how many the merge installed.
    pub rows_changed: usize,
    /// Which validator decided: `KeyStamp` falls back to the base read before the horizon.
    pub decided_by: Validation,
    /// Why the install refused, when `refused` is [`Refusal::Install`].
    pub install_error: Option<String>,
}

/// What [`Merger::prepare`] found for one branch.
struct Prepared {
    scope: Option<&'static str>,
    /// Changed rows in install order: deletes, then updates, then inserts, each by key.
    rows: Vec<(i64, i64)>,
    changes: Vec<Change>,
    schema: Arc<Schema>,
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
/// member writes the table. `Err(reason)`: the engine cannot resolve the table's foreign keys, so
/// with foreign keys on a member that writes the table is refused.
struct FkChecks {
    version: u32,
    child: HashMap<i64, std::result::Result<Vec<ChildFk>, String>>,
    parent: HashMap<i64, std::result::Result<Vec<ParentFk>, String>>,
}

/// Merges branches into the trunk, keeping the trunk statements the replay install prepares, per
/// table root AND trunk schema version.
pub struct Merger {
    /// The caller's trunk connection. Read at every merge for its settings; never written through.
    caller: Arc<Connection>,
    /// The merger's OWN trunk connection, which compiles for row images for its whole life: no
    /// triggers and no foreign-key actions.
    trunk: Arc<Connection>,
    store: Arc<BranchStore>,
    stmts: HashMap<(i64, u32), TableStmts>,
    fks: Option<FkChecks>,
}

impl Drop for Merger {
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

/// Every b-tree root `schema` names, for the derivation: sqlite_schema (page 1), each table (a
/// WITHOUT ROWID table is an index-kind tree), each index.
fn trees_of(schema: &Schema) -> Vec<Tree> {
    let mut trees = vec![Tree {
        root: 1,
        table: true,
        without_rowid: false,
    }];
    for t in schema.tables.values() {
        if let Table::BTree(bt) = t.as_ref() {
            if bt.root_page > 1 {
                trees.push(Tree {
                    root: bt.root_page as u32,
                    table: bt.has_rowid,
                    without_rowid: !bt.has_rowid,
                });
            }
        }
    }
    for ix in schema.indexes.values().flatten() {
        if ix.root_page > 1 {
            trees.push(Tree {
                root: ix.root_page as u32,
                table: false,
                without_rowid: false,
            });
        }
    }
    trees
}

/// Run `stmt` to completion and reset it, even when it fails, so a cached statement is reusable.
fn step_to_end(stmt: &mut Statement) -> Result<()> {
    let ran = stmt.run_ignore_rows();
    let reset = stmt.reset();
    ran?;
    reset
}

/// A constraint error while installing refuses the member (`Ok(Some(reason))`); any other error
/// aborts the merge.
fn member_error(err: LimboError) -> Result<Option<String>> {
    match err {
        LimboError::Constraint(msg) | LimboError::ForeignKeyConstraint(msg) => Ok(Some(msg)),
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

/// The savepoint that brackets one member's install.
const MEMBER: &str = "r13_merge_member";

/// Read trunk page `page` through `pager`, as its connection sees it now.
fn read_trunk_page(pager: &Pager, page: u32) -> Result<Vec<u8>> {
    let (p, pending) = pager.io.block(|| pager.read_page(i64::from(page)))?;
    if let Some(c) = pending {
        pager.io.wait_for_completion(c)?;
    }
    Ok(p.get_contents().as_slice().to_vec())
}

/// The database size in pages that page 1's header names.
fn header_size(page1: &[u8]) -> u32 {
    u32::from_be_bytes(page1[28..32].try_into().unwrap())
}

/// A version of the database as the branch store resolves it for branch `id`: the branch's own view,
/// or its base (the trunk at the branch's fork). A page the store does not hold in the arena is the
/// trunk's current version, read through the merger's trunk pager (inside its write transaction,
/// before the member installs anything).
struct StoreView<'a> {
    store: &'a BranchStore,
    id: BranchId,
    base: bool,
    trunk: &'a Pager,
    page_size: usize,
    size: Option<u32>,
}

impl StoreView<'_> {
    fn fetch(&mut self, page: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.page_size];
        let held = if self.base {
            self.store.base_page_into(self.id, page, &mut buf)?
        } else {
            self.store.resolve_into(self.id, page, &mut buf)?
        };
        if held {
            Ok(buf)
        } else {
            read_trunk_page(self.trunk, page)
        }
    }
}

impl PageSource for StoreView<'_> {
    fn read(&mut self, page: u32) -> Result<Option<Arc<Vec<u8>>>> {
        if page == 0 {
            return Ok(None);
        }
        if self.size.is_none() {
            let one = self.fetch(1)?;
            self.size = Some(header_size(&one));
            if page == 1 {
                return Ok(Some(Arc::new(one)));
            }
        }
        if page > self.size.unwrap_or(0) {
            return Ok(None);
        }
        Ok(Some(Arc::new(self.fetch(page)?)))
    }
}

/// The trunk now, as the merger's own connection sees it inside its write transaction.
struct TrunkNow<'a> {
    pager: &'a Pager,
    size: Option<u32>,
}

impl PageSource for TrunkNow<'_> {
    fn read(&mut self, page: u32) -> Result<Option<Arc<Vec<u8>>>> {
        if page == 0 {
            return Ok(None);
        }
        if self.size.is_none() {
            let one = read_trunk_page(self.pager, 1)?;
            self.size = Some(header_size(&one));
        }
        if page > self.size.unwrap_or(0) {
            return Ok(None);
        }
        Ok(Some(Arc::new(read_trunk_page(self.pager, page)?)))
    }
}

impl Merger {
    /// A merger for the trunk `trunk` is connected to, which must be a trunk connection. The
    /// merger writes through a connection of its own.
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

    /// Give the merger's connection the caller's settings that decide how a merge writes and
    /// commits (an allowlist, as in b161e861d: a setting added later is NOT carried until it is
    /// added here).
    fn mirror_settings(&self) {
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

    /// Merge one branch. The branch is released afterwards, merged or refused, unless the policy
    /// keeps merged branches (then it is detached, merged or refused).
    pub fn merge(&mut self, branch: Branch, policy: MergePolicy) -> Result<MergeOutcome> {
        if !self.caller.get_auto_commit() {
            return Err(LimboError::InvalidArgument(
                "the caller's connection has a transaction open; a merge commits its own"
                    .to_string(),
            ));
        }
        self.mirror_settings();
        let fk_on = self.caller.foreign_keys_enabled();
        let outcome = self.run(&branch, policy, fk_on);
        self.store.merge_counted(|w| {
            w.merge_attempts += 1;
            if let Ok(o) = &outcome {
                match o.refused {
                    None => w.merge_commits += 1,
                    Some(Refusal::Scope) => w.merge_refused_scope += 1,
                    Some(Refusal::Key) => w.merge_refused_key += 1,
                    Some(Refusal::Base) => w.merge_refused_base += 1,
                    Some(Refusal::Install) => w.merge_refused_install += 1,
                }
            }
        });
        if policy.keep_merged {
            let _ = branch.into_id();
        } else {
            drop(branch);
        }
        outcome
    }

    fn run(&mut self, branch: &Branch, policy: MergePolicy, fk_on: bool) -> Result<MergeOutcome> {
        // BEGIN IMMEDIATE checks the schema cookie once it holds the write lock, so from here to
        // COMMIT the merger's schema is the trunk's.
        self.trunk.execute("BEGIN IMMEDIATE")?;
        let result = self.validate_and_install(branch, policy, fk_on);
        match &result {
            Ok(outcome) if outcome.refused.is_none() => {
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
        branch: &Branch,
        policy: MergePolicy,
        fk_on: bool,
    ) -> Result<MergeOutcome> {
        let pager = self.trunk.pager.load().clone();
        crate::turso_assert!(
            pager.holds_write_lock(),
            "a merge validates only inside its trunk write transaction"
        );
        let mut outcome = MergeOutcome {
            branch: branch.id(),
            refused: None,
            scope: None,
            rows_changed: 0,
            decided_by: policy.validation,
            install_error: None,
        };
        let view = self.store.merge_view(branch.id())?;
        let early = if !view.parent_is_trunk {
            Some("the branch is not a child of the trunk")
        } else if view.open || view.writer {
            Some("the branch has an open connection")
        } else if view.live_children > 0 {
            Some("the branch has a live child")
        } else {
            None
        };
        if let Some(scope) = early {
            outcome.refused = Some(Refusal::Scope);
            outcome.scope = Some(scope);
            return Ok(outcome);
        }
        let from = branch.connect()?;
        let prep = self.prepare(branch.id(), &view, &from, &pager)?;
        outcome.rows_changed = prep.rows.len();
        if let Some(scope) = prep.scope {
            outcome.refused = Some(Refusal::Scope);
            outcome.scope = Some(scope);
            return Ok(outcome);
        }
        // Validation.
        let keys: Vec<(i64, i64)> = prep.rows.clone();
        let roots: Vec<i64> = {
            let mut r: Vec<i64> = keys.iter().map(|k| k.0).collect();
            r.sort_unstable();
            r.dedup();
            r
        };
        let stamped = match policy.validation {
            Validation::KeyStamp => pager.with_trunk_pending(|pending| {
                self.store
                    .keystamp_verdict(view.trunk_at, &keys, &roots, pending)
            }),
            Validation::BaseRead => None,
        };
        let refused = match stamped {
            Some(true) => Some(Refusal::Key),
            Some(false) => None,
            None => {
                outcome.decided_by = Validation::BaseRead;
                self.base_read(&prep, &pager)?
            }
        };
        if refused.is_some() {
            outcome.refused = refused;
            return Ok(outcome);
        }
        if let Some(reason) = self.install_member(branch, &from, &prep, fk_on)? {
            outcome.refused = Some(Refusal::Install);
            outcome.install_error = Some(reason);
        }
        Ok(outcome)
    }

    /// MV4: refused if, for any changed row, the trunk's record now differs from the base record.
    fn base_read(&self, prep: &Prepared, pager: &Pager) -> Result<Option<Refusal>> {
        let usable = pager.usable_space();
        let mut now = TrunkNow { pager, size: None };
        let mut pages = 0u64;
        let mut conflicts = 0u64;
        let mut same = 0u64;
        for (&(root, rowid), change) in prep.rows.iter().zip(&prep.changes) {
            let ours = derive::row_at(&mut now, root as u32, rowid, usable, &mut pages)?;
            if ours != change.base {
                conflicts += 1;
                if ours == change.theirs {
                    same += 1;
                }
            }
        }
        let n = prep.rows.len() as u64;
        self.store.merge_counted(|w| {
            w.mv4_keys += n;
            w.derive_pages_read += pages;
            if conflicts > 0 && same == conflicts {
                w.refusals_same_change += 1;
            }
        });
        Ok((conflicts > 0).then_some(Refusal::Base))
    }

    /// Derive branch `id`'s changed rows (A5's algorithm D) and check the scope they fall in.
    fn prepare(
        &self,
        id: BranchId,
        view: &super::store::MergeView,
        from: &Arc<Connection>,
        pager: &Pager,
    ) -> Result<Prepared> {
        let schema = from.schema.read().clone();
        let trunk_schema = self.trunk.schema.read().schema_version;
        let trees = trees_of(&schema);
        let page_size = pager.get_page_size_unchecked().get() as usize;
        let usable = pager.usable_space();
        let mut theirs = StoreView {
            store: &self.store,
            id,
            base: false,
            trunk: pager,
            page_size,
            size: None,
        };
        let mut base = StoreView {
            store: &self.store,
            id,
            base: true,
            trunk: pager,
            page_size,
            size: None,
        };
        let derived = derive::derive(&mut theirs, &mut base, &view.owned, &trees, usable)?;
        let c = derived.counters;
        let refusal = derived.refusal;
        let n_changes = derived.changes.len() as u64;
        self.store.merge_counted(|w| {
            w.derive_pages_read += c.pages_read;
            w.derive_attribution_seeks += c.attribution_seeks;
            w.derive_rows_compared += c.rows_compared;
            w.derive_subtrees_enumerated += c.subtrees_enumerated;
            w.derive_subtrees_cancelled += c.subtrees_cancelled;
            w.derive_freelist_reads += c.freelist_reads;
            w.derive_keys += n_changes;
            match refusal {
                Some(derive::DeriveRefusal::Ddl) => w.derive_refusals_ddl += 1,
                Some(derive::DeriveRefusal::ClearOrDeleteAll) => {
                    w.derive_refusals_clear_or_delete_all += 1
                }
                Some(derive::DeriveRefusal::Unattributed) => w.derive_refusals_unattributed += 1,
                Some(derive::DeriveRefusal::WithoutRowid) => w.derive_refusals_without_rowid += 1,
                None => {}
            }
        });
        let mut scope = refusal.map(|r| r.scope());
        if scope.is_none() && schema.schema_version != trunk_schema {
            scope = Some("the trunk changed its schema since the fork");
        }
        // Install order: deletes, then updates, then inserts, each by key.
        let mut deletes = Vec::new();
        let mut updates = Vec::new();
        let mut inserts = Vec::new();
        for ((root, rowid), change) in derived.changes {
            let key = (i64::from(root), rowid);
            match (&change.base, &change.theirs) {
                (Some(_), None) => deletes.push((key, change)),
                (Some(_), Some(_)) => updates.push((key, change)),
                (None, _) => inserts.push((key, change)),
            }
        }
        let mut rows = Vec::new();
        let mut changes = Vec::new();
        for (key, change) in deletes.into_iter().chain(updates).chain(inserts) {
            rows.push(key);
            changes.push(change);
        }
        if scope.is_none() && rows.iter().any(|&(root, _)| table_at(&schema, root).is_none()) {
            scope = Some("the branch wrote rows of a table a merge does not take");
        }
        Ok(Prepared {
            scope,
            rows,
            changes,
            schema,
        })
    }

    /// Install one validated member inside its own SAVEPOINT. `Ok(Some(reason))`: the install
    /// refused, and everything this member wrote is rolled back.
    fn install_member(
        &mut self,
        branch: &Branch,
        from: &Arc<Connection>,
        prep: &Prepared,
        fk_on: bool,
    ) -> Result<Option<String>> {
        self.trunk.execute(format!("SAVEPOINT {MEMBER}"))?;
        let installed = self.install_checked(branch, from, prep, fk_on);
        match installed {
            Ok(None) => {
                self.trunk.execute(format!("RELEASE {MEMBER}"))?;
                Ok(None)
            }
            Ok(Some(reason)) => {
                self.trunk.execute(format!("ROLLBACK TO {MEMBER}"))?;
                self.trunk.execute(format!("RELEASE {MEMBER}"))?;
                Ok(Some(reason))
            }
            Err(err) => {
                let _ = self.trunk.execute(format!("ROLLBACK TO {MEMBER}"));
                let _ = self.trunk.execute(format!("RELEASE {MEMBER}"));
                Err(err)
            }
        }
    }

    /// Install one member's rows and, when the caller's connection enforces foreign keys, check
    /// them.
    fn install_checked(
        &mut self,
        branch: &Branch,
        from: &Arc<Connection>,
        prep: &Prepared,
        fk_on: bool,
    ) -> Result<Option<String>> {
        if prep.rows.is_empty() {
            return Ok(None);
        }
        let held = if fk_on {
            self.children_before(prep, from)?
        } else {
            Vec::new()
        };
        match self.install_rows(branch, from, prep)? {
            None if fk_on => self.foreign_key_violation(prep, &held),
            other => Ok(other),
        }
    }

    /// Replay the branch's changed rows as row images. `Ok(Some(reason))` when a trunk constraint
    /// refuses a row or a row no longer fits its table's column list.
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
                    let t = table_at(&prep.schema, root).expect("prepare admitted this table");
                    tables.insert(root, t.clone());
                    t
                }
            };
            let key = (root, version);
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
                    stmts
                        .delete
                        .bind_at(1.try_into().unwrap(), Value::from_i64(rowid))?;
                    if let Err(err) = step_to_end(&mut stmts.delete) {
                        return member_error(err);
                    }
                }
                [row] => {
                    if row.len() != stmts.columns {
                        return Ok(Some(format!(
                            "branch {} row {rowid} has {} columns; the trunk statement for {} has {}",
                            branch.id().0,
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
                    if !updated {
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
                        branch.id().0,
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

    /// One row of the child `c` whose key is wholly non-NULL and held by no parent row (SQLite's
    /// match for a foreign key: the parent column's affinity and collation).
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

    /// The children of parent row `?1`.
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
    fn children_before(
        &mut self,
        prep: &Prepared,
        from: &Arc<Connection>,
    ) -> Result<Vec<(i64, usize, i64)>> {
        let mut held = Vec::new();
        let mut theirs: HashMap<(i64, usize), Statement> = HashMap::new();
        for &(root, rowid) in &prep.rows {
            let fks = self.fk_sides(root);
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
    /// caller's connection enforces, over this member's rows only.
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
        // The rowid alias stays out of the SET list (b161e861d: assigning it runs a two-pass
        // update through an ephemeral table). OR ABORT overrides a constraint-level ON CONFLICT
        // REPLACE, so installing a row can never delete a row only the trunk has.
        let sets: Vec<String> = names
            .iter()
            .enumerate()
            .filter(|(i, _)| alias != Some(*i))
            .enumerate()
            .map(|(param, (_, n))| format!("{n} = ?{}", param + 2))
            .collect();
        let or_abort = " OR ABORT";
        let update = (!sets.is_empty())
            .then(|| format!("UPDATE{or_abort} {t} SET {} WHERE rowid = ?1", sets.join(", ")));
        let insert = match alias {
            Some(_) if names.len() == 1 => {
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
