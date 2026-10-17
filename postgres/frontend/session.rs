use turso_parser::sql_error::{SqlErrorKind, SqlErrorMessage};
use std::num::NonZero;
use std::str;
use std::sync::Arc;

use crate::aliases;
use crate::catalog::{self, PostgresDialect};
use crate::result_types::StatementTypes;
use turso_core::{Connection, LimboError, PrepareOptions, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_checkpoint, is_comment_on, is_refresh_matview, try_extract_add_constraints,
    try_extract_branch_call, try_extract_copy_from, try_extract_create_schema,
    try_extract_drop_objects, try_extract_drop_schema, try_extract_set, try_extract_show,
    PgAddConstraints, PgBranchArg, PgBranchCall, PgCopyFromStmt, PgCreateSchemaStmt, PgDropKind,
    PgDropObjectsStmt, PgDropSchemaStmt, PgSetStmt, PgSetValue,
    PgShowStmt, PostgreSQLTranslator, BRANCH_FUNCTION_PREFIX,
};

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
    /// A parking_lot lock, as is the view cache's: std's Mutex on Apple boxes its pthread mutex at
    /// the first lock, one allocation per new connection paid inside the branch switch that opens
    /// it (L5's switch budget).
    session_state: parking_lot::Mutex<SessionState>,
    /// A statement failed and undoing it failed too: the engine connection holds a transaction in
    /// an unknown state, so the server must end the session (FATAL 08006) rather than serve more
    /// on it. A flag the server reads, not a sentinel in an error's text (wire review 5 item 4).
    broken: std::sync::atomic::AtomicBool,
    /// The parameter-type walk's views, read once per schema snapshot (wire review 14 item 10).
    view_cache: parking_lot::Mutex<crate::result_types::ViewCache>,
    /// SET lock_timeout, in milliseconds: 0 when unset or set to 0, PostgreSQL's "no timeout",
    /// which here is the server's own lock wait. Lock-free, as the server reads it while a branch
    /// call waits (wire review 16 item 12).
    lock_timeout_ms: std::sync::atomic::AtomicU64,
    /// SET statement_timeout, in milliseconds, 0 none: the server stops a statement running past
    /// it with 57014 (it was refused unless 0).
    statement_timeout_ms: std::sync::atomic::AtomicU64,
    /// SET client_min_messages, as its rank in PostgreSQL's order for it (see [`message_rank`]):
    /// a message of a lower rank is not sent. NOTICE by default.
    min_messages: std::sync::atomic::AtomicU8,
    /// A stamp of the last assignment to the search path, unique across connections, 0 before any
    /// ([`PgConnection::search_path_stamp`]).
    search_path_stamp: std::sync::atomic::AtomicU64,
    /// Notices a statement raised for the client ((severity, condition, message): a WARNING, or a
    /// NOTICE such as DROP ... IF EXISTS's for a missing object), which the server sends before
    /// the statement's result ([`PgConnection::take_warnings`]).
    warnings: parking_lot::Mutex<Vec<(&'static str, SqlErrorKind, String)>>,
}

/// The source of [`PgConnectionInner::search_path_stamp`]'s values.
static SEARCH_PATH_ASSIGNMENTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The client settings this server answers by, under PostgreSQL's names, at the values it behaves
/// by: what SHOW gives and what startup announces (ParameterStatus), from this one table, as they
/// disagreed (wire review 18 item 19). SET accepts each only at a value this server behaves by
/// ([`handle_pg_set`]); all but two leave it as listed, and TimeZone's equivalent zone names and
/// extra_float_digits above 1 are accepted with SHOW unchanged (postgres/COMPAT.md). DateStyle is
/// PostgreSQL's boot value, which this server's ISO dates fit.
pub const FIXED_SETTINGS: &[(&str, &str)] = &[
    ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"),
    ("bytea_output", "hex"),
    ("DateStyle", "ISO, MDY"),
    ("IntervalStyle", "postgres"),
    ("TimeZone", "UTC"),
    ("extra_float_digits", "1"),
];

/// search_path as SHOW gives it, and startup announces it, before any SET: PostgreSQL's default.
pub const DEFAULT_SEARCH_PATH: &str = "\"$user\", public";

/// A search path as SHOW search_path gives it: each name quoted as PostgreSQL quotes it, the
/// default for an empty path.
fn shown_search_path(path: &[String]) -> String {
    if path.is_empty() {
        return DEFAULT_SEARCH_PATH.to_string();
    }
    path.iter()
        .map(|n| turso_pg_parser::quote_identifier(n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A message level's rank in PostgreSQL's order for client_min_messages: debug5 to debug1 1 to 5,
/// log 6, notice 7, warning 8, error 9 (LOG ranks below NOTICE here, unlike for the server's own
/// log). None for no such level.
pub fn message_rank(level: &str) -> Option<u8> {
    Some(match level.to_ascii_lowercase().as_str() {
        "debug5" => 1,
        "debug4" => 2,
        "debug3" => 3,
        "debug2" | "debug" => 4,
        "debug1" => 5,
        "log" => 6,
        "notice" => 7,
        "warning" => 8,
        "error" => 9,
        _ => return None,
    })
}

/// The name SHOW gives a client_min_messages rank ([`message_rank`]), PostgreSQL's spelling (`debug`
/// is shown as debug2).
fn message_level(rank: u8) -> &'static str {
    match rank {
        1 => "debug5",
        2 => "debug4",
        3 => "debug3",
        4 => "debug2",
        5 => "debug1",
        6 => "log",
        8 => "warning",
        9 => "error",
        _ => "notice",
    }
}

/// A millisecond setting as SHOW gives it: 0, or the value in the largest of d, h, min, s and ms
/// that divides it (recorded, pg18-settings.out: 1000ms is 1s, 90000 is 90s, 60000 is 1min, 1500ms
/// stays 1500ms).
fn shown_ms(ms: u64) -> String {
    if ms == 0 {
        return "0".to_string();
    }
    for (unit, size) in [("d", 86_400_000), ("h", 3_600_000), ("min", 60_000), ("s", 1000)] {
        if ms % size == 0 {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{ms}ms")
}

/// The savepoint a SAVEPOINT, RELEASE or ROLLBACK TO statement names, lowercased as PostgreSQL
/// folds an unquoted name; None when it cannot be read.
fn savepoint_name(sql: &str) -> Option<String> {
    let parsed = turso_pg_parser::parse(sql).ok()?;
    let stmt = parsed.protobuf.stmts.first()?.stmt.as_ref()?.node.as_ref()?;
    match stmt {
        turso_pg_parser::pg_query::protobuf::node::Node::TransactionStmt(t)
            if !t.savepoint_name.is_empty() =>
        {
            Some(t.savepoint_name.clone())
        }
        _ => None,
    }
}

/// A time setting's value in milliseconds, as PostgreSQL reads one: a number, its unit ms by
/// default, or followed by us, ms, s, min, h or d. None for a negative or unreadable value.
fn setting_ms(text: &str) -> Option<u64> {
    let text = text.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let value: f64 = text[..split].parse().ok()?;
    let ms = match text[split..].trim() {
        "" | "ms" => 1.0,
        "us" => 0.001,
        "s" => 1000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };
    let ms = (value * ms).round();
    (ms.is_finite() && ms >= 0.0 && ms <= i32::MAX as f64).then_some(ms as u64)
}

impl PgConnectionInner {
    /// Store the settings' effective values where the server reads them, lock-free.
    fn publish(&self, gucs: &Gucs) {
        use std::sync::atomic::Ordering::Relaxed;
        self.lock_timeout_ms.store(gucs.lock_timeout_ms, Relaxed);
        self.statement_timeout_ms.store(gucs.statement_timeout_ms, Relaxed);
        self.min_messages.store(gucs.min_messages, Relaxed);
        // Never off over pending deferred violations (C41-H1: the check the COMMIT makes is the
        // engine's, so turning it off let an orphan commit). The SET that asks for it is refused
        // in `handle_pg_set`. This second guard is UNREACHABLE defence in depth (fk-guard review
        // LOW 4): every block end, ROLLBACK TO and RELEASE reaches here after the engine settled
        // the count, and no client sequence was found that arrives with the setting off and a
        // violation pending; a fault-injection test (`pretend_deferred_violations`) holds it.
        // The count is read only when the setting would turn the check off (LOW 7).
        let enforce = gucs.foreign_keys || deferred_violations(&self.conn) > 0;
        // Only a change reaches the engine (review wire-rework A-M1): the setter bumped the
        // prepare generation whatever the value, so every SET and block end re-prepared every
        // kept statement. The engine's setter now bumps on a change only (fastest-engine
        // 49e10e02a); this keeps the wire's side of the class whatever a setter does.
        if self.conn.foreign_keys_enabled() != enforce {
            self.conn.set_foreign_keys_enabled(enforce);
        }
        let mut state = self.session_state.lock();
        if state.search_path != gucs.search_path {
            state.search_path.clone_from(&gucs.search_path);
            self.search_path_stamp
                .store(SEARCH_PATH_ASSIGNMENTS.fetch_add(1, Relaxed), Relaxed);
        }
    }

}

#[derive(Default)]
struct SessionState {
    /// Effective search-path cache for planning; all assignments come from GucScope::effective.
    search_path: Vec<String>,
    /// The settings this server acts on, scoped as PostgreSQL scopes a GUC (see [`GucScope`]).
    gucs: GucScope,
}

/// Every mutable session setting. GucScope snapshots these values together; publish installs
/// their effective values in the engine and in the server's read caches.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Gucs {
    search_path: Vec<String>,
    foreign_keys: bool,
    lock_timeout_ms: u64,
    statement_timeout_ms: u64,
    min_messages: u8,
}

impl Default for Gucs {
    fn default() -> Self {
        Self {
            search_path: Vec::new(),
            foreign_keys: true,
            lock_timeout_ms: 0,
            statement_timeout_ms: 0,
            min_messages: 7,
        }
    }
}

/// A SET LOCAL's values, each until its block ends.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
struct Locals {
    search_path: Option<Vec<String>>,
    foreign_keys: Option<bool>,
    lock_timeout_ms: Option<u64>,
    statement_timeout_ms: Option<u64>,
    min_messages: Option<u8>,
}

/// PostgreSQL's GUC scoping for [`Gucs`] (recorded, artie pg18-diff/pg18-settings.out and
/// pg18-c26.out; the C26 MED 3 class: ours kept every SET for the session):
/// - SET changes the session's value and replaces any SET LOCAL of it; a ROLLBACK of its block
///   undoes it, a COMMIT keeps it;
/// - SET LOCAL lasts until its block ends, however it ends; outside a block it warns (25P01) and
///   changes nothing;
/// - ROLLBACK TO a savepoint puts back what the settings were at it (the savepoint stays);
///   RELEASE keeps them.
#[derive(Default)]
struct GucScope {
    session: Gucs,
    local: Locals,
    /// The session's values when the block's first SET came; None outside a block, or before.
    block_start: Option<Gucs>,
    /// Each live savepoint's name (lowercased) and the values at it.
    savepoints: Vec<(String, Gucs, Locals)>,
}

impl GucScope {
    fn effective(&self) -> Gucs {
        Gucs {
            search_path: self
                .local
                .search_path
                .as_ref()
                .unwrap_or(&self.session.search_path)
                .clone(),
            foreign_keys: self.local.foreign_keys.unwrap_or(self.session.foreign_keys),
            lock_timeout_ms: self.local.lock_timeout_ms.unwrap_or(self.session.lock_timeout_ms),
            statement_timeout_ms: self
                .local
                .statement_timeout_ms
                .unwrap_or(self.session.statement_timeout_ms),
            min_messages: self.local.min_messages.unwrap_or(self.session.min_messages),
        }
    }
}

/// A transaction-control event the server reports to the connection's settings ([`GucScope`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TxEvent {
    /// The block ended: committed, or rolled back.
    Ended { committed: bool },
    /// A SAVEPOINT was defined.
    Savepoint,
    /// A RELEASE SAVEPOINT ran.
    Release,
    /// A ROLLBACK TO SAVEPOINT ran.
    RollbackTo,
}

/// Open a database with the PostgreSQL schema dialect, resolving the IO
/// backend from `vfs` or the path like [`turso_core::Database::open_new`].
pub fn open_database(
    path: &str,
    vfs: Option<&str>,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<(Arc<dyn turso_core::IO>, Arc<turso_core::Database>)> {
    let io = match vfs {
        Some(vfs) => turso_core::Database::io_for_vfs(vfs)?,
        None => turso_core::Database::io_for_path(path)?,
    };
    let db = open_database_with_io(io.clone(), path, flags, opts)?;
    Ok((io, db))
}

/// Open a database with the PostgreSQL schema dialect on an existing IO
/// backend.
pub fn open_database_with_io(
    io: Arc<dyn turso_core::IO>,
    path: &str,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<Arc<turso_core::Database>> {
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts),
    )
}

/// Each engine connection's session ([`PgConnectionInner`]), by the connection's address, for the
/// engine's re-prepare: [`crate::catalog::PostgresDialect`]'s `parse_for_reprepare` gets the
/// engine connection only and translates as the session's prepare does through it (ONE
/// TRANSLATION PATH, the wire line review M1-M4). An entry is removed when its session drops.
static SESSIONS_BY_CONNECTION: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<usize, std::sync::Weak<PgConnectionInner>>>,
> = std::sync::LazyLock::new(Default::default);

/// The session of the engine connection `conn`, if one is alive.
pub(crate) fn session_of(conn: &Arc<Connection>) -> Option<Arc<PgConnectionInner>> {
    SESSIONS_BY_CONNECTION
        .lock()
        .get(&(Arc::as_ptr(conn) as usize))
        .and_then(std::sync::Weak::upgrade)
        .filter(|inner| Arc::ptr_eq(&inner.conn, conn))
}

impl Drop for PgConnectionInner {
    fn drop(&mut self) {
        let key = Arc::as_ptr(&self.conn) as usize;
        let mut sessions = SESSIONS_BY_CONNECTION.lock();
        // Only this session's own entry: another may since have registered the same address.
        if sessions.get(&key).is_some_and(|w| w.strong_count() == 0) {
            sessions.remove(&key);
        }
    }
}

impl PgConnection {
    pub fn new(conn: Arc<Connection>) -> Self {
        aliases::install(&conn);
        // PostgreSQL enforces every foreign key; the engine only under PRAGMA foreign_keys.
        conn.set_foreign_keys_enabled(true);
        let pg = Self {
            inner: Arc::new(PgConnectionInner {
                conn,
                session_state: parking_lot::Mutex::new(SessionState::default()),
                broken: std::sync::atomic::AtomicBool::new(false),
                view_cache: parking_lot::Mutex::new(crate::result_types::ViewCache::default()),
                lock_timeout_ms: std::sync::atomic::AtomicU64::new(0),
                statement_timeout_ms: std::sync::atomic::AtomicU64::new(0),
                min_messages: std::sync::atomic::AtomicU8::new(7),
                search_path_stamp: std::sync::atomic::AtomicU64::new(0),
                warnings: parking_lot::Mutex::new(Vec::new()),
            }),
        };
        SESSIONS_BY_CONNECTION
            .lock()
            .insert(Arc::as_ptr(&pg.inner.conn) as usize, Arc::downgrade(&pg.inner));
        pg
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    /// SET lock_timeout, in milliseconds; 0 when unset or set to 0 (see
    /// `PgConnectionInner::lock_timeout_ms`).
    pub fn lock_timeout_ms(&self) -> u64 {
        self.inner
            .lock_timeout_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// SET statement_timeout, in milliseconds; 0 when unset or set to 0.
    pub fn statement_timeout_ms(&self) -> u64 {
        self.inner
            .statement_timeout_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// SET client_min_messages' rank ([`message_rank`]); NOTICE's when unset.
    pub fn min_messages_rank(&self) -> u8 {
        self.inner
            .min_messages
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A stamp that changes whenever the search path is assigned (SET search_path, or a branch
    /// switch copying it), unique across connections; 0 for a connection whose path was never
    /// assigned. Lock-free, so the server can check it at every ReadyForQuery and read
    /// [`PgConnection::shown_search_path`] only when it moved.
    pub fn search_path_stamp(&self) -> u64 {
        self.inner
            .search_path_stamp
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether the session's search path is `path`, read with no copy: a switch to another
    /// connection moves the stamp whatever the path is, and the server asks this before it pays
    /// for [`PgConnection::shown_search_path`] (L5).
    pub fn search_path_is(&self, path: &[String]) -> bool {
        self.inner.session_state.lock().search_path == path
    }

    /// The session's search path, as SET gave it.
    pub fn search_path(&self) -> Vec<String> {
        self.inner.session_state.lock().search_path.clone()
    }

    /// The session's search path as SHOW search_path gives it.
    pub fn shown_search_path(&self) -> String {
        shown_search_path(&self.inner.session_state.lock().search_path)
    }

    /// Whether a failed statement's undo failed too, leaving the engine connection in an unknown
    /// transaction (see `PgConnectionInner::broken`): the session must end.
    pub fn is_broken(&self) -> bool {
        self.inner.broken.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Run `tx` from the engine's AST: no SQL text is parsed, so an implicit block costs no
    /// libpg_query call.
    pub fn engine_tx(&self, tx: TxStmt) -> Result<()> {
        let was_in_block = !self.inner.conn.get_auto_commit();
        let r = run_tx(&self.inner.conn, tx);
        // A block this ended, for the acted-on settings' scope (an implicit block's end comes here).
        if was_in_block && self.inner.conn.get_auto_commit() {
            let committed = r.is_ok() && matches!(tx, TxStmt::Commit);
            self.tx_event(TxEvent::Ended { committed }, "");
        }
        r
    }

    /// Run `f` in a transaction of its own ([`own_transaction`]'s rule: an undo that fails breaks
    /// the connection); `name` is its savepoint's inside an open transaction, `what` names the
    /// statement in a broken connection's error.
    pub fn in_own_transaction<T>(
        &self,
        name: &'static str,
        what: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        own_transaction(&self.inner, name, what, f)
    }

    /// Roll back the transaction the engine holds (an implicit block's): a rollback that fails, or
    /// leaves a transaction held, breaks the connection ([`own_transaction`]'s rule), and its error
    /// says so. Nothing to do in autocommit.
    pub fn roll_back_or_break(&self) -> Result<()> {
        let conn = &self.inner.conn;
        if conn.get_auto_commit() {
            return Ok(());
        }
        let r = undo_failpoint()
            .and_then(|()| run_tx(conn, TxStmt::Rollback))
            .and_then(|()| held_after_undo(conn, false))
            .map_err(|undo| break_connection(&self.inner, "the transaction's rollback", &undo));
        if conn.get_auto_commit() {
            self.tx_event(TxEvent::Ended { committed: false }, "");
        }
        r
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        prepare_statement(&self.inner, sql.as_ref())
    }

    /// [`PgConnection::prepare`], with the types the same parse gives: PostgreSQL's type OID for
    /// each result column the engine cannot type itself (aggregates; see
    /// [`crate::result_types::aggregate_types`]; `None`, or a short list, where the engine types
    /// the column), and for each parameter its context types (see
    /// [`crate::result_types::parameter_types`]).
    pub fn prepare_typed(&self, sql: impl AsRef<str>) -> Result<(Statement, StatementTypes)> {
        let mut types = StatementTypes::default();
        let stmt = prepare_statement_typed(&self.inner, sql.as_ref(), Some(&mut types))?;
        Ok((stmt, types))
    }

    /// [`PgConnection::prepare_typed`] for a Describe, which must never perform the statement:
    /// `None` for one that preparing would perform (SET, CREATE/DROP SCHEMA, ALTER TABLE ADD
    /// CONSTRAINT, COPY FROM, or a statement whose prerequisites would run first). Such a statement
    /// returns no rows; its Execute prepares and performs it, once.
    pub fn prepare_for_describe(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Statement, StatementTypes)>> {
        let mut types = StatementTypes::default();
        let stmt = prepare_statement_inner(&self.inner, sql.as_ref(), Some(&mut types), true)?;
        Ok(stmt.map(|stmt| (stmt, types)))
    }

    /// The Parse step's analysis (C53): a statement PostgreSQL analyses at Parse and this engine
    /// compiles without performing it (a query, INSERT, UPDATE, DELETE, MERGE) is prepared from
    /// `msg`'s parse, so whatever its analysis refuses (a missing relation, an unknown function, a
    /// parameter it cannot type) refuses the Parse, as there; the statement is returned with its
    /// types for the round's Bind, Describe and Execute. Any other statement is analysed when it
    /// runs, as PostgreSQL analyses a utility statement: None, and nothing is compiled. A branch
    /// call or blank statement has no parse: None.
    pub fn parse_step(
        &self,
        sql: &str,
        msg: &mut ParseMessage,
    ) -> Result<Option<(Statement, StatementTypes)>> {
        use turso_pg_parser::pg_query::NodeRef;
        let analysed = msg.parse.as_ref().is_some_and(|parse| {
            matches!(
                parse.protobuf.nodes().first().map(|t| &t.0),
                Some(
                    NodeRef::SelectStmt(_)
                        | NodeRef::InsertStmt(_)
                        | NodeRef::UpdateStmt(_)
                        | NodeRef::DeleteStmt(_)
                        | NodeRef::MergeStmt(_)
                )
            )
        });
        if !analysed || msg.call.is_some() {
            return Ok(None);
        }
        let mut types = StatementTypes::default();
        let stmt = prepare_statement_parsed(
            &self.inner,
            sql,
            msg.parse.take(),
            Some(&mut types),
            true,
            true,
        )?;
        Ok(stmt.map(|stmt| (stmt, types)))
    }

    pub fn query(&self, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        let sql = turso_pg_parser::pg_trim(sql.as_ref());
        if sql.is_empty() {
            return Ok(None);
        }
        self.prepare(sql).map(Some)
    }

    pub fn execute(&self, sql: impl AsRef<str>) -> Result<()> {
        for stmt in self.query_runner(sql.as_ref().as_bytes()) {
            if let Some(mut stmt) = stmt? {
                stmt.run_ignore_rows()?;
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.inner.conn.close()
    }

    pub fn pragma_update(&self, name: &str, value: impl std::fmt::Display) -> Result<()> {
        let sql = catalog::sqlite_text(format!("PRAGMA {name} = {value}"));
        let mut stmt = self.inner.conn.prepare_internal(sql)?;
        stmt.run_ignore_rows()
    }

    pub fn query_runner<'a>(&'a self, sql: &'a [u8]) -> PgQueryRunner<'a> {
        PgQueryRunner::new(&self.inner, sql)
    }

    /// Take over `from`'s session settings (the search path, and the acted-on settings' session
    /// values): a wire session that moves to another engine connection — onto a branch, or back to
    /// the trunk — keeps what it SET. A switch runs outside any block, so no SET LOCAL or block
    /// scope is open to carry.
    pub fn adopt_session_of(&self, from: &PgConnection) {
        if Arc::ptr_eq(&self.inner, &from.inner) {
            return;
        }
        let gucs = from.inner.session_state.lock().gucs.effective();
        {
            let mut state = self.inner.session_state.lock();
            state.gucs = GucScope {
                session: gucs.clone(),
                ..GucScope::default()
            };
        }
        self.inner.publish(&gucs);
    }

    /// A transaction-control event of the session's ([`TxEvent`]), for the acted-on settings'
    /// scope: a block's end drops its SET LOCALs and, rolled back, undoes its SETs; a savepoint
    /// keeps the values at it for a ROLLBACK TO. `sql` names the savepoint.
    pub fn tx_event(&self, event: TxEvent, sql: &str) {
        let mut state = self.inner.session_state.lock();
        let gucs = &mut state.gucs;
        match event {
            TxEvent::Ended { committed } => {
                if let (false, Some(start)) = (committed, gucs.block_start.take()) {
                    gucs.session = start;
                }
                gucs.local = Locals::default();
                gucs.block_start = None;
                gucs.savepoints.clear();
            }
            TxEvent::Savepoint | TxEvent::Release | TxEvent::RollbackTo => {
                let Some(name) = savepoint_name(sql) else {
                    return;
                };
                if event == TxEvent::Savepoint {
                    if gucs.block_start.is_none() {
                        gucs.block_start = Some(gucs.session.clone());
                    }
                    gucs.savepoints
                        .push((name, gucs.session.clone(), gucs.local.clone()));
                } else if let Some(at) = gucs.savepoints.iter().rposition(|(n, ..)| *n == name) {
                    if event == TxEvent::RollbackTo {
                        let (_, session, local) = gucs.savepoints[at].clone();
                        gucs.session = session;
                        gucs.local = local;
                        gucs.savepoints.truncate(at + 1);
                    } else {
                        gucs.savepoints.truncate(at);
                    }
                }
            }
        }
        let effective = gucs.effective();
        drop(state);
        self.inner.publish(&effective);
    }

    /// The notices the last statements raised for the client, as (severity, SQLSTATE, message).
    pub fn take_warnings(&self) -> Vec<(&'static str, SqlErrorKind, String)> {
        std::mem::take(&mut *self.inner.warnings.lock())
    }
}

/// The branch function call `sql` is, if it is one (see [`PgBranchCall`]). A statement whose text
/// does not contain the branch-function prefix, in any case, is not parsed. One that does is read
/// by [`fast_branch_call`], the server's fast path (DECISIONS L5: a branch call reaches the engine
/// with no libpg_query call); a form it does not cover — a comment, an escape or dollar-quoted
/// string, a second argument, a quoted name — is parsed by libpg_query, which then decides.
pub fn branch_call(sql: &str) -> Option<PgBranchCall> {
    let prefix = BRANCH_FUNCTION_PREFIX.as_bytes();
    if !sql
        .as_bytes()
        .windows(prefix.len())
        .any(|w| w.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    if let Some(call) = fast_branch_call(sql) {
        return Some(call);
    }
    let parsed = turso_pg_parser::parse(sql).ok()?;
    // libpg_query wraps a $n above i32::MAX into a 32-bit int, so its tree's number is not the
    // text's: such a statement is no call, and the ordinary prepare refuses it with 42601 (wire
    // review 12 item 1).
    if sql.contains('$') && turso_pg_parser::checked_param_numbers(&parsed, sql).is_err() {
        return None;
    }
    try_extract_branch_call(&parsed)
}

/// A Parse message's text as PostgreSQL's exec_parse_message reads it before anything runs (C53):
/// with ONE libpg_query parse (none for a blank statement or a branch call's fast form), the
/// statement's syntax (a lexical or syntax error refuses the Parse, 42601, where it passed Parse
/// and Bind and failed at Execute: C40 review LOW 2), whether it is a branch call, and its
/// parameters. The branch call's parse, the parameter scan and the prepare each made their own
/// call (C40 review LOW 6).
pub struct ParseMessage {
    /// The branch call the text is, if any.
    pub call: Option<PgBranchCall>,
    /// PostgreSQL analyses the statement at Parse and so counts its `$n` (scan_params' list: a
    /// query, INSERT, UPDATE, DELETE, MERGE, CALL, EXPLAIN, DECLARE, CREATE TABLE ... AS); any
    /// other statement's parameters are the ones Parse declared.
    pub counts_params: bool,
    /// The highest `$n` of the tree, 0 for none; None when a number is past what the tree holds
    /// (libpg_query's 32-bit wrap), which the prepare refuses (42601).
    pub highest: Option<u32>,
    /// The parse, for [`PgConnection::parse_step`]; None for a blank statement or a fast call.
    parse: Option<turso_pg_parser::pg_query::ParseResult>,
}

pub fn read_parse_message(sql: &str) -> Result<ParseMessage> {
    use turso_pg_parser::pg_query::NodeRef;
    let none = |call| ParseMessage {
        call,
        counts_params: false,
        highest: Some(0),
        parse: None,
    };
    let trimmed = turso_pg_parser::pg_trim(sql);
    if trimmed.is_empty() {
        return Ok(none(None));
    }
    if let Some(call) = fast_branch_call(sql) {
        return Ok(none(Some(call)));
    }
    // The trimmed text, as the prepare reads it: the tree's locations must index the text the
    // prepare holds (a leading blank shifted every $n's location by one).
    let parse =
        turso_pg_parser::parse(trimmed).map_err(|e| LimboError::ParseError(e.into()))?;
    let numbers = turso_pg_parser::checked_param_numbers(&parse, trimmed);
    let prefix = BRANCH_FUNCTION_PREFIX.as_bytes();
    let mentions = sql
        .as_bytes()
        .windows(prefix.len())
        .any(|w| w.eq_ignore_ascii_case(prefix));
    // As branch_call reads it: a wrapped $n makes no call.
    let call = if mentions && numbers.is_ok() {
        try_extract_branch_call(&parse)
    } else {
        None
    };
    let counts_params = matches!(
        parse.protobuf.nodes().first().map(|t| &t.0),
        Some(
            NodeRef::SelectStmt(_)
                | NodeRef::InsertStmt(_)
                | NodeRef::UpdateStmt(_)
                | NodeRef::DeleteStmt(_)
                | NodeRef::MergeStmt(_)
                | NodeRef::CallStmt(_)
                | NodeRef::ExplainStmt(_)
                | NodeRef::DeclareCursorStmt(_)
                | NodeRef::CreateTableAsStmt(_)
        )
    );
    let highest = numbers.ok().map(|ns| {
        ns.into_iter()
            .map(|n| n.max(0) as u32)
            .max()
            .unwrap_or(0)
    });
    Ok(ParseMessage {
        call,
        counts_params,
        highest,
        parse: Some(parse),
    })
}

/// The common forms of a branch call, read byte by byte with no allocation but the call itself:
/// `SELECT turso_branch_<op>(<arg>?)` with an optional trailing `;`, where `<arg>` is a standard
/// string literal (`''` for a quote; a backslash is an ordinary character, as PostgreSQL reads one
/// with standard_conforming_strings on) or a `$n` parameter, either optionally cast to `text`,
/// `varchar` or `character varying` with no length (a cast that changes nothing; any other cast
/// is read by libpg_query, and [`try_extract_branch_call`] refuses it). Keywords and the
/// function name in any case; PostgreSQL's whitespace (space, tab, newline, carriage return, form
/// feed, vertical tab) anywhere a token boundary allows it. `None` means only "not one of these
/// forms": the caller asks libpg_query. Whatever this returns, [`try_extract_branch_call`] returns
/// for the same statement (pinned by a differential test over a corpus).
fn fast_branch_call(sql: &str) -> Option<PgBranchCall> {
    let b = sql.as_bytes();
    let mut i = 0;
    let ws = |i: &mut usize| {
        while *i < b.len() && turso_pg_parser::pg_space(b[*i]) {
            *i += 1;
        }
    };
    // An identifier or keyword: [A-Za-z_][A-Za-z0-9_]*, ended by a byte that cannot continue one
    // (PostgreSQL identifiers also take '$' and non-ASCII bytes after the first: those end the
    // fast path, not the identifier).
    let word = |i: &mut usize| -> Option<std::ops::Range<usize>> {
        let start = *i;
        if *i >= b.len() || !(b[*i].is_ascii_alphabetic() || b[*i] == b'_') {
            return None;
        }
        while *i < b.len() && (b[*i].is_ascii_alphanumeric() || b[*i] == b'_') {
            *i += 1;
        }
        if *i < b.len() && (b[*i] == b'$' || b[*i] >= 0x80) {
            return None;
        }
        Some(start..*i)
    };
    ws(&mut i);
    let select = word(&mut i)?;
    if !b[select].eq_ignore_ascii_case(b"select") {
        return None;
    }
    let before_name = i;
    ws(&mut i);
    if i == before_name {
        return None;
    }
    let name = word(&mut i)?;
    // Borrowed when it is a branch function's name: no allocation for the common calls.
    let function = turso_pg_parser::translator::branch_function_name(&sql[name]);
    if !function.starts_with(BRANCH_FUNCTION_PREFIX) {
        return None;
    }
    ws(&mut i);
    if b.get(i) != Some(&b'(') {
        return None;
    }
    i += 1;
    ws(&mut i);
    let mut args = smallvec::SmallVec::new();
    if b.get(i) != Some(&b')') {
        let arg = match b.get(i)? {
            b'\'' => {
                i += 1;
                let mut text = compact_str::CompactString::default();
                let mut run = i;
                loop {
                    let q = i + b[i..].iter().position(|&c| c == b'\'')?;
                    if b.get(q + 1) == Some(&b'\'') {
                        text.push_str(&sql[run..q + 1]);
                        i = q + 2;
                        run = i;
                    } else {
                        text.push_str(&sql[run..q]);
                        i = q + 1;
                        break;
                    }
                }
                PgBranchArg::Text(text)
            }
            b'$' => {
                i += 1;
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                // More than five digits, or a number outside 1..=MAX_PARAMETER, is for the slow
                // path: no call is made of a number out of range, so the ordinary prepare refuses
                // it (42P02, or 42601 above i32::MAX, which `branch_call` reads from the text
                // before it trusts libpg_query's 32-bit number; wire review 9 item 1, review 12
                // item 1).
                if i - start > 5 {
                    return None;
                }
                let n: usize = sql[start..i].parse().ok()?;
                if !(1..=turso_pg_parser::MAX_PARAMETER as usize).contains(&n)
                    || (i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_'))
                {
                    return None;
                }
                PgBranchArg::Param(n)
            }
            _ => return None,
        };
        ws(&mut i);
        if b[i..].starts_with(b"::") {
            i += 2;
            ws(&mut i);
            // Only a cast that changes nothing: to text, or to varchar with no length. A length,
            // array bounds, a qualified name or any other type ends the fast path here.
            let ty = &b[word(&mut i)?];
            if ty.eq_ignore_ascii_case(b"character") {
                let before = i;
                ws(&mut i);
                if i == before || !b[word(&mut i)?].eq_ignore_ascii_case(b"varying") {
                    return None;
                }
            } else if !(ty.eq_ignore_ascii_case(b"text") || ty.eq_ignore_ascii_case(b"varchar")) {
                return None;
            }
            ws(&mut i);
        }
        args.push(arg);
    }
    if b.get(i) != Some(&b')') {
        return None;
    }
    i += 1;
    ws(&mut i);
    if b.get(i) == Some(&b';') {
        i += 1;
        ws(&mut i);
    }
    if i != b.len() {
        return None;
    }
    Some(PgBranchCall { function, args })
}

/// Attach every PostgreSQL schema database file (`turso-postgres-schema-<name>.db`) beside
/// `db_file` to `conn`, so a connection opened after a `CREATE SCHEMA` sees the schema. Failures
/// are logged and skipped.
pub fn attach_schema_files(conn: &PgConnection, db_file: &str) {
    if db_file == ":memory:" {
        return;
    }
    let dir = std::path::Path::new(db_file)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(schema) = name
            .strip_prefix("turso-postgres-schema-")
            .and_then(|s| s.strip_suffix(".db"))
        else {
            continue;
        };
        let path = entry.path().to_string_lossy().to_string();
        let sql = catalog::sqlite_text(format!("ATTACH '{path}' AS \"{schema}\""));
        tracing::info!("Auto-attaching PG schema '{}' from {}", schema, path);
        if let Err(e) = conn.inner().execute(&sql) {
            tracing::warn!("Failed to attach schema '{}': {}", schema, e);
        }
    }
}

pub struct PgQueryRunner<'a> {
    conn: &'a Arc<PgConnectionInner>,
    stmts: Vec<String>,
    index: usize,
}

impl<'a> PgQueryRunner<'a> {
    fn new(conn: &'a Arc<PgConnectionInner>, sql: &'a [u8]) -> Self {
        let sql = str::from_utf8(sql).unwrap_or("");
        Self {
            conn,
            // Statement by statement, as psql runs a script: a malformed part fails in its place,
            // the others run (wire review 18 item 7). A simple Query over the wire is parsed
            // whole instead ([`split_statements`]).
            stmts: split_script(sql)
                .into_iter()
                .filter(|stmt| !turso_pg_parser::pg_trim(stmt).is_empty())
                .collect(),
            index: 0,
        }
    }
}

impl Iterator for PgQueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.stmts.len() {
            return None;
        }

        let sql = &self.stmts[self.index];
        self.index += 1;
        Some(prepare_statement(self.conn, sql).map(Some))
    }
}

/// A script's statements, one by one ([`turso_pg_parser::split_statements_lenient`]): a malformed
/// part is a part of its own, which fails when run. Text with no separator but a trailing one is
/// one statement, with no libpg_query call, as in [`split_statements`].
fn split_script(sql: &str) -> Vec<String> {
    let trimmed = turso_pg_parser::pg_trim(sql);
    let body = turso_pg_parser::pg_trim(trimmed.strip_suffix(';').unwrap_or(trimmed));
    if !body.contains(';') {
        return vec![body.to_string()];
    }
    turso_pg_parser::split_statements_lenient(sql).unwrap_or_else(|_| vec![trimmed.to_string()])
}

/// The one statement `sql` is when it holds no separator but a trailing one (no literal, comment or
/// dollar quote can make it two), borrowed: [`split_statements`]' first case, which a server can
/// take without the split's Vec and copy (L5).
pub fn single_statement(sql: &str) -> Option<&str> {
    let trimmed = turso_pg_parser::pg_trim(sql);
    let body = turso_pg_parser::pg_trim(trimmed.strip_suffix(';').unwrap_or(trimmed));
    (!body.is_empty() && !body.contains(';')).then_some(body)
}

pub fn split_statements(sql: &str) -> Result<Vec<String>> {
    // Text with no separator but a trailing one is one statement: no literal, comment or dollar
    // quote can make it two, so libpg_query is not asked.
    if let Some(body) = single_statement(sql) {
        return Ok(vec![body.to_string()]);
    }
    let trimmed = turso_pg_parser::pg_trim(sql);
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !trimmed.is_empty() => Ok(vec![trimmed.to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![trimmed.to_string()]),
    }
}

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<Statement> {
    prepare_statement_typed(pg_conn, sql, None)
}

/// [`prepare_statement`], filling `types` (when asked) with the types the parse gives.
fn prepare_statement_typed(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    types: Option<&mut StatementTypes>,
) -> Result<Statement> {
    prepare_statement_inner(pg_conn, sql, types, false)?.ok_or_else(|| {
        LimboError::InternalError("only a Describe declines to prepare a statement".to_string())
    })
}

/// A statement this frontend answers itself instead of through the translator, read from the
/// parse once ([`Special::of`]), never from the text's first word. The Describe decline, the `$n`
/// guard and the dispatch ([`try_prepare_special`]) all read this one value; they read two lists
/// kept by hand, which could drift, and walked the parse up to nineteen times per statement where
/// this walks it at most nine (wire review 17 item 11, review 2 L17).
enum Special {
    Set(PgSetStmt),
    Show(PgShowStmt),
    CreateSchema(PgCreateSchemaStmt),
    DropSchema(PgDropSchemaStmt),
    DropObjects(PgDropObjectsStmt),
    RefreshMatview,
    CommentOn,
    Checkpoint,
    AddConstraints(PgAddConstraints),
    CopyFrom(PgCopyFromStmt),
}

impl Special {
    fn of(parse_result: &turso_pg_parser::pg_query::ParseResult) -> Option<Special> {
        if let Some(stmt) = try_extract_set(parse_result) {
            return Some(Special::Set(stmt));
        }
        if let Some(stmt) = try_extract_show(parse_result) {
            return Some(Special::Show(stmt));
        }
        if let Some(stmt) = try_extract_create_schema(parse_result) {
            return Some(Special::CreateSchema(stmt));
        }
        if let Some(stmt) = try_extract_drop_schema(parse_result) {
            return Some(Special::DropSchema(stmt));
        }
        if let Some(stmt) = try_extract_drop_objects(parse_result) {
            return Some(Special::DropObjects(stmt));
        }
        if is_refresh_matview(parse_result) {
            return Some(Special::RefreshMatview);
        }
        if is_comment_on(parse_result) {
            return Some(Special::CommentOn);
        }
        if is_checkpoint(parse_result) {
            return Some(Special::Checkpoint);
        }
        if let Some(add) = try_extract_add_constraints(parse_result) {
            return Some(Special::AddConstraints(add));
        }
        if let Some(stmt) = try_extract_copy_from(parse_result) {
            return Some(Special::CopyFrom(stmt));
        }
        None
    }

    /// Whether preparing it performs it: SET, CREATE SCHEMA, DROP SCHEMA, ALTER TABLE ADD
    /// CONSTRAINT's rebuild and COPY FROM's import run in the prepare.
    fn performs(&self) -> bool {
        matches!(
            self,
            Special::Set(_)
                | Special::CreateSchema(_)
                | Special::DropSchema(_)
                | Special::DropObjects(_)
                | Special::AddConstraints(_)
                | Special::CopyFrom(_)
        )
    }
}

/// The prepare behind [`prepare_statement_typed`]. With `describe`, a statement that preparing
/// would perform (see [`Special::performs`]), or one that needs prerequisite statements run
/// first (a SERIAL column's sequence), is declined with `None`: it returns no rows, and only its
/// execution may perform it (wire review 2 item 2, review 3 item 2).
fn prepare_statement_inner(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    types: Option<&mut StatementTypes>,
    describe: bool,
) -> Result<Option<Statement>> {
    prepare_statement_checked(pg_conn, sql, types, describe, true)
}

/// [`prepare_statement_inner`]; `check_keys` false for an ALTER's own rebuild statements, whose
/// CREATE TABLEs carry the table's keys: the ALTER checks the keys it adds on its own, and an older
/// key that no longer resolves must not refuse the rebuild (wire review 13 item 3).
fn prepare_statement_checked(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    types: Option<&mut StatementTypes>,
    describe: bool,
    check_keys: bool,
) -> Result<Option<Statement>> {
    prepare_statement_parsed(pg_conn, sql, None, types, describe, check_keys)
}

/// [`prepare_statement_checked`] from `parsed`, the text's parse when the caller already made it
/// (a Parse message's: [`read_parse_message`]), so a Parse costs one libpg_query call.
fn prepare_statement_parsed(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    parsed: Option<turso_pg_parser::pg_query::ParseResult>,
    mut types: Option<&mut StatementTypes>,
    describe: bool,
    check_keys: bool,
) -> Result<Option<Statement>> {
    use crate::phase::{enter, Phase};
    let parsing = enter(Phase::Parse);
    // PostgreSQL's whitespace only (wire review 13 item 1).
    let sql = turso_pg_parser::pg_trim(sql);
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string().into(),
        ));
    }

    reject_sqlite_catalog_access(sql)?;
    // A kind is never carried by text a client can write (review wire-line2 H1, the lead's class):
    // the marker is the frontend's own SQLite's, and this text is stored as the statement's SQL,
    // which an engine re-prepare would then read as SQLite instead of the PostgreSQL statement
    // checked here. Client text that starts with it is refused. BLIND SPOT, closed only by the
    // engine's typed text kind at integration: a statement's text is still what decides its parse.
    if sql.starts_with(crate::catalog::SQLITE_TEXT_MARKER.trim_end()) {
        return Err(LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::SyntaxError,
            "a statement may not start with the frontend's internal marker comment",
        )));
    }

    // One parse serves both the special forms and the translation (it was two), and a Parse
    // message's serves its classification too (C53).
    let parse_result = match parsed {
        Some(parsed) => parsed,
        None => turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.into()))?,
    };
    // One statement per prepare, as PostgreSQL's Parse takes one (42601): the translator reads the
    // first, so `COMMIT; INSERT ...` in one Parse committed and skipped the INSERT, answering
    // success (wire review 14 item 5). The simple protocol splits a query before it prepares.
    if parse_result.protobuf.stmts.len() > 1 {
        return Err(LimboError::ParseError(
            "cannot insert multiple commands into a prepared statement".to_string().into(),
        ));
    }
    // Every $n of the whole tree, before anything is sized by one: a number past
    // MAX_PARAMETER (Bind counts in 16 bits) or below 1 names no parameter (42P02, PostgreSQL's
    // "there is no parameter"); `SELECT 1 LIMIT $2147483647` sized a 16 GiB list (wire review 8
    // item 3). Only a statement whose text holds a '$' pays the walk.
    let used: Vec<u32> = if sql.contains('$') {
        // Read from the text as PostgreSQL 18 reads them: a number above i32::MAX is 42601, never
        // libpg_query's 32-bit wrap (wire review 12 item 1).
        let numbers = turso_pg_parser::checked_param_numbers(&parse_result, sql)
            .map_err(|e| LimboError::ParseError(e.into()))?;
        if let Some(n) = numbers
            .iter()
            .find(|n| !(1..=crate::result_types::MAX_PARAMETER as i32).contains(*n))
        {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedParameter, format!(
                "there is no parameter ${n}"
            ))));
        }
        numbers.into_iter().map(|n| n as u32).collect()
    } else {
        Vec::new()
    };
    let special = Special::of(&parse_result);
    drop(parsing);
    let performs = special.as_ref().is_some_and(Special::performs);
    if describe && performs {
        return Ok(None);
    }
    // Everything below reads the schema the compile will use: the engine adopts the database's
    // newest (another session's DDL) in compile_cmd, which comes last, so it is adopted here first.
    // Read from the session's older snapshot, a CREATE TABLE's key precheck refused a parent another
    // session had just made, every retry alike, since a refused precheck compiles nothing (wire
    // review 16 item 10); DELETE ... USING's column lookup missed a column; and the parameter types
    // were fixed stale at the first Describe, every later Execute refused 0A000 (wire review 18
    // item 8). compile_cmd's own adoption then finds nothing new.
    let planning = enter(Phase::Plan);
    pg_conn.conn.maybe_update_schema();
    drop(planning);
    // A statement this prepare performs, or whose prerequisites it runs (a SERIAL column's
    // sequence), reads no parameter: one holding a $n is refused before anything of it runs,
    // 42P02 "there is no parameter $n" on both protocols, as PostgreSQL answers a utility
    // statement's $n (it parses one with no parameter hook, utility.c; parse_expr.c raises
    // ERRCODE_UNDEFINED_PARAMETER), a Bind's values notwithstanding: the Bind counted its values
    // against the parameters Parse declared, and one sent for a $n here was refused there (08P01).
    // `COPY ... WHERE v = $1` ran inside the prepare before any guard saw it and imported every
    // row (wire review 13 item 9); a Bind's then answered an invented 0A000 (wire review 17 item
    // 4, the lead's ruling).
    let first_param = used.first().copied();
    let parameter_refused = |n: u32| {
        LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::UndefinedParameter,
            format!("there is no parameter ${n}"),
        ))
    };
    if let Some(n) = first_param.filter(|_| performs) {
        return Err(parameter_refused(n));
    }
    if let Some(stmt) = try_prepare_special(pg_conn, special)? {
        return Ok(Some(stmt));
    }
    // A CREATE TABLE's foreign keys: their parents checked now, every key resolved by the server
    // once the table exists, in the CREATE's own transaction (wire review 13 item 2).
    let translating = enter(Phase::Translate);
    let new_table_keys = if check_keys {
        precheck_create_keys(&pg_conn.conn, &parse_result)?
    } else {
        None
    };
    if let Some(types) = types.as_deref_mut() {
        let schema = pg_conn.conn.current_schema();
        // An aggregate's result type is read through the walk, as a parameter's is, so its
        // argument's relation is the one the engine reads (wire review 17 item 15): the walk runs
        // for a statement with a parameter or an aggregate result column.
        let aggregates = crate::result_types::aggregate_targets(&parse_result);
        types.columns = vec![None; aggregates.len()];
        if !used.is_empty() || aggregates.contains(&true) {
            // A parameter compared with something no context types is refused (42P18) by the
            // server, which alone reads the types the client declared (wire review 8 item 7,
            // review 11 item 1).
            let search_path = pg_conn.session_state.lock().search_path.clone();
            // Which attached schema holds a name, probed only when the walk resolves an
            // unqualified name through one, once per (schema, name) per statement (wire review 17
            // item 3).
            let names = pg_conn.conn.attached_database_names();
            let probed = std::cell::RefCell::new(std::collections::HashMap::new());
            let holds = |schema: &str, name: &str| {
                let key = (schema.to_ascii_lowercase(), name.to_ascii_lowercase());
                if let Some(held) = probed.borrow().get(&key) {
                    return *held;
                }
                let held = get_table_columns(&pg_conn.conn, name, Some(schema))
                    .is_ok_and(|columns| !columns.is_empty());
                probed.borrow_mut().insert(key, held);
                held
            };
            let attached = crate::result_types::Attached {
                names: &names,
                holds: &holds,
            };
            // A column reference naming no FROM entry is refused here, 42P01 (wire review 17
            // item 5).
            let walked = crate::result_types::parameter_types(
                &parse_result,
                &schema,
                &search_path,
                &pg_conn.view_cache,
                &attached,
            )
            .map_err(LimboError::ParseError)?;
            types.params = walked.params;
            types.untyped = walked.untyped;
            types.columns = crate::result_types::aggregate_types(&aggregates, &walked.columns);
        }
        // The columns whose type PostgreSQL's grammar fixes (a boolean expression, a float4 cast),
        // where no aggregate typed them: the engine has no boolean type (PG 18 differential,
        // 2026-10-15).
        types.param_targets = crate::result_types::param_targets(&parse_result);
        types.folds_at_plan = folds_at_plan(&parse_result);
        // A table's width, for a `*` beside a typed column, from the in-memory schema alone.
        let tables = |schema: &str, table: &str| resident_relation_columns(pg_conn, schema, table);
        let fixed = crate::result_types::expression_types(&parse_result, &tables);
        if fixed.len() == types.columns.len() {
            for (column, fixed) in types.columns.iter_mut().zip(fixed) {
                *column = column.or(fixed);
            }
        } else if types.columns.is_empty() {
            // A `*` over derived tables and CTEs alone, which expression_types expanded (the
            // aggregate list cannot line up with a `*`, so it is empty).
            types.columns = fixed;
        }
        types.used = used;
    }

    // A DELETE ... USING reads its relations' declared columns (wire review 13 item 5, review 15
    // item 7), wherever it sits (EXPLAIN's, a CTE's: wire review 16 item 21); a translator with no
    // lookup refuses one. An INSERT whose VALUES lists, with no column list, hold DEFAULT names its
    // positions from the in-memory schema alone (no catalog query). No other statement pays for a
    // lookup.
    let translated = session_translation(pg_conn, sql, &parse_result, false)?;
    drop(translating);
    // A statement with prerequisites (a CREATE TABLE's SERIAL sequences) is performed below, keys
    // and all, so the server has no keys left to resolve.
    let performs = !translated.prereqs.is_empty();
    if let Some(types) = types {
        types.new_table_keys = if performs {
            None
        } else {
            new_table_keys.clone()
        };
        types.performed = performs;
        types.commits = matches!(translated.cmd.stmt(), ast::Stmt::Commit { .. });
        types.rolls_back = matches!(
            translated.cmd.stmt(),
            ast::Stmt::Rollback {
                savepoint_name: None,
                ..
            }
        );
    }
    if describe && !translated.prereqs.is_empty() {
        return Ok(None);
    }
    if let Some(n) = first_param.filter(|_| !translated.prereqs.is_empty()) {
        return Err(parameter_refused(n));
    }

    let options = {
        let state = pg_conn.session_state.lock();
        let path = state.search_path.clone();
        PrepareOptions {
            unqualified_database_search_path: if path.is_empty() { None } else { Some(path) },
        }
    };
    if !performs {
        let _planning = enter(Phase::Plan);
        let prepared = pg_conn
            .conn
            .prepare_translated_cmd_with_options(translated.cmd, sql, &options);
        // A positional INSERT whose VALUES lists are of another length than the table's is refused
        // by the engine, which takes only the full length; PostgreSQL fills a shorter list's
        // leading columns. Asked again, the translator reads the table's columns and names them, on
        // this refusal path only, so a positional INSERT that fits pays for no lookup.
        if prepared.is_err() && is_positional_insert_values(&parse_result) {
            let retried = session_translation(pg_conn, sql, &parse_result, true)?;
            return pg_conn
                .conn
                .prepare_translated_cmd_with_options(retried.cmd, sql, &options)
                .map(Some);
        }
        return prepared.map(Some);
    }
    // Performed here, whole, in a transaction of its own (own_transaction): the prerequisites, the
    // statement, and for a table with foreign keys their resolution (42830 if one does not
    // resolve). The prerequisites ran and committed first, on their own, so a refused CREATE TABLE
    // left its sequences, which PostgreSQL never creates (wire review 16 item 15). Its Execute
    // steps a no-op; a Describe declined it above.
    own_transaction(pg_conn, "__turso_create_prereqs", "CREATE TABLE", || {
        for prereq in translated.prereqs {
            let input = prereq.to_string();
            pg_conn
                .conn
                .prepare_translated_stmt_with_options(prereq, &input, &options)?
                .run_ignore_rows()?;
        }
        pg_conn
            .conn
            .prepare_translated_cmd_with_options(translated.cmd, sql, &options)?
            .run_ignore_rows()?;
        match new_table_keys.as_deref() {
            Some(table) => check_table_keys(&pg_conn.conn, table),
            None => Ok(()),
        }
    })?;
    noop_statement(&pg_conn.conn).map(Some)
}

/// THE translation of a statement, for the session's prepare and for the engine's re-prepare of it
/// alike (ONE TRANSLATION PATH, the wire line review M1-M4: the re-prepare translated with a plain
/// translator, so a DELETE ... USING lost its column lookup, a domain cast its CHECK, and
/// 92257e453's refusal list over-refused a CTE alias and any text with "default").
/// - A DELETE ... USING reads its relations' declared columns (wire review 13 item 5, review 15
///   item 7), wherever it sits (EXPLAIN's, a CTE's: wire review 16 item 21); a translator with no
///   lookup refuses one.
/// - An INSERT whose VALUES lists, with no column list, hold DEFAULT, or a FROM item with an alias
///   column list, names its positions from the in-memory schema alone (no catalog query).
/// - `positional_retry`: a positional INSERT the engine refused at its first compile (a VALUES list
///   of another length than the table's) is asked again with the in-memory column lookup, which
///   names the columns. The re-prepare takes that translator for every positional INSERT VALUES:
///   it cannot see a compile failure, and naming the columns is the same statement when the
///   lengths agree.
/// - A cast to a type that is not built-in is kept by name only when it is the engine's own (a
///   domain, a custom type), so the engine runs its CHECKs (`CAST(-5 AS dm)` returned -5: the
///   domain CHECK class's PG 18.6 replay, 2026-10-15).
/// No other statement pays for a lookup. The search path is the session's own: every lookup reads
/// it, and so does the prepare's PrepareOptions.
pub(crate) fn session_translation(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    parse_result: &turso_pg_parser::pg_query::ParseResult,
    positional_retry: bool,
) -> Result<turso_pg_parser::translator::TranslateResult> {
    let translator = || {
        if is_delete_using(sql, parse_result) {
            let pg_conn = pg_conn.clone();
            PostgreSQLTranslator::new()
                .with_columns(move |schema, table| relation_columns(&pg_conn, schema, table))
        } else if positional_retry
            || is_positional_insert_default(sql, parse_result)
            || has_alias_column_list(parse_result)
        {
            let pg_conn = pg_conn.clone();
            PostgreSQLTranslator::new()
                .with_columns(move |schema, table| resident_relation_columns(&pg_conn, schema, table))
        } else {
            PostgreSQLTranslator::new()
        }
    };
    let first = translator();
    let mut translated = first
        .translate_with_prereqs(parse_result)
        .map_err(|e| LimboError::ParseError(e.into()))?;
    let unmapped = first.unmapped_cast_types();
    if !unmapped.is_empty() {
        let schema = pg_conn.conn.current_schema();
        let known: std::collections::HashSet<String> = unmapped
            .into_iter()
            .filter(|name| schema.get_type_def_unchecked(name).is_some())
            .collect();
        if !known.is_empty() {
            translated = translator()
                .with_known_types(known)
                .translate_with_prereqs(parse_result)
                .map_err(|e| LimboError::ParseError(e.into()))?;
        }
    }
    reject_catalog_dml(translated.cmd.stmt())?;
    Ok(translated)
}

/// The engine's re-prepare of a statement this session prepared: through
/// [`session_translation`], as the prepare translated it. A statement with prerequisites (a CREATE
/// TABLE's SERIAL sequences) is performed whole by its prepare and is never re-prepared, so one
/// here is refused rather than compiled without them.
pub(crate) fn reprepare_translation(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    parse_result: &turso_pg_parser::pg_query::ParseResult,
) -> Result<ast::Cmd> {
    let translated =
        session_translation(pg_conn, sql, parse_result, is_positional_insert_values(parse_result))?;
    if !translated.prereqs.is_empty() {
        return Err(LimboError::InternalError(
            "a statement with prerequisites is not re-prepared".to_string(),
        ));
    }
    Ok(translated.cmd)
}

fn reject_catalog_dml(stmt: &ast::Stmt) -> Result<()> {
    let table_name = match stmt {
        ast::Stmt::Insert { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Delete { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Update(update) => Some(update.tbl_name.name.as_str()),
        _ => None,
    };

    let Some(table_name) = table_name else {
        return Ok(());
    };

    if !catalog::is_catalog_table_name(table_name) {
        return Ok(());
    }

    let verb = match stmt {
        ast::Stmt::Insert { .. } => "insert into",
        ast::Stmt::Delete { .. } => "delete from",
        ast::Stmt::Update { .. } => "update",
        _ => unreachable!(),
    };
    Err(LimboError::ParseError(format!(
        "cannot {verb} pg_catalog table \"{table_name}\""
    ).into()))
}

fn reject_sqlite_catalog_access(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    for table_name in ["sqlite_master", "sqlite_schema"] {
        if lower.contains(table_name) {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedTable, format!(
                "no such table: {table_name}"
            ))));
        }
    }
    Ok(())
}

fn try_prepare_special(
    pg_conn: &Arc<PgConnectionInner>,
    special: Option<Special>,
) -> Result<Option<Statement>> {
    let Some(special) = special else {
        return Ok(None);
    };
    let stmt = match special {
        Special::Set(set_stmt) => handle_pg_set(pg_conn, &set_stmt)?,
        Special::Show(show_stmt) => handle_pg_show(pg_conn, &show_stmt)?,
        Special::CreateSchema(stmt) => {
            handle_pg_create_schema(&pg_conn.conn, &stmt)?;
            noop_statement(&pg_conn.conn)?
        }
        Special::DropSchema(stmt) => {
            handle_pg_drop_schema(pg_conn, &stmt)?;
            noop_statement(&pg_conn.conn)?
        }
        Special::DropObjects(stmt) => {
            handle_pg_drop_objects(pg_conn, &stmt)?;
            noop_statement(&pg_conn.conn)?
        }
        Special::RefreshMatview | Special::CommentOn => noop_statement(&pg_conn.conn)?,
        // PostgreSQL's CHECKPOINT writes every dirty page to the data files; a WAL checkpoint that
        // copies every frame back into the database file and truncates the log is the same act
        // here.
        Special::Checkpoint => pg_conn
            .conn
            .prepare(catalog::sqlite_text("PRAGMA wal_checkpoint(TRUNCATE)"))?,
        Special::AddConstraints(add) => {
            handle_pg_add_constraints(pg_conn, &add)?;
            noop_statement(&pg_conn.conn)?
        }
        Special::CopyFrom(stmt) => {
            let rows_inserted = handle_pg_copy_from(pg_conn, &stmt)?;
            let stmt = noop_statement(&pg_conn.conn)?;
            stmt.set_n_change(rows_inserted as i64);
            stmt
        }
    };
    Ok(Some(stmt))
}

fn noop_statement(conn: &Arc<Connection>) -> Result<Statement> {
    conn.prepare("SELECT 0 WHERE 0")
}

fn execute_sqlite_internal(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_internal(sql)?;
    stmt.run_ignore_rows()
}

/// Parameters SET accepts and this server need not act on, whatever their value: planner, resource
/// and client settings that change no answer it gives (`enable_*` too).
const NOOP_PARAMETERS: &[&str] = &[
    "application_name",
    "check_function_bodies",
    "cpu_index_tuple_cost",
    "cpu_operator_cost",
    "cpu_tuple_cost",
    "default_statistics_target",
    "effective_cache_size",
    "effective_io_concurrency",
    "from_collapse_limit",
    "geqo",
    "jit",
    "join_collapse_limit",
    "maintenance_work_mem",
    "max_parallel_workers_per_gather",
    "plan_cache_mode",
    "random_page_cost",
    "row_security",
    "seq_page_cost",
    "synchronous_commit",
    "temp_buffers",
    "work_mem",
];

/// A boolean setting or input as PostgreSQL's parse_bool reads one: blanks around it, any case,
/// `t`/`true`, `y`/`yes`, `f`/`false`, `n`/`no` or any prefix of those words, `on`, `off` (at least
/// two letters, so `o` is ambiguous), `1` and `0`.
pub fn pg_bool(text: &str) -> Option<bool> {
    let s = text
        .trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c'))
        .to_ascii_lowercase();
    let prefix_of = |word: &str| !s.is_empty() && word.starts_with(s.as_str());
    match s.as_str() {
        "1" | "on" => Some(true),
        "0" | "of" | "off" => Some(false),
        _ if prefix_of("true") || prefix_of("yes") => Some(true),
        _ if prefix_of("false") || prefix_of("no") => Some(false),
        _ => None,
    }
}

/// SET of `set_stmt`'s parameter, from an allowlist (wire review 15 item 2): search_path; the
/// engine's foreign_keys, exposed deliberately; a read-only transaction asked for through
/// transaction_read_only or default_transaction_read_only is 0A000 (the engine has none, as BEGIN
/// READ ONLY is refused), `off` changes nothing; client_encoding, standard_conforming_strings,
/// bytea_output, DateStyle, IntervalStyle, TimeZone and extra_float_digits are accepted at the
/// values this server answers by (UTF8, on, hex, ISO, postgres, UTC, 1 or more) and 0A000 at any
/// other; NOOP_PARAMETERS and `enable_*` are accepted; a dotted (custom) name is 0A000; any other
/// name is 42704 "unrecognized configuration parameter", as PostgreSQL answers a name it does not
/// know (one it knows and this list omits gets 42704 too). Every SET became `PRAGMA name = value`,
/// which the engine ignores for a name it does not know, so `transaction_read_only = on` answered
/// SET and the writes after it committed, and one it does know (synchronous, journal_mode) changed
/// the engine.
fn handle_pg_set(pg_conn: &Arc<PgConnectionInner>, set_stmt: &PgSetStmt) -> Result<Statement> {
    let name = set_stmt.name.to_ascii_lowercase();
    let in_block = !pg_conn.conn.get_auto_commit();
    // PostgreSQL warns of a SET LOCAL outside a block first, for every setting and before the
    // value is checked (ExecSetVariableStmt's WarnNoTransactionBlock before set_config_option), so
    // an invalid value is the warning and then its error (review wire-rework C-M1, C-M2: the
    // warning came only after an accepted value, and search_path's arm never reached it).
    if set_stmt.is_local && !in_block {
        pg_conn.warnings.lock().push((
            "WARNING",
            SqlErrorKind::NoActiveSqlTransaction,
            "SET LOCAL can only be used in transaction blocks".to_string(),
        ));
    }
    // Every mutable setting uses this assignment path, including the outside-block LOCAL rule.
    let assign = |apply: &dyn Fn(&mut Gucs, &mut Locals, bool)| -> Result<()> {
        // Outside a block a SET LOCAL lasts only its own statement: nothing to keep. Its warning
        // was given above.
        if set_stmt.is_local && !in_block {
            return Ok(());
        }
        let mut state = pg_conn.session_state.lock();
        let gucs = &mut state.gucs;
        // A foreign_keys SET is applied to a copy first: a change of the effective value to off
        // while the transaction holds deferred violations is refused, by the effective value and
        // not by the statement's form (SET, SET LOCAL, SET SESSION, inside an implicit block alike:
        // C41-H1, review c39799c5d..9e24b455d MED 3), and nothing of the refused SET is kept. No
        // other setting is copied (fk-guard review LOW 7).
        if name == "foreign_keys" {
            let (mut session, mut local) = (gucs.session.clone(), gucs.local.clone());
            apply(&mut session, &mut local, set_stmt.is_local);
            let was = gucs.effective().foreign_keys;
            let now = local.foreign_keys.unwrap_or(session.foreign_keys);
            if was && !now {
                let pending = deferred_violations(&pg_conn.conn);
                if pending > 0 {
                    return Err(LimboError::ParseError(SqlErrorMessage::classified(
                        SqlErrorKind::ObjectNotInPrerequisiteState,
                        format!(
                            "cannot turn foreign_keys off while the transaction holds {pending} \
                             deferred foreign key violation{}",
                            if pending == 1 { "" } else { "s" }
                        ),
                    )));
                }
            }
        }
        if in_block && gucs.block_start.is_none() {
            gucs.block_start = Some(gucs.session.clone());
        }
        apply(&mut gucs.session, &mut gucs.local, set_stmt.is_local);
        let effective = gucs.effective();
        drop(state);
        pg_conn.publish(&effective);
        Ok(())
    };
    if name == "search_path" {
        let path = set_stmt
            .values
            .iter()
            .map(|value| value.as_search_path_name().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| LimboError::ParseError("incorrect format".to_string().into()))?;
        assign(&|session, local, is_local| {
            if is_local {
                local.search_path = Some(path.clone());
            } else {
                session.search_path.clone_from(&path);
                local.search_path = None;
            }
        })?;
        return noop_statement(&pg_conn.conn);
    }
    let value = set_stmt.values.first().ok_or_else(|| {
        LimboError::ParseError(format!("SET {}: no value provided", set_stmt.name).into())
    })?;
    // DateStyle is a list in PostgreSQL, read whole below (search_path is one too, above); any
    // other setting given more than one value is refused there (its flatten_set_variable_args).
    // Only the first value was read, so `SET client_encoding TO UTF8, LATIN1` answered SET (wire
    // review 18 item 19).
    if set_stmt.values.len() > 1 && name != "datestyle" {
        return Err(LimboError::InvalidArgument(SqlErrorMessage::classified(SqlErrorKind::InvalidParameterValue, format!(
            "SET {name} takes only one argument"
        ))));
    }
    let value_text = |value: &PgSetValue| match value {
        PgSetValue::Identifier(v)
        | PgSetValue::StringLiteral(v)
        | PgSetValue::Number(v)
        | PgSetValue::RawSql(v) => v.clone(),
        PgSetValue::Bool(b) => b.to_string(),
        PgSetValue::Null => String::new(),
        // As PostgreSQL quotes it in its refusal: `INTERVAL '1 day'`.
        PgSetValue::Interval { .. } => value.to_sql_string(),
    };
    let text = if name == "datestyle" {
        set_stmt
            .values
            .iter()
            .map(value_text)
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        value_text(value)
    };
    let lower = text.to_ascii_lowercase();
    let unsupported = |what: &str| {
        Err(LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::FeatureNotSupported,
            format!("SET {name} = {text} is not supported: {what}"),
        )))
    };
    // A value PostgreSQL itself refuses is its 22023 with its message, under its spelling of the
    // name; one it accepts and this server does not act on is 0A000 (`unsupported`). Every refused
    // value was 0A000 (C44-M2; recorded, pg18-c44-m2-sets.out).
    let pg_name = pg_parameter_name(&name);
    let refused = |message: String| {
        Err(LimboError::InvalidArgument(SqlErrorMessage::classified(
            SqlErrorKind::InvalidParameterValue,
            message,
        )))
    };
    let invalid = || refused(format!("invalid value for parameter \"{pg_name}\": \"{text}\""));
    let not_boolean = || refused(format!("parameter \"{pg_name}\" requires a Boolean value"));
    // A timeout PostgreSQL reads as a number of milliseconds outside 0 .. i32::MAX.
    let out_of_range_ms = || {
        let digits = text.trim();
        match digits.parse::<i64>() {
            Ok(n) => refused(format!(
                "{n} ms is outside the valid range for parameter \"{pg_name}\" (0 ms .. 2147483647 ms)"
            )),
            Err(_) => invalid(),
        }
    };
    let accepted = match name.as_str() {
        "foreign_keys" => {
            let Some(on) = pg_bool(&lower) else {
                return invalid();
            };
            assign(&|session, local, is_local| {
                if is_local {
                    local.foreign_keys = Some(on);
                } else {
                    session.foreign_keys = on;
                    local.foreign_keys = None;
                }
            })?;
            true
        }
        // These change answers PostgreSQL gives, so they act, or are refused, rather than being
        // accepted as no-ops (wire review 16 item 12): lock_timeout is the session's lock wait
        // (0 the server's), client_min_messages filters the messages sent, and the two timeouts
        // this server does not enforce are accepted at 0 alone.
        "lock_timeout" => {
            let Some(ms) = setting_ms(&text) else {
                return out_of_range_ms();
            };
            assign(&|session, local, is_local| {
                if is_local {
                    local.lock_timeout_ms = Some(ms);
                } else {
                    session.lock_timeout_ms = ms;
                    local.lock_timeout_ms = None;
                }
            })?;
            true
        }
        "client_min_messages" => {
            let Some(rank) = message_rank(&text) else {
                return invalid();
            };
            assign(&|session, local, is_local| {
                if is_local {
                    local.min_messages = Some(rank);
                } else {
                    session.min_messages = rank;
                    local.min_messages = None;
                }
            })?;
            true
        }
        // The server stops a statement running past it (57014).
        "statement_timeout" => {
            let Some(ms) = setting_ms(&text) else {
                return out_of_range_ms();
            };
            assign(&|session, local, is_local| {
                if is_local {
                    local.statement_timeout_ms = Some(ms);
                } else {
                    session.statement_timeout_ms = ms;
                    local.statement_timeout_ms = None;
                }
            })?;
            true
        }
        "idle_in_transaction_session_timeout" | "transaction_timeout" => {
            match setting_ms(&text) {
                Some(0) => true,
                Some(_) => {
                    return unsupported("this server enforces no such timeout; 0 is accepted")
                }
                None => return invalid(),
            }
        }
        // A pg_dump header's settings, each accepted at the value that changes nothing here (wire
        // review 16 item 17): XML values are content (this server has no XML type), the default
        // tablespace is none and every table is the engine's own.
        "xmloption" => match lower.as_str() {
            "content" => true,
            "document" => false,
            _ => return invalid(),
        },
        // PostgreSQL refuses a tablespace or an access method that does not exist (22023), and
        // this server has none but the ones every cluster has: pg_default (the default, so a no-op)
        // and pg_global, which PostgreSQL accepts as a SET (review c39799c5d..9e24b455d LOW 6,
        // pg18-rev-c39-misc.out).
        "default_tablespace" => {
            matches!(lower.as_str(), "" | "pg_default" | "pg_global") || return invalid()
        }
        "default_table_access_method" => lower == "heap" || return invalid(),
        "transaction_read_only" | "default_transaction_read_only" => match pg_bool(&text) {
            Some(false) => true,
            None => return not_boolean(),
            Some(true) => {
                return Err(LimboError::ParseError(
                    SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "READ ONLY transactions are not supported".to_string()),
                ))
            }
        },
        "client_encoding" => {
            let clean: String = lower.chars().filter(char::is_ascii_alphanumeric).collect();
            if !PG_ENCODINGS.contains(&clean.as_str()) {
                return invalid();
            }
            clean == "utf8" || clean == "unicode"
        }
        "standard_conforming_strings" => match pg_bool(&text) {
            Some(on) => on,
            None => return not_boolean(),
        },
        "bytea_output" => match lower.as_str() {
            "hex" => true,
            "escape" => false,
            _ => return invalid(),
        },
        // Every element ISO, MDY or PostgreSQL's other names for that order (US, NonEuro...), or
        // DEFAULT, PostgreSQL's ISO, MDY: what SHOW gives stays true. DMY, YMD and the SQL,
        // Postgres and German output styles are refused, as this server reads and writes ISO
        // dates alone; `ISO, DMY` was accepted and SHOW still said MDY (wire review 18 item 19).
        "datestyle" => {
            let parts: Vec<&str> = lower.split(',').map(str::trim).collect();
            // PostgreSQL's check_datestyle words; any other is 22023.
            let known = |p: &str| {
                matches!(
                    p,
                    "iso" | "sql" | "postgres" | "german" | "ymd" | "dmy" | "mdy" | "us"
                        | "european" | "default"
                ) || p.starts_with("noneuro")
                    || p.starts_with("euro")
            };
            if !parts.iter().all(|p| known(p)) {
                return invalid();
            }
            // check_datestyle's conflicts: two output styles, or two field orders, are 22023,
            // before any style this server does not act on is refused (review c39799c5d..9e24b455d
            // LOW 8: `ISO, SQL` and `MDY, DMY` were 0A000; pg18-rev-c39-misc.out).
            let style = |p: &str| matches!(p, "iso" | "sql" | "postgres" | "german");
            let order = |p: &str| {
                matches!(p, "ymd" | "dmy" | "mdy" | "us" | "european")
                    || p.starts_with("noneuro")
                    || p.starts_with("euro")
            };
            let distinct = |f: &dyn Fn(&str) -> bool| {
                let mut seen: Vec<&str> = parts.iter().copied().filter(|p| f(p)).collect();
                // US and NonEuro are MDY; Euro and European are DMY.
                for p in seen.iter_mut() {
                    *p = match *p {
                        "us" => "mdy",
                        "european" => "dmy",
                        q if q.starts_with("noneuro") => "mdy",
                        q if q.starts_with("euro") => "dmy",
                        q => q,
                    };
                }
                seen.sort_unstable();
                seen.dedup();
                seen.len() <= 1
            };
            if !distinct(&style) || !distinct(&order) {
                return invalid();
            }
            parts.iter().all(|part| {
                matches!(*part, "iso" | "mdy" | "us" | "default") || part.starts_with("noneuro")
            })
        }
        "intervalstyle" => match lower.as_str() {
            "postgres" => true,
            "postgres_verbose" | "sql_standard" | "iso_8601" => false,
            _ => return invalid(),
        },
        "timezone" => {
            let utc = matches!(
                lower.as_str(),
                "utc" | "gmt" | "etc/utc" | "etc/gmt" | "z" | "zulu" | "uct" | "universal" | "0"
            );
            // A zone PostgreSQL would know, as its check_timezone reads it (review
            // c39799c5d..9e24b455d LOW 7, pg18-rev-c39-tzforms.out): an INTERVAL; a number of
            // hours read whole as strtod reads it (`--5` and `inf` refused, `nan` and `5.5`
            // accepted); a name in the host's zone database, matched without case as PostgreSQL
            // matches it; or a POSIX TZ spec (`UTC+3`, `EST5EDT,M3.2.0,M11.1.0`, `+05:30`). A valid
            // zone other than UTC is refused 0A000 below, as this server runs in UTC alone.
            // BLIND SPOTS: a host zone database lacking a name PostgreSQL knows answers 22023 for
            // it; PostgreSQL's range bound for a numeric offset is not recorded, so any finite
            // number or NaN is valid here.
            // UNVERIFIED beyond the recorded rows (review wire-rework C, finding (1)): the
            // interval, strtod and tzcode grammar below follow PostgreSQL's check_timezone and
            // tzparse as read, until one strict PG 18.6 recording of every case (LOUD-OWED).
            // SET TIME ZONE INTERVAL '...' [fields] arrives as its own value (the wire line review
            // M6: it arrived as the bare literal, quotes and all, which posix_tz read as a name);
            // a TimeZone string starting "interval" is a zone name like any other (PG 18.6:
            // 'interval' and 'interval x' are 22023, pg18-rev-cd-tz2.out).
            if let PgSetValue::Interval { literal, range } = value {
                return match timezone_interval(literal, *range) {
                    TzInterval::Offset => unsupported("this server answers only by its own setting of it"),
                    // check_timezone refuses months and days with the setting's own message
                    // (PG 18.6: invalid value for parameter "TimeZone": "INTERVAL '1 day'").
                    TzInterval::NotAllowed => invalid(),
                    TzInterval::Malformed => Err(LimboError::ParseError(SqlErrorMessage::classified(
                        SqlErrorKind::InvalidDatetimeFormat,
                        format!("invalid input syntax for type interval: \"{literal}\""),
                    ))),
                };
            }
            let numeric = strtod_whole(&lower).is_some_and(|h| h.is_nan() || h.abs() < 168.0);
            let valid = utc || numeric || zone_file(&text) || posix_tz(&text);
            if !valid {
                return invalid();
            }
            utc
        }
        "extra_float_digits" => match lower.parse::<i32>() {
            Ok(d) if (-15..=3).contains(&d) => d >= 1,
            Ok(d) => {
                return refused(format!(
                    "{d} is outside the valid range for parameter \"extra_float_digits\" (-15 .. 3)"
                ))
            }
            Err(_) => return invalid(),
        },
        n if NOOP_PARAMETERS.contains(&n) || n.starts_with("enable_") => true,
        n if n.contains('.') => {
            return unsupported("custom configuration parameters are not supported")
        }
        // One PostgreSQL knows is refused as one this server does not act on, not as one nobody
        // knows: 42704 claimed PostgreSQL would not know it (wire review 16 item 17).
        n if crate::pg_settings::is_postgresql_parameter(n) => {
            return unsupported("this server does not act on it")
        }
        _ => {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedObject, format!(
                "unrecognized configuration parameter \"{name}\""
            ))))
        }
    };
    if accepted {
        noop_statement(&pg_conn.conn)
    } else {
        unsupported("this server answers only by its own setting of it")
    }
}

/// PostgreSQL's spelling of a parameter name, as its errors quote it (`DateStyle`, not `datestyle`).
fn pg_parameter_name(name: &str) -> &str {
    match name {
        "datestyle" => "DateStyle",
        "intervalstyle" => "IntervalStyle",
        "timezone" => "TimeZone",
        other => other,
    }
}

/// PostgreSQL's encoding names and aliases, letters and digits only (pg_wchar.c's table, as
/// client_encoding accepts them): any other is 22023.
/// Whether `name` is a zone in the host's zone database: only name components of letters, digits,
/// `_`, `-` and `+` (no leading `/`, no `.` or `..`, nothing empty), joined under
/// /usr/share/zoneinfo, naming a TZif file, and not `posixrules`, which PostgreSQL refuses by name.
/// A value that is a host path never reaches the filesystem, so no answer says whether a host file
/// exists (review c39799c5d..9e24b455d MED 2: `/etc/passwd` replaced the base in `join` and
/// answered 0A000, `/etc/nosuch` 22023), and a zone-directory file that is not a zone
/// (`zone.tab`, `tzdata.zi`) is not one; PG 18.6 answers 22023 for all of them
/// (pg18-rev-c39-tzpath.out).
fn zone_file(name: &str) -> bool {
    use std::io::Read;
    let named = name.len() <= 255
        && name.split('/').all(|c| {
            !c.is_empty()
                && c.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+'))
        });
    if !named || name.eq_ignore_ascii_case("posixrules") {
        return false;
    }
    // Each component matched without case, as PostgreSQL matches zone names (a case-sensitive
    // host filesystem found `America/New_York` and not `america/new_york`).
    let mut path = std::path::PathBuf::from("/usr/share/zoneinfo");
    for part in name.split('/') {
        let exact = path.join(part);
        if exact.exists() {
            path = exact;
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&path) else {
            return false;
        };
        let Some(found) = entries
            .flatten()
            .find(|e| e.file_name().to_str().is_some_and(|f| f.eq_ignore_ascii_case(part)))
        else {
            return false;
        };
        path = found.path();
    }
    let mut magic = [0u8; 4];
    std::fs::File::open(&path)
        .and_then(|mut f| f.read_exact(&mut magic))
        .is_ok()
        && &magic == b"TZif"
}

/// A TimeZone value's `INTERVAL '...' [fields]` form, as check_timezone reads it: an offset with
/// no months or days, or refused.
enum TzInterval {
    /// A valid offset.
    Offset,
    /// Months or days given: 22023, the setting's own refusal.
    NotAllowed,
    /// A literal interval_in refuses: 22007.
    Malformed,
}

/// `literal` is the interval's string, `range` its fields' INTERVAL_RANGE mask (None: no fields;
/// read, not yet used). The literal is `[+-]h[:m[:s]]`, a bare number, or a list of
/// `<number> <unit>`, where a non-zero year, month or day is not allowed. PG 18.6 (pg18-rev-cd-tz2.out): '1 day' and
/// '1 month' 22023, 'x' 22007, '+05:00' HOUR TO MINUTE valid. BLIND SPOT: interval_in's full
/// grammar (ISO 8601 `PT5H`, `@` forms, fractional units) reads as Malformed here.
fn timezone_interval(literal: &str, range: Option<i32>) -> TzInterval {
    let literal = literal.trim();
    if literal.is_empty() {
        return TzInterval::Malformed;
    }
    // A bare number is an offset: which unit the fields give it (`INTERVAL '1' DAY`) is not
    // recorded, so no refusal is read into it (the lead: no rule a recollection alone supports;
    // owed in the L1 recording).
    let _ = range;
    if literal.parse::<f64>().is_ok() {
        return TzInterval::Offset;
    }
    let clock = |t: &str| {
        let t = t.trim_start_matches(['+', '-']);
        let parts: Vec<&str> = t.split(':').collect();
        parts.len() <= 3 && parts.iter().all(|p| !p.is_empty() && p.parse::<f64>().is_ok())
    };
    if clock(literal) {
        return TzInterval::Offset;
    }
    let words: Vec<&str> = literal.split_whitespace().collect();
    if words.len() % 2 != 0 {
        return TzInterval::Malformed;
    }
    let mut date = false;
    for pair in words.chunks(2) {
        let Ok(n) = pair[0].parse::<f64>() else {
            return TzInterval::Malformed;
        };
        let unit = pair[1].to_ascii_lowercase();
        let unit = unit.trim_end_matches('s');
        match unit {
            "hour" | "minute" | "min" | "second" | "sec" => {}
            "day" | "month" | "mon" | "year" => date |= n != 0.0,
            _ => return TzInterval::Malformed,
        }
    }
    if date {
        TzInterval::NotAllowed
    } else {
        TzInterval::Offset
    }
}

/// `text` read whole as C's strtod reads a number (leading whitespace skipped; decimal, `0x` hex,
/// `inf`/`infinity`, `nan` and `nan(...)`), or None when anything is left after it.
fn strtod_whole(text: &str) -> Option<f64> {
    let t = text.trim_start();
    let (sign, body) = match t.as_bytes().first() {
        Some(b'-') => (-1.0, &t[1..]),
        Some(b'+') => (1.0, &t[1..]),
        _ => (1.0, t),
    };
    if body.starts_with(['-', '+']) {
        return None;
    }
    let lower = body.to_ascii_lowercase();
    if let Some(hex) = lower.strip_prefix("0x") {
        let (int, frac) = hex.split_once('.').unwrap_or((hex, ""));
        if (int.is_empty() && frac.is_empty())
            || !int.bytes().chain(frac.bytes()).all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        let mut v = 0.0f64;
        for c in int.chars() {
            v = v * 16.0 + c.to_digit(16)? as f64;
        }
        let mut scale = 1.0 / 16.0;
        for c in frac.chars() {
            v += c.to_digit(16)? as f64 * scale;
            scale /= 16.0;
        }
        return Some(sign * v);
    }
    if lower == "nan" || (lower.starts_with("nan(") && lower.ends_with(')')
        && lower[4..lower.len() - 1].bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
    {
        return Some(f64::NAN);
    }
    if lower == "inf" || lower == "infinity" {
        return Some(sign * f64::INFINITY);
    }
    // Rust's parse also takes `infinity`/`nan` spellings and no other trailing text.
    let ok = !body.is_empty()
        && body.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-'));
    ok.then(|| body.parse::<f64>().ok()).flatten().map(|v| sign * v)
}

/// Whether `spec` is a POSIX TZ string as tzcode's tzparse reads one (UNVERIFIED beyond the
/// recorded rows, review wire-rework C finding (1)): a standard name (`<...>`, non-empty, or
/// getzname's run of characters other than digits, `,`, `-` and `+`), an offset
/// `[+-]h[:m[:s]]` with hours at most 167 and minutes and seconds at most 59 (getsecs; any digit
/// count), then optionally a daylight name, its own offset, and two rules after `,` or `;`, each
/// `Jn` (1-365), `n` (0-365) or `Mm.w.d` (1-12, 1-5, 0-6) with an optional `/offset`. A bare
/// offset alone (`+05:30`) is a whole spec, as PG 18.6 answered it (pg18-rev-c39-tzforms.out).
fn posix_tz(spec: &str) -> bool {
    let b = spec.as_bytes();
    let name = |i: &mut usize| -> bool {
        if b.get(*i) == Some(&b'<') {
            // An empty `<>` is a name too (PG 18.6: '<>5' is SET, pg18-rev-cd-tz2.out).
            let Some(end) = spec[*i + 1..].find('>') else { return false };
            *i += end + 2;
            return true;
        }
        let start = *i;
        while b
            .get(*i)
            .is_some_and(|c| !c.is_ascii_digit() && !matches!(c, b',' | b'-' | b'+'))
        {
            *i += 1;
        }
        *i > start
    };
    // getsecs: h[:m[:s]]. The bounds are the recorded ones (pg18-rev-cd-tz2.out): hours up to 167
    // (UTC+167 SET, UTC+168 refused), minutes up to 59 (UTC+3:59 SET, UTC+3:60 refused); seconds
    // have no recorded bound and none is applied (the lead: no rule a recollection alone supports).
    let secs = |i: &mut usize| -> bool {
        let limits = [167u32, 59, u32::MAX];
        for (g, max) in limits.iter().enumerate() {
            let start = *i;
            let mut v: u64 = 0;
            while let Some(c) = b.get(*i).filter(|c| c.is_ascii_digit()) {
                v = v.saturating_mul(10).saturating_add(u64::from(c - b'0'));
                *i += 1;
            }
            if *i == start || v > u64::from(*max) {
                return false;
            }
            if g < 2 && b.get(*i) == Some(&b':') {
                *i += 1;
                continue;
            }
            return true;
        }
        true
    };
    let offset = |i: &mut usize| -> bool {
        if matches!(b.get(*i), Some(b'+' | b'-')) {
            *i += 1;
        }
        secs(i)
    };
    let mut i = 0;
    let had_name = name(&mut i);
    if !had_name {
        i = 0;
        // A bare offset alone: hours and minutes only (PG 18.6: '05:30' and '+5:3' are SET,
        // '+05:30:15' is refused, pg18-rev-cd-tz2.out).
        let bare = spec.trim_start_matches(['+', '-']);
        if bare.split(':').count() > 2 {
            return false;
        }
    }
    if !offset(&mut i) {
        return false;
    }
    if i == b.len() {
        return true;
    }
    if !had_name || !name(&mut i) {
        return false;
    }
    if i < b.len() && b[i] != b',' && !offset(&mut i) {
        return false;
    }
    if i == b.len() {
        return true;
    }
    // A rule's shape only: its fields' ranges are not recorded, so none is applied (the lead: no
    // rule a recollection alone supports); a time of four groups is refused (recorded).
    let digits = |t: &str| !t.is_empty() && t.bytes().all(|c| c.is_ascii_digit());
    let rule = |r: &str| -> bool {
        let (date, time) = r.split_once('/').unwrap_or((r, "0"));
        let date_ok = if let Some(m) = date.strip_prefix('M') {
            let f: Vec<&str> = m.split('.').collect();
            f.len() == 3 && f.iter().all(|x| digits(x))
        } else {
            digits(date.strip_prefix('J').unwrap_or(date))
        };
        let time = time.strip_prefix(['+', '-']).unwrap_or(time);
        let groups: Vec<&str> = time.split(':').collect();
        date_ok && groups.len() <= 3 && groups.iter().all(|x| digits(x))
    };
    // The rules follow a `,` only (PG 18.6: 'EST5EDT;M3.2.0;M11.1.0' is refused).
    if b[i] != b',' {
        return false;
    }
    let rules: Vec<&str> = spec[i + 1..].split(',').collect();
    rules.len() == 2 && rules.iter().all(|r| rule(r))
}

const PG_ENCODINGS: &[&str] = &[
    "sqlascii", "utf8", "unicode", "eucjp", "euccn", "euckr", "euctw", "eucjis2004",
    "muleinternal", "latin1", "latin2", "latin3", "latin4", "latin5", "latin6", "latin7",
    "latin8", "latin9", "latin10", "win1256", "win1258", "win866", "win874", "koi8r", "koi8",
    "win1251", "win1252", "iso88595", "iso88596", "iso88597", "iso88598", "win1250", "win1253",
    "win1254", "win1255", "win1257", "koi8u", "sjis", "big5", "gbk", "uhc", "gb18030", "johab",
    "shiftjis2004", "alt", "win", "mskanji", "shiftjis", "tcvn", "tcvn5712", "vscii", "abc",
    "iso885910", "iso885913", "iso885914", "iso885915", "iso885916", "iso88591", "iso88592",
    "iso88593", "iso88594", "iso88599", "win932", "win936", "win949", "win950",
    "windows1250", "windows1251", "windows1252", "windows1253", "windows1254", "windows1255",
    "windows1256", "windows1257", "windows1258", "windows866", "windows874", "windows932",
    "windows936", "windows949", "windows950",
];

/// SHOW of `show`'s parameter, from the settings SET keeps (see [`handle_pg_set`]; wire review 16
/// item 4), one text row under PostgreSQL's name for it: search_path from the session's own path,
/// each name quoted as PostgreSQL quotes it (`"$user", public`, PostgreSQL's default, when none was
/// set); foreign_keys from the engine; transaction_read_only and default_transaction_read_only
/// off; the client settings at the values this server answers by. SHOW ALL is 0A000, and any other
/// name 42704, as PostgreSQL answers a name it does not know (one it knows that this list omits,
/// NOOP_PARAMETERS among them, gets 42704 too: their values are not kept). Every SHOW became
/// `PRAGMA <name>` from the client's identifier, which the engine performed while compiling, a
/// Describe included: `SHOW "synchronous = off"` set the sync mode, `SHOW "fullfsync = off"`
/// turned F_FULLFSYNC into fsync, `SHOW wal_checkpoint` checkpointed.
fn handle_pg_show(pg_conn: &Arc<PgConnectionInner>, show: &PgShowStmt) -> Result<Statement> {
    let name = show.name.to_ascii_lowercase();
    let fixed = |column: &'static str, value: &'static str| (column, value.to_string());
    let setting = FIXED_SETTINGS
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(&name));
    let (column, value) = match name.as_str() {
        "search_path" => {
            let shown = shown_search_path(&pg_conn.session_state.lock().search_path);
            ("search_path", shown)
        }
        "foreign_keys" => {
            let mut stmt = pg_conn.conn.prepare(catalog::sqlite_text("PRAGMA foreign_keys"))?;
            let rows = stmt.run_collect_rows()?;
            let on = rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.as_int())
                == Some(1);
            ("foreign_keys", if on { "on" } else { "off" }.to_string())
        }
        "lock_timeout" => ("lock_timeout", shown_ms(pg_conn.session_state.lock().gucs.effective().lock_timeout_ms)),
        "statement_timeout" => (
            "statement_timeout",
            shown_ms(pg_conn.session_state.lock().gucs.effective().statement_timeout_ms),
        ),
        "client_min_messages" => (
            "client_min_messages",
            message_level(pg_conn.session_state.lock().gucs.effective().min_messages).to_string(),
        ),
        "transaction_read_only" => fixed("transaction_read_only", "off"),
        "default_transaction_read_only" => fixed("default_transaction_read_only", "off"),
        _ if setting.is_some() => {
            let &(column, value) = setting.expect("matched above");
            fixed(column, value)
        }
        "all" => {
            return Err(LimboError::ParseError(
                SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "SHOW ALL is not supported".to_string()),
            ))
        }
        _ if crate::pg_settings::is_postgresql_parameter(&name) => {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, format!(
                "SHOW {name} is not supported: this server does not act on it"
            ))))
        }
        _ => {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedObject, format!(
                "unrecognized configuration parameter \"{name}\""
            ))))
        }
    };
    pg_conn.conn.prepare(format!(
        "SELECT '{}' AS \"{}\"",
        value.replace('\'', "''"),
        column
    ))
}

fn handle_pg_create_schema(conn: &Arc<Connection>, stmt: &PgCreateSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        ).into()));
    }

    if schema_exists(conn, &name)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        ).into()));
    }

    let path = schema_file_path(conn, &name);
    execute_sqlite_internal(
        conn,
        catalog::sqlite_text(format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name)),
    )?;
    Ok(())
}

fn schema_file_path(conn: &Connection, schema_name: &str) -> String {
    let main_path = conn.db_file_path();
    let filename = format!("turso-postgres-schema-{schema_name}.db");
    if main_path == ":memory:" {
        filename
    } else {
        let parent = std::path::Path::new(&main_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        parent.join(&filename).to_string_lossy().to_string()
    }
}

fn handle_pg_drop_schema(pg_conn: &Arc<PgConnectionInner>, stmt: &PgDropSchemaStmt) -> Result<()> {
    let conn = &pg_conn.conn;
    let name = stmt.name.to_lowercase();
    if name == "public" {
        return handle_pg_drop_schema_public(conn, stmt.cascade);
    }

    if !schema_exists(conn, &name)? {
        if stmt.if_exists {
            // PostgreSQL's notice for the missing schema (recorded, pg18-c53-dropnotice.out).
            skipping_notice(pg_conn, "schema", &stmt.name);
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        ).into()));
    }

    if stmt.cascade {
        drop_all_tables_in_schema(conn, &name)?;
    }

    execute_sqlite_internal(conn, catalog::sqlite_text(format!("DETACH \"{name}\"")))?;
    Ok(())
}

/// PostgreSQL's NOTICE for an object DROP ... IF EXISTS did not find: `<kind> "<name>" does not
/// exist, skipping`, the name as written, without its schema (recorded, pg18-c53-dropnotice.out).
fn skipping_notice(pg_conn: &Arc<PgConnectionInner>, kind: &str, name: &str) {
    pg_conn.warnings.lock().push((
        "NOTICE",
        SqlErrorKind::SuccessfulCompletion,
        format!("{kind} \"{name}\" does not exist, skipping"),
    ));
}

/// [`PgDropObjectsStmt`] as PostgreSQL's RemoveRelations drops it: every object resolved first,
/// each missing one a NOTICE under IF EXISTS (else the engine's own refusal, before anything is
/// dropped), then every found one dropped, all or none. An object is found when the engine can
/// prepare its DROP without IF EXISTS: the engine's own name resolution (search path, attached
/// schemas) decides, so this never disagrees with the DROP that follows. A schema that is not
/// attached is missing too, with PostgreSQL's schema notice. A table that is a view, or a view that
/// is a table, is refused as PostgreSQL refuses it (`"v" is not a table`), never skipped. BLIND
/// SPOT: "missing" is read from the engine's refusal text ([`drop_refused_as_missing`]); a refusal
/// worded otherwise propagates as an error, never as a skip.
fn handle_pg_drop_objects(pg_conn: &Arc<PgConnectionInner>, stmt: &PgDropObjectsStmt) -> Result<()> {
    let conn = &pg_conn.conn;
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let mut found = Vec::with_capacity(stmt.objects.len());
    for (schema, name) in &stmt.objects {
        if let Some(schema) = schema.as_deref().filter(|s| *s != "main") {
            if !schema_exists(conn, schema)? {
                if stmt.missing_ok {
                    skipping_notice(pg_conn, "schema", schema);
                    continue;
                }
                return Err(LimboError::ParseError(
                    format!("schema \"{schema}\" does not exist").into(),
                ));
            }
        }
        // A type is named unqualified in the engine (the translator's DROP TYPE kept the last name).
        let unqualified = matches!(stmt.kind, PgDropKind::Type | PgDropKind::Domain);
        let target = match schema.as_deref() {
            Some(schema) if !(unqualified && schema == "main") => {
                format!("{}.{}", quote(schema), quote(name))
            }
            _ => quote(name),
        };
        let drop = |keyword: &str| format!("DROP {keyword} {target}");
        let sql = drop(stmt.kind.keyword());
        match conn.prepare(&sql) {
            Ok(_) => found.push(sql),
            Err(e) if drop_refused_as_missing(stmt.kind, &e) => {
                // The other relation kind of the name: PostgreSQL refuses the wrong kind.
                let other = match stmt.kind {
                    PgDropKind::Table => Some(PgDropKind::View),
                    PgDropKind::View => Some(PgDropKind::Table),
                    _ => None,
                };
                if let Some(other) = other {
                    if conn.prepare(drop(other.keyword())).is_ok() {
                        return Err(LimboError::ParseError(
                            format!("\"{name}\" is not a {}", stmt.kind.word()).into(),
                        ));
                    }
                }
                if !stmt.missing_ok {
                    return Err(e);
                }
                skipping_notice(pg_conn, stmt.kind.word(), name);
            }
            Err(e) => return Err(e),
        }
    }
    // All or none: several drops run inside a savepoint of their own (in the session's
    // transaction, or as one of their own).
    if found.len() < 2 {
        return found
            .iter()
            .try_for_each(|sql| conn.prepare(sql)?.run_ignore_rows());
    }
    execute_sqlite_internal(conn, "SAVEPOINT turso_pg_drop_objects")?;
    let dropped = found
        .iter()
        .try_for_each(|sql| conn.prepare(sql)?.run_ignore_rows());
    match dropped {
        Ok(()) => execute_sqlite_internal(conn, "RELEASE turso_pg_drop_objects"),
        Err(e) => {
            let _ = execute_sqlite_internal(conn, "ROLLBACK TO turso_pg_drop_objects");
            let _ = execute_sqlite_internal(conn, "RELEASE turso_pg_drop_objects");
            Err(e)
        }
    }
}

/// Whether the engine refused a DROP of `kind` without IF EXISTS because the object is missing,
/// by the engine's own words for it (core translate: drop_table, drop_view, drop_index, sequence,
/// drop_type).
fn drop_refused_as_missing(kind: PgDropKind, e: &LimboError) -> bool {
    match (kind, e) {
        (PgDropKind::Table, LimboError::ParseError(m)) => m.starts_with("no such table"),
        (PgDropKind::View, LimboError::ParseError(m)) => m.starts_with("no such view"),
        (PgDropKind::Index, LimboError::InvalidArgument(m)) => m.starts_with("No such index"),
        (PgDropKind::Sequence, LimboError::ParseError(m)) => {
            m.starts_with("sequence \"") && m.ends_with("\" does not exist")
        }
        (PgDropKind::Type, LimboError::ParseError(m)) => m.starts_with("no such type"),
        (PgDropKind::Domain, LimboError::ParseError(m)) => m.starts_with("no such domain"),
        _ => false,
    }
}

fn handle_pg_drop_schema_public(conn: &Arc<Connection>, cascade: bool) -> Result<()> {
    let table_names = list_user_tables(conn, None)?;
    if !cascade && !table_names.is_empty() {
        return Err(LimboError::ParseError(
            "cannot drop schema \"public\" because other objects depend on it".to_string().into(),
        ));
    }

    for table_name in table_names {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{table_name}\""))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

fn drop_all_tables_in_schema(conn: &Arc<Connection>, schema_name: &str) -> Result<()> {
    for table_name in list_user_tables(conn, Some(schema_name))? {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{schema_name}\".\"{table_name}\"",))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

/// `ALTER TABLE t ADD <constraint>...` ([`PgAddConstraints`]): the engine adds no constraint to an
/// existing table, so the table is rebuilt from its own PostgreSQL definition with the
/// constraints appended, atomically (in the session's transaction, or in one of its own): its
/// rows are copied aside, the table is dropped and created anew, the rows are inserted back (each
/// checked against the new PRIMARY KEY, UNIQUE and CHECK constraints), and its indexes and
/// triggers are created again, all with foreign keys off, so its children and the connection's
/// deferred-key count are untouched. Then each foreign key the ALTER adds is checked as
/// PostgreSQL checks it: its parent exists (42P01), its parent key is the parent's primary key or
/// a unique one with as many columns (42830), and no row is an orphan (23503). A table created by
/// CREATE TABLE AS, or not through this frontend, has no definition to rebuild from: refused.
fn handle_pg_add_constraints(
    pg_conn: &Arc<PgConnectionInner>,
    add: &PgAddConstraints,
) -> Result<()> {
    let conn = &pg_conn.conn;
    if add.schema.as_deref().is_some_and(|s| {
        turso_pg_parser::schema_qualifier(s) != turso_pg_parser::SchemaQualifier::Main
    }) {
        return Err(LimboError::ParseError(
            "ALTER TABLE ADD CONSTRAINT is supported only for tables in schema public".to_string().into(),
        ));
    }
    let table = add.table.as_str();
    let mut definition = None;
    let mut dependents = Vec::new();
    // The catalog lookup is an internal helper statement, which keeps the connection nested until
    // it is dropped; every statement of the rebuild run while nested opens no transaction of its
    // own, and its DDL reaches SetCookie with none (wire review 2 item 1). So the lookup's rows are
    // collected and the statement dropped here, before the rebuild's first statement.
    {
        let mut stmt = conn.prepare_internal(catalog::sqlite_text(
            "SELECT type, sql FROM sqlite_schema WHERE tbl_name = ?1 AND sql IS NOT NULL \
             ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, rowid",
        ))?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(table.to_string()),
        )?;
        for row in stmt.run_collect_rows()? {
            let (Some(Value::Text(kind)), Some(Value::Text(sql))) = (row.first(), row.get(1))
            else {
                continue;
            };
            match kind.as_str() {
                "table" => definition = Some(sql.as_str().to_string()),
                "index" | "trigger" => dependents.push(sql.as_str().to_string()),
                _ => {}
            }
        }
    }
    // The aside table's name: not under the engine's reserved prefixes, which a client statement
    // may not create; no longer than PostgreSQL's 63-byte identifiers, which libpg_query cuts in
    // the aside's CREATE while the copy and the DROP below name it in full; and the name of no
    // table, index, view or trigger in sqlite_schema (a user's object of that name is never
    // touched). Probed here, before the rebuild, for the same reason as the lookup above (wire
    // review 3 item 11).
    let mut aside_table_name = None;
    for n in 0..100 {
        let name = aside_name_for(table, n);
        let taken = {
            let mut stmt =
                conn.prepare_internal(catalog::sqlite_text("SELECT 1 FROM sqlite_schema WHERE lower(name) = lower(?1)"))?;
            stmt.bind_at(NonZero::new(1).unwrap(), Value::build_text(name.clone()))?;
            !stmt.run_collect_rows()?.is_empty()
        };
        if !taken && conn.current_schema().get_table(&name).is_none() {
            aside_table_name = Some(name);
            break;
        }
    }
    if conn.is_nested_stmt() {
        return Err(LimboError::InternalError(
            "ALTER TABLE ADD CONSTRAINT: the connection is inside another statement, so the \
             rebuild's statements would run without their own transaction"
                .to_string(),
        ));
    }
    let definition = definition.ok_or_else(|| {
        LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::UndefinedTable,
            format!("relation \"{table}\" does not exist"),
        ))
    })?;
    let pg_definition = catalog::decode_stored_pg_schema_sql(&definition).ok_or_else(|| {
        LimboError::ParseError(
            format!(
                "ALTER TABLE ADD CONSTRAINT: table \"{table}\" was not created by CREATE TABLE in \
             this frontend, so it has no definition to rebuild"
            )
            .into(),
        )
    })?;
    let mut parsed =
        turso_pg_parser::parse(pg_definition).map_err(|e| LimboError::ParseError(e.into()))?;
    // Taken before the definition is edited below (a clone made while `create` borrows it would
    // not compile).
    let mut aside_tree = parsed.protobuf.clone();
    let create = parsed
        .protobuf
        .stmts
        .first_mut()
        .and_then(|raw| raw.stmt.as_mut())
        .and_then(|s| s.node.as_mut());
    let Some(turso_pg_parser::pg_query::protobuf::node::Node::CreateStmt(create)) = create else {
        return Err(LimboError::ParseError(format!(
            "ALTER TABLE ADD CONSTRAINT: table \"{table}\" was created by CREATE TABLE AS, so it \
             has no column definitions to rebuild"
        ).into()));
    };
    create.if_not_exists = false;
    // The rows wait in a table of the same columns and types, so each value is decoded and
    // encoded by its own type both ways and reaches the rebuilt table as it was stored. (A copy
    // made by CREATE TABLE AS would take its column types from the decoded values.)
    let aside_name = aside_table_name.ok_or_else(|| {
        LimboError::InternalError(format!(
            "ALTER TABLE ADD CONSTRAINT: no free name for the rebuild's aside table of \
                 \"{table}\""
        ))
    })?;
    if let Some(turso_pg_parser::pg_query::protobuf::node::Node::CreateStmt(c)) = aside_tree
        .stmts
        .first_mut()
        .and_then(|raw| raw.stmt.as_mut())
        .and_then(|s| s.node.as_mut())
    {
        c.if_not_exists = false;
        if let Some(relation) = c.relation.as_mut() {
            relation.relname = aside_name.clone();
            relation.schemaname.clear();
        }
        // A serial column of the aside is a plain integer, what every serial spelling is stored
        // as: as serial it would create a sequence named after the aside, which the aside's DROP
        // leaves behind (wire review 3 item 11); as int2 or int8 it became the engine's smallint,
        // whose encoding refuses values past 32767, or bigint, no longer the rowid key (wire
        // review 6 item 2).
        for elt in &mut c.table_elts {
            if let Some(turso_pg_parser::pg_query::protobuf::node::Node::ColumnDef(col)) =
                elt.node.as_mut()
            {
                if let Some(type_name) = col.type_name.as_mut() {
                    for part in &mut type_name.names {
                        if let Some(turso_pg_parser::pg_query::protobuf::node::Node::String(s)) =
                            part.node.as_mut()
                        {
                            if turso_pg_parser::translator::is_serial_type(&s.sval) {
                                s.sval = "int4".to_string();
                            }
                        }
                    }
                }
            }
        }
    }
    let aside_def =
        turso_pg_parser::deparse(&aside_tree).map_err(|e| LimboError::ParseError(e.into()))?;
    create.table_elts.extend(add.constraints.iter().cloned());
    let rebuilt =
        turso_pg_parser::deparse(&parsed.protobuf).map_err(|e| LimboError::ParseError(e.into()))?;
    // Every refusal the translator makes of the rebuilt CREATE TABLE (MATCH FULL over several
    // columns among them) is answered here, before anything is copied, dropped or locked: it came
    // from the rebuild's own CREATE, after the BEGIN, the aside copy and the DROP, under the trunk's
    // write lock, so a refused key cost an O(rows) rebuild, or 55P03 after the lock wait (wire
    // review 16 item 9). One translation of the tree already in hand, no parse.
    PostgreSQLTranslator::new()
        .translate_with_prereqs(&parsed)
        .map_err(|e| LimboError::ParseError(e.into()))?;

    let quoted = format!("\"{}\"", table.replace('"', "\"\""));
    let aside = format!("\"{}\"", aside_name.replace('"', "\"\""));
    // The keys the ALTER adds, each checked against the catalog before anything is copied or
    // dropped (a key onto the table itself as far as it can be before the table's own new keys
    // exist): a refused key cost two copies of the table and an index rebuild, and an arity the
    // engine refuses in the rebuilt CREATE TABLE was 42601 (wire review 13 item 4).
    let keys: Vec<AddedForeignKey> = add
        .constraints
        .iter()
        .filter_map(added_foreign_key)
        .collect();
    // A parent in another schema, as precheck_create_keys refuses one (wire review 16 item 11).
    if keys.iter().any(|k| {
        !matches!(
            turso_pg_parser::schema_qualifier(&k.parent_schema),
            turso_pg_parser::SchemaQualifier::Unqualified | turso_pg_parser::SchemaQualifier::Main
        )
    }) {
        return Err(LimboError::ParseError(
            SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "foreign keys in a schema other than public are not supported".to_string()),
        ));
    }
    let child_columns: Vec<String> = conn
        .current_schema()
        .get_btree_table(table)
        .map(|t| t.columns().iter().filter_map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    for key in &keys {
        check_foreign_key(conn, table, &child_columns, key, true)?;
    }
    // Whether the table's older keys resolve: if they do, the engine's own resolution after the
    // rebuild can refuse only for a key this ALTER adds, and backs the per-key checks.
    let resolved_before = keys.is_empty() || check_table_keys(conn, table).is_ok();
    let enforced = conn.foreign_keys_enabled();
    // In a transaction of its own, under one undo rule (own_transaction): its end, COMMIT or
    // RELEASE, fails into the same undo as its steps (a failed COMMIT returned without it left the
    // transaction open, the session 'T' holding the trunk's write lock; wire review 6 item 1), and
    // an undo that fails breaks the connection (it left the table half rebuilt).
    own_transaction(pg_conn, "__turso_rebuild", "ALTER TABLE ADD CONSTRAINT", || {
        // Every step is a root statement: an internal helper statement opens no transaction of its
        // own (DDL through one panicked the engine at SetCookie) and skips foreign key checks.
        let rebuild = (|| {
            conn.set_foreign_keys_enabled(false);
            run_pg_statement(pg_conn, &aside_def)?;
            execute_root(conn, format!("INSERT INTO {aside} SELECT * FROM {quoted}"))?;
            execute_root(conn, format!("DROP TABLE {quoted}"))?;
            run_pg_statement(pg_conn, &rebuilt)?;
            // The copy back checks every row against the new PRIMARY KEY, UNIQUE and CHECK
            // constraints. It runs with foreign keys off, as the DROP does, so neither half touches
            // the connection's pending deferred-key count: with keys on it counted the rows again,
            // and a block's deferred orphan was cancelled, or a fixed one counted twice (wire
            // review 6 item 1).
            execute_root(conn, format!("INSERT INTO {quoted} SELECT * FROM {aside}"))?;
            execute_root(conn, format!("DROP TABLE {aside}"))?;
            for sql in &dependents {
                match catalog::decode_stored_pg_schema_sql(sql) {
                    Some(pg_sql) => run_pg_statement(pg_conn, pg_sql)?,
                    None => execute_root(conn, sql)?,
                }
            }
            // The keys the ALTER adds are checked whole once the table's indexes are back (a unique
            // index the ALTER adds can be a self-reference's parent key, and the orphan query can
            // use the indexes): each key on its own (check_foreign_key), then, when the table's
            // older keys resolved, the engine's own resolution of every key with unique parent keys
            // required (with keys off, a non-unique parent key was accepted, and every later INSERT
            // into the table failed 'foreign key mismatch'; wire review 11 item 5), then the
            // orphans (23503).
            let mut parent_keys = Vec::with_capacity(keys.len());
            for key in &keys {
                parent_keys.push(check_foreign_key(conn, table, &child_columns, key, false)?);
            }
            if !keys.is_empty() && resolved_before {
                check_table_keys(conn, table)?;
            }
            for (key, parent_columns) in keys.iter().zip(&parent_keys) {
                check_added_foreign_key(conn, &quoted, key, parent_columns)?;
            }
            Ok(())
        })();
        conn.set_foreign_keys_enabled(enforced);
        rebuild
    })
}

/// A FOREIGN KEY an ALTER adds: its columns, the parent table and the parent's columns (empty: the
/// parent's primary key).
struct AddedForeignKey {
    columns: Vec<String>,
    /// The parent's schema as written, empty when unqualified.
    parent_schema: String,
    parent: String,
    parent_columns: Vec<String>,
}

fn added_foreign_key(node: &turso_pg_parser::pg_query::protobuf::Node) -> Option<AddedForeignKey> {
    use turso_pg_parser::pg_query::protobuf::{node::Node, ConstrType};
    let Some(Node::Constraint(c)) = node.node.as_ref() else {
        return None;
    };
    if ConstrType::try_from(c.contype).ok()? != ConstrType::ConstrForeign {
        return None;
    }
    let names = |nodes: &[turso_pg_parser::pg_query::protobuf::Node]| -> Vec<String> {
        nodes
            .iter()
            .filter_map(|n| match n.node.as_ref() {
                Some(Node::String(s)) => Some(s.sval.clone()),
                _ => None,
            })
            .collect()
    };
    Some(AddedForeignKey {
        columns: names(&c.fk_attrs),
        parent_schema: c.pktable.as_ref()?.schemaname.clone(),
        parent: c.pktable.as_ref()?.relname.clone(),
        parent_columns: names(&c.pk_attrs),
    })
}

/// Every foreign key of `table` resolves as the engine resolves it for a write, unique parent keys
/// required (Schema::resolved_fks_for_child): one that does not is 42830 "there is no unique
/// constraint matching given keys for referenced table", naming the parent the engine names. A key
/// that did not resolve made every later INSERT into the table fail 'foreign key mismatch'. Shared
/// by ALTER TABLE ADD FOREIGN KEY (wire review 11 item 5) and CREATE TABLE (review 13 item 2), so
/// the two cannot drift.
pub fn check_table_keys(conn: &Arc<Connection>, table: &str) -> Result<()> {
    let schema = conn.current_schema();
    // A self-reference with an omitted parent column list resolves only after CREATE has
    // installed the table. Its missing primary key is distinct from a non-unique explicit key.
    if let Some(child) = schema.get_btree_table(table) {
        if child.primary_key_columns.is_empty()
            && child.foreign_keys.iter().any(|key| {
                key.parent_table.eq_ignore_ascii_case(table) && key.parent_columns.is_empty()
            })
        {
            return Err(LimboError::ParseError(SqlErrorMessage::classified(
                SqlErrorKind::UndefinedObject,
                format!("there is no primary key for referenced table \"{table}\""),
            )));
        }
    }
    schema
        .resolved_fks_for_child(table)
        .map(|_| ())
        .map_err(|e| {
            let message = e.to_string();
            // fk_mismatch_err: 'foreign key mismatch - "<child>" referencing "<parent>"'.
            let parent = message
                .rsplit_once("referencing \"")
                .and_then(|(_, p)| p.strip_suffix('"'))
                .unwrap_or(table)
                .to_string();
            LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::InvalidForeignKey, format!(
                "there is no unique constraint matching given keys for referenced table \"{parent}\""
            )))
        })
}

/// The foreign keys a CREATE TABLE declares, in its column constraints (the column the key's own)
/// and its table constraints.
fn create_foreign_keys(
    create: &turso_pg_parser::pg_query::protobuf::CreateStmt,
) -> Vec<AddedForeignKey> {
    use turso_pg_parser::pg_query::protobuf::node::Node;
    let mut keys = Vec::new();
    for elt in &create.table_elts {
        match elt.node.as_ref() {
            Some(Node::ColumnDef(col)) => {
                for mut key in col.constraints.iter().filter_map(added_foreign_key) {
                    if key.columns.is_empty() {
                        key.columns = vec![col.colname.clone()];
                    }
                    keys.push(key);
                }
            }
            Some(Node::Constraint(_)) => keys.extend(added_foreign_key(elt)),
            _ => {}
        }
    }
    keys
}

/// A CREATE TABLE's foreign keys, each checked before the table is created, as PostgreSQL checks
/// them ([`check_foreign_key`]: 42P01, 42703, 42830); a key onto the table itself as far as it can
/// be before the table exists. Returns the table's name when it declares a key, for the server to
/// resolve every key once the table exists ([`check_table_keys`]). CREATE TABLE IF NOT EXISTS of a
/// table that exists checks nothing, as in PostgreSQL, which creates nothing. CREATE TABLE checked
/// no parent, so a key onto a missing parent or a non-unique column was accepted and every INSERT
/// into the table then failed (wire review 13 item 2).
fn precheck_create_keys(
    conn: &Arc<Connection>,
    parse: &turso_pg_parser::pg_query::ParseResult,
) -> Result<Option<String>> {
    use turso_pg_parser::pg_query::protobuf::node::Node;
    let [raw] = parse.protobuf.stmts.as_slice() else {
        return Ok(None);
    };
    let Some(Node::CreateStmt(create)) = raw.stmt.as_ref().and_then(|s| s.node.as_ref()) else {
        return Ok(None);
    };
    let Some(relation) = create.relation.as_ref() else {
        return Ok(None);
    };
    let keys = create_foreign_keys(create);
    if keys.is_empty()
        || (create.if_not_exists && conn.current_schema().get_table(&relation.relname).is_some())
    {
        return Ok(None);
    }
    // A table, or a parent, in another schema (an attached database): these checks read public's
    // catalog alone, so such a key was refused for a parent public lacks, or checked against
    // public's table of the name and never against its own; refused, as ALTER TABLE ADD
    // CONSTRAINT refuses outside public (DECISIONS: a schema is branched or refused; wire review
    // 16 item 11).
    let elsewhere = |schema: &str| {
        !matches!(
            turso_pg_parser::schema_qualifier(schema),
            turso_pg_parser::SchemaQualifier::Unqualified | turso_pg_parser::SchemaQualifier::Main
        )
    };
    if elsewhere(&relation.schemaname) || keys.iter().any(|k| elsewhere(&k.parent_schema)) {
        return Err(LimboError::ParseError(
            SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "foreign keys in a schema other than public are not supported".to_string()),
        ));
    }
    let child_columns: Vec<String> = create
        .table_elts
        .iter()
        .filter_map(|elt| match elt.node.as_ref() {
            Some(Node::ColumnDef(col)) => Some(col.colname.clone()),
            _ => None,
        })
        .collect();
    for key in &keys {
        check_foreign_key(conn, &relation.relname, &child_columns, key, true)?;
    }
    Ok(Some(relation.relname.clone()))
}

/// The names the engine reads as a parent table's rowid when no column of the table has the name
/// (ROWID_STRS in core's planner).
const ROWID_NAMES: [&str; 3] = ["rowid", "_rowid_", "oid"];

/// The checks PostgreSQL makes of a foreign key a CREATE TABLE or an ALTER adds, each key on its
/// own and in PostgreSQL's order (ATAddForeignKeyConstraint): the parent exists (42P01); the key's
/// columns are the child's (42703); the parent's columns exist, or it has a primary key to default
/// to (42703, 42830); a unique key of the parent covers them as the engine resolves a parent key
/// ([`parent_key_resolves`]; 42830, naming this key's parent); the two column counts agree
/// (42830). Returns the parent key's columns. `child_columns` are the child's columns. With
/// `own_keys_pending`, a key onto the child itself is checked as far as it can be before the
/// child's own keys exist (its columns, and an explicit parent key's columns and count); its
/// default and its uniqueness wait for the table. Resolving every key of the table instead blamed
/// the first added key's parent, refused every new key beside an older one that no longer
/// resolved, and gave a missing column 42830 (wire review 13 item 3).
fn check_foreign_key(
    conn: &Arc<Connection>,
    child: &str,
    child_columns: &[String],
    key: &AddedForeignKey,
    own_keys_pending: bool,
) -> Result<Vec<String>> {
    let schema = conn.current_schema();
    let missing = |column: &str| {
        LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::UndefinedColumn,
            format!("column \"{column}\" referenced in foreign key constraint does not exist"),
        ))
    };
    let disagree = |parent_columns: &[String]| {
        LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::InvalidForeignKey,
            format!(
            "number of referencing and referenced columns for foreign key disagree (the key on \
             {child} names {}, its parent key {})",
            key.columns.len(),
            parent_columns.len()
        ),
        ))
    };
    let parent = if own_keys_pending && key.parent.eq_ignore_ascii_case(child) {
        None
    } else {
        Some(schema.get_btree_table(&key.parent).ok_or_else(|| {
            LimboError::ParseError(SqlErrorMessage::classified(
                SqlErrorKind::UndefinedTable,
                format!("relation \"{}\" does not exist", key.parent),
            ))
        })?)
    };
    if let Some(column) = key
        .columns
        .iter()
        .find(|k| !child_columns.iter().any(|c| c.eq_ignore_ascii_case(k)))
    {
        return Err(missing(column));
    }
    // A key onto a rowid name is one the engine never resolves, declared column or not (core's
    // ForeignKey::validate refused it after the CREATE ran, or after the ALTER's lock-held copies):
    // 42703 for a name the parent does not declare, as PostgreSQL answers, and 0A000 for a declared
    // column of the name (wire review 16 item 16).
    let is_rowid = |c: &str| ROWID_NAMES.iter().any(|r| c.eq_ignore_ascii_case(r));
    if let Some(column) = key.parent_columns.iter().find(|p| is_rowid(p)) {
        let declared = match &parent {
            Some(parent) => parent.get_column(column).is_some(),
            None => child_columns.iter().any(|c| c.eq_ignore_ascii_case(column)),
        };
        if !declared {
            return Err(missing(column));
        }
        return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, format!(
            "a foreign key onto a column named {column} is not supported: the engine reads the \
             name as the rowid"
        ))));
    }
    let Some(parent) = parent else {
        if let Some(column) = key
            .parent_columns
            .iter()
            .find(|p| !child_columns.iter().any(|c| c.eq_ignore_ascii_case(p)))
        {
            return Err(missing(column));
        }
        if !key.parent_columns.is_empty() && key.parent_columns.len() != key.columns.len() {
            return Err(disagree(&key.parent_columns));
        }
        return Ok(key.parent_columns.clone());
    };
    let parent_columns: Vec<String> = if key.parent_columns.is_empty() {
        parent
            .primary_key_columns
            .iter()
            .map(|(c, _)| c.clone())
            .collect()
    } else {
        if let Some(column) = key
            .parent_columns
            .iter()
            .find(|p| parent.get_column(p).is_none())
        {
            return Err(missing(column));
        }
        key.parent_columns.clone()
    };
    if parent_columns.is_empty() {
        return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::UndefinedObject, format!(
            "there is no primary key for referenced table \"{}\"",
            key.parent
        ))));
    }
    if !parent_key_resolves(&schema, &parent, &parent_columns) {
        return Err(LimboError::ParseError(SqlErrorMessage::classified(SqlErrorKind::InvalidForeignKey, format!(
            "there is no unique constraint matching given keys for referenced table \"{}\"",
            key.parent
        ))));
    }
    if parent_columns.len() != key.columns.len() || key.columns.is_empty() {
        return Err(disagree(&parent_columns));
    }
    Ok(parent_columns)
}

/// Whether the engine resolves `columns` of `parent` as a parent key, as core's Schema::resolve_fk
/// does with unique keys required: the rowid (the column an INTEGER PRIMARY KEY makes the rowid; a
/// rowid name, which ForeignKey::validate refuses, is refused before this by check_foreign_key), or
/// a UNIQUE index with no WHERE on exactly those columns in that order. A key it does not resolve
/// made every INSERT into the child fail 'foreign key mismatch'. PostgreSQL also accepts a unique
/// key's columns in another order (wire review 13 item 20).
fn parent_key_resolves(
    schema: &turso_core::schema::Schema,
    parent: &turso_core::schema::BTreeTable,
    columns: &[String],
) -> bool {
    if let [column] = columns {
        let rowid_alias = parent.columns().iter().any(|c| {
            c.is_rowid_alias()
                && c.name
                    .as_deref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(column))
        });
        if rowid_alias {
            return true;
        }
    }
    schema.get_indices(&parent.name).any(|index| {
        index.unique
            && index.where_clause.is_none()
            && index.columns.len() == columns.len()
            && index
                .columns
                .iter()
                .zip(columns)
                .all(|(i, c)| i.name.eq_ignore_ascii_case(c))
    })
}

/// The check PostgreSQL makes when it adds a foreign key, as one query: a row of `table` whose key
/// columns are all non-NULL (MATCH SIMPLE) and match no row of the parent key
/// ([`check_foreign_key`]) fails the ALTER with 23503.
fn check_added_foreign_key(
    conn: &Arc<Connection>,
    table: &str,
    key: &AddedForeignKey,
    parent_columns: &[String],
) -> Result<()> {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let not_null = key
        .columns
        .iter()
        .map(|c| format!("c.{} IS NOT NULL", quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let matches = key
        .columns
        .iter()
        .zip(parent_columns)
        .map(|(c, p)| format!("p.{} = c.{}", quote(p), quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT 1 FROM {table} AS c WHERE {not_null} AND NOT EXISTS (SELECT 1 FROM {} AS p WHERE \
         {matches}) LIMIT 1",
        quote(&key.parent)
    );
    let orphans = conn.prepare_sqlite(catalog::sqlite_text(&sql))?.run_collect_rows()?;
    if orphans.is_empty() {
        Ok(())
    } else {
        Err(LimboError::ForeignKeyConstraint(format!(
            "insert or update on table {table} violates the foreign key constraint it adds: a \
             row's key ({}) is not present in table \"{}\"",
            key.columns.join(", "),
            key.parent
        )))
    }
}

/// The rebuild's aside table name for `table`, attempt `n`: `<table>__turso_rebuild[_n]`, the
/// table's part cut at a character boundary so the whole is at most 63 bytes, PostgreSQL's
/// identifier limit (NAMEDATALEN - 1).
fn aside_name_for(table: &str, n: usize) -> String {
    const MAX_IDENTIFIER_BYTES: usize = 63;
    let suffix = match n {
        0 => "__turso_rebuild".to_string(),
        n => format!("__turso_rebuild_{n}"),
    };
    let mut cut = table.len().min(MAX_IDENTIFIER_BYTES - suffix.len());
    while !table.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{suffix}", &table[..cut])
}

#[cfg(feature = "failpoints")]
thread_local! {
    static UNDO_FAILS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PRETEND_DEFERRED: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
}

/// Test failpoint (feature `failpoints`, never in a credited build): every read of a connection's
/// pending deferred foreign-key violations on this thread answers at least `n` until it is reset
/// to 0, so the publish guard, which no client sequence reaches, can be shown to hold (fk-guard
/// review LOW 4).
#[cfg(feature = "failpoints")]
pub fn pretend_deferred_violations(n: isize) {
    PRETEND_DEFERRED.with(|f| f.set(n));
}

/// The connection's pending deferred foreign-key violations (the engine's count), through the
/// `pretend_deferred_violations` failpoint in a failpoints build.
#[inline]
fn deferred_violations(conn: &Connection) -> isize {
    let n = conn.deferred_foreign_key_violations();
    #[cfg(feature = "failpoints")]
    let n = n.max(PRETEND_DEFERRED.with(std::cell::Cell::get));
    n
}

/// Test failpoint (feature `failpoints`, never in a credited build): the next undo of a transaction
/// a statement or an implicit block opened, on this thread, fails before it runs, as a ROLLBACK the
/// engine refused would (wire review 16 item 14's red).
#[cfg(feature = "failpoints")]
pub fn fail_next_undo() {
    UNDO_FAILS.with(|f| f.set(true));
}

/// Where `fail_next_undo` strikes, before every undo [`own_transaction`] and
/// [`PgConnection::roll_back_or_break`] run: an error once after it, else Ok. Without the
/// `failpoints` feature it is always Ok and compiles to nothing.
#[inline]
fn undo_failpoint() -> Result<()> {
    #[cfg(feature = "failpoints")]
    if UNDO_FAILS.with(|f| f.replace(false)) {
        return Err(LimboError::InternalError(
            "failpoint: the undo failed".to_string(),
        ));
    }
    Ok(())
}

/// A transaction-control statement the client did not send: the server's implicit block's own, or
/// the transaction a statement runs in ([`own_transaction`]).
#[derive(Debug, Clone, Copy)]
pub enum TxStmt {
    Begin,
    Commit,
    Rollback,
    Savepoint(&'static str),
    Release(&'static str),
    RollbackTo(&'static str),
}

/// Run `tx` on `conn` from the engine's AST: no SQL text is parsed for it.
fn run_tx(conn: &Arc<Connection>, tx: TxStmt) -> Result<()> {
    use turso_parser::ast::{Name, Stmt};
    let (stmt, text) = match tx {
        TxStmt::Begin => (
            Stmt::Begin {
                typ: None,
                name: None,
            },
            "BEGIN".to_string(),
        ),
        TxStmt::Commit => (Stmt::Commit { name: None }, "COMMIT".to_string()),
        TxStmt::Rollback => (
            Stmt::Rollback {
                tx_name: None,
                savepoint_name: None,
            },
            "ROLLBACK".to_string(),
        ),
        TxStmt::Savepoint(name) => (
            Stmt::Savepoint {
                name: Name::from_string(name),
            },
            format!("SAVEPOINT {name}"),
        ),
        TxStmt::Release(name) => (
            Stmt::Release {
                name: Name::from_string(name),
            },
            format!("RELEASE SAVEPOINT {name}"),
        ),
        TxStmt::RollbackTo(name) => (
            Stmt::Rollback {
                tx_name: None,
                savepoint_name: Some(Name::from_string(name)),
            },
            format!("ROLLBACK TO SAVEPOINT {name}"),
        ),
    };
    conn.prepare_translated_stmt(stmt, &text)?.run_ignore_rows()
}

/// Run `f` in a transaction of its own, so that what it does is kept whole or not at all: under the
/// savepoint `name` inside a transaction already open (a client's block, an implicit block),
/// otherwise between its own BEGIN and COMMIT. Its end (RELEASE or COMMIT) fails into the same
/// undo as `f`'s steps. One undo rule for every statement run this way (CREATE TABLE with keys,
/// ALTER TABLE ADD CONSTRAINT's rebuild, COPY FROM; wire review 16 item 14, where the CREATE's
/// undo was `let _ =`): roll back to the savepoint and release it, or roll the own transaction
/// back, then read the engine's state again. An undo that fails, or one after which the engine
/// still holds a transaction this call opened, breaks the connection (the server ends the session,
/// FATAL 08006): its work sits in a transaction nobody can name, and a statement after it would
/// join it (a client's COMMIT kept the refused table). Inside a block the engine no longer holds
/// (the error rolled the whole transaction back) there is nothing to undo.
fn own_transaction<T>(
    pg_conn: &Arc<PgConnectionInner>,
    name: &'static str,
    what: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let conn = &pg_conn.conn;
    let in_tx = !conn.get_auto_commit();
    run_tx(
        conn,
        if in_tx {
            TxStmt::Savepoint(name)
        } else {
            TxStmt::Begin
        },
    )?;
    let ended = f().and_then(|done| {
        run_tx(
            conn,
            if in_tx {
                TxStmt::Release(name)
            } else {
                TxStmt::Commit
            },
        )
        .map(|()| done)
    });
    let e = match ended {
        Ok(done) => return Ok(done),
        Err(e) => e,
    };
    let undone = if conn.get_auto_commit() {
        Ok(())
    } else {
        undo_failpoint().and_then(|()| {
            if in_tx {
                run_tx(conn, TxStmt::RollbackTo(name))
                    .and_then(|()| run_tx(conn, TxStmt::Release(name)))
            } else {
                run_tx(conn, TxStmt::Rollback)
            }
        })
    }
    .and_then(|()| held_after_undo(conn, in_tx));
    match undone {
        Ok(()) => Err(e),
        Err(undo) => Err(break_connection(
            pg_conn,
            &format!("{what} failed ({e}) and its undo"),
            &undo,
        )),
    }
}

/// After an undo, whether the engine still holds a transaction the undone work opened: with no
/// block open before (`in_tx` false), any transaction, or a write transaction in autocommit (a
/// COMMIT that left autocommit before failing, which no ROLLBACK on this connection can end).
/// Inside a block its transaction is the client's and stays. Blind spot: a stranded READ
/// transaction in autocommit is not visible (Connection::is_in_write_tx).
fn held_after_undo(conn: &Arc<Connection>, in_tx: bool) -> Result<()> {
    if !in_tx && (!conn.get_auto_commit() || conn.is_in_write_tx()) {
        return Err(LimboError::InternalError(
            "the engine still holds the transaction".to_string(),
        ));
    }
    Ok(())
}

/// Mark the connection broken (the server ends the session on it, FATAL 08006) and say why: `what`
/// could not be undone (`undo`).
fn break_connection(pg_conn: &Arc<PgConnectionInner>, what: &str, undo: &LimboError) -> LimboError {
    pg_conn
        .broken
        .store(true, std::sync::atomic::Ordering::Release);
    LimboError::InternalError(format!("{CONNECTION_BROKEN}: {what} failed ({undo})"))
}

/// The start of the error text of a statement whose undo failed too. The server ends the session on
/// [`PgConnection::is_broken`], which is set with it, never on this text (wire review 5 item 4).
pub const CONNECTION_BROKEN: &str = "connection broken";

/// Run one statement as the engine runs a client's (a root statement), in SQLite text.
fn execute_root(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    // Marked as this frontend's SQLite: a stored index's text was re-read as PostgreSQL and
    // translated again (the dialect audit of 2026-10-17, C).
    conn.prepare_sqlite(catalog::sqlite_text(sql))?.run_ignore_rows()
}

/// Run one PostgreSQL statement through this frontend, as a client's would be.
/// One statement of an ALTER's rebuild, its foreign keys not checked again
/// ([`prepare_statement_checked`]).
fn run_pg_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<()> {
    prepare_statement_checked(pg_conn, sql, None, false, false)?
        .ok_or_else(|| {
            LimboError::InternalError("only a Describe declines to prepare a statement".to_string())
        })?
        .run_ignore_rows()
}

fn handle_pg_copy_from(pg_conn: &Arc<PgConnectionInner>, stmt: &PgCopyFromStmt) -> Result<usize> {
    if let Some(why) = &stmt.refused {
        return Err(LimboError::ParseError(why.clone().into()));
    }
    if stmt.has_where {
        return Err(LimboError::ParseError(
            SqlErrorMessage::classified(SqlErrorKind::FeatureNotSupported, "COPY FROM ... WHERE is not supported".to_string()),
        ));
    }
    let conn = &pg_conn.conn;
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e).into())
    })?;

    // An unqualified table is found through the search path, as the engine finds every other
    // statement's; COPY's INSERT was prepared with no path, so `COPY t` under `search_path TO s,
    // public` filled public.t (wire review 18 item 22).
    let schema = match &stmt.schema_name {
        Some(schema) => Some(schema.clone()),
        None => schema_on_search_path(pg_conn, &stmt.table_name)?,
    };
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let table_name = match &schema {
        Some(schema) => format!("{}.{}", quoted(schema), quoted(&stmt.table_name)),
        None => quoted(&stmt.table_name),
    };
    let column_names = get_table_columns(conn, &stmt.table_name, schema.as_deref())?;
    if column_names.is_empty() {
        return Err(LimboError::ParseError(format!(
            "COPY FROM: table '{}' not found or has no columns",
            stmt.table_name
        ).into()));
    }

    let (insert_cols, num_columns) = match &stmt.columns {
        Some(cols) => {
            let col_list = cols
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            (format!(" ({col_list})"), cols.len())
        }
        None => (String::new(), column_names.len()),
    };

    let placeholders = (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = catalog::sqlite_text(format!(
        "INSERT INTO {table_name}{insert_cols} VALUES ({placeholders})"
    ));

    let delimiter = stmt
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = stmt.null_string.as_deref().unwrap_or("\\N");

    let mut rows = parse_copy_text_format(&data, delimiter, null_string, num_columns)?;
    if stmt.header && !rows.is_empty() {
        rows.remove(0);
    }

    let rows_inserted = rows.len();
    // The rows go in atomically: in a transaction of their own, or, inside one already open (a
    // client's block, or the implicit block of a multi-statement query or a pipeline), under a
    // savepoint, so a failed COPY undoes its own rows and leaves the block to its owner. Its own
    // BEGIN was refused inside any open transaction (wire review 4 item 2).
    // An undo that fails breaks the connection, as for ALTER's rebuild (own_transaction): it left
    // some of the rows in a transaction nobody can name.
    own_transaction(pg_conn, "__turso_copy", "COPY FROM", || {
        let mut insert_stmt = conn.prepare_sqlite(&insert_sql)?;
        for row in &rows {
            for (i, val) in row.iter().enumerate() {
                let index = NonZero::new(i + 1).unwrap();
                match val {
                    Some(s) => insert_stmt.bind_at(index, Value::build_text(s.clone()))?,
                    None => insert_stmt.bind_at(index, Value::Null)?,
                }
            }
            insert_stmt.run_ignore_rows()?;
            insert_stmt.reset()?;
            insert_stmt.clear_bindings();
        }
        Ok(rows_inserted)
    })
}

/// The database holding `table` on the session's search path, as the engine resolves an
/// unqualified name (core's resolve_search_path_database_id): public is main, any other entry the
/// attached schema of its name, skipped when there is none ("$user", a schema not created), the
/// first entry holding the table wins, and none holding it is 42P01. None with no path set, where
/// the engine's default order (main, then the attached schemas) applies.
fn schema_on_search_path(pg_conn: &Arc<PgConnectionInner>, table: &str) -> Result<Option<String>> {
    let path = pg_conn.session_state.lock().search_path.clone();
    if path.is_empty() {
        return Ok(None);
    }
    for entry in &path {
        let database = if entry.eq_ignore_ascii_case("public") {
            "main"
        } else {
            entry.as_str()
        };
        let holds = get_table_columns(&pg_conn.conn, table, Some(database))
            .is_ok_and(|columns| !columns.is_empty());
        if holds {
            return Ok(Some(database.to_string()));
        }
    }
    Err(LimboError::ParseError(SqlErrorMessage::classified(
        SqlErrorKind::UndefinedTable,
        format!(
        "relation \"{table}\" does not exist"
    ),
    )))
}

/// Whether `parse` holds a DELETE ... USING anywhere (its rewrite reads the relations' declared
/// columns): the statement itself, EXPLAIN's, a CTE's. The text is read for the word first, so any
/// other statement pays one byte scan and no tree walk.
/// [`crate::StatementTypes::folds_at_plan`]: the statement is a SELECT or VALUES with no FROM and
/// no set operation.
fn folds_at_plan(parse: &turso_pg_parser::pg_query::ParseResult) -> bool {
    use turso_pg_parser::pg_query::protobuf::{node::Node, SetOperation};
    let Some(Some(Node::SelectStmt(select))) = parse
        .protobuf
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .map(|n| n.node.as_ref())
    else {
        return false;
    };
    select.from_clause.is_empty() && select.op() == SetOperation::SetopNone
}

pub(crate) fn is_delete_using(sql: &str, parse: &turso_pg_parser::pg_query::ParseResult) -> bool {
    use turso_pg_parser::pg_query::NodeRef;
    if !sql
        .as_bytes()
        .windows(5)
        .any(|w| w.eq_ignore_ascii_case(b"using"))
    {
        return false;
    }
    parse
        .protobuf
        .nodes()
        .iter()
        .any(|(n, ..)| matches!(n, NodeRef::DeleteStmt(d) if !d.using_clause.is_empty()))
}

/// Whether a FROM item's alias carries a column list (`t AS a (x, y)`), which the translator reads
/// the table's columns for.
pub(crate) fn has_alias_column_list(parse: &turso_pg_parser::pg_query::ParseResult) -> bool {
    use turso_pg_parser::pg_query::NodeRef;
    parse.protobuf.nodes().iter().any(|(n, ..)| {
        matches!(n, NodeRef::RangeVar(rv) if rv.alias.as_ref().is_some_and(|a| !a.colnames.is_empty()))
    })
}

/// Whether the statement holds an INSERT with no column list whose VALUES lists hold DEFAULT.
pub(crate) fn is_positional_insert_default(sql: &str, parse: &turso_pg_parser::pg_query::ParseResult) -> bool {
    sql.as_bytes()
        .windows(7)
        .any(|w| w.eq_ignore_ascii_case(b"default"))
        && is_positional_insert_values(parse)
}

/// Whether the statement holds an INSERT with no column list whose body is VALUES lists.
fn is_positional_insert_values(parse: &turso_pg_parser::pg_query::ParseResult) -> bool {
    use turso_pg_parser::pg_query::protobuf::node::Node;
    use turso_pg_parser::pg_query::NodeRef;
    parse.protobuf.nodes().iter().any(|(n, ..)| match n {
        NodeRef::InsertStmt(i) if i.cols.is_empty() => {
            matches!(
                i.select_stmt.as_ref().and_then(|s| s.node.as_ref()),
                Some(Node::SelectStmt(s)) if !s.values_lists.is_empty()
            )
        }
        _ => false,
    })
}

/// The columns `table` declares, read from the connection's in-memory schemas alone, with no
/// catalog query (lead ruling: an INSERT's lookup adds none): main, an attached schema by name, or
/// an unqualified name as the engine resolves it (temp first, then the whole search path; see
/// `Connection::resident_unqualified_table_columns`). Only a path starting at public was read, so
/// under PostgreSQL's own default path ("$user", public) a positional INSERT with DEFAULT was
/// refused (ultrareview of fastest-wire, finding 6). None for a relation none of them holds.
fn resident_relation_columns(
    pg_conn: &Arc<PgConnectionInner>,
    schema: &str,
    table: &str,
) -> Option<Vec<String>> {
    use turso_pg_parser::SchemaQualifier;
    let columns_in = |database: &str| pg_conn.conn.resident_table_columns(database, table);
    match turso_pg_parser::schema_qualifier(schema) {
        SchemaQualifier::Main => columns_in("main"),
        SchemaQualifier::Unqualified => {
            let path = pg_conn.session_state.lock().search_path.clone();
            pg_conn
                .conn
                .resident_unqualified_table_columns(table, &path)
        }
        SchemaQualifier::Attached(database) => columns_in(database),
        _ => None,
    }
}

/// The columns `table` declares, from the table the engine reads under the name: an unqualified
/// name through the session's search path (the first schema on it that holds the table; public,
/// or no path, is main), public from the connection's schema, another schema from its attached
/// table_info. None for a relation it cannot read (a view, one that does not exist). An
/// unqualified name read public's table whatever the path (wire review 16 item 21).
fn relation_columns(
    pg_conn: &Arc<PgConnectionInner>,
    schema: &str,
    table: &str,
) -> Option<Vec<String>> {
    use turso_pg_parser::SchemaQualifier;
    let conn = &pg_conn.conn;
    let main = || {
        conn.current_schema()
            .get_btree_table(table)
            .map(|t| t.columns().iter().filter_map(|c| c.name.clone()).collect())
    };
    let attached = |schema: &str| {
        get_table_columns(conn, table, Some(schema))
            .ok()
            .filter(|columns| !columns.is_empty())
    };
    match turso_pg_parser::schema_qualifier(schema) {
        SchemaQualifier::Main => main(),
        SchemaQualifier::Unqualified => match schema_on_search_path(pg_conn, table) {
            Ok(Some(database)) if database != "main" => attached(&database),
            Ok(_) => main(),
            Err(_) => None,
        },
        _ => attached(schema),
    }
}

#[cfg(test)]
thread_local! {
    /// This thread's PRAGMA table_info reads (`get_table_columns`): the catalog queries a prepare
    /// may make, for a test to assert none was made.
    static TABLE_INFO_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// This thread's PRAGMA table_info reads so far.
#[cfg(test)]
fn table_info_reads() -> u64 {
    TABLE_INFO_READS.with(|n| n.get())
}

fn get_table_columns(
    conn: &Arc<Connection>,
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<Vec<String>> {
    #[cfg(test)]
    TABLE_INFO_READS.with(|n| n.set(n.get() + 1));
    // The names quoted here, once, for every caller: COPY passed its table's name as written, so
    // a quote in it broke the PRAGMA's text.
    let table_name = table_name.replace('\'', "''");
    let sql = match schema_name {
        Some(schema) => catalog::sqlite_text(format!(
            "PRAGMA \"{}\".table_info('{table_name}')",
            schema.replace('"', "\"\"")
        )),
        None => catalog::sqlite_text(format!("PRAGMA table_info('{table_name}')")),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn list_user_tables(conn: &Arc<Connection>, schema_name: Option<&str>) -> Result<Vec<String>> {
    let filter = "type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'";
    let sql = match schema_name {
        Some(name) => format!("SELECT name FROM \"{name}\".sqlite_schema WHERE {filter}"),
        None => format!("SELECT name FROM sqlite_schema WHERE {filter}"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.first() {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn schema_exists(conn: &Arc<Connection>, schema_name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT 1 FROM pragma_database_list WHERE name = '{}'",
        schema_name.replace('\'', "''")
    );
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(!rows.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(conn: &crate::Connection, sql: &str) {
        let mut stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        stmt.run_ignore_rows().unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Review wire-line2 M1: an engine re-prepare of a session's statement really goes through
    /// PostgresDialect::parse_for_reprepare and translates as the session did. A prepared
    /// DELETE ... USING (whose translation reads its relations' columns), with the prepare
    /// generation bumped by a setting change (set_cache_size) before its first step, re-prepares
    /// (the reprepares metric rises: the premise) and deletes the one matching row. MUTANT owed with
    /// the run: the override returning `self.parse(sql)` translates with no lookup and must fail it.
    #[test]
    fn an_engine_reprepare_translates_as_the_session_did() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        run(&conn, "CREATE TABLE a (x INT)");
        run(&conn, "CREATE TABLE b (x INT)");
        run(&conn, "INSERT INTO a VALUES (1), (2), (3)");
        run(&conn, "INSERT INTO b VALUES (2)");
        let mut stmt = conn.prepare("DELETE FROM a USING b WHERE a.x = b.x").unwrap();
        let before = stmt.metrics().reprepares;
        let size = conn.inner().get_cache_size();
        conn.inner().set_cache_size(size + 1);
        stmt.run_ignore_rows().unwrap();
        assert!(
            stmt.metrics().reprepares > before,
            "premise: the statement was not re-prepared after the generation moved"
        );
        assert_eq!(stmt.n_change(), 1, "the re-prepared DELETE ... USING deleted the wrong rows");
        drop(stmt);
        let mut left = conn.prepare("SELECT x FROM a ORDER BY x").unwrap();
        let rows = left.run_collect_rows().unwrap();
        let xs: Vec<String> = rows.iter().map(|r| format!("{}", r[0])).collect();
        assert_eq!(xs, ["1", "3"]);
    }

    /// A multi-row UPDATE inside a block whose third row sets a primary key NULL fails the key's
    /// NOT NULL and leaves every row as it was: the rows it already changed are undone by the
    /// statement journal (engine fix c29eaa7f4, engine review 20 LOWs 24 and 26; the engine's own red
    /// is 8a9f0aaad), here through the PostgresDialect and the translator, as a client's UPDATE
    /// runs. Outside a block the whole transaction is undone, so the block is what discriminates.
    #[test]
    fn a_multi_row_update_that_nulls_a_key_changes_no_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        run(&conn, "CREATE TABLE k (id INT PRIMARY KEY, v INT)");
        run(&conn, "INSERT INTO k VALUES (1, 10), (2, 20), (3, 30)");
        run(&conn, "BEGIN");
        let sql = "UPDATE k SET id = CASE WHEN id = 3 THEN NULL ELSE id + 100 END";
        let e = conn
            .prepare(sql)
            .and_then(|mut stmt| stmt.run_ignore_rows())
            .expect_err("a NULL key was accepted");
        assert!(
            matches!(e, LimboError::Constraint(ref m) if m.starts_with("NOT NULL constraint failed")),
            "{sql}: {e:?}"
        );
        let mut stmt = conn.prepare("SELECT id FROM k ORDER BY id").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        let ids: Vec<String> = rows.iter().map(|r| format!("{}", r[0])).collect();
        assert_eq!(ids, ["1", "2", "3"], "the rows the UPDATE changed before the NULL stayed");
        run(&conn, "COMMIT");
    }

    /// A positional INSERT shorter than its table is filled from the in-memory schema, at prepare,
    /// with no catalog query (lead ruling, DECISIONS newest: "It must add NO catalog query"), also
    /// with a search path set, which sent the lookup through PRAGMA table_info once per path entry.
    /// The instrument, `table_info_reads` (this thread's PRAGMA table_info reads), is shown to fire
    /// by a DELETE ... USING under the same path.
    #[test]
    fn a_short_positional_insert_reads_no_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        run(&conn, "CREATE TABLE s3 (a int, b int, c text default 'd')");
        run(&conn, "SET search_path TO public");
        let before = table_info_reads();
        run(&conn, "INSERT INTO s3 VALUES (1, 2)");
        run(&conn, "INSERT INTO s3 VALUES (DEFAULT, 3)");
        assert_eq!(table_info_reads() - before, 0, "a positional INSERT read the catalog");
        // A table's `*` beside a derived boolean reads the table's width from memory too.
        let before = table_info_reads();
        run(&conn, "SELECT s3.*, flag FROM s3, (SELECT true AS flag) x");
        assert_eq!(table_info_reads() - before, 0, "a table's * read the catalog");
        let before = table_info_reads();
        run(&conn, "DELETE FROM s3 USING s3 AS x WHERE s3.a = x.a AND s3.a = 9");
        assert!(table_info_reads() > before, "the instrument never fires");
    }

    /// A positional INSERT with DEFAULT, or one shorter than its table, finds the table through the
    /// whole search path, still with no catalog query: under PostgreSQL's default path ("$user",
    /// public), under a path whose first schema is attached, and for an attached table named by its
    /// schema. Only a path starting at public was read, so each was refused (ultrareview of
    /// fastest-wire, finding 6).
    #[test]
    fn a_positional_insert_finds_its_table_through_the_whole_search_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        run(&conn, "CREATE TABLE m (a int, b int, c text default 'm')");
        run(&conn, "CREATE SCHEMA app");
        run(
            &conn,
            "CREATE TABLE app.w (a int, b int, c text default 'w')",
        );
        let before = table_info_reads();
        run(&conn, "SET search_path TO \"$user\", public");
        run(&conn, "INSERT INTO m VALUES (DEFAULT, 1)");
        run(&conn, "INSERT INTO m VALUES (2)");
        run(&conn, "SET search_path TO app, public");
        run(&conn, "INSERT INTO w VALUES (DEFAULT, 3)");
        run(&conn, "INSERT INTO w VALUES (4)");
        run(&conn, "INSERT INTO m VALUES (5)");
        run(&conn, "SET search_path TO public");
        run(&conn, "INSERT INTO app.w VALUES (DEFAULT, 6)");
        assert_eq!(
            table_info_reads() - before,
            0,
            "a positional INSERT read the catalog"
        );
        let rows = |sql: &str| -> Vec<String> {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.run_collect_rows()
                .unwrap()
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };
        assert_eq!(
            rows("SELECT a, b, c FROM m ORDER BY rowid"),
            ["|1|m", "2||m", "5||m"]
        );
        assert_eq!(
            rows("SELECT a, b, c FROM app.w ORDER BY rowid"),
            ["|3|w", "4||w", "|6|w"]
        );
    }

    /// The lookup resolves an unqualified name as the engine does, not by its own walk: a temp
    /// table first, and a search path entry naming main (which the engine's path does not take;
    /// only public and attached names) skipped. A temp table's positional DEFAULT INSERT was
    /// refused (only main was read), and `main, app` read main's table where the engine writes
    /// app's (fresh-context review of the ultrareview fixes).
    #[test]
    fn a_positional_insert_reads_the_table_the_engine_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        run(&conn, "CREATE TEMP TABLE m (x int default 7, y int)");
        run(&conn, "INSERT INTO m VALUES (DEFAULT, 1)");
        let rows = |sql: &str| -> Vec<String> {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.run_collect_rows()
                .unwrap()
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };
        assert_eq!(rows("SELECT x, y FROM m"), ["7|1"]);
        run(
            &conn,
            "CREATE TABLE w (a int, b int, c text default 'main')",
        );
        run(&conn, "CREATE SCHEMA app");
        run(&conn, "CREATE TABLE app.w (p int default 3, q int)");
        run(&conn, "SET search_path TO main, app");
        run(&conn, "INSERT INTO w VALUES (DEFAULT, 4)");
        assert_eq!(rows("SELECT p, q FROM app.w"), ["3|4"]);
    }

    /// With no search path, the attached databases are searched in ascending id order, as the
    /// resolver's BitSet orders them, not the attached map's hash order; and a path entry is looked
    /// up as written, as the resolver does: a quoted 'App' names no attached app (fresh-context
    /// review of 46dcb4c25).
    #[test]
    fn the_positional_insert_lookup_takes_the_engines_order_and_spelling() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let io = Arc::new(crate::PlatformIO::new().unwrap());
        let db = open_database_with_io(
            io,
            path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = crate::Connection::new(db.connect().unwrap());
        for i in 0..12 {
            run(&conn, &format!("CREATE SCHEMA s{i}"));
        }
        run(&conn, "CREATE TABLE s0.w (x int default 1, y int)");
        for i in 1..12 {
            run(
                &conn,
                &format!("CREATE TABLE s{i}.w (p{i} int, q int, r int default 9)"),
            );
        }
        run(&conn, "INSERT INTO w VALUES (DEFAULT, 2)");
        let rows = |sql: &str| -> Vec<String> {
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.run_collect_rows()
                .unwrap()
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };
        assert_eq!(rows("SELECT x, y FROM s0.w"), ["1|2"]);
        run(&conn, "CREATE TABLE v (a int default 5, b int)");
        run(&conn, "CREATE TABLE s1.v (c int, d int, e int default 6)");
        run(&conn, "SET search_path TO 'S1', public");
        run(&conn, "INSERT INTO v VALUES (DEFAULT, 4)");
        assert_eq!(rows("SELECT a, b FROM main.v"), ["5|4"]);
    }

    fn slow(sql: &str) -> Option<PgBranchCall> {
        try_extract_branch_call(&turso_pg_parser::parse(sql).ok()?)
    }

    fn call(function: &str, args: Vec<PgBranchArg>) -> Option<PgBranchCall> {
        Some(PgBranchCall {
            function: function.to_string().into(),
            args: args.into(),
        })
    }

    /// The forms clients send are read on the fast path, to exactly the call they are.
    #[test]
    fn the_fast_path_reads_the_forms_clients_send() {
        use PgBranchArg::{Param, Text};
        let create = "turso_branch_create";
        let cases = [
            (
                "SELECT turso_branch_create('b_1_0_17')",
                call(create, vec![Text("b_1_0_17".into())]),
            ),
            (
                "select turso_branch_switch('b');",
                call("turso_branch_switch", vec![Text("b".into())]),
            ),
            (
                "SELECT turso_branch_current()",
                call("turso_branch_current", vec![]),
            ),
            (
                "SELECT TURSO_BRANCH_DELETE ( 'b' ) ;",
                call("turso_branch_delete", vec![Text("b".into())]),
            ),
            (
                "SELECT turso_branch_create($1)",
                call(create, vec![Param(1)]),
            ),
            (
                "SELECT turso_branch_create($12::text)",
                call(create, vec![Param(12)]),
            ),
            (
                "SELECT turso_branch_create('it''s'::varchar)",
                call(create, vec![Text("it's".into())]),
            ),
            (
                "SELECT turso_branch_create('a\\b')",
                call(create, vec![Text("a\\b".into())]),
            ),
            (
                "SELECT turso_branch_create('')",
                call(create, vec![Text(String::new().into())]),
            ),
            (
                "\tSELECT\nturso_branch_create(\r'x'\x0b)\x0c;\n",
                call(create, vec![Text("x".into())]),
            ),
            (
                "SELECT turso_branch_create('é ü')",
                call(create, vec![Text("é ü".into())]),
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(fast_branch_call(sql), want, "fast path, {sql:?}");
            assert_eq!(branch_call(sql), want, "branch_call, {sql:?}");
        }
    }

    /// Differential: whatever the fast path reads, libpg_query reads the same; every form, read
    /// there or not, reaches PostgreSQL 18's answer: libpg_query's, except that a `$n` above
    /// i32::MAX, which libpg_query's PostgreSQL 17 scanner wraps and PostgreSQL 18's refuses
    /// ("parameter number too large", 42601), makes no call. That answer is read from the corpus's
    /// argument by PostgreSQL's scanner rule, never from the code under test; against libpg_query
    /// alone no implementation could pass, since `$4294967297` is `$1` there (wire review 14 item
    /// 12; FLAGGED edit, lead pre-approval in DECISIONS, PG 18 recording owed). The corpus crosses
    /// keyword case, spacing, argument forms, casts, terminators and trailing junk.
    #[test]
    fn the_fast_path_agrees_with_libpg_query() {
        // PostgreSQL 18's scanner refuses a `$n` whose number is above i32::MAX.
        let pg18_refuses = |arg: &str| {
            arg.strip_prefix('$').is_some_and(|rest| {
                let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                !digits.is_empty()
                    && digits
                        .parse::<u128>()
                        .map_or(true, |n| n > i32::MAX as u128)
            })
        };
        let selects = ["SELECT", "select", "SeLeCt"];
        let names = [
            "turso_branch_create",
            "TURSO_BRANCH_SWITCH",
            "turso_branch_current",
            "turso_branch_nope",
            "\"turso_branch_create\"",
            "public.turso_branch_create",
            "turso_branch_create$x",
            "turso_branchx",
        ];
        let gaps = ["", " ", "  ", "\n", "\t \r\n"];
        let args = [
            "",
            "'b'",
            "'it''s'",
            "'a\\b'",
            "''",
            "''''",
            "'x'::text",
            "'x' :: text",
            "'x'::text[]",
            "'x'::character varying",
            "$1",
            "$1::text",
            "$0",
            "$0001",
            "$65535",
            "$65536",
            "$2147483647",
            "$2147483648",
            "$4294967297",
            "$18446744073709551615",
            "$1x",
            "E'x'",
            "$$x$$",
            "'a'\n'b'",
            "'a' 'b'",
            "'a', 'b'",
            "NULL",
            "true",
            "1",
            "'a' || 'b'",
            "'é'",
        ];
        let ends = [
            "",
            ";",
            " ; ",
            ";;",
            "; SELECT 1",
            " FROM t",
            " -- c",
            "/* c */",
            ")",
        ];
        let mut fast_hits = 0;
        let mut n = 0;
        for sel in selects {
            for name in names {
                for g in gaps {
                    for arg in args {
                        for end in ends {
                            let sql = format!("{sel} {name}{g}({g}{arg}{g}){end}");
                            n += 1;
                            let slow = slow(&sql);
                            let want = if pg18_refuses(arg) {
                                None
                            } else {
                                slow.clone()
                            };
                            if let Some(fast) = fast_branch_call(&sql) {
                                fast_hits += 1;
                                assert_eq!(Some(fast.clone()), slow, "libpg_query, {sql:?}");
                                assert_eq!(Some(fast), want, "{sql:?}");
                            }
                            assert_eq!(branch_call(&sql), want, "{sql:?}");
                        }
                    }
                }
            }
        }
        assert!(
            fast_hits > n / 20,
            "the fast path read only {fast_hits} of {n}"
        );
    }

    /// `type $n` (libpg_query's extension of PostgreSQL 17's grammar; PostgreSQL 18 answers 42601)
    /// is no branch call on either path, so `turso_branch_delete(varchar $1)` bound with 'prod'
    /// cannot delete prod: the slow path read it as a cast to varchar of $1 (wire review 14 item
    /// 14). `$1::varchar`, the cast PostgreSQL has, is still a call (the control).
    #[test]
    fn a_type_name_before_a_parameter_is_no_branch_call() {
        for sql in [
            "SELECT turso_branch_delete(varchar $1)",
            "SELECT turso_branch_create(text $1)",
            "SELECT turso_branch_switch(pg_catalog.text $1)",
        ] {
            assert_eq!(fast_branch_call(sql), None, "fast path, {sql:?}");
            assert_eq!(branch_call(sql), None, "branch_call, {sql:?}");
        }
        assert_eq!(
            branch_call("SELECT turso_branch_delete($1::varchar)"),
            call("turso_branch_delete", vec![PgBranchArg::Param(1)]),
            "control"
        );
        // A typed literal's cast has no location either (libpg_query), and stays a call.
        assert_eq!(
            branch_call("SELECT turso_branch_delete(text 'b')"),
            call("turso_branch_delete", vec![PgBranchArg::Text("b".into())]),
            "control: a typed literal"
        );
    }

    /// A `$n` outside 1..=MAX_PARAMETER names no parameter, so no branch call holds one: on both
    /// paths the statement is no call, and the ordinary prepare refuses it (42P02). Both paths took
    /// any n above 0, and Describe sized the call's parameter list by it: a capacity-overflow panic
    /// for `$18446744073709551615`, about 32 GiB for `$2147483647` (wire review 9 item 1). Leading
    /// zeros change nothing: `$0001` is $1 to both.
    #[test]
    fn a_parameter_past_the_limit_is_no_branch_call() {
        use PgBranchArg::Param;
        let create = "turso_branch_create";
        for n in [
            "0",
            "65536",
            "2147483647",
            "2147483648",
            "18446744073709551615",
            "99999999999999999999999",
        ] {
            let sql = format!("SELECT turso_branch_create(${n})");
            assert_eq!(fast_branch_call(&sql), None, "fast path, {sql:?}");
            assert_eq!(slow(&sql), None, "libpg_query, {sql:?}");
            assert_eq!(branch_call(&sql), None, "branch_call, {sql:?}");
        }
        // libpg_query reads a $n with atol into a 32-bit int (PostgreSQL 17's scanner), so these
        // wrap to $1, $65535 and $0 in its tree: a call of them would bind the wrong parameter.
        // PostgreSQL 18 refuses a number above i32::MAX (42601), so no call holds one (wire review
        // 12 item 1).
        for n in ["4294967297", "4295032831", "4294967296", "2147483648"] {
            let sql = format!("SELECT turso_branch_create(${n})");
            assert_eq!(fast_branch_call(&sql), None, "fast path, {sql:?}");
            assert_eq!(branch_call(&sql), None, "branch_call, {sql:?}");
        }
        // The reference's wrap, the wrong call the check above exists to stop (wire review 14
        // item 12): libpg_query reads $4294967297 as $1.
        assert_eq!(
            slow("SELECT turso_branch_create($4294967297)"),
            call(create, vec![Param(1)]),
            "libpg_query's wrap"
        );
        for (sql, n) in [
            ("SELECT turso_branch_create($65535)", 65535),
            ("SELECT turso_branch_create($0001)", 1),
            ("SELECT turso_branch_create($000001)", 1),
        ] {
            assert_eq!(branch_call(sql), call(create, vec![Param(n)]), "{sql:?}");
            assert_eq!(
                slow(sql),
                call(create, vec![Param(n)]),
                "libpg_query, {sql:?}"
            );
        }
    }

    /// Every PostgreSQL keyword (libpg_query's kwlist.h, PostgreSQL 17: 491 words) as the cast of a
    /// branch call's argument, in both argument forms: the fast path reads a call only where
    /// libpg_query reads the same one, and `branch_call` answers as libpg_query does. At 472023b72
    /// the fast path took any word after `::`, so the keywords libpg_query refuses as a type (78
    /// reserved and 44 column-name keywords, 244 statements, as two reviewers measured) were calls
    /// to it and syntax errors to PostgreSQL: `turso_branch_delete('prod'::where)` deleted the
    /// branch (wire review 1 item 3).
    #[test]
    fn a_keyword_cast_is_read_as_libpg_query_reads_it() {
        #[rustfmt::skip]
        const KEYWORDS: [&str; 491] = [
            "abort", "absent", "absolute", "access", "action", "add", "admin", "after", "aggregate",
            "all", "also", "alter", "always", "analyse", "analyze", "and", "any", "array", "as",
            "asc", "asensitive", "assertion", "assignment", "asymmetric", "at", "atomic", "attach",
            "attribute", "authorization", "backward", "before", "begin", "between", "bigint",
            "binary", "bit", "boolean", "both", "breadth", "by", "cache", "call", "called",
            "cascade", "cascaded", "case", "cast", "catalog", "chain", "char", "character",
            "characteristics", "check", "checkpoint", "class", "close", "cluster", "coalesce",
            "collate", "collation", "column", "columns", "comment", "comments", "commit",
            "committed", "compression", "concurrently", "conditional", "configuration", "conflict",
            "connection", "constraint", "constraints", "content", "continue", "conversion", "copy",
            "cost", "create", "cross", "csv", "cube", "current", "current_catalog", "current_date",
            "current_role", "current_schema", "current_time", "current_timestamp", "current_user",
            "cursor", "cycle", "data", "database", "day", "deallocate", "dec", "decimal", "declare",
            "default", "defaults", "deferrable", "deferred", "definer", "delete", "delimiter",
            "delimiters", "depends", "depth", "desc", "detach", "dictionary", "disable", "discard",
            "distinct", "do", "document", "domain", "double", "drop", "each", "else", "empty",
            "enable", "encoding", "encrypted", "end", "enum", "error", "escape", "event", "except",
            "exclude", "excluding", "exclusive", "execute", "exists", "explain", "expression",
            "extension", "external", "extract", "false", "family", "fetch", "filter", "finalize",
            "first", "float", "following", "for", "force", "foreign", "format", "forward", "freeze",
            "from", "full", "function", "functions", "generated", "global", "grant", "granted",
            "greatest", "group", "grouping", "groups", "handler", "having", "header", "hold",
            "hour", "identity", "if", "ilike", "immediate", "immutable", "implicit", "import", "in",
            "include", "including", "increment", "indent", "index", "indexes", "inherit",
            "inherits", "initially", "inline", "inner", "inout", "input", "insensitive", "insert",
            "instead", "int", "integer", "intersect", "interval", "into", "invoker", "is", "isnull",
            "isolation", "join", "json", "json_array", "json_arrayagg", "json_exists",
            "json_object", "json_objectagg", "json_query", "json_scalar", "json_serialize",
            "json_table", "json_value", "keep", "key", "keys", "label", "language", "large", "last",
            "lateral", "leading", "leakproof", "least", "left", "level", "like", "limit", "listen",
            "load", "local", "localtime", "localtimestamp", "location", "lock", "locked", "logged",
            "mapping", "match", "matched", "materialized", "maxvalue", "merge", "merge_action",
            "method", "minute", "minvalue", "mode", "month", "move", "name", "names", "national",
            "natural", "nchar", "nested", "new", "next", "nfc", "nfd", "nfkc", "nfkd", "no", "none",
            "normalize", "normalized", "not", "nothing", "notify", "notnull", "nowait", "null",
            "nullif", "nulls", "numeric", "object", "of", "off", "offset", "oids", "old", "omit",
            "on", "only", "operator", "option", "options", "or", "order", "ordinality", "others",
            "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner",
            "parallel", "parameter", "parser", "partial", "partition", "passing", "password",
            "path", "placing", "plan", "plans", "policy", "position", "preceding", "precision",
            "prepare", "prepared", "preserve", "primary", "prior", "privileges", "procedural",
            "procedure", "procedures", "program", "publication", "quote", "quotes", "range", "read",
            "real", "reassign", "recheck", "recursive", "ref", "references", "referencing",
            "refresh", "reindex", "relative", "release", "rename", "repeatable", "replace",
            "replica", "reset", "restart", "restrict", "return", "returning", "returns", "revoke",
            "right", "role", "rollback", "rollup", "routine", "routines", "row", "rows", "rule",
            "savepoint", "scalar", "schema", "schemas", "scroll", "search", "second", "security",
            "select", "sequence", "sequences", "serializable", "server", "session", "session_user",
            "set", "setof", "sets", "share", "show", "similar", "simple", "skip", "smallint",
            "snapshot", "some", "source", "sql", "stable", "standalone", "start", "statement",
            "statistics", "stdin", "stdout", "storage", "stored", "strict", "string", "strip",
            "subscription", "substring", "support", "symmetric", "sysid", "system", "system_user",
            "table", "tables", "tablesample", "tablespace", "target", "temp", "template",
            "temporary", "text", "then", "ties", "time", "timestamp", "to", "trailing",
            "transaction", "transform", "treat", "trigger", "trim", "true", "truncate", "trusted",
            "type", "types", "uescape", "unbounded", "uncommitted", "unconditional", "unencrypted",
            "union", "unique", "unknown", "unlisten", "unlogged", "until", "update", "user",
            "using", "vacuum", "valid", "validate", "validator", "value", "values", "varchar",
            "variadic", "varying", "verbose", "version", "view", "views", "volatile", "when",
            "where", "whitespace", "window", "with", "within", "without", "work", "wrapper",
            "write", "xml", "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest",
            "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot", "xmlserialize", "xmltable", "year",
            "yes", "zone",
        ];
        let mut wrong = Vec::new();
        for kw in KEYWORDS {
            for arg in ["'b'", "$1"] {
                let sql = format!("SELECT turso_branch_delete({arg}::{kw})");
                let slow = slow(&sql);
                let fast = fast_branch_call(&sql);
                if (fast.is_some() && fast != slow) || branch_call(&sql) != slow {
                    wrong.push(sql);
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {} keyword casts are read unlike libpg_query, e.g. {:?}",
            wrong.len(),
            2 * KEYWORDS.len(),
            &wrong[..wrong.len().min(8)]
        );
    }

    /// A cast that changes the value or the type is not dropped: the statement is not a branch
    /// call, so it reaches the engine and fails there as PostgreSQL fails it (in PostgreSQL
    /// `'feature'::char` is 'f' and `'abc'::varchar(2)` is 'ab'; `::int`, `::bytea` and an unknown
    /// type are not text). A cast to text, or to varchar with no length, changes nothing and is
    /// read through. A quoted function name keeps its case, as in PostgreSQL, so an uppercase one
    /// names no branch function (wire review 1 item 3).
    #[test]
    fn only_a_cast_that_changes_nothing_is_read_through() {
        use PgBranchArg::{Param, Text};
        let refused = [
            "SELECT turso_branch_create('abc'::varchar(2))",
            "SELECT turso_branch_create('x'::char)",
            "SELECT turso_branch_create('x'::character)",
            "SELECT turso_branch_create('feature'::char(1))",
            "SELECT turso_branch_create('b'::character varying(1))",
            "SELECT turso_branch_create('b'::int)",
            "SELECT turso_branch_create('b'::bytea)",
            "SELECT turso_branch_create('b'::nosuchtype)",
            "SELECT turso_branch_create('b'::text[])",
            "SELECT turso_branch_create('b'::public.text)",
            "SELECT turso_branch_create('b'::\"TEXT\")",
            "SELECT turso_branch_create('b'::from)",
            "SELECT turso_branch_create($1::int)",
            "SELECT turso_branch_delete('prod'::where)",
            "SELECT \"TURSO_BRANCH_CREATE\"('b')",
            "SELECT \"turso_branch_Create\"('b')",
        ];
        for sql in refused {
            assert_eq!(fast_branch_call(sql), None, "fast path, {sql:?}");
            assert_eq!(slow(sql), None, "libpg_query, {sql:?}");
            assert_eq!(branch_call(sql), None, "branch_call, {sql:?}");
        }
        let create = "turso_branch_create";
        let b = || vec![Text("b".into())];
        let fast_forms = [
            "SELECT turso_branch_create('b'::text)",
            "SELECT turso_branch_create('b'::TEXT)",
            "SELECT turso_branch_create('b' :: varchar)",
            "SELECT turso_branch_create('b'::character varying)",
            "SELECT turso_branch_create('b'::CHARACTER\n VARYING)",
        ];
        for sql in fast_forms {
            assert_eq!(
                fast_branch_call(sql),
                call(create, b()),
                "fast path, {sql:?}"
            );
            assert_eq!(slow(sql), call(create, b()), "libpg_query, {sql:?}");
        }
        let slow_forms = [
            ("SELECT turso_branch_create('b'::pg_catalog.text)", b()),
            ("SELECT turso_branch_create('b'::\"text\")", b()),
            ("SELECT turso_branch_create('b'::varchar::text)", b()),
            (
                "SELECT turso_branch_create(true::text)",
                vec![Text("true".into())],
            ),
            ("SELECT turso_branch_create($1::varchar)", vec![Param(1)]),
            ("SELECT \"turso_branch_create\"('b')", b()),
        ];
        for (sql, args) in slow_forms {
            assert_eq!(branch_call(sql), call(create, args), "{sql:?}");
        }
    }
}
