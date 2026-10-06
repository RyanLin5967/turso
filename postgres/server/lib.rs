//! The PostgreSQL wire-protocol server behind `tursopg --server`.
//!
//! **One OS thread per session**, as PostgreSQL runs one backend process per session: the accept
//! loop hands each admitted socket to a thread of its own, which drives the protocol on a
//! single-threaded runtime and runs every statement of the session on the session's own engine
//! connection, so a statement blocks only its own session and no two sessions ever share a
//! transaction.
//!
//! A session starts on the trunk ('main') or, when its startup database name is
//! `<database>/<branch>`, on that named branch, and moves with the branch functions. Each is one
//! call into the engine's named-branch API and nothing else (no extra flush, no catalog lookup on
//! the way to the call):
//!
//! | statement                          | engine call                                             |
//! |------------------------------------|---------------------------------------------------------|
//! | `SELECT turso_branch_create('b')`  | `Connection::create_branch` on the session's connection |
//! | `SELECT turso_branch_switch('b')`  | `Database::connect_named` (`'main'`: the trunk)          |
//! | `SELECT turso_branch_delete('b')`  | `Database::drop_branch`                                 |
//! | `SELECT turso_branch_current()`    | none                                                    |
//!
//! A branch serves one connection at a time (the engine refuses a second), so a branch serves one
//! session at a time. Transactions follow PostgreSQL: an error inside a transaction block aborts
//! it, and ReadyForQuery reports the session's real state.

pub mod counters;

use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::{self, StreamExt};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use turso_core::{Database, LimboError, Value};
use turso_pg::{
    attach_schema_files, branch_call, split_statements, PgBranchArg, PgBranchCall, PgConnection,
};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{send_ready_for_query, ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{
    ClientInfo, ClientPortalStore, PgWireConnectionState, PgWireServerHandlers, Type,
    METADATA_DATABASE,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::{DataRow, RowDescription};
use pgwire::messages::extendedquery::Sync as PgSync;
use pgwire::messages::response::{EmptyQueryResponse, ReadyForQuery, TransactionStatus};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use pgwire::types::format::FormatOptions;

/// The name of the trunk in the branch functions and in a startup database name.
pub const TRUNK: &str = "main";

/// The default session limit: C + 16 at the benchmark's largest C (1024), with room to spare
/// (PREREG v1 §6 Server configuration (1)).
pub const DEFAULT_MAX_CONNECTIONS: usize = 2048;

/// How long a branch call waits, by default, for a lock another session holds before it fails
/// with 55P03 (PostgreSQL's statements wait on locks; its default lock_timeout is no limit).
pub const DEFAULT_LOCK_WAIT_MS: u64 = 60_000;

/// The stack of a session thread: the main thread's default, since a session runs the same
/// statements a REPL would.
const SESSION_STACK: usize = 8 << 20;

/// The options `tursopg` opens a database with, as a server and in its REPL: every frontend
/// feature on, and named branches kept per `branches`. Anything that must open a database exactly as
/// the server does (an embedded measurement of the same operations) takes them from here.
pub fn database_opts(branches: turso_core::branch::BranchDurability) -> turso_core::DatabaseOpts {
    turso_core::DatabaseOpts::new()
        .with_views(true)
        .with_custom_types(true)
        .with_encryption(true)
        .with_index_method(true)
        .with_autovacuum(true)
        .with_attach(true)
        .with_generated_columns(true)
        .with_branch_durability(branches)
}

pub struct TursoPgServer {
    address: String,
    shared: Arc<Shared>,
    interrupt_count: Arc<AtomicUsize>,
}

/// What every session of one server shares.
struct Shared {
    db: Arc<Database>,
    db_file: String,
    max_connections: usize,
    /// How long a branch call waits for a lock another session holds (see [`Session::waiting`]).
    lock_wait: std::time::Duration,
    /// Sessions admitted and not yet ended.
    live: AtomicUsize,
    /// The named branches a session is on, or that a delete is releasing: a delete is refused
    /// while a session is on the branch (55006, as PostgreSQL refuses DROP DATABASE of a database in
    /// use), and a switch while the branch is being deleted. One hash operation under the lock per
    /// switch, per leave and two per delete; no engine call is made under it.
    in_use: Mutex<std::collections::HashMap<String, BranchUse>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchUse {
    Held,
    Deleting,
}

impl Shared {
    fn uses(&self) -> MutexGuard<'_, std::collections::HashMap<String, BranchUse>> {
        self.in_use.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Claim `name` for a session about to connect to it.
    fn claim(&self, name: &str, severity: &str) -> SqlResult<()> {
        let mut uses = self.uses();
        if let Some(u) = uses.get(name) {
            let mut e = in_use_error(name, *u);
            e.severity = severity.to_string();
            return Err(e);
        }
        uses.insert(name.to_string(), BranchUse::Held);
        Ok(())
    }

    /// A session left `name`, or never reached it, or a delete of it ended.
    fn release(&self, name: &str) {
        self.uses().remove(name);
    }

    /// Mark `name` as being deleted: refused while a session is on it.
    fn begin_delete(&self, name: &str) -> SqlResult<()> {
        let mut uses = self.uses();
        if let Some(u) = uses.get(name) {
            return Err(in_use_error(name, *u));
        }
        uses.insert(name.to_string(), BranchUse::Deleting);
        Ok(())
    }
}

fn in_use_error(name: &str, u: BranchUse) -> Box<ErrorInfo> {
    match u {
        BranchUse::Held => error(
            "55006",
            format!("branch \"{name}\" is in use by another session"),
        ),
        BranchUse::Deleting => error("55006", format!("branch \"{name}\" is being deleted")),
    }
}

impl TursoPgServer {
    pub fn new(
        address: String,
        db_file: String,
        db: Arc<Database>,
        max_connections: usize,
        lock_wait: std::time::Duration,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            shared: Arc::new(Shared {
                db,
                db_file,
                max_connections,
                lock_wait,
                live: AtomicUsize::new(0),
                in_use: Mutex::new(std::collections::HashMap::new()),
            }),
            interrupt_count,
        }
    }

    pub fn run(&self) -> anyhow::Result<()> {
        raise_open_file_limit(self.shared.max_connections);
        // The accept loop needs no more than this thread; every session gets a thread of its own.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(self.run_async())
    }

    async fn run_async(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!(
            "PostgreSQL server listening on {} (database: {}, at most {} sessions)",
            self.address, self.shared.db_file, self.shared.max_connections
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            self.admit(socket, addr);
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\nShutting down PostgreSQL server...");
                    break;
                }
            }

            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                println!("Shutting down PostgreSQL server...");
                break;
            }
        }

        Ok(())
    }

    /// Start a session thread for `socket`, or refuse it with 53300 when the server is full.
    fn admit(&self, socket: tokio::net::TcpStream, addr: std::net::SocketAddr) {
        let Some(slot) = SessionSlot::take(&self.shared) else {
            // As PostgreSQL does: the client gets FATAL 53300 at startup. The refusal is cheap and
            // runs here, on the accept thread.
            tokio::spawn(async move {
                let _ = process_socket(socket, None, Refusal).await;
            });
            return;
        };
        let socket = match socket.into_std() {
            Ok(s) => s,
            Err(e) => {
                error!("Error detaching connection from {}: {}", addr, e);
                return;
            }
        };
        let shared = self.shared.clone();
        let spawned = std::thread::Builder::new()
            .name("pg-session".to_string())
            .stack_size(SESSION_STACK)
            .spawn(move || run_session(shared, socket, addr, slot));
        if let Err(e) = spawned {
            error!("Error starting a session thread for {}: {}", addr, e);
        }
    }
}

/// A session's place under the connection limit, given back when the session ends.
struct SessionSlot(Arc<Shared>);

impl SessionSlot {
    fn take(shared: &Arc<Shared>) -> Option<Self> {
        let admitted = shared
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < shared.max_connections).then_some(n + 1)
            })
            .is_ok();
        admitted.then(|| Self(shared.clone()))
    }
}

impl Drop for SessionSlot {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The body of a session thread: the protocol on a runtime of this thread alone, until the client
/// goes. The session's connections close when it returns.
fn run_session(
    shared: Arc<Shared>,
    socket: std::net::TcpStream,
    addr: std::net::SocketAddr,
    _slot: SessionSlot,
) {
    // IO and timers only: a signal driver would cost every session a socket pair.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(
                "Error starting the runtime of the session from {}: {}",
                addr, e
            );
            return;
        }
    };
    rt.block_on(async move {
        let socket = match tokio::net::TcpStream::from_std(socket) {
            Ok(s) => s,
            Err(e) => {
                error!("Error attaching connection from {}: {}", addr, e);
                return;
            }
        };
        let session = Arc::new(Session::new(shared));
        if let Err(e) = process_socket(socket, None, SessionHandlers(session)).await {
            error!("Error processing connection from {}: {}", addr, e);
        }
    });
}

/// Raise this process's open-file soft limit so `max_connections` sessions fit. A session holds
/// its socket and its runtime's descriptors (tokio's I/O driver: 4 measured with tokio 1.47 on
/// macOS, a kqueue among them; 5 per session in all, read with lsof on a live server), so 6 are
/// budgeted per session. A limit that cannot be raised that far is reported, and the server then
/// refuses connections the kernel cannot open.
fn raise_open_file_limit(max_connections: usize) {
    const FDS_PER_SESSION: u64 = 6;
    #[cfg(unix)]
    unsafe {
        let want = (max_connections as u64)
            .saturating_mul(FDS_PER_SESSION)
            .saturating_add(256);
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let mut target = (want as libc::rlim_t).min(lim.rlim_max);
        // macOS refuses a soft limit above kern.maxfilesperproc with EINVAL even under a higher
        // hard limit: halve until the kernel accepts.
        while target > lim.rlim_cur {
            let next = libc::rlimit {
                rlim_cur: target,
                rlim_max: lim.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &next) == 0 {
                lim.rlim_cur = target;
                break;
            }
            target /= 2;
        }
        if lim.rlim_cur < want as libc::rlim_t {
            warn!(
                "open-file limit {} is below the {} that {} sessions need",
                lim.rlim_cur, want, max_connections
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The connection-limit refusal
// ---------------------------------------------------------------------------

/// The handlers of a connection over the limit: its startup fails with FATAL 53300.
struct Refusal;

impl PgWireServerHandlers for Refusal {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(Refusal)
    }
}

#[async_trait]
impl StartupHandler for Refusal {
    async fn on_startup<C>(
        &self,
        _client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(_) = message {
            return Err(fatal("53300", "sorry, too many clients already"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

struct Session {
    shared: Arc<Shared>,
    state: Mutex<SessionState>,
    query_parser: Arc<NoopQueryParser>,
}

#[derive(Default)]
struct SessionState {
    /// The session's trunk connection, opened at its first use and kept while the session is on a
    /// branch, so switching back to the trunk opens nothing.
    trunk: Option<PgConnection>,
    /// The named branch the session is on, with the branch's connection.
    branch: Option<(String, PgConnection)>,
    /// A statement failed inside a transaction block: as in PostgreSQL, everything but the block's
    /// end (or a ROLLBACK TO SAVEPOINT) is refused with 25P02 until then.
    aborted: bool,
    /// The branch a switch just left, released in [`Shared`]'s map when the switch's statement ends.
    left: Option<String>,
}

/// pgwire's handler set for one session: every handler is the session itself.
struct SessionHandlers(Arc<Session>);

impl PgWireServerHandlers for SessionHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.0.clone()
    }
}

impl Session {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            state: Mutex::new(SessionState::default()),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    /// The session's state. Only the session's own thread takes the lock, so it never waits; a
    /// statement that panicked mid-way leaves the state as it was, which is still valid.
    fn state(&self) -> MutexGuard<'_, SessionState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn open_trunk(&self) -> SqlResult<PgConnection> {
        let conn = PgConnection::new(self.shared.db.connect().map_err(|e| engine_info(&e))?);
        attach_schema_files(&conn, &self.shared.db_file);
        Ok(conn)
    }

    /// The connection the session's statements run on: its branch's, or its trunk connection.
    fn current(&self, st: &mut SessionState) -> SqlResult<PgConnection> {
        if let Some((_, conn)) = &st.branch {
            return Ok(conn.clone());
        }
        if st.trunk.is_none() {
            st.trunk = Some(self.open_trunk()?);
        }
        Ok(st.trunk.clone().expect("opened above"))
    }

    /// ReadyForQuery's status, read from the session's state rather than inferred from the
    /// statements it ran.
    fn transaction_status(&self) -> TransactionStatus {
        let st = self.state();
        if st.aborted {
            return TransactionStatus::Error;
        }
        let conn = match &st.branch {
            Some((_, conn)) => Some(conn),
            None => st.trunk.as_ref(),
        };
        match conn {
            Some(conn) if !conn.inner().get_auto_commit() => TransactionStatus::Transaction,
            _ => TransactionStatus::Idle,
        }
    }

    /// Put a session that asked for `<database>/<branch>` at startup on that branch.
    fn start_on_branch(&self, database: &str) -> PgWireResult<()> {
        let Some((_, branch)) = database.rsplit_once('/') else {
            return Ok(());
        };
        if branch.is_empty() || branch == TRUNK {
            return Ok(());
        }
        self.shared
            .claim(branch, "FATAL")
            .map_err(PgWireError::UserError)?;
        let conn = match self.waiting(|| self.shared.db.connect_named(branch)) {
            Ok(conn) => conn,
            Err(e) => {
                self.shared.release(branch);
                return Err(PgWireError::UserError(
                    self.switch_error(branch, &e, "FATAL"),
                ));
            }
        };
        self.state().branch = Some((branch.to_string(), PgConnection::new(conn)));
        Ok(())
    }

    /// Every statement of one simple-protocol query, in order, up to and including the first that
    /// fails.
    fn simple(&self, query: &str) -> Vec<Response> {
        // A query that is one branch call goes straight to the engine: no split, no parse (L5).
        if let Some(call) = branch_call(query) {
            return vec![self
                .run(query, Some(call), None, &Format::UnifiedText)
                .unwrap_or_else(Response::Error)];
        }
        let statements = match split_statements(query) {
            Ok(s) => s,
            Err(e) => return vec![Response::Error(engine_info(&e))],
        };
        let mut responses = Vec::with_capacity(statements.len());
        for sql in &statements {
            match self.statement(sql, None, &Format::UnifiedText) {
                Ok(r) => responses.push(r),
                Err(e) => {
                    responses.push(Response::Error(e));
                    break;
                }
            }
        }
        responses
    }

    /// Run one statement on the session, `portal` carrying its bound parameters (extended
    /// protocol).
    fn statement(
        &self,
        sql: &str,
        portal: Option<&Portal<String>>,
        format: &Format,
    ) -> SqlResult<Response> {
        self.run(sql, branch_call(sql), portal, format)
    }

    /// [`Session::statement`] with the statement's branch call, if it is one, already read.
    fn run(
        &self,
        sql: &str,
        call: Option<PgBranchCall>,
        portal: Option<&Portal<String>>,
        format: &Format,
    ) -> SqlResult<Response> {
        let verb = TxVerb::of(sql);
        let mut st = self.state();
        let conn = self.current(&mut st)?;
        let in_tx = !conn.inner().get_auto_commit();
        if st.aborted {
            match verb {
                TxVerb::Commit | TxVerb::Rollback => {
                    // PostgreSQL ends a failed block with ROLLBACK whichever was asked.
                    if in_tx {
                        conn.execute("ROLLBACK").map_err(|e| engine_info(&e))?;
                    }
                    st.aborted = false;
                    return Ok(Response::Execution(Tag::new("ROLLBACK")));
                }
                TxVerb::RollbackTo => {}
                _ => {
                    return Err(error(
                        "25P02",
                        "current transaction is aborted, commands ignored until end of \
                         transaction block"
                            .to_string(),
                    ))
                }
            }
        }
        match verb {
            // PostgreSQL warns and carries on for these; the engine would refuse them.
            TxVerb::Begin if in_tx => return Ok(Response::Execution(Tag::new("BEGIN"))),
            TxVerb::Commit if !in_tx => return Ok(Response::Execution(Tag::new("COMMIT"))),
            TxVerb::Rollback if !in_tx => return Ok(Response::Execution(Tag::new("ROLLBACK"))),
            _ => {}
        }
        let result = match call {
            Some(call) => self.branch(&mut st, &conn, &call, portal, format),
            None => {
                drop(st);
                let r = self.engine_statement(&conn, sql, portal, format);
                st = self.state();
                r
            }
        };
        if result.is_err() {
            match verb {
                // A failed COMMIT ends the block, as in PostgreSQL: whatever the engine kept of
                // the transaction is rolled back and the session is idle.
                TxVerb::Commit => {
                    if !conn.inner().get_auto_commit() {
                        let _ = conn.execute("ROLLBACK");
                    }
                    st.aborted = false;
                }
                TxVerb::Rollback => st.aborted = false,
                // Inside a block any other error aborts it. Whether the session was in a block is
                // read from before the statement: some errors make the engine roll the whole
                // transaction back itself, and reading autocommit after them would let the rest of
                // the block commit statement by statement (wire review 1 item 1).
                _ if in_tx => st.aborted = true,
                _ => {}
            }
        }
        // A switch's branch is free for a delete once the last handle on its connection is gone:
        // this statement's.
        drop(conn);
        if let Some(left) = st.left.take() {
            self.shared.release(&left);
        }
        if result.is_ok() && verb == TxVerb::RollbackTo {
            st.aborted = false;
        }
        result
    }

    fn engine_statement(
        &self,
        conn: &PgConnection,
        sql: &str,
        portal: Option<&Portal<String>>,
        format: &Format,
    ) -> SqlResult<Response> {
        let mut stmt = conn.prepare(sql).map_err(|e| engine_info(&e))?;
        self.shared.cleanup_dropped_schema_file(sql);
        if let Some(portal) = portal {
            bind_portal_parameters(&mut stmt, portal).map_err(wire_info)?;
        }
        let r = if stmt.num_columns() == 0 || is_pg_non_query(sql) {
            execute_non_query(&mut stmt, sql)
        } else {
            // An extended-protocol client took the column types from Describe, which runs nothing:
            // its rows keep those types. A simple-protocol reply carries its own RowDescription.
            execute_query(
                &mut stmt,
                format,
                portal.is_none(),
                &conn.inner().current_schema(),
            )
        };
        r.map_err(wire_info)
    }

    // ---- branch functions ----

    fn branch(
        &self,
        st: &mut SessionState,
        conn: &PgConnection,
        call: &PgBranchCall,
        portal: Option<&Portal<String>>,
        format: &Format,
    ) -> SqlResult<Response> {
        let f = call.function.as_str();
        if f == "turso_branch_current" {
            arity(call, 0)?;
            let name = st.branch.as_ref().map_or(TRUNK, |(n, _)| n.as_str());
            return Ok(one_text(f, name, format));
        }
        if f == "turso_branch_stats" {
            arity(call, 0)?;
            return Ok(stats_row(format));
        }
        if !matches!(
            f,
            "turso_branch_create" | "turso_branch_switch" | "turso_branch_delete"
        ) {
            return Err(error(
                "42883",
                format!("function {f} does not exist; the branch functions are turso_branch_create(name), turso_branch_switch(name), turso_branch_delete(name), turso_branch_current() and turso_branch_stats()"),
            ));
        }
        arity(call, 1)?;
        let name = text_arg(&call.args[0], portal)?;
        // As PostgreSQL refuses CREATE DATABASE and DROP DATABASE in a transaction block. The
        // engine refuses a fork there too; a switch would abandon the transaction.
        if !conn.inner().get_auto_commit() {
            return Err(error(
                "25001",
                format!("{f} cannot run inside a transaction block"),
            ));
        }
        match f {
            "turso_branch_create" => {
                if name == TRUNK {
                    return Err(error(
                        "42939",
                        format!("branch name \"{TRUNK}\" is reserved for the trunk"),
                    ));
                }
                match self.waiting(|| conn.inner().create_branch(&name)) {
                    Ok(id) => Ok(one_int8(f, id.0 as i64, format)),
                    Err(e) => Err(self.create_error(&name, &e)),
                }
            }
            "turso_branch_switch" => {
                let on = st.branch.as_ref().map_or(TRUNK, |(n, _)| n.as_str());
                if on == name {
                    return Ok(one_text(f, &name, format));
                }
                let left = if name == TRUNK {
                    let trunk = match st.trunk.take() {
                        Some(t) => t,
                        None => self.open_trunk()?,
                    };
                    trunk.adopt_session_of(conn);
                    st.trunk = Some(trunk);
                    // Dropping the branch's connection closes the branch for connections.
                    st.branch.take()
                } else {
                    self.shared.claim(&name, "ERROR")?;
                    let opened = match self.waiting(|| self.shared.db.connect_named(&name)) {
                        Ok(opened) => opened,
                        Err(e) => {
                            self.shared.release(&name);
                            return Err(self.switch_error(&name, &e, "ERROR"));
                        }
                    };
                    let next = PgConnection::new(opened);
                    next.adopt_session_of(conn);
                    st.branch.replace((name.clone(), next))
                };
                // The branch left is free for a delete once its connection is closed: `run`
                // releases it after the statement's own handle on the connection goes.
                if let Some((left, left_conn)) = left {
                    drop(left_conn);
                    st.left = Some(left);
                }
                Ok(one_text(f, &name, format))
            }
            _ if st.branch.as_ref().is_some_and(|(on, _)| *on == name) => Err(error(
                "55006",
                format!(
                    "cannot delete branch \"{name}\": this session is on it; switch to another \
                     branch first"
                ),
            )),
            _ => {
                self.shared.begin_delete(&name)?;
                let dropped = self.waiting(|| self.shared.db.drop_branch(&name));
                self.shared.release(&name);
                match dropped {
                    Ok(_) => Ok(one_text(f, &name, format)),
                    Err(e) => Err(self.missing_or(&name, &e, "ERROR")),
                }
            }
        }
    }

    /// The engine call `f`, retried while a lock it needs is held by another session
    /// ([`LimboError::Busy`], or a snapshot a concurrent commit outdated), sleeping on SQLite's
    /// default busy schedule (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50 ms, then 100 ms) for up to
    /// the server's lock wait. Only this session's thread sleeps.
    fn waiting<T>(&self, mut f: impl FnMut() -> turso_core::Result<T>) -> turso_core::Result<T> {
        const DELAYS_MS: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
        let deadline = std::time::Instant::now() + self.shared.lock_wait;
        let mut attempt = 0;
        loop {
            match f() {
                Err(LimboError::Busy | LimboError::BusySnapshot) => {
                    let delay = std::time::Duration::from_millis(
                        DELAYS_MS[attempt.min(DELAYS_MS.len() - 1)],
                    );
                    attempt += 1;
                    if std::time::Instant::now() + delay > deadline {
                        return f();
                    }
                    std::thread::sleep(delay);
                }
                r => return r,
            }
        }
    }

    /// A failed create: the name is taken (42P04, as CREATE DATABASE of an existing name), or the
    /// engine's own refusal. Classified by looking the name up, on the error path only.
    fn create_error(&self, name: &str, e: &LimboError) -> Box<ErrorInfo> {
        match self.shared.db.branch_named(name) {
            Ok(Some(_)) => error("42P04", format!("branch \"{name}\" already exists")),
            _ => engine_info(e),
        }
    }

    fn switch_error(&self, name: &str, e: &LimboError, severity: &str) -> Box<ErrorInfo> {
        self.missing_or(name, e, severity)
    }

    /// A branch call on a name with no branch: 3D000, as for a database that does not exist; on a
    /// branch another session holds: 55006 (object_in_use), as for a database in use. Classified on
    /// the error path only.
    fn missing_or(&self, name: &str, e: &LimboError, severity: &str) -> Box<ErrorInfo> {
        let mut info = match self.shared.db.branch_named(name) {
            Ok(None) => error("3D000", format!("branch \"{name}\" does not exist")),
            Ok(Some(_)) if e.to_string().contains("already has an open connection") => {
                let mut info = error(
                    "55006",
                    format!("branch \"{name}\" is in use by another session"),
                );
                info.detail = Some(e.to_string());
                info
            }
            _ => engine_info(e),
        };
        info.severity = severity.to_string();
        info
    }
}

impl Drop for Session {
    /// The session's branch is free once its connection is closed.
    fn drop(&mut self) {
        let left = self.state().branch.take();
        if let Some((name, conn)) = left {
            drop(conn);
            self.shared.release(&name);
        }
    }
}

impl Shared {
    /// After a DROP SCHEMA query succeeds, delete the schema's database file.
    /// Uses simple string matching to detect DROP SCHEMA statements.
    fn cleanup_dropped_schema_file(&self, query: &str) {
        if self.db_file == ":memory:" {
            return;
        }
        // Simple detection: look for DROP SCHEMA pattern
        let trimmed = query.trim().to_lowercase();
        if !trimmed.starts_with("drop schema") {
            return;
        }
        // Extract schema name: "drop schema [if exists] <name> [cascade|restrict]"
        let rest = trimmed.strip_prefix("drop schema").unwrap().trim();
        let rest = rest
            .strip_prefix("if exists")
            .map(|s| s.trim())
            .unwrap_or(rest);
        // Take the first word as the schema name
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"');
        if name.is_empty() || name == "public" {
            return;
        }
        let parent = std::path::Path::new(&self.db_file)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let schema_file = parent.join(format!("turso-postgres-schema-{name}.db"));
        if schema_file.exists() {
            if let Err(e) = std::fs::remove_file(&schema_file) {
                tracing::warn!("Failed to delete schema file {:?}: {}", schema_file, e);
            } else {
                tracing::info!("Deleted schema file {:?}", schema_file);
            }
            // Also clean up WAL and SHM files
            let wal = schema_file.with_extension("db-wal");
            let shm = schema_file.with_extension("db-shm");
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(shm);
        }
    }
}

/// What a statement does to the transaction block, from its leading keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxVerb {
    Begin,
    Commit,
    Rollback,
    RollbackTo,
    Other,
}

impl TxVerb {
    fn of(sql: &str) -> Self {
        let mut words = sql
            .split(|c: char| c.is_ascii_whitespace() || c == ';')
            .filter(|w| !w.is_empty());
        let first = words.next().unwrap_or("");
        let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
        if is(first, "BEGIN") || is(first, "START") {
            TxVerb::Begin
        } else if is(first, "COMMIT") || is(first, "END") {
            match words.next() {
                Some(w) if is(w, "PREPARED") => TxVerb::Other,
                _ => TxVerb::Commit,
            }
        } else if is(first, "ROLLBACK") || is(first, "ABORT") {
            let mut next = words.next();
            if next.is_some_and(|w| is(w, "WORK") || is(w, "TRANSACTION")) {
                next = words.next();
            }
            match next {
                Some(w) if is(w, "TO") => TxVerb::RollbackTo,
                Some(w) if is(w, "PREPARED") => TxVerb::Other,
                _ => TxVerb::Rollback,
            }
        } else {
            TxVerb::Other
        }
    }
}

fn arity(call: &PgBranchCall, n: usize) -> SqlResult<()> {
    if call.args.len() == n {
        return Ok(());
    }
    Err(error(
        "42883",
        format!(
            "function {}() takes {n} argument{}, not {}",
            call.function,
            if n == 1 { "" } else { "s" },
            call.args.len()
        ),
    ))
}

/// A branch name argument: a string literal, or a `$n` bound to text.
fn text_arg(arg: &PgBranchArg, portal: Option<&Portal<String>>) -> SqlResult<String> {
    match arg {
        PgBranchArg::Text(s) => Ok(s.clone()),
        PgBranchArg::Null => Err(error("22004", "a branch name must not be null".to_string())),
        PgBranchArg::Bool(_) => Err(error("42804", "a branch name is text".to_string())),
        PgBranchArg::Param(n) => {
            let bytes = portal
                .and_then(|p| p.parameters.get(n - 1))
                .ok_or_else(|| error("08P01", format!("parameter ${n} is not bound")))?;
            let Some(bytes) = bytes else {
                return Err(error("22004", "a branch name must not be null".to_string()));
            };
            String::from_utf8(bytes.to_vec())
                .map_err(|e| error("22021", format!("invalid UTF-8 in parameter ${n}: {e}")))
        }
    }
}

fn one_row(
    f: &str,
    pg_type: Type,
    format: &Format,
    encode: impl FnOnce(&mut DataRowEncoder) -> PgWireResult<()>,
) -> Response {
    let header = Arc::new(vec![FieldInfo::new(
        f.to_string(),
        None,
        None,
        pg_type,
        format.format_for(0),
    )]);
    let mut encoder = DataRowEncoder::new(header.clone());
    let row = encode(&mut encoder).and_then(|()| encoder.finish());
    Response::Query(QueryResponse::new(header, stream::iter(vec![row])))
}

fn one_text(f: &str, value: &str, format: &Format) -> Response {
    one_row(f, Type::TEXT, format, |e| e.encode_field(&value))
}

fn one_int8(f: &str, value: i64, format: &Format) -> Response {
    one_row(f, Type::INT8, format, |e| e.encode_field(&value))
}

const STATS_COLUMNS: [&str; 4] = ["unix_syscalls", "mach_syscalls", "instructions", "cycles"];

/// turso_branch_stats(): the server process's counters ([`counters::process_counters`]), read as
/// the call runs; NULLs where the platform does not count them.
fn stats_row(format: &Format) -> Response {
    let header = Arc::new(stats_fields(format));
    let mut encoder = DataRowEncoder::new(header.clone());
    let values = counters::process_counters()
        .map(|c| [c.unix_syscalls, c.mach_syscalls, c.instructions, c.cycles].map(|v| v as i64));
    let row = (0..STATS_COLUMNS.len())
        .try_for_each(|i| encoder.encode_field(&values.map(|v| v[i])))
        .and_then(|()| encoder.finish());
    Response::Query(QueryResponse::new(header, stream::iter(vec![row])))
}

fn stats_fields(format: &Format) -> Vec<FieldInfo> {
    STATS_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, name)| {
            FieldInfo::new(
                name.to_string(),
                None,
                None,
                Type::INT8,
                format.format_for(i),
            )
        })
        .collect()
}

/// The row a branch call returns, for Describe.
fn branch_call_fields(call: &PgBranchCall, format: &Format) -> Vec<FieldInfo> {
    if call.function == "turso_branch_stats" {
        return stats_fields(format);
    }
    let pg_type = if call.function == "turso_branch_create" {
        Type::INT8
    } else {
        Type::TEXT
    };
    vec![FieldInfo::new(
        call.function.clone(),
        None,
        None,
        pg_type,
        format.format_for(0),
    )]
}

/// A statement's failure, boxed: an `ErrorInfo` is ~280 bytes and failure is the rare path.
type SqlResult<T> = Result<T, Box<ErrorInfo>>;

fn error(code: &str, message: String) -> Box<ErrorInfo> {
    Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    ))
}

fn fatal(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_string(),
        code.to_string(),
        message.to_string(),
    )))
}

/// The SQLSTATE an engine error is reported with. Lock contention is PostgreSQL's
/// lock_not_available, and a stale snapshot its serialization_failure: both tell a client to retry.
fn engine_info(e: &LimboError) -> Box<ErrorInfo> {
    let code = match e {
        LimboError::Busy => "55P03",
        LimboError::BusySnapshot => "40001",
        _ => "XX000",
    };
    error(code, e.to_string())
}

fn wire_info(e: PgWireError) -> Box<ErrorInfo> {
    match e {
        PgWireError::UserError(info) => info,
        other => error("XX000", other.to_string()),
    }
}

fn engine_error(e: LimboError) -> PgWireError {
    PgWireError::UserError(engine_info(&e))
}

// ---------------------------------------------------------------------------
// pgwire handlers
// ---------------------------------------------------------------------------

#[async_trait]
impl NoopStartupHandler for Session {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match client.metadata().get(METADATA_DATABASE).cloned() {
            Some(database) => self.start_on_branch(&database),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for Session {
    /// pgwire's `_on_query`, except that ReadyForQuery carries the session's real transaction
    /// state (see [`Session::transaction_status`]), a failed statement's ErrorResponse follows the
    /// results of the statements before it, and the whole reply is written with one flush
    /// (pgwire's helpers flush after RowDescription, after CommandComplete and after
    /// ReadyForQuery: three writes for a one-row SELECT).
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let trimmed = query.query.trim();
        let responses = if trimmed.is_empty() || trimmed == ";" {
            vec![Response::EmptyQuery]
        } else {
            self.simple(&query.query)
        };
        for response in responses {
            match response {
                Response::Query(mut results) => {
                    let fields = results.row_schema().iter().map(Into::into).collect();
                    client
                        .feed(PgWireBackendMessage::RowDescription(RowDescription::new(
                            fields,
                        )))
                        .await?;
                    let tag = results.command_tag().to_owned();
                    let mut rows = 0;
                    let data = results.data_rows();
                    while let Some(row) = data.next().await {
                        client.feed(PgWireBackendMessage::DataRow(row?)).await?;
                        rows += 1;
                    }
                    client
                        .feed(PgWireBackendMessage::CommandComplete(
                            Tag::new(&tag).with_rows(rows).into(),
                        ))
                        .await?;
                }
                Response::Execution(tag)
                | Response::TransactionStart(tag)
                | Response::TransactionEnd(tag) => {
                    client
                        .feed(PgWireBackendMessage::CommandComplete(tag.into()))
                        .await?;
                }
                Response::Error(e) => {
                    client
                        .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
                        .await?;
                }
                Response::EmptyQuery => {
                    client
                        .feed(PgWireBackendMessage::EmptyQueryResponse(
                            EmptyQueryResponse::new(),
                        ))
                        .await?;
                }
                // This server answers COPY FROM a file itself and never streams COPY.
                Response::CopyIn(_) | Response::CopyOut(_) | Response::CopyBoth(_) => {
                    return Err(PgWireError::UserError(error(
                        "0A000",
                        "COPY over the protocol is not supported".to_string(),
                    )));
                }
            }
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
        let status = self.transaction_status();
        client.set_transaction_status(status);
        client
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                status,
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }

    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(self.simple(query))
    }
}

#[async_trait]
impl ExtendedQueryHandler for Session {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.statement(
            &portal.statement.statement,
            Some(portal),
            &portal.result_column_format,
        )
        .map_err(PgWireError::UserError)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();
        if let Some(call) = branch_call(&target.statement) {
            let fields = branch_call_fields(&call, &Format::UnifiedText);
            return Ok(DescribeStatementResponse::new(param_types, fields));
        }
        let conn = self
            .current(&mut self.state())
            .map_err(PgWireError::UserError)?;
        let stmt = conn.prepare(&target.statement).map_err(engine_error)?;
        let fields = build_field_info(&stmt, &Format::UnifiedText);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if let Some(call) = branch_call(&portal.statement.statement) {
            let fields = branch_call_fields(&call, &portal.result_column_format);
            return Ok(DescribePortalResponse::new(fields));
        }
        let conn = self
            .current(&mut self.state())
            .map_err(PgWireError::UserError)?;
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(engine_error)?;
        let fields = build_field_info(&stmt, &portal.result_column_format);
        Ok(DescribePortalResponse::new(fields))
    }

    /// ReadyForQuery after Sync carries the session's real transaction state.
    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let status = self.transaction_status();
        client.set_transaction_status(status);
        send_ready_for_query(client, status).await?;
        Ok(())
    }
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_core::Statement, format: &Format) -> Vec<FieldInfo> {
    field_info(stmt, format, |i| resolve_pg_type_for_column(stmt, i))
}

fn field_info(
    stmt: &turso_core::Statement,
    format: &Format,
    pg_type: impl Fn(usize) -> Type,
) -> Vec<FieldInfo> {
    (0..stmt.num_columns())
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            FieldInfo::new(name, None, None, pg_type(i), format.format_for(i))
        })
        .collect()
}

/// A result column's type from the statement alone, if the engine can tell it (a table column, a
/// literal, a cast, an operator over known types); `None` for one it cannot, such as an aggregate.
fn static_pg_type(stmt: &turso_core::Statement, idx: usize) -> Option<Type> {
    stmt.get_column_type_info(idx)
        .ok()
        .flatten()
        .map(|_| resolve_pg_type_for_column(stmt, idx))
}

/// The type of a result column the statement could not type, from the values it returned: a
/// SQLite value's storage class is its type. Integers only: bigint (PostgreSQL's type for count and
/// for the sum of integers); any real among numbers: double precision; text: text; blobs: bytea.
/// Mixed classes, or no non-null value at all, stay text.
fn infer_pg_type(values: &[Vec<Value>], idx: usize) -> Type {
    use turso_core::Numeric;
    let (mut ints, mut reals, mut texts, mut blobs) = (0usize, 0usize, 0usize, 0usize);
    for row in values {
        match row.get(idx) {
            Some(Value::Numeric(Numeric::Integer(_))) => ints += 1,
            Some(Value::Numeric(Numeric::Float(_))) => reals += 1,
            Some(Value::Text(_)) => texts += 1,
            Some(Value::Blob(_)) => blobs += 1,
            _ => {}
        }
    }
    match (ints, reals, texts, blobs) {
        (1.., 0, 0, 0) => Type::INT8,
        (_, 1.., 0, 0) => Type::FLOAT8,
        (0, 0, 0, 1..) => Type::BYTEA,
        _ => Type::TEXT,
    }
}

/// Decide the PG wire type for a result column.
///
/// `get_column_type_info` is the single source of truth: it handles direct
/// table-column references (declared name, array depth, custom-type kind,
/// resolved primitive), bare literals (`SELECT 42` -> INTEGER), and typed
/// expressions like CAST. When it returns `Ok(None)` (no determined primitive)
/// or `Err` (custom types not enabled — won't happen in PG mode, but the wire
/// layer shouldn't panic if it does), the safe default is TEXT;
/// `encode_value` already handles per-value type mismatches.
fn resolve_pg_type_for_column(stmt: &turso_core::Statement, idx: usize) -> Type {
    use turso_core::ColumnTypeKind;

    let Some(info) = stmt.get_column_type_info(idx).ok().flatten() else {
        return Type::TEXT;
    };
    // STRUCT and UNION columns live as BLOBs on disk, but exposing them as
    // BYTEA would force clients to deal with raw bytes. Map them to JSONB so
    // libpq/psql/JDBC see structured data they can introspect.
    let mut base = match info.kind {
        ColumnTypeKind::Struct | ColumnTypeKind::Union => Type::JSONB,
        _ => {
            // Prefer the declared name (the user-visible type), then fall
            // back to the resolved base for custom/domain types whose
            // declared name isn't in the lookup table.
            let mapped = sqlite_type_to_pg_type(&info.declared_name);
            if mapped == Type::TEXT {
                info.base_type
                    .as_deref()
                    .map(sqlite_type_to_pg_type)
                    .unwrap_or(Type::TEXT)
            } else {
                mapped
            }
        }
    };
    if info.array_dimensions > 0 {
        base = scalar_pg_type_to_array_type(&base);
    }
    base
}

/// Map a scalar PG type to its array counterpart.
fn scalar_pg_type_to_array_type(scalar: &Type) -> Type {
    if *scalar == Type::INT4 {
        Type::INT4_ARRAY
    } else if *scalar == Type::INT8 {
        Type::INT8_ARRAY
    } else if *scalar == Type::FLOAT8 {
        Type::FLOAT8_ARRAY
    } else if *scalar == Type::BOOL {
        Type::BOOL_ARRAY
    } else if *scalar == Type::TEXT || *scalar == Type::VARCHAR {
        Type::TEXT_ARRAY
    } else if *scalar == Type::UUID {
        Type::UUID_ARRAY
    } else if *scalar == Type::JSON {
        Type::JSON_ARRAY
    } else if *scalar == Type::JSONB {
        Type::JSONB_ARRAY
    } else if *scalar == Type::DATE {
        Type::DATE_ARRAY
    } else if *scalar == Type::TIME {
        Type::TIME_ARRAY
    } else if *scalar == Type::TIMESTAMP {
        Type::TIMESTAMP_ARRAY
    } else if *scalar == Type::TIMESTAMPTZ {
        Type::TIMESTAMPTZ_ARRAY
    } else if *scalar == Type::INET {
        Type::INET_ARRAY
    } else if *scalar == Type::CIDR {
        Type::CIDR_ARRAY
    } else if *scalar == Type::MACADDR {
        Type::MACADDR_ARRAY
    } else if *scalar == Type::MACADDR8 {
        Type::MACADDR8_ARRAY
    } else if *scalar == Type::NUMERIC {
        Type::NUMERIC_ARRAY
    } else if *scalar == Type::BYTEA {
        Type::BYTEA_ARRAY
    } else if *scalar == Type::FLOAT4 {
        Type::FLOAT4_ARRAY
    } else {
        Type::TEXT_ARRAY
    }
}

/// Execute a query that returns rows and build a Query response.
///
/// With `infer`, a column the statement cannot type (see [`static_pg_type`]) is typed from its
/// values ([`infer_pg_type`]), so the rows are kept as values until every row is read; a statement
/// whose columns are all typed, or a reply without `infer`, encodes each row as it comes (an
/// untyped column as text, as Describe reported it).
fn execute_query(
    stmt: &mut turso_core::Statement,
    format: &Format,
    infer: bool,
    schema: &turso_core::schema::Schema,
) -> PgWireResult<Response> {
    let mut statics: Vec<Option<Type>> = (0..stmt.num_columns())
        .map(|i| static_pg_type(stmt, i))
        .collect();
    if !infer {
        for t in &mut statics {
            t.get_or_insert(Type::TEXT);
        }
    }
    let pads: Vec<Option<usize>> = (0..stmt.num_columns())
        .map(|i| bpchar_width(stmt, schema, i))
        .collect();
    if statics.iter().all(Option::is_some) {
        let header = Arc::new(field_info(stmt, format, |i| {
            statics[i].clone().expect("all typed")
        }));
        let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(encode_row(&header, &pads, row.get_values()));
            Ok(())
        })
        .map_err(engine_error)?;
        return Ok(Response::Query(QueryResponse::new(
            header,
            stream::iter(rows),
        )));
    }
    let mut values: Vec<Vec<Value>> = Vec::new();
    stmt.run_with_row_callback(|row| {
        values.push(row.get_values().cloned().collect());
        Ok(())
    })
    .map_err(engine_error)?;
    let header = Arc::new(field_info(stmt, format, |i| {
        statics[i]
            .clone()
            .unwrap_or_else(|| infer_pg_type(&values, i))
    }));
    let rows: Vec<PgWireResult<DataRow>> = values
        .iter()
        .map(|row| encode_row(&header, &pads, row.iter()))
        .collect();
    Ok(Response::Query(QueryResponse::new(
        header,
        stream::iter(rows),
    )))
}

/// The declared length n of a result column that is a `character(n)` table column: the engine
/// stores its values without their trailing blanks, and PostgreSQL returns them padded to n.
/// The statement says the column is a bpchar but not its length, so the length is read from the
/// table's schema, by the result column's name; a column renamed by an alias takes the table's
/// one bpchar length if all its bpchar columns share it, and is otherwise returned unpadded
/// (COMPAT.md).
fn bpchar_width(
    stmt: &turso_core::Statement,
    schema: &turso_core::schema::Schema,
    idx: usize,
) -> Option<usize> {
    let info = stmt.get_column_type_info(idx).ok().flatten()?;
    if !info.declared_name.trim().eq_ignore_ascii_case("bpchar") {
        return None;
    }
    let table = schema.get_btree_table(&stmt.get_column_table_name(idx)?)?;
    let width = |col: &turso_core::schema::Column| -> Option<usize> {
        match col.ty_params.first().map(|p| p.as_ref()) {
            Some(turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Numeric(n))) => {
                n.parse().ok()
            }
            _ => None,
        }
    };
    if let Some((_, col)) = table.get_column(&stmt.get_column_name(idx)) {
        if col.ty_str.eq_ignore_ascii_case("bpchar") {
            return width(col);
        }
    }
    let mut widths = table
        .columns()
        .iter()
        .filter(|c| c.ty_str.eq_ignore_ascii_case("bpchar"))
        .map(width);
    let first = widths.next()??;
    widths.all(|w| w == Some(first)).then_some(first)
}

fn encode_row<'a>(
    header: &Arc<Vec<FieldInfo>>,
    pads: &[Option<usize>],
    values: impl Iterator<Item = &'a Value>,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(header.clone());
    for (i, val) in values.enumerate() {
        let pg_type = header
            .get(i)
            .map(|fi| fi.datatype().clone())
            .unwrap_or(Type::TEXT);
        if let (Some(Some(width)), Value::Text(t)) = (pads.get(i), val) {
            let chars = t.as_str().chars().count();
            if chars < *width {
                let padded = format!("{}{}", t.as_str(), " ".repeat(width - chars));
                encode_value(&mut encoder, &Value::build_text(padded), &pg_type)
                    .map_err(engine_error)?;
                continue;
            }
        }
        encode_value(&mut encoder, val, &pg_type).map_err(engine_error)?;
    }
    encoder.finish()
}

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(stmt: &mut turso_core::Statement, query: &str) -> PgWireResult<Response> {
    stmt.run_ignore_rows().map_err(engine_error)?;

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
}

/// Extract parameters from a Portal and bind them to a prepared statement.
///
/// PostgreSQL parameters ($1, $2, ...) map to portal parameters 0, 1, ...
/// The bytecode compiler may allocate internal parameter indices in a different
/// order than the $N numbering (e.g. if $2 appears before $1 in the SQL), so we
/// look up each parameter's internal index by name.
fn bind_portal_parameters(
    stmt: &mut turso_core::Statement,
    portal: &Portal<String>,
) -> PgWireResult<()> {
    for i in 0..portal.parameter_len() {
        let value = match &portal.parameters[i] {
            None => Value::Null,
            Some(bytes) => {
                let pg_type = portal
                    .statement
                    .parameter_types
                    .get(i)
                    .and_then(|t| t.as_ref())
                    .unwrap_or(&Type::UNKNOWN);
                pg_bytes_to_value(bytes, pg_type)?
            }
        };
        // Portal parameter i corresponds to PostgreSQL $N where N = i + 1.
        // Look up the internal index that the bytecode compiler assigned to $N.
        let pg_param_name = format!("${}", i + 1);
        let idx = stmt
            .parameter_index(&pg_param_name)
            .unwrap_or_else(|| NonZero::new(i + 1).expect("parameter index must be non-zero"));
        // Ignore bind errors: parameter index mismatches or value coercion
        // failures surface as wire-protocol errors during the subsequent
        // execute, with a more useful message than a generic Bind failure.
        let _ = stmt.bind_at(idx, value);
    }
    Ok(())
}

/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;

    match *pg_type {
        Type::INT2 | Type::INT4 | Type::INT8 => {
            let i: i64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid integer parameter: {e}"
                ))))
            })?;
            Ok(Value::from_i64(i))
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let f: f64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid float parameter: {e}"
                ))))
            })?;
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match text {
            "t" | "true" | "TRUE" | "1" | "yes" | "on" => Ok(Value::from_i64(1)),
            "f" | "false" | "FALSE" | "0" | "no" | "off" => Ok(Value::from_i64(0)),
            _ => Err(PgWireError::UserError(Box::new(error_info(&format!(
                "invalid boolean parameter: {text}"
            ))))),
        },
        Type::BYTEA => {
            // PostgreSQL text format for bytea uses \x hex encoding
            if let Some(hex_str) = text.strip_prefix("\\x") {
                let data = decode_hex(hex_str).map_err(|e| {
                    PgWireError::UserError(Box::new(error_info(&format!(
                        "invalid bytea hex parameter: {e}"
                    ))))
                })?;
                Ok(Value::from_blob(data))
            } else {
                // Raw bytes as-is
                Ok(Value::from_blob(bytes.to_vec()))
            }
        }
        // UNKNOWN: try to infer type from text content (numeric-looking values
        // should be bound as numbers so comparisons with COUNT/SUM etc. work)
        Type::UNKNOWN => {
            if let Ok(i) = text.parse::<i64>() {
                Ok(Value::from_i64(i))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(Value::from_f64(f))
            } else if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("t") {
                Ok(Value::from_i64(1))
            } else if text.eq_ignore_ascii_case("false") || text.eq_ignore_ascii_case("f") {
                Ok(Value::from_i64(0))
            } else {
                Ok(Value::from_text(text.to_owned()))
            }
        }
        // TEXT, VARCHAR, and all other types → text
        _ => Ok(Value::from_text(text.to_owned())),
    }
}

/// Decode a hex string into bytes.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("odd-length hex string".to_owned());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| format!("invalid hex at position {i}: {e}"))
        })
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
) -> turso_core::Result<()> {
    match val {
        Value::Null => encoder
            .encode_field(&None::<i8>)
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Numeric(turso_core::Numeric::Integer(i)) => {
            // Boolean columns: encode as true/false instead of 0/1
            if *pg_type == Type::BOOL {
                encoder
                    .encode_field(&(*i != 0))
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(i)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Numeric(turso_core::Numeric::Float(f)) => encoder
            .encode_field(&f64::from(*f))
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Text(t) => {
            let text = t.value.as_ref();
            // For TIMESTAMPTZ columns, ensure timezone info is present so clients
            // parse the value correctly (as UTC, not local time).
            // TIMESTAMP (without TZ) should NOT have timezone suffix.
            if *pg_type == Type::TIMESTAMPTZ
                && !text.contains('+')
                && !text.contains('Z')
                && !text.ends_with("-00")
            {
                let with_tz = format!("{text}+00");
                encoder
                    .encode_field(&with_tz.as_str())
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else if pg_type.name().starts_with('_') {
                // Array types: pgwire's to_sql_text quotes strings containing
                // {, }, or commas when the type is Kind::Array. Since we store
                // array values as pre-formatted PG array literals (e.g.
                // "{1,2,3}"), encode with Type::TEXT to bypass the quoting.
                encoder
                    .encode_field_with_type_and_format(
                        &text,
                        &Type::TEXT,
                        FieldFormat::Text,
                        &FormatOptions::default(),
                    )
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(&text)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Blob(b) => encoder
            .encode_field(&b.as_slice())
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
    }
}

fn sqlite_type_to_pg_type(type_str: &str) -> Type {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2" | "SERIAL" | "SMALLSERIAL" => Type::INT4,
        "BIGINT" | "INT8" | "BIGSERIAL" => Type::INT8,
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC"
        | "DECIMAL" => Type::FLOAT8,
        "VARCHAR" | "CHARACTER VARYING" => Type::VARCHAR,
        "TEXT" | "CHAR" | "CHARACTER" | "NAME" => Type::TEXT,
        "BLOB" | "BYTEA" => Type::BYTEA,
        "BOOLEAN" | "BOOL" => Type::BOOL,
        "UUID" => Type::UUID,
        "JSON" => Type::JSON,
        "JSONB" => Type::JSONB,
        "DATE" => Type::DATE,
        "TIME" | "TIMETZ" => Type::TIME,
        "TIMESTAMP" => Type::TIMESTAMP,
        "TIMESTAMPTZ" => Type::TIMESTAMPTZ,
        "INET" => Type::INET,
        "CIDR" => Type::CIDR,
        "MACADDR" => Type::MACADDR,
        "MACADDR8" => Type::MACADDR8,
        _ => {
            // Handle parameterized types like varchar(50), numeric(10,2)
            if upper.starts_with("BPCHAR") {
                Type::BPCHAR
            } else if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                Type::VARCHAR
            } else if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
                Type::NUMERIC
            } else {
                Type::TEXT
            }
        }
    }
}

/// PG statements handled by `try_prepare_pg()` that return a dummy SELECT
/// but should produce a command-tag response, not a result set.
fn is_pg_non_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("COPY")
        || upper.starts_with("CREATE SCHEMA")
        || upper.starts_with("DROP SCHEMA")
        || upper.starts_with("REFRESH MATERIALIZED VIEW")
        || upper.starts_with("COMMENT")
        || upper.starts_with("CHECKPOINT")
        || upper.starts_with("ALTER")
        || upper.starts_with("SET ")
        || upper.starts_with("RESET ")
}

fn command_tag(query: &str, affected_rows: usize) -> Tag {
    let upper = query.trim().to_uppercase();
    if upper.starts_with("INSERT") {
        Tag::new("INSERT").with_oid(0).with_rows(affected_rows)
    } else if upper.starts_with("UPDATE") {
        Tag::new("UPDATE").with_rows(affected_rows)
    } else if upper.starts_with("DELETE") {
        Tag::new("DELETE").with_rows(affected_rows)
    } else if upper.starts_with("TRUNCATE") {
        Tag::new("TRUNCATE TABLE")
    } else if upper.starts_with("CHECKPOINT") {
        Tag::new("CHECKPOINT")
    } else if upper.starts_with("RESET") {
        Tag::new("RESET")
    } else if is_create_table_as(&upper) {
        // PostgreSQL reports CREATE TABLE AS completion as `SELECT n` (the
        // rows inserted), except WITH NO DATA which skips the insert and
        // keeps the plain tag.
        if ends_with_with_no_data(&upper) {
            Tag::new("CREATE TABLE AS")
        } else {
            Tag::new("SELECT").with_rows(affected_rows)
        }
    } else if let Some(tag) = ddl_tag(&upper) {
        tag
    } else if upper.starts_with("CREATE") {
        Tag::new("CREATE TABLE")
    } else if upper.starts_with("DROP") {
        Tag::new("DROP TABLE")
    } else if upper.starts_with("ALTER") {
        Tag::new("ALTER TABLE")
    } else if upper.starts_with("BEGIN") || upper.starts_with("START") {
        Tag::new("BEGIN")
    } else if upper.starts_with("COMMIT") || upper.starts_with("END") {
        Tag::new("COMMIT")
    } else if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
        Tag::new("ROLLBACK")
    } else if upper.starts_with("SAVEPOINT") {
        Tag::new("SAVEPOINT")
    } else if upper.starts_with("RELEASE") {
        Tag::new("RELEASE")
    } else if upper.starts_with("SET") {
        Tag::new("SET")
    } else if upper.starts_with("COPY") {
        Tag::new("COPY").with_rows(affected_rows)
    } else if upper.starts_with("COMMENT") {
        Tag::new("COMMENT")
    } else if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        // Row-returning SELECTs never reach command_tag (they take the
        // query-response path), so a zero-column SELECT- or WITH-prefixed
        // statement is SELECT ... INTO (writable CTEs are unsupported),
        // which PostgreSQL reports as `SELECT n` like CREATE TABLE AS.
        Tag::new("SELECT").with_rows(affected_rows)
    } else {
        Tag::new("OK")
    }
}

/// `CREATE|DROP|ALTER [modifiers] <object> ...`'s tag, as PostgreSQL reports it: the verb and the
/// object, whatever modifiers come between (CREATE UNIQUE INDEX completes as CREATE INDEX, CREATE
/// OR REPLACE VIEW as CREATE VIEW). `None` for an object not listed here.
fn ddl_tag(upper: &str) -> Option<Tag> {
    let mut words = upper
        .split(|c: char| c.is_ascii_whitespace() || c == '(' || c == ';')
        .filter(|w| !w.is_empty());
    let verb = words.next()?;
    if !matches!(verb, "CREATE" | "DROP" | "ALTER") {
        return None;
    }
    for word in words {
        match word {
            "OR" | "REPLACE" | "UNIQUE" | "TEMP" | "TEMPORARY" | "UNLOGGED" | "GLOBAL"
            | "LOCAL" | "RECURSIVE" => continue,
            "TABLE" | "INDEX" | "VIEW" | "SEQUENCE" | "SCHEMA" | "TYPE" | "DOMAIN" | "TRIGGER" => {
                return Some(Tag::new(&format!("{verb} {word}")));
            }
            _ => return None,
        }
    }
    None
}

/// Whether the statement ends with `WITH NO DATA`, token-wise (ignoring
/// trailing whitespace and statement terminators).
fn ends_with_with_no_data(upper: &str) -> bool {
    let mut tokens = upper
        .trim_end()
        .trim_end_matches(';')
        .split_whitespace()
        .rev();
    tokens.next() == Some("DATA") && tokens.next() == Some("NO") && tokens.next() == Some("WITH")
}

/// Best-effort detection of `CREATE [TEMP|UNLOGGED] TABLE [IF NOT EXISTS]
/// <name> AS ...` from the statement text, in the same spirit as the prefix
/// matching in `command_tag`. Quoted table names containing whitespace are
/// not recognized and fall back to the plain CREATE TABLE tag.
fn is_create_table_as(upper: &str) -> bool {
    let mut tokens = upper.split_whitespace();
    if tokens.next() != Some("CREATE") {
        return false;
    }
    let mut tok = tokens.next();
    while matches!(
        tok,
        Some("TEMP" | "TEMPORARY" | "UNLOGGED" | "GLOBAL" | "LOCAL")
    ) {
        tok = tokens.next();
    }
    if tok != Some("TABLE") {
        return false;
    }
    tok = tokens.next();
    if tok == Some("IF") {
        if tokens.next() != Some("NOT") || tokens.next() != Some("EXISTS") {
            return false;
        }
        tok = tokens.next();
    }
    // `tok` is the table name; AS must follow it (possibly fused with an
    // opening parenthesis, as in `AS(SELECT 1)`).
    let Some(name) = tok else {
        return false;
    };
    // A quoted name that opens without closing in the same token spans
    // whitespace, so the next token is part of the name, not AS.
    if name.starts_with('"') && (name.len() == 1 || !name.ends_with('"')) {
        return false;
    }
    matches!(tokens.next(), Some(t) if t == "AS" || t.starts_with("AS("))
}

fn error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session on a fresh database of its own (catalog branch store, D0: no flush slows the test).
    fn session(dir: &tempfile::TempDir) -> Session {
        let path = dir.path().join("w.db").to_string_lossy().into_owned();
        let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
            sync: turso_core::branch::SyncClass::Off,
        });
        let (_io, db) =
            turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts).unwrap();
        Session::new(Arc::new(Shared {
            db,
            db_file: path,
            max_connections: 1,
            lock_wait: std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
            live: AtomicUsize::new(0),
            in_use: Mutex::new(std::collections::HashMap::new()),
        }))
    }

    fn ok(session: &Session, sql: &str) {
        for r in session.simple(sql) {
            if let Response::Error(e) = r {
                panic!("{sql}: {} {}", e.code, e.message);
            }
        }
    }

    /// PREREG/DECISIONS L5: a branch call goes from the wire to the engine's named-branch API with no
    /// statement parse — no libpg_query call at all (split, parse or scan) — in every form a client
    /// sends: any case, spacing, a trailing semicolon, a cast on the name.
    #[test]
    fn a_branch_call_makes_no_libpg_query_call() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY)");
        ok(&s, "INSERT INTO t VALUES (1)");
        for sql in [
            "SELECT turso_branch_create('b1')",
            "select turso_branch_switch('b1');",
            "SELECT turso_branch_current()",
            "  SELECT  TURSO_BRANCH_SWITCH ( 'main' ) ;  ",
            "SELECT turso_branch_create('it''s'::text)",
            "SELECT turso_branch_delete(\n'b1'\n)",
            "SELECT turso_branch_delete('it''s')",
        ] {
            let before = turso_pg_parser::libpg_query_calls();
            ok(&s, sql);
            assert_eq!(
                turso_pg_parser::libpg_query_calls() - before,
                0,
                "libpg_query calls for {sql:?}"
            );
        }
    }

    /// An ordinary statement is parsed by libpg_query exactly once on its way to the engine: the
    /// server splits a query without a statement separator without asking libpg_query, and the
    /// frontend reads the special forms (SET, SHOW, CHECKPOINT, COPY ...) from the same parse it
    /// translates. Three calls before (split, the special-form check's parse, the translation's).
    #[test]
    fn an_ordinary_statement_is_parsed_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        ok(&s, "INSERT INTO t VALUES (1, 0)");
        for sql in [
            "SELECT 1",
            "UPDATE t SET v = v + 1 WHERE id = 1",
            "UPDATE t SET v = v + 1 WHERE id = 1;",
            "SELECT v FROM t WHERE id = 1",
            "INSERT INTO t VALUES (2, 0)",
        ] {
            let before = turso_pg_parser::libpg_query_calls();
            ok(&s, sql);
            assert_eq!(
                turso_pg_parser::libpg_query_calls() - before,
                1,
                "libpg_query calls for {sql:?}"
            );
        }
        // A query that holds a separator is still split by libpg_query, which alone knows where a
        // literal, a comment or a dollar quote ends.
        let before = turso_pg_parser::libpg_query_calls();
        ok(&s, "SELECT 'a;b'; SELECT 2");
        assert_eq!(
            turso_pg_parser::libpg_query_calls() - before,
            3,
            "split + one parse each"
        );
    }

    /// The instrument above counts: an ordinary statement does call libpg_query.
    #[test]
    fn an_ordinary_statement_calls_libpg_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        let before = turso_pg_parser::libpg_query_calls();
        ok(&s, "SELECT 1");
        assert!(turso_pg_parser::libpg_query_calls() > before);
    }

    #[test]
    fn test_pg_bytes_to_value_integer() {
        let val = pg_bytes_to_value(b"42", &Type::INT4).unwrap();
        assert_eq!(val, Value::from_i64(42));

        let val = pg_bytes_to_value(b"-100", &Type::INT8).unwrap();
        assert_eq!(val, Value::from_i64(-100));

        let val = pg_bytes_to_value(b"0", &Type::INT2).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_float() {
        let val = pg_bytes_to_value(b"3.25", &Type::FLOAT8).unwrap();
        assert_eq!(val, Value::from_f64(3.25));

        let val = pg_bytes_to_value(b"-0.5", &Type::FLOAT4).unwrap();
        assert_eq!(val, Value::from_f64(-0.5));

        let val = pg_bytes_to_value(b"1.23", &Type::NUMERIC).unwrap();
        assert_eq!(val, Value::from_f64(1.23));
    }

    #[test]
    fn test_pg_bytes_to_value_bool() {
        let val = pg_bytes_to_value(b"t", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"f", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(0));

        let val = pg_bytes_to_value(b"true", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"false", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_text() {
        let val = pg_bytes_to_value(b"hello world", &Type::TEXT).unwrap();
        assert_eq!(val, Value::from_text("hello world".to_owned()));

        let val = pg_bytes_to_value(b"Alice", &Type::VARCHAR).unwrap();
        assert_eq!(val, Value::from_text("Alice".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_bytea() {
        let val = pg_bytes_to_value(b"\\xDEADBEEF", &Type::BYTEA).unwrap();
        assert_eq!(val, Value::from_blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn test_pg_bytes_to_value_unknown_type_as_text() {
        // Unknown types should be treated as text
        let val = pg_bytes_to_value(b"some-uuid-value", &Type::UUID).unwrap();
        assert_eq!(val, Value::from_text("some-uuid-value".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_integer_parse_error() {
        let result = pg_bytes_to_value(b"not_a_number", &Type::INT4);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_float_parse_error() {
        let result = pg_bytes_to_value(b"not_a_float", &Type::FLOAT8);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_bool_invalid() {
        let result = pg_bytes_to_value(b"maybe", &Type::BOOL);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_hex() {
        assert_eq!(
            decode_hex("DEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(decode_hex("00ff").unwrap(), vec![0x00, 0xFF]);
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
        assert!(decode_hex("0").is_err()); // odd length
        assert!(decode_hex("GG").is_err()); // invalid hex
    }

    #[test]
    fn test_sqlite_type_to_pg_type() {
        assert_eq!(sqlite_type_to_pg_type("INTEGER"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT4"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("BIGINT"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("INT8"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("REAL"), Type::FLOAT8);
        assert_eq!(sqlite_type_to_pg_type("TEXT"), Type::TEXT);
        assert_eq!(sqlite_type_to_pg_type("BLOB"), Type::BYTEA);
        assert_eq!(sqlite_type_to_pg_type("BOOLEAN"), Type::BOOL);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMP"), Type::TIMESTAMP);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMPTZ"), Type::TIMESTAMPTZ);
        assert_eq!(sqlite_type_to_pg_type("DATE"), Type::DATE);
        assert_eq!(sqlite_type_to_pg_type("JSON"), Type::JSON);
        assert_eq!(sqlite_type_to_pg_type("JSONB"), Type::JSONB);
        assert_eq!(sqlite_type_to_pg_type("UUID"), Type::UUID);
        // Unknown types map to TEXT
        assert_eq!(sqlite_type_to_pg_type("UNKNOWN"), Type::TEXT);
    }

    #[test]
    fn test_unknown_type_inference() {
        // UNKNOWN type should infer integers from numeric-looking strings
        let val = pg_bytes_to_value(b"42", &Type::UNKNOWN).unwrap();
        assert!(matches!(
            val,
            Value::Numeric(turso_core::Numeric::Integer(42))
        ));

        // UNKNOWN type should infer floats
        let val = pg_bytes_to_value(b"3.14", &Type::UNKNOWN).unwrap();
        if let Value::Numeric(turso_core::Numeric::Float(f)) = val {
            #[allow(clippy::approx_constant)]
            let expected = 3.14;
            assert!((f64::from(f) - expected).abs() < 0.001);
        } else {
            panic!("Expected Float");
        }

        // UNKNOWN type should keep text for non-numeric strings
        let val = pg_bytes_to_value(b"hello", &Type::UNKNOWN).unwrap();
        assert!(matches!(val, Value::Text(_)));
    }

    #[test]
    fn test_is_create_table_as() {
        assert!(is_create_table_as("CREATE TABLE T AS SELECT 1"));
        assert!(is_create_table_as("CREATE TEMP TABLE T AS SELECT 1"));
        assert!(is_create_table_as("CREATE UNLOGGED TABLE T AS SELECT 1"));
        assert!(is_create_table_as(
            "CREATE TABLE IF NOT EXISTS T AS SELECT 1"
        ));
        assert!(is_create_table_as("CREATE TABLE S.T AS SELECT 1"));
        assert!(is_create_table_as("CREATE TABLE T AS(SELECT 1)"));
        assert!(is_create_table_as("CREATE TABLE \"T\" AS SELECT 1"));

        assert!(!is_create_table_as("CREATE TABLE T (X INT)"));
        assert!(!is_create_table_as("CREATE INDEX I ON T (X)"));
        assert!(!is_create_table_as("CREATE VIEW V AS SELECT 1"));
        // Quoted name containing whitespace: `AS` is part of the name.
        assert!(!is_create_table_as("CREATE TABLE \"A AS B\" (X INT)"));
    }

    #[test]
    fn test_ends_with_with_no_data() {
        assert!(ends_with_with_no_data(
            "CREATE TABLE T AS SELECT 1 WITH NO DATA"
        ));
        assert!(ends_with_with_no_data(
            "CREATE TABLE T AS SELECT 1 WITH  NO\nDATA ; "
        ));

        assert!(!ends_with_with_no_data("CREATE TABLE T AS SELECT 1"));
        assert!(!ends_with_with_no_data("SELECT 'WITH NO DATA'"));
    }
}
