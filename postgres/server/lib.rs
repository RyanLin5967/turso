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

use turso_parser::sql_error::SqlErrorKind;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard, OnceLock,
};

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::{self, StreamExt};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use turso_core::{CheckpointMode, Database, LimboError, Value};
use turso_pg::{
    attach_schema_files, branch_call, element_of, pg_bool, single_statement, split_statements,
    PgBranchArg,
    PgBranchCall, PgConnection, StatementTypes, TxStmt,
};
use turso_pg_parser::{pg_space, scan_params, skip_blank, sql_comment, ParamScan};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::portal::{Format, Portal, PortalExecutionState};
use pgwire::api::query::{send_ready_for_query, ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ErrorHandler, NoopHandler, PgWireConnectionState,
    PgWireServerHandlers, Type, DEFAULT_NAME, METADATA_DATABASE,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::{DataRow, NoData, ParameterDescription, RowDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Parse, ParseComplete,
    PortalSuspended, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{
    EmptyQueryResponse, NoticeResponse, ReadyForQuery, TransactionStatus,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::ParameterStatus;
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
    /// Every session thread started and not yet joined; the finished ones are reaped at each
    /// admission, and the shutdown joins the rest.
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

/// How long the shutdown waits for the session threads it ended to finish.
const SHUTDOWN_JOIN: std::time::Duration = std::time::Duration::from_secs(30);

/// One statement's waits on a lock another session holds: SQLite's default busy schedule (1, 2,
/// 5, 10, 15, 20, 25, 25, 25, 50, 50 ms, then 100 ms) up to the server's lock wait. The clock is
/// first read at the first wait, so a statement that meets no lock reads it not at all (wire
/// review 5 item 20: every create read it). A wait is the session's: its cancel or
/// statement_timeout ends it, which the engine's progress handler cannot see while no statement
/// steps (ultrareview of fastest-wire, finding 9).
struct Backoff<'a> {
    lock_wait: std::time::Duration,
    deadline: Option<std::time::Instant>,
    attempt: usize,
    interrupts: Option<&'a Interrupts>,
}

impl<'a> Backoff<'a> {
    fn new(lock_wait: std::time::Duration, interrupts: &'a Interrupts) -> Self {
        Self {
            lock_wait,
            deadline: None,
            attempt: 0,
            interrupts: Some(interrupts),
        }
    }

    /// A Backoff that never waits: its statement is never stepped again or run anew.
    fn never() -> Self {
        Self {
            lock_wait: std::time::Duration::ZERO,
            deadline: None,
            attempt: 0,
            interrupts: None,
        }
    }

    /// Sleep the schedule's next delay and say so, or say false, without sleeping, when the delay
    /// would end past the lock wait: then the next attempt is the last. Err(Interrupt) when the
    /// session's cancel or statement_timeout stops the statement, before or after the sleep.
    fn wait(&mut self) -> turso_core::Result<bool> {
        const DELAYS_MS: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
        self.stop_if_interrupted()?;
        let now = std::time::Instant::now();
        let deadline = *self.deadline.get_or_insert(now + self.lock_wait);
        let delay =
            std::time::Duration::from_millis(DELAYS_MS[self.attempt.min(DELAYS_MS.len() - 1)]);
        self.attempt += 1;
        if now + delay > deadline {
            return Ok(false);
        }
        std::thread::sleep(delay);
        self.stop_if_interrupted()?;
        Ok(true)
    }

    /// [`Backoff::wait`] that no cancel or statement_timeout ends, for a statement that changed
    /// rows (see [`run_waiting`]).
    fn wait_uninterrupted(&mut self) -> bool {
        let interrupts = self.interrupts.take();
        let waited = self.wait();
        self.interrupts = interrupts;
        waited.expect("a wait with no interrupts never stops")
    }

    fn stop_if_interrupted(&self) -> turso_core::Result<()> {
        match self.interrupts {
            Some(interrupts) if interrupts.should_stop() => Err(LimboError::Interrupt),
            _ => Ok(()),
        }
    }
}

/// Run `stmt` to its end, `row` called with each row it returns. A Busy after the statement changed
/// rows (its commit refused part-way) is waited out on `backoff` by stepping the SAME statement
/// again: the engine resumes it where it stopped (core fastest_tests.rs,
/// a_trunk_commit_retried_after_a_busy_decision_pass_retains_every_page). Dropped there, a write
/// whose RETURNING rows went out is committed by the engine's reset, and prepared anew it ran
/// again in full: applied twice, its rows returned twice (wire review 5 item 2). A Busy before it
/// changed anything is the statement's, for [`Session::engine_statement`] to run it anew, as
/// before 64ba2c66b: that the engine resumes a statement refused at its first lock is not
/// established (the commit's contract test is the only resume shown). Past the lock wait the Busy
/// is the statement's.
fn run_waiting(
    stmt: &mut turso_core::Statement,
    backoff: &mut Backoff<'_>,
    mut row: impl FnMut(&turso_core::Row) -> turso_core::Result<()>,
) -> turso_core::Result<()> {
    loop {
        match stmt.run_with_row_callback(&mut row) {
            // Not stopped by a cancel or statement_timeout: the statement changed rows, and
            // dropped here the engine's reset commits them, so a 57014 would report a write that
            // was committed (fresh-context review of the ultrareview fixes).
            Err(LimboError::Busy) if stmt.n_change() > 0 => {
                if !backoff.wait_uninterrupted() {
                    return Err(LimboError::Busy);
                }
            }
            r => return r,
        }
    }
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
    in_use: Mutex<std::collections::HashMap<compact_str::CompactString, BranchUse>>,
    /// Signalled when an entry leaves `in_use`, for a delete waiting on a session that is closing.
    in_use_freed: std::sync::Condvar,
    /// Threads waiting on `in_use_freed`: a release signals only when there is one, so an
    /// uncontended switch or delete wakes nobody.
    in_use_waiters: AtomicUsize,
    /// How long a claim or a delete waits on `in_use`: IN_USE_WAIT (a field so a unit test can
    /// shorten it).
    in_use_wait: std::time::Duration,
    /// Each live session's interrupt flags by its BackendKeyData (process id, secret key), for a
    /// CancelRequest on another connection to stop its statement.
    cancels: Mutex<std::collections::HashMap<(i32, i32), std::sync::Weak<Interrupts>>>,
    /// The next session's process id.
    next_pid: std::sync::atomic::AtomicI32,
    /// How long a client has to complete its startup (TursoPgServer::with_startup_timeout).
    startup_timeout: std::time::Duration,
    /// Every live session's socket, for the shutdown that ends them.
    sessions: SessionSockets,
}

/// A handle on each live session's socket (a dup of its descriptor), so the server's shutdown can
/// shut them: a session thread blocked reading its socket then reads the stream's end and ends.
#[derive(Default)]
struct SessionSockets {
    next: AtomicU64,
    open: Mutex<std::collections::HashMap<u64, std::net::TcpStream>>,
}

impl SessionSockets {
    fn register(&self, socket: std::net::TcpStream) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, socket);
        id
    }

    fn deregister(&self, id: u64) {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// Shut every live session's socket both ways; returns how many.
    fn shut_all(&self) -> usize {
        let open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        for socket in open.values() {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
        open.len()
    }
}

/// A session's requests to stop its running statement: a CancelRequest, and SET statement_timeout's
/// deadline. The engine reads them through each of the session's connections' progress handler
/// (every PROGRESS_OPS virtual-machine steps), and stops the statement with an interrupt (57014).
/// A cancel that arrives between statements is dropped, as PostgreSQL drops it.
struct Interrupts {
    cancel: std::sync::atomic::AtomicBool,
    /// The running statement's deadline, in microseconds since `base`; 0 none.
    deadline_us: AtomicU64,
    base: std::time::Instant,
}

/// Virtual-machine steps between two reads of a session's [`Interrupts`].
const PROGRESS_OPS: u64 = 1000;

thread_local! {
    /// Why this thread's last statement was interrupted: 1 a cancel, 2 statement_timeout (read by
    /// `engine_info` for PostgreSQL's message).
    static INTERRUPT_CAUSE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

impl Interrupts {
    fn new() -> Self {
        Self {
            cancel: std::sync::atomic::AtomicBool::new(false),
            deadline_us: AtomicU64::new(0),
            base: std::time::Instant::now(),
        }
    }

    /// A statement starts: earlier cancels are dropped, and its deadline set (0 ms none).
    fn begin(&self, timeout_ms: u64) -> InterruptsArmed<'_> {
        self.cancel.store(false, Ordering::Relaxed);
        let deadline = match timeout_ms {
            0 => 0,
            ms => (self.base.elapsed().as_micros() as u64)
                .saturating_add(ms.saturating_mul(1000))
                .max(1),
        };
        self.deadline_us.store(deadline, Ordering::Relaxed);
        INTERRUPT_CAUSE.with(|c| c.set(0));
        InterruptsArmed(self)
    }

    /// The progress handler's question: stop the running statement?
    fn should_stop(&self) -> bool {
        if self.cancel.load(Ordering::Relaxed) {
            INTERRUPT_CAUSE.with(|c| c.set(1));
            return true;
        }
        let deadline = self.deadline_us.load(Ordering::Relaxed);
        if deadline != 0 && self.base.elapsed().as_micros() as u64 >= deadline {
            INTERRUPT_CAUSE.with(|c| c.set(2));
            return true;
        }
        false
    }
}

/// The statement's span: its end drops a cancel that came too late and clears the deadline.
struct InterruptsArmed<'a>(&'a Interrupts);

impl Drop for InterruptsArmed<'_> {
    fn drop(&mut self) {
        self.0.deadline_us.store(0, Ordering::Relaxed);
        self.0.cancel.store(false, Ordering::Relaxed);
    }
}

/// pgwire's CancelRequest handler: the named session's statement is asked to stop.
struct CancelSessions(Arc<Shared>);

#[async_trait]
impl pgwire::api::cancel::CancelHandler for CancelSessions {
    async fn on_cancel_request(&self, request: pgwire::messages::cancel::CancelRequest) {
        let Some(secret) = request.secret_key.as_i32() else {
            return;
        };
        let target = self
            .0
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(request.pid, secret))
            .and_then(std::sync::Weak::upgrade);
        if let Some(interrupts) = target {
            interrupts.cancel.store(true, Ordering::Relaxed);
        }
    }
}

/// How long a delete waits for the session on its branch to go, and a switch for a delete of its
/// branch to end: PostgreSQL's DROP DATABASE waits 5 s for exiting backends (CountOtherDBBackends,
/// 50 x 100 ms).
const IN_USE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// How often a statement's in-use wait looks at its cancel and statement_timeout.
const IN_USE_SLICE: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchUse {
    Held,
    Deleting,
}

impl Shared {
    fn new(
        db: Arc<Database>,
        db_file: String,
        max_connections: usize,
        lock_wait: std::time::Duration,
    ) -> Self {
        Self {
            db,
            db_file,
            max_connections,
            lock_wait,
            live: AtomicUsize::new(0),
            in_use: Mutex::new(std::collections::HashMap::new()),
            in_use_freed: std::sync::Condvar::new(),
            in_use_waiters: AtomicUsize::new(0),
            in_use_wait: IN_USE_WAIT,
            cancels: Mutex::new(std::collections::HashMap::new()),
            next_pid: std::sync::atomic::AtomicI32::new(1),
            startup_timeout: std::time::Duration::from_secs(DEFAULT_AUTHENTICATION_TIMEOUT_S),
            sessions: SessionSockets::default(),
        }
    }

    fn uses(&self) -> MutexGuard<'_, std::collections::HashMap<compact_str::CompactString, BranchUse>> {
        self.in_use.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The map once `name` is in none of `states` there, or at the wait's end: what a closing session
    /// or a running delete leaves behind is waited out, not refused (wire review 3 item 4). The wait
    /// is its session's statement's: with `interrupts`, a cancel or statement_timeout ends it with
    /// 57014, checked every IN_USE_SLICE (Claude C37 M1: both were ignored for the whole wait, the
    /// class of ultrareview finding 9).
    #[allow(clippy::type_complexity)]
    fn wait_while<'a>(
        &'a self,
        mut uses: MutexGuard<'a, std::collections::HashMap<compact_str::CompactString, BranchUse>>,
        name: &str,
        states: &[BranchUse],
        interrupts: Option<&Interrupts>,
    ) -> SqlResult<MutexGuard<'a, std::collections::HashMap<compact_str::CompactString, BranchUse>>>
    {
        let held = |uses: &std::collections::HashMap<compact_str::CompactString, BranchUse>| {
            uses.get(name).is_some_and(|u| states.contains(u))
        };
        if !held(&uses) {
            return Ok(uses);
        }
        let stopped = || interrupts.is_some_and(Interrupts::should_stop);
        self.in_use_waiters.fetch_add(1, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + self.in_use_wait;
        let mut interrupted = false;
        while held(&uses) {
            if stopped() {
                interrupted = true;
                break;
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                break;
            }
            let slice = if interrupts.is_some() { left.min(IN_USE_SLICE) } else { left };
            uses = self
                .in_use_freed
                .wait_timeout(uses, slice)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        self.in_use_waiters.fetch_sub(1, Ordering::SeqCst);
        if interrupted {
            return Err(engine_info(&LimboError::Interrupt));
        }
        Ok(uses)
    }

    /// Claim `name` for a session about to connect to it. One session per branch: a branch another
    /// session is on, or that a delete is releasing, is waited for, up to the wait, as begin_delete
    /// waits, then refused with 55006. Refused at once, a reconnect right after a close met 55006
    /// whenever the closed session's thread had not yet released the branch (wire review 6 item
    /// 5); only a refusal pays the wait (lead ruling).
    fn claim(&self, name: &str, severity: &str) -> SqlResult<()> {
        self.claim_for(name, severity, None)
    }

    /// [`Shared::claim`] for a statement: its cancel or statement_timeout ends the wait.
    fn claim_for(&self, name: &str, severity: &str, interrupts: Option<&Interrupts>) -> SqlResult<()> {
        let mut uses =
            self.wait_while(self.uses(), name, &[BranchUse::Held, BranchUse::Deleting], interrupts)?;
        if let Some(u) = uses.get(name) {
            let mut e = in_use_error(name, *u);
            e.severity = severity.to_string();
            return Err(e);
        }
        // A name of up to 24 bytes is held inline: a claim allocates nothing for it (L5).
        uses.insert(name.into(), BranchUse::Held);
        Ok(())
    }

    /// A session left `name`, or never reached it, or a delete of it ended.
    fn release(&self, name: &str) {
        self.uses().remove(name);
        if self.in_use_waiters.load(Ordering::SeqCst) > 0 {
            self.in_use_freed.notify_all();
        }
    }

    /// Mark `name` as being deleted. A session on it is waited for (up to `in_use_wait`: one that is
    /// closing releases it within that), then refused with 55006.
    #[cfg(test)]
    fn begin_delete(&self, name: &str) -> SqlResult<()> {
        self.begin_delete_for(name, None)
    }

    /// [`Shared::begin_delete`] for a statement: its cancel or statement_timeout ends the wait.
    fn begin_delete_for(&self, name: &str, interrupts: Option<&Interrupts>) -> SqlResult<()> {
        let mut uses =
            self.wait_while(self.uses(), name, &[BranchUse::Held, BranchUse::Deleting], interrupts)?;
        if let Some(u) = uses.get(name) {
            return Err(in_use_error(name, *u));
        }
        uses.insert(name.into(), BranchUse::Deleting);
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
            shared: Arc::new(Shared::new(db, db_file, max_connections, lock_wait)),
            interrupt_count,
            threads: Mutex::new(Vec::new()),
        }
    }

    /// How long a client has to complete its startup (PostgreSQL's authentication_timeout).
    pub fn with_startup_timeout(mut self, timeout: std::time::Duration) -> Self {
        Arc::get_mut(&mut self.shared)
            .expect("set before the server runs")
            .startup_timeout = timeout;
        self
    }

    /// End every session and join its thread: each running statement is cancelled (57014) and
    /// each session's socket is shut, so a thread blocked reading it reads the stream's end and
    /// ends. Waits up to SHUTDOWN_JOIN; returns (sessions shut, threads joined, threads left).
    fn end_sessions(&self) -> (usize, usize, usize) {
        for interrupts in self
            .shared
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter_map(std::sync::Weak::upgrade)
        {
            interrupts.cancel.store(true, Ordering::Relaxed);
        }
        let shut = self.shared.sessions.shut_all();
        let threads = std::mem::take(&mut *self.threads.lock().unwrap_or_else(|e| e.into_inner()));
        let deadline = std::time::Instant::now() + SHUTDOWN_JOIN;
        let (mut joined, mut left) = (0, 0);
        for t in threads {
            while !t.is_finished() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            if t.is_finished() {
                let _ = t.join();
                joined += 1;
            } else {
                left += 1;
            }
        }
        (shut, joined, left)
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
        // The budget harness refuses a --plant split-reply run whose server does not say this.
        #[cfg(feature = "budget-plant-split-reply")]
        println!(
            "PLANT split-reply: every simple-query reply goes out in two writes, and every \
             extended round writes its BindComplete on its own"
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
        drop(listener);
        let (shut, joined, left) = self.end_sessions();
        println!(
            "PostgreSQL server stopped: {shut} sessions shut, {joined} session threads joined, \
             {left} still running"
        );
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
        match spawned {
            Ok(handle) => {
                let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
                threads.retain(|t| !t.is_finished());
                threads.push(handle);
            }
            Err(e) => error!("Error starting a session thread for {}: {}", addr, e),
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

/// The body of a session thread: the protocol on this thread's own blocking socket, until the
/// client goes. The session's connections close when it returns.
///
/// No runtime (L5 step 3(a), lead 2026-10-15): the socket is a std socket in blocking mode
/// (vendored pgwire change 9, BlockingTcp) and the protocol's futures run on a plain executor
/// whose every poll completes, so the thread reads and writes in the system calls themselves.
/// Under tokio's current-thread runtime the session thread made 6 to 8 kevents in every
/// statement's window, the empty query's included (scshim, artie DECISIONS 88b959382d).
fn run_session(
    shared: Arc<Shared>,
    socket: std::net::TcpStream,
    addr: std::net::SocketAddr,
    _slot: SessionSlot,
) {
    // A handle on the socket for the server's shutdown, which shuts it so a blocked read returns.
    // Given back by a guard, so a session that panics (an unwinding build) does not leak the dup
    // (Claude C37 M3).
    let _registered = socket
        .try_clone()
        .ok()
        .map(|handle| Registered::new(&shared.sessions, handle));
    let session = Arc::new(Session::new(shared.clone()));
    if let Err(e) = futures::executor::block_on(serve_session(socket, session)) {
        error!("Error processing connection from {}: {}", addr, e);
    }
}

/// A session's socket held in [`SessionSockets`] until the guard drops, however the session ends.
struct Registered<'a> {
    sockets: &'a SessionSockets,
    id: u64,
}

impl<'a> Registered<'a> {
    fn new(sockets: &'a SessionSockets, socket: std::net::TcpStream) -> Self {
        Self {
            id: sockets.register(socket),
            sockets,
        }
    }
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.sockets.deregister(self.id);
    }
}

/// How long a client has to finish its startup by default, in seconds, as pgwire's process_socket
/// allows (and PostgreSQL's authentication_timeout's default); `--authentication-timeout` sets it.
pub const DEFAULT_AUTHENTICATION_TIMEOUT_S: u64 = 60;

/// Before startup completes the session reads its startup's own messages alone: a malformed
/// message of any kind, or any extended-protocol message (Parse, Bind, Close, Describe, Execute,
/// Flush, Sync), is answered FATAL 08P01 and the session ends, as PostgreSQL ends a session that
/// sends anything but its password response while it authenticates. A malformed P/B/C/D/E/H
/// reached the extended protocol's skip to Sync, and the next Sync set ReadyForQuery: a session
/// that served queries it never authenticated (wire review 16 item 22). Unreachable with
/// NoopStartupHandler, which never waits in AuthenticationInProgress; this holds for the day a
/// handler does. A well-formed simple Query there is left to the startup handler, as before.
fn refused_before_startup(
    state: &PgWireConnectionState,
    msg: &PgWireFrontendMessage,
) -> Option<ErrorInfo> {
    let starting = matches!(
        state,
        PgWireConnectionState::AwaitingSslRequest
            | PgWireConnectionState::AwaitingStartup
            | PgWireConnectionState::AuthenticationInProgress
    );
    let refused = match msg {
        PgWireFrontendMessage::Malformed(kind, code, message) => {
            format!("{code}: malformed message of type '{}' ({message})", *kind as char)
        }
        msg if msg.is_extended_query() => "an extended-protocol message".to_string(),
        _ => return None,
    };
    starting.then(|| {
        ErrorInfo::new(
            "FATAL".to_string(),
            "08P01".to_string(),
            format!("invalid frontend message before startup completed: {refused}"),
        )
    })
}

/// pgwire's process_socket, plus the end of a session. pgwire drops a Terminate ('X') and serves
/// on until the client's socket reaches EOF, so a session, its branch connection and its in-use
/// entry outlived the client's close: bbload's synchronous close waits for the server's EOF and
/// failed every connect op after 5 s, and a delete right after a close met 55006 (wire review 3
/// items 3 and 4). Here a Terminate ends the session, and on every way out the session releases
/// its branch and its in-use entry BEFORE the socket closes (the socket is dropped on return).
async fn serve_session(
    socket: std::net::TcpStream,
    session: Arc<Session>,
) -> Result<(), std::io::Error> {
    // The whole startup must end by one deadline: a read that reaches it reads as the stream's end
    // (BlockingTcp), so a client that never finishes its startup, or trickles it a byte at a time,
    // cannot hold the thread and its max_connections slot.
    let startup_deadline = std::time::Instant::now() + session.shared.startup_timeout;
    // Every way a session ends before its startup completes is logged: the client sees only the
    // socket close (the "closed the connection during startup" flake, lead).
    let socket = pgwire::tokio::server::negotiate_blocking(socket, Some(startup_deadline))
        .await
        .inspect_err(|e| {
            warn!("session ended before startup: TLS negotiation failed: {}", e);
        })?;
    let Some(mut socket) = socket else {
        warn!(
            "session ended before startup: a direct TLS handshake, or a repeated SSLRequest or \
             GSSENCRequest (refused with FATAL 0A000)"
        );
        return Ok(());
    };
    // The handlers by their concrete types: the session, and pgwire's no-op ones for COPY FROM
    // STDIN, cancel and error logging. Taken through PgWireServerHandlers' `impl Trait` returns,
    // the codec's statement type borrowed the handler set, which the socket outlived (E0597 at
    // the 376e950aa gate check).
    let startup_handler = session.clone();
    let simple_query_handler = session.clone();
    let extended_query_handler = session.clone();
    let copy_handler = Arc::new(NoopHandler);
    let cancel_handler = Arc::new(CancelSessions(session.shared.clone()));
    let error_handler = NoopHandler;
    let mut started = false;
    let result = loop {
        let starting = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );
        if !starting && !started {
            started = true;
            if let pgwire::tokio::server::MaybeTls::Blocking(io) = socket.get_mut() {
                io.deadline = None;
                io.socket.set_read_timeout(None)?;
                io.socket.set_write_timeout(None)?;
            }
        }
        let msg = {
            let _reading = turso_pg::phase::enter(turso_pg::phase::Phase::Read);
            socket.next().await
        };
        let msg = match msg {
            Some(Ok(msg)) => msg,
            // A frame the codec cannot read: a length below 4 or past the limit, an unknown
            // message type, or a startup-phase message that does not hold what it says. The
            // stream is out of step (or the session never started), so the session ends with
            // FATAL 08P01, as PostgreSQL ends it on an invalid frontend message (wire review 5
            // item 4: it closed without a word). A message read whole whose body is short or
            // malformed does not come here: the vendored pgwire hands it on as
            // PgWireFrontendMessage::Malformed, answered below with an ERROR (08P01) and, in the
            // extended protocol, a skip to Sync, as PostgreSQL answers it (wire review 10 item 2;
            // postgres/vendor/pgwire/VENDORED.md).
            Some(Err(e)) => {
                error!("invalid frontend message: {}", e);
                let info = ErrorInfo::new(
                    "FATAL".to_string(),
                    "08P01".to_string(),
                    format!("invalid frontend message: {e}"),
                );
                let _ = socket
                    .send(PgWireBackendMessage::ErrorResponse(info.into()))
                    .await;
                break Ok(());
            }
            None => {
                if starting {
                    let timed_out = matches!(socket.get_ref(),
                        pgwire::tokio::server::MaybeTls::Blocking(io) if io.timed_out);
                    if timed_out {
                        warn!("session ended before startup: the startup timeout passed");
                    } else {
                        warn!("session ended before startup: the client's stream ended");
                    }
                }
                break Ok(());
            }
        };
        if matches!(msg, PgWireFrontendMessage::Terminate(_)) {
            break Ok(());
        }
        if let Some(info) = refused_before_startup(&socket.state(), &msg) {
            let _ = socket
                .send(PgWireBackendMessage::ErrorResponse(info.into()))
                .await;
            break Ok(());
        }
        // A Sync whose body is malformed ends any skip; a simple Query whose body is malformed is
        // ignored during one, as PostgreSQL ignores every message but Sync there before reading
        // its body (it ended the skip, and the extended messages after it ran: wire review 15
        // item 5). Outside a skip either fails the block it arrives in and ends its round as
        // on_sync does (Session::end_round, then the ended transaction's named portals dropped:
        // the round's portals outlived it, wire review 15 item 4), answered with its ERROR and
        // ReadyForQuery carrying the session's real state, as PostgreSQL answers them. A malformed
        // Sync got no ReadyForQuery, so the client hung; a malformed Query's error never reached
        // the session's block, so COMMIT committed (wire review 12 items 6 and 7). Before startup
        // completes there is no round: refused_before_startup ended the session above (wire
        // review 15 item 19, review 16 item 22). A malformed CopyFail outside COPY is ignored, as
        // PostgreSQL ignores a stray CopyFail.
        if let PgWireFrontendMessage::Malformed(kind, code, ref message) = msg {
            let state = socket.state();
            let copying = matches!(state, PgWireConnectionState::CopyInProgress(_));
            if kind == b'Q' && matches!(state, PgWireConnectionState::AwaitingSync) {
                continue;
            }
            if matches!(kind, b'S' | b'Q') && !copying {
                let info = ErrorInfo::from(PgWireError::MalformedMessage {
                    code,
                    message: message.clone(),
                });
                session.fail_block();
                socket.set_state(PgWireConnectionState::ReadyForQuery);
                let (notices, status) = session.end_round();
                socket.set_transaction_status(status);
                for name in session.ended_portals() {
                    socket.portal_store().rm_portal(&name);
                }
                let changed = session.changed_parameters();
                let sent = async {
                    socket
                        .feed(PgWireBackendMessage::ErrorResponse(info.into()))
                        .await?;
                    for notice in notices {
                        socket
                            .feed(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                                *notice,
                            )))
                            .await?;
                    }
                    if let Some(changed) = changed {
                        socket
                            .feed(PgWireBackendMessage::ParameterStatus(changed))
                            .await?;
                    }
                    socket
                        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            status,
                        )))
                        .await
                }
                .await;
                if let Err(e) = sent {
                    break Err(std::io::Error::other(e));
                }
                continue;
            }
            if kind == b'f' && !copying {
                continue;
            }
        }
        let is_extended_query = match socket.state() {
            PgWireConnectionState::CopyInProgress(extended) => extended,
            _ => msg.is_extended_query(),
        };
        // A Sync or a simple query can end the transaction; its named portals go with it.
        let may_end_transaction = matches!(
            msg,
            PgWireFrontendMessage::Sync(_) | PgWireFrontendMessage::Query(_)
        );
        if let Err(mut e) = pgwire::tokio::server::process_message(
            msg,
            &mut socket,
            startup_handler.clone(),
            simple_query_handler.clone(),
            extended_query_handler.clone(),
            copy_handler.clone(),
            cancel_handler.clone(),
        )
        .await
        {
            error_handler.on_error(&socket, &mut e);
            // Whatever raised it, an extended-protocol error fails the block it is in (wire
            // review 4 item 1).
            if is_extended_query {
                session.fail_block();
            }
            // A FATAL error ends the session here: its ErrorResponse, no ReadyForQuery, and the
            // session gone before anything more is read. pgwire's process_error answered it, shut
            // only its write half and returned, so the loop read on: a pipelined query ran unseen
            // (on the trunk, after a refused startup) and the branch's in-use entry stayed until
            // the client's EOF (wire review 5 item 4).
            let info = ErrorInfo::from(e);
            if info.is_fatal() {
                let sent = socket
                    .send(PgWireBackendMessage::ErrorResponse(info.into()))
                    .await;
                break sent.map_err(std::io::Error::other);
            }
            // Outside the extended protocol process_error ends the round with ReadyForQuery; a
            // changed reported setting goes ahead of it, as on_query sends it.
            if !is_extended_query {
                if let Some(changed) = session.changed_parameters() {
                    if let Err(e) = socket
                        .feed(PgWireBackendMessage::ParameterStatus(changed))
                        .await
                    {
                        break Err(std::io::Error::other(e));
                    }
                }
            }
            if let Err(io) = pgwire::tokio::server::process_error(
                &mut socket,
                PgWireError::UserError(Box::new(info)),
                is_extended_query,
            )
            .await
            {
                break Err(io);
            }
        }
        // As PostgreSQL drops a transaction's portals when it ends, so a named portal is not run
        // after its transaction (wire review 12 item 5).
        if may_end_transaction {
            for name in session.ended_portals() {
                socket.portal_store().rm_portal(&name);
            }
        }
    };
    // The session's branch and in-use entry go first; the socket closes when it drops below.
    session.end();
    drop(socket);
    result
}

/// Raise this process's open-file soft limit so `max_connections` sessions fit. A session holds
/// its socket and the shutdown's dup of it (2; with tokio's runtime per session it held 5, read
/// with lsof on a live server), and 6 stay budgeted per session. A limit that cannot be raised that far is reported, and the server then
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

/// A simple Query's replies, each with its statement's notices: inline room for one, so a query
/// of one statement (a branch call's) costs no allocation for its list (L5).
type Replies = smallvec::SmallVec<[(Vec<Box<ErrorInfo>>, Response); 1]>;

struct Session {
    shared: Arc<Shared>,
    state: Mutex<SessionState>,
    query_parser: Arc<ParsedQueryParser>,
    /// The session's SET lock_timeout (ms, 0 none) and client_min_messages rank, copied from its
    /// connection as each statement starts (`run`), so a branch call's wait and the notice drain
    /// read them without the session's lock (wire review 16 item 12).
    lock_timeout_ms: AtomicU64,
    min_messages: std::sync::atomic::AtomicU8,
    /// The session's stop requests, which its connections' progress handlers read.
    interrupts: Arc<Interrupts>,
    /// The session's BackendKeyData: (process id, secret key).
    key: (i32, i32),
}


/// A statement as Parse stored it: its text, and the branch call the text is, read once, at Parse
/// ([`branch_call`]). Bind, Describe and Execute read the call from here: each read the text
/// again, so a slow-form call (a comment, a quoted name) cost a libpg_query parse at Bind, at each
/// Describe and at Execute, and an ordinary statement mentioning the prefix up to 3 beside its own
/// prepare (wire review 13 item 6).
///
/// With the parameter and result types the statement had when it first met the engine, at its
/// first Describe or Execute: PostgreSQL fixes both at Parse, and a later Describe or Execute that
/// finds others (a branch switch, a DDL or a search_path change altered what the statement reads)
/// is refused, 0A000, as PostgreSQL's RevalidateCachedQuery refuses a changed result type. They
/// were inferred again at each prepare, so a parameter Describe announced as int4 was read as text
/// on a branch where its column is TEXT, its four binary bytes as UTF-8 (wire review 14 item 11).
/// A re-Parse stores a new statement and Close drops this one, which clears them.
///
/// With its parameters as PostgreSQL's lexer reads them ([`scan_params`]), read here once, so Bind
/// counts every statement's values before anything of it runs (wire review 17 item 2).
#[derive(Debug, Clone)]
struct Parsed {
    sql: String,
    call: Option<PgBranchCall>,
    scan: ParamScan,
    params: OnceLock<Vec<Type>>,
    columns: OnceLock<Vec<(String, Type)>>,
}

impl Parsed {
    fn new(sql: String) -> Self {
        let call = branch_call(&sql);
        // A branch call's parameters are its arguments' (branch_call_types). Without a '$'
        // byte there can be no parameter token; otherwise use the parser's lexer once at Parse.
        let scan = if call.is_some() || !sql.as_bytes().contains(&b'$') {
            ParamScan::default()
        } else {
            scan_params(&sql)
        };
        Self {
            sql,
            call,
            scan,
            params: OnceLock::new(),
            columns: OnceLock::new(),
        }
    }

    /// Fix the statement's parameter types at the first call, and refuse any later call whose
    /// types are others. The same text with the same declared types has the same parameters, so
    /// only their types can differ.
    fn fix_params(&self, params: &[Type]) -> SqlResult<()> {
        match fix_or_first_change(&self.params, params) {
            None => Ok(()),
            Some(i) => Err(error(
                "0A000",
                format!(
                    "cached plan must not change the type of parameter ${}",
                    i + 1
                ),
            )),
        }
    }

    /// Fix the statement's result columns, each its name and type, at the first call, and refuse
    /// any later call whose columns are others, in PostgreSQL's words: its equalRowTypes compares
    /// names with types, so the same types in another order under other names are another row
    /// type. Compared by type alone, a SELECT * over (b int, a int) ran under the (a int, b int)
    /// RowDescription its Describe sent, each value in the other's column (wire review 18 item 9).
    fn fix_columns(&self, columns: &[(String, Type)]) -> SqlResult<()> {
        match fix_or_first_change(&self.columns, columns) {
            None => Ok(()),
            Some(_) => Err(error(
                "0A000",
                "cached plan must not change result type".to_string(),
            )),
        }
    }
}

/// [`Parsed::fix_params`] and [`Parsed::fix_columns`]' one comparison (they were the same code
/// twice; wire review 18 item 23): `now` fixed in `cell` at the first call; at a later one, None
/// when it is what was fixed, else the index of the first element that differs (the shorter
/// one's length when one is a prefix of the other).
fn fix_or_first_change<T: Clone + PartialEq>(cell: &OnceLock<Vec<T>>, now: &[T]) -> Option<usize> {
    let fixed = cell.get_or_init(|| now.to_vec());
    (fixed.as_slice() != now).then(|| fixed.iter().zip(now).take_while(|(a, b)| a == b).count())
}

/// pgwire's query parser for [`Parsed`] statements. This server overrides every handler that
/// calls one (Parse among them), so it is pgwire's type requirement only.
struct ParsedQueryParser;

#[async_trait]
impl QueryParser for ParsedQueryParser {
    type Statement = Parsed;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Parsed::new(sql.to_owned()))
    }
}

#[derive(Default)]
struct SessionState {
    /// The session's trunk connection, opened at its first use and kept while the session is on a
    /// branch, so switching back to the trunk opens nothing.
    trunk: Option<PgConnection>,
    /// The named branch the session is on, with the branch's connection.
    /// The name inline up to 24 bytes: a switch allocates nothing for it (L5).
    branch: Option<(compact_str::CompactString, PgConnection)>,
    /// A statement failed inside a transaction block: as in PostgreSQL, everything but the block's
    /// end (or a ROLLBACK TO SAVEPOINT) is refused with 25P02 until then.
    aborted: bool,
    /// The branch a switch just left, released in [`Shared`]'s map when the switch's statement ends.
    left: Option<compact_str::CompactString>,
    /// The session's connection is in the implicit transaction block PostgreSQL wraps a
    /// multi-statement query, or a pipeline up to Sync, in: an engine BEGIN the client did not send
    /// (see [`Session::begin_implicit`]).
    implicit: bool,
    /// The statement the last Describe prepared, for the Execute that follows it to run instead of
    /// preparing it again (see [`Session::describe_prepare`]); cleared at Sync.
    described: Option<Described>,
    /// Notices the session's statements raised and the client has not been sent yet: the simple
    /// protocol sends each before its statement's result, the extended one before ReadyForQuery.
    notices: Vec<Box<ErrorInfo>>,
    /// The named portals bound in the current transaction, dropped when it ends, as PostgreSQL
    /// drops a transaction's portals (see `serve_session`; wire review 12 item 5).
    portals: Vec<String>,
    /// The named portals that ran to completion without rows, or failed: a second Execute of one
    /// is 55000, never a second run (wire review 12 item 5).
    done_portals: std::collections::HashSet<String>,
    /// The completion tag of each query portal that returned all its rows (its verb's, `SELECT`,
    /// `INSERT 0`, ...), which a further Execute answers with 0 rows, as PostgreSQL does (wire
    /// review 15 item 17: it answered SELECT 0 for every portal).
    finished_tags: std::collections::HashMap<String, String>,
    /// The first result format code other than 0 or 1 each portal was bound with: refused
    /// (22023) when a row is formatted, as PostgreSQL's printtup refuses it, never at the Bind,
    /// so a statement that returns no row succeeds (wire review 13 item 10).
    bad_result_codes: std::collections::HashMap<String, i16>,
    /// The search path last sent in a ParameterStatus, with its connection's stamp then and the
    /// path as SET gave it ([`Session::changed_parameters`]); None while it is the one startup
    /// announced.
    reported_path: Option<(u64, Vec<String>, String)>,
}

/// An engine statement's failure, with whether it got as far as running: a COMMIT or ROLLBACK that
/// failed to prepare (a syntax error) is a failed statement like any other, while one that ran and
/// failed ended or did not end the block by its outcome (wire review 3 item 5). `rerunnable`: it
/// changed nothing before it failed, so running it anew cannot apply it twice.
struct StatementFailure {
    prepared: bool,
    rerunnable: bool,
    /// The statement, as translated, is a COMMIT ([`StatementTypes::commits`]).
    commits: bool,
    /// The statement, as translated, is a ROLLBACK of the whole block
    /// ([`StatementTypes::rolls_back`]).
    rolls_back: bool,
    info: Box<ErrorInfo>,
}

/// A statement Describe prepared: on which engine connection, from which text.
struct Described {
    conn: Arc<turso_core::Connection>,
    sql: String,
    declared: Vec<Option<u32>>,
    stmt: turso_core::Statement,
    /// The types the parse gave (see [`PgConnection::prepare_typed`]).
    types: StatementTypes,
}

/// CHECKPOINTs skipped inside a block because the WAL was busy (see [`Session::checkpoint`]),
/// since the server started: turso_branch_stats' `checkpoints_skipped`.
static CHECKPOINTS_SKIPPED: AtomicU64 = AtomicU64::new(0);

impl Session {
    fn new(shared: Arc<Shared>) -> Self {
        let interrupts = Arc::new(Interrupts::new());
        let pid = shared.next_pid.fetch_add(1, Ordering::Relaxed);
        // The secret from the OS's CSPRNG (one draw per session): only a client that read the
        // BackendKeyData can cancel the session, and a CancelRequest must name both. A session
        // whose secret cannot be drawn is registered for no cancel at all (fail closed); its
        // BackendKeyData still carries a key, which names nothing.
        let mut bytes = [0u8; 4];
        let drawn = getrandom::fill(&mut bytes).is_ok();
        let secret = i32::from_ne_bytes(bytes);
        if drawn {
            shared
                .cancels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert((pid, secret), Arc::downgrade(&interrupts));
        } else {
            warn!("no cancel key for session {pid}: the OS random source failed");
        }
        Self {
            shared,
            state: Mutex::new(SessionState::default()),
            query_parser: Arc::new(ParsedQueryParser),
            lock_timeout_ms: AtomicU64::new(0),
            min_messages: std::sync::atomic::AtomicU8::new(7),
            interrupts,
            key: (pid, secret),
        }
    }

    /// A connection the session opened stops its statement when the session's [`Interrupts`] ask.
    fn arm(&self, conn: &Arc<turso_core::Connection>) {
        let interrupts = self.interrupts.clone();
        conn.set_progress_handler(PROGRESS_OPS, Some(Box::new(move || interrupts.should_stop())));
    }

    /// How long a statement waits for a lock: the session's SET lock_timeout, else (unset, or 0,
    /// PostgreSQL's "no timeout") the server's lock wait.
    fn lock_wait(&self) -> std::time::Duration {
        match self.lock_timeout_ms.load(Ordering::Relaxed) {
            0 => self.shared.lock_wait,
            ms => std::time::Duration::from_millis(ms),
        }
    }

    /// The notices the session's client_min_messages lets through: a NOTICE or WARNING below its
    /// level is not sent (an INFO is always sent, as in PostgreSQL).
    fn deliverable(&self, notices: Vec<Box<ErrorInfo>>) -> Vec<Box<ErrorInfo>> {
        let least = self.min_messages.load(Ordering::Relaxed);
        notices
            .into_iter()
            .filter(|n| turso_pg::message_rank(&n.severity).is_none_or(|rank| rank >= least))
            .collect()
    }

    /// The session's state. Only the session's own thread takes the lock, so it never waits; a
    /// statement that panicked mid-way leaves the state as it was, which is still valid.
    fn state(&self) -> MutexGuard<'_, SessionState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn open_trunk(&self) -> SqlResult<PgConnection> {
        let conn = PgConnection::new(self.shared.db.connect().map_err(|e| engine_info(&e))?);
        self.arm(conn.inner());
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

    /// A named portal's statement ran to completion without rows, or failed: it does not run again.
    fn portal_done(&self, name: &str) {
        if name != DEFAULT_NAME {
            self.state().done_portals.insert(name.to_string());
        }
    }

    /// The end of an extended-protocol round, at a Sync whether or not its body was well formed
    /// (wire review 15 item 4): the extended protocol's results were fed as each Execute ran, so
    /// what is left is its notices, to go out now, a statement a Describe kept but no Execute ran,
    /// dropped, and the transaction status ReadyForQuery carries.
    fn end_round(&self) -> (Vec<Box<ErrorInfo>>, TransactionStatus) {
        let notices = {
            let mut st = self.state();
            st.described = None;
            std::mem::take(&mut st.notices)
        };
        (self.deliverable(notices), self.transaction_status())
    }

    /// The ParameterStatus PostgreSQL sends ahead of a ReadyForQuery for a reported setting
    /// (GUC_REPORT) whose value changed since it was last sent: here search_path, the one such
    /// setting SET changes (the others keep the values startup announced, from turso_pg's
    /// FIXED_SETTINGS). None when the path is as last sent, or before any connection is open, as
    /// nothing has set one then. A SET search_path changed SHOW's answer and the client's copy
    /// stayed at the startup value (wire review 18 item 19). Per round: one relaxed load of the
    /// connection's stamp; the path is read only when the stamp moved.
    fn changed_parameters(&self) -> Option<ParameterStatus> {
        let mut st = self.state();
        let conn = match (&st.branch, &st.trunk) {
            (Some((_, conn)), _) | (None, Some(conn)) => conn.clone(),
            (None, None) => return None,
        };
        let stamp = conn.search_path_stamp();
        let (last_stamp, last_path, last_shown) = st
            .reported_path
            .get_or_insert_with(|| (0, Vec::new(), turso_pg::DEFAULT_SEARCH_PATH.to_string()));
        if *last_stamp == stamp {
            return None;
        }
        *last_stamp = stamp;
        // A branch switch moves the stamp (another connection) and keeps the path (adopted): the
        // path is compared first, so the switch formats nothing (L5: the shown path was the
        // switch's one allocation over the empty query's).
        if conn.search_path_is(last_path) {
            return None;
        }
        *last_path = conn.search_path();
        let shown = conn.shown_search_path();
        if *last_shown == shown {
            return None;
        }
        last_shown.clone_from(&shown);
        Some(ParameterStatus::new("search_path".to_string(), shown))
    }

    /// The portals of a transaction that has ended, the unnamed one included, for `serve_session`
    /// to drop: none while a block is open (wire review 12 item 5, review 15 item 16).
    fn ended_portals(&self) -> Vec<String> {
        if !matches!(self.transaction_status(), TransactionStatus::Idle) {
            return Vec::new();
        }
        let mut st = self.state();
        st.done_portals.clear();
        let ended = std::mem::take(&mut st.portals);
        for name in &ended {
            st.bad_result_codes.remove(name);
            st.finished_tags.remove(name);
        }
        ended
    }

    /// ReadyForQuery's status, read from the session's state rather than inferred from the
    /// statements it ran (its doc sat on portal_done, inserted between them: wire review 15 item
    /// 22).
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
        let conn = PgConnection::new(conn);
        self.arm(conn.inner());
        self.state().branch = Some((branch.into(), conn));
        Ok(())
    }

    /// Every statement of one simple-protocol query, in order, up to and including the first that
    /// fails.
    fn simple(&self, query: &str) -> Vec<Response> {
        self.simple_with_notices(query)
            .into_iter()
            .map(|(_, r)| r)
            .collect()
    }

    /// [`Session::simple`], each result with the notices its statement raised, in [`Replies`].
    fn simple_with_notices(&self, query: &str) -> Replies {
        let with_notices = |r: Response| {
            let notices = std::mem::take(&mut self.state().notices);
            (self.deliverable(notices), r)
        };
        // A query that is one branch call goes straight to the engine: no split, no parse (L5).
        // Inside an unsynced pipeline's implicit block it ends the block as any Query does: a
        // refusal rolls it back and leaves the session idle, success commits it. The fast path
        // left the block open, and a refusal left the session failed (wire review 16 item 6);
        // with no block open it costs one look at the session's flag.
        let parsing = turso_pg::phase::enter(turso_pg::phase::Phase::Parse);
        let call = branch_call(query);
        drop(parsing);
        if let Some(call) = call {
            let result = self.run(query, Some(call), None, &Format::UnifiedText);
            let open = self.state().implicit;
            if open {
                self.after_implicit(query, result.is_err());
            }
            // Inline room for the one reply: a branch call's list costs no allocation (L5).
            let mut responses = Replies::new();
            responses.push(with_notices(result.unwrap_or_else(Response::Error)));
            if open {
                if let Err(e) = self.end_implicit() {
                    responses.push(with_notices(Response::Error(e)));
                }
            }
            return responses;
        }
        // One statement (no separator but a trailing one): run as it stands in the query, with no
        // split Vec or copy (L5). It is no branch call (read above).
        let parsing = turso_pg::phase::enter(turso_pg::phase::Phase::Parse);
        let single = single_statement(query);
        drop(parsing);
        if let Some(sql) = single {
            let result = self.run(sql, None, None, &Format::UnifiedText);
            self.after_implicit(sql, result.is_err());
            let mut responses = Replies::new();
            responses.push(with_notices(result.unwrap_or_else(Response::Error)));
            if let Err(e) = self.end_implicit() {
                responses.push(with_notices(Response::Error(e)));
            }
            return responses;
        }
        let statements = match split_statements(query) {
            Ok(s) => s,
            Err(e) => {
                let mut responses = Replies::new();
                responses.push((Vec::new(), Response::Error(engine_info(&e))));
                return responses;
            }
        };
        let mut responses = Replies::with_capacity(statements.len());
        // More than one statement: one implicit transaction, as in PostgreSQL (wire review 1 item 8).
        let multi = statements.len() > 1;
        for sql in &statements {
            // A query of one statement was read above as a whole and is no call: it is not read
            // again (wire review 13 item 6).
            let call = if multi { branch_call(sql) } else { None };
            let result = if multi {
                self.begin_implicit(sql, call.as_ref(), false)
            } else {
                Ok(())
            }
            .and_then(|()| self.run(sql, call, None, &Format::UnifiedText));
            // Every statement of a Query in an implicit block, a lone one inside an unsynced
            // pipeline's included: a lone BEGIN there makes the pipeline's block the client's, as
            // in PostgreSQL, where end_implicit committed it and answered idle (wire review 16
            // item 6). With no block open after_implicit returns at once.
            self.after_implicit(sql, result.is_err());
            match result {
                Ok(r) => responses.push(with_notices(r)),
                Err(e) => {
                    responses.push(with_notices(Response::Error(e)));
                    break;
                }
            }
        }
        if let Err(e) = self.end_implicit() {
            responses.push(with_notices(Response::Error(e)));
        }
        responses
    }

    /// Open the implicit transaction block PostgreSQL runs a multi-statement query, or a pipeline
    /// up to Sync, in, before `sql`: an engine BEGIN the client did not send, when the session is
    /// in no block. Not for the block verbs themselves (BEGIN makes the block the client's, COMMIT
    /// and ROLLBACK end it), and not for a branch call that opens a pipeline (`pipeline`), which
    /// runs and commits at once as CREATE DATABASE does in PostgreSQL; inside an open block a
    /// branch call is refused with 25001 like any other. Through the engine connection itself, so
    /// it costs no libpg_query call (wire review 1 item 8).
    fn begin_implicit(
        &self,
        sql: &str,
        call: Option<&PgBranchCall>,
        pipeline: bool,
    ) -> SqlResult<()> {
        let mut st = self.state();
        // A flag the engine no longer backs (a statement ended its transaction) is cleared, and the
        // block begun afresh: the flag follows the engine, never the text (wire review 13 item 1).
        if st.implicit {
            match self.current(&mut st) {
                Ok(conn) if conn.inner().get_auto_commit() => st.implicit = false,
                _ => return Ok(()),
            }
        }
        if st.aborted || TxVerb::of(sql) != TxVerb::Other {
            return Ok(());
        }
        // A CHECKPOINT that would open the block runs at top level instead, as the full checkpoint
        // (an in-block one is only a passive attempt, see `checkpoint`).
        if (pipeline && call.is_some()) || is_checkpoint(sql) {
            return Ok(());
        }
        let conn = self.current(&mut st)?;
        if !conn.inner().get_auto_commit() {
            return Ok(());
        }
        conn.engine_tx(TxStmt::Begin).map_err(|e| engine_info(&e))?;
        st.implicit = true;
        Ok(())
    }

    /// After a statement of an open implicit block: a failure rolls the whole block back and leaves
    /// the session idle, as PostgreSQL does; a block verb (BEGIN, COMMIT, ROLLBACK) has made the
    /// block the client's or ended it. Any other statement leaves the block implicit: a savepoint
    /// verb (refused there, see `run`) never ends it (wire review 11 item 6).
    fn after_implicit(&self, sql: &str, failed: bool) {
        let mut st = self.state();
        if !st.implicit {
            return;
        }
        st.implicit = false;
        if failed {
            st.aborted = false;
            // A rollback that fails breaks the connection, and the next statement ends the
            // session (run): the block's work is still held (wire review 16 item 14).
            if let Ok(conn) = self.current(&mut st) {
                let _ = conn.roll_back_or_break();
            }
        } else if TxVerb::of(sql) != TxVerb::Begin {
            // The block lasts while the engine holds its transaction: a statement that ended it,
            // whatever its text was read as, ends the block. Read from the text, a COMMIT the verb
            // reader missed (`COMMIT<NBSP>`) left the flag set while the engine was back in
            // autocommit, and every later statement committed on its own (wire review 13 item 1).
            st.implicit = self
                .current(&mut st)
                .is_ok_and(|conn| !conn.inner().get_auto_commit());
        }
    }

    /// An extended-protocol message failed, whatever raised the error: a statement's run, a refused
    /// binary encoding, a Bind or Execute of a portal that does not exist, pgwire itself. As in
    /// PostgreSQL the failure rolls back the pipeline's implicit block, or fails the client's block
    /// (25P02 until its end, which answers ROLLBACK), so neither commits at Sync or at the block's
    /// COMMIT. Errors raised outside `run` reached neither (wire review 4 item 1). A block the
    /// statement's own failure already ended or failed is left as it is. The session's open
    /// connection is read, never opened: a session with none has no block.
    fn fail_block(&self) {
        let mut st = self.state();
        let implicit = std::mem::take(&mut st.implicit);
        let open = |st: &SessionState| match &st.branch {
            Some((_, conn)) => Some(conn.clone()),
            None => st.trunk.clone(),
        };
        let Some(conn) = open(&st).filter(|conn| !conn.inner().get_auto_commit()) else {
            return;
        };
        if implicit {
            st.aborted = false;
            // A failed rollback breaks the connection (roll_back_or_break), read by run.
            let _ = conn.roll_back_or_break();
        } else {
            st.aborted = true;
        }
    }

    /// Commit an open implicit block: after a multi-statement query's last statement, or at Sync. A
    /// failed commit (a deferred constraint) is the query's error, and nothing of it is kept.
    fn end_implicit(&self) -> SqlResult<()> {
        let mut st = self.state();
        if !std::mem::take(&mut st.implicit) {
            return Ok(());
        }
        let conn = self.current(&mut st)?;
        if conn.inner().get_auto_commit() {
            return Ok(());
        }
        // A statement of the block failed where nothing rolled it back (a simple Query inside an
        // unsynced pipeline): the block rolls back, never commits (wire review 13 item 1).
        if std::mem::take(&mut st.aborted) {
            return conn
                .roll_back_or_break()
                .map_err(|e| broken_fatal(engine_info(&e)));
        }
        if let Err(e) = conn.engine_tx(TxStmt::Commit) {
            // The same rule as a client's COMMIT (wire review 8 item 1); a rollback that fails
            // breaks the connection (wire review 16 item 14).
            if let Err(undo) = conn.roll_back_or_break() {
                return Err(broken_fatal(engine_info(&undo)));
            }
            return Err(commit_failed(&conn, engine_info(&e)));
        }
        Ok(())
    }

    /// Run one statement on the session, `call` its branch call if it is one (already read), and
    /// `portal` carrying its bound parameters (extended protocol). Its transaction-control outcome
    /// is reported to the connection's settings (turso_pg's GucScope; the C26 MED 3 class): a block
    /// the statement ended, read from the engine (in a transaction before, in none after), is
    /// committed only by a COMMIT that succeeded in a block that had not failed; a savepoint verb
    /// that succeeded is reported as itself. The warnings the frontend raised go out as notices.
    fn run(
        &self,
        sql: &str,
        call: Option<PgBranchCall>,
        portal: Option<&Portal<Parsed>>,
        format: &Format,
    ) -> SqlResult<Response> {
        let before = {
            let mut st = self.state();
            let aborted = st.aborted;
            self.current(&mut st)
                .ok()
                .map(|conn| (conn.clone(), !conn.inner().get_auto_commit(), aborted))
        };
        let result = self.run_statement(sql, call, portal, format);
        if let Some((conn, in_tx, aborted)) = before {
            let verb = TxVerb::of_leading_keywords(sql).unwrap_or_else(|| TxVerb::of(sql));
            if in_tx && conn.inner().get_auto_commit() {
                let committed = result.is_ok() && verb == TxVerb::Commit && !aborted;
                conn.tx_event(turso_pg::TxEvent::Ended { committed }, sql);
            } else if result.is_ok() {
                let event = match verb {
                    TxVerb::Savepoint => Some(turso_pg::TxEvent::Savepoint),
                    TxVerb::Release => Some(turso_pg::TxEvent::Release),
                    TxVerb::RollbackTo => Some(turso_pg::TxEvent::RollbackTo),
                    _ => None,
                };
                if let Some(event) = event {
                    conn.tx_event(event, sql);
                }
            }
            let warnings = conn.take_warnings();
            if !warnings.is_empty() {
                let mut st = self.state();
                for (code, message) in warnings {
                    st.notices.push(warning(code, &message));
                }
            }
        }
        result
    }

    fn run_statement(
        &self,
        sql: &str,
        call: Option<PgBranchCall>,
        portal: Option<&Portal<Parsed>>,
        format: &Format,
    ) -> SqlResult<Response> {
        let verb = TxVerb::of(sql);
        let mut st = self.state();
        let conn = self.current(&mut st)?;
        // A connection an undo failed on (a rollback of an implicit block, after its statement
        // was answered) holds work nobody can name: it serves nothing more, the next statement
        // ending the session, never joining that transaction (wire review 16 item 14).
        if conn.is_broken() {
            return Err(broken_fatal(error(
                "08006",
                format!(
                    "{}: an earlier undo failed; the connection is closed",
                    turso_pg::CONNECTION_BROKEN
                ),
            )));
        }
        // The session's lock wait and message level, as its last SET left them.
        self.lock_timeout_ms
            .store(conn.lock_timeout_ms(), Ordering::Relaxed);
        // A CancelRequest or statement_timeout stops this statement (dropped at its end).
        let _interrupts = self.interrupts.begin(conn.statement_timeout_ms());
        self.min_messages
            .store(conn.min_messages_rank(), Ordering::Relaxed);
        let in_tx = !conn.inner().get_auto_commit();
        // Which kind of block the statement runs in, for the settings a ROLLBACK cannot undo here
        // (turso_pg's handle_pg_set; wire review 16 item 18).
        conn.set_explicit_block(in_tx && !st.implicit);
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
                // A failed block the engine holds no transaction for is one whose failing error
                // made the engine roll the whole transaction back, savepoints and all (a failed
                // block is otherwise still open in the engine): the savepoint does not exist, as
                // PostgreSQL says of one that does not, and the block stays failed. It was XX000
                // "no such savepoint" from the engine (wire review 3 item 10). No statement known
                // today reaches this state over the wire (the sum() overflow that did no longer
                // rolls the transaction back); a_rollback_to_in_a_block_the_engine_ended_is_3b001
                // builds it directly.
                TxVerb::RollbackTo if !in_tx => {
                    return Err(error(
                        "3B001",
                        "savepoint does not exist: the error that failed this transaction block \
                         rolled the whole transaction back, its savepoints with it; end the block \
                         with ROLLBACK"
                            .to_string(),
                    ))
                }
                TxVerb::RollbackTo => {}
                // AND CHAIN ends a failed block as its COMMIT or ROLLBACK does, answered
                // ROLLBACK, and chains a new block, as PostgreSQL does (recorded on 18.6; it was
                // 25P02, the block left failed: wire review 15 item 23).
                TxVerb::Chain { .. } => {
                    if in_tx {
                        conn.execute("ROLLBACK").map_err(|e| engine_info(&e))?;
                    }
                    st.aborted = false;
                    conn.engine_tx(TxStmt::Begin)
                        .map_err(|e| engine_info(&e))?;
                    return Ok(Response::Execution(Tag::new("ROLLBACK")));
                }
                _ => return Err(aborted_error()),
            }
        }
        // Outside a block the client opened, a savepoint verb is refused (below) by its leading
        // keywords, whatever its name's spelling (TxVerb::of_leading_keywords; wire review 14
        // item 15).
        let verb = match verb {
            TxVerb::Other if !in_tx || st.implicit => {
                TxVerb::of_leading_keywords(sql).unwrap_or(TxVerb::Other)
            }
            verb => verb,
        };
        // A block end inside an implicit block (a multi-statement query's, a pipeline's) ends that
        // block through the engine, as below, and warns as PostgreSQL warns there: there is no
        // transaction in progress, for the client (wire review 14 item 20).
        if in_tx && st.implicit && matches!(verb, TxVerb::Commit | TxVerb::Rollback) {
            st.notices
                .push(warning("25P01", "there is no transaction in progress"));
        }
        match verb {
            // PostgreSQL warns and carries on for these; the engine would refuse them. A BEGIN in
            // an implicit block makes the block the client's, unwarned (after_implicit). The
            // warnings were missing (wire review 9 item 7).
            TxVerb::Begin if in_tx => {
                // READ ONLY is refused here as the translator refuses it outside a block (0A000),
                // and the block fails: answered with 25001's warning it was never read, and an
                // implicit block, handed to the client, wrote and committed under it (wire review
                // 15 item 3).
                if begins_read_only(sql) {
                    st.aborted = true;
                    return Err(error(
                        "0A000",
                        "READ ONLY transactions are not supported".to_string(),
                    ));
                }
                if !st.implicit {
                    st.notices.push(warning(
                        "25001",
                        "there is already a transaction in progress",
                    ));
                }
                return Ok(Response::Execution(Tag::new("BEGIN")));
            }
            // AND CHAIN outside a block is PostgreSQL's error, not its warning; inside one the
            // chain is not supported (below). It was read as an ordinary COMMIT, which committed
            // and left the session idle (wire review 11 item 7).
            // In an implicit block too, as PostgreSQL answers there (it was 0A000: wire review 13
            // item 13); the error rolls the implicit block back.
            TxVerb::Chain { commit } if !in_tx || st.implicit => {
                let what = if commit {
                    "COMMIT AND CHAIN"
                } else {
                    "ROLLBACK AND CHAIN"
                };
                return Err(error(
                    "25P01",
                    format!("{what} can only be used in transaction blocks"),
                ));
            }
            TxVerb::Commit | TxVerb::Rollback if !in_tx => {
                st.notices
                    .push(warning("25P01", "there is no transaction in progress"));
                let tag = if verb == TxVerb::Commit {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                };
                return Ok(Response::Execution(Tag::new(tag)));
            }
            // Savepoints exist only in a block the client opened: outside one, and in an implicit
            // block (a multi-statement query, a pipeline before Sync), PostgreSQL refuses them
            // (25P01). Outside a block they reached the engine, which opened a transaction for
            // SAVEPOINT and answered XX000 for the others (wire review 6 item 6, review 3 item 24);
            // in an implicit block they ran and the block's engine transaction was left open with
            // nobody to commit it (wire review 11 item 6).
            TxVerb::RollbackTo | TxVerb::Release | TxVerb::Savepoint if !in_tx || st.implicit => {
                let what = match verb {
                    TxVerb::RollbackTo => "ROLLBACK TO SAVEPOINT",
                    TxVerb::Release => "RELEASE SAVEPOINT",
                    _ => "SAVEPOINT",
                };
                return Err(error(
                    "25P01",
                    format!("{what} can only be used in transaction blocks"),
                ));
            }
            _ => {}
        }
        // Whether a failed engine statement got as far as running (a branch call, a CHECKPOINT
        // or a refusal here is a statement that ran), and whether its translated parse was a
        // COMMIT or a ROLLBACK of the block.
        let mut ran = true;
        let mut parsed: Option<TxVerb> = None;
        let result = match call {
            Some(call) => {
                // A statement a Describe kept holds its connection open: a switch away, or a
                // delete of the branch it is on, must not find that connection still alive.
                st.described = None;
                self.branch(&mut st, &conn, &call, portal, format)
            }
            None if is_checkpoint(sql) => self.checkpoint(&mut st, in_tx),
            // A chain in a block: refused, which fails the block, as any error in it does.
            None if matches!(verb, TxVerb::Chain { .. }) => Err(error(
                "0A000",
                "AND CHAIN is not supported: end the block with COMMIT or ROLLBACK, then BEGIN a \
                 new one"
                    .to_string(),
            )),
            None if schema_ddl(sql).is_some_and(|name| !name.eq_ignore_ascii_case("public")) => {
                Err(error(
                    "0A000",
                    "schemas other than public are not supported by the branch server: its \
                     branches cover the one schema (CREATE SCHEMA and DROP SCHEMA are refused)"
                        .to_string(),
                ))
            }
            None => {
                drop(st);
                let r = self.engine_statement(&conn, sql, verb, portal, format);
                st = self.state();
                r.map_err(|f| {
                    ran = f.prepared;
                    parsed = if f.commits {
                        Some(TxVerb::Commit)
                    } else if f.rolls_back {
                        Some(TxVerb::Rollback)
                    } else {
                        None
                    };
                    f.info
                })
            }
        };
        // The frontend could not undo a failed statement: the connection's state is unknown, so the
        // session ends (FATAL 08006, which serve_session ends the session on) instead of serving
        // more statements on it (wire review 2 item 1). Read from the connection's flag, not the
        // error's text (wire review 5 item 4).
        let result = match result {
            Err(info) if conn.is_broken() => Err(broken_fatal(info)),
            r => r,
        };
        if result.is_err() {
            // The block's end is read from the verb the reader saw or the parse the engine ran,
            // whichever says COMMIT or ROLLBACK, never from the text alone (wire review 14 item 7).
            let verb = parsed.unwrap_or(verb);
            match verb {
                // A COMMIT that ran and failed ends the block, as in PostgreSQL: whatever the
                // engine kept of the transaction is rolled back and the session is idle. If that
                // rollback fails too the block is still open, and failed.
                TxVerb::Commit if ran => {
                    if !conn.inner().get_auto_commit() {
                        let _ = conn.execute("ROLLBACK");
                    }
                    st.aborted = !conn.inner().get_auto_commit();
                }
                // A ROLLBACK that ran: the block is over unless the engine still holds it.
                TxVerb::Rollback if ran => st.aborted = !conn.inner().get_auto_commit(),
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

    /// CHECKPOINT is the trunk's, as PostgreSQL's is the cluster's, so a session on a branch runs it
    /// on its trunk connection. Outside a block it is a TRUNCATE checkpoint that waits for writers
    /// and readers within the lock timeout and fails with 55P03 after it, then the branch store's
    /// checkpoint (wire review 14 item 13). Inside a block the
    /// session's own transaction may hold the very locks a TRUNCATE waits for, so it is a PASSIVE
    /// checkpoint on a connection of its own, which never waits: it backfills what no reader pins.
    /// A busy answer there skips the checkpoint, and says so: a NOTICE, one more in
    /// turso_branch_stats' `checkpoints_skipped`, then the tag (lead ruling 2026-10-06T13:47Z; a
    /// tag alone would claim a checkpoint that did not run). Committed data is durable in the WAL
    /// either way. Any other failure is the statement's (wire review 1 item 4).
    fn checkpoint(&self, st: &mut SessionState, in_tx: bool) -> SqlResult<Response> {
        if in_tx {
            let conn = self.shared.db.connect().map_err(|e| engine_info(&e))?;
            match conn.checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive: None,
            }) {
                Ok(_) => {}
                Err(LimboError::Busy) => {
                    CHECKPOINTS_SKIPPED.fetch_add(1, Ordering::Relaxed);
                    st.notices.push(Box::new(ErrorInfo::new(
                        "NOTICE".to_string(),
                        "00000".to_string(),
                        "checkpoint skipped: the WAL is busy, and inside a transaction block \
                         CHECKPOINT does not wait (this block may hold the lock it needs); run it \
                         outside the block"
                            .to_string(),
                    )));
                }
                Err(e) => return Err(engine_info(&e)),
            }
            return Ok(Response::Execution(Tag::new("CHECKPOINT")));
        }
        if st.trunk.is_none() {
            st.trunk = Some(self.open_trunk()?);
        }
        let trunk = st.trunk.as_ref().expect("opened above").inner().clone();
        self.waiting(|| {
            trunk.checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
        })
        .map_err(|e| engine_info(&e))?;
        // Then the branch store's: its catalog checkpoint, which cuts the branch log, counted in
        // turso_branch_stats' `store_checkpoints` (a no-op for volatile branches). PostgreSQL's
        // CHECKPOINT carries all of the cluster's checkpoint I/O; over the wire this one carried
        // none of the store's, so no client could force or count it (PREREG section 5 M2; wire
        // review 14 item 13). It waits for a fuzzy checkpoint in flight, which is why a block's
        // CHECKPOINT, which never waits, leaves the store alone.
        self.waiting(|| self.shared.db.branch_compact_now())
            .map_err(|e| engine_info(&e))?;
        Ok(Response::Execution(Tag::new("CHECKPOINT")))
    }

    /// As in PostgreSQL, a failed block refuses to describe anything but its own end (25P02).
    fn refuse_describe_if_aborted(&self, sql: &str) -> PgWireResult<()> {
        if self.state().aborted && !TxVerb::of(sql).ends_block() {
            return Err(PgWireError::UserError(aborted_error()));
        }
        Ok(())
    }

    /// Describe's prepare. In the extended protocol Describe is where a statement first meets the
    /// engine, so a statement that fails to prepare inside a block aborts the block, as its
    /// execution would (wire review 1 item 2).
    ///
    /// The prepared statement is kept for the Execute that follows (wire review 1 item 12: one
    /// parse, translation and compile per extended statement, not two): read its columns with
    /// [`Session::described_fields`]. A Describe of the statement already kept prepares nothing.
    #[cfg(test)]
    fn describe_prepare(&self, sql: &str) -> PgWireResult<()> {
        self.describe_prepare_with_params(sql, &[])
    }

    fn describe_prepare_with_params(
        &self,
        sql: &str,
        declared: &[Option<Type>],
    ) -> PgWireResult<()> {
        let declared: Vec<_> = declared
            .iter()
            .map(|ty| ty.as_ref().map(Type::oid))
            .collect();
        let conn = self
            .current(&mut self.state())
            .map_err(PgWireError::UserError)?;
        if self.state().described.as_ref().is_some_and(|d| {
            d.sql == sql && d.declared == declared && Arc::ptr_eq(&d.conn, conn.inner())
        }) {
            return Ok(());
        }
        let in_tx = !conn.inner().get_auto_commit();
        // Never the prepare that performs a statement (ALTER ADD CONSTRAINT, COPY FROM, SET,
        // CREATE/DROP SCHEMA): the frontend declines those from the parse, they keep nothing and
        // answer NoData, and Execute performs them once (wire review 2 item 2, review 3 item 2).
        match conn.prepare_for_describe_with_params(sql, &declared) {
            Ok(Some((stmt, types))) => {
                self.state().described = Some(Described {
                    conn: conn.inner().clone(),
                    sql: sql.to_string(),
                    declared,
                    stmt,
                    types,
                });
                Ok(())
            }
            Ok(None) => {
                self.state().described = None;
                Ok(())
            }
            Err(e) => {
                if in_tx {
                    self.state().aborted = true;
                }
                // In a pipeline's implicit block the failure rolls the whole pipeline back.
                self.after_implicit(sql, true);
                Err(engine_error(e))
            }
        }
    }

    /// The result columns of the statement [`Session::describe_prepare`] kept, a bare parameter's
    /// typed by the parameter's type (`declared`, else inferred: [`result_column_types`]).
    fn described_fields(
        &self,
        format: &Format,
        declared: &[Option<Type>],
    ) -> SqlResult<Vec<FieldInfo>> {
        self.state().described.as_ref().map_or(Ok(Vec::new()), |d| {
            result_fields(&d.stmt, &result_column_types(&d.types, declared), format)
        })
    }

    /// The parameter types of the statement [`Session::describe_prepare`] kept (see
    /// [`parameter_types`]); only the declared ones when it kept none.
    fn described_parameters(&self, declared: &[Option<Type>]) -> SqlResult<Vec<Type>> {
        let st = self.state();
        match st.described.as_ref() {
            Some(d) => parameter_types(&d.types, declared),
            None => Ok(declared
                .iter()
                .map(|t| t.clone().unwrap_or(Type::TEXT))
                .collect()),
        }
    }

    /// The statement a Describe prepared for this Execute, if it is this text on this connection.
    fn take_described(
        &self,
        conn: &PgConnection,
        sql: &str,
        declared: &[Option<Type>],
    ) -> Option<(turso_core::Statement, StatementTypes)> {
        let declared: Vec<_> = declared.iter().map(|ty| ty.as_ref().map(Type::oid)).collect();
        let mut st = self.state();
        match st.described.take() {
            Some(d) if d.sql == sql && d.declared == declared && Arc::ptr_eq(&d.conn, conn.inner()) => {
                Some((d.stmt, d.types))
            }
            _ => None,
        }
    }

    /// One statement through the engine. A statement that meets a lock another session holds waits
    /// for it, up to the server's lock wait, as a PostgreSQL row lock waits, instead of failing at
    /// once with 55P03 (wire review 1 item 9). A statement refused Busy after it changed rows is
    /// stepped again where it stopped ([`run_waiting`]), never dropped and run anew: a Busy can come
    /// at its commit, after it changed rows and returned them (wire review 5 item 2). One refused
    /// before it changed anything (at prepare, or at its first lock) is prepared and run anew. A
    /// stale snapshot (40001) is run again only outside a block, where nothing was written yet; in
    /// one the snapshot is the block's, and waiting cannot cure it. Not the engine's own busy
    /// timeout: its blocking loops (run_ignore_rows and the like) answer its Sleep with an IO step
    /// that returns at once on this platform, so they would spin a core for the whole wait. The
    /// statement's waits share one lock wait.
    fn engine_statement(
        &self,
        conn: &PgConnection,
        sql: &str,
        verb: TxVerb,
        portal: Option<&Portal<Parsed>>,
        format: &Format,
    ) -> Result<Response, StatementFailure> {
        let in_tx = !conn.inner().get_auto_commit();
        // A COMMIT or ROLLBACK is never stepped again or run anew: the engine's COMMIT has left
        // autocommit and armed its rollback before it meets a Busy, so stepped again it fails and
        // strands the transaction, and dropped it rolls the block back, after which run anew it
        // finds no transaction (wire review 8 item 1, review 7 item 3). Decided by the verb,
        // never by the statement's change count (review 8 item 13).
        let ends_block = matches!(verb, TxVerb::Commit | TxVerb::Rollback);
        let mut backoff = if ends_block {
            Backoff::never()
        } else {
            Backoff::new(self.lock_wait(), &self.interrupts)
        };
        // The last attempt, once the lock wait is spent, goes through the same rules as every
        // other: it returned unchecked, past the backstop (wire review 14 item 7).
        let mut last = false;
        loop {
            // sqlstate() gives 55P03 to LimboError::Busy alone and 40001 to BusySnapshot alone.
            match self.engine_statement_once(conn, sql, portal, format, &mut backoff) {
                // A COMMIT, as the verb reader or the parse the engine ran says, is settled as a
                // COMMIT is and never run again. So is the state backstop's case: a statement that
                // failed after the engine left the block it ran in, which, now that the reader and
                // the parse agree (wire review 14 item 6), is an error the engine answered by
                // rolling the whole transaction back. A sum() overflow is one, and there E, the
                // answer either way, is right; the backstop changes an answer only for a Busy that
                // also rolled the transaction back, which the retry would otherwise run anew
                // outside the block, and which no test reaches without a core failpoint (wire
                // review 9 item 7, review 17 item 19; 476798d89 had dropped f2804f119's state
                // check).
                Err(mut f)
                    if verb == TxVerb::Commit
                        || f.commits
                        || (in_tx && conn.inner().get_auto_commit()) =>
                {
                    f.info = commit_failed(conn, f.info);
                    return Err(f);
                }
                Err(f)
                    if !last
                        && f.rerunnable
                        && (f.info.code == "55P03" || (!in_tx && f.info.code == "40001")) =>
                {
                    match backoff.wait() {
                        Ok(true) => {}
                        Ok(false) => last = true,
                        Err(e) => {
                            return Err(StatementFailure {
                                info: engine_info(&e),
                                ..f
                            })
                        }
                    }
                }
                r => return r,
            }
        }
    }

    fn engine_statement_once(
        &self,
        conn: &PgConnection,
        sql: &str,
        portal: Option<&Portal<Parsed>>,
        format: &Format,
        backoff: &mut Backoff<'_>,
    ) -> Result<Response, StatementFailure> {
        let unprepared = |info: Box<ErrorInfo>| StatementFailure {
            prepared: false,
            rerunnable: true,
            commits: false,
            rolls_back: false,
            info,
        };
        let described = portal.and_then(|portal| self.take_described(conn, sql, &portal.statement.parameter_types));
        let (mut stmt, types) = match described {
            Some(kept) => kept,
            None => conn
                .prepare_typed_with_params(
                    sql,
                    &portal.map_or_else(Vec::new, |portal| {
                        portal
                            .statement
                            .parameter_types
                            .iter()
                            .map(|ty| ty.as_ref().map(Type::oid))
                            .collect()
                    }),
                )
                .map_err(|e| unprepared(engine_info(&e)))?,
        };
        self.shared.cleanup_dropped_schema_file(sql);
        // A statement that ends the transaction, as the translator read it, is never stepped
        // again, whatever the verb reader made of its text (wire review 13 item 1).
        if types.commits || types.rolls_back {
            *backoff = Backoff::never();
        }
        let query = types.new_table_keys.is_none()
            && !types.performed
            && stmt.num_columns() != 0
            && !is_pg_non_query(sql);
        let columns: Vec<Type> = if query {
            let declared = portal.map_or(&[][..], |p| p.statement.parameter_types.as_slice());
            let fixed = result_column_types(&types, declared);
            (0..stmt.num_columns())
                .map(|i| column_type(&stmt, &fixed, i))
                .collect()
        } else {
            Vec::new()
        };
        match portal {
            // In exec_bind_message's order: the value count (08P01, as PostgreSQL's Bind answers),
            // then the types the statement had when it first met the engine, or a refusal (see
            // [`Parsed`]), before any value is read by a type.
            Some(portal) => {
                let params = parameter_types(&types, &portal.statement.parameter_types)
                    .map_err(unprepared)?;
                check_bind_arity(portal.parameter_len(), &portal.statement.id, params.len())
                    .map_err(unprepared)?;
                let parsed = &portal.statement.statement;
                parsed.fix_params(&params).map_err(unprepared)?;
                let named: Vec<(String, Type)> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, ty)| (stmt.get_column_name(i).into_owned(), ty.clone()))
                    .collect();
                parsed.fix_columns(&named).map_err(unprepared)?;
                bind_portal_parameters(&mut stmt, portal, &params)
                    .map_err(|e| unprepared(wire_info(e)))?
            }
            // Nothing binds a parameter over the simple protocol, so a $n names none: 42P02, as
            // PostgreSQL answers, before the statement runs. It ran with the parameter unbound,
            // which the engine reads as NULL: `UPDATE t SET v = $1` nulled every row (wire review
            // 9 item 3).
            None => {
                if let Some(n) = types.used.first() {
                    return Err(unprepared(error(
                        "42P02",
                        format!("there is no parameter ${n}"),
                    )));
                }
            }
        }
        let r = if let Some(table) = types.new_table_keys.as_deref() {
            create_with_keys(conn, &mut stmt, sql, table, backoff)
        } else if !query {
            execute_non_query(&mut stmt, sql, backoff, portal.is_none())
        } else {
            // The column types are the statement's alone, so Describe (which runs nothing) and
            // this reply agree on both protocols (wire review 1 item 14).
            execute_query(
                &mut stmt,
                format,
                &columns,
                &conn.inner().current_schema(),
                backoff,
            )
            .map(|response| match response {
                // INSERT, UPDATE and DELETE with RETURNING complete with their own verb's tag, as
                // PostgreSQL answers them: the query path's default was SELECT n, and drivers read
                // the affected count from the tag (found by the PG 18 differential, 2026-10-15).
                Response::Query(mut results) => {
                    if let Some(tag) = returning_tag(sql) {
                        results.set_command_tag(tag);
                    }
                    Response::Query(results)
                }
                other => other,
            })
        };
        r.map_err(|e| StatementFailure {
            prepared: true,
            rerunnable: stmt.n_change() == 0 && !(types.commits || types.rolls_back),
            commits: types.commits,
            rolls_back: types.rolls_back,
            info: wire_info(e),
        })
    }

    // ---- branch functions ----

    fn branch(
        &self,
        st: &mut SessionState,
        conn: &PgConnection,
        call: &PgBranchCall,
        portal: Option<&Portal<Parsed>>,
        format: &Format,
    ) -> SqlResult<Response> {
        // A result format code other than 0 or 1, refused as PostgreSQL refuses it when it formats
        // the row, which every branch call returns: after what an engine statement checks first
        // (the function's name and arity, 42883; the result-format count, 08P01) and before the
        // call changes anything (wire review 13 item 10). It came first, so codes [0, 2] answered
        // 22023 where an engine SELECT answers 08P01 (wire review 17 item 9).
        let bad_code =
            |st: &SessionState| match portal.and_then(|p| st.bad_result_codes.get(&p.name)) {
                Some(code) => Err(error("22023", format!("unsupported format code: {code}"))),
                None => Ok(()),
            };
        let f = &*call.function;
        if f == "turso_branch_current" {
            arity(call, 0)?;
            result_format(format, 0, 1)?;
            bad_code(&*st)?;
            let name = st.branch.as_ref().map_or(TRUNK, |(n, _)| n.as_str());
            return branch_text(f, name, portal.is_some(), format);
        }
        if f == "turso_branch_stats" {
            arity(call, 0)?;
            result_format(format, 0, STATS_COLUMNS.len())?;
            bad_code(&*st)?;
            return stats_row(format, &self.shared.db);
        }
        // The allocations the server process has made, for the per-step allocation budget
        // (budget.py work); NULL in a build that counts none (no `alloc-count` feature).
        if f == "turso_branch_allocations" {
            arity(call, 0)?;
            result_format(format, 0, 1)?;
            bad_code(&*st)?;
            let n = counters::allocations().map(|n| n as i64);
            return one_row(f, Type::INT8, format, |e| e.encode_field(&n));
        }
        // The work budget's one reading (budget.py work): unix system calls, instructions and
        // allocations read together, so a window holds one reading statement, as its floor does
        // (read as two calls, the allocations' call fell inside the next window's system calls).
        if f == "turso_branch_work" {
            arity(call, 0)?;
            result_format(format, 0, WORK_COLUMNS.len())?;
            bad_code(&*st)?;
            return work_row(format, &self.shared.db);
        }
        if !matches!(
            f,
            "turso_branch_create" | "turso_branch_switch" | "turso_branch_delete"
        ) {
            return Err(error(
                "42883",
                format!("function {f} does not exist; the branch functions are turso_branch_create(name), turso_branch_switch(name), turso_branch_delete(name), turso_branch_current(), turso_branch_stats(), turso_branch_allocations() and turso_branch_work()"),
            ));
        }
        arity(call, 1)?;
        // The result-format list is checked before the call changes anything: a create refused
        // after it ran would leave its branch behind (wire review 9 item 2).
        result_format(format, 0, 1)?;
        bad_code(&*st)?;
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
                    Ok(id) => branch_int8(f, id.0 as i64, portal.is_some(), format),
                    Err(e) => Err(self.create_error(&name, &e)),
                }
            }
            "turso_branch_switch" => {
                let on = st.branch.as_ref().map_or(TRUNK, |(n, _)| n.as_str());
                if on == name {
                    // A no-op only while the session's branch is still the one the name names: a
                    // branch released behind the session and re-created under its name is another
                    // branch (wire review 1 item 6). The session already holds the name's claim.
                    let stale = st.branch.as_ref().is_some_and(|(_, c)| {
                        c.inner().branch_id() != self.shared.db.branch_named(&name).ok().flatten()
                    });
                    if stale {
                        let opened = self
                            .waiting(|| self.shared.db.connect_named(&name))
                            .map_err(|e| self.switch_error(&name, &e, "ERROR"))?;
                        let next = PgConnection::new(opened);
                        self.arm(next.inner());
                        next.adopt_session_of(conn);
                        st.branch = Some((name.as_ref().into(), next));
                    }
                    return branch_text(f, &name, portal.is_some(), format);
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
                    self.shared.claim_for(&name, "ERROR", Some(&self.interrupts))?;
                    let opened = match self.waiting(|| self.shared.db.connect_named(&name)) {
                        Ok(opened) => opened,
                        Err(e) => {
                            self.shared.release(&name);
                            return Err(self.switch_error(&name, &e, "ERROR"));
                        }
                    };
                    let next = PgConnection::new(opened);
                    self.arm(next.inner());
                    next.adopt_session_of(conn);
                    st.branch.replace((name.as_ref().into(), next))
                };
                // The branch left is free for a delete once its connection is closed: `run`
                // releases it after the statement's own handle on the connection goes.
                if let Some((left, left_conn)) = left {
                    drop(left_conn);
                    st.left = Some(left);
                }
                branch_text(f, &name, portal.is_some(), format)
            }
            _ if st.branch.as_ref().is_some_and(|(on, _)| *on == name) => Err(error(
                "55006",
                format!(
                    "cannot delete branch \"{name}\": this session is on it; switch to another \
                     branch first"
                ),
            )),
            _ => {
                self.shared.begin_delete_for(&name, Some(&self.interrupts))?;
                let dropped = self.waiting(|| self.shared.db.drop_branch(&name));
                self.shared.release(&name);
                match dropped {
                    Ok(_) => branch_text(f, &name, portal.is_some(), format),
                    Err(e) => Err(self.missing_or(&name, &e, "ERROR")),
                }
            }
        }
    }

    /// The engine call `f`, retried while a lock it needs is held by another session
    /// ([`LimboError::Busy`], or a snapshot a concurrent commit outdated), sleeping on SQLite's
    /// default busy schedule (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50 ms, then 100 ms) for up to
    /// the server's lock wait. Only this session's thread sleeps.
    fn waiting<T>(&self, f: impl FnMut() -> turso_core::Result<T>) -> turso_core::Result<T> {
        let _executing = turso_pg::phase::enter(turso_pg::phase::Phase::Execute);
        // A schema change committed under the call (SchemaUpdated, which the engine leaves to the
        // caller to retry: fork_trunk_registered; wire review 1 item 11) is waited out on the same
        // schedule and bound: retried 50 times back to back it outran nothing, and the next one
        // escaped as XX000 (wire review 5 item 9).
        self.retrying(f, |e| {
            matches!(
                e,
                LimboError::Busy | LimboError::BusySnapshot | LimboError::SchemaUpdated
            )
        })
    }

    /// `f` run again while `busy` says its failure was a lock another session holds, on the
    /// schedule of [`Session::waiting`], up to the server's lock wait; the attempt after the
    /// deadline is final. The session's cancel or statement_timeout ends the wait (57014).
    fn retrying<T>(
        &self,
        mut f: impl FnMut() -> turso_core::Result<T>,
        busy: impl Fn(&LimboError) -> bool,
    ) -> turso_core::Result<T> {
        let mut backoff = Backoff::new(self.lock_wait(), &self.interrupts);
        loop {
            match f() {
                Err(e) if busy(&e) => {
                    if !backoff.wait()? {
                        return f();
                    }
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

impl Session {
    /// The session's end: its engine connections close (a statement Describe kept holds one, so
    /// it goes first) and its branch is released in the in-use map. Run when the client
    /// terminates or goes, before its socket closes (serve_session), and again, as a no-op, when
    /// the session drops.
    fn end(&self) {
        let mut st = self.state();
        st.described = None;
        st.implicit = false;
        st.trunk = None;
        if let Some((name, conn)) = st.branch.take() {
            drop(conn);
            self.shared.release(&name);
        }
    }
}

impl Drop for Session {
    /// The session's branch is free once its connection is closed, and a CancelRequest can no
    /// longer name it.
    fn drop(&mut self) {
        self.end();
        self.shared
            .cancels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

impl Shared {
    /// After a DROP SCHEMA query succeeds, delete the schema's database file.
    /// Uses simple string matching to detect DROP SCHEMA statements.
    fn cleanup_dropped_schema_file(&self, query: &str) {
        if self.db_file == ":memory:" {
            return;
        }
        // Simple detection: look for DROP SCHEMA pattern. Every other statement is turned away
        // before the lowered copy, so a write pays no allocation here (L5).
        let head = query.trim_start().as_bytes();
        if head.len() < 4 || !head[..4].eq_ignore_ascii_case(b"drop") {
            return;
        }
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

/// What a statement does to the transaction block, read by the verb's WHOLE grammar (PostgreSQL's
/// gram.y), after any leading whitespace, comments and empty statements (`;COMMIT` is COMMIT), and
/// with any run of `;` after it (`COMMIT;;` is COMMIT and an empty statement; [`tx_words`]):
/// - Begin: `BEGIN [WORK | TRANSACTION] [modes]`, `START TRANSACTION [modes]`;
/// - Commit: `COMMIT | END [WORK | TRANSACTION] [AND NO CHAIN]`;
/// - Rollback: `ROLLBACK | ABORT [WORK | TRANSACTION] [AND NO CHAIN]`;
/// - RollbackTo: `ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] name`;
/// - Release: `RELEASE [SAVEPOINT] name`; Savepoint: `SAVEPOINT name`;
/// - Other: anything else, malformed spellings of the above included, which go to the engine and
///   are syntax errors there. Read by two words, `COMMIT garbage` was answered as COMMIT and
///   `/* c */ ROLLBACK` was not a ROLLBACK (wire review 6 item 4). `AND CHAIN` is Other: the
///   engine has no chained transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxVerb {
    Begin,
    Commit,
    Rollback,
    /// COMMIT AND CHAIN or ROLLBACK AND CHAIN (`commit`: which), which the server does not run.
    Chain {
        commit: bool,
    },
    RollbackTo,
    Release,
    Savepoint,
    Other,
}

impl TxVerb {
    fn of(sql: &str) -> Self {
        let Some(w) = tx_words(sql) else {
            return TxVerb::Other;
        };
        let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
        let work = |w: &[&str]| -> usize {
            usize::from(
                w.first()
                    .is_some_and(|x| is(x, "WORK") || is(x, "TRANSACTION")),
            )
        };
        let no_chain = |w: &[&str]| -> bool {
            matches!(w, [])
                || (w.len() == 3 && is(w[0], "AND") && is(w[1], "NO") && is(w[2], "CHAIN"))
        };
        let name = |w: &[&str]| -> bool { w.len() == 1 && !w[0].is_empty() && w[0] != "," };
        let chain = |w: &[&str]| -> bool { w.len() == 2 && is(w[0], "AND") && is(w[1], "CHAIN") };
        match w.as_slice() {
            [first, rest @ ..] if is(first, "BEGIN") => {
                let rest = &rest[work(rest)..];
                if tx_modes(rest) {
                    TxVerb::Begin
                } else {
                    TxVerb::Other
                }
            }
            [first, second, rest @ ..] if is(first, "START") && is(second, "TRANSACTION") => {
                if tx_modes(rest) {
                    TxVerb::Begin
                } else {
                    TxVerb::Other
                }
            }
            [first, rest @ ..] if is(first, "COMMIT") || is(first, "END") => {
                let rest = &rest[work(rest)..];
                if no_chain(rest) {
                    TxVerb::Commit
                } else if chain(rest) {
                    TxVerb::Chain { commit: true }
                } else {
                    TxVerb::Other
                }
            }
            [first, rest @ ..] if is(first, "ROLLBACK") || is(first, "ABORT") => {
                let rest = &rest[work(rest)..];
                if no_chain(rest) {
                    TxVerb::Rollback
                } else if chain(rest) {
                    TxVerb::Chain { commit: false }
                } else if is(first, "ROLLBACK") && rest.first().is_some_and(|x| is(x, "TO")) {
                    let rest = &rest[1..];
                    let rest = savepoint_keyword(rest);
                    if name(rest) {
                        TxVerb::RollbackTo
                    } else {
                        TxVerb::Other
                    }
                } else {
                    TxVerb::Other
                }
            }
            [first, rest @ ..] if is(first, "RELEASE") => {
                let rest = savepoint_keyword(rest);
                if name(rest) {
                    TxVerb::Release
                } else {
                    TxVerb::Other
                }
            }
            [first, rest @ ..] if is(first, "SAVEPOINT") && name(rest) => TxVerb::Savepoint,
            _ => TxVerb::Other,
        }
    }

    /// The savepoint verb a statement starts with, read by its leading keywords alone, whatever
    /// follows: `SAVEPOINT x`, `RELEASE x`, `ROLLBACK [WORK | TRANSACTION] TO x`, x any non-blank
    /// text. For the refusal outside a block (25P01), where a name the verb reader cannot hold as
    /// one word (`U&"a"`, one after a \v) made the statement Other, and the engine then opened a
    /// transaction with no BEGIN for SAVEPOINT, or answered XX000 (wire review 14 item 15).
    /// Blind spot: a statement PostgreSQL refuses as a syntax error after those keywords
    /// (`SAVEPOINT 1x`) is answered 25P01 here, a refusal either way.
    fn of_leading_keywords(sql: &str) -> Option<Self> {
        // The next word of letters at `*i`, then the blanks and comments after it, never a `;`
        // (it would end the statement). A fn, not a closure, so its output borrows `sql` alone.
        fn word<'s>(sql: &'s str, i: &mut usize) -> Option<&'s str> {
            let b = sql.as_bytes();
            let start = *i;
            while *i < b.len() && b[*i].is_ascii_alphabetic() {
                *i += 1;
            }
            let w = &sql[start..*i];
            loop {
                while *i < b.len() && pg_space(b[*i]) {
                    *i += 1;
                }
                match sql_comment(&b[*i..]) {
                    Some(len) => *i += len?,
                    None => break,
                }
            }
            (!w.is_empty()).then_some(w)
        }
        let b = sql.as_bytes();
        let mut i = skip_blank(b, 0)?;
        let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
        let verb = match word(sql, &mut i)? {
            w if is(w, "SAVEPOINT") => TxVerb::Savepoint,
            w if is(w, "RELEASE") => TxVerb::Release,
            w if is(w, "ROLLBACK") => {
                let mut next = word(sql, &mut i)?;
                if is(next, "WORK") || is(next, "TRANSACTION") {
                    next = word(sql, &mut i)?;
                }
                if !is(next, "TO") {
                    return None;
                }
                TxVerb::RollbackTo
            }
            _ => return None,
        };
        // A name follows, whatever its spelling.
        (i < b.len() && b[i] != b';').then_some(verb)
    }

    /// The statements a failed block still accepts.
    fn ends_block(self) -> bool {
        matches!(self, TxVerb::Commit | TxVerb::Rollback | TxVerb::RollbackTo)
    }
}

/// The words after `ROLLBACK ... TO` or `RELEASE`, with an optional SAVEPOINT keyword dropped: the
/// keyword is dropped only when a name follows it, so `ROLLBACK TO SAVEPOINT` and `RELEASE
/// SAVEPOINT` name a savepoint called savepoint, as PostgreSQL's grammar reads them (TO ColId,
/// RELEASE ColId; recorded on PostgreSQL 18.6, wire review 14 item 21). The reader dropped it
/// always, found no name and read Other, so a failed block refused the statement 25P02.
fn savepoint_keyword<'a, 's>(rest: &'a [&'s str]) -> &'a [&'s str] {
    match rest {
        [first, name, ..] if first.eq_ignore_ascii_case("SAVEPOINT") && !name.is_empty() => {
            &rest[1..]
        }
        _ => rest,
    }
}

/// The words of a statement for [`TxVerb::of`], each a slice of `sql`: a comment is whitespace
/// wherever it stands (`--` to the line's end, `/* */` nested), as PostgreSQL's lexer reads one, so
/// it also ends a word; a `"quoted"` name is one word, `,` a word of its own, and `$` continues a
/// word after its first byte, as in an identifier; a `;` ends the statement, after which only
/// whitespace, comments and more `;` (empty statements) may follow, as whitespace, comments and
/// empty statements may come before the verb. None for a statement that does not start with a
/// transaction verb (one word's scan, no allocation), and for text the verbs' grammar cannot hold
/// (a `'` string, a word starting with `$`, a second statement, an unterminated comment), which is
/// Other. A comment after the verb made it Other, so `COMMIT -- c` skipped the failed-COMMIT rule
/// and `ROLLBACK /* c */` could never end a failed block (wire review 9 item 7, review 11 item 7).
fn tx_words(sql: &str) -> Option<Vec<&str>> {
    let b = sql.as_bytes();
    // Whitespace, comments and empty statements (`;COMMIT`) before the verb (wire review 14 item 6).
    let mut i = skip_blank(b, 0)?;
    let first_end = b[i..]
        .iter()
        .position(|c| !c.is_ascii_alphabetic())
        .map_or(b.len(), |p| i + p);
    let first = &sql[i..first_end];
    if ![
        "BEGIN",
        "START",
        "COMMIT",
        "END",
        "ROLLBACK",
        "ABORT",
        "RELEASE",
        "SAVEPOINT",
    ]
    .iter()
    .any(|k| first.eq_ignore_ascii_case(k))
    {
        return None;
    }
    let mut words = Vec::new();
    let mut ended = false;
    while i < b.len() {
        if let Some(len) = sql_comment(&b[i..]) {
            i += len?;
            continue;
        }
        match b[i] {
            c if pg_space(c) => i += 1,
            // A run of `;` ends the statement: `COMMIT;;` is COMMIT and an empty statement.
            b';' => {
                ended = true;
                i += 1;
            }
            _ if ended => return None,
            b',' => {
                words.push(&sql[i..i + 1]);
                i += 1;
            }
            b'"' => {
                let start = i;
                i += 1;
                loop {
                    match b.get(i) {
                        Some(b'"') if b.get(i + 1) == Some(&b'"') => i += 2,
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(_) => i += 1,
                        None => return None,
                    }
                }
                words.push(&sql[start..i]);
            }
            b'\'' | b'$' => return None,
            _ => {
                let start = i;
                while i < b.len()
                    && !pg_space(b[i])
                    && !matches!(b[i], b',' | b'"' | b'\'' | b';')
                    && sql_comment(&b[i..]).is_none()
                {
                    i += 1;
                }
                words.push(&sql[start..i]);
            }
        }
    }
    Some(words)
}

/// Whether a BEGIN or START TRANSACTION ([`TxVerb::Begin`]) asks for READ ONLY among its modes.
fn begins_read_only(sql: &str) -> bool {
    tx_words(sql).is_some_and(|w| {
        w.windows(2)
            .any(|p| p[0].eq_ignore_ascii_case("READ") && p[1].eq_ignore_ascii_case("ONLY"))
    })
}

/// Whether `w` is a list of transaction modes, as BEGIN and START TRANSACTION take them:
/// `ISOLATION LEVEL {SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED}`,
/// `READ WRITE`, `READ ONLY`, `[NOT] DEFERRABLE`, separated by commas or spaces.
fn tx_modes(mut w: &[&str]) -> bool {
    let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
    while !w.is_empty() {
        let taken = match w {
            [a, b, c, ..] if is(a, "ISOLATION") && is(b, "LEVEL") && is(c, "SERIALIZABLE") => 3,
            [a, b, c, d, ..]
                if is(a, "ISOLATION")
                    && is(b, "LEVEL")
                    && ((is(c, "REPEATABLE") && is(d, "READ"))
                        || (is(c, "READ") && (is(d, "COMMITTED") || is(d, "UNCOMMITTED")))) =>
            {
                4
            }
            [a, b, ..] if is(a, "READ") && (is(b, "WRITE") || is(b, "ONLY")) => 2,
            [a, b, ..] if is(a, "NOT") && is(b, "DEFERRABLE") => 2,
            [a, ..] if is(a, "DEFERRABLE") => 1,
            _ => return false,
        };
        w = &w[taken..];
        // A comma separates modes; one may not end the list.
        if let Some([",", rest @ ..]) = Some(w) {
            if rest.is_empty() {
                return false;
            }
            w = rest;
        }
    }
    true
}

/// A broken connection's answer (an undo failed; turso_pg's PgConnection::is_broken): FATAL 08006,
/// which serve_session ends the session on.
fn broken_fatal(mut info: Box<ErrorInfo>) -> Box<ErrorInfo> {
    info.code = "08006".to_string();
    info.severity = "FATAL".to_string();
    info
}

/// What a failed COMMIT tells the client, once the engine's transaction is settled: a COMMIT the
/// engine refused for a lock (Busy at the trunk's commit) ended the block, rolled back, which is a
/// serialization failure the client retries (40001; PostgreSQL's answer to a commit that cannot
/// serialize). Any other failure (a deferred foreign key, 23503) is its own. A transaction the
/// engine still holds in autocommit cannot be ended on this connection (its COMMIT left autocommit
/// before failing, and a ROLLBACK there finds no block): the connection is broken, and the session
/// ends (FATAL 08006) rather than answer statements that would join the stranded transaction.
/// The caller rolls back a block the engine still has open. Blind spot: only a WRITE transaction
/// is visible (Connection::is_in_write_tx); a stranded read transaction is not.
fn commit_failed(conn: &PgConnection, mut info: Box<ErrorInfo>) -> Box<ErrorInfo> {
    if conn.inner().get_auto_commit() && conn.inner().is_in_write_tx() {
        info.code = "08006".to_string();
        info.severity = "FATAL".to_string();
        info.message = format!(
            "the transaction could not be ended after its COMMIT failed ({}); the connection is \
             closed",
            info.message
        );
        return info;
    }
    if matches!(info.code.as_str(), "55P03" | "40001") {
        info.code = "40001".to_string();
        info.message = format!(
            "could not serialize access: the transaction was rolled back ({})",
            info.message
        );
    }
    info
}

/// The schema a CREATE SCHEMA or DROP SCHEMA names (`[IF [NOT] EXISTS] name`, quotes dropped), if
/// `sql` is one. In server mode only public passes (wire review 1 item 16): each session attaches
/// the schema files that exist when it opens, so a schema created or dropped while others run
/// leaves sessions that disagree about it, and a branch would not cover it.
fn schema_ddl(sql: &str) -> Option<&str> {
    let mut words = sql
        .split(|c: char| c.is_ascii_whitespace() || c == ';')
        .filter(|w| !w.is_empty());
    let verb = words.next()?;
    if !(verb.eq_ignore_ascii_case("CREATE") || verb.eq_ignore_ascii_case("DROP")) {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("SCHEMA") {
        return None;
    }
    let mut name = words.next()?;
    while ["IF", "NOT", "EXISTS"]
        .iter()
        .any(|k| name.eq_ignore_ascii_case(k))
    {
        name = words.next()?;
    }
    Some(name.trim_matches('"'))
}

/// Whether `sql` is CHECKPOINT alone, which the server runs itself ([`Session::checkpoint`]), read
/// as PostgreSQL's lexer reads it: whitespace (a vertical tab included), comments and empty
/// statements around it. A comment was a second word, so `CHECKPOINT -- x` went to the engine as
/// text (wire review 14 item 6).
fn is_checkpoint(sql: &str) -> bool {
    let b = sql.as_bytes();
    let Some(start) = skip_blank(b, 0) else {
        return false;
    };
    let end = b[start..]
        .iter()
        .position(|c| !c.is_ascii_alphabetic())
        .map_or(b.len(), |p| start + p);
    sql[start..end].eq_ignore_ascii_case("CHECKPOINT") && skip_blank(b, end) == Some(b.len())
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
fn text_arg<'a>(
    arg: &'a PgBranchArg,
    portal: Option<&Portal<Parsed>>,
) -> SqlResult<std::borrow::Cow<'a, str>> {
    match arg {
        // Borrowed: a literal name costs the call no copy (L5).
        PgBranchArg::Text(s) => Ok(std::borrow::Cow::Borrowed(s.as_str())),
        PgBranchArg::Null => Err(error("22004", "a branch name must not be null".to_string())),
        PgBranchArg::Bool(_) => Err(error("42804", "a branch name is text".to_string())),
        PgBranchArg::Param(n) => {
            // Over the simple protocol nothing binds one (wire review 9 item 3).
            let portal =
                portal.ok_or_else(|| error("42P02", format!("there is no parameter ${n}")))?;
            // A parameter declared as another type is not a name, whatever its bytes spell. A text
            // or varchar value (or one of unspecified type, which PostgreSQL resolves to the
            // function's text) is its bytes in either format: textrecv reads binary text as is.
            if let Some(Some(ty)) = portal.statement.parameter_types.get(n - 1) {
                if ![Type::TEXT, Type::VARCHAR, Type::UNKNOWN].contains(ty) {
                    return Err(error(
                        "42804",
                        format!("a branch name is text, and parameter ${n} is {ty}"),
                    ));
                }
            }
            let bytes = portal
                .parameters
                .get(n - 1)
                .ok_or_else(|| error("08P01", format!("parameter ${n} is not bound")))?;
            let Some(bytes) = bytes else {
                return Err(error("22004", "a branch name must not be null".to_string()));
            };
            String::from_utf8(bytes.to_vec())
                .map(std::borrow::Cow::Owned)
                .map_err(|e| error("22021", format!("invalid UTF-8 in parameter ${n}: {e}")))
        }
    }
}

fn one_row(
    f: &str,
    pg_type: Type,
    format: &Format,
    encode: impl FnOnce(&mut DataRowEncoder) -> PgWireResult<()>,
) -> SqlResult<Response> {
    let header = Arc::new(vec![FieldInfo::new(
        f.to_string(),
        None,
        None,
        pg_type,
        result_format(format, 0, 1)?,
    )]);
    let mut encoder = DataRowEncoder::new(header.clone());
    let row = encode(&mut encoder).and_then(|()| encoder.finish());
    Ok(Response::Query(QueryResponse::new(
        header,
        stream::iter(vec![row]),
    )))
}

fn one_text(f: &str, value: &str, format: &Format) -> SqlResult<Response> {
    one_row(f, Type::TEXT, format, |e| e.encode_field(&value))
}

fn one_int8(f: &str, value: i64, format: &Format) -> SqlResult<Response> {
    one_row(f, Type::INT8, format, |e| e.encode_field(&value))
}

/// A branch call's one-row, one-column text reply to a simple Query, encoded here into a fixed
/// buffer: RowDescription (the column named `f`, of `pg_type`, text format), the DataRow holding
/// `value`'s text, and CommandComplete `SELECT 1`, byte for byte what pgwire encodes for
/// [`one_row`]'s response (inline_replies_are_the_messages_pgwire_encodes), with no heap
/// allocation (L5: they cost the wire 7 of its 20 allocations a call). None when they do not fit.
fn inline_one_row(f: &str, pg_type: &Type, value: &[u8]) -> Option<pgwire::messages::Inline> {
    let mut m = pgwire::messages::Inline::default();
    let t_len = 4 + 2 + f.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    let d_len = 4 + 2 + 4 + value.len();
    let tag = b"SELECT 1\0";
    let ok = m.push(b"T")
        && m.push(&(i32::try_from(t_len).ok()?).to_be_bytes())
        && m.push(&1i16.to_be_bytes())
        && m.push(f.as_bytes())
        && m.push(&[0])
        && m.push(&0i32.to_be_bytes())
        && m.push(&0i16.to_be_bytes())
        && m.push(&pg_type.oid().to_be_bytes())
        // pgwire's FieldInfo sends type size and modifier 0 (its From<&FieldInfo>), and so must this.
        && m.push(&0i16.to_be_bytes())
        && m.push(&0i32.to_be_bytes())
        && m.push(&0i16.to_be_bytes())
        && m.push(b"D")
        && m.push(&(i32::try_from(d_len).ok()?).to_be_bytes())
        && m.push(&1i16.to_be_bytes())
        && m.push(&(i32::try_from(value.len()).ok()?).to_be_bytes())
        && m.push(value)
        && m.push(b"C")
        && m.push(&((4 + tag.len()) as i32).to_be_bytes())
        && m.push(tag);
    ok.then_some(m)
}

/// A branch call's text reply: inline to a simple Query (`portal` None, text), else pgwire's.
fn branch_text(f: &str, value: &str, portal_or_described: bool, format: &Format) -> SqlResult<Response> {
    if !portal_or_described && matches!(format, Format::UnifiedText) {
        if let Some(m) = inline_one_row(f, &Type::TEXT, value.as_bytes()) {
            return Ok(Response::Inline(m));
        }
    }
    one_text(f, value, format)
}

/// [`branch_text`] for a branch id (int8).
fn branch_int8(f: &str, value: i64, portal_or_described: bool, format: &Format) -> SqlResult<Response> {
    if !portal_or_described && matches!(format, Format::UnifiedText) {
        let mut digits = [0u8; 20];
        let text = int_text(value, &mut digits);
        if let Some(m) = inline_one_row(f, &Type::INT8, text) {
            return Ok(Response::Inline(m));
        }
    }
    one_int8(f, value, format)
}

/// `value`'s decimal text in `buf`, no allocation.
fn int_text(value: i64, buf: &mut [u8; 20]) -> &[u8] {
    let mut n = value.unsigned_abs();
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if value < 0 {
        at -= 1;
        buf[at] = b'-';
    }
    &buf[at..]
}

#[cfg(not(feature = "phase-count"))]
const WORK_COLUMNS: [&str; 6] = [
    "unix_syscalls",
    "instructions",
    "allocations",
    "thread_instructions",
    "confirms_written",
    "thread_allocations",
];
/// A phase build (feature phase-count, a diagnostic: budget.py work --phases) adds this session
/// thread's instructions per statement phase (turso_pg::phase::NAMES), so the default build's row
/// is unchanged.
#[cfg(feature = "phase-count")]
const WORK_COLUMNS: [&str; 14] = [
    "unix_syscalls",
    "instructions",
    "allocations",
    "thread_instructions",
    "confirms_written",
    "thread_allocations",
    "phase_session",
    "phase_read",
    "phase_parse",
    "phase_translate",
    "phase_plan",
    "phase_execute",
    "phase_encode",
    "phase_write",
];

/// turso_branch_work(): this process's system calls and instructions (counters::process_counters),
/// allocations (counters::allocations, NULL without the alloc-count feature), this session
/// thread's own instructions (counters::thread_counters) and the branch store's confirmation words
/// written (its background confirm writer's work, Database::branch_confirm_counts), read at once.
fn work_row(format: &Format, db: &Database) -> SqlResult<Response> {
    let fields = WORK_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, name)| {
            Ok(FieldInfo::new(
                name.to_string(),
                None,
                None,
                Type::INT8,
                result_format(format, i, WORK_COLUMNS.len())?,
            ))
        })
        .collect::<SqlResult<Vec<_>>>()?;
    let header = Arc::new(fields);
    let mut encoder = DataRowEncoder::new(header.clone());
    let counters = counters::process_counters();
    let allocations = counters::allocations();
    let row = encoder
        .encode_field(&counters.map(|c| c.unix_syscalls as i64))
        .and_then(|()| encoder.encode_field(&counters.map(|c| c.instructions as i64)))
        .and_then(|()| encoder.encode_field(&allocations.map(|n| n as i64)))
        .and_then(|()| encoder.encode_field(&counters::thread_counters().map(|c| c.0 as i64)))
        .and_then(|()| encoder.encode_field(&(db.branch_confirm_counts()[0] as i64)))
        // This session thread's own allocations (counters::thread_allocations; 0 without the
        // alloc-count feature): the work budget judges every counter by the operation's own thread.
        .and_then(|()| encoder.encode_field(&(counters::thread_allocations() as i64)));
    #[cfg(feature = "phase-count")]
    let row = row.and_then(|()| {
        let phases = turso_pg::phase::totals().unwrap_or_default();
        phases
            .iter()
            .try_for_each(|n| encoder.encode_field(&(*n as i64)))
    });
    let row = row.and_then(|()| encoder.finish());
    Ok(Response::Query(QueryResponse::new(header, stream::iter(vec![row]))))
}

const STATS_COLUMNS: [&str; 10] = [
    "unix_syscalls",
    "mach_syscalls",
    "instructions",
    "cycles",
    "checkpoints_skipped",
    "store_checkpoints",
    "hardcap_upgrades",
    "header_raise_exclusive",
    "inline_compaction_syncs",
    "compaction_waits",
];

/// turso_branch_stats(): the server process's counters ([`counters::process_counters`]), read as
/// the call runs, NULLs where the platform does not count them; then the server's own count of
/// skipped CHECKPOINTs ([`CHECKPOINTS_SKIPPED`]); then the branch store's checkpoints installed
/// since the database opened (its own count, the first of `branch_checkpoint_counters`; 0 for a
/// store that is not a catalog store), so a client can count the store checkpoints in a timed
/// window (PREREG section 5 M2; wire review 14 item 13); then the store's four counted exceptions
/// to "no create, connect, first write, delete or trunk commit waits on background work"
/// (Database::branch_exception_counters), so every run prints them and a window in which one
/// moved is labelled and not credited (DECISIONS 2026-10-15T05:16:51Z).
fn stats_row(format: &Format, db: &Database) -> SqlResult<Response> {
    let header = Arc::new(stats_fields(format)?);
    let mut encoder = DataRowEncoder::new(header.clone());
    let values = counters::process_counters()
        .map(|c| [c.unix_syscalls, c.mach_syscalls, c.instructions, c.cycles].map(|v| v as i64));
    let skipped = CHECKPOINTS_SKIPPED.load(Ordering::Relaxed) as i64;
    let store_checkpoints = db.branch_checkpoint_counters()[0] as i64;
    let exceptions = db.branch_exception_counters().map(|v| v as i64);
    let row = (0..4)
        .try_for_each(|i| encoder.encode_field(&values.map(|v| v[i])))
        .and_then(|()| encoder.encode_field(&skipped))
        .and_then(|()| encoder.encode_field(&store_checkpoints))
        .and_then(|()| {
            exceptions
                .iter()
                .try_for_each(|v| encoder.encode_field(v))
        })
        .and_then(|()| encoder.finish());
    Ok(Response::Query(QueryResponse::new(
        header,
        stream::iter(vec![row]),
    )))
}

fn stats_fields(format: &Format) -> SqlResult<Vec<FieldInfo>> {
    STATS_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, name)| {
            Ok(FieldInfo::new(
                name.to_string(),
                None,
                None,
                Type::INT8,
                result_format(format, i, STATS_COLUMNS.len())?,
            ))
        })
        .collect()
}

/// The format of result column `i` of `columns` under a Bind's result-format codes: none (every
/// column text), one (for every column), or one per column. Any other count is a protocol
/// violation, refused in PostgreSQL's words (its PortalSetResultFormat), where pgwire's
/// `Format::format_for` indexed the list unchecked: two codes for three columns panicked the
/// session, and under the release build's panic=abort ended every session (wire review 9 item 2,
/// review 10 item 1). PostgreSQL refuses at Bind; here the check is made where the column count is
/// first known, at Describe of the portal or at Execute, since an engine statement's columns are
/// known only once it is prepared. A statement returning no rows builds no columns and ignores the
/// list, as in PostgreSQL.
fn result_format(format: &Format, i: usize, columns: usize) -> SqlResult<FieldFormat> {
    match format {
        Format::Individual(codes) => match codes.get(i) {
            Some(code) if codes.len() == columns => Ok(FieldFormat::from(*code)),
            _ => Err(error(
                "08P01",
                format!(
                    "bind message has {} result formats but query has {columns} columns",
                    codes.len()
                ),
            )),
        },
        unified => Ok(unified.format_for(i)),
    }
}

/// A branch call's parameters as a parse types them: each `$n` it holds is text (a branch name),
/// so Describe answers what [`parameter_types`] answers for any statement, a gap below the highest
/// $n included (42P18). Describe sized the list by the highest $n with no limit, so one client's
/// `$18446744073709551615` panicked the server (wire review 9 item 1); a call holds only $n in
/// 1..=MAX_PARAMETER ([`branch_call`]).
fn branch_call_types(call: &PgBranchCall) -> StatementTypes {
    let mut used: Vec<u32> = call
        .args
        .iter()
        .filter_map(|a| match a {
            PgBranchArg::Param(n) => Some(*n as u32),
            _ => None,
        })
        .collect();
    used.sort_unstable();
    used.dedup();
    StatementTypes {
        params: used.iter().map(|n| (*n, Type::TEXT.oid())).collect(),
        used,
        ..StatementTypes::default()
    }
}

/// The row a branch call returns, for Describe.
fn branch_call_fields(call: &PgBranchCall, format: &Format) -> SqlResult<Vec<FieldInfo>> {
    if call.function == "turso_branch_stats" {
        return stats_fields(format);
    }
    if call.function == "turso_branch_work" {
        return WORK_COLUMNS
            .iter()
            .enumerate()
            .map(|(i, name)| {
                Ok(FieldInfo::new(
                    name.to_string(),
                    None,
                    None,
                    Type::INT8,
                    result_format(format, i, WORK_COLUMNS.len())?,
                ))
            })
            .collect();
    }
    let pg_type = if matches!(
        &*call.function,
        "turso_branch_create" | "turso_branch_allocations"
    ) {
        Type::INT8
    } else {
        Type::TEXT
    };
    Ok(vec![FieldInfo::new(
        call.function.to_string(),
        None,
        None,
        pg_type,
        result_format(format, 0, 1)?,
    )])
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

/// PostgreSQL's answer to a portal that does not exist (34000, invalid_cursor_name; pgwire's own
/// PortalNotFound is 26000, a statement's code).
fn portal_not_found(name: &str) -> Box<ErrorInfo> {
    let name = if name == DEFAULT_NAME { "" } else { name };
    error("34000", format!("portal \"{name}\" does not exist"))
}

/// A statement with nothing in it: only PostgreSQL's whitespace, semicolons and comments, the text
/// libpg_query parses to no statement. PostgreSQL's empty query, answered with
/// EmptyQueryResponse on both protocols. Read by bytes, with no parse; an unterminated comment is
/// not blank (the parser refuses it). Tested as whitespace or a lone `;`, `;;`, `-- c` and `/* c
/// */` reached begin_implicit and the translator's "No statements found", which rolled a
/// pipeline's earlier writes back (wire review 15 item 6).
fn is_blank(sql: &str) -> bool {
    skip_blank(sql.as_bytes(), 0) == Some(sql.len())
}

/// A WARNING notice, as PostgreSQL sends for a transaction verb that changes nothing.
fn warning(code: &str, message: &str) -> Box<ErrorInfo> {
    Box::new(ErrorInfo::new(
        "WARNING".to_string(),
        code.to_string(),
        message.to_string(),
    ))
}

fn aborted_error() -> Box<ErrorInfo> {
    error(
        "25P02",
        "current transaction is aborted, commands ignored until end of transaction block"
            .to_string(),
    )
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
    if let LimboError::Interrupt = e {
        let message = match INTERRUPT_CAUSE.with(|c| c.get()) {
            2 => "canceling statement due to statement timeout",
            _ => "canceling statement due to user request",
        };
        return error(sqlstate(e), message.to_string());
    }
    error(sqlstate(e), e.to_string())
}

/// PostgreSQL's SQLSTATE for the condition selected by the error's producer. Diagnostic text
/// has no role here: rewording it preserves the code and the engine's error handling.
fn sqlstate(e: &LimboError) -> &'static str {
    use SqlErrorKind::*;
    match e.sql_error_kind() {
        SyntaxError => "42601",
        FeatureNotSupported => "0A000",
        InvalidTextRepresentation => "22P02",
        NumericValueOutOfRange => "22003",
        DivisionByZero => "22012",
        IntegrityConstraintViolation => "23000",
        UniqueViolation => "23505",
        NotNullViolation => "23502",
        CheckViolation => "23514",
        ForeignKeyViolation => "23503",
        InvalidColumnReference => "42P10",
        DatetimeFieldOverflow => "22008",
        InvalidParameterValue => "22023",
        UndefinedTable => "42P01",
        InvalidForeignKey => "42830",
        UndefinedColumn => "42703",
        UndefinedObject => "42704",
        UndefinedParameter => "42P02",
        InvalidSavepointSpecification => "3B001",
        AmbiguousColumn => "42702",
        AmbiguousParameter => "42P08",
        DatatypeMismatch => "42804",
        UndefinedFunction => "42883",
        DuplicateAlias => "42712",
        StatementTooComplex => "54001",
        InternalError => "XX000",
        SerializationFailure => "40001",
        LockNotAvailable => "55P03",
        ReadOnlyTransaction => "25006",
        DiskFull => "53100",
        QueryCanceled => "57014",
    }
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
    /// The settings SHOW answers, from turso_pg's one table, over pgwire's defaults, which
    /// announced DateStyle `ISO, YMD`, TimeZone `Etc/UTC` and search_path `public` (wire review 18
    /// item 19). bytea_output and extra_float_digits are not reported settings in PostgreSQL.
    fn server_parameters(&self) -> DefaultServerParameterProvider {
        let mut provider = DefaultServerParameterProvider::default();
        for &(name, value) in turso_pg::FIXED_SETTINGS {
            match name {
                "client_encoding" => provider.client_encoding = Some(value.to_string()),
                "standard_conforming_strings" => {
                    provider.standard_conforming_strings = value == "on"
                }
                "DateStyle" => provider.date_style = value.to_string(),
                "IntervalStyle" => provider.interval_style = value.to_string(),
                "TimeZone" => provider.time_zone = value.to_string(),
                _ => {}
            }
        }
        provider.search_path = turso_pg::DEFAULT_SEARCH_PATH.to_string();
        provider
    }

    fn backend_key(&self) -> Option<(i32, pgwire::messages::startup::SecretKey)> {
        Some((self.key.0, pgwire::messages::startup::SecretKey::I32(self.key.1)))
    }

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
        let responses = if is_blank(&query.query) {
            // An empty Query ends an unsynced pipeline's implicit block as any Query does
            // (committing it); it left the block open (wire review 16 item 6).
            let mut responses = Replies::new();
            responses.push((Vec::new(), Response::EmptyQuery));
            if let Err(e) = self.end_implicit() {
                responses.push((Vec::new(), Response::Error(e)));
            }
            responses
        } else {
            self.simple_with_notices(&query.query)
        };
        let encoding = turso_pg::phase::enter(turso_pg::phase::Phase::Encode);
        for (notices, response) in responses {
            for notice in notices {
                client
                    .feed(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                        *notice,
                    )))
                    .await?;
            }
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
                // A FATAL error ends the session: pgwire sends it, flushes what is fed before it
                // and closes the socket (process_error).
                Response::Error(e) if e.severity == "FATAL" => {
                    return Err(PgWireError::UserError(e));
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
                // A branch call's reply, encoded by the fast path (inline_one_row).
                Response::Inline(m) => client.feed(PgWireBackendMessage::Inline(m)).await?,
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
        // The wire budget's fire-check plant (feature budget-plant-split-reply, never in a default
        // build): the results in one write and ReadyForQuery in a second, the double reply write
        // its one-write-per-statement check must catch (gap review item 2, wire review 5 item 3).
        #[cfg(feature = "budget-plant-split-reply")]
        client.flush().await?;
        if let Some(changed) = self.changed_parameters() {
            client
                .feed(PgWireBackendMessage::ParameterStatus(changed))
                .await?;
        }
        client
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                status,
            )))
            .await?;
        drop(encoding);
        let _writing = turso_pg::phase::enter(turso_pg::phase::Phase::Write);
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
    type Statement = Parsed;
    type QueryParser = ParsedQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    // pgwire's default Parse, Bind, Describe, Execute and Close handlers each flush their reply: an
    // extended round of five messages cost five socket writes against one for a simple Query. The
    // overrides below are the same handlers with every reply fed, so a round's replies go out
    // together when the client asks: at Sync (`on_sync`) or Flush (pgwire's `on_flush`); an error
    // is still flushed at once by pgwire (wire review 1 item 12).

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // In a failed block only a block exit is parsed, as PostgreSQL's exec_parse_message refuses
        // anything else (25P02) ahead of parse analysis (postgres.c:1522-1528); a blank query is no
        // statement there and parses. ParseComplete went out for every statement, and a utility
        // statement's declared unspecified parameter then answered 42P18 at Bind ahead of the
        // 25P02 (wire review 17 item 8).
        if self.state().aborted
            && !is_blank(&message.query)
            && !TxVerb::of(&message.query).ends_block()
        {
            return Err(PgWireError::UserError(aborted_error()));
        }
        // The text as it is, and the branch call it is, read here once (wire review 13 item 6).
        let types = message
            .type_oids
            .iter()
            .map(|o| Type::from_oid(*o))
            .collect();
        let id = message.name.unwrap_or_else(|| DEFAULT_NAME.to_owned());
        client
            .portal_store()
            .put_statement(Arc::new(StoredStatement::new(
                id,
                Parsed::new(message.query),
                types,
            )));
        client
            .feed(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let Some(statement) = client.portal_store().get_statement(name) else {
            return Err(PgWireError::StatementNotFound(name.to_owned()));
        };
        check_bind(&message).map_err(PgWireError::UserError)?;
        // Every statement's parameter count is checked here, first, as PostgreSQL's
        // exec_bind_message checks it: a branch call's (its $n); a statement PostgreSQL analyses at
        // Parse, the declared parameters and every `$n` its lexer reads ([`scan_params`]); any
        // other, the declared ones alone (each with a type: 42P18 for one declared unspecified).
        // None was checked at first, so two values for turso_branch_create($1) created the branch
        // (wire review 10 item 5) and a value for CHECKPOINT or BEGIN ran it (wire review 12 item
        // 2); then a statement whose text held a `$` byte was counted only at Execute, after its
        // prepare, so a `$` in a comment or a quoted name let a SET be performed before the
        // refusal, and a failed block's 25P02 or a format code's 22023 answered before the count
        // (wire review 14 item 4, review 17 item 2). A `$n` past MAX_PARAMETER is left to the
        // prepare, which refuses it (42601).
        let sql = &statement.statement.sql;
        let scan = statement.statement.scan;
        let required = if let Some(call) = &statement.statement.call {
            Some(
                parameter_types(&branch_call_types(call), &statement.parameter_types)
                    .map_err(PgWireError::UserError)?
                    .len(),
            )
        } else if scan.analysed && scan.highest != Some(0) {
            scan.highest
                .map(|n| statement.parameter_types.len().max(n as usize))
        } else {
            Some(
                parameter_types(&StatementTypes::default(), &statement.parameter_types)
                    .map_err(PgWireError::UserError)?
                    .len(),
            )
        };
        if let Some(required) = required {
            check_bind_arity(message.parameters.len(), &statement.id, required)
                .map_err(PgWireError::UserError)?;
        }
        // In a failed block only a block exit with no values may be bound, as PostgreSQL's
        // exec_bind_message refuses the rest (25P02); a ROLLBACK with a value was run, ending the
        // block (wire review 14 item 4).
        if self.state().aborted && (!TxVerb::of(sql).ends_block() || !message.parameters.is_empty())
        {
            return Err(PgWireError::UserError(aborted_error()));
        }
        check_parameter_codes(&message).map_err(PgWireError::UserError)?;
        let portal = Portal::try_new(&message, statement)?;
        // A named portal lives until its Close or its transaction's end: binding the name again
        // is 42P03, as PostgreSQL's CreatePortal refuses it. It replaced the portal silently, a
        // suspended one included (wire review 15 item 20). The unnamed portal is replaced, as
        // there.
        if portal.name != DEFAULT_NAME && client.portal_store().get_portal(&portal.name).is_some()
        {
            return Err(PgWireError::UserError(error(
                "42P03",
                format!("cursor \"{}\" already exists", portal.name),
            )));
        }
        {
            let mut st = self.state();
            match message
                .result_column_format_codes
                .iter()
                .copied()
                .find(|c| !matches!(c, 0 | 1))
            {
                Some(code) => {
                    st.bad_result_codes.insert(portal.name.clone(), code);
                }
                None => {
                    st.bad_result_codes.remove(&portal.name);
                }
            }
            st.finished_tags.remove(&portal.name);
            if portal.name != DEFAULT_NAME {
                st.done_portals.remove(&portal.name);
            }
            // The unnamed portal too ends with its transaction (it may outlive a Sync, suspended
            // in a block: wire review 15 item 16).
            if !st.portals.contains(&portal.name) {
                st.portals.push(portal.name.clone());
            }
        }
        client.portal_store().put_portal(Arc::new(portal));
        client
            .feed(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        // The wire budget's extended fire-check plant (feature budget-plant-split-reply, never in a
        // default build): BindComplete written on its own, a second reply write per extended round
        // that `budget.py counts --protocol extended` must catch on net_out alone (wire review 13
        // item 11).
        #[cfg(feature = "budget-plant-split-reply")]
        client.flush().await?;
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let (parameters, fields) = match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                let Some(statement) = client.portal_store().get_statement(name) else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                };
                let r = self.do_describe_statement(client, &statement).await?;
                (Some(r.parameters), r.fields)
            }
            TARGET_TYPE_BYTE_PORTAL => {
                let Some(portal) = client.portal_store().get_portal(name) else {
                    return Err(PgWireError::UserError(portal_not_found(name)));
                };
                (None, self.do_describe_portal(client, &portal).await?.fields)
            }
            other => return Err(PgWireError::InvalidTargetType(other)),
        };
        if let Some(parameters) = parameters {
            client
                .feed(PgWireBackendMessage::ParameterDescription(
                    ParameterDescription::new(parameters.iter().map(|t| t.oid()).collect()),
                ))
                .await?;
        }
        // NoData whenever there are no columns (pgwire's helper sends an empty RowDescription
        // for a statement with parameters and no columns).
        let reply = if fields.is_empty() {
            PgWireBackendMessage::NoData(NoData::new())
        } else {
            PgWireBackendMessage::RowDescription(RowDescription::new(
                fields.iter().map(Into::into).collect(),
            ))
        };
        client.feed(reply).await?;
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        let Some(portal) = client.portal_store().get_portal(name) else {
            return Err(PgWireError::UserError(portal_not_found(name)));
        };
        // An empty statement is EmptyQueryResponse every time, before any portal-state check, as
        // PostgreSQL's exec_execute_message answers an empty command; it is never marked done. Its
        // second Execute was 55000 and rolled the pipeline back (wire review 15 item 1).
        if is_blank(&portal.statement.statement.sql) {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
            if name == DEFAULT_NAME {
                client.portal_store().rm_portal(name);
            }
            return Ok(());
        }
        // A portal that ran without rows, or failed, does not run again (PostgreSQL's
        // PortalRun refuses one that is not ready); it ran again and wrote twice (wire review 12
        // item 5).
        if self.state().done_portals.contains(name) {
            return Err(PgWireError::UserError(error(
                "55000",
                format!("portal \"{name}\" cannot be run"),
            )));
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let max_rows = message.max_rows.max(0) as usize;
        let state = portal.state();
        let mut state = state.lock().await;
        match &mut *state {
            PortalExecutionState::Initial => {
                match ExtendedQueryHandler::do_query(self, client, &portal, max_rows).await? {
                    Response::Query(mut results) => {
                        let refuse = self.state().bad_result_codes.get(name).copied();
                        let tag = results.command_tag().to_owned();
                        *state = if feed_rows(client, &mut results, max_rows, refuse).await? {
                            PortalExecutionState::Suspended(results)
                        } else {
                            self.state().finished_tags.insert(name.to_string(), tag);
                            PortalExecutionState::Finished
                        };
                    }
                    Response::Execution(tag)
                    | Response::TransactionStart(tag)
                    | Response::TransactionEnd(tag) => {
                        self.portal_done(name);
                        client
                            .feed(PgWireBackendMessage::CommandComplete(tag.into()))
                            .await?;
                    }
                    Response::EmptyQuery => {
                        client
                            .feed(PgWireBackendMessage::EmptyQueryResponse(
                                EmptyQueryResponse::new(),
                            ))
                            .await?;
                    }
                    Response::Error(e) => {
                        self.portal_done(name);
                        client
                            .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
                            .await?;
                    }
                    // The fast path encodes a simple Query's reply alone (branch, portal None).
                    Response::Inline(_) => {
                        return Err(PgWireError::UserError(error(
                            "XX000",
                            "internal error: an inline reply reached an Execute".to_string(),
                        )));
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
            PortalExecutionState::Suspended(results) => {
                let tag = results.command_tag().to_owned();
                if !feed_rows(client, results, max_rows, None).await? {
                    self.state().finished_tags.insert(name.to_string(), tag);
                    *state = PortalExecutionState::Finished;
                }
            }
            // A query portal that returned all its rows returns none more: its own tag with 0
            // rows (`SELECT 0`, `INSERT 0 0`, ...), as PostgreSQL copies the portal's tag (it
            // answered NoData, wire review 12 item 5; then SELECT 0 for every verb, review 15
            // item 17).
            PortalExecutionState::Finished => {
                let tag = self
                    .state()
                    .finished_tags
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| "SELECT".to_string());
                client
                    .feed(PgWireBackendMessage::CommandComplete(
                        Tag::new(&tag).with_rows(0).into(),
                    ))
                    .await?;
            }
        }
        let suspended = matches!(&*state, PortalExecutionState::Suspended(_));
        drop(state);
        client.set_state(PgWireConnectionState::ReadyForQuery);
        // As pgwire does: the unnamed portal goes with its execution, unless a row limit
        // suspended it, when it continues at the next Execute until its transaction ends or the
        // next unnamed Bind replaces it, as in PostgreSQL (it was removed, so the next Execute was
        // 34000: wire review 15 item 16).
        if name == DEFAULT_NAME && !suspended {
            client.portal_store().rm_portal(name);
        }
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => client.portal_store().rm_statement(name),
            TARGET_TYPE_BYTE_PORTAL => client.portal_store().rm_portal(name),
            _ => {}
        }
        client
            .feed(PgWireBackendMessage::CloseComplete(CloseComplete::new()))
            .await?;
        Ok(())
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
        // Executes up to Sync are one implicit transaction, as in PostgreSQL (wire review 1 item 8).
        let sql = &portal.statement.statement.sql;
        // An empty statement is EmptyQueryResponse and joins no block, as over the simple
        // protocol; it failed ("contains no statements") after joining the pipeline's implicit
        // block, which rolled the pipeline back (wire review 12 item 4).
        if is_blank(sql) {
            return Ok(Response::EmptyQuery);
        }
        let call = portal.statement.statement.call.clone();
        let result = self
            .begin_implicit(sql, call.as_ref(), true)
            .and_then(|()| self.run(sql, call, Some(portal), &portal.result_column_format));
        self.after_implicit(sql, result.is_err());
        result.map_err(PgWireError::UserError)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = &target.statement.sql;
        self.refuse_describe_if_aborted(sql)?;
        // An empty statement returns no rows: NoData (wire review 12 item 4).
        if is_blank(sql) {
            let declared = target
                .parameter_types
                .iter()
                .map(|t| t.clone().unwrap_or(Type::TEXT))
                .collect();
            return Ok(DescribeStatementResponse::new(declared, vec![]));
        }
        if let Some(call) = &target.statement.call {
            let fields =
                branch_call_fields(call, &Format::UnifiedText).map_err(PgWireError::UserError)?;
            let params = parameter_types(&branch_call_types(call), &target.parameter_types)
                .map_err(PgWireError::UserError)?;
            return Ok(DescribeStatementResponse::new(params, fields));
        }
        // The special statements return no rows: NoData, from the text (their prepared stand-ins
        // have a dummy column). That their prepare is never what performs them is describe_prepare's
        // job, from the parse, so a form this text test misses is still not performed.
        if is_pg_non_query(sql) {
            // The parameters the client declared, as declared, the rest as text (wire review 1
            // item 13).
            let declared = target
                .parameter_types
                .iter()
                .map(|t| t.clone().unwrap_or(Type::TEXT))
                .collect();
            return Ok(DescribeStatementResponse::new(declared, vec![]));
        }
        self.describe_prepare_with_params(sql, &target.parameter_types)?;
        let fields = self
            .described_fields(&Format::UnifiedText, &target.parameter_types)
            .map_err(PgWireError::UserError)?;
        let params = self
            .described_parameters(&target.parameter_types)
            .map_err(PgWireError::UserError)?;
        // Fixed, or checked against the fixed ones, by a Describe that prepared the statement
        // (see [`Parsed`]); one whose prepare would perform it prepared nothing, and its Execute
        // fixes them.
        if self.state().described.is_some() {
            let parsed = &target.statement;
            parsed.fix_params(&params).map_err(PgWireError::UserError)?;
            parsed
                .fix_columns(&field_types(&fields))
                .map_err(PgWireError::UserError)?;
        }
        Ok(DescribeStatementResponse::new(params, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = &portal.statement.statement.sql;
        self.refuse_describe_if_aborted(sql)?;
        // An empty statement returns no rows: NoData, its result formats ignored (wire review 12
        // item 4).
        if is_blank(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        if let Some(call) = &portal.statement.statement.call {
            let fields = branch_call_fields(call, &portal.result_column_format)
                .map_err(PgWireError::UserError)?;
            return Ok(DescribePortalResponse::new(fields));
        }
        if is_pg_non_query(sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        self.describe_prepare_with_params(sql, &portal.statement.parameter_types)?;
        let fields = self
            .described_fields(
                &portal.result_column_format,
                &portal.statement.parameter_types,
            )
            .map_err(PgWireError::UserError)?;
        // As do_describe_statement: the result types only, which a portal's Describe reports.
        if self.state().described.is_some() {
            portal
                .statement
                .statement
                .fix_columns(&field_types(&fields))
                .map_err(PgWireError::UserError)?;
        }
        Ok(DescribePortalResponse::new(fields))
    }

    /// ReadyForQuery after Sync carries the session's real transaction state.
    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // A pipeline's implicit block commits here; a failed commit is reported before
        // ReadyForQuery (wire review 1 item 8).
        if let Err(e) = self.end_implicit() {
            // A FATAL one (a transaction the engine could not end) ends the session: serve_session
            // sends it and closes.
            if e.severity == "FATAL" {
                return Err(PgWireError::UserError(e));
            }
            client
                .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
                .await?;
        }
        let (notices, status) = self.end_round();
        for notice in notices {
            client
                .feed(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                    *notice,
                )))
                .await?;
        }
        client.set_transaction_status(status);
        if let Some(changed) = self.changed_parameters() {
            client
                .feed(PgWireBackendMessage::ParameterStatus(changed))
                .await?;
        }
        send_ready_for_query(client, status).await?;
        Ok(())
    }
}

/// pgwire's send_partial_query_response with every message fed: up to `max_rows` rows (0: all),
/// then CommandComplete, or PortalSuspended when the limit was reached. True when suspended.
async fn feed_rows<C>(
    client: &mut C,
    results: &mut QueryResponse,
    max_rows: usize,
    refuse: Option<i16>,
) -> PgWireResult<bool>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: std::fmt::Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let tag = results.command_tag().to_owned();
    let data = results.data_rows();
    let mut rows = 0;
    while max_rows == 0 || rows < max_rows {
        match data.next().await {
            Some(row) => {
                // `refuse`: a result format code other than 0 or 1, refused at the first row as
                // PostgreSQL's printtup refuses it (wire review 13 item 10).
                if let Some(code) = refuse {
                    return Err(PgWireError::UserError(error(
                        "22023",
                        format!("unsupported format code: {code}"),
                    )));
                }
                client.feed(PgWireBackendMessage::DataRow(row?)).await?;
                rows += 1;
            }
            None => {
                client
                    .feed(PgWireBackendMessage::CommandComplete(
                        Tag::new(&tag).with_rows(rows).into(),
                    ))
                    .await?;
                return Ok(false);
            }
        }
    }
    client
        .feed(PgWireBackendMessage::PortalSuspended(PortalSuspended::new()))
        .await?;
    Ok(true)
}

/// The name and type of each result column a Describe reported, as [`Parsed::fix_columns`] reads
/// them.
fn field_types(fields: &[FieldInfo]) -> Vec<(String, Type)> {
    fields
        .iter()
        .map(|f| (f.name().to_string(), f.datatype().clone()))
        .collect()
}

/// The result column types the parse gives (StatementTypes::columns), with each bare-parameter
/// column (StatementTypes::param_targets) typed as its parameter: the declared type, else the one
/// inferred ([`parameter_types`]), as PostgreSQL announces `SELECT $1` (it was text). Describe
/// and Execute read the same list.
fn result_column_types(types: &StatementTypes, declared: &[Option<Type>]) -> Vec<Option<u32>> {
    let mut columns = types.columns.clone();
    if types.param_targets.len() != columns.len() || types.param_targets.iter().all(Option::is_none)
    {
        return columns;
    }
    let Ok(params) = parameter_types(types, declared) else {
        return columns;
    };
    for (column, target) in columns.iter_mut().zip(&types.param_targets) {
        if let Some(t) = target.and_then(|n| params.get(n as usize - 1)) {
            *column = Some(t.oid());
        }
    }
    columns
}

/// A prepared statement's result columns, typed by [`column_type`]: what Describe reports and what
/// the rows are encoded as, on both protocols.
fn result_fields(
    stmt: &turso_core::Statement,
    types: &[Option<u32>],
    format: &Format,
) -> SqlResult<Vec<FieldInfo>> {
    field_info(stmt, format, |i| column_type(stmt, types, i))
}

/// A result column's type from the statement alone, never from its values: the type the parse
/// gave (an aggregate, [`PgConnection::prepare_typed`]), else the engine's
/// ([`resolve_pg_type_for_column`], text where it cannot tell).
fn column_type(stmt: &turso_core::Statement, types: &[Option<u32>], idx: usize) -> Type {
    types
        .get(idx)
        .copied()
        .flatten()
        .and_then(Type::from_oid)
        .unwrap_or_else(|| resolve_pg_type_for_column(stmt, idx))
}

fn field_info(
    stmt: &turso_core::Statement,
    format: &Format,
    pg_type: impl Fn(usize) -> Type,
) -> SqlResult<Vec<FieldInfo>> {
    let columns = stmt.num_columns();
    (0..columns)
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            let format = result_format(format, i, columns)?;
            Ok(FieldInfo::new(name, None, None, pg_type(i), format))
        })
        .collect()
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

/// A CREATE TABLE with foreign keys, run in a transaction of its own (a savepoint inside a block) so
/// that every key is resolved once the table exists, as the engine resolves it for a write, and the
/// table is undone if one does not resolve (42830, turso_pg::check_table_keys): the parents were
/// checked at prepare, a key onto the table itself only now. It was created unchecked, and every
/// INSERT into it then failed 'foreign key mismatch' (wire review 13 item 2). The transaction and
/// its undo are the frontend's one rule (PgConnection::in_own_transaction): an undo that fails
/// breaks the connection, FATAL 08006 here, where `let _ =` kept the refused table in a transaction
/// the client's next COMMIT committed (wire review 16 item 14). In its own transaction a lock met
/// at any step was rolled back with it: 40001, as for a failed COMMIT (commit_failed).
fn create_with_keys(
    conn: &PgConnection,
    stmt: &mut turso_core::Statement,
    sql: &str,
    table: &str,
    backoff: &mut Backoff<'_>,
) -> PgWireResult<Response> {
    let own = conn.inner().get_auto_commit();
    conn.in_own_transaction("__turso_create_keys", "CREATE TABLE", || {
        run_waiting(stmt, backoff, |_| Ok(()))?;
        turso_pg::check_table_keys(conn.inner(), table)
    })
    .map_err(|e| {
        let info = engine_info(&e);
        if conn.is_broken() {
            broken_fatal(info)
        } else if own {
            commit_failed(conn, info)
        } else {
            info
        }
    })
    .map_err(PgWireError::UserError)?;
    Ok(Response::Execution(command_tag(sql, stmt.n_change() as usize)))
}

/// Execute a query that returns rows and build a Query response. Each column has the type the
/// statement gives it ([`column_type`]), the one Describe reported, computed by the caller once per
/// column (`columns`, one per result column), and each row is encoded as it comes (wire review 1
/// item 14: no value inference, no row buffering beyond the reply's).
fn execute_query(
    stmt: &mut turso_core::Statement,
    format: &Format,
    columns: &[Type],
    schema: &turso_core::schema::Schema,
    backoff: &mut Backoff<'_>,
) -> PgWireResult<Response> {
    let header =
        Arc::new(field_info(stmt, format, |i| columns[i].clone()).map_err(PgWireError::UserError)?);
    // A binary column of a type encode_binary has no encoding for (numeric, date, timestamp,
    // uuid, ...) is refused before the statement runs, by its type alone: refused at its first
    // row, a write's RETURNING was refused after the write (wire review 4 item 1), and a numeric
    // would have gone out as a float's eight bytes.
    if let Some(f) = header
        .iter()
        .find(|f| f.format() == FieldFormat::Binary && !BINARY_ENCODED.contains(f.datatype()))
    {
        return Err(PgWireError::UserError(error(
            "0A000",
            format!(
                "binary format for column \"{}\" of type {} is not supported; ask for text format",
                f.name(),
                f.datatype()
            ),
        )));
    }
    let pads: Vec<Option<usize>> = (0..stmt.num_columns())
        .map(|i| bpchar_width(stmt, schema, i))
        .collect();
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    // A row that cannot be encoded (a value out of its binary type's range) fails the statement
    // there, so its block fails with it, instead of going out as an error after the statement
    // succeeded (wire review 4 item 1).
    let mut unencodable = None;
    let ran = run_waiting(stmt, backoff, |row| {
        match encode_row(&header, &pads, row.get_values()) {
            Ok(encoded) => {
                rows.push(Ok(encoded));
                Ok(())
            }
            Err(e) => {
                unencodable = Some(e);
                Err(LimboError::InternalError(
                    "a row could not be encoded".to_string(),
                ))
            }
        }
    });
    if let Some(e) = unencodable {
        return Err(e);
    }
    ran.map_err(engine_error)?;
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
        // A binary value is the described type's encoding, not the engine value's (wire review 1
        // item 14); text format reads the same either way.
        if header
            .get(i)
            .is_some_and(|fi| fi.format() == FieldFormat::Binary)
        {
            encode_binary(&mut encoder, val, &pg_type)?;
            continue;
        }
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

/// One value in binary format, encoded as its column's described type: an engine integer as int2,
/// int4 or int8 (refused 22003 out of range), as float4 or float8, or as bool; a real as float4
/// or float8; anything else, in a text-like column, as its text's bytes (binary text is the text).
/// A value its column's type has no binary form for here is refused rather than sent as the
/// engine's bytes under that type's name.
/// The column types [`encode_binary`] encodes; a binary column of any other type is refused before
/// its statement runs.
const BINARY_ENCODED: [Type; 13] = [
    Type::BOOL,
    Type::INT2,
    Type::INT4,
    Type::INT8,
    Type::FLOAT4,
    Type::FLOAT8,
    Type::TEXT,
    Type::VARCHAR,
    Type::BPCHAR,
    Type::NAME,
    Type::UNKNOWN,
    Type::JSON,
    Type::BYTEA,
];

fn encode_binary(encoder: &mut DataRowEncoder, val: &Value, pg_type: &Type) -> PgWireResult<()> {
    use turso_core::Numeric;
    let range = |_| {
        PgWireError::UserError(error(
            "22003",
            format!("value out of range for type {pg_type}"),
        ))
    };
    let t = pg_type;
    let text_like = [
        Type::TEXT,
        Type::VARCHAR,
        Type::BPCHAR,
        Type::NAME,
        Type::UNKNOWN,
    ]
    .contains(t);
    match val {
        Value::Null => encoder.encode_field(&None::<i8>),
        Value::Numeric(Numeric::Integer(i)) => match t {
            _ if *t == Type::BOOL => encoder.encode_field(&(*i != 0)),
            _ if *t == Type::INT2 => encoder.encode_field(&i16::try_from(*i).map_err(range)?),
            _ if *t == Type::INT4 => encoder.encode_field(&i32::try_from(*i).map_err(range)?),
            _ if *t == Type::INT8 => encoder.encode_field(i),
            _ if *t == Type::FLOAT4 => encoder.encode_field(&(*i as f32)),
            _ if *t == Type::FLOAT8 => encoder.encode_field(&(*i as f64)),
            _ if text_like => encoder.encode_field(&i.to_string().as_str()),
            _ => Err(no_binary(val, t)),
        },
        Value::Numeric(Numeric::Float(f)) => match t {
            _ if *t == Type::FLOAT8 => encoder.encode_field(&f64::from(*f)),
            _ if *t == Type::FLOAT4 => encoder.encode_field(&(f64::from(*f) as f32)),
            _ if text_like => encoder.encode_field(&f64::from(*f).to_string().as_str()),
            _ => Err(no_binary(val, t)),
        },
        Value::Text(s) if text_like || *t == Type::JSON => encoder.encode_field(&s.as_str()),
        Value::Blob(b) if *t == Type::BYTEA => encoder.encode_field(&b.as_slice()),
        _ => Err(no_binary(val, t)),
    }
}

fn no_binary(val: &Value, t: &Type) -> PgWireError {
    let class = match val {
        Value::Null => "null",
        Value::Numeric(turso_core::Numeric::Integer(_)) => "integer",
        Value::Numeric(turso_core::Numeric::Float(_)) => "real",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
    };
    PgWireError::UserError(error(
        "0A000",
        format!(
            "binary format for a {class} value in a column of type {t} is not supported; ask for \
             text format"
        ),
    ))
}

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(
    stmt: &mut turso_core::Statement,
    query: &str,
    backoff: &mut Backoff<'_>,
    simple: bool,
) -> PgWireResult<Response> {
    let executing = turso_pg::phase::enter(turso_pg::phase::Phase::Execute);
    run_waiting(stmt, backoff, |_| Ok(())).map_err(engine_error)?;
    drop(executing);

    let affected = stmt.n_change();
    // A simple Query's INSERT, UPDATE or DELETE completes with its tag encoded inline, with no
    // allocation (L5; inline_dml_completes_are_the_messages_pgwire_encodes).
    if simple {
        if let Some(m) = inline_dml_complete(query, affected as usize) {
            return Ok(Response::Inline(m));
        }
    }
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
}

/// INSERT, UPDATE or DELETE, read from the statement's first token without copying it, as
/// [`command_tag`] reads it (skip_blank, then the verb in any case); None for any other verb.
fn dml_verb(query: &str) -> Option<&'static str> {
    let start = skip_blank(query.as_bytes(), 0).unwrap_or(query.len());
    let head = query.as_bytes()[start..].trim_ascii_start();
    ["INSERT", "UPDATE", "DELETE"].into_iter().find(|verb| {
        head.len() >= verb.len() && head[..verb.len()].eq_ignore_ascii_case(verb.as_bytes())
    })
}

/// The CommandComplete of an INSERT (`INSERT 0 n`), UPDATE or DELETE (`UPDATE n`), encoded inline.
fn inline_dml_complete(query: &str, rows: usize) -> Option<pgwire::messages::Inline> {
    let verb = dml_verb(query)?;
    let mut digits = [0u8; 20];
    let n = int_text(rows as i64, &mut digits);
    let oid: &[u8] = if verb == "INSERT" { b" 0 " } else { b" " };
    let len = 4 + verb.len() + oid.len() + n.len() + 1;
    let mut m = pgwire::messages::Inline::default();
    let ok = m.push(b"C")
        && m.push(&(len as i32).to_be_bytes())
        && m.push(verb.as_bytes())
        && m.push(oid)
        && m.push(n)
        && m.push(&[0]);
    ok.then_some(m)
}

/// The type of each parameter, $1 up to the highest the statement holds or the client declared:
/// the declared one, else the one its context gives (see [`StatementTypes::params`]), else text, as
/// PostgreSQL resolves a parameter nothing types. One below the highest that the statement does
/// not hold and the client did not declare cannot be typed: 42P18, as PostgreSQL refuses it (wire
/// review 4 item 3). Which $n the statement holds is read from its whole parse tree
/// ([`StatementTypes::used`]), not from the engine's slots, which a clause it folds away does not
/// get (`HAVING count(*) > $1` was 42P18, `WHERE false AND id = $1` 08P01; wire review 8 item 5).
/// Describe and Bind read the same list.
fn parameter_types(types: &StatementTypes, declared: &[Option<Type>]) -> SqlResult<Vec<Type>> {
    let highest = types.used.last().map_or(0, |n| *n as usize);
    (1..=highest.max(declared.len()))
        .map(|n| {
            if let Some(Some(t)) = declared.get(n - 1) {
                if *t != Type::UNKNOWN {
                    return Ok(t.clone());
                }
            }
            // Undeclared (or UNKNOWN) and compared with something no context types: refused, never
            // compared as text (wire review 8 item 7). Here, where the declared types are read,
            // not at prepare (wire review 11 item 1).
            if types.used.binary_search(&(n as u32)).is_err() || types.untyped.contains(&(n as u32))
            {
                return Err(error(
                    "42P18",
                    format!("could not determine data type of parameter ${n}"),
                ));
            }
            Ok(types
                .params
                .get(&(n as u32))
                .copied()
                .and_then(Type::from_oid)
                .unwrap_or(Type::TEXT))
        })
        .collect()
}

/// Bind a portal's parameters to its statement, each converted from its text by its type, `types`
/// (see [`parameter_types`], fixed by [`Parsed::fix_params`]): an undeclared parameter's is the one
/// its context gives, the type Describe reported, not one guessed from the value (an integer, then
/// a float, then a boolean: '007' went into a text column as 7; wire review 4 item 3). The caller
/// has checked the values are as many as `types` ([`check_bind_arity`]). A value the engine
/// refuses fails the bind. The engine numbers PostgreSQL's $n as its parameter n.
fn bind_portal_parameters(
    stmt: &mut turso_core::Statement,
    portal: &Portal<Parsed>,
    types: &[Type],
) -> PgWireResult<()> {
    // check_bind_arity made the counts equal; zipped short, a mismatch bound a value under another's
    // type or left a parameter unbound (NULL to the engine), so it is refused (wire review 18 item
    // 23).
    if types.len() != portal.parameters.len() {
        return Err(PgWireError::UserError(error(
            "XX000",
            format!(
                "internal error: {} parameter types for {} values",
                types.len(),
                portal.parameters.len()
            ),
        )));
    }
    // The format codes were checked at Bind ([`check_bind`]): none, one, or one per value.
    for (i, (pg_type, sent)) in types.iter().zip(&portal.parameters).enumerate() {
        let value = match sent {
            None => Value::Null,
            Some(bytes) if portal.parameter_format.is_binary(i) => {
                pg_binary_to_value(bytes, pg_type, i + 1)?
            }
            Some(bytes) => pg_bytes_to_value(bytes, pg_type)?,
        };
        let index = NonZero::new(i + 1).expect("i + 1 >= 1");
        // A parameter with no engine slot (declared but unused, or in a clause the engine folds
        // away) binds nothing.
        if stmt.parameters().has_index(index) {
            stmt.bind_at(index, value)
                .map_err(|e| PgWireError::UserError(engine_info(&e)))?;
        }
    }
    Ok(())
}

/// PostgreSQL's first check of a Bind message (exec_bind_message), made before anything else and
/// before BindComplete: a parameter-format list is empty, one code, or one per value sent, else
/// 08P01. The value count, the failed-block refusal and the parameter codes follow, in
/// exec_bind_message's order ([`check_parameter_codes`]); a result code is refused only when a
/// row is formatted (`SessionState::bad_result_codes`). They were made at Execute, after
/// BindComplete, or for a branch call not at all, and a single bad code was invisible there: pgwire
/// folds one code into text (wire review 10 item 5); then every code was refused here first, so a
/// result code 2 on an INSERT and a parameter code 2 with no values were refused where PostgreSQL
/// succeeds, and codes [2, 2, 2] for one value was 22023 where it answers 08P01 (wire review 13
/// item 10).
fn check_bind(bind: &Bind) -> SqlResult<()> {
    let (codes, values) = (bind.parameter_format_codes.len(), bind.parameters.len());
    if codes > 1 && codes != values {
        return Err(error(
            "08P01",
            format!("bind message has {codes} parameter formats but {values} parameters"),
        ));
    }
    Ok(())
}

/// The format code of each value a Bind sends is 0 (text) or 1 (binary), else 22023 "unsupported
/// format code: N" (even for a NULL value), as PostgreSQL reads each value's format: one code
/// applies to every value, so it is read only when a value is sent.
fn check_parameter_codes(bind: &Bind) -> SqlResult<()> {
    if bind.parameters.is_empty() {
        return Ok(());
    }
    match bind
        .parameter_format_codes
        .iter()
        .find(|c| !matches!(c, 0 | 1))
    {
        Some(code) => Err(error("22023", format!("unsupported format code: {code}"))),
        None => Ok(()),
    }
}

/// A Bind's value count against its statement's parameters, in PostgreSQL's words (08P01), the
/// unnamed statement named "" as PostgreSQL names it (pgwire stores it as DEFAULT_NAME).
fn check_bind_arity(values: usize, statement: &str, required: usize) -> SqlResult<()> {
    if values == required {
        return Ok(());
    }
    let name = if statement == DEFAULT_NAME {
        ""
    } else {
        statement
    };
    Err(error(
        "08P01",
        format!(
            "bind message supplies {values} parameters, but prepared statement \"{name}\" \
             requires {required}"
        ),
    ))
}

/// A parameter sent in binary format, read as PostgreSQL's binary receive function for its type
/// reads it: int2/int4/int8 and float4/float8 big-endian of their exact width, bool one byte
/// (non-zero is true), bytea its bytes, text-like types and json their UTF-8 bytes, jsonb its
/// version byte (1) then the text. A wrong width or version is 22P03, as PostgreSQL's "incorrect
/// binary data format"; any other type's binary form is refused (0A000), never read as text: it
/// was, so a binary int4 2 failed and the bytes "0001" bound 1 (wire review 8 item 4). `n` is the
/// parameter's number, for the messages.
fn pg_binary_to_value(bytes: &[u8], pg_type: &Type, n: usize) -> PgWireResult<Value> {
    let bad = || {
        PgWireError::UserError(error(
            "22P03",
            format!("incorrect binary data format in bind parameter {n} (type {pg_type})"),
        ))
    };
    let text = |b: &[u8]| -> PgWireResult<Value> {
        std::str::from_utf8(b)
            .map(|s| Value::from_text(s.to_owned()))
            .map_err(|e| {
                PgWireError::UserError(error(
                    "22021",
                    format!("invalid UTF-8 in bind parameter {n}: {e}"),
                ))
            })
    };
    let t = pg_type;
    if *t == Type::INT2 {
        Ok(Value::from_i64(
            i16::from_be_bytes(bytes.try_into().map_err(|_| bad())?).into(),
        ))
    } else if *t == Type::INT4 {
        Ok(Value::from_i64(
            i32::from_be_bytes(bytes.try_into().map_err(|_| bad())?).into(),
        ))
    } else if *t == Type::INT8 {
        Ok(Value::from_i64(i64::from_be_bytes(
            bytes.try_into().map_err(|_| bad())?,
        )))
    } else if *t == Type::FLOAT4 {
        Ok(Value::from_f64(
            f32::from_be_bytes(bytes.try_into().map_err(|_| bad())?).into(),
        ))
    } else if *t == Type::FLOAT8 {
        Ok(Value::from_f64(f64::from_be_bytes(
            bytes.try_into().map_err(|_| bad())?,
        )))
    } else if *t == Type::BOOL {
        match bytes {
            [b] => Ok(Value::from_i64((*b != 0) as i64)),
            _ => Err(bad()),
        }
    } else if *t == Type::BYTEA {
        Ok(Value::from_blob(bytes.to_vec()))
    } else if *t == Type::JSONB {
        match bytes.split_first() {
            Some((1, rest)) => text(rest),
            _ => Err(bad()),
        }
    } else if [
        Type::TEXT,
        Type::VARCHAR,
        Type::BPCHAR,
        Type::NAME,
        Type::UNKNOWN,
        Type::JSON,
    ]
    .contains(t)
    {
        text(bytes)
    } else {
        Err(PgWireError::UserError(error(
            "0A000",
            format!(
                "binary format for bind parameter {n} of type {t} is not supported; send it in \
                 text format"
            ),
        )))
    }
}

/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations). A value the type cannot read is
/// refused as the type's input function refuses it: 22P02 for bad syntax, 22003 for a value outside
/// the type, 22023 for bad bytea hex; they were XX000, and no integer's range below int8 was checked
/// (wire review 13 item 8, review 14 item 28).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(error(
            "22021",
            format!("invalid byte sequence for encoding \"UTF8\" in parameter: {e}"),
        ))
    })?;
    if let Some(element) = element_of(pg_type.oid()).and_then(Type::from_oid) {
        return pg_array_to_value(text, &element);
    }
    let syntax = |name: &str| {
        PgWireError::UserError(error(
            "22P02",
            format!("invalid input syntax for type {name}: \"{text}\""),
        ))
    };
    let out_of_range = |name: &str| {
        PgWireError::UserError(error(
            "22003",
            format!("value \"{text}\" is out of range for type {name}"),
        ))
    };

    match *pg_type {
        Type::INT2 | Type::INT4 | Type::INT8 => {
            let (name, min, max) = match *pg_type {
                Type::INT2 => ("smallint", i64::from(i16::MIN), i64::from(i16::MAX)),
                Type::INT4 => ("integer", i64::from(i32::MIN), i64::from(i32::MAX)),
                _ => ("bigint", i64::MIN, i64::MAX),
            };
            match pg_integer(text) {
                Some(Some(i)) if (min..=max).contains(&i) => Ok(Value::from_i64(i)),
                Some(_) => Err(out_of_range(name)),
                None => Err(syntax(name)),
            }
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let name = match *pg_type {
                Type::FLOAT4 => "real",
                Type::FLOAT8 => "double precision",
                _ => "numeric",
            };
            let trimmed = text.trim_matches(|c: char| c.is_ascii() && pg_space(c as u8));
            let f: f64 = trimmed.parse().map_err(|_| syntax(name))?;
            // A finite spelling past the type's range ('1e400'), which Rust reads as infinity.
            let infinite_spelled = trimmed
                .trim_start_matches(['+', '-'])
                .to_ascii_lowercase()
                .starts_with("inf");
            // float4's range is read as float4in reads it, rounded to the nearest float4:
            // '3.4028235e+38', float4out's own spelling of FLT_MAX, is above f32::MAX as an f64
            // and was refused (wire review 16 item 23).
            let in_type = match *pg_type {
                Type::FLOAT4 => trimmed.parse::<f32>().map_or(f, f64::from),
                _ => f,
            };
            if in_type.is_infinite() && !infinite_spelled {
                return Err(out_of_range(name));
            }
            // A spelling of a nonzero value that underflows the type to zero ('1e-400'), which
            // float4in and float8in refuse (ERANGE with a zero result; recalled from their source,
            // not re-read); numeric's range holds it. It bound 0 (wire review 16 item 23).
            let mantissa = trimmed.split(['e', 'E']).next().unwrap_or("");
            if in_type == 0.0
                && *pg_type != Type::NUMERIC
                && mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b))
            {
                return Err(out_of_range(name));
            }
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match pg_bool(text) {
            Some(b) => Ok(Value::from_i64(i64::from(b))),
            None => Err(syntax("boolean")),
        },
        Type::BYTEA => {
            // PostgreSQL text format for bytea uses \x hex encoding
            if let Some(hex_str) = text.strip_prefix("\\x") {
                let data =
                    decode_hex(hex_str).map_err(|e| PgWireError::UserError(error("22023", e)))?;
                Ok(Value::from_blob(data))
            } else {
                // byteain's escape format: `\\` one backslash, `\ooo` (first digit 0-3) one
                // byte, any other backslash 22P02, every other byte itself. It stored the raw
                // bytes, backslashes included (wire review 13 item 15).
                decode_bytea_escape(bytes).map(Value::from_blob).ok_or_else(|| {
                    PgWireError::UserError(error(
                        "22P02",
                        "invalid input syntax for type bytea".to_string(),
                    ))
                })
            }
        }
        // TEXT, VARCHAR, UNKNOWN and all other types: the text as given (a parameter nothing
        // types is text, as in PostgreSQL; wire review 4 item 3).
        _ => Ok(Value::from_text(text.to_owned())),
    }
}

/// A text-format array parameter, read as PostgreSQL's array_in reads a one-dimensional array:
/// `{` elements `}` separated by commas, each double-quoted or not (a backslash escapes the next
/// character in either; an unquoted element loses its surrounding whitespace, and `NULL` in any
/// case is NULL); `{}` is empty. Each element is read by the element type's own text rule
/// ([`pg_bytes_to_value`]), and the array is bound as the engine's record-format array blob (as
/// core's values_to_record_blob builds it, from its public parts). The text was bound as is and the
/// engine guessed each element's type from its spelling, so bool[] '{t}' matched no true row,
/// text[] '{1,2}' no text '1', and int4[] '{1.0}' matched 1 (wire review 10 item 4). Bad input is
/// 22P02; a multidimensional or dimension-decorated array is refused (0A000).
fn pg_array_to_value(text: &str, element: &Type) -> PgWireResult<Value> {
    let malformed = || {
        PgWireError::UserError(error(
            "22P02",
            format!("malformed array literal: \"{text}\""),
        ))
    };
    // PostgreSQL's array_in reads blanks as its scanner does (scanner_isspace, \v included; wire
    // review 13 item 16: \v was not one).
    let blank = |c: char| c.is_ascii() && pg_space(c as u8);
    let s = text.trim_matches(blank);
    if s.starts_with('[') {
        return Err(PgWireError::UserError(error(
            "0A000",
            "array parameters with dimension information are not supported".to_string(),
        )));
    }
    let inner = s
        .strip_prefix('{')
        .and_then(|r| r.strip_suffix('}'))
        .ok_or_else(malformed)?;
    let mut values = Vec::new();
    if !inner.trim_matches(blank).is_empty() {
        let mut chars = inner.chars().peekable();
        loop {
            while chars.next_if(|c| blank(*c)).is_some() {}
            let mut item = String::new();
            // An element with any backslash escape is never SQL NULL (`NUL\L` is the string
            // NULL, as in array_in; it bound NULL: wire review 13 item 16).
            let mut escaped = false;
            let quoted = chars.next_if_eq(&'"').is_some();
            if quoted {
                loop {
                    match chars.next().ok_or_else(malformed)? {
                        '"' => break,
                        '\\' => item.push(chars.next().ok_or_else(malformed)?),
                        c => item.push(c),
                    }
                }
                while chars.next_if(|c| blank(*c)).is_some() {}
            } else {
                // Trailing whitespace is dropped, but not an escaped character's.
                let mut kept = 0;
                while let Some(c) = chars.next_if(|c| *c != ',') {
                    match c {
                        '{' => {
                            return Err(PgWireError::UserError(error(
                                "0A000",
                                "multidimensional array parameters are not supported".to_string(),
                            )))
                        }
                        // A `}` with no `{` open is malformed, as array_in answers (22P02; it was
                        // the multidimensional refusal: wire review 13 item 16).
                        '}' => return Err(malformed()),
                        '"' => return Err(malformed()),
                        '\\' => {
                            item.push(chars.next().ok_or_else(malformed)?);
                            kept = item.len();
                            escaped = true;
                        }
                        c => {
                            item.push(c);
                            if !blank(c) {
                                kept = item.len();
                            }
                        }
                    }
                }
                item.truncate(kept);
                if item.is_empty() {
                    return Err(malformed());
                }
            }
            // An element its type cannot read fails with that type's own error, as array_in fails
            // with its element's input function's: it was always a generic 22P02, so an int4
            // element out of range or a bytea element's bad digit had the wrong code (wire review
            // 13 LOW 17).
            values.push(if !quoted && !escaped && item.eq_ignore_ascii_case("null") {
                Value::Null
            } else {
                pg_bytes_to_value(item.as_bytes(), element)?
            });
            match chars.next() {
                None => break,
                Some(',') => {}
                Some(_) => return Err(malformed()),
            }
        }
    }
    let record = turso_core::types::ImmutableRecord::from_values(values.as_slice(), values.len())
        .map_err(|e| PgWireError::UserError(engine_info(&e)))?;
    Ok(Value::Blob(record.into_payload()))
}

/// An integer as PostgreSQL's integer input functions read one (pg_strtoint64_safe, PostgreSQL 16
/// and later): blanks around it, an optional sign, then decimal digits, or 0x / 0o / 0b and that
/// base's digits, with one underscore allowed between two digits, or between the prefix and the
/// first digit. `Some(None)` for a well-formed
/// integer past i64 (out of range), `None` for bad syntax.
fn pg_integer(text: &str) -> Option<Option<i64>> {
    let s = text.trim_matches(|c: char| c.is_ascii() && pg_space(c as u8));
    let (negative, s) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (radix, digits) = match s.get(..2).map(|p| p.to_ascii_lowercase()).as_deref() {
        Some("0x") => (16, &s[2..]),
        Some("0o") => (8, &s[2..]),
        Some("0b") => (2, &s[2..]),
        _ => (10, s),
    };
    // An underscore may follow a base prefix ('0b_10_0101' is 37), never begin a decimal
    // (wire review 16 item 23: it was refused in every base).
    if digits.is_empty() || (radix == 10 && digits.starts_with('_')) || digits.ends_with('_') {
        return None;
    }
    let mut magnitude: u128 = 0;
    let mut previous_underscore = false;
    for c in digits.chars() {
        if c == '_' {
            if previous_underscore {
                return None;
            }
            previous_underscore = true;
            continue;
        }
        previous_underscore = false;
        let d = c.to_digit(radix)?;
        // Saturates past every i64 magnitude, which is all an out-of-range verdict needs.
        magnitude = magnitude
            .saturating_mul(u128::from(radix))
            .saturating_add(u128::from(d));
    }
    let limit = if negative {
        u128::from(i64::MIN.unsigned_abs())
    } else {
        i64::MAX as u128
    };
    if magnitude > limit {
        return Some(None);
    }
    let value = if negative {
        (magnitude as i128).wrapping_neg() as i64
    } else {
        magnitude as i64
    };
    Some(Some(value))
}

/// PostgreSQL's escape-format bytea text, as its byteain reads it (PostgreSQL 18.6 recorded,
/// artie raw loud-20261015/pg18-diff/pg18-bytea.out): `\\` is a backslash, `\` and three octal
/// digits whose first is 0-3 is that byte, any other backslash is invalid (None).
fn decode_bytea_escape(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        match bytes.get(i + 1..i + 4) {
            _ if bytes.get(i + 1) == Some(&b'\\') => {
                out.push(b'\\');
                i += 2;
            }
            Some(&[a, b, c])
                if (b'0'..=b'3').contains(&a)
                    && (b'0'..=b'7').contains(&b)
                    && (b'0'..=b'7').contains(&c) =>
            {
                out.push(((a - b'0') << 6) | ((b - b'0') << 3) | (c - b'0'));
                i += 4;
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Decode PostgreSQL's hex bytea text (what follows `\x`) as its byteain reads it: pairs of hex
/// digits, whitespace skipped between pairs, its messages on bad input (22023 at the caller, as
/// byteain raises them). Read
/// by character, never sliced: `&hex[i..i + 2]` cut a multi-byte character and panicked, and under
/// the release build's panic=abort one client's Bind ended every session (wire review 9 item 5).
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    let digit = |c: char| {
        c.to_digit(16)
            .ok_or_else(|| format!("invalid hexadecimal digit: \"{c}\""))
    };
    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let Some(c) = chars.next() {
        if matches!(c, ' ' | '\t' | '\n' | '\r') {
            continue;
        }
        let high = digit(c)?;
        let low = match chars.next() {
            Some(d) => digit(d)?,
            None => return Err("invalid hexadecimal data: odd number of digits".to_owned()),
        };
        out.push((high * 16 + low) as u8);
    }
    Ok(out)
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
        // A float4 column is written as float4 (float4out's digits and its exponent threshold,
        // both protocols), a float8 as float8.
        Value::Numeric(turso_core::Numeric::Float(f)) if *pg_type == Type::FLOAT4 => encoder
            .encode_field(&(f64::from(*f) as f32))
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
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
        Value::Blob(b) if turso_pg::element_of(pg_type.oid()).is_some() => {
            let element = turso_pg::element_of(pg_type.oid())
                .and_then(Type::from_oid)
                .expect("a known PostgreSQL array element");
            let text = array_result_text(b, &element)?;
            encoder
                .encode_field_with_type_and_format(
                    &text.as_str(),
                    &Type::TEXT,
                    FieldFormat::Text,
                    &FormatOptions::default(),
                )
                .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
        }
        Value::Blob(b) => encoder
            .encode_field(&b.as_slice())
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
    }
}

/// Array elements use the same text encodings as scalar results; only their quoting is different.
fn array_result_text(blob: &[u8], element: &Type) -> turso_core::Result<String> {
    let fields = Arc::new(vec![FieldInfo::new(
        String::new(),
        None,
        None,
        element.clone(),
        FieldFormat::Text,
    )]);
    let mut text = String::from("{");
    for (index, value) in turso_core::types::ValueIterator::new(blob)?.enumerate() {
        if index > 0 {
            text.push(',');
        }
        let value = value?.to_owned()?;
        if matches!(value, Value::Null) {
            text.push_str("NULL");
            continue;
        }
        let mut scalar = DataRowEncoder::new(fields.clone());
        encode_value(&mut scalar, &value, element)?;
        let row = scalar
            .finish()
            .map_err(|e| LimboError::InternalError(e.to_string()))?;
        let value = std::str::from_utf8(&row.data[4..])
            .map_err(|e| LimboError::InternalError(e.to_string()))?;
        let quote = value.is_empty()
            || value.eq_ignore_ascii_case("null")
            || value
                .chars()
                .any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\') || c.is_whitespace());
        if quote {
            text.push('"');
        }
        for ch in value.chars() {
            if matches!(ch, '"' | '\\') {
                text.push('\\');
            }
            text.push(ch);
        }
        if quote {
            text.push('"');
        }
    }
    text.push('}');
    Ok(text)
}

fn sqlite_type_to_pg_type(type_str: &str) -> Type {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2" | "SERIAL" | "SMALLSERIAL" => Type::INT4,
        "BIGINT" | "INT8" | "BIGSERIAL" => Type::INT8,
        // FLOAT4 is a real column's declaration. REAL stays float8: it is also the name the engine's
        // get_column_type_info gives a computed expression of REAL affinity (`SELECT 2.5`,
        // random()), which PostgreSQL types float8; an old REAL table column is typed float4 by
        // the frontend's declared-type walk (catalog's sqlite_type_to_pg_oid) before this.
        "FLOAT4" => Type::FLOAT4,
        "REAL" | "FLOAT" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC" | "DECIMAL" => {
            Type::FLOAT8
        }
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

/// The completion tag, before its row count, of an INSERT, UPDATE or DELETE that returns rows
/// (RETURNING), read from the statement's first token as [`command_tag`] reads it: `INSERT 0`,
/// `UPDATE`, `DELETE`; None for any other statement, whose rows keep the query tag (SELECT n).
/// Blind spot, as command_tag's: a data-modifying statement behind WITH keeps SELECT n.
fn returning_tag(query: &str) -> Option<&'static str> {
    let start = skip_blank(query.as_bytes(), 0)?;
    let word: String = query[start..]
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    match word.to_ascii_uppercase().as_str() {
        "INSERT" => Some("INSERT 0"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        _ => None,
    }
}

fn command_tag(query: &str, affected_rows: usize) -> Tag {
    // From the statement's first token, as PostgreSQL's lexer reads it: whitespace, comments and
    // empty statements before it are skipped (skip_blank, the verb reader's helper), so `;COMMIT`
    // in an open block is tagged COMMIT, not OK (wire review 17 item 7's pin found it).
    let start = skip_blank(query.as_bytes(), 0).unwrap_or(query.len());
    let upper = query[start..].trim().to_uppercase();
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

#[cfg(test)]
mod crash_batch_tests;

#[cfg(test)]
mod sqlstate_tests;

/// This crate's tests count allocations per thread (the per-statement allocation budget's
/// in-process tests).
#[cfg(test)]
#[global_allocator]
static TEST_ALLOC: counters::CountingAlloc = counters::CountingAlloc;

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
        Session::new(Arc::new(Shared::new(
            db,
            path,
            1,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        )))
    }

    /// The fast path's inline replies are, byte for byte, the messages pgwire encodes for the same
    /// reply through one_row (RowDescription, DataRow, CommandComplete `SELECT 1`), for a text and
    /// an int8 column, an empty and a long value (L5, vendored change 8).
    #[test]
    fn inline_replies_are_the_messages_pgwire_encodes() {
        use futures::StreamExt;
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let encoded = |r: Response| -> Vec<u8> {
            let Response::Query(mut results) = r else {
                panic!("not a query response")
            };
            let mut buf = bytes::BytesMut::new();
            let fields = results.row_schema().iter().map(Into::into).collect();
            PgWireBackendMessage::RowDescription(RowDescription::new(fields))
                .encode(&mut buf)
                .unwrap();
            let tag = results.command_tag().to_owned();
            let mut rows = 0;
            rt.block_on(async {
                let data = results.data_rows();
                while let Some(row) = data.next().await {
                    PgWireBackendMessage::DataRow(row.unwrap())
                        .encode(&mut buf)
                        .unwrap();
                    rows += 1;
                }
            });
            PgWireBackendMessage::CommandComplete(Tag::new(&tag).with_rows(rows).into())
                .encode(&mut buf)
                .unwrap();
            buf.to_vec()
        };
        let long = "x".repeat(150);
        for (f, value) in [
            ("turso_branch_current", "main"),
            ("turso_branch_switch", ""),
            ("turso_branch_delete", long.as_str()),
        ] {
            let inline = inline_one_row(f, &Type::TEXT, value.as_bytes()).expect("fits");
            let slow = encoded(one_text(f, value, &Format::UnifiedText).unwrap());
            assert_eq!(inline.bytes(), &slow[..], "{f} {value:?}");
        }
        for v in [0i64, 7, 1234567890123, -5] {
            let mut digits = [0u8; 20];
            let inline =
                inline_one_row("turso_branch_create", &Type::INT8, int_text(v, &mut digits)).unwrap();
            let slow = encoded(one_int8("turso_branch_create", v, &Format::UnifiedText).unwrap());
            assert_eq!(inline.bytes(), &slow[..], "create {v}");
        }
        assert!(inline_one_row("f", &Type::TEXT, &[b'x'; 300]).is_none(), "too long fits");
    }

    /// The inline CommandComplete of an INSERT, UPDATE or DELETE is, byte for byte, the message
    /// pgwire encodes for command_tag's Tag of the same statement and count (vendored change 8),
    /// in any case and after blanks and comments; any other verb has none.
    #[test]
    fn inline_dml_completes_are_the_messages_pgwire_encodes() {
        for (sql, rows) in [
            ("UPDATE t SET v = 1", 1usize),
            ("  update t set v = 1", 0),
            ("/* c */ DELETE FROM t", 12345),
            ("insert into t values (1)", 1),
            ("INSERT INTO t SELECT 1", 7),
        ] {
            let inline = inline_dml_complete(sql, rows).expect("a DML verb");
            let mut buf = bytes::BytesMut::new();
            PgWireBackendMessage::CommandComplete(command_tag(sql, rows).into())
                .encode(&mut buf)
                .unwrap();
            assert_eq!(inline.bytes(), &buf[..], "{sql} {rows}");
        }
        for sql in ["SELECT 1", "CREATE TABLE x (a int)", "WITH x AS (SELECT 1) UPDATE t SET v = 1"] {
            assert!(inline_dml_complete(sql, 1).is_none(), "{sql}");
        }
    }

    /// L5's allocation budget in process (lead 2026-10-15; budget.py work's pre-registered rule,
    /// DECISIONS 2f239db68a, at one statement): a branch call allocates, beyond what the engine's
    /// own call allocates (create_branch, connect_named, drop_branch; none for current and for a
    /// switch back to the kept trunk), no more than the empty query does. Counted per thread
    /// (counters::thread_allocations) around Session::on_query with a client that stores the reply
    /// messages in reserved room, as the socket's codec encodes them into its reused buffer. At
    /// 154e357e6 the wire paid 15 to 36 allocations a call over the engine.
    #[test]
    fn a_branch_call_allocates_no_more_than_an_empty_query_beyond_the_engine() {
        let n = counters::thread_allocations;
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        ok(&s, "SELECT turso_branch_create('b')");
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let mut c = MemClient::new();
        c.replies.reserve(64);
        let mut wire = |sql: &str| {
            let q = pgwire::messages::simplequery::Query::new(sql.to_string());
            c.replies.clear();
            c.info.state = PgWireConnectionState::ReadyForQuery;
            let a0 = n();
            rt.block_on(s.on_query(&mut c, q)).unwrap();
            let used = n() - a0;
            assert!(
                !c.replies.iter().any(|m| matches!(m, PgWireBackendMessage::ErrorResponse(_))),
                "{sql}: {:?}",
                c.replies
            );
            used
        };
        // Warm: every statement once, so a first use's one-time setup is not counted.
        for sql in [
            ";",
            "SELECT turso_branch_current()",
            "SELECT turso_branch_switch('b')",
            "SELECT turso_branch_switch('main')",
            "SELECT turso_branch_create('w')",
            "SELECT turso_branch_delete('w')",
        ] {
            wire(sql);
        }
        let empty = wire(";");
        let measure_engine = |f: &dyn Fn()| {
            let a0 = n();
            f();
            n() - a0
        };
        let mut over = Vec::new();
        let mut check = |what: &str, wire_cost: u64, engine_cost: u64| {
            let excess = wire_cost.saturating_sub(engine_cost);
            if excess > empty {
                over.push(format!(
                    "{what}: wire {wire_cost}, engine {engine_cost}, excess {excess} > the empty query's {empty}"
                ));
            }
        };
        check("current", wire("SELECT turso_branch_current()"), 0);
        // The engine's connect first: the session on b would hold it (BranchInUse).
        let engine_switch = measure_engine(&|| drop(s.shared.db.connect_named("b").unwrap()));
        check("switch b", wire("SELECT turso_branch_switch('b')"), engine_switch);
        check("switch main", wire("SELECT turso_branch_switch('main')"), 0);
        // On the session's own connection, as the wire's create runs (a fresh connection's create
        // allocated one fewer: the engine's cost is the connection's, and is not the wire's).
        let own = s.current(&mut s.state()).unwrap();
        let engine_create = measure_engine(&|| {
            own.inner().create_branch("e1").unwrap();
        });
        let engine_delete = measure_engine(&|| {
            s.shared.db.drop_branch("e1").unwrap();
        });
        check("create", wire("SELECT turso_branch_create('c')"), engine_create);
        check("delete", wire("SELECT turso_branch_delete('c')"), engine_delete);
        assert!(over.is_empty(), "over the budget: {over:#?}");
    }

    /// A session's registered socket dup is given back however the session ends, a panic in an
    /// unwinding build included (Claude C37 M3: the deregistration ran only on a normal return).
    #[test]
    fn a_panicking_session_gives_its_registered_socket_back() {
        let sockets = SessionSockets::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _registered = Registered::new(&sockets, socket);
            assert_eq!(sockets.open.lock().unwrap().len(), 1, "premise: registered");
            panic!("the session panics");
        }));
        assert!(unwound.is_err(), "premise: the session panicked");
        assert_eq!(sockets.open.lock().unwrap().len(), 0, "the dup leaked");
    }

    /// The startup deadline bounds a session's WRITES as well as its reads (C37 H1): a client that
    /// never reads, so that the server's replies fill its window, cannot hold the session thread
    /// in a blocking write past the deadline. Here 64 MiB go to a peer that never reads, through
    /// BlockingTcp with a 300 ms deadline: the write must fail as timed out within 5 s (it blocked
    /// for good).
    #[test]
    fn a_startup_write_to_a_client_that_never_reads_ends_at_the_deadline() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server_side, _) = listener.accept().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut io = pgwire::tokio::server::BlockingTcp {
                socket: server_side,
                deadline: Some(std::time::Instant::now() + std::time::Duration::from_millis(300)),
                timed_out: false,
            };
            let big = vec![b'N'; 64 << 20];
            let r = futures::executor::block_on(tokio::io::AsyncWriteExt::write_all(&mut io, &big));
            let _ = tx.send((r.map_err(|e| e.kind()), io.timed_out));
        });
        let got = rx.recv_timeout(std::time::Duration::from_secs(5));
        drop(client);
        match got {
            Ok((Err(kind), timed_out)) => {
                assert_eq!(kind, std::io::ErrorKind::TimedOut, "the write's error");
                assert!(timed_out, "timed_out is set");
            }
            Ok((Ok(()), _)) => panic!("64 MiB were written to a peer that never reads"),
            Err(_) => panic!("the write to a peer that never reads still blocks 5 s after a 300 ms deadline"),
        }
    }

    /// L5's allocation budget for a write over the simple protocol (the cycle's write step,
    /// budget.py work): an UPDATE allocates, beyond the engine's own prepare and run of the same
    /// statement on the session's connection, no more than the empty query does. At 4f28b42c6 it
    /// paid 5: the statement split's Vec and String, the command tag's upper-cased copy of the
    /// query, its Tag String and the CommandComplete's.
    #[test]
    fn a_write_allocates_no_more_than_an_empty_query_beyond_the_engine() {
        let n = counters::thread_allocations;
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        ok(&s, "INSERT INTO t VALUES (1, 0)");
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let mut c = MemClient::new();
        c.replies.reserve(64);
        let wire = |c: &mut MemClient, sql: &str| {
            let q = pgwire::messages::simplequery::Query::new(sql.to_string());
            c.replies.clear();
            c.info.state = PgWireConnectionState::ReadyForQuery;
            let a0 = n();
            rt.block_on(s.on_query(c, q)).unwrap();
            n() - a0
        };
        // Every wire write must have happened, checked outside the counted window (Codex C35 MED 1:
        // with the UPDATE answered as an empty query, the budget passed): its reply carries exactly
        // the CommandComplete "UPDATE 1" and no ErrorResponse, and the committed row holds its new
        // value, read by a connection of its own.
        let written = |c: &MemClient, want: i64| {
            let mut tags = Vec::new();
            for m in &c.replies {
                match m {
                    PgWireBackendMessage::ErrorResponse(e) => panic!("the write failed: {e:?}"),
                    PgWireBackendMessage::CommandComplete(cc) => tags.push(cc.tag.clone()),
                    PgWireBackendMessage::Inline(m) if m.bytes().first() == Some(&b'C') => {
                        let b = m.bytes();
                        tags.push(String::from_utf8_lossy(&b[5..b.len() - 1]).into_owned());
                    }
                    _ => {}
                }
            }
            assert_eq!(tags, ["UPDATE 1"], "the write's CommandComplete: {:?}", c.replies);
            let rows = s
                .shared
                .db
                .connect()
                .unwrap()
                .prepare("SELECT v FROM t WHERE id = 1")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![Value::from_i64(want)]], "the row after the write");
        };
        let sql = "UPDATE t SET v = v + 1 WHERE id = 1";
        let mut want = 0;
        for _ in 0..3 {
            wire(&mut c, ";");
            wire(&mut c, sql);
            want += 1;
            written(&c, want);
        }
        let empty = wire(&mut c, ";");
        let own = s.current(&mut s.state()).unwrap();
        let a0 = n();
        let mut stmt = own.prepare(sql).unwrap();
        stmt.run_ignore_rows().unwrap();
        drop(stmt);
        let engine = n() - a0;
        want += 1;
        let write = wire(&mut c, sql);
        want += 1;
        written(&c, want);
        assert!(
            write.saturating_sub(engine) <= empty,
            "the write: wire {write}, engine {engine}, excess {} > the empty query's {empty}",
            write.saturating_sub(engine)
        );
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

    /// A switch to the branch the session is on is a no-op only while that branch is still the one
    /// the name names: a branch released behind the session (by any path that does not pass this
    /// server's in-use map) and re-created under its name is another branch, and the switch lands
    /// on it. At 472023b72 the switch compared names and stayed on the released branch (wire
    /// review 1 item 6). The engine now refuses to drop a branch a connection is open on
    /// (BranchInUse, op Drop), so this asserts that refusal and that the switch stays on the
    /// session's branch.
    #[test]
    fn a_switch_to_a_recreated_name_lands_on_the_new_branch() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY)");
        ok(&s, "SELECT turso_branch_create('h')");
        ok(&s, "SELECT turso_branch_switch('h')");
        let on = |s: &Session| {
            s.state()
                .branch
                .as_ref()
                .and_then(|(_, c)| c.inner().branch_id())
        };
        let old = on(&s);
        assert!(old.is_some(), "premise: the session is on branch h");
        // The engine now makes the hazard unrepresentable: a branch a connection is open on cannot
        // be dropped, so it cannot be re-created behind the session (ruled c9c821dd8d, P3).
        let refused = s.shared.db.drop_branch("h");
        assert!(
            matches!(
                refused,
                Err(LimboError::BranchInUse {
                    op: turso_core::BranchOp::Drop,
                    ..
                })
            ),
            "the engine dropped a branch a connection is open on: {refused:?}"
        );
        ok(&s, "SELECT turso_branch_switch('h')");
        assert_eq!(on(&s), old, "the switch left the session's branch");
    }

    /// Wire review 16 item 22, a pin (no red: NoopStartupHandler never authenticates, so no client
    /// reaches the state; serve_session's concrete handler types admit no password-requiring stub):
    /// before startup completes, a malformed message of any kind and every extended-protocol
    /// message is FATAL 08P01, never the extended skip whose Sync set ReadyForQuery; a simple Query
    /// is left to the startup handler, and once ready nothing is refused here. LOUD-OWED mutants:
    /// deleting the guard's call, or its `is_extended_query` arm, must let a session pass.
    #[test]
    fn nothing_but_startup_is_read_before_startup_completes() {
        use PgWireConnectionState as St;
        let malformed = |kind: u8| PgWireFrontendMessage::Malformed(kind, "08P01", "short".into());
        let parse = || PgWireFrontendMessage::Parse(Parse::new(None, "SELECT 1".into(), vec![]));
        let query = || {
            PgWireFrontendMessage::Query(pgwire::messages::simplequery::Query::new(
                "SELECT 1".to_string(),
            ))
        };
        for state in [St::AwaitingStartup, St::AuthenticationInProgress] {
            for msg in [
                malformed(b'P'),
                malformed(b'B'),
                malformed(b'H'),
                malformed(b'S'),
                malformed(b'Q'),
                parse(),
                PgWireFrontendMessage::Sync(PgSync::new()),
            ] {
                let info = refused_before_startup(&state, &msg)
                    .unwrap_or_else(|| panic!("{state:?}: {msg:?} was not refused"));
                assert_eq!(
                    (info.severity.as_str(), info.code.as_str()),
                    ("FATAL", "08P01"),
                    "{state:?}: {msg:?}"
                );
            }
            assert!(
                refused_before_startup(&state, &query()).is_none(),
                "{state:?}: a simple Query is the startup handler's"
            );
        }
        for msg in [malformed(b'P'), parse(), query()] {
            assert!(
                refused_before_startup(&St::ReadyForQuery, &msg).is_none(),
                "ready: {msg:?}"
            );
        }
    }

    /// The 3B001 arm of a failed block (Session::run): a block failed by an error that made the
    /// engine roll the WHOLE transaction back, savepoints with it, so the engine holds no
    /// transaction while the session's block is still failed. ROLLBACK TO a savepoint of that
    /// block is 3B001 (it does not exist, as PostgreSQL says of one that does not), the block stays
    /// failed (25P02), and ROLLBACK ends it. No statement known today brings the engine there (the
    /// sum() overflow that did no longer rolls the transaction back: wire 3b9d4900e, ruled
    /// c9c821dd8d P7), so the state is built directly: the engine's transaction is rolled back
    /// behind the session, and the block marked failed. A pin of the arm, which no other test
    /// reached (the lead's ruling on P7's finding); its mutant, the arm deleted, sends the ROLLBACK
    /// TO to the engine, XX000.
    #[test]
    fn a_rollback_to_in_a_block_the_engine_ended_is_3b001() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT)");
        ok(&s, "BEGIN");
        ok(&s, "INSERT INTO t VALUES (1)");
        ok(&s, "SAVEPOINT s");
        {
            let mut st = s.state();
            let conn = s.current(&mut st).unwrap();
            conn.engine_tx(TxStmt::Rollback)
                .expect("premise: the engine rolls its transaction back");
            assert!(
                conn.inner().get_auto_commit(),
                "premise: the engine holds no transaction"
            );
            st.aborted = true;
        }
        assert_eq!(
            errors(&s, "ROLLBACK TO s"),
            vec![("3B001".to_string(), "ERROR".to_string())]
        );
        assert_eq!(
            errors(&s, "SELECT 1"),
            vec![("25P02".to_string(), "ERROR".to_string())],
            "the block stays failed"
        );
        assert!(errors(&s, "ROLLBACK").is_empty(), "ROLLBACK ends the block");
        assert!(
            matches!(s.transaction_status(), TransactionStatus::Idle),
            "the session is idle after the ROLLBACK"
        );
    }

    /// The errors `sql` answers, as (code, severity).
    fn errors(session: &Session, sql: &str) -> Vec<(String, String)> {
        session
            .simple(sql)
            .into_iter()
            .filter_map(|r| match r {
                Response::Error(e) => Some((e.code.clone(), e.severity.clone())),
                _ => None,
            })
            .collect()
    }

    /// Whether table `name` is committed, read by a connection of its own.
    fn committed_table(s: &Session, name: &str) -> bool {
        let conn = s.shared.db.connect().unwrap();
        let rows = conn
            .prepare(format!(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = '{name}'"
            ))
            .unwrap()
            .run_collect_rows()
            .unwrap();
        rows != vec![vec![Value::from_i64(0)]]
    }

    const FATAL: (&str, &str) = ("08006", "FATAL");

    /// Wire review 16 item 14: an undo that fails is never swallowed. A CREATE TABLE whose key onto
    /// itself does not resolve (42830 after the CREATE) is undone in the transaction it ran in;
    /// when that undo fails (the failpoint), the engine still holds the table in an open
    /// transaction, so the connection is broken: FATAL 08006, and the session ends rather than
    /// serve the next statement, which joined that transaction (`let _ = rollback`: the client's
    /// next COMMIT committed the refused table). In autocommit (the statement's own BEGIN), and in a
    /// client's block (its savepoint).
    #[test]
    fn a_failed_undo_of_a_refused_create_breaks_the_connection() {
        let refused = "CREATE TABLE c(id INT, x INT REFERENCES c(id))";
        for in_block in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let s = session(&dir);
            ok(&s, "CREATE TABLE z(id INT)");
            assert_eq!(
                errors(&s, refused),
                vec![("42830".to_string(), "ERROR".to_string())],
                "premise: the key onto c(id), which is not unique, is refused after the CREATE"
            );
            assert!(!committed_table(&s, "c"), "premise: the refused c was undone");
            if in_block {
                ok(&s, "BEGIN");
            }
            turso_pg::fail_next_undo();
            let got = errors(&s, refused);
            let fatal = vec![(FATAL.0.to_string(), FATAL.1.to_string())];
            assert_eq!(got, fatal, "in_block={in_block}: the failed undo was swallowed");
            assert_eq!(
                errors(&s, "COMMIT"),
                fatal,
                "in_block={in_block}: the session served a statement after its undo failed"
            );
            assert!(
                !committed_table(&s, "c"),
                "in_block={in_block}: the refused table was committed"
            );
        }
    }

    /// Wire review 16 item 14, the implicit block's half: a multi-statement query whose second
    /// statement fails is rolled back whole; when that rollback fails (the failpoint), the engine
    /// still holds the block's transaction, so the connection is broken and the next statement is
    /// FATAL 08006, never run inside it (`let _ = engine_tx(Rollback)`: the next COMMIT committed
    /// the block's first statement).
    #[test]
    fn a_failed_rollback_of_an_implicit_block_breaks_the_connection() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE p(id INT PRIMARY KEY)");
        ok(&s, "INSERT INTO p VALUES (1)");
        turso_pg::fail_next_undo();
        let got = errors(&s, "CREATE TABLE kept(id INT); INSERT INTO p VALUES (1)");
        assert!(
            got.iter().any(|(code, _)| code == "23505"),
            "premise: the second statement fails: {got:?}"
        );
        assert_eq!(
            errors(&s, "COMMIT"),
            vec![(FATAL.0.to_string(), FATAL.1.to_string())],
            "the session served a statement after its block's rollback failed"
        );
        assert!(
            !committed_table(&s, "kept"),
            "the failed block's first statement was committed"
        );
    }

    /// An extended-protocol statement is prepared once: Execute runs the statement its Describe
    /// prepared, instead of parsing, translating and compiling it a second time (wire review 1
    /// item 12). Two libpg_query calls before: Describe's and Execute's.
    #[test]
    fn a_described_statement_is_not_prepared_again_at_execute() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        ok(&s, "INSERT INTO t VALUES (1, 7)");
        let sql = "SELECT v FROM t WHERE id = 1";
        let stored = Arc::new(StoredStatement::new(
            String::new(),
            Parsed::new(sql.to_string()),
            vec![],
        ));
        let bind = pgwire::messages::extendedquery::Bind::new(None, None, vec![], vec![], vec![]);
        let portal = Portal::try_new(&bind, stored).unwrap();
        let before = turso_pg_parser::libpg_query_calls();
        s.describe_prepare(sql).unwrap();
        assert!(s
            .run(sql, None, Some(&portal), &Format::UnifiedText)
            .is_ok());
        assert_eq!(
            turso_pg_parser::libpg_query_calls() - before,
            1,
            "libpg_query calls for Describe then Execute"
        );
    }

    /// bind_portal_parameters refuses parameter types and values that differ in number, where it
    /// zipped them short: a value bound under another's type, or a parameter left unbound, which
    /// the engine reads as NULL. check_bind_arity makes them equal before it is called, so this
    /// guards an invariant, refused XX000 (wire review 18 item 23). Equal counts bind (the
    /// control).
    #[test]
    fn binding_refuses_types_and_values_that_differ_in_number() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
        let sql = "SELECT v FROM t WHERE id = $1 OR v = $2";
        let stored = Arc::new(StoredStatement::new(
            String::new(),
            Parsed::new(sql.to_string()),
            vec![],
        ));
        let bind = Bind::new(
            None,
            None,
            vec![],
            vec![Some(b"1".to_vec().into()), Some(b"2".to_vec().into())],
            vec![],
        );
        let portal = Portal::try_new(&bind, stored).unwrap();
        let conn = s.current(&mut s.state()).unwrap();
        let (mut stmt, _) = conn.prepare_typed(sql).unwrap();
        assert!(
            bind_portal_parameters(&mut stmt, &portal, &[Type::INT4, Type::INT4]).is_ok(),
            "the control: two types for two values"
        );
        for types in [vec![Type::INT4], vec![Type::INT4, Type::INT4, Type::INT4]] {
            match bind_portal_parameters(&mut stmt, &portal, &types) {
                Err(PgWireError::UserError(e)) => assert_eq!(e.code, "XX000", "{types:?}"),
                other => panic!("{} types for 2 values: {other:?}", types.len()),
            }
        }
    }

    type Stmt = <Session as ExtendedQueryHandler>::Statement;

    /// A client of the extended-protocol handlers in memory: pgwire's DefaultClient holds the
    /// connection state and the portal store, and every reply is kept, in order.
    struct MemClient {
        info: pgwire::api::DefaultClient<Stmt>,
        replies: Vec<PgWireBackendMessage>,
    }

    impl MemClient {
        fn new() -> Self {
            let mut info = pgwire::api::DefaultClient::new(
                std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
                false,
            );
            info.state = PgWireConnectionState::ReadyForQuery;
            Self {
                info,
                replies: Vec::new(),
            }
        }
    }

    impl ClientInfo for MemClient {
        fn socket_addr(&self) -> std::net::SocketAddr {
            self.info.socket_addr()
        }
        fn is_secure(&self) -> bool {
            self.info.is_secure()
        }
        fn protocol_version(&self) -> pgwire::messages::ProtocolVersion {
            self.info.protocol_version()
        }
        fn set_protocol_version(&mut self, version: pgwire::messages::ProtocolVersion) {
            self.info.set_protocol_version(version)
        }
        fn pid_and_secret_key(&self) -> (i32, pgwire::messages::startup::SecretKey) {
            self.info.pid_and_secret_key()
        }
        fn set_pid_and_secret_key(
            &mut self,
            pid: i32,
            secret_key: pgwire::messages::startup::SecretKey,
        ) {
            self.info.set_pid_and_secret_key(pid, secret_key)
        }
        fn state(&self) -> PgWireConnectionState {
            self.info.state()
        }
        fn set_state(&mut self, new_state: PgWireConnectionState) {
            self.info.set_state(new_state)
        }
        fn transaction_status(&self) -> TransactionStatus {
            self.info.transaction_status()
        }
        fn set_transaction_status(&mut self, new_status: TransactionStatus) {
            self.info.set_transaction_status(new_status)
        }
        fn metadata(&self) -> &std::collections::HashMap<String, String> {
            self.info.metadata()
        }
        fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
            self.info.metadata_mut()
        }
        fn sni_server_name(&self) -> Option<&str> {
            None
        }
        fn client_certificates<'a>(
            &self,
        ) -> Option<&[pgwire::tokio::tokio_rustls::rustls::pki_types::CertificateDer<'a>]> {
            None
        }
    }

    impl ClientPortalStore for MemClient {
        type PortalStore = pgwire::api::store::MemPortalStore<Stmt>;

        fn portal_store(&self) -> &Self::PortalStore {
            &self.info.portal_store
        }
    }

    impl Sink<PgWireBackendMessage> for MemClient {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn start_send(
            mut self: std::pin::Pin<&mut Self>,
            item: PgWireBackendMessage,
        ) -> Result<(), Self::Error> {
            self.replies.push(item);
            Ok(())
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// One execution of the unnamed statement through the session's own handlers: Bind (with
    /// `param`, if any), Describe of the statement and of the portal when asked, Execute and Sync.
    /// Returns the libpg_query calls it made; an ErrorResponse fails the test, and so does a round
    /// that did not run: it must answer BindComplete, a ParameterDescription and a RowDescription
    /// when described, one DataRow and `CommandComplete SELECT 1`, so an Execute that returned
    /// early cannot pass as zero calls (wire review 16 item 25).
    async fn execute_unnamed(
        s: &Session,
        c: &mut MemClient,
        param: Option<&str>,
        describe: bool,
    ) -> u64 {
        let parameters = param
            .map(|p| vec![Some(p.as_bytes().to_vec().into())])
            .unwrap_or_default();
        let before = turso_pg_parser::libpg_query_calls();
        s.on_bind(c, Bind::new(None, None, vec![], parameters, vec![]))
            .await
            .unwrap();
        if describe {
            s.on_describe(c, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
                .await
                .unwrap();
            s.on_describe(c, Describe::new(TARGET_TYPE_BYTE_PORTAL, None))
                .await
                .unwrap();
        }
        s.on_execute(c, Execute::new(None, 0)).await.unwrap();
        s.on_sync(c, PgSync::new()).await.unwrap();
        let calls = turso_pg_parser::libpg_query_calls() - before;
        let (mut bound, mut params, mut rows_described, mut rows, mut tags) = (0, 0, 0, 0, vec![]);
        for reply in c.replies.drain(..) {
            match reply {
                PgWireBackendMessage::ErrorResponse(e) => panic!("an error in the round: {e:?}"),
                PgWireBackendMessage::BindComplete(_) => bound += 1,
                PgWireBackendMessage::ParameterDescription(_) => params += 1,
                PgWireBackendMessage::RowDescription(_) => rows_described += 1,
                PgWireBackendMessage::DataRow(_) => rows += 1,
                PgWireBackendMessage::CommandComplete(done) => tags.push(done.tag),
                _ => {}
            }
        }
        assert_eq!(bound, 1, "the round's BindComplete");
        assert_eq!(rows, 1, "the round's one row");
        assert_eq!(tags, vec!["SELECT 1".to_string()], "the round's CommandComplete");
        if describe {
            assert_eq!(params, 1, "the statement Describe's ParameterDescription");
            assert_eq!(rows_described, 2, "a RowDescription per Describe");
        }
        calls
    }

    /// Wire review 13 items 6 and 7: an extended-protocol statement is classified as a branch call
    /// or not once, at Parse, through the session's own on_parse, on_bind, on_describe, on_execute
    /// and on_sync. A fast form (`turso_branch_create($1)`, `$1::text`) makes no libpg_query call
    /// at Parse or in any execution (DECISIONS L5); a slow form (a comment) makes at most one at
    /// Parse and none per execution, Describes included; an ordinary statement whose text mentions
    /// the prefix makes at most one at Parse and exactly one per execution (the engine's prepare).
    /// Bind, each Describe and Execute read the text again, one call each: 4 per execution of the
    /// slow form with both Describes, 3 for the ordinary statement (Bind, Execute's reading, the
    /// engine's prepare). The L5 counter had been read only through the simple protocol and a
    /// hand-built portal.
    #[test]
    fn an_extended_statement_is_classified_once_at_parse() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        // The engine refuses to branch an empty database (no committed page 1): a table first
        // (ruled c9c821dd8d, P2).
        ok(&s, "CREATE TABLE t(id INT)");
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = MemClient::new();
            for (n, (sql, param, parse_at_most, per_execution)) in [
                ("SELECT turso_branch_create($1)", true, 0, 0),
                ("SELECT turso_branch_create($1::text)", true, 0, 0),
                ("SELECT turso_branch_current() /* c */", false, 1, 0),
                ("SELECT 'turso_branch_' AS s", false, 1, 1),
            ]
            .into_iter()
            .enumerate()
            {
                let before = turso_pg_parser::libpg_query_calls();
                s.on_parse(&mut c, Parse::new(None, sql.to_string(), vec![]))
                    .await
                    .unwrap();
                let at_parse = turso_pg_parser::libpg_query_calls() - before;
                assert!(
                    at_parse <= parse_at_most,
                    "{sql:?}: {at_parse} libpg_query calls at Parse"
                );
                for round in 0..3 {
                    let name = format!("x{n}_{round}");
                    let describe = per_execution == 0;
                    let calls =
                        execute_unnamed(&s, &mut c, param.then_some(name.as_str()), describe).await;
                    assert_eq!(
                        calls, per_execution,
                        "{sql:?}: libpg_query calls in execution {round}"
                    );
                    // A create that answered without creating cannot pass as zero calls.
                    if param {
                        assert!(
                            s.shared.db.branch_named(&name).unwrap().is_some(),
                            "{sql:?}: execution {round} created no branch {name}"
                        );
                    }
                }
            }
        });
    }

    /// Wire review 14 item 10: a view is parsed once per schema snapshot by the parameter-type
    /// walk, however often the statements over it are prepared and however often it is reached:
    /// v5 joins v4 twice, v4 joins v3 twice, and so on, so the walk opened 2^5 - 1 views (31
    /// parses beside the statement's own) at every prepare. The first Describe of a statement over
    /// v5 may parse each view once (6 calls with the statement's), the second, after a Sync, only
    /// the statement. Without the Sync the second Describe reused the statement the first one kept
    /// and prepared nothing (measured [6, 0]), so it showed nothing about the memo. The memo is
    /// the snapshot's: after DDL replaces the snapshot (v5 dropped and created again) and after a
    /// SET search_path, a Describe parses the views again, as the first one did (wire review 18
    /// HIGH 1; the lead's ruling 2026-10-15T02:59:32Z).
    #[test]
    fn a_view_is_parsed_once_per_schema_snapshot() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, n INT)");
        ok(&s, "CREATE VIEW v1 AS SELECT id, n FROM t");
        for k in 2..=5 {
            ok(
                &s,
                &format!(
                    "CREATE VIEW v{k} AS SELECT a.id, a.n FROM v{p} AS a JOIN v{p} AS b ON a.id = b.id",
                    p = k - 1
                ),
            );
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = MemClient::new();
            let sql = "SELECT n FROM v5 WHERE id = $1";
            s.on_parse(&mut c, Parse::new(None, sql.to_string(), vec![]))
                .await
                .unwrap();
            let mut calls = Vec::new();
            for _ in 0..2 {
                let before = turso_pg_parser::libpg_query_calls();
                s.on_describe(&mut c, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
                    .await
                    .unwrap();
                calls.push(turso_pg_parser::libpg_query_calls() - before);
                // The round ends, so the next Describe prepares the statement again rather than
                // reuse the one this Describe kept.
                s.on_sync(&mut c, PgSync::new()).await.unwrap();
                for reply in c.replies.drain(..) {
                    if let PgWireBackendMessage::ErrorResponse(e) = reply {
                        panic!("Describe: {e:?}");
                    }
                }
            }
            assert!(
                calls[0] <= 6,
                "first Describe: {} libpg_query calls, want at most 6",
                calls[0]
            );
            assert_eq!(calls[1], 1, "second Describe: the statement's parse only");
            // Invalidation: a new snapshot, then a new search path, each parse the views again.
            for change in [
                "DROP VIEW v5; CREATE VIEW v5 AS SELECT a.id, a.n FROM v4 AS a JOIN v4 AS b ON a.id = b.id",
                "SET search_path TO public",
            ] {
                ok(&s, change);
                s.on_parse(&mut c, Parse::new(None, sql.to_string(), vec![]))
                    .await
                    .unwrap();
                let before = turso_pg_parser::libpg_query_calls();
                s.on_describe(&mut c, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
                    .await
                    .unwrap();
                let after_change = turso_pg_parser::libpg_query_calls() - before;
                s.on_sync(&mut c, PgSync::new()).await.unwrap();
                for reply in c.replies.drain(..) {
                    if let PgWireBackendMessage::ErrorResponse(e) = reply {
                        panic!("Describe after {change:?}: {e:?}");
                    }
                }
                assert_eq!(
                    after_change, calls[0],
                    "after {change:?}: the views were not parsed again, as at the first Describe"
                );
            }
        });
    }

    /// Wire review 16 item 3: the parameter-type walk reads each view at most once per statement,
    /// cycles included. Eight views that each list all eight in FROM (the engine checks no view's
    /// relations at CREATE VIEW) cost about e*7! = 13,700 libpg_query parses at one Describe: a
    /// view on the walk's stack was cut, and a view whose reading saw a cut was kept nowhere, so
    /// each sibling FROM item read its view again. The chain v1..v8, each view's scalar subquery
    /// reading the one before through a derived table, doubled per level under 86aed81f8's second
    /// FROM walk (511 parses at 4db63aca4) and is linear since 14b253e05's memo: this arm pins
    /// that. At most 9 calls each: 8 views and the statement.
    #[test]
    fn a_view_is_read_once_per_statement_cycles_included() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY, x INT)");
        let all = (1..=8)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        for i in 1..=8 {
            ok(&s, &format!("CREATE VIEW w{i} AS SELECT w1.x FROM {all}"));
        }
        ok(&s, "CREATE VIEW v1 AS SELECT id, x FROM t");
        for k in 2..=8 {
            ok(
                &s,
                &format!(
                    "CREATE VIEW v{k} AS SELECT id, (SELECT max(d.x) FROM (SELECT x FROM v{p}) AS d) AS x FROM t",
                    p = k - 1
                ),
            );
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            for sql in [
                "SELECT * FROM w1 WHERE x = $1",
                "SELECT * FROM v8 WHERE x = $1",
            ] {
                let mut c = MemClient::new();
                s.on_parse(&mut c, Parse::new(None, sql.to_string(), vec![]))
                    .await
                    .unwrap();
                let before = turso_pg_parser::libpg_query_calls();
                // The cycle's Describe is refused (the engine's circular view); the walk before
                // the refusal is what is counted.
                let _ = s
                    .on_describe(&mut c, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
                    .await;
                let calls = turso_pg_parser::libpg_query_calls() - before;
                assert!(
                    calls <= 9,
                    "{sql}: {calls} libpg_query calls at Describe, want at most 9"
                );
            }
        });
    }

    /// Wire review 16 item 4: SHOW answers from an allowlist and never builds PRAGMA text from the
    /// client's identifier. `SHOW "synchronous = off"` set the session's sync mode and `SHOW
    /// "fullfsync = off"` turned F_FULLFSYNC into fsync on Apple, both while compiling (a Describe
    /// too), and `SHOW wal_checkpoint` checkpointed: each is 42704 now, and the trunk connection's
    /// settings read the same before and after. This D0 session already holds synchronous and
    /// fullfsync off, so the SHOWs name the values it does not hold (full, on), and a premise says
    /// so: a SHOW of the held value could change nothing, and the pin could not fail (ruled
    /// c9c821dd8d, P8).
    #[test]
    fn show_cannot_change_an_engine_setting() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT)");
        let pragma = |name: &str| {
            let conn = s
                .state()
                .trunk
                .as_ref()
                .expect("premise: the trunk connection is open")
                .inner()
                .clone();
            conn.prepare(format!("PRAGMA {name}"))
                .ok()
                .and_then(|mut stmt| stmt.run_collect_rows().ok())
        };
        let before = (pragma("synchronous"), pragma("fullfsync"));
        let off = Some(vec![vec![Value::from_i64(0)]]);
        assert_eq!(
            before,
            (off.clone(), off),
            "premise: the D0 session holds synchronous and fullfsync off, values the SHOWs differ from"
        );
        for sql in [
            "SHOW \"synchronous = full\"",
            "SHOW \"fullfsync = on\"",
            "SHOW wal_checkpoint",
            "SHOW synchronous",
        ] {
            let codes: Vec<String> = s
                .simple(sql)
                .into_iter()
                .filter_map(|r| match r {
                    Response::Error(e) => Some(e.code.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(codes, vec!["42704".to_string()], "{sql}");
        }
        assert_eq!(
            (pragma("synchronous"), pragma("fullfsync")),
            before,
            "a SHOW changed an engine setting"
        );
    }

    /// Wire review 17 item 7: a CHECKPOINT with a comment is the server's CHECKPOINT, read with no
    /// libpg_query call (is_checkpoint reads a comment as whitespace). Before 3067f87d8 `CHECKPOINT
    /// -- x` and `/* c */ CHECKPOINT` went to the engine as text, one parse each. A pin at the tip;
    /// the mutant is_checkpoint split on whitespace must turn it red.
    #[test]
    fn a_commented_checkpoint_makes_no_libpg_query_call() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT)");
        for sql in ["CHECKPOINT -- x", "/* c */ CHECKPOINT"] {
            let before = turso_pg_parser::libpg_query_calls();
            ok(&s, sql);
            assert_eq!(turso_pg_parser::libpg_query_calls() - before, 0, "{sql}");
        }
    }

    /// Wire review 18 item 3: a CHECKPOINT never holds up a branch create. Its store checkpoint is
    /// the fuzzy one, written on a thread of its own, so while that write is held (the hook at stage
    /// 2: rows written, not committed) a create from another session returns. The sharp checkpoint
    /// (branch_compact_now) held the store mutex through the settle, the catalog write and its
    /// syncs and the log's rewrite, so every create, switch and connect of every session waited
    /// for it. At the base the premise fails: no fuzzy checkpoint ever reaches the hold. Mutant:
    /// the sharp call.
    #[test]
    fn a_checkpoint_does_not_hold_up_a_create() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY)");
        ok(&s, "INSERT INTO t VALUES (1)");
        ok(&s, "SELECT turso_branch_create('b0')");
        let db = s.shared.db.clone();
        db.branch_checkpoint_hold(2);
        let checkpoint = {
            let shared = s.shared.clone();
            std::thread::spawn(move || ok(&Session::new(shared), "CHECKPOINT"))
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while db.branch_checkpoint_held() & 0x80 == 0 {
            if std::time::Instant::now() > deadline {
                db.branch_checkpoint_hold(0);
                panic!("premise: the CHECKPOINT's store checkpoint never reached the fuzzy hold");
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let create = {
            let shared = s.shared.clone();
            std::thread::spawn(move || {
                ok(&Session::new(shared), "SELECT turso_branch_create('b1')");
                let _ = tx.send(());
            })
        };
        let returned = rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok();
        db.branch_checkpoint_hold(0);
        checkpoint.join().unwrap();
        create.join().unwrap();
        assert!(
            returned,
            "a create waited for the CHECKPOINT's store checkpoint"
        );
    }

    /// Wire review 18 item 14 and its appendix: the parameter-type walk opens a view chain of any
    /// depth. Over v1..v40, each reading the one before, `SELECT * FROM v40 WHERE x = $1` types
    /// `$1` int4 at its first Describe, with at most 41 libpg_query calls (each view once and the
    /// statement), and again after a Sync. The walk cut every view past 32 levels
    /// (MAX_VIEW_DEPTH, a bound against the session thread's stack), so the same Describe was
    /// 42P18 cold and int4 once a shallower statement had memoised the views: the answer depended
    /// on what the connection had prepared before.
    #[test]
    fn a_view_chain_of_any_depth_is_typed() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT PRIMARY KEY)");
        ok(&s, "CREATE VIEW v1 AS SELECT id AS x FROM t");
        for k in 2..=40 {
            ok(
                &s,
                &format!("CREATE VIEW v{k} AS SELECT x FROM v{p}", p = k - 1),
            );
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = MemClient::new();
            let sql = "SELECT * FROM v40 WHERE x = $1";
            s.on_parse(&mut c, Parse::new(None, sql.to_string(), vec![]))
                .await
                .unwrap();
            for round in 0..2 {
                let before = turso_pg_parser::libpg_query_calls();
                if let Err(e) = s
                    .on_describe(&mut c, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
                    .await
                {
                    panic!("round {round}: Describe refused: {e:?}");
                }
                let calls = turso_pg_parser::libpg_query_calls() - before;
                s.on_sync(&mut c, PgSync::new()).await.unwrap();
                let params: Vec<Vec<u32>> = c
                    .replies
                    .drain(..)
                    .filter_map(|reply| match reply {
                        PgWireBackendMessage::ParameterDescription(d) => Some(d.types),
                        PgWireBackendMessage::ErrorResponse(e) => panic!("round {round}: {e:?}"),
                        _ => None,
                    })
                    .collect();
                assert_eq!(params, vec![vec![23]], "round {round}");
                if round == 0 {
                    assert!(
                        calls <= 41,
                        "{calls} libpg_query calls at the first Describe"
                    );
                }
            }
        });
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

    /// A text bytea parameter without the `\x` prefix is read in byteain's escape format, as
    /// PostgreSQL 18.6 reads it (recorded, artie raw loud-20261015/pg18-diff/pg18-bytea.out): `\\`
    /// is one backslash, `\ooo` (three octal digits, the first 0-3) one byte, and any other
    /// backslash is 22P02 "invalid input syntax for type bytea"; bytes with no backslash are
    /// themselves. It was stored as the raw bytes, backslashes included (wire review 13 item 15).
    #[test]
    fn a_bytea_parameter_is_read_in_the_escape_format() {
        let bytes = |text: &[u8]| match pg_bytes_to_value(text, &Type::BYTEA) {
            Ok(Value::Blob(b)) => Ok(b),
            Ok(other) => panic!("{other:?}"),
            Err(PgWireError::UserError(e)) => Err(e.code.clone()),
            Err(e) => panic!("{e:?}"),
        };
        assert_eq!(bytes(b"abc"), Ok(b"abc".to_vec()));
        assert_eq!(bytes(b"a\\\\b"), Ok(b"a\\b".to_vec()));
        assert_eq!(bytes(b"a\\101b"), Ok(b"aAb".to_vec()));
        assert_eq!(bytes(b"\\x4142"), Ok(b"AB".to_vec()));
        for bad in [&b"a\\qb"[..], b"a\\1b", b"a\\400b", b"a\\"] {
            assert_eq!(
                bytes(bad),
                Err("22P02".to_string()),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    /// Wire review 16 item 23: parameter decoding at the edges, as PostgreSQL's input functions read
    /// them. float4's own largest output, '3.4028235e+38' (float4out of FLT_MAX), binds, where it
    /// was refused 22003 (compared as an f64 above f32::MAX); a spelling that underflows to zero
    /// ('1e-400' for float8, '1e-50' for float4) is 22003 (recalled from float8in's ERANGE rule,
    /// not re-read), where it bound 0, while a zero spelled as zero and the smallest denormals
    /// bind; numeric keeps its range. An integer in base 2, 8 or 16 may put an underscore right
    /// after its prefix ('0b_10_0101' is 37 in PostgreSQL 16 and later, conformance
    /// upstream/int4.out), where it was 22P02; in base 10 a leading underscore is still 22P02, and
    /// so is a prefix with no digit.
    #[test]
    fn parameters_decode_at_postgresqls_edges() {
        let code = |bytes: &[u8], t: &Type| match pg_bytes_to_value(bytes, t) {
            Err(PgWireError::UserError(e)) => Some(e.code.clone()),
            Err(e) => panic!("{e:?}"),
            Ok(_) => None,
        };
        let float4_max = pg_bytes_to_value(b"3.4028235e+38", &Type::FLOAT4).unwrap();
        assert_eq!(float4_max, Value::from_f64(3.4028235e+38));
        assert_eq!(code(b"3.5e+38", &Type::FLOAT4).as_deref(), Some("22003"));
        assert_eq!(code(b"1e-400", &Type::FLOAT8).as_deref(), Some("22003"));
        assert_eq!(code(b"-1e-400", &Type::FLOAT8).as_deref(), Some("22003"));
        assert_eq!(code(b"1e-50", &Type::FLOAT4).as_deref(), Some("22003"));
        for (zero, t) in [
            (&b"0e-400"[..], &Type::FLOAT8),
            (b"0.000", &Type::FLOAT8),
            (b"-0", &Type::FLOAT4),
            (b"4.9e-324", &Type::FLOAT8),
            (b"1.4e-45", &Type::FLOAT4),
            (b"1e-400", &Type::NUMERIC),
        ] {
            assert_eq!(code(zero, t), None, "{:?} as {t}", std::str::from_utf8(zero));
        }
        let based = pg_bytes_to_value(b"0b_10_0101", &Type::INT4).unwrap();
        assert_eq!(based, Value::from_i64(37));
        let based = pg_bytes_to_value(b"0x_1EEE_FFFF", &Type::INT4).unwrap();
        assert_eq!(based, Value::from_i64(518979583));
        for bad in [&b"_100"[..], b"0x_", b"0b__1", b"0o_", b"100_"] {
            assert_eq!(
                code(bad, &Type::INT4).as_deref(),
                Some("22P02"),
                "{:?}",
                std::str::from_utf8(bad)
            );
        }
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

    /// A bytea parameter's hex is read byte by byte: a multi-byte character among the digits is
    /// an invalid digit (22023, as PostgreSQL's byteain answers: measured on 17.11 by wire review
    /// 13 item 8, which found 22P02 asserted here), never a slice inside it.
    /// `&hex[i..i + 2]` sliced a &str inside `é` and panicked, and under the release build's
    /// panic=abort one client's Bind ended every session (wire review 9 item 5).
    #[test]
    fn a_bytea_parameter_with_a_non_ascii_digit_is_refused() {
        for text in [
            "\\x0\u{e9}0",
            "\\x\u{e9}",
            "\\x0\u{1F600}0",
            "\\xGG",
            "\\x0",
        ] {
            let e = match pg_bytes_to_value(text.as_bytes(), &Type::BYTEA) {
                Err(PgWireError::UserError(info)) => info,
                other => panic!("{text:?}: {other:?}"),
            };
            assert_eq!(e.code, "22023", "{text:?}: {e:?}");
        }
        assert_eq!(
            pg_bytes_to_value(b"\\x00Ff", &Type::BYTEA).ok(),
            Some(Value::from_blob(vec![0x00, 0xff]))
        );
    }

    #[test]
    fn test_sqlite_type_to_pg_type() {
        assert_eq!(sqlite_type_to_pg_type("INTEGER"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT4"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("BIGINT"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("INT8"), Type::INT8);
        // REAL stays float8 here: it is also the engine's name for a computed expression of REAL
        // affinity (float8 in PostgreSQL). FLAGGED: this row of the never-green red 0748936d4
        // asserted FLOAT4 on a premise that failed when run (`SELECT 2.5` and random() announced
        // 700); an old REAL column's float4 is pinned by catalog's an_old_real_declaration test.
        assert_eq!(sqlite_type_to_pg_type("REAL"), Type::FLOAT8);
        assert_eq!(sqlite_type_to_pg_type("FLOAT4"), Type::FLOAT4);
        assert_eq!(sqlite_type_to_pg_type("FLOAT8"), Type::FLOAT8);
        assert_eq!(sqlite_type_to_pg_type("DOUBLE PRECISION"), Type::FLOAT8);
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

    /// A parameter of unknown type is its text as given, as PostgreSQL resolves a parameter nothing
    /// types: its context's type (result_types::parameter_types) is applied before this, never a
    /// guess from the value. The guess this test pinned put '007' into a text column as 7 (wire
    /// review 4 item 3; FLAGGED test edit, the guess is the ruled defect).
    #[test]
    fn test_unknown_type_inference() {
        for text in ["42", "3.14", "007", "t", "1e3", "hello"] {
            let val = pg_bytes_to_value(text.as_bytes(), &Type::UNKNOWN).unwrap();
            assert!(
                matches!(&val, Value::Text(t) if t.as_str() == text),
                "{text}: {val:?}"
            );
        }
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

    /// A server's shared state on a fresh database, its in-use map waiting `wait`.
    fn shared(dir: &tempfile::TempDir, wait: std::time::Duration) -> Arc<Shared> {
        let path = dir.path().join("w.db").to_string_lossy().into_owned();
        let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
            sync: turso_core::branch::SyncClass::Off,
        });
        let (_io, db) =
            turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts).unwrap();
        let mut shared = Shared::new(
            db,
            path,
            1,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        );
        shared.in_use_wait = wait;
        Arc::new(shared)
    }

    /// The in-use map refuses by state: a claim of a branch being deleted and a delete of a
    /// branch being deleted are "being deleted", a claim or a delete of a held branch "in use",
    /// all 55006; a release frees the name for either. No wire test reached the Deleting state,
    /// and removing either refusal survived every test (wire review 3 item 7).
    #[test]
    fn the_in_use_map_refuses_by_state_and_a_release_frees_the_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = shared(&dir, std::time::Duration::from_millis(20));
        let refused = |r: SqlResult<()>, what: &str| {
            let e = r.expect_err(what);
            assert_eq!(e.code, "55006", "{what}: {e:?}");
            e.message
        };
        s.begin_delete("x").expect("delete of a free name");
        let m = refused(s.claim("x", "ERROR"), "claim while deleting");
        assert!(m.contains("being deleted"), "{m}");
        let m = refused(s.begin_delete("x"), "delete while deleting");
        assert!(m.contains("being deleted"), "{m}");
        s.release("x");
        s.claim("x", "ERROR").expect("claim once the delete ended");
        let m = refused(s.begin_delete("x"), "delete while held");
        assert!(m.contains("in use by another session"), "{m}");
        let m = refused(s.claim("x", "ERROR"), "claim while held");
        assert!(m.contains("in use by another session"), "{m}");
        s.release("x");
        s.begin_delete("x").expect("delete once released");
        s.release("x");
        s.claim("x", "ERROR").expect("claim once released");
        assert_eq!(s.in_use_waiters.load(Ordering::SeqCst), 0);
    }

    /// A delete of a held branch waits for its release, and a claim of a branch being deleted for
    /// the delete's end: each returns at the release, well inside the wait, and succeeds (wire
    /// review 3 item 4's wait, which no test timed).
    #[test]
    fn a_waiting_delete_or_claim_returns_at_the_release() {
        let dir = tempfile::TempDir::new().unwrap();
        let wait = std::time::Duration::from_secs(5);
        let s = shared(&dir, wait);
        for first_held in [true, false] {
            if first_held {
                s.claim("x", "ERROR").unwrap();
            } else {
                s.begin_delete("x").unwrap();
            }
            let start = std::time::Instant::now();
            let releaser = {
                let s = s.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    s.release("x");
                })
            };
            let r = if first_held {
                s.begin_delete("x")
            } else {
                s.claim("x", "ERROR")
            };
            let waited = start.elapsed();
            releaser.join().unwrap();
            r.unwrap_or_else(|e| panic!("first_held={first_held}: {e:?}"));
            assert!(
                waited >= std::time::Duration::from_millis(100) && waited < wait / 2,
                "first_held={first_held}: waited {waited:?}"
            );
            s.release("x");
            assert_eq!(s.in_use_waiters.load(Ordering::SeqCst), 0);
        }
    }

    /// An autocommit write whose commit meets Busy after its rows went out is finished by stepping
    /// the same statement again, the engine's contract (core fastest_tests.rs,
    /// a_trunk_commit_retried_after_a_busy_decision_pass_retains_every_page). It was dropped, which
    /// committed it, then prepared and run again: applied twice, its row returned twice (wire
    /// review 5 item 2).
    #[test]
    fn a_write_whose_commit_meets_busy_is_applied_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        // No key, so a second application is a second row rather than a refusal.
        ok(&s, "CREATE TABLE t(id INT, v TEXT)");
        ok(&s, "INSERT INTO t VALUES (1, 'a')");
        // A live child, so the trunk commit decides the pages it overwrites.
        ok(&s, "SELECT turso_branch_create('c')");
        s.shared
            .db
            .branch_failpoint(Some(turso_core::branch::BranchFailpoint::TrunkDecisionBusy));
        let mut replies = s.simple("INSERT INTO t VALUES (9, 'x') RETURNING id");
        assert_eq!(replies.len(), 1);
        let rows = match replies.pop() {
            Some(Response::Query(mut q)) => tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut n = 0;
                    while let Some(row) = q.data_rows().next().await {
                        row.unwrap();
                        n += 1;
                    }
                    n
                }),
            Some(Response::Error(e)) => panic!("{} {}", e.code, e.message),
            _ => panic!("not a query reply"),
        };
        assert_eq!(rows, 1, "the RETURNING row went out {rows} times");
        let conn = s.shared.db.connect().unwrap();
        let count = conn
            .prepare("SELECT count(*) FROM t WHERE id = 9")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            count,
            vec![vec![Value::from_i64(1)]],
            "the INSERT was applied more than once"
        );
    }

    /// A write whose commit meets Busy after its rows went out is stepped again to its end even
    /// when its statement_timeout passes during the wait: dropped there, a write with RETURNING is
    /// committed by the engine's reset, so the 57014 it answered reported as cancelled a write that
    /// was stored (measured: 57014 with its row stored; fresh-context review of the ultrareview
    /// fixes, on finding 9's Backoff).
    #[test]
    fn a_write_whose_commit_meets_busy_is_not_cancelled_by_its_timeout() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT, v TEXT)");
        ok(&s, "INSERT INTO t VALUES (1, 'a')");
        ok(&s, "SELECT turso_branch_create('c')");
        ok(&s, "SET statement_timeout = 1");
        s.shared
            .db
            .branch_failpoint(Some(turso_core::branch::BranchFailpoint::TrunkDecisionBusy));
        std::thread::sleep(std::time::Duration::from_millis(5));
        let replies = s.simple("INSERT INTO t VALUES (9, 'x') RETURNING id");
        let conn = s.shared.db.connect().unwrap();
        let count = conn
            .prepare("SELECT count(*) FROM t WHERE id = 9")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        match replies.as_slice() {
            [Response::Query(_)] => {}
            [Response::Error(e)] => panic!(
                "the write answered {} {} with {count:?} rows of it stored",
                e.code, e.message
            ),
            other => panic!("{} replies", other.len()),
        }
        assert_eq!(
            count,
            vec![vec![Value::from_i64(1)]],
            "the write was not applied once"
        );
    }

    /// A COMMIT that meets Busy at the trunk's commit is not run again: the failed COMMIT already
    /// ended the block (rolled back), so a COMMIT prepared anew found no transaction and answered
    /// XX000. It answers 40001, a serialization failure the client retries, with the block's write
    /// gone and the session idle (wire review 5 item 8).
    #[test]
    fn a_commit_that_meets_busy_is_a_serialization_failure() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        ok(&s, "CREATE TABLE t(id INT, v TEXT)");
        ok(&s, "INSERT INTO t VALUES (1, 'a')");
        ok(&s, "SELECT turso_branch_create('c')");
        ok(&s, "BEGIN");
        ok(&s, "INSERT INTO t VALUES (9, 'x')");
        s.shared
            .db
            .branch_failpoint(Some(turso_core::branch::BranchFailpoint::TrunkDecisionBusy));
        let replies = s.simple("COMMIT");
        match replies.as_slice() {
            [Response::Error(e)] => assert_eq!(e.code, "40001", "{}", e.message),
            other => panic!("COMMIT answered {} replies, not one error", other.len()),
        }
        assert!(
            matches!(s.transaction_status(), TransactionStatus::Idle),
            "the failed COMMIT left a block open"
        );
        let conn = s.shared.db.connect().unwrap();
        let count = conn
            .prepare("SELECT count(*) FROM t WHERE id = 9")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            count,
            vec![vec![Value::from_i64(0)]],
            "the block's write was kept"
        );
    }

    /// A schema change that keeps landing under a branch call (SchemaUpdated, which the engine
    /// leaves to its caller) is waited out on the busy schedule, up to the lock wait. It was retried
    /// 50 times back to back, a few microseconds in all, after which the next one escaped as XX000
    /// (wire review 5 item 9; branch_creates_racing_trunk_ddl_all_succeed failed 40 of 40 so in the
    /// suite at 82cabdf5c).
    #[test]
    fn waiting_waits_out_schema_changes_on_the_busy_schedule() {
        let dir = tempfile::TempDir::new().unwrap();
        let s = session(&dir);
        let mut left = 55;
        let r = s.waiting(|| {
            if left > 0 {
                left -= 1;
                Err(LimboError::SchemaUpdated)
            } else {
                Ok(7)
            }
        });
        assert_eq!(r.ok(), Some(7), "55 schema changes in a row escaped");
    }

    /// The rows of `id` in t, read on a connection of its own.
    fn count_id(s: &Session, id: i64) -> i64 {
        let conn = s.shared.db.connect().unwrap();
        let rows = conn
            .prepare(format!("SELECT count(*) FROM t WHERE id = {id}"))
            .unwrap()
            .run_collect_rows()
            .unwrap();
        match rows.as_slice() {
            [row] => match row.first() {
                Some(Value::Numeric(turso_core::Numeric::Integer(n))) => *n,
                other => panic!("count: {other:?}"),
            },
            other => panic!("count rows: {other:?}"),
        }
    }

    /// The engine connection the session runs on holds no write transaction.
    fn holds_no_write(s: &Session) -> bool {
        let st = s.state();
        let conn = match &st.branch {
            Some((_, conn)) => Some(conn),
            None => st.trunk.as_ref(),
        };
        conn.is_none_or(|c| !c.inner().is_in_write_tx())
    }

    /// A trunk table with a live child, so the trunk's commit decides the pages it overwrites, and
    /// the decision pass armed to be refused once (TrunkDecisionBusy).
    fn busy_commit_fixture(dir: &tempfile::TempDir) -> Session {
        let s = session(dir);
        ok(&s, "CREATE TABLE t(id INT, v TEXT)");
        ok(&s, "INSERT INTO t VALUES (1, 'a')");
        ok(&s, "SELECT turso_branch_create('c')");
        s
    }

    /// A COMMIT refused at the trunk's commit (Busy at the decision pass, a live child) is never
    /// stepped again or run anew: the block either commits (once the engine resumes a refused
    /// COMMIT) or answers 40001 with nothing kept, and either way the session is idle and the engine
    /// holds no write transaction. Never XX000: dropped and run anew, the COMMIT found no
    /// transaction; stepped again, it was stranded holding the WAL lock (wire review 8 item 1,
    /// review 7 item 3). The same for a multi-statement query's implicit COMMIT and a pipeline's,
    /// at Sync.
    #[test]
    fn a_refused_commit_is_never_run_again() {
        let arm = |s: &Session| {
            s.shared
                .db
                .branch_failpoint(Some(turso_core::branch::BranchFailpoint::TrunkDecisionBusy))
        };
        // Either outcome is the block's alone: committed, or a serialization failure with nothing
        // kept.
        let settled = |s: &Session, what: &str, err: Option<&ErrorInfo>, ids: &[i64]| {
            let kept: Vec<i64> = ids.iter().map(|id| count_id(s, *id)).collect();
            match err {
                None => assert!(kept.iter().all(|n| *n == 1), "{what}: ok, kept {kept:?}"),
                Some(e) => {
                    assert_eq!(e.code, "40001", "{what}: {}", e.message);
                    assert!(
                        kept.iter().all(|n| *n == 0),
                        "{what}: failed, kept {kept:?}"
                    );
                }
            }
            assert!(
                matches!(s.transaction_status(), TransactionStatus::Idle),
                "{what}: a block is open"
            );
            assert!(
                holds_no_write(s),
                "{what}: the engine still holds a write transaction"
            );
        };
        // An explicit block's COMMIT.
        let dir = tempfile::TempDir::new().unwrap();
        let s = busy_commit_fixture(&dir);
        ok(&s, "BEGIN");
        ok(&s, "INSERT INTO t VALUES (9, 'x')");
        arm(&s);
        let replies = s.simple("COMMIT");
        let err = match replies.as_slice() {
            [Response::Error(e)] => Some(&**e),
            [_] => None,
            other => panic!("COMMIT answered {} replies", other.len()),
        };
        settled(&s, "BEGIN; INSERT; COMMIT", err, &[9]);
        // A multi-statement query: one implicit block, committed after its last statement.
        let dir = tempfile::TempDir::new().unwrap();
        let s = busy_commit_fixture(&dir);
        arm(&s);
        let replies = s.simple("INSERT INTO t VALUES (9, 'x'); INSERT INTO t VALUES (10, 'y')");
        let err = replies.iter().find_map(|r| match r {
            Response::Error(e) => Some(&**e),
            _ => None,
        });
        settled(&s, "two-statement query", err, &[9, 10]);
        // A pipeline's implicit block, committed at Sync.
        let dir = tempfile::TempDir::new().unwrap();
        let s = busy_commit_fixture(&dir);
        let sql = "INSERT INTO t VALUES (9, 'x')";
        s.begin_implicit(sql, None, true).unwrap();
        assert!(
            s.run(sql, None, None, &Format::UnifiedText).is_ok(),
            "premise: the pipeline's insert runs"
        );
        s.after_implicit(sql, false);
        arm(&s);
        let r = s.end_implicit();
        settled(&s, "pipeline at Sync", r.as_ref().err().map(|e| &**e), &[9]);
    }

    /// A transaction verb is read past comments anywhere a token boundary allows one, as
    /// PostgreSQL's lexer reads a comment as whitespace: before the verb, between its words, after
    /// it and after its `;`. A comment after the verb made it Other, so `COMMIT/*x*/` and
    /// `COMMIT -- x` skipped the failed-COMMIT rule (rerun after a refusal; wire review 9 item 7).
    #[test]
    fn a_transaction_verb_is_read_past_its_comments() {
        for (sql, want) in [
            ("/* c */ COMMIT", TxVerb::Commit),
            ("-- x\nCOMMIT", TxVerb::Commit),
            ("COMMIT/*x*/", TxVerb::Commit),
            ("COMMIT -- x", TxVerb::Commit),
            ("COMMIT; -- x", TxVerb::Commit),
            ("COMMIT /* a /* nested */ b */ WORK", TxVerb::Commit),
            ("END/**/AND NO CHAIN", TxVerb::Commit),
            ("/* c */ ROLLBACK", TxVerb::Rollback),
            ("ROLLBACK -- x\n", TxVerb::Rollback),
            ("/* c */ BEGIN", TxVerb::Begin),
            ("BEGIN /*x*/ ISOLATION LEVEL READ COMMITTED", TxVerb::Begin),
            ("COMMIT /* unterminated", TxVerb::Other),
            ("COMMIT; SELECT 1", TxVerb::Other),
            ("COMMIT -- x\n garbage", TxVerb::Other),
            ("COMMIT/* x */garbage", TxVerb::Other),
        ] {
            assert_eq!(TxVerb::of(sql), want, "{sql:?}");
        }
    }

    /// The savepoint verbs' edges (wire review 14 items 21 and 22). A `$` continues a name after its
    /// first byte, as in an identifier, and no name starts with one; a `--` comment ends at \r as at
    /// \n; an unterminated nested comment is no verb (pins of c305f1a13's halves, which no test
    /// held). A savepoint may be named savepoint: `ROLLBACK TO SAVEPOINT` and `RELEASE SAVEPOINT`
    /// with no other word name it, as PostgreSQL's grammar reads them (TO ColId, RELEASE ColId),
    /// where the reader stripped the keyword and found no name (Other: in a failed block, 25P02).
    #[test]
    fn the_savepoint_verbs_are_read_by_their_grammar() {
        for (sql, want) in [
            ("SAVEPOINT a$b", TxVerb::Savepoint),
            ("RELEASE a$1", TxVerb::Release),
            ("SAVEPOINT $1", TxVerb::Other),
            ("-- x\rCOMMIT", TxVerb::Commit),
            ("/* a /* b */ COMMIT", TxVerb::Other),
            ("SAVEPOINT savepoint", TxVerb::Savepoint),
            ("ROLLBACK TO SAVEPOINT", TxVerb::RollbackTo),
            ("ROLLBACK WORK TO savepoint", TxVerb::RollbackTo),
            ("ROLLBACK TO SAVEPOINT savepoint", TxVerb::RollbackTo),
            ("RELEASE SAVEPOINT", TxVerb::Release),
            ("RELEASE SAVEPOINT savepoint", TxVerb::Release),
            ("ROLLBACK TO", TxVerb::Other),
            ("RELEASE", TxVerb::Other),
        ] {
            assert_eq!(TxVerb::of(sql), want, "{sql:?}");
        }
    }

    /// A COMMIT behind a comment that the trunk refuses settles as a COMMIT's refusal: committed
    /// or 40001 with nothing kept, the session idle, never run again (wire review 9 item 7).
    #[test]
    fn a_commented_commit_that_meets_busy_is_never_run_again() {
        for sql in ["/* c */ COMMIT", "COMMIT -- x", "COMMIT/*x*/"] {
            let dir = tempfile::TempDir::new().unwrap();
            let s = busy_commit_fixture(&dir);
            ok(&s, "BEGIN");
            ok(&s, "INSERT INTO t VALUES (9, 'x')");
            s.shared
                .db
                .branch_failpoint(Some(turso_core::branch::BranchFailpoint::TrunkDecisionBusy));
            let replies = s.simple(sql);
            let kept = count_id(&s, 9);
            match replies.as_slice() {
                [Response::Error(e)] => {
                    assert_eq!(e.code, "40001", "{sql}: {}", e.message);
                    assert_eq!(kept, 0, "{sql}: failed, yet kept");
                }
                [_] => assert_eq!(kept, 1, "{sql}: ok, yet not kept"),
                other => panic!("{sql} answered {} replies", other.len()),
            }
            assert!(
                matches!(s.transaction_status(), TransactionStatus::Idle),
                "{sql}: a block is open"
            );
            assert!(holds_no_write(&s), "{sql}: the engine still holds a write");
        }
    }

    /// A claim of a held name waits for its release, as a delete does: released during the wait,
    /// the claim succeeds; never released, it is refused once the wait is spent (wire review 6
    /// item 5).
    #[test]
    fn a_claim_of_a_held_name_waits_for_its_release() {
        let dir = tempfile::TempDir::new().unwrap();
        let wait = std::time::Duration::from_millis(400);
        let s = shared(&dir, wait);
        s.claim("x", "ERROR").unwrap();
        let start = std::time::Instant::now();
        let r = s.claim("x", "ERROR");
        assert!(r.is_err(), "a second claim of a held name succeeded");
        assert!(
            start.elapsed() >= wait,
            "refused after {:?}, before the wait was spent",
            start.elapsed()
        );
        let releaser = {
            let s = s.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                s.release("x");
            })
        };
        s.claim("x", "ERROR")
            .expect("a claim of a name released during the wait");
        releaser.join().unwrap();
        assert_eq!(s.in_use_waiters.load(Ordering::SeqCst), 0);
    }
}
