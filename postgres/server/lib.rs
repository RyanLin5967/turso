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
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::{self, StreamExt};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use turso_core::{CheckpointMode, Database, LimboError, Value};
use turso_pg::{
    attach_schema_files, branch_call, element_of, split_statements, PgBranchArg, PgBranchCall,
    PgConnection, StatementTypes,
};

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal, PortalExecutionState};
use pgwire::api::query::{send_ready_for_query, ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
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

/// One statement's waits on a lock another session holds: SQLite's default busy schedule (1, 2,
/// 5, 10, 15, 20, 25, 25, 25, 50, 50 ms, then 100 ms) up to the server's lock wait. The clock is
/// first read at the first wait, so a statement that meets no lock reads it not at all (wire
/// review 5 item 20: every create read it).
struct Backoff {
    lock_wait: std::time::Duration,
    deadline: Option<std::time::Instant>,
    attempt: usize,
}

impl Backoff {
    fn new(lock_wait: std::time::Duration) -> Self {
        Self {
            lock_wait,
            deadline: None,
            attempt: 0,
        }
    }

    /// A Backoff that never waits: its statement is never stepped again or run anew.
    fn never() -> Self {
        Self::new(std::time::Duration::ZERO)
    }

    /// Sleep the schedule's next delay and say so, or say false, without sleeping, when the delay
    /// would end past the lock wait: then the next attempt is the last.
    fn wait(&mut self) -> bool {
        const DELAYS_MS: [u64; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
        let now = std::time::Instant::now();
        let deadline = *self.deadline.get_or_insert(now + self.lock_wait);
        let delay =
            std::time::Duration::from_millis(DELAYS_MS[self.attempt.min(DELAYS_MS.len() - 1)]);
        self.attempt += 1;
        if now + delay > deadline {
            return false;
        }
        std::thread::sleep(delay);
        true
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
    backoff: &mut Backoff,
    mut row: impl FnMut(&turso_core::Row) -> turso_core::Result<()>,
) -> turso_core::Result<()> {
    loop {
        match stmt.run_with_row_callback(&mut row) {
            Err(LimboError::Busy) if stmt.n_change() > 0 && backoff.wait() => {}
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
    in_use: Mutex<std::collections::HashMap<String, BranchUse>>,
    /// Signalled when an entry leaves `in_use`, for a delete waiting on a session that is closing.
    in_use_freed: std::sync::Condvar,
    /// Threads waiting on `in_use_freed`: a release signals only when there is one, so an
    /// uncontended switch or delete wakes nobody.
    in_use_waiters: AtomicUsize,
    /// How long a claim or a delete waits on `in_use`: IN_USE_WAIT (a field so a unit test can
    /// shorten it).
    in_use_wait: std::time::Duration,
}

/// How long a delete waits for the session on its branch to go, and a switch for a delete of its
/// branch to end: PostgreSQL's DROP DATABASE waits 5 s for exiting backends (CountOtherDBBackends,
/// 50 x 100 ms).
const IN_USE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

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
        }
    }

    fn uses(&self) -> MutexGuard<'_, std::collections::HashMap<String, BranchUse>> {
        self.in_use.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The map once `name` is in none of `states` there, or at the wait's end: what a closing session
    /// or a running delete leaves behind is waited out, not refused (wire review 3 item 4).
    fn wait_while<'a>(
        &'a self,
        mut uses: MutexGuard<'a, std::collections::HashMap<String, BranchUse>>,
        name: &str,
        states: &[BranchUse],
    ) -> MutexGuard<'a, std::collections::HashMap<String, BranchUse>> {
        let held = |uses: &std::collections::HashMap<String, BranchUse>| {
            uses.get(name).is_some_and(|u| states.contains(u))
        };
        if !held(&uses) {
            return uses;
        }
        self.in_use_waiters.fetch_add(1, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + self.in_use_wait;
        while held(&uses) {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                break;
            }
            uses = self
                .in_use_freed
                .wait_timeout(uses, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        self.in_use_waiters.fetch_sub(1, Ordering::SeqCst);
        uses
    }

    /// Claim `name` for a session about to connect to it. One session per branch: a branch another
    /// session is on, or that a delete is releasing, is waited for, up to the wait, as begin_delete
    /// waits, then refused with 55006. Refused at once, a reconnect right after a close met 55006
    /// whenever the closed session's thread had not yet released the branch (wire review 6 item
    /// 5); only a refusal pays the wait (lead ruling).
    fn claim(&self, name: &str, severity: &str) -> SqlResult<()> {
        let mut uses = self.wait_while(self.uses(), name, &[BranchUse::Held, BranchUse::Deleting]);
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
        if self.in_use_waiters.load(Ordering::SeqCst) > 0 {
            self.in_use_freed.notify_all();
        }
    }

    /// Mark `name` as being deleted. A session on it is waited for (up to `in_use_wait`: one that is
    /// closing releases it within that), then refused with 55006.
    fn begin_delete(&self, name: &str) -> SqlResult<()> {
        let mut uses = self.wait_while(self.uses(), name, &[BranchUse::Held, BranchUse::Deleting]);
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
            shared: Arc::new(Shared::new(db, db_file, max_connections, lock_wait)),
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
        // The budget harness refuses a --plant split-reply run whose server does not say this.
        #[cfg(feature = "budget-plant-split-reply")]
        println!("PLANT split-reply: every simple-query reply goes out in two writes");

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
        if let Err(e) = serve_session(socket, session).await {
            error!("Error processing connection from {}: {}", addr, e);
        }
    });
}

/// How long a client has to finish its startup, as pgwire's process_socket allows (and
/// PostgreSQL's authentication_timeout's default).
const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// pgwire's process_socket, plus the end of a session. pgwire drops a Terminate ('X') and serves
/// on until the client's socket reaches EOF, so a session, its branch connection and its in-use
/// entry outlived the client's close: bbload's synchronous close waits for the server's EOF and
/// failed every connect op after 5 s, and a delete right after a close met 55006 (wire review 3
/// items 3 and 4). Here a Terminate ends the session, and on every way out the session releases
/// its branch and its in-use entry BEFORE the socket closes (the socket is dropped on return).
async fn serve_session(
    socket: tokio::net::TcpStream,
    session: Arc<Session>,
) -> Result<(), std::io::Error> {
    let startup_timeout = tokio::time::sleep(STARTUP_TIMEOUT);
    tokio::pin!(startup_timeout);
    // Every way a session ends before its startup completes is logged: the client sees only the
    // socket close (the "closed the connection during startup" flake, lead).
    let socket = tokio::select! {
        _ = &mut startup_timeout => {
            warn!("session ended before startup: the startup timeout passed during TLS negotiation");
            return Ok(());
        }
        socket = pgwire::tokio::server::negotiate_tls(socket, None) => socket.inspect_err(|e| {
            warn!("session ended before startup: TLS negotiation failed: {}", e);
        })?,
    };
    let Some(mut socket) = socket else {
        warn!("session ended before startup: TLS negotiation returned no socket");
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
    let cancel_handler = Arc::new(NoopHandler);
    let error_handler = NoopHandler;
    let result = loop {
        let msg = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => None,
                msg = socket.next() => msg,
            }
        } else {
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
                if matches!(
                    socket.state(),
                    PgWireConnectionState::AwaitingStartup
                        | PgWireConnectionState::AuthenticationInProgress
                ) {
                    warn!(
                        "session ended before startup: the client's stream ended, or the startup \
                         timeout passed"
                    );
                }
                break Ok(());
            }
        };
        if matches!(msg, PgWireFrontendMessage::Terminate(_)) {
            break Ok(());
        }
        let is_extended_query = match socket.state() {
            PgWireConnectionState::CopyInProgress(extended) => extended,
            _ => msg.is_extended_query(),
        };
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
    };
    // The session's branch and in-use entry go first; the socket closes when it drops below.
    session.end();
    drop(socket);
    result
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
}

/// An engine statement's failure, with whether it got as far as running: a COMMIT or ROLLBACK that
/// failed to prepare (a syntax error) is a failed statement like any other, while one that ran and
/// failed ended or did not end the block by its outcome (wire review 3 item 5). `rerunnable`: it
/// changed nothing before it failed, so running it anew cannot apply it twice.
struct StatementFailure {
    prepared: bool,
    rerunnable: bool,
    info: Box<ErrorInfo>,
}

/// A statement Describe prepared: on which engine connection, from which text.
struct Described {
    conn: Arc<turso_core::Connection>,
    sql: String,
    stmt: turso_core::Statement,
    /// The types the parse gave (see [`PgConnection::prepare_typed`]).
    types: StatementTypes,
}

/// CHECKPOINTs skipped inside a block because the WAL was busy (see [`Session::checkpoint`]),
/// since the server started: turso_branch_stats' `checkpoints_skipped`.
static CHECKPOINTS_SKIPPED: AtomicU64 = AtomicU64::new(0);

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
        self.simple_with_notices(query)
            .into_iter()
            .map(|(_, r)| r)
            .collect()
    }

    /// [`Session::simple`], each result with the notices its statement raised.
    fn simple_with_notices(&self, query: &str) -> Vec<(Vec<Box<ErrorInfo>>, Response)> {
        let with_notices = |r: Response| (std::mem::take(&mut self.state().notices), r);
        // A query that is one branch call goes straight to the engine: no split, no parse (L5).
        if let Some(call) = branch_call(query) {
            return vec![with_notices(
                self.run(query, Some(call), None, &Format::UnifiedText)
                    .unwrap_or_else(Response::Error),
            )];
        }
        let statements = match split_statements(query) {
            Ok(s) => s,
            Err(e) => return vec![(Vec::new(), Response::Error(engine_info(&e)))],
        };
        let mut responses = Vec::with_capacity(statements.len());
        // More than one statement: one implicit transaction, as in PostgreSQL (wire review 1 item 8).
        let multi = statements.len() > 1;
        for sql in &statements {
            let call = branch_call(sql);
            let result = if multi {
                self.begin_implicit(sql, call.as_ref(), false)
            } else {
                Ok(())
            }
            .and_then(|()| self.run(sql, call, None, &Format::UnifiedText));
            if multi {
                self.after_implicit(sql, result.is_err());
            }
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
        if st.implicit || st.aborted || TxVerb::of(sql) != TxVerb::Other {
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
        engine_tx(&conn, TxStmt::Begin).map_err(|e| engine_info(&e))?;
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
            if let Ok(conn) = self.current(&mut st) {
                if !conn.inner().get_auto_commit() {
                    let _ = engine_tx(&conn, TxStmt::Rollback);
                }
            }
        } else if !matches!(
            TxVerb::of(sql),
            TxVerb::Begin | TxVerb::Commit | TxVerb::Rollback
        ) {
            st.implicit = true;
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
            let _ = engine_tx(&conn, TxStmt::Rollback);
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
        if let Err(e) = engine_tx(&conn, TxStmt::Commit) {
            if !conn.inner().get_auto_commit() {
                let _ = engine_tx(&conn, TxStmt::Rollback);
            }
            // The same rule as a client's COMMIT (wire review 8 item 1).
            return Err(commit_failed(&conn, engine_info(&e)));
        }
        Ok(())
    }

    /// Run one statement on the session, `call` its branch call if it is one (already read), and
    /// `portal` carrying its bound parameters (extended protocol).
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
                // A failed block the engine holds no transaction for is one whose failing error
                // made the engine roll the whole transaction back, savepoints and all (a failed
                // block is otherwise still open in the engine): the savepoint does not exist, as
                // PostgreSQL says of one that does not, and the block stays failed. It was XX000
                // "no such savepoint" from the engine (wire review 3 item 10).
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
                _ => return Err(aborted_error()),
            }
        }
        match verb {
            // PostgreSQL warns and carries on for these; the engine would refuse them.
            TxVerb::Begin if in_tx => return Ok(Response::Execution(Tag::new("BEGIN"))),
            TxVerb::Commit if !in_tx => return Ok(Response::Execution(Tag::new("COMMIT"))),
            TxVerb::Rollback if !in_tx => return Ok(Response::Execution(Tag::new("ROLLBACK"))),
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
        // or a refusal here is a statement that ran).
        let mut ran = true;
        let result = match call {
            Some(call) => {
                // A statement a Describe kept holds its connection open: a switch away, or a
                // delete of the branch it is on, must not find that connection still alive.
                st.described = None;
                self.branch(&mut st, &conn, &call, portal, format)
            }
            None if is_checkpoint(sql) => self.checkpoint(&mut st, in_tx),
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
                    f.info
                })
            }
        };
        // The frontend could not undo a failed statement: the connection's state is unknown, so the
        // session ends (FATAL 08006, which serve_session ends the session on) instead of serving
        // more statements on it (wire review 2 item 1). Read from the connection's flag, not the
        // error's text (wire review 5 item 4).
        let result = match result {
            Err(mut info) if conn.is_broken() => {
                info.code = "08006".to_string();
                info.severity = "FATAL".to_string();
                Err(info)
            }
            r => r,
        };
        if result.is_err() {
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
    /// and readers within the lock timeout and fails with 55P03 after it. Inside a block the
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
    fn describe_prepare(&self, sql: &str) -> PgWireResult<()> {
        let conn = self
            .current(&mut self.state())
            .map_err(PgWireError::UserError)?;
        if self
            .state()
            .described
            .as_ref()
            .is_some_and(|d| d.sql == sql && Arc::ptr_eq(&d.conn, conn.inner()))
        {
            return Ok(());
        }
        let in_tx = !conn.inner().get_auto_commit();
        // Never the prepare that performs a statement (ALTER ADD CONSTRAINT, COPY FROM, SET,
        // CREATE/DROP SCHEMA): the frontend declines those from the parse, they keep nothing and
        // answer NoData, and Execute performs them once (wire review 2 item 2, review 3 item 2).
        match conn.prepare_for_describe(sql) {
            Ok(Some((stmt, types))) => {
                self.state().described = Some(Described {
                    conn: conn.inner().clone(),
                    sql: sql.to_string(),
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

    /// The result columns of the statement [`Session::describe_prepare`] kept.
    fn described_fields(&self, format: &Format) -> SqlResult<Vec<FieldInfo>> {
        self.state().described.as_ref().map_or(Ok(Vec::new()), |d| {
            result_fields(&d.stmt, &d.types.columns, format)
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
    ) -> Option<(turso_core::Statement, StatementTypes)> {
        let mut st = self.state();
        match st.described.take() {
            Some(d) if d.sql == sql && Arc::ptr_eq(&d.conn, conn.inner()) => {
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
        portal: Option<&Portal<String>>,
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
            Backoff::new(self.shared.lock_wait)
        };
        loop {
            // sqlstate() gives 55P03 to LimboError::Busy alone and 40001 to BusySnapshot alone.
            match self.engine_statement_once(conn, sql, portal, format, &mut backoff) {
                Err(mut f) if verb == TxVerb::Commit => {
                    f.info = commit_failed(conn, f.info);
                    return Err(f);
                }
                Err(f)
                    if f.rerunnable
                        && (f.info.code == "55P03" || (!in_tx && f.info.code == "40001")) =>
                {
                    if !backoff.wait() {
                        return self.engine_statement_once(conn, sql, portal, format, &mut backoff);
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
        portal: Option<&Portal<String>>,
        format: &Format,
        backoff: &mut Backoff,
    ) -> Result<Response, StatementFailure> {
        let unprepared = |info: Box<ErrorInfo>| StatementFailure {
            prepared: false,
            rerunnable: true,
            info,
        };
        let described = portal.and_then(|_| self.take_described(conn, sql));
        let (mut stmt, types) = match described {
            Some(kept) => kept,
            None => conn
                .prepare_typed(sql)
                .map_err(|e| unprepared(engine_info(&e)))?,
        };
        self.shared.cleanup_dropped_schema_file(sql);
        match portal {
            Some(portal) => bind_portal_parameters(&mut stmt, portal, &types)
                .map_err(|e| unprepared(wire_info(e)))?,
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
        let r = if stmt.num_columns() == 0 || is_pg_non_query(sql) {
            execute_non_query(&mut stmt, sql, backoff)
        } else {
            // The column types are the statement's alone, so Describe (which runs nothing) and
            // this reply agree on both protocols (wire review 1 item 14).
            execute_query(
                &mut stmt,
                format,
                &types.columns,
                &conn.inner().current_schema(),
                backoff,
            )
        };
        r.map_err(|e| StatementFailure {
            prepared: true,
            rerunnable: stmt.n_change() == 0,
            info: wire_info(e),
        })
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
            return one_text(f, name, format);
        }
        if f == "turso_branch_stats" {
            arity(call, 0)?;
            return stats_row(format);
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
        // The result-format list is checked before the call changes anything: a create refused
        // after it ran would leave its branch behind (wire review 9 item 2).
        result_format(format, 0, 1)?;
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
                    Ok(id) => one_int8(f, id.0 as i64, format),
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
                        next.adopt_session_of(conn);
                        st.branch = Some((name.clone(), next));
                    }
                    return one_text(f, &name, format);
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
                one_text(f, &name, format)
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
                    Ok(_) => one_text(f, &name, format),
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
    /// deadline is final.
    fn retrying<T, E>(
        &self,
        mut f: impl FnMut() -> Result<T, E>,
        busy: impl Fn(&E) -> bool,
    ) -> Result<T, E> {
        let mut backoff = Backoff::new(self.shared.lock_wait);
        loop {
            match f() {
                Err(e) if busy(&e) => {
                    if !backoff.wait() {
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
    /// The session's branch is free once its connection is closed.
    fn drop(&mut self) {
        self.end();
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

/// What a statement does to the transaction block, read by the verb's WHOLE grammar (PostgreSQL's
/// gram.y), after any leading comments:
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
                if no_chain(&rest[work(rest)..]) {
                    TxVerb::Commit
                } else {
                    TxVerb::Other
                }
            }
            [first, rest @ ..] if is(first, "ROLLBACK") || is(first, "ABORT") => {
                let rest = &rest[work(rest)..];
                if no_chain(rest) {
                    TxVerb::Rollback
                } else if is(first, "ROLLBACK") && rest.first().is_some_and(|x| is(x, "TO")) {
                    let rest = &rest[1..];
                    let rest = if rest.first().is_some_and(|x| is(x, "SAVEPOINT")) {
                        &rest[1..]
                    } else {
                        rest
                    };
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
                let rest = if rest.first().is_some_and(|x| is(x, "SAVEPOINT")) {
                    &rest[1..]
                } else {
                    rest
                };
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

    /// The statements a failed block still accepts.
    fn ends_block(self) -> bool {
        matches!(self, TxVerb::Commit | TxVerb::Rollback | TxVerb::RollbackTo)
    }
}

/// The words of a statement for [`TxVerb::of`]: leading `--` and `/* */` comments skipped, a
/// trailing `;` dropped, a `"quoted"` name one word, `,` a word of its own, each a slice of `sql`.
/// None for a statement that does not start with a transaction verb (one word's scan, no
/// allocation), and for text the verbs' grammar cannot hold (a `'` string, `$`, another `;`, a
/// comment after the start), which is Other.
fn tx_words(sql: &str) -> Option<Vec<&str>> {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split_once('\n').map_or("", |(_, r)| r).trim_start();
        } else if s.starts_with("/*") {
            let b = s.as_bytes();
            let (mut depth, mut i) = (0usize, 0usize);
            while i < b.len() {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return None;
            }
            s = s[i..].trim_start();
        } else {
            break;
        }
    }
    let first_end = s
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(s.len());
    let first = &s[..first_end];
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
    let body = s.trim_end();
    let body = body.strip_suffix(';').unwrap_or(body).trim_end();
    let b = body.as_bytes();
    let mut words = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            c if c.is_ascii_whitespace() => i += 1,
            b',' => {
                words.push(&body[i..i + 1]);
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
                words.push(&body[start..i]);
            }
            b'\'' | b'$' | b';' => return None,
            _ if b[i..].starts_with(b"--") || b[i..].starts_with(b"/*") => return None,
            _ => {
                let start = i;
                while i < b.len()
                    && !b[i].is_ascii_whitespace()
                    && !matches!(b[i], b',' | b'"' | b'\'' | b'$' | b';')
                {
                    i += 1;
                }
                words.push(&body[start..i]);
            }
        }
    }
    Some(words)
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

/// A transaction-control statement the client did not send: an implicit block's own.
#[derive(Debug, Clone, Copy)]
enum TxStmt {
    Begin,
    Commit,
    Rollback,
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

/// Run `tx` on `conn` from the engine's AST: no SQL text is parsed, so an implicit block costs no
/// libpg_query call (and keeps an_ordinary_statement_is_parsed_once's count).
fn engine_tx(conn: &PgConnection, tx: TxStmt) -> turso_core::Result<()> {
    use turso_parser::ast::Stmt;
    let (stmt, text) = match tx {
        TxStmt::Begin => (
            Stmt::Begin {
                typ: None,
                name: None,
            },
            "BEGIN",
        ),
        TxStmt::Commit => (Stmt::Commit { name: None }, "COMMIT"),
        TxStmt::Rollback => (
            Stmt::Rollback {
                tx_name: None,
                savepoint_name: None,
            },
            "ROLLBACK",
        ),
    };
    conn.inner()
        .prepare_translated_stmt(stmt, text)?
        .run_ignore_rows()
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

/// Whether `sql` is a bare CHECKPOINT (the server runs it itself, see [`Session::checkpoint`]). A
/// form this does not read, e.g. one behind a comment, reaches the engine's PRAGMA path.
fn is_checkpoint(sql: &str) -> bool {
    let mut words = sql
        .split(|c: char| c.is_ascii_whitespace() || c == ';')
        .filter(|w| !w.is_empty());
    words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("CHECKPOINT"))
        && words.next().is_none()
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

const STATS_COLUMNS: [&str; 5] = [
    "unix_syscalls",
    "mach_syscalls",
    "instructions",
    "cycles",
    "checkpoints_skipped",
];

/// turso_branch_stats(): the server process's counters ([`counters::process_counters`]), read as
/// the call runs, NULLs where the platform does not count them; then the server's own count of
/// skipped CHECKPOINTs ([`CHECKPOINTS_SKIPPED`]).
fn stats_row(format: &Format) -> SqlResult<Response> {
    let header = Arc::new(stats_fields(format)?);
    let mut encoder = DataRowEncoder::new(header.clone());
    let values = counters::process_counters()
        .map(|c| [c.unix_syscalls, c.mach_syscalls, c.instructions, c.cycles].map(|v| v as i64));
    let skipped = CHECKPOINTS_SKIPPED.load(Ordering::Relaxed) as i64;
    let row = (0..4)
        .try_for_each(|i| encoder.encode_field(&values.map(|v| v[i])))
        .and_then(|()| encoder.encode_field(&skipped))
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
    let pg_type = if call.function == "turso_branch_create" {
        Type::INT8
    } else {
        Type::TEXT
    };
    Ok(vec![FieldInfo::new(
        call.function.clone(),
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
    error(sqlstate(e), e.to_string())
}

/// PostgreSQL's SQLSTATE (errcodes.txt) for an engine error, so a driver raises the right class
/// (wire review 1 item 7). The engine types a constraint failure's kind only in SQLite's fixed
/// message forms ("UNIQUE constraint failed: ..."), and a statement it cannot compile only as a
/// ParseError whose text comes from libpg_query ("Invalid statement: ...", always a grammar
/// error), the translator ("... not supported ...") or the planner ("no such table: ..."); those
/// forms are read here. Anything unrecognised stays XX000, internal_error.
fn sqlstate(e: &LimboError) -> &'static str {
    match e {
        LimboError::Busy => "55P03",
        LimboError::BusySnapshot => "40001",
        LimboError::ForeignKeyConstraint(_) => "23503",
        LimboError::Constraint(m) if m.starts_with("UNIQUE constraint failed") => "23505",
        LimboError::Constraint(m) if m.starts_with("NOT NULL constraint failed") => "23502",
        LimboError::Constraint(m) if m.starts_with("CHECK constraint failed") => "23514",
        LimboError::Constraint(m) if m.starts_with("invalid ") => "22P02",
        LimboError::Constraint(_) => "23000",
        LimboError::ParseError(m) if m.starts_with("Invalid statement:") => "42601",
        LimboError::ParseError(m)
            if m.starts_with("no such table")
                || m.starts_with("no such view")
                || (m.starts_with("relation \"") && m.ends_with("\" does not exist")) =>
        {
            "42P01"
        }
        // A foreign key's parent key is not one (an ALTER's added key; wire review 11 item 5).
        LimboError::ParseError(m)
            if m.starts_with("there is no unique constraint matching given keys")
                || m.starts_with("there is no primary key for referenced table")
                || m.starts_with("number of referencing and referenced columns") =>
        {
            "42830"
        }
        LimboError::ParseError(m) if m.starts_with("no such column") => "42703",
        LimboError::ParseError(m) if m.starts_with("there is no parameter") => "42P02",
        // A savepoint name that names none (wire review 6 item 6).
        LimboError::TxError(m) if m.starts_with("no such savepoint") => "3B001",
        LimboError::ParseError(m)
            if m.contains("is ambiguous") || m.starts_with("ambiguous column name") =>
        {
            "42702"
        }
        LimboError::ParseError(m) if m.starts_with("no such function") => "42883",
        LimboError::ParseError(m) if m.contains("specified more than once") => "42712",
        LimboError::ParseError(m) if m.contains("not supported") || m.contains("Unsupported") => {
            "0A000"
        }
        LimboError::ParseError(_) => "42601",
        LimboError::IntegerOverflow => "22003",
        LimboError::ReadOnly => "25006",
        LimboError::DatabaseFull(_) => "53100",
        LimboError::Interrupt => "57014",
        _ => "XX000",
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
            vec![(Vec::new(), Response::EmptyQuery)]
        } else {
            self.simple_with_notices(&query.query)
        };
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
        // NoopQueryParser keeps the text as it is; so does this.
        let types = message
            .type_oids
            .iter()
            .map(|o| Type::from_oid(*o))
            .collect();
        let id = message.name.unwrap_or_else(|| DEFAULT_NAME.to_owned());
        client
            .portal_store()
            .put_statement(Arc::new(StoredStatement::new(id, message.query, types)));
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
        // A statement's parameter count, where it is known from the text, is checked here, as
        // PostgreSQL checks every statement's at Bind: a branch call's (its $n), and that of a
        // statement the server answers without the engine (CHECKPOINT, a transaction verb), which
        // has none but those Parse declared. Neither was checked, so two values for
        // turso_branch_create($1) created the branch (wire review 10 item 5) and a value for
        // CHECKPOINT or BEGIN ran it (wire review 12 item 2). An engine statement's count is known
        // once it is prepared, and is checked at Execute (E5-QUEUE R2).
        let sql = &statement.statement;
        let required = if let Some(call) = branch_call(sql) {
            Some(
                parameter_types(&branch_call_types(&call), &statement.parameter_types)
                    .map_err(PgWireError::UserError)?
                    .len(),
            )
        } else if TxVerb::of(sql) != TxVerb::Other || is_checkpoint(sql) {
            Some(statement.parameter_types.len())
        } else {
            None
        };
        if let Some(required) = required {
            check_bind_arity(message.parameters.len(), &statement.id, required)
                .map_err(PgWireError::UserError)?;
        }
        let portal = Portal::try_new(&message, statement)?;
        client.portal_store().put_portal(Arc::new(portal));
        client
            .feed(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
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
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
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
            return Err(PgWireError::PortalNotFound(name.to_owned()));
        };
        client.set_state(PgWireConnectionState::QueryInProgress);
        let max_rows = message.max_rows.max(0) as usize;
        let state = portal.state();
        let mut state = state.lock().await;
        match &mut *state {
            PortalExecutionState::Initial => {
                match ExtendedQueryHandler::do_query(self, client, &portal, max_rows).await? {
                    Response::Query(mut results) => {
                        *state = if feed_rows(client, &mut results, max_rows).await? {
                            PortalExecutionState::Suspended(results)
                        } else {
                            PortalExecutionState::Finished
                        };
                    }
                    Response::Execution(tag)
                    | Response::TransactionStart(tag)
                    | Response::TransactionEnd(tag) => {
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
                        client
                            .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
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
            PortalExecutionState::Suspended(results) => {
                if !feed_rows(client, results, max_rows).await? {
                    *state = PortalExecutionState::Finished;
                }
            }
            PortalExecutionState::Finished => {
                client
                    .feed(PgWireBackendMessage::NoData(NoData::new()))
                    .await?;
            }
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
        // As pgwire does: the unnamed portal goes with its execution.
        if name == DEFAULT_NAME {
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
        let sql = &portal.statement.statement;
        let call = branch_call(sql);
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
        self.refuse_describe_if_aborted(&target.statement)?;
        if let Some(call) = branch_call(&target.statement) {
            let fields =
                branch_call_fields(&call, &Format::UnifiedText).map_err(PgWireError::UserError)?;
            let params = parameter_types(&branch_call_types(&call), &target.parameter_types)
                .map_err(PgWireError::UserError)?;
            return Ok(DescribeStatementResponse::new(params, fields));
        }
        // The special statements return no rows: NoData, from the text (their prepared stand-ins
        // have a dummy column). That their prepare is never what performs them is describe_prepare's
        // job, from the parse, so a form this text test misses is still not performed.
        if is_pg_non_query(&target.statement) {
            // The parameters the client declared, as declared, the rest as text (wire review 1
            // item 13).
            let declared = target
                .parameter_types
                .iter()
                .map(|t| t.clone().unwrap_or(Type::TEXT))
                .collect();
            return Ok(DescribeStatementResponse::new(declared, vec![]));
        }
        self.describe_prepare(&target.statement)?;
        let fields = self
            .described_fields(&Format::UnifiedText)
            .map_err(PgWireError::UserError)?;
        let params = self
            .described_parameters(&target.parameter_types)
            .map_err(PgWireError::UserError)?;
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
        self.refuse_describe_if_aborted(&portal.statement.statement)?;
        if let Some(call) = branch_call(&portal.statement.statement) {
            let fields = branch_call_fields(&call, &portal.result_column_format)
                .map_err(PgWireError::UserError)?;
            return Ok(DescribePortalResponse::new(fields));
        }
        if is_pg_non_query(&portal.statement.statement) {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        self.describe_prepare(&portal.statement.statement)?;
        let fields = self
            .described_fields(&portal.result_column_format)
            .map_err(PgWireError::UserError)?;
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
        // The extended protocol's results were fed as each Execute ran; its notices go out here, and
        // a statement a Describe kept but no Execute ran is dropped.
        let notices = {
            let mut st = self.state();
            st.described = None;
            std::mem::take(&mut st.notices)
        };
        for notice in notices {
            client
                .feed(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                    *notice,
                )))
                .await?;
        }
        let status = self.transaction_status();
        client.set_transaction_status(status);
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

/// Execute a query that returns rows and build a Query response. Each column has the type the
/// statement gives it ([`column_type`]), the one Describe reported, and each row is encoded as it
/// comes (wire review 1 item 14: no value inference, no row buffering beyond the reply's).
fn execute_query(
    stmt: &mut turso_core::Statement,
    format: &Format,
    types: &[Option<u32>],
    schema: &turso_core::schema::Schema,
    backoff: &mut Backoff,
) -> PgWireResult<Response> {
    let header = Arc::new(result_fields(stmt, types, format).map_err(PgWireError::UserError)?);
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
    backoff: &mut Backoff,
) -> PgWireResult<Response> {
    run_waiting(stmt, backoff, |_| Ok(())).map_err(engine_error)?;

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
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

/// Bind a portal's parameters to its statement, each converted from its text by its type (see
/// [`parameter_types`]): an undeclared parameter's is the one its context gives, the type Describe
/// reported, not one guessed from the value (an integer, then a float, then a boolean: '007' went
/// into a text column as 7; wire review 4 item 3). A parameter count other than the statement's
/// (08P01, as PostgreSQL's Bind answers), or a value the engine refuses, fails the bind. The
/// engine numbers PostgreSQL's $n as its parameter n.
fn bind_portal_parameters(
    stmt: &mut turso_core::Statement,
    portal: &Portal<String>,
    statement_types: &StatementTypes,
) -> PgWireResult<()> {
    let types = parameter_types(statement_types, &portal.statement.parameter_types)
        .map_err(PgWireError::UserError)?;
    check_bind_arity(portal.parameter_len(), &portal.statement.id, types.len())
        .map_err(PgWireError::UserError)?;
    // The format codes were checked at Bind ([`check_bind`]): none, one, or one per value, and
    // the values are as many as the statement's parameters (just above).
    for (i, pg_type) in types.iter().enumerate() {
        let value = match &portal.parameters[i] {
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

/// PostgreSQL's checks of a Bind message alone, made before BindComplete (exec_bind_message): every
/// parameter and result format code is 0 (text) or 1 (binary), else 22023 "unsupported format
/// code: N" (even for a NULL value), and a parameter-format list is empty, one code, or one per value
/// sent, else 08P01. They were made at Execute, after BindComplete, or for a branch call not at
/// all, and a single bad code was invisible there: pgwire folds one code into text (wire review 10
/// item 5). PostgreSQL refuses a bad result code at Execute instead, as it formats the first row
/// (E5-QUEUE R2).
fn check_bind(bind: &Bind) -> SqlResult<()> {
    if let Some(code) = bind
        .parameter_format_codes
        .iter()
        .chain(&bind.result_column_format_codes)
        .find(|c| !matches!(c, 0 | 1))
    {
        return Err(error("22023", format!("unsupported format code: {code}")));
    }
    let (codes, values) = (bind.parameter_format_codes.len(), bind.parameters.len());
    if codes > 1 && codes != values {
        return Err(error(
            "08P01",
            format!("bind message has {codes} parameter formats but {values} parameters"),
        ));
    }
    Ok(())
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
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;
    if let Some(element) = element_of(pg_type.oid()).and_then(Type::from_oid) {
        return pg_array_to_value(text, &element);
    }

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
                let data =
                    decode_hex(hex_str).map_err(|e| PgWireError::UserError(error("22P02", e)))?;
                Ok(Value::from_blob(data))
            } else {
                // Raw bytes as-is
                Ok(Value::from_blob(bytes.to_vec()))
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
    let s = text.trim_matches(|c: char| c.is_ascii_whitespace());
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
    if !inner
        .trim_matches(|c: char| c.is_ascii_whitespace())
        .is_empty()
    {
        let mut chars = inner.chars().peekable();
        loop {
            while chars.next_if(|c| c.is_ascii_whitespace()).is_some() {}
            let mut item = String::new();
            let quoted = chars.next_if_eq(&'"').is_some();
            if quoted {
                loop {
                    match chars.next().ok_or_else(malformed)? {
                        '"' => break,
                        '\\' => item.push(chars.next().ok_or_else(malformed)?),
                        c => item.push(c),
                    }
                }
                while chars.next_if(|c| c.is_ascii_whitespace()).is_some() {}
            } else {
                // Trailing whitespace is dropped, but not an escaped character's.
                let mut kept = 0;
                while let Some(c) = chars.next_if(|c| *c != ',') {
                    match c {
                        '{' | '}' => {
                            return Err(PgWireError::UserError(error(
                                "0A000",
                                "multidimensional array parameters are not supported".to_string(),
                            )))
                        }
                        '"' => return Err(malformed()),
                        '\\' => {
                            item.push(chars.next().ok_or_else(malformed)?);
                            kept = item.len();
                        }
                        c => {
                            item.push(c);
                            if !c.is_ascii_whitespace() {
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
            values.push(if !quoted && item.eq_ignore_ascii_case("null") {
                Value::Null
            } else {
                pg_bytes_to_value(item.as_bytes(), element).map_err(|_| {
                    PgWireError::UserError(error(
                        "22P02",
                        format!("invalid input syntax for type {element}: \"{item}\""),
                    ))
                })?
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

/// Decode PostgreSQL's hex bytea text (what follows `\x`) as its byteain reads it: pairs of hex
/// digits, whitespace skipped between pairs, its messages on bad input (22P02 at the caller). Read
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
        Session::new(Arc::new(Shared::new(
            db,
            path,
            1,
            std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
        )))
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
    /// review 1 item 6).
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
        s.shared
            .db
            .drop_branch("h")
            .expect("premise: the engine releases a branch a connection is open on");
        let fresh = s.shared.db.connect().unwrap().create_branch("h").unwrap();
        assert_ne!(
            Some(fresh),
            old,
            "premise: the re-created h is another branch"
        );
        ok(&s, "SELECT turso_branch_switch('h')");
        assert_eq!(
            on(&s),
            Some(fresh),
            "the switch stayed on the released branch"
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
        let stored = Arc::new(StoredStatement::new(String::new(), sql.to_string(), vec![]));
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

    /// A bytea parameter's hex is read byte by byte: a multi-byte character among the digits is
    /// an invalid digit (22P02, as PostgreSQL's byteain answers), never a slice inside it.
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
            assert_eq!(e.code, "22P02", "{text:?}: {e:?}");
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
