//! `tursopg --server` as a multi-session server with named branches (fastest-wire lane, PREREG v1
//! §11 M3): one OS thread and one engine connection per session, the branch SQL functions, connecting
//! a session to a branch by its startup database name, and the connection limit. Every test drives
//! the shipped binary over the wire with a minimal protocol-v3 client, so what is pinned is what a
//! stock PostgreSQL client sees.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Server and scratch directory
// ---------------------------------------------------------------------------

/// A scratch directory under the system temp dir, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "tursopg_wire_{name}_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn db(&self) -> PathBuf {
        self.0.join("w.db")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Server {
    child: Child,
    port: u16,
}

impl Server {
    /// Start `tursopg DB --server` on a kernel-assigned port, retrying on a lost bind race (see
    /// `start_tursopg_server` in tursopg.rs for why the port is not derived from a seed).
    fn start(db: &Path, extra: &[&str]) -> Self {
        for _ in 0..10 {
            let port = TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let addr = format!("127.0.0.1:{port}");
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_tursopg"));
            cmd.arg(db)
                .arg("--server")
                .arg(&addr)
                .args(extra)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // The server starts at macOS's default open-file soft limit (256) whatever this test
            // process raised its own to, so a server that serves C + 16 sessions must raise it.
            unsafe {
                use std::os::unix::process::CommandExt;
                cmd.pre_exec(|| {
                    let mut lim = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
                        lim.rlim_cur = lim.rlim_cur.min(256);
                        libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
                    }
                    Ok(())
                });
            }
            let mut child = cmd.spawn().expect("failed to start tursopg server");
            for _ in 0..100 {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                if TcpStream::connect(&addr).is_ok() && child.try_wait().unwrap().is_none() {
                    return Self { child, port };
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            child.kill().ok();
            child.wait().ok();
        }
        panic!("tursopg server did not start");
    }

    fn connect(&self) -> Wire {
        Wire::connect(self.port, "postgres").unwrap_or_else(|e| panic!("startup refused: {e:?}"))
    }

    fn connect_to(&self, database: &str) -> Result<Wire, WireError> {
        Wire::connect(self.port, database)
    }

    /// SIGKILL: nothing the server buffered survives unless it was made durable.
    fn kill(mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

// ---------------------------------------------------------------------------
// Minimal protocol-v3 client
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct WireError {
    code: String,
    message: String,
}

#[derive(Debug, Default)]
struct Reply {
    rows: Vec<Vec<Option<String>>>,
    tags: Vec<String>,
    error: Option<WireError>,
    /// The ReadyForQuery transaction status byte: b'I' idle, b'T' in a transaction, b'E' failed.
    status: u8,
}

impl Reply {
    fn ok(self, sql: &str) -> Self {
        if let Some(e) = &self.error {
            panic!("{sql}: {e:?}");
        }
        self
    }

    fn single(self, sql: &str) -> String {
        let r = self.ok(sql);
        assert_eq!(r.rows.len(), 1, "{sql}: rows {:?}", r.rows);
        assert_eq!(r.rows[0].len(), 1, "{sql}: row {:?}", r.rows[0]);
        r.rows[0][0]
            .clone()
            .unwrap_or_else(|| panic!("{sql}: NULL"))
    }

    fn err(self, sql: &str) -> WireError {
        self.error
            .unwrap_or_else(|| panic!("{sql}: expected an error, got rows {:?}", self.rows))
    }
}

struct Wire {
    s: TcpStream,
}

impl Wire {
    fn connect(port: u16, database: &str) -> Result<Self, WireError> {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s.set_nodelay(true).unwrap();
        let mut w = Self { s };
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in [("user", "postgres"), ("database", database)] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        w.s.write_all(&((body.len() + 4) as i32).to_be_bytes())
            .unwrap();
        w.s.write_all(&body).unwrap();
        let r = w.read_reply();
        match r.error {
            Some(e) => Err(e),
            None if r.status == 0 => Err(WireError {
                code: String::new(),
                message: "the server closed the connection during startup".to_string(),
            }),
            None => Ok(w),
        }
    }

    fn send(&mut self, tag: u8, body: &[u8]) {
        let mut m = Vec::with_capacity(body.len() + 5);
        m.push(tag);
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        self.s.write_all(&m).unwrap();
    }

    /// One simple-protocol query.
    fn q(&mut self, sql: &str) -> Reply {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body);
        self.read_reply()
    }

    /// One extended-protocol round: Parse (all parameters text), Bind, Describe portal, Execute, Sync.
    fn x(&mut self, sql: &str, params: &[&str]) -> Reply {
        let mut parse = vec![0u8]; // unnamed statement
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for _ in params {
            parse.extend_from_slice(&25i32.to_be_bytes()); // text
        }
        self.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8]; // unnamed portal, unnamed statement
        bind.extend_from_slice(&0i16.to_be_bytes()); // all parameters text
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for p in params {
            bind.extend_from_slice(&(p.len() as i32).to_be_bytes());
            bind.extend_from_slice(p.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes()); // all results text
        self.send(b'B', &bind);
        self.send(b'D', b"P\0");
        let mut exec = vec![0u8];
        exec.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &exec);
        self.send(b'S', &[]);
        self.read_reply()
    }

    /// Read messages up to and including ReadyForQuery (or the server closing the connection).
    fn read_reply(&mut self) -> Reply {
        let mut r = Reply::default();
        loop {
            let mut tag = [0u8; 1];
            if self.s.read_exact(&mut tag).is_err() {
                return r;
            }
            let mut len = [0u8; 4];
            self.s.read_exact(&mut len).unwrap();
            let mut body = vec![0u8; i32::from_be_bytes(len) as usize - 4];
            self.s.read_exact(&mut body).unwrap();
            match tag[0] {
                b'D' => {
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut p = 2;
                    let mut row = Vec::with_capacity(n);
                    for _ in 0..n {
                        let l =
                            i32::from_be_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]);
                        p += 4;
                        if l < 0 {
                            row.push(None);
                        } else {
                            let l = l as usize;
                            row.push(Some(String::from_utf8_lossy(&body[p..p + l]).into_owned()));
                            p += l;
                        }
                    }
                    r.rows.push(row);
                }
                b'C' => r.tags.push(cstr(&body)),
                b'E' => {
                    // Only the first error of a reply is kept.
                    if r.error.is_none() {
                        r.error = Some(error_fields(&body));
                    }
                }
                b'Z' => {
                    r.status = body[0];
                    return r;
                }
                _ => {}
            }
        }
    }
}

fn cstr(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

fn error_fields(body: &[u8]) -> WireError {
    let mut e = WireError {
        code: String::new(),
        message: String::new(),
    };
    let mut p = 0;
    while p < body.len() && body[p] != 0 {
        let field = body[p];
        let v = cstr(&body[p + 1..]);
        p += 1 + v.len() + 1;
        match field {
            b'C' => e.code = v,
            b'M' => e.message = v,
            _ => {}
        }
    }
    e
}

/// Threads of process `pid` as the OS reports them.
fn thread_count(pid: u32) -> usize {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        status
            .lines()
            .find_map(|l| l.strip_prefix("Threads:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
    #[cfg(not(target_os = "linux"))]
    {
        // `ps -M` prints one line per thread after a header.
        let out = Command::new("ps")
            .args(["-M", "-p", &pid.to_string()])
            .output()
            .unwrap();
        assert!(out.status.success(), "ps -M failed");
        String::from_utf8_lossy(&out.stdout).lines().count() - 1
    }
}

/// Raise this process's open-file soft limit to at least `want` (each session is one socket here).
fn raise_nofile(want: u64) {
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim), 0);
        if lim.rlim_cur < want as libc::rlim_t {
            lim.rlim_cur = (want as libc::rlim_t).min(lim.rlim_max);
            assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &lim), 0);
        }
        assert!(
            lim.rlim_cur >= want as libc::rlim_t,
            "the hard open-file limit {} is below {want}",
            lim.rlim_max
        );
    }
}

fn seeded(server: &Server) -> Wire {
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)")
        .ok("create");
    a.q("INSERT INTO t VALUES (1, 'trunk')").ok("insert");
    a
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// Every session is served by a thread of its own, and the thread goes with the session.
#[test]
fn each_session_runs_on_its_own_thread() {
    let dir = Scratch::new("threads");
    let server = Server::start(&dir.db(), &[]);
    let mut first = server.connect();
    first.q("SELECT 1").ok("select 1");
    let before = thread_count(server.pid());
    let mut sessions: Vec<Wire> = (0..40).map(|_| server.connect()).collect();
    for s in &mut sessions {
        s.q("SELECT 1").ok("select 1");
    }
    let during = thread_count(server.pid());
    assert!(
        during >= before + 40,
        "40 more sessions, but threads went {before} -> {during}"
    );
    drop(sessions);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut after = thread_count(server.pid());
    while after > before && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        after = thread_count(server.pid());
    }
    assert!(
        after <= before,
        "40 sessions closed, but threads went {before} -> {during} -> {after}"
    );
}

/// An open transaction belongs to its session: another session neither sees its uncommitted rows
/// nor runs inside it.
#[test]
fn two_sessions_do_not_share_a_transaction() {
    let dir = Scratch::new("tx");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert");
    assert_eq!(
        b.q("SELECT count(*) FROM t").single("count in b"),
        "1",
        "b sees a's uncommitted row"
    );
    assert!(
        b.q("BEGIN").error.is_none(),
        "b cannot begin while a is in a transaction: they share one"
    );
    b.q("ROLLBACK").ok("rollback b");
    a.q("COMMIT").ok("commit a");
    assert_eq!(b.q("SELECT count(*) FROM t").single("count in b"), "2");
}

/// A ROLLBACK discards only its own session's writes (PREREG §2 E2 Sessions).
#[test]
fn a_rollback_discards_only_its_own_sessions_writes() {
    let dir = Scratch::new("rollback");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin a");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert a");
    b.q("BEGIN").ok("begin b");
    b.q("ROLLBACK").ok("rollback b");
    a.q("COMMIT").ok("commit a");
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 2").single("a's row"),
        "a",
        "b's rollback discarded a's write"
    );
}

/// A write that conflicts with another session's open transaction serialises or fails with SQLSTATE
/// 40001 or 55P03, and is never silently merged into the other transaction (PREREG §2 E2 Sessions).
#[test]
fn a_conflicting_write_fails_with_a_retryable_sqlstate_or_serialises() {
    let dir = Scratch::new("conflict");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin a");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert a");
    let r = b.q("INSERT INTO t VALUES (3, 'b')");
    if let Some(e) = &r.error {
        assert!(
            e.code == "55P03" || e.code == "40001",
            "the conflicting write failed with {e:?}"
        );
    }
    let b_wrote = r.error.is_none();
    a.q("ROLLBACK").ok("rollback a");
    let n = b.q("SELECT count(*) FROM t WHERE id = 3").single("b's row");
    assert_eq!(
        n,
        if b_wrote { "1" } else { "0" },
        "b's autocommit write was merged into a's transaction (b succeeded={b_wrote})"
    );
}

/// ReadyForQuery reports the session's transaction state, which psycopg 3 and libpq's
/// PQtransactionStatus read to decide whether to send BEGIN.
#[test]
fn ready_for_query_reports_the_transaction_state() {
    let dir = Scratch::new("status");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    assert_eq!(a.q("SELECT 1").status, b'I');
    assert_eq!(a.q("BEGIN").status, b'T', "after BEGIN");
    assert_eq!(a.q("INSERT INTO t VALUES (2, 'a')").status, b'T', "inside");
    assert_eq!(
        a.x("SELECT v FROM t WHERE id = $1", &["2"]).status,
        b'T',
        "extended, inside"
    );
    assert_eq!(a.q("COMMIT").status, b'I', "after COMMIT");
    assert_eq!(a.q("BEGIN").status, b'T');
    assert_eq!(a.q("ROLLBACK").status, b'I', "after ROLLBACK");
}

/// The default connection limit admits C + 16 sessions at C = 1024 (PREREG §6 Server configuration
/// (1)), each one live.
#[test]
fn the_default_limit_admits_1040_live_sessions() {
    raise_nofile(1200);
    let dir = Scratch::new("many");
    let server = Server::start(&dir.db(), &[]);
    seeded(&server);
    let mut sessions = Vec::with_capacity(1040);
    for i in 0..1040 {
        match server.connect_to("postgres") {
            Ok(w) => sessions.push(w),
            Err(e) => panic!("session {i} refused: {e:?}"),
        }
    }
    for (i, s) in sessions.iter_mut().enumerate() {
        assert_eq!(
            s.q("SELECT count(*) FROM t").single("count"),
            "1",
            "session {i}"
        );
    }
}

/// `--max-connections N` admits N sessions and refuses the next with SQLSTATE 53300, as PostgreSQL
/// does ("sorry, too many clients already"); a closed session frees its place.
#[test]
fn max_connections_refuses_the_next_session_with_53300() {
    let dir = Scratch::new("limit");
    let server = Server::start(&dir.db(), &["--max-connections", "3"]);
    let mut held: Vec<Wire> = (0..3).map(|_| server.connect()).collect();
    for s in &mut held {
        s.q("SELECT 1").ok("select 1");
    }
    let e = server
        .connect_to("postgres")
        .err()
        .expect("a fourth session was admitted");
    assert_eq!(e.code, "53300", "{e:?}");
    drop(held.pop());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match server.connect_to("postgres") {
            Ok(mut w) => {
                w.q("SELECT 1").ok("select 1");
                break;
            }
            Err(e) if Instant::now() < deadline => {
                assert_eq!(e.code, "53300", "{e:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("a closed session did not free its place: {e:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Named branches
// ---------------------------------------------------------------------------

/// Create, switch to, write on, switch away from and delete a named branch, all from SQL; the
/// branch is isolated from the trunk and from another session on the trunk.
#[test]
fn branch_functions_create_switch_and_delete() {
    let dir = Scratch::new("branch");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();

    let r = a.q("SELECT turso_branch_create('b1')").ok("create");
    assert_eq!(r.tags, vec!["SELECT 1".to_string()]);
    assert_eq!(r.rows.len(), 1);
    r.rows[0][0]
        .as_deref()
        .unwrap()
        .parse::<i64>()
        .expect("create returns the branch id");
    assert_eq!(
        a.q("SELECT turso_branch_current()").single("current"),
        "main"
    );

    assert_eq!(
        a.q("SELECT turso_branch_switch('b1')").single("switch"),
        "b1"
    );
    assert_eq!(a.q("SELECT turso_branch_current()").single("current"), "b1");
    let r = a.q("UPDATE t SET v = 'b1' WHERE id = 1").ok("update on b1");
    assert_eq!(r.tags, vec!["UPDATE 1".to_string()]);
    assert_eq!(a.q("SELECT v FROM t WHERE id = 1").single("on b1"), "b1");
    assert_eq!(
        b.q("SELECT v FROM t WHERE id = 1").single("trunk from b"),
        "trunk",
        "the branch's write reached the trunk"
    );

    assert_eq!(
        a.q("SELECT turso_branch_switch('main')").single("switch"),
        "main"
    );
    assert_eq!(a.q("SELECT v FROM t WHERE id = 1").single("trunk"), "trunk");
    a.q("SELECT turso_branch_switch('b1')").ok("back to b1");
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 1").single("b1 again"),
        "b1",
        "the branch lost its write"
    );

    a.q("SELECT turso_branch_switch('main')").ok("to main");
    assert_eq!(
        a.q("SELECT turso_branch_delete('b1')").single("delete"),
        "b1"
    );
    let e = a
        .q("SELECT turso_branch_switch('b1')")
        .err("switch to a deleted branch");
    assert_eq!(e.code, "3D000", "{e:?}");
    a.q("SELECT turso_branch_create('b1')")
        .ok("the deleted branch's name is free");
    a.q("SELECT turso_branch_switch('b1')").ok("switch");
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 1").single("new b1"),
        "trunk",
        "a re-created name forks the trunk afresh"
    );
}

/// A branch forked from a branch sees its parent's writes at the fork and nothing after.
#[test]
fn a_branch_forks_from_the_sessions_current_branch() {
    let dir = Scratch::new("nested");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('p')").ok("create p");
    a.q("SELECT turso_branch_switch('p')").ok("switch p");
    a.q("UPDATE t SET v = 'p' WHERE id = 1").ok("write p");
    a.q("SELECT turso_branch_create('c')").ok("create c from p");
    a.q("UPDATE t SET v = 'p2' WHERE id = 1")
        .ok("write p after the fork");
    a.q("SELECT turso_branch_switch('c')").ok("switch c");
    assert_eq!(a.q("SELECT v FROM t WHERE id = 1").single("c"), "p");
}

/// A session connects straight to a branch with startup database `<db>/<branch>`; a branch serves
/// one session at a time, and a name with no branch is refused with SQLSTATE 3D000.
#[test]
fn a_session_connects_to_a_branch_by_its_startup_database_name() {
    let dir = Scratch::new("startup");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b2')").ok("create");
    a.q("SELECT turso_branch_switch('b2')").ok("switch");
    a.q("UPDATE t SET v = 'b2' WHERE id = 1").ok("write");
    a.q("SELECT turso_branch_switch('main')").ok("leave");

    let mut b = server.connect_to("postgres/b2").expect("connect to b2");
    assert_eq!(b.q("SELECT turso_branch_current()").single("current"), "b2");
    assert_eq!(b.q("SELECT v FROM t WHERE id = 1").single("on b2"), "b2");

    let e = server
        .connect_to("postgres/b2")
        .err()
        .expect("a second session on b2 was admitted");
    assert!(!e.message.is_empty(), "{e:?}");
    let e = a
        .q("SELECT turso_branch_switch('b2')")
        .err("switch to b2 while b holds it");
    assert!(!e.message.is_empty(), "{e:?}");

    let e = server
        .connect_to("postgres/nope")
        .err()
        .expect("a session on a missing branch was admitted");
    assert_eq!(e.code, "3D000", "{e:?}");

    drop(b);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match server.connect_to("postgres/b2") {
            Ok(mut c) => {
                assert_eq!(c.q("SELECT v FROM t WHERE id = 1").single("on b2"), "b2");
                break;
            }
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("b2 stayed held after its session closed: {e:?}"),
        }
    }
}

/// Branch calls are refused inside a transaction block with SQLSTATE 25001, as PostgreSQL refuses
/// CREATE DATABASE there, and the refusal aborts the transaction as any error does in PostgreSQL:
/// the session reports 'E', refuses everything but its end with 25P02, and COMMIT rolls it back.
#[test]
fn branch_calls_inside_a_transaction_are_refused_with_25001() {
    let dir = Scratch::new("intx");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('x')").ok("create x");
    for sql in [
        "SELECT turso_branch_create('y')",
        "SELECT turso_branch_switch('x')",
        "SELECT turso_branch_switch('main')",
        "SELECT turso_branch_delete('x')",
    ] {
        a.q("BEGIN").ok("begin");
        a.q("INSERT INTO t VALUES (2, 'a')").ok("insert");
        let r = a.q(sql);
        assert_eq!(
            r.status, b'E',
            "{sql}: the transaction is not marked failed"
        );
        let e = r.err(sql);
        assert_eq!(e.code, "25001", "{sql}: {e:?}");
        let e = a.q("SELECT 1").err("a statement in the failed transaction");
        assert_eq!(e.code, "25P02", "{sql}: {e:?}");
        let r = a.q("COMMIT").ok("commit of a failed transaction");
        assert_eq!(r.tags, vec!["ROLLBACK".to_string()], "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
        assert_eq!(
            a.q("SELECT count(*) FROM t").single("count"),
            "1",
            "{sql}: committed"
        );
    }
    assert_eq!(
        a.q("SELECT turso_branch_current()").single("current"),
        "main"
    );
    a.q("SELECT turso_branch_switch('x')")
        .ok("x is still there");
}

/// Any error inside an explicit transaction aborts it, as in PostgreSQL: nothing of it commits,
/// and a client that keeps going is told so (25P02) rather than having half a transaction commit.
#[test]
fn an_error_inside_a_transaction_aborts_it_as_in_postgres() {
    let dir = Scratch::new("abort");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert 2");
    let r = a.q("INSERT INTO t VALUES (1, 'duplicate')");
    assert!(r.error.is_some(), "a duplicate key was accepted");
    assert_eq!(r.status, b'E');
    let r = a.q("INSERT INTO t VALUES (3, 'c')");
    assert_eq!(r.status, b'E');
    assert_eq!(r.err("insert 3").code, "25P02");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t").single("count"),
        "1",
        "part of a failed transaction committed"
    );
    // ROLLBACK and COMMIT outside a transaction are accepted with no effect (PostgreSQL warns).
    assert_eq!(a.q("ROLLBACK").ok("rollback").status, b'I');
    assert_eq!(a.q("COMMIT").ok("commit").status, b'I');
}

/// ROLLBACK TO SAVEPOINT recovers a failed transaction, as in PostgreSQL.
#[test]
fn rollback_to_savepoint_recovers_a_failed_transaction() {
    let dir = Scratch::new("savepoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert 2");
    a.q("SAVEPOINT s").ok("savepoint");
    assert_eq!(a.q("INSERT INTO t VALUES (1, 'duplicate')").status, b'E');
    assert_eq!(
        a.q("ROLLBACK TO SAVEPOINT s").ok("rollback to").status,
        b'T'
    );
    a.q("INSERT INTO t VALUES (3, 'c')").ok("insert 3");
    assert_eq!(a.q("COMMIT").ok("commit").tags, vec!["COMMIT".to_string()]);
    assert_eq!(a.q("SELECT count(*) FROM t").single("count"), "3");
}

/// 'main' names the trunk, so no branch may take it.
#[test]
fn the_trunk_name_main_is_reserved() {
    let dir = Scratch::new("main");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let e = a.q("SELECT turso_branch_create('main')").err("create main");
    assert!(e.message.contains("main"), "{e:?}");
}

/// The branch functions take bind parameters through the extended protocol (psycopg 3 and JDBC
/// send every statement that way).
#[test]
fn branch_functions_take_bind_parameters() {
    let dir = Scratch::new("extended");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let r = a
        .x("SELECT turso_branch_create($1)", &["b5"])
        .ok("create $1");
    assert_eq!(r.rows.len(), 1, "{r:?}");
    assert_eq!(
        a.x("SELECT turso_branch_switch($1)", &["b5"])
            .single("switch $1"),
        "b5"
    );
    assert_eq!(
        a.x("SELECT turso_branch_current()", &[]).single("current"),
        "b5"
    );
    a.x("UPDATE t SET v = $1 WHERE id = 1", &["b5"])
        .ok("update");
    a.x("SELECT turso_branch_switch($1)", &["main"]).ok("main");
    assert_eq!(
        a.x("SELECT turso_branch_delete($1)", &["b5"])
            .single("delete $1"),
        "b5"
    );
}

/// The server opens the database with durable branches by default: a branch and its write survive
/// SIGKILL of the server and are connectable by name after a restart (PREREG §2 E3).
#[test]
fn a_branch_survives_a_server_kill_and_restart() {
    let dir = Scratch::new("restart");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b3')").ok("create");
    a.q("SELECT turso_branch_switch('b3')").ok("switch");
    a.q("INSERT INTO t VALUES (3, 'b3')").ok("write");
    drop(a);
    server.kill();

    let server = Server::start(&dir.db(), &[]);
    let mut b = server
        .connect_to("postgres/b3")
        .expect("connect to b3 after restart");
    assert_eq!(b.q("SELECT v FROM t WHERE id = 3").single("b3's row"), "b3");
    let mut m = server.connect();
    assert_eq!(m.q("SELECT count(*) FROM t").single("trunk"), "1");
}
