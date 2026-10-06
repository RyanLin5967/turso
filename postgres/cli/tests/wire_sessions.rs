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
    /// The type OIDs of the last RowDescription, if one came.
    oids: Option<Vec<u32>>,
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
                b'T' => {
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut p = 2;
                    let mut oids = Vec::with_capacity(n);
                    for _ in 0..n {
                        p += body[p..].iter().position(|&c| c == 0).unwrap() + 1;
                        oids.push(u32::from_be_bytes([
                            body[p + 6],
                            body[p + 7],
                            body[p + 8],
                            body[p + 9],
                        ]));
                        p += 18;
                    }
                    r.oids = Some(oids);
                }
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

// ---------------------------------------------------------------------------
// E5' findings (the benchmark's statements, compared with PostgreSQL 18)
// ---------------------------------------------------------------------------

/// Aggregates report a numeric type, as PostgreSQL does (count and sum of integers: bigint; avg: numeric). A
/// driver converts by the type OID, so a count reported as text reaches the client as a string.
#[test]
fn aggregates_report_numeric_types() {
    let dir = Scratch::new("aggtypes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, 'x')").ok("insert");
    let r = a
        .q("SELECT count(*), sum(id), min(id), max(id), avg(id) FROM t")
        .ok("aggregates");
    let oids = r.oids.clone().expect("no RowDescription");
    let int = [20u32, 21, 23];
    let number = [700u32, 701, 1700];
    assert_eq!(oids[0], 20, "count(*) is bigint: {oids:?}");
    assert_eq!(oids[1], 20, "sum(int) is bigint: {oids:?}");
    assert!(
        int.contains(&oids[2]) && int.contains(&oids[3]),
        "min/max(int): {oids:?}"
    );
    assert!(number.contains(&oids[4]), "avg(int): {oids:?}");
    assert_eq!(
        r.rows,
        vec![vec![
            Some("2".to_string()),
            Some("3".to_string()),
            Some("1".to_string()),
            Some("2".to_string()),
            Some("1.5".to_string())
        ]]
    );
}

/// CHECKPOINT is accepted, as PostgreSQL's post-load maintenance runs it (PREREG §7 S), on the trunk and on a
/// branch.
#[test]
fn checkpoint_is_accepted() {
    let dir = Scratch::new("checkpoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let r = a.q("CHECKPOINT").ok("checkpoint on the trunk");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert!(r.oids.is_none(), "CHECKPOINT returned a row set");
    a.q("SELECT turso_branch_create('c')").ok("create");
    a.q("SELECT turso_branch_switch('c')").ok("switch");
    a.q("UPDATE t SET v = 'c' WHERE id = 1").ok("write");
    let r = a.q("CHECKPOINT").ok("checkpoint on a branch");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert_eq!(a.q("SELECT v FROM t WHERE id = 1").single("after"), "c");
}

/// SET and TRUNCATE complete with PostgreSQL's command tags and no row set.
#[test]
fn set_and_truncate_report_their_command_tags() {
    let dir = Scratch::new("tags");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let r = a.q("SET search_path TO public").ok("set");
    assert_eq!(r.tags, vec!["SET".to_string()]);
    assert!(r.oids.is_none(), "SET returned a row set");
    let r = a.q("TRUNCATE t").ok("truncate");
    assert_eq!(r.tags, vec!["TRUNCATE TABLE".to_string()]);
    assert_eq!(a.q("SELECT count(*) FROM t").single("count"), "0");
}

/// A session cannot delete the branch it is on (PostgreSQL: "cannot drop the currently open database", 55006).
#[test]
fn deleting_the_sessions_own_branch_is_refused_with_55006() {
    let dir = Scratch::new("ownbranch");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('own')").ok("create");
    a.q("SELECT turso_branch_switch('own')").ok("switch");
    let e = a
        .q("SELECT turso_branch_delete('own')")
        .err("delete own branch");
    assert_eq!(e.code, "55006", "{e:?}");
    a.q("UPDATE t SET v = 'own' WHERE id = 1")
        .ok("the branch is still there");
    a.q("SELECT turso_branch_switch('main')").ok("leave");
    a.q("SELECT turso_branch_delete('own')")
        .ok("delete from elsewhere");
}

/// A branch another session holds is refused with 55006 (object_in_use), naming the branch, at a switch and at
/// startup.
#[test]
fn a_branch_held_by_another_session_is_refused_with_55006() {
    let dir = Scratch::new("held");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('h')").ok("create");
    let _b = server.connect_to("postgres/h").expect("b on h");
    let e = a.q("SELECT turso_branch_switch('h')").err("switch to held");
    assert_eq!(e.code, "55006", "{e:?}");
    assert!(e.message.contains("\"h\""), "{e:?}");
    let e = server
        .connect_to("postgres/h")
        .err()
        .expect("admitted on held");
    assert_eq!(e.code, "55006", "{e:?}");
}

/// END and ABORT complete with PostgreSQL's tags, COMMIT and ROLLBACK (pgbench ends every
/// transaction with END).
#[test]
fn end_and_abort_report_commit_and_rollback() {
    let dir = Scratch::new("endabort");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert");
    let r = a.q("END;").ok("end");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(r.status, b'I');
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (3, 'a')").ok("insert");
    let r = a.q("ABORT").ok("abort");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(a.q("SELECT count(*) FROM t").single("count"), "2");
}

/// A branch call that meets a held lock waits for it, as PostgreSQL's statements wait on locks,
/// rather than failing: here the trunk's first child is forked under the WAL write lock while
/// another session's transaction holds it, and the create completes once that transaction
/// commits.
#[test]
fn a_branch_create_waits_for_a_held_write_lock() {
    let dir = Scratch::new("waitlock");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'a')").ok("insert");
    let committer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        a.q("COMMIT").ok("commit");
        a
    });
    let t0 = Instant::now();
    let r = b.q("SELECT turso_branch_create('w')");
    let waited = t0.elapsed();
    let mut a = committer.join().unwrap();
    assert!(
        r.error.is_none(),
        "the create failed instead of waiting: {:?}",
        r.error
    );
    assert!(
        waited >= Duration::from_millis(250),
        "the create did not wait for the lock ({waited:?})"
    );
    b.q("SELECT turso_branch_switch('w')").ok("switch");
    assert_eq!(
        b.q("SELECT count(*) FROM t").single("rows on w"),
        "2",
        "the branch forked before the commit it waited for"
    );
    a.q("SELECT 1").ok("a is fine");
}

/// Foreign keys are enforced, as PostgreSQL always enforces them, on the trunk and on a branch:
/// a child row with no parent is refused, and ON DELETE CASCADE removes the children.
#[test]
fn foreign_keys_are_enforced_on_the_trunk_and_on_a_branch() {
    let dir = Scratch::new("fk");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("parent");
    a.q("CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE)")
        .ok("child");
    a.q("INSERT INTO p VALUES (1)").ok("parent row");
    a.q("INSERT INTO c VALUES (10, 1)").ok("child row");
    assert!(
        a.q("INSERT INTO c VALUES (11, 99)").error.is_some(),
        "a child row with no parent was accepted on the trunk"
    );
    a.q("SELECT turso_branch_create('fk')").ok("create");
    a.q("SELECT turso_branch_switch('fk')").ok("switch");
    assert!(
        a.q("INSERT INTO c VALUES (12, 98)").error.is_some(),
        "a child row with no parent was accepted on a branch"
    );
    a.q("DELETE FROM p WHERE id = 1").ok("delete parent");
    assert_eq!(
        a.q("SELECT count(*) FROM c").single("children"),
        "0",
        "ON DELETE CASCADE did not remove the child"
    );
}

/// A set-returning function in FROM names its one column after its alias, as PostgreSQL does
/// (pgbench -I G: `insert into pgbench_branches(bid, bbalance) select bid, 0 from
/// generate_series(1, 1) as bid`), or after the function with no alias.
#[test]
fn generate_series_in_from_names_its_column_after_the_alias() {
    let dir = Scratch::new("genseries");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE b(bid INT, bbalance INT)").ok("create");
    let r = a
        .q("INSERT INTO b(bid, bbalance) SELECT bid, 0 FROM generate_series(1, 3) AS bid")
        .ok("insert select from generate_series");
    assert_eq!(r.tags, vec!["INSERT 0 3".to_string()]);
    assert_eq!(a.q("SELECT sum(bid) FROM b").single("sum"), "6");
    let r = a
        .q("SELECT x * 2 FROM generate_series(1, 3) AS g(x) ORDER BY 1")
        .ok("column alias list");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("2".into())],
            vec![Some("4".into())],
            vec![Some("6".into())]
        ]
    );
    let r = a
        .q("SELECT generate_series FROM generate_series(5, 6) ORDER BY 1")
        .ok("no alias");
    assert_eq!(r.rows, vec![vec![Some("5".into())], vec![Some("6".into())]]);
}

/// Row-value comparisons work, as in PostgreSQL (BranchBench's RANGE_READ and RANGE_UPDATE walk a
/// composite key with `(a, b) >= (x, y) AND (a, b) <= (z, w)`).
#[test]
fn row_value_comparisons_work() {
    let dir = Scratch::new("rowvalue");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE k(a INT, b INT, PRIMARY KEY (a, b))")
        .ok("create");
    a.q("INSERT INTO k VALUES (1, 1), (1, 2), (2, 1), (2, 2), (3, 1)")
        .ok("insert");
    let r = a
        .q("SELECT a, b FROM k WHERE (a, b) >= (1, 2) AND (a, b) <= (2, 2) ORDER BY a, b")
        .ok("range");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("1".into()), Some("2".into())],
            vec![Some("2".into()), Some("1".into())],
            vec![Some("2".into()), Some("2".into())],
        ]
    );
    let r = a
        .q("UPDATE k SET b = b + 10 WHERE (a, b) > (2, 1)")
        .ok("update by row value");
    assert_eq!(r.tags, vec!["UPDATE 2".to_string()]);
}

fn constraint_fixture(a: &mut Wire) {
    a.q("CREATE TABLE p(id INT PRIMARY KEY, name TEXT)")
        .ok("parent");
    a.q(
        "CREATE TABLE c(id INT NOT NULL, pid INT, v VARCHAR(10), n NUMERIC(8, 2), \
         b BOOLEAN, ts TIMESTAMP, ch CHAR(3))",
    )
    .ok("child");
    a.q("CREATE INDEX c_v ON c(v)").ok("index");
    a.q("INSERT INTO p VALUES (1, 'one'), (2, 'two')")
        .ok("parents");
    a.q(
        "INSERT INTO c VALUES (10, 1, 'x', 12.50, true, '2026-10-06 01:02:03', 'ab'), \
         (11, 2, 'y', -0.01, false, NULL, NULL), (12, NULL, NULL, NULL, NULL, NULL, NULL)",
    )
    .ok("children");
}

const C_ROWS: &str = "SELECT id, pid, v, n, b, ts, ch FROM c ORDER BY id";

/// ALTER TABLE ... ADD PRIMARY KEY / FOREIGN KEY ... ON DELETE CASCADE / UNIQUE take effect, as in
/// PostgreSQL (pgbench -I p adds its primary keys this way; BranchBench's dump adds 13 foreign
/// keys), and keep the table's rows, values and types exactly, on the trunk and on a branch.
#[test]
fn alter_table_add_constraint_takes_effect_and_keeps_the_rows() {
    let dir = Scratch::new("addconstraint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    constraint_fixture(&mut a);
    let before = a.q(C_ROWS).ok("rows before");
    a.q("SELECT turso_branch_create('ac')").ok("create");
    for on in ["main", "ac"] {
        a.q(&format!("SELECT turso_branch_switch('{on}')"))
            .ok("switch");
        let r = a
            .q("ALTER TABLE c ADD PRIMARY KEY (id)")
            .ok("add primary key");
        assert_eq!(r.tags, vec!["ALTER TABLE".to_string()], "{on}");
        a.q("ALTER TABLE c ADD CONSTRAINT c_pid_fkey FOREIGN KEY (pid) REFERENCES p (id) ON DELETE CASCADE")
            .ok("add foreign key");
        a.q("ALTER TABLE c ADD UNIQUE (v)").ok("add unique");
        let after = a.q(C_ROWS).ok("rows after");
        assert_eq!(
            after.rows, before.rows,
            "{on}: the rebuild changed the rows"
        );
        assert_eq!(
            after.oids, before.oids,
            "{on}: the rebuild changed the column types"
        );
        assert!(
            a.q("INSERT INTO c (id) VALUES (10)").error.is_some(),
            "{on}: duplicate key accepted"
        );
        assert!(
            a.q("INSERT INTO c (id) VALUES (NULL)").error.is_some(),
            "{on}: NULL key accepted"
        );
        assert!(
            a.q("INSERT INTO c (id, pid) VALUES (20, 99)")
                .error
                .is_some(),
            "{on}: a child with no parent accepted"
        );
        assert!(
            a.q("INSERT INTO c (id, v) VALUES (21, 'x')")
                .error
                .is_some(),
            "{on}: duplicate unique"
        );
        a.q("DELETE FROM p WHERE id = 1").ok("delete parent");
        assert_eq!(
            a.q("SELECT count(*) FROM c WHERE pid = 1")
                .single("cascade"),
            "0",
            "{on}: ON DELETE CASCADE did not take"
        );
        assert_eq!(a.q("SELECT count(*) FROM c").single("left"), "2", "{on}");
    }
    a.q("SELECT turso_branch_switch('main')").ok("main");
}

/// An ADD CONSTRAINT the existing rows break fails, as PostgreSQL's validation does, and leaves
/// the table as it was; inside a transaction it rolls back with the transaction.
#[test]
fn alter_table_add_constraint_is_validated_and_atomic() {
    let dir = Scratch::new("addconstraint2");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    constraint_fixture(&mut a);
    a.q("INSERT INTO c (id, pid) VALUES (13, 77)").ok("orphan");
    let before = a.q(C_ROWS).ok("before");
    assert!(
        a.q("ALTER TABLE c ADD FOREIGN KEY (pid) REFERENCES p (id)")
            .error
            .is_some(),
        "an orphan row did not fail the new foreign key"
    );
    a.q("ALTER TABLE c ADD UNIQUE (pid)")
        .ok("unique on distinct values");
    assert_eq!(
        a.q(C_ROWS).ok("after").rows,
        before.rows,
        "a failed ALTER changed the table"
    );
    a.q("DELETE FROM c WHERE id = 13").ok("remove the orphan");
    a.q("BEGIN").ok("begin");
    a.q("ALTER TABLE c ADD FOREIGN KEY (pid) REFERENCES p (id)")
        .ok("add in a transaction");
    a.q("ROLLBACK").ok("rollback");
    a.q("INSERT INTO c (id, pid) VALUES (30, 99)")
        .ok("the rolled-back foreign key is gone");
    a.q("SELECT count(*) FROM c WHERE v = 'x'")
        .ok("the index still answers");
}

/// information_schema answers the introspection clients run (BranchBench reads a table's columns
/// and its primary key this way), with PostgreSQL's names and values.
#[test]
fn information_schema_describes_tables_columns_and_keys() {
    let dir = Scratch::new("infoschema");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE w(w_id INT PRIMARY KEY, name VARCHAR(10))")
        .ok("w");
    a.q(
        "CREATE TABLE s(s_i_id INT NOT NULL, s_w_id SMALLINT NOT NULL, \
         s_ytd NUMERIC(8, 2), s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id), \
         FOREIGN KEY (s_w_id) REFERENCES w (w_id))",
    )
    .ok("s");
    let r = a
        .q(
            "SELECT column_name, udt_name, is_nullable, character_maximum_length, \
            numeric_precision, numeric_scale FROM information_schema.columns \
            WHERE table_name = 's' ORDER BY ordinal_position",
        )
        .ok("columns");
    let row = |v: [&str; 6]| -> Vec<Option<String>> {
        v.iter()
            .map(|x| (!x.is_empty()).then(|| x.to_string()))
            .collect()
    };
    assert_eq!(
        r.rows,
        vec![
            row(["s_i_id", "int4", "NO", "", "32", "0"]),
            row(["s_w_id", "int2", "NO", "", "16", "0"]),
            row(["s_ytd", "numeric", "YES", "", "8", "2"]),
            row(["s_data", "varchar", "YES", "50", "", ""]),
        ]
    );
    let r = a
        .q(
            "SELECT column_name, ordinal_position FROM information_schema.key_column_usage \
            WHERE table_schema = 'public' AND table_name = 's' AND constraint_name = \
            (SELECT constraint_name FROM information_schema.table_constraints \
             WHERE table_schema = 'public' AND table_name = 's' \
             AND constraint_type = 'PRIMARY KEY') ORDER BY ordinal_position DESC",
        )
        .ok("primary key columns");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("s_i_id".into()), Some("2".into())],
            vec![Some("s_w_id".into()), Some("1".into())],
        ]
    );
    let r = a
        .q(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' \
            AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name",
        )
        .ok("tables");
    assert_eq!(r.rows, vec![vec![Some("s".into())], vec![Some("w".into())]]);
    let r = a
        .q(
            "SELECT constraint_type FROM information_schema.table_constraints \
            WHERE table_name = 's' ORDER BY constraint_type",
        )
        .ok("constraint types");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("FOREIGN KEY".into())],
            vec![Some("PRIMARY KEY".into())]
        ]
    );
    // A user table named like a view is still the user's.
    a.q("CREATE TABLE columns(x INT)")
        .ok("a table named columns");
    a.q("INSERT INTO columns VALUES (7)").ok("insert");
    assert_eq!(a.q("SELECT x FROM columns").single("user table"), "7");
}

/// CHAR(n) is PostgreSQL's blank-padded character type: a value reads back padded to n characters
/// with type bpchar (1042), trailing spaces do not count in comparisons or length, and a value
/// longer than n is refused (BranchBench's stock rows: 1001 of 1256 rows differed).
#[test]
fn char_n_is_blank_padded_as_in_postgres() {
    let dir = Scratch::new("bpchar");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ch(id INT PRIMARY KEY, c CHAR(6), one CHAR, v VARCHAR(6))")
        .ok("create");
    a.q("INSERT INTO ch VALUES (1, 'ab', 'x', 'ab'), (2, 'abc   ', NULL, 'ab  ')")
        .ok("insert");
    let r = a.q("SELECT c, one, v FROM ch ORDER BY id").ok("select");
    assert_eq!(r.oids, Some(vec![1042, 1042, 1043]));
    assert_eq!(
        r.rows,
        vec![
            vec![Some("ab    ".into()), Some("x".into()), Some("ab".into())],
            vec![Some("abc   ".into()), None, Some("ab  ".into())],
        ]
    );
    assert_eq!(
        a.q("SELECT id FROM ch WHERE c = 'ab'")
            .single("comparison ignores padding"),
        "1"
    );
    assert_eq!(
        a.q("SELECT length(c) FROM ch WHERE id = 2")
            .single("length"),
        "3"
    );
    assert!(
        a.q("INSERT INTO ch VALUES (3, 'abcdefg', NULL, NULL)")
            .error
            .is_some(),
        "a value longer than CHAR(6) was accepted"
    );
    a.q("INSERT INTO ch VALUES (4, 'abcdef    ', NULL, NULL)")
        .ok("trailing spaces beyond n are dropped, as PostgreSQL does");
}

/// pg_indexes lists a table's indexes under PostgreSQL's names (the primary key's as <t>_pkey, a
/// UNIQUE column's as <t>_<col>_key), and pg_database_size answers in bytes (BranchBench reads
/// both).
#[test]
fn pg_indexes_and_pg_database_size_answer() {
    let dir = Scratch::new("pgindexes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ix(id INT PRIMARY KEY, a INT, b TEXT UNIQUE)")
        .ok("create");
    a.q("CREATE INDEX ix_a ON ix(a)").ok("index");
    let r = a
        .q("SELECT indexname FROM pg_indexes WHERE tablename = 'ix' ORDER BY indexname")
        .ok("pg_indexes");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("ix_a".into())],
            vec![Some("ix_b_key".into())],
            vec![Some("ix_pkey".into())]
        ]
    );
    let r = a
        .q("SELECT indexname FROM pg_indexes WHERE tablename = 'ix' AND indexname NOT LIKE '%_pkey' ORDER BY 1")
        .ok("BranchBench's query");
    assert_eq!(r.rows.len(), 2);
    let size: i64 = a
        .q("SELECT pg_database_size(current_database())")
        .single("size")
        .parse()
        .expect("a size in bytes");
    assert!(size > 0, "pg_database_size = {size}");
}
