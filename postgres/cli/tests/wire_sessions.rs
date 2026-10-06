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
    /// The parameter type OIDs of the last ParameterDescription, if one came.
    params: Option<Vec<u32>>,
    /// Every NoticeResponse of the reply, in order.
    notices: Vec<WireError>,
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

    /// [`Wire::x`] with no parameters, its five messages written in ONE write, so the server reads
    /// them in one read and its system calls per statement do not depend on how TCP split them.
    fn x_one_write(&mut self, sql: &str) -> Reply {
        let mut out = Vec::new();
        let mut put = |tag: u8, body: &[u8]| {
            out.push(tag);
            out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            out.extend_from_slice(body);
        };
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        put(b'P', &parse);
        put(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        put(b'D', b"P\0");
        put(b'E', &[0, 0, 0, 0, 0]);
        put(b'S', &[]);
        self.s.write_all(&out).unwrap();
        self.read_reply()
    }

    /// Parse with no declared parameter types, Describe the statement, Sync: what asyncpg,
    /// tokio-postgres and pgx send to learn a statement's parameters.
    fn describe_statement(&mut self, sql: &str) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        self.send(b'P', &parse);
        self.send(b'D', b"S\0");
        self.send(b'S', &[]);
        self.read_reply()
    }

    /// A pipeline: Parse, Bind, Describe portal and Execute for each statement (no parameters),
    /// then one Sync, as pgjdbc's batches and libpq's pipeline mode send them.
    fn pipeline(&mut self, sqls: &[&str]) -> Reply {
        for sql in sqls {
            let mut parse = vec![0u8];
            parse.extend_from_slice(sql.as_bytes());
            parse.push(0);
            parse.extend_from_slice(&0i16.to_be_bytes());
            self.send(b'P', &parse);
            let mut bind = vec![0u8, 0u8];
            bind.extend_from_slice(&0i16.to_be_bytes());
            bind.extend_from_slice(&0i16.to_be_bytes());
            bind.extend_from_slice(&0i16.to_be_bytes());
            self.send(b'B', &bind);
            self.send(b'D', b"P\0");
            let mut exec = vec![0u8];
            exec.extend_from_slice(&0i32.to_be_bytes());
            self.send(b'E', &exec);
        }
        self.send(b'S', &[]);
        self.read_reply()
    }

    /// [`Wire::x`] with each parameter's declared type OID (0: unspecified), format code (0 text,
    /// 1 binary) and bytes.
    fn xt(&mut self, sql: &str, params: &[(u32, i16, &[u8])]) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for (oid, _, _) in params {
            parse.extend_from_slice(&oid.to_be_bytes());
        }
        self.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for (_, format, _) in params {
            bind.extend_from_slice(&format.to_be_bytes());
        }
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for (_, _, value) in params {
            bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bind.extend_from_slice(value);
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
                b'N' => r.notices.push(error_fields(&body)),
                b't' => {
                    let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                    r.params = Some(
                        (0..n)
                            .map(|i| {
                                let p = 2 + 4 * i;
                                u32::from_be_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]])
                            })
                            .collect(),
                    );
                }
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

/// pgbench's table probe runs, as on PostgreSQL 18: a CROSS JOIN LATERAL of a FROM-less subselect
/// (one row of expressions over the outer row), current_schemas(true) and array_position. pgbench
/// stops when it fails, so pgbench cannot run without it.
#[test]
fn pgbench_table_probe_runs() {
    let dir = Scratch::new("pgbenchprobe");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE pgbench_accounts(aid INT NOT NULL, bid INT, abalance INT, filler CHAR(84))")
        .ok("create");
    let probe = "select o.n, p.partstrat, pg_catalog.count(i.inhparent) \
        from pg_catalog.pg_class as c \
        join pg_catalog.pg_namespace as n on (n.oid = c.relnamespace) \
        cross join lateral (select pg_catalog.array_position(pg_catalog.current_schemas(true), n.nspname)) as o(n) \
        left join pg_catalog.pg_partitioned_table as p on (p.partrelid = c.oid) \
        left join pg_catalog.pg_inherits as i on (c.oid = i.inhparent) \
        where c.relname = 'pgbench_accounts' and o.n is not null \
        group by 1, 2 \
        order by 1 asc \
        limit 1";
    let r = a.q(probe).ok("pgbench's probe");
    assert_eq!(r.rows, vec![vec![Some("2".into()), None, Some("0".into())]]);
    assert_eq!(
        a.q("SELECT current_schemas(false)")
            .single("current_schemas(false)"),
        "{public}"
    );
    // The same LATERAL shape over a user table, with two expressions.
    a.q("CREATE TABLE lt(x INT)").ok("lt");
    a.q("INSERT INTO lt VALUES (1), (2)").ok("rows");
    let r = a
        .q("SELECT t.x, o.d, o.s FROM lt AS t CROSS JOIN LATERAL (SELECT t.x * 2, t.x + 10) AS o(d, s) ORDER BY t.x")
        .ok("lateral over a user table");
    assert_eq!(
        r.rows,
        vec![
            vec![Some("1".into()), Some("2".into()), Some("11".into())],
            vec![Some("2".into()), Some("4".into()), Some("12".into())]
        ]
    );
}

/// DDL completes with PostgreSQL's command tag for its object (the E2 suite's CREATE SEQUENCE read
/// CREATE TABLE): the verb and the object, whatever modifiers come between.
#[test]
fn ddl_statements_report_their_command_tags() {
    let dir = Scratch::new("ddltags");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for (sql, tag) in [
        ("CREATE SEQUENCE sq", "CREATE SEQUENCE"),
        ("CREATE UNIQUE INDEX t_v_u ON t (v)", "CREATE INDEX"),
        ("CREATE INDEX IF NOT EXISTS t_v_i ON t (v)", "CREATE INDEX"),
        ("CREATE VIEW tv AS SELECT id FROM t", "CREATE VIEW"),
        ("CREATE TABLE IF NOT EXISTS t2 (a INT)", "CREATE TABLE"),
        ("DROP INDEX t_v_u", "DROP INDEX"),
        ("DROP VIEW tv", "DROP VIEW"),
        ("DROP SEQUENCE sq", "DROP SEQUENCE"),
        ("DROP TABLE IF EXISTS t2", "DROP TABLE"),
    ] {
        assert_eq!(a.q(sql).ok(sql).tags, vec![tag.to_string()], "{sql}");
    }
}

/// turso_branch_stats() reports the server process's own counters — unix system calls, mach
/// traps, instructions retired, cycles — for the wire-versus-embedded budgets: one row of four
/// int8 columns that never decrease, where a branch create between two reads costs system calls.
#[cfg(target_vendor = "apple")]
#[test]
fn branch_stats_reports_the_servers_counters() {
    let dir = Scratch::new("stats");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let read = |a: &mut Wire| -> Vec<i64> {
        let r = a.q("SELECT turso_branch_stats()").ok("stats");
        assert_eq!(r.oids, Some(vec![20, 20, 20, 20, 20]), "five int8 columns");
        assert_eq!(r.rows.len(), 1);
        r.rows[0]
            .iter()
            .map(|v| {
                v.as_deref()
                    .expect("a counter is never NULL here")
                    .parse()
                    .unwrap()
            })
            .collect()
    };
    let before = read(&mut a);
    a.q("SELECT turso_branch_create('st')").ok("create");
    let after = read(&mut a);
    for (i, (b, c)) in before.iter().zip(&after).enumerate() {
        assert!(c >= b, "counter {i} went down: {before:?} -> {after:?}");
    }
    assert!(
        after[0] > before[0],
        "a create made no system call: {before:?} -> {after:?}"
    );
    assert!(
        after[2] > before[2],
        "a create retired no instruction: {before:?} -> {after:?}"
    );
    // Inside a transaction too: it reads, it changes nothing.
    a.q("BEGIN").ok("begin");
    read(&mut a);
    a.q("COMMIT").ok("commit");
}

/// A branch another session is on cannot be deleted under it (PostgreSQL: DROP DATABASE of a
/// database other sessions use fails with 55006), and once that session leaves, it can.
#[test]
fn deleting_a_branch_another_session_is_on_is_refused_with_55006() {
    let dir = Scratch::new("delheld");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('x')").ok("create");
    let mut b = server.connect_to("postgres/x").expect("b on x");
    let e = a.q("SELECT turso_branch_delete('x')").err("delete under b");
    assert_eq!(e.code, "55006", "{e:?}");
    b.q("UPDATE t SET v = 'b' WHERE id = 1")
        .ok("b's branch is still there");
    b.q("SELECT turso_branch_switch('main')").ok("b leaves");
    a.q("SELECT turso_branch_delete('x')")
        .ok("delete once b left");
    a.q("SELECT turso_branch_create('y')").ok("create y");
    let mut c = server.connect_to("postgres/y").expect("c on y");
    drop(c.q("SELECT 1"));
    drop(c);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = a.q("SELECT turso_branch_delete('y')");
        match r.error {
            None => break,
            Some(e) if e.code == "55006" && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20))
            }
            Some(e) => panic!("y stayed held after c disconnected: {e:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire review 1 (frontier/fastest/reviews/REVIEW-1-wire-db11f642d..472023b72.md)
// ---------------------------------------------------------------------------

/// Review 1 item 1: an error the engine answers by rolling the whole transaction back itself (an
/// integer overflow in sum()) still aborts the block as PostgreSQL does: 25P02 until its end,
/// status 'E', and COMMIT answered ROLLBACK with nothing of the block kept. Before, the block's
/// state was read from the engine after the error, saw autocommit, and the rest committed.
#[test]
fn an_engine_side_rollback_inside_a_block_still_aborts_it() {
    let dir = Scratch::new("enginerollback");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE big(v BIGINT)").ok("big");
    a.q("INSERT INTO big VALUES (9223372036854775807), (1)")
        .ok("rows");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    let r = a.q("SELECT sum(v) FROM big");
    assert!(
        r.error.is_some(),
        "sum overflowed without an error: {:?}",
        r.rows
    );
    assert_eq!(r.status, b'E', "the block is not marked failed");
    let r = a.q("INSERT INTO t VALUES (3, 'three')");
    assert_eq!(r.status, b'E');
    assert_eq!(r.err("insert 3").code, "25P02");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id IN (2, 3)")
            .single("rows"),
        "0",
        "part of a failed block committed"
    );
}

/// Review 1 item 1: a COMMIT that fails ends the block (PostgreSQL rolls it back): the session is
/// idle after it, not left inside the transaction.
#[test]
fn a_failed_commit_leaves_the_session_idle() {
    let dir = Scratch::new("failedcommit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("p");
    a.q("CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)")
        .ok("c");
    a.q("BEGIN").ok("begin");
    let r = a.q("INSERT INTO c VALUES (1, 99)");
    if r.error.is_none() {
        // A deferred foreign key fails at COMMIT, as in PostgreSQL.
        let r = a.q("COMMIT");
        assert!(r.error.is_some(), "an orphan row committed");
        assert_eq!(
            r.status, b'I',
            "a failed COMMIT left the session in a block"
        );
        assert_eq!(a.q("SELECT count(*) FROM c").single("rows"), "0");
    }
}

/// Review 1 item 2: a statement that fails at Describe (the extended protocol, as libpq's
/// PQsendQueryParams, pgjdbc and tokio-postgres send every statement) aborts the block too: the
/// Sync reports 'E', and COMMIT rolls the block back. Before, Describe set nothing, Sync said 'T',
/// and COMMIT kept the writes made before the failed statement.
#[test]
fn a_describe_time_error_inside_a_block_aborts_it() {
    let dir = Scratch::new("describeerror");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    let r = a.x("SELECT * FROM nope", &[]);
    assert!(
        r.error.is_some(),
        "a missing table described without an error"
    );
    assert_eq!(r.status, b'E', "Sync after the failed statement");
    let r = a.x("SELECT 1", &[]);
    assert_eq!(r.err("a statement in the failed block").code, "25P02");
    assert_eq!(r.status, b'E');
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
        "0",
        "the block committed past its failed statement"
    );
}

/// A branch call's cast is read only when it changes nothing: `'b'::from` is a syntax error in
/// PostgreSQL (42601), not a delete of b (wire review 1 item 3). The statement reaches the engine,
/// whose syntax error is reported as 42601 once the SQLSTATE mapping lands (item 7).
#[test]
fn a_branch_call_with_a_keyword_cast_is_a_syntax_error() {
    let dir = Scratch::new("keywordcast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b')").ok("create");
    let r = a.q("SELECT turso_branch_delete('b'::from)");
    assert_eq!(r.err("delete 'b'::from").code, "42601");
    assert_eq!(
        a.q("SELECT turso_branch_switch('b')").single("b survives"),
        "b"
    );
}

/// A bound branch name is text: a parameter declared as another type is refused (42804) and names
/// nothing. A text or varchar parameter, or one of unspecified type, is read as text in either
/// format: PostgreSQL's textrecv takes a binary text value as its bytes (wire review 1 item 3).
#[test]
fn a_branch_name_parameter_must_be_text() {
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    const VARCHAR: u32 = 1043;
    let dir = Scratch::new("paramtype");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let create = "SELECT turso_branch_create($1)";
    let r = a.xt(create, &[(INT4, 0, b"5")]);
    assert_eq!(r.err("create, $1 declared int4").code, "42804");
    let r = a.xt(create, &[(INT4, 1, &5i32.to_be_bytes())]);
    assert_eq!(r.err("create, $1 declared int4, binary").code, "42804");
    let r = a.xt("SELECT turso_branch_create($1::text)", &[(INT4, 0, b"6")]);
    assert_eq!(r.err("create, $1 declared int4 and cast").code, "42804");
    for name in ["5", "6"] {
        let r = a.q(&format!("SELECT turso_branch_switch('{name}')"));
        assert!(r.error.is_some(), "an int4 parameter created branch {name}");
    }
    a.xt(create, &[(TEXT, 1, b"bin")])
        .ok("create, text, binary");
    a.xt(create, &[(VARCHAR, 0, b"vc")]).ok("create, varchar");
    a.xt(create, &[(0, 0, b"untyped")])
        .ok("create, unspecified");
    for name in ["bin", "vc", "untyped"] {
        assert_eq!(
            a.q(&format!("SELECT turso_branch_switch('{name}')"))
                .single("switch"),
            name
        );
    }
}

/// The size of the trunk's WAL file beside `db` (0 when there is none).
fn wal_bytes(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_owned();
    wal.push("-wal");
    std::fs::metadata(PathBuf::from(wal)).map_or(0, |m| m.len())
}

/// CHECKPOINT does what it says or fails, judged by the trunk WAL rather than the tag: while
/// another session holds a write transaction it waits for it, within the lock timeout, and then
/// leaves the WAL empty. At 472023b72 the engine's busy answer was a row nobody read, and the tag
/// was sent over a checkpoint that never ran (wire review 1 item 4 (a)).
#[test]
fn checkpoint_waits_for_a_writer_and_empties_the_wal() {
    let dir = Scratch::new("ckptwait");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "20000"]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    b.q("BEGIN").ok("begin");
    b.q("INSERT INTO t VALUES (2, 'b')").ok("insert");
    assert!(
        wal_bytes(&dir.db()) > 0,
        "premise: the trunk WAL holds frames"
    );
    let committer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        b.q("COMMIT").ok("commit");
        b
    });
    let t0 = Instant::now();
    let r = a.q("CHECKPOINT");
    let waited = t0.elapsed();
    let _b = committer.join().unwrap();
    let r = r.ok("checkpoint");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert!(
        waited >= Duration::from_millis(250),
        "the checkpoint did not wait for the writer ({waited:?})"
    );
    assert_eq!(
        wal_bytes(&dir.db()),
        0,
        "CHECKPOINT left frames in the trunk WAL"
    );
}

/// A writer that outlasts the lock timeout fails the CHECKPOINT with 55P03, PostgreSQL's
/// lock_not_available, instead of a CHECKPOINT tag over nothing (wire review 1 item 4 (a)).
#[test]
fn checkpoint_still_busy_at_the_lock_timeout_fails_with_55p03() {
    let dir = Scratch::new("ckptbusy");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "300"]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    b.q("BEGIN").ok("begin");
    b.q("INSERT INTO t VALUES (2, 'b')").ok("insert");
    assert_eq!(a.q("CHECKPOINT").err("checkpoint").code, "55P03");
    b.q("COMMIT").ok("commit");
    a.q("CHECKPOINT").ok("checkpoint once the writer is gone");
    assert_eq!(
        wal_bytes(&dir.db()),
        0,
        "the trunk WAL after the checkpoint"
    );
}

/// CHECKPOINT is the trunk's, as PostgreSQL's is the cluster's: from a session on a branch it
/// empties the trunk WAL, and the session stays on its branch (wire review 1 item 4 (b)).
#[test]
fn checkpoint_from_a_branch_session_empties_the_trunk_wal() {
    let dir = Scratch::new("ckptbranch");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "5000"]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('c')").ok("create");
    a.q("SELECT turso_branch_switch('c')").ok("switch");
    a.q("UPDATE t SET v = 'c' WHERE id = 1").ok("write on c");
    assert!(
        wal_bytes(&dir.db()) > 0,
        "premise: the trunk WAL holds frames"
    );
    let r = a.q("CHECKPOINT").ok("checkpoint from a branch session");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert_eq!(
        wal_bytes(&dir.db()),
        0,
        "CHECKPOINT on a branch left the trunk WAL"
    );
    assert_eq!(a.q("SELECT turso_branch_current()").single("current"), "c");
    assert_eq!(a.q("SELECT v FROM t WHERE id = 1").single("on c"), "c");
}

/// CHECKPOINT inside a block runs and leaves the block as it was, as in PostgreSQL 18.6 (the
/// reviewer's measurement); at 472023b72 the engine refused it there (TableLocked), which
/// aborted the block (wire review 1 item 4 (c)).
#[test]
fn checkpoint_inside_a_block_keeps_the_block() {
    let dir = Scratch::new("ckptblock");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let r = a
        .q("BEGIN; INSERT INTO t VALUES (2, 'two'); CHECKPOINT; COMMIT")
        .ok("block with a checkpoint");
    assert_eq!(
        r.tags,
        vec![
            "BEGIN".to_string(),
            "INSERT 0 1".to_string(),
            "CHECKPOINT".to_string(),
            "COMMIT".to_string()
        ]
    );
    assert_eq!(r.status, b'I');
    assert_eq!(a.q("SELECT count(*) FROM t").single("rows"), "2");
}

/// generate_series in FROM may read the row it is joined to, as PostgreSQL's implicitly LATERAL
/// function call does: `FROM s, generate_series(1, s.x) AS g` gives one row per x and g <= x
/// (PostgreSQL 18.6 returns them, the reviewer measured); at 472023b72 the wrapped form could not
/// see s. WITH ORDINALITY and a second column alias are refused, not dropped (wire review 1
/// item 5).
#[test]
fn generate_series_may_read_the_row_it_joins() {
    let dir = Scratch::new("genlateral");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE s(x INT)").ok("create");
    a.q("INSERT INTO s VALUES (2), (3)").ok("insert");
    let row = |x: &str, g: &str| vec![Some(x.to_string()), Some(g.to_string())];
    let r = a
        .q("SELECT s.x, g FROM s, generate_series(1, s.x) AS g ORDER BY 1, 2")
        .ok("correlated, bare alias");
    assert_eq!(
        r.rows,
        vec![
            row("2", "1"),
            row("2", "2"),
            row("3", "1"),
            row("3", "2"),
            row("3", "3")
        ]
    );
    let r = a
        .q("SELECT s.x, g.y FROM s, generate_series(1, s.x) AS g(y) WHERE g.y = s.x ORDER BY 1")
        .ok("correlated, column alias");
    assert_eq!(r.rows, vec![row("2", "2"), row("3", "3")]);
    assert!(
        a.q("SELECT * FROM generate_series(1, 2) WITH ORDINALITY")
            .error
            .is_some(),
        "WITH ORDINALITY was dropped"
    );
    assert!(
        a.q("SELECT * FROM generate_series(1, 2) AS g(a, b)")
            .error
            .is_some(),
        "a second column alias was dropped"
    );
}

/// The server's count of CHECKPOINTs skipped inside a block (turso_branch_stats' fifth column).
fn checkpoints_skipped(a: &mut Wire) -> i64 {
    let r = a.q("SELECT turso_branch_stats()").ok("stats");
    r.rows[0][4]
        .as_deref()
        .expect("the skip count is never NULL")
        .parse()
        .unwrap()
}

/// Inside a block CHECKPOINT does not wait (the block may hold the lock it needs), so when the
/// WAL is busy it is skipped, and says so: a NOTICE before the tag, and one more in
/// turso_branch_stats' skip count. A tag alone would recreate the lie wire review 1 item 4
/// flagged (lead ruling, 2026-10-06T13:47Z). One it can run sends no notice and counts nothing.
/// Busy here: once a TRUNCATE checkpoint has emptied the WAL, a reader holds read mark 0 (the
/// engine's SQLite rule, core/storage/wal.rs), which every checkpoint mode needs exclusively.
#[test]
fn a_busy_checkpoint_inside_a_block_says_it_was_skipped() {
    let dir = Scratch::new("ckptskip");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    // Runnable: the WAL holds frames, no other reader.
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert");
    let before = checkpoints_skipped(&mut a);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (3, 'three')")
        .ok("insert in block");
    let r = a.q("CHECKPOINT").ok("checkpoint in a block");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert!(
        r.notices.is_empty(),
        "a checkpoint that ran sent {:?}",
        r.notices
    );
    a.q("COMMIT").ok("commit");
    assert_eq!(
        checkpoints_skipped(&mut a),
        before,
        "a checkpoint that ran was counted"
    );
    // Busy: the WAL emptied, then b reads from read mark 0.
    a.q("CHECKPOINT").ok("checkpoint outside a block");
    assert_eq!(wal_bytes(&dir.db()), 0, "premise: the WAL is empty");
    b.q("BEGIN").ok("b begin");
    b.q("SELECT count(*) FROM t").ok("b reads");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (4, 'four')")
        .ok("insert in block");
    let r = a.q("CHECKPOINT").ok("busy checkpoint in a block");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert_eq!(r.notices.len(), 1, "notices {:?}", r.notices);
    assert!(
        r.notices[0].message.contains("checkpoint skipped"),
        "notice {:?}",
        r.notices[0]
    );
    assert_eq!(r.status, b'T', "the block goes on");
    a.q("COMMIT").ok("commit");
    b.q("COMMIT").ok("b commit");
    assert_eq!(
        checkpoints_skipped(&mut a),
        before + 1,
        "the skip was not counted"
    );
    assert_eq!(a.q("SELECT count(*) FROM t").single("rows"), "4");
}

/// Engine errors carry PostgreSQL's SQLSTATE, so a driver raises the right exception class
/// (psycopg's IntegrityError, not InternalError): at 472023b72 every one but Busy and
/// BusySnapshot was XX000 (wire review 1 item 7). The codes are PostgreSQL's (errcodes.txt).
#[test]
fn engine_errors_carry_postgres_sqlstates() {
    let dir = Scratch::new("sqlstate");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("parent");
    a.q("CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        .ok("child");
    a.q("CREATE TABLE n(id INT PRIMARY KEY, v INT NOT NULL)")
        .ok("not null");
    a.q("CREATE TABLE k(id INT PRIMARY KEY, v INT CHECK (v > 0))")
        .ok("check");
    for (sql, code) in [
        ("INSERT INTO t VALUES (1, 'again')", "23505"),
        ("INSERT INTO c VALUES (1, 99)", "23503"),
        ("INSERT INTO n VALUES (1, NULL)", "23502"),
        ("INSERT INTO k VALUES (1, 0)", "23514"),
        ("SELECT * FROM nope", "42P01"),
        ("SELECT nope FROM t", "42703"),
        ("SELECT FROM WHERE", "42601"),
        (
            "SELECT * FROM generate_series(1, 2) WITH ORDINALITY",
            "0A000",
        ),
    ] {
        assert_eq!(a.q(sql).err(sql).code, code, "{sql}");
    }
    assert_eq!(
        a.q("SELECT count(*) FROM t").single("t after"),
        "1",
        "a refused insert left a row"
    );
}

/// Whether a session can switch to branch `name`, i.e. whether it exists (and nobody holds it).
fn branch_exists(a: &mut Wire, name: &str) -> bool {
    let exists = a
        .q(&format!("SELECT turso_branch_switch('{name}')"))
        .error
        .is_none();
    if exists {
        a.q("SELECT turso_branch_switch('main')").ok("back to main");
    }
    exists
}

/// A multi-statement simple query runs as one implicit transaction, as in PostgreSQL: the first
/// error rolls back every statement of the string before it, an explicit COMMIT inside it commits
/// what came before and starts a new one, and a branch call inside it is refused with 25001 (as
/// PostgreSQL refuses CREATE DATABASE there). At 472023b72 each statement autocommitted (wire
/// review 1 item 8).
#[test]
fn a_multi_statement_query_is_one_implicit_transaction() {
    let dir = Scratch::new("implicit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let ids = |a: &mut Wire| -> Vec<String> {
        a.q("SELECT id FROM t ORDER BY id")
            .ok("ids")
            .rows
            .into_iter()
            .map(|r| r[0].clone().unwrap())
            .collect()
    };
    let r = a.q("INSERT INTO t VALUES (2, 'two'); INSERT INTO t VALUES (1, 'dup')");
    assert_eq!(r.err("insert, then a duplicate").code, "23505");
    assert_eq!(
        r.status, b'I',
        "an implicit block's failure leaves the session idle"
    );
    assert_eq!(
        ids(&mut a),
        vec!["1"],
        "the first insert outlived the failed string"
    );
    let r = a.q("INSERT INTO t VALUES (3, 'three'); SELECT turso_branch_create('b')");
    assert_eq!(r.err("insert, then a branch call").code, "25001");
    assert_eq!(
        ids(&mut a),
        vec!["1"],
        "the insert outlived the refused branch call"
    );
    assert!(
        !branch_exists(&mut a, "b"),
        "the branch call ran in a multi-statement string"
    );
    let r = a.q(
        "INSERT INTO t VALUES (7, 'seven'); COMMIT; INSERT INTO t VALUES (8, 'eight'); \
         INSERT INTO t VALUES (1, 'dup')",
    );
    assert_eq!(r.err("commit inside the string").code, "23505");
    assert_eq!(
        ids(&mut a),
        vec!["1", "7"],
        "the explicit COMMIT keeps 7, the failure drops 8"
    );
    let r = a
        .q("INSERT INTO t VALUES (9, 'nine'); INSERT INTO t VALUES (10, 'ten')")
        .ok("two inserts");
    assert_eq!(r.status, b'I');
    assert_eq!(ids(&mut a), vec!["1", "7", "9", "10"]);
}

/// Extended-protocol Executes up to a Sync run as one implicit transaction, as in PostgreSQL: a
/// failure rolls back the pipeline's earlier statements, success commits them all at the Sync. A
/// branch call may open a pipeline (it commits at once, as CREATE DATABASE does) but is refused
/// with 25001 inside one (wire review 1 item 8).
#[test]
fn a_pipeline_up_to_sync_is_one_implicit_transaction() {
    let dir = Scratch::new("pipeline");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let count = |a: &mut Wire, id: i64| {
        a.q(&format!("SELECT count(*) FROM t WHERE id = {id}"))
            .single("count")
    };
    let r = a.pipeline(&[
        "INSERT INTO t VALUES (4, 'four')",
        "INSERT INTO t VALUES (1, 'dup')",
    ]);
    assert_eq!(r.err("pipeline with a duplicate").code, "23505");
    assert_eq!(r.status, b'I');
    assert_eq!(
        count(&mut a, 4),
        "0",
        "the pipeline's first insert outlived its failure"
    );
    let r = a
        .pipeline(&[
            "INSERT INTO t VALUES (5, 'five')",
            "INSERT INTO t VALUES (6, 'six')",
        ])
        .ok("pipeline of two inserts");
    assert_eq!(r.status, b'I', "the pipeline committed at its Sync");
    assert_eq!(count(&mut a, 5), "1");
    assert_eq!(count(&mut a, 6), "1");
    let r = a
        .pipeline(&[
            "SELECT turso_branch_create('p1')",
            "INSERT INTO t VALUES (11, 'eleven')",
        ])
        .ok("a branch call opening a pipeline");
    assert_eq!(r.status, b'I');
    assert!(branch_exists(&mut a, "p1"));
    assert_eq!(count(&mut a, 11), "1");
    let r = a.pipeline(&[
        "INSERT INTO t VALUES (12, 'twelve')",
        "SELECT turso_branch_create('p2')",
    ]);
    assert_eq!(r.err("a branch call inside a pipeline").code, "25001");
    assert_eq!(
        count(&mut a, 12),
        "0",
        "the insert outlived the refused branch call"
    );
    assert!(!branch_exists(&mut a, "p2"));
}

/// A CHECKPOINT over the extended protocol, in no block, is the full one: it empties the trunk
/// WAL. The pipeline's implicit block (item 8) must not turn it into the in-block passive attempt
/// (item 4), which never truncates.
#[test]
fn an_extended_checkpoint_outside_a_block_empties_the_wal() {
    let dir = Scratch::new("ckptext");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    assert!(
        wal_bytes(&dir.db()) > 0,
        "premise: the trunk WAL holds frames"
    );
    let r = a.x("CHECKPOINT", &[]).ok("extended checkpoint");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert!(r.notices.is_empty(), "notices {:?}", r.notices);
    assert_eq!(
        wal_bytes(&dir.db()),
        0,
        "the extended CHECKPOINT left the WAL"
    );
}

/// COPY FROM over the extended protocol loads its rows once. Describe answered by preparing the
/// statement, and the frontend runs COPY's load at prepare, so Describe and Execute each loaded
/// the file (wire review 1 item 12).
#[test]
fn an_extended_copy_from_loads_its_rows_once() {
    let dir = Scratch::new("copyext");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let file = dir.0.join("rows.tsv");
    std::fs::write(&file, "2\ttwo\n3\tthree\n").unwrap();
    a.x(&format!("COPY t FROM '{}'", file.display()), &[])
        .ok("extended COPY FROM");
    assert_eq!(
        a.q("SELECT count(*) FROM t").single("rows"),
        "3",
        "the seed row and the file's two, each once"
    );
}

/// An extended-protocol round costs the server no system call more than the same statement over
/// the simple protocol: its replies are written once, at Sync. pgwire's default handlers flushed
/// after ParseComplete, BindComplete, the Describe reply, CommandComplete and ReadyForQuery, five
/// writes against one (wire review 1 item 12). The statement is a branch call that touches no
/// storage; the five messages arrive in one write; the least of 7 rounds per protocol is compared,
/// counted by the server itself (turso_branch_stats' unix_syscalls).
#[test]
fn an_extended_round_costs_no_system_call_more_than_a_simple_one() {
    let dir = Scratch::new("extsyscalls");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let sys = |a: &mut Wire| -> i64 {
        a.q("SELECT turso_branch_stats()").ok("stats").rows[0][0]
            .as_deref()
            .expect("an Apple build counts system calls")
            .parse()
            .unwrap()
    };
    let sql = "SELECT turso_branch_current()";
    let (mut simple, mut extended) = (Vec::new(), Vec::new());
    for _ in 0..7 {
        let before = sys(&mut a);
        a.q(sql).ok("simple");
        simple.push(sys(&mut a) - before);
        let before = sys(&mut a);
        a.x_one_write(sql).ok("extended");
        extended.push(sys(&mut a) - before);
    }
    let (s, e) = (
        *simple.iter().min().unwrap(),
        *extended.iter().min().unwrap(),
    );
    assert!(
        e <= s,
        "extended {e} system calls against simple {s} (all rounds: {extended:?} vs {simple:?})"
    );
}

/// Describe of a statement whose parameters the client did not declare reports them all, $1 up to
/// the highest $n used, as text, as PostgreSQL reports every parameter it infers. At 472023b72 it
/// reported only the declared ones, so asyncpg, tokio-postgres and pgx (which declare none) saw no
/// parameter and refused to bind (wire review 1 item 13).
#[test]
fn describe_statement_reports_undeclared_parameters() {
    let dir = Scratch::new("describeparams");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    const TEXT: u32 = 25;
    for (sql, want) in [
        ("SELECT turso_branch_create($1)", vec![TEXT]),
        ("SELECT v FROM t WHERE id = $1", vec![TEXT]),
        ("SELECT v FROM t WHERE id = $2", vec![TEXT, TEXT]),
        ("SELECT v FROM t", vec![]),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(want), "{sql}");
    }
}
