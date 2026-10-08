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
            // Observation only: with WIRE_SERVER_TRACE_DIR set, each server writes its log there
            // (`-t`), so a session the server ends without a word says why (lead: find the cause
            // of "closed the connection during startup").
            if let Some(trace_dir) = std::env::var_os("WIRE_SERVER_TRACE_DIR") {
                cmd.arg("-t")
                    .arg(std::path::Path::new(&trace_dir).join(format!("server-{port}.log")));
            }
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

    fn err(&self, sql: &str) -> WireError {
        self.error
            .clone()
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

    /// [`Wire::x`] with no parameters and every result column in BINARY format, as tokio-postgres
    /// asks for them. A binary value comes back in `rows` as its bytes, lossily as UTF-8.
    fn x_binary(&mut self, sql: &str) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        self.send(b'P', &parse);
        // unnamed portal and statement, no parameter formats, no parameters, one result format: 1
        self.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 1, 0, 1]);
        self.send(b'D', b"P\0");
        self.send(b'E', &[0, 0, 0, 0, 0]);
        self.send(b'S', &[]);
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
    // A lock wait well inside the client's 30 s read timeout: at the default 60 s the blocked
    // INSERT's reply came after the client gave up on it (wire review 1 item 9 added the wait).
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "2000"]);
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
/// PostgreSQL, not a delete of b (wire review 1 item 3). Its SQLSTATE is the next test's, so a
/// SQLSTATE regression (item 7) is not reported under this one (wire review 3 item 9).
#[test]
fn a_branch_call_with_a_keyword_cast_is_a_syntax_error() {
    let dir = Scratch::new("keywordcast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b')").ok("create");
    let r = a.q("SELECT turso_branch_delete('b'::from)");
    assert!(
        r.error.is_some(),
        "delete 'b'::from succeeded: {:?}",
        r.rows
    );
    assert_eq!(
        a.q("SELECT turso_branch_switch('b')").single("b survives"),
        "b"
    );
}

/// The syntax error of a branch call with a keyword cast is 42601, as PostgreSQL reports it: the
/// statement reaches the engine through libpg_query, whose errors are 42601 (wire review 1 items 3
/// and 7).
#[test]
fn a_keyword_cast_in_a_branch_call_reports_42601() {
    let dir = Scratch::new("keywordcast42601");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b')").ok("create");
    let r = a.q("SELECT turso_branch_delete('b'::from)");
    assert_eq!(r.err("delete 'b'::from").code, "42601");
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

/// Aggregates have PostgreSQL's result types, the same over both protocols and whatever the rows
/// hold: count bigint, sum(int4) bigint, min/max(int4) integer, avg(int4) numeric (PostgreSQL's
/// aggregate signatures, docs "Aggregate Functions"; the re-recording from PG18 with record_pg.sh is
/// owed, item 17). At 472023b72 the simple protocol typed them from their values (max over an empty
/// table was text) and the extended one called them all text (wire review 1 item 14).
#[test]
fn aggregates_have_postgres_types_on_both_protocols() {
    let dir = Scratch::new("aggstatic");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, 'x')").ok("insert");
    a.q("CREATE TABLE e(id INT PRIMARY KEY)").ok("empty table");
    let sql = "SELECT count(*), sum(id), min(id), max(id), avg(id) FROM t";
    let want = Some(vec![20, 20, 23, 23, 1700]);
    assert_eq!(a.q(sql).ok("simple").oids, want, "simple protocol");
    assert_eq!(a.x(sql, &[]).ok("extended").oids, want, "extended protocol");
    for r in [
        a.q("SELECT max(id) FROM e"),
        a.x("SELECT max(id) FROM e", &[]),
    ] {
        assert_eq!(r.ok("max over no rows").oids, Some(vec![23]));
    }
}

/// A binary-format result value is encoded as the type its column is described with: bigint as 8
/// bytes, integer as 4, a column described as text as the text's bytes. At 472023b72 a value was
/// encoded by its engine storage class whatever the described type, so an integer in a column
/// described as text went out as 8 binary bytes (wire review 1 item 14). numeric has no binary
/// encoder here, so binary numeric is refused with 0A000 rather than sent as a float's bytes.
#[test]
fn binary_results_are_encoded_as_their_described_type() {
    let dir = Scratch::new("binaryresults");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let bytes = |b: &[u8]| Some(String::from_utf8_lossy(b).into_owned());
    let r = a
        .x_binary("SELECT count(*), min(id) FROM t")
        .ok("binary aggregates");
    assert_eq!(r.oids, Some(vec![20, 23]));
    assert_eq!(
        r.rows,
        vec![vec![bytes(&1i64.to_be_bytes()), bytes(&1i32.to_be_bytes())]]
    );
    let r = a
        .x_binary("SELECT abs(id) FROM t WHERE id = 1")
        .ok("binary function");
    let oid = r.oids.clone().expect("a RowDescription")[0];
    let want = match oid {
        25 | 1043 => bytes(b"1"),
        20 => bytes(&1i64.to_be_bytes()),
        23 => bytes(&1i32.to_be_bytes()),
        other => panic!("abs(int) described as type {other}"),
    };
    assert_eq!(r.rows, vec![vec![want]], "described as {oid}");
    let r = a.x_binary("SELECT avg(id) FROM t");
    assert_eq!(r.err("binary numeric").code, "0A000");
}

/// Two sessions running pgbench-shaped transactions on the same row both commit: the second one's
/// write waits for the first's commit, within the lock timeout, as PostgreSQL's row lock waits. At
/// 472023b72 ordinary statements had no busy timeout, so the second write failed at once with
/// 55P03 and pgbench at -c 2 aborted its clients (wire review 1 item 9).
#[test]
fn a_write_waits_for_another_sessions_commit() {
    let dir = Scratch::new("busywrite");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "20000"]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("UPDATE t SET v = '0' WHERE id = 1").ok("reset");
    a.q("BEGIN").ok("a begin");
    a.q("UPDATE t SET v = v || 'a' WHERE id = 1").ok("a write");
    let committer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        a.q("COMMIT").ok("a commit");
        a
    });
    b.q("BEGIN").ok("b begin");
    let t0 = Instant::now();
    let r = b.q("UPDATE t SET v = v || 'b' WHERE id = 1");
    let waited = t0.elapsed();
    let mut a = committer.join().unwrap();
    r.ok("b write, after a's commit");
    assert!(
        waited >= Duration::from_millis(250),
        "b's write did not wait ({waited:?})"
    );
    b.q("COMMIT").ok("b commit");
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 1").single("both writes"),
        "0ab"
    );
}

/// Branch creates racing trunk DDL all succeed: the engine answers a fork that met a schema change
/// with SchemaUpdated, documented as "the caller retries" (fork_trunk_registered), and the server
/// now does. At 472023b72 waiting() retried only Busy, so such a create failed with XX000, against
/// PREREG's trunk writers committing DDL during the gate (wire review 1 item 11).
#[test]
fn branch_creates_racing_trunk_ddl_all_succeed() {
    let dir = Scratch::new("ddlrace");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "20000"]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    let ddl = std::thread::spawn(move || {
        for i in 0..40 {
            a.q(&format!("ALTER TABLE t ADD COLUMN c{i} INT"))
                .ok("trunk DDL");
        }
        a
    });
    let mut failures = Vec::new();
    for i in 0..40 {
        let name = format!("r{i}");
        if let Some(e) = b.q(&format!("SELECT turso_branch_create('{name}')")).error {
            failures.push((name, e));
            continue;
        }
        b.q(&format!("SELECT turso_branch_delete('{name}')"))
            .ok("delete");
    }
    let _a = ddl.join().unwrap();
    assert!(
        failures.is_empty(),
        "{} of 40 creates failed beside trunk DDL: {failures:?}",
        failures.len()
    );
}

/// The claim is single-schema (PREREG §1): in server mode CREATE SCHEMA and DROP SCHEMA of any
/// schema but public are refused with 0A000, so no session's attached schemas can differ from
/// another's. At 472023b72 a schema created in one session was left unbranched by sessions opened
/// before it (the E2 scope rule), DROP SCHEMA unlinked a file other sessions had attached, and once
/// a schema existed every create in a session that had it attached failed XX000 (wire review 1
/// item 16).
#[test]
fn schema_ddl_is_refused_in_server_mode() {
    let dir = Scratch::new("schemaddl");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    for sql in [
        "CREATE SCHEMA s",
        "CREATE SCHEMA IF NOT EXISTS s",
        "DROP SCHEMA IF EXISTS s",
        "drop schema s cascade",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    }
    assert!(
        !dir.0.join("turso-postgres-schema-s.db").exists(),
        "a refused CREATE SCHEMA left its file"
    );
    b.q("SELECT turso_branch_create('afterschema')")
        .ok("a create in another session");
    a.q("SELECT turso_branch_create('afterschema2')")
        .ok("a create in the session that asked");
}

/// ALTER TABLE ADD PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK works in every transaction state a
/// client can be in (autocommit; right after BEGIN; after BEGIN and a read; after BEGIN and a
/// write): it commits with the block, keeps every row, takes effect, and leaves no aside table.
/// At 15e96b3a9 the rebuild ran with the engine's nested-statement flag up (its catalog lookup
/// statement stayed alive), so it opened no transaction and the DDL reached SetCookie's
/// unreachable! in every state but the last (wire review 2 item 1).
#[test]
fn alter_table_add_constraint_works_in_every_transaction_state() {
    let dir = Scratch::new("addstates");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE par(id INT PRIMARY KEY)").ok("parent");
    a.q("INSERT INTO par VALUES (1), (2)").ok("parents");
    let kinds = [
        (
            "PRIMARY KEY (id)",
            "INSERT INTO {t} VALUES (1, 99, 1)",
            "23505",
        ),
        ("UNIQUE (v)", "INSERT INTO {t} VALUES (9, 10, 1)", "23505"),
        (
            "FOREIGN KEY (p) REFERENCES par (id)",
            "INSERT INTO {t} VALUES (9, 90, 99)",
            "23503",
        ),
        (
            "CHECK (id > 0)",
            "INSERT INTO {t} VALUES (0, 0, 1)",
            "23514",
        ),
    ];
    let states = ["autocommit", "begin", "begin+read", "begin+write"];
    for (k, (constraint, violation, code)) in kinds.iter().enumerate() {
        for (s, state) in states.iter().enumerate() {
            let t = format!("c{k}{s}");
            a.q(&format!("CREATE TABLE {t}(id INT, v INT, p INT)"))
                .ok("table");
            a.q(&format!("INSERT INTO {t} VALUES (1, 10, 1), (2, 20, 2)"))
                .ok("rows");
            let alter = format!("ALTER TABLE {t} ADD {constraint}");
            let mut rows = 2;
            match *state {
                "autocommit" => {}
                "begin" => {
                    a.q("BEGIN").ok("begin");
                }
                "begin+read" => {
                    a.q("BEGIN").ok("begin");
                    a.q(&format!("SELECT count(*) FROM {t}")).ok("read");
                }
                _ => {
                    a.q("BEGIN").ok("begin");
                    a.q(&format!("INSERT INTO {t} VALUES (3, 30, 1)"))
                        .ok("write");
                    rows = 3;
                }
            }
            let r = a.q(&alter).ok(&alter);
            assert_eq!(r.tags, vec!["ALTER TABLE".to_string()], "{alter} {state}");
            let r = if *state == "autocommit" {
                r
            } else {
                let r = a.q("COMMIT").ok("commit");
                assert_eq!(r.tags, vec!["COMMIT".to_string()], "{alter} {state}");
                r
            };
            assert_eq!(r.status, b'I', "{alter} {state}");
            assert_eq!(
                a.q(&format!("SELECT count(*) FROM {t}")).single("rows"),
                rows.to_string(),
                "{alter} {state}"
            );
            let sql = violation.replace("{t}", &t);
            assert_eq!(a.q(&sql).err(&sql).code, *code, "{alter} {state}");
        }
    }
    // A unique constraint the rows break is refused and changes nothing.
    a.q("CREATE TABLE d(id INT, v INT)").ok("table");
    a.q("INSERT INTO d VALUES (1, 5), (2, 5)").ok("duplicates");
    assert_eq!(
        a.q("ALTER TABLE d ADD UNIQUE (v)")
            .err("unique over duplicates")
            .code,
        "23505"
    );
    assert_eq!(a.q("SELECT count(*) FROM d").single("rows"), "2");
    a.q("INSERT INTO d VALUES (3, 5)")
        .ok("no unique constraint was left behind");
    assert_eq!(
        a.q("SELECT count(*) FROM pg_class WHERE relname LIKE '%turso_rebuild%'")
            .single("aside tables"),
        "0",
        "an aside table was left behind"
    );
}

/// ALTER TABLE ADD PRIMARY KEY / UNIQUE over the extended protocol (Parse, Bind, Describe,
/// Execute, Sync, as tokio-postgres, pgjdbc, sqlx and PQexecParams send it) adds its constraint
/// once, in autocommit and inside a block, with Describe of the portal or of the statement. The
/// frontend performs ALTER while preparing it, and Describe prepared: the constraint was added at
/// Describe and again at Execute ("more than one primary key"), and inside BEGIN the error aborted
/// the block, so COMMIT answered ROLLBACK and lost the block's writes (wire review 3 item 2).
#[test]
fn an_extended_alter_add_constraint_adds_it_once() {
    let dir = Scratch::new("extalter");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let constraints = |a: &mut Wire, t: &str, kind: &str| {
        a.q(&format!(
            "SELECT count(*) FROM information_schema.table_constraints \
             WHERE table_name = '{t}' AND constraint_type = '{kind}'"
        ))
        .single("constraints")
    };
    for t in ["e1", "e2", "e3", "e4"] {
        a.q(&format!("CREATE TABLE {t}(id INT, v INT)")).ok("table");
        a.q(&format!("INSERT INTO {t} VALUES (1, 10), (2, 20)"))
            .ok("rows");
    }
    // Autocommit, Describe of the portal.
    let r = a
        .x("ALTER TABLE e1 ADD PRIMARY KEY (id)", &[])
        .ok("extended ADD PRIMARY KEY");
    assert_eq!(r.tags, vec!["ALTER TABLE".to_string()]);
    assert_eq!(constraints(&mut a, "e1", "PRIMARY KEY"), "1");
    assert_eq!(a.q("INSERT INTO e1 VALUES (1, 0)").err("dup").code, "23505");
    // Inside a block that has written.
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO e2 VALUES (3, 30)")
        .ok("write in the block");
    let r = a
        .x("ALTER TABLE e2 ADD PRIMARY KEY (id)", &[])
        .ok("extended ADD PRIMARY KEY in a block");
    assert_eq!(r.status, b'T', "the block was aborted");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(a.q("SELECT count(*) FROM e2").single("rows"), "3");
    assert_eq!(constraints(&mut a, "e2", "PRIMARY KEY"), "1");
    // ADD UNIQUE: one constraint, the rows once.
    a.x("ALTER TABLE e3 ADD UNIQUE (v)", &[])
        .ok("extended ADD UNIQUE");
    assert_eq!(constraints(&mut a, "e3", "UNIQUE"), "1");
    assert_eq!(a.q("SELECT count(*) FROM e3").single("rows"), "2");
    // Describe of the statement before Bind (Parse, Describe S, Bind, Execute, Sync).
    let mut parse = vec![0u8];
    parse.extend_from_slice(b"ALTER TABLE e4 ADD PRIMARY KEY (id)");
    parse.extend_from_slice(&[0, 0, 0]);
    a.send(b'P', &parse);
    a.send(b'D', b"S\0");
    a.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    a.read_reply().ok("Describe S then Execute");
    assert_eq!(constraints(&mut a, "e4", "PRIMARY KEY"), "1");
}

/// Describe never performs a statement, however it is written: one behind a comment is still
/// answered from its parse (0ddc38bc5 classified the special statements by their first word, so a
/// leading comment sent this ALTER through Describe's prepare, which performs it).
#[test]
fn describe_never_performs_a_statement_behind_a_comment() {
    let dir = Scratch::new("extaltercomment");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE e5(id INT, v INT)").ok("table");
    a.q("INSERT INTO e5 VALUES (1, 10)").ok("row");
    a.x("/* app */ ALTER TABLE e5 ADD PRIMARY KEY (id)", &[])
        .ok("extended ALTER behind a comment");
    assert_eq!(
        a.q("SELECT count(*) FROM information_schema.table_constraints \
             WHERE table_name = 'e5' AND constraint_type = 'PRIMARY KEY'")
            .single("constraints"),
        "1"
    );
    let file = dir.0.join("rows.tsv");
    std::fs::write(&file, "2\t20\n").unwrap();
    a.x(&format!("-- load\nCOPY e5 FROM '{}'", file.display()), &[])
        .ok("extended COPY behind a comment");
    assert_eq!(a.q("SELECT count(*) FROM e5").single("rows"), "2");
}

/// A Terminate ends the session: the server closes the socket (the client reads EOF promptly,
/// though it keeps its own end open, as bbload's synchronous close does with a dup of the
/// socket), and the branch the session was on is free by then, so a delete right after succeeds
/// with no retry. At 15e96b3a9 pgwire dropped the Terminate and served on until the client's EOF,
/// so that close waited out its 5 s and failed (wire review 3 item 3).
#[test]
fn terminate_ends_the_session_and_frees_its_branch_first() {
    let dir = Scratch::new("terminate");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('y')").ok("create");
    let mut c = server.connect_to("postgres/y").expect("startup on y");
    c.send(b'X', &[]);
    c.s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = [0u8; 1];
    let read = c.s.read(&mut buf);
    assert!(
        matches!(read, Ok(0)),
        "after Terminate the server did not close within 2 s: {read:?}"
    );
    a.q("SELECT turso_branch_delete('y')")
        .ok("a delete right after the close, first try");
}

/// A delete right after a session on the branch closed its socket (no Terminate, no retry)
/// succeeds: the server waits, as PostgreSQL's DROP DATABASE waits up to 5 s for exiting backends,
/// for the closing session to release the branch, instead of refusing 55006 because the release
/// had not happened yet. Twenty rounds, so the race shows. The older retrying test stays as it is
/// (wire review 3 item 4).
#[test]
fn a_delete_right_after_a_close_waits_for_the_release() {
    let dir = Scratch::new("closedelete");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for i in 0..20 {
        let name = format!("z{i}");
        a.q(&format!("SELECT turso_branch_create('{name}')"))
            .ok("create");
        let c = server
            .connect_to(&format!("postgres/{name}"))
            .expect("startup on the branch");
        drop(c);
        a.q(&format!("SELECT turso_branch_delete('{name}')"))
            .ok("a delete right after the close, first try");
    }
}

/// char(n) compares with trailing blanks insignificant, as PostgreSQL's bpchar does, whether the
/// row is reached by a scan or an index seek and whether the operand is a literal or a bound
/// parameter. Values are stored without their padding; at 15e96b3a9 `=` and `<` compared them as
/// plain text, so a padded value a client had read and sent back matched by seek (the seek key is
/// encoded) and not by scan (wire review 2 item 3). Expected values are PostgreSQL's bpchar
/// semantics (docs, "Character Types": trailing spaces are treated as semantically insignificant
/// and disregarded when comparing two values of type character).
#[test]
fn char_n_compares_without_its_padding_by_scan_and_by_seek() {
    let dir = Scratch::new("bpcharcmp");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (t, index) in [("s", false), ("k", true)] {
        a.q(&format!(
            "CREATE TABLE {t}(id INT PRIMARY KEY, code CHAR(5))"
        ))
        .ok("create");
        if index {
            a.q(&format!("CREATE INDEX {t}_code ON {t}(code)"))
                .ok("index");
        }
        a.q(&format!("INSERT INTO {t} VALUES (1, 'ab')"))
            .ok("insert");
        let read_back = a
            .q(&format!("SELECT code FROM {t} WHERE id = 1"))
            .single("the padded value");
        assert_eq!(read_back, "ab   ");
        for (sql, want) in [
            (
                format!("SELECT count(*) FROM {t} WHERE code = 'ab   '"),
                "1",
            ),
            (
                format!("SELECT count(*) FROM {t} WHERE code = '{read_back}'"),
                "1",
            ),
            (format!("SELECT count(*) FROM {t} WHERE code = 'ab'"), "1"),
            (format!("SELECT count(*) FROM {t} WHERE code < 'ab '"), "0"),
            (
                format!("SELECT count(*) FROM {t} WHERE code > 'aa   '"),
                "1",
            ),
            (
                format!("SELECT count(*) FROM {t} WHERE code <> 'ab  '"),
                "0",
            ),
        ] {
            assert_eq!(a.q(&sql).single(&sql), want, "{sql} (index: {index})");
        }
        let sql = format!("SELECT count(*) FROM {t} WHERE code = $1");
        assert_eq!(
            a.x(&sql, &["ab   "]).single("parameter"),
            "1",
            "{sql} with 'ab   ' (index: {index})"
        );
    }
}

/// A COMMIT or ROLLBACK that does not parse is a failed statement like any other: inside a block it
/// aborts the block (status E), and the block then ends with ROLLBACK, its writes gone, as in
/// PostgreSQL. 5b81204b3 classified a failure by the statement's first word, so a mistyped COMMIT
/// rolled the block back and left the session idle (a retried COMMIT then answered COMMIT for
/// discarded work), and a mistyped ROLLBACK left the block open, so a later COMMIT kept what the
/// client meant to discard (wire review 3 item 5).
#[test]
fn a_mistyped_commit_or_rollback_aborts_the_block() {
    let dir = Scratch::new("mistyped");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for (typo, id) in [("COMMIT TRANSACTON", 2), ("ROLLBACK TRANSACTON", 3)] {
        a.q("BEGIN").ok("begin");
        a.q(&format!("INSERT INTO t VALUES ({id}, 'x')"))
            .ok("insert");
        let r = a.q(typo);
        assert_eq!(r.err(typo).code, "42601", "{typo}");
        assert_eq!(r.status, b'E', "{typo} left the block {}", r.status as char);
        let r = a.q("COMMIT").ok("the block's end");
        assert_eq!(r.tags, vec!["ROLLBACK".to_string()], "after {typo}");
        assert_eq!(
            a.q(&format!("SELECT count(*) FROM t WHERE id = {id}"))
                .single("rows"),
            "0",
            "after {typo} the block's insert survived"
        );
    }
}

/// A foreign key declared DEFERRABLE INITIALLY DEFERRED is checked at COMMIT, as in PostgreSQL: an
/// orphan insert inside the block succeeds, the COMMIT fails with 23503, and nothing is kept; the
/// session is idle after it. Column-level and table-level declarations alike. At 15e96b3a9 the
/// translator dropped the deferral, so the insert failed at once and the existing
/// a_failed_commit_leaves_the_session_idle never reached its assertions (wire review 3 item 6).
#[test]
fn a_deferred_foreign_key_is_checked_at_commit() {
    let dir = Scratch::new("deferredfk");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE dp(id INT PRIMARY KEY)").ok("parent");
    a.q("CREATE TABLE dc1(id INT PRIMARY KEY, pid INT REFERENCES dp(id) DEFERRABLE INITIALLY DEFERRED)")
        .ok("column-level deferred");
    a.q("CREATE TABLE dc2(id INT PRIMARY KEY, pid INT, \
         FOREIGN KEY (pid) REFERENCES dp(id) DEFERRABLE INITIALLY DEFERRED)")
        .ok("table-level deferred");
    for t in ["dc1", "dc2"] {
        a.q("BEGIN").ok("begin");
        a.q(&format!("INSERT INTO {t} VALUES (1, 99)"))
            .ok("an orphan insert is deferred");
        let r = a.q("COMMIT");
        assert_eq!(r.err("commit with an orphan").code, "23503", "{t}");
        assert_eq!(
            r.status, b'I',
            "{t}: the failed COMMIT left the session in a block"
        );
        assert_eq!(
            a.q(&format!("SELECT count(*) FROM {t}")).single("rows"),
            "0",
            "{t}: the failed COMMIT kept the orphan"
        );
        // Fixed up before the end, the block commits.
        a.q("BEGIN").ok("begin");
        a.q(&format!("INSERT INTO {t} VALUES (2, 7)"))
            .ok("orphan for now");
        a.q("INSERT INTO dp VALUES (7) ON CONFLICT DO NOTHING")
            .ok("its parent, later in the block");
        a.q("COMMIT").ok("the deferred check passes at COMMIT");
    }
}

/// A session that switched onto a branch holds it as one that started on it does: another
/// session's delete of it is 55006 once the wait for its release runs out, and the switched
/// session's writes still land on it. No test reached the switch path's claim, and removing it
/// survived every test, while the engine deletes a branch a connection is open on (wire review 3
/// item 7).
#[test]
fn deleting_a_branch_another_session_switched_onto_is_refused_with_55006() {
    let dir = Scratch::new("delswitched");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('x')").ok("create");
    let mut b = server.connect();
    b.q("SELECT turso_branch_switch('x')")
        .ok("b switches onto x");
    let e = a.q("SELECT turso_branch_delete('x')").err("delete under b");
    assert_eq!(e.code, "55006", "{e:?}");
    b.q("UPDATE t SET v = 'b' WHERE id = 1")
        .ok("b's branch is still there");
    assert_eq!(
        b.q("SELECT v FROM t WHERE id = 1")
            .single("b reads its write"),
        "b"
    );
    assert_eq!(b.q("SELECT turso_branch_current()").single("current"), "x");
    b.q("SELECT turso_branch_switch('main')").ok("b leaves");
    a.q("SELECT turso_branch_delete('x')")
        .ok("delete once b left");
}

/// Inside a block, an error the engine answers by rolling the whole transaction back (an integer
/// overflow in sum(): a read statement gets no statement savepoint) takes the block's savepoints
/// with it. A later ROLLBACK TO one of them is refused as PostgreSQL refuses a savepoint that does
/// not exist (3B001), and the block stays failed until its end; it was XX000 "no such savepoint".
/// Keeping the work before the savepoint, as PostgreSQL does, is the engine's half (wire review 3
/// item 10); once it lands this test needs another way to make the engine drop a block.
#[test]
fn a_rollback_to_a_savepoint_the_engine_discarded_is_3b001() {
    let dir = Scratch::new("discardedsavepoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE big(v BIGINT)").ok("big");
    a.q("INSERT INTO big VALUES (9223372036854775807), (1)")
        .ok("rows");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    a.q("SAVEPOINT s").ok("savepoint");
    let r = a.q("SELECT sum(v) FROM big");
    assert!(r.error.is_some(), "sum did not overflow: {:?}", r.rows);
    let r = a.q("ROLLBACK TO s");
    assert_eq!(r.err("rollback to s").code, "3B001");
    assert_eq!(r.status, b'E', "the block is no longer failed");
    assert_eq!(a.q("SELECT 1").err("in the failed block").code, "25P02");
    let r = a.q("COMMIT").ok("the block's end");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
        "0"
    );
}

/// ALTER TABLE ADD CONSTRAINT rebuilds a table through an aside table named after it. A table name
/// of 49 to 63 bytes gave an aside name over PostgreSQL's 63-byte identifier limit: libpg_query cut
/// it in the aside's CREATE while the copy and the DROP named it in full (at 63 bytes the cut name
/// was the table's own). A name already taken by an index made the aside's CREATE fail. A serial
/// column made the aside's CREATE create a sequence of its own that the DROP left behind, one per
/// ALTER (wire review 3 item 11).
#[test]
fn alter_add_constraint_rebuilds_long_names_and_serial_tables_cleanly() {
    let dir = Scratch::new("asidename");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for len in [49, 55, 63] {
        let t = format!("l{}", "x".repeat(len - 1));
        assert_eq!(t.len(), len);
        a.q(&format!("CREATE TABLE {t}(id INT, v INT)"))
            .ok("long table");
        a.q(&format!("INSERT INTO {t} VALUES (1, 10), (2, 20)"))
            .ok("rows");
        a.q(&format!("ALTER TABLE {t} ADD PRIMARY KEY (id)"))
            .ok("add pk to a long name");
        assert_eq!(
            a.q(&format!("SELECT count(*) FROM {t}")).single("rows"),
            "2",
            "{len}-byte name"
        );
        let e = a
            .q(&format!("INSERT INTO {t} VALUES (1, 99)"))
            .err("the key is enforced");
        assert_eq!(e.code, "23505", "{len}-byte name");
    }
    a.q("CREATE TABLE other(x INT)").ok("other");
    a.q("CREATE TABLE ix(id INT, v INT)").ok("ix");
    a.q("INSERT INTO ix VALUES (1, 1)").ok("row");
    a.q("CREATE INDEX ix__turso_rebuild ON other(x)")
        .ok("an index with the aside's name");
    a.q("ALTER TABLE ix ADD PRIMARY KEY (id)")
        .ok("the aside's name is taken by an index");
    a.q("CREATE TABLE ser(id SERIAL, v INT)").ok("serial");
    a.q("INSERT INTO ser(v) VALUES (1), (2)").ok("rows");
    let sequences = |a: &mut Wire| {
        a.q("SELECT count(*) FROM pg_sequences")
            .single("pg_sequences")
    };
    let before = sequences(&mut a);
    a.q("ALTER TABLE ser ADD PRIMARY KEY (id)").ok("add pk");
    a.q("ALTER TABLE ser ADD UNIQUE (v)").ok("add unique");
    assert_eq!(sequences(&mut a), before, "the rebuild left a sequence");
    a.q("INSERT INTO ser(v) VALUES (3)")
        .ok("the serial goes on");
    assert_eq!(
        a.q("SELECT id FROM ser WHERE v = 3").single("id"),
        "3",
        "the serial's sequence restarted"
    );
}

/// A primary key column is NOT NULL, as in PostgreSQL and in the engine's STRICT tables (a NULL
/// in a composite key is refused with 23502): information_schema.columns says is_nullable NO and
/// pg_attribute's attnotnull matches a NOT NULL column's for it, declared at column level, at
/// table level, or SERIAL. Both reported a key column declared without NOT NULL as nullable, and ORM introspection
/// made nullable key fields of it (wire review 2 item 9).
#[test]
fn primary_key_columns_are_reported_not_null() {
    let dir = Scratch::new("pknotnull");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE pk1(id INT PRIMARY KEY, v INT, nn INT NOT NULL)")
        .ok("pk1");
    a.q("CREATE TABLE pk2(a INT, b INT, v INT, PRIMARY KEY (a, b))")
        .ok("pk2");
    a.q("CREATE TABLE pk3(id SERIAL PRIMARY KEY, v INT)")
        .ok("pk3");
    // A composite key: an INTEGER PRIMARY KEY is the rowid, and a NULL there takes a new rowid.
    let e = a.q("INSERT INTO pk2 VALUES (NULL, 1, 1)").err("a NULL key");
    assert_eq!(e.code, "23502", "premise: the key is NOT NULL: {e:?}");
    let r = a
        .q(
            "SELECT table_name, column_name, is_nullable FROM information_schema.columns \
            WHERE table_name IN ('pk1', 'pk2', 'pk3') ORDER BY table_name, ordinal_position",
        )
        .ok("columns");
    let rows: Vec<Vec<Option<String>>> = [
        ["pk1", "id", "NO"],
        ["pk1", "v", "YES"],
        ["pk1", "nn", "NO"],
        ["pk2", "a", "NO"],
        ["pk2", "b", "NO"],
        ["pk2", "v", "YES"],
        ["pk3", "id", "NO"],
        ["pk3", "v", "YES"],
    ]
    .iter()
    .map(|r| r.iter().map(|x| Some(x.to_string())).collect())
    .collect();
    assert_eq!(r.rows, rows);
    let r = a
        .q(
            "SELECT c.relname, a.attname, a.attnotnull FROM pg_attribute a \
            JOIN pg_class c ON a.attrelid = c.oid \
            WHERE c.relname IN ('pk1', 'pk2', 'pk3') AND a.attnum > 0 \
            ORDER BY c.relname, a.attnum",
        )
        .ok("pg_attribute");
    let notnull = |table: &str, column: &str| -> Option<String> {
        r.rows
            .iter()
            .find(|row| row[0].as_deref() == Some(table) && row[1].as_deref() == Some(column))
            .unwrap_or_else(|| panic!("no {table}.{column}: {:?}", r.rows))[2]
            .clone()
    };
    let yes = notnull("pk1", "nn");
    let no = notnull("pk1", "v");
    assert_ne!(yes, no, "premise: attnotnull tells NOT NULL apart");
    for (table, column) in [("pk1", "id"), ("pk2", "a"), ("pk2", "b"), ("pk3", "id")] {
        assert_eq!(notnull(table, column), yes, "{table}.{column}");
    }
    assert_eq!(notnull("pk2", "v"), no);
}

/// information_schema's catalog columns name the database current_database() and pg_database
/// name (the file's stem), so `WHERE table_catalog = current_database()` keeps every row; they
/// said "turso" whatever the file (wire review 2 item 10).
#[test]
fn information_schema_catalog_is_the_current_database() {
    let dir = Scratch::new("infocatalog");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("p");
    a.q("CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id))")
        .ok("c");
    assert_eq!(
        a.q("SELECT current_database()").single("current_database"),
        "w",
        "premise: the database is named after its file"
    );
    for (view, catalogs) in [
        ("tables", &["table_catalog"][..]),
        ("columns", &["table_catalog", "udt_catalog"][..]),
        (
            "table_constraints",
            &["constraint_catalog", "table_catalog"][..],
        ),
        (
            "key_column_usage",
            &["constraint_catalog", "table_catalog"][..],
        ),
    ] {
        let all = a
            .q(&format!(
                "SELECT count(*) FROM information_schema.{view} WHERE table_name IN ('p', 'c')"
            ))
            .single("all rows");
        assert_ne!(all, "0", "premise: {view} has rows");
        for column in catalogs {
            let kept = a
                .q(&format!(
                    "SELECT count(*) FROM information_schema.{view} \
                     WHERE table_name IN ('p', 'c') AND {column} = current_database()"
                ))
                .single("kept rows");
            assert_eq!(kept, all, "{view}.{column}");
        }
    }
}

/// An extended-protocol error raised outside a statement's run fails the block it is in, as any
/// other error does: the pipeline's implicit block up to Sync is rolled back, and a client block is
/// failed (status E, its COMMIT answers ROLLBACK). An Execute of a portal that does not exist,
/// and a binary value with no encoder or out of its type's range, never reached the block's
/// bookkeeping: Sync committed the pipeline's write, and a client block stayed 'T' and committed
/// (wire review 4 item 1).
#[test]
fn an_error_outside_a_statements_run_fails_its_block() {
    let dir = Scratch::new("pipelinefail");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // P, B and E of an INSERT, then an Execute of portal "nope", then Sync.
    let pipeline = |a: &mut Wire, id: i32| {
        let mut parse = vec![0u8];
        parse.extend_from_slice(format!("INSERT INTO t VALUES ({id}, 'p')").as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        a.send(b'P', &parse);
        a.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        a.send(b'E', &[0, 0, 0, 0, 0]);
        a.send(b'E', b"nope\0\0\0\0\0");
        a.send(b'S', &[]);
        a.read_reply()
    };
    let count = |a: &mut Wire, id: i32| {
        a.q(&format!("SELECT count(*) FROM t WHERE id = {id}"))
            .single("count")
    };
    let r = pipeline(&mut a, 4);
    assert!(r.error.is_some(), "no error for portal nope: {:?}", r.tags);
    assert_eq!(r.status, b'I');
    assert_eq!(count(&mut a, 4), "0", "Sync committed the pipeline");
    a.q("BEGIN").ok("begin");
    let r = pipeline(&mut a, 5);
    assert!(r.error.is_some(), "no error for portal nope: {:?}", r.tags);
    assert_eq!(r.status, b'E', "the client block is not failed");
    let r = a.q("COMMIT").ok("the block's end");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(count(&mut a, 5), "0", "the failed block committed");
    // A binary result type with no encoder: refused before the statement runs.
    a.q("CREATE TABLE ev(id INT, d DATE)").ok("ev");
    let r = a.x_binary("INSERT INTO ev VALUES (1, '2026-01-01') RETURNING d");
    assert_eq!(r.err("binary date").code, "0A000");
    assert_eq!(
        a.q("SELECT count(*) FROM ev").single("count"),
        "0",
        "the refused INSERT was kept"
    );
    // A value out of its binary type's range: the statement fails, and nothing of it is kept.
    a.q("CREATE TABLE big(id INT, n INT)").ok("big");
    a.q("INSERT INTO big VALUES (1, 5000)").ok("row");
    let r = a.x_binary("UPDATE big SET n = n * 1000000 WHERE id = 1 RETURNING n");
    assert_eq!(r.err("binary int4 out of range").code, "22003");
    assert_eq!(
        a.q("SELECT n FROM big WHERE id = 1").single("n"),
        "5000",
        "the failed UPDATE was kept"
    );
}

/// A correlated generate_series column is the name of its own SELECT only, as in PostgreSQL. It
/// was one name for the whole statement: a bare `g` in a sublink, another UNION arm, a CTE body,
/// a derived table's parent, RETURNING or an outer ORDER BY read the series, and two series
/// columns named alike overwrote each other. The answers are PostgreSQL's (by the semantics of
/// these fixtures; a PG18 re-recording is owed with item 17): `NOT IN (SELECT g FROM u)` reads
/// u's g and keeps all 3 rows, where it gave 0 (gap review item 1).
#[test]
fn a_correlated_generate_series_column_is_scoped_to_its_select() {
    let dir = Scratch::new("seriesscope");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE s(x INT)").ok("s");
    a.q("INSERT INTO s VALUES (1), (2)").ok("s rows");
    a.q("CREATE TABLE u(k INT, g INT)").ok("u");
    a.q("INSERT INTO u VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .ok("u rows");
    a.q("CREATE TABLE w(k INT)").ok("w");
    a.q("INSERT INTO w VALUES (1), (2), (3), (4), (5)")
        .ok("w rows");
    a.q("CREATE TABLE ins(g INT)").ok("ins");
    let series = "FROM s, generate_series(1, s.x) AS g";
    let col = |r: &Reply| -> Vec<String> {
        r.rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| v.clone().unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect()
    };
    let rows = |a: &mut Wire, sql: &str| -> Vec<String> { col(&a.q(sql).ok(sql)) };
    let s = |v: &[&str]| -> Vec<String> { v.iter().map(|x| x.to_string()).collect() };
    // Premise: the series itself.
    assert_eq!(
        rows(&mut a, &format!("SELECT g {series} ORDER BY g")),
        s(&["1", "1", "2"])
    );
    // A sublink's bare g is its own relation's column.
    assert_eq!(
        rows(
            &mut a,
            &format!("SELECT g {series} WHERE g NOT IN (SELECT g FROM u) ORDER BY g")
        ),
        s(&["1", "1", "2"])
    );
    // A correlated reference by the series' alias reads it.
    assert_eq!(
        rows(
            &mut a,
            &format!("SELECT g {series} WHERE EXISTS (SELECT 1 FROM w WHERE w.k = g.g) ORDER BY g")
        ),
        s(&["1", "1", "2"])
    );
    // A bare correlated reference names a column w lacks: PostgreSQL reads the series; here it
    // may be refused, but never answered from another column.
    let r = a.q(&format!(
        "SELECT g {series} WHERE EXISTS (SELECT 1 FROM w WHERE w.k = g) ORDER BY g"
    ));
    if r.error.is_none() {
        assert_eq!(col(&r), s(&["1", "1", "2"]), "a bare correlated g");
    }
    // A derived table's parent reads the derived column.
    assert_eq!(
        rows(
            &mut a,
            &format!("SELECT g FROM (SELECT s.x, g {series}) AS sub ORDER BY g")
        ),
        s(&["1", "1", "2"])
    );
    // Another UNION arm's g is u's, in either order.
    let both = s(&["1", "1", "2", "10", "20", "30", "40", "50"]);
    assert_eq!(
        rows(
            &mut a,
            &format!("SELECT g {series} UNION ALL SELECT g FROM u ORDER BY 1")
        ),
        both
    );
    assert_eq!(
        rows(
            &mut a,
            &format!("SELECT g FROM u UNION ALL SELECT g {series} ORDER BY 1")
        ),
        both
    );
    // A CTE body and a scalar subquery read their own relations' g.
    assert_eq!(
        rows(
            &mut a,
            &format!(
                "WITH c AS (SELECT g FROM u) SELECT (SELECT sum(g) FROM c), g {series} ORDER BY g"
            )
        ),
        s(&["150,1", "150,1", "150,2"])
    );
    // RETURNING names the inserted table's column.
    let mut returned = rows(
        &mut a,
        &format!("INSERT INTO ins SELECT g {series} RETURNING g"),
    );
    returned.sort();
    assert_eq!(returned, s(&["1", "1", "2"]));
    // An outer ORDER BY after a sublink that declared the series reads the outer g.
    assert_eq!(
        rows(
            &mut a,
            &format!(
                "SELECT k, g FROM u WHERE EXISTS (SELECT 1 {series} WHERE g.g = u.k) ORDER BY g"
            )
        ),
        s(&["1,10", "2,20"])
    );
    // Two series columns named alike, by their aliases; bare, ambiguous (42702).
    let two = "FROM s, generate_series(1, s.x) AS a(i), generate_series(s.x, 2) AS b(i)";
    assert_eq!(
        rows(&mut a, &format!("SELECT a.i, b.i {two} ORDER BY 1, 2")),
        s(&["1,1", "1,2", "1,2", "2,2"])
    );
    assert_eq!(
        a.q(&format!("SELECT i {two}"))
            .err("bare i of two series")
            .code,
        "42702"
    );
}

/// COPY FROM runs inside whatever block it is in, as PostgreSQL's does: a multi-statement query's
/// implicit block, a pipeline's, a client's BEGIN (whose ROLLBACK undoes it and COMMIT keeps it).
/// A failed COPY in a block undoes its own rows and fails the block. Its own BEGIN was refused
/// inside any open transaction ("cannot start a transaction within a transaction"), so COPY failed
/// in every implicit block since 7bc7dab70, and in every client block (wire review 4 item 2).
#[test]
fn copy_from_runs_inside_the_block_it_is_in() {
    let dir = Scratch::new("copyblock");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cp(id INT, v TEXT)").ok("cp");
    let good = dir.0.join("good.tsv");
    std::fs::write(&good, "1\tone\n2\ttwo\n").unwrap();
    let bad = dir.0.join("bad.tsv");
    std::fs::write(&bad, "5\tfive\nnot-a-number\tx\n").unwrap();
    let copy = format!("COPY cp FROM '{}'", good.display());
    let count = |a: &mut Wire| a.q("SELECT count(*) FROM cp").single("count");
    a.q(&format!("{copy}; SELECT 1"))
        .ok("COPY in a multi-statement query");
    assert_eq!(count(&mut a), "2");
    a.q("BEGIN").ok("begin");
    let r = a.q(&copy).ok("COPY in a block");
    assert_eq!(r.status, b'T');
    a.q("ROLLBACK").ok("rollback");
    assert_eq!(count(&mut a), "2", "ROLLBACK kept the block's COPY");
    a.q("BEGIN").ok("begin");
    a.q(&copy).ok("COPY in a block");
    a.q("COMMIT").ok("commit");
    assert_eq!(count(&mut a), "4", "COMMIT lost the block's COPY");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO cp VALUES (9, 'nine')").ok("insert");
    let r = a.q(&format!("COPY cp FROM '{}'", bad.display()));
    assert!(r.error.is_some(), "a COPY with a bad row loaded it");
    assert_eq!(r.status, b'E', "the failed COPY did not fail the block");
    let r = a.q("COMMIT").ok("the block's end");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(count(&mut a), "4", "the failed block kept rows");
}

/// An undeclared parameter has the type PostgreSQL infers from its context, the same at Describe
/// and at Bind: the column it is compared with, assigned to or inserted into, an aggregate it is
/// compared with, LIMIT's bigint, a cast's type; text where nothing says. A $n with an unused $k
/// below it is refused with 42P18. Bind guessed from the value (an integer, then a float, then a
/// boolean), so '007', 't' and '1e3' went into a text column as 7, 1 and 1000.0 while Describe
/// said text; and a failed bind was dropped (wire review 4 item 3).
#[test]
fn an_undeclared_parameter_has_its_inferred_type_at_describe_and_bind() {
    const INT8: u32 = 20;
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    let dir = Scratch::new("paraminfer");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Bind: a text column's value as given.
    for (id, v) in [("2", "007"), ("3", "t"), ("4", "1e3")] {
        a.xt(
            "INSERT INTO t VALUES ($1, $2)",
            &[(0, 0, id.as_bytes()), (0, 0, v.as_bytes())],
        )
        .ok("insert, undeclared");
        assert_eq!(
            a.q(&format!("SELECT v FROM t WHERE id = {id}")).single("v"),
            v,
            "the text parameter was rewritten"
        );
    }
    // An integer column's parameter compares as an integer; so does one compared with count(*).
    let r = a.xt("SELECT v FROM t WHERE id = $1", &[(0, 0, b"2")]);
    assert_eq!(r.ok("by key").rows, vec![vec![Some("007".to_string())]]);
    let r = a.xt(
        "SELECT count(*) FROM t HAVING count(*) > $1",
        &[(0, 0, b"1")],
    );
    assert_eq!(
        r.ok("having").rows.len(),
        1,
        "count(*) > $1 compared as text"
    );
    let r = a.xt("SELECT id FROM t ORDER BY id LIMIT $1", &[(0, 0, b"2")]);
    assert_eq!(r.ok("limit").rows.len(), 2);
    // Describe says the same.
    for (sql, want) in [
        ("SELECT v FROM t WHERE id = $1", vec![INT4]),
        ("INSERT INTO t VALUES ($1, $2)", vec![INT4, TEXT]),
        ("UPDATE t SET v = $2 WHERE id = $1", vec![INT4, TEXT]),
        ("SELECT count(*) FROM t HAVING count(*) > $1", vec![INT8]),
        ("SELECT id FROM t LIMIT $1", vec![INT8]),
        ("SELECT $1::int4", vec![INT4]),
        ("SELECT $1", vec![TEXT]),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(want), "{sql}");
    }
    // A gap below the highest $n: PostgreSQL cannot type $1.
    let r = a.describe_statement("SELECT v FROM t WHERE id = $2");
    assert_eq!(r.err("$2 alone").code, "42P18");
    // One parameter more than the statement has, none declared: the bind fails (08P01). Declared
    // (one OID 0) but unused, the parameter cannot be typed: 42P18, as PostgreSQL answers.
    let mut parse = vec![0u8];
    parse.extend_from_slice(b"SELECT 1");
    parse.extend_from_slice(&[0, 0, 0]);
    a.send(b'P', &parse);
    let mut bind = vec![0u8, 0u8];
    bind.extend_from_slice(&0i16.to_be_bytes());
    bind.extend_from_slice(&1i16.to_be_bytes());
    bind.extend_from_slice(&1i32.to_be_bytes());
    bind.push(b'1');
    bind.extend_from_slice(&0i16.to_be_bytes());
    a.send(b'B', &bind);
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err("an extra parameter, none declared").code, "08P01");
    let r = a.xt("SELECT 1", &[(0, 0, b"1")]);
    assert_eq!(r.err("an unused declared parameter").code, "42P18");
}

/// HAVING without GROUP BY filters the one aggregate row, as in PostgreSQL: `SELECT count(*) FROM t
/// HAVING count(*) > 5` over 4 rows returns no row. The translator dropped a HAVING that had no
/// GROUP BY, so the row came back whatever the condition (found by
/// an_undeclared_parameter_has_its_inferred_type_at_describe_and_bind's HAVING $1 case).
#[test]
fn having_without_group_by_filters_the_aggregate_row() {
    let dir = Scratch::new("havingonly");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, 'b'), (3, 'c'), (4, 'd')")
        .ok("rows");
    let r = a
        .q("SELECT count(*) FROM t HAVING count(*) > 5")
        .ok("having, false");
    assert_eq!(r.rows.len(), 0, "a false HAVING kept the row: {:?}", r.rows);
    let r = a
        .q("SELECT count(*) FROM t HAVING count(*) > 3")
        .ok("having, true");
    assert_eq!(r.rows, vec![vec![Some("4".to_string())]]);
    // An undeclared $1 is bigint here (count(*)'s type), so it compares as a number.
    let having = "SELECT count(*) FROM t HAVING count(*) > $1";
    let r = a.xt(having, &[(0, 0, b"5")]);
    assert_eq!(r.ok("having $1 = 5").rows.len(), 0);
    let r = a.xt(having, &[(0, 0, b"3")]);
    assert_eq!(
        r.ok("having $1 = 3").rows,
        vec![vec![Some("4".to_string())]]
    );
}

/// A FATAL error ends the session: after a startup refused FATAL (a branch that does not exist),
/// a query sent on the same socket is never run, and the socket closes. pgwire answered the FATAL,
/// shut its write half only and went on reading, so the query ran on the trunk unseen (wire review
/// 5 item 4).
#[test]
fn a_fatal_startup_error_ends_the_session() {
    let dir = Scratch::new("fatalend");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let s = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut w = Wire { s };
    let mut body = Vec::new();
    body.extend_from_slice(&196608i32.to_be_bytes());
    for (k, v) in [("user", "postgres"), ("database", "postgres/nosuch")] {
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
    assert_eq!(
        r.err("startup on a branch that does not exist").code,
        "3D000"
    );
    // The query goes out whatever the server did; whether the write is refused does not matter.
    let mut q = b"INSERT INTO t VALUES (7, 'after fatal')".to_vec();
    q.push(0);
    let mut m = vec![b'Q'];
    m.extend_from_slice(&((q.len() + 4) as i32).to_be_bytes());
    m.extend_from_slice(&q);
    let _ = w.s.write_all(&m);
    let r = w.read_reply();
    assert!(
        r.error.is_none() && r.status == 0 && r.tags.is_empty(),
        "the session answered after its FATAL: {:?} {:?}",
        r.tags,
        r.error
    );
    // Give a query that did reach the session the time to run before looking.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 7").single("rows"),
        "0",
        "a query after the FATAL ran"
    );
}

/// A parameter number past PostgreSQL's limit (65535: Bind counts parameters in 16 bits) is refused
/// with 42P02 before anything is sized by it, on both protocols, and the session answers on.
/// `SELECT 1 LIMIT $2147483647` sized the inferred-type list by the number (about 16 GiB zeroed),
/// from one unauthenticated simple query (wire review 8 item 3).
#[test]
fn a_parameter_number_past_the_limit_is_refused() {
    let dir = Scratch::new("paramlimit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in [
        "SELECT 1 LIMIT $2147483647",
        "SELECT v FROM t WHERE id = $65536",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "42P02", "{sql}");
        let r = a.describe_statement(sql);
        assert_eq!(r.err(sql).code, "42P02", "{sql}, extended");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
    }
}

/// A `$n` sent over the simple protocol names no parameter (nothing binds one there): 42P02 "there
/// is no parameter $1", before the statement runs, as PostgreSQL answers. It ran with the parameter
/// unbound, which the engine reads as NULL: `UPDATE t SET v = $1` set every row's v to NULL,
/// `DELETE ... WHERE id = $1` answered DELETE 0, and a branch call answered 08P01 (wire review 9
/// item 3).
#[test]
fn a_parameter_over_the_simple_protocol_is_refused() {
    let dir = Scratch::new("simpleparam");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in [
        "UPDATE t SET v = $1",
        "DELETE FROM t WHERE id = $1",
        "INSERT INTO t VALUES (2, $1)",
        "SELECT v FROM t WHERE id = $1",
        "SELECT turso_branch_create($1)",
    ] {
        let r = a.q(sql);
        let e = r.err(sql);
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("42P02", "there is no parameter $1"),
            "{sql}"
        );
        assert_eq!(
            a.q("SELECT id, v FROM t ORDER BY id").ok(sql).rows,
            vec![vec![Some("1".to_string()), Some("trunk".to_string())]],
            "{sql}: nothing changed"
        );
    }
    // The same statement in a multi-statement query fails it there, and the statements before it
    // are rolled back with the implicit block.
    let r = a.q("INSERT INTO t VALUES (3, 'x'); UPDATE t SET v = $1");
    assert_eq!(r.err("in a multi-statement query").code, "42P02");
    assert_eq!(
        a.q("SELECT count(*) FROM t")
            .single("the block rolled back"),
        "1"
    );
}

/// A branch call's `$n` past the limit is refused with 42P02 too, at Describe and over the simple
/// protocol, and the server serves on. Branch calls never reached the prepare-time limit: Describe
/// sized its parameter list by the number, so `$18446744073709551615` panicked on capacity overflow
/// and `$2147483647` asked for about 32 GiB, and under the release build's panic=abort one client
/// ended every session (wire review 9 item 1). The 20-digit case comes first, so at the base the
/// test fails on its panic before it asks for the 32 GiB.
#[test]
fn a_branch_call_parameter_past_the_limit_is_refused() {
    let dir = Scratch::new("branchparamlimit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in [
        "SELECT turso_branch_create($18446744073709551615)",
        "SELECT turso_branch_create($65536)",
        "SELECT turso_branch_switch($2147483647)",
        "SELECT turso_branch_create($99999999999999999999999)",
    ] {
        let r = a.describe_statement(sql);
        assert_eq!(r.err(sql).code, "42P02", "{sql}, extended");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "42P02", "{sql}");
        let mut b = server.connect();
        assert_eq!(b.q("SELECT 1").single("a second session is served"), "1");
    }
}

/// A statement's parameters are every $n its text holds, whatever the engine compiles: a $n in a
/// clause the engine folds away (a false AND, an OR with a true side) or a HAVING still counts, is
/// described and is bound, as in PostgreSQL. They were read from the engine's slots, so
/// `HAVING count(*) > $1` was 42P18, `WHERE false AND id = $1` refused its one parameter (08P01),
/// and `id = $2 AND (true OR v = $1)` was 42P18 (wire review 8 item 5).
#[test]
fn every_parameter_in_the_text_is_a_parameter() {
    const INT8: u32 = 20;
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    let dir = Scratch::new("paramtree");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for (sql, want, binds, rows) in [
        (
            "SELECT count(*) FROM t HAVING count(*) > $1",
            vec![INT8],
            vec![&b"0"[..]],
            1,
        ),
        (
            "SELECT v FROM t WHERE false AND id = $1",
            vec![INT4],
            vec![&b"1"[..]],
            0,
        ),
        (
            "SELECT v FROM t WHERE id = $2 AND (true OR v = $1)",
            vec![TEXT, INT4],
            vec![&b"x"[..], &b"1"[..]],
            1,
        ),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(want), "{sql}");
        let params: Vec<(u32, i16, &[u8])> = binds.iter().map(|b| (0, 0, *b)).collect();
        let r = a.xt(sql, &params).ok(sql);
        assert_eq!(r.rows.len(), rows, "{sql}");
    }
}

/// Bind decodes each parameter in the format its code names: a binary int4, int8, float8, bool,
/// bytea and jsonb (whose first byte is its version) as PostgreSQL's binary receive functions read
/// them, the text format as text. A format-code list that is neither 0, 1 nor one per parameter is
/// a protocol violation (08P01). Every parameter was decoded as UTF-8 text: a binary int4 2 failed
/// XX000, the bytes "0001" bound 1, a jsonb kept its version byte and a non-UTF-8 bytea failed
/// (wire review 8 item 4; drivers send binary once Describe names a type).
#[test]
fn bind_reads_each_parameter_in_its_format() {
    let dir = Scratch::new("bindformat");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, '007')").ok("row 2");
    let r = a
        .xt(
            "SELECT v FROM t WHERE id = $1",
            &[(0, 1, &2i32.to_be_bytes())],
        )
        .ok("binary int4");
    assert_eq!(r.rows, vec![vec![Some("007".to_string())]]);
    // "0001" as a binary int4 is 808464433, not 1.
    let r = a
        .xt("SELECT v FROM t WHERE id = $1", &[(0, 1, b"0001")])
        .ok("binary int4 of text bytes");
    assert!(
        r.rows.is_empty(),
        "the bytes 0001 bound as text: {:?}",
        r.rows
    );
    a.q("CREATE TABLE ty(i BIGINT, f DOUBLE PRECISION, b BOOLEAN, y BYTEA, j JSONB)")
        .ok("ty");
    let mut jsonb = vec![1u8];
    jsonb.extend_from_slice(br#"{"a": 1}"#);
    a.xt(
        "INSERT INTO ty VALUES ($1, $2, $3, $4, $5)",
        &[
            (0, 1, &(-5i64).to_be_bytes()),
            (0, 1, &2.5f64.to_be_bytes()),
            (0, 1, &[1u8]),
            (0, 1, &[0xff, 0x00, 0xfe]),
            (0, 1, &jsonb),
        ],
    )
    .ok("binary int8, float8, bool, bytea, jsonb");
    let r = a
        .q("SELECT i, f, b, encode(y, 'hex'), j->>'a' FROM ty")
        .ok("read back");
    let row =
        |v: [&str; 5]| -> Vec<Option<String>> { v.iter().map(|x| Some(x.to_string())).collect() };
    assert_eq!(r.rows, vec![row(["-5", "2.5", "t", "ff00fe", "1"])]);
    // Two parameters, three format codes.
    let mut parse = vec![0u8];
    parse.extend_from_slice(b"SELECT $1::int4 + $2::int4");
    parse.extend_from_slice(&[0, 0, 0]);
    a.send(b'P', &parse);
    let mut bind = vec![0u8, 0u8];
    bind.extend_from_slice(&3i16.to_be_bytes());
    for _ in 0..3 {
        bind.extend_from_slice(&0i16.to_be_bytes());
    }
    bind.extend_from_slice(&2i16.to_be_bytes());
    for v in [b"1", b"2"] {
        bind.extend_from_slice(&1i32.to_be_bytes());
        bind.extend_from_slice(v);
    }
    bind.extend_from_slice(&0i16.to_be_bytes());
    a.send(b'B', &bind);
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err("three format codes for two parameters").code, "08P01");
    assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
}

/// A bytea parameter in text format whose hex holds a non-ASCII character is 22P02, and the server
/// serves on: the hex was sliced as a &str, inside the character, which panicked the session (and
/// under the release build's panic=abort every session) from one Bind that declares OID 17 (wire
/// review 9 item 5).
#[test]
fn a_bytea_parameter_with_a_non_ascii_digit_is_refused() {
    const BYTEA: u32 = 17;
    let dir = Scratch::new("byteahex");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE ty(y BYTEA)").ok("ty");
    for bytes in [&b"\\x0\xc3\xa90"[..], "\\x0\u{1F600}0".as_bytes()] {
        let r = a.xt("INSERT INTO ty(y) VALUES ($1)", &[(BYTEA, 0, bytes)]);
        assert_eq!(r.err("a non-ASCII hex digit").code, "22P02", "{bytes:?}");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
        let mut b = server.connect();
        assert_eq!(b.q("SELECT 1").single("a second session"), "1");
    }
    a.xt("INSERT INTO ty(y) VALUES ($1)", &[(BYTEA, 0, b"\\x00ff")])
        .ok("valid hex");
    assert_eq!(a.q("SELECT count(*) FROM ty").single("one row"), "1");
}

/// A Bind PostgreSQL refuses is refused AT the Bind, before BindComplete, so the Execute after it
/// runs nothing: a parameter format code other than 0 or 1 (22023 "unsupported format code: 2",
/// even for a NULL value), a parameter-format list that is neither 0, 1 nor one per parameter
/// (08P01), and, for a branch call, a parameter count other than the call's (08P01). A result
/// format code other than 0 or 1 is refused here too (22023), where PostgreSQL refuses it at
/// Execute as it formats the first row: pgwire folds a single code into text, so only the Bind
/// shows it (E5-QUEUE R2).
/// These were checked at Execute, or for a branch call not at all: `SELECT turso_branch_create($1)`
/// bound with three format codes, with two values or with code 2 created the branch durably and
/// acknowledged it (wire review 10 item 5).
#[test]
fn a_bind_postgresql_refuses_is_refused_before_bind_complete() {
    let dir = Scratch::new("bindchecks");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse, Bind (the given parameter codes, values and result codes), Execute, Sync; then every
    // message type byte up to ReadyForQuery and the first error.
    fn round(
        w: &mut Wire,
        sql: &str,
        pcodes: &[i16],
        values: &[Option<&[u8]>],
        rcodes: &[i16],
    ) -> (Vec<u8>, Option<WireError>) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&(pcodes.len() as i16).to_be_bytes());
        for c in pcodes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            match v {
                Some(v) => {
                    bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    bind.extend_from_slice(v);
                }
                None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        bind.extend_from_slice(&(rcodes.len() as i16).to_be_bytes());
        for c in rcodes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        let (mut tags, mut error) = (Vec::new(), None);
        loop {
            let mut head = [0u8; 5];
            w.s.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            w.s.read_exact(&mut body).unwrap();
            tags.push(head[0]);
            if head[0] == b'E' && error.is_none() {
                error = Some(error_fields(&body));
            }
            if head[0] == b'Z' {
                return (tags, error);
            }
        }
    }
    let create = "SELECT turso_branch_create($1)";
    let cases: Vec<(&str, &str, Vec<i16>, Vec<Option<&[u8]>>, Vec<i16>, &str)> = vec![
        (
            "three codes, one parameter",
            create,
            vec![0, 0, 0],
            vec![Some(&b"b1"[..])],
            vec![],
            "08P01",
        ),
        (
            "two values for one parameter",
            create,
            vec![],
            vec![Some(&b"b1"[..]), Some(&b"b2"[..])],
            vec![],
            "08P01",
        ),
        (
            "parameter code 2",
            create,
            vec![2],
            vec![Some(&b"b1"[..])],
            vec![],
            "22023",
        ),
        (
            "parameter code 2, NULL",
            create,
            vec![2],
            vec![None],
            vec![],
            "22023",
        ),
        (
            "result code 2",
            create,
            vec![],
            vec![Some(&b"b1"[..])],
            vec![2],
            "22023",
        ),
        (
            "result code -1",
            "SELECT 1",
            vec![],
            vec![],
            vec![-1],
            "22023",
        ),
        (
            "engine: three codes, two values",
            "SELECT $1::int4 + $2::int4",
            vec![0, 0, 0],
            vec![Some(&b"1"[..]), Some(&b"2"[..])],
            vec![],
            "08P01",
        ),
    ];
    for (what, sql, pcodes, values, rcodes, code) in cases {
        let (tags, error) = round(&mut a, sql, &pcodes, &values, &rcodes);
        let e = error.unwrap_or_else(|| panic!("{what}: no error, messages {tags:?}"));
        assert_eq!(e.code, code, "{what}: {e:?}");
        if code == "22023" {
            assert!(
                e.message.starts_with("unsupported format code: "),
                "{what}: {e:?}"
            );
        }
        assert!(
            !tags.contains(&b'2'),
            "{what}: BindComplete was sent: {tags:?}"
        );
        assert_eq!(
            a.q("SELECT 1").single(what),
            "1",
            "{what}: the session answers"
        );
    }
    for name in ["b1", "b2"] {
        let r = a.q(&format!("SELECT turso_branch_switch('{name}')"));
        assert_eq!(r.err(name).code, "3D000", "branch {name} must not exist");
    }
}

/// A Parse or Bind whose body ends before what it says it holds is refused with 08P01
/// "insufficient data left in message", an ERROR, as PostgreSQL's pq_getmsg* refuse it: the frame
/// was read whole, so the session skips to Sync and serves on, and no byte past the frame is read
/// as its body. A parameter length below -1 is the same refusal (only -1 is NULL). A frame whose
/// length cannot hold itself (below 4) ends the session. pgwire decoded a message's body from the
/// whole read buffer with unchecked reads: a 6-byte Bind panicked the session (and under the
/// release build's panic=abort, the server, before any login), a length past the frame read the
/// messages pipelined behind it as parameter bytes, and every negative length read as NULL (wire
/// review 10 item 2).
#[test]
fn a_message_body_shorter_than_it_says_is_refused_in_step() {
    let dir = Scratch::new("shortbody");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    let parse = |sql: &str| -> Vec<u8> {
        let mut p = vec![0u8];
        p.extend_from_slice(sql.as_bytes());
        p.extend_from_slice(&[0, 0, 0]);
        frame(b'P', &p)
    };
    // A Bind of one parameter, its length field as given and `bytes` after it, then no result
    // formats.
    let bind_one = |len: i32, bytes: &[u8]| -> Vec<u8> {
        let mut b = vec![0u8, 0u8];
        b.extend_from_slice(&0i16.to_be_bytes());
        b.extend_from_slice(&1i16.to_be_bytes());
        b.extend_from_slice(&len.to_be_bytes());
        b.extend_from_slice(bytes);
        b.extend_from_slice(&0i16.to_be_bytes());
        frame(b'B', &b)
    };
    let execute = frame(b'E', &[0, 0, 0, 0, 0]);
    let sync = frame(b'S', &[]);
    let rounds: Vec<(&str, Vec<u8>)> = vec![
        // Portal and statement names, then nothing: the format count is missing.
        (
            "a 6-byte Bind",
            [parse("SELECT 1"), frame(b'B', &[0, 0]), sync.clone()].concat(),
        ),
        (
            "a Parse with no parameter count",
            [frame(b'P', b"\0SELECT 1\0"), sync.clone()].concat(),
        ),
        (
            "a parameter length of -2",
            [
                parse("SELECT $1::text"),
                bind_one(-2, b""),
                execute.clone(),
                sync.clone(),
            ]
            .concat(),
        ),
        (
            "a parameter length past its frame",
            [
                parse("SELECT $1::text"),
                bind_one(8, b"x"),
                execute.clone(),
                sync.clone(),
            ]
            .concat(),
        ),
        (
            "a parameter length past everything sent",
            [
                parse("SELECT $1::text"),
                bind_one(1000, b"x"),
                execute.clone(),
                sync.clone(),
            ]
            .concat(),
        ),
    ];
    for (what, bytes) in rounds {
        a.s.write_all(&bytes).unwrap();
        let r = a.read_reply();
        let e = r.err(what);
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("08P01", "insufficient data left in message"),
            "{what}"
        );
        assert_eq!(r.status, b'I', "{what}: the session is idle after Sync");
        assert_eq!(
            a.q("SELECT 1").single(what),
            "1",
            "{what}: the session answers"
        );
        let mut b = server.connect();
        assert_eq!(
            b.q("SELECT 1").single(what),
            "1",
            "{what}: a second session"
        );
    }
    // A frame length of 2 cannot hold itself: the session ends, and the server serves on.
    let mut c = server.connect();
    c.s.write_all(&[b'S', 0, 0, 0, 2]).unwrap();
    let r = c.read_reply();
    assert_eq!(
        r.status, 0,
        "a frame length below 4 ends the session: {r:?}"
    );
    assert_eq!(a.q("SELECT 1").single("after the bad frame"), "1");
    let mut b = server.connect();
    assert_eq!(b.q("SELECT 1").single("after the bad frame"), "1");
}

/// A Bind whose result-format list is neither empty, one code, nor one code per result column is a
/// protocol violation (08P01, PostgreSQL's "bind message has N result formats but query has M
/// columns"), at Describe of the portal and at Execute, and the session and a second connection are
/// served; the same for a parameter-format list that is neither 0, 1 nor one per parameter. pgwire
/// reads the result list unchecked (`fv[idx]`), so two codes for three columns indexed past it and
/// panicked the session, and under the release build's panic=abort ended every session (wire
/// review 9 item 2). A statement that returns no rows ignores the result list, as PostgreSQL does.
#[test]
fn a_bind_format_list_of_the_wrong_length_is_a_protocol_violation() {
    let dir = Scratch::new("bindformats");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse (no declared types), Bind with the given parameter codes, values and result codes,
    // optionally Describe the portal, Execute, Sync.
    fn round(
        w: &mut Wire,
        sql: &str,
        pcodes: &[i16],
        values: &[&[u8]],
        rcodes: &[i16],
        describe: bool,
    ) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&(pcodes.len() as i16).to_be_bytes());
        for c in pcodes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
            bind.extend_from_slice(v);
        }
        bind.extend_from_slice(&(rcodes.len() as i16).to_be_bytes());
        for c in rcodes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        w.send(b'B', &bind);
        if describe {
            w.send(b'D', b"P\0");
        }
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    for describe in [true, false] {
        for (sql, rcodes) in [
            ("SELECT 1, 2, 3", &[0i16, 0][..]),
            ("SELECT 1, 2, 3", &[0, 0, 0, 0][..]),
            ("SELECT turso_branch_stats()", &[0, 1][..]),
            ("SELECT turso_branch_current()", &[0, 0][..]),
        ] {
            let what = format!(
                "{sql} with {} result formats, describe {describe}",
                rcodes.len()
            );
            let r = round(&mut a, sql, &[], &[], rcodes, describe);
            assert_eq!(r.err(&what).code, "08P01", "{what}");
            assert_eq!(r.status, b'I', "{what}");
            assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
            let mut b = server.connect();
            assert_eq!(b.q("SELECT 1").single("a second session is served"), "1");
        }
    }
    // Two parameter codes for three parameters.
    let r = round(
        &mut a,
        "SELECT $1::int4 + $2::int4 + $3::int4",
        &[0, 0],
        &[b"1", b"2", b"3"],
        &[],
        false,
    );
    assert_eq!(
        r.err("two parameter formats, three parameters").code,
        "08P01"
    );
    assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
    // One code per column, and a write with no rows, which ignores the list.
    let r = round(&mut a, "SELECT 1, 2, 3", &[], &[], &[0, 0, 0], true);
    assert_eq!(
        r.ok("three formats, three columns").rows,
        vec![vec![Some("1".into()), Some("2".into()), Some("3".into())]]
    );
    round(
        &mut a,
        "INSERT INTO t VALUES (7, 'x')",
        &[],
        &[],
        &[0, 0],
        false,
    )
    .ok("no rows, two formats");
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 7").single("the write ran"),
        "x"
    );
}

/// `col = ANY($1)` and `col <> ALL($1)` give an undeclared $1 the ARRAY of the column's type, as
/// PostgreSQL does (int4[], 1007), so a client sends '{1,2}' and gets rows 1 and 2. It was given
/// the element type (int4), so '{1,2}' failed at Bind (wire review 8 item 6).
#[test]
fn any_and_all_of_a_parameter_take_an_array() {
    const INT4_ARRAY: u32 = 1007;
    let dir = Scratch::new("anyparam");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, 'b'), (3, 'c')").ok("rows");
    for (sql, want) in [
        (
            "SELECT id FROM t WHERE id = ANY($1) ORDER BY id",
            vec!["1", "2"],
        ),
        (
            "SELECT id FROM t WHERE id <> ALL($1) ORDER BY id",
            vec!["3"],
        ),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(vec![INT4_ARRAY]), "{sql}");
        let r = a.xt(sql, &[(0, 0, b"{1,2}")]).ok(sql);
        let got: Vec<String> = r.rows.iter().map(|row| row[0].clone().unwrap()).collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// `$1 = ANY(xs)` over an array column takes the column's ELEMENT type, as PostgreSQL types it
/// (int4, 23, for an int[] column), so '1' finds the rows whose array holds 1 and `$1 <> ALL(xs)`
/// excludes them. The column's type was read from its declared name alone (an int[] column's is
/// INTEGER: the dimensions are kept apart), so xs was int4, its element type nothing, and $1 text:
/// text '1' equals no integer element, so `= ANY` found no row and `<> ALL` kept the rows holding 1
/// (wire review 10 item 3, a regression from ed890b346).
#[test]
fn a_parameter_against_an_array_column_takes_its_element_type() {
    const INT4: u32 = 23;
    let dir = Scratch::new("anycolumn");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE a(id INT PRIMARY KEY, xs INT[])").ok("a");
    a.q("INSERT INTO a VALUES (1, ARRAY[1, 2]), (2, ARRAY[3]), (3, ARRAY[1])")
        .ok("rows");
    for (sql, want) in [
        (
            "SELECT id FROM a WHERE $1 = ANY(xs) ORDER BY id",
            vec!["1", "3"],
        ),
        (
            "SELECT id FROM a WHERE $1 <> ALL(xs) ORDER BY id",
            vec!["2"],
        ),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(vec![INT4]), "{sql}");
        let r = a.xt(sql, &[(0, 0, b"1")]).ok(sql);
        let got: Vec<String> = r.rows.iter().map(|row| row[0].clone().unwrap()).collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// An array parameter is read at Bind as PostgreSQL's array_in reads it, each element by the array's
/// element type: bool[] '{t}' is {true}, text[] '{1,2}' is two texts, a quoted element keeps its
/// comma, NULL is NULL, and an element its type cannot read ('1.0' for int4) is 22P02. The text was
/// bound as is and the engine guessed each element's type from its spelling (an integer, then a
/// float, then text), so '{t}' matched no true row, '{1,2}' no text '1', and int4 '{1.0}' matched 1
/// (wire review 10 item 4).
#[test]
fn an_array_parameter_is_read_by_its_element_type() {
    let dir = Scratch::new("arrayparam");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE b(id INT PRIMARY KEY, flag BOOLEAN, v TEXT)")
        .ok("b");
    a.q("INSERT INTO b VALUES (1, true, '1'), (2, false, '007'), (3, false, 'x,y')")
        .ok("rows");
    for (sql, value, want) in [
        (
            "SELECT id FROM b WHERE flag = ANY($1) ORDER BY id",
            "{t}",
            vec!["1"],
        ),
        (
            "SELECT id FROM b WHERE v = ANY($1) ORDER BY id",
            "{1,2}",
            vec!["1"],
        ),
        (
            "SELECT id FROM b WHERE v = ANY($1) ORDER BY id",
            "{007}",
            vec!["2"],
        ),
        (
            "SELECT id FROM b WHERE v = ANY($1) ORDER BY id",
            "{\"x,y\", NULL}",
            vec!["3"],
        ),
        (
            "SELECT id FROM b WHERE id = ANY($1) ORDER BY id",
            "{ 1 , 3 }",
            vec!["1", "3"],
        ),
        (
            "SELECT id FROM b WHERE id = ANY($1) ORDER BY id",
            "{}",
            vec![],
        ),
    ] {
        let r = a.xt(sql, &[(0, 0, value.as_bytes())]).ok(sql);
        let got: Vec<String> = r.rows.iter().map(|row| row[0].clone().unwrap()).collect();
        assert_eq!(got, want, "{sql} with {value}");
    }
    for value in ["{1.0}", "{1,x}", "1,2", "{1,2"] {
        let sql = "SELECT id FROM b WHERE id = ANY($1)";
        let r = a.xt(sql, &[(0, 0, value.as_bytes())]);
        assert_eq!(r.err(value).code, "22P02", "{value}");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
    }
}

/// An undeclared parameter is typed by every context PostgreSQL types it by, so a value sent as
/// text compares as PostgreSQL compares it: a scalar function's result (length() is int4), COALESCE,
/// CASE, a scalar subquery and sum() take their arms' or arguments' types; a bare $n in WHERE or OR
/// is boolean; a derived table's and a CTE's columns have their sources' types; ON CONFLICT DO
/// UPDATE and a set operation's LIMIT are read. Each was text (no context typed it), so '3' never
/// equalled length('abc') and a derived count never exceeded '1'. A parameter compared with
/// something no context types is refused (42P18) rather than compared as text (wire review 8
/// item 7). Expected values: PostgreSQL's by these fixtures' semantics; the PG18 re-recording is
/// owed with item 17.
#[test]
fn untyped_contexts_type_their_parameters() {
    const BOOL: u32 = 16;
    const INT8: u32 = 20;
    const INT4: u32 = 23;
    let dir = Scratch::new("untypedctx");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, name TEXT, n INT)")
        .ok("p");
    a.q("INSERT INTO p VALUES (1, 'abc', 10), (2, 'de', NULL), (3, 'fghi', 30)")
        .ok("rows");
    for (sql, want_type, bind, want_rows) in [
        (
            "SELECT id FROM p WHERE length(name) = $1",
            INT4,
            "3",
            vec!["1"],
        ),
        (
            "SELECT id FROM p WHERE coalesce(n, 0) = $1",
            INT4,
            "0",
            vec!["2"],
        ),
        (
            "SELECT id FROM p WHERE CASE WHEN n IS NULL THEN 0 ELSE n END = $1",
            INT4,
            "30",
            vec!["3"],
        ),
        (
            "SELECT id FROM p WHERE (SELECT max(n) FROM p) = $1 ORDER BY id",
            INT4,
            "30",
            vec!["1", "2", "3"],
        ),
        (
            "SELECT 1 FROM p HAVING sum(n * 2) > $1",
            INT8,
            "79",
            vec!["1"],
        ),
        (
            "SELECT id FROM p WHERE $1 OR id = 1 ORDER BY id",
            BOOL,
            "false",
            vec!["1"],
        ),
        (
            "SELECT c FROM (SELECT count(*) AS c FROM p) AS d WHERE c > $1",
            INT8,
            "2",
            vec!["3"],
        ),
        (
            "WITH w AS (SELECT n FROM p) SELECT count(*) FROM w WHERE n > $1",
            INT4,
            "10",
            vec!["1"],
        ),
        (
            "SELECT id FROM p UNION ALL SELECT id FROM p ORDER BY 1 LIMIT $1",
            INT8,
            "2",
            vec!["1", "1"],
        ),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(vec![want_type]), "{sql}");
        let r = a.xt(sql, &[(0, 0, bind.as_bytes())]).ok(sql);
        let got: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].clone().unwrap_or_default())
            .collect();
        assert_eq!(got, want_rows, "{sql} with {bind}");
    }
    // ON CONFLICT DO UPDATE: its SET takes the column's type and its WHERE is boolean.
    let sql = "INSERT INTO p VALUES (1, 'x', 0) ON CONFLICT (id) DO UPDATE SET n = $1 WHERE $2";
    let r = a.describe_statement(sql).ok(sql);
    assert_eq!(r.params, Some(vec![INT4, BOOL]), "{sql}");
    a.xt(sql, &[(0, 0, b"11"), (0, 0, b"true")]).ok(sql);
    assert_eq!(a.q("SELECT n FROM p WHERE id = 1").single("n"), "11");
    // Compared with something no context types: refused, not compared as text.
    let sql = "SELECT id FROM p WHERE no_such_typing(name) = $1";
    let r = a.describe_statement(sql);
    assert!(
        r.error
            .as_ref()
            .is_some_and(|e| e.code == "42P18" || e.code == "42883"),
        "{sql}: {:?}",
        r.error
    );
}

/// Set operations keep PostgreSQL's grouping: INTERSECT binds tighter than UNION and EXCEPT, and
/// parentheses group; a parenthesised arm keeps its own ORDER BY, LIMIT and WITH; and the WITH of
/// the whole set operation is in scope for every arm. The tree was flattened and run left to right
/// (`SELECT 1 UNION SELECT 2 INTERSECT SELECT 2` gave {2}), an arm's ORDER BY / LIMIT and every
/// WITH were dropped, so a CTE named like a table read the table (wire review 7 items 1 and 2).
/// Expected values: PostgreSQL's by the SQL standard's grouping; the PG18 re-recording is owed
/// with item 17.
#[test]
fn set_operations_keep_their_grouping_and_clauses() {
    let dir = Scratch::new("setops");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE u(k INT)").ok("u");
    a.q("INSERT INTO u VALUES (1), (2), (3)").ok("u rows");
    a.q("CREATE TABLE w(k INT)").ok("w");
    a.q("INSERT INTO w VALUES (7), (8)").ok("w rows");
    let sorted = |a: &mut Wire, sql: &str| -> Vec<String> {
        let mut v: Vec<String> = a
            .q(sql)
            .ok(sql)
            .rows
            .iter()
            .map(|r| r[0].clone().unwrap_or_default())
            .collect();
        v.sort();
        v
    };
    let s = |v: &[&str]| -> Vec<String> { v.iter().map(|x| x.to_string()).collect() };
    for (sql, want) in [
        ("SELECT 1 UNION SELECT 2 INTERSECT SELECT 2", s(&["1", "2"])),
        ("SELECT 1 EXCEPT (SELECT 1 EXCEPT SELECT 1)", s(&["1"])),
        ("SELECT 1 UNION ALL (SELECT 1 UNION SELECT 1)", s(&["1", "1"])),
        ("(SELECT 1 UNION SELECT 2) INTERSECT SELECT 2", s(&["2"])),
        (
            "(SELECT k FROM u ORDER BY k DESC LIMIT 1) UNION ALL (SELECT k FROM w ORDER BY k LIMIT 1)",
            s(&["3", "7"]),
        ),
        ("WITH c AS (SELECT 5 AS x) SELECT x FROM c UNION ALL SELECT 2", s(&["2", "5"])),
        (
            "WITH t AS (SELECT 99 AS id) SELECT id FROM t UNION ALL SELECT 2",
            s(&["2", "99"]),
        ),
    ] {
        assert_eq!(sorted(&mut a, sql), want, "{sql}");
    }
}

/// DELETE ... USING reads its target and its USING items in ONE namespace, as PostgreSQL does: an
/// unqualified name both have is ambiguous (42702) and nothing is deleted, and the target named
/// again in USING is 42712 ("table name specified more than once"). Translated as EXISTS over the
/// USING items, a bare name bound to the USING side alone (the innermost scope), so `id = 2`
/// became uncorrelated and deleted EVERY target row (wire review 11 item 4, a regression from
/// 911392515). An aliased target is read by its alias.
#[test]
fn delete_using_reads_one_namespace() {
    let dir = Scratch::new("deleteusingns");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .ok("d rows");
    a.q("CREATE TABLE k(id INT, flag BOOLEAN)").ok("k");
    a.q("INSERT INTO k VALUES (2, true), (3, false)")
        .ok("k rows");
    let count = |a: &mut Wire| a.q("SELECT count(*) FROM d").single("count");
    let r = a.q("DELETE FROM d USING k WHERE id = 2");
    assert_eq!(r.err("an unqualified name both have").code, "42702");
    assert_eq!(count(&mut a), "3", "nothing deleted");
    let r = a.q("DELETE FROM d USING d WHERE d.id = 1");
    assert_eq!(r.err("the target again in USING").code, "42712");
    assert_eq!(count(&mut a), "3", "nothing deleted");
    let r = a
        .q("DELETE FROM d AS x USING k WHERE x.id = k.id AND k.flag RETURNING x.v")
        .ok("an aliased target");
    assert_eq!(r.rows, vec![vec![Some("b".to_string())]]);
    let r = a
        .q("DELETE FROM d USING k AS j WHERE v = 'c' AND j.id = 3")
        .ok("a name only the target has");
    assert_eq!(r.tags, vec!["DELETE 1".to_string()]);
    assert_eq!(a.q("SELECT id FROM d").single("one row left"), "1");
}

/// DELETE ... USING deletes only the rows its join condition matches, as in PostgreSQL. The USING
/// clause was dropped, so the WHERE ran against the target alone: every row whose columns made it
/// true was deleted, and a WHERE over the USING table's columns failed or deleted everything (wire
/// review 7 item 18). Expected values: PostgreSQL's by these fixtures' semantics.
#[test]
fn delete_using_deletes_only_the_joined_rows() {
    let dir = Scratch::new("deleteusing");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .ok("d rows");
    a.q("CREATE TABLE k(id INT, flag BOOLEAN)").ok("k");
    a.q("INSERT INTO k VALUES (2, true), (3, false)")
        .ok("k rows");
    let left = |a: &mut Wire| -> Vec<String> {
        a.q("SELECT id FROM d ORDER BY id")
            .ok("d")
            .rows
            .iter()
            .map(|r| r[0].clone().unwrap())
            .collect()
    };
    let r = a
        .q("DELETE FROM d USING k WHERE d.id = k.id AND k.flag")
        .ok("delete using");
    assert_eq!(r.tags, vec!["DELETE 1".to_string()]);
    assert_eq!(left(&mut a), vec!["1", "3", "4"]);
    let r = a
        .q("DELETE FROM d USING k WHERE d.id = k.id RETURNING d.v")
        .ok("delete using, returning");
    assert_eq!(r.rows, vec![vec![Some("c".to_string())]]);
    assert_eq!(left(&mut a), vec!["1", "4"]);
    a.q("DELETE FROM k").ok("empty k");
    a.q("DELETE FROM d USING k")
        .ok("delete using an empty table");
    assert_eq!(left(&mut a), vec!["1", "4"], "nothing joins an empty table");
}

/// ALTER TABLE ADD CONSTRAINT's rebuild leaves the deferred foreign keys' pending count as it found
/// it. Its copy-back ran with foreign keys enforced, so it counted rows again: (i) a block's
/// deferred orphan was cancelled by a valid child the copy re-inserted, and COMMIT kept the
/// orphan; (ii) an orphan made from the parent's side was counted twice, so fixing it did not let
/// COMMIT through; (iii) a rebuild whose own COMMIT failed on such a count left the transaction
/// open ('T', holding the trunk's write lock). (iv) An added foreign key is still checked against
/// the rows: 23503 at the ALTER, nothing changed (wire review 6 item 1).
#[test]
fn an_alter_rebuild_keeps_the_deferred_foreign_key_count() {
    let dir = Scratch::new("rebuilddefer");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let fresh = |a: &mut Wire| {
        for sql in [
            "DROP TABLE IF EXISTS c",
            "DROP TABLE IF EXISTS p",
            "CREATE TABLE p(id INT PRIMARY KEY, v INT)",
            "CREATE TABLE c(id INT PRIMARY KEY, pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED, w INT)",
            "INSERT INTO p VALUES (1, 0)",
            "INSERT INTO c VALUES (1, 1, 0)",
        ] {
            a.q(sql).ok(sql);
        }
    };
    // (i) A block's orphan survives an ALTER of the parent: COMMIT fails 23503, nothing kept.
    fresh(&mut a);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO c VALUES (2, 99, 0)")
        .ok("a deferred orphan");
    a.q("ALTER TABLE p ADD UNIQUE (v)").ok("alter the parent");
    let r = a.q("COMMIT");
    assert_eq!(r.err("(i) commit with an orphan").code, "23503");
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM c WHERE id = 2").single("(i)"),
        "0"
    );
    // (ii) An orphan made from the parent's side, fixed before COMMIT, commits.
    fresh(&mut a);
    a.q("BEGIN").ok("begin");
    a.q("DELETE FROM p WHERE id = 1").ok("orphan the child");
    a.q("ALTER TABLE c ADD UNIQUE (w)").ok("alter the child");
    a.q("INSERT INTO p VALUES (1, 0)").ok("the parent back");
    a.q("COMMIT").ok("(ii) the block commits");
    // (iii) An orphan the database already holds (made with foreign keys off): the rebuild of its
    // table commits and the session is idle.
    fresh(&mut a);
    a.q("SET foreign_keys = off").ok("premise: keys off");
    a.q("INSERT INTO c VALUES (3, 98, 0)").ok("an orphan");
    a.q("SET foreign_keys = on").ok("keys on");
    let r = a.q("ALTER TABLE c ADD UNIQUE (w)");
    assert_eq!(
        r.status, b'I',
        "(iii) the rebuild left a block open: {:?}",
        r.error
    );
    // (iv) An added foreign key over an orphan: 23503 at the ALTER, the table unchanged.
    a.q("DROP TABLE IF EXISTS c2").ok("drop c2");
    a.q("CREATE TABLE c2(id INT PRIMARY KEY, pid INT)").ok("c2");
    a.q("INSERT INTO c2 VALUES (1, 1), (2, 97)")
        .ok("c2 rows, one orphan");
    let r = a.q("ALTER TABLE c2 ADD FOREIGN KEY (pid) REFERENCES p (id)");
    assert_eq!(r.err("(iv) an added key over an orphan").code, "23503");
    assert_eq!(r.status, b'I');
    assert_eq!(a.q("SELECT count(*) FROM c2").single("(iv)"), "2");
    a.q("INSERT INTO c2 VALUES (3, 96)")
        .ok("(iv) no key was added");
}

/// The ADD CONSTRAINT rebuild's aside table keeps a serial column's values whatever its spelling:
/// SMALLSERIAL and BIGSERIAL are integers there, as SERIAL is. smallserial became the engine's
/// smallint, whose encoding refuses anything past 32767, so a table holding 40000 could no longer
/// take any constraint; bigserial became bigint, no longer the table's rowid key (wire review 6
/// item 2).
#[test]
fn an_alter_rebuild_keeps_every_serial_spellings_values() {
    let dir = Scratch::new("serialspell");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for ty in ["SMALLSERIAL", "SERIAL2", "BIGSERIAL", "SERIAL8"] {
        let t = format!("s_{}", ty.to_lowercase());
        a.q(&format!("CREATE TABLE {t}(id {ty}, v INT)"))
            .ok("table");
        a.q(&format!("INSERT INTO {t} VALUES (40000, 1), (5, 2)"))
            .ok("a value past smallint");
        a.q(&format!("ALTER TABLE {t} ADD PRIMARY KEY (id)"))
            .ok(&format!("{ty}: add a key"));
        assert_eq!(
            a.q(&format!("SELECT count(*) FROM {t} WHERE id = 40000"))
                .single("row"),
            "1",
            "{ty}"
        );
        let e = a
            .q(&format!("INSERT INTO {t} VALUES (40000, 3)"))
            .err("dup");
        assert_eq!(e.code, "23505", "{ty}: the key is enforced");
    }
}

/// A reconnect to a branch right after its session closed is not refused: the closed session's
/// server thread may not have released the branch yet, and the new claim waits for that release
/// as a delete already does. claim refused a held name at once, so `close; connect db/x` met FATAL
/// 55006 whenever the old thread was behind (wire review 6 item 5, review 5 item 6).
#[test]
fn a_reconnect_right_after_a_close_is_not_refused() {
    let dir = Scratch::new("reconnect");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('x')").ok("create");
    let mut refused = Vec::new();
    for round in 0..100 {
        let mut c = match server.connect_to("postgres/x") {
            Ok(c) => c,
            Err(e) => {
                refused.push((round, e));
                continue;
            }
        };
        // Terminate and close without waiting for the server's EOF.
        c.send(b'X', &[]);
        drop(c);
    }
    assert!(
        refused.is_empty(),
        "{} of 100 reconnects refused: {refused:?}",
        refused.len()
    );
}

/// SAVEPOINT, RELEASE and ROLLBACK TO in an IMPLICIT block (a multi-statement query, or a pipeline
/// before its Sync) are 25P01, as PostgreSQL refuses them there: the block is rolled back, the
/// session is idle, and nothing is kept or held. They ran, and ended the implicit block's
/// bookkeeping while the engine's transaction stayed open with nobody to commit it: every statement
/// answered success, ReadyForQuery said 'T', and the rows the client was told were written went
/// with the connection (wire review 11 item 6, a regression from e8d402e6e for SAVEPOINT and
/// RELEASE).
#[test]
fn a_savepoint_verb_in_an_implicit_block_is_25p01() {
    let dir = Scratch::new("implicitsavepoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let ids = |a: &mut Wire| -> Vec<String> {
        a.q("SELECT id FROM t ORDER BY id")
            .ok("ids")
            .rows
            .iter()
            .map(|r| r[0].clone().unwrap())
            .collect()
    };
    for verb in ["SAVEPOINT s", "RELEASE s", "ROLLBACK TO s"] {
        let sql = format!("INSERT INTO t VALUES (2, 'x'); {verb}; INSERT INTO t VALUES (3, 'y')");
        let r = a.q(&sql);
        assert_eq!(r.err(&sql).code, "25P01", "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
        assert_eq!(ids(&mut a), vec!["1"], "{sql}: nothing kept");
        let r = a.pipeline(&[
            "INSERT INTO t VALUES (2, 'x')",
            verb,
            "INSERT INTO t VALUES (3, 'y')",
        ]);
        assert_eq!(r.err(verb).code, "25P01", "pipeline {verb}");
        assert_eq!(r.status, b'I', "pipeline {verb}");
        assert_eq!(ids(&mut a), vec!["1"], "pipeline {verb}: nothing kept");
        // Nothing is held: another session writes at once.
        let mut b = server.connect();
        b.q("INSERT INTO t VALUES (9, 'z')")
            .ok("another session writes");
        b.q("DELETE FROM t WHERE id = 9").ok("undo");
    }
}

/// A savepoint that does not exist is 3B001 wherever it is named, and ROLLBACK TO or RELEASE
/// outside a block is 25P01, as in PostgreSQL. Only an engine-dropped failed block answered 3B001;
/// a mistyped ROLLBACK TO in a live block, RELEASE of an unknown name and either verb in autocommit
/// reached the engine's "no such savepoint" as XX000 (wire review 6 item 6, review 3 item 24).
#[test]
fn a_missing_savepoint_is_3b001_and_outside_a_block_25p01() {
    let dir = Scratch::new("savepoints");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("SAVEPOINT s").ok("savepoint");
    let r = a.q("ROLLBACK TO b");
    assert_eq!(
        r.err("rollback to a savepoint that does not exist").code,
        "3B001"
    );
    assert_eq!(r.status, b'E', "the block is not failed");
    a.q("ROLLBACK").ok("end the block");
    a.q("BEGIN").ok("begin");
    let r = a.q("RELEASE nosuch");
    assert_eq!(
        r.err("release a savepoint that does not exist").code,
        "3B001"
    );
    a.q("ROLLBACK").ok("end the block");
    for sql in ["ROLLBACK TO s", "RELEASE s", "RELEASE SAVEPOINT s"] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "25P01", "{sql} outside a block");
        assert_eq!(r.status, b'I', "{sql} opened a block");
    }
}

/// A transaction verb is read by its whole grammar: `COMMIT garbage`, `END x`, `ROLLBACK foo`,
/// `ABORT x` and `BEGIN garbage` are syntax errors (42601) as in PostgreSQL, wherever they are sent:
/// they fail a block, and outside one change nothing. The classifier read at most two words, so
/// they were answered as the verb (a success tag, or the end of a failed block). And a verb behind
/// a comment is still the verb: `/* c */ ROLLBACK` ends a failed block (wire review 6 item 4).
#[test]
fn a_transaction_verb_is_read_by_its_whole_grammar() {
    let dir = Scratch::new("txgrammar");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for bad in [
        "COMMIT garbage",
        "END x",
        "ROLLBACK foo",
        "ABORT x",
        "BEGIN garbage",
    ] {
        // Outside a block: a syntax error, the session idle.
        let r = a.q(bad);
        assert_eq!(r.err(bad).code, "42601", "{bad} outside a block");
        assert_eq!(r.status, b'I', "{bad} outside a block");
        // Inside one: a syntax error that fails it; its end answers ROLLBACK.
        a.q("BEGIN").ok("begin");
        a.q("INSERT INTO t VALUES (2, 'two')").ok("insert");
        let r = a.q(bad);
        assert_eq!(r.err(bad).code, "42601", "{bad} inside a block");
        assert_eq!(r.status, b'E', "{bad} did not fail the block");
        let r = a.q("COMMIT").ok("end");
        assert_eq!(r.tags, vec!["ROLLBACK".to_string()], "after {bad}");
        // In a failed block: still a syntax error, the block still failed.
        a.q("BEGIN").ok("begin");
        a.q("SELECT 1/0").err("fail the block");
        let r = a.q(bad);
        assert_eq!(r.status, b'E', "{bad} ended a failed block");
        a.q("ROLLBACK").ok("end");
    }
    // Good spellings still work.
    for good in [
        "BEGIN TRANSACTION",
        "COMMIT WORK",
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "ROLLBACK AND NO CHAIN",
    ] {
        a.q(good).ok(good);
    }
    // A verb behind a comment ends a failed block.
    a.q("BEGIN").ok("begin");
    a.q("SELECT 1/0").err("fail the block");
    let r = a.q("/* c */ ROLLBACK").ok("commented ROLLBACK");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
}

/// A transaction verb with a comment before, inside or after it is that verb (PostgreSQL's lexer
/// reads a comment as whitespace): `ROLLBACK -- why` ends a failed block, `/* c */ BEGIN` in a
/// block is BEGIN's warning (25001 "there is already a transaction in progress") with the block
/// still open, and `COMMIT /* c */` outside one is COMMIT's (25P01 "there is no transaction in
/// progress"). A comment after the verb made it Other: refused 25P02 in a failed block, and a
/// BEGIN in a block reached the engine, which failed the block (wire review 9 item 7). PostgreSQL
/// warns for every such BEGIN, COMMIT and ROLLBACK; the server answered their tags with no warning.
#[test]
fn a_commented_transaction_verb_is_its_verb() {
    let dir = Scratch::new("txcomments");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for rollback in ["ROLLBACK -- why", "/* c */ ROLLBACK", "ROLLBACK/**/;"] {
        a.q("BEGIN").ok("begin");
        a.q("SELECT 1/0").err("fail the block");
        let r = a.q(rollback).ok(rollback);
        assert_eq!(r.tags, vec!["ROLLBACK".to_string()], "{rollback}");
        assert_eq!(r.status, b'I', "{rollback}");
    }
    for begin in ["/* c */ BEGIN", "BEGIN -- again", "BEGIN"] {
        a.q("BEGIN").ok("begin");
        a.q("INSERT INTO t VALUES (2, 'two')").ok("insert");
        let r = a.q(begin).ok(begin);
        assert_eq!(r.tags, vec!["BEGIN".to_string()], "{begin}");
        assert_eq!(r.status, b'T', "{begin}: the block is still open");
        let codes: Vec<&str> = r.notices.iter().map(|n| n.code.as_str()).collect();
        assert_eq!(codes, vec!["25001"], "{begin}: {:?}", r.notices);
        a.q("ROLLBACK").ok("end");
    }
    for end in ["COMMIT /* c */", "-- c\nROLLBACK", "COMMIT"] {
        let r = a.q(end).ok(end);
        assert_eq!(r.status, b'I', "{end}");
        let codes: Vec<&str> = r.notices.iter().map(|n| n.code.as_str()).collect();
        assert_eq!(codes, vec!["25P01"], "{end}: {:?}", r.notices);
    }
}
