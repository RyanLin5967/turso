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
            if let Some(server) = Self::start_on(db, extra, port) {
                return server;
            }
        }
        panic!("tursopg server did not start");
    }

    /// One attempt on `port`: the server, once it is ready, or None when it is not (it exited, or
    /// never became ready), the child reaped. Ready means the server's own stdout line "PostgreSQL
    /// server listening on ADDR", printed after its bind: a connect probe was answered by any
    /// listener on the port, so a server that lost the bind race was handed out while it exited
    /// (the startup flake). Its stdout and stderr go to `server_log_dir()`, one file each per
    /// server.
    fn start_on(db: &Path, extra: &[&str], port: u16) -> Option<Self> {
        use std::io::BufRead;
        let addr = format!("127.0.0.1:{port}");
        let logs = server_log_dir();
        let stderr = std::fs::File::create(logs.join(format!("server-{port}.stderr")))
            .expect("server stderr log");
        let mut stdout_log = std::fs::File::create(logs.join(format!("server-{port}.stdout")))
            .expect("server stdout log");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_tursopg"));
        cmd.arg(db)
            .arg("--server")
            .arg(&addr)
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr));
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
        // The reader copies every stdout line to the log until the server exits, and reports the
        // listening line once; a server that never prints it, or exits first, is not ready.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let listening = format!("PostgreSQL server listening on {addr} ");
        let stdout = child.stdout.take().expect("piped stdout");
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let _ = writeln!(stdout_log, "{line}");
                if line.starts_with(&listening) {
                    let _ = ready_tx.send(());
                }
            }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match ready_rx.recv_timeout(Duration::from_millis(50)) {
                Ok(()) => return Some(Self { child, port }),
                // The reader ended: stdout closed, so the server is exiting.
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if child.try_wait().unwrap().is_some() {
                break;
            }
        }
        child.kill().ok();
        child.wait().ok();
        None
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

/// Where each server's stdout and stderr logs go: WIRE_SERVER_LOG_DIR, or a directory per test
/// process under the temp directory. Kept after the test (a Scratch is removed on drop, a failing
/// test's included), so a failure's server side can be read.
fn server_log_dir() -> PathBuf {
    let dir = std::env::var_os("WIRE_SERVER_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join("tursopg_wire_server_logs")
                .join(std::process::id().to_string())
        });
    std::fs::create_dir_all(&dir).expect("server log dir");
    dir
}

impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// The harness takes a server as ready only from the server itself, never from whatever accepts a
/// connection on its port: a listener another process holds there must not pass for it. The
/// connect probe did take one, so a server that lost the bind race (and exits) was handed out, and
/// its sessions met the other listener: "closed the connection during startup" (the startup flake,
/// raws artie lanes/wire/raw/loud-20261015/startup-flake/; lead: a real race, fix accepted
/// 2026-10-15 as test infrastructure that changes no assertion).
#[test]
fn a_foreign_listener_on_the_port_is_not_taken_for_the_server() {
    let dir = Scratch::new("foreignlistener");
    let foreign = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = foreign.local_addr().unwrap().port();
    let server = Server::start_on(&dir.db(), &[], port);
    assert!(
        server.is_none(),
        "a server that could not bind {port} was taken as ready"
    );
    drop(foreign);
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
    /// The column names of the last RowDescription, if one came.
    names: Option<Vec<String>>,
    /// The BackendKeyData's body, if one came.
    key: Option<Vec<u8>>,
    tags: Vec<String>,
    error: Option<WireError>,
    /// The parameter type OIDs of the last ParameterDescription, if one came.
    params: Option<Vec<u32>>,
    /// Every NoticeResponse of the reply, in order.
    notices: Vec<WireError>,
    /// Every ParameterStatus of the reply, name and value, in order.
    parameters: Vec<(String, String)>,
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
    /// The ParameterStatus values the server announced at startup, in order.
    parameters: Vec<(String, String)>,
    /// The BackendKeyData's body (process id and secret key), for a CancelRequest.
    key: Option<Vec<u8>>,
}

impl Wire {
    fn connect(port: u16, database: &str) -> Result<Self, WireError> {
        let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s.set_nodelay(true).unwrap();
        let mut w = Self {
            s,
            parameters: Vec::new(),
            key: None,
        };
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
            None => {
                w.parameters = r.parameters;
                w.key = r.key;
                Ok(w)
            }
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

    /// Parse (no declared types), Bind (the given parameter codes, values and result codes),
    /// Execute, Sync; then every message type byte up to ReadyForQuery, and the first error. The
    /// type bytes show whether BindComplete ('2') came before a refusal (wire review 17 item 20:
    /// one helper for the nested copies of it).
    fn bind_round(
        &mut self,
        sql: &str,
        pcodes: &[i16],
        values: &[Option<&[u8]>],
        rcodes: &[i16],
    ) -> (Vec<u8>, Option<WireError>) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        self.send(b'P', &parse);
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
        self.send(b'B', &bind);
        self.send(b'E', &[0, 0, 0, 0, 0]);
        self.send(b'S', &[]);
        let (mut types, mut error) = (Vec::new(), None);
        loop {
            let mut head = [0u8; 5];
            self.s.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            self.s.read_exact(&mut body).unwrap();
            types.push(head[0]);
            if head[0] == b'E' && error.is_none() {
                error = Some(error_fields(&body));
            }
            if head[0] == b'Z' {
                return (types, error);
            }
        }
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

    /// [`Wire::x`] with NO declared parameter types (the Parse lists none) and `values` bound as
    /// text, no Describe: what a client that lets the server infer every type sends.
    fn x_undeclared(&mut self, sql: &str, values: &[&str]) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        self.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
            bind.extend_from_slice(v.as_bytes());
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'B', &bind);
        let mut exec = vec![0u8];
        exec.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &exec);
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
                    let mut names = Vec::with_capacity(n);
                    for _ in 0..n {
                        let end = p + body[p..].iter().position(|&c| c == 0).unwrap();
                        names.push(String::from_utf8_lossy(&body[p..end]).into_owned());
                        p = end + 1;
                        oids.push(u32::from_be_bytes([
                            body[p + 6],
                            body[p + 7],
                            body[p + 8],
                            body[p + 9],
                        ]));
                        p += 18;
                    }
                    r.oids = Some(oids);
                    r.names = Some(names);
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
                b'K' => r.key = Some(body.clone()),
                b'S' => {
                    let name = cstr(&body);
                    let value = cstr(&body[name.len() + 1..]);
                    r.parameters.push((name, value));
                }
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

/// Run `script` through the non-server CLI on `dir`'s database, as a test makes a schema before
/// the server starts (the server refuses CREATE SCHEMA, and attaches a schema file present when a
/// session opens).
fn cli_script(dir: &Scratch, script: &[u8]) {
    let mut cli = Command::new(env!("CARGO_BIN_EXE_tursopg"))
        .arg(dir.db())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the CLI");
    cli.stdin.take().unwrap().write_all(script).unwrap();
    assert!(
        cli.wait().unwrap().success(),
        "premise: the CLI ran the script"
    );
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

/// Wait up to `limit` for the server's threads to fall back to `want` or fewer; the last count.
fn threads_settle(server: &Server, want: usize, limit: Duration) -> usize {
    let deadline = Instant::now() + limit;
    let mut now = thread_count(server.pid());
    while now > want && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        now = thread_count(server.pid());
    }
    now
}

/// A socket cannot hold a session thread once its client is gone or never starts: a connection
/// that sends nothing ends at the authentication timeout (here 1 s), one whose client half-closes
/// it ends at the stream's end, and one reset by its client ends at the reset. Each thread goes
/// (L5 step 3(a), lead 2026-10-15: the session's blocking socket must not leak a thread).
#[test]
fn an_idle_or_half_closed_socket_does_not_keep_its_session_thread() {
    let dir = Scratch::new("socketleak");
    let server = Server::start(&dir.db(), &["--authentication-timeout", "1"]);
    let mut first = server.connect();
    first.q("SELECT 1").ok("select 1");
    let before = thread_count(server.pid());
    // Never starts: no byte is sent.
    let mut silent = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    // Half-closed after its startup.
    let half = server.connect();
    // Reset after its startup: SO_LINGER 0, so the close sends RST.
    let reset = server.connect();
    let during = thread_count(server.pid());
    assert!(during >= before + 3, "premise: 3 more sessions, threads {before} -> {during}");
    half.s.shutdown(std::net::Shutdown::Write).unwrap();
    unsafe {
        use std::os::fd::AsRawFd;
        let l = libc::linger { l_onoff: 1, l_linger: 0 };
        assert_eq!(
            libc::setsockopt(
                reset.s.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                &l as *const libc::linger as *const libc::c_void,
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            ),
            0
        );
    }
    drop(reset);
    let after = threads_settle(&server, before, Duration::from_secs(10));
    assert!(
        after <= before,
        "3 sockets gone or never started, but threads went {before} -> {during} -> {after}"
    );
    // The silent connection was ended by the server: its read sees the stream's end.
    silent.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut b = [0u8; 1];
    assert_eq!(silent.read(&mut b).unwrap_or(1), 0, "the silent socket was not closed");
    drop(half);
    first.q("SELECT 1").ok("the first session is unaffected");
}

/// The authentication timeout bounds the whole startup, not each read: a client that sends its
/// startup packet one byte at a time, each byte well inside the timeout (here 1 s), is still ended
/// once the timeout has passed since it connected. Bounding each read alone let such a client hold
/// a session thread and its max_connections slot for as long as it kept trickling (ultrareview of
/// fastest-wire, finding 4).
#[test]
fn a_trickled_startup_ends_at_the_authentication_timeout() {
    let dir = Scratch::new("startuptrickle");
    let server = Server::start(&dir.db(), &["--authentication-timeout", "1"]);
    let mut slow = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    slow.set_read_timeout(Some(Duration::from_millis(1)))
        .unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(&1024i32.to_be_bytes());
    startup.extend_from_slice(&196608i32.to_be_bytes());
    startup.resize(1024, b'u');
    let connected = Instant::now();
    let mut closed_after = None;
    for byte in &startup {
        if slow.write_all(std::slice::from_ref(byte)).is_err() {
            closed_after = Some(connected.elapsed());
            break;
        }
        let mut b = [0u8; 64];
        let ended = match slow.read(&mut b) {
            Ok(n) => n == 0,
            Err(e) => !matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
        };
        if ended {
            closed_after = Some(connected.elapsed());
            break;
        }
        if connected.elapsed() > Duration::from_secs(6) {
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    let closed_after = closed_after.expect("a startup trickled for 6 s was never ended");
    assert!(
        closed_after < Duration::from_secs(3),
        "the trickled startup was ended only after {closed_after:?}"
    );
}

/// A Parse that declares no types and the statement's parameters counted as PostgreSQL 18.6 counts
/// them (recorded, pg18-diff/pg18-c26.out, c26.py; Claude C26 MED 1): an empty statement or a comment
/// before the SELECT, and an E-string continued over a newline (`$2` inside it is text, not a
/// parameter), each take ONE value; two values for the E-string statement are refused 08P01 with
/// PostgreSQL's words. 745585f27's count began at the first token (`;`) and read the continuation
/// as a standard string, so one value was refused.
#[test]
fn a_bind_counts_the_parameters_postgresql_counts_after_an_empty_prefix_and_in_an_e_string() {
    let dir = Scratch::new("bindcount");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (sql, want) in [
        (";SELECT $1::int", "41"),
        (" ; /* c */ ; SELECT $1::int", "41"),
        ("SELECT E'one'\n'\\'$2' || $1::text", "one'$241"),
    ] {
        let r = a.x_undeclared(sql, &["41"]).ok(sql);
        assert_eq!(r.rows, [[Some(want.to_string())]], "{sql:?}");
    }
    let sql = "SELECT E'one'\n'\\'$2' || $1::text";
    let e = a.x_undeclared(sql, &["41", "2"]).err(sql);
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        (
            "08P01",
            "bind message supplies 2 parameters, but prepared statement \"\" requires 1"
        )
    );
}

/// lock_timeout and client_min_messages follow the transaction as PostgreSQL 18.6's do (recorded,
/// pg18-diff/pg18-c26.out; Claude C26 MED 3): a SET inside a block that rolls back, and SET LOCAL in
/// a block that commits, leave the session's value; a SET in a block that commits keeps it. Ours
/// kept every one for the session (later warnings dropped, later lock waits failing early).
#[test]
fn lock_timeout_and_client_min_messages_follow_the_transaction() {
    let dir = Scratch::new("settingscope");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let show = |a: &mut Wire, name: &str| a.q(&format!("SHOW {name}")).ok("SHOW").rows[0][0].clone();
    for (script, name, want) in [
        (["BEGIN", "SET client_min_messages = error", "ROLLBACK"], "client_min_messages", "notice"),
        (["BEGIN", "SET LOCAL client_min_messages = error", "COMMIT"], "client_min_messages", "notice"),
        (["BEGIN", "SET LOCAL lock_timeout = '100ms'", "COMMIT"], "lock_timeout", "0"),
        (["BEGIN", "SET lock_timeout = '100ms'", "ROLLBACK"], "lock_timeout", "0"),
        (["BEGIN", "SET lock_timeout = '100ms'", "COMMIT"], "lock_timeout", "100ms"),
        (["BEGIN", "SET client_min_messages = error", "COMMIT"], "client_min_messages", "error"),
    ] {
        for sql in script {
            a.q(sql).ok(sql);
        }
        assert_eq!(show(&mut a, name).as_deref(), Some(want), "{script:?}");
        a.q("SET lock_timeout = 0").ok("reset lock_timeout");
        a.q("SET client_min_messages = notice").ok("reset client_min_messages");
    }
}

/// The three settings this server acts on (lock_timeout, statement_timeout, client_min_messages)
/// follow PostgreSQL 18.6's rules case for case (recorded, pg18-diff/pg18-settings.out, every case
/// replayed): SHOW's text with PostgreSQL's units; SET LOCAL outside a block warns 25P01 and changes
/// nothing; ROLLBACK TO a savepoint undoes the SETs after it and RELEASE keeps them; SET after SET
/// LOCAL replaces both, and SET LOCAL after SET lasts until the block ends; a failed implicit
/// block's SET is undone (the C26 MED 3 class).
#[test]
fn the_acted_on_settings_show_and_scope_as_postgresql_does() {
    let dir = Scratch::new("settingrules");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let show = |a: &mut Wire, name: &str| {
        a.q(&format!("SHOW {name}")).ok("SHOW").rows[0][0].clone().unwrap_or_default()
    };
    for (set, want) in [
        ("SET lock_timeout = '1000ms'", "1s"),
        ("SET lock_timeout = 90000", "90s"),
        ("SET lock_timeout = 60000", "1min"),
        ("SET lock_timeout = '1500ms'", "1500ms"),
        ("SET lock_timeout = '2h'", "2h"),
        ("SET lock_timeout = 0", "0"),
    ] {
        a.q(set).ok(set);
        assert_eq!(show(&mut a, "lock_timeout"), want, "{set}");
    }
    a.q("SET statement_timeout = '100ms'").ok("statement_timeout");
    assert_eq!(show(&mut a, "statement_timeout"), "100ms");
    a.q("SET statement_timeout = 0").ok("statement_timeout 0");
    assert_eq!(show(&mut a, "statement_timeout"), "0");
    a.q("SET client_min_messages = WARNING").ok("client_min_messages");
    assert_eq!(show(&mut a, "client_min_messages"), "warning");
    a.q("SET client_min_messages = notice").ok("client_min_messages back");
    let r = a.q("SET LOCAL lock_timeout = '1s'").ok("SET LOCAL outside a block");
    assert_eq!(
        r.notices.iter().map(|n| (n.code.as_str(), n.message.as_str())).collect::<Vec<_>>(),
        [("25P01", "SET LOCAL can only be used in transaction blocks")],
        "the warning"
    );
    assert_eq!(show(&mut a, "lock_timeout"), "0");
    for sql in ["BEGIN", "SET lock_timeout = '1s'", "SAVEPOINT a", "SET lock_timeout = '2s'", "ROLLBACK TO a"] {
        a.q(sql).ok(sql);
    }
    assert_eq!(show(&mut a, "lock_timeout"), "1s", "after ROLLBACK TO a");
    for sql in ["SAVEPOINT b", "SET lock_timeout = '3s'", "RELEASE b"] {
        a.q(sql).ok(sql);
    }
    assert_eq!(show(&mut a, "lock_timeout"), "3s", "after RELEASE b");
    a.q("COMMIT").ok("COMMIT");
    assert_eq!(show(&mut a, "lock_timeout"), "3s", "after COMMIT");
    a.q("SET lock_timeout = 0").ok("reset");
    for sql in ["BEGIN", "SET LOCAL lock_timeout = '1s'", "SET lock_timeout = '2s'"] {
        a.q(sql).ok(sql);
    }
    assert_eq!(show(&mut a, "lock_timeout"), "2s", "SET after SET LOCAL");
    a.q("COMMIT").ok("COMMIT");
    assert_eq!(show(&mut a, "lock_timeout"), "2s", "after COMMIT");
    a.q("SET lock_timeout = 0").ok("reset");
    for sql in ["BEGIN", "SET lock_timeout = '2s'", "SET LOCAL lock_timeout = '1s'"] {
        a.q(sql).ok(sql);
    }
    assert_eq!(show(&mut a, "lock_timeout"), "1s", "SET LOCAL after SET");
    a.q("COMMIT").ok("COMMIT");
    assert_eq!(show(&mut a, "lock_timeout"), "2s", "after COMMIT");
    a.q("SET lock_timeout = 0").ok("reset");
    // The recording's 1/0 case is not replayed here: this engine answers 1/0 with NULL (the known
    // red integer_division_by_zero_is_22012); its missing-table twin is the same recorded rule.
    let e = a
        .q("SET lock_timeout = '5s'; SELECT * FROM no_such_table")
        .err("a failed implicit block");
    assert_eq!(e.code, "42P01");
    assert_eq!(show(&mut a, "lock_timeout"), "0", "the failed implicit block's SET");
}

/// A branch call waiting for another session to leave its branch (the in-use wait, up to 5 s) is
/// the session's statement: statement_timeout ends it with 57014, and so does a CancelRequest
/// (Claude C37 M1: both got 55006 after the whole 5 s, scripts/inuse.py; PostgreSQL's equivalent
/// wait, CountOtherDBBackends, checks for interrupts; recorded cancel and timeout answers,
/// pg18-cancel.out: 57014 with "canceling statement due to statement timeout" and "... due to user
/// request").
#[test]
fn the_branch_in_use_wait_stops_on_statement_timeout_and_on_cancel() {
    let dir = Scratch::new("inusewait");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT)").ok("create");
    a.q("SELECT turso_branch_create('x')").ok("create x");
    a.q("SELECT turso_branch_switch('x')").ok("A on x");
    let mut b = server.connect();
    b.q("SET statement_timeout = 200").ok("statement_timeout");
    let started = Instant::now();
    let e = b.q("SELECT turso_branch_switch('x')").err("B's switch while A is on x");
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("57014", "canceling statement due to statement timeout"),
        "after {:?}",
        started.elapsed()
    );
    assert!(started.elapsed() < Duration::from_secs(2), "the timeout took {:?}", started.elapsed());
    b.q("SET statement_timeout = 0").ok("statement_timeout off");
    let key = b.key.clone().expect("BackendKeyData");
    let sql = "SELECT turso_branch_switch('x')";
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
    q.extend_from_slice(sql.as_bytes());
    q.push(0);
    let started = Instant::now();
    b.s.write_all(&q).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mut cancel = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    let mut m = ((8 + key.len()) as i32).to_be_bytes().to_vec();
    m.extend_from_slice(&80877102i32.to_be_bytes());
    m.extend_from_slice(&key);
    cancel.write_all(&m).unwrap();
    drop(cancel);
    let r = b.read_reply();
    let e = r.error.expect("the cancelled switch's error");
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("57014", "canceling statement due to user request"),
        "after {:?}",
        started.elapsed()
    );
    assert!(started.elapsed() < Duration::from_secs(3), "the cancel took {:?}", started.elapsed());
    b.q("SELECT 1").ok("B after the cancel");
}

/// A failed domain CHECK answers 23514 with PostgreSQL's message and constraint name on every path,
/// case for case as PostgreSQL 18.6 recorded them (artie lanes/engine/raw/pg18-domain-check-message.txt,
/// 0673f158a3, every recorded statement replayed in order): CAST, a column's INSERT and UPDATE, a
/// two-CHECK domain (each CHECK its own name), and a named CONSTRAINT. The SQLSTATE comes from the
/// engine's error CODE, never the wording: mapped by the message's prefix, the engine's corrected
/// PG-exact message (fastest-engine 814c05755) read as 23000 (the lead's class order, 2026-10-15).
#[test]
fn a_domain_check_failure_is_23514_with_postgresqls_message_on_every_path() {
    let dir = Scratch::new("domaincheck");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let fails = |a: &mut Wire, sql: &str, message: &str| {
        let e = a.q(sql).err(sql);
        assert_eq!((e.code.as_str(), e.message.as_str()), ("23514", message), "{sql}");
    };
    a.q("CREATE DOMAIN dm AS integer CHECK (VALUE > 0)").ok("CREATE DOMAIN dm");
    fails(&mut a, "SELECT CAST(-5 AS dm)", "value for domain dm violates check constraint \"dm_check\"");
    a.q("CREATE TABLE tm(id integer PRIMARY KEY, x dm)").ok("CREATE TABLE tm");
    fails(&mut a, "INSERT INTO tm VALUES (1, -5)", "value for domain dm violates check constraint \"dm_check\"");
    a.q("CREATE DOMAIN d2 AS integer CHECK (VALUE > 0) CHECK (VALUE < 100)").ok("CREATE DOMAIN d2");
    fails(&mut a, "SELECT CAST(500 AS d2)", "value for domain d2 violates check constraint \"d2_check1\"");
    fails(&mut a, "SELECT CAST(-1 AS d2)", "value for domain d2 violates check constraint \"d2_check\"");
    a.q("CREATE DOMAIN dn AS integer CONSTRAINT positive CHECK (VALUE > 0)").ok("CREATE DOMAIN dn");
    fails(&mut a, "SELECT CAST(-5 AS dn)", "value for domain dn violates check constraint \"positive\"");
    a.q("CREATE TABLE tn(id integer PRIMARY KEY, x dn)").ok("CREATE TABLE tn");
    fails(&mut a, "INSERT INTO tn VALUES (1, -5)", "value for domain dn violates check constraint \"positive\"");
    assert_eq!(a.q("UPDATE tm SET x = -1").ok("UPDATE of no row").tags, ["UPDATE 0"]);
    assert_eq!(a.q("INSERT INTO tm VALUES (2, 5)").ok("INSERT").tags, ["INSERT 0 1"]);
    fails(&mut a, "UPDATE tm SET x = -1 WHERE id = 2", "value for domain dm violates check constraint \"dm_check\"");
}

/// A client that floods SSLRequests and never reads cannot hold its session, nor the server's one
/// slot, past the authentication timeout (Codex/Claude C37 H1: the server answered every repeated
/// SSLRequest with 'N' and blocked for good in a write with no deadline; a second client got
/// 53300 for 30 s and more). With --max-connections 1 and --authentication-timeout 1, a second
/// client must log in within 8 s. A GUARD, not a discriminating red: on this Mac the base passes it
/// too (the server's send buffer, autotuned to 4 MiB, took every 'N' before the read deadline;
/// raws c37-h1-red*). The write deadline itself is pinned in process by
/// a_startup_write_to_a_client_that_never_reads_ends_at_the_deadline.
#[test]
fn an_ssl_request_flood_that_never_reads_does_not_hold_the_slot() {
    let dir = Scratch::new("sslflood");
    let server = Server::start(&dir.db(), &["--max-connections", "1", "--authentication-timeout", "1"]);
    // The receive buffer is set small BEFORE the connect, so the window the server can fill stays small
    // (set after the connect, the server's 'N' bytes went on being taken and the flood never blocked it).
    let flood = unsafe {
        use std::os::fd::FromRawFd;
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        assert!(fd >= 0);
        let small: libc::c_int = 4096;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &small as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let mut addr: libc::sockaddr_in = std::mem::zeroed();
        addr.sin_family = libc::AF_INET as libc::sa_family_t;
        addr.sin_port = server.port.to_be();
        addr.sin_addr.s_addr = u32::from_ne_bytes([127, 0, 0, 1]);
        #[cfg(target_os = "macos")]
        {
            addr.sin_len = std::mem::size_of::<libc::sockaddr_in>() as u8;
        }
        let rc = libc::connect(
            fd,
            &addr as *const libc::sockaddr_in as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
        assert_eq!(rc, 0, "connect");
        TcpStream::from_raw_fd(fd)
    };
    flood.set_nonblocking(true).unwrap();
    let mut ssl = Vec::new();
    for _ in 0..512 {
        ssl.extend_from_slice(&8i32.to_be_bytes());
        ssl.extend_from_slice(&80877103i32.to_be_bytes());
    }
    let started = Instant::now();
    let mut f = &flood;
    while started.elapsed() < Duration::from_millis(1500) {
        match f.write(&ssl) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(_) => break, // the server closed it: the refusal this test wants
        }
    }
    let mut last = None;
    while started.elapsed() < Duration::from_secs(8) {
        match server.connect_to("postgres") {
            Ok(mut w) => {
                w.q("SELECT 1").ok("the second client's query");
                return;
            }
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("8 s after the flood began the slot is still held: {last:?}");
}

/// A second SSLRequest on one connection is refused as PostgreSQL 18.6 refuses it (recorded,
/// artie lanes/wire/raw/loud-20261015/pg18-diff/pg18-ssl2.out, ssl2.py): 'N' to the first, then
/// FATAL 0A000 "unsupported frontend protocol 1234.5679: server supports 3.0 to 3.2", and the
/// connection closes.
#[test]
fn a_second_ssl_request_is_refused_as_postgresql_refuses_it() {
    let dir = Scratch::new("ssltwice");
    let server = Server::start(&dir.db(), &[]);
    let mut s = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut ssl = 8i32.to_be_bytes().to_vec();
    ssl.extend_from_slice(&80877103i32.to_be_bytes());
    s.write_all(&ssl).unwrap();
    let mut b = [0u8; 1];
    s.read_exact(&mut b).unwrap();
    assert_eq!(&b, b"N", "the first SSLRequest");
    s.write_all(&ssl).unwrap();
    let mut head = [0u8; 5];
    s.read_exact(&mut head[..1]).unwrap();
    assert_eq!(head[0], b'E', "the second SSLRequest's answer: {:?}", head[0] as char);
    s.read_exact(&mut head[1..]).unwrap();
    let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).unwrap();
    let e = error_fields(&body);
    assert_eq!(e.code, "0A000");
    assert_eq!(
        e.message,
        "unsupported frontend protocol 1234.5679: server supports 3.0 to 3.2"
    );
    let mut rest = Vec::new();
    assert_eq!(s.read_to_end(&mut rest).map(|_| rest.len()).unwrap_or(0), 0, "then the close");
}

/// Shutting the server down (SIGINT) ends every session and joins its thread before the process
/// exits: idle sessions, one inside a transaction, and one running a long statement, which is
/// cancelled. The server says how many it shut and joined, and exits 0 (lead 2026-10-15).
#[test]
fn shutdown_joins_every_session_thread() {
    let dir = Scratch::new("shutdownjoin");
    let mut server = Server::start(&dir.db(), &[]);
    let mut idle: Vec<Wire> = (0..3).map(|_| server.connect()).collect();
    for w in &mut idle {
        w.q("SELECT 1").ok("select 1");
    }
    let mut block = server.connect();
    block.q("BEGIN").ok("begin");
    let mut busy = server.connect();
    let slow = "SELECT count(*) FROM generate_series(1, 1000000000000000)";
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((slow.len() + 5) as i32).to_be_bytes());
    q.extend_from_slice(slow.as_bytes());
    q.push(0);
    busy.s.write_all(&q).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let started = Instant::now();
    assert_eq!(unsafe { libc::kill(server.pid() as i32, libc::SIGINT) }, 0);
    let status = loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the server did not exit within 20 s of SIGINT"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success(), "the server exited {status:?}");
    let log = std::fs::read_to_string(server_log_dir().join(format!("server-{}.stdout", server.port)))
        .unwrap();
    assert!(
        log.contains("PostgreSQL server stopped: 5 sessions shut, 5 session threads joined, 0 still running"),
        "the server's stop line: {log}"
    );
    // Every client sees its session end.
    for mut w in idle.into_iter().chain([block, busy]) {
        w.s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut rest = Vec::new();
        let _ = w.s.read_to_end(&mut rest);
    }
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
        // The NOT NULL refusal itself, 23502, not any error (engine review 20 LOW 26).
        let e = a.q("INSERT INTO c (id) VALUES (NULL)").err("a NULL key");
        assert_eq!(e.code, "23502", "{on}: NULL key: {}", e.message);
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
/// traps, instructions retired, cycles — and the server's skipped CHECKPOINTs, the branch
/// store's checkpoints and its four counted exceptions, for the wire-versus-embedded budgets: one
/// row of ten int8 columns that never decrease, where a branch create between two reads costs
/// system calls (ten from six: PRE-APPROVED spec change, artie DECISIONS 2026-10-15T05:16:51Z,
/// the routing of Database::branch_exception_counters).
#[cfg(target_vendor = "apple")]
#[test]
fn branch_stats_reports_the_servers_counters() {
    let dir = Scratch::new("stats");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let read = |a: &mut Wire| -> Vec<i64> {
        let r = a.q("SELECT turso_branch_stats()").ok("stats");
        assert_eq!(
            r.oids,
            Some(vec![20, 20, 20, 20, 20, 20, 20, 20, 20, 20]),
            "ten int8 columns"
        );
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
/// integer overflow) still aborts the block as PostgreSQL does: 25P02 until its end, status 'E',
/// and COMMIT answered ROLLBACK with nothing of the block kept. Before, the block's state was read
/// from the engine after the error, saw autocommit, and the rest committed. Every expected value
/// is PostgreSQL 18.6's (recorded, artie raw loud-20261015/pg18-diff/pg18-c33-engineerr.out). The
/// overflow was sum() over the int8 maximum until that recording showed PostgreSQL answers it with
/// a numeric and no error (C33; the sum's own value is pinned by
/// a_bigint_sum_and_arithmetic_past_int8_answer_as_postgresql_does).
#[test]
fn an_engine_side_rollback_inside_a_block_still_aborts_it() {
    let dir = Scratch::new("enginerollback");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    let r = a.q("SELECT abs(-9223372036854775808::int8)");
    assert!(
        r.error.is_some(),
        "abs overflowed without an error: {:?}",
        r.rows
    );
    assert_eq!(r.status, b'E', "the block is not marked failed");
    let code = r.err("abs").code;
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
    assert_eq!(code, "22003", "abs(int8 minimum) is not PostgreSQL's code");
}

/// sum() over int8 is a numeric in PostgreSQL and does not overflow; int8 + and * past the int8
/// range raise 22003 "bigint out of range" (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-c33-engineerr.out, every case of its first part). The two tests
/// below used sum()'s overflow as an engine error until the recording showed PostgreSQL has none.
#[test]
fn a_bigint_sum_and_arithmetic_past_int8_answer_as_postgresql_does() {
    let dir = Scratch::new("int8range");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT)").ok("big");
    a.q("INSERT INTO big VALUES (9223372036854775807), (1)")
        .ok("rows");
    let r = a.q("SELECT sum(v) FROM big").ok("sum");
    assert_eq!(r.oids, Some(vec![1700]), "sum(int8) is not a numeric");
    assert_eq!(r.rows, vec![vec![Some("9223372036854775808".to_string())]]);
    for sql in [
        "SELECT v + 1 FROM big WHERE v > 1",
        "SELECT v * 2 FROM big WHERE v > 1",
        "SELECT abs(-9223372036854775808::int8)",
        "SELECT abs(v - 9223372036854775807 - 9223372036854775807 - 2) FROM big WHERE v > 1",
    ] {
        let e = a.q(sql).err(sql);
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("22003", "bigint out of range"),
            "{sql}"
        );
    }
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

/// turso_branch_stats' last four columns are the engine's counted exceptions to "no create,
/// connect, first write, delete or trunk commit waits on background work" (Database::
/// branch_exception_counters: hardcap_upgrades, header_raise_exclusive, inline_compaction_syncs,
/// compaction_waits), so every run can print them and label a window in which one moved
/// (artie DECISIONS 2026-10-15T05:16:51Z). On every platform: present, never NULL, integers.
#[test]
fn branch_stats_reports_the_engines_counted_exceptions() {
    let dir = Scratch::new("statsexc");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('e')").ok("create");
    let r = a.q("SELECT turso_branch_stats()").ok("stats");
    assert_eq!(r.rows.len(), 1);
    let row = &r.rows[0];
    assert_eq!(row.len(), 10, "columns {row:?}");
    for (i, v) in row[6..].iter().enumerate() {
        let v = v.as_deref().unwrap_or_else(|| panic!("exception counter {i} is NULL"));
        v.parse::<u64>()
            .unwrap_or_else(|e| panic!("exception counter {i} = {v:?}: {e}"));
    }
}

/// The branch store's checkpoints since the server started (turso_branch_stats' sixth column,
/// `store_checkpoints`, of ten since the engine's counted exceptions joined it).
fn store_checkpoints(a: &mut Wire) -> i64 {
    let r = a.q("SELECT turso_branch_stats()").ok("stats");
    assert_eq!(
        r.oids.as_ref().map(Vec::len),
        Some(10),
        "turso_branch_stats has no store_checkpoints column: {:?}",
        r.oids
    );
    r.rows[0][5]
        .as_deref()
        .expect("the store checkpoint count is never NULL")
        .parse()
        .unwrap()
}

/// A CHECKPOINT outside a block checkpoints the branch store after the trunk, and
/// turso_branch_stats counts the store's checkpoints: PREREG section 5 M2 needs at least 3 store
/// checkpoints in a timed window, or a forced one whose syncs and time are added, and over the
/// wire there was neither, so per-create syncs and bytes left out the branch log's checkpoint I/O
/// that PostgreSQL pays inline (wire review 14 item 13). Inside a block CHECKPOINT never waits, so
/// it leaves the store alone. The branch log (`<db>-branch-log`) is cut by the store's checkpoint.
#[test]
fn a_checkpoint_checkpoints_the_branch_store() {
    let dir = Scratch::new("storeckpt");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    // The engine refuses to branch an empty database (no committed page 1): a table first (ruled
    // c9c821dd8d, P1).
    a.q("CREATE TABLE t(id INT)").ok("t");
    for i in 0..8 {
        a.q(&format!("SELECT turso_branch_create('c{i}')"))
            .ok("create");
    }
    let mut log = dir.db().into_os_string();
    log.push("-branch-log");
    let log = PathBuf::from(log);
    let len = || {
        std::fs::metadata(&log)
            .map(|m| m.len())
            .unwrap_or_else(|e| panic!("{}: {e}", log.display()))
    };
    let before = (len(), store_checkpoints(&mut a));
    assert!(
        before.0 > 40,
        "premise: the creates are in the branch log, past its 40-byte header ({} bytes)",
        before.0
    );
    a.q("BEGIN").ok("begin");
    a.q("CHECKPOINT").ok("checkpoint in a block");
    a.q("COMMIT").ok("commit");
    assert_eq!(
        store_checkpoints(&mut a),
        before.1,
        "a checkpoint inside a block checkpointed the store"
    );
    a.q("CHECKPOINT").ok("checkpoint");
    let after = (len(), store_checkpoints(&mut a));
    assert!(
        after.1 > before.1,
        "the store's checkpoint count did not move: {before:?} -> {after:?}"
    );
    assert!(
        after.0 < before.0,
        "the branch log was not cut: {before:?} -> {after:?}"
    );
    a.q("SELECT turso_branch_switch('c7')")
        .ok("a branch created before the checkpoint");
}

/// SHOW answers the settings SET keeps, as PostgreSQL shows them: search_path from the session
/// (`"$user", public` before any SET, `s, public` after one), the client settings at the values
/// this server answers by, transaction_read_only off; a name PostgreSQL does not know either is
/// 42704 and reaches nothing, and SHOW ALL is 0A000 (wire review 16 item 4). SHOW became `PRAGMA
/// <name>`: a name the engine did not know answered no row, and one it knew ran.
#[test]
fn show_answers_from_the_settings_set_keeps() {
    let dir = Scratch::new("show");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    assert_eq!(
        a.q("SHOW search_path").single("the default path"),
        "\"$user\", public"
    );
    a.q("SET search_path TO s, public").ok("set the path");
    assert_eq!(a.q("SHOW search_path").single("the path"), "s, public");
    for (name, want) in [
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("bytea_output", "hex"),
        ("transaction_read_only", "off"),
    ] {
        assert_eq!(a.q(&format!("SHOW {name}")).single(name), want, "{name}");
    }
    for sql in [
        "SHOW \"synchronous = off\"",
        "SHOW wal_checkpoint",
        "SHOW journal_mode",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "42704", "{sql}");
    }
    assert_eq!(a.q("SHOW ALL").err("SHOW ALL").code, "0A000");
}

/// The settings the server announces at startup (ParameterStatus) are the ones SHOW answers, which
/// are the ones it behaves by: DateStyle `ISO, MDY` (PostgreSQL's boot value), TimeZone UTC,
/// IntervalStyle postgres, client_encoding UTF8, standard_conforming_strings on, and search_path as
/// SHOW gives it before any SET. Startup announced pgwire's defaults (`ISO, YMD`, `Etc/UTC`,
/// `public`) while SHOW answered otherwise (wire review 18 item 19). search_path is reported in
/// PostgreSQL (GUC_REPORT), so a SET that changes it is followed by a ParameterStatus with the new
/// path, and one that leaves it as it was by none. SET DateStyle keeps what it is accepted with:
/// `ISO`, `MDY` and `ISO, MDY` all leave `ISO, MDY`, as in PostgreSQL, and an order or output
/// style this server does not use (DMY, YMD, SQL, German) is 0A000, where `ISO, DMY` was accepted
/// and SHOW still said MDY.
#[test]
fn startup_announces_the_settings_show_answers() {
    let dir = Scratch::new("announce");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let announced = a.parameters.clone();
    for name in [
        "DateStyle",
        "TimeZone",
        "IntervalStyle",
        "client_encoding",
        "standard_conforming_strings",
        "search_path",
    ] {
        let sent: Vec<&str> = announced
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
            .collect();
        let shown = a.q(&format!("SHOW {name}")).single(name);
        assert_eq!(sent, vec![shown.as_str()], "{name}");
    }
    let path = |r: &Reply| {
        r.parameters
            .iter()
            .filter(|(n, _)| n == "search_path")
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let r = a.q("SET search_path TO s, public").ok("set the path");
    assert_eq!(path(&r), vec!["s, public".to_string()], "a changed path");
    let r = a.q("SET search_path TO s, public").ok("the same path");
    assert_eq!(path(&r), Vec::<String>::new(), "an unchanged path");
    for sql in [
        "SET DateStyle = 'ISO'",
        "SET DateStyle = 'MDY'",
        "SET DateStyle = 'ISO, MDY'",
    ] {
        a.q(sql).ok(sql);
        assert_eq!(a.q("SHOW DateStyle").single(sql), "ISO, MDY", "{sql}");
    }
    for sql in [
        "SET DateStyle = 'ISO, DMY'",
        "SET DateStyle = 'YMD'",
        "SET DateStyle = 'SQL, MDY'",
        "SET DateStyle = 'German'",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    }
}

/// `SELECT turso_branch_delete(varchar $1)` is 42601, as PostgreSQL 18 answers `type $n`, a form
/// only libpg_query's grammar accepts, and the branch the value names is not deleted. It ran as a
/// branch call, so Bind 'prod' deleted prod (wire review 14 item 14). Over the extended protocol,
/// $1 declared text.
#[test]
fn a_type_name_before_a_parameter_is_refused() {
    let dir = Scratch::new("typedparam");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('prod')").ok("prod");
    for sql in [
        "SELECT turso_branch_delete(varchar $1)",
        "SELECT varchar $1",
    ] {
        let r = a.xt(sql, &[(25, 0, b"prod")]);
        assert_eq!(
            r.error.as_ref().map(|e| e.code.as_str()),
            Some("42601"),
            "{sql}: rows {:?}",
            r.rows
        );
    }
    a.q("SELECT turso_branch_switch('prod')")
        .ok("prod was not deleted");
}

/// A parameter compared with an inet[], cidr[] or macaddr[] column takes that array type, and one
/// compared with ANY of an inet[] column the element type, as PostgreSQL 18.6 types them
/// (recorded, artie raw loud-20261015/pg18-diff/pg18-arrays.out: 1041, 651, 1040, 869). The array
/// table lacked them, so the first three were 42P18 and ANY's element text (wire review 13 item
/// 18; oid[] is an_oid_array_column_types_its_parameter_oid_array).
#[test]
fn network_and_oid_array_columns_type_their_parameters() {
    let dir = Scratch::new("netarrays");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE n(a INET[], c CIDR[], m MACADDR[], o OID[])").ok("n");
    for (sql, oid) in [
        ("SELECT 1 FROM n WHERE a = $1", 1041),
        ("SELECT 1 FROM n WHERE c = $1", 651),
        ("SELECT 1 FROM n WHERE m = $1", 1040),
        ("SELECT 1 FROM n WHERE $1 = ANY(a)", 869),
    ] {
        let r = a.describe_statement(sql);
        assert_eq!(r.params, Some(vec![oid]), "{sql}: {:?}", r.error);
    }
}

/// KNOWN RED (known-red.txt): a parameter compared with an oid[] column is oid[] (1028) in
/// PostgreSQL 18.6 (recorded, pg18-arrays.out). Here it is int4[] (1007): the translator stores an
/// OID column as the engine's INTEGER, so the declared type no longer says oid (and a scalar oid
/// column reads as int4 too). Split out of network_and_oid_array_columns_type_their_parameters
/// (wire review 13 item 18) when its fix covered the other four rows; the expectation is
/// unchanged. Green when an OID column keeps its type.
#[test]
fn an_oid_array_column_types_its_parameter_oid_array() {
    let dir = Scratch::new("oidarray");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE n(o OID[])").ok("n");
    let sql = "SELECT 1 FROM n WHERE o = $1";
    let r = a.describe_statement(sql);
    assert_eq!(r.params, Some(vec![1028]), "{sql}: {:?}", r.error);
}

/// A text[] parameter is read as PostgreSQL 18.6's array_in reads it (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-r15.out; wire review 13 item 16): an escaped `NUL\L` is the string
/// NULL, never SQL NULL (an unquoted, unescaped NULL is SQL NULL); a vertical tab is whitespace
/// around an element; a stray `}` is 22P02 "malformed array literal" (a `{` inside an element
/// stays the multidimensional refusal, 0A000). Read through INSERT into a text[] column, whose
/// rows this server prints as PostgreSQL does.
#[test]
fn an_array_parameter_is_read_as_array_in_reads_it() {
    let dir = Scratch::new("arrayin");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ar(id INT, x TEXT[])").ok("ar");
    let sql = "INSERT INTO ar VALUES ($1, $2)";
    for (id, value, want) in [
        (1, &b"{\"NUL\\L\"}"[..], Some("{\"NULL\"}")),
        (2, b"{NUL\\L}", Some("{\"NULL\"}")),
        (3, b"{NULL}", Some("{NULL}")),
        (4, b"{a,\x0bb}", Some("{a,b}")),
    ] {
        let id = id.to_string();
        a.xt(sql, &[(23, 0, id.as_bytes()), (1009, 0, value)])
            .ok(&format!("{:?}", String::from_utf8_lossy(value)));
        let got = a
            .q(&format!("SELECT x FROM ar WHERE id = {id}"))
            .ok("read back")
            .rows;
        assert_eq!(
            got,
            vec![vec![want.map(str::to_string)]],
            "{:?}",
            String::from_utf8_lossy(value)
        );
    }
    let r = a.xt(sql, &[(23, 0, b"5"), (1009, 0, b"{a}}")]);
    assert_eq!(r.err("a stray }").code, "22P02");
    let r = a.xt(sql, &[(23, 0, b"6"), (1009, 0, b"{{a}}")]);
    assert_eq!(r.err("a nested array").code, "0A000");
}

/// AND CHAIN where PostgreSQL 18.6 answers it (recorded, artie raw loud-20261015/pg18-diff/
/// pg18-r15.out; wire review 13 item 13, review 15 item 23): in an implicit block (`SELECT 1;
/// COMMIT AND CHAIN`) it is 25P01 "... can only be used in transaction blocks" and the session ends
/// idle, as outside a block (it was 0A000); in a failed block it ends the block, answered
/// ROLLBACK, and chains a new one (status T, and the next statement runs in it), as a failed
/// block's COMMIT or ROLLBACK ends it (it was 25P02, the block left failed). In a live block it
/// stays refused 0A000 (no chain support; COMPAT).
#[test]
fn and_chain_answers_as_postgresql_in_implicit_and_failed_blocks() {
    let dir = Scratch::new("andchain");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for verb in ["COMMIT AND CHAIN", "ROLLBACK AND CHAIN"] {
        let sql = format!("SELECT 1; {verb}");
        let r = a.q(&sql);
        assert_eq!(r.err(&sql).code, "25P01", "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
    }
    for verb in ["COMMIT AND CHAIN", "ROLLBACK AND CHAIN"] {
        a.q("BEGIN").ok("begin");
        a.q("INSERT INTO t VALUES (2, 'two')").ok("a write");
        assert_eq!(a.q("SELECT * FROM nosuch").status, b'E', "premise: the block failed");
        let r = a.q(verb).ok(verb);
        assert_eq!(r.tags, vec!["ROLLBACK".to_string()], "{verb}");
        assert_eq!(r.status, b'T', "{verb}: a chained block is open");
        assert_eq!(a.q("SELECT 1").single("in the chained block"), "1");
        a.q("ROLLBACK").ok("end the chained block");
        assert_eq!(
            a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
            "0",
            "{verb}: the failed block's write was rolled back"
        );
    }
}

/// Arrays in a result and casts to arrays, as PostgreSQL 18.6 answers them (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-arrres.out), on both protocols: `ARRAY[1, 2]` is int4[] (1007) and
/// `ARRAY['a', 'b']` text[] (1009); a string literal cast to an array type is that array, typed so
/// (`'{1,2}'::int[]` was int4 0: the engine casts by the element's scalar affinity); a bound array
/// parameter cast to its array type stays the array, so it is stored as one (`$1::int[]` stored 0).
/// A cast to an array of anything else (a column, an expression) is refused, 0A000, rather than
/// answered by scalar affinity (a divergence, COMPAT). A bound array sent back in a result is the
/// known red a_bound_array_in_a_result_is_sent_as_its_text.
#[test]
fn array_results_are_typed_or_refused() {
    let dir = Scratch::new("arrayresults");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    for (sql, oid, text) in [
        ("SELECT ARRAY[1, 2]", 1007, "{1,2}"),
        ("SELECT ARRAY['a', 'b']", 1009, "{a,b}"),
        ("SELECT '{1,2}'::int[]", 1007, "{1,2}"),
        ("SELECT '{a,b}'::text[]", 1009, "{a,b}"),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!((r.oids, r.rows), (Some(vec![oid]), vec![vec![t(text)]]), "{sql}");
        let r = a.xt(sql, &[]).ok(sql);
        assert_eq!((r.oids, r.rows), (Some(vec![oid]), vec![vec![t(text)]]), "{sql}: extended");
    }
    a.q("CREATE TABLE ar(id INT, x INT[])").ok("ar");
    a.xt("INSERT INTO ar VALUES (1, $1::int[])", &[(1007, 0, b"{1,2}")])
        .ok("a bound array cast to its type");
    assert_eq!(
        a.q("SELECT x FROM ar WHERE id = 1").single("stored"),
        "{1,2}",
        "the bound array was stored as the array"
    );
    let sql = "SELECT CAST(id AS int[]) FROM ar";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    // A cast to an array of a domain is refused too: its check or coercion would not run
    // (PostgreSQL truncates ARRAY['too long']::vc4[] to {"too "}; domain.sql of the regression
    // corpus caught the identity storing 'too long').
    a.q("CREATE DOMAIN vc4 AS varchar(4)").ok("vc4");
    let sql = "SELECT ARRAY['too long']::vc4[]";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
}

/// KNOWN RED (known-red.txt): a bound array parameter sent back in a result (`SELECT $1::text[]`,
/// `SELECT $1` declared text[]) is PostgreSQL 18.6's text[] '{a,b}' (recorded, pg18-arrres.out).
/// Here it is the engine's record blob: rendering it needs core's serialize_array_from_blob, which is
/// pub(crate) (asked of the engine), and the server does not copy it.
#[test]
fn a_bound_array_in_a_result_is_sent_as_its_text() {
    let dir = Scratch::new("arrayback");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in ["SELECT $1::text[]", "SELECT $1"] {
        let r = a.xt(sql, &[(1009, 0, b"{a,b}")]).ok(sql);
        assert_eq!(
            (r.oids, r.rows),
            (Some(vec![1009]), vec![vec![Some("{a,b}".to_string())]]),
            "{sql}"
        );
    }
}

/// Portals as PostgreSQL 18.6 keeps them (recorded, artie raw loud-20261015/pg18-diff/
/// pg18-r15.out; wire review 15 items 16, 17 and 20):
/// - an unnamed portal suspended by a row limit continues at its next Execute, in a block, after
///   a Sync: it was removed after every Execute, so the next one was 34000;
/// - a portal that returned all its rows answers its own verb's tag with 0 rows when executed
///   again (`INSERT 0 0` for an INSERT ... RETURNING): the Finished arm said SELECT 0;
/// - a Bind of a named portal that already exists is 42P03 "cursor ... already exists": it was
///   replaced silently, a suspended one included.
#[test]
fn portals_live_as_postgresql_keeps_them() {
    let dir = Scratch::new("portals");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE r(id INT PRIMARY KEY, v TEXT)").ok("r");
    a.q("INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c')").ok("rows");
    let rows = |r: &Reply| r.rows.iter().map(|r| r[0].clone().unwrap()).collect::<Vec<_>>();
    // Unnamed, suspended, continued after a Sync in a block.
    a.q("BEGIN").ok("begin");
    a.send(b'P', b"\0SELECT id FROM r ORDER BY id\0\0\0");
    a.send(b'B', b"\0\0\0\0\0\0\0\0");
    a.send(b'E', b"\0\0\0\0\x01");
    a.send(b'S', &[]);
    let r = a.read_reply().ok("first row");
    assert_eq!(rows(&r), vec!["1"]);
    a.send(b'E', b"\0\0\0\0\0");
    a.send(b'S', &[]);
    let r = a.read_reply().ok("the unnamed portal continues");
    assert_eq!(rows(&r), vec!["2", "3"]);
    assert_eq!(r.tags, vec!["SELECT 2".to_string()]);
    a.q("COMMIT").ok("commit");
    // A finished RETURNING portal answers INSERT 0 0.
    a.q("BEGIN").ok("begin");
    a.send(b'P', b"s1\0INSERT INTO r VALUES (10, 'x') RETURNING id\0\0\0");
    a.send(b'B', b"p1\0s1\0\0\0\0\0\0\0");
    a.send(b'E', b"p1\0\0\0\0\0");
    a.send(b'E', b"p1\0\0\0\0\0");
    a.send(b'S', &[]);
    let r = a.read_reply().ok("p1 twice");
    assert_eq!(rows(&r), vec!["10"]);
    assert_eq!(
        r.tags,
        vec!["INSERT 0 1".to_string(), "INSERT 0 0".to_string()]
    );
    a.q("ROLLBACK").ok("end");
    // Binding a named portal that exists is 42P03.
    a.q("BEGIN").ok("begin");
    a.send(b'P', b"s2\0SELECT id FROM r ORDER BY id\0\0\0");
    a.send(b'B', b"p2\0s2\0\0\0\0\0\0\0");
    a.send(b'E', b"p2\0\0\0\0\x01");
    a.send(b'S', &[]);
    a.read_reply().ok("p2 suspended");
    a.send(b'B', b"p2\0s2\0\0\0\0\0\0\0");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err("p2 bound again").code, "42P03");
    assert_eq!(r.status, b'E');
    a.q("ROLLBACK").ok("end");
}

/// A float's text is PostgreSQL's float8out / float4out at the default extra_float_digits (1): the
/// shortest exact digits, in fixed notation while the decimal exponent is in [-4, 15) for float8
/// and [-4, 6) for float4, else `d.ddde+XX` with a sign and at least two exponent digits; no
/// trailing `.0`; Infinity spelled out (PostgreSQL 18.6 recorded, artie raw loud-20261015/
/// pg18-diff/pg18-float.out). pgwire's ryu text was `1.0`, `100.0`, `1e16`, `0.00001` and `inf`
/// (the PG 18 differential's finding (d)).
#[test]
fn floats_are_written_as_postgresql_writes_them() {
    let dir = Scratch::new("floattext");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE f(id INT, d DOUBLE PRECISION)").ok("f");
    let cases = [
        ("1", "1"),
        ("-1", "-1"),
        ("100", "100"),
        ("0.1", "0.1"),
        ("1234567", "1234567"),
        ("100000000000000", "100000000000000"),
        ("1e15", "1e+15"),
        ("1e20", "1e+20"),
        ("0.0001", "0.0001"),
        ("1e-5", "1e-05"),
        ("1.5e-5", "1.5e-05"),
        ("3.4028235e38", "3.4028235e+38"),
        ("0.30000000000000004", "0.30000000000000004"),
        ("1234567890123456", "1.234567890123456e+15"),
    ];
    for (i, (literal, _)) in cases.iter().enumerate() {
        a.q(&format!("INSERT INTO f VALUES ({i}, {literal})")).ok(literal);
    }
    let got: Vec<String> = a
        .q("SELECT d FROM f ORDER BY id")
        .ok("read")
        .rows
        .into_iter()
        .map(|r| r[0].clone().unwrap())
        .collect();
    let want: Vec<String> = cases.iter().map(|(_, w)| w.to_string()).collect();
    assert_eq!(got, want);
    let r = a
        .q("SELECT 1234567::float4, 123456::float4, 1e-5::float4")
        .ok("float4");
    assert_eq!(
        r.rows,
        vec![vec![
            Some("1.234567e+06".to_string()),
            Some("123456".to_string()),
            Some("1e-05".to_string()),
        ]]
    );
}

/// The float recording replayed whole, every statement and answer as PostgreSQL 18.6 gave them
/// (artie raw loud-20261015/pg18-diff/pg18-float.out): a real and a double column side by side,
/// the two values out of range for real refused with 22003 and PostgreSQL's message, and the cast
/// row. floats_are_written_as_postgresql_writes_them had kept 14 of its double values and none of
/// its real ones (C33 audit, 2026-10-15).
#[test]
fn the_float_recording_replays_as_postgresql_answered_it() {
    let dir = Scratch::new("floatrecorded");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE f(id INT, r REAL, d DOUBLE PRECISION)")
        .ok("f");
    let stored = [
        "0",
        "1",
        "-1",
        "0.1",
        "1.5",
        "100",
        "123456",
        "1234567",
        "12345678",
        "1e10",
        "1e14",
        "1e15",
        "1e16",
        "1e20",
        "1e-3",
        "1e-4",
        "1e-5",
        "1.5e-5",
        "3.4028235e38",
    ];
    for (i, lit) in stored.iter().enumerate() {
        a.q(&format!("INSERT INTO f VALUES ({}, {lit}, {lit})", i + 1))
            .ok(lit);
    }
    let big = "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    let tiny = format!("0.{}5", "0".repeat(323));
    for (id, lit, shown) in [
        (20, "1.7976931348623157e308", big.to_string()),
        (21, "5e-324", tiny),
    ] {
        let e = a
            .q(&format!("INSERT INTO f VALUES ({id}, {lit}, {lit})"))
            .err(lit);
        assert_eq!(
            (e.code, e.message),
            (
                "22003".to_string(),
                format!("\"{shown}\" is out of range for type real")
            ),
            "{lit}"
        );
    }
    for (id, lit) in [
        (22, "123456789012345"),
        (23, "1234567890123456"),
        (24, "0.30000000000000004"),
        (25, "-0.0"),
    ] {
        a.q(&format!("INSERT INTO f VALUES ({id}, {lit}, {lit})"))
            .ok(lit);
    }
    let r = a.q("SELECT id, r, d FROM f ORDER BY id").ok("read");
    assert_eq!(r.oids, Some(vec![23, 700, 701]));
    let want: Vec<Vec<Option<String>>> = [
        ["1", "0", "0"],
        ["2", "1", "1"],
        ["3", "-1", "-1"],
        ["4", "0.1", "0.1"],
        ["5", "1.5", "1.5"],
        ["6", "100", "100"],
        ["7", "123456", "123456"],
        ["8", "1.234567e+06", "1234567"],
        ["9", "1.2345678e+07", "12345678"],
        ["10", "1e+10", "10000000000"],
        ["11", "1e+14", "100000000000000"],
        ["12", "1e+15", "1e+15"],
        ["13", "1e+16", "1e+16"],
        ["14", "1e+20", "1e+20"],
        ["15", "0.001", "0.001"],
        ["16", "0.0001", "0.0001"],
        ["17", "1e-05", "1e-05"],
        ["18", "1.5e-05", "1.5e-05"],
        ["19", "3.4028235e+38", "3.4028235e+38"],
        ["22", "1.2345679e+14", "123456789012345"],
        ["23", "1.234568e+15", "1.234567890123456e+15"],
        ["24", "0.3", "0.30000000000000004"],
        ["25", "0", "0"],
    ]
    .iter()
    .map(|row| row.iter().map(|c| Some(c.to_string())).collect())
    .collect();
    assert_eq!(r.rows, want);
    let r = a
        .q("SELECT 1e20::float8, 1e15::float8, 1e-5::float8, 1234567::float4, 1e-5::float4")
        .ok("casts");
    assert_eq!(r.oids, Some(vec![701, 701, 701, 700, 700]));
    assert_eq!(
        r.rows,
        vec![["1e+20", "1e+15", "1e-05", "1.234567e+06", "1e-05"]
            .iter()
            .map(|c| Some(c.to_string()))
            .collect::<Vec<_>>()]
    );
}

/// A string literal cast to a float type is read as PostgreSQL's float8in / float4in reads it
/// (recorded, artie raw loud-20261015/pg18-diff/pg18-infcast.out): `'Infinity'`, `'-Infinity'`,
/// `'inf'`, `'+INF'` and `' infinity '` are the infinities, and `'infinityx'` is 22P02. The engine
/// cast them by affinity, to 0, so `'Infinity'::float8 > 1e308` was false. NaN, which the engine
/// cannot hold, is refused 0A000 (PostgreSQL has it: a divergence).
#[test]
fn a_float_literal_cast_reads_infinity() {
    let dir = Scratch::new("infcast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let r = a
        .q("SELECT 'Infinity'::float8 > 1e308, '-Infinity'::float8 < -1e308, 'inf'::float8 > 1e308, \
            '+INF'::float8 > 1e308, ' infinity '::float8 > 1e308, 'Infinity'::float4 > 1e38")
        .ok("infinities");
    assert_eq!(r.rows, vec![vec![Some("t".to_string()); 6]]);
    let sql = "SELECT 'infinityx'::float8";
    assert_eq!(a.q(sql).err(sql).code, "22P02", "{sql}");
    let sql = "SELECT 'NaN'::float8";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
}

/// KNOWN RED (known-red.txt; ruling asked): PostgreSQL 18.6 writes the infinities `Infinity` and
/// `-Infinity` and a negative zero `-0` (recorded, artie raw loud-20261015/pg18-diff/pg18-inf.out);
/// the vendored pgwire writes `inf`, `-inf` and `0`, and its own unit test (test_format) asserts
/// `inf`, so the change waits on a ruling for that test. Split out of
/// floats_are_written_as_postgresql_writes_them, its expectations unchanged.
#[test]
fn infinities_and_negative_zero_are_written_as_postgresql_writes_them() {
    let dir = Scratch::new("floatinf");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let r = a
        .q("SELECT 'Infinity'::float8, '-Infinity'::float8, 'NaN'::float8, '-0'::float8")
        .ok("specials");
    assert_eq!(
        r.rows,
        vec![vec![
            Some("Infinity".to_string()),
            Some("-Infinity".to_string()),
            Some("NaN".to_string()),
            Some("-0".to_string()),
        ]]
    );
}

/// The infinities are written `Infinity` and `-Infinity` (float8 and float4) and a negative zero
/// `-0`, as PostgreSQL 18.6 writes them (recorded, artie raw loud-20261015/pg18-diff/pg18-inf.out);
/// the vendored pgwire wrote `inf`, `-inf` and `0`. NaN, which the engine cannot hold, is left to
/// infinities_and_negative_zero_are_written_as_postgresql_writes_them (known red) (lead ruling
/// 2026-10-15T07:40:59Z: pgwire's test_format moves to 'Infinity').
#[test]
fn infinities_and_negative_zero_are_written_without_nan() {
    let dir = Scratch::new("floatinf2");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let r = a
        .q("SELECT 'Infinity'::float8, '-Infinity'::float8, 'Infinity'::float4")
        .ok("infinities");
    assert_eq!(r.rows, vec![vec![t("Infinity"), t("-Infinity"), t("Infinity")]]);
    let r = a.q("SELECT '-0'::float8").ok("negative zero");
    assert_eq!(r.rows, vec![vec![t("-0")]]);
}

/// A real (float4) column and a double precision (float8) column keep their declared width: their
/// results are announced 700 and 701, a parameter compared with one takes its type, and their text
/// and arithmetic are a float's (REAL affinity, so 2 / 4 is 0.5), as PostgreSQL 18.6 answers
/// (recorded, artie raw loud-20261015/pg18-diff/pg18-types.out: `SELECT r, d FROM k` is
/// [700, 701]). Both were stored as REAL and announced float8 (the PG 18 differential's finding
/// (b); lead ruling 2026-10-15T07:40:59Z: the declarations keep FLOAT4 / FLOAT8, REAL affinity).
#[test]
fn float_columns_keep_their_declared_width() {
    let dir = Scratch::new("floatwidth");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("CREATE TABLE fw(r REAL, f4 FLOAT4, d DOUBLE PRECISION, f8 FLOAT8, f FLOAT)")
        .ok("fw");
    a.q("INSERT INTO fw VALUES (2, 2, 2, 2, 2)").ok("row");
    let r = a.q("SELECT r, f4, d, f8, f FROM fw").ok("select");
    assert_eq!(r.oids, Some(vec![700, 700, 701, 701, 701]));
    assert_eq!(r.rows, vec![vec![t("2"); 5]]);
    let r = a.q("SELECT r / 4, f4 / 4, d / 4, f8 / 4, f / 4 FROM fw").ok("affinity");
    assert_eq!(r.rows, vec![vec![t("0.5"); 5]], "REAL affinity: a float divides as a float");
    let r = a.describe_statement("SELECT 1 FROM fw WHERE r = $1 AND d = $2").ok("params");
    assert_eq!(r.params, Some(vec![700, 701]));
}

/// CREATE TABLE's LIKE clause is refused 0A000, and no table is created: it was silently dropped,
/// so `CREATE TABLE p11 (LIKE p1)` made a table with none of p1's columns (or only the other ones
/// listed), and `ALTER TABLE p11 DROP a` on it then panicked the engine (core/translate/alter.rs:941,
/// the first statement to kill a session in the regress corpus's alter_table, insert and
/// updatable_views scripts). PostgreSQL copies the columns (a divergence, COMPAT's LIKE row).
#[test]
fn create_table_like_is_refused_not_dropped() {
    let dir = Scratch::new("createlike");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p1(a int, b int)").ok("p1");
    for sql in ["CREATE TABLE p11 (LIKE p1)", "CREATE TABLE p11 (LIKE p1, c int)"] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
        let probe = "SELECT * FROM p11";
        assert_eq!(a.q(probe).err(probe).code, "42P01", "{sql} created a table");
    }
    a.q("SELECT 1").ok("the session lives");
}

/// INSERT's VALUES lists are checked as PostgreSQL 18.6 checks them (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-insdef.out): lists of different lengths are 42601 "VALUES lists must
/// all be the same length", and a list shorter or longer than the column list 42601 "INSERT has
/// more target columns than expressions" / "... more expressions than target columns"; a DEFAULT
/// in a positional list stands for that column, so the later values keep their columns. Here the
/// all-DEFAULT positions were dropped first: `(c1, c2, c3) VALUES (DEFAULT, DEFAULT)` left an empty
/// row and panicked the engine (core/translate/select.rs:868, the regress corpus's insert.sql), a
/// ragged `VALUES (1, 2), (3)` inserted a row the engine made up, and a positional
/// `VALUES (DEFAULT, 5, 'x')` was refused, its values shifted one column left.
#[test]
fn insert_values_lists_are_checked_as_postgresql_checks_them() {
    let dir = Scratch::new("insdef");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create table it (col1 int4, col2 int4, col3 text default 'testing')")
        .ok("it");
    for (sql, message) in [
        (
            "insert into it (col1, col2, col3) values (DEFAULT, DEFAULT)",
            "INSERT has more target columns than expressions",
        ),
        (
            "insert into it (col1, col2) values (1, 2, 3)",
            "INSERT has more expressions than target columns",
        ),
        (
            "insert into it (col1, col2) values (1, 2), (3)",
            "VALUES lists must all be the same length",
        ),
    ] {
        let e = a.q(sql).err(sql);
        assert_eq!(e.code, "42601", "{sql}: {}", e.message);
        assert!(e.message.contains(message), "{sql}: {}", e.message);
    }
    a.q("insert into it values (DEFAULT, 5, 'x')").ok("positional DEFAULT");
    a.q("insert into it values (DEFAULT, DEFAULT)").ok("positional all DEFAULT");
    // The recording's own multi-row statement, restored (Claude C33: red 8e8469d1f had replaced it
    // by a one-row INSERT and dropped its (8, DEFAULT, DEFAULT) row; replayed, it showed col1's
    // NULLs sorted first where PostgreSQL's ascending default puts them last).
    a.q("insert into it (col1, col2, col3) values (DEFAULT, 7, DEFAULT), (8, DEFAULT, DEFAULT)")
        .ok("named DEFAULT, two rows");
    let r = a
        .q("select * from it order by col2 nulls first, col1")
        .ok("rows");
    assert_eq!(
        r.rows,
        vec![
            vec![t("8"), None, t("testing")],
            vec![None, None, t("testing")],
            vec![None, t("5"), t("x")],
            vec![None, t("7"), t("testing")],
        ]
    );
}

/// ORDER BY's default NULL placement is PostgreSQL's (recorded, pg18-diff/pg18-nullorder.out, every
/// case): ascending puts NULLs last and descending first, in a plain ORDER BY, by ordinal, after a
/// UNION and in a window; NULLS FIRST / LAST as written. The engine's default (SQLite's) put them
/// first ascending.
#[test]
fn order_by_places_nulls_as_postgresql_does_by_default() {
    let dir = Scratch::new("nullorder");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create table nt (a int4, b int4)").ok("nt");
    a.q("insert into nt values (1, 1), (NULL, 2), (3, 3), (NULL, 4)").ok("rows");
    let col = |a: &mut Wire, sql: &str| {
        a.q(sql).ok(sql).rows.into_iter().map(|r| r[0].clone()).collect::<Vec<_>>()
    };
    let asc = vec![t("1"), t("3"), None, None];
    assert_eq!(col(&mut a, "select a from nt order by a"), asc);
    assert_eq!(col(&mut a, "select a from nt order by a asc"), asc);
    assert_eq!(col(&mut a, "select a from nt order by a desc"), vec![None, None, t("3"), t("1")]);
    assert_eq!(col(&mut a, "select a from nt order by a nulls first"), vec![None, None, t("1"), t("3")]);
    assert_eq!(
        col(&mut a, "select a from nt order by a desc nulls last"),
        vec![t("3"), t("1"), None, None]
    );
    let r = a.q("select b, row_number() over (order by a) from nt order by b").ok("window");
    assert_eq!(
        r.rows,
        vec![
            vec![t("1"), t("1")],
            vec![t("2"), t("3")],
            vec![t("3"), t("2")],
            vec![t("4"), t("4")],
        ]
    );
    assert_eq!(col(&mut a, "select a from nt order by 1"), asc);
    assert_eq!(
        col(&mut a, "select a from nt union select 5 order by 1"),
        vec![t("1"), t("3"), t("5"), None]
    );
}

/// A DEFAULT in only some rows of a multi-row VALUES, named or positional, takes the column's
/// default, as PostgreSQL's does (core's resolve_defaults_in_row replaces the translator's
/// Expr::Default); pinned while the all-DEFAULT positions are dropped by the translator instead.
#[test]
fn a_default_in_some_rows_takes_the_column_default() {
    let dir = Scratch::new("partialdefault");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create table pd (k int, v text default 'dflt')").ok("pd");
    a.q("insert into pd (k, v) values (1, DEFAULT), (2, 'y')").ok("named");
    a.q("insert into pd values (3, DEFAULT), (4, 'z')").ok("positional");
    let r = a.q("select * from pd order by k").ok("rows");
    assert_eq!(
        r.rows,
        vec![
            vec![t("1"), t("dflt")],
            vec![t("2"), t("y")],
            vec![t("3"), t("dflt")],
            vec![t("4"), t("z")],
        ]
    );
}

/// An identity column numbers its rows as PostgreSQL 18.6 numbers them, 1, 2, ... (recorded, artie
/// raw loud-20261015/pg18-diff/pg18-ident.out), GENERATED ALWAYS or BY DEFAULT, and a stored
/// generated column, which the engine cannot hold, is refused 0A000 with no table created. The
/// translator dropped both clauses: identity ids were NULL (or a NOT NULL failure under a primary
/// key), and a generated column read NULL.
#[test]
fn identity_columns_number_rows_and_stored_generated_is_refused() {
    let dir = Scratch::new("identity");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create table idt (id int generated always as identity, v text)").ok("idt");
    a.q("insert into idt (v) values ('a'), ('b')").ok("idt rows");
    a.q("create table idb (id bigint generated by default as identity primary key, v text)")
        .ok("idb");
    a.q("insert into idb (v) values ('a'), ('b')").ok("idb rows");
    for table in ["idt", "idb"] {
        let r = a.q(&format!("select id, v from {table} order by v")).ok(table);
        assert_eq!(r.rows, vec![vec![t("1"), t("a")], vec![t("2"), t("b")]], "{table}");
    }
    let sql = "create table gs (a int, b int generated always as (a * 2) stored)";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    let probe = "select * from gs";
    assert_eq!(a.q(probe).err(probe).code, "42P01", "the generated table was created");
}

/// A positional VALUES list shorter than the table fills its leading columns, the rest taking
/// their defaults, and one longer is 42601 "INSERT has more expressions than target columns", as
/// PostgreSQL 18.6 answers (recorded, artie raw loud-20261015/pg18-diff/pg18-short.out); the engine
/// refused a short list ("table s3 has 3 columns but 2 values were supplied").
#[test]
fn a_short_positional_values_list_fills_the_leading_columns() {
    let dir = Scratch::new("shortvalues");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create table s3 (a int, b int, c text default 'd')").ok("s3");
    a.q("insert into s3 values (1, 2)").ok("two of three");
    a.q("insert into s3 values (3)").ok("one of three");
    a.q("insert into s3 values (4, 5), (6, 7)").ok("two rows");
    let sql = "insert into s3 values (1, 2, 'x', 9)";
    let e = a.q(sql).err(sql);
    assert_eq!(e.code, "42601", "{sql}: {}", e.message);
    assert!(
        e.message.contains("INSERT has more expressions than target columns"),
        "{sql}: {}",
        e.message
    );
    let r = a.q("select * from s3 order by a").ok("rows");
    assert_eq!(
        r.rows,
        vec![
            vec![t("1"), t("2"), t("d")],
            vec![t("3"), None, t("d")],
            vec![t("4"), t("5"), t("d")],
            vec![t("6"), t("7"), t("d")],
        ]
    );
}

/// A domain over numeric or a float type takes a quoted value, as PostgreSQL's input function reads
/// it (the regress corpus's domain.sql: `INSERT INTO basictest VALUES (..., '123.12')`). Since the
/// float types are stored FLOAT4 / FLOAT8 (0e4a259fb) a domain's base became the engine's custom
/// float8 type, which refused the text ("cannot store TEXT value in REAL column").
#[test]
fn a_domain_over_numeric_takes_a_quoted_value() {
    let dir = Scratch::new("domainnumeric");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("create domain dn numeric(8,2)").ok("dn");
    a.q("create domain dd double precision").ok("dd");
    a.q("create table bt (n dn, d dd)").ok("bt");
    a.q("insert into bt values ('123.5', '2.5')").ok("quoted values");
    let r = a.q("select n, d from bt").ok("rows");
    assert_eq!(r.rows, vec![vec![t("123.5"), t("2.5")]]);
}

/// A FROM alias's column list renames the relation's columns, the first ones, in order, as in
/// PostgreSQL 18.6 (recorded, artie raw loud-20261015/pg18-diff/pg18-join1.out): `j1 AS t1 (a, b,
/// c)` is read as a, b, c and named so in the result, `j1 t1 (a)` renames only the first, and a
/// NATURAL JOIN joins on the new names. The list was dropped: the result kept i, j, t (the regress
/// corpus's join.sql) and `t1.a` was 42703. A list longer than the relation is 42P10.
#[test]
fn a_from_alias_column_list_renames_the_columns() {
    let dir = Scratch::new("aliascols");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let names = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    a.q("CREATE TABLE j1 (i int, j int, t text)").ok("j1");
    a.q("CREATE TABLE j2 (i int, k int)").ok("j2");
    a.q("INSERT INTO j1 VALUES (1, 4, 'one'), (2, 3, 'two')").ok("j1 rows");
    a.q("INSERT INTO j2 VALUES (1, -1), (5, -5)").ok("j2 rows");
    let r = a.q("SELECT * FROM j1 AS t1 (a, b, c) ORDER BY a").ok("all renamed");
    assert_eq!(r.names, names(&["a", "b", "c"]));
    assert_eq!(r.rows, vec![vec![t("1"), t("4"), t("one")], vec![t("2"), t("3"), t("two")]]);
    let r = a.q("SELECT t1.a, c FROM j1 t1 (a, b, c) ORDER BY a").ok("qualified");
    assert_eq!((r.names, r.rows), (names(&["a", "c"]), vec![vec![t("1"), t("one")], vec![t("2"), t("two")]]));
    let r = a.q("SELECT * FROM j1 t1 (a) ORDER BY a").ok("first renamed");
    assert_eq!(r.names, names(&["a", "j", "t"]));
    let r = a
        .q("SELECT * FROM j1 t1 (a, b, c) NATURAL JOIN j2 t2 (a, d) ORDER BY a")
        .ok("natural join on the new names");
    assert_eq!(r.names, names(&["a", "b", "c", "d"]));
    assert_eq!(r.rows, vec![vec![t("1"), t("4"), t("one"), t("-1")]]);
    let sql = "SELECT * FROM j2 t2 (a, b, c)";
    assert_eq!(a.q(sql).err(sql).code, "42P10", "{sql}");
}

/// A FROM alias column list on a CTE renames the CTE's columns, as PostgreSQL does, also when a
/// table has the CTE's name: the table's columns were read, so `WITH t AS (SELECT 1 AS k, 2 AS m)
/// SELECT * FROM t AS a (x)` asked the CTE for the table's columns (42703), and with no such table
/// the list was refused (ultrareview of fastest-wire, finding 8).
#[test]
fn a_from_alias_column_list_on_a_cte_renames_the_ctes_columns() {
    let dir = Scratch::new("aliascolscte");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let names = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    a.q("CREATE TABLE t (id int, v int)").ok("t");
    a.q("INSERT INTO t VALUES (10, 20)").ok("t row");
    let r = a
        .q("WITH t AS (SELECT 1 AS k, 2 AS m) SELECT * FROM t AS a (x)")
        .ok("a CTE named like a table");
    assert_eq!(
        (r.names, r.rows),
        (names(&["x", "m"]), vec![vec![t("1"), t("2")]])
    );
    let r = a
        .q("WITH c (p, q) AS (SELECT id, v FROM t) SELECT a.y, q FROM c a (y)")
        .ok("a CTE with no table of its name, its own column list");
    assert_eq!(
        (r.names, r.rows),
        (names(&["y", "q"]), vec![vec![t("10"), t("20")]])
    );
    let r = a
        .q("WITH c (p) AS (SELECT 1 + 1) SELECT * FROM c AS a (y)")
        .ok("a CTE whose own column list names an unnamed output");
    assert_eq!((r.names, r.rows), (names(&["y"]), vec![vec![t("2")]]));
    let sql = "WITH c AS (SELECT 1 AS x, 2 AS x) SELECT * FROM c AS a (p, q)";
    let r = a.q(sql);
    assert!(
        r.error.is_some() || r.rows == vec![vec![t("1"), t("2")]],
        "{sql}: two outputs named x read as one: {:?}",
        r.rows
    );
    let r = a
        .q("WITH c (m, n) AS (SELECT 1 AS x, 2 AS x) SELECT * FROM c AS a (p, q)")
        .ok("a CTE whose column list names its duplicate outputs");
    assert_eq!(
        (r.names, r.rows),
        (names(&["p", "q"]), vec![vec![t("1"), t("2")]])
    );
    let r = a
        .q("WITH v AS (VALUES (1, 2)) SELECT * FROM v AS a (p)")
        .ok("a VALUES CTE");
    assert_eq!(
        (r.names, r.rows),
        (names(&["p", "column2"]), vec![vec![t("1"), t("2")]])
    );
    let r = a
        .q("SELECT * FROM t AS a (x) WHERE EXISTS (WITH t AS (SELECT 1 AS k) SELECT 1 FROM t)")
        .ok("a CTE out of the FROM's scope");
    assert_eq!(
        (r.names, r.rows),
        (names(&["x", "v"]), vec![vec![t("10"), t("20")]])
    );
}

/// KNOWN RED (ENGINE; known-red.txt): `SELECT *` over a RIGHT JOIN ... USING or a NATURAL RIGHT
/// JOIN puts the merged join column first, then the left table's other columns, then the right's,
/// as PostgreSQL 18.6 does (recorded, artie raw loud-20261015/pg18-diff/pg18-join2.out). The
/// engine expands it as the right table's own columns after the left's (`j, t, i, k`), so a client
/// that reads the row by position reads `j` where `i` stands.
#[test]
fn a_right_join_using_puts_the_merged_column_first() {
    let dir = Scratch::new("rightusing");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let names = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    a.q("CREATE TABLE j1 (i int, j int, t text)").ok("j1");
    a.q("CREATE TABLE j2 (i int, k int)").ok("j2");
    a.q("INSERT INTO j1 VALUES (1, 4, 'one'), (3, 2, 'three')").ok("j1 rows");
    a.q("INSERT INTO j2 VALUES (1, -1), (5, -5)").ok("j2 rows");
    for sql in [
        "SELECT * FROM j1 RIGHT JOIN j2 USING (i) ORDER BY i",
        "SELECT * FROM j1 NATURAL RIGHT JOIN j2 ORDER BY i",
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(r.names, names(&["i", "j", "t", "k"]), "{sql}");
        assert_eq!(
            r.rows,
            vec![vec![t("1"), t("4"), t("one"), t("-1")], vec![t("5"), None, None, t("-5")]],
            "{sql}"
        );
    }
}

/// A boolean or float4 column of a derived table or a CTE keeps its type when the outer SELECT reads
/// it, by name or through `*`, alone or across a join, as PostgreSQL 18.6 types it (recorded, artie
/// raw loud-20261015/pg18-diff/pg18-derivedbool.out). It was announced bytea (17) holding 1 and 0
/// (the regress corpus's join.sql: `true AS linked` read through a LEFT JOIN).
#[test]
fn a_derived_tables_boolean_column_stays_boolean() {
    let dir = Scratch::new("derivedbool");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("CREATE TABLE parent (k int primary key, pd int)").ok("parent");
    a.q("CREATE TABLE child (k int unique, cd int)").ok("child");
    a.q("INSERT INTO parent VALUES (1, 10), (2, 20)").ok("parent rows");
    a.q("INSERT INTO child VALUES (1, 100), (4, 400)").ok("child rows");
    for (sql, oids, rows) in [
        ("SELECT linked FROM (SELECT true AS linked) s", vec![16], vec![vec![t("t")]]),
        (
            "SELECT s.b, s.c FROM (SELECT 1 = 1 AS b, 1.5::float4 AS c) s",
            vec![16, 700],
            vec![vec![t("t"), t("1.5")]],
        ),
        (
            "SELECT p.k, linked FROM parent p LEFT JOIN (SELECT c.*, true AS linked FROM child c) \
             AS ss ON (p.k = ss.k) ORDER BY p.k",
            vec![23, 16],
            vec![vec![t("1"), t("t")], vec![t("2"), None]],
        ),
        (
            "WITH w AS (SELECT k > 1 AS big FROM parent) SELECT big FROM w ORDER BY big",
            vec![16],
            vec![vec![t("f")], vec![t("t")]],
        ),
        (
            "SELECT * FROM (SELECT k, k = 1 AS one FROM parent) s ORDER BY k",
            vec![23, 16],
            vec![vec![t("1"), t("t")], vec![t("2"), t("f")]],
        ),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!((r.oids, r.rows), (Some(oids), rows), "{sql}");
    }
}

/// A `*` over a table beside a derived item's boolean column lines the result up with the table's
/// width, so the boolean stays boolean (recorded, artie raw loud-20261015/pg18-diff/
/// pg18-tablestar.out; the regress corpus's join.sql `p.*, linked`): the typing walk could not
/// expand a table's `*` and left every column to the engine, which announced the boolean bytea.
#[test]
fn a_tables_star_beside_a_derived_boolean_keeps_it_boolean() {
    let dir = Scratch::new("tablestar");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    a.q("CREATE TABLE parent (k int primary key, pd int)").ok("parent");
    a.q("CREATE TABLE child (k int unique, cd int)").ok("child");
    a.q("INSERT INTO parent VALUES (1, 10), (2, 20)").ok("parent rows");
    a.q("INSERT INTO child VALUES (1, 100), (4, 400)").ok("child rows");
    for (sql, rows) in [
        (
            "SELECT p.*, linked FROM parent p LEFT JOIN (SELECT c.*, true AS linked FROM child c) \
             AS ss ON (p.k = ss.k) ORDER BY p.k",
            vec![vec![t("1"), t("10"), t("t")], vec![t("2"), t("20"), None]],
        ),
        (
            "SELECT * FROM parent p, (SELECT true AS b) s ORDER BY p.k",
            vec![vec![t("1"), t("10"), t("t")], vec![t("2"), t("20"), t("t")]],
        ),
        (
            "SELECT * FROM (SELECT c.*, k > 1 AS big FROM child c) s ORDER BY k",
            vec![vec![t("1"), t("100"), t("f")], vec![t("4"), t("400"), t("t")]],
        ),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!((r.oids, r.rows), (Some(vec![23, 23, 16]), rows), "{sql}");
    }
}

/// KNOWN RED (ENGINE; known-red.txt): generate_series with an infinite start, stop or step is
/// 22023, as PostgreSQL 18.6 refuses it ("stop value cannot be infinity"; recorded, artie raw
/// loud-20261015/pg18-diff/pg18-gsinf.out). Since the crash batch reads 'inf' text as the
/// infinity, the engine's series to an infinite stop never ends: the session's thread spins and
/// answers nothing more (the regress corpus's numeric.sql statement 867 stopped the script). The
/// client gives up after 5 s here, so the red fails rather than hangs.
#[test]
fn generate_series_refuses_infinite_bounds() {
    let dir = Scratch::new("gsinf");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    for sql in [
        "select * from generate_series(1, 3, 'inf'::numeric)",
        "select * from generate_series('-inf'::numeric, 0, 10)",
        "select * from generate_series(0::numeric, 'inf'::numeric, 10::numeric)",
    ] {
        let e = a.q(sql).err(sql);
        assert_eq!(e.code, "22023", "{sql}: {}", e.message);
    }
}

/// A result column with no alias is named as PostgreSQL's FigureColname names it (recorded, artie
/// raw loud-20261015/pg18-diff/pg18-colnames.out): a column reference or a cast of one by the
/// column, a cast of anything else by the type, a function, CASE, COALESCE, GREATEST, LEAST,
/// NULLIF, ARRAY and EXISTS by their keyword, a scalar subquery by its one column, and an operator
/// or a literal `?column?`. They were named by the engine's text of the translated expression
/// (`CAST (9e999 AS REAL)`, `MAX (1, 2)`, `'abc'`): the regress corpus's numeric.sql and others,
/// and a client reading a row by column name. Every recorded statement is replayed whole: the
/// test had dropped `'nan'::numeric / '0'` and `ROW(1, 2)` from two of them (C33 audit,
/// 2026-10-15); the types and values of the same statements are
/// unaliased_results_carry_postgresqls_types_and_values.
#[test]
fn an_unaliased_result_column_is_named_as_postgresql_names_it() {
    let dir = Scratch::new("colnames");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cn (a int, b text)").ok("cn");
    a.q("INSERT INTO cn VALUES (1, 'x')").ok("row");
    let q = "?column?";
    for (sql, names) in [
        (
            "SELECT 1 + 1, 'nan'::numeric / '0', 'Infinity'::float8::numeric, '42'::int2::numeric, \
             a::int8, b::text FROM cn",
            vec![q, q, "numeric", "numeric", "a", "b"],
        ),
        (
            "SELECT CASE WHEN true THEN 1 END, CASE WHEN true THEN a ELSE a END, coalesce(1, 2), \
             greatest(1, 2), least(1, 2), nullif(1, 2) FROM cn",
            vec!["case", "a", "coalesce", "greatest", "least", "nullif"],
        ),
        (
            "SELECT ARRAY[1], ROW(1, 2), EXISTS (SELECT 1), (SELECT 1 AS q), (SELECT a FROM cn), \
             round(1.5) = 2, -1, NOT true, 1 IS NULL",
            vec!["array", "row", "exists", "q", "a", q, q, q, q],
        ),
        ("SELECT 'abc', 42, 1.5, true, NULL, $$d$$", vec![q; 6]),
        (
            "SELECT current_date IS NOT NULL, 'x'::varchar(3), CAST(1 AS integer), a + 0 FROM cn",
            vec![q, "varchar", "int4", q],
        ),
    ] {
        let r = a.q(sql).ok(sql);
        let want: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        assert_eq!(r.names, Some(want), "{sql}");
    }
}

/// The types and values of the statements an_unaliased_result_column_is_named_as_postgresql_names_it
/// replays, every one as PostgreSQL 18.6 answered it (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-colnames.out; C33 audit, 2026-10-15).
#[test]
fn unaliased_results_carry_postgresqls_types_and_values() {
    let dir = Scratch::new("colvalues");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cn (a int, b text)").ok("cn");
    a.q("INSERT INTO cn VALUES (1, 'x')").ok("row");
    let v = |r: &[Option<&str>]| vec![r.iter().map(|c| c.map(str::to_string)).collect::<Vec<_>>()];
    for (sql, oids, rows) in [
        (
            "SELECT 1 + 1, 'nan'::numeric / '0', 'Infinity'::float8::numeric, '42'::int2::numeric, \
             a::int8, b::text FROM cn",
            vec![23, 1700, 1700, 1700, 20, 25],
            v(&[Some("2"), Some("NaN"), Some("Infinity"), Some("42"), Some("1"), Some("x")]),
        ),
        (
            "SELECT CASE WHEN true THEN 1 END, CASE WHEN true THEN a ELSE a END, coalesce(1, 2), \
             greatest(1, 2), least(1, 2), nullif(1, 2) FROM cn",
            vec![23; 6],
            v(&[Some("1"), Some("1"), Some("1"), Some("2"), Some("1"), Some("1")]),
        ),
        (
            "SELECT ARRAY[1], ROW(1, 2), EXISTS (SELECT 1), (SELECT 1 AS q), (SELECT a FROM cn), \
             round(1.5) = 2, -1, NOT true, 1 IS NULL",
            vec![1007, 2249, 16, 23, 23, 16, 23, 16, 16],
            v(&[
                Some("{1}"),
                Some("(1,2)"),
                Some("t"),
                Some("1"),
                Some("1"),
                Some("t"),
                Some("-1"),
                Some("f"),
                Some("f"),
            ]),
        ),
        (
            "SELECT 'abc', 42, 1.5, true, NULL, $$d$$",
            vec![25, 23, 1700, 16, 25, 25],
            v(&[Some("abc"), Some("42"), Some("1.5"), Some("t"), None, Some("d")]),
        ),
        (
            "SELECT current_date IS NOT NULL, 'x'::varchar(3), CAST(1 AS integer), a + 0 FROM cn",
            vec![16, 1043, 23, 23],
            v(&[Some("t"), Some("x"), Some("1"), Some("1")]),
        ),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!((r.oids, r.rows), (Some(oids), rows), "{sql}");
    }
}

/// CASE, COALESCE, GREATEST, LEAST and NULLIF over integers are int4, as PostgreSQL 18.6 types
/// them (recorded, artie raw loud-20261015/pg18-diff/pg18-colnames.out: oids 23); they were
/// announced text (25), so a driver read a number as a string.
#[test]
fn conditional_expressions_over_integers_are_int4() {
    let dir = Scratch::new("condint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let sql = "SELECT CASE WHEN true THEN 1 END, coalesce(1, 2), greatest(1, 2), least(1, 2), \
               nullif(1, 2)";
    let r = a.q(sql).ok(sql);
    assert_eq!(r.oids, Some(vec![23; 5]), "{sql}");
    assert_eq!(r.rows, vec![vec![t("1"), t("1"), t("2"), t("1"), t("1")]], "{sql}");
}

/// A `*` or `alias.*` over a correlated generate_series names its column as PostgreSQL does, by the
/// alias or the alias's column list (recorded, artie raw loud-20261015/pg18-diff/pg18-gsstar.out):
/// the engine's table call names it `value`, and only a reference by name was renamed (the regress
/// corpus's numeric.sql `generate_series(i, 3) j`).
#[test]
fn a_star_over_a_correlated_series_names_its_column() {
    let dir = Scratch::new("gsstar");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let names = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    a.q("CREATE TABLE gt (x int)").ok("gt");
    a.q("INSERT INTO gt VALUES (2), (3)").ok("rows");
    for (sql, want, rows) in [
        ("select * from generate_series(1, 3) i, generate_series(i, 3) j order by 1, 2", ["i", "j"], 6),
        ("select * from gt, generate_series(1, gt.x) g order by 1, 2", ["x", "g"], 5),
        ("select * from gt, generate_series(1, gt.x) g(n) order by 1, 2", ["x", "n"], 5),
        ("select gt.*, g.* from gt, generate_series(1, gt.x) g order by 1, 2", ["x", "g"], 5),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(r.names, names(&want), "{sql}");
        assert_eq!(r.rows.len(), rows, "{sql}");
    }
}

/// CREATE TABLE ... INHERITS is refused 0A000, with no table created. PostgreSQL gives the child
/// its parents' columns too and shows its rows through the parent (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-inherits.out); the clause was dropped, so the child had its own
/// columns alone and an INSERT of the parents' values went into the wrong columns or failed (the
/// regress corpus's truncate.sql and returning.sql).
#[test]
fn create_table_inherits_is_refused_not_dropped() {
    let dir = Scratch::new("inherits");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ipa (a int)").ok("parent");
    let sql = "CREATE TABLE ich (b text) INHERITS (ipa)";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    let probe = "SELECT * FROM ich";
    assert_eq!(a.q(probe).err(probe).code, "42P01", "the child was created");
}

/// A statement running long stops on a CancelRequest sent on a second connection with the
/// session's BackendKeyData, and on SET statement_timeout, with 57014 within seconds, and the
/// session answers after, as PostgreSQL 18.6 does (recorded with cancel.py, artie raw
/// loud-20261015/pg18-diff/pg18-cancel.out: 1.03 s and 1.01 s). Here a cancel did nothing (the
/// server's cancel handler was pgwire's no-op) and statement_timeout was refused unless 0, so any
/// client could hold a session thread at full CPU for as long as its statement ran
/// (turso-cancel-3f6b2dbff.out: no answer in 20 s, the server at 198% CPU after).
#[test]
fn a_long_statement_stops_on_cancel_and_on_statement_timeout() {
    let dir = Scratch::new("cancel");
    let server = Server::start(&dir.db(), &[]);
    let slow = "SELECT count(*) FROM generate_series(1, 1000000000000000)";
    let mut a = server.connect();
    a.s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    let key = a.key.clone().expect("BackendKeyData at startup");
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((slow.len() + 5) as i32).to_be_bytes());
    q.extend_from_slice(slow.as_bytes());
    q.push(0);
    let started = Instant::now();
    a.s.write_all(&q).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mut cancel = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    let mut m = ((8 + key.len()) as i32).to_be_bytes().to_vec();
    m.extend_from_slice(&80877102i32.to_be_bytes());
    m.extend_from_slice(&key);
    cancel.write_all(&m).unwrap();
    drop(cancel);
    let r = a.read_reply();
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("57014"),
        "cancel: {:?} after {:?}",
        r.error,
        started.elapsed()
    );
    assert!(started.elapsed() < Duration::from_secs(10), "cancel took {:?}", started.elapsed());
    a.q("SELECT 1").ok("the session after the cancel");
    a.q("SET statement_timeout = 1000").ok("statement_timeout");
    let started = Instant::now();
    let e = a.q(slow).err("statement_timeout");
    assert_eq!(e.code, "57014", "statement_timeout: {}", e.message);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "timeout took {:?}",
        started.elapsed()
    );
    a.q("SET statement_timeout = 0").ok("statement_timeout off");
    a.q("SELECT 1").ok("the session after the timeout");
}

/// A statement waiting on a lock another session holds is stopped by its statement_timeout and by
/// a cancel, as one that is running is: 57014 within the timeout, not 55P03 once the server's lock
/// wait (60 s) is spent. An INSERT stopped on both before; a CHECKPOINT, whose wait is the
/// server's own (Session::waiting) with no statement stepping, waited until the holder committed
/// whatever its timeout, and ignored a cancel (ultrareview of fastest-wire, finding 9).
#[test]
fn a_statement_waiting_on_a_lock_stops_on_statement_timeout_and_on_cancel() {
    let dir = Scratch::new("cancelwait");
    let server = Server::start(&dir.db(), &[]);
    let mut holder = server.connect();
    holder.q("CREATE TABLE t(id INTEGER)").ok("create");
    holder.q("BEGIN").ok("begin");
    holder.q("INSERT INTO t VALUES (1)").ok("the held write");
    let mut b = server.connect();
    b.s.set_read_timeout(Some(Duration::from_secs(90))).unwrap();
    b.q("SET statement_timeout = 500").ok("statement_timeout");
    let started = Instant::now();
    let e = b
        .q("INSERT INTO t VALUES (2)")
        .err("statement_timeout during a lock wait");
    assert_eq!(
        e.code,
        "57014",
        "statement_timeout during a lock wait: {} after {:?}",
        e.message,
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "timeout took {:?}",
        started.elapsed()
    );
    b.q("SET statement_timeout = 0").ok("statement_timeout off");
    let key = b.key.clone().expect("BackendKeyData at startup");
    for sql in ["INSERT INTO t VALUES (3)", "CHECKPOINT"] {
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        let started = Instant::now();
        b.s.write_all(&q).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        let mut cancel = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
        let mut m = ((8 + key.len()) as i32).to_be_bytes().to_vec();
        m.extend_from_slice(&80877102i32.to_be_bytes());
        m.extend_from_slice(&key);
        cancel.write_all(&m).unwrap();
        drop(cancel);
        let r = b.read_reply();
        assert_eq!(
            r.error.as_ref().map(|e| e.code.as_str()),
            Some("57014"),
            "cancel of {sql} during a lock wait: {:?} after {:?}",
            r.error,
            started.elapsed()
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "cancel of {sql} took {:?}",
            started.elapsed()
        );
    }
    b.q("SET statement_timeout = 500").ok("statement_timeout");
    let started = Instant::now();
    let e = b
        .q("CHECKPOINT")
        .err("statement_timeout during CHECKPOINT's lock wait");
    assert_eq!(
        e.code,
        "57014",
        "statement_timeout during CHECKPOINT's lock wait: {} after {:?}",
        e.message,
        started.elapsed()
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "timeout took {:?}",
        started.elapsed()
    );
    b.q("SET statement_timeout = 0").ok("statement_timeout off");
    holder.q("COMMIT").ok("commit");
    b.q("INSERT INTO t VALUES (4)").ok("the lock is free again");
}

/// A CancelRequest stops a statement only when it names the session's process id AND its secret
/// key: one with a wrong secret, or with the right process id and a wrong secret, or another
/// session's key, cancels nothing (the statement ran on until the right key came, 1.5 s later);
/// two sessions' keys differ (lead's condition on the cancel fix, 2026-10-15).
#[test]
fn a_cancel_must_name_the_sessions_pid_and_secret() {
    let dir = Scratch::new("cancelkey");
    let server = Server::start(&dir.db(), &[]);
    let slow = "SELECT count(*) FROM generate_series(1, 1000000000000000)";
    let mut a = server.connect();
    let other = server.connect();
    a.s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    let key = a.key.clone().expect("BackendKeyData");
    let other_key = other.key.clone().expect("BackendKeyData");
    assert_eq!(key.len(), 8, "a 3.0 BackendKeyData: process id and a 4-byte secret");
    assert_ne!(key, other_key, "two sessions share a key");
    let cancel = |body: &[u8]| {
        let mut c = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
        let mut m = ((8 + body.len()) as i32).to_be_bytes().to_vec();
        m.extend_from_slice(&80877102i32.to_be_bytes());
        m.extend_from_slice(body);
        c.write_all(&m).unwrap();
    };
    let mut wrong_secret = key.clone();
    wrong_secret[7] ^= 0x5a;
    let mut wrong_pid = key.clone();
    wrong_pid[3] ^= 0x5a;
    let mut q = vec![b'Q'];
    q.extend_from_slice(&((slow.len() + 5) as i32).to_be_bytes());
    q.extend_from_slice(slow.as_bytes());
    q.push(0);
    let started = Instant::now();
    a.s.write_all(&q).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    for body in [&wrong_secret, &wrong_pid, &other_key] {
        cancel(body);
    }
    std::thread::sleep(Duration::from_millis(1200));
    // Nothing has answered yet: the statement still runs (a byte waiting here is its reply).
    a.s.set_nonblocking(true).unwrap();
    let early = a.s.peek(&mut [0u8; 1]);
    a.s.set_nonblocking(false).unwrap();
    assert!(
        early.as_ref().is_err_and(|e| e.kind() == std::io::ErrorKind::WouldBlock),
        "a wrong key stopped the statement: {early:?} after {:?}",
        started.elapsed()
    );
    cancel(&key);
    let r = a.read_reply();
    assert_eq!(r.error.as_ref().map(|e| e.code.as_str()), Some("57014"), "{:?}", r.error);
    a.q("SELECT 1").ok("the session after");
}

/// turso_branch_allocations() is one int8 column, NULL in a build that counts no allocations (this
/// test's: the counter is the budget builds' `alloc-count` feature alone), so the budget refuses
/// a server that cannot count rather than read 0 as none.
#[test]
fn branch_allocations_is_null_where_nothing_counts() {
    let dir = Scratch::new("allocs");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let r = a.q("SELECT turso_branch_allocations()").ok("allocations");
    assert_eq!((r.oids, r.rows), (Some(vec![20]), vec![vec![None]]));
}

/// random() is PostgreSQL's: a float8 (701) in [0, 1), so `(random()*3)::int` is 0 to 3 (recorded,
/// artie raw loud-20261015/pg18-diff/pg18-random.out). The engine's random() is SQLite's, a 64-bit
/// integer, so `random()*3` overflowed and the regress corpus's union script drew
/// 9223372036854775807.
#[test]
fn random_is_a_float8_in_the_unit_interval() {
    let dir = Scratch::new("random");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for _ in 0..50 {
        let r = a.q("SELECT random(), (random()*3)::int BETWEEN 0 AND 3").ok("random");
        assert_eq!(r.oids, Some(vec![701, 16]));
        let v: f64 = r.rows[0][0].as_deref().unwrap().parse().unwrap();
        assert!((0.0..1.0).contains(&v), "random() = {v}");
        assert_eq!(r.rows[0][1].as_deref(), Some("t"));
    }
}

/// A result column that is a bare parameter is announced as the parameter's type, declared or
/// inferred, as PostgreSQL 18.6 announces it (recorded, artie raw loud-20261015/pg18-diff/
/// pg18-types.out: `SELECT $1` declared float4 is 700 '1.5', declared bool 16 't'), on the
/// statement's and the portal's Describe; it was text whatever the declaration (the PG 18
/// differential's finding (c)).
#[test]
fn a_bare_parameter_result_takes_the_parameters_type() {
    let dir = Scratch::new("paramresult");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    for (oid, value, text) in [
        (700, &b"1.5"[..], "1.5"),
        (16, b"t", "t"),
        (23, b"42", "42"),
        (25, b"x", "x"),
    ] {
        let r = a.xt("SELECT $1", &[(oid, 0, value)]).ok("SELECT $1");
        assert_eq!((r.oids, r.rows), (Some(vec![oid]), vec![vec![t(text)]]), "declared {oid}");
    }
    let r = a.describe_statement("SELECT $1::int4 + 1, $1").ok("describe");
    assert_eq!(r.oids, Some(vec![23, 23]), "the second column is the parameter, inferred int4");
}

/// A result column whose expression is boolean in PostgreSQL's grammar (a boolean literal, a
/// comparison, AND / OR / NOT, IS [NOT] NULL, IN, BETWEEN, LIKE, EXISTS, a cast to bool, a CASE
/// whose every result is boolean) is announced bool (16) and sent as t / f, and a cast to float4 or
/// real is announced float4 (700), on both protocols, as PostgreSQL 18 announces them (expected
/// values from its recording, artie lanes/wire/raw/loud-20261015/pg18-diff/pg18-types.out, never
/// from this server). They went out as int4 or text holding 1 and 0, and float4 casts as float8: a
/// driver read a boolean as a number or a string (found by the PG 18 differential; the lead ruled
/// them FIXED, 2026-10-15T06:03:10Z).
#[test]
fn boolean_and_float4_results_are_typed_as_postgresql_types_them() {
    let dir = Scratch::new("resulttypes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE k(id INT, flag BOOLEAN, r REAL, d DOUBLE PRECISION)")
        .ok("k");
    a.q("INSERT INTO k VALUES (2, true, 1.5, 2.5)").ok("row");
    let t = |s: &str| Some(s.to_string());
    for (sql, oids, row) in [
        ("SELECT true, false", vec![16, 16], vec![t("t"), t("f")]),
        (
            "SELECT 1 = 1, 1 <> 1, 1 < 2, 2 >= 3",
            vec![16, 16, 16, 16],
            vec![t("t"), t("f"), t("t"), t("f")],
        ),
        (
            "SELECT EXISTS (SELECT 1), NOT EXISTS (SELECT 1)",
            vec![16, 16],
            vec![t("t"), t("f")],
        ),
        (
            "SELECT NOT true, true AND false, true OR false",
            vec![16, 16, 16],
            vec![t("f"), t("f"), t("t")],
        ),
        (
            "SELECT id > 1, flag, flag = true, id IS NULL, id IS NOT NULL FROM k",
            vec![16, 16, 16, 16, 16],
            vec![t("t"), t("t"), t("t"), t("f"), t("t")],
        ),
        (
            "SELECT id IN (1, 2), id BETWEEN 1 AND 3, 'a' LIKE 'a%' FROM k",
            vec![16, 16, 16],
            vec![t("t"), t("t"), t("t")],
        ),
        (
            "SELECT CASE WHEN id > 1 THEN true ELSE false END FROM k",
            vec![16],
            vec![t("t")],
        ),
        (
            "SELECT d::float4, 1.5::float4, 1.5::real, 2.5::float8 FROM k",
            vec![700, 700, 700, 701],
            vec![t("2.5"), t("1.5"), t("1.5"), t("2.5")],
        ),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(r.oids, Some(oids.clone()), "{sql}: simple");
        assert_eq!(r.rows, vec![row.clone()], "{sql}: simple");
        let r = a.xt(sql, &[]).ok(sql);
        assert_eq!(r.oids, Some(oids), "{sql}: extended");
        assert_eq!(r.rows, vec![row], "{sql}: extended");
    }
    let r = a.xt("SELECT $1::bool", &[(16, 0, b"true")]).ok("$1::bool");
    assert_eq!((r.oids, r.rows), (Some(vec![16]), vec![vec![t("t")]]));
    let r = a.describe_statement("SELECT id > $1 FROM k").ok("describe");
    assert_eq!(r.oids, Some(vec![16]), "a comparison over a parameter");
}

/// A float8 math function types its result as PostgreSQL resolves it, so an arithmetic over it and
/// a parameter compared with that are float8: `sqrt(2) + 1` announces 701 and `sqrt(2) + 1 > $1`
/// types $1 float8, as PostgreSQL 18.6 does (recorded, artie raw
/// loud-20261015/pg18-diff/pg18-r14.out), so a value '1.5' runs. RULED SPEC CHANGE (lead, DECISIONS
/// 9d8c1c38ef, item 17): this test was `an_operand_the_walk_cannot_type_types_nothing` and pinned
/// the 42P18 refusal, a divergence. A parameter operand still takes the other side's type
/// (`$1 + 1 > 2`, int4) (wire review 14 item 17).
#[test]
fn a_float8_math_function_types_its_comparison_parameter() {
    let dir = Scratch::new("untypedoperand");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let t = |s: &str| Some(s.to_string());
    let r = a.q("SELECT sqrt(2) + 1").ok("result");
    assert_eq!(r.oids, Some(vec![701]), "sqrt(2) + 1: rows {:?}", r.rows);
    let sql = "SELECT 1 WHERE sqrt(2) + 1 > $1";
    let r = a.describe_statement(sql).ok(sql);
    assert_eq!(r.params, Some(vec![701]), "{sql}");
    let r = a.xt(sql, &[(0, 0, b"1.5")]).ok("1.5");
    assert_eq!(r.rows, vec![vec![t("1")]], "{sql} with 1.5");
    let r = a.describe_statement("SELECT 1 WHERE $1 + 1 > 2").ok("control");
    assert_eq!(r.params, Some(vec![23]), "a parameter operand takes the other side's type");
}

/// INSERT, UPDATE and DELETE with RETURNING complete with their own verb's tag and row count, as
/// PostgreSQL 18 answers them (`INSERT 0 n`, `UPDATE n`, `DELETE n`; recorded in artie
/// lanes/wire/raw/loud-20261015/pg18-diff), on both protocols: drivers read the affected count from
/// the tag. Their rows went out through the query path, whose tag is `SELECT n` (found by the
/// PG 18 differential of 2026-10-15).
#[test]
fn dml_returning_is_tagged_by_its_verb() {
    let dir = Scratch::new("returningtag");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE r(id INT PRIMARY KEY, v TEXT)").ok("r");
    for (sql, tag) in [
        ("INSERT INTO r VALUES (1, 'a'), (2, 'b') RETURNING id", "INSERT 0 2"),
        ("UPDATE r SET v = 'c' WHERE id = 1 RETURNING v", "UPDATE 1"),
        ("DELETE FROM r WHERE id = 2 RETURNING id", "DELETE 1"),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(r.tags, vec![tag.to_string()], "{sql}: rows {:?}", r.rows);
    }
    for (sql, tag) in [
        ("INSERT INTO r VALUES ($1, 'x') RETURNING id", "INSERT 0 1"),
        ("UPDATE r SET v = 'y' WHERE id = $1 RETURNING v", "UPDATE 1"),
        ("DELETE FROM r WHERE id = $1 RETURNING id", "DELETE 1"),
    ] {
        let r = a.xt(sql, &[(23, 0, b"7")]).ok(sql);
        assert_eq!(r.tags, vec![tag.to_string()], "extended {sql}: rows {:?}", r.rows);
    }
}

/// An integer literal in base 2, 8 or 16, or with underscores, past int4 (PostgreSQL 16 and later)
/// is its value, as PostgreSQL 18.6 answers (recorded, artie raw loud-20261015/pg18-diff/
/// pg18-num.out), and never ends the session: `SELECT 0b10000000000000000000000000000000` reached
/// the engine as that text, whose literal parser assumes SQLite's grammar checked it and hit
/// unreachable!(), closing the connection (found by the PostgreSQL regression corpus,
/// numerology.sql, 2026-10-15). Each literal's column type is PostgreSQL's too (Claude C33: red
/// a7aac0094 had dropped the recorded types; recorded again with every case, pg18-diff/pg18-c33.out):
/// int4 (23) within int4, int8 (20) past it, numeric (1700) past int8, with the exact value.
#[test]
fn a_non_decimal_integer_literal_is_its_value() {
    let dir = Scratch::new("numlit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (sql, oid, want) in [
        ("SELECT 0b10000000000000000000000000000000", 20, "2147483648"),
        ("SELECT 0o20000000000", 20, "2147483648"),
        ("SELECT 0x80000000", 20, "2147483648"),
        ("SELECT -0x8000000000000000", 20, "-9223372036854775808"),
        ("SELECT 1_000_000", 23, "1000000"),
        ("SELECT 0b1_0", 23, "2"),
        ("SELECT 0x_7FFF_FFFF_FFFF_FFFF_F", 1700, "147573952589676412927"),
        ("SELECT 2147483647", 23, "2147483647"),
        ("SELECT 2147483648", 20, "2147483648"),
        ("SELECT 9223372036854775807", 20, "9223372036854775807"),
        ("SELECT 9223372036854775808", 1700, "9223372036854775808"),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(
            (r.oids.clone(), r.rows.clone()),
            (Some(vec![oid]), vec![vec![Some(want.to_string())]]),
            "{sql}"
        );
    }
    assert_eq!(a.q("SELECT 1").single("the session serves on"), "1");
}

/// A negative zero literal is written as PostgreSQL 18.6 writes it (recorded, pg18-diff/
/// pg18-c33.out): an integer -0 and a numeric -0.0 or '-0' have no sign (0, 0.0, 0), a float8's
/// keeps it (-0). Claude C33: since 27d41a19a the numeric literal read '-0'.
#[test]
fn a_negative_zero_literal_is_written_as_postgresql_writes_it() {
    let dir = Scratch::new("negzero");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (sql, oid, want) in [
        ("SELECT -0", 23, "0"),
        ("SELECT -0.0", 1700, "0.0"),
        ("SELECT -0.0::float8", 701, "-0"),
        ("SELECT '-0'::float8", 701, "-0"),
        ("SELECT '-0'::numeric", 1700, "0"),
    ] {
        let r = a.q(sql).ok(sql);
        assert_eq!(
            (r.oids.clone(), r.rows.clone()),
            (Some(vec![oid]), vec![vec![Some(want.to_string())]]),
            "{sql}"
        );
    }
}

/// A real (float4) value out of float4's range is refused 22003 at storage and at a cast, with
/// PostgreSQL's message, as float4in refuses it (recorded, pg18-diff/pg18-c33.out, every case);
/// 3.4e38 fits. Claude C33: ours stored Infinity or 0.
#[test]
fn a_real_out_of_range_is_22003_at_storage_and_at_a_cast() {
    let dir = Scratch::new("realrange");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("create table rr (x real)").ok("rr");
    for (sql, message) in [
        ("insert into rr values (1e39)", "\"1000000000000000000000000000000000000000\" is out of range for type real"),
        ("insert into rr values ('1e39')", "\"1e39\" is out of range for type real"),
        ("insert into rr values (-1e39)", "\"-1000000000000000000000000000000000000000\" is out of range for type real"),
        (
            "insert into rr values (1e-50)",
            "\"0.00000000000000000000000000000000000000000000000001\" is out of range for type real",
        ),
        ("SELECT CAST(1e39 AS real)", "\"1000000000000000000000000000000000000000\" is out of range for type real"),
        ("SELECT 1e39::real", "\"1000000000000000000000000000000000000000\" is out of range for type real"),
        ("SELECT '1e39'::real", "\"1e39\" is out of range for type real"),
    ] {
        let e = a.q(sql).err(sql);
        assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", message), "{sql}");
    }
    let r = a.q("select * from rr").ok("rr after");
    assert_eq!((r.oids.clone(), r.rows.len()), (Some(vec![700]), 0));
    let r = a.q("SELECT '3.4e38'::real").ok("3.4e38");
    assert_eq!(
        (r.oids.clone(), r.rows.clone()),
        (Some(vec![700]), vec![vec![Some("3.4e+38".to_string())]])
    );
}

/// A CREATE TABLE with a SERIAL column completes as CREATE TABLE, with no row description, on both
/// protocols, as PostgreSQL answers it. Since 0693b6c6d performs such a CREATE at prepare (with its
/// sequence, in one transaction) and steps a no-op, the no-op's one column made the server answer it
/// as a query: a RowDescription, 0 rows and `SELECT 0` (found by the PostgreSQL regression corpus,
/// uuid.out, 2026-10-15).
#[test]
fn a_create_table_with_a_serial_completes_as_create_table() {
    let dir = Scratch::new("serialtag");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let r = a.q("CREATE TABLE s1(id SERIAL, v INT)").ok("simple");
    assert_eq!((r.oids, r.tags), (None, vec!["CREATE TABLE".to_string()]), "simple");
    let r = a.xt("CREATE TABLE s2(id SERIAL, v INT)", &[]).ok("extended");
    assert_eq!((r.oids, r.tags), (None, vec!["CREATE TABLE".to_string()]), "extended");
}

/// A refused CREATE TABLE leaves nothing behind, its SERIAL column's sequence included, as in
/// PostgreSQL: one whose key onto itself does not resolve (42830, after the CREATE), and one whose
/// table already exists. The sequence's `CREATE SEQUENCE IF NOT EXISTS` ran and committed at prepare,
/// before the CREATE's own transaction began, so each refusal left a sequence (wire review 16 item
/// 15). The controls: the same CREATE that succeeds adds its one sequence.
#[test]
fn a_refused_create_table_leaves_no_sequence() {
    let dir = Scratch::new("serialundo");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sequences = |a: &mut Wire| {
        a.q("SELECT count(*) FROM pg_sequences")
            .single("pg_sequences")
    };
    a.q("CREATE TABLE p(id INT)").ok("p");
    let before = sequences(&mut a);
    let keyed = "CREATE TABLE tree(id SERIAL PRIMARY KEY, code INT, parent INT REFERENCES tree(code))";
    assert_eq!(a.q(keyed).err(keyed).code, "42830", "premise: code is not unique");
    assert_eq!(sequences(&mut a), before, "the refused tree left its sequence");
    let exists = "CREATE TABLE p(id SERIAL)";
    a.q(exists).err(exists);
    assert_eq!(sequences(&mut a), before, "the refused p left its sequence");
    a.q("CREATE TABLE tree(id SERIAL PRIMARY KEY, code INT UNIQUE, parent INT REFERENCES tree(code))")
        .ok("control: tree with a unique code");
    let after: u64 = sequences(&mut a).parse().unwrap();
    assert_eq!(
        after,
        before.parse::<u64>().unwrap() + 1,
        "control: the CREATE that succeeds adds one sequence"
    );
}

/// The settings whose effect a ROLLBACK would have to undo, foreign_keys and search_path, are
/// refused (0A000) inside an explicit transaction block and as SET LOCAL, as this server keeps a
/// SET past the block whatever ends it: `BEGIN; SET foreign_keys = off; ROLLBACK` left keys off for
/// the session, and `BEGIN; SET search_path TO s, public; ROLLBACK` sent later unqualified writes
/// to s (wire review 16 item 18; review 17 folded search_path into it). Each refusal fails its
/// block, as any error does; outside a block, and in a multi-statement query's implicit block,
/// both still act.
#[test]
fn a_setting_a_rollback_cannot_undo_is_refused_in_a_block() {
    let dir = Scratch::new("setinblock");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("p");
    a.q("CREATE TABLE c(x INT REFERENCES p(id))").ok("c");
    for sql in ["SET foreign_keys = off", "SET search_path TO s, public"] {
        a.q("BEGIN").ok("begin");
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "0A000", "{sql} in a block");
        assert_eq!(r.status, b'E', "{sql} fails the block");
        a.q("ROLLBACK").ok("end");
    }
    let orphan = "INSERT INTO c VALUES (5)";
    assert_eq!(a.q(orphan).err(orphan).code, "23503", "keys still on");
    for sql in [
        "SET LOCAL foreign_keys = off",
        "SET LOCAL search_path TO public",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    }
    a.q("SET search_path TO public; SELECT 1")
        .ok("in a multi-statement query's implicit block");
    a.q("SET search_path TO public").ok("outside a block");
}

/// A pg_dump -s header's settings each answer SET: those PostgreSQL 17 and 18 dumps write, one by
/// one (the list is recalled from pg_dump's output, not recorded here; the owed run replays the
/// BranchBench dump through psql). xmloption, transaction_timeout, default_tablespace and
/// default_table_access_method answered 42704 since the SET allowlist (wire review 16 item 17).
/// Each is accepted only at the value that changes nothing here, 0A000 at another; a name
/// PostgreSQL knows that this server does not act on is 0A000 for SET and SHOW alike, not 42704
/// "unrecognized" (the name list is PostgreSQL 18.6's settings table), and a name PostgreSQL does
/// not know stays 42704.
#[test]
fn a_dump_headers_settings_are_accepted() {
    let dir = Scratch::new("dumpheader");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in [
        "SET statement_timeout = 0",
        "SET lock_timeout = 0",
        "SET idle_in_transaction_session_timeout = 0",
        "SET transaction_timeout = 0",
        "SET client_encoding = 'UTF8'",
        "SET standard_conforming_strings = on",
        "SET check_function_bodies = false",
        "SET xmloption = content",
        "SET client_min_messages = warning",
        "SET row_security = off",
        "SET default_tablespace = ''",
        "SET default_table_access_method = heap",
    ] {
        a.q(sql).ok(sql);
    }
    for (sql, code) in [
        ("SET xmloption = document", "0A000"),
        ("SET transaction_timeout = '5s'", "0A000"),
        ("SET default_tablespace = 'fast'", "0A000"),
        ("SET default_table_access_method = columnar", "0A000"),
        ("SET geqo_threshold = 12", "0A000"),
        ("SHOW geqo_threshold", "0A000"),
        ("SET no_such_parameter = 1", "42704"),
        ("SHOW no_such_parameter", "42704"),
    ] {
        assert_eq!(a.q(sql).err(sql).code, code, "{sql}");
    }
}

/// A client's value never chooses its error's SQLSTATE: a SET refused for its value is 0A000
/// whatever the value says, `'is ambiguous'` and `'specified more than once'` included. The
/// mapping matched those words anywhere in the message, which embeds the value, so `SET
/// bytea_output = 'is ambiguous'` answered 42702 (wire review 16 item 20).
#[test]
fn a_clients_value_never_chooses_the_sqlstate() {
    let dir = Scratch::new("valuecode");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    // C44 lead ruling: invalid values are PG 18.6's 22023. The valid escape mode remains
    // an unsupported feature here; its existing 0A000 expectation is unchanged.
    // Fresh oracle: c44-pg18-errors-r2.txt beside this test.
    for (sql, expected) in [
        ("SET bytea_output = 'is ambiguous'", "22023"),
        ("SET bytea_output = 'specified more than once'", "22023"),
        ("SET bytea_output = 'escape'", "0A000"),
    ] {
        assert_eq!(a.q(sql).err(sql).code, expected, "{sql}");
    }
}

/// SET reads every value it is given, as PostgreSQL does: a setting PostgreSQL reads as one value
/// takes one, so `SET client_encoding TO UTF8, LATIN1` and `SET TimeZone TO UTC, GMT` are 22023
/// "SET <name> takes only one argument" (its flatten_set_variable_args, guc_funcs.c:233, read),
/// and DateStyle, a list there, is read whole: `SET DateStyle TO ISO, DMY` is 0A000 and `SET
/// DateStyle TO ISO, MDY` keeps `ISO, MDY`. Only the first value was read, so each of these
/// answered SET on its first (wire review 18 item 19).
#[test]
fn set_reads_every_value_it_is_given() {
    let dir = Scratch::new("setvalues");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in [
        "SET client_encoding TO UTF8, LATIN1",
        "SET TimeZone TO UTC, GMT",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "22023", "{sql}");
    }
    let sql = "SET DateStyle TO ISO, DMY";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    a.q("SET DateStyle TO ISO, MDY")
        .ok("a list that keeps ISO, MDY");
    assert_eq!(a.q("SHOW DateStyle").single("DateStyle"), "ISO, MDY");
}

/// encode(bytea, format) answers as PostgreSQL's does: 'abc' in hex is 616263 and 'ab' in base64
/// YWI=, a NULL argument is NULL, and an unknown format is 22023 "unrecognized encoding". It was no
/// function at all (42883), found by the cloud run of bind_reads_each_parameter_in_its_format.
#[test]
fn encode_answers_as_postgresql_does() {
    let dir = Scratch::new("encode");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    assert_eq!(a.q("SELECT encode('abc', 'hex')").single("hex"), "616263");
    assert_eq!(
        a.q("SELECT encode('ab', 'base64')").single("base64"),
        "YWI="
    );
    let r = a.q("SELECT encode(NULL, 'hex')").ok("NULL");
    assert_eq!(r.rows, vec![vec![None]]);
    let sql = "SELECT encode('abc', 'nosuch')";
    assert_eq!(a.q(sql).err(sql).code, "22023");
}

/// The parameters SET accepted as no-ops but which change PostgreSQL's answers act as they do
/// there: `SET lock_timeout = '100ms'` makes a branch create behind another session's write
/// transaction fail 55P03 within a second, not wait out the server's lock wait (5 s here, 60 s
/// by default); idle_in_transaction_session_timeout is accepted at 0 alone (another value is
/// 0A000, as this server does not enforce it); and client_min_messages filters the
/// warnings a session is sent (COMMIT outside a block warns 25P01 at the default, at warning, not
/// at error). Each answered SET and changed nothing (wire review 16 item 12).
#[test]
fn settings_that_change_answers_act_as_in_postgresql() {
    let dir = Scratch::new("settings");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "5000"]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')")
        .ok("a write transaction holds the lock");
    b.q("SET lock_timeout = '100ms'").ok("lock_timeout");
    let started = Instant::now();
    let r = b.q("SELECT turso_branch_create('x')");
    let took = started.elapsed();
    assert_eq!(r.err("create behind the lock").code, "55P03");
    assert!(took < Duration::from_secs(1), "the create waited {took:?}");
    a.q("ROLLBACK").ok("release");
    // FLAGGED TEST EDIT (lead's cancel check, 2026-10-15): statement_timeout is enforced now
    // (a_long_statement_stops_on_cancel_and_on_statement_timeout), so its refusal at a nonzero
    // value is dropped from this list; the refusal pinned "this server enforces no such timeout".
    for sql in ["SET idle_in_transaction_session_timeout = '5s'"] {
        assert_eq!(b.q(sql).err(sql).code, "0A000", "{sql}");
    }
    b.q("SET statement_timeout = 0").ok("statement_timeout 0");
    let warned = |w: &mut Wire| w.q("COMMIT").ok("COMMIT outside a block").notices.len();
    assert_eq!(warned(&mut b), 1, "the default level sends the warning");
    b.q("SET client_min_messages = warning").ok("warning");
    assert_eq!(warned(&mut b), 1, "at warning");
    b.q("SET client_min_messages = error").ok("error");
    assert_eq!(warned(&mut b), 0, "at error no warning is sent");
}

/// The failure triggers the failure-path tests use do fail, each with its code, so a test that
/// fails a statement or a block takes its failure path: a missing relation at prepare (42P01; in a
/// block the block fails) and a duplicate key at execution (23505). `SELECT 1/0`, which they used,
/// is one row holding NULL on this engine, so those tests never failed anything (wire review 16
/// item 2; the divergence itself is integer_division_by_zero_is_22012).
#[test]
fn the_failure_triggers_fail() {
    let dir = Scratch::new("triggers");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let sql = "SELECT * FROM nosuch";
    assert_eq!(a.q(sql).err(sql).code, "42P01");
    a.q("BEGIN").ok("begin");
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "42P01");
    assert_eq!(r.status, b'E', "{sql} did not fail the block");
    a.q("ROLLBACK").ok("rollback");
    let sql = "INSERT INTO t VALUES (1, 'dup')";
    assert_eq!(a.q(sql).err(sql).code, "23505");
}

/// In a failed block Parse refuses any statement but a block exit, 25P02, with no ParseComplete
/// before it and before anything reads the statement's parameters, as PostgreSQL's
/// exec_parse_message refuses it ahead of parse analysis (postgres.c:1522-1528 at REL_19_BETA1-820,
/// read). Parse succeeded for every statement, so CHECKPOINT declared one unspecified parameter
/// answered 42P18 at Bind ahead of the 25P02 (wire review 17 item 8), and ParseComplete went out
/// for a statement PostgreSQL never parses. A block exit still parses and ends the block.
#[test]
fn a_failed_block_refuses_at_parse() {
    let dir = Scratch::new("abortedparse");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse (with `oids` declared), Bind (no values), Execute, Sync: every message type byte up to
    // ReadyForQuery, the first error and the status ReadyForQuery carries.
    fn round(w: &mut Wire, sql: &str, oids: &[u32]) -> (Vec<u8>, Option<WireError>, u8) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for oid in oids {
            parse.extend_from_slice(&oid.to_be_bytes());
        }
        w.send(b'P', &parse);
        w.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        let (mut types, mut error) = (Vec::new(), None);
        loop {
            let mut head = [0u8; 5];
            w.s.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            w.s.read_exact(&mut body).unwrap();
            types.push(head[0]);
            if head[0] == b'E' && error.is_none() {
                error = Some(error_fields(&body));
            }
            if head[0] == b'Z' {
                return (types, error, body[0]);
            }
        }
    }
    a.q("BEGIN").ok("begin");
    let fail = "SELECT * FROM nosuch";
    assert_eq!(a.q(fail).status, b'E', "{fail} did not fail the block");
    for (sql, oids) in [
        ("CHECKPOINT", &[0u32][..]),
        ("SELECT 1", &[0][..]),
        ("SELECT v FROM t", &[][..]),
    ] {
        let (types, error, status) = round(&mut a, sql, oids);
        assert_eq!(error.map(|e| e.code), Some("25P02".to_string()), "{sql}");
        assert_eq!(types, vec![b'E', b'Z'], "{sql}: nothing before the refusal");
        assert_eq!(status, b'E', "{sql}");
    }
    let (types, error, status) = round(&mut a, "ROLLBACK", &[]);
    assert!(error.is_none(), "a block exit parses: {error:?}");
    assert_eq!(types, vec![b'1', b'2', b'C', b'Z'], "ROLLBACK");
    assert_eq!(status, b'I', "ROLLBACK ended the block");
}

/// A block exit bound with a value in a failed block is refused at the Bind, 25P02, with no
/// BindComplete before it, as PostgreSQL's exec_bind_message refuses it (numParams != 0), and the
/// block stays failed; with no value the same ROLLBACK binds and ends the block. Parse refuses
/// every other statement there (a_failed_block_refuses_at_parse), so this is the Bind's own
/// refusal. The 25P02 rows before asserted the code alone, which a refusal after BindComplete
/// gives too (wire review 17 item 13). The mutant that drops `|| !message.parameters.is_empty()`
/// from on_bind's aborted check must turn this red.
#[test]
fn a_block_exit_bound_with_a_value_is_refused_before_bind_complete() {
    let dir = Scratch::new("abortedbind");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse ROLLBACK declaring `values.len()` text parameters, Bind them, Execute, Sync: every
    // message type byte up to ReadyForQuery, the first error and the status.
    fn round(w: &mut Wire, values: &[&[u8]]) -> (Vec<u8>, Option<WireError>, u8) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(b"ROLLBACK\0");
        parse.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for _ in values {
            parse.extend_from_slice(&25u32.to_be_bytes());
        }
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8, 0, 0];
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
            bind.extend_from_slice(v);
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        let (mut types, mut error) = (Vec::new(), None);
        loop {
            let mut head = [0u8; 5];
            w.s.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            w.s.read_exact(&mut body).unwrap();
            types.push(head[0]);
            if head[0] == b'E' && error.is_none() {
                error = Some(error_fields(&body));
            }
            if head[0] == b'Z' {
                return (types, error, body[0]);
            }
        }
    }
    a.q("BEGIN").ok("begin");
    let fail = "SELECT * FROM nosuch";
    assert_eq!(a.q(fail).status, b'E', "{fail} did not fail the block");
    let (types, error, status) = round(&mut a, &[b"x"]);
    assert_eq!(error.map(|e| e.code), Some("25P02".to_string()));
    assert_eq!(
        types,
        vec![b'1', b'E', b'Z'],
        "no BindComplete before the refusal"
    );
    assert_eq!(status, b'E', "the block is still failed");
    let (types, error, status) = round(&mut a, &[]);
    assert!(error.is_none(), "ROLLBACK with no value: {error:?}");
    assert_eq!(types, vec![b'1', b'2', b'C', b'Z']);
    assert_eq!(status, b'I', "ROLLBACK ended the block");
}

/// KNOWN RED (known-red.txt; E5-QUEUE M1): an integer division by zero is 22012 "division by zero"
/// in PostgreSQL. This engine answers one row holding NULL: core's exec_divide sends an integer /0
/// to the float path, whose checked division answers None, and the PostgreSQL frontend maps `/` to
/// the engine's Divide (wire review 16 item 2). Green when the engine, or a frontend override of
/// `/`, raises it.
#[test]
fn integer_division_by_zero_is_22012() {
    let dir = Scratch::new("div0");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "SELECT 1/0";
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "22012", "rows {:?}", r.rows);
}

/// A NUMERIC division by zero is 22012 "division by zero", as PostgreSQL answers: the engine's
/// NUMERIC divide refuses a zero divisor as a constraint error with those words, which was mapped
/// with every other constraint error to 23000, integrity_constraint_violation, so a driver raised an
/// IntegrityError (wire review 16 item 2, the wire's half; the integer half is the engine's,
/// integer_division_by_zero_is_22012). The column is NUMERIC(10,2): only numeric(p,s) reaches the
/// engine's NUMERIC divide, as a bare NUMERIC is REAL here (wire review 18 HIGH 2, a test change
/// the lead ruled at 2026-10-15T02:59:32Z; bare NUMERIC's own divergence is the known red
/// a_bare_numeric_division_by_zero_is_22012).
#[test]
fn a_numeric_division_by_zero_is_22012() {
    let dir = Scratch::new("numdiv0");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE nd(x NUMERIC(10,2))").ok("nd");
    a.q("INSERT INTO nd VALUES (1.5)").ok("row");
    let sql = "SELECT x / 0 FROM nd";
    let e = a.q(sql).err(sql);
    assert_eq!(
        e.message, "division by zero",
        "premise: the engine's NUMERIC divide refused the zero divisor"
    );
    assert_eq!(e.code, "22012");
}

/// KNOWN RED (known-red.txt; E5-QUEUE M1b): a bare NUMERIC's division by zero is 22012
/// "division by zero" in PostgreSQL; here a bare NUMERIC is REAL (the translator's type map), so
/// x / 0 answers one row holding NULL, as an integer's does (the cloud run at abefd2e71: "expected
/// an error, got rows [[None]]"). It keeps the divergence a_numeric_division_by_zero_is_22012
/// exposed watched after that test moved to NUMERIC(10,2) (wire review 18 HIGH 2; the lead's
/// condition, 2026-10-15T02:59:32Z). Green when a bare NUMERIC is a numeric or its divide by zero
/// raises 22012.
#[test]
fn a_bare_numeric_division_by_zero_is_22012() {
    let dir = Scratch::new("barenumdiv0");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE nd(x NUMERIC)").ok("nd");
    a.q("INSERT INTO nd VALUES (1.5)").ok("row");
    let sql = "SELECT x / 0 FROM nd";
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "22012", "rows {:?}", r.rows);
}

/// KNOWN RED (known-red.txt; E5-QUEUE M1): an integer modulo by zero is 22012 in PostgreSQL; this
/// engine answers one row holding NULL, as for `/` (wire review 16 item 2). Green with the engine's
/// fix for integer_division_by_zero_is_22012.
#[test]
fn integer_modulo_by_zero_is_22012() {
    let dir = Scratch::new("mod0");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "SELECT 1 % 0";
    assert_eq!(a.q(sql).err(sql).code, "22012");
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
/// counted by the server itself (turso_branch_stats' unix_syscalls), which only an Apple build
/// counts, so the test is Apple's alone, as branch_stats_reports_the_servers_counters is (ruled
/// c9c821dd8d, P5).
#[cfg(target_vendor = "apple")]
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
/// the highest $n used, each typed as PostgreSQL infers it: from the column it is compared with
/// (int4 for id), text for the branch call's name. At 472023b72 it reported only the declared
/// ones, so asyncpg, tokio-postgres and pgx (which declare none) saw no parameter and refused to
/// bind (wire review 1 item 13). A parameter below the highest that the statement does not hold
/// is 42P18, as PostgreSQL's Parse refuses a parameter it cannot type. The expected values were
/// text throughout until the context typing landed; moved to PostgreSQL's answers, PROVISIONALLY
/// (ruled c9c821dd8d, P6): a PostgreSQL 18 recording is LOUD-OWED, and if it differs the edit is
/// reverted.
#[test]
fn describe_statement_reports_undeclared_parameters() {
    let dir = Scratch::new("describeparams");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    const TEXT: u32 = 25;
    const INT4: u32 = 23;
    for (sql, want) in [
        ("SELECT turso_branch_create($1)", vec![TEXT]),
        ("SELECT v FROM t WHERE id = $1", vec![INT4]),
        ("SELECT v FROM t", vec![]),
    ] {
        let r = a.describe_statement(sql).ok(sql);
        assert_eq!(r.params, Some(want), "{sql}");
    }
    let gap = "SELECT v FROM t WHERE id = $2";
    let r = a.describe_statement(gap);
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("42P18"),
        "{gap}: $1 is not in the statement and nothing types it: {:?}",
        r.params
    );
}

/// Wire review 18 item 14, the deep arm: over v1..v1000, each reading the one before, a Describe
/// of `SELECT * FROM v1000 WHERE x = $1` is refused 54001, cold and again, and the session and the
/// server still answer after (a stack overflow on a session thread aborts the whole process). The
/// parameter-type walk cut every view nested 32 deep, so the cold Describe was 42P18. The engine's
/// own reading of the chain, at CREATE VIEW and at prepare, is core's and has not been run at this
/// depth: if this arm dies there, the finding is the engine's.
#[test]
fn a_thousand_view_chain_is_typed_and_the_server_survives() {
    let dir = Scratch::new("viewchain");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT PRIMARY KEY)").ok("t");
    let views = (1..=1000)
        .map(|k| match k {
            1 => "CREATE VIEW v1 AS SELECT id AS x FROM t".to_string(),
            k => format!("CREATE VIEW v{k} AS SELECT x FROM v{p}", p = k - 1),
        })
        .collect::<Vec<_>>()
        .join("; ");
    a.q(&views).ok("the chain");
    // FLAGGED RULED SPEC CHANGE (lead, view cap ruling, 2026-10-15): the engine bounds a view
    // chain by the stack it has, so a chain this deep is refused 54001 (PostgreSQL's stack depth
    // limit), cleanly, and the session and the server live on; this asserted `$1` int4.
    let sql = "SELECT * FROM v1000 WHERE x = $1";
    for round in ["cold", "warm"] {
        let r = a.describe_statement(sql);
        assert_eq!(
            r.error.as_ref().map(|e| e.code.as_str()),
            Some("54001"),
            "{round}: {:?}",
            r.params
        );
    }
    a.q("SELECT 1").ok("the session still answers");
    server
        .connect()
        .q("SELECT 1")
        .ok("the server still answers");
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

/// CREATE INDEX on a schema-qualified table indexes that table, in its schema, as PostgreSQL does:
/// a UNIQUE index on s.t refuses a duplicate in s.t (23505) and leaves public.t's duplicates
/// alone; one on public.t refuses public.t's. The translator dropped the table's schema, so the
/// index went onto whichever t the unqualified name found (public's, with the default path): s.t
/// took duplicates its UNIQUE index forbids, and public.t refused rows nothing forbids. Schema s is
/// made by the non-server CLI before the server starts. Found writing wire review 17 item 1's fix.
#[test]
fn an_index_on_a_qualified_table_is_that_tables() {
    let dir = Scratch::new("qualindex");
    cli_script(
        &dir,
        b"CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.t(id INT PRIMARY KEY, c INT);\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE UNIQUE INDEX su ON s.t (c)").ok("index on s.t");
    a.q("INSERT INTO s.t VALUES (1, 7)").ok("s.t row");
    let sql = "INSERT INTO s.t VALUES (2, 7)";
    assert_eq!(
        a.q(sql).err("a duplicate in s.t").code,
        "23505",
        "s.t's UNIQUE index did not hold: {sql}"
    );
    a.q("INSERT INTO public.t VALUES (1, 7)").ok("public.t row");
    a.q("INSERT INTO public.t VALUES (2, 7)")
        .ok("public.t's duplicate, which no index forbids");
    // Control: an index on public.t holds there.
    a.q("DELETE FROM public.t WHERE id = 2")
        .ok("drop the duplicate");
    a.q("CREATE UNIQUE INDEX pu ON public.t (c)")
        .ok("index on public.t");
    let sql = "INSERT INTO public.t VALUES (3, 7)";
    assert_eq!(a.q(sql).err(sql).code, "23505");
    assert_eq!(
        a.q("SELECT count(*) FROM s.t").single("s.t rows"),
        "1",
        "s.t holds its one row"
    );
}

/// A view over `public.t` (pg_dump's form) types the parameters compared with its columns: the
/// view is stored with the engine's name for public, `main.t`, which the parameter-type walk
/// now reads as public's t, so `c = $1` is int4 (23), as at 0823803c1 and in PostgreSQL. Since
/// 594edd413 translated public to main, the walk read `main` as another schema it cannot open and
/// refused every such parameter 42P18 (wire review 18 item 5).
#[test]
fn a_view_over_public_t_types_its_parameters() {
    let dir = Scratch::new("publicview");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT PRIMARY KEY, c INT)").ok("t");
    a.q("CREATE VIEW v AS SELECT id, c FROM public.t")
        .ok("a view over public.t");
    let r = a.describe_statement("SELECT id FROM v WHERE c = $1");
    assert_eq!(r.params, Some(vec![23]), "{:?}", r.error);
}

/// A three-part column reference resolves through its schema as the two-part form does: with
/// pg_class in FROM, `pg_catalog.pg_class.relname = $1` types `$1` exactly as `pg_class.relname
/// = $1` does (PostgreSQL types both name, 19; this catalog declares relname text, a separate
/// divergence), and `public.t.c` with t aliased away (FROM t AS x) names no FROM entry, 42P01, as
/// PostgreSQL's errorMissingRTE answers. The three-part path hard-coded public, so the pg_catalog
/// form was refused 42P18 (wire review 17 item 5).
#[test]
fn a_three_part_reference_resolves_through_its_schema() {
    let dir = Scratch::new("threepart");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT PRIMARY KEY, c INT)").ok("t");
    let two = a.describe_statement("SELECT 1 FROM pg_class WHERE pg_class.relname = $1");
    assert!(
        two.error.is_none() && two.params.is_some(),
        "premise: the two-part form is typed: {:?}",
        two.error
    );
    let three =
        a.describe_statement("SELECT 1 FROM pg_class WHERE pg_catalog.pg_class.relname = $1");
    assert_eq!(three.params, two.params, "{:?}", three.error);
    let sql = "SELECT 1 FROM t AS x WHERE public.t.c = $1";
    let r = a.describe_statement(sql);
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("42P01"),
        "{sql}: {:?}",
        r.params
    );
}

/// An unqualified relation is typed from the relation the engine resolves along the search path:
/// an entry that names no attached schema (`"$user"`, PostgreSQL's default, or `nosuch`) is
/// skipped, and an attached schema that does not hold the name is passed over, so under `"$user",
/// public`, `nosuch, public` and `s, public` a Describe over public's orders types its `$1` int4
/// (23), as PostgreSQL does; a named INSERT executed again after each such SET still runs, its $1
/// typed int4 as at its first Execute;
/// and with no path set, a table the engine finds only in attached s (u) is one the walk cannot
/// read, so `$1` into it is refused 42P18, never typed text. The walk stopped at the first entry
/// that was not public, so all three paths refused every parameter (42P18), and with no path it
/// took a name absent from main as main's, typing nothing and falling to text (wire review 17 item
/// 3). Schema s is made by the non-server CLI before the server starts.
#[test]
fn an_unqualified_relation_is_resolved_as_the_engine_resolves_it() {
    let dir = Scratch::new("resolver");
    cli_script(
        &dir,
        b"CREATE TABLE orders(id INT PRIMARY KEY);\n\
          CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.u(id INT PRIMARY KEY, n INT);\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let r = a.describe_statement("UPDATE u SET n = $1");
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("42P18"),
        "u is s's, found by the engine after main: {:?}",
        r.params
    );
    // s1 runs under the default path first (id 3), which fixes its $1 as int4 (id's type).
    a.send(b'P', b"s1\0INSERT INTO t VALUES ($1, 7)\0\0\0");
    a.send(b'B', b"\0s1\0\0\0\0\x01\0\0\0\x013\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert!(
        r.error.is_none(),
        "s1 under the default path: {:?}",
        r.error
    );
    for path in ["\"$user\", public", "nosuch, public", "s, public"] {
        a.q(&format!("SET search_path TO {path}")).ok(path);
        let r = a.describe_statement("SELECT 1 FROM orders WHERE id = $1");
        assert_eq!(r.params, Some(vec![23]), "{path}: {:?}", r.error);
        let id = 10 + path.len();
        let value = format!("{id}");
        let mut bind = b"\0s1\0\0\0\0\x01".to_vec();
        bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
        bind.extend_from_slice(value.as_bytes());
        bind.extend_from_slice(&[0, 0]);
        a.send(b'B', &bind);
        a.send(b'E', &[0, 0, 0, 0, 0]);
        a.send(b'S', &[]);
        let r = a.read_reply();
        assert!(r.error.is_none(), "s1 under {path}: {:?}", r.error);
    }
    a.q("SET search_path TO public").ok("public alone");
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE c = 7").single("rows"),
        "4",
        "s1 ran once per path and once before"
    );
}

/// A foreign key in a schema other than public is refused (0A000), and no table is made: the key
/// checks read public's catalog alone, so `CREATE TABLE s.c(x INT REFERENCES s.p(id))` was refused
/// 42P01 with no public p, refused 42830 naming "<unknown>" with a public p but no public c (and
/// the table rolled back), or committed with its key checked against public's p and never against
/// s.p, every later INSERT then failing "foreign key mismatch" (wire review 16 item 11; DECISIONS:
/// a schema is branched or refused). As ALTER TABLE ADD CONSTRAINT refuses outside public. Schema
/// s is made by the non-server CLI before the server starts.
#[test]
fn a_foreign_key_outside_public_is_refused() {
    let dir = Scratch::new("schemakeys");
    cli_script(&dir, b"CREATE SCHEMA s;\nCREATE TABLE s.p(id INT PRIMARY KEY);\n");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "CREATE TABLE s.c(x INT REFERENCES s.p(id))";
    assert_eq!(a.q(sql).err(sql).code, "0A000", "no public p");
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("a public p");
    assert_eq!(a.q(sql).err(sql).code, "0A000", "with a public p");
    let r = a.q("SELECT count(*) FROM s.c");
    assert!(r.error.is_some(), "s.c was made: {:?}", r.rows);
}

/// An explicit `public.` names public's relation whatever the search path, as in PostgreSQL. With
/// public.t(c int) and s.t(c text) under `SET search_path TO s, public`, a literal INSERT, an UPDATE,
/// a DELETE and an INSERT of an undeclared '007' (typed int4 from public.t, stored 7) all reach
/// public.t and leave s.t as it was, and DROP TABLE public.t drops public.t. The translator dropped
/// the qualifier, so all of them followed the search path into s.t, the '007' stored there typed
/// from public.t; DROP answered "no such database: public" (wire review 17 item 1). Schema s is made
/// by the non-server CLI before the server starts.
#[test]
fn an_explicit_public_qualifier_names_public() {
    let dir = Scratch::new("publicqual");
    cli_script(
        &dir,
        b"CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.t(id INT PRIMARY KEY, c TEXT);\n\
          INSERT INTO s.t VALUES (1, 'one');\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("SET search_path TO s, public").ok("search path");
    a.q("INSERT INTO public.t VALUES (4, 4)")
        .ok("literal insert");
    let r = a.xt("INSERT INTO public.t VALUES (5, $1)", &[(0, 0, b"007")]);
    assert!(r.error.is_none(), "the '007' insert: {:?}", r.error);
    a.q("UPDATE public.t SET c = c + 1 WHERE id = 4")
        .ok("update");
    a.q("DELETE FROM public.t WHERE id = 1").ok("delete");
    assert_eq!(
        a.q("SELECT count(*) || ',' || coalesce(max(c), '') FROM s.t")
            .single("s.t"),
        "1,one",
        "a public.t statement reached s.t"
    );
    assert_eq!(
        a.q("SELECT c FROM public.t WHERE id = 5").single("'007'"),
        "7"
    );
    assert_eq!(a.q("SELECT c FROM public.t WHERE id = 4").single("4"), "5");
    a.q("DROP TABLE public.t").ok("drop public.t");
    let sql = "SELECT * FROM public.t";
    assert_eq!(a.q(sql).err(sql).code, "42P01", "public.t still there");
    assert_eq!(a.q("SELECT count(*) FROM s.t").single("s.t"), "1");
}

/// An aggregate's result type comes from the relation the engine reads, through the search path:
/// with public.t(c int) and s.t(c text), after `SET search_path TO s, public` `SELECT max(c) FROM
/// t` reads s.t, so its column is text (OID 25), on both protocols, as its rows are; `SELECT
/// max(c) FROM public.t` stays int4. aggregate_types read every FROM table in public alone, so the
/// first announced int4 over s.t's text (wire review 17 item 15).
#[test]
fn an_aggregate_is_typed_from_the_relation_the_engine_reads() {
    let dir = Scratch::new("aggpath");
    cli_script(
        &dir,
        b"CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.t(id INT PRIMARY KEY, c TEXT);\n\
          INSERT INTO s.t VALUES (1, 'x');\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("SET search_path TO s, public").ok("search path");
    let sql = "SELECT max(c) FROM t";
    let r = a.q(sql).ok(sql);
    assert_eq!(
        r.rows,
        vec![vec![Some("x".to_string())]],
        "premise: s.t's rows"
    );
    assert_eq!(r.oids, Some(vec![25]), "simple protocol");
    assert_eq!(
        a.x(sql, &[]).ok(sql).oids,
        Some(vec![25]),
        "extended protocol"
    );
    let sql = "SELECT max(c) FROM public.t";
    assert_eq!(a.q(sql).ok(sql).oids, Some(vec![23]), "public.t");
}

/// A parameter only in an ON CONFLICT clause is typed from the target's row, `excluded` included:
/// with public.t(c int) and s.t(c text), `INSERT INTO s.t ... DO UPDATE SET c = $1` and `... DO
/// UPDATE SET c = 'z' WHERE excluded.c = $1` are refused 42P18 (the walk cannot read an attached
/// schema) or typed text, never int4 from public's t; and an UPDATE whose target has a CTE's name
/// in scope is typed from the table (public p(name text): [25]), not the CTE (an int). The ON
/// CONFLICT arm of a_parameter_is_typed_from_the_relation_the_engine_writes put its `$1` in VALUES,
/// which refuses it first, so it never read `excluded` (wire review 17 item 14). The mutant that
/// types `excluded` from rel.relname (public's t) must turn this red.
#[test]
fn a_parameter_in_on_conflict_is_typed_from_the_target() {
    let dir = Scratch::new("onconflict");
    cli_script(
        &dir,
        b"CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE TABLE p(name TEXT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.t(id INT PRIMARY KEY, c TEXT);\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in [
        "INSERT INTO s.t VALUES (2, 'y') ON CONFLICT (id) DO UPDATE SET c = $1",
        "INSERT INTO s.t VALUES (2, 'y') ON CONFLICT (id) DO UPDATE SET c = 'z' WHERE excluded.c = $1",
    ] {
        let r = a.describe_statement(sql);
        let refused = r.error.as_ref().is_some_and(|e| e.code == "42P18");
        assert!(
            refused || r.params == Some(vec![25]),
            "{sql}: {:?} {:?}, want 42P18 or [25], never [23]",
            r.params,
            r.error
        );
    }
    let sql = "WITH p AS (SELECT 1 AS name) UPDATE p SET name = 'x' WHERE name = $1";
    let r = a.describe_statement(sql);
    assert_eq!(r.params, Some(vec![25]), "{sql}: {:?}", r.error);
}

/// A parameter is typed from the relation the engine will write, through its schema and the
/// session's search path: never from public's table of the same name. With public.t(c int) and
/// s.t(c text), an undeclared '007' into s.t (INSERT, UPDATE SET, ON CONFLICT's excluded) or into
/// t after `SET search_path TO s, public` is stored as '007' or refused 42P18 (the walk cannot read
/// an attached schema), never typed int4 and stored as 7; a DML target is the table even when a
/// CTE of its name is in scope; and a three-part `s.t.c` reference with only public's t in FROM
/// names no FROM entry, 42P01 as PostgreSQL's errorMissingRTE answers (FLAGGED edit, the lead's
/// pre-approval of wire review 17 item 5: it asserted 42P18), not text. The
/// walk read every target in public alone (wire review 14 item 3; psycopg3 sends str parameters
/// with OID 0, so it reaches this with default settings). The schema is made by the non-server CLI
/// before the server starts: the server refuses CREATE SCHEMA, and attaches a schema file present
/// at session open.
#[test]
fn a_parameter_is_typed_from_the_relation_the_engine_writes() {
    let dir = Scratch::new("schematarget");
    cli_script(
        &dir,
        b"CREATE TABLE t(id INT PRIMARY KEY, c INT);\n\
          CREATE TABLE p(name TEXT);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.t(id INT PRIMARY KEY, c TEXT);\n",
    );
    assert!(
        dir.0.join("turso-postgres-schema-s.db").exists(),
        "premise: schema s has its file"
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let mut wrong = Vec::new();
    let mut check = |a: &mut Wire, what: &str, sql: &str, value: &[u8], read: &str| {
        let r = a.xt(sql, &[(0, 0, value)]);
        match &r.error {
            Some(e) if e.code == "42P18" => {}
            Some(e) => wrong.push(format!("{what}: {} {}", e.code, e.message)),
            None => {
                let got = a.q(read).single(what);
                if got != "007" {
                    wrong.push(format!("{what}: stored {got:?}, want '007' (or 42P18)"));
                }
            }
        }
    };
    check(
        &mut a,
        "INSERT INTO s.t",
        "INSERT INTO s.t VALUES (1, $1)",
        b"007",
        "SELECT c FROM s.t WHERE id = 1",
    );
    a.q("INSERT INTO s.t VALUES (2, 'x') ON CONFLICT DO NOTHING")
        .ok("a row to update");
    check(
        &mut a,
        "UPDATE s.t SET",
        "UPDATE s.t SET c = $1 WHERE id = 2",
        b"007",
        "SELECT c FROM s.t WHERE id = 2",
    );
    check(
        &mut a,
        "ON CONFLICT excluded",
        "INSERT INTO s.t VALUES (2, $1) ON CONFLICT (id) DO UPDATE SET c = excluded.c",
        b"007",
        "SELECT c FROM s.t WHERE id = 2",
    );
    a.q("SET search_path TO s, public").ok("search path");
    check(
        &mut a,
        "INSERT INTO t by the search path",
        "INSERT INTO t VALUES (3, $1)",
        b"007",
        "SELECT c FROM s.t WHERE id = 3",
    );
    a.q("SET search_path TO public").ok("search path back");
    let r = a.describe_statement("WITH p AS (SELECT 1 AS name) DELETE FROM p WHERE name = $1");
    if r.error.is_some() || r.params != Some(vec![25]) {
        wrong.push(format!(
            "DELETE FROM p beside a CTE p: Describe {:?} {:?}, want [25] (the table's text)",
            r.params, r.error
        ));
    }
    let r = a.describe_statement("SELECT 1 FROM t WHERE s.t.c = $1");
    if r.error.as_ref().map(|e| e.code.as_str()) != Some("42P01") {
        wrong.push(format!(
            "a three-part reference: {:?} {:?}, want 42P01",
            r.params, r.error
        ));
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// A column alias list over a `*` the walk cannot expand (a relation of another schema) leaves the
/// columns unknown, so a parameter compared with one is refused 42P18: a CTE's column list and a
/// FROM alias's. The alias list renamed the `*` marker away, so the relation read as having the
/// aliased columns, untyped, and $1 fell to text (wire review 14 item 8). A view's arm cannot be
/// reached: the engine refuses a view over the attached table, and that refusal is pinned here
/// (ruled c9c821dd8d, P4; COMPAT's views table).
#[test]
fn an_alias_list_over_an_unknown_star_stays_unknown() {
    let dir = Scratch::new("aliasstar");
    cli_script(&dir, b"CREATE SCHEMA s;\nCREATE TABLE s.u(k INT, n INT);\n");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let view = "CREATE VIEW vu(k, n) AS SELECT * FROM s.u";
    let refused = a.q(view).err(view);
    assert!(
        refused
            .message
            .contains("view cannot reference table in attached database"),
        "a view over the attached table: {refused:?}, want the engine's refusal"
    );
    let mut wrong = Vec::new();
    for sql in [
        "WITH w(k, n) AS (SELECT * FROM s.u) SELECT k FROM w WHERE n > $1",
        "SELECT k FROM (SELECT * FROM s.u) AS d(k, n) WHERE n > $1",
    ] {
        let r = a.describe_statement(sql);
        if r.error.as_ref().map(|e| e.code.as_str()) != Some("42P18") {
            wrong.push(format!("{sql}: {:?} {:?}, want 42P18", r.params, r.error));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
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

/// The connect form's close frees the branch at once, seen in time and not only in outcome: after
/// a session that wrote on a branch sends Terminate and the server closes, the next create, a new
/// session on the branch and the delete each answer within 500 ms, with the server's lock wait at
/// 300 ms so that no wait can be absorbed by a retry. a_branch_session_that_wrote_closes_at_terminate
/// bounds only the close: the claim and delete wait up to 5 s and a create retries for the lock
/// wait (60 s by default), and a read that times out returns an empty reply that `.ok()` accepts,
/// so the dry run's 1-2 s create passed it (wire review 16 item 8). Mutants that must turn it red
/// (LOUD-owed): drop the socket before the session's end, and release the branch on a deferred
/// thread.
#[test]
fn a_branch_session_that_wrote_frees_its_branch_at_once() {
    let dir = Scratch::new("terminatetimed");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "300"]);
    let mut a = seeded(&server);
    let bound = Duration::from_millis(500);
    let timed = |what: &str, f: &mut dyn FnMut()| {
        let started = Instant::now();
        f();
        let took = started.elapsed();
        assert!(took < bound, "{what} took {took:?}");
    };
    for (name, in_block) in [("t0", false), ("t1", true)] {
        a.q(&format!("SELECT turso_branch_create('{name}')"))
            .ok("create");
        let mut c = server
            .connect_to(&format!("postgres/{name}"))
            .expect("startup on the branch");
        if in_block {
            c.q("BEGIN").ok("begin");
        }
        c.q("UPDATE t SET v = 'branch' WHERE id = 1").ok("update");
        c.send(b'X', &[]);
        c.s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut buf = [0u8; 1];
        let read = c.s.read(&mut buf);
        assert!(
            matches!(read, Ok(0)),
            "{name}: no close within 2 s: {read:?}"
        );
        timed(&format!("{name}: the next create"), &mut || {
            a.q(&format!("SELECT turso_branch_create('{name}n')"))
                .ok("the next create");
        });
        timed(&format!("{name}: a new session on the branch"), &mut || {
            let mut r = server
                .connect_to(&format!("postgres/{name}"))
                .expect("a new session on the branch");
            r.q("SELECT 1").ok("select");
            r.send(b'X', &[]);
            r.s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut buf = [0u8; 1];
            assert!(
                matches!(r.s.read(&mut buf), Ok(0)),
                "{name}: the second close"
            );
        });
        timed(&format!("{name}: the delete"), &mut || {
            a.q(&format!("SELECT turso_branch_delete('{name}')"))
                .ok("the delete");
        });
    }
}

/// The create-latency pilot's connect form: a session started on a new branch runs SELECT 1 and an
/// UPDATE, then sends Terminate and reads to EOF with its own end open, as bbload's synchronous
/// close does. The server closes within 2 s, having kept the UPDATE (or rolled back the block it
/// was in), and the next create and the delete succeed first try. In fastest-tools' dry run
/// (frontier/fastest/tools/pilot/raw/20261008T212622Z-EXPLORATORY-dry2-*, T_m1-connect) every close
/// waited out bbload's 5 s bound and the next create or delete took 1-2 s, on a binary built
/// 2026-10-06T03:39Z, before 02ebcc464 (wire review 3 item 3); the dev tree's head then was
/// 6dcd40163 (reflog). terminate_ends_the_session_and_frees_its_branch_first runs no statement on
/// the branch before its Terminate.
#[test]
fn a_branch_session_that_wrote_closes_at_terminate() {
    let dir = Scratch::new("terminatewrite");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let close = |c: &mut Wire, what: &str| {
        c.send(b'X', &[]);
        c.s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut buf = [0u8; 1];
        let read = c.s.read(&mut buf);
        assert!(
            matches!(read, Ok(0)),
            "{what}: after Terminate the server did not close within 2 s: {read:?}"
        );
    };
    for (name, in_block, kept) in [("w0", false, "branch"), ("w1", true, "trunk")] {
        a.q(&format!("SELECT turso_branch_create('{name}')"))
            .ok("create");
        let mut c = server
            .connect_to(&format!("postgres/{name}"))
            .expect("startup on the branch");
        c.q("SELECT 1").ok("select");
        if in_block {
            c.q("BEGIN").ok("begin");
        }
        c.q("UPDATE t SET v = 'branch' WHERE id = 1").ok("update");
        close(&mut c, name);
        a.q(&format!("SELECT turso_branch_create('{name}n')"))
            .ok("the next create, first try");
        let mut r = server
            .connect_to(&format!("postgres/{name}"))
            .expect("a new session on the branch, first try");
        assert_eq!(
            r.q("SELECT v FROM t WHERE id = 1").single("the row"),
            kept,
            "{name}"
        );
        close(&mut r, name);
        a.q(&format!("SELECT turso_branch_delete('{name}')"))
            .ok("a delete right after the close, first try");
    }
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

/// Inside a block, an error from a statement with no statement savepoint of its own (an integer
/// overflow, a read) leaves the block's savepoints, as in PostgreSQL: ROLLBACK TO one ends the
/// failed state, the block carries on, and COMMIT keeps the work done before the savepoint. Until
/// the engine kept them (wire review 3 item 10, the engine's half) such an error rolled the whole
/// transaction back, savepoints with it, and this test pinned the server's 3B001 for the vanished
/// savepoint; its expected values moved to PostgreSQL's behaviour, PROVISIONALLY (ruled
/// c9c821dd8d, P7). The recording since made (artie raw loud-20261015/pg18-diff/pg18.out) agrees
/// with every value after the error, and it showed the error itself was not PostgreSQL's: sum()
/// over the int8 maximum is a numeric there. So the error is abs() of the int8 minimum, 22003 in
/// PostgreSQL (pg18-c33-engineerr.out, its savepoint part), and the recorded scenario itself, with
/// its sum and its 1/0, is replayed by a_savepoint_block_replays_postgresqls_recording (C33). The
/// name is kept from then. The 3B001 arm (Session::run, an aborted block with no engine
/// transaction) has no test that reaches it now.
#[test]
fn a_rollback_to_a_savepoint_the_engine_discarded_is_3b001() {
    let dir = Scratch::new("discardedsavepoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    a.q("SAVEPOINT s").ok("savepoint");
    let r = a.q("SELECT abs(-9223372036854775808::int8)");
    assert!(r.error.is_some(), "abs did not overflow: {:?}", r.rows);
    assert_eq!(r.status, b'E', "the block is not marked failed");
    let r = a.q("ROLLBACK TO s").ok("rollback to s");
    assert_eq!(r.status, b'T', "the block is still failed");
    assert_eq!(a.q("SELECT 1").single("in the block"), "1");
    let r = a.q("COMMIT").ok("the block's end");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
        "1",
        "the work before the savepoint was kept"
    );
}

/// The savepoint scenario exactly as PostgreSQL 18.6 answered it (recorded, artie raw
/// loud-20261015/pg18-diff/pg18.out, its BEGIN to its count): sum() over the int8 maximum is the
/// numeric 9223372036854775808, SELECT 1/0 is 22012 and fails the block, ROLLBACK TO s ends the
/// failure, and COMMIT keeps the row inserted before the savepoint.
#[test]
fn a_savepoint_block_replays_postgresqls_recording() {
    let dir = Scratch::new("savepointrecorded");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE big(v BIGINT)").ok("big");
    a.q("INSERT INTO big VALUES (9223372036854775807), (1)")
        .ok("rows");
    assert_eq!(a.q("BEGIN").ok("begin").status, b'T');
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    a.q("SAVEPOINT s").ok("savepoint");
    let r = a.q("SELECT sum(v) FROM big").ok("sum");
    assert_eq!(r.oids, Some(vec![1700]));
    assert_eq!(r.rows, vec![vec![Some("9223372036854775808".to_string())]]);
    assert_eq!(r.status, b'T');
    let r = a.q("SELECT 1/0");
    assert_eq!(r.status, b'E');
    let e = r.err("1/0");
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("22012", "division by zero")
    );
    let r = a.q("ROLLBACK TO s").ok("rollback to s");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'T');
    assert_eq!(a.q("SELECT 1").single("in the block"), "1");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
        "1"
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

/// A function in FROM reads the FROM items before it (PostgreSQL's functions in FROM are implicitly
/// LATERAL), in its arguments too: in `... EXISTS (SELECT 1 FROM q, generate_series(1, CASE WHEN
/// name > $1 ...))` the bare name is q's, int4, the nearer item, never the outer p's text. The walk
/// read the arguments in the enclosing scope alone, so $1 was typed from p.name (wire review 16
/// item 24).
#[test]
fn a_from_functions_arguments_read_the_items_before_it() {
    let dir = Scratch::new("fromfnargs");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(name TEXT)").ok("p");
    a.q("CREATE TABLE q(name INT)").ok("q");
    let sql = "SELECT 1 FROM p WHERE EXISTS (SELECT 1 FROM q, \
               generate_series(1, CASE WHEN name > $1 THEN 2 ELSE 3 END))";
    let r = a.describe_statement(sql).ok(sql);
    assert_eq!(r.params, Some(vec![23]), "{sql}: $1 is compared with q.name");
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

/// COPY FROM reads only the options it implements (FORMAT text, DELIMITER, NULL, a boolean HEADER)
/// and refuses every other before the file is read, nothing imported: an option PostgreSQL knows
/// and this COPY does not (QUOTE and FORCE_NOT_NULL, which need CSV mode, DEFAULT, ON_ERROR,
/// ENCODING, HEADER MATCH) is 0A000, and a name PostgreSQL does not know, or one given twice, is
/// 42601, as PostgreSQL's ProcessCopyOptions answers. Its option loop ignored any name it did not
/// read, so each of these imported the file's rows (HEADER MATCH as data, its header line
/// included) (wire review 17 item 6). Control: HEADER false and a delimiter import both rows.
#[test]
fn copy_refuses_an_option_it_does_not_implement() {
    let dir = Scratch::new("copyopts");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE co(id INT, v TEXT)").ok("co");
    let rows = dir.0.join("rows.tsv");
    std::fs::write(&rows, "1\tone\n2\ttwo\n").unwrap();
    let count = |a: &mut Wire| a.q("SELECT count(*) FROM co").single("count");
    for (options, code) in [
        ("(QUOTE '\"')", "0A000"),
        ("(FORCE_NOT_NULL (v))", "0A000"),
        ("(DEFAULT 'x')", "0A000"),
        ("(ON_ERROR ignore)", "0A000"),
        ("(ENCODING 'LATIN1')", "0A000"),
        ("(HEADER MATCH)", "0A000"),
        ("(NOSUCH 1)", "42601"),
        ("(DELIMITER E'\\t', DELIMITER E'\\t')", "42601"),
    ] {
        let sql = format!("COPY co FROM '{}' {options}", rows.display());
        assert_eq!(a.q(&sql).err(&sql).code, code, "{options}");
        assert_eq!(count(&mut a), "0", "{options}: rows imported");
    }
    let sql = format!(
        "COPY co FROM '{}' (FORMAT text, HEADER false, DELIMITER E'\\t')",
        rows.display()
    );
    a.q(&sql).ok("control");
    assert_eq!(count(&mut a), "2");
}

/// A statement the frontend performs while preparing it is refused before it runs when it holds a
/// parameter: 42P02 on both protocols, as PostgreSQL answers a utility statement's $n (FLAGGED
/// edit, the lead's pre-approval of wire review 17 item 4: the extended arm asserted an invented
/// 0A000); and COPY ... WHERE is refused (0A000, COMPAT.md), as it
/// has no WHERE. The COPY ran inside the prepare before any guard saw it, and its WHERE was never
/// read, so `WHERE id > 5` and `WHERE v = $1` imported every row (wire review 13 item 9).
#[test]
fn a_prepare_time_statement_is_refused_before_it_runs() {
    let dir = Scratch::new("copywhere");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cw(id INT, v TEXT)").ok("cw");
    let rows = dir.0.join("rows.tsv");
    std::fs::write(&rows, "1\tone\n7\tseven\n").unwrap();
    let count = |a: &mut Wire| a.q("SELECT count(*) FROM cw").single("count");
    let with_param = format!("COPY cw FROM '{}' WHERE v = $1", rows.display());
    let r = a.q(&with_param);
    assert_eq!(r.err("COPY with $1 by simple query").code, "42P02");
    assert_eq!(count(&mut a), "0", "nothing imported");
    let r = a.xt(&with_param, &[(25, 0, b"one")]);
    assert_eq!(r.err("COPY with $1 by Bind").code, "42P02");
    assert_eq!(count(&mut a), "0", "nothing imported");
    let r = a.q(&format!("COPY cw FROM '{}' WHERE id > 5", rows.display()));
    assert_eq!(r.err("COPY ... WHERE").code, "0A000");
    assert_eq!(count(&mut a), "0", "nothing imported");
    a.q(&format!("COPY cw FROM '{}'", rows.display()))
        .ok("a plain COPY still imports");
    assert_eq!(count(&mut a), "2");
}

/// The prepare-time `$n` guards leave nothing behind, by simple query and by Bind (wire review 17
/// item 12: the extended arms of these guards passed without the guard). `ALTER TABLE ... ADD
/// CONSTRAINT CHECK (v > $1)` is 42P02 and adds no CHECK: bound with 100, a row with v = 5 still
/// goes in. `CREATE TABLE sn(id SERIAL, v INT DEFAULT $1)` is 42P02 and makes neither the table nor
/// its sequence: both are created afterwards with no "already exists". Mutants that must turn this
/// red: the `$n` guard's `performs` filter deleted (the ALTER), and the prerequisites' `$n` refusal
/// deleted (the sequence).
#[test]
fn a_parameter_in_a_prepare_time_statement_runs_nothing() {
    let dir = Scratch::new("guardpins");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ck(id INT PRIMARY KEY, v INT)").ok("ck");
    let alter = "ALTER TABLE ck ADD CONSTRAINT big CHECK (v > $1)";
    let r = a.q(alter);
    assert_eq!(r.err("ALTER with $1 by simple query").code, "42P02");
    a.q("INSERT INTO ck VALUES (1, 5)")
        .ok("no CHECK after the simple query");
    let r = a.xt(alter, &[(23, 0, b"100")]);
    assert_eq!(r.err("ALTER with $1 by Bind").code, "42P02");
    a.q("INSERT INTO ck VALUES (2, 5)")
        .ok("no CHECK (v > 100) after the Bind");
    let create = "CREATE TABLE sn(id SERIAL, v INT DEFAULT $1)";
    let r = a.q(create);
    assert_eq!(r.err("SERIAL with $1 by simple query").code, "42P02");
    let r = a.xt(create, &[(23, 0, b"7")]);
    assert_eq!(r.err("SERIAL with $1 by Bind").code, "42P02");
    a.q("CREATE SEQUENCE sn_id_seq")
        .ok("the refused CREATEs left no sequence");
    a.q("CREATE TABLE sn(id INT, v INT)")
        .ok("the refused CREATEs left no table");
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
    let mut w = Wire {
        s,
        parameters: Vec::new(),
        key: None,
    };
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

/// A branch call's `$n` past the limit is refused, at Describe and over the simple protocol, and the
/// server serves on: 42P02 up to i32::MAX, and above it 42601 "parameter number too large", as
/// PostgreSQL 18's scanner refuses it. Branch calls never reached the prepare-time limit: Describe
/// sized its parameter list by the number, so `$18446744073709551615` panicked on capacity overflow
/// and `$2147483647` asked for about 32 GiB, and under the release build's panic=abort one client
/// ended every session (wire review 9 item 1). The 20-digit case comes first, so at the base the
/// test fails on its panic before it asks for the 32 GiB. (The arms above i32::MAX expected 42P02,
/// libpg_query's PostgreSQL 17 answer; wire review 12 item 1 makes PG18 the reference.)
#[test]
fn a_branch_call_parameter_past_the_limit_is_refused() {
    let dir = Scratch::new("branchparamlimit");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for (sql, code) in [
        ("SELECT turso_branch_create($18446744073709551615)", "42601"),
        ("SELECT turso_branch_create($65536)", "42P02"),
        ("SELECT turso_branch_switch($2147483647)", "42P02"),
        ("SELECT turso_branch_create($2147483648)", "42601"),
        ("SELECT turso_branch_create($4294967297)", "42601"),
        (
            "SELECT turso_branch_create($99999999999999999999999)",
            "42601",
        ),
    ] {
        let r = a.describe_statement(sql);
        assert_eq!(r.err(sql).code, code, "{sql}, extended");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, code, "{sql}");
        let mut b = server.connect();
        assert_eq!(b.q("SELECT 1").single("a second session is served"), "1");
    }
}

/// A `$n` above i32::MAX is 42601 "parameter number too large", as PostgreSQL 18's scanner refuses
/// it, before anything runs: libpg_query reads the number into a 32-bit int (PostgreSQL 17), so
/// `turso_branch_create($4294967297)` was a call of $1, and Bind 'x' then Execute created branch x
/// (wire review 12 item 1). The same over the simple protocol and for an ordinary statement.
#[test]
fn a_parameter_number_the_scanner_would_wrap_is_refused() {
    let dir = Scratch::new("paramwrap");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let r = a.x("SELECT turso_branch_create($4294967297)", &["x"]);
    assert_eq!(r.err("a wrapped branch-call parameter").code, "42601");
    let r = a.q("SELECT turso_branch_switch('x')");
    assert_eq!(r.err("branch x must not exist").code, "3D000");
    for sql in [
        "SELECT v FROM t WHERE id = $4294967297",
        "SELECT 1 LIMIT $2147483648",
        "SELECT turso_branch_create($4295032831)",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "42601", "{sql}");
        let r = a.x(sql, &["1"]);
        assert_eq!(r.err(sql).code, "42601", "{sql}, extended");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
    }
}

/// The parameter refusals can be told apart by their words, not by the code alone (42601 is shared):
/// a `$n` above i32::MAX is PostgreSQL 18's `parameter number too large at or near "$n"`, on both
/// protocols, and a Bind whose value count is wrong names the unnamed statement as `""`, as
/// PostgreSQL's exec_bind_message does (recorded on 18.6: `bind message supplies 1 parameters, but
/// prepared statement "" requires 0`, artie raw loud-20261015/pg18-diff/pg18.out). The arms above
/// asserted the code only, so a mutant answering the fail-closed "could not read the statement's
/// parameters" refusal passed every one (wire review 14 item 18). That refusal, which no client
/// text is known to reach, now has its own code, XX000.
#[test]
fn the_parameter_refusals_can_be_told_apart() {
    let dir = Scratch::new("paramwords");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let sql = "SELECT v FROM t WHERE id = $4294967297";
    let too_large = "parameter number too large at or near \"$4294967297\"";
    let e = a.q(sql).err(sql);
    assert!(e.message.contains(too_large), "simple: {e:?}");
    let e = a.x(sql, &["1"]).err(sql);
    assert!(e.message.contains(too_large), "extended: {e:?}");
    let (_, e) = a.bind_round("SELECT 1", &[], &[Some(b"x")], &[]);
    let e = e.expect("one value for no parameter, none declared, is refused");
    assert_eq!(e.code, "08P01");
    assert!(
        e.message
            .contains("bind message supplies 1 parameters, but prepared statement \"\" requires 0"),
        "{e:?}"
    );
}

/// A BEGIN inside an open block, bound with a value it has no parameter for, is 08P01 at Bind
/// (status E: the block fails, as any error there does), never run as the engine's nested BEGIN
/// nor answered with the 25001 warning; ROLLBACK then ends the block. This is the case
/// every_statement_checks_its_bind_arity's fix was for, which its rows never sent: its
/// BEGIN-with-a-value row ran outside a block, refused at the base already, and its two-codes
/// CHECKPOINT row is refused by check_bind whatever the fix (wire review 14 item 19; the failed
/// block's ROLLBACK bound with a value is pinned by a_block_exit_bound_with_a_value_is_refused_
/// before_bind_complete).
#[test]
fn a_begin_bound_with_a_value_in_an_open_block_is_08p01() {
    let dir = Scratch::new("beginvalue");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("a write in the block");
    let (types, e) = a.bind_round("BEGIN", &[], &[Some(b"v")], &[]);
    assert_eq!(e.map(|e| e.code), Some("08P01".to_string()));
    assert!(
        !types.contains(&b'2'),
        "BindComplete came before the refusal: {:?}",
        String::from_utf8_lossy(&types)
    );
    assert_eq!(types.last(), Some(&b'Z'));
    let r = a.q("SELECT 1");
    assert_eq!(r.err("in the failed block").code, "25P02", "the block failed");
    a.q("ROLLBACK").ok("end");
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("rows"),
        "0",
        "the block was rolled back"
    );
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

/// A bytea parameter in text format whose hex holds a non-ASCII character is 22023, PostgreSQL's
/// invalid_parameter_value for "invalid hexadecimal digit" (measured on PostgreSQL 17.11 by wire
/// review 13 item 8; it was asserted 22P02), and the server serves on: the hex was sliced as a
/// &str, inside the character, which panicked the session (and under the release build's
/// panic=abort every session) from one Bind that declares OID 17 (wire review 9 item 5).
#[test]
fn a_bytea_parameter_with_a_non_ascii_digit_is_refused() {
    const BYTEA: u32 = 17;
    let dir = Scratch::new("byteahex");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("CREATE TABLE ty(y BYTEA)").ok("ty");
    for bytes in [&b"\\x0\xc3\xa90"[..], "\\x0\u{1F600}0".as_bytes()] {
        let r = a.xt("INSERT INTO ty(y) VALUES ($1)", &[(BYTEA, 0, bytes)]);
        assert_eq!(r.err("a non-ASCII hex digit").code, "22023", "{bytes:?}");
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
        let mut b = server.connect();
        assert_eq!(b.q("SELECT 1").single("a second session"), "1");
    }
    a.xt("INSERT INTO ty(y) VALUES ($1)", &[(BYTEA, 0, b"\\x00ff")])
        .ok("valid hex");
    assert_eq!(a.q("SELECT count(*) FROM ty").single("one row"), "1");
}

/// A text-format parameter its declared type cannot read is refused with the code PostgreSQL's
/// input function raises (int2in/int4in/int8in, float8in, numeric_in, boolin, byteain, array_in
/// over each; a PG18 record_pg.sh recording is owed): 22P02 for bad syntax, 22003 for an integer
/// out of its type's range, 22023 for a bad bytea hex digit, and an array element's own code (an
/// int4 element out of range is 22003, a bytea element's bad digit 22023). An integer with
/// surrounding blanks is read, as int4in reads it. The scalar refusals were XX000
/// (internal_error), no integer's range was checked below int8 (int4 '3000000000' and int2
/// '70000' were bound), and an array element's error was replaced by a generic 22P02 (wire review
/// 13 item 8 and LOW 17; review 14 item 28). Every case is checked before the test fails.
#[test]
fn a_parameter_its_type_cannot_read_is_refused_with_its_types_code() {
    const BOOL: u32 = 16;
    const INT8: u32 = 20;
    const INT2: u32 = 21;
    const INT4: u32 = 23;
    const FLOAT8: u32 = 701;
    const BYTEA_ARRAY: u32 = 1001;
    const INT4_ARRAY: u32 = 1007;
    const NUMERIC: u32 = 1700;
    let dir = Scratch::new("paramcodes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let cases: [(u32, &[u8], &str); 12] = [
        (INT4, b"x", "22P02"),
        (INT4, b"1.5", "22P02"),
        (INT2, b"70000", "22003"),
        (INT4, b"3000000000", "22003"),
        (INT8, b"9223372036854775808", "22003"),
        (FLOAT8, b"abc", "22P02"),
        (NUMERIC, b"abc", "22P02"),
        (BOOL, b"maybe", "22P02"),
        (INT4_ARRAY, b"{3000000000}", "22003"),
        (INT4_ARRAY, b"{1.0}", "22P02"),
        (BYTEA_ARRAY, b"{\"\\\\xZZ\"}", "22023"),
        (BYTEA_ARRAY, b"{\"\\\\x0\"}", "22023"),
    ];
    let mut wrong = Vec::new();
    for (oid, value, code) in cases {
        let r = a.xt("SELECT $1", &[(oid, 0, value)]);
        let got = r.error.as_ref().map(|e| e.code.clone());
        if got.as_deref() != Some(code) {
            wrong.push(format!(
                "OID {oid} {:?}: {got:?} {:?}, want {code}",
                String::from_utf8_lossy(value),
                r.rows
            ));
        }
        assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
    }
    let r = a.xt("SELECT $1", &[(INT4, 0, b" 7 ")]);
    if r.error.is_some() || r.rows != vec![vec![Some("7".to_string())]] {
        wrong.push(format!("int4 ' 7 ': {:?} {:?}, want 7", r.error, r.rows));
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// A branch call checks what an engine statement checks first before it refuses a bad result
/// format code: two result codes for its one column are 08P01, as `SELECT 1` bound with them is,
/// and an unknown branch function is 42883, whatever the codes; code 2 alone is still 22023,
/// before the call changes anything (no branch z). The 22023 came first, so codes [0, 2] answered
/// 22023 where an engine SELECT answers 08P01 (wire review 17 item 9).
#[test]
fn a_branch_call_checks_its_shape_before_its_result_codes() {
    let dir = Scratch::new("branchcodes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let code = |a: &mut Wire, sql: &str, rcodes: &[i16]| {
        let (_, error) = a.bind_round(sql, &[], &[], rcodes);
        error.map(|e| e.code)
    };
    assert_eq!(
        code(&mut a, "SELECT 1", &[0, 2]).as_deref(),
        Some("08P01"),
        "premise: an engine SELECT"
    );
    assert_eq!(
        code(&mut a, "SELECT turso_branch_current()", &[0, 2]).as_deref(),
        Some("08P01")
    );
    assert_eq!(
        code(&mut a, "SELECT turso_branch_nosuch()", &[2]).as_deref(),
        Some("42883")
    );
    assert_eq!(
        code(&mut a, "SELECT turso_branch_current()", &[2]).as_deref(),
        Some("22023")
    );
    assert_eq!(
        code(&mut a, "SELECT turso_branch_create('z')", &[2]).as_deref(),
        Some("22023")
    );
    let sql = "SELECT turso_branch_switch('z')";
    assert!(a.q(sql).error.is_some(), "the refused create left branch z");
}

/// A Bind PostgreSQL refuses is refused AT the Bind, before BindComplete, so the Execute after it
/// runs nothing: a parameter format code other than 0 or 1 (22023 "unsupported format code: 2",
/// even for a NULL value), a parameter-format list that is neither 0, 1 nor one per parameter
/// (08P01), and, for a branch call, a parameter count other than the call's (08P01). A result
/// format code other than 0 or 1 is refused (22023) as PostgreSQL refuses it, at Execute when the
/// row is formatted, so after BindComplete; a branch call refuses it before the call runs
/// (FLAGGED edit, wire review 13 item 10: these two rows asserted no BindComplete, the placement
/// E5-QUEUE R2(b) recorded, which refused result codes on statements that return no row).
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
        if rcodes.is_empty() {
            assert!(
                !tags.contains(&b'2'),
                "{what}: BindComplete was sent: {tags:?}"
            );
        } else {
            assert!(
                tags.contains(&b'2'),
                "{what}: a result code is refused at Execute, after BindComplete: {tags:?}"
            );
        }
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

/// A Bind's format codes are refused only where PostgreSQL refuses them: a parameter code only for
/// a parameter that exists (one code 2 with no values is accepted), after the parameter-format
/// count (codes [2, 2, 2] for one value is 08P01, not 22023), and a result code only when a row is
/// formatted (code 2 on an INSERT or on a SELECT of no rows succeeds; on a SELECT with a row it is
/// 22023 at Execute). check_bind refused all three at the Bind with 22023 (wire review 13 item
/// 10; E5-QUEUE R2(b) is an outcome divergence, not one of placement).
#[test]
fn a_format_code_is_refused_only_where_postgresql_refuses_it() {
    let dir = Scratch::new("bindcodes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse, Bind (parameter codes, text values, result codes), Execute, Sync.
    fn round(w: &mut Wire, sql: &str, pcodes: &[i16], values: &[&[u8]], rcodes: &[i16]) -> Reply {
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
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    let mut wrong = Vec::new();
    let cases: Vec<(&str, Vec<i16>, Vec<&[u8]>, Vec<i16>, Option<&str>)> = vec![
        (
            "INSERT INTO t VALUES (2, 'two')",
            vec![],
            vec![],
            vec![2],
            None,
        ),
        (
            "SELECT id FROM t WHERE false",
            vec![],
            vec![],
            vec![2],
            None,
        ),
        ("SELECT id FROM t", vec![], vec![], vec![2], Some("22023")),
        ("SELECT 1", vec![2], vec![], vec![], None),
        (
            "SELECT $1::int4",
            vec![2, 2, 2],
            vec![b"1"],
            vec![],
            Some("08P01"),
        ),
    ];
    for (sql, pcodes, values, rcodes, code) in cases {
        let r = round(&mut a, sql, &pcodes, &values, &rcodes);
        let got = r.error.as_ref().map(|e| e.code.as_str());
        if got != code {
            wrong.push(format!(
                "{sql} with codes {pcodes:?} / {rcodes:?}: {got:?}, want {code:?}"
            ));
        }
    }
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2")
            .single("the INSERT with result code 2 ran"),
        "1"
    );
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// Reply messages up to ReadyForQuery: each type byte, the error fields of each ErrorResponse, and
/// the ReadyForQuery status. Waits on the client's 30-second read timeout, so a reply that never
/// ends fails the test there instead of hanging it.
fn read_raw_reply(w: &mut Wire) -> (Vec<u8>, Vec<WireError>, u8) {
    let (mut tags, mut errors) = (Vec::new(), Vec::new());
    loop {
        let mut head = [0u8; 5];
        w.s.read_exact(&mut head)
            .expect("the server sent ReadyForQuery before the read timeout");
        let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        w.s.read_exact(&mut body).unwrap();
        tags.push(head[0]);
        if head[0] == b'E' {
            errors.push(error_fields(&body));
        }
        if head[0] == b'Z' {
            return (tags, errors, body[0]);
        }
    }
}

/// A message string that is not valid UTF-8 is 22021 "invalid byte sequence for encoding \"UTF8\"",
/// as PostgreSQL refuses it on every path, and names no branch: a branch name 0xFF sent as a simple
/// query's literal, as an extended Parse's literal, and as a bound text parameter. The decoder read
/// strings lossily, so the literals '\xff' and '\xfe' both became U+FFFD and named one branch, while
/// the same bytes bound as a parameter were 22021 (wire review 12 item 8).
#[test]
fn invalid_utf8_names_no_branch() {
    let dir = Scratch::new("badutf8");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    let literal = b"SELECT turso_branch_create('\xff')";
    let mut query = literal.to_vec();
    query.push(0);
    a.s.write_all(&frame(b'Q', &query)).unwrap();
    let (_, errors, status) = read_raw_reply(&mut a);
    assert_eq!(
        errors.first().map(|e| e.code.as_str()),
        Some("22021"),
        "simple: {errors:?}"
    );
    assert_eq!(status, b'I');
    let mut parse = vec![0u8];
    parse.extend_from_slice(literal);
    parse.extend_from_slice(&[0, 0, 0]);
    let round = [
        frame(b'P', &parse),
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
        frame(b'E', &[0, 0, 0, 0, 0]),
        frame(b'S', &[]),
    ]
    .concat();
    a.s.write_all(&round).unwrap();
    let (_, errors, _) = read_raw_reply(&mut a);
    assert_eq!(
        errors.first().map(|e| e.code.as_str()),
        Some("22021"),
        "extended: {errors:?}"
    );
    let r = a.xt("SELECT turso_branch_create($1)", &[(0, 0, b"\xfe")]);
    assert_eq!(r.err("a bound parameter").code, "22021");
    let r = a.q("SELECT turso_branch_switch('\u{FFFD}')");
    assert_eq!(r.err("no branch U+FFFD").code, "3D000");
}

/// A simple Query whose body is malformed (a string with no terminator) fails the block it arrives
/// in, as any error there does: inside BEGIN the block is failed (25P02 until its end, which
/// answers ROLLBACK), and in a pipeline its implicit block is rolled back. The malformed message's
/// error never reached the session's block state: ReadyForQuery said 'E' from pgwire's own copy,
/// the next statement ran, and COMMIT committed (wire review 12 item 7).
#[test]
fn a_malformed_query_fails_its_block() {
    let dir = Scratch::new("badquery");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert");
    a.s.write_all(&frame(b'Q', b"SELECT 1")).unwrap();
    let (_, errors, status) = read_raw_reply(&mut a);
    let codes: Vec<(&str, &str)> = errors
        .iter()
        .map(|e| (e.code.as_str(), e.message.as_str()))
        .collect();
    assert_eq!(codes, vec![("08P01", "invalid string in message")]);
    assert_eq!(status, b'E', "the block is failed");
    let r = a.q("SELECT 1");
    assert_eq!(r.err("in the failed block").code, "25P02");
    let r = a.q("COMMIT").ok("end");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2")
            .single("not committed"),
        "0"
    );
    // A pipeline: Execute an INSERT, the malformed Query, then Sync.
    let mut parse = vec![0u8];
    parse.extend_from_slice(b"INSERT INTO t VALUES (3, 'three')");
    parse.extend_from_slice(&[0, 0, 0]);
    let pipeline = [
        frame(b'P', &parse),
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
        frame(b'E', &[0, 0, 0, 0, 0]),
        frame(b'Q', b"SELECT 1"),
    ]
    .concat();
    a.s.write_all(&pipeline).unwrap();
    let (_, errors, _) = read_raw_reply(&mut a);
    assert_eq!(errors.len(), 1, "{errors:?}");
    a.send(b'S', &[]);
    let (_, _, status) = read_raw_reply(&mut a);
    assert_eq!(status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 3")
            .single("the pipeline rolled back"),
        "0"
    );
}

/// A Sync whose body is not empty is refused (08P01 "invalid message format") and still answered
/// with ReadyForQuery, as PostgreSQL ends a skip at any Sync before it reads the body; the pipeline
/// it ends is rolled back. Read as an extended message in error, it set the session waiting for a
/// Sync that had just arrived, or was dropped while one was awaited: no ReadyForQuery came, and the
/// client hung holding its connection slot and branch (wire review 12 item 6).
#[test]
fn a_malformed_sync_is_still_answered() {
    let dir = Scratch::new("badsync");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    let mut parse = vec![0u8];
    parse.extend_from_slice(b"INSERT INTO t VALUES (5, 'five')");
    parse.extend_from_slice(&[0, 0, 0]);
    let pipeline = [
        frame(b'P', &parse),
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
        frame(b'E', &[0, 0, 0, 0, 0]),
        frame(b'S', &[0]),
    ]
    .concat();
    a.s.write_all(&pipeline).unwrap();
    let (_, errors, status) = read_raw_reply(&mut a);
    let codes: Vec<(&str, &str)> = errors
        .iter()
        .map(|e| (e.code.as_str(), e.message.as_str()))
        .collect();
    assert_eq!(codes, vec![("08P01", "invalid message format")]);
    assert_eq!(status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 5")
            .single("the pipeline rolled back"),
        "0"
    );
    // A malformed Parse starts a skip; the malformed Sync still ends it.
    let skip = [frame(b'P', b"\0SELECT 1\0"), frame(b'S', &[0])].concat();
    a.s.write_all(&skip).unwrap();
    let (_, errors, status) = read_raw_reply(&mut a);
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert_eq!(status, b'I');
    assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
}

/// A malformed Sync ends its round as a well-formed one does: the transaction it ends takes its
/// named portals with it, so an Execute of one after it is 34000 and runs nothing; and inside a
/// block a portal suspended by max_rows survives a Sync and a simple Query, then is gone after
/// COMMIT. The malformed path answered its ERROR and ReadyForQuery and skipped the rest of the
/// round, so the portal outlived the transaction and its UPDATE ran and committed (wire review 15
/// item 4).
#[test]
fn a_malformed_sync_ends_its_round() {
    let dir = Scratch::new("badsyncround");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("INSERT INTO t VALUES (2, 'two'), (3, 'three')")
        .ok("rows");
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    let round = [
        frame(b'P', b"s1\0UPDATE t SET v = 'changed' WHERE id = 1\0\0\0"),
        frame(b'B', b"p\0s1\0\0\0\0\0\0\0"),
        frame(b'S', &[0]),
    ]
    .concat();
    a.s.write_all(&round).unwrap();
    let (_, errors, status) = read_raw_reply(&mut a);
    assert_eq!(errors.len(), 1, "the malformed Sync: {errors:?}");
    assert_eq!(errors[0].code, "08P01");
    assert_eq!(status, b'I');
    a.s.write_all(&[frame(b'E', b"p\0\0\0\0\0"), frame(b'S', &[])].concat())
        .unwrap();
    let (_, errors, _) = read_raw_reply(&mut a);
    assert_eq!(
        errors.iter().map(|e| e.code.as_str()).collect::<Vec<_>>(),
        vec!["34000"],
        "the portal went with its transaction"
    );
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 1").single("unchanged"),
        "trunk"
    );
    // Inside a block a suspended portal lives until the block ends.
    a.q("BEGIN").ok("begin");
    let fetch = |rows: i32| -> Vec<u8> {
        let mut e = b"p2\0".to_vec();
        e.extend_from_slice(&rows.to_be_bytes());
        e
    };
    let round = [
        frame(b'P', b"s2\0SELECT id FROM t ORDER BY id\0\0\0"),
        frame(b'B', b"p2\0s2\0\0\0\0\0\0\0"),
        frame(b'E', &fetch(1)),
        frame(b'S', &[]),
    ]
    .concat();
    a.s.write_all(&round).unwrap();
    let (tags, errors, status) = read_raw_reply(&mut a);
    assert!(errors.is_empty(), "{errors:?}");
    assert!(tags.contains(&b's'), "suspended: {tags:?}");
    assert_eq!(status, b'T');
    assert_eq!(a.q("SELECT 1").single("a query in the block"), "1");
    a.s.write_all(&[frame(b'E', &fetch(0)), frame(b'S', &[])].concat())
        .unwrap();
    let (tags, errors, _) = read_raw_reply(&mut a);
    assert!(errors.is_empty(), "the portal resumes: {errors:?}");
    assert_eq!(
        tags.iter().filter(|t| **t == b'D').count(),
        2,
        "the two rows left: {tags:?}"
    );
    a.q("COMMIT").ok("end");
    a.s.write_all(&[frame(b'E', &fetch(0)), frame(b'S', &[])].concat())
        .unwrap();
    let (_, errors, _) = read_raw_reply(&mut a);
    assert_eq!(
        errors.iter().map(|e| e.code.as_str()).collect::<Vec<_>>(),
        vec!["34000"],
        "the portal went with the block"
    );
}

/// A malformed simple Query that arrives while the session skips to Sync after an extended error is
/// ignored, as PostgreSQL ignores every message but Sync there before it reads the body: one
/// ErrorResponse and one ReadyForQuery for the round, and the INSERT pipelined after the Query
/// never runs. The malformed Query ended the skip with an ERROR and a ReadyForQuery of its own, so
/// the extended messages after it ran and Sync committed them (wire review 15 item 5; a regression
/// of e4e5fc79d, wider since 569e1793c sends invalid UTF-8 down the same path).
#[test]
fn a_malformed_query_in_a_skip_is_ignored() {
    let dir = Scratch::new("badqueryskip");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let frame = |tag: u8, body: &[u8]| -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    };
    for (n, query) in [(9, &b"SELECT 1"[..]), (10, &b"SELECT '\xff'\0"[..])] {
        let insert = format!("\0INSERT INTO t VALUES ({n}, 'n')\0\0\0");
        let round = [
            frame(b'P', b"\0SELECT * FROM nosuch\0\0\0"),
            frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
            frame(b'E', &[0, 0, 0, 0, 0]),
            frame(b'Q', query),
            frame(b'P', insert.as_bytes()),
            frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
            frame(b'E', &[0, 0, 0, 0, 0]),
            frame(b'S', &[]),
        ]
        .concat();
        a.s.write_all(&round).unwrap();
        let (tags, errors, status) = read_raw_reply(&mut a);
        assert_eq!(errors.len(), 1, "{n}: one error for the round: {errors:?}");
        assert_eq!(status, b'I');
        a.s.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut more = [0u8; 1];
        let extra = a.s.read(&mut more);
        a.s.set_read_timeout(None).unwrap();
        assert!(
            extra.is_err(),
            "{n}: a second reply after the round's ReadyForQuery: {extra:?} (tags {tags:?})"
        );
        assert_eq!(
            a.q(&format!("SELECT count(*) FROM t WHERE id = {n}"))
                .single("the INSERT after the Query never ran"),
            "0"
        );
    }
}

/// A Bind that supplies a value count other than the statement's parameters is 08P01 for EVERY
/// statement, as PostgreSQL's exec_bind_message refuses it, before anything runs: a branch call
/// (create with two format codes and one value, create with two values, delete of a literal with one
/// value; `turso_branch_create($2)` with no Describe is 42P18, its $1 untyped), and the statements
/// the server answers without the engine (CHECKPOINT, BEGIN, COMMIT, ROLLBACK), which no Bind
/// check reached: one value for a statement of none ran it (wire review 12 item 2).
#[test]
fn every_statement_checks_its_bind_arity() {
    let dir = Scratch::new("bindarity");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('keep')")
        .ok("a branch to keep");
    // Parse (no declared types), Bind (the given codes and values, no result codes), Execute, Sync.
    fn round(w: &mut Wire, sql: &str, pcodes: &[i16], values: &[&[u8]]) -> Reply {
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
        bind.extend_from_slice(&0i16.to_be_bytes());
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    let cases: Vec<(&str, Vec<i16>, Vec<&[u8]>, &str)> = vec![
        (
            "SELECT turso_branch_create($1)",
            vec![0, 0],
            vec![&b"b1"[..]],
            "08P01",
        ),
        (
            "SELECT turso_branch_create($1)",
            vec![],
            vec![&b"b1"[..], &b"b2"[..]],
            "08P01",
        ),
        (
            "SELECT turso_branch_delete('keep')",
            vec![],
            vec![&b"x"[..]],
            "08P01",
        ),
        (
            "SELECT turso_branch_create($2)",
            vec![],
            vec![&b"b1"[..], &b"b2"[..]],
            "42P18",
        ),
        ("CHECKPOINT", vec![], vec![&b"x"[..]], "08P01"),
        ("CHECKPOINT", vec![0, 0], vec![], "08P01"),
        ("BEGIN", vec![], vec![&b"x"[..]], "08P01"),
        ("COMMIT", vec![], vec![&b"x"[..]], "08P01"),
        ("ROLLBACK", vec![], vec![&b"x"[..]], "08P01"),
    ];
    for (sql, pcodes, values, code) in cases {
        let what = format!(
            "{sql} with {} codes and {} values",
            pcodes.len(),
            values.len()
        );
        let r = round(&mut a, sql, &pcodes, &values);
        assert_eq!(r.err(&what).code, code, "{what}");
        assert_eq!(r.status, b'I', "{what}: nothing was begun");
    }
    for name in ["b1", "b2"] {
        let r = a.q(&format!("SELECT turso_branch_switch('{name}')"));
        assert_eq!(r.err(name).code, "3D000", "branch {name} must not exist");
    }
    a.q("SELECT turso_branch_switch('keep')")
        .ok("keep was not deleted");
}

/// The fixed-types tests' schemas: ft(k INT) holding 1 on main, and a branch textk where ft is
/// (k TEXT) holding `branch_row`. The session is left on main.
fn ft_text_on_a_branch(a: &mut Wire, branch_row: &str) {
    a.q("CREATE TABLE ft(k INT)").ok("ft");
    a.q("INSERT INTO ft VALUES (1)").ok("row");
    a.q("SELECT turso_branch_create('textk')").ok("branch");
    a.q("SELECT turso_branch_switch('textk')").ok("switch");
    a.q("DROP TABLE ft").ok("drop on the branch");
    a.q("CREATE TABLE ft(k TEXT)")
        .ok("ft as text on the branch");
    a.q(&format!("INSERT INTO ft VALUES ('{branch_row}')"))
        .ok("row on the branch");
    a.q("SELECT turso_branch_switch('main')").ok("back to main");
}

/// A named statement keeps the types it had when it first met the engine (its first Describe or
/// Execute), as PostgreSQL fixes a prepared statement's parameter and result types at Parse: on a
/// branch where its column is TEXT, its Execute is refused (0A000, PostgreSQL's code for "cached
/// plan must not change result type"), not run with its parameters re-inferred as text. Re-inferred,
/// the 4 binary int4 bytes Describe announced were read as UTF-8 and matched nothing, and a text '1'
/// matched, where PostgreSQL errors (text = integer has no operator). Back on the trunk, where the
/// types are the fixed ones again, it runs (wire review 14 item 11).
#[test]
fn a_named_statement_keeps_its_described_parameter_types() {
    let dir = Scratch::new("frozentypes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    ft_text_on_a_branch(&mut a, "1");
    // s is fixed by its Describe: its parameter is int4 on main.
    a.send(b'P', b"s\0SELECT count(*) FROM ft WHERE k = $1\0\0\0");
    a.send(b'D', b"Ss\0");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.params,
        Some(vec![23]),
        "premise: s's parameter is int4: {:?}",
        r.error
    );
    // t is fixed by its first Execute, with 1 in text format: one row on main.
    a.send(b'P', b"t\0SELECT count(*) FROM ft WHERE k = $1\0\0\0");
    a.send(b'B', b"\0t\0\0\0\0\x01\0\0\0\x011\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.rows,
        vec![vec![Some("1".to_string())]],
        "t on main: {:?}",
        r.error
    );
    a.q("SELECT turso_branch_switch('textk')")
        .ok("onto the branch");
    // s with 1 as binary int4.
    a.send(b'B', b"\0s\0\0\x01\0\x01\0\x01\0\0\0\x04\0\0\0\x01\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("0A000"),
        "s ran with its parameter re-typed: rows {:?}",
        r.rows
    );
    assert!(r.rows.is_empty(), "rows {:?}", r.rows);
    assert_eq!(r.status, b'I');
    // t with 1 in text format.
    a.send(b'B', b"\0t\0\0\0\0\x01\0\0\0\x011\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.error.as_ref().map(|e| e.code.as_str()),
        Some("0A000"),
        "t ran with its parameter re-typed: rows {:?}",
        r.rows
    );
    // Control: back on main the types are the fixed ones, and s runs.
    a.q("SELECT turso_branch_switch('main')").ok("main again");
    a.send(b'B', b"\0s\0\0\x01\0\x01\0\x01\0\0\0\x04\0\0\0\x01\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.rows,
        vec![vec![Some("1".to_string())]],
        "s on main: {:?}",
        r.error
    );
}

/// A CREATE TABLE's foreign keys are checked against the newest schema, not a session's older
/// snapshot: after session b creates p, session a's `CREATE TABLE c(x INT REFERENCES p(id))`
/// succeeds and keeps its key (an orphan is 23503). The key precheck read a's snapshot, from
/// before p existed, and refused 42P01, and since a refused precheck compiles nothing the snapshot
/// never moved, so every retry was refused the same way (wire review 16 item 10).
#[test]
fn a_foreign_key_is_checked_against_the_newest_schema() {
    let dir = Scratch::new("stalekeys");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let mut b = server.connect();
    a.q("CREATE TABLE z(id INT)")
        .ok("a's snapshot, from before p");
    b.q("CREATE TABLE p(id INT PRIMARY KEY)")
        .ok("p, by another session");
    let sql = "CREATE TABLE c(x INT REFERENCES p(id))";
    a.q(sql).ok(sql);
    let orphan = "INSERT INTO c VALUES (5)";
    assert_eq!(a.q(orphan).err(orphan).code, "23503", "c's key was kept");
}

/// A statement's parameter types are inferred from the schema its compile uses, the newest one,
/// not from a session's older snapshot: after another session replaces ft(k INT) with ft(k TEXT),
/// a Describe of `count(*) ... WHERE k = $1` answers [25], and its executions return one row each,
/// as PostgreSQL does. The inference read the session's snapshot before the compile adopted the
/// newer schema, so it fixed int4 ([23]) and every Execute after was refused 0A000 against the
/// text column it then saw (wire review 18 item 8).
#[test]
fn parameter_types_come_from_the_schema_the_compile_uses() {
    let dir = Scratch::new("staletypes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let mut b = server.connect();
    a.q("CREATE TABLE ft(k INT)").ok("ft");
    a.q("SELECT count(*) FROM ft")
        .ok("a's snapshot holds ft(k INT)");
    b.q("DROP TABLE ft").ok("drop");
    b.q("CREATE TABLE ft(k TEXT)").ok("ft as text");
    b.q("INSERT INTO ft VALUES ('x')").ok("row");
    let sql = "SELECT count(*) FROM ft WHERE k = $1";
    let r = a.describe_statement(sql);
    assert_eq!(r.params, Some(vec![25]), "{:?}", r.error);
    for _ in 0..2 {
        let r = a.xt(sql, &[(0, 0, b"x")]).ok(sql);
        assert_eq!(r.rows, vec![vec![Some("1".to_string())]]);
    }
}

/// A statement's result columns are fixed as PostgreSQL's equalRowTypes compares them, names with
/// types: on a branch where ft is (b int, a int), a `SELECT * FROM ft` described on main as
/// (a int, b int) is refused 0A000 "cached plan must not change result type". Compared by type
/// alone it ran, and its rows went out in (b, a) order under the (a, b) RowDescription the client
/// had, each value in the other's column, silently (wire review 18 item 9; equalRowTypes read in
/// PostgreSQL's tupdesc.c, REL_19_BETA1).
#[test]
fn a_named_statement_keeps_its_described_column_names() {
    let dir = Scratch::new("fixednames");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ft(a INT, b INT)").ok("ft");
    a.q("INSERT INTO ft VALUES (1, 2)").ok("row");
    a.q("SELECT turso_branch_create('swapped')").ok("branch");
    a.q("SELECT turso_branch_switch('swapped')").ok("switch");
    a.q("DROP TABLE ft").ok("drop on the branch");
    a.q("CREATE TABLE ft(b INT, a INT)").ok("ft swapped");
    a.q("INSERT INTO ft VALUES (2, 1)").ok("row on the branch");
    a.q("SELECT turso_branch_switch('main')").ok("back to main");
    a.send(b'P', b"r\0SELECT * FROM ft\0\0\0");
    a.send(b'D', b"Sr\0");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.oids, Some(vec![23, 23]), "premise: {:?}", r.error);
    a.q("SELECT turso_branch_switch('swapped')")
        .ok("onto the branch");
    a.send(b'B', b"\0r\0\0\0\0\0\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.error
            .as_ref()
            .map(|e| (e.code.as_str(), e.message.as_str())),
        Some(("0A000", "cached plan must not change result type")),
        "rows {:?}",
        r.rows
    );
}

/// A named statement's result types are fixed at its first Describe too: on a branch where its
/// column is TEXT, its Describe and its Execute are refused, 0A000 "cached plan must not change
/// result type", as PostgreSQL's RevalidateCachedQuery refuses both. Its rows went out as text
/// under the int4 RowDescription the client had been sent (wire review 14 item 11, the result half
/// of the same mechanism).
#[test]
fn a_named_statement_keeps_its_described_result_types() {
    let dir = Scratch::new("frozencols");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    ft_text_on_a_branch(&mut a, "x");
    a.send(b'P', b"r\0SELECT k FROM ft\0\0\0");
    a.send(b'D', b"Sr\0");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.oids, Some(vec![23]), "premise: k is int4: {:?}", r.error);
    a.q("SELECT turso_branch_switch('textk')")
        .ok("onto the branch");
    a.send(b'D', b"Sr\0");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.error
            .as_ref()
            .map(|e| (e.code.as_str(), e.message.as_str())),
        Some(("0A000", "cached plan must not change result type")),
        "Describe on the branch: oids {:?}",
        r.oids
    );
    a.send(b'B', b"\0r\0\0\0\0\0\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(
        r.error
            .as_ref()
            .map(|e| (e.code.as_str(), e.message.as_str())),
        Some(("0A000", "cached plan must not change result type")),
        "Execute on the branch: rows {:?}",
        r.rows
    );
    assert!(r.rows.is_empty(), "rows {:?}", r.rows);
}

// C40: expected rows recorded with PostgreSQL 18.6 (C26 MED 1).
#[test]
fn parameter_count_after_an_empty_statement() {
    parameter_lexer_round_trip(";SELECT $1::int", "41");
}

#[test]
fn parameter_count_after_comments_and_empty_statements() {
    parameter_lexer_round_trip(" ; /* c */ ; SELECT $1::int", "41");
}

#[test]
fn parameter_count_in_a_continued_escape_string() {
    parameter_lexer_round_trip("SELECT E'one'\n'\\'$2' || $1::text", "one'$241");
}

fn parameter_lexer_round_trip(sql: &str, expected: &str) {
    let dir = Scratch::new("parameter-lexer");
    let server = Server::start(&dir.db(), &[]);
    let mut w = seeded(&server);
    let mut parse = vec![0];
    parse.extend_from_slice(sql.as_bytes());
    parse.extend_from_slice(&[0, 0, 0]);
    w.send(b'P', &parse);
    // Unnamed portal and statement, text format, one value "41", text result.
    w.send(b'B', b"\0\0\0\0\0\x01\0\0\0\x0241\0\0");
    w.send(b'E', &[0, 0, 0, 0, 0]);
    w.send(b'S', &[]);
    let r = w.read_reply();
    assert_eq!(r.status, b'I', "{sql}: {:?}", r.error);
    assert_eq!(r.single(sql), expected);

    let (messages, err) = w.bind_round(sql, &[], &[Some(b"41"), Some(b"42")], &[]);
    let err = err.expect("two values for a statement requiring one must fail");
    assert_eq!(err.code, "08P01", "{sql}: {err:?}");
    assert!(err.message.contains("requires 1"), "{sql}: {err:?}");
    assert!(
        !messages.contains(&b'2'),
        "BindComplete before the count refusal"
    );
}

/// A Bind's value count is checked at Bind for every statement, its parameters counted as
/// PostgreSQL counts them (wire review 17 item 2): a `$` that is no parameter (in a comment, a quoted
/// identifier) leaves a SET with the parameters Parse declared, none, so one value is 08P01 at Bind
/// and the SET is not performed; and a query's count comes first in exec_bind_message's order, so
/// two values for `SELECT $1` are 08P01 in a failed block (not 25P02) and with format code 2 (not
/// 22023). The count was skipped for any text holding a `$`: the prepare at Execute performed the
/// SET, and only then was the Bind refused.
#[test]
fn a_bind_counts_parameters_as_the_lexer_reads_them() {
    let dir = Scratch::new("bindlexer");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse (no declared types), Bind (one format code if given, then the text values), Execute,
    // Sync.
    fn round(w: &mut Wire, sql: &str, codes: &[i16], values: &[&[u8]]) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8];
        bind.extend_from_slice(&(codes.len() as i16).to_be_bytes());
        for c in codes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
            bind.extend_from_slice(v);
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    a.q("CREATE TABLE fp(id INT PRIMARY KEY)").ok("parent");
    a.q("CREATE TABLE fc(pid INT REFERENCES fp(id))")
        .ok("child");
    for sql in [
        "SET foreign_keys = off /* $1 */",
        "SET foreign_keys = off -- $1",
    ] {
        let r = round(&mut a, sql, &[], &[b"x"]);
        assert_eq!(r.err(sql).code, "08P01", "{sql}");
        assert_eq!(
            a.q("INSERT INTO fc VALUES (5)").err(sql).code,
            "23503",
            "{sql} was performed before its Bind was refused"
        );
    }
    let path = a.q("SHOW search_path").single("the search path");
    let sql = "SET search_path TO \"s$1\", public";
    let r = round(&mut a, sql, &[], &[b"x"]);
    assert_eq!(r.err(sql).code, "08P01");
    assert_eq!(
        a.q("SHOW search_path").single("the search path after"),
        path,
        "{sql} was performed before its Bind was refused"
    );
    // Control: a query's own $1 with one value runs.
    let r = round(&mut a, "SELECT $1::int + 1", &[], &[b"41"]);
    assert_eq!(r.rows, vec![vec![Some("42".to_string())]], "{:?}", r.error);
    // In a failed block: the statement is parsed BEFORE the failure (named "two"), since a Parse in
    // a failed block is itself 25P02, as PostgreSQL 18's exec_parse_message refuses it (recorded,
    // artie raw loud-20261015/pg18-diff); then its Bind with two values is 08P01, the count coming
    // first (ruled 2026-10-15T06:03:10Z: the Parse moved ahead of the failure, this expectation
    // unchanged; and the Parse refusal asserted on its own below).
    a.send(b'P', b"two\0SELECT $1\0\0\0");
    a.send(b'S', &[]);
    a.read_reply().ok("Parse of two, before the block");
    a.q("BEGIN").ok("begin");
    assert_eq!(a.q("SELECT * FROM nosuch").status, b'E');
    a.send(b'B', b"\0two\0\0\0\0\x02\0\0\0\x011\0\0\0\x012\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err("two values in a failed block").code, "08P01");
    assert_eq!(r.status, b'E', "the block is still failed");
    let r = round(&mut a, "SELECT $1", &[], &[b"1"]);
    assert_eq!(r.err("a Parse in a failed block").code, "25P02");
    assert_eq!(r.status, b'E', "the block is still failed");
    // C40 lead ruling: the original request includes a new Parse and two values. PostgreSQL
    // 18.6 refuses that Parse with 25P02, before either ParseComplete or BindComplete.
    let (messages, err) = a.bind_round("SELECT $1", &[], &[Some(b"1"), Some(b"2")], &[]);
    assert_eq!(err.expect("Parse in failed block").code, "25P02");
    assert_eq!(messages, vec![b'E', b'Z']);
    a.q("ROLLBACK").ok("end");
    let r = round(&mut a, "SELECT $1", &[2], &[b"1", b"2"]);
    assert_eq!(r.err("two values, format code 2").code, "08P01");
}

/// A Bind is checked before anything of its statement runs, for every statement without a `$n`
/// (its parameters are the ones Parse declared, none if it declared none): one value for `SET
/// search_path TO nosuch` or `SET foreign_keys = off` is 08P01 and the setting is unchanged; a
/// declared but unspecified type for BEGIN is 42P18; and in a failed block a Bind of anything but
/// a block exit without values is 25P02, the block still failed, as PostgreSQL's exec_bind_message
/// refuses it. The count was checked at Execute, after the prepare had performed the SET, so the
/// search path changed and the error came after (wire review 14 item 4).
#[test]
fn a_bind_is_checked_before_its_statement_runs() {
    let dir = Scratch::new("bindbefore");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    // Parse (the given declared OIDs), Bind (the given text values), Execute, Sync.
    fn round(w: &mut Wire, sql: &str, oids: &[u32], values: &[&[u8]]) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for oid in oids {
            parse.extend_from_slice(&oid.to_be_bytes());
        }
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8, 0, 0];
        bind.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            bind.extend_from_slice(&(v.len() as i32).to_be_bytes());
            bind.extend_from_slice(v);
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    let path = a.q("SHOW search_path").single("the search path");
    let r = round(&mut a, "SET search_path TO nosuch", &[], &[b"x"]);
    assert_eq!(r.err("SET with a value").code, "08P01");
    assert_eq!(
        a.q("SHOW search_path").single("the search path after"),
        path,
        "the SET ran before its Bind was refused"
    );
    a.q("CREATE TABLE fp(id INT PRIMARY KEY)").ok("parent");
    a.q("CREATE TABLE fc(pid INT REFERENCES fp(id))")
        .ok("child");
    let r = round(&mut a, "SET foreign_keys = off", &[], &[b"x"]);
    assert_eq!(r.err("SET foreign_keys with a value").code, "08P01");
    assert_eq!(
        a.q("INSERT INTO fc VALUES (5)")
            .err("keys still enforced")
            .code,
        "23503"
    );
    let r = round(&mut a, "BEGIN", &[0], &[b"x"]);
    assert_eq!(
        r.err("BEGIN with an unspecified declared type").code,
        "42P18"
    );
    assert_eq!(r.status, b'I', "no block begun");
    a.q("BEGIN").ok("begin");
    assert_eq!(a.q("SELECT * FROM nosuch").status, b'E');
    let r = round(&mut a, "ROLLBACK", &[25], &[b"x"]);
    assert_eq!(
        r.err("ROLLBACK with a value in a failed block").code,
        "25P02"
    );
    assert_eq!(r.status, b'E', "the block is still failed");
    let r = round(&mut a, "SELECT 1", &[], &[]);
    assert_eq!(r.err("a SELECT in a failed block").code, "25P02");
    a.q("ROLLBACK").ok("end");
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

/// A named portal runs once, as in PostgreSQL: a second Execute of a portal whose statement ran to
/// completion without rows (an UPDATE) is 55000 'portal "p" cannot be run', which fails the
/// pipeline, so its implicit block rolls back and the row is unchanged; a second Execute of a query
/// portal that returned all its rows answers "SELECT 0"; and a named portal is gone once the
/// transaction it was bound in ends, so Execute of it after Sync is 34000 'portal "p" does not
/// exist'. The UPDATE ran twice and committed both, a finished query answered NoData, and portals
/// outlived their transaction (wire review 12 item 5).
#[test]
fn a_named_portal_runs_once_and_ends_with_its_transaction() {
    let dir = Scratch::new("portalonce");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE c(id INT PRIMARY KEY, n INT)").ok("c");
    a.q("INSERT INTO c VALUES (1, 0)").ok("row");
    fn frame(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
    }
    // Parse the unnamed statement, Bind it to portal "p", then `executes` Executes of p and a Sync;
    // every message type of the reply, the first error, and each CommandComplete's tag.
    fn rounds(
        w: &mut Wire,
        sql: &str,
        bind: bool,
        executes: usize,
    ) -> (Vec<u8>, Option<WireError>, Vec<String>) {
        let mut out = Vec::new();
        if bind {
            let mut parse = vec![0u8];
            parse.extend_from_slice(sql.as_bytes());
            parse.extend_from_slice(&[0, 0, 0]);
            frame(&mut out, b'P', &parse);
            frame(&mut out, b'B', &[b'p', 0, 0, 0, 0, 0, 0, 0, 0]);
        }
        for _ in 0..executes {
            frame(&mut out, b'E', &[b'p', 0, 0, 0, 0, 0]);
        }
        frame(&mut out, b'S', &[]);
        w.s.write_all(&out).unwrap();
        let (mut tags, mut error, mut completes) = (Vec::new(), None, Vec::new());
        loop {
            let mut head = [0u8; 5];
            w.s.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            w.s.read_exact(&mut body).unwrap();
            match head[0] {
                b'E' if error.is_none() => error = Some(error_fields(&body)),
                b'C' => completes.push(cstr(&body)),
                _ => {}
            }
            tags.push(head[0]);
            if head[0] == b'Z' {
                return (tags, error, completes);
            }
        }
    }
    let (_, error, completes) = rounds(&mut a, "UPDATE c SET n = n + 1 WHERE id = 1", true, 2);
    let e = error.expect("the second Execute of a finished UPDATE portal");
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("55000", "portal \"p\" cannot be run")
    );
    assert_eq!(completes, vec!["UPDATE 1".to_string()]);
    assert_eq!(
        a.q("SELECT n FROM c WHERE id = 1")
            .single("the pipeline rolled back"),
        "0"
    );
    let (_, error, completes) = rounds(&mut a, "SELECT n FROM c", true, 2);
    assert!(error.is_none(), "{error:?}");
    assert_eq!(
        completes,
        vec!["SELECT 1".to_string(), "SELECT 0".to_string()]
    );
    let (_, error, _) = rounds(&mut a, "", false, 1);
    let e = error.expect("Execute of p after its transaction ended");
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("34000", "portal \"p\" does not exist")
    );
    assert_eq!(a.q("SELECT 1").single("the session answers"), "1");
}

/// An empty statement over the extended protocol is no error: Describe of its portal answers NoData,
/// Execute answers EmptyQueryResponse (whatever result formats the Bind named), and a pipeline it
/// sits in commits at Sync, as in PostgreSQL. It failed at Describe and Execute with "contains no
/// statements", and Execute had already joined the implicit block, so the pipeline's INSERT was
/// rolled back (wire review 12 item 4).
#[test]
fn an_empty_extended_statement_is_an_empty_query() {
    let dir = Scratch::new("emptyext");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut out = Vec::new();
    let mut put = |tag: u8, body: &[u8]| {
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
    };
    let insert = b"INSERT INTO t VALUES (5, 'five')";
    let mut parse = vec![0u8];
    parse.extend_from_slice(insert);
    parse.extend_from_slice(&[0, 0, 0]);
    put(b'P', &parse);
    put(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
    put(b'E', &[0, 0, 0, 0, 0]);
    for sql in [&b""[..], &b" ;"[..]] {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql);
        parse.extend_from_slice(&[0, 0, 0]);
        put(b'P', &parse);
        // Two result formats for a statement of no columns: ignored.
        put(b'B', &[0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0]);
        put(b'D', b"P\0");
        put(b'E', &[0, 0, 0, 0, 0]);
    }
    put(b'S', &[]);
    a.s.write_all(&out).unwrap();
    let mut tags = Vec::new();
    let mut error = None;
    loop {
        let mut head = [0u8; 5];
        a.s.read_exact(&mut head).unwrap();
        let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        a.s.read_exact(&mut body).unwrap();
        if head[0] == b'E' && error.is_none() {
            error = Some(error_fields(&body));
        }
        tags.push(head[0]);
        if head[0] == b'Z' {
            assert_eq!(body, vec![b'I'], "idle after Sync");
            break;
        }
    }
    assert!(error.is_none(), "an error: {error:?}");
    assert_eq!(
        tags.iter().filter(|t| **t == b'I').count(),
        2,
        "one EmptyQueryResponse per empty statement: {tags:?}"
    );
    assert_eq!(
        tags.iter().filter(|t| **t == b'n').count(),
        2,
        "NoData for each empty portal: {tags:?}"
    );
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 5")
            .single("the pipeline committed"),
        "five"
    );
}

/// A Parse of more than one statement is refused (42601 "cannot insert multiple commands into a
/// prepared statement"), as PostgreSQL refuses it, failing the block it arrives in. The prepare
/// translated only the first statement, so `COMMIT; INSERT ...` inside a block committed it,
/// skipped the INSERT and answered success (wire review 14 item 5).
#[test]
fn a_parse_of_several_statements_is_refused() {
    let dir = Scratch::new("parsemulti");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')")
        .ok("a write in the block");
    let r = a.x("COMMIT; INSERT INTO t VALUES (9, 'nine')", &[]);
    assert_eq!(r.err("two statements in one Parse").code, "42601");
    assert_eq!(r.status, b'E', "the block failed");
    a.q("ROLLBACK").ok("end");
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id IN (2, 9)")
            .single("nothing kept"),
        "0"
    );
}

/// A statement of nothing but semicolons and comments (`;;`, `-- c`, `/* c */`) is PostgreSQL's
/// empty query on both protocols: EmptyQueryResponse, no error, and a pipeline it sits in commits
/// its earlier INSERT at Sync. A lone U+00A0 is not whitespace to PostgreSQL's lexer: 42601. Read
/// by a text test (whitespace or a lone `;`), these reached the translator's "No statements
/// found", an ERROR that rolled the pipeline back (wire review 15 item 6).
#[test]
fn a_comment_only_statement_is_an_empty_query() {
    let dir = Scratch::new("commentonly");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let blanks: [&[u8]; 3] = [b";;", b"-- c", b"/* c */"];
    for sql in blanks {
        let text = std::str::from_utf8(sql).unwrap();
        let r = a.q(text);
        assert!(r.error.is_none(), "{text:?} by simple query: {:?}", r.error);
    }
    let r = a.q("\u{a0}");
    assert_eq!(r.err("a lone U+00A0").code, "42601");
    let mut out = Vec::new();
    let mut put = |tag: u8, body: &[u8]| {
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
    };
    put(b'P', b"\0INSERT INTO t VALUES (7, 'seven')\0\0\0");
    put(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
    put(b'E', &[0, 0, 0, 0, 0]);
    for sql in blanks {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql);
        parse.extend_from_slice(&[0, 0, 0]);
        put(b'P', &parse);
        put(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        put(b'D', b"P\0");
        put(b'E', &[0, 0, 0, 0, 0]);
    }
    put(b'S', &[]);
    a.s.write_all(&out).unwrap();
    let (tags, errors, status) = read_raw_reply(&mut a);
    assert!(errors.is_empty(), "errors: {errors:?}");
    assert_eq!(
        tags.iter().filter(|t| **t == b'I').count(),
        3,
        "an EmptyQueryResponse per blank statement: {tags:?}"
    );
    assert_eq!(status, b'I');
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 7")
            .single("the pipeline committed"),
        "seven"
    );
}

/// A named portal of an empty statement executed twice answers EmptyQueryResponse both times, and
/// the pipeline it sits in commits at Sync, as PostgreSQL answers it (exec_execute_message's empty
/// command, before any portal-state check). The first Execute marked the portal done, so the second
/// was 55000 "portal cannot be run" and rolled the pipeline's INSERT back (wire review 15 item 1).
#[test]
fn an_empty_named_portal_runs_twice_as_an_empty_query() {
    let dir = Scratch::new("emptyportal2");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut out = Vec::new();
    let mut put = |tag: u8, body: &[u8]| {
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
    };
    put(b'P', b"\0INSERT INTO t VALUES (6, 'six')\0\0\0");
    put(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
    put(b'E', &[0, 0, 0, 0, 0]);
    put(b'P', b"s2\0\0\0\0");
    put(b'B', b"p\0s2\0\0\0\0\0\0\0");
    put(b'E', b"p\0\0\0\0\0");
    put(b'E', b"p\0\0\0\0\0");
    put(b'S', &[]);
    a.s.write_all(&out).unwrap();
    let (tags, errors, status) = read_raw_reply(&mut a);
    assert!(errors.is_empty(), "errors: {errors:?}");
    assert_eq!(
        tags.iter().filter(|t| **t == b'I').count(),
        2,
        "an EmptyQueryResponse per Execute: {tags:?}"
    );
    assert_eq!(status, b'I');
    assert_eq!(
        a.q("SELECT v FROM t WHERE id = 6")
            .single("the pipeline committed"),
        "six"
    );
}

/// A wrong-length result-format list is refused before the statement runs, shown by a statement
/// that would FAIL if it ran: `INSERT ... RETURNING` of a key that exists answers 08P01 (the format
/// list), never 23505 (the key). a_wrong_result_format_list_is_refused_before_the_side_effect's
/// INSERT leg cannot tell the orders apart: the implicit rollback hides its row whichever runs
/// first (wire review 15 item 21).
#[test]
fn a_wrong_result_format_list_is_refused_before_a_failing_insert_runs() {
    let dir = Scratch::new("formatdup");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let sql = "INSERT INTO t VALUES (1, 'dup') RETURNING id, v";
    let mut parse = vec![0u8];
    parse.extend_from_slice(sql.as_bytes());
    parse.extend_from_slice(&[0, 0, 0]);
    a.send(b'P', &parse);
    a.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0]);
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err(sql).code, "08P01", "the format list is checked first");
    let r = a.xt(sql, &[]);
    assert_eq!(r.err(sql).code, "23505", "control: run, the key refuses it");
}

/// A wrong-length result-format list is refused BEFORE a statement with a side effect runs: a branch
/// create, switch or delete changes nothing, and an `INSERT ... RETURNING` writes no row; the
/// refusals are worded as PostgreSQL words them. No test pinned the order: the earlier rows change
/// nothing, so a fix that checked after the create would have left the branch behind and passed
/// (wire review 12 item 3). The guard is `result_format(format, 0, 1)?` in `branch()` before
/// `text_arg`, and `result_fields` before `execute_query` runs.
#[test]
fn a_wrong_result_format_list_is_refused_before_the_side_effect() {
    let dir = Scratch::new("formatorder");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('old')")
        .ok("a branch to keep");
    // Parse, Bind with the given result codes (no parameters), Execute, Sync.
    fn round(w: &mut Wire, sql: &str, rcodes: &[i16]) -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        let mut bind = vec![0u8, 0u8, 0, 0, 0, 0];
        bind.extend_from_slice(&(rcodes.len() as i16).to_be_bytes());
        for c in rcodes {
            bind.extend_from_slice(&c.to_be_bytes());
        }
        w.send(b'B', &bind);
        w.send(b'E', &[0, 0, 0, 0, 0]);
        w.send(b'S', &[]);
        w.read_reply()
    }
    for (sql, columns) in [
        ("SELECT turso_branch_create('rf')", 1),
        ("SELECT turso_branch_switch('old')", 1),
        ("SELECT turso_branch_delete('old')", 1),
    ] {
        let r = round(&mut a, sql, &[0, 0]);
        let e = r.err(sql);
        assert_eq!(e.code, "08P01", "{sql}");
        assert_eq!(
            e.message,
            format!("bind message has 2 result formats but query has {columns} columns"),
            "{sql}"
        );
    }
    let r = a.q("SELECT turso_branch_switch('rf')");
    assert_eq!(r.err("rf must not exist").code, "3D000");
    assert_eq!(
        a.q("SELECT turso_branch_current()")
            .single("still on the trunk"),
        "main"
    );
    a.q("SELECT turso_branch_switch('old')")
        .ok("old was not deleted");
    a.q("SELECT turso_branch_switch('main')")
        .ok("back to the trunk");
    let sql = "INSERT INTO t VALUES (8, 'y') RETURNING id, v";
    let r = round(&mut a, sql, &[0, 0, 0]);
    let e = r.err(sql);
    assert_eq!(e.code, "08P01", "{sql}");
    assert_eq!(
        e.message,
        "bind message has 3 result formats but query has 2 columns"
    );
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 8")
            .single("no row written"),
        "0"
    );
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

/// A view libpg_query cannot reread types a parameter only from columns the engine traced to a
/// table column; a computed one (count(*), max(id) + 1) or one read through an inner view the
/// engine gives a placeholder TEXT, so a parameter compared with it is refused 42P18 (or typed as
/// PostgreSQL types it), never text: typed text, `n = $1` bound '1' as a string and the row went
/// missing silently, where the base refused it (wire review 18 item 4).
#[test]
fn a_view_libpg_query_cannot_reread_never_types_a_computed_column_text() {
    let dir = Scratch::new("viewcomputed");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in [
        "CREATE TABLE vt(id INT PRIMARY KEY, flag BOOLEAN)",
        "INSERT INTO vt VALUES (1, true), (2, false)",
        "CREATE VIEW vc AS SELECT count(*) AS n, max(id) + 1 AS m FROM vt WHERE flag IS TRUE",
        "CREATE VIEW vinner AS SELECT id, flag FROM vt",
        "CREATE VIEW vouter AS SELECT id FROM vinner WHERE flag IS TRUE",
    ] {
        a.q(sql).ok(sql);
    }
    for (sql, typed) in [
        ("SELECT 1 FROM vc WHERE n = $1", 20),
        ("SELECT 1 FROM vc WHERE m = $1", 23),
        ("SELECT 1 FROM vouter WHERE id = $1", 23),
    ] {
        let r = a.describe_statement(sql);
        let refused = r.error.as_ref().is_some_and(|e| e.code == "42P18");
        assert!(
            refused || r.params == Some(vec![typed]),
            "{sql}: {:?} {:?}, want [{typed}] or 42P18, never [25]",
            r.params,
            r.error
        );
    }
}

/// A view whose stored text libpg_query cannot read (the engine stores a view as SQLite text, and
/// `IS TRUE` becomes `IS 1`) is typed from the engine's own columns for the view: `id = $1` over
/// it is int4, not refused 42P18 (wire review 14 item 10).
#[test]
fn a_view_libpg_query_cannot_reread_is_typed_from_the_engine() {
    let dir = Scratch::new("viewisone");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE vt(id INT PRIMARY KEY, flag BOOLEAN)")
        .ok("vt");
    a.q("INSERT INTO vt VALUES (1, true), (2, false)")
        .ok("rows");
    a.q("CREATE VIEW vtrue AS SELECT id FROM vt WHERE flag IS TRUE")
        .ok("an IS TRUE view");
    let sql = "SELECT id FROM vtrue WHERE id = $1";
    let r = a.describe_statement(sql);
    assert!(
        r.error.is_none() && r.params == Some(vec![23]),
        "{sql}: {:?} {:?}, want [23]",
        r.params,
        r.error
    );
    let r = a.xt(sql, &[(0, 0, b"1")]).ok(sql);
    assert_eq!(r.rows, vec![vec![Some("1".to_string())]]);
}

/// KNOWN RED until the engine keeps a view's PostgreSQL text: `CREATE VIEW vb AS SELECT true AS
/// flag` is stored as SQLite text (`SELECT 1 AS flag`), so `flag = $1` is typed int4 where
/// PostgreSQL types bool (16). Storing the PostgreSQL text, as tables are stored, needs a view hook
/// in the engine's Dialect (core/, not this lane's): E5-QUEUE V1 (wire review 14 item 10).
#[test]
fn a_bool_literal_view_types_its_parameter_bool() {
    let dir = Scratch::new("viewbool");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE VIEW vb AS SELECT true AS flag").ok("vb");
    let r = a.describe_statement("SELECT 1 FROM vb WHERE flag = $1");
    assert!(
        r.error.is_none() && r.params == Some(vec![16]),
        "{:?} {:?}, want [16]",
        r.params,
        r.error
    );
}

/// An undeclared parameter compared with a column of a relation the inference walk must open to
/// type it is typed from that relation: a view's column (walked from the view's query), an
/// alias-less subquery's, a `*` a CTE or derived table expands, and a column of the innermost
/// relation that has it even when an outer one has a column of that name. Each fell to the text
/// fallback, or bound to the outer relation's column: count(*) compared as Numeric against Text and
/// rows went missing, and a text view column was bound as the outer int column, so 'abc' failed
/// (wire review 11 item 3: review 8 item 7's failure, still open; E5-QUEUE P7 reopened). Every case
/// is checked before the test fails, so one run names all of them.
#[test]
fn a_parameter_is_typed_through_views_subqueries_and_stars() {
    const INT8: u32 = 20;
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    let dir = Scratch::new("inferviews");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, n INT, name TEXT)")
        .ok("p");
    a.q("INSERT INTO p VALUES (1, 5, 'abc'), (2, 7, 'def'), (3, 9, 'ghi')")
        .ok("rows");
    a.q("CREATE VIEW d AS SELECT count(*) AS c FROM p")
        .ok("view d");
    a.q("CREATE VIEW v AS SELECT id, name AS n FROM p")
        .ok("view v");
    let cases: Vec<(&str, u32, &str, Vec<&str>)> = vec![
        ("SELECT c FROM d WHERE c > $1", INT8, "2", vec!["3"]),
        (
            "SELECT c FROM (SELECT count(*) AS c FROM p) WHERE c > $1",
            INT8,
            "2",
            vec!["3"],
        ),
        (
            "WITH w AS (SELECT * FROM p) SELECT id FROM w WHERE n > $1 ORDER BY id",
            INT4,
            "6",
            vec!["2", "3"],
        ),
        (
            "WITH w(a, b, c) AS (SELECT * FROM p) SELECT a FROM w WHERE a = $1",
            INT4,
            "1",
            vec!["1"],
        ),
        (
            "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM v WHERE v.id = p.id AND n = $1)",
            TEXT,
            "abc",
            vec!["1"],
        ),
    ];
    let mut wrong = Vec::new();
    for (sql, oid, value, want) in cases {
        let r = a.describe_statement(sql);
        if r.error.is_some() || r.params != Some(vec![oid]) {
            wrong.push(format!(
                "{sql}: Describe {:?} {:?}, want [{oid}]",
                r.params, r.error
            ));
        }
        let r = a.xt(sql, &[(0, 0, value.as_bytes())]);
        let got: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].clone().unwrap_or_default())
            .collect();
        if r.error.is_some() || got != want {
            wrong.push(format!(
                "{sql} with {value}: {got:?} {:?}, want {want:?}",
                r.error
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// An undeclared parameter compared with a column of a FROM item the walk did not model is typed
/// from that item, as PostgreSQL types it (OIDs from PostgreSQL's rules; a PG18 record_pg.sh
/// recording is owed): a scalar subquery's FROM holding a derived table (a, c) or a join (b), a
/// function in FROM at the same level (with and without a column alias), a function whose column
/// shadows an outer one in an EXISTS, and a `*` over a function. A scalar subquery's FROM kept only
/// its tables and a function in FROM added nothing to its level, so $1 was typed from the outer p's
/// column of the name (a: text where PostgreSQL says int8; b: int4 where it says text, and the
/// query failed), refused 42P18 with no such outer column (c), or fell to text (wire review 14 item
/// 2: what 85195930a claimed to have closed). Every case is checked before the test fails.
#[test]
fn a_parameter_is_typed_by_every_kind_of_from_item() {
    const INT8: u32 = 20;
    const INT4: u32 = 23;
    const TEXT: u32 = 25;
    let dir = Scratch::new("inferfromitems");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, n INT, name TEXT)")
        .ok("p");
    a.q("INSERT INTO p VALUES (1, 5, 'abc'), (2, 7, 'def'), (3, 9, 'ghi')")
        .ok("rows");
    a.q("CREATE VIEW d AS SELECT count(*) AS c FROM p")
        .ok("view d");
    a.q("CREATE VIEW v AS SELECT id, name AS n FROM p")
        .ok("view v");
    let all = vec!["1", "2", "3"];
    let cases: Vec<(&str, u32, &str, Vec<&str>)> = vec![
        (
            "SELECT id FROM p WHERE (SELECT name FROM (SELECT count(*) AS name FROM p) s) > $1 \
             ORDER BY id",
            INT8,
            "2",
            all.clone(),
        ),
        (
            "SELECT id FROM p WHERE (SELECT n FROM v JOIN d ON true ORDER BY v.id LIMIT 1) = $1 \
             ORDER BY id",
            TEXT,
            "abc",
            all.clone(),
        ),
        (
            "SELECT id FROM p WHERE (SELECT c FROM (SELECT count(*) AS c FROM p) s) > $1 \
             ORDER BY id",
            INT8,
            "2",
            all.clone(),
        ),
        (
            "SELECT g FROM generate_series(1, 10) AS g WHERE g > $1 ORDER BY g",
            INT4,
            "8",
            vec!["9", "10"],
        ),
        (
            "SELECT x FROM generate_series(1, 10) AS g(x) WHERE x > $1 ORDER BY x",
            INT4,
            "8",
            vec!["9", "10"],
        ),
        (
            "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM generate_series(1, 3) AS name \
             WHERE name = $1) ORDER BY id",
            INT4,
            "2",
            all.clone(),
        ),
        (
            "SELECT c FROM (SELECT * FROM generate_series(1, 3) AS c) s WHERE c > $1 ORDER BY c",
            INT4,
            "1",
            vec!["2", "3"],
        ),
    ];
    let mut wrong = Vec::new();
    for (sql, oid, value, want) in cases {
        let r = a.describe_statement(sql);
        if r.error.is_some() || r.params != Some(vec![oid]) {
            wrong.push(format!(
                "{sql}: Describe {:?} {:?}, want [{oid}]",
                r.params, r.error
            ));
        }
        let r = a.xt(sql, &[(0, 0, value.as_bytes())]);
        let got: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].clone().unwrap_or_default())
            .collect();
        if r.error.is_some() || got != want {
            wrong.push(format!(
                "{sql} with {value}: {got:?} {:?}, want {want:?}",
                r.error
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// A bare column a join merges (USING, NATURAL) is typed from the merged column: `JOIN q USING
/// (id) WHERE id = $1` and the NATURAL form type $1 int4. One found in two relations of a level was
/// read as not found, so $1 fell to text (wire review 14 item 9). A column two relations have that
/// no join merges stays PostgreSQL's 42702.
#[test]
fn a_join_merged_column_types_its_parameter() {
    const INT4: u32 = 23;
    let dir = Scratch::new("joinusing");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE jp(id INT PRIMARY KEY, n INT)").ok("jp");
    a.q("CREATE TABLE jq(id INT PRIMARY KEY, flag BOOLEAN)")
        .ok("jq");
    a.q("INSERT INTO jp VALUES (1, 10), (2, 20)").ok("jp rows");
    a.q("INSERT INTO jq VALUES (1, true), (2, false)")
        .ok("jq rows");
    let mut wrong = Vec::new();
    for sql in [
        "SELECT n FROM jp JOIN jq USING (id) WHERE id = $1",
        "SELECT n FROM jp NATURAL JOIN jq WHERE id = $1",
    ] {
        let r = a.describe_statement(sql);
        if r.error.is_some() || r.params != Some(vec![INT4]) {
            wrong.push(format!(
                "{sql}: {:?} {:?}, want [{INT4}]",
                r.params, r.error
            ));
        }
        let r = a.xt(sql, &[(0, 0, b"2")]);
        if r.error.is_some() || r.rows != vec![vec![Some("20".to_string())]] {
            wrong.push(format!(
                "{sql} with 2: {:?} {:?}, want [20]",
                r.rows, r.error
            ));
        }
    }
    let sql = "SELECT n FROM jp JOIN jq ON true WHERE id = $1";
    let r = a.describe_statement(sql);
    if r.error.as_ref().map(|e| e.code.as_str()) != Some("42702") {
        wrong.push(format!("{sql}: {:?} {:?}, want 42702", r.params, r.error));
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// A dense cycle of views costs one read of each view per statement: ten views that each list all
/// ten in FROM cost about e*9! (986k) libpg_query parses at one Describe, which pinned the session
/// thread with no cancel. The Describe is answered (refused: the views are circular) within 2 s, and
/// another session is served (wire review 16 item 3).
#[test]
fn a_dense_view_cycle_is_described_at_once() {
    let dir = Scratch::new("viewcycle");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(x INT)").ok("t");
    let all = (1..=10)
        .map(|i| format!("w{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    for i in 1..=10 {
        let sql = format!("CREATE VIEW w{i} AS SELECT w1.x FROM {all}");
        a.q(&sql).ok(&format!("premise: {sql}"));
    }
    let started = Instant::now();
    let r = a.describe_statement("SELECT * FROM w1 WHERE x = $1");
    let took = started.elapsed();
    assert!(
        r.error.is_some(),
        "a circular view was described: {:?}",
        r.params
    );
    assert!(took < Duration::from_secs(2), "the Describe took {took:?}");
    let mut b = server.connect();
    assert_eq!(b.q("SELECT 1").single("a second session"), "1");
}

/// A circular view is answered with an error and the server serves on. The inference walk opened a
/// view by walking its query, which reached the view again: with no visited set it recursed until
/// the session thread's 8 MiB stack overflowed, which aborts the process, every session with it,
/// from one unauthenticated query with a parameter. The walk runs before translation, so the
/// engine's own "circularly defined" refusal never ran (wire review 14 item 1). Three cycles: a
/// view over itself, a pair, and a chain closed by dropping and re-creating its middle; each queried
/// with an undeclared $1 by simple query, by Parse/Describe statement, and by Parse/Bind/Describe
/// portal/Execute. The server is a spawned process, so an abort fails this test, not the harness.
#[test]
fn a_circular_view_is_an_error_and_the_server_serves_on() {
    let dir = Scratch::new("circularview");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(x INT)").ok("t");
    a.q("INSERT INTO t VALUES (1)").ok("t row");
    for sql in [
        "CREATE VIEW s AS SELECT * FROM s",
        "CREATE VIEW p1 AS SELECT * FROM p2",
        "CREATE VIEW p2 AS SELECT * FROM p1",
        "CREATE VIEW c1 AS SELECT x FROM t",
        "CREATE VIEW c2 AS SELECT x FROM c1",
        "DROP VIEW c1",
        "CREATE VIEW c1 AS SELECT x FROM c2",
    ] {
        a.q(sql).ok(&format!("premise: {sql}"));
    }
    for view in ["s", "p1", "c1"] {
        let sql = format!("SELECT * FROM {view} WHERE x = $1");
        let replies = [
            ("simple", a.q(&sql)),
            ("Describe", a.describe_statement(&sql)),
            ("Execute", a.xt(&sql, &[(0, 0, b"1")])),
        ];
        for (how, r) in replies {
            assert!(r.error.is_some(), "{sql} by {how}: no error: {:?}", r.rows);
            let mut b = server
                .connect_to("postgres")
                .unwrap_or_else(|e| panic!("{sql} by {how}: the server is gone: {e:?}"));
            assert_eq!(
                b.q("SELECT 1").single("a second session"),
                "1",
                "{sql} by {how}"
            );
        }
    }
}

/// A recursive CTE's self-reference is typed by its non-recursive term, as PostgreSQL types it, so
/// `x < $1` in the recursive term compares x (int4, from `SELECT 1`) with an int4 and the recursion
/// stops at 10. The CTE was walked before it was in scope, so x was untyped, $1 bound as text, and
/// the engine's self-reference (Blob affinity) made `x < '10'` true for every x: the recursion never
/// ended, buffering without bound (wire review 11 item 2). The outer LIMIT keeps the base from
/// running away: there it returns 20 rows, here 10.
#[test]
fn a_recursive_ctes_self_reference_types_its_parameter() {
    const INT4: u32 = 23;
    let dir = Scratch::new("recursivecte");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < $1) \
               SELECT x FROM cnt LIMIT 20";
    let r = a.describe_statement(sql).ok("describe");
    assert_eq!(r.params, Some(vec![INT4]));
    let r = a.xt(sql, &[(0, 0, b"10")]).ok("execute");
    let got: Vec<String> = r.rows.iter().map(|row| row[0].clone().unwrap()).collect();
    let want: Vec<String> = (1..=10).map(|n| n.to_string()).collect();
    assert_eq!(got, want);
}

/// The 42P18 refusal of a parameter compared with something no context types applies only to a
/// parameter the client left untyped (OID 0, or none declared): one declared in Parse has its
/// declared type, at Describe and at Execute, as in PostgreSQL. The refusal was made at prepare,
/// from the text alone, before the declared OIDs were read: `typeof(v) = $1` declared text was
/// 42P18 at Describe and at Execute alike, so it could never run, and inside a block its Describe
/// failed the block (wire review 11 item 1, a regression from 935586643).
#[test]
fn a_declared_parameter_is_never_refused_as_untyped() {
    const TEXT: u32 = 25;
    let dir = Scratch::new("declared42p18");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let sql = "SELECT id FROM t WHERE typeof(v) = $1";
    // Parse declaring `oid`, Describe the statement, Sync.
    let describe = |a: &mut Wire, oid: u32| -> Reply {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&1i16.to_be_bytes());
        parse.extend_from_slice(&oid.to_be_bytes());
        a.send(b'P', &parse);
        a.send(b'D', b"S\0");
        a.send(b'S', &[]);
        a.read_reply()
    };
    let r = describe(&mut a, TEXT).ok("declared text, Describe");
    assert_eq!(r.params, Some(vec![TEXT]));
    let r = a
        .xt(sql, &[(TEXT, 0, b"text")])
        .ok("declared text, Execute");
    assert_eq!(r.rows, vec![vec![Some("1".to_string())]]);
    let r = describe(&mut a, 0);
    assert_eq!(r.err("undeclared, Describe").code, "42P18");
    let r = a.xt(sql, &[(0, 0, b"text")]);
    assert_eq!(r.err("undeclared, Execute").code, "42P18");
    a.q("BEGIN").ok("begin");
    describe(&mut a, TEXT).ok("declared text, Describe in a block");
    let r = a.q("SELECT 1").ok("the block is live");
    assert_eq!(r.status, b'T');
    a.q("ROLLBACK").ok("end");
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
    const TEXT: u32 = 25;
    let dir = Scratch::new("untypedctx");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, name TEXT, n INT)")
        .ok("p");
    a.q("INSERT INTO p VALUES (1, 'abc', 10), (2, 'de', NULL), (3, 'fghi', 30)")
        .ok("rows");
    // Every case is checked before the test fails, so one run names every wrong one.
    let mut wrong = Vec::new();
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
        // 'true', so a parameter bound as text ('true' is not the boolean) would return row 1 only;
        // and 'false', the arm 8570d4847 replaced (wire review 15 item 13: kept, not replaced).
        (
            "SELECT id FROM p WHERE $1 OR id = 1 ORDER BY id",
            BOOL,
            "true",
            vec!["1", "2", "3"],
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
        let r = a.describe_statement(sql);
        if r.error.is_some() || r.params != Some(vec![want_type]) {
            wrong.push(format!(
                "{sql}: Describe {:?} {:?}, want [{want_type}]",
                r.params, r.error
            ));
        }
        let r = a.xt(sql, &[(0, 0, bind.as_bytes())]);
        let got: Vec<String> = r
            .rows
            .iter()
            .map(|row| row[0].clone().unwrap_or_default())
            .collect();
        if r.error.is_some() || got != want_rows {
            wrong.push(format!(
                "{sql} with {bind}: {got:?} {:?}, want {want_rows:?}",
                r.error
            ));
        }
    }
    // Describes of further contexts the walk types (PostgreSQL's types by these fixtures'
    // semantics; the PG18 recording is owed with E5-QUEUE P7). A TEXT expectation cannot tell a
    // typed context from the text fallback; those rows guard against a refusal only (wire review 15
    // items 14 and 28: the shadowing row now expects int8, the SET rows include two non-text
    // columns, and the condition, OFFSET and arithmetic contexts are added).
    for (sql, want) in [
        ("SELECT count(*) FROM p GROUP BY n > $1", vec![INT4]),
        ("SELECT id FROM p ORDER BY n = $1, id", vec![INT4]),
        (
            "SELECT sum(n) OVER (PARTITION BY n > $1) FROM p",
            vec![INT4],
        ),
        (
            "INSERT INTO p VALUES (1, 'x', 0) ON CONFLICT (id) DO UPDATE SET n = excluded.n \
             WHERE excluded.n > $1",
            vec![INT4],
        ),
        (
            "UPDATE p SET (n, name) = ($1, $2) WHERE id = 1",
            vec![INT4, TEXT],
        ),
        (
            "UPDATE p SET (id, n) = ($1, $2) WHERE id = 1",
            vec![INT4, INT4],
        ),
        (
            "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM (SELECT count(*) AS n FROM p) AS q \
             WHERE n = $1)",
            vec![INT8],
        ),
        (
            "SELECT p.id FROM p JOIN p AS q ON $1 WHERE p.id = 1",
            vec![BOOL],
        ),
        ("SELECT id FROM p WHERE $1 AND id = 1", vec![BOOL]),
        ("SELECT id FROM p WHERE NOT $1", vec![BOOL]),
        ("SELECT CASE WHEN $1 THEN 1 ELSE 0 END FROM p", vec![BOOL]),
        ("SELECT id FROM p WHERE $1 IS TRUE", vec![BOOL]),
        ("SELECT id FROM p ORDER BY id OFFSET $1", vec![INT8]),
        ("SELECT id FROM p WHERE n + $1 = 11", vec![INT4]),
        (
            "WITH w(a) AS (SELECT n FROM p) SELECT a FROM w WHERE a = $1",
            vec![INT4],
        ),
        ("SELECT id FROM p WHERE upper($1) = name", vec![TEXT]),
        ("SELECT id FROM p WHERE greatest(n, 0) = $1", vec![INT4]),
    ] {
        let r = a.describe_statement(sql);
        if r.error.is_some() || r.params != Some(want.clone()) {
            wrong.push(format!(
                "{sql}: Describe {:?} {:?}, want {want:?}",
                r.params, r.error
            ));
        }
    }
    // ON CONFLICT DO UPDATE: its SET takes the column's type and its WHERE is boolean.
    let sql = "INSERT INTO p VALUES (1, 'x', 0) ON CONFLICT (id) DO UPDATE SET n = $1 WHERE $2";
    let r = a.describe_statement(sql);
    if r.error.is_some() || r.params != Some(vec![INT4, BOOL]) {
        wrong.push(format!("{sql}: Describe {:?} {:?}", r.params, r.error));
    }
    let r = a.xt(sql, &[(0, 0, b"11"), (0, 0, b"true")]);
    if r.error.is_some() {
        wrong.push(format!("{sql}: {:?}", r.error));
    }
    let n = a.q("SELECT n FROM p WHERE id = 1").single("n");
    if n != "11" {
        wrong.push(format!("ON CONFLICT SET n = $1 left n = {n}"));
    }
    // Compared with something no context types (a function the engine has, which the walk does not
    // type): exactly 42P18, the lead's fail-closed rule, where the base served it as text; a
    // function that does not exist is the engine's 42883. ENGINE-ONLY: PostgreSQL has no typeof
    // and answers 42883 for the first row too, so a PG18 re-recording of E5-QUEUE P7 must not
    // "correct" it; it is the only fail-closed probe here (wire review 15 item 27).
    for (sql, code) in [
        ("SELECT id FROM p WHERE typeof(name) = $1", "42P18"),
        ("SELECT id FROM p WHERE no_such_typing(name) = $1", "42883"),
    ] {
        let r = a.describe_statement(sql);
        if r.error.as_ref().map(|e| e.code.as_str()) != Some(code) {
            wrong.push(format!("{sql}: {:?}, want {code}", r.error));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// INTERSECT ALL and EXCEPT ALL are refused (0A000): the engine has neither, and they ran as
/// INTERSECT and EXCEPT, dropping the duplicates PostgreSQL keeps (union.out:271-277: `SELECT 1
/// UNION ALL SELECT 1 INTERSECT ALL SELECT 1` keeps two). A VALUES arm runs as a set operation's arm
/// (`SELECT 1 UNION VALUES (2)` is {1, 2}); it failed (wire review 11 item 14).
#[test]
fn intersect_all_and_except_all_are_refused_and_values_arms_run() {
    let dir = Scratch::new("setopall");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for sql in [
        "SELECT 1 INTERSECT ALL SELECT 1",
        "SELECT 1 EXCEPT ALL SELECT 2",
        "SELECT 1 UNION ALL (SELECT 1 INTERSECT ALL SELECT 1)",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
    }
    let r = a
        .q("SELECT 1 AS x UNION VALUES (2) ORDER BY 1")
        .ok("a VALUES arm");
    assert_eq!(
        r.rows,
        vec![vec![Some("1".to_string())], vec![Some("2".to_string())]]
    );
    let r = a
        .q("VALUES (3) UNION ALL SELECT 4 ORDER BY 1")
        .ok("a VALUES left arm");
    assert_eq!(
        r.rows,
        vec![vec![Some("3".to_string())], vec![Some("4".to_string())]]
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

/// DELETE ... USING ... RETURNING that names a USING relation, or returns `*`, is refused (0A000)
/// and deletes nothing: the delete returns only its target's columns, so `RETURNING k.flag` was
/// 42703 and `RETURNING *` lacked the USING columns PostgreSQL returns (wire review 11 item 13). A
/// RETURNING over the target's own columns runs.
#[test]
fn delete_using_returning_a_using_column_is_refused() {
    let dir = Scratch::new("deleteusingret");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b')").ok("d rows");
    a.q("CREATE TABLE k(id INT, flag BOOLEAN)").ok("k");
    a.q("INSERT INTO k VALUES (2, true)").ok("k row");
    for sql in [
        "DELETE FROM d USING k WHERE d.id = k.id RETURNING k.flag",
        "DELETE FROM d USING k AS j WHERE d.id = j.id RETURNING d.v, j.*",
        "DELETE FROM d USING k WHERE d.id = k.id RETURNING *",
    ] {
        assert_eq!(a.q(sql).err(sql).code, "0A000", "{sql}");
        assert_eq!(
            a.q("SELECT count(*) FROM d").single(sql),
            "2",
            "{sql}: nothing deleted"
        );
    }
    let r = a
        .q("DELETE FROM d USING k WHERE d.id = k.id RETURNING d.id, v")
        .ok("the target's columns");
    assert_eq!(
        r.rows,
        vec![vec![Some("2".to_string()), Some("b".to_string())]]
    );
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

/// DELETE ... USING deletes exactly the target rows that join when the target declares a column
/// named rowid (PostgreSQL reserves no such name): the rewrite names the table's own row by the
/// first of rowid, _rowid_ and oid the table does not declare. The declared column shadowed the
/// real rowid on both sides of the rewrite, so with duplicates and a NULL in it a row that joined
/// nothing was deleted and a joined row was kept, under the same tag DELETE 2 (wire review 13
/// item 5). A table that declares all three is refused 0A000, deleting nothing.
#[test]
fn delete_using_reaches_the_row_behind_a_rowid_column() {
    let dir = Scratch::new("deleterowid");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE u(k INT)").ok("u");
    a.q("INSERT INTO u VALUES (1), (3)").ok("u rows");
    for (t, columns) in [("r1", "rowid INT"), ("r2", "rowid INT, _rowid_ INT")] {
        a.q(&format!("CREATE TABLE {t}(id INT PRIMARY KEY, {columns})"))
            .ok("create");
        let values = if t == "r1" {
            "(1, 7), (2, 7), (3, NULL)"
        } else {
            "(1, 7, 7), (2, 7, 7), (3, NULL, NULL)"
        };
        a.q(&format!("INSERT INTO {t} VALUES {values}")).ok("rows");
        let r = a
            .q(&format!("DELETE FROM {t} USING u WHERE {t}.id = u.k"))
            .ok("delete");
        assert_eq!(r.tags, vec!["DELETE 2".to_string()], "{t}");
        assert_eq!(
            a.q(&format!("SELECT id FROM {t}"))
                .single("the row that joined nothing"),
            "2",
            "{t}"
        );
    }
    a.q("CREATE TABLE r3(id INT, rowid INT, _rowid_ INT, oid INT)")
        .ok("all three names declared");
    a.q("INSERT INTO r3 VALUES (1, 1, 1, 1), (2, 2, 2, 2)")
        .ok("r3 rows");
    let r = a.q("DELETE FROM r3 USING u WHERE r3.id = u.k");
    assert_eq!(r.err("no name left for the row").code, "0A000");
    assert_eq!(
        a.q("SELECT count(*) FROM r3").single("nothing deleted"),
        "2"
    );
}

/// DELETE ... USING ... RETURNING resolves a bare name as PostgreSQL does, over the target and the
/// USING items together: a name both have is 42702 and nothing is deleted; a name only a USING item
/// has, or a whole-row reference to one, returns what this server cannot (0A000), deleting
/// nothing; a subquery in RETURNING reads its own FROM, so its columns are not the USING items'.
/// Bare names were never checked: `RETURNING id` deleted and returned the target's id where
/// PostgreSQL 17.11 answers 42702, `RETURNING flag` was 42703, and a subquery naming a USING table
/// was refused (wire review 15 item 7).
#[test]
fn delete_using_returning_resolves_a_bare_name_over_both() {
    let dir = Scratch::new("deleteusingret");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .ok("d rows");
    a.q("CREATE TABLE k(id INT, flag BOOLEAN)").ok("k");
    a.q("INSERT INTO k VALUES (2, true), (3, false)")
        .ok("k rows");
    let count = |a: &mut Wire| a.q("SELECT count(*) FROM d").single("count");
    for (sql, code) in [
        (
            "DELETE FROM d USING k WHERE d.id = k.id RETURNING id",
            "42702",
        ),
        (
            "DELETE FROM d USING k WHERE d.id = k.id RETURNING flag",
            "0A000",
        ),
        (
            "DELETE FROM d USING k WHERE d.id = k.id RETURNING k",
            "0A000",
        ),
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, code, "{sql}");
        assert_eq!(count(&mut a), "3", "{sql}: nothing deleted");
    }
    let r = a
        .q("DELETE FROM d USING k WHERE d.id = k.id AND k.flag \
            RETURNING v, EXISTS (SELECT 1 FROM k AS j WHERE j.flag)")
        .ok("a subquery in RETURNING");
    assert_eq!(r.rows.len(), 1, "{:?}", r.rows);
    assert_eq!(r.rows[0][0].as_deref(), Some("b"));
    assert_eq!(count(&mut a), "2");
}

/// A subquery in DELETE ... USING ... RETURNING reads its own FROM: a qualified USING name, a bare
/// name only a USING table has, and `*` inside it are the subquery's, never refused as the
/// RETURNING's own references to the USING items (wire review 16 item 21 (d): the existing arm
/// passed at its base and under a mutant that walks into subqueries). Expected values: PostgreSQL's
/// by these fixtures' semantics, recorded against PostgreSQL 18 (LOUD-OWED).
#[test]
fn delete_using_returning_subqueries_read_their_own_from() {
    let dir = Scratch::new("deleteusingsub");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE k(id INT, flag BOOLEAN)").ok("k");
    a.q("INSERT INTO k VALUES (2, true), (3, false)")
        .ok("k rows");
    for returning in [
        "v, EXISTS (SELECT 1 FROM k WHERE k.flag)",
        "v, EXISTS (SELECT 1 FROM k AS j WHERE flag)",
        "v, EXISTS (SELECT * FROM k)",
    ] {
        a.q("DROP TABLE IF EXISTS d").ok("drop");
        a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
        a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .ok("d rows");
        let sql = format!("DELETE FROM d USING k WHERE d.id = k.id AND k.flag RETURNING {returning}");
        let r = a.q(&sql).ok(&sql);
        assert_eq!(r.rows.len(), 1, "{sql}: {:?}", r.rows);
        assert_eq!(r.rows[0][0].as_deref(), Some("b"), "{sql}");
        assert_eq!(
            a.q("SELECT count(*) FROM d").single("count"),
            "2",
            "{sql}"
        );
    }
}

/// DELETE ... USING names the target's row, and resolves RETURNING's bare names, by the columns of
/// the table the engine deletes from: under `search_path TO s, public` an unqualified r1 is s.r1,
/// which declares a rowid column where public.r1 does not, so the rewrite must name its row
/// `_rowid_`, deleting exactly the two joined rows. The column lookup read public's r1 whatever
/// the path, so it chose the bare rowid, which s.r1's column shadows on both sides (review 13
/// item 5's bug; wire review 16 item 21 (b)). Schema s is made by the non-server CLI.
#[test]
fn delete_using_reads_the_target_on_the_search_path() {
    let dir = Scratch::new("deleteusingpath");
    cli_script(
        &dir,
        b"CREATE TABLE u(k INT);\n\
          INSERT INTO u VALUES (1), (3);\n\
          CREATE TABLE r1(id INT PRIMARY KEY);\n\
          CREATE SCHEMA s;\n\
          CREATE TABLE s.r1(id INT PRIMARY KEY, rowid INT);\n\
          INSERT INTO s.r1 VALUES (1, 7), (2, 7), (3, NULL);\n",
    );
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("SET search_path TO s, public").ok("path");
    let r = a
        .q("DELETE FROM r1 USING u WHERE r1.id = u.k")
        .ok("delete");
    assert_eq!(r.tags, vec!["DELETE 2".to_string()]);
    assert_eq!(
        a.q("SELECT id FROM s.r1").single("the row that joined nothing"),
        "2"
    );
}

/// A USING subquery's output named by a cast (`SELECT id::int`) is named as PostgreSQL names it,
/// after the cast's argument, so `RETURNING id` over both is 42702 and nothing is deleted. Every
/// output that was not a column reference, a function call or aliased was named `?column?`, so
/// the bare id was the target's alone and the DELETE ran (wire review 16 item 21 (c)).
#[test]
fn delete_using_names_a_cast_output_as_postgresql_does() {
    let dir = Scratch::new("deleteusingcast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b')").ok("d rows");
    a.q("CREATE TABLE k(id INT)").ok("k");
    a.q("INSERT INTO k VALUES (2)").ok("k row");
    let sql = "DELETE FROM d USING (SELECT id::int FROM k) s WHERE d.id = s.id RETURNING id";
    assert_eq!(a.q(sql).err(sql).code, "42702", "{sql}");
    assert_eq!(
        a.q("SELECT count(*) FROM d").single("count"),
        "2",
        "nothing deleted"
    );
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

/// CREATE TABLE with a foreign key checks its parent as PostgreSQL does, in the column-constraint and
/// the table-constraint form: a parent that does not exist is 42P01; a parent with no primary key
/// to default to, or a parent key no UNIQUE or PRIMARY KEY covers, is 42830; each leaves no table
/// behind. They were accepted, after which every INSERT into the child failed 'foreign key
/// mismatch' and every later valid ALTER ADD FOREIGN KEY on it failed 42830 blaming its new parent
/// (wire review 13 item 2; foreign_key.out:876-878). A key onto a unique column, the parent's
/// primary key, or the table itself is accepted and enforced.
#[test]
fn create_table_checks_its_foreign_keys_parents() {
    let dir = Scratch::new("createfk");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT)").ok("p");
    a.q("INSERT INTO p VALUES (1, 5), (2, 5)").ok("p rows");
    a.q("CREATE TABLE np(id INT)").ok("np: no primary key");
    a.q("CREATE TABLE q(id INT PRIMARY KEY, code INT UNIQUE)")
        .ok("q");
    a.q("INSERT INTO q VALUES (1, 5)").ok("q row");
    for (sql, code) in [
        ("CREATE TABLE c(x INT REFERENCES p(code))", "42830"),
        (
            "CREATE TABLE c(x INT, FOREIGN KEY (x) REFERENCES p(code))",
            "42830",
        ),
        ("CREATE TABLE c(x INT REFERENCES np)", "42704"),
        (
            "CREATE TABLE c(x INT, FOREIGN KEY (x) REFERENCES np)",
            "42704",
        ),
        ("CREATE TABLE c(x INT REFERENCES nosuch(id))", "42P01"),
        (
            "CREATE TABLE c(x INT, FOREIGN KEY (x) REFERENCES nosuch(id))",
            "42P01",
        ),
        (
            "CREATE TABLE c(x INT, y INT, FOREIGN KEY (x, y) REFERENCES p(id))",
            "42830",
        ),
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, code, "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
        let r = a.q("SELECT * FROM c");
        assert_eq!(r.err(&format!("after {sql}: no table c")).code, "42P01");
    }
    a.q("CREATE TABLE c(x INT REFERENCES q(code), y INT REFERENCES p)")
        .ok("unique keys and a primary key");
    assert_eq!(
        a.q("INSERT INTO c VALUES (7, 1)").err("an orphan").code,
        "23503"
    );
    a.q("INSERT INTO c VALUES (5, 1)").ok("a matching child");
    a.q("CREATE TABLE tree(id INT PRIMARY KEY, parent INT REFERENCES tree(id))")
        .ok("a self-reference");
    a.q("INSERT INTO tree VALUES (1, NULL), (2, 1)")
        .ok("tree rows");
    // Inside a block the refusal fails the block, and its end leaves no table.
    a.q("BEGIN").ok("begin");
    let r = a.q("CREATE TABLE d(x INT REFERENCES p(code))");
    assert_eq!(r.err("in a block").code, "42830");
    assert_eq!(r.status, b'E');
    a.q("ROLLBACK").ok("end");
    assert_eq!(a.q("SELECT * FROM d").err("no table d").code, "42P01");
}

/// A multi-column foreign key declared MATCH FULL is refused (0A000), in CREATE TABLE and in ALTER
/// TABLE ADD FOREIGN KEY, the child unchanged: the engine enforces MATCH SIMPLE only, under which a
/// row with some key columns NULL is exempt where MATCH FULL refuses it. It was accepted, validated
/// and enforced as MATCH SIMPLE (wire review 11 item 12). One column is the same key under either.
#[test]
fn a_multi_column_match_full_key_is_refused() {
    let dir = Scratch::new("matchfull");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(a INT, b INT, PRIMARY KEY (a, b))")
        .ok("p");
    let sql = "CREATE TABLE c(x INT, y INT, FOREIGN KEY (x, y) REFERENCES p(a, b) MATCH FULL)";
    assert_eq!(a.q(sql).err(sql).code, "0A000");
    a.q("CREATE TABLE c(x INT, y INT)").ok("c");
    a.q("INSERT INTO c VALUES (1, NULL)").ok("a half-NULL row");
    let sql = "ALTER TABLE c ADD FOREIGN KEY (x, y) REFERENCES p(a, b) MATCH FULL";
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "0A000");
    assert_eq!(r.status, b'I');
    a.q("INSERT INTO c VALUES (2, NULL)")
        .ok("c is unchanged: no key");
    a.q("CREATE TABLE q(id INT PRIMARY KEY)").ok("q");
    a.q("CREATE TABLE d(x INT REFERENCES q(id) MATCH FULL)")
        .ok("one column: MATCH FULL is MATCH SIMPLE");
    a.q("ALTER TABLE c ADD FOREIGN KEY (x, y) REFERENCES p(a, b) MATCH SIMPLE")
        .ok("MATCH SIMPLE");
}

/// ALTER TABLE ADD FOREIGN KEY refuses a parent key PostgreSQL would: columns no UNIQUE or PRIMARY
/// KEY constraint covers (42830 "there is no unique constraint matching given keys"), a column
/// count that differs from the parent key's (42830), and a parent that does not exist (42P01); the
/// child is unchanged and still takes rows. With the copy-back's keys off, only orphans were
/// checked, so a non-unique parent key was accepted, and afterwards every INSERT into the child
/// failed 'foreign key mismatch' (23503) with no way back but DROP TABLE (wire review 11 item 5, a
/// regression from c9763b97b). A parent key a unique index covers is accepted and enforced.
#[test]
fn an_added_foreign_key_needs_a_unique_parent_key() {
    let dir = Scratch::new("fkunique");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT)").ok("p");
    a.q("INSERT INTO p VALUES (1, 5), (2, 5)").ok("p rows");
    a.q("CREATE TABLE q(id INT PRIMARY KEY, code INT UNIQUE)")
        .ok("q");
    a.q("INSERT INTO q VALUES (1, 5)").ok("q row");
    a.q("CREATE TABLE c(x INT)").ok("c");
    a.q("INSERT INTO c VALUES (5)").ok("c row");
    for (sql, code) in [
        (
            "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(code)",
            "42830",
        ),
        (
            "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(id, code)",
            "42830",
        ),
        (
            "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES nosuch(id)",
            "42P01",
        ),
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, code, "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
        a.q("INSERT INTO c VALUES (6)")
            .ok(&format!("after {sql}: c takes rows"));
        a.q("DELETE FROM c WHERE x = 6").ok("undo");
    }
    a.q("ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES q(code)")
        .ok("a unique parent key");
    let r = a.q("INSERT INTO c VALUES (7)");
    assert_eq!(r.err("an orphan after the key").code, "23503");
    a.q("INSERT INTO c VALUES (5)").ok("a matching child");
}

/// A refused ALTER TABLE ADD FOREIGN KEY leaves the child's rows exactly as they were, in
/// autocommit and inside a block the refusal fails (then ROLLBACK): `SELECT x FROM c` is ["5"]
/// after each. an_added_foreign_key_needs_a_unique_parent_key checked only that c still takes rows,
/// so a refusal that lost or doubled the existing ones passed (wire review 13 item 21).
#[test]
fn a_refused_foreign_key_leaves_the_childs_rows() {
    let dir = Scratch::new("fkrows");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT)").ok("p");
    a.q("INSERT INTO p VALUES (1, 5), (2, 5)").ok("p rows");
    a.q("CREATE TABLE c(x INT)").ok("c");
    a.q("INSERT INTO c VALUES (5)").ok("c row");
    let rows = |a: &mut Wire| -> Vec<Option<String>> {
        a.q("SELECT x FROM c").ok("c").rows.into_iter().map(|r| r[0].clone()).collect()
    };
    for sql in [
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(code)",
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(id, code)",
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES nosuch(id)",
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(id)",
    ] {
        assert!(a.q(sql).error.is_some(), "premise: {sql} is refused");
        assert_eq!(rows(&mut a), vec![Some("5".to_string())], "after {sql}");
        a.q("BEGIN").ok("begin");
        assert_eq!(a.q(sql).status, b'E', "{sql} fails the block");
        a.q("ROLLBACK").ok("end");
        assert_eq!(rows(&mut a), vec![Some("5".to_string())], "after {sql} in a block");
    }
}

/// ALTER TABLE ADD FOREIGN KEY checks each key it adds on its own, in PostgreSQL's order
/// (ATAddForeignKeyConstraint): a second added key onto a non-unique parent column is 42830 naming
/// that key's parent; a parent column or a key column that does not exist is 42703 "column ...
/// referenced in foreign key constraint does not exist" (the parent's was 42830); and a child
/// whose older key no longer resolves still takes a valid new key. The engine's resolution of
/// every key of the table was the check, so its error always blamed the first added key's parent,
/// and an older unresolvable key refused every new one (wire review 13 item 3). The older key is
/// made unresolvable by dropping the unique index it needs, which PostgreSQL refuses (2BP01) and
/// this server does not (E5-QUEUE); if that DROP is ever refused, this arm's premise fails, loudly.
#[test]
fn an_added_foreign_key_is_checked_on_its_own() {
    let dir = Scratch::new("fkown");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT)").ok("p");
    a.q("CREATE TABLE q(id INT PRIMARY KEY, code INT UNIQUE)")
        .ok("q");
    a.q("INSERT INTO q VALUES (1, 5)").ok("q row");
    a.q("CREATE TABLE c(x INT, y INT)").ok("c");
    a.q("INSERT INTO c VALUES (5, NULL)").ok("c row");
    let sql =
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES q(code), ADD FOREIGN KEY (y) REFERENCES p(code)";
    let r = a.q(sql);
    assert_eq!(r.status, b'I', "{sql}");
    let e = r.err(sql);
    assert_eq!(e.code, "42830", "{sql}");
    assert!(
        e.message.contains("referenced table \"p\""),
        "the second key's parent is named: {}",
        e.message
    );
    for sql in [
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES q(nosuch)",
        "ALTER TABLE c ADD FOREIGN KEY (nosuch) REFERENCES q(code)",
    ] {
        let r = a.q(sql);
        assert_eq!(r.status, b'I', "{sql}");
        let e = r.err(sql);
        assert_eq!(e.code, "42703", "{sql}");
        assert!(
            e.message
                .contains("\"nosuch\" referenced in foreign key constraint"),
            "{sql}: {}",
            e.message
        );
    }
    // No key was added: c takes an orphan.
    a.q("INSERT INTO c VALUES (6, 6)")
        .ok("c is unchanged after the refusals");
    a.q("CREATE TABLE r(id INT PRIMARY KEY, code INT)").ok("r");
    a.q("CREATE UNIQUE INDEX r_code ON r(code)")
        .ok("r's unique key");
    a.q("CREATE TABLE d(x INT REFERENCES r(code), y INT)")
        .ok("d, keyed on r(code)");
    a.q("DROP INDEX r_code")
        .ok("premise: the index d's key needs can be dropped here");
    a.q("ALTER TABLE d ADD FOREIGN KEY (y) REFERENCES q(code)")
        .ok("a valid key beside an older one that no longer resolves");
}

/// ALTER TABLE ADD FOREIGN KEY refuses a key the catalog alone condemns before it rebuilds the
/// table: a parent key of another column count is 42830, and `REFERENCES nosuch(a, b)` is 42P01
/// (both were 42601, the engine refusing the rebuilt CREATE TABLE after the table was copied
/// aside and dropped). While another session holds the trunk's write lock the refusal is answered
/// as it is, not 55P03 after the lock wait: nothing is written for it, so a refused ALTER costs
/// O(catalog) instead of two copies of the table and an index rebuild (wire review 13 item 4).
#[test]
fn a_foreign_key_the_catalog_refuses_is_refused_before_the_rebuild() {
    let dir = Scratch::new("fkearly");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT UNIQUE)")
        .ok("p");
    a.q("CREATE TABLE c(x INT)").ok("c");
    a.q("INSERT INTO c VALUES (5)").ok("c row");
    let mut w = server.connect();
    w.q("BEGIN").ok("begin");
    w.q("INSERT INTO p VALUES (9, 9)")
        .ok("another session takes the write lock");
    for (sql, code) in [
        (
            "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(id, code)",
            "42830",
        ),
        (
            "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES nosuch(a, b)",
            "42P01",
        ),
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, code, "{sql}");
        assert_eq!(r.status, b'I', "{sql}");
    }
    w.q("ROLLBACK").ok("end");
    a.q("INSERT INTO c VALUES (6)").ok("c is unchanged");
}

/// create_with_keys's refusal and undo paths, which no test reached (wire review 16 item 13; the
/// mutants that must turn this red are the server's check_table_keys call deleted and
/// parent_key_resolves always true): a self-referencing key onto a column with no unique index
/// (`up INT REFERENCES t(code)`) is 42830 and leaves no table t, in autocommit and in a block
/// (which fails, then ROLLBACK, and still no t); a self-reference with no primary key to default
/// to is 42830 too; a keyed CREATE that resolves, in a block, is kept by COMMIT (the RELEASE arm);
/// and an ALTER adding a key onto a non-unique single column, while another session holds the
/// trunk's write lock, is 42830 at once, not 55P03 after the wait (server lock wait 300 ms).
#[test]
fn create_with_keys_refuses_and_undoes() {
    let dir = Scratch::new("keysundo");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "300"]);
    let mut a = server.connect();
    let exists = |a: &mut Wire, table: &str| {
        a.q(&format!("SELECT count(*) FROM {table}"))
            .error
            .is_none()
    };
    let bad = "CREATE TABLE t(id INT PRIMARY KEY, code INT, up INT REFERENCES t(code))";
    assert_eq!(a.q(bad).err(bad).code, "42830", "autocommit");
    assert!(!exists(&mut a, "t"), "a refused t was kept");
    a.q("BEGIN").ok("begin");
    let r = a.q(bad);
    assert_eq!(r.err(bad).code, "42830", "in a block");
    assert_eq!(r.status, b'E', "the block fails");
    a.q("ROLLBACK").ok("rollback");
    assert!(!exists(&mut a, "t"), "a refused t was kept by the block");
    let nopk = "CREATE TABLE n(id INT, up INT REFERENCES n)";
    assert_eq!(
        a.q(nopk).err(nopk).code,
        "42704",
        "a self-reference with no primary key"
    );
    assert!(!exists(&mut a, "n"));
    a.q("BEGIN").ok("begin");
    a.q("CREATE TABLE g(id INT PRIMARY KEY, up INT REFERENCES g(id))")
        .ok("a keyed CREATE that resolves");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert!(exists(&mut a, "g"), "COMMIT kept g");
    let orphan = "INSERT INTO g VALUES (1, 9)";
    assert_eq!(a.q(orphan).err(orphan).code, "23503", "g's key holds");
    a.q("CREATE TABLE p(id INT PRIMARY KEY, code INT)").ok("p");
    a.q("CREATE TABLE c(x INT)").ok("c");
    let mut w = server.connect();
    w.q("BEGIN").ok("begin");
    w.q("INSERT INTO p VALUES (9, 9)")
        .ok("another session takes the write lock");
    let alter = "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(code)";
    let r = a.q(alter);
    assert_eq!(r.err(alter).code, "42830", "not 55P03 after the lock wait");
    assert_eq!(r.status, b'I');
    w.q("ROLLBACK").ok("end");
}

/// A key the translator refuses (MATCH FULL over two columns, 0A000) is refused before an ALTER
/// TABLE ADD FOREIGN KEY rebuilds the table, so while another session holds the trunk's write lock
/// it is answered as it is, status I, not 55P03 after the lock wait, and the table is unchanged.
/// The refusal came from translating the rebuild's own CREATE TABLE, after the BEGIN, the aside copy
/// and the DROP, under the write lock (wire review 16 item 9). The server's lock wait is 300 ms so
/// the base answers 55P03 at once rather than after a minute.
#[test]
fn a_key_the_translator_refuses_is_refused_before_the_rebuild() {
    let dir = Scratch::new("fkmatchfull");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "300"]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(a INT, b INT, PRIMARY KEY (a, b))")
        .ok("p");
    a.q("CREATE TABLE c(x INT, y INT)").ok("c");
    a.q("INSERT INTO c VALUES (5, 6)").ok("c row");
    let mut w = server.connect();
    w.q("BEGIN").ok("begin");
    w.q("INSERT INTO p VALUES (9, 9)")
        .ok("another session takes the write lock");
    let sql = "ALTER TABLE c ADD FOREIGN KEY (x, y) REFERENCES p(a, b) MATCH FULL";
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "0A000");
    assert_eq!(r.status, b'I');
    w.q("ROLLBACK").ok("end");
    a.q("INSERT INTO c VALUES (7, 8)")
        .ok("c is unchanged: no key was added");
    assert_eq!(a.q("SELECT count(*) FROM c").single("rows"), "2");
}

/// A foreign key onto a rowid name the parent does not declare (`REFERENCES p(rowid)`, `_rowid_`,
/// `oid`) is 42703, as PostgreSQL answers, by the precheck: before a CREATE TABLE runs and before an
/// ALTER's rebuild, so while another session holds the trunk's write lock the ALTER is answered at
/// once, status I, not 55P03 after the lock wait. A key onto a declared column named rowid is
/// refused 0A000: the engine never resolves a key onto a rowid name (ForeignKey::validate). The
/// precheck took any rowid name as the parent's rowid, so the CREATE was refused after it ran and
/// the ALTER after its lock-held copies (wire review 16 item 16). The server's lock wait is 300 ms.
#[test]
fn a_key_onto_a_rowid_name_is_refused_before_anything_runs() {
    let dir = Scratch::new("fkrowid");
    let server = Server::start(&dir.db(), &["--lock-timeout-ms", "300"]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT PRIMARY KEY)").ok("p");
    a.q("CREATE TABLE c(x INT)").ok("c");
    a.q("INSERT INTO c VALUES (5)").ok("c row");
    for name in ["rowid", "_rowid_", "oid"] {
        let sql = format!("CREATE TABLE k(x INT REFERENCES p({name}))");
        assert_eq!(a.q(&sql).err(&sql).code, "42703", "{sql}");
    }
    let mut w = server.connect();
    w.q("BEGIN").ok("begin");
    w.q("INSERT INTO p VALUES (9)")
        .ok("another session takes the write lock");
    let alter = "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p(rowid)";
    let r = a.q(alter);
    assert_eq!(r.err(alter).code, "42703", "not 55P03 after the lock wait");
    assert_eq!(r.status, b'I');
    w.q("ROLLBACK").ok("end");
    a.q("INSERT INTO c VALUES (7)")
        .ok("c is unchanged: no key was added");
    a.q("CREATE TABLE q(rowid INT UNIQUE)")
        .ok("q declares a column named rowid");
    let declared = "CREATE TABLE k(x INT REFERENCES q(rowid))";
    assert_eq!(a.q(declared).err(declared).code, "0A000", "{declared}");
    a.q("CREATE TABLE k(x INT REFERENCES p(id))")
        .ok("control: no k was left behind, and a key onto p(id) is accepted");
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

/// A simple Query is parsed whole before any of it runs, as PostgreSQL parses it: a part that is
/// no statement (`COMMIT<NBSP>`, which lexes as one identifier; `foo`; `ROLBACK`) is 42601, and
/// nothing of the query runs, in autocommit or in a block (which fails). libpg_query's scanner
/// split emitted only the parts holding a keyword and dropped the rest, so the other statements ran
/// and nothing was refused (wire review 16 item 1). Failure here is a duplicate key, never `1/0`,
/// which this engine answers NULL (wire review 16 item 2).
#[test]
fn a_query_with_a_part_that_is_no_statement_runs_nothing() {
    let dir = Scratch::new("nostmt");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE sp(id INT PRIMARY KEY)").ok("sp");
    for sql in [
        "INSERT INTO sp VALUES (2); COMMIT\u{a0}; INSERT INTO sp VALUES (3)",
        "foo; INSERT INTO sp VALUES (4)",
        "INSERT INTO sp VALUES (5); ROLBACK",
        "INSERT INTO sp VALUES (6); 'x'",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "42601", "{sql:?}");
        assert_eq!(r.status, b'I', "{sql:?}");
    }
    assert_eq!(
        a.q("SELECT count(*) FROM sp").single("rows"),
        "0",
        "a statement of a refused query ran"
    );
    // Control: the same statements with no stray part run, and a comment or a run of `;` between
    // them is no part.
    a.q("INSERT INTO sp VALUES (2); /* c */ ;; INSERT INTO sp VALUES (3) -- end")
        .ok("two inserts");
    assert_eq!(a.q("SELECT count(*) FROM sp").single("rows"), "2");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO sp VALUES (7)").ok("insert in the block");
    let sql = "UPDATE sp SET id = id + 100; ROLBACK";
    let r = a.q(sql);
    assert_eq!(r.err(sql).code, "42601");
    assert_eq!(r.status, b'E', "the block fails");
    a.q("ROLLBACK").ok("rollback");
    assert_eq!(
        a.q("SELECT sum(id) FROM sp").single("ids"),
        "5",
        "the block's insert or the UPDATE survived"
    );
}

/// A simple Query is parsed whole before any of it runs even when its malformed part holds a
/// keyword (`SELEC * FROM t`, FROM being reserved; `ROLBACK WORK`, WORK unreserved): 42601, and
/// nothing of it runs, neither the INSERT and COMMIT before it nor a CHECKPOINT (the store's
/// checkpoint count does not move). The scanner split emitted every stretch holding a keyword as a
/// part, so the parts before ran and committed, and only then was the malformed one refused (wire
/// review 18 item 6; 16a9ff5bc refused only stretches holding no keyword).
#[test]
fn a_query_with_a_malformed_part_runs_nothing() {
    let dir = Scratch::new("malformedpart");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("SELECT turso_branch_create('b0')")
        .ok("a branch, so the store has a log");
    let store_checkpoints = |a: &mut Wire| {
        a.q("SELECT turso_branch_stats()").ok("stats").rows[0][5]
            .clone()
            .expect("the store checkpoint count")
    };
    let before = store_checkpoints(&mut a);
    for sql in [
        "INSERT INTO t VALUES (2, 'two'); COMMIT; SELEC * FROM t",
        "INSERT INTO t VALUES (3, 'three'); COMMIT; ROLBACK WORK",
        "CHECKPOINT; SELEC * FROM t",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "42601", "{sql:?}");
        assert_eq!(r.status, b'I', "{sql:?}");
    }
    assert_eq!(
        a.q("SELECT count(*) FROM t").single("rows"),
        "1",
        "a part before the malformed one ran"
    );
    assert_eq!(
        store_checkpoints(&mut a),
        before,
        "the CHECKPOINT before the malformed part ran"
    );
}

/// An implicit block (a multi-statement query, or a pipeline up to Sync) lives as long as the
/// engine's transaction does, whatever the statements' text says: a statement that ends the
/// engine's transaction ends the block, and a failure after it cannot be committed by the next
/// statement. Its state was read from the statement text: `COMMIT<NBSP>` read as an ordinary
/// statement, the frontend trimmed the NBSP and the engine committed, and every later statement of
/// the query committed on its own, so a failure at the end kept rows 2 and 3 (PostgreSQL lexes
/// the NBSP into the keyword and answers 42601, keeping neither). A simple Query inside an unsynced
/// pipeline that fails leaves the pipeline's rows rolled back at Sync (wire review 13 item 1).
#[test]
fn an_implicit_block_follows_the_engines_transaction() {
    let dir = Scratch::new("implicitstate");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let count = |a: &mut Wire| {
        a.q("SELECT count(*) FROM t WHERE id > 1")
            .single("rows kept")
    };
    let sql = "INSERT INTO t VALUES (2, 'two'); COMMIT\u{a0}; INSERT INTO t VALUES (3, 'three'); INSERT INTO t VALUES (1, 'dup')";
    let r = a.q(sql);
    assert_eq!(r.err("COMMIT with an NBSP").code, "42601");
    assert_eq!(r.status, b'I');
    assert_eq!(count(&mut a), "0", "simple: nothing kept");
    let r = a.pipeline(&[
        "INSERT INTO t VALUES (2, 'two')",
        "COMMIT\u{a0}",
        "INSERT INTO t VALUES (3, 'three')",
        "INSERT INTO t VALUES (1, 'dup')",
    ]);
    assert_eq!(r.err("pipeline COMMIT with an NBSP").code, "42601");
    assert_eq!(count(&mut a), "0", "pipeline: nothing kept");
    let r =
        a.q("INSERT INTO t VALUES (4, 'four'); COMMIT AND CHAIN; SELECT turso_branch_create('x')");
    assert!(r.error.is_some(), "AND CHAIN in an implicit block");
    assert_eq!(count(&mut a), "0", "chain: nothing kept");
    let r = a.q("SELECT turso_branch_switch('x')");
    assert_eq!(r.err("no branch x").code, "3D000");
    // An unsynced pipeline INSERT, then a simple Query that fails, then Sync: rolled back.
    for query in ["SELECT * FROM nosuch", "SELECT turso_branch_create('y')"] {
        let mut parse = vec![0u8];
        parse.extend_from_slice(b"INSERT INTO t VALUES (5, 'five')");
        parse.extend_from_slice(&[0, 0, 0]);
        a.send(b'P', &parse);
        a.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        a.send(b'E', &[0, 0, 0, 0, 0]);
        let r = a.q(query);
        assert!(
            r.error.is_some(),
            "{query} in an unsynced pipeline: {:?}",
            r.tags
        );
        a.send(b'S', &[]);
        let r = a.read_reply();
        assert_eq!(r.status, b'I', "{query}: idle after Sync");
        assert_eq!(
            count(&mut a),
            "0",
            "{query}: the pipeline's insert rolled back"
        );
    }
    let r = a.q("SELECT turso_branch_switch('y')");
    assert_eq!(r.err("no branch y").code, "3D000");
}

/// A simple Query inside an unsynced pipeline is part of the pipeline's implicit block, as in
/// PostgreSQL, whatever form the Query takes: a lone BEGIN makes the block the client's (status T),
/// so its ROLLBACK drops the pipeline's write and its own; a branch call refused there (25001)
/// rolls the block back and leaves the session idle, so the next Query runs before any Sync; and
/// turso_branch_current() or an empty Query ends the block like any other Query, committing it
/// (status I). A lone BEGIN committed the pipeline's write and answered I; the refused call left
/// the session failed (E, then 25P02); and the fast path and the empty Query left the block open
/// (T) (wire review 16 item 6).
#[test]
fn a_query_in_an_unsynced_pipeline_takes_part_in_its_block() {
    let dir = Scratch::new("pipequery");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    // Parse, Bind and Execute an INSERT of `id`, with no Sync: the pipeline's implicit block.
    fn insert(w: &mut Wire, id: i32) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(format!("INSERT INTO t VALUES ({id}, 'p')").as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        w.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        w.send(b'E', &[0, 0, 0, 0, 0]);
    }
    let kept = |b: &mut Wire, id: i32| {
        b.q(&format!("SELECT count(*) FROM t WHERE id = {id}"))
            .single("kept")
    };
    insert(&mut a, 2);
    let r = a.q("BEGIN");
    assert!(r.error.is_none(), "BEGIN: {:?}", r.error);
    assert_eq!(r.status, b'T', "BEGIN makes the block the client's");
    a.q("UPDATE t SET v = 'u' WHERE id = 1")
        .ok("update in the block");
    a.q("ROLLBACK").ok("the client's rollback");
    a.send(b'S', &[]);
    a.read_reply();
    assert_eq!(kept(&mut b, 2), "0", "the pipeline's insert was kept");
    assert_eq!(
        b.q("SELECT v FROM t WHERE id = 1").single("v"),
        "trunk",
        "the UPDATE was kept"
    );
    insert(&mut a, 3);
    let r = a.q("SELECT turso_branch_create('y')");
    assert!(r.error.is_some(), "a branch call inside the block");
    assert_eq!(r.status, b'I', "the refused call rolled the block back");
    assert_eq!(a.q("SELECT 1").single("a Query before Sync"), "1");
    a.send(b'S', &[]);
    a.read_reply();
    assert_eq!(kept(&mut b, 3), "0", "the rolled-back insert");
    insert(&mut a, 4);
    let r = a.q("SELECT turso_branch_current()");
    assert!(r.error.is_none(), "turso_branch_current(): {:?}", r.error);
    assert_eq!(r.status, b'I', "turso_branch_current() ends the block");
    assert_eq!(kept(&mut b, 4), "1", "committed by the Query");
    insert(&mut a, 5);
    let r = a.q("");
    assert_eq!(r.status, b'I', "an empty Query ends the block");
    assert_eq!(kept(&mut b, 5), "1", "committed by the empty Query");
    a.send(b'S', &[]);
    assert_eq!(a.read_reply().status, b'I');
}

/// A transaction verb followed by a run of `;` is its verb over the extended protocol too (wire
/// review 16 item 7; pins of 216149918 (i) and 3067f87d8, which review 16 found unreddened): in an
/// unsynced pipeline `COMMIT;;` ends the implicit block, committing its INSERT, and the flag goes
/// with it, so a Query BEGIN then opens the client's block (T), never one committed at once (the
/// stale-flag mutant answers I); and `ROLLBACK;;` ends a failed block with ROLLBACK, status I,
/// where the verb reader read it as an ordinary statement and refused it 25P02.
#[test]
fn a_verb_before_empty_statements_is_its_verb_in_a_pipeline() {
    let dir = Scratch::new("pipesemis");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    // Parse, Bind and Execute `sql` (no parameters, no Sync).
    fn execute(w: &mut Wire, sql: &str) {
        let mut parse = vec![0u8];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        w.send(b'P', &parse);
        w.send(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]);
        w.send(b'E', &[0, 0, 0, 0, 0]);
    }
    execute(&mut a, "INSERT INTO t VALUES (2, 'p')");
    execute(&mut a, "COMMIT;;");
    let r = a.q("BEGIN");
    assert!(r.error.is_none(), "{:?}", r.error);
    assert_eq!(
        r.status, b'T',
        "BEGIN after COMMIT;; opens the client's block"
    );
    a.q("ROLLBACK").ok("end");
    a.send(b'S', &[]);
    a.read_reply();
    assert_eq!(
        b.q("SELECT count(*) FROM t WHERE id = 2").single("kept"),
        "1",
        "COMMIT;; committed the pipeline's insert"
    );
    a.q("BEGIN").ok("begin");
    assert_eq!(a.q("SELECT * FROM nosuch").status, b'E');
    execute(&mut a, "ROLLBACK;;");
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert!(
        r.error.is_none(),
        "ROLLBACK;; in a failed block: {:?}",
        r.error
    );
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
}

/// An empty statement takes no value at Bind: one value is 08P01 before any BindComplete, as
/// PostgreSQL counts it (its parameters are the ones Parse declared, none). It got BindComplete,
/// then EmptyQueryResponse (wire review 16 item 5; a pin of 745585f27's Bind-time count).
#[test]
fn an_empty_statement_takes_no_value_at_bind() {
    let dir = Scratch::new("emptybind");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let (types, error) = a.bind_round("", &[], &[Some(b"v")], &[]);
    assert_eq!(error.map(|e| e.code), Some("08P01".to_string()));
    assert!(
        !types.contains(&b'2'),
        "BindComplete came before the refusal: {:?}",
        String::from_utf8_lossy(&types)
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

/// SAVEPOINT, RELEASE and ROLLBACK TO outside a block are 25P01 whatever the name's spelling, read
/// by their leading keywords: a name the verb reader cannot hold as one word (`U&"a"`) made the
/// statement Other, so the engine ran SAVEPOINT and opened a transaction with no BEGIN (status T;
/// after a write it held the trunk's write lock, and other sessions' writes and creates met
/// 55P03), and RELEASE or ROLLBACK TO reached the engine's XX000 (wire review 14 item 15).
/// PostgreSQL 18.6 answers 25P01 (recorded, artie raw loud-20261015/pg18-diff/pg18-r14.out).
/// `SAVEPOINT a$b`, a name the reader
/// holds, is the control (wire review 14 item 22's wire half).
#[test]
fn a_savepoint_verb_outside_a_block_is_25p01_whatever_its_name() {
    let dir = Scratch::new("savepointnames");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in [
        "SAVEPOINT U&\"a\"",
        "RELEASE U&\"a\"",
        "ROLLBACK TO U&\"a\"",
        "ROLLBACK TRANSACTION TO SAVEPOINT U&\"a\"",
        "SAVEPOINT a$b",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "25P01", "{sql}");
        assert_eq!(r.status, b'I', "{sql} opened a block");
        a.q("INSERT INTO t VALUES (5, 'x')").ok("a write after it");
        a.q("DELETE FROM t WHERE id = 5").ok("undo");
        let mut b = server.connect();
        b.q("INSERT INTO t VALUES (9, 'z')")
            .ok("another session writes at once");
        b.q("DELETE FROM t WHERE id = 9").ok("undo");
    }
}

/// A savepoint may be named savepoint, as in PostgreSQL 18.6 (recorded, pg18-r14.out): in a failed
/// block, `ROLLBACK TO
/// SAVEPOINT` rolls back to it and the block carries on to its COMMIT. The verb reader stripped the
/// keyword, found no name and read Other, so the failed block refused it 25P02 (wire review 14
/// item 21).
#[test]
fn a_savepoint_named_savepoint_is_reached() {
    let dir = Scratch::new("savepointsavepoint");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert 2");
    a.q("SAVEPOINT savepoint").ok("savepoint savepoint");
    let r = a.q("SELECT * FROM nosuch");
    assert!(r.error.is_some(), "premise: the statement fails");
    let r = a.q("ROLLBACK TO SAVEPOINT").ok("rollback to savepoint savepoint");
    assert_eq!(r.status, b'T', "the block is still failed");
    assert_eq!(a.q("SELECT 2").single("in the block"), "2");
    let r = a.q("COMMIT").ok("commit");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2").single("kept"),
        "1"
    );
}

/// COMMIT or ROLLBACK inside an implicit block warns 25P01 "there is no transaction in progress",
/// as PostgreSQL warns for `SELECT 1; COMMIT` (18.6, recorded, pg18-r14.out) and for a pipeline's
/// COMMIT before its Sync (17.11, the review's), and still ends the block. They reached the engine
/// with no notice, though the commit that made them end the block claimed PostgreSQL's behaviour
/// (wire review 14 item 20).
#[test]
fn a_block_end_in_an_implicit_block_warns_25p01() {
    let dir = Scratch::new("implicitend");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for verb in ["COMMIT", "ROLLBACK"] {
        let sql = format!("SELECT 1; {verb}");
        let r = a.q(&sql).ok(&sql);
        assert!(
            r.notices.iter().any(|n| n.code == "25P01"),
            "{sql}: notices {:?}",
            r.notices
        );
        assert_eq!(r.status, b'I', "{sql}");
        let r = a.pipeline(&["SELECT 1", verb]).ok(verb);
        assert!(
            r.notices.iter().any(|n| n.code == "25P01"),
            "pipeline {verb}: notices {:?}",
            r.notices
        );
        assert_eq!(r.status, b'I', "pipeline {verb}");
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
        a.q("SELECT * FROM nosuch").err("fail the block");
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
    a.q("SELECT * FROM nosuch").err("fail the block");
    let r = a.q("/* c */ ROLLBACK").ok("commented ROLLBACK");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
}

/// COMMIT AND CHAIN and ROLLBACK AND CHAIN outside a block are 25P01 ("... can only be used in
/// transaction blocks"), as PostgreSQL refuses them; inside one the chain is not supported (0A000),
/// which fails the block, and its writes are not committed. BEGIN READ ONLY is not supported either
/// (0A000): the engine has no read-only transaction. COMMIT AND CHAIN committed and left the session
/// idle, so a later ROLLBACK undid nothing and writes the client meant to discard stayed committed,
/// and BEGIN READ ONLY began a block that wrote (wire review 11 item 7).
#[test]
fn a_chained_or_read_only_transaction_is_refused() {
    let dir = Scratch::new("txchain");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in ["COMMIT AND CHAIN", "ROLLBACK AND CHAIN", "END AND CHAIN"] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "25P01", "{sql} outside a block");
        assert_eq!(r.status, b'I', "{sql}");
    }
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("insert");
    let r = a.q("COMMIT AND CHAIN");
    assert_eq!(r.err("COMMIT AND CHAIN in a block").code, "0A000");
    assert_eq!(r.status, b'E', "the block failed");
    let r = a.q("ROLLBACK").ok("end");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2")
            .single("not committed"),
        "0"
    );
    for sql in [
        "BEGIN READ ONLY",
        "START TRANSACTION READ ONLY",
        "BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY",
    ] {
        let r = a.q(sql);
        assert_eq!(r.err(sql).code, "0A000", "{sql}");
        assert_eq!(r.status, b'I', "{sql}: no block begun");
    }
    a.q("BEGIN READ WRITE").ok("read write");
    a.q("COMMIT").ok("end");
}

/// BEGIN READ ONLY inside an open block is refused (0A000) as it is outside one, failing the block,
/// so nothing commits: in a client's block, and in a simple query's implicit block. The in-block
/// BEGIN arm answered 25001's warning before the translator could refuse the mode, and in an
/// implicit block it handed the block to the client, whose COMMIT committed both writes; PostgreSQL
/// 17.11 refuses the write (25006) and keeps nothing (wire review 15 item 3).
#[test]
fn a_read_only_begin_inside_a_block_is_refused() {
    let dir = Scratch::new("readonlyinblock");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    let r = a.q("BEGIN READ ONLY");
    assert_eq!(r.err("BEGIN READ ONLY in a block").code, "0A000");
    assert_eq!(r.status, b'E', "the block failed");
    assert_eq!(a.q("INSERT INTO t VALUES (2, 'two')").status, b'E');
    a.q("COMMIT").ok("ends the failed block");
    let r = a.q("INSERT INTO t VALUES (3, 'three'); BEGIN READ ONLY; INSERT INTO t VALUES (4, 'four'); COMMIT");
    assert_eq!(r.err("BEGIN READ ONLY in an implicit block").code, "0A000");
    assert_eq!(r.status, b'I');
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id > 1")
            .single("nothing kept"),
        "0"
    );
}

/// SET accepts the parameters it can honour and refuses the rest: a read-only transaction asked
/// for through `transaction_read_only` or `default_transaction_read_only` is 0A000 (the engine has
/// no read-only transaction), so a block that asked for one keeps nothing; `off` changes nothing.
/// A planner or client setting PostgreSQL has and this server need not act on (enable_seqscan,
/// application_name) is accepted; one whose value would change how statements are read or
/// answered (standard_conforming_strings off, a client_encoding other than UTF8) is 0A000; a name
/// PostgreSQL does not know is 42704, and so is an engine pragma, which SET reached as PRAGMA
/// `synchronous` and could change the engine's durability. Every SET became `PRAGMA name = value`,
/// which the engine ignores for a name it does not know, so `transaction_read_only = on` answered
/// SET and the block's writes committed; PostgreSQL 17.11 refuses them (25006) (wire review 15
/// item 2).
#[test]
fn set_honours_or_refuses_each_parameter() {
    let dir = Scratch::new("setallow");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    let r = a.q("SET transaction_read_only = on");
    assert_eq!(r.err("SET transaction_read_only = on").code, "0A000");
    assert_eq!(a.q("INSERT INTO t VALUES (2, 'two')").status, b'E');
    a.q("ROLLBACK").ok("end");
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2")
            .single("nothing kept"),
        "0"
    );
    let mut wrong = Vec::new();
    for (sql, code) in [
        ("SET default_transaction_read_only = on", Some("0A000")),
        ("SET LOCAL transaction_read_only TO true", Some("0A000")),
        ("SET default_transaction_read_only = off", None),
        ("SET enable_seqscan = off", None),
        ("SET application_name = 'bench'", None),
        ("SET client_encoding = 'UTF8'", None),
        ("SET client_encoding = 'LATIN1'", Some("0A000")),
        ("SET standard_conforming_strings = off", Some("0A000")),
        ("SET synchronous = off", Some("42704")),
        ("SET no_such_parameter = 1", Some("42704")),
    ] {
        let r = a.q(sql);
        let got = r.error.as_ref().map(|e| e.code.as_str());
        if got != code {
            wrong.push(format!("{sql}: {got:?}, want {code:?}"));
        }
    }
    a.q("INSERT INTO t VALUES (3, 'three')")
        .ok("writes still work");
    assert!(
        wrong.is_empty(),
        "{} wrong:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
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
        a.q("SELECT * FROM nosuch").err("fail the block");
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

/// Coverage for 84c32da85 and 3067f87d8, whose reds passed at their own base (wire review 17 item
/// 7; these are pins at the tip, red at 84c32da85 by reading): `\vCOMMIT` by Parse ends a failed
/// block with ROLLBACK (a vertical tab is whitespace); `;COMMIT` commits an open block, tagged
/// COMMIT; `CHECKPOINT -- $1` with one value is 08P01 before any BindComplete (its `$1` is in a
/// comment, and CHECKPOINT takes the parameters Parse declared, none); and `CHECKPOINT -- x` in a
/// block while another session pins the WAL is the server's CHECKPOINT, skipped with a NOTICE
/// and counted, the block going on, where the engine's text checkpoint failed the block.
#[test]
fn a_commented_verb_is_the_servers_verb() {
    let dir = Scratch::new("commentverb");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    let mut b = server.connect();
    a.q("BEGIN").ok("begin");
    assert_eq!(
        a.q("SELECT * FROM nosuch").status,
        b'E',
        "premise: the block failed"
    );
    let r = a.x("\u{b}COMMIT", &[]).ok("\\vCOMMIT");
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'I');
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (3, 'three')").ok("a write");
    let r = a.x(";COMMIT", &[]).ok(";COMMIT");
    assert_eq!(r.tags, vec!["COMMIT".to_string()]);
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 3").single("kept"),
        "1"
    );
    let (types, error) = a.bind_round("CHECKPOINT -- $1", &[], &[Some(b"v")], &[]);
    assert_eq!(error.map(|e| e.code), Some("08P01".to_string()));
    assert!(
        !types.contains(&b'2'),
        "BindComplete came before the refusal: {:?}",
        String::from_utf8_lossy(&types)
    );
    let before = checkpoints_skipped(&mut a);
    a.q("CHECKPOINT").ok("checkpoint outside a block");
    assert_eq!(wal_bytes(&dir.db()), 0, "premise: the WAL is empty");
    b.q("BEGIN").ok("b begin");
    b.q("SELECT count(*) FROM t").ok("b reads");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (4, 'four')")
        .ok("insert in block");
    let r = a
        .q("CHECKPOINT -- x")
        .ok("a commented CHECKPOINT in a block");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    assert_eq!(r.notices.len(), 1, "notices {:?}", r.notices);
    assert_eq!(r.status, b'T', "the block goes on");
    a.q("COMMIT").ok("commit");
    b.q("COMMIT").ok("b commit");
    assert_eq!(checkpoints_skipped(&mut a), before + 1);
}

/// A transaction verb or CHECKPOINT is read as PostgreSQL's lexer reads it: runs of `;` before
/// and after it are empty statements, a vertical tab is whitespace, and a comment after
/// CHECKPOINT is whitespace. `;COMMIT` and `COMMIT;;` by Parse end a failed block with ROLLBACK
/// and commit an open one; `\vBEGIN` begins one; `CHECKPOINT -- x` is the server's CHECKPOINT, and
/// with a value bound it is 08P01. Read as ordinary statements, the failed block refused them
/// (25P02) and the engine ran `CHECKPOINT -- x` as text (wire review 14 item 6).
#[test]
fn a_verb_beside_empty_statements_is_its_verb() {
    let dir = Scratch::new("verbsemis");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    for sql in [";COMMIT", "COMMIT;;"] {
        a.q("BEGIN").ok("begin");
        assert_eq!(
            a.q("SELECT * FROM nosuch").status,
            b'E',
            "premise: the block failed"
        );
        let r = a.x(sql, &[]).ok(sql);
        assert_eq!(
            r.tags,
            vec!["ROLLBACK".to_string()],
            "{sql} ends the failed block"
        );
        assert_eq!(r.status, b'I', "{sql}");
        a.q("BEGIN").ok("begin");
        a.q("INSERT INTO t VALUES (2, 'two')").ok("a write");
        let r = a.x(sql, &[]).ok(sql);
        assert_eq!(r.status, b'I', "{sql} commits");
        assert_eq!(
            a.q("SELECT count(*) FROM t WHERE id = 2").single("kept"),
            "1",
            "{sql}"
        );
        a.q("DELETE FROM t WHERE id = 2").ok("undo");
    }
    let r = a.x("\u{b}BEGIN", &[]).ok("\\vBEGIN");
    assert_eq!(r.status, b'T');
    a.q("ROLLBACK").ok("end");
    let r = a.q("CHECKPOINT -- x").ok("a commented CHECKPOINT");
    assert_eq!(r.tags, vec!["CHECKPOINT".to_string()]);
    // Parse declaring no parameter, Bind with one value: a bad count.
    a.send(b'P', b"\0CHECKPOINT -- x\0\0\0");
    a.send(b'B', b"\0\0\0\0\0\x01\0\0\0\x01v\0\0");
    a.send(b'E', &[0, 0, 0, 0, 0]);
    a.send(b'S', &[]);
    let r = a.read_reply();
    assert_eq!(r.err("CHECKPOINT -- x with a value").code, "08P01");
}

// ---------------------------------------------------------------------------
// PostgreSQL 18.6 recordings, replayed whole (C33, the lead's class rule: every wire expectation
// comes from a committed PostgreSQL 18.6 recording and keeps every recorded case)
// ---------------------------------------------------------------------------

/// A recording made with the lane's pgproto.py (artie frontier/fastest/lanes/wire/raw/
/// loud-20261015/pg18-diff/pgproto.py) against PostgreSQL 18.6, copied verbatim into
/// tests/pg18/ (tests/pg18/PROVENANCE names each file's artie commit), is replayed here byte for
/// byte as pgproto.py sent it: the same messages, the same Sync points. Every backend message is
/// written as pgproto.py's describe() writes it, so each recorded command's answer compares line
/// by line with PostgreSQL's. Nothing is hand-copied, so no recorded case can be dropped or
/// reworded: the hand-written tests had dropped cases and changed seeds (the C33 audit,
/// 2026-10-15).
///
/// A command whose answer differs from PostgreSQL's is a DIVERGENCE. Each recording's test names
/// its divergences, by the command's ordinal in the file (the n-th `>>` line, from 1), with the
/// reason and owner, and its exact answer from `tests/ours`. The test passes only when the commands
/// that differ are EXACTLY those and our registered answers have not changed. A new difference,
/// changed registered answer, or registered command that now agrees fails. Only random() compares
/// its result shape instead of its sampled value. Each entry is one known divergent command.
///
/// Refuses, rather than skips, a recording with a command kind pgproto.py does not send, or with
/// no command at all.
mod recorded {
    use super::*;

    /// pgproto.py's describe(): one line per backend message, or None for a message it does not
    /// print. A RowDescription is written with its column names; [`same`] drops them when the
    /// recording's line has none (pgproto.py printed names only with PGPROTO_NAMES=1).
    fn describe(tag: u8, b: &[u8]) -> Option<String> {
        match tag {
            b'E' | b'N' => {
                let mut f: Vec<(u8, String)> = Vec::new();
                for part in b.split(|&c| c == 0).filter(|p| !p.is_empty()) {
                    let v = String::from_utf8_lossy(&part[1..]).into_owned();
                    f.retain(|(k, _)| *k != part[0]);
                    f.push((part[0], v));
                }
                let get = |k: u8| {
                    f.iter()
                        .find(|(c, _)| *c == k)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| "None".to_string())
                };
                Some(format!(
                    "{} S={} C={} M={}",
                    tag as char,
                    get(b'S'),
                    get(b'C'),
                    get(b'M')
                ))
            }
            b'T' => {
                let n = u16::from_be_bytes([b[0], b[1]]) as usize;
                let (mut i, mut oids, mut names) = (2, Vec::new(), Vec::new());
                for _ in 0..n {
                    let j = i + b[i..].iter().position(|&c| c == 0).unwrap();
                    names.push(py_str(&String::from_utf8_lossy(&b[i..j])));
                    i = j + 1;
                    oids.push(
                        u32::from_be_bytes([b[i + 6], b[i + 7], b[i + 8], b[i + 9]]).to_string(),
                    );
                    i += 18;
                }
                Some(format!(
                    "T oids=[{}] names=[{}]",
                    oids.join(", "),
                    names.join(", ")
                ))
            }
            b't' => {
                let n = u16::from_be_bytes([b[0], b[1]]) as usize;
                let oids: Vec<String> = (0..n)
                    .map(|k| {
                        let p = 2 + 4 * k;
                        u32::from_be_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]).to_string()
                    })
                    .collect();
                Some(format!("t params=[{}]", oids.join(", ")))
            }
            b'D' => {
                let n = u16::from_be_bytes([b[0], b[1]]) as usize;
                let (mut i, mut vals) = (2, Vec::new());
                for _ in 0..n {
                    let l = i32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
                    i += 4;
                    if l < 0 {
                        vals.push("None".to_string());
                    } else {
                        let l = l as usize;
                        vals.push(py_str(&String::from_utf8_lossy(&b[i..i + l])));
                        i += l;
                    }
                }
                Some(format!("D [{}]", vals.join(", ")))
            }
            b'C' => Some(format!("C {}", cstr(b))),
            b'Z' => Some(format!("Z {}", String::from_utf8_lossy(b))),
            b'1' => Some("ParseComplete".to_string()),
            b'2' => Some("BindComplete".to_string()),
            b'n' => Some("NoData".to_string()),
            b'3' => Some("CloseComplete".to_string()),
            b's' => Some("PortalSuspended".to_string()),
            _ => None,
        }
    }

    /// Python's repr() of a str, as pgproto.py's `f"D {vals}"` writes each value: single quotes
    /// unless the text has a single quote and no double one; backslash, the quote, \t \n \r
    /// escaped; other non-printable characters as \xNN, \uNNNN or \UNNNNNNNN. "Printable" is
    /// approximated from Rust's classes (control, whitespace other than space, the common format
    /// characters); a value outside that approximation shows as a divergence, never as agreement.
    pub(super) fn py_str(s: &str) -> String {
        let q = if s.contains('\'') && !s.contains('"') {
            '"'
        } else {
            '\''
        };
        let mut out = String::with_capacity(s.len() + 2);
        out.push(q);
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                c if c == q => {
                    out.push('\\');
                    out.push(c);
                }
                c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                    out.push_str(&format!("\\x{:02x}", c as u32))
                }
                c if c.is_ascii() => out.push(c),
                c if c.is_control()
                    || (c.is_whitespace() && c != ' ')
                    || matches!(c, '\u{ad}' | '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}'
                        | '\u{2060}'..='\u{2064}' | '\u{feff}') =>
                {
                    let v = c as u32;
                    if v < 0x100 {
                        out.push_str(&format!("\\x{v:02x}"));
                    } else if v < 0x10000 {
                        out.push_str(&format!("\\u{v:04x}"));
                    } else {
                        out.push_str(&format!("\\U{v:08x}"));
                    }
                }
                c => out.push(c),
            }
        }
        out.push(q);
        out
    }

    /// The IDs of postgres/COMPAT.md's "PostgreSQL 18.6 recorded divergences" table: the first cell
    /// of each of its rows.
    pub(super) fn compat_ids() -> Vec<&'static str> {
        let compat = include_str!("../../COMPAT.md");
        let section = compat
            .split("\n## ")
            .find(|s| s.starts_with("PostgreSQL 18.6 recorded divergences"))
            .expect("postgres/COMPAT.md has no \"PostgreSQL 18.6 recorded divergences\" section");
        section
            .lines()
            .filter_map(|l| l.strip_prefix("| "))
            .filter_map(|l| l.split(" |").next())
            .map(|id| id.trim().trim_matches('`'))
            .filter(|id| id.starts_with("PGDIV-"))
            .collect()
    }

    /// One recorded command: its `>>` text and PostgreSQL's lines for it.
    struct Command {
        text: String,
        want: Vec<String>,
    }

    fn parse(recording: &str) -> Vec<Command> {
        let mut out: Vec<Command> = Vec::new();
        for line in recording.lines() {
            if let Some(cmd) = line.strip_prefix(">> ") {
                out.push(Command {
                    text: cmd.to_string(),
                    want: Vec::new(),
                });
            } else if let Some(answer) = line.strip_prefix("   ") {
                out.last_mut()
                    .expect("an answer line before any command")
                    .want
                    .push(answer.to_string());
            }
        }
        out
    }

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![tag];
        m.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        m.extend_from_slice(body);
        m
    }

    fn c(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    fn unnamed(s: &str) -> &str {
        if s == "-" {
            ""
        } else {
            s
        }
    }

    /// Text values as pgproto.py binds them: each `|||` part stripped of surrounding spaces.
    fn bind_values(vals: &[&str]) -> Vec<u8> {
        let mut b = (vals.len() as u16).to_be_bytes().to_vec();
        for v in vals {
            b.extend_from_slice(&(v.len() as i32).to_be_bytes());
            b.extend_from_slice(v.as_bytes());
        }
        b
    }

    fn read_until_ready(w: &mut Wire) -> Vec<String> {
        let mut lines = Vec::new();
        loop {
            let mut head = [0u8; 5];
            if w.s.read_exact(&mut head).is_err() {
                lines.push("(the server closed the connection)".to_string());
                return lines;
            }
            let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            w.s.read_exact(&mut body).unwrap();
            if let Some(l) = describe(head[0], &body) {
                lines.push(l);
            }
            if head[0] == b'Z' {
                return lines;
            }
        }
    }

    /// pgproto.py's main loop for one command; the answer lines, or none for a queued message.
    fn send(w: &mut Wire, pending: &mut Vec<u8>, cmd: &str) -> Vec<String> {
        let (kind, rest) = cmd.split_once(' ').unwrap_or((cmd, ""));
        match kind {
            "P" => {
                let (name, sql) = rest.split_once(' ').unwrap();
                let mut b = c(unnamed(name));
                b.extend(c(sql));
                b.extend_from_slice(&0u16.to_be_bytes());
                pending.extend(frame(b'P', &b));
                Vec::new()
            }
            "B" => {
                let parts: Vec<&str> = rest.split("|||").map(str::trim).collect();
                let mut head = parts[0].split_whitespace();
                let (portal, stmt) = (head.next().unwrap(), head.next().unwrap());
                let mut b = c(unnamed(portal));
                b.extend(c(unnamed(stmt)));
                b.extend_from_slice(&0u16.to_be_bytes());
                b.extend(bind_values(&parts[1..]));
                b.extend_from_slice(&0u16.to_be_bytes());
                pending.extend(frame(b'B', &b));
                Vec::new()
            }
            "E" => {
                let mut it = rest.split_whitespace();
                let (portal, n) = (it.next().unwrap(), it.next().unwrap());
                let mut b = c(unnamed(portal));
                b.extend_from_slice(&n.parse::<i32>().unwrap().to_be_bytes());
                pending.extend(frame(b'E', &b));
                Vec::new()
            }
            "C" => {
                let mut it = rest.split_whitespace();
                let (what, name) = (it.next().unwrap(), it.next().unwrap());
                let mut b = what.as_bytes().to_vec();
                b.extend(c(unnamed(name)));
                pending.extend(frame(b'C', &b));
                Vec::new()
            }
            "S" => {
                let mut out = std::mem::take(pending);
                out.extend(frame(b'S', &[]));
                w.s.write_all(&out).unwrap();
                read_until_ready(w)
            }
            "Q" => {
                w.s.write_all(&frame(b'Q', &c(rest))).unwrap();
                read_until_ready(w)
            }
            "X" | "D" => {
                let (oids, rest) = rest.split_once(' ').unwrap();
                let parts: Vec<&str> = rest.split("|||").map(str::trim).collect();
                let (sql, vals) = (parts[0], &parts[1..]);
                let oid_list: Vec<u32> = if oids == "-" {
                    Vec::new()
                } else {
                    oids.split(',').map(|o| o.parse().unwrap()).collect()
                };
                let mut parse = c("");
                parse.extend(c(sql));
                parse.extend_from_slice(&(oid_list.len() as u16).to_be_bytes());
                for o in &oid_list {
                    parse.extend_from_slice(&o.to_be_bytes());
                }
                let mut out = frame(b'P', &parse);
                if kind == "D" {
                    out.extend(frame(b'D', &c("S")));
                } else {
                    let mut bind = c("");
                    bind.extend(c(""));
                    bind.extend_from_slice(&0u16.to_be_bytes());
                    bind.extend(bind_values(vals));
                    bind.extend_from_slice(&0u16.to_be_bytes());
                    out.extend(frame(b'B', &bind));
                    out.extend(frame(b'D', &c("P")));
                    let mut exec = c("");
                    exec.extend_from_slice(&0i32.to_be_bytes());
                    out.extend(frame(b'E', &exec));
                }
                out.extend(frame(b'S', &[]));
                w.s.write_all(&out).unwrap();
                read_until_ready(w)
            }
            other => panic!("refused: command kind {other:?} is not one pgproto.py sends ({cmd})"),
        }
    }

    /// A RowDescription line compares without its names when PostgreSQL's line has none, and the
    /// one-letter lines of the earliest recordings (pgproto.py printed `1`, `2`, `n` before it
    /// named them) compare as the names it prints now.
    fn same(want: &[String], got: &[String]) -> bool {
        let named = |w: &str| match w {
            "1" => "ParseComplete",
            "2" => "BindComplete",
            "n" => "NoData",
            "3" => "CloseComplete",
            "s" => "PortalSuspended",
            other => other,
        }
        .to_string();
        want.len() == got.len()
            && want.iter().map(|w| named(w)).zip(got).all(|(w, g)| {
                if w.starts_with("T oids=") && !w.contains(" names=") {
                    g.split(" names=").next() == Some(w.as_str())
                } else {
                    &w == g
                }
            })
    }

    fn answer_difference(
        n: usize,
        cmd: &Command,
        got: &[String],
        registered: Option<(&str, &[String])>,
    ) -> Option<String> {
        let Some((why, ours)) = registered else {
            return (!same(&cmd.want, got)).then(|| {
                format!(
                    "\n#{n} >> {}\n  PostgreSQL: {:?}\n  ours:       {:?}",
                    cmd.text, cmd.want, got
                )
            });
        };
        // A random draw is not expected to repeat, even if it happens to equal PostgreSQL's draw.
        let random = why.split_whitespace().next() == Some("PGDIV-RANDOM-VALUE");
        if !random && same(&cmd.want, got) {
            return Some(format!(
                "\n#{n} >> {} now AGREES with PostgreSQL; remove its divergence ({why})",
                cmd.text
            ));
        }
        let unchanged = if random {
            let shape = |lines: &[String]| {
                lines
                    .iter()
                    .map(|line| {
                        if line.starts_with("D ") {
                            "D".to_string()
                        } else {
                            line.clone()
                        }
                    })
                    .collect::<Vec<_>>()
            };
            shape(ours) == shape(got)
        } else {
            ours == got
        };
        (!unchanged).then(|| {
            format!(
                "\n#{n} >> {} our answer changed ({why})\n  recorded ours: {:?}\n  current ours:  {:?}",
                cmd.text, ours, got
            )
        })
    }

    #[cfg(test)]
    mod register_tests {
        use super::*;

        fn arrres() -> (Command, Vec<String>) {
            let pg = parse(include_str!("pg18/pg18-arrres.out"));
            let ours = parse(include_str!("ours/pg18-arrres.out"));
            (
                pg.into_iter().next().unwrap(),
                ours.into_iter().next().unwrap().want,
            )
        }

        fn check(cmd: &Command, got: &[String], original: &[String]) -> Option<String> {
            answer_difference(
                1,
                cmd,
                got,
                Some(("PGDIV-ARRAY-RESULT-BLOB value [wire]", original)),
            )
        }

        #[test]
        fn a_registered_answer_changed_to_xx000_is_refused() {
            let (cmd, ours) = arrres();
            let got = vec![
                "E S=ERROR C=XX000 M=recorded regression".into(),
                "Z I".into(),
            ];
            assert!(
                check(&cmd, &got, &ours).is_some(),
                "a registered command changed into an error"
            );
        }

        #[test]
        fn a_registered_answer_changed_to_another_wrong_value_is_refused() {
            let (cmd, ours) = arrres();
            let mut got = ours.clone();
            *got.iter_mut().find(|l| l.starts_with("D ")).unwrap() = "D ['{b,a}']".into();
            assert!(
                check(&cmd, &got, &ours).is_some(),
                "a registered wrong answer changed"
            );
        }

        #[test]
        fn a_registered_cascade_count_change_is_refused() {
            let pg = parse(include_str!("pg18/pg18-c33-engineerr.out"));
            let ours = parse(include_str!("ours/pg18-c33-engineerr.out"));
            let mut got = ours[16].want.clone();
            *got.iter_mut().find(|l| l.starts_with("D ")).unwrap() = "D ['5']".into();
            assert!(
                answer_difference(
                    17,
                    &pg[16],
                    &got,
                    Some(("PGDIV-INT-RANGE cascade [engine]", &ours[16].want))
                )
                .is_some(),
                "a registered cascade count changed from 2 to 5"
            );
        }

        #[test]
        fn a_registered_answer_starting_to_agree_is_refused() {
            let (cmd, ours) = arrres();
            assert!(check(&cmd, &cmd.want, &ours)
                .unwrap()
                .contains("now AGREES"));
        }

        #[test]
        fn an_unregistered_difference_is_refused() {
            let (cmd, ours) = arrres();
            assert!(answer_difference(1, &cmd, &ours, None).is_some());
        }

        #[test]
        fn the_exact_registered_answer_still_passes() {
            let (cmd, ours) = arrres();
            assert!(check(&cmd, &ours, &ours).is_none());
        }

        #[test]
        fn a_random_draw_may_change_but_its_type_and_row_count_may_not() {
            let pg = parse(include_str!("pg18/pg18-random.out"));
            let ours = parse(include_str!("ours/pg18-random.out"));
            let original = &ours[1].want;
            let registered = Some(("PGDIV-RANDOM-VALUE harness [harness]", original.as_slice()));
            let mut changed = original.clone();
            *changed.iter_mut().find(|l| l.starts_with("D ")).unwrap() = "D ['0.25']".into();
            assert!(answer_difference(2, &pg[1], &changed, registered).is_none());
            let mut wrong_type = changed.clone();
            *wrong_type.iter_mut().find(|l| l.starts_with("T ")).unwrap() =
                "T oids=[25] names=['random']".into();
            assert!(answer_difference(2, &pg[1], &wrong_type, registered).is_some());
            let mut extra_row = changed.clone();
            extra_row.insert(2, "D ['0.5']".into());
            assert!(answer_difference(2, &pg[1], &extra_row, registered).is_some());
            let mut missing_row = changed;
            missing_row.retain(|l| !l.starts_with("D "));
            assert!(answer_difference(2, &pg[1], &missing_row, registered).is_some());
            let error = vec![
                "E S=ERROR C=XX000 M=recorded regression".into(),
                "Z I".into(),
            ];
            assert!(answer_difference(2, &pg[1], &error, registered).is_some());
        }
    }

    /// Replay an unchanged sequence of commands selected from a committed PG recording.
    pub(super) fn replay_selected(name: &str, recording: &str, ordinals: &[usize]) {
        let commands = parse(recording);
        let mut selected = String::new();
        for &ordinal in ordinals {
            let command = &commands[ordinal - 1];
            selected.push_str(&format!(">> {}\n", command.text));
            for line in &command.want {
                selected.push_str(&format!("   {line}\n"));
            }
        }
        replay(name, &selected, &selected, &[]);
    }

    /// Replay `recording` on a fresh server and check that the commands answering otherwise than
    /// PostgreSQL are exactly `divergences`, with each registered answer pinned by `ours`.
    pub(super) fn replay(name: &str, recording: &str, ours: &str, divergences: &[(usize, &str)]) {
        let cmds = parse(recording);
        let baseline = parse(ours);
        assert!(!cmds.is_empty(), "refused: recording {name} has no command");
        assert_eq!(
            cmds.len(),
            baseline.len(),
            "{name}: our recording has a different command count"
        );
        for (pg, ours) in cmds.iter().zip(&baseline) {
            assert_eq!(
                pg.text, ours.text,
                "{name}: our recording describes a different command"
            );
        }
        let mut ordinals = std::collections::BTreeSet::new();
        for (n, why) in divergences {
            assert!(ordinals.insert(n), "{name}: duplicate divergence #{n}");
            if why.split_whitespace().next() == Some("PGDIV-RANDOM-VALUE") {
                assert!(
                    name == "pg18-random.out" && *n == 2,
                    "the random shape exception belongs only to pg18-random.out #2"
                );
            }
        }
        let dir = Scratch::new(&format!("rec{}", name.len()));
        let server = Server::start(&dir.db(), &[]);
        let mut w = server.connect();
        let mut pending = Vec::new();
        let mut report = String::new();
        let mut differing = Vec::new();
        for (i, cmd) in cmds.iter().enumerate() {
            let got = send(&mut w, &mut pending, &cmd.text);
            let n = i + 1;
            let registered = divergences.iter().find(|(k, _)| *k == n);
            let agrees = same(&cmd.want, &got);
            if !agrees {
                differing.push(n);
            }
            if let Some(problem) = answer_difference(
                n,
                cmd,
                &got,
                registered.map(|(_, why)| (*why, baseline[i].want.as_slice())),
            ) {
                report.push_str(&problem);
            }
        }
        for (k, why) in divergences {
            assert!(
                *k >= 1 && *k <= cmds.len(),
                "{name}: divergence #{k} ({why}) names no command"
            );
            let id = why.split_whitespace().next().unwrap_or("");
            assert!(
                id.starts_with("PGDIV-") && compat_ids().contains(&id),
                "{name}: divergence #{k} cites no COMPAT.md entry ({why}); every registered \
                 divergence names its row of postgres/COMPAT.md's recorded-divergence table"
            );
        }
        assert!(
            report.is_empty(),
            "{name}: {} of {} commands differ from PostgreSQL 18.6 (registered: {:?}; differing: {:?}){report}",
            differing.len(),
            cmds.len(),
            divergences.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            differing
        );
    }
}

#[test]
fn python_repr_matches_pgproto_for_the_cases_recordings_hold() {
    // Expected values are CPython 3's repr() of each string (python3 -c 'print(repr(...))').
    for (s, want) in [
        ("abc", "'abc'"),
        ("it's", "\"it's\""),
        ("a'b\"c", "'a\\'b\"c'"),
        ("a\\b", "'a\\\\b'"),
        ("x\ny\tz", "'x\\ny\\tz'"),
        ("\u{0b}", "'\\x0b'"),
        ("é", "'é'"),
        ("\u{a0}", "'\\xa0'"),
        ("\u{fffd}", "'\u{fffd}'"),
    ] {
        assert_eq!(recorded::py_str(s), want, "{s:?}");
    }
}

/// Every ID in COMPAT.md's recorded-divergence table is cited by some replay's register, and every
/// cited ID is in the table: the two cannot drift apart (the lead's condition on the register,
/// 2026-10-15: every registered divergence has a COMPAT entry and its PostgreSQL recording).
#[test]
fn every_recorded_divergence_has_its_compat_entry_and_every_entry_is_cited() {
    let ids = recorded::compat_ids();
    assert!(!ids.is_empty(), "refused: COMPAT.md's divergence table has no row");
    let source = include_str!("wire_sessions.rs");
    let cited: std::collections::BTreeSet<&str> = source
        .split('"')
        .filter(|s| s.starts_with("PGDIV-"))
        .filter_map(|s| s.split_whitespace().next())
        .filter(|id| id.len() > "PGDIV-".len())
        .collect();
    let table: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
    let uncited: Vec<_> = table.difference(&cited).collect();
    let missing: Vec<_> = cited.difference(&table).collect();
    assert!(
        uncited.is_empty() && missing.is_empty(),
        "COMPAT.md rows no register cites: {uncited:?}; register IDs with no COMPAT.md row: {missing:?}"
    );
}

macro_rules! replays_postgresql {
    ($test:ident, $file:literal, [$(($n:literal, $why:literal)),* $(,)?]) => {
        #[test]
        fn $test() {
            recorded::replay($file, include_str!(concat!("pg18/", $file)),
                include_str!(concat!("ours/", $file)), &[$(($n, $why)),*]);
        }
    };
}

// One test per recording; a divergence list is filled only from a read of that command's diff.
replays_postgresql!(
    pg18_arrays_replays_as_postgresql_answered,
    "pg18-arrays.out",
    [
        (5, "PGDIV-DESCRIBE-PARAM-TYPE value [wire]: Describe types $1 in `o = $1` as 1007 int4[] where PG says 1028 oid[]"),
    ]
);
replays_postgresql!(
    pg18_arrres_replays_as_postgresql_answered,
    "pg18-arrres.out",
    [
        (1, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: text-format bound text[] $1::text[] comes back as our internal array blob rendered as bytea hex '\\\\x030f0f6162' instead of '{a,b}' (oid 1009 agrees)"),
        (2, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: text-format bound int[] $1::int[] comes back as '\\\\x03090102' instead of '{1,2}' (oid 1007 agrees)"),
        (3, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: uncast $1 declared 1009 comes back as '\\\\x030f0f6162' instead of '{a,b}' (oid 1009 agrees)"),
    ]
);
replays_postgresql!(
    pg18_audit1_lib_a_bytea_parameter_with_a_non_ascii_digit_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-lib-a_bytea_parameter_with_a_non_ascii_digit_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (2, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param \\x0é0: PG refuses 22023 at Bind; ours BindComplete+RowDescription first (same text)"),
        (3, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param \\xé: PG refuses 22023 at Bind; ours BindComplete+RowDescription first"),
        (4, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param \\x0😀0: PG refuses 22023 at Bind; ours BindComplete+RowDescription first"),
        (5, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param \\xGG: PG refuses 22023 at Bind; ours BindComplete+RowDescription first"),
        (6, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param \\x0 (odd digits): PG refuses 22023 at Bind; ours BindComplete+RowDescription first"),
    ]
);
replays_postgresql!(
    pg18_audit1_lib_parameters_decode_at_postgresqls_edges_replays_as_postgresql_answered,
    "pg18-audit1-lib-parameters_decode_at_postgresqls_edges.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (3, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: real param 3.5e+38: PG refuses at Bind (no BindComplete); ours sends BindComplete+RowDescription then 22003, text also gains a leading 'value '"),
        (4, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: float8 param 1e-400: PG refuses at Bind; ours BindComplete+RowDescription then 22003 with extra 'value ' prefix"),
        (5, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: float8 param -1e-400: PG refuses at Bind; ours BindComplete+RowDescription then 22003 with extra 'value ' prefix"),
        (6, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: real param 1e-50: PG refuses at Bind; ours BindComplete+RowDescription then 22003 with extra 'value ' prefix"),
        (12, "PGDIV-NUMERIC-AS-FLOAT value [engine]: SILENT: numeric param 1e-400 answers '0' where PG answers the exact 0.(399 zeros)1; the numeric decode underflows"),
        (15, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int4 param '_100': PG refuses 22P02 at Bind; ours BindComplete+RowDescription first (same code and text)"),
        (16, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int4 param '0x_': PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
        (17, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int4 param '0b__1': PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
        (18, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int4 param '0o_': PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
        (19, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int4 param '100_': PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_bytea_parameter_with_a_non_ascii_digit_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_bytea_parameter_with_a_non_ascii_digit_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (5, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: INSERT bytea param \\x0é0: PG refuses 22023 at Bind; ours BindComplete+NoData first"),
        (7, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: INSERT bytea param \\x0😀0: PG refuses 22023 at Bind; ours BindComplete+NoData first"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_chained_or_read_only_transaction_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_chained_or_read_only_transaction_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (9, "PGDIV-COMMIT-AND-CHAIN ours-only [ours-only]: COMMIT AND CHAIN in a block refused 0A000 and aborts it (PG: COMMIT, Z T)"),
        (11, "PGDIV-COMMIT-AND-CHAIN cascade [ours-only]: count(*) id=2 is 0 vs 1 because #9 refused the COMMIT AND CHAIN so #10 ROLLBACK discarded row 2 (cascade of #9)"),
        (12, "PGDIV-READ-ONLY-TX ours-only [ours-only]: BEGIN READ ONLY refused 0A000 (PG: BEGIN, Z T)"),
        (13, "PGDIV-READ-ONLY-TX ours-only [ours-only]: START TRANSACTION READ ONLY refused 0A000 (PG warns 25001 since #12 opened a block)"),
        (14, "PGDIV-READ-ONLY-TX ours-only [ours-only]: BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY refused 0A000 (PG warns 25001, BEGIN)"),
        (15, "PGDIV-READ-ONLY-TX cascade [ours-only]: BEGIN READ WRITE: PG warns 25001 already in a transaction (opened by #12); ours idle so no warning (cascade of #12)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_correlated_generate_series_column_is_scoped_to_its_select_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_correlated_generate_series_column_is_scoped_to_its_select.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (12, "PGDIV-FUNC-ALIAS-CORRELATED value [wire]: bare function alias g referenced from a correlated EXISTS (w.k = g): ours errors 42703 'no such column: g' where PG answers 3 rows (g.g in #11 works)"),
        (16, "PGDIV-EXPR-TYPE-TEXT value [wire]: (SELECT sum(g) FROM c) over a CTE typed 25 text in RowDescription; PG 20 int8 (rows agree)"),
        (20, "PGDIV-PARSE-PREFIX error-text [wire]: 42702 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_dump_headers_settings_are_accepted_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_dump_headers_settings_are_accepted.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (14, "PGDIV-SET-REFUSED ours-only [ours-only]: SET xmloption = document refused 0A000 (PG: SET)"),
        (15, "PGDIV-SET-REFUSED ours-only [ours-only]: SET transaction_timeout = '5s' refused 0A000 (PG: SET)"),
        (16, "PGDIV-SET-REFUSED error-code [wire]: SET default_tablespace = 'fast': PG 22023 invalid value for parameter; ours 0A000 not supported"),
        (17, "PGDIV-SET-REFUSED error-code [wire]: SET default_table_access_method = columnar: PG 22023 invalid value; ours 0A000 not supported"),
        (18, "PGDIV-SET-REFUSED ours-only [ours-only]: SET geqo_threshold = 12 refused 0A000 (PG: SET)"),
        (19, "PGDIV-SET-REFUSED ours-only [ours-only]: SHOW geqo_threshold refused 0A000 (PG: '12'; would also cascade from #18)"),
        (20, "PGDIV-PARSE-PREFIX error-text [wire]: 42704 both; ours prefixes the message with 'Parse error: '"),
        (21, "PGDIV-PARSE-PREFIX error-text [wire]: SHOW no_such_parameter 42704 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_parameter_is_typed_by_every_kind_of_from_item_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_parameter_is_typed_by_every_kind_of_from_item.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_parameter_over_the_simple_protocol_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_parameter_over_the_simple_protocol_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_read_only_begin_inside_a_block_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_read_only_begin_inside_a_block_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (5, "PGDIV-READ-ONLY-TX ours-only [ours-only]: BEGIN READ ONLY inside a block: ours 0A000 aborts the block (Z E); PG warns 25001 and makes it read-only (Z T)"),
        (6, "PGDIV-READ-ONLY-TX cascade [ours-only]: INSERT: PG 25006 read-only; ours 25P02 because #5 aborted the block (cascade of #5)"),
        (8, "PGDIV-READ-ONLY-TX ours-only [ours-only]: multi-statement: BEGIN READ ONLY refused 0A000 (Z I) where PG opens it and INSERT fails 25006 (Z E)"),
        (9, "PGDIV-READ-ONLY-TX cascade [ours-only]: PG is in an aborted block (25P02); ours idle and answers count 0 (cascade of #8)"),
        (10, "PGDIV-READ-ONLY-TX cascade [ours-only]: ROLLBACK: ours warns 25P01 no transaction because #8 left us idle (cascade of #8)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_a_three_part_reference_resolves_through_its_schema_replays_as_postgresql_answered,
    "pg18-audit1-ws-a_three_part_reference_resolves_through_its_schema.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (3, "PGDIV-DESCRIBE-PARAM-TYPE value [wire]: Describe types $1 in pg_class.relname = $1 as 25 text; PG 19 name"),
        (4, "PGDIV-DESCRIBE-PARAM-TYPE value [wire]: Describe types $1 in pg_catalog.pg_class.relname = $1 as 25 text; PG 19 name"),
        (5, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: public.t.c with t aliased x: PG refuses at Parse (no ParseComplete) 'invalid reference to FROM-clause entry'; ours ParseComplete then 'Parse error: missing FROM-clause entry' (same 42P01)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_aggregates_have_postgres_types_on_both_protocols_replays_as_postgresql_answered,
    "pg18-audit1-ws-aggregates_have_postgres_types_on_both_protocols.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (6, "PGDIV-AVG-SCALE value [engine]: SILENT: avg(int) over simple protocol is '1.5'; PG '1.5000000000000000' (numeric display scale 16)"),
        (7, "PGDIV-AVG-SCALE value [engine]: SILENT: avg(int) over extended protocol is '1.5'; PG '1.5000000000000000'"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_checkpoint_inside_a_block_keeps_the_block_replays_as_postgresql_answered,
    "pg18-audit1-ws-checkpoint_inside_a_block_keeps_the_block.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_delete_using_deletes_only_the_joined_rows_replays_as_postgresql_answered,
    "pg18-audit1-ws-delete_using_deletes_only_the_joined_rows.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_delete_using_reads_one_namespace_replays_as_postgresql_answered,
    "pg18-audit1-ws-delete_using_reads_one_namespace.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (6, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42702 both; ours 'Parse error: ambiguous column name: id' vs PG 'column reference \"id\" is ambiguous'"),
        (8, "PGDIV-PARSE-PREFIX error-text [wire]: 42712 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_delete_using_returning_a_using_column_is_refused_replays_as_postgresql_answered,
    "pg18-audit1-ws-delete_using_returning_a_using_column_is_refused.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (6, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: DELETE ... USING k RETURNING k.flag refused 0A000; PG deletes row 2 and returns 't'"),
        (7, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 2 vs 1 (cascade of #6)"),
        (8, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING d.v, j.* refused 0A000; PG DELETE 0"),
        (9, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 2 vs 1 (cascade of #6)"),
        (10, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING * over DELETE ... USING refused 0A000; PG DELETE 0"),
        (11, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 2 vs 1 (cascade of #6)"),
        (12, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: RETURNING d.id, v deletes row 2 (DELETE 1) where PG had already deleted it at #6 (DELETE 0); cascade of #6"),
        (15, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING d.v, j.* refused 0A000 inside a block (Z E)"),
        (18, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING * over DELETE ... USING refused 0A000 inside a block (Z E)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_delete_using_returning_resolves_a_bare_name_over_both_replays_as_postgresql_answered,
    "pg18-audit1-ws-delete_using_returning_resolves_a_bare_name_over_both.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (6, "PGDIV-PARSE-PREFIX error-text [wire]: 42702 both; ours prefixes 'Parse error: '"),
        (8, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: DELETE ... USING ... RETURNING flag (a USING column) refused 0A000; PG deletes 2 rows"),
        (9, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 3 vs 1: #8 refused so rows 2,3 were not deleted (cascade of #8)"),
        (10, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING k (whole-row of a USING relation, PG composite oid 16566) refused 0A000; PG DELETE 0"),
        (11, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 3 vs 1 (cascade of #8)"),
        (12, "PGDIV-BOOL-TEXT value [wire]: SILENT: EXISTS(...) in RETURNING typed 25 text where PG 16 bool; the DELETE 1 vs DELETE 0 row count is a cascade of #8"),
        (13, "PGDIV-DELETE-USING-RETURNING cascade [ours-only]: count(*) 2 vs 1 (cascade of #8; #12 deleted row 2 that PG had already deleted)"),
        (16, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING flag (USING column) refused 0A000 inside a block, aborting it (Z E)"),
        (19, "PGDIV-DELETE-USING-RETURNING ours-only [ours-only]: RETURNING k (whole-row of USING relation) refused 0A000 inside a block (Z E)"),
        (22, "PGDIV-BOOL-TEXT value [wire]: SILENT: EXISTS(...) in RETURNING answers '1' typed 25 text; PG 't' typed 16 bool (row b and DELETE 1 agree)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_generate_series_may_read_the_row_it_joins_replays_as_postgresql_answered,
    "pg18-audit1-ws-generate_series_may_read_the_row_it_joins.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (6, "PGDIV-WITH-ORDINALITY ours-only [ours-only]: generate_series(...) WITH ORDINALITY refused 0A000"),
        (7, "PGDIV-COLUMN-ALIAS-COUNT-CODE error-code [wire]: AS g(a, b) over a 1-column function: PG 42P10 'table \"g\" has 1 columns available but 2 columns specified'; ours 42601 'too many column aliases'"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_set_honours_or_refuses_each_parameter_replays_as_postgresql_answered,
    "pg18-audit1-ws-set_honours_or_refuses_each_parameter.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (5, "PGDIV-READ-ONLY-TX ours-only [ours-only]: SET transaction_read_only = on in a block refused 0A000 and aborts it"),
        (6, "PGDIV-READ-ONLY-TX cascade [ours-only]: INSERT: PG 25006 read-only; ours 25P02 because #5 aborted the block (cascade of #5)"),
        (9, "PGDIV-READ-ONLY-TX ours-only [ours-only]: SET default_transaction_read_only = on refused 0A000"),
        (10, "PGDIV-READ-ONLY-TX ours-only [ours-only]: SET LOCAL transaction_read_only TO true outside a block refused 0A000; PG warns 25P01 and answers SET"),
        (15, "PGDIV-SET-REFUSED ours-only [ours-only]: SET client_encoding = 'LATIN1' refused 0A000"),
        (16, "PGDIV-SET-REFUSED ours-only [ours-only]: SET standard_conforming_strings = off refused 0A000"),
        (17, "PGDIV-PARSE-PREFIX error-text [wire]: SET synchronous: 42704 both; ours prefixes 'Parse error: '"),
        (18, "PGDIV-PARSE-PREFIX error-text [wire]: SET no_such_parameter: 42704 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_set_operations_keep_their_grouping_and_clauses_replays_as_postgresql_answered,
    "pg18-audit1-ws-set_operations_keep_their_grouping_and_clauses.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (13, "PGDIV-DERIVED-COLUMN-BYTEA value [wire]: CTE column x in WITH ... UNION ALL typed 17 bytea; PG 23 int4 (rows agree)"),
        (14, "PGDIV-DERIVED-COLUMN-BYTEA value [wire]: CTE column id in WITH ... UNION ALL typed 17 bytea; PG 23 int4 (rows agree)"),
    ]
);
replays_postgresql!(
    pg18_audit1_ws_untyped_contexts_type_their_parameters_replays_as_postgresql_answered,
    "pg18-audit1-ws-untyped_contexts_type_their_parameters.out",
    [
        (1, "PGDIV-PUBLIC-SCHEMA-NOOP refusal [wire]: DROP SCHEMA public CASCADE drops tables, but CREATE SCHEMA public is unconditionally refused 42601; PostgreSQL recreates it (C56)"),
        (18, "PGDIV-DERIVED-COLUMN-BYTEA value [wire]: Describe: derived-table column c = count(*) typed 17 bytea in RowDescription; PG 20 int8"),
        (19, "PGDIV-DERIVED-COLUMN-BYTEA value [wire]: derived-table count(*) column typed 17 bytea in RowDescription (row '3' agrees); PG 20"),
        (26, "PGDIV-EXPR-TYPE-TEXT value [wire]: Describe: sum(n) OVER (PARTITION BY n > $1) typed 25 text in RowDescription; PG 20 int8 (param 23 agrees)"),
        (44, "PGDIV-SQLITE-FUNCTION-LEAK error-code [wire]: typeof(name) = $1: PG refuses at Parse 42883 'function typeof(text) does not exist'; ours ParseComplete then 42P18 (the SQLite typeof() leaks through)"),
        (45, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: no_such_typing(name) = $1: PG refuses at Parse (no ParseComplete); ours ParseComplete then 42883 'Parse error: no such function: no_such_typing'"),
    ]
);
replays_postgresql!(pg18_audit2_a_create_table_with_a_serial_completes_as_create_table_replays_as_postgresql_answered, "pg18-audit2-a_create_table_with_a_serial_completes_as_create_table.out", []);
replays_postgresql!(
    pg18_audit2_a_failed_commit_leaves_the_session_idle_replays_as_postgresql_answered,
    "pg18-audit2-a_failed_commit_leaves_the_session_idle.out",
    [
        (5, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: deferred FK at COMMIT 23503 both; ours 'deferred foreign key constraint failed on commit' vs PG 'insert or update on table \"c\" violates foreign key constraint \"c_pid_fkey\"'"),
    ]
);
replays_postgresql!(
    pg18_audit2_a_refused_create_table_leaves_no_sequence_replays_as_postgresql_answered,
    "pg18-audit2-a_refused_create_table_leaves_no_sequence.out",
    [
        (3, "PGDIV-PARSE-PREFIX error-text [wire]: 42830 both; ours prefixes 'Parse error: '"),
        (5, "PGDIV-RELATION-EXISTS-CODE error-code [wire]: CREATE TABLE of an existing name: PG 42P07 'relation \"p\" already exists'; ours 42601 'Parse error: table p already exists'"),
    ]
);
replays_postgresql!(
    pg18_audit2_aggregates_report_numeric_types_replays_as_postgresql_answered,
    "pg18-audit2-aggregates_report_numeric_types.out",
    [
        (4, "PGDIV-AVG-SCALE value [engine]: SILENT: avg(int) is '1.5'; PG '1.5000000000000000' (numeric display scale)"),
    ]
);
replays_postgresql!(
    pg18_audit2_alter_table_add_constraint_is_validated_and_atomic_replays_as_postgresql_answered,
    "pg18-audit2-alter_table_add_constraint_is_validated_and_atomic.out",
    [
        (7, "PGDIV-NUMERIC-OID-FLOAT8 value [wire]: NUMERIC(p,s) column n typed 701 float8 in RowDescription; PG 1700 (text '12.50' agrees)"),
        (8, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: ALTER TABLE ADD FOREIGN KEY 23503 both; ours 'violates the foreign key constraint it adds: a row's key (pid) is not present in table \"p\"' vs PG '... violates foreign key constraint \"c_pid_fkey\"'"),
        (10, "PGDIV-NUMERIC-OID-FLOAT8 value [wire]: NUMERIC(p,s) column n typed 701 float8; PG 1700 (same as #7, not a cascade)"),
    ]
);
replays_postgresql!(
    pg18_audit2_alter_table_add_constraint_takes_effect_and_keeps_the_rows_replays_as_postgresql_answered,
    "pg18-audit2-alter_table_add_constraint_takes_effect_and_keeps_the_rows.out",
    [
        (6, "PGDIV-NUMERIC-OID-FLOAT8 value [wire]: NUMERIC(p,s) column n typed 701 float8; PG 1700 (text agrees)"),
        (10, "PGDIV-NUMERIC-OID-FLOAT8 value [wire]: NUMERIC(p,s) column n typed 701 float8; PG 1700 (text agrees)"),
        (11, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: c.id' vs PG '... unique constraint \"c_pkey\"'"),
        (12, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23502 both; ours 'NOT NULL constraint failed: c.id' vs PG 'null value in column \"id\" of relation \"c\" violates not-null constraint'"),
        (13, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours 'FOREIGN KEY constraint failed' vs PG '... violates foreign key constraint \"c_pid_fkey\"'"),
        (14, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: c.v' vs PG '... unique constraint \"c_v_key\"'"),
    ]
);
replays_postgresql!(
    pg18_audit2_alter_table_add_constraint_works_in_every_transaction_state_replays_as_postgresql_answered,
    "pg18-audit2-alter_table_add_constraint_works_in_every_transaction_state.out",
    [
        (7, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.id\" vs PG \"duplicate key value violates unique constraint <table>_pkey\""),
        (14, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.id\" vs PG \"duplicate key value violates unique constraint <table>_pkey\""),
        (22, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.id\" vs PG \"duplicate key value violates unique constraint <table>_pkey\""),
        (30, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.id\" vs PG \"duplicate key value violates unique constraint <table>_pkey\""),
        (35, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.v\" vs PG \"... unique constraint <table>_v_key\""),
        (42, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.v\" vs PG \"... unique constraint <table>_v_key\""),
        (50, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.v\" vs PG \"... unique constraint <table>_v_key\""),
        (58, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours \"UNIQUE constraint failed: <table>.v\" vs PG \"... unique constraint <table>_v_key\""),
        (63, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours \"FOREIGN KEY constraint failed\" vs PG \"insert or update on table ... violates foreign key constraint <table>_p_fkey\""),
        (70, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours \"FOREIGN KEY constraint failed\" vs PG \"insert or update on table ... violates foreign key constraint <table>_p_fkey\""),
        (78, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours \"FOREIGN KEY constraint failed\" vs PG \"insert or update on table ... violates foreign key constraint <table>_p_fkey\""),
        (86, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours \"FOREIGN KEY constraint failed\" vs PG \"insert or update on table ... violates foreign key constraint <table>_p_fkey\""),
        (91, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23514 both; ours \"CHECK constraint failed: id > 0\" vs PG \"new row for relation ... violates check constraint <table>_id_check\""),
        (98, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23514 both; ours \"CHECK constraint failed: id > 0\" vs PG \"new row for relation ... violates check constraint <table>_id_check\""),
        (106, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23514 both; ours \"CHECK constraint failed: id > 0\" vs PG \"new row for relation ... violates check constraint <table>_id_check\""),
        (114, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23514 both; ours \"CHECK constraint failed: id > 0\" vs PG \"new row for relation ... violates check constraint <table>_id_check\""),
        (117, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: ALTER TABLE d ADD UNIQUE (v) over duplicates, 23505 both; ours \"UNIQUE constraint failed: d.v\" vs PG \"could not create unique index \\\"d_v_key\\\"\""),
    ]
);
replays_postgresql!(
    pg18_audit2_an_error_inside_a_transaction_aborts_it_as_in_postgres_replays_as_postgresql_answered,
    "pg18-audit2-an_error_inside_a_transaction_aborts_it_as_in_postgres.out",
    [
        (5, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.id' vs PG 'duplicate key value violates unique constraint \"t_pkey\"'"),
    ]
);
replays_postgresql!(
    pg18_audit2_char_n_is_blank_padded_as_in_postgres_replays_as_postgresql_answered,
    "pg18-audit2-char_n_is_blank_padded_as_in_postgres.out",
    [
        (5, "PGDIV-EXPR-TYPE-TEXT value [wire]: length(char(n)) typed 25 text in RowDescription; PG 23 int4 (value 3 agrees)"),
        (6, "PGDIV-CHAR-OVERFLOW-CODE error-code [wire]: char(6) overflow: PG 22001 'value too long for type character(6)'; ours XX000 'value too long for type character'"),
    ]
);
replays_postgresql!(
    pg18_audit2_ddl_statements_report_their_command_tags_replays_as_postgresql_answered,
    "pg18-audit2-ddl_statements_report_their_command_tags.out",
    []
);
replays_postgresql!(
    pg18_audit2_encode_answers_as_postgresql_does_replays_as_postgresql_answered,
    "pg18-audit2-encode_answers_as_postgresql_does.out",
    []
);
replays_postgresql!(
    pg18_audit2_end_and_abort_report_commit_and_rollback_replays_as_postgresql_answered,
    "pg18-audit2-end_and_abort_report_commit_and_rollback.out",
    []
);
replays_postgresql!(
    pg18_audit2_foreign_keys_are_enforced_on_the_trunk_and_on_a_branch_replays_as_postgresql_answered,
    "pg18-audit2-foreign_keys_are_enforced_on_the_trunk_and_on_a_branch.out",
    [
        (5, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours 'FOREIGN KEY constraint failed' vs PG 'insert or update on table \"c\" violates foreign key constraint \"c_pid_fkey\"'"),
        (6, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours 'FOREIGN KEY constraint failed' vs PG's table+constraint-named message"),
    ]
);
replays_postgresql!(pg18_audit2_generate_series_in_from_names_its_column_after_the_alias_replays_as_postgresql_answered, "pg18-audit2-generate_series_in_from_names_its_column_after_the_alias.out", []);
replays_postgresql!(
    pg18_audit2_information_schema_describes_tables_columns_and_keys_replays_as_postgresql_answered,
    "pg18-audit2-information_schema_describes_tables_columns_and_keys.out",
    [
        (3, "PGDIV-CATALOG-NAME-TYPES value [wire]: information_schema.columns: column_name/udt_name typed 25 (PG 19 name), is_nullable 25 (PG 1043 varchar); rows agree"),
        (4, "PGDIV-CATALOG-NAME-TYPES value [wire]: key_column_usage.column_name typed 25; PG 19 name (rows agree)"),
        (5, "PGDIV-CATALOG-NAME-TYPES value [wire]: information_schema.tables.table_name typed 25; PG 19 name (rows agree)"),
        (6, "PGDIV-INFOSCHEMA-NOTNULL-CHECK value [wire]: SILENT: table_constraints: constraint_type typed 25 (PG 1043) and ours omits PG18's two NOT NULL constraints listed as CHECK (2 rows vs 4)"),
    ]
);
replays_postgresql!(
    pg18_audit2_pg_indexes_and_pg_database_size_answer_replays_as_postgresql_answered,
    "pg18-audit2-pg_indexes_and_pg_database_size_answer.out",
    [
        (3, "PGDIV-CATALOG-NAME-TYPES value [wire]: pg_indexes.indexname typed 25 text; PG 19 name (rows agree)"),
        (4, "PGDIV-CATALOG-NAME-TYPES value [wire]: pg_indexes.indexname typed 25 text; PG 19 name (rows agree)"),
        (5, "PGDIV-EXPR-TYPE-TEXT value [wire]: pg_database_size() typed 25 text; PG 20 int8 (the byte count 24728 vs 8443583 is cluster-dependent and cannot agree)"),
    ]
);
replays_postgresql!(
    pg18_audit2_pgbench_table_probe_runs_replays_as_postgresql_answered,
    "pg18-audit2-pgbench_table_probe_runs.out",
    [
        (2, "PGDIV-CATALOG-NAME-TYPES value [wire]: pgbench probe: o.n (array_position) typed 25 (PG 23) and partstrat typed 25 (PG 18 char); row agrees"),
        (3, "PGDIV-CATALOG-NAME-TYPES value [wire]: current_schemas(false) typed 25 text; PG 1003 name[] (value '{public}' agrees)"),
    ]
);
replays_postgresql!(
    pg18_audit2_rollback_to_savepoint_recovers_a_failed_transaction_replays_as_postgresql_answered,
    "pg18-audit2-rollback_to_savepoint_recovers_a_failed_transaction.out",
    [
        (6, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.id' vs PG 'duplicate key value violates unique constraint \"t_pkey\"'"),
    ]
);
replays_postgresql!(
    pg18_audit2_row_value_comparisons_work_replays_as_postgresql_answered,
    "pg18-audit2-row_value_comparisons_work.out",
    []
);
replays_postgresql!(
    pg18_audit2_set_and_truncate_report_their_command_tags_replays_as_postgresql_answered,
    "pg18-audit2-set_and_truncate_report_their_command_tags.out",
    []
);
replays_postgresql!(
    pg18_audit2_set_reads_every_value_it_is_given_replays_as_postgresql_answered,
    "pg18-audit2-set_reads_every_value_it_is_given.out",
    [
        (3, "PGDIV-SET-REFUSED ours-only [ours-only]: SET DateStyle TO ISO, DMY refused 0A000"),
        (5, "PGDIV-SHOW-TAG protocol [wire]: SHOW DateStyle completes with tag 'SELECT 1' where PG sends 'SHOW' (value 'ISO, MDY' agrees)"),
    ]
);
replays_postgresql!(
    pg18_audit2_show_answers_from_the_settings_set_keeps_replays_as_postgresql_answered,
    "pg18-audit2-show_answers_from_the_settings_set_keeps.out",
    [
        (1, "PGDIV-SHOW-TAG protocol [wire]: SHOW search_path completes with tag 'SELECT 1' where PG sends 'SHOW' (value agrees)"),
        (3, "PGDIV-SHOW-TAG protocol [wire]: SHOW search_path tag 'SELECT 1' vs 'SHOW' (value 's, public' agrees)"),
        (4, "PGDIV-SHOW-TAG protocol [wire]: SHOW client_encoding tag 'SELECT 1' vs 'SHOW'"),
        (5, "PGDIV-SHOW-TAG protocol [wire]: SHOW standard_conforming_strings tag 'SELECT 1' vs 'SHOW'"),
        (6, "PGDIV-CLUSTER-SETTINGS cluster-state [cluster]: SHOW DateStyle: PG cluster's config file sets 'ISO, YMD', ours default 'ISO, MDY'; tag 'SELECT 1' vs 'SHOW' also differs (wire)"),
        (7, "PGDIV-SHOW-TAG protocol [wire]: SHOW IntervalStyle tag 'SELECT 1' vs 'SHOW'"),
        (8, "PGDIV-CLUSTER-SETTINGS cluster-state [cluster]: SHOW TimeZone: PG cluster's 'America/Toronto' vs ours 'UTC'; tag 'SELECT 1' vs 'SHOW' also differs (wire)"),
        (9, "PGDIV-SHOW-TAG protocol [wire]: SHOW bytea_output tag 'SELECT 1' vs 'SHOW'"),
        (10, "PGDIV-SHOW-TAG protocol [wire]: SHOW transaction_read_only tag 'SELECT 1' vs 'SHOW'"),
        (11, "PGDIV-PARSE-PREFIX error-text [wire]: 42704 both; ours prefixes 'Parse error: '"),
        (12, "PGDIV-PARSE-PREFIX error-text [wire]: 42704 both; ours prefixes 'Parse error: '"),
        (13, "PGDIV-PARSE-PREFIX error-text [wire]: 42704 both; ours prefixes 'Parse error: '"),
        (14, "PGDIV-SHOW-ALL ours-only [ours-only]: SHOW ALL refused 0A000 (PG lists every GUC, many of them cluster paths and ports)"),
        (15, "PGDIV-PG-SETTINGS value [wire]: SELECT ... FROM pg_settings: ours errors 42601 'no such object: pg_settings' where PG answers 5 rows (and boot/reset values that are partly the cluster's config file)"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_bare_numeric_division_by_zero_is_22012_replays_as_postgresql_answered,
    "pg18-audit3-a_bare_numeric_division_by_zero_is_22012.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s ...; DROP SCHEMA public: ours refuses the whole query 0A000 (schemas other than public)"),
        (4, "PGDIV-DIV-ZERO-NULL value [engine]: SILENT: bare NUMERIC x / 0 answers one NULL row typed 701 float8; PG RowDescription 1700 then 22012 division by zero"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_branch_call_parameter_past_the_limit_is_refused_replays_as_postgresql_answered,
    "pg18-audit3-a_branch_call_parameter_past_the_limit_is_refused.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of $18446744073709551615: PG refuses at Parse; ours ParseComplete then 42601 with 'Parse error: ' prefix"),
        (6, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 parameter number too large both; ours prefixes 'Parse error: '"),
        (7, "PGDIV-TURSO-FUNCTIONS error-code [ours-only]: Describe of turso_branch_create($65536): PG 42883 function turso_branch_create(unknown) does not exist (PG has no such function); ours ParseComplete then 42P02 there is no parameter $65536"),
        (9, "PGDIV-PARSE-PREFIX error-text [wire]: 42P02 both; ours prefixes 'Parse error: '"),
        (10, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of $2147483647: PG refuses 42P02 at Parse; ours ParseComplete then 42P02 with 'Parse error: ' prefix"),
        (12, "PGDIV-PARSE-PREFIX error-text [wire]: 42P02 both; ours prefixes 'Parse error: '"),
        (13, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of $2147483648: PG refuses at Parse; ours ParseComplete then 42601 with prefix"),
        (15, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (16, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of $4294967297: PG refuses at Parse; ours ParseComplete then 42601 with prefix"),
        (18, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (19, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of $99999999999999999999999: PG refuses at Parse; ours ParseComplete then 42601 with prefix"),
        (21, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_deferred_foreign_key_is_checked_at_commit_replays_as_postgresql_answered,
    "pg18-audit3-a_deferred_foreign_key_is_checked_at_commit.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (7, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: deferred FK at COMMIT 23503 both; ours 'deferred foreign key constraint failed on commit' vs PG naming table dc1 and constraint dc1_pid_fkey"),
        (15, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: deferred FK at COMMIT 23503 both; ours generic text vs PG naming dc2/dc2_pid_fkey"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_from_functions_arguments_read_the_items_before_it_replays_as_postgresql_answered,
    "pg18-audit3-a_from_functions_arguments_read_the_items_before_it.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_mistyped_commit_or_rollback_aborts_the_block_replays_as_postgresql_answered,
    "pg18-audit3-a_mistyped_commit_or_rollback_aborts_the_block.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (6, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: Invalid statement: '"),
        (11, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: Invalid statement: '"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_multi_statement_query_is_one_implicit_transaction_replays_as_postgresql_answered,
    "pg18-audit3-a_multi_statement_query_is_one_implicit_transaction.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.id' vs PG 'duplicate key value violates unique constraint \"t_pkey\"'"),
        (6, "PGDIV-TURSO-FUNCTIONS error-code [ours-only]: turso_branch_create in a multi-statement query: PG 42883 function does not exist (no turso_* in PG); ours 25001 cannot run inside a transaction block"),
        (8, "PGDIV-VACUUM ours-only [ours-only]: VACUUM t after an INSERT in one query: ours 0A000 VACUUM is not supported; PG 25001 VACUUM cannot run inside a transaction block"),
        (10, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.id' vs PG t_pkey message (COMMIT warning sequence agrees)"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_numeric_division_by_zero_is_22012_replays_as_postgresql_answered,
    "pg18-audit3-a_numeric_division_by_zero_is_22012.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-ERROR-BEFORE-ROWDESC protocol [wire]: x / 0 over NUMERIC: PG sends RowDescription 1700 then 22012; ours sends only the 22012 error (no RowDescription)"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_parameter_is_typed_from_the_relation_the_engine_writes_replays_as_postgresql_answered,
    "pg18-audit3-a_parameter_is_typed_from_the_relation_the_engine_writes.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: CREATE SCHEMA s refused 0A000 (recording relies on a schema s)"),
        (5, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE TABLE s.t fails because #4 made no schema s (cascade of #4); our code XX000 'no such database: s' is itself not PG's 3F000; missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (6, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT INTO s.t: XX000 no such database: s after BindComplete (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (7, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT FROM s.t: XX000 no such database: s (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (8, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT INTO s.t ON CONFLICT: XX000 (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (9, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: UPDATE s.t: XX000 after BindComplete (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (10, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT FROM s.t: XX000 (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (11, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT ... ON CONFLICT DO UPDATE into s.t: XX000 (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (12, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT FROM s.t: XX000 (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (15, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT FROM s.t: XX000 (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (18, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: s.t.c with only t in FROM: PG refuses at Parse 'invalid reference to FROM-clause entry for table \"t\"'; ours ParseComplete then 'Parse error: missing FROM-clause entry' (42P01 both)"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_parameter_number_past_the_limit_is_refused_replays_as_postgresql_answered,
    "pg18-audit3-a_parameter_number_past_the_limit_is_refused.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s (PG had s.t) ...; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-PARSE-PREFIX error-text [wire]: 42P02 both; ours prefixes 'Parse error: '"),
        (5, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of LIMIT $2147483647: PG refuses 42P02 at Parse; ours ParseComplete then 42P02 with prefix"),
        (7, "PGDIV-PARSE-PREFIX error-text [wire]: 42P02 both; ours prefixes 'Parse error: '"),
        (8, "PGDIV-PARAM-OVER-LIMIT-CODE error-code [wire]: Describe of id = $65536: PG refuses at Parse 42P18 'could not determine data type of parameter $1'; ours ParseComplete then 42P02 there is no parameter $65536"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_parameter_number_the_scanner_would_wrap_is_refused_replays_as_postgresql_answered,
    "pg18-audit3-a_parameter_number_the_scanner_would_wrap_is_refused.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: $4294967297 over Parse/Bind/Execute: PG refuses at Parse; ours ParseComplete+BindComplete then 42601 with 'Parse error: ' prefix"),
        (5, "PGDIV-TURSO-FUNCTIONS error-code [ours-only]: turso_branch_switch('x'): PG 42883 function does not exist (PG has no turso_* functions); ours 3D000 branch \"x\" does not exist"),
        (6, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (7, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: $4294967297 over Parse/Bind/Execute: PG refuses at Parse; ours ParseComplete+BindComplete then 42601"),
        (9, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (10, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: LIMIT $2147483648 over Parse/Bind/Execute: PG refuses at Parse; ours ParseComplete+BindComplete then 42601"),
        (12, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (13, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: $4295032831 over Parse/Bind/Execute: PG refuses at Parse; ours ParseComplete+BindComplete then 42601"),
    ]
);
replays_postgresql!(
    pg18_audit3_a_view_over_public_t_types_its_parameters_replays_as_postgresql_answered,
    "pg18-audit3-a_view_over_public_t_types_its_parameters.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
    ]
);
replays_postgresql!(
    pg18_audit3_an_explicit_public_qualifier_names_public_replays_as_postgresql_answered,
    "pg18-audit3-an_explicit_public_qualifier_names_public.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (3, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: CREATE SCHEMA s refused 0A000 (recording relies on schema s)"),
        (4, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE TABLE s.t: XX000 no such database: s (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (5, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT INTO s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (11, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT ... FROM s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (15, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: SELECT * FROM public.t, 42P01 both; ours 'Parse error: no such table: main.t' (leaks SQLite schema name main) vs PG 'relation \"public.t\" does not exist'"),
        (16, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT count(*) FROM s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit3_an_index_on_a_qualified_table_is_that_tables_replays_as_postgresql_answered,
    "pg18-audit3-an_index_on_a_qualified_table_is_that_tables.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s (PG had s.t); DROP SCHEMA public: ours refuses the whole query 0A000"),
        (3, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: CREATE SCHEMA s refused 0A000 (recording relies on schema s)"),
        (4, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE TABLE s.t: XX000 no such database: s (cascade of #3; XX000 is not PG's 3F000); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (5, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE UNIQUE INDEX su ON s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (6, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT INTO s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (7, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: INSERT INTO s.t duplicate: XX000 instead of PG 23505 on su (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (12, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.c' vs PG '... unique constraint \"pu\"'"),
        (13, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: SELECT count(*) FROM s.t: XX000 (cascade of #3); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
    ]
);
replays_postgresql!(
    pg18_audit3_an_undeclared_parameter_has_its_inferred_type_at_describe_and_bind_replays_as_postgresql_answered,
    "pg18-audit3-an_undeclared_parameter_has_its_inferred_type_at_describe_and_bind.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s (PG had s.t); DROP SCHEMA public: ours refuses the whole query 0A000"),
        (20, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of id = $2 (no $1): PG refuses 42P18 at Parse; ours ParseComplete then the same 42P18"),
        (25, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Parse SELECT 1 with one declared-0 param: PG refuses 42P18 at Parse; ours ParseComplete then the same 42P18"),
    ]
);
replays_postgresql!(
    pg18_audit3_an_unqualified_relation_is_resolved_as_the_engine_resolves_it_replays_as_postgresql_answered,
    "pg18-audit3-an_unqualified_relation_is_resolved_as_the_engine_resolves_it.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (4, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: CREATE SCHEMA s refused 0A000 (recording relies on schema s)"),
        (5, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE TABLE s.u: XX000 no such database: s (cascade of #4); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (6, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe UPDATE u (u only in s): PG refuses 42P01 at Parse 'relation \"u\" does not exist'; ours ParseComplete then 'Parse error: no such table: u'"),
    ]
);
replays_postgresql!(
    pg18_audit3_copy_from_runs_inside_the_block_it_is_in_replays_as_postgresql_answered,
    "pg18-audit3-copy_from_runs_inside_the_block_it_is_in.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (15, "PGDIV-COPY-INPUT-CODE error-code [wire]: COPY of a non-integer into INT: PG 22P02 invalid input syntax for type integer: \"not-a-number\"; ours 23000 'cannot store TEXT value in INTEGER column cp.id (19)'"),
    ]
);
replays_postgresql!(
    pg18_audit3_copy_refuses_an_option_it_does_not_implement_replays_as_postgresql_answered,
    "pg18-audit3-copy_refuses_an_option_it_does_not_implement.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (3, "PGDIV-COPY-CSV-OPTION-TEXT error-text [wire]: COPY (QUOTE) in text mode 0A000 both; ours 'COPY option \"quote\" is not supported' vs PG 'COPY QUOTE requires CSV mode'"),
        (5, "PGDIV-COPY-CSV-OPTION-TEXT error-text [wire]: COPY (FORCE_NOT_NULL) in text mode 0A000 both; ours 'option \"force_not_null\" is not supported' vs PG 'requires CSV mode'"),
        (7, "PGDIV-COPY-OPTION-REFUSED ours-only [ours-only]: COPY ... (DEFAULT 'x') refused 0A000; PG COPY 2"),
        (8, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 2 (cascade of #7)"),
        (9, "PGDIV-COPY-OPTION-REFUSED ours-only [ours-only]: COPY ... (ON_ERROR ignore) refused 0A000; PG COPY 2"),
        (10, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 4 (cascade of #7 and #9)"),
        (11, "PGDIV-COPY-OPTION-REFUSED ours-only [ours-only]: COPY ... (ENCODING 'LATIN1') refused 0A000; PG COPY 2"),
        (12, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 6 (cascade of #7, #9, #11)"),
        (13, "PGDIV-COPY-OPTION-REFUSED ours-only [ours-only]: COPY ... (HEADER MATCH) refused 0A000; PG 22P04 header mismatch (the file has no header)"),
        (14, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 6 (cascade of #7, #9, #11)"),
        (15, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (16, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 6 (cascade of #7, #9, #11)"),
        (17, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (18, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 0 vs 6 (cascade of #7, #9, #11)"),
        (20, "PGDIV-COPY-OPTION-REFUSED cascade [ours-only]: count 2 vs 8: #19 loaded 2 rows on both; the 6 missing are #7/#9/#11 (cascade)"),
    ]
);
replays_postgresql!(
    pg18_audit3_engine_errors_carry_postgres_sqlstates_replays_as_postgresql_answered,
    "pg18-audit3-engine_errors_carry_postgres_sqlstates.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (8, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23505 both; ours 'UNIQUE constraint failed: t.id' vs PG t_pkey message"),
        (9, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23503 both; ours 'FOREIGN KEY constraint failed' vs PG c_pid_fkey message"),
        (10, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23502 both; ours 'NOT NULL constraint failed: n.v' vs PG not-null message"),
        (11, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23514 both; ours 'CHECK constraint failed: v > 0' vs PG 'new row for relation \"k\" violates check constraint \"k_v_check\"'"),
        (12, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42P01 both; ours 'Parse error: no such table: nope' vs PG 'relation \"nope\" does not exist'"),
        (13, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42703 both; ours 'Parse error: no such column: nope' vs PG 'column \"nope\" does not exist'"),
        (14, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: Invalid statement: '"),
        (15, "PGDIV-WITH-ORDINALITY ours-only [ours-only]: generate_series WITH ORDINALITY refused 0A000"),
    ]
);
replays_postgresql!(
    pg18_audit3_every_parameter_in_the_text_is_a_parameter_replays_as_postgresql_answered,
    "pg18-audit3-every_parameter_in_the_text_is_a_parameter.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
    ]
);
replays_postgresql!(
    pg18_audit3_having_without_group_by_filters_the_aggregate_row_replays_as_postgresql_answered,
    "pg18-audit3-having_without_group_by_filters_the_aggregate_row.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
    ]
);
replays_postgresql!(
    pg18_audit3_integer_division_by_zero_is_22012_replays_as_postgresql_answered,
    "pg18-audit3-integer_division_by_zero_is_22012.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (2, "PGDIV-DIV-ZERO-NULL value [engine]: SILENT: SELECT 1/0 answers one NULL row (int4); PG 22012 division by zero"),
    ]
);
replays_postgresql!(
    pg18_audit3_integer_modulo_by_zero_is_22012_replays_as_postgresql_answered,
    "pg18-audit3-integer_modulo_by_zero_is_22012.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (2, "PGDIV-DIV-ZERO-NULL value [engine]: SILENT: SELECT 1 % 0 answers one NULL row (int4); PG 22012 division by zero"),
    ]
);
replays_postgresql!(
    pg18_audit3_primary_key_columns_are_reported_not_null_replays_as_postgresql_answered,
    "pg18-audit3-primary_key_columns_are_reported_not_null.out",
    [
        (1, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: DROP SCHEMA IF EXISTS s; DROP SCHEMA public: ours refuses the whole query 0A000"),
        (5, "PGDIV-CONSTRAINT-MESSAGE error-text [wire]: 23502 both; ours 'NOT NULL constraint failed: pk2.a (19)' vs PG 'null value in column \"a\" of relation \"pk2\" violates not-null constraint'"),
        (6, "PGDIV-CATALOG-NAME-TYPES value [wire]: information_schema.columns: table_name/column_name typed 25 (PG 19), is_nullable 25 (PG 1043); rows agree"),
        (7, "PGDIV-BOOL-TEXT value [wire]: SILENT: pg_attribute.attnotnull typed 23 int answering '1'/'0' where PG 16 bool 't'/'f'; relname/attname 25 vs PG 19"),
    ]
);
replays_postgresql!(
    pg18_bytea_replays_as_postgresql_answered,
    "pg18-bytea.out",
    [
        (5, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param a\\qb: PG refuses 22P02 at Bind; ours BindComplete+NoData first (same text)"),
        (6, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param a\\1b: PG refuses 22P02 at Bind; ours BindComplete+NoData first"),
        (7, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: bytea param a\\400b: PG refuses 22P02 at Bind; ours BindComplete+NoData first"),
    ]
);
replays_postgresql!(
    pg18_c33_engineerr_replays_as_postgresql_answered,
    "pg18-c33-engineerr.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS big: ours omits PG's NOTICE"),
        (2, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS t: ours omits PG's NOTICE"),
        (7, "PGDIV-SUM-INT8-NUMERIC value [engine]: sum(bigint) past int8: ours 22003 'integer overflow'; PG answers numeric 9223372036854775808"),
        (8, "PGDIV-INT-RANGE value [engine]: SILENT: v + 1 past int8 answers '9.223372036854776e+18' typed 23; PG 20 then 22003"),
        (9, "PGDIV-INT-RANGE value [engine]: SILENT: v * 2 answers '1.8446744073709552e+19' typed 23; PG 20 then 22003"),
        (10, "PGDIV-INT-RANGE value [engine]: SILENT: abs(-9223372036854775808::int8) answers 9223372036854775807 typed 25; PG 22003"),
        (11, "PGDIV-INT-RANGE value [engine]: SILENT: abs(v - max - max - 2) answers '9.223372036854776e+18' typed 25; PG 20 then 22003"),
        (14, "PGDIV-INT-RANGE value [engine]: SILENT: v + 1 overflow inside a block answers a float row and leaves the block open (Z T); PG 22003 aborts it (Z E)"),
        (15, "PGDIV-INT-RANGE cascade [engine]: INSERT (3) succeeds; PG 25P02 because #14 aborted its block (cascade of #14)"),
        (16, "PGDIV-INT-RANGE cascade [engine]: COMMIT commits rows 2 and 3; PG answers ROLLBACK (cascade of #14)"),
        (17, "PGDIV-INT-RANGE cascade [engine]: count 2 vs 0: rows 2,3 committed because #14 did not abort (cascade of #14)"),
        (19, "PGDIV-INT-RANGE cascade [engine]: INSERT (2) 23505 because #16 committed row 2 (cascade of #14)"),
        (20, "PGDIV-INT-RANGE cascade [engine]: 25P02 because #19 aborted our block (cascade of #14); without it this would be the #10 silent abs"),
        (23, "PGDIV-INT-RANGE cascade [engine]: count 2 vs 0 (cascade of #14)"),
        (25, "PGDIV-INT-RANGE cascade [engine]: INSERT (2) 23505 duplicate (cascade of #14)"),
        (26, "PGDIV-INT-RANGE cascade [engine]: SAVEPOINT s refused 25P02 because #25 aborted our block (cascade of #14)"),
        (27, "PGDIV-INT-RANGE cascade [engine]: 25P02 in our aborted block (cascade of #14)"),
        (28, "PGDIV-INT-RANGE cascade [engine]: ROLLBACK TO s: 3B001 no such savepoint since #26 made none (cascade of #14)"),
        (29, "PGDIV-INT-RANGE cascade [engine]: SELECT 1: 25P02 in our still-aborted block (cascade of #14)"),
        (30, "PGDIV-INT-RANGE cascade [engine]: COMMIT answers ROLLBACK where PG commits (cascade of #14)"),
    ]
);
replays_postgresql!(
    pg18_c33_replays_as_postgresql_answered,
    "pg18-c33.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: drop table if exists pd: ours omits PG's NOTICE"),
        (13, "PGDIV-NUMERIC-AS-FLOAT value [engine]: SILENT: 0x_7FFF_FFFF_FFFF_FFFF_F answers '1.4757395258967641e+20' typed 701; PG exact 147573952589676412927 typed 1700"),
        (17, "PGDIV-NUMERIC-AS-FLOAT value [engine]: SILENT: SELECT 9223372036854775808 answers '9.223372036854776e+18' typed 701; PG exact numeric 9223372036854775808"),
        (19, "PGDIV-NEG-ZERO value [engine]: SILENT: SELECT -0.0 answers '-0' typed 701; PG numeric '0.0'"),
        (20, "PGDIV-NEG-ZERO value [engine]: SILENT: SELECT -0.0::float8 answers '0'; PG '-0' (cast binds before unary minus)"),
        (22, "PGDIV-NEG-ZERO value [engine]: SILENT: '-0'::numeric answers '-0' typed 701; PG '0' typed 1700"),
        (23, "PGDIV-DROP-NOTICE protocol [wire]: drop table if exists rr: ours omits PG's NOTICE"),
        (25, "PGDIV-REAL-RANGE value [engine]: SILENT: insert 1e39 into a real column succeeds (stored as Infinity); PG 22003 out of range for type real"),
        (26, "PGDIV-REAL-RANGE value [engine]: SILENT: insert '1e39' into real succeeds (Infinity); PG 22003"),
        (27, "PGDIV-REAL-RANGE value [engine]: SILENT: insert -1e39 into real succeeds (-Infinity); PG 22003"),
        (28, "PGDIV-REAL-RANGE value [engine]: SILENT: insert 1e-50 into real succeeds (stored 0); PG 22003 underflow"),
        (29, "PGDIV-REAL-RANGE cascade [engine]: select * from rr returns Infinity, Infinity, -Infinity, 0 where PG has 0 rows (cascade of #25-#28)"),
        (30, "PGDIV-REAL-RANGE value [engine]: SILENT: CAST(1e39 AS real) answers 'Infinity'; PG 22003"),
        (31, "PGDIV-REAL-RANGE value [engine]: SILENT: 1e39::real answers 'Infinity'; PG 22003"),
        (32, "PGDIV-PARSE-PREFIX error-text [wire]: '1e39'::real 22003 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_colnames_replays_as_postgresql_answered,
    "pg18-colnames.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS of a missing table: ours omits PG's NOTICE 'table \"cn\" does not exist, skipping'"),
        (4, "PGDIV-NAN ours-only [ours-only]: 'nan'::numeric refused 0A000 (engine holds no NaN); PG answers the whole row"),
        (5, "PGDIV-EXPR-TYPE-TEXT value [wire]: CASE WHEN true THEN a ELSE a END typed 25 text; PG 23 int4 (values agree)"),
        (6, "PGDIV-ROW-CONSTRUCTOR value [wire]: SELECT ARRAY[1], ROW(1, 2), EXISTS ..., ...: ours errors 42601 'row value misused' where PG answers one row"),
        (7, "PGDIV-NUMERIC-OID-FLOAT8 value [wire]: numeric literal 1.5 typed 701 float8; PG 1700 numeric (text agrees)"),
        (8, "PGDIV-EXPR-TYPE-TEXT value [wire]: 'x'::varchar(3) typed 25 text; PG 1043 varchar"),
    ]
);
replays_postgresql!(
    pg18_derivedbool_replays_as_postgresql_answered,
    "pg18-derivedbool.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS parent, child: ours omits PG's two NOTICEs"),
    ]
);
replays_postgresql!(
    pg18_float_replays_as_postgresql_answered,
    "pg18-float.out",
    [
        (21, "PGDIV-REAL-RANGE value [engine]: SILENT: INSERT of 1.7976931348623157e308 into a real column succeeds (stored Infinity); PG 22003 out of range for type real"),
        (22, "PGDIV-REAL-RANGE value [engine]: SILENT: INSERT of 5e-324 into a real column succeeds (stored 0); PG 22003 out of range for type real"),
        (27, "PGDIV-NEG-ZERO value [engine]: SILENT: row 25 (-0.0 numeric literal) reads '-0','-0' where PG stores '0','0'; extra rows 20 and 21 are a cascade of #21/#22"),
    ]
);
replays_postgresql!(
    pg18_gs_replays_as_postgresql_answered,
    "pg18-gs.out",
    [
        (7, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42702 both; ours 'Parse error: ambiguous column name: g' vs PG 'column reference \"g\" is ambiguous'"),
        (8, "PGDIV-EXPR-TYPE-TEXT value [wire]: scalar subquery (SELECT g FROM u) typed 25 text; PG 23 int4 (rows agree)"),
        (9, "PGDIV-EXPR-TYPE-TEXT value [wire]: correlated scalar subquery reading the function alias g typed 25 text; PG 23 (rows agree)"),
        (10, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42702 both; ours 'Parse error: ambiguous column name: g'"),
    ]
);
replays_postgresql!(
    pg18_gsinf_replays_as_postgresql_answered,
    "pg18-gsinf.out",
    [
        (1, "PGDIV-ERROR-BEFORE-ROWDESC protocol [wire]: generate_series(numeric, 'inf'): PG sends RowDescription 1700 then 22023; ours only the error, prefixed 'Extension error: '"),
        (2, "PGDIV-GS-MIXED-TYPES error-code [wire]: generate_series(int, float8, int): PG 42883 function does not exist; ours 22023 'Extension error: stop value cannot be infinity' (accepts a mixed-type call PG refuses)"),
        (3, "PGDIV-ERROR-BEFORE-ROWDESC protocol [wire]: generate_series step 'inf'::numeric: PG RowDescription 1700 then 22023; ours only the error, prefixed 'Extension error: '"),
        (4, "PGDIV-ERROR-BEFORE-ROWDESC protocol [wire]: generate_series start '-inf'::numeric: PG RowDescription 1700 then 22023; ours only the error, prefixed 'Extension error: '"),
    ]
);
replays_postgresql!(
    pg18_gsstar_replays_as_postgresql_answered,
    "pg18-gsstar.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS gt: ours omits PG's NOTICE 'does not exist, skipping'"),
    ]
);
replays_postgresql!(
    pg18_ident_replays_as_postgresql_answered,
    "pg18-ident.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS idt, idb, gv: ours omits PG's three NOTICEs"),
        (6, "PGDIV-VIRTUAL-GENERATED value [wire]: create table with b ... GENERATED ALWAYS AS (a * 2) VIRTUAL: ours errors 42601 syntax error at \"virtual\" where PG creates it (a refusal should be 0A000)"),
        (7, "PGDIV-VIRTUAL-GENERATED cascade [wire]: insert into gv: 42P01 no such table (cascade of #6)"),
        (10, "PGDIV-VIRTUAL-GENERATED cascade [wire]: select * from gv: 42P01 no such table (cascade of #6)"),
    ]
);
replays_postgresql!(
    pg18_inf_replays_as_postgresql_answered,
    "pg18-inf.out",
    [
        (1, "PGDIV-NAN ours-only [ours-only]: SELECT with 'NaN'::float8 refused 0A000; PG answers Infinity, -Infinity, NaN, Infinity, -0"),
    ]
);
replays_postgresql!(
    pg18_infcast_replays_as_postgresql_answered,
    "pg18-infcast.out",
    [
        (2, "PGDIV-NAN ours-only [ours-only]: 'NaN'::float8 refused 0A000 (engine holds no NaN); PG answers t, t"),
        (3, "PGDIV-PARSE-PREFIX error-text [wire]: 22P02 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_inherits_replays_as_postgresql_answered,
    "pg18-inherits.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS ich, ipa: ours omits PG's two NOTICEs 'does not exist, skipping'"),
        (3, "PGDIV-INHERITS ours-only [ours-only]: CREATE TABLE ... INHERITS refused 0A000"),
        (4, "PGDIV-INHERITS cascade [ours-only]: INSERT INTO ich: 42P01 no such table (cascade of #3)"),
        (5, "PGDIV-INHERITS cascade [ours-only]: SELECT FROM ich: 42P01 (cascade of #3)"),
        (6, "PGDIV-INHERITS cascade [ours-only]: SELECT FROM ipa: 0 rows vs PG's inherited row (cascade of #3)"),
        (7, "PGDIV-INHERITS cascade [ours-only]: DROP TABLE ich, ipa: 42P01 no such table ich (cascade of #3)"),
    ]
);
replays_postgresql!(
    pg18_insdef_replays_as_postgresql_answered,
    "pg18-insdef.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS it: ours omits PG's NOTICE"),
        (3, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (4, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
        (8, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_join1_replays_as_postgresql_answered,
    "pg18-join1.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS j1, j2: ours omits PG's two NOTICEs"),
    ]
);
replays_postgresql!(
    pg18_join1b_replays_as_postgresql_answered,
    "pg18-join1b.out",
    [
        (2, "PGDIV-PARSE-PREFIX error-text [wire]: 42P10 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_join2_replays_as_postgresql_answered,
    "pg18-join2.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS j1, j2: ours omits PG's two NOTICEs"),
        (8, "PGDIV-FULL-JOIN-USING value [wire]: FULL JOIN ... USING (i): ours errors 42601 'FULL OUTER JOIN requires an equality condition in the ON clause' where PG answers 4 rows"),
        (10, "PGDIV-FULL-JOIN-USING value [wire]: SELECT i FROM j1 FULL JOIN j2 USING (i): ours errors 42601 where PG answers 4 rows"),
        (12, "PGDIV-FULL-JOIN-USING value [wire]: NATURAL FULL JOIN: ours errors 42601 where PG answers 4 rows"),
    ]
);
replays_postgresql!(
    pg18_nullorder_replays_as_postgresql_answered,
    "pg18-nullorder.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: drop table if exists nt: ours omits PG's NOTICE"),
        (9, "PGDIV-EXPR-TYPE-TEXT value [wire]: row_number() OVER typed 25 text; PG 20 int8 (rows agree)"),
    ]
);
replays_postgresql!(
    pg18_num_replays_as_postgresql_answered,
    "pg18-num.out",
    [
        (4, "PGDIV-NUMERIC-AS-FLOAT value [engine]: SILENT: SELECT 0x_7FFF_FFFF_FFFF_FFFF_F answers float '1.4757395258967641e+20' typed 701; PG exact 147573952589676412927 numeric"),
    ]
);
replays_postgresql!(
    pg18_r14_replays_as_postgresql_answered,
    "pg18-r14.out",
    [
        (10, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42P01 both; ours 'Parse error: no such table: nosuch' vs PG 'relation \"nosuch\" does not exist'"),
    ]
);
replays_postgresql!(
    pg18_r15_replays_as_postgresql_answered,
    "pg18-r15.out",
    [
        (27, "PGDIV-SQLITE-NAME-MESSAGE error-text [wire]: 42P01 both; ours 'Parse error: no such table: nosuch' vs PG 'relation \"nosuch\" does not exist'"),
        (33, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: text[] param {\"NUL\\L\"} comes back as blob '\\\\x02154e554c4c' instead of '{\"NULL\"}'"),
        (34, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: text[] param {NULL} comes back as blob '\\\\x0200' instead of '{NULL}'"),
        (35, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: malformed text[] param {a}}: PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
        (36, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: int[] param {1,x}: PG refuses 22P02 at Bind; ours BindComplete+RowDescription first"),
        (37, "PGDIV-ARRAY-RESULT-BLOB value [wire]: SILENT: text[] param {a,b} comes back as blob '\\\\x030f0f6162' instead of '{a,b}'"),
    ]
);
replays_postgresql!(
    pg18_random_replays_as_postgresql_answered,
    "pg18-random.out",
    [
        (1, "PGDIV-PG-TYPEOF value [wire]: pg_typeof(random()): ours 42883 'no such function: pg_typeof' where PG answers t, 'double precision'"),
        (2, "PGDIV-RANDOM-VALUE harness [harness]: SELECT random() compares a random value ('0.14257649920542403' vs PG's '0.812971034057977') that can never agree; oid 701 agrees"),
    ]
);
replays_postgresql!(
    pg18_short_replays_as_postgresql_answered,
    "pg18-short.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS s3: ours omits PG's NOTICE"),
        (6, "PGDIV-PARSE-PREFIX error-text [wire]: 42601 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_tablestar_replays_as_postgresql_answered,
    "pg18-tablestar.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS parent, child: ours omits PG's two NOTICEs"),
    ]
);
replays_postgresql!(
    pg18_types_replays_as_postgresql_answered,
    "pg18-types.out",
    []
);
replays_postgresql!(
    pg18_replays_as_postgresql_answered,
    "pg18.out",
    [
        (6, "PGDIV-SCHEMA-REFUSED refusal [ours-only]: CREATE SCHEMA s refused 0A000 (recording relies on schema s)"),
        (7, "PGDIV-MISSING-SCHEMA-CODE cascade [wire]: CREATE TABLE s.t: XX000 no such database: s (cascade of #6); missing-schema XX000 rather than 3F000 is a frontend defect (C56)"),
        (9, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of id = $2 (no $1): PG refuses 42P18 at Parse; ours ParseComplete then the same error"),
        (14, "PGDIV-SUM-INT8-NUMERIC value [engine]: sum(bigint) past int8 inside a savepoint block: ours 22003 'integer overflow' and aborts (Z E); PG answers numeric 9223372036854775808 (Z T)"),
        (15, "PGDIV-SUM-INT8-NUMERIC cascade [engine]: SELECT 1/0: ours 25P02 because #14 aborted the block; PG 22012 (cascade of #14)"),
        (20, "PGDIV-PARSE-PREFIX error-text [wire]: COPY ... WHERE v = $1 over Q: 42P02 both; ours prefixes 'Parse error: '"),
        (21, "PGDIV-PARSE-PREFIX error-text [wire]: COPY ... WHERE v = $1 extended: message sequence agrees (Parse/Bind/NoData then 42P02); ours prefixes 'Parse error: '"),
        (22, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: COPY ... WHERE v = $1 with param type 0: PG refuses 42P18 at Parse; ours ParseComplete then the same error"),
        (25, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: s.t.c with only t in FROM: PG refuses at Parse 'invalid reference to FROM-clause entry'; ours ParseComplete then 'Parse error: missing FROM-clause entry' (42P01 both)"),
        (26, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: $1::float8 param 1e-400: PG refuses at Bind (only ParseComplete); ours BindComplete+RowDescription then 22003 with 'value ' prefix"),
        (27, "PGDIV-BIND-REFUSAL-STEP protocol [wire]: $1::float4 param 1e-50: PG refuses at Bind; ours BindComplete+RowDescription then 22003 with 'value ' prefix"),
        (31, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: Describe of SELECT varchar $1: PG refuses 42601 at Parse; ours ParseComplete then 'Parse error: syntax error'"),
        (32, "PGDIV-PARSE-REFUSAL-STEP protocol [wire]: SELECT varchar $1 over Parse/Bind/Execute: PG refuses at Parse; ours ParseComplete+BindComplete then 42601 with prefix"),
        (37, "PGDIV-BOOL-TEXT value [wire]: SILENT: EXISTS (SELECT 1 FROM k WHERE k.flag) in DELETE RETURNING answers '1' typed 25; PG 't' typed 16"),
        (39, "PGDIV-BOOL-TEXT value [wire]: SILENT: EXISTS (... AS j WHERE flag) in DELETE RETURNING answers '1' typed 25; PG 't' typed 16"),
        (41, "PGDIV-BOOL-TEXT value [wire]: SILENT: EXISTS (SELECT * FROM k) in DELETE RETURNING answers '1' typed 25; PG 't' typed 16"),
        (42, "PGDIV-PARSE-PREFIX error-text [wire]: 42702 both; ours prefixes 'Parse error: '"),
    ]
);
replays_postgresql!(
    pg18_c33_intrange_replays_as_postgresql_answered,
    "pg18-c33-intrange.out",
    [
        (1, "PGDIV-DROP-NOTICE protocol [wire]: DROP TABLE IF EXISTS of a missing table: ours omits PG's NOTICE 00000 'table \"big\" does not exist, skipping'"),
        (4, "PGDIV-INT-RANGE value [engine]: SILENT: -9223372036854775808::int8 (cast binds before minus, so 2^63 overflows) answers -9223372036854775807 typed 23; PG 22003 bigint out of range"),
        (5, "PGDIV-INT-RANGE value [engine]: SILENT: abs(-9223372036854775808::int8) answers 9223372036854775807 typed 25; PG 22003"),
        (6, "PGDIV-INT-RANGE value [engine]: SILENT: '9223372036854775808'::int8 clamps to 9223372036854775807 typed 23; PG 22003 out of range for type bigint"),
        (7, "PGDIV-INT-RANGE value [engine]: SILENT: 9223372036854775807 + 1 answers float '9.223372036854776e+18' typed 23; PG 22003 bigint out of range"),
        (8, "PGDIV-INT-RANGE value [engine]: SILENT: bigint column v + 1 answers '9.223372036854776e+18' typed 23; PG RowDescription 20 then 22003"),
        (9, "PGDIV-INT-RANGE value [engine]: SILENT: v * 2 answers '1.8446744073709552e+19' typed 23; PG 20 then 22003"),
        (10, "PGDIV-INT-RANGE value [engine]: SILENT: -v - 2 answers '-9.223372036854776e+18' typed 23; PG 20 then 22003"),
        (11, "PGDIV-INT-RANGE value [engine]: SILENT: 2147483647::int4 + 1 answers 2147483648 typed 23; PG 22003 integer out of range"),
        (12, "PGDIV-INT-RANGE value [engine]: SILENT: int column i + 1 answers 2147483648; PG RowDescription 23 then 22003"),
        (13, "PGDIV-INT-RANGE value [engine]: SILENT: i * 2 answers 4294967294; PG 23 then 22003"),
        (14, "PGDIV-INT-RANGE value [engine]: SILENT: '2147483648'::int4 answers 2147483648; PG 22003 out of range for type integer"),
        (15, "PGDIV-INT-RANGE value [engine]: SILENT: 32767::int2 + 1::int2 answers 32768 typed 23; PG 22003 smallint out of range"),
        (16, "PGDIV-INT-RANGE value [engine]: SILENT: CAST(9.5e18 AS BIGINT) clamps to 9223372036854775807; PG 22003 bigint out of range"),
        (17, "PGDIV-INT-RANGE value [engine]: SILENT: CAST(3e9 AS INT) answers 3000000000; PG 22003 integer out of range"),
        (18, "PGDIV-INT-RANGE value [engine]: SILENT: INSERT of 2147483648 into an INT column succeeds; PG 22003 integer out of range"),
        (19, "PGDIV-INT-RANGE cascade [engine]: sum(i) 4294967296 vs 2147483648 because #18 stored the out-of-range row (cascade of #18)"),
        (20, "PGDIV-SUM-INT8-NUMERIC value [engine]: sum(bigint) past int8: ours 22003 'integer overflow'; PG sum(int8) is numeric and answers 9223372036854775808 typed 1700"),
    ]
);

// PG 18.6 card-row recordings: c44-pg18-errors.txt beside these tests.
#[test]
fn c44_numeric_overflow_is_22003() {
    let dir = Scratch::new("c44-numeric-overflow");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE c44_num(x numeric(10,2))").ok("typed numeric column");
    a.q("INSERT INTO c44_num VALUES (100)").ok("numeric operand");
    let sql = "SELECT '1e131072' / x FROM c44_num";
    assert_eq!(a.q(sql).err(sql).code, "22003");
}

#[test]
fn c44_missing_referenced_primary_key_is_42704() {
    let dir = Scratch::new("c44-reference-key");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "CREATE TABLE c44_n(id INT, up INT REFERENCES c44_n)";
    assert_eq!(a.q(sql).err(sql).code, "42704");
}

#[test]
fn c44_lock_timeout_above_int32_is_22023() {
    let dir = Scratch::new("c44-timeout-int32");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "SET lock_timeout=2147483648";
    assert_eq!(a.q(sql).err(sql).code, "22023");
}

#[test]
fn c44_lock_timeout_u64_max_is_22023() {
    let dir = Scratch::new("c44-timeout-u64");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    let sql = "SET lock_timeout='18446744073709551615ms'";
    assert_eq!(a.q(sql).err(sql).code, "22023");
}

#[test]
fn c44_lock_timeout_int32_max_is_accepted() {
    let dir = Scratch::new("c44-timeout-boundary");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("SET lock_timeout=2147483647").ok("maximum accepted timeout");
    assert_eq!(a.q("SHOW lock_timeout").single("timeout"), "2147483647ms");
}

// C50: each row is recorded in pg18/pg18-c33-intrange.out.
#[test]
fn c50_int8_cast_before_negation() {
    let dir = Scratch::new("c50-int8_cast_before_negation");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT -9223372036854775808::int8";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int8_min_abs() {
    let dir = Scratch::new("c50-int8_min_abs");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT abs(-9223372036854775808::int8)";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int8_text_cast() {
    let dir = Scratch::new("c50-int8_text_cast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT '9223372036854775808'::int8";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "value \"9223372036854775808\" is out of range for type bigint"), "{sql}");
}

#[test]
fn c50_int8_constant_add() {
    let dir = Scratch::new("c50-int8_constant_add");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT 9223372036854775807 + 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int8_column_add() {
    let dir = Scratch::new("c50-int8_column_add");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT v + 1 FROM big WHERE v > 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int8_column_mul() {
    let dir = Scratch::new("c50-int8_column_mul");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT v * 2 FROM big WHERE v > 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int8_column_neg_sub() {
    let dir = Scratch::new("c50-int8_column_neg_sub");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT -v - 2 FROM big WHERE v > 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int4_constant_add() {
    let dir = Scratch::new("c50-int4_constant_add");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT 2147483647::int4 + 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "integer out of range"), "{sql}");
}

#[test]
fn c50_int4_column_add() {
    let dir = Scratch::new("c50-int4_column_add");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT i + 1 FROM big WHERE v > 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "integer out of range"), "{sql}");
}

#[test]
fn c50_int4_column_mul() {
    let dir = Scratch::new("c50-int4_column_mul");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT i * 2 FROM big WHERE v > 1";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "integer out of range"), "{sql}");
}

#[test]
fn c50_int4_text_cast() {
    let dir = Scratch::new("c50-int4_text_cast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT '2147483648'::int4";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "value \"2147483648\" is out of range for type integer"), "{sql}");
}

#[test]
fn c50_int2_constant_add() {
    let dir = Scratch::new("c50-int2_constant_add");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT 32767::int2 + 1::int2";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "smallint out of range"), "{sql}");
}

#[test]
fn c50_int8_numeric_cast() {
    let dir = Scratch::new("c50-int8_numeric_cast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT CAST(9.5e18 AS BIGINT)";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "bigint out of range"), "{sql}");
}

#[test]
fn c50_int4_numeric_cast() {
    let dir = Scratch::new("c50-int4_numeric_cast");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "SELECT CAST(3e9 AS INT)";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "integer out of range"), "{sql}");
}

#[test]
fn c50_int4_insert() {
    let dir = Scratch::new("c50-int4_insert");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE big(v BIGINT, i INT)").ok("typed operands");
    a.q("INSERT INTO big VALUES (9223372036854775807, 2147483647), (1, 1)")
        .ok("boundary operands");
    let sql = "INSERT INTO big VALUES (1, 2147483648)";
    let reply = a.q(sql);
    let e = reply.err(sql);
    assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", "integer out of range"), "{sql}");
}

// The complete numeric-special row in pg18/pg18-colnames.out, command 4.
#[test]
fn c50_numeric_specials_from_colnames_recording() {
    let dir = Scratch::new("c50-numeric-specials");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cn (a int, b text)").ok("cn");
    a.q("INSERT INTO cn VALUES (1, 'x')").ok("row");
    let sql = "SELECT 1 + 1, 'nan'::numeric / '0', 'Infinity'::float8::numeric, '42'::int2::numeric, a::int8, b::text FROM cn";
    let r = a.q(sql).ok(sql);
    assert_eq!(r.oids, Some(vec![23, 1700, 1700, 1700, 20, 25]));
    assert_eq!(r.rows, vec![vec![Some("2".into()), Some("NaN".into()), Some("Infinity".into()), Some("42".into()), Some("1".into()), Some("x".into())]]);
}
// C52: expectations are selected unchanged from the committed PG 18.6 recordings.
macro_rules! c52_recorded_types {
    ($test:ident, $file:literal, [$($n:literal),+]) => {
        #[test]
        fn $test() {
            recorded::replay_selected($file, include_str!(concat!("pg18/", $file)), &[$($n),+]);
        }
    };
}

c52_recorded_types!(c52_boolean_returning_uses_pg_type, "pg18.out", [33, 34, 35, 36, 37, 38, 39, 40, 41]);
c52_recorded_types!(c52_array_parameters_return_pg_text, "pg18-arrres.out", [1, 2, 3]);
c52_recorded_types!(c52_parameter_description_uses_oid_array, "pg18-arrays.out", [1, 5]);
c52_recorded_types!(c52_derived_count_has_pg_type, "pg18-audit1-ws-untyped_contexts_type_their_parameters.out", [2, 3, 18, 19]);
c52_recorded_types!(c52_expression_columns_have_pg_types, "pg18-colnames.out", [2, 3, 5, 8]);
c52_recorded_types!(c52_numeric_literal_has_pg_type, "pg18-colnames.out", [7]);
c52_recorded_types!(c52_catalog_columns_have_pg_types, "pg18-audit2-pg_indexes_and_pg_database_size_answer.out", [1, 2, 3, 4]);
c52_recorded_types!(c52_pg_typeof_reads_the_inferred_type, "pg18-random.out", [1]);

// C50 class cases recorded unchanged in c50-pg18-class1.txt on PG 18.6.
#[test]
fn c50_class_expression_types_preserve_ranges_and_precision() {
    let dir = Scratch::new("c50-class-types");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (sql, expected) in [
        ("SELECT coalesce(NULL::smallint,32767::integer)+1::smallint", "32768"),
        ("SELECT CAST(CASE WHEN true THEN 0.5 ELSE 0.5 END AS int4)", "1"),
        ("SELECT CAST(9007199254740993.0 + 0 AS bigint)", "9007199254740993"),
        ("SELECT '32766.5'::numeric::int2", "32767"),
        ("SELECT '32766.5'::float8::int2", "32766"),
    ] { assert_eq!(a.q(sql).single(sql), expected, "{sql}"); }
}

#[test]
fn c50_class_all_integer_widths_share_checked_operations() {
    let dir = Scratch::new("c50-class-ops");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    for (sql, message) in [
        ("SELECT 2147483647::int4 - (-1)::int4", "integer out of range"),
        ("SELECT 32767::int2 * 2::int2", "smallint out of range"),
        ("SELECT '-32768'::int2 / (-1)::int2", "smallint out of range"),
        ("SELECT '-2147483648'::int4 / (-1)::int4", "integer out of range"),
        ("SELECT '-9223372036854775808'::int8 / (-1)::int8", "bigint out of range"),
        ("SELECT -('-32768'::int2)", "smallint out of range"),
        ("SELECT abs('-32768'::int2)", "smallint out of range"),
        ("SELECT abs('-2147483648'::int4)", "integer out of range"),
        ("SELECT '32767.5'::numeric::int2", "smallint out of range"),
    ] {
        let e = a.q(sql).err(sql);
        assert_eq!((e.code.as_str(), e.message.as_str()), ("22003", message), "{sql}");
    }
}

#[test]
fn c50_class_numeric_assignment_retains_source_kind() {
    let dir = Scratch::new("c50-class-writes");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE class_i(a smallint, b integer, c bigint)").ok("table");
    let r = a.q("INSERT INTO class_i VALUES ('1.5'::numeric, '-1.5'::numeric, '9007199254740993.0'::numeric) RETURNING *").ok("insert");
    assert_eq!(r.rows, vec![vec![Some("2".into()),Some("-2".into()),Some("9007199254740993".into())]]);
    let r = a.q("UPDATE class_i SET a='2.5'::numeric, b='-2.5'::numeric, c=42.5 RETURNING *").ok("update");
    assert_eq!(r.rows, vec![vec![Some("3".into()),Some("-3".into()),Some("43".into())]]);
    for (sql,message) in [
        ("INSERT INTO class_i VALUES ('32768'::numeric,0,0)","smallint out of range"),
        ("UPDATE class_i SET b=2147483648","integer out of range"),
    ] { let e=a.q(sql).err(sql); assert_eq!((e.code.as_str(),e.message.as_str()),("22003",message)); }
}

#[test]
fn c50_class_integer_primary_key_checks_its_width() {
    let dir=Scratch::new("c50-class-key"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE class_pk(a integer PRIMARY KEY)").ok("table");
    let e=a.q("INSERT INTO class_pk VALUES (2147483648)").err("overflow");
    assert_eq!((e.code.as_str(),e.message.as_str()),("22003","integer out of range"));
}

#[test]
fn c50_class_oid_cast_keeps_unsigned_range() {
    let dir=Scratch::new("c50-class-oid"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    assert_eq!(a.q("SELECT '4294967295'::oid").single("oid"),"4294967295");
}

#[test]
fn c50_class_domain_cast_checks_base_width() {
    let dir=Scratch::new("c50-class-domain"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE DOMAIN class_small AS smallint").ok("domain");
    let e=a.q("SELECT '32768'::class_small").err("overflow");
    assert_eq!((e.code.as_str(),e.message.as_str()),("22003","value \"32768\" is out of range for type smallint"));
    assert_eq!(a.q("SELECT '1.5'::numeric::class_small").single("rounded"),"2");
}

#[test]
fn c50_class_numeric_special_cast_refusals() {
    let dir=Scratch::new("c50-class-special-cast"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    for (sql,message) in [
        ("SELECT 'NaN'::numeric::int8","cannot convert NaN to bigint"),
        ("SELECT 'Infinity'::numeric::int4","cannot convert infinity to integer"),
    ] { let e=a.q(sql).err(sql); assert_eq!((e.code.as_str(),e.message.as_str()),("0A000",message)); }
}

#[test]
fn c50_class_numeric_typmod_cast_retains_range_and_rounding() {
    let dir=Scratch::new("c50-class-typmod"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    assert_eq!(a.q("SELECT CAST('123.45' AS numeric(4,1))").single("round"),"123.5");
    let e=a.q("SELECT CAST('123.45' AS numeric(3,1))").err("overflow");
    assert_eq!((e.code.as_str(),e.message.as_str()),("22003","numeric field overflow"));
}

#[test]
fn c50_class_numeric_special_comparisons() {
    let dir=Scratch::new("c50-class-special-order"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    let sql="SELECT '-Infinity'::numeric < 0::numeric, 'Infinity'::numeric < 'NaN'::numeric, 'NaN'::numeric = 'NaN'::numeric";
    assert_eq!(a.q(sql).ok(sql).rows,vec![vec![Some("t".into()),Some("t".into()),Some("t".into())]]);
}

#[test]
fn c50_class_sum_windows_keep_exact_total_and_remove_nan() {
    let dir=Scratch::new("c50-class-sum-window"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    for (sql,expected) in [
        ("SELECT sum(x) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1,9223372036854775807::int8),(2,1),(3,2))t(n,x)",vec!["9223372036854775807","9223372036854775808","3"]),
        ("SELECT sum(x) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES (1,'NaN'::numeric),(2,1),(3,2))t(n,x)",vec!["NaN","NaN","3"]),
    ] { assert_eq!(a.q(sql).ok(sql).rows,expected.into_iter().map(|x|vec![Some(x.to_string())]).collect::<Vec<_>>()); }
}

// Same PG18-recorded operands and answers as the VALUES-window test, using real tables so
// arithmetic also runs independently of derived VALUES alias translation.
#[test]
fn c50_class_sum_windows_on_typed_tables() {
    let dir=Scratch::new("c50-class-sum-table"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE cw(n integer, v bigint)").ok("table");
    a.q("INSERT INTO cw VALUES (1,9223372036854775807),(2,1),(3,2)").ok("rows");
    let r=a.q("SELECT sum(v) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM cw").ok("sum window");
    assert_eq!(r.rows, vec![vec![Some("9223372036854775807".into())],vec![Some("9223372036854775808".into())],vec![Some("3".into())]]);
    a.q("CREATE TABLE cwn(n integer, v numeric)").ok("numeric table");
    a.q("INSERT INTO cwn VALUES (1,'NaN'),(2,1),(3,2)").ok("numeric rows");
    let r=a.q("SELECT sum(v) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM cwn").ok("numeric window");
    assert_eq!(r.rows,vec![vec![Some("NaN".into())],vec![Some("NaN".into())],vec![Some("3".into())]]);
}

/// Types come from the same inference even when Parse declares an argument, and evaluating
/// pg_typeof must retain its argument's side effect exactly once.
#[test]
fn c52_pg_typeof_keeps_declared_types_and_argument_evaluation() {
    let dir = Scratch::new("c52polymorphic");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    for (oid, value, name) in [(23, &b"7"[..], "integer"), (16, &b"true"[..], "boolean")] {
        let reply = w.xt("SELECT pg_typeof($1)::text, $1", &[(oid, 0, value)]).ok("declared type");
        assert_eq!(reply.oids, Some(vec![25, oid]));
        assert_eq!(reply.rows[0][0].as_deref(), Some(name));
    }
    assert_eq!(w.q("SELECT pg_typeof(NULL)::text").single("unknown NULL"), "unknown");
    assert_eq!(w.q("SELECT pg_typeof('literal')::text").single("unknown literal"), "unknown");
    w.q("CREATE SEQUENCE c52_seq").ok("sequence");
    assert_eq!(w.q("SELECT pg_typeof(nextval('c52_seq'))::text").single("side effect type"), "bigint");
    assert_eq!(w.q("SELECT nextval('c52_seq')").single("exactly one previous evaluation"), "2");
}

#[test]
fn c52_common_expression_types_consider_every_arm() {
    let dir = Scratch::new("c52common");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    for sql in [
        "SELECT CASE WHEN true THEN 1::int4 ELSE 2::int8 END",
        "SELECT coalesce(1::int4, 2::int8)",
        "SELECT greatest(1::int4, 2::int8)",
        "SELECT 1::int4 UNION ALL SELECT 2::int8",
        "VALUES (1::int4), (2::int8)",
    ] {
        let reply = w.q(sql).ok(sql);
        assert_eq!(reply.oids, Some(vec![20]), "{sql}");
    }
}


#[test]
fn c52_returning_star_and_merged_join_star_keep_column_alignment() {
    let dir = Scratch::new("c52star");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    w.q("CREATE TABLE a(id INT, flag BOOL)").ok("a");
    w.q("CREATE TABLE b(id INT, txt TEXT)").ok("b");
    let reply = w.q("INSERT INTO a VALUES (1, true) RETURNING *, false").ok("returning star");
    assert_eq!(reply.oids, Some(vec![23, 16, 16]));
    assert_eq!(reply.rows, vec![vec![Some("1".into()), Some("t".into()), Some("f".into())]]);
    w.q("INSERT INTO b VALUES (1, 'b')").ok("b row");
    let reply = w.q("SELECT * FROM a JOIN b USING(id)").ok("merged star");
    assert_eq!(reply.oids, Some(vec![23, 16, 25]));
    let reply = w.q("SELECT a.* FROM a JOIN b USING(id)").ok("qualified star");
    assert_eq!(reply.oids, Some(vec![23, 16]));
}

#[test]
fn c52_pg_typeof_arrays_and_select_bodies_use_the_shared_types() {
    let dir = Scratch::new("c52bodies");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    for (oid, name) in [(1016, "bigint[]"), (1028, "oid[]"), (1005, "smallint[]"), (1015, "character varying[]")] {
        assert_eq!(w.xt("SELECT pg_typeof($1)::text", &[(oid, 0, b"{1,2}")]).single("array type"), name);
    }
    w.q("CREATE VIEW c52_v AS SELECT pg_typeof(1)::text AS t").ok("view body");
    assert_eq!(w.q("SELECT t FROM c52_v").single("view value"), "integer");
    w.q("CREATE TABLE c52_t AS SELECT pg_typeof(1)::text AS t").ok("CTAS body");
    assert_eq!(w.q("SELECT t FROM c52_t").single("CTAS value"), "integer");
}

#[test]
fn c52_described_statement_cache_includes_declared_parameter_types() {
    let dir = Scratch::new("c52cache");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    for (name, oid) in [("a", 23u32), ("b", 25u32)] {
        let mut parse = format!("{name}\0SELECT pg_typeof($1)::text\0").into_bytes();
        parse.extend_from_slice(&1i16.to_be_bytes());
        parse.extend_from_slice(&oid.to_be_bytes());
        w.send(b'P', &parse);
    }
    w.send(b'D', b"Sa\0");
    w.send(b'S', &[]);
    let reply = w.read_reply().ok("describe integer statement");
    assert_eq!(reply.params, Some(vec![23]));
    w.send(b'B', b"\0b\0\0\0\0\x01\0\0\0\x01x\0\0");
    w.send(b'E', &[0, 0, 0, 0, 0]);
    w.send(b'S', &[]);
    assert_eq!(w.read_reply().single("execute distinct text statement"), "text");
}


#[test]
fn c52_database_size_matches_its_documented_file_scope() {
    let dir = Scratch::new("c52size");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    w.q("CREATE TABLE ix(id INT PRIMARY KEY, a INT, b TEXT UNIQUE)").ok("ix");
    w.q("CREATE INDEX ix_a ON ix(a)").ok("index");
    for after_write in [false, true] {
        if after_write {
            w.q("INSERT INTO ix VALUES(1, 1, 'one')").ok("change physical state");
        }
        let reply = w.q("SELECT pg_database_size(current_database())").ok("physical size");
        assert_eq!(reply.oids, Some(vec![20]));
        let bytes: u64 = reply.rows[0][0].as_ref().unwrap().parse().unwrap();
        let main = std::fs::metadata(dir.db()).unwrap().len();
        let wal_path = format!("{}-wal", dir.db().display());
        let wal = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        eprintln!("C52 database size: after_write={after_write}, main={main}, wal={wal}, reported={bytes}");
        assert_eq!(bytes, main + wal);
    }
}

#[test]
fn c52_view_of_integer_constant_retains_its_type() {
    let dir = Scratch::new("c52viewconstant");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    w.q("CREATE VIEW c52_constant AS SELECT 1 AS n").ok("constant view");
    let reply = w.q("SELECT n FROM c52_constant").ok("constant result");
    assert_eq!(reply.oids, Some(vec![23]));
    assert_eq!(reply.rows, vec![vec![Some("1".into())]]);
}

// PG18.6 raw oracle: c50-pg18-class2.txt (54d3c589f9505aec6e0d845e914759b68b46ea605d6c4740b2cbba674772707c).
#[test]
fn c50_class2_unconstrained_numeric_keeps_exact_values_and_specials() {
    let dir=Scratch::new("c50-exact-storage"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE c_exact(v numeric)").ok("table");
    a.q("INSERT INTO c_exact VALUES (9223372036854775808), ('NaN'), ('Infinity')").ok("values");
    let r=a.q("SELECT v, pg_typeof(v) FROM c_exact ORDER BY v").ok("numeric values");
    assert_eq!(r.oids,Some(vec![1700,2206]));
    assert_eq!(r.rows,vec![vec![Some("9223372036854775808".into()),Some("numeric".into())],vec![Some("Infinity".into()),Some("numeric".into())],vec![Some("NaN".into()),Some("numeric".into())]]);
}

#[test]
fn c50_class2_boolean_and_oid_writes_retain_their_types() {
    let dir=Scratch::new("c50-nonnumeric-storage"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE c_bool(v boolean)").ok("bool table");
    a.q("INSERT INTO c_bool VALUES ('true'), ('false')").ok("bool values");
    assert_eq!(a.q("SELECT v FROM c_bool ORDER BY v").ok("bool result").rows,vec![vec![Some("f".into())],vec![Some("t".into())]]);
    a.q("CREATE TABLE c_oid(v oid)").ok("oid table");
    a.q("INSERT INTO c_oid VALUES (4294967295)").ok("oid value");
    assert_eq!(a.q("SELECT v, pg_typeof(v) FROM c_oid").ok("oid result").rows,vec![vec![Some("4294967295".into()),Some("oid".into())]]);
}

#[test]
fn c50_class2_assignment_common_types_follow_the_query_context() {
    let dir=Scratch::new("c50-common-assignment"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE c_common(v integer)").ok("table");
    let r=a.q("INSERT INTO c_common SELECT 0.5::numeric UNION ALL SELECT 0.5::float8 RETURNING v").ok("union common source");
    assert_eq!(r.rows,vec![vec![Some("0".into())],vec![Some("0".into())]]);
    let r=a.q("INSERT INTO c_common VALUES (0.5::numeric), (0.5::float8) RETURNING v").ok("values assignment contexts");
    assert_eq!(r.rows,vec![vec![Some("1".into())],vec![Some("0".into())]]);
    a.q("CREATE TABLE cn_source(x numeric)").ok("source");
    a.q("INSERT INTO cn_source VALUES (0.5),(1.5)").ok("source values");
    a.q("CREATE TABLE cn_dest(x integer)").ok("destination");
    assert_eq!(a.q("INSERT INTO cn_dest SELECT x FROM cn_source ORDER BY x RETURNING x").ok("sorted assignment").rows,vec![vec![Some("1".into())],vec![Some("2".into())]]);
    assert_eq!(a.q("INSERT INTO cn_dest SELECT 0.1::numeric UNION SELECT 0.2::numeric RETURNING x").ok("deduplicate before assignment").rows,vec![vec![Some("0".into())],vec![Some("0".into())]]);
    assert_eq!(a.q("SELECT count(*) FROM cn_dest").single("all distinct source rows stored"),"4");
}

#[test]
fn c50_class2_update_from_and_upsert_retain_numeric_sources() {
    let dir=Scratch::new("c50-update-sources"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TABLE ci(id integer PRIMARY KEY, i smallint)").ok("table");
    a.q("INSERT INTO ci VALUES (1,0)").ok("row");
    assert_eq!(a.q("INSERT INTO ci VALUES (1,'1.5'::numeric) ON CONFLICT(id) DO UPDATE SET i=excluded.i + '1.5'::numeric RETURNING i").single("upsert"),"4");
    assert_eq!(a.q("UPDATE ci SET i=s.n FROM (SELECT '2.5'::numeric AS n) s WHERE id=1 RETURNING i").single("update from"),"3");
}

#[test]
fn c50_class2_omitted_domain_default_and_nullif_width() {
    let dir=Scratch::new("c50-domain-nullif"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE DOMAIN c_dom AS integer DEFAULT 1.5").ok("domain");
    a.q("CREATE TABLE c_default(a integer,b c_dom)").ok("table");
    assert_eq!(a.q("INSERT INTO c_default(a) VALUES(1) RETURNING b").single("inherited default"),"2");
    let e=a.q("SELECT abs(NULLIF('-32768'::int2,1::int8))").err("retained width");
    assert_eq!((e.code.as_str(),e.message.as_str()),("22003","smallint out of range"));
}

#[test]
fn c50_class2_lower_integer_boundaries_and_float_casts() {
    let dir=Scratch::new("c50-lower-bounds"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    for (sql,message) in [
        ("SELECT abs('-9223372036854775808'::int8)","bigint out of range"),
        ("SELECT -('-2147483648'::int4)","integer out of range"),
        ("SELECT -('-9223372036854775808'::int8)","bigint out of range"),
        ("SELECT '-32768'::int2 - 1::int2","smallint out of range"),
        ("SELECT '-2147483648'::int4 - 1::int4","integer out of range"),
        ("SELECT '-9223372036854775808'::int8 - 1::int8","bigint out of range"),
        ("SELECT '32768'::int2","value \"32768\" is out of range for type smallint"),
        ("SELECT '-32769'::int2","value \"-32769\" is out of range for type smallint"),
        ("SELECT '-2147483649'::int4","value \"-2147483649\" is out of range for type integer"),
        ("SELECT '-9223372036854775809'::int8","value \"-9223372036854775809\" is out of range for type bigint"),
        ("SELECT '32768'::float8::int2","smallint out of range"),
        ("SELECT '2147483648'::float8::int4","integer out of range"),
        ("SELECT '9223372036854775808'::float8::int8","bigint out of range"),
    ] { let e=a.q(sql).err(sql); assert_eq!((e.code.as_str(),e.message.as_str()),("22003",message),"{sql}"); }
}

// PG18.6 c50-pg18-class3.txt SHA256 e567ee85cf7dd86d2d8e65f2a96cb4d2cd74ab04fdad3de43b9e3c6c710e2bbc.
#[test]
fn c50_class3_nested_compounds_use_their_common_source_type() {
    let dir=Scratch::new("c50-nested-sources"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE TEMP TABLE c3_int(i integer)").ok("table");
    for sql in [
        "INSERT INTO c3_int SELECT x FROM (SELECT 0.5::numeric AS x UNION ALL SELECT 0.5::float8) q RETURNING i",
        "WITH q AS MATERIALIZED (SELECT 0.5::numeric AS x UNION ALL SELECT 0.5::float8) INSERT INTO c3_int SELECT x FROM q RETURNING i",
    ] { assert_eq!(a.q(sql).ok(sql).rows,vec![vec![Some("0".into())],vec![Some("0".into())]]); }
}

#[test]
fn c50_class3_numeric_domains_and_typmods_return_numeric_values() {
    let dir=Scratch::new("c50-numeric-domain"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    a.q("CREATE DOMAIN c3_num AS numeric").ok("domain");
    for (sql,value) in [
        ("SELECT (1.5::c3_num)::int4","2"),
        ("SELECT ('1.5'::numeric(3,1))::int4","2"),
        ("SELECT '999.4'::numeric(3)","999"),
        ("SELECT ' 1.5 '::numeric::int4","2"),
    ] { assert_eq!(a.q(sql).single(sql),value); }
    let e=a.q("SELECT '999.5'::numeric(3)").err("implicit scale zero overflow");
    assert_eq!((e.code.as_str(),e.message.as_str()),("22003","numeric field overflow"));
}

#[test]
fn c50_class3_float_operations_check_overflow_and_underflow() {
    let dir=Scratch::new("c50-float-ops"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    for (sql,message) in [
        ("SELECT '3.4e38'::float4*2::float4","value out of range: overflow"),
        ("SELECT '1e-30'::float4*'1e-30'::float4","value out of range: underflow"),
        ("SELECT '1e-300'::float8*'1e-300'::float8","value out of range: underflow"),
    ] { let e=a.q(sql).err(sql); assert_eq!((e.code.as_str(),e.message.as_str()),("22003",message),"{sql}"); }
}

#[test]
fn c50_class3_serial_aliases_retain_integer_widths() {
    let dir=Scratch::new("c50-serial-widths"); let server=Server::start(&dir.db(),&[]); let mut a=server.connect();
    for (table,small,big) in [("c3_serial","smallserial","bigserial"),("c3_serial2","serial2","serial8")] {
        a.q(&format!("CREATE TEMP TABLE {table}(s {small}, b {big})")).ok("table");
        let e=a.q(&format!("INSERT INTO {table}(s,b) VALUES (40000,1)")).err("smallserial overflow");
        assert_eq!((e.code.as_str(),e.message.as_str()),("22003","smallint out of range"));
        let r=a.q(&format!("INSERT INTO {table}(s,b) VALUES (1,9223372036854775807) RETURNING s,b,pg_typeof(s),pg_typeof(b)")).ok("bigserial max");
        assert_eq!(r.rows,vec![vec![Some("1".into()),Some("9223372036854775807".into()),Some("smallint".into()),Some("bigint".into())]]);
        assert_eq!(r.oids,Some(vec![21,20,2206,2206]));
    }
}

#[test]
fn c52_derived_merged_join_keeps_named_boolean_columns_and_parameters() {
    let dir = Scratch::new("c52derivedjoined");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    // The key is deliberately not the first left column. Selecting named columns
    // outside the join avoids relying on the engine's existing star-order gap.
    w.q("CREATE TABLE c52_left(payload BOOL, id INT)").ok("left");
    w.q("CREATE TABLE c52_right(id INT, ready BOOL)").ok("right");
    w.q("INSERT INTO c52_left VALUES(true, 1)").ok("left row");
    w.q("INSERT INTO c52_right VALUES(1, false)").ok("right row");
    let sql = "SELECT d.payload,d.id,d.ready FROM (SELECT * FROM c52_left JOIN c52_right USING(id)) d WHERE d.payload=$1";
    let described = w.describe_statement(sql).ok("derived merged columns");
    assert_eq!(described.params, Some(vec![16]));
    assert_eq!(described.oids, Some(vec![16, 23, 16]));
    let reply = w.x_undeclared(sql, &["true"]).ok("boolean parameter");
    assert_eq!(reply.rows, vec![vec![Some("t".into()), Some("1".into()), Some("f".into())]]);
}

#[test]
fn c52_merged_join_key_has_common_type_but_members_keep_their_types() {
    let dir = Scratch::new("c52mergedwidth");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    w.q("CREATE TABLE c52_small(id INT4, flag BOOL)").ok("small");
    w.q("CREATE TABLE c52_big(id INT8, txt TEXT)").ok("big");
    w.q("INSERT INTO c52_small VALUES(1, true)").ok("small row");
    w.q("INSERT INTO c52_big VALUES(1, 'big')").ok("big row");
    let sql = "SELECT id,c52_small.id,c52_big.id,c52_small.*,c52_big.* FROM c52_small JOIN c52_big USING(id) WHERE id=$1";
    let described = w.describe_statement(sql).ok("common merged key");
    assert_eq!(described.params, Some(vec![20]));
    assert_eq!(described.oids, Some(vec![20, 23, 20, 23, 16, 20, 25]));
    let reply = w.x_undeclared(sql, &["1"]).ok("common key parameter");
    assert_eq!(reply.rows, vec![vec![Some("1".into()), Some("1".into()), Some("1".into()), Some("1".into()), Some("t".into()), Some("1".into()), Some("big".into())]]);
}


#[test]
fn c52_common_parameter_contexts_infer_from_the_concrete_arm() {
    let dir = Scratch::new("c52commonparams");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    let mut observed = Vec::new();
    for (sql, oid) in [
        ("SELECT coalesce($1, 1)", 23),
        ("SELECT CASE WHEN true THEN $1 ELSE 1 END", 23),
        ("SELECT greatest($1, 1)", 23),
        ("SELECT ARRAY[$1, 1]", 1007),
        ("VALUES (1), ($1)", 23),
    ] {
        let reply = w.describe_statement(sql);
        observed.push((reply.params, reply.oids));
        eprintln!("C52 common parameter context: {sql}, expected parameter23/result{oid}, observed {:?}", observed.last().unwrap());
    }
    assert_eq!(observed, vec![
        (Some(vec![23]), Some(vec![23])),
        (Some(vec![23]), Some(vec![23])),
        (Some(vec![23]), Some(vec![23])),
        (Some(vec![23]), Some(vec![1007])),
        (Some(vec![23]), Some(vec![23])),
    ]);
}

#[test]
fn c52_nullif_integer_comparison_keeps_the_left_operand_width() {
    let dir = Scratch::new("c52nullifwidth");
    let server = Server::start(&dir.db(), &[]);
    let mut w = server.connect();
    let sql = "SELECT pg_typeof(NULLIF(1::int2,2::int8))::text, NULLIF(1::int2,2::int8)";
    let reply = w.q(sql).ok("integer comparison overload");
    assert_eq!(reply.oids, Some(vec![25, 21]));
    assert_eq!(reply.rows, vec![vec![Some("smallint".into()), Some("1".into())]]);
}
