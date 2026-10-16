use super::*;

fn failed_block_chains(verb: &str) {
    let dir = Scratch::new("c45chain");
    let server = Server::start(&dir.db(), &[]);
    let mut a = seeded(&server);
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO t VALUES (2, 'two')").ok("a write");
    assert_eq!(a.q("SELECT * FROM c45_missing").status, b'E');
    let r = a.x(verb, &[]).ok(verb);
    assert_eq!(r.tags, vec!["ROLLBACK".to_string()]);
    assert_eq!(r.status, b'T', "the replacement block is open");
    assert_eq!(a.q("SELECT 1").single("replacement block"), "1");
    a.q("ROLLBACK").ok("end replacement block");
    assert_eq!(
        a.q("SELECT count(*) FROM t WHERE id = 2")
            .single("rolled back write"),
        "0"
    );
}

#[test]
fn c45_extended_rollback_chain_recovers_a_failed_block() {
    failed_block_chains("ROLLBACK AND CHAIN");
}

#[test]
fn c45_extended_commit_chain_recovers_a_failed_block() {
    failed_block_chains("COMMIT AND CHAIN");
}

#[test]
fn c45_delete_using_an_unnamed_expression_returns_the_target_column() {
    let dir = Scratch::new("c45using");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE d(id INT PRIMARY KEY, v TEXT)").ok("d");
    a.q("INSERT INTO d VALUES (1, 'a'), (2, 'b')").ok("d rows");
    a.q("CREATE TABLE k(id INT)").ok("k");
    a.q("INSERT INTO k VALUES (2)").ok("k row");
    let sql =
        "DELETE FROM d USING (SELECT id + 0 FROM k) s WHERE d.id = s.\"?column?\" RETURNING id";
    let r = a.q(sql).ok(sql);
    assert_eq!(r.rows, vec![vec![Some("2".to_string())]]);
    assert_eq!(r.tags, vec!["DELETE 1".to_string()]);
    assert_eq!(a.q("SELECT id FROM d").single("remaining row"), "1");
}

fn partial_startup_logs_its_timeout(partial: &[u8]) {
    let dir = Scratch::new("c45startuplog");
    let trace = dir.0.join("trace.log");
    let server = Server::start(
        &dir.db(),
        &[
            "--authentication-timeout",
            "1",
            "-t",
            trace.to_str().unwrap(),
        ],
    );
    let mut slow = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
    slow.write_all(partial).unwrap();
    let mut received = Vec::new();
    let _ = slow.read_to_end(&mut received);
    // The server writes its trace through a non-blocking appender, and the harness kills it
    // (SIGKILL) on drop, so a line not yet written is lost: read the trace while the server lives,
    // until the session's end is logged or 5 s pass (FLAGGED TEST FIX at the C45 merge: killed at
    // once, the line was missing in 3 of 6 runs).
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut log = String::new();
    while Instant::now() < deadline {
        log = std::fs::read_to_string(&trace).unwrap_or_default();
        if log.contains("session ended before startup") || log.contains("invalid frontend message") {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(server);
    assert!(
        log.contains("the startup timeout passed"),
        "missing timeout explanation: {log}"
    );
    assert!(
        !log.contains("invalid frontend message"),
        "timeout classified as a malformed message: {log}"
    );
}

#[test]
fn c45_partial_startup_logs_its_timeout() {
    partial_startup_logs_its_timeout(&[0, 0, 4]);
}

#[test]
fn c45_partial_startup_body_logs_its_timeout() {
    // A complete length word and version leave a partial startup body in the codec.
    partial_startup_logs_its_timeout(&[0, 0, 4, 0, 0, 3, 0, 0, b'u']);
}
