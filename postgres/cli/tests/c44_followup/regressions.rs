use super::*;

fn diagnostic(reply: Reply, code: &str, message: &str) {
    let error = reply.err("recorded PostgreSQL diagnostic");
    assert_eq!(error.code, code);
    assert_eq!(error.message, message);
}

#[test]
fn c44_followup_copy_rejects_invalid_integer_input() {
    let dir = Scratch::new("c44copyinput");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE cp(id INT, v TEXT)").ok("table");
    a.q("BEGIN").ok("begin");
    a.q("INSERT INTO cp VALUES (9, 'nine')").ok("prior write");
    let input = dir.0.join("bad.tsv");
    std::fs::write(&input, "not-a-number\tbad\n").unwrap();
    let reply = a.q(&format!("COPY cp FROM '{}'", input.display()));
    assert_eq!(reply.status, b'E');
    diagnostic(
        reply,
        "22P02",
        "invalid input syntax for type integer: \"not-a-number\"",
    );
    a.q("ROLLBACK").ok("rollback");
    assert_eq!(
        a.q("SELECT count(*) FROM cp").single("rolled back rows"),
        "0"
    );
}

#[test]
fn c44_followup_character_overflow_is_typed() {
    let dir = Scratch::new("c44charinput");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE ch(c CHAR(6))").ok("table");
    diagnostic(
        a.q("INSERT INTO ch VALUES ('abcdefg')"),
        "22001",
        "value too long for type character(6)",
    );
    assert_eq!(
        a.q("SELECT count(*) FROM ch").single("no overflow row"),
        "0"
    );
}

#[test]
fn c44_followup_existing_relation_is_typed() {
    let dir = Scratch::new("c44exists");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE p(id INT)").ok("table");
    diagnostic(
        a.q("CREATE TABLE p(id SERIAL)"),
        "42P07",
        "relation \"p\" already exists",
    );
    assert_eq!(
        a.q("SELECT count(*) FROM pg_sequences")
            .single("no leaked sequence"),
        "0"
    );
}

#[test]
fn c44_followup_excess_function_aliases_are_typed() {
    let dir = Scratch::new("c44aliascount");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    diagnostic(
        a.q("SELECT * FROM generate_series(1, 2) AS g(a, b)"),
        "42P10",
        "table \"g\" has 1 columns available but 2 columns specified",
    );
}

#[test]
fn c44_followup_large_parameter_reports_the_first_untyped_gap() {
    let dir = Scratch::new("c44paramgap");
    let server = Server::start(&dir.db(), &[]);
    let mut a = server.connect();
    a.q("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)")
        .ok("table");
    diagnostic(
        a.describe_statement("SELECT v FROM t WHERE id = $65536"),
        "42P18",
        "could not determine data type of parameter $1",
    );
}
