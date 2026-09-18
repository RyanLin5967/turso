use std::process::Command;

#[test]
fn dump_stays_restorable_when_a_value_cannot_be_read() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let source = dir.path().join("source.db");
    {
        let seed = rusqlite::Connection::open(&source).expect("failed to open source database");
        seed.execute_batch(
            "CREATE TABLE u(x TEXT);
             INSERT INTO u VALUES('a'),('b');
             CREATE TABLE t(x TEXT);
             INSERT INTO t VALUES(CAST(X'FF' AS TEXT));
             CREATE TABLE v(x TEXT);
             INSERT INTO v VALUES('c');",
        )
        .expect("failed to seed source database");
    }

    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .arg("-q")
        .arg(&source)
        .arg(".dump")
        .output()
        .expect("failed to run tursodb");
    let dump = String::from_utf8(output.stdout).expect("dump output is utf-8");

    assert!(
        dump.contains("TEXT value contains invalid UTF-8"),
        "the unreadable value should be reported: {dump}"
    );
    assert_eq!(
        dump.matches("INSERT INTO \"u\" VALUES(").count(),
        2,
        "rows before the unreadable value should be dumped: {dump}"
    );
    assert_eq!(
        dump.matches("INSERT INTO \"v\" VALUES(").count(),
        1,
        "rows after the unreadable value should be dumped: {dump}"
    );
    assert!(
        dump.contains("COMMIT;"),
        "the dump must be committed or it restores as nothing: {dump}"
    );

    let restored = dir.path().join("restored.db");
    {
        let target = rusqlite::Connection::open(&restored).expect("failed to open target database");
        target
            .execute_batch(&dump)
            .expect("failed to replay the dump");
    }

    let reopened = rusqlite::Connection::open(&restored).expect("failed to reopen target database");
    let readable: i64 = reopened
        .query_row("SELECT count(*) FROM u", [], |row| row.get(0))
        .expect("failed to count rows restored from the first table");
    assert_eq!(
        readable, 2,
        "rows before the unreadable value should survive"
    );
    let trailing: i64 = reopened
        .query_row("SELECT count(*) FROM v", [], |row| row.get(0))
        .expect("failed to count rows restored from the last table");
    assert_eq!(
        trailing, 1,
        "rows after the unreadable value should survive"
    );
}
