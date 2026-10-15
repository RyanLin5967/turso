//! Parse PostgreSQL statements with the parser the frontend uses (libpg_query through `pg_query`) and
//! print each parse tree as one JSON line, for tools that inspect statements without a parser of
//! their own (fastest-wire's E5' scanner).
//!
//! Input on stdin: statements separated by NUL bytes. Output, one line per statement:
//! `{"i": <index>, "tree": <ParseResult as JSON>}` or `{"i": <index>, "error": "<message>"}`.

use std::io::{Read, Write};

fn main() {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input).unwrap();
    let out = std::io::stdout();
    let mut out = std::io::BufWriter::new(out.lock());
    for (i, sql) in input.split(|&b| b == 0).enumerate() {
        if sql.is_empty() {
            continue;
        }
        let sql = String::from_utf8_lossy(sql);
        let line = match pg_query::parse(&sql) {
            Ok(parsed) => serde_json::json!({ "i": i, "tree": parsed.protobuf }),
            Err(e) => serde_json::json!({ "i": i, "error": e.to_string() }),
        };
        writeln!(out, "{line}").unwrap();
    }
    out.flush().unwrap();
}
