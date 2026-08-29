//! Call `shrink_statement` directly on three files: state script, history
//! script, failing statement. Prints what the shrinker returned.
//!
//!     shrink_harness <state.sql> <history.sql> <failing.sql> [out_dir]
//!
//! Logs go to stderr so stdout stays a clean, comparable result. With
//! `out_dir` the reduced state script and statement are also written there
//! byte for byte, so two builds can be compared with `cmp`.

use anyhow::Result;
use differential_fuzzer::shrink::shrink_statement;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    let state = std::fs::read_to_string(&args[1])?;
    let history = std::fs::read_to_string(&args[2])?;
    let failing = std::fs::read_to_string(&args[3])?;
    let failing = failing.trim();
    println!("state bytes   : {}", state.len());
    println!("history bytes : {}", history.len());
    println!("failing bytes : {}", failing.len());
    match shrink_statement(&state, &history, failing)? {
        None => println!("RESULT: None  (shrink produced nothing)"),
        Some(m) => {
            println!("RESULT: Some");
            println!("state_sql bytes : {}", m.state_sql.len());
            println!("state_sql lines : {}", m.state_sql.lines().count());
            println!("statement bytes : {}", m.statement.len());
            if let Some(dir) = args.get(4) {
                std::fs::create_dir_all(dir)?;
                std::fs::write(format!("{dir}/out-state.sql"), &m.state_sql)?;
                std::fs::write(format!("{dir}/out-stmt.sql"), &m.statement)?;
                println!("wrote {dir}/out-state.sql and {dir}/out-stmt.sql");
            }
            println!("--- minimized statement ---");
            println!("{};", m.statement);
        }
    }
    Ok(())
}
