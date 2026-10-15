//! Call `shrink_statement` directly on three files: state script, history
//! script, failing statement. Prints what the shrinker returned.
//!
//!     shrink_harness <state.sql> <history.sql> <failing.sql>

use anyhow::Result;
use differential_fuzzer::shrink::shrink_statement;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    let state = std::fs::read_to_string(&args[1])?;
    let history = std::fs::read_to_string(&args[2])?;
    let failing = std::fs::read_to_string(&args[3])?;
    let failing = failing.trim();
    println!("state lines   : {}", state.lines().count());
    println!("history lines : {}", history.lines().count());
    println!("failing bytes : {}", failing.len());
    match shrink_statement(&state, &history, failing)? {
        None => println!("RESULT: None  (shrink produced nothing)"),
        Some(m) => {
            println!("RESULT: Some");
            println!("--- state script ({} lines) ---", m.state_sql.lines().count());
            println!("{}", m.state_sql);
            println!("--- minimized statement ({} bytes) ---", m.statement.len());
            println!("{};", m.statement);
        }
    }
    Ok(())
}
