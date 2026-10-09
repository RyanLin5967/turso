//! turso-dbhash: Compute SHA1 hash of SQLite database content.
//!
//! This tool computes a hash of a database's **logical content**, independent
//! of its physical representation. Two databases with identical data produce
//! the same hash even if they have different page sizes, encodings, or layouts.

mod encoder;

use std::sync::Arc;
use turso_core::SqliteDialect;

use sha1::{Digest, Sha1};
use std::num::NonZero;
use turso_core::{
    Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, StepResult, Value, IO,
};

pub use encoder::encode_value;

#[derive(Debug, Clone, Default)]
pub struct DbHashOptions {
    /// Only hash tables matching this SQL LIKE pattern.
    pub table_filter: Option<String>,
    /// If true, only hash schema (no table content).
    pub schema_only: bool,
    /// If true, only hash content (no schema).
    pub without_schema: bool,
    /// If true, print each value to stderr as it's hashed.
    pub debug_trace: bool,
}

#[derive(Debug)]
pub struct DbHashResult {
    /// 40-character lowercase hex SHA1.
    pub hash: String,
    /// Number of tables hashed.
    pub tables_hashed: usize,
    /// Number of rows hashed.
    pub rows_hashed: usize,
}

/// Compute content hash of a database.
///
/// The hash is computed over the logical content of the database:
/// 1. Table content (unless `schema_only` is set)
/// 2. Schema entries (unless `without_schema` is set)
///
/// System tables (sqlite_%), virtual tables, and statistics tables are excluded.
pub fn hash_database(path: &str, options: &DbHashOptions) -> Result<DbHashResult, LimboError> {
    hash_database_with_database_opts(path, options, DatabaseOpts::new())
}

/// Compute content hash of a database, opening it with explicit feature flags.
pub fn hash_database_with_database_opts(
    path: &str,
    options: &DbHashOptions,
    database_opts: DatabaseOpts,
) -> Result<DbHashResult, LimboError> {
    assert!(
        !(options.schema_only && options.without_schema),
        "`schema_only` and `without_schema` cannot both be true"
    );
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
    let db = Database::open_file_with_flags(
        io.clone(),
        path,
        OpenFlags::default(),
        database_opts,
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;

    let mut hasher = Sha1::new();
    let mut tables_hashed = 0;
    let mut rows_hashed = 0;

    let filter = options.table_filter.as_deref().unwrap_or("%");

    // 1. Hash table content (unless schema_only)
    if !options.schema_only {
        let tables = get_table_names(&conn, &io, filter)?;
        for table in &tables {
            tables_hashed += 1;
            rows_hashed += hash_table(&conn, &io, table, &mut hasher, options.debug_trace)?;
        }
    }

    // 2. Hash schema (unless without_schema)
    if !options.without_schema {
        hash_schema(&conn, &io, filter, &mut hasher, options.debug_trace)?;
    }

    let hash = hex::encode(hasher.finalize());

    Ok(DbHashResult {
        hash,
        tables_hashed,
        rows_hashed,
    })
}

/// Get list of user tables (excludes sqlite_%, virtual tables).
fn get_table_names(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    like_pattern: &str,
) -> Result<Vec<String>, LimboError> {
    let sql = r#"SELECT name FROM sqlite_schema
           WHERE type = 'table'
             AND sql NOT LIKE 'CREATE VIRTUAL%'
             AND name NOT LIKE 'sqlite_%'
             AND name LIKE ?1
           ORDER BY name COLLATE nocase"#;

    let mut stmt = conn.prepare(sql)?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::from_text(like_pattern.to_string()),
    )?;
    let mut names = Vec::new();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                let name = row.get_value(0).to_text().expect("table name must be text");
                names.push(name.to_string());
            }
            StepResult::Sleep { duration } if waits_out_busy() => stmt.wait_out_busy(duration)?,
            StepResult::IO | StepResult::Sleep { .. } => io.step()?,
            StepResult::Yield => continue,
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => {
                return Err(LimboError::Busy);
            }
        }
    }

    Ok(names)
}

/// Whether the step loops answer a busy handler's `StepResult::Sleep` by waiting out its backoff
/// (`Statement::wait_out_busy`) and stepping again. Stepping the IO backend instead returned at
/// once when nothing was in flight, so a busy wait spun a core for the whole timeout (engine review
/// 11 MED 4); the busy statement has no IO of its own in flight then. Mutant `dbhash_sleep_spins`
/// (test builds only): the IO step, as before.
fn waits_out_busy() -> bool {
    !fe_mutant("dbhash_sleep_spins")
}

/// dbhash's registered mutants (`FE_MUTANT`, the convention of turso_core's branch store): one
/// names one deliberate defect, so each red can be shown to fail on its mutant from the same test
/// binary. TEST BUILDS ONLY: a production binary has no mutant to switch on. Read once per process.
fn fe_mutant(name: &str) -> bool {
    #[cfg(test)]
    {
        static ON: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
        ON.get_or_init(|| std::env::var("FE_MUTANT").ok())
            .as_deref()
            == Some(name)
    }
    #[cfg(not(test))]
    {
        let _ = name;
        false
    }
}

/// Hash all rows from a prepared statement.
fn hash_rows(
    stmt: &mut turso_core::Statement,
    io: &Arc<dyn IO>,
    hasher: &mut Sha1,
    debug: bool,
) -> Result<usize, LimboError> {
    let mut row_count = 0;
    let mut buf = Vec::new();

    loop {
        match stmt.step()? {
            StepResult::Row => {
                row_count += 1;
                let row = stmt.row().unwrap();
                for value in row.get_values() {
                    buf.clear();
                    encode_value(value, &mut buf);
                    if debug {
                        eprintln!("{value:?}");
                    }
                    hasher.update(&buf);
                }
            }
            StepResult::Sleep { duration } if waits_out_busy() => stmt.wait_out_busy(duration)?,
            StepResult::IO | StepResult::Sleep { .. } => io.step()?,
            StepResult::Yield => continue,
            StepResult::Done => break,
            StepResult::Busy | StepResult::Interrupt => {
                return Err(LimboError::Busy);
            }
        }
    }

    Ok(row_count)
}

/// Hash all rows in a table.
fn hash_table(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    table_name: &str,
    hasher: &mut Sha1,
    debug: bool,
) -> Result<usize, LimboError> {
    // Quote table name for safety (escape internal double quotes)
    let sql = format!("SELECT * FROM \"{}\"", table_name.replace('"', "\"\""));
    let mut stmt = conn.prepare(&sql)?;
    hash_rows(&mut stmt, io, hasher, debug)
}

/// Hash schema entries.
fn hash_schema(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<dyn IO>,
    like_pattern: &str,
    hasher: &mut Sha1,
    debug: bool,
) -> Result<(), LimboError> {
    let sql = r#"SELECT type, name, tbl_name, sql FROM sqlite_schema
           WHERE tbl_name LIKE ?1
           ORDER BY name COLLATE nocase"#;

    let mut stmt = conn.prepare(sql)?;
    stmt.bind_at(
        NonZero::new(1).unwrap(),
        Value::from_text(like_pattern.to_string()),
    )?;
    hash_rows(&mut stmt, io, hasher, debug)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`), read as core's busy red reads it.
    #[cfg(unix)]
    fn thread_cpu() -> std::time::Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is plain old data that clock_gettime writes whole.
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
        std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    /// Engine review 11 MED 4, dbhash's half of the busy-timeout spin (DECISIONS f8eb23bca): the
    /// step loops (`hash_rows`, `get_table_names`) answered a busy handler's `StepResult::Sleep` by
    /// stepping the IO backend, which returns at once when nothing is in flight, so a busy wait
    /// spun a core for the whole timeout. `hash_database` opens a connection with no busy handler,
    /// so no Sleep reaches the loops today; this drives `hash_rows` over a connection that has one.
    /// It waits out a 500 ms busy timeout behind another connection's open write transaction, and
    /// the thread's CPU time over the wait must be a small fraction of it.
    #[cfg(unix)]
    #[test]
    fn a_dbhash_busy_wait_sleeps_rather_than_spins() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("busy.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let holder = db.connect().unwrap();
        for sql in ["CREATE TABLE t(x)", "BEGIN", "INSERT INTO t VALUES (1)"] {
            holder.execute(sql).unwrap();
        }
        let waiter = db.connect().unwrap();
        waiter.set_busy_timeout(std::time::Duration::from_millis(500));
        let mut stmt = waiter.prepare("INSERT INTO t VALUES (2)").unwrap();
        let mut hasher = Sha1::new();
        let (wall, cpu) = (std::time::Instant::now(), thread_cpu());
        let refused = hash_rows(&mut stmt, &io, &mut hasher, false);
        let (waited, spent) = (wall.elapsed(), thread_cpu() - cpu);
        assert!(
            matches!(refused, Err(LimboError::Busy)),
            "premise: the second writer is refused busy, got {refused:?}"
        );
        assert!(
            waited >= std::time::Duration::from_millis(450),
            "premise: the busy timeout was waited out (waited {waited:?})"
        );
        assert!(
            spent < std::time::Duration::from_millis(100),
            "the busy wait spent {spent:?} of CPU over {waited:?}: it spun"
        );
        drop(stmt);
        holder.execute("COMMIT").unwrap();
    }
}
