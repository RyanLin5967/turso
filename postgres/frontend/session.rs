use std::num::NonZero;
use std::str;
use std::sync::{Arc, Mutex};

use crate::aliases;
use crate::catalog::{self, PostgresDialect};
use turso_core::{Connection, LimboError, PrepareOptions, Result, Statement, Value};
use turso_parser::ast::{self};
use turso_pg_parser::translator::{
    is_checkpoint, is_comment_on, is_refresh_matview, try_extract_add_constraints,
    try_extract_branch_call, try_extract_copy_from, try_extract_create_schema,
    try_extract_drop_schema, try_extract_set, try_extract_show, PgAddConstraints, PgBranchArg,
    PgBranchCall, PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt, PgSetStmt,
    PostgreSQLTranslator, BRANCH_FUNCTION_PREFIX,
};

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    inner: Arc<PgConnectionInner>,
}

struct PgConnectionInner {
    conn: Arc<Connection>,
    session_state: Mutex<SessionState>,
}

impl PgConnectionInner {
    fn set_search_path(&self, path: Vec<String>) {
        let mut state = self.session_state.lock().unwrap();
        state.search_path = path;
    }
}

#[derive(Default)]
struct SessionState {
    search_path: Vec<String>,
}

/// Open a database with the PostgreSQL schema dialect, resolving the IO
/// backend from `vfs` or the path like [`turso_core::Database::open_new`].
pub fn open_database(
    path: &str,
    vfs: Option<&str>,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<(Arc<dyn turso_core::IO>, Arc<turso_core::Database>)> {
    let io = match vfs {
        Some(vfs) => turso_core::Database::io_for_vfs(vfs)?,
        None => turso_core::Database::io_for_path(path)?,
    };
    let db = open_database_with_io(io.clone(), path, flags, opts)?;
    Ok((io, db))
}

/// Open a database with the PostgreSQL schema dialect on an existing IO
/// backend.
pub fn open_database_with_io(
    io: Arc<dyn turso_core::IO>,
    path: &str,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<Arc<turso_core::Database>> {
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts),
    )
}

impl PgConnection {
    pub fn new(conn: Arc<Connection>) -> Self {
        aliases::install(&conn);
        // PostgreSQL enforces every foreign key; the engine only under PRAGMA foreign_keys.
        conn.set_foreign_keys_enabled(true);
        Self {
            inner: Arc::new(PgConnectionInner {
                conn,
                session_state: Mutex::new(SessionState::default()),
            }),
        }
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.inner.conn
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        prepare_statement(&self.inner, sql.as_ref())
    }

    /// [`PgConnection::prepare`], with PostgreSQL's type OID for each result column the engine
    /// cannot type itself, read from the same parse (aggregates; see
    /// [`crate::result_types::aggregate_types`]). `None` (or a short list) where the engine types
    /// the column.
    pub fn prepare_typed(&self, sql: impl AsRef<str>) -> Result<(Statement, Vec<Option<u32>>)> {
        let mut types = Vec::new();
        let stmt = prepare_statement_typed(&self.inner, sql.as_ref(), Some(&mut types))?;
        Ok((stmt, types))
    }

    /// [`PgConnection::prepare_typed`] for a Describe, which must never perform the statement:
    /// `None` for one that preparing would perform (SET, CREATE/DROP SCHEMA, ALTER TABLE ADD
    /// CONSTRAINT, COPY FROM, or a statement whose prerequisites would run first). Such a statement
    /// returns no rows; its Execute prepares and performs it, once.
    pub fn prepare_for_describe(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Option<(Statement, Vec<Option<u32>>)>> {
        let mut types = Vec::new();
        let stmt = prepare_statement_inner(&self.inner, sql.as_ref(), Some(&mut types), true)?;
        Ok(stmt.map(|stmt| (stmt, types)))
    }

    pub fn query(&self, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return Ok(None);
        }
        self.prepare(sql).map(Some)
    }

    pub fn execute(&self, sql: impl AsRef<str>) -> Result<()> {
        for stmt in self.query_runner(sql.as_ref().as_bytes()) {
            if let Some(mut stmt) = stmt? {
                stmt.run_ignore_rows()?;
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.inner.conn.close()
    }

    pub fn pragma_update(&self, name: &str, value: impl std::fmt::Display) -> Result<()> {
        let sql = format!("PRAGMA {name} = {value}");
        let mut stmt = self.inner.conn.prepare_internal(sql)?;
        stmt.run_ignore_rows()
    }

    pub fn query_runner<'a>(&'a self, sql: &'a [u8]) -> PgQueryRunner<'a> {
        PgQueryRunner::new(&self.inner, sql)
    }

    /// Take over `from`'s session settings (the search path): a wire session that moves to another
    /// engine connection — onto a branch, or back to the trunk — keeps what it SET.
    pub fn adopt_session_of(&self, from: &PgConnection) {
        if Arc::ptr_eq(&self.inner, &from.inner) {
            return;
        }
        let path = from.inner.session_state.lock().unwrap().search_path.clone();
        self.inner.set_search_path(path);
    }
}

/// The branch function call `sql` is, if it is one (see [`PgBranchCall`]). A statement whose text
/// does not contain the branch-function prefix, in any case, is not parsed. One that does is read
/// by [`fast_branch_call`], the server's fast path (DECISIONS L5: a branch call reaches the engine
/// with no libpg_query call); a form it does not cover — a comment, an escape or dollar-quoted
/// string, a second argument, a quoted name — is parsed by libpg_query, which then decides.
pub fn branch_call(sql: &str) -> Option<PgBranchCall> {
    let prefix = BRANCH_FUNCTION_PREFIX.as_bytes();
    if !sql
        .as_bytes()
        .windows(prefix.len())
        .any(|w| w.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    if let Some(call) = fast_branch_call(sql) {
        return Some(call);
    }
    let parsed = turso_pg_parser::parse(sql).ok()?;
    try_extract_branch_call(&parsed)
}

/// The common forms of a branch call, read byte by byte with no allocation but the call itself:
/// `SELECT turso_branch_<op>(<arg>?)` with an optional trailing `;`, where `<arg>` is a standard
/// string literal (`''` for a quote; a backslash is an ordinary character, as PostgreSQL reads one
/// with standard_conforming_strings on) or a `$n` parameter, either optionally cast to `text`,
/// `varchar` or `character varying` with no length (a cast that changes nothing; any other cast
/// is read by libpg_query, and [`try_extract_branch_call`] refuses it). Keywords and the
/// function name in any case; PostgreSQL's whitespace (space, tab, newline, carriage return, form
/// feed, vertical tab) anywhere a token boundary allows it. `None` means only "not one of these
/// forms": the caller asks libpg_query. Whatever this returns, [`try_extract_branch_call`] returns
/// for the same statement (pinned by a differential test over a corpus).
fn fast_branch_call(sql: &str) -> Option<PgBranchCall> {
    let b = sql.as_bytes();
    let mut i = 0;
    let ws = |i: &mut usize| {
        while *i < b.len() && matches!(b[*i], b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b) {
            *i += 1;
        }
    };
    // An identifier or keyword: [A-Za-z_][A-Za-z0-9_]*, ended by a byte that cannot continue one
    // (PostgreSQL identifiers also take '$' and non-ASCII bytes after the first: those end the
    // fast path, not the identifier).
    let word = |i: &mut usize| -> Option<std::ops::Range<usize>> {
        let start = *i;
        if *i >= b.len() || !(b[*i].is_ascii_alphabetic() || b[*i] == b'_') {
            return None;
        }
        while *i < b.len() && (b[*i].is_ascii_alphanumeric() || b[*i] == b'_') {
            *i += 1;
        }
        if *i < b.len() && (b[*i] == b'$' || b[*i] >= 0x80) {
            return None;
        }
        Some(start..*i)
    };
    ws(&mut i);
    let select = word(&mut i)?;
    if !b[select].eq_ignore_ascii_case(b"select") {
        return None;
    }
    let before_name = i;
    ws(&mut i);
    if i == before_name {
        return None;
    }
    let name = word(&mut i)?;
    let function = sql[name].to_ascii_lowercase();
    if !function.starts_with(BRANCH_FUNCTION_PREFIX) {
        return None;
    }
    ws(&mut i);
    if b.get(i) != Some(&b'(') {
        return None;
    }
    i += 1;
    ws(&mut i);
    let mut args = Vec::new();
    if b.get(i) != Some(&b')') {
        let arg = match b.get(i)? {
            b'\'' => {
                i += 1;
                let mut text = String::new();
                let mut run = i;
                loop {
                    let q = i + b[i..].iter().position(|&c| c == b'\'')?;
                    if b.get(q + 1) == Some(&b'\'') {
                        text.push_str(&sql[run..q + 1]);
                        i = q + 2;
                        run = i;
                    } else {
                        text.push_str(&sql[run..q]);
                        i = q + 1;
                        break;
                    }
                }
                PgBranchArg::Text(text)
            }
            b'$' => {
                i += 1;
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                let n: usize = sql[start..i].parse().ok()?;
                if n == 0 || (i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_')) {
                    return None;
                }
                PgBranchArg::Param(n)
            }
            _ => return None,
        };
        ws(&mut i);
        if b[i..].starts_with(b"::") {
            i += 2;
            ws(&mut i);
            // Only a cast that changes nothing: to text, or to varchar with no length. A length,
            // array bounds, a qualified name or any other type ends the fast path here.
            let ty = &b[word(&mut i)?];
            if ty.eq_ignore_ascii_case(b"character") {
                let before = i;
                ws(&mut i);
                if i == before || !b[word(&mut i)?].eq_ignore_ascii_case(b"varying") {
                    return None;
                }
            } else if !(ty.eq_ignore_ascii_case(b"text") || ty.eq_ignore_ascii_case(b"varchar")) {
                return None;
            }
            ws(&mut i);
        }
        args.push(arg);
    }
    if b.get(i) != Some(&b')') {
        return None;
    }
    i += 1;
    ws(&mut i);
    if b.get(i) == Some(&b';') {
        i += 1;
        ws(&mut i);
    }
    if i != b.len() {
        return None;
    }
    Some(PgBranchCall { function, args })
}

/// Attach every PostgreSQL schema database file (`turso-postgres-schema-<name>.db`) beside
/// `db_file` to `conn`, so a connection opened after a `CREATE SCHEMA` sees the schema. Failures
/// are logged and skipped.
pub fn attach_schema_files(conn: &PgConnection, db_file: &str) {
    if db_file == ":memory:" {
        return;
    }
    let dir = std::path::Path::new(db_file)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(schema) = name
            .strip_prefix("turso-postgres-schema-")
            .and_then(|s| s.strip_suffix(".db"))
        else {
            continue;
        };
        let path = entry.path().to_string_lossy().to_string();
        let sql = format!("ATTACH '{path}' AS \"{schema}\"");
        tracing::info!("Auto-attaching PG schema '{}' from {}", schema, path);
        if let Err(e) = conn.inner().execute(&sql) {
            tracing::warn!("Failed to attach schema '{}': {}", schema, e);
        }
    }
}

pub struct PgQueryRunner<'a> {
    conn: &'a Arc<PgConnectionInner>,
    stmts: Vec<String>,
    index: usize,
}

impl<'a> PgQueryRunner<'a> {
    fn new(conn: &'a Arc<PgConnectionInner>, sql: &'a [u8]) -> Self {
        let sql = str::from_utf8(sql).unwrap_or("");
        Self {
            conn,
            stmts: split_statements(sql)
                .unwrap_or_else(|_| vec![sql.trim().to_string()])
                .into_iter()
                .filter(|stmt| !stmt.trim().is_empty())
                .collect(),
            index: 0,
        }
    }
}

impl Iterator for PgQueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.stmts.len() {
            return None;
        }

        let sql = &self.stmts[self.index];
        self.index += 1;
        Some(prepare_statement(self.conn, sql).map(Some))
    }
}

pub fn split_statements(sql: &str) -> Result<Vec<String>> {
    // Text with no separator but a trailing one is one statement: no literal, comment or dollar
    // quote can make it two, so libpg_query is not asked.
    let trimmed = sql.trim();
    let body = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    if !body.is_empty() && !body.contains(';') {
        return Ok(vec![body.to_string()]);
    }
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !sql.trim().is_empty() => Ok(vec![sql.trim().to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![sql.trim().to_string()]),
    }
}

fn prepare_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<Statement> {
    prepare_statement_typed(pg_conn, sql, None)
}

/// [`prepare_statement`], filling `types` (when asked) with the result types the parse gives.
fn prepare_statement_typed(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    types: Option<&mut Vec<Option<u32>>>,
) -> Result<Statement> {
    prepare_statement_inner(pg_conn, sql, types, false)?.ok_or_else(|| {
        LimboError::InternalError("only a Describe declines to prepare a statement".to_string())
    })
}

/// Whether preparing this statement performs it: the special forms the frontend carries out at
/// prepare (SET, CREATE SCHEMA, DROP SCHEMA, ALTER TABLE ADD CONSTRAINT, COPY FROM), read from the
/// parse, never from the text's first word.
fn performs_at_prepare(parse_result: &turso_pg_parser::pg_query::ParseResult) -> bool {
    try_extract_set(parse_result).is_some()
        || try_extract_create_schema(parse_result).is_some()
        || try_extract_drop_schema(parse_result).is_some()
        || try_extract_add_constraints(parse_result).is_some()
        || try_extract_copy_from(parse_result).is_some()
}

/// The prepare behind [`prepare_statement_typed`]. With `describe`, a statement that preparing
/// would perform (see [`performs_at_prepare`]), or one that needs prerequisite statements run
/// first (a SERIAL column's sequence), is declined with `None`: it returns no rows, and only its
/// execution may perform it (wire review 2 item 2, review 3 item 2).
fn prepare_statement_inner(
    pg_conn: &Arc<PgConnectionInner>,
    sql: &str,
    types: Option<&mut Vec<Option<u32>>>,
    describe: bool,
) -> Result<Option<Statement>> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string(),
        ));
    }

    reject_sqlite_catalog_access(sql)?;

    // One parse serves both the special forms and the translation (it was two).
    let parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    if describe && performs_at_prepare(&parse_result) {
        return Ok(None);
    }
    if let Some(stmt) = try_prepare_special(pg_conn, &parse_result)? {
        return Ok(Some(stmt));
    }
    if let Some(types) = types {
        *types =
            crate::result_types::aggregate_types(&parse_result, &pg_conn.conn.current_schema());
    }

    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(translated.cmd.stmt())?;
    if describe && !translated.prereqs.is_empty() {
        return Ok(None);
    }

    let options = {
        let state = pg_conn.session_state.lock().unwrap();
        let path = state.search_path.clone();
        PrepareOptions {
            unqualified_database_search_path: if path.is_empty() { None } else { Some(path) },
        }
    };
    for prereq in translated.prereqs {
        let input = prereq.to_string();
        let mut stmt = pg_conn
            .conn
            .prepare_translated_stmt_with_options(prereq, &input, &options)?;
        stmt.run_ignore_rows()?;
    }

    pg_conn
        .conn
        .prepare_translated_cmd_with_options(translated.cmd, sql, &options)
        .map(Some)
}

fn reject_catalog_dml(stmt: &ast::Stmt) -> Result<()> {
    let table_name = match stmt {
        ast::Stmt::Insert { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Delete { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Update(update) => Some(update.tbl_name.name.as_str()),
        _ => None,
    };

    let Some(table_name) = table_name else {
        return Ok(());
    };

    if !catalog::is_catalog_table_name(table_name) {
        return Ok(());
    }

    let verb = match stmt {
        ast::Stmt::Insert { .. } => "insert into",
        ast::Stmt::Delete { .. } => "delete from",
        ast::Stmt::Update { .. } => "update",
        _ => unreachable!(),
    };
    Err(LimboError::ParseError(format!(
        "cannot {verb} pg_catalog table \"{table_name}\""
    )))
}

fn reject_sqlite_catalog_access(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    for table_name in ["sqlite_master", "sqlite_schema"] {
        if lower.contains(table_name) {
            return Err(LimboError::ParseError(format!(
                "no such table: {table_name}"
            )));
        }
    }
    Ok(())
}

fn try_prepare_special(
    pg_conn: &Arc<PgConnectionInner>,
    parse_result: &turso_pg_parser::pg_query::ParseResult,
) -> Result<Option<Statement>> {
    if let Some(set_stmt) = try_extract_set(&parse_result) {
        let stmt = handle_pg_set(pg_conn, &set_stmt)?;
        return Ok(Some(stmt));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let pragma_sql = format!("PRAGMA {}", show_stmt.name);
        return Ok(Some(pg_conn.conn.prepare(&pragma_sql)?));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result) {
        handle_pg_create_schema(&pg_conn.conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        handle_pg_drop_schema(&pg_conn.conn, &stmt)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_refresh_matview(&parse_result) {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    // PostgreSQL's CHECKPOINT writes every dirty page to the data files; a WAL checkpoint that
    // copies every frame back into the database file and truncates the log is the same act here.
    if is_checkpoint(&parse_result) {
        return Ok(Some(
            pg_conn.conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?,
        ));
    }

    if let Some(add) = try_extract_add_constraints(&parse_result) {
        handle_pg_add_constraints(pg_conn, &add)?;
        return Ok(Some(noop_statement(&pg_conn.conn)?));
    }

    if let Some(stmt) = try_extract_copy_from(&parse_result) {
        let rows_inserted = handle_pg_copy_from(&pg_conn.conn, &stmt)?;
        let stmt = noop_statement(&pg_conn.conn)?;
        stmt.set_n_change(rows_inserted as i64);
        return Ok(Some(stmt));
    }

    Ok(None)
}

fn noop_statement(conn: &Arc<Connection>) -> Result<Statement> {
    conn.prepare("SELECT 0 WHERE 0")
}

fn execute_sqlite_internal(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_internal(sql)?;
    stmt.run_ignore_rows()
}

fn handle_pg_set(pg_conn: &Arc<PgConnectionInner>, set_stmt: &PgSetStmt) -> Result<Statement> {
    if set_stmt.name == "search_path" {
        let path = set_stmt
            .values
            .iter()
            .map(|value| value.as_search_path_name().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| LimboError::ParseError("incorrect format".to_string()))?;
        pg_conn.set_search_path(path);
        return noop_statement(&pg_conn.conn);
    }
    let value = set_stmt.values.first().ok_or_else(|| {
        LimboError::ParseError(format!("SET {}: no value provided", set_stmt.name))
    })?;
    let pragma_sql = format!("PRAGMA {} = {}", set_stmt.name, value.to_sql_string());
    pg_conn.conn.prepare(&pragma_sql)
}

fn handle_pg_create_schema(conn: &Arc<Connection>, stmt: &PgCreateSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }

    if schema_exists(conn, &name)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }

    let path = schema_file_path(conn, &name);
    execute_sqlite_internal(
        conn,
        format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name),
    )?;
    Ok(())
}

fn schema_file_path(conn: &Connection, schema_name: &str) -> String {
    let main_path = conn.db_file_path();
    let filename = format!("turso-postgres-schema-{schema_name}.db");
    if main_path == ":memory:" {
        filename
    } else {
        let parent = std::path::Path::new(&main_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        parent.join(&filename).to_string_lossy().to_string()
    }
}

fn handle_pg_drop_schema(conn: &Arc<Connection>, stmt: &PgDropSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" {
        return handle_pg_drop_schema_public(conn, stmt.cascade);
    }

    if !schema_exists(conn, &name)? {
        if stmt.if_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }

    if stmt.cascade {
        drop_all_tables_in_schema(conn, &name)?;
    }

    execute_sqlite_internal(conn, format!("DETACH \"{name}\""))?;
    Ok(())
}

fn handle_pg_drop_schema_public(conn: &Arc<Connection>, cascade: bool) -> Result<()> {
    let table_names = list_user_tables(conn, None)?;
    if !cascade && !table_names.is_empty() {
        return Err(LimboError::ParseError(
            "cannot drop schema \"public\" because other objects depend on it".to_string(),
        ));
    }

    for table_name in table_names {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{table_name}\""))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

fn drop_all_tables_in_schema(conn: &Arc<Connection>, schema_name: &str) -> Result<()> {
    for table_name in list_user_tables(conn, Some(schema_name))? {
        let mut stmt = conn.prepare(format!("DROP TABLE \"{schema_name}\".\"{table_name}\"",))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

/// `ALTER TABLE t ADD <constraint>...` ([`PgAddConstraints`]): the engine adds no constraint to an
/// existing table, so the table is rebuilt from its own PostgreSQL definition with the
/// constraints appended, atomically (in the session's transaction, or in one of its own):
/// its rows are copied aside, the table is dropped and created anew, the rows are inserted back
/// with foreign keys enforced (so a row that breaks a new constraint fails the ALTER, as
/// PostgreSQL validates it), and its indexes and triggers are created again. Foreign keys are not
/// enforced while the old table is dropped, so its children are untouched. A table created by
/// CREATE TABLE AS, or not through this frontend, has no definition to rebuild from: refused.
fn handle_pg_add_constraints(
    pg_conn: &Arc<PgConnectionInner>,
    add: &PgAddConstraints,
) -> Result<()> {
    let conn = &pg_conn.conn;
    if add
        .schema
        .as_deref()
        .is_some_and(|s| !s.eq_ignore_ascii_case("public"))
    {
        return Err(LimboError::ParseError(
            "ALTER TABLE ADD CONSTRAINT is supported only for tables in schema public".to_string(),
        ));
    }
    let table = add.table.as_str();
    let mut definition = None;
    let mut dependents = Vec::new();
    // The catalog lookup is an internal helper statement, which keeps the connection nested until
    // it is dropped; every statement of the rebuild run while nested opens no transaction of its
    // own, and its DDL reaches SetCookie with none (wire review 2 item 1). So the lookup's rows are
    // collected and the statement dropped here, before the rebuild's first statement.
    {
        let mut stmt = conn.prepare_internal(
            "SELECT type, sql FROM sqlite_schema WHERE tbl_name = ?1 AND sql IS NOT NULL \
             ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, rowid",
        )?;
        stmt.bind_at(
            NonZero::new(1).unwrap(),
            Value::build_text(table.to_string()),
        )?;
        for row in stmt.run_collect_rows()? {
            let (Some(Value::Text(kind)), Some(Value::Text(sql))) = (row.first(), row.get(1))
            else {
                continue;
            };
            match kind.as_str() {
                "table" => definition = Some(sql.as_str().to_string()),
                "index" | "trigger" => dependents.push(sql.as_str().to_string()),
                _ => {}
            }
        }
    }
    if conn.is_nested_stmt() {
        return Err(LimboError::InternalError(
            "ALTER TABLE ADD CONSTRAINT: the connection is inside another statement, so the \
             rebuild's statements would run without their own transaction"
                .to_string(),
        ));
    }
    let definition = definition
        .ok_or_else(|| LimboError::ParseError(format!("relation \"{table}\" does not exist")))?;
    let pg_definition = catalog::decode_stored_pg_schema_sql(&definition).ok_or_else(|| {
        LimboError::ParseError(format!(
            "ALTER TABLE ADD CONSTRAINT: table \"{table}\" was not created by CREATE TABLE in \
             this frontend, so it has no definition to rebuild"
        ))
    })?;
    let mut parsed =
        turso_pg_parser::parse(pg_definition).map_err(|e| LimboError::ParseError(e.to_string()))?;
    // Taken before the definition is edited below (a clone made while `create` borrows it would
    // not compile).
    let mut aside_tree = parsed.protobuf.clone();
    let create = parsed
        .protobuf
        .stmts
        .first_mut()
        .and_then(|raw| raw.stmt.as_mut())
        .and_then(|s| s.node.as_mut());
    let Some(turso_pg_parser::pg_query::protobuf::node::Node::CreateStmt(create)) = create else {
        return Err(LimboError::ParseError(format!(
            "ALTER TABLE ADD CONSTRAINT: table \"{table}\" was created by CREATE TABLE AS, so it \
             has no column definitions to rebuild"
        )));
    };
    create.if_not_exists = false;
    // The rows wait in a table of the same columns and types, so each value is decoded and
    // encoded by its own type both ways and reaches the rebuilt table as it was stored. (A copy
    // made by CREATE TABLE AS would take its column types from the decoded values.) The name is
    // not under the engine's reserved prefixes, which a client statement may not create, and not
    // the name of any table that exists (a user's table of that name is never touched).
    let schema = conn.current_schema();
    let aside_name = (0..100)
        .map(|n| match n {
            0 => format!("{table}__turso_rebuild"),
            n => format!("{table}__turso_rebuild_{n}"),
        })
        .find(|name| schema.get_table(name).is_none())
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "ALTER TABLE ADD CONSTRAINT: no free name for the rebuild's aside table of \
                 \"{table}\""
            ))
        })?;
    drop(schema);
    if let Some(turso_pg_parser::pg_query::protobuf::node::Node::CreateStmt(c)) = aside_tree
        .stmts
        .first_mut()
        .and_then(|raw| raw.stmt.as_mut())
        .and_then(|s| s.node.as_mut())
    {
        c.if_not_exists = false;
        if let Some(relation) = c.relation.as_mut() {
            relation.relname = aside_name.clone();
            relation.schemaname.clear();
        }
    }
    let aside_def =
        turso_pg_parser::deparse(&aside_tree).map_err(|e| LimboError::ParseError(e.to_string()))?;
    create.table_elts.extend(add.constraints.iter().cloned());
    let rebuilt = turso_pg_parser::deparse(&parsed.protobuf)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;

    let quoted = format!("\"{}\"", table.replace('"', "\"\""));
    let aside = format!("\"{}\"", aside_name.replace('"', "\"\""));
    let in_tx = !conn.get_auto_commit();
    execute_root(
        conn,
        if in_tx {
            "SAVEPOINT __turso_rebuild"
        } else {
            "BEGIN"
        },
    )?;
    let enforced = conn.foreign_keys_enabled();
    // Every step is a root statement: an internal helper statement opens no transaction of its
    // own (DDL through one panicked the engine at SetCookie) and skips foreign key checks.
    let rebuild = (|| {
        conn.set_foreign_keys_enabled(false);
        run_pg_statement(pg_conn, &aside_def)?;
        execute_root(conn, format!("INSERT INTO {aside} SELECT * FROM {quoted}"))?;
        execute_root(conn, format!("DROP TABLE {quoted}"))?;
        run_pg_statement(pg_conn, &rebuilt)?;
        // With foreign keys enforced, the copy back checks every row against the new
        // constraints, as PostgreSQL validates an added constraint.
        conn.set_foreign_keys_enabled(true);
        execute_root(conn, format!("INSERT INTO {quoted} SELECT * FROM {aside}"))?;
        conn.set_foreign_keys_enabled(false);
        execute_root(conn, format!("DROP TABLE {aside}"))?;
        for sql in &dependents {
            match catalog::decode_stored_pg_schema_sql(sql) {
                Some(pg_sql) => run_pg_statement(pg_conn, pg_sql)?,
                None => execute_root(conn, sql)?,
            }
        }
        Ok(())
    })();
    conn.set_foreign_keys_enabled(enforced);
    match rebuild {
        Ok(()) => execute_root(
            conn,
            if in_tx {
                "RELEASE SAVEPOINT __turso_rebuild"
            } else {
                "COMMIT"
            },
        ),
        Err(e) => {
            let undone = if in_tx {
                execute_root(conn, "ROLLBACK TO SAVEPOINT __turso_rebuild")
                    .and_then(|()| execute_root(conn, "RELEASE SAVEPOINT __turso_rebuild"))
            } else {
                execute_root(conn, "ROLLBACK")
            };
            // An undo that failed leaves the table half rebuilt in a transaction nobody can name:
            // the connection is broken, not merely the statement (the server ends the session).
            match undone {
                Ok(()) => Err(e),
                Err(undo) => Err(LimboError::InternalError(format!(
                    "{CONNECTION_BROKEN}: ALTER TABLE ADD CONSTRAINT failed ({e}) and undoing it \
                     failed too ({undo})"
                ))),
            }
        }
    }
}

/// The start of an engine error's text after which the connection's state is unknown: the server
/// ends the session (FATAL 08006) rather than serve more statements on it.
pub const CONNECTION_BROKEN: &str = "connection broken";

/// Run one statement as the engine runs a client's (a root statement), in SQLite text.
fn execute_root(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    conn.prepare_sqlite(sql)?.run_ignore_rows()
}

/// Run one PostgreSQL statement through this frontend, as a client's would be.
fn run_pg_statement(pg_conn: &Arc<PgConnectionInner>, sql: &str) -> Result<()> {
    prepare_statement(pg_conn, sql)?.run_ignore_rows()
}

fn handle_pg_copy_from(conn: &Arc<Connection>, stmt: &PgCopyFromStmt) -> Result<usize> {
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e))
    })?;

    let table_name = match &stmt.schema_name {
        Some(schema) => format!("\"{schema}\".\"{}\"", stmt.table_name),
        None => format!("\"{}\"", stmt.table_name),
    };
    let column_names = get_table_columns(conn, &stmt.table_name, stmt.schema_name.as_deref())?;
    if column_names.is_empty() {
        return Err(LimboError::ParseError(format!(
            "COPY FROM: table '{}' not found or has no columns",
            stmt.table_name
        )));
    }

    let (insert_cols, num_columns) = match &stmt.columns {
        Some(cols) => {
            let col_list = cols
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            (format!(" ({col_list})"), cols.len())
        }
        None => (String::new(), column_names.len()),
    };

    let placeholders = (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {table_name}{insert_cols} VALUES ({placeholders})");

    let delimiter = stmt
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = stmt.null_string.as_deref().unwrap_or("\\N");

    let mut rows = parse_copy_text_format(&data, delimiter, null_string, num_columns)?;
    if stmt.header && !rows.is_empty() {
        rows.remove(0);
    }

    let rows_inserted = rows.len();
    let mut begin = conn.prepare_sqlite("BEGIN")?;
    begin.run_ignore_rows()?;

    let result = (|| {
        let mut insert_stmt = conn.prepare_sqlite(&insert_sql)?;
        for row in &rows {
            for (i, val) in row.iter().enumerate() {
                let index = NonZero::new(i + 1).unwrap();
                match val {
                    Some(s) => insert_stmt.bind_at(index, Value::build_text(s.clone()))?,
                    None => insert_stmt.bind_at(index, Value::Null)?,
                }
            }
            insert_stmt.run_ignore_rows()?;
            insert_stmt.reset()?;
            insert_stmt.clear_bindings();
        }

        let mut commit = conn.prepare_sqlite("COMMIT")?;
        commit.run_ignore_rows()?;
        Ok(rows_inserted)
    })();

    if result.is_err() {
        if let Ok(mut rollback) = conn.prepare_sqlite("ROLLBACK") {
            let _ = rollback.run_ignore_rows();
        }
    }

    result
}

fn get_table_columns(
    conn: &Arc<Connection>,
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<Vec<String>> {
    let sql = match schema_name {
        Some(schema) => format!("PRAGMA \"{schema}\".table_info('{table_name}')"),
        None => format!("PRAGMA table_info('{table_name}')"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn list_user_tables(conn: &Arc<Connection>, schema_name: Option<&str>) -> Result<Vec<String>> {
    let filter = "type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'";
    let sql = match schema_name {
        Some(name) => format!("SELECT name FROM \"{name}\".sqlite_schema WHERE {filter}"),
        None => format!("SELECT name FROM sqlite_schema WHERE {filter}"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.first() {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn schema_exists(conn: &Arc<Connection>, schema_name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT 1 FROM pragma_database_list WHERE name = '{}'",
        schema_name.replace('\'', "''")
    );
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(!rows.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slow(sql: &str) -> Option<PgBranchCall> {
        try_extract_branch_call(&turso_pg_parser::parse(sql).ok()?)
    }

    fn call(function: &str, args: Vec<PgBranchArg>) -> Option<PgBranchCall> {
        Some(PgBranchCall {
            function: function.to_string(),
            args,
        })
    }

    /// The forms clients send are read on the fast path, to exactly the call they are.
    #[test]
    fn the_fast_path_reads_the_forms_clients_send() {
        use PgBranchArg::{Param, Text};
        let create = "turso_branch_create";
        let cases = [
            (
                "SELECT turso_branch_create('b_1_0_17')",
                call(create, vec![Text("b_1_0_17".into())]),
            ),
            (
                "select turso_branch_switch('b');",
                call("turso_branch_switch", vec![Text("b".into())]),
            ),
            (
                "SELECT turso_branch_current()",
                call("turso_branch_current", vec![]),
            ),
            (
                "SELECT TURSO_BRANCH_DELETE ( 'b' ) ;",
                call("turso_branch_delete", vec![Text("b".into())]),
            ),
            (
                "SELECT turso_branch_create($1)",
                call(create, vec![Param(1)]),
            ),
            (
                "SELECT turso_branch_create($12::text)",
                call(create, vec![Param(12)]),
            ),
            (
                "SELECT turso_branch_create('it''s'::varchar)",
                call(create, vec![Text("it's".into())]),
            ),
            (
                "SELECT turso_branch_create('a\\b')",
                call(create, vec![Text("a\\b".into())]),
            ),
            (
                "SELECT turso_branch_create('')",
                call(create, vec![Text(String::new())]),
            ),
            (
                "\tSELECT\nturso_branch_create(\r'x'\x0b)\x0c;\n",
                call(create, vec![Text("x".into())]),
            ),
            (
                "SELECT turso_branch_create('é ü')",
                call(create, vec![Text("é ü".into())]),
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(fast_branch_call(sql), want, "fast path, {sql:?}");
            assert_eq!(branch_call(sql), want, "branch_call, {sql:?}");
        }
    }

    /// Differential: whatever the fast path reads, libpg_query reads the same; every form it does
    /// not read still reaches the same answer through libpg_query. The corpus crosses keyword case,
    /// spacing, argument forms, casts, terminators and trailing junk.
    #[test]
    fn the_fast_path_agrees_with_libpg_query() {
        let selects = ["SELECT", "select", "SeLeCt"];
        let names = [
            "turso_branch_create",
            "TURSO_BRANCH_SWITCH",
            "turso_branch_current",
            "turso_branch_nope",
            "\"turso_branch_create\"",
            "public.turso_branch_create",
            "turso_branch_create$x",
            "turso_branchx",
        ];
        let gaps = ["", " ", "  ", "\n", "\t \r\n"];
        let args = [
            "",
            "'b'",
            "'it''s'",
            "'a\\b'",
            "''",
            "''''",
            "'x'::text",
            "'x' :: text",
            "'x'::text[]",
            "'x'::character varying",
            "$1",
            "$1::text",
            "$0",
            "$1x",
            "E'x'",
            "$$x$$",
            "'a'\n'b'",
            "'a' 'b'",
            "'a', 'b'",
            "NULL",
            "true",
            "1",
            "'a' || 'b'",
            "'é'",
        ];
        let ends = [
            "",
            ";",
            " ; ",
            ";;",
            "; SELECT 1",
            " FROM t",
            " -- c",
            "/* c */",
            ")",
        ];
        let mut fast_hits = 0;
        let mut n = 0;
        for sel in selects {
            for name in names {
                for g in gaps {
                    for arg in args {
                        for end in ends {
                            let sql = format!("{sel} {name}{g}({g}{arg}{g}){end}");
                            n += 1;
                            let slow = slow(&sql);
                            if let Some(fast) = fast_branch_call(&sql) {
                                fast_hits += 1;
                                assert_eq!(Some(fast), slow, "{sql:?}");
                            }
                            assert_eq!(branch_call(&sql), slow, "{sql:?}");
                        }
                    }
                }
            }
        }
        assert!(
            fast_hits > n / 20,
            "the fast path read only {fast_hits} of {n}"
        );
    }

    /// Every PostgreSQL keyword (libpg_query's kwlist.h, PostgreSQL 17: 491 words) as the cast of a
    /// branch call's argument, in both argument forms: the fast path reads a call only where
    /// libpg_query reads the same one, and `branch_call` answers as libpg_query does. At 472023b72
    /// the fast path took any word after `::`, so the keywords libpg_query refuses as a type (78
    /// reserved and 44 column-name keywords, 244 statements, as two reviewers measured) were calls
    /// to it and syntax errors to PostgreSQL: `turso_branch_delete('prod'::where)` deleted the
    /// branch (wire review 1 item 3).
    #[test]
    fn a_keyword_cast_is_read_as_libpg_query_reads_it() {
        #[rustfmt::skip]
        const KEYWORDS: [&str; 491] = [
            "abort", "absent", "absolute", "access", "action", "add", "admin", "after", "aggregate",
            "all", "also", "alter", "always", "analyse", "analyze", "and", "any", "array", "as",
            "asc", "asensitive", "assertion", "assignment", "asymmetric", "at", "atomic", "attach",
            "attribute", "authorization", "backward", "before", "begin", "between", "bigint",
            "binary", "bit", "boolean", "both", "breadth", "by", "cache", "call", "called",
            "cascade", "cascaded", "case", "cast", "catalog", "chain", "char", "character",
            "characteristics", "check", "checkpoint", "class", "close", "cluster", "coalesce",
            "collate", "collation", "column", "columns", "comment", "comments", "commit",
            "committed", "compression", "concurrently", "conditional", "configuration", "conflict",
            "connection", "constraint", "constraints", "content", "continue", "conversion", "copy",
            "cost", "create", "cross", "csv", "cube", "current", "current_catalog", "current_date",
            "current_role", "current_schema", "current_time", "current_timestamp", "current_user",
            "cursor", "cycle", "data", "database", "day", "deallocate", "dec", "decimal", "declare",
            "default", "defaults", "deferrable", "deferred", "definer", "delete", "delimiter",
            "delimiters", "depends", "depth", "desc", "detach", "dictionary", "disable", "discard",
            "distinct", "do", "document", "domain", "double", "drop", "each", "else", "empty",
            "enable", "encoding", "encrypted", "end", "enum", "error", "escape", "event", "except",
            "exclude", "excluding", "exclusive", "execute", "exists", "explain", "expression",
            "extension", "external", "extract", "false", "family", "fetch", "filter", "finalize",
            "first", "float", "following", "for", "force", "foreign", "format", "forward", "freeze",
            "from", "full", "function", "functions", "generated", "global", "grant", "granted",
            "greatest", "group", "grouping", "groups", "handler", "having", "header", "hold",
            "hour", "identity", "if", "ilike", "immediate", "immutable", "implicit", "import", "in",
            "include", "including", "increment", "indent", "index", "indexes", "inherit",
            "inherits", "initially", "inline", "inner", "inout", "input", "insensitive", "insert",
            "instead", "int", "integer", "intersect", "interval", "into", "invoker", "is", "isnull",
            "isolation", "join", "json", "json_array", "json_arrayagg", "json_exists",
            "json_object", "json_objectagg", "json_query", "json_scalar", "json_serialize",
            "json_table", "json_value", "keep", "key", "keys", "label", "language", "large", "last",
            "lateral", "leading", "leakproof", "least", "left", "level", "like", "limit", "listen",
            "load", "local", "localtime", "localtimestamp", "location", "lock", "locked", "logged",
            "mapping", "match", "matched", "materialized", "maxvalue", "merge", "merge_action",
            "method", "minute", "minvalue", "mode", "month", "move", "name", "names", "national",
            "natural", "nchar", "nested", "new", "next", "nfc", "nfd", "nfkc", "nfkd", "no", "none",
            "normalize", "normalized", "not", "nothing", "notify", "notnull", "nowait", "null",
            "nullif", "nulls", "numeric", "object", "of", "off", "offset", "oids", "old", "omit",
            "on", "only", "operator", "option", "options", "or", "order", "ordinality", "others",
            "out", "outer", "over", "overlaps", "overlay", "overriding", "owned", "owner",
            "parallel", "parameter", "parser", "partial", "partition", "passing", "password",
            "path", "placing", "plan", "plans", "policy", "position", "preceding", "precision",
            "prepare", "prepared", "preserve", "primary", "prior", "privileges", "procedural",
            "procedure", "procedures", "program", "publication", "quote", "quotes", "range", "read",
            "real", "reassign", "recheck", "recursive", "ref", "references", "referencing",
            "refresh", "reindex", "relative", "release", "rename", "repeatable", "replace",
            "replica", "reset", "restart", "restrict", "return", "returning", "returns", "revoke",
            "right", "role", "rollback", "rollup", "routine", "routines", "row", "rows", "rule",
            "savepoint", "scalar", "schema", "schemas", "scroll", "search", "second", "security",
            "select", "sequence", "sequences", "serializable", "server", "session", "session_user",
            "set", "setof", "sets", "share", "show", "similar", "simple", "skip", "smallint",
            "snapshot", "some", "source", "sql", "stable", "standalone", "start", "statement",
            "statistics", "stdin", "stdout", "storage", "stored", "strict", "string", "strip",
            "subscription", "substring", "support", "symmetric", "sysid", "system", "system_user",
            "table", "tables", "tablesample", "tablespace", "target", "temp", "template",
            "temporary", "text", "then", "ties", "time", "timestamp", "to", "trailing",
            "transaction", "transform", "treat", "trigger", "trim", "true", "truncate", "trusted",
            "type", "types", "uescape", "unbounded", "uncommitted", "unconditional", "unencrypted",
            "union", "unique", "unknown", "unlisten", "unlogged", "until", "update", "user",
            "using", "vacuum", "valid", "validate", "validator", "value", "values", "varchar",
            "variadic", "varying", "verbose", "version", "view", "views", "volatile", "when",
            "where", "whitespace", "window", "with", "within", "without", "work", "wrapper",
            "write", "xml", "xmlattributes", "xmlconcat", "xmlelement", "xmlexists", "xmlforest",
            "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot", "xmlserialize", "xmltable", "year",
            "yes", "zone",
        ];
        let mut wrong = Vec::new();
        for kw in KEYWORDS {
            for arg in ["'b'", "$1"] {
                let sql = format!("SELECT turso_branch_delete({arg}::{kw})");
                let slow = slow(&sql);
                let fast = fast_branch_call(&sql);
                if (fast.is_some() && fast != slow) || branch_call(&sql) != slow {
                    wrong.push(sql);
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {} keyword casts are read unlike libpg_query, e.g. {:?}",
            wrong.len(),
            2 * KEYWORDS.len(),
            &wrong[..wrong.len().min(8)]
        );
    }

    /// A cast that changes the value or the type is not dropped: the statement is not a branch
    /// call, so it reaches the engine and fails there as PostgreSQL fails it (in PostgreSQL
    /// `'feature'::char` is 'f' and `'abc'::varchar(2)` is 'ab'; `::int`, `::bytea` and an unknown
    /// type are not text). A cast to text, or to varchar with no length, changes nothing and is
    /// read through. A quoted function name keeps its case, as in PostgreSQL, so an uppercase one
    /// names no branch function (wire review 1 item 3).
    #[test]
    fn only_a_cast_that_changes_nothing_is_read_through() {
        use PgBranchArg::{Param, Text};
        let refused = [
            "SELECT turso_branch_create('abc'::varchar(2))",
            "SELECT turso_branch_create('x'::char)",
            "SELECT turso_branch_create('x'::character)",
            "SELECT turso_branch_create('feature'::char(1))",
            "SELECT turso_branch_create('b'::character varying(1))",
            "SELECT turso_branch_create('b'::int)",
            "SELECT turso_branch_create('b'::bytea)",
            "SELECT turso_branch_create('b'::nosuchtype)",
            "SELECT turso_branch_create('b'::text[])",
            "SELECT turso_branch_create('b'::public.text)",
            "SELECT turso_branch_create('b'::\"TEXT\")",
            "SELECT turso_branch_create('b'::from)",
            "SELECT turso_branch_create($1::int)",
            "SELECT turso_branch_delete('prod'::where)",
            "SELECT \"TURSO_BRANCH_CREATE\"('b')",
            "SELECT \"turso_branch_Create\"('b')",
        ];
        for sql in refused {
            assert_eq!(fast_branch_call(sql), None, "fast path, {sql:?}");
            assert_eq!(slow(sql), None, "libpg_query, {sql:?}");
            assert_eq!(branch_call(sql), None, "branch_call, {sql:?}");
        }
        let create = "turso_branch_create";
        let b = || vec![Text("b".into())];
        let fast_forms = [
            "SELECT turso_branch_create('b'::text)",
            "SELECT turso_branch_create('b'::TEXT)",
            "SELECT turso_branch_create('b' :: varchar)",
            "SELECT turso_branch_create('b'::character varying)",
            "SELECT turso_branch_create('b'::CHARACTER\n VARYING)",
        ];
        for sql in fast_forms {
            assert_eq!(
                fast_branch_call(sql),
                call(create, b()),
                "fast path, {sql:?}"
            );
            assert_eq!(slow(sql), call(create, b()), "libpg_query, {sql:?}");
        }
        let slow_forms = [
            ("SELECT turso_branch_create('b'::pg_catalog.text)", b()),
            ("SELECT turso_branch_create('b'::\"text\")", b()),
            ("SELECT turso_branch_create('b'::varchar::text)", b()),
            (
                "SELECT turso_branch_create(true::text)",
                vec![Text("true".into())],
            ),
            ("SELECT turso_branch_create($1::varchar)", vec![Param(1)]),
            ("SELECT \"turso_branch_create\"('b')", b()),
        ];
        for (sql, args) in slow_forms {
            assert_eq!(branch_call(sql), call(create, args), "{sql:?}");
        }
    }
}
