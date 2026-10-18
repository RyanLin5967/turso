//! SQL dialects.
//!
//! The [`Dialect`] trait is the boundary between the engine and the SQL
//! dialect a frontend speaks. The engine owns the mechanics — pages,
//! B-trees, the `sqlite_schema` table itself, bytecode — and consults the
//! dialect wherever the meaning of SQL text is dialect-specific: parsing
//! statements into the engine AST and interpreting persisted schema text.
//! The [`sqlite`] module owns [`SqliteDialect`], the SQLite
//! implementation, and the catalog tables that ship with every Turso
//! build (`pragma_*`, `json_each`/`json_tree`, `sqlite_dbpage`,
//! `btree_dump`, `sqlite_turso_types`).

pub mod sqlite;

pub use sqlite::SqliteDialect;

/// What a RENAME or an ALTER did to a table, for `Dialect::format_changed_table_sql`.
pub enum TableChange<'a> {
    /// RENAME rebuilt the table's CREATE statement.
    Rewritten(&'a turso_parser::ast::Stmt),
    /// ADD/DROP COLUMN changed the table in place.
    Altered(&'a crate::schema::BTreeTable),
}

/// SQL dialect layered on top of the engine.
///
/// Every [`crate::Database`] carries a dialect, supplied explicitly by
/// every open path and fixed for the lifetime of the database;
/// SQLite-compatible callers pass [`SqliteDialect`]. Initial statement
/// preparation, re-preparation, and every schema load go through this
/// interface.
pub trait Dialect: Send + Sync + 'static {
    /// Stable identifier for this dialect (e.g. "sqlite", "postgres").
    ///
    /// A database file must always be opened with the same dialect it was
    /// created with; the process-wide database registry uses this name to
    /// reject an open whose dialect differs from the already-open instance.
    fn name(&self) -> &'static str;

    /// Parse the first statement in `sql` into the engine AST.
    ///
    /// Returns the parsed command, if any, and the number of input bytes
    /// consumed. The engine uses the same method for initial preparation and
    /// re-preparation, so dialect-specific SQL remains valid after schema or
    /// connection compilation state changes. Implementations must accept the
    /// canonical SQLite text produced by the engine AST formatter because
    /// engine-generated and AST-only statements use that representation.
    fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)>;

    /// Parse `sql` again for a statement the engine re-prepares on `conn` (after a
    /// prepare-generation bump or another session's schema change), so a frontend whose
    /// translation needs the connection (its schema, for a column lookup: DELETE ... USING, a
    /// positional INSERT with DEFAULT, an alias column list) re-translates as its own prepare did
    /// (wire-rework review E-M2). Default: [`Dialect::parse`], for dialects that need no
    /// connection.
    fn parse_for_reprepare(
        &self,
        sql: &str,
        _conn: &crate::sync::Arc<crate::Connection>,
    ) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
        self.parse(sql)
    }

    /// Parse a `sqlite_schema` `type='table'` row's SQL into a table
    /// definition.
    ///
    /// Rows written by internal engine paths (sequence backing tables,
    /// `sqlite_sequence`) are plain SQLite text and carry no frontend
    /// marker, so every implementation must fall back to SQLite parsing
    /// for text it does not recognize as its own.
    fn parse_table_sql(
        &self,
        sql: &str,
        root_page: i64,
    ) -> crate::Result<crate::schema::BTreeTable>;

    /// Decode a storage-backed table's persisted SQL into its `CREATE TABLE`
    /// AST.
    ///
    /// Unlike [`Dialect::parse`], this method receives SQL read from
    /// `sqlite_schema` and must recognize the representation produced by
    /// [`Dialect::format_table_sql`] and
    /// [`Dialect::format_rewritten_table_sql`]. Internal engine tables use
    /// plain SQLite text, so implementations must retain the same SQLite
    /// fallback required by [`Dialect::parse_table_sql`].
    fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt>;

    /// Recover SQL that can be prepared to recreate a persisted table.
    ///
    /// Dialects that wrap original frontend DDL in their stored representation
    /// must unwrap it here so replay preserves that DDL. The returned statement
    /// must create the table in the connection's main schema, even when the
    /// persisted statement originally qualified the source database. Unmarked
    /// internal engine tables must retain the SQLite fallback used by the
    /// schema parsing methods.
    fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String>;

    /// `table_sql_for_replay` with the syntax the returned text is in (dialect design REVISION 2 item 4):
    /// VACUUM prepares the text with that parser and stores the row's own bytes verbatim. REQUIRED, with
    /// no default (dialect code review MED-4: a default labelled every replayed row SQLite, which is wrong
    /// for any dialect whose replay text is its own): a dialect whose replay text is SQLite returns
    /// `StoredSql::sqlite`, a frontend whose replay text is its own DDL `StoredSql::client` for those rows.
    fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql>;

    /// Produce the SQL text to store in `sqlite_schema` for a
    /// `CREATE TABLE`.
    ///
    /// `input` is the original statement text as the user wrote it, in the
    /// frontend's dialect; `tbl_name` and `body` are the translated AST.
    /// The SQLite dialect formats canonical SQLite text from the AST; a
    /// frontend dialect typically stores `input` with a marker it can
    /// recognize in [`Dialect::parse_table_sql`].
    fn format_table_sql(
        &self,
        input: &str,
        tbl_name: &turso_parser::ast::QualifiedName,
        body: &turso_parser::ast::CreateTableBody,
    ) -> crate::Result<String>;

    /// Produce stored SQL after the engine rewrites a `CREATE TABLE` AST.
    ///
    /// Schema rewrites cannot reuse the original frontend text because it no
    /// longer describes the rewritten table. Dialects that need syntax beyond
    /// a marker around canonical SQL can override this to render their native
    /// table definition from the rewritten AST.
    fn format_rewritten_table_sql(&self, stmt: &turso_parser::ast::Stmt) -> crate::Result<String> {
        let turso_parser::ast::Stmt::CreateTable { tbl_name, body, .. } = stmt else {
            return Err(crate::LimboError::InternalError(
                "format_rewritten_table_sql requires CREATE TABLE".to_string(),
            ));
        };
        self.format_table_sql(&stmt.to_string(), tbl_name, body)
    }

    /// Produce stored SQL after `ALTER TABLE ... ADD COLUMN` or `DROP COLUMN` changed `table` in
    /// place. The default stores the table's canonical SQLite text (`BTreeTable::to_sql`), as
    /// before. A dialect whose `parse_table_sql` marks properties on its own tables (a PostgreSQL
    /// frontend's NOT NULL rowid alias, `BTreeTable::rowid_alias_not_null`) overrides this, so the
    /// stored text still carries them through a reparse: a reopen, a restart, a branch's first
    /// connect (fastest-engine; engine review 20 HIGH 1).
    fn format_altered_table_sql(&self, table: &crate::schema::BTreeTable) -> crate::Result<String> {
        Ok(table.to_sql())
    }

    /// Whether `sql`, a stored `sqlite_schema` table row, is in this dialect's own form (dialect design
    /// REVISION 2 item 8): the schema loader records the answer on the table (`BTreeTable::dialect_owned`),
    /// so a RENAME or ALTER rewrites the row in the same form it was in. Required, with no default: a
    /// default of "none" would canonicalise a frontend's own rows on ALTER, and one of "all" would
    /// re-mark the engine's rows (review dialect LOW-9), so each dialect must say. SQLite owns none.
    fn owns_stored_table_sql(&self, sql: &str) -> bool;

    /// The stored text of a table row that a RENAME or an ALTER changed (dialect design REVISION 1 item 2,
    /// REVISION 2 item 8): a row stays in the form it was in. `dialect_owned` is the row's owner as the
    /// loader recorded it (`BTreeTable::dialect_owned`). An engine row (not the dialect's) is written as
    /// canonical SQLite; the dialect's own row through its own formatters (`format_rewritten_table_sql`,
    /// `format_altered_table_sql`). The engine calls only this.
    fn format_changed_table_sql(&self, dialect_owned: bool, change: TableChange<'_>) -> crate::Result<String> {
        match (dialect_owned, change) {
            (false, TableChange::Rewritten(stmt)) => Ok(stmt.to_string()),
            (false, TableChange::Altered(table)) => Ok(table.to_sql()),
            (true, TableChange::Rewritten(stmt)) => self.format_rewritten_table_sql(stmt),
            (true, TableChange::Altered(table)) => self.format_altered_table_sql(table),
        }
    }

    /// Install the dialect's catalog tables into a freshly constructed
    /// schema.
    ///
    /// Called by [`crate::schema::Schema::with_options`] on every schema
    /// construction and rebuild, so catalog tables survive rebuilds
    /// structurally instead of being re-registered by hand. The SQLite
    /// dialect registers the standard built-in catalog here; other
    /// dialects typically compose with it via
    /// [`sqlite::register_builtin_catalog`] and then add their own tables
    /// (constructed with [`crate::VirtualTable::new_internal`], which
    /// requires no connection).
    fn register_catalog(
        &self,
        schema: &mut crate::schema::Schema,
        enable_custom_types: bool,
    ) -> crate::Result<()>;

    /// Resolve a function name in user SQL to the engine's function IR.
    ///
    /// The dialect owns its scalar function surface: the SQLite dialect
    /// resolves the built-in set, another dialect resolves its own —
    /// mapping names onto engine primitives where it wants them (usually
    /// by composing with [`sqlite::resolve_builtin_function`]) and onto
    /// [`crate::Func::Dialect`] for functions it executes
    /// itself via [`Dialect::exec_scalar_function`]. Consulted
    /// before extension functions; engine-generated helper statements
    /// always resolve with SQLite semantics instead.
    fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::Func>>;

    /// Execute a dialect scalar function at runtime.
    ///
    /// Receives the connection — unlike extension functions — because
    /// catalog functions (e.g. `pg_get_tabledef`) need to inspect the
    /// schema. Only reached through [`crate::Func::Dialect`],
    /// so a dialect that never resolves to that variant can keep the
    /// default "no such function" error.
    fn exec_scalar_function(
        &self,
        _conn: &crate::Connection,
        name: &str,
        _args: &[crate::Value],
    ) -> crate::Result<crate::Value> {
        Err(crate::LimboError::ParseError(format!(
            "no such function: {name}"
        )))
    }

    /// Whether this dialect needs the custom-type machinery (DECODE/ENCODE,
    /// affinity metadata) regardless of the experimental database flag.
    /// A dialect whose type system leans on custom types (e.g. PostgreSQL)
    /// returns true so its databases never open with the machinery off.
    fn requires_custom_types(&self) -> bool {
        false
    }

    /// A text this dialect reads as a float in its own way, for a CAST to REAL (`Ok`), or refuses
    /// (`Err`); `None` takes SQLite's rules, which read a text that is no number as 0. The default
    /// for every dialect but PostgreSQL's, which reads 'Infinity' as the infinity (the crash batch,
    /// 2026-10-15).
    fn cast_text_to_float(&self, _text: &str) -> Option<crate::Result<f64>> {
        None
    }

    /// Check a table-valued function's arguments before its filter runs: `name` the function,
    /// `idx_num` its plan's argument mask, `args` the values in the order the filter gets them.
    /// The default checks nothing (SQLite's rules); PostgreSQL's refuses an infinite
    /// generate_series bound, which SQLite clamps (the crash batch, 2026-10-15).
    fn check_table_function_args(&self, _name: &str, _idx_num: i64, _args: &[crate::Value]) -> crate::Result<()> {
        Ok(())
    }
}

/// The SQLite builtin function names, read from `sqlite::resolve_builtin_function`'s own source (dialect
/// design REVISION 2 item 7, R10), with whether each arm sits under `#[cfg(feature = "json")]`. Read from the
/// source, not a typed list, so a builtin added there is in the contract the day it lands. Every quoted
/// name in a match-arm pattern of that function's top-level match counts (alternations included).
#[cfg(any(test, feature = "dialect_contract"))]
#[doc(hidden)]
pub fn sqlite_builtin_names() -> Vec<(String, bool)> {
    const SRC: &str = include_str!("sqlite.rs");
    let Some(start) = SRC.find("pub fn resolve_builtin_function(") else {
        return Vec::new();
    };
    // The function body: from its first `{` to the matching `}`, skipping strings (the arms hold
    // format strings with braces) and line comments.
    let bytes = SRC.as_bytes();
    let Some(open) = SRC[start..].find('{').map(|i| start + i) else {
        return Vec::new();
    };
    let (mut depth, mut i, mut end) = (0i32, open, SRC.len());
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let mut names = Vec::new();
    let mut json = false;
    for line in SRC[open..end].lines() {
        let t = line.trim_start();
        if t.starts_with("#[cfg(feature = \"json\")]") {
            json = true;
            continue;
        }
        if t.starts_with('"') {
            if let Some(arrow) = t.find("=>") {
                let pattern = &t[..arrow];
                let pattern = pattern.split(" if ").next().unwrap_or(pattern);
                for part in pattern.split('|') {
                    let p = part.trim().trim_matches('"');
                    if !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                        names.push((p.to_string(), json));
                    }
                }
            }
        }
        if !t.starts_with("#[") && !t.is_empty() {
            json = false;
        }
    }
    names
}

/// The builtin (name, arity) pairs that `dialect` resolves differently from SQLite (R10: functions always
/// resolve through the database's dialect, so a dialect must give every SQLite builtin SQLite's meaning).
/// Compared at every arity from 0 to 8: both errors, both `None`, or both `Some` with equal `Func`, where
/// two `Func`s are equal iff their `Debug` forms are (`Func` has no `PartialEq`). Err if the source read
/// yielded fewer than `min_names`: a contract that collected nothing would pass vacuously.
#[cfg(any(test, feature = "dialect_contract"))]
#[doc(hidden)]
pub fn sqlite_builtins_resolved_differently(dialect: &dyn Dialect, min_names: usize) -> Result<Vec<String>, String> {
    let names = sqlite_builtin_names();
    if names.len() < min_names {
        return Err(format!("the builtin source read found {} names, fewer than {min_names}: the read is broken", names.len()));
    }
    let shape = |r: crate::Result<Option<crate::Func>>| match r {
        Err(_) => "Err".to_string(),
        Ok(None) => "None".to_string(),
        Ok(Some(f)) => format!("Some({f:?})"),
    };
    let mut different = Vec::new();
    for (name, _) in &names {
        for arity in 0..=8 {
            let want = shape(sqlite::resolve_builtin_function(name, arity));
            let got = shape(dialect.resolve_function(name, arity));
            if want != got {
                different.push(format!("{name}/{arity}: sqlite {want}, dialect {got}"));
            }
        }
    }
    Ok(different)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeTable;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, IO};
    use std::sync::Arc;

    /// A dialect that counts schema-row parses and strips a `/* test */ `
    /// marker before delegating to SQLite parsing, mirroring how a frontend
    /// dialect recognizes its own stored text and falls back to SQLite for
    /// unmarked rows.
    #[derive(Default)]
    struct TestDialect {
        parse_calls: AtomicUsize,
        statement_parse_calls: AtomicUsize,
    }

    impl Dialect for TestDialect {
        fn name(&self) -> &'static str {
            "test"
        }
        fn owns_stored_table_sql(&self, sql: &str) -> bool {
            sql.starts_with("/* test */ ")
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            self.statement_parse_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(sql) = sql.strip_prefix("test: ") {
                let (cmd, offset) = sqlite::parse(sql)?;
                Ok((cmd, "test: ".len() + offset))
            } else {
                sqlite::parse(sql)
            }
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            self.parse_calls.fetch_add(1, Ordering::SeqCst);
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            sqlite::parse_table_sql_ast(sql)
        }

        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("/* test */ {input}"))
        }

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            if name.eq_ignore_ascii_case("nvl") {
                return sqlite::resolve_builtin_function("coalesce", arg_count);
            }
            if name.eq_ignore_ascii_case("test_add_one") && arg_count == 1 {
                return Ok(Some(crate::function::Func::Dialect(
                    "test_add_one".to_string(),
                )));
            }
            sqlite::resolve_builtin_function(name, arg_count)
        }

        fn exec_scalar_function(
            &self,
            _conn: &crate::Connection,
            name: &str,
            args: &[crate::Value],
        ) -> crate::Result<crate::Value> {
            assert_eq!(name, "test_add_one");
            let crate::Value::Numeric(crate::numeric::Numeric::Integer(v)) = args[0] else {
                return Err(crate::LimboError::InvalidArgument(
                    "test_add_one expects an integer".to_string(),
                ));
            };
            Ok(crate::Value::Numeric(crate::numeric::Numeric::Integer(
                v + 1,
            )))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)?;
            let vtab = crate::VirtualTable::new_internal(
                "test_catalog".to_string(),
                "CREATE TABLE test_catalog (value INTEGER)".to_string(),
                turso_ext::VTabKind::VirtualTable,
                Arc::new(crate::sync::RwLock::new(TestCatalogTable)),
            )?;
            schema.add_virtual_table(Arc::new(vtab))
        }
    }

    /// A frontend dialect that parses ONLY its own client text (marked `client: `) and refuses
    /// everything else, so any engine helper whose SQLite text goes through `Dialect::parse` fails
    /// instead of silently switching dialects (the wire's NO SILENT DIALECT SWITCH class).
    struct ClientOnlyDialect;

    impl Dialect for ClientOnlyDialect {
        fn name(&self) -> &'static str {
            "client-only"
        }
        fn owns_stored_table_sql(&self, _sql: &str) -> bool {
            false
        }
        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            // The tail after a client statement holds no statement (an `execute` loop parses it to
            // find that out), so it is not engine text: no statement, as every real dialect answers.
            if sql.trim().is_empty() {
                return Ok((None, sql.len()));
            }
            let Some(rest) = sql.strip_prefix("client: ") else {
                return Err(crate::LimboError::ParseError(format!("client-only dialect refused engine text: {sql}")));
            };
            let (cmd, offset) = sqlite::parse(rest)?;
            Ok((cmd, "client: ".len() + offset))
        }
        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(sql)
        }
        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(input.strip_prefix("client: ").unwrap_or(input).to_string())
        }
        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
        fn register_catalog(&self, schema: &mut crate::schema::Schema, enable_custom_types: bool) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }
    }

    /// The wire's NO SILENT DIALECT SWITCH class (fastest-wire DIALECT-SWITCH-AUDIT, 297bbe33cd):
    /// the engine's own helper statements are SQLite text, parsed as SQLite whatever the
    /// database's dialect, never handed to the frontend's parser (where text it rejects used to fall
    /// back to SQLite, and text it accepts could mean something else). Each case runs an engine
    /// path that prepares helper SQL, on a database whose dialect refuses all but client text: the
    /// pragma table functions, VACUUM, ALTER TABLE's schema rewrite, CDC capture and the MVCC
    /// journal-mode switch. At bcf8e75a0 helpers went through `Dialect::parse`.
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn engine_helper_sql_is_parsed_as_sqlite_whatever_the_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let open = |name: &str| {
            let db = Database::open_file_with_flags(
                io.clone(),
                dir.path().join(name).to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new().with_vacuum(true),
                None,
                Arc::new(ClientOnlyDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("client: CREATE TABLE t(x INTEGER)").unwrap();
            conn.execute("client: INSERT INTO t VALUES (1)").unwrap();
            (db, conn)
        };
        let mut failed = Vec::new();
        let mut case = |what: &str, r: crate::Result<()>| {
            if let Err(e) = r {
                failed.push(format!("{what}: {e}"));
            }
        };
        let (_db, c) = open("pragma.db");
        case("pragma table function", c.prepare("client: SELECT name FROM pragma_table_info('t')").and_then(|mut s| s.run_collect_rows()).map(drop));
        let (_db, c) = open("vacuum.db");
        case("VACUUM", c.execute("client: VACUUM"));
        let (_db, c) = open("alter.db");
        case("ALTER TABLE ADD COLUMN", c.execute("client: ALTER TABLE t ADD COLUMN y INTEGER").and_then(|()| c.execute("client: INSERT INTO t VALUES (2, 3)")));
        let (_db, c) = open("cdc.db");
        case("CDC capture", c.execute("client: PRAGMA capture_data_changes_conn('id')").and_then(|()| c.execute("client: INSERT INTO t VALUES (2)")));
        let (_db, c) = open("mvcc.db");
        case("MVCC switch", c.execute("client: PRAGMA journal_mode = 'mvcc'").and_then(|()| c.execute("client: INSERT INTO t VALUES (2)")));
        assert!(failed.is_empty(), "CLAIM: engine helper SQL went through the frontend dialect's parser: {failed:#?}");
    }

    /// Dialect design R5 (review dialect MED-4): the engine sites that still hand their own SQLite text
    /// to the client path. Under a dialect that refuses all but client text, each must still work:
    /// - DROP COLUMN's view probe (`SELECT * FROM {view}`, vdbe/execute.rs);
    /// - `Connection::pragma()` (`PRAGMA {name}({value})`);
    /// - the stats load at open (stats.rs `STATS_QUERY`, whose error is swallowed, so a refused query left
    ///   the planner with no stats, silently). Its control: the same sequence under SqliteDialect has the
    ///   table's stats after the reopen.
    /// Not covered here (stated): the FTS and toy-vector backing tables (feature- and flag-gated; R3's
    /// reopen covers their storage), and extension SQL (ext/vtab_xconnect.rs, which needs a loaded
    /// extension).
    /// At 846697d35 each went through `Dialect::parse`.
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn the_engine_sites_on_the_client_path_reach_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let open = |name: &str, dialect: Arc<dyn Dialect>| {
            Database::open_file_with_flags(
                io.clone(),
                dir.path().join(name).to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                dialect,
            )
            .unwrap()
        };
        let mut failed = Vec::new();
        {
            let db = open("probe.db", Arc::new(ClientOnlyDialect));
            let c = db.connect().unwrap();
            let r = c
                .execute("client: CREATE TABLE t2(a, b)")
                .and_then(|()| c.execute("client: CREATE VIEW v AS SELECT a FROM t2"))
                .and_then(|()| c.execute("client: ALTER TABLE t2 DROP COLUMN b"));
            if let Err(e) = r {
                failed.push(format!("DROP COLUMN's view probe: {e}"));
            }
            if let Err(e) = c.pragma("table_info", "t2") {
                failed.push(format!("Connection::pragma(): {e}"));
            }
        }
        // The stats load at open, with its control.
        let analyzed = |name: &str, dialect: Arc<dyn Dialect>, prefix: &str| -> crate::Result<bool> {
            {
                let db = open(name, dialect.clone());
                let c = db.connect()?;
                c.execute(format!("{prefix}CREATE TABLE s(a INTEGER)"))?;
                c.execute(format!("{prefix}CREATE INDEX si ON s(a)"))?;
                c.execute(format!("{prefix}INSERT INTO s VALUES (1), (2), (3)"))?;
                c.execute(format!("{prefix}ANALYZE"))?;
            }
            let db = open(name, dialect);
            let c = db.connect()?;
            let has = c.schema.read().analyze_stats.table_stats("s").is_some();
            Ok(has)
        };
        match analyzed("stats-control.db", Arc::new(SqliteDialect), "") {
            Ok(true) => {}
            other => failed.push(format!("premise: under SqliteDialect the reopened database has no stats for s: {other:?}")),
        }
        match analyzed("stats.db", Arc::new(ClientOnlyDialect), "client: ") {
            Ok(true) => {}
            other => failed.push(format!("the stats load at open (STATS_QUERY): the reopened database has no stats for s: {other:?}")),
        }
        assert!(failed.is_empty(), "CLAIM: an engine site handed its own SQLite text to the client parser: {failed:#?}");
    }

    /// `ClientOnlyDialect`, storing tables as a frontend does: `format_table_sql` marks the text
    /// it stores (`/* client */ `), and marked text must be the dialect's own (`client: ...`), as a
    /// frontend parses its marked rows with its own parser; unmarked rows fall back to SQLite.
    struct MarkingDialect;

    impl MarkingDialect {
        fn unmark(sql: &str) -> crate::Result<&str> {
            match sql.strip_prefix("/* client */ ") {
                Some(marked) => marked.strip_prefix("client: ").ok_or_else(|| {
                    crate::LimboError::ParseError(format!("a marked row that is not client text: {sql}"))
                }),
                None => Ok(sql),
            }
        }
    }

    impl Dialect for MarkingDialect {
        fn name(&self) -> &'static str {
            "marking"
        }
        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            ClientOnlyDialect.parse(sql)
        }
        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(Self::unmark(sql)?, root_page)
        }
        // Its own rows are the marked ones (the dialect design's owner bit, REVISION 2 item 8).
        fn owns_stored_table_sql(&self, sql: &str) -> bool {
            sql.starts_with("/* client */ ")
        }
        // Its own form for its own rows changed by RENAME or ALTER: marked client text, as it stores
        // them (REVISION 2 item 8: a changed row stays in the form it was in).
        fn format_rewritten_table_sql(&self, stmt: &turso_parser::ast::Stmt) -> crate::Result<String> {
            Ok(format!("/* client */ client: {stmt}"))
        }
        fn format_altered_table_sql(&self, table: &BTreeTable) -> crate::Result<String> {
            Ok(format!("/* client */ client: {}", table.to_sql()))
        }
        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(Self::unmark(sql)?)
        }
        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(Self::unmark(sql)?)
        }
        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("/* client */ {input}"))
        }
        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
        fn register_catalog(&self, schema: &mut crate::schema::Schema, enable_custom_types: bool) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }
    }

    /// The wire's NO SILENT DIALECT SWITCH class, stored side (lead order): an engine helper's
    /// CREATE TABLE (CDC's capture table, MVCC's metadata table) stores SQLite text, never its text
    /// through the frontend's `format_table_sql`, which marks it as the frontend's own and makes the
    /// frontend's parser read it at the next open. Each database is created under `MarkingDialect`
    /// with a client table, the helper's table made, closed, and reopened: it must load and read.
    /// At 7b5093ef0 the helper's text went through the dialect's formatter.
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn an_engine_helper_s_table_stores_sqlite_text_and_reopens_under_any_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let open = |name: &str| {
            Database::open_file_with_flags(
                io.clone(),
                dir.path().join(name).to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(MarkingDialect),
            )
        };
        let mut failed = Vec::new();
        for (name, make_helper_table) in [
            ("cdc.db", "client: PRAGMA capture_data_changes_conn('id')"),
            ("mvcc.db", "client: PRAGMA journal_mode = 'mvcc'"),
        ] {
            {
                let db = open(name).unwrap();
                let conn = db.connect().unwrap();
                conn.execute("client: CREATE TABLE t(x INTEGER)").unwrap();
                if let Err(e) = conn.execute(make_helper_table).and_then(|()| conn.execute("client: INSERT INTO t VALUES (1)")) {
                    failed.push(format!("{name}: premise: the helper's table was not made: {e}"));
                    continue;
                }
            }
            let reopened = open(name).and_then(|db| {
                let conn = db.connect()?;
                conn.prepare("client: SELECT x FROM t")?.run_collect_rows().map(drop)
            });
            if let Err(e) = reopened {
                failed.push(format!("{name}: {e}"));
            }
        }
        assert!(failed.is_empty(), "CLAIM: a database whose engine helper made a table does not reopen under its dialect: {failed:#?}");
    }

    /// R10 (dialect design REVISION 2 item 7): every core dialect gives every SQLite builtin SQLite's meaning,
    /// so resolving functions always through the database's dialect changes nothing for SQLite names. The
    /// names come from `resolve_builtin_function`'s source (`sqlite_builtin_names`); the read must find at
    /// least the count it found at its own commit, and the json-gated subset likewise. Negative control: a
    /// dialect that shadows `abs` must be caught (else the comparison is blind).
    #[test]
    fn every_core_dialect_resolves_every_sqlite_builtin_as_sqlite_does() {
        // Measured at this test's commit (ecetesla2, uwrun engine-step3-reds-1): 214 names, 28 json-gated.
        const MIN_NAMES: usize = 214;
        const MIN_JSON: usize = 28;
        let names = super::sqlite_builtin_names();
        let json = names.iter().filter(|(_, j)| *j).count();
        assert!(names.len() >= MIN_NAMES, "premise: the builtin source read found {} names, fewer than {MIN_NAMES}", names.len());
        assert!(json >= MIN_JSON, "premise: the builtin source read found {json} json-gated names, fewer than {MIN_JSON}");
        struct ShadowAbs;
        impl Dialect for ShadowAbs {
            fn name(&self) -> &'static str {
                "shadow-abs"
            }
            fn owns_stored_table_sql(&self, _sql: &str) -> bool {
                false
            }
            fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
                sqlite::parse(sql)
            }
            fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
                BTreeTable::from_sql(sql, root_page)
            }
            fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
                sqlite::parse_table_sql_ast(sql)
            }
            // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
            fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
                Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
            }

            fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
                sqlite::table_sql_for_replay(sql)
            }
            fn format_table_sql(
                &self,
                input: &str,
                _tbl_name: &turso_parser::ast::QualifiedName,
                _body: &turso_parser::ast::CreateTableBody,
            ) -> crate::Result<String> {
                Ok(input.to_string())
            }
            fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::function::Func>> {
                if name.eq_ignore_ascii_case("abs") {
                    return sqlite::resolve_builtin_function("sign", arg_count);
                }
                sqlite::resolve_builtin_function(name, arg_count)
            }
            fn register_catalog(&self, schema: &mut crate::schema::Schema, enable_custom_types: bool) -> crate::Result<()> {
                sqlite::register_builtin_catalog(schema, enable_custom_types)
            }
        }
        let shadow = super::sqlite_builtins_resolved_differently(&ShadowAbs, MIN_NAMES).unwrap();
        assert!(
            shadow.iter().any(|d| d.starts_with("abs/")),
            "premise (negative control): a dialect shadowing abs was not caught: {shadow:?}"
        );
        let dialects: [(&str, &dyn Dialect); 5] = [
            ("SqliteDialect", &SqliteDialect),
            ("ClientOnlyDialect", &ClientOnlyDialect),
            ("MarkingDialect", &MarkingDialect),
            ("ReparseDialect", &ReparseDialect::default()),
            ("StrictTestDialect", &StrictTestDialect),
        ];
        let mut failed = Vec::new();
        for (name, d) in dialects {
            match super::sqlite_builtins_resolved_differently(d, MIN_NAMES) {
                Ok(diff) if diff.is_empty() => {}
                Ok(diff) => failed.push(format!("{name}: {} pairs differ, first {:?}", diff.len(), &diff[..diff.len().min(4)])),
                Err(e) => failed.push(format!("{name}: {e}")),
            }
        }
        assert!(failed.is_empty(), "CLAIM: a core dialect resolves a SQLite builtin differently from SQLite: {failed:#?}");
    }

    /// R11 (dialect design REVISION 2 item 9): a frontend's canonical SQLite text never creates a table (a
    /// frontend makes its tables through its own parser, as the wire does with run_pg_statement): a CREATE
    /// TABLE, CTAS included, under `SqlKind::frontend()` is refused with a named error and stores nothing.
    /// Control: the same CREATE under `SqlKind::client()` works.
    #[test]
    fn a_frontend_create_table_is_refused() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "frontend-create.db", Arc::new(SqliteDialect)).unwrap();
        let c = db.connect().unwrap();
        let run = |sql: &str, kind: crate::SqlKind| {
            c.prepare_with(sql, kind, crate::Origin::Root).and_then(|mut s| s.run_ignore_rows())
        };
        let mut failed = Vec::new();
        if let Err(e) = run("CREATE TABLE ok(a)", crate::SqlKind::client()) {
            failed.push(format!("premise (control): a client CREATE TABLE failed: {e}"));
        }
        for sql in ["CREATE TABLE f(a)", "CREATE TABLE g AS SELECT 1 AS a"] {
            match run(sql, crate::SqlKind::frontend()) {
                Err(e) if e.to_string().contains("frontend") => {}
                other => failed.push(format!("{sql}: not refused by name: {other:?}")),
            }
        }
        let rows = c
            .prepare("SELECT name FROM sqlite_schema WHERE name IN ('f', 'g')")
            .and_then(|mut s| s.run_collect_rows())
            .unwrap();
        if !rows.is_empty() {
            failed.push(format!("a refused frontend CREATE stored a row: {rows:?}"));
        }
        assert!(failed.is_empty(), "CLAIM: a frontend's text created a table: {failed:#?}");
    }

    /// R3 (dialect design REVISION 2 item 1; reviews dialect HIGH-2, adversary-2 H1): VACUUM keeps every
    /// schema row BYTE FOR BYTE. Under MarkingDialect (which marks the client rows it stores), a database with a
    /// marked client table, an AUTOINCREMENT + UNIQUE table (whose CREATE also writes an autoindex, a sequence
    /// table and sqlite_sequence), a composite-key table, an index, a view, a trigger and an ALTERed table is
    /// VACUUMed. `SELECT type, name, tbl_name, sql FROM sqlite_schema` must be identical before, after, and
    /// after a reopen. Premise: the marking dialect marked the client rows. At 846697d35 VACUUM replayed the
    /// unmarked replay text through prepare_sqlite and stored it unmarked.
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_keeps_every_schema_row_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let path = dir.path().join("r3.db");
        let open = || {
            Database::open_file_with_flags(
                io.clone(),
                path.to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new().with_vacuum(true).with_views(true),
                None,
                Arc::new(MarkingDialect),
            )
            .unwrap()
        };
        let rows = |c: &Arc<crate::Connection>| {
            c.prepare("client: SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name")
                .and_then(|mut s| s.run_collect_rows())
                .map(|rows| rows.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>())
        };
        let before = {
            let db = open();
            let c = db.connect().unwrap();
            for sql in [
                "client: CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, u TEXT UNIQUE, w INTEGER)",
                "client: CREATE TABLE p(a, b, PRIMARY KEY (a, b))",
                "client: CREATE TABLE plain(x INTEGER)",
                "client: CREATE INDEX ti ON t(w)",
                "client: CREATE VIEW tv AS SELECT id FROM t",
                "client: CREATE TRIGGER tt AFTER INSERT ON plain BEGIN UPDATE t SET w = 0 WHERE id = new.x; END",
                "client: ALTER TABLE plain ADD COLUMN y INTEGER",
                "client: INSERT INTO t(u, w) VALUES ('a', 1), ('b', 2)",
                "client: INSERT INTO p VALUES (1, 2)",
                "client: INSERT INTO plain VALUES (1, 2)",
            ] {
                c.execute(sql).unwrap_or_else(|e| panic!("premise: {sql}: {e}"));
            }
            let before = rows(&c).unwrap();
            assert!(
                before.iter().any(|r| r.contains("/* client */")),
                "premise: the marking dialect marked no client row: {before:#?}"
            );
            if let Err(e) = c.execute("client: VACUUM") {
                panic!("CLAIM: VACUUM failed: {e}");
            }
            let after = rows(&c).unwrap();
            assert_eq!(before, after, "CLAIM: VACUUM changed a schema row's bytes");
            before
        };
        let db = open();
        let c = db.connect().unwrap();
        assert_eq!(before, rows(&c).unwrap(), "CLAIM: after VACUUM and a reopen, a schema row's bytes changed");
    }

    /// R7 (dialect design REVISION 1 item 2, REVISION 2 item 8; review dialect LOW-9): RENAME and ALTER never
    /// change whose form a row is in. Under MarkingDialect, an engine-made table is stored plain (step 3);
    /// a client's RENAME of it, and a client's ADD COLUMN on it, must keep it plain, so it reopens (a marked
    /// row must be the dialect's own text, which this one is not). Control: the client table renamed beside it
    /// reopens. At b9b7f34ea RENAME re-formatted every row through the dialect (`format_rewritten_table_sql`),
    /// marking the engine's row.
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn rename_and_alter_keep_an_engine_row_plain() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let path = dir.path().join("r7.db");
        let open = || {
            Database::open_file_with_flags(
                io.clone(),
                path.to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(MarkingDialect),
            )
            .unwrap()
        };
        {
            let db = open();
            let c = db.connect().unwrap();
            c.execute_engine(crate::connection::EngineSql::new("CREATE TABLE helper(x INTEGER)"), crate::Origin::Root)
                .expect("premise: the engine made its table");
            c.execute("client: CREATE TABLE mine(y INTEGER)").expect("premise: the client made its table");
            let helper_row = c
                .prepare("client: SELECT sql FROM sqlite_schema WHERE name = 'helper'")
                .and_then(|mut s| s.run_collect_rows())
                .unwrap();
            assert!(
                !format!("{helper_row:?}").contains("/* client */"),
                "premise: the engine's row is stored plain: {helper_row:?}"
            );
            for sql in [
                "client: ALTER TABLE helper RENAME TO helper2",
                "client: ALTER TABLE helper2 ADD COLUMN z INTEGER",
                "client: ALTER TABLE mine RENAME TO mine2",
            ] {
                c.execute(sql).unwrap_or_else(|e| panic!("premise: {sql}: {e}"));
            }
        }
        let reopened = open().connect().and_then(|c| {
            c.prepare("client: SELECT x, z FROM helper2")?.run_collect_rows()?;
            c.prepare("client: SELECT y FROM mine2")?.run_collect_rows()?;
            c.prepare("client: SELECT name, sql FROM sqlite_schema WHERE name IN ('helper2', 'mine2') ORDER BY name")?
                .run_collect_rows()
        });
        let rows = match reopened {
            Ok(rows) => format!("{rows:?}"),
            Err(e) => panic!("CLAIM: RENAME or ALTER changed whose form a row is in, so the database did not reopen: {e}"),
        };
        let (helper, mine) = rows.split_once("mine2").unwrap_or((rows.as_str(), ""));
        assert!(mine.contains("/* client */"), "premise (control): the client's renamed row is still the dialect's: {rows}");
        assert!(!helper.contains("/* client */"), "CLAIM: RENAME or ALTER marked the engine's row as the dialect's: {rows}");
    }

    /// R7b (dialect code review MED-1): the halves R7 could not see. R7's later ADD COLUMN rewrites the
    /// renamed engine row from the in-memory owner bit, which erased a mark the RENAME made, so forcing
    /// every RENAME owner site to "owned" left R7 green. Here, apart: (a) a client's RENAME ALONE of an
    /// engine-made table keeps its row plain, read straight after and after a reopen; (b) a client's ADD
    /// COLUMN on a client table keeps its row the dialect's (marked).
    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn a_rename_alone_keeps_an_engine_row_plain_and_add_column_keeps_a_client_row_marked() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let path = dir.path().join("r7b.db");
        let open = || {
            Database::open_file_with_flags(
                io.clone(),
                path.to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new(),
                None,
                Arc::new(MarkingDialect),
            )
            .unwrap()
        };
        let row = |c: &Arc<Connection>, name: &str| {
            c.prepare(format!("client: SELECT sql FROM sqlite_schema WHERE name = '{name}'"))
                .and_then(|mut s| s.run_collect_rows())
                .map(|r| format!("{r:?}"))
        };
        let mut failed: Vec<String> = Vec::new();
        {
            let db = open();
            let c = db.connect().unwrap();
            c.execute_engine(crate::connection::EngineSql::new("CREATE TABLE helper(x INTEGER)"), crate::Origin::Root)
                .expect("premise: the engine made its table");
            c.execute("client: CREATE TABLE mine(y INTEGER)").expect("premise: the client made its table");
            c.execute("client: ALTER TABLE helper RENAME TO helper2").expect("premise: the RENAME ran");
            c.execute("client: ALTER TABLE mine ADD COLUMN w INTEGER").expect("premise: the ADD COLUMN ran");
            match row(&c, "helper2") {
                Ok(r) if !r.contains("/* client */") && r.contains("helper2") => {}
                other => failed.push(format!("(a) straight after the RENAME alone, the engine's row is {other:?}, want plain")),
            }
            match row(&c, "mine") {
                Ok(r) if r.contains("/* client */") && r.contains(" w ") => {}
                other => failed.push(format!("(b) after ADD COLUMN, the client's row is {other:?}, want marked, with w")),
            }
        }
        let c = open().connect().unwrap();
        match c.prepare("client: SELECT x FROM helper2").and_then(|mut s| s.run_collect_rows()) {
            Ok(_) => {}
            Err(e) => failed.push(format!("(a) after a reopen, helper2 does not read: {e}")),
        }
        match row(&c, "helper2") {
            Ok(r) if !r.contains("/* client */") => {}
            other => failed.push(format!("(a) after a reopen, the engine's row is {other:?}, want plain")),
        }
        match row(&c, "mine") {
            Ok(r) if r.contains("/* client */") => {}
            other => failed.push(format!("(b) after a reopen, the client's row is {other:?}, want marked")),
        }
        assert!(failed.is_empty(), "CLAIM: a RENAME alone or an ADD COLUMN changed whose form a row is in: {failed:#?}");
    }

    /// The frontend-context slot (dialect design REVISION 1, fastest-wire's M4): set once, read back as the
    /// frontend's own type, refused on a second set, and gone once the frontend drops its session (the
    /// connection holds it weakly). A new API, so no red at a base: the claims are pinned here.
    #[test]
    fn a_frontend_context_is_set_once_and_held_weakly() {
        struct Session(u32);
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "frontend-context.db", Arc::new(SqliteDialect)).unwrap();
        let c = db.connect().unwrap();
        assert!(c.frontend_context().is_none(), "CLAIM: a fresh connection has a frontend context");
        let session: std::sync::Arc<dyn std::any::Any + Send + Sync> = std::sync::Arc::new(Session(7));
        c.set_frontend_context(std::sync::Arc::downgrade(&session)).unwrap();
        let got = c.frontend_context().expect("CLAIM: the context set is not read back");
        assert_eq!(got.downcast_ref::<Session>().map(|s| s.0), Some(7), "CLAIM: the context is not the frontend's own type");
        drop(got);
        let other: std::sync::Arc<dyn std::any::Any + Send + Sync> = std::sync::Arc::new(Session(8));
        assert!(c.set_frontend_context(std::sync::Arc::downgrade(&other)).is_err(), "CLAIM: a second set was not refused");
        assert_eq!(
            c.frontend_context().and_then(|g| g.downcast_ref::<Session>().map(|s| s.0)),
            Some(7),
            "CLAIM: a refused second set replaced the first"
        );
        drop(session);
        assert!(c.frontend_context().is_none(), "CLAIM: the connection kept the dropped session alive");
    }

    /// R4 (review dialect MED-3, structural half): every entry gives its statement the kind of the text it
    /// was given, and the statement keeps it (it defaulted to Client in `execute_text`). Type-enforced since
    /// a9afc6c0c (no constructor without a kind); pinned here per entry.
    #[test]
    fn every_entry_s_statement_carries_the_kind_of_its_text() {
        use crate::connection::{EngineSql, Origin, SqlKind, SqlSource};
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "kinds.db", Arc::new(SqliteDialect)).unwrap();
        let c = db.connect().unwrap();
        let source = |s: &crate::Statement| s.kind().source().clone();
        let mut wrong = Vec::new();
        let mut check = |entry: &str, got: crate::Result<SqlSource>, want: SqlSource| match got {
            Ok(g) if g == want => {}
            other => wrong.push(format!("{entry}: {other:?}, want {want:?}")),
        };
        check("prepare", c.prepare("SELECT 1").map(|s| source(&s)), SqlSource::Client);
        check("prepare_sqlite", c.prepare_sqlite("SELECT 1").map(|s| source(&s)), SqlSource::Frontend);
        check("prepare_internal", c.prepare_internal("SELECT 1").map(|s| source(&s)), SqlSource::Frontend);
        check("prepare_engine", c.prepare_engine(EngineSql::new("SELECT 1"), Origin::Root).map(|s| source(&s)), SqlSource::Engine);
        check("prepare_with(client)", c.prepare_with("SELECT 1", SqlKind::client(), Origin::Root).map(|s| source(&s)), SqlSource::Client);
        check("prepare_with(frontend)", c.prepare_with("SELECT 1", SqlKind::frontend(), Origin::Nested).map(|s| source(&s)), SqlSource::Frontend);
        check("consume_stmt", c.consume_stmt("SELECT 1").map(|o| source(&o.expect("a statement").0)), SqlSource::Client);
        check("query", c.query("SELECT 1").map(|o| source(&o.expect("a statement"))), SqlSource::Client);
        let translated = crate::dialect::sqlite::parse("SELECT 1").unwrap().0.unwrap();
        check(
            "prepare_translated_cmd_with_kind(frontend)",
            c.prepare_translated_cmd_with_kind(translated, "SELECT 1", crate::PrepareOptions::default(), SqlKind::frontend(), Origin::Root)
                .map(|s| source(&s)),
            SqlSource::Frontend,
        );
        assert!(wrong.is_empty(), "CLAIM: an entry's statement does not carry its text's kind: {wrong:#?}");
    }

    /// R4b (dialect code review MED-5): the EXECUTE entries carry their text's kind too, the class R4's doc
    /// names (it defaulted to Client in `execute_text`) and R4 did not check: under ClientOnlyDialect, which
    /// refuses any text not marked `client: `, `execute_engine`, `execute_sqlite` and `execute_with(frontend)`
    /// run plain SQLite text, and `execute` runs client text. Control: `execute` of plain text is refused
    /// (client text goes to the dialect). Forcing `execute_text`'s statements to the client kind fails this.
    #[test]
    fn every_execute_entry_runs_its_text_with_its_kind() {
        use crate::connection::{EngineSql, Origin, SqlKind};
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "execute-kinds.db", Arc::new(ClientOnlyDialect)).unwrap();
        let c = db.connect().unwrap();
        assert!(c.execute("CREATE TABLE plain(x)").is_err(), "premise (control): execute sends its text to the dialect, which refuses plain text");
        let mut wrong = Vec::new();
        let mut check = |entry: &str, r: crate::Result<()>| {
            if let Err(e) = r {
                wrong.push(format!("{entry}: {e}"));
            }
        };
        check("execute(client)", c.execute("client: CREATE TABLE t0(x)"));
        check("execute_engine", c.execute_engine(EngineSql::new("CREATE TABLE t1(x); INSERT INTO t1 VALUES (1)"), Origin::Root));
        check("execute_sqlite", c.execute_sqlite("CREATE TABLE t2(x); INSERT INTO t2 VALUES (1)"));
        check("execute_with(frontend)", c.execute_with("CREATE TABLE t3(x); INSERT INTO t3 VALUES (1)", SqlKind::frontend(), Origin::Root));
        check("execute_with(client)", c.execute_with("client: CREATE TABLE t4(x)", SqlKind::client(), Origin::Root));
        assert!(wrong.is_empty(), "CLAIM: an execute entry ran its text under another kind: {wrong:#?}");
    }

    /// R8 (dialect design R8, review dialect LOW-6): an engine statement keeps its kind through a re-prepare,
    /// so a re-prepared engine statement never reaches a strict dialect's parser. Under ClientOnlyDialect an
    /// engine SELECT is prepared, a keyed setting changes (the statement must re-prepare: premise), and it
    /// runs again. A guard: the class was fixed at 7b5093ef0 (the kind on the Statement) and is
    /// type-enforced since a9afc6c0c.
    #[test]
    fn an_engine_statement_keeps_its_kind_through_a_reprepare() {
        use crate::connection::{EngineSql, Origin};
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "reprepare-kind.db", Arc::new(ClientOnlyDialect)).unwrap();
        let c = db.connect().unwrap();
        c.execute("client: CREATE TABLE t(x INTEGER)").unwrap();
        c.execute("client: INSERT INTO t VALUES (1)").unwrap();
        let mut s = c.prepare_engine(EngineSql::new("SELECT x FROM t"), Origin::Root).unwrap();
        assert_eq!(s.run_collect_rows().unwrap().len(), 1, "premise: the engine SELECT ran");
        s.reset().unwrap();
        let reprepares0 = s.metrics().reprepares;
        c.set_cache_size(c.get_cache_size() + 1);
        let rows = s.run_collect_rows();
        assert!(s.metrics().reprepares > reprepares0, "premise: the statement was re-prepared");
        assert!(matches!(&rows, Ok(r) if r.len() == 1), "CLAIM: a re-prepared engine statement reached the client parser: {rows:?}");
    }

    /// A dialect that parks one translation in the middle: resolving the function `park_here()`
    /// signals `parked` and waits for `go`, recording whether `go` (not the bound) ended the park
    /// (review c42b MED-4).
    #[derive(Default)]
    struct ParkingDialect {
        parked: crate::sync::atomic::AtomicBool,
        go: crate::sync::atomic::AtomicBool,
        released_by_go: crate::sync::atomic::AtomicBool,
    }

    impl Dialect for ParkingDialect {
        fn name(&self) -> &'static str {
            "parking"
        }
        fn owns_stored_table_sql(&self, _sql: &str) -> bool {
            false
        }
        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }
        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(sql)
        }
        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(input.to_string())
        }
        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::function::Func>> {
            if name.eq_ignore_ascii_case("park_here") {
                if !self.parked.swap(true, Ordering::SeqCst) {
                    let t = std::time::Instant::now();
                    while !self.go.load(Ordering::SeqCst) && t.elapsed() < std::time::Duration::from_secs(30) {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    self.released_by_go.store(self.go.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                return sqlite::resolve_builtin_function("random", arg_count);
            }
            sqlite::resolve_builtin_function(name, arg_count)
        }
        fn register_catalog(&self, schema: &mut crate::schema::Schema, enable_custom_types: bool) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }
    }

    /// Review c42b MED-4: a program is keyed at the prepare generation read BEFORE its translation
    /// reads the state it keys, so a keyed change landing during translation re-prepares it. The
    /// prepare parks inside translation (at `park_here()`, after `translate` read `mvcc_enabled`);
    /// another thread changes a keyed setting of the same connection (`set_cache_size`, a
    /// change-only setter that bumps the generation and touches no schema); the prepare finishes.
    /// Stepped, the statement must re-prepare. At 7b74fa9fa the key was read at `build`, after the
    /// change, so the program matched and never re-prepared.
    ///
    /// Not the review's journal switch: switching to MVCC bootstraps a metadata table, a schema
    /// change, which re-prepares the parked statement at the base too (by reading, UNVERIFIED), so
    /// that red could not tell the base from the fix. The setter isolates the generation.
    #[test]
    fn a_keyed_change_during_translation_re_prepares_the_program() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(ParkingDialect::default());
        let db = open_db(&io, "park.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        let changer = {
            let conn = conn.clone();
            let dialect = dialect.clone();
            std::thread::spawn(move || {
                let t = std::time::Instant::now();
                while !dialect.parked.load(Ordering::SeqCst) {
                    if t.elapsed() > std::time::Duration::from_secs(10) {
                        dialect.go.store(true, Ordering::SeqCst);
                        return None;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let before = conn.prepare_context_generation();
                conn.set_cache_size(conn.get_cache_size() + 1);
                let after = conn.prepare_context_generation();
                dialect.go.store(true, Ordering::SeqCst);
                Some((before, after))
            })
        };
        let mut stmt = conn.prepare("SELECT park_here(), x FROM t").unwrap();
        let moved = changer.join().unwrap();
        assert!(moved.is_some(), "premise: the prepare never reached park_here()");
        let (before, after) = moved.unwrap();
        assert!(after > before, "premise: the setter moved the prepare generation ({before} -> {after})");
        assert!(
            dialect.released_by_go.load(Ordering::SeqCst),
            "premise: the change landed while the translation was parked (the park ended at its bound)"
        );
        let reprepares0 = stmt.metrics().reprepares;
        let rows = stmt.run_collect_rows();
        assert!(matches!(&rows, Ok(r) if r.len() == 1), "premise: the statement read its row: {rows:?}");
        assert!(
            stmt.metrics().reprepares > reprepares0,
            "CLAIM: a program whose translation straddled a keyed change was not re-prepared"
        );
    }

    /// A dialect that counts its connection-aware re-parses (wire-rework E-M2).
    #[derive(Default)]
    struct ReparseDialect {
        reparses: AtomicUsize,
    }

    impl Dialect for ReparseDialect {
        fn name(&self) -> &'static str {
            "reparse-test"
        }
        fn owns_stored_table_sql(&self, _sql: &str) -> bool {
            false
        }
        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }
        fn parse_for_reprepare(
            &self,
            sql: &str,
            conn: &crate::sync::Arc<crate::Connection>,
        ) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            assert!(!conn.is_closed(), "the re-preparing connection is open");
            self.reparses.fetch_add(1, Ordering::SeqCst);
            sqlite::parse(sql)
        }
        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }
        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(sql)
        }
        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(sql)
        }
        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(input.to_string())
        }
        fn resolve_function(&self, name: &str, arg_count: usize) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
        fn register_catalog(&self, schema: &mut crate::schema::Schema, enable_custom_types: bool) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }
    }

    /// Wire-rework review E-M2 (lead order): a statement the ENGINE re-prepares (here after a real
    /// setting change bumps the prepare generation) is re-parsed through the dialect's
    /// connection-aware hook, with the re-preparing connection, so a frontend can re-translate as
    /// its own prepare did. At b119a4f1e `Statement::reprepare` called `Dialect::parse`, which has
    /// no connection. Premise: the statement was re-prepared.
    #[test]
    fn an_engine_reprepare_reparses_through_the_dialect_with_its_connection() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(ReparseDialect::default());
        let db = open_db(&io, "reparse.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (7)").unwrap();
        let mut stmt = conn.prepare("SELECT x FROM t").unwrap();
        assert_eq!(stmt.run_collect_rows().unwrap().len(), 1, "premise: the SELECT reads its row");
        stmt.reset().unwrap();
        let reprepares0 = stmt.metrics().reprepares;
        conn.set_cache_size(conn.get_cache_size() + 1);
        assert_eq!(stmt.run_collect_rows().unwrap().len(), 1, "premise: the re-prepared SELECT reads its row");
        assert!(stmt.metrics().reprepares > reprepares0, "premise: the statement was re-prepared");
        assert!(
            dialect.reparses.load(Ordering::SeqCst) >= 1,
            "CLAIM: the engine re-prepared without the dialect's connection-aware re-parse"
        );
    }

    /// Stores table definitions in syntax that SQLite cannot parse and always
    /// adds its marker, so replay tests detect repeated storage formatting.
    struct StrictTestDialect;

    impl StrictTestDialect {
        const PREFIX: &'static str = "strict: ";
    }

    impl Dialect for StrictTestDialect {
        fn name(&self) -> &'static str {
            "strict-test"
        }
        fn owns_stored_table_sql(&self, sql: &str) -> bool {
            sql.starts_with(Self::PREFIX)
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            sqlite::parse_table_sql_ast(sql)
        }

        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("{}{input}", Self::PREFIX))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
    }

    /// A one-row catalog table installed by [`TestDialect`],
    /// standing in for a frontend catalog surface like `pg_class`.
    #[derive(Debug)]
    struct TestCatalogTable;

    impl crate::InternalVirtualTable for TestCatalogTable {
        fn name(&self) -> String {
            "test_catalog".to_string()
        }

        fn sql(&self) -> String {
            "CREATE TABLE test_catalog (value INTEGER)".to_string()
        }

        fn open(
            &self,
            _conn: Arc<crate::Connection>,
        ) -> crate::Result<Arc<crate::sync::RwLock<dyn crate::InternalVirtualTableCursor>>>
        {
            Ok(Arc::new(crate::sync::RwLock::new(TestCatalogCursor {
                row: 0,
            })))
        }

        fn best_index(
            &self,
            constraints: &[turso_ext::ConstraintInfo],
            _order_by: &[turso_ext::OrderByInfo],
        ) -> std::result::Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
            Ok(turso_ext::IndexInfo {
                idx_num: 0,
                idx_str: None,
                order_by_consumed: false,
                estimated_cost: 1.0,
                estimated_rows: 1,
                constraint_usages: constraints
                    .iter()
                    .map(|_| turso_ext::ConstraintUsage {
                        argv_index: None,
                        omit: false,
                    })
                    .collect(),
            })
        }
    }

    struct TestCatalogCursor {
        row: usize,
    }

    impl crate::InternalVirtualTableCursor for TestCatalogCursor {
        fn filter(
            &mut self,
            _args: &[crate::Value],
            _idx_str: Option<String>,
            _idx_num: i32,
        ) -> crate::Result<bool> {
            self.row = 0;
            Ok(true)
        }

        fn next(&mut self) -> crate::Result<bool> {
            self.row += 1;
            Ok(self.row < 1)
        }

        fn rowid(&self) -> i64 {
            self.row as i64
        }

        fn column(&self, column: usize) -> crate::Result<crate::Value> {
            match column {
                0 => Ok(crate::Value::Numeric(crate::numeric::Numeric::Integer(42))),
                _ => Ok(crate::Value::Null),
            }
        }
    }

    fn open_db(
        io: &Arc<dyn IO>,
        path: &str,
        dialect: Arc<dyn Dialect>,
    ) -> crate::Result<Arc<Database>> {
        let file = io.open_file(path, OpenFlags::Create, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Database::open(
            io.clone(),
            path,
            crate::OpenOptions::new(dialect).storage(db_file),
        )
    }

    #[test]
    fn schema_load_routes_through_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-load.db", Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        // Reopening the database parses the stored schema row for `t`
        // through the dialect.
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-load.db", dialect.clone()).unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);

        // DDL reparses the schema via the ParseSchema opcode, again through
        // the dialect.
        let conn = db.connect().unwrap();
        let before = dialect.parse_calls.load(Ordering::SeqCst);
        conn.execute("CREATE TABLE u (y INTEGER)").unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) > before);

        // Both tables are usable under the dialect.
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO u VALUES (2)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn dialect_parser_is_used_for_reprepare() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-reprepare.db", dialect.clone()).unwrap();
        let conn = db.connect().unwrap();

        let mut stmt = conn.prepare("test: SELECT 42").unwrap();
        conn.set_full_column_names(true);
        let rows = stmt.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
        assert_eq!(
            stmt.stmt_status(crate::StatementStatusCounter::Reprepare),
            1
        );
        assert_eq!(dialect.statement_parse_calls.load(Ordering::SeqCst), 2);
        conn.close().unwrap();
    }

    #[test]
    fn query_runner_reports_invalid_utf8_once() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "query-runner-invalid-utf8.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let mut runner = conn.query_runner(b"SELECT 1;\xff");

        let Some(Err(crate::LimboError::ParseError(message))) = runner.next() else {
            panic!("invalid UTF-8 must produce a parse error");
        };
        assert!(message.contains("invalid UTF-8"));
        assert!(runner.next().is_none());
        conn.close().unwrap();
    }

    #[test]
    fn query_runner_reports_parse_error_once() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "query-runner-parse-error.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let mut runner = conn.query_runner(b"SELECT * FROM");

        assert!(runner.next().is_some_and(|result| result.is_err()));
        assert!(runner.next().is_none());
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_available_on_every_schema_and_rebuild() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-catalog.db", Arc::new(TestDialect::default())).unwrap();

        let query_catalog = |conn: &Arc<crate::Connection>| -> Vec<Vec<crate::Value>> {
            conn.prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap()
        };

        let conn1 = db.connect().unwrap();
        let conn2 = db.connect().unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );
        assert_eq!(
            query_catalog(&conn2),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );

        // DDL on another connection forces conn1 to rebuild its schema from
        // sqlite_schema; the catalog table must survive because schema
        // construction re-registers it.
        conn2.execute("CREATE TABLE t (x INTEGER)").unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );

        conn1.close().unwrap();
        conn2.close().unwrap();
    }

    #[test]
    fn dialect_catalog_cannot_be_dropped() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-catalog-drop.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        let error = conn.execute("DROP TABLE test_catalog").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table test_catalog may not be dropped"),
            "unexpected error: {error}"
        );

        let new_conn = db.connect().unwrap();
        for catalog_conn in [&conn, &new_conn] {
            let rows = catalog_conn
                .prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
        }
        conn.close().unwrap();
        new_conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_survives_mvcc_recovery() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let path = "dialect-catalog-mvcc-recovery.db";

        {
            let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA journal_mode = mvcc").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect().unwrap();
        let rows = conn
            .prepare("SELECT value FROM test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_available_in_initialized_temp_schema() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-catalog-temp.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();

        for temp_store in ["MEMORY", "FILE"] {
            let conn = db.connect().unwrap();
            conn.execute(format!("PRAGMA temp_store = {temp_store}"))
                .unwrap();
            conn.execute("CREATE TEMP TABLE t (x INTEGER)").unwrap();
            let rows = conn
                .prepare("SELECT value FROM temp.test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![crate::Value::from_i64(42)]]);
            conn.close().unwrap();
        }
    }

    #[test]
    fn create_table_stores_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let dialect = Arc::new(TestDialect::default());
            let db = open_db(&io, "dialect-store.db", dialect).unwrap();
            let conn = db.connect().unwrap();

            // A frontend prepares its translated AST while supplying the
            // original statement text.
            let input = "CREATE TABLE t (x INTEGER)";
            let stmt = match turso_parser::parser::Parser::new(input.as_bytes())
                .next_cmd()
                .unwrap()
                .unwrap()
            {
                turso_parser::ast::Cmd::Stmt(stmt) => stmt,
                other => panic!("unexpected command: {other:?}"),
            };
            conn.prepare_translated_stmt(stmt, input)
                .unwrap()
                .run_ignore_rows()
                .unwrap();

            // The stored schema row carries the dialect marker and the
            // original text.
            let rows = conn
                .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows.len(), 1);
            let stored = rows[0][0].to_string();
            assert_eq!(stored.trim_matches('\''), format!("/* test */ {input}"));
            conn.close().unwrap();
        }

        // Round-trip: reopening parses the marked row back through the
        // dialect and the table stays usable.
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-store.db", dialect.clone()).unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);
        let conn = db.connect().unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        let rows = conn
            .prepare("SELECT x FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        conn.close().unwrap();
    }

    /// A dialect that resolves no functions at all, proving the dialect —
    /// not the engine — owns the function name surface of user SQL.
    struct NoFunctionsDialect;

    impl Dialect for NoFunctionsDialect {
        fn name(&self) -> &'static str {
            "nofuncs"
        }
        fn owns_stored_table_sql(&self, _sql: &str) -> bool {
            false
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(sql)
        }

        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            _input: &str,
            tbl_name: &turso_parser::ast::QualifiedName,
            body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!(
                "CREATE TABLE {} {}",
                tbl_name.name.as_ident(),
                body
            ))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(
            &self,
            _name: &str,
            _arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            Ok(None)
        }
    }

    #[test]
    fn dialect_scalar_function_resolves_and_executes() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-funcs.db", Arc::new(TestDialect::default())).unwrap();
        let conn = db.connect().unwrap();

        // Dialect-provided scalar executes through exec_scalar_function.
        let rows = conn
            .prepare("SELECT test_add_one(41)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(42)
            )]]
        );

        // Built-ins still resolve because the dialect composes with the
        // shared table.
        let rows = conn
            .prepare("SELECT abs(-7)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![crate::Value::Numeric(
                crate::numeric::Numeric::Integer(7)
            )]]
        );

        // Unknown names still error.
        let err = conn.prepare("SELECT no_such_function(1)").unwrap_err();
        assert!(err.to_string().contains("no such function"));
        conn.close().unwrap();
    }

    #[test]
    fn dialect_function_alias_preserves_outer_join() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-outer-join.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE lhs (id INTEGER)").unwrap();
        conn.execute("CREATE TABLE rhs (id INTEGER, value INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO lhs VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO rhs VALUES (1, 0)").unwrap();

        let rows = conn
            .prepare(
                "SELECT lhs.id FROM lhs LEFT JOIN rhs ON rhs.id = lhs.id \
                 WHERE nvl(rhs.value, 1) = 1 ORDER BY lhs.id",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![crate::Value::from_i64(2)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_owns_the_function_surface() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-nofuncs.db", Arc::new(NoFunctionsDialect)).unwrap();
        let conn = db.connect().unwrap();

        // Function-free SQL works, including DDL (whose internal helper
        // statements resolve with SQLite semantics regardless of dialect).
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (-7)").unwrap();

        // A SQLite built-in is not part of this dialect's surface.
        let err = conn.prepare("SELECT abs(x) FROM t").unwrap_err();
        assert!(
            err.to_string().contains("no such function"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }

    #[test]
    fn cdc_generated_functions_bypass_the_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-cdc.db", Arc::new(NoFunctionsDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (7)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (8)").unwrap();
        conn.execute("COMMIT").unwrap();

        let rows = conn
            .prepare(
                "SELECT change_type, table_name, id, change_txn_id \
                 FROM turso_cdc ORDER BY change_id",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], crate::Value::from_i64(1));
        assert_eq!(rows[0][1], crate::Value::build_text("t"));
        assert_eq!(rows[0][2], crate::Value::from_i64(1));
        assert_eq!(rows[1][0], crate::Value::from_i64(2));
        assert_eq!(rows[1][1], crate::Value::Null);
        assert_eq!(rows[1][2], crate::Value::Null);
        assert_eq!(rows[0][3], rows[1][3]);
        assert_eq!(rows[2][0], crate::Value::from_i64(1));
        assert_eq!(rows[2][1], crate::Value::build_text("t"));
        assert_eq!(rows[2][2], crate::Value::from_i64(2));
        assert_eq!(rows[3][0], crate::Value::from_i64(2));
        assert_eq!(rows[3][1], crate::Value::Null);
        assert_eq!(rows[3][2], crate::Value::Null);
        assert_eq!(rows[2][3], rows[3][3]);
        conn.close().unwrap();
    }

    #[test]
    fn alter_table_rewrites_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-alter-table.db", Arc::new(StrictTestDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("ALTER TABLE t RENAME TO u").unwrap();

        let rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 'u'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE u (x INTEGER)"
        );
        conn.execute("INSERT INTO u VALUES (1)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn alter_table_rename_column_decodes_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-alter-column.db", Arc::new(StrictTestDialect)).unwrap();
        let conn = db.connect().unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("ALTER TABLE t RENAME COLUMN x TO y").unwrap();

        let rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t (y INTEGER)"
        );
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(
            conn.prepare("SELECT y FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![crate::Value::from_i64(1)]]
        );
        conn.close().unwrap();
    }

    #[test]
    fn registry_rejects_dialect_mismatch() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(&io, "dialect-mismatch.db", Arc::new(SqliteDialect)).unwrap();

        let err =
            open_db(&io, "dialect-mismatch.db", Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn registry_rejects_default_open_of_dialect_database() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(
            &io,
            "dialect-mismatch-reverse.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();

        let err = open_db(&io, "dialect-mismatch-reverse.db", Arc::new(SqliteDialect)).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn shared_memory_registry_rejects_dialect_mismatch() {
        let name = "dialect-shared-memory-mismatch";
        let _db = Database::open_shared_memory(name, Arc::new(SqliteDialect)).unwrap();

        let err = Database::open_shared_memory(name, Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_into_replays_schema_with_source_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let output_path = dir.path().join("output.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
            .unwrap();

        let output_db = Database::open_file(
            io,
            output_path.to_str().unwrap(),
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let output_conn = output_db.connect().unwrap();
        let schema_rows = output_conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(schema_rows.len(), 1);
        assert_eq!(
            schema_rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t(x INTEGER)"
        );
        assert_eq!(
            output_conn
                .prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![crate::Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_attached_database_strips_source_schema_from_replay() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let attached_path = dir.path().join("attached.db");
        let output_path = dir.path().join("output.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute(format!(
            "ATTACH DATABASE '{}' AS aux",
            attached_path.display()
        ))
        .unwrap();
        conn.execute("CREATE TABLE aux.t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES (42)").unwrap();

        conn.execute(format!("VACUUM aux INTO '{}'", output_path.display()))
            .unwrap();

        let output_db = Database::open_file(
            io,
            output_path.to_str().unwrap(),
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let output_conn = output_db.connect().unwrap();
        assert_eq!(
            output_conn
                .prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![crate::Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn in_place_vacuum_replays_schema_with_source_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.db");
        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        conn.execute("VACUUM").unwrap();

        let schema_rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(schema_rows.len(), 1);
        assert_eq!(
            schema_rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t(x INTEGER)"
        );
        assert_eq!(
            conn.prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![crate::Value::from_i64(42)]]
        );
    }

    /// Mirrors the PG frontend (postgres/frontend/catalog.rs): the tables it creates carry a
    /// marker in their stored SQL, and its `parse_table_sql` sets
    /// `BTreeTable::rowid_alias_not_null` on the tables it recognizes as its own, because every
    /// PostgreSQL primary key is NOT NULL. Unmarked rows keep SQLite semantics.
    struct PgKeyTestDialect;

    impl PgKeyTestDialect {
        const PREFIX: &'static str = "/* pgkey */ ";
        /// The frontend's mark on its own table's canonical SQLite text after ALTER rewrote it
        /// (`format_altered_table_sql`), as the PG frontend's second marker.
        const ALTERED_PREFIX: &'static str = "/* pgkey sqlite */ ";

        fn own(sql: &str) -> Option<&str> {
            sql.strip_prefix(Self::PREFIX)
                .or_else(|| sql.strip_prefix(Self::ALTERED_PREFIX))
        }
    }

    impl Dialect for PgKeyTestDialect {
        fn name(&self) -> &'static str {
            "pgkey-test"
        }
        fn owns_stored_table_sql(&self, sql: &str) -> bool {
            Self::own(sql).is_some()
        }

        fn parse(&self, sql: &str) -> crate::Result<(Option<turso_parser::ast::Cmd>, usize)> {
            sqlite::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> crate::Result<BTreeTable> {
            match Self::own(sql) {
                Some(own) => {
                    let mut table = BTreeTable::from_sql(own, root_page)?;
                    table.rowid_alias_not_null = true;
                    Ok(table)
                }
                None => BTreeTable::from_sql(sql, root_page),
            }
        }

        fn parse_table_sql_ast(&self, sql: &str) -> crate::Result<turso_parser::ast::Stmt> {
            sqlite::parse_table_sql_ast(Self::own(sql).unwrap_or(sql))
        }

        // Its replay text is SQLite (it goes through SQLite's table_sql_for_replay).
        fn table_sql_for_replay_stored(&self, sql: &str) -> crate::Result<crate::connection::StoredSql> {
            Ok(crate::connection::StoredSql::sqlite(self.table_sql_for_replay(sql)?))
        }

        fn table_sql_for_replay(&self, sql: &str) -> crate::Result<String> {
            sqlite::table_sql_for_replay(Self::own(sql).unwrap_or(sql))
        }

        fn format_altered_table_sql(&self, table: &BTreeTable) -> crate::Result<String> {
            if table.rowid_alias_not_null {
                Ok(format!("{}{}", Self::ALTERED_PREFIX, table.to_sql()))
            } else {
                Ok(table.to_sql())
            }
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &turso_parser::ast::QualifiedName,
            _body: &turso_parser::ast::CreateTableBody,
        ) -> crate::Result<String> {
            Ok(format!("{}{input}", Self::PREFIX))
        }

        fn register_catalog(
            &self,
            schema: &mut crate::schema::Schema,
            enable_custom_types: bool,
        ) -> crate::Result<()> {
            sqlite::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(
            &self,
            name: &str,
            arg_count: usize,
        ) -> crate::Result<Option<crate::function::Func>> {
            sqlite::resolve_builtin_function(name, arg_count)
        }
    }

    /// fastest-engine 4c (LEAD-ORDER 6; the 2026-10-06T20:26Z ruling on wire COMPAT 02b7729d3): in
    /// a table whose frontend makes every primary key NOT NULL, an EXPLICIT NULL into the INTEGER
    /// PRIMARY KEY rowid alias raises the NOT NULL constraint (23502 at the wire): a literal, a
    /// bound parameter, a named column, a row of a multi-row INSERT (the statement undoes its
    /// earlier rows and only them), UPDATE SET key = NULL (literal and bound) and an upsert's DO
    /// UPDATE SET key = NULL. An omitted key still takes a new rowid; the key stays the rowid
    /// alias with no index of its own; the rule holds after ALTER TABLE and after a reopen. A plain
    /// SQLite table keeps SQLite's rule: a NULL key takes a new rowid.
    #[test]
    fn an_explicit_null_into_a_frontend_tables_integer_key_raises_not_null() {
        fn not_null(result: &crate::Result<()>) -> bool {
            matches!(
                result,
                Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id"
            )
        }
        fn run_bound(conn: &Arc<crate::Connection>, sql: &str) -> crate::Result<()> {
            let mut stmt = conn.prepare(sql)?;
            stmt.bind_at(std::num::NonZero::new(1).unwrap(), crate::Value::Null)?;
            stmt.run_ignore_rows()
        }
        fn ints(conn: &Arc<crate::Connection>, sql: &str) -> Vec<i64> {
            conn.prepare(sql)
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .iter()
                .map(|row| row[0].as_int().unwrap())
                .collect()
        }

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "pgkey.db", Arc::new(PgKeyTestDialect)).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            let literal = conn.execute("INSERT INTO k VALUES (NULL, 'literal')");
            assert!(
                not_null(&literal),
                "CLAIM: a literal NULL key did not raise NOT NULL: {literal:?}"
            );
            let bound = run_bound(&conn, "INSERT INTO k VALUES (?1, 'bound')");
            assert!(not_null(&bound), "a bound NULL key: {bound:?}");
            let named = conn.execute("INSERT INTO k (id, v) VALUES (NULL, 'named')");
            assert!(not_null(&named), "a NULL key in a named column list: {named:?}");

            conn.execute("BEGIN").unwrap();
            conn.execute("INSERT INTO k VALUES (3, 'three')").unwrap();
            let multi = conn.execute("INSERT INTO k VALUES (5, 'five'), (NULL, 'null')");
            assert!(not_null(&multi), "a NULL key in a multi-row INSERT: {multi:?}");
            conn.execute("COMMIT").unwrap();
            assert_eq!(
                ints(&conn, "SELECT id FROM k ORDER BY id"),
                vec![3],
                "the failed INSERT undid its own earlier row and nothing else"
            );

            conn.execute("INSERT INTO k (v) VALUES ('omitted')").unwrap();
            assert_eq!(
                ints(&conn, "SELECT id FROM k ORDER BY id"),
                vec![3, 4],
                "an omitted key takes a new rowid"
            );
            let update = conn.execute("UPDATE k SET id = NULL WHERE id = 4");
            assert!(not_null(&update), "UPDATE SET key = NULL: {update:?}");
            let update_bound = run_bound(&conn, "UPDATE k SET id = ?1 WHERE id = 4");
            assert!(
                not_null(&update_bound),
                "UPDATE SET key = a bound NULL: {update_bound:?}"
            );
            let upsert = conn.execute(
                "INSERT INTO k VALUES (4, 'again') ON CONFLICT (id) DO UPDATE SET id = NULL",
            );
            assert!(not_null(&upsert), "an upsert's DO UPDATE SET key = NULL: {upsert:?}");
            assert_eq!(
                ints(&conn, "SELECT id FROM k ORDER BY id"),
                vec![3, 4],
                "no refused statement changed a row"
            );
            assert_eq!(
                ints(&conn, "SELECT count(*) FROM k WHERE rowid = id"),
                vec![2],
                "the key is still the rowid alias"
            );
            assert_eq!(
                ints(
                    &conn,
                    "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'k'"
                ),
                vec![0],
                "the key has no index of its own"
            );

            conn.execute("ALTER TABLE k ADD COLUMN w INTEGER").unwrap();
            let after_alter = conn.execute("INSERT INTO k (id, v) VALUES (NULL, 'after alter')");
            assert!(not_null(&after_alter), "after ALTER TABLE: {after_alter:?}");
            conn.close().unwrap();
        }
        {
            let db = open_db(&io, "pgkey.db", Arc::new(PgKeyTestDialect)).unwrap();
            let conn = db.connect().unwrap();
            let reopened = conn.execute("INSERT INTO k (id, v) VALUES (NULL, 'reopened')");
            assert!(not_null(&reopened), "after a reopen: {reopened:?}");
            conn.execute("INSERT INTO k (v) VALUES ('omitted after the reopen')")
                .unwrap();
            assert_eq!(ints(&conn, "SELECT id FROM k ORDER BY id"), vec![3, 4, 5]);
            conn.close().unwrap();
        }
        // Engine review 20 HIGH 1: ALTER TABLE DROP COLUMN stores the table again, and a reopen
        // reparses it.
        {
            let db = open_db(&io, "pgkey.db", Arc::new(PgKeyTestDialect)).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("ALTER TABLE k DROP COLUMN w").unwrap();
            let after_drop = conn.execute("INSERT INTO k (id, v) VALUES (NULL, 'after drop')");
            assert!(not_null(&after_drop), "after ALTER TABLE DROP COLUMN: {after_drop:?}");
            conn.close().unwrap();
        }
        {
            let db = open_db(&io, "pgkey.db", Arc::new(PgKeyTestDialect)).unwrap();
            let conn = db.connect().unwrap();
            let reopened = conn.execute("INSERT INTO k (id, v) VALUES (NULL, 'reopened after drop')");
            assert!(not_null(&reopened), "after DROP COLUMN and a reopen: {reopened:?}");
            conn.close().unwrap();
        }

        let db = open_db(&io, "sqlitekey.db", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO k VALUES (NULL, 'literal')").unwrap();
        run_bound(&conn, "INSERT INTO k VALUES (?1, 'bound')").unwrap();
        assert_eq!(
            ints(&conn, "SELECT id FROM k ORDER BY id"),
            vec![1, 2],
            "a SQLite table turns a NULL key into a new rowid"
        );
        conn.close().unwrap();
    }

    /// Engine review 20 LOW 26: the 4c paths the first red never ran, each on its own: INSERT OR
    /// IGNORE skips a NULL-key row (the others are written); INSERT ... SELECT of a NULL key raises
    /// NOT NULL; UPDATE SET rowid = NULL raises it; UPDATE OR IGNORE SET id = NULL leaves the row as
    /// it was. Mutant `null_key_takes_rowid` (all paths at once) fails every arm.
    #[test]
    fn every_null_key_path_of_a_frontend_table_is_refused_or_skipped() {
        fn not_null(result: &crate::Result<()>) -> bool {
            matches!(result, Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id")
        }
        fn ids(conn: &Arc<crate::Connection>) -> Vec<i64> {
            conn.prepare("SELECT id FROM k ORDER BY id")
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .iter()
                .map(|row| row[0].as_int().unwrap())
                .collect()
        }
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "pgkey-paths.db", Arc::new(PgKeyTestDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        conn.execute("CREATE TABLE src (x INTEGER)").unwrap();
        conn.execute("INSERT INTO src VALUES (NULL)").unwrap();
        let ignored = conn.execute("INSERT OR IGNORE INTO k VALUES (1, 'a'), (NULL, 'skipped'), (2, 'b')");
        assert!(ignored.is_ok(), "INSERT OR IGNORE with a NULL key failed: {ignored:?}");
        assert_eq!(ids(&conn), vec![1, 2], "CLAIM: INSERT OR IGNORE did not skip the NULL-key row alone");
        let selected = conn.execute("INSERT INTO k (id, v) SELECT x, 's' FROM src");
        assert!(not_null(&selected), "CLAIM: INSERT ... SELECT of a NULL key: {selected:?}");
        let rowid = conn.execute("UPDATE k SET rowid = NULL WHERE id = 1");
        assert!(not_null(&rowid), "CLAIM: UPDATE SET rowid = NULL: {rowid:?}");
        let update_ignored = conn.execute("UPDATE OR IGNORE k SET id = NULL WHERE id = 2");
        assert!(update_ignored.is_ok(), "UPDATE OR IGNORE SET id = NULL failed: {update_ignored:?}");
        assert_eq!(ids(&conn), vec![1, 2], "CLAIM: the refused and skipped statements changed the table");
    }

    /// Engine review 20 LOW 24: a multi-row UPDATE inside BEGIN whose third row sets the NOT NULL
    /// key to NULL raises NOT NULL, and must undo the rows it already changed: the statement journal
    /// counted NOT NULL columns among the SET targets but skipped the rowid and its alias, so none was
    /// opened and the first two rows' new keys stayed. After COMMIT the keys are as before. Mutant
    /// `update_key_journal_skipped`.
    #[test]
    fn a_multi_row_update_failing_the_key_not_null_undoes_its_own_rows() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "pgkey-update.db", Arc::new(PgKeyTestDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        conn.execute("INSERT INTO k VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        conn.execute("BEGIN").unwrap();
        let updated = conn.execute("UPDATE k SET id = CASE WHEN id = 3 THEN NULL ELSE id + 10 END");
        assert!(
            matches!(&updated, Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id"),
            "premise: the NULL key raised NOT NULL: {updated:?}"
        );
        conn.execute("COMMIT").unwrap();
        let ids: Vec<i64> = conn
            .prepare("SELECT id FROM k ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .iter()
            .map(|row| row[0].as_int().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3], "CLAIM: the failed UPDATE kept the keys it had already changed");
    }

    /// Engine review 20 item 12: the NOT NULL key check's statement-journal term matters only where
    /// nothing else opens the journal: under statement-level OR REPLACE (default ABORT already does).
    /// `INSERT OR REPLACE` of two rows whose second key is NULL raises NOT NULL (OR REPLACE has no
    /// default to substitute for a key), and undoes only its own first row: after COMMIT the
    /// table holds the earlier statement's row alone. Mutant `notnull_key_journal_dropped`.
    #[test]
    fn an_or_replace_insert_with_a_null_key_undoes_only_its_own_rows() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "pgkey-replace.db", Arc::new(PgKeyTestDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO k VALUES (3, 'three')").unwrap();
        let replaced = conn.execute("INSERT OR REPLACE INTO k VALUES (5, 'five'), (NULL, 'n')");
        assert!(
            matches!(&replaced, Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id"),
            "premise: the NULL key raised NOT NULL under OR REPLACE: {replaced:?}"
        );
        conn.execute("COMMIT").unwrap();
        let ids: Vec<i64> = conn
            .prepare("SELECT id FROM k ORDER BY id")
            .unwrap()
            .run_collect_rows()
            .unwrap()
            .iter()
            .map(|row| row[0].as_int().unwrap())
            .collect();
        assert_eq!(ids, vec![3], "CLAIM: the failed OR REPLACE INSERT did not undo its own first row");
    }

    /// Engine review 20 HIGH 1, the branch arm: a branch forked after ALTER TABLE ADD COLUMN on a
    /// frontend table refuses an explicit NULL key as the trunk does. A lock-free fork that sees a
    /// trunk commit parses the stored schema at its first connect (678b18ba4; review 20 #19), which
    /// lost the frontend's mark when ALTER stored the table without it. Red at base when that first
    /// connect reparses; a guard otherwise. Both stores.
    #[test]
    fn a_branch_forked_after_alter_refuses_an_explicit_null_key() {
        for catalog in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("pgkey-branch.db");
            let sync = crate::branch::SyncClass::Off;
            let durability = if catalog {
                crate::branch::BranchDurability::Catalog { sync }
            } else {
                crate::branch::BranchDurability::Durable { sync }
            };
            let db = Database::open_file_with_flags(
                Arc::new(crate::PlatformIO::new().unwrap()),
                path.to_str().unwrap(),
                OpenFlags::Create,
                DatabaseOpts::new().with_branch_durability(durability),
                None,
                Arc::new(PgKeyTestDialect),
            )
            .unwrap();
            let trunk = db.connect().unwrap();
            trunk
                .execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            trunk.execute("ALTER TABLE k ADD COLUMN w INTEGER").unwrap();
            trunk.execute("INSERT INTO k (id, v) VALUES (1, 'one')").unwrap();
            let on_trunk = trunk.execute("INSERT INTO k (id, v) VALUES (NULL, 'trunk')");
            assert!(
                matches!(&on_trunk, Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id"),
                "catalog={catalog}: premise: the trunk refuses an explicit NULL key after the ALTER: {on_trunk:?}"
            );
            let branch = trunk.fork_branch().unwrap();
            let on_branch = branch
                .connect()
                .unwrap()
                .execute("INSERT INTO k (id, v) VALUES (NULL, 'branch')");
            assert!(
                matches!(&on_branch, Err(crate::LimboError::Constraint(m)) if m == "NOT NULL constraint failed: k.id"),
                "catalog={catalog}: CLAIM: a branch forked after the ALTER took a new rowid for a NULL key: {on_branch:?}"
            );
            drop(branch);
        }
    }
}
