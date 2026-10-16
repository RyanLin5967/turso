use std::ops::{Deref, DerefMut};
use thiserror::Error;

pub use turso_parser::sql_error::{SqlErrorKind, SqlErrorMessage};

pub mod translator;

/// libpg_query's bindings, for callers that edit a parse tree (the frontend's table rebuild).
pub use pg_query;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("{0}")]
    ParseError(SqlErrorMessage),
    /// A recursive entry of the translator found its thread's stack nearly spent
    /// (`turso_stack_guard::NestGuard`): PostgreSQL's 54001, which the frontend reports as the engine's
    /// `LimboError::StackDepthLimitExceeded`.
    #[error("stack depth limit exceeded")]
    StackDepthLimitExceeded,
}

/// Provision for libpg_query, its protobuf codec, and frontend preparation. The C
/// tree-output walk needs a segment sized from the SQL; the Rust codec and translator
/// additionally use the shared NestGuard. Debug measurements over scalar subqueries,
/// sums, and spaced unary expressions found at most 17,936 stack bytes per SQL byte;
/// round up to the next power of two (32 KiB), retaining the 4 MiB base provision.
/// The widest measured codec guard gap was 34,784 bytes, below shared LEVEL_COST.
/// The provision stays capped at 256 MiB; the shared guard refuses at its margin.
/// These measurements cover debug builds; optimized stack costs were not measured.
fn pg_query_stack(sql: &str) -> usize {
    PG_QUERY_BASE.saturating_add(sql.len().saturating_mul(PG_QUERY_PER_BYTE)).min(PG_QUERY_MAX)
}
const PG_QUERY_BASE: usize = 4 << 20;
const PG_QUERY_PER_BYTE: usize = 32 << 10;
const PG_QUERY_MAX: usize = 256 << 20;

/// Encoding and the C deparser both walk decoded trees, including those accepted
/// near the parser's maximum provision.
const PG_DEPARSE_STACK: usize = PG_QUERY_MAX;

/// The codec's shared guard and libpg_query's stack refusal both map to 54001.
fn pg_query_error(e: pg_query::Error) -> ParseError {
    let text = e.to_string();
    if text.contains("recursion limit reached") || text.contains("stack depth limit exceeded") {
        ParseError::StackDepthLimitExceeded
    } else {
        ParseError::ParseError(text.into())
    }
}

/// One of the translator's recursive steps (`turso_stack_guard::NestGuard`), held for the step.
pub(crate) fn nest() -> Result<turso_stack_guard::NestGuard, ParseError> {
    turso_stack_guard::NestGuard::enter().map_err(|_| ParseError::StackDepthLimitExceeded)
}

impl From<ParseError> for SqlErrorMessage {
    fn from(error: ParseError) -> Self {
        match error {
            ParseError::ParseError(message) => message,
            ParseError::StackDepthLimitExceeded => SqlErrorMessage::classified(
                SqlErrorKind::StatementTooComplex,
                "stack depth limit exceeded",
            ),
        }
    }
}

thread_local! {
    static LIBPG_QUERY_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Calls into libpg_query (parse, split, scan, normalize, fingerprint) made by this thread, for
/// per-statement work budgets: a wire session runs on one thread, so the difference across a
/// statement is that statement's count. Observation only.
pub fn libpg_query_calls() -> u64 {
    LIBPG_QUERY_CALLS.with(|c| c.get())
}

fn count_libpg_query_call() {
    LIBPG_QUERY_CALLS.with(|c| c.set(c.get() + 1));
}

/// Parse a PostgreSQL SQL statement using pg_query
pub fn parse(sql: &str) -> Result<ParseResult, ParseError> {
    count_libpg_query_call();
    let stack = pg_query_stack(sql);
    turso_stack_guard::with_stack(stack, || {
        match pg_query::parse(sql) {
            Ok(inner) => Ok(ParseResult {
                inner: Some(inner), stack, virtual_generated: Default::default(),
            }),
            Err(original) => {
                if let Some((rewritten, virtual_generated)) = normalize_virtual_generated(sql) {
                    count_libpg_query_call();
                    if let Ok(inner) = pg_query::parse(&rewritten) {
                        return Ok(ParseResult { inner: Some(inner), stack, virtual_generated });
                    }
                }
                Err(pg_query_error(original))
            }
        }
    })
}

// libpg_query 17 knows STORED but not PG18's VIRTUAL storage keyword. Its lexer identifies the
// clause and byte ranges; equal-length replacement preserves all AST source locations.
fn normalize_virtual_generated(sql: &str) -> Option<(String, std::collections::HashSet<i32>)> {
    let columns = generated_storage_tokens(sql)?;
    let mut rewritten = sql.to_owned();
    let mut locations = std::collections::HashSet::new();
    for (location, range) in columns {
        if sql[range.clone()].eq_ignore_ascii_case("virtual") {
            rewritten.replace_range(range, "STORED ");
            locations.insert(location);
        }
    }
    (!locations.is_empty()).then_some((rewritten, locations))
}

fn generated_storage_tokens(sql: &str) -> Option<Vec<(i32, std::ops::Range<usize>)>> {
    use pg_query::protobuf::Token;
    count_libpg_query_call();
    let scanned = turso_stack_guard::with_stack(pg_query_stack(sql), || pg_query::scan(sql)).ok()?;
    let tokens: Vec<_> = scanned.tokens.iter()
        .filter(|token| !matches!(token.token(), Token::SqlComment | Token::CComment))
        .collect();
    let mut columns = Vec::new();
    for (i, token) in tokens.iter().enumerate() {
        if token.token() != Token::Generated
            || tokens.get(i + 1).map(|token| token.token()) != Some(Token::Always)
            || tokens.get(i + 2).map(|token| token.token()) != Some(Token::As)
            || tokens.get(i + 3).map(|token| token.token()) != Some(Token::Ascii40)
        {
            continue;
        }
        let mut depth = 1usize;
        let mut j = i + 4;
        while let Some(token) = tokens.get(j) {
            match token.token() {
                Token::Ascii40 => depth += 1,
                Token::Ascii41 => depth -= 1,
                _ => {}
            }
            j += 1;
            if depth == 0 { break; }
        }
        if depth != 0 { continue; }
        let Some(storage) = tokens.get(j) else { continue; };
        let range = storage.start as usize..storage.end as usize;
        if sql[range.clone()].eq_ignore_ascii_case("virtual")
            || sql[range.clone()].eq_ignore_ascii_case("stored")
        {
            let location = if i >= 2 && tokens[i - 2].token() == Token::Constraint {
                tokens[i - 2].start
            } else { token.start };
            columns.push((location, range));
        }
    }
    Some(columns)
}

/// A decoded tree owns its destruction stack even when a caller returns early.
#[derive(Debug)]
pub struct ParseResult {
    inner: Option<pg_query::ParseResult>,
    stack: usize,
    virtual_generated: std::collections::HashSet<i32>,
}

impl ParseResult {
    pub(crate) fn virtual_generated_columns(&self) -> &std::collections::HashSet<i32> {
        &self.virtual_generated
    }
}

impl Deref for ParseResult {
    type Target = pg_query::ParseResult;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("the parse tree is present until Drop")
    }
}

impl DerefMut for ParseResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("the parse tree is present until Drop")
    }
}

impl Drop for ParseResult {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            turso_stack_guard::with_stack(self.stack, || drop(inner));
        }
    }
}

/// Keep metadata walks, translation, compilation, and their intermediate drops on
/// the same bounded preparation stack. Re-entrant work keeps the existing segment.
pub fn with_preparation_stack<R>(sql: &str, f: impl FnOnce() -> R) -> R {
    turso_stack_guard::with_preparation_stack(pg_query_stack(sql), f)
}

/// Deparse a parse tree back to SQL text (libpg_query's deparser).
pub fn deparse(protobuf: &pg_query::protobuf::ParseResult) -> Result<String, ParseError> {
    count_libpg_query_call();
    turso_stack_guard::with_stack(PG_DEPARSE_STACK, || pg_query::deparse(protobuf)).map_err(pg_query_error)
}

/// Deparse a changed tree while retaining PG18 generated-column storage kinds from its owner.
pub fn deparse_with_metadata(
    protobuf: &pg_query::protobuf::ParseResult,
    owner: &ParseResult,
) -> Result<String, ParseError> {
    turso_stack_guard::with_stack(owner.stack.max(PG_DEPARSE_STACK), || {
    let mut sql = deparse(protobuf)?;
    if owner.virtual_generated.is_empty() { return Ok(sql); }
    use pg_query::protobuf::node::Node;
    let mut kinds = Vec::new();
    for raw in &protobuf.stmts {
        if let Some(Node::CreateStmt(create)) = raw.stmt.as_ref().and_then(|node| node.node.as_ref()) {
            for element in &create.table_elts {
                if let Some(Node::ColumnDef(column)) = element.node.as_ref() {
                    for constraint in &column.constraints {
                        if let Some(Node::Constraint(constraint)) = constraint.node.as_ref() {
                            if constraint.contype() == pg_query::protobuf::ConstrType::ConstrGenerated {
                                kinds.push(owner.virtual_generated.contains(&constraint.location));
                            }
                        }
                    }
                }
            }
        }
    }
    let columns = generated_storage_tokens(&sql).filter(|columns| columns.len() == kinds.len())
        .ok_or_else(|| ParseError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::FeatureNotSupported, "cannot preserve generated column storage kinds")))?;
    for ((_, range), is_virtual) in columns.into_iter().zip(kinds).rev() {
        if is_virtual { sql.replace_range(range, "VIRTUAL"); }
    }
    Ok(sql)
    })
}

/// The highest parameter number a statement may hold: Bind counts parameters in 16 bits. A `$n`
/// outside 1..=MAX_PARAMETER names no parameter (42P02) on every path, a branch call's included,
/// before anything is sized by it (wire review 8 item 3, review 9 item 1); one above i32::MAX is
/// 42601, as PostgreSQL 18's scanner refuses it ([`checked_param_numbers`]; review 12 item 1).
pub const MAX_PARAMETER: u32 = 65535;

/// Every parameter number ($n) in a parse tree, sorted and without repeats, from the WHOLE tree:
/// WITH, ON CONFLICT, RETURNING, set operations and every other clause, whether or not the
/// translator or the engine keeps it (the engine registers only the $n it compiles: a $n in a
/// clause it folds away has no slot; wire review 8 item 5). No libpg_query call: the tree is
/// serialized (pg_query's protobuf types derive Serialize) and each
/// `"ParamRef":{"number":N,"location":L` read off it, a pattern a string value in the tree cannot
/// hold, since its quotes are escaped. The numbers are libpg_query's, which wrap above i32::MAX: so
/// each number is read again from `sql` (the text `parse` parsed) at its ParamRef's location, as
/// PostgreSQL 18's scanner reads it. libpg_query reads a `$n` with atol into a 32-bit int
/// (PostgreSQL 17's scanner), so `$4294967297` is $1 in its tree, and a branch call of it bound the
/// wrong parameter and created a branch (wire review 12 item 1). A number above i32::MAX is
/// PostgreSQL 18's "parameter number too large" (42601); a tree number that is not the text's, or a
/// tree whose parameters cannot be read, is refused (fail closed).
pub fn checked_param_numbers(parse: &ParseResult, sql: &str) -> Result<Vec<i32>, ParseError> {
    let unreadable = || {
        ParseError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::InternalError,
            "could not read the statement's parameters from its parse tree".to_string(),
        ))
    };
    let mut numbers = Vec::new();
    for (number, location) in param_refs(parse).ok_or_else(unreadable)? {
        let digits = sql
            .as_bytes()
            .get(location..)
            .and_then(|b| b.strip_prefix(b"$"))
            .ok_or_else(unreadable)?;
        let len = digits.iter().take_while(|c| c.is_ascii_digit()).count();
        let text = &sql[location + 1..location + 1 + len];
        let value = text.bytes().try_fold(0u64, |v, d| {
            v.checked_mul(10)?.checked_add(u64::from(d - b'0'))
        });
        match value {
            Some(v) if v <= i32::MAX as u64 => {
                if v as i32 != number {
                    return Err(unreadable());
                }
                numbers.push(number);
            }
            _ => {
                return Err(ParseError::ParseError(format!(
                    "parameter number too large at or near \"${text}\""
                ).into()))
            }
        }
    }
    numbers.sort_unstable();
    numbers.dedup();
    Ok(numbers)
}

/// Each ParamRef's number and byte location in a parse tree, read off its serialization (see
/// [`checked_param_numbers`]); None if the tree cannot be serialized or a ParamRef not read.
fn param_refs(parse: &ParseResult) -> Option<Vec<(i32, usize)>> {
    const KEY: &str = "\"ParamRef\":{\"number\":";
    const LOCATION: &str = ",\"location\":";
    let json =
        turso_stack_guard::with_stack(parse.stack, || serde_json::to_string(&parse.protobuf))
            .ok()?;
    let int = |s: &str| -> (String, usize) {
        let end = s
            .find(|c: char| !(c.is_ascii_digit() || c == '-'))
            .unwrap_or(s.len());
        (s[..end].to_string(), end)
    };
    json.match_indices(KEY)
        .map(|(at, _)| {
            let rest = &json[at + KEY.len()..];
            let (number, end) = int(rest);
            let rest = rest[end..].strip_prefix(LOCATION)?;
            let (location, _) = int(rest);
            Some((number.parse().ok()?, location.parse().ok()?))
        })
        .collect()
}

/// `s` without PostgreSQL's whitespace at either end: its lexer's `space` (space, tab, newline,
/// carriage return, form feed, vertical tab) and nothing else. `str::trim` also strips Unicode
/// spaces, which PostgreSQL lexes as identifier bytes: `COMMIT<NBSP>` was trimmed to a COMMIT the
/// engine ran, where PostgreSQL answers 42601 (wire review 13 item 1).
pub fn pg_trim(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_ascii() && pg_space(c as u8))
}

pub use turso_parser::pg_input::pg_space;

/// The length of the SQL comment at the start of `b`, as PostgreSQL's lexer reads one (`--` to the
/// line's end, `/* */` nested): None if none starts there, Some(None) for one that never ends.
pub fn sql_comment(b: &[u8]) -> Option<Option<usize>> {
    if b.starts_with(b"--") {
        let end = b.iter().position(|&c| c == b'\n' || c == b'\r');
        return Some(Some(end.map_or(b.len(), |p| p + 1)));
    }
    if !b.starts_with(b"/*") {
        return None;
    }
    let (mut depth, mut i) = (0usize, 0usize);
    while i < b.len() {
        if b[i..].starts_with(b"/*") {
            depth += 1;
            i += 2;
        } else if b[i..].starts_with(b"*/") {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return Some(Some(i));
            }
        } else {
            i += 1;
        }
    }
    Some(None)
}

/// A statement's parameters as PostgreSQL's lexer reads them, without a parse (see [`scan_params`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamScan {
    /// The highest `$n` the text holds, 0 for none; None for a number past MAX_PARAMETER, which
    /// the parser refuses (42601), or unreadable tokens; no count is taken from either.
    pub highest: Option<u32>,
    /// PostgreSQL analyses the statement at Parse and so counts its `$n`: a query (SELECT, VALUES,
    /// TABLE, WITH, a parenthesised one), INSERT, UPDATE, DELETE, MERGE, CALL, EXPLAIN, DECLARE,
    /// and CREATE TABLE ... AS. Any other statement is a utility statement, whose parameters are
    /// the ones Parse declared, whatever its text holds (COPY's, ALTER's and CREATE's `$n` are
    /// never counted).
    pub analysed: bool,
}

impl Default for ParamScan {
    fn default() -> Self {
        Self {
            highest: Some(0),
            analysed: false,
        }
    }
}

/// Read parameter tokens with the same libpg_query lexer that produces the parse tree used at
/// Execute. In particular, continued strings retain their escape mode and identifier bytes never
/// become parameter tokens. Bind keeps this result from Parse; it does not tokenize SQL itself.
pub fn scan_params(sql: &str) -> ParamScan {
    use pg_query::protobuf::Token;

    count_libpg_query_call();
    let Ok(tokens) = pg_query::scan(sql) else {
        // No count is available for a lexical error. The normal prepare path reports that error;
        // never treat an unreadable statement as requiring zero values.
        return ParamScan {
            highest: None,
            analysed: true,
        };
    };
    let mut scan = ParamScan::default();
    let mut first = None;
    let mut depth = 0usize;
    let (mut saw_table, mut after_as) = (false, false);
    for token in tokens.tokens {
        let kind = token.token();
        if matches!(kind, Token::SqlComment | Token::CComment | Token::Ascii59) {
            continue;
        }
        if kind == Token::Param {
            // libpg_query 17 stores parameter numbers in an i32. Read its token's original digits
            // so a wrapped number cannot become a smaller count (checked_param_numbers also checks
            // the original text at Execute).
            let value = sql
                .get(token.start as usize..token.end as usize)
                .and_then(|text| text.strip_prefix('$'))
                .and_then(|digits| digits.parse::<u32>().ok())
                .filter(|n| *n <= MAX_PARAMETER);
            scan.highest = scan.highest.zip(value).map(|(a, b)| a.max(b));
        }
        let first = *first.get_or_insert(kind);
        if first == Token::Create && depth == 0 {
            if after_as {
                if saw_table
                    && matches!(
                        kind,
                        Token::Ascii40
                            | Token::Select
                            | Token::With
                            | Token::Values
                            | Token::Table
                            | Token::Execute
                    )
                {
                    scan.analysed = true;
                }
                after_as = false;
            }
            match kind {
                Token::As => after_as = true,
                Token::Table => saw_table = true,
                _ => {}
            }
        }
        match kind {
            Token::Ascii40 => depth += 1,
            Token::Ascii41 => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    scan.analysed |= matches!(
        first,
        Some(
            Token::Select
                | Token::Values
                | Token::Table
                | Token::With
                | Token::Ascii40
                | Token::Insert
                | Token::Update
                | Token::DeleteP
                | Token::Merge
                | Token::Call
                | Token::Explain
                | Token::Declare
        )
    );
    scan
}

/// The index of the first byte at or after `i` that is neither PostgreSQL's whitespace, nor a
/// comment, nor a `;` (an empty statement): where the next token starts, `b.len()` when none does.
/// None inside a comment that never ends.
pub fn skip_blank(b: &[u8], mut i: usize) -> Option<usize> {
    loop {
        while i < b.len() && (pg_space(b[i]) || b[i] == b';') {
            i += 1;
        }
        match sql_comment(&b[i..]) {
            Some(len) => i += len?,
            None => return Some(i),
        }
    }
}

/// Split a multi-statement SQL string into its statements as PostgreSQL does for a simple Query:
/// the whole text is parsed first (libpg_query's parser split), so a text any part of which does
/// not parse is refused whole, and its caller runs none of it. Returns the statements' texts,
/// trimmed; empty statements are none.
///
/// The scanner split this replaced emitted every stretch holding a keyword as a statement and
/// skipped the rest: `INSERT ..; COMMIT<NBSP>; INSERT ..` ran both INSERTs (wire review 16 item 1),
/// and `INSERT ..; COMMIT; SELEC * FROM t` committed the INSERT before the malformed part, which
/// holds the keyword FROM, was refused (wire review 18 item 6). For the statement-at-a-time split
/// a REPL wants, see [`split_statements_lenient`].
pub fn split_statements(sql: &str) -> Result<Vec<String>, ParseError> {
    count_libpg_query_call();
    let stack = pg_query_stack(sql);
    let parts = turso_stack_guard::with_stack(stack, || pg_query::split_with_parser(sql));
    match parts {
        Ok(parts) => Ok(parts.into_iter().map(|part| pg_trim(part).to_owned())
            .filter(|part| !part.is_empty()).collect()),
        Err(original) => {
            if let Some((rewritten, _)) = normalize_virtual_generated(sql) {
                count_libpg_query_call();
                if let Ok(parts) = turso_stack_guard::with_stack(stack, || pg_query::split_with_parser(&rewritten)) {
                    return Ok(parts.into_iter().map(|part| {
                        let start = part.as_ptr() as usize - rewritten.as_ptr() as usize;
                        pg_trim(&sql[start..start + part.len()]).to_owned()
                    }).filter(|part| !part.is_empty()).collect());
                }
            }
            Err(pg_query_error(original))
        }
    }
}

/// Split a multi-statement SQL string statement by statement, as psql sends a script: libpg_query's
/// scanner finds the statements (`;` inside a string literal, a comment or a dollar quote is no
/// separator), and a stretch it skips that is not blank (whitespace, comments, `;`) is a part of
/// its own, in its place, so it fails when it is run instead of vanishing. The scanner emits a
/// statement only where it saw a keyword: `SELECT 1; selecgt 2` lost `selecgt 2`, and `INSERT ..;
/// COMMIT<NBSP>; INSERT ..` lost its COMMIT (wire review 16 item 1). The non-server REPL and
/// PgConnection::execute run their parts this way: the valid parts run, a malformed one fails
/// (wire review 18 item 7). A simple Query over the wire uses [`split_statements`].
pub fn split_statements_lenient(sql: &str) -> Result<Vec<String>, ParseError> {
    count_libpg_query_call();
    let found = turso_stack_guard::with_stack(pg_query_stack(sql), || pg_query::split_with_scanner(sql))
        .map_err(pg_query_error)?;
    let b = sql.as_bytes();
    let blank = |gap: &[u8]| skip_blank(gap, 0) == Some(gap.len());
    let mut parts = Vec::with_capacity(found.len());
    let mut at = 0;
    for part in &found {
        // Each part is a slice of `sql` (split_with_scanner returns `&query[start..end]`); one that
        // is not refuses the split, and its caller runs the text whole.
        let start = (part.as_ptr() as usize)
            .checked_sub(sql.as_ptr() as usize)
            .filter(|start| *start >= at && *start + part.len() <= b.len())
            .ok_or_else(|| {
                ParseError::ParseError("the scanner split lost its place".to_string().into())
            })?;
        if !blank(&b[at..start]) {
            parts.push(&sql[at..start]);
        }
        parts.push(*part);
        at = start + part.len();
    }
    if !blank(&b[at..]) {
        parts.push(&sql[at..]);
    }
    Ok(parts
        .into_iter()
        .map(|s| {
            // A stray stretch keeps the `;` that ends it; a statement's is not in its slice.
            let s = pg_trim(s);
            pg_trim(s.trim_end_matches(';')).to_string()
        })
        .filter(|s| !s.is_empty())
        .collect())
}

/// Get tables referenced in a query
pub fn get_tables(sql: &str) -> Result<Vec<String>, ParseError> {
    let result = parse(sql)?;
    Ok(result.tables())
}

/// Normalize a query (replace constants with $1, $2, etc.)
pub fn normalize(sql: &str) -> Result<String, ParseError> {
    count_libpg_query_call();
    turso_stack_guard::with_stack(pg_query_stack(sql), || pg_query::normalize(sql)).map_err(pg_query_error)
}

/// Get a fingerprint for a query (for caching/deduplication)
pub fn fingerprint(sql: &str) -> Result<String, ParseError> {
    count_libpg_query_call();
    turso_stack_guard::with_stack(pg_query_stack(sql), || pg_query::fingerprint(sql))
        .map(|fp| fp.hex)
        .map_err(pg_query_error)
}

/// What a schema qualifier names in this frontend: the one rule every reader of a relation's or a
/// column's qualifier uses. It was spelled seven ways, and a three-part column reference and COPY
/// missed it (wire review 18 items 21 and 22). Case-insensitive, as every spelling of it was. A
/// search path's entries follow the engine's own rule instead (public alone is main there).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaQualifier<'a> {
    /// No qualifier: the search path decides.
    Unqualified,
    /// The main database: public, PostgreSQL's schema for it, whatever the search path (wire
    /// review 17 item 1), and main, the engine's name for it, which stored view text holds.
    Main,
    /// pg_catalog, whose relations the engine finds unqualified.
    PgCatalog,
    /// information_schema, whose views the engine names `information_schema_<view>`.
    InformationSchema,
    /// Any other schema: the attached database of that name.
    Attached(&'a str),
}

/// [`SchemaQualifier`] of `schema` ("" for none).
pub fn schema_qualifier(schema: &str) -> SchemaQualifier<'_> {
    if schema.is_empty() {
        SchemaQualifier::Unqualified
    } else if schema.eq_ignore_ascii_case("public") || schema.eq_ignore_ascii_case("main") {
        SchemaQualifier::Main
    } else if schema.eq_ignore_ascii_case("pg_catalog") {
        SchemaQualifier::PgCatalog
    } else if schema.eq_ignore_ascii_case("information_schema") {
        SchemaQualifier::InformationSchema
    } else {
        SchemaQualifier::Attached(schema)
    }
}

/// Quote an identifier following PostgreSQL's server-side quote_identifier()
/// rules: return it bare only when it is all lower-case ASCII letters,
/// digits, and underscores, does not start with a digit, and is not a
/// keyword outside the unreserved category; otherwise wrap it in double
/// quotes with embedded quotes doubled.
pub fn quote_identifier(ident: &str) -> String {
    let safe_chars = !ident.is_empty()
        && !ident.starts_with(|c: char| c.is_ascii_digit())
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if safe_chars && !keyword_requires_quoting(ident) {
        return ident.to_string();
    }
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// pg_query's scanner is the same lexer the PostgreSQL server uses, so its
/// keyword classification matches server-side quote_identifier().
fn keyword_requires_quoting(ident: &str) -> bool {
    use pg_query::protobuf::KeywordKind;
    count_libpg_query_call();
    let scan =
        pg_query::scan(ident).expect("scanning a bare lower-case ASCII identifier cannot fail");
    match scan.tokens.as_slice() {
        [token] => !matches!(
            token.keyword_kind(),
            KeywordKind::NoKeyword | KeywordKind::UnreservedKeyword
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let result = parse(sql);
        assert!(result.is_ok());

        let tables = get_tables(sql).unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_parse_complex_query() {
        let sql = "WITH regional_sales AS (
            SELECT region, SUM(amount) AS total_sales
            FROM orders
            GROUP BY region
        )
        SELECT * FROM regional_sales ORDER BY total_sales DESC";

        assert!(parse(sql).is_ok());
    }

    #[test]
    fn test_normalize() {
        let sql = "SELECT * FROM users WHERE age > 25 AND name = 'John'";
        let normalized = normalize(sql).unwrap();
        assert!(normalized.contains("$1"));
        assert!(normalized.contains("$2"));
    }

    #[test]
    fn test_parse_postgresql_specific() {
        // Test PostgreSQL-specific syntax that we struggled with before
        let queries = vec![
            "SELECT * FROM users ORDER BY name USING >",
            "SELECT * FROM person* p",
            "VALUES (1,2), (3,4)",
            "SELECT foo FROM (SELECT 1) AS foo",
            "INSERT INTO users (name, data) VALUES ('John', '{\"key\": \"value\"}'::jsonb)",
            "SELECT * FROM users WHERE data @> '{\"active\": true}'",
            "UPDATE users SET (name, age) = ('John', 30) WHERE id = 1",
            "CREATE TABLE posts PARTITION OF main_posts FOR VALUES IN (1, 2, 3)",
            "SELECT COUNT(*) FILTER (WHERE active) FROM users",
            "SELECT DISTINCT ON (region) * FROM sales ORDER BY region, amount DESC",
        ];

        for sql in queries {
            let result = parse(sql);
            assert!(result.is_ok(), "Failed to parse: {sql}");
        }
    }

    /// checked_param_numbers finds every $n in the tree, in clauses the engine folds away or the
    /// translator never reads (WITH, ON CONFLICT, a false AND, HAVING), and none in a string, a
    /// comment, a quoted identifier or a dollar-quoted string that looks like one (wire review 8
    /// item 5). It read param_numbers, deleted as dead (wire review 14 item 24).
    #[test]
    fn checked_param_numbers_reads_the_whole_tree() {
        for (sql, want) in [
            ("SELECT 1", vec![]),
            ("SELECT v FROM t WHERE false AND id = $1", vec![1]),
            ("SELECT v FROM t WHERE id = $2 AND (true OR v = $1)", vec![1, 2]),
            ("SELECT count(*) FROM t HAVING count(*) > $1", vec![1]),
            ("WITH c AS (SELECT $3 AS x) SELECT x FROM c LIMIT $1", vec![1, 3]),
            (
                "INSERT INTO t VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = $4 WHERE t.v <> $5",
                vec![1, 2, 4, 5],
            ),
            ("SELECT $1, $1, $1", vec![1]),
            ("SELECT '$1', \"$2\", $$ $3 $$ /* $4 */ -- $5\n", vec![]),
            ("SELECT '\"ParamRef\":{\"number\":7'", vec![]),
            ("SELECT 1 LIMIT $2147483647", vec![2147483647]),
        ] {
            let parsed = parse(sql).unwrap();
            assert_eq!(checked_param_numbers(&parsed, sql).ok(), Some(want), "{sql}");
        }
    }
}
